package rules

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// goroutineRows are the rows of the R1 table that Sagüin starts itself, with
// a go statement. main is the process's own goroutine, and net/http starts
// the HTTP request goroutines (and a WebSocket connection's reader, which is
// still the reader row; a TCP connection's reader is started by Sagüin).
var goroutineRows = map[string]bool{
	"core":         true,
	"store writer": true,
	"store reader": true,
	"logger":       true,
	"listener":     true,
	"reader":       true,
	"writer":       true,
	"dialer":       true,
}

// goAllowed lists the functions of the broker's code allowed a go statement
// (or sync.WaitGroup's Go method, which is one), each entry's comment naming
// the R1 table rows it starts. A function is its package path and
// name, a method's name carrying its receiver type without type parameters:
// "github.com/ifnesi/saguin/internal/core.(*Core).Run" or
// "….Core.Run" for a value receiver. Each item that starts a goroutine adds
// its function here, with the maintainer's approval, after the table in the
// design lists the goroutine.
var goAllowed = map[string]bool{
	// 0002a: the logger, the one goroutine that owns the log output.
	"github.com/ifnesi/saguin/internal/logger.Start": true,
	// 0006a: the store's start function, which starts the writer and the
	// read_connections readers, and no other row; approved by the
	// maintainer on 261011 (0006a's comment of 261011 133832).
	"github.com/ifnesi/saguin/internal/store.Start": true,
	// 0005a: the listener's accept loop, which starts each accepted
	// connection's reader and writer, and no other row; approved by the
	// maintainer on 261012.
	"github.com/ifnesi/saguin/internal/connection.(*Listener).Run": true,
	// A WebSocket connection's reader, the goroutine net/http starts
	// for it, which starts the connection's writer once the upgrade
	// succeeds, and no other row (design.md, R1: "That reader starts the
	// connection's writer once the upgrade succeeds"); agreed with the
	// maintainer on 261103 (0013d's comment of 261103 070113).
	"github.com/ifnesi/saguin/internal/connection.(*Listener).ServeHTTP": true,
	// 0008a: main's run, which starts the core and each listener, and no
	// other row; approved by the maintainer on 261018.
	"github.com/ifnesi/saguin/cmd/saguin.run": true,
	// 0019b: the core's side starting one dialer per bridge connection
	// attempt, and no other row (design.md, the Bridges diagram, "Bridges:
	// the core starts and cancels each connection attempt": the core starts
	// the dialer); approved by the maintainer on 261105.
	"github.com/ifnesi/saguin/internal/core.(*Core).startDialer": true,
	// 0019b: the dialer, which starts its link's reader and writer, and no
	// other row ("Dials, starts"); approved by the maintainer on 261105.
	"github.com/ifnesi/saguin/internal/connection.Dial": true,
}

// atomicAllowed lists the packages allowed sync/atomic: R3 puts atomics only
// in metric values and in swapping in a reloaded copy (Access and reload).
var atomicAllowed = map[string]bool{
	// 0002a: the metric values, which every goroutine changes and a scrape
	// reads (metrics.md, Rules).
	"github.com/ifnesi/saguin/internal/metrics": true,
	// 0018g: the reloaded copies SIGUSR1 swaps in, read by connections,
	// listeners and TLS handshakes (Access and reload: "The core swaps it in
	// atomically as it applies that event"); approved by the maintainer on
	// 261105.
	"github.com/ifnesi/saguin/internal/config": true,
}

// syncNotLocks are the names of package sync that R3 does not count as locks.
// R3 is "Messages, not locks", and allows no mutex anywhere. Mutex, RWMutex
// and Locker are mutexes; Cond waits on one; Map is state shared between
// goroutines behind one, which R1 gives to the core alone; Once, OnceFunc,
// OnceValue and OnceValues hold every other caller until the first returns,
// which is a lock. WaitGroup only waits for goroutines to end (main does, at a
// graceful stop) and Pool never blocks anyone; neither excludes a goroutine
// from state, so both are allowed anywhere. WaitGroup's Go method starts a
// goroutine, which the R1 scan treats as a go statement (offTableMethods).
// Every other name, including any Go adds later, counts as a lock until it is
// listed here.
var syncNotLocks = map[string]bool{
	"WaitGroup": true,
	"Pool":      true,
}

// offTable is what makes the standard library run a function of Sagüin's on
// a goroutine the library starts. design.md, Execution, State ownership:
// "Sagüin's code runs only on the goroutines in the R1 table"; the table's
// last row allows the goroutine net/http starts for each operations
// connection, and "HTTP/2 is off".
type offTable struct {
	kind string
	why  string
}

// offTableFuncs are package-level functions, by import path and name, that
// run a caller's function on a goroutine outside the table. Each was checked
// in Go's source.
var offTableFuncs = map[string]map[string]offTable{
	// The design forbids both by name (R1, the goroutines).
	"time":    {"AfterFunc": {kindAfterFunc, "the design forbids it (R1)"}},
	"context": {"AfterFunc": {kindAfterFunc, "the design forbids it (R1)"}},
	"runtime": {
		// Finalizers and cleanups run on the runtime's own goroutine.
		"SetFinalizer": {kindHook, "runs the finalizer on the runtime's finalizer goroutine, outside the R1 table"},
		"AddCleanup":   {kindHook, "runs the cleanup on the runtime's cleanup goroutine, outside the R1 table"},
	},
	"iter": {
		// Pull runs the sequence on a coroutine, which the runtime makes as a
		// goroutine of its own (runtime.newcoro).
		"Pull":  {kindHook, "runs the sequence on a coroutine goroutine, outside the R1 table"},
		"Pull2": {kindHook, "runs the sequence on a coroutine goroutine, outside the R1 table"},
	},
	"net/http": {
		// TimeoutHandler serves each request on a goroutine of its own, not
		// the HTTP request goroutine the table lists.
		"TimeoutHandler": {kindHook, "serves the handler on a goroutine of its own, not the table's HTTP request goroutine"},
		// These build a Server with the default protocols, HTTP/2 included,
		// which serves each stream on a goroutine of its own.
		"ListenAndServe":    {kindHTTP2, "serves with a default Server, whose HTTP/2 is not off (R1)"},
		"ListenAndServeTLS": {kindHTTP2, "serves with a default Server, whose HTTP/2 is not off (R1)"},
		"Serve":             {kindHTTP2, "serves with a default Server, whose HTTP/2 is not off (R1)"},
		"ServeTLS":          {kindHTTP2, "serves with a default Server, whose HTTP/2 is not off (R1)"},
	},
}

// offTableMethods are methods that run a caller's function on a goroutine
// outside the table. The scan reads syntax, not types, so any method of the
// name counts, whatever its receiver; a method of Sagüin's own with one of
// these names needs another name.
var offTableMethods = map[string]offTable{
	// sync.WaitGroup.Go (Go 1.25) starts a goroutine running f: a go
	// statement by another name, allowed where a go statement is.
	"Go": {kindGo, "starts a goroutine (sync.WaitGroup.Go)"},
	// (*http.Server).RegisterOnShutdown: Shutdown runs each function with
	// `go f()`.
	"RegisterOnShutdown": {kindHook, "Shutdown runs the function on a goroutine of its own, outside the R1 table"},
}

// offTableFields are struct fields, by import path, type and field, whose
// function the standard library calls on a goroutine it starts. Only fields
// written in a composite literal are seen.
var offTableFields = map[string]map[string]map[string]bool{
	"net": {
		// With both IPv4 and IPv6 addresses, the dial races on goroutines of
		// its own (dialParallel), and Control runs there.
		"Dialer": {"Control": true, "ControlContext": true},
		// Lookups run on a singleflight goroutine, which calls Dial.
		"Resolver": {"Dial": true},
	},
	"net/http": {
		// The Transport dials on a goroutine of its own (startDialConnForLocked).
		"Transport": {"Dial": true, "DialContext": true, "DialTLS": true, "DialTLSContext": true,
			"GetProxyConnectHeader": true, "OnProxyConnectResponse": true},
	},
}

// watched are the packages whose members the scans resolve by the name a file
// gives them. A dot import of one would hide its uses.
var watched = map[string]bool{
	"time": true, "context": true, "sync": true, "runtime": true,
	"iter": true, "net": true, "net/http": true,
}

// inTestSupport says whether rel, a slash-separated path from the module
// root, is one of the test-support packages or lies under one. They are the
// test harness (0001c) and the helpers the tests share (testkit), listed
// once, in harness.TestSupportDirs, for these scans and the fuzz runner's
// gap report alike (audit finding 261015-007a). They are exempt from the
// scans because they are never linked into the broker, so the broker's code
// may not import them; a goroutine one of them starts runs only in a test.
// The prefix rule over the list is the harness's own, held once beside it
// (audit round 261015-03, observation 2).
func inTestSupport(rel string) bool {
	return harness.InTestSupport(rel)
}

// What a finding breaks. The unit tests compare these, not the messages.
const (
	kindGo          = "go statement"
	kindAfterFunc   = "AfterFunc"
	kindLock        = "lock"
	kindAtomic      = "atomic"
	kindTestSupport = "test-support import"
	kindDotImport   = "dot import"
	kindHook        = "off-table goroutine"
	kindHTTP2       = "HTTP/2 not off"
)

// finding is one place in a file that breaks R1 or R3.
type finding struct {
	pos  token.Position
	kind string
	text string
}

func (f finding) String() string { return fmt.Sprintf("%s: %s", f.pos, f.text) }

// scanner holds what the R1 and R3 scans allow, so the unit tests can run
// them on snippets with allowlists of their own.
type scanner struct {
	module  string          // the module's path; the test-support packages live under it
	goFuncs map[string]bool // functions allowed a go statement
	atomics map[string]bool // packages allowed sync/atomic
}

// scanFile returns what breaks R1 and R3 in one file of package pkg, and
// every function starting a goroutine (a go statement or a Go method), allowed
// or not. Imports are resolved by the name the file gives them, so an aliased
// time, context, sync, runtime, iter, net or net/http is still seen. A dot
// import of one of them would hide its uses from the scan, so it is a finding
// itself.
func (s scanner) scanFile(fset *token.FileSet, f *ast.File, pkg string) (found []finding, starts []string) {
	report := func(n ast.Node, kind, format string, args ...any) {
		found = append(found, finding{fset.Position(n.Pos()), kind, fmt.Sprintf(format, args...)})
	}
	// names maps the name each import has in this file to its path, for the
	// watched packages, whose members are checked one by one.
	names := map[string]string{}
	for _, im := range fileImports(f) {
		switch p := im.path; {
		case strings.HasPrefix(p, s.module+"/") && inTestSupport(strings.TrimPrefix(p, s.module+"/")):
			report(im.spec, kindTestSupport, "imports %s: only tests may use the test-support packages, which are never linked into the broker", p)
		case p == "sync/atomic":
			if !s.atomics[pkg] {
				report(im.spec, kindAtomic, "imports sync/atomic in %s: R3 allows atomics only for metric values and reload swaps", pkg)
			}
		case watched[p]:
			if im.name == "." {
				report(im.spec, kindDotImport, "dot-imports %s, which hides its uses from the R1 and R3 scans", p)
			}
			names[im.name] = p
		}
	}

	// member returns the import path and member name of a selector on a
	// watched package, such as http.Server, or "" when it is not one.
	member := func(e ast.Expr) (string, string) {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return "", ""
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok {
			return "", ""
		}
		return names[x.Name], sel.Sel.Name
	}

	// R1: "HTTP/2 is off". A Server is seen to turn it off only in its
	// composite literal (http2Off). Any other use of the type itself, such
	// as new(http.Server), a variable or a field of type http.Server, makes
	// a Server whose literal the scan cannot see; only a pointer to one,
	// *http.Server, passes. checked holds the type expressions of literals
	// and pointers, so the walk below skips them.
	checked := map[ast.Expr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CompositeLit:
			checked[n.Type] = true
		case *ast.StarExpr:
			checked[n.X] = true
		}
		return true
	})

	for _, decl := range f.Decls {
		fn := pkg + " (package level)"
		if d, ok := decl.(*ast.FuncDecl); ok {
			fn = funcID(pkg, d)
		}
		goroutine := func(n ast.Node, what string) {
			starts = append(starts, fn)
			if !s.goFuncs[fn] {
				report(n, kindGo, "%s in %s, which starts no goroutine of the R1 table", what, fn)
			}
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.GoStmt:
				goroutine(n, "go statement")
			case *ast.CompositeLit:
				p, typ := member(n.Type)
				if p == "net/http" && typ == "Server" && !http2Off(n, decl) {
					report(n, kindHTTP2, "http.Server in %s does not turn HTTP/2 off: R1 needs TLSNextProto set to an empty map, or Protocols to HTTP/1 alone", fn)
				}
				for _, elt := range n.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); ok && offTableFields[p][typ][key.Name] {
						report(kv, kindHook, "%s.%s's %s in %s: the library calls it on a goroutine of its own, outside the R1 table", p, typ, key.Name, fn)
					}
				}
			case *ast.SelectorExpr:
				if m, ok := offTableMethods[n.Sel.Name]; ok {
					if m.kind == kindGo {
						goroutine(n, n.Sel.Name+" ("+m.why+")")
					} else {
						report(n, m.kind, "%s in %s: %s", n.Sel.Name, fn, m.why)
					}
				}
				p, name := member(n)
				if p == "net/http" && name == "Server" && !checked[n] {
					report(n, kindHTTP2, "http.Server outside a composite literal in %s: R1 needs HTTP/2 off, set in the Server's literal", fn)
				}
				if h, ok := offTableFuncs[p][name]; ok {
					report(n, h.kind, "%s.%s in %s: %s", p, name, fn, h.why)
				}
				if p == "sync" && !syncNotLocks[name] {
					report(n, kindLock, "sync.%s in %s: R3 allows no lock", name, pkg)
				}
			}
			return true
		})
	}
	return found, starts
}

// http2Off says whether an http.Server literal, in decl, turns HTTP/2 off as
// net/http reads its fields: a Protocols set decides alone, so it must hold
// HTTP/1 alone (http1Only); without one, TLSNextProto must be a non-nil map
// with no "h2" entry, which the scan sees only as an empty map made in the
// literal itself. Protocols: nil is no set; TLSNextProto: nil, or a map the
// scan cannot see into, leaves HTTP/2 on.
func http2Off(lit *ast.CompositeLit, decl ast.Decl) bool {
	if protocols := keyValue(lit, "Protocols"); protocols != nil && !isIdent(protocols, "nil") {
		return http1Only(protocols, decl)
	}
	switch e := keyValue(lit, "TLSNextProto").(type) {
	case *ast.CompositeLit:
		_, isMap := e.Type.(*ast.MapType)
		return isMap && len(e.Elts) == 0
	case *ast.CallExpr:
		if len(e.Args) == 0 || !isIdent(e.Fun, "make") {
			return false
		}
		_, isMap := e.Args[0].(*ast.MapType)
		return isMap
	}
	return false
}

// http1Only says whether e, a Server's Protocols, names a variable (v or &v)
// of decl on which HTTP/1 is set and neither HTTP/2 nor unencrypted HTTP/2
// is: SetHTTP1(true) is called, and SetHTTP2 and SetUnencryptedHTTP2 only
// with false. Anything else may leave HTTP/2 on.
func http1Only(e ast.Expr, decl ast.Decl) bool {
	if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND {
		e = u.X
	}
	v, ok := e.(*ast.Ident)
	if !ok {
		return false
	}
	// doubt is any call that may turn HTTP/1 off or HTTP/2 on.
	http1, doubt := false, false
	ast.Inspect(decl, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isIdent(sel.X, v.Name) || len(call.Args) != 1 {
			return true
		}
		switch sel.Sel.Name {
		case "SetHTTP1":
			if isIdent(call.Args[0], "true") {
				http1 = true
			} else {
				doubt = true
			}
		case "SetHTTP2", "SetUnencryptedHTTP2":
			doubt = doubt || !isIdent(call.Args[0], "false")
		}
		return true
	})
	return http1 && !doubt
}

// keyValue returns the value a composite literal gives the named field, or
// nil when it gives none.
func keyValue(lit *ast.CompositeLit, key string) ast.Expr {
	for _, elt := range lit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok && isIdent(kv.Key, key) {
			return kv.Value
		}
	}
	return nil
}

// isIdent says whether e is the identifier name.
func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// funcID names a function as goAllowed does: pkg.F, pkg.T.M or pkg.(*T).M,
// without a generic receiver's type parameters. A receiver it cannot read
// is named "?", which no allowlist entry matches.
func funcID(pkg string, d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return pkg + "." + d.Name.Name
	}
	recv, pointer := recvType(d)
	if recv == "" {
		recv = "?"
	}
	if pointer {
		recv = "(*" + recv + ")"
	}
	return pkg + "." + recv + "." + d.Name.Name
}

// skipDir says whether the walk leaves a directory out by its name: vendored
// code, test data and dot directories are not the broker's code. A nested
// module is left out by what it holds (nestedModule).
func skipDir(name string) bool {
	return name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".")
}

// brokerFile says whether a file, by its slash-separated path from the module
// root, is the broker's code: a Go file that is not a test, not in the
// test-support packages and not in a directory the walk leaves out.
func brokerFile(rel string) bool {
	if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
		return false
	}
	if inTestSupport(path.Dir(rel)) {
		return false
	}
	for dir := range strings.SplitSeq(path.Dir(rel), "/") {
		if dir != "." && skipDir(dir) {
			return false
		}
	}
	return true
}

// TestBrokerCodeKeepsR1AndR3 applies the scans to every file of the broker's
// code, then checks that each allowlist entry still names something in the
// tree, so a stale entry cannot let a later function or package through.
func TestBrokerCodeKeepsR1AndR3(t *testing.T) {
	fset, module, files := brokerCode(t)
	s := scanner{module: module, goFuncs: goAllowed, atomics: atomicAllowed}
	starts := map[string]bool{}
	atomics := map[string]bool{} // packages a file of which imports sync/atomic
	for _, pf := range files {
		if slices.ContainsFunc(pf.f.Imports, func(spec *ast.ImportSpec) bool { return spec.Path.Value == strconv.Quote("sync/atomic") }) {
			atomics[pf.pkg] = true
		}
		found, fns := s.scanFile(fset, pf.f, pf.pkg)
		for _, v := range found {
			t.Error(v)
		}
		for _, fn := range fns {
			starts[fn] = true
		}
	}

	for fn := range goAllowed {
		if !starts[fn] {
			t.Errorf("goAllowed lists %s, which starts no goroutine in the broker's code", fn)
		}
	}
	// As a goAllowed entry must still start a goroutine, an atomicAllowed
	// entry must still use sync/atomic: a package that stopped needing it
	// must not keep the right to start again unreviewed.
	for pkg := range atomicAllowed {
		if !atomics[pkg] {
			t.Errorf("atomicAllowed lists %s, which imports sync/atomic in none of its files", pkg)
		}
	}
}

// TestTestSupportIsNotBrokerCode: the walk leaves the test-support packages
// out, so a goroutine one of them starts (testkit's ReturnsWithin, a
// harness's) is not the broker's; a directory that only begins like one is
// still scanned.
func TestTestSupportIsNotBrokerCode(t *testing.T) {
	for rel, want := range map[string]bool{
		"internal/testkit/testkit.go":       false,
		"internal/testkit/sub/x.go":         false,
		"internal/harness/client.go":        false,
		"internal/harness/cmd/fuzz/main.go": false,
		"internal/testkitx/x.go":            true,
		"internal/harnessx/x.go":            true,
		"internal/store/store.go":           true,
		"internal/store/store_test.go":      false,
	} {
		if got := brokerFile(rel); got != want {
			t.Errorf("brokerFile(%q) = %v, want %v", rel, got, want)
		}
	}
}

// TestScanRefusesTestSupportImports: broker code importing a test-support
// package is a finding, the harness or testkit alike, and a package whose
// path only begins like one is not.
func TestScanRefusesTestSupportImports(t *testing.T) {
	const module = "example.com/m"
	s := scanner{module: module}
	for imp, want := range map[string]bool{
		module + "/internal/testkit":       true,
		module + "/internal/harness":       true,
		module + "/internal/harness/cmd/x": true,
		module + "/internal/testkitx":      false,
		"testkit":                          false,
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package x\nimport _ "+strconv.Quote(imp)+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		found, _ := s.scanFile(fset, f, module+"/internal/x")
		got := slices.ContainsFunc(found, func(f finding) bool { return f.kind == kindTestSupport })
		if got != want {
			t.Errorf("importing %s: a test-support finding %v, want %v (%v)", imp, got, want, found)
		}
	}
}

// TestBrokerDoesNotLinkTestSupport is the guard on what the build links,
// beyond the scan's reading of import lines: no package of the broker's
// depends, directly or through another, on a test-support package; and
// testkit depends on the standard library alone, so any test package can
// import it without a cycle.
func TestBrokerDoesNotLinkTestSupport(t *testing.T) {
	root := moduleRoot(t)
	module := strings.TrimSpace(run(t, root, time.Minute, "go", "list", "-m"))
	out := run(t, root, 5*time.Minute, "go", "list", "-f", "{{.ImportPath}}{{range .Deps}} {{.}}{{end}}", "./...")
	packages := 0
	for line := range strings.Lines(out) {
		fields := strings.Fields(line)
		if len(fields) == 0 || inTestSupport(strings.TrimPrefix(fields[0], module+"/")) {
			continue
		}
		packages++
		for _, dep := range fields[1:] {
			if rel, ours := strings.CutPrefix(dep, module+"/"); ours && inTestSupport(rel) {
				t.Errorf("%s depends on %s: only tests may use the test-support packages", fields[0], dep)
			}
		}
	}
	if packages == 0 {
		t.Fatal("go list found no broker package")
	}
	deps := run(t, root, time.Minute, "go", "list", "-deps", "-f", "{{.ImportPath}} {{.Standard}}", "./internal/testkit")
	for line := range strings.Lines(deps) {
		dep, standard, _ := strings.Cut(strings.TrimSpace(line), " ")
		if dep != module+"/internal/testkit" && standard != "true" {
			t.Errorf("testkit depends on %s, which is not in the standard library", dep)
		}
	}
}
