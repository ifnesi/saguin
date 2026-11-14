package rules

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// netHTTPRead is the one place R1 lets Sagüin's code run on a goroutine
// net/http starts for itself. The design (R1, the goroutines): "On an
// operations connection with proxy_protocol, that connection's goroutine
// reads the PROXY header. Afterwards, the wrapper through which net/http
// reads only passes bytes on. This also applies to the short read net/http
// starts per request to notice a hang-up. The wrapper starts nothing, holds
// no broker state and takes no lock." (audits/261009-001a)
const netHTTPRead = "net/http's reads (the operations PROXY wrapper)"

// connWrappers lists the broker's connection wrappers: every type with a
// Read of its own (io.Reader's shape), whatever it holds, since that code
// runs on whatever goroutine reads through it. Seeing only a net.Conn field
// missed a wrapper holding *net.UnixConn, *tls.Conn or an interface, so any
// Read counts (audits/261009-004a). Each names that goroutine: a row of the
// R1 table Sagüin starts, or netHTTPRead for the one wrapper the design
// lets net/http read through. A type is its package path and name, as
// "github.com/ifnesi/saguin/internal/ops.proxyConn". At most one entry
// is netHTTPRead, and that type is held to the design's sentence, so the
// allowance cannot grow. Each entry has the maintainer's approval: 0013a's
// TLS layer, which the reader reads through (design.md, R1: "Under TLS, a
// connection's reader and writer reach the socket through a thin layer. The
// WebSocket adapter uses the same layer, also without TLS. The layer runs
// in the reader or writer that calls it and passes bytes on unchanged. The
// writer publishes the close's one deadline by closing a notification
// channel. That deadline is immutable after publication. The reader
// observes the notification when its TLS or WebSocket control writes set a
// deadline. Once the close has begun, the layer lowers any later write
// deadline to the close's one deadline").
var connWrappers = map[string]string{
	"github.com/ifnesi/saguin/internal/connection.tlsSocket": "reader",
	// The WebSocket stream: the reader, net/http's goroutine for the
	// connection, reads MQTT's bytes through it, and the writer writes
	// through it (design.md, R1: "A WebSocket connection's reader is the
	// goroutine net/http starts for it"; "MQTT output and the orderly close
	// stay with the writer"); agreed with the maintainer on 261103 (0013d's
	// plan of 261103 070329).
	"github.com/ifnesi/saguin/internal/connection.websocketStream": "reader",
	// 0020e's operations PROXY wrapper, the one netHTTPRead entry (design.md,
	// R1: "On an operations connection with proxy_protocol, that
	// connection's goroutine reads the PROXY header. Afterwards, the wrapper
	// through which net/http reads only passes bytes on"); item 0020e,
	// agreed with the maintainer on 261103.
	"github.com/ifnesi/saguin/internal/operations.proxyConn": netHTTPRead,
}

// What a wrapper finding breaks.
const (
	kindWrapper      = "connection wrapper"
	kindProxyWrapper = "PROXY wrapper"
)

// plainTypes and plainSelectors are the field types the PROXY wrapper may
// hold: the connection (a unix listener's accept gives *net.UnixConn), what it
// reads ahead, the header's values (connection.ProxyHeader, what the shared
// parser returns: a flag, an address and a name), its own deadlines and the
// error its header read ended in, for the log. Any other field could hold
// broker state, which the design's sentence forbids.
var plainTypes = map[string]bool{
	"bool": true, "string": true, "byte": true, "rune": true, "uintptr": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"error": true,
}

var plainSelectors = map[string]bool{
	"net.Conn": true, "net.UnixConn": true, "net.TCPConn": true, "net.Addr": true,
	"io.Reader": true, "bufio.Reader": true, "time.Time": true, "time.Duration": true,
	"github.com/ifnesi/saguin/internal/connection.ProxyHeader": true,
}

// headerCalls are the broker functions the PROXY wrapper may call: the one
// parser of a PROXY v2 header the MQTT and operations listeners share,
// which reads the header's bytes from the reader it is given and returns
// what they say, holding nothing (design.md, R1: "that connection's
// goroutine reads the PROXY header").
var headerCalls = map[string]bool{
	"github.com/ifnesi/saguin/internal/connection.ReadProxyHeader": true,
}

// pkgFile is one parsed file of the broker's code and its package's path.
type pkgFile struct {
	pkg string
	f   *ast.File
}

// fnRef is a function or method with the import names of its file.
type fnRef struct {
	d     *ast.FuncDecl
	names map[string]string
}

// fileImport is one import of a file: its spec, its path, and the name the
// file gives it.
type fileImport struct {
	spec       *ast.ImportSpec
	path, name string
}

// fileImports lists a file's imports in order, each with the name the file
// gives it: its own name where it has one, else its path's last element.
func fileImports(f *ast.File) []fileImport {
	imports := make([]fileImport, 0, len(f.Imports))
	for _, spec := range f.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue // the parser has already refused a malformed literal
		}
		name := path.Base(p)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports = append(imports, fileImport{spec, p, name})
	}
	return imports
}

// importNames maps the name each import has in a file to its path.
func importNames(f *ast.File) map[string]string {
	names := map[string]string{}
	for _, im := range fileImports(f) {
		names[im.name] = im.path
	}
	return names
}

// selectorOf returns "path.Name" for a selector on an imported package, such
// as net.Conn, or "".
func selectorOf(e ast.Expr, names map[string]string) string {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok || names[x.Name] == "" {
		return ""
	}
	return names[x.Name] + "." + sel.Sel.Name
}

// recvType is a method's receiver type name, without type parameters, and
// whether the receiver is a pointer to it. The name is "" for a function,
// and for a receiver it cannot read.
func recvType(d *ast.FuncDecl) (name string, pointer bool) {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return "", false
	}
	t := d.Recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.ParenExpr:
			t = x.X
		case *ast.StarExpr:
			t, pointer = x.X, true
		case *ast.IndexExpr:
			t = x.X
		case *ast.IndexListExpr:
			t = x.X
		case *ast.Ident:
			return x.Name, pointer
		default:
			return "", pointer
		}
	}
}

// fieldTypes lists a field list's types, one per name (one for an unnamed or
// embedded field).
func fieldTypes(l *ast.FieldList) []ast.Expr {
	var out []ast.Expr
	if l == nil {
		return out
	}
	for _, f := range l.List {
		for range max(len(f.Names), 1) {
			out = append(out, f.Type)
		}
	}
	return out
}

// isReadSig says whether a method reads like io.Reader's Read: one []byte in,
// int and error out.
func isReadSig(ft *ast.FuncType) bool {
	in, out := fieldTypes(ft.Params), fieldTypes(ft.Results)
	if len(in) != 1 || len(out) != 2 {
		return false
	}
	return plainName(in[0], "[]byte") && plainName(out[0], "int") && plainName(out[1], "error")
}

// plainName says whether a type expression is the named built-in type, or
// []byte.
func plainName(e ast.Expr, name string) bool {
	if arr, ok := e.(*ast.ArrayType); ok {
		return name == "[]byte" && arr.Len == nil && plainName(arr.Elt, "byte")
	}
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// plainField says whether a field type is one the PROXY wrapper may hold.
func plainField(e ast.Expr, names map[string]string) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return plainTypes[x.Name]
	case *ast.ArrayType:
		return plainName(x, "[]byte")
	case *ast.StarExpr:
		return plainField(x.X, names)
	case *ast.SelectorExpr:
		return plainSelectors[selectorOf(x, names)]
	}
	return false
}

// scanConnWrappers finds the broker's connection wrappers in files (every
// type with a Read of io.Reader's shape), checks
// each against allowed (connWrappers), and holds the netHTTPRead entry to the
// design's sentence: its methods, and the functions of its package they call,
// start no goroutine, use no channel, take no lock and touch no state outside
// the wrapper (no package variable, no call into another broker package); its
// fields are plain. It reads syntax, like the R1 scan.
func scanConnWrappers(fset *token.FileSet, module string, files []pkgFile, allowed map[string]string) []finding {
	var found []finding
	report := func(n ast.Node, kind, format string, args ...any) {
		found = append(found, finding{fset.Position(n.Pos()), kind, fmt.Sprintf(format, args...)})
	}

	type typeDecl struct {
		spec  *ast.TypeSpec
		names map[string]string
	}
	typeDecls := map[string]typeDecl{}     // every type declared, by ID
	readers := map[string]*ast.FuncDecl{}  // types with a Read of their own
	methods := map[string][]fnRef{}        // every method, by type
	funcs := map[string]map[string]fnRef{} // package functions, by package
	vars := map[string]map[string]bool{}   // package variables, by package
	for _, pf := range files {
		names := importNames(pf.f)
		if funcs[pf.pkg] == nil {
			funcs[pf.pkg], vars[pf.pkg] = map[string]fnRef{}, map[string]bool{}
		}
		for _, decl := range pf.f.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						typeDecls[pf.pkg+"."+s.Name.Name] = typeDecl{s, names}
					case *ast.ValueSpec:
						if d.Tok == token.VAR {
							for _, n := range s.Names {
								vars[pf.pkg][n.Name] = true
							}
						}
					}
				}
			case *ast.FuncDecl:
				t, _ := recvType(d)
				if t == "" {
					funcs[pf.pkg][d.Name.Name] = fnRef{d, names}
					continue
				}
				id := pf.pkg + "." + t
				methods[id] = append(methods[id], fnRef{d, names})
				if d.Name.Name == "Read" && isReadSig(d.Type) {
					readers[id] = d
				}
			}
		}
	}

	// Every type with a Read of its own is listed, with a goroutine Sagüin
	// starts or netHTTPRead.
	wrappers := map[string]bool{}
	for id, read := range readers {
		wrappers[id] = true
		if _, ok := allowed[id]; !ok {
			report(read, kindWrapper, "%s has a Read of its own, so that code runs on whatever goroutine reads through it: list it in connWrappers with that goroutine (R1)", id)
		}
	}
	var proxies []string
	for _, id := range slices.Sorted(maps.Keys(allowed)) {
		row := allowed[id]
		if row == netHTTPRead {
			proxies = append(proxies, id)
		} else if !goroutineRows[row] {
			found = append(found, finding{kind: kindWrapper, text: fmt.Sprintf("connWrappers: %s names %q, neither a goroutine of the R1 table Sagüin starts nor netHTTPRead", id, row)})
		}
		if !wrappers[id] {
			found = append(found, finding{kind: kindWrapper, text: fmt.Sprintf("connWrappers lists %s, which is no connection wrapper in the broker's code", id)})
		}
	}
	if len(proxies) > 1 {
		found = append(found, finding{kind: kindWrapper, text: fmt.Sprintf("connWrappers gives netHTTPRead to %d types %v: the design allows one wrapper on net/http's goroutines", len(proxies), proxies)})
	}

	// The one PROXY wrapper keeps the design's sentence.
	for _, id := range proxies {
		td, ok := typeDecls[id]
		if !ok || !wrappers[id] {
			continue
		}
		pkg := id[:strings.LastIndex(id, ".")]
		// A struct of plain fields; any other kind of type (a function, a
		// map, a channel) carries code or state the scan cannot see.
		st, ok := td.spec.Type.(*ast.StructType)
		if !ok {
			report(td.spec, kindProxyWrapper, "%s is a %s, not a struct of plain fields: the PROXY wrapper holds no broker state (R1)", id, types(fset, td.spec.Type))
			continue
		}
		for _, f := range st.Fields.List {
			if !plainField(f.Type, td.names) {
				report(f, kindProxyWrapper, "%s holds a field of type %s: the PROXY wrapper holds no broker state (R1)", id, types(fset, f.Type))
			}
		}
		// Its methods, then each function of its package they call.
		queue := slices.Clone(methods[id])
		seen := map[*ast.FuncDecl]bool{}
		for len(queue) > 0 {
			fn := queue[0]
			queue = queue[1:]
			if seen[fn.d] {
				continue
			}
			seen[fn.d] = true
			where := funcID(pkg, fn.d)
			var visit func(n ast.Node) bool
			visit = func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.GoStmt:
					report(n, kindProxyWrapper, "go statement in %s: the PROXY wrapper starts nothing (R1)", where)
				case *ast.SendStmt, *ast.SelectStmt, *ast.ChanType:
					report(n, kindProxyWrapper, "a channel in %s: the PROXY wrapper only passes bytes on (R1)", where)
				case *ast.UnaryExpr:
					if n.Op == token.ARROW {
						report(n, kindProxyWrapper, "a channel receive in %s: the PROXY wrapper only passes bytes on (R1)", where)
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok {
						if id.Name == "close" {
							report(n, kindProxyWrapper, "close of a channel in %s: the PROXY wrapper only passes bytes on (R1)", where)
						} else if callee, ok := funcs[pkg][id.Name]; ok {
							queue = append(queue, callee)
						}
					}
				case *ast.SelectorExpr:
					if x, ok := n.X.(*ast.Ident); ok {
						if p := fn.names[x.Name]; p != "" {
							switch {
							case p == "sync" || p == "sync/atomic":
								report(n, kindProxyWrapper, "%s.%s in %s: the PROXY wrapper takes no lock (R1)", p, n.Sel.Name, where)
							case headerCalls[p+"."+n.Sel.Name]:
							case p == module || strings.HasPrefix(p, module+"/"):
								report(n, kindProxyWrapper, "calls into %s from %s: the PROXY wrapper holds no broker state (R1)", p, where)
							}
							return false
						}
					}
					ast.Inspect(n.X, visit) // the field or method name is not a variable
					return false
				case *ast.Ident:
					if vars[pkg][n.Name] {
						report(n, kindProxyWrapper, "package variable %s in %s: the PROXY wrapper holds no broker state (R1)", n.Name, where)
					}
				}
				return true
			}
			if fn.d.Body != nil {
				ast.Inspect(fn.d.Body, visit)
			}
		}
	}
	return found
}

// types prints a type expression as written.
func types(fset *token.FileSet, e ast.Expr) string {
	var b strings.Builder
	if err := printer.Fprint(&b, fset, e); err != nil {
		return "?"
	}
	return b.String()
}

// TestBrokerConnWrappers applies the wrapper scan to every file of the
// broker's code with connWrappers.
func TestBrokerConnWrappers(t *testing.T) {
	fset, module, files := brokerCode(t)
	for _, f := range scanConnWrappers(fset, module, files, connWrappers) {
		t.Error(f)
	}
}

// brokerCode parses every file of the broker's code (walkModule): the
// module the binary is built from, around this package, and no module nested
// in it. It returns them with the module's path. Every scan of the broker's
// code reads its files from here.
func brokerCode(t *testing.T) (fset *token.FileSet, module string, files []pkgFile) {
	t.Helper()
	root := moduleRoot(t)
	module = strings.TrimSpace(run(t, root, time.Minute, "go", "list", "-m"))
	fset, files, err := walkModule(root, module)
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("found no broker code to scan")
	}
	return fset, module, files
}

// nestedModule says whether dir, a directory below the module's root, is the
// root of another module: it holds a go.mod file of its own. The Go tool
// leaves such a directory, and everything below it, out of the module around
// it (`go list ./...` names none of its packages), so it is not the broker's
// code, whatever it holds.
func nestedModule(dir string) bool {
	fi, err := os.Stat(filepath.Join(dir, "go.mod"))
	return err == nil && !fi.IsDir()
}

// walkModule parses every file of the broker's code (brokerFile) in the
// module rooted at root, whose path is module, each with its package's path.
// It walks the directories rather than asking `go list` for the packages, so
// a file the build leaves out here, by a build constraint, is still scanned;
// it stops where the Go tool does, at a nested module (nestedModule).
func walkModule(root, module string) (fset *token.FileSet, files []pkgFile, err error) {
	fset = token.NewFileSet()
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && (skipDir(d.Name()) || nestedModule(p)) {
				return filepath.SkipDir
			}
			return nil
		}
		if !brokerFile(rel) {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		files = append(files, pkgFile{path.Join(module, path.Dir(rel)), f})
		return nil
	})
	return fset, files, err
}

// TestWalkStopsAtNestedModule plants a module holding a nested module and
// checks the walk against the Go tool: the nested module's files, at its
// root and below it, are left out, as `go list ./...` leaves out its
// packages, while the module's own files are scanned, one the build leaves
// out by a constraint among them.
func TestWalkStopsAtNestedModule(t *testing.T) {
	// One level below the temporary folder: the Go tool ignores a go.mod
	// at the system temporary root itself.
	root := filepath.Join(t.TempDir(), "m")
	const module = "example.com/outer"
	for rel, text := range map[string]string{
		"go.mod":                 "module " + module + "\n\ngo 1.25.0\n",
		"a.go":                   "package outer\n",
		"sub/b.go":               "package sub\n",
		"tagged/c.go":            "//go:build ignore\n\npackage tagged\n",
		"nested/go.mod":          "module example.com/nested\n\ngo 1.25.0\n",
		"nested/n.go":            "package nested\n",
		"nested/deeper/d.go":     "package deeper\n",
		"sub/inner/go.mod":       "module example.com/inner\n\ngo 1.25.0\n",
		"sub/inner/i.go":         "package inner\n",
		"modfile/go.mod/keep.go": "package keep\n", // a directory named go.mod is no module
	} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	fset, files, err := walkModule(root, module)
	if err != nil {
		t.Fatalf("walking the planted module: %v", err)
	}
	got := make([]string, 0, len(files))
	for _, pf := range files {
		rel, err := filepath.Rel(root, fset.Position(pf.f.Package).Filename)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, pf.pkg+" "+filepath.ToSlash(rel))
	}
	slices.Sort(got)
	want := []string{
		module + " a.go",
		module + "/modfile/go.mod modfile/go.mod/keep.go",
		module + "/sub sub/b.go",
		module + "/tagged tagged/c.go",
	}
	if !slices.Equal(got, want) {
		t.Errorf("the walk scanned\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// The oracle: the Go tool's own view of which packages are the module's.
	listed := strings.Fields(run(t, root, time.Minute, "go", "list", "-f", "{{.ImportPath}}", "./..."))
	slices.Sort(listed)
	if wantListed := []string{module, module + "/modfile/go.mod", module + "/sub"}; !slices.Equal(listed, wantListed) {
		t.Errorf("go list ./... in the planted module named %v, want %v", listed, wantListed)
	}
}
