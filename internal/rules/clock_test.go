package rules

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
)

// design.md, Testing: "The core's decisions are a function of its state, one
// event and that event's time. They return what to do. The loop around the
// core carries that out and owns the one real timer. The core never reads the
// clock, and a test fails if it does. An inside test therefore drives it with
// scripted events and fake times." The core's code, the decisions and the
// loop alike, is package internal/core: the loop takes the time from the
// clock its caller hands it, as a connection's reader and writer take theirs,
// so a test hands it a fake one.

// timeRule is a guard's list of the functions of package time that one
// package must not use, and what a use of one breaks.
type timeRule struct {
	names []string // the functions of package time
	kind  string   // a finding's kind
	why   string   // the rule a use breaks
}

// coreClock is the core's rule: the functions of package time that read
// the clock, or wait on it, which needs it read: each reads the real time,
// so a test with fake times cannot drive the code that calls one.
var coreClock = timeRule{
	names: []string{"Now", "Since", "Until", "Sleep", "After", "Tick", "NewTimer", "NewTicker", "AfterFunc"},
	kind:  "clock read",
	why:   "the core never reads the clock (design.md, Testing)",
}

// storeTimers is the store's rule: the functions of package time that make
// a timer, or wait on one. design.md, Execution, State ownership: the core
// owns "every timer except the following: - A connection owns its deadlines
// for connect_timeout, the PROXY header, keep-alive, the write timeout, the
// publish-rate wait and the operations listener's waits. - Each MQTT
// listener owns its accept-rate wait, max_connect_rate. - Main owns its
// wait for the logger at a graceful stop", and the goroutine table gives the store writer "its one connection
// to the database" and each store reader "one read-only connection": the
// collection deadline of design.md, Store, Writes, is the core's (R1). The
// store's one clock read, at its start (sessions_test.go: "Start reads the
// clock once"), makes no timer.
var storeTimers = timeRule{
	names: []string{"NewTimer", "NewTicker", "After", "Tick", "AfterFunc"},
	kind:  "store timer",
	why:   "the store holds no timer: every timer but those R1 names is the core's (design.md, R1)",
}

// scan returns each use in one file of a function r names, called or taken
// as a value, by whatever name the file gives package time. A dot import of
// time, which would hide them, is the R1 scan's finding (watched).
func (r timeRule) scan(fset *token.FileSet, f *ast.File) []finding {
	names := importNames(f)
	var found []finding
	ast.Inspect(f, func(n ast.Node) bool {
		e, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		if p, name, ok := strings.Cut(selectorOf(e, names), "."); ok && p == "time" && slices.Contains(r.names, name) {
			found = append(found, finding{fset.Position(n.Pos()), r.kind, "time." + name + ": " + r.why})
		}
		return true
	})
	return found
}

// guard applies r's scan to every file of pkg, below the module's path, and
// its packages. pkg must hold code: a guard over no code would pass for
// nothing.
func (r timeRule) guard(t *testing.T, pkg string) {
	t.Helper()
	fset, module, files := brokerCode(t)
	pkg = module + "/" + pkg
	scanned := 0
	for _, pf := range files {
		if pf.pkg != pkg && !strings.HasPrefix(pf.pkg, pkg+"/") {
			continue
		}
		scanned++
		for _, f := range r.scan(fset, pf.f) {
			t.Error(f)
		}
	}
	if scanned == 0 {
		t.Fatalf("found no file of %s to scan", pkg)
	}
}

// TestCoreNeverReadsTheClock applies coreClock to every file of the core's
// package, internal/core.
func TestCoreNeverReadsTheClock(t *testing.T) {
	coreClock.guard(t, "internal/core")
}

// TestStoreHoldsNoTimer applies storeTimers to every file of the store's
// package, internal/store.
func TestStoreHoldsNoTimer(t *testing.T) {
	storeTimers.guard(t, "internal/store")
}

// TestClockScan: the scan finds a clock read through any name the file
// gives package time, called or taken as a value, and passes time's types
// and values that read nothing, and another package's Now.
func TestClockScan(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		want int
	}{
		{"time.Now called", `import "time"; var x = time.Now()`, 1},
		{"time.Now taken as a value", `import "time"; var now = time.Now`, 1},
		{"time aliased", `import clock "time"; var d = clock.Since(clock.Time{})`, 1},
		{"a timer", `import "time"; var tm = time.NewTimer(time.Second)`, 1},
		{"each of several", `import "time"; func f() { time.Sleep(1); <-time.After(1); _ = time.Until(time.Time{}) }`, 3},
		{"types and constants", `import "time"; var d time.Duration = time.Second; var at time.Time`, 0},
		{"a time handed in", `import "time"; func f(now func() time.Time, at time.Time) time.Duration { return now().Sub(at) }`, 0},
		{"another package's Now", `import clock "example.com/clock"; var x = clock.Now()`, 0},
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package x\n"+tc.src+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := coreClock.scan(fset, f); len(got) != tc.want {
			t.Errorf("%s: %d findings %v, want %d", tc.name, len(got), got, tc.want)
		}
	}
}
