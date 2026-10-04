// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// scraped is each saguin_go_ series in one catalogue, by name.
func scraped(t *testing.T, catalogue []byte) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, line := range strings.Split(string(catalogue), "\n") {
		if !strings.HasPrefix(line, "saguin_go_") {
			continue
		}
		name, v, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("a runtime line with no value: %q", line)
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		out[name] = f
	}
	return out
}

// runtimeSources is each series' runtime/metrics key as RFC 0005's
// catalogue names it, read from the rows ("From `key`"), not from
// runtimeSeries, so that a key swapped there is a key this disagrees with.
func runtimeSources(t *testing.T) map[string]string {
	t.Helper()
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs", "0005-operations.md"))
	if err != nil {
		t.Fatalf("read RFC 0005: %v", err)
	}
	row := regexp.MustCompile("(?m)^\\| `(saguin_go_[a-z_]+)` \\|.* From `([^`]+)`\\. \\|$")
	out := map[string]string{}
	for _, m := range row.FindAllStringSubmatch(string(md), -1) {
		out[m[1]] = m[2]
	}
	if len(out) == 0 {
		t.Fatal("RFC 0005 names no saguin_go_ row with its key, so there is nothing to hold the catalogue to")
	}
	return out
}

// readRuntime is every key in sources read now, by series name: GOGC, which
// the runtime writes unsigned, as the signed number it means.
func readRuntime(sources map[string]string) map[string]float64 {
	var names []string
	var samples []rtmetrics.Sample
	for name, key := range sources {
		names = append(names, name)
		samples = append(samples, rtmetrics.Sample{Name: key})
	}
	rtmetrics.Read(samples)
	out := map[string]float64{}
	for i, name := range names {
		switch samples[i].Value.Kind() {
		case rtmetrics.KindUint64:
			u := samples[i].Value.Uint64()
			out[name] = float64(u)
			if name == "saguin_go_gogc_percent" {
				out[name] = float64(int64(u))
			}
		case rtmetrics.KindFloat64:
			out[name] = samples[i].Value.Float64()
		}
	}
	return out
}

// **Each runtime series is its runtime/metrics key, read at the scrape**:
// the key exists in this Go, a cumulative key is a counter and every other
// a gauge, and a scrape's value lies between reads taken just before and
// just after it - a counter anywhere between, a heap figure equal where no
// collection ran between, and GOGC and GOMEMLIMIT exactly what they were
// set to. A series read from the wrong key, or scaled, or not read at
// scrape, lands outside.
func TestEveryRuntimeSeriesIsItsRuntimeMetricsSource(t *testing.T) {
	descs := map[string]rtmetrics.Description{}
	for _, d := range rtmetrics.All() {
		descs[d.Name] = d
	}
	sources := runtimeSources(t)
	if len(runtimeSeries) != len(sources) {
		t.Errorf("the catalogue serves %d runtime series and RFC 0005 names %d", len(runtimeSeries), len(sources))
	}
	for _, s := range runtimeSeries {
		if want := sources[s.name]; s.key != want {
			t.Errorf("%s is read from %s, and RFC 0005 names %q", s.name, s.key, want)
		}
		d, ok := descs[s.key]
		if !ok {
			t.Errorf("%s reads %s, which this Go's runtime/metrics does not have", s.name, s.key)
			continue
		}
		if want := map[bool]string{true: "counter", false: "gauge"}[d.Cumulative]; s.kind != want {
			t.Errorf("%s is a %s, and its key %s is cumulative=%v, which is a %s", s.name, s.kind, s.key, d.Cumulative, want)
		}
		if strings.HasSuffix(s.name, "_total") != (s.kind == "counter") {
			t.Errorf("%s: a counter's name ends in _total and only a counter's (RFC 0005)", s.name)
		}
	}

	runtime.GC() // a live heap to read: before the first collection it is 0
	defer debug.SetGCPercent(debug.SetGCPercent(173))
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(1 << 40))
	b := metricsBroker(t)
	var got, before, after map[string]float64
	for try := 0; ; try++ {
		before = readRuntime(sources)
		got = scraped(t, b.catalogue())
		after = readRuntime(sources)
		if before["saguin_go_gc_cycles_total"] == after["saguin_go_gc_cycles_total"] {
			break // no collection between: the heap figures are fixed
		}
		if try == 20 {
			t.Fatal("a collection ran during every one of 20 scrapes, so the heap figures could not be held to their keys")
		}
	}
	if len(got) != len(runtimeSeries) {
		t.Fatalf("the scrape carries %d runtime series, want %d: %v", len(got), len(runtimeSeries), got)
	}
	for _, s := range runtimeSeries {
		v, lo, hi := got[s.name], before[s.name], after[s.name]
		switch {
		case s.kind == "counter", s.name == "saguin_go_stack_bytes":
			// A counter only grows; the stacks move with every goroutine,
			// so they are held to a band a stack or two wide around the reads.
			slack := 0.0
			if s.name == "saguin_go_stack_bytes" {
				lo, hi, slack = math.Min(lo, hi), math.Max(lo, hi), 64<<10
			}
			if v < lo-slack || v > hi+slack {
				t.Errorf("%s is %v, outside %v..%v read from %s either side of the scrape", s.name, v, lo, hi, s.key)
			}
		default:
			if v != lo || v != hi {
				t.Errorf("%s is %v, and %s read %v before the scrape and %v after", s.name, v, s.key, lo, hi)
			}
		}
	}
	if got["saguin_go_gogc_percent"] != 173 || got["saguin_go_memory_limit_bytes"] != 1<<40 {
		t.Errorf("GOGC %v and GOMEMLIMIT %v, want the 173 and %d this test set",
			got["saguin_go_gogc_percent"], got["saguin_go_memory_limit_bytes"], int64(1<<40))
	}
	debug.SetGCPercent(-1)
	if v := scraped(t, b.catalogue())["saguin_go_gogc_percent"]; v != -1 {
		t.Errorf("with garbage collection off GOGC reads %v, want -1", v)
	}
	debug.SetGCPercent(173)
	if got["saguin_go_heap_live_bytes"] <= 0 || got["saguin_go_heap_goal_bytes"] < got["saguin_go_heap_live_bytes"] {
		t.Errorf("live heap %v and goal %v: the goal is never below what is live", got["saguin_go_heap_live_bytes"],
			got["saguin_go_heap_goal_bytes"])
	}
}

// **The runtime is read only at scrape**: the one runtime/metrics read in
// the tree's production code is renderRuntime's, which only a recomputed
// catalogue reaches, and nothing calls runtime.ReadMemStats, which stops the
// world. A read on a timer or on a client's path would cost the broker
// while nobody looks (RFC 0005 "What a metric is allowed to cost"). Walks
// the syntax tree of every non-test Go file in the repository.
func TestTheRuntimeIsReadOnlyAtScrape(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var scanned, inRender int
	var elsewhere []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly|parser.ParseComments)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		metricsName, runtimeName := "", ""
		for _, im := range f.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			name := filepath.Base(p)
			if im.Name != nil {
				name = im.Name.Name
			}
			switch p {
			case "runtime/metrics":
				metricsName = name
			case "runtime":
				runtimeName = name
			}
		}
		if metricsName == "" && runtimeName == "" {
			return nil
		}
		if f, err = parser.ParseFile(fset, path, nil, 0); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				x, ok := sel.X.(*ast.Ident)
				if !ok {
					return true
				}
				read := metricsName != "" && x.Name == metricsName && sel.Sel.Name == "Read"
				memStats := runtimeName != "" && x.Name == runtimeName && sel.Sel.Name == "ReadMemStats"
				if !read && !memStats {
					return true
				}
				rel, _ := filepath.Rel(root, path)
				if read && fn.Name.Name == "renderRuntime" && rel == filepath.Join("internal", "broker", "metrics.go") {
					inRender++
					return true
				}
				elsewhere = append(elsewhere, rel+":"+strconv.Itoa(fset.Position(sel.Pos()).Line)+" in "+fn.Name.Name)
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d Go files examined; %d runtime/metrics reads in renderRuntime", scanned, inRender)
	if scanned < 50 {
		t.Fatalf("%d files examined: the walk is not reaching the repository", scanned)
	}
	if inRender != 1 {
		t.Fatalf("renderRuntime reads runtime/metrics %d times, want 1: the walk has stopped matching it", inRender)
	}
	if len(elsewhere) > 0 {
		t.Errorf("the runtime is read outside a scrape: %v", elsewhere)
	}
}

// BenchmarkRuntimeRead is what the runtime's figures add to a scrape: one
// runtime/metrics read of the ten keys and their ten lines.
func BenchmarkRuntimeRead(b *testing.B) {
	for b.Loop() {
		var m metricWriter
		renderRuntime(&m)
	}
}
