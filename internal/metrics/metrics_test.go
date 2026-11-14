package metrics_test

import (
	"bytes"
	"math"
	"runtime"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The API these tests fix:
//
//	func New() *Registry
//	func (r *Registry) Counter(name string, labels ...string) *Counter
//	func (r *Registry) Gauge(name string, labels ...string) *Gauge
//	func (r *Registry) Info(name string, labels ...string)
//	func (r *Registry) WriteTo(w io.Writer) (int64, error)
//	func (c *Counter) Inc()
//	func (c *Counter) Add(n uint64)
//	func (g *Gauge) Set(v int64)
//	func (g *Gauge) Add(d int64)
//	func ReasonLabel(t packet.Type, code byte) string
//
// labels are name, value pairs. Every series is registered at start, before
// any scrape (metrics.md, Rules: "Every series exists from start"); a name
// breaking the naming rules, or a series registered twice, panics, being a
// mistake in the broker's code. WriteTo renders the Prometheus text format,
// the Go runtime series included.

// panics says whether f panics.
func panics(f func()) (p bool) {
	defer func() { p = recover() != nil }()
	f()
	return false
}

// metrics.md, opening: "This document lists every metric Sagüin exposes
// [...] in the Prometheus text format". Rules: "Every name starts with `saguin_`; a counter ends in
// `_total`". Every series a scrape renders, the Go runtime's included, keeps
// both.
func TestScrapeIsPrometheusTextAndKeepsTheNamingRules(t *testing.T) {
	r := metrics.New()
	r.Counter("saguin_test_events_total", "part", "access", "result", "applied")
	r.Gauge("saguin_test_open")
	r.Info("saguin_test_info", "version", "1.2.3")
	e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))
	for name, typ := range e.Types {
		if !strings.HasPrefix(name, "saguin_") {
			t.Errorf("%s does not start with saguin_", name)
		}
		if typ == "counter" && !strings.HasSuffix(name, "_total") {
			t.Errorf("counter %s does not end in _total", name)
		}
	}
}

// metrics.md, Rules: "Every series exists from start, for every value of its
// labels that applies [...] Counters start at 0. The _info series start at 1.
// Other gauges start at their current value."
func TestSeriesExistFromStart(t *testing.T) {
	r := metrics.New()
	r.Counter("saguin_test_reloads_total", "part", "access", "result", "applied")
	r.Counter("saguin_test_reloads_total", "part", "access", "result", "refused")
	r.Counter("saguin_test_plain_total")
	r.Gauge("saguin_test_open_sockets", "listener", "tcp")
	r.Info("saguin_test_build_info", "version", "1.2.3", "broker_id", "edge-1")
	e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))

	for _, c := range []struct {
		typ    string
		value  float64
		name   string
		labels []string
	}{
		{"counter", 0, "saguin_test_reloads_total", []string{"part", "access", "result", "applied"}},
		{"counter", 0, "saguin_test_reloads_total", []string{"part", "access", "result", "refused"}},
		{"counter", 0, "saguin_test_plain_total", nil},
		{"gauge", 0, "saguin_test_open_sockets", []string{"listener", "tcp"}},
		{"gauge", 1, "saguin_test_build_info", []string{"version", "1.2.3", "broker_id", "edge-1"}},
	} {
		if got := e.Types[c.name]; got != c.typ {
			t.Errorf("TYPE of %s: got %q, want %q", c.name, got, c.typ)
		}
		if got := e.Value(t, c.name, c.labels...); got != c.value {
			t.Errorf("%s at start: got %v, want %v", testkit.SeriesKey(c.name, c.labels...), got, c.value)
		}
	}
}

// metrics.md, Rules: "A counter is an atomic changed by the goroutine that
// sees its event. The same applies to a gauge moved where its event happens
// [...] A gauge only the core knows is an atomic the core sets (R3)."
func TestCountersAndGaugesMove(t *testing.T) {
	r := metrics.New()
	c := r.Counter("saguin_test_events_total", "qos", "1")
	moved := r.Gauge("saguin_test_bytes_in_transit_bytes")
	set := r.Gauge("saguin_test_sessions")
	c.Inc()
	c.Add(41)
	moved.Add(10)
	moved.Add(-3)
	set.Set(5)
	set.Set(2)
	e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))
	if got := e.Value(t, "saguin_test_events_total", "qos", "1"); got != 42 {
		t.Errorf("counter: got %v, want 42", got)
	}
	if got := e.Value(t, "saguin_test_bytes_in_transit_bytes"); got != 7 {
		t.Errorf("moved gauge: got %v, want 7", got)
	}
	if got := e.Value(t, "saguin_test_sessions"); got != 2 {
		t.Errorf("set gauge: got %v, want 2", got)
	}
}

// metrics.md, Rules: a counter is "an atomic changed by the goroutine that
// sees its event", and "A scrape returns published values without waiting.
// In particular, it never waits on the core or the store". Many goroutines change values while scrapes run;
// none is lost, and the race detector sees no unsynchronised access.
func TestUpdatesFromManyGoroutinesDuringScrapes(t *testing.T) {
	r := metrics.New()
	c := r.Counter("saguin_test_events_total")
	g := r.Gauge("saguin_test_open_sockets")
	const workers, each = 8, 1000

	stop := make(chan struct{})
	scraped := make(chan error, 1)
	go func() {
		var err error
		for {
			select {
			case <-stop:
				scraped <- err
				return
			default:
			}
			var b bytes.Buffer
			if _, werr := r.WriteTo(&b); werr != nil && err == nil {
				err = werr
			}
		}
	}()

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				c.Inc()
				g.Add(1)
			}
		})
	}
	wg.Wait()
	close(stop)
	if err := <-scraped; err != nil {
		t.Fatalf("a scrape during updates failed: %v", err)
	}

	e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))
	if got := e.Value(t, "saguin_test_events_total"); got != workers*each {
		t.Errorf("counter: got %v, want %d", got, workers*each)
	}
	if got := e.Value(t, "saguin_test_open_sockets"); got != workers*each {
		t.Errorf("gauge: got %v, want %d", got, workers*each)
	}
}

// metrics.md, Rules: "Every name starts with `saguin_`; a counter ends in
// `_total`". A series breaking either is refused when it is registered. So
// is a name the Prometheus text format (metrics.md) cannot carry: a metric
// name of letters, digits, `_` and `:`, never starting with a digit, and a
// label name of letters, digits and `_`, never `:`, never starting with a
// digit.
func TestNamingRulesRefuseBadNames(t *testing.T) {
	for _, c := range []struct {
		what     string
		register func(*metrics.Registry)
		refused  bool
	}{
		{"counter without saguin_", func(r *metrics.Registry) { r.Counter("test_events_total") }, true},
		{"counter without _total", func(r *metrics.Registry) { r.Counter("saguin_test_events") }, true},
		{"gauge without saguin_", func(r *metrics.Registry) { r.Gauge("test_open_sockets") }, true},
		{"_info series without saguin_", func(r *metrics.Registry) { r.Info("test_build_info") }, true},
		{"counter", func(r *metrics.Registry) { r.Counter("saguin_test_events_total") }, false},
		{"gauge", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets") }, false},
		{"_info series", func(r *metrics.Registry) { r.Info("saguin_test_build_info", "version", "1") }, false},
		{"a metric name holding -", func(r *metrics.Registry) { r.Gauge("saguin_test-open_sockets") }, true},
		{"a metric name holding a space", func(r *metrics.Registry) { r.Gauge("saguin_test open_sockets") }, true},
		{"a metric name holding a digit", func(r *metrics.Registry) { r.Gauge("saguin_test2_open_sockets") }, false},
		{"a label name holding :", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets", "a:b", "v") }, true},
		{"a label name holding -", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets", "a-b", "v") }, true},
		{"a label name starting with a digit", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets", "2a", "v") }, true},
		{"an empty label name", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets", "", "v") }, true},
		{"a label name of _, letters and digits", func(r *metrics.Registry) { r.Gauge("saguin_test_open_sockets", "_Ab9", "v") }, false},
	} {
		if got := panics(func() { c.register(metrics.New()) }); got != c.refused {
			t.Errorf("%s: refused %v, want %v", c.what, got, c.refused)
		}
	}
}

// metrics.md, opening: "in the Prometheus text format", where each metric
// name has one TYPE and one group of samples. Series of one name registered
// apart are rendered together; one series registered twice, or one name
// given two types, is refused.
func TestOneNameOneTypeOneGroup(t *testing.T) {
	r := metrics.New()
	r.Counter("saguin_test_events_total", "qos", "0")
	r.Gauge("saguin_test_open_sockets")
	r.Counter("saguin_test_events_total", "qos", "1")
	e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))
	e.Value(t, "saguin_test_events_total", "qos", "0")
	e.Value(t, "saguin_test_events_total", "qos", "1")

	twice := func(r *metrics.Registry) {
		r.Counter("saguin_test_events_total", "qos", "0")
		r.Counter("saguin_test_events_total", "qos", "0")
	}
	if !panics(func() { twice(metrics.New()) }) {
		t.Error("a series registered twice was accepted")
	}
	twoTypes := func(r *metrics.Registry) {
		r.Counter("saguin_test_events_total", "qos", "0")
		r.Gauge("saguin_test_events_total", "qos", "1")
	}
	if !panics(func() { twoTypes(metrics.New()) }) {
		t.Error("one name registered as a counter and a gauge was accepted")
	}
}

// metrics.md, Rules: a bridge rule's "`filter` and `topic` ... are as
// configured", so a label value may hold any character; the Prometheus text
// format writes a backslash, a double quote and a line break in a label value
// as \\, \" and \n.
func TestLabelValuesAreEscaped(t *testing.T) {
	r := metrics.New()
	r.Counter("saguin_test_rule_total", "filter", "a\"b\\c\nd")
	text := testkit.Scrape(t, r, testkit.CountChecked())
	want := `saguin_test_rule_total{filter="a\"b\\c\nd"} 0`
	if !slices.Contains(strings.Split(text, "\n"), want) {
		t.Errorf("the scrape lacks the line %s:\n%s", want, text)
	}
	e := testkit.ParseScrape(t, text)
	e.Value(t, "saguin_test_rule_total", "filter", "a\"b\\c\nd")
}

// goSeries is metrics.md's Go runtime table: each series and its type.
var goSeries = map[string]string{
	"saguin_go_heap_live_bytes":             "gauge",
	"saguin_go_heap_goal_bytes":             "gauge",
	"saguin_go_gc_cycles_total":             "counter",
	"saguin_go_gc_cpu_seconds_total":        "counter",
	"saguin_go_gc_assist_cpu_seconds_total": "counter",
	"saguin_go_stack_bytes":                 "gauge",
	"saguin_go_allocated_bytes_total":       "counter",
	"saguin_go_allocated_objects_total":     "counter",
	"saguin_go_gogc_percent":                "gauge",
	"saguin_go_memory_limit_bytes":          "gauge",
}

// metrics.md, Go runtime: "Each scrape reads these values from
// runtime/metrics", with the table's series and types. After a collection,
// the heap, the stacks, the allocations, the cycles and the memory limit are
// all above zero.
func TestGoRuntimeSeries(t *testing.T) {
	runtime.GC()
	e := testkit.ParseScrape(t, testkit.Scrape(t, metrics.New(), testkit.CountChecked()))
	positive := map[string]bool{
		"saguin_go_heap_live_bytes": true, "saguin_go_heap_goal_bytes": true,
		"saguin_go_gc_cycles_total": true, "saguin_go_stack_bytes": true,
		"saguin_go_allocated_bytes_total": true, "saguin_go_allocated_objects_total": true,
		"saguin_go_memory_limit_bytes": true,
	}
	for name, typ := range goSeries {
		if got := e.Types[name]; got != typ {
			t.Errorf("TYPE of %s: got %q, want %q", name, got, typ)
			continue
		}
		v := e.Value(t, name)
		switch {
		case positive[name] && v <= 0:
			t.Errorf("%s: got %v, want above 0", name, v)
		case name != "saguin_go_gogc_percent" && v < 0:
			t.Errorf("%s: got %v, want 0 or more", name, v)
		}
	}
}

// metrics.md, Go runtime: "Each scrape reads these values";
// `saguin_go_gogc_percent` is "the effective `GOGC`: `/gc/gogc:percent` read
// as a signed 32-bit value, so -1 when off"; `saguin_go_memory_limit_bytes`
// is "the effective `GOMEMLIMIT`". Each scrape shows the runtime's settings
// and counts as they are then, from the same registry.
func TestGoRuntimeReadAtEachScrape(t *testing.T) {
	gogc := debug.SetGCPercent(100)
	t.Cleanup(func() { debug.SetGCPercent(gogc) })
	limit := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(limit) })
	r := metrics.New()

	for _, percent := range []int{137, -1, 100} {
		debug.SetGCPercent(percent)
		if got := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked())).Value(t, "saguin_go_gogc_percent"); got != float64(percent) {
			t.Errorf("saguin_go_gogc_percent with GOGC %d: got %v", percent, got)
		}
	}

	debug.SetMemoryLimit(3 << 30)
	if got := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked())).Value(t, "saguin_go_memory_limit_bytes"); got != 3<<30 {
		t.Errorf("saguin_go_memory_limit_bytes: got %v, want %d", got, 3<<30)
	}
	debug.SetMemoryLimit(limit)
	if got := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked())).Value(t, "saguin_go_memory_limit_bytes"); got != float64(limit) {
		t.Errorf("saguin_go_memory_limit_bytes: got %v, want %d", got, limit)
	}

	before := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked())).Value(t, "saguin_go_gc_cycles_total")
	runtime.GC()
	if after := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked())).Value(t, "saguin_go_gc_cycles_total"); after <= before {
		t.Errorf("saguin_go_gc_cycles_total across a collection: %v, then %v", before, after)
	}
}

// The runtime/metrics names behind metrics.md's Go runtime meanings. Stacks
// are read twice: "memory held for goroutine stacks" is the heap's stack
// memory, and may include the stacks the operating system allocates.
const (
	rtHeapLive  = "/gc/heap/live:bytes"                     // heap the last collection found reachable
	rtHeapGoal  = "/gc/heap/goal:bytes"                     // the heap goal
	rtCycles    = "/gc/cycles/total:gc-cycles"              // collections completed
	rtGCCPU     = "/cpu/classes/gc/total:cpu-seconds"       // CPU spent collecting
	rtAssistCPU = "/cpu/classes/gc/mark/assist:cpu-seconds" // the part of it goroutines spent helping
	rtStacks    = "/memory/classes/heap/stacks:bytes"       // memory held for goroutine stacks
	rtOSStacks  = "/memory/classes/os-stacks:bytes"         // stacks the operating system allocated
	rtAllocB    = "/gc/heap/allocs:bytes"                   // heap allocations, in bytes
	rtAllocObj  = "/gc/heap/allocs:objects"                 // heap allocations, in objects
)

// readRuntime reads the named runtime metrics directly, as numbers.
func readRuntime(t *testing.T, names ...string) map[string]float64 {
	t.Helper()
	samples := make([]rtmetrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	rtmetrics.Read(samples)
	values := map[string]float64{}
	for _, s := range samples {
		switch s.Value.Kind() {
		case rtmetrics.KindUint64:
			values[s.Name] = float64(s.Value.Uint64())
		case rtmetrics.KindFloat64:
			values[s.Name] = s.Value.Float64()
		default:
			t.Fatalf("runtime/metrics has no number for %s", s.Name)
		}
	}
	return values
}

// near says whether a rendered value is a direct read's, allowing a
// microsecond's rounding on CPU seconds.
func near(rendered, read float64) bool {
	return math.Abs(rendered-read) <= 1e-6+1e-9*math.Abs(read)
}

// metrics.md, Go runtime: "Each scrape reads these values from
// runtime/metrics", each series with its meaning: `saguin_go_heap_live_bytes`
// "heap the last collection found reachable", `saguin_go_heap_goal_bytes`
// "heap size by which the collector aims to finish its current or next
// cycle", `saguin_go_gc_cycles_total` "collections completed",
// `saguin_go_gc_cpu_seconds_total` "CPU spent collecting",
// `saguin_go_gc_assist_cpu_seconds_total` "the part of the CPU spent
// collecting that Sagüin's goroutines spent helping", `saguin_go_stack_bytes` "memory held for goroutine stacks",
// `saguin_go_allocated_bytes_total`, `saguin_go_allocated_objects_total`
// "heap allocations". Each series is told apart from the others: a scrape
// runs between two direct reads, a counter lies between them and a gauge
// equals them when they agree, so a series mapped to another's runtime metric
// fails.
func TestGoRuntimeSeriesReadTheirOwnMetrics(t *testing.T) {
	runtime.GC()
	runtime.GC()
	r := metrics.New()
	names := []string{rtHeapLive, rtHeapGoal, rtCycles, rtGCCPU, rtAssistCPU, rtStacks, rtOSStacks, rtAllocB, rtAllocObj}
	for range 100 {
		before := readRuntime(t, names...)
		e := testkit.ParseScrape(t, testkit.Scrape(t, r, testkit.CountChecked()))
		after := readRuntime(t, names...)
		// A gauge that moved between the two reads (a collection, a stack
		// grown or freed) is unknown at the scrape: read again.
		moved := false
		for _, rt := range []string{rtCycles, rtHeapLive, rtHeapGoal, rtStacks, rtOSStacks} {
			moved = moved || before[rt] != after[rt]
		}
		if moved {
			continue
		}

		for series, rt := range map[string]string{
			"saguin_go_heap_live_bytes": rtHeapLive,
			"saguin_go_heap_goal_bytes": rtHeapGoal,
		} {
			if got := e.Value(t, series); got != before[rt] {
				t.Errorf("%s: got %v, want %v (%s)", series, got, before[rt], rt)
			}
		}
		if got, lo, hi := e.Value(t, "saguin_go_stack_bytes"), before[rtStacks], before[rtStacks]+before[rtOSStacks]; got < lo || got > hi {
			t.Errorf("saguin_go_stack_bytes: got %v, want from %v (%s) to %v (with %s)", got, lo, rtStacks, hi, rtOSStacks)
		}
		for series, rt := range map[string]string{
			"saguin_go_gc_cycles_total":             rtCycles,
			"saguin_go_gc_cpu_seconds_total":        rtGCCPU,
			"saguin_go_gc_assist_cpu_seconds_total": rtAssistCPU,
			"saguin_go_allocated_bytes_total":       rtAllocB,
			"saguin_go_allocated_objects_total":     rtAllocObj,
		} {
			got := e.Value(t, series)
			if (got < before[rt] || got > after[rt]) && !near(got, before[rt]) && !near(got, after[rt]) {
				t.Errorf("%s: got %v, want from %v to %v (%s, read before and after)", series, got, before[rt], after[rt], rt)
			}
		}
		if gc, assist := e.Value(t, "saguin_go_gc_cpu_seconds_total"), e.Value(t, "saguin_go_gc_assist_cpu_seconds_total"); assist > gc {
			t.Errorf("saguin_go_gc_assist_cpu_seconds_total %v is above saguin_go_gc_cpu_seconds_total %v, of which it is a part", assist, gc)
		}
		return
	}
	t.Fatal("a runtime gauge moved between every pair of reads")
}

// design.md, Testing: "The busy path is anything run per packet, per message
// or per connection. It does no needless work. [...] avoids every copy or
// allocation it can." Every goroutine counts and moves metrics as it works,
// and names a reason code per acknowledgement: none of it allocates.
// reasonSink keeps ReasonLabel's result in use, so the call cannot be
// dropped.
var reasonSink string

func TestUpdatesDoNotAllocate(t *testing.T) {
	reg := metrics.New()
	counter := reg.Counter("saguin_alloc_test_total", "cause", "test")
	gauge := reg.Gauge("saguin_alloc_test")
	for _, c := range []struct {
		what string
		f    func()
	}{
		{"Counter.Inc", counter.Inc},
		{"Gauge.Add", func() { gauge.Add(-1) }},
		{"ReasonLabel", func() { reasonSink = metrics.ReasonLabel(packet.DISCONNECT, 0x97) }},
	} {
		if n := testing.AllocsPerRun(100, c.f); n != 0 {
			t.Errorf("%s allocates %v times a call; want 0", c.what, n)
		}
	}
}
