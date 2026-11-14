// Package metrics holds Sagüin's in-process metric registry.
package metrics

import (
	"bytes"
	"fmt"
	"io"
	runtimemetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindInfo
	kindRuntimeCounter
	kindRuntimeGauge
)

// Registry contains the complete set of metric series exposed by a broker.
// Series are registered during startup, before the registry is scraped.
type Registry struct {
	families []*family
	byName   map[string]*family
	roles    map[string]*Gauge
}

type family struct {
	name         string
	kind         metricKind
	series       []metricSeries
	seriesKeys   map[string]struct{}
	runtimeIndex int
}

type metricSeries struct {
	labels []label
	value  metricValue
}

type label struct {
	name  string
	value string
}

type metricValue interface {
	appendValue([]byte) []byte
}

// Counter is a monotonically increasing metric value.
type Counter struct {
	value atomic.Uint64
}

// Inc adds one to c.
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds n to c.
func (c *Counter) Add(n uint64) {
	c.value.Add(n)
}

// Value reads a counter for operations exposition, never for broker decisions.
func (c *Counter) Value() uint64 { return c.value.Load() }

func (c *Counter) appendValue(dst []byte) []byte {
	return strconv.AppendUint(dst, c.value.Load(), 10)
}

// DurationCounter accumulates whole microseconds in one atomic word.
// Sub-microsecond remainders are discarded per interval; only exposition
// converts the accumulated integer to seconds.
type DurationCounter struct{ value atomic.Uint64 }

// Add counts the whole microseconds of an elapsed interval.
func (c *DurationCounter) Add(d time.Duration) {
	c.value.Add(uint64(d.Microseconds()))
}

func (c *DurationCounter) appendValue(dst []byte) []byte {
	return appendSeconds(dst, float64(c.value.Load())/float64(time.Second/time.Microsecond))
}

func appendSeconds(dst []byte, seconds float64) []byte {
	return strconv.AppendFloat(dst, seconds, 'f', -1, 64)
}

// SecondsGauge publishes a nanosecond value in seconds, retaining fractions.
// It shares Gauge's storage and publication with ordinary integer gauges.
type SecondsGauge struct{ Gauge }

func (g *SecondsGauge) appendValue(dst []byte) []byte {
	return appendSeconds(dst, float64(g.value.Load())/float64(time.Second))
}

// Gauge is a metric value which can move in either direction.
type Gauge struct {
	value atomic.Int64
}

// OptionalGauge omits its sample until Set supplies a value. A nil pointer
// represents absence, so a scrape observes presence and value in one load.
type OptionalGauge struct {
	value atomic.Pointer[int64]
}

// Set publishes v, or removes the sample when v is nil.
func (g *OptionalGauge) Set(v *int64) { g.value.Store(v) }

func (g *OptionalGauge) appendValue(dst []byte) []byte {
	if v := g.value.Load(); v != nil {
		return strconv.AppendInt(dst, *v, 10)
	}
	return dst
}

// Set replaces the value of g.
func (g *Gauge) Set(v int64) {
	g.value.Store(v)
}

// Add changes g by d.
func (g *Gauge) Add(d int64) {
	g.value.Add(d)
}

func (g *Gauge) appendValue(dst []byte) []byte {
	return strconv.AppendInt(dst, g.value.Load(), 10)
}

type runtimeDefinition struct {
	name        string
	runtime     string
	kind        metricKind
	signedInt32 bool
}

var runtimeDefinitions = [...]runtimeDefinition{
	{"saguin_go_heap_live_bytes", "/gc/heap/live:bytes", kindRuntimeGauge, false},
	{"saguin_go_heap_goal_bytes", "/gc/heap/goal:bytes", kindRuntimeGauge, false},
	{"saguin_go_gc_cycles_total", "/gc/cycles/total:gc-cycles", kindRuntimeCounter, false},
	{"saguin_go_gc_cpu_seconds_total", "/cpu/classes/gc/total:cpu-seconds", kindRuntimeCounter, false},
	{"saguin_go_gc_assist_cpu_seconds_total", "/cpu/classes/gc/mark/assist:cpu-seconds", kindRuntimeCounter, false},
	{"saguin_go_stack_bytes", "/memory/classes/heap/stacks:bytes", kindRuntimeGauge, false},
	{"saguin_go_allocated_bytes_total", "/gc/heap/allocs:bytes", kindRuntimeCounter, false},
	{"saguin_go_allocated_objects_total", "/gc/heap/allocs:objects", kindRuntimeCounter, false},
	{"saguin_go_gogc_percent", "/gc/gogc:percent", kindRuntimeGauge, true},
	{"saguin_go_memory_limit_bytes", "/gc/gomemlimit:bytes", kindRuntimeGauge, false},
}

// New returns an empty registry with the Go runtime series already present.
func New() *Registry {
	r := &Registry{byName: make(map[string]*family, len(runtimeDefinitions))}
	for i, definition := range runtimeDefinitions {
		f := &family{
			name:         definition.name,
			kind:         definition.kind,
			runtimeIndex: i,
		}
		r.families = append(r.families, f)
		r.byName[f.name] = f
	}
	r.roles = make(map[string]*Gauge, 10)
	for _, role := range [...]string{"main", "core", "store_writer", "store_reader", "logger", "listener", "reader", "writer", "dialer", "http_request"} {
		r.roles[role] = r.Gauge("saguin_goroutines", "role", role)
	}
	return r
}

// Goroutine returns the live count for an R1 role, registered by New.
// Owners increment on entry and decrement on return; the registry is immutable
// once startup has registered the other series.
func (r *Registry) Goroutine(role string) *Gauge {
	g := r.roles[role]
	if g == nil {
		panic("metrics: unknown goroutine role " + role)
	}
	return g
}

type uptime struct{ started time.Time }

func (u uptime) appendValue(dst []byte) []byte {
	return strconv.AppendFloat(dst, time.Since(u.started).Seconds(), 'f', -1, 64)
}

// Process registers immutable build information and elapsed process time.
func (r *Registry) Process(version, brokerID string, started time.Time) {
	r.Info("saguin_build_info", "version", version, "broker_id", brokerID)
	r.register("saguin_uptime_seconds", kindGauge, nil, uptime{started: started})
}

// Counter registers and returns one counter series.
func (r *Registry) Counter(name string, labels ...string) *Counter {
	validateName(name, kindCounter)
	c := new(Counter)
	r.register(name, kindCounter, labels, c)
	return c
}

// DurationCounter registers a counter expressed in seconds.
func (r *Registry) DurationCounter(name string, labels ...string) *DurationCounter {
	validateName(name, kindCounter)
	c := new(DurationCounter)
	r.register(name, kindCounter, labels, c)
	return c
}

// Gauge registers and returns one gauge series.
func (r *Registry) Gauge(name string, labels ...string) *Gauge {
	validateName(name, kindGauge)
	g := new(Gauge)
	r.register(name, kindGauge, labels, g)
	return g
}

// SecondsGauge registers a gauge expressed in seconds. Set takes nanoseconds.
func (r *Registry) SecondsGauge(name string) *SecondsGauge {
	validateName(name, kindGauge)
	g := new(SecondsGauge)
	r.register(name, kindGauge, nil, g)
	return g
}

// OptionalGauge registers a gauge whose sample is initially absent. Values
// passed to Set must remain immutable after publication.
func (r *Registry) OptionalGauge(name string, labels ...string) *OptionalGauge {
	validateName(name, kindGauge)
	g := new(OptionalGauge)
	r.register(name, kindGauge, labels, g)
	return g
}

// Info registers a gauge series whose value is always one.
func (r *Registry) Info(name string, labels ...string) {
	validateName(name, kindInfo)
	g := new(Gauge)
	g.Set(1)
	r.register(name, kindInfo, labels, g)
}

func (r *Registry) register(name string, kind metricKind, rawLabels []string, value metricValue) {
	labels, key := makeLabels(rawLabels)
	f := r.byName[name]
	if f == nil {
		f = &family{
			name:         name,
			kind:         kind,
			seriesKeys:   make(map[string]struct{}),
			runtimeIndex: -1,
		}
		r.byName[name] = f
		r.families = append(r.families, f)
	} else if f.kind != kind {
		panic(fmt.Sprintf("metrics: %q registered with two types", name))
	}
	if f.runtimeIndex >= 0 {
		panic(fmt.Sprintf("metrics: %q is a Go runtime metric", name))
	}
	if _, exists := f.seriesKeys[key]; exists {
		panic(fmt.Sprintf("metrics: series %q registered twice", name))
	}
	f.seriesKeys[key] = struct{}{}
	f.series = append(f.series, metricSeries{labels: labels, value: value})
}

func validateName(name string, kind metricKind) {
	if !strings.HasPrefix(name, "saguin_") || !validName(name, true) {
		panic(fmt.Sprintf("metrics: invalid metric name %q", name))
	}
	if kind == kindCounter && !strings.HasSuffix(name, "_total") {
		panic(fmt.Sprintf("metrics: counter %q does not end in _total", name))
	}
	if kind == kindInfo && !strings.HasSuffix(name, "_info") {
		panic(fmt.Sprintf("metrics: info metric %q does not end in _info", name))
	}
}

// validName checks Prometheus names; only metric names allow a colon.
func validName(name string, allowColon bool) bool {
	for i, c := range name {
		letter := c == '_' || (allowColon && c == ':') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return name != ""
}

func makeLabels(raw []string) (labels []label, seriesKey string) {
	if len(raw)%2 != 0 {
		panic("metrics: labels must be name and value pairs")
	}
	labels = make([]label, 0, len(raw)/2)
	seen := make(map[string]struct{}, len(raw)/2)
	for i := 0; i < len(raw); i += 2 {
		name := raw[i]
		if !validName(name, false) {
			panic(fmt.Sprintf("metrics: invalid label name %q", name))
		}
		if _, exists := seen[name]; exists {
			panic(fmt.Sprintf("metrics: duplicate label name %q", name))
		}
		seen[name] = struct{}{}
		labels = append(labels, label{name: name, value: raw[i+1]})
	}

	// Prometheus identifies a series by its label set, not label order.
	canonical := append([]label(nil), labels...)
	slices.SortFunc(canonical, func(a, b label) int { return strings.Compare(a.name, b.name) })
	var key strings.Builder
	for _, label := range canonical {
		key.WriteString(strconv.Quote(label.name))
		key.WriteByte('=')
		key.WriteString(strconv.Quote(label.value))
		key.WriteByte(',')
	}
	return labels, key.String()
}

// helpEscaper escapes a HELP text as the Prometheus text format requires:
// a backslash as \\ and a line feed as \n.
var helpEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`)

// WriteTo writes a snapshot in the Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	runtimeSamples := make([]runtimemetrics.Sample, len(runtimeDefinitions))
	for i, definition := range runtimeDefinitions {
		runtimeSamples[i].Name = definition.runtime
	}
	runtimemetrics.Read(runtimeSamples)

	var buf bytes.Buffer
	var scratch [32]byte
	for _, f := range r.families {
		if text, ok := help[f.name]; ok {
			buf.WriteString("# HELP ")
			buf.WriteString(f.name)
			buf.WriteByte(' ')
			_, _ = helpEscaper.WriteString(&buf, text) // a bytes.Buffer write never fails
			buf.WriteByte('\n')
		}
		buf.WriteString("# TYPE ")
		buf.WriteString(f.name)
		buf.WriteByte(' ')
		buf.WriteString(f.prometheusType())
		buf.WriteByte('\n')
		if f.runtimeIndex >= 0 {
			buf.WriteString(f.name)
			buf.WriteByte(' ')
			buf.Write(runtimeValue(buf.AvailableBuffer(), runtimeDefinitions[f.runtimeIndex], runtimeSamples[f.runtimeIndex].Value))
			buf.WriteByte('\n')
			continue
		}
		for _, series := range f.series {
			value := series.value.appendValue(scratch[:0])
			if len(value) == 0 {
				continue
			}
			buf.WriteString(f.name)
			appendLabels(&buf, series.labels)
			buf.WriteByte(' ')
			buf.Write(value)
			buf.WriteByte('\n')
		}
	}
	return io.Copy(w, &buf)
}

func (f *family) prometheusType() string {
	switch f.kind {
	case kindCounter, kindRuntimeCounter:
		return "counter"
	case kindGauge, kindInfo, kindRuntimeGauge:
		return "gauge"
	default:
		panic("metrics: unknown metric type")
	}
}

func appendLabels(buf *bytes.Buffer, labels []label) {
	if len(labels) == 0 {
		return
	}
	buf.WriteByte('{')
	for i, label := range labels {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(label.name)
		buf.WriteString("=\"")
		appendLabelValue(buf, label.value)
		buf.WriteByte('"')
	}
	buf.WriteByte('}')
}

func appendLabelValue(buf *bytes.Buffer, value string) {
	start := 0
	for i := range len(value) {
		var escape string
		switch value[i] {
		case '\\':
			escape = `\\`
		case '"':
			escape = `\"`
		case '\n':
			escape = `\n`
		default:
			continue
		}
		buf.WriteString(value[start:i])
		buf.WriteString(escape)
		start = i + 1
	}
	buf.WriteString(value[start:])
}

func runtimeValue(dst []byte, definition runtimeDefinition, value runtimemetrics.Value) []byte {
	switch value.Kind() {
	case runtimemetrics.KindUint64:
		if definition.signedInt32 {
			// The runtime encodes disabled GOGC as the uint64 form of int32(-1).
			return strconv.AppendInt(dst, int64(int32(value.Uint64())), 10)
		}
		return strconv.AppendUint(dst, value.Uint64(), 10)
	case runtimemetrics.KindFloat64:
		return strconv.AppendFloat(dst, value.Float64(), 'g', -1, 64)
	default:
		panic(fmt.Sprintf("metrics: runtime metric %q has an unsupported value kind", definition.runtime))
	}
}
