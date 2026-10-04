// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package scaletest

// The compare role's `phases` case: the same-host baseline. Each broker holds
// a number of connections through three phases - idle, a steady QoS 1 load,
// and idle again with the connections still held - and every phase is its
// own row: the broker's CPU and RSS from /proc, and for Sagüin what its own
// runtime says about the phase.
//
// **What Sagüin's runtime says is read from its GC trace** (GODEBUG=gctrace=1,
// on stderr into broker.log), so the binary measured is the one released:
// nothing is added to it and nothing is scraped. Each phase reads the lines
// written between its start and its end, by the log's byte offset:
// collections a second, GC CPU and the assists' part of it, the allocation
// rate (the heap at each collection's start less what the one before left
// live), and the live heap and the goroutine stack it scanned as of the
// last collection in or before the phase. **No collection is forced**: the
// heap figures are the most recent collection's, so they separate what is
// live from garbage without a cycle the broker would not have run; an idle
// phase often has none of its own, and then they are the one before it.
// The trace reports whole megabytes, and the stack it reports is what was
// scanned, the part in use, not what the stacks hold.

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// phaseShape is one run of the phases case: connections held, half of
// them publishers and half subscribers, one topic a pair, and the total
// rate offered in the busy phase.
type phaseShape struct{ conns, rate int }

// phaseShapes is PHASE_NS at PHASE_RATE, then PHASE_NS again at
// PER_PAIR msg/s a pair, so that traffic grows with the connections.
func (c cmpCfg) phaseShapes() []phaseShape {
	var out []phaseShape
	for _, n := range c.phaseNs {
		out = append(out, phaseShape{n, c.phaseRate})
	}
	if c.perPair > 0 {
		for _, n := range c.phaseNs {
			out = append(out, phaseShape{n, n / 2 * c.perPair})
		}
	}
	return out
}

// gcPhase is what Sagüin's GC trace said during one phase.
type gcPhase struct {
	cycles             int
	cpuMs, assistMs    float64
	allocMB, allocSecs float64 // between consecutive collections in the phase
	liveMB, stacksMB   float64 // as of the last collection in or before the phase
}

// gctraceLine is one line of GODEBUG=gctrace=1 output, as Go 1.25 writes
// it: "gc 12 @3.456s 2%: 0.02+1.2+0.01 ms clock, 0.1+0.5/2.1/0.8+0.02 ms
// cpu, 4->4->2 MB, 4 MB goal, 0 MB stacks, 0 MB globals, 8 P".
var gctraceLine = regexp.MustCompile(`^gc \d+ @([\d.]+)s \d+%: \S+ ms clock, (\S+) ms cpu, (\d+)->(\d+)->(\d+) MB, \d+ MB goal, (\d+) MB stacks`)

// parseGCTrace reads the phase's lines. A line it cannot read fails the
// case rather than being skipped: a trace in another format would
// otherwise measure nothing and say so nowhere.
func parseGCTrace(text string) (gcPhase, error) {
	g := gcPhase{liveMB: math.NaN(), stacksMB: math.NaN()}
	prevLive, prevAt := math.NaN(), math.NaN()
	for _, l := range strings.Split(text, "\n") {
		if !strings.HasPrefix(l, "gc ") {
			continue
		}
		m := gctraceLine.FindStringSubmatch(l)
		if m == nil {
			return g, fmt.Errorf("a GC trace line in a format this does not read: %q", l)
		}
		at, _ := strconv.ParseFloat(m[1], 64)
		for i, part := range strings.FieldsFunc(m[2], func(r rune) bool { return r == '+' || r == '/' }) {
			v, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return g, fmt.Errorf("the CPU of GC trace line %q: %v", l, err)
			}
			g.cpuMs += v
			if i == 1 { // sweep termination, then assist/background/idle, then mark termination
				g.assistMs += v
			}
		}
		start, _ := strconv.ParseFloat(m[3], 64)
		live, _ := strconv.ParseFloat(m[5], 64)
		stacks, _ := strconv.ParseFloat(m[6], 64)
		if !math.IsNaN(prevLive) {
			g.allocMB += math.Max(0, start-prevLive)
			g.allocSecs += at - prevAt
		}
		prevLive, prevAt = live, at
		g.cycles++
		g.liveMB, g.stacksMB = live, stacks
	}
	return g, nil
}

// logOffset is broker.log's size now: where the next phase's lines begin.
func (c cmpCfg) logOffset(t testing.TB, b *cmpBrokerDef) int64 {
	out, err := c.run(fmt.Sprintf(`wc -c < "%s/%s/broker.log"`, c.dir, b.name))
	if err != nil {
		t.Fatalf("reading %s's log size: %v", b.name, err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		t.Fatalf("%s's log size %q: %v", b.name, out, err)
	}
	return n
}

// gcBetween is the GC trace between two offsets of broker.log. The live
// heap and stacks are the last collection's up to the phase's end: an idle
// phase may hold none of its own, and then they are the one before it.
func (c cmpCfg) gcBetween(t testing.TB, b *cmpBrokerDef, from, to int64) *gcPhase {
	if b.comm != "saguin" {
		return nil
	}
	f := fmt.Sprintf(`"%s/%s/broker.log"`, c.dir, b.name)
	out, err := c.run(fmt.Sprintf(`tail -c +%d %s | head -c %d`, from+1, f, to-from))
	if err != nil {
		t.Fatalf("reading %s's GC trace: %v", b.name, err)
	}
	g, err := parseGCTrace(out)
	if err != nil {
		t.Fatalf("%s: %v", b.name, err)
	}
	last, err := c.run(fmt.Sprintf(`head -c %d %s | grep '^gc ' | tail -1`, to, f))
	if err != nil && strings.TrimSpace(last) != "" {
		t.Fatalf("reading %s's GC trace: %v", b.name, err)
	}
	if l, err := parseGCTrace(last); err != nil {
		t.Fatalf("%s: %v", b.name, err)
	} else {
		g.liveMB, g.stacksMB = l.liveMB, l.stacksMB
	}
	return &g
}

// phases runs one shape against b: connect, idle, busy, idle again.
func (c cmpCfg) phases(t testing.TB, b *cmpBrokerDef, sh phaseShape) []cmpRow {
	t.Helper()
	pairs := sh.conns / 2
	var arrived atomic.Int64
	var win atomic.Pointer[stepStats]
	t0 := time.Now()
	subs := c.connectAll(t, pairs, func(i int) string { return fmt.Sprintf("h2h-psub-%d", i) }, true, c.lost,
		func(int) func(paho.PublishReceived) {
			return func(pr paho.PublishReceived) {
				_, due, sent, ok := unstamp(pr.Packet.Payload)
				if !ok {
					return
				}
				arrived.Add(1)
				if s := win.Load(); s != nil && !due.Before(s.from) && due.Before(s.to) {
					s.take(due, sent, time.Now())
				}
			}
		})
	defer disconnectAll(c.lost, subs)
	subscribeAll(t, subs, c.phaseTopic, 1)
	pubs := c.connectAll(t, pairs, func(i int) string { return fmt.Sprintf("h2h-ppub-%d", i) }, true, c.lost, nil)
	defer disconnectAll(c.lost, pubs)
	up := time.Since(t0)
	name := fmt.Sprintf("%dc %d/s", sh.conns, sh.rate)

	idle := func(label string) cmpRow {
		o0, from := c.logOffset(t, b), time.Now()
		time.Sleep(c.idle)
		to, o1 := time.Now(), c.logOffset(t, b)
		return cmpRow{label: name + " " + label, from: from, to: to, pass: c.lost.n.Load() == 0,
			conns: sh.conns, gc: c.gcBetween(t, b, o0, o1),
			extra: fmt.Sprintf("connected=%d connect_and_subscribe=%s lost=%d", sh.conns, up.Round(time.Millisecond), c.lost.n.Load())}
	}
	rows := []cmpRow{idle("idle")}

	// Busy: open loop, each message due at its place in the schedule, at
	// most 16 publishes in flight a publisher, as the rate case.
	start := time.Now().Add(100 * time.Millisecond)
	end := start.Add(c.warm + c.window)
	st := &stepStats{from: start.Add(c.warm), to: end}
	win.Store(st)
	var o0 atomic.Int64
	o0.Store(-1)
	warmed := time.AfterFunc(time.Until(st.from), func() { o0.Store(c.logOffset(t, b)) })
	defer warmed.Stop()
	interval := time.Duration(float64(time.Second) * float64(pairs) / float64(sh.rate))
	var offered, sendErrs atomic.Int64
	var ackMu sync.Mutex
	var puback latHist
	var wg sync.WaitGroup
	for i, cl := range pubs {
		phase := time.Duration(int64(interval) * int64(i) / int64(pairs))
		due := make(chan time.Time)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(due)
			for d := start.Add(phase); d.Before(end); d = d.Add(interval) {
				time.Sleep(time.Until(d))
				if !d.Before(st.from) {
					offered.Add(1)
				}
				due <- d
			}
		}()
		var seq atomic.Uint64
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var mine latHist
				for d := range due {
					sent := time.Now()
					_, err := cl.Publish(context.Background(), &paho.Publish{
						Topic: c.phaseTopic(i), QoS: 1, Payload: stamp(seq.Add(1), d, c.size)})
					if err != nil {
						sendErrs.Add(1)
					} else if !d.Before(st.from) {
						mine.add(time.Since(sent))
					}
				}
				ackMu.Lock()
				puback.merge(&mine)
				ackMu.Unlock()
			}()
		}
	}
	wg.Wait()
	o1 := c.logOffset(t, b)
	for wait := time.Now().Add(time.Second); time.Now().Before(wait); time.Sleep(10 * time.Millisecond) {
		st.mu.Lock()
		all := st.delivered >= offered.Load()
		st.mu.Unlock()
		if all {
			break
		}
	}
	if o0.Load() < 0 {
		t.Fatalf("%s: the busy window's start was not marked in the log", name)
	}
	st.mu.Lock()
	busy := cmpRow{label: name + " busy", from: st.from, to: st.to, rate: sh.rate, conns: sh.conns,
		offered: offered.Load(), delivered: st.delivered,
		e2eP50: st.e2e.quantile(0.50), e2eP99: st.e2e.quantile(0.99),
		p50: st.lat.quantile(0.50), p99: st.lat.quantile(0.99),
		pubackP50: puback.quantile(0.50), pubackP99: puback.quantile(0.99),
		gc:    c.gcBetween(t, b, o0.Load(), o1),
		extra: fmt.Sprintf("send_errors=%d lost=%d", sendErrs.Load(), c.lost.n.Load())}
	st.mu.Unlock()
	busy.pass = busy.passes(c.lost.n.Load())
	rows = append(rows, busy)

	// Idle again, once what was sent has arrived, with every connection held.
	settled := settle(arrived.Load, c.quiet, 30*time.Second)
	after := idle("idle after")
	after.extra += fmt.Sprintf(" settled_in=%s", settled.Round(time.Millisecond))
	return append(rows, after)
}

// phasesSummary is the phases case's own table: one row per measure, one
// column per configuration, median (min-max) over the runs; then each
// phase's RSS slope per connection across PHASE_NS at PHASE_RATE.
func (c cmpCfg) phasesSummary(rows []cmpRow) string {
	type key struct{ measure, config string }
	vals := map[key][]float64{}
	var order []string
	add := func(m, cf string, v float64) {
		if !contains(order, m) {
			order = append(order, m)
		}
		vals[key{m, cf}] = append(vals[key{m, cf}], v)
	}
	type slopeKey struct{ config, phase string }
	pts := map[slopeKey]map[int][]float64{}
	for _, r := range rows {
		if r.kase != "phases" || r.caseFailed {
			continue
		}
		sh := strings.Fields(r.label)
		shape, phase := sh[0]+" "+sh[1], strings.Join(sh[2:], " ")
		p := shape + " " + phase + ": "
		add(p+"RSS as held, MB", r.config, r.proc.rssMedMB)
		if phase == "busy" {
			add(p+"delivered %", r.config, 100*float64(r.delivered)/math.Max(1, float64(r.offered)))
			add(p+"CPU µs per message", r.config, r.proc.cpuCores*1e6/r.deliveredRate())
			add(p+"PUBACK p50 ms", r.config, float64(r.pubackP50)/1e6)
			add(p+"PUBACK p99 ms", r.config, float64(r.pubackP99)/1e6)
			add(p+"end to end p50 ms (from send)", r.config, float64(r.p50)/1e6)
			add(p+"end to end p99 ms (from send)", r.config, float64(r.p99)/1e6)
		} else {
			add(p+"CPU, cores", r.config, r.proc.cpuCores)
		}
		if g := r.gc; g != nil {
			secs := r.to.Sub(r.from).Seconds()
			add(p+"GC cycles a second", r.config, float64(g.cycles)/secs)
			// Only under load: the trace counts a stop-the-world pause as
			// every P's time, which beside an idle broker's few ticks of
			// CPU reads as more than all of it.
			if phase == "busy" {
				add(p+"GC CPU, % of the broker's", r.config, 100*g.cpuMs/1e3/math.Max(1e-9, r.proc.cpuCores*secs))
				add(p+"GC assists, % of the broker's", r.config, 100*g.assistMs/1e3/math.Max(1e-9, r.proc.cpuCores*secs))
				add(p+"allocation MB/s", r.config, g.allocMB/math.Max(1e-9, g.allocSecs)*boolNaN(g.allocSecs > 0))
			}
			add(p+"live heap MB (last collection)", r.config, g.liveMB)
			add(p+"stack scanned MB (last collection)", r.config, g.stacksMB)
		}
		if sh[1] == fmt.Sprintf("%d/s", c.phaseRate) {
			k := slopeKey{r.config, phase}
			if pts[k] == nil {
				pts[k] = map[int][]float64{}
			}
			pts[k][r.conns] = append(pts[k][r.conns], r.proc.rssMedMB)
		}
	}
	var b strings.Builder
	b.WriteString("| measure | " + strings.Join(c.configs, " | ") + " |\n|---|" + strings.Repeat("---|", len(c.configs)) + "\n")
	for _, m := range order {
		format := "%.1f"
		if strings.Contains(m, " ms") || strings.Contains(m, "cores") {
			format = "%.2f"
		}
		b.WriteString("| " + m + " |")
		for _, cf := range c.configs {
			b.WriteString(" " + spread(vals[key{m, cf}], format) + " |")
		}
		b.WriteString("\n")
	}
	b.WriteString(fmt.Sprintf("\nRSS slope per connection, KB, least squares over the median RSS at each of %v connections at %d msg/s:\n\n", c.phaseNs, c.phaseRate))
	for _, cf := range c.configs {
		for _, ph := range []string{"idle", "busy", "idle after"} {
			byN := pts[slopeKey{cf, ph}]
			var xs, ys []float64
			for _, n := range c.phaseNs {
				if v := byN[n]; len(v) > 0 {
					xs = append(xs, float64(n))
					ys = append(ys, median(v))
				}
			}
			if len(xs) >= 2 {
				fmt.Fprintf(&b, "- %s, %s: %.1f KB\n", cf, ph, slope(xs, ys)*1024)
			}
		}
	}
	return b.String()
}

// boolNaN is 1, or NaN where ok is false.
func boolNaN(ok bool) float64 {
	if ok {
		return 1
	}
	return math.NaN()
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

// slope is the least-squares slope of ys over xs.
func slope(xs, ys []float64) float64 {
	var mx, my float64
	for i := range xs {
		mx += xs[i]
		my += ys[i]
	}
	mx /= float64(len(xs))
	my /= float64(len(ys))
	var num, den float64
	for i := range xs {
		num += (xs[i] - mx) * (ys[i] - my)
		den += (xs[i] - mx) * (xs[i] - mx)
	}
	return num / den
}

// **The GC trace is read as the runtime writes it**: three lines Go 1.25
// wrote, a log line between them, and a line in another format refused
// rather than skipped. This runs without a broker, in every test run.
func TestTheGCTraceIsReadAsTheRuntimeWritesIt(t *testing.T) {
	trace := `gc 1 @0.000s 7%: 0.024+0.12+0.035 ms clock, 0.38+0.023/0.060/0+0.57 ms cpu, 3->4->4 MB, 4 MB goal, 0 MB stacks, 0 MB globals, 16 P
time=2026-09-28T20:10:45Z level=INFO msg="a log line between"
gc 2 @0.001s 4%: 0.013+0.071+0.006 ms clock, 0.21+0.022/0.053/0.006+0.10 ms cpu, 8->9->9 MB, 8 MB goal, 0 MB stacks, 0 MB globals, 16 P
gc 3 @0.003s 4%: 0.013+0.091+0.012 ms clock, 0.20+0.022/0.063/0+0.19 ms cpu, 18->19->10 MB, 20 MB goal, 2 MB stacks, 0 MB globals, 16 P
`
	g, err := parseGCTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	near := func(what string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("%s: %v, want %v", what, got, want)
		}
	}
	if g.cycles != 3 {
		t.Errorf("cycles: %d, want 3", g.cycles)
	}
	near("GC CPU ms", g.cpuMs, 0.38+0.023+0.060+0+0.57+0.21+0.022+0.053+0.006+0.10+0.20+0.022+0.063+0+0.19)
	near("assist ms", g.assistMs, 0.023+0.022+0.022)
	near("allocated MB", g.allocMB, (8-4)+(18-9))
	near("allocation seconds", g.allocSecs, 0.003)
	near("live MB", g.liveMB, 10)
	near("stacks MB", g.stacksMB, 2)
	if _, err := parseGCTrace("gc 4 @0.004s 4%: something else entirely\n"); err == nil {
		t.Error("a GC trace line in another format was read without complaint")
	}
	if g, err := parseGCTrace("no collections here\n"); err != nil || g.cycles != 0 || !math.IsNaN(g.liveMB) {
		t.Errorf("a phase with no collection: %+v, %v; want no cycles and no live heap", g, err)
	}
}
