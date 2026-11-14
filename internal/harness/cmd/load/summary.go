package main

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// perSecond is n over the measured window, NaN where n is unknown (-1).
func perSecond(n int64) float64 {
	if n < 0 {
		return math.NaN()
	}
	return float64(n) / measuredWindow.Seconds()
}

// ms is d in milliseconds.
func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// mbPerSecond is n bytes over the measured window, in MB (10^6 bytes) a
// second.
func mbPerSecond(n int64) float64 { return perSecond(n) / 1e6 }

// mib is n bytes in MiB.
func mib(n uint64) float64 { return float64(n) / (1 << 20) }

// quantileMS is quantile q of h in milliseconds, NaN where h is empty, and
// whether it falls among the overflow.
func quantileMS(h *histogram, q float64) (float64, bool) {
	v := h.quantile(q)
	if v.none {
		return math.NaN(), false
	}
	return ms(v.value), v.over
}

// latencyMS is quantile q of h, formatted: "n/a" where h is empty, "over"
// before the histogram's top where it falls among the overflow.
func latencyMS(h *histogram, q float64) string {
	v, over := quantileMS(h, q)
	if over {
		return fmt.Sprintf("over %.0f", v)
	}
	return num("%.2f", v)
}

// num formats v, "n/a" where it is NaN.
func num(format string, v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf(format, v)
}

// String is one run's account in the results.
func (o *outcome) String() string {
	var b strings.Builder
	for q := range 2 {
		fmt.Fprintf(&b, "  QoS %d publications/s: attempted %.0f, written %.0f", q, perSecond(o.attempted[q]), perSecond(o.written[q]))
		if q == 1 {
			fmt.Fprintf(&b, ", acknowledged %.0f", perSecond(o.acked[q]))
		}
		fmt.Fprintf(&b, "; surviving records with intake in the window %s/s; %d sent in the run\n", num("%.0f", perSecond(o.recordsIn[q])), o.sent[q])
	}
	fmt.Fprintf(&b, "  QoS 1 at the cutoff: %d unacknowledged; %d acknowledged after it; send to PUBACK p50 %s ms, p99 %s ms\n",
		o.unacknowledged, o.ackedLate, latencyMS(&o.ackLatency, 0.5), latencyMS(&o.ackLatency, 0.99))
	fmt.Fprintf(&b, "  publication writes cut at the run's end: %d (unsent at the stop, never counted as sent or lost)\n", o.cut)
	if o.rate > 0 {
		fmt.Fprintf(&b, "  fixed rate: target %.1f/s; of %d slots due in the window, %d sent (%.2f%%), %d missed and skipped, %d unsent at the stop\n",
			o.rate, o.scheduled, o.scheduled-o.missed-o.unsent, 100*o.scheduledShare(), o.missed, o.unsent)
		fmt.Fprintf(&b, "    delay from due to send: p50 %s ms, p90 %s ms, p99 %s ms, p99.9 %s ms, max %s ms\n",
			latencyMS(&o.delay, 0.5), latencyMS(&o.delay, 0.9), latencyMS(&o.delay, 0.99), latencyMS(&o.delay, 0.999), latencyMS(&o.delay, 1))
	}
	fmt.Fprintf(&b, "  deliveries received in the window: %.0f/s; backlog %d at the window's start, %d at its end\n", perSecond(o.arrived), o.backlog[0], o.backlog[1])
	fmt.Fprintf(&b, "  payload bytes in the window: published %.2f MB/s, delivered %.2f MB/s\n", mbPerSecond(o.writtenBytes), mbPerSecond(o.arrivedBytes))
	label := "complete cohort"
	if o.cohortReceived < o.cohortOwed {
		label = "INCOMPLETE cohort: percentiles of the received part only"
	}
	fmt.Fprintf(&b, "  latency of the window's publications, %.2f%% of %d deliveries received by the cutoff, the worst subscriber %.2f%% (%s):\n",
		100*o.cohortCoverage(), o.cohortOwed, 100*o.worstCohort, label)
	fmt.Fprintf(&b, "    send to receive p50 %s ms, p99 %s ms; due to receive p50 %s ms, p99 %s ms\n",
		latencyMS(&o.sendLatency, 0.5), latencyMS(&o.sendLatency, 0.99), latencyMS(&o.dueLatency, 0.5), latencyMS(&o.dueLatency, 0.99))
	for q := range 2 {
		fmt.Fprintf(&b, "  QoS %d deliveries owed %d, wire: received %d, received late %d, gap %d, unresolved tail %d\n",
			q, o.owed[q], o.received[q], o.receivedLate[q], o.gap[q], o.tail[q])
		fmt.Fprintf(&b, "    gaps: too_large %d, expired %d, retention %d; gap, cause unknown %d\n",
			o.gapTooLarge[q], o.gapExpired[q], o.gapRetention[q], o.gapUnknown[q])
		if o.target != "mosquitto" && o.target != "peer" {
			fmt.Fprintf(&b, "    database findings beside the wire: pending %d, below the floor %d, absent below the floor %d, never stored %d, unstored tail %d, skipped %d, unknown %d\n",
				o.pending[q], o.belowFloor[q], o.absentBelow[q], o.neverStored[q], o.notTaken[q], o.skipped[q], o.unknown[q])
			fmt.Fprintf(&b, "  QoS %d publications never stored: unstored tail %d after the publisher's last stored one (admission unproven, cause unknown), %d before a later stored one\n",
				q, o.unstoredTail[q], o.unstoredGap[q])
		}
	}
	fmt.Fprintf(&b, "  coverage %.2f%%, observed %v after the window; duplicates %d, out of order %d",
		100*o.coverage(), o.observed.Round(time.Millisecond), o.duplicates, o.reordered)
	fmt.Fprintf(&b, "; MQTT resends with DUP %d (counted apart)", o.resends)
	if o.target == "saguin" {
		fmt.Fprintf(&b, ", QoS 1 acknowledged not stored %d; $broadcast floor %d, tail %d", o.ackedUnstored, o.floor, o.next)
	}
	b.WriteString("\n")
	r := &o.resources
	if o.target != "peer" {
		fmt.Fprintf(&b, "  broker: CPU %.1fs in the window; peak RSS %.0f MiB over its life through the observation; memory stall %s%% of the window; cgroup %s\n",
			r.brokerCPU.Seconds(), mib(o.peakRSS), num("%.1f", 100*r.brokerMemStall), o.brokerMemory)
	}
	fmt.Fprintf(&b, "  load tool: CPU %.1fs (GC %.1fs, %d cycles), scheduling p99 %v in the window; peak RSS %.0f MiB; memory stall %s%%; cgroup %s\n",
		r.clientCPU.Seconds(), r.gcCPU.Seconds(), r.gcCycles, r.sched99, mib(o.clientRSS), num("%.1f", 100*r.toolMemStall), o.toolMemory)
	fmt.Fprintf(&b, "  host: CPUs %.0f%% busy; some task stalled on I/O %s%%, on CPU %s%%, on memory %s%% of the window\n",
		100*r.hostBusy, num("%.1f", 100*r.ioStall), num("%.1f", 100*r.cpuStall), num("%.1f", 100*r.memStall))
	return b.String()
}

// A column is one measure of the results table: its heading, its format,
// and how it is read from a run, NaN where unavailable.
type column struct {
	heading string
	format  string
	value   func(*outcome) float64
}

// columns are the results table's measures.
var columns = []column{
	{"QoS0 written/s", "%.0f", func(o *outcome) float64 { return perSecond(o.written[0]) }},
	{"QoS1 acked/s", "%.0f", func(o *outcome) float64 { return perSecond(o.acked[1]) }},
	{"records in window/s", "%.0f", func(o *outcome) float64 { return perSecond(o.recordsIn[0]) + perSecond(o.recordsIn[1]) }},
	{"deliveries/s", "%.0f", func(o *outcome) float64 { return perSecond(o.arrived) }},
	{"published MB/s", "%.2f", func(o *outcome) float64 { return mbPerSecond(o.writtenBytes) }},
	{"delivered MB/s", "%.2f", func(o *outcome) float64 { return mbPerSecond(o.arrivedBytes) }},
	{"coverage %", "%.1f", func(o *outcome) float64 { return 100 * o.coverage() }},
	{"cohort %", "%.1f", func(o *outcome) float64 { return 100 * o.cohortCoverage() }},
	{"send p50 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.sendLatency, 0.5); return v }},
	{"send p99 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.sendLatency, 0.99); return v }},
	{"due p99 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.dueLatency, 0.99); return v }},
	{"sent of scheduled %", "%.2f", func(o *outcome) float64 { return nanIf(o.rate == 0, 100*o.scheduledShare()) }},
	{"send delay p99 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.delay, 0.99); return v }},
	{"broker CPU s", "%.1f", func(o *outcome) float64 { return nanIf(o.target == "peer", o.resources.brokerCPU.Seconds()) }},
	{"broker RSS MiB", "%.0f", func(o *outcome) float64 { return nanIf(o.target == "peer", mib(o.peakRSS)) }},
	{"tool CPU s", "%.1f", func(o *outcome) float64 { return o.resources.clientCPU.Seconds() }},
	{"host busy %", "%.0f", func(o *outcome) float64 { return 100 * o.resources.hostBusy }},
}

// nanIf is NaN where unknown, else v.
func nanIf(unknown bool, v float64) float64 {
	if unknown {
		return math.NaN()
	}
	return v
}

// summarize writes the results table: one row per case, each measure as
// its median over the case's runs, then its spread, lowest to highest.
// Latencies of a case with an incomplete cohort are marked *.
func summarize(w io.Writer, all []runResult) {
	t := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	harness.Say(t, "size\tmode\truns")
	for _, c := range columns {
		harness.Say(t, "\t%s", c.heading)
	}
	harness.Say(t, "\n")
	for _, sz := range sizes {
		for _, m := range []mode{maximum, fixed} {
			var runs []*outcome
			incomplete := false
			for i := range all {
				if all[i].size.name == sz.name && all[i].mode == m {
					runs = append(runs, all[i].outcome)
					incomplete = incomplete || all[i].cohortReceived < all[i].cohortOwed
				}
			}
			if len(runs) == 0 {
				continue
			}
			harness.Say(t, "%s\t%s\t%d", sz.name, m, len(runs))
			for _, c := range columns {
				cell := spread(c, runs)
				if incomplete && (strings.HasPrefix(c.heading, "send p") || strings.HasPrefix(c.heading, "due p")) && cell != "n/a" {
					cell += "*"
				}
				harness.Say(t, "\t%s", cell)
			}
			harness.Say(t, "\n")
		}
	}
	_ = t.Flush() // a report the terminal cannot take has nowhere else to go (harness.Say)
	harness.Say(w, "median (lowest–highest) over the runs; * an incomplete cohort: percentiles of its received part only;\n")
	harness.Say(w, "records in window: surviving records whose intake time is in the window, read after the stop;\n")
	harness.Say(w, "broker RSS is its peak over its life through the observation, CPU the measured window's\n")
}

// spread is column c over runs: the median and the lowest and highest,
// "n/a" where no run has a value.
func spread(c column, runs []*outcome) string {
	var values []float64
	for _, o := range runs {
		if v := c.value(o); !math.IsNaN(v) {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return "n/a"
	}
	slices.Sort(values)
	return fmt.Sprintf(c.format+" ("+c.format+"–"+c.format+")", median(values), values[0], values[len(values)-1])
}

// median is the median of sorted, which is not empty: its middle value, or
// the mean of its two middle ones.
func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
