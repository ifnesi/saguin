package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// A fileReport is the results file: short, in Markdown. A setup table,
// then one table per run set, the essential columns only; the full detail
// of every run stays on the terminal.
type fileReport struct {
	title    string
	setup    [][2]string
	sections []section
}

// A section is one table of the results file, with notes beneath it.
type section struct {
	heading string
	columns []string
	rows    [][]string
	notes   []string
}

// markdown is the file's text.
func (f *fileReport) markdown() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n## Setup\n\n| | |\n|---|---|\n", f.title)
	for _, row := range f.setup {
		fmt.Fprintf(&b, "| %s | %s |\n", row[0], row[1])
	}
	b.WriteString("\nSagüin commits to disk before acknowledging; acknowledged messages survive a crash or power cut. " +
		"Mosquitto keeps its backlog in memory and writes persistence at autosave (default 1800 s) and shutdown; " +
		"an acknowledgement does not guarantee a disk commit. These results compare those different durability designs. " +
		"Every Mosquitto run sets `queue_qos0_messages true`, including the drain diagnostic, so away durable sessions queue QoS 0 too.\n")
	for _, s := range f.sections {
		fmt.Fprintf(&b, "\n## %s\n\n", s.heading)
		if len(s.rows) > 0 {
			fmt.Fprintf(&b, "| %s |\n|%s\n", strings.Join(s.columns, " | "), strings.Repeat("---|", len(s.columns)))
			for _, row := range s.rows {
				fmt.Fprintf(&b, "| %s |\n", strings.Join(row, " | "))
			}
			b.WriteString("\n")
		}
		for _, n := range s.notes {
			fmt.Fprintf(&b, "%s\n", n)
		}
	}
	return []byte(b.String())
}

// brokerSetup is the setup row naming the broker and its key
// configuration, from a run's recorded configuration: for Sagüin, the
// storage mode it resolved first.
func brokerSetup(target, config string) [2]string {
	switch target {
	case "peer":
		return [2]string{"Target", "the in-process test peer (no broker): the load tool's headroom"}
	case "mosquitto":
		return [2]string{"Mosquitto", mosquittoVersion() + " (sha256 " + sumOr(mosquittoBinary)[:12] + "…), persistence on with default autosave, `max_inflight_messages 20`, " +
			"`max_queued_messages 0`, `max_queued_bytes 0`, `queue_qos0_messages true`, `set_tcp_nodelay true`"}
	}
	var keys []string
	for _, key := range []string{"default_retention_period", "default_retention_bytes", "max_inflight_per_client", "max_inflight", "max_incoming_memory", "max_outgoing_memory"} {
		if v, ok := resolvedValue(config, key); ok {
			keys = append(keys, fmt.Sprintf("`%s %s`", key, v))
		}
	}
	text := strings.Join(keys, ", ")
	if mode, ok := resolvedValue(config, "mode"); ok {
		text = mode + "-mode store; " + text
	}
	return [2]string{"Sagüin", text}
}

// resolvedValue is key's value at the first line of config, the broker's
// resolved configuration, that sets it.
func resolvedValue(config, key string) (string, bool) {
	for line := range strings.Lines(config) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+": "); ok {
			return v, true
		}
	}
	return "", false
}

// resultColumns are the results table's columns.
var resultColumns = []string{"Case", "Runs", "QoS 0 written/s", "QoS 1 acked/s", "Deliveries/s", "Published MB/s", "Delivered MB/s", "Coverage %",
	"Latency p50 / p99 ms", "Broker CPU s", "Broker peak MiB", "Gap / dup / reordered"}

// A resultCase groups repetitions of one size, mode and offered rate.
type resultCase struct {
	name string
	runs []*outcome
}

func resultCases(all []runResult) []resultCase {
	var cases []resultCase
	byCase := map[string]int{}
	for _, res := range all {
		name := res.size.name + ", " + res.mode.String()
		if res.rate > 0 {
			name += fmt.Sprintf(" %.1f/s", res.rate)
		}
		i, ok := byCase[name]
		if !ok {
			i = len(cases)
			byCase[name] = i
			cases = append(cases, resultCase{name: name})
		}
		cases[i].runs = append(cases[i].runs, res.outcome)
	}
	return cases
}

// resultRows is one row per case of runs, in the order the cases ran: each
// value the median over its runs, with the lowest and highest where there
// are several.
func resultRows(all []runResult) [][]string {
	cases := resultCases(all)
	rows := make([][]string, 0, len(cases))
	for _, c := range cases {
		runs := c.runs
		cell := func(format string, value func(*outcome) float64) string { return cellOf(format, runs, value) }
		latency := cell("%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.sendLatency, 0.5); return v }) + " / " +
			cell("%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.sendLatency, 0.99); return v })
		if slices.ContainsFunc(runs, func(o *outcome) bool { return o.cohortReceived < o.cohortOwed }) && !strings.HasPrefix(latency, "n/a / n/a") {
			latency += " *"
		}
		rows = append(rows, []string{
			c.name, strconv.Itoa(len(runs)),
			cell("%.0f", func(o *outcome) float64 { return perSecond(o.written[0]) }),
			cell("%.0f", func(o *outcome) float64 { return perSecond(o.acked[1]) }),
			cell("%.0f", func(o *outcome) float64 { return perSecond(o.arrived) }),
			cell("%.2f", func(o *outcome) float64 { return mbPerSecond(o.writtenBytes) }),
			cell("%.2f", func(o *outcome) float64 { return mbPerSecond(o.arrivedBytes) }),
			cell("%.1f", func(o *outcome) float64 { return 100 * o.coverage() }),
			latency,
			cell("%.1f", func(o *outcome) float64 { return nanIf(o.target == "peer", o.resources.brokerCPU.Seconds()) }),
			cell("%.0f", func(o *outcome) float64 { return nanIf(o.target == "peer", mib(o.peakRSS)) }),
			cell("%.0f", func(o *outcome) float64 { return float64(o.lost()) }) + " / " +
				cell("%.0f", func(o *outcome) float64 { return float64(o.duplicates) }) + " / " +
				cell("%.0f", func(o *outcome) float64 { return float64(o.reordered) }),
		})
	}
	return rows
}

// resultNotes explain the results table.
var resultNotes = []string{
	"Deliveries/s counts the distinct deliveries received in it, never a duplicate or an MQTT resend; published and delivered MB/s, the payload bytes written by the publishers and received by the subscribers in it (MB is 10^6 bytes); coverage, the share of all deliveries owed in the run received by the cutoff. " +
		"Latency is from send to receipt, of the publications sent in the window; * marks a case where not all of them were received, the percentiles then of the received part only. " +
		"Broker CPU is the window's; peak memory is resident, over the broker's life through observation. Gap counts a missing delivery before a later receipt from the same publisher, including late receipts. " +
		"The unseen suffix is an unresolved tail, never presumed pending or lost. Sagüin's database evidence is reported separately and never changes the wire categories. Values: median (lowest–highest) over the runs.",
}

// lost counts observed gaps only. An unresolved tail or unknown database
// finding establishes no wire loss and is never added to this count.
func (o *outcome) lost() int64 {
	return o.gap[0] + o.gap[1]
}

const saturationNote = "Maximum mode measures each broker's own saturation behaviour: the actual offered rates and QoS mix can differ as brokers slow their publishers differently. Fixed mode is the comparison of record at the same requested rate."

// resultSections builds every saved table of the recipe's runs, including
// schedule attainment and the evidence behind the loss comparison.
func resultSections(all []runResult) []section {
	if len(all) == 0 {
		return nil
	}
	notes := append([]string{fmt.Sprintf("Rates are over the %v measured window.", measuredWindow)}, resultNotes...)
	if slices.ContainsFunc(all, func(r runResult) bool { return r.samplesSeconds() }) {
		notes = append(notes, "For runs with a per-second series, successful whole writes count at socket-write completion. The same events and timestamps determine the totals and every interval; MQTT resends are reported separately.")
	}
	if slices.ContainsFunc(all, func(r runResult) bool { return r.mode == maximum }) {
		notes = append(notes, saturationNote)
	}
	sections := []section{{heading: "Results", columns: resultColumns, rows: resultRows(all), notes: notes}}
	wire := qosColumns([]qosCount{
		{"Received", func(o *outcome) [2]int64 { return o.received }},
		{"Received late", func(o *outcome) [2]int64 { return o.receivedLate }},
		{"Gap", func(o *outcome) [2]int64 { return o.gap }},
		{"Unresolved tail", func(o *outcome) [2]int64 { return o.tail }},
		{"Gap too_large", func(o *outcome) [2]int64 { return o.gapTooLarge }},
		{"Gap expired", func(o *outcome) [2]int64 { return o.gapExpired }},
		{"Gap retention", func(o *outcome) [2]int64 { return o.gapRetention }},
		{"Gap, cause unknown", func(o *outcome) [2]int64 { return o.gapUnknown }},
	})
	wire = append(wire,
		column{"Writes cut (unsent)", "%.0f", func(o *outcome) float64 { return float64(o.cut) }},
		column{"MQTT resends (DUP)", "%.0f", func(o *outcome) float64 { return float64(o.resends) }})
	sections = append(sections, measureSection("Wire accounting", all, wire, []string{
		"Duplicates and reordering fail every broker. The reconnect experiment counts apart, as MQTT resends, QoS 1 deliveries with DUP that are a publication's first receipt on a connection after a reconnect, the publication sent before that reconnect; any other repeat, with DUP or without, still fails. Unexplained gaps fail QoS 1 for both brokers and QoS 0 for Sagüin; MQTT permits Mosquitto to drop QoS 0. The recipe has no Message Expiry or subscriber Maximum Packet Size limit; only proven retention can explain its gaps.",
	}))
	var fixedRuns, databaseRuns []runResult
	for _, r := range all {
		if r.mode == fixed {
			fixedRuns = append(fixedRuns, r)
		}
		if r.target != "mosquitto" && r.target != "peer" {
			databaseRuns = append(databaseRuns, r)
		}
	}
	if len(fixedRuns) > 0 {
		sections = append(sections, keepingSection(fixedRuns), measureSection("Fixed schedule", fixedRuns, []column{
			{"Scheduled slots", "%.0f", func(o *outcome) float64 { return float64(o.scheduled) }},
			{"Schedule sent %", "%.1f", func(o *outcome) float64 { return 100 * o.scheduledShare() }},
			{"Missed slots", "%.0f", func(o *outcome) float64 { return float64(o.missed) }},
			{"Unsent slots", "%.0f", func(o *outcome) float64 { return float64(o.unsent) }},
			{"Due to send p99 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.delay, 0.99); return v }},
			{"Due to receive p99 ms", "%.2f", func(o *outcome) float64 { v, _ := quantileMS(&o.dueLatency, 0.99); return v }},
			{"Backlog at start", "%.0f", func(o *outcome) float64 { return float64(o.backlog[0]) }},
			{"Backlog at end", "%.0f", func(o *outcome) float64 { return float64(o.backlog[1]) }},
		}, []string{"Slots are due in the measured window. Missed slots are skipped without catch-up; unsent slots were due in the window but not begun when the publishers stopped. Due-to-receive latency covers only receipts by the cutoff."}))
	}
	if len(databaseRuns) > 0 {
		db := qosColumns([]qosCount{
			{"Pending", func(o *outcome) [2]int64 { return o.pending }},
			{"Below floor", func(o *outcome) [2]int64 { return o.belowFloor }},
			{"Absent below floor", func(o *outcome) [2]int64 { return o.absentBelow }},
			{"Never stored", func(o *outcome) [2]int64 { return o.neverStored }},
			{"Unstored tail deliveries", func(o *outcome) [2]int64 { return o.notTaken }},
			{"Skipped", func(o *outcome) [2]int64 { return o.skipped }},
			{"Unknown", func(o *outcome) [2]int64 { return o.unknown }},
			{"Unstored tail publications", func(o *outcome) [2]int64 { return o.unstoredTail }},
			{"Unstored gap publications", func(o *outcome) [2]int64 { return o.unstoredGap }},
		})
		db = append(db, column{"QoS 1 acked unstored", "%.0f", func(o *outcome) float64 { return float64(o.ackedUnstored) }})
		sections = append(sections, measureSection("Sagüin database evidence", databaseRuns, db, []string{
			"Findings describe missing deliveries unless labelled publications; they overlap the wire categories. An unstored tail has unproven admission and unknown cause. An absent record below the floor may have been retained away or never stored. Unknown is never counted as lost; an observed unexplained gap still fails as specified. A QoS 1 acknowledgement without storage fails R4.",
		}))
	}
	return append(sections, secondSections(all)...)
}

type qosCount struct {
	name  string
	value func(*outcome) [2]int64
}

// qosColumns keeps the same QoS presentation for wire and database counts.
func qosColumns(counts []qosCount) []column {
	cols := make([]column, 0, 2*len(counts))
	for _, c := range counts {
		for q := range 2 {
			cols = append(cols, column{fmt.Sprintf("%s QoS %d", c.name, q), "%.0f", func(o *outcome) float64 { return float64(c.value(o)[q]) }})
		}
	}
	return cols
}

func measureSection(heading string, all []runResult, cols []column, notes []string) section {
	s := section{heading: heading, columns: []string{"Case", "Runs"}, notes: notes}
	for _, c := range cols {
		s.columns = append(s.columns, c.heading)
	}
	for _, c := range resultCases(all) {
		row := make([]string, 0, 2+len(cols))
		row = append(row, c.name, strconv.Itoa(len(c.runs)))
		for _, col := range cols {
			row = append(row, cellOf(col.format, c.runs, col.value))
		}
		s.rows = append(s.rows, row)
	}
	return s
}

// cellOf is a value over runs: the median, with the lowest and highest
// where they differ; "n/a" where no run has one.
func cellOf(format string, runs []*outcome, value func(*outcome) float64) string {
	var values []float64
	for _, o := range runs {
		if v := value(o); !math.IsNaN(v) {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return "n/a"
	}
	slices.Sort(values)
	m := fmt.Sprintf(format, median(values))
	lo, hi := fmt.Sprintf(format, values[0]), fmt.Sprintf(format, values[len(values)-1])
	if lo == hi {
		return m
	}
	return m + " (" + lo + "–" + hi + ")"
}
