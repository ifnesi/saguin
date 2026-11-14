package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync/atomic"
	"time"
)

// second is one measured interval, normally a whole second. Offered deliveries count each
// written publication once per subscriber of its group. Backlog includes
// warm-up publications still owed, just as the window's endpoint counts do.
// Skipped counts the fixed mode's slots due in it and never sent.
type second struct {
	offered, delivered, backlog, skipped int64
	slow                                 bool
	length                               time.Duration // zero in synthetic series means one second
}

// secondCounts partitions the same timestamped events as the run totals.
// Before holds warm-up events; during includes a fractional final interval
// if necessary. Allocation is per client and second, never per message.
type secondCounts struct {
	before atomic.Int64
	during []atomic.Int64
}

func newSecondCounts(clients int, window time.Duration) []secondCounts {
	counts := make([]secondCounts, clients)
	n := int(window / time.Second)
	if window%time.Second != 0 {
		n++
	}
	for i := range counts {
		counts[i].during = make([]atomic.Int64, n)
	}
	return counts
}

func (c *secondCounts) add(at, window time.Duration) {
	if at < warmUp {
		c.before.Add(1)
	} else if at-warmUp < window {
		c.during[(at-warmUp)/time.Second].Add(1)
	}
}

// refreshSeconds runs on the window's owner. A delayed write/receipt can
// update an earlier bucket, so every refresh derives the entire prefix
// anew. The last refresh is after all clients join: the series and totals
// then contain exactly the same events, including both window boundaries.
func (o *outcome) refreshSeconds() {
	if o.run.writes == nil {
		return
	}
	weights := make([]int64, len(o.run.topics))
	for _, s := range o.subs {
		for k := range groupSize {
			weights[s.group*groupSize+k]++
		}
	}
	var backlog int64
	for p := range o.run.writes {
		backlog += weights[p] * o.run.writes[p].before.Load()
	}
	for s := range o.run.receipts {
		backlog -= o.run.receipts[s].before.Load()
	}
	o.backlog[0] = backlog
	for i := range o.seconds {
		s := &o.seconds[i]
		s.offered, s.delivered = 0, 0
		for p := range o.run.writes {
			s.offered += weights[p] * o.run.writes[p].during[i].Load()
		}
		for sub := range o.run.receipts {
			s.delivered += o.run.receipts[sub].during[i].Load()
		}
		s.skipped = 0
		for p := range o.run.skipped {
			s.skipped += o.run.skipped[p].during[i].Load()
		}
		backlog += s.offered - s.delivered
		s.backlog = backlog
		if o.disk != nil {
			s.slow = o.disk.seconds[i].slow
		}
	}
	o.backlog[1] = backlog
}

type keepingUp struct {
	kept, largest, catchUp float64
}

// keepingUp follows recovery from the earliest unresolved phase end.
// Later phases do not restart that wait. An unfinished wait is +Inf;
// without any slow phase the catch-up figure is unavailable (NaN).
func (o *outcome) keepingUp() keepingUp {
	k := keepingUp{kept: math.NaN(), largest: math.NaN(), catchUp: math.NaN()}
	if len(o.seconds) == 0 {
		return k
	}
	whole := len(o.seconds)
	if last := o.seconds[whole-1]; last.length > 0 && last.length < time.Second {
		whole--
	}
	if whole == 0 {
		return k
	}
	var kept, largest, longest int64
	pending := -1
	sawSlow := false
	for i, s := range o.seconds[:whole] {
		// Judged against the schedule, not only what was written: a broker
		// slowing its intake holds its publishers back, so they write and
		// are owed less, and their slots due go unsent.
		up := s.skipped == 0 && s.backlog <= s.offered
		if up {
			kept++
			if pending >= 0 {
				longest = max(longest, int64(i+1-pending))
				pending = -1
			}
		}
		largest = max(largest, s.backlog)
		if s.slow && (i+1 == whole || !o.seconds[i+1].slow) {
			sawSlow = true
			if !up && pending < 0 {
				pending = i + 1
			}
		}
	}
	k.kept = 100 * float64(kept) / float64(whole)
	k.largest = float64(largest)
	if sawSlow {
		k.catchUp = float64(longest)
		if pending >= 0 {
			k.catchUp = math.Inf(1)
		}
	}
	return k
}

func keepingSection(all []runResult) section {
	s := measureSection("Keeping up", all, []column{
		{"Kept up %", "%.1f", func(o *outcome) float64 { return o.keepingUp().kept }},
		{"Largest backlog", "%.0f", func(o *outcome) float64 { return o.keepingUp().largest }},
	}, []string{
		"A whole measured second kept up when every slot due in it was sent and its ending backlog was no more than its offered deliveries. Largest backlog is the highest at a second's end, including any warm-up backlog. A fractional final second is excluded.",
		"Catch-up is the longest whole seconds from a slow phase's end to the first kept-up second, including recovery during another slow phase; zero if already kept up at the phase's end. Not caught up means at least one phase remained unrecovered at the window's end; n/a means no slow phase was observed. Values are median (lowest–highest) over runs; any unrecovered run is reported as not caught up.",
	})
	s.columns = append(s.columns, "Catch-up s")
	for i, c := range resultCases(all) {
		value := func(o *outcome) float64 { return o.keepingUp().catchUp }
		cell := "not caught up"
		if !slices.ContainsFunc(c.runs, func(o *outcome) bool { return math.IsInf(value(o), 1) }) {
			cell = cellOf("%.0f", c.runs, value)
		}
		s.rows[i] = append(s.rows[i], cell)
	}
	return s
}

// secondSections keeps each run's raw series beside the aggregate tables,
// so its rates, backlog and drive phases can be inspected together.
func secondSections(all []runResult) []section {
	var sections []section
	for _, c := range resultCases(all) {
		for i, o := range c.runs {
			if len(o.seconds) == 0 || !o.samplesSeconds() {
				continue
			}
			s := section{heading: fmt.Sprintf("Seconds: %s, run %d", c.name, i+1), columns: []string{
				"Second", "Offered deliveries/s", "Delivered/s", "Backlog"},
				notes: []string{"Intervals since the measured window's start, partitioned by the same write-completion and distinct-receipt timestamps as the run totals. Successful whole writes count when the socket write completes. Backlog is conserved from the window's starting backlog, including warm-up deliveries still owed. Any fractional final interval is included in the totals but excluded from Keeping up."}}
			if o.rate > 0 {
				s.columns = append(s.columns, "Slots unsent")
				s.notes = append(s.notes, "Slots unsent are the fixed schedule's slots due in the interval and never sent, missed or unsent when the publishers stopped.")
			}
			if o.disk != nil {
				s.columns = append(s.columns, "Writes", "Write latency ms", "Drive phase")
				s.notes = append(s.notes, fmt.Sprintf("Disk monitor on %s, slow above %v. Write latency is mean write time from /proc/diskstats; no completed write with I/O in progress is slow, idle is fast. The whole drive includes other processes' I/O.", o.disk.drive, o.disk.slow))
			}
			for j, second := range o.seconds {
				row := []string{strconv.Itoa(j + 1), strconv.FormatInt(second.offered, 10),
					strconv.FormatInt(second.delivered, 10), strconv.FormatInt(second.backlog, 10)}
				if o.rate > 0 {
					row = append(row, strconv.FormatInt(second.skipped, 10))
				}
				if o.disk != nil {
					d := o.disk.seconds[j]
					phase := "fast"
					if d.slow {
						phase = "slow"
					}
					row = append(row, strconv.FormatInt(d.writes, 10), fmt.Sprintf("%.3f", ms(d.latency)), phase)
				}
				s.rows = append(s.rows, row)
			}
			sections = append(sections, s)
		}
	}
	return sections
}

// samplesSeconds preserves the ordinary maximum-mode recipe's output,
// including when reporting a supplied outcome rather than a live run.
func (o *outcome) samplesSeconds() bool {
	return o.rate > 0 || windowRequested || measuredWindow != measured || o.disk != nil || (o.run != nil && o.run.writes != nil)
}
