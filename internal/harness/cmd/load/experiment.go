package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The experiments are diagnostics outside the recipe (-experiment), on the
// small size's clients, to tell publishing's cost from delivery's:
//
//   - publish-only: the small size's publishers, maximum mode, no
//     subscriber; then again with only its QoS 1 publishers publishing.
//     publish-mixed is its first run alone.
//   - drain-only: the small size's durable subscribers subscribe and leave,
//     a backlog of drainBacklog publications is published and settled,
//     then the subscribers come back with nothing publishing and drain
//     it; then again with the subscribers connected throughout, the drain
//     measured from the end of publishing.
const (
	// drainBacklog is the publications of the drain's backlog, over all
	// publishers, in the recipe's QoS mix and topics.
	drainBacklog = 200_000
	// drainCommit is how long the backlog is left, after its last PUBACK,
	// for its last QoS 0 publications to be stored.
	drainCommit = 2 * time.Second
	// drainLongest bounds the wait for the drain to complete; drainQuiet
	// ends it early once nothing has arrived for that long.
	drainLongest = 2 * time.Minute
	drainQuiet   = 5 * time.Second
)

// experiments names the experiments.
var experiments = []string{"publish-only", "publish-mixed", "drain-only", "reconnect"}

// effectiveReconnectInterval supplies the diagnostic's pacing when its
// optional flag is unset. Direct callers can still request every == 0.
func effectiveReconnectInterval() time.Duration {
	if reconnectInterval == 0 {
		return time.Second
	}
	return reconnectInterval
}

// reconnects receives throughout alternating takeovers and returns, first
// a takeover, until stop closes; the last connection then receives until
// the run's shutdown, as receive does.
func (s *subscriber) reconnects(addr string, r *run, closing *atomic.Bool, stop <-chan struct{}, every time.Duration) error {
	return s.receiveConnections(r, closing, &reconnectPlan{addr: addr, stop: stop, every: every})
}

// reconnectSection pools each kind's CONNACK delays across subscribers.
func reconnectSection(subs []*subscriber) section {
	sec := section{heading: "Reconnect CONNACK delays", columns: []string{
		"Kind", "Reconnects", "p50 ms", "p90 ms", "p99 ms", "Max ms"},
		notes: []string{"CONNECT through CONNACK, excluding the TCP dial and the old connection's departure; takeovers and returns counted separately."}}
	for kind, name := range []string{"takeover", "return"} {
		var h histogram
		for _, s := range subs {
			h.merge(&s.connack[kind])
		}
		sec.rows = append(sec.rows, []string{name, strconv.FormatUint(h.total, 10),
			latencyMS(&h, 0.5), latencyMS(&h, 0.9), latencyMS(&h, 0.99), latencyMS(&h, 1)})
	}
	return sec
}

// runExperiment runs the named experiment against o's target, each run
// reported as it ends.
func runExperiment(out io.Writer, o *options, name string, f *fileReport) error {
	small := sizes[0]
	switch name {
	case "reconnect":
		return measureAll(out, o, f)
	case "publish-only", "publish-mixed":
		var all []runResult
		defer func() {
			if len(all) > 0 {
				f.setup = append(f.setup, brokerSetup(o.target, all[0].config))
				f.sections = append(f.sections, section{heading: "Publish only, no subscriber", columns: []string{
					"Case", "QoS 0 written/s", "QoS 1 acked/s", "QoS 1 PUBACK p50 / p99 ms", "Records in window/s", "Broker CPU s", "Broker peak MiB"}, rows: publishRows(all),
					notes: []string{fmt.Sprintf("Records in window: records surviving in the store whose intake time is in the %v window.", measuredWindow), saturationNote}})
				f.sections = append(f.sections, secondSections(all)...)
			}
		}()
		cases := []size{
			{name: "small publishers, no subscriber", publishers: small.publishers},
			{name: "small's QoS 1 publishers only, no subscriber", publishers: small.publishers, qos1Only: true},
		}
		if name == "publish-mixed" {
			cases = cases[:1]
		}
		for _, sz := range cases {
			res, err := measure(o.binary, sz, 0, o.target, false)
			if res != nil {
				all = append(all, runResult{sz, maximum, res})
				harness.Say(out, "\n%s, maximum mode:\n%s", sz.name, res)
			}
			if err != nil {
				return err
			}
		}
		return nil
	case "drain-only":
		s := section{heading: fmt.Sprintf("Drain only: a backlog of %d publications, then the subscribers drain it", drainBacklog),
			columns: []string{"Subscribers", "QoS 0: all received after (deliveries/s)", "QoS 1: all received after (deliveries/s)",
				fmt.Sprintf("Received in the %v window: QoS 0 / QoS 1", measuredWindow), "Broker CPU s (window)", "Written to storage MiB (window)"},
			notes: []string{"Deliveries/s are from the drain's start to each QoS's last delivery. Written to storage is /proc/<pid>/io's write_bytes; no fsync count."}}
		var config string
		defer func() {
			f.setup = append(f.setup, brokerSetup(o.target, config))
			f.sections = append(f.sections, s)
		}()
		for _, connected := range []bool{false, true} {
			row, err := drainOnly(out, o, small, connected, &config)
			if row != nil {
				s.rows = append(s.rows, row)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("no experiment %q: %s", name, strings.Join(experiments, ", "))
}

// drainOnly runs the drain experiment once: the subscribers away while the
// backlog is published, or connected throughout.
func drainOnly(out io.Writer, o *options, sz size, connected bool, config *string) (row []string, err error) {
	t, err := startTarget(o.binary, o.target)
	if err != nil {
		return nil, err
	}
	if t.broker != nil {
		if *config, err = t.resolved(); err != nil {
			return nil, errors.Join(err, t.stop())
		}
	}
	defer func() {
		err = errors.Join(err, t.stop())
		if err == nil && t.dir != "" {
			_ = os.RemoveAll(t.dir) // the store is the run's alone
		} else if t.dir != "" {
			err = fmt.Errorf("%w (the broker's files are kept in %s)", err, t.dir)
		}
	}()
	r := newRun(sz, 0)
	r.observation.Store(int64(2*drainLongest + drainCommit))
	var closing atomic.Bool
	var readers sync.WaitGroup
	var failed atomic.Value
	fail := func(e error) {
		if e != nil {
			failed.CompareAndSwap(nil, e)
		}
	}
	subs, pubs, err := setUp(t.addr, r, sz)
	if err != nil {
		return nil, err
	}
	r.epoch = time.Now()
	if connected {
		for _, s := range subs {
			readers.Go(func() { fail(s.receive(r, &closing)) })
		}
	} else if err := leave(subs, nil); err != nil {
		return nil, err
	}
	took, err := backlog(r, pubs, fail)
	if err != nil {
		return nil, err
	}
	if !connected {
		if err := comeBack(t.addr, subs); err != nil {
			return nil, err
		}
		for _, s := range subs {
			readers.Go(func() { fail(s.receive(r, &closing)) })
		}
	}
	report, row, err := drainWindow(t.pid(), subs, pubs)
	r.endObservation()
	closing.Store(true)
	for _, c := range clients(subs, nil) {
		_ = c.Send(&harness.Disconnect{}) // a client already gone shows in its reader
	}
	readers.Wait()
	if e, _ := failed.Load().(error); e != nil {
		err = errors.Join(err, e)
	}
	how, short := "away while the backlog was published, then reconnected", "away, then reconnected"
	if connected {
		how, short = "connected throughout; the drain measured from the end of publishing", "connected throughout"
	}
	harness.Say(out, "\ndrain-only, %s, subscribers %s:\n  backlog: %d publications (QoS 0 %d, QoS 1 %d) published and settled in %v, then %v left to commit\n%s",
		o.target, how, sent(pubs, 0)+sent(pubs, 1), sent(pubs, 0), sent(pubs, 1), took.Round(time.Millisecond), drainCommit, report)
	if row != nil {
		row = append([]string{short}, row...)
	}
	return row, err
}

// sent is how many publications the publishers at qos sent.
func sent(pubs []*publisher, qos byte) int64 {
	var n int64
	for _, p := range pubs {
		if p.qos == qos {
			n += int64(p.sent.Load())
		}
	}
	return n
}

// leave disconnects the subscribers, their sessions kept (Session Expiry
// 3600), each reading to its connection's end. A drain callback continues
// recording deliveries on a connection whose reader is already running.
func leave(subs []*subscriber, drain func(*subscriber) error) error {
	for _, s := range subs {
		if err := s.c.Send(&harness.Disconnect{}); err != nil {
			return err
		}
		var err error
		if drain == nil {
			err = s.c.AwaitClose()
		} else {
			err = drain(s)
		}
		if err != nil {
			return fmt.Errorf("subscriber %d leaving: %w", s.index, err)
		}
		_ = s.c.Close() // closed by the broker already
	}
	return nil
}

// comeBack reconnects each subscriber to its session (Clean Start 0),
// which must be present, and subscribes nothing new.
func comeBack(addr string, subs []*subscriber) error {
	for _, s := range subs {
		c, ack, _, err := connectSubscriber(addr, s.index)
		if err != nil {
			return err
		}
		if !ack.SessionPresent {
			_ = c.Close()
			return fmt.Errorf("subscriber %d: its session is not present", s.index)
		}
		s.c = c
	}
	return nil
}

// backlog publishes drainBacklog publications, the publishers sharing them
// evenly, as fast as the target takes them, waits for every PUBACK, gives
// the target drainCommit, and disconnects the publishers.
func backlog(r *run, pubs []*publisher, fail func(error)) (time.Duration, error) {
	start := time.Now()
	var writers, acks sync.WaitGroup
	stop := make(chan struct{}) // never closed: each publisher stops at its limit
	var pubClosing atomic.Bool
	cutoff := start.Add(drainLongest)
	for _, p := range pubs {
		p.limit = drainBacklog / uint64(len(pubs))
		writers.Go(func() { fail(p.publish(r, stop, cutoff)) })
		if p.qos == 1 {
			acks.Go(func() { fail(p.acks(r, &pubClosing)) })
		}
	}
	writers.Wait()
	took := time.Since(start)
	for _, p := range pubs {
		if n := p.unacknowledged(); n > 0 {
			return took, fmt.Errorf("publisher %d: %d publications unacknowledged after its backlog", p.index, n)
		}
	}
	time.Sleep(drainCommit)
	pubClosing.Store(true)
	for _, p := range pubs {
		_ = p.c.Send(&harness.Disconnect{})
		if p.qos == 0 {
			_ = p.c.Close() // no reader: nothing more comes to a QoS 0 publisher
		}
	}
	acks.Wait()
	return took, nil
}

// drainWindow measures the drain from now: deliveries by QoS over the
// recipe's measured window, the target's CPU and I/O counters over it,
// then the time each QoS completes, up to drainLongest, or until nothing
// has arrived for drainQuiet.
func drainWindow(pid int, subs []*subscriber, pubs []*publisher) (text string, row []string, err error) {
	var want [2]int64
	for _, s := range subs {
		for k := range groupSize {
			p := pubs[s.group*groupSize+k]
			want[p.qos] += int64(p.sent.Load())
		}
	}
	got := func() (n [2]int64) {
		for _, s := range subs {
			n[0] += s.byQoS[0].Load()
			n[1] += s.byQoS[1].Load()
		}
		return n
	}
	start := time.Now()
	before := got()
	s0, err := take(pid)
	if err != nil {
		return "", nil, err
	}
	io0 := procIO(pid)
	var after [2]int64
	var s1 sample
	var io1 map[string]int64
	var completed [2]time.Duration // when each QoS had all it is owed
	last, quietSince := before[0]+before[1], time.Now()
	for {
		n := got()
		for q := range 2 {
			if completed[q] == 0 && n[q] >= want[q] {
				completed[q] = time.Since(start)
			}
		}
		if s1.hostTotal == 0 && time.Since(start) >= measuredWindow {
			after = n
			if s1, err = take(pid); err != nil {
				return "", nil, err
			}
			io1 = procIO(pid)
		}
		if n[0]+n[1] != last {
			last, quietSince = n[0]+n[1], time.Now()
		}
		if s1.hostTotal != 0 && ((n[0] >= want[0] && n[1] >= want[1]) || time.Since(start) >= drainLongest || time.Since(quietSince) >= drainQuiet) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	end := got()
	res := s1.since(&s0)
	var b strings.Builder
	fmt.Fprintf(&b, "  in the %v window from the drain's start: QoS 0 %d, QoS 1 %d deliveries (received before it: QoS 0 %d, QoS 1 %d)\n",
		measuredWindow, after[0]-before[0], after[1]-before[1], before[0], before[1])
	for q := range 2 {
		if completed[q] == 0 {
			fmt.Fprintf(&b, "  QoS %d: %d of %d received; NOT complete when the wait ended after %v\n", q, end[q], want[q], time.Since(start).Round(time.Millisecond))
		} else {
			fmt.Fprintf(&b, "  QoS %d: all %d received %v after the drain's start: %.0f deliveries/s from the start\n",
				q, want[q], completed[q].Round(time.Millisecond), float64(want[q]-before[q])/completed[q].Seconds())
		}
	}
	fmt.Fprintf(&b, "  broker: CPU %.1fs in the window; I/O in the window (/proc/<pid>/io: write and read calls count read/write-family calls, not socket sends; no fsync count): %s\n",
		res.brokerCPU.Seconds(), ioDelta(io0, io1))
	fmt.Fprintf(&b, "  load tool: CPU %.1fs in the window; host CPUs %.0f%% busy, I/O stall %s%%\n",
		res.clientCPU.Seconds(), 100*res.hostBusy, num("%.1f", 100*res.ioStall))
	row = make([]string, 0, 5)
	for q := range 2 {
		if completed[q] == 0 {
			row = append(row, fmt.Sprintf("not complete: %d of %d", end[q], want[q]))
		} else {
			row = append(row, fmt.Sprintf("%.1f s (%.0f)", completed[q].Seconds(), float64(want[q]-before[q])/completed[q].Seconds()))
		}
	}
	row = append(row, fmt.Sprintf("%d / %d", after[0]-before[0], after[1]-before[1]), fmt.Sprintf("%.1f", res.brokerCPU.Seconds()),
		fmt.Sprintf("%.1f", mib(uint64(io1["write_bytes"]-io0["write_bytes"]))))
	return b.String(), row, nil
}

// procIO reads process pid's I/O counters, /proc/<pid>/io; nil where they
// cannot be read.
func procIO(pid int) map[string]int64 {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/io")
	if err != nil {
		return nil
	}
	counts := map[string]int64{}
	for line := range strings.Lines(string(data)) {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ": ")
		if n, err := strconv.ParseInt(value, 10, 64); ok && err == nil {
			counts[key] = n
		}
	}
	return counts
}

// ioDelta describes the change in I/O counters from a to b.
func ioDelta(a, b map[string]int64) string {
	if a == nil || b == nil {
		return "unreadable"
	}
	return fmt.Sprintf("write calls %d, read calls %d, bytes written to storage %.1f MiB, read from storage %.1f MiB",
		b["syscw"]-a["syscw"], b["syscr"]-a["syscr"], mib(uint64(b["write_bytes"]-a["write_bytes"])), mib(uint64(b["read_bytes"]-a["read_bytes"])))
}

// publishRows are the publish-only experiment's rows, one per run.
func publishRows(all []runResult) [][]string {
	rows := make([][]string, 0, len(all))
	for i := range all {
		o := all[i].outcome
		rows = append(rows, []string{all[i].size.name, fmt.Sprintf("%.0f", perSecond(o.written[0])), fmt.Sprintf("%.0f", perSecond(o.acked[1])),
			latencyMS(&o.ackLatency, 0.5) + " / " + latencyMS(&o.ackLatency, 0.99),
			num("%.0f", perSecond(o.recordsIn[0])+perSecond(o.recordsIn[1])), fmt.Sprintf("%.1f", o.resources.brokerCPU.Seconds()), fmt.Sprintf("%.0f", mib(o.peakRSS))})
	}
	return rows
}
