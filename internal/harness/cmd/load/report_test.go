package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The tests of 0028k's results files and Mosquitto's configuration, on
// Astra's findings 4, 5 and 7 and the maintainer's decisions on them.

// noMosquitto points -mosquitto at a file that does not exist for the
// test, so that describing Mosquitto runs nothing.
func noMosquitto(t *testing.T) {
	was := mosquittoBinary
	mosquittoBinary = filepath.Join(t.TempDir(), "no-mosquitto")
	t.Cleanup(func() { mosquittoBinary = was })
}

// TestDurabilityStated checks the maintainer's test 7 for 0028k: "every
// results file states both durability guarantees", the drain diagnostic's
// included ("The drain result also carries finding 4's durability
// statement (Mosquitto keeps that backlog in memory, Sagüin on disk)").
// Mosquitto writes its persistence file only at autosave and at shutdown
// (outside.go, mosquittoConfig); Sagüin commits before acknowledging
// (design.md, R4: "A reply confirming a change waits for that change's
// commit"; Store: "everything acknowledged survives a crash or a power
// cut"). markdown writes every results file, a failed run's and the
// drain's included, so each file it writes states both.
func TestDurabilityStated(t *testing.T) {
	noMosquitto(t)
	for _, f := range []*fileReport{
		{title: "a failed run", sections: []section{{heading: "Failed", notes: []string{"the run failed"}}}},
		{title: "Sagüin's", setup: [][2]string{brokerSetup("saguin", "")}},
		{title: "Mosquitto's", setup: [][2]string{brokerSetup("mosquitto", "")}},
	} {
		text := strings.ToLower(string(f.markdown()))
		for _, fact := range []string{"autosave", "shutdown", "before acknowledg"} {
			if !strings.Contains(text, fact) {
				t.Errorf("%s results file says nothing of %q: both brokers' durability is stated in every file", f.title, fact)
			}
		}
	}
}

// TestMosquittoQueuesQoS0 checks the maintainer's second decision for
// 0028k, on Astra's finding 7, and its test 11: "Mosquitto's configuration
// sets queue_qos0_messages true in every run, recorded in each results
// file, so the drain diagnostic compares the same backlog". Sagüin
// delivers QoS 0 messages to away sessions on their return (design.md,
// Choices MQTT leaves open: "QoS 0 messages for away sessions | delivered
// on return"); MQTT 5 §4.1 leaves it open: "QoS 1 and QoS 2 messages
// pending transmission to the Client and OPTIONALLY QoS 0 messages pending
// transmission to the Client". Every Mosquitto run, the drain's included,
// is started on mosquittoConfig, and brokerSetup is its results file's
// record of it.
func TestMosquittoQueuesQoS0(t *testing.T) {
	noMosquitto(t)
	config := fmt.Sprintf(mosquittoConfig, 1883, t.TempDir())
	if !slices.Contains(strings.Split(config, "\n"), "queue_qos0_messages true") {
		t.Fatalf("Mosquitto's configuration:\n%s\nwant queue_qos0_messages true", config)
	}
	if row := brokerSetup("mosquitto", config); !strings.Contains(row[1], "queue_qos0_messages true") {
		t.Fatalf("the results file records Mosquitto as %q; want queue_qos0_messages true recorded", row[1])
	}
}

// TestStorageModeNamed checks the results file's setup row for Sagüin, on
// audit finding 261108-004a, 1: it names the storage mode the run's broker
// ran in (config.md, broker.storage.mode: full, normal or memory, full by
// default), never "full-mode store" whatever the mode. Each mode's
// configuration is the one a load run launches with it (brokerOptions,
// as -storage-mode sets it), as the broker binary resolves it, which is
// what each results file records (target.resolved). Memory mode requires
// memory_dir on a tmpfs, so its folder is made under /dev/shm and removed
// when the test ends.
func TestStorageModeNamed(t *testing.T) {
	shm, err := os.MkdirTemp("/dev/shm", "saguin-load-test-")
	if err != nil {
		t.Fatalf("a tmpfs folder for memory_dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shm) })
	modes := []string{"full", "normal", "memory"}
	for _, c := range []struct {
		mode string
		args []string
	}{
		{"full", nil},
		{"full", []string{"-storage-mode", "full"}},
		{"normal", []string{"-storage-mode", "normal"}},
		{"memory", []string{"-storage-mode", "memory", "-memory-dir", shm}},
	} {
		asked(t, c.args...)
		dir := t.TempDir()
		if _, err := harness.WriteConfig(brokerOptions(dir)); err != nil {
			t.Fatalf("load %q: writing the run's configuration: %v", c.args, err)
		}
		config, err := (&target{binary: brokerBinary, dir: dir}).resolved()
		if err != nil {
			t.Fatalf("load %q: %v", c.args, err)
		}
		row := brokerSetup("saguin", config)
		for _, m := range modes {
			if named := strings.Contains(row[1], m+"-mode"); named != (m == c.mode) {
				t.Errorf("load %q, a run in %s mode: the results' setup records Sagüin as %q; want %s mode named, and no other", c.args, c.mode, row[1], c.mode)
				break
			}
		}
	}
}

// TestMaximumModeSaysSaturation checks the maintainer's test 8 for 0028k,
// on Astra's finding 5 ("label maximum mode as broker-dependent saturation
// behaviour; retain steady rate as the comparison of record"): "a saved
// maximum-mode result says it measures each broker's own saturation
// behaviour". resultSections are the results file's tables of the
// recipe's runs, which measureAll saves.
func TestMaximumModeSaysSaturation(t *testing.T) {
	f := &fileReport{title: "maximum", sections: resultSections([]runResult{{sizes[0], maximum, &outcome{target: "saguin"}}})}
	if text := string(f.markdown()); !strings.Contains(strings.ToLower(text), "saturation") {
		t.Fatalf("a maximum-mode results file says nothing of saturation:\n%s", text)
	}
}

// TestFixedModeSaved checks the maintainer's test 9 for 0028k, on Astra's
// finding 5 ("Save achieved schedule share, missed/unsent slots, due-time
// latency and backlog alongside its requested rate"): "a saved fixed-mode
// result records the share of the schedule sent, missed and unsent slots,
// due-time latency and backlog". The run: 22,220 slots due, 1,234 missed
// and 4,321 unsent, so 75% sent; every latency from due time 12.03 ms, to
// send and to receipt alike, as either is due-time latency; a backlog of
// 24,681 at the window's start and 13,579 at its end.
func TestFixedModeSaved(t *testing.T) {
	o := &outcome{target: "saguin", rate: 1000, scheduled: 22220, missed: 1234, unsent: 4321, backlog: [2]int64{24681, 13579}}
	o.delay.add(12031 * time.Microsecond) // a bucket's upper bound: read back as it is
	o.dueLatency.add(12031 * time.Microsecond)
	f := &fileReport{title: "fixed", sections: resultSections([]runResult{{sizes[0], fixed, o}})}
	text := string(f.markdown())
	var missing []string
	for _, v := range [][2]string{{"share sent", "75.0"}, {"missed slots", "1234"}, {"unsent slots", "4321"},
		{"due-time latency", "12.0"}, {"backlog at the start", "24681"}, {"backlog at the end", "13579"}} {
		if !strings.Contains(text, v[1]) {
			missing = append(missing, v[0]+" "+v[1])
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the fixed-mode results file does not record its %s:\n%s", strings.Join(missing, ", "), text)
	}
}

// TestKeepingUp checks the steady-rate figures of 0028j's plan [261107
// 065639]: "reported per run: the share of time kept up, the largest
// backlog, the catch-up time after a slow phase", "from a known
// per-second series".
//
// The interface: a run records each second of its measured window, in
// order, in outcome.seconds, one second each:
//
//   - offered: the deliveries the subscribers are owed for what the
//     publishers wrote in that second, each publication once to each
//     subscriber of its group;
//   - delivered: the deliveries the subscribers received in it;
//   - backlog: at the second's end, the deliveries owed for everything
//     written so far and not yet received, as outcome.backlog is at the
//     window's two ends;
//   - slow: the drive's phase in it, from the disk monitor
//     (TestDiskMonitor), false (fast) without one.
//
// The results file has a table headed "Keeping up" for the fixed mode's
// runs, a row per case as in its other tables, with these columns, defined
// for an operator:
//
//   - Kept up %: the share of the window's seconds in which the broker kept
//     up: every slot the schedule had due in the second was sent, and the
//     backlog at the second's end was no more than that second's offered
//     deliveries, so the subscribers were less than one second behind the
//     publishers. A second that delivered less than it was offered can
//     still keep up; one that delivered more while still further behind
//     does not. A second in which a slot due was never sent, missed or
//     unsent when the publishers stopped (publisher.publish's missed-slot
//     policy), does not keep up whatever its backlog: a broker slowing its
//     intake by backpressure holds its publishers back, so it is offered
//     less and leaves less backlog (TestKeptUpBySchedule).
//   - Largest backlog: the largest backlog at the end of any second.
//   - Catch-up s: the longest catch-up after a slow phase in the window. A
//     slow phase is a run of consecutive slow seconds, ending where the
//     next fast second begins. Its catch-up is 0 where the broker kept up
//     in the phase's last second; otherwise it is the whole seconds from
//     the phase's end to the end of the first second, from there on, in
//     which the broker kept up, whatever that second's phase. The column
//     says "not caught up" where the broker had not caught up after some
//     slow phase by the window's end, and n/a where the window had no slow
//     second.
//
// Values are written as the file's other tables write them: the share to
// one decimal, the backlog and the seconds whole.
//
// Six runs, each offered 1,000 deliveries a second, each its own case:
//
//   - 12 seconds: 0 and 1 fast and kept up (backlog 200, 300); 2 and 3
//     slow and behind (1,200, then 2,000, the largest); 4 and 5 fast,
//     draining more than offered but still behind (1,500, 1,100); 6 kept up
//     (600), so the first phase's catch-up is 3 s, from the start of second
//     4 to the end of second 6; 7 kept up (400); 8 slow but kept up (900),
//     so the second phase's catch-up is 0; 9 and 10 fast and kept up (300,
//     100); 11 fast and behind (1,050), after no slow phase. Kept up 7 of
//     12 seconds, 58.3%; largest backlog 2,000; catch-up 3 s, the longer of
//     3 and 0. (Counting the seconds that delivered at least what they were
//     offered gives 50.0%; counting the catch-up from the phase's start, 5.)
//   - 4 seconds: 0 fast and kept up (100); 1 slow (1,100); 2 and 3 fast and
//     behind (1,800, 1,500), the window ending before it caught up. Kept up
//     25.0%; largest 1,800; not caught up.
//   - 3 seconds, all fast, as without a disk monitor: kept up (100), behind
//     (1,050), kept up (300). Kept up 66.7%; largest 1,050; catch-up n/a.
//   - 3 seconds, a backlog exactly one second's offer: 0 fast (1,000) and 1
//     slow (1,000) both keep up, "no more than" including equality; 2 fast
//     (500). Kept up 100.0%; largest 1,000; catch-up 0, the phase's last
//     second kept up.
//   - 3 seconds, the window ending in a slow phase: 0 and 1 fast and kept
//     up (100, 200); 2 slow and behind (1,300). Kept up 66.7%; largest
//     1,300; not caught up, the phase ending with the window.
//   - 5 seconds, a recovery spanning another slow phase: 0 slow (1,100); 1
//     fast (1,500); 2 slow (1,900); 3 fast (1,400), all behind; 4 kept up
//     (800). The first phase's catch-up runs from the start of second 1 to
//     the end of second 4, 4 s, the second phase not restarting it (from
//     the second phase's end it is 2 s). Kept up 20.0%; largest 1,900;
//     catch-up 4.
//
// The series must also be the run's own: a short run in the fixed mode
// against the in-process test peer, -window 3s, the disk monitor on, its
// threshold 1 ms, and its drive's diskstats scripted (diskstatsFile) as
// stalled for 3 s in every 4 (no write completed, one in progress) and
// writing fast (0.1 ms a write) for the other, so at least one of the
// window's seconds is slow, whenever it begins, records three seconds, each
// offered and delivering more than nothing. Over them, offered sums
// exactly to the publications the run counts written in its window
// (outcome.written, both QoS) times each group's subscribers (fanout), and
// delivered to the deliveries it counts received in it (outcome.arrived).
// The backlog is conserved: each second's is the one before it, the
// window start's (outcome.backlog[0]) for the first, plus what it was
// offered less what it delivered, and the last second's is the window
// end's (outcome.backlog[1]). Each second carries the phase of the disk
// monitor's second of the same place (outcome.disk.seconds), one for each,
// and at least one is slow.
//
// The series is the run's live record, kept as the window runs, not one
// filled in once the clients have stopped: the same clients, driven by the
// run's own loops (publisher.publish and acks, subscriber.receive) against
// the test peer, with the warm-up already behind them, show each second of
// a 2 s window offered and delivering more than nothing, its backlog
// conserved, as soon as the window ends (outcome.window returns), while
// the clients still publish and receive.
//
// The backlog is the deliveries owed and not yet received, counted apart
// from the series: the same loops at 200 publications a second, with one
// subscriber never reading, so its deliveries stay owed, and the
// publishers stopped 500 ms before the 2 s window ends, so by its end every
// other delivery has arrived and nothing more changes. The last second's
// backlog is then the deliveries owed for every publication written less
// the distinct deliveries received (outcome.owedSoFar, receivedSoFar), as
// the maximum mode samples the window's ends, and more than nothing.
//
// A publication is written when its socket write completes, with the
// series on or off: one begun in the window's last moment whose write
// completes only after the window has ended counts as written in no
// second of it (publisher.written, from which outcome.written and the
// rates come), with -disk-monitor as without it.
func TestKeepingUp(t *testing.T) {
	f, s := false, true
	runs := []struct {
		seconds  []second
		kept     string
		largest  string
		catchUp  string
		describe string
	}{
		{secondsOf(100, phased{f, 200}, phased{f, 300}, phased{s, 1200}, phased{s, 2000}, phased{f, 1500}, phased{f, 1100},
			phased{f, 600}, phased{f, 400}, phased{s, 900}, phased{f, 300}, phased{f, 100}, phased{f, 1050}),
			"58.3", "2000", "3", "two slow phases, caught up 3 s and 0 s after them"},
		{secondsOf(0, phased{f, 100}, phased{s, 1100}, phased{f, 1800}, phased{f, 1500}),
			"25.0", "1800", "not caught up", "a slow phase, still behind at the window's end"},
		{secondsOf(0, phased{f, 100}, phased{f, 1050}, phased{f, 300}),
			"66.7", "1050", "n/a", "no slow second"},
		{secondsOf(0, phased{f, 1000}, phased{s, 1000}, phased{f, 500}),
			"100.0", "1000", "0", "a backlog equal to the second's offer"},
		{secondsOf(0, phased{f, 100}, phased{f, 200}, phased{s, 1300}),
			"66.7", "1300", "not caught up", "the window ending in a slow phase"},
		{secondsOf(200, phased{s, 1100}, phased{f, 1500}, phased{s, 1900}, phased{f, 1400}, phased{f, 800}),
			"20.0", "1900", "4", "a recovery spanning another slow phase"},
	}
	all := make([]runResult, 0, len(runs))
	for i, r := range runs {
		all = append(all, runResult{sizes[0], fixed, &outcome{target: "saguin", rate: float64(100 + i), seconds: r.seconds}})
	}
	file := string((&fileReport{title: "keeping up", sections: resultSections(all)}).markdown())
	columns, rows := savedTable(file, "Keeping up")
	if columns == nil {
		t.Fatalf("the fixed-mode results file has no table headed Keeping up:\n%s", file)
	}
	for i, r := range runs {
		name := resultCases(all[i : i+1])[0].name
		j := slices.IndexFunc(rows, func(row []string) bool { return len(row) > 0 && row[0] == name })
		if j < 0 {
			t.Errorf("the Keeping up table, rows %q, has no row for %s, %s", rows, name, r.describe)
			continue
		}
		for _, want := range [][2]string{{"Kept up %", r.kept}, {"Largest backlog", r.largest}, {"Catch-up s", r.catchUp}} {
			got := cellAt(rows, j, slices.Index(columns, want[0]))
			if (want[1] == "not caught up" && !strings.Contains(strings.ToLower(got), want[1])) || (want[1] != "not caught up" && got != want[1]) {
				t.Errorf("%s, %s: %s %q; want %q (columns %q)", name, r.describe, want[0], got, want[1], columns)
			}
		}
	}
	liveSeconds(t)
	midRunSeconds(t)
	owedBacklog(t)
	writtenAfterTheWindow(t)
}

// TestKeptUpBySchedule checks Kept up % against what the fixed mode's
// schedule asked for, as TestKeepingUp defines it, on audit finding
// 261108-004a: a broker slowing its intake must not read as kept up. One
// publisher, at QoS 0 over a pipe to the test playing the broker, its slot
// due every 100 ms (100 publications a second over the small size's 10
// publishers), through a 3 s window, the run's own window loop
// (outcome.window) keeping the series. The broker takes nothing for the
// window's first 1.5 s, so the publisher's first write waits that long and
// the slots due meanwhile are missed: seconds 1 and 2 do not keep up, and
// Kept up is at most 33.3%. No subscriber is owed anything, so judged by
// the backlog alone every second would keep up: 100%.
func TestKeptUpBySchedule(t *testing.T) {
	asked(t, "-window", "3s")
	r := newRun(sizes[0], 100)
	r.epoch = time.Now().Add(-warmUp) // the warm-up behind: the window opens now
	client, broker := pipe(t)
	p := &publisher{index: publisherAt(r, 0), qos: 0, c: client, outstanding: map[uint16]sentAt{}}
	go func() {
		time.Sleep(1500 * time.Millisecond) // the broker's scripted backpressure, not a wait for a condition
		_, _ = io.Copy(io.Discard, broker)  // until the pipe closes at the test's end
	}()
	stop := make(chan struct{})
	published := make(chan error, 1)
	go func() { published <- p.publish(r, stop, r.epoch.Add(warmUp+measuredWindow+time.Second)) }()
	o := &outcome{run: r, pubs: []*publisher{p}, rate: 100, size: sizes[0]}
	err := o.window(0, nil)
	close(stop)
	if err = errors.Join(err, <-published); err != nil {
		t.Fatalf("a 3 s window, the broker taking nothing for 1.5 s: %v", err)
	}
	o.refreshSeconds()
	if kept := o.keepingUp().kept; !(kept <= 100.0/3) {
		t.Errorf("a 3 s window in which the broker took nothing for 1.5 s, %d slots missed (series %+v): Kept up %.1f%%; want at most 33.3%%, seconds 1 and 2 not kept up, their slots due never sent",
			p.missed, o.seconds, kept)
	}
}

// writtenAfterTheWindow checks, with and without the disk monitor, the
// publication whose write completes after the window, as TestKeepingUp
// says: publisher 0 at QoS 0 over a pipe to the test playing the broker,
// which takes nothing until 100 ms after the window has ended.
func writtenAfterTheWindow(t *testing.T) {
	t.Helper()
	on, slow := monitorDisk, slowWrite
	t.Cleanup(func() { monitorDisk, slowWrite = on, slow })
	for _, args := range [][]string{nil, {"-disk-monitor", "-disk-slow", "2ms"}} {
		asked(t, args...)
		r := newRun(sizes[0], 0)
		r.epoch = time.Now().Add(-(warmUp + measuredWindow) + 50*time.Millisecond) // the window ends in 50 ms
		end := r.epoch.Add(warmUp + measuredWindow)
		client, broker := pipe(t)
		p := &publisher{index: publisherAt(r, 0), qos: 0, limit: 1, c: client, outstanding: map[uint16]sentAt{}}
		taken := make(chan time.Time, 1)
		go func() {
			time.Sleep(time.Until(end.Add(100 * time.Millisecond)))
			taken <- time.Now()
			_, _ = io.Copy(io.Discard, broker) // until the pipe closes at the test's end
		}()
		if err := p.publish(r, make(chan struct{}), end.Add(2*time.Second)); err != nil {
			t.Fatalf("load %q: publishing one publication: %v", args, err)
		}
		if at := <-taken; !at.After(end) || p.cohort[1] != 1 {
			t.Fatalf("load %q: the write was taken %v after the window's end, begun in it %v; want it begun in the window and completed after", args, at.Sub(end), p.cohort[1] == 1)
		}
		if n := p.written.Load(); n != 0 {
			t.Errorf("load %q: a publication whose write completed after the window counts %d written in it; want 0, as with every other option", args, n)
		}
	}
}

// midRunSeconds reads the series as the window leaves it, the clients
// still running, as TestKeepingUp says.
func midRunSeconds(t *testing.T) {
	t.Helper()
	_, seconds, start := windowDriven(t, 1000, 0, 0)
	backlog := start
	for i, s := range seconds {
		if s.offered <= 0 || s.delivered <= 0 {
			t.Errorf("as the window ended, second %d at 1,000 publications/s: offered %d, delivered %d; want both above 0, recorded as the window ran", i+1, s.offered, s.delivered)
		}
		if backlog += s.offered - s.delivered; s.backlog != backlog {
			t.Errorf("as the window ended, second %d: backlog %d; want %d, the second before's plus its offer %d less its deliveries %d", i+1, s.backlog, backlog, s.offered, s.delivered)
		}
	}
}

// owedBacklog checks the series' backlog against the deliveries owed and
// not received, counted apart from it, as TestKeepingUp says.
func owedBacklog(t *testing.T) {
	t.Helper()
	o, seconds, _ := windowDriven(t, 200, 1, 500*time.Millisecond)
	owed, received := o.owedSoFar(), o.receivedSoFar()
	if last := seconds[len(seconds)-1].backlog; last != owed-received || last <= 0 {
		t.Errorf("a 2 s window at 200 publications/s, one subscriber never reading, the publishers stopped 500 ms before its end: the last second's backlog %d; want %d, the %d deliveries owed for every publication written less the %d received, above 0",
			last, owed-received, owed, received)
	}
}

// windowDriven runs a 2 s window of the fixed mode at rate on the run's
// own loops (publisher.publish and acks, subscriber.receive) against the
// test peer, the warm-up already behind them, as TestKeepingUp says: the
// first idle subscribers never read, and the publishers stop stopEarly
// before the window ends. It returns the run's outcome once its clients
// have stopped, with the series and the starting backlog as the window
// left them, two seconds.
func windowDriven(t *testing.T, rate float64, idle int, stopEarly time.Duration) (o *outcome, seconds []second, start int64) {
	t.Helper()
	window := measuredWindow
	t.Cleanup(func() { measuredWindow = window })
	asked(t, "-window", "2s")
	p, err := startPeer()
	if err != nil {
		t.Fatal(err)
	}
	r := newRun(sizes[0], rate)
	subs, pubs, err := setUp(p.addr(), r, sizes[0])
	if err != nil {
		t.Fatal(err)
	}
	o = &outcome{run: r, subs: subs, pubs: pubs, rate: rate, size: sizes[0], addr: p.addr(), target: "peer"}
	r.epoch = time.Now().Add(-warmUp) // the warm-up behind: the window opens now
	o.wallEpoch = r.epoch
	var (
		stop             = make(chan struct{})
		closing          atomic.Bool
		writers, readers sync.WaitGroup
		mu               sync.Mutex
		failures         []error
	)
	fail := func(err error) {
		if err != nil {
			mu.Lock()
			failures = append(failures, err)
			mu.Unlock()
		}
	}
	cutoff := r.epoch.Add(time.Duration(r.observation.Load()))
	for _, pub := range pubs {
		writers.Go(func() { fail(pub.publish(r, stop, cutoff)) })
		if pub.qos == 1 {
			readers.Go(func() { fail(pub.acks(r, &closing)) })
		}
	}
	for _, sub := range subs[idle:] {
		readers.Go(func() { fail(sub.receive(r, &closing)) })
	}
	time.AfterFunc(time.Until(r.epoch.Add(warmUp+measuredWindow-stopEarly)), func() { close(stop) })
	err = o.window(0, nil)
	seconds = slices.Clone(o.seconds) // as the window left it, before the clients stop
	start = o.backlog[0]
	writers.Wait() // once stop has closed
	closing.Store(true)
	for _, c := range clients(subs, pubs) {
		_ = c.Send(&harness.Disconnect{})
	}
	for _, pub := range pubs {
		if pub.qos == 0 {
			_ = pub.c.Close() // no reader: nothing more comes to a QoS 0 publisher
		}
	}
	for _, sub := range subs[:idle] {
		_ = sub.c.Close() // no reader: it never read
	}
	readers.Wait()
	if stopErr := p.stop(); stopErr != nil {
		t.Error(stopErr)
	}
	if err = errors.Join(append(failures, err)...); err != nil {
		t.Fatalf("a 2 s window driven by the run's own loops against the test peer: %v", err)
	}
	if len(seconds) != 2 {
		t.Fatalf("as its 2 s window ended, the run had recorded %d seconds, %+v; want 2, kept as the window ran", len(seconds), seconds)
	}
	return o, seconds, start
}

// liveSeconds runs the short live run TestKeepingUp describes and checks
// its series against the run's own counts.
func liveSeconds(t *testing.T) {
	t.Helper()
	on, slow, window, file := monitorDisk, slowWrite, measuredWindow, diskstatsFile
	t.Cleanup(func() { monitorDisk, slowWrite, measuredWindow, diskstatsFile = on, slow, window, file })
	asked(t, "-window", "3s", "-disk-monitor", "-disk-slow", "1ms")
	scripted := scriptedPhases(t)
	tg, err := startTarget("", "peer")
	if err != nil {
		t.Fatal(err)
	}
	o, err := tg.measure(sizes[0], 1000, false)
	if err = errors.Join(tg.finish(o, err), scripted()); err != nil {
		t.Fatalf("a 3 s window at 1,000 publications/s against the test peer: %v", err)
	}
	if len(o.seconds) != 3 {
		t.Fatalf("a 3 s window recorded %d seconds, %+v; want 3", len(o.seconds), o.seconds)
	}
	var offered, delivered int64
	backlog := o.backlog[0]
	for i, s := range o.seconds {
		if s.offered <= 0 || s.delivered <= 0 {
			t.Errorf("second %d at 1,000 publications/s: offered %d, delivered %d; want both above 0", i+1, s.offered, s.delivered)
		}
		offered, delivered = offered+s.offered, delivered+s.delivered
		if backlog += s.offered - s.delivered; s.backlog != backlog {
			t.Errorf("second %d: backlog %d; want %d, the second before's plus its offer %d less its deliveries %d (the window began %d behind)",
				i+1, s.backlog, backlog, s.offered, s.delivered, o.backlog[0])
		}
	}
	if last := o.seconds[len(o.seconds)-1].backlog; last != o.backlog[1] {
		t.Errorf("the last second's backlog %d; want %d, the window end's", last, o.backlog[1])
	}
	if want := (o.written[0] + o.written[1]) * int64(fanout); offered != want {
		t.Errorf("the seconds offer %d deliveries in all; want %d, the %d publications the run counts written in its window times %d subscribers each",
			offered, want, o.written[0]+o.written[1], fanout)
	}
	if delivered != o.arrived {
		t.Errorf("the seconds deliver %d in all; want %d, the deliveries the run counts received in its window", delivered, o.arrived)
	}
	if o.disk == nil || len(o.disk.seconds) != len(o.seconds) {
		t.Fatalf("with -disk-monitor, the run's disk monitor %+v; want one disk second for each of its %d seconds", o.disk, len(o.seconds))
	}
	for i, s := range o.seconds {
		if s.slow != o.disk.seconds[i].slow {
			t.Errorf("second %d: slow %v; want %v, its disk second's phase %+v", i+1, s.slow, o.disk.seconds[i].slow, o.disk.seconds[i])
		}
	}
	if !slices.ContainsFunc(o.disk.seconds, func(d diskSecond) bool { return d.slow }) {
		t.Errorf("the disk monitor's seconds %+v: none slow; want at least one of the three, its drive stalled 3 s in every 4", o.disk.seconds)
	}
}

// scriptedPhases points diskstatsFile at a scripted diskstats for the
// drive the run's monitor watches, as TestKeepingUp says, rewritten whole
// every 100 ms: stalled for 3 s in every 4, no write completed and one in
// progress, then writing fast for 1 s, each rewrite 100 writes more in
// 10 ms. It returns what stops the rewriting, with the first error it met.
func scriptedPhases(t *testing.T) (stop func() error) {
	t.Helper()
	drive, err := driveOf(os.TempDir())
	if err != nil {
		t.Fatalf("the drive of %s, the store's directory: %v", os.TempDir(), err)
	}
	path := filepath.Join(t.TempDir(), "diskstats")
	write := func(writes, writing, inProgress int64) error {
		next := path + ".next"
		if err := os.WriteFile(next, []byte(diskstatsLine(0, drive, writes, writing, inProgress)), 0o644); err != nil {
			return err
		}
		return os.Rename(next, path) // whole, as the monitor reads it
	}
	writes, writing := int64(1000), int64(100)
	if err := write(writes, writing, 1); err != nil {
		t.Fatal(err)
	}
	diskstatsFile = path
	quit, done := make(chan struct{}), make(chan error, 1)
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-quit:
				done <- nil
				return
			case <-tick.C:
			}
			inProgress := int64(1)
			if i%40 >= 30 {
				writes, writing, inProgress = writes+100, writing+10, 0
			}
			if err := write(writes, writing, inProgress); err != nil {
				done <- err
				return
			}
		}
	}()
	return func() error { close(quit); return <-done }
}

// A phased second is one second of a test's series: its phase and its
// backlog at its end.
type phased struct {
	slow    bool
	backlog int64
}

// seriesOffer is the deliveries each second of TestKeepingUp's series is
// offered.
const seriesOffer = 1000

// secondsOf is a measured window's seconds, each offered seriesOffer
// deliveries, the backlog start at the window's start: each second
// delivers what it was offered less its backlog's growth.
func secondsOf(start int64, steps ...phased) []second {
	seconds := make([]second, len(steps))
	for i, p := range steps {
		seconds[i] = second{offered: seriesOffer, delivered: seriesOffer - (p.backlog - start), backlog: p.backlog, slow: p.slow}
		start = p.backlog
	}
	return seconds
}
