package main

import (
	"bytes"
	"database/sql"
	"flag"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/logger"
)

// TestQoSByPublisher checks the recipe's QoS: half the publishers at QoS 0
// and half at QoS 1, the same on every run.
func TestQoSByPublisher(t *testing.T) {
	r := newRun(sizes[1], 0)
	counts := [2]int{}
	for _, q := range r.qos {
		counts[q]++
	}
	if counts != [2]int{50, 50} {
		t.Fatalf("QoS 0 and 1 publishers: %v, want 50 each", counts)
	}
	if again := newRun(sizes[1], 0); !bytes.Equal(again.qos, r.qos) {
		t.Fatal("the seed gave two assignments")
	}
	if r.topics[12] != "load/1/12" {
		t.Fatalf("publisher 12's topic %q, want load/1/12", r.topics[12])
	}
}

// TestFixedPeriod checks that the fixed mode's publishers together send
// at the rate asked.
func TestFixedPeriod(t *testing.T) {
	if r := newRun(sizes[0], 1000); r.period != 10*time.Millisecond {
		t.Fatalf("10 publishers at 1000/s: a period of %v each, want 10ms", r.period)
	}
}

// TestFixedNeedsRate checks that a run of the fixed mode alone without
// -rate fails, naming -rate, before it prints its header or starts
// anything: with no rate it would test nothing (design.md, Testing: "a run
// that tested nothing never passes"). The binary does not exist, so no
// broker can start whatever the command decides.
func TestFixedNeedsRate(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := load([]string{"-modes", "fixed", "-binary", filepath.Join(dir, "no-saguin"), "-results", dir}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "-rate") || stdout.Len() != 0 {
		t.Fatalf("-modes fixed without -rate: exit %d, stdout %q, stderr %q; want a failure naming -rate before anything starts",
			code, stdout.String(), stderr.String())
	}
}

// TestStamp checks that a payload carries what a subscriber reads back.
func TestStamp(t *testing.T) {
	payload := make([]byte, payloadSize)
	want := stamp{due: time.Second, sent: 1500 * time.Millisecond, publisher: 999, seq: 1 << 40}
	want.put(payload)
	if got, err := readStamp(payload); err != nil || got != want {
		t.Fatalf("readStamp: %+v, %v; want %+v", got, err, want)
	}
	if _, err := readStamp(payload[:100]); err == nil {
		t.Fatal("a payload cut short read without an error")
	}
	if _, err := readStamp(payload[:stampSize-1]); err == nil {
		t.Fatal("a payload shorter than its stamp read without an error")
	}
}

// TestPayloadMix checks -payload mix: each publisher draws the same sizes
// on every run, every size within a band and above the stamp, the bands
// drawn by their weights.
func TestPayloadMix(t *testing.T) {
	d, err := parsePayload("mix")
	if err != nil {
		t.Fatal(err)
	}
	src, again := d.source(7), d.source(7)
	const draws = 200000
	counts := make([]int, len(mix))
	for range draws {
		n := d.draw(src)
		if m := d.draw(again); m != n {
			t.Fatalf("publisher 7 drew %d, then %d on a second run", n, m)
		}
		band := slices.IndexFunc(mix, func(b struct{ weight, lo, hi int }) bool { return n >= b.lo && n <= b.hi })
		if band < 0 || n < stampSize || n > maxPayload {
			t.Fatalf("a payload of %d bytes, outside every band", n)
		}
		counts[band]++
	}
	for i, b := range mix {
		if got := 100 * float64(counts[i]) / draws; math.Abs(got-float64(b.weight)) > 0.5 {
			t.Fatalf("band %d-%d B drawn %.2f%% of the time, want %d%%", b.lo, b.hi, got, b.weight)
		}
	}
	if d.draw(d.source(8)) == d.draw(d.source(7)) && d.draw(d.source(9)) == d.draw(d.source(7)) {
		t.Fatal("publishers 7, 8 and 9 drew the same first size: one sequence for all")
	}
	for v, want := range map[string]int{"256": 256, "1KiB": 1 << 10, "10KiB": 10 << 10} {
		if got, err := parsePayload(v); err != nil || got.fixed != want || got.draw(src) != want {
			t.Fatalf("-payload %s: %+v, %v; want %d bytes", v, got, err, want)
		}
	}
	for _, bad := range []string{"10", "1MiB", "2KB", "-1", ""} {
		if _, err := parsePayload(bad); err == nil {
			t.Fatalf("-payload %q without an error", bad)
		}
	}
}

// TestQoS1Share checks -qos1-share: its share of the publishers at QoS 1,
// the same on every run, every larger share keeping the QoS 1 publishers
// of a smaller, and the recipe's half where the share is 0.5.
func TestQoS1Share(t *testing.T) {
	t.Cleanup(func() { qos1Share = 0.5 })
	var before []byte
	for _, share := range []float64{0, 0.3, 0.5, 1} {
		qos1Share = share
		r := newRun(sizes[1], 0)
		if again := newRun(sizes[1], 0); !bytes.Equal(again.qos, r.qos) {
			t.Fatalf("share %g: the seed gave two assignments", share)
		}
		if n := bytes.Count(r.qos, []byte{1}); n != int(100*share) {
			t.Fatalf("share %g of 100 publishers: %d at QoS 1", share, n)
		}
		for i := range before {
			if before[i] == 1 && r.qos[i] != 1 {
				t.Fatalf("share %g: publisher %d left QoS 1, which a smaller share gave it", share, i)
			}
		}
		before = r.qos
	}
}

// TestFanout checks -fanout: each group has that many subscribers, each
// receiving its group's publications, the publishers still groupSize a
// group.
func TestFanout(t *testing.T) {
	t.Cleanup(func() { fanout = groupSize })
	o, err := parse([]string{"-peer", "-sizes", "medium", "-fanout", "3"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	sz := o.sizes[0]
	if sz.publishers != 100 || sz.subscribers != 30 || !slices.ContainsFunc(o.partial, func(s string) bool { return strings.Contains(s, "fanout 3 subscribers per group") }) {
		t.Fatalf("-fanout 3 on medium: %d publishers, %d subscribers, header %q", sz.publishers, sz.subscribers, o.partial)
	}
	p, err := startPeer()
	if err != nil {
		t.Fatal(err)
	}
	subs, pubs, err := setUp(p.addr(), newRun(sz, 0), sz)
	if err != nil {
		t.Fatal(err)
	}
	perGroup := make([]int, sz.publishers/groupSize)
	for _, s := range subs {
		perGroup[s.group]++
	}
	pub := pubs[42] // group 4
	if _, err := pub.c.Publish(&harness.Publish{QoS: 0, Topic: "load/4/42", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	for _, s := range subs {
		if s.group != 4 {
			continue
		}
		if got, err := s.c.Recv(); err != nil || got.(*harness.Publish).Topic != "load/4/42" {
			t.Fatalf("subscriber %d of group 4 got %+v, %v", s.index, got, err)
		}
	}
	closeAll(subs, pubs)
	if err := p.stop(); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(perGroup, func(n int) bool { return n != 3 }) {
		t.Fatalf("subscribers per group %v, want 3 each", perGroup)
	}
}

// TestSeqSet checks the set of sequence numbers a subscriber received.
func TestSeqSet(t *testing.T) {
	var s seqSet
	if _, ok := s.last(); ok {
		t.Fatal("an empty set has a last number")
	}
	for _, n := range []uint64{3, 64, 200} {
		if !s.add(n) {
			t.Fatalf("add(%d) on a set without it reported it present", n)
		}
	}
	if s.add(64) {
		t.Fatal("add(64) twice reported it new")
	}
	if !s.has(3) || s.has(4) || s.has(1000) {
		t.Fatal("has disagrees with what was added")
	}
	if last, ok := s.last(); !ok || last != 200 {
		t.Fatalf("last %d, %v; want 200", last, ok)
	}
}

// TestHistogram checks that each value's bucket holds it, within the
// stated resolution; that a fraction of a microsecond rounds up, so a
// percentile is never below the truth; that a value past the top is
// overflow, and a percentile among it says so; and the quantiles read.
func TestHistogram(t *testing.T) {
	for _, v := range []uint64{0, 1, 63, 64, 65, 127, 128, 1000, 123456, 1 << 31, 1<<32 - 1} {
		i, ok := bucket(v)
		if up := upper(i); !ok || up < v || float64(up-v) > 0.016*float64(v)+1 || (i > 0 && upper(i-1) >= v) {
			t.Fatalf("%d µs: bucket %d (%v), upper %d", v, i, ok, up)
		}
	}
	if _, ok := bucket(1 << 32); ok {
		t.Fatal("a value past the top has a bucket")
	}
	var h histogram
	if !h.quantile(0.5).none {
		t.Fatal("an empty histogram has a quantile")
	}
	h.add(1500 * time.Nanosecond)
	if got := h.quantile(1); got.value != 2*time.Microsecond {
		t.Fatalf("1.5µs read back as %v, want 2µs: rounded up, never below", got.value)
	}
	h = histogram{}
	for i := 1; i <= 100; i++ {
		h.add(time.Duration(i) * time.Microsecond)
	}
	h.add(100 * time.Hour)
	var m histogram
	m.merge(&h)
	if p50 := m.quantile(0.5); p50.value != 51*time.Microsecond || p50.over {
		t.Fatalf("p50 of 1..100µs and one overflow: %+v, want 51µs", p50)
	}
	if top := m.quantile(1); !top.over || m.overflow != 1 {
		t.Fatalf("the top percentile %+v, overflow %d: want it flagged over", top, m.overflow)
	}
}

// TestAbsentCauses checks how a publication without a record is
// explained from its publisher's neighbouring records and the floor.
func TestAbsentCauses(t *testing.T) {
	// Offsets of one publisher's publications 0 to 6; 0 is no record.
	offsets := []int64{0, 10, 0, 30, 0, 50, 0}
	got := absentCauses(offsets, 31)
	want := []byte{absentBelow, 0, absentBelow, 0, neverStored, 0, neverStored}
	if !bytes.Equal(got, want) {
		t.Fatalf("floor 31: %v, want %v", got, want)
	}
	got = absentCauses([]int64{0, 0, 40}, 35)
	if !bytes.Equal(got, []byte{unknownCause, unknownCause, 0}) {
		t.Fatalf("an absent one with only a later record past the floor: %v, want unknown", got)
	}
	if got := absentCauses([]int64{0, 5}, 1); got[0] != neverStored {
		t.Fatalf("with retention never run: %v, want never stored", got)
	}
}

// TestAccount checks each publication's classification for a subscriber,
// by its record's offset against the subscriber's position, its exchanges
// in flight and the floor.
func TestAccount(t *testing.T) {
	r := newRun(sizes[0], 0)
	o := &outcome{run: r}
	st := &stored{floor: 20, next: 100, positions: []int64{60}, inflight: []map[int64]bool{{55: true}}}
	for i := range 10 {
		p := &publisher{index: i, qos: r.qos[i]}
		p.sent.Store(6)
		o.pubs = append(o.pubs, p)
		// Publisher i's publications at offsets 10, 30, 50, 55, 70 and
		// none: only publisher 0's are not all received.
		st.offsets = append(st.offsets, []int64{10, 30, 50, 55, 70, 0})
	}
	s := &subscriber{}
	for i := 1; i < 10; i++ {
		for seq := range uint64(6) {
			s.seen[i].add(seq)
		}
	}
	s.seen[0].add(1)
	o.subs = []*subscriber{s}
	err := o.account(st)
	q := r.qos[0]
	// Publication 0 (offset 10): below the floor, the position past it.
	// 1: received. 2 (50): skipped, above the floor, below the position.
	// 3 (55): in flight, pending. 4 (70): past the position, pending.
	// 5: no record, after the publisher's last record at 70 past the floor:
	// not taken by the broker when the run ended, not a loss.
	if o.belowFloor[q] != 1 || o.received[0]+o.received[1] != 1+9*6 || o.skipped[q] != 1 || o.pending[q] != 2 || o.notTaken[q] != 1 || o.neverStored[q] != 0 {
		t.Fatalf("below floor %v, received %v, skipped %v, pending %v, not taken %v, never stored %v",
			o.belowFloor, o.received, o.skipped, o.pending, o.notTaken, o.neverStored)
	}
	if err == nil {
		t.Fatal("a skipped publication passed")
	}
}

// TestMissedSlots checks the fixed mode's missed-slot policy: a
// publication begun late takes the latest slot due, the slots passed
// missed, never sent in a burst.
func TestMissedSlots(t *testing.T) {
	r := newRun(sizes[0], 1000) // 10 publishers, a 10ms period each
	r.epoch = time.Now().Add(-warmUp - 35*time.Millisecond)
	p := &publisher{index: 0}
	slot, due := p.skip(r, 1000) // slot 1000 is due as the window starts
	if slot != 1003 || due != warmUp+30*time.Millisecond || p.missed != 3 || p.scheduled != 4 {
		t.Fatalf("35ms into the window from its first slot: slot %d due %v, missed %d of %d; want slot 1003, 3 of 4 missed",
			slot, due, p.missed, p.scheduled)
	}
}

// TestPeer checks the test peer the headroom runs drive: a publication
// reaches the subscriber of its group at its QoS, and a QoS 1 one is
// acknowledged.
func TestPeer(t *testing.T) {
	p, err := startPeer()
	if err != nil {
		t.Fatal(err)
	}
	s, err := subscribe(p.addr(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := connectPublisher(p.addr(), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cap(pub.window) != peerReceiveMaximum {
		t.Fatalf("window %d, want %d", cap(pub.window), peerReceiveMaximum)
	}
	if _, err := pub.c.Publish(&harness.Publish{QoS: 1, Topic: "load/0/3", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	got, err := s.c.Recv()
	if d, ok := got.(*harness.Publish); err != nil || !ok || d.Topic != "load/0/3" || d.QoS != 1 {
		t.Fatalf("subscriber got %+v, %v", got, err)
	}
	closeAll([]*subscriber{s}, []*publisher{pub})
	if err := p.stop(); err != nil {
		t.Fatal(err)
	}
}

// TestProc checks the /proc readings on this process itself.
func TestProc(t *testing.T) {
	if _, err := take(os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if rss, err := peakRSS(os.Getpid()); err != nil || rss == 0 {
		t.Fatalf("peak RSS %d, %v", rss, err)
	}
}

// noModelCPUInfo is a /proc/cpuinfo naming neither a processor ("model
// name", as x86 does) nor a board ("Model", as a Raspberry Pi's kernel
// does), as a generic arm64 kernel in a virtual machine gives it.
const noModelCPUInfo = `processor	: 0
BogoMIPS	: 108.00
Features	: fp asimd evtstrm crc32 cpuid
CPU implementer	: 0x41
CPU architecture: 8
CPU variant	: 0x0
CPU part	: 0xd08
CPU revision	: 3

`

// The machine a run is reproduced from (environment: "the commit, the
// target it drives, the tool's Go, the machine, the disk under the
// database"; the results file's Setup table, recipe.go: "a results file
// says when it did"), on a host whose /proc/cpuinfo names no model: item
// 0059a, 1, "the tool refuses a machine whose /proc/cpuinfo names no model
// (this VM; the stock Pi does name one)". The model is something a run
// reports, not a condition of running.
//
// With cpuInfoPath at noModelCPUInfo, a run against the in-process test
// peer, the small size in the maximum mode once with a 1 s window, exits 0,
// its header's machine line and its results file's Machine row each
// beginning "model unknown". The test runs alone: cpuInfoPath and the
// window are the package's, set back once it ends.
func TestUnknownModelRuns(t *testing.T) {
	was, window, requested := cpuInfoPath, measuredWindow, windowRequested
	t.Cleanup(func() { cpuInfoPath, measuredWindow, windowRequested = was, window, requested })
	cpuInfoPath = filepath.Join(t.TempDir(), "cpuinfo")
	if err := os.WriteFile(cpuInfoPath, []byte(noModelCPUInfo), 0o644); err != nil {
		t.Fatal(err)
	}
	asked(t) // every option set back once the test ends
	results := t.TempDir()
	args := []string{"-peer", "-sizes", "small", "-modes", "maximum", "-runs", "1", "-window", "1s", "-results", results}
	var stdout, stderr bytes.Buffer
	if code := load(args, &stdout, &stderr); code != 0 {
		t.Fatalf("load %q with %s naming no model: exit %d; stdout:\n%s\nstderr:\n%s", args, cpuInfoPath, code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "machine:  model unknown, ") {
		t.Errorf("load %q with %s naming no model printed:\n%s\nwant its machine line to begin \"model unknown\"", args, cpuInfoPath, stdout.String())
	}
	if file := savedFile(t, results); !strings.Contains(file, "| Machine | model unknown, ") {
		t.Errorf("load %q with %s naming no model saved:\n%s\nwant its Machine row to begin \"model unknown\"", args, cpuInfoPath, file)
	}
}

// TestPick checks that -sizes names the recipe's sizes only, each once.
func TestPick(t *testing.T) {
	name := func(s size) string { return s.name }
	if got, err := pick("large,small", sizes, name); err != nil || len(got) != 2 || got[0].name != "small" {
		t.Fatalf("pick(large,small): %v, %v", got, err)
	}
	for _, bad := range []string{"tiny", "small,small", ""} {
		if _, err := pick(bad, sizes, name); err == nil {
			t.Fatalf("pick(%q) without an error", bad)
		}
	}
	if median([]float64{1, 2, 9}) != 2 || median([]float64{1, 3}) != 2 {
		t.Fatal("median of {1, 2, 9} or {1, 3} is not 2")
	}
}

// TestInertOptionsRefused checks, on audit finding 261108-004a, that an
// option with no effect on the chosen target or experiment is refused, as
// -sync-delay is without -broker saguin, rather than named in the results
// as applied: -log-level, -read-connections, -storage-mode, -memory-dir,
// -commit-interval and -commit-max set only Sagüin's configuration, so
// they require -broker saguin, which
// the test peer (-peer) and Mosquitto are not; -reconnect-interval paces
// only -experiment reconnect; -disk-slow is only the disk monitor's
// threshold, so it requires -disk-monitor. Each refusal names the option.
// Where each has its effect it is accepted.
//
// The same holds for every option, not only those (audit 261108-004a,
// [261109 170708]): -broker-memory bounds a broker's scope, and the test
// peer is no broker; -rate paces the fixed mode alone, which -modes maximum
// leaves out; -ack-delay, -receive-maximum and -fanout shape subscribers,
// and -experiment publish-only has none ("Publish only, no subscriber").
// Where those act they are accepted: -broker-memory against Mosquitto,
// -rate with -modes fixed, -ack-delay and -receive-maximum in drain-only,
// whose subscribers drain the backlog, and -fanout in the reconnect
// experiment, which runs the recipe's cases.
func TestInertOptionsRefused(t *testing.T) {
	asked(t) // every option set back once the test ends
	memory, subscribers := brokerMemory, fanout
	t.Cleanup(func() { brokerMemory, fanout = memory, subscribers })
	// A memory directory parse only names: no broker starts here.
	shm := "/dev/shm/saguin-load-test-unused"
	for _, c := range []struct {
		option string
		args   []string
	}{
		{"-log-level", []string{"-peer", "-log-level", "debug"}},
		{"-log-level", []string{"-broker", "mosquitto", "-log-level", "debug"}},
		{"-read-connections", []string{"-peer", "-read-connections", "4"}},
		{"-read-connections", []string{"-broker", "mosquitto", "-read-connections", "4"}},
		{"-storage-mode", []string{"-peer", "-storage-mode", "normal"}},
		{"-storage-mode", []string{"-broker", "mosquitto", "-storage-mode", "full"}},
		{"-memory-dir", []string{"-broker", "mosquitto", "-storage-mode", "memory", "-memory-dir", shm}},
		{"-commit-interval", []string{"-peer", "-commit-interval", "10ms"}},
		{"-commit-interval", []string{"-broker", "mosquitto", "-commit-interval", "10ms"}},
		{"-commit-max", []string{"-peer", "-commit-max", "100"}},
		{"-commit-max", []string{"-broker", "mosquitto", "-commit-max", "100"}},
		{"-reconnect-interval", []string{"-reconnect-interval", "2s"}},
		{"-reconnect-interval", []string{"-experiment", "publish-only", "-reconnect-interval", "2s"}},
		{"-disk-slow", []string{"-disk-slow", "2ms"}},
		{"-broker-memory", []string{"-peer", "-broker-memory", "2G"}},
		{"-rate", []string{"-modes", "maximum", "-rate", "500"}},
		{"-ack-delay", []string{"-experiment", "publish-only", "-ack-delay", "5ms"}},
		{"-receive-maximum", []string{"-experiment", "publish-only", "-receive-maximum", "10"}},
		{"-fanout", []string{"-experiment", "publish-only", "-fanout", "2"}},
		// Audit finding 261110-003a: only Sagüin is started from -binary,
		// the peer keeps no files on the drive the disk monitor samples, and
		// -peer drives the peer whatever -broker names.
		{"-binary", []string{"-broker", "mosquitto", "-binary", "bin/saguin"}},
		{"-binary", []string{"-peer", "-binary", "bin/saguin"}},
		{"-disk-monitor", []string{"-peer", "-disk-monitor", "-disk-slow", "2ms"}},
		{"-disk-slow", []string{"-peer", "-disk-monitor", "-disk-slow", "2ms"}},
		{"-broker", []string{"-peer", "-broker", "mosquitto"}},
		{"-broker", []string{"-peer", "-broker", "saguin"}},
	} {
		if _, err := parse(c.args, io.Discard); err == nil || !strings.Contains(err.Error(), c.option) {
			t.Errorf("load %q: %v; want it refused, naming %s, which has no effect there", c.args, err, c.option)
		}
		brokerMemory, fanout = memory, subscribers // a refused parse has set them already
	}
	for _, args := range [][]string{
		{"-log-level", "debug", "-read-connections", "4", "-storage-mode", "memory", "-memory-dir", shm},
		{"-commit-interval", "10ms", "-commit-max", "100"},
		{"-experiment", "reconnect", "-reconnect-interval", "2s"},
		{"-disk-monitor", "-disk-slow", "2ms"},
		{"-broker", "saguin", "-binary", "bin/saguin"},
		{"-broker", "mosquitto", "-disk-monitor", "-disk-slow", "2ms"},
		{"-broker", "mosquitto", "-broker-memory", "2G"},
		{"-modes", "fixed", "-rate", "500"},
		{"-experiment", "drain-only", "-ack-delay", "5ms", "-receive-maximum", "10"},
		{"-experiment", "reconnect", "-fanout", "2"},
	} {
		if _, err := parse(args, io.Discard); err != nil {
			t.Errorf("load %q: %v; want it accepted, each option acting there", args, err)
		}
		brokerMemory, fanout = memory, subscribers
	}
}

// asked parses args as load's command line does, failing t where parse
// refuses them, and, when t ends, sets the options outside the recipe that
// these tests ask for back to unset: -ack-delay 0 (each PUBACK at once),
// -receive-maximum 0 (no Receive Maximum on CONNECT), -log-level "" and
// -read-connections 0, -storage-mode "" and -memory-dir "" (each key left
// out of the broker's configuration, config.md's default), as
// -commit-interval 0 and -commit-max 0 are, and -sync-delay 0 (the
// broker's syncs as the disk makes them).
func asked(t *testing.T, args ...string) *options {
	t.Helper()
	o, err := parse(args, io.Discard)
	if err != nil {
		t.Fatalf("load %q: %v", args, err)
	}
	t.Cleanup(func() {
		unset := []string{"-ack-delay", "0", "-receive-maximum", "0", "-log-level", "", "-read-connections", "0",
			"-storage-mode", "", "-memory-dir", "", "-sync-delay", "0"}
		if _, err := parse(unset, io.Discard); err != nil {
			t.Errorf("load %q, every option unset: %v", unset, err)
		}
	})
	return o
}

// running is Sagüin started as every load run starts it (startTarget),
// with the options parsed so far. When t ends, the broker is stopped,
// unless the test has stopped it, its ports are released and its files
// removed: they are the test's alone.
func running(t *testing.T) *target {
	t.Helper()
	tg, err := startTarget(brokerBinary, "saguin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tg.stop(); err != nil { // a stopped broker stops again at once
			t.Error(err)
		}
		tg.broker.Release()
		_ = os.RemoveAll(tg.dir)
	})
	return tg
}

// launched is the configuration the broker of a load run is launched
// with, read as the broker reads it: startTarget starts Sagüin as every run
// does, on the configuration file it names with --config (harness.Broker,
// Config), which an operator reads.
func launched(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load(running(t).broker.Config)
	if err != nil {
		t.Fatalf("the configuration the run's broker was launched with: %v", err)
	}
	return cfg
}

// TestLogLevelWritten checks -log-level, 0008c's plan [261107 065639]: "the
// broker's log level (in the configuration the tool writes)", for its
// "debug logging under load" run. With -log-level debug, the configuration
// a run's broker is launched with sets broker.log_level to debug
// (config.md, broker).
func TestLogLevelWritten(t *testing.T) {
	asked(t, "-log-level", "debug")
	if got := launched(t).Broker.LogLevel; got != logger.Debug {
		t.Fatalf("with -log-level debug, the run's broker was launched with broker.log_level %v; want debug", got)
	}
}

// TestReadConnectionsWritten checks -read-connections, 0008e's plan
// [261107 065639]: "-read-connections (in the configuration the tool
// writes)", for its sweep of 2, 4 and 8. With -read-connections 6, the
// configuration a run's broker is launched with sets
// broker.storage.read_connections to 6
// (config.md, broker.storage: 2 to 8, default 2).
func TestReadConnectionsWritten(t *testing.T) {
	asked(t, "-read-connections", "6")
	if got := launched(t).Broker.Storage.ReadConnections; got != 6 {
		t.Fatalf("with -read-connections 6, the run's broker was launched with broker.storage.read_connections %d; want 6", got)
	}
}

// TestStorageModeWritten checks -storage-mode and -memory-dir, for
// comparing full, normal and memory mode on one machine (design.md, Store;
// config.md, broker.storage): empty or full leaves the default, full. With
// -storage-mode memory -memory-dir <folder>, the configuration a run's
// broker is launched with sets broker.storage.mode to memory and its
// memory_dir to that folder; with -storage-mode normal, mode normal. Each
// option set is named in the results' setup as outside the recipe, as the
// other overrides are. Memory mode requires memory_dir on a tmpfs, so the
// folder is made under /dev/shm and removed when the test ends.
func TestStorageModeWritten(t *testing.T) {
	outside := func(t *testing.T, o *options, option, value string) {
		t.Helper()
		named := func(s string) bool {
			return strings.Contains(s, option+" "+value) && strings.Contains(s, "outside the recipe")
		}
		if !slices.ContainsFunc(o.partial, named) {
			t.Errorf("with -%s %s, the results' setup names %q; want %s %s, outside the recipe", option, value, o.partial, option, value)
		}
	}
	t.Run("memory", func(t *testing.T) {
		dir, err := os.MkdirTemp("/dev/shm", "saguin-load-test-")
		if err != nil {
			t.Fatalf("a tmpfs folder for memory_dir: %v", err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) }) // after the broker stops: cleanups run last first
		o := asked(t, "-storage-mode", "memory", "-memory-dir", dir)
		outside(t, o, "storage-mode", "memory")
		outside(t, o, "memory-dir", dir)
		storage := launched(t).Broker.Storage
		if storage.Mode != config.Memory || storage.MemoryDir != dir {
			t.Fatalf("with -storage-mode memory -memory-dir %s, the run's broker was launched with broker.storage.mode %v, memory_dir %q; want memory, %q",
				dir, storage.Mode, storage.MemoryDir, dir)
		}
	})
	t.Run("normal", func(t *testing.T) {
		o := asked(t, "-storage-mode", "normal")
		outside(t, o, "storage-mode", "normal")
		if got := launched(t).Broker.Storage.Mode; got != config.Normal {
			t.Fatalf("with -storage-mode normal, the run's broker was launched with broker.storage.mode %v; want normal", got)
		}
	})
}

// TestSyncDelay checks -sync-delay, 0008c's plan [261107 065639]: "a slow
// disk (10 ms per sync)", the broker run with every sync of its database
// delayed, as testkit.SlowIO delays it for the conformance tests. The
// interface: the flag -sync-delay, a duration, held in the package's
// syncDelay as -ack-delay is held in ackDelay; zero, its default, delays
// nothing. -binary stays the broker binary, whose build the results
// describe; the delay comes from what the broker is launched under.
//
// With -sync-delay set, the broker startTarget launches answers each of
// commits QoS 1 PUBLISHes, sent one at a time, no sooner than the delay
// after it was sent: in full mode, the tool's (harness.WriteConfig: "a
// full-mode store"), each PUBACK waits for its publication's commit
// (design.md, R4: "A reply confirming a change waits for that change's
// commit"; PUBLISH: "PUBACK once stored"), and each commit for its sync of
// the store's log (Store: "with synchronous=FULL"; "Everything
// acknowledged survives a crash or a power cut"). The PUBLISH is sent
// before the broker reads it and the PUBACK read after the broker writes
// it, so the time between them is a lower bound that scheduling can only
// lengthen. The delay asked is far above the PUBACK a disk gives without
// one (under a millisecond here), so a broker launched without the delay,
// with it on a file its commits never sync (the database file alone, not
// its -wal log), or on only its first few syncs, answers some PUBLISH
// sooner and fails.
//
// The process the tool samples for the broker's CPU and memory (pid, which
// every run's drive and the experiments' drain take) is the broker binary
// itself, not the strace it runs under: its /proc/<pid>/exe is -binary.
// The broker then stops as every run's broker stops before its database
// is read (target.stop: a graceful stop, exit status 0, within its bound),
// the delay in place: strace given an output file blocks the SIGTERM a
// stop sends it, so a tool that signals strace rather than the broker
// waits out the bound and fails. The client has gone first, so the stop
// has no connection to wait for. The results name the option outside the
// recipe, as the other diagnostic options are named.
func TestSyncDelay(t *testing.T) {
	const delay, commits = 100 * time.Millisecond, 10
	o := asked(t, "-sync-delay", delay.String())
	named := func(s string) bool {
		return strings.Contains(s, "sync-delay "+delay.String()) && strings.Contains(s, "outside the recipe")
	}
	if !slices.ContainsFunc(o.partial, named) {
		t.Errorf("with -sync-delay %v, the results' setup names %q; want sync-delay %v, outside the recipe", delay, o.partial, delay)
	}
	tg := running(t)
	want, err := filepath.Abs(brokerBinary)
	if err == nil {
		want, err = filepath.EvalSymlinks(want) // as /proc/<pid>/exe names it
	}
	if err != nil {
		t.Fatal(err)
	}
	if exe, err := os.Readlink("/proc/" + strconv.Itoa(tg.pid()) + "/exe"); err != nil || exe != want {
		t.Errorf("with -sync-delay %v, the tool samples process %d, running %q (%v); want the broker, %s", delay, tg.pid(), exe, err, want)
	}
	c, _, err := connect(tg.addr, "load-sync-delay", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }() // closed below; again if the test ends early
	payload := make([]byte, payloadSize)
	stamp{}.put(payload)
	for n := range commits {
		sent := time.Now()
		ack, err := c.Publish(&harness.Publish{QoS: 1, Topic: "load/0/0", Payload: payload})
		took := time.Since(sent)
		switch {
		case err != nil:
			t.Fatalf("QoS 1 PUBLISH %d of %d with -sync-delay %v: %v", n+1, commits, delay, err)
		case ack.Reason != 0:
			t.Fatalf("QoS 1 PUBLISH %d of %d with -sync-delay %v got PUBACK 0x%02X; want 0x00", n+1, commits, delay, ack.Reason)
		case took < delay:
			t.Fatalf("with -sync-delay %v, QoS 1 PUBLISH %d of %d was acknowledged %v after it was sent; want no sooner than %v, its commit's sync delayed (R4)",
				delay, n+1, commits, took, delay)
		}
	}
	_ = c.Send(&harness.Disconnect{}) // a clean end: the session is clean, nothing to store
	_ = c.Close()
	if err := tg.stop(); err != nil {
		t.Fatalf("with -sync-delay %v, the broker's stop, which a run needs before reading its database: %v", delay, err)
	}
}

// TestLongerWindow checks -window, 0028j's plan [261107 065639]: "a
// measured window longer than the recipe's, the results file saying so",
// for its steady-rate runs "of 10 minutes or more". The interface: the
// flag -window, a duration, held in the package's measuredWindow, which is
// the recipe's measured (30 s) unless -window sets it. The recipe's
// constant itself does not change (recipe.go: "nothing on the command line
// changes them"). Every figure a run takes over its measured window is
// then taken over -window's:
//
//   - the results name the option outside the recipe, as the other options
//     are named ("window 10m0s (outside the recipe)"), and the results
//     table's notes name the window its rates are over (10m0s), never the
//     recipe's 30 s;
//   - the window is [warmUp, warmUp+window) of the run, and its
//     observation cutoff observeLimit after that (newRun, run.inWindow);
//   - the run takes its window's closing sample at warmUp+window, not at
//     the recipe's end (outcome.window), and its resources are over that
//     length, the stall shares included (sample.since);
//   - the fixed mode counts the slots due in the whole window: at 1,000 a
//     second over the small size's 10 publishers, publisher 0 has 60,000
//     slots due in 10 minutes, all unsent where it stops at the start
//     (publisher.settle);
//   - rates are per second of it: 600,000 deliveries received in 10
//     minutes are 1,000 a second;
//   - the database evidence counts the records whose intake time
//     (storage.md, records: time, "the core's intake time", in Unix
//     milliseconds as the tool reads it) falls in the window, so a record
//     taken in 40 s into the window and one in its last millisecond count,
//     one at its end or a millisecond before its start does not
//     (readStore).
func TestLongerWindow(t *testing.T) {
	const long = 10 * time.Minute
	t.Cleanup(func() { measuredWindow = measured })
	o := asked(t, "-window", long.String())
	named := func(s string) bool {
		return strings.Contains(s, "window "+long.String()) && strings.Contains(s, "outside the recipe")
	}
	if !slices.ContainsFunc(o.partial, named) {
		t.Errorf("with -window %v, the results' setup names %q; want window %v, outside the recipe", long, o.partial, long)
	}

	r := newRun(sizes[0], 1000)
	if got := time.Duration(r.observation.Load()); got != warmUp+long+observeLimit {
		t.Errorf("with -window %v, the run's observation cutoff is %v after its start; want %v, the warm-up, the window and observeLimit", long, got, warmUp+long+observeLimit)
	}
	if !r.inWindow(warmUp) || !r.inWindow(warmUp+long-time.Nanosecond) || r.inWindow(warmUp+long) || r.inWindow(warmUp-time.Nanosecond) {
		t.Errorf("with -window %v, the measured window is not [%v, %v): in it at %v %v, at %v %v; before it at %v %v",
			long, warmUp, warmUp+long, warmUp, r.inWindow(warmUp), warmUp+long-time.Nanosecond, r.inWindow(warmUp+long-time.Nanosecond),
			warmUp-time.Nanosecond, r.inWindow(warmUp-time.Nanosecond))
	}
	p := &publisher{index: 0}
	if err := p.settle(r, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if want := int64(long / r.period); p.scheduled != want || p.unsent != want {
		t.Errorf("with -window %v at 1,000/s, a publisher stopping at the start has %d slots due in the window, %d unsent; want %d of each", long, p.scheduled, p.unsent, want)
	}

	// Past the recipe's end, the window still runs: its closing sample is
	// taken only at warmUp+window.
	waiting := &outcome{run: newRun(sizes[0], 0)}
	waiting.run.epoch = time.Now().Add(-(warmUp + measured + time.Second))
	failed := make(chan struct{})
	ended := make(chan error, 1)
	go func() { ended <- waiting.window(0, failed) }()
	select {
	case err := <-ended:
		t.Errorf("with -window %v, the measured window ended at once, %v after the run began, past the recipe's %v only (%v); want it to run on to %v",
			long, warmUp+measured+time.Second, warmUp+measured, err, warmUp+long)
	case <-time.After(500 * time.Millisecond):
		close(failed)
		<-ended
	}
	ending := &outcome{run: newRun(sizes[0], 0)}
	ending.run.epoch = time.Now().Add(-(warmUp + long) + 300*time.Millisecond)
	if err := ending.window(0, nil); err != nil {
		t.Fatal(err)
	}
	if ending.resources.length != long {
		t.Errorf("with -window %v, the window's resources are over %v; want %v", long, ending.resources.length, long)
	}
	hist := &metrics.Float64Histogram{Counts: []uint64{0}, Buckets: []float64{0, 1}}
	before, after := sample{sched: hist}, sample{ioStall: (long / 10).Microseconds(), sched: hist}
	if got := after.since(&before); math.Abs(got.ioStall-0.1) > 1e-9 {
		t.Errorf("with -window %v, %v stalled on I/O is a share %g of the window; want 0.1", long, long/10, got.ioStall)
	}

	o2, _ := smallRun("saguin")
	o2.subs = nil // the evidence of the publications alone
	o2.wallEpoch = time.UnixMilli(1_800_000_000_000)
	k := publisherAt(o2.run, 1)
	o2.pubs[k].sent.Store(4)
	db := filepath.Join(t.TempDir(), "saguin.db")
	storedAtTimes(t, db, k, o2.wallEpoch, warmUp+40*time.Second, warmUp+long-time.Millisecond, warmUp+long, warmUp-time.Millisecond)
	st, err := readStore(db, o2)
	if err != nil {
		t.Fatal(err)
	}
	if want := [2]int64{0, 2}; st.inWindow != want {
		t.Errorf("with -window %v, the database evidence counts %v records in the window, by QoS, of records taken in at 40 s, %v, %v and %v after its start; want %v",
			long, st.inWindow, long-time.Millisecond, long, -time.Millisecond, want)
	}

	res := resultSections([]runResult{{sizes[0], maximum, &outcome{target: "saguin", arrived: int64(1000 * long.Seconds())}}})[0]
	if col := slices.Index(res.columns, "Deliveries/s"); col < 0 || res.rows[0][col] != "1000" {
		t.Errorf("with -window %v, %d deliveries received in it are reported as %q per second, columns %q; want 1000", long, int64(1000*long.Seconds()), cellAt(res.rows, 0, col), res.columns)
	}
	if notes := strings.Join(res.notes, " "); !strings.Contains(notes, long.String()) || strings.Contains(notes, "30 s") {
		t.Errorf("with -window %v, the results table's notes say:\n%s\nwant the rates said to be over the %v window, not the recipe's 30 s", long, notes, long)
	}
}

// storedAtTimes writes at path the part of Sagüin's database readStore
// reads (storage.md: channels, records, sessions, positions,
// session_inflight): on $broadcast, publisher k's publications 0, 1 and
// so on at offsets 1, 2 and so on, each taken in at epoch plus its time,
// in Unix milliseconds; no session.
func storedAtTimes(t *testing.T, path string, k int, epoch time.Time, times ...time.Duration) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }() // written whole below
	for _, ddl := range []string{
		`CREATE TABLE channels (id INTEGER PRIMARY KEY, name TEXT, floor INTEGER, next INTEGER)`,
		`CREATE TABLE records (channel INTEGER, "offset" INTEGER, qos INTEGER, time INTEGER, payload BLOB, payload_size INTEGER)`,
		`CREATE TABLE sessions (id INTEGER PRIMARY KEY, client TEXT)`,
		`CREATE TABLE positions (session INTEGER, channel INTEGER, "offset" INTEGER)`,
		`CREATE TABLE session_inflight (session INTEGER, channel INTEGER, "offset" INTEGER)`,
		`INSERT INTO channels VALUES (1, '$broadcast', 1, ` + strconv.Itoa(len(times)+1) + `)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	for seq, at := range times {
		payload := make([]byte, payloadSize)
		stamp{publisher: k, seq: uint64(seq)}.put(payload)
		if _, err := db.Exec(`INSERT INTO records VALUES (1, ?, 1, ?, ?, ?)`, seq+1, epoch.Add(at).UnixMilli(), payload, len(payload)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestEveryOptionReaches checks, on audit finding 261108-004a, that every
// option of load's command line has its entry in the one table of where
// options act (reaches): the targets, experiments and modes it acts on,
// from which parse both refuses it elsewhere and names it in the results.
// An option added without an entry fails here; so does an entry naming no
// option, a target other than saguin, mosquitto and peer, an experiment
// other than the recipe ("") and those of -experiment, or a mode other
// than maximum and fixed.
func TestEveryOptionReaches(t *testing.T) {
	// Binding the flags below writes each option's variable: its default,
	// or, for -broker-memory, -qos1-share, -fanout and -mosquitto, whose
	// default is the variable's current value, that value again. asked
	// sets the options outside the recipe back to unset when t ends.
	asked(t)
	flags := newCommandLine(&options{}, io.Discard)
	flags.VisitAll(func(f *flag.Flag) {
		if !slices.ContainsFunc(reaches, func(r reach) bool { return r.name == f.Name }) {
			t.Errorf("-%s has no entry in reaches: where it acts is unknown, so it cannot be refused where it does not", f.Name)
		}
	})
	known := append([]string{""}, experiments...)
	for _, r := range reaches {
		if flags.Lookup(r.name) == nil {
			t.Errorf("reaches names -%s, which is no option of load's", r.name)
		}
		if n := len(slices.DeleteFunc(slices.Clone(r.targets), func(s string) bool { return slices.Contains([]string{"saguin", "mosquitto", "peer"}, s) })); n > 0 {
			t.Errorf("-%s acts on targets %q; want each of saguin, mosquitto or peer", r.name, r.targets)
		}
		if n := len(slices.DeleteFunc(slices.Clone(r.experiments), func(s string) bool { return slices.Contains(known, s) })); n > 0 {
			t.Errorf("-%s acts on experiments %q; want each of %q", r.name, r.experiments, known)
		}
		if n := len(slices.DeleteFunc(slices.Clone(r.modes), func(m mode) bool { return m == maximum || m == fixed })); n > 0 {
			t.Errorf("-%s acts on modes %v; want each maximum or fixed", r.name, r.modes)
		}
	}
}
