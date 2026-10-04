package scaletest

// A seconds-scale run of the retention role against an in-process broker.
//
// **It is a plumbing guard and not evidence**, and the distinction is the
// whole reason it exists. What the role proves - one reader passed
// correctly while its neighbour on the same channel loses nothing - needs
// real pacing and minutes of floor movement, and `make retention` is where
// that happens. This asserts only that the role still runs: that it reaches
// a broker, that the accounting fires, that the route and the counter are
// read and agree, and that the positive control still detects a gap. The
// difference between "we can run it" and "it still runs".
//
// **It is not gated on SAGUIN_SCALE_ROLE**, unlike TestScaleRole, and that
// is deliberate: `make check` runs `go test ./...`, so a guard behind the
// role variable would skip in every check run and report success for work
// nobody did - which is the failure the five orphaned benchmarks and this
// package's own make targets are written against.

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"
)

func TestTheRetentionRoleStillRuns(t *testing.T) {
	// **Small enough that the floor moves within seconds, large enough that
	// a reader keeping up stays ahead of it.** Both halves are load-bearing.
	// At 400 bytes the channel holds about one record, so every reader is
	// overtaken including the fast ones, and the run fails on a budget
	// rather than on the broker: measured, 1,211 records published and a
	// fast reader passed. 64KiB holds a working set of a few hundred, which
	// the floor still walks through several times in six seconds.
	brokertest.Retain = map[string]int64{"events": 64 << 10}
	t.Cleanup(func() { brokertest.Retain = nil })
	h := brokertest.StartTrimming(t, brokertest.Retain)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	ops := ln.Addr().String()
	_ = ln.Close()
	// A millisecond, so /metrics is never answered from a cached body this
	// role would read as the broker standing still.
	const scrapeInterval = time.Millisecond
	stop, err := h.B.ServeOperations(brokertest.TCPOnly(ops), scrapeInterval, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	t.Cleanup(stop)
	// **The role reads the cache interval off the broker and refuses to run
	// without it**, because a live route compared against a counter cached
	// for a minute fails on arithmetic rather than on the broker. The
	// harness serves no configuration of its own, so this gives it the one
	// key the role asks for - which is also what makes the refusal itself
	// covered: without this the role fatals here, as it should.
	h.B.SetConfigDocument(map[string]any{
		"broker": map[string]any{
			"operations": map[string]any{"min_scrape_interval": scrapeInterval.String()},
		},
	})

	report := filepath.Join(t.TempDir(), "retention-smoke.md")
	c := cfg{
		role: "retention", broker: h.Addr, ops: ops, report: report,
		duration: 6 * time.Second, settle: 2 * time.Second, // settle outlasts the 1ms cache
		channels:   []retentionChannel{{name: "events", prefix: "events"}},
		fast:       2,
		slow:       2,
		publishers: 2,
		pause:      120 * time.Millisecond,
		pad:        400,
		seed:       1,
	}
	log := &runlog{}

	runRetention(t, c, log)

	// **The role wrote its evidence, or it did not run.** A role that
	// asserted its way to the end and left no file behind is one whose
	// report nobody can read afterwards.
	for _, p := range []string{report, topicRowsPath(report)} {
		st, err := os.Stat(p)
		if err != nil {
			t.Errorf("the role left no %s: %v", filepath.Base(p), err)
			continue
		}
		if st.Size() == 0 {
			t.Errorf("%s is empty", filepath.Base(p))
		}
	}

	// **The headroom is in the report**, and it is checked here because the
	// day this test goes red on a slower machine it is the number that says
	// whether the run was sized too tightly or the broker misbehaved. The
	// two look identical without it: this smoke's first budget held about
	// one record, so every cohort including the fast one was overtaken, and
	// that read as a broker defect until the budget was measured.
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatalf("reading the report back: %v", err)
	}
	if !strings.Contains(string(body), "fast cohort headroom on events") {
		t.Errorf("the report carries no headroom for the fast cohort, so a future failure "+
			"here cannot be told from a sizing problem without re-running it by hand:\n%s",
			body)
	}
}

// The positive control, checked rather than trusted. The role asserts that
// the slow cohort recorded gaps, and that assertion is what makes the fast
// cohort's zero mean anything - so this drives the gap checker itself and
// proves it can still see one.
//
// It is here because the checker has been blind before: sequences were kept
// per reader rather than per reader per topic, so four interleaved streams
// looked contiguous and the zero it reported was an artefact.
func TestTheRetentionGapCheckerStillSeesAGap(t *testing.T) {
	r := &retReader{id: "probe", channel: "events"}
	// Two topics interleaved, each contiguous: no gap, and a checker that
	// tracked one sequence per reader would call this four gaps.
	for _, step := range []struct {
		topic string
		seq   uint64
	}{{"events/p0", 1}, {"events/p1", 1}, {"events/p0", 2}, {"events/p1", 2}} {
		r.got(step.topic, step.seq)
	}
	if _, gaps, records, _ := r.snapshot(); gaps != 0 || records != 4 {
		t.Fatalf("two contiguous streams read as %d gap(s) over %d records, want 0 over 4: "+
			"sequences are not being kept per topic", gaps, records)
	}

	// Now a real gap on one of them, which must be seen.
	r.got("events/p0", 9)
	_, gaps, _, firstGap := r.snapshot()
	if gaps != 1 {
		t.Fatalf("a jump from seq 2 to 9 on one topic read as %d gap(s), want 1: the "+
			"checker is blind and every zero it reports is worthless", gaps)
	}
	if firstGap == "" {
		t.Error("the gap was counted and not described, so a failing run says how many " +
			"records went missing and never which")
	}
}

// The replay tells retention's cut from a gap, and still sees a gap. A
// channel whose floor has moved holds each topic's tail, so a topic's first
// stored record above seq 1 is the cut; the same start on a channel whose
// floor never moved is a gap, because nothing was removed; and a jump
// above the floor is a gap whatever the floor says. Found by the gateway
// scale run of 2026-09-27, whose replay reported retention's 60 cuts as 60
// gaps.
func TestTheReplayTellsRetentionsCutFromAGap(t *testing.T) {
	read := func(seqs ...uint64) *replayRow {
		r := &replayRow{target: "gw-evt", kind: "append"}
		for _, s := range seqs {
			r.take("scale/gw/evt/air/p0", s)
		}
		return r
	}

	moved := map[string]uint64{"gw-evt": 5000}
	rows := map[string]*replayRow{"scale/gw/evt/air/p0": read(4001, 4002, 4003)}
	if cut := judgeCuts(rows, moved); cut != 1 || rows["scale/gw/evt/air/p0"].gaps != 0 {
		t.Errorf("a topic whose tail starts at 4001 on a channel whose floor moved read as %d cut and %d "+
			"gap(s), want 1 and 0: retention's cut is being reported as loss",
			cut, rows["scale/gw/evt/air/p0"].gaps)
	}

	still := map[string]uint64{"gw-evt": 1}
	rows = map[string]*replayRow{"scale/gw/evt/air/p0": read(4001, 4002, 4003)}
	if cut := judgeCuts(rows, still); cut != 0 || rows["scale/gw/evt/air/p0"].gaps != 1 {
		t.Errorf("a topic starting at 4001 on a channel whose floor never moved read as %d cut and %d "+
			"gap(s), want 0 and 1: 4,000 records that were never stored are being excused as retention",
			cut, rows["scale/gw/evt/air/p0"].gaps)
	}
	if rows["scale/gw/evt/air/p0"].firstGap == "" {
		t.Error("the gap was counted and not described")
	}

	rows = map[string]*replayRow{"scale/gw/evt/air/p0": read(4001, 4002, 4009)}
	if judgeCuts(rows, moved); rows["scale/gw/evt/air/p0"].gaps != 1 {
		t.Errorf("a jump from 4002 to 4009 above a moved floor read as %d gap(s), want 1: the checker "+
			"is blind above the cut", rows["scale/gw/evt/air/p0"].gaps)
	}

	rows = map[string]*replayRow{"scale/gw/evt/air/p0": read(1, 2, 3)}
	if cut := judgeCuts(rows, moved); cut != 0 || rows["scale/gw/evt/air/p0"].gaps != 0 {
		t.Errorf("a whole topic from seq 1 read as %d cut and %d gap(s), want 0 and 0",
			cut, rows["scale/gw/evt/air/p0"].gaps)
	}
}

// **The replay's contiguity check reads a late duplicate as a duplicate, not
// a gap**. take moved its high-water mark back on
// a re-sent copy landing after a later seq, so 5, 6, 5, 7 reported "seq 7
// after 5" and failed "storage holds no gap" on a stream that has none. Its
// siblings, the consumer's own stream and retReader.got, advance forward only.
func TestTheReplayReadsALateDuplicateAsNoGap(t *testing.T) {
	r := &replayRow{kind: "append"}
	for _, seq := range []uint64{5, 6, 5, 7} {
		r.take("t/a", seq)
	}
	if r.gaps != 0 || r.dups != 1 || r.records != 4 {
		t.Errorf("5, 6, 5, 7 read as %d gaps (%q) and %d dups over %d records, want 0 gaps and 1 dup "+
			"over 4", r.gaps, r.firstGap, r.dups, r.records)
	}
	// And a real gap still reads as one: the fix must not blind it.
	g := &replayRow{kind: "append"}
	for _, seq := range []uint64{5, 6, 8} {
		g.take("t/b", seq)
	}
	if g.gaps != 1 {
		t.Errorf("5, 6, 8 read as %d gaps, want 1", g.gaps)
	}
}
