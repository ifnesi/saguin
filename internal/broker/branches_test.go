package broker

// Branches of the delivery core that a running broker does not reach on
// demand, driven directly, each beside a control that takes the other way.
//
// Invariant 3's are the first three: an answer the store says is no longer
// current changes nothing, **and says so**. Reaching them on a running broker
// takes an answer landing in the moment between a store moving a record's
// attempt on and the broker forgetting the old one, so they are driven here
// against a queue store that reports every attempt superseded - the one thing
// a store can say that these branches exist to hear.

import (
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// logLines keeps what a broker logged, one line per record.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

// count is how many lines at level carry msg.
func (l *logLines) count(level, msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, "level="+level) && strings.Contains(line, msg) {
			n++
		}
	}
	return n
}

func loggingBroker(t *testing.T) (*Broker, *logLines) {
	t.Helper()
	b := metricsBroker(t)
	lines := &logLines{}
	b.log = slog.New(slog.NewTextHandler(io.MultiWriter(lines), &slog.HandlerOptions{Level: slog.LevelDebug}))
	return b, lines
}

// staleQueue is a queue store whose every answer names an attempt that is no
// longer current, or - with current set - one that is.
type staleQueue struct {
	QueueStore
	current bool
}

func (q staleQueue) Resolve(store.Held) (store.Item, bool, error) {
	return store.Item{Attempts: 1}, q.current, nil
}

func (q staleQueue) Release(store.Held, time.Time, bool, int, store.DeadLetter) (store.Outcome, bool, error) {
	return store.Outcome{}, q.current, nil
}

func (q staleQueue) Lease(store.Held, time.Time, time.Duration) (store.Item, bool, error) {
	return store.Item{Attempts: 1, LeaseUntil: time.Now().Add(time.Minute)}, q.current, nil
}

// An acknowledgement for a superseded attempt resolves nothing, is not
// counted, and is said out loud: worker A overran its deadline, B took the
// job, A answered late, and this Warn is all that tells an operator the work
// was done twice.
func TestASupersededAcknowledgementChangesNothingAndSaysSo(t *testing.T) {
	for _, current := range []bool{false, true} {
		b, lines := loggingBroker(t)
		b.queues["jobs"] = staleQueue{QueueStore: b.queues["jobs"], current: current}
		d := &delivery{channel: "jobs", offset: 7, epoch: 1, holder: "worker"}
		b.resolve(d)

		warned := lines.count("WARN", "ignoring an acknowledgement for a superseded delivery")
		acked := b.counted.forChannel("jobs").acknowledged.Load()
		if current {
			if warned != 0 || acked != 1 {
				t.Fatalf("a current acknowledgement: %d warnings and %d counted, want 0 and 1, so this proves nothing",
					warned, acked)
			}
			continue
		}
		if warned != 1 {
			t.Errorf("a superseded acknowledgement was logged %d times at Warn, want once", warned)
		}
		if acked != 0 {
			t.Errorf("a superseded acknowledgement was counted as work done (%d)", acked)
		}
	}
}

// A superseded return is the same rule, and **only a worker's own answer is
// worth a Warn**: the same outcome reached by cleanup - a disconnect handing
// back a delivery whose lease had already been reassigned - is ordinary on a
// busy queue, and is said at Debug.
func TestASupersededReturnIsWarnedOnlyWhenAWorkerAnswered(t *testing.T) {
	for _, tc := range []struct {
		answered   bool
		warn, dbug int
	}{{true, 1, 0}, {false, 0, 1}} {
		b, lines := loggingBroker(t)
		b.queues["jobs"] = staleQueue{QueueStore: b.queues["jobs"]}
		d := &delivery{channel: "jobs", offset: 7, epoch: 1, holder: "worker"}
		b.releaseDelivery(d, "returned by worker", tc.answered)
		if got := lines.count("WARN", "ignoring a return for a superseded delivery"); got != tc.warn {
			t.Errorf("answered=%v: %d Warn lines, want %d", tc.answered, got, tc.warn)
		}
		if got := lines.count("DEBUG", "a superseded delivery was already returned"); got != tc.dbug {
			t.Errorf("answered=%v: %d Debug lines, want %d", tc.answered, got, tc.dbug)
		}
		if n := b.counted.forChannel("jobs").returned.Load(); n != 0 {
			t.Errorf("answered=%v: a superseded return was counted (%d)", tc.answered, n)
		}
	}
}

// A PUBACK for an attempt the store no longer holds leases nothing: no
// deadline starts and no attempt is recorded for a delivery another worker
// now has - which is invariant 7's clock started on the wrong job.
func TestAPubackForASupersededAttemptLeasesNothing(t *testing.T) {
	for _, current := range []bool{false, true} {
		b, lines := loggingBroker(t)
		b.SetServer(mqtt.New(&mqtt.Options{}))
		b.queues["jobs"] = staleQueue{QueueStore: b.queues["jobs"], current: current}
		cl := &mqtt.Client{ID: "worker"}
		b.mu.Lock()
		b.deliveries["07"] = &delivery{channel: "jobs", offset: 7, epoch: 1, holder: "worker", conn: cl}
		b.byPacket[packetKey("worker", 5)] = "07"
		b.mu.Unlock()
		b.OnQosComplete(cl, packets.Packet{PacketID: 5})
		leased := lines.count("DEBUG", "leased")
		if current && leased != 1 {
			t.Fatalf("a current attempt's PUBACK was leased %d times, want once, so this proves nothing", leased)
		}
		if !current && leased != 0 {
			t.Errorf("a PUBACK for a superseded attempt was leased (%d)", leased)
		}
	}
}

// A gap in announcements that stays open holds one run above it, however much
// is announced above it, and closing it carries through all of it (invariant
// 13). It used to hold every offset, and past 4,096 stopped for good: a
// channel never stepped over anything again, and the broadcast drain's
// cursors froze.
func TestAGapThatStaysOpenHoldsOneRunAboveIt(t *testing.T) {
	const n = 100_000
	var w watermark
	for off := uint64(2); off <= n; off++ {
		w.done(off)
	}
	if got := w.through.Load(); got != 0 {
		t.Errorf("through stepped to %d over an offset never announced", got)
	}
	if len(w.early) != 1 {
		t.Errorf("%d offsets announced above one gap are held as %d runs, want 1", n-1, len(w.early))
	}
	w.done(1)
	if got := w.through.Load(); got != n || len(w.early) != 0 {
		t.Errorf("the gap closed and through is %d with %d runs held, want %d and none", got, len(w.early), n)
	}
}

// The watermark against the slow way of knowing how far announcing has run:
// every offset announced, and through the highest with all below it. Offsets
// finish in a random order within a random number of publishes in progress
// at once; at every step through is exactly the reference's, and the runs
// held are no more than the gaps below the highest announced - each gap an
// offset still in progress.
func TestAWatermarkIsExactlyContiguityAndHoldsARunPerGap(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q is not a number", v)
		}
		seed = n
	}
	t.Logf("seed %d; replay with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	var steps, outOfOrder, maxRuns int
	for range 300 {
		var w watermark
		inProgress := 1 + rng.Intn(64)
		n := uint64(1 + rng.Intn(3000))
		done := map[uint64]bool{}
		var pending []uint64
		next, ref, top := uint64(1), uint64(0), uint64(0)
		for next <= n || len(pending) > 0 {
			for len(pending) < inProgress && next <= n {
				pending = append(pending, next)
				next++
			}
			i := rng.Intn(len(pending))
			off := pending[i]
			pending = slices.Delete(pending, i, i+1)
			if off != ref+1 {
				outOfOrder++
			}
			// Now and then an offset is announced twice, which changes nothing.
			w.done(off)
			if rng.Intn(20) == 0 {
				w.done(off)
			}
			done[off] = true
			top = max(top, off)
			for done[ref+1] {
				ref++
			}
			steps++
			if got := w.through.Load(); got != ref {
				t.Fatalf("after %d finished, through is %d, want %d", off, got, ref)
			}
			gaps := 0
			for o := ref + 1; o < top; o++ {
				if !done[o] {
					gaps++
				}
			}
			if len(w.early) > gaps {
				t.Fatalf("%d runs held above through %d with %d gaps below %d", len(w.early), ref, gaps, top)
			}
			maxRuns = max(maxRuns, len(w.early))
		}
		if got := w.through.Load(); got != n || len(w.early) != 0 {
			t.Fatalf("all %d announced, and through is %d with %d runs held", n, got, len(w.early))
		}
	}
	t.Logf("%d steps, %d out of order, at most %d runs held", steps, outOfOrder, maxRuns)
	if outOfOrder == 0 || maxRuns < 2 {
		t.Fatalf("%d offsets finished out of order and at most %d runs were held: the case the runs exist for "+
			"never happened", outOfOrder, maxRuns)
	}
}

// A record that already carries dead-letter metadata is given the broker's,
// not a second copy: one of each name, with this dead-lettering's answers,
// and the record's own properties unchanged. Nothing sends a queue such a
// record today - the reserved prefix is stripped from every publish - so this
// is the rule held where it is written.
func TestADeadLetterCarriesEachOfItsNamesOnce(t *testing.T) {
	b := metricsBroker(t)
	jobs := b.reg.Get("jobs")
	rec := b.deadLettered(jobs, "attempts_exhausted")(store.Item{
		Record: store.Record{Offset: 9, Headers: []store.Header{
			{Key: "saguin-dlq-reason", Value: "expired"},
			{Key: "saguin-dlq-attempts", Value: "1"},
			{Key: "tenant", Value: "north"},
		}},
		Attempts: 3,
	})
	seen := map[string][]string{}
	for _, h := range rec.Headers {
		seen[h.Key] = append(seen[h.Key], h.Value)
	}
	for _, k := range []string{"saguin-dlq-reason", "saguin-dlq-attempts", "saguin-dlq-channel"} {
		if len(seen[k]) != 1 {
			t.Errorf("%s appears %d times: %v", k, len(seen[k]), seen[k])
		}
	}
	if got := seen["saguin-dlq-reason"]; len(got) == 1 && got[0] != "attempts_exhausted" {
		t.Errorf("the reason is %q, want this dead-lettering's", got[0])
	}
	if got := seen["tenant"]; len(got) != 1 || got[0] != "north" {
		t.Errorf("the record's own property is %v, want [north]", got)
	}
}

// A session a restart brings back holding a subscription this broker's rules
// refuse is not restored with it (RFC 0002 "Every session's state"): each of
// the structural refusals is asked of what a stored filter is, before
// anything is restored. The queue's refusal is the one a running broker
// reaches through a changed configuration; these four need a stored filter
// the SUBSCRIBE path would have refused, so they are asked directly - and a
// filter the rules accept, the control, is refused by none of them.
func TestARestartRefusesAStoredSubscriptionForWhatItIs(t *testing.T) {
	b := metricsBroker(t)
	for _, tc := range []struct {
		name string
		sub  store.SessionSubscription
	}{
		{"a malformed filter", store.SessionSubscription{Filter: "events/a+b"}},
		{"a partition declared on a shared subscription",
			store.SessionSubscription{Filter: "$share/g/events/#", PartitionCount: 2, PartitionIndices: []int{0}}},
		{"$share spelled another way", store.SessionSubscription{Filter: "$SHARE/g/events/#"}},
		{"a shared subscription with no share name", store.SessionSubscription{Filter: "$share//events/#"}},
	} {
		why, filter := b.refusedNow(store.Session{Client: "c", Subscriptions: []store.SessionSubscription{
			{Filter: "events/#"}, tc.sub,
		}})
		if why == "" || filter != tc.sub.Filter {
			t.Errorf("%s (%q) was restored: refused %q for %q", tc.name, tc.sub.Filter, why, filter)
		}
	}
	if why, _ := b.refusedNow(store.Session{Client: "c", Subscriptions: []store.SessionSubscription{
		{Filter: "events/#"}, {Filter: "$share/g/events/#"},
		// Deeper than max_topic_levels: a bound on what is added, not on what
		// a session holds (RFC 0002 "How deep a topic may be").
		{Filter: "events" + strings.Repeat("/x", 200)},
	}}); why != "" {
		t.Fatalf("filters the rules accept were refused (%q), so this proves nothing", why)
	}
}

// At a start the broker counts the values a latest channel holds under topics
// its filter no longer matches, and logs the number (RFC 0002): a filter
// change moves nothing already stored, and without the count nothing says
// those values can no longer be served. The line names the command that says
// where such a topic goes now. A channel whose values all still match, the
// control, is not reported at all.
func TestAStartCountsTheValuesAFilterChangeStranded(t *testing.T) {
	for _, topics := range [][]string{{"state/a", "state/b"}, {"state/a", "moved/x", "moved/y"}} {
		b, lines := loggingBroker(t)
		for _, topic := range topics {
			if _, err := b.latest["state"].Set(store.Record{Topic: topic, Payload: []byte("v")}); err != nil {
				t.Fatalf("store a value: %v", err)
			}
		}
		b.CountStranded()
		const msg = "stored values are no longer reachable"
		warned := lines.count("WARN", msg)
		if len(topics) == 2 {
			if warned != 0 {
				t.Fatalf("values that all match their channel were reported stranded (%d)", warned)
			}
			continue
		}
		if warned != 1 {
			t.Fatalf("two stranded values were reported %d times, want once", warned)
		}
		lines.mu.Lock()
		var line string
		for _, l := range lines.lines {
			if strings.Contains(l, msg) {
				line = l
			}
		}
		lines.mu.Unlock()
		if !strings.Contains(line, "topics=2") {
			t.Errorf("the count is not two: %s", line)
		}
		if !strings.Contains(line, "saguin --route") {
			t.Errorf("the line does not name saguin --route: %s", line)
		}
	}
}

// A start restoring snapshots refuses a channel the configuration no longer
// has - its records would come back under nothing that can serve them, which
// is invariant 14's "an error naming what was wrong" rather than a quiet
// start - and leaves a retained file alone once broker.retained is gone: the
// operator removed the store on purpose, and refusing to start over it would
// be worse than the values nobody can reach. A configured channel, the
// control, is restored.
func TestARestoreRefusesAnUnconfiguredChannelAndLeavesAnUnwantedRetainedFile(t *testing.T) {
	b := metricsBroker(t)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.restore(&store.ChannelState{Name: "events", Kind: store.KindAppend, Next: 1, Floor: 1}); err != nil {
		t.Fatalf("a configured channel was refused (%v), so this proves nothing", err)
	}
	err := b.restore(&store.ChannelState{Name: "gone", Kind: store.KindAppend, Next: 1, Floor: 1})
	if err == nil || !strings.Contains(err.Error(), `"gone"`) || !strings.Contains(err.Error(), "not in the configuration") {
		t.Errorf("a snapshotted channel the configuration no longer has was answered %v", err)
	}
	if b.retained != nil {
		t.Fatal("this broker has a retained store, so the case below is not the one under test")
	}
	if err := b.restore(&store.ChannelState{Name: RetainedStoreName, Kind: store.KindLatest}); err != nil {
		t.Errorf("a retained file with no retained store configured stopped the start: %v", err)
	}
	if b.retained != nil {
		t.Error("a retained file was loaded although broker.retained is gone")
	}
}

// A memory provider's retained store is counted against that provider's
// max_bytes, as its channels are (invariant 13). It is not in the registry,
// so the loop that bounds every channel cannot reach it, and a store bounded
// by nothing is a machine's memory bounded by the OOM killer. A value that
// fits is kept and charged, the control; one past the bound is refused.
func TestAMemoryRetainedStoreIsBoundedByItsProvider(t *testing.T) {
	b := metricsBroker(t)
	quota := store.NewQuota(4096, 0)
	b.SetQuotas(map[string]*store.Quota{"local": quota})
	b.SetRetained("local", store.NewLatest(), 0)
	b.mu.Lock()
	b.applyBounds()
	b.mu.Unlock()

	if _, err := b.retained.Set(store.Record{Topic: "loose/a", Payload: make([]byte, 100)}); err != nil {
		t.Fatalf("a retained value that fits was refused (%v), so this proves nothing", err)
	}
	if quota.Bytes() == 0 {
		t.Fatal("a retained value was kept and charged to nothing")
	}
	if _, err := b.retained.Set(store.Record{Topic: "loose/b", Payload: make([]byte, 8192)}); !errors.Is(err, store.ErrFull) {
		t.Errorf("a retained value past its provider's max_bytes was answered %v, want ErrFull", err)
	}
}

// Two subscribers that declare different sizes for one partition space are
// told apart out loud: the records in the slices neither of them claims reach
// nobody, and no counter would ever say so. A third that agrees with the
// first, the control, is not reported.
func TestSubscribersThatDisagreeAboutAPartitionSpaceAreReported(t *testing.T) {
	b, lines := loggingBroker(t)
	const msg = "two subscribers disagree about the size of a partition space"
	b.declare("first", "events/#", partition{count: 2, indices: []int{0}})
	b.declare("agrees", "events/#", partition{count: 2, indices: []int{1}})
	if n := lines.count("WARN", msg); n != 0 {
		t.Fatalf("two subscribers declaring the same space were reported %d times, so this proves nothing", n)
	}
	b.declare("disagrees", "events/#", partition{count: 3, indices: []int{1}})
	if n := lines.count("WARN", msg); n != 1 {
		t.Errorf("a subscriber declaring a space of 3 beside ones of 2 was reported %d times, want once", n)
	}
}
