package broker

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// durableBroker is a broker whose channels are all kept in one directory,
// so that a second one built on the same directory is a restart.
func durableBroker(t *testing.T, dir string) *Broker {
	t.Helper()
	chans := []*channel.Channel{
		{Name: "events", Type: channel.Append, Storage: "local"},
		{Name: "state", Type: channel.Latest, Storage: "local"},
		{Name: "jobs", Type: channel.Queue, Storage: "local", VisibilityTimeout: 30, MaxAttempts: 3},
	}
	reg, err := channel.NewRegistry(chans)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	// The derived dead-letter channel takes its queue's provider, which is
	// what puts the two in one file.
	reg.Get("jobs").DLQ.Storage = "local"

	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.SetSnapshotDirs(map[string]*store.Dir{"local": store.NewDir(dir, "0.1.0-test")})
	return b
}

// sweepWatcher is a log store that says when it is being swept and when it
// is being read for a snapshot, and holds the sweep open until the test
// lets it go. Embedding the real store rather than reimplementing it keeps
// every other method - Export and Positions among them, which is how a
// snapshot reads a channel at all.
type sweepWatcher struct {
	*store.Log
	events chan string
	hold   chan struct{}
}

func (s *sweepWatcher) Trim(before time.Time, maxBytes int64) (int, int64, error) {
	s.events <- "sweep started"
	<-s.hold
	s.events <- "sweep finished"
	return s.Log.Trim(before, maxBytes)
}

func (s *sweepWatcher) Export() (uint64, uint64, []store.Record) {
	s.events <- "snapshot read"
	return s.Log.Export()
}

// The snapshot is written after the retention sweeps have stopped, not
// merely after they have been asked to stop.
//
// Cancelling Run's context returns immediately and says nothing about
// whether a sweep is inside a store at that moment, deleting records from
// the channel the snapshot is about to read. Nothing has shown that
// costing anything - which is why this asserts the order rather than an
// outcome, and why it is here rather than in main: three statements in a
// function are three statements anybody can reorder, and this fails when
// somebody does.
func TestTheSnapshotWaitsForTheRetentionSweeps(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)

	// A size the channel is over, so that the sweep Run performs before
	// either ticker fires actually reaches Trim. Without it sweepBySize
	// skips the channel and the test would hold nothing open.
	b.reg.Get("events").RetentionBytes = 1

	w := &sweepWatcher{
		Log:    store.NewLog(),
		events: make(chan string, 8),
		hold:   make(chan struct{}),
	}
	b.logs["events"] = w
	w.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("x")})

	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)

	if got := <-w.events; got != "sweep started" {
		t.Fatalf("first event was %q, want the sweep to have started", got)
	}
	cancel()

	// Shutdown must now be waiting for the sweep this test is holding.
	done := make(chan error, 1)
	go func() { done <- b.Shutdown() }()

	select {
	case err := <-done:
		t.Fatalf("Shutdown returned (%v) while a retention sweep was still inside the "+
			"store, so the snapshot can be taken from a channel being deleted from", err)
	case got := <-w.events:
		t.Fatalf("the store saw %q while a retention sweep was still in progress", got)
	case <-time.After(250 * time.Millisecond):
	}

	close(w.hold)
	if err := <-done; err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// The whole sequence, in order, from the store's own point of view.
	want := []string{"sweep finished", "snapshot read"}
	for i, w2 := range want {
		select {
		case got := <-w.events:
			if got != w2 {
				t.Fatalf("event %d was %q, want %q", i, got, w2)
			}
		default:
			t.Fatalf("event %d never happened, want %q", i, w2)
		}
	}
}

func restart(t *testing.T, dir string, b *Broker) (*Broker, []string) {
	t.Helper()
	if err := b.SaveSnapshots(); err != nil {
		t.Fatalf("save: %v", err)
	}
	next := durableBroker(t, dir)
	warnings, err := next.LoadSnapshots()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return next, warnings
}

// The whole point: a broker that shuts down gracefully comes back with
// what it held.
func TestEveryChannelComesBackFromItsSnapshot(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)

	for i := range 5 {
		b.logs["events"].Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte{byte(i)}})
	}
	b.latest["state"].Set(store.Record{Topic: "state/a", Payload: []byte("on")})
	b.latest["state"].Set(store.Record{Topic: "state/b", Payload: []byte("off")})
	b.queues["jobs"].Enqueue(store.Record{Topic: "jobs/one", Payload: []byte("work")})
	b.queues["jobs"].Enqueue(store.Record{Topic: "jobs/two"})
	b.logs["jobs__dlq"].Append(store.Record{Topic: "jobs/__dlq/failed"})

	b2, warnings := restart(t, dir, b)
	if len(warnings) != 0 {
		t.Errorf("a clean shutdown warned: %v", warnings)
	}

	if got := memLen(t, b2.logs["events"]); got != 5 {
		t.Errorf("events came back with %d records, want 5", got)
	}
	if got := b2.latest["state"].(*store.Latest).Len(); got != 2 {
		t.Errorf("state came back with %d topics, want 2", got)
	}
	if total, _ := b2.queues["jobs"].(*store.Queue).Depth(); total != 2 {
		t.Errorf("jobs came back with %d items, want 2", total)
	}
	if got := memLen(t, b2.logs["jobs__dlq"]); got != 1 {
		t.Errorf("the dead-letter channel came back with %d records, want 1", got)
	}

	// Invariant 9: offsets are never reused, including after a restart.
	// Computing the next offset from the records that survive is what
	// breaks this, and it breaks it silently.
	if r, _ := b2.logs["events"].Append(store.Record{Topic: "events/y"}); r.Offset != 6 {
		t.Errorf("the first record after a restart took offset %d, want 6", r.Offset)
	}
	if r, _ := b2.queues["jobs"].Enqueue(store.Record{Topic: "jobs/three"}); r.Offset != 3 {
		t.Errorf("the first job after a restart took offset %d, want 3", r.Offset)
	}
	if r, _ := b2.latest["state"].Set(store.Record{Topic: "state/c"}); r.Offset != 3 {
		t.Errorf("the first value after a restart took offset %d, want 3", r.Offset)
	}
}

// Invariant 9, on its dangerous path: a channel retention has emptied.
//
// Every record is gone and the counters are well past 1. Recomputing
// either of them from the records that survive reads as a fresh channel,
// so the next record reuses an offset a stored consumer position already
// points at - and the consumer reads unrelated data in order and reports
// success.
func TestAnEmptiedChannelKeepsItsOffsets(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)
	b.logs["events"] = store.RestoreLog(41, 41, nil, nil)

	b2, _ := restart(t, dir, b)

	if got := b2.logs["events"].Next(); got != 41 {
		t.Errorf("the next offset came back %d, want 41", got)
	}
	if got := b2.logs["events"].Floor(); got != 41 {
		t.Errorf("the retention floor came back %d, want 41", got)
	}
	if r, _ := b2.logs["events"].Append(store.Record{Topic: "events/x"}); r.Offset != 41 {
		t.Errorf("the first record after the restart took offset %d, want 41", r.Offset)
	}
	if _, err := b2.logs["events"].ReadFrom(40); err != store.ErrBelowFloor {
		t.Errorf("a read below the restored floor returned %v, want ErrBelowFloor", err)
	}
}

// Invariant 15: a restart never resurrects a delivery.
func TestARestartResurrectsNoDelivery(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)

	b.queues["jobs"].Enqueue(store.Record{Topic: "jobs/held"})
	b.queues["jobs"].Enqueue(store.Record{Topic: "jobs/sent"})

	// Arranged through the real transitions rather than by writing the
	// fields, so that what is on disk is a state the broker can actually
	// reach. Offset 1 goes out, comes back on a timeout, and goes out
	// again: leased, attempted twice, and on its second epoch. Offset 2 is
	// written to a worker and not yet acknowledged.
	q := b.queues["jobs"]
	dl := store.DeadLetter{Log: b.logs["jobs__dlq"], Record: b.deadLettered(b.reg.Get("jobs"), "attempts_exhausted")}

	mustOffer(t, q)
	q.Lease(store.Held{Offset: 1, Epoch: 0, Holder: "worker-1"}, time.Now(), time.Hour)
	q.Release(store.Held{Offset: 1, Epoch: 0}, time.Now(), false, 5, dl)
	offered := mustOffer(t, q)
	q.Lease(store.Held{Offset: 1, Epoch: 1, Holder: "worker-1"}, time.Now(), time.Hour)

	if len(offered) != 1 || offered[0].Offset != 1 || offered[0].Epoch != 1 {
		t.Fatalf("the second offer was %+v; want offset 1 on epoch 1", offered)
	}
	b.deliveries[offered[0].DeliveryID] = &delivery{channel: "jobs", offset: 1, epoch: 1, holder: "worker-1"}

	b2, _ := restart(t, dir, b)

	total, inflight := b2.queues["jobs"].(*store.Queue).Depth()
	if total != 2 || inflight != 0 {
		t.Fatalf("after a restart: %d items, %d in flight; want 2 and none", total, inflight)
	}
	if len(b2.deliveries) != 0 {
		t.Errorf("%d deliveries survived the restart", len(b2.deliveries))
	}
	_, items := b2.queues["jobs"].(exportableQueue).Export()
	for _, it := range items {
		if it.State != store.Available {
			t.Errorf("offset %d came back %v, want Available", it.Offset, it.State)
		}
		if it.DeliveryID != "" || it.Holder != "" || !it.LeaseUntil.IsZero() || it.Epoch != 0 {
			t.Errorf("offset %d came back holding a delivery: %+v", it.Offset, it)
		}
	}
	// The attempt count does survive, so a job that has spent its attempts
	// is still dead-lettered rather than starting over.
	if items[0].Attempts != 2 {
		t.Errorf("attempts = %d, want 2", items[0].Attempts)
	}
	if items[0].FirstSeen.IsZero() {
		t.Error("the record of when the job was first attempted was lost")
	}
}

// A consumer resumes where it stopped. Without this the snapshot makes
// things worse than losing the data did: every channel comes back full
// and every consumer replays it from the beginning.
func TestConsumerPositionsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)
	for range 10 {
		b.logs["events"].Append(store.Record{Topic: "events/x"})
	}

	cur := b.cursor("reader", "events", false)
	cur.next = 7
	cur.expiresIn = time.Hour
	b.flushPositions()

	// A session that has already expired by the time the broker comes
	// back holds a position for nobody.
	stale := b.cursor("gone", "events", false)
	stale.next = 3
	stale.expiresIn = time.Second
	b.flushPositions()
	// Backdated so the one-second session has already run out by the time
	// the snapshot is read.
	if err := b.logs["events"].SavePosition(store.Position{
		Reader: store.MQTTReader("gone"), Offset: 3,
		LastSeen: time.Now().Add(-time.Hour), ExpiresIn: time.Second,
	}); err != nil {
		t.Fatalf("save position: %v", err)
	}

	b2, warnings := restart(t, dir, b)

	if got := b2.cursor("reader", "events", false).next; got != 7 {
		t.Errorf("the consumer resumed at %d, want 7 - it will replay records it acknowledged", got)
	}
	if got := b2.cursor("gone", "events", false).next; got != 1 {
		t.Errorf("a position whose session had expired came back as %d", got)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "expired") {
		t.Errorf("dropping a position said %v, want one warning about the expiry", warnings)
	}
}

// A channel that changed type between two runs must not load. Append
// records in a latest map, or queue work in a log where nothing can
// acknowledge it, is wrong in a way nothing reports afterwards.
func TestAChannelThatChangedTypeRefusesToLoad(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)
	b.logs["events"].Append(store.Record{Topic: "events/x"})
	if err := b.SaveSnapshots(); err != nil {
		t.Fatalf("save: %v", err)
	}

	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Latest, Storage: "local"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	changed := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	changed.SetSnapshotDirs(map[string]*store.Dir{"local": store.NewDir(dir, "test")})

	_, err = changed.LoadSnapshots()
	if err == nil {
		t.Fatal("a channel whose type changed loaded anyway")
	}
	if !strings.Contains(err.Error(), "events") {
		t.Errorf("the error does not name the channel: %v", err)
	}
}

// A broker with no storage provider writes nothing and says so. Silence
// about it would be the durability surprise invariant 14 is written
// against.
func TestNoProviderKeepsNothing(t *testing.T) {
	b := testBroker(t)
	if b.Durable() {
		t.Fatal("a broker with no snapshot directory reported itself durable")
	}
	if err := b.SaveSnapshots(); err != nil {
		t.Fatalf("saving nothing failed: %v", err)
	}
	if w, err := b.LoadSnapshots(); err != nil || len(w) != 0 {
		t.Fatalf("loading nothing gave %v and %v", w, err)
	}
}

// A queue and its dead-letter channel are one file, so the move between
// them cannot be recorded by half (invariant 5).
func TestAQueueAndItsDLQShareOneFile(t *testing.T) {
	dir := t.TempDir()
	b := durableBroker(t, dir)
	b.queues["jobs"].Enqueue(store.Record{Topic: "jobs/a"})
	b.logs["jobs__dlq"].Append(store.Record{Topic: "jobs/__dlq/a"})
	if err := b.SaveSnapshots(); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := store.NewDir(dir, "test").Load([]string{"jobs"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := loaded.Snapshots["jobs"]
	if got == nil || len(got.Channels) != 2 {
		t.Fatalf("the queue's file holds %v, want the queue and its DLQ", got)
	}
	if got.Channels[0].Name != "jobs" || got.Channels[1].Name != "jobs__dlq" {
		t.Errorf("the file holds %q and %q", got.Channels[0].Name, got.Channels[1].Name)
	}
	// And no file of its own, or the two could be written apart and a
	// crash between the writes would lose a record that had left the
	// queue and not yet arrived in the DLQ.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	for _, n := range names {
		if strings.HasPrefix(n, "jobs__dlq") {
			t.Errorf("the dead-letter channel has a file of its own: %v", names)
		}
	}
}

// mustOffer hands out every available record, failing the test if the
// store could not. The bound is what a caller with room to spare would
// pass; these tests are about what comes back, not about the bound.
func mustOffer(t *testing.T, q QueueStore) []store.Offered {
	t.Helper()
	out, err := q.Offer(1024, time.Now())
	if err != nil {
		t.Fatalf("offering work: %v", err)
	}
	return out
}

// memLen reads a memory store's record count. It is not on the store
// interfaces: counting costs a scan in a store that keeps its records on a
// disk, and nothing in the broker asks.
func memLen(t *testing.T, s LogStore) int {
	t.Helper()
	lg, ok := s.(*store.Log)
	if !ok {
		t.Fatalf("expected a memory log, got %T", s)
	}
	return lg.Len()
}

// refusingLatest is a latest store that will not take a value, which is
// what a provider at its max_bytes looks like from here.
type refusingLatest struct {
	*store.Latest
	err error
}

func (r *refusingLatest) Set(rec store.Record) (store.Record, error) { return rec, r.err }

// MQTT-3.3.2-5, at the one place a value could outlive it across a
// restart: the settle would otherwise move an expired retained value into
// a `latest` channel, where the operator's clock alone deletes - state
// MQTT had already discarded, resurrected with no expiry left to kill it.
func TestSettleDeletesAnExpiredRetainedValueRatherThanMovingIt(t *testing.T) {
	b := durableBroker(t, t.TempDir())
	rt := store.NewLatest()
	if _, err := rt.Set(store.Record{Topic: "state/room", Payload: []byte("v"),
		Timestamp: time.Now().Add(-2 * time.Hour), MessageExpiry: 3600}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b.SetRetained("local", rt, 0)

	if err := b.SettleRetained(); err != nil {
		t.Fatalf("settle: %v", err)
	}

	got, err := b.latest["state"].Match(func(string) bool { return true })
	if err != nil {
		t.Fatalf("read the channel: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("an expired retained value was moved into the channel as %+v; "+
			"it stopped being state when its expiry ran out", got[0].Topic)
	}
	if _, ok, err := rt.Get("state/room"); err != nil || ok {
		t.Errorf("the expired value is still in the retained store (held=%v, err=%v), "+
			"want it deleted as the sweep would have", ok, err)
	}
}

// RFC 0002 "Retained messages on a broadcast topic": a stored topic that a
// channel comes to claim is moved or dropped at startup, and never held in
// both places.
//
// This is the migration RFC 0003 recommends, seen from the broker's side:
// an operator whose fleet has been publishing retained to `state/...`
// names a `latest` channel for that prefix, and the values they already
// have should be there when it starts.
func TestRetainedMessagesSettleAgainstTheChannels(t *testing.T) {
	// A record for a topic each of the configured channels claims, plus one
	// that stays broadcast.
	seed := func(b *Broker) {
		rt := store.NewLatest()
		for _, topic := range []string{
			"state/room",        // a latest channel: moved
			"events/thing",      // an append channel: dropped
			"jobs/work",         // a queue: dropped
			"jobs/__dlq/failed", // a dead-letter channel: dropped
			"loose/thing",       // no channel claims it: left alone
		} {
			if _, err := rt.Set(store.Record{Topic: topic, Payload: []byte("v")}); err != nil {
				t.Fatalf("seed %s: %v", topic, err)
			}
		}
		b.SetRetained("local", rt, 0)
	}

	t.Run("moved into a latest channel, dropped for the rest", func(t *testing.T) {
		b := durableBroker(t, t.TempDir())
		seed(b)

		if err := b.SettleRetained(); err != nil {
			t.Fatalf("settle: %v", err)
		}

		// The one a latest channel claims is now in that channel.
		got, err := b.latest["state"].Match(func(topic string) bool { return topic == "state/room" })
		if err != nil {
			t.Fatalf("read the channel: %v", err)
		}
		if len(got) != 1 || string(got[0].Payload) != "v" {
			t.Errorf("the latest channel holds %d value(s) for state/room, want 1", len(got))
		}

		// **And it is a record, which means it has an identity.** A retained
		// broadcast value has no Message ID - nothing ever gave it one,
		// because a broadcast publish becomes no record - so moved in as it
		// stood it reached consumers with `saguin-id` empty, and a consumer
		// deduplicating on identity saw every moved value as the same
		// message (invariant 8). The move is where a value becomes a record,
		// so it is where the identity is minted.
		if len(got) == 1 && got[0].MessageID == "" {
			t.Error("the moved value carries no Message ID, so a consumer deduplicating " +
				"on identity cannot tell it from every other one")
		}

		// And the retained store holds only what no channel claims.
		left, err := b.retained.Match(func(string) bool { return true })
		if err != nil {
			t.Fatalf("read the retained store: %v", err)
		}
		if len(left) != 1 || left[0].Topic != "loose/thing" {
			var topics []string
			for _, r := range left {
				topics = append(topics, r.Topic)
			}
			t.Errorf("the retained store still holds %v, want only loose/thing", topics)
		}
	})

	t.Run("a destination that refuses stops the broker", func(t *testing.T) {
		b := durableBroker(t, t.TempDir())
		seed(b)
		b.latest["state"] = &refusingLatest{Latest: store.NewLatest(), err: store.ErrFull}

		err := b.SettleRetained()
		if err == nil {
			t.Fatal("settling succeeded against a channel that cannot hold the value")
		}
		if !strings.Contains(err.Error(), "state") {
			t.Errorf("the error does not name the channel: %v", err)
		}

		// Refused, not dropped: an operator who raises the bound and starts
		// again must not have lost the value in the meantime.
		left, err := b.retained.Match(func(topic string) bool { return topic == "state/room" })
		if err != nil {
			t.Fatalf("read the retained store: %v", err)
		}
		if len(left) != 1 {
			t.Error("the value was removed from the retained store although the move failed, " +
				"so raising the bound and restarting would not bring it back")
		}
	})
}

// RFC 0004 "The file format": the broker reads only the format it writes. A
// snapshot of any other is refused by name - the file, the format it holds,
// the build that wrote it and the format this broker reads - and the broker
// does not start on it rather than starting empty, which would be
// indistinguishable from a fresh install and destroy the evidence (invariant
// 14). Nothing converts one.
//
// The store's decoders are held to the rule by
// TestEveryOtherFormatIsRefusedByName; this is the broker's start, which is
// where a refusal could still be swallowed into an empty channel.
func TestABrokerDoesNotStartOnASnapshotOfAnotherFormat(t *testing.T) {
	dir := t.TempDir()
	if err := store.NewDir(dir, "0.1.0-test").Save([]*store.Snapshot{{
		Writer: "0.1.0-test", WrittenAt: time.Unix(1700000000, 0),
		Channels: []store.ChannelState{{
			Name: "events", Kind: store.KindAppend, Next: 41, Floor: 1,
			Positions: []store.Position{{Reader: store.MQTTReader("sensor-gateway"), Offset: 40,
				LastSeen: time.Now(), ExpiresIn: time.Hour}},
		}},
	}}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// The control: the file as written starts the broker, so the refusal
	// below is the format's doing.
	if _, err := durableBroker(t, dir).LoadSnapshots(); err != nil {
		t.Fatalf("a broker refused the snapshot it writes: %v", err)
	}

	name, err := store.SnapshotFileName("events")
	if err != nil {
		t.Fatalf("snapshot file name: %v", err)
	}
	path := filepath.Join(dir, name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}
	// The version sits at bytes 8:12 of every format, and the checksum
	// trails and covers everything before it - so both move together or the
	// file is damaged rather than of another format.
	binary.LittleEndian.PutUint32(raw[8:12], 1)
	binary.LittleEndian.PutUint32(raw[len(raw)-4:],
		crc32.Checksum(raw[:len(raw)-4], crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("rewrite the snapshot: %v", err)
	}

	_, err = durableBroker(t, dir).LoadSnapshots()
	if err == nil {
		t.Fatal("a broker started on a snapshot of another format: its channel came back as " +
			"something rather than being refused, or empty, which reads as a fresh install")
	}
	for _, want := range []string{path, "format 1,", "0.1.0-test", "this broker reads format"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
}

// **The move is where a value becomes a record, so it is where the record
// rule applies.**
//
// The retained store keeps what a publisher sent verbatim, reserved names
// included, because a broadcast message reaches its subscribers as the
// packet it arrived as and the substrate's own fan-out carries those names
// too - a stored copy that differed from the live one would be the defect
// rather than the fix. A record is held to a different rule: the reserved
// prefix is the broker's, and a publisher may not hand a consumer metadata
// it will read as the broker's word.
//
// Between those two rules is the move, and it applied neither: a retained
// value carrying a forged `saguin-offset` and `saguin-dlq-reason` arrived
// in the channel with them beside the broker's own - one record with two
// offsets, two timestamps, two ids and a dead-letter reason on a value that
// was never dead-lettered. A consumer reading properties into a map keeps
// whichever came last, which is the forged one.
func TestAMovedRetainedValueIsHeldToTheRecordRule(t *testing.T) {
	b := durableBroker(t, t.TempDir())
	rt := store.NewLatest()
	if _, err := rt.Set(store.Record{
		Topic:   "state/forged",
		Payload: []byte("v"),
		Headers: []store.Header{
			{Key: "saguin-id", Value: "chosen-by-publisher"},
			{Key: "saguin-offset", Value: "999"},
			{Key: "saguin-dlq-reason", Value: "forged"},
			{Key: "saguin-timestamp", Value: "1"},
			{Key: "mine", Value: "kept"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b.SetRetained("local", rt, 0)

	if err := b.SettleRetained(); err != nil {
		t.Fatalf("settle: %v", err)
	}
	got, err := b.latest["state"].Match(func(topic string) bool { return topic == "state/forged" })
	if err != nil {
		t.Fatalf("read the channel: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the channel holds %d values for state/forged, want 1", len(got))
	}
	r := got[0]

	// The publisher's own id is lifted into the identity rather than minted
	// beside - the same thing the publish path does with it.
	if r.MessageID != "chosen-by-publisher" {
		t.Errorf("Message ID is %q, want the publisher's own: a `saguin-id` it sent is its "+
			"idempotency key, and minting a second identity beside it leaves one record "+
			"carrying two", r.MessageID)
	}
	for _, h := range r.Headers {
		if strings.HasPrefix(h.Key, "saguin-") {
			t.Errorf("the moved record still carries %q=%q, which a consumer reads as the "+
				"broker's word about a value the broker never said it about",
				h.Key, h.Value)
		}
	}
	// And what was actually the publisher's survives.
	var kept bool
	for _, h := range r.Headers {
		if h.Key == "mine" && h.Value == "kept" {
			kept = true
		}
	}
	if !kept {
		t.Error("the move dropped the publisher's own property along with the reserved ones")
	}
}
