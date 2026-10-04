package broker

import (
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// **A record carries the bridge it arrived on, and nothing on the wire can
// say otherwise.** That mark is the whole of saguin's loop guard: an `out`
// rule skips every record carrying one, so a record that came in over a
// bridge is never sent back out over a bridge (RFC 0002 "Bridges").
//
// It is asserted here rather than through a live link because the rule is
// about *who published*, and this is the only layer where that is still in
// hand. A test driving two brokers would prove the link works and say
// nothing about a forged property, which is the half that matters.
func TestARecordIsMarkedWithTheBridgeItArrivedOn(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append, Storage: "mem"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := reg.Get("events")

	// A publish carrying a forged mark, which is what a foreign broker
	// echoing saguin's own property back at it looks like from here.
	forged := packets.Packet{
		TopicName: "events/x",
		Payload:   []byte("hello"),
		Properties: packets.Properties{User: []packets.UserProperty{
			{Key: "saguin-bridge", Val: "not-a-real-bridge"},
			{Key: "tag", Val: "kept"},
		}},
	}

	t.Run("from a client, carrying a forged mark", func(t *testing.T) {
		rec := b.record(forged, c, "", "a-device")
		if rec.Bridge != "" {
			t.Errorf("a client's publish was marked %q, so anything on the wire can "+
				"make saguin refuse to forward its own records", rec.Bridge)
		}
		for _, h := range rec.Headers {
			if h.Key == "saguin-bridge" {
				t.Errorf("the forged property was stored as a header: %+v", rec.Headers)
			}
		}
		// The publisher is the connection, never the packet - and it is a
		// field rather than a header, so nothing above stamps it onto a
		// delivery. See store.Record.Publisher.
		if rec.Publisher != "a-device" {
			t.Errorf("the record says %q published it, want a-device", rec.Publisher)
		}
		// The ordinary header is untouched, or this test would pass on a
		// record that had lost everything.
		if len(rec.Headers) != 1 || rec.Headers[0].Key != "tag" {
			t.Fatalf("the publisher's own headers did not survive: %+v", rec.Headers)
		}
	})

	t.Run("from a bridge", func(t *testing.T) {
		rec := b.record(forged, c, "head-office", "bridge:head-office")
		if rec.Bridge != "head-office" {
			t.Errorf("a bridge's publish is marked %q, want head-office - unmarked, an "+
				"out rule forwards it back where it came from", rec.Bridge)
		}
	})
}

// **Dead-lettering clears the mark, because it is this broker stating a new
// fact rather than relaying somebody's record.**
//
// Carried, the mark would stop a `__dlq` shipping anywhere - and it would
// do it silently, for exactly the records an operator most needs out of the
// box.
func TestADeadLetterIsNotMarkedWithTheBridgeItsWorkArrivedOn(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, Storage: "mem", VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := reg.Get("jobs")

	// Work that arrived over a bridge and then failed.
	spent := store.Item{
		Record: store.Record{
			MessageID: "m-1", Topic: "jobs/resize", Payload: []byte("job"),
			Bridge: "head-office",
		},
		Attempts: 3,
	}
	out := b.deadLettered(c, "attempts_exhausted")(spent)

	if out.Bridge != "" {
		t.Errorf("the dead letter is marked %q, so an out rule would skip it and the "+
			"failed work could never leave this broker", out.Bridge)
	}
	// It is still the same message, or clearing the mark would have cost
	// the link between the failure and the work (invariant 8).
	if out.MessageID != "m-1" {
		t.Errorf("the dead letter's message id is %q, want m-1", out.MessageID)
	}
}

// **The retained store is the one thing a broadcast topic keeps, so it
// keeps the mark too.**
//
// Broadcast stores nothing else - the mark rides the packet through the
// fan-out there - but a retained value outlives the connection that set it
// and is handed to whoever subscribes next. Unmarked, an outbound rule fed
// a retained pass would hand the peer its own retained set back, once per
// restart, for ever.
//
// It is also the ordering trap keepRetained already documents: SetProps
// writes a whole set, so a field assigned before it and left out of that
// set is zeroed again in silence. This is what says the mark survived it.
func TestARetainedBroadcastValueKeepsTheBridgeItArrivedOn(t *testing.T) {
	reg, err := channel.NewRegistry(nil)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	t.Cleanup(func() { _ = srv.Close() })

	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.SetServer(srv)
	kept := store.NewLatest()
	kept.SetQuota(store.NewQuota(0, 0))
	b.SetRetained("mem", kept, 0)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Retain: true},
		TopicName:   "weather/hallway",
		Payload:     []byte("21.5"),
		Properties:  packets.Properties{ContentType: "text/plain"},
	}

	// From a bridge: registered the way a real one is, so the lookup under
	// test is the same lookup the publish path makes.
	bc := b.NewBridgeClient("head-office")
	if err := b.keepRetained(bc.cl, pk); err != nil {
		t.Fatalf("keep retained from a bridge: %v", err)
	}
	got, had, err := kept.Get("weather/hallway")
	if err != nil || !had {
		t.Fatalf("the value was not kept: had=%v err=%v", had, err)
	}
	if got.Bridge != "head-office" {
		t.Errorf("a retained value from a bridge is marked %q, want head-office - unmarked, "+
			"an out rule hands the peer its own retained set back on every restart", got.Bridge)
	}
	// The value itself is intact, or this would pass on a record that had
	// lost everything else to the same ordering trap.
	if got.ContentType != "text/plain" || string(got.Payload) != "21.5" {
		t.Errorf("the retained value did not survive: %+v", got)
	}

	// And from an ordinary client it is unmarked, or every retained value
	// on the broker would be skipped by an out rule.
	ordinary := srv.NewClient(nil, mqtt.LocalListener, "device-7", false)
	pk.TopicName = "weather/kitchen"
	if err := b.keepRetained(ordinary, pk); err != nil {
		t.Fatalf("keep retained from a client: %v", err)
	}
	plain, had, err := kept.Get("weather/kitchen")
	if err != nil || !had {
		t.Fatalf("the client's value was not kept: had=%v err=%v", had, err)
	}
	if plain.Bridge != "" {
		t.Errorf("a client's retained value is marked %q, so an out rule would refuse to "+
			"forward records this broker originated", plain.Bridge)
	}
}

// **A filter that crosses a queue is served everything else it matches and
// nothing from the queue** - invariant 11, and after this rework it is the
// only guard on it.
//
// `channel:` used to be on every bridge rule, so a queue could be refused
// by name at startup. It is gone, because it made the bridge the one client
// that had to know where the channels are. What replaced it is the rule
// every other subscriber already lives under: a filter reaches the append
// and latest channels it matches, and a queue admits exactly one
// subscription form which no rule's filter may name.
//
// The crossing case is the one worth pinning. The exact form
// (`$saguin/queue/<name>`) is refused at startup and tested in
// internal/config; this is the *quiet* half - a rule that is accepted, runs,
// and simply is not served a queue's records. Nothing logs it and nothing
// refuses it, so without this test nothing would notice a bridge draining a
// work queue into somebody's peer.
func TestAnOutFilterCrossingAQueueDrainsEverythingButTheQueue(t *testing.T) {
	// One topic space, three channels under it - which is what makes the
	// filter below cross all three at once.
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "orders", Type: channel.Append, Filter: "orders/+/events/#", Storage: "mem"},
		{Name: "carts", Type: channel.Latest, Filter: "orders/+/cart", Storage: "mem"},
		{Name: "fulfil", Type: channel.Queue, Filter: "orders/+/work", Storage: "mem",
			VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bc := &BridgeClient{b: b, name: "head-office"}

	var served []string
	for _, s := range bc.Sources("orders/#") {
		served = append(served, s.Name)
	}

	// Everything else it matches, and the derived dead-letter channel with
	// it: a `__dlq` is an ordinary append channel, and shipping failed work
	// out is allowed.
	want := []string{"carts", "fulfil__dlq", "orders"}
	if len(served) != len(want) {
		t.Fatalf("the rule reads %v, want %v", served, want)
	}
	for i := range want {
		if served[i] != want[i] {
			t.Errorf("source %d is %q, want %q", i, served[i], want[i])
		}
	}

	// And not the queue, which is the whole point. A filter reaching it
	// would make the bridge a worker: every job it took would be leased to
	// a link that never acknowledges, so the work redelivers, spends its
	// attempts and dead-letters because a bridge read it.
	for _, s := range served {
		if s == "fulfil" {
			t.Fatal("an outbound rule reads a queue, so it drains work into the peer and " +
				"the workers that were meant to do it never see it (invariant 11)")
		}
	}

	// The other half of the promise: the queue is crossed rather than
	// absent, or this test would pass on a filter that matched nothing at
	// all and prove nothing about queues.
	crossed := false
	for _, c := range reg.ResolveFilters("orders/#") {
		if c.Name == "fulfil" {
			crossed = true
		}
	}
	if !crossed {
		t.Fatal("the filter does not reach the queue's topic space at all, so this test " +
			"says nothing about what happens when it does")
	}
}

// watchedBroker is a broker an outbound rule can watch: a bridge client
// needs the server it is made on.
func watchedBroker(t *testing.T) *Broker {
	t.Helper()
	b := metricsBroker(t)
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	t.Cleanup(func() { _ = srv.Close() })
	b.SetServer(srv)
	return b
}

// **A broadcast publish builds its record only for somebody watching.**
// Building one takes a UUID and copies the payload and properties, which a
// broker with no outbound rule on a broadcast topic hands to nobody. A
// watcher that was removed leaves an empty slot, and that is nobody too.
func TestABroadcastRecordIsBuiltOnlyWhenSomebodyWatches(t *testing.T) {
	b := watchedBroker(t)
	bc := b.NewBridgeClient("head-office")
	cl := &mqtt.Client{ID: "p"}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "sensors/hall",
		Payload:     make([]byte, 128),
	}
	publish := func() {
		if _, err := b.OnPublish(cl, pk); err != nil {
			t.Fatalf("a broadcast publish was refused: %v", err)
		}
	}
	allocs := func() float64 { return testing.AllocsPerRun(200, publish) }

	unwatched := allocs()
	var seen atomic.Int64
	stop := bc.WatchBroadcast(func(store.Record) { seen.Add(1) })
	watched := allocs()
	stop()
	removed := allocs()
	t.Logf("allocations a publish: %.0f unwatched, %.0f watched, %.0f after the watcher left",
		unwatched, watched, removed)

	// The watched run is what proves the record is built at all, and that
	// the publishes counted here reached the watcher.
	if seen.Load() < 200 {
		t.Fatalf("the watcher was handed %d publishes, want at least 200", seen.Load())
	}
	// A record is eight allocations; the UUID alone is seven.
	for name, got := range map[string]float64{"with nobody watching": unwatched,
		"after the only watcher left": removed} {
		if got > watched-7 {
			t.Errorf("a broadcast publish %s makes %.0f allocations, and %.0f with a "+
				"watcher: its record is still being built", name, got, watched)
		}
	}
}

// **Every watcher is handed the one record, including one behind an empty
// slot.** Building the record at the first watcher found must not stop at a
// watcher that was removed, nor give each watcher a record of its own - two
// watchers told one publish under two message ids would forward it twice.
func TestEveryBroadcastWatcherIsHandedTheOneRecord(t *testing.T) {
	b := watchedBroker(t)
	bc := b.NewBridgeClient("head-office")
	stopFirst := bc.WatchBroadcast(func(store.Record) {})
	stopFirst()

	var mu sync.Mutex
	var got [2][]store.Record
	for i := range got {
		stop := bc.WatchBroadcast(func(r store.Record) {
			mu.Lock()
			got[i] = append(got[i], r)
			mu.Unlock()
		})
		defer stop()
	}

	payload := []byte("hello")
	if _, err := b.OnPublish(&mqtt.Client{ID: "a-device"}, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "sensors/hall",
		Payload:     payload,
	}); err != nil {
		t.Fatalf("a broadcast publish was refused: %v", err)
	}
	payload[0] = 'j' // the publisher's buffer, reused

	for i, recs := range got {
		if len(recs) != 1 {
			t.Fatalf("watcher %d was handed %d records for one publish, want 1", i+1, len(recs))
		}
		r := recs[0]
		if r.Topic != "sensors/hall" || string(r.Payload) != "hello" || r.Publisher != "a-device" ||
			r.Bridge != "" || r.MessageID == "" {
			t.Errorf("watcher %d was handed %+v", i+1, r)
		}
	}
	if got[0][0].MessageID != got[1][0].MessageID {
		t.Errorf("one publish reached two watchers as %s and %s", got[0][0].MessageID, got[1][0].MessageID)
	}
}

// **A watcher is handed every publish that starts after it was added.**
// Whether a record is built is decided under watchMu, the lock
// WatchBroadcast takes to add a watcher; a decision made outside it could
// see no watcher for a publish that started after one was added, and that
// record would never reach the rule - broadcast stores nothing to read it
// back from. Scheduling-dependent, so repeated; run it under -race.
func TestAWatcherAddedMidStreamMissesNothingPublishedAfterIt(t *testing.T) {
	const trials, after = 50, 50
	for trial := range trials {
		b := watchedBroker(t)
		bc := b.NewBridgeClient("head-office")
		var started atomic.Int64
		limit := atomic.Int64{}
		limit.Store(1 << 62)
		done := make(chan error, 1)
		go func() {
			cl := &mqtt.Client{ID: "p"}
			for {
				n := started.Add(1)
				if n > limit.Load() {
					done <- nil
					return
				}
				if _, err := b.OnPublish(cl, packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish},
					TopicName:   "sensors/hall",
					Payload:     []byte(strconv.FormatInt(n, 10)),
				}); err != nil {
					done <- err
					return
				}
			}
		}()
		for started.Load() < 20 {
			runtime.Gosched()
		}
		var mu sync.Mutex
		seen := map[int64]bool{}
		stop := bc.WatchBroadcast(func(r store.Record) {
			n, _ := strconv.ParseInt(string(r.Payload), 10, 64)
			mu.Lock()
			seen[n] = true
			mu.Unlock()
		})
		from := started.Load() + 1 // the first publish to start after the watcher was added
		limit.Store(from + after)
		if err := <-done; err != nil {
			t.Fatalf("trial %d: a broadcast publish was refused: %v", trial, err)
		}
		stop()
		mu.Lock()
		for n := from; n <= from+after; n++ {
			if !seen[n] {
				t.Errorf("trial %d: publish %d started after the watcher was added and never reached it", trial, n)
			}
		}
		mu.Unlock()
	}
}
