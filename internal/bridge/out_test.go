package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// peer is a stand-in for the far end: it records what was published and
// answers each publish with whatever reason code the test set.
type peer struct {
	mu   sync.Mutex
	got  []string
	code map[string]byte // topic -> reason code, absent means success
}

func (p *peer) Publish(_ context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = append(p.got, pk.Topic+" "+string(pk.Payload))
	return &paho.PublishResponse{ReasonCode: p.code[pk.Topic]}, nil
}

func (p *peer) sent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.got...)
}

// reader is a stand-in for the broker an out rule reads.
type reader struct {
	mu        sync.Mutex
	sources   map[string][]channel.Source
	records   map[string][]store.Record
	floor     map[string]uint64
	positions map[string]uint64
	saved     map[string]uint64
	lost      []string
	lostAs    []lostPosition
	wake      chan struct{}
	// latest is the rules watching each latest channel, which feed hands
	// each value to as the broker's publish would.
	latest map[string][]func(store.Record)
}

func newReader() *reader {
	return &reader{
		sources: map[string][]channel.Source{}, records: map[string][]store.Record{},
		floor: map[string]uint64{}, positions: map[string]uint64{},
		saved: map[string]uint64{}, wake: make(chan struct{}, 1),
	}
}

func (r *reader) Sources(filter string) []channel.Source { return r.sources[filter] }

func (r *reader) ReadFrom(name string, offset uint64, max int) ([]store.Record, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f := r.floor[name]; f > offset {
		return nil, f, store.ErrBelowFloor
	}
	var out []store.Record
	for _, rec := range r.records[name] {
		if rec.Offset >= offset {
			out = append(out, rec)
		}
		if len(out) == max {
			break
		}
	}
	return out, r.floor[name], nil
}

func (r *reader) Head(string) uint64 { return 1 }

// feed hands a value to every rule watching a latest channel, as the broker's
// publishLatest does, and reports how many were watching.
func (r *reader) feed(name string, rec store.Record) int {
	r.mu.Lock()
	var fns []func(store.Record)
	fns = append(fns, r.latest[name]...)
	r.mu.Unlock()
	for _, fn := range fns {
		fn(rec)
	}
	return len(fns)
}

// watching reports whether a rule is watching a latest channel.
func (r *reader) watching(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.latest[name]) > 0
}

func (r *reader) Position(name, _ string) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.positions[name]
	return at, ok, nil
}

func (r *reader) SavePosition(name, _ string, offset uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved[name] = offset
	return nil
}

func (r *reader) PositionLost(name, reader string, position, floor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lost = append(r.lost, name)
	// Kept, not discarded: the broker names this reader and these two
	// offsets on /v1/operations/position-lost, and a fake that threw them
	// away let the call site pass them in either order for ever.
	r.lostAs = append(r.lostAs, lostPosition{channel: name, reader: reader,
		position: position, floor: floor})
}

// lostPosition is one call's arguments, so a test can assert what the rule
// actually told the broker rather than only that it told it something.
type lostPosition struct {
	channel, reader string
	position, floor uint64
}

func (r *reader) Watch(string) (<-chan struct{}, func()) { return r.wake, func() {} }

func (r *reader) WatchLatest(name string, fn func(store.Record)) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest == nil {
		r.latest = map[string][]func(store.Record){}
	}
	r.latest[name] = append(r.latest[name], fn)
	return func() {}
}
func (r *reader) WatchBroadcast(func(store.Record)) func() { return func() {} }

func (r *reader) savedAt(name string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saved[name]
}

// outBridge builds a bridge with one out rule over a filter, and runs its
// drain once against the stand-in peer.
func outBridge(t *testing.T, filter string, rd *reader) (*outRule, *peer) {
	t.Helper()
	cfg := rules(t, []string{"events"},
		"      - filter: "+filter+"\n        direction: both\n")
	b, _ := logging(t, cfg, &fake{})
	b.rd = rd
	return &outRule{b: b, rule: b.cfg.Topics[0]}, &peer{code: map[string]byte{}}
}

func rec(offset uint64, topic, payload string) store.Record {
	return store.Record{Offset: offset, MessageID: "m", Topic: topic, Payload: []byte(payload)}
}

// **The position advances only through the contiguous acknowledged
// prefix.** A peer refusing one record in the middle of a window must leave
// the position before it, or everything behind that record is skipped
// silently and for ever - invariant 1's failure reached by arithmetic.
func TestTheOutPositionStopsAtTheFirstRecordThePeerDidNotTake(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"), rec(2, "events/b", "two"),
		rec(3, "events/c", "three"), rec(4, "events/d", "four"),
	}
	o, p := outBridge(t, "events/#", rd)
	// The third is refused for a reason that can stop being true.
	p.code["events/c"] = 0x97

	moved, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if !moved {
		t.Fatal("nothing moved, so this test proved nothing about where the position stopped")
	}
	if next != 3 {
		t.Errorf("the position moved to %d, want 3 - it must stop at the first record the "+
			"peer did not take, or record 3 is never sent and nothing says so", next)
	}
	if got := rd.savedAt("events"); got != 3 {
		t.Errorf("the stored position is %d, want 3", got)
	}
}

// A record the peer will never take is skipped rather than held, because
// holding it stops every record behind it for ever - and the skip is
// counted and logged, because it is loss.
func TestARecordThePeerWillNeverTakeIsSkippedAndCounted(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"), rec(2, "events/bad", "two"), rec(3, "events/c", "three"),
	}
	o, p := outBridge(t, "events/#", rd)
	p.code["events/bad"] = 0x99 // payload format invalid: answered every time

	_, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if next != 4 {
		t.Errorf("the position is %d, want 4: a record the peer can never take must not "+
			"hold the ones behind it", next)
	}
	if n := o.b.peerRefused.Load(); n != 1 {
		t.Errorf("peerRefused counts %d, want 1 - a skip nobody counts is loss nobody sees", n)
	}
	// And the ones around it did cross - by set rather than in order,
	// because *which* three is the point and the order across topics is not
	// promised. An `out` rule serialises per topic, which is the order RFC
	// 0003 makes a promise about, and pipelines across them; these three
	// are on three different topics, so their order on the wire is whichever
	// goroutine reached the link first. The sequential loop gave a total
	// order for free and this test used to assert it.
	want := []string{"events/a one", "events/bad two", "events/c three"}
	got := p.sent()
	slices.Sort(got)
	sorted := slices.Clone(want)
	slices.Sort(sorted)
	if !slices.Equal(got, sorted) {
		t.Errorf("the peer saw %v, want exactly %v in any order", got, want)
	}
}

// **A record that arrived over a bridge is never forwarded out over a
// bridge**, which is the loop guard. It is a blanket rather than a
// comparison against this bridge's own name: any mark permitting a second
// hop cannot detect a cycle.
func TestARecordThatArrivedOverABridgeIsNotForwarded(t *testing.T) {
	rd := newReader()
	local := rec(1, "events/a", "mine")
	bridged := rec(2, "events/b", "theirs")
	bridged.Bridge = "some-other-link"
	rd.records["events"] = []store.Record{local, bridged, rec(3, "events/c", "mine too")}

	o, p := outBridge(t, "events/#", rd)
	_, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)

	// By set rather than in order: what this test is about is *which*
	// records crossed, and an `out` rule pipelines across topics, so the
	// order of two records on two topics is whichever reached the link
	// first. Per-topic order is the promise and is asserted where it is the
	// point.
	want := []string{"events/a mine", "events/c mine too"}
	got := p.sent()
	slices.Sort(got)
	sorted := slices.Clone(want)
	slices.Sort(sorted)
	if !slices.Equal(got, sorted) {
		t.Fatalf("the peer saw %v, want exactly %v in any order - a bridged record "+
			"was sent back out", got, want)
	}
	if n := o.b.loopsSkipped.Load(); n != 1 {
		t.Errorf("loops skipped counts %d, want 1", n)
	}
	// It is still carried: a record this rule will never forward must not
	// stop the ones behind it.
	if next != 4 {
		t.Errorf("the position is %d, want 4 - a skipped record held the window", next)
	}
}

// Retention passing an out rule's position is invariant 1's own case: it
// resumes at the floor, says how much went, and counts it against the
// channel - the same treatment an MQTT consumer gets.
func TestRetentionPassingTheOutPositionIsCountedAndResumed(t *testing.T) {
	rd := newReader()
	rd.floor["events"] = 50
	rd.records["events"] = []store.Record{rec(50, "events/a", "survivor")}

	o, p := outBridge(t, "events/#", rd)
	moved, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 10, false)
	if !moved || next != 50 {
		t.Errorf("resumed at %d moved=%v, want 50 and true", next, moved)
	}
	if len(rd.lost) != 1 || rd.lost[0] != "events" {
		t.Errorf("the loss was counted as %v, want one against events", rd.lost)
	}
	if got := rd.savedAt("events"); got != 50 {
		t.Errorf("the stored position is %d, want the floor 50", got)
	}
	// **What it told the broker, and not only that it told it.** The broker
	// records these on /v1/operations/position-lost under the rule's own
	// name, and until this was asserted the two offsets could be handed over
	// in either order with every test in both packages still passing: the
	// broker's own guard drops a row whose floor is not above its position,
	// so the route would simply have shown no bridge rows at all.
	if len(rd.lostAs) != 1 {
		t.Fatalf("the rule reported %d losses in detail, want 1", len(rd.lostAs))
	}
	got := rd.lostAs[0]
	want := lostPosition{channel: "events", reader: "bridge:head-office", position: 10, floor: 50}
	if got != want {
		t.Errorf("the rule reported %+v, want %+v.\n\tThe position it had reached is 10 "+
			"and the floor that passed it is 50, so it lost 40 records; reversed, the "+
			"broker records nothing and the route is silent about every bridge",
			got, want)
	}
}

// The window is the smaller of what the peer will take and what this broker
// was told to send. A peer that says nothing means 65,535 by the
// specification, which is no bound at all - so the configured value has to
// be the one that wins there (invariant 13).
func TestTheOutWindowIsBoundedByTheOperatorsOwnNumber(t *testing.T) {
	for _, tc := range []struct {
		name             string
		peer, configured int
		want             int
	}{
		{"the peer is smaller", 5, 20, 5},
		{"the configured value is smaller", 100, 20, 20},
		{"the peer said nothing", 0, 20, 20},
		{"neither is usable", 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := outWindow(uint16(tc.peer), tc.configured); got != tc.want {
				t.Errorf("window is %d, want %d", got, tc.want)
			}
		})
	}
}

// The reason codes, split the way the inbound direction splits its
// refusals: the list is of what may be retried, and anything unclassified
// is permanent so that one record cannot hold a link for ever.
//
// **`0x87` is on the retried list and is the one that is not obvious.** A
// peer's authorization is a file somebody edits: the grant is missing while
// the mistake stands and back the minute it is fixed, which is the shape of
// a full store rather than of a malformed packet. Classified permanent it
// skipped - and advanced the position past - every record published during
// a misconfiguration, permanently, for a condition that heals itself.
func TestAnUnclassifiedRefusalIsPermanent(t *testing.T) {
	for code, want := range map[byte]outcome{
		0x00: sent, 0x10: sent,
		0x97: contingent, 0x80: contingent, 0x87: contingent,
		0x99: permanent, 0x90: permanent, 0x2B: permanent, 0x83: permanent,
	} {
		if got := readOutcome(code); got != want {
			t.Errorf("0x%02X read as %v, want %v", code, got, want)
		}
	}
}

// **A refusal streak is one line when it starts and one when it clears, and
// both are readable at `warn`.**
//
// This test read the log at `info` once, *because* the clear was written
// there - which is precisely the defect it now guards against. An operator
// at `warn`, a level `broker.log_level` offers, was told the peer was
// refusing this bridge's records and never told it had stopped: the log
// asserted a broken link for the life of the process. Reading at `warn`
// here is what fails if a clear is ever lowered again.
//
// A peer answering `0x97` to every publish is a bridge that is connected
// and shipping nothing - the worst shape for an operator, because
// `connected` reads 1 and the counter is merely flat. One line per refused
// record would bury that fact in the evidence for it, so the discipline is
// the link's own: say it once, and say when it stops.
func TestARefusalStreakIsLoggedOnceAndClearedOnce(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"), rec(2, "events/a", "two"), rec(3, "events/a", "three"),
	}
	cfg := rules(t, []string{"events"},
		"      - filter: events/#\n        direction: both\n")
	b, logged := loggingAt(t, cfg, &fake{}, slog.LevelWarn)
	b.rd = rd
	o := &outRule{b: b, rule: b.cfg.Topics[0]}
	p := &peer{code: map[string]byte{"events/a": 0x97}}

	// Three records, every one refused for a reason that can stop being true.
	o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if n := strings.Count(logged(), "the peer is refusing this bridge's records"); n != 1 {
		t.Errorf("the refusal was logged %d times for 3 records, want 1 - a line each "+
			"buries the fact in the evidence for it:\n%s", n, logged())
	}
	// And nothing moved, which is the other half: a contingent refusal
	// leaves the position where it was so the records are offered again.
	if got := rd.savedAt("events"); got != 0 {
		t.Errorf("the position was stored as %d, want unmoved", got)
	}

	// The peer starts taking them, and that is said once - at `warn`, the
	// level the refusal was said at, because a reader filtering by severity
	// asked about the problem and its end is the same fact.
	delete(p.code, "events/a")
	o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if n := strings.Count(logged(), "answering this bridge's records again"); n != 1 {
		t.Errorf("the recovery was logged %d times at `warn`, want 1 - a clear written "+
			"below the line it retracts leaves an operator at this level holding a "+
			"warning that stopped being true:\n%s", n, logged())
	}
	if got := rd.savedAt("events"); got != 4 {
		t.Errorf("the position is %d after the peer took all three, want 4", got)
	}

	// A second run of successes says nothing more, or "cleared" would be a
	// line per record in the other direction.
	o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if n := strings.Count(logged(), "answering this bridge's records again"); n != 1 {
		t.Errorf("the recovery line was written again on a link that never stopped "+
			"working: %d times\n%s", n, logged())
	}
}

// **A record an out rule can build no topic for is counted and said out
// loud**, rather than dropped into the gap between two brokers.
//
// The check itself is not this file's: it lives in config's Match, which
// both directions call, so the reserved `$` space is closed on the way out
// for the same reason it is on the way in - a `$saguin/…` topic built from
// what a local publisher sent would forge this broker's control topics at a
// saguin peer, and at a foreign one would write into a namespace saguin does
// not own. What is asserted here is the half that was missing: the record is
// on this broker and will never be on the other, and nothing at the far end
// can notice it missing, so the drop is a counter and a line rather than a
// silent return.
func TestARecordAnOutRuleCannotBuildATopicForIsCountedAndSaid(t *testing.T) {
	for _, tc := range []struct {
		name, filter, template, topic, says string
	}{
		{
			name:     "a capture that builds into the reserved space",
			filter:   "events/+/#",
			template: "$1/$#",
			topic:    "events/$saguin/queue/jobs/response",
			says:     "reserved `$` space",
		},
		{
			name:     "a tail that came out empty",
			filter:   "events/#",
			template: "$#",
			topic:    "events",
			says:     "produced no topic",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := rules(t, []string{"events"},
				"      - filter: "+tc.filter+"\n        topic: "+tc.template+
					"\n        direction: out\n")
			b, logged := loggingAt(t, cfg, &fake{}, slog.LevelError)
			o := &outRule{b: b, rule: b.cfg.Topics[0]}

			// It must match, or this test is about a rule that covers
			// nothing and every assertion below passes for that reason.
			if _, _, ok := matched(o, tc.topic); !ok {
				t.Fatalf("the rule does not cover %q, so nothing below is about a drop", tc.topic)
			}

			if topic, ok := o.forward(rec(1, tc.topic, "payload")); ok || topic != "" {
				t.Fatalf("forward answered (%q, %v), want it not sent", topic, ok)
			}
			if n := b.unmappable.Load(); n != 1 {
				t.Errorf("the drop counts %d, want 1 - a record that will never reach the "+
					"peer and is counted nowhere is one an operator finds by its absence", n)
			}
			if out := logged(); !strings.Contains(out, tc.says) {
				t.Errorf("the line does not say %q, so the two drops RFC 0002 lists as "+
					"different read as the same one: %q", tc.says, out)
			}
		})
	}
}

// matched reports what the rule makes of a topic, so a test can prove its
// own premise before asserting anything about the drop.
func matched(o *outRule, topic string) (string, bool, bool) {
	built, ok := o.rule.Match(topic)
	return built, built != "", ok
}

// **A record's identity crosses the link** (invariant 8). A receiving saguin
// lifts `saguin-id` into the record it stores, so a record that crossed keeps
// the Message ID it was given here and takes a local offset there.
//
// Without it the far end mints a fresh identity, and the same record on two
// brokers has two - which is what makes a duplicate after a link drop
// impossible to recognise as one, on either side.
func TestARecordCarriesItsIdentityToThePeer(t *testing.T) {
	rd := newReader()
	one := rec(1, "events/a", "hello")
	one.MessageID = "0192f0d0-0000-7000-8000-000000000001"
	one.Headers = []store.Header{{Key: "tag", Value: "kept"}}
	rd.records["events"] = []store.Record{one}

	o, _ := outBridge(t, "events/#", rd)
	p := &props{code: map[string]byte{}}
	if _, _, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false); stuck {
		t.Fatal("the stand-in peer refused the record, so nothing below is about what crossed")
	}

	got := p.user()
	if got["saguin-id"] != one.MessageID {
		t.Errorf("the peer was sent saguin-id %q, want %q - without it the far end mints a "+
			"fresh identity and one record has two", got["saguin-id"], one.MessageID)
	}
	// The publisher's own header still crosses beside it, or this would pass
	// on a publish that carried nothing else.
	if got["tag"] != "kept" {
		t.Errorf("the publisher's own header did not cross: %v", got)
	}
}

// **The publisher's expiry is decremented by the time the record waited
// here**, which is what MQTT asks of anything that holds a message and
// forwards it later. Shipping the value as stored restarts the clock at
// every hop, so a record with an hour on it has an hour left however long
// it sat in a channel and however many brokers it crossed.
func TestAnExpiryCrossesAsWhatIsLeftOfIt(t *testing.T) {
	rd := newReader()
	waited := rec(1, "events/a", "hello")
	waited.MessageExpiry = 3600
	waited.Timestamp = time.Now().Add(-30 * time.Minute)

	gone := rec(2, "events/b", "too late")
	gone.MessageExpiry = 60
	gone.Timestamp = time.Now().Add(-2 * time.Hour)

	rd.records["events"] = []store.Record{waited, gone}

	o, _ := outBridge(t, "events/#", rd)
	p := &props{code: map[string]byte{}}
	_, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)

	if left := p.expiry("events/a"); left == 0 || left > 1801 || left < 1750 {
		t.Errorf("the record crossed with %d seconds left, want about 1800 - it waited half "+
			"of its hour here, and a hop that resets the clock makes the interval mean "+
			"nothing past the first broker", left)
	}
	if p.was("events/b") {
		t.Error("a record whose expiry had run out was sent anyway; the publisher said when " +
			"it stopped being worth delivering")
	}
	// And it does not hold the ones behind it: the position passes it.
	if next != 3 {
		t.Errorf("the position is %d, want 3 - an expired record must not stall the drain", next)
	}
}

// **A refusal that can stop being true needs a clock, because the wake is
// not one.** Watch fires when a record lands, so a rule stopped on `0x97`
// would wait for the next local publish to try again - and on a channel that
// has gone quiet, which is the one an outage is noticed on last, that is a
// stall with no end.
//
// Driven through drainAppend rather than carry, because the stall is in the
// loop around it: carry answers correctly either way.
func TestARefusedRecordIsRetriedWithoutANewOne(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{rec(1, "events/a", "one")}
	o, p := outBridge(t, "events/#", rd)
	o.b.firstRetry = time.Millisecond // the clock, not the wait

	p.mu.Lock()
	p.code["events/a"] = 0x97
	p.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); o.drainAppend(ctx, p, channel.Source{Name: "events"}) }()

	// Nothing is published into the channel from here on: every further
	// attempt has to come from the rule's own clock.
	if !eventually(t, 3*time.Second, func() bool { return len(p.sent()) >= 3 }) {
		t.Fatalf("the record was offered %d times and then stopped, so a peer that "+
			"recovered an hour later would never be sent it: a quiet channel gives the "+
			"rule no wake to try again on", len(p.sent()))
	}

	// And when the peer recovers, it moves - with no new record to prompt it.
	p.mu.Lock()
	delete(p.code, "events/a")
	p.mu.Unlock()
	if !eventually(t, 3*time.Second, func() bool { return rd.savedAt("events") == 2 }) {
		t.Errorf("the position is %d after the peer recovered, want 2", rd.savedAt("events"))
	}
	cancel()
	<-done
}

// **A rule that has never stored a position has lost nothing**, however far
// retention has already trimmed the channel it starts on.
//
// It begins at 1 because that is where a channel begins, so on a trimmed
// channel its very first read is below the floor - and reported as loss that
// is a new bridge rule raising invariant 1's alarm for the broker's whole
// history, every time anybody adds one.
func TestANewRuleOnATrimmedChannelHasLostNothing(t *testing.T) {
	rd := newReader()
	rd.floor["events"] = 500
	rd.records["events"] = []store.Record{rec(500, "events/a", "survivor")}

	o, p := outBridge(t, "events/#", rd)
	moved, next, _ := o.carry(t.Context(), p, "events", "bridge:head-office", 1, true)
	if !moved || next != 500 {
		t.Fatalf("carry answered (%v, %d), want it to start at the floor 500", moved, next)
	}
	if len(rd.lost) != 0 {
		t.Errorf("a rule that had never stored a position counted %d channels lost - that "+
			"counter is what an operator watches for records nobody was sent, and a new "+
			"rule starting up is not one of them", len(rd.lost))
	}

	// The same read from a rule that *had* stored a position is loss, and is
	// counted - or this test would pass on a branch that never reports any.
	o2, p2 := outBridge(t, "events/#", rd)
	if _, _, _ = o2.carry(t.Context(), p2, "events", "bridge:head-office", 10, false); len(rd.lost) != 1 {
		t.Errorf("a position retention had passed counted %d, want 1", len(rd.lost))
	}
}

// **The batch stops at the first record the peer did not take.** The position
// cannot pass that record anyway, so everything published after it is
// published again on the next pass - a batch of systematic duplicates per
// retry, for as long as the refusal stands.
func TestNothingIsPublishedPastARecordThePeerDidNotTake(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"), rec(2, "events/stuck", "two"),
		rec(3, "events/c", "three"), rec(4, "events/d", "four"),
	}
	o, p := outBridge(t, "events/#", rd)
	p.code["events/stuck"] = 0x97

	_, next, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if !stuck {
		t.Fatal("carry did not report the refusal, so the retry clock never starts")
	}
	if next != 2 {
		t.Errorf("the position is %d, want 2", next)
	}
	// **The records that matter crossed, and the duplication is bounded.**
	// A windowed rule cannot know where the gap is until the records beyond
	// it are already in flight, so some go; the first refusal stops anything
	// further being issued, which caps what may be sent twice at the window
	// rather than at the batch. What the position does is unchanged and is
	// asserted above - it stops at the gap, so nothing is skipped.
	got := p.sent()
	for _, must := range []string{"events/a one", "events/stuck two"} {
		if !slices.Contains(got, must) {
			t.Fatalf("the peer never saw %q; it saw %v", must, got)
		}
	}
	// The records beyond the gap that went are bounded by the window, which
	// the batch above is read with - so that bound is structural rather than
	// asserted here. What the first refusal must do is stop anything
	// *further* being issued, and that has a test of its own:
	// TestARefusalStopsTheRestOfThePassBeingIssued.
}

// props is a peer that keeps each publish's properties, for the two
// assertions that are about what crossed rather than about how much.
type props struct {
	mu   sync.Mutex
	got  map[string]*paho.Publish
	code map[string]byte
}

func (p *props) Publish(_ context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.got == nil {
		p.got = map[string]*paho.Publish{}
	}
	p.got[pk.Topic] = pk
	return &paho.PublishResponse{ReasonCode: p.code[pk.Topic]}, nil
}

func (p *props) user() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]string{}
	for _, pk := range p.got {
		for _, u := range pk.Properties.User {
			out[u.Key] = u.Value
		}
	}
	return out
}

// topics is every topic the peer was sent, for a failure that needs to say
// what did cross rather than only what did not.
func (p *props) topics() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.got))
	for topic := range p.got {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

// publish is the packet the peer was sent for one topic, or nil.
func (p *props) publish(topic string) *paho.Publish {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.got[topic]
}

func (p *props) expiry(topic string) uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	pk, ok := p.got[topic]
	if !ok || pk.Properties == nil || pk.Properties.MessageExpiry == nil {
		return 0
	}
	return *pk.Properties.MessageExpiry
}

func (p *props) was(topic string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.got[topic]
	return ok
}

// eventually polls until want is true, so a test about a clock does not
// depend on how fast this machine is.
func eventually(t *testing.T, within time.Duration, want func() bool) bool {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if want() {
			return true
		}
	}
	return want()
}

// **And the same clock on a `latest` channel**, which is a separate loop and
// would stall the same way: a value refused for a reason the peer recovers
// from is offered again only when some topic on that channel next changes.
//
// Asserted separately from the append case because the two share a fix and
// not a line of code - a test of one says nothing about the other.
func TestARefusedValueIsRetriedWithoutANewChange(t *testing.T) {
	rd := newReader()
	rd.records["state"] = []store.Record{rec(1, "state/a", "current")}
	rd.sources["state/#"] = []channel.Source{{Name: "state", Latest: true}}

	o, p := outBridge(t, "state/#", rd)
	o.b.firstRetry = time.Millisecond
	p.mu.Lock()
	p.code["state/a"] = 0x97
	p.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.drainLatest(ctx, p, channel.Source{Name: "state", Latest: true})
	}()
	if !eventually(t, 3*time.Second, func() bool { return rd.watching("state") }) {
		t.Fatal("the rule never started watching the channel")
	}
	rd.feed("state", rec(2, "state/a", "changed"))

	if !eventually(t, 3*time.Second, func() bool { return len(p.sent()) >= 3 }) {
		t.Fatalf("the value was offered %d times and then stopped: a latest channel whose "+
			"state is not changing gives the rule no wake to retry on", len(p.sent()))
	}
	cancel()
	<-done
}

// RFC 0002 "Bridges", RFC 0003 "Retained messages"
//
// **The publisher's retain flag crosses the link.** A value retained here
// becomes state at the far end, so a subscriber arriving there later is
// served it - which is what mosquitto's bridge does and what an operator
// migrating one expects. Before this the flag was dropped on the crossing,
// the peer stored nothing, and a late subscriber there found nothing, with
// nothing saying so.
//
// The unretained case is asserted beside it, or the test would pass on a
// bridge that set the flag on everything - which would write into the peer's
// retained store for every record that crossed.
func TestTheRetainFlagCrossesAsThePublisherSetIt(t *testing.T) {
	rd := newReader()
	kept := rec(1, "events/state", "current")
	kept.Retain = true
	live := rec(2, "events/live", "passing")
	rd.records["events"] = []store.Record{kept, live}

	o, _ := outBridge(t, "events/#", rd)
	p := &props{code: map[string]byte{}}
	// From offset 1, which is where a channel starts: reading from 2 would
	// skip the retained record and leave the assertion below passing on an
	// empty map rather than on what crossed.
	if _, _, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false); stuck {
		t.Fatal("the stand-in peer refused a record, so nothing below is about what crossed")
	}
	if p.publish("events/state") == nil || p.publish("events/live") == nil {
		t.Fatalf("both records must have crossed before their flags mean anything; "+
			"the peer holds %v", p.topics())
	}

	if pk := p.publish("events/state"); !pk.Retain {
		t.Error("a value its publisher retained crossed with the flag down: the peer " +
			"stores nothing and a subscriber arriving there later finds nothing")
	}
	if pk := p.publish("events/live"); pk.Retain {
		t.Error("an ordinary record crossed with the retain flag up, which writes it " +
			"into the peer's retained store for ever")
	}
}

// RFC 0002 "Bridges"
//
// **A peer that refuses retained messages gets the value with the flag
// down**, and the link keeps moving.
//
// MQTT 5 lets a broker advertise Retain Available 0, and paho refuses a
// retained publish to such a peer client-side rather than sending it - so a
// bridge that carried the flag regardless would fail that write, hold the
// rule's position at that record, and offer it again on every reconnect for
// ever. One retained value at the source would stop the link for good, which
// is a shape that has been found once already.
//
// The degradation is stated rather than hidden: the value crosses, the
// retained promise does not. mosquitto meets the same mismatch with
// `bridge_outgoing_retain false`, an operator switch it documents as
// existing because v3.1.1 has no way to advertise the limit - for a v5 peer
// the advertisement is there and is what a bridge should honour, which is
// what this does automatically.
func TestAPeerThatRefusesRetainedGetsTheValueLive(t *testing.T) {
	rd := newReader()
	kept := rec(1, "events/state", "current")
	kept.Retain = true
	rd.records["events"] = []store.Record{kept}

	o, _ := outBridge(t, "events/#", rd)
	o.b.peerNoRetain.Store(true)

	p := &props{code: map[string]byte{}}
	if _, _, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false); stuck {
		t.Fatal("the record did not cross at all: a peer that refuses retained must " +
			"still be sent the value, or one retained record stops the link")
	}
	pk := p.publish("events/state")
	if pk == nil {
		t.Fatal("nothing was published to the peer")
	}
	if pk.Retain {
		t.Error("the retain flag was carried to a peer that advertised Retain " +
			"Available 0: paho refuses that publish client-side, so the position " +
			"never moves past this record and the link retries it for ever")
	}
	if string(pk.Payload) != "current" {
		t.Errorf("the peer was sent %q, want the value itself: clearing the flag "+
			"must cost the retained promise and nothing else", pk.Payload)
	}
}

// RFC 0002 "Bridges", RFC 0003 "`append` - Ordering"
//
// **Two records on one topic never overlap on the wire**, which is the
// promise that decides the shape of the whole pipelined pass.
//
// A bridged record is given a *local* offset where it lands, so at the far
// end currency is arrival order: two writes to one topic arriving out of
// order would leave a `latest` channel there holding the older one for
// good, and a device reading it stale with nothing to correct it. Per topic
// is therefore serial however wide the window is; across topics it
// pipelines, which is where a channel's traffic spreads.
//
// **The stand-in peer holds each publish open** so that an overlap is
// visible at all. A rule that serialised everything would also pass the
// first assertion, so the second one is what says this is a window and not
// the sequential loop wearing a hat.
func TestOneTopicIsNeverPublishedConcurrently(t *testing.T) {
	rd := newReader()
	var recs []store.Record
	for i := 1; i <= 6; i++ {
		// Three records on one topic, three on their own.
		topic := "events/serial"
		if i%2 == 0 {
			topic = fmt.Sprintf("events/spread%d", i)
		}
		recs = append(recs, rec(uint64(i), topic, fmt.Sprintf("r%d", i)))
	}
	rd.records["events"] = recs

	o, _ := outBridge(t, "events/#", rd)
	p := &overlapping{hold: 40 * time.Millisecond, code: map[string]byte{}}
	if _, _, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false); stuck {
		t.Fatal("the stand-in peer refused a record, so nothing below is about ordering")
	}

	if n := p.maxFor("events/serial"); n != 1 {
		t.Errorf("%d publishes to events/serial were in flight at once: a bridged "+
			"record takes a local offset where it lands, so two writes to one topic "+
			"overtaking each other leave the older value current at the peer", n)
	}
	// The order they were written in, for that topic, is the order they were read.
	if got := p.orderFor("events/serial"); !slices.Equal(got, []string{"r1", "r3", "r5"}) {
		t.Errorf("events/serial was written %v, want [r1 r3 r5]", got)
	}
	// And the pass really did pipeline, or this asserts nothing about a window.
	if n := p.maxOverall(); n < 2 {
		t.Errorf("never more than %d publish in flight across all topics, so this "+
			"pass was sequential and the ordering assertion above is free", n)
	}
}

// overlapping is a stand-in peer that holds every publish open for a while
// and records how many were in flight at once, per topic and overall.
type overlapping struct {
	hold time.Duration
	code map[string]byte

	mu      sync.Mutex
	live    map[string]int
	peak    map[string]int
	order   map[string][]string
	all     int
	allPeak int
}

func (o *overlapping) Publish(ctx context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	o.mu.Lock()
	if o.live == nil {
		o.live, o.peak, o.order = map[string]int{}, map[string]int{}, map[string][]string{}
	}
	o.live[pk.Topic]++
	o.all++
	if o.live[pk.Topic] > o.peak[pk.Topic] {
		o.peak[pk.Topic] = o.live[pk.Topic]
	}
	if o.all > o.allPeak {
		o.allPeak = o.all
	}
	o.order[pk.Topic] = append(o.order[pk.Topic], string(pk.Payload))
	o.mu.Unlock()

	select {
	case <-time.After(o.hold):
	case <-ctx.Done():
	}

	o.mu.Lock()
	o.live[pk.Topic]--
	o.all--
	o.mu.Unlock()
	return &paho.PublishResponse{ReasonCode: o.code[pk.Topic]}, nil
}

func (o *overlapping) maxFor(topic string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.peak[topic]
}

func (o *overlapping) maxOverall() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.allPeak
}

func (o *overlapping) orderFor(topic string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.order[topic])
}

// docs/invariants.md 1, and RFC 0002 "Bridges"
//
// **An acknowledged suffix never carries the position past an unacknowledged
// gap.**
//
// This is the rule the window makes real. Sequentially, acknowledgements
// arrived in order because only one was outstanding, so the prefix walk was
// trivially right and never exercised. With several topics in flight the
// acknowledgements genuinely interleave, while the position is still one
// number per channel - so a walk that took the highest settled offset, or
// stepped over a gap, would carry the position past a record the peer never
// took. Everything behind it would be skipped silently and for ever, which
// is invariant 1's failure reached by arithmetic rather than by loss.
//
// The peer here refuses the record in the middle and takes the ones after
// it, which is exactly the shape a prefix walk must survive: later records
// settle, and the position must still stop before the one that did not.
func TestAnAcknowledgedSuffixDoesNotCarryThePositionPastAGap(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"),
		rec(2, "events/gap", "two"),
		rec(3, "events/c", "three"),
		rec(4, "events/d", "four"),
	}
	o, _ := outBridge(t, "events/#", rd)

	// The gap is answered slowly and refused; the records behind it are
	// answered at once and taken, so their acknowledgements land first.
	p := &lopsided{slow: "events/gap", hold: 60 * time.Millisecond,
		code: map[string]byte{"events/gap": 0x97}}

	_, next, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)

	if !stuck {
		t.Fatal("the refusal was not reported, so the retry clock never starts")
	}
	if next != 2 {
		t.Fatalf("the position is %d, want 2: records 3 and 4 were acknowledged after "+
			"the refusal of record 2, and an acknowledged suffix must not carry the "+
			"position past the gap - everything behind it would be skipped for ever",
			next)
	}
	// The premise: the later records really were settled before the walk ran,
	// or this passes on a pass where no reordering happened.
	if !p.settled("events/c") || !p.settled("events/d") {
		t.Fatalf("records after the gap were not acknowledged at all (c=%v d=%v), so "+
			"this test did not exercise out-of-order settlement",
			p.settled("events/c"), p.settled("events/d"))
	}
}

// lopsided answers one topic slowly and every other at once, so the
// acknowledgements come back in an order the offsets do not.
type lopsided struct {
	slow string
	hold time.Duration
	code map[string]byte

	// afterSlowStarts, when set, holds every *refusal* until the slow topic
	// has entered publish. Without it a test that means "the refusal landed
	// while this record was already in flight" is asking for a race it
	// usually wins: both goroutines start together, the refused topic is
	// answered at once, and if it gets there first the slow topic's record
	// is skipped before it is ever issued. That is a legitimate outcome of
	// the code and a useless one to assert about - it made
	// TestARefusalStopsTheRestOfThePassBeingIssued fail about one run in
	// ten, on main, in CI, for as long as it has existed.
	//
	// Opt-in because the other test using this peer makes the slow topic the
	// refused one, where holding refusals until the slow topic starts would
	// be a deadlock on itself.
	afterSlowStarts chan struct{}
	startOnce       sync.Once

	mu   sync.Mutex
	done map[string]bool
	seen map[string]int
}

func (l *lopsided) Publish(ctx context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	switch {
	case pk.Topic == l.slow:
		if l.afterSlowStarts != nil {
			l.startOnce.Do(func() { close(l.afterSlowStarts) })
		}
		select {
		case <-time.After(l.hold):
		case <-ctx.Done():
		}
	case l.afterSlowStarts != nil && l.code[pk.Topic] != 0:
		// The refusal waits for the slow topic to be in flight, so that
		// "already issued" is a fact of the run rather than a race it won.
		select {
		case <-l.afterSlowStarts:
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	l.mu.Lock()
	if l.done == nil {
		l.done, l.seen = map[string]bool{}, map[string]int{}
	}
	l.done[pk.Topic] = true
	l.seen[pk.Topic]++
	l.mu.Unlock()
	return &paho.PublishResponse{ReasonCode: l.code[pk.Topic]}, nil
}

// count is how many publishes this topic received.
func (l *lopsided) count(topic string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seen[topic]
}

func (l *lopsided) settled(topic string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done[topic]
}

// RFC 0002 "Bridges"
//
// **The first refusal stops the pass issuing anything further.**
//
// A window cannot find the gap before the records beyond it are in flight,
// so some go and are sent again on the next pass - at-least-once keeping
// its promise. What must not happen is the pass going on *adding* records
// behind a gap it already knows about: that would widen the duplication
// from what was in flight to the whole batch.
//
// Here the refused record is answered at once and a record on another topic
// is answered slowly, so the refusal is known before that topic's second
// record would be issued. Without the stop, it is issued.
func TestARefusalStopsTheRestOfThePassBeingIssued(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"),   // refused, answered at once
		rec(2, "events/b", "two"),   // taken, answered slowly
		rec(3, "events/a", "three"), // behind the refusal on its own topic
		rec(4, "events/b", "four"),  // behind the refusal on another topic
	}
	o, _ := outBridge(t, "events/#", rd)
	p := &lopsided{slow: "events/b", hold: 60 * time.Millisecond,
		code:            map[string]byte{"events/a": 0x97},
		afterSlowStarts: make(chan struct{})}

	_, next, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)
	if !stuck || next != 1 {
		t.Fatalf("stuck=%v position=%d, want true and 1 - the first record was "+
			"refused, so nothing may advance", stuck, next)
	}

	if p.count("events/a") != 1 {
		t.Errorf("events/a was published %d times, want 1: a topic stops at its own "+
			"refusal, or the record behind it goes out ahead of a re-send",
			p.count("events/a"))
	}
	if n := p.count("events/b"); n != 1 {
		t.Errorf("events/b was published %d times, want 1: the refusal of record 1 "+
			"was known before record 4 could be issued, and a pass that goes on "+
			"adding records behind a known gap widens the duplication from what "+
			"was in flight to the whole batch", n)
	}
}

// gated forces the interleaving the silent skip needs, rather than waiting
// for a scheduler to produce it. One topic's first record is held inside
// Publish until another topic's record has been refused, so the holder
// wakes with the refusal already recorded.
type gated struct {
	mu      sync.Mutex
	got     []string
	held    bool
	code    map[string]byte
	hold    string
	entered chan struct{} // closed once the holder is inside publish
	refused chan struct{} // closed once the refusal has been returned
	stored  chan struct{} // closed once carry has recorded it (refusalStored)
}

// Publish is a two-way rendezvous, so the interleaving is forced in both
// directions rather than raced for: the refusal waits until the holder is
// inside publish, and the holder waits until the refusal has been returned.
// Without the first half the refusal can land before the holder's goroutine
// runs at all, which skips its record without ever attempting it - a
// different path, and one that would let this test report nothing useful
// most of the time.
func (p *gated) Publish(_ context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	key := pk.Topic + " " + string(pk.Payload)
	if code := p.code[pk.Topic]; code != 0 {
		select {
		case <-p.entered:
		case <-time.After(5 * time.Second):
			panic("gated: the holder never reached publish")
		}
		p.mu.Lock()
		p.got = append(p.got, key)
		p.mu.Unlock()
		close(p.refused)
		return &paho.PublishResponse{ReasonCode: code}, nil
	}
	if key == p.hold {
		close(p.entered)
		// The refusal has been returned, and carry records it a few
		// statements later: the holder waits for that record
		// (refusalStored), or it loops round before the gap is visible and
		// the interleaving under test never happens. held says it did.
		select {
		case <-p.stored:
		case <-time.After(5 * time.Second):
			panic("gated: the refusal was never recorded")
		}
		p.mu.Lock()
		p.held = true
		p.mu.Unlock()
	}
	p.mu.Lock()
	p.got = append(p.got, key)
	p.mu.Unlock()
	return &paho.PublishResponse{ReasonCode: 0}, nil
}

func (p *gated) sent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.got...)
}

func (p *gated) forced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}

// **Nothing below the stored position may be unsent.** That is invariant 1
// stated as arithmetic, and it is the promise rather than any mechanism: a
// position past a record the peer never saw skips it silently and for ever,
// because the next pass reads from beyond it.
//
// The interleaving is forced rather than waited for: one topic is held
// inside publish until another topic is refused. Before the fix the holder
// was abandoned on a refusal ordered *after* its own record, left its
// outcome at a zero value that read as sent, and the walk carried the
// position straight past it.
func TestNoRecordBelowTheStoredPositionWentUnsent(t *testing.T) {
	rd := newReader()
	rd.records["events"] = []store.Record{
		rec(1, "events/a", "one"), rec(2, "events/a", "two"),
		rec(3, "events/stuck", "three"),
	}
	o, _ := outBridge(t, "events/#", rd)
	p := &gated{code: map[string]byte{"events/stuck": 0x97},
		hold:    "events/a one",
		entered: make(chan struct{}), refused: make(chan struct{}), stored: make(chan struct{})}
	var once sync.Once
	refusalStored = func() { once.Do(func() { close(p.stored) }) }
	t.Cleanup(func() { refusalStored = nil })

	_, next, stuck := o.carry(t.Context(), p, "events", "bridge:head-office", 1, false)

	if !p.forced() {
		t.Fatal("the holder never waited on the refusal, so this run did not " +
			"exercise the interleaving and proves nothing")
	}
	got := p.sent()
	for _, r := range rd.records["events"] {
		if r.Offset >= next {
			continue
		}
		want := r.Topic + " " + string(r.Payload)
		if !slices.Contains(got, want) {
			t.Errorf("the position is %d but offset %d (%q) was never sent: "+
				"it is skipped for ever, because the next pass reads from beyond it. "+
				"the peer saw %v", next, r.Offset, want, got)
		}
	}
	if !stuck {
		t.Error("carry did not report the refusal, so the retry clock never starts")
	}
}

// RFC 0003: **a bridge forwards the live fan-out and never a stored pass.** An
// outbound rule on a latest channel sends what is published while it runs -
// and not the state the channel held when it started, which a rule that read
// it would re-ship at every start and, for values a bridge had brought in,
// hand the peer back. It did the opposite: it sent the state it found at
// start and then waited for a wake nothing on a latest channel sent, so a
// peer never saw another change.
func TestAnOutRuleOnALatestChannelSendsWhatIsPublishedWhileItRuns(t *testing.T) {
	rd := newReader()
	rd.records["state"] = []store.Record{rec(1, "state/a", "held-at-start")}
	rd.sources["state/#"] = []channel.Source{{Name: "state", Latest: true}}
	o, p := outBridge(t, "state/#", rd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.drainLatest(ctx, p, channel.Source{Name: "state", Latest: true})
	}()
	if !eventually(t, 3*time.Second, func() bool { return rd.watching("state") }) {
		t.Fatal("the rule never started watching the channel")
	}
	if n := rd.feed("state", rec(2, "state/a", "published-now")); n != 1 {
		t.Fatalf("%d rules were watching, want the one", n)
	}
	// **An end marker rather than a quiet period**: values cross in the
	// order they are published, so once the marker has, nothing sent before
	// it is still to come.
	rd.feed("state", rec(3, "state/z", "end"))
	if !eventually(t, 3*time.Second, func() bool { return slices.Contains(p.sent(), "state/z end") }) {
		t.Fatalf("the peer was sent %v, and never the end marker", p.sent())
	}
	cancel()
	<-done
	if got := p.sent(); !slices.Equal(got, []string{"state/a published-now", "state/z end"}) {
		t.Errorf("the peer was sent %v, want only [state/a published-now] before the end marker: "+
			"the state held at start is a stored pass, which a bridge never forwards", got)
	}
}

// gatedPeer holds its first publish until released, which is a link that has
// gone slow - or down, since autopaho's Publish waits for a connection.
type gatedPeer struct {
	peer
	first   sync.Once
	holding chan struct{}
	release chan struct{}
}

func (g *gatedPeer) Publish(ctx context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	g.first.Do(func() {
		close(g.holding)
		<-g.release
	})
	return g.peer.Publish(ctx, pk)
}

// **Newest wins per topic while waiting to cross**, as a subscriber's own
// pending list does: values published while the link is slow or down wait in
// the rule, a newer value for a topic replaces an older one that has not yet
// crossed, and what crosses when the link comes back is each topic's newest,
// in the order they were published. The replaced one is counted, since it is
// the only sign that a rule ran behind.
func TestALatestValueWaitingToCrossIsReplacedByANewerOne(t *testing.T) {
	rd := newReader()
	rd.sources["state/#"] = []channel.Source{{Name: "state", Latest: true}}
	o, _ := outBridge(t, "state/#", rd)
	g := &gatedPeer{peer: peer{code: map[string]byte{}},
		holding: make(chan struct{}), release: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.drainLatest(ctx, g, channel.Source{Name: "state", Latest: true})
	}()
	if !eventually(t, 3*time.Second, func() bool { return rd.watching("state") }) {
		t.Fatal("the rule never started watching the channel")
	}

	rd.feed("state", rec(1, "state/a", "a1"))
	select {
	case <-g.holding: // a1 is in flight and the link is stuck
	case <-time.After(3 * time.Second):
		t.Fatal("the first value never reached the link")
	}
	rd.feed("state", rec(2, "state/a", "a2"))
	rd.feed("state", rec(3, "state/b", "b1"))
	rd.feed("state", rec(4, "state/a", "a3"))  // replaces a2, which never crossed
	rd.feed("state", rec(5, "state/z", "end")) // the end marker: published last, so it crosses last
	close(g.release)

	if !eventually(t, 3*time.Second, func() bool { return slices.Contains(g.sent(), "state/z end") }) {
		t.Fatalf("the peer was sent %v after the link came back, and never the end marker", g.sent())
	}
	cancel()
	<-done
	if got := strings.Join(g.sent(), ","); got != "state/a a1,state/b b1,state/a a3,state/z end" {
		t.Errorf("the peer was sent [%s], want [state/a a1,state/b b1,state/a a3] before the end "+
			"marker: each topic's newest, in the order published, and not a2", got)
	}
	if n := o.b.Superseded(); n != 1 {
		t.Errorf("%d values counted as superseded, want 1 (a2)", n)
	}
}

// ruleReader is one append channel, "events", whose positions are kept by
// reader name and whose Watch gives each watcher a wake of its own - both as
// the broker's are, and both what the reader above leaves out, which is why
// no test built on it could see two rules sharing a position.
type ruleReader struct {
	mu      sync.Mutex
	records []store.Record
	floor   uint64
	saved   map[string]uint64
	wakes   []chan struct{}
	lostAs  []lostPosition
}

func (r *ruleReader) Sources(string) []channel.Source { return []channel.Source{{Name: "events"}} }

func (r *ruleReader) ReadFrom(_ string, offset uint64, limit int) ([]store.Record, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.floor > offset {
		return nil, r.floor, store.ErrBelowFloor
	}
	var out []store.Record
	for _, rec := range r.records {
		if rec.Offset >= offset && rec.Offset >= r.floor {
			out = append(out, rec)
			if len(out) == limit {
				break
			}
		}
	}
	return out, max(r.floor, 1), nil
}

func (r *ruleReader) Head(string) uint64 { return 1 }

func (r *ruleReader) Position(_, reader string) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.saved[reader]
	return at, ok, nil
}

func (r *ruleReader) SavePosition(_, reader string, off uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved[reader] = off
	return nil
}

func (r *ruleReader) PositionLost(name, reader string, position, floor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lostAs = append(r.lostAs, lostPosition{channel: name, reader: reader, position: position, floor: floor})
}

func (r *ruleReader) Watch(string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	r.wakes = append(r.wakes, ch)
	r.mu.Unlock()
	return ch, func() {}
}

func (r *ruleReader) WatchLatest(string, func(store.Record)) func() { return func() {} }
func (r *ruleReader) WatchBroadcast(func(store.Record)) func()      { return func() {} }

// publish appends records and wakes every watcher, as the broker does.
func (r *ruleReader) publish(recs ...store.Record) {
	r.mu.Lock()
	r.records = append(r.records, recs...)
	wakes := slices.Clone(r.wakes)
	r.mu.Unlock()
	for _, w := range wakes {
		select {
		case w <- struct{}{}:
		default:
		}
	}
}

func (r *ruleReader) at(reader string) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.saved[reader]
	return at, ok
}

// refusingPeer answers 0x97 to every topic under peer/b/ while refusing is
// set, takes everything else, and counts every attempt per record.
type refusingPeer struct {
	mu       sync.Mutex
	refusing bool
	tried    map[string]int
	taken    map[string]int
}

func (p *refusingPeer) Publish(_ context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := pk.Topic + " " + string(pk.Payload)
	p.tried[key]++
	if p.refusing && strings.HasPrefix(pk.Topic, "peer/b/") {
		return &paho.PublishResponse{ReasonCode: 0x97}, nil
	}
	p.taken[key]++
	return &paho.PublishResponse{ReasonCode: 0}, nil
}

func (p *refusingPeer) count(m map[string]int, key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return m[key]
}

func (p *refusingPeer) set(refusing bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refusing = refusing
}

// twoRules is a bridge with two outbound rules over the one channel: the
// peer takes one rule's records and, while it refuses, not the other's.
func twoRules(t *testing.T, rd *ruleReader) (*Bridge, string, string) {
	t.Helper()
	cfg := rules(t, []string{"events"},
		"      - filter: events/a/#\n        topic: peer/a/$#\n        direction: out\n",
		"      - filter: events/b/#\n        topic: peer/b/$#\n        direction: out\n",
	)
	b, _ := logging(t, cfg, &fake{})
	b.rd = rd
	b.firstRetry = 10 * time.Millisecond
	// The rules' own names, so a test on what the rules do fails on what
	// they do, whatever the names are.
	return b, (&outRule{b: b, rule: b.cfg.Topics[0]}).readerName(), (&outRule{b: b, rule: b.cfg.Topics[1]}).readerName()
}

// RFC 0002 "Bridges": an outbound rule draining an append channel keeps a
// position in it. Each rule keeps its own, so:
//   - a rule whose records the peer keeps refusing holds its own position and
//     nobody else's: the other rule goes on sending, past more than a whole
//     window of records behind the refused one;
//   - after a restart the refused record is sent once the peer takes it, and
//     the rule that had sent everything sends nothing again.
//
// With one position per bridge, the rule the peer took saved the shared
// position past the refused record, and the restart resumed both rules
// beyond it: the record was never sent, and nothing counted it.
func TestEachOutboundRuleKeepsItsOwnPositionInAChannel(t *testing.T) {
	rd := &ruleReader{saved: map[string]uint64{}}
	rd.records = []store.Record{
		rec(1, "events/a/x", "a1"),
		rec(2, "events/b/1", "b1"), // refused, and refused again for as long as the peer is full
		rec(3, "events/a/y", "a2"),
	}
	p := &refusingPeer{refusing: true, tried: map[string]int{}, taken: map[string]int{}}
	b, readerA, readerB := twoRules(t, rd)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.runOut(ctx, p) }()
	if !eventually(t, 3*time.Second, func() bool {
		return p.count(p.taken, "peer/a/x a1") == 1 && p.count(p.taken, "peer/a/y a2") == 1 &&
			p.count(p.tried, "peer/b/1 b1") > 0
	}) {
		t.Fatalf("the first pass never completed: tried %v", p.tried)
	}

	// More than a whole window of the healthy rule's records behind the
	// refused one: it sends every one of them while the refusal stands.
	window := outWindow(0, b.cfg.ReceiveMaximum)
	var more []store.Record
	for i := range 3 * window {
		more = append(more, rec(uint64(4+i), "events/a/z", fmt.Sprintf("more-%d", i)))
	}
	rd.publish(more...)
	last := uint64(4 + 3*window - 1)
	if !eventually(t, 3*time.Second, func() bool {
		at, _ := rd.at(readerA)
		return at == last+1
	}) {
		at, _ := rd.at(readerA)
		t.Fatalf("the healthy rule's position is %d while the other rule is refused, want %d: "+
			"a refusal on one rule is holding another", at, last+1)
	}
	if at, _ := rd.at(readerB); at != 2 {
		t.Errorf("the refused rule's position is %d, want 2: it may not pass the record the peer refused", at)
	}
	if p.count(p.taken, "peer/b/1 b1") != 0 {
		t.Fatal("the premise failed: the peer took the refused record")
	}
	cancel()
	<-done

	// The broker restarts, and the peer has room again.
	p.set(false)
	b2, _, _ := twoRules(t, rd)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { defer close(done2); b2.runOut(ctx2, p) }()
	if !eventually(t, 3*time.Second, func() bool { return p.count(p.taken, "peer/b/1 b1") == 1 }) {
		at, _ := rd.at(readerB)
		t.Fatalf("after the restart the refused record was never sent (its rule is at %d): "+
			"the loss this position exists to prevent", at)
	}
	// **An end marker for each rule, read off its stored position.** A rule
	// stores its position once every publish of a pass has returned (carry),
	// and passes read in offset order, so a position past the marker says
	// nothing below it is still being sent - re-sent or not.
	rd.publish(rec(last+1, "events/a/end", "end-a"), rec(last+2, "events/b/end", "end-b"))
	if !eventually(t, 3*time.Second, func() bool {
		a, _ := rd.at(readerA)
		b, _ := rd.at(readerB)
		return a == last+3 && b == last+3
	}) {
		a, _ := rd.at(readerA)
		b, _ := rd.at(readerB)
		t.Fatalf("after the restart the rules stopped at %d and %d, want both past the end "+
			"markers at %d", a, b, last+3)
	}
	cancel2()
	<-done2
	for _, key := range []string{"peer/a/x a1", "peer/a/y a2", fmt.Sprintf("peer/a/z more-%d", 3*window-1)} {
		if n := p.count(p.taken, key); n != 1 {
			t.Errorf("%s reached the peer %d times, want once: a rule that had sent it sent it again", key, n)
		}
	}
	if n := p.count(p.taken, "peer/b/1 b1"); n != 1 {
		t.Errorf("the refused record reached the peer %d times after the restart, want once", n)
	}
}

// Retention passing a rule the peer keeps refusing is counted once, against
// that rule's own name, and the rule that kept up is not passed at all -
// with one position per bridge every rule reading the channel reported the
// same loss, and the rule that had sent everything reported it too.
func TestRetentionPassingOneOutboundRuleIsCountedForThatRuleAlone(t *testing.T) {
	rd := &ruleReader{saved: map[string]uint64{}}
	rd.records = []store.Record{rec(1, "events/a/x", "a1"), rec(2, "events/b/1", "b1"), rec(3, "events/a/y", "a2")}
	p := &refusingPeer{refusing: true, tried: map[string]int{}, taken: map[string]int{}}
	b, readerA, readerB := twoRules(t, rd)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.runOut(ctx, p) }()
	defer func() { cancel(); <-done }()
	if !eventually(t, 3*time.Second, func() bool {
		at, _ := rd.at(readerA)
		return at == 4 && p.count(p.tried, "peer/b/1 b1") > 0
	}) {
		t.Fatalf("the first pass never completed: tried %v", p.tried)
	}

	// Retention trims through record 3: the refused rule, at 2, is passed;
	// the healthy one, at 4, is not.
	rd.mu.Lock()
	rd.floor = 4
	rd.mu.Unlock()
	rd.publish(rec(4, "events/a/z", "a3"))
	if !eventually(t, 3*time.Second, func() bool {
		rd.mu.Lock()
		defer rd.mu.Unlock()
		return len(rd.lostAs) > 0
	}) {
		t.Fatal("retention passed the refused rule and nothing reported it")
	}
	// **An end marker rather than a quiet period.** The peer takes the
	// refused rule's records again and a marker follows: a rule past the
	// marker has finished every pass that could report the loss a second
	// time, since each reads from where the last stored its position.
	p.set(false)
	rd.publish(rec(5, "events/b/end", "end"))
	if !eventually(t, 3*time.Second, func() bool {
		a, _ := rd.at(readerA)
		b, _ := rd.at(readerB)
		return a == 6 && b == 6
	}) {
		a, _ := rd.at(readerA)
		b, _ := rd.at(readerB)
		t.Fatalf("the rules stopped at %d and %d, want both past the end marker at 6", a, b)
	}
	rd.mu.Lock()
	lost := slices.Clone(rd.lostAs)
	rd.mu.Unlock()
	want := lostPosition{channel: "events", reader: readerB, position: 2, floor: 4}
	if len(lost) != 1 || lost[0] != want {
		t.Fatalf("the loss was reported as %+v, want once as %+v", lost, want)
	}
}

// A rule's position is named by its bridge, its filter and its topic, so two
// rules differing in any one of them never share one.
func TestAnOutboundRulesPositionIsNamedByItsFilterAndTopic(t *testing.T) {
	cfg := rules(t, []string{"events"},
		"      - filter: events/#\n        topic: up/a/$#\n        direction: out\n",
		"      - filter: events/#\n        topic: up/b/$#\n        direction: out\n",
		"      - filter: events/x/#\n        topic: up/a/$#\n        direction: out\n",
	)
	b, _ := logging(t, cfg, &fake{})
	seen := map[string]bool{}
	for _, r := range b.cfg.Topics {
		name := (&outRule{b: b, rule: r}).readerName()
		if seen[name] {
			t.Errorf("two rules share the position %q", name)
		}
		seen[name] = true
	}
	if len(seen) != 3 {
		t.Fatalf("named %d positions for three rules", len(seen))
	}
	// The spelling is what /v1/operations/consumers shows an operator.
	if want := `bridge:` + cfg.Name + ` "events/#" "up/a/$#"`; !seen[want] {
		t.Errorf("no rule's position is named %s: %v", want, seen)
	}
}

// holdingPeer takes a publish and holds it unacknowledged until released,
// or until the publish's own context ends it, as a peer with a record in
// flight does while a link is down or while it simply has not answered.
type holdingPeer struct {
	mu      sync.Mutex
	entered int
	release chan struct{}
}

func (p *holdingPeer) Publish(ctx context.Context, _ *paho.Publish) (*paho.PublishResponse, error) {
	p.mu.Lock()
	p.entered++
	p.mu.Unlock()
	select {
	case <-p.release:
		return &paho.PublishResponse{ReasonCode: 0}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *holdingPeer) tries() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.entered
}

func outOnly(t *testing.T) (*outRule, func() string) {
	t.Helper()
	cfg := rules(t, []string{"events"}, "      - filter: events/#\n        topic: up/$#\n        direction: out\n")
	b, logged := logging(t, cfg, &fake{})
	b.rd = newReader()
	return &outRule{b: b, rule: b.cfg.Topics[0]}, logged
}

func connack(present bool) *paho.Connack {
	return &paho.Connack{SessionPresent: present, Properties: &paho.ConnackProperties{RetainAvailable: true}}
}

// **One resend mechanism, the session's**. A publish in flight when the link drops waits across the
// reconnect for the PUBACK to paho's own resend, so a reconnect that finds
// the peer's session leaves it waiting. A reconnect that finds none ends it -
// paho has dropped it from its session without answering - and the rule
// sends the record again, which is the at-least-once resend.
func TestAPublishWaitsAcrossAReconnectUnlessThePeerKeptNoSession(t *testing.T) {
	o, logged := outOnly(t)
	p := &holdingPeer{release: make(chan struct{})}
	got := make(chan outcome, 1)
	go func() { got <- o.publish(context.Background(), p, "up/x", rec(1, "events/x", "one")) }()
	if !eventually(t, 2*time.Second, func() bool { return p.tries() == 1 }) {
		t.Fatal("the publish never reached the peer")
	}

	o.b.connected(nil, connack(true))
	select {
	case out := <-got:
		t.Fatalf("a reconnect that found the session ended the publish waiting on it (%v): "+
			"the rule would send the record again beside paho's own resend", out)
	case <-time.After(100 * time.Millisecond):
	}

	o.b.connected(nil, connack(false))
	select {
	case out := <-got:
		if out != contingent {
			t.Fatalf("a publish the peer lost with its session came back %v, want contingent, "+
				"so its rule sends it again", out)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a reconnect that found no session left the publish waiting for a PUBACK " +
			"that paho will never deliver: the rule stalls for good")
	}
	if !strings.Contains(logged(), "the peer kept no session for this bridge") {
		t.Errorf("the resend was not said for what it is:\n%s", logged())
	}
	if p.tries() != 1 {
		t.Errorf("the publish reached the peer %d times, want once", p.tries())
	}
}

// A peer that stays connected and does not acknowledge a record is waited on,
// not sent the record again - MQTT's at-least-once, as mosquitto's bridge
// does - and the wait is said once it has lasted, and retracted when the
// acknowledgement comes, so a rule that has gone quiet says why.
func TestARecordThePeerHoldsIsWaitedOnAndSaid(t *testing.T) {
	o, logged := outOnly(t)
	o.b.unackedAfter = 20 * time.Millisecond
	p := &holdingPeer{release: make(chan struct{})}
	got := make(chan outcome, 1)
	go func() { got <- o.publish(context.Background(), p, "up/x", rec(1, "events/x", "one")) }()
	if !eventually(t, 2*time.Second, func() bool {
		return strings.Contains(logged(), "the peer has not acknowledged a record on a link that is up")
	}) {
		t.Fatalf("a record held unacknowledged was not said:\n%s", logged())
	}
	close(p.release)
	select {
	case out := <-got:
		if out != sent {
			t.Fatalf("the acknowledged record came back %v, want sent", out)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the publish never returned after its PUBACK")
	}
	if !strings.Contains(logged(), "the peer acknowledged the record it was holding") {
		t.Errorf("the wait was never retracted:\n%s", logged())
	}
	if p.tries() != 1 {
		t.Errorf("the record reached the peer %d times while it waited, want once", p.tries())
	}
}

// A wait is per rule: two rules whose peers each hold a record are each said,
// and one rule's acknowledgement takes back its own wait and not the other's
// - a flag for the whole bridge announced only the first and retracted it
// while the second still waited.
func TestEachRulesWaitIsSaidAndTakenBackOnItsOwn(t *testing.T) {
	cfg := rules(t, []string{"events"},
		"      - filter: events/a/#\n        topic: up/a/$#\n        direction: out\n",
		"      - filter: events/b/#\n        topic: up/b/$#\n        direction: out\n")
	b, logged := logging(t, cfg, &fake{})
	b.rd = newReader()
	b.unackedAfter = 20 * time.Millisecond
	ra, rb := &outRule{b: b, rule: b.cfg.Topics[0]}, &outRule{b: b, rule: b.cfg.Topics[1]}
	pa, pb := &holdingPeer{release: make(chan struct{})}, &holdingPeer{release: make(chan struct{})}
	doneA, doneB := make(chan outcome, 1), make(chan outcome, 1)
	go func() { doneA <- ra.publish(context.Background(), pa, "up/a/x", rec(1, "events/a/x", "a")) }()
	go func() { doneB <- rb.publish(context.Background(), pb, "up/b/x", rec(2, "events/b/x", "b")) }()

	said := func(line, rule string) int {
		n := 0
		for _, l := range strings.Split(logged(), "\n") {
			if strings.Contains(l, line) && strings.Contains(l, "rule="+rule) {
				n++
			}
		}
		return n
	}
	const waitLine, backLine = "the peer has not acknowledged a record", "the peer acknowledged the record it was holding"
	if !eventually(t, 2*time.Second, func() bool {
		return said(waitLine, "events/a/#") == 1 && said(waitLine, "events/b/#") == 1
	}) {
		t.Fatalf("each rule's wait was not said once:\n%s", logged())
	}
	close(pa.release)
	<-doneA
	if said(backLine, "events/a/#") != 1 || said(backLine, "events/b/#") != 0 {
		t.Fatalf("rule a's acknowledgement took back %d of its own waits and %d of rule b's, want 1 and 0:\n%s",
			said(backLine, "events/a/#"), said(backLine, "events/b/#"), logged())
	}
	close(pb.release)
	<-doneB
	if said(backLine, "events/b/#") != 1 {
		t.Fatalf("rule b's wait was never taken back:\n%s", logged())
	}
}

// RFC 0003: expiry on a latest channel deletes the current value, "and that
// is the point rather than a defect": a stale reading is worse than none. A
// value the channel's retention has removed does not cross, though it was
// waiting to when the period passed, and the rule does not hold it while it
// waits: 50,000 topics published once each during an outage, each older than
// the period, were held (20MiB) and all sent when the link came back. A
// deletion is held to the channel's deletion period.
func TestALatestValueTheChannelHasExpiredDoesNotCross(t *testing.T) {
	for _, tc := range []struct {
		name     string
		src      channel.Source
		held     bool   // whether the stale values are held while they wait
		wantSent string // what crosses once the link is back
	}{
		{"with the channel's periods", channel.Source{Name: "state", Latest: true,
			RetentionPeriod: 3600, DeletionRetentionPeriod: 60},
			false, "state/a a1,state/b b1,state/c "},
		// The control: with no period nothing expires, so the stale values
		// wait - which is what proves the heap below measures them.
		{"with no period", channel.Source{Name: "state", Latest: true}, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rd := newReader()
			rd.sources["state/#"] = []channel.Source{tc.src}
			o, _ := outBridge(t, "state/#", rd)
			g := &gatedPeer{peer: peer{code: map[string]byte{}},
				holding: make(chan struct{}), release: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				o.drainLatest(ctx, g, tc.src)
			}()
			if !eventually(t, 3*time.Second, func() bool { return rd.watching("state") }) {
				t.Fatal("the rule never started watching the channel")
			}
			now := time.Now()
			aged := func(r store.Record, age time.Duration) store.Record {
				r.Timestamp = now.Add(-age)
				return r
			}
			rd.feed("state", aged(rec(1, "state/a", "a1"), 0))
			select {
			case <-g.holding: // a1 is in flight and the link is stuck
			case <-time.After(3 * time.Second):
				t.Fatal("the first value never reached the link")
			}
			heap := func() int64 {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				return int64(m.HeapAlloc)
			}
			before := heap()
			const n = 50000
			for i := range n {
				if rd.feed("state", aged(rec(uint64(10+i), fmt.Sprintf("state/old/%05d", i), "stale"), 2*time.Hour)) == 0 {
					t.Fatal("nothing watched the channel, so nothing was fed")
				}
			}
			grew := heap() - before
			rd.feed("state", aged(rec(n+10, "state/b", "b1"), time.Minute))
			rd.feed("state", aged(rec(n+11, "state/c", ""), 10*time.Second)) // a deletion inside its period
			rd.feed("state", aged(rec(n+12, "state/d", ""), 2*time.Minute))  // one past it
			rd.feed("state", aged(rec(n+13, "state/z", "end"), 0))           // the end marker, published last
			close(g.release)
			t.Logf("%d values two hours old fed while the link was stuck: the heap grew %d KiB", n, grew>>10)
			if tc.held {
				if grew < 8<<20 {
					t.Fatalf("with no period the rule held %d stale values in %d KiB, so the heap "+
						"measures nothing", n, grew>>10)
				}
				return
			}
			if !eventually(t, 3*time.Second, func() bool { return slices.Contains(g.sent(), "state/z end") }) {
				t.Fatalf("the peer was sent %.300s after the link came back, and never the end marker", g.sent())
			}
			cancel()
			<-done
			if got := strings.Join(g.sent(), ","); got != tc.wantSent+",state/z end" {
				t.Errorf("the peer was sent %.300s, want [%s] before the end marker: nothing the "+
					"channel's periods removed", got, tc.wantSent)
			}
			if grew > 4<<20 {
				t.Errorf("the rule held %d KiB for %d values the channel had expired", grew>>10, n)
			}
		})
	}
}

// A value can expire while it waits in a batch behind a slow send, and it
// is asked again at its turn: a batch is built from what is current and then
// sent one value at a time, and a link that takes its time with one value
// holds up the rest.
func TestALatestValueExpiringBehindASlowSendDoesNotCross(t *testing.T) {
	src := channel.Source{Name: "state", Latest: true, RetentionPeriod: 1}
	rd := newReader()
	rd.sources["state/#"] = []channel.Source{src}
	o, _ := outBridge(t, "state/#", rd)
	p := &pausingPeer{peer: peer{code: map[string]byte{}}, pauseAt: 2, pause: 700 * time.Millisecond,
		holding: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.drainLatest(ctx, p, src)
	}()
	if !eventually(t, 3*time.Second, func() bool { return rd.watching("state") }) {
		t.Fatal("the rule never started watching the channel")
	}
	now := time.Now()
	first := rec(1, "state/a", "a1")
	first.Timestamp = now
	rd.feed("state", first)
	select {
	case <-p.holding:
	case <-time.After(3 * time.Second):
		t.Fatal("the first value never reached the link")
	}
	// Both wait for the next batch: slow first, and old, 500ms into a period
	// of one second, behind it. By the time slow has taken 700ms, old is past.
	slow, old := rec(2, "state/slow", "s1"), rec(3, "state/old", "o1")
	slow.Timestamp, old.Timestamp = time.Now(), time.Now().Add(-500*time.Millisecond)
	rd.feed("state", slow)
	rd.feed("state", old)
	close(p.release)
	if !eventually(t, 3*time.Second, func() bool { return slices.Contains(p.sent(), "state/slow s1") }) {
		t.Fatalf("the peer was sent %v", p.sent())
	}
	// **An end marker rather than a quiet period**, fed once slow has
	// crossed - fresh, so its own period cannot run out behind slow. It is
	// a later batch than old's, so old, if it went, went before it.
	end := rec(4, "state/z", "end")
	end.Timestamp = time.Now()
	rd.feed("state", end)
	if !eventually(t, 3*time.Second, func() bool { return slices.Contains(p.sent(), "state/z end") }) {
		t.Fatalf("the peer was sent %v, and never the end marker", p.sent())
	}
	cancel()
	<-done
	if got := strings.Join(p.sent(), ","); got != "state/a a1,state/slow s1,state/z end" {
		t.Errorf("the peer was sent [%s], want [state/a a1,state/slow s1] before the end marker: "+
			"old passed its period while it waited behind slow", got)
	}
}

// pausingPeer holds its first publish until released, and takes pause over
// the publish numbered pauseAt: a link that has come back slow.
type pausingPeer struct {
	peer
	n       atomic.Int32
	pauseAt int32
	pause   time.Duration
	first   sync.Once
	holding chan struct{}
	release chan struct{}
}

func (p *pausingPeer) Publish(ctx context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	p.first.Do(func() {
		close(p.holding)
		<-p.release
	})
	if p.n.Add(1) == p.pauseAt {
		time.Sleep(p.pause)
	}
	return p.peer.Publish(ctx, pk)
}

// A broadcast record has no store to be read back from, so an outbound rule
// is handed it as it is published and queues it (outRule.live). The queue
// is bounded and a full one drops and counts rather than waits: waiting
// would hold every publisher on the broker behind one link (invariant 16).
// The guard had no test that filled it.
//
// The peer takes one record and then holds, so the queue behind it fills.
// The first is taken before the rest are handed over, so exactly the queue's
// 256 fit and the rest are dropped.
func TestABroadcastRuleHeldByItsPeerHoldsNoPublisher(t *testing.T) {
	o, _ := outBridge(t, "loose/#", &reader{})
	rd := &broadcastReader{reader: &reader{}, fns: make(chan func(store.Record), 1)}
	o.b.rd = rd
	p := &heldPeer{took: make(chan string, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go o.live(ctx, p)
	fn := <-rd.fns

	fn(store.Record{Topic: "loose/first", Payload: []byte("1")})
	select {
	case <-p.took:
	case <-time.After(2 * time.Second):
		t.Fatal("the rule never handed its peer the first record, so nothing is held")
	}

	const n = 300
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			fn(store.Record{Topic: "loose/x", Payload: []byte("x")})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("a publisher is held by a peer that takes nothing: %d of %d handed over "+
			"were dropped", o.b.LiveDropped(), n)
	}
	if got, want := o.b.LiveDropped(), uint64(n-256); got != want {
		t.Fatalf("%d of %d records were dropped behind a held peer, want %d - the queue "+
			"holds 256", got, n, want)
	}
}

// heldPeer takes one publish and then holds it until the rule stops.
type heldPeer struct{ took chan string }

func (p *heldPeer) Publish(ctx context.Context, pk *paho.Publish) (*paho.PublishResponse, error) {
	p.took <- pk.Topic
	<-ctx.Done()
	return nil, ctx.Err()
}

// broadcastReader hands over the function a rule watches broadcast with.
type broadcastReader struct {
	*reader
	fns chan func(store.Record)
}

func (r *broadcastReader) WatchBroadcast(fn func(store.Record)) func() {
	r.fns <- fn
	return func() {}
}
