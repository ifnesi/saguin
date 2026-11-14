package core_test

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// These tests drive the core's write groups (design.md, Store, Writes) with
// scripted events and fake times: what a group holds, and when it is
// sealed, are orderings no client can force (design.md, Testing). The
// script refuses a write sent alone, an empty group, and a group sent while
// another is outstanding (request).

// collection is the publish_commit_interval of the tests that collect
// writes, long beside collectingScript's events, a millisecond apart.
const collection = 100 * time.Millisecond

// unreached is a publish_commit_max_records no test collecting with it
// reaches.
const unreached = 100

// collectingScript returns a script of a core that collects writes for
// collection, its publish_commit_interval, up to maxRecords, its
// publish_commit_max_records, started on a store holding $broadcast, into
// which every publish goes; its events come a millisecond apart.
func collectingScript(t *testing.T, maxRecords int) *script {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Storage.PublishCommitInterval = collection
	cfg.Broker.Storage.PublishCommitMaxRecords = maxRecords
	s := newScriptOn(t, cfg, &broadcast)
	s.tick = time.Millisecond
	return s
}

// nextGroup returns the one group acts, a decision, send, failing the test
// unless they send exactly one, after every reply among them: "After a
// transaction answer, it first applies every member's result" (design.md,
// Store, Writes), its replies included.
func nextGroup(t *testing.T, acts []core.Action) *core.Group {
	t.Helper()
	groups := groupsIn(acts)
	if len(groups) != 1 {
		t.Fatalf("the decision sent %d groups, want one: %v", len(groups), writesIn(acts))
	}
	at := slices.IndexFunc(acts, func(a core.Action) bool { return a.Write != nil })
	if slices.ContainsFunc(acts[at:], func(a core.Action) bool { return a.Reply != nil }) {
		t.Fatalf("the decision made a reply ready after it sent the next group: every member's result is applied first")
	}
	return groups[0]
}

// design.md, Store, Writes: "The core keeps at most one write transaction
// outstanding at the store. Publications, session saves and endings,
// positions, Wills and other writes may share a group. Eligible writes join
// in their store-request order, within the read boundaries (Store reads)";
// "With no publish_commit_interval, the core seals eligible pending work at
// the end of the current core event whenever no write transaction is
// outstanding, before taking another event. After a transaction answer, it
// first applies every member's result"; CONNECT, 7: "The core stores step 6's
// changes [...] and the session record in one transaction".
//
// Without an interval, on a store holding an away durable session for
// client n: p, whose session ends with its connection, and d, durable, are
// connected; n and m send CONNECTs, both durable, n's a clean start, and
// both sessions are read. n's read answered, its clean start ends the away
// session and stores a new one: the two writes of that one event go out at
// its end as one group, the ending first. While that group is outstanding,
// m's read is answered, p publishes at QoS 1 and d's connection is lost:
// m's session save, p's publication and d's going away wait. The group's
// answer makes n's CONNACK ready, then sends the waiting writes as the next
// group, in their store-request order. While that one is outstanding, p
// publishes again; its answer makes m's CONNACK and p's first PUBACK ready, then
// sends the second publication, whose answer acknowledges it.
func TestOneWriteTransactionAtATime(t *testing.T) {
	away := store.Session{ID: 5, Client: "n", Protocol: 5, Expiry: 3600, Left: epoch.UnixMilli(), ReceiveMaximum: 20, KeepAlive: 60}
	state := broadcast
	state.HighestSession = away.ID
	s := newScriptOn(t, newConfig(t), &state, away)
	p := s.connected(connectOf(packet.V5, "p"))
	d := s.connected(keeping(packet.V5, "d", 60))
	n, m := s.accept(), s.accept()
	s.receive(n, connectOf(packet.V5, "n", expiryProperty(packet.V5, 60)...), 0)
	s.receive(m, keeping(packet.V5, "m", 60), 0)
	loads := pending[*store.LoadSession](s)

	first := nextGroup(t, s.answer(loads[0]))
	if got, want := s.members(first), []string{"end n", "save n"}; !slices.Equal(got, want) {
		t.Fatalf("n's clean start sent the group %v, want %v: its session's ending, then its new session", got, want)
	}
	s.answer(loads[1])
	s.publish(p, 1, 20)
	s.end(d, nil, 0)

	acts := s.answer(first)
	if ack := connackIn(acts, n); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the first group's answer: n got %v, want CONNACK 0x00", showAll(repliesTo(acts, n)))
	}
	second := nextGroup(t, acts)
	if got, want := s.members(second), []string{"save m", "records 1", "leave d"}; !slices.Equal(got, want) {
		t.Fatalf("the first group's answer sent the group %v, want %v: the writes that waited, in store-request order", got, want)
	}

	s.publish(p, 2, 20)
	acts = s.answer(second)
	if ack := connackIn(acts, m); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the second group's answer: m got %v, want CONNACK 0x00", showAll(repliesTo(acts, m)))
	}
	if got := repliesTo(acts, p); len(got) != 1 || !isPuback(got[0], 1, 0) {
		t.Fatalf("the second group's answer: p got %v, want PUBACK 0x00 to its first PUBLISH", showAll(got))
	}
	if got, want := s.members(nextGroup(t, acts)), []string{"records 1"}; !slices.Equal(got, want) {
		t.Fatalf("the second group's answer sent the group %v, want %v: the publication that waited", got, want)
	}
	if got := repliesTo(s.settle(), p); len(got) != 1 || !isPuback(got[0], 2, 0) {
		t.Fatalf("the last group's answer: p got %v, want PUBACK 0x00 to its second PUBLISH", showAll(got))
	}
	s.expectRows()
}

// batchBound is the design's bound on a channel's publication batches
// (design.md, Execution, Capacities: "Accepted, unresolved publication
// batches are limited to 4 per channel and 8 in all. This counts
// accumulating, sealed and outstanding batches together"). Only $broadcast is
// built, so no test here reaches the 8 in all.
const batchBound = 4

// batchBytes is the design's byte limit on a publication batch (design.md,
// Execution, Capacities: "A publication batch holds
// publish_commit_max_records records or 4 MiB of message bytes").
const batchBytes = 4 << 20

// batchScript returns a script of a core with no publish_commit_interval
// and publish_commit_max_records 1, started on a store holding $broadcast,
// into which every publish goes: every publication is a batch of its own.
func batchScript(t *testing.T) *script {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Storage.PublishCommitMaxRecords = 1
	return newScriptOn(t, cfg, &broadcast)
}

// fillBound has conn publish at qos, a body of 20 bytes each, packet
// identifiers from first (none at QoS 0), until $broadcast, holding already
// publication batches unresolved, holds batchBound, each a publication
// alone (batchScript(t)); conn then asks for one more body. It fails the
// test unless each publication's body is granted below the bound, and the
// last ask, at the bound, is taken but given nothing, the core still taking
// events (Intake) while it has store credit (design.md, Execution,
// Communication: "While unresolved publication batches are at their bound,
// the core grants no publication body but takes every other event"). The ask
// waits; grantedAgain checks that it is granted.
func (s *script) fillBound(conn *connection.Conn, qos byte, first uint16, already int) {
	s.t.Helper()
	for n := already; n < batchBound; n++ {
		var id uint16
		if qos != 0 {
			id = first + uint16(n-already)
		}
		s.publishAt(conn, qos, id, 20)
	}
	if got := s.ask(conn, 20); got != 0 {
		s.t.Fatalf("a reader given %d bytes with %d publication batches of one channel unresolved; want no publication body granted at the bound",
			got, batchBound)
	}
	if !s.core.Intake() {
		s.t.Fatalf("Intake() = false at the batch bound with store credit %d; want every other event still taken", s.core.Credit())
	}
}

// grantedAgain fails the test unless acts, the decision that released a
// batch, grant conn the body of 20 bytes it asked for at the bound.
func (s *script) grantedAgain(acts []core.Action, conn *connection.Conn, when string) {
	s.t.Helper()
	if got := given(acts, conn); got != 20 {
		s.t.Fatalf("%s: the reader waiting at the bound was given %d bytes, want 20: %s releases a batch", when, got, when)
	}
}

// expectBatches checks Batches(), the publication batches and granted
// bodies' slots unresolved.
func (s *script) expectBatches(want int, when string) {
	s.t.Helper()
	if got := s.core.Batches(); got != want {
		s.t.Fatalf("%s: Batches() = %d, want %d (the bound is %d)", when, got, want, batchBound)
	}
}

// design.md, Execution, Communication: "The core does not take from the
// channel while it has no store credit. While unresolved publication batches
// are at their bound, the core grants no publication body but takes every
// other event (Capacities). Intake stops before the incoming budget fills,
// for QoS 0 and QoS 1 alike, in the order of that one channel"; Capacities:
// "Accepted, unresolved publication batches are limited to 4 per channel and
// 8 in all. This counts accumulating, sealed and outstanding batches
// together. Commit, refusal or abandonment releases a batch".
//
// For QoS 0 and for QoS 1: with no interval and publish_commit_max_records
// 1, p publishes four times: the first batch is sent, the other three wait
// behind it, and p's fifth ask is taken but not granted. The first batch's
// commit grants it; its publication reaches the bound again, and a sixth
// ask waits; once every batch is committed, it is granted, and, its body
// stored, nothing stays counted.
func TestIntakeStopsAtTheBatchBound(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		t.Run("QoS "+strconv.Itoa(int(qos)), func(t *testing.T) {
			s := batchScript(t)
			p := s.connected(connectOf(packet.V5, "p"))
			s.fillBound(p, qos, 1, 0)
			first := only[*core.Group](s)
			if got, want := s.members(first), []string{"records 1"}; !slices.Equal(got, want) {
				t.Fatalf("the group outstanding at the bound is %v, want %v: the first publication's batch", got, want)
			}
			s.grantedAgain(s.answer(first), p, "the first batch's commit")
			s.handOver(p, qos, uint16(qos)*(batchBound+1), 20)
			s.fillBound(p, qos, 0, batchBound)
			s.grantedAgain(s.settle(), p, "every batch's commit")
			s.handOver(p, qos, uint16(qos)*(batchBound+2), 20)
			s.settle()
			s.expectBatches(0, "every body stored")
		})
	}
}

// design.md, Execution, Capacities: "commit, refusal or abandonment
// releases a batch"; Communication: "While unresolved publication batches are
// at their bound, the core grants no publication body"; Message memory: "Once
// a stop begins or the store fails, the core grants no more"; R8: "A store
// error stops the broker".
//
// With no interval and publish_commit_max_records 1, p publishes four times
// at QoS 1, reaching the bound, and asks for a fifth body. The store
// refuses the first batch, full: the fifth is granted, and its publication
// reaches the bound again, a sixth ask waiting; the store commits the next
// batch: the sixth is granted. Every batch answered, nothing stays
// counted. Then d's durable CONNECT is read, and its session save goes at
// once; while it is outstanding, p publishes four times more, reaching the
// bound with the four batches waiting behind it, and asks again. The store
// answers the save with an error other than full: the four batches can no
// longer be stored, and their abandonment leaves none counted, while the
// ask, the store failed, is not granted.
func TestEveryEndReleasesItsBatch(t *testing.T) {
	s := batchScript(t)
	p := s.connected(connectOf(packet.V5, "p"))
	s.fillBound(p, 1, 1, 0)
	s.grantedAgain(s.reply(only[*core.Group](s), store.ErrFull), p, "a refusal")
	s.handOver(p, 1, 5, 20)
	s.fillBound(p, 1, 0, batchBound)
	s.grantedAgain(s.answer(only[*core.Group](s)), p, "a commit")
	s.handOver(p, 1, 6, 20)
	s.settle()
	s.expectBatches(0, "every batch answered")

	d := s.accept()
	s.receive(d, keeping(packet.V5, "d", 60), 0)
	s.answer(only[*store.LoadSession](s))
	save := only[*core.Group](s)
	s.fillBound(p, 1, 7, 0)
	if got := given(s.reply(save, errors.New("disk I/O error")), p); got != 0 {
		t.Fatalf("the store failed, and the reader waiting at the bound was given %d bytes; want none: the core grants no more", got)
	}
	s.expectBatches(0, "a store error abandoning the batches waiting")
}

// design.md, Message memory: "A granted body, unread or held behind its
// CONNECT, reserves one potential batch slot; batches and reserved slots
// together obey the batch bound"; Execution, Capacities: "Accepted,
// unresolved publication batches are limited to 4 per channel and 8 in all.
// This counts accumulating, sealed and outstanding batches together. Commit,
// refusal or abandonment releases a batch". So a reader may be given a body
// while its CONNECT is at the store, the body "held behind its CONNECT" (the
// core holds that PUBLISH until the CONNACK).
//
// With no interval and publish_commit_max_records 1, p is connected. c's
// CONNECT, Clean Start 1, is read, and while that read is unanswered c
// publishes once at QoS 1, held behind its CONNECT, its slot reserved.
// Three of p's publications reach the bound beside it, and p's next ask
// waits. The read's answer gives c its CONNACK, its publication taking its
// own slot: still four, p's ask still waiting. The first group's commit
// releases one: p's ask is granted, its body's slot making four again.
// Settled, c and p have every PUBACK, 0x00, and no slot is left.
func TestHeldPublicationsWaitForTheBound(t *testing.T) {
	s := batchScript(t)
	p := s.connected(connectOf(packet.V5, "p"))
	c := s.accept()
	s.receive(c, connectOf(packet.V5, "c"), 0)
	load := only[*store.LoadSession](s)
	s.publish(c, 1, 20)
	s.expectBatches(1, "c's publication held behind its CONNECT")
	s.fillBound(p, 1, 1, 1)
	s.expectBatches(batchBound, "p at the bound beside c's held publication")
	acts := s.answer(load)
	if ack := connackIn(acts, c); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the read's answer: c got %v, want CONNACK 0x00", showAll(repliesTo(acts, c)))
	}
	if got := given(acts, p); got != 0 {
		t.Fatalf("c's CONNACK, its held publication keeping its slot: p was given %d bytes, want none at the bound", got)
	}
	s.expectBatches(batchBound, "c's CONNACK, its held publication in its own slot")
	s.grantedAgain(s.answer(only[*core.Group](s)), p, "the first group's commit")
	s.expectBatches(batchBound, "a batch committed, p's granted body taking its slot")
	s.handOver(p, 1, 4, 20)
	acts = s.settle()
	if got := repliesTo(acts, c); len(got) != 1 || !isPuback(got[0], 1, 0) {
		t.Fatalf("settled, c got %v; want PUBACK 0x00 to its held PUBLISH", showAll(got))
	}
	s.expectBatches(0, "every batch answered")
}

// design.md, Execution, Capacities: "A publication batch holds
// publish_commit_max_records records or 4 MiB of message bytes"; Store,
// Writes: "A batch closes before either of its limits would be exceeded";
// "one otherwise valid publication larger than the byte limit forms a batch
// alone, fully charged"; "A write is never split: a channel's next
// publication starts another batch, which waits its turn"; "The limits hold
// with or without an interval"; design.md, Store, Requests: each channel
// "keeps at most one request of its own in flight, plus bounded pending
// work".
//
// With no interval, publish_commit_max_records 3 and max_packet_size 6 MiB,
// p publishes small messages but where named. Its first publication is sent
// at once, a group of one record; while it is outstanding, p publishes two
// of 2.5 MiB, then three small ones: the second large one would take the
// first's batch past 4 MiB, so that batch holds one record; the next holds
// the second large one and two small ones, three records; the last small
// one starts a fourth. Each batch goes as a group of its own, in that
// order. Every batch committed, p publishes one small message, sent at
// once; then, while it is outstanding, a small one, one over 4 MiB, and a
// small one: each forms a batch of its own, the large one's alone though
// under the record limit, and each goes as a group of its own. Each size is
// past the byte limit, or within it, however a PUBLISH's message bytes are
// counted: whole body or payload.
func TestBatchClosesAtEitherLimit(t *testing.T) {
	const small, half, over = 20, batchBytes * 5 / 8, batchBytes + 1024
	cfg := newConfig(t)
	cfg.Broker.Storage.PublishCommitMaxRecords = 3
	cfg.Broker.Limits.MaxPacketSize = 6 << 20
	s := newScriptOn(t, cfg, &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	for _, round := range []struct {
		what   string
		bodies []int64
		groups [][]string
	}{
		{"two of 2.5 MiB, then three small", []int64{half, half, small, small, small},
			[][]string{{"records 1"}, {"records 3"}, {"records 1"}}},
		{"a small one, one over 4 MiB, a small one", []int64{small, over, small},
			[][]string{{"records 1"}, {"records 1"}, {"records 1"}}},
	} {
		s.publish(p, 1, small)
		g := only[*core.Group](s)
		for i, n := range round.bodies {
			s.publish(p, uint16(2+i), n)
		}
		for _, want := range round.groups {
			g = nextGroup(t, s.answer(g))
			if got := s.members(g); !slices.Equal(got, want) {
				t.Fatalf("%s: the next group is %v, want %v", round.what, got, want)
			}
		}
		s.settle()
	}
}

// design.md, Store, Writes: "Its first publication fixes the core's
// collection deadline (R1). The group is due at that deadline [...] A due
// group keeps gathering until it can dispatch under the store-credit,
// sealed-group order and Store-read rules, unless a record limit, barrier or
// stop seals it first (Store reads). It seals and dispatches at the end of
// the first event permitting dispatch, after every member result of a
// transaction answer has been applied"; "Eligible writes join in their
// store-request order, within the read boundaries (Store reads)".
//
// With an interval: p, d and e are connected, d and e durable. n's durable
// CONNECT is read, and its session save, a write that is not a
// publication, goes at once, a group of its own. While it is outstanding,
// p publishes: the core's timer is set for the interval after the
// publication. The deadline comes with the save still outstanding; then
// d's and e's connections are lost, and their goings away join the due
// group. The save's answer makes n's CONNACK ready, then sends that group:
// the publication, and d's and e's goings away in that order.
func TestDueGroupKeepsGathering(t *testing.T) {
	s := collectingScript(t, unreached)
	p := s.connected(connectOf(packet.V5, "p"))
	d := s.connected(keeping(packet.V5, "d", 60))
	e := s.connected(keeping(packet.V5, "e", 60))
	n := s.accept()
	s.receive(n, keeping(packet.V5, "n", 60), 0)
	s.answer(only[*store.LoadSession](s))
	save := only[*core.Group](s)
	s.publish(p, 1, 20)
	deadline := s.now.Add(collection)
	if got := s.core.Timer(); !got.Equal(deadline) {
		t.Fatalf("Timer() = %v after the publication at %v, want %v, the collection deadline", got, s.now, deadline)
	}
	s.fireAt(deadline)
	s.end(d, nil, 0)
	s.end(e, nil, 0)

	acts := s.answer(save)
	if ack := connackIn(acts, n); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the save's answer: n got %v, want CONNACK 0x00", showAll(repliesTo(acts, n)))
	}
	if got, want := s.members(nextGroup(t, acts)), []string{"records 1", "leave d", "leave e"}; !slices.Equal(got, want) {
		t.Fatalf("the save's answer sent the group %v, want %v: the due group, with what it gathered after its deadline", got, want)
	}
	s.settle()
	s.expectRows()
}

// design.md, Store, Requests: "Each dispatched read and each dispatched
// write group consumes one store credit until its answer is processed";
// Execution, Communication: the core "does not take from [the readers'
// channel] while it has no store credit"; design.md, Store, Writes: "A due
// group keeps gathering until it can dispatch under the store-credit,
// sealed-group order and Store-read rules [...]. It seals and dispatches at
// the end of the first event permitting dispatch"; Store, reads: "Later
// writes wait until the reader confirms that its read transaction has
// established that view". Audit 261022-002a: a sealed group sent with no
// store credit passed every test. Its M13 is caught, the finding says, by
// "every credit spent on CONNECTs' reads and a connection's going away made
// due, requiring no group until a read is answered, then one".
//
// A durable MQTT 5 session "stays" is connected. store.Credit CONNECTs,
// each for a session of its own and none creating a write (clean start,
// ending with its connection), are then received: their LoadSession reads
// take every credit, none answered. These reads are earlier in
// store-request order than what follows, so no barrier of theirs seals an
// earlier write. "stays" then loses its connection: its going away, a
// LeaveSession and so a write that is not a publication, makes a group due
// at the end of its event, but no credit can dispatch it, so no group is
// sent. Each read's view is confirmed as it is sent; the first read's
// result returns one credit, and that event dispatches the due group,
// holding the one going away. No later event sends another.
func TestDueGroupWaitsForStoreCredit(t *testing.T) {
	s := newScript(t, newConfig(t))
	stays := s.connected(keeping(packet.V5, "stays", 60))
	for i := range store.Credit {
		s.receive(s.accept(), connectOf(packet.V5, "c"+strconv.Itoa(i)), 0)
	}
	if got := s.core.Credit(); got != 0 {
		t.Fatalf("Credit() = %d with every credit spent on CONNECTs' reads, want 0", got)
	}
	reads := pending[*store.LoadSession](s)
	if got := groupsIn(s.end(stays, nil, 0)); len(got) != 0 {
		t.Fatalf("the going away, made due with no store credit, sent %d groups; want none until a credit comes back", len(got))
	}
	acts := s.answer(reads[0])
	if got := groupsIn(acts); len(got) != 1 || !slices.Equal(s.members(got[0]), []string{"leave stays"}) {
		t.Fatalf("the first read's answer, a credit back, sent %v; want the due group, the one going away", writesIn(acts))
	}
	if got := groupsIn(s.settle()); len(got) != 0 {
		t.Fatalf("after the due group, %d more groups were sent; want it sent once", len(got))
	}
}

// design.md, Store, Writes: a due group keeps gathering "unless a record
// limit, barrier or stop seals it first"; "A Store-read barrier or a stop
// also seals it at once"; "Stop ends collection delays and drains accepted
// work under the shutdown rules".
//
// With an interval, p publishes at QoS 1, its publication collecting. The
// stop comes a tick later, long before the collection deadline: its own
// decision sends the group, the publication in it.
func TestStopSealsTheCollectingGroup(t *testing.T) {
	s := collectingScript(t, unreached)
	p := s.connected(connectOf(packet.V5, "p"))
	s.publish(p, 1, 20)
	acts := s.stop()
	if got := groupsIn(acts); len(got) != 1 || !slices.Equal(s.members(got[0]), []string{"records 1"}) {
		t.Fatalf("the stop, %v before the collection deadline, sent %v; want the collecting group, its publication", collection-s.tick, writesIn(acts))
	}
	s.settle()
}

// design.md, Store, Writes: "The first event making a group due or sealing
// it fixes its commit cause: event for dispatch without a collection delay,
// interval for the collection deadline, or max_records, barrier or stop for
// those boundaries. Later events do not change the cause"; "A write
// awaiting its owner's earlier request or another required predecessor
// stays pending"; "A Store-read barrier or a stop also seals it at once";
// "Stop ends collection delays"; "Success follows commit"; design.md, Store,
// Requests: "Applying a predecessor's result schedules its dependent
// request"; metrics.md: saguin_store_commits_total{closed_by},
// "successfully committed transactions, by their fixed commit cause", and
// saguin_store_committed_records_total, "records carried by successfully
// committed transactions".
//
// Without an interval, a publication's group goes at its event's end, and
// counts at its answer: event. The stop comes while it is outstanding and
// a second publication, $broadcast's next batch, waits behind it: not yet
// eligible, it is in no group the stop seals, and goes once the first
// answer is applied, in a later group: event. With an interval and
// publish_commit_max_records 3, each of these groups is committed in turn:
// a durable CONNECT's session save, a write that is not a publication,
// going at its event's end: event; a publication's, at its deadline:
// interval; three publications', at the record limit: max_records; a
// publication's, made due by its deadline while another CONNECT's save is
// outstanding, then sealed by the record limit: interval; a publication's
// that the stop sends: stop; and, after the stop, a's going away as its
// connection ends, a group formed afterwards, going without a collection
// delay: event, only the group the stop's own event seals taking stop. A
// publication's group answered full counts nothing, nor its record. Nothing
// here meets a Store-read barrier.
func TestCommitCause(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	s.publish(p, 1, 20)
	s.expectCommits(commits{})
	s.publish(p, 2, 20)
	s.stop()
	s.settle()
	s.expectCommits(commits{event: 2, records: 2})

	s = collectingScript(t, 3)
	p = s.connected(connectOf(packet.V5, "p"))
	a := s.connected(keeping(packet.V5, "a", 60))
	want := commits{event: 1}
	s.expectCommits(want)

	s.publish(p, 1, 20)
	s.fireAt(s.now.Add(collection))
	s.settle()
	want.interval, want.records = 1, 1
	s.expectCommits(want)

	s.publish(p, 2, 20)
	s.fireAt(s.now.Add(collection))
	s.reply(only[*core.Group](s), store.ErrFull)
	s.expectCommits(want)

	for id := uint16(3); id <= 5; id++ {
		s.publish(p, id, 20)
	}
	s.settle()
	want.maxRecords, want.records = 1, 4
	s.expectCommits(want)

	x := s.accept()
	s.receive(x, keeping(packet.V5, "x", 60), 0)
	s.answer(only[*store.LoadSession](s))
	s.publish(p, 6, 20)
	s.fireAt(s.now.Add(collection))
	s.publish(p, 7, 20)
	s.publish(p, 8, 20)
	s.settle()
	want.event, want.interval, want.records = 2, 2, 7
	s.expectCommits(want)

	s.publish(p, 9, 20)
	s.stop()
	s.settle()
	want.stop, want.records = 1, 8
	s.expectCommits(want)

	s.end(a, nil, 0)
	s.settle()
	want.event = 3
	s.expectCommits(want)
}

// design.md, Execution, Event order: "Equal-deadline core timers run in
// this order: graceful-stop cutoff, session expiry, Will publication,
// [...], commit cutoff, metric refresh, snapshot"; Store, Writes: "Its
// first publication fixes the core's collection deadline (R1)"; "A Store-read
// barrier or a stop also seals it at once"; "The first event making a group
// due or sealing it fixes its commit cause"; Store reads: "Earlier accepted
// writes are resolved before its view is fixed".
//
// With an interval: p is connected, and the start's read of the sessions
// due is outstanding when p publishes, its group collecting until the
// interval after. That read's answer gives the next session expiry at the
// same instant, for which the core's timer is set. When it comes, the
// session expiry's work goes first: its read of the sessions due seals the
// collecting group, which is sent while the read waits for it; the
// group's answer sends the read. The group is committed under barrier, not
// interval: the commit cutoff came after the read had sealed it.
func TestCommitCutoffAfterSessionExpiry(t *testing.T) {
	s := collectingScript(t, unreached)
	p := s.connected(connectOf(packet.V5, "p"))
	s.fire()
	s.publish(p, 1, 20)
	at := s.now.Add(collection)
	due := only[*store.DueSessions](s)
	due.Next = at.UnixMilli()
	s.reply(due, nil)
	checkTimer(t, s, at.UnixMilli())

	acts := s.fireAt(at)
	if got := groupsIn(acts); len(got) != 1 || !slices.Equal(s.members(got[0]), []string{"records 1"}) {
		t.Fatalf("at a session expiry and a collection deadline of one instant, the core sent %v; want the publication's group", writesIn(acts))
	}
	if slices.ContainsFunc(acts, func(a core.Action) bool { return a.Read != nil }) {
		t.Fatalf("the read of the sessions due was sent beside the group it sealed; want it sent once that group is answered")
	}
	acts = s.answer(only[*core.Group](s))
	if !slices.ContainsFunc(acts, func(a core.Action) bool { _, ok := a.Read.(*store.DueSessions); return ok }) {
		t.Fatalf("the sealed group's answer sent no read of the sessions due")
	}
	s.settle()
	s.expectCommits(commits{barrier: 1, records: 1})
}

// design.md, Store, Writes: "A Store-read barrier seals the earlier pending
// writes it covers. Later changes cannot join those writes or their
// groups"; "A Store-read barrier or a stop also seals it at once"; "The first
// event making a group due or sealing it fixes its commit cause [...]
// barrier"; "Sealed groups dispatch in order and never gain members while
// awaiting the write slot"; metrics.md: saguin_store_commits_total
// {closed_by}.
//
// With an interval: p is connected, and n's durable CONNECT is read, its
// session save, a write that is not a publication, going at once. While it
// is outstanding, p publishes, its publication collecting; then x's
// CONNECT comes, and its read of x's session seals that group at once. p
// publishes again, in a later event: the save's answer sends the sealed
// group, one record, and its answer sends x's read; the second
// publication, in a group of its own, goes at its own deadline. The sealed
// group is committed under barrier.
func TestReadBarrierSealsTheGroup(t *testing.T) {
	s := collectingScript(t, unreached)
	p := s.connected(connectOf(packet.V5, "p"))
	n := s.accept()
	s.receive(n, keeping(packet.V5, "n", 60), 0)
	s.answer(only[*store.LoadSession](s))
	save := only[*core.Group](s)
	s.publish(p, 1, 20)
	x := s.accept()
	s.receive(x, connectOf(packet.V5, "x"), 0)
	s.publish(p, 2, 20)

	acts := s.answer(save)
	if got, want := s.members(nextGroup(t, acts)), []string{"records 1"}; !slices.Equal(got, want) {
		t.Fatalf("the save's answer sent the group %v, want %v: the publication the barrier sealed, without the later one", got, want)
	}
	acts = s.answer(only[*core.Group](s))
	if !slices.ContainsFunc(acts, func(a core.Action) bool { _, ok := a.Read.(*store.LoadSession); return ok }) {
		t.Fatalf("the sealed group's answer sent no read of x's session")
	}
	s.settle()
	acts = s.fireAt(s.now.Add(collection))
	if got, want := s.members(nextGroup(t, acts)), []string{"records 1"}; !slices.Equal(got, want) {
		t.Fatalf("the later publication's deadline sent the group %v, want %v: the later publication, alone", got, want)
	}
	s.settle()
	s.expectCommits(commits{event: 1, barrier: 1, interval: 1, records: 2})
}

// design.md, Store, Writes: "Covered writes must be resolved before the read
// fixes its view"; Store reads: "Earlier accepted writes are resolved
// before its view is fixed"; "The core keeps at most one write transaction
// outstanding at the store".
//
// Without an interval: p is connected and publishes, its group sent at
// once. While that group is outstanding, p publishes again, its
// publication collecting behind it; then x's CONNECT comes, its read
// sealing that group. No read of x's session is sent while either group is
// unanswered: the first's answer sends the second group, and the second's
// answer, after p's PUBACK, sends the read.
func TestReadWaitsForTheGroupsItCovers(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	s.publish(p, 1, 20)
	first := only[*core.Group](s)
	s.publish(p, 2, 20)
	x := s.accept()
	reads := func(acts []core.Action) bool {
		return slices.ContainsFunc(acts, func(a core.Action) bool { return a.Read != nil })
	}
	if acts := s.receive(x, connectOf(packet.V5, "x"), 0); reads(acts) {
		t.Fatalf("x's CONNECT read its session while the groups it covers are unanswered")
	}
	acts := s.answer(first)
	second := nextGroup(t, acts)
	if reads(acts) {
		t.Fatalf("the first group's answer read x's session while the second, which x's CONNECT covers, is unanswered")
	}
	acts = s.answer(second)
	ack := slices.IndexFunc(acts, func(a core.Action) bool { return a.Conn == p && isPuback(a.Reply, 2, 0) })
	read := slices.IndexFunc(acts, func(a core.Action) bool { _, ok := a.Read.(*store.LoadSession); return ok })
	if ack < 0 || read < ack {
		t.Fatalf("the second group's answer: PUBACK at %d, read of x's session at %d of %d actions; want both, the PUBACK first", ack, read, len(acts))
	}
	s.settle()
}

// design.md, Store reads: "Later writes wait until the reader confirms that
// its read transaction has established that view [...]. Later writes may
// then proceed while the read finishes"; Store, Writes: "Later writes may
// proceed after the view-established confirmation, without waiting for
// the read to finish".
//
// Without an interval, the store holding back each read's confirmation:
// p is connected, and x's CONNECT is read. p then publishes, and pings: no
// group is sent while the read's view is unconfirmed. The confirmation
// sends the publication's group, before the read's result; the result
// then sends x's CONNACK.
func TestLaterWritesWaitForTheView(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	s.holdViews = true
	x := s.accept()
	s.receive(x, connectOf(packet.V5, "x"), 0)
	load := only[*store.LoadSession](s)
	if got := groupsIn(append(s.publish(p, 1, 20), s.ping(p)...)); len(got) != 0 {
		t.Fatalf("a publication sent %d groups with the read of x's session unconfirmed, want none", len(got))
	}
	if got, want := s.members(nextGroup(t, s.view(load))), []string{"records 1"}; !slices.Equal(got, want) {
		t.Fatalf("the read's confirmation sent the group %v, want %v: the publication that waited", got, want)
	}
	if acts := s.answer(load); connackIn(acts, x) == nil || connackIn(acts, x).ReasonCode != 0 {
		t.Fatalf("the read's result: x got %v, want CONNACK 0x00", showAll(repliesTo(acts, x)))
	}
	s.settle()
}

// otherChanges is the design's bound on a write group's writes that are not
// publications (Capacities: "A write group holds one batch bound of
// publications and 512 other changes"). Each session's going away here is
// one.
const otherChanges = 512

// design.md, Execution, Capacities: "A write group holds one batch bound of
// publications and 512 other changes"; Store, Writes: "Reaching a group's
// limit seals it at once, before a write that would take it past"; "Eligible
// writes join in their store-request order, within the read boundaries (Store
// reads)".
//
// Without an interval: p and 513 durable sessions, of client ids d000 up,
// are connected. n's durable CONNECT is read, and its session save goes at
// once, a group of its own. While it is outstanding, p publishes once at
// QoS 1, then every durable connection is lost, in client-id order: the
// publication and the goings away wait. The save's answer sends the first
// waiting group: the publication, outside the 512, and exactly the first
// 512 goings away, d000 to d511, in that order; its answer sends the next,
// d512's going away alone.
func TestGroupHoldsAtMost512OtherChanges(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	ids := make([]string, otherChanges+1)
	conns := make([]*connection.Conn, len(ids))
	for i := range ids {
		ids[i] = fmt.Sprintf("d%03d", i)
		conns[i] = s.connected(keeping(packet.V5, ids[i], 60))
	}
	n := s.accept()
	s.receive(n, keeping(packet.V5, "n", 60), 0)
	s.answer(only[*store.LoadSession](s))
	save := only[*core.Group](s)
	s.publish(p, 1, 20)
	for _, conn := range conns {
		s.end(conn, nil, 0)
	}

	want := make([]string, 0, 1+otherChanges)
	want = append(want, "records 1")
	for _, id := range ids[:otherChanges] {
		want = append(want, "leave "+id)
	}
	first := nextGroup(t, s.answer(save))
	if got := s.members(first); !slices.Equal(got, want) {
		t.Fatalf("the save's answer sent a group of %d members, beginning %v and ending %v; want %d: the publication, then d000 to d511's goings away in order",
			len(got), got[:min(3, len(got))], got[max(0, len(got)-2):], len(want))
	}
	if got, want := s.members(nextGroup(t, s.answer(first))), []string{"leave " + ids[otherChanges]}; !slices.Equal(got, want) {
		t.Fatalf("the first group's answer sent the group %v, want %v: the going away past the bound, in the next group", got, want)
	}
	s.settle()
	s.expectRows()
}
