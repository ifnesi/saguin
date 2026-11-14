package core_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// The tests of a connected consumer's position on a full disk (design.md,
// Store, A full disk), driven inside: which store request is refused, and
// when the retry comes, are orderings no client can force (design.md,
// Testing: "A test inside the code checks only what neither can see: [...]
// orderings no client can force"). They drive the core's decisions with
// scripted events, a scripted store and fake times, as core_test.go does.

// dirtyClient is the client id of the durable session the tests resume.
const dirtyClient = "dirty"

// dirtyTopic is the topic of the session's records, which its one
// subscription matches.
const dirtyTopic = "dirty/x"

// dirtyRecords is how many records $broadcast holds, offsets 1 to
// dirtyRecords, each in flight to the session when it resumes: its stored
// position is 1, below them all.
const dirtyRecords = 3

// retryAfter is design.md, Store, A full disk: "A housekeeping write
// refused for room sets a retry deadline one second after that answer".
const retryAfter = time.Second

// eventGap is how far apart the scripted events are: far below
// retryAfter, so that no deadline comes between them unasked.
const eventGap = time.Millisecond

// answerLimit is the watchdog of a script's store: more requests than
// any of these tests' events lead to. Reaching it is reported as the core
// asking without end, apart from any deadline under test.
const answerLimit = 1000

// sent is what the script keeps of a delivery handed to the writer.
type sent struct {
	id    uint16
	dup   bool
	topic string
	conn  *connection.Conn // the connection whose writer it was handed to
}

// positionScript plays its connections' writers and the store to a core:
// the store
// holds the session's position and its session_inflight rows, and the
// records publications commit after $broadcast's first dirtyRecords and
// into each configured channel; it applies every write the core sends
// unless the disk is full, and answers each read from them, a metadata read
// from its own channel's records.
type positionScript struct {
	clock
	reg  *metrics.Registry
	conn *connection.Conn
	full bool // the store answers every write "full" (store.ErrFull)
	// stuck holds the writer on its socket: serve returns none of the
	// deliveries handed to it, until the test does (unstick).
	stuck bool

	position int64                             // the session's stored position on $broadcast
	rows     map[uint16]int64                  // its stored session_inflight rows: packet identifier to offset
	seqs     map[uint16]int64                  // and their seq
	unsent   map[uint16]bool                   // those the core saved and no writer has been handed since: its preparations
	records  map[int64]*store.Record           // $broadcast's committed records past dirtyRecords, by offset
	routed   map[int64]map[int64]*store.Record // each configured channel's committed records, by channel id, then offset

	acts       []core.Action                  // every action the core decided, in order
	pending    []any                          // requests sent and not yet answered, oldest first
	hold       func(*core.Group) bool         // if set, the groups serve leaves to the test, unanswered
	held       []*core.Group                  // those groups, oldest first
	holdRead   func(store.Read) bool          // if set, the reads serve leaves to the test, unanswered
	heldReads  []store.Read                   // those reads, oldest first
	deliveries []sent                         // deliveries handed to the writer, in order
	toWriter   []core.Action                  // those not yet returned as written, each with its connection: the core reuses each once it is
	saved      []store.DeliveryChanges        // delivery changes committed, in order
	committed  time.Time                      // when the store last answered a write of records, committing them
	grantedAt  map[*connection.Conn]time.Time // when the core last gave each connection's reader a body
	refused    []store.DeliveryChanges        // delivery changes answered "full", in order
	refusedAt  []time.Time                    // when each refusal was answered
	answers    int
}

// newPositionScript starts a core on a store whose $broadcast holds
// dirtyRecords records, and a durable session, away, subscribed at QoS 1
// to them, its position at 1 and every record in flight to it under packet
// identifiers 1 up, moves 1 (storage.md, session_inflight).
func newPositionScript(t *testing.T) *positionScript {
	return newPositionScriptOf(t, newConfig(t), dirtyRecords)
}

// newPositionScriptOf is newPositionScript of a core for cfg, the first
// inflight records in flight to the session, the rest not yet delivered.
func newPositionScriptOf(t *testing.T, cfg *config.Config, inflight int) *positionScript {
	reg := metrics.New()
	state := &store.State{HighestSession: 1, Channels: []store.Channel{
		{ID: 1, Name: "$broadcast", Type: store.Append, Next: dirtyRecords + 1, Floor: 1},
	}}
	s := &positionScript{t: t, core: core.New(cfg, reg, state, nil, nil, nil, logger.NewCredits(cfg.Broker.LogLevel, reg)),
		reg: reg, now: epoch, position: 1, rows: map[uint16]int64{}, seqs: map[uint16]int64{}, unsent: map[uint16]bool{},
		records: map[int64]*store.Record{}, routed: map[int64]map[int64]*store.Record{},
		grantedAt: map[*connection.Conn]time.Time{}}
	s.run = s
	for i := range inflight {
		s.rows[uint16(i+1)], s.seqs[uint16(i+1)] = int64(i+1), int64(i+1)
	}
	s.take(s.core.Fire(nil, s.now))
	return s
}

// event hands the core ev, eventGap after the last event, then serves
// what it leads to; it returns the time the core took ev.
func (s *positionScript) event(ev connection.Event) time.Time {
	s.t.Helper()
	s.now = s.now.Add(eventGap)
	at := s.now
	s.take(s.core.Handle(nil, ev, at))
	s.serve()
	return at
}

// take keeps a decision's actions, its store requests and deliveries.
func (s *positionScript) take(acts []core.Action) {
	s.acts = append(s.acts, acts...)
	for _, a := range acts {
		if a.Bytes != 0 {
			s.grantedAt[a.Conn] = s.now
		}
		switch {
		case a.Read != nil:
			s.pending = append(s.pending, a.Read)
		case a.Write != nil:
			s.pending = append(s.pending, a.Write)
		}
		if d, ok := a.Reply.(*connection.Delivery); ok {
			s.deliveries = append(s.deliveries, sent{d.PacketID, d.Dup, string(d.Topic), a.Conn})
			delete(s.unsent, d.PacketID)
			s.toWriter = append(s.toWriter, a)
		}
	}
}

// serve answers every request outstanding, oldest first, and returns
// every delivery as written, unless the writer is stuck, until the core
// asks for nothing more.
func (s *positionScript) serve() {
	s.t.Helper()
	for len(s.pending) > 0 || (len(s.toWriter) > 0 && !s.stuck) {
		s.answers++
		if s.answers > answerLimit {
			s.t.Fatalf("watchdog: the core still asks after %d answers", answerLimit)
		}
		if len(s.pending) == 0 {
			s.writeNext()
			continue
		}
		op := s.pending[0]
		s.pending = s.pending[1:]
		s.now = s.now.Add(eventGap)
		switch op := op.(type) {
		case store.Read:
			if s.holdRead != nil && s.holdRead(op) {
				s.heldReads = append(s.heldReads, op)
				continue
			}
			s.answerRead(op)
		case *core.Group:
			if s.hold != nil && s.hold(op) {
				s.held = append(s.held, op)
				continue
			}
			var err error
			if s.full {
				err = store.ErrFull
			}
			s.write(op, err)
			s.take(s.core.Answer(nil, store.Answer{Op: op, Err: err}, s.now))
		default:
			s.t.Fatalf("the core sent %T, neither a read nor a group", op)
		}
	}
}

// writeNext returns the oldest delivery handed to a writer, and not yet
// returned, as written, eventGap after the last event.
func (s *positionScript) writeNext() {
	a := s.toWriter[0]
	s.toWriter = s.toWriter[1:]
	d := a.Reply.(*connection.Delivery)
	d.Result = connection.DeliveryWritten
	s.now = s.now.Add(eventGap)
	s.take(s.core.Handle(nil, connection.Event{Kind: connection.Delivered, Conn: a.Conn, Packet: d}, s.now))
}

// writeAll returns every delivery handed to a writer, and not yet
// returned, as written, a stuck writer's too, then serves what that leads
// to.
func (s *positionScript) writeAll() {
	s.t.Helper()
	for len(s.toWriter) > 0 {
		s.writeNext()
	}
	s.serve()
}

// scripted is what a script does with the core's decisions: keeps one
// (take), and carries out what they ask, until the core asks for nothing
// more (serve).
type scripted interface {
	take(acts []core.Action)
	serve()
}

// clock is the core's one timer as a script drives it: the core, the
// script's time, and the script itself, run, which keeps each decision on
// the timer and serves what it leads to. positionScript and liveScript
// embed it.
type clock struct {
	t    *testing.T
	core *core.Core
	now  time.Time
	run  scripted
}

// fireAt hands the core its timer at at, then serves what it leads to.
func (c *clock) fireAt(at time.Time) {
	c.t.Helper()
	c.now = at
	c.run.take(c.core.Fire(nil, c.now))
	c.run.serve()
}

// advance runs the core's timer at each deadline it sets up to to, as the
// loop around the core does (design.md, Testing: it "owns the one real
// timer"), serving what each leads to, then moves the script's clock to
// to; a timer still due after answerLimit fires is a core asking for its
// timer without end.
func (c *clock) advance(to time.Time) {
	c.t.Helper()
	for fires := 0; ; fires++ {
		at := c.core.Timer()
		if at.IsZero() || at.After(to) {
			break
		}
		if fires == answerLimit {
			c.t.Fatalf("watchdog: the core's timer still comes due at %v after %d fires, before %v", at, answerLimit, to)
		}
		c.fireAt(later(at, c.now))
	}
	c.now = later(to, c.now)
}

// later is the later of a and b.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// release answers held, a group serve left to the test, with err, then
// serves what that leads to.
func (s *positionScript) release(held *core.Group, err error) {
	s.t.Helper()
	i := slices.Index(s.held, held)
	if i < 0 {
		s.t.Fatalf("an answer to group %p, which is not held", held)
	}
	s.held = slices.Delete(s.held, i, i+1)
	s.now = s.now.Add(eventGap)
	s.write(held, err)
	s.take(s.core.Answer(nil, store.Answer{Op: held, Err: err}, s.now))
	s.serve()
}

// answerRead answers a read: its view established, then its result, the
// one the store holds.
func (s *positionScript) answerRead(op store.Read) {
	s.take(s.core.Answer(nil, store.Answer{Op: op, View: true}, s.now))
	s.fill(op)
	s.take(s.core.Answer(nil, store.Answer{Op: op}, s.now))
}

// releaseRead answers held, a read serve left to the test, then serves
// what that leads to.
func (s *positionScript) releaseRead(held store.Read) {
	s.t.Helper()
	i := slices.Index(s.heldReads, held)
	if i < 0 {
		s.t.Fatalf("an answer to read %T, which is not held", held)
	}
	s.heldReads = slices.Delete(s.heldReads, i, i+1)
	s.now = s.now.Add(eventGap)
	s.answerRead(held)
	s.serve()
}

// fill gives a read the result the store holds.
func (s *positionScript) fill(r store.Read) {
	s.t.Helper()
	switch r := r.(type) {
	case *store.LoadSession:
		if r.Client != dirtyClient {
			return
		}
		r.Found = true
		r.Session = store.Session{ID: 1, Client: dirtyClient, Protocol: 5, Expiry: 3600, Left: epoch.UnixMilli(),
			ReceiveMaximum: 65535, Listener: "mqtt", Remote: "10.0.0.1:40000"}
		r.Subscriptions = map[string]store.Subscription{"dirty/#": {Filter: "dirty/#", QoS: 1,
			Starts: []store.SubscriptionStart{{Channel: 1, Start: 1}}}}
		r.Position = s.position
		for _, seq := range s.seqs {
			r.Sequence = max(r.Sequence, seq)
		}
	case *store.ReadSessionDeliveries:
		ids := slices.Sorted(maps.Keys(s.rows))
		slices.SortFunc(ids, func(a, b uint16) int { return int(s.seqs[a] - s.seqs[b]) })
		for _, id := range ids {
			if seq := s.seqs[id]; seq > r.After && seq <= r.Through && len(r.Rows) < r.Limit {
				r.Rows = append(r.Rows, store.RecoveredDelivery{PacketID: id, Seq: seq, Offset: s.rows[id], QoS: 1,
					Record: store.RecordMetadata{Offset: s.rows[id], Topic: []byte(dirtyTopic), QoS: 1, PayloadSize: 1}})
			}
		}
	case *store.ReadRecordMetadata:
		records := s.routed[r.Channel]
		if r.Channel == 1 {
			records = s.records
			// The first dirtyRecords are no current retained value.
			for o := max(r.From, 1); !r.Retained && o < min(r.Before, dirtyRecords+1) && len(r.Rows) < r.Limit; o++ {
				r.Rows = append(r.Rows, store.RecordMetadata{Offset: o, Topic: []byte(dirtyTopic), QoS: 1, PayloadSize: 1})
			}
		}
		for _, o := range slices.Sorted(maps.Keys(records)) {
			rec := records[o]
			if o >= r.From && o < r.Before && (!r.Retained || rec.Retained) && len(r.Rows) < r.Limit {
				r.Rows = append(r.Rows, store.RecordMetadata{Offset: o, Topic: []byte(rec.Topic), Properties: rec.Properties,
					QoS: rec.QoS, Publisher: rec.Publisher, Expires: rec.Expires, PayloadSize: max(len(rec.Payload), rec.PayloadSize),
					StoredPayloadSize: len(rec.StoredPayload), PublishedRetained: rec.PublishedRetained})
			}
		}
	case *store.DueSessions:
		// The session's expiry is an hour away: none is due.
	default:
		if page, done := core.ChargesOf(r); done != nil {
			for _, charge := range page {
				for _, rec := range s.records {
					if rec.Retained && rec.Topic == charge.Topic {
						charge.Bytes = rec.Bytes()
					}
				}
			}
			done()
			return
		}
		if reads := core.PayloadsOf(r); len(reads) > 0 {
			for _, p := range reads {
				*p.Into = []byte("x")
			}
			return
		}
		s.t.Fatalf("the core read %T, which these tests do not script", r)
	}
}

// write applies a group's records, a retained one replacing its topic's
// current value, and its delivery changes to the store, or, refused, keeps
// the delivery changes as refused. A committed deletion is one of the design's two:
// a settled exchange's, which must leave the position past its row's
// record (design.md, Delivery, Positions, "Deleting a durable session's
// settled exchange row must commit with its position beyond that record,
// unless a seek supersedes that progress.": these tests script no seek), or a
// cancelled preparation's, a row the core saved and no writer was handed
// since, which must leave the position at or before its record
// (Preparation, "deliveries prepared and never handed to the writer are
// cancelled: their exchanges are deleted in the departure's transaction; a
// live scan's position stays before their records": the script's store
// holds no retained message, so every preparation is a live scan's). A row
// the store held before the core started, or one a writer was handed, has
// been sent, so only the first rule can allow its deletion.
func (s *positionScript) write(g *core.Group, err error) {
	s.t.Helper()
	for _, w := range g.Writes {
		if err == nil && len(core.RecordsOf(w)) != 0 {
			s.committed = s.now
		}
		for i, records := 0, core.RecordsOf(w); err == nil && i < len(records); i++ {
			rec := &records[i]
			if rec.Channel != 1 {
				if s.routed[rec.Channel] == nil {
					s.routed[rec.Channel] = map[int64]*store.Record{}
				}
				s.routed[rec.Channel][rec.Offset] = rec
				continue
			}
			for _, old := range s.records {
				if rec.PublishedRetained && old.Topic == rec.Topic {
					old.Retained = false // replaced or cleared (design.md, Channels, Retained)
				}
			}
			s.records[rec.Offset] = rec
		}
		d, ok := w.(*store.DeliveryChanges)
		if !ok {
			continue
		}
		if err != nil {
			s.refused = append(s.refused, *d)
			s.refusedAt = append(s.refusedAt, s.now)
			continue
		}
		s.saved = append(s.saved, *d)
		for _, row := range d.Save {
			if _, ok := s.rows[row.PacketID]; !ok {
				s.unsent[row.PacketID] = true
			}
			s.rows[row.PacketID], s.seqs[row.PacketID] = row.Offset, row.Seq
		}
		if d.Position != 0 {
			s.position = d.Position
		}
		for _, id := range d.Delete {
			offset, ok := s.rows[id]
			switch {
			case !ok:
			case s.unsent[id] && s.position > offset:
				s.t.Errorf("a commit deletes the row of identifier %d, offset %d, a cancelled preparation, with the position at %d, past it", id, offset, s.position)
			case !s.unsent[id] && s.position <= offset:
				s.t.Errorf("a commit deletes the row of identifier %d, offset %d, with the position at %d, not beyond it", id, offset, s.position)
			}
			delete(s.rows, id)
			delete(s.seqs, id)
			delete(s.unsent, id)
		}
	}
}

// resume connects the session's client, MQTT 5, Clean Start 0, and serves
// the resume, failing the test unless the client then holds every stored
// exchange again, DUP 1, under its identifier (design.md, CONNECT, 9.
// Resume): the stored position below them, and the exchanges unsettled,
// established in the store and on the connection.
func (s *positionScript) resume() {
	s.t.Helper()
	s.conn = new(connection.Conn)
	s.connect(s.conn, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	if len(s.deliveries) != dirtyRecords {
		s.t.Fatalf("the resumed session got %d deliveries; want its %d stored exchanges", len(s.deliveries), dirtyRecords)
	}
	for i, d := range s.deliveries {
		if !d.dup || d.id != uint16(i+1) {
			s.t.Fatalf("resend %d: DUP %v, identifier %d; want DUP 1, identifier %d", i+1, d.dup, d.id, i+1)
		}
	}
	if s.position != 1 || len(s.rows) != dirtyRecords {
		s.t.Fatalf("after the resume, the store holds position %d and %d rows; want 1 and %d", s.position, len(s.rows), dirtyRecords)
	}
}

// connect hands the core conn's Accepted, from the mqtt listener, then its
// CONNECT, p, serving what each leads to; it returns the time the core took
// the CONNECT.
func (s *positionScript) connect(conn *connection.Conn, p *packet.Connect) time.Time {
	s.t.Helper()
	s.event(connection.Event{Kind: connection.Accepted, Conn: conn,
		Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 40000}}})
	return s.event(connection.Event{Kind: connection.Received, Conn: conn, Packet: p})
}

// puback hands the core the client's PUBACK of id.
func (s *positionScript) puback(id uint16) {
	s.t.Helper()
	s.ack(s.conn, id)
}

// ack hands the core conn's PUBACK of id.
func (s *positionScript) ack(conn *connection.Conn, id uint16) {
	s.t.Helper()
	s.event(connection.Event{Kind: connection.Received, Conn: conn, Packet: &packet.Ack{PacketID: id}})
}

// bodyOf is the MQTT 5 body of p in bytes, including its property length.
func bodyOf(p *packet.Publish) int64 {
	prefix, _ := packet.AppendVarInt(nil, uint32(len(p.Properties)))
	body := int64(2 + len(p.Topic) + len(prefix) + len(p.Properties) + len(p.Payload))
	if p.QoS != 0 {
		body += 2 // the Packet Identifier
	}
	return body
}

// ask has conn's reader ask for p's body, and returns the bytes the core
// gives it in that event.
func (s *positionScript) ask(conn *connection.Conn, p *packet.Publish) int64 {
	s.t.Helper()
	before := given(s.acts, conn)
	s.event(connection.Event{Kind: connection.BytesRequested, Conn: conn, Bytes: bodyOf(p)})
	return given(s.acts, conn) - before
}

// hand has conn's reader hand p over, its body given.
func (s *positionScript) hand(conn *connection.Conn, p *packet.Publish) {
	s.t.Helper()
	s.event(connection.Event{Kind: connection.Received, Conn: conn, Packet: p, Bytes: bodyOf(p)})
}

// publish has conn's reader ask for p's body, MQTT 5 with no properties,
// failing the test unless it is given, then hand p over.
func (s *positionScript) publish(conn *connection.Conn, p *packet.Publish) {
	s.t.Helper()
	if got := s.ask(conn, p); got != bodyOf(p) {
		s.t.Fatalf("a reader asked for a PUBLISH body of %d bytes and was given %d", bodyOf(p), got)
	}
	s.hand(conn, p)
}

// subscribe hands the core conn's SUBSCRIBE of filter at QoS 0.
func (s *positionScript) subscribe(conn *connection.Conn, id uint16, filter string) {
	s.t.Helper()
	s.subscribeAt(conn, id, filter, 0)
}

// subscribeAt hands the core conn's SUBSCRIBE of filter at qos.
func (s *positionScript) subscribeAt(conn *connection.Conn, id uint16, filter string, qos byte) {
	s.t.Helper()
	s.event(connection.Event{Kind: connection.Received, Conn: conn, Packet: &connection.Subscribe{PacketID: id,
		Filters: []connection.Subscription{{Filter: filter, QoS: qos}}}})
}

// refusedPositions returns the positions the refused delivery changes
// carried, 0 for none.
func (s *positionScript) refusedPositions() []int64 {
	var ps []int64
	for _, d := range s.refused {
		if d.Position != 0 {
			ps = append(ps, d.Position)
		}
	}
	return ps
}

// fullThenRoom is the part both tests share: the session resumes; with the
// disk full, the client acknowledges the first two exchanges; the first
// PUBACK's position save is answered "full", which sets the retry
// deadline one second after that answer, and the second, even if the core
// tries again on it and is refused again, does not postpone it. It
// returns that deadline, which the test drives the core to (retriedAt).
func (s *positionScript) fullThenRoom() time.Time {
	s.t.Helper()
	s.resume()
	s.full = true
	s.puback(1)
	if len(s.refusedPositions()) == 0 {
		s.t.Fatalf("on a full disk, the PUBACK of 1 led to no position save answered full; refused changes %+v", s.refused)
	}
	first := slices.IndexFunc(s.refused, func(d store.DeliveryChanges) bool { return d.Position != 0 })
	if p := s.refused[first].Position; p <= 1 {
		t := s.t
		t.Fatalf("the refused save carried position %d; want it past the acknowledged record 1", p)
	}
	deadline := s.refusedAt[first].Add(retryAfter)
	s.puback(2)
	if s.position != 1 || len(s.rows) != dirtyRecords {
		s.t.Fatalf("with the disk full, the store holds position %d and %d rows; want 1 and %d, every save refused", s.position, len(s.rows), dirtyRecords)
	}
	return deadline
}

// retriedAt drives the core through its timer, as the loop around it does
// (advance), and fails the test unless the full-store retry asks for its
// writes exactly at deadline: every deadline the core sets before it fires
// and asks for no delivery change, the core's timer is then deadline, and
// its fire asks for one. Other deadlines may come first, such as the
// delivery admission gate's: a full refusal's retry wait counts as broker
// delay (design.md, Execution, Capacities, Delivery admission). when names
// the refusal deadline follows, for the failure messages.
func (s *positionScript) retriedAt(deadline time.Time, when string) {
	s.t.Helper()
	asked := len(s.saved) + len(s.refused)
	s.advance(deadline.Add(-time.Nanosecond))
	if n := len(s.saved) + len(s.refused) - asked; n != 0 {
		s.t.Fatalf("%s, the core asked for %d delivery changes before %v; want none before the retry, one second after that answer", when, n, deadline)
	}
	if got := s.core.Timer(); !got.Equal(deadline) {
		s.t.Fatalf("%s, every earlier deadline fired, the core's timer is %v; want %v, one second after that answer: an existing earlier retry is not postponed", when, got, deadline)
	}
	s.fireAt(deadline)
	if len(s.saved)+len(s.refused) == asked {
		s.t.Fatalf("%s, at the retry deadline %v the core asked for no delivery change; want the dirty position retried", when, deadline)
	}
}

// latestSaved fails the test unless the store holds the latest position:
// past the two acknowledged records, at least as far as any refused save
// asked, every committed position as far (no intermediate position ever
// written), and the two settled exchanges' rows gone.
func (s *positionScript) latestSaved(when string) {
	s.t.Helper()
	least := int64(3)
	if ps := s.refusedPositions(); len(ps) > 0 {
		least = max(least, slices.Max(ps))
	}
	if s.position < least {
		s.t.Errorf("%s, the stored position is %d; want the latest, at least %d", when, s.position, least)
	}
	for _, d := range s.saved {
		if d.Position != 0 && d.Position < least {
			s.t.Errorf("%s, a commit wrote position %d, an intermediate one; want only the latest, at least %d", when, d.Position, least)
		}
	}
	for _, id := range []uint16{1, 2} {
		if _, ok := s.rows[id]; ok {
			s.t.Errorf("%s, the store still holds the row of identifier %d, acknowledged", when, id)
		}
	}
}

// design.md, Store, A full disk: "For a connected consumer's position save,
// the core retains its latest position as one bounded dirty mark. A full
// refusal leaves that mark dirty. The next successful opportunity saves the
// latest position. Intermediate positions are not queued."; "A housekeeping
// write refused for room sets a retry deadline one second after that answer.
// An existing earlier retry is not postponed. At the deadline, the core [...]
// retries connected consumers' dirty-position marks"; the full-store retry
// covers position saves (the maintainer's decision of 261024). Delivery,
// Positions: "Deleting a durable session's settled exchange row must commit
// with its position beyond that record, unless a seek supersedes that
// progress."
//
// A durable session resumes, the store holding its position at 1 and
// three exchanges in flight; the client holds them again, DUP 1. With the
// disk full, it acknowledges the first: the position save that follows is
// answered "full", which sets the retry one second after that answer. It
// acknowledges the second: the retry is not postponed. With room back, the
// core, driven through every deadline its timer sets, asks for no save
// before the retry, and asks for it exactly one second after that answer.
// The store then holds the latest position, past both, every committed
// position as far, and the two settled rows gone, each deleted with the
// position beyond its record.
func TestDirtyPositionSavedAtTheRetry(t *testing.T) {
	s := newPositionScript(t)
	deadline := s.fullThenRoom()
	s.full = false
	s.retriedAt(deadline, "after the refused position save")
	s.latestSaved("at the retry deadline, with room back")
}

// design.md, Store, A full disk: "Another full refusal schedules the next
// retry"; "A full refusal leaves that mark dirty".
//
// As TestDirtyPositionSavedAtTheRetry, but the disk is still full at the
// deadline: the retried save is answered "full", and the store keeps
// position 1. With room back, the core asks for no save before the next
// retry, and asks for it exactly one second after that answer; the store
// then holds the latest position.
func TestFullRetryRetriesAgain(t *testing.T) {
	s := newPositionScript(t)
	deadline := s.fullThenRoom()
	before := len(s.refused)
	s.retriedAt(deadline, "after the refused position save, on a full disk")
	if len(s.refused) == before {
		t.Fatalf("at the retry deadline, on a full disk, no save was tried; want the dirty position retried")
	}
	if s.position != 1 {
		t.Fatalf("the retried save refused, the store holds position %d; want 1", s.position)
	}
	next := s.refusedAt[len(s.refusedAt)-1].Add(retryAfter)
	s.full = false
	s.retriedAt(next, "after the retried save was refused")
	s.latestSaved("at the next retry deadline, with room back")
}

// savesOffset reports whether g saves an exchange of the record at offset.
func savesOffset(g *core.Group, offset int64) bool {
	return slices.ContainsFunc(g.Writes, func(w store.Write) bool {
		d, ok := w.(*store.DeliveryChanges)
		return ok && slices.ContainsFunc(d.Save, func(row store.SessionDelivery) bool { return row.Offset == offset })
	})
}

// design.md, Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records, except that a
// durable session may also pass records represented by stored QoS 1 or QoS
// 2 exchanges"; "a failed save leaves it unsent". Delivery, Preparation:
// "each consumer's live scan may select records ahead of the client's free
// window. It may keep at most one window of selected deliveries not yet
// handed to the writer. A selected QoS 1 or 2 delivery has its packet
// identifier and, for a durable session, its stored exchange". A record whose
// exchange save is refused is neither settled, skipped nor represented, so no
// committed position passes it: the session's next connection gets it.
// "A connection's prepared and sent deliveries together are at most twice
// its window"; `$saguin` topics, Every request: "One descriptor within the
// connection's twice-window bound is reserved for direct answers", so
// ordinary deliveries have twice the window minus one: a window of 1
// prepares nothing ahead of its one sent delivery, and a window of 2 one.
//
// The session's in-flight window is 2 (Receive Maximum 2), its stored
// position 1, nothing in flight. It is handed offsets 1 and 2; the core
// prepares offset 3, whose exchange save the store holds. Its writer is
// stuck, so offsets 1 and 2 hold their reservations, and
// limits.max_outgoing_memory is what those two reserve, as a first script
// on the same records measures it (saguin_memory_in_transit_bytes, the
// outgoing budget's): so offset 3, once the client acknowledges offsets 1
// and 2, waits for its payload reservation (design.md, Message memory: "A
// delivery reservation ends when the writer reports completion or
// discard"). The client leaves, and its writer reports what it held; the
// held save is answered "full"; the settlements of offsets 1 and 2 and the
// departure then commit, and at the retry deadline anything still dirty.
// Every committed position is at most 3, and the store ends at 3.
func TestRefusedPreparationIsNeverSkipped(t *testing.T) {
	// handed connects the session to a script on cfg, its writer stuck, and
	// returns the script once offsets 1 and 2 are handed to it.
	handed := func(cfg *config.Config) *positionScript {
		s := newPositionScriptOf(t, cfg, 0)
		s.hold = func(g *core.Group) bool { return savesOffset(g, 3) }
		s.stuck = true
		s.conn = new(connection.Conn)
		s.connect(s.conn, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 2})
		if len(s.deliveries) != 2 || len(s.held) != 1 {
			t.Fatalf("with a window of 2, the session got %d deliveries and the store holds %d saves of offset 3; want 2 and 1, offset 3 prepared ahead",
				len(s.deliveries), len(s.held))
		}
		return s
	}
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxOutgoingMemory = config.Size(inTransit(t, handed(newConfig(t)).reg, "outgoing"))
	s := handed(cfg)

	s.puback(1)
	s.puback(2)
	s.event(connection.Event{Kind: connection.Ended, Conn: s.conn, Packet: &packet.Disconnect{}})
	s.stuck = false
	s.writeAll()
	s.release(s.held[0], store.ErrFull)
	if at := s.core.Timer(); !at.IsZero() {
		s.fireAt(at)
	}

	for _, d := range s.saved {
		if d.Position > 3 {
			t.Errorf("a commit wrote position %d, past offset 3, whose exchange save was refused: the next connection never gets it", d.Position)
		}
	}
	if s.position != 3 {
		t.Errorf("the store ends at position %d; want 3, past the acknowledged offsets 1 and 2", s.position)
	}
}

// savesSessionOf reports whether g stores client's session row.
func savesSessionOf(g *core.Group, client string) bool {
	return slices.ContainsFunc(g.Writes, func(w store.Write) bool {
		save, ok := w.(*store.SaveSession)
		return ok && save.Session.Client == client
	})
}

// design.md, CONNECT: "3. Order. While an earlier CONNECT for the same id
// waits for the store, this one waits too"; "Unless the id is connected, the
// core reads its session from the store, including its Will"; "An old
// connection for the id gets DISCONNECT 0x8E on MQTT 5 and is closed"; "Once
// the changes are stored, the broker sends CONNACK"; "Session Present is 1
// when the CONNECT keeps its session and a session exists for the client id,
// either away or connected with a session expiry above 0. Keeping the session
// means Clean Start 0, or MQTT 3.1.1 Clean Session 0". R2: "The core never
// waits. It never blocks on a socket, the store, a clock or a log: it sends a
// request and handles the answer as a later event". Store, Requests: "A
// dependent request becomes eligible after its predecessor's result is
// applied; unrelated owners continue". Store reads: "Every other read used
// for a broker decision has a place in the core's store-request order". Which
// request the store answers first is an ordering no client can force
// (Testing).
//
// The order: client z, durable, is connected. Client dirty's CONNECT (B)
// resumes its away session, three exchanges stored to resend, and the
// store holds B's session save. z leaves, its session going away: a write
// behind B's. Client y's CONNECT (its session load, X, a decision read)
// waits behind them. A second CONNECT for dirty (C) waits for B's. The
// store then answers B's save: B gets its CONNACK and starts its resume
// (its read of the stored exchanges, made after C), z's write and X follow.
// B is taken over, its connection ends and closes, and a new client w
// connects, its session to load. However the core orders what follows, C
// gets its CONNACK, Session Present 1, and so do y and w: the takeover
// waits on store answers, never on itself, and the session loads of
// unrelated clients are still served.
func TestTakeoverBehindAHeldDecisionRead(t *testing.T) {
	s := newPositionScript(t)
	z := new(connection.Conn)
	s.connect(z, &packet.Connect{Version: packet.V5, ClientID: "z", SessionExpiryInterval: 3600})
	if ack := connackIn(s.acts, z); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("z's CONNACK is %+v; want 0x00", ack)
	}

	s.hold = func(g *core.Group) bool { return savesSessionOf(g, dirtyClient) }
	b := new(connection.Conn)
	s.conn = b
	s.connect(b, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	s.hold = nil
	if len(s.held) != 1 || connackIn(s.acts, b) != nil {
		t.Fatalf("the store holds %d saves of B's session, B's CONNACK %+v; want its one save held, no CONNACK before it is stored",
			len(s.held), connackIn(s.acts, b))
	}

	s.event(connection.Event{Kind: connection.Ended, Conn: z, Packet: &packet.Disconnect{}})
	y := new(connection.Conn)
	s.connect(y, &packet.Connect{Version: packet.V5, ClientID: "y"})
	c := new(connection.Conn)
	s.connect(c, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	if ack := connackIn(s.acts, y); ack != nil {
		t.Fatalf("y's CONNACK %+v before the held write is answered; want its session load behind it", ack)
	}

	s.conn = c
	s.release(s.held[0], nil)
	if ack := connackIn(s.acts, b); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
		t.Fatalf("B's CONNACK is %+v; want 0x00, Session Present 1, once its session is stored", ack)
	}
	got := repliesTo(s.acts, b)
	if !slices.ContainsFunc(got, func(r connection.Reply) bool { return isDisconnect(r, 0x8E) }) {
		t.Fatalf("B, taken over, got %v; want DISCONNECT 0x8E", showAll(got))
	}
	s.event(connection.Event{Kind: connection.Ended, Conn: b})
	s.event(connection.Event{Kind: connection.Closed, Conn: b})

	w := new(connection.Conn)
	s.connect(w, &packet.Connect{Version: packet.V5, ClientID: "w"})

	if ack := connackIn(s.acts, c); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
		t.Errorf("C, taking over B, got CONNACK %+v; want 0x00, Session Present 1", ack)
	}
	for _, p := range []struct {
		name string
		conn *connection.Conn
	}{{"y", y}, {"w", w}} {
		if ack := connackIn(s.acts, p.conn); ack == nil || ack.ReasonCode != 0 {
			t.Errorf("%s, an unrelated client whose session is loaded, got CONNACK %+v; want 0x00", p.name, ack)
		}
	}
}

// design.md, Delivery, Preparation: "When the connection ends, deliveries
// prepared and never handed to the writer are cancelled. Their exchanges are
// deleted in the departure's transaction. A live scan's position stays before
// their records, so a continuing session receives them later. They are never
// counted as missed. A retained pass's prepared deliveries are dropped. After
// a crash, a stored preparation is resent as recovered, with DUP 1, because
// it might have been sent". A full disk: "A write request answered "full" is
// discarded. The core does not retain its body for retry." Store, Requests:
// "A takeover does not await the reads of the connection it replaces. Those
// reads are superseded, and their results have no further effects [...] The
// takeover still follows that connection's writes". CONNECT, "in-flight
// messages [...] from the store and resends them [...] Resent messages have
// DUP set". Which answer the store gives first is an ordering no client can
// force (Testing).
//
// The order: client dirty's durable session resumes on B, its position 1,
// nothing in flight; B's live scan selects $broadcast's records, and the
// store holds both the save of their exchanges and the read of their
// payloads, so none is handed to B's writer. C takes over B, keeping the
// session; B is told 0x8E, ends and closes. The held save is answered
// "full"; the takeover's transaction then commits, C resumes, and only
// after that is B's payload read answered. B having handed nothing to its
// writer, every exchange it selected is a never-sent preparation: the
// takeover's transaction stores none of them, and C receives the records
// afresh, as new deliveries, none of them resent with DUP 1.
func TestTakeoverCancelsAPreparationWhoseSaveWasRefused(t *testing.T) {
	s := newPositionScriptOf(t, newConfig(t), 0)
	s.hold = func(g *core.Group) bool { return savesOffset(g, 1) }
	s.holdRead = func(r store.Read) bool { return len(core.PayloadsOf(r)) > 0 }
	b := new(connection.Conn)
	s.conn = b
	s.connect(b, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	s.hold, s.holdRead = nil, nil
	if ack := connackIn(s.acts, b); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("B's CONNACK is %+v; want 0x00", ack)
	}
	if len(s.held) != 1 || len(s.heldReads) != 1 || len(s.deliveries) != 0 {
		t.Fatalf("the store holds %d exchange saves and %d payload reads, B got %d deliveries; want 1, 1 and none",
			len(s.held), len(s.heldReads), len(s.deliveries))
	}
	save, payloads := s.held[0], s.heldReads[0]

	c := new(connection.Conn)
	s.conn = c
	s.connect(c, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	if !slices.ContainsFunc(repliesTo(s.acts, b), func(r connection.Reply) bool { return isDisconnect(r, 0x8E) }) {
		t.Fatalf("B, taken over, got %v; want DISCONNECT 0x8E", showAll(repliesTo(s.acts, b)))
	}
	s.event(connection.Event{Kind: connection.Ended, Conn: b})
	s.event(connection.Event{Kind: connection.Closed, Conn: b})

	s.release(save, store.ErrFull)
	if ack := connackIn(s.acts, c); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
		t.Fatalf("C, taking over B, got CONNACK %+v; want 0x00, Session Present 1, once the held save is answered", ack)
	}
	s.releaseRead(payloads)
	if at := s.core.Timer(); !at.IsZero() {
		s.fireAt(at)
	}

	for _, g := range groupsIn(s.acts) {
		if g == save || !savesSessionOf(g, dirtyClient) {
			continue
		}
		for _, w := range g.Writes {
			if d, ok := w.(*store.DeliveryChanges); ok && len(d.Save) != 0 {
				t.Errorf("the takeover's transaction stores exchanges %+v of B's never-sent preparations; want them cancelled", d.Save)
			}
		}
	}
	// B got no delivery: every one handed to a writer since went to C.
	for _, d := range s.deliveries {
		if d.dup {
			t.Errorf("C got identifier %d resent with DUP 1; want B's never-sent preparation offered afresh", d.id)
		}
	}
	if len(s.deliveries) != dirtyRecords {
		t.Errorf("C got %d deliveries; want the %d records B never sent, offered afresh", len(s.deliveries), dirtyRecords)
	}
}

// design.md, Store, Requests: "A takeover does not await the reads of the
// connection it replaces. Those reads are superseded, and their results have
// no further effects (Store reads). The takeover still follows that
// connection's writes"; "a request waiting for its predecessor holds back
// only its own owner's later requests". Writes: "A write awaiting its owner's
// earlier request or another required predecessor stays pending". CONNECT:
// "3. Order"; "4. Load. Unless the id is connected, the core reads its
// session from the store"; "Once the changes are stored, the broker sends
// CONNACK". Delivery, Slow clients: "A slow client fills only its own
// writer". Which answer comes first, the writer's or the store's, is an
// ordering no client can force (Testing).
//
// Client dirty's durable session resumes on B, whose writer is stuck: the
// three exchanges it resends are handed to it and never reported. B's
// connection ends, and its session's departure waits for that writer. The
// same client connects again on C. While the departure waits for B's
// writer, C's session is not loaded and C gets no CONNACK. The writer then
// reports its deliveries, and the departure's write goes to the store,
// which holds it: C still waits, and still after B's close. Once the
// store answers the departure's write, C's session is loaded and C gets
// its CONNACK, Session Present 1.
func TestReconnectFollowsItsDepartureBehindAStuckWriter(t *testing.T) {
	s := newPositionScript(t)
	s.stuck = true
	b := new(connection.Conn)
	s.conn = b
	s.connect(b, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	if ack := connackIn(s.acts, b); ack == nil || ack.ReasonCode != 0 || len(s.toWriter) != dirtyRecords {
		t.Fatalf("B's CONNACK is %+v, its writer holding %d deliveries; want 0x00 and the %d resent exchanges", ack, len(s.toWriter), dirtyRecords)
	}
	s.event(connection.Event{Kind: connection.Ended, Conn: b})

	since := len(s.acts)
	c := new(connection.Conn)
	s.connect(c, &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535})
	waits := func(when string) {
		t.Helper()
		loaded := slices.ContainsFunc(s.acts[since:], func(a core.Action) bool {
			load, ok := a.Read.(*store.LoadSession)
			return ok && load.Client == dirtyClient
		})
		if ack := connackIn(s.acts, c); loaded || ack != nil {
			t.Fatalf("%s, C's session load is sent: %v, C's CONNACK %+v; want neither before the store answers B's departure", when, loaded, ack)
		}
	}
	waits("while B's departure waits for its stuck writer")

	s.hold = func(g *core.Group) bool { return len(departures([]core.Action{{Write: g}})) > 0 }
	s.stuck = false
	s.serve()
	s.hold = nil
	if len(s.held) != 1 {
		t.Fatalf("once B's writer reports, the store holds %d departures; want B's one", len(s.held))
	}
	waits("while the store holds B's departure")
	s.event(connection.Event{Kind: connection.Closed, Conn: b})
	waits("after B's close, while the store holds its departure")

	s.conn = c
	s.release(s.held[0], nil)
	if ack := connackIn(s.acts, c); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
		t.Errorf("once B's departure is stored, C got CONNACK %+v; want 0x00, Session Present 1", ack)
	}
}

// takeoverCut is design.md, Flows, CONNECT, step 5's 500 ms.
const takeoverCut = 500 * time.Millisecond

// design.md, Flows, CONNECT, step 5: "The same holds for an old connection
// that has already ended: if its departure still waits for its writer's
// delivery reports 500 ms after the CONNECT, its socket is cut. It keeps its
// own closure cause"; "3. Order. While an earlier CONNECT for the same id
// waits for the store, this one waits too"; "Once the changes are stored,
// the broker sends CONNACK". Store, Requests: "A request waiting for its
// predecessor holds back only its own owner's later requests"; "The
// takeover still follows that connection's writes and its writer's
// reports". Where the departure's request sits when the 500 ms are up,
// whether the CONNECT comes before or after the old connection ends, and a
// second CONNECT within them, are orderings no client can force (Testing).
//
// Client dirty's durable session resumes on B, whose writer is stuck: the
// three exchanges it resends are handed to it and never reported. B
// subscribes, and the store holds that subscription change, a request of
// the same client. The same client connects again on C, and, 250 ms later,
// on D; B's connection ends, its departure behind the held change:
//   - before C's CONNECT, or 100 ms after it: the 500 ms after C's CONNECT
//     are up with the departure still behind the change and B's writer
//     still holding its deliveries. The core, driven through every deadline
//     its timer sets, cuts B at exactly that time, not before;
//   - 700 ms after C's CONNECT: B is not cut while connected, and is cut at
//     once when it ends, its departure beginning past those 500 ms.
//
// In each, D's CONNECT does not time the cut, and C has no CONNACK at the
// cut. B's writer then reports, B closes, and the store answers the change
// and what follows it: C gets its CONNACK, Session Present 1.
// TestReconnectFollowsItsDepartureBehindAStuckWriter shows the CONNACK
// waiting for the departure's store write.
func TestReconnectCutsADepartureQueuedBehindItsClientsRequest(t *testing.T) {
	const second = takeoverCut / 2 // D's CONNECT, after C's
	for _, tc := range []struct {
		name string
		ends time.Duration // when B ends, after C's CONNECT; below 0: before it
	}{
		{"B ends before the CONNECT", -1},
		{"B ends 100 ms after the CONNECT", 100 * time.Millisecond},
		{"B ends 700 ms after the CONNECT", 700 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newPositionScript(t)
			s.stuck = true
			b := new(connection.Conn)
			s.conn = b
			reconnect := &packet.Connect{Version: packet.V5, ClientID: dirtyClient, SessionExpiryInterval: 3600, ReceiveMaximum: 65535}
			s.connect(b, reconnect)
			if ack := connackIn(s.acts, b); ack == nil || ack.ReasonCode != 0 || len(s.toWriter) != dirtyRecords {
				t.Fatalf("B's CONNACK is %+v, its writer holding %d deliveries; want 0x00 and the %d resent exchanges", ack, len(s.toWriter), dirtyRecords)
			}
			s.hold = func(g *core.Group) bool { return slices.ContainsFunc(g.Writes, isSubscriptionChange) }
			s.subscribe(b, 1, "more/#")
			s.hold = nil
			if len(s.held) != 1 {
				t.Fatalf("the store holds %d subscription changes of B; want its one", len(s.held))
			}
			change := s.held[0]
			end := func() time.Time { return s.event(connection.Event{Kind: connection.Ended, Conn: b, Reason: 0x8D}) }

			if tc.ends < 0 {
				end()
			}
			c, d := new(connection.Conn), new(connection.Conn)
			at := s.connect(c, reconnect)
			if tc.ends >= 0 && tc.ends < second {
				s.advance(at.Add(tc.ends))
				end()
			}
			s.advance(at.Add(second))
			s.connect(d, reconnect)
			if tc.ends > takeoverCut {
				s.advance(at.Add(tc.ends))
				if s.cut(b) {
					t.Fatalf("B is cut while still connected, before its departure began")
				}
				if ended := end(); !s.cut(b) {
					t.Fatalf("B, its departure beginning at %v, past %v after C's CONNECT, with its writer holding its deliveries, is not cut at once", ended, takeoverCut)
				}
			} else {
				cut := at.Add(takeoverCut)
				s.advance(cut.Add(-time.Nanosecond))
				if s.cut(b) {
					t.Fatalf("B is cut before %v, %v after C's CONNECT", cut, takeoverCut)
				}
				s.advance(cut)
				if !s.cut(b) {
					t.Fatalf("B, its departure behind the held change and its writer holding its deliveries, is not cut at %v, %v after C's CONNECT", cut, takeoverCut)
				}
			}
			if len(s.held) != 1 || connackIn(s.acts, c) != nil {
				t.Fatalf("at the cut, the store holds %d requests and C got CONNACK %+v; want B's change held, no CONNACK", len(s.held), connackIn(s.acts, c))
			}

			s.stuck = false
			s.serve()
			s.event(connection.Event{Kind: connection.Closed, Conn: b})
			s.release(change, nil)
			if ack := connackIn(s.acts, c); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
				t.Errorf("once B's writer reported and the store answered, C got CONNACK %+v; want 0x00, Session Present 1", ack)
			}
		})
	}
}

// isAdmission reports whether r is a publication batch's retained
// admission read.
func isAdmission(r store.Read) bool {
	page, _ := core.ChargesOf(r)
	return len(page) > 0
}

// design.md, Flows, SUBSCRIBE, The start point: "The core captures that
// channel's tail when it accepts the subscription [...]; the captured
// boundary separates its initial pass (the retained or current values
// below it) from subsequent live records"; "An initial pass reads in
// pages; each page sends the values current when it is read, below the
// tail at the cut"; the subscription table: $broadcast starts with "the
// matching retained values, as Retain Handling allows, then the next
// message". Channels, $broadcast, Retained: "A proposed replacement is
// admitted if that sum, with its proposed charge included, fits". Store,
// Store reads: "Earlier accepted writes are resolved before its view is
// fixed". Which answer the store gives first is an ordering no client can
// force (Testing).
//
// With retained.max_bytes set, publisher P sends a retained value on
// kept/x: the core takes it into $broadcast, past the tail, and asks the
// store for the topic's current charge, an answer the store holds.
// Subscriber S, connected and not durable, then subscribes to kept/#, its
// live start captured past P's value. The store answers every other read
// at once; only then does it answer P's admission read, and P's value is
// stored. S receives P's value exactly once: in its initial pass or live,
// never neither, never both.
func TestSubscribeDuringRetainedAdmissionGetsTheValueOnce(t *testing.T) {
	const topic = "kept/x"
	cfg := newConfig(t)
	cfg.Broker.Retained.MaxBytes = 1 << 20
	s := newPositionScriptOf(t, cfg, 0)
	sub := new(connection.Conn)
	s.conn = sub
	s.connect(sub, &packet.Connect{Version: packet.V5, ClientID: "subscriber"})
	pub := new(connection.Conn)
	s.connect(pub, &packet.Connect{Version: packet.V5, ClientID: "publisher"})

	s.holdRead = isAdmission
	s.publish(pub, &packet.Publish{QoS: 1, PacketID: 1, Retain: true, Topic: []byte(topic), Payload: []byte("value")})
	s.holdRead = nil
	if len(s.heldReads) != 1 {
		t.Fatalf("the store holds %d admission reads of P's retained value; want 1", len(s.heldReads))
	}

	s.subscribe(sub, 1, "kept/#")
	s.releaseRead(s.heldReads[0])
	if len(s.records) != 1 {
		t.Fatalf("the store holds %d records past $broadcast's first %d; want P's retained value", len(s.records), dirtyRecords)
	}

	// S is the only subscriber: every delivery is S's.
	got := 0
	for _, d := range s.deliveries {
		if d.topic == topic {
			got++
		}
	}
	if got != 1 {
		t.Errorf("S, subscribing while P's retained value awaited its admission read, received it %d times; want exactly once", got)
	}
}

// design.md, Words, Offset: "assigned by the core when the record's write
// is accepted (Store, Writes), in the channel's order"; Store, Writes: "A
// write awaiting its owner's earlier request or another required
// predecessor stays pending"; "A write is accepted when it joins a group;
// until then it is pending"; "a channel's next publication starts another
// batch, which waits its turn". Execution, Decisions: "Each connection's
// packet order is preserved". Delivery, Order: "a channel's offset order on
// $broadcast". Store reads: "Earlier accepted writes are resolved before its
// view is fixed". Which answer the store gives first is an ordering no client
// can force (Testing).
//
// With publish_commit_interval set and retained.max_bytes set, publisher P
// sends, at QoS 1, a plain message A, which joins the collecting group; a
// retained message B, whose batch waits behind A and then for its
// admission read, which the store holds; and a plain message C. With a
// read barrier, subscriber S2 subscribes between B and C. The store then
// answers B's admission read, and every group commits at its deadline. A,
// B and C are stored at offsets in that order, and subscriber S, there from
// the start, receives them in that order; S2 receives B and C, in order.
func TestPublisherOrderBehindARetainedAdmission(t *testing.T) {
	for _, barrier := range []bool{false, true} {
		t.Run(map[bool]string{false: "no barrier", true: "a SUBSCRIBE between B and C"}[barrier], func(t *testing.T) {
			cfg := newConfig(t)
			cfg.Broker.Retained.MaxBytes = 1 << 20
			cfg.Broker.Storage.PublishCommitInterval = 100 * time.Millisecond
			s := newPositionScriptOf(t, cfg, 0)
			sub, late, pub := new(connection.Conn), new(connection.Conn), new(connection.Conn)
			s.connect(sub, &packet.Connect{Version: packet.V5, ClientID: "subscriber"})
			s.connect(late, &packet.Connect{Version: packet.V5, ClientID: "late"})
			s.connect(pub, &packet.Connect{Version: packet.V5, ClientID: "publisher"})
			s.subscribe(sub, 1, "order/#")

			s.holdRead = isAdmission
			s.publish(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("order/a"), Payload: []byte("A")})
			if len(s.records) != 0 {
				t.Fatalf("A is stored before its group's collection deadline; want it collecting")
			}
			s.publish(pub, &packet.Publish{QoS: 1, PacketID: 2, Retain: true, Topic: []byte("order/b"), Payload: []byte("B")})
			if barrier {
				s.subscribe(late, 1, "order/#")
			}
			s.publish(pub, &packet.Publish{QoS: 1, PacketID: 3, Topic: []byte("order/c"), Payload: []byte("C")})
			admitted := false
			for range 10 {
				if len(s.heldReads) != 0 {
					s.holdRead, admitted = nil, true
					s.releaseRead(s.heldReads[0])
				} else if at := s.core.Timer(); len(s.records) < 3 && !at.IsZero() {
					s.fireAt(at)
				}
			}
			if !admitted || len(s.records) != 3 {
				t.Fatalf("the store answered B's admission read: %v, and holds %d of the 3 records; want B admitted and all stored", admitted, len(s.records))
			}

			offsets := map[string]int64{}
			for o, rec := range s.records {
				offsets[rec.Topic] = o
			}
			if a, b, c := offsets["order/a"], offsets["order/b"], offsets["order/c"]; a >= b || b >= c {
				t.Errorf("A, B and C, published in that order, are stored at offsets %d, %d and %d; want them increasing", a, b, c)
			}
			received := func(conn *connection.Conn) []string {
				var topics []string
				for _, d := range s.deliveries {
					if d.conn == conn {
						topics = append(topics, d.topic)
					}
				}
				return topics
			}
			if got := received(sub); !slices.Equal(got, []string{"order/a", "order/b", "order/c"}) {
				t.Errorf("S received %v; want order/a, order/b and order/c, in their publisher's order", got)
			}
			if got := received(late); barrier && !slices.Equal(got, []string{"order/b", "order/c"}) {
				t.Errorf("S2, subscribing between B and C, received %v; want order/b and order/c, in their publisher's order", got)
			}
		})
	}
}

// keptEvents is an append channel, events, holding events/#, whose
// records retention keeps a day (baseYAML): nothing but the scripts' own
// events moves a position on it.
const keptEvents = `channels:
  events:
    type: append
    filter: "events/#"
`

// positionOf reads the stored position of client's durable session on the
// channel named, and whether the database holds one (storage.md,
// positions).
func (s *liveScript) positionOf(client, channel string) (int64, bool) {
	s.t.Helper()
	var offset int64
	err := s.db.QueryRowContext(context.Background(), `SELECT p."offset" FROM positions p JOIN sessions s ON s.id = p.session
		JOIN channels c ON c.id = p.channel WHERE s.client = ? AND c.name = ?`, client, channel).Scan(&offset)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		s.t.Fatalf("reading the position of %q on %s: %v", client, channel, err)
	}
	return offset, true
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last subscription
// reaching an append channel or a queue is removed, a durable session keeps
// its position there. If a delivery was prepared but never handed to the
// writer, the core first sets the position back to the earliest such
// delivery's offset, so no record is skipped"; "The kept position is saved
// in the UNSUBSCRIBE's own transaction, with the removal of the stored
// preparations"; "Exchanges already sent finish as before. A PUBACK [...]
// ends one"; "Their completion never moves the kept position"; "The next
// subscription reaching the channel reads from the kept position";
// Delivery, Preparation: "When the connection ends, deliveries prepared and
// never handed to the writer are cancelled. Their exchanges are deleted in
// the departure's transaction"; Positions: "That removal marks the
// session's own stored exchanges on the channel moves = 0"; Store, Writes:
// "The core keeps at most one write transaction outstanding at the store";
// CONNECT, 9. Resume. Which events the core takes while a transaction is
// at the store is an ordering no client can force (Testing).
//
// A durable session, Receive Maximum 3, subscribes at QoS 1 to events/#;
// a publisher stores events/1 to events/5: the first three are handed to
// the writer, the last two prepared, the database holding five rows. The
// session unsubscribes, and the store holds the UNSUBSCRIBE's transaction;
// meanwhile the client acknowledges the three deliveries and leaves. Once
// that transaction commits, the database holds a kept position from
// events/1's offset to events/4's, the earliest preparation; once every
// later write has committed, the same kept position, no row in
// session_inflight, and the session away. The broker restarts and the
// session resumes: nothing is resent, and its SUBSCRIBE to events/# reads
// every record from the kept position, in offset order, DUP 0.
func TestAcknowledgementsBehindTheUnsubscribeLeaveTheKeptPosition(t *testing.T) {
	s := newLiveScript(t, keptEvents)
	keeper := s.connect("keeper", 3600, 3)
	s.subscribe(keeper, "events/#")
	pub := s.connect("publisher", 0, 65535)
	for id := uint16(1); id <= 5; id++ {
		s.publish(pub, id, fmt.Sprintf("events/%d", id))
		s.serve()
	}
	const rows = `SELECT count(*) FROM session_inflight WHERE session = (SELECT id FROM sessions WHERE client = 'keeper')`
	var sent []uint16
	for _, r := range s.replies[keeper] {
		if d, ok := r.(*connection.Delivery); ok {
			sent = append(sent, d.PacketID)
		}
	}
	if n := s.count(rows); len(sent) != 3 || n != 5 {
		t.Fatalf("with five records stored, %d deliveries were handed to the writer and session_inflight holds %d rows; want 3 and 5, two prepared", len(sent), n)
	}
	offset := func(topic string) int64 {
		t.Helper()
		return int64(s.count(`SELECT "offset" FROM records WHERE topic = '` + topic + `'`))
	}

	s.hold = func(g *core.Group) bool { return slices.ContainsFunc(g.Writes, isSubscriptionChange) }
	s.step(connection.Event{Kind: connection.Received, Conn: keeper, Packet: &connection.Unsubscribe{PacketID: 2, Filters: []string{"events/#"}}})
	if len(s.held) != 1 {
		t.Fatalf("after the UNSUBSCRIBE, %d groups held; want its transaction, held at the store", len(s.held))
	}
	for _, id := range sent {
		s.event(connection.Event{Kind: connection.Received, Conn: keeper, Packet: &packet.Ack{PacketID: id}})
	}
	s.event(connection.Event{Kind: connection.Ended, Conn: keeper, Packet: &packet.Disconnect{}})
	s.serve()
	held := s.held[0]
	s.hold, s.held = nil, nil
	s.commit(held)
	kept, ok := s.positionOf("keeper", "events")
	if !ok || kept < offset("events/1") || kept > offset("events/4") {
		t.Fatalf("once the UNSUBSCRIBE's transaction committed, the database holds position %d (stored %v) on events; want one kept from %d to %d, events/4 the earliest preparation",
			kept, ok, offset("events/1"), offset("events/4"))
	}
	s.serve()
	s.step(connection.Event{Kind: connection.Closed, Conn: keeper})
	if got, _ := s.positionOf("keeper", "events"); got != kept {
		t.Errorf("once the PUBACKs and the departure committed, the kept position is %d; want %d, where the UNSUBSCRIBE kept it", got, kept)
	}
	if n := s.count(rows); n != 0 {
		t.Errorf("once the PUBACKs and the departure committed, session_inflight holds %d rows; want none: each PUBACK ended its exchange, the preparations removed", n)
	}
	if n := s.count(`SELECT count(*) FROM sessions WHERE client = 'keeper' AND "left" > 0`); n != 1 {
		t.Errorf("once the departure committed, the database holds %d away sessions of keeper; want 1", n)
	}

	s.restart(s.now.Add(eventGap))
	conn, ack := s.connectWith(&packet.Connect{Version: packet.V5, ClientID: "keeper", SessionExpiryInterval: 3600, ReceiveMaximum: 10})
	if !ack.SessionPresent {
		t.Fatal("the resumed session's CONNACK has Session Present 0; want 1")
	}
	if d := slices.IndexFunc(s.replies[conn], func(r connection.Reply) bool { _, ok := r.(*connection.Delivery); return ok }); d >= 0 {
		t.Fatalf("on the resume, the core handed the writer %s; want nothing resent, every exchange ended", show(s.replies[conn][d]))
	}
	s.subscribe(conn, "events/#")
	var got, want []string
	for _, r := range s.replies[conn] {
		if d, ok := r.(*connection.Delivery); ok {
			got = append(got, fmt.Sprintf("%s DUP %v", d.Topic, d.Dup))
		}
	}
	for id := 1; id <= 5; id++ {
		if topic := fmt.Sprintf("events/%d", id); offset(topic) >= kept {
			want = append(want, topic+" DUP false")
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("the SUBSCRIBE after the resume got %q; want %q, every record from the kept position %d, in offset order", got, want, kept)
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "If a delivery was prepared but
// never handed to the writer, the core first sets the position back to the
// earliest such delivery's offset, so no record is skipped"; "The kept
// position is saved in the UNSUBSCRIBE's own transaction, with the removal
// of the stored preparations"; "The next subscription reaching the channel
// reads from the kept position"; Delivery, Positions: "a durable session
// may also pass records represented by stored QoS 1 or QoS 2 exchanges".
//
// A durable session, Receive Maximum 3, subscribes at QoS 1 to events/#;
// a publisher stores events/1 to events/5: the first three are handed to
// the writer, the last two prepared and stored. The session's position
// passes the stored exchanges to events/5's next offset (core.SettleThrough:
// no PUBACK a client sends settles a delivery beyond a preparation never
// handed to the writer). The session unsubscribes, and the store holds the
// UNSUBSCRIBE's transaction. Once it commits, the database holds the kept
// position at events/4's offset exactly, the earliest preparation; the
// SUBSCRIBE to events/# that follows, the three exchanges sent then
// acknowledged, has events/4 handed to the writer first, DUP 0.
func TestUnsubscribeKeepsThePositionAtTheEarliestPreparation(t *testing.T) {
	s := newLiveScript(t, keptEvents)
	keeper := s.connect("keeper", 3600, 3)
	s.subscribe(keeper, "events/#")
	pub := s.connect("publisher", 0, 65535)
	for id := uint16(1); id <= 5; id++ {
		s.publish(pub, id, fmt.Sprintf("events/%d", id))
		s.serve()
	}
	offset := func(topic string) int64 {
		t.Helper()
		return int64(s.count(`SELECT "offset" FROM records WHERE topic = '` + topic + `'`))
	}
	deliveries := func(from int) []*connection.Delivery {
		var ds []*connection.Delivery
		for _, r := range s.replies[keeper][from:] {
			if d, ok := r.(*connection.Delivery); ok {
				ds = append(ds, d)
			}
		}
		return ds
	}
	const rows = `SELECT count(*) FROM session_inflight WHERE session = (SELECT id FROM sessions WHERE client = 'keeper')`
	sent := deliveries(0)
	if n := s.count(rows); len(sent) != 3 || n != 5 {
		t.Fatalf("with five records stored, %d deliveries were handed to the writer and session_inflight holds %d rows; want 3 and 5, two prepared", len(sent), n)
	}
	beyond := offset("events/5") + 1
	if got := s.core.SettleThrough("keeper", s.channelID("events"), beyond, s.now); got != beyond {
		t.Fatalf("passing the stored exchanges through events/5 left the position at %d; want %d, beyond the preparation of events/4 at %d", got, beyond, offset("events/4"))
	}

	s.hold = func(g *core.Group) bool { return slices.ContainsFunc(g.Writes, isSubscriptionChange) }
	s.step(connection.Event{Kind: connection.Received, Conn: keeper, Packet: &connection.Unsubscribe{PacketID: 2, Filters: []string{"events/#"}}})
	if len(s.held) != 1 {
		t.Fatalf("after the UNSUBSCRIBE, %d groups held; want its transaction, held at the store", len(s.held))
	}
	if before, _ := s.positionOf("keeper", "events"); before == offset("events/4") {
		t.Fatalf("before the UNSUBSCRIBE's transaction committed, the database already holds position %d on events; want it saved by that transaction", before)
	}
	held := s.held[0]
	s.hold, s.held = nil, nil
	s.commit(held)
	if kept, ok := s.positionOf("keeper", "events"); !ok || kept != offset("events/4") {
		t.Fatalf("once the UNSUBSCRIBE's transaction committed, the database holds position %d (stored %v) on events; want %d, events/4's offset, the earliest preparation",
			kept, ok, offset("events/4"))
	}
	s.serve()

	from := len(s.replies[keeper])
	s.step(connection.Event{Kind: connection.Received, Conn: keeper, Packet: &connection.Subscribe{PacketID: 3,
		Filters: []connection.Subscription{{Filter: "events/#", QoS: 1}}}})
	for _, d := range sent {
		s.step(connection.Event{Kind: connection.Received, Conn: keeper, Packet: &packet.Ack{PacketID: d.PacketID}})
	}
	if got := deliveries(from); len(got) == 0 || string(got[0].Topic) != "events/4" || got[0].Dup {
		t.Errorf("the SUBSCRIBE after the UNSUBSCRIBE, the three exchanges sent acknowledged, had the writer handed %v; want events/4 first, DUP 0, read from the kept position",
			showAll(s.replies[keeper][from:]))
	}
}
