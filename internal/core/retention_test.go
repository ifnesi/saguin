package core_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of retention passes driven inside (design.md, Words: Retention
// and Missed): the order a retention write takes among pending writes,
// how many records one write deletes, and the times records carry after a
// clock is set back are orderings and inputs no client can force (design.md, Testing: "A test
// inside the code checks only what neither can see. These checks cover
// [...] the busy path, and orderings no client can force"). They drive the
// core's decisions with scripted events and fake times, as core_test.go
// does, but with the real store behind it: each store request the core
// decides is carried out by the store, one at a time, and its answer handed
// back, so the counts retention reports are the store's own.

// storeWait bounds each wait for the real store's answer; a correct store
// answers far sooner. It is the test's watchdog, not a broker deadline.
const storeWait = 30 * time.Second

// liveLimit is the watchdog of a live script's serving: far more store
// requests and writer reports than any of these tests' events lead to.
const liveLimit = 3000

// liveScript plays a broker's connections to one core, as positionScript
// does, with a real store, in full mode unless a test configures another,
// its file in the test's own folder, carrying out its requests: one at a
// time, oldest first, a read's view and result handed back in turn, a
// group's answer once committed. Every delivery, PUBREL and request
// acknowledgement the core hands a writer is reported written; no client
// acknowledges one.
type liveScript struct {
	clock
	cfg *config.Config
	reg *metrics.Registry
	st  *store.Store
	db  *sql.DB // the database, read as an operator's own tools read it

	queue   []any                            // requests decided and not yet carried out, oldest first
	hold    func(*core.Group) bool           // if set, the groups serve leaves to the test, unanswered
	held    []*core.Group                    // those groups, oldest first
	writing []handed                         // entries handed to writers that report them, not yet reported
	stuck   bool                             // while set, serve reports none of writing: the writers wait for the test (report)
	answer  func(store.Answer) []core.Action // optional scheduling of a completed store operation
	refuse  func(*core.Group) bool           // optional rollback-confirmed full response, before any member runs
	startup func()                           // optional driver for startup requests, before Serve
	nudge   func([]core.Action)              // if set, called with every decision take keeps, once kept
	// measured, if set, is called with every read the store carries out,
	// once its last answer has arrived and before the core takes it, with
	// the bytes the process allocated from the read's dispatch to then:
	// the store reader's work on it, and the core's on its view's
	// confirmation. The bytes are the whole process's, so a test using it
	// runs alone in a process of its own (testkit.Rerun).
	measured func(op store.Read, allocated uint64)
	granted  map[*connection.Conn]int64
	replies  map[*connection.Conn][]connection.Reply
	sessions map[string]int64 // session ids by client id, as the core saved them
	groups   []*core.Group    // the groups the store committed, in order

	watch   int64 // the channel whose records commit counts, 0 for none
	deleted []int // for each committed group that deleted records of watch, how many
}

// newLiveScript starts a store on baseYAML with channels, its file in the
// test's own folder, and a core on what it loaded, starting at epoch
// (start).
func newLiveScript(t *testing.T, channels string, configure ...func(*config.Config)) *liveScript {
	t.Helper()
	path := filepath.Join(t.TempDir(), "saguin.db")
	cfg, err := config.Parse([]byte(strings.Replace(baseYAML, "/var/lib/saguin/saguin.db", path, 1) + channels))
	if err != nil {
		t.Fatalf("the test's configuration: %v", err)
	}
	for _, apply := range configure {
		apply(cfg)
	}
	s := &liveScript{t: t, cfg: cfg, now: epoch}
	s.run = s
	t.Cleanup(s.stop)
	s.start()
	return s
}

// start starts the store on the script's configuration, with a registry
// of its own, and a new core on what it loaded, at the script's time: it
// carries out the startup's store requests, then ends the startup
// (Serve).
func (s *liveScript) start() {
	s.t.Helper()
	path := s.cfg.Broker.Storage.FilePath
	s.reg = metrics.New()
	lock, err := store.Lock(s.cfg)
	if err != nil {
		s.t.Fatalf("Lock(%s): %v", path, err)
	}
	st, state, err := store.Start(s.cfg, s.reg, lock)
	if err != nil {
		s.t.Fatalf("Start(%s): %v", path, err)
	}
	s.st = st
	working := path
	if s.cfg.Broker.Storage.Mode == config.Memory {
		working = s.cfg.MemoryPath() // memory mode "runs on the RAM copy" (design.md, Store)
	}
	if s.db, err = sql.Open("sqlite3", "file:"+working+"?mode=ro&_busy_timeout=5000"); err != nil {
		s.t.Fatal(err)
	}
	s.core = core.New(s.cfg, s.reg, state, nil, nil, nil, logger.NewCredits(s.cfg.Broker.LogLevel, s.reg))
	s.granted, s.replies, s.sessions = map[*connection.Conn]int64{}, map[*connection.Conn][]connection.Reply{}, map[string]int64{}
	s.take(s.core.Fire(nil, s.now))
	if s.startup != nil {
		s.startup()
	} else {
		s.serve()
	}
	if !s.core.Serve() {
		s.t.Fatal("with every startup request answered, the core is still starting")
	}
}

// stop closes the script's own view of the database and stops the store,
// as a stopped broker leaves it; the core, every request answered, is
// left behind.
func (s *liveScript) stop() {
	s.t.Helper()
	if s.st == nil {
		return
	}
	_ = s.db.Close() // the test's own reads are over
	if err := s.st.Stop(); err != nil {
		s.t.Errorf("stopping the store: %v", err)
	}
	s.st = nil
}

// restart stops the store and starts it again, with a new core, at at:
// a broker restarted at that time, its connections gone.
func (s *liveScript) restart(at time.Time) {
	s.t.Helper()
	if len(s.queue) != 0 || len(s.held) != 0 {
		s.t.Fatalf("a restart with %d requests queued and %d groups held; want every one answered", len(s.queue), len(s.held))
	}
	s.stop()
	s.now = at
	s.start()
}

// take keeps a decision's actions: its store requests, the bytes it gives
// readers, its replies and deliveries; and fails the test on a store error
// the core reports (R8).
func (s *liveScript) take(acts []core.Action) {
	s.t.Helper()
	for _, a := range acts {
		switch {
		case a.Read != nil:
			s.queue = append(s.queue, a.Read)
		case a.Write != nil:
			s.queue = append(s.queue, a.Write)
		case a.Bytes != 0:
			s.granted[a.Conn] += a.Bytes
		case a.Reply != nil:
			// A delivery is recorded as handed over: the core resets its own
			// descriptor once the delivery is reported written.
			reply := a.Reply
			if d, ok := reply.(*connection.Delivery); ok {
				copied := *d
				reply = &copied
			}
			s.replies[a.Conn] = append(s.replies[a.Conn], reply)
			switch a.Reply.(type) {
			case *connection.Delivery, *connection.Release:
				s.writing = append(s.writing, handed{a, reply})
			}
		}
	}
	select {
	case err := <-s.core.Failures():
		s.t.Fatalf("the core stopped on a store error: %v", err)
	default:
	}
	if s.nudge != nil {
		s.nudge(acts)
	}
}

// handed is an entry handed to a writer that reports it: the action as the
// core decided it, whose Reply is the core's own descriptor, the one the
// writer's report carries back; and recorded, the entry as s.replies
// records it, by which a driver that writes from s.replies finds it.
type handed struct {
	core.Action
	recorded connection.Reply
}

// report hands the core the writer's report of s.writing[i], carried out
// with result, and removes it from s.writing.
func (s *liveScript) report(i int, result connection.DeliveryResult) {
	s.t.Helper()
	a := s.writing[i]
	s.writing = slices.Delete(s.writing, i, i+1)
	s.event(writerReport(a.Conn, a.Reply, result))
}

// writerReport is conn's writer's report of reply, an entry the core handed
// it, carried out with result: a delivery or a PUBREL comes back on
// Delivered. A request's acknowledgement is an ordinary reply, its slot
// returned once it is written (design.md, Flows, Replies: "The slot is
// returned once the reply is written"; $saguin topics: "The request's
// reply slot follows the ordinary rule in Replies"), so its write is no
// report to the core.
func writerReport(conn *connection.Conn, reply connection.Reply, result connection.DeliveryResult) connection.Event {
	switch r := reply.(type) {
	case *connection.Delivery:
		r.Result = result
	case *connection.Release:
		r.Result = result
	}
	return connection.Event{Kind: connection.Delivered, Conn: conn, Packet: reply}
}

// event hands the core ev, eventGap after the last event, and keeps its
// decision, carrying out nothing.
func (s *liveScript) event(ev connection.Event) {
	s.t.Helper()
	s.now = s.now.Add(eventGap)
	s.take(s.core.Handle(nil, ev, s.now))
}

// step hands the core ev, then serves what it leads to.
func (s *liveScript) step(ev connection.Event) {
	s.t.Helper()
	s.event(ev)
	s.serve()
}

// serve carries out every request decided, oldest first, but the groups
// hold leaves to the test, and reports every delivery handed to a writer
// as written, unless the writers are stuck, until the core asks for
// nothing more.
func (s *liveScript) serve() {
	s.t.Helper()
	for answers := 1; len(s.queue) > 0 || (len(s.writing) > 0 && !s.stuck); answers++ {
		if answers > liveLimit {
			s.t.Fatalf("watchdog: the core still asks after %d answers", liveLimit)
		}
		if len(s.queue) == 0 {
			s.report(0, connection.DeliveryWritten)
			continue
		}
		op := s.queue[0]
		s.queue = s.queue[1:]
		switch op := op.(type) {
		case store.Read:
			s.read(op)
		case *core.Group:
			if s.hold != nil && s.hold(op) {
				s.held = append(s.held, op)
				continue
			}
			s.commit(op)
		case store.Write:
			s.carry(op)
		default:
			s.t.Fatalf("the core sent %T, neither a read nor a write", op)
		}
	}
}

// carry has the store's writer carry out w, a write the core sends outside
// a group, and hands the core its answer: normal mode's sync or memory
// mode's snapshot fold, each of which "takes its place in the store-request
// order like a write" (design.md, Store), on the writer's channel, and is
// answered as a write is.
func (s *liveScript) carry(w store.Write) {
	s.t.Helper()
	testkit.Send(s.t, s.st.Writes(), w, storeWait)
	a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a write's answer")
	if a.Op != w {
		s.t.Fatalf("the store answered %T %p; want %T %p, the one write outstanding", a.Op, a.Op, w, w)
	}
	s.now = s.now.Add(eventGap)
	s.take(s.storeAnswer(a))
}

// read has a store reader carry out op, handing the core its answers in
// turn: a read with a place in the store-request order has its view's
// confirmation first, then its result; a read of channel records, its
// result alone (design.md, Store reads: it "has no place in the
// store-request order [...] and is answered once").
func (s *liveScript) read(op store.Read) {
	s.t.Helper()
	var before runtime.MemStats
	if s.measured != nil {
		runtime.ReadMemStats(&before)
	}
	testkit.Send(s.t, s.st.Reads(), op, storeWait)
	for {
		a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a read's answer")
		if a.Op != op {
			s.t.Fatalf("the store answered %T %p; want %T %p, the one read outstanding", a.Op, a.Op, op, op)
		}
		if s.measured != nil && !a.View {
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			s.measured(op, after.TotalAlloc-before.TotalAlloc)
		}
		s.now = s.now.Add(eventGap)
		s.take(s.storeAnswer(a))
		if !a.View {
			return
		}
	}
}

// commit has the store's writer carry out g and hands the core its answer,
// noting the sessions it saved and, where watch is set, the records of
// watch the transaction deleted. It returns the core's decision on the
// answer, kept as take keeps it.
func (s *liveScript) commit(g *core.Group) []core.Action {
	s.t.Helper()
	if s.refuse != nil && s.refuse(g) {
		s.now = s.now.Add(eventGap)
		acts := s.storeAnswer(store.Answer{Op: g, Err: store.ErrFull})
		s.take(acts)
		return acts
	}
	before := s.recordsOn(s.watch)
	testkit.Send(s.t, s.st.Writes(), store.Write(g), storeWait)
	a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a group's answer")
	if a.Op != g {
		s.t.Fatalf("the store answered %T %p; want group %p", a.Op, a.Op, g)
	}
	if a.Err == nil {
		s.groups = append(s.groups, core.CopyGroup(g))
		for _, w := range g.Writes {
			if save, ok := w.(*store.SaveSession); ok {
				s.sessions[save.Session.Client] = save.Session.ID
			}
		}
		if n := before + recordsIn(g, s.watch) - s.recordsOn(s.watch); s.watch != 0 && n > 0 {
			s.deleted = append(s.deleted, n)
		}
	}
	s.now = s.now.Add(eventGap)
	acts := s.storeAnswer(a)
	s.take(acts)
	return acts
}

// storeAnswer applies an answer immediately unless a driver models a worker
// paused before sending it. That driver must preserve each worker's order.
func (s *liveScript) storeAnswer(a store.Answer) []core.Action {
	if s.answer != nil {
		return s.answer(a)
	}
	return s.core.Answer(nil, a, s.now)
}

// release has the store carry out held, a group serve left to the test,
// then serves what that leads to.
func (s *liveScript) release(held *core.Group) {
	s.t.Helper()
	i := slices.Index(s.held, held)
	if i < 0 {
		s.t.Fatalf("an answer to group %p, which is not held", held)
	}
	s.held = slices.Delete(s.held, i, i+1)
	s.commit(held)
	s.serve()
}

// connect accepts a connection and hands the core its MQTT 5 CONNECT for
// client, Clean Start 1, with session expiry and Receive Maximum,
// failing the test unless its CONNACK is 0x00.
func (s *liveScript) connect(client string, expiry uint32, receiveMaximum uint16) *connection.Conn {
	s.t.Helper()
	conn, _ := s.connectWith(&packet.Connect{Version: packet.V5, CleanStart: true,
		ClientID: client, SessionExpiryInterval: expiry, ReceiveMaximum: receiveMaximum})
	return conn
}

// connectWith accepts a connection and hands the core p, its CONNECT,
// serving what each leads to, failing the test unless its CONNACK is 0x00,
// which it returns with the connection.
func (s *liveScript) connectWith(p *packet.Connect) (*connection.Conn, *packet.Connack) {
	s.t.Helper()
	conn := new(connection.Conn)
	n := len(s.replies)
	s.step(connection.Event{Kind: connection.Accepted, Conn: conn,
		Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 1, byte(n)), Port: 40000 + n}}})
	s.step(connection.Event{Kind: connection.Received, Conn: conn, Packet: p})
	ack := connackIn(s.decisions(conn), conn)
	if ack == nil || ack.ReasonCode != 0 {
		s.t.Fatalf("CONNECT for %q answered with %v; want CONNACK 0x00", p.ClientID, showAll(s.replies[conn]))
	}
	return conn, ack
}

// decisions returns the replies conn has had, as actions.
func (s *liveScript) decisions(conn *connection.Conn) []core.Action {
	acts := make([]core.Action, 0, len(s.replies[conn]))
	for _, r := range s.replies[conn] {
		acts = append(acts, core.Action{Conn: conn, Reply: r})
	}
	return acts
}

// subscribe hands the core conn's first SUBSCRIBE, packet identifier 1,
// of filters at QoS 1, and serves what it leads to, failing the test
// unless the SUBACK grants each.
func (s *liveScript) subscribe(conn *connection.Conn, filters ...string) {
	const id = 1
	s.t.Helper()
	subs := make([]connection.Subscription, len(filters))
	for i, f := range filters {
		subs[i] = connection.Subscription{Filter: f, QoS: 1}
	}
	s.step(connection.Event{Kind: connection.Received, Conn: conn, Packet: &connection.Subscribe{PacketID: id, Filters: subs}})
	for _, r := range s.replies[conn] {
		if ack, ok := r.(*packet.Suback); ok && ack.PacketID == id {
			for i, code := range ack.ReasonCodes {
				if code != 1 {
					s.t.Fatalf("SUBACK %d grants %s with 0x%02X; want 0x01", id, filters[i], code)
				}
			}
			return
		}
	}
	s.t.Fatalf("SUBSCRIBE %d of %q got no SUBACK: %v", id, filters, showAll(s.replies[conn]))
}

// publish has conn's reader ask for the body of a QoS 1 PUBLISH of id on
// topic, testkit.RecordBytes stored (its PUBLISH carries no property), and
// hand it over, serving the store first where the core withholds the body;
// it serves nothing after.
func (s *liveScript) publish(conn *connection.Conn, id uint16, topic string) {
	s.t.Helper()
	s.send(conn, &packet.Publish{QoS: 1, PacketID: id, Topic: []byte(topic), Payload: testkit.Filling(topic, testkit.RecordBytes)})
}

// send has conn's reader ask for p's body and hand p over, as publish
// does.
func (s *liveScript) send(conn *connection.Conn, p *packet.Publish) {
	s.t.Helper()
	s.hand(conn, bodyOf(p), p)
}

// hand has conn's reader ask for a body of n bytes and hand p, a PUBLISH
// or a request, over with it, as send does.
func (s *liveScript) hand(conn *connection.Conn, n int64, p any) {
	s.t.Helper()
	s.event(connection.Event{Kind: connection.BytesRequested, Conn: conn, Bytes: n})
	if s.granted[conn] < n {
		s.serve()
	}
	if s.granted[conn] < n {
		s.t.Fatalf("a reader asked for a PUBLISH body of %d bytes and, the store idle, was given %d", n, s.granted[conn])
	}
	s.granted[conn] -= n
	s.event(connection.Event{Kind: connection.Received, Conn: conn, Packet: p, Bytes: n})
}

// leave hands the core conn's DISCONNECT, Normal disconnection, and its
// close report, serving what each leads to: a durable session goes away.
func (s *liveScript) leave(conn *connection.Conn) {
	s.t.Helper()
	s.step(connection.Event{Kind: connection.Ended, Conn: conn, Packet: &packet.Disconnect{}})
	s.step(connection.Event{Kind: connection.Closed, Conn: conn})
}

// channelID returns the id of the channel named, as the database holds it.
func (s *liveScript) channelID(name string) int64 {
	s.t.Helper()
	var id int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT id FROM channels WHERE name = ?`, name).Scan(&id); err != nil {
		s.t.Fatalf("the channel %s: %v", name, err)
	}
	return id
}

// recordsOn counts the records the database holds on channel id, 0 for none.
func (s *liveScript) recordsOn(channel int64) int {
	s.t.Helper()
	if channel == 0 {
		return 0
	}
	var n int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM records WHERE channel = ?`, channel).Scan(&n); err != nil {
		s.t.Fatalf("counting the records of channel %d: %v", channel, err)
	}
	return n
}

// topics returns the topics the database holds on channel id, in offset
// order.
func (s *liveScript) topics(channel int64) []string {
	s.t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT topic FROM records WHERE channel = ? ORDER BY "offset"`, channel)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var topics []string
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			s.t.Fatal(err)
		}
		topics = append(topics, topic)
	}
	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}
	return topics
}

// missed is saguin_missed_total of channel, cause retention (metrics.md).
func (s *liveScript) missed(channel string) float64 {
	s.t.Helper()
	return testkit.Sample(s.t, s.reg, testkit.MissedSeries(channel))
}

// eventsChannel is an append channel, events, holding events/#, keeping
// 150 bytes: one record and a half.
const eventsChannel = `channels:
  events:
    type: append
    filter: "events/#"
    retention_bytes: 150
`

// design.md, Words: Missed, "Retention determines losses before removing
// the information needed to count them. It uses the deletion's ordered
// boundary. In the retention write, the core names its connected consumers
// [...] with their filters"; "A loss counts records owed under the
// consumer's filtering"; Store, Writes: "Eligible writes join in their
// store-request order"; "The writer executes exactly the sealed group as
// one transaction"; Flows, SUBSCRIBE, The start point: "Adding a filter
// does not move an existing consumer position [...] A filter added later
// therefore receives what the position has not yet passed"; UNSUBSCRIBE:
// "The UNSUBACK is sent once the session is stored without the filter".
// metrics.md, saguin_missed_total.
//
// A durable session, connected, has a consumer on events, at its floor,
// through a subscription the change leaves. A publisher's events/b/1 and
// events/b/2 are committed in one group, which the store holds; meanwhile
// the client changes its subscription to events/b/#, a change pending
// behind that group. The group's commit, 200 bytes, brings a size pass
// while the change is still pending: the change comes first in the
// store-request order, so its retention write follows the change, behind
// it in one group or in a later one, and passes events/b/1. Added first,
// events/b/# makes events/b/1 owed: one record missed. Removed first,
// events/b/# leaves it not owed: none.
func TestRetentionCountsAfterAnEarlierSubscriptionChange(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hold  []string                                // the session's filters before the change
		event func(*connection.Conn) connection.Event // the change
		want  float64
	}{
		{"SUBSCRIBE", []string{"events/a/#"}, func(conn *connection.Conn) connection.Event {
			return connection.Event{Kind: connection.Received, Conn: conn, Packet: &connection.Subscribe{PacketID: 2,
				Filters: []connection.Subscription{{Filter: "events/b/#", QoS: 1}}}}
		}, 1},
		{"UNSUBSCRIBE", []string{"events/a/#", "events/b/#"}, func(conn *connection.Conn) connection.Event {
			return connection.Event{Kind: connection.Received, Conn: conn, Packet: &connection.Unsubscribe{PacketID: 2, Filters: []string{"events/b/#"}}}
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLiveScript(t, eventsChannel)
			events := s.channelID("events")
			owed := s.connect("owed", 3600, 65535)
			s.subscribe(owed, tc.hold...)
			pub := s.connect("publisher", 0, 65535)

			// A publication on $broadcast holds the store, so the two that
			// follow on events form one batch, and one group of their own.
			s.hold = func(*core.Group) bool { return true }
			s.publish(pub, 1, "other/x")
			s.serve()
			s.publish(pub, 2, "events/b/1")
			s.publish(pub, 3, "events/b/2")
			if len(s.held) != 1 || len(s.queue) != 0 {
				t.Fatalf("with one group held, %d groups held and %d requests queued; want 1 and none", len(s.held), len(s.queue))
			}
			s.release(s.held[0])
			if len(s.held) != 1 || recordsIn(s.held[0], events) != 2 {
				t.Fatalf("after the first group's commit, %d groups held; want 1, events/b/1 and events/b/2 together", len(s.held))
			}
			s.event(tc.event(owed))
			s.serve()
			if len(s.held) != 1 || len(s.queue) != 0 {
				t.Fatalf("with the publications' group held, %d groups held and %d requests queued; want the change pending", len(s.held), len(s.queue))
			}
			s.hold = nil
			s.release(s.held[0])

			change, retention := s.committedAt(isSubscriptionChange), s.committedAt(func(w store.Write) bool {
				op, ok := w.(*store.Retention)
				return ok && op.Channel == events && op.Removed != 0
			})
			if change < 0 || retention < change {
				t.Fatalf("the change committed as write %d and events' deletion as write %d; want the change first, as it was stored first", change, retention)
			}
			if got := s.topics(events); !slices.Equal(got, []string{"events/b/2"}) {
				t.Fatalf("after the size pass, events holds %q; want events/b/2 alone", got)
			}
			if got := s.missed("events"); got != tc.want {
				t.Errorf("with the %s stored before retention passed events/b/1, saguin_missed_total for events = %v; want %v", tc.name, got, tc.want)
			}
		})
	}
}

// committedAt returns the place, among every write the store committed,
// group after group and member after member, of the first for which is
// reports true, -1 for none.
func (s *liveScript) committedAt(is func(store.Write) bool) int {
	at := 0
	for _, g := range s.groups {
		for _, w := range g.Writes {
			if is(w) {
				return at
			}
			at++
		}
	}
	return -1
}

// isSubscriptionChange reports whether w stores a session's subscription
// change: a SUBSCRIBE's or an UNSUBSCRIBE's.
func isSubscriptionChange(w store.Write) bool {
	_, saved := w.(*store.SaveSubscriptions)
	_, removed := w.(*store.RemoveSubscriptions)
	return saved || removed
}

// recordsIn counts the records g stores on channel.
func recordsIn(g *core.Group, channel int64) int {
	n := 0
	for _, w := range g.Writes {
		records := core.RecordsOf(w)
		for i := range records {
			if records[i].Channel == channel {
				n++
			}
		}
	}
	return n
}

// design.md, Words: Missed, "In the retention write, the core names its
// connected consumers and bridge rules"; Words: Consumer, "a session's
// subscription to a channel"; Testing: "The busy path is anything run per
// packet, per message or per connection. It does no needless work"; Store,
// Requests: "Each session, shared group and channel keeps at most one
// request of its own in flight [...] Unrelated owners continue".
//
// A broker holds two append channels, events (150 bytes) and other. A
// durable session, connected, has a consumer on events; another, connected
// with Receive Maximum 1, has one on other alone, with a delivery in
// flight and more records waiting. A publisher's records on events pass
// its 150 bytes, bringing size passes. Each retention write on events
// names the first session alone; the second, with no consumer on events,
// is neither named nor given anything by the decision on that write's
// answer.
func TestRetentionNamesOnlyTheChannelsConsumers(t *testing.T) {
	s := newLiveScript(t, eventsChannel+"  other:\n    type: append\n    filter: \"other/#\"\n")
	named := s.connect("named", 3600, 65535)
	s.subscribe(named, "events/a/#")
	elsewhere := s.connect("elsewhere", 3600, 1)
	s.subscribe(elsewhere, "other/#")
	pub := s.connect("publisher", 0, 65535)
	for i, topic := range []string{"other/1", "other/2", "other/3"} {
		s.publish(pub, uint16(i+1), topic)
		s.serve()
	}
	if got := len(deliveriesTo(s.replies[elsewhere])); got != 1 {
		t.Fatalf("the session on other alone, Receive Maximum 1, has %d deliveries; want 1, in flight", got)
	}

	// The decision on each retention write's answer is the one serve takes
	// next; a hold on that write's group gives it to the test.
	s.hold = func(g *core.Group) bool {
		return slices.ContainsFunc(g.Writes, func(w store.Write) bool { _, ok := w.(*store.Retention); return ok })
	}
	for i, topic := range []string{"events/b/1", "events/b/2", "events/b/3"} {
		s.publish(pub, uint16(i+4), topic)
		s.serve()
	}
	events, passes := s.channelID("events"), 0
	for len(s.held) > 0 {
		g := s.held[0]
		for _, w := range g.Writes {
			op, ok := w.(*store.Retention)
			if !ok || op.Channel != events {
				continue
			}
			passes++
			var sessions []int64
			for _, u := range op.Consumers {
				sessions = append(sessions, u.Session)
			}
			if _, ok := op.Named[s.sessions["elsewhere"]]; ok || !slices.Equal(sessions, []int64{s.sessions["named"]}) {
				t.Errorf("events' retention write names sessions %v (named %v); want %d alone, the one session with a consumer on events (%d has one on other alone)",
					sessions, op.Named, s.sessions["named"], s.sessions["elsewhere"])
			}
		}
		s.held = s.held[1:]
		for _, a := range s.commit(g) {
			if a.Conn == elsewhere || (a.Write != nil && slices.ContainsFunc(a.Write.(*core.Group).Writes, func(w store.Write) bool {
				d, ok := w.(*store.DeliveryChanges)
				return ok && d.Session == s.sessions["elsewhere"]
			})) {
				t.Errorf("the decision on a retention write's answer acts for the session on other alone: %+v", a)
			}
		}
		s.serve()
	}
	if passes == 0 {
		t.Fatal("300 bytes on events, which keeps 150, brought no retention write on events")
	}
}

// deliveriesTo returns the deliveries among replies.
func deliveriesTo(replies []connection.Reply) []*connection.Delivery {
	var ds []*connection.Delivery
	for _, r := range replies {
		if d, ok := r.(*connection.Delivery); ok {
			ds = append(ds, d)
		}
	}
	return ds
}

// agedChannel is an append channel, aged, holding aged/#, keeping records
// for 10 seconds.
const agedChannel = `channels:
  aged:
    type: append
    filter: "aged/#"
    retention_period: 10s
`

// agedPeriod is agedChannel's retention_period.
const agedPeriod = 10 * time.Second

// design.md, Words: Retention, "A channel's records are deleted oldest
// first, in offset order from the floor. A pass [...] stops at the first
// record not yet due, whose time sets the channel's next age deadline. A
// record carrying an earlier time than one before it, as after the clock
// is set back, waits behind it: retention deletes late, never early";
// "Startup processes overdue retention before serving"; "The core keeps
// one next age-retention deadline"; PUBLISH: "The record's timestamp is
// the core's intake time"; config.md, retention_period, "append: removes
// old records". Audit finding 261028-009a, the maintainer's decision: a
// record's stored time is in whole milliseconds, the true time up to 1 ms
// later, so its deadline is the stored time plus the period plus 1 ms,
// at most 1 ms late and never early.
//
// The channel aged keeps records for 10 seconds. A publisher stores
// aged/young; the clock is then set back five seconds, and it stores
// aged/old, which carries the earlier time at the later offset. The broker
// restarts once aged/old is past its retention_period and aged/young is
// not: startup's overdue retention deletes nothing, aged/young not yet due
// and aged/old waiting behind it. The core's next deadline is aged/young's
// stored time plus the period plus 1 ms; driven through every deadline
// before it, its stored time plus the period included, the core still
// deletes nothing, and at it both go.
func TestRetentionWaitsBehindAYoungerRecord(t *testing.T) {
	s := newLiveScript(t, agedChannel)
	aged := s.channelID("aged")
	pub := s.connect("publisher", 0, 65535)
	s.now = epoch.Add(time.Minute)
	s.publish(pub, 1, "aged/young")
	s.serve()
	s.now = s.now.Add(-5 * time.Second)
	s.publish(pub, 2, "aged/old")
	s.serve()
	var young, old int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT
		(SELECT time FROM records WHERE channel = ? AND topic = 'aged/young'),
		(SELECT time FROM records WHERE channel = ? AND topic = 'aged/old')`, aged, aged).Scan(&young, &old); err != nil {
		t.Fatalf("the records' times: %v", err)
	}
	if old >= young {
		t.Fatalf("aged/old's time is %d, aged/young's %d; want the earlier at the later offset", old, young)
	}
	due := time.UnixMilli(young).Add(agedPeriod + time.Millisecond)

	s.restart(time.UnixMilli(old).Add(agedPeriod + time.Second))
	if got := s.topics(aged); !slices.Equal(got, []string{"aged/young", "aged/old"}) {
		t.Fatalf("after startup's overdue retention, aged/old past its retention_period and aged/young not, aged holds %q; want both, aged/old waiting behind aged/young", got)
	}
	if got := s.core.Timer(); !got.Equal(due) {
		t.Fatalf("with aged/young the first record not yet due, the core's timer is %v; want %v, its stored time plus the period plus 1 ms", got, due)
	}
	s.advance(due.Add(-time.Millisecond))
	if got := s.topics(aged); !slices.Equal(got, []string{"aged/young", "aged/old"}) {
		t.Fatalf("before aged/young's deadline %v, aged holds %q; want both", due, got)
	}
	s.fireAt(due)
	if got := s.topics(aged); len(got) != 0 {
		t.Errorf("at aged/young's deadline, aged holds %q; want nothing", got)
	}
}

// backlog is how many records the backlog test's channel holds once due:
// two whole passes and part of a third.
const backlog = 2*512 + 100

// design.md, Words: Retention, "A pass reads the times of up to 512 records
// in one ordered read, deletes the run of them past retention in one range,
// and stops at the first record not yet due"; Words: Missed, "A retention
// write deletes at most 512 records"; "The successful deletion answer
// carries the sum of consumer-record losses per channel [...] The core
// applies these counts once"; "Connected consumers use their progress held
// by the core [...] Away sessions use their stored state. Deliveries
// already settled or handed to a writer are excluded". metrics.md,
// saguin_missed_total: "one record owed to two consumers contributes two".
//
// The channel aged keeps records for 10 seconds. A durable session
// subscribes at QoS 1 to aged/# and goes away; another, connected with
// Receive Maximum 1, subscribes too, and its first delivery stays
// unacknowledged. A publisher stores 1,124 records. The core's timer then
// comes once, every record past its retention_period: without another
// timer, the store deletes them all, in writes of 512, 512 and 100
// records. Each record was owed to the away session, and every one but
// the first, handed to its writer, to the connected one: 2,247 missed.
func TestRetentionClearsABacklogInPassesOf512(t *testing.T) {
	s := newLiveScript(t, agedChannel)
	gone := s.connect("gone", 3600, 65535)
	s.subscribe(gone, "aged/#")
	s.leave(gone)
	slow := s.connect("slow", 3600, 1)
	s.subscribe(slow, "aged/#")
	pub := s.connect("publisher", 0, 65535)
	for i := range backlog {
		s.publish(pub, uint16(i+1), fmt.Sprintf("aged/%d", i))
	}
	s.serve()
	s.watch = s.channelID("aged")
	if got := s.recordsOn(s.watch); got != backlog {
		t.Fatalf("aged holds %d records; want the %d published", got, backlog)
	}
	if got := len(deliveriesTo(s.replies[slow])); got != 1 {
		t.Fatalf("the connected session, Receive Maximum 1, has %d deliveries; want 1, unacknowledged", got)
	}

	s.fireAt(s.now.Add(agedPeriod))
	if want := []int{512, 512, backlog - 1024}; !slices.Equal(s.deleted, want) {
		t.Errorf("after one timer, every record due, the retention writes deleted %v records; want %v, up to 512 each, back to back", s.deleted, want)
	}
	if got := s.recordsOn(s.watch); got != 0 {
		t.Fatalf("after one timer, every record due, aged holds %d records; want none, its passes back to back", got)
	}
	if got, want := s.missed("aged"), float64(2*backlog-1); got != want {
		t.Errorf("saguin_missed_total for aged = %v; want %v, each record owed to the away session and all but the first to the connected one", got, want)
	}
}

// idleChannel is an append channel, idle, holding idle/#, keeping records
// for an hour.
const idleChannel = `  idle:
    type: append
    filter: "idle/#"
    retention_period: 1h
`

// design.md, Words: Retention, "Age retention becomes due at the earliest
// applicable record, retained-value or tombstone deadline. The core keeps
// one next age-retention deadline"; "Due work follows R2's timer order,
// with channels ordered by id"; "Every committed change that affects size
// schedules a size pass for its channel"; "A pass reads the times of up to
// 512 records in one ordered read"; Store, Writes: "Empty groups are not
// sent"; Testing: "The busy path [...] does no needless work". Audit
// finding 261028-009a, the maintainer's decision: a record's deadline is
// its stored time, in whole milliseconds, plus the period plus 1 ms, at
// most 1 ms late and never early.
//
// Two append channels: aged keeps records for 10 seconds, idle for an
// hour. A publisher stores idle/1, then aged/1. At aged/1's stored time
// plus the period, it is still held; when its deadline comes, 1 ms later,
// the core's timer deletes it. idle, with nothing due by age and no
// size pass scheduled, gets no retention write, the write that reads a
// channel's records; nor does $broadcast, empty: every retention write
// from the deadline on is aged's.
func TestAgeDeadlineLeavesIdleChannelsAlone(t *testing.T) {
	s := newLiveScript(t, agedChannel+idleChannel)
	aged, idle := s.channelID("aged"), s.channelID("idle")
	pub := s.connect("publisher", 0, 65535)
	s.publish(pub, 1, "idle/1")
	s.publish(pub, 2, "aged/1")
	s.serve()
	var at int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT time FROM records WHERE channel = ?`, aged).Scan(&at); err != nil {
		t.Fatalf("aged/1's time: %v", err)
	}
	due := time.UnixMilli(at).Add(agedPeriod + time.Millisecond)

	s.advance(due.Add(-time.Millisecond))
	if got := s.topics(aged); !slices.Equal(got, []string{"aged/1"}) {
		t.Fatalf("at aged/1's stored time plus the period, 1 ms before its deadline %v, aged holds %q; want aged/1", due, got)
	}
	from := len(s.groups)
	s.advance(due)
	if got := s.topics(aged); len(got) != 0 {
		t.Fatalf("at aged/1's deadline, aged holds %q; want nothing", got)
	}
	if got := s.topics(idle); !slices.Equal(got, []string{"idle/1"}) {
		t.Fatalf("at aged/1's deadline, idle holds %q; want idle/1, an hour from its deadline", got)
	}
	for _, g := range s.groups[from:] {
		for _, w := range g.Writes {
			if op, ok := w.(*store.Retention); ok && op.Channel != aged {
				t.Errorf("at aged/1's deadline, a retention write for channel %d, with nothing due by age and no size pass; want aged's (%d) alone",
					op.Channel, aged)
			}
		}
	}
}

// broadcastKeeps gives $broadcast a retention_bytes of two and a half
// records (config.md, broker.broadcast), its retention_period the
// storage default, a day.
const broadcastKeeps = `  broadcast:
    retention_bytes: 250
`

// retain has conn's reader hand the core a retained QoS 1 PUBLISH of id on
// topic, carrying payload, as publish does.
func (s *liveScript) retain(conn *connection.Conn, id uint16, topic string, payload []byte) {
	s.t.Helper()
	s.send(conn, &packet.Publish{QoS: 1, Retain: true, PacketID: id, Topic: []byte(topic), Payload: payload})
}

// passValues has pub store values retained values of testkit.RecordBytes
// on $broadcast, kept/1 up, then three plain records, and serves what that
// leads to: on broadcastKeeps, the size pass the third plain record brings
// passes every value, keeping it, and deletes the first plain record
// (design.md, Channels, $broadcast: "Retention counts only unflagged
// records toward its size"). It fails the test unless the floor is then
// above every value, and returns the floor.
func (s *liveScript) passValues(pub *connection.Conn, values int) int64 {
	s.t.Helper()
	for i := range values {
		topic := fmt.Sprintf("kept/%d", i+1)
		s.retain(pub, uint16(i+1), topic, testkit.Filling(topic, testkit.RecordBytes))
	}
	for i := range 3 {
		s.publish(pub, uint16(values+i+1), fmt.Sprintf("plain/%d", i+1))
	}
	s.serve()
	var floor int64
	var below int
	if err := s.db.QueryRowContext(context.Background(), `SELECT floor,
		(SELECT count(*) FROM records r WHERE r.channel = 1 AND r.retained = 1 AND r."offset" < c.floor)
		FROM channels c WHERE c.id = 1`).Scan(&floor, &below); err != nil {
		s.t.Fatalf("$broadcast's floor: %v", err)
	}
	if below != values {
		s.t.Fatalf("$broadcast holds %d retained values below its floor %d; want the %d stored, passed by the size pass", below, floor, values)
	}
	return floor
}

// design.md, Execution, Resource bounds: "Startup obtains counts and next
// deadlines without loading every retained value or unfinished job";
// Restart, Start: "The core loads channels, bridges, and the counts and
// next deadlines without loading every retained value or unfinished job.
// The counts and deadlines are defined in Execution, Resource bounds";
// Words: Retention, "Startup processes overdue retention
// before serving"; "The floor is the oldest record retention has not
// passed"; "A channel's records are deleted oldest first, in offset order
// from the floor"; "A retained value kept below the floor is sent only as
// a retained value": it is current, not overdue. Testing: a test inside
// the code checks "the busy path".
//
// $broadcast keeps two and a half records. A publisher stores 600
// retained values, more than a retention write's page of 512, then three
// plain records, and the size pass passes the values, keeping them below
// the floor; the broker restarts a second later. Every retention write the
// start commits on $broadcast reads its records from the floor: the offset
// its record read starts after, as the store ran it, is at least the floor
// less one, so no value below the floor is read.
func TestStartupWorkIgnoresRetainedValuesBelowTheFloor(t *testing.T) {
	s := newLiveScript(t, broadcastKeeps)
	floor := s.passValues(s.connect("publisher", 0, 65535), 600)
	from := len(s.groups)
	s.restart(s.now.Add(time.Second))
	for _, g := range s.groups[from:] {
		for _, w := range g.Writes {
			if op, ok := w.(*store.Retention); ok && op.Channel == 1 && op.After < floor-1 {
				t.Errorf("a retention write of the start read $broadcast's records after offset %d, with 600 retained values below its floor %d; want after %d at least, from the floor",
					op.After, floor, floor-1)
			}
		}
	}
}

// design.md, Words: Retention, "Only a current retained value on
// $broadcast stays, until its flag ends by its own rules"; "The floor is
// the oldest record retention has not passed"; Channels, $broadcast: "A
// retained value ends only by losing its flag to a newer retained publish,
// an empty one, its Message Expiry or retained.retention_period"; Store,
// Writes: "The writer executes exactly the sealed group as one
// transaction"; Store, full mode: "A restart recovers committed state".
//
// $broadcast keeps two and a half records. A publisher stores kept/1, a
// retained value, then three plain records: the size pass passes kept/1,
// keeping it below the floor. A newer retained publish on kept/1, or an
// empty one, then ends its flag: once the transaction storing that publish
// commits, kept/1's passed record is gone from the database, so no later
// pass, nor a crash before one, leaves it behind.
func TestRetainedValueBelowTheFloorGoesWithItsFlag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"a newer value", testkit.Filling("kept/1", testkit.RecordBytes)},
		{"an empty retained publish", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLiveScript(t, broadcastKeeps)
			pub := s.connect("publisher", 0, 65535)
			floor := s.passValues(pub, 1)
			passed := func() (n int) {
				if err := s.db.QueryRowContext(context.Background(),
					`SELECT count(*) FROM records WHERE channel = 1 AND topic = 'kept/1' AND "offset" < ?`, floor).Scan(&n); err != nil {
					t.Fatalf("kept/1's records: %v", err)
				}
				return n
			}
			if passed() != 1 {
				t.Fatalf("$broadcast holds %d records of kept/1 below its floor %d; want its value", passed(), floor)
			}

			s.hold = func(*core.Group) bool { return true }
			s.retain(pub, 5, "kept/1", tc.payload)
			s.serve()
			if len(s.held) != 1 || recordsIn(s.held[0], 1) != 1 {
				t.Fatalf("after %s on kept/1, %d groups held; want one, storing it", tc.name, len(s.held))
			}
			g := s.held[0]
			s.held, s.hold = nil, nil
			s.commit(g)
			if n := passed(); n != 0 {
				t.Errorf("once the transaction storing %s on kept/1 has committed, $broadcast still holds kept/1's passed record below its floor %d; want it removed in that transaction",
					tc.name, floor)
			}
			s.serve()
		})
	}
}

// retainedFor gives retained values on $broadcast a retained.retention_period
// of agedPeriod (config.md, broker.retained).
const retainedFor = `  retained:
    retention_period: 10s
`

// storeAt starts a live script on channels, connects a publisher, and has
// its reader hand the core p, the core taking the PUBLISH at intake; it
// then serves what that leads to.
func storeAt(t *testing.T, channels string, intake time.Time, p *packet.Publish) *liveScript {
	t.Helper()
	s := newLiveScript(t, channels)
	pub := s.connect("publisher", 0, 65535)
	s.advance(intake.Add(-2 * eventGap))
	if !s.now.Equal(intake.Add(-2 * eventGap)) {
		t.Fatalf("the script's clock is at %s after the connection; want %s, two events before the intake", clockOf(s.now), clockOf(intake.Add(-2*eventGap)))
	}
	s.send(pub, p)
	if !s.now.Equal(intake) {
		t.Fatalf("the core took the PUBLISH at %s; want %s", clockOf(s.now), clockOf(intake))
	}
	s.serve()
	return s
}

// design.md, Words: Retention, "A pass [...] deletes the run of them past
// retention in one range, and stops at the first record not yet due";
// "retention deletes late, never early"; "Age retention becomes due at the
// earliest applicable record, retained-value or tombstone deadline";
// PUBLISH: "The record's timestamp is the core's intake time"; Channels,
// $broadcast: "A retained value ends only by losing its flag to a newer
// retained publish, an empty one, its Message Expiry or
// retained.retention_period, measured from the publish". config.md,
// retention_period, "append: removes old records"; retained.retention_period,
// "a retained value ends this long after its publish". design.md, Testing:
// "The core never reads the clock [...] An inside test therefore drives it
// with scripted events and fake times"; where an intake falls within a
// millisecond is an input no client can force.
//
// The core takes a PUBLISH at a time a part of a millisecond past one: 0,
// 1 µs, 700 µs or 999 µs. A record on aged, kept 10 seconds, is not deleted
// by any timer the core asks for before intake + 10 s, nor by what that
// timer leads to, and is deleted by the first timer at or after it. A
// retained value on $broadcast, under a retained.retention_period of 10
// seconds, keeps its flag until intake + 10 s, and loses it to the first
// timer at or after it.
func TestRetentionIsTimedFromTheIntake(t *testing.T) {
	const flagged = `SELECT count(*) FROM records WHERE channel = 1 AND topic = 'kept/1' AND retained = 1`
	for _, past := range []time.Duration{0, time.Microsecond, 700 * time.Microsecond, 999 * time.Microsecond} {
		intake := epoch.Add(time.Minute + past)
		due := intake.Add(agedPeriod)

		t.Run(fmt.Sprintf("a record, %v past a millisecond", past), func(t *testing.T) {
			s := storeAt(t, agedChannel, intake, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("aged/1"),
				Payload: testkit.Filling("aged/1", testkit.RecordBytes)})
			aged := s.channelID("aged")
			if n := s.recordsOn(aged); n != 1 {
				t.Fatalf("aged holds %d records once aged/1 is stored; want 1", n)
			}
			first := s.fireBefore(due, func(at time.Time) {
				if s.recordsOn(aged) == 0 {
					t.Fatalf("aged/1 is deleted by the timer at %s, %v before its retention_period has passed, at %s",
						clockOf(at), due.Sub(at), clockOf(due))
				}
			})
			s.fireStill(first)
			if n := s.recordsOn(aged); n != 0 {
				t.Fatalf("aged/1 is not deleted by the first timer at or after its retention_period has passed, at %s (due %s)", clockOf(first), clockOf(due))
			}
		})

		t.Run(fmt.Sprintf("a retained value, %v past a millisecond", past), func(t *testing.T) {
			s := storeAt(t, retainedFor, intake, &packet.Publish{QoS: 1, Retain: true, PacketID: 1, Topic: []byte("kept/1"),
				Payload: testkit.Filling("kept/1", testkit.RecordBytes)})
			if n := s.count(flagged); n != 1 {
				t.Fatalf("$broadcast holds %d retained values on kept/1 once it is stored; want 1", n)
			}
			first := s.fireBefore(due, func(at time.Time) {
				if s.count(flagged) == 0 {
					t.Fatalf("kept/1's retained value ends at the timer at %s, %v before its retained.retention_period has passed, at %s",
						clockOf(at), due.Sub(at), clockOf(due))
				}
			})
			s.fireStill(first)
			if n := s.count(flagged); n != 0 {
				t.Fatalf("kept/1's retained value does not end at the first timer at or after its retained.retention_period has passed, at %s (due %s)",
					clockOf(first), clockOf(due))
			}
		})
	}
}
