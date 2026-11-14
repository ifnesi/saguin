package core_test

import (
	"cmp"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
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

// The API these tests fix (items 0007a, the core running without session
// rows, and 0007d, the core's side of sessions in the store; 0026b, the
// core's write groups):
//
//	type Core struct{ ... } // all broker state, owned by the core goroutine (R1)
//
//	// New returns the core for cfg, a resolved configuration, with state,
//	// what the store's Start loaded, sending its store requests on writes
//	// and reads (store.Store's Writes and Reads). It registers on reg the
//	// series the core sets (metrics.md, Connections and sessions; Store and
//	// memory), each at 0 from start (metrics.md, Rules):
//	// saguin_connections{protocol} for "3.1.1" and "5",
//	// saguin_sessions_ended_total{cause} for "clean_start", "expired" and
//	// "connection_end", saguin_session_expiry_capped_total,
//	// saguin_store_commits_total{closed_by} for "event", "interval",
//	// "max_records", "barrier" and "stop", and
//	// saguin_store_committed_records_total. It groups its writes as cfg's
//	// broker.storage publish_commit_interval and publish_commit_max_records
//	// say (design.md, Store, Writes).
//	// The core sends the log lines it admits on lines (the logger's Lines)
//	// and owns credits, log admission, while it runs: main hands it over
//	// before Run and takes it back once Run has returned (design.md,
//	// Flows, Logging: "Before the core starts and after it finishes, main
//	// alone owns admission; ownership does not overlap"; internal/logger,
//	// logger_test.go).
//	func New(cfg *config.Config, reg *metrics.Registry, state *store.State,
//		writes chan<- store.Write, reads chan<- store.Read,
//		lines chan<- logger.Line, credits *logger.Credits) *Core
//
//	// Action is one thing a decision asks the loop to do, exactly one of:
//	// put Reply on Conn's replies queue (Conn.Replies), where it always
//	// finds room; send Write to the store's writer; send Read to its
//	// readers; send Line, a line credits admitted, to the logger. A read
//	// carries in Conn the connection it came from, nil for none (0007a.md,
//	// 261016 071128: "Each store request carries the connection it came
//	// from"); a write is a group, whose members may come from several, and
//	// these tests read no connection from it. The loop sends a request as
//	// it is.
//	type Action struct {
//		Conn  *connection.Conn
//		Reply connection.Reply
//		Write store.Write
//		Read  store.Read
//		Line  logger.Line // a line to send, when not the zero Line
//	}
//
//	// Logged is the decision on one of the logger's results: credits
//	// processes it (Credits.Return), so its line's credit comes back
//	// (design.md, Flows, Logging: "An admitted line holds its credit until
//	// the core processes its output-write result").
//	func (c *Core) Logged(r logger.Result)
//
//	// The core logs, at Error, with the event's time, a store error once
//	// (design.md, R8), and the error a writer's close report carries (a
//	// Closed event's Packet, connection.CannotBuild, for a reply it could
//	// not build), once per report (design.md, Flows, Logging: "Runtime log-worthy events reach
//	// the core through their existing event or failure reports").
//
//	// Group is one write transaction, its members writes that are not
//	// groups, run in order (design.md, Store, Writes: "The writer executes
//	// exactly the sealed group, in member order, and returns one transaction
//	// result"). Every write the core sends is a group, never empty, one per
//	// Action, and the store answers the group itself, as Answer.Op. A
//	// CONNECT's session ending and its new session row are two consecutive
//	// members of one group (design.md, CONNECT, 7: "in one transaction").
//	type Group struct{ Writes []store.Write }
//	func (g *Group) WriteTx(tx *store.Tx) error // its members', in order, the first error ending it
//	func (g *Group) Records() int               // the sum of its members'
//
//	// On each successful answer to a group, the core adds 1 to
//	// saguin_store_commits_total under the group's commit cause, which the
//	// first event making it due or sealing it fixes, and the group's
//	// Records() to saguin_store_committed_records_total (metrics.md, Store
//	// and memory; design.md, Store, Writes).
//
//	// Handle is the core's decision on a connection's event (design.md,
//	// Testing: "a function of its state, one event and that
//	// event's time, returning what to do"): it takes ev, which happened
//	// at now, appends to acts what ev asks for, in the order it is to be
//	// done, and returns the extended slice. It reads nothing of a Conn but
//	// its identity, and never the clock.
//	func (c *Core) Handle(acts []Action, ev connection.Event, now time.Time) []Action
//
//	// Answer is the decision on the store's answer to a request the core
//	// sent, a.Op being that request, the very value sent. A read is
//	// answered twice (internal/store, store_test.go): first with View set,
//	// the reader's confirmation that the read's view is established, which
//	// returns no credit; then with its result, which does (design.md,
//	// Store reads; R2).
//	func (c *Core) Answer(acts []Action, a store.Answer, now time.Time) []Action
//
//	// Fire is the decision on the one timer (design.md, Words: an event is
//	// "a timer"): the loop hands it the start's time first, before any
//	// other event, and then each time Timer's time comes. The deadlines
//	// due at now run in R2's equal-deadline order, the collection
//	// deadline, the commit cutoff, among them (design.md, Store, Writes).
//	func (c *Core) Fire(acts []Action, now time.Time) []Action
//
//	// Timer returns when the core wants Fire next, the earliest of its
//	// deadlines, the zero time for never: a time already past asks for it
//	// at once.
//	func (c *Core) Timer() time.Time
//
//	// Credit returns the store requests the core may still send (R2):
//	// store.Credit less those it has sent and had no answer to.
//	func (c *Core) Credit() int
//
//	// Intake reports whether Run may take from the readers' channel
//	// (design.md, Execution, Communication: "the core does not take
//	// from it while it has no store credit"):
//	// Credit is above 0. The batch bound withholds publication bodies'
//	// grants alone: "while the unresolved publication batches are at
//	// their bound (Capacities), it grants no publication body, taking
//	// every other event".
//	func (c *Core) Intake() bool
//
//	// Batches returns the publication batches accepted and unresolved,
//	// accumulating, sealed and outstanding together, on every channel,
//	// with the slot each granted body, unread or held behind its CONNECT,
//	// reserves (design.md, Message memory: "A granted body, unread or held
//	// behind its CONNECT, reserves one potential batch slot; batches and
//	// reserved slots together obey the batch bound"; Execution,
//	// Capacities: "commit, refusal or abandonment releases a batch").
//	func (c *Core) Batches() int
//
//	// Clock is the loop's time and its one real timer (design.md, Testing:
//	// the loop "owns the one real timer"): Now is read once for each event
//	// the loop takes, the start's Fire included, as its time; Timer(t)
//	// makes a timer, returning its channel, which receives once Now has
//	// reached t, at once where it has, and reset, which moves that same
//	// timer to another time, a fire the loop has not taken dropped. At(t),
//	// a channel of its own for one wait, is the connections' (internal/
//	// connection), not the loop's.
//	type Clock interface {
//		Now() time.Time
//		At(t time.Time) <-chan time.Time
//		Timer(t time.Time) (c <-chan time.Time, reset func(time.Time))
//	}
//
//	// Run is the loop around the decisions, on the core goroutine. It
//	// first hands Fire the start's time; then, until stop is closed, it
//	// takes events one at a time from stop, answers (the store's
//	// Answers), its one timer's channel, set for Timer's time (none while
//	// Timer is zero) and inbound, which it takes from only while Intake
//	// reports true (design.md, Execution, Communication: "the core does
//	// not take from it while it has no store credit"). It takes the
//	// logger's results
//	// from results (the logger's Results). When several are ready it takes
//	// them in design.md, Execution, Event order's: "stop, store answers,
//	// logger results, the core's timer, then the readers' channel". It carries out each action, in order, and sets its timer
//	// when Timer's time changes, and only then: clock.Timer makes it the
//	// first time, and its reset moves it every time after. The
//	// listeners, readers and writers send on inbound, which is never closed
//	// (R3); stop is the core's own.
//	func (c *Core) Run(inbound <-chan connection.Event, answers <-chan store.Answer,
//		results <-chan logger.Result, stop <-chan struct{}, clock Clock)
//
// The events are the connections' (internal/connection, connection_test.go):
// Accepted, Received (a *packet.Connect with the reader's Check result as
// its Reason, or a Pingreq), Ended (the client's *packet.Disconnect, or the
// DISCONNECT reason the reader's end owes), and Closed, the writer's close
// report, its Packet a connection.CannotBuild carrying the error of a reply
// it could not build, else nil; the store's
// answers; the logger's results; and the timer. The replies are a
// *packet.Connack, a Pingresp, and a final entry, a *packet.Disconnect or a
// Close, of which each connection gets exactly one: by its Ended, or, where
// its Ended leaves a store request of that connection outstanding, by that
// request's answer (design.md, DISCONNECT: the new session expiry "is
// stored before the connection closes"), since its writer ends only on it
// (0007a.md, 261012 112716).
//
// The store requests are the store's own (internal/store, sessions_test.go):
// reads, a *store.LoadSession at a CONNECT (design.md, CONNECT, 4) and the
// *store.DueSessions of a pass through the sessions due, each after the
// last session the pass handled (design.md, DISCONNECT: "Each time its
// timer comes, the core goes once through the sessions due, in expiry
// order"); writes, a *store.SaveSession, a *store.LeaveSession, a
// *store.EndSession or a write of messages into a channel, each a member of
// a *Group. The core assigns session ids (storage.md, Ids: "ids start from 1
// and assignment resumes at the highest of that kind loaded at start plus
// one"), every session one, durable or not; a durable session's row is its
// sessions row (storage.md): Client its client id, Protocol 4 or 5, Expiry
// the session expiry granted, Left 0 while connected and else when its
// connection ended, ReceiveMaximum the client's (MQTT 5's default 65,535
// where absent), KeepAlive the keep-alive granted, CleanStart its last
// CONNECT's, Listener and Remote its connection's Accepted event's
// (connection.Accept), Remote as its address's String. User is "" until
// access is built. A session's expiry, while it is away, is its Left plus its
// Expiry in seconds.
//
// These tests drive the decisions alone, with scripted events, a scripted
// store, fake times and zero connections (new(connection.Conn)), which
// stand for a connection by identity only; loop_test.go drives Run.

// epoch is the scripted events' first time: a fake one, before any the
// tests run at.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// baseYAML is the smallest valid configuration, with one tcp listener on a
// loopback port the system picks. Its store file is opened only by the
// tests that run a real store, which move it under the test's directory.
const baseYAML = `
broker:
  id: test
  storage:
    file_path: /var/lib/saguin/saguin.db
    default_retention_period: 1d
    default_retention_bytes: 1GiB
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
`

// newConfig returns baseYAML resolved, every default filled in.
func newConfig(t testing.TB) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(baseYAML))
	if err != nil {
		t.Fatalf("the tests' configuration: %v", err)
	}
	return cfg
}

// connackCodes are the codes a CONNACK may carry (design.md, CONNECT):
// 0x00, and the refusal table's "Refused with" column; 0x80 and above but
// those, and any other code, are never sent (0007a.md, 261012 112631).
var connackCodes = []byte{0x00, 0x84, 0x85, 0x86, 0x87, 0x90, 0x9A, 0x9B, 0x95, 0x83, 0x8C, 0x97}

// disconnectCodes are design.md, DISCONNECT, "Reasons Sagüin sends": 0x81,
// 0x82, 0x8B, 0x8D, 0x8E, 0x93, 0x94, 0x95, 0x98, 0x9A, 0x9B and 0x9E; 0x80
// is "never sent".
var disconnectCodes = []byte{0x81, 0x82, 0x8B, 0x8D, 0x8E, 0x93, 0x94, 0x95, 0x98, 0x9A, 0x9B, 0x9E}

// protocols names each version as saguin_connections' protocol label
// (metrics.md, Rules: "protocol is 3.1.1 or 5").
var protocols = map[packet.Version]string{packet.V311: "3.1.1", packet.V5: "5"}

// endCauses are saguin_sessions_ended_total's causes (metrics.md).
var endCauses = []string{"clean_start", "expired", "connection_end"}

// connLog is what the script knows of one connection.
type connLog struct {
	accept  connection.Accept // what its Accepted carried
	version packet.Version    // its CONNECT's: V311 for MQTT 3.1.1 or any level the reader refused
	connect *packet.Connect   // its CONNECT, once handed over
	replies []connection.Reply
	connack *packet.Connack // its CONNACK, once sent
	final   bool            // its final entry has been sent
	cut     bool            // the core has cut it: closed its raw socket
	ended   bool            // its Ended has been handed over
	closed  bool            // its close report has been handed over
	asked   int64           // the body bytes its reader asked for and has not been given
	granted int64           // the body bytes it was given and has not handed over
}

// holds reports whether the connection holds its session: its CONNACK was
// 0x00 and it has had no final entry.
func (l *connLog) holds() bool {
	return l.connack != nil && l.connack.ReasonCode == 0 && !l.final
}

// request is a store request the core sent and the script's store has not
// answered.
type request struct {
	op      any      // the Read or *core.Group, as sent
	clients []string // the client ids whose sessions it reads or writes
	viewed  bool     // a read whose view the store has confirmed (view)
}

// script plays a broker's connections and its store to one core, an event
// at a time, tick apart (a second of fake time unless a test sets it),
// checking after each decision what every decision keeps (decided). Its
// store holds the sessions rows and applies each write the core sends as
// the real one would (apply), failing the test on any the real one refuses.
// It answers each read as the store's readers do (design.md, Store reads):
// first the confirmation that the read's view is established, its result
// fixed from the rows held then (view), and later the result (reply). The
// confirmation comes at once, the next event after the decision that sent
// the read, at that decision's time, unless the test holds it (holdViews)
// and gives it itself. Its clock's time is the last event's. In memory mode
// it tells the requests of a snapshot from the rest (snapshot_test.go), in
// normal mode a sync (sync_test.go).
type script struct {
	clock
	cfg       *config.Config
	reg       *metrics.Registry
	tick      time.Duration // how far each event is from the last
	holdViews bool          // reads' views are confirmed only by the test's view
	conns     map[*connection.Conn]*connLog
	credits   *logger.Credits // the core's log admission
	lines     []logger.Line   // the log lines the core sent, in order

	rows        map[string]store.Session // the store's sessions rows, by client id
	clients     map[int64]string         // session ids, as rows and requests named them, to client ids
	outstanding []request                // the store requests sent and not answered, oldest first
	snap        snapshots                // memory mode's snapshots, as the store sees them
	sync        syncs                    // normal mode's syncs, as the store sees them
}

// newScript returns a script of a new core for cfg, started on an empty
// store.
func newScript(t *testing.T, cfg *config.Config) *script {
	return newScriptOn(t, cfg, &store.State{})
}

// newScriptOn returns a script of a new core for cfg, started on a store
// that loaded state and holds rows.
func newScriptOn(t *testing.T, cfg *config.Config, state *store.State, rows ...store.Session) *script {
	reg := metrics.New()
	credits := logger.NewCredits(cfg.Broker.LogLevel, reg)
	s := &script{
		t: t, cfg: cfg, reg: reg, core: core.New(cfg, reg, state, nil, nil, nil, credits), now: epoch, tick: time.Second,
		conns: map[*connection.Conn]*connLog{}, credits: credits,
		rows: map[string]store.Session{}, clients: map[int64]string{},
	}
	s.run = s
	for _, row := range rows {
		s.rows[row.Client] = row
		s.clients[row.ID] = row.Client
	}
	return s
}

// step hands ev to the core and returns its decision, checked (decided).
func (s *script) step(ev *connection.Event) []core.Action {
	s.t.Helper()
	s.now = s.now.Add(s.tick)
	log := s.conns[ev.Conn]
	switch ev.Kind {
	case connection.Accepted:
		log = &connLog{version: packet.V311, accept: ev.Packet.(connection.Accept)}
		s.conns[ev.Conn] = log
	case connection.Received:
		if p, ok := ev.Packet.(*packet.Connect); ok {
			log.connect = p
			if p.Version == packet.V5 {
				log.version = packet.V5
			}
		}
	case connection.Ended:
		log.ended = true
	case connection.Closed:
		log.closed = true
	case connection.BytesRequested:
		log.asked = ev.Bytes
	}
	s.body(ev, log)
	return s.decided(s.core.Handle(nil, *ev, s.now))
}

// body fails the test unless ev carries a PUBLISH body only as a reader
// does: design.md, Message memory, Asking: "Before allocating or reading a
// nonempty PUBLISH body, its reader requests the body's Remaining Length
// from the core. Every such body uses this request. A reader has at most
// one request or granted body." A PUBLISH handed over (Received), refused
// (Refused) or carried by its reader's end (Ended) carries exactly the
// bytes its reader was given; an end carrying none leaves a grant unread.
func (s *script) body(ev *connection.Event, log *connLog) {
	s.t.Helper()
	carries := ev.Kind == connection.Refused
	switch ev.Packet.(type) {
	case *packet.Publish:
		carries = ev.Kind == connection.Received
	case connection.PublishRefusal:
		carries = true
	}
	if !carries {
		if ev.Bytes != 0 && ev.Kind != connection.BytesRequested {
			s.t.Fatalf("%v carries %d body bytes and no PUBLISH", ev.Kind, ev.Bytes)
		}
		if ev.Kind == connection.Ended {
			log.granted = 0
		}
		return
	}
	if ev.Bytes <= 0 || ev.Bytes != log.granted {
		s.t.Fatalf("a PUBLISH body of %d bytes handed to the core, its reader given %d: every nonempty body is asked for and given first",
			ev.Bytes, log.granted)
	}
	log.granted = 0
}

// fire hands the core its timer, a tick after the last event, and returns
// the decision, checked.
func (s *script) fire() []core.Action {
	s.t.Helper()
	return s.fireAt(s.now.Add(s.tick))
}

// fireAt hands the core its timer at at, and returns the decision, checked.
func (s *script) fireAt(at time.Time) []core.Action {
	s.t.Helper()
	s.now = at
	return s.decided(s.core.Fire(nil, s.now))
}

// take checks a decision the script's clock hands the core (decided).
func (s *script) take(acts []core.Action) {
	s.t.Helper()
	s.decided(acts)
}

// serve answers what the decisions the script's clock hands the core ask
// for (settle).
func (s *script) serve() {
	s.t.Helper()
	s.settle()
}

// stop hands the core the stop (design.md, Execution, Event order: "stop,
// store answers, logger results, the core's timer, then the readers'
// channel"; export_test.go) and returns the decision, checked.
func (s *script) stop() []core.Action {
	s.t.Helper()
	s.now = s.now.Add(s.tick)
	return s.decided(s.core.BeginStop(nil, s.now))
}

// view hands the core the store's confirmation that op, an outstanding
// read whose view is not yet confirmed, has established its view
// (design.md, Store reads: "the reader confirms that its read transaction
// has established that view"), and returns the decision, checked. The
// store fixes op's result as it holds then (fill): writes it applies later
// do not change it. The confirmation returns no credit: the read holds its
// one until its result is processed (R2).
func (s *script) view(op store.Read) []core.Action {
	s.t.Helper()
	i := slices.IndexFunc(s.outstanding, func(r request) bool { return r.op == op })
	if i < 0 || s.outstanding[i].viewed {
		s.t.Fatalf("a view confirmed for %T %p, which is not an outstanding read awaiting one", op, op)
	}
	s.outstanding[i].viewed = true
	s.fill(op)
	return s.decided(s.core.Answer(nil, store.Answer{Op: op, View: true}, s.now))
}

// reply hands the core the store's answer to op, an outstanding request or
// a member of an outstanding group, with err, and returns the decision,
// checked. A member's answer is its group's: the store answers the group,
// every member with it (design.md, Store, Writes: the writer "returns one
// transaction result"). A group answered with no error is applied to the
// script's store first. A read's view is confirmed first where it is not
// yet (view); its result is then as the test left it.
func (s *script) reply(op any, err error) []core.Action {
	s.t.Helper()
	var acts []core.Action
	if r, ok := op.(store.Read); ok && slices.ContainsFunc(s.outstanding, func(o request) bool { return o.op == op && !o.viewed }) {
		acts = s.view(r)
	}
	i := slices.IndexFunc(s.outstanding, func(r request) bool {
		g, ok := r.op.(*core.Group)
		return r.op == op || (ok && slices.ContainsFunc(g.Writes, func(w store.Write) bool { return w == op }))
	})
	if i < 0 {
		s.t.Fatalf("an answer to %T %p, which is not outstanding", op, op)
	}
	op = s.outstanding[i].op
	s.outstanding = slices.Delete(s.outstanding, i, i+1)
	if w, ok := op.(store.Write); ok && err == nil {
		s.apply(w)
	}
	s.now = s.now.Add(s.tick)
	s.answered(op)
	return append(acts, s.decided(s.core.Answer(nil, store.Answer{Op: op, Err: err}, s.now))...)
}

// answer answers op as the script's store does: a read with what the store
// held as its view was established (view), a write saved.
func (s *script) answer(op any) []core.Action {
	s.t.Helper()
	return s.reply(op, nil)
}

// settle answers every outstanding request, oldest first, but those a
// test holds (holds), and every one those answers lead to, until none is
// left, and returns the decisions. A core still asking after settleLimit
// answers is asking without end.
func (s *script) settle() []core.Action {
	s.t.Helper()
	var acts []core.Action
	for n := 0; ; n++ {
		i := slices.IndexFunc(s.outstanding, func(r request) bool { return !s.holds(r.op) })
		if i < 0 {
			return acts
		}
		if n == settleLimit {
			s.t.Fatalf("the core still asks the store after %d answers, the last %T", n, s.outstanding[i].op)
		}
		acts = append(acts, s.answer(s.outstanding[i].op)...)
	}
}

// settleLimit is far more answers than any test's store requests: one per
// request each connection, session and due session in a test can need.
const settleLimit = 4 * store.Credit

// decided checks every action of a decision (check), then what holds
// between decisions:
//   - the core's credit is store.Credit less the requests outstanding, a
//     group counted once, never lost and never counted twice (design.md,
//     Store, Requests: "Each dispatched read and each dispatched write
//     group consumes one store credit until its answer is processed; a
//     group's members consume no separate credits");
//   - a connection whose Ended has been taken has had its final entry, or
//     a store request is outstanding, whose answer the entry waits for: its
//     own request, or the one write transaction outstanding, which its write
//     is in or waits behind (design.md, DISCONNECT: the new expiry "is
//     stored before the connection closes"; Store, Writes: "The core keeps
//     at most one write transaction outstanding at the store"; 0007a.md,
//     261012 112716: "a missed final entry would leave a writer goroutine
//     alive for good");
//   - a connection cut has had its final entry (design.md, Flows, Closing a
//     connection: "The core cuts a connection only by closing its raw socket.
//     It then sends the writer its final entry at once").
//
// Unless the test holds views, the store then confirms the view of each
// read the decision sent, in order (view), and the decisions on those
// confirmations follow the decision's own actions.
func (s *script) decided(acts []core.Action) []core.Action {
	s.t.Helper()
	for i := range acts {
		s.check(&acts[i])
	}
	if got, want := s.core.Credit(), store.Credit-len(s.outstanding); got != want {
		s.t.Fatalf("Credit() = %d with %d requests outstanding, want %d (R2)", got, len(s.outstanding), want)
	}
	for _, log := range s.conns {
		if log.ended && !log.final && len(s.outstanding) == 0 {
			s.t.Fatalf("a connection's Ended taken, and no final entry sent nor store request outstanding: its writer would never end")
		}
		if log.cut && !log.final {
			s.t.Fatalf("a connection cut, and no final entry sent: its writer would never end (R2)")
		}
	}
	if !s.holdViews {
		for _, a := range acts { // the decision's own actions: views add to acts
			if a.Read != nil {
				acts = append(acts, s.view(a.Read)...)
			}
		}
	}
	return acts
}

// check fails the test unless a is one thing to do, and keeps decided's
// rules:
//   - a log line is noted, in order (lines);
//   - a store request names a connection accepted, if any, and is one this
//     item's core sends, a write being a group sent while no other is
//     outstanding (request);
//   - each reply names a connection accepted and not yet reported closed,
//     a writer that has ended taking nothing (R2);
//   - each reply is a *packet.Connack, a Pingresp, an acknowledgement, a
//     *packet.Suback, a *packet.Disconnect or a Close (connection.Reply), and nothing follows a connection's final
//     entry; after its Ended, nothing but that entry: "If the store confirms
//     after that connection has ended or been taken over, what was stored
//     stays and the reply is dropped" (R4);
//   - a connection's first packet is its CONNACK, its only one, and a
//     PINGRESP goes only after a CONNACK of 0x00: "The Server MUST send a
//     CONNACK with a 0x00 (Success) Reason Code before sending any Packet
//     other than AUTH [MQTT-3.2.0-1]. The Server MUST NOT send more than
//     one CONNACK in a Network Connection [MQTT-3.2.0-2]"; after a refused
//     CONNECT it "MUST NOT process any data sent by the Client after the
//     CONNECT packet except AUTH packets [MQTT-3.1.4-6]";
//   - each code is one the design lists (connackCodes, disconnectCodes;
//     0007a.md, 261012 112631), a refusing CONNACK's Session Present 0
//     [MQTT-3.2.2-6], and each CONNACK one the writer can build in the
//     connection's version (packet.AppendConnack: no properties on MQTT
//     3.1.1, every MQTT 5 property within its rules);
//   - a cut names a connection accepted, not yet reported closed and not
//     cut before (design.md, Flows, Closing a connection: the core "cuts a
//     connection only by closing its raw socket");
//   - bytes are given to a reader that asked, before its final entry, and
//     exactly what it asked: design.md, Message memory: "a request
//     receives its whole reservation or waits"; "A reader has at most one
//     request or granted body" (connection.Conn.MessageBytes).
func (s *script) check(a *core.Action) {
	s.t.Helper()
	line := a.Line != (logger.Line{})
	if kinds := countTrue(a.Reply != nil, a.Write != nil, a.Read != nil, a.Cut, a.Bytes != 0, line); kinds != 1 {
		s.t.Fatalf("an action of reply %v, write %T, read %T, cut %v, %d bytes given and line %+v: want exactly one",
			a.Reply, a.Write, a.Read, a.Cut, a.Bytes, a.Line)
	}
	if line {
		s.lines = append(s.lines, a.Line)
		return
	}
	log := s.conns[a.Conn]
	if a.Bytes != 0 {
		switch {
		case log == nil:
			s.t.Fatalf("%d bytes given to a connection never accepted", a.Bytes)
		case log.final || log.closed:
			s.t.Fatalf("%d bytes given after the connection's final entry", a.Bytes)
		case a.Bytes != log.asked:
			s.t.Fatalf("%d bytes given to a reader that asked for %d", a.Bytes, log.asked)
		}
		log.asked, log.granted = 0, a.Bytes
		return
	}
	if a.Cut {
		switch {
		case log == nil:
			s.t.Fatalf("a cut of a connection never accepted")
		case log.closed:
			s.t.Fatalf("a cut after the connection's close report")
		case log.cut:
			s.t.Fatalf("a second cut of one connection")
		}
		log.cut = true
		return
	}
	if a.Reply == nil {
		s.request(a)
		return
	}
	switch {
	case log == nil:
		s.t.Fatalf("%s for a connection never accepted", show(a.Reply))
	case log.closed:
		s.t.Fatalf("%s after the connection's close report", show(a.Reply))
	case log.final:
		s.t.Fatalf("%s after the connection's final entry", show(a.Reply))
	}
	log.replies = append(log.replies, a.Reply)
	switch r := a.Reply.(type) {
	case *packet.Connack:
		if log.connack != nil {
			s.t.Fatalf("a second CONNACK, 0x%02X, after 0x%02X [MQTT-3.2.0-2]", r.ReasonCode, log.connack.ReasonCode)
		}
		log.connack = r
		if !slices.Contains(connackCodes, r.ReasonCode) {
			s.t.Fatalf("CONNACK 0x%02X, a code design.md's CONNECT does not list", r.ReasonCode)
		}
		if r.SessionPresent && r.ReasonCode != 0 {
			s.t.Fatalf("CONNACK 0x%02X with Session Present 1 [MQTT-3.2.2-6]", r.ReasonCode)
		}
		// design.md, CONNECT: 0x95's MQTT 3.1.1 return code is "none: the
		// connection closes", which the writer does in its place.
		v311TooLarge := log.version == packet.V311 && r.ReasonCode == 0x95
		if _, err := packet.AppendConnack(nil, log.version, r); err != nil && !v311TooLarge {
			s.t.Fatalf("CONNACK %+v cannot be built for protocol level %d: %v", *r, log.version, err)
		}
	case connection.Pingresp, *connection.Ack, *packet.Suback:
		if log.connack == nil || log.connack.ReasonCode != 0 {
			s.t.Fatalf("%s before a CONNACK of 0x00 [MQTT-3.2.0-1], or after a refused CONNECT [MQTT-3.1.4-6]", show(a.Reply))
		}
	case *packet.Disconnect:
		if !slices.Contains(disconnectCodes, r.ReasonCode) {
			s.t.Fatalf("DISCONNECT 0x%02X, a code design.md's DISCONNECT does not list", r.ReasonCode)
		}
		if _, err := packet.AppendDisconnect(nil, packet.V5, r); err != nil {
			s.t.Fatalf("DISCONNECT %+v cannot be built: %v", *r, err)
		}
		log.final = true
		return
	case connection.Close:
		log.final = true
		return
	default:
		s.t.Fatalf("a reply of type %T, which no writer takes", a.Reply)
	}
	if log.ended {
		s.t.Fatalf("%s after the connection's Ended: the reply is dropped (R4)", show(a.Reply))
	}
}

// request notes a store request the core sent, failing the test unless the
// connection it carries, if any, was accepted; unless a write is a group,
// with members, or memory mode's snapshot (snapshotRequest), sent while no
// other write is outstanding, and while no snapshot's read awaits the
// confirmation of its view (snapshotWritable): design.md, Store, Writes:
// "The core keeps at most one write transaction outstanding at the store";
// "Empty groups are not sent"; and unless no other request
// outstanding reads or writes a session of a client it names: design.md,
// Store, Requests, "Each session [...] keeps at most one request of its own
// in flight"; design.md, CONNECT, 3: "While an earlier CONNECT for the same
// id waits for the store, this one waits too"; 0007d.md, 261016 091943: a
// reconnect "is ordered with its ending by R2's one store request per
// session".
func (s *script) request(a *core.Action) {
	s.t.Helper()
	var op any = a.Write
	if a.Read != nil {
		op = a.Read
	}
	if a.Conn != nil && s.conns[a.Conn] == nil {
		s.t.Fatalf("%T carries a connection never accepted", op)
	}
	if s.snapshotRequest(op) || s.syncRequest(op) {
		s.outstanding = append(s.outstanding, request{op: op})
		return
	}
	if a.Write != nil {
		if _, ok := a.Write.(*core.Group); !ok {
			s.t.Fatalf("a write of type %T sent alone, want it a member of a group", a.Write)
		}
		if len(pending[store.Write](s)) != 0 {
			s.t.Fatalf("a group sent while another write is outstanding: one write transaction at a time (Store, Writes)")
		}
		s.snapshotWritable()
	}
	clients := s.named(op)
	for _, r := range s.outstanding {
		if i := slices.IndexFunc(clients, func(c string) bool { return slices.Contains(r.clients, c) }); i >= 0 {
			s.t.Fatalf("%T for client %q while %T for it is outstanding: one store request per session (R2)", op, clients[i], r.op)
		}
	}
	s.outstanding = append(s.outstanding, request{op: op, clients: clients})
}

// named returns the client ids whose sessions op reads or writes, failing
// the test on a request this item's core never sends: a group within a
// group, or one with no member.
func (s *script) named(op any) []string {
	s.t.Helper()
	switch op := op.(type) {
	case *store.LoadSession:
		return []string{op.Client}
	case *store.DueSessions:
		return nil
	case *store.SaveSession:
		s.clients[op.Session.ID] = op.Session.Client
		return []string{op.Session.Client}
	case *store.LeaveSession:
		return []string{s.clientOf(op.ID)}
	case *store.EndSession:
		return []string{s.clientOf(op.ID)}
	case *store.SaveSubscriptions:
		return []string{s.clientOf(op.Session)}
	case *core.Group:
		if len(op.Writes) == 0 {
			s.t.Fatalf("a group of no write (Store, Writes: \"Empty groups are not sent\")")
		}
		var clients []string
		for _, w := range op.Writes {
			if _, nested := w.(*core.Group); nested {
				s.t.Fatalf("a group within a group")
			}
			for _, c := range s.named(w) {
				if !slices.Contains(clients, c) {
					clients = append(clients, c)
				}
			}
		}
		return clients
	case store.Write:
		if op.Records() > 0 {
			return nil // messages into a channel: no session's rows
		}
	}
	s.t.Fatalf("a store request of type %T, which this item's core never sends", op)
	return nil
}

// clientOf returns the client id of session id, which a row or a request
// has named.
func (s *script) clientOf(id int64) string {
	s.t.Helper()
	client, ok := s.clients[id]
	if !ok {
		s.t.Fatalf("a request names session %d, which no row or request has", id)
	}
	return client
}

// apply applies w to the script's store as the store's writer does
// (internal/store, sessions_test.go), failing the test on a write the real
// one refuses (storage.md: "A reused key is a failed write and a store error
// (R8)"; the sessions CHECK) or on one that changes nothing.
func (s *script) apply(w store.Write) {
	s.t.Helper()
	switch w := w.(type) {
	case *core.Group:
		for _, member := range w.Writes {
			s.apply(member)
		}
	case *store.SaveSession:
		row := w.Session
		if row.ID < 1 || (row.Protocol != 4 && row.Protocol != 5) || row.Expiry < 1 || row.Left < 0 || row.ReceiveMaximum < 1 {
			s.t.Fatalf("SaveSession %+v breaks the sessions CHECK (storage.md)", row)
		}
		old, held := s.rows[row.Client]
		_, reused := s.rowWithID(row.ID)
		switch {
		case w.New && held:
			s.t.Fatalf("SaveSession inserts a second row for client %q (storage.md: client is unique)", row.Client)
		case w.New && reused:
			s.t.Fatalf("SaveSession inserts session %d, which a row has (storage.md: a reused key is a failed write)", row.ID)
		case !w.New && (!held || old.ID != row.ID):
			s.t.Fatalf("SaveSession updates session %d of %q, which has no row", row.ID, row.Client)
		}
		s.rows[row.Client] = row
	case *store.LeaveSession:
		row := s.rowOf(w.ID)
		if w.Expiry < 1 || w.Left < 1 {
			s.t.Fatalf("LeaveSession %+v breaks the sessions CHECK, or leaves no time (storage.md)", *w)
		}
		row.Expiry, row.Left = w.Expiry, w.Left
		s.rows[row.Client] = row
	case *store.EndSession:
		delete(s.rows, s.rowOf(w.ID).Client)
	case *store.SaveSubscriptions:
		// A durable session's subscriptions: the script keeps its sessions
		// row alone, which they need (storage.md, subscriptions).
		s.rowOf(w.Session)
	default:
		// Messages stored, a snapshot's fold or a sync: the script keeps
		// sessions rows alone.
		if w.Records() == 0 && w != s.snap.fold && w != s.sync.op {
			s.t.Fatalf("a write of type %T", w)
		}
	}
}

// rowOf returns the row of session id, failing the test where there is
// none: in this item a session's only row is its sessions row, so a write
// for a session without one is a write that changes nothing (design.md,
// Flows, DISCONNECT, A lost connection: "A session that ends with its
// connection is ended in the store where it has rows.").
func (s *script) rowOf(id int64) store.Session {
	s.t.Helper()
	row, ok := s.rowWithID(id)
	if !ok {
		s.t.Fatalf("a write for session %d, which has no row", id)
	}
	return row
}

// rowWithID returns the row of session id, if the script's store has one.
func (s *script) rowWithID(id int64) (store.Session, bool) {
	for _, row := range s.rows {
		if row.ID == id {
			return row, true
		}
	}
	return store.Session{}, false
}

// countTrue returns how many of conditions hold.
func countTrue(conditions ...bool) int {
	n := 0
	for _, c := range conditions {
		if c {
			n++
		}
	}
	return n
}

// fill fills read r as the store does: a LoadSession with the client's
// row; a DueSessions with the away sessions due by its At strictly after
// its After, in expiry, then id, order, at most store.DueBatch, and Next,
// the earliest expiry of the rest (internal/store, sessions_test.go), a
// session whose expiry never runs out left out of both (storage.md,
// sessions: an expiry of 4,294,967,295 "is excluded from the index and from
// every due-expiry query").
func (s *script) fill(r store.Read) {
	s.t.Helper()
	switch r := r.(type) {
	case *store.LoadSession:
		r.Session, r.Found = s.rows[r.Client]
	case *store.DueSessions:
		r.Sessions, r.Next = r.Sessions[:0], 0 // filled afresh, from the rows held now
		var away []store.Session
		for _, row := range s.rows {
			if row.Left > 0 && row.Expiry != math.MaxUint32 {
				away = append(away, row)
			}
		}
		slices.SortFunc(away, func(a, b store.Session) int {
			return cmp.Or(cmp.Compare(testkit.ExpiryAt(a.Left, a.Expiry), testkit.ExpiryAt(b.Left, b.Expiry)), cmp.Compare(a.ID, b.ID))
		})
		for i := range away {
			row := &away[i]
			expiry := testkit.ExpiryAt(row.Left, row.Expiry)
			if cmp.Or(cmp.Compare(expiry, r.After.Expiry), cmp.Compare(row.ID, r.After.ID)) <= 0 {
				continue // the pass has handled it
			}
			if expiry > r.At || len(r.Sessions) == store.DueBatch {
				r.Next = expiry
				break
			}
			r.Sessions = append(r.Sessions, store.DueSession{ID: row.ID, Client: row.Client, Expiry: expiry})
		}
	default:
		// A snapshot's read copies the database file and returns nothing.
		if r != s.snap.copy {
			s.t.Fatalf("a read of type %T", r)
		}
	}
}

// pending returns the outstanding requests of type T, oldest first, an
// outstanding group that is not a T giving its members of type T in its
// place.
func pending[T any](s *script) []T {
	var ops []T
	for _, r := range s.outstanding {
		if op, ok := r.op.(T); ok {
			ops = append(ops, op)
			continue
		}
		if g, ok := r.op.(*core.Group); ok {
			for _, w := range g.Writes {
				if op, ok := w.(T); ok {
					ops = append(ops, op)
				}
			}
		}
	}
	return ops
}

// only returns the one outstanding request of type T, failing the test
// unless there is exactly one.
func only[T any](s *script) T {
	s.t.Helper()
	ops := pending[T](s)
	if len(ops) != 1 {
		var zero T
		s.t.Fatalf("%d outstanding requests of type %T, want 1", len(ops), zero)
	}
	return ops[0]
}

// accept hands the core a new connection's Accepted, from listener "mqtt"
// or "ws" in turn, each from an address of its own.
func (s *script) accept() *connection.Conn {
	s.t.Helper()
	conn := new(connection.Conn)
	n := len(s.conns)
	remote := &net.TCPAddr{IP: net.IPv4(10, 0, byte(n>>8), byte(n)), Port: 40000 + n}
	s.step(&connection.Event{Kind: connection.Accepted, Conn: conn,
		Packet: connection.Accept{Listener: [...]string{"mqtt", "ws"}[n%2], Remote: remote}})
	return conn
}

// receive hands the core conn's CONNECT, with the reader's Check result,
// and returns the decision, answering nothing.
func (s *script) receive(conn *connection.Conn, p *packet.Connect, reason byte) []core.Action {
	s.t.Helper()
	return s.step(&connection.Event{Kind: connection.Received, Conn: conn, Packet: p, Reason: reason})
}

// connect hands the core conn's CONNECT, with the reader's Check result,
// answers every store request outstanding as the script's store does, and
// returns the CONNACK conn got, with every action of the CONNECT's decision
// and of the answers.
func (s *script) connect(conn *connection.Conn, p *packet.Connect, reason byte) (*packet.Connack, []core.Action) {
	s.t.Helper()
	acts := s.receive(conn, p, reason)
	acts = append(acts, s.settle()...)
	if ack := connackIn(acts, conn); ack != nil {
		return ack, acts
	}
	s.t.Fatalf("CONNECT for %q (reason 0x%02X) answered with %v, want a CONNACK", p.ClientID, reason, showAll(repliesTo(acts, conn)))
	return nil, nil
}

// connected accepts a connection whose CONNECT, p, every check passes, for
// a client id with no session, and checks its CONNACK: 0x00, Session
// Present 0 (design.md, CONNECT: Session Present is 1 only when "a session
// exists for the client id").
func (s *script) connected(p *packet.Connect) *connection.Conn {
	s.t.Helper()
	conn := s.accept()
	ack, _ := s.connect(conn, p, 0)
	if ack.ReasonCode != 0 || ack.SessionPresent {
		s.t.Fatalf("CONNACK 0x%02X, Session Present %v for %q; want 0x00, 0", ack.ReasonCode, ack.SessionPresent, p.ClientID)
	}
	return conn
}

// ping hands the core a PINGREQ from conn and returns the decision.
func (s *script) ping(conn *connection.Conn) []core.Action {
	s.t.Helper()
	return s.step(&connection.Event{Kind: connection.Received, Conn: conn, Packet: connection.Pingreq{}})
}

// pinged checks that a PINGREQ from conn is answered with a PINGRESP.
func (s *script) pinged(conn *connection.Conn) {
	s.t.Helper()
	if got := repliesTo(s.ping(conn), conn); len(got) != 1 || got[0] != (connection.Pingresp{}) {
		s.t.Fatalf("PINGREQ answered with %v, want a PINGRESP", showAll(got))
	}
}

// end hands the core conn's Ended, with the client's DISCONNECT or nil, and
// the reason the reader's end owes, and returns the decision.
func (s *script) end(conn *connection.Conn, d *packet.Disconnect, reason byte) []core.Action {
	s.t.Helper()
	ev := connection.Event{Kind: connection.Ended, Conn: conn, Reason: reason}
	if d != nil {
		ev.Packet = d
	}
	return s.step(&ev)
}

// close hands the core conn's close report, which asks for nothing: the
// writer has ended.
func (s *script) close(conn *connection.Conn) {
	s.t.Helper()
	s.closeReport(conn, nil)
}

// closeReport hands the core conn's close report carrying err, the error
// of a reply its writer could not build, or nil, and returns the decision.
func (s *script) closeReport(conn *connection.Conn, err error) []core.Action {
	s.t.Helper()
	ev := connection.Event{Kind: connection.Closed, Conn: conn}
	if err != nil {
		ev.Packet = connection.CannotBuild{Err: err}
	}
	return s.step(&ev)
}

// gone ends conn as a lost connection, answers what that asks of the store,
// and reports its close.
func (s *script) gone(conn *connection.Conn) {
	s.t.Helper()
	s.end(conn, nil, 0)
	s.settle()
	s.close(conn)
}

// expectConnections checks saguin_connections{protocol} for both versions.
func (s *script) expectConnections(v311, v5 float64) {
	s.t.Helper()
	for version, want := range map[packet.Version]float64{packet.V311: v311, packet.V5: v5} {
		series := `saguin_connections{protocol="` + protocols[version] + `"}`
		if got := testkit.Sample(s.t, s.reg, series); got != want {
			s.t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
}

// expectEnded checks saguin_sessions_ended_total for each cause: clean
// start, expired and connection end, in endCauses' order.
func (s *script) expectEnded(want ...float64) {
	s.t.Helper()
	for i, cause := range endCauses {
		series := `saguin_sessions_ended_total{cause="` + cause + `"}`
		if got := testkit.Sample(s.t, s.reg, series); got != want[i] {
			s.t.Errorf("%s = %v, want %v", series, got, want[i])
		}
	}
}

// commits is what metrics.md's commit series say: committed transactions by
// their commit cause, and the records they carried.
type commits struct {
	event, interval, maxRecords, barrier, stop, records float64
}

// expectCommits checks saguin_store_commits_total{closed_by} for each cause
// metrics.md names, and saguin_store_committed_records_total.
func (s *script) expectCommits(want commits) {
	s.t.Helper()
	got := commits{records: testkit.Sample(s.t, s.reg, "saguin_store_committed_records_total")}
	for _, c := range []struct {
		cause string
		n     *float64
	}{{"event", &got.event}, {"interval", &got.interval}, {"max_records", &got.maxRecords}, {"barrier", &got.barrier}, {"stop", &got.stop}} {
		*c.n = testkit.Sample(s.t, s.reg, `saguin_store_commits_total{closed_by="`+c.cause+`"}`)
	}
	if got != want {
		s.t.Errorf("the commit series: %+v, want %+v", got, want)
	}
}

// expectRows checks, with no store request outstanding, that the script's
// store holds what a restart would restore (storage.md, What is stored: "what
// a restart restores, and no more"): a row with Left 0 for each durable
// session a connection holds, as its CONNECT and CONNACK made it, with the
// listener and remote address its Accepted carried (storage.md, sessions:
// "the listener of its last connection", "its last connection's remote
// address"), none for a session that ends with its connection, and every
// other row away, its Left set (design.md, DISCONNECT: "The session goes away
// until its expiry").
func (s *script) expectRows() {
	s.t.Helper()
	if len(s.outstanding) != 0 {
		s.t.Fatalf("expectRows with %d requests outstanding", len(s.outstanding))
	}
	held := map[string]bool{}
	for _, log := range s.conns {
		if !log.holds() {
			continue
		}
		p := log.connect
		client, expiry := grantedTo(s.cfg, p, log.connack)
		held[client] = true
		row, ok := s.rows[client]
		switch {
		case expiry == 0 && ok:
			s.t.Errorf("client %q, connected with session expiry 0, has a row %+v: its session ends with its connection", client, row)
		case expiry == 0:
		case !ok:
			s.t.Errorf("client %q, connected with session expiry %d, has no row [MQTT-3.1.2-23]", client, expiry)
		case row.Left != 0 || row.Expiry != expiry || row.Protocol != byte(p.Version) || row.CleanStart != p.CleanStart ||
			uint32(row.KeepAlive) != settingsOf(log.connack, p).keepAlive ||
			(receiveMaximumOf(p) != 0 && row.ReceiveMaximum != receiveMaximumOf(p)) ||
			row.Listener != log.accept.Listener || row.Remote != log.accept.Remote.String():
			s.t.Errorf("client %q's row is %+v; want it connected (Left 0), Expiry %d, Protocol %d, CleanStart %v, KeepAlive %d, ReceiveMaximum %d, Listener %q, Remote %s",
				client, row, expiry, p.Version, p.CleanStart, settingsOf(log.connack, p).keepAlive, receiveMaximumOf(p), log.accept.Listener, log.accept.Remote)
		}
	}
	for client, row := range s.rows {
		if !held[client] && row.Left == 0 {
			s.t.Errorf("client %q, not connected, has a row left connected: %+v", client, row)
		}
	}
}

// grantedTo returns the client id and session expiry a CONNECT accepted
// with ack holds: MQTT 5's from the CONNACK, as the client reads it
// (settingsOf); MQTT 3.1.1's max_session_expiry for Clean Session 0
// (config.md: max_session_expiry "is MQTT 3.1.1 Clean Session 0's") and 0
// for Clean Session 1.
func grantedTo(cfg *config.Config, p *packet.Connect, ack *packet.Connack) (client string, expiry uint32) {
	if p.Version == packet.V5 {
		s := settingsOf(ack, p)
		return s.clientID, s.expiry
	}
	if p.CleanStart {
		return p.ClientID, 0
	}
	return p.ClientID, uint32(cfg.Broker.Limits.MaxSessionExpiry / time.Second)
}

// receiveMaximumOf is the client's Receive Maximum: its CONNECT's, else
// 65,535 (MQTT 5 3.1.2.11.3: "If the Receive Maximum value is absent then
// its value defaults to 65,535"). MQTT 3.1.1 has none; storage.md leaves its
// value to the item that builds in-flight windows, so 0 here stands for
// "any in the CHECK's range".
func receiveMaximumOf(p *packet.Connect) uint16 {
	if p.Version != packet.V5 {
		return 0
	}
	for prop := range p.Properties.All() {
		if prop.ID == packet.ReceiveMaximum {
			return uint16(prop.Int)
		}
	}
	return 65_535
}

// repliesTo returns, in order, the replies acts send conn.
func repliesTo(acts []core.Action, conn *connection.Conn) []connection.Reply {
	var replies []connection.Reply
	for _, a := range acts {
		if a.Conn == conn && a.Reply != nil {
			replies = append(replies, a.Reply)
		}
	}
	return replies
}

// replyCount returns how many of acts are replies, to any connection.
func replyCount(acts []core.Action) int {
	n := 0
	for _, a := range acts {
		if a.Reply != nil {
			n++
		}
	}
	return n
}

// connackIn returns the CONNACK acts send conn, nil for none.
func connackIn(acts []core.Action, conn *connection.Conn) *packet.Connack {
	for _, r := range repliesTo(acts, conn) {
		if ack, ok := r.(*packet.Connack); ok {
			return ack
		}
	}
	return nil
}

// groupsIn returns, in order, the groups acts send the store: every write
// the core sends is one (request).
func groupsIn(acts []core.Action) []*core.Group {
	var groups []*core.Group
	for _, a := range acts {
		if g, ok := a.Write.(*core.Group); ok {
			groups = append(groups, g)
		}
	}
	return groups
}

// writesIn returns the store writes in acts, each group's members in its
// place.
func writesIn(acts []core.Action) []store.Write {
	var writes []store.Write
	for _, g := range groupsIn(acts) {
		writes = append(writes, g.Writes...)
	}
	return writes
}

// members names g's members, in order, as the tests compare them: "save",
// "leave", "end" or "subscribe" and the client id of the session a member
// writes, or "records" and how many a write of messages holds.
func (s *script) members(g *core.Group) []string {
	s.t.Helper()
	names := make([]string, 0, len(g.Writes))
	for _, w := range g.Writes {
		switch w := w.(type) {
		case *store.SaveSession:
			names = append(names, "save "+w.Session.Client)
		case *store.LeaveSession:
			names = append(names, "leave "+s.clientOf(w.ID))
		case *store.EndSession:
			names = append(names, "end "+s.clientOf(w.ID))
		case *store.SaveSubscriptions:
			names = append(names, "subscribe "+s.clientOf(w.Session))
		default:
			names = append(names, "records "+strconv.Itoa(len(core.RecordsOf(w))))
		}
	}
	return names
}

// connectOf is a CONNECT, as a reader hands it over, at version, for
// clientID, Clean Start 1, a keep-alive of 60 seconds and properties
// (MQTT 5 only).
func connectOf(version packet.Version, clientID string, properties ...packet.Property) *packet.Connect {
	return decodedConnect(&packet.Connect{Version: version, CleanStart: true, KeepAlive: 60, ClientID: clientID,
		Properties: connectProperties(properties)})
}

// keeping is connectOf with Clean Start (MQTT 3.1.1: Clean Session) 0, and,
// for MQTT 5, a Session Expiry Interval of expiry seconds where it is not 0.
func keeping(version packet.Version, clientID string, expiry uint32) *packet.Connect {
	return decodedConnect(&packet.Connect{Version: version, KeepAlive: 60, ClientID: clientID,
		Properties: connectProperties(expiryProperty(version, expiry))})
}

// decodedConnect is c written and read back by the packet package
// (packet.AppendConnect, then packet.DecodeConnect of its body), as a
// reader hands a CONNECT over: carrying every field the decoder gives, the
// values it reads from the properties included, never restated here.
func decodedConnect(c *packet.Connect) *packet.Connect {
	wire, err := packet.AppendConnect(nil, c)
	if err != nil {
		panic(err)
	}
	fixed := 2 // the type byte, then the Remaining Length's bytes
	for wire[fixed-1]&0x80 != 0 {
		fixed++
	}
	p, err := packet.DecodeConnect(wire[fixed:])
	if err != nil {
		panic(err)
	}
	return &p
}

// expiryProperty is a Session Expiry Interval of seconds for MQTT 5 where
// seconds is not 0, and nothing otherwise.
func expiryProperty(version packet.Version, seconds uint32) []packet.Property {
	if version != packet.V5 || seconds == 0 {
		return nil
	}
	return []packet.Property{{ID: packet.SessionExpiryInterval, Int: seconds}}
}

// connectProperties is a CONNECT's property section, written and read back
// by the packet package, as a reader hands it over.
func connectProperties(properties []packet.Property) packet.Properties {
	wire, err := packet.AppendProperties(nil, packet.CONNECT, properties)
	if err != nil {
		panic(err)
	}
	section, _, err := packet.DecodeProperties(wire, packet.CONNECT)
	if err != nil {
		panic(err)
	}
	return section
}

// isDisconnect reports whether r is a DISCONNECT of code.
func isDisconnect(r connection.Reply, code byte) bool {
	d, ok := r.(*packet.Disconnect)
	return ok && d.ReasonCode == code
}

// show renders a reply for messages.
func show(r connection.Reply) string {
	switch r := r.(type) {
	case *packet.Connack:
		return fmt.Sprintf("CONNACK 0x%02X", r.ReasonCode)
	case *packet.Disconnect:
		return fmt.Sprintf("DISCONNECT 0x%02X", r.ReasonCode)
	case *connection.Ack:
		return fmt.Sprintf("acknowledgement of type %d, 0x%02X, for %d", r.Type, r.ReasonCode, r.PacketID)
	case *packet.Suback:
		return fmt.Sprintf("SUBACK for %d", r.PacketID)
	}
	return fmt.Sprintf("%T", r)
}

// showAll renders replies for messages.
func showAll(replies []connection.Reply) []string {
	shown := make([]string, len(replies))
	for i, r := range replies {
		shown[i] = show(r)
	}
	return shown
}
