package core_test

import (
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Store, Requests: "Each dispatched read and each dispatched
// write group consumes one store credit until its answer is processed";
// "Answers return on the bounded answer channel, always drained. Each
// session, consumer, shared group and channel keeps at most one request of
// its own in flight, plus bounded pending work"; 0007a.md, 261009 211151:
// "tests drive every path that leaves a request outstanding (connection
// lost, takeover, session end) and check each credit comes back exactly
// once, never lost and never twice"; design.md, Flows, Replies: "Such a reply
// goes only on the connection that asked. If the store confirms after that
// connection has ended or been taken over, what was stored stays and the
// reply is dropped". After every decision the script checks that the core's
// credit is store.Credit less the requests outstanding (decided), that no
// session has two requests outstanding (request), and that nothing but its
// final entry goes to a connection after its Ended (check). Each path leaves
// a request outstanding as a connection ends, is taken over or its session
// ends, and checks what follows; once the store has answered everything, the
// core has its whole credit back and the store holds what a restart restores
// (expectRows).
func TestEveryCreditComesBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		path func(t *testing.T, s *script)
	}{
		{"lost while its session is read", lostWhileRead},
		{"lost while its session is stored", lostWhileStored},
		{"a new expiry while its session is stored", markWhileStored},
		{"taken over while its session is stored", takenOverWhileStored},
		{"a CONNECT while the session goes away", connectWhileAway},
		{"a CONNECT while the session ends", connectWhileEnding},
		{"closed while its session goes away", closedWhileAway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, newConfig(t))
			tc.path(t, s)
			s.settle()
			if got := s.core.Credit(); got != store.Credit {
				t.Errorf("Credit() = %d with every request answered, want %d", got, store.Credit)
			}
			s.expectRows()
		})
	}
}

// lostWhileRead loses a connection while its CONNECT's session is read from
// the store: the CONNACK, which the store's answer would have allowed, is
// dropped (check), and the connection gets its final entry.
func lostWhileRead(t *testing.T, s *script) {
	conn := s.accept()
	s.receive(conn, keeping(packet.V5, "x", 60), 0)
	s.end(conn, nil, 0)
	s.settle()
	if !s.conns[conn].final {
		t.Errorf("no final entry once the store answered")
	}
}

// lostWhileStored loses a connection while its CONNECT's new durable
// session is stored: the CONNACK is dropped (check), and what was stored
// stays, the session going away (R4; design.md, CONNECT: "A connection
// dropped before CONNACK is an ordinary disconnect").
func lostWhileStored(t *testing.T, s *script) {
	conn := s.accept()
	s.receive(conn, keeping(packet.V5, "x", 60), 0)
	s.answer(only[*store.LoadSession](s))
	s.end(conn, nil, 0)
	s.settle()
	if row, ok := s.rows["x"]; !ok || row.Left == 0 || row.Expiry != 60 {
		t.Errorf("the session is stored as %+v (%v), want it away, its expiry 60", row, ok)
	}
}

// markWhileStored has a client's DISCONNECT change its new session's
// expiry while the CONNECT's write is outstanding: the change is a mark,
// sent as the session going away when that write's answer arrives, with
// the expiry and the time of the Ended (design.md, Store, Requests: "Each
// session, consumer, shared group and channel keeps at most one request of
// its own in flight, plus bounded pending work"; "A dependent request
// becomes eligible after its predecessor's result is applied"; storage.md: left is "when its
// connection ended").
func markWhileStored(t *testing.T, s *script) {
	seconds := uint32(120)
	conn := s.accept()
	s.receive(conn, keeping(packet.V5, "x", 60), 0)
	s.answer(only[*store.LoadSession](s))
	save := only[store.Write](s)
	if acts := s.end(conn, disconnectWith(0, &seconds), 0); len(writesIn(acts)) != 0 {
		t.Fatalf("the DISCONNECT wrote %v while the session's write is outstanding, want a mark", writesIn(acts))
	}
	left := s.now.UnixMilli()
	acts := s.answer(save)
	want := &store.LeaveSession{ID: s.rows["x"].ID, Expiry: seconds, Left: left}
	if got := writesIn(acts); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Errorf("the write's answer wrote %v, want the mark, %+v", got, want)
	}
}

// takenOverWhileStored has a second CONNECT for a client id arrive while
// the first's new session is stored: it waits (design.md, CONNECT, 3); the
// first is answered once stored, then taken over (DISCONNECT 0x8E), and the
// second resumes the session, Session Present 1.
func takenOverWhileStored(t *testing.T, s *script) {
	first, second := s.accept(), s.accept()
	s.receive(first, keeping(packet.V5, "x", 60), 0)
	s.answer(only[*store.LoadSession](s))
	s.receive(second, keeping(packet.V5, "x", 60), 0)
	acts := s.settle()
	if got := repliesTo(acts, first); len(got) != 2 || !isDisconnect(got[1], 0x8E) {
		t.Errorf("the first connection got %v, want its CONNACK, then DISCONNECT 0x8E", showAll(got))
	}
	if ack := connackIn(acts, second); ack == nil || !ack.SessionPresent {
		t.Errorf("the second CONNECT got %v, want Session Present 1", showAll(repliesTo(acts, second)))
	}
	s.end(first, nil, 0)
}

// connectWhileAway has a CONNECT for a client id arrive while its session's
// going away is outstanding: it waits for that answer (R2), then resumes
// the session, Session Present 1.
func connectWhileAway(t *testing.T, s *script) {
	seconds := uint32(120)
	old := s.connected(keeping(packet.V5, "x", 60))
	s.end(old, disconnectWith(0, &seconds), 0)
	conn := s.accept()
	if acts := s.receive(conn, keeping(packet.V5, "x", 60), 0); len(acts) != 0 {
		t.Fatalf("the CONNECT asked %d things while its session's going away is outstanding, want none", len(acts))
	}
	if ack := connackIn(s.settle(), conn); ack == nil || !ack.SessionPresent {
		t.Errorf("CONNACK %+v, want Session Present 1", ack)
	}
}

// connectWhileEnding has a CONNECT for a client id arrive while its
// session's ending is outstanding (a DISCONNECT changing the expiry to 0):
// it waits for that answer (R2), then finds no session, Session Present 0.
func connectWhileEnding(t *testing.T, s *script) {
	var zero uint32
	old := s.connected(keeping(packet.V5, "x", 60))
	s.end(old, disconnectWith(0, &zero), 0)
	conn := s.accept()
	if acts := s.receive(conn, keeping(packet.V5, "x", 60), 0); len(acts) != 0 {
		t.Fatalf("the CONNECT asked %d things while its session's ending is outstanding, want none", len(acts))
	}
	if ack := connackIn(s.settle(), conn); ack == nil || ack.SessionPresent {
		t.Errorf("CONNACK %+v, want Session Present 0", ack)
	}
	s.expectEnded(0, 0, 1)
}

// closedWhileAway has a lost connection report its close while its
// session's going away is outstanding, where the core sent the final entry
// at once: the answer then asks nothing of the closed connection (check).
func closedWhileAway(_ *testing.T, s *script) {
	conn := s.connected(keeping(packet.V5, "x", 60))
	s.end(conn, nil, 0)
	if s.conns[conn].final {
		s.close(conn)
	}
	s.settle()
	if !s.conns[conn].closed {
		s.close(conn)
	}
}

// design.md, Store, Requests: "Each dispatched read and each dispatched
// write group consumes one store credit until its answer is processed; a
// group's members consume no separate credits"; 0007d.md, 261016 093545:
// "each ending is one EndSession". With every credit but one spent on
// CONNECTs' reads, the timer's read takes the last; its answer, three
// sessions due, gives one credit back, which the group of the three endings
// takes, its members none, leaving no credit. Once the CONNECTs' reads are
// answered, that group alone is outstanding, and the core's credit is
// store.Credit less one. The three are ended, each once (the script's store
// refuses ending a session twice), and counted expired.
func TestGroupTakesOneCredit(t *testing.T) {
	rows := make([]store.Session, 0, 3)
	for id := range int64(3) {
		rows = append(rows, store.Session{ID: id + 1, Client: "due" + strconv.FormatInt(id, 10), Protocol: 5, Expiry: 1, Left: epoch.UnixMilli() - 1000, ReceiveMaximum: 20})
	}
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 3}, rows...)
	s.fire()
	for i := range store.Credit - 1 {
		s.receive(s.accept(), connectOf(packet.V5, "c"+strconv.Itoa(i)), 0)
	}
	if got := s.core.Credit(); got != 0 {
		t.Fatalf("Credit() = %d, want 0", got)
	}
	acts := s.answer(only[*store.DueSessions](s))
	if got := groupsIn(acts); len(got) != 1 || !slices.Equal(s.members(got[0]), []string{"end due0", "end due1", "end due2"}) {
		t.Fatalf("the due read's answer, with one credit back, wrote %v; want one group of the three endings", writesIn(acts))
	}
	if got := s.core.Credit(); got != 0 {
		t.Fatalf("Credit() = %d with the group of three endings sent on the one credit back, want 0", got)
	}
	for _, load := range pending[*store.LoadSession](s) {
		s.answer(load)
	}
	if got, want := s.core.Credit(), store.Credit-1; len(s.outstanding) != 1 || got != want {
		t.Fatalf("Credit() = %d with %d requests outstanding, want %d with the group of three endings alone", got, len(s.outstanding), want)
	}
	s.settle()
	if len(s.rows) != 0 {
		t.Errorf("the store holds %v, want every due session ended", s.rows)
	}
	s.expectEnded(0, 3, 0)
}

// metrics.md, Store and memory: saguin_store_no_credit_seconds_total, "time
// the core held no store credit and took no packets (R2). Includes
// completed intervals only; an ongoing interval is added when it ends".
// design.md, Execution, Communication: "The core does not take from the
// channel while it has no store credit"; Store, Requests: "Each dispatched
// read [...] consumes one store credit until its answer is processed";
// Testing: an inside test checks "orderings no client can force", the
// core driven "with scripted events and fake times": no client can hold
// 1,024 of the store's answers back.
//
// store.Credit CONNECTs, each a read of its session, take every credit,
// the last at the time of its event. Events come 3 seconds apart; while no
// credit comes back, the counter stays 0, the interval ongoing. The first
// read's answer gives one back: the interval ends, and the counter holds
// its 3 seconds, from the event that took the last credit to that answer's.
func TestNoCreditTimeIsCounted(t *testing.T) {
	const series = "saguin_store_no_credit_seconds_total"
	s := newScript(t, newConfig(t))
	for i := range store.Credit {
		s.receive(s.accept(), connectOf(packet.V5, "c"+strconv.Itoa(i)), 0)
	}
	if got := s.core.Credit(); got != 0 {
		t.Fatalf("Credit() = %d after %d CONNECTs' reads, want 0", got, store.Credit)
	}
	ranOut := s.now
	s.tick = 3 * time.Second
	if got := testkit.Sample(t, s.reg, series, testkit.Typed("counter")); got != 0 {
		t.Fatalf("%s = %v with no credit since the last event, the interval ongoing; want 0", series, got)
	}
	s.answer(pending[*store.LoadSession](s)[0])
	if got := s.core.Credit(); got != 1 {
		t.Fatalf("Credit() = %d after one read's answer, want 1", got)
	}
	if got, want := testkit.Sample(t, s.reg, series), s.now.Sub(ranOut).Seconds(); got != want {
		t.Errorf("%s = %v once one credit came back, want %v", series, got, want)
	}
}
