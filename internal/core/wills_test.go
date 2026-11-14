package core_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// willTopic is the topic of the delayed Will the client gone leaves, which
// the client watcher subscribes to.
const willTopic = "wills/gone"

// goneDelay and goneExpiry are the client gone's Will Delay Interval and
// session expiry, in seconds: the expiry the longer, so that its Will goes
// at its delay, not at its session's end.
const goneDelay, goneExpiry = 2, 3

// willScript is a live script (retention_test.go) in which a client, gone,
// has departed at left, its connection lost, and a client, watcher, is
// subscribed to willTopic.
type willScript struct {
	*liveScript
	watcher *connection.Conn
	left    time.Time
}

// departAt starts a live script, connects watcher, subscribed to
// willTopic, then gone, Clean Start 1, its session expiry goneExpiry, with,
// where will is set, a QoS 1 Will on willTopic of Will Delay goneDelay; it
// fires every timer the core sets before left, then hands the core gone's
// connection's end, lost, at left, and its close report, serving what each
// leads to.
func departAt(t *testing.T, left time.Time, will bool) *willScript {
	t.Helper()
	s := &willScript{liveScript: newLiveScript(t, ""), left: left}
	s.watcher = s.connect("watcher", 0, 10)
	s.subscribe(s.watcher, willTopic)

	p := &packet.Connect{Version: packet.V5, CleanStart: true, KeepAlive: 60, ClientID: "gone",
		Properties: connectProperties(expiryProperty(packet.V5, goneExpiry))}
	if will {
		wire, err := packet.AppendWillProperties(nil, []packet.Property{{ID: packet.WillDelayInterval, Int: goneDelay}})
		if err != nil {
			t.Fatal(err)
		}
		properties, _, err := packet.DecodeWillProperties(wire)
		if err != nil {
			t.Fatal(err)
		}
		p.WillFlag, p.Will = true, packet.Will{QoS: 1, Topic: willTopic, Payload: []byte("gone"), Properties: properties}
	}
	gone := new(connection.Conn)
	s.step(connection.Event{Kind: connection.Accepted, Conn: gone,
		Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 2, 1), Port: 41000}}})
	s.step(connection.Event{Kind: connection.Received, Conn: gone, Packet: decodedConnect(p)})
	if ack := connackIn(s.decisions(gone), gone); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("gone's CONNECT answered with %v; want CONNACK 0x00", showAll(s.replies[gone]))
	}

	s.advance(left.Add(-eventGap))
	if !s.now.Before(left) {
		t.Fatalf("the script's clock is at %s after the connections; want it before the departure, %s", clockOf(s.now), clockOf(left))
	}
	s.now = left
	s.take(s.core.Handle(nil, connection.Event{Kind: connection.Ended, Conn: gone}, s.now))
	s.serve()
	s.step(connection.Event{Kind: connection.Closed, Conn: gone})
	if n := s.count(`SELECT count(*) FROM sessions WHERE client = 'gone'`); n != 1 {
		t.Fatalf("gone's session has %d rows once its connection closed; want it stored away", n)
	}
	return s
}

// clockOf renders at in UTC to the microsecond, for messages.
func clockOf(at time.Time) string { return at.UTC().Format("15:04:05.000000") }

// count is the count query returns, as the database holds it.
func (s *liveScript) count(query string) int {
	s.t.Helper()
	var n int
	if err := s.db.QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		s.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// willPublished says how the Will on willTopic has been published, "" for
// not at all: a group writing its record committed, the database holding
// that record, or a delivery of it handed to watcher's writer.
func (s *willScript) willPublished() string {
	s.t.Helper()
	for _, g := range s.groups {
		for _, w := range g.Writes {
			records := core.RecordsOf(w)
			for i := range records {
				if records[i].Topic == willTopic {
					return "a group committed its record"
				}
			}
		}
	}
	if n := s.count(`SELECT count(*) FROM records WHERE topic = '` + willTopic + `'`); n != 0 {
		return fmt.Sprintf("the store holds %d records of it", n)
	}
	for _, r := range s.replies[s.watcher] {
		if d, ok := r.(*connection.Delivery); ok && string(d.Topic) == willTopic {
			return "watcher was handed a delivery of it"
		}
	}
	return ""
}

// fireBefore fires the core's timer at each time it asks for before end,
// as the loop does, each fire's store requests and deliveries carried out
// at that fire's own time (serveStill), and calls check after each fire
// with its time; it returns the first time the timer asks for at or after
// end, failing the test where the core asks for none.
func (s *liveScript) fireBefore(end time.Time, check func(at time.Time)) time.Time {
	s.t.Helper()
	for fires := 0; ; fires++ {
		at := s.core.Timer()
		switch {
		case at.IsZero():
			s.t.Fatalf("the core asks for no timer, a deadline at %s still to come", clockOf(end))
		case !at.Before(end):
			return at
		case fires == answerLimit:
			s.t.Fatalf("watchdog: the core's timer still comes due at %s after %d fires, before %s", clockOf(at), answerLimit, clockOf(end))
		}
		s.fireStill(later(at, s.now))
		check(s.now)
	}
}

// fireStill hands the core its timer at at, then serves what it leads to,
// every answer at at (serveStill).
func (s *liveScript) fireStill(at time.Time) {
	s.t.Helper()
	s.now = at
	s.take(s.core.Fire(nil, s.now))
	s.serveStill()
}

// serveStill serves as serve does, every request decided carried out by
// the store, oldest first, and every delivery handed to a writer reported
// written, but hands the core each answer and report at the script's
// present time: what a fire leads to is decided at that fire's time, never
// at a later one the script's own pace would bring.
func (s *liveScript) serveStill() {
	s.t.Helper()
	for answers := 1; len(s.queue) > 0 || len(s.writing) > 0; answers++ {
		if answers > liveLimit {
			s.t.Fatalf("watchdog: the core still asks after %d answers", liveLimit)
		}
		if len(s.queue) == 0 {
			a := s.writing[0]
			s.writing = s.writing[1:]
			s.take(s.core.Handle(nil, writerReport(a.Conn, a.Reply, connection.DeliveryWritten), s.now))
			continue
		}
		op := s.queue[0]
		s.queue = s.queue[1:]
		switch op := op.(type) {
		case store.Read:
			testkit.Send(s.t, s.st.Reads(), op, storeWait)
			for {
				a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a read's answer")
				if a.Op != op {
					s.t.Fatalf("the store answered %T %p; want %T %p, the one read outstanding", a.Op, a.Op, op, op)
				}
				s.take(s.core.Answer(nil, a, s.now))
				if !a.View {
					break
				}
			}
		case *core.Group:
			testkit.Send(s.t, s.st.Writes(), store.Write(op), storeWait)
			a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a group's answer")
			if a.Op != op {
				s.t.Fatalf("the store answered %T %p; want group %p", a.Op, a.Op, op)
			}
			if a.Err == nil {
				s.groups = append(s.groups, op)
			}
			s.take(s.core.Answer(nil, a, s.now))
		case store.Write:
			testkit.Send(s.t, s.st.Writes(), op, storeWait)
			a := testkit.Recv(s.t, s.st.Answers(), storeWait, "a write's answer")
			if a.Op != op {
				s.t.Fatalf("the store answered %T %p; want %T %p, the one write outstanding", a.Op, a.Op, op, op)
			}
			s.take(s.core.Answer(nil, a, s.now))
		default:
			s.t.Fatalf("the core sent %T, neither a read nor a write", op)
		}
	}
}

// design.md, DISCONNECT: "The Will goes after its Will Delay"; "A
// reconnect in time cancels it"; "A delayed Will whose session outlives its
// connection is timed from the saved departure"; "The session goes away
// until its expiry"; "When the expiry runs out, the delayed Will is
// published and the session deleted in one transaction". CONNECT, 5: "A
// delayed Will is cancelled only by reconnection to the still-existing
// session before its due time, judged when the core takes the CONNECT. A
// CONNECT taken at or after the due time is too late"; 6: "Keeping the
// session resumes it and cancels the delayed Will". MQTT 5 3.1.2.5: "The
// Will Message MUST be published after the Network Connection is
// subsequently closed and either the Will Delay Interval has elapsed or the
// Session ends, unless [...] a new Network Connection for the ClientID is
// opened before the Will Delay Interval has elapsed [MQTT-3.1.2-8]"; 3.1.3.2:
// "The Server delays publishing the Client's Will Message until the Will
// Delay Interval has passed or the Session ends, whichever happens first. If
// a new Network Connection to this Session is made before the Will Delay
// Interval has passed, the Server MUST NOT send the Will Message
// [MQTT-3.1.3-9]"; 3.1.2.11.2: "The Client and Server MUST store the
// Session State after the Network Connection is closed if the Session
// Expiry Interval is greater than 0 [MQTT-3.1.2-23]". design.md, Testing:
// "The core never reads the clock [...] An inside test therefore drives it
// with scripted events and fake times"; where a departure falls within a
// millisecond is an input no client can force.
//
// gone's connection ends at a time a part of a millisecond past one: 0,
// 1 µs, 700 µs or 999 µs. Its Will, delayed goneDelay seconds, is not
// published, by any timer the core asks for before left + goneDelay, nor by
// what that timer leads to, and is published by the first timer at or after
// it. A session of expiry goneExpiry, without a Will, is not ended before
// left + goneExpiry and is ended by the first timer at or after it. A
// CONNECT keeping the session, taken at left + goneDelay - 1 µs, is in time:
// the Will is never published.
func TestDelayedWillIsTimedFromTheDeparture(t *testing.T) {
	for _, past := range []time.Duration{0, time.Microsecond, 700 * time.Microsecond, 999 * time.Microsecond} {
		left := epoch.Add(time.Minute + past)
		due := left.Add(goneDelay * time.Second)
		ends := left.Add(goneExpiry * time.Second)

		t.Run(fmt.Sprintf("the Will, %v past a millisecond", past), func(t *testing.T) {
			s := departAt(t, left, true)
			first := s.fireBefore(due, func(at time.Time) {
				if how := s.willPublished(); how != "" {
					t.Fatalf("the Will is published by the timer at %s (%s), %v before its Will Delay has passed, at %s",
						clockOf(at), how, due.Sub(at), clockOf(due))
				}
			})
			s.fireStill(first)
			if s.willPublished() == "" {
				t.Fatalf("the Will is not published by the first timer at or after its Will Delay has passed, at %s (due %s)", clockOf(first), clockOf(due))
			}
			if n := s.count(`SELECT count(*) FROM wills WHERE client = 'gone'`); n != 0 {
				t.Errorf("the published Will still has %d rows; want it removed with its publication", n)
			}
		})

		t.Run(fmt.Sprintf("the session's expiry, %v past a millisecond", past), func(t *testing.T) {
			s := departAt(t, left, false)
			first := s.fireBefore(ends, func(at time.Time) {
				if n := s.count(`SELECT count(*) FROM sessions WHERE client = 'gone'`); n != 1 {
					t.Fatalf("the session is ended by the timer at %s, %v before its expiry runs out, at %s", clockOf(at), ends.Sub(at), clockOf(ends))
				}
			})
			s.fireStill(first)
			if n := s.count(`SELECT count(*) FROM sessions WHERE client = 'gone'`); n != 0 {
				t.Fatalf("the session is not ended by the first timer at or after its expiry, at %s (expiry %s)", clockOf(first), clockOf(ends))
			}
		})

		t.Run(fmt.Sprintf("a reconnect, %v past a millisecond", past), func(t *testing.T) {
			s := departAt(t, left, true)
			back := due.Add(-time.Microsecond)
			s.fireBefore(back, func(at time.Time) {
				if how := s.willPublished(); how != "" {
					t.Fatalf("the Will is published by the timer at %s (%s), %v before its Will Delay has passed, at %s",
						clockOf(at), how, due.Sub(at), clockOf(due))
				}
			})
			conn := new(connection.Conn)
			s.now = back
			s.take(s.core.Handle(nil, connection.Event{Kind: connection.Accepted, Conn: conn,
				Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 2, 2), Port: 41001}}}, s.now))
			s.serveStill()
			s.take(s.core.Handle(nil, connection.Event{Kind: connection.Received, Conn: conn, Packet: keeping(packet.V5, "gone", goneExpiry)}, s.now))
			s.serveStill()
			if ack := connackIn(s.decisions(conn), conn); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
				t.Fatalf("the CONNECT keeping gone's session, taken at %s, 1 µs before its Will is due, got %v; want CONNACK 0x00, Session Present 1",
					clockOf(back), showAll(s.replies[conn]))
			}
			s.advance(ends.Add(time.Second))
			if how := s.willPublished(); how != "" {
				t.Errorf("the Will is published (%s) though its client reconnected at %s, 1 µs before its Will Delay passed; want it cancelled", how, clockOf(back))
			}
			if n := s.count(`SELECT count(*) FROM wills WHERE client = 'gone'`); n != 0 {
				t.Errorf("the cancelled Will still has %d rows; want it removed", n)
			}
		})
	}
}
