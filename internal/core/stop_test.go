package core_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// These tests drive orderings no client can force (design.md, Testing): a
// client's DISCONNECT and the graceful stop, met in the core in one order
// or the other. From the wire, a client cannot tell when the core took its
// DISCONNECT, nor whether its reader read it before the writer stopped it
// (R2).

// stopCases are a durable MQTT 5 session's DISCONNECT changing its session
// expiry: to 0, which ends the session, and to another value, which keeps
// it away for that long.
var stopCases = []struct {
	name   string
	expiry uint32 // the DISCONNECT's Session Expiry Interval
}{
	{"to 0", 0},
	{"to 120 seconds", 120},
}

// connectExpiry is the Session Expiry Interval of the CONNECTs these tests
// make: an hour, which max_session_expiry is set to allow.
const connectExpiry = 3600

// stopScript returns a script whose core holds one connected durable MQTT 5
// session, client "x", of connectExpiry, and its connection.
func stopScript(t *testing.T) (*script, *connection.Conn) {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxSessionExpiry = connectExpiry * time.Second
	s := newScript(t, cfg)
	conn := s.connected(keeping(packet.V5, "x", connectExpiry))
	return s, conn
}

// departures returns the writes in acts that send a session away or end it.
func departures(acts []core.Action) []store.Write {
	var gone []store.Write
	for _, w := range writesIn(acts) {
		switch w.(type) {
		case *store.LeaveSession, *store.EndSession:
			gone = append(gone, w)
		}
	}
	return gone
}

// expectDeparture checks that acts hold exactly one departure of session
// id, carrying the DISCONNECT's expiry: an EndSession for 0, else a
// LeaveSession of that expiry; and that the store holds it so.
func expectDeparture(t *testing.T, s *script, acts []core.Action, id int64, expiry uint32) {
	t.Helper()
	gone := departures(acts)
	if len(gone) != 1 {
		t.Fatalf("the session's departures: %v; want one, carrying the DISCONNECT's Session Expiry Interval, %d", gone, expiry)
	}
	switch w := gone[0].(type) {
	case *store.EndSession:
		if expiry != 0 || w.ID != id {
			t.Fatalf("the session's departure ends session %d; want it sent away with the DISCONNECT's Session Expiry Interval, %d", w.ID, expiry)
		}
		if row, ok := s.rows["x"]; ok {
			t.Errorf("the session ended, and the store still holds %+v", row)
		}
	case *store.LeaveSession:
		if w.Expiry != expiry || w.ID != id {
			t.Fatalf("the session's departure: %+v; want session %d with the DISCONNECT's Session Expiry Interval, %d, not the CONNECT's, %d (0 ends it)",
				*w, id, expiry, connectExpiry)
		}
		if row := s.rows["x"]; row.Expiry != expiry || row.Left == 0 {
			t.Errorf("the store holds %+v; want the session away, its expiry %d", row, expiry)
		}
	}
}

// design.md, Restart, Graceful stop: main waits until every reader, writer
// and dialer has ended, "The core takes events until then. It acts on a
// client's DISCONNECT by storing its session expiry change"; DISCONNECT: "The
// client may change its session expiry [...] The change is stored before the
// connection closes"; MQTT 5 3.14.2.2.2: "If the Session Expiry Interval is
// absent, the Session Expiry Interval in the CONNECT packet is used", so
// present, the DISCONNECT's is used; design.md, Flows, Closing a connection:
// the reader "stops reading and says so before it reports its end to the
// core", and its end carries the DISCONNECT it read. Audit 261018-001a: a
// DISCONNECT taken after the stop began was ignored, the session leaving with
// its CONNECT's expiry. The stop begins while the session is connected; its
// client's end then arrives carrying a DISCONNECT that changes the expiry:
// the session's one departure carries the DISCONNECT's expiry.
func TestStopActsOnADisconnectItTakes(t *testing.T) {
	for _, tc := range stopCases {
		t.Run(tc.name, func(t *testing.T) {
			s, conn := stopScript(t)
			id := s.rows["x"].ID
			acts := s.stop()
			acts = append(acts, s.end(conn, disconnectWith(0, &tc.expiry), 0)...)
			for i, a := range acts {
				if g, ok := a.Write.(*core.Group); ok {
					acts[i].Write = core.CopyGroup(g)
				}
			}
			acts = append(acts, s.settle()...)
			expectDeparture(t, s, acts, id, tc.expiry)
			s.close(conn)
			s.expectRows()
		})
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken", and "It acts on a client's
// DISCONNECT"; MQTT 5 3.14.4: "After sending a DISCONNECT packet the sender
// [...] MUST close the Network Connection [MQTT-3.14.4-2]", and "On receipt
// of DISCONNECT, the receiver: SHOULD close the Network Connection"; audit
// 261018-01, Observation 3: a client whose DISCONNECT was handled just before
// the stop read 0x8B. The client's end, carrying a DISCONNECT that changes
// the expiry, is taken first, its departure still at the store when the stop
// begins: the connection was the client's to end, and it has ended it, so its
// final entry is not the stop's DISCONNECT 0x8B (Server shutting down), and
// the departure carries the DISCONNECT's expiry.
func TestStopSendsNo0x8BAfterAClientsDisconnect(t *testing.T) {
	for _, tc := range stopCases {
		t.Run(tc.name, func(t *testing.T) {
			s, conn := stopScript(t)
			id := s.rows["x"].ID
			acts := s.end(conn, disconnectWith(0, &tc.expiry), 0)
			if len(departures(acts)) != 1 || s.conns[conn].final {
				t.Fatalf("the DISCONNECT's end: departures %v, replies %v; want its departure at the store and the final entry waiting for it",
					departures(acts), showAll(repliesTo(acts, conn)))
			}
			acts = append(acts, s.stop()...)
			for i, a := range acts {
				if g, ok := a.Write.(*core.Group); ok {
					acts[i].Write = core.CopyGroup(g)
				}
			}
			acts = append(acts, s.settle()...)
			for _, r := range repliesTo(acts, conn) {
				if isDisconnect(r, 0x8B) {
					t.Fatalf("a client whose DISCONNECT the core took before the stop got DISCONNECT 0x8B; want its connection closed without it")
				}
			}
			if !s.conns[conn].final {
				t.Fatalf("no final entry once the departure was stored")
			}
			expectDeparture(t, s, acts, id, tc.expiry)
			s.close(conn)
			s.expectRows()
		})
	}
}

// R8: "A store error stops the broker, except a failed memory-mode snapshot";
// design.md, Restart: "A stop for a failure, whether a store error or a
// failed accept, follows the graceful stop", in which "Every client gets
// 0x8B, except one whose DISCONNECT the core has taken", main waits until
// every reader and writer has ended, "The core takes events until then", and
// "the core waits until every store request it sent, read or write, is
// answered". Failures is where the core reports the store's failure to main,
// and Drained where it says it has nothing left to wait for (shutdown.go).
// Audit 261018-02, Observation 2: the stop's failure branch had no test of
// its own.
//
// Three durable MQTT 5 sessions are connected. Two clients' DISCONNECTs,
// one ending its session and one sending it away, are taken: the first
// departure is at the store, the second waiting behind it (Store, Writes:
// "The core keeps at most one write transaction outstanding at the
// store"), their final entries waiting for them. The stop begins, and the
// store answers the first departure with an error other than full, then
// every request after it with the same error, as a failed store does. The
// core reports the failure once, every connection gets its final entry,
// the third client's end and every close report are taken, and no store
// request is left outstanding. Run, carrying on from
// the core so left, closes Drained without waiting for anything more. (The
// script's connections stand for a connection by identity only and cannot
// take a reply from Run: the decisions are scripted, and Run is given what
// they leave.)
func TestStopDrainsThroughAStoreFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, stays := stopScript(t)
		ends := s.connected(keeping(packet.V5, "y", connectExpiry))
		leaves := s.connected(keeping(packet.V5, "z", connectExpiry))
		zero, away := uint32(0), uint32(120)
		s.end(ends, disconnectWith(0, &zero), 0)
		s.end(leaves, disconnectWith(0, &away), 0)
		if len(s.outstanding) != 1 || s.conns[ends].final || s.conns[leaves].final {
			t.Fatalf("the DISCONNECTs: %d store requests outstanding, final entries sent %v and %v; "+
				"want the first departure at the store, the second behind it, and both final entries waiting for them",
				len(s.outstanding), s.conns[ends].final, s.conns[leaves].final)
		}
		s.stop()

		failure := errors.New("disk I/O error")
		s.reply(s.outstanding[0].op, failure)
		select {
		case err := <-s.core.Failures():
			if !errors.Is(err, failure) {
				t.Fatalf("the core reported %v, want the store's error, %v", err, failure)
			}
		default:
			t.Fatalf("a store error other than full, and no failure reported (R8: it stops the broker)")
		}
		failAll := func() {
			for len(s.outstanding) != 0 {
				s.reply(s.outstanding[0].op, failure)
			}
		}
		failAll()
		s.end(stays, nil, 0)
		failAll()
		for _, conn := range []*connection.Conn{stays, ends, leaves} {
			if !s.conns[conn].final {
				t.Fatalf("after the store's failure a connection has no final entry: its writer would never end")
			}
			s.close(conn)
		}
		select {
		case err := <-s.core.Failures():
			t.Fatalf("the failure reported a second time, %v: main is told once", err)
		default:
		}

		l := newLoop(t)
		l.run(s.core)
		l.decide(s.now) // the time of Run's first act
		select {
		case <-s.core.Drained():
		default:
			t.Fatalf("every connection closed and every store request answered, and Drained not closed: " +
				"the core waits for an answer that cannot come")
		}
	})
}

// cutsIn returns, in order, the connections acts cut.
func cutsIn(acts []core.Action) []*connection.Conn {
	var cut []*connection.Conn
	for _, a := range acts {
		if a.Cut {
			cut = append(cut, a.Conn)
		}
	}
	return cut
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken [...] After limits.write_timeout from
// when the core accepts the stop, the remaining sockets are cut [...] A
// connection whose DISCONNECT change is still at the store is the exception.
// It closes once the store confirms that change"; design.md, Flows, Closing a
// connection: "The core cuts a connection only by closing its raw socket";
// design.md, Execution, State ownership: the core owns "every timer except
// the following: - A connection owns its deadlines". Audit 261018-105a: the
// stop's write_timeout ran from each writer's taking of its final entry,
// which replies queued in front of it could put off. Three durable MQTT 5
// sessions are connected when the stop begins: one whose writer has not
// ended, one whose client's DISCONNECT is taken with its expiry change still
// at the store, and one that ends and reports its close before the deadline.
// The core's timer is set for write_timeout after the stop began; firing a
// moment before, it cuts nothing; firing then, it cuts the first alone.
func TestStopCutsWhatIsOpenWriteTimeoutAfterItBegan(t *testing.T) {
	s, open := stopScript(t)
	held := s.connected(keeping(packet.V5, "y", connectExpiry))
	closes := s.connected(keeping(packet.V5, "z", connectExpiry))
	away := uint32(120)
	s.end(held, disconnectWith(0, &away), 0)
	acts := s.stop()
	began := s.now
	for _, conn := range []*connection.Conn{open, closes} {
		if got := repliesTo(acts, conn); len(got) != 1 || !isDisconnect(got[0], 0x8B) {
			t.Fatalf("the stop sent a connected client %v, want DISCONNECT 0x8B", showAll(got))
		}
	}
	s.end(closes, nil, 0)
	s.close(closes)

	writeTimeout := s.cfg.Broker.Limits.WriteTimeout
	if got, want := s.core.Timer(), began.Add(writeTimeout); !got.Equal(want) {
		t.Fatalf("the core's timer after the stop began at %v is set for %v, want %v: write_timeout (%v) later",
			began, got, want, writeTimeout)
	}
	if cut := cutsIn(s.fireAt(began.Add(writeTimeout - time.Nanosecond))); len(cut) != 0 {
		t.Fatalf("a nanosecond before write_timeout after the stop began, the core cut %d connections, want none", len(cut))
	}
	if cut := cutsIn(s.fireAt(began.Add(writeTimeout))); len(cut) != 1 || cut[0] != open {
		t.Fatalf("write_timeout after the stop began, the core cut %d connections; want one, the one still open, "+
			"and not the one whose DISCONNECT's change is still at the store", len(cut))
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop whose
// changes the stop stores (R4). The core issues it at the end of the event
// that makes the last of those replies ready. It issues it at once if the
// store fails, or at the stop for a client with none"; design.md, Flows,
// Replies: PUBACK "sent only once the store confirms"; R8: "A store error
// stops the broker". Without an interval, p publishes at QoS 1, and its
// publication's group is at the store when the stop begins: the stop sends p
// nothing, its 0x8B waiting for its PUBACK. The store answers the group with
// an error other than full: that event gives p its final entry, DISCONNECT
// 0x8B, alone, without the PUBACK, which the store never confirmed.
func TestStoreFailureDuringTheStopSends0x8BAtOnce(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	s.publish(p, 1, 20)
	if got := repliesTo(s.stop(), p); len(got) != 0 {
		t.Fatalf("the stop, p's PUBLISH at the store, sent p %v; want nothing while its PUBACK waits for the store", showAll(got))
	}
	acts := s.reply(only[*core.Group](s), errors.New("disk I/O error"))
	if got := repliesTo(acts, p); len(got) != 1 || !isDisconnect(got[0], 0x8B) {
		t.Fatalf("the store's failure during the stop sent p %v; want DISCONNECT 0x8B alone, no PUBACK", showAll(got))
	}
}

// design.md, Restart, Graceful stop: main waits until every reader, writer
// and dialer has ended, "The core takes events until then. It acts on a
// client's DISCONNECT by storing its session expiry change and keeping or
// discarding its Will as the DISCONNECT says. Any other packet it takes is
// neither stored nor answered"; "Every client gets 0x8B, except one whose
// DISCONNECT the core has taken. The 0x8B follows the PUBACK [...] of packets
// taken before the stop whose changes the stop stores (R4)"; design.md,
// Flows, Closing a connection: the writer, taking its final entry, "stops the
// reader", which reads on until then.
//
// Without an interval, p publishes at QoS 1: its publication's group is at
// the store (outstanding) when the stop begins, so p's 0x8B is held for its
// PUBACK and p stays open, awaiting that PUBACK — the stop sends it no final
// entry. While so open, p's reader hands over a second PUBLISH at QoS 1,
// taken after the stop, its body's bytes given before the stop (design.md,
// Message memory: "Once a stop begins or the store fails, the core grants
// no more"). Settling every store request, p's replies are
// exactly its first publication's PUBACK (0x00) then its DISCONNECT 0x8B:
// the second PUBLISH is neither stored (no write from the stop on holds a
// record) nor answered (no reply beyond those two, none for its identifier).
func TestPublishTakenAfterTheStopIsNeitherStoredNorAnswered(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	p := s.connected(connectOf(packet.V5, "p"))
	s.publish(p, 1, 20) // dispatched and outstanding: p awaits its PUBACK
	if got := s.ask(p, 20); got != 20 {
		t.Fatalf("p asked for its second body's 20 bytes before the stop and was given %d", got)
	}
	if got := repliesTo(s.stop(), p); len(got) != 0 {
		t.Fatalf("the stop sent p %v while its PUBACK waits for the store; want p still open, awaiting it, no final entry", showAll(got))
	}
	acts := s.handOver(p, 1, 2, 20)
	acts = append(acts, s.settle()...)
	for _, w := range writesIn(acts) {
		if n := w.Records(); n != 0 {
			t.Fatalf("a write sent from the stop on holds %d records: the PUBLISH taken after the stop was stored", n)
		}
	}
	got := repliesTo(acts, p)
	if len(got) != 2 || !isPuback(got[0], 1, 0) || !isDisconnect(got[1], 0x8B) {
		t.Fatalf("after the stop p got %v; want only its first PUBACK (0x00) then DISCONNECT 0x8B: the PUBLISH taken after the stop is neither stored nor answered",
			showAll(got))
	}
}
