package broker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// subscribeRaw subscribes a raw client to filter at QoS 1 and reads the SUBACK.
func (c *rawClient) subscribeRaw(filter string, id uint16) {
	c.t.Helper()
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: id,
		Filters: packets.Subscriptions{{Filter: filter, Qos: 1}}})
	if pk := c.read(); pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) != 1 || pk.ReasonCodes[0] != 1 {
		c.t.Fatalf("subscribe %s: %+v", filter, pk)
	}
}

// MQTT-4.4.0-1 and MQTT-3.3.1-1: a delivery with an empty payload - which MQTT
// allows, and which a latest channel's deletion always is - is a message like
// any other, so a session resumed after a link cut is sent it again under the
// identifier it carried, with DUP set, whichever path first sent it: the
// broadcast drain, a latest channel's pump, an append channel's pump. None of
// them is a delivery the session store keeps, so its payload was never the
// store's to give back.
func TestAnEmptyDeliveryIsSentAgainAfterALinkCut(t *testing.T) {
	for _, c := range []struct {
		name, filter string
		// deliver makes the empty delivery reach the client, reading and
		// acknowledging anything before it, and answers what it read.
		deliver func(t *testing.T, h *brokertest.Harness, lg drainLog, raw *rawClient) sent
	}{
		{"a broadcast from the log", "news/#", func(t *testing.T, h *brokertest.Harness, lg drainLog, raw *rawClient) sent {
			h.B.AttachBroadcast("raw")
			owe(t, h, lg, store.Record{Topic: "news/empty", QoS: 1}, "raw")
			return sentOf(raw.read())
		}},
		{"a latest channel's deletion", "state/#", func(t *testing.T, h *brokertest.Harness, lg drainLog, raw *rawClient) sent {
			p := connect(t, h, "producer", true, false)
			p.Pub(t, "state/a", "on")
			if pk := raw.read(); string(pk.Payload) != "on" {
				t.Fatalf("the value arrived as %+v", sentOf(pk))
			} else {
				raw.ack(pk)
			}
			p.Pub(t, "state/a", "")
			return sentOf(raw.read())
		}},
		{"an append channel's record", "events/#", func(t *testing.T, h *brokertest.Harness, lg drainLog, raw *rawClient) sent {
			p := connect(t, h, "producer", true, false)
			p.Pub(t, "events/a", "")
			return sentOf(raw.read())
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, lg, _ := drainHarness(t)
			raw, _ := connectRaw(t, h, "raw", 10)
			raw.subscribeRaw(c.filter, 1)
			first := c.deliver(t, h, lg, raw)
			if first.typ != packets.Publish || first.payload != "" || first.id == 0 {
				t.Fatalf("the delivery was %+v, want an empty QoS 1 PUBLISH", first)
			}
			_ = raw.conn.Close()

			back, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not resume")
			}
			// Whatever else a resume sends, it sends this one again.
			var got []sent
			for len(got) < 5 {
				_ = back.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				fb, body, err := brokertest.ReadRawPacket(back.conn)
				if err != nil {
					break
				}
				pk := packets.Packet{ProtocolVersion: 5}
				if err := pk.FixedHeader.Decode(fb); err != nil || pk.FixedHeader.Type != packets.Publish {
					continue
				}
				if err := pk.PublishDecode(body); err != nil {
					t.Fatalf("decode: %v", err)
				}
				s := sentOf(pk)
				got = append(got, s)
				if s.id == first.id {
					if !s.dup || s.payload != "" {
						t.Fatalf("it came again as %+v, want it empty with DUP set", s)
					}
					return
				}
			}
			t.Fatalf("the empty delivery under identifier %d was not sent again; after the resume came %+v", first.id, got)
		})
	}
}

// holdsReads is a broadcast log that, once armed, holds the drain's next read
// of what it picked until it is let go.
type holdsReads struct {
	drainReads
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *holdsReads) ReadAt(offsets ...uint64) ([]store.Record, error) {
	if l.armed.CompareAndSwap(true, false) {
		close(l.entered)
		<-l.release
	}
	return l.drainReads.ReadAt(offsets...)
}

func (l *holdsReads) let() { l.once.Do(func() { close(l.release) }) }

// takenOver waits until a connection other than old is registered for the
// client id: the takeover, the copy of old's in-flight table with it, is done.
func takenOver(t *testing.T, h *brokertest.Harness, id string, old *mqtt.Client) {
	t.Helper()
	eventually(t, "the takeover done", 5*time.Second, func() bool {
		now, ok := h.Srv.Clients.Get(id)
		return ok && now != old
	})
}

// releasesAtTakeover is an engine hook that, once armed, lets a held batch
// go as the next connection for the client id is established, just before
// the engine takes the old one over and copies its in-flight table: so the
// batch's registering races the copy.
type releasesAtTakeover struct {
	mqtt.HookBase
	client string
	armed  atomic.Bool
	let    func()
}

func (h *releasesAtTakeover) ID() string { return "releases-at-takeover" }
func (h *releasesAtTakeover) Provides(b byte) bool {
	return b == mqtt.OnSessionEstablish
}
func (h *releasesAtTakeover) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) error {
	if cl.ID == h.client && h.armed.CompareAndSwap(true, false) {
		h.let()
	}
	return nil
}

// MQTT-4.4.0-1 and RFC 0003 "Broadcast": a session taken over while its
// drain is part-way through a batch loses nothing. The takeover moves the
// session's subscriptions to the new connection and empties the old one's,
// closing it first, so a batch that reads them after the takeover finds none:
// what it decided from them must not be let go. The batch is held reading
// the log until the takeover is done, then let go; the message is sent on the
// new connection by the drain the resume woke, with nothing further to wake
// it.
func TestATakeoverWhileADrainReadsLosesNothing(t *testing.T) {
	h := start(t)
	lg := new(holdsReads)
	lg.entered, lg.release = make(chan struct{}), make(chan struct{})
	t.Cleanup(lg.let)
	log := attachDrainWith(t, h, func(l drainReads) drainReads {
		lg.drainReads = l
		return lg
	}, func(s drainSessions) drainSessions { return s })
	dialRaw(t, h, "raw", "news/#")
	old, _ := h.Srv.Clients.Get("raw")

	lg.armed.Store(true)
	m := owe(t, h, log, news("a", "through a takeover"), "raw")
	select {
	case <-lg.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no batch read the log, so this proves nothing")
	}
	back, present := connectRaw(t, h, "raw", 10)
	if !present {
		t.Fatal("the session did not resume")
	}
	takenOver(t, h, "raw", old)
	lg.let()

	pk := back.read()
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != string(m.Payload) {
		t.Fatalf("the new connection was sent %+v, want the message the old one's batch was reading", sentOf(pk))
	}
	back.ack(pk)
	if !back.quiet(300 * time.Millisecond) {
		t.Fatal("the connection ended")
	}
	eventually(t, "the message acknowledged and let go", 5*time.Second, func() bool {
		held, _ := log.ReadAt(m.Offset)
		return len(held) == 0 && len(tableOf(t, "raw")) == 0
	})
}

// MQTT-4.4.0-1, MQTT-4.3.3 and RFC 0003 "Broadcast": a session taken over
// while its drain is recording a batch in the in-flight table loses nothing
// and is sent nothing twice, at QoS 1 and at QoS 2 - where a second copy
// under another identifier is a second delivery. The batch is held at its
// record while the session is taken over:
//   - let go once the takeover is done, so the old connection's batch finds
//     it gone and is given up, and the drain the resume woke sends it;
//   - let go as the new connection is established, just before the takeover
//     copies the old connection's table, so its registering races the copy:
//     one registered before is carried and sent again by the engine, one
//     after is not registered at all, and either way it goes out once.
//
// A second message sent while the first is unacknowledged carries another
// identifier: none is reused while in flight. And with Clean Start 1 the
// session ends instead, and its ending waits for the record under way: the
// record is let go once the drain has let go of the session's list. Nothing
// of the old session is owed, left in the log, or left in the in-flight table
// or the cursor of the session that replaced it, and that session is sent
// nothing of the old - and is owed, and sent, the next broadcast.
func TestATakeoverWhileADrainRecordsSendsEachMessageOnce(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		for _, when := range []string{"after the takeover", "racing the takeover", "a clean start"} {
			t.Run(fmt.Sprintf("QoS %d/%s", qos, when), func(t *testing.T) {
				h := start(t)
				st := newGatesWrites()
				t.Cleanup(st.let)
				lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
					st.drainSessions = s
					return st
				})
				race := &releasesAtTakeover{client: "raw", let: st.let}
				if err := h.Srv.AddHook(race, nil); err != nil {
					t.Fatal(err)
				}
				dialRaw(t, h, "raw", "news/#")
				old, _ := h.Srv.Clients.Get("raw")

				st.armed.Store(true)
				r := news("a", "first")
				r.QoS = qos
				first := owe(t, h, lg, r, "raw")
				st.await(t)
				race.armed.Store(when == "racing the takeover")
				list := h.B.BroadcastList("raw")
				back, present := connectRawAs(t, h, "raw", 10, when == "a clean start")
				if when == "a clean start" {
					eventually(t, "the drain let go of the ended session's list", 5*time.Second, func() bool {
						return h.B.BroadcastList("raw") != list
					})
					st.let()
				}
				takenOver(t, h, "raw", old)
				st.let()

				if when == "a clean start" {
					if present {
						t.Fatal("a clean start was told its session was present")
					}
					eventually(t, "the ended session's message out of the log", 5*time.Second, func() bool {
						held, _ := lg.ReadAt(first.Offset)
						return len(held) == 0
					})
					if owed, _ := owedOf(h, "raw"); len(owed) != 0 {
						t.Errorf("the new session owes %v", owed)
					}
					if table := tableOf(t, "raw"); len(table) != 0 {
						t.Errorf("the new session's in-flight table holds %+v of the old one's", table)
					}
					if pos, ok, _ := lg.Position(store.MQTTReader("raw")); ok {
						t.Errorf("the new session has a cursor at %d, written for the old one", pos.Offset)
					}
					if !back.quiet(300 * time.Millisecond) {
						t.Fatal("the connection ended")
					}
					back.subscribeRaw("news/#", 2)
					owe(t, h, lg, news("a", "for the new session"), "raw")
					if got := sentOf(back.read()); got.typ != packets.Publish || got.payload != "for the new session" {
						t.Fatalf("the new session was sent %+v, want the broadcast it is owed", got)
					}
					return
				}
				if !present {
					t.Fatal("the session did not resume")
				}

				pk := back.read()
				if got := sentOf(pk); got.typ != packets.Publish || got.payload != "first" || pk.FixedHeader.Qos != qos {
					t.Fatalf("the new connection was sent %+v at QoS %d, want the first message at QoS %d",
						got, pk.FixedHeader.Qos, qos)
				}
				r = news("a", "second")
				r.QoS = qos
				owe(t, h, lg, r, "raw")
				next := back.read()
				if got := sentOf(next); got.typ != packets.Publish || got.payload != "second" {
					t.Fatalf("then the new connection was sent %+v, want the second message", got)
				}
				if next.PacketID == pk.PacketID {
					t.Fatalf("the second message went under identifier %d, which the first still holds", pk.PacketID)
				}
				back.ack(pk)
				back.ack(next)
				if !back.quiet(300 * time.Millisecond) {
					t.Fatal("the connection ended")
				}
				eventually(t, "both messages acknowledged and let go", 5*time.Second, func() bool {
					held, _ := lg.ReadAt(first.Offset, first.Offset+1)
					return len(held) == 0 && len(tableOf(t, "raw")) == 0
				})
			})
		}
	}
}

// refusesEnding is a session store that refuses one client id's Begin, Drop
// or Get as many times as it is told to, as a store failing then would.
type refusesEnding struct {
	broker.SessionStore
	client           string
	begin, drop, get atomic.Int32
	refused, begun   atomic.Int32
}

func (s *refusesEnding) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *refusesEnding) InFlight(client string) (uint16, []store.InFlight, error) {
	return s.SessionStore.(interface {
		InFlight(string) (uint16, []store.InFlight, error)
	}).InFlight(client)
}

// refuse spends one of n's refusals for client, and says whether it did.
func (s *refusesEnding) refuse(client string, n *atomic.Int32) bool {
	for client == s.client {
		v := n.Load()
		if v <= 0 {
			return false
		}
		if n.CompareAndSwap(v, v-1) {
			s.refused.Add(1)
			return true
		}
	}
	return false
}

func (s *refusesEnding) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if s.refuse(client, &s.begin) {
		return store.Dropped{}, errors.New("refused for the test")
	}
	dropped, err := s.SessionStore.Begin(client, held, next)
	if client == s.client && err == nil {
		s.begun.Add(1)
	}
	return dropped, err
}

func (s *refusesEnding) Drop(client string, held []string) (store.Dropped, error) {
	if s.refuse(client, &s.drop) {
		return store.Dropped{}, errors.New("refused for the test")
	}
	return s.SessionStore.Drop(client, held)
}

func (s *refusesEnding) Get(client string) (store.Session, bool, error) {
	if s.refuse(client, &s.get) {
		return store.Session{}, false, errors.New("refused for the test")
	}
	return s.SessionStore.Get(client)
}

// refusingEndingsOf has every harness started after it wrap its session store
// in one refusesEnding for client.
func refusingEndingsOf(t *testing.T, client string) *refusesEnding {
	t.Helper()
	re := &refusesEnding{client: client}
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		re.SessionStore = s
		if _, ok := s.(interface {
			Snapshot() *store.SessionsSnapshot
		}); ok {
			return snapshotsRefusing{re}
		}
		return re
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	return re
}

// snapshotsRefusing is a refusesEnding around a memory store, which a stop
// writes to its sessions file through Snapshot.
type snapshotsRefusing struct{ *refusesEnding }

func (s snapshotsRefusing) Snapshot() *store.SessionsSnapshot {
	return s.SessionStore.(interface {
		Snapshot() *store.SessionsSnapshot
	}).Snapshot()
}

// connectRawWith connects id with the Clean Start, Receive Maximum and Session
// Expiry Interval given, and answers the CONNACK whatever it says.
func connectRawWith(t *testing.T, h *brokertest.Harness, id string, rx uint16, clean bool, expiry uint32) (*rawClient, packets.Packet) {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &rawClient{t: t, conn: conn}
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
		Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: id, Keepalive: 60, Clean: clean},
		Properties: packets.Properties{SessionExpiryInterval: expiry, SessionExpiryIntervalFlag: true, ReceiveMaximum: rx}})
	return c, c.read()
}

// threeOnTheWire leaves raw with three broadcasts it was sent and has not
// acknowledged, under identifiers 4 to 6 - past three it acknowledged, so that
// a new session's own 1 to 3 cannot replace them - with other away and owed
// the same, so the log keeps them. Its Receive Maximum is 3.
func threeOnTheWire(t *testing.T, h *brokertest.Harness, lg drainLog, raw *rawClient) {
	t.Helper()
	raw.subscribeRaw("news/#", 1)
	h.B.AttachBroadcast("raw")
	for i := range 3 {
		owe(t, h, lg, news("a", fmt.Sprint("acked-", i)), "raw", "other")
	}
	for _, pk := range raw.publishesUntilQuiet(500 * time.Millisecond) {
		raw.ack(pk)
	}
	eventually(t, "the three acknowledged let go", 5*time.Second, func() bool { return len(tableOf(t, "raw")) == 0 })
	for i := range 3 {
		owe(t, h, lg, news("a", fmt.Sprint("old-", i)), "raw", "other")
	}
	if got := raw.publishesUntilQuiet(500 * time.Millisecond); len(got) != 3 {
		t.Fatalf("raw was sent %d of the three it is to hold unacknowledged", len(got))
	}
	eventually(t, "three entries on the wire", 5*time.Second, func() bool { return len(tableOf(t, "raw")) == 3 })
}

// inheritsNothing says that the session under raw holds nothing an ended one
// left, that it is sent three new broadcasts into a window of three, and,
// after a restart, that none of the ended session's is sent to it again.
func inheritsNothing(t *testing.T, p restartable, h *brokertest.Harness, lg drainLog, back *rawClient) {
	t.Helper()
	if table := tableOf(t, "raw"); len(table) != 0 {
		t.Fatalf("the new session's in-flight table holds %+v, the ended session's", table)
	}
	if pos, ok, _ := lg.Position(store.MQTTReader("raw")); ok {
		t.Errorf("the new session has a cursor at %d, the ended session's", pos.Offset)
	}
	back.subscribeRaw("news/#", 2)
	h.B.AttachBroadcast("raw")
	for i := range 3 {
		owe(t, h, lg, news("a", fmt.Sprint("new-", i)), "raw")
	}
	got := back.publishesUntilQuiet(time.Second)
	if len(got) != 3 {
		t.Fatalf("the new session was sent %d of three new broadcasts into its window of three: the "+
			"ended session's entries filled it", len(got))
	}
	for _, pk := range got {
		back.ack(pk)
	}
	eventually(t, "the new session's acknowledgements let go", 5*time.Second, func() bool {
		return len(tableOf(t, "raw")) == 0
	})
	_ = back.conn.Close()
	sessionGone(t, h, "raw")
	h.Stop()

	h = p.start(t)
	again, present := connectRawAs(t, h, "raw", 3, false)
	if !present {
		t.Fatal("the new session was not resumed after the restart, so this proves nothing")
	}
	if resent := again.publishesUntilQuiet(time.Second); len(resent) != 0 {
		var payloads []string
		for _, pk := range resent {
			payloads = append(payloads, string(pk.Payload))
		}
		t.Errorf("after a restart the new session was sent %q: the ended session's, never owed to it", payloads)
	}
}

// RFC 0003 "Sessions": a session that begins new begins with nothing an ended
// one left under its client id - not its in-flight table, not its cursor, not
// its subscriptions - whichever way the ending went. On both providers, and
// across a restart.
//
// Each used to end what was kept and then save the new record, and an ending
// the store refused left the ended session's entries under the id: the new
// session, Receive Maximum 3, was sent none of three new broadcasts into a
// window full of entries it never had, and after a restart was sent the
// ended session's three again as re-sends of messages it was never owed.
//   - A clean start whose one write the store refuses: its connection is
//     hung up, and the next clean start begins it.
//   - An ending the store refused at the disconnect, then a Clean Start 0
//     CONNECT for the id no session is held for: it begins new, carrying
//     none of the ended session's subscriptions.
func TestASessionBegunNewInheritsNothingAnEndedOneLeft(t *testing.T) {
	for _, arm := range []string{"a clean start whose write is refused", "a Clean Start 0 after a refused ending"} {
		t.Run(arm, func(t *testing.T) {
			for _, p := range restartables(t) {
				t.Run(p.name, func(t *testing.T) {
					re := refusingEndingsOf(t, "raw")
					h := p.start(t)
					lg := attachDrainWith(t, h, func(l drainReads) drainReads { return l },
						func(s drainSessions) drainSessions { return s })
					away(t, h, "other", "news/#")
					var back *rawClient
					if arm == "a clean start whose write is refused" {
						raw, _ := connectRawAs(t, h, "raw", 3, false)
						threeOnTheWire(t, h, lg, raw)
						re.begin.Store(1)
						hungUp, _ := connectRawAs(t, h, "raw", 3, true)
						eventually(t, "the refused write", 5*time.Second, func() bool { return re.refused.Load() == 1 })
						if !hungUp.quietUntilClosed(3 * time.Second) {
							t.Fatal("the connection whose clean start the store refused was not ended")
						}
						begun := re.begun.Load()
						back, _ = connectRawAs(t, h, "raw", 3, true)
						// The clean start's write follows its CONNACK.
						eventually(t, "the clean start's write", 5*time.Second, func() bool { return re.begun.Load() > begun })
					} else {
						raw, ack := connectRawWith(t, h, "raw", 3, true, 0)
						if ack.ReasonCode != 0 {
							t.Fatalf("connect: reason code 0x%02x", ack.ReasonCode)
						}
						threeOnTheWire(t, h, lg, raw)
						re.drop.Store(1)
						_ = raw.conn.Close()
						sessionGone(t, h, "raw")
						eventually(t, "the refused ending", 5*time.Second, func() bool { return re.refused.Load() == 1 })
						if len(tableOf(t, "raw")) != 3 {
							t.Fatal("the refused ending left no entries, so this proves nothing")
						}
						var connack packets.Packet
						back, connack = connectRawWith(t, h, "raw", 3, false, 3600)
						if connack.ReasonCode != 0 || connack.SessionPresent {
							t.Fatalf("a Clean Start 0 CONNECT for an id no session is held for was answered 0x%02x, "+
								"Session Present %t, want accepted with Session Present 0", connack.ReasonCode, connack.SessionPresent)
						}
						if sess, _, _ := re.SessionStore.Get("raw"); len(sess.Subscriptions) != 0 {
							t.Errorf("the new session's record holds %d subscriptions, the ended session's",
								len(sess.Subscriptions))
						}
					}
					inheritsNothing(t, p, h, lg, back)
				})
			}
		})
	}
}

// **A clean start the store refused to begin is not resumed by the next
// CONNECT** (RFC 0003 "Sessions": "the client's next CONNECT begins the
// session again"). The hang-up after its CONNACK ended the connection and not
// the session, so the next CONNECT with Clean Start 0 - what a durable
// client's library sends on every reconnect - resumed it, with the ended
// session's in-flight table under it, and a restart sent that session's three
// messages to the session that began new. TestASessionBegunNewInheritsNothing-
// AnEndedOneLeft follows the refusal with another clean start, which Begins;
// this follows it with Clean Start 0.
func TestAReconnectAfterARefusedCleanStartBeginsTheSessionAgain(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			re := refusingEndingsOf(t, "raw")
			h := p.start(t)
			lg := attachDrainWith(t, h, func(l drainReads) drainReads { return l },
				func(s drainSessions) drainSessions { return s })
			away(t, h, "other", "news/#")
			raw, _ := connectRawAs(t, h, "raw", 3, false)
			threeOnTheWire(t, h, lg, raw)
			re.begin.Store(1)
			hungUp, _ := connectRawAs(t, h, "raw", 3, true)
			eventually(t, "the refused write", 5*time.Second, func() bool { return re.refused.Load() == 1 })
			if !hungUp.quietUntilClosed(3 * time.Second) {
				t.Fatal("the connection whose clean start the store refused was not ended")
			}
			sessionGone(t, h, "raw")

			table := tableOf(t, "raw")
			pos, hasCursor, _ := lg.Position(store.MQTTReader("raw"))
			sess, kept, _ := re.SessionStore.Get("raw")
			_, held := h.Srv.Clients.Get("raw")
			t.Logf("after the hang-up: engine holds a session=%t; record kept=%t expiry=%d subs=%d; "+
				"in-flight table %d entries; broadcast cursor kept=%t at %d",
				held, kept, sess.ExpiryInterval, len(sess.Subscriptions), len(table), hasCursor, pos.Offset)

			back, connack := connectRawWith(t, h, "raw", 3, false, 3600)
			t.Logf("the next CONNECT (Clean Start 0) was answered 0x%02x, Session Present %t",
				connack.ReasonCode, connack.SessionPresent)
			if connack.SessionPresent {
				t.Errorf("the next CONNECT resumed the session (Session Present 1); RFC 0003 says it begins the session again")
			}
			if table := tableOf(t, "raw"); len(table) != 0 {
				t.Errorf("the session's in-flight table holds %d entries of the ended session's", len(table))
			}
			if pos, ok, _ := lg.Position(store.MQTTReader("raw")); ok {
				t.Errorf("the session has a broadcast cursor at %d, written for the ended session", pos.Offset)
			}

			_ = back.conn.Close()
			sessionGone(t, h, "raw")
			h.Stop()
			h = p.start(t)
			again, present := connectRawAs(t, h, "raw", 3, false)
			resent := again.publishesUntilQuiet(time.Second)
			var payloads []string
			for _, pk := range resent {
				payloads = append(payloads, string(pk.Payload))
			}
			t.Logf("after a restart: Session Present %t, sent %q", present, payloads)
			if len(resent) != 0 {
				t.Errorf("after a restart the session that began new was sent %q: the ended session's, never owed to it", payloads)
			}
		})
	}
}

// **A late PUBACK from a taken-over connection leaves the successor's record
// owed** (acksFor), end to end on both providers. r1 goes to the old
// connection under identifier 1 and is acknowledged; the session is resumed
// by a second connection, which is sent r2 under identifier 1 again; the hook
// is then called as the engine calls it when the old connection's PUBACK
// lands after that - for the old connection, with the identifier the
// successor has since used. The stored position must not pass r2, and a
// restart must send r2 again. The third arm makes the session, restarts, and
// takes over a connection that resumed it after the start.
func TestALateAckFromATakenOverConnectionLeavesTheSuccessorsRecordOwed(t *testing.T) {
	for _, arm := range []string{"control: no late hook", "the late hook lands", "the late hook lands, after a restart"} {
		t.Run(arm, func(t *testing.T) {
			for _, p := range restartables(t) {
				t.Run(p.name, func(t *testing.T) {
					h := p.start(t)
					var old *rawClient
					if arm == "the late hook lands, after a restart" {
						// The session is made, the broker restarted, and the
						// connection that is then taken over is one that resumed
						// it: nothing it holds was registered before the start.
						made, _ := connectRawAs(t, h, "cons", 10, true)
						made.subscribeRaw("events/#", 1)
						_ = made.conn.Close()
						sessionGone(t, h, "cons")
						h.Stop()
						h = p.start(t)
						old, _ = connectRawAs(t, h, "cons", 10, false)
					} else {
						old, _ = connectRawAs(t, h, "cons", 10, true)
						old.subscribeRaw("events/#", 1)
					}
					pub := connect(t, h, "producer", true, false)
					pub.Pub(t, "events/x", "r1")
					first := old.read()
					if first.FixedHeader.Type != packets.Publish || string(first.Payload) != "r1" {
						t.Fatalf("the consumer was sent %+v, want r1", sentOf(first))
					}
					old.ack(first)
					oldCl, ok := h.Srv.Clients.Get("cons")
					if !ok {
						t.Fatal("no client under cons")
					}
					eventually(t, "the first record acknowledged and its position stored", 5*time.Second, func() bool {
						off, ok, _ := h.B.ChannelPosition("events", "cons")
						return ok && off == 2
					})

					next, present := connectRawAs(t, h, "cons", 10, false)
					if !present {
						t.Fatal("the session did not resume")
					}
					takenOver(t, h, "cons", oldCl)
					pub.Pub(t, "events/x", "r2")
					second := next.read()
					if second.FixedHeader.Type != packets.Publish || string(second.Payload) != "r2" {
						t.Fatalf("the successor was sent %+v, want r2", sentOf(second))
					}
					t.Logf("r1 went to the old connection under identifier %d; r2 to the successor under %d",
						first.PacketID, second.PacketID)
					if second.PacketID != first.PacketID {
						t.Skipf("the successor did not reuse identifier %d (got %d), so the race is not set up", first.PacketID, second.PacketID)
					}

					flushes, restore := broker.CountPositionFlushes(h.B)
					if arm != "control: no late hook" {
						h.B.OnQosComplete(oldCl, packets.Packet{
							FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: first.PacketID})
					}
					awaitPasses(t, "the position flush", flushes, 1) // a flush whole after the hook
					restore()
					off, ok, err := h.B.ChannelPosition("events", "cons")
					t.Logf("stored position while r2 (offset 2) is unacknowledged on the successor: %d (kept=%t, err=%v)", off, ok, err)
					if ok && off > 2 {
						t.Errorf("the stored position is %d while the successor holds offset 2 unacknowledged: a position that leads", off)
					}

					_ = next.conn.Close()
					sessionGone(t, h, "cons")
					h.Stop()
					h = p.start(t)
					again, present := connectRawAs(t, h, "cons", 10, false)
					if !present {
						t.Fatal("the session did not come back after the restart")
					}
					resent := again.publishesUntilQuiet(time.Second)
					var payloads []string
					for _, pk := range resent {
						payloads = append(payloads, string(pk.Payload))
					}
					t.Logf("after a restart the resumed session was sent %q", payloads)
					if len(payloads) != 1 || payloads[0] != "r2" {
						t.Errorf("after a restart the session was sent %q, want [r2]: the record it never acknowledged", payloads)
					}
				})
			}
		})
	}
}

// RFC 0003 "Sessions": a CONNECT for an id no session is held for begins a
// new session and reads nothing of the store, so a store refusing a read then
// refuses nothing. It used to read the record, for its subscriptions, and a
// read the store refused answered a Clean Start 0 CONNECT 0x83.
//
// And what the store refuses at the start of a session is answered by what
// the client has been told. A record that cannot be kept, where what an ended
// session left could still be ended, is a session accepted and ending with its
// connection; an ending the store refuses is 0x83, the CONNECT refused, since
// the session would begin inside what the ended one left. The next CONNECT
// begins it.
func TestWhatTheStoreRefusesAsASessionBeginsIsAnswered(t *testing.T) {
	type want struct {
		code   byte
		expiry uint32
	}
	for _, c := range []struct {
		name          string
		get, begin    int32
		expiry        uint32
		want          want
		refusedWanted int32
	}{
		{"a read refused, which is never made", 1, 0, 3600, want{0x00, 3600}, 0},
		{"a kept record refused, its ending made", 0, 1, 3600, want{0x00, 0}, 1},
		{"a kept record and its ending refused", 0, 2, 3600, want{0x83, 0}, 2},
		{"the ending of a session not kept refused", 0, 1, 0, want{0x83, 0}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, p := range restartables(t) {
				t.Run(p.name, func(t *testing.T) {
					re := refusingEndingsOf(t, "raw")
					h := p.start(t)
					re.get.Store(c.get)
					re.begin.Store(c.begin)
					_, ack := connectRawWith(t, h, "raw", 3, false, c.expiry)
					if ack.FixedHeader.Type != packets.Connack || ack.ReasonCode != c.want.code {
						t.Fatalf("the CONNECT was answered type %d, reason code 0x%02x, want a CONNACK with 0x%02x",
							ack.FixedHeader.Type, ack.ReasonCode, c.want.code)
					}
					// Absent from the CONNACK, it is what the CONNECT asked for.
					granted := c.expiry
					if ack.Properties.SessionExpiryIntervalFlag {
						granted = ack.Properties.SessionExpiryInterval
					}
					if c.want.code == 0 && granted != c.want.expiry {
						t.Errorf("the CONNACK grants Session Expiry %d, want %d", granted, c.want.expiry)
					}
					if got := re.refused.Load(); got != c.refusedWanted {
						t.Errorf("the store refused %d calls, want %d: the arm did not run as it says", got, c.refusedWanted)
					}
					if c.want.code != 0 {
						if _, again := connectRawWith(t, h, "raw", 3, false, c.expiry); again.ReasonCode != 0 {
							t.Errorf("the next CONNECT, the store refusing nothing, was answered 0x%02x", again.ReasonCode)
						}
					}
				})
			}
		})
	}
}

// RFC 0003 "Sessions": a session retention passed ends whole, and the one
// begun in its place, to Session Present 0, holds nothing of it - not its
// in-flight table, not its broadcast cursor. Neither was ended: its client,
// Receive Maximum 3, was sent none of three new broadcasts into a window full
// of the ended session's entries, and after a restart was sent those again.
// On both providers, and across a restart.
func TestASessionBegunWhereRetentionPassedInheritsNothing(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			brokertest.Retain = map[string]int64{"events": 200}
			t.Cleanup(func() { brokertest.Retain = nil })
			h := p.start(t)
			lg := attachDrain(t, h)
			away(t, h, "other", "news/#")
			pub := connect(t, h, "producer", true, false)
			raw, _ := connectRawAs(t, h, "raw", 3, false)
			raw.subscribeRaw("events/#", 1)
			pub.Pub(t, "events/a/1", "first")
			got := raw.publishesUntilQuiet(500 * time.Millisecond)
			if len(got) != 1 {
				t.Fatalf("raw was sent %d events, want 1", len(got))
			}
			raw.ack(got[0])
			awaitStoredPosition(t, h, "events", 2, 5*time.Second) // its position stored
			threeOnTheWire(t, h, lg, raw)
			_ = raw.conn.Close()
			sessionGone(t, h, "raw")
			for i := range 30 {
				pub.Pub(t, "events/a/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
			}
			awaitFloorPast(t, h, "events", 2, 5*time.Second) // the size sweep past the position

			back, ack := connectRawWith(t, h, "raw", 3, false, 3600)
			if ack.ReasonCode != 0 || ack.SessionPresent {
				t.Fatalf("the CONNECT was answered 0x%02x, Session Present %t, want accepted with Session "+
					"Present 0: retention did not end the session, so this proves nothing", ack.ReasonCode, ack.SessionPresent)
			}
			inheritsNothing(t, p, h, lg, back)
		})
	}
}

// quietUntilClosed reports whether the broker ends the connection within d,
// with a DISCONNECT or without, sending nothing else first.
func (c *rawClient) quietUntilClosed(d time.Duration) bool {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	for {
		first, _, err := brokertest.ReadRawPacket(c.conn)
		if err != nil {
			var ne net.Error
			return !(errors.As(err, &ne) && ne.Timeout())
		}
		if first>>4 == packets.Disconnect {
			return true
		}
	}
}

// failsReleased holds, once armed, the drain's write of a QoS 2 exchange
// moving to Released, and then refuses it.
type failsReleased struct {
	drainSessions
	*heldCall
}

func (s *failsReleased) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	if len(fs) == 1 && fs[0].State == store.MessageReleased && s.armed.Load() {
		s.hold(true)()
		return errors.New("refused for the test")
	}
	return s.drainSessions.SetInFlightAll(client, window, fs)
}

// RFC 0003 "Exactly once", and invariant 17: a PUBREC whose write the store
// refuses puts the drain's entry back as it was, and only if nothing has
// moved it on meanwhile. The session resumed on another connection while the
// write was out, was sent the PUBLISH again, and answered it with a PUBREC of
// its own, which moved the entry to Released on the new connection and was
// written. Put back unconditionally, the old write's failure turned that entry
// back into Sent on the old connection over the new one's PUBREC: nothing the
// client could see, and a drain whose state no longer matched its store's.
func TestAFailedReleaseLeavesAResumedConnectionsExchangeAlone(t *testing.T) {
	h := start(t)
	held := newHeldCall()
	t.Cleanup(held.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		return &failsReleased{drainSessions: s, heldCall: held}
	})
	old := dialRaw(t, h, "raw", "news/#")
	r := news("a", "exactly once")
	r.QoS = 2
	owed := owe(t, h, lg, r, "raw")
	pub := old.read()
	if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != 2 {
		t.Fatalf("the session was sent %+v, want the QoS 2 broadcast", pub.FixedHeader)
	}
	held.armed.Store(true)
	old.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID})
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the PUBREC's write never came, so this proves nothing")
	}

	back, present := connectRawAs(t, h, "raw", 10, false)
	if !present {
		t.Fatal("the session was not resumed, so this proves nothing")
	}
	again := back.read()
	if again.FixedHeader.Type != packets.Publish || again.PacketID != pub.PacketID || !again.FixedHeader.Dup {
		t.Fatalf("the resumed connection was sent %+v id %d, want the PUBLISH again under %d with DUP",
			again.FixedHeader, again.PacketID, pub.PacketID)
	}
	back.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: again.PacketID})
	now, _ := h.Srv.Clients.Get("raw")
	eventually(t, "the resumed connection's PUBREC on the entry", 5*time.Second, func() bool {
		state, conn, ok := h.B.BroadcastFlight("raw", pub.PacketID)
		return ok && state == store.MessageReleased && conn == now
	})

	held.let()
	select {
	case <-held.returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the held write never returned")
	}
	rel := back.read()
	if rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != pub.PacketID {
		t.Fatalf("the resumed connection was sent %+v id %d, want its PUBREL", rel.FixedHeader, rel.PacketID)
	}
	time.Sleep(100 * time.Millisecond) // for anything the failed write does after it returns
	if state, conn, ok := h.B.BroadcastFlight("raw", pub.PacketID); !ok || state != store.MessageReleased || conn != now {
		t.Fatalf("after the first connection's write failed the drain's entry is state %d on the resumed "+
			"connection %v (held %v), want Released on it: the failure put back what the resumed "+
			"connection's PUBREC had moved on", state, conn == now, ok)
	}
	back.ack(rel)
	eventually(t, "the exchange finished and let go", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(owed.Offset)
		return len(tableOf(t, "raw")) == 0 && len(held) == 0
	})
}

// heldCall holds, once armed, the next call its store sees that matches until
// it is let go, and says when that call has returned.
type heldCall struct {
	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func newHeldCall() *heldCall {
	return &heldCall{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
}

// hold holds the call if it matches and is the one armed for, and answers
// what to run once it has returned.
func (c *heldCall) hold(match bool) func() {
	if !match || !c.armed.CompareAndSwap(true, false) {
		return func() {}
	}
	close(c.entered)
	<-c.release
	return func() { close(c.returned) }
}

func (c *heldCall) let() { c.once.Do(func() { close(c.release) }) }

// holdsWrite is a session store that holds the drain's next write that
// matches.
type holdsWrite struct {
	drainSessions
	*heldCall
	match func(call string, fs []store.InFlight) bool
}

func (s *holdsWrite) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	defer s.hold(s.match("SetInFlightAll", fs))()
	return s.drainSessions.SetInFlightAll(client, window, fs)
}

func (s *holdsWrite) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	defer s.hold(s.match("Acknowledge", done))()
	return s.drainSessions.Acknowledge(client, cursor, done)
}

// holdsLogRead is a broadcast log that holds the drain's next read of what a
// batch picked: a batch held before it records anything.
type holdsLogRead struct {
	drainReads
	*heldCall
}

func (l *holdsLogRead) ReadAt(offsets ...uint64) ([]store.Record, error) {
	defer l.hold(true)()
	return l.drainReads.ReadAt(offsets...)
}

// releasesAfterTheEnding is an engine hook that, once armed, lets a held call
// go when a clean start's CONNACK for the client id has been written - after
// saguin has ended the session it replaces in the drain and in the store
// (confirmClaim) and before the engine takes the old connection over - and
// holds the takeover while the call returns and what follows it runs.
type releasesAfterTheEnding struct {
	mqtt.HookBase
	client string
	held   *heldCall
	armed  atomic.Bool
}

func (h *releasesAfterTheEnding) ID() string { return "releases-after-the-ending" }
func (h *releasesAfterTheEnding) Provides(b byte) bool {
	return b == mqtt.OnPacketSent
}
func (h *releasesAfterTheEnding) OnPacketSent(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	if pk.FixedHeader.Type != packets.Connack || cl.ID != h.client || !h.armed.CompareAndSwap(true, false) {
		return
	}
	h.held.let()
	select {
	case <-h.held.returned:
		time.Sleep(100 * time.Millisecond) // what the batch does after its read
	case <-time.After(5 * time.Second):
	}
}

// RFC 0003 "Sessions" and "Broadcast": a store write of a session's drain
// that is under way when the session ends lands before the store's ending of
// it, or is not made - never after it, where at a clean start the store
// already holds the session that replaced it, and the write lands in that
// session's in-flight table or cursor. Each of the drain's writers is held
// while the client id is taken with Clean Start 1, until the drain has let go
// of the old session's list, and is then let go:
//   - a batch recording what it puts on the wire, held in the store;
//   - a batch reading what it picked, held before it records anything and
//     let go once the ending is done and before the takeover empties the
//     old connection's subscriptions, so it records after the ending;
//   - a QoS 2 delivery's PUBREC, recorded as released;
//   - an acknowledgement, written with the cursor;
//   - an away session's cursor, written where its bound moved it (settle).
//
// The session that replaced it has an empty in-flight table and no cursor,
// and is owed the next broadcast and sent it: the ending run again when the
// takeover registers it (endSession) leaves its list alone. On both
// providers.
func TestADrainWriteUnderWayWhenASessionEndsLandsBeforeItsEnd(t *testing.T) {
	sets := func(state store.MessageState) func(string, []store.InFlight) bool {
		return func(call string, fs []store.InFlight) bool {
			return call == "SetInFlightAll" && len(fs) > 0 && fs[0].State == state
		}
	}
	acknowledges := func(call string, _ []store.InFlight) bool { return call == "Acknowledge" }
	none := func(string, []store.InFlight) bool { return false }
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	for _, w := range []struct {
		name string
		// match is the session store's write held, and onLog says the log's
		// read is held instead.
		match func(string, []store.InFlight) bool
		onLog bool
		// bound is limits.session_queue_bytes, or zero for the default.
		bound int64
		// hold arms held and makes the drain start what it holds.
		hold func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall)
	}{
		{"a batch's record", sets(store.MessageSent), false, 0, func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall) {
			dialRaw(t, h, "raw", "news/#")
			held.armed.Store(true)
			owe(t, h, lg, news("a", "recorded"), "raw")
		}},
		{"a batch's read", none, true, 0, func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall) {
			dialRaw(t, h, "raw", "news/#")
			held.armed.Store(true)
			owe(t, h, lg, news("a", "read"), "raw")
		}},
		{"a PUBREC's release", sets(store.MessageReleased), false, 0, func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall) {
			c := dialRaw(t, h, "raw", "news/#")
			r := news("a", "received")
			r.QoS = 2
			owe(t, h, lg, r, "raw")
			pk := c.read()
			held.armed.Store(true)
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pk.PacketID})
		}},
		{"an acknowledgement", acknowledges, false, 0, func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall) {
			c := dialRaw(t, h, "raw", "news/#")
			owe(t, h, lg, news("a", "acknowledged"), "raw")
			pk := c.read()
			held.armed.Store(true)
			c.ack(pk)
		}},
		{"an away session's cursor", acknowledges, false, 2 * costOf(m(0)), func(t *testing.T, h *brokertest.Harness, lg drainLog, held *heldCall) {
			c := dialRaw(t, h, "raw", "news/#")
			before := h.Disconnects.Snapshot("raw")
			_ = c.conn.Close()
			sessionGoneAfter(t, h, "raw", before)
			held.armed.Store(true)
			for i := range 3 {
				owe(t, h, lg, m(i), "raw")
			}
		}},
	} {
		for _, p := range restartables(t) {
			t.Run(w.name+"/"+p.name, func(t *testing.T) {
				if w.bound > 0 {
					capAt(t, w.bound)
				}
				h := p.start(t)
				held := newHeldCall()
				t.Cleanup(held.let)
				lg := attachDrainWith(t, h, func(l drainReads) drainReads {
					if !w.onLog {
						return l
					}
					return &holdsLogRead{drainReads: l, heldCall: held}
				}, func(s drainSessions) drainSessions {
					return &holdsWrite{drainSessions: s, heldCall: held, match: w.match}
				})
				after := &releasesAfterTheEnding{client: "raw", held: held}
				if err := h.Srv.AddHook(after, nil); err != nil {
					t.Fatal(err)
				}
				w.hold(t, h, lg, held)
				after.armed.Store(w.onLog)
				select {
				case <-held.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("the drain never made the call held, so this proves nothing")
				}
				old, _ := h.Srv.Clients.Get("raw")
				list := h.B.BroadcastList("raw")
				if list == nil {
					t.Fatal("the drain holds no list for the session, so this proves nothing")
				}

				back, present := connectRawAs(t, h, "raw", 10, true)
				if present {
					t.Fatal("a clean start was told its session was present")
				}
				eventually(t, "the drain let go of the ended session's list", 5*time.Second, func() bool {
					return h.B.BroadcastList("raw") != list
				})
				held.let()
				select {
				case <-held.returned:
				case <-time.After(5 * time.Second):
					t.Fatal("the held call never returned")
				}
				takenOver(t, h, "raw", old)
				// Long enough for whatever the batch does after its read.
				if !back.quiet(300 * time.Millisecond) {
					t.Fatal("the connection ended")
				}

				if table := tableOf(t, "raw"); len(table) != 0 {
					t.Errorf("the session that replaced it holds %+v in its in-flight table, "+
						"written for the one that ended", table)
				}
				if pos, ok, _ := lg.Position(store.MQTTReader("raw")); ok {
					t.Errorf("the session that replaced it has a cursor at %d, written for the one that ended",
						pos.Offset)
				}
				back.subscribeRaw("news/#", 2)
				owe(t, h, lg, news("a", "for the new session"), "raw")
				if got := sentOf(back.read()); got.typ != packets.Publish || got.payload != "for the new session" {
					t.Fatalf("the session that replaced it was sent %+v, want the broadcast it is owed", got)
				}
			})
		}
	}
}

// publishesUntilQuiet is every PUBLISH the broker sends a raw client until it
// has sent nothing for d, unanswered.
func (c *rawClient) publishesUntilQuiet(d time.Duration) []packets.Packet {
	c.t.Helper()
	var got []packets.Packet
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(d))
		first, body, err := brokertest.ReadRawPacket(c.conn)
		if err != nil {
			return got
		}
		pk := packets.Packet{ProtocolVersion: 5}
		if err := pk.FixedHeader.Decode(first); err != nil {
			c.t.Fatalf("fixed header: %v", err)
		}
		if pk.FixedHeader.Type != packets.Publish {
			continue
		}
		if err := pk.PublishDecode(body); err != nil {
			c.t.Fatalf("decode a PUBLISH: %v", err)
		}
		got = append(got, pk)
	}
}

// MQTT-4.4.0-1, RFC 0003 "`append` - Durable consumers", "`latest`" and
// "Moving a consumer's position: seek": a channel delivery a takeover lands
// in the middle of reaches the connection that has the session, once, and
// without waiting for another record. Each way a channel's delivery is
// written on a connection is held while the client id is taken over, and let
// go either once the takeover is done or as the new connection is
// established, just before the takeover copies the old connection's
// in-flight table:
//   - an append channel's pump, between reading its batch and registering it;
//   - a latest channel's value, before it is written;
//   - a seek, after it has moved the position and before its reply, which
//     feeds the consumer from there once it is answered.
//
// At QoS 1 and 2. The new connection is sent the record once - carried by
// the takeover under the identifier it went out with, or sent there - and
// nothing is sent twice under two identifiers, which at QoS 2 is two
// deliveries.
func TestAChannelDeliveryATakeoverLandsInReachesTheNewConnectionOnce(t *testing.T) {
	for _, kind := range []string{"append", "latest", "seek"} {
		for _, qos := range []byte{1, 2} {
			for _, when := range []string{"after the takeover", "racing the takeover"} {
				t.Run(fmt.Sprintf("%s/QoS %d/%s", kind, qos, when), func(t *testing.T) {
					h := start(t)
					var armed atomic.Bool
					entered, release := make(chan struct{}), make(chan struct{})
					var once sync.Once
					let := func() { once.Do(func() { close(release) }) }
					t.Cleanup(let)
					holdFor := func(id string) {
						if id == "c" && armed.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
					}
					var restore func()
					switch kind {
					case "append":
						restore = broker.SetPumpBetweenReadAndCommit(holdFor)
					case "latest":
						restore = broker.SetLatestBeforeWrite(holdFor)
					case "seek":
						restore = broker.SetSeekBeforeReply(holdFor)
					}
					t.Cleanup(restore)
					race := &releasesAtTakeover{client: "c", let: let}
					if err := h.Srv.AddHook(race, nil); err != nil {
						t.Fatal(err)
					}

					filter := "events/#"
					if kind == "latest" {
						filter = "state/#"
					}
					c, _ := connectRaw(t, h, "c", 10)
					c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 1,
						Filters: packets.Subscriptions{{Filter: filter, Qos: qos}}})
					if pk := c.read(); pk.FixedHeader.Type != packets.Suback {
						t.Fatalf("subscribe: %+v", pk)
					}
					old, _ := h.Srv.Clients.Get("c")
					p := connect(t, h, "producer", true, false)
					publish := func(topic, payload string) {
						t.Helper()
						if _, err := p.C.Publish(context.Background(), &paho.Publish{
							Topic: topic, QoS: qos, Payload: []byte(payload)}); err != nil {
							t.Fatalf("publish %s: %v", payload, err)
						}
					}

					want := "r1"
					switch kind {
					case "append":
						armed.Store(true)
						publish("events/a", "r1")
					case "latest":
						armed.Store(true)
						want = "v1"
						publish("state/a", "v1")
					case "seek":
						// r1 is sent and acknowledged, then sought back to.
						publish("events/a", "r1")
						first := c.read()
						c.ack(first)
						offset := ""
						for _, u := range first.Properties.User {
							if u.Key == "saguin-offset" {
								offset = u.Val
							}
						}
						if offset == "" {
							t.Fatalf("r1 carried no offset: %+v", first.Properties.User)
						}
						armed.Store(true)
						c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
							TopicName: "$saguin/consumer/events/seek", Payload: []byte(offset)})
					}
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("the delivery was never held, so this proves nothing")
					}

					race.armed.Store(when == "racing the takeover")
					back, present := connectRaw(t, h, "c", 10)
					if !present {
						t.Fatal("the session did not resume")
					}
					takenOver(t, h, "c", old)
					let()

					var ids []uint16
					for _, pk := range back.publishesUntilQuiet(1500 * time.Millisecond) {
						if string(pk.Payload) == want {
							ids = append(ids, pk.PacketID)
						}
					}
					switch {
					case len(ids) == 0:
						t.Fatalf("%s never reached the new connection without another record to wake it", want)
					case len(ids) > 1:
						t.Fatalf("%s reached the new connection %d times, under identifiers %v", want, len(ids), ids)
					}
				})
			}
		}
	}
}

// RFC 0003 "`latest`": a value the broker counts sent from the moment it is
// registered on a connection - so that a takeover's copy of it is not served
// again beside it - is still owed to the session's next resume when its write
// fails with no takeover to carry it. Two ways a registered value fails:
//   - a live value whose write is held until its client's link has been cut,
//     so it fails on the closed connection;
//   - a current value, sent at SUBSCRIBE, larger than the Maximum Packet
//     Size its client declared, so it is refused and the connection ended
//     (MQTT-3.1.2-25). A current value is not recorded missed as a live one
//     is (missedLatest), so this arm is the one that leans on what the
//     drain held being recorded missed as the connection went.
//
// The session resumes - the second arm declaring no limit - and is sent the
// value once, fresh rather than as a re-send, which shows the resume served
// it and nothing carried it.
func TestALatestValueWhoseWriteFailedIsServedByTheNextResumeOnce(t *testing.T) {
	dial := func(t *testing.T, h *brokertest.Harness, maxPacket uint32) (*rawClient, bool) {
		t.Helper()
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		c := &rawClient{t: t, conn: conn}
		c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect},
			Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "c", Keepalive: 60},
			Properties: packets.Properties{SessionExpiryInterval: 3600, SessionExpiryIntervalFlag: true,
				ReceiveMaximum: 10, MaximumPacketSize: maxPacket}})
		pk := c.read()
		if pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
			t.Fatalf("connect: %+v", pk)
		}
		return c, pk.SessionPresent
	}
	for _, how := range []string{"a live value, its link cut", "a current value, too large for its subscriber"} {
		for _, qos := range []byte{1, 2} {
			t.Run(fmt.Sprintf("%s/QoS %d", how, qos), func(t *testing.T) {
				h := start(t)
				p := connect(t, h, "producer", true, false)
				publish := func(payload []byte) {
					t.Helper()
					if _, err := p.C.Publish(context.Background(), &paho.Publish{
						Topic: "state/a", QoS: qos, Payload: payload}); err != nil {
						t.Fatalf("publish: %v", err)
					}
				}
				subscribe := func(c *rawClient) {
					t.Helper()
					c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 1,
						Filters: packets.Subscriptions{{Filter: "state/#", Qos: qos}}})
				}
				want := []byte("v1")
				before := h.Disconnects.Snapshot("c")
				if how == "a live value, its link cut" {
					var armed atomic.Bool
					entered, release := make(chan struct{}), make(chan struct{})
					var once sync.Once
					let := func() { once.Do(func() { close(release) }) }
					t.Cleanup(let)
					t.Cleanup(broker.SetLatestBeforeWrite(func(id string) {
						if id == "c" && armed.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
					}))
					c, _ := dial(t, h, 0)
					subscribe(c)
					if pk := c.read(); pk.FixedHeader.Type != packets.Suback {
						t.Fatalf("subscribe: %+v", pk)
					}
					armed.Store(true)
					publish(want)
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("the value's write was never held, so this proves nothing")
					}
					_ = c.conn.Close()
					sessionGoneAfter(t, h, "c", before)
					let()
					// The held write runs on the closed connection, fails, and
					// is recorded missed, which is what the resume reads.
					eventually(t, "the failed write recorded missed", 5*time.Second, func() bool {
						return h.B.LatestMissed("c", "state") != 0
					})
				} else {
					want = bytes.Repeat([]byte("v"), 2048)
					publish(want)
					c, _ := dial(t, h, 1024)
					subscribe(c)
					sessionGoneAfter(t, h, "c", before)
					if got := c.publishesUntilQuiet(200 * time.Millisecond); len(got) != 0 {
						t.Fatalf("a client that declared 1024 bytes was sent %d PUBLISHes", len(got))
					}
				}

				back, present := dial(t, h, 0)
				if !present {
					t.Fatal("the session did not resume")
				}
				var got []sent
				for _, pk := range back.publishesUntilQuiet(1500 * time.Millisecond) {
					if bytes.Equal(pk.Payload, want) {
						got = append(got, sentOf(pk))
					}
				}
				switch {
				case len(got) == 0:
					t.Fatal("the resume was not sent the value whose write failed: it stepped over a value that never left")
				case len(got) > 1:
					t.Fatalf("the resume was sent the value %d times", len(got))
				case got[0].dup:
					t.Fatalf("the value came as a re-send under %d: something carried it, so this proves nothing "+
						"about the resume", got[0].id)
				}
			})
		}
	}
}

// RFC 0003 "Sessions" and invariant 17: a message acknowledged on the
// connection that had the session stays acknowledged when a takeover lands
// before the acknowledgement is stored. The write of the acknowledgement is
// held while the session is taken over, then let go; after a restart the
// session is not sent the message again, and the log no longer holds it. On
// both providers.
func TestAnAcknowledgementATakeoverOvertakesStaysAcknowledged(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			st := &holdsAcknowledgements{entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(st.let)
			lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				st.drainSessions = s
				return st
			})
			c := dialRaw(t, h, "raw", "news/#")
			old, _ := h.Srv.Clients.Get("raw")
			m := owe(t, h, lg, news("a", "acknowledged once"), "raw")
			pk := c.read()

			st.armed.Store(true)
			c.ack(pk)
			select {
			case <-st.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the acknowledgement was never written, so this proves nothing")
			}
			back, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not resume")
			}
			takenOver(t, h, "raw", old)
			st.let()
			eventually(t, "the acknowledged message out of the table", 5*time.Second, func() bool {
				return len(tableOf(t, "raw")) == 0
			})
			_ = back.conn.Close()
			h.Stop()

			h = p.start(t)
			lg = attachDrain(t, h)
			again, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not survive the restart")
			}
			if !again.quiet(500 * time.Millisecond) {
				t.Fatal("the connection ended")
			}
			if held, _ := lg.ReadAt(m.Offset); len(held) != 0 {
				t.Error("the log still holds a message its only session acknowledged")
			}
		})
	}
}

// refusesAcknowledgements is a session store that refuses the drain's writes
// of what a session acknowledged while armed, counting what it refused.
type refusesAcknowledgements struct {
	drainSessions
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *refusesAcknowledgements) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	if s.armed.Load() {
		s.refused.Add(1)
		return 0, errors.New("disk I/O error")
	}
	return s.drainSessions.Acknowledge(client, cursor, done)
}

// What was finished stays finished (RFC 0003 "Sessions"): acknowledgements
// the store refused to write are kept and written with the next, so a restart
// sends the session nothing it had acknowledged, and the log keeps nothing
// only it was owed. On both providers.
func TestAnAcknowledgementTheStoreRefusedIsWrittenAgain(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			st := new(refusesAcknowledgements)
			lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				st.drainSessions = s
				return st
			})
			c := dialRaw(t, h, "raw", "news/#")

			st.armed.Store(true)
			var ms []store.Record
			for i := range 3 {
				ms = append(ms, owe(t, h, lg, news("a", fmt.Sprint("refused-", i)), "raw"))
				c.ack(c.read())
			}
			eventually(t, "the store refusing the acknowledgements", 5*time.Second, func() bool {
				return st.refused.Load() > 0
			})
			st.armed.Store(false)
			ms = append(ms, owe(t, h, lg, news("a", "written"), "raw"))
			c.ack(c.read())
			eventually(t, "every acknowledgement written", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(offsets(ms)...)
				pos, ok, _ := lg.Position(store.MQTTReader("raw"))
				return len(held) == 0 && len(tableOf(t, "raw")) == 0 && ok && pos.Offset == lg.Next()
			})
			_ = c.conn.Close()
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			again, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not survive the restart")
			}
			if !again.quiet(500 * time.Millisecond) {
				t.Fatal("the connection ended")
			}
		})
	}
}
