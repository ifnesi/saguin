package broker_test

// Two things a client can send in fewer bytes than usual, driven at the
// wire: a property with no bytes in it, and a DISCONNECT that is only a
// Reason Code. Each was read as something else.

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

// connectedV5 opens a raw MQTT 5 connection and reads its CONNACK.
func connectedV5(t *testing.T, addr string, id string, flags byte, extra ...[]byte) *rawConn {
	t.Helper()
	r := rawDialTo(t, addr)
	r.write(connectPacket(5, flags, []byte(id), extra...))
	if a := r.answer(3 * time.Second); !a.got || a.kind != 2 || len(a.body) < 2 || a.body[1] != 0 {
		t.Fatalf("%s: no CONNACK 0x00: %+v", id, a)
	}
	return r
}

func subscribeV5(t *testing.T, r *rawConn, filter string) {
	t.Helper()
	body := append([]byte{0, 1, 0}, str([]byte(filter))...)
	body = append(body, 0)
	r.write(append([]byte{0x82, byte(len(body))}, body...))
	if a := r.answer(3 * time.Second); !a.got || a.kind != 9 {
		t.Fatalf("no SUBACK: %+v", a)
	}
}

// [MQTT-3.3.2-16] "The Server MUST send the Correlation Data unaltered to
// all subscribers", and Binary Data may be zero bytes long (MQTT 5 section
// 1.5.6): a subscriber is sent the property, empty, as the publisher sent it.
// Content Type and Response Topic are the same shape ([MQTT-3.3.2-20],
// [MQTT-3.3.2-15]).
func TestAnEmptyCorrelationDataReachesTheSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name  string
		props []byte
	}{
		{"Correlation Data", []byte{0x09, 0, 0}},
		{"Content Type", []byte{0x03, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t)
			sub := connectedV5(t, h.Addr, "short-sub", 0x02)
			subscribeV5(t, sub, "short/t")
			pub := connectedV5(t, h.Addr, "short-pub", 0x02)

			body := cat(str([]byte("short/t")), []byte{byte(len(tc.props))}, tc.props, []byte("x"))
			pub.write(cat([]byte{0x30, byte(len(body))}, body))

			a := sub.answer(3 * time.Second)
			if !a.got || a.kind != 3 {
				t.Fatalf("the subscriber was sent %+v, want the PUBLISH", a)
			}
			if !bytes.Contains(a.body, tc.props) {
				t.Errorf("the PUBLISH the subscriber got carries % x, which has lost the empty property % x",
					a.body, tc.props)
			}
		})
	}
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// MQTT 5 section 3.14.2.1: only a Remaining Length of 0 means Reason Code
// 0x00 with no Properties. A Remaining Length of 1 is a Reason Code, and 0x04
// is "Disconnect with Will Message": the client asked for its Will.
func TestADisconnectOfOneByteIsRead(t *testing.T) {
	for _, tc := range []struct {
		name     string
		packet   []byte
		wantWill bool
	}{
		{"reason 0x04, Remaining Length 1", []byte{0xE0, 1, 0x04}, true},
		{"reason 0x04 and no properties, Remaining Length 2", []byte{0xE0, 2, 0x04, 0}, true},
		{"reason 0x00, Remaining Length 1", []byte{0xE0, 1, 0x00}, false},
		{"Remaining Length 0", []byte{0xE0, 0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t)
			watch := connectedV5(t, h.Addr, "short-watch", 0x02)
			subscribeV5(t, watch, "will/t")

			// Will Flag, Will QoS 0, Clean Start; Will Properties, topic, message.
			dying := connectedV5(t, h.Addr, "short-dying", 0x02|0x04, []byte{0}, str([]byte("will/t")), str([]byte("gone")))
			dying.write(tc.packet)

			a := watch.answer(2 * time.Second)
			if got := a.got && a.kind == 3; got != tc.wantWill {
				t.Errorf("Will published = %v, want %v (watcher saw %+v)", got, tc.wantWill, a)
			}
		})
	}
}

// The same two properties on every path that STORES a message: an append
// channel keeps the record, and a subscriber that comes later is sent it
// from the start. Present and empty is what the publisher sent
// ([MQTT-3.3.2-15], [MQTT-3.3.2-16], [MQTT-3.3.2-20]), so present and empty
// is what has to arrive - from memory, from a snapshot file and from SQLite,
// and across a restart.
func TestAnEmptyPropertyIsKeptWithTheRecord(t *testing.T) {
	// Content Type and Correlation Data, both empty.
	props := []byte{0x03, 0, 0, 0x09, 0, 0}

	publish := func(t *testing.T, addr string) {
		t.Helper()
		pub := connectedV5(t, addr, "keep-pub", 0x02)
		body := cat(str([]byte("events/kept")), []byte{byte(len(props))}, props, []byte("x"))
		pub.write(cat([]byte{0x30, byte(len(body))}, body))
		// A round trip on the same connection, so the PUBLISH has been
		// handled before anything is stopped.
		pub.write([]byte{0xC0, 0})
		if a := pub.answer(3 * time.Second); !a.got || a.kind != 13 {
			t.Fatalf("no PINGRESP: %+v", a)
		}
	}
	replay := func(t *testing.T, addr string) {
		t.Helper()
		sub := connectedV5(t, addr, "keep-sub", 0x02)
		subscribeV5(t, sub, "events/#")
		a := sub.answer(3 * time.Second)
		if !a.got || a.kind != 3 {
			t.Fatalf("the replay was %+v, want the PUBLISH", a)
		}
		for _, p := range [][]byte{{0x03, 0, 0}, {0x09, 0, 0}} {
			if !bytes.Contains(a.body, p) {
				t.Errorf("the replayed PUBLISH carries % x, which has lost the empty property % x", a.body, p)
			}
		}
	}

	t.Run("memory", func(t *testing.T) {
		h := start(t)
		publish(t, h.Addr)
		replay(t, h.Addr)
	})
	t.Run("snapshot file, across a restart", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)
		publish(t, h.Addr)
		h.Stop()
		h = startDurable(t, dir)
		replay(t, h.Addr)
	})
	t.Run("sqlite, across a restart", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "k.db")
		h := startDurableSQLite(t, path)
		publish(t, h.Addr)
		h.Stop()
		h = startDurableSQLite(t, path)
		replay(t, h.Addr)
	})
}

// A Will is a stored message until it fires. One armed with an empty
// Correlation Data fires with it present.
func TestAWillKeepsItsEmptyCorrelationData(t *testing.T) {
	h := start(t)
	watch := connectedV5(t, h.Addr, "will-watch", 0x02)
	subscribeV5(t, watch, "will/t")

	// Will Flag, Clean Start; Will Properties = an empty Correlation Data.
	dying := connectedV5(t, h.Addr, "will-dying", 0x02|0x04,
		[]byte{3, 0x09, 0, 0}, str([]byte("will/t")), str([]byte("gone")))
	_ = dying.c.Close() // no DISCONNECT: the Will fires

	a := watch.answer(3 * time.Second)
	if !a.got || a.kind != 3 {
		t.Fatalf("the watcher was sent %+v, want the Will", a)
	}
	if !bytes.Contains(a.body, []byte{0x09, 0, 0}) {
		t.Errorf("the Will carries % x, which has lost the empty Correlation Data", a.body)
	}
}
