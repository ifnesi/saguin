package harness

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// pipe connects a client to a fake peer over net.Pipe. The peer runs script
// on its own goroutine; pipe returns a function that waits for it and fails
// the test with the peer's error.
func pipe(t *testing.T, version byte, script func(peer *Client) error) (client *Client, wait func()) {
	t.Helper()
	a, b := net.Pipe()
	client = NewClient(a, version)
	client.Timeout = 2 * time.Second
	peer := NewClient(b, 0)
	peer.Timeout = 2 * time.Second
	done := make(chan error, 1)
	go func() { done <- script(peer) }()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() }) // net.Pipe's Close never fails
	return client, func() {
		t.Helper()
		if err := <-done; err != nil {
			t.Errorf("peer: %v", err)
		}
	}
}

// expect reads the next packet on the peer and checks it equals want.
func expect(peer *Client, want Packet) error {
	got, err := peer.Recv()
	if err != nil {
		return fmt.Errorf("waiting for %s: %w", Name(want.Type()), err)
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("got %+v, want %+v", got, want)
	}
	return nil
}

// TestConnectAndQoS1 runs a CONNECT and a QoS 1 publish whose PUBACK comes
// after a delivery: the delivery is kept for Recv, and its PUBACK goes back.
func TestConnectAndQoS1(t *testing.T) {
	delivery := &Publish{QoS: 1, Topic: "t", ID: 7, Payload: []byte("in"),
		Props: Properties{User("saguin-offset", "3")}}
	c, wait := pipe(t, V5, func(peer *Client) error {
		err := expect(peer, &Connect{ProtocolName: "MQTT", Version: V5, CleanStart: true, KeepAlive: 30, ClientID: "c1"})
		if err != nil {
			return err
		}
		if peer.Version != V5 {
			return fmt.Errorf("the peer read CONNECT but its version is %d", peer.Version)
		}
		if err := peer.Send(&Connack{Props: Properties{{ID: ReceiveMaximum, Int: 20}}}); err != nil {
			return err
		}
		if err := expect(peer, &Publish{QoS: 1, Topic: "t", ID: 1, Payload: []byte("out")}); err != nil {
			return err
		}
		if err := peer.Send(delivery); err != nil {
			return err
		}
		if err := peer.Send(&Ack{Kind: PUBACK, ID: 1}); err != nil {
			return err
		}
		return expect(peer, &Ack{Kind: PUBACK, ID: 7})
	})
	ack, err := c.Connect(&Connect{CleanStart: true, KeepAlive: 30, ClientID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if rm, _ := ack.Props.Get(ReceiveMaximum); rm.Int != 20 {
		t.Errorf("CONNACK Receive Maximum %d, want 20", rm.Int)
	}
	if puback, err := c.Publish(&Publish{QoS: 1, Topic: "t", Payload: []byte("out")}); err != nil || puback.Reason != 0 {
		t.Fatalf("Publish: %+v, %v", puback, err)
	}
	got, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, delivery) {
		t.Fatalf("the delivery read past while waiting: got %+v", got)
	}
	if v := got.(*Publish).Props.User("saguin-offset"); len(v) != 1 || v[0] != "3" {
		t.Errorf("saguin-offset = %v", v)
	}
	if err := c.Acknowledge(got.(*Publish)); err != nil {
		t.Fatal(err)
	}
	wait()
}

// TestConnectRefusesAReplyOverItsMaximumPacketSize: MQTT 5 3.1.2.11.4,
// "The Server MUST NOT send packets exceeding Maximum Packet Size to the
// Client [MQTT-3.1.2-24]"; design.md, Testing: "Every answer is checked
// against MQTT and this document". Audits 261018-108a: a CONNECT advertising
// a Maximum Packet Size of 100 is refused with an otherwise valid CONNACK
// 0x8C, Session Present 0, carrying a Reason String of 200 characters:
// Connect refuses it as too large, before its body is read.
func TestConnectRefusesAReplyOverItsMaximumPacketSize(t *testing.T) {
	const limit = 100
	refusal := &Connack{Reason: 0x8C, Props: Properties{{ID: ReasonString, Str: strings.Repeat("r", 2*limit)}}}
	c, wait := pipe(t, V5, func(peer *Client) error {
		if _, err := peer.Recv(); err != nil {
			return fmt.Errorf("waiting for CONNECT: %w", err)
		}
		// A client that refuses the CONNACK reads its fixed header alone:
		// the rest of this write ends when the client closes.
		_ = peer.Send(refusal)
		return nil
	})
	ack, err := c.Connect(&Connect{CleanStart: true, ClientID: "c",
		Props: Properties{{ID: AuthenticationMethod, Str: "m"}, {ID: MaximumPacketSize, Int: limit}}})
	if !errors.Is(err, ErrTooLarge) {
		t.Errorf("Connect = %+v, %v; want %v: the CONNACK is over the %d bytes the CONNECT advertised", ack, err, ErrTooLarge, limit)
	}
	_ = c.Close() // net.Pipe's Close never fails
	wait()
}

// aliased is a PUBLISH at QoS 0 of topic, empty for one sent by alias,
// carrying Topic Alias alias and no payload.
func aliased(topic string, alias uint32) *Publish {
	return &Publish{Topic: topic, Props: Properties{{ID: TopicAlias, Int: alias}}}
}

// TestTopicAliasesFromTheServer: MQTT 5 3.3.2.3.4, the receiver of a
// Topic Name and a Topic Alias "sets the specified Topic Alias mapping to
// this Topic Name"; given the alias and "a zero length Topic Name", it
// "treats the incoming PUBLISH as if it had contained the Topic Name of the
// Topic Alias"; "A sender can modify the Topic Alias mapping by sending
// another PUBLISH [...] with the same Topic Alias value and a different
// non-zero length Topic Name". A client whose CONNECT sends a Topic Alias
// Maximum of 2 reads each PUBLISH by alias under the topic its alias maps,
// ByAlias set, re-encoding it as the empty Topic Name read. It refuses an
// alias over its maximum [MQTT-3.1.2-26], any alias where its CONNECT sent
// none [MQTT-3.1.2-27], and an empty Topic Name whose alias maps nothing.
// A peer playing the server, having read a CONNECT, takes the client's own
// PUBLISH by alias as it came, its Topic Name empty, the test's to check.
func TestTopicAliasesFromTheServer(t *testing.T) {
	sent := []*Publish{aliased("a", 1), aliased("", 1), aliased("b", 2), aliased("", 2), aliased("c", 1), aliased("", 1)}
	topics := []string{"a", "a", "b", "b", "c", "c"}
	c, wait := pipe(t, V5, func(peer *Client) error {
		if _, err := peer.Recv(); err != nil {
			return fmt.Errorf("waiting for CONNECT: %w", err)
		}
		if err := peer.Send(&Connack{}); err != nil {
			return err
		}
		for _, p := range sent {
			if err := peer.Send(p); err != nil {
				return err
			}
		}
		return expect(peer, aliased("", 5))
	})
	if _, err := c.Connect(&Connect{CleanStart: true, ClientID: "c", Props: Properties{{ID: TopicAliasMaximum, Int: 2}}}); err != nil {
		t.Fatal(err)
	}
	for i, want := range topics {
		got := Expect[*Publish](t, c)
		if got.Topic != want || got.ByAlias != (sent[i].Topic == "") {
			t.Errorf("PUBLISH %d read with Topic %q and ByAlias %v; want %q and %v", i+1, got.Topic, got.ByAlias, want, sent[i].Topic == "")
		}
		if b, err := Encode(got, V5); err != nil || !bytes.Equal(b, Encoded(t, sent[i], V5)) {
			t.Errorf("PUBLISH %d re-encodes as % X, %v; want % X, as read", i+1, b, err, Encoded(t, sent[i], V5))
		}
	}
	if err := c.Send(&Publish{ByAlias: true, Topic: "x", Props: Properties{{ID: TopicAlias, Int: 5}}}); err != nil {
		t.Fatal(err)
	}
	wait()

	for _, tc := range []struct {
		name    string
		maximum uint32 // the CONNECT's Topic Alias Maximum; 0 sends none
		in      []*Publish
		err     error
	}{
		{"an alias over the maximum", 2, []*Publish{aliased("a", 3)}, ErrTopicAlias},
		{"an alias where the CONNECT sent no maximum", 0, []*Publish{aliased("a", 1)}, ErrTopicAlias},
		{"an empty Topic Name whose alias maps nothing", 2, []*Publish{aliased("a", 1), aliased("", 2)}, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, wait := pipe(t, V5, func(peer *Client) error {
				if _, err := peer.Recv(); err != nil {
					return fmt.Errorf("waiting for CONNECT: %w", err)
				}
				for _, p := range append([]Packet{&Connack{}}, packets(tc.in)...) {
					if err := peer.Send(p); err != nil {
						return err
					}
				}
				return nil
			})
			connect := &Connect{CleanStart: true, ClientID: "c"}
			if tc.maximum != 0 {
				connect.Props = Properties{{ID: TopicAliasMaximum, Int: tc.maximum}}
			}
			if _, err := c.Connect(connect); err != nil {
				t.Fatal(err)
			}
			for range tc.in[1:] {
				if _, err := c.Recv(); err != nil {
					t.Fatal(err)
				}
			}
			if p, err := c.Recv(); !errors.Is(err, tc.err) {
				t.Errorf("read %+v, %v; want %v", p, err, tc.err)
			}
			wait()
		})
	}
}

// packets returns ps as Packets.
func packets(ps []*Publish) []Packet {
	out := make([]Packet, len(ps))
	for i, p := range ps {
		out[i] = p
	}
	return out
}

// TestQoS2BothWays runs the four-packet exchange in each direction, 3.1.1.
func TestQoS2BothWays(t *testing.T) {
	c, wait := pipe(t, V311, func(peer *Client) error {
		peer.Version = V311
		steps := []struct {
			in  Packet // expected from the client, or nil
			out Packet // sent to it, or nil
		}{
			{&Publish{QoS: 2, Topic: "q", ID: 1}, &Ack{Kind: PUBREC, ID: 1}},
			{&Ack{Kind: PUBREL, ID: 1}, &Ack{Kind: PUBCOMP, ID: 1}},
			{nil, &Publish{QoS: 2, Topic: "q", ID: 900}},
			{&Ack{Kind: PUBREC, ID: 900}, &Ack{Kind: PUBREL, ID: 900}},
			{&Ack{Kind: PUBCOMP, ID: 900}, nil},
		}
		for _, s := range steps {
			if s.in != nil {
				if err := expect(peer, s.in); err != nil {
					return err
				}
			}
			if s.out != nil {
				if err := peer.Send(s.out); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if comp, err := c.Publish(&Publish{QoS: 2, Topic: "q"}); err != nil || comp.Kind != PUBCOMP {
		t.Fatalf("Publish QoS 2: %+v, %v", comp, err)
	}
	p, err := c.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Acknowledge(p.(*Publish)); err != nil {
		t.Fatal(err)
	}
	wait()
}

// TestSendRaw: raw bytes reach the peer exactly as given, a malformed
// CONNECT included.
func TestSendRaw(t *testing.T) {
	raw := []byte{0x10, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}
	c, wait := pipe(t, V5, func(peer *Client) error {
		got := make([]byte, len(raw))
		if _, err := io.ReadFull(peer.Conn, got); err != nil {
			return err
		}
		if !bytes.Equal(got, raw) {
			return fmt.Errorf("read % X, want % X", got, raw)
		}
		return nil
	})
	if err := c.SendRaw(raw); err != nil {
		t.Fatal(err)
	}
	wait()
}

// TestSubscribeUnsubscribePing covers the remaining requests a test client
// makes, and the server's DISCONNECT ending an Await.
func TestSubscribeUnsubscribePing(t *testing.T) {
	c, wait := pipe(t, V5, func(peer *Client) error {
		peer.Version = V5
		if err := expect(peer, &Subscribe{ID: 1, Subs: []Subscription{{Filter: "a/#", QoS: 2, NoLocal: true}}}); err != nil {
			return err
		}
		if err := peer.Send(&Suback{ID: 1, Reasons: []byte{0x02}}); err != nil {
			return err
		}
		if err := expect(peer, &Unsubscribe{ID: 2, Filters: []string{"a/#"}}); err != nil {
			return err
		}
		if err := peer.Send(&Unsuback{ID: 2, Reasons: []byte{0x00}}); err != nil {
			return err
		}
		if err := expect(peer, &Pingreq{}); err != nil {
			return err
		}
		if err := peer.Send(&Pingresp{}); err != nil {
			return err
		}
		return peer.Send(&Disconnect{Reason: 0x8B})
	})
	if err := c.Send(&Subscribe{ID: c.NextID(), Subs: []Subscription{{Filter: "a/#", QoS: 2, NoLocal: true}}}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Recv(); err != nil || !reflect.DeepEqual(p, &Suback{ID: 1, Reasons: []byte{0x02}}) {
		t.Fatalf("SUBACK: %+v, %v", p, err)
	}
	if err := c.Send(&Unsubscribe{ID: c.NextID(), Filters: []string{"a/#"}}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Recv(); err != nil || p.Type() != UNSUBACK {
		t.Fatalf("UNSUBACK: %+v, %v", p, err)
	}
	if err := c.Send(&Pingreq{}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.Recv(); err != nil || p.Type() != PINGRESP {
		t.Fatalf("PINGRESP: %+v, %v", p, err)
	}
	_, err := c.AwaitAck(PUBACK, 1)
	if err == nil || !strings.Contains(err.Error(), "DISCONNECT 0x8B") {
		t.Errorf("Await across a DISCONNECT: %v", err)
	}
	wait()
}

// TestRecvTimeout: a missing answer is a timeout, which is how a lost
// acknowledgement shows.
func TestRecvTimeout(t *testing.T) {
	c, wait := pipe(t, V311, func(peer *Client) error {
		_, err := peer.Recv() // the PUBLISH, never answered
		return err
	})
	c.Timeout = 50 * time.Millisecond
	_, err := c.Publish(&Publish{QoS: 1, Topic: "t"})
	if !IsTimeout(err) {
		t.Errorf("unanswered QoS 1 publish: %v, want a timeout", err)
	}
	wait()
}

// TestNextIDSkipsZero: packet identifiers are non-zero (3.1.1 [MQTT-2.3.1-1]).
func TestNextIDSkipsZero(t *testing.T) {
	c := &Client{lastID: 65534}
	for _, want := range []uint16{65535, 1, 2} {
		if got := c.NextID(); got != want {
			t.Errorf("NextID = %d, want %d", got, want)
		}
	}
}

// scriptedConn is a peer that sends a script of packets, each due a fixed
// time after the conn is made, and discards what it is sent. Inside a
// synctest bubble, whose clock is fake, Read sleeps until the next packet is
// due or the read deadline passes, whichever comes first: no real time
// passes, and the outcome never depends on the machine's speed.
type scriptedConn struct {
	net.Conn // nil: only the methods below are called
	start    time.Time
	script   []scriptedPacket
	unread   []byte
	deadline time.Time
}

// scriptedPacket is one packet of a script and when it is due.
type scriptedPacket struct {
	at   time.Duration // after the conn is made
	wire []byte
}

func newScriptedConn(script ...scriptedPacket) *scriptedConn {
	return &scriptedConn{start: time.Now(), script: script}
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if len(c.unread) == 0 {
		if len(c.script) == 0 {
			return 0, io.EOF
		}
		due := c.start.Add(c.script[0].at)
		if !c.deadline.IsZero() && !c.deadline.After(due) {
			time.Sleep(time.Until(c.deadline))
			return 0, os.ErrDeadlineExceeded
		}
		time.Sleep(time.Until(due))
		c.unread, c.script = c.script[0].wire, c.script[1:]
	}
	n := copy(b, c.unread)
	c.unread = c.unread[n:]
	return n, nil
}

func (c *scriptedConn) Write(b []byte) (int, error) { return len(b), nil }

func (c *scriptedConn) SetDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func (c *scriptedConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// TestAwaitDeadlineIsFixed: Await sets one deadline, Timeout after the wait
// begins, which unrelated packets never move: waiting past it on an
// acknowledgement means the acknowledgement was lost (DefaultTimeout). The
// peer sends a PINGRESP every 0.9 s and the PUBACK only at 4.5 s; with a
// Timeout of 1 s, AwaitAck times out at exactly 1 s.
func TestAwaitDeadlineIsFixed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pingresp, err := Encode(&Pingresp{}, V5)
		if err != nil {
			t.Fatal(err)
		}
		puback, err := Encode(&Ack{Kind: PUBACK, ID: 7}, V5)
		if err != nil {
			t.Fatal(err)
		}
		const step = 900 * time.Millisecond
		conn := newScriptedConn(
			scriptedPacket{1 * step, pingresp}, scriptedPacket{2 * step, pingresp},
			scriptedPacket{3 * step, pingresp}, scriptedPacket{4 * step, pingresp},
			scriptedPacket{5 * step, puback})
		c := NewClient(conn, V5)
		c.Timeout = time.Second
		began := time.Now()
		ack, err := c.AwaitAck(PUBACK, 7)
		if waited := time.Since(began); !IsTimeout(err) || waited != c.Timeout {
			t.Errorf("AwaitAck = %+v, %v after %v; want a timeout after %v", ack, err, waited, c.Timeout)
		}
	})
}
