package mqtt

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/mqtt/system"
)

// A delivery larger than its client's Maximum Packet Size is discarded
// without being sent, and the server behaves as if it had completed sending
// it [MQTT-3.1.2-25]. Each way the engine got that wrong is held here: the refusal was treated as a write, so what was
// parked behind it was never flushed, and a QoS 1 or 2 delivery kept its
// in-flight entry and its slot of the client's window for good.

// **Nothing is left parked once nothing is queued behind it.** A write made
// while a packet is queued is parked in the connection's buffer for that
// packet's write to carry. One the loop takes and cannot write carries
// nothing, and the parked write waited for whatever the client was sent next
// - with keepalive 0 and no other traffic, for ever.
func TestAWriteParkedBehindAPacketTheLoopCannotWriteIsSent(t *testing.T) {
	r, w := net.Pipe()
	cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{ClientNetWriteBufferSize: 2048,
			Capabilities: &Capabilities{MaximumClientWritesPending: 8, maximumPacketID: 65535}}})
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.MaximumPacketSize = 64
	defer cl.Stop(nil)

	big := &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName: "t", Payload: make([]byte, 100)}
	require.True(t, cl.enqueue(big))
	require.NoError(t, cl.WritePacket(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: 3}))
	cl.Lock()
	parked := cl.Net.outbuf != nil && cl.Net.outbuf.Len() > 0
	cl.Unlock()
	require.True(t, parked, "the PUBACK was not parked behind the queued packet, so this "+
		"exercised nothing")

	cl.armWriter()
	pk, ok := readOne(t, r, 2*time.Second)
	require.True(t, ok, "the parked PUBACK was never written: the queued packet behind "+
		"it was refused for its size and nothing flushed what it was to carry")
	require.Equal(t, packets.Puback, pk.FixedHeader.Type)
	require.Equal(t, uint16(3), pk.PacketID)
}

// **A DISCONNECT is never parked.** It is the last packet on the connection,
// which closes behind it, and a queued packet that would have carried it is
// never written: parked, the client was hung up on without the reason.
func TestADisconnectWrittenWhilePacketsAreQueuedIsSent(t *testing.T) {
	s := newServer()
	defer s.Close()
	r, w := net.Pipe()
	cl := s.NewClient(w, "t", "dev", false)
	cl.Properties.ProtocolVersion = 5
	require.True(t, cl.enqueue(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName: "t", Payload: []byte("x")}))
	require.Equal(t, 1, cl.State.outbound.len(), "nothing was queued, so this exercised nothing")

	go func() { _ = s.DisconnectClient(cl, packets.ErrPacketTooLarge) }()
	pk, ok := readOne(t, r, 2*time.Second)
	require.True(t, ok, "the DISCONNECT was not written before the connection closed")
	require.Equal(t, packets.Disconnect, pk.FixedHeader.Type)
	require.Equal(t, packets.ErrPacketTooLarge.Code, pk.ReasonCode)
}

// readOne reads one packet from the client's end of a pipe, or reports none
// within d.
func readOne(t *testing.T, r net.Conn, d time.Duration) (packets.Packet, bool) {
	t.Helper()
	reader := newClient(r, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{Capabilities: NewDefaultServerCapabilities()}})
	reader.Properties.ProtocolVersion = 5
	_ = r.SetReadDeadline(time.Now().Add(d))
	fh := new(packets.FixedHeader)
	if err := reader.ReadFixedHeader(fh); err != nil {
		return packets.Packet{}, false
	}
	pk, err := reader.ReadPacket(fh)
	return pk, err == nil
}

// **The shape that failed, on the engine.** One connection declares Maximum
// Packet Size 64, subscribes to its own topic and publishes a 64-byte payload:
// its own delivery is too large for it and is discarded, and its publish is
// acknowledged all the same (invariant 16). Unacknowledged, a publisher
// re-sends, and every other subscriber is delivered the message twice.
//
// Every QoS it can be delivered at, because each goes through the socket
// queue: QoS 0 for every session, QoS 1 and 2 for one whose broadcasts are not
// kept in a log (saguin). Repeated, because what made it certain rather than
// possible was scheduling: before the socket queue, a delivery handed to a
// write loop already waiting left nothing queued for the PUBACK to park
// behind.
//
// **Each trial waits for its own discard before it hangs up, on a topic of
// its own.** The write loop can take the delivery before the PUBACK is
// written and count it after, so a count read at the PUBACK was one short;
// and a close landing before the count once hid it (writeQueued). A
// connection the broker has not yet seen close is still subscribed, so on a
// topic every trial shared it was delivered the next trial's publish and
// counted that too.
func TestAPublisherIsAcknowledgedWhenItsOwnDeliveryIsTooLarge(t *testing.T) {
	s, addr := serveEngine(t)
	defer s.Close()
	const trials = 10
	for _, subQoS := range []byte{0, 1, 2} {
		for _, pubQoS := range []byte{1, 2} {
			t.Run(fmt.Sprintf("subscribed at %d, published at %d", subQoS, pubQoS), func(t *testing.T) {
				before := s.Info.DeliveriesTooLarge.Load()
				for trial := 0; trial < trials; trial++ {
					id := fmt.Sprintf("self-%d-%d-%d", subQoS, pubQoS, trial)
					c := dialWire(t, addr, id, true, 0, 64, 0)
					c.subscribe(1, "mt/"+id, subQoS)
					c.publish(3, "mt/"+id, make([]byte, 64), pubQoS)
					want := pahopackets.PUBACK
					if pubQoS == 2 {
						want = pahopackets.PUBREC
					}
					c.expect(want, 3, "trial %d: the publish", trial)
					if pubQoS == 2 {
						rel := pahopackets.NewControlPacket(pahopackets.PUBREL)
						rel.Content.(*pahopackets.Pubrel).PacketID = 3
						c.send(rel)
						c.expect(pahopackets.PUBCOMP, 3, "trial %d: the release", trial)
					}
					counted := before + int64(trial) + 1
					for deadline := time.Now().Add(5 * time.Second); s.Info.DeliveriesTooLarge.Load() < counted &&
						time.Now().Before(deadline); time.Sleep(time.Millisecond) {
					}
					_ = c.c.Close()
				}
				require.Equal(t, int64(trials), s.Info.DeliveriesTooLarge.Load()-before,
					"every delivery discarded at QoS %d is counted, once", min(subQoS, pubQoS))
			})
		}
	}
}

// **A delivery refused for its size is discarded though its client closed
// meanwhile.** Nothing of it was written, so it is not a failed write: asked
// after the close, it went uncounted, and at QoS 1 kept its entry as if it
// had been sent. The close is put where the race above put it - after
// WritePacket's own check, before the refusal - by a hook that runs there.
func TestADeliveryRefusedForItsSizeAsItsClientClosesIsDiscarded(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("at QoS %d", qos), func(t *testing.T) {
			_, w := net.Pipe()
			hooks, hangUp := new(Hooks), new(hangUpOnEncode)
			require.NoError(t, hooks.Add(hangUp, nil))
			cl := newClient(w, &ops{info: new(system.Info), hooks: hooks, log: logger,
				options: &Options{ClientNetWriteBufferSize: 2048,
					Capabilities: &Capabilities{MaximumClientWritesPending: 8, maximumPacketID: 65535}}})
			cl.Properties.ProtocolVersion = 5
			cl.Properties.Props.MaximumPacketSize = 64
			defer cl.Stop(nil)

			big := &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
				TopicName: "t", Payload: make([]byte, 100)}
			if qos > 0 {
				big.PacketID = 1
				require.True(t, cl.State.Inflight.Set(*big))
			}
			require.True(t, cl.enqueue(big))
			pk, _ := cl.State.outbound.pop()

			require.False(t, cl.writeQueued(pk), "the write loop went on, so the client was "+
				"not closed when the refusal came back, and this exercised nothing")
			require.True(t, hangUp.fired, "the hook never saw the delivery")
			require.Equal(t, int64(1), cl.ops.info.DeliveriesTooLarge.Load(),
				"a delivery refused for its size as its client closed was not counted as discarded")
			if qos > 0 {
				_, held := cl.State.Inflight.Get(1)
				require.False(t, held, "the discarded delivery kept its in-flight entry")
			}
		})
	}
}

// hangUpOnEncode closes the client as a PUBLISH is about to be encoded.
type hangUpOnEncode struct {
	HookBase
	fired bool
}

func (h *hangUpOnEncode) ID() string { return "hang-up-on-encode" }

func (h *hangUpOnEncode) Provides(b byte) bool { return b == OnPacketEncode }

func (h *hangUpOnEncode) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Publish {
		h.fired = true
		cl.Stop(errors.New("the client hung up"))
	}
	return pk
}

// **A delivery too large for its subscriber gives its slot of window back.**
// It kept it: a subscriber with Receive Maximum 2 sent two was never sent
// anything again, while every publisher was told its message was accepted.
func TestADeliveryTooLargeGivesBackItsSlotOfWindow(t *testing.T) {
	s, addr := serveEngine(t)
	defer s.Close()
	sub := dialWire(t, addr, "narrow", true, 0, 64, 2)
	sub.subscribe(1, "w/x", 1)
	pub := dialWire(t, addr, "pub", true, 0, 0, 0)
	id := uint16(1)
	for i := 0; i < 2; i++ {
		pub.publish(id, "w/x", make([]byte, 80), 1)
		pub.expect(pahopackets.PUBACK, id, "publish %d", id)
		id++
	}
	for i := 0; i < 3; i++ {
		pub.publish(id, "w/x", []byte(fmt.Sprintf("small-%d", i)), 1)
		pub.expect(pahopackets.PUBACK, id, "publish %d", id)
		id++
	}
	for i := 0; i < 3; i++ {
		p := sub.expectPublish("the small message %d of 3, after two too large", i)
		require.Equal(t, fmt.Sprintf("small-%d", i), string(p.Payload))
		sub.ack(p)
	}
	require.Equal(t, int64(2), s.Info.DeliveriesTooLarge.Load())
}

// **A withheld delivery too large for its subscriber is discarded when its
// turn comes, and the next is written.** Written from the withheld set it took
// its slot and kept it, so nothing behind it was written.
func TestAWithheldDeliveryTooLargeIsDiscardedAndTheNextWritten(t *testing.T) {
	s, addr := serveEngine(t)
	defer s.Close()
	sub := dialWire(t, addr, "one-at-a-time", true, 0, 64, 1)
	sub.subscribe(1, "w/x", 1)
	pub := dialWire(t, addr, "pub", true, 0, 0, 0)
	for i, payload := range [][]byte{[]byte("first"), make([]byte, 80), []byte("third")} {
		pub.publish(uint16(i+1), "w/x", payload, 1)
		pub.expect(pahopackets.PUBACK, uint16(i+1), "publish %d", i+1)
	}
	first := sub.expectPublish("the first message, which fills the window")
	require.Equal(t, "first", string(first.Payload))
	require.Nil(t, sub.read(200*time.Millisecond), "a second delivery was written into a "+
		"window of one, so nothing here was withheld")
	sub.ack(first)
	third := sub.expectPublish("the third message, behind one too large")
	require.Equal(t, "third", string(third.Payload))
	require.Equal(t, int64(1), s.Info.DeliveriesTooLarge.Load())
}

// **A resumed session is re-sent everything it is owed past one too large
// for it.** The session was kept by a connection that took any size, and
// resumed by one that declares 64: the re-send stopped at the first delivery
// it could not write, and the ones behind it were not sent again.
func TestAResumeCarriesOnPastADeliveryTooLarge(t *testing.T) {
	s, addr := serveEngine(t)
	defer s.Close()
	first := dialWire(t, addr, "resumer", true, 300, 0, 0)
	first.subscribe(1, "w/x", 1)
	pub := dialWire(t, addr, "pub", true, 0, 0, 0)
	pub.publish(1, "w/x", make([]byte, 80), 1)
	pub.expect(pahopackets.PUBACK, 1, "the large publish")
	pub.publish(2, "w/x", []byte("small"), 1)
	pub.expect(pahopackets.PUBACK, 2, "the small publish")
	first.expectPublish("the large message, unacknowledged")
	first.expectPublish("the small message, unacknowledged")
	_ = first.c.Close()
	waitFor(t, func() bool {
		cl, ok := s.Clients.Get("resumer")
		return ok && cl.Closed()
	}, "the first connection to go")

	second := dialWire(t, addr, "resumer", false, 300, 64, 2)
	require.True(t, second.present, "the session was not resumed, so nothing was re-sent")
	again := second.expectPublish("the small message, re-sent past the large one")
	require.Equal(t, "small", string(again.Payload))
	require.True(t, again.Duplicate, "the re-send was not marked as one")
	second.ack(again)

	// Both slots are back: the large one's, discarded, and the small one's,
	// acknowledged.
	for i := 3; i <= 4; i++ {
		pub.publish(uint16(i), "w/x", []byte(fmt.Sprintf("after-%d", i)), 1)
		pub.expect(pahopackets.PUBACK, uint16(i), "publish %d", i)
	}
	for i := 3; i <= 4; i++ {
		p := second.expectPublish("message %d, after the resume", i)
		require.Equal(t, fmt.Sprintf("after-%d", i), string(p.Payload))
	}
	require.Equal(t, int64(1), s.Info.DeliveriesTooLarge.Load())
}

// serveEngine is the engine alone on a loopback port, every client allowed,
// with its default capabilities: newServer's Receive Maximum of 0 refuses
// every QoS 1 and 2 publish.
func serveEngine(t *testing.T) (*Server, string) {
	t.Helper()
	s := New(&Options{Logger: logger, Capabilities: NewDefaultServerCapabilities()})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	return s, tcp.Address()
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// wire is one MQTT 5 connection written and read with paho's codec, so what
// a test says its client sent is what paho encoded, not what the engine
// under test would have.
type wire struct {
	t       *testing.T
	c       net.Conn
	present bool
}

// dialWire connects as id. maxPacket and rxMax of 0 leave the property out.
func dialWire(t *testing.T, addr, id string, clean bool, expiry, maxPacket uint32, rxMax uint16) *wire {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	w := &wire{t: t, c: c}
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	conn := cp.Content.(*pahopackets.Connect)
	conn.ClientID, conn.CleanStart, conn.KeepAlive = id, clean, 0
	conn.Properties = &pahopackets.Properties{SessionExpiryInterval: &expiry}
	if maxPacket > 0 {
		conn.Properties.MaximumPacketSize = &maxPacket
	}
	if rxMax > 0 {
		conn.Properties.ReceiveMaximum = &rxMax
	}
	w.send(cp)
	ca, ok := w.read(5 * time.Second).Content.(*pahopackets.Connack)
	require.True(t, ok && ca.ReasonCode == 0, "the CONNECT was not accepted: %+v", ca)
	w.present = ca.SessionPresent
	return w
}

func (w *wire) send(cp *pahopackets.ControlPacket) {
	w.t.Helper()
	_, err := cp.WriteTo(w.c)
	require.NoError(w.t, err)
}

// read reads one packet, or answers nil where none arrives within d.
func (w *wire) read(d time.Duration) *pahopackets.ControlPacket {
	w.t.Helper()
	cp, err := w.next(d)
	if err != nil {
		w.t.Fatalf("read: %v", err)
	}
	return cp
}

// next reads one packet: nil and no error where none arrives within d, and
// the error where the connection ended.
func (w *wire) next(d time.Duration) (*pahopackets.ControlPacket, error) {
	_ = w.c.SetReadDeadline(time.Now().Add(d))
	cp, err := pahopackets.ReadPacket(w.c)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, nil
	}
	return cp, err
}

func (w *wire) subscribe(id uint16, topic string, qos byte) {
	w.t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
	s := cp.Content.(*pahopackets.Subscribe)
	s.PacketID = id
	s.Subscriptions = []pahopackets.SubOptions{{Topic: topic, QoS: qos}}
	w.send(cp)
	w.expect(pahopackets.SUBACK, id, "the subscription to %s", topic)
}

func (w *wire) publish(id uint16, topic string, payload []byte, qos byte) {
	w.t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBLISH)
	p := cp.Content.(*pahopackets.Publish)
	p.Topic, p.Payload, p.QoS = topic, payload, qos
	if qos > 0 {
		p.PacketID = id
	}
	w.send(cp)
}

// expect reads until a packet of type want with identifier id arrives, failing
// with everything else the broker sent if it does not within two seconds.
func (w *wire) expect(want byte, id uint16, what string, args ...any) {
	w.t.Helper()
	var other []string
	var err error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		var cp *pahopackets.ControlPacket
		if cp, err = w.next(time.Until(deadline)); cp == nil {
			break
		}
		if cp.Type == want && cp.PacketID() == id {
			return
		}
		other = append(other, cp.String())
	}
	w.t.Fatalf("%s: no %s %d within 2s; the broker sent %v, and the connection %s",
		fmt.Sprintf(what, args...), pahopackets.NewControlPacket(want).PacketType(), id, other, ended(err))
}

// expectPublish reads the next PUBLISH, failing with what else arrived.
func (w *wire) expectPublish(what string, args ...any) *pahopackets.Publish {
	w.t.Helper()
	var other []string
	var err error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		var cp *pahopackets.ControlPacket
		if cp, err = w.next(time.Until(deadline)); cp == nil {
			break
		}
		if p, ok := cp.Content.(*pahopackets.Publish); ok {
			return p
		}
		other = append(other, cp.String())
	}
	w.t.Fatalf("%s: no PUBLISH within 2s; the broker sent %v, and the connection %s",
		fmt.Sprintf(what, args...), other, ended(err))
	return nil
}

// ended says how a connection stood when a read gave up on it.
func ended(err error) string {
	if err != nil {
		return fmt.Sprintf("ended: %v", err)
	}
	return "stayed open"
}

func (w *wire) ack(p *pahopackets.Publish) {
	w.t.Helper()
	if p.QoS == 0 {
		return
	}
	cp := pahopackets.NewControlPacket(pahopackets.PUBACK)
	cp.Content.(*pahopackets.Puback).PacketID = p.PacketID
	w.send(cp)
}

// [MQTT-3.3.2-5] and [MQTT-3.3.2-6]: a message whose Message Expiry Interval
// has passed before its delivery starts is not sent, and one that is sent
// carries what is left of its interval.
//
// **A delivery withheld for the client's window keeps its deadline.** The
// withheld marker was written over it (Inflight.Withhold, and makeDelivery
// for one withheld as it is made), so a withheld message never expired, and
// when the window opened it was written with the interval its publisher gave
// it, however long it had waited. Held here behind a first delivery the
// subscriber does not acknowledge, with a Receive Maximum of 1, and let go
// by acknowledging that one.
func TestAWithheldDeliveryKeepsItsMessageExpiry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expiry uint32
		sent   bool
	}{
		{"its interval passes while it waits", 1, false},
		{"it is sent with what is left of its interval", 60, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr := serveEngine(t)
			defer s.Close()
			sub := dialWire(t, addr, "sub", true, 0, 0, 1)
			sub.subscribe(1, "t/x", 1)
			pub := dialWire(t, addr, "pub", true, 0, 0, 0)

			pub.publish(1, "t/x", []byte("first"), 1)
			first := sub.expectPublish("the delivery that fills the window")

			cp := pahopackets.NewControlPacket(pahopackets.PUBLISH)
			p := cp.Content.(*pahopackets.Publish)
			p.Topic, p.Payload, p.QoS, p.PacketID = "t/x", []byte("second"), 1, 2
			expiry := tc.expiry
			p.Properties = &pahopackets.Properties{MessageExpiry: &expiry}
			published := time.Now()
			pub.send(cp)
			// **Withheld is read off the broker**, where a quiet 300ms said
			// only that nothing had arrived yet.
			cl, ok := s.Clients.Get("sub")
			require.True(t, ok)
			withheld := func() int { return len(cl.State.Inflight.GetAll(true)) }
			waitFor(t, func() bool { return withheld() == 1 }, "the second delivery to be withheld for the window")

			if !tc.sent {
				// Its interval passes, and the sweep takes it out.
				for deadline := time.Now().Add(10 * time.Second); withheld() != 0; time.Sleep(10 * time.Millisecond) {
					require.True(t, time.Now().Before(deadline),
						"a withheld delivery was still held 10s after its 1s interval passed")
				}
				sub.ack(first)
				// The end marker: published after the window opened, so a
				// withheld "second" still owed would be written ahead of it.
				pub.publish(3, "t/x", []byte("third"), 1)
				got := sub.expectPublish("the delivery published after the window opened")
				require.Equal(t, "third", string(got.Payload),
					"a message whose Message Expiry Interval had passed while it was withheld was sent")
				return
			}
			// The clock is the input here: the interval left is what the
			// wait took from it, so the wait is at least two whole seconds.
			time.Sleep(time.Until(published.Add(2500 * time.Millisecond)))
			sub.ack(first)
			got := sub.read(1500 * time.Millisecond)
			require.NotNil(t, got, "the withheld delivery was never sent once the window opened")
			second, ok := got.Content.(*pahopackets.Publish)
			require.True(t, ok, "the broker sent %v", got)
			require.Equal(t, "second", string(second.Payload))
			require.NotNil(t, second.Properties.MessageExpiry, "the delivery lost its Message Expiry Interval")
			require.LessOrEqual(t, *second.Properties.MessageExpiry, tc.expiry-2,
				"the delivery waited 2.5s and was sent with %ds of its %ds left", *second.Properties.MessageExpiry, tc.expiry)
		})
	}
}
