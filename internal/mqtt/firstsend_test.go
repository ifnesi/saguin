package mqtt

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// A Message Expiry Interval past is not forwarded [MQTT-3.3.2-5]: a delivery
// with nothing of its interval left is never put on the wire for the first
// time, on any of the paths that put one there - the withheld drain, the
// write loop's queue, and a resumed session's resend of what it never sent -
// while one already sent is owed its re-send until acknowledged (MQTT 5
// 4.3.3). Each test reads what reached the client's socket: a PINGRESP is
// written after the work under test, so the first packet the client reads
// says whether anything went before it.

// firstSendRig is a client on one end of a pipe, its peer read by one
// goroutine into the packets it received, and the hooks counted.
type firstSendRig struct {
	s       *Server
	cl      *Client
	peer    net.Conn
	got     chan byte
	dropped atomic.Int32
	done    atomic.Int32
	unsent  atomic.Int32
}

type countDropped struct {
	HookBase
	rig *firstSendRig
}

func (h *countDropped) ID() string { return "count-dropped" }
func (h *countDropped) Provides(b byte) bool {
	return b == OnQosDropped || b == OnDeliveryDone || b == OnDeliveryUnsent
}
func (h *countDropped) OnDeliveryUnsent(*Client, packets.Packet)         { h.rig.unsent.Add(1) }
func (h *countDropped) OnQosDropped(*Client, packets.Packet)             { h.rig.dropped.Add(1) }
func (h *countDropped) OnDeliveryDone(*Client, packets.Packet)           { h.rig.done.Add(1) }
func (h *countDropped) OnDeliveryReleased(*Client, packets.Packet) error { return nil }

func newFirstSendRig(t *testing.T, receiveMaximum int32) *firstSendRig {
	t.Helper()
	s := New(&Options{Logger: logger})
	rig := &firstSendRig{s: s, got: make(chan byte, 64)}
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	require.NoError(t, s.AddHook(&countDropped{rig: rig}, nil))
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
	rig.cl, rig.peer = s.NewClient(conn, "t", "fs", false), peer
	rig.cl.Properties.ProtocolVersion = 5
	rig.cl.State.Inflight.ResetSendQuota(receiveMaximum)
	rig.cl.armWriter()
	go func() {
		for {
			var fh [2]byte
			if _, err := io.ReadFull(peer, fh[:]); err != nil {
				return
			}
			n := int(fh[1]) // every packet here is under 128 bytes
			if _, err := io.ReadFull(peer, make([]byte, n)); err != nil {
				return
			}
			rig.got <- fh[0] & 0xF0
		}
	}()
	return rig
}

// first is the type of the first packet the client read before the marker,
// or the marker's.
func (r *firstSendRig) first(t *testing.T) byte {
	t.Helper()
	require.NoError(t, r.cl.WritePacket(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingresp}}))
	select {
	case b := <-r.got:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("the client read nothing, not even the marker")
	}
	return 0
}

func expiring(id uint16, expiry int64) packets.Packet {
	return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID: id, ProtocolVersion: 5, TopicName: "t", Payload: []byte("m"),
		Created: time.Now().Unix() - 1, Expiry: expiry}
}

// The withheld drain: a delivery whose interval has run out while it waited
// for the client's window is retired, not written, and holds no slot.
func TestAWithheldDeliveryWithNothingOfItsExpiryLeftIsNotSent(t *testing.T) {
	rig := newFirstSendRig(t, 1)
	stillEarly(t)                        // so it is the claim that finds it expired, not the last check
	pk := expiring(1, time.Now().Unix()) // nothing of it remains
	rig.cl.State.Inflight.Set(pk)
	rig.cl.State.Inflight.Withhold(1)

	rig.cl.drainWithheld()
	require.Equal(t, byte(0xD0), rig.first(t), "the client was sent a delivery with no expiry left")
	_, inFlight := rig.cl.State.Inflight.Get(1)
	require.False(t, inFlight, "the expired delivery is still in flight")
	require.Equal(t, int32(1), rig.cl.State.Inflight.SendQuota(), "the window lost or gained a slot")
	require.Equal(t, int32(1), rig.done.Load(), "OnDeliveryDone was not told exactly once")
	require.Equal(t, int32(1), rig.dropped.Load(), "OnQosDropped was not told exactly once")
}

// The write loop's queue: a delivery queued, never written, that the expiry
// sweep retires is not then written from the queue, and the slot it took
// comes back once.
func TestAQueuedDeliveryTheSweepRetiredIsNotWritten(t *testing.T) {
	rig := newFirstSendRig(t, 2)
	release := make(chan struct{})
	began := make(chan struct{}, 1)
	hold := func(cl *Client) {
		if cl == rig.cl {
			began <- struct{}{}
			<-release
		}
	}
	writeLoopBegins.Store(&hold)
	t.Cleanup(func() { writeLoopBegins.Store(nil) })

	now := time.Now().Unix()
	pk := expiring(1, now+1)
	pk.Origin = "publisher"
	out, err := rig.s.publishToClient(rig.cl, packets.Subscription{Filter: "t", Qos: 1}, pk)
	require.NoError(t, err)
	<-began
	_, inFlight := rig.cl.State.Inflight.Get(out.PacketID)
	require.True(t, inFlight, "the delivery was not put in flight, so this proves nothing")
	require.Equal(t, int32(1), rig.cl.State.Inflight.SendQuota(), "the delivery did not take its slot")

	// The sweep, once its interval has passed and before the loop writes.
	require.Equal(t, []uint16{out.PacketID}, rig.cl.ClearExpiredInflights(now+5, 0),
		"the sweep did not retire the queued delivery, so this proves nothing")
	close(release)
	// The loop has taken the copy and written it or not: the marker goes after.
	require.Eventually(t, func() bool { return atomic.LoadInt32(&rig.cl.State.outboundQty) == 0 },
		5*time.Second, time.Millisecond, "the write loop never took the queued copy")

	require.Equal(t, byte(0xD0), rig.first(t), "the client was sent a delivery the sweep had retired")
	require.Equal(t, int32(2), rig.cl.State.Inflight.SendQuota(), "the slot did not come back exactly once")
}

// The race the claim closes: the sweep runs between the drain claiming a
// withheld delivery and writing it. A delivery the client is sent is in
// flight, and the window counts it once.
func TestTheSweepCannotRetireADeliveryBeingFirstSent(t *testing.T) {
	rig := newFirstSendRig(t, 1)
	now := time.Now().Unix()
	rig.cl.State.Inflight.Set(expiring(1, now+3600))
	rig.cl.State.Inflight.Withhold(1)
	var swept []uint16
	var once sync.Once
	between := func(cl *Client) {
		once.Do(func() { swept = cl.ClearExpiredInflights(now+7200, 0) })
	}
	firstSendClaimed.Store(&between)
	t.Cleanup(func() { firstSendClaimed.Store(nil) })

	rig.cl.drainWithheld()
	sent := rig.first(t)
	_, inFlight := rig.cl.State.Inflight.Get(1)
	t.Logf("swept %v; the client read %#x first; in flight %v; send quota %d", swept, sent, inFlight,
		rig.cl.State.Inflight.SendQuota())
	require.Equal(t, byte(0x30), sent, "the delivery claimed for writing was not written")
	require.True(t, inFlight, "the client was sent a delivery the sweep retired as it was written")
	require.Empty(t, swept, "the sweep retired a delivery being written")
	require.Equal(t, int32(0), rig.cl.State.Inflight.SendQuota(),
		"the window does not count the one delivery on the wire exactly once")
}

// A resumed session's resend of what it never sent: one withheld before the
// disconnect whose interval has run out is retired, not sent.
func TestAResumedSessionDoesNotFirstSendAnExpiredDelivery(t *testing.T) {
	rig := newFirstSendRig(t, 2)
	stillEarly(t) // so it is the claim that finds it expired, not the last check
	rig.cl.State.Inflight.Set(expiring(1, time.Now().Unix()))
	rig.cl.State.Inflight.Withhold(1)

	require.NoError(t, rig.cl.resendInflight(rig.cl.State.Inflight.GetAll(false)))
	require.Equal(t, byte(0xD0), rig.first(t), "the resumed client was first sent an expired delivery")
	_, inFlight := rig.cl.State.Inflight.Get(1)
	require.False(t, inFlight, "the expired delivery is still in flight")
	require.Equal(t, int32(2), rig.cl.State.Inflight.SendQuota(), "the window lost or gained a slot")
	require.Equal(t, int32(1), rig.done.Load(), "OnDeliveryDone was not told exactly once")
}

// One sent to a resumable session is not expired: the sweep leaves it in
// flight, owed its re-send (RFC 0005). And the sweep's own rule: a never-sent
// delivery expires at its deadline, with nothing of its interval left, not a
// second after.
func TestTheSweepExpiresOnlyWhatWasNeverSent(t *testing.T) {
	rig := newFirstSendRig(t, 2)
	rig.cl.Properties.Props.SessionExpiryInterval = 3600 // resumable
	now := time.Now().Unix()
	rig.cl.State.Inflight.Set(expiring(1, now+3600))
	rig.cl.State.Inflight.Withhold(1)
	rig.cl.drainWithheld()
	require.Equal(t, byte(0x30), rig.first(t), "the delivery was not sent, so this proves nothing")

	rig.cl.State.Inflight.Set(expiring(2, now+3600))
	rig.cl.State.Inflight.Withhold(2)
	deleted := rig.cl.ClearExpiredInflights(now+3600, 0)
	require.Equal(t, []uint16{2}, deleted,
		"the sweep did not retire exactly the never-sent delivery at its deadline")
	_, inFlight := rig.cl.State.Inflight.Get(1)
	require.True(t, inFlight, "the sweep expired a delivery already sent")
}

// The last check, right before encoding: a deadline that passes between the
// claim and the write is not sent either, and its writer - which holds the
// claim - retires it and gives back the slot it took, once.
func TestADeadlinePassingAfterTheClaimIsNotSent(t *testing.T) {
	rig := newFirstSendRig(t, 1)
	now := time.Now().Unix()
	rig.cl.State.Inflight.Set(expiring(1, now+60))
	rig.cl.State.Inflight.Withhold(1)
	later := func() int64 { return now + 120 }
	firstSendClock.Store(&later)
	t.Cleanup(func() { firstSendClock.Store(nil) })

	rig.cl.drainWithheld()
	require.Equal(t, byte(0xD0), rig.first(t), "the client was sent a delivery whose deadline passed before it was encoded")
	_, inFlight := rig.cl.State.Inflight.Get(1)
	require.False(t, inFlight, "the expired delivery is still in flight")
	require.Equal(t, int32(1), rig.cl.State.Inflight.SendQuota(), "the slot it took did not come back exactly once")
	require.Equal(t, int32(1), rig.done.Load(), "OnDeliveryDone was not told exactly once")
}

// stillEarly has writeFirstSend's last check read a time long before any
// deadline here, so a test of the claim's own check cannot pass on it.
func stillEarly(t *testing.T) {
	early := func() int64 { return 1 }
	firstSendClock.Store(&early)
	t.Cleanup(func() { firstSendClock.Store(nil) })
}

// MQTT-4.3.3-7: once a QoS 2 PUBLISH has been sent, its sender does not apply
// message expiry - whatever the session, here one that ends with its
// connection. The sweep long past the deadline leaves it in flight, holding
// its identifier and its slot, and the client's PUBREC is answered with a
// PUBREL and its PUBCOMP completes it (RFC 0005,
// saguin_deliveries_expired_total).
func TestASentQoS2DeliveryIsNotExpiredAndItsExchangeCompletes(t *testing.T) {
	rig := newFirstSendRig(t, 1)
	require.True(t, rig.cl.EndsWithConnection(), "the session outlives its connection, so this proves less")
	now := time.Now().Unix()
	pk := expiring(1, now+60)
	pk.FixedHeader.Qos = 2
	rig.cl.State.Inflight.Set(pk)
	rig.cl.State.Inflight.Withhold(1)
	rig.cl.drainWithheld()
	require.Equal(t, byte(0x30), rig.first(t), "the QoS 2 delivery was not sent, so this proves nothing")
	require.Equal(t, byte(0xD0), <-rig.got, "the marker did not follow the delivery")

	swept := rig.cl.ClearExpiredInflights(now+3600, 0)
	_, inFlight := rig.cl.State.Inflight.Get(1)
	t.Logf("swept %v an hour past the deadline; in flight %v; send quota %d", swept, inFlight,
		rig.cl.State.Inflight.SendQuota())
	require.Empty(t, swept, "the sweep expired a QoS 2 delivery already sent [MQTT-4.3.3-7]")
	require.True(t, inFlight, "the sent delivery left the table, so its PUBREC names nothing")
	require.Equal(t, int32(0), rig.cl.State.Inflight.SendQuota(), "the slot was given back while the exchange was open")
	require.Zero(t, rig.unsent.Load()+rig.dropped.Load(), "a hook was told the delivery was dropped")

	require.NoError(t, rig.s.processPacket(rig.cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: 1, ProtocolVersion: 5}))
	require.Equal(t, byte(0x60), rig.first(t), "the client's PUBREC was not answered with a PUBREL")
	require.Equal(t, byte(0xD0), <-rig.got, "the marker did not follow the PUBREL")
	require.NoError(t, rig.s.processPacket(rig.cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}, PacketID: 1, ProtocolVersion: 5}))
	_, inFlight = rig.cl.State.Inflight.Get(1)
	require.False(t, inFlight, "the PUBCOMP did not complete the exchange")
	require.Equal(t, int32(1), rig.cl.State.Inflight.SendQuota(), "the completed exchange did not give its slot back once")
}

// A delivery that expires in the write loop's queue, never written, is
// reported as unsent - the one call that says the client never had it, which
// a queue's job is given back on (OnDeliveryUnsent) - once, and not written.
func TestADeliveryExpiredAtItsQueuedClaimIsReportedUnsent(t *testing.T) {
	rig := newFirstSendRig(t, 2)
	pk := expiring(1, 0)
	pk.Origin = "publisher"
	// Older than the server holds any message, so the write loop's claim
	// finds it expired with no clock to wait for.
	pk.Created = time.Now().Unix() - rig.s.Options.Capabilities.MaximumMessageExpiryInterval - 10
	out, err := rig.s.publishToClient(rig.cl, packets.Subscription{Filter: "t", Qos: 1}, pk)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&rig.cl.State.outboundQty) == 0 },
		5*time.Second, time.Millisecond, "the write loop never took the queued copy")
	require.Equal(t, byte(0xD0), rig.first(t), "the client was sent a delivery that expired in its queue")
	_, inFlight := rig.cl.State.Inflight.Get(out.PacketID)
	t.Logf("in flight %v; unsent %d, done %d, dropped %d; send quota %d", inFlight, rig.unsent.Load(),
		rig.done.Load(), rig.dropped.Load(), rig.cl.State.Inflight.SendQuota())
	require.False(t, inFlight, "the expired delivery is still in flight")
	require.Equal(t, int32(1), rig.unsent.Load(), "OnDeliveryUnsent was not told exactly once")
	require.Equal(t, int32(2), rig.cl.State.Inflight.SendQuota(), "the slot it took did not come back exactly once")
}

// A delivery queued and never written is still never sent on the session's
// next connection: carried by a takeover's copy of the table, or left by its
// queue's closing. There it is a first send, claimed and checked for expiry -
// so one whose interval ran out is retired unsent, not sent as a DUP re-send
// [MQTT-3.3.2-5].
func TestAnUnwrittenDeliveryIsStillUnsentOnTheNextConnection(t *testing.T) {
	for _, how := range []string{"taken over", "queue closed"} {
		t.Run(how, func(t *testing.T) {
			old := newFirstSendRig(t, 2)
			pk := expiring(1, time.Now().Unix()) // nothing of it remains
			old.cl.State.Inflight.Set(pk)
			cp := pk
			cp.FirstSend = true
			old.cl.State.Inflight.Queue(&cp)

			rig := newFirstSendRig(t, 2)
			if how == "taken over" {
				rig.cl.State.Inflight = old.cl.State.Inflight.Clone()
			} else {
				old.cl.State.Inflight.Unqueue(&cp)
				rig.cl.State.Inflight = old.cl.State.Inflight
			}
			rig.cl.State.Inflight.ResetSendQuota(2)
			stillEarly(t) // so it is the claim that finds it expired, not the last check

			all := rig.cl.State.Inflight.GetAll(false)
			require.Len(t, all, 1, "nothing was carried, so this proves nothing")
			t.Logf("carried with Expiry %d", all[0].Expiry)
			require.NoError(t, rig.cl.resendInflight(all))
			require.Equal(t, byte(0xD0), rig.first(t), "the next connection was sent a delivery never sent that had expired")
			_, inFlight := rig.cl.State.Inflight.Get(1)
			require.False(t, inFlight, "the expired delivery is still in flight")
			require.Equal(t, int32(1), rig.unsent.Load(), "OnDeliveryUnsent was not told exactly once")
			require.Equal(t, int32(2), rig.cl.State.Inflight.SendQuota(), "the window lost or gained a slot")
		})
	}
}
