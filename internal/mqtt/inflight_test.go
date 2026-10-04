// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/stretchr/testify/require"
)

func TestInflightSet(t *testing.T) {
	cl, _, _ := newTestClient()

	r := cl.State.Inflight.Set(packets.Packet{PacketID: 1})
	require.True(t, r)
	require.NotNil(t, cl.State.Inflight.internal[1])
	require.NotEqual(t, 0, cl.State.Inflight.internal[1].pk.PacketID)

	r = cl.State.Inflight.Set(packets.Packet{PacketID: 1})
	require.False(t, r)
}

func TestInflightGet(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})

	msg, ok := cl.State.Inflight.Get(2)
	require.True(t, ok)
	require.NotEqual(t, 0, msg.PacketID)
}

func TestInflightGetAllAndImmediate(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Created: 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: 2})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3, Created: 3, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 4, Created: 4, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 5, Created: 5})

	require.Equal(t, []packets.Packet{
		{PacketID: 1, Created: 1},
		{PacketID: 2, Created: 2},
		{PacketID: 3, Created: 3, Expiry: -1},
		{PacketID: 4, Created: 4, Expiry: -1},
		{PacketID: 5, Created: 5},
	}, cl.State.Inflight.GetAll(false))

	require.Equal(t, []packets.Packet{
		{PacketID: 3, Created: 3, Expiry: -1},
		{PacketID: 4, Created: 4, Expiry: -1},
	}, cl.State.Inflight.GetAll(true))
}

// MQTT-4.6.0-1: re-sent PUBLISH packets must go out in the order the
// originals were sent. Created is a unix timestamp in SECONDS, so a
// session's unacknowledged window is usually one second wide and every
// packet in it compares equal, and the table is a map, whose iteration order
// Go randomises deliberately. So the order is the one they were registered
// in, whatever their identifiers - which is why these are registered out of
// identifier order: the answer is that order, not a sorted one
// (TestADeliveryKeepsItsPlaceAcrossAnIdentifierWrap has why an identifier
// cannot say it).
//
// Repeated rather than run once: a map walked eight entries at a time is a
// coin flip, and a single pass cannot tell "almost always wrong" from
// "never wrong".
func TestInflightGetAllOrdersPacketsCreatedInTheSameSecond(t *testing.T) {
	const sameSecond int64 = 1700000000
	for trial := 0; trial < 50; trial++ {
		cl, _, _ := newTestClient()
		for _, id := range []uint16{5, 3, 8, 1, 7, 2, 6, 4} {
			cl.State.Inflight.Set(packets.Packet{PacketID: id, Created: sameSecond})
		}

		got := cl.State.Inflight.GetAll(false)
		require.Len(t, got, 8)

		order := make([]uint16, 0, len(got))
		for _, pk := range got {
			order = append(order, pk.PacketID)
		}
		require.Equal(t, []uint16{5, 3, 8, 1, 7, 2, 6, 4}, order,
			"trial %d: packets created in the same second were not returned in the order they were registered", trial)
	}
}

// The comparison truncated Created to uint16 before comparing it, so two
// packets straddling a 65536-second boundary - one every 18.2 hours - were
// ordered by the remainder rather than by the timestamp: 1786970111 becomes
// 65535 and 1786970113 becomes 1, so the newer packet sorted first.
//
// The older packet is given the HIGHER identifier here on purpose. It is
// what makes this a test of the timestamp: an implementation that compared
// identifiers alone, or one that kept the truncation, puts them the other
// way round.
func TestInflightGetAllDoesNotTruncateCreated(t *testing.T) {
	const boundary int64 = 1786970112 // a multiple of 65536

	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 9, Created: boundary - 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: boundary + 1})

	got := cl.State.Inflight.GetAll(false)
	require.Len(t, got, 2)
	require.Equal(t, uint16(9), got[0].PacketID,
		"the packet created first was returned second: Created was compared as a uint16, "+
			"so it wrapped between the two")
}

func TestInflightLen(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})
	require.Equal(t, 1, cl.State.Inflight.Len())
}

func TestInflightClone(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})
	require.Equal(t, 1, cl.State.Inflight.Len())

	cloned := cl.State.Inflight.Clone()
	require.NotNil(t, cloned)
	require.NotSame(t, cloned, cl.State.Inflight)
}

func TestInflightDelete(t *testing.T) {
	cl, _, _ := newTestClient()

	cl.State.Inflight.Set(packets.Packet{PacketID: 3})
	require.NotNil(t, cl.State.Inflight.internal[3])

	r := cl.State.Inflight.Delete(3)
	require.True(t, r)
	require.Nil(t, cl.State.Inflight.internal[3])

	_, ok := cl.State.Inflight.Get(3)
	require.False(t, ok)

	r = cl.State.Inflight.Delete(3)
	require.False(t, r)
}

func TestResetReceiveQuota(t *testing.T) {
	i := NewInflights()
	require.Equal(t, int32(0), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))
	i.ResetReceiveQuota(6)
	require.Equal(t, int32(6), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(6), atomic.LoadInt32(&i.receiveQuota))
}

func TestReceiveQuota(t *testing.T) {
	i := NewInflights()
	i.receiveQuota = 4
	i.maximumReceiveQuota = 5
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(4), atomic.LoadInt32(&i.receiveQuota))

	// Return 1
	i.IncreaseReceiveQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.receiveQuota))

	// Try to go over max limit
	i.IncreaseReceiveQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.receiveQuota))

	// Reset to max 1
	i.ResetReceiveQuota(1)
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(1), atomic.LoadInt32(&i.receiveQuota))

	// Take 1
	i.DecreaseReceiveQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))

	// Try to go below zero
	i.DecreaseReceiveQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))
}

func TestResetSendQuota(t *testing.T) {
	i := NewInflights()
	require.Equal(t, int32(0), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))
	i.ResetSendQuota(6)
	require.Equal(t, int32(6), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(6), atomic.LoadInt32(&i.sendQuota))
}

func TestSendQuota(t *testing.T) {
	i := NewInflights()
	i.sendQuota = 4
	i.maximumSendQuota = 5
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(4), atomic.LoadInt32(&i.sendQuota))

	// Return 1
	i.IncreaseSendQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.sendQuota))

	// Try to go over max limit
	i.IncreaseSendQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.sendQuota))

	// Reset to max 1
	i.ResetSendQuota(1)
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(1), atomic.LoadInt32(&i.sendQuota))

	// Take 1
	i.DecreaseSendQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))

	// Try to go below zero
	i.DecreaseSendQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))
}

// TakeImmediate claims withheld packets in the order they were registered,
// and a claimed packet stays in flight rather than leaving the map: it is on
// the wire, and its acknowledgement has to find it.
func TestTakeImmediate(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Created: 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: 2})
	cl.State.Inflight.Set(packets.Packet{PacketID: 4, Created: 3, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3, Created: 3, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 5, Created: 5})

	// 4 was registered before 3 in the same second, so it goes first.
	pk, ok := takeImmediate(cl.State.Inflight)
	require.True(t, ok)
	require.Equal(t, packets.Packet{PacketID: 4, Created: 3}, pk)
	stored, _ := cl.State.Inflight.Get(4)
	require.Equal(t, int64(0), stored.Expiry, "a claimed packet is still marked withheld")

	pk, ok = takeImmediate(cl.State.Inflight)
	require.True(t, ok)
	require.Equal(t, uint16(3), pk.PacketID)

	_, ok = takeImmediate(cl.State.Inflight)
	require.False(t, ok)
	require.Equal(t, 5, cl.State.Inflight.Len(), "claiming a packet removed it from flight")
}

// A withheld packet has one owner: whichever of TakeWithheld and
// DeleteWithheld reaches it first. The loser finds nothing, so a queue
// delivery returned to its queue is never also written.
func TestAWithheldPacketIsTakenOrDeletedNeverBoth(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3})

	_, ok := cl.State.Inflight.TakeWithheld(1)
	require.True(t, ok)
	require.False(t, cl.State.Inflight.DeleteWithheld(1), "a claimed packet was deleted")

	require.True(t, cl.State.Inflight.DeleteWithheld(2))
	_, ok = cl.State.Inflight.TakeWithheld(2)
	require.False(t, ok, "a deleted packet was claimed")

	require.False(t, cl.State.Inflight.DeleteWithheld(3), "a written packet was deleted as withheld")
	_, ok = cl.State.Inflight.TakeWithheld(3)
	require.False(t, ok, "a written packet was claimed as withheld")

	cl.State.Inflight.Withhold(3)
	_, ok = cl.State.Inflight.TakeWithheld(3)
	require.True(t, ok, "Withhold did not mark the packet")
}

// The claim and the deletion race, and exactly one of them wins every time.
func TestTakeAndDeleteOfAWithheldPacketRaceToOneWinner(t *testing.T) {
	for round := 0; round < 2000; round++ {
		cl, _, _ := newTestClient()
		cl.State.Inflight.Set(packets.Packet{PacketID: 9, Expiry: -1})
		var took, deleted atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, ok := takeImmediate(cl.State.Inflight); took.Store(ok) }()
		go func() { defer wg.Done(); deleted.Store(cl.State.Inflight.DeleteWithheld(9)) }()
		wg.Wait()
		require.NotEqual(t, took.Load(), deleted.Load(), "round %d: taken %v, deleted %v",
			round, took.Load(), deleted.Load())
	}
}

// TakeImmediate must not deadlock against concurrent writers of the same map.
//
// **The interleaving is made, not hunted for.** A seam holds TakeImmediate
// inside its lock while a writer is started against the same table, and -
// were that lock a read lock - until the writer is queued behind it, which
// TryRLock failing says. Anything after that taking the lock again waits for
// ever, shared or exclusive alike. Readers and writers hammered at it for a
// second found the gap only when the scheduler put a writer in it. A deadlock
// cannot be failed politely from inside it, so the assertion is that the
// call returns at all.
func TestTakeImmediateDoesNotDeadlockAgainstAWriter(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Created: 1, Expiry: -1})

	writerDone := make(chan struct{})
	held := 0
	hold := func() {
		go func() {
			defer close(writerDone)
			cl.State.Inflight.Set(packets.Packet{PacketID: 2})
			cl.State.Inflight.Delete(2)
		}()
		for cl.State.Inflight.TryRLock() {
			cl.State.Inflight.RUnlock()
			time.Sleep(time.Millisecond)
		}
		held++
	}
	takeImmediateLocked.Store(&hold)
	defer takeImmediateLocked.Store(nil)

	took := make(chan bool, 1)
	go func() { _, ok := takeImmediate(cl.State.Inflight); took <- ok }()
	select {
	case ok := <-took:
		require.True(t, ok, "the withheld packet was not taken, so this did not reach the claim")
	case <-time.After(10 * time.Second):
		t.Fatal("TakeImmediate and a concurrent Set deadlocked on the in-flight lock")
	}
	require.Equal(t, 1, held, "TakeImmediate never reached its lock with a writer started, so this tested nothing")
	<-writerDone
}

// A session's bytes and message count follow every way an entry arrives,
// is replaced and leaves, and count PUBLISH entries alone.
func TestInflightBytesFollowEveryChange(t *testing.T) {
	i := NewInflights()
	pub := func(id uint16, payload string) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID: id, TopicName: "a/b", Payload: []byte(payload), Origin: "p"}
	}
	i.Set(pub(1, "12345"))
	i.Set(pub(2, "1"))
	i.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel}, PacketID: 3})
	want := InflightTableCost + inflightSize(pub(1, "12345")) + inflightSize(pub(2, "1"))
	require.Equal(t, want, i.Bytes())
	require.Equal(t, int64(2), i.Messages())

	i.Set(pub(2, "123")) // replaced, not added
	want = InflightTableCost + inflightSize(pub(1, "12345")) + inflightSize(pub(2, "123"))
	require.Equal(t, want, i.Bytes())
	require.Equal(t, int64(2), i.Messages())

	c := i.Clone()
	require.Equal(t, want, c.Bytes(), "a session taken over lost its accounting")

	i.Withhold(1)
	require.Equal(t, want, i.Bytes(), "withholding changed what an entry weighs")
	require.True(t, i.DeleteWithheld(1))
	require.True(t, i.Delete(2))
	require.True(t, i.Delete(3))
	require.Equal(t, int64(0), i.Bytes())
	require.Equal(t, int64(0), i.Messages())
}

// DropOldest takes back only what is safe to, oldest first, and never the
// entry just queued.
func TestDropOldestTakesOnlyWhatIsSafeToTakeBack(t *testing.T) {
	entry := func(id uint16, created int64, qos byte, expiry int64, origin string) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
			PacketID: id, Created: created, Expiry: expiry, TopicName: "t", Payload: []byte("x"),
			Origin: origin}
	}
	one := inflightSize(entry(1, 0, 1, 0, "p"))

	t.Run("online", func(t *testing.T) {
		i := NewInflights()
		i.Set(entry(1, 1, 1, 0, "p"))             // on the wire: stays
		i.Set(entry(2, 2, 2, -1, "p"))            // QoS 2: stays
		i.Set(entry(3, 3, 1, -1, ""))             // saguin's own record: stays
		i.Set(entry(4, 4, 1, -1, InlineClientId)) // a queue's offer: stays
		i.Set(entry(5, 5, 1, -1, "p"))            // withheld: may go
		i.Set(entry(6, 6, 1, -1, "p"))            // withheld: may go
		i.Set(entry(7, 7, 1, -1, "p"))            // the one just queued: stays
		dropped := i.DropOldest(InflightTableCost+5*one, 7, false)
		var ids []uint16
		for _, d := range dropped {
			ids = append(ids, d.PacketID)
		}
		require.Equal(t, []uint16{5, 6}, ids)
		require.Equal(t, int64(5), i.Messages())
		require.Equal(t, InflightTableCost+5*one, i.Bytes())
	})

	t.Run("offline", func(t *testing.T) {
		i := NewInflights()
		i.Set(entry(1, 1, 1, 0, "p")) // sent before the client went: may go now
		i.Set(entry(2, 2, 2, 0, "p")) // QoS 2: stays
		i.Set(entry(3, 3, 1, 0, "p"))
		i.Set(entry(4, 4, 1, 0, "p"))
		dropped := i.DropOldest(InflightTableCost+2*one, 4, true)
		require.Len(t, dropped, 2)
		require.Equal(t, uint16(1), dropped[0].PacketID, "the oldest did not go first")
		require.Equal(t, uint16(3), dropped[1].PacketID)
	})

	t.Run("nothing safe to take", func(t *testing.T) {
		i := NewInflights()
		i.Set(entry(1, 1, 2, 0, "p"))
		i.Set(entry(2, 2, 2, 0, "p"))
		require.Empty(t, i.DropOldest(0, 2, true))
		require.Equal(t, int64(2), i.Messages())
	})
}

// The expiry sweep reads what has expired under one lock and deletes under
// another, so an expired entry can be acknowledged in between and its packet
// identifier claimed by a new delivery. Deleting by identifier alone would
// then remove the new one: a live delivery gone from the table and never
// retried. RetireExpired asks again, and a new entry has not expired.
func TestTheExpirySweepLeavesADeliveryThatReusedAnExpiredIdentifier(t *testing.T) {
	i := NewInflights()
	now := time.Now().Unix()
	i.Set(packets.Packet{PacketID: 7, ProtocolVersion: 5, Expiry: now - 10, Created: now - 20,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})
	i.Withhold(7) // never sent: only that expires (neverSent)

	expired := i.Expired(now, 0)
	require.Len(t, expired, 1, "the entry past its expiry was not reported, so nothing below means anything")

	// Acknowledged, and identifier 7 taken by a fresh delivery, between the
	// sweep's read and its delete.
	require.True(t, i.Delete(7))
	i.Set(packets.Packet{PacketID: 7, ProtocolVersion: 5, Expiry: now + 3600, Created: now,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})

	retired, _ := i.RetireExpired(expired[0].PacketID, now, 0)
	require.False(t, retired,
		"the sweep deleted the delivery that reused an expired identifier")
	_, ok := i.Get(7)
	require.True(t, ok, "the fresh delivery under identifier 7 is gone from the in-flight table")

	// And the converse, or the rule is satisfied by never deleting.
	i.Set(packets.Packet{PacketID: 8, ProtocolVersion: 5, Expiry: now - 1, Created: now - 5,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})
	i.Withhold(8)
	retired, _ = i.RetireExpired(8, now, 0)
	require.True(t, retired, "an entry still expired was not deleted")
}

// An identifier leaves the table and stays unavailable to Claim until it is
// Unclaimed, by each of the three ways a delivery the server sent is taken
// out: Retire, RetireExpired and DropOldest. Claim scans from the identifier
// before the retired one, so the retired one is the first it would give out.
func TestARetiredIdentifierIsNotClaimedUntilUnclaimed(t *testing.T) {
	publish := func(id uint16, created int64) packets.Packet {
		return packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    id, Created: created, Expiry: -1, Origin: "someone",
			Payload: make([]byte, 64),
		}
	}
	for _, c := range []struct {
		name   string
		retire func(i *Inflight) bool
	}{
		{"Retire", func(i *Inflight) bool { return i.Retire(5) }},
		{"RetireExpired", func(i *Inflight) bool {
			m := publish(5, 1)
			m.ProtocolVersion, m.Expiry = 5, 2 // expired at 3; only MQTT 5 carries one
			i.Set(m)
			i.Withhold(5) // never sent: only that expires (neverSent)
			retired, _ := i.RetireExpired(5, 3, 0)
			return retired
		}},
		{"DropOldest", func(i *Inflight) bool {
			return len(i.DropOldest(0, 0, true)) == 1
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			i := NewInflights()
			i.Set(publish(5, 1))
			require.True(t, c.retire(i), "nothing was retired, so this proves nothing")
			_, inFlight := i.Get(5)
			require.False(t, inFlight, "the entry is still in the table")

			got, ok := i.Claim(4, 10)
			require.True(t, ok)
			require.NotEqual(t, uint32(5), got, "a retired identifier was given out before it was Unclaimed")

			i.Unclaim(5)
			got, ok = i.Claim(4, 10)
			require.True(t, ok)
			require.Equal(t, uint32(5), got, "an Unclaimed identifier was never given out again")
		})
	}
}

// The order of a session's deliveries is the order they were registered, and
// neither a packet identifier nor a creation time in whole seconds says it.
// Identifiers wrap at 65,535 - every 65,535 deliveries to one client - and
// a connection that takes over a session starts its counter again, so within
// one second a later delivery can hold a lower identifier. Ordered by
// identifier, the later one was written first [MQTT-4.6.0-1], re-sent first,
// and kept when the bound gave up the oldest - so a device holding stale
// data was sent it last, and the newest was the one given up.
func TestADeliveryKeepsItsPlaceAcrossAnIdentifierWrap(t *testing.T) {
	const second int64 = 1_700_000_000
	entry := func(id uint16, expiry int64) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID: id, Created: second, Expiry: expiry, TopicName: "t", Payload: []byte("x"),
			Origin: "p"}
	}
	registered := []uint16{65534, 65535, 1, 2}
	ids := func(ps []packets.Packet) []uint16 {
		var out []uint16
		for _, p := range ps {
			out = append(out, p.PacketID)
		}
		return out
	}

	t.Run("written", func(t *testing.T) {
		i := NewInflights()
		for _, id := range registered {
			i.Set(entry(id, -1))
		}
		var got []uint16
		for {
			pk, ok := takeImmediate(i)
			if !ok {
				break
			}
			got = append(got, pk.PacketID)
		}
		require.Equal(t, registered, got, "withheld deliveries were written out of the order they were made")
	})

	t.Run("re-sent", func(t *testing.T) {
		i := NewInflights()
		for _, id := range registered {
			i.Set(entry(id, 0))
		}
		require.Equal(t, registered, ids(i.GetAll(false)), "a re-sent window came back out of order")
	})

	t.Run("given up", func(t *testing.T) {
		for _, offline := range []bool{false, true} {
			i := NewInflights()
			expiry := int64(-1)
			if offline {
				expiry = 0
			}
			for _, id := range registered {
				i.Set(entry(id, expiry))
			}
			one := inflightSize(entry(1, 0))
			dropped := i.DropOldest(InflightTableCost+2*one, 0, offline)
			require.Equal(t, registered[:2], ids(dropped),
				"offline=%v: the bound gave up the newest rather than the oldest", offline)
		}
	})

	t.Run("taken over", func(t *testing.T) {
		old := NewInflights()
		old.Set(entry(50, -1))
		old.Set(entry(51, -1))
		i := old.Clone()
		i.Set(entry(1, -1)) // the new connection's counter started again
		i.Set(entry(2, -1))
		pk, ok := takeImmediate(i)
		require.True(t, ok)
		require.Equal(t, uint16(50), pk.PacketID, "the copied session wrote a new delivery before an older one")
		one := inflightSize(entry(1, 0))
		require.Equal(t, []uint16{51}, ids(i.DropOldest(InflightTableCost+3*one, 0, false)),
			"the copied session gave up a new delivery before an older one")
	})
}

// The in-flight table against a model of it - a list in registration order -
// under random registrations, updates, writes, withholds, removals, give-ups
// and copies. After every step the order, the withheld order, what each call
// chose, and every count must agree with the model, and the order must not
// grow past what it indexes.
func TestTheInFlightTableKeepsItsOrderUnderAnyMixOfChanges(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("seed %d (SAGUIN_RANDOM_SEED)", seed)
	rng := rand.New(rand.NewSource(seed))

	type held struct{ pk packets.Packet }
	var model []held // in registration order
	find := func(id uint16) int {
		for k, h := range model {
			if h.pk.PacketID == id {
				return k
			}
		}
		return -1
	}
	counted := func(h held) int64 {
		if h.pk.FixedHeader.Type != packets.Publish {
			return 0
		}
		return inflightSize(h.pk)
	}
	// total is what the table is charged for the model's entries: each, and
	// InflightTableCost once while it holds any.
	total := func(model []held) int64 {
		var n int64
		for _, h := range model {
			n += counted(h)
		}
		if n > 0 {
			n += InflightTableCost
		}
		return n
	}
	origins := []string{"p", "p", "p", "", InlineClientId}
	fresh := func(id uint16) packets.Packet {
		qos := byte(1)
		if rng.Intn(5) == 0 {
			qos = 2
		}
		expiry := int64(0)
		if rng.Intn(2) == 0 {
			expiry = -1
		}
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
			PacketID: id, Created: 1_700_000_000, Expiry: expiry, TopicName: "t",
			Payload: make([]byte, rng.Intn(64)), Origin: origins[rng.Intn(len(origins))]}
	}
	ids := func(ps []packets.Packet) []uint16 {
		out := []uint16{}
		for _, p := range ps {
			out = append(out, p.PacketID)
		}
		return out
	}

	i := NewInflights()
	steps, choices := 0, 0
	for step := 0; step < 20000; step++ {
		steps++
		id := uint16(1 + rng.Intn(24))
		k := find(id)
		switch op := rng.Intn(11); {
		case op <= 2 && k < 0: // a new delivery under a free identifier
			m := fresh(id)
			i.Set(m)
			model = append(model, held{m})
		case op == 3 && k >= 0: // the same delivery registered again, as withheld
			m := model[k].pk
			m.Expiry = -1
			i.Set(m)
			model[k] = held{m}
		case op == 4 && k >= 0 && model[k].pk.Expiry >= 0: // put back
			i.Withhold(id)
			model[k].pk.Expiry = -1
		case op == 5: // the writer takes the earliest withheld
			pk, ok := takeImmediate(i)
			want := -1
			for n, h := range model {
				if h.pk.Expiry < 0 {
					want = n
					break
				}
			}
			require.Equal(t, want >= 0, ok, "step %d: TakeImmediate found %v, the model %v", step, ok, want >= 0)
			if ok {
				choices++
				require.Equal(t, model[want].pk.PacketID, pk.PacketID, "step %d: wrote out of order", step)
				model[want].pk.Expiry = 0
				i.Claimed()
			}
		case op == 6 && k >= 0: // taken for writing by identifier
			_, ok := i.TakeWithheld(id)
			require.Equal(t, model[k].pk.Expiry < 0, ok, "step %d", step)
			if ok {
				model[k].pk.Expiry = 0
			}
		case op == 7 && k >= 0: // deleted while withheld
			ok := i.DeleteWithheld(id)
			require.Equal(t, model[k].pk.Expiry < 0, ok, "step %d", step)
			if ok {
				model = append(model[:k], model[k+1:]...)
			}
		case op == 8 && k >= 0: // acknowledged
			require.True(t, i.Retire(id))
			i.Unclaim(id)
			model = append(model[:k], model[k+1:]...)
		case op == 9: // the bound gives up the oldest it may
			offline := rng.Intn(3) == 0
			keep := uint16(1 + rng.Intn(24))
			max := total(model) - int64(rng.Intn(3000))
			var want []uint16
			for total(model) > max {
				n := -1
				for x, h := range model {
					if h.pk.PacketID != keep && droppable(&h.pk, offline) {
						n = x
						break
					}
				}
				if n < 0 {
					break
				}
				want = append(want, model[n].pk.PacketID)
				model = append(model[:n], model[n+1:]...)
			}
			got := ids(i.DropOldest(max, keep, offline))
			require.Equal(t, append([]uint16{}, want...), got, "step %d: gave up %v, the model %v", step, got, want)
			choices += len(got)
			for _, g := range got {
				i.Unclaim(g)
			}
		case op == 10: // a takeover
			i = i.Clone()
		}

		want, withheld := []uint16{}, []uint16{}
		var msgs int64
		for _, h := range model {
			want = append(want, h.pk.PacketID)
			if h.pk.Expiry < 0 {
				withheld = append(withheld, h.pk.PacketID)
			}
			if h.pk.FixedHeader.Type == packets.Publish {
				msgs++
			}
		}
		require.Equal(t, want, ids(i.GetAll(false)), "step %d: the order", step)
		require.Equal(t, withheld, ids(i.GetAll(true)), "step %d: the withheld order", step)
		require.Equal(t, total(model), i.Bytes(), "step %d: bytes", step)
		require.Equal(t, msgs, i.Messages(), "step %d: messages", step)
		i.RLock()
		require.Equal(t, int64(len(withheld)), i.withheld, "step %d: the withheld count", step)
		require.LessOrEqual(t, len(i.order.slots)-i.order.head, 2*len(i.internal)+64+1, "step %d: the order outgrew its table", step)
		for _, e := range i.internal {
			require.True(t, e.pk.Expiry >= 0 || e.queued, "step %d: withheld %d has no place in the queue", step, e.pk.PacketID)
		}
		slots := map[uint64]int{}
		for _, sl := range i.waiting.slots[i.waiting.head:] {
			if _, ok := i.live(sl); ok {
				slots[sl.seq]++
				require.Equal(t, 1, slots[sl.seq], "step %d: entry %d has two places in the queue", step, sl.id)
			}
		}
		i.RUnlock()
	}
	if choices < 1000 {
		t.Fatalf("only %d writes and give-ups were checked in %d steps, so the order was hardly asked", choices, steps)
	}
	t.Logf("%d steps, %d writes and give-ups checked against the model", steps, choices)
}

// A full identifier table is known before any search. A client whose Receive
// Maximum is the identifier space held all 65,535 in flight, and every
// delivery tried meanwhile scanned them all under the table's lock before
// failing: 1.69 ms a claim, and a replay at half its rate (the 2026-09-27
// fleet rerun's replay A/B). A failed claim costs nothing now, and a freed
// identifier is still found.
func TestAClaimOnAFullTableDoesNotSearchIt(t *testing.T) {
	i := NewInflights()
	for id := uint16(1); id != 0; id++ {
		i.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id})
	}
	if n := i.Len(); n != 65535 {
		t.Fatalf("the table holds %d entries, want every identifier, so this proves nothing", n)
	}
	const tries = 1000
	start := time.Now()
	for range tries {
		if _, ok := i.Claim(1, 65535); ok {
			t.Fatal("an identifier was claimed from a full table")
		}
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("%d failed claims on a full table took %v: the table is being searched", tries, d)
	}
	i.Delete(40000)
	if id, ok := i.Claim(1, 65535); !ok || id != 40000 {
		t.Errorf("with identifier 40000 freed, the claim got %d (%v), want 40000", id, ok)
	}
}

// "packet identifiers exhausted" is said once an episode: the first failure
// since the client last got an identifier. A replay logged it 79,504 times.
func TestIdentifierExhaustionIsSaidOnceAnEpisode(t *testing.T) {
	cl, _, _ := newTestClient() // its identifiers run 1 to 10
	for id := uint16(1); id <= 10; id++ {
		cl.State.Inflight.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id})
	}
	for n := range 3 {
		if _, err := cl.NextPacketID(); err == nil {
			t.Fatal("an identifier was claimed from a full table, so this proves nothing")
		}
		if first := cl.Exhausted(); first != (n == 0) {
			t.Errorf("failure %d reported first-of-episode=%v, want %v", n+1, first, n == 0)
		}
	}
	cl.State.Inflight.Delete(7)
	if _, err := cl.NextPacketID(); err != nil {
		t.Fatalf("with an identifier freed the claim failed: %v", err)
	}
	cl.State.Inflight.Delete(8)
	cl.State.Inflight.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 8})
	if _, err := cl.NextPacketID(); err == nil {
		t.Fatal("the table was full again and a claim succeeded")
	}
	if !cl.Exhausted() {
		t.Error("the first failure after a successful claim was not reported as a new episode")
	}
}

// A withheld entry keeps its deadline by every way an entry is withheld -
// Withhold (a delivery the queue had no room for, or held behind a resend),
// WithholdAgain (a resumed session's re-send the window does not admit), and
// a Set of the entry with the withheld marker (makeDelivery) - and across a
// takeover's Clone. The expiry sweep reads it, and the claim that writes the
// entry puts it back, so what is written carries what is left of its Message
// Expiry Interval [MQTT-3.3.2-5] [MQTT-3.3.2-6]
// (TestAWithheldDeliveryKeepsItsMessageExpiry has the same on the wire).
func TestAWithheldEntryKeepsItsDeadline(t *testing.T) {
	const deadline = 1000
	for _, how := range []string{"Withhold", "WithholdAgain", "Set", "Clone"} {
		for _, claim := range []string{"TakeImmediate", "TakeWithheld"} {
			t.Run(how+", "+claim, func(t *testing.T) {
				i := NewInflights()
				pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
					PacketID: 7, ProtocolVersion: 5, Created: deadline - 10, Expiry: deadline}
				i.Set(pk)
				switch how {
				case "Withhold":
					i.Withhold(7)
				case "WithholdAgain":
					i.WithholdAgain(7)
				case "Set", "Clone":
					held := pk
					held.Expiry = -1
					i.Set(held)
				}
				if how == "Clone" {
					i = i.Clone()
				}
				m, _ := i.Get(7)
				require.Negative(t, m.Expiry, "the entry was not withheld, so this tests nothing")

				// Expired when nothing remains of its interval, at its
				// deadline as after it (expiredAt).
				require.Empty(t, i.Expired(deadline-1, 0), "an entry not yet at its deadline read as expired")
				gone := i.Expired(deadline, 0)
				require.Len(t, gone, 1, "a withheld entry past its deadline was not found by the expiry sweep")
				require.Equal(t, int64(deadline), gone[0].Expiry, "the sweep was not given the entry's deadline")

				var got packets.Packet
				var ok bool
				if claim == "TakeImmediate" {
					got, ok = takeImmediate(i)
				} else {
					got, ok = i.TakeWithheld(7)
				}
				require.True(t, ok, "the withheld entry could not be claimed")
				require.Equal(t, int64(deadline), got.Expiry,
					"the claim wrote over the entry's deadline, so it would be sent with its whole interval")
				m, _ = i.Get(7)
				require.Equal(t, int64(deadline), m.Expiry)
			})
		}
	}
}

// takeImmediate claims the earliest withheld delivery as drainWithheld does,
// with no expiry rule in force.
func takeImmediate(i *Inflight) (packets.Packet, bool) {
	pk, how := i.TakeImmediate(0, 0, 0)
	return pk, how == FirstSendWrite
}
