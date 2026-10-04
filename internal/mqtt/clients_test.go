// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/mqtt/system"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/require"
)

const pkInfo = "packet type %v, %s"

var errClientStop = errors.New("test stop")

func newTestClient() (cl *Client, r net.Conn, w net.Conn) {
	r, w = net.Pipe()

	cl = newClient(w, &ops{
		info:  new(system.Info),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{
			Capabilities: &Capabilities{
				ReceiveMaximum:             10,
				TopicAliasMaximum:          10000,
				MaximumClientWritesPending: 3,
				maximumPacketID:            10,
			},
		},
	})

	cl.ID = "mochi"
	cl.State.Inflight.maximumSendQuota = 5
	cl.State.Inflight.sendQuota = 5
	cl.State.Inflight.maximumReceiveQuota = 10
	cl.State.Inflight.receiveQuota = 10
	cl.Properties.Props.TopicAliasMaximum = 0
	cl.Properties.Props.RequestResponseInfo = 0x1

	cl.armWriter()

	return
}

func TestNewInflights(t *testing.T) {
	i := NewInflights()
	require.Nil(t, i.internal, "made by the first entry")
	require.Nil(t, i.reserved, "made by the first reservation")
	require.Equal(t, 0, i.Len())
}

func TestNewClients(t *testing.T) {
	cl := NewClients()
	require.NotNil(t, cl.internal)
}

func TestClientsAdd(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1"})
	require.Contains(t, cl.internal, "t1")
}

func TestClientsGet(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1"})
	cl.Add(&Client{ID: "t2"})
	require.Contains(t, cl.internal, "t1")
	require.Contains(t, cl.internal, "t2")

	client, ok := cl.Get("t1")
	require.Equal(t, true, ok)
	require.Equal(t, "t1", client.ID)
}

func TestClientsGetAll(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1"})
	cl.Add(&Client{ID: "t2"})
	cl.Add(&Client{ID: "t3"})
	require.Contains(t, cl.internal, "t1")
	require.Contains(t, cl.internal, "t2")
	require.Contains(t, cl.internal, "t3")

	clients := cl.GetAll()
	require.Len(t, clients, 3)
}

func TestClientsLen(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1"})
	cl.Add(&Client{ID: "t2"})
	require.Contains(t, cl.internal, "t1")
	require.Contains(t, cl.internal, "t2")
	require.Equal(t, 2, cl.Len())
}

func TestClientsDelete(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1"})
	require.Contains(t, cl.internal, "t1")

	cl.Delete("t1")
	_, ok := cl.Get("t1")
	require.Equal(t, false, ok)
	require.Nil(t, cl.internal["t1"])
}

// **A connected client keeps nothing of its CONNECT that nothing reads.** A
// CONNECT's User Properties and Authentication Data are for the hooks that
// judge it, which are handed the packet; kept on the client they are memory
// the client chose, for the life of the connection. What the session does use
// - Receive Maximum, Session Expiry, Topic Alias Maximum - is still kept.
func TestParseConnectKeepsNoUserPropertiesOrAuthenticationData(t *testing.T) {
	cl, _, _ := newTestClient()
	pk := packets.Packet{
		ProtocolVersion: 5,
		Connect:         &packets.ConnectParams{ClientIdentifier: "device"},
		Properties: packets.Properties{
			User:                  []packets.UserProperty{{Key: "firmware", Val: "1.2.3"}},
			AuthenticationData:    []byte("token"),
			ReceiveMaximum:        4,
			SessionExpiryInterval: 60,
			TopicAliasMaximum:     3,
		},
	}
	cl.ParseConnect("tcp", pk)

	require.Empty(t, cl.Properties.Props.User, "the client kept its CONNECT's User Properties")
	require.Empty(t, cl.Properties.Props.AuthenticationData, "the client kept its CONNECT's Authentication Data")
	require.Equal(t, uint16(4), cl.Properties.Props.ReceiveMaximum)
	require.Equal(t, uint32(60), cl.Properties.Props.SessionExpiryInterval)
	require.Equal(t, uint16(3), cl.Properties.Props.TopicAliasMaximum)
	require.Len(t, pk.Properties.User, 1, "the packet the hooks are handed lost its User Properties")
}

// DeleteIf removes the client it is given and nothing else: a different
// client now holding the id stays.
func TestClientsDeleteIf(t *testing.T) {
	cl := NewClients()
	old := &Client{ID: "t1"}
	newer := &Client{ID: "t1"}
	cl.Add(newer)

	require.False(t, cl.DeleteIf("t1", old), "deleted a client it was not given")
	got, ok := cl.Get("t1")
	require.True(t, ok)
	require.Same(t, newer, got)

	require.True(t, cl.DeleteIf("t1", newer))
	_, ok = cl.Get("t1")
	require.False(t, ok)
	require.False(t, cl.DeleteIf("t1", newer), "reported a delete of a client that was not there")
}

func TestClientsGetByListener(t *testing.T) {
	cl := NewClients()
	cl.Add(&Client{ID: "t1", State: ClientState{open: context.Background()}, Net: ClientConnection{Listener: "tcp1"}})
	cl.Add(&Client{ID: "t2", State: ClientState{open: context.Background()}, Net: ClientConnection{Listener: "ws1"}})
	require.Contains(t, cl.internal, "t1")
	require.Contains(t, cl.internal, "t2")

	clients := cl.GetByListener("tcp1")
	require.NotEmpty(t, clients)
	require.Equal(t, 1, len(clients))
	require.Equal(t, "tcp1", clients[0].Net.Listener)
}

// GetByListener must not hold the read lock across Len, which takes it
// again. sync.RWMutex is not reentrant for readers - "if a goroutine holds
// a RWMutex for reading and another goroutine might call Lock, no goroutine
// should expect to be able to acquire a read lock until the initial read
// lock is released" - so a writer arriving between the two acquisitions
// blocks the second one, and is itself waiting on the first.
//
// It is reached on every shutdown: Server.Close calls CloseAll, which calls
// closeListenerClients, which calls this, while attachClient calls Delete
// for any client still connecting. A server that deadlocks there never
// finishes closing, so whatever the application does after Close never
// runs.
//
// **The interleaving is made, not hunted for.** A seam holds GetByListener
// inside its read lock until a writer is queued behind it - TryRLock failing
// says so, a queued writer being the one thing that refuses a reader while
// only readers hold the lock - and anything after that reading the lock
// again waits for ever. Readers and writers hammered at it for a second
// found the gap only when the scheduler happened to put a writer in it. A
// deadlock cannot be failed politely from inside it, so the assertion is
// that the call returns at all.
func TestGetByListenerDoesNotDeadlockAgainstAWriter(t *testing.T) {
	cl := NewClients()
	for i := 0; i < 8; i++ {
		cl.Add(&Client{
			ID:    fmt.Sprintf("t%d", i),
			State: ClientState{open: context.Background()},
			Net:   ClientConnection{Listener: "tcp1"},
		})
	}

	writerDone := make(chan struct{})
	queued := 0
	hold := func() {
		go func() {
			defer close(writerDone)
			cl.Add(&Client{ID: "w", State: ClientState{open: context.Background()},
				Net: ClientConnection{Listener: "tcp1"}})
			cl.Delete("w")
		}()
		for cl.TryRLock() {
			cl.RUnlock()
			time.Sleep(time.Millisecond)
		}
		queued++
	}
	getByListenerLocked.Store(&hold)
	defer getByListenerLocked.Store(nil)

	got := make(chan []*Client, 1)
	go func() { got <- cl.GetByListener("tcp1") }()
	select {
	case clients := <-got:
		require.Len(t, clients, 8)
	case <-time.After(5 * time.Second):
		t.Fatal("GetByListener deadlocked against a writer queued while it held the read lock")
	}
	require.Equal(t, 1, queued, "no writer was queued inside GetByListener, so this tested nothing")
	<-writerDone
}

func TestNewClient(t *testing.T) {
	cl, _, _ := newTestClient()

	require.NotNil(t, cl)
	require.NotNil(t, cl.State.Inflight)
	require.NotNil(t, cl.State.Subscriptions)
	require.NotNil(t, cl.State.TopicAliases)
	require.Equal(t, defaultKeepalive, cl.State.Keepalive)
	require.Equal(t, defaultClientProtocolVersion, cl.Properties.ProtocolVersion)
	require.NotNil(t, cl.Net.Conn)
	require.NotNil(t, cl.Net.bconn)
	require.NotNil(t, cl.ops)
	require.NotNil(t, cl.ops.options.Capabilities)
	require.False(t, cl.Net.Inline)
}

func TestClientParseConnect(t *testing.T) {
	cl, _, _ := newTestClient()

	pk := packets.Packet{
		ProtocolVersion: 4,
		Connect: &packets.ConnectParams{
			ProtocolName:     []byte{'M', 'Q', 'T', 'T'},
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "mochi",
			WillFlag:         true,
			WillTopic:        "lwt",
			WillPayload:      []byte("lol gg"),
			WillQos:          1,
			WillRetain:       false,
		},
		Properties: packets.Properties{
			ReceiveMaximum: uint16(5),
		},
	}

	cl.ParseConnect("tcp1", pk)
	require.Equal(t, pk.Connect.ClientIdentifier, cl.ID)
	require.Equal(t, pk.Connect.Keepalive, cl.State.Keepalive)
	require.Equal(t, pk.Connect.Clean, cl.Properties.Clean)
	require.Equal(t, pk.Connect.ClientIdentifier, cl.ID)
	require.Equal(t, pk.Connect.WillTopic, cl.Properties.Will.TopicName)
	require.Equal(t, pk.Connect.WillPayload, cl.Properties.Will.Payload)
	require.Equal(t, pk.Connect.WillQos, cl.Properties.Will.Qos)
	require.Equal(t, pk.Connect.WillRetain, cl.Properties.Will.Retain)
	require.Equal(t, uint32(1), cl.Properties.Will.Flag)
	require.Equal(t, int32(cl.ops.options.Capabilities.ReceiveMaximum), cl.State.Inflight.receiveQuota)
	require.Equal(t, int32(cl.ops.options.Capabilities.ReceiveMaximum), cl.State.Inflight.maximumReceiveQuota)
	require.Equal(t, int32(pk.Properties.ReceiveMaximum), cl.State.Inflight.sendQuota)
	require.Equal(t, int32(pk.Properties.ReceiveMaximum), cl.State.Inflight.maximumSendQuota)
}

// A client's Receive Maximum is its window as it states it, up to the 65,535
// MQTT allows: nothing caps it at a count of the broker's own.
func TestClientParseConnectKeepsTheReceiveMaximumItStates(t *testing.T) {
	cl, _, _ := newTestClient()
	pk := packets.Packet{
		ProtocolVersion: 5,
		Connect: &packets.ConnectParams{
			ProtocolName:     []byte{'M', 'Q', 'T', 'T'},
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "mochi",
		},
		Properties: packets.Properties{
			ReceiveMaximum: uint16(65535),
		},
	}

	cl.ParseConnect("tcp1", pk)
	require.Equal(t, uint16(65535), cl.Properties.Props.ReceiveMaximum)
	require.Equal(t, int32(65535), cl.State.Inflight.sendQuota)
	require.Equal(t, int32(65535), cl.State.Inflight.maximumSendQuota)
}

// The Will keeps the delay it asked for, even past the Session Expiry
// Interval the CONNECT states: saguin bounds the wait by the granted session
// itself (Broker.willDelay), and says so when it does.
func TestClientParseConnectKeepsTheWillDelayItAsked(t *testing.T) {
	cl, _, _ := newTestClient()

	pk := packets.Packet{
		ProtocolVersion: 4,
		Connect: &packets.ConnectParams{
			ProtocolName:     []byte{'M', 'Q', 'T', 'T'},
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: "mochi",
			WillFlag:         true,
			WillProperties: packets.Properties{
				WillDelayInterval: 200,
			},
		},
		Properties: packets.Properties{
			SessionExpiryInterval:     100,
			SessionExpiryIntervalFlag: true,
		},
	}

	cl.ParseConnect("tcp1", pk)
	require.Equal(t, uint32(200), cl.Properties.Will.WillDelayInterval)
}

func TestClientParseConnectNoID(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.ParseConnect("tcp1", packets.Packet{Connect: &packets.ConnectParams{}})
	require.NotEmpty(t, cl.ID)
}

func TestClientParseConnectBelowMinimumKeepalive(t *testing.T) {
	cl, _, _ := newTestClient()
	var b bytes.Buffer
	x := bufio.NewWriter(&b)
	cl.ops.log = slog.New(slog.NewTextHandler(x, nil))

	pk := packets.Packet{
		ProtocolVersion: 4,
		Connect: &packets.ConnectParams{
			ProtocolName:     []byte{'M', 'Q', 'T', 'T'},
			Keepalive:        minimumKeepalive - 1,
			ClientIdentifier: "mochi",
		},
	}
	cl.ParseConnect("tcp1", pk)
	err := x.Flush()
	require.NoError(t, err)
	require.True(t, strings.Contains(b.String(), ErrMinimumKeepalive.Error()))
	require.NotEmpty(t, cl.ID)
}

func TestClientNextPacketID(t *testing.T) {
	cl, _, _ := newTestClient()

	i, err := cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, uint32(1), i)

	i, err = cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, uint32(2), i)
}

func TestClientNextPacketIDInUse(t *testing.T) {
	cl, _, _ := newTestClient()

	// skip over 2
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})

	// Each identifier is released again, because these deliveries are never
	// made: one chosen and not yet registered is reserved and would not be
	// given out twice (Inflight.Claim), which is what the identifier below
	// wrapping round to 1 depends on.
	i, err := cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, uint32(1), i)
	cl.State.Inflight.Unclaim(uint16(i))

	i, err = cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, uint32(3), i)
	cl.State.Inflight.Unclaim(uint16(i))

	// Skip over overflow
	cl.State.Inflight.Set(packets.Packet{PacketID: 65535})
	atomic.StoreUint32(&cl.State.packetID, 65534)

	i, err = cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, uint32(1), i)
}

func TestClientNextPacketIDExhausted(t *testing.T) {
	cl, _, _ := newTestClient()
	for i := uint32(1); i <= cl.ops.options.Capabilities.maximumPacketID; i++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: uint16(i)})
	}

	i, err := cl.NextPacketID()
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
	require.Equal(t, uint32(0), i)
}

func TestClientNextPacketIDOverflow(t *testing.T) {
	cl, _, _ := newTestClient()
	for i := uint32(0); i < cl.ops.options.Capabilities.maximumPacketID; i++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: uint16(i)})
	}

	cl.State.packetID = cl.ops.options.Capabilities.maximumPacketID - 1
	i, err := cl.NextPacketID()
	require.NoError(t, err)
	require.Equal(t, cl.ops.options.Capabilities.maximumPacketID, i)
	cl.State.Inflight.Set(packets.Packet{PacketID: uint16(cl.ops.options.Capabilities.maximumPacketID)})

	cl.State.packetID = cl.ops.options.Capabilities.maximumPacketID
	_, err = cl.NextPacketID()
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
}

func TestClientClearInflights(t *testing.T) {
	cl, _, _ := newTestClient()
	n := time.Now().Unix()

	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 1, Expiry: n - 1})
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 2, Expiry: n - 2})
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 3, Created: n - 3}) // within bounds
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 5, Created: n - 5}) // over max server expiry limit
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 7, Created: n})

	require.Equal(t, 5, cl.State.Inflight.Len())
	cl.ClearInflights()
	require.Equal(t, 0, cl.State.Inflight.Len())
}

// An expired delivery that took a slot of send quota - queued, never yet
// written (Inflight.Queue) - gives it back, as an acknowledgement does, and
// one withheld - which never took any - gives nothing back. Before, every
// expiry kept a slot for the connection's life. One written is not expired
// at all (Inflight.neverSent).
func TestAnExpiredDeliveryGivesBackTheSendQuotaItTook(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.ResetSendQuota(4)
	n := time.Now().Unix()
	queued := packets.Packet{ProtocolVersion: 5, PacketID: 1, Expiry: n - 1, Created: n - 5,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}
	cl.State.Inflight.Set(queued)
	cl.State.Inflight.DecreaseSendQuota()
	cl.State.Inflight.Queue(&queued)
	withheld := packets.Packet{ProtocolVersion: 5, PacketID: 2, Expiry: -1, Created: n - 5,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}
	cl.State.Inflight.Set(withheld)
	// One more written and still live, so the quota starts two below its
	// maximum and a slot given back that was never taken shows rather than
	// being capped away.
	live := packets.Packet{ProtocolVersion: 5, PacketID: 3, Expiry: n + 3600, Created: n,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}
	cl.State.Inflight.Set(live)
	cl.State.Inflight.DecreaseSendQuota()
	require.Equal(t, int32(2), cl.State.Inflight.SendQuota())

	// A maximum expiry of four seconds retires the withheld one as well.
	deleted := cl.ClearExpiredInflights(n, 4)
	require.ElementsMatch(t, []uint16{1, 2}, deleted, "the sweep did not retire both, so this proves nothing")
	require.Equal(t, int32(3), cl.State.Inflight.SendQuota(),
		"the queued delivery's slot came back once and the withheld one's, never taken, not at all")
}

func TestClientClearExpiredInflights(t *testing.T) {
	cl, _, _ := newTestClient()

	n := time.Now().Unix()
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 1, Expiry: n - 1})
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 2, Expiry: n - 2})
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 3, Created: n - 3}) // within bounds
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 5, Created: n - 5}) // over max server expiry limit
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 7, Created: n})
	require.Equal(t, 5, cl.State.Inflight.Len())
	// Never sent: only those expire (Inflight.neverSent).
	for _, id := range []uint16{1, 2, 3, 5, 7} {
		cl.State.Inflight.Withhold(id)
	}

	deleted := cl.ClearExpiredInflights(n, 4)
	require.Len(t, deleted, 3)
	require.ElementsMatch(t, []uint16{1, 2, 5}, deleted)
	require.Equal(t, 2, cl.State.Inflight.Len())

	cl.State.Inflight.Set(packets.Packet{PacketID: 11, Expiry: n - 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 12, Expiry: n - 2})  // expiry is ineffective for v3.
	cl.State.Inflight.Set(packets.Packet{PacketID: 13, Created: n - 3}) // within bounds for v3
	cl.State.Inflight.Set(packets.Packet{PacketID: 15, Created: n - 5}) // over max server expiry limit
	for _, id := range []uint16{11, 12, 13, 15} {
		cl.State.Inflight.Withhold(id)
	}
	require.Equal(t, 6, cl.State.Inflight.Len())

	deleted = cl.ClearExpiredInflights(n, 4)
	require.Len(t, deleted, 3)
	require.ElementsMatch(t, []uint16{11, 12, 15}, deleted)
	require.Equal(t, 3, cl.State.Inflight.Len())

	cl.State.Inflight.Set(packets.Packet{PacketID: 17, Created: n - 1})
	deleted = cl.ClearExpiredInflights(n, 0) // maximumExpiry = 0 do not process abandon messages
	require.Len(t, deleted, 0)
	require.Equal(t, 4, cl.State.Inflight.Len())

	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 18, Expiry: n - 1})
	cl.State.Inflight.Withhold(18)
	deleted = cl.ClearExpiredInflights(n, 0)        // maximumExpiry = 0 do not abandon messages
	require.ElementsMatch(t, []uint16{18}, deleted) // expiry is still effective for v5.
	require.Len(t, deleted, 1)
	require.Equal(t, 4, cl.State.Inflight.Len())
}

func TestClientResendInflightMessages(t *testing.T) {
	pk1 := packets.TPacketData[packets.Puback].Get(packets.TPuback)
	cl, r, w := newTestClient()

	cl.State.Inflight.Set(*pk1.Packet)
	require.Equal(t, 1, cl.State.Inflight.Len())

	go func() {
		err := cl.ResendInflightMessages(true)
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, pk1.RawBytes, buf)
}

// A resumed session is resent no more than the client's Receive Maximum
// admits [MQTT-3.3.4-9], and the rest wait, marked withheld, for the
// acknowledgements that free the window.
func TestClientResendInflightMessagesHonoursTheSendQuota(t *testing.T) {
	cl, r, w := newTestClient()
	cl.State.Inflight.ResetSendQuota(2)
	template := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	for id := uint16(1); id <= 4; id++ {
		pk := template
		pk.PacketID, pk.Created = id, int64(id)
		cl.State.Inflight.Set(pk)
	}

	done := make(chan error, 1)
	go func() {
		done <- cl.ResendInflightMessages(true)
		_ = w.Close()
	}()
	_, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, <-done)

	require.Equal(t, int64(2), cl.ops.info.MessagesSent.Load(),
		"a resumed session was sent more than its Receive Maximum")
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	for id := uint16(1); id <= 4; id++ {
		pk, ok := cl.State.Inflight.Get(id)
		require.True(t, ok, "packet %d left the session", id)
		require.Equal(t, id > 2, pk.Expiry < 0, "packet %d: withheld is %v", id, pk.Expiry < 0)
	}
}

// **A delivery withheld before a disconnect is a first delivery on the
// resume** (clients.go, ResendInflightMessages): it was never written, so it
// is claimed and sent with DUP 0, where one already on the wire is re-sent
// with DUP 1 [MQTT-3.3.1-1]. Receive Maximum 1 withheld the second of two
// QoS 1 deliveries; the session resumes with room for both.
func TestAWithheldDeliveryIsAFirstDeliveryOnTheResume(t *testing.T) {
	cl, r, w := newTestClient()
	template := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	sent, held := template, template
	sent.PacketID, sent.Created = 1, 1
	held.PacketID, held.Created = 2, 2
	cl.State.Inflight.Set(sent)
	cl.State.Inflight.Set(held)
	cl.State.Inflight.Withhold(2)
	got, _ := cl.State.Inflight.Get(2)
	require.Less(t, got.Expiry, int64(0), "the second delivery was not withheld, so this proves nothing")

	cl.State.Inflight.ResetSendQuota(2) // the resuming connection's Receive Maximum
	done := make(chan error, 1)
	go func() {
		done <- cl.ResendInflightMessages(true)
		_ = w.Close()
	}()
	raw, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, <-done)

	var firstBytes []byte
	for len(raw) > 0 {
		firstBytes = append(firstBytes, raw[0])
		n, bu, err := packets.DecodeLength(bytes.NewReader(raw[1:]))
		require.NoError(t, err)
		raw = raw[1+bu+n:]
	}
	require.Len(t, firstBytes, 2, "the resume wrote %d packets, want both deliveries", len(firstBytes))
	require.NotZero(t, firstBytes[0]&0x08, "the delivery already on the wire was re-sent without DUP")
	require.Zero(t, firstBytes[1]&0x08, "the delivery withheld before the disconnect was sent as a re-send, with DUP")
	got, ok := cl.State.Inflight.Get(2)
	require.True(t, ok, "the withheld delivery left the session")
	require.GreaterOrEqual(t, got.Expiry, int64(0), "the withheld delivery is still withheld after it was sent")
}

// A Bounded PUBLISH - one whose session's bound is kept by whoever wrote it,
// saguin's broadcast log - is counted once, there: the in-flight table leaves
// it out of its messages and bytes, of what it has on the wire, and of what it
// could take back. It still holds its slot in the client's window: it is in
// the table, and a resumed session is re-sent it within the send quota like
// any other [MQTT-3.3.4-9], and one past the quota is held back, counted as
// withheld, for the acknowledgements that free it.
func TestABoundedPublishIsNotCountedByTheTableButTakesItsWindowSlot(t *testing.T) {
	cl, r, w := newTestClient()
	cl.State.Inflight.ResetSendQuota(1)
	template := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	bounded := template
	bounded.PacketID, bounded.Created, bounded.Bounded = 1, 1, true
	bounded.Origin = "publisher" // what the table could take back, were it counted
	plain := template
	plain.PacketID, plain.Created = 2, 2
	later := bounded
	later.PacketID, later.Created = 3, 3

	cl.State.Inflight.Set(bounded)
	require.Equal(t, 1, cl.State.Inflight.Len(), "a Bounded PUBLISH is not in the table")
	require.Equal(t, int64(0), cl.State.Inflight.Messages())
	require.Equal(t, int64(0), cl.State.Inflight.Bytes())
	require.Equal(t, int64(0), cl.State.Inflight.DroppableBytes(true))
	require.True(t, cl.State.Inflight.WireHasRoom(1, 1), "a Bounded PUBLISH counted on the wire")

	cl.State.Inflight.Set(plain)
	cl.State.Inflight.Set(later)
	require.Equal(t, int64(1), cl.State.Inflight.Messages())
	// The table's own cost is the log's to charge while it holds a Bounded
	// entry (InflightTableCost), so only the plain one's size is here.
	require.Equal(t, inflightSize(plain), cl.State.Inflight.Bytes())

	done := make(chan error, 1)
	go func() {
		done <- cl.ResendInflightMessages(true)
		_ = w.Close()
	}()
	_, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota),
		"the Bounded PUBLISH was re-sent without taking a send-quota slot")
	got, ok := cl.State.Inflight.Get(1)
	require.True(t, ok)
	require.GreaterOrEqual(t, got.Expiry, int64(0), "the Bounded PUBLISH, first, was not re-sent")
	got, ok = cl.State.Inflight.Get(2)
	require.True(t, ok)
	require.Less(t, got.Expiry, int64(0), "the second was re-sent past the send quota")
	got, ok = cl.State.Inflight.Get(3)
	require.True(t, ok)
	require.Less(t, got.Expiry, int64(0), "the second Bounded PUBLISH was re-sent past the send quota")
	require.Equal(t, int64(2), cl.State.Inflight.withheld, "a Bounded PUBLISH held back is not counted as withheld")

	require.True(t, cl.State.Inflight.Delete(1))
	require.Equal(t, int64(1), cl.State.Inflight.Messages(), "removing a Bounded PUBLISH took off what it never added")
	require.Equal(t, inflightSize(plain), cl.State.Inflight.Bytes())
	require.True(t, cl.State.Inflight.Delete(3))
	require.Equal(t, InflightTableCost+inflightSize(plain), cl.State.Inflight.Bytes(),
		"the table's cost is not charged once no Bounded entry is left to be charged it for")
}

func TestClientResendInflightMessagesWriteFailure(t *testing.T) {
	pk1 := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Dup)
	cl, r, _ := newTestClient()
	_ = r.Close()

	cl.State.Inflight.Set(*pk1.Packet)
	require.Equal(t, 1, cl.State.Inflight.Len())
	err := cl.ResendInflightMessages(true)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Equal(t, 1, cl.State.Inflight.Len())
}

func TestClientResendInflightMessagesNoMessages(t *testing.T) {
	cl, _, _ := newTestClient()
	err := cl.ResendInflightMessages(true)
	require.NoError(t, err)
}

func TestClientRefreshDeadline(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.refreshDeadline(10)
	require.NotNil(t, cl.Net.Conn) // how do we check net.Conn deadline?
}

func TestClientReadFixedHeader(t *testing.T) {
	cl, r, _ := newTestClient()

	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{packets.Connect << 4, 0x00})
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.NoError(t, err)
	require.Equal(t, int64(2), cl.ops.info.BytesReceived.Load())
}

func TestClientReadFixedHeaderDecodeError(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	go func() {
		_, _ = r.Write([]byte{packets.Connect<<4 | 1<<1, 0x00, 0x00})
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.Error(t, err)
}

func TestClientReadFixedHeaderPacketOversized(t *testing.T) {
	cl, r, _ := newTestClient()
	cl.ops.options.Capabilities.MaximumPacketSize = 2
	defer cl.Stop(errClientStop)

	go func() {
		_, _ = r.Write(packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Dup).RawBytes)
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrPacketTooLarge)
}

// **The Maximum Packet Size is the whole packet**: MQTT 5 §3.1.2.11.4 calls it
// "the total number of bytes in an MQTT Control Packet", and §2.1.4 says the
// Remaining Length does not include its own bytes. So a packet is one byte
// of fixed header, one to four bytes of Remaining Length, and the Remaining
// Length - and the limit holds it all. mosquitto's packet__check_oversize
// counts the same three.
//
// Each packet is tried at exactly its own size, which is admitted, and one
// byte under it, which is not. A check that left the length bytes out
// admitted both, by one and two bytes here and by up to four on a packet
// over two megabytes, measured on the binary.
func TestClientReadFixedHeaderCountsTheWholePacket(t *testing.T) {
	// A PUBLISH whose Remaining Length takes two bytes: topic "a/b" and 200
	// bytes of payload is 205, encoded 0xCD 0x01.
	long := append([]byte{packets.Publish << 4, 0xCD, 0x01, 0, 3, 'a', '/', 'b'},
		bytes.Repeat([]byte("x"), 200)...)
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"a one-byte Remaining Length", packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Dup).RawBytes},
		{"a two-byte Remaining Length", long},
	} {
		for _, max := range []int{len(tc.raw), len(tc.raw) - 1} {
			t.Run(fmt.Sprintf("%s, %d bytes against %d", tc.name, len(tc.raw), max), func(t *testing.T) {
				cl, r, _ := newTestClient()
				cl.ops.options.Capabilities.MaximumPacketSize = uint32(max)
				defer cl.Stop(errClientStop)
				go func() {
					_, _ = r.Write(tc.raw)
					_ = r.Close()
				}()

				err := cl.ReadFixedHeader(new(packets.FixedHeader))
				if max >= len(tc.raw) && err != nil {
					t.Fatalf("a packet exactly at the maximum was refused: %v", err)
				}
				if max < len(tc.raw) && !errors.Is(err, packets.ErrPacketTooLarge) {
					t.Fatalf("a packet of %d bytes against a maximum of %d was admitted (%v)",
						len(tc.raw), max, err)
				}
			})
		}
	}
}

func TestClientReadFixedHeaderReadEOF(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	go func() {
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
}

func TestClientReadFixedHeaderNoLengthTerminator(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	go func() {
		_, _ = r.Write([]byte{packets.Connect << 4, 0xd5, 0x86, 0xf9, 0x9e, 0x01})
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.Error(t, err)
}

func TestClientReadOK(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			packets.Publish << 4, 18, // Fixed header
			0, 5, // Topic Name - LSB+MSB
			'a', '/', 'b', '/', 'c', // Topic Name
			'h', 'e', 'l', 'l', 'o', ' ', 'm', 'o', 'c', 'h', 'i', // Payload,
			packets.Publish << 4, 11, // Fixed header
			0, 5, // Topic Name - LSB+MSB
			'd', '/', 'e', '/', 'f', // Topic Name
			'y', 'e', 'a', 'h', // Payload
		})
		_ = r.Close()
	}()

	var pks []packets.Packet
	o := make(chan error)
	go func() {
		o <- cl.Read(func(cl *Client, pk packets.Packet) error {
			pks = append(pks, pk)
			return nil
		})
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, len(pks))
	require.Equal(t, []packets.Packet{
		{
			ProtocolVersion: cl.Properties.ProtocolVersion,
			FixedHeader: packets.FixedHeader{
				Type:      packets.Publish,
				Remaining: 18,
			},
			TopicName: "a/b/c",
			Payload:   []byte("hello mochi"),
		},
		{
			ProtocolVersion: cl.Properties.ProtocolVersion,
			FixedHeader: packets.FixedHeader{
				Type:      packets.Publish,
				Remaining: 11,
			},
			TopicName: "d/e/f",
			Payload:   []byte("yeah"),
		},
	}, pks)

	require.Equal(t, int64(2), cl.ops.info.MessagesReceived.Load())
}

// A read loop that finds its connection closed by something other than the
// client's DISCONNECT does not answer as that DISCONNECT would: nil withdraws
// the Will [MQTT-3.1.2-8] (Client.endedElsewhere).
func TestClientReadDone(t *testing.T) {
	cl, _, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.State.cancelOpen()

	o := make(chan error)
	go func() {
		o <- cl.Read(func(cl *Client, pk packets.Packet) error {
			return nil
		})
	}()

	require.ErrorIs(t, <-o, ErrConnectionClosed)
}

func TestClientStop(t *testing.T) {
	cl, _, _ := newTestClient()
	require.True(t, cl.StopTime().IsZero(), "a client that has not disconnected has no stop moment")
	cl.Stop(nil)
	require.Equal(t, nil, cl.State.stopCause.Load())
	// To the nanosecond: a session's lifetime is judged against this, and
	// whole seconds ended one as much as a second early.
	require.WithinDuration(t, time.Now(), cl.StopTime(), time.Second)
	require.Equal(t, cl.State.disconnected.Load(), cl.StopTime().UnixNano())
	require.True(t, cl.Closed())
	require.Equal(t, nil, cl.StopCause())
}

func TestClientClosed(t *testing.T) {
	cl, _, _ := newTestClient()
	require.False(t, cl.Closed())
	cl.Stop(nil)
	require.True(t, cl.Closed())
}

func TestClientIsTakenOver(t *testing.T) {
	cl, _, _ := newTestClient()
	require.False(t, cl.IsTakenOver())
	cl.State.isTakenOver.Store(true)
	require.True(t, cl.IsTakenOver())
}

func TestClientReadFixedHeaderError(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			packets.Publish << 4, 11, // Fixed header
		})
		_ = r.Close()
	}()

	cl.Net.bconn = nil
	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.Error(t, err)
	require.ErrorIs(t, ErrConnectionClosed, err)
}

func TestClientReadReadHandlerErr(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			packets.Publish << 4, 11, // Fixed header
			0, 5, // Topic Name - LSB+MSB
			'd', '/', 'e', '/', 'f', // Topic Name
			'y', 'e', 'a', 'h', // Payload
		})
		_ = r.Close()
	}()

	err := cl.Read(func(cl *Client, pk packets.Packet) error {
		return errors.New("test")
	})

	require.Error(t, err)
}

func TestClientReadReadPacketOK(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			packets.Publish << 4, 11, // Fixed header
			0, 5,
			'd', '/', 'e', '/', 'f',
			'y', 'e', 'a', 'h',
		})
		_ = r.Close()
	}()

	fh := new(packets.FixedHeader)
	err := cl.ReadFixedHeader(fh)
	require.NoError(t, err)

	pk, err := cl.ReadPacket(fh)
	require.NoError(t, err)
	require.NotNil(t, pk)

	require.Equal(t, packets.Packet{
		ProtocolVersion: cl.Properties.ProtocolVersion,
		FixedHeader: packets.FixedHeader{
			Type:      packets.Publish,
			Remaining: 11,
		},
		TopicName: "d/e/f",
		Payload:   []byte("yeah"),
	}, pk)
}

func TestClientReadPacket(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)

	for _, tx := range pkTable {
		tt := tx // avoid data race
		t.Run(tt.Desc, func(t *testing.T) {
			go func() {
				_, _ = r.Write(tt.RawBytes)
			}()

			fh := new(packets.FixedHeader)
			err := cl.ReadFixedHeader(fh)
			require.NoError(t, err)

			if tt.Packet.ProtocolVersion == 5 {
				cl.Properties.ProtocolVersion = 5
			} else {
				cl.Properties.ProtocolVersion = 0
			}

			pk, err := cl.ReadPacket(fh)
			require.NoError(t, err, pkInfo, tt.Case, tt.Desc)
			require.NotNil(t, pk, pkInfo, tt.Case, tt.Desc)
			require.Equal(t, *tt.Packet, pk, pkInfo, tt.Case, tt.Desc)

			if tt.Packet.FixedHeader.Type == packets.Publish {
			}
		})
	}
}

func TestClientReadPacketInvalidTypeError(t *testing.T) {
	cl, _, _ := newTestClient()
	_ = cl.Net.Conn.Close()
	_, err := cl.ReadPacket(&packets.FixedHeader{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid packet type")
}

func TestClientWritePacket(t *testing.T) {
	for _, tt := range pkTable {
		cl, r, _ := newTestClient()
		defer cl.Stop(errClientStop)

		cl.Properties.ProtocolVersion = tt.Packet.ProtocolVersion

		o := make(chan []byte)
		go func() {
			buf, err := io.ReadAll(r)
			require.NoError(t, err)
			o <- buf
		}()

		// A DISCONNECT is written by the owner of the connection's end, which
		// closes the connection (Client.claimEnd).
		owner := tt.Packet.FixedHeader.Type == packets.Disconnect
		if owner {
			require.True(t, cl.claimEnd(), pkInfo, tt.Case, tt.Desc)
		}
		err := cl.WritePacket(*tt.Packet)
		require.NoError(t, err, pkInfo, tt.Case, tt.Desc)

		time.Sleep(2 * time.Millisecond)
		_ = cl.Net.Conn.Close()

		require.Equal(t, tt.RawBytes, <-o, pkInfo, tt.Case, tt.Desc)

		if owner {
			cl.finishEnd(errClientStop)
		}
		cl.Stop(errClientStop)
		time.Sleep(time.Millisecond * 1)

		// The stop cause is either the test error, EOF, or a
		// closed pipe, depending on which goroutine runs first.
		err = cl.StopCause()
		require.True(t,
			errors.Is(err, errClientStop) ||
				errors.Is(err, io.EOF) ||
				errors.Is(err, io.ErrClosedPipe))

		require.Equal(t, int64(len(tt.RawBytes)), cl.ops.info.BytesSent.Load())
		if tt.Packet.FixedHeader.Type == packets.Publish {
			require.Equal(t, int64(1), cl.ops.info.MessagesSent.Load())
		}
	}
}

func TestClientWritePacketBuffer(t *testing.T) {
	r, w := net.Pipe()

	cl := newClient(w, &ops{
		info:  new(system.Info),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{
			Capabilities: &Capabilities{
				ReceiveMaximum:             10,
				TopicAliasMaximum:          10000,
				MaximumClientWritesPending: 3,
				maximumPacketID:            10,
			},
		},
	})

	cl.ID = "mochi"
	cl.State.Inflight.maximumSendQuota = 5
	cl.State.Inflight.sendQuota = 5
	cl.State.Inflight.maximumReceiveQuota = 10
	cl.State.Inflight.receiveQuota = 10
	cl.Properties.Props.TopicAliasMaximum = 0
	cl.Properties.Props.RequestResponseInfo = 0x1

	cl.ops.options.ClientNetWriteBufferSize = 10
	defer cl.Stop(errClientStop)

	small := packets.TPacketData[packets.Publish].Get(packets.TPublishNoPayload).Packet
	large := packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet

	cl.State.outbound.push(small)

	tt := []struct {
		pks  []*packets.Packet
		size int
	}{
		{
			pks:  []*packets.Packet{small, small},
			size: 18,
		},
		{
			pks:  []*packets.Packet{large},
			size: 20,
		},
		{
			pks:  []*packets.Packet{small},
			size: 0,
		},
	}

	go func() {
		for i, tx := range tt {
			for _, pk := range tx.pks {
				cl.Properties.ProtocolVersion = pk.ProtocolVersion
				err := cl.WritePacket(*pk)
				require.NoError(t, err, "index: %d", i)
				if i == len(tt)-1 {
					cl.Net.Conn.Close()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	var n int
	var err error
	for i, tx := range tt {
		buf := make([]byte, 100)
		if i == len(tt)-1 {
			buf, err = io.ReadAll(r)
			n = len(buf)
		} else {
			n, err = io.ReadAtLeast(r, buf, 1)
		}
		require.NoError(t, err, "index: %d", i)
		require.Equal(t, tx.size, n, "index: %d", i)
	}
}

func TestWriteClientOversizePacket(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.Properties.Props.MaximumPacketSize = 2
	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishDropOversize).Packet
	err := cl.WritePacket(pk)
	require.Error(t, err)
	require.ErrorIs(t, packets.ErrPacketTooLarge, err)
	// Refused before a byte reached the connection, so the stream is intact
	// and the client carries on.
	require.False(t, cl.Closed(), "a packet refused before it was written stopped the client")
}

func TestClientReadPacketReadingError(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			0, 11, // Fixed header
			0, 5,
			'd', '/', 'e', '/', 'f',
			'y', 'e', 'a', 'h',
		})
		_ = r.Close()
	}()

	_, err := cl.ReadPacket(&packets.FixedHeader{
		Type:      0,
		Remaining: 11,
	})
	require.Error(t, err)
}

func TestClientReadPacketReadUnknown(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	go func() {
		_, _ = r.Write([]byte{
			0, 11, // Fixed header
			0, 5,
			'd', '/', 'e', '/', 'f',
			'y', 'e', 'a', 'h',
		})
		_ = r.Close()
	}()

	_, err := cl.ReadPacket(&packets.FixedHeader{
		Remaining: 1,
	})
	require.Error(t, err)
}

func TestClientWritePacketWriteNoConn(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.Stop(errClientStop)

	err := cl.WritePacket(*pkTable[1].Packet)
	require.Error(t, err)
	require.Equal(t, ErrConnectionClosed, err)
}

func TestClientWritePacketWriteError(t *testing.T) {
	cl, _, _ := newTestClient()
	_ = cl.Net.Conn.Close()

	err := cl.WritePacket(*pkTable[1].Packet)
	require.Error(t, err)
}

func TestClientWritePacketInvalidPacket(t *testing.T) {
	cl, _, _ := newTestClient()
	err := cl.WritePacket(packets.Packet{})
	require.Error(t, err)
	require.False(t, cl.Closed(), "a packet that could not be encoded stopped the client")
}

// **A write that fails on the connection stops the client**, whether or not
// it was a timeout. Such a write may have put part of a packet on the wire,
// and the next packet written would land inside it: a subscriber would parse
// the tail of one packet as the head of another. Only a timeout used to stop
// the client, so a reset or a broken pipe left it open and written to.
//
// Driven through WritePacket directly, which is how every acknowledgement the
// server sends and every delivery saguin writes itself reaches the
// connection - not only the write loop's queued packets.
func TestAFailedWriteStopsTheClient(t *testing.T) {
	cl, r, _ := newTestClient()
	require.NoError(t, r.Close()) // the peer is gone: writes fail at once, and not by timeout

	err := cl.WritePacket(*pkTable[1].Packet)
	require.Error(t, err)
	require.False(t, isTimeout(err), "want a transport failure that is not a timeout, got %v", err)
	require.True(t, cl.Closed(), "a write that failed on the connection left the client open")
	require.ErrorIs(t, cl.StopCause(), err, "the client was not stopped for the failed write")
}

// **A packet refused before it was written does not end the write loop.** It
// put nothing on the wire, so the stream is intact, and the packets queued
// behind it are still owed to the client.
func TestClientWriteLoopGoesOnAfterAPacketRefusedBeforeItWasWritten(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	// The raw bytes compared below are this packet's MQTT 5 encoding, and a
	// client speaking 3.1.1 writes a different one.
	cl.Properties.ProtocolVersion = pkTable[1].Packet.ProtocolVersion

	cl.startWriter()

	refused := *pkTable[1].Packet
	refused.Mods.MaxSize = 2 // larger than this peer accepts
	next := *pkTable[1].Packet
	want := pkTable[1].RawBytes
	for _, pk := range []*packets.Packet{&refused, &next} {
		cl.State.outbound.push(pk)
		atomic.AddInt32(&cl.State.outboundQty, 1)
	}

	got := make([]byte, len(want))
	_ = r.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := io.ReadFull(r, got)
	require.NoError(t, err, "the packet queued behind a refused one never reached the client")
	require.Equal(t, want, got)
	require.False(t, cl.Closed(), "a packet refused before it was written stopped the client")
}

// The write loop's half of the same rule: a queued packet whose write fails
// for any transport reason ends the loop and the client, not only a timeout.
func TestClientWriteLoopStopsTheClientWhenAWriteFails(t *testing.T) {
	cl, r, _ := newTestClient()
	require.NoError(t, r.Close())

	cl.startWriter()

	pk := *pkTable[1].Packet
	cl.State.outbound.push(&pk)
	atomic.AddInt32(&cl.State.outboundQty, 1)

	require.Eventually(t, cl.Closed, 2*time.Second, 10*time.Millisecond,
		"the write loop left the client open after a write failed on the connection")
	require.Error(t, cl.StopCause())
	require.False(t, isTimeout(cl.StopCause()), "want the transport failure, got %v", cl.StopCause())
}

var (
	pkTable = []packets.TPacketCase{
		packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311),
		packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedMqtt5),
		packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession),
		packets.TPacketData[packets.Publish].Get(packets.TPublishBasic),
		packets.TPacketData[packets.Publish].Get(packets.TPublishMqtt5),
		packets.TPacketData[packets.Puback].Get(packets.TPuback),
		packets.TPacketData[packets.Pubrec].Get(packets.TPubrec),
		packets.TPacketData[packets.Pubrel].Get(packets.TPubrel),
		packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp),
		packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe),
		packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeMqtt5),
		packets.TPacketData[packets.Suback].Get(packets.TSuback),
		packets.TPacketData[packets.Suback].Get(packets.TSubackMqtt5),
		packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribe),
		packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribeMqtt5),
		packets.TPacketData[packets.Unsuback].Get(packets.TUnsuback),
		packets.TPacketData[packets.Unsuback].Get(packets.TUnsubackMqtt5),
		packets.TPacketData[packets.Pingreq].Get(packets.TPingreq),
		packets.TPacketData[packets.Pingresp].Get(packets.TPingresp),
		packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect),
		packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectMqtt5),
		packets.TPacketData[packets.Auth].Get(packets.TAuth),
	}
)

// [MQTT-3.1.2-29] names PUBLISH among the packets a Reason String or User
// Properties may still be sent on when a client sets Request Problem
// Information to 0. A PUBLISH's User Properties are application data
// forwarded from the publisher, not problem information about a failure,
// so suppressing them silently drops data the subscriber was sent.
func TestClientWritePacketRequestProblemInfoExemptsPublish(t *testing.T) {
	tt := []struct {
		name string
		pk   packets.Packet
		want bool
	}{
		{
			name: "publish keeps its user properties",
			pk: packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish},
				TopicName:   "a/b/c",
				Payload:     []byte("hello"),
				Properties: packets.Properties{
					User: []packets.UserProperty{{Key: "prop-key", Val: "prop-val"}},
				},
			},
			want: true,
		},
		{
			name: "puback does not",
			pk: packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Puback},
				PacketID:    1,
				Properties: packets.Properties{
					User: []packets.UserProperty{{Key: "prop-key", Val: "prop-val"}},
				},
			},
			want: false,
		},
	}

	for _, tx := range tt {
		t.Run(tx.name, func(t *testing.T) {
			cl, r, _ := newTestClient()
			defer cl.Stop(errClientStop)
			cl.Properties.ProtocolVersion = 5
			cl.Properties.Props.RequestProblemInfoFlag = true
			cl.Properties.Props.RequestProblemInfo = 0x0

			o := make(chan []byte)
			go func() {
				buf, err := io.ReadAll(r)
				require.NoError(t, err)
				o <- buf
			}()

			tx.pk.ProtocolVersion = 5
			require.NoError(t, cl.WritePacket(tx.pk))

			time.Sleep(2 * time.Millisecond)
			_ = cl.Net.Conn.Close()

			require.Equal(t, tx.want, bytes.Contains(<-o, []byte("prop-key")))
		})
	}
}

// A client that never reads its socket must not hold the client lock for
// ever, because every write happens under it - including the one
// publishToClient needs a packet identifier for before it can reach the
// outbound queue that would have shed that client.
//
// net.Pipe is unbuffered, so a write to it blocks until the other side
// reads. Nothing reads here, which is a client that has stopped reading
// exactly as a full socket buffer is.
func TestClientWritePacketTimesOutRatherThanHoldingTheLock(t *testing.T) {
	cl, _, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.ops.options.ClientNetWriteTimeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		done <- cl.WritePacket(*pkTable[1].Packet)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, isTimeout(err), "want a network timeout, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("WritePacket to a client that never reads did not return: it is holding " +
			"the client lock, and every other goroutine that needs it - publishToClient " +
			"taking a packet identifier for a QoS 1 delivery - waits behind it")
	}

	// And the lock is free again, which is the whole point of bounding it.
	locked := make(chan struct{})
	go func() {
		cl.Lock()
		cl.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("the client lock was still held after the write timed out")
	}
}

// Zero is the default and must leave writes exactly as they were, so that
// taking this patch changes nothing for anyone who does not set it.
func TestClientWritePacketIsUnboundedByDefault(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	require.Zero(t, cl.ops.options.ClientNetWriteTimeout)

	done := make(chan error, 1)
	go func() { done <- cl.WritePacket(*pkTable[1].Packet) }()

	// It is still blocked a moment later, because nothing has read yet.
	select {
	case err := <-done:
		t.Fatalf("the write returned early with %v: without a timeout it waits for a reader", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Read, and it completes normally.
	buf := make([]byte, len(pkTable[1].RawBytes))
	go func() { _, _ = io.ReadFull(r, buf) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the write never completed even once its reader arrived")
	}
}

// A write that ran out of time has put part of a packet on the wire, so
// the next packet written would land in the middle of the last one. It is
// also the moment the loop would otherwise take the client lock again for
// every packet still queued, a timeout at a time, while publishToClient
// waits for that lock to take a packet identifier.
func TestClientWriteLoopStopsTheClientWhenAWriteTimesOut(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.ops.options.ClientNetWriteTimeout = 50 * time.Millisecond

	cl.startWriter()

	pk := *pkTable[1].Packet
	cl.State.outbound.push(&pk)
	atomic.AddInt32(&cl.State.outboundQty, 1)

	// Nothing reads the pipe, so the write times out and the loop stops the
	// client rather than trying the next packet into a broken stream.
	require.Eventually(t, cl.Closed, 2*time.Second, 10*time.Millisecond,
		"the client was not stopped after its write ran out of time")
	require.True(t, isTimeout(cl.StopCause()), "want a network timeout, got %v", cl.StopCause())
}

func TestClientWritePacketDeadlineSurvivesThePacketsTheClientSends(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.ops.options.ClientNetWriteTimeout = 50 * time.Millisecond
	cl.State.Keepalive = 0 // the worst case: the refresh's expiry is the zero time

	go func() { _ = cl.Read(func(*Client, packets.Packet) error { return nil }) }()

	// The client keeps sending packets that need no reply, which is what
	// takes the read loop round again - and round again is where the
	// keepalive deadline is refreshed.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			if _, err := r.Write([]byte{packets.Pingreq << 4, 0}); err != nil {
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() { done <- cl.WritePacket(*pkTable[1].Packet) }()

	select {
	case err := <-done:
		require.True(t, isTimeout(err), "want a network timeout, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("a client that keeps sending switched off the write deadline armed for " +
			"it: the keepalive refresh runs at the top of every pass of the read loop, " +
			"so it has to bound reads and not both")
	}
}

// A write to a client that has stopped reading is bounded by the keepalive
// even when ClientNetWriteTimeout is unset.
//
// This is a regression test for the default configuration rather than for the
// option. refreshDeadline arms only the read deadline, so the keepalive no
// longer bounds a write the way SetDeadline used to - and with the option at
// its zero value that would leave a write able to block forever while holding
// the client's lock, which is the deadlock the option exists to prevent. An
// operator who upgrades and sets nothing must not end up worse off.
func TestWriteIsBoundedByKeepaliveWithNoWriteTimeout(t *testing.T) {
	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()

	cl, _, _ := newTestClient()
	cl.Net.Conn = r
	cl.State.Keepalive = 1
	cl.ops.options.ClientNetWriteTimeout = 0 // the default, and the point

	done := make(chan error, 1)
	go func() {
		// Nothing reads w, so this write can only end at a deadline.
		done <- cl.WritePacket(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, isTimeout(err), "want a timeout, got %v", err)
	case <-time.After(4 * time.Second):
		t.Fatal("a write to a client that stopped reading was never bounded, with the keepalive set and the write timeout unset")
	}
}

// And the option still overrides the keepalive when it is set, which is what
// makes it useful: a deployment wanting a tighter bound than 1.5x keepalive
// can have one.
func TestWriteTimeoutOverridesTheKeepaliveBound(t *testing.T) {
	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()

	cl, _, _ := newTestClient()
	cl.Net.Conn = r
	cl.State.Keepalive = 60 // 90s if the keepalive decided it
	cl.ops.options.ClientNetWriteTimeout = 200 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		done <- cl.WritePacket(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.True(t, isTimeout(err), "want a timeout, got %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("the write timeout did not override the longer keepalive bound")
	}
}

// recordingConn notes which deadline a caller armed. Which one matters more
// than it looks: a websocket connection overrides SetWriteDeadline ONLY, and
// passes SetDeadline and SetReadDeadline through to the socket underneath -
// where gorilla overwrites the write deadline before every frame it sends. So
// a bound armed with SetDeadline reaches a TCP client and silently does not
// reach a websocket one.
type recordingConn struct {
	net.Conn
	write, read, both time.Time
	cleared           bool
}

// The armed deadline, not the last one: WritePacket clears it again on the
// way out, so recording the most recent call records the clearing and reads
// as though nothing was ever armed.
func (c *recordingConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.cleared = true
	} else if c.write.IsZero() {
		c.write = t
	}
	return nil
}
func (c *recordingConn) SetReadDeadline(t time.Time) error { c.read = t; return nil }
func (c *recordingConn) SetDeadline(t time.Time) error     { c.both = t; return nil }
func (c *recordingConn) Write(p []byte) (int, error)       { return len(p), nil }

// The keepalive bound is armed as a WRITE deadline, which is the only one a
// websocket connection routes to the thing that actually bounds the write.
//
// Armed as SetDeadline instead, this would still pass for a TCP client and
// bound nothing at all for a websocket one - so the assertion is on which
// method was called, not merely on the write ending.
func TestTheKeepaliveWriteBoundIsArmedWhereAWebsocketCanSeeIt(t *testing.T) {
	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()

	rec := &recordingConn{Conn: r}
	cl, _, _ := newTestClient()
	cl.Net.Conn = rec
	cl.State.Keepalive = 10
	cl.ops.options.ClientNetWriteTimeout = 0

	require.NoError(t, cl.WritePacket(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet))

	require.False(t, rec.write.IsZero(), "no write deadline was armed, so a websocket write is unbounded")
	require.True(t, rec.both.IsZero(), "armed with SetDeadline, which a websocket connection does not route to the write")

	// 10s keepalive means the 15s expiry refreshDeadline uses.
	require.WithinDuration(t, time.Now().Add(15*time.Second), rec.write, 2*time.Second)

	// And it is released with the lock, so it never outlives the write it
	// was armed for.
	require.True(t, rec.cleared, "the deadline was left armed on the connection")
}

// Deliveries being made to one client at once are never given the same packet
// identifier [MQTT-2.2.1-4]. A delivery takes its identifier before its
// session store write and is put in flight after it, so an identifier chosen
// and not yet in flight must not be chosen again - which with few left free
// is what the search comes back round to.
func TestAnIdentifierChosenAndNotYetInFlightIsNotChosenAgain(t *testing.T) {
	cl, _, _ := newTestClient()
	max := uint16(cl.ops.options.Capabilities.maximumPacketID)
	for id := uint16(1); id <= max-2; id++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: id})
	}
	given := map[uint32]int{}
	for n := 0; n < 3; n++ {
		id, err := cl.NextPacketID()
		if err != nil {
			continue
		}
		given[id]++
	}
	for id, times := range given {
		require.Equal(t, 1, times, "identifier %d was given to %d deliveries at once (given %v)", id, times, given)
	}
}

// An identifier chosen for a delivery that is then not made is given out
// again: a reservation nobody releases would spend the slot for the life of
// the session, which is the same exhaustion by another route.
func TestAnIdentifierAbandonedIsGivenOutAgain(t *testing.T) {
	cl, _, _ := newTestClient()
	max := uint16(cl.ops.options.Capabilities.maximumPacketID)
	for id := uint16(1); id <= max-1; id++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: id})
	}

	id, err := cl.NextPacketID()
	require.NoError(t, err, "the one free identifier was not given out")
	_, err = cl.NextPacketID()
	require.ErrorIs(t, err, packets.ErrQuotaExceeded, "an identifier already chosen was given out again")

	cl.State.Inflight.Unclaim(uint16(id))
	again, err := cl.NextPacketID()
	require.NoError(t, err, "the identifier of a delivery that was never made was not given out again")
	require.Equal(t, id, again)
}

// An identifier whose delivery was registered in flight and then finished is
// given out again: registering it is what releases the reservation, so a
// session that keeps working does not spend its identifiers one by one.
func TestAnIdentifierRegisteredAndFinishedIsGivenOutAgain(t *testing.T) {
	cl, _, _ := newTestClient()
	max := uint16(cl.ops.options.Capabilities.maximumPacketID)
	for id := uint16(1); id <= max-1; id++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: id})
	}

	id, err := cl.NextPacketID()
	require.NoError(t, err)
	cl.State.Inflight.Set(packets.Packet{PacketID: uint16(id)}) // made
	require.True(t, cl.State.Inflight.Delete(uint16(id)))       // acknowledged

	again, err := cl.NextPacketID()
	require.NoError(t, err, "the identifier of a delivery that was made and acknowledged was never given out again")
	require.Equal(t, id, again)
}

// Deliveries choosing identifiers at once are never given the same one, with
// as many choosing as there are identifiers left free.
func TestConcurrentChoicesNeverRepeatAnIdentifier(t *testing.T) {
	const choosers = 8
	cl, _, _ := newTestClient()
	max := uint16(cl.ops.options.Capabilities.maximumPacketID)
	require.Greater(t, int(max), choosers, "the client has fewer identifiers than goroutines, so this proves nothing")

	var wg sync.WaitGroup
	ids := make(chan uint32, choosers)
	for range choosers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id, err := cl.NextPacketID(); err == nil {
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)

	given := map[uint32]int{}
	for id := range ids {
		given[id]++
	}
	require.Len(t, given, choosers, "identifiers were given out %v, and %d deliveries chose one", given, choosers)
	for id, times := range given {
		require.Equal(t, 1, times, "identifier %d was given to %d deliveries at once", id, times)
	}
}

// A session taken over does not inherit the reservations of the connection it
// replaced: they belong to deliveries being made on that connection, and the
// new one would otherwise avoid identifiers nothing holds.
func TestATakenOverSessionInheritsNoReservations(t *testing.T) {
	i := NewInflights()
	id, ok := i.Claim(0, 10)
	require.True(t, ok)

	got, ok := i.Clone().Claim(0, 10)
	require.True(t, ok)
	require.Equal(t, id, got, "the new connection avoided an identifier reserved on the old one")
}

// pattern is n bytes that differ from their neighbours, so a payload read
// with a piece missing, doubled or shifted does not compare equal.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/251)
	}
	return b
}

// **A packet larger than the read buffer is read whole**, through the
// engine: published at QoS 1 and delivered to a subscriber byte for byte.
// The read buffer is smaller than every one of these, so each is read
// across it; above its size bufio reads straight into the packet's own
// buffer instead.
func TestAPacketLargerThanTheReadBufferIsReadWhole(t *testing.T) {
	s, addr := serveEngine(t)
	defer s.Close()
	sub := dialWire(t, addr, "large-sub", true, 0, 0, 0)
	sub.subscribe(1, "large/+", 1)
	pub := dialWire(t, addr, "large-pub", true, 0, 0, 0)
	for i, n := range []int{600, 2_100, 65_536, 70_000, 300_000} {
		require.Less(t, s.Options.ClientNetReadBufferSize, n, "a %d-byte payload fits the read buffer, so it tests nothing here", n)
		topic := fmt.Sprintf("large/%d", n)
		pub.publish(uint16(i+1), topic, pattern(n), 1)
		pub.expect(pahopackets.PUBACK, uint16(i+1), "the %d-byte publish", n)
		p := sub.expectPublish("the %d-byte delivery", n)
		require.Equal(t, topic, p.Topic)
		require.True(t, bytes.Equal(pattern(n), p.Payload), "the %d-byte payload arrived changed (%d bytes)", n, len(p.Payload))
		sub.ack(p)
	}
}

// readCounter counts the reads made on a connection.
type readCounter struct {
	net.Conn
	reads atomic.Int64
}

func (c *readCounter) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

// **A packet split across many small reads is read whole**: the bytes a
// client sends may arrive a few at a time, and the read buffer is filled
// and emptied many times over for one packet. Over a pipe, each read
// returns at most one write, so the client's writes set the pieces: 7
// bytes, and 513, one past the buffer.
func TestAPacketSplitAcrossManySmallReadsIsReadWhole(t *testing.T) {
	for _, n := range []int{3_000, 70_000} {
		for _, piece := range []int{7, 513} {
			t.Run(fmt.Sprintf("%d bytes in pieces of %d", n, piece), func(t *testing.T) {
				r, w := net.Pipe()
				defer w.Close()
				rc := &readCounter{Conn: r}
				cl := newClient(rc, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
					options: &Options{ClientNetReadBufferSize: 512, Capabilities: NewDefaultServerCapabilities()}})
				cl.Properties.ProtocolVersion = 5

				cp := pahopackets.NewControlPacket(pahopackets.PUBLISH)
				pp := cp.Content.(*pahopackets.Publish)
				pp.Topic, pp.Payload, pp.QoS, pp.PacketID = "split/x", pattern(n), 1, 9
				var raw bytes.Buffer
				_, err := cp.WriteTo(&raw)
				require.NoError(t, err)
				wire := raw.Bytes()
				go func() {
					for b := wire; len(b) > 0; {
						k := min(piece, len(b))
						if _, err := w.Write(b[:k]); err != nil {
							return
						}
						b = b[k:]
					}
				}()

				fh := new(packets.FixedHeader)
				require.NoError(t, cl.ReadFixedHeader(fh))
				pk, err := cl.ReadPacket(fh)
				require.NoError(t, err)
				require.Equal(t, "split/x", pk.TopicName)
				require.Equal(t, uint16(9), pk.PacketID)
				require.True(t, bytes.Equal(pattern(n), pk.Payload), "the payload arrived changed (%d bytes)", len(pk.Payload))

				// The instrument: the packet did arrive in pieces.
				pieces := int64((len(wire) + piece - 1) / piece)
				require.GreaterOrEqual(t, rc.reads.Load(), pieces, "%d reads for %d pieces", rc.reads.Load(), pieces)
				t.Logf("%d bytes in %d pieces of %d: %d reads", len(wire), pieces, piece, rc.reads.Load())
			})
		}
	}
}

// stamped is the payload a test packet carries: its length and every byte
// follow from the producer and sequence number in its topic, so a reader
// can tell a packet whose bytes another write changed.
func stamped(p, i int) []byte {
	size := [...]int{16, 3000, 70 * 1024}[i%3]
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(p*31 + i*7 + j)
	}
	return b
}

// **No two writes share an encode buffer.** writePacket encodes into a
// pooled buffer and gives it back once the packet is written; given back
// before, another write takes it and encodes over bytes still to be sent.
// Written the two ways a connection is written - straight, as a publisher's
// acknowledgement is, and through the write loop, as a delivery is - from
// eight goroutines at once, in the three sizes that reach the socket
// directly, through outbuf, and past the size the pool keeps. Every packet
// must arrive with its own bytes.
func TestNoTwoWritesShareAnEncodeBuffer(t *testing.T) {
	r, w := net.Pipe()
	cl := newClient(w, &ops{
		info:  new(system.Info),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{ClientNetWriteBufferSize: 2048,
			Capabilities: &Capabilities{MaximumClientWritesPending: 64, maximumPacketID: 65535}},
	})
	cl.armWriter()
	defer cl.Stop(nil)

	const producers, each = 8, 120
	type got struct {
		topic   string
		payload []byte
	}
	arrived := make(chan got, producers*each)
	unreadable := make(chan error, 1)
	reader := newClient(r, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{Capabilities: NewDefaultServerCapabilities()}})
	reader.Properties.ProtocolVersion = cl.Properties.ProtocolVersion
	go func() {
		for n := 0; ; n++ {
			fh := new(packets.FixedHeader)
			err := reader.ReadFixedHeader(fh)
			var pk packets.Packet
			if err == nil {
				pk, err = reader.ReadPacket(fh)
			}
			if err != nil {
				unreadable <- fmt.Errorf("after %d packets, a header of %+v: %w", n, *fh, err)
				return
			}
			arrived <- got{pk.TopicName, pk.Payload}
		}
	}()

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
					TopicName: fmt.Sprintf("p%d/%d", p, i), Payload: stamped(p, i)}
				if p%2 == 0 {
					if err := cl.WritePacket(pk); err != nil {
						t.Errorf("producer %d packet %d: %v", p, i, err)
						return
					}
					continue
				}
				for deadline := time.Now().Add(10 * time.Second); !cl.enqueue(&pk); runtime.Gosched() {
					if time.Now().After(deadline) {
						t.Errorf("producer %d could not queue packet %d for 10s", p, i)
						return
					}
				}
			}
		}()
	}
	written := make(chan struct{})
	go func() { wg.Wait(); close(written) }()
	for n := range producers * each {
		select {
		case err := <-unreadable:
			t.Fatalf("the stream stopped parsing - bytes of one packet were changed "+
				"or cut as it was written: %v", err)
		case g := <-arrived:
			var p, i int
			_, err := fmt.Sscanf(g.topic, "p%d/%d", &p, &i)
			require.NoError(t, err, "a topic arrived as %q", g.topic)
			if want := stamped(p, i); !bytes.Equal(want, g.payload) {
				t.Fatalf("%s arrived with %d bytes that are not its own (want %d): another "+
					"write encoded into its buffer before it was sent", g.topic, len(g.payload), len(want))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d packets arrived", n, producers*each)
		}
	}
	<-written
}

// **The Maximum Packet Size is the packet's, not its buffer's.** An encode
// buffer comes from a pool, so it may be far larger than the packet in it:
// here the one a refused 20 KB packet grew. A packet of exactly the
// client's maximum is still written, and one a byte over is still refused
// [MQTT-3.1.2-24] [MQTT-3.1.2-25].
func TestAPacketOfExactlyTheMaximumIsWrittenFromAReusedBuffer(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	publish := func(n int) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
			TopicName: "a/b", Payload: make([]byte, n)}
	}
	read := make(chan int, 4)
	go func() {
		for {
			buf := make([]byte, 64*1024)
			n, err := r.Read(buf)
			if err != nil {
				return
			}
			read <- n
		}
	}()

	require.NoError(t, cl.WritePacket(publish(1000)))
	size := <-read
	cl.Properties.Props.MaximumPacketSize = uint32(size)

	// Repeated, because which buffer the pool hands back is its own choice.
	for range 20 {
		require.ErrorIs(t, cl.WritePacket(publish(20*1000)), packets.ErrPacketTooLarge)
		require.NoError(t, cl.WritePacket(publish(1000)), "a packet of exactly the maximum was refused")
		select {
		case n := <-read:
			require.Equal(t, size, n)
		case <-time.After(5 * time.Second):
			t.Fatal("a packet of exactly the maximum was not written")
		}
		require.ErrorIs(t, cl.WritePacket(publish(1001)), packets.ErrPacketTooLarge)
	}
}

// **A write allocates nothing for the bytes it encodes.** Each packet was
// encoded into a buffer of its own, made at 64 bytes and grown to fit, so a
// QoS 1 message took three allocations for its delivery and its PUBACK. So
// a write now costs the same allocations whatever it carries: a PUBACK, a
// 128-byte publish and a 2 KB one.
func TestAWriteAllocatesNothingForTheBytesItEncodes(t *testing.T) {
	if underRace {
		t.Skip("the race detector's sync.Pool drops a quarter of what it is given back")
	}
	r, w := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{Capabilities: &Capabilities{MaximumClientWritesPending: 64, maximumPacketID: 65535}}})
	defer cl.Stop(errClientStop)
	cl.Properties.ProtocolVersion = 5

	allocs := func(pk packets.Packet) float64 {
		return testing.AllocsPerRun(1000, func() {
			if err := cl.WritePacket(pk); err != nil {
				t.Fatalf("write: %v", err)
			}
		})
	}
	publish := func(n int) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID: 7, TopicName: "sensors/hall", Payload: make([]byte, n)}
	}
	ack := allocs(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: 7})
	small, large := allocs(publish(128)), allocs(publish(2048))
	t.Logf("allocations a write: PUBACK %.1f, 128 B publish %.1f, 2 KB publish %.1f", ack, small, large)
	require.Equal(t, cl.ops.info.MessagesSent.Load(), int64(2*1001),
		"the publishes counted here were not all written") // AllocsPerRun runs each once more to warm up
	require.Equal(t, ack, small, "a 128 B publish allocates more than a PUBACK to be written")
	require.Equal(t, ack, large, "a 2 KB publish allocates more than a PUBACK to be written")
}

// Each packet ReadPacket returns owns its bytes: reading the next packet on
// the connection changes nothing in one already returned. A publish's topic
// and payload are slices of what was read (ReadPacket), held by every
// subscriber's delivery and by the session store, so a read buffer reused
// across packets would rewrite messages those holders already have.
func TestClientReadPacketNextPacketChangesNothingReturned(t *testing.T) {
	cl, r, _ := newTestClient()
	defer cl.Stop(errClientStop)
	cl.Properties.ProtocolVersion = 5

	raw := func(topic, payload string) []byte {
		pk := packets.Packet{ProtocolVersion: 5, FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: topic, Payload: []byte(payload)}
		buf := new(bytes.Buffer)
		require.NoError(t, pk.PublishEncode(buf))
		return buf.Bytes()
	}
	go func() {
		_, _ = r.Write(raw("first/topic", "first payload"))
		_, _ = r.Write(raw("other/topic", "other payload"))
	}()

	read := func() packets.Packet {
		fh := new(packets.FixedHeader)
		require.NoError(t, cl.ReadFixedHeader(fh))
		pk, err := cl.ReadPacket(fh)
		require.NoError(t, err)
		return pk
	}
	first := read()
	require.Equal(t, "first/topic", first.TopicName)
	require.Equal(t, "first payload", string(first.Payload))
	second := read()
	require.Equal(t, "other payload", string(second.Payload))
	require.Equal(t, "first/topic", first.TopicName, "reading the next packet changed this one's topic")
	require.Equal(t, "first payload", string(first.Payload), "reading the next packet changed this one's payload")
}
