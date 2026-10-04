package mqtt

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/mqtt/system"
	"github.com/ifnesi/saguin/internal/sourcetree"
)

// The queue for a client's socket (outQueue): the channel it replaced was
// first in, first out, at most MaximumClientWritesPending packets, and a send
// past that was refused. These hold the same of the queue, and that it holds
// storage only while something is queued.

func mark(i int) *packets.Packet {
	return &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, PacketID: uint16(i)}
}

// **In order across the ring's growth and its wrap.** Pushes and pops
// interleave, so the oldest packet is never at index 0 when the ring grows.
func TestTheSocketQueueIsFirstInFirstOut(t *testing.T) {
	q := newOutQueue(1000)
	next, want := 1, 1
	for round := 0; round < 50; round++ {
		for i := 0; i < 13; i++ {
			require.True(t, q.push(mark(next)))
			next++
		}
		for i := 0; i < 7; i++ {
			pk, _ := q.pop()
			require.NotNil(t, pk)
			require.Equal(t, uint16(want), pk.PacketID, "out of order at round %d", round)
			want++
		}
	}
	for {
		pk, _ := q.pop()
		if pk == nil {
			break
		}
		require.Equal(t, uint16(want), pk.PacketID)
		want++
	}
	require.Equal(t, next, want, "every packet pushed came out once")
}

// **At most max, as the channel's capacity was, and refused past it** -
// never blocked, never grown past it. A slot freed takes the next.
func TestTheSocketQueueRefusesPastItsBound(t *testing.T) {
	q := newOutQueue(5)
	for i := 1; i <= 5; i++ {
		require.True(t, q.push(mark(i)))
	}
	require.False(t, q.push(mark(6)), "a sixth packet was queued past a bound of five")
	require.Equal(t, 5, q.len())
	require.LessOrEqual(t, len(q.buf), 5, "the ring grew past its bound")
	pk, _ := q.pop()
	require.Equal(t, uint16(1), pk.PacketID)
	require.True(t, q.push(mark(6)), "a slot freed by the write loop did not take the next")

	none := newOutQueue(0)
	require.False(t, none.push(mark(1)), "a queue bounded at nothing took a packet")
}

// **An idle client holds no queue storage**, and a client whose queue has
// drained holds a small ring at most. The 64 KiB channel this replaced was
// made at connect whatever came after.
func TestAnIdleClientHoldsNoQueueStorage(t *testing.T) {
	cl, _, _ := newTestClient()
	require.Nil(t, cl.State.outbound.buf, "a client that has queued nothing holds a ring")
	require.Equal(t, int(cl.ops.options.Capabilities.MaximumClientWritesPending), cl.State.outbound.max)

	q := newOutQueue(8192)
	for i := 1; i <= 3; i++ {
		q.push(mark(i))
	}
	for q.len() > 0 {
		q.pop()
	}
	require.LessOrEqual(t, len(q.buf), outQueueKeep, "a small queue that drained kept more than a small ring")

	for i := 1; i <= 1000; i++ {
		q.push(mark(i))
	}
	for q.len() > 0 {
		q.pop()
	}
	require.Nil(t, q.buf, "a burst's ring outlived the burst")
}

// **Storage goes only when nothing is left in it.** A queue drained part of
// the way still holds every packet not yet taken, in order: letting the ring
// go early would lose packets the write loop was still to write.
func TestAQueueKeepsItsRingWhileAnythingIsQueued(t *testing.T) {
	q := newOutQueue(8192)
	for i := 1; i <= 500; i++ {
		require.True(t, q.push(mark(i)))
	}
	for i := 1; i <= 499; i++ {
		pk, more := q.pop()
		require.NotNil(t, pk)
		require.True(t, more, "packet %d of 500 said nothing remained behind it", i)
		require.Equal(t, uint16(i), pk.PacketID)
	}
	require.Equal(t, 1, q.len())
	pk, more := q.pop()
	require.NotNil(t, pk, "the last packet was lost when the ring was let go")
	require.False(t, more)
	require.Equal(t, uint16(500), pk.PacketID)
	pk, _ = q.pop()
	require.Nil(t, pk)
}

// **One signal for a queue with something in it, and pop says what is
// left**, so a write loop woken once takes everything queued: a queue whose
// pushes all landed on a raised signal would otherwise leave all but one
// waiting for a push that may never come. Once it has drained, the next push
// raises the signal again.
func TestAWokenWriteLoopIsToldWhatRemains(t *testing.T) {
	q := newOutQueue(100)
	for i := 1; i <= 10; i++ {
		q.push(mark(i))
	}
	select {
	case <-q.ready:
	default:
		t.Fatal("ten packets were queued with no signal raised")
	}
	for i := 1; i <= 10; i++ {
		pk, more := q.pop()
		require.NotNil(t, pk)
		require.Equal(t, uint16(i), pk.PacketID)
		require.Equal(t, i < 10, more, "after packet %d of 10 pop said more=%v", i, more)
	}
	q.push(mark(11))
	select {
	case <-q.ready:
	default:
		t.Fatal("a push to a drained queue raised no signal")
	}
}

// **The write loop writes everything queued, in order**, whatever the
// producers' interleaving: eight of them queue 250 packets each while the
// loop writes to a pipe, and the other end reads all 2,000, each producer's
// in the order it queued them.
func TestTheWriteLoopWritesEveryQueuedPacketInOrder(t *testing.T) {
	r, w := net.Pipe()
	cl := newClient(w, &ops{
		info:    new(system.Info),
		hooks:   new(Hooks),
		log:     logger,
		options: &Options{Capabilities: &Capabilities{MaximumClientWritesPending: 64, maximumPacketID: 65535}},
	})
	cl.ops.options.Capabilities.MaximumClientWritesPending = 64
	cl.armWriter()
	defer cl.Stop(nil)

	const producers, each = 8, 250
	got := make(chan string, producers*each)
	// One reader for the whole stream: a fresh buffered reader per packet
	// would read ahead into the next and lose it.
	reader := newClient(r, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{Capabilities: NewDefaultServerCapabilities()}})
	reader.Properties.ProtocolVersion = cl.Properties.ProtocolVersion
	go func() {
		for {
			fh := new(packets.FixedHeader)
			if err := reader.ReadFixedHeader(fh); err != nil {
				return
			}
			pk, err := reader.ReadPacket(fh)
			if err != nil {
				return
			}
			got <- pk.TopicName
		}
	}()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				pk := &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
					TopicName: fmt.Sprintf("p%d/%04d", p, i), Payload: []byte("x")}
				for deadline := time.Now().Add(10 * time.Second); !cl.enqueue(pk); runtime.Gosched() {
					if time.Now().After(deadline) {
						t.Errorf("producer %d could not queue packet %d for 10s: the write loop "+
							"stopped taking from the queue", p, i)
						return
					}
				}
			}
		}(p)
	}
	wg.Wait()
	last := map[string]int{}
	for i := 0; i < producers*each; i++ {
		select {
		case topic := <-got:
			var p, n int
			_, err := fmt.Sscanf(topic, "p%d/%d", &p, &n)
			require.NoError(t, err)
			key := fmt.Sprint(p)
			if prev, seen := last[key]; seen {
				require.Equal(t, prev+1, n, "producer %d's packets were written out of order", p)
			} else {
				require.Equal(t, 0, n, "producer %d's first packet was not written first", p)
			}
			last[key] = n
		case <-time.After(5 * time.Second):
			t.Fatalf("%d of %d queued packets were written", i, producers*each)
		}
	}
	// **Waited for, not read at once.** The writer takes a packet off the
	// count only once its socket write has returned (writeQueued), which can
	// be after the reader above has the bytes: at one P it was, every time.
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt32(&cl.State.outboundQty) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.outboundQty),
		"the queue's count did not come back to zero in 10s of every packet having been written")
}

// BenchmarkSocketQueue is the queue against the channel it replaced: n
// producers pushing, one consumer taking what it is woken for, as WriteLoop
// does.
//
//   - "shed" is the broker's shape: each delivery is pushed once, and one
//     that finds the queue full is shed (QoS 0) or withheld (QoS 1 and 2) -
//     never retried. ns/op is per delivery offered; the shed count is
//     reported beside it.
//   - "retry" retries a full push until it lands, so every item goes through:
//     a stress of the lock with producers spinning on a queue that stays
//     full, which the broker never does.
func BenchmarkSocketQueue(b *testing.B) {
	for _, producers := range []int{1, 8, 64} {
		for _, mode := range []string{"shed", "retry"} {
			b.Run(fmt.Sprintf("chan/%s/producers=%d", mode, producers), func(b *testing.B) {
				benchQueue(b, producers, mode, chanQueue(8192))
			})
			b.Run(fmt.Sprintf("fifo/%s/producers=%d", mode, producers), func(b *testing.B) {
				q := newOutQueue(8192)
				benchQueue(b, producers, mode, &q)
			})
		}
	}
}

type benchQ interface {
	push(*packets.Packet) bool
	take() int // blocks until at least one is taken, and says how many
}

type chanQ chan *packets.Packet

func chanQueue(n int) chanQ { return make(chanQ, n) }

func (c chanQ) push(pk *packets.Packet) bool {
	select {
	case c <- pk:
		return true
	default:
		return false
	}
}

func (c chanQ) take() int { <-c; return 1 }

// take is WriteLoop's: woken once, it takes all that is there.
func (q *outQueue) take() int {
	<-q.ready
	n := 0
	for {
		pk, more := q.pop()
		if pk != nil {
			n++
		}
		if !more {
			return n
		}
	}
}

func benchQueue(b *testing.B, producers int, mode string, q benchQ) {
	pk := mark(1)
	per := b.N/producers + 1
	var pushed, shed atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if mode == "shed" {
					if q.push(pk) {
						pushed.Add(1)
					} else {
						shed.Add(1)
					}
					continue
				}
				for !q.push(pk) {
					runtime.Gosched()
				}
				pushed.Add(1)
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()
	taken := int64(0)
	for {
		select {
		case <-done:
			for taken < pushed.Load() {
				taken += int64(q.take())
			}
			b.StopTimer()
			b.ReportMetric(float64(shed.Load())/float64(per*producers)*100, "%shed")
			return
		default:
		}
		if taken < pushed.Load() {
			taken += int64(q.take())
		} else {
			runtime.Gosched()
		}
	}
}

// Invariant 13: what a session is owed while its client is away is bounded
// and counted, and "one unbounded output buffer turns one slow consumer into
// a dead broker". A session that outlives its connection keeps its Client for
// as long as it is away - up to limits.max_session_expiry - so what belonged
// only to the connection has to go with the connection: the packets queued
// for a socket that is closed will never be written, and the read buffer will
// never be read. Measured before this: a subscriber that stopped reading and
// went away kept half its session_queue_bytes queued (516,018 bytes at the
// default) and its 2 KiB read buffer, for the whole of its expiry.
func TestAnAwaySessionKeepsNothingItsConnectionHeld(t *testing.T) {
	s := newServer()
	defer s.Close()
	cl := stalledAway(t, s, "away", 0)

	require.Zero(t, cl.State.outbound.len(), "packets for a closed socket are still queued")
	require.Zero(t, cl.State.outboundBytes.Load(), "the queue's bytes are still counted")
	require.Zero(t, cl.State.outboundQoS.Load())
	cl.Lock()
	bconn, outbuf := cl.Net.bconn, cl.Net.outbuf
	cl.Unlock()
	require.Nil(t, bconn, "the closed connection's read buffer is still held")
	require.Nil(t, outbuf, "the closed connection's write buffer is still held")

	// And nothing queues onto it afterwards.
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a/b", Payload: []byte("x")}
	require.False(t, cl.enqueue(&pk), "a packet was queued for a socket that is closed")
	require.Zero(t, cl.State.outbound.len())
}

// MQTT-4.4.0-1: a session resumed with Clean Start 0 is sent again every QoS 1
// and 2 PUBLISH it had not had acknowledged. Letting go of a closed
// connection's queue must not take any of them with it: each queued QoS 1
// packet is in flight too, and the next connection sends it from there.
//
// **DUP only on what was written before** [MQTT-3.3.1-1]: one still queued,
// never written, is withheld as the queue closes (Inflight.Unqueue) and is a
// first send on the next connection, claimed and checked for expiry. This
// test used to want DUP on every one; the queued ones were then sent as
// re-sends with no expiry check (MQTT-3.3.2-5).
func TestAQoS1DeliveryQueuedAsItsConnectionEndedIsSentAgainOnResume(t *testing.T) {
	s := newServer()
	defer s.Close()
	// Make every PUBLISH reach the connection rather than being counted as
	// sent while it waits in the client's write buffer.
	s.Options.ClientNetWriteBufferSize = 1
	const n, pass = 20, 3
	var held *holdsPublishWrites
	away := stalledAwayOn(t, s, "resumer", n, func(c net.Conn) net.Conn {
		held = newHoldsPublishWrites(c, pass)
		return held
	}, func() {
		select {
		case <-held.blocked:
		case <-time.After(5 * time.Second):
			t.Fatal("the connection did not hold the PUBLISH after the allowed prefix")
		}
	})
	unsent, never := map[uint16]bool{}, 0
	written, unwritten := []uint16{}, []uint16{}
	for _, pk := range away.State.Inflight.GetAll(false) {
		if unsent[pk.PacketID] = pk.Expiry < 0; unsent[pk.PacketID] {
			never++
			unwritten = append(unwritten, pk.PacketID)
		} else {
			written = append(written, pk.PacketID)
		}
	}
	t.Logf("away with %d in flight: written %v, never written %v; connection passed %d PUBLISH writes and held the next",
		len(unsent), written, unwritten, pass)
	require.Equal(t, int32(pass+1), held.writes.Load(),
		"the controlled connection did not pass %d PUBLISH writes and hold the next", pass)
	require.Positive(t, never, "nothing was left unwritten, so this proves nothing")
	require.Equal(t, n-pass-1, never,
		"the controlled connection did not leave the expected tail unwritten: written %v, never written %v", written, unwritten)

	r, w := net.Pipe()
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	peer := connectPipe(t, s, w, "resumer")
	got := map[uint16]bool{}
	for len(got) < n {
		require.NoError(t, w.SetReadDeadline(time.Now().Add(5*time.Second)))
		fh := new(packets.FixedHeader)
		require.NoError(t, peer.ReadFixedHeader(fh), "the resumed session was sent %d of %d: %v", len(got), n, got)
		pk, err := peer.ReadPacket(fh)
		require.NoError(t, err)
		require.Equal(t, packets.Publish, pk.FixedHeader.Type, "the broker sent %+v", pk)
		require.Equal(t, !unsent[pk.PacketID], pk.FixedHeader.Dup,
			"DUP is not whether the delivery was written before (never written %v): %+v", unsent[pk.PacketID], pk)
		got[pk.PacketID] = true
	}
	require.Len(t, got, n)
}

// TestAnAwaySessionKeepsNothingItsConnectionHeld, with the write loop held
// after a write and before it lets go of the packet it took off the queue -
// where it was, now and then, as the connection ended. It holds that packet
// counted until it returns, and released without waiting for the loop, the
// away session kept it counted: that test saw it in 2 runs of 30 under -race. Held here every time: the connection takes
// the loop's PUBLISH writes without passing them on, the connection is ended
// only once the loop is held in OnPacketSent after its first, and the loop
// stays held until the release is waiting for it - or, where the release
// does not wait, until the counts have been read.
func TestAnAwaySessionKeepsNothingItsWriteLoopWasStillHolding(t *testing.T) {
	s := newServer()
	defer s.Close()
	h := &heldAfterWrite{entered: make(chan struct{}), letGo: make(chan struct{})}
	var once sync.Once
	letGo := func() { once.Do(func() { close(h.letGo) }) }
	defer letGo()
	require.NoError(t, s.AddHook(h, nil))
	cl := stalledAwayOn(t, s, "away", 0, func(c net.Conn) net.Conn { return &takesPublishes{Conn: c} },
		func() {
			select {
			case <-h.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the write loop never wrote a PUBLISH, so this measures nothing")
			}
		})
	outbound, bytes, qty := cl.State.outbound.len(), cl.State.outboundBytes.Load(), atomic.LoadInt32(&cl.State.outboundQty)
	qos := cl.State.outboundQoS.Load()
	letGo()
	require.Positive(t, h.held.Load(), "the write loop was never held after a write, so this measures nothing")
	require.Zero(t, outbound, "packets for a closed socket are still queued")
	require.Zero(t, bytes, "the queue's bytes are still counted")
	require.Zero(t, qty, "the queue's packets are still counted")
	require.Zero(t, qos)
	require.True(t, h.waited.Load(), "the loop was let go before the release waited for it, so this measures nothing")
}

// heldAfterWrite holds the write loop in OnPacketSent for its first PUBLISH,
// saying so on entered, until the connection's release is waiting for the
// loop to return or the test closes letGo.
//
// **Held until the release waits, not for a while.** The window is a loop
// holding a packet as the connection is released; a hold measured in time
// ended before the release in most runs without the race detector, which
// schedules differently, and the test then measured nothing (17 runs in 20). The release waiting on writerDone is seen in the goroutine
// dump rather than through a seam in releaseConnection, so that a release
// that does not wait cannot also skip telling the hook to let go.
type heldAfterWrite struct {
	HookBase
	held    atomic.Int32
	waited  atomic.Bool // let go because the release was waiting
	entered chan struct{}
	letGo   chan struct{}
}

func (h *heldAfterWrite) ID() string { return "held-after-write" }
func (h *heldAfterWrite) Provides(b byte) bool {
	return b == OnPacketSent
}
func (h *heldAfterWrite) OnPacketSent(cl *Client, pk packets.Packet, _ []byte) {
	if pk.FixedHeader.Type != packets.Publish || !h.held.CompareAndSwap(0, 1) {
		return
	}
	close(h.entered)
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); {
		if releaseWaitsOnTheWriteLoop() {
			h.waited.Store(true)
			return
		}
		select {
		case <-h.letGo:
			return
		case <-time.After(time.Millisecond):
		}
	}
}

// releaseWaitsOnTheWriteLoop is whether a goroutine is blocked receiving in
// releaseConnection, which waits there only on writerDone.
func releaseWaitsOnTheWriteLoop() bool {
	buf := make([]byte, 1<<20)
	for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "[chan receive") && strings.Contains(g, "mqtt.(*Client).releaseConnection(") {
			return true
		}
	}
	return false
}

// takesPublishes is a connection that takes every write beginning with a
// PUBLISH without passing it on, so the loop's writes succeed while nothing
// reads them, and passes every other write through.
type takesPublishes struct{ net.Conn }

func (c *takesPublishes) Write(b []byte) (int, error) {
	if len(b) > 0 && b[0]>>4 == packets.Publish {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// holdsPublishWrites accepts a fixed prefix of PUBLISH writes, then holds the
// next until the connection is closed. It models a socket whose send buffer
// filled at a known point, independent of the machine and scheduler.
type holdsPublishWrites struct {
	net.Conn
	pass      int32
	writes    atomic.Int32
	blocked   chan struct{}
	closed    chan struct{}
	blockOnce sync.Once
	closeOnce sync.Once
}

func newHoldsPublishWrites(c net.Conn, pass int32) *holdsPublishWrites {
	return &holdsPublishWrites{Conn: c, pass: pass, blocked: make(chan struct{}), closed: make(chan struct{})}
}

func (c *holdsPublishWrites) Write(b []byte) (int, error) {
	if len(b) == 0 || b[0]>>4 != packets.Publish {
		return c.Conn.Write(b)
	}
	if c.writes.Add(1) <= c.pass {
		return len(b), nil
	}
	c.blockOnce.Do(func() { close(c.blocked) })
	<-c.closed
	return 0, net.ErrClosed
}

func (c *holdsPublishWrites) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// stalledAway connects id with a session that outlives its connection, has
// nothing read from its socket while n QoS 1 deliveries (or 20 at QoS 0 where
// n is 0) are made to it - so one holds the write loop and the rest wait in
// the queue - then ends the connection and returns the Client its session
// keeps.
func stalledAway(t *testing.T, s *Server, id string, n int) *Client {
	t.Helper()
	return stalledAwayOn(t, s, id, n, nil, nil)
}

// stalledAwayOn is stalledAway with the server's end of the connection
// wrapped, where wrap is not nil, and before called just before the
// connection is ended, where it is not nil.
func stalledAwayOn(t *testing.T, s *Server, id string, n int, wrap func(net.Conn) net.Conn, before func()) *Client {
	t.Helper()
	r, w := net.Pipe()
	var conn net.Conn = r
	if wrap != nil {
		conn = wrap(r)
	}
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", conn) }()
	connectPipe(t, s, w, id)

	// Registered just after the CONNACK is written, so it may lag the read.
	var cl *Client
	require.Eventually(t, func() bool {
		var ok bool
		cl, ok = s.Clients.Get(id)
		return ok
	}, 5*time.Second, time.Millisecond, "the client never registered")

	qos := byte(1)
	if n == 0 {
		n, qos = 20, 0
	}
	payload := bytes.Repeat([]byte("x"), 1024)
	for range n {
		_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b", Qos: qos},
			packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}, TopicName: "a/b", Payload: payload})
		require.NoError(t, err)
	}
	require.Positive(t, cl.State.outbound.len(), "nothing was queued, so this measures nothing")

	if before != nil {
		before()
	}
	require.NoError(t, w.Close())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never ended")
	}
	require.True(t, cl.Closed())
	held, ok := s.Clients.Get(id)
	require.True(t, ok && held == cl, "the session did not outlive its connection, so this proves nothing")
	return cl
}

// connectPipe writes an MQTT 5 CONNECT for id on w - Clean Start 0, a session
// that outlives the connection - reads the CONNACK, and returns a Client that
// reads what the broker sends on w.
func connectPipe(t *testing.T, s *Server, w net.Conn, id string) *Client {
	t.Helper()
	var body bytes.Buffer
	connect := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
		Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: id, Keepalive: 30},
		Properties: packets.Properties{SessionExpiryInterval: 3600, SessionExpiryIntervalFlag: true}}
	require.NoError(t, connect.ConnectEncode(&body))
	go func() { _, _ = w.Write(body.Bytes()) }()
	peer := s.NewClient(w, "peer", "peer", false)
	peer.Properties.ProtocolVersion = 5
	require.NoError(t, w.SetReadDeadline(time.Now().Add(5*time.Second)))
	fh := new(packets.FixedHeader)
	require.NoError(t, peer.ReadFixedHeader(fh))
	pk, err := peer.ReadPacket(fh)
	require.NoError(t, err)
	require.Equal(t, packets.Connack, pk.FixedHeader.Type, "the broker answered the CONNECT with %+v", pk)
	require.Zero(t, pk.ReasonCode, "the CONNECT was refused: %+v", pk)
	return peer
}

// A QoS 0 delivery that loses the race with its connection's end - the
// connection was open when the delivery was prepared, and its queue closed
// (releaseConnection) before the packet reached it - went to a connection
// that has ended, and is answered as one, as the check before it answers a
// delivery to a connection already closed. It is not a delivery dropped for a
// full queue: counted by OnPublishDropped, it read as a subscriber too slow to
// keep up, and the queue it was said to have filled was empty.
func TestAQoS0DeliveryRacingItsConnectionsEndIsNotCountedAsDropped(t *testing.T) {
	s := newServer()
	defer s.Close()
	drops := new(dropCounter)
	require.NoError(t, s.AddHook(drops, nil))

	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()
	cl := s.NewClient(r, "tcp", "racer", false)
	cl.State.outbound.close() // the end, after the delivery found the client open
	require.False(t, cl.Closed(), "the client is closed, so this is the check before the queue rather than the race")

	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b"},
		packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "a/b", Payload: []byte("x")})
	require.ErrorIs(t, err, packets.CodeDisconnect, "a delivery to a connection that has ended was answered %v", err)
	require.Zero(t, drops.n.Load(), "a delivery to a connection that has ended was counted as dropped for a full queue")
}

// dropCounter counts OnPublishDropped.
type dropCounter struct {
	HookBase
	n atomic.Int32
}

func (h *dropCounter) ID() string { return "drop-counter" }

func (h *dropCounter) Provides(b byte) bool { return b == OnPublishDropped }

func (h *dropCounter) OnPublishDropped(*Client, packets.Packet) { h.n.Add(1) }

// writeLoops counts the write loops running for each of cls, from every
// goroutine's stack: a parked loop is found by its receiver.
func writeLoops(cls ...*Client) (loops, readers int) {
	buf := make([]byte, 64<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])
	for _, cl := range cls {
		loops += strings.Count(stacks, fmt.Sprintf("mqtt.(*Client).WriteLoop(%p", cl))
		readers += strings.Count(stacks, fmt.Sprintf("mqtt.(*Client).Read(%p", cl))
	}
	return loops, readers
}

// **An idle connection has no write loop until it is sent something**, and
// has one once it is: a connection that is only subscribed was written its
// CONNACK and SUBACK by its reader, and held a goroutine parked on an empty
// queue for as long as it stayed.
func TestAnIdleClientHasNoWriteLoopUntilItIsSentSomething(t *testing.T) {
	const n = 50
	s, addr := serveEngine(t)
	defer s.Close()
	subs := make([]*wire, n)
	for i := range n {
		subs[i] = dialWire(t, addr, fmt.Sprintf("idle-%d", i), false, 3600, 0, 0)
		subs[i].subscribe(1, fmt.Sprintf("idle/%d", i), 1)
	}
	cls := make([]*Client, n)
	for i := range n {
		var ok bool
		cls[i], ok = s.Clients.Get(fmt.Sprintf("idle-%d", i))
		require.True(t, ok, "idle-%d is not registered", i)
	}

	loops, readers := writeLoops(cls...)
	require.Equal(t, n, readers, "the stacks show %d of %d readers, so they cannot show the write loops", readers, n)
	require.Zero(t, loops, "%d of %d idle connections hold a write loop", loops, n)

	// A publisher at QoS 0 is written nothing back, so it starts none of its
	// own; each subscriber is sent one delivery.
	pub := dialWire(t, addr, "idle-pub", true, 0, 0, 0)
	for i := range n {
		pub.publish(0, fmt.Sprintf("idle/%d", i), []byte("x"), 0)
	}
	for i, w := range subs {
		p := w.expectPublish("the delivery to idle-%d", i)
		require.Equal(t, fmt.Sprintf("idle/%d", i), p.Topic)
		w.ack(p)
	}
	loops, readers = writeLoops(cls...)
	require.Equal(t, n, readers)
	require.Equal(t, n, loops, "every connection sent a delivery has one write loop")
}

// **A packet queued as the write loop is armed is written**, and in order:
// armWriter asks for what is queued after it arms, and a push after the
// arming starts the loop itself, so whichever comes second sees the other.
// Queued before the arming, after it, and raced 200 times as scheduling
// decides.
func TestAPacketQueuedAsTheWriterIsArmedIsWritten(t *testing.T) {
	const each = 20
	for _, order := range []string{"queued first", "armed first", "raced"} {
		trials := 1
		if order == "raced" {
			trials = 200
		}
		t.Run(order, func(t *testing.T) {
			for trial := range trials {
				armQueueAndRead(t, order, trial, each)
			}
		})
	}
}

// armQueueAndRead queues each packets and arms the write loop, in order, and
// fails unless all of them are written in the order they were queued.
func armQueueAndRead(t *testing.T, order string, trial, each int) {
	{
		r, w := net.Pipe()
		cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
			options: &Options{Capabilities: &Capabilities{MaximumClientWritesPending: 64, maximumPacketID: 65535}}})
		cl.Properties.ProtocolVersion = 5
		reader := newClient(r, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
			options: &Options{Capabilities: NewDefaultServerCapabilities()}})
		reader.Properties.ProtocolVersion = 5
		got := make(chan string, each)
		go func() {
			for {
				fh := new(packets.FixedHeader)
				if err := reader.ReadFixedHeader(fh); err != nil {
					return
				}
				pk, err := reader.ReadPacket(fh)
				if err != nil {
					return
				}
				got <- pk.TopicName
			}
		}()

		queue := func() {
			for i := range each {
				require.True(t, cl.enqueue(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
					TopicName: fmt.Sprintf("t/%02d", i), Payload: []byte("x")}))
			}
		}
		switch order {
		case "queued first":
			queue()
			cl.armWriter()
		case "armed first":
			cl.armWriter()
			queue()
		default:
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); <-start; queue() }()
			go func() { defer wg.Done(); <-start; cl.armWriter() }()
			close(start)
			wg.Wait()
		}

		for i := range each {
			select {
			case topic := <-got:
				require.Equal(t, fmt.Sprintf("t/%02d", i), topic, "%s, trial %d: out of order", order, trial)
			case <-time.After(2 * time.Second):
				loops, _ := writeLoops(cl)
				t.Fatalf("%s, trial %d: %d of %d packets written, %d queued, %d write loops: one was left waiting "+
					"for a loop that never started", order, trial, i, each, cl.State.outbound.len(), loops)
			}
		}
		cl.Stop(nil)
		_ = r.Close()
	}
}

// **No write loop starts for a client that has none to serve**: one no
// connection armed (the inline client, a session restored from the store),
// and one whose connection has ended. A packet queued for either stays
// queued, as it did while the loop was started with the connection.
func TestNoWriteLoopStartsWithoutAConnectionToServe(t *testing.T) {
	pk := func() *packets.Packet {
		return &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "t", Payload: []byte("x")}
	}
	for _, c := range []struct {
		name  string
		setup func(cl *Client)
	}{
		{"never armed", func(cl *Client) {}},
		{"armed, then ended", func(cl *Client) { cl.armWriter(); cl.Stop(nil) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			// A loop started by mistake after the end may still take the
			// packet or return first, as its select chooses: 20 trials.
			for trial := range 20 {
				_, w := net.Pipe()
				cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
					options: &Options{Capabilities: &Capabilities{MaximumClientWritesPending: 8, maximumPacketID: 65535}}})
				c.setup(cl)
				require.True(t, cl.enqueue(pk()), "trial %d: the packet was not queued, so nothing could take it", trial)
				cl.WakeWriter()
				require.Never(t, func() bool { l, _ := writeLoops(cl); return l > 0 || cl.State.outbound.len() != 1 },
					30*time.Millisecond, 3*time.Millisecond, "trial %d: a write loop started, or the queued packet was taken", trial)
				// The instrument: the same client, armed and open, does start one.
				if c.name == "never armed" {
					cl.armWriter()
					require.Eventually(t, func() bool { l, _ := writeLoops(cl); return l == 1 }, 2*time.Second, 3*time.Millisecond,
						"arming a client with a packet queued started no write loop, so this test sees none")
				}
				cl.Stop(nil)
			}
		})
	}
}

// A connection that ends between startWriter's
// look at it and its claim of the loop is not given one. The window is held
// open by the seam between the two, which ends the connection there; a loop
// that begins is counted where it begins, so one that returns at once is
// still seen. The control is the same client with nothing ending it in the
// window, which does start one.
//
// **Only this test's client is counted.** The seam is process-wide, and a
// write loop an earlier test started for its own client can begin late - at
// one P, inside this test's window - and read as this one's.
func TestNoWriteLoopStartsForAConnectionThatEndsAsItIsStarted(t *testing.T) {
	var begun atomic.Int32
	var counted atomic.Pointer[Client]
	began := func(c *Client) {
		if c == counted.Load() {
			begun.Add(1)
		}
	}
	writeLoopBegins.Store(&began)
	defer writeLoopBegins.Store(nil)
	for _, ends := range []bool{false, true} {
		begun.Store(0)
		_, w := net.Pipe()
		cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
			options: &Options{Capabilities: &Capabilities{MaximumClientWritesPending: 8, maximumPacketID: 65535}}})
		counted.Store(cl)
		cl.armWriter()
		stop := func(c *Client) {
			if ends {
				c.Stop(nil)
			}
		}
		startWriterBeforeClaim.Store(&stop)
		pk := &packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "t", Payload: []byte("x")}
		require.True(t, cl.enqueue(pk), "the packet was not queued, so nothing asked for a loop")
		startWriterBeforeClaim.Store(nil)
		if !ends {
			require.Eventually(t, func() bool { return begun.Load() == 1 }, 2*time.Second, time.Millisecond,
				"an open connection with a packet queued started no write loop, so this sees none")
			cl.Stop(nil)
			continue
		}
		require.True(t, cl.Closed(), "the seam did not end the connection in the window")
		require.Never(t, func() bool { return begun.Load() > 0 }, 50*time.Millisecond, time.Millisecond,
			"a write loop began for a connection that had ended before it was claimed")
		select {
		case <-cl.State.writerDone:
		default:
			t.Fatal("the loop claimed and not started is not recorded as returned, so releasing the connection would wait on it")
		}
	}
}

// **Only startWriter starts the write loop**, so there is one per
// connection by construction: two would take from one queue at once and
// write its packets out of order [MQTT-4.6.0-1]. Walks the syntax tree of
// every Go file in the repository, tests included, for every mention of
// WriteLoop.
func TestOnlyStartWriterStartsTheWriteLoop(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	var scanned, starts int
	var other []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err, path)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if g, ok := n.(*ast.GoStmt); ok && fn.Name.Name == "startWriter" {
					if sel, ok := g.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "WriteLoop" && len(g.Call.Args) == 0 {
						starts++
						return false
					}
				}
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "WriteLoop" {
					rel, _ := filepath.Rel(root, path)
					other = append(other, fmt.Sprintf("%s:%d in %s", rel, fset.Position(sel.Pos()).Line, fn.Name.Name))
				}
				return true
			})
		}
		return nil
	})
	require.NoError(t, err)
	t.Logf("%d Go files examined, %d starts in startWriter", scanned, starts)
	require.Greater(t, scanned, 100, "the walk is not reaching the repository")
	require.Equal(t, 1, starts, "startWriter does not start the write loop, so the walk has stopped matching it")
	require.Empty(t, other, "the write loop is started or named outside startWriter; start it with armWriter or startWriter")
}
