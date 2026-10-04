package mqtt

import (
	"sync"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// outQueue is a client's queue of packets for its socket: first in, first
// out, at most max of them, taken by WriteLoop one at a time.
//
// **Storage only while something waits.** It replaced a buffered channel of
// MaximumClientWritesPending slots made at connect: 8,192 pointers, 64 KiB a
// client, whether anything was ever queued or not - 89% of an idle
// connection's heap, and kept for an offline durable session until it
// expired, because the engine keeps the session's client. Measured with
// 20,000 idle connections: 1.23 GB of 1.39 GB. The ring grows as packets are
// queued, up to max, and is let go once it is empty.
//
// **Everything else is the channel's.** A push past max is refused and the
// caller decides, as a full channel's non-blocking send was, and the order is
// the order of the pushes. ready carries at most one signal, raised by a push;
// WriteLoop, woken by it, takes packets while pop says more remain, and asks
// wake and open.Done between every two of them, as its select asked them
// between every two receives. A stopped client's queue keeps what it held, as
// the channel did: nothing reads it after WriteLoop returns, and whether it is
// empty is still what OutboundHasRoom and WritePacket ask.
//
// **One lock per packet on the way out, not three.** Re-raising ready after
// every pop so WriteLoop's select came back for the next measured 75% slower
// than the channel with 64 producers (BenchmarkSocketQueue): the loop takes
// the next with the one lock a pop takes, and needs no signal to come back.
type outQueue struct {
	mu   sync.Mutex
	buf  []*packets.Packet
	head int // index of the oldest packet in buf
	n    int // packets queued
	max  int

	// closed is set when the connection ends (close), and refuses every
	// push after it.
	closed bool

	// size is n, readable without mu: whether the queue is full or empty is
	// asked by every publisher delivering to this client (OutboundHasRoom)
	// and by every write (WritePacket), and a full queue refuses a push
	// without taking the lock, as a full channel refused a send. Taking mu
	// for it measured the queue at twice the channel's cost with 64
	// producers against one full queue (BenchmarkSocketQueue).
	size atomic.Int64

	// ready has one slot, filled while something is queued and WriteLoop has
	// not yet been told.
	ready chan struct{}
}

// outQueueKeep is the ring a queue keeps when it empties. Above it the
// storage goes, so a burst does not leave its high-water mark behind for the
// life of the connection; at or below it a busy client keeps a ring that
// costs half a kilobyte rather than allocating one per packet.
const outQueueKeep = 64

func newOutQueue(max int) outQueue {
	return outQueue{max: max, ready: make(chan struct{}, 1)}
}

// push queues pk, and reports false where the queue already holds max or has
// been closed.
func (q *outQueue) push(pk *packets.Packet) bool {
	if q.size.Load() >= int64(q.max) {
		return false
	}
	q.mu.Lock()
	if q.n >= q.max || q.closed {
		q.mu.Unlock()
		return false
	}
	if q.n == len(q.buf) {
		q.grow()
	}
	q.buf[(q.head+q.n)%len(q.buf)] = pk
	q.n++
	q.size.Store(int64(q.n))
	q.mu.Unlock()
	q.signal()
	return true
}

// close ends the queue with its connection: it gives back every packet still
// queued, lets go of the ring, and refuses every push after it. A pop after
// it finds nothing.
func (q *outQueue) close() []*packets.Packet {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	queued := make([]*packets.Packet, 0, q.n)
	for i := 0; i < q.n; i++ {
		queued = append(queued, q.buf[(q.head+i)%len(q.buf)])
	}
	q.buf, q.head, q.n = nil, 0, 0
	q.size.Store(0)
	return queued
}

// ended reports whether the queue has been closed with its connection.
func (q *outQueue) ended() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// grow doubles the ring, from 16, up to max, keeping the order. The caller
// holds mu.
func (q *outQueue) grow() {
	size := 2 * len(q.buf)
	if size < 16 {
		size = 16
	}
	if size > q.max {
		size = q.max
	}
	ring := make([]*packets.Packet, size)
	for i := 0; i < q.n; i++ {
		ring[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf, q.head = ring, 0
}

// pop takes the oldest packet, or nil where nothing is queued, and reports
// whether more remain behind it.
func (q *outQueue) pop() (pk *packets.Packet, more bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.n == 0 {
		return nil, false
	}
	pk = q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.n--
	q.size.Store(int64(q.n))
	if q.n == 0 {
		q.head = 0
		if len(q.buf) > outQueueKeep {
			q.buf = nil
		}
	}
	return pk, q.n > 0
}

// len is how many packets are queued, read without the lock.
func (q *outQueue) len() int { return int(q.size.Load()) }

// signal tells WriteLoop something is queued, unless it has been told.
func (q *outQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}
