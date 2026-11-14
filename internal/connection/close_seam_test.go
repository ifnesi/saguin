package connection_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// These tests drive one connection's close step by step through the seam
// export_test.go states: a scripted socket that holds every call its reader
// and writer make until the test answers it, and a fake clock that moves
// only when the test moves it. Nothing in them depends on which goroutine
// runs first or on how long anything takes: each waits for the next socket
// call or event. Each runs in a synctest bubble, so that synctest.Wait can
// tell when the connection's goroutines have all stopped at a channel (the
// writer waiting, or holding out its next socket call), and so that
// backstop, on the bubble's own time, ends at once a test whose code never
// makes the call it waits for.

// seamTimeout is limits.write_timeout here. It runs on the fake clock, so
// its size costs no time.
const seamTimeout = 10 * time.Second

// epoch is the fake clock's first reading: before any real time the tests
// run at, so a time read from the real clock shows in a deadline.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// The scripted socket's operations, as calls name them.
const (
	opRead          = "Read"
	opWrite         = "Write"
	opReadDeadline  = "SetReadDeadline"
	opWriteDeadline = "SetWriteDeadline"
	opCloseWrite    = "CloseWrite"
	opClose         = "Close"
)

// The two goroutines that use a connection's socket.
const (
	reader = "reader"
	writer = "writer"
)

// fakeClock is the connection's clock, moved only by set. A test reads and
// moves it from its own goroutine while the connection's read it, hence the
// mutex: this is test code, not the broker's (R3).
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

// fakeTimer is a channel At handed out, waiting for the clock to reach at.
type fakeTimer struct {
	at time.Time
	c  chan time.Time
}

// Now returns the clock's time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// At returns a channel that receives once the clock has reached t, at once
// where it has.
func (c *fakeClock) At(t time.Time) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.arm(ch, t)
	return ch
}

// Timer is At's channel and a reset that moves its timer to another time,
// or stops it at the zero time, a fire not yet received dropped. The
// connections wait with At alone; the
// core's loop moves its one timer (design.md, Testing), and Timer makes
// the fake a whole Clock.
func (c *fakeClock) Timer(t time.Time) (timer <-chan time.Time, reset func(time.Time)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.arm(ch, t)
	return ch, func(t time.Time) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.timers = slices.DeleteFunc(c.timers, func(tm fakeTimer) bool { return tm.c == ch })
		select {
		case <-ch:
		default:
		}
		if !t.IsZero() {
			c.arm(ch, t)
		}
	}
}

// arm has ch receive once the clock has reached t, at once where it has.
// The caller holds the mutex.
func (c *fakeClock) arm(ch chan time.Time, t time.Time) {
	if !c.now.Before(t) {
		ch <- c.now
		return
	}
	c.timers = append(c.timers, fakeTimer{at: t, c: ch})
}

// set moves the clock to t, never back, and fires every timer t reaches.
func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.Before(c.now) {
		panic(fmt.Sprintf("the fake clock moved back from %v to %v", c.now, t))
	}
	c.now = t
	waiting := c.timers[:0]
	for _, tm := range c.timers {
		if t.Before(tm.at) {
			waiting = append(waiting, tm)
			continue
		}
		tm.c <- t
	}
	c.timers = waiting
}

// sockCall is one socket operation, held until the test answers it.
type sockCall struct {
	who  string    // reader or writer
	op   string    // one of the op constants
	p    []byte    // Read's buffer, or Write's bytes
	t    time.Time // a deadline's value
	at   time.Time // the clock when the call was made
	done chan sockResult
}

// sockResult is a call's answer.
type sockResult struct {
	n   int
	err error
}

// String describes c for a failure.
func (c *sockCall) String() string {
	switch c.op {
	case opReadDeadline, opWriteDeadline:
		return fmt.Sprintf("%s(%v) at %v", c.op, c.t, c.at)
	case opRead, opWrite:
		return fmt.Sprintf("%s of %d bytes at %v", c.op, len(c.p), c.at)
	}
	return fmt.Sprintf("%s() at %v", c.op, c.at)
}

// ok answers a call with success: a whole Write, or a deadline set.
func (c *sockCall) ok() { c.done <- sockResult{n: len(c.p)} }

// give answers a Read with what of data fits its buffer, and returns how
// much that was.
func (c *sockCall) give(data []byte) int {
	n := copy(c.p, data)
	c.done <- sockResult{n: n}
	return n
}

// fail answers a call with err.
func (c *sockCall) fail(err error) { c.done <- sockResult{err: err} }

// deadlinePassed is what a real socket's Read returns once its read
// deadline has passed.
var deadlinePassed = &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}

// closedSocket is what a real socket's Read returns once it is closed.
var closedSocket = &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}

// scriptedSocket is the connection's socket: each call goes to the test
// and waits for its answer. The goroutine making the first call is the
// reader, which sets connect_timeout before the writer has anything to
// write; any other is the writer. The test's own goroutine plays the core,
// whose one socket call is Cut's Close: it is noted and returns at once,
// as the core never waits (R2).
type scriptedSocket struct {
	clock *fakeClock
	calls chan *sockCall
	gone  chan struct{} // closed when the test ends: every call fails at once
	core  string        // the test's goroutine id, set before the connection starts

	mu     sync.Mutex
	reader string // the reader's goroutine id
	cut    bool   // the core has closed the socket
}

// wasCut reports whether the core has closed the socket.
func (s *scriptedSocket) wasCut() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cut
}

func (s *scriptedSocket) call(op string, p []byte, t time.Time) (int, error) {
	id := testkit.Goid()
	s.mu.Lock()
	if id == s.core {
		// design.md, Flows, Closing a connection: "The core cuts a connection
		// only by closing its raw socket".
		defer s.mu.Unlock()
		if op != opClose {
			panic(fmt.Sprintf("the core called %s on a connection's socket: it only closes it (R2)", op))
		}
		s.cut = true
		return 0, nil
	}
	if s.reader == "" {
		s.reader = id
	}
	who := writer
	if id == s.reader {
		who = reader
	}
	s.mu.Unlock()
	c := &sockCall{who: who, op: op, p: p, t: t, at: s.clock.Now(), done: make(chan sockResult, 1)}
	select {
	case s.calls <- c:
	case <-s.gone:
		return 0, closedSocket
	}
	select {
	case r := <-c.done:
		return r.n, r.err
	case <-s.gone:
		return 0, closedSocket
	}
}

func (s *scriptedSocket) Read(p []byte) (int, error)  { return s.call(opRead, p, time.Time{}) }
func (s *scriptedSocket) Write(p []byte) (int, error) { return s.call(opWrite, p, time.Time{}) }

func (s *scriptedSocket) SetReadDeadline(t time.Time) error {
	_, err := s.call(opReadDeadline, nil, t)
	return err
}

func (s *scriptedSocket) SetWriteDeadline(t time.Time) error {
	_, err := s.call(opWriteDeadline, nil, t)
	return err
}

func (s *scriptedSocket) CloseWrite() error {
	_, err := s.call(opCloseWrite, nil, time.Time{})
	return err
}

func (s *scriptedSocket) Close() error {
	_, err := s.call(opClose, nil, time.Time{})
	return err
}

func (s *scriptedSocket) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1883}
}

// seam is one connection on a scripted socket, the test in the core's place
// on an unbuffered inbound channel: the reader's send of an event waits
// until the test takes it.
type seam struct {
	t       *testing.T
	clock   *fakeClock
	sock    *scriptedSocket
	inbound chan connection.Event
	conn    *connection.Conn
	reg     *metrics.Registry
	metrics *connection.Metrics // the connection's series, counted into reg

	pending map[string]*sockCall // a call taken while waiting for the other goroutine's
	reading *sockCall            // the reader's Read, held
	stopped bool                 // the reader's last read was answered: it must call nothing more

	// decide, where set, is the core in the stand-in's place: it takes
	// each event the tests hand the core and sends the replies itself.
	decide func(connection.Event)
	runID  string // the goroutine running the listener's Run

	// What the core's stand-in knows of the connection.
	final    bool // it has sent its final entry
	ended    bool // it has taken the reader's Ended
	reported bool // it has taken the writer's close report
	shut     bool // the test has answered the writer's Close
	// publishes are the PUBLISHes finish's core has taken.
	publishes []*packet.Publish
}

// startSeam runs a listener of newConfig's configuration whose one socket
// is scripted, and returns once the core has been told of it.
func startSeam(t *testing.T) *seam {
	t.Helper()
	s := launchSeam(t, newConfig(t))
	s.accepted()
	return s
}

// launchSeam runs a listener of cfg, its write_timeout seamTimeout and no
// accept rate, whose one socket is scripted, each of options applied to it
// before it runs, and returns at once: the listener's Accepted waits until
// the test takes it with accepted.
func launchSeam(t *testing.T, cfg *config.Config, options ...func(*connection.Listener)) *seam {
	t.Helper()
	cfg.Broker.Limits.WriteTimeout = seamTimeout
	cfg.Broker.Limits.MaxConnectRate = 0 // no rate bucket: the listener reads no clock for it
	clk := &fakeClock{now: epoch}
	s := &seam{
		t:       t,
		clock:   clk,
		sock:    &scriptedSocket{clock: clk, calls: make(chan *sockCall), gone: make(chan struct{}), core: testkit.Goid()},
		inbound: make(chan connection.Event),
		pending: map[string]*sockCall{},
		reg:     metrics.New(),
	}
	reg := s.reg
	s.metrics = connection.NewMetrics(reg)
	stop := make(chan struct{})
	given := false // touched only by Run's goroutine
	accept := func() (connection.Socket, error) {
		if !given {
			given = true
			return s.sock, nil
		}
		<-stop
		return nil, net.ErrClosed
	}
	l := connection.NewListener("scripted", accept, clk, cfg, s.inbound, s.metrics)
	for _, apply := range options {
		apply(l)
	}
	done, ids := make(chan struct{}), make(chan string, 1)
	go func() {
		defer close(done)
		ids <- testkit.Goid()
		// The scripted accept's last error stands for the listener's
		// close; what Run makes of an accept failure is listener_test.go's.
		_ = l.Run()
	}()
	// However the test ended, its end lets every goroutine of the bubble
	// return: each socket call fails, the writer gets its final entry, and
	// the reader's last events are taken until none is left to take.
	t.Cleanup(func() {
		close(s.sock.gone)
		if s.conn != nil {
			select {
			case s.conn.Replies() <- connection.Close{}:
			default:
			}
		}
		close(stop)
	drain:
		for {
			select {
			case <-s.inbound:
			case <-time.After(backstop): // the bubble's time: once all are blocked
				break drain
			}
		}
		select {
		case <-done:
		case <-time.After(backstop):
			t.Errorf("the listener's Run did not return within %v of its accept failing", backstop)
		}
	})
	s.runID = <-ids
	return s
}

// accepted takes the listener's Accepted, as the core does, and keeps its
// connection.
func (s *seam) accepted() {
	s.t.Helper()
	s.conn = s.event(connection.Accepted).Conn
}

// event returns the next event the reader, writer or listener sends, which
// must be of kind.
func (s *seam) event(kind connection.Kind) connection.Event {
	s.t.Helper()
	ev := s.take()
	return checkKind(s.t, &ev, kind)
}

// take returns the next event the reader, writer or listener sends,
// whatever its kind.
func (s *seam) take() connection.Event {
	s.t.Helper()
	ev, ok := testkit.RecvUntil(s.inbound, backstop, nil)
	if !ok {
		s.t.Fatalf("the core got no event within %v", backstop)
	}
	switch ev.Kind {
	case connection.Ended:
		s.ended = true
	case connection.Closed:
		s.reported = true
	}
	return ev
}

// next returns who's next socket call, keeping the other goroutine's for
// later. A reader that calls anything once stopped fails the test.
func (s *seam) next(who string) *sockCall {
	s.t.Helper()
	if c := s.pending[who]; c != nil {
		delete(s.pending, who)
		return c
	}
	deadline := time.After(backstop)
	for {
		select {
		case c := <-s.sock.calls:
			if c.who == reader && s.stopped {
				s.t.Fatalf("the reader called %v after it stopped reading (R2: \"the reader stops reading\")", c)
			}
			if c.who == who {
				return c
			}
			s.pending[c.who] = c
		case <-deadline:
			s.t.Fatalf("the %s made no socket call within %v", who, backstop)
		}
	}
}

// expect returns the writer's next socket call, which must be op.
func (s *seam) expect(op string) *sockCall {
	s.t.Helper()
	c := s.next(writer)
	if c.op != op {
		s.t.Fatalf("the writer's next socket call is %v, want %s", c, op)
	}
	return c
}

// feed answers the reader's calls, its deadlines set, until it has read
// data.
func (s *seam) feed(data []byte) {
	s.t.Helper()
	for len(data) > 0 {
		switch c := s.next(reader); c.op {
		case opRead:
			data = data[c.give(data):]
		case opReadDeadline:
			c.ok()
		default:
			s.t.Fatalf("the reader called %v, want a read", c)
		}
	}
}

// hold answers the reader's deadlines until it reads, and keeps that Read
// unanswered: the reader waits in it, as for a client sending nothing.
func (s *seam) hold() {
	s.t.Helper()
	for {
		switch c := s.next(reader); c.op {
		case opRead:
			s.reading = c
			return
		case opReadDeadline:
			c.ok()
		default:
			s.t.Fatalf("the reader called %v, want a read", c)
		}
	}
}

// stopReader answers the reader's held Read with err, as its socket does
// once the writer stops it; from then on the reader must call nothing.
func (s *seam) stopReader(err error) {
	s.t.Helper()
	s.stopped = true
	s.reading.fail(err)
	s.reading = nil
}

// connect has the reader read an MQTT 5 CONNECT every check passes, and
// the writer write its CONNACK 0x00, the reader then waiting in a read.
func (s *seam) connect() {
	s.t.Helper()
	s.connectWith(connectPacket(harness.V5))
}

// connectWith is connect with p, an MQTT 5 CONNECT every check passes.
func (s *seam) connectWith(p *harness.Connect) {
	s.t.Helper()
	s.connackWrite(p).ok()
	s.hold()
}

// connectHeld is connect, the writer held in the CONNACK's Write, which it
// returns unanswered: what the core queues meanwhile is all waiting when it
// is answered.
func (s *seam) connectHeld() *sockCall {
	s.t.Helper()
	return s.connackWrite(connectPacket(harness.V5))
}

// connackWrite feeds the CONNECT p and returns the writer's Write of its
// CONNACK, unanswered.
func (s *seam) connackWrite(p *harness.Connect) *sockCall {
	s.t.Helper()
	s.feed(harness.Encoded(s.t, p, harness.V5))
	ev := s.event(connection.Received)
	switch {
	case ev.Reason != 0:
		s.t.Fatalf("CONNECT refused with 0x%02X, want 0x00", ev.Reason)
	case s.decide != nil:
		s.decide(ev)
	default:
		s.queue(&packet.Connack{})
	}
	for {
		c := s.next(writer)
		if c.op == opWriteDeadline {
			c.ok()
			continue
		}
		if c.op != opWrite {
			s.t.Fatalf("the writer called %v, want the CONNACK's write", c)
		}
		return c
	}
}

// queue puts r on the writer's queue, as the core does.
func (s *seam) queue(r connection.Reply) {
	s.t.Helper()
	if sendReply(s.t, s.conn, r) {
		s.final = true
	}
}

// beginClose sets the clock to the close's start, sends the core's final
// entry, and returns the deadline the close must fix: write_timeout later
// (design.md, Flows, Closing a connection: "It keeps its first recorded
// cause. When the close begins, it fixes one deadline limits.write_timeout
// later").
func (s *seam) beginClose(r connection.Reply) time.Time {
	s.t.Helper()
	start := epoch.Add(time.Hour)
	s.clock.set(start)
	s.queue(r)
	return start.Add(seamTimeout)
}

// lastPacket expects the DISCONNECT of code the close writes under
// deadline, the clock moved to at while the write is under way.
func (s *seam) lastPacket(deadline, at time.Time, code byte) {
	s.t.Helper()
	if c := s.expect(opWriteDeadline); !c.t.Equal(deadline) {
		s.t.Fatalf("the last packet's write deadline is %v, want the close's %v", c.t, deadline)
	} else {
		c.ok()
	}
	c := s.expect(opWrite)
	p, err := harness.ReadPacket(bytes.NewReader(c.p), harness.V5, 0)
	if d, ok := p.(*harness.Disconnect); err != nil || !ok || d.Reason != code {
		s.t.Fatalf("the last packet written is %x (%v), want DISCONNECT 0x%02X", c.p, err, code)
	}
	s.clock.set(at)
	c.ok()
}

// interrupt expects the writer's stop of the reader: a read deadline the
// clock has reached, so that the reader's read returns at once.
func (s *seam) interrupt() *sockCall {
	s.t.Helper()
	c := s.expect(opReadDeadline)
	if c.t.IsZero() || c.t.After(c.at) {
		s.t.Fatalf("the writer stopped the reader with %v: want a read deadline already reached, which ends a read under way", c)
	}
	return c
}

// discardFrom expects the writer to begin discarding: its read deadline is
// the close's own.
func (s *seam) discardFrom(deadline time.Time) {
	s.t.Helper()
	if c := s.expect(opReadDeadline); !c.t.Equal(deadline) {
		s.t.Fatalf("the writer discards under %v, want the close's deadline %v, which nothing extends", c, deadline)
	} else {
		c.ok()
	}
}

// closed expects the writer's Close, made with the clock at at.
func (s *seam) closed(at time.Time) {
	s.t.Helper()
	c := s.expect(opClose)
	if !c.at.Equal(at) {
		s.t.Errorf("the socket was closed at %v, want %v", c.at, at)
	}
	s.shut = true
	c.ok()
}

// pingreq is a PINGREQ the client goes on sending during the close.
var pingreq = []byte{0xC0, 0x00}

// design.md, Flows, Closing a connection, 1 to 3: "The writer writes the
// last packet, ends its sending and stops the reader." "The reader stops
// reading and says so before it reports its end to the core." "Only then
// does the writer discard what the client still sends, until the client's
// end of stream or the deadline, and close."; and below them: "A client
// that stops in time sees an orderly close". The writer's calls
// are exactly: the last packet's write deadline and write (the core's
// DISCONNECT; Close{} has none), CloseWrite, the read deadline that stops
// the reader; then, the reader's read ended, the discard under the close's
// deadline, reading what the client still sends until its end of stream,
// and Close. The core takes no event meanwhile: the writer discards while
// the reader still waits to report its end, which it can only once it has
// said it stopped. Nothing is left unread when the socket closes.
func TestCloseStepsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		final connection.Reply
	}{
		{"server DISCONNECT", &packet.Disconnect{ReasonCode: 0x98}},
		{"Close", connection.Close{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := startSeam(t)
				s.connect()
				deadline := s.beginClose(tc.final)
				start := s.clock.Now()
				if _, ok := tc.final.(*packet.Disconnect); ok {
					s.lastPacket(deadline, start, 0x98)
				}
				s.expect(opCloseWrite).ok()
				s.interrupt().ok()
				s.stopReader(deadlinePassed)

				s.discardFrom(deadline)
				s.expect(opRead).give(pingreq)
				s.expect(opRead).give(pingreq)
				ev := s.event(connection.Ended) // only now does the core take the reader's end
				if ev.Packet != nil {
					t.Errorf("Ended carries %T, want nothing: the writer stopped the reader", ev.Packet)
				}
				s.expect(opRead).fail(io.EOF)
				s.closed(start)
			})
		})
	}
}

// design.md, Flows, Closing a connection: "It keeps its first recorded cause.
// [...] When the close begins, it fixes one deadline limits.write_timeout
// later. Nothing extends that deadline"; "at the deadline the socket is
// closed whatever is left". The last packet's write, the wait for the reader and
// each discard take time on the clock; every deadline the close sets is still
// the one it fixed. The client never stops sending: while the clock is short
// of the deadline, even by a nanosecond, the writer reads on; when the
// deadline passes in a read, its next call is Close, at the deadline.
func TestCloseDeadlineIsFixed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x98})
		start := s.clock.Now()
		quarter := seamTimeout / 4

		s.lastPacket(deadline, start.Add(quarter), 0x98) // a write that takes a quarter of it
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.clock.set(start.Add(2 * quarter)) // the wait for the reader
		s.stopReader(deadlinePassed)
		s.discardFrom(deadline)
		s.clock.set(start.Add(3 * quarter))
		s.expect(opRead).give(pingreq)
		s.clock.set(deadline.Add(-time.Nanosecond))
		s.expect(opRead).give(pingreq) // not earlier: the writer reads on
		r := s.expect(opRead)
		s.clock.set(deadline)
		r.fail(deadlinePassed)
		s.closed(deadline)
		s.event(connection.Ended)
	})
}

// design.md, Flows, Closing a connection: "Where the end and the deadline
// meet, the deadline wins"; "at the deadline the socket is closed whatever
// is left". At the deadline, the client's last bytes arrive with its end of
// stream right behind them: the writer's read returns those bytes with the
// clock at the deadline, and its next call is Close, the end of stream left
// unread.
func TestCloseDeadlineWinsOverTheEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x98})
		start := s.clock.Now()
		s.lastPacket(deadline, start, 0x98)
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.stopReader(deadlinePassed)
		s.discardFrom(deadline)
		r := s.expect(opRead)
		s.clock.set(deadline)
		r.give(pingreq) // the end of stream is next
		s.closed(deadline)
		s.event(connection.Ended)
	})
}

// design.md, Flows, Closing a connection, 2 and 3: "The reader stops
// reading and says so before it reports its end to the core." "Only then
// does the writer discard"; design.md, Execution,
// Decisions: "Map iteration, an observed metric, queue emptiness and Go's
// choice between ready cases decide none of them"; design.md, Flows,
// Closing a connection: "Where the end and the deadline meet, the deadline
// wins". The writer's stop of the reader is held while
// the reader's read ends, the reader says it stopped and reports its end to
// the core, and the clock reaches the deadline: when the writer goes on,
// the reader's stop and the deadline are both ready, and its next call is
// Close, with nothing discarded. The outcome is exact; a writer leaving the
// choice to Go's select between two ready channels is caught only on the
// runs where Go picks the reader's stop (28 of 40 in a trial), which no
// test can force.
func TestCloseDeadlineWinsOverTheReadersStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x98})
		start := s.clock.Now()
		s.lastPacket(deadline, start, 0x98)
		s.expect(opCloseWrite).ok()
		stop := s.interrupt()
		s.stopReader(deadlinePassed)
		s.event(connection.Ended) // the reader said it stopped before this
		s.clock.set(deadline)
		stop.ok()
		s.closed(deadline)
	})
}

// design.md, Flows, Closing a connection, 2 and 3: "The reader stops
// reading and says so before it reports its end to the core." "Only then
// does the writer discard ..."; and below them: "at the deadline the socket
// is closed whatever is left". The reader's read does not end when
// the writer stops it (its socket holds it), so the reader never says it
// stopped: the writer discards nothing, since only the reader may read
// until it has, and closes the socket when the clock reaches the deadline.
// The reader's read then ends on the closed socket and the reader reports
// its end.
func TestCloseDiscardsOnlyOnceTheReaderStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x98})
		start := s.clock.Now()
		s.lastPacket(deadline, start, 0x98)
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		synctest.Wait()       // the writer waits for the reader, or holds out its next call
		s.clock.set(deadline) // the reader's read still under way
		s.closed(deadline)
		s.stopReader(closedSocket)
		s.event(connection.Ended)
	})
}

// design.md, Closing a connection, 2: "Once started, that handover sends
// exactly once and finishes before the reader reports its end; it does not
// compete with stop in a `select`"; "The core ignores packets from a
// connection it has already closed or taken over", so a packet arriving
// after the core's close is harmless; design.md, Execution, Communication:
// "All readers hand packets to the core on one bounded channel. As its last
// act, each writer reports on that channel that its socket is closed". The
// reader has read a PINGREQ whole and offers it to a core that is not taking;
// the core then closes the connection, and the writer stops the reader while
// the offer stands. Once every goroutine waits, the core takes: its first
// event is the PINGREQ, the handover started before the stop, then the
// reader's plain Ended; the writer discards and closes as for any close, its
// close report follows, and nothing follows that.
func TestCloseFinishesAStartedHandover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		s.arrive(pingreq)
		s.stopped = true // the PINGREQ read whole: the reader must read nothing more
		synctest.Wait()  // the reader offers the PINGREQ, which the core does not take

		deadline := s.beginClose(connection.Close{})
		start := s.clock.Now()
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		// Taken only once the reader and writer both wait: the stop has
		// reached the reader while its handover is under way.
		synctest.Wait()
		if ev := s.take(); ev.Kind != connection.Received || ev.Packet != (connection.Pingreq{}) {
			t.Fatalf("the core's first event is %s, want the PINGREQ: the reader started its handover "+
				"before the stop, and a started handover finishes", describe(&ev))
		}
		if ev := s.event(connection.Ended); ev.Packet != nil || ev.Reason != 0 {
			t.Fatalf("the core got %s, want a plain Ended: the writer stopped the reader", describe(&ev))
		}
		s.discardFrom(deadline)
		s.expect(opRead).fail(io.EOF)
		s.closed(start)
		s.event(connection.Closed)

		synctest.Wait() // the reader and writer have ended, or wait to send
		select {
		case ev := <-s.inbound:
			t.Fatalf("after its Ended and the close report the core got %s", describe(&ev))
		default:
		}
	})
}

// design.md, Flows, Closing a connection, 1 and 2: "The writer writes the
// last packet, ends its sending and stops the reader." "The reader stops
// reading and says so before it reports its end to the core." "An
// unsubmitted complete DISCONNECT travels with the end; any other
// unsubmitted packet is dropped."; design.md, Restart, Graceful stop: "It
// acts on a client's DISCONNECT by storing its session expiry change". Audit
// 261018-02, Observation 1: a DISCONNECT read whole became a bare end when
// the writer's stop came between the read and the report, the client reading
// 0x8B and keeping the session it had ended. A durable MQTT 5 client sends a
// DISCONNECT ending its session (Session Expiry Interval 0), read up to its
// last byte; the stop's DISCONNECT 0x8B is written and the writer stops the
// reader while its read of that last byte is under way; that read then
// returns the byte, so the reader holds the DISCONNECT whole when it sees the
// stop. The core's first event is the reader's end, carrying that DISCONNECT
// and its Session Expiry Interval; the close then goes on as for any other.
func TestCloseStillPassesAPacketReadWhole(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connectWith(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "client",
			Props: harness.Properties{{ID: harness.SessionExpiryInterval, Int: 3600}}})
		disconnect := harness.Encoded(t, &harness.Disconnect{Props: harness.Properties{{ID: harness.SessionExpiryInterval}}}, harness.V5)
		last := len(disconnect) - 1
		s.arrive(disconnect[:last])
		s.hold() // the read of the DISCONNECT's last byte, under way

		deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x8B})
		start := s.clock.Now()
		s.lastPacket(deadline, start, 0x8B)
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.stopped = true // the DISCONNECT is whole: the reader must read nothing more
		s.reading.give(disconnect[last:])
		s.reading = nil

		ev := s.event(connection.Ended)
		d, ok := ev.Packet.(*packet.Disconnect)
		if !ok || ev.Reason != 0 {
			t.Fatalf("the core got %s, want the reader's end carrying the client's DISCONNECT: "+
				"the reader read it whole before it saw the stop (R2)", describe(&ev))
		}
		var expiry []uint32
		for prop := range d.Properties.Views() {
			if prop.ID == packet.SessionExpiryInterval {
				expiry = append(expiry, prop.Int)
			}
		}
		if d.ReasonCode != 0 || len(expiry) != 1 || expiry[0] != 0 {
			t.Fatalf("the core got DISCONNECT 0x%02X with Session Expiry Intervals %v, want the client's: 0x00, and 0",
				d.ReasonCode, expiry)
		}
		s.discardFrom(deadline)
		s.expect(opRead).fail(io.EOF)
		s.closed(start)
		s.event(connection.Closed)
	})
}

// design.md, Closing a connection, 2: "Before starting a handover, the
// reader checks stop. If stop is already observed, no handover starts";
// "An unsubmitted complete DISCONNECT travels with the end; any other
// unsubmitted packet is dropped". The reader waits in a read for the
// client's next packet when the core closes the connection and the writer
// stops it; that read then returns a whole PINGREQ, so the reader holds a
// complete packet when it sees the stop. No handover starts: the core's
// first event is the reader's plain Ended, and the close goes on as for
// any other. TestCloseStillPassesAPacketReadWhole is the DISCONNECT's case.
func TestCloseDropsAPacketReadWholeAtTheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect() // the reader waits in a read for the client's next packet

		deadline := s.beginClose(connection.Close{})
		start := s.clock.Now()
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.stopped = true // the PINGREQ is whole: the reader must read nothing more
		s.reading.give(pingreq)
		s.reading = nil

		if ev := s.take(); ev.Kind != connection.Ended || ev.Packet != nil || ev.Reason != 0 {
			t.Fatalf("the core's first event is %s, want a plain Ended: the reader saw the stop "+
				"before handing the PINGREQ over, so no handover starts", describe(&ev))
		}
		s.discardFrom(deadline)
		s.expect(opRead).fail(io.EOF)
		s.closed(start)
		s.event(connection.Closed)
	})
}

// design.md, Flows, DISCONNECT, A lost connection: "a nonzero effective
// Keep Alive gives the reader 1.5 times that interval when it begins
// reading the next packet." "A keep-alive timeout closes the connection with
// 0x8D."; design.md, Execution, Decisions: "Go's choice between ready cases
// decide[s] none of them. Time changes eligibility only at a deadline named
// here." The read deadline is the socket's: a Read that returns bytes
// returned them because they came in time, however late the reader runs
// again (audits 261012-007a). A CONNECT with a keep-alive of 10 seconds is
// accepted and the reader's next read is made with the clock at the start,
// under a deadline 15 seconds later; the socket hands that read a PINGREQ
// with the clock just short of the deadline, at it, or past it. The socket's
// answers are its deadline's verdict: in every case the core receives the
// PINGREQ.
func TestKeepAliveBytesTheSocketReturnsAreInTime(t *testing.T) {
	const keepAlive = 10 // seconds
	silence := 15 * time.Second
	for _, tc := range []struct {
		name string
		at   time.Duration // the clock when the socket hands the bytes
	}{
		{"a nanosecond short of the deadline", silence - time.Nanosecond},
		{"at the deadline", silence},
		{"a millisecond past the deadline", silence + time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := startSeam(t)
				s.feed(harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, CleanStart: true,
					ClientID: "c", KeepAlive: keepAlive}, harness.V5))
				if ev := s.event(connection.Received); ev.Reason != 0 {
					t.Fatalf("CONNECT refused with 0x%02X, want 0x00", ev.Reason)
				}
				s.queue(&packet.Connack{})
				for c := s.next(writer); ; c = s.next(writer) {
					if c.op != opWriteDeadline && c.op != opWrite {
						t.Fatalf("the writer called %v, want the CONNACK's write", c)
					}
					c.ok()
					if c.op == opWrite {
						break
					}
				}

				// The read is made at the clock's start, under a deadline
				// 1.5 times the keep-alive later.
				deadline := epoch.Add(silence)
				c := s.next(reader)
				if c.op != opReadDeadline || !c.t.Equal(deadline) {
					t.Fatalf("the reader called %v, want SetReadDeadline(%v)", c, deadline)
				}
				c.ok()
				read := s.next(reader)
				if read.op != opRead || !read.at.Equal(epoch) {
					t.Fatalf("the reader called %v, want a read at %v", read, epoch)
				}

				// The socket hands the PINGREQ with the clock at tc.at, the
				// rest of it to every read that follows.
				s.clock.set(epoch.Add(tc.at))
				rest := pingreq[read.give(pingreq):]
				for len(rest) > 0 {
					select {
					case c := <-s.sock.calls:
						if c.who != reader || c.op != opRead {
							t.Fatalf("the %s called %v, want the reader's read of the PINGREQ's rest", c.who, c)
						}
						rest = rest[c.give(rest):]
					case ev := <-s.inbound:
						t.Fatalf("the core got %s with the PINGREQ's %d bytes left unread: "+
							"the socket's deadline decides, not the reader's clock", describe(&ev), len(rest))
					case <-time.After(backstop):
						t.Fatalf("the reader made no call within %v with the PINGREQ's %d bytes left", backstop, len(rest))
					}
				}
				if ev := s.event(connection.Received); ev.Packet != (connection.Pingreq{}) || ev.Reason != 0 {
					t.Fatalf("the core got %s, want the PINGREQ", describe(&ev))
				}
			})
		})
	}
}
