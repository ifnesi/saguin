package mqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// budgetOf is the overflow budget RFC 0002 gives max_connections limit:
// max_connections or 32, whichever is fewer.
func budgetOf(limit int64) int64 { return min(32, limit) }

// gateServer is a server with every one of its limit slots taken, for
// driving its admission directly.
func gateServer(t *testing.T, limit int64) *Server {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc})
	s.slots.Store(limit)
	return s
}

// closingServer serves a TCP door at the given connection limit, closed by
// the test itself: Close may be called once.
func closingServer(t *testing.T, limit int64, connectTimeout time.Duration) (*Server, string) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: connectTimeout})
	require.NoError(t, s.AddHook(&slowAuthHook{}, nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	return s, tcp.Address()
}

// timedServer is closingServer with a door that records, for each socket,
// when it was accepted and when the broker closed it (lifetime).
func timedServer(t *testing.T, limit int64, connectTimeout time.Duration) (*Server, string, *timedListener) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: connectTimeout})
	require.NoError(t, s.AddHook(&slowAuthHook{}, nil))
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	timed := &timedListener{Listener: raw, accepted: map[string]time.Time{}, closed: map[string]time.Time{}}
	require.NoError(t, s.AddListener(listeners.NewNet("t", timed)))
	require.NoError(t, s.Serve())
	return s, raw.Addr().String(), timed
}

// timedListener records when each socket was accepted and when the broker
// closed it, keyed by the peer's address.
//
// **The broker's bounds are timed at the broker.** RFC 0002 bounds a socket
// from its arrival to its close, and a client timing from before its dial to
// the close it reads adds its handshake and its own goroutine's wake-up to
// the broker's figure - on two loaded cores, more than the slack the bound
// is checked with.
type timedListener struct {
	net.Listener
	mu       sync.Mutex
	accepted map[string]time.Time
	closed   map[string]time.Time
}

func (l *timedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.accepted[c.RemoteAddr().String()] = time.Now()
	l.mu.Unlock()
	return &timedConn{Conn: c, l: l}, nil
}

// lifetime is how long the broker held the socket whose client end is c, from
// its accept to the broker's close of it.
func (l *timedListener) lifetime(t *testing.T, c net.Conn) time.Duration {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	key := c.LocalAddr().String()
	from, ok := l.accepted[key]
	require.True(t, ok, "the door never accepted %s", key)
	to, ok := l.closed[key]
	require.True(t, ok, "the broker never closed %s", key)
	return to.Sub(from)
}

type timedConn struct {
	net.Conn
	l    *timedListener
	once sync.Once
}

func (c *timedConn) Close() error {
	c.once.Do(func() {
		c.l.mu.Lock()
		c.l.closed[c.RemoteAddr().String()] = time.Now()
		c.l.mu.Unlock()
	})
	return c.Conn.Close()
}

// waiting is how many sockets wait for a slot on s, and overflow how many
// take a place in its overflow budget.
func waiting(s *Server) (waiting, overflow int) {
	s.gate.mu.Lock()
	defer s.gate.mu.Unlock()
	return len(s.gate.waiters), s.gate.overflow
}

// granted reports whether a slot has been handed to w.
func granted(w *slotWaiter) bool {
	select {
	case <-w.granted:
		return true
	default:
		return false
	}
}

// **A slot given back goes to the socket that has waited longest**, and
// one arriving after it queues behind every waiter rather than taking the
// slot first (Server.arrive, Server.ReleaseSlot).
func TestASlotGivenBackGoesToTheOldestWaiter(t *testing.T) {
	s := gateServer(t, 3)
	now := time.Now()
	held, w1 := s.arrive(now)
	require.False(t, held)
	require.NotNil(t, w1)
	_, w2 := s.arrive(now)
	require.NotNil(t, w2)

	s.ReleaseSlot()
	require.True(t, granted(w1), "the slot given back did not go to the oldest waiter")
	require.False(t, granted(w2), "the slot given back went to the second waiter")
	require.Equal(t, int64(3), s.slots.Load(), "a slot handed on was counted free on the way")

	held, w3 := s.arrive(time.Now())
	require.False(t, held, "a socket arriving after the hand-off took a slot ahead of a waiter")
	require.NotNil(t, w3)
	require.True(t, w1.Await(), "the waiter handed a slot does not hold it")

	s.ReleaseSlot()
	require.True(t, granted(w2), "the second slot did not go to the next oldest waiter")
	require.False(t, granted(w3), "the second slot went to the newest waiter")
	require.True(t, w2.Await())
	w3.Done()
	n, over := waiting(s)
	require.Zero(t, n)
	require.Zero(t, over)
	require.Equal(t, int64(3), s.slots.Load())
}

// **The overflow budget is bounded**: max_connections or 32, whichever is
// fewer, and a socket arriving to a full budget is refused at once.
func TestTheOverflowBudgetIsBounded(t *testing.T) {
	for _, limit := range []int64{1, 5, 32, 100} {
		s := gateServer(t, limit)
		var ws []*slotWaiter
		for i := int64(0); i < budgetOf(limit); i++ {
			held, w := s.arrive(time.Now())
			require.False(t, held)
			require.NotNil(t, w, "limit %d: socket %d of %d had no place", limit, i+1, budgetOf(limit))
			ws = append(ws, w)
		}
		held, w := s.arrive(time.Now())
		require.False(t, held)
		require.Nil(t, w, "limit %d: a socket took a place past a budget of %d", limit, budgetOf(limit))
		for _, w := range ws {
			w.Done()
		}
		_, over := waiting(s)
		require.Zero(t, over)
	}
}

// **One budget holds the waiters and the sockets being refused**: a socket
// whose wait ended with no slot keeps its place while it is read to be
// answered 0x89, and gives it back only as it closes.
func TestTheBudgetHoldsASocketBeingRefused(t *testing.T) {
	s := gateServer(t, 1)
	_, w := s.arrive(time.Now().Add(-time.Second)) // its wait is over
	require.NotNil(t, w)
	require.False(t, w.Await())

	held, next := s.arrive(time.Now())
	require.False(t, held)
	require.Nil(t, next, "a socket took the place of one still being refused")

	w.Done()
	_, next = s.arrive(time.Now())
	require.NotNil(t, next, "the place of a socket that closed did not come back")
	next.Done()
}

// **A waiter whose wait has ended is passed over**, decided under the
// gate's lock: a slot given back after it goes to the count, not to a
// socket already being refused.
func TestAWaiterWhoseWaitHasEndedIsPassedOver(t *testing.T) {
	s := gateServer(t, 2)
	_, late := s.arrive(time.Now().Add(-time.Second))
	require.NotNil(t, late)
	_, live := s.arrive(time.Now())
	require.NotNil(t, live)

	s.ReleaseSlot()
	require.False(t, granted(late), "a slot was handed to a waiter whose wait had ended")
	require.True(t, granted(live), "the slot passed over the live waiter")
	require.False(t, late.Await())
	require.True(t, live.Await())

	s.ReleaseSlot()
	require.Equal(t, int64(1), s.slots.Load(), "a slot with only an ended waiter queued was not given back")
	late.Done()
	_, over := waiting(s)
	require.Zero(t, over)
}

// **Nothing is admitted once the server is stopping**, a free slot or not.
func TestNothingIsAdmittedOnceStopping(t *testing.T) {
	s := gateServer(t, 2)
	s.slots.Store(0)
	require.NoError(t, s.Close())
	held, w := s.arrive(time.Now())
	require.False(t, held, "a socket took a free slot after the server began to stop")
	require.Nil(t, w, "a socket queued after the server began to stop")
}

// countingListener counts the sockets it has accepted that are still open,
// and the most there have been at once.
type countingListener struct {
	net.Listener
	open, peak atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	n := l.open.Add(1)
	for p := l.peak.Load(); n > p && !l.peak.CompareAndSwap(p, n); p = l.peak.Load() {
	}
	return &countedConn{Conn: c, l: l}, nil
}

type countedConn struct {
	net.Conn
	l    *countingListener
	once sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.l.open.Add(-1) })
	return c.Conn.Close()
}

// RFC 0002 "How long a socket may wait to send CONNECT".
//
// **A flood holds at most max_connections plus the overflow budget**: far
// more sockets than both arrive at once, half of them sending a CONNECT and
// the rest nothing. A CONNECT is answered 0x00 or 0x89 or its socket closed,
// and none outlives connect_timeout. Open on the broker's side at once are
// at most max_connections and the budget, and beside them two kinds of
// socket that hold neither: the one the door's accept loop has in hand while
// it closes it at once, and a connection whose end is decided, which gives
// its slot back before its socket closes (Client.beginEnd) - at most one for
// each slot.
//
// **Each socket is timed from its connection, not from before its dial.**
// RFC 0002 bounds a socket from its arrival, and a SYN the kernel drops
// never arrives: the client retries it 1s, 3s, 7s... later, before the
// broker has a socket to bound. Timed from before the dial, a socket read
// that wait as the broker's - 1-36s on Linux whenever conntrack's table was
// full, while every socket's accept-to-close stayed within 412ms over 4,788
// runs.
//
// **And n stays below the kernel's listen backlog** - macOS's default
// kern.ipc.somaxconn is 128, Linux's 4096 - because a flood past it is a
// flood of the kernel's accept queue: with 400 sockets at a backlog of 128,
// 10 runs in 10 failed, one socket connected by Linux and never handed to
// the broker at all.
func TestAFloodHoldsMaxConnectionsPlusTheBudget(t *testing.T) {
	const limit, n = 4, 100
	const bound, overflow, slack = 400 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: bound})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counted := &countingListener{Listener: raw}
	require.NoError(t, s.AddListener(listeners.NewNet("t", counted)))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })

	type outcome struct {
		code    byte // the CONNACK's reason code; 0xFF for none
		elapsed time.Duration
	}
	outcomes := make([]outcome, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			speaks := i%2 == 0
			c, err := net.Dial("tcp", raw.Addr().String())
			if err != nil {
				outcomes[i] = outcome{0xFE, 0}
				return
			}
			begun := time.Now()
			defer c.Close()
			if speaks {
				_, _ = c.Write(takeoverConnect(fmt.Sprintf("flood-%d", i)))
			}
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			code := byte(0xFF)
			if speaks {
				h := make([]byte, 4)
				if _, err := io.ReadFull(c, h); err == nil && h[0] == 0x20 {
					code = h[3]
				}
			}
			if code == 0x00 {
				outcomes[i] = outcome{code, time.Since(begun)} // admitted, and stays
				return
			}
			_, err = io.Copy(io.Discard, c)
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				outcomes[i] = outcome{code, -1}
				return
			}
			outcomes[i] = outcome{code, time.Since(begun)}
		}(i)
	}
	close(start)
	wg.Wait()

	budget := budgetOf(limit)
	codes := map[byte]int{}
	for i, o := range outcomes {
		require.GreaterOrEqual(t, o.elapsed, time.Duration(0), "socket %d was neither answered nor closed", i)
		require.NotEqual(t, byte(0xFE), o.code, "socket %d could not connect", i)
		codes[o.code]++
		if i%2 == 0 {
			require.Contains(t, []byte{0x00, packets.ErrServerBusy.Code, 0xFF}, o.code)
		}
		require.LessOrEqual(t, o.elapsed, bound+slack, "socket %d outlived connect_timeout", i)
		if o.code == packets.ErrServerBusy.Code {
			require.LessOrEqual(t, o.elapsed, overflow+slack, "socket %d was refused past the overflow bound", i)
		}
	}
	t.Logf("answers %v; at most %d sockets open at once (limit %d + budget %d)",
		codes, counted.peak.Load(), limit, budget)
	require.LessOrEqual(t, counted.peak.Load(), int64(limit)+budget+1+int64(limit),
		"more sockets were open at once than max_connections, the overflow budget, the one in the accept "+
			"loop's hand and one ending connection a slot")
}

// **Waiters stop at once when the server stops**: Close does not wait out
// their grace, and no slot or place in the budget is left behind.
func TestWaitersStopAtOnceOnShutdown(t *testing.T) {
	const limit = 4
	s, addr := closingServer(t, limit, 0)
	// The slots are held by nothing that shutdown ends, so only the stop
	// itself can end the waits.
	s.slots.Store(limit)
	for i := int64(0); i < budgetOf(limit); i++ {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer c.Close()
	}
	require.Eventually(t, func() bool { n, _ := waiting(s); return n == int(budgetOf(limit)) },
		time.Second, time.Millisecond, "the sockets past the limit did not queue")

	begun := time.Now()
	require.NoError(t, s.Close())
	took := time.Since(begun)
	require.Less(t, took, admitGrace/2, "Close waited %s, on the waiters' grace", took)
	n, over := waiting(s)
	require.Zero(t, n, "a waiter was left in the queue")
	require.Zero(t, over, "a place in the budget was left taken")
	require.Equal(t, int64(limit), s.slots.Load(), "a waiter left holding a slot, or took one")
}

// **A socket's wait for a slot counts against connect_timeout**: its
// arrival is recorded as its door accepts it, before it waits, so a silent
// socket handed a slot part-way through its bound is closed when that bound
// ends, not one wait later.
func TestTheWaitForASlotCountsAgainstConnectTimeout(t *testing.T) {
	const bound, freed = 150 * time.Millisecond, 40 * time.Millisecond
	for trial := 0; trial < 5; trial++ {
		s, addr, timed := timedServer(t, 1, bound)
		holder, code := dialAndConnect(t, addr, takeoverConnect("holder"))
		require.Equal(t, byte(0x00), code)

		begun := time.Now()
		silent, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		require.Eventually(t, func() bool { n, _ := waiting(s); return n == 1 }, time.Second, time.Millisecond)
		time.Sleep(freed - time.Since(begun)) // the input: a slot freed part-way through the bound
		_ = holder.Close()

		_ = silent.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = io.Copy(io.Discard, silent)
		took := timed.lifetime(t, silent)
		_ = silent.Close()
		_ = s.Close()
		require.Greater(t, took, freed+admitGrace, "trial %d: the socket was not admitted, so this measures nothing", trial)
		require.Less(t, took, bound+freed/2,
			"trial %d: a socket handed a slot %s after it arrived was closed %s after, past connect_timeout %s",
			trial, freed, took, bound)
	}
}

// wsSlotServer is a server with a ws door - wss when tlsConfig is set - at
// max_connections 1.
func wsSlotServer(t *testing.T, tlsConfig *tls.Config) (*Server, string) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 1
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: 5 * time.Second})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	ws := listeners.NewWebsocket(listeners.Config{ID: "w", Address: "127.0.0.1:0", TLSConfig: tlsConfig})
	require.NoError(t, s.AddListener(ws))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return s, ws.Address()
}

// RFC 0002: **a request the ws door answers instead of upgrading gives its
// slot back before that answer is written** - a request that is not an
// upgrade (gorilla's 400), a malformed one and one whose headers are too
// large (net/http's own 400 and 431; after the 431 net/http holds the socket
// half a second more). Given back at the close, the slot was still held
// when the client read the answer.
func TestTheWebsocketDoorGivesItsSlotBackBeforeAnHTTPRefusal(t *testing.T) {
	s, addr := wsSlotServer(t, nil)
	huge := "GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + fmt.Sprintf("%01100000d", 0) + "\r\n\r\n"
	for _, tc := range []struct{ name, request, status string }{
		{"not an upgrade", "GET / HTTP/1.1\r\nHost: x\r\n\r\n", "400"},
		{"malformed", "GARBAGE\r\n\r\n", "400"},
		{"headers too large", huge, "431"},
	} {
		for trial := 0; trial < 10; trial++ {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			_, _ = c.Write([]byte(tc.request))
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			line := make([]byte, 12)
			_, err = io.ReadFull(c, line)
			require.NoError(t, err, tc.name)
			require.Contains(t, string(line), tc.status, "%s: answered %q", tc.name, line)
			held := s.slots.Load()
			_ = c.Close()
			require.Zero(t, held, "%s, trial %d: the slot was still held when the client read %q",
				tc.name, trial, line)
			require.Eventually(t, func() bool { return s.slots.Load() == 0 }, time.Second, time.Millisecond)
		}
	}
}

// **On a wss door the same refusal gives its slot back first**, whatever
// the client offers: a client offering HTTP/2 is served HTTP/1.1, the only
// protocol a WebSocket upgrade has here. Served HTTP/2, the refusal went
// out on a connection kept open for more, its slot held.
func TestTheWssDoorGivesItsSlotBackBeforeAnHTTPRefusal(t *testing.T) {
	s, addr := wsSlotServer(t, selfSigned(t))
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	for trial := 0; trial < 10; trial++ {
		resp, err := client.Get("https://" + addr + "/")
		require.NoError(t, err)
		held := s.slots.Load()
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Equal(t, 1, resp.ProtoMajor, "trial %d: the wss door was spoken to in %s", trial, resp.Proto)
		require.Zero(t, held, "trial %d: the slot was still held when the client read the refusal", trial)
		require.Eventually(t, func() bool { return s.slots.Load() == 0 }, time.Second, time.Millisecond)
	}
}

// **A socket that gets no slot is closed within the overflow bound of its
// arrival**, 100 ms, its wait included, however long connect_timeout is: a
// silent one waits its 50 ms, is read for a CONNECT that never comes, and is
// closed.
func TestASocketThatGetsNoSlotIsClosedWithinTheOverflowBound(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		s, addr, timed := timedServer(t, 1, 5*time.Second)
		holder, code := dialAndConnect(t, addr, takeoverConnect("holder"))
		require.Equal(t, byte(0x00), code)

		silent, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		_ = silent.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, _ = io.Copy(io.Discard, silent)
		took := timed.lifetime(t, silent)
		_ = silent.Close()
		_ = holder.Close()
		_ = s.Close()
		require.Greater(t, took, admitGrace, "trial %d: the socket did not wait for a slot, so this measures nothing", trial)
		require.Less(t, took, overflowBound+admitGrace/2,
			"trial %d: a socket that got no slot was held %s, past the overflow bound", trial, took)
	}
}
