package listeners

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// heldSlots is an Admission that gives every socket a slot and counts the
// slots held.
type heldSlots struct{ arrived, held atomic.Int32 }

func (s *heldSlots) Arrive(time.Time) (bool, SlotWait) {
	s.arrived.Add(1)
	s.held.Add(1)
	return true, nil
}
func (s *heldSlots) ReleaseSlot()                  { s.held.Add(-1) }
func (s *heldSlots) ConnectTimeout() time.Duration { return 0 }
func (s *heldSlots) SocketRefused(string)          {}

// shutAtAccept shuts its door down as it accepts its first socket: after the
// socket is admitted and before Serve looks at whether the door is ending,
// which is where a shutdown can land.
type shutAtAccept struct {
	net.Listener
	shut func()
	once sync.Once
}

func (l *shutAtAccept) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(l.shut)
	}
	return c, err
}

// **A socket a door accepted as it was shut down is closed, and gives its
// max_connections slot back.** It was admitted - holding a slot - before
// Serve saw the door ending, so Serve neither hands it on nor may leave it:
// left, nothing would ever close it, and its slot would be held for as long
// as the broker ran.
func TestASocketAcceptedAsItsDoorShutsIsClosedAndGivesItsSlotBack(t *testing.T) {
	for _, door := range []struct {
		name string
		open func(t *testing.T, a Admission) (Listener, string, string, func(func(net.Listener) net.Listener))
	}{
		{"tcp", func(t *testing.T, a Admission) (Listener, string, string, func(func(net.Listener) net.Listener)) {
			l := NewTCP(Config{ID: "t1", Address: "127.0.0.1:0"})
			l.SetAdmission(a)
			require.NoError(t, l.Init(logger))
			return l, "tcp", l.listen.Addr().String(), func(w func(net.Listener) net.Listener) { l.listen = w(l.listen) }
		}},
		{"unix", func(t *testing.T, a Admission) (Listener, string, string, func(func(net.Listener) net.Listener)) {
			path := shortSocketPath(t)
			l := NewUnixSock(Config{ID: "t1", Address: path})
			l.SetAdmission(a)
			require.NoError(t, l.Init(logger))
			return l, "unix", path, func(w func(net.Listener) net.Listener) { l.listen = w(l.listen) }
		}},
		{"net", func(t *testing.T, a Admission) (Listener, string, string, func(func(net.Listener) net.Listener)) {
			n, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			l := NewNet("t1", n)
			l.SetAdmission(a)
			require.NoError(t, l.Init(logger))
			return l, "tcp", n.Addr().String(), func(w func(net.Listener) net.Listener) { l.listener = w(l.listener) }
		}},
	} {
		t.Run(door.name, func(t *testing.T) {
			slots := &heldSlots{}
			l, network, addr, wrap := door.open(t, slots)
			wrap(func(ln net.Listener) net.Listener {
				return &shutAtAccept{Listener: ln, shut: func() { l.Close(MockCloser) }}
			})
			var established atomic.Int32
			served := make(chan struct{})
			go func() {
				defer close(served)
				l.Serve(func(_ string, c net.Conn) error {
					established.Add(1)
					return c.Close()
				})
			}()

			c, err := net.Dial(network, addr)
			require.NoError(t, err)
			defer c.Close()
			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not return once its door was shut")
			}
			// The socket was admitted and was not handed on: the case.
			require.EqualValues(t, 1, slots.arrived.Load(), "sockets admitted")
			require.EqualValues(t, 0, established.Load(), "sockets handed on after the door was shut")
			if held := slots.held.Load(); held != 0 {
				t.Errorf("%d slots still held once Serve has returned", held)
			}

			require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
			n, err := c.Read(make([]byte, 1))
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the socket is still open 2s after its door was shut")
			}
			require.ErrorIs(t, err, io.EOF, "read %d bytes", n)
		})
	}
}

// **The same on a ws door, whose sockets net/http accepts**: one admitted
// and not yet upgraded when the door shuts is closed, and gives its slot
// back, as the door shuts. net/http's own accept loop hands every socket it
// accepts to the server before it returns, so the door shutting is the one
// moment to look at; a socket upgraded after it is refused by
// Listeners.Establish.
func TestASocketNotYetUpgradedAsAWsDoorShutsIsClosedAndGivesItsSlotBack(t *testing.T) {
	slots := &heldSlots{}
	l := NewWebsocket(Config{ID: "w1", Address: "127.0.0.1:0"})
	l.SetAdmission(slots)
	require.NoError(t, l.Init(logger))
	served := make(chan struct{})
	go func() {
		defer close(served)
		l.Serve(func(_ string, c net.Conn) error { return c.Close() })
	}()
	c, err := net.Dial("tcp", l.Address())
	require.NoError(t, err)
	defer c.Close()
	until := time.Now().Add(5 * time.Second)
	for slots.arrived.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	require.EqualValues(t, 1, slots.arrived.Load(), "sockets admitted")

	began := time.Now()
	l.Close(MockCloser)
	took := time.Since(began)
	<-served
	if held := slots.held.Load(); held != 0 {
		t.Errorf("%d slots still held once the door has shut (it took %v)", held, took)
	}
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	n, err := c.Read(make([]byte, 1))
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the socket is still open 2s after its door shut (it took %v)", took)
	}
	require.ErrorIs(t, err, io.EOF, "read %d bytes", n)
}
