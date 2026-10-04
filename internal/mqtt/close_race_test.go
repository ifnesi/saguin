package mqtt

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/stretchr/testify/require"
)

// TestCloseWhileConnecting covers the shutdown accounting.
//
// attachClient registers each client with Listeners.ClientsWg, and
// CloseAll waits on it. sync.WaitGroup forbids a positive Add concurrent
// with Wait, so a connection accepted while the server is closing used to
// race the very wait that was meant to cover it - and Close could return
// while that client was still being attached.
//
// **The window is made, not hunted for.** One client is held inside
// attachClient, so Close latches shutdown, closes that client's connection
// and waits for it; the second connection arrives once the first has seen
// its connection closed, which Close does after the latch and before the
// wait. Dialling in a loop and closing a millisecond later found the window
// only when the scheduler happened to land there.
//
// Without the latch the second connection reaches admission, and under
// -race the detector reports a write in Close against a read in
// EstablishConnection.
func TestCloseWhileConnecting(t *testing.T) {
	h := &countedConnectHook{entered: make(chan struct{}, 2), release: make(chan struct{})}
	s := newServer()
	require.NoError(t, s.AddHook(h, nil))

	first, peer := net.Pipe()
	attached := make(chan error, 1)
	go func() { attached <- s.EstablishConnection("tcp", first) }()
	go func() {
		_, _ = peer.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first client never reached OnConnect")
	}

	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.Copy(io.Discard, peer)
	require.NoError(t, err, "Close did not close the connection still being attached")

	second, other := net.Pipe()
	defer other.Close()
	refused := make(chan error, 1)
	go func() { refused <- s.EstablishConnection("tcp", second) }()
	select {
	case err := <-refused:
		// Refused by the latch, which closes the connection and answers
		// nil, before the wait was told of it - not by admission, which
		// answers Server busy for a connection the wait was told of.
		require.NoError(t, err, "a connection arriving while Close waited was registered with the wait")
	case <-time.After(5 * time.Second):
		t.Fatal("a connection arriving while Close waited was attached rather than refused")
	}
	require.Equal(t, int32(1), h.count.Load(), "the connection arriving while Close waited reached OnConnect")
	select {
	case <-closed:
		t.Fatal("Close returned while a client was still being attached")
	default:
	}

	close(h.release)
	<-closed
	<-attached
}

// countedConnectHook holds every client in OnConnect until it is released,
// and counts them.
type countedConnectHook struct {
	HookBase
	count   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (h *countedConnectHook) ID() string           { return "counted-connect" }
func (h *countedConnectHook) Provides(b byte) bool { return b == OnConnect }
func (h *countedConnectHook) OnConnect(*Client, packets.Packet) error {
	h.count.Add(1)
	h.entered <- struct{}{}
	<-h.release
	return nil
}
