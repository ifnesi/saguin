// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package listeners

import (
	"crypto/tls"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"log/slog"
)

// Config contains configuration values for a listener.
//
// **Type is gone and was the whole of what the type constants named.** It
// held "tcp", "ws", "unix" or "mock" for Server.AddListenersFromConfig,
// which read it to choose a constructor and went with mochi's YAML
// loader. saguin builds its listeners in code and names the constructor
// directly, so nothing has written this field since; it was a field
// nothing wrote that still answered when it was read, which is the
// failure the six $SYS counters were removed for.
type Config struct {
	ID      string
	Address string
	// TLSConfig is a tls.Config configuration to be used with the listener. See examples folder for basic and mutual-tls use.
	TLSConfig *tls.Config
	// ConnectRate is how many connections the listener accepts a second, or
	// zero for no limit. See RateLimit.
	ConnectRate int
	// FileMode is a Unix socket's permissions, set where it is created
	// (ListenUnix), or zero to leave the umask's.
	FileMode os.FileMode
}

// RateLimit returns ln accepting at most perSecond connections a second, with
// a burst of twice that, or ln itself for zero or less.
//
// **The limit is in Accept, so a connection over it waits rather than being
// refused**: it stays in the kernel's accept queue until a token is free, as
// EMQX pauses accepting when its max_conn_rate is reached. A fleet
// reconnecting all at once after an outage is slowed, not turned away. The
// burst of twice the rate is HiveMQ's default for its connect-rate.
//
// What it protects is the work before authentication - a password check per
// CONNECT - rather than memory, which max_connections and the CONNECT size
// bound.
func RateLimit(ln net.Listener, perSecond int) net.Listener {
	if perSecond <= 0 {
		return ln
	}
	return &rateLimited{
		Listener: ln,
		rate:     float64(perSecond),
		burst:    float64(2 * perSecond),
		tokens:   float64(2 * perSecond),
		last:     time.Now(),
	}
}

type rateLimited struct {
	net.Listener
	rate, burst float64
	mu          sync.Mutex
	tokens      float64
	last        time.Time
}

// Accept waits for a token, then accepts.
func (l *rateLimited) Accept() (net.Conn, error) {
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens = math.Min(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
		l.last = now
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return l.Listener.Accept()
		}
		wait := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()
		time.Sleep(wait)
	}
}

// EstablishFn is a callback function for establishing new clients.
type EstablishFn func(id string, c net.Conn) error

// CloseFn is a callback function for closing all listener clients.
type CloseFn func(id string)

// Listener is an interface for network listeners. A network listener listens
// for incoming client connections and adds them to the server.
type Listener interface {
	Init(*slog.Logger) error // open the network address
	Serve(EstablishFn)       // starting actively listening for new connections
	ID() string              // return the id of the listener
	Address() string         // the address of the listener
	Protocol() string        // the protocol in use by the listener
	Close(CloseFn)           // stop and close the listener
}

// Listeners contains the network listeners for the broker.
type Listeners struct {
	ClientsWg sync.WaitGroup      // a waitgroup that waits for all clients in all listeners to finish.
	internal  map[string]Listener // a map of active listeners.
	shutdown  sync.RWMutex        // guards closed; taken for writing while shutdown latches.
	closed    bool                // once set, no further client is registered.
	connsMu   sync.Mutex          // guards conns.
	conns     map[net.Conn]func() // nil until a client governs the connection (Govern)
	sync.RWMutex
}

// Establish registers a connection with ClientsWg for the duration of
// establish, and refuses it once shutdown has begun.
//
// sync.WaitGroup forbids a positive Add concurrent with Wait. Registering
// a client from the connection's own goroutine, as the server used to do,
// means a connection accepted while CloseAll is waiting races the very
// wait that is meant to cover it -- and CloseAll can return while that
// client is still being attached, which is the one thing a graceful
// shutdown promises not to do.
//
// Latching shutdown under the write lock, and registering only under the
// read lock, makes that ordering impossible rather than unlikely.
//
// Every connection the server attaches passes through here, because
// EstablishConnection is both what ServeAll hands to a listener and what a
// caller embedding the server calls with a connection of its own. Wrapping
// the listener's establisher instead covered only the first of those, and
// left a direct caller outside the latch and outside the wait -- so Close
// returned while such a client was still being attached, which is the very
// thing this exists to prevent.
func (l *Listeners) Establish(id string, c net.Conn, establish EstablishFn) error {
	l.shutdown.RLock()
	if l.closed {
		l.shutdown.RUnlock()
		// Shutdown has begun and this connection will not be served.
		// Closing it is the whole of the answer; there is nothing here
		// worth logging on every connection that arrives while a
		// server is going down.
		return c.Close()
	}
	l.ClientsWg.Add(1)
	// Recorded under the same read lock as the Add, so a connection is
	// either refused above or in this set by the time CloseAll latches.
	l.connsMu.Lock()
	if l.conns == nil {
		l.conns = map[net.Conn]func(){}
	}
	l.conns[c] = nil
	l.connsMu.Unlock()
	l.shutdown.RUnlock()

	defer func() {
		l.connsMu.Lock()
		delete(l.conns, c)
		l.connsMu.Unlock()
		l.ClientsWg.Done()
	}()
	return establish(id, c)
}

// Govern hands c, a connection Establish holds, to its client: a shutdown
// then ends it through end rather than closing the socket (CloseAll).
func (l *Listeners) Govern(c net.Conn, end func()) {
	l.connsMu.Lock()
	defer l.connsMu.Unlock()
	if _, ok := l.conns[c]; ok {
		l.conns[c] = end
	}
}

// New returns a new instance of Listeners.
func New() *Listeners {
	return &Listeners{
		internal: map[string]Listener{},
	}
}

// Add adds a new listener to the listeners map, keyed on id.
func (l *Listeners) Add(val Listener) {
	l.Lock()
	defer l.Unlock()
	l.internal[val.ID()] = val
}

// Get returns the value of a listener if it exists.
func (l *Listeners) Get(id string) (Listener, bool) {
	l.RLock()
	defer l.RUnlock()
	val, ok := l.internal[id]
	return val, ok
}

// Len returns the length of the listeners map.
func (l *Listeners) Len() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.internal)
}

// Delete removes a listener from the internal map.
func (l *Listeners) Delete(id string) {
	l.Lock()
	defer l.Unlock()
	delete(l.internal, id)
}

// Serve starts a listener serving from the internal map.
func (l *Listeners) Serve(id string, establisher EstablishFn) {
	l.RLock()
	defer l.RUnlock()
	listener := l.internal[id]

	go func(e EstablishFn) {
		listener.Serve(e)
	}(establisher)
}

// ServeAll starts all listeners serving from the internal map.
func (l *Listeners) ServeAll(establisher EstablishFn) {
	l.RLock()
	i := 0
	ids := make([]string, len(l.internal))
	for id := range l.internal {
		ids[i] = id
		i++
	}
	l.RUnlock()

	for _, id := range ids {
		l.Serve(id, establisher)
	}
}

// Close stops a listener from the internal map.
func (l *Listeners) Close(id string, closer CloseFn) {
	l.RLock()
	defer l.RUnlock()
	if listener, ok := l.internal[id]; ok {
		listener.Close(closer)
	}
}

// CloseAll iterates and closes all registered listeners.
func (l *Listeners) CloseAll(closer CloseFn) {
	// Latch shutdown before waiting, so that no client can be registered
	// once the wait below has begun.
	l.shutdown.Lock()
	l.closed = true
	l.shutdown.Unlock()

	l.RLock()
	i := 0
	ids := make([]string, len(l.internal))
	for id := range l.internal {
		ids[i] = id
		i++
	}
	l.RUnlock()

	for _, id := range ids {
		l.Close(id, closer)
	}

	// **Then every connection still open is closed.** The closer reaches
	// only registered clients, and has already told each one it is going. A
	// connection that has not yet sent its CONNECT is registered nowhere, so
	// the wait below used to last until that peer chose to speak - and a
	// broker writes its snapshot after this returns, so a peer sending
	// nothing kept a stopping broker from saving until it was killed.
	//
	// **A connection a client governs is ended through it** (Govern): its
	// socket may be carrying the CONNACK that refuses it, written by the
	// goroutine that owns its end, and a close here closed the socket under
	// that write. Only a socket no client has yet is closed outright.
	l.connsMu.Lock()
	var ends []func()
	for c, end := range l.conns {
		if end == nil {
			_ = c.Close()
			continue
		}
		ends = append(ends, end)
	}
	l.connsMu.Unlock()
	for _, end := range ends {
		end()
	}
	l.ClientsWg.Wait()
}
