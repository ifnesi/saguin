package harness

import (
	"cmp"
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
)

// Peer is an MQTT server a test owns on loopback, standing in for the peer
// broker a bridge dials (design.md, Bridges: "A bridge is a connection to a
// peer broker, opened by a dialer. On that connection Sagüin is an ordinary
// MQTT 5 client of the peer"). It accepts every connection and hands each
// to the test as a PeerLink, in accept order. A link records every packet
// that arrives on it, with its receipt time and its size on the wire, and
// sends only what the test scripts: a CONNACK with chosen properties or a
// refusal, SUBACK grants and refusals, PINGRESP or silence, or the link
// dropped. The test plays the peer's side, so it decides what the broker
// sees, and reads what the broker sent in the order it came. Its options
// (PeerOptions) have it serve TLS, a WebSocket, or silence before MQTT.
type Peer struct {
	ln    net.Listener
	opts  PeerOptions
	links chan *PeerLink // accepted, not yet taken by Next
	done  chan struct{}  // closed by Close: every goroutine of the peer returns
	wg    sync.WaitGroup

	mu    sync.Mutex
	all   []*PeerLink // every link accepted, closed by Close
	ended bool
}

// peerBacklog bounds the links accepted and not yet taken, and a link's
// arrivals not yet read: more wait in the kernel, then in the sender.
const peerBacklog = 256

// PeerOptions says what a Peer speaks under MQTT, and how its URL names it
// (config.md, bridges.<name>, peer: "the peer's URL: tcp://, tls://, ws://
// or wss://, [...] ws:// and wss:// may add a path (default /)"). The zero
// value is plain TCP.
type PeerOptions struct {
	// Scheme is the URL's scheme: "tcp" ("" too), "tls", "ws" or "wss". On
	// ws and wss each link is a WebSocket first, its upgrade answered as
	// ServeWebSocket answers it (MQTT 5 chapter 6).
	Scheme string
	// Path is what the URL adds after the address, "" for nothing: the
	// path of a ws or wss peer, the bridge's default / where none.
	Path string
	// TLS, where not nil, has each link speak TLS as the server with it,
	// before anything else: a tls or wss peer.
	TLS *tls.Config
	// UpgradeHeader is further header lines the answer to each WebSocket
	// upgrade carries, none where nil.
	UpgradeHeader http.Header
	// Silent has the peer answer nothing once any TLS handshake is done: a
	// link reads and discards what the bridge sends, and its one arrival is
	// the stream's end. With TLS nil the peer never answers a TLS handshake
	// or a WebSocket upgrade; with TLS, it never answers the upgrade that
	// follows the handshake.
	Silent bool
}

// ListenPeer listens on a loopback port of its own and accepts every
// connection made to it until Close.
func ListenPeer() (*Peer, error) {
	return ListenPeerWith(PeerOptions{})
}

// ListenPeerWith is ListenPeer serving what o says under MQTT.
func ListenPeerWith(o PeerOptions) (*Peer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Peer{ln: ln, opts: o, links: make(chan *PeerLink, peerBacklog), done: make(chan struct{})}
	p.wg.Go(p.accept)
	return p, nil
}

// ListenedPeer is ListenPeer for a test: it fails the test where the peer
// cannot listen, and closes the peer when the test ends.
func ListenedPeer(t testing.TB) *Peer {
	t.Helper()
	return ListenedPeerWith(t, PeerOptions{})
}

// ListenedPeerWith is ListenedPeer serving what o says under MQTT.
func ListenedPeerWith(t testing.TB, o PeerOptions) *Peer {
	t.Helper()
	p, err := ListenPeerWith(o)
	if err != nil {
		t.Fatalf("listening for the bridge's peer: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() }) // the test is over: how the links end no longer matters
	return p
}

// DownPeer returns the address of a peer that is down: a loopback port
// reserved (reserveAddrs) until t ends and never listened on, so a dial
// there is refused, and no broker or peer is given the port meanwhile.
func DownPeer(t testing.TB) string {
	t.Helper()
	addrs, release, err := reserveAddrs(1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return addrs[0]
}

// Addr is the peer's address, host and port.
func (p *Peer) Addr() string { return p.ln.Addr().String() }

// URL is the peer's address as a bridge's peer key names it (config.md,
// bridges.<name>, peer: "the peer's URL: tcp://, tls://, ws:// or
// wss://"), under its options' scheme and with their path.
func (p *Peer) URL() string { return cmp.Or(p.opts.Scheme, "tcp") + "://" + p.Addr() + p.opts.Path }

// accept takes every connection until Close, each a link reading on its
// own goroutine.
func (p *Peer) accept() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		accepted := time.Now()
		if p.opts.TLS != nil {
			conn = tls.Server(conn, p.opts.TLS) // its handshake runs on the link's reader
		}
		l := &PeerLink{conn: conn, stream: conn, Accepted: accepted, arrivals: make(chan Arrival, peerBacklog), done: p.done,
			silent: p.opts.Silent, webSocket: p.opts.Scheme == "ws" || p.opts.Scheme == "wss", upgradeHeader: p.opts.UpgradeHeader}
		p.mu.Lock()
		if p.ended {
			p.mu.Unlock()
			_ = conn.Close() // the peer is closing: nothing reads it
			return
		}
		p.all = append(p.all, l)
		p.mu.Unlock()
		p.wg.Go(l.read)
		select {
		case p.links <- l:
		case <-p.done:
			return
		}
	}
}

// Next returns the next link accepted, waiting for at most limit.
func (p *Peer) Next(limit time.Duration) (*PeerLink, error) {
	select {
	case l := <-p.links:
		return l, nil
	case <-time.After(limit):
		return nil, fmt.Errorf("no connection reached the peer at %s within %v", p.Addr(), limit)
	}
}

// Link is Next for a test, within DefaultTimeout, failing the test where no
// connection comes.
func (p *Peer) Link(t testing.TB) *PeerLink {
	t.Helper()
	return p.LinkWithin(t, DefaultTimeout)
}

// LinkWithin is Link with its own limit.
func (p *Peer) LinkWithin(t testing.TB, limit time.Duration) *PeerLink {
	t.Helper()
	l, err := p.Next(limit)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// Close stops accepting, closes every link and waits until every goroutine
// of the peer has returned.
func (p *Peer) Close() error {
	p.mu.Lock()
	p.ended = true
	links := p.all
	p.mu.Unlock()
	err := p.ln.Close()
	close(p.done)
	for _, l := range links {
		_ = l.conn.Close() // dropped with the peer
	}
	p.wg.Wait()
	return err
}

// PeerLink is one connection the peer accepted: what arrives on it, in
// order, and what the test sends back.
type PeerLink struct {
	conn     net.Conn  // the connection accepted, under TLS where the peer serves it
	Accepted time.Time // when the peer accepted the connection
	arrivals chan Arrival
	done     <-chan struct{} // the peer's: closed when it closes

	// TLS is the link's TLS handshake, where the peer serves TLS: what was
	// agreed, and the certificates the bridge presented. Upgrade is the
	// bridge's WebSocket upgrade request, where the peer serves a
	// WebSocket: its request target and headers. The reader sets each
	// before the link's first arrival; read them after one.
	TLS     *tls.ConnectionState
	Upgrade *http.Request

	// silent, webSocket and upgradeHeader are the peer's options
	// (PeerOptions); stream
	// is what MQTT travels over, conn or the WebSocket over it, set
	// under writing by the reader once the upgrade is done.
	silent, webSocket bool
	upgradeHeader     http.Header
	stream            net.Conn

	// version is the protocol level of the CONNECT read, V5 until one is:
	// set by the reader before the CONNECT is handed on, so a Send that
	// follows it sees it.
	version atomic.Uint32
	// pings has the reader answer each PINGREQ with PINGRESP itself
	// (AnswerPings).
	pings atomic.Bool

	writing sync.Mutex // one write at a time: the test's and the reader's PINGRESP
}

// Arrival is one packet that arrived on a link, or the link's end: when it
// was read whole, and its size on the wire, fixed header included. The last
// arrival of a link has no Packet and Err set: io.EOF where the broker ended
// the stream in order, another error where it reset the connection or sent
// what does not decode.
type Arrival struct {
	Packet Packet
	At     time.Time
	Size   int
	Err    error
}

// read opens the link as the peer's options say, then records every packet
// until the stream ends, then the end, answering each PINGREQ where pings
// is set. A silent link records only the end.
func (l *PeerLink) read() {
	defer close(l.arrivals)
	l.version.Store(uint32(V5))
	if err := l.open(); err != nil {
		l.arrive(Arrival{At: time.Now(), Err: err})
		return
	}
	if l.silent {
		_, err := io.Copy(io.Discard, l.conn)
		l.arrive(Arrival{At: time.Now(), Err: cmp.Or(err, io.EOF)})
		return
	}
	for {
		counted := &countReader{r: l.stream}
		p, err := ReadPacket(counted, byte(l.version.Load()), 0)
		a := Arrival{Packet: p, At: time.Now(), Size: counted.n, Err: err}
		if c, ok := p.(*Connect); ok {
			l.version.Store(uint32(c.Version))
		}
		if _, ok := p.(*Pingreq); ok && l.pings.Load() {
			// What the peer answers is its own; a write failing ends the
			// link, which the next read reports.
			_, _ = l.Send(&Pingresp{})
		}
		if !l.arrive(a) || err != nil {
			return
		}
	}
}

// open runs what the link speaks before MQTT: the TLS handshake where the
// peer serves TLS, then, unless the link is silent, the WebSocket upgrade
// where it serves one, under writing so no Send comes before its answer.
func (l *PeerLink) open() error {
	if c, ok := l.conn.(*tls.Conn); ok {
		if err := c.Handshake(); err != nil {
			return err
		}
		state := c.ConnectionState()
		l.TLS = &state
	}
	if l.silent || !l.webSocket {
		return nil
	}
	l.writing.Lock()
	defer l.writing.Unlock()
	ws, req, err := ServeWebSocket(l.conn, l.upgradeHeader)
	l.Upgrade = req
	if err != nil {
		return err
	}
	l.stream = ws
	return nil
}

// arrive hands a to the test, reporting false where the peer closed first.
func (l *PeerLink) arrive(a Arrival) bool {
	select {
	case l.arrivals <- a:
		return true
	case <-l.done:
		return false
	}
}

// Next returns the next arrival, waiting for at most limit. A link whose
// end has been returned has nothing more.
func (l *PeerLink) Next(limit time.Duration) (Arrival, error) {
	select {
	case a, ok := <-l.arrivals:
		if !ok {
			return Arrival{}, errors.New("the link has ended: nothing more arrives")
		}
		return a, nil
	case <-time.After(limit):
		return Arrival{}, fmt.Errorf("nothing arrived on the link within %v", limit)
	}
}

// Send encodes p in the link's protocol version and writes it, within
// DefaultTimeout, and returns when the write began: no packet of it can
// have reached the broker earlier.
func (l *PeerLink) Send(p Packet) (time.Time, error) {
	b, err := Encode(p, byte(l.version.Load()))
	if err != nil {
		return time.Time{}, err
	}
	return l.SendRaw(b)
}

// SendRaw writes b as it is, within DefaultTimeout, and returns when the
// write began: half of a packet, the rest of one, or several.
func (l *PeerLink) SendRaw(b []byte) (time.Time, error) {
	l.writing.Lock()
	defer l.writing.Unlock()
	at := time.Now()
	if err := l.stream.SetWriteDeadline(at.Add(DefaultTimeout)); err != nil {
		return at, err
	}
	_, err := l.stream.Write(b)
	return at, err
}

// AnswerPings has the link answer every PINGREQ that arrives from now on
// with PINGRESP, as a live peer does ([MQTT-3.12.4-1]), each PINGREQ still
// recorded; false leaves them unanswered.
func (l *PeerLink) AnswerPings(on bool) { l.pings.Store(on) }

// Close drops the link, as a peer that goes away does, without a DISCONNECT.
func (l *PeerLink) Close() error { return l.conn.Close() }

// Arrive reads l's next arrival within limit, failing the test unless it
// is a T, and returns it with its arrival. The failure names T by its type,
// as *Ack's Type needs an Ack to name it.
func Arrive[T Packet](t testing.TB, l *PeerLink, limit time.Duration) (T, Arrival) {
	t.Helper()
	var want T
	a, err := l.Next(limit)
	if err != nil {
		t.Fatalf("waiting for a %T on the peer: %v", want, err)
	}
	got, ok := a.Packet.(T)
	if !ok {
		t.Fatalf("the peer received %s, want a %T", a, want)
	}
	return got, a
}

// Ended reads l's next arrival within limit, failing the test unless it is
// the end of the stream in order, and returns it.
func (l *PeerLink) Ended(t testing.TB, limit time.Duration) Arrival {
	t.Helper()
	a, err := l.Next(limit)
	switch {
	case err != nil:
		t.Fatalf("waiting for the link's end on the peer: %v", err)
	case a.Packet != nil:
		t.Fatalf("the peer received %s, want the link ended", a)
	case !errors.Is(a.Err, io.EOF):
		t.Fatalf("the link ended with %v, want an orderly end of stream", a.Err)
	}
	return a
}

// Accept reads l's CONNECT within DefaultTimeout, failing the test where
// another arrival comes first, answers it with ack, and returns the CONNECT
// with its arrival and when the CONNACK was sent.
func (l *PeerLink) Accept(t testing.TB, ack *Connack) (*Connect, Arrival, time.Time) {
	t.Helper()
	c, a := Arrive[*Connect](t, l, DefaultTimeout)
	sent, err := l.Send(ack)
	if err != nil {
		t.Fatalf("sending CONNACK 0x%02X to the bridge: %v", ack.Reason, err)
	}
	return c, a, sent
}

// String describes an arrival for a failure message.
func (a Arrival) String() string {
	if a.Packet == nil {
		return fmt.Sprintf("the link's end (%v)", a.Err)
	}
	return fmt.Sprintf("%s %+v (%d bytes)", Name(a.Packet.Type()), a.Packet, a.Size)
}
