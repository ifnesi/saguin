package harness

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// DefaultTimeout is how long a Client waits on its connection when Timeout is
// zero. Waiting past it on an acknowledgement means the acknowledgement was
// lost.
const DefaultTimeout = 10 * time.Second

// Client speaks MQTT over one connection, one packet at a time, with no
// goroutine of its own: a test decides what is sent and reads what comes
// back. It can play either end, which is how tests stand in for a broker.
type Client struct {
	Conn    net.Conn
	Version byte // V311 or V5: the layout of every packet but CONNECT
	// Timeout bounds each Send, Recv and Await; zero means DefaultTimeout.
	Timeout time.Duration
	// MaxPacketSize refuses a larger incoming packet, counted whole with its
	// fixed header as MQTT 5 counts it, before reading its body; zero means
	// the protocol's limit.
	MaxPacketSize int
	// TopicAliasMaximum is the Topic Alias Maximum of the client's MQTT 5
	// CONNECT (Advertise): the highest Topic Alias it accepts on a PUBLISH
	// from the server, 0 for none.
	TopicAliasMaximum uint16

	backlog []Packet          // packets Await read past, returned first by Recv
	aliases map[uint16]string // the server's Topic Alias mappings on this connection
	server  bool              // the client has read a CONNECT: it plays the server
	lastID  uint16
}

// NewClient wraps conn. version may be 0 on a peer that learns it from the
// first CONNECT it reads.
func NewClient(conn net.Conn, version byte) *Client {
	return &Client{Conn: conn, Version: version}
}

// Dial opens a TCP connection to addr.
func Dial(addr string, version byte) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, DefaultTimeout)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, version), nil
}

// DialUnix opens a connection to the unix socket at path.
func DialUnix(path string, version byte) (*Client, error) {
	conn, err := net.DialTimeout("unix", path, DefaultTimeout)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, version), nil
}

// DialTLS opens a TCP connection to addr through dialer (nil for the
// default) and completes a TLS handshake over it with config, all within
// DefaultTimeout; the Client speaks MQTT over the *tls.Conn. A handshake
// that fails is the error, its connection closed.
func DialTLS(dialer *net.Dialer, addr string, version byte, config *tls.Config) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	d := tls.Dialer{NetDialer: dialer, Config: config}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, version), nil
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

// Send encodes p in the client's version and writes it.
func (c *Client) Send(p Packet) error {
	return c.SendUntil(p, time.Now().Add(c.timeout()))
}

// SendUntil encodes p and writes it with an absolute deadline. A caller
// bounding a whole operation can keep later writes within that bound.
func (c *Client) SendUntil(p Packet, deadline time.Time) error {
	b, err := Encode(p, c.Version)
	if err != nil {
		return err
	}
	return c.sendRawUntil(b, deadline)
}

// SendRaw writes b as it is: a malformed packet, half of one, or several.
func (c *Client) SendRaw(b []byte) error {
	return c.sendRawUntil(b, time.Now().Add(c.timeout()))
}

func (c *Client) sendRawUntil(b []byte, deadline time.Time) error {
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	_, err := c.Conn.Write(b)
	return err
}

// Recv returns the next packet: one Await read past, else one from the wire.
// A CONNECT read sets the client's version, so a peer can answer in kind. A
// PUBLISH from the server has its Topic Alias applied (resolve).
func (c *Client) Recv() (Packet, error) {
	if len(c.backlog) == 0 {
		if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout())); err != nil {
			return nil, err
		}
	}
	return c.recv()
}

// recv reads with the caller's deadline, returning the backlog first.
func (c *Client) recv() (Packet, error) {
	if len(c.backlog) > 0 {
		p := c.backlog[0]
		c.backlog = c.backlog[1:]
		return p, nil
	}
	p, err := ReadPacket(c.Conn, c.Version, c.MaxPacketSize)
	if err != nil {
		return nil, err
	}
	switch p := p.(type) {
	case *Connect:
		c.Version, c.server = p.Version, true
	case *Publish:
		// A peer playing the server leaves the client's aliases to the
		// test, which checks them against its own CONNACK.
		if !c.server && c.Version == V5 {
			if err := c.resolve(p); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

// resolve applies p's Topic Alias, p being a PUBLISH from the server, as
// MQTT 5 3.3.2.3.4 has its receiver do: a Topic Name with an alias sets
// that alias's mapping, and an empty one takes the topic its alias maps,
// ByAlias marking it. An alias over TopicAliasMaximum is refused, any alias
// where it is 0: "The Server MUST NOT send a Topic Alias in a PUBLISH
// packet to the Client greater than Topic Alias Maximum [MQTT-3.1.2-26]",
// and "If Topic Alias Maximum is absent or zero, the Server MUST NOT send
// any Topic Aliases to the Client [MQTT-3.1.2-27]". So is an empty Topic
// Name whose alias maps nothing yet: a sender sends one only "If a Topic
// Alias mapping has been set at the receiver". Mappings live as long as
// the connection, and so the Client [MQTT-3.3.2-7].
func (c *Client) resolve(p *Publish) error {
	a, ok := p.Props.Get(TopicAlias)
	if !ok {
		return nil
	}
	alias := uint16(a.Int)
	if alias > c.TopicAliasMaximum {
		return fmt.Errorf("%w: Topic Alias %d, over the Topic Alias Maximum of %d the CONNECT sent", ErrTopicAlias, alias, c.TopicAliasMaximum)
	}
	if p.Topic != "" {
		if c.aliases == nil {
			c.aliases = make(map[uint16]string, c.TopicAliasMaximum)
		}
		c.aliases[alias] = p.Topic
		return nil
	}
	topic, ok := c.aliases[alias]
	if !ok {
		return fmt.Errorf("%w: an empty Topic Name with Topic Alias %d, which maps no topic yet", ErrProtocol, alias)
	}
	p.Topic, p.ByAlias = topic, true
	return nil
}

// Await reads until a packet matches, keeping the others, in order, for
// Recv. Deliveries can arrive between a request and its acknowledgement, so a
// test waits for the one it wants without losing the rest. A DISCONNECT that
// does not match ends the wait, as the sender closes after it. One deadline
// bounds the whole wait; unrelated packets never extend it.
func (c *Client) Await(match func(Packet) bool) (Packet, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout())); err != nil {
		return nil, err
	}
	var kept []Packet
	defer func() { c.backlog = append(kept, c.backlog...) }()
	for {
		p, err := c.recv()
		if err != nil {
			return nil, err
		}
		if match(p) {
			return p, nil
		}
		if d, ok := p.(*Disconnect); ok {
			return nil, fmt.Errorf("DISCONNECT 0x%02X while waiting", d.Reason)
		}
		kept = append(kept, p)
	}
}

// Pending returns the packets Await read past and Recv has not yet returned.
// Between exchanges it should be empty: anything there was not expected.
func (c *Client) Pending() []Packet {
	return append([]Packet(nil), c.backlog...)
}

// AwaitAck waits for the acknowledgement of one kind with identifier id.
func (c *Client) AwaitAck(kind byte, id uint16) (*Ack, error) {
	p, err := c.Await(func(p Packet) bool {
		a, ok := p.(*Ack)
		return ok && a.Kind == kind && a.ID == id
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for %s %d: %w", Name(kind), id, err)
	}
	return p.(*Ack), nil
}

// NextID returns the next packet identifier, 1 to 65535 and round again,
// never 0.
func (c *Client) NextID() uint16 {
	c.lastID++
	if c.lastID == 0 {
		c.lastID = 1
	}
	return c.lastID
}

// Advertise makes the client refuse what p, the CONNECT it sends, tells the
// server not to send: MaxPacketSize becomes the Maximum Packet Size of an
// MQTT 5 CONNECT, the CONNACK included ("The Server MUST NOT send packets
// exceeding Maximum Packet Size to the Client" [MQTT-3.1.2-24]), and
// TopicAliasMaximum its Topic Alias Maximum, 0 where it sends none
// (resolve). A CONNECT advertising no Maximum Packet Size leaves
// MaxPacketSize as it is. A CONNECT the server
// refuses before it reads the Maximum Packet Size, one over
// limits.max_connect_size (design.md, CONNECT's 0x95 row: "sent without the
// client's Maximum Packet Size, which is in the unread body"), is sent
// without it.
func (c *Client) Advertise(p *Connect) {
	if p.Version != V5 {
		return
	}
	if mps, ok := p.Props.Get(MaximumPacketSize); ok {
		c.MaxPacketSize = int(mps.Int)
	}
	tam, _ := p.Props.Get(TopicAliasMaximum)
	c.TopicAliasMaximum = uint16(tam.Int)
}

// Connect sends p, first filling in, in p itself, the protocol name "MQTT"
// and the client's version where p leaves them empty, and returns the
// CONNACK. Anything else first is an error: CONNACK is the server's first
// packet ([MQTT-3.2.0-1]). From then on the client refuses a packet over
// the Maximum Packet Size p advertises (Advertise); a CONNECT the server
// refuses before reading it, over limits.max_connect_size, is sent with
// Send instead.
func (c *Client) Connect(p *Connect) (*Connack, error) {
	if p.ProtocolName == "" {
		p.ProtocolName = "MQTT"
	}
	if p.Version == 0 {
		p.Version = c.Version
	}
	c.Advertise(p)
	if err := c.Send(p); err != nil {
		return nil, err
	}
	got, err := c.Recv()
	if err != nil {
		return nil, fmt.Errorf("waiting for CONNACK: %w", err)
	}
	ack, ok := got.(*Connack)
	if !ok {
		return nil, fmt.Errorf("%w: %s before CONNACK", ErrProtocol, Name(got.Type()))
	}
	return ack, nil
}

// Publish sends p and completes its QoS exchange: nothing at QoS 0, PUBACK at
// QoS 1, PUBREC then PUBREL and PUBCOMP at QoS 2. It returns the final
// acknowledgement (nil at QoS 0). A refusal in PUBREC (reason 0x80 or above)
// ends the exchange there: a PUBREL answers only a PUBREC below 0x80 (5.0
// §4.3.3).
// A QoS above 0 with no ID takes the next one.
func (c *Client) Publish(p *Publish) (*Ack, error) {
	if p.QoS > 0 && p.ID == 0 {
		p.ID = c.NextID()
	}
	if err := c.Send(p); err != nil {
		return nil, err
	}
	switch p.QoS {
	case 0:
		return nil, nil
	case 1:
		return c.AwaitAck(PUBACK, p.ID)
	}
	rec, err := c.AwaitAck(PUBREC, p.ID)
	if err != nil || rec.Reason >= 0x80 {
		return rec, err
	}
	if err := c.Send(&Ack{Kind: PUBREL, ID: p.ID}); err != nil {
		return nil, err
	}
	return c.AwaitAck(PUBCOMP, p.ID)
}

// Acknowledge completes the receiving side of a delivery: PUBACK at QoS 1;
// PUBREC, then PUBCOMP once PUBREL arrives, at QoS 2.
func (c *Client) Acknowledge(p *Publish) error {
	switch p.QoS {
	case 1:
		return c.Send(&Ack{Kind: PUBACK, ID: p.ID})
	case 2:
		if err := c.Send(&Ack{Kind: PUBREC, ID: p.ID}); err != nil {
			return err
		}
		if _, err := c.AwaitAck(PUBREL, p.ID); err != nil {
			return err
		}
		return c.Send(&Ack{Kind: PUBCOMP, ID: p.ID})
	}
	return nil
}

// Close closes the connection without a DISCONNECT, as a lost client would.
func (c *Client) Close() error {
	return c.Conn.Close()
}

// CloseWrite ends what the client sends, keeping its reading open: a client
// that has stopped sending, whose end of stream the peer then reads. The
// connection must be TCP, a unix socket, or TLS, whose CloseWrite sends
// close_notify.
func (c *Client) CloseWrite() error {
	return c.Conn.(interface{ CloseWrite() error }).CloseWrite()
}

// Connected dials addr in version and connects with p, failing the test
// unless the CONNACK is a success (0x00), and returns the client and its
// CONNACK. The connection is closed when the test ends.
func Connected(t testing.TB, addr string, version byte, p *Connect) (*Client, *Connack) {
	t.Helper()
	c, err := Dial(addr, version)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	return c, ConnectedOver(t, c, p)
}

// ConnectedOver is Connected on c, already dialled, over TCP, TLS or a unix
// socket: it
// connects with p, failing the test unless the CONNACK is a success
// (0x00), and returns the CONNACK. The connection is closed when the test
// ends.
func ConnectedOver(t testing.TB, c *Client, p *Connect) *Connack {
	t.Helper()
	// The test is over: whatever the close finds no longer matters.
	t.Cleanup(func() { _ = c.Close() })
	ack, err := c.Connect(p)
	if err != nil {
		t.Fatalf("connecting %q: %v", p.ClientID, err)
	}
	if ack.Reason != 0 {
		t.Fatalf("connecting %q: CONNACK 0x%02X, want 0x00", p.ClientID, ack.Reason)
	}
	return ack
}

// Encoded is p's bytes in version, as Encode writes them, failing the test
// where they cannot be written: Encode's form for tests.
func Encoded(t testing.TB, p Packet, version byte) []byte {
	t.Helper()
	b, err := Encode(p, version)
	if err != nil {
		t.Fatalf("encoding %s: %v", Name(p.Type()), err)
	}
	return b
}

// SendBytes writes b on c as it is, as SendRaw does, failing the test if
// it cannot: SendRaw's form for tests.
func SendBytes(t testing.TB, c *Client, b []byte) {
	t.Helper()
	if err := c.SendRaw(b); err != nil {
		t.Fatalf("sending %d bytes: %v", len(b), err)
	}
}

// Expect reads c's next packet and fails the test unless it is a T: a
// CONNACK, DISCONNECT, PINGRESP or any packet whose Type needs no value,
// since a failure names the packet wanted by its zero value's Type.
func Expect[T Packet](t testing.TB, c *Client) T {
	t.Helper()
	var want T
	p, err := c.Recv()
	if err != nil {
		t.Fatalf("waiting for %s: %v", Name(want.Type()), err)
	}
	got, ok := p.(T)
	if !ok {
		t.Fatalf("%s, want %s", Name(p.Type()), Name(want.Type()))
	}
	return got
}

// ExpectClosed reads c's next packet and fails the test unless there is
// none: the peer closed the connection in order, an end of stream, never a
// reset (design.md, Flows, Closing a connection: "The writer handles the
// DISCONNECT and orderly close"; audits 261012-002a).
func ExpectClosed(t testing.TB, c *Client) {
	t.Helper()
	if err := c.AwaitClose(); err != nil {
		t.Fatal(err)
	}
}

// AwaitClose reads c's next packet and returns an error unless there is
// none: the peer closed the connection in order, an end of stream, never a
// reset. ExpectClosed is its form for tests; the runners call it directly.
func (c *Client) AwaitClose() error {
	return c.Closed(c.Recv())
}

// Closed is AwaitClose's verdict on what one Recv returned, for a caller
// that reads past packets it expects before the close (the end-to-end
// fuzzer's acknowledgements of publishes still unanswered).
func (c *Client) Closed(p Packet, err error) error {
	switch {
	case err == nil:
		return fmt.Errorf("%s %+v, want the connection closed", Name(p.Type()), p)
	case IsTimeout(err):
		return fmt.Errorf("the connection is still open after %v", c.timeout())
	case !errors.Is(err, io.EOF):
		return fmt.Errorf("reading after the close: %w, want an orderly end of stream", err)
	}
	return nil
}

// Ping sends PINGREQ and returns an error unless the next packet is
// PINGRESP: "The Server MUST send a PINGRESP packet in response to a
// PINGREQ packet" [MQTT-3.12.4-1].
func (c *Client) Ping() error {
	if err := c.Send(&Pingreq{}); err != nil {
		return fmt.Errorf("PINGREQ: %w", err)
	}
	p, err := c.Recv()
	if err != nil {
		return fmt.Errorf("waiting for PINGRESP: %w", err)
	}
	if _, ok := p.(*Pingresp); !ok {
		return fmt.Errorf("%s %+v, want PINGRESP", Name(p.Type()), p)
	}
	return nil
}

// IsTimeout reports whether err is a read or write deadline passing.
func IsTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Name is a packet type's name, for messages.
func Name(kind byte) string {
	names := [16]string{"reserved 0", "CONNECT", "CONNACK", "PUBLISH", "PUBACK",
		"PUBREC", "PUBREL", "PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE",
		"UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT", "AUTH"}
	return names[kind&0x0F]
}

// AnswerKind is the acknowledgement a PUBLISH at qos above 0 gets first:
// PUBACK at QoS 1, PUBREC at QoS 2 (MQTT 5 4.3.2: the receiver "MUST
// respond with a PUBACK packet" [MQTT-4.3.2-4]; 4.3.3: it "MUST respond
// with a PUBREC" [MQTT-4.3.3-8]).
func AnswerKind(qos byte) byte {
	if qos == 2 {
		return PUBREC
	}
	return PUBACK
}

// VersionName names an MQTT version, V311 or V5, for messages and
// subtests.
func VersionName(v byte) string {
	if v == V311 {
		return "MQTT 3.1.1"
	}
	return "MQTT 5"
}
