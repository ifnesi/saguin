package connection_test

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// streamSocket is a connection's socket over the bytes a fast client has
// already sent: each Read gives as many as it asks, up to the end of
// stream, so a reader that asks for more than its packet takes the next
// one's bytes too. With loop, the stream starts again at its end, for as
// many packets as a benchmark reads. It counts its Reads and calls onRead,
// where set, at each, before it gives anything.
type streamSocket struct {
	stream []byte
	loop   bool
	pos    int
	reads  int
	onRead func()
}

func (s *streamSocket) Read(p []byte) (int, error) {
	s.reads++
	if s.onRead != nil {
		s.onRead()
	}
	if s.loop && s.pos == len(s.stream) {
		s.pos = 0
	}
	if s.pos == len(s.stream) {
		return 0, io.EOF
	}
	n := copy(p, s.stream[s.pos:])
	s.pos += n
	return n, nil
}

func (*streamSocket) Write(p []byte) (int, error)      { return len(p), nil }
func (*streamSocket) SetReadDeadline(time.Time) error  { return nil }
func (*streamSocket) SetWriteDeadline(time.Time) error { return nil }
func (*streamSocket) CloseWrite() error                { return nil }
func (*streamSocket) Close() error                     { return nil }
func (*streamSocket) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1883}
}

// pingreqs is the stream of streamSocket of a client sending only PINGREQs,
// a packet with no body: many of them, so a reader that read past its
// packet would read the next one's bytes, not the stream's end.
var pingreqs = bytes.Repeat([]byte{0xC0, 0x00}, 1024)

// packetReader returns a reader, connected as MQTT 5 under the tests'
// configuration, of socket, and the registry its counts go to.
func packetReader(t testing.TB, socket *streamSocket) (*connection.PacketReader, *metrics.Registry) {
	t.Helper()
	reg := metrics.New()
	return connection.NewPacketReader(socket, packet.V5, &newConfig(t).Broker.Limits, connection.NewMetrics(reg)), reg
}

// readPingreq reads the next packet from r, which must be a PINGREQ. It
// marks no helper, which would cost the allocation guard its own.
func readPingreq(t testing.TB, r *connection.PacketReader) {
	ev, err := r.Next()
	if err != nil || ev.Kind != connection.Received || ev.Packet != (connection.Pingreq{}) {
		t.Fatalf("read %s, %v; want a PINGREQ", describe(&ev), err)
	}
}

// design.md, Testing: a test inside the code checks "the guards of [...]
// the busy path"; design.md, Flows, Packet buffers: "Between packets, a
// connection holds no read or write buffer". 0005b.md, 261012 112516, on
// the reader's fixed header: "headerInput escapes to the heap as
// ReadHeader's io.ByteReader, one 16-byte allocation per packet beside its
// body [...] The rework removes all three costs (calls, allocation, a count
// per byte)". A connected reader reading PINGREQs, which have no body,
// allocates nothing for any.
func TestHeaderReadAllocatesNothing(t *testing.T) {
	r, _ := packetReader(t, &streamSocket{stream: pingreqs, loop: true})
	testkit.NoAllocs(t, "a connected reader's read of a PINGREQ", func() { readPingreq(t, r) })
}

// metrics.md, Publishing: saguin_bytes_received_total, "bytes on MQTT
// connections". 0005b.md, 261012 112516: "each header byte read adds to
// saguin_bytes_received_total on its own", a cost the header rework
// removes; the count stays exact. A connected reader reads two PINGREQs,
// then a DISCONNECT whose Remaining Length takes two bytes and whose body
// follows: at every Read on the socket the count is every byte the Reads
// before it returned, a packet cut short included, and after the last it
// is every byte sent.
func TestBytesReceivedCountedAsRead(t *testing.T) {
	disconnect := harness.Encoded(t, &harness.Disconnect{Props: harness.Properties{{ID: harness.ReasonString, Str: strings.Repeat("z", 200)}}}, harness.V5)
	packets := [][]byte{{0xC0, 0x00}, {0xC0, 0x00}, disconnect}
	socket := &streamSocket{stream: bytes.Join(packets, nil)}
	r, reg := packetReader(t, socket)
	socket.onRead = func() {
		if got := testkit.Sample(t, reg, "saguin_bytes_received_total"); got != float64(socket.pos) {
			t.Errorf("at a Read %d bytes into the stream, saguin_bytes_received_total is %v; want %d, the bytes read before it",
				socket.pos, got, socket.pos)
		}
	}
	for i := range packets {
		if _, err := r.Next(); err != nil {
			t.Fatalf("packet %d: %v", i+1, err)
		}
	}
	socket.onRead = nil
	if got, want := testkit.Sample(t, reg, "saguin_bytes_received_total"), len(socket.stream); got != float64(want) {
		t.Errorf("after %d packets of %d bytes in all, saguin_bytes_received_total is %v", len(packets), want, got)
	}
}

// design.md, Testing, the busy path; 0005b.md, 261012 103723: "the reader
// takes a packet's fixed header one byte per read, about three system
// calls for a small packet where two would do [...] read the fixed header
// in fewer calls, shown with a benchmark". A connected reader reads
// PINGREQs, as a client keeping its connection alive sends them; the
// benchmark reports the Reads each takes and what each allocates.
func BenchmarkReadFixedHeader(b *testing.B) {
	socket := &streamSocket{stream: pingreqs, loop: true}
	r, _ := packetReader(b, socket)
	b.ReportAllocs()
	packets := 0
	for b.Loop() {
		readPingreq(b, r)
		packets++
	}
	b.ReportMetric(float64(socket.reads)/float64(packets), "reads/packet")
}
