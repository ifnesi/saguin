package harness

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
)

// RFC 6455's own examples, written out here by hand: 1.3, the key
// "dGhlIHNhbXBsZSBub25jZQ==" is answered "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
// 5.7, "A single-frame masked text message: 0x81 0x85 0x37 0xfa 0x21 0x3d
// 0x7f 0x9f 0x4d 0x51 0x58 (contains "Hello")"; and 5.2, a payload of 126
// to 65,535 bytes has the length 126 then 2 bytes, big-endian, as 5.7's
// "256 bytes binary message in a single unmasked frame: 0x82 0x7E 0x0100"
// has, a client's frame setting MASK besides.
func TestWebSocketFramesAreTheRFCs(t *testing.T) {
	if got, want := AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="; got != want {
		t.Errorf("AcceptKey: %q; want %q", got, want)
	}
	hello := AppendFrame(nil, true, OpText, [4]byte{0x37, 0xfa, 0x21, 0x3d}, []byte("Hello"))
	if want := []byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}; !bytes.Equal(hello, want) {
		t.Errorf("the masked text frame of Hello: % X; want % X", hello, want)
	}
	long := AppendFrame(nil, true, OpBinary, [4]byte{}, make([]byte, 256))
	if want := []byte{0x82, 0xFE, 0x01, 0x00, 0, 0, 0, 0}; !bytes.Equal(long[:len(want)], want) || len(long) != len(want)+256 {
		t.Errorf("a masked 256-byte binary frame begins % X and is %d bytes; want % X and %d", long[:len(want)], len(long), want, len(want)+256)
	}
}

// written is a connection that keeps what is written to it.
type written struct {
	net.Conn
	sent bytes.Buffer
}

func (w *written) Write(p []byte) (int, error) { return w.sent.Write(p) }

// RFC 6455 5.7: "A fragmented unmasked text message: 0x01 0x03 0x48 0x65
// 0x6c (contains "Hel") 0x80 0x02 0x6c 0x6f (contains "lo")"; "Unmasked
// Ping request and masked Ping response: 0x89 0x05 0x48 0x65 0x6c 0x6c
// 0x6f (contains a body of "Hello", but the contents of the body are
// arbitrary) 0x8a 0x85 0x37 0xfa 0x21 0x3d 0x7f 0x9f 0x4d 0x51 0x58"; 5.4:
// "Control frames MAY be injected in the middle of a fragmented message".
// The same message in binary (0x02), a Ping between its fragments, then
// the end of the stream, read as one stream: "Hello", one Pong answered
// with the Ping's body, masked, then the end of the stream. A text frame
// is an error (MQTT-6.0.0-1), and so is a Close frame (0x88, 5.5.1), which
// the broker never sends (design.md, Flows, Transport setup: "Closing a
// WebSocket connection ends the stream as on TCP, with no WebSocket Close
// frame").
func TestWebSocketReadsTheRFCsFrames(t *testing.T) {
	stream := []byte{0x02, 0x03, 'H', 'e', 'l', 0x89, 0x05, 'H', 'e', 'l', 'l', 'o', 0x80, 0x02, 'l', 'o'}
	out := new(written)
	w := &WebSocket{Conn: out, in: bufio.NewReader(bytes.NewReader(stream))}
	got, err := io.ReadAll(w)
	if err != nil || string(got) != "Hello" {
		t.Errorf("read %q, %v; want Hello and the end of the stream", got, err)
	}
	pong := out.sent.Bytes()
	if len(pong) != 2+4+5 || pong[0] != 0x8A || pong[1] != 0x85 {
		t.Fatalf("the answer to the Ping: % X; want a masked Pong of 5 bytes, 0x8A 0x85, its key and body", pong)
	}
	for i := range 5 {
		pong[6+i] ^= pong[2+i%4]
	}
	if string(pong[6:]) != "Hello" {
		t.Errorf("the Pong's body is %q; want the Ping's, Hello", pong[6:])
	}

	for _, c := range []struct {
		frame []byte
		want  error
	}{
		{[]byte{0x81, 0x05, 'H', 'e', 'l', 'l', 'o'}, ErrTextFrame},
		{[]byte{0x88, 0x00}, ErrCloseFrame},
	} {
		r := &WebSocket{Conn: out, in: bufio.NewReader(bytes.NewReader(c.frame))}
		if _, err := r.Read(make([]byte, 5)); !errors.Is(err, c.want) {
			t.Errorf("reading % X: %v; want %v", c.frame, err, c.want)
		}
	}
}

// RFC 6455 has a client fail the connection on each of these frames from
// the server: 5.2, "RSV1, RSV2, RSV3: [...] MUST be 0 unless an extension
// is negotiated that defines meanings for non-zero values"; "the minimal
// number of bytes MUST be used to encode the length"; "the most
// significant bit MUST be 0"; 5.4, "A fragmented message consists of a
// single frame with the FIN bit clear and an opcode other than 0, followed
// by zero or more frames with the FIN bit clear and the opcode set to 0,
// and terminated by a single frame with the FIN bit set and an opcode of
// 0", and "the fragments of one message MUST NOT be interleaved between
// the fragments of another message"; 5.5, "All control frames MUST have a
// payload length of 125 bytes or less and MUST NOT be fragmented"; 5.5.1,
// a Close's body, if any, begins with "a 2-byte unsigned integer".
//
// Each stream is read as one: what the binary frames before the bad one
// carry is read, then the bad frame is ErrBadFrame, never MQTT's bytes.
func TestWebSocketRefusesTheFramesTheRFCForbids(t *testing.T) {
	long := func(header ...byte) []byte { return append(header, make([]byte, 126)...) }
	for _, c := range []struct {
		name   string
		stream []byte
		before string // what is read before the bad frame
	}{
		{"a continuation with no message begun", []byte{0x80, 0x02, 'n', 'o'}, ""},
		{"RSV1 set", []byte{0xC2, 0x02, 'n', 'o'}, ""},
		{"RSV2 set on a Ping", []byte{0xA9, 0x00}, ""},
		{"RSV3 set on a continuation", []byte{0x02, 0x02, 'o', 'k', 0x90, 0x02, 'n', 'o'}, "ok"},
		{"a new binary message before the fragmented one ends", []byte{0x02, 0x02, 'o', 'k', 0x82, 0x02, 'n', 'o'}, "ok"},
		{"a fragmented Ping", []byte{0x09, 0x00, 0x80, 0x00}, ""},
		{"a Ping of 126 bytes", long(0x89, 126, 0x00, 126), ""},
		{"a length of 5 in 2 bytes", []byte{0x82, 126, 0x00, 0x05, 'n', 'o', 'n', 'o', '!'}, ""},
		{"a length of 126 in 8 bytes", long(0x82, 127, 0, 0, 0, 0, 0, 0, 0x00, 126), ""},
		{"a length with its most significant bit set", []byte{0x82, 127, 0x80, 0, 0, 0, 0, 0, 0x00, 0x05}, ""},
		{"a Close with a one-byte body", []byte{0x88, 0x01, 0x03}, ""},
	} {
		w := &WebSocket{Conn: new(written), in: bufio.NewReader(bytes.NewReader(c.stream))}
		got, err := io.ReadAll(w)
		if string(got) != c.before || !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s, % X: read %q, %v; want %q, then ErrBadFrame", c.name, c.stream, got, err, c.before)
		}
	}
}

// discarded is a connection that takes every write whole and keeps nothing.
type discarded struct{ net.Conn }

func (discarded) Write(p []byte) (int, error) { return len(p), nil }

// The sprint skill's standard for code and tests, which the harness is held
// to: "no copy or allocation it can avoid"; design.md, Testing: the busy
// path "makes no second pass over the same data and avoids every copy or
// allocation it can". RFC 6455 5.4: a message's frames, "a single frame
// with the FIN bit clear and an opcode other than 0, followed by zero or
// more frames with the FIN bit clear and the opcode set to 0, and
// terminated by a single frame with the FIN bit set and an opcode of 0".
//
// A Write of 4,096 bytes cut into messages of 1,000 bytes, each in frames
// of one byte, allocates once, averaged over 100 Writes: the buffer of the
// wire it writes, framed in place, whatever the number of frames.
func TestWebSocketWriteAllocatesOnlyItsWire(t *testing.T) {
	w := &WebSocket{Conn: discarded{}, messageSize: 1000, frameSize: 1}
	p := bytes.Repeat([]byte("x"), 4096)
	if allocs := testing.AllocsPerRun(100, func() { _, _ = w.Write(p) }); allocs != 1 {
		t.Errorf("a fragmented Write allocated %v times; want once, its wire", allocs)
	}
}

// pausedReader gives its chunks in turn, a passed read deadline where a
// chunk is nil, as a connection whose read deadline passed mid-frame does,
// then the end of the stream: with the last chunk's last bytes where
// endWithLast is set, as TLS gives a record followed by close_notify.
type pausedReader struct {
	chunks      [][]byte
	endWithLast bool
}

func (r *pausedReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	c := r.chunks[0]
	if c == nil {
		r.chunks = r.chunks[1:]
		return 0, os.ErrDeadlineExceeded
	}
	n := copy(p, c)
	if r.chunks[0] = c[n:]; len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	if r.endWithLast && len(r.chunks) == 0 {
		return n, io.EOF
	}
	return n, nil
}

// RFC 6455 5.2: a frame's "Payload length" is "The length of the "Payload
// data""; 5.4: a fragmented message is "terminated by a single frame with
// the FIN bit set". A stream that ends inside a frame's header or payload,
// or between the frames of a fragmented message, ended inside a message:
// io.ErrUnexpectedEOF, after what the whole frames before carried. A
// stream that ends between messages ends cleanly, io.EOF. A read whose
// deadline passes inside a frame returns that error, and reading goes on.
func TestWebSocketStreamCutShortIsUnexpected(t *testing.T) {
	for _, c := range []struct {
		name   string
		stream []byte
		read   string
		want   error // io.ReadAll's: nil for the clean end
	}{
		{"a whole message, then the end", []byte{0x82, 0x02, 'o', 'k'}, "ok", nil},
		{"a payload short of its length", []byte{0x82, 0x05, 0xD0, 0x00}, "\xD0\x00", io.ErrUnexpectedEOF},
		{"a fragmented message unfinished", []byte{0x02, 0x02, 0xD0, 0x00}, "\xD0\x00", io.ErrUnexpectedEOF},
		{"a header cut after its first byte", []byte{0x82, 0x02, 'o', 'k', 0x82}, "ok", io.ErrUnexpectedEOF},
		{"a header cut inside its length", []byte{0x82, 126, 0x01}, "", io.ErrUnexpectedEOF},
		{"a Ping cut inside its payload", []byte{0x89, 0x05, 'p'}, "", io.ErrUnexpectedEOF},
	} {
		w := &WebSocket{Conn: new(written), in: bufio.NewReader(bytes.NewReader(c.stream))}
		got, err := io.ReadAll(w)
		if string(got) != c.read || !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s, % X: read %q, %v; want %q, then %v", c.name, c.stream, got, err, c.read, c.want)
		}
	}

	// A stream whose last read returns a message's final frame together with
	// the end of the stream, as TLS 1.2 does when close_notify follows the
	// record: a clean end. The read's buffer is larger than the reader's own,
	// so the read reaches the stream itself.
	whole := bytes.Repeat([]byte("x"), 8192)
	w := &WebSocket{Conn: new(written), in: bufio.NewReaderSize(&pausedReader{
		chunks: [][]byte{{0x82, 126, 0x20, 0x00}, whole}, endWithLast: true}, 16)}
	n, err := w.Read(make([]byte, 2*len(whole)))
	if n != len(whole) || (err != nil && !errors.Is(err, io.EOF)) {
		t.Errorf("a final frame's bytes with the end of the stream: read %d bytes, %v; want %d, then the clean end", n, err, len(whole))
	} else if _, err := w.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("after a final frame and the end of the stream: %v; want io.EOF, the clean end", err)
	}

	in := &pausedReader{chunks: [][]byte{{0x82, 0x04, 'o'}, nil, {'n', 'e'}, nil, {'s'}}}
	w = &WebSocket{Conn: new(written), in: bufio.NewReader(in)}
	var got []byte
	paused := 0
	for {
		b := make([]byte, 8)
		n, err := w.Read(b)
		got = append(got, b[:n]...)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			paused++
			continue
		}
		if err != nil {
			if !errors.Is(err, io.EOF) { // the clean end, never ErrUnexpectedEOF
				t.Errorf("reading on after passed deadlines: %v; want the message whole, then io.EOF", err)
			}
			break
		}
	}
	if string(got) != "ones" || paused != 2 {
		t.Errorf("a frame paused twice read %q, %d deadlines passing; want \"ones\" and 2", got, paused)
	}
}
