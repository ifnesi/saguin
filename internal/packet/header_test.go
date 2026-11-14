package packet_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
	"testing/iotest"

	"github.com/ifnesi/saguin/internal/packet"
)

// versions are the two protocols Sagüin speaks (design R7).
var versions = []struct {
	v    packet.Version
	name string
}{
	{packet.V311, "3.1.1"},
	{packet.V5, "5"},
}

// guardReader hands out data one byte at a time and fails the test when asked
// for a byte at or past limit, the first byte the reader under test must not
// take. It is how these tests see that a fixed header is read without
// touching the body behind it.
type guardReader struct {
	t     *testing.T
	data  []byte
	limit int
	pos   int
}

// errPastLimit is what guardReader returns after it has failed the test.
var errPastLimit = errors.New("guardReader: read past the fixed header")

func (g *guardReader) ReadByte() (byte, error) {
	if g.pos >= g.limit {
		g.t.Errorf("% X: asked for byte %d (from 1), past the %d bytes the fixed header may use", g.data, g.pos+1, g.limit)
		return 0, errPastLimit
	}
	if g.pos >= len(g.data) {
		return 0, io.EOF
	}
	b := g.data[g.pos]
	g.pos++
	return b, nil
}

// guard returns a guardReader over header followed by three body bytes, which
// may read only the header.
func guard(t *testing.T, header []byte) *guardReader {
	data := append(append([]byte{}, header...), 0xEE, 0xEE, 0xEE)
	return &guardReader{t: t, data: data, limit: len(header)}
}

// TestReadHeaderPublishFlags checks that PUBLISH's flags are its own, not
// Reserved: DUP, QoS and RETAIN (MQTT 5 2.1.3 Table 2-2; MQTT 3.1.1 2.2.2
// Table 2.2), handed back as received. QoS 3 is left to the PUBLISH packet's
// own rules (3.3.1.2), not tested here. QoS 2's flags, with and without DUP
// and RETAIN, are checked on the wire, where a client sends them
// (conformance: the tests of incoming QoS 2, qos2_test.go).
func TestReadHeaderPublishFlags(t *testing.T) {
	for _, ver := range versions {
		for flags := range byte(16) {
			if flags>>1&0b11 >= 2 {
				continue // QoS 2, and QoS 3
			}
			header := []byte{0x30 | flags, 0x00}
			h, err := packet.ReadHeader(guard(t, header), ver.v, math.MaxInt)
			want := packet.Header{Type: packet.PUBLISH, Flags: flags, Length: 0}
			if err != nil || h != want {
				t.Errorf("MQTT %s, % X: got %+v, %v; want %+v, nil", ver.name, header, h, err, want)
			}
		}
	}
}

// TestReadHeaderReaderError hands back the reader's own failure, at the first
// byte and inside the Remaining Length. This is the package's contract, not a
// standard's: a connection that fails is not a packet that breaks a rule, and
// the caller must tell the two apart.
func TestReadHeaderReaderError(t *testing.T) {
	failure := errors.New("connection reset")
	for _, prefix := range [][]byte{{}, {0x30}, {0x30, 0x80}} {
		r := bufio.NewReader(io.MultiReader(bytes.NewReader(prefix), iotest.ErrReader(failure)))
		_, err := packet.ReadHeader(r, packet.V5, math.MaxInt)
		if !errors.Is(err, failure) {
			t.Errorf("reader failing after % X: err = %v, want the reader's error", prefix, err)
		}
	}
}

// FuzzReadHeader feeds random bytes to ReadHeader. It must never panic; a
// refusal is a packet error or the reader running out; a header it accepts
// fits the limit (design R6) and writes back as exactly the bytes it read,
// under both versions: a non-minimal Remaining Length is refused by MQTT 5
// [MQTT-1.5.5-1] and, for MQTT 3.1.1, by the design (Choices MQTT leaves
// open: "A Remaining Length not in its shortest form ... refused, as MQTT 5
// refuses it").
func FuzzReadHeader(f *testing.F) {
	f.Add([]byte{0x30, 0x00}, true, uint32(1024))
	f.Add([]byte{0x82, 0xC1, 0x02, 0x00}, false, uint32(1024))
	f.Add([]byte{0x3B, 0xFF, 0xFF, 0xFF, 0x7F}, true, uint32(math.MaxUint32))
	f.Add([]byte{0x30, 0x80, 0x80, 0x80, 0x80, 0x01}, true, uint32(1024))
	f.Add([]byte{0xF0, 0x00}, false, uint32(1024))
	f.Add([]byte{0x30, 0x80, 0x00}, true, uint32(1024))
	f.Add([]byte{0x30, 0x80, 0x00}, false, uint32(1024))
	f.Fuzz(func(t *testing.T, data []byte, v5 bool, maxSize uint32) {
		version, name := packet.V311, "3.1.1"
		if v5 {
			version, name = packet.V5, "5"
		}
		r := bytes.NewReader(data)
		h, err := packet.ReadHeader(r, version, int(maxSize))
		if err != nil {
			if !isKind(err) && !errors.Is(err, packet.ErrTooLarge) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("% X: err = %v, neither a packet error nor the reader running out", data, err)
			}
			return
		}
		n := len(data) - r.Len()
		if h.Length < 0 || h.Length > 268_435_455 {
			t.Fatalf("% X: Remaining Length %d out of range", data, h.Length)
		}
		if size := n + h.Length; size > int(maxSize) {
			t.Fatalf("% X: packet size %d accepted over the limit %d", data, size, maxSize)
		}
		if got, err := packet.AppendHeader(nil, h); err != nil || !bytes.Equal(got, data[:n]) {
			t.Fatalf("MQTT %s: read % X as %+v, which writes back as % X (err %v)", name, data[:n], h, got, err)
		}
	})
}
