package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// isKind reports whether err is one of the two kinds MQTT 5 4.13.1 gives a
// refused packet: Malformed Packet (0x81) or Protocol Error (0x82). Tests use
// it where the standard requires a refusal without naming its kind; where it
// names one, they check that one.
func isKind(err error) bool {
	return errors.Is(err, packet.ErrMalformed) || errors.Is(err, packet.ErrProtocol)
}

// prefixed returns data behind its Two Byte Integer length, as UTF-8 Encoded
// Strings and Binary Data are sent (MQTT 5 1.5.4 Figure 1-1, 1.5.6).
func prefixed(data []byte) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(len(data))), data...)
}

// prefixedField reads what prefixed writes, without the code under test: a
// Two Byte Integer length and that many bytes. It reports false when b
// holds fewer.
func prefixedField(b []byte) (value, rest []byte, ok bool) {
	if len(b) < 2 || len(b)-2 < int(binary.BigEndian.Uint16(b)) {
		return nil, nil, false
	}
	n := 2 + int(binary.BigEndian.Uint16(b))
	return b[2:n], b[n:], true
}

// illFormed is character data that is not well-formed UTF-8 (RFC 3629): stray
// continuation bytes, overlong forms, bytes UTF-8 never uses, sequences cut
// short, and code points past U+10FFFF.
var illFormed = [][]byte{
	{0x80},
	{0xBF},
	{0x41, 0x80, 0x42},
	{0xC0, 0x80},             // overlong U+0000
	{0xC1, 0xBF},             // overlong U+007F
	{0xE0, 0x80, 0x80},       // overlong U+0000
	{0xE0, 0x9F, 0xBF},       // overlong U+07FF
	{0xF0, 0x80, 0x80, 0x80}, // overlong U+0000
	{0xF0, 0x8F, 0xBF, 0xBF}, // overlong U+FFFF
	{0xF4, 0x90, 0x80, 0x80}, // U+110000
	{0xF5, 0x80, 0x80, 0x80},
	{0xF8, 0x88, 0x80, 0x80, 0x80},
	{0xFE},
	{0xFF},
	{0xC2},
	{0x41, 0xC3},
	{0xE2, 0x82},
	{0xE2, 0x82, 0x41},
	{0xF0, 0x9F, 0x98},
}

// surrogates are encodings of code points between U+D800 and U+DFFF, alone
// and as the two halves of a pair (U+1F600 written as two surrogates).
var surrogates = [][]byte{
	{0xED, 0xA0, 0x80}, // U+D800
	{0xED, 0xAD, 0xBF}, // U+DB7F
	{0xED, 0xB0, 0x80}, // U+DC00
	{0xED, 0xBF, 0xBF}, // U+DFFF
	{0x41, 0xED, 0xA0, 0x80, 0x42},
	{0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80},
}

// nulls are strings holding U+0000.
var nulls = [][]byte{
	{0x00},
	{0x41, 0x00},
	{0x41, 0x00, 0x42},
}

// TestStringBOM keeps U+FEFF wherever it stands. MQTT 5 1.5.4: "A UTF-8
// encoded sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ('ZERO
// WIDTH NO-BREAK SPACE') wherever it appears in a string and MUST NOT be
// skipped over or stripped off by a packet receiver [MQTT-1.5.4-3]." MQTT
// 3.1.1 1.5.3: the same [MQTT-1.5.3-3].
func TestStringBOM(t *testing.T) {
	cases := []string{
		"\uFEFF",
		"\uFEFFA",
		"A\uFEFFB",
		"AB\uFEFF",
		"\uFEFF\uFEFF",
	}
	for _, want := range cases {
		wire := prefixed([]byte(want))
		s, _, err := packet.DecodeString(wire)
		if err != nil || s != want {
			t.Errorf("DecodeString(% X) = %q, %v; want %q", wire, s, err, want)
		}
		if got, err := packet.AppendString(nil, want); err != nil || !bytes.Equal(got, wire) {
			t.Errorf("AppendString(%q) = % X, want % X (err %v)", want, got, wire, err)
		}
	}
}

// TestBinary checks Binary Data. MQTT 5 1.5.6: "a Two Byte Integer length
// which indicates the number of data bytes, followed by that number of bytes
// ... limited to the range of 0 to 65,535 Bytes." Its bytes are any bytes:
// those a string may not hold included.
func TestBinary(t *testing.T) {
	cases := [][]byte{
		{},
		{0x00, 0xFF, 0x80, 0xED, 0xA0, 0x80},
		bytes.Repeat([]byte{0x5A}, 65_535),
	}
	for _, want := range cases {
		wire := prefixed(want)
		b, rest, err := packet.DecodeBinary(append(append([]byte{}, wire...), 0xAA))
		if err != nil || !bytes.Equal(b, want) || !bytes.Equal(rest, []byte{0xAA}) {
			t.Errorf("DecodeBinary(%d bytes) = %d bytes, % X, %v; want them back, AA, nil", len(want), len(b), rest, err)
		}
		if got, err := packet.AppendBinary([]byte{0xAA}, want); err != nil || !bytes.Equal(got, append([]byte{0xAA}, wire...)) {
			t.Errorf("AppendBinary(AA, %d bytes) = % .16X..., want AA % .16X... (err %v)", len(want), got, wire, err)
		}
	}
}

// TestStringPair checks the UTF-8 String Pair. MQTT 5 1.5.7: "two UTF-8
// Encoded Strings ... The first string serves as the name, and the second
// string contains the value."
func TestStringPair(t *testing.T) {
	cases := []struct {
		p    packet.StringPair
		wire []byte
	}{
		{packet.StringPair{Name: "", Value: ""}, []byte{0x00, 0x00, 0x00, 0x00}},
		{packet.StringPair{Name: "name", Value: "value"}, []byte{0x00, 0x04, 'n', 'a', 'm', 'e', 0x00, 0x05, 'v', 'a', 'l', 'u', 'e'}},
		{packet.StringPair{Name: "A\U0002A6D4", Value: "\uFEFF"}, []byte{0x00, 0x05, 0x41, 0xF0, 0xAA, 0x9B, 0x94, 0x00, 0x03, 0xEF, 0xBB, 0xBF}},
	}
	for _, c := range cases {
		p, rest, err := packet.DecodeStringPair(append(append([]byte{}, c.wire...), 0xAA))
		if err != nil || p != c.p || !bytes.Equal(rest, []byte{0xAA}) {
			t.Errorf("DecodeStringPair(% X AA) = %+v, % X, %v; want %+v, AA, nil", c.wire, p, rest, err, c.p)
		}
		if got, err := packet.AppendStringPair([]byte{0xAA}, c.p); err != nil || !bytes.Equal(got, append([]byte{0xAA}, c.wire...)) {
			t.Errorf("AppendStringPair(AA, %+v) = % X, want AA % X (err %v)", c.p, got, c.wire, err)
		}
	}
}

// consumed returns the bytes of data a decoder took, given the rest it handed
// back, and fails the test unless rest is what follows them.
func consumed(t *testing.T, data, rest []byte) []byte {
	t.Helper()
	n := len(data) - len(rest)
	if n < 0 || !bytes.Equal(data[n:], rest) {
		t.Fatalf("% X: rest % X is not what follows the decoded field", data, rest)
	}
	return data[:n]
}

// The fuzz targets below feed random bytes to each data type's decoder. A
// decoder must never panic; it refuses with a packet error; what it accepts
// writes back as exactly the bytes it read (design.md, Testing: "Code that
// reads bytes or text from outside Sagüin is fuzzed, whether the input comes
// from clients, a bridge's peer or the operator's files").
