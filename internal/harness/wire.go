// Package harness is Sagüin's test client: a raw MQTT 3.1.1 and 5 client
// written from the OASIS standards, with its own encoder and decoder.
//
// It never imports a broker package, so a codec bug in the broker cannot hide
// itself by being shared with the client that tests it. It is not linked into
// the broker, which is why it may start goroutines (the R1 and R3 scans skip
// it, and fail if broker code imports it).
//
// The encoder is permissive and the decoder strict, on purpose: tests must be
// able to send what a broker has to refuse (invalid UTF-8, a QoS of 3,
// reserved bits, a packet identifier of 0), while everything the broker sends
// back is checked against the standards.
package harness

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// Control packet types: MQTT 3.1.1 §2.2.1, MQTT 5.0 §2.1.2.
const (
	CONNECT     byte = 1
	CONNACK     byte = 2
	PUBLISH     byte = 3
	PUBACK      byte = 4
	PUBREC      byte = 5
	PUBREL      byte = 6
	PUBCOMP     byte = 7
	SUBSCRIBE   byte = 8
	SUBACK      byte = 9
	UNSUBSCRIBE byte = 10
	UNSUBACK    byte = 11
	PINGREQ     byte = 12
	PINGRESP    byte = 13
	DISCONNECT  byte = 14
	AUTH        byte = 15 // MQTT 5 only; reserved in 3.1.1
)

// Protocol levels carried by CONNECT: MQTT 3.1.1 §3.1.2.2, MQTT 5.0 §3.1.2.2.
const (
	V311 byte = 4
	V5   byte = 5
)

// MaxVarInt is the largest Variable Byte Integer, and so the largest
// Remaining Length: MQTT 3.1.1 §2.2.3, MQTT 5.0 §1.5.5.
const MaxVarInt = 268_435_455

// ErrMalformed marks bytes that do not parse as the standard says (MQTT 5
// reason 0x81). ErrProtocol marks bytes that parse but break a rule of the
// standard (MQTT 5 reason 0x82). ErrTooLarge marks a packet over the
// reader's limit, refused before its body is read (MQTT 5 reason 0x95).
// Decoding errors wrap one of the three. ErrTopicAlias, which wraps
// ErrProtocol, marks the Protocol Error to which MQTT 5 gives a reason of
// its own, a Topic Alias the receiver does not accept: "A Topic Alias value
// of 0 or greater than the Maximum Topic Alias is a Protocol Error, the
// receiver uses DISCONNECT with Reason Code of 0x94 (Topic Alias invalid)"
// (5.0 §3.3.4).
var (
	ErrMalformed  = errors.New("malformed packet")
	ErrProtocol   = errors.New("protocol error")
	ErrTooLarge   = errors.New("packet too large")
	ErrTopicAlias = fmt.Errorf("%w: topic alias invalid", ErrProtocol)
)

// AppendVarInt appends v as a Variable Byte Integer. A value above MaxVarInt
// is still written, in the five-byte form the standard forbids, so a test can
// send it to provoke a malformed packet.
func AppendVarInt(b []byte, v uint32) []byte {
	for {
		digit := byte(v % 128)
		v /= 128
		if v > 0 {
			digit |= 0x80
		}
		b = append(b, digit)
		if v == 0 {
			return b
		}
	}
}

// ReadVarInt reads an MQTT 5 Variable Byte Integer. It refuses more than four
// bytes and a value written in more bytes than it needs ([MQTT-1.5.5-1]: "the
// minimum number of bytes necessary").
func ReadVarInt(r io.ByteReader) (uint32, error) {
	return readVarInt(r, true)
}

// readVarInt reads a Variable Byte Integer, at most four bytes in both
// versions (3.1.1 §2.2.3: "The maximum number of bytes in the Remaining
// Length field is four"). shortest also refuses a longer form than needed,
// which only MQTT 5 forbids ([MQTT-1.5.5-1]); MQTT 3.1.1 states no such rule,
// so a 3.1.1 Remaining Length is read without it.
func readVarInt(r io.ByteReader, shortest bool) (uint32, error) {
	var v uint32
	for i := range 4 {
		digit, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if shortest && i > 0 && digit == 0 {
			// A last byte of zero adds nothing: a shorter encoding existed.
			return 0, fmt.Errorf("%w: variable byte integer not in its shortest form", ErrMalformed)
		}
		v |= uint32(digit&0x7F) << (7 * i)
		if digit&0x80 == 0 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("%w: variable byte integer longer than four bytes", ErrMalformed)
}

// AppendString appends a UTF-8 Encoded String: a two-byte length, then the
// bytes as given, unchecked, so a test can send an invalid one.
func AppendString(b []byte, s string) []byte {
	return AppendBinary(b, []byte(s))
}

// AppendBinary appends Binary Data: a two-byte length, then the bytes.
func AppendBinary(b, data []byte) []byte {
	b = append(b, byte(len(data)>>8), byte(len(data)))
	return append(b, data...)
}

// encoder builds a packet body. The first error sticks, so callers check once.
type encoder struct {
	b   []byte
	err error
}

func (e *encoder) byte(v byte)     { e.b = append(e.b, v) }
func (e *encoder) uint16(v uint16) { e.b = append(e.b, byte(v>>8), byte(v)) }
func (e *encoder) uint32(v uint32) {
	e.b = append(e.b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func (e *encoder) varint(v uint32) { e.b = AppendVarInt(e.b, v) }
func (e *encoder) bytes(v []byte)  { e.b = append(e.b, v...) }

func (e *encoder) string(s string) {
	e.data([]byte(s), "string")
}

func (e *encoder) binary(data []byte) {
	e.data(data, "binary data")
}

// data writes the common two-byte length and body of strings and binary data.
func (e *encoder) data(data []byte, kind string) {
	if len(data) > 0xFFFF {
		e.fail("%s of %d bytes is longer than 65,535", kind, len(data))
		return
	}
	e.b = AppendBinary(e.b, data)
}

func (e *encoder) fail(format string, args ...any) {
	if e.err == nil {
		e.err = fmt.Errorf(format, args...)
	}
}

// decoder reads a packet body. The first error sticks and every later read
// returns zero values, so a decoding function checks d.err once at its end.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n > len(d.b) {
		d.malformed("%d bytes needed, %d left", n, len(d.b))
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

func (d *decoder) byte() byte {
	if v := d.take(1); v != nil {
		return v[0]
	}
	return 0
}

func (d *decoder) uint16() uint16 {
	if v := d.take(2); v != nil {
		return uint16(v[0])<<8 | uint16(v[1])
	}
	return 0
}

func (d *decoder) uint32() uint32 {
	if v := d.take(4); v != nil {
		return uint32(v[0])<<24 | uint32(v[1])<<16 | uint32(v[2])<<8 | uint32(v[3])
	}
	return 0
}

func (d *decoder) varint() uint32 {
	if d.err != nil {
		return 0
	}
	r := &sliceReader{b: d.b}
	v, err := ReadVarInt(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			d.malformed("variable byte integer cut short")
		} else {
			d.err = err
		}
		return 0
	}
	d.b = r.b
	return v
}

func (d *decoder) binary() []byte {
	n := d.uint16()
	if d.err != nil {
		return nil
	}
	return clone(d.take(int(n)))
}

// string reads a UTF-8 Encoded String and checks it: well-formed UTF-8 with
// no surrogates ([MQTT-1.5.4-1], 3.1.1 [MQTT-1.5.3-1]) and no U+0000
// ([MQTT-1.5.4-2], 3.1.1 [MQTT-1.5.3-2]). Go's utf8.ValidString rejects the
// surrogate code points, as the standards ask.
func (d *decoder) string() string {
	n := d.uint16()
	v := d.take(int(n))
	if d.err != nil {
		return ""
	}
	s := string(v)
	if !utf8.ValidString(s) {
		d.malformed("string %q is not well-formed UTF-8", s)
	} else if strings.ContainsRune(s, 0) {
		d.malformed("string %q holds U+0000", s)
	}
	return s
}

// rest takes every byte left (a payload).
func (d *decoder) rest() []byte {
	v := clone(d.b)
	d.b = nil
	return v
}

// clone copies decoded bytes, so a packet does not keep the read buffer
// alive. Empty is nil, so a decoded packet compares equal to the one encoded.
func clone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte{}, b...)
}

func (d *decoder) malformed(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
	}
}

func (d *decoder) protocol(what string) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %s", ErrProtocol, what)
	}
}

// sliceReader is an io.ByteReader over a slice that keeps what is left.
type sliceReader struct{ b []byte }

func (r *sliceReader) ReadByte() (byte, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c, nil
}
