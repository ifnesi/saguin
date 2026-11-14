package packet

import (
	"io"
	"slices"
)

// Header is the MQTT fixed header. Length counts only the variable header and
// payload; the encoded fixed-header bytes are accounted for by ReadHeader.
type Header struct {
	Type   Type
	Flags  byte
	Length int
}

// fixedFlags gives the reserved low nibble for every type except PUBLISH,
// whose low nibble carries live flags and is validated with the packet body.
var fixedFlags = [...]byte{
	CONNECT:     0,
	CONNACK:     0,
	PUBACK:      0,
	PUBREC:      0,
	PUBREL:      2,
	PUBCOMP:     0,
	SUBSCRIBE:   2,
	SUBACK:      0,
	UNSUBSCRIBE: 2,
	UNSUBACK:    0,
	PINGREQ:     0,
	PINGRESP:    0,
	DISCONNECT:  0,
	AUTH:        0,
}

// ReadHeader reads only an MQTT fixed header. It checks the complete packet
// size before its caller can read or allocate the body.
func ReadHeader(r io.ByteReader, version Version, maxSize int) (Header, error) {
	first, err := r.ReadByte()
	if err != nil {
		return Header{}, err
	}

	typ := Type(first >> 4)
	flags := first & 0x0f
	if version != V311 && version != V5 {
		return Header{}, ErrMalformed
	}
	if typ < CONNECT || typ > AUTH || (version == V311 && typ == AUTH) {
		return Header{}, ErrMalformed
	}
	if typ != PUBLISH && flags != fixedFlags[typ] {
		return Header{}, ErrMalformed
	}

	remaining, n, err := readRemainingLength(r)
	if err != nil {
		return Header{}, err
	}
	header := Header{Type: typ, Flags: flags, Length: int(remaining)}
	fixedSize := 1 + n
	// The subtraction form avoids overflowing when callers use the largest
	// int as an effectively unlimited bound.
	if maxSize < fixedSize || header.Length > maxSize-fixedSize {
		return header, ErrTooLarge
	}
	return header, nil
}

// readRemainingLength is deliberately separate from DecodeVarInt: a stream
// must not consume a fifth byte which could belong to something outside the
// malformed packet.
func readRemainingLength(r io.ByteReader) (value uint32, bytesRead int, err error) {
	for pos := range 4 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, pos, err
		}
		value |= uint32(b&0x7f) << (7 * pos)
		if b&0x80 == 0 {
			if pos+1 != varIntSize(value) {
				return 0, pos + 1, ErrMalformed
			}
			return value, pos + 1, nil
		}
	}
	return 0, 4, ErrMalformed
}

// appendFixedHeader reserves room for a checked packet and writes its fixed
// header. The caller appends the body into the reserved space.
func appendFixedHeader(dst []byte, first byte, bodySize int) []byte {
	dst = slices.Grow(dst, 1+varIntSize(uint32(bodySize))+bodySize)
	dst = append(dst, first)
	return binaryAppendVarInt(dst, uint32(bodySize))
}

// AppendHeader appends the first byte and Remaining Length of a fixed header.
func AppendHeader(dst []byte, header Header) ([]byte, error) {
	if header.Type < CONNECT || header.Type > AUTH || header.Flags > 0x0f ||
		header.Length < 0 || header.Length > maxVarInt {
		return dst, ErrCannotEncode
	}
	if header.Type != PUBLISH && header.Flags != fixedFlags[header.Type] {
		return dst, ErrCannotEncode
	}
	dst = append(dst, byte(header.Type)<<4|header.Flags)
	return AppendVarInt(dst, uint32(header.Length))
}
