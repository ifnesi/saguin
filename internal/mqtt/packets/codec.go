// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"bytes"
	"encoding/binary"
	"io"
	"unicode/utf8"
	"unsafe"
)

// bytesToString converts without copying.
//
// **Safe only because decodeBytes copies**, and that is the one place a
// decoded field is copied: the bytes this is handed are already the field's
// own, so the string shares nothing with the packet. Copying here as well
// allocated every string twice.
func bytesToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

// decodeUint16 extracts the value of two bytes from a byte array.
func decodeUint16(buf []byte, offset int) (uint16, int, error) {
	if len(buf) < offset+2 {
		return 0, 0, ErrMalformedOffsetUintOutOfRange
	}

	return binary.BigEndian.Uint16(buf[offset : offset+2]), offset + 2, nil
}

// decodeUint32 extracts the value of four bytes from a byte array.
func decodeUint32(buf []byte, offset int) (uint32, int, error) {
	if len(buf) < offset+4 {
		return 0, 0, ErrMalformedOffsetUintOutOfRange
	}

	return binary.BigEndian.Uint32(buf[offset : offset+4]), offset + 4, nil
}

// decodeString extracts a string from a byte array, beginning at an offset.
func decodeString(buf []byte, offset int) (string, int, error) {
	b, n, err := decodeBytes(buf, offset)
	if err != nil {
		return "", 0, err
	}

	if !validUTF8(b) { // [MQTT-1.5.4-1]
		return "", 0, ErrMalformedInvalidUTF8
	}

	return bytesToString(b), n, nil
}

// validUTF8 checks if the byte array contains valid UTF-8 characters.
func validUTF8(b []byte) bool {
	return utf8.Valid(b) && bytes.IndexByte(b, 0x00) == -1 // [MQTT-1.5.4-1] [MQTT-1.5.4-2]
}

// decodeBytes extracts a length-prefixed field from a byte array, beginning at
// an offset: every string decodeString returns, a Will payload, a user name,
// correlation or authentication data.
//
// **The field is copied, and this is the one place it is.** mochi returned a
// view into the packet's read buffer, so anything the broker kept from a
// packet - a client id, a user name, a subscription filter - kept the whole
// packet alive with it. Measured with 1,000 clients: a CONNECT carrying 60 KiB
// of User Properties left 129 MiB of live heap against 68 MiB for a plain
// one, the CONNECT pinned by its 5-byte client id; a SUBSCRIBE carrying the
// same left 64 MiB against 2 MiB.
//
// What it costs is an allocation per field: decoding a 256-byte PUBLISH went
// from 290 ns and 4 allocations to 373 ns and 10, and a CONNECT from 446 ns to
// 511 ns - noise against an end-to-end publish, where storage and delivery
// dominate.
//
// A PUBLISH's own payload is not decoded here - it is the rest of the packet,
// delivered as it came - so what it can hold on to beyond itself is the topic
// and properties before it, which max_topic_length and the header limits
// already bound.
func decodeBytes(buf []byte, offset int) ([]byte, int, error) {
	length, next, err := decodeUint16(buf, offset)
	if err != nil {
		return make([]byte, 0), 0, err
	}

	if next+int(length) > len(buf) {
		return make([]byte, 0), 0, ErrMalformedOffsetBytesOutOfRange
	}

	// make and copy rather than append to nil, so a zero-length field is still
	// an empty field and not an absent one.
	out := make([]byte, length)
	copy(out, buf[next:next+int(length)])
	return out, next + int(length), nil
}

// decodeByte extracts the value of a byte from a byte array.
func decodeByte(buf []byte, offset int) (byte, int, error) {
	if len(buf) <= offset {
		return 0, 0, ErrMalformedOffsetByteOutOfRange
	}
	return buf[offset], offset + 1, nil
}

// decodeByteBool extracts the value of a byte from a byte array and returns a bool.
func decodeByteBool(buf []byte, offset int) (bool, int, error) {
	if len(buf) <= offset {
		return false, 0, ErrMalformedOffsetBoolOutOfRange
	}
	return 1&buf[offset] > 0, offset + 1, nil
}

// encodeBool returns a byte instead of a bool.
func encodeBool(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// encodeBytes encodes a byte array to a byte array. Used primarily for message payloads.
func encodeBytes(val []byte) []byte {
	// In most circumstances the number of bytes being encoded is small.
	// Setting the cap to a low amount allows us to account for those without
	// triggering allocation growth on append unless we need to.
	buf := make([]byte, 2, 32)
	binary.BigEndian.PutUint16(buf, uint16(len(val)))
	return append(buf, val...)
}

// encodeUint16 encodes a uint16 value to a byte array.
func encodeUint16(val uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, val)
	return buf
}

// encodeUint32 encodes a uint16 value to a byte array.
func encodeUint32(val uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, val)
	return buf
}

// encodeString encodes a string to a byte array.
func encodeString(val string) []byte {
	// Like encodeBytes, we set the cap to a small number to avoid
	// triggering allocation growth on append unless we absolutely need to.
	buf := make([]byte, 2, 32)
	binary.BigEndian.PutUint16(buf, uint16(len(val)))
	return append(buf, []byte(val)...)
}

// encodeLength writes length bits for the header.
func encodeLength(b *bytes.Buffer, length int64) {
	// 1.5.5 Variable Byte Integer encode non-normative
	// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027
	for {
		eb := byte(length % 128)
		length /= 128
		if length > 0 {
			eb |= 0x80
		}
		b.WriteByte(eb)
		if length == 0 {
			break // [MQTT-1.5.5-1]
		}
	}
}

// DecodeLength reads a Variable Byte Integer.
//
// see 1.5.5 Variable Byte Integer decode non-normative
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027
//
// **The encoding must be the shortest one for the value**, which is
// [MQTT-1.5.5-1]: "The encoded value MUST use the minimum number of bytes
// necessary to represent the value." A longer encoding of the same number is
// a malformed packet and not a second spelling of it.
//
// One check for three rules. This function reads every Variable Byte
// Integer the broker decodes: the Remaining Length of any packet
// (Client.ReadFixedHeader), the property length of a CONNECT, PUBLISH or
// any other packet carrying properties, and the Subscription Identifier
// (Properties.Decode, both sites). The conformance suite probes three of
// those and names them separately; they were always one defect.
func DecodeLength(b io.ByteReader) (n, bu int, err error) {
	var multiplier uint32
	var value uint32
	var eb byte
	bu = 1
	for {
		eb, err = b.ReadByte()
		if err != nil {
			return 0, bu, err
		}

		value |= uint32(eb&127) << multiplier
		if value > 268435455 {
			return 0, bu, ErrMalformedVariableByteInteger
		}

		if (eb & 128) == 0 {
			break
		}

		// "The maximum number of bytes in the Remaining Length field is
		// four" (MQTT 5 section 1.5.5, 3.1.1 section 2.2.3). A fourth byte
		// with its continuation bit set promises a fifth, and the value
		// check above sees nothing wrong while the fifth and every
		// following byte is zero, so a run of 0x80 was read for as long as
		// it lasted.
		if bu == 4 {
			return 0, bu, ErrMalformedVariableByteInteger
		}

		multiplier += 7
		bu++
	}

	// A continuation byte promises that the next byte carries something.
	// The last byte is the one with its continuation bit clear, so when
	// more than one was used and that byte is zero, the bytes before it
	// promised a value they did not deliver: 0x80 0x00 is 0 written in two
	// bytes, 0x81 0x00 is 1 written in two. Both have a one-byte spelling
	// and are therefore malformed.
	if bu > 1 && eb == 0 {
		return 0, bu, ErrMalformedVariableByteInteger // [MQTT-1.5.5-1]
	}

	return int(value), bu, nil
}
