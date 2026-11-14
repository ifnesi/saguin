// Package packet encodes and decodes MQTT control packets and their shared
// wire-level building blocks.
// Encoders trust their callers; malformed packets the broker sends are caught
// by the strict test client in every test from outside (design.md, Testing),
// rather than checked here.
package packet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"
)

// Packet errors are separate from transport errors so a connection can apply
// MQTT's error handling only to bytes that arrived successfully.
var (
	ErrMalformed          = errors.New("malformed packet")
	ErrProtocol           = errors.New("protocol error")
	ErrTooLarge           = errors.New("packet too large")
	ErrCannotEncode       = errors.New("cannot encode packet")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
)

const (
	maxVarInt    = 268_435_455
	maxQoS       = 2
	packetIDSize = 2 // Packet Identifiers are Two Byte Integers.
)

// Version identifies the MQTT wire version used by a connection.
type Version byte

// Supported MQTT protocol levels.
const (
	V311 Version = 4
	V5   Version = 5
)

// Type is the four-bit MQTT Control Packet type.
type Type byte

// MQTT control packet types.
const (
	CONNECT Type = 1 + iota
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
	AUTH
)

// StringPair is the name and value of an MQTT UTF-8 String Pair.
type StringPair struct {
	Name  string
	Value string
}

// DecodeUint16 decodes a Two Byte Integer and returns the following bytes.
func DecodeUint16(src []byte) (value uint16, rest []byte, err error) {
	if len(src) < 2 {
		return 0, nil, ErrMalformed
	}
	return binary.BigEndian.Uint16(src), src[2:], nil
}

// AppendUint16 appends a Two Byte Integer to dst.
func AppendUint16(dst []byte, value uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, value)
}

// DecodeUint32 decodes a Four Byte Integer and returns the following bytes.
func DecodeUint32(src []byte) (value uint32, rest []byte, err error) {
	if len(src) < 4 {
		return 0, nil, ErrMalformed
	}
	return binary.BigEndian.Uint32(src), src[4:], nil
}

// AppendUint32 appends a Four Byte Integer to dst.
func AppendUint32(dst []byte, value uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, value)
}

// DecodeVarInt decodes a minimal MQTT Variable Byte Integer and returns the
// following bytes.
func DecodeVarInt(src []byte) (value uint32, rest []byte, err error) {
	wireValue, n := binary.Uvarint(src)
	if n <= 0 || n > 4 || wireValue > maxVarInt || n != varIntSize(uint32(wireValue)) {
		return 0, nil, ErrMalformed
	}
	return uint32(wireValue), src[n:], nil
}

// AppendVarInt appends a checked value using MQTT's Variable Byte Integer encoding.
func AppendVarInt(dst []byte, value uint32) ([]byte, error) {
	return binaryAppendVarInt(dst, value), nil
}

func varIntSize(value uint32) int {
	switch {
	case value < 1<<7:
		return 1
	case value < 1<<14:
		return 2
	case value < 1<<21:
		return 3
	default:
		return 4
	}
}

// DecodeString decodes an MQTT UTF-8 Encoded String. Go's UTF-8 validator
// also rejects overlong encodings, surrogate encodings, and values above the
// Unicode maximum.
func DecodeString(src []byte) (value string, rest []byte, err error) {
	data, rest, err := decodeStringBytes(src)
	if err != nil {
		return "", nil, ErrMalformed
	}
	return string(data), rest, nil
}

// decodeStringBytes checks an MQTT string without copying it. Property
// validation keeps the wire bytes rather than allocating a Go string.
func decodeStringBytes(src []byte) (value, rest []byte, err error) {
	data, rest, err := decodePrefixed(src)
	if err != nil || !validStringBytes(data) {
		return nil, nil, ErrMalformed
	}
	return data, rest, nil
}

// AppendString appends a checked MQTT UTF-8 Encoded String to dst.
func AppendString(dst []byte, value string) ([]byte, error) {
	return appendString(dst, value), nil
}

// appendString writes an already checked UTF-8 Encoded String.
func appendString(dst []byte, value string) []byte {
	dst = AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

// DecodeBinary decodes MQTT Binary Data and returns the following bytes.
func DecodeBinary(src []byte) (value, rest []byte, err error) {
	return decodePrefixed(src)
}

// AppendBinary appends checked MQTT Binary Data to dst.
func AppendBinary(dst, value []byte) ([]byte, error) {
	dst = AppendUint16(dst, uint16(len(value)))
	return append(dst, value...), nil
}

func decodePrefixed(src []byte) (value, rest []byte, err error) {
	length, rest, err := DecodeUint16(src)
	if err != nil || int(length) > len(rest) {
		return nil, nil, ErrMalformed
	}
	return rest[:length], rest[length:], nil
}

// DecodeStringPair decodes two consecutive MQTT UTF-8 Encoded Strings.
func DecodeStringPair(src []byte) (pair StringPair, rest []byte, err error) {
	name, rest, err := DecodeString(src)
	if err != nil {
		return StringPair{}, nil, err
	}
	value, rest, err := DecodeString(rest)
	if err != nil {
		return StringPair{}, nil, err
	}
	return StringPair{Name: name, Value: value}, rest, nil
}

// AppendStringPair appends a checked MQTT UTF-8 String Pair to dst.
func AppendStringPair(dst []byte, pair StringPair) ([]byte, error) {
	dst = appendString(dst, pair.Name)
	return appendString(dst, pair.Value), nil
}

func validString(value string) bool {
	return len(value) <= math.MaxUint16 && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

// validStringBytes checks an MQTT string kept as bytes without a temporary
// Go string. A non-escaping string conversion can still copy a long topic.
func validStringBytes(value []byte) bool {
	return len(value) <= math.MaxUint16 && utf8.Valid(value) && bytes.IndexByte(value, 0) < 0
}
