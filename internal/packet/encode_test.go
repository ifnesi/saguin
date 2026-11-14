package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The maintainer's decision after audit round 261010-07 (finding
// 261010-007a): an encoder refuses a value MQTT cannot carry, with an error
// of the broker's own, ErrCannotEncode, instead of writing a packet the
// decoders would refuse. ErrMalformed and ErrProtocol say what a peer sent;
// ErrCannotEncode says what the broker itself asked for.

// maxVarInt is the largest Variable Byte Integer: "268,435,455 (0xFF, 0xFF,
// 0xFF, 0x7F)" (MQTT 5 1.5.5 Table 1-1).
const maxVarInt = 268_435_455

// cannotEncode reports whether err is the encoders' refusal and none of the
// errors that describe a peer.
func cannotEncode(err error) bool {
	return errors.Is(err, packet.ErrCannotEncode) &&
		!errors.Is(err, packet.ErrMalformed) && !errors.Is(err, packet.ErrProtocol) && !errors.Is(err, packet.ErrTooLarge)
}

// encodable is the oracle for a set of properties in p: every identifier in
// Table 2-4 and allowed in p, none repeated where only one may appear, and
// every value within its type.
func encodable(p place, props []packet.Property) bool {
	seen := map[byte]bool{}
	for _, prop := range props {
		i, known := rowOf[prop.ID]
		if !known || !p.in(table24[i].where) || (seen[prop.ID] && !p.in(mayRepeat[prop.ID])) {
			return false
		}
		seen[prop.ID] = true
		switch table24[i].typ {
		case "Byte":
			if prop.Int > 255 {
				return false
			}
		case "Two Byte Integer":
			if prop.Int > 65_535 {
				return false
			}
		case "Variable Byte Integer":
			if prop.Int > maxVarInt {
				return false
			}
		case "UTF-8 Encoded String":
			if !testkit.UTF8StringOK(prop.String) {
				return false
			}
		case "Binary Data":
			if len(prop.Binary) > 65_535 {
				return false
			}
		case "UTF-8 String Pair":
			if !testkit.UTF8StringOK(prop.Pair.Name) || !testkit.UTF8StringOK(prop.Pair.Value) {
				return false
			}
		}
	}
	return true
}

// sameValue reports whether b holds a's identifier and the value of a's
// type: the one field the wire carries.
func sameValue(a, b *packet.Property) bool {
	if a.ID != b.ID {
		return false
	}
	switch table24[rowOf[a.ID]].typ {
	case "UTF-8 Encoded String":
		return a.String == b.String
	case "Binary Data":
		return bytes.Equal(a.Binary, b.Binary)
	case "UTF-8 String Pair":
		return a.Pair == b.Pair
	default:
		return a.Int == b.Int
	}
}

// wireOf writes props as a property section's bytes, by the types of Table
// 2-4 (MQTT 5 2.2.2.2), so a packet can carry them without the code under
// test writing its own input; an identifier the table lacks is written
// alone. It reports false when a value is too large for its type, which no
// wire can carry.
func wireOf(props []packet.Property) (packet.Properties, bool) {
	var w packet.Properties
	for _, prop := range props {
		w = append(w, prop.ID)
		i, known := rowOf[prop.ID]
		if !known {
			continue
		}
		switch table24[i].typ {
		case "Byte":
			if prop.Int > 255 {
				return nil, false
			}
			w = append(w, byte(prop.Int))
		case "Two Byte Integer":
			if prop.Int > 65_535 {
				return nil, false
			}
			w = binary.BigEndian.AppendUint16(w, uint16(prop.Int))
		case "Four Byte Integer":
			w = binary.BigEndian.AppendUint32(w, prop.Int)
		case "Variable Byte Integer":
			if prop.Int > maxVarInt {
				return nil, false
			}
			w = binary.AppendUvarint(w, uint64(prop.Int))
		case "UTF-8 Encoded String":
			if len(prop.String) > 65_535 {
				return nil, false
			}
			w = append(w, prefixed([]byte(prop.String))...)
		case "Binary Data":
			if len(prop.Binary) > 65_535 {
				return nil, false
			}
			w = append(w, prefixed(prop.Binary)...)
		case "UTF-8 String Pair":
			if len(prop.Pair.Name) > 65_535 || len(prop.Pair.Value) > 65_535 {
				return nil, false
			}
			w = append(w, prefixed([]byte(prop.Pair.Name))...)
			w = append(w, prefixed([]byte(prop.Pair.Value))...)
		}
	}
	return w, true
}

// valuesOf reads a property section, as a packet carries it, by the types
// of Table 2-4 (MQTT 5 2.2.2.2) without the code under test, so an oracle
// can judge what a Properties holds. It reports false unless the bytes are
// whole properties with identifiers the table lists, each integer a
// Variable Byte Integer in its fewest bytes (MQTT 5 1.5.5).
func valuesOf(raw packet.Properties) ([]packet.Property, bool) {
	var props []packet.Property
	b := []byte(raw)
	for len(b) > 0 {
		i, known := rowOf[b[0]]
		if !known {
			return nil, false
		}
		prop := packet.Property{ID: b[0]}
		b = b[1:]
		ok := true
		switch table24[i].typ {
		case "Byte":
			if ok = len(b) >= 1; ok {
				prop.Int, b = uint32(b[0]), b[1:]
			}
		case "Two Byte Integer":
			if ok = len(b) >= 2; ok {
				prop.Int, b = uint32(binary.BigEndian.Uint16(b)), b[2:]
			}
		case "Four Byte Integer":
			if ok = len(b) >= 4; ok {
				prop.Int, b = binary.BigEndian.Uint32(b), b[4:]
			}
		case "Variable Byte Integer":
			v, n := binary.Uvarint(b)
			if ok = n > 0 && n <= 4 && n == len(binary.AppendUvarint(nil, v)); ok {
				prop.Int, b = uint32(v), b[n:]
			}
		case "UTF-8 Encoded String":
			var s []byte
			if s, b, ok = prefixedField(b); ok {
				prop.String = string(s)
			}
		case "Binary Data":
			prop.Binary, b, ok = prefixedField(b)
		case "UTF-8 String Pair":
			var name, value []byte
			if name, b, ok = prefixedField(b); ok {
				if value, b, ok = prefixedField(b); ok {
					prop.Pair = packet.StringPair{Name: string(name), Value: string(value)}
				}
			}
		}
		if !ok {
			return nil, false
		}
		props = append(props, prop)
	}
	return props, true
}
