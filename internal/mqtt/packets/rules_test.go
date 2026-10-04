// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package packets

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rules a receiver must enforce about the FORMAT of a packet, MQTT 5
// and MQTT 3.1.1, each with the identifier the specification gives it.
//
// **Every oracle here is written from the specification** -
// docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html (section 2.2.2.2's
// property table, the "It is a Protocol Error" and "Malformed Packet"
// sentences, Appendix C) and .../v3.1.1/os/mqtt-v3.1.1-os.html (Appendix A) -
// and none from the decoder. A row says what the bytes are and what the
// specification makes of them.

// specProperty is one row of MQTT 5 section 2.2.2.2, Table 2-4, with what the
// property's own section adds: whether it may be repeated, and the values it
// may take.
type specProperty struct {
	id     byte
	name   string
	kind   string // the data type the table names
	pkts   []byte // the packet types the table lists; WillProperties for "Will Properties"
	repeat []byte // the packets that may carry it more than once
	ranged bool
	lo, hi uint32 // the values the property's section allows, when ranged
	code   byte   // the reason code for a value outside lo..hi
}

var anyPacket = []byte{Connect, Connack, Publish, Puback, Pubrec, Pubrel, Pubcomp,
	Subscribe, Suback, Unsubscribe, Unsuback, Pingreq, Pingresp, Disconnect, Auth, WillProperties}

// specProperties is Table 2-4 written out, with the section each constraint
// comes from beside it.
var specProperties = []specProperty{
	// 3.3.2.3.2 lists the values 0 and 1 and makes only a repeat a Protocol
	// Error. **A value of 2 or more is refused on section 1.2's definition**:
	// a Protocol Error is "found to contain data that is not allowed by the
	// protocol", and the section allows no other value. No sentence there
	// says so, and the specification names no reason code for it.
	{id: 0x01, name: "Payload Format Indicator", kind: "byte", pkts: []byte{Publish, WillProperties}, ranged: true, lo: 0, hi: 1, code: 0x82},
	{id: 0x02, name: "Message Expiry Interval", kind: "u32", pkts: []byte{Publish, WillProperties}},
	{id: 0x03, name: "Content Type", kind: "str", pkts: []byte{Publish, WillProperties}},
	{id: 0x08, name: "Response Topic", kind: "str", pkts: []byte{Publish, WillProperties}},
	{id: 0x09, name: "Correlation Data", kind: "bin", pkts: []byte{Publish, WillProperties}},
	// 3.3.2.3.8 and 3.8.2.1.2: "1 to 268,435,455"; more than once in a PUBLISH only.
	{id: 0x0B, name: "Subscription Identifier", kind: "vbi", pkts: []byte{Publish, Subscribe}, repeat: []byte{Publish}, ranged: true, lo: 1, hi: 268435455, code: 0x82},
	{id: 0x11, name: "Session Expiry Interval", kind: "u32", pkts: []byte{Connect, Connack, Disconnect}},
	{id: 0x12, name: "Assigned Client Identifier", kind: "str", pkts: []byte{Connack}},
	{id: 0x13, name: "Server Keep Alive", kind: "u16", pkts: []byte{Connack}},
	{id: 0x15, name: "Authentication Method", kind: "str", pkts: []byte{Connect, Connack, Auth}},
	{id: 0x16, name: "Authentication Data", kind: "bin", pkts: []byte{Connect, Connack, Auth}},
	// 3.1.2.11.7: "a value other than 0 or 1" is a Protocol Error.
	{id: 0x17, name: "Request Problem Information", kind: "byte", pkts: []byte{Connect}, ranged: true, lo: 0, hi: 1, code: 0x82},
	{id: 0x18, name: "Will Delay Interval", kind: "u32", pkts: []byte{WillProperties}},
	// 3.1.2.11.6: "a value other than 0 or 1".
	{id: 0x19, name: "Request Response Information", kind: "byte", pkts: []byte{Connect}, ranged: true, lo: 0, hi: 1, code: 0x82},
	{id: 0x1A, name: "Response Information", kind: "str", pkts: []byte{Connack}},
	{id: 0x1C, name: "Server Reference", kind: "str", pkts: []byte{Connack, Disconnect}},
	{id: 0x1F, name: "Reason String", kind: "str", pkts: []byte{Connack, Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback, Disconnect, Auth}},
	// 3.1.2.11.3 and 3.2.2.3.3: "or for it to have the value 0".
	{id: 0x21, name: "Receive Maximum", kind: "u16", pkts: []byte{Connect, Connack}, ranged: true, lo: 1, hi: math.MaxUint16, code: 0x82},
	{id: 0x22, name: "Topic Alias Maximum", kind: "u16", pkts: []byte{Connect, Connack}},
	// 3.3.4: "A Topic Alias value of 0 ... is a Protocol Error, the receiver uses
	// DISCONNECT with Reason Code of 0x94 (Topic Alias invalid)".
	{id: 0x23, name: "Topic Alias", kind: "u16", pkts: []byte{Publish}, ranged: true, lo: 1, hi: math.MaxUint16, code: 0x94},
	// 3.2.2.3.4: "a value other than 0 or 1".
	{id: 0x24, name: "Maximum QoS", kind: "byte", pkts: []byte{Connack}, ranged: true, lo: 0, hi: 1, code: 0x82},
	// 3.2.2.3.5: "a value other than 0 or 1".
	{id: 0x25, name: "Retain Available", kind: "byte", pkts: []byte{Connack}, ranged: true, lo: 0, hi: 1, code: 0x82},
	// 3.1.2.11.8: "allowed to appear multiple times".
	{id: 0x26, name: "User Property", kind: "pair", pkts: []byte{Connect, Connack, Publish, WillProperties, Puback, Pubrec, Pubrel, Pubcomp, Subscribe, Suback, Unsubscribe, Unsuback, Disconnect, Auth}, repeat: anyPacket},
	// 3.1.2.11.4 and 3.2.2.3.9: "or for the value to be set to zero".
	{id: 0x27, name: "Maximum Packet Size", kind: "u32", pkts: []byte{Connect, Connack}, ranged: true, lo: 1, hi: math.MaxUint32, code: 0x82},
	// 3.2.2.3.11, .12, .13: "a value other than 0 or 1".
	{id: 0x28, name: "Wildcard Subscription Available", kind: "byte", pkts: []byte{Connack}, ranged: true, lo: 0, hi: 1, code: 0x82},
	{id: 0x29, name: "Subscription Identifier Available", kind: "byte", pkts: []byte{Connack}, ranged: true, lo: 0, hi: 1, code: 0x82},
	{id: 0x2A, name: "Shared Subscription Available", kind: "byte", pkts: []byte{Connack}, ranged: true, lo: 0, hi: 1, code: 0x82},
}

func contains(s []byte, b byte) bool { return bytes.IndexByte(s, b) >= 0 }

// codeOf is the reason code a decoder's error carries, found the way the
// server finds it.
func codeOf(err error) (byte, bool) {
	var c Code
	if errors.As(err, &c) {
		return c.Code, true
	}
	return 0, false
}

// encodeValue writes a value in the data type the specification gives it.
func encodeValue(kind string, v uint32) []byte {
	switch kind {
	case "byte":
		return []byte{byte(v)}
	case "u16":
		return []byte{byte(v >> 8), byte(v)}
	case "u32":
		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case "vbi":
		b := new(bytes.Buffer)
		encodeLength(b, int64(v))
		return b.Bytes()
	case "str", "bin":
		return []byte{0, 1, 'a'}
	case "pair":
		return []byte{0, 1, 'a', 0, 1, 'b'}
	}
	panic("unknown kind " + kind)
}

// block is one property written as the wire has it.
func (s specProperty) block(v uint32) []byte {
	return append([]byte{s.id}, encodeValue(s.kind, v)...)
}

// goodValue is a value the property's own section allows.
func (s specProperty) goodValue() uint32 {
	if s.ranged {
		return s.lo
	}
	return 1
}

// props prefixes a Property Length.
func props(blocks ...[]byte) []byte {
	body := bytes.Join(blocks, nil)
	return append(vbi(len(body)), body...)
}

// context is what a property needs beside it to be a whole property set:
// section 3.1.2.11.10 makes Authentication Data without an Authentication
// Method a Protocol Error, and section 3.15.2.2.2 makes an AUTH packet
// without a method one.
func context(pkt, id byte) [][]byte {
	method := specProperties[9].block(0)
	if (pkt == Auth && id != 0x15) || (pkt == Connect && id == 0x16) {
		return [][]byte{method}
	}
	return nil
}

func decodeProps(pkt byte, block []byte) (Properties, int, error) {
	var p Properties
	n, err := p.Decode(pkt, bytes.NewBuffer(block))
	return p, n, err
}

// TestPropertyTableIsTheSpecification walks section 2.2.2.2 as written above
// and fails when the table lacks a property, has one the specification does
// not, or differs in the packets it allows, in whether it may repeat, or in the
// values it allows.
func TestPropertyTableIsTheSpecification(t *testing.T) {
	require.Len(t, specProperties, 27, "section 2.2.2.2 lists 27 properties")

	listed := 0
	for id := range validPacketProperties {
		if validPacketProperties[id].on != 0 {
			listed++
		}
	}
	require.Equal(t, len(specProperties), listed, "the table holds a property the specification does not list")

	examined := 0
	for _, s := range specProperties {
		r := validPacketProperties[s.id]
		require.NotZero(t, r.on, "the table lacks 0x%02x %s", s.id, s.name)
		require.Equal(t, s.name, r.name, "0x%02x", s.id)
		require.Equal(t, s.kind, r.kind.String(), "%s data type", s.name)
		for pkt := byte(1); pkt <= WillProperties; pkt++ {
			if pkt > Auth && pkt != WillProperties {
				continue
			}
			examined++
			require.Equal(t, contains(s.pkts, pkt), r.on&pktBit(pkt) != 0,
				"%s on packet type %d", s.name, pkt)
			require.Equal(t, contains(s.repeat, pkt), r.repeat&pktBit(pkt) != 0,
				"%s may repeat on packet type %d", s.name, pkt)
		}
		require.Equal(t, s.ranged, r.ranged, "%s has a value range", s.name)
		if s.ranged {
			require.Equal(t, s.lo, r.lo, "%s lowest value", s.name)
			require.Equal(t, s.hi, r.hi, "%s highest value", s.name)
			require.Equal(t, s.code, r.bad.Code, "%s reason code", s.name)
		}
	}
	require.Equal(t, 27*16, examined)
}

// TestPropertyRulesOnEveryPacketType puts each property on each packet type,
// alone, twice, and at the edges of its range, and checks what the
// specification makes of each. It counts what it examined.
func TestPropertyRulesOnEveryPacketType(t *testing.T) {
	accepted, repeats, values, wrongType, unknown := 0, 0, 0, 0, 0

	for _, s := range specProperties {
		for _, pkt := range anyPacket {
			ctx := context(pkt, s.id)
			one := props(append(append([][]byte{}, ctx...), s.block(s.goodValue()))...)
			_, _, err := decodeProps(pkt, one)
			if !contains(s.pkts, pkt) {
				wrongType++
				code, ok := codeOf(err)
				require.True(t, ok && code == 0x81, "%s on packet type %d is a Malformed Packet (section 2.2.2.2), got %v", s.name, pkt, err)
				continue
			}
			accepted++
			require.NoError(t, err, "%s on packet type %d", s.name, pkt)

			// A second copy.
			two := props(append(append([][]byte{}, ctx...), s.block(s.goodValue()), s.block(s.goodValue()))...)
			_, _, err = decodeProps(pkt, two)
			repeats++
			if contains(s.repeat, pkt) {
				require.NoError(t, err, "%s may repeat on packet type %d", s.name, pkt)
			} else {
				code, ok := codeOf(err)
				require.True(t, ok && code == 0x82, "%s twice on packet type %d is a Protocol Error, got %v", s.name, pkt, err)
			}

			if !s.ranged {
				continue
			}
			// The edges: lo and hi are accepted, one outside is refused.
			for _, v := range []uint32{s.lo, s.hi} {
				values++
				_, _, err = decodeProps(pkt, props(append(append([][]byte{}, ctx...), s.block(v))...))
				require.NoError(t, err, "%s = %d on packet type %d", s.name, v, pkt)
			}
			var outside []uint32
			if s.lo > 0 {
				outside = append(outside, s.lo-1)
			}
			if s.hi < math.MaxUint32 {
				outside = append(outside, s.hi+1)
			}
			for _, v := range outside {
				if s.kind == "u16" && v > math.MaxUint16 || s.kind == "vbi" && v > 268435455 {
					continue // not writable in the type
				}
				if s.kind == "byte" && v > 0xFF {
					continue
				}
				values++
				_, _, err = decodeProps(pkt, props(append(append([][]byte{}, ctx...), s.block(v))...))
				code, ok := codeOf(err)
				require.True(t, ok && code == s.code, "%s = %d on packet type %d is refused with 0x%02x, got %v", s.name, v, pkt, s.code, err)
			}
			// A byte-valued property holding 0x30 is the saved crasher's shape.
			if s.kind == "byte" && s.ranged {
				values++
				_, _, err = decodeProps(pkt, props(append(append([][]byte{}, ctx...), s.block(0x30))...))
				code, ok := codeOf(err)
				require.True(t, ok && code == s.code, "%s = 0x30 on packet type %d, got %v", s.name, pkt, err)
			}
		}
	}

	// Identifiers the table does not list: Malformed Packet on every type.
	for id := 0; id < 256; id++ {
		known := false
		for _, s := range specProperties {
			known = known || int(s.id) == id
		}
		if known {
			continue
		}
		for _, pkt := range anyPacket {
			unknown++
			_, _, err := decodeProps(pkt, props([]byte{byte(id), 0, 0, 0, 0, 0}))
			code, ok := codeOf(err)
			require.True(t, ok && code == 0x81, "identifier 0x%02x on packet type %d is a Malformed Packet, got %v", id, pkt, err)
		}
	}

	// Every sweep ran: 27 properties x 16 packet types, split by what the table says.
	require.Equal(t, 27*16, accepted+wrongType)
	require.Equal(t, 229*16, unknown)
	require.Positive(t, repeats)
	require.Positive(t, values)
	t.Logf("examined: %d allowed placements, %d wrong-type placements, %d repeats, %d values, %d unknown identifiers",
		accepted, wrongType, repeats, values, unknown)
}

// TestPropertyOverrunningItsBlock: section 2.2.2.1 makes the Property Length
// the length of the properties, so one that runs past it cannot be parsed.
func TestPropertyOverrunningItsBlock(t *testing.T) {
	// Length 3, holding the identifier of a Four Byte Integer and two bytes of
	// it; the other two are beyond the block, where a payload begins.
	block := []byte{3, 0x02, 0, 0, 0, 0}
	_, _, err := decodeProps(Publish, block)
	code, ok := codeOf(err)
	require.True(t, ok && code == 0x81, "got %v", err)

	// A Property Length longer than what follows it.
	_, _, err = decodeProps(Publish, []byte{9, 0x02, 0, 0, 0, 0})
	code, ok = codeOf(err)
	require.True(t, ok && code == 0x81, "got %v", err)
}

// TestAuthenticationMethodRules: sections 3.1.2.11.10 and 3.15.2.2.2.
func TestAuthenticationMethodRules(t *testing.T) {
	data := specProperties[10].block(0)
	method := specProperties[9].block(0)

	_, _, err := decodeProps(Connect, props(data))
	code, ok := codeOf(err)
	require.True(t, ok && code == 0x82, "Authentication Data without a method on CONNECT: %v", err)
	_, _, err = decodeProps(Connect, props(method, data))
	require.NoError(t, err)
	_, _, err = decodeProps(Auth, props())
	code, ok = codeOf(err)
	require.True(t, ok && code == 0x82, "AUTH without a method: %v", err)
	_, _, err = decodeProps(Auth, props(data))
	code, ok = codeOf(err)
	require.True(t, ok && code == 0x82, "AUTH with data and no method: %v", err)
	_, _, err = decodeProps(Auth, props(method))
	require.NoError(t, err)
	// CONNACK may carry either alone.
	_, _, err = decodeProps(Connack, props(data))
	require.NoError(t, err)
}

// ---- Wire-level rules -------------------------------------------------

func vbi(n int) []byte {
	b := new(bytes.Buffer)
	encodeLength(b, int64(n))
	return b.Bytes()
}

func str(s string) []byte { return append([]byte{byte(len(s) >> 8), byte(len(s))}, []byte(s)...) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// wire is a whole packet: the first byte, the Remaining Length, the body.
func wire(first byte, body ...[]byte) []byte {
	b := cat(body...)
	return cat([]byte{first}, vbi(len(b)), b)
}

const (
	fUser   = 0x80
	fPass   = 0x40
	fRetain = 0x20
	fWill   = 0x04
	fClean  = 0x02
)

func fQos(q byte) byte { return q << 3 }

// connectWire is a CONNECT. props is the v5 Properties, without their
// length; a v5 packet always carries a Property Length.
func connectWire(ver, flags byte, props5 []byte, payload ...[]byte) []byte {
	body := cat([]byte{0, 4, 'M', 'Q', 'T', 'T', ver, flags, 0, 60})
	if ver == 5 {
		body = cat(body, vbi(len(props5)), props5)
	}
	return wire(Connect<<4, body, cat(payload...))
}

// willProps is a v5 Will Properties block, empty.
var noProps = []byte{0}

type wireRule struct {
	id   string // the identifiers of the specification, v5 first
	desc string
	ver  byte
	data []byte
	want int // ok, any, or the reason code the specification names (MQTT 5 only: 3.1.1 has none)
}

const (
	ok  = -1 // the packet is accepted
	any = -2 // refused, and the specification names no reason code
)

// wireRules is every row of the format rules. A row is a packet the
// specification says is accepted, or one it says is refused with that reason
// code. The rows that accept are the neighbours of the ones that refuse:
// without them a decoder that refuses everything would pass.
func wireRules() []wireRule {
	var r []wireRule
	add := func(id, desc string, ver byte, data []byte, want int) {
		r = append(r, wireRule{id, desc, ver, data, want})
	}
	for _, ver := range []byte{4, 5} {
		p5 := func(b ...byte) []byte {
			if ver == 5 {
				return b
			}
			return nil
		}
		wp := func() []byte { // Will Properties, v5 only
			if ver == 5 {
				return noProps
			}
			return nil
		}
		id := str("c1")

		// -- CONNECT
		add("MQTT-3.1.2-3 / 3.1.2-3", "reserved CONNECT flag set", ver, connectWire(ver, 0x03, nil, id), 0x81)
		add("(none)", "a plain CONNECT is accepted", ver, connectWire(ver, fClean, nil, id), ok)
		add("MQTT-3.1.2-14 / 3.1.2.6", "Will QoS 3 with the Will Flag set", ver,
			connectWire(ver, fWill|fQos(3)|fClean, nil, id, wp(), str("w/t"), str("hi")), 0x81)
		add("MQTT-3.1.2-14 / 3.1.2.6", "Will QoS 3 with the Will Flag clear", ver,
			connectWire(ver, fQos(3)|fClean, nil, id), 0x81)
		add("MQTT-3.1.2-11 / 3.1.2-13", "Will QoS 1 with the Will Flag clear", ver,
			connectWire(ver, fQos(1)|fClean, nil, id), any)
		add("MQTT-3.1.2-11 / 3.1.2-13", "Will QoS 2 with the Will Flag clear", ver,
			connectWire(ver, fQos(2)|fClean, nil, id), any)
		add("MQTT-3.1.2-12 / 3.1.2-14", "Will QoS 2 with the Will Flag set", ver,
			connectWire(ver, fWill|fQos(2)|fClean, nil, id, wp(), str("w/t"), str("hi")), ok)
		add("MQTT-3.1.2-13 / 3.1.2-15", "Will Retain with the Will Flag clear", ver,
			connectWire(ver, fRetain|fClean, nil, id), any)
		add("MQTT-3.1.2-9 / 3.1.2-9", "Will Flag set and no Will Topic or Message", ver,
			connectWire(ver, fWill|fClean, nil, id), any)
		add("MQTT-3.1.2-9", "Will Flag set and no Will Message", ver,
			connectWire(ver, fWill|fClean, nil, id, wp(), str("w/t")), any)
		add("MQTT-3.1.2-9", "an empty Will Message is a Will Message", ver,
			connectWire(ver, fWill|fClean, nil, id, wp(), str("w/t"), str("")), ok)
		add("MQTT-3.1.2-16 / 3.1.2-18", "a User Name with the User Name Flag clear", ver,
			connectWire(ver, fClean, nil, id, str("u")), any)
		add("MQTT-3.1.2-17 / 3.1.2-19", "the User Name Flag set and no User Name", ver,
			connectWire(ver, fUser|fClean, nil, id), any)
		add("MQTT-3.1.2-18 / 3.1.2-20", "a Password with the Password Flag clear", ver,
			connectWire(ver, fUser|fClean, nil, id, str("u"), str("p")), any)
		add("MQTT-3.1.2-19 / 3.1.2-21", "the Password Flag set and no Password", ver,
			connectWire(ver, fUser|fPass|fClean, nil, id, str("u")), any)
		add("MQTT-3.1.2-16 / 3.1.2-19", "user name and password", ver,
			connectWire(ver, fUser|fPass|fClean, nil, id, str("u"), str("p")), ok)
		if ver == 4 {
			add("(MQTT 3.1.1) MQTT-3.1.2-22", "Password Flag set with the User Name Flag clear", ver,
				connectWire(ver, fPass|fClean, nil, id, str("p")), any)
		} else {
			add("MQTT 5 3.1.2.9", "Password Flag set with the User Name Flag clear is allowed in MQTT 5", ver,
				connectWire(ver, fPass|fClean, nil, id, str("p")), ok)
		}
		add("MQTT-3.1.3-1 / 3.1.3-1", "bytes after the last field the flags describe", ver,
			connectWire(ver, fClean, nil, id, []byte{1}), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "invalid UTF-8 in the Client Identifier", ver,
			connectWire(ver, fClean, nil, cat([]byte{0, 2, 0xC3, 0x28})), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "a surrogate (U+D800) in the Client Identifier", ver,
			connectWire(ver, fClean, nil, cat([]byte{0, 3, 0xED, 0xA0, 0x80})), any)
		add("MQTT-1.5.4-2 / 1.5.3-2", "U+0000 in the Client Identifier", ver,
			connectWire(ver, fClean, nil, cat([]byte{0, 2, 'a', 0})), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "invalid UTF-8 in the Will Topic", ver,
			connectWire(ver, fWill|fClean, nil, id, wp(), cat([]byte{0, 2, 0xC3, 0x28}), str("hi")), 0x81)
		add("MQTT-1.5.4-2 / 1.5.3-2", "U+0000 in the User Name", ver,
			connectWire(ver, fUser|fClean, nil, id, cat([]byte{0, 2, 'a', 0})), 0x81)
		add("MQTT-3.1.2-1 / 3.1.2-1", "a protocol name that is not MQTT", ver,
			wire(Connect<<4, []byte{0, 4, 'M', 'Q', 'T', 'X', ver, fClean, 0, 60}, p5(0), id), any)
		add("MQTT-3.1.2-2 / 3.1.2-2", "an unsupported protocol version", ver,
			wire(Connect<<4, []byte{0, 4, 'M', 'Q', 'T', 'T', 9, fClean, 0, 60}, id), any)

		// -- PUBLISH
		pub := func(first byte, topic string, pid []byte, p5b []byte, payload string) []byte {
			body := cat(str(topic), pid)
			if ver == 5 {
				body = cat(body, vbi(len(p5b)), p5b)
			}
			return wire(first, body, []byte(payload))
		}
		add("(none)", "a plain QoS 1 PUBLISH is accepted", ver, pub(0x32, "a/b", []byte{0, 7}, nil, "x"), ok)
		add("MQTT-3.3.1-4 / 3.3.1-4", "a PUBLISH with QoS 3", ver, pub(0x36, "a/b", []byte{0, 7}, nil, "x"), 0x81)
		add("MQTT-3.3.1-2 / 3.3.1-2", "DUP set on a QoS 0 PUBLISH", ver, pub(0x38, "a/b", nil, nil, "x"), 0x82)
		add("MQTT-3.3.2-2 / 3.3.2-2", "a + in a PUBLISH topic", ver, pub(0x30, "a/+", nil, nil, "x"), any)
		add("MQTT-3.3.2-2 / 3.3.2-2", "a # in a PUBLISH topic", ver, pub(0x30, "a/#", nil, nil, "x"), 0x82)
		add("MQTT-2.2.1-3 / 2.3.1-1", "packet identifier 0 at QoS 1", ver, pub(0x32, "a/b", []byte{0, 0}, nil, "x"), any)
		add("MQTT-2.2.1-3 / 2.3.1-1", "packet identifier 0 at QoS 2", ver, pub(0x34, "a/b", []byte{0, 0}, nil, "x"), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "invalid UTF-8 in a PUBLISH topic", ver,
			wire(0x30, []byte{0, 2, 0xFF, 0xFE}, p5(0)), 0x81)
		add("MQTT-1.5.4-2 / 1.5.3-2", "U+0000 in a PUBLISH topic", ver,
			wire(0x30, []byte{0, 2, 'a', 0}, p5(0)), 0x81)
		add("MQTT-4.7.3-1 / 3.3.2.1", "an empty topic and no Topic Alias", ver, pub(0x30, "", nil, nil, "x"), 0x82)
		add("(none)", "a PUBLISH too short for its packet identifier", ver,
			wire(0x32, str("a")), 0x81)
		if ver == 5 {
			pfi := func(v byte) []byte { return cat([]byte{0x01, v}) }
			add("3.3.2.3.2", "Payload Format Indicator 1", ver, pub(0x30, "a", nil, pfi(1), "x"), ok)
			add("1.2 Protocol Error; 3.3.2.3.2 (saved fuzz crasher)", "Payload Format Indicator 0x30", ver, pub(0x30, "a", nil, pfi(0x30), "x"), 0x82)
			add("3.3.2.3.2", "Payload Format Indicator twice", ver, pub(0x30, "a", nil, cat(pfi(0), pfi(1)), "x"), 0x82)
			add("3.3.2.3.4 / MQTT-3.3.2-8", "Topic Alias 0", ver, pub(0x30, "a", nil, []byte{0x23, 0, 0}, "x"), 0x94)
			add("3.3.2.3.4", "Topic Alias twice", ver, pub(0x30, "a", nil, []byte{0x23, 0, 1, 0x23, 0, 2}, "x"), 0x82)
			add("MQTT-3.3.2-9 / 3.3.2-12", "Topic Alias above the Topic Alias Maximum", ver,
				pub(0x30, "a", nil, []byte{0x23, 0, saguinTopicAliasMaximum + 1}, "x"), 0x94)
			add("MQTT-3.3.2-12", "Topic Alias at the Topic Alias Maximum", ver,
				pub(0x30, "a", nil, []byte{0x23, 0, saguinTopicAliasMaximum}, "x"), ok)
			add("3.3.2.1", "an empty topic with a Topic Alias", ver, pub(0x30, "", nil, []byte{0x23, 0, 1}, "x"), ok)
			add("MQTT-3.3.4-6", "a Subscription Identifier in a PUBLISH from a client", ver, pub(0x30, "a", nil, []byte{0x0B, 1}, "x"), 0x82)
			add("3.3.2.3.8", "Subscription Identifier 0", ver, pub(0x30, "a", nil, []byte{0x0B, 0}, "x"), 0x82)
			add("MQTT-3.3.2-14", "a wildcard in the Response Topic", ver,
				pub(0x30, "a", nil, cat([]byte{0x08}, str("r/+")), "x"), 0x82)
			add("2.2.2.2", "a property not valid for PUBLISH (Server Keep Alive)", ver,
				pub(0x30, "a", nil, []byte{0x13, 0, 5}, "x"), 0x81)
			add("2.2.2.2", "an unknown property identifier", ver, pub(0x30, "a", nil, []byte{0x7F, 0}, "x"), 0x81)
			add("2.2.2.2 / 2.2.2.1", "a property that runs past the Property Length", ver,
				wire(0x30, str("a"), []byte{1, 0x02, 0, 0, 0, 5}), 0x81)
			add("2.2.2.1", "a Property Length longer than the packet", ver,
				wire(0x30, str("a"), []byte{9, 0x01, 0}), 0x81)
			add("MQTT-1.5.7-1", "invalid UTF-8 in a User Property", ver,
				pub(0x30, "a", nil, cat([]byte{0x26}, str("k"), []byte{0, 1, 0xFF}), "x"), 0x81)
			add("(none)", "User Properties may repeat", ver,
				pub(0x30, "a", nil, cat([]byte{0x26}, str("k"), str("v"), []byte{0x26}, str("k"), str("v")), "x"), ok)
		}

		// -- SUBSCRIBE and UNSUBSCRIBE
		sub := func(first byte, pid []byte, p5b []byte, tail ...[]byte) []byte {
			body := cat(pid)
			if ver == 5 {
				body = cat(body, vbi(len(p5b)), p5b)
			}
			return wire(first, body, cat(tail...))
		}
		add("(none)", "a plain SUBSCRIBE is accepted", ver, sub(0x82, []byte{0, 1}, nil, str("a/#"), []byte{1}), ok)
		add("MQTT-3.8.3-2 / 3.8.3-3", "a SUBSCRIBE with no filter", ver, sub(0x82, []byte{0, 1}, nil), 0x82)
		add("MQTT-2.2.1-3 / 2.3.1-1", "a SUBSCRIBE with packet identifier 0", ver, sub(0x82, []byte{0, 0}, nil, str("a"), []byte{0}), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "invalid UTF-8 in a topic filter", ver,
			sub(0x82, []byte{0, 1}, nil, []byte{0, 2, 0xC3, 0x28}, []byte{0}), 0x81)
		add("(none)", "a filter with no options byte", ver, sub(0x82, []byte{0, 1}, nil, str("a")), 0x81)
		add("3.8.3.1 / MQTT-3-8.3-4", "requested QoS 3", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{3}), 0x82)
		if ver == 4 {
			add("MQTT-3-8.3-4", "a reserved bit set in the requested QoS byte", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{4}), 0x82)
		} else {
			add("MQTT-3.8.3-5", "reserved bit 6 of the Subscription Options", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{0x40}), 0x81)
			add("MQTT-3.8.3-5", "reserved bit 7 of the Subscription Options", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{0x80}), 0x81)
			add("3.8.3.1", "Retain Handling 3", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{0x30}), 0x82)
			add("3.8.3.1", "every legal option byte", ver, sub(0x82, []byte{0, 1}, nil, str("a"), []byte{0x2E}), ok)
			add("3.8.2.1.2", "Subscription Identifier 0", ver, sub(0x82, []byte{0, 1}, []byte{0x0B, 0}, str("a"), []byte{0}), 0x82)
			add("3.8.2.1.2", "Subscription Identifier twice", ver, sub(0x82, []byte{0, 1}, []byte{0x0B, 1, 0x0B, 2}, str("a"), []byte{0}), 0x82)
			add("3.8.2.1.2", "Subscription Identifier once", ver, sub(0x82, []byte{0, 1}, []byte{0x0B, 1}, str("a"), []byte{0}), ok)
		}
		add("MQTT-3.8.1-1 / 3.8.1-1", "SUBSCRIBE with fixed header flags 0000", ver, sub(0x80, []byte{0, 1}, nil, str("a"), []byte{0}), 0x81)
		add("(none)", "a plain UNSUBSCRIBE is accepted", ver, sub(0xA2, []byte{0, 1}, nil, str("a")), ok)
		add("MQTT-3.10.3-2 / 3.10.3-2", "an UNSUBSCRIBE with no filter", ver, sub(0xA2, []byte{0, 1}, nil), 0x82)
		add("MQTT-2.2.1-3 / 2.3.1-1", "an UNSUBSCRIBE with packet identifier 0", ver, sub(0xA2, []byte{0, 0}, nil, str("a")), any)
		add("MQTT-1.5.4-1 / 1.5.3-1", "invalid UTF-8 in an UNSUBSCRIBE filter", ver, sub(0xA2, []byte{0, 1}, nil, []byte{0, 2, 0xC3, 0x28}), 0x81)
		add("MQTT-3.10.1-1 / 3.10.1-1", "UNSUBSCRIBE with fixed header flags 0000", ver, sub(0xA0, []byte{0, 1}, nil, str("a")), 0x81)

		// -- PUBACK, PUBREC, PUBREL, PUBCOMP
		for _, c := range []struct {
			first byte
			name  string
			id    string
		}{{0x40, "PUBACK", "2.1.3-1"}, {0x50, "PUBREC", "2.1.3-1"}, {0x62, "PUBREL", "MQTT-3.6.1-1"}, {0x70, "PUBCOMP", "2.1.3-1"}} {
			add("(none)", "a plain "+c.name+" is accepted", ver, wire(c.first, []byte{0, 5}), ok)
			add("(none)", c.name+" with no packet identifier", ver, wire(c.first, []byte{0}), 0x81)
			if ver == 4 {
				add("2.1.4", c.name+" with a byte after the packet identifier", ver, wire(c.first, []byte{0, 5, 0}), any)
			}
			if ver == 5 {
				add("(none)", c.name+" with a reason code is accepted", ver, wire(c.first, []byte{0, 5, 0x10}), ok)
				add("2.2.2.2", c.name+" with a property not valid for it", ver, wire(c.first, []byte{0, 5, 0x80, 2, 0x13, 0}), 0x81)
				add("2.2.2.2", c.name+" with Reason String twice", ver,
					wire(c.first, []byte{0, 5, 0, 8}, []byte{0x1F}, str("a"), []byte{0x1F}, str("b")), 0x82)
				add("2.1.4", c.name+" with a byte after its properties", ver, wire(c.first, []byte{0, 5, 0, 0, 0}), 0x81)
			}
		}

		// -- PINGREQ, DISCONNECT, CONNACK
		add("(none)", "a PINGREQ is accepted", ver, wire(0xC0), ok)
		add("3.12.1", "a PINGREQ with a body", ver, wire(0xC0, []byte{1}), 0x81)
		add("(none)", "a PINGRESP is accepted", ver, wire(0xD0), ok)
		add("3.13.1", "a PINGRESP with a body", ver, wire(0xD0, []byte{1}), 0x81)
		add("(none)", "a DISCONNECT with no body is accepted", ver, wire(0xE0), ok)
		if ver == 4 {
			add("(MQTT 3.1.1) 3.14.1", "a DISCONNECT with a body", ver, wire(0xE0, []byte{0}), 0x81)
			// With an Authentication Method, so that only the packet type is wrong:
			// without one it is refused for that too, and a decoder that stopped
			// refusing the type would pass.
			add("(MQTT 3.1.1) 2.2.1", "an AUTH packet (reserved, forbidden)", ver,
				wire(0xF0, []byte{0x18, 4, 0x15}, str("m")), 0x82)
			add("(none)", "a CONNACK is accepted", ver, wire(0x20, []byte{0, 0}), ok)
			add("(MQTT 3.1.1) 3.2.2.1", "CONNACK with a body longer than two bytes", ver, wire(0x20, []byte{0, 0, 0}), 0x81)
		} else {
			add("3.14.2.2.2", "DISCONNECT with Session Expiry Interval twice", ver,
				wire(0xE0, []byte{0, 10, 0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2}), 0x82)
			add("2.2.2.2", "DISCONNECT with a property not valid for it", ver,
				wire(0xE0, []byte{0, 4, 0x12, 0, 1, 'a'}), 0x81)
			add("2.1.4", "DISCONNECT with a byte after its properties", ver, wire(0xE0, []byte{0, 0, 0}), 0x81)
			add("MQTT-3.2.2-1", "CONNACK with a reserved acknowledge flag set", ver, wire(0x20, []byte{2, 0, 0}), 0x81)
			add("(none)", "a CONNACK is accepted", ver, wire(0x20, []byte{1, 0, 0}), ok)
			add("2.1.4", "CONNACK with a byte after its properties", ver, wire(0x20, []byte{0, 0, 0, 0}), 0x81)
			add("3.15.2.1", "an AUTH with Remaining Length 0 is Success with no properties", ver, wire(0xF0), ok)
			add("3.15.2.2.2", "an AUTH with no Authentication Method", ver, wire(0xF0, []byte{0x18, 0}), 0x82)
			add("3.15.2.2.2", "an AUTH with an Authentication Method", ver, wire(0xF0, []byte{0x18, 4, 0x15}, str("m")), ok)
			add("3.15.2.2.2", "an AUTH with Authentication Method twice", ver,
				wire(0xF0, []byte{0x18, 8, 0x15}, str("m"), []byte{0x15}, str("m")), 0x82)
			add("MQTT-3.15.2-1", "an AUTH with a reason code that is not an Authenticate Reason Code", ver,
				wire(0xF0, []byte{0x10, 4, 0x15}, str("m")), 0x82)
			add("MQTT-3.15.1-1", "AUTH with fixed header flags 0001", ver, wire(0xF1, []byte{0x18, 0}), 0x81)
			add("3.1.2.11.3", "CONNECT with Receive Maximum 0", ver,
				connectWire(ver, fClean, []byte{0x21, 0, 0}, id), 0x82)
			add("3.1.2.11.3", "CONNECT with Receive Maximum twice", ver,
				connectWire(ver, fClean, []byte{0x21, 0, 1, 0x21, 0, 1}, id), 0x82)
			add("3.1.2.11.4", "CONNECT with Maximum Packet Size 0", ver,
				connectWire(ver, fClean, []byte{0x27, 0, 0, 0, 0}, id), 0x82)
			add("3.1.2.11.4", "CONNECT with Maximum Packet Size twice", ver,
				connectWire(ver, fClean, []byte{0x27, 0, 0, 0, 9, 0x27, 0, 0, 0, 9}, id), 0x82)
			add("3.1.2.11.2", "CONNECT with Session Expiry Interval twice", ver,
				connectWire(ver, fClean, []byte{0x11, 0, 0, 0, 9, 0x11, 0, 0, 0, 9}, id), 0x82)
			add("3.1.2.11.5", "CONNECT with Topic Alias Maximum twice", ver,
				connectWire(ver, fClean, []byte{0x22, 0, 1, 0x22, 0, 1}, id), 0x82)
			add("3.1.2.11.6", "CONNECT with Request Response Information 2", ver,
				connectWire(ver, fClean, []byte{0x19, 2}, id), 0x82)
			add("3.1.2.11.7", "CONNECT with Request Problem Information 2", ver,
				connectWire(ver, fClean, []byte{0x17, 2}, id), 0x82)
			add("3.1.2.11.10", "CONNECT with Authentication Data and no Authentication Method", ver,
				connectWire(ver, fClean, cat([]byte{0x16}, str("d")), id), 0x82)
			add("3.1.3.2.2", "Will Delay Interval twice in the Will Properties", ver,
				connectWire(ver, fWill|fClean, nil, id, props([]byte{0x18, 0, 0, 0, 1, 0x18, 0, 0, 0, 2}), str("w"), str("m")), 0x82)
			add("2.2.2.2", "Will Properties holding a property not valid for them (Session Expiry Interval)", ver,
				connectWire(ver, fWill|fClean, nil, id, props([]byte{0x11, 0, 0, 0, 1}), str("w"), str("m")), 0x81)
			add("3.1.3.2.2", "Will Properties with a Will Delay Interval", ver,
				connectWire(ver, fWill|fClean, nil, id, props([]byte{0x18, 0, 0, 0, 1}), str("w"), str("m")), ok)
		}
	}
	return r
}

// TestWireRules runs every row through the same steps the server takes: the
// header byte, the Remaining Length, the decoder for the type, and the
// validator for it.
func TestWireRules(t *testing.T) {
	rules := wireRules()
	refusals, acceptances := 0, 0
	for _, r := range rules {
		t.Run(fmt.Sprintf("v%d %s [%s]", r.ver, r.desc, r.id), func(t *testing.T) {
			want := r.want
			if r.ver == 4 && want != ok {
				want = any // MQTT 3.1.1 has no reason codes: the connection is closed
			}
			_, err := decodeWire(r.data, r.ver)
			if want == ok {
				acceptances++
				require.NoError(t, err, "wire % x", r.data)
				return
			}
			refusals++
			require.Error(t, err, "wire % x was accepted; the specification refuses it", r.data)
			if want == any {
				return
			}
			code, isCode := codeOf(err)
			require.True(t, isCode, "wire % x: error %v carries no reason code", r.data, err)
			require.Equal(t, want, int(code), "wire % x: %v", r.data, err)
		})
	}
	t.Logf("examined %d rows: %d refusals, %d acceptances", len(rules), refusals, acceptances)
	require.Greater(t, len(rules), 150, "the sweep shrank")
}

// TestFixedHeaderFlagsEverySixteen: [MQTT-2.1.3-1] / [MQTT-2.2.2-1],
// [MQTT-2.2.2-2], [MQTT-3.3.1-4], [MQTT-3.3.1-2], [MQTT-3.6.1-1],
// [MQTT-3.8.1-1], [MQTT-3.10.1-1], [MQTT-3.14.1-1], [MQTT-3.15.1-1]: all 256
// first bytes, against the flags the specification's Table 2-2 gives.
func TestFixedHeaderFlagsEverySixteen(t *testing.T) {
	examined := 0
	for first := 0; first < 256; first++ {
		typ, flags := byte(first>>4), byte(first&0x0F)
		var fh FixedHeader
		err := fh.Decode(byte(first))
		examined++

		bad := false
		switch typ {
		case Publish:
			qos := (flags >> 1) & 3
			bad = qos == 3 || (flags&8 != 0 && qos == 0)
		case Pubrel, Subscribe, Unsubscribe:
			bad = flags != 2
		default:
			bad = flags != 0
		}
		if bad {
			code, ok := codeOf(err)
			require.True(t, ok && (code == 0x81 || code == 0x82), "first byte 0x%02x is refused, got %v", first, err)
		} else {
			require.NoError(t, err, "first byte 0x%02x", first)
		}
	}
	require.Equal(t, 256, examined)
}

// TestRemainingLengthEncoding: [MQTT-1.5.5-1] and section 2.1.4, "up to four
// bytes".
func TestRemainingLengthEncoding(t *testing.T) {
	for name, tc := range map[string]struct {
		in   []byte
		want int // -1 accepted
	}{
		"one byte":                 {[]byte{0x7F}, 127},
		"four bytes, the maximum":  {[]byte{0xFF, 0xFF, 0xFF, 0x7F}, 268435455},
		"not the shortest, zero":   {[]byte{0x80, 0x00}, -2},
		"not the shortest, one":    {[]byte{0x81, 0x00}, -2},
		"five bytes":               {[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, -2},
		"five bytes, last is zero": {[]byte{0x80, 0x80, 0x80, 0x81, 0x00}, -2},
	} {
		t.Run(name, func(t *testing.T) {
			n, _, err := DecodeLength(bytes.NewReader(tc.in))
			if tc.want >= 0 {
				require.NoError(t, err)
				require.Equal(t, tc.want, n)
				return
			}
			require.Error(t, err)
			code, ok := codeOf(err)
			require.True(t, ok && code == 0x81, "got %v", err)
		})
	}

	// An endless run of continuation bytes is refused at the fifth byte,
	// whatever follows, rather than read for as long as it lasts.
	r := bytes.NewReader(bytes.Repeat([]byte{0x80}, 1000))
	_, _, err := DecodeLength(r)
	require.Error(t, err)
	require.LessOrEqual(t, 1000-r.Len(), 5, "bytes read before refusing")
}

// TestSavedCrasherIsRefused is the input FuzzDecodeSurvivesAReencode found
// (testdata/fuzz/FuzzDecodeSurvivesAReencode/7ef710b07d75f735): an MQTT 5
// PUBLISH whose Payload Format Indicator is 0x30, where the specification
// allows 0 and 1, and which repeats a property the specification allows
// once. The decoder took it and the re-encoded packet decoded to something
// else. It is a packet to refuse, and it is refused for both reasons.
func TestSavedCrasherIsRefused(t *testing.T) {
	data := []byte("00\x00\x0500000 \x010\x020000\x03\x00\n0000000000\b\x00\x0500000\t\x00\x00\x0100000000")
	_, err := decodeWire(data, 5)
	require.Error(t, err)
	code, ok := codeOf(err)
	require.True(t, ok, "%v", err)
	require.Equal(t, byte(0x82), code, "%v", err)
}
