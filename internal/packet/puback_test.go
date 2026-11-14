package packet_test

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// The acknowledgements of a publish: PUBACK, PUBREC, PUBREL and PUBCOMP
// (MQTT 5 3.4 to 3.7, MQTT 3.1.1 3.4 to 3.7). The four share one layout:
// MQTT 5, "Packet Identifier ..., Reason Code, Property Length, and the
// Properties" (3.4.2, 3.5.2, 3.6.2, 3.7.2); MQTT 3.1.1, the Packet
// Identifier alone, "For the PUBACK Packet this has the value 2" (3.4.1,
// 3.5.1, 3.6.1, 3.7.1). Each has its own reason-code table, which
// reasonTables holds. Error classes follow connect_test.go.
//
// The API these tests fix:
//
//	type Ack struct {
//		PacketID   uint16
//		ReasonCode byte
//		Properties Properties
//	}
//	func DecodeAck(src []byte, t Type, version Version) (Ack, error)
//	func AppendAck(dst []byte, t Type, version Version, a *Ack) ([]byte, error)
//
// t is PUBACK, PUBREC, PUBREL or PUBCOMP. DecodeAck decodes a body without
// its fixed header (PUBREL's reserved flags are ReadHeader's,
// [MQTT-3.6.1-1]); Properties share src. AppendAck writes the whole packet
// in the shortest form the version allows, after what dst holds, or
// returns ErrCannotEncode leaving dst as it was. MQTT 3.1.1's
// acknowledgement reads as Success with no properties.
//
// AppendAck trusts its Properties, checked when made (DecodeAck,
// Properties.Without, NewProperties), and writes them as given (the
// maintainer's decision on 261013-024a, TestAppendAckWritesAsGiven). It
// checks only what a caller works out per packet: the type, the version,
// Packet Identifier 0, the reason code against the type's table, MQTT
// 3.1.1's empty form and the Remaining Length.

// ackType is one of the four acknowledgements: its codec, its type and the
// first byte of its fixed header among it. MQTT 5 Figures 3-10, 3-12 and
// 3-16 (MQTT 3.1.1 Figures 3.12, 3.14 and 3.18): the type, then four
// reserved bits of 0. PUBREL, Figure 3-14 (3.16): "Bits 3,2,1 and 0 of the
// Fixed Header in the PUBREL packet are reserved and MUST be set to 0,0,1
// and 0 respectively" [MQTT-3.6.1-1].
type ackType struct {
	codec[packet.Ack]
	failure byte // a code of 0x80 or above from the type's own table
}

// newAckType describes the codec of the acknowledgement typ. Its fuzz
// oracle is ackOK, and it writes the shortest form (shortestAck) for an
// acknowledgement without properties. The encoder writes its Properties as
// given (TestAppendAckWritesAsGiven), which NewProperties makes.
func newAckType(name string, typ packet.Type, first, failure byte) ackType {
	return ackType{
		name: name, typ: typ, first: first,
		read: func(src []byte, version packet.Version) (packet.Ack, error) {
			return packet.DecodeAck(src, typ, version)
		},
		write: func(dst []byte, version packet.Version, a *packet.Ack) ([]byte, error) {
			return packet.AppendAck(dst, typ, version, a)
		},
		same:     sameAck,
		ok:       func(version packet.Version, a *packet.Ack) bool { return ackOK(name, typ, version, a) },
		packetID: func(a *packet.Ack) *uint16 { return &a.PacketID },
		shortest: func(data []byte, a *packet.Ack) []byte {
			if len(a.Properties) != 0 {
				return data
			}
			return shortestAck(a.PacketID, a.ReasonCode)
		},
		newProperties: true,
		failure:       failure,
	}
}

// ackTypes are the four acknowledgements, in Table 2-1's order. failure is
// "Not authorized" for PUBACK and PUBREC (Tables 3-4, 3-5) and "Packet
// Identifier not found" for PUBREL and PUBCOMP (Tables 3-6, 3-7).
var ackTypes = []ackType{
	newAckType("PUBACK", packet.PUBACK, 0x40, 0x87),
	newAckType("PUBREC", packet.PUBREC, 0x50, 0x87),
	newAckType("PUBREL", packet.PUBREL, 0x62, 0x92),
	newAckType("PUBCOMP", packet.PUBCOMP, 0x70, 0x92),
}

// sameAck reports whether two acknowledgements hold the same fields.
func sameAck(a, b *packet.Ack) bool {
	return a.PacketID == b.PacketID && a.ReasonCode == b.ReasonCode && sameValues(a.Properties, b.Properties)
}

// ackOK is the oracle for an Ack the encoder writes and the decoder returns
// under version, for the acknowledgement typ, named name. MQTT 5: a code of
// the type's own table ([MQTT-3.4.2-1], [MQTT-3.5.2-1], [MQTT-3.6.2-1],
// [MQTT-3.7.2-1]) and properties within Table 2-4 and their repeat rules.
// MQTT 3.1.1: no reason and no properties. Both: a non-zero Packet
// Identifier (TestAckPacketIdentifier).
func ackOK(name string, typ packet.Type, version packet.Version, ack *packet.Ack) bool {
	if ack.PacketID == 0 {
		return false
	}
	switch version {
	case packet.V5:
		props, ok := valuesOf(ack.Properties)
		return ok && slices.Contains(codesOf(typ), ack.ReasonCode) && encodable(placeNamed[name], props)
	case packet.V311:
		return ack.ReasonCode == 0 && len(ack.Properties) == 0
	}
	return false
}

// shortestAck is the body the encoder writes for an acknowledgement with no
// properties: the shortest the standard allows. 3.4.2.1 (and 3.5.2.1,
// 3.6.2.1, 3.7.2.1): "The Reason Code and Property Length can be omitted
// if the Reason Code is 0x00 (Success) and there are no Properties. In this
// case the PUBACK has a Remaining Length of 2." 3.4.2.2.1 (and 3.5.2.2.1,
// 3.6.2.2.1, 3.7.2.2.1): "If the Remaining Length is less than 4 there is
// no Property Length and the value of 0 is used", so a reason code without
// properties needs no Property Length. MQTT 3.1.1 writes only the Packet
// Identifier.
func shortestAck(id uint16, code byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, id)
	if code != 0 {
		b = append(b, code)
	}
	return b
}

// everyAckProperty sets every property the four sections list, Reason
// String and User Property, the User Property twice under one name
// (3.4.2.2.3: "The same name is allowed to appear more than once").
var everyAckProperty = []wired{stringProp(0x1F, "why"), userProp("n", "v"), userProp("n", "w")}

// validAcks are, for each of ackTypes, acknowledgements the standards
// allow, each in the form the encoder writes.
var validAcks = func() []packetCase[packet.Ack] {
	cases := make([]packetCase[packet.Ack], 0, 5*len(ackTypes))
	for i := range ackTypes {
		a := &ackTypes[i]
		k := &a.codec
		cases = append(cases,
			// MQTT 3.1.1 3.4.2: "the Packet Identifier from the PUBLISH
			// Packet that is being acknowledged".
			packetCase[packet.Ack]{a.name + ", MQTT 3.1.1", k, packet.V311, []byte{0x00, 0x0A}, packet.Ack{PacketID: 10}},
			// MQTT 5 3.4.2.1: "If the Remaining Length is 2, then there is
			// no Reason Code and the value of 0x00 (Success) is used."
			packetCase[packet.Ack]{a.name + ", MQTT 5, Remaining Length 2", k, packet.V5, []byte{0x00, 0x0A}, packet.Ack{PacketID: 10}},
			// 3.4.2.2.1: "If the Remaining Length is less than 4 there is
			// no Property Length". A Packet Identifier is a Two Byte
			// Integer (2.2.1): 65,535 is the largest.
			packetCase[packet.Ack]{
				a.name + ", MQTT 5, a failure, Remaining Length 3", k, packet.V5, []byte{0xFF, 0xFF, a.failure},
				packet.Ack{PacketID: 0xFFFF, ReasonCode: a.failure},
			},
			// Success with properties keeps its Reason Code: only Success
			// with no properties may omit it (3.4.2.1).
			packetCase[packet.Ack]{
				a.name + ", MQTT 5, Success with a Reason String", k, packet.V5,
				append([]byte{0x00, 0x01, 0x00}, setOf([]wired{stringProp(0x1F, "ok")})...),
				packet.Ack{PacketID: 1, Properties: propsOf([]wired{stringProp(0x1F, "ok")})},
			},
			packetCase[packet.Ack]{
				a.name + ", MQTT 5, a failure, every property", k, packet.V5,
				append([]byte{0x12, 0x34, a.failure}, setOf(everyAckProperty)...),
				packet.Ack{PacketID: 0x1234, ReasonCode: a.failure, Properties: propsOf(everyAckProperty)},
			},
		)
	}
	return cases
}()

// qos2Acks are the acknowledgements of a QoS 2 exchange, PUBREC, PUBREL and
// PUBCOMP. The broker writes and reads all three, which the wire tests
// check: conformance's qos2_test.go and qos2_delivery_test.go, their rows of
// harness.BadPackets (TestBadPacketsAreRefused), as PUBACK's, and the
// harness decoder, which holds every acknowledgement the broker writes to
// Tables 3-4 to 3-7. TestAckLongerForms checks the longer forms a client
// may send, which no wire test sends. The fuzz target, its oracle and the
// busy path's guards keep all four.
var qos2Acks = ackTypes[1:]

// TestAckLongerForms: an MQTT 5 acknowledgement may carry a Reason Code or
// a Property Length it could have left out (3.4.2.1, 3.4.2.2.1 and their
// like: "can be omitted"). The decoder reads the longer forms; the encoder
// writes the shortest (shortestAck).
func TestAckLongerForms(t *testing.T) {
	for _, a := range qos2Acks {
		cases := []struct {
			body []byte
			want packet.Ack
		}{
			{[]byte{0x00, 0x0A, 0x00}, packet.Ack{PacketID: 10}},
			{[]byte{0x00, 0x0A, 0x00, 0x00}, packet.Ack{PacketID: 10}},
			{[]byte{0x00, 0x0A, a.failure, 0x00}, packet.Ack{PacketID: 10, ReasonCode: a.failure}},
		}
		for _, c := range cases {
			got, err := packet.DecodeAck(c.body, a.typ, packet.V5)
			if err != nil || !sameAck(&got, &c.want) {
				t.Errorf("%s: DecodeAck(% X) = %+v, %v; want %+v", a.name, c.body, got, err, c.want)
			}
			wire := packetWith(a.first, shortestAck(c.want.PacketID, c.want.ReasonCode))
			if out, err := packet.AppendAck(nil, a.typ, packet.V5, &c.want); err != nil || !bytes.Equal(out, wire) {
				t.Errorf("%s, %+v: AppendAck = % X, %v; want % X", a.name, c.want, out, err, wire)
			}
		}
	}
}

// refusedResponseProps are acknowledgement property sets the standard
// forbids: the four acknowledgements', and SUBACK's and UNSUBACK's, which
// list the same two properties, Reason String and User Property, under the
// same sentences (MQTT 5 3.4.2.2 to 3.7.2.2, 3.9.2.1, 3.11.2.1).
var refusedResponseProps = []propertyCase{
	// 3.4.2.2.2 (and 3.5.2.2.2, 3.6.2.2.2, 3.7.2.2.2, 3.9.2.1.2 and
	// 3.11.2.1.2): "It is a Protocol Error to include the Reason String
	// more than once."
	{"Reason String twice", []wired{stringProp(0x1F, "a"), stringProp(0x1F, "b")}, packet.ErrProtocol},
	// 2.2.2.2: "A Control Packet which contains an Identifier which is not
	// valid for its packet type ... is a Malformed Packet." Table 2-4 gives
	// Topic Alias to PUBLISH; the Subscription Identifier to PUBLISH and
	// SUBSCRIBE; Session Expiry Interval to CONNECT, CONNACK and
	// DISCONNECT.
	{"Topic Alias, a PUBLISH property", []wired{intProp(0x23, 1)}, packet.ErrMalformed},
	{"Subscription Identifier, a SUBSCRIBE property", []wired{intProp(0x0B, 1)}, packet.ErrMalformed},
	{"Session Expiry Interval, a CONNECT property", []wired{intProp(0x11, 1)}, packet.ErrMalformed},
	// 3.4.2.2.2: the Reason String is a UTF-8 Encoded String; 3.4.2.2.3:
	// the User Property a UTF-8 String Pair [MQTT-1.5.7-1] (3.9.2.1.2 and
	// 3.9.2.1.3, 3.11.2.1.2 and 3.11.2.1.3 likewise). U+0000
	// [MQTT-1.5.4-2] or ill-formed UTF-8 [MQTT-1.5.4-1] "is a Malformed
	// Packet".
	{"a Reason String holding U+0000", []wired{stringProp(0x1F, "a\x00")}, packet.ErrMalformed},
	{"a User Property name, ill-formed", []wired{userProp("\xff", "v")}, packet.ErrMalformed},
	{"a User Property value, ill-formed", []wired{userProp("n", "\xff")}, packet.ErrMalformed},
}

// uncheckedSections are property sections only a check would refuse, which
// the acknowledgements' and the replies' encoders write as given
// (TestAppendAckWritesAsGiven, TestAppendReplyWritesAsGiven).
var uncheckedSections = [][]byte{
	{0x23, 0x00, 0x01},       // Topic Alias, a PUBLISH property
	{0x0B, 0x01},             // Subscription Identifier, a SUBSCRIBE property
	{0x1F, 0x00, 0x01, 0x00}, // a Reason String holding U+0000
	{0x7F},                   // an identifier Table 2-4 lacks
	{0x26, 0x00},             // a User Property cut short
	{0x1F, 0x00, 0x01, 'a', 0x1F, 0x00, 0x01, 'b'}, // Reason String twice
}

// TestAppendAckWritesAsGiven guards the busy path's standard (design.md,
// Testing: "It does no needless work. It makes no second pass over the same
// data"; the maintainer's decision on 261013-024a). Properties are checked
// when made, so AppendAck writes them byte for byte without examining them,
// after the reason code, Success included (3.4.2.1: only Success with no
// properties may omit it). Each section of uncheckedSections is one only a
// check would refuse: a property misplaced, holding U+0000, unknown, cut
// short or repeated (refusedResponseProps). Such a section can reach the
// encoder only through a bug elsewhere, which is where it is caught.
func TestAppendAckWritesAsGiven(t *testing.T) {
	for _, a := range ackTypes {
		for _, code := range []byte{0x00, a.failure} {
			for _, section := range uncheckedSections {
				ack := packet.Ack{PacketID: 1, ReasonCode: code, Properties: section}
				wire := packetWith(a.first, slices.Concat([]byte{0x00, 0x01, code}, binary.AppendUvarint(nil, uint64(len(section))), section))
				if out, err := packet.AppendAck(nil, a.typ, packet.V5, &ack); err != nil || !bytes.Equal(out, wire) {
					t.Errorf("AppendAck, %s, reason code %#02X, Properties % X: wrote % X, %v; want % X", a.name, code, section, out, err, wire)
				}
			}
		}
	}
}

// FuzzDecodeAck feeds random bytes to DecodeAck for each of the four
// acknowledgements under both versions, through the shared body
// (codec.fuzzDecode). It must never panic; it refuses with a packet error; what
// it accepts obeys ackOK and writes back in the shortest form: the bytes it
// came from when they carry properties, else shortestAck. That form reads back
// as the same Ack (design.md, Testing: "Code that reads bytes or text from
// outside Sagüin is fuzzed, whether the input comes from clients, a bridge's
// peer or the operator's files").
func FuzzDecodeAck(f *testing.F) {
	for _, c := range validAcks {
		which := slices.IndexFunc(ackTypes, func(a ackType) bool { return a.typ == c.codec.typ })
		f.Add(c.body, byte(which), c.version == packet.V5)
	}
	f.Add([]byte{0x00, 0x01, 0x00, 0x00}, byte(0), true)
	f.Add([]byte{0x00, 0x01, 0x10}, byte(1), true)
	f.Add([]byte{0x00, 0x01, 0x10}, byte(2), true)
	f.Add([]byte{0x00, 0x01, 0x00, 0x02, 0x23, 0x00}, byte(3), true)
	f.Add([]byte{0x00, 0x01, 0x00}, byte(0), false)
	f.Add([]byte{0x00, 0x00}, byte(1), true)
	f.Fuzz(func(t *testing.T, data []byte, which byte, v5 bool) {
		ackTypes[int(which)%len(ackTypes)].fuzzDecode(t, versionOf(v5), data)
	})
}
