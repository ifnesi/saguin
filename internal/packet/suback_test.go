package packet_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// The acknowledgements of subscribing: SUBACK and UNSUBACK (MQTT 5 3.9 and
// 3.11, MQTT 3.1.1 3.9 and 3.11). The broker writes them per SUBSCRIBE and
// UNSUBSCRIBE; a bridge reads a SUBACK from its peer (design.md, Bridges),
// and what these tests checked of reading it is checked from the wire,
// where a bridge's peer sends it (item 0019b): the tests here hold the
// encoders (writer), the fuzz target and the allocation guard SUBACK's
// decoder. Nothing of Sagüin reads an UNSUBACK. MQTT
// 5 lays both out alike: "the Packet Identifier from the SUBSCRIBE Packet
// that is being acknowledged, and Properties" (3.9.2; 3.11.2 likewise),
// then "a list of Reason Codes" (3.9.3, 3.11.3), "one or more" (2.4). MQTT
// 3.1.1: a SUBACK carries "a list of return codes" (3.9.3); an UNSUBACK
// "has no payload" (3.11.3). Each packet's reason codes are its own table,
// which reasonTables holds and ReasonName gives (item 0003d, comment
// 261011 122831: no list of their own). Error classes follow
// connect_test.go.
//
// The API these tests fix:
//
//	type Suback struct {
//		PacketID    uint16
//		Properties  Properties
//		ReasonCodes []byte
//	}
//	func DecodeSuback(src []byte, version Version) (Suback, error)
//	func AppendSuback(dst []byte, version Version, s *Suback) ([]byte, error)
//	type Unsuback struct {
//		PacketID    uint16
//		Properties  Properties
//		ReasonCodes []byte
//	}
//	func AppendUnsuback(dst []byte, version Version, u *Unsuback) ([]byte, error)
//
// DecodeSuback decodes a body without its fixed header (the reserved flags
// 0000 are ReadHeader's); Properties and
// ReasonCodes share src. MQTT 3.1.1's four SUBACK return codes are the
// MQTT 5 codes of the same value and meaning (0x00, 0x01 and 0x02 granted
// QoS 0, 1 and 2; 0x80 a failure), so ReasonCodes holds the wire bytes in
// both versions.
//
// The encoders take MQTT 5 codes in both versions, the core's own, as
// AppendConnack does (the maintainer's decision on item 0003d's tests).
// Under MQTT 3.1.1, AppendSuback takes a code of Table 3-8, writes 0x00,
// 0x01 and 0x02 as they are and every failure (0x80 or above) as 0x80:
// design.md, Flows, SUBSCRIBE, "MQTT 3.1.1 gets 0x80 for each", and
// MQTT 3.1.1 [MQTT-3.9.3-2], "SUBACK return codes other than 0x00, 0x01,
// 0x02 and 0x80 are reserved and MUST NOT be used". AppendUnsuback takes
// no code or only successes of Table 3-9 and writes no payload (3.11.3);
// a failure, which MQTT 3.1.1 cannot express, it refuses: design.md,
// Flows, SUBSCRIBE, "MQTT 3.1.1 has no code, so its connection is closed",
// the core's decision, which the encoder does not hide.
//
// An encoder writes the
// whole packet after what dst holds, or returns ErrCannotEncode leaving
// dst as it was. It trusts its Properties, checked when made (a decoder,
// Properties.Without, NewProperties), and writes them as given
// (TestAppendReplyWritesAsGiven); it checks what the core works out per
// packet: the version, the Packet Identifier, each reason code against the
// type's table and version, and the Remaining Length.

// reply holds the fields SUBACK and UNSUBACK share, so one set of tests
// covers both.
type reply struct {
	PacketID    uint16
	Properties  packet.Properties
	ReasonCodes []byte
}

// replyType is SUBACK or UNSUBACK: its codec, through reply (its type and
// the first byte of its fixed header among it), a failure code of its own
// table, and MQTT 3.1.1's return codes.
type replyType struct {
	codec[reply]
	failure  byte
	codes311 []byte
}

// newReplyType describes a reply's codec. The encoders write their
// Properties as given (TestAppendReplyWritesAsGiven), which NewProperties
// makes.
func newReplyType(name string, typ packet.Type, first, failure byte, codes311 []byte,
	read func([]byte, packet.Version) (reply, error), write func([]byte, packet.Version, *reply) ([]byte, error),
) replyType {
	return replyType{
		name: name, typ: typ, first: first, read: read, write: write, same: sameReply,
		packetID:      func(r *reply) *uint16 { return &r.PacketID },
		newProperties: true,
		failure:       failure,
		codes311:      codes311,
	}
}

// replyTypes are SUBACK and UNSUBACK. Fixed headers: MQTT 5 Figures 3-22
// and 3.31, MQTT 3.1.1 Figures 3.24 and 3.31, the type then four reserved
// bits of 0. "Not authorized" (0x87) is in both Table 3-8 and Table 3-9.
// MQTT 3.1.1 3.9.3, "Allowed return codes: 0x00 - Success - Maximum QoS 0,
// 0x01 - Success - Maximum QoS 1, 0x02 - Success - Maximum QoS 2, 0x80 -
// Failure"; 3.11.3, "The UNSUBACK Packet has no payload."
// Each type's fuzz oracle is replyOK.
var replyTypes = func() []replyType {
	rts := []replyType{
		newReplyType("SUBACK", packet.SUBACK, 0x90, 0x87, []byte{0x00, 0x01, 0x02, 0x80}, readSuback, writeSuback),
		newReplyType("UNSUBACK", packet.UNSUBACK, 0xB0, 0x87, nil, nil, writeUnsuback),
	}
	for i := range rts {
		rt := &rts[i]
		rt.ok = func(version packet.Version, r *reply) bool { return replyOK(rt, version, r) }
	}
	return rts
}()

func readSuback(src []byte, version packet.Version) (reply, error) {
	s, err := packet.DecodeSuback(src, version)
	return reply{s.PacketID, s.Properties, s.ReasonCodes}, err
}

func writeSuback(dst []byte, version packet.Version, r *reply) ([]byte, error) {
	return packet.AppendSuback(dst, version, &packet.Suback{PacketID: r.PacketID, Properties: r.Properties, ReasonCodes: r.ReasonCodes})
}

func writeUnsuback(dst []byte, version packet.Version, r *reply) ([]byte, error) {
	return packet.AppendUnsuback(dst, version, &packet.Unsuback{PacketID: r.PacketID, Properties: r.Properties, ReasonCodes: r.ReasonCodes})
}

// sameReply reports whether two replies hold the same fields.
func sameReply(a, b *reply) bool {
	return a.PacketID == b.PacketID && sameValues(a.Properties, b.Properties) && bytes.Equal(a.ReasonCodes, b.ReasonCodes)
}

// replyBody lays out a reply's body as the standard orders it: the Packet
// Identifier, in MQTT 5 the Property Length and properties, then the
// reason codes.
func replyBody(version packet.Version, id uint16, props []wired, codes []byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, id)
	if version == packet.V5 {
		b = append(b, setOf(props)...)
	}
	return append(b, codes...)
}

// replyOK is the oracle for a reply the decoder returns under version. A
// non-zero Packet Identifier (TestReplyPacketIdentifier). MQTT 5:
// properties within Table 2-4 and their repeat rules, and one or more
// reason codes (2.4), each of the type's own table ([MQTT-3.9.3-2],
// [MQTT-3.11.3-2]). MQTT 3.1.1: no properties; a SUBACK's one or more
// return codes each one of the four ([MQTT-3.9.3-2]); an UNSUBACK's none.
func replyOK(rt *replyType, version packet.Version, r *reply) bool {
	if r.PacketID == 0 {
		return false
	}
	switch version {
	case packet.V5:
		props, ok := valuesOf(r.Properties)
		return ok && encodable(placeNamed[rt.name], props) && len(r.ReasonCodes) != 0 && allIn(r.ReasonCodes, codesOf(rt.typ))
	case packet.V311:
		if rt.typ == packet.UNSUBACK {
			return len(r.Properties) == 0 && len(r.ReasonCodes) == 0
		}
		return len(r.Properties) == 0 && len(r.ReasonCodes) != 0 && allIn(r.ReasonCodes, rt.codes311)
	}
	return false
}

// allIn reports whether every code is one of allowed.
func allIn(codes, allowed []byte) bool {
	for _, c := range codes {
		if !slices.Contains(allowed, c) {
			return false
		}
	}
	return true
}

// everyReplyProperty sets every property the two sections list, Reason
// String and User Property, the User Property twice under one name
// (3.9.2.1.3, 3.11.2.1.3: "The same name is allowed to appear more than
// once").
var everyReplyProperty = []wired{stringProp(0x1F, "why"), userProp("n", "v"), userProp("n", "w")}

// validReplies are, for each of replyTypes, replies the standards allow,
// in the one form each has.
var validReplies = func() []packetCase[reply] {
	var cases []packetCase[reply]
	for i := range replyTypes {
		rt := &replyTypes[i]
		add := func(name string, version packet.Version, id uint16, props []wired, codes []byte) {
			cases = append(cases, packetCase[reply]{
				rt.name + ", " + name, &rt.codec, version, replyBody(version, id, props, codes),
				reply{PacketID: id, Properties: propsOf(props), ReasonCodes: codes},
			})
		}
		// One code, the type's first; a Packet Identifier is a Two Byte
		// Integer (2.2.1), 65,535 the largest. MQTT 5 3.9.2.1.1 and
		// 3.11.2.1.1: the Property Length is always there.
		add("MQTT 5, one code", packet.V5, 0xFFFF, nil, []byte{0x00})
		// Every code of the type's own table, in one list, and every
		// property (3.9.2.1, 3.11.2.1).
		add("MQTT 5, every code and property", packet.V5, 0x1234, everyReplyProperty, codesOf(rt.typ))
		// The non-normative comment of 3.9.3 and 3.11.3: "If the Reason
		// Code is not specific to a Topic Filters (such as 0x91 (Packet
		// Identifier in use)) it is set for each Topic Filter."
		add("MQTT 5, a failure for each filter", packet.V5, 10, []wired{userProp("k", "v")}, []byte{0x91, 0x91, 0x91})
		if rt.typ == packet.SUBACK {
			// MQTT 3.1.1 3.9.3.1 Table 3.6: "Success - Maximum QoS 0",
			// "Success - Maximum QoS 2", "Failure"; and QoS 1 alone.
			add("MQTT 3.1.1, the standard's example", packet.V311, 10, nil, []byte{0x00, 0x02, 0x80})
			add("MQTT 3.1.1, one code", packet.V311, 1, nil, []byte{0x01})
		} else {
			// MQTT 3.1.1 3.11.1: "For the UNSUBACK Packet this has the
			// value 2"; 3.11.2, "the Packet Identifier of the UNSUBSCRIBE
			// Packet that is being acknowledged".
			add("MQTT 3.1.1", packet.V311, 10, nil, nil)
		}
	}
	return cases
}()

// TestReply encodes each of validReplies: the encoder writes the whole
// packet, its fixed header's first byte as replyTypes gives it, after what
// dst holds.
func TestReply(t *testing.T) {
	testRoundTrips(t, writing(validReplies))
}

// TestReplyReasonCodes tries every code byte, alone and on either side of
// an allowed code, in each reply that carries codes, writing it as an MQTT
// 5 code (onWire). MQTT 5: a code of the type's own table is written:
// SUBACK, Table 3-8, "The Server
// sending a SUBACK packet MUST use one of the Subscribe Reason Codes for
// each Topic Filter received [MQTT-3.9.3-2]"; UNSUBACK, Table 3-9, "MUST
// use one of the Unsubscribe Reason Code values for each Topic Filter
// received [MQTT-3.11.3-2]". MQTT 3.1.1 SUBACK: "SUBACK return codes other
// than 0x00, 0x01, 0x02 and 0x80 are reserved and MUST NOT be used
// [MQTT-3.9.3-2]": the encoder takes any code of Table 3-8 and writes a
// failure as 0x80 (the decision at the API comment above). The rest it
// refuses with ErrCannotEncode.
func TestReplyReasonCodes(t *testing.T) {
	for i := range replyTypes {
		rt := &replyTypes[i]
		for _, version := range bothVersions {
			if version == packet.V311 && rt.codes311 == nil {
				continue // TestUnsubackMQTT311
			}
			for code := range 256 {
				for _, codes := range [][]byte{{byte(code)}, {0x00, byte(code)}, {byte(code), 0x00}} {
					want := reply{PacketID: 1, ReasonCodes: codes}
					// The encoders take the type's MQTT 5 table in both
					// versions; MQTT 3.1.1 writes each failure as 0x80.
					if !slices.Contains(codesOf(rt.typ), byte(code)) {
						refusedToWrite(t, fmt.Sprintf("%s, MQTT %v, codes % X", rt.name, version, codes), func(dst []byte) ([]byte, error) {
							return rt.write(dst, version, &want)
						})
						continue
					}
					wire := packetWith(rt.first, replyBody(version, 1, nil, onWire(version, codes)))
					if out, err := rt.write(nil, version, &want); err != nil || !bytes.Equal(out, wire) {
						t.Errorf("%s, MQTT %v, codes % X: wrote % X, %v; want % X", rt.name, version, codes, out, err, wire)
					}
				}
			}
		}
	}
}

// onWire returns the SUBACK codes version carries for MQTT 5 codes: as
// they are in MQTT 5; in MQTT 3.1.1, each failure (0x80 or above) as
// 0x80, "Failure" (3.9.3), design.md, Flows, SUBSCRIBE: "MQTT 3.1.1 gets
// 0x80 for each".
func onWire(version packet.Version, codes []byte) []byte {
	if version == packet.V5 {
		return codes
	}
	wire := make([]byte, len(codes))
	for i, c := range codes {
		wire[i] = min(c, 0x80)
	}
	return wire
}

// TestReplyNoReasonCodes: "The SUBACK and UNSUBACK packets contain a list
// of one or more Reason Codes in the Payload" (MQTT 5 2.4), one "for each
// Topic Filter" ([MQTT-3.8.4-6]; 3.11.3), of which a SUBSCRIBE or
// UNSUBSCRIBE carries at least one ([MQTT-3.8.3-2], [MQTT-3.10.3-2]). MQTT
// 3.1.1 likewise for SUBACK: "a return code for each Topic Filter/QoS
// pair" [MQTT-3.8.4-5], of which there is at least one [MQTT-3.8.3-3].
// No sentence names a class: the encoder refuses an empty list with
// ErrCannotEncode, leaving dst as it was.
func TestReplyNoReasonCodes(t *testing.T) {
	for i := range replyTypes {
		rt := &replyTypes[i]
		under := []packet.Version{packet.V5} // an MQTT 3.1.1 UNSUBACK has no codes
		if rt.codes311 != nil {
			under = bothVersions
		}
		testNoPayload(t, rt.writer(), under, nil, everyReplyProperty, func(codes []byte) reply {
			return reply{PacketID: 1, ReasonCodes: codes}
		})
	}
}

// TestReplyPacketIdentifier: a SUBACK or UNSUBACK "MUST contain the Packet
// Identifier that was used in the corresponding SUBSCRIBE and UNSUBSCRIBE
// packet respectively" (MQTT 5 [MQTT-2.2.1-6]; MQTT 3.1.1 [MQTT-2.3.1-7]),
// which is non-zero ([MQTT-2.2.1-3]; [MQTT-2.3.1-1]). No class is named:
// the encoder refuses Packet Identifier 0 with ErrCannotEncode, leaving
// dst as it was.
func TestReplyPacketIdentifier(t *testing.T) {
	testPacketIdentifierZero(t, writing(validReplies))
}

// TestUnsubackMQTT311: an MQTT 3.1.1 UNSUBACK's Remaining Length "has the
// value 2" (3.11.1) and it "has no payload" (3.11.3). The encoder takes
// MQTT 5 Unsubscribe
// Reason Codes (Table 3-9) and writes none of them: no code, or codes
// that are all successes (below 0x80, 2.4), make the bare UNSUBACK. A
// failure of Table 3-9, which MQTT 3.1.1 cannot express (design.md,
// Flows, SUBSCRIBE: "MQTT 3.1.1 has no code, so its connection is closed"), a
// code Table 3-9 lacks, or properties (3.11.2: the Packet Identifier alone)
// are refused, leaving dst as it was.
func TestUnsubackMQTT311(t *testing.T) {
	bare := []byte{0xB0, 0x02, 0x00, 0x0A}
	lists := append(make([][]byte, 0, 2+3*256), nil, []byte{}) // no code, as nil and as empty
	for code := range 256 {
		lists = append(lists, []byte{byte(code)}, []byte{0x00, byte(code)}, []byte{byte(code), 0x11})
	}
	for _, codes := range lists {
		u := packet.Unsuback{PacketID: 10, ReasonCodes: codes}
		ok := true
		for _, c := range codes {
			ok = ok && c < 0x80 && slices.Contains(codesOf(packet.UNSUBACK), c)
		}
		if !ok {
			refusedToWrite(t, fmt.Sprintf("AppendUnsuback, MQTT 3.1.1, codes % X", codes), func(dst []byte) ([]byte, error) {
				return packet.AppendUnsuback(dst, packet.V311, &u)
			})
			continue
		}
		if out, err := packet.AppendUnsuback(nil, packet.V311, &u); err != nil || !bytes.Equal(out, bare) {
			t.Errorf("AppendUnsuback, MQTT 3.1.1, codes % X: wrote % X, %v; want % X", codes, out, err, bare)
		}
	}
	replyTypes[1].writeRefused(t, "with properties", packet.V311, &reply{PacketID: 1, Properties: propsOf(everyReplyProperty)})
}

// TestReplyProperties checks the rules of 3.9.2.1 and 3.11.2.1 on the
// writing side: NewProperties refuses to make a section of
// refusedResponseProps (puback_test.go: SUBACK and UNSUBACK carry the
// acknowledgements' two properties, under the same sentences), with
// ErrCannotEncode and no section. The encoder writes the section it is
// given, checked when made (TestAppendReplyWritesAsGiven).
func TestReplyProperties(t *testing.T) {
	for i := range replyTypes {
		rt := &replyTypes[i]
		testProperties(t, rt.writer(), propsCases(nil, refusedResponseProps), func(c *propsCase) ([]byte, reply) {
			return replyBody(packet.V5, 1, c.props, []byte{0x00}), reply{PacketID: 1, Properties: propsOf(c.props), ReasonCodes: []byte{0x00}}
		})
	}
}

// TestAppendReplyWritesAsGiven guards the busy path's standard (design.md,
// Testing: "It does no needless work. It makes no second pass over the same
// data"; the maintainer's decision on 261013-024a, as AppendAck keeps it).
// The broker writes a SUBACK or UNSUBACK for every SUBSCRIBE and UNSUBSCRIBE,
// and its Properties are checked when made, so the encoder writes them byte
// for byte without examining them. Each section of uncheckedSections
// (puback_test.go) is one only a check would refuse: a property misplaced,
// holding U+0000, unknown, cut short or repeated (refusedResponseProps). Such
// a section can reach the encoder only through a bug elsewhere, which is
// where it is caught.
func TestAppendReplyWritesAsGiven(t *testing.T) {
	for i := range replyTypes {
		rt := &replyTypes[i]
		for _, section := range uncheckedSections {
			r := reply{PacketID: 1, Properties: section, ReasonCodes: []byte{0x00, rt.failure}}
			wire := packetWith(rt.first, slices.Concat([]byte{0x00, 0x01}, binary.AppendUvarint(nil, uint64(len(section))), section, r.ReasonCodes))
			if out, err := rt.write(nil, packet.V5, &r); err != nil || !bytes.Equal(out, wire) {
				t.Errorf("%s, Properties % X: wrote % X, %v; want % X", rt.name, section, out, err, wire)
			}
		}
	}
}

// FuzzDecodeSuback feeds random bytes to DecodeSuback under both versions,
// after validReplies' SUBACKs as seeds, through the shared body
// (codec.fuzzDecode). The decoder must never panic; it refuses with a packet
// error; what it accepts obeys replyOK and writes back as exactly the packet
// it came from, a SUBACK having one form (design.md, Testing: "Code that
// reads bytes or text from outside Sagüin is fuzzed, whether the input comes
// from clients, a bridge's peer or the operator's files"): MQTT 3.1.1's four
// return codes are MQTT 5 codes the encoder writes as they are (onWire).
func FuzzDecodeSuback(f *testing.F) {
	rt := &replyTypes[0]
	for _, c := range validReplies {
		if c.codec == &rt.codec {
			f.Add(c.body, c.version == packet.V5)
		}
	}
	f.Add([]byte{0x00, 0x01, 0x00, 0x03}, true)                        // a code of neither table
	f.Add([]byte{0x00, 0x01, 0x00, 0x11}, true)                        // No subscription existed
	f.Add([]byte{0x00, 0x01, 0x03, 0x0B, 0x01, 0x00}, true)            // a misplaced property
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}, true)                        // Packet Identifier 0
	f.Add([]byte{0x00, 0x01, 0x00}, true)                              // no codes
	f.Add([]byte{0x00, 0x01, 0x87}, false)                             // an MQTT 5 code in MQTT 3.1.1
	f.Add([]byte{0x00, 0x01}, false)                                   // MQTT 3.1.1 with no codes
	f.Add([]byte{0x00, 0x01, 0x04, 0x1F, 0x00, 0x01, 'r', 0x80}, true) // a Reason String
	f.Fuzz(func(t *testing.T, data []byte, v5 bool) {
		rt.fuzzDecode(t, versionOf(v5), data)
	})
}
