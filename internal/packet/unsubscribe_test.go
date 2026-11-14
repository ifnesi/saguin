package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// Unsubscribing, UNSUBSCRIBE (MQTT 5 3.10, MQTT 3.1.1 3.10). The broker
// decodes it from clients; nothing of Sagüin writes one (no bridge flow
// unsubscribes: item 0019b). MQTT 5
// 3.10.2: "The Variable Header of the UNSUBSCRIBE Packet contains the
// following fields in the order: Packet Identifier, and Properties"; MQTT
// 3.1.1 3.10.2: "The variable header contains a Packet Identifier." The
// payload is "the list of Topic Filters ... packed contiguously" (3.10.3).
// As for SUBSCRIBE (subscribe_test.go), a filter is taken only as the
// UTF-8 Encoded String the payload carries [MQTT-3.10.3-1]; whether it is
// a valid filter is internal/topic's. Error classes follow connect_test.go.
//
// The API these tests fix:
//
//	type TopicFilters []byte
//	func (f TopicFilters) All() iter.Seq[[]byte]
//	type Unsubscribe struct {
//		PacketID   uint16
//		Properties Properties
//		Filters    TopicFilters
//	}
//	func DecodeUnsubscribe(src []byte, version Version) (Unsubscribe, error)
//
// TopicFilters is an UNSUBSCRIBE's payload as its wire bytes, checked by
// DecodeUnsubscribe, in place, the same in both versions; All reads each
// filter in wire order, sharing the payload's bytes. DecodeUnsubscribe
// decodes a body without its fixed header (the reserved flags 0010 are
// ReadHeader's, [MQTT-3.10.1-1]); Properties share src. MQTT 3.1.1 carries
// no properties.

// unsubscribeFirst is UNSUBSCRIBE's first fixed-header byte: type 10,
// then "Bits 3,2,1 and 0 ... MUST be set to 0,0,1 and 0 respectively"
// (MQTT 5 Figure 3.28, [MQTT-3.10.1-1]; MQTT 3.1.1 Figure 3.28,
// [MQTT-3.10.1-1]).
const unsubscribeFirst = 0xA2

// unsubscribeCodec describes UNSUBSCRIBE's codec, a decoder alone. Its
// fuzz oracle is unsubscribeOK.
var unsubscribeCodec = codec[packet.Unsubscribe]{
	name: "UNSUBSCRIBE", typ: packet.UNSUBSCRIBE, first: unsubscribeFirst,
	read: packet.DecodeUnsubscribe, same: sameUnsubscribe, ok: unsubscribeOK,
	packetID:      func(u *packet.Unsubscribe) *uint16 { return &u.PacketID },
	newProperties: true,
}

// filtersWire lays out an UNSUBSCRIBE payload: each Topic Filter as a
// UTF-8 Encoded String (1.5.4 Figure 1-1), "packed contiguously" (3.10.3).
func filtersWire(filters [][]byte) []byte {
	parts := make([][]byte, len(filters))
	for i, filter := range filters {
		parts[i] = prefixed(filter)
	}
	return slices.Concat(parts...)
}

// topicFiltersOf reads an UNSUBSCRIBE payload without the code under test:
// whole UTF-8 Encoded Strings (stringField), at least one
// ([MQTT-3.10.3-2]). It reports false for anything else.
func topicFiltersOf(payload []byte) ([][]byte, bool) {
	var filters [][]byte
	for len(payload) != 0 {
		filter, rest, ok := stringField(payload)
		if !ok {
			return nil, false
		}
		filters = append(filters, filter)
		payload = rest
	}
	return filters, len(filters) != 0
}

// filterList returns filters as the list NewTopicFilters takes.
func filterList(filters ...string) [][]byte {
	list := make([][]byte, len(filters))
	for i, filter := range filters {
		list[i] = []byte(filter)
	}
	return list
}

// sameUnsubscribe reports whether two UNSUBSCRIBEs hold the same fields.
func sameUnsubscribe(a, b *packet.Unsubscribe) bool {
	return a.PacketID == b.PacketID && sameValues(a.Properties, b.Properties) && bytes.Equal(a.Filters, b.Filters)
}

// unsubscribeOK is the oracle for an Unsubscribe the decoder returns under
// version: a non-zero Packet Identifier (TestUnsubscribePacketIdentifier);
// in MQTT 5, properties within Table 2-4 and their repeat rules; in MQTT
// 3.1.1, none; a payload topicFiltersOf accepts, which All reads as
// exactly the filters it holds.
func unsubscribeOK(version packet.Version, u *packet.Unsubscribe) bool {
	if u.PacketID == 0 {
		return false
	}
	switch version {
	case packet.V5:
		props, ok := valuesOf(u.Properties)
		if !ok || !encodable(placeNamed["UNSUBSCRIBE"], props) {
			return false
		}
	case packet.V311:
		if len(u.Properties) != 0 {
			return false
		}
	default:
		return false
	}
	filters, ok := topicFiltersOf(u.Filters)
	return ok && slices.EqualFunc(slices.Collect(u.Filters.All()), filters, bytes.Equal)
}

// unsubscribeSpec describes an UNSUBSCRIBE field by field. body lays it
// out as the standard orders it; want is the Unsubscribe it decodes to.
type unsubscribeSpec struct {
	name    string
	version packet.Version
	id      uint16
	props   []wired // MQTT 5 only
	filters [][]byte
}

// body returns the Packet Identifier, in MQTT 5 the Property Length and
// properties, then the payload.
func (s *unsubscribeSpec) body() []byte {
	b := binary.BigEndian.AppendUint16(nil, s.id)
	if s.version == packet.V5 {
		b = append(b, setOf(s.props)...)
	}
	return append(b, filtersWire(s.filters)...)
}

// want returns the Unsubscribe the body decodes to.
func (s *unsubscribeSpec) want() packet.Unsubscribe {
	return packet.Unsubscribe{PacketID: s.id, Properties: propsOf(s.props), Filters: filtersWire(s.filters)}
}

// caseOf returns the spec as the case unsubscribeCodec takes.
func (s *unsubscribeSpec) caseOf() packetCase[packet.Unsubscribe] {
	return packetCase[packet.Unsubscribe]{s.name, &unsubscribeCodec, s.version, s.body(), s.want()}
}

// validUnsubscribes are UNSUBSCRIBEs the standards allow, in the one form
// each has.
var validUnsubscribes = []unsubscribeSpec{
	// MQTT 3.1.1 3.10.3.1 Table 3.7 and MQTT 5 3.10.3 Figure 3.30: "a/b"
	// and "c/d".
	{name: "MQTT 3.1.1, the standard's example", version: packet.V311, id: 10, filters: filterList("a/b", "c/d")},
	{name: "MQTT 5, the standard's example", version: packet.V5, id: 10, filters: filterList("a/b", "c/d")},
	// A Packet Identifier is a Two Byte Integer (2.2.1); 3.10.2.1.2: "The
	// User Property is allowed to appear multiple times ... The same name
	// is allowed to appear more than once."
	{
		name: "MQTT 5, User Properties", version: packet.V5, id: 0xFFFF,
		props: []wired{userProp("n", "v"), userProp("n", "w")}, filters: filterList("A\U0002A6D4/+/#"),
	},
	// 1.5.4: a string of up to 65,535 bytes.
	{name: "MQTT 3.1.1, a filter of 65,535 bytes", version: packet.V311, id: 1, filters: filterList(strings.Repeat("a", 65_535))},
	{name: "MQTT 5, a filter of 65,535 bytes", version: packet.V5, id: 1, filters: filterList(strings.Repeat("a", 65_535), "b")},
}

// TestUnsubscribe decodes each of validUnsubscribes into its fields and
// reads its filters back through All.
func TestUnsubscribe(t *testing.T) {
	testRoundTrips(t, casesOf(validUnsubscribes, (*unsubscribeSpec).caseOf))
	for i := range validUnsubscribes {
		s := &validUnsubscribes[i]
		got, _ := packet.DecodeUnsubscribe(s.body(), s.version) // testRoundTrips checks the decoding
		if filters := slices.Collect(got.Filters.All()); !slices.EqualFunc(filters, s.filters, bytes.Equal) {
			t.Errorf("%s: All reads %q; want %q", s.name, filters, s.filters)
		}
	}
}

// TestTopicFiltersAllStops: All is an iter.Seq, which its consumer may stop
// at any filter, as a range loop's break does; once yield returns false,
// All yields nothing more (stopsWhenAsked).
func TestTopicFiltersAllStops(t *testing.T) {
	for i := range validUnsubscribes {
		s := &validUnsubscribes[i]
		stopsWhenAsked(t, s.name, s.want().Filters.All(), len(s.filters))
	}
}

// TestUnsubscribePacketIdentifier: a Client "MUST assign it a non-zero
// Packet Identifier" to each new UNSUBSCRIBE (MQTT 5 [MQTT-2.2.1-3]); MQTT
// 3.1.1: UNSUBSCRIBE "MUST contain a non-zero 16-bit Packet Identifier"
// [MQTT-2.3.1-1]. Neither names a class: the decoder refuses Packet
// Identifier 0 with a packet error.
func TestUnsubscribePacketIdentifier(t *testing.T) {
	testPacketIdentifierZero(t, casesOf(validUnsubscribes, (*unsubscribeSpec).caseOf))
}

// TestUnsubscribeNoPayload: "The Payload of an UNSUBSCRIBE packet MUST
// contain at least one Topic Filter [MQTT-3.10.3-2]. An UNSUBSCRIBE packet
// with no Payload is a Protocol Error." (MQTT 5 3.10.3). MQTT 3.1.1
// 3.10.3: "An UNSUBSCRIBE packet with no payload is a protocol violation
// [MQTT-3.10.3-2]", naming no class. An MQTT 5 body of the Packet
// Identifier alone also lacks the Property Length, so it is refused
// without a class.
func TestUnsubscribeNoPayload(t *testing.T) {
	testNoPayload(t, &unsubscribeCodec, bothVersions, packet.ErrProtocol, []wired{userProp("n", "v")}, func(payload []byte) packet.Unsubscribe {
		return packet.Unsubscribe{PacketID: 1, Filters: payload}
	})
}

// TestUnsubscribeTopicFilterString: "The Topic Filters in an UNSUBSCRIBE
// packet MUST be UTF-8 Encoded Strings [MQTT-3.10.3-1]" (MQTT 5 3.10.3;
// MQTT 3.1.1 [MQTT-3.10.3-1]). Ill-formed UTF-8 or a surrogate
// [MQTT-1.5.4-1], or U+0000 [MQTT-1.5.4-2], "is a Malformed Packet" (MQTT
// 5 1.5.4); MQTT 3.1.1 1.5.3 closes the connection [MQTT-1.5.3-1],
// [MQTT-1.5.3-2], naming no class. The decoder refuses such a filter
// wherever it stands.
func TestUnsubscribeTopicFilterString(t *testing.T) {
	bad := slices.Concat(illFormed, surrogates, nulls)
	for _, filter := range bad {
		for _, list := range [][][]byte{{filter}, {[]byte("a/b"), filter}, {filter, []byte("a/b")}} {
			for _, version := range bothVersions {
				s := unsubscribeSpec{version: version, id: 1, filters: list}
				got, err := packet.DecodeUnsubscribe(s.body(), version)
				switch {
				case version == packet.V5 && !errors.Is(err, packet.ErrMalformed):
					t.Errorf("MQTT 5, filters %q: got %+v, %v; want ErrMalformed", list, got, err)
				case !isKind(err):
					t.Errorf("MQTT %v, filters %q: got %+v, %v; want ErrMalformed or ErrProtocol", version, list, got, err)
				}
			}
		}
	}
}

// refusedUnsubscribeProps are UNSUBSCRIBE property sets the standard
// forbids. 3.10.2.1 lists the User Property alone.
var refusedUnsubscribeProps = []propertyCase{
	// 2.2.2.2: "A Control Packet which contains an Identifier which is not
	// valid for its packet type ... is a Malformed Packet." Table 2-4 gives
	// the Subscription Identifier to PUBLISH and SUBSCRIBE, the Reason
	// String to the acknowledgements.
	{"Subscription Identifier, a SUBSCRIBE property", []wired{intProp(0x0B, 1)}, packet.ErrMalformed},
	{"Reason String, an UNSUBACK property", []wired{stringProp(0x1F, "r")}, packet.ErrMalformed},
	// 3.10.2.1.2: the User Property is a UTF-8 String Pair [MQTT-1.5.7-1];
	// U+0000 [MQTT-1.5.4-2] or a surrogate [MQTT-1.5.4-1] "is a Malformed
	// Packet".
	{"a User Property name holding U+0000", []wired{userProp("a\x00", "v")}, packet.ErrMalformed},
	{"a User Property value holding a surrogate", []wired{userProp("n", "\xed\xa0\x80")}, packet.ErrMalformed},
}

// TestUnsubscribeProperties checks 3.10.2.1: the decoder refuses each of
// refusedUnsubscribeProps with the class its sentence names, and
// NewProperties refuses to make the section, with ErrCannotEncode and no
// section. User Properties, repeated under one name, decode and are made
// into exactly their wire bytes.
func TestUnsubscribeProperties(t *testing.T) {
	cases := append(propsCases(nil, refusedUnsubscribeProps),
		propsCase{name: "User Properties", props: []wired{userProp("n", "v"), userProp("n", "v"), userProp("", "")}, ok: true})
	testProperties(t, &unsubscribeCodec, cases, func(c *propsCase) ([]byte, packet.Unsubscribe) {
		s := unsubscribeSpec{version: packet.V5, id: 1, props: c.props, filters: filterList("a/b")}
		return s.body(), s.want()
	})
}

// FuzzDecodeUnsubscribe feeds random bytes to DecodeUnsubscribe under both
// versions, through the shared body (codec.fuzzDecode). It must never panic; it
// refuses with a packet error; what it accepts obeys unsubscribeOK
// (design.md, Testing: "Code that reads bytes or text from outside Sagüin is
// fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files").
func FuzzDecodeUnsubscribe(f *testing.F) {
	for i := range validUnsubscribes {
		s := &validUnsubscribes[i]
		if body := s.body(); len(body) < 256 {
			f.Add(body, s.version == packet.V5)
		}
	}
	f.Add([]byte{0x00, 0x01, 0x02, 0x0B, 0x01, 0x00, 0x01, 'a'}, true) // a SUBSCRIBE property
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 'a'}, true)             // Packet Identifier 0
	f.Add([]byte{0x00, 0x01, 0x00}, true)                              // no payload
	f.Add([]byte{0x00, 0x01}, false)                                   // no payload
	f.Add([]byte{0x00, 0x01, 0x00, 0x02, 0xC0, 0x80}, false)           // an ill-formed filter
	f.Add([]byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, true)            // two empty strings
	f.Fuzz(func(t *testing.T, data []byte, v5 bool) {
		unsubscribeCodec.fuzzDecode(t, versionOf(v5), data)
	})
}
