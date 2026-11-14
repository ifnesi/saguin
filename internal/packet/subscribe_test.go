package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Subscribing, SUBSCRIBE (MQTT 5 3.8, MQTT 3.1.1 3.8). The broker decodes
// it from clients; a bridge writes it to its peer (design.md, Bridges:
// "the bridge subscribes at the peer with No Local"). MQTT 5 3.8.2: "The
// Variable Header of the SUBSCRIBE Packet contains the following fields in
// the order: Packet Identifier, and Properties"; MQTT 3.1.1 3.8.2: "The
// variable header contains a Packet Identifier." The payload is "a list of
// Topic Filters ... Each Topic Filter is followed by a Subscription Options
// byte" (3.8.3); in MQTT 3.1.1 "a byte called the Requested QoS". Error
// classes follow connect_test.go.
//
// Whether a Topic Filter is a valid filter (wildcards, $share, lengths) is
// internal/topic's and the reader's, and what each subscription is granted
// is the core's (design.md, Flows, SUBSCRIBE): these tests take a filter
// only as the UTF-8 Encoded String the payload carries [MQTT-3.8.3-1].
//
// The API these tests fix:
//
//	type Subscription struct {
//		Filter            []byte
//		QoS               byte
//		NoLocal           bool
//		RetainAsPublished bool
//		RetainHandling    byte
//	}
//	type Subscriptions []byte
//	func (s Subscriptions) All() iter.Seq[Subscription]
//	func NewSubscriptions(version Version, list []Subscription) (Subscriptions, error)
//	type Subscribe struct {
//		PacketID      uint16
//		Properties    Properties
//		Subscriptions Subscriptions
//	}
//	func DecodeSubscribe(src []byte, version Version) (Subscribe, error)
//	func AppendSubscribe(dst []byte, version Version, s *Subscribe) ([]byte, error)
//
// Subscriptions is a SUBSCRIBE's payload as its wire bytes, checked when
// made, as Properties is: by DecodeSubscribe, which returns it in place
// (sharing src), or by NewSubscriptions, which makes it for one version
// from a list, the filters copied, or refuses with ErrCannotEncode and no
// payload. All reads each subscription in wire order, Filter sharing the
// payload's bytes. DecodeSubscribe decodes a body without its fixed header
// (the reserved flags 0010 are ReadHeader's, [MQTT-3.8.1-1]); Properties
// share src. AppendSubscribe
// writes the whole packet after what dst holds, Properties and
// Subscriptions as given, checked when made (NewProperties gives a
// SUBSCRIBE its section), or returns ErrCannotEncode leaving dst as it
// was. MQTT 3.1.1 carries no properties.
//
// Only a bridge writes a SUBSCRIBE, so what these tests checked of
// NewSubscriptions and AppendSubscribe is checked from the wire, where a
// bridge's peer receives it (item 0019b; design.md, Bridges): the tests
// here hold the decoder (subscribeCodec.reader), the fuzz target and the
// allocation guards the encoder.

// Subscription Options bits, MQTT 5 3.8.3.1 Figure 3-20: "Bits 0 and 1 ...
// Maximum QoS", "Bit 2 ... No Local", "Bit 3 ... Retain As Published",
// "Bits 4 and 5 ... Retain Handling", "Bits 6 and 7 ... reserved". MQTT
// 3.1.1 Figure 3.22: the Requested QoS in bits 0 and 1, the upper six
// reserved.
const (
	optionQoS            = 0x03
	optionNoLocal        = 0x04
	optionRetainAsPub    = 0x08
	optionRetainHandling = 0x30
	optionRetainShift    = 4
	optionReserved       = 0xC0
)

// subscribeFirst is SUBSCRIBE's first fixed-header byte: type 8, then
// "Bits 3,2,1 and 0 ... MUST be set to 0,0,1 and 0 respectively" (MQTT 5
// Figure 3-18, [MQTT-3.8.1-1]; MQTT 3.1.1 Figure 3.20, [MQTT-3.8.1-1]).
const subscribeFirst = 0x82

// subscribeCodec describes SUBSCRIBE's codec. Its fuzz oracle is
// subscribeOK. The encoder writes its Properties as given, which
// NewProperties makes.
var subscribeCodec = codec[packet.Subscribe]{
	name: "SUBSCRIBE", typ: packet.SUBSCRIBE, first: subscribeFirst,
	read: packet.DecodeSubscribe, write: packet.AppendSubscribe, same: sameSubscribe, ok: subscribeOK,
	packetID:      func(s *packet.Subscribe) *uint16 { return &s.PacketID },
	newProperties: true,
}

// optionsByte returns a subscription's options byte as Figure 3-20 lays it
// out; under MQTT 3.1.1 only the QoS bits are set (Figure 3.22). The
// subscription's QoS and Retain Handling must fit their two bits.
func optionsByte(s *packet.Subscription) byte {
	o := s.QoS | s.RetainHandling<<optionRetainShift
	if s.NoLocal {
		o |= optionNoLocal
	}
	if s.RetainAsPublished {
		o |= optionRetainAsPub
	}
	return o
}

// subscriptionOf reads an options byte by Figure 3-20.
func subscriptionOf(filter []byte, o byte) packet.Subscription {
	return packet.Subscription{
		Filter:            filter,
		QoS:               o & optionQoS,
		NoLocal:           o&optionNoLocal != 0,
		RetainAsPublished: o&optionRetainAsPub != 0,
		RetainHandling:    (o & optionRetainHandling) >> optionRetainShift,
	}
}

// optionsOK is the oracle for an options byte under version. MQTT 5
// 3.8.3.1: "It is a Protocol Error if the Maximum QoS field has the value
// 3", "It is a Protocol Error to send a Retain Handling value of 3", and
// "The Server MUST treat a SUBSCRIBE packet as malformed if any of Reserved
// bits in the Payload are non-zero [MQTT-3.8.3-5]". MQTT 3.1.1 3.8.3: "The
// Server MUST treat a SUBSCRIBE packet as malformed ... if any of Reserved
// bits in the payload are non-zero, or QoS is not 0,1 or 2 [MQTT-3-8.3-4]".
func optionsOK(version packet.Version, o byte) bool {
	if version == packet.V311 {
		return o <= 2
	}
	return o&optionReserved == 0 && o&optionQoS != 3 && o&optionRetainHandling != 3<<optionRetainShift
}

// subscriptionsWire lays out a SUBSCRIBE payload: each Topic Filter as a
// UTF-8 Encoded String (1.5.4 Figure 1-1), then its options byte (MQTT 5
// 3.8.3 Figure 3-20; MQTT 3.1.1 3.8.3 Figure 3.22, "packed contiguously").
func subscriptionsWire(subs []packet.Subscription) []byte {
	parts := make([][]byte, 0, 2*len(subs))
	for i := range subs {
		parts = append(parts, prefixed(subs[i].Filter), []byte{optionsByte(&subs[i])})
	}
	return slices.Concat(parts...)
}

// subscriptionsOf reads a SUBSCRIBE payload without the code under test:
// whole Topic Filter and options pairs, each filter a UTF-8 Encoded String
// (testkit.UTF8StringOK), each options byte allowed under version (optionsOK), and at
// least one pair ([MQTT-3.8.3-2]; MQTT 3.1.1 [MQTT-3.8.3-3]). It reports
// false for anything else.
func subscriptionsOf(version packet.Version, payload []byte) ([]packet.Subscription, bool) {
	var subs []packet.Subscription
	for len(payload) != 0 {
		filter, rest, ok := stringField(payload)
		if !ok || len(rest) == 0 || !optionsOK(version, rest[0]) {
			return nil, false
		}
		subs = append(subs, subscriptionOf(filter, rest[0]))
		payload = rest[1:]
	}
	return subs, len(subs) != 0
}

// stringField reads one UTF-8 Encoded String (1.5.4): a Two Byte Integer
// length, then that many bytes (prefixedField) of well-formed UTF-8
// without U+0000 (testkit.UTF8StringOK).
func stringField(b []byte) (value, rest []byte, ok bool) {
	value, rest, ok = prefixedField(b)
	return value, rest, ok && testkit.UTF8StringOK(string(value))
}

// sameSubscription reports whether two subscriptions hold the same filter
// and options.
func sameSubscription(a, b *packet.Subscription) bool {
	return bytes.Equal(a.Filter, b.Filter) && a.QoS == b.QoS && a.NoLocal == b.NoLocal &&
		a.RetainAsPublished == b.RetainAsPublished && a.RetainHandling == b.RetainHandling
}

// sameSubscriptionList reports whether two lists hold the same
// subscriptions in the same order.
func sameSubscriptionList(a, b []packet.Subscription) bool {
	return slices.EqualFunc(a, b, func(x, y packet.Subscription) bool { return sameSubscription(&x, &y) })
}

// sameSubscribe reports whether two SUBSCRIBEs hold the same fields.
func sameSubscribe(a, b *packet.Subscribe) bool {
	return a.PacketID == b.PacketID && sameValues(a.Properties, b.Properties) && bytes.Equal(a.Subscriptions, b.Subscriptions)
}

// subscribeValueRules are the value rules of MQTT 5 3.8.2.1, beyond the
// type Table 2-4 gives each property.
var subscribeValueRules = []valueRule{
	// 3.8.2.1.2: "The Subscription Identifier can have the value of 1 to
	// 268,435,455. It is a Protocol Error if the Subscription Identifier
	// has a value of 0."
	{"SUBSCRIBE", "Subscription Identifier", 0x0B, nonZero, []uint32{1, 2, 321, maxVarInt}, []uint32{0}, packet.ErrProtocol},
}

// subscribeValuesOK reports whether props keep subscribeValueRules.
func subscribeValuesOK(props []packet.Property) bool {
	for i := range props {
		for _, r := range subscribeValueRules {
			if r.id == props[i].ID && !r.ok(props[i].Int) {
				return false
			}
		}
	}
	return true
}

// subscribeOK is the oracle for a Subscribe the decoder returns under
// version: everything MQTT 5 3.8 or MQTT 3.1.1 3.8 asks of a SUBSCRIBE,
// filter validity aside. A non-zero Packet Identifier
// (TestSubscribePacketIdentifier); in MQTT 5, properties within Table 2-4,
// their repeat rules (mayRepeat) and subscribeValueRules; in MQTT 3.1.1,
// none; a payload subscriptionsOf accepts, which All reads as exactly the
// subscriptions it holds.
func subscribeOK(version packet.Version, s *packet.Subscribe) bool {
	if s.PacketID == 0 {
		return false
	}
	switch version {
	case packet.V5:
		props, ok := valuesOf(s.Properties)
		if !ok || !encodable(placeNamed["SUBSCRIBE"], props) || !subscribeValuesOK(props) {
			return false
		}
	case packet.V311:
		if len(s.Properties) != 0 {
			return false
		}
	default:
		return false
	}
	subs, ok := subscriptionsOf(version, s.Subscriptions)
	return ok && sameSubscriptionList(slices.Collect(s.Subscriptions.All()), subs)
}

// subscribeSpec describes a SUBSCRIBE field by field. body lays it out as
// the standard orders it; want is the Subscribe it decodes to.
type subscribeSpec struct {
	name    string
	version packet.Version
	id      uint16
	props   []wired // MQTT 5 only
	subs    []packet.Subscription
}

// body returns the Packet Identifier, in MQTT 5 the Property Length and
// properties, then the payload.
func (s *subscribeSpec) body() []byte {
	b := binary.BigEndian.AppendUint16(nil, s.id)
	if s.version == packet.V5 {
		b = append(b, setOf(s.props)...)
	}
	return append(b, subscriptionsWire(s.subs)...)
}

// want returns the Subscribe the body decodes to.
func (s *subscribeSpec) want() packet.Subscribe {
	return packet.Subscribe{PacketID: s.id, Properties: propsOf(s.props), Subscriptions: subscriptionsWire(s.subs)}
}

// caseOf returns the spec as the case subscribeCodec takes.
func (s *subscribeSpec) caseOf() packetCase[packet.Subscribe] {
	return packetCase[packet.Subscribe]{s.name, &subscribeCodec, s.version, s.body(), s.want()}
}

// sub returns a subscription to filter with the given options.
func sub(filter string, qos byte, noLocal, retainAsPublished bool, retainHandling byte) packet.Subscription {
	return packet.Subscription{Filter: []byte(filter), QoS: qos, NoLocal: noLocal, RetainAsPublished: retainAsPublished, RetainHandling: retainHandling}
}

// validSubscribes are SUBSCRIBEs the standards allow, in the one form each
// has.
var validSubscribes = []subscribeSpec{
	// MQTT 3.1.1 3.8.2.1 Figure 3.21, "a Packet Identifier of 10", and
	// 3.8.3.1 Table 3.5: "a/b" at Requested QoS 1, "c/d" at 2.
	{name: "MQTT 3.1.1, the standard's example", version: packet.V311, id: 10, subs: []packet.Subscription{sub("a/b", 1, false, false, 0), sub("c/d", 2, false, false, 0)}},
	// A Packet Identifier is a Two Byte Integer (2.3.1 Figure 2.3); QoS 0.
	{name: "MQTT 3.1.1, one filter at QoS 0", version: packet.V311, id: 0xFFFF, subs: []packet.Subscription{sub("sport/tennis/#", 0, false, false, 0)}},
	// MQTT 5 3.8.2 Figure 3-19, "a Packet Identifier of 10 and no
	// properties", and 3.8.3.1 Figure 3-21: "a/b" with QoS 1, "c/d" with
	// QoS 2.
	{name: "MQTT 5, the standard's example", version: packet.V5, id: 10, subs: []packet.Subscription{sub("a/b", 1, false, false, 0), sub("c/d", 2, false, false, 0)}},
	// 3.8.3.1: every option at every value it may take, Retain Handling 0,
	// 1 and 2 among them; 3.8.2.1.2 and 3.8.2.1.3: a Subscription
	// Identifier and a User Property twice under one name ("The same name
	// is allowed to appear more than once").
	{
		name: "MQTT 5, every option and property", version: packet.V5, id: 0x1234,
		props: []wired{intProp(0x0B, 321), userProp("n", "v"), userProp("n", "w")},
		subs: []packet.Subscription{
			sub("a", 0, true, true, 0), sub("b", 1, false, true, 1), sub("c", 2, true, false, 2),
			sub("A\U0002A6D4/+/#", 1, false, false, 2),
		},
	},
	// 3.8.2.1.2: "The Subscription Identifier can have the value of 1 to
	// 268,435,455"; 1.5.4: a string of up to 65,535 bytes.
	{name: "MQTT 5, the largest Subscription Identifier", version: packet.V5, id: 1, props: []wired{intProp(0x0B, maxVarInt)}, subs: []packet.Subscription{sub("x", 2, true, true, 2)}},
	{name: "MQTT 5, a filter of 65,535 bytes", version: packet.V5, id: 1, subs: []packet.Subscription{sub(strings.Repeat("a", 65_535), 1, false, false, 0)}},
	{name: "MQTT 3.1.1, a filter of 65,535 bytes", version: packet.V311, id: 1, subs: []packet.Subscription{sub(strings.Repeat("a", 65_535), 1, false, false, 0)}},
}

// TestSubscribe decodes each of validSubscribes into its fields and reads
// its subscriptions back through All.
func TestSubscribe(t *testing.T) {
	testRoundTrips(t, reading(casesOf(validSubscribes, (*subscribeSpec).caseOf)))
	for i := range validSubscribes {
		s := &validSubscribes[i]
		got, _ := packet.DecodeSubscribe(s.body(), s.version) // testRoundTrips checks the decoding
		if subs := slices.Collect(got.Subscriptions.All()); !sameSubscriptionList(subs, s.subs) {
			t.Errorf("%s: All reads %+v; want %+v", s.name, subs, s.subs)
		}
	}
}

// TestSubscriptionsAllStops: All is an iter.Seq, which its consumer may
// stop at any subscription, as a range loop's break does; once yield
// returns false, All yields nothing more (stopsWhenAsked).
func TestSubscriptionsAllStops(t *testing.T) {
	for i := range validSubscribes {
		s := &validSubscribes[i]
		stopsWhenAsked(t, s.name, s.want().Subscriptions.All(), len(s.subs))
	}
}

// TestSubscribePacketIdentifier: a Client "MUST assign it a non-zero
// Packet Identifier" to each new SUBSCRIBE (MQTT 5 [MQTT-2.2.1-3]); MQTT
// 3.1.1: SUBSCRIBE "MUST contain a non-zero 16-bit Packet Identifier"
// [MQTT-2.3.1-1]. Neither names a class: the decoder refuses Packet
// Identifier 0 with a packet error.
func TestSubscribePacketIdentifier(t *testing.T) {
	testPacketIdentifierZero(t, reading(casesOf(validSubscribes, (*subscribeSpec).caseOf)))
}

// TestSubscribeNoPayload: "The Payload MUST contain at least one Topic
// Filter and Subscription Options pair [MQTT-3.8.3-2]. A SUBSCRIBE packet
// with no Payload is a Protocol Error." (MQTT 5 3.8.3). MQTT 3.1.1 3.8.3:
// "A SUBSCRIBE packet with no payload is a protocol violation
// [MQTT-3.8.3-3]", naming no class. An MQTT 5 body of the Packet
// Identifier alone also lacks the Property Length, which cannot be
// omitted, so it is refused without a class.
func TestSubscribeNoPayload(t *testing.T) {
	testNoPayload(t, subscribeCodec.reader(), bothVersions, packet.ErrProtocol, []wired{intProp(0x0B, 1), userProp("n", "v")}, func(payload []byte) packet.Subscribe {
		return packet.Subscribe{PacketID: 1, Subscriptions: payload}
	})
}

// TestSubscribeTopicFilterString: "The Topic Filters MUST be a UTF-8
// Encoded String [MQTT-3.8.3-1]" (MQTT 5 3.8.3; MQTT 3.1.1 [MQTT-3.8.3-1]).
// Ill-formed UTF-8 or a surrogate [MQTT-1.5.4-1], or U+0000
// [MQTT-1.5.4-2], "is a Malformed Packet" (MQTT 5 1.5.4); MQTT 3.1.1 1.5.3
// closes the connection [MQTT-1.5.3-1], [MQTT-1.5.3-2], naming no class.
// The decoder refuses such a filter wherever it stands in the list.
func TestSubscribeTopicFilterString(t *testing.T) {
	bad := slices.Concat(illFormed, surrogates, nulls)
	ok := sub("a/b", 1, false, false, 0)
	for _, version := range bothVersions {
		for _, filter := range bad {
			s := packet.Subscription{Filter: filter, QoS: 1}
			for _, list := range [][]packet.Subscription{{s}, {ok, s}, {s, ok}} {
				spec := subscribeSpec{version: version, id: 1, subs: list}
				got, err := packet.DecodeSubscribe(spec.body(), version)
				switch {
				case version == packet.V5 && !errors.Is(err, packet.ErrMalformed):
					t.Errorf("MQTT 5, filters %q: got %+v, %v; want ErrMalformed", filtersOf(list), got, err)
				case !isKind(err):
					t.Errorf("MQTT %v, filters %q: got %+v, %v; want ErrMalformed or ErrProtocol", version, filtersOf(list), got, err)
				}
			}
		}
	}
}

// filtersOf returns the filters of a list, for messages.
func filtersOf(list []packet.Subscription) []string {
	filters := make([]string, len(list))
	for i := range list {
		filters[i] = string(list[i].Filter)
	}
	return filters
}

// refusedSubscribeProps are SUBSCRIBE property sets the standard forbids,
// beyond subscribeValueRules.
var refusedSubscribeProps = []propertyCase{
	// 3.8.2.1.2: "It is a Protocol Error to include the Subscription
	// Identifier more than once."
	{"Subscription Identifier twice", []wired{intProp(0x0B, 1), intProp(0x0B, 2)}, packet.ErrProtocol},
	{"Subscription Identifier twice, a User Property between", []wired{intProp(0x0B, 1), userProp("n", "v"), intProp(0x0B, 1)}, packet.ErrProtocol},
	// 3.8.2.1.2: "can have the value of 1 to 268,435,455", a Variable Byte
	// Integer (1.5.5: at most four bytes). A fifth byte is "a value not of
	// the specified data type", a Malformed Packet (2.2.2.2).
	{"Subscription Identifier of 268,435,456", []wired{{[]byte{0x0B, 0x80, 0x80, 0x80, 0x80, 0x01}, packet.Property{ID: 0x0B, Int: maxVarInt + 1}}}, packet.ErrMalformed},
	// 2.2.2.2: "A Control Packet which contains an Identifier which is not
	// valid for its packet type ... is a Malformed Packet." Table 2-4 gives
	// the Reason String to the acknowledgements and the Topic Alias to
	// PUBLISH.
	{"Reason String, a SUBACK property", []wired{stringProp(0x1F, "r")}, packet.ErrMalformed},
	{"Topic Alias, a PUBLISH property", []wired{intProp(0x23, 1)}, packet.ErrMalformed},
	// 3.8.2.1.3: the User Property is a UTF-8 String Pair [MQTT-1.5.7-1];
	// U+0000 [MQTT-1.5.4-2] "is a Malformed Packet".
	{"a User Property name holding U+0000", []wired{userProp("a\x00", "v")}, packet.ErrMalformed},
}

// TestSubscribeProperties checks the rules of 3.8.2.1 (subscribeValueRules,
// refusedSubscribeProps): the decoder refuses with the class the section
// names, and NewProperties refuses to make the section, with
// ErrCannotEncode and no section. The values the rules allow decode and are
// made into exactly their wire bytes.
func TestSubscribeProperties(t *testing.T) {
	testProperties(t, subscribeCodec.reader(), propsCases(subscribeValueRules, refusedSubscribeProps), func(c *propsCase) ([]byte, packet.Subscribe) {
		s := subscribeSpec{version: packet.V5, id: 1, props: c.props, subs: []packet.Subscription{sub("a/b", 1, false, false, 0)}}
		return s.body(), s.want()
	})
}

// FuzzDecodeSubscribe feeds random bytes to DecodeSubscribe under both
// versions, through the shared body (codec.fuzzDecode). It must never panic; it
// refuses with a packet error; what it accepts obeys subscribeOK and writes
// back as exactly the packet it came from, a SUBSCRIBE having one form
// (design.md, Testing: "Code that reads bytes or text from outside Sagüin is
// fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files").
func FuzzDecodeSubscribe(f *testing.F) {
	for i := range validSubscribes {
		s := &validSubscribes[i]
		if body := s.body(); len(body) < 256 {
			f.Add(body, s.version == packet.V5)
		}
	}
	f.Add([]byte{0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x03}, true)             // Maximum QoS 3
	f.Add([]byte{0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x30}, true)             // Retain Handling 3
	f.Add([]byte{0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x40}, true)             // a reserved bit
	f.Add([]byte{0x00, 0x01, 0x00, 0x01, 'a', 0x04}, false)                  // No Local in MQTT 3.1.1
	f.Add([]byte{0x00, 0x01, 0x02, 0x0B, 0x00, 0x00, 0x01, 'a', 0x00}, true) // Subscription Identifier 0
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 'a', 0x00}, true)             // Packet Identifier 0
	f.Add([]byte{0x00, 0x01, 0x00}, true)                                    // no payload
	f.Add([]byte{0x00, 0x01, 0x00, 0x02, 0xC0, 0x80, 0x00}, false)           // an ill-formed filter
	f.Fuzz(func(t *testing.T, data []byte, v5 bool) {
		subscribeCodec.fuzzDecode(t, versionOf(v5), data)
	})
}
