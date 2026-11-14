package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// place is where a set of properties is carried (MQTT 5 2.2.2): the variable
// header of a packet type, or the Will Properties in a CONNECT's payload.
type place struct {
	name string
	typ  packet.Type // for a packet; unused for Will Properties
	will bool
}

// places are the fifteen packet types in Table 2-1's order, then Will
// Properties. PINGREQ and PINGRESP carry no properties: no row of Table 2-4
// names them.
var places = []place{
	{"CONNECT", packet.CONNECT, false},
	{"CONNACK", packet.CONNACK, false},
	{"PUBLISH", packet.PUBLISH, false},
	{"PUBACK", packet.PUBACK, false},
	{"PUBREC", packet.PUBREC, false},
	{"PUBREL", packet.PUBREL, false},
	{"PUBCOMP", packet.PUBCOMP, false},
	{"SUBSCRIBE", packet.SUBSCRIBE, false},
	{"SUBACK", packet.SUBACK, false},
	{"UNSUBSCRIBE", packet.UNSUBSCRIBE, false},
	{"UNSUBACK", packet.UNSUBACK, false},
	{"PINGREQ", packet.PINGREQ, false},
	{"PINGRESP", packet.PINGRESP, false},
	{"DISCONNECT", packet.DISCONNECT, false},
	{"AUTH", packet.AUTH, false},
	{"Will Properties", 0, true},
}

// decode reads a set of properties carried in p, and returns each property
// the set holds (Properties.All).
func (p place) decode(b []byte) ([]packet.Property, []byte, error) {
	props, rest, err := p.decodeSection(b)
	return values(props), rest, err
}

// decodeSection reads a set of properties carried in p, and returns the
// section as the decoder keeps it.
func (p place) decodeSection(b []byte) (packet.Properties, []byte, error) {
	if p.will {
		return packet.DecodeWillProperties(b)
	}
	return packet.DecodeProperties(b, p.typ)
}

// encode writes a set of properties to be carried in p.
func (p place) encode(dst []byte, props []packet.Property) ([]byte, error) {
	if p.will {
		return packet.AppendWillProperties(dst, props)
	}
	return packet.AppendProperties(dst, p.typ, props)
}

// placeNamed finds a place by its name.
var placeNamed = func() map[string]place {
	m := make(map[string]place, len(places))
	for _, p := range places {
		m[p.name] = p
	}
	return m
}()

// packetPlaces are the places that are packets: all but Will Properties.
var packetPlaces = slices.DeleteFunc(slices.Clone(places), func(p place) bool { return p.will })

// listed holds each comma-separated list of places that table24 and
// mayRepeat name, split once: in runs for every property of every fuzz
// input.
var listed = func() map[string]map[string]bool {
	m := map[string]map[string]bool{}
	add := func(list string) {
		names := map[string]bool{}
		for name := range strings.SplitSeq(list, ", ") {
			names[name] = true
		}
		m[list] = names
	}
	for _, r := range table24 {
		add(r.where)
	}
	for _, list := range mayRepeat {
		add(list)
	}
	add("")
	return m
}()

// in reports whether list, a column of Table 2-4 or a value of mayRepeat,
// names p.
func (p place) in(list string) bool {
	names, ok := listed[list]
	if !ok {
		panic("place.in: a list neither table24 nor mayRepeat holds: " + list)
	}
	return names[p.name]
}

// table24 is MQTT 5 2.2.2.2 Table 2-4 - Properties, copied row by row: the
// identifier, the name, the Type column and the Packet / Will Properties
// column, in the standard's own words.
var table24 = []struct {
	id    byte
	name  string
	typ   string
	where string
}{
	{0x01, "Payload Format Indicator", "Byte", "PUBLISH, Will Properties"},
	{0x02, "Message Expiry Interval", "Four Byte Integer", "PUBLISH, Will Properties"},
	{0x03, "Content Type", "UTF-8 Encoded String", "PUBLISH, Will Properties"},
	{0x08, "Response Topic", "UTF-8 Encoded String", "PUBLISH, Will Properties"},
	{0x09, "Correlation Data", "Binary Data", "PUBLISH, Will Properties"},
	{0x0B, "Subscription Identifier", "Variable Byte Integer", "PUBLISH, SUBSCRIBE"},
	{0x11, "Session Expiry Interval", "Four Byte Integer", "CONNECT, CONNACK, DISCONNECT"},
	{0x12, "Assigned Client Identifier", "UTF-8 Encoded String", "CONNACK"},
	{0x13, "Server Keep Alive", "Two Byte Integer", "CONNACK"},
	{0x15, "Authentication Method", "UTF-8 Encoded String", "CONNECT, CONNACK, AUTH"},
	{0x16, "Authentication Data", "Binary Data", "CONNECT, CONNACK, AUTH"},
	{0x17, "Request Problem Information", "Byte", "CONNECT"},
	{0x18, "Will Delay Interval", "Four Byte Integer", "Will Properties"},
	{0x19, "Request Response Information", "Byte", "CONNECT"},
	{0x1A, "Response Information", "UTF-8 Encoded String", "CONNACK"},
	{0x1C, "Server Reference", "UTF-8 Encoded String", "CONNACK, DISCONNECT"},
	{0x1F, "Reason String", "UTF-8 Encoded String", "CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH"},
	{0x21, "Receive Maximum", "Two Byte Integer", "CONNECT, CONNACK"},
	{0x22, "Topic Alias Maximum", "Two Byte Integer", "CONNECT, CONNACK"},
	{0x23, "Topic Alias", "Two Byte Integer", "PUBLISH"},
	{0x24, "Maximum QoS", "Byte", "CONNACK"},
	{0x25, "Retain Available", "Byte", "CONNACK"},
	{0x26, "User Property", "UTF-8 String Pair", "CONNECT, CONNACK, PUBLISH, Will Properties, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH"},
	{0x27, "Maximum Packet Size", "Four Byte Integer", "CONNECT, CONNACK"},
	{0x28, "Wildcard Subscription Available", "Byte", "CONNACK"},
	{0x29, "Subscription Identifier Available", "Byte", "CONNACK"},
	{0x2A, "Shared Subscription Available", "Byte", "CONNACK"},
}

// rowOf finds an identifier's row in table24.
var rowOf = func() map[byte]int {
	m := map[byte]int{}
	for i, r := range table24 {
		m[r.id] = i
	}
	return m
}()

// mayRepeat names, by identifier, where a property may appear more than once.
// User Property: "The User Property is allowed to appear multiple times to
// represent multiple name, value pairs. The same name is allowed to appear
// more than once." (3.1.2.11.8, 3.1.3.2.8, 3.2.2.3.10, 3.3.2.3.7, 3.4.2.2.3,
// 3.5.2.2.3, 3.6.2.2.3, 3.7.2.2.3, 3.8.2.1.3, 3.9.2.1.3, 3.10.2.1.2,
// 3.11.2.1.3, 3.14.2.2.4, 3.15.2.2.5). Subscription Identifier in PUBLISH:
// "Multiple Subscription Identifiers will be included if the publication is
// the result of a match to more than one subscription" (3.3.2.3.8).
//
// Every other property, everywhere it may be carried, is under a sentence of
// the form "It is a Protocol Error to include the ... more than once":
// CONNECT 3.1.2.11.2-7, 3.1.2.11.9-10; Will Properties 3.1.3.2.2-7; CONNACK
// 3.2.2.3.2-9, 3.2.2.3.11-18; PUBLISH 3.3.2.3.4-6, 3.3.2.3.9; PUBACK
// 3.4.2.2.2; PUBREC 3.5.2.2.2; PUBREL 3.6.2.2.2; PUBCOMP 3.7.2.2.2; SUBSCRIBE
// 3.8.2.1.2; SUBACK 3.9.2.1.2; UNSUBACK 3.11.2.1.2; DISCONNECT 3.14.2.2.2-3,
// 3.14.2.2.5; AUTH 3.15.2.2.2-4.
//
// The two the standard leaves unsaid, Payload Format Indicator and Message
// Expiry Interval in PUBLISH (3.3.2.3.2, 3.3.2.3.3), the design settles
// (Choices MQTT leaves open): "Payload Format Indicator or Message Expiry
// Interval twice in one PUBLISH, which MQTT 5 does not forbid | a Protocol
// Error, as for every other property that may appear once".
var mayRepeat = map[byte]string{
	0x26: "CONNECT, CONNACK, PUBLISH, Will Properties, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH",
	0x0B: "PUBLISH",
}

// set returns properties behind their Property Length (MQTT 5 2.2.2.1), a
// Variable Byte Integer: the standard library's unsigned varint, so these
// tests build their inputs without the code under test.
func set(props ...[]byte) []byte {
	body := slices.Concat(props...)
	return append(binary.AppendUvarint(nil, uint64(len(body))), body...)
}

// sample returns one property of Table 2-4's type typ, as wire bytes and as
// the Property that decodes from them. Each value is legal for every property
// of its type (no zero where a property forbids one).
func sample(id byte, typ string) ([]byte, packet.Property) {
	p := packet.Property{ID: id}
	switch typ {
	case "Byte":
		p.Int = 0x01
		return []byte{id, 0x01}, p
	case "Two Byte Integer":
		p.Int = 0x1234
		return []byte{id, 0x12, 0x34}, p
	case "Four Byte Integer":
		p.Int = 0x01020304
		return []byte{id, 0x01, 0x02, 0x03, 0x04}, p
	case "Variable Byte Integer":
		p.Int = 321
		return []byte{id, 0xC1, 0x02}, p
	case "UTF-8 Encoded String":
		p.String = "A\U0002A6D4"
		return []byte{id, 0x00, 0x05, 0x41, 0xF0, 0xAA, 0x9B, 0x94}, p
	case "Binary Data":
		p.Binary = []byte{0x00, 0xFF}
		return []byte{id, 0x00, 0x02, 0x00, 0xFF}, p
	case "UTF-8 String Pair":
		p.Pair = packet.StringPair{Name: "n", Value: "v"}
		return []byte{id, 0x00, 0x01, 'n', 0x00, 0x01, 'v'}, p
	}
	panic("no sample for type " + typ)
}

// userProperty returns a User Property named name with value value, as wire
// bytes and as a Property.
func userProperty(name, value string) ([]byte, packet.Property) {
	wire := append([]byte{0x26}, prefixed([]byte(name))...)
	wire = append(wire, prefixed([]byte(value))...)
	return wire, packet.Property{ID: 0x26, Pair: packet.StringPair{Name: name, Value: value}}
}

// same reports whether two properties hold the same identifier and value.
func same(a, b *packet.Property) bool {
	return a.ID == b.ID && a.Int == b.Int && a.String == b.String && a.Pair == b.Pair && bytes.Equal(a.Binary, b.Binary)
}

// sameList reports whether two lists hold the same properties in the same
// order. It compares in place: slices.EqualFunc would copy every 80-byte
// Property into its callback.
func sameList(a, b []packet.Property) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !same(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// fromTheWire are the places the broker reads today, whose property rules
// the conformance tests check from the wire, row by row
// (harness.BadPackets, TestBadPacketsAreRefused): this file's tests check
// the other places, whose packets are read once their features are built.
var fromTheWire = map[string]bool{"CONNECT": true, "Will Properties": true, "DISCONNECT": true}

// TestPropertyTable checks every one-byte identifier, 0 to 127, in every
// place but fromTheWire, against Table 2-4. Where the table lets the place carry it, a value
// of the table's type decodes into the Property field for that type, and the
// bytes after the set are handed back untouched. Elsewhere, and for every
// identifier the table lacks, MQTT 5 2.2.2.2: "A Control Packet which
// contains an Identifier which is not valid for its packet type, or contains
// a value not of the specified data type, is a Malformed Packet."
func TestPropertyTable(t *testing.T) {
	if len(table24) != 27 {
		t.Fatalf("table24 has %d rows; Table 2-4 has 27", len(table24))
	}
	for id := range 128 {
		for _, p := range places {
			if fromTheWire[p.name] {
				continue
			}
			i, known := rowOf[byte(id)]
			if !known {
				in := set([]byte{byte(id), 0x01})
				if props, _, err := p.decode(in); !errors.Is(err, packet.ErrMalformed) {
					t.Errorf("%s, identifier %#02x (not in Table 2-4): got %+v, %v; want ErrMalformed", p.name, id, props, err)
				}
				continue
			}
			r := table24[i]
			wire, want := sample(r.id, r.typ)
			in := append(set(wire), 0xAA)
			props, rest, err := p.decode(in)
			if !p.in(r.where) {
				if !errors.Is(err, packet.ErrMalformed) {
					t.Errorf("%s, %s (%#02x, not allowed here): got %+v, %v; want ErrMalformed", p.name, r.name, r.id, props, err)
				}
				continue
			}
			if err != nil || len(props) != 1 || !same(&props[0], &want) || !bytes.Equal(rest, []byte{0xAA}) {
				t.Errorf("%s, %s (%#02x, %s) from % X: got %+v, % X, %v; want [%+v], AA, nil", p.name, r.name, r.id, r.typ, in, props, rest, err, want)
			}
		}
	}
}

// TestPropertyRepeats includes every property twice, everywhere it may be
// carried but fromTheWire, side by side and with another property between. Where it may
// appear more than once (mayRepeat) both are kept, in order; everywhere
// else, by the sentences and the design row cited at mayRepeat, it is a
// Protocol Error: Payload Format Indicator and Message Expiry Interval in
// PUBLISH included.
func TestPropertyRepeats(t *testing.T) {
	user, userWant := userProperty("n", "v")
	for _, r := range table24 {
		wire, want := sample(r.id, r.typ)
		for _, p := range places {
			if !p.in(r.where) || fromTheWire[p.name] {
				continue
			}
			cases := []struct {
				in   []byte
				want []packet.Property
			}{
				{set(wire, wire), []packet.Property{want, want}},
				{set(wire, user, wire), []packet.Property{want, userWant, want}},
			}
			for _, c := range cases {
				props, _, err := p.decode(c.in)
				if p.in(mayRepeat[r.id]) {
					if err != nil || !sameList(props, c.want) {
						t.Errorf("%s, %s repeated, % X: got %+v, %v; want %+v, nil", p.name, r.name, c.in, props, err, c.want)
					}
				} else if !errors.Is(err, packet.ErrProtocol) {
					t.Errorf("%s, %s repeated, % X: got %+v, %v; want ErrProtocol", p.name, r.name, c.in, props, err)
				}
			}
		}
	}
}

// TestPropertyOrder keeps properties in the order received, User Properties
// above all: "The Server MUST maintain the order of User Properties when
// forwarding the Application Message [MQTT-3.3.2-18]", and when publishing
// the Will Message [MQTT-3.1.3-10]. The same name may come more than once
// (3.3.2.3.7).
func TestPropertyOrder(t *testing.T) {
	a1, a1Want := userProperty("a", "1")
	b2, b2Want := userProperty("b", "2")
	a3, a3Want := userProperty("a", "3")
	ct, ctWant := sample(0x03, "UTF-8 Encoded String")
	si, siWant := sample(0x0B, "Variable Byte Integer")
	si1 := []byte{0x0B, 0x01}
	si1Want := packet.Property{ID: 0x0B, Int: 1}
	pl := []struct {
		p    place
		in   []byte
		want []packet.Property
	}{
		{placeNamed["PUBLISH"], set(a1, ct, b2, si, a3, si1), []packet.Property{a1Want, ctWant, b2Want, siWant, a3Want, si1Want}},
		{placeNamed["Will Properties"], set(b2, a1, ct, a3), []packet.Property{b2Want, a1Want, ctWant, a3Want}},
	}
	for _, c := range pl {
		props, _, err := c.p.decode(c.in)
		if err != nil || !sameList(props, c.want) {
			t.Errorf("%s, % X: got %+v, %v; want %+v, nil", c.p.name, c.in, props, err, c.want)
		}
	}
}

// TestPropertyLength checks the Property Length. MQTT 5 2.2.2.1: "The
// Property Length is encoded as a Variable Byte Integer. The Property Length
// does not include the bytes used to encode itself, but includes the length
// of the Properties. If there are no properties, this MUST be indicated by
// including a Property Length of zero [MQTT-2.2.2-1]." A length that runs
// past the bytes there are, or that ends inside a property, leaves a packet
// that "cannot be parsed" (1.2): Malformed. The bytes after the length belong
// to the rest of the packet, so a property may not borrow them.
func TestPropertyLength(t *testing.T) {
	for _, p := range places {
		if p.name == "PINGREQ" || p.name == "PINGRESP" || fromTheWire[p.name] {
			continue
		}
		props, rest, err := p.decode([]byte{0x00, 0xAA})
		if err != nil || len(props) != 0 || !bytes.Equal(rest, []byte{0xAA}) {
			t.Errorf("%s, 00 AA: got %+v, % X, %v; want no properties, AA, nil", p.name, props, rest, err)
		}
	}

	publish := placeNamed["PUBLISH"]
	// Twenty User Properties, 140 bytes: a Property Length of two bytes.
	many := make([][]byte, 0, 20)
	manyWant := make([]packet.Property, 0, 20)
	for i := range 20 {
		wire, want := userProperty(strconv.Itoa(i%10), "v")
		many = append(many, wire)
		manyWant = append(manyWant, want)
	}
	in := append(set(many...), 0xAA)
	if props, rest, err := publish.decode(in); err != nil || !sameList(props, manyWant) || !bytes.Equal(rest, []byte{0xAA}) {
		t.Errorf("PUBLISH, twenty User Properties (length % X): got %d properties, % X, %v; want 20, AA, nil", in[:2], len(props), rest, err)
	}

	malformed := [][]byte{
		{},
		{0x80},
		{0x05, 0x01, 0x01},                   // the length runs past the end
		{0x02, 0x02, 0x00, 0x00, 0x00, 0x01}, // ends inside a Four Byte Integer
		{0x03, 0x03, 0x00, 0x05, 'h', 'e', 'l', 'l'},   // ends inside a string
		{0x01, 0x26, 0x00, 0x01, 'n', 0x00, 0x01, 'v'}, // ends after the identifier
		{0x80, 0x80, 0x80, 0x80, 0x01},                 // a fifth byte
	}
	for _, in := range malformed {
		if props, _, err := publish.decode(in); !errors.Is(err, packet.ErrMalformed) {
			t.Errorf("PUBLISH, % X: got %+v, %v; want ErrMalformed", in, props, err)
		}
	}

	// A length in more bytes than it needs [MQTT-1.5.5-1]: 0, and 2 before a
	// well-formed Payload Format Indicator. The kind is not named.
	for _, in := range [][]byte{{0x80, 0x00}, {0x82, 0x00, 0x01, 0x01}} {
		props, _, err := publish.decode(in)
		if err == nil {
			t.Errorf("PUBLISH, % X: non-minimal Property Length accepted as %+v", in, props)
		} else if !isKind(err) {
			t.Errorf("PUBLISH, % X: err = %v, want ErrMalformed or ErrProtocol", in, err)
		}
	}
}

// TestPropertyValueType refuses a value not of its property's type, or cut
// short inside the Property Length while bytes follow the set. MQTT 5
// 2.2.2.2: a packet that "contains a value not of the specified data type, is
// a Malformed Packet"; strings and string pairs follow 1.5.4 and 1.5.7
// [MQTT-1.5.4-1], [MQTT-1.5.4-2], [MQTT-1.5.7-1]; a Variable Byte Integer has
// at most four bytes (1.5.5).
func TestPropertyValueType(t *testing.T) {
	publish, suback, subscribe, puback := placeNamed["PUBLISH"], placeNamed["SUBACK"], placeNamed["SUBSCRIBE"], placeNamed["PUBACK"]
	cases := []struct {
		p  place
		in []byte
	}{
		{publish, set([]byte{0x03, 0x00, 0x01, 0xC0})},                    // Content Type, ill-formed
		{publish, set([]byte{0x08, 0x00, 0x02, 0x41, 0x00})},              // Response Topic, U+0000
		{publish, set([]byte{0x03, 0x00, 0x03, 0xED, 0xA0, 0x80})},        // Content Type, a surrogate
		{puback, set([]byte{0x1F, 0x00, 0x02, 0xC3, 0x28})},               // Reason String, ill-formed
		{suback, set([]byte{0x26, 0x00, 0x01, 0x00, 0x00, 0x01, 'v'})},    // User Property, U+0000 in the name
		{subscribe, set([]byte{0x26, 0x00, 0x01, 'n', 0x00, 0x01, 0xFF})}, // User Property, ill-formed value
		{subscribe, set([]byte{0x0B, 0x80, 0x80, 0x80, 0x80, 0x01})},      // Subscription Identifier, five bytes
		// Cut short: the bytes after the set must not complete them.
		{publish, append(set([]byte{0x01}), 0xAA, 0xAA, 0xAA)},                   // Byte
		{publish, append(set([]byte{0x23, 0x00}), 0xAA, 0xAA, 0xAA)},             // Two Byte Integer
		{publish, append(set([]byte{0x02, 0x00, 0x00, 0x00}), 0xAA, 0xAA, 0xAA)}, // Four Byte Integer
		{publish, append(set([]byte{0x0B, 0x80}), 0x01, 0xAA, 0xAA)},             // Variable Byte Integer
		{publish, append(set([]byte{0x03, 0x00, 0x02, 'a'}), 'a', 0xAA, 0xAA)},   // UTF-8 Encoded String
		{publish, append(set([]byte{0x09, 0x00, 0x03, 0x01}), 0xAA, 0xAA, 0xAA)}, // Binary Data
		{publish, append(set([]byte{0x26, 0x00, 0x01, 'n', 0x00}), 0x01, 'v')},   // UTF-8 String Pair
	}
	for _, c := range cases {
		if props, _, err := c.p.decode(c.in); !errors.Is(err, packet.ErrMalformed) {
			t.Errorf("%s, % X: got %+v, %v; want ErrMalformed", c.p.name, c.in, props, err)
		}
	}

	// A Subscription Identifier of 1 in two bytes [MQTT-1.5.5-1]; the kind is
	// not named.
	in := set([]byte{0x0B, 0x81, 0x00})
	if props, _, err := subscribe.decode(in); err == nil {
		t.Errorf("SUBSCRIBE, % X: non-minimal Subscription Identifier accepted as %+v", in, props)
	} else if !isKind(err) {
		t.Errorf("SUBSCRIBE, % X: err = %v, want ErrMalformed or ErrProtocol", in, err)
	}
}

// TestPropertyIdentifier reads the identifier as what it is, a Variable Byte
// Integer (MQTT 5 2.2.2.2: "The Identifier is encoded as a Variable Byte
// Integer"). No identifier of Table 2-4 is above 127, so one of two bytes or
// more is "not valid for its packet type": Malformed. One that names a valid
// identifier in two bytes breaks [MQTT-1.5.5-1], whose kind is not named.
func TestPropertyIdentifier(t *testing.T) {
	publish := placeNamed["PUBLISH"]
	for _, in := range [][]byte{
		set([]byte{0x80, 0x01, 0x01}),                   // 128
		set([]byte{0xFF, 0x7F, 0x01}),                   // 16,383
		set([]byte{0xFF, 0xFF, 0xFF, 0x7F, 0x01}),       // 268,435,455
		set([]byte{0x80, 0x80, 0x80, 0x80, 0x01, 0x01}), // a fifth byte
	} {
		if props, _, err := publish.decode(in); !errors.Is(err, packet.ErrMalformed) {
			t.Errorf("PUBLISH, % X: got %+v, %v; want ErrMalformed", in, props, err)
		}
	}
	for _, in := range [][]byte{
		set([]byte{0x81, 0x00, 0x01}),                             // Payload Format Indicator
		set([]byte{0xA6, 0x00, 0x00, 0x01, 'n', 0x00, 0x01, 'v'}), // User Property
	} {
		props, _, err := publish.decode(in)
		if err == nil {
			t.Errorf("PUBLISH, % X: non-minimal identifier accepted as %+v", in, props)
		} else if !isKind(err) {
			t.Errorf("PUBLISH, % X: err = %v, want ErrMalformed or ErrProtocol", in, err)
		}
	}
}

// TestAppendProperties writes the Property Length, then each property as its
// identifier and its value in Table 2-4's type, in the order given (MQTT 5
// 2.2.2.1, 2.2.2.2), every property where Table 2-4 lets it be carried; no
// properties is a Property Length of zero [MQTT-2.2.2-1].
func TestAppendProperties(t *testing.T) {
	for _, p := range places {
		if p.name == "PINGREQ" || p.name == "PINGRESP" {
			continue
		}
		if got, err := p.encode([]byte{0xAA}, nil); err != nil || !bytes.Equal(got, []byte{0xAA, 0x00}) {
			t.Errorf("%s: encoding no properties after AA = % X, %v; want AA 00, nil", p.name, got, err)
		}
	}
	for _, r := range table24 {
		wire, prop := sample(r.id, r.typ)
		want := append([]byte{0xAA}, set(wire)...)
		for _, p := range places {
			if !p.in(r.where) {
				continue
			}
			if got, err := p.encode([]byte{0xAA}, []packet.Property{prop}); err != nil || !bytes.Equal(got, want) {
				t.Errorf("%s: encoding %s %+v after AA = % X, %v; want % X, nil", p.name, r.name, prop, got, err, want)
			}
		}
	}
	a1, a1Want := userProperty("a", "1")
	ct, ctWant := sample(0x03, "UTF-8 Encoded String")
	a2, a2Want := userProperty("a", "2")
	want := set(a1, ct, a2)
	if got, err := placeNamed["PUBLISH"].encode(nil, []packet.Property{a1Want, ctWant, a2Want}); err != nil || !bytes.Equal(got, want) {
		t.Errorf("PUBLISH: encoding three = % X, %v; want % X, nil", got, err, want)
	}
	many := make([][]byte, 0, 20)
	manyProps := make([]packet.Property, 0, 20)
	for range 20 {
		wire, p := userProperty("n", "v")
		many = append(many, wire)
		manyProps = append(manyProps, p)
	}
	want = set(many...)
	if got, err := placeNamed["PUBLISH"].encode(nil, manyProps); err != nil || !bytes.Equal(got, want) {
		t.Errorf("PUBLISH: encoding twenty, 140 bytes = % .8X..., %v; want % .8X..., nil", got, err, want)
	}
}

// obeys fails the test unless every property in props may be carried in p
// (Table 2-4) and none appears twice where the standard or the design
// forbids it (mayRepeat).
func obeys(t *testing.T, p place, props []packet.Property, read []byte) {
	t.Helper()
	seen := map[byte]bool{}
	for _, prop := range props {
		i, ok := rowOf[prop.ID]
		if !ok || !p.in(table24[i].where) {
			t.Fatalf("%s, % X: accepted identifier %#02x, which Table 2-4 does not allow here", p.name, read, prop.ID)
		}
		if seen[prop.ID] && !p.in(mayRepeat[prop.ID]) {
			t.Fatalf("%s, % X: accepted %s twice", p.name, read, table24[i].name)
		}
		seen[prop.ID] = true
	}
}

// seed returns one sample of every property p may carry.
func seed(p place) []byte {
	var props [][]byte
	for _, r := range table24 {
		if p.in(r.where) {
			wire, _ := sample(r.id, r.typ)
			props = append(props, wire)
		}
	}
	return set(props...)
}

// FuzzDecodeProperties feeds random bytes to DecodeProperties, for each of the
// fifteen packet types. It must never panic; it refuses with a packet error;
// what it accepts obeys Table 2-4 and the rules on repeats, and writes back as
// exactly the bytes it read (design.md, Testing: "Code that reads bytes or
// text from outside Sagüin is fuzzed, whether the input comes from clients, a
// bridge's peer or the operator's files").
func FuzzDecodeProperties(f *testing.F) {
	for i, p := range packetPlaces {
		f.Add(seed(p), byte(i))
	}
	f.Add([]byte{0x00}, byte(2))
	f.Add([]byte{0x80, 0x00}, byte(2))
	f.Add(set([]byte{0x01, 0x01}, []byte{0x01, 0x01}), byte(2))
	f.Add(set([]byte{0x02, 0x00, 0x00, 0x00, 0x01}, []byte{0x26, 0x00, 0x00, 0x00, 0x00}, []byte{0x02, 0x00, 0x00, 0x00, 0x01}), byte(2))
	f.Fuzz(func(t *testing.T, data []byte, which byte) {
		p := packetPlaces[int(which)%len(packetPlaces)]
		props, rest, err := p.decode(data)
		if err != nil {
			if !isKind(err) {
				t.Fatalf("%s, % X: err = %v, not a packet error", p.name, data, err)
			}
			return
		}
		read := consumed(t, data, rest)
		obeys(t, p, props, read)
		if got, err := p.encode(nil, props); err != nil || !bytes.Equal(got, read) {
			t.Fatalf("%s: read % X as %+v, which writes back as % X (err %v)", p.name, read, props, got, err)
		}
		section, _, _ := p.decodeSection(data)
		readsAndOmits(t, p, section)
	})
}

// FuzzDecodeWillProperties does the same for Will Properties.
func FuzzDecodeWillProperties(f *testing.F) {
	will := placeNamed["Will Properties"]
	f.Add(seed(will))
	f.Add([]byte{0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		props, rest, err := will.decode(data)
		if err != nil {
			if !isKind(err) {
				t.Fatalf("Will Properties, % X: err = %v, not a packet error", data, err)
			}
			return
		}
		read := consumed(t, data, rest)
		obeys(t, will, props, read)
		if got, err := will.encode(nil, props); err != nil || !bytes.Equal(got, read) {
			t.Fatalf("Will Properties: read % X as %+v, which writes back as % X (err %v)", read, props, got, err)
		}
		section, _, _ := will.decodeSection(data)
		readsAndOmits(t, will, section)
	})
}

// readsAndOmits fails the test unless a section p accepted reads back
// through Properties.All, and through Properties.Views, as exactly the
// properties an independent reading of its bytes finds (valuesOf, by Table
// 2-4's types), in wire order, repeats kept; and unless, for each identifier the section holds,
// Properties.Without leaves out every property with that identifier and
// keeps every other as it arrived, in order, without touching the section
// it was given.
func readsAndOmits(t *testing.T, p place, section packet.Properties) {
	t.Helper()
	want, ok := valuesOf(section)
	if !ok {
		t.Fatalf("%s: accepted % X, which is not a section of whole Table 2-4 properties", p.name, []byte(section))
	}
	if got := values(section); !sameList(got, want) {
		t.Fatalf("%s: % X reads back as %+v; want %+v", p.name, []byte(section), got, want)
	}
	if got := slices.Collect(section.Views()); !sameViews(got, want) {
		t.Fatalf("%s: % X reads back through Views as %+v; want %+v", p.name, []byte(section), got, want)
	}
	before := bytes.Clone(section)
	var omitted []byte
	for _, prop := range want {
		if slices.Contains(omitted, prop.ID) {
			continue
		}
		omitted = append(omitted, prop.ID)
		kept := slices.DeleteFunc(slices.Clone(want), func(k packet.Property) bool { return k.ID == prop.ID })
		wantBytes, _ := wireOf(kept)
		if got := section.Without(prop.ID); !bytes.Equal(got, wantBytes) {
			t.Fatalf("%s: % X without %#02x is % X; want % X", p.name, before, prop.ID, []byte(got), []byte(wantBytes))
		}
		if !bytes.Equal(section, before) {
			t.Fatalf("%s: Without(%#02x) changed the section it was given from % X to % X", p.name, prop.ID, before, []byte(section))
		}
	}
}

// readsBack fails the test unless props are exactly the wire bytes of ws,
// as they arrived, and yield ws's values one by one, in order (sameValue).
func readsBack(t *testing.T, what string, props packet.Properties, ws []wired) {
	t.Helper()
	if want := propsOf(ws); !bytes.Equal(props, want) {
		t.Errorf("%s: kept % .20X... (%d bytes); want the %d bytes that arrived", what, []byte(props), len(props), len(want))
	}
	got := values(props)
	if len(got) != len(ws) {
		t.Fatalf("%s: read back %d properties; want %d", what, len(got), len(ws))
	}
	for i := range ws {
		if !sameValue(&got[i], &ws[i].prop) {
			t.Fatalf("%s: property %d reads back as %+v; want %+v", what, i, got[i], ws[i].prop)
		}
	}
}

// manyUserProps returns n User Properties with names and values that
// repeat, the same name and the same pair coming back many times, with
// other properties of place among them.
func manyUserProps(n int, others ...wired) []wired {
	ws := make([]wired, 0, n+len(others))
	for i := range n {
		if i%(n/(len(others)+1)) == 0 && i > 0 && len(others) > 0 {
			ws, others = append(ws, others[0]), others[1:]
		}
		ws = append(ws, userProp([]string{"a", "b", "a"}[i%3], strconv.Itoa(i%7)))
	}
	return append(ws, others...)
}

// TestDecodedPropertiesReadBack: properties kept as the bytes they arrived
// in (TestDecodedPropertiesCostTheirWireBytes) still read back exactly,
// through Properties.All: every one, in the order sent, each User Property
// kept however often its name or pair repeats. MQTT 5 2.2.2.2: "The User
// Property is allowed to appear multiple times to represent multiple name,
// value pairs. The same name is allowed to appear more than once." The
// order is kept: the Server "MUST maintain the order of User Properties"
// when publishing a Will Message [MQTT-3.1.3-10] and when forwarding an
// Application Message [MQTT-3.3.2-18].
func TestDecodedPropertiesReadBack(t *testing.T) {
	props := manyUserProps(300, intProp(0x11, 3600), intProp(0x21, 20), binaryProp(0x16, []byte{0x00, 0xFF}), stringProp(0x15, "m"))
	willProps := manyUserProps(300, intProp(0x18, 5), stringProp(0x03, "text/plain"), intProp(0x01, 1))
	s := connectSpec{
		level: 5, flags: cleanStart | willFlag, keepAlive: 60, props: props,
		clientID: "c", willProps: willProps, willTopic: "t", willPayload: []byte("p"),
	}
	c, err := packet.DecodeConnect(s.body())
	if err != nil {
		t.Fatalf("DecodeConnect: %v", err)
	}
	readsBack(t, "CONNECT properties", c.Properties, props)
	readsBack(t, "Will Properties", c.Will.Properties, willProps)

	// A PUBLISH may also repeat the Subscription Identifier (3.3.2.3.8).
	publish := manyUserProps(300, intProp(0x0B, 1), stringProp(0x08, "reply"), intProp(0x0B, 268_435_455), intProp(0x23, 7))
	section := setOf(publish)
	got, rest, err := packet.DecodeProperties(append(section, 0xAA), packet.PUBLISH)
	if err != nil || !bytes.Equal(rest, []byte{0xAA}) {
		t.Fatalf("DecodeProperties(PUBLISH): rest % X, %v; want AA, nil", rest, err)
	}
	readsBack(t, "PUBLISH properties", got, publish)
}

// everyProperty returns a section's properties holding every property
// Table 2-4 lets p carry, each of its type (sample), with a User Property
// after each where p may carry one, and a second Subscription Identifier
// where PUBLISH may repeat it (3.3.2.3.8); and the identifiers of the
// table's rows it holds, in order.
func everyProperty(p place) (ws []wired, ids []byte) {
	for _, r := range table24 {
		if !p.in(r.where) {
			continue
		}
		wire, prop := sample(r.id, r.typ)
		ws = append(ws, wired{wire, prop})
		ids = append(ids, r.id)
		if p.in(table24[rowOf[packet.UserProperty]].where) {
			ws = append(ws, userProp(r.name, strconv.Itoa(len(ws))))
		}
	}
	if p.in(mayRepeat[packet.SubscriptionIdentifier]) {
		ws = append(ws, intProp(packet.SubscriptionIdentifier, 268_435_455))
	}
	return ws, ids
}

// TestPropertiesWithout: Properties.Without leaves out every property whose
// identifier it is given, however often it repeats, and keeps every other
// property as the bytes it arrived in, in the order received: the order the
// Server "MUST maintain" for User Properties [MQTT-3.1.3-10]
// [MQTT-3.3.2-18]. What it returns is still a property section the same
// place accepts (MQTT 5 2.2.2), and the section it was given is left as it
// was: a connection's writer trims a packet the core handed it, which it
// never changes (design.md, Execution, Communication: "Once a buffer is sent,
// its sender never touches it again"). Each place holds every property Table
// 2-4 lets it carry, with User Properties before, between and after them, and
// a second Subscription Identifier where PUBLISH may repeat it (3.3.2.3.8).
func TestPropertiesWithout(t *testing.T) {
	for _, p := range places {
		ws, ids := everyProperty(p)
		removals := make([][]byte, 0, 4+len(ids))
		removals = append(removals, nil, []byte{packet.ReasonString, packet.UserProperty}, []byte{0x00}, ids)
		for _, id := range ids {
			removals = append(removals, []byte{id})
		}
		section, _, err := p.decodeSection(setOf(ws))
		if err != nil {
			t.Fatalf("%s: decoding % X: %v", p.name, setOf(ws), err)
		}
		before := bytes.Clone(section)
		for _, omit := range removals {
			want := propsOf(slices.DeleteFunc(slices.Clone(ws), func(w wired) bool { return slices.Contains(omit, w.prop.ID) }))
			got := section.Without(omit...)
			if !bytes.Equal(got, want) {
				t.Errorf("%s, without % X: got % X; want % X", p.name, omit, []byte(got), []byte(want))
				continue
			}
			if again, rest, err := p.decodeSection(set(got)); err != nil || len(rest) != 0 || !bytes.Equal(again, got) {
				t.Errorf("%s, without % X: % X does not decode again as itself: % X, rest % X, %v", p.name, omit, []byte(got), []byte(again), rest, err)
			}
			if !bytes.Equal(section, before) {
				t.Fatalf("%s, without % X: the section given changed from % X to % X", p.name, omit, before, []byte(section))
			}
		}
	}
}

// sameView reports whether a view holds p's identifier and the value of
// its type (Table 2-4): the integer as a number, a UTF-8 Encoded String or
// Binary Data as Value, a UTF-8 String Pair as Name and Value.
func sameView(v *packet.PropertyView, p *packet.Property) bool {
	if v.ID != p.ID {
		return false
	}
	switch table24[rowOf[p.ID]].typ {
	case "UTF-8 Encoded String":
		return string(v.Value) == p.String
	case "Binary Data":
		return bytes.Equal(v.Value, p.Binary)
	case "UTF-8 String Pair":
		return string(v.Name) == p.Pair.Name && string(v.Value) == p.Pair.Value
	default:
		return v.Int == p.Int
	}
}

// sameViews reports whether views hold the properties of want, one by one,
// in order (sameView).
func sameViews(views []packet.PropertyView, want []packet.Property) bool {
	if len(views) != len(want) {
		return false
	}
	for i := range views {
		if !sameView(&views[i], &want[i]) {
			return false
		}
	}
	return true
}

// NewProperties makes a checked section from a caller's properties (the
// maintainer's decision on 261013-024a):
//
//	func NewProperties(typ Type, properties []Property) (Properties, error)
//
// It makes the section a PUBLISH or an acknowledgement (PUBACK, PUBREC,
// PUBREL, PUBCOMP) carries, and, with item 0003d's codecs, a SUBSCRIBE,
// SUBACK, UNSUBSCRIBE or UNSUBACK, and, for the core's own replies, a
// CONNACK or DISCONNECT (0007a.md, 261013 135359: "a direct constructor for
// the core's outgoing properties (CONNACK, DISCONNECT), tested in
// internal/packet"), the properties in their order, without a Property
// Length, checking them as that packet's decoder checks a section it reads:
// Table 2-4's identifiers, places and types, the repeat rules (mayRepeat)
// and the packet's value rules (publishValueRules, subscribeValueRules,
// valueRules' CONNACK rows). A Properties is so checked when made, by a
// decoder, by Properties.Without or here, and never again when written
// (TestAppendPublishWritesAsGiven, TestAppendAckWritesAsGiven,
// TestAppendReplyWritesAsGiven). It refuses with ErrCannotEncode and no
// section. It allocates the section it returns: it runs where properties
// are built, not where a packet is written.

// builders are the places NewProperties makes a section for: the packets
// whose encoders write the section they are given.
var builders = []place{
	placeNamed["PUBLISH"], placeNamed["PUBACK"], placeNamed["PUBREC"], placeNamed["PUBREL"], placeNamed["PUBCOMP"],
	placeNamed["SUBSCRIBE"], placeNamed["SUBACK"], placeNamed["UNSUBSCRIBE"], placeNamed["UNSUBACK"],
	placeNamed["CONNACK"], placeNamed["DISCONNECT"],
}

// madeAs fails the test unless NewProperties makes props, for p, into
// exactly wire.
func madeAs(t *testing.T, p place, props []packet.Property, wire []byte) {
	t.Helper()
	if got, err := packet.NewProperties(p.typ, props); err != nil || !bytes.Equal(got, wire) {
		t.Errorf("%s, NewProperties(%+v) = % X, %v; want % X, nil", p.name, props, []byte(got), err, wire)
	}
}

// TestNewProperties makes a section, for each of builders, of each
// property Table 2-4 lets it carry, at a value every rule allows (sample),
// alone and all together with a User Property repeated (mayRepeat), in the
// order given: the properties' wire bytes, each its identifier and its
// value in Table 2-4's type (MQTT 5 2.2.2.2), which the decoder reads back
// as the same properties. No properties make an empty section.
func TestNewProperties(t *testing.T) {
	for _, p := range builders {
		madeAs(t, p, nil, nil)
		var wires [][]byte
		var props []packet.Property
		for _, r := range table24 {
			if !p.in(r.where) {
				continue
			}
			wire, prop := sample(r.id, r.typ)
			madeAs(t, p, []packet.Property{prop}, wire)
			wires, props = append(wires, wire), append(props, prop)
		}
		user, userProp := userProperty("n", "w")
		wires, props = append(wires, user), append(props, userProp)
		want := slices.Concat(wires...)
		madeAs(t, p, props, want)
		section, err := packet.NewProperties(p.typ, props)
		if err != nil {
			continue
		}
		if back, rest, err := p.decode(set(section)); err != nil || len(rest) != 0 || !sameList(back, props) {
			t.Errorf("%s: made % X, which reads back as %+v, %v; want %+v", p.name, []byte(section), back, err, props)
		}
	}
}
