package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The connection handshake, CONNECT (MQTT 5 3.1, MQTT 3.1.1 3.1). A decoder
// takes a packet's body, the Remaining Length bytes ReadHeader leaves behind;
// an encoder writes the whole packet, fixed header included.
//
// Error classes: where a sentence names one, the tests check that one; where
// none is named they use isKind (types_test.go), as 0003a's tests do. Bytes
// that cannot be split into the packet's fields, cut short or left over, are
// a Malformed Packet by MQTT 5 1.2's definition: "A control packet that
// cannot be parsed according to this specification." MQTT 3.1.1 names no
// classes; its refusals close the connection ([MQTT-4.8.0-1]).

// Connect Flags bits (MQTT 5 3.1.2.3 Figure 3-4; MQTT 3.1.1 3.1.2.3 Figure
// 3.4). Will QoS is bits 4 and 3.
const (
	reservedBit  = 0x01
	cleanStart   = 0x02
	willFlag     = 0x04
	willQoSShift = 3
	willRetain   = 0x20
	passwordFlag = 0x40
	userNameFlag = 0x80
)

// mqttName is the Protocol Name, the UTF-8 Encoded String "MQTT" (MQTT 5
// 3.1.2.1 Figure 3-2; MQTT 3.1.1 3.1.2.1 Figure 3.2).
var mqttName = []byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}

// mqisdpName is MQTT 3.1's Protocol Name, "MQIsdp", as a UTF-8 Encoded
// String; MQTT 3.1 is level 3 (design.md, Flows, CONNECT).
var mqisdpName = []byte{0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p'}

// connectCodec describes CONNECT's codec: the decoder reads the version
// from the body, and the encoder from the Connect. Its oracle is connectOK;
// its encoder writes its Properties as given. Fixed header:
// flags 0 (MQTT 5 3.1.1, MQTT 3.1.1 3.1.1).
var connectCodec = codec[packet.Connect]{
	name: "CONNECT", typ: packet.CONNECT, first: byte(packet.CONNECT) << 4,
	read: func(body []byte, _ packet.Version) (packet.Connect, error) { return packet.DecodeConnect(body) },
	write: func(dst []byte, _ packet.Version, c *packet.Connect) ([]byte, error) {
		return packet.AppendConnect(dst, c)
	},
	same:             sameConnect,
	ok:               func(_ packet.Version, c *packet.Connect) bool { return connectOK(c) },
	checksProperties: true,
}

// wired is one property as its bytes on the wire and as the Property they
// decode to, built by hand from Table 2-4's types (MQTT 5 2.2.2.2), so no
// input is made by the code under test.
type wired struct {
	wire []byte
	prop packet.Property
}

// intProp is an integer property, written as the type table24 gives its
// identifier.
func intProp(id byte, v uint32) wired {
	w := []byte{id}
	switch table24[rowOf[id]].typ {
	case "Byte":
		w = append(w, byte(v))
	case "Two Byte Integer":
		w = binary.BigEndian.AppendUint16(w, uint16(v))
	case "Four Byte Integer":
		w = binary.BigEndian.AppendUint32(w, v)
	case "Variable Byte Integer":
		w = binary.AppendUvarint(w, uint64(v))
	default:
		panic("intProp: not an integer property")
	}
	return wired{w, packet.Property{ID: id, Int: v}}
}

// stringProp is a UTF-8 Encoded String property.
func stringProp(id byte, s string) wired {
	return wired{append([]byte{id}, prefixed([]byte(s))...), packet.Property{ID: id, String: s}}
}

// binaryProp is a Binary Data property.
func binaryProp(id byte, b []byte) wired {
	return wired{append([]byte{id}, prefixed(b)...), packet.Property{ID: id, Binary: b}}
}

// userProp is a User Property.
func userProp(name, value string) wired {
	w, p := userProperty(name, value)
	return wired{w, p}
}

// setOf returns ws as a property set, behind its Property Length.
func setOf(ws []wired) []byte {
	wires := make([][]byte, len(ws))
	for i := range ws {
		wires[i] = ws[i].wire
	}
	return set(wires...)
}

// propsOf returns the Properties ws decode to: their wire bytes, kept as
// they arrive, without the Property Length.
func propsOf(ws []wired) packet.Properties {
	var props packet.Properties
	for i := range ws {
		props = append(props, ws[i].wire...)
	}
	return props
}

// listOf returns the properties ws hold, in order, as a caller lists them
// for NewProperties.
func listOf(ws []wired) []packet.Property {
	list := make([]packet.Property, len(ws))
	for i := range ws {
		list[i] = ws[i].prop
	}
	return list
}

// values returns each property props hold, in order (Properties.All).
func values(props packet.Properties) []packet.Property {
	return slices.Collect(props.All())
}

// sameValues reports whether two property sections hold the same
// identifiers and values in the same order, comparing only the field each
// identifier's type uses (sameValue).
func sameValues(a, b packet.Properties) bool {
	av, bv := values(a), values(b)
	if len(av) != len(bv) {
		return false
	}
	for i := range av {
		if !sameValue(&av[i], &bv[i]) {
			return false
		}
	}
	return true
}

// sameConnect reports whether two CONNECTs hold the same fields.
func sameConnect(a, b *packet.Connect) bool {
	return a.Version == b.Version && a.CleanStart == b.CleanStart && a.KeepAlive == b.KeepAlive &&
		sameValues(a.Properties, b.Properties) && a.ClientID == b.ClientID &&
		a.WillFlag == b.WillFlag && a.Will.QoS == b.Will.QoS && a.Will.Retain == b.Will.Retain &&
		sameValues(a.Will.Properties, b.Will.Properties) && a.Will.Topic == b.Will.Topic &&
		bytes.Equal(a.Will.Payload, b.Will.Payload) &&
		a.UserNameFlag == b.UserNameFlag && a.UserName == b.UserName &&
		a.PasswordFlag == b.PasswordFlag && bytes.Equal(a.Password, b.Password)
}

// connectSpec describes a CONNECT field by field. body lays it out as the
// standard orders it; want is the Connect it decodes to.
type connectSpec struct {
	name        string
	level       byte
	flags       byte
	keepAlive   uint16
	props       []wired // MQTT 5 only
	clientID    string
	willProps   []wired // MQTT 5 only
	willTopic   string
	willPayload []byte
	userName    string
	password    []byte
}

// body returns the variable header and payload. MQTT 5 3.1.2: "Protocol
// Name, Protocol Level, Connect Flags, Keep Alive, and Properties"; 3.1.3:
// "These fields, if present, MUST appear in the order Client Identifier,
// Will Properties, Will Topic, Will Payload, User Name, Password
// [MQTT-3.1.3-1]", each present as its flag says (3.1.2.5, 3.1.2.8,
// 3.1.2.9). MQTT 3.1.1 3.1.2 and 3.1.3 [MQTT-3.1.3-1]: the same, without
// Properties or Will Properties.
func (s *connectSpec) body() []byte {
	b := slices.Concat(mqttName, []byte{s.level, s.flags}, binary.BigEndian.AppendUint16(nil, s.keepAlive))
	if s.level == 5 {
		b = append(b, setOf(s.props)...)
	}
	b = append(b, prefixed([]byte(s.clientID))...)
	if s.flags&willFlag != 0 {
		if s.level == 5 {
			b = append(b, setOf(s.willProps)...)
		}
		b = append(b, prefixed([]byte(s.willTopic))...)
		b = append(b, prefixed(s.willPayload)...)
	}
	if s.flags&userNameFlag != 0 {
		b = append(b, prefixed([]byte(s.userName))...)
	}
	if s.flags&passwordFlag != 0 {
		b = append(b, prefixed(s.password)...)
	}
	return b
}

// caseOf returns the spec as the case connectCodec takes.
func (s *connectSpec) caseOf() packetCase[packet.Connect] {
	return packetCase[packet.Connect]{s.name, &connectCodec, packet.Version(s.level), s.body(), s.want()}
}

// want returns the Connect the spec decodes to: the Will, the user name
// and the password only when their flags are set.
func (s *connectSpec) want() packet.Connect {
	c := packet.Connect{
		Version:    packet.Version(s.level),
		CleanStart: s.flags&cleanStart != 0,
		KeepAlive:  s.keepAlive,
		Properties: propsOf(s.props),
		ClientID:   s.clientID,
	}
	if s.flags&willFlag != 0 {
		c.WillFlag = true
		c.Will = packet.Will{
			QoS:        s.flags >> willQoSShift & 3,
			Retain:     s.flags&willRetain != 0,
			Properties: propsOf(s.willProps),
			Topic:      s.willTopic,
			Payload:    s.willPayload,
		}
	}
	if s.flags&userNameFlag != 0 {
		c.UserNameFlag, c.UserName = true, s.userName
	}
	if s.flags&passwordFlag != 0 {
		c.PasswordFlag, c.Password = true, s.password
	}
	return c
}

// validConnects are CONNECTs both standards allow. The first two carry the
// variable headers of the standards' examples.
var validConnects = []connectSpec{
	{
		name: "MQTT 5, the example of 3.1.2.12", level: 5, flags: 0xCE, keepAlive: 10,
		props: []wired{intProp(0x11, 10)}, clientID: "client",
		willProps: []wired{intProp(0x18, 5)}, willTopic: "will/topic", willPayload: []byte{0x00, 0xFF},
		userName: "user", password: []byte{0x01, 0x02},
	},
	{
		name: "MQTT 3.1.1, the example of 3.1.2.11", level: 4, flags: 0xCE, keepAlive: 10,
		clientID: "client", willTopic: "will/topic", willPayload: []byte{0x00, 0xFF},
		userName: "user", password: []byte{0x01, 0x02},
	},
	{
		// MQTT 5 3.1.3.1: "A Server MAY allow a Client to supply a ClientID
		// that has a length of zero bytes" [MQTT-3.1.3-6]; MQTT 5 ties it to
		// no Clean Start value.
		name: "MQTT 5, nothing optional, an empty client id, Clean Start 0", level: 5,
	},
	{
		// MQTT 5 3.1.2.9: "This version of the protocol allows the sending
		// of a Password with no User Name, where MQTT v3.1.1 did not."
		name: "MQTT 5, a password without a user name", level: 5, flags: passwordFlag | cleanStart,
		keepAlive: 0xFFFF, clientID: "c", password: []byte("secret"),
	},
	{
		// MQTT 3.1.1 3.1.2.6 [MQTT-3.1.2-14]: Will QoS 2 with the Will Flag;
		// 3.1.2.7: Will Retain 1 [MQTT-3.1.2-17]. A present field may be
		// empty: a Will Message of "zero or more bytes" (3.1.3.3), a password
		// of "0 to 65535 bytes" (3.1.3.5).
		name: "MQTT 3.1.1, a retained QoS 2 Will, empty user name and password", level: 4,
		flags:     userNameFlag | passwordFlag | willRetain | 2<<willQoSShift | willFlag | cleanStart,
		keepAlive: 0xFFFF, clientID: "a", willTopic: "t", willPayload: []byte{},
		userName: "", password: []byte{},
	},
	{
		// Every property 3.1.2.11 lists, and every one 3.1.3.2 lists, each at
		// a value its section allows.
		name: "MQTT 5, every CONNECT property and every Will property", level: 5,
		flags: willRetain | 1<<willQoSShift | willFlag | userNameFlag, keepAlive: 60,
		props: []wired{
			intProp(0x11, 0xFFFFFFFF), intProp(0x21, 1), intProp(0x27, 1), intProp(0x22, 0),
			intProp(0x19, 1), intProp(0x17, 0), userProp("n", "v"), userProp("n", "w"),
			stringProp(0x15, "SCRAM-SHA-1"), binaryProp(0x16, []byte{0x01}),
		},
		clientID: "c1",
		willProps: []wired{
			intProp(0x18, 0), intProp(0x01, 1), intProp(0x02, 3600), stringProp(0x03, "text/plain"),
			stringProp(0x08, "r/t"), binaryProp(0x09, []byte{0xC0}), userProp("a", "b"),
		},
		willTopic: "w", willPayload: []byte("hello"), userName: "u",
	},
	{
		// MQTT 5 3.1.2.11.10: "It is a Protocol Error to include
		// Authentication Data if there is no Authentication Method." Here
		// there is one, after the data.
		name: "MQTT 5, Authentication Data before its Authentication Method", level: 5,
		props:    []wired{binaryProp(0x16, []byte{0x01}), stringProp(0x15, "M")},
		clientID: "c",
	},
}

// TestConnect decodes and encodes each of validConnects: the decoder returns
// every field; the encoder writes the whole packet, after what dst holds.
// The first two bodies begin with the standards' example variable headers,
// byte for byte (MQTT 5 3.1.2.12 Figure 3-6; MQTT 3.1.1 3.1.2.11 Figure 3.6).
func TestConnect(t *testing.T) {
	examples := [][]byte{
		{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0xCE, 0x00, 0x0A, 0x05, 0x11, 0x00, 0x00, 0x00, 0x0A},
		{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0xCE, 0x00, 0x0A},
	}
	for i, ex := range examples {
		if body := validConnects[i].body(); !bytes.HasPrefix(body, ex) {
			t.Fatalf("%s: body % X does not begin with the example % X", validConnects[i].name, body, ex)
		}
	}
	testRoundTrips(t, casesOf(validConnects, (*connectSpec).caseOf))
}

// valueRule is a value rule a property's own section states, beyond the
// type Table 2-4 gives it: the values allowed, values the tests try on each
// side, and the class the section gives the rest (nil where it names none).
type valueRule struct {
	place string // "CONNECT", "CONNACK" or "Will Properties"
	name  string
	id    byte
	ok    func(uint32) bool
	good  []uint32
	bad   []uint32
	class error
}

func nonZero(v uint32) bool   { return v != 0 }
func zeroOrOne(v uint32) bool { return v <= 1 }

// valueRules are the value rules of MQTT 5 3.1.2.11, 3.1.3.2 and 3.2.2.3.
var valueRules = []valueRule{
	// 3.1.2.11.3: "It is a Protocol Error to include the Receive Maximum
	// value more than once or for it to have the value 0."
	{"CONNECT", "Receive Maximum", 0x21, nonZero, []uint32{1, 65_535}, []uint32{0}, packet.ErrProtocol},
	// 3.1.2.11.4: "It is a Protocol Error to include the Maximum Packet
	// Size more than once, or for the value to be set to zero."
	{"CONNECT", "Maximum Packet Size", 0x27, nonZero, []uint32{1, 0xFFFFFFFF}, []uint32{0}, packet.ErrProtocol},
	// 3.1.2.11.6: "It is Protocol Error to include the Request Response
	// Information more than once, or to have a value other than 0 or 1."
	{"CONNECT", "Request Response Information", 0x19, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.1.2.11.7: "It is a Protocol Error to include Request Problem
	// Information more than once, or to have a value other than 0 or 1."
	{"CONNECT", "Request Problem Information", 0x17, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.1.3.2.3: "Followed by the value of the Payload Format Indicator,
	// either of: 0 (0x00) ... 1 (0x01)". No class is named.
	{"Will Properties", "Payload Format Indicator", 0x01, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, nil},
	// 3.2.2.3.3: as 3.1.2.11.3.
	{"CONNACK", "Receive Maximum", 0x21, nonZero, []uint32{1, 65_535}, []uint32{0}, packet.ErrProtocol},
	// 3.2.2.3.4: "It is a Protocol Error to include Maximum QoS more than
	// once, or to have a value other than 0 or 1."
	{"CONNACK", "Maximum QoS", 0x24, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.2.2.3.5: "It is a Protocol Error to include Retain Available more
	// than once or to use a value other than 0 or 1."
	{"CONNACK", "Retain Available", 0x25, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.2.2.3.6: as 3.1.2.11.4.
	{"CONNACK", "Maximum Packet Size", 0x27, nonZero, []uint32{1, 0xFFFFFFFF}, []uint32{0}, packet.ErrProtocol},
	// 3.2.2.3.11: "It is a Protocol Error to include the Wildcard
	// Subscription Available more than once or to send a value other than 0
	// or 1."
	{"CONNACK", "Wildcard Subscription Available", 0x28, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.2.2.3.12: "... the Subscription Identifier Available more than
	// once, or to send a value other than 0 or 1."
	{"CONNACK", "Subscription Identifiers Available", 0x29, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
	// 3.2.2.3.13: "... the Shared Subscription Available more than once or
	// to send a value other than 0 or 1."
	{"CONNACK", "Shared Subscription Available", 0x2A, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, packet.ErrProtocol},
}

// valuesOK reports whether props keep the value rules of place, and, in a
// CONNECT, MQTT 5 3.1.2.11.10: "It is a Protocol Error to include
// Authentication Data if there is no Authentication Method."
func valuesOK(place string, props []packet.Property) bool {
	data, method := false, false
	for i := range props {
		for _, r := range valueRules {
			if r.place == place && r.id == props[i].ID && !r.ok(props[i].Int) {
				return false
			}
		}
		data = data || props[i].ID == 0x16
		method = method || props[i].ID == 0x15
	}
	return place != "CONNECT" || !data || method
}

// connectOK is the oracle for a Connect the encoder writes and the decoder
// returns: everything MQTT 5 3.1 or MQTT 3.1.1 3.1 asks of a CONNECT.
func connectOK(c *packet.Connect) bool {
	v5 := c.Version == packet.V5
	if !v5 && c.Version != packet.V311 || !testkit.UTF8StringOK(c.ClientID) {
		return false
	}
	w := &c.Will
	if c.WillFlag {
		// [MQTT-3.1.2-12] (MQTT 3.1.1 [MQTT-3.1.2-14]); 3.1.3.3, 3.1.3.4.
		if w.QoS > 2 || !testkit.UTF8StringOK(w.Topic) || len(w.Payload) > 65_535 {
			return false
		}
	} else if w.QoS != 0 || w.Retain || w.Topic != "" || len(w.Payload) != 0 || len(w.Properties) != 0 {
		// [MQTT-3.1.2-11], [MQTT-3.1.2-13]; 3.1.3: fields "whose presence
		// is determined by the flags".
		return false
	}
	if !c.UserNameFlag && c.UserName != "" || c.UserNameFlag && !testkit.UTF8StringOK(c.UserName) {
		return false // [MQTT-3.1.2-16], [MQTT-3.1.3-12]
	}
	if !c.PasswordFlag && len(c.Password) != 0 || len(c.Password) > 65_535 {
		return false // [MQTT-3.1.2-18], 1.5.6
	}
	if !v5 {
		// MQTT 3.1.1 has no properties (3.1.2: "four fields"; 3.1.3), and
		// [MQTT-3.1.2-22].
		return len(c.Properties) == 0 && len(w.Properties) == 0 && (!c.PasswordFlag || c.UserNameFlag)
	}
	props, ok := valuesOf(c.Properties)
	willProps, willOK := valuesOf(w.Properties)
	return ok && encodable(placeNamed["CONNECT"], props) && valuesOK("CONNECT", props) &&
		willOK && encodable(placeNamed["Will Properties"], willProps) && valuesOK("Will Properties", willProps)
}

// connectWritable is the oracle for a Connect the encoder writes: one
// connectOK allows that a Client may also send. MQTT 3.1.1 3.1.3.1: "If the
// Client supplies a zero-byte ClientId, the Client MUST also set
// CleanSession to 1 [MQTT-3.1.3-7]." The Server still decodes such a
// CONNECT, to answer it (the conformance tests' TestConnectRefusals). A
// Will Topic is a Topic Name; the Server still decodes another, to refuse
// it with 0x90 (TestConnectRefusedWithConnack).
func connectWritable(c *packet.Connect) bool {
	return connectOK(c) && (c.Version != packet.V311 || c.ClientID != "" || c.CleanStart) &&
		(!c.WillFlag || testkit.TopicNameOK(c.Will.Topic))
}

// TestConnectRefusedWithConnack: a CONNECT the broker refuses with a
// CONNACK must reach the check that refuses it, so the decoder returns it.
// design.md, Flows, CONNECT, the refusal table: "0x85 | a client id ...
// holding a control character" (MQTT 3.1.1: 2) and "0x90 | a Will topic
// that is not a valid topic name" (MQTT 3.1.1: 5). The control characters
// are those MQTT 5 1.5.4 names, U+0001 to U+001F and U+007F to U+009F; a
// topic name holds no wildcard and is at least one character long
// ([MQTT-4.7.0-1], [MQTT-4.7.3-1]).
func TestConnectRefusedWithConnack(t *testing.T) {
	controlIDs := []string{"\x01", "a\x1Fb", "\x7F", "a\u0080", "\u009F"}
	invalidTopics := []string{"a/+/b", "a/#", "+", "#", ""}
	for _, level := range []byte{4, 5} {
		specs := make([]connectSpec, 0, len(controlIDs)+len(invalidTopics))
		for _, id := range controlIDs {
			specs = append(specs, connectSpec{
				name: fmt.Sprintf("client id %q", id), level: level, flags: cleanStart, clientID: id,
			})
		}
		for _, topic := range invalidTopics {
			specs = append(specs, connectSpec{
				name: fmt.Sprintf("Will topic %q", topic), level: level, flags: cleanStart | willFlag,
				clientID: "c", willTopic: topic, willPayload: []byte("p"),
			})
		}
		for i := range specs {
			body, want := specs[i].body(), specs[i].want()
			if got, err := packet.DecodeConnect(body); err != nil || !sameConnect(&got, &want) {
				t.Errorf("level %d, %s: got %+v, %v; want %+v", level, specs[i].name, got, err, want)
			}
		}
	}
}

// propertyCase is a set of properties for a CONNECT or a CONNACK, and the
// class the standard gives it (nil: it names none).
type propertyCase struct {
	name  string
	props []wired
	class error
}

// FuzzDecodeConnect feeds random bytes to DecodeConnect. It must never
// panic. A refusal is a packet error, or ErrUnsupportedVersion for a
// CONNECT named "MQTT" whose level is neither 4 nor 5, or named "MQIsdp"
// at level 3. What it accepts
// obeys connectOK and, where connectWritable says a Client may send it,
// writes back as exactly the packet it came from (design.md, Testing: code
// that reads bytes from outside Sagüin "is fuzzed").
func FuzzDecodeConnect(f *testing.F) {
	for i := range validConnects {
		f.Add(validConnects[i].body())
	}
	for _, flags := range []byte{0x01, 0x08, 0x1C, 0x20, 0x40} {
		f.Add((&connectSpec{level: 4, flags: flags, clientID: "c"}).body())
		f.Add((&connectSpec{level: 5, flags: flags, clientID: "c"}).body())
	}
	f.Add((&connectSpec{level: 4}).body())
	f.Add(slices.Concat(mqttName, []byte{0x03}))
	f.Add(slices.Concat(prefixed([]byte("MQIsdp")), []byte{0x03, 0x02, 0x00, 0x3C}, prefixed([]byte("c"))))
	// Any other Protocol Name, here "mqtt" at level 4 (design.md, Flows,
	// CONNECT: "Any other Protocol Name [...] closes the connection without
	// a CONNACK (MQTT-3.1.2-1)").
	f.Add(slices.Concat(prefixed([]byte("mqtt")), []byte{0x04, 0x02, 0x00, 0x3C}, prefixed([]byte("c"))))
	f.Add((&connectSpec{level: 5, props: []wired{intProp(0x21, 0)}}).body())
	f.Add((&connectSpec{level: 5, props: []wired{binaryProp(0x16, nil)}}).body())
	f.Add((&connectSpec{level: 5, flags: willFlag, willProps: []wired{intProp(0x01, 2)}}).body())
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := packet.DecodeConnect(data)
		if errors.Is(err, packet.ErrUnsupportedVersion) {
			mqtt := bytes.HasPrefix(data, mqttName) && len(data) >= 7 && data[6] != 4 && data[6] != 5
			mqisdp := bytes.HasPrefix(data, mqisdpName) && len(data) >= 9 && data[8] == 3
			if !mqtt && !mqisdp {
				t.Fatalf("% X: ErrUnsupportedVersion, but this is neither \"MQTT\" with another level nor \"MQIsdp\" at level 3", data)
			}
			return
		}
		if err != nil {
			if !isKind(err) {
				t.Fatalf("% X: err = %v, not a packet error", data, err)
			}
			return
		}
		if !connectOK(&c) {
			t.Fatalf("% X: accepted %+v, which the standard forbids", data, c)
		}
		if !connectWritable(&c) {
			return // the server reads what a Client may not send, to answer it
		}
		out, err := packet.AppendConnect(nil, &c)
		if err != nil || !bytes.Equal(out, connectCodec.wire(data)) {
			t.Fatalf("read % X as %+v, which writes back as % X (err %v)", data, c, out, err)
		}
	})
}
