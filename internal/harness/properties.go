package harness

import "fmt"

// Property identifiers: MQTT 5.0 §2.2.2.2, Table 2-4.
const (
	PayloadFormatIndicator          byte = 0x01
	MessageExpiryInterval           byte = 0x02
	ContentType                     byte = 0x03
	ResponseTopic                   byte = 0x08
	CorrelationData                 byte = 0x09
	SubscriptionIdentifier          byte = 0x0B
	SessionExpiryInterval           byte = 0x11
	AssignedClientIdentifier        byte = 0x12
	ServerKeepAlive                 byte = 0x13
	AuthenticationMethod            byte = 0x15
	AuthenticationData              byte = 0x16
	RequestProblemInformation       byte = 0x17
	WillDelayInterval               byte = 0x18
	RequestResponseInformation      byte = 0x19
	ResponseInformation             byte = 0x1A
	ServerReference                 byte = 0x1C
	ReasonString                    byte = 0x1F
	ReceiveMaximum                  byte = 0x21
	TopicAliasMaximum               byte = 0x22
	TopicAlias                      byte = 0x23
	MaximumQoS                      byte = 0x24
	RetainAvailable                 byte = 0x25
	UserProperty                    byte = 0x26
	MaximumPacketSize               byte = 0x27
	WildcardSubscriptionAvailable   byte = 0x28
	SubscriptionIdentifierAvailable byte = 0x29
	SharedSubscriptionAvailable     byte = 0x2A
)

// propKind is a property's data type (MQTT 5.0 §1.5).
type propKind int

const (
	kindByte propKind = iota + 1
	kindTwoByte
	kindFourByte
	kindVarInt
	kindString
	kindBinary
	kindPair
)

// where names the places a property may appear: a packet type's bit, or
// willProps for a CONNECT's Will Properties (MQTT 5.0 §2.2.2.2, Table 2-4's
// last column).
type where uint32

// willProps is the Will Properties' bit, past every packet type's.
const willProps where = 1 << 16

// in is the set of packet types given, as a where.
func in(kinds ...byte) where {
	var w where
	for _, k := range kinds {
		w |= 1 << k
	}
	return w
}

// propDef is one row of Table 2-4: a property's data type and where it may
// appear.
type propDef struct {
	kind propKind
	in   where
}

// propDefs is Table 2-4. An identifier not in it is unknown, and one in a
// packet its row does not name is not valid there: both make a Malformed
// Packet (5.0 §2.2.2.2: "A Control Packet which contains an Identifier
// which is not valid for its packet type, or contains a value not of the
// specified data type, is a Malformed Packet").
var propDefs = map[byte]propDef{
	PayloadFormatIndicator:          {kindByte, in(PUBLISH) | willProps},
	MessageExpiryInterval:           {kindFourByte, in(PUBLISH) | willProps},
	ContentType:                     {kindString, in(PUBLISH) | willProps},
	ResponseTopic:                   {kindString, in(PUBLISH) | willProps},
	CorrelationData:                 {kindBinary, in(PUBLISH) | willProps},
	SubscriptionIdentifier:          {kindVarInt, in(PUBLISH, SUBSCRIBE)},
	SessionExpiryInterval:           {kindFourByte, in(CONNECT, CONNACK, DISCONNECT)},
	AssignedClientIdentifier:        {kindString, in(CONNACK)},
	ServerKeepAlive:                 {kindTwoByte, in(CONNACK)},
	AuthenticationMethod:            {kindString, in(CONNECT, CONNACK, AUTH)},
	AuthenticationData:              {kindBinary, in(CONNECT, CONNACK, AUTH)},
	RequestProblemInformation:       {kindByte, in(CONNECT)},
	WillDelayInterval:               {kindFourByte, willProps},
	RequestResponseInformation:      {kindByte, in(CONNECT)},
	ResponseInformation:             {kindString, in(CONNACK)},
	ServerReference:                 {kindString, in(CONNACK, DISCONNECT)},
	ReasonString:                    {kindString, in(CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH)},
	ReceiveMaximum:                  {kindTwoByte, in(CONNECT, CONNACK)},
	TopicAliasMaximum:               {kindTwoByte, in(CONNECT, CONNACK)},
	TopicAlias:                      {kindTwoByte, in(PUBLISH)},
	MaximumQoS:                      {kindByte, in(CONNACK)},
	RetainAvailable:                 {kindByte, in(CONNACK)},
	UserProperty:                    {kindPair, in(CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH) | willProps},
	MaximumPacketSize:               {kindFourByte, in(CONNECT, CONNACK)},
	WildcardSubscriptionAvailable:   {kindByte, in(CONNACK)},
	SubscriptionIdentifierAvailable: {kindByte, in(CONNACK)},
	SharedSubscriptionAvailable:     {kindByte, in(CONNACK)},
}

// propBad reports a value the standard makes a Protocol Error: 0 for
// Receive Maximum (§3.1.2.11.3, §3.2.2.3.3), Maximum Packet Size
// (§3.1.2.11.4, §3.2.2.3.6) and Subscription Identifier (§3.3.2.3.8,
// §3.8.2.1.2); anything but 0 or 1 for Request Response Information
// (§3.1.2.11.6), Request Problem Information (§3.1.2.11.7), Maximum QoS
// (§3.2.2.3.4) and Payload Format Indicator, whose two values are its whole
// definition (§3.1.3.2.3, §3.3.2.3.2).
func propBad(id byte, v uint32) bool {
	switch id {
	case ReceiveMaximum, MaximumPacketSize, SubscriptionIdentifier:
		return v == 0
	case RequestResponseInformation, RequestProblemInformation, MaximumQoS, PayloadFormatIndicator:
		return v > 1
	}
	return false
}

// Property is one MQTT 5 property. Which field holds the value depends on the
// identifier's type: Int for the integer types, Str for a UTF-8 string, Data
// for binary data, and Str and Value for a User Property's name and value.
type Property struct {
	ID    byte
	Int   uint32
	Str   string
	Value string
	Data  []byte
}

// Properties keeps the order properties were written or read in, and allows
// repeats: User Property and Subscription Identifier may appear more than once.
type Properties []Property

// User makes a User Property.
func User(name, value string) Property {
	return Property{ID: UserProperty, Str: name, Value: value}
}

// Get returns the first property with the identifier.
func (ps Properties) Get(id byte) (Property, bool) {
	for _, p := range ps {
		if p.ID == id {
			return p, true
		}
	}
	return Property{}, false
}

// User returns the values of every User Property with the name, in order.
func (ps Properties) User(name string) []string {
	var values []string
	for _, p := range ps {
		if p.ID == UserProperty && p.Str == name {
			values = append(values, p.Value)
		}
	}
	return values
}

// AppendProperties appends ps as a property section, its Property Length
// then each property (MQTT 5.0 §2.2.2): the wire form in which storage.md
// keeps a message's properties ("a Property Length followed by the
// properties").
func AppendProperties(b []byte, ps Properties) ([]byte, error) {
	e := &encoder{b: b}
	e.properties(ps)
	return e.b, e.err
}

// properties writes the Property Length, then each property (MQTT 5.0
// §2.2.2). The identifier is a Variable Byte Integer; every defined one fits
// in one byte.
func (e *encoder) properties(ps Properties) {
	inner := &encoder{}
	for _, p := range ps {
		def, ok := propDefs[p.ID]
		if !ok {
			inner.fail("property 0x%02X is not defined by MQTT 5", p.ID)
			break
		}
		inner.varint(uint32(p.ID))
		switch def.kind {
		case kindByte:
			if p.Int > 0xFF {
				inner.fail("property 0x%02X is a byte, not %d", p.ID, p.Int)
			}
			inner.byte(byte(p.Int))
		case kindTwoByte:
			if p.Int > 0xFFFF {
				inner.fail("property 0x%02X is a two-byte integer, not %d", p.ID, p.Int)
			}
			inner.uint16(uint16(p.Int))
		case kindFourByte:
			inner.uint32(p.Int)
		case kindVarInt:
			inner.varint(p.Int)
		case kindString:
			inner.string(p.Str)
		case kindBinary:
			inner.binary(p.Data)
		case kindPair:
			inner.string(p.Str)
			inner.string(p.Value)
		}
	}
	if inner.err != nil {
		e.fail("%v", inner.err)
		return
	}
	e.varint(uint32(len(inner.b)))
	e.bytes(inner.b)
}

// properties reads a Property Length and the properties it covers, for a
// packet of type kind or, with willProps, a Will. Each property must be
// valid there (§2.2.2.2, Table 2-4); one that may appear once appears once,
// User Property repeating anywhere and Subscription Identifier on PUBLISH
// alone (§3.3.2.3.8, §3.8.2.1.2: "It is a Protocol Error to include the
// Subscription Identifier more than once"), every other property's own
// section making a repeat a Protocol Error; and no value is one propBad
// refuses.
func (d *decoder) properties(place where) Properties {
	n := d.varint()
	body := d.take(int(n))
	if d.err != nil {
		return nil
	}
	inner := &decoder{b: body}
	var ps Properties
	seen := map[byte]bool{}
	for len(inner.b) > 0 && inner.err == nil {
		id := inner.varint()
		def, ok := propDefs[byte(id)]
		switch {
		case inner.err != nil:
		case !ok || id > 0xFF:
			inner.malformed("unknown property identifier 0x%02X", id)
		case def.in&place == 0:
			inner.malformed("property 0x%02X is not valid here", id)
		case seen[byte(id)] && id != uint32(UserProperty) && (id != uint32(SubscriptionIdentifier) || place != in(PUBLISH)):
			inner.protocol(fmt.Sprintf("property 0x%02X more than once", id))
		}
		seen[byte(id)] = true
		p := Property{ID: byte(id)}
		switch def.kind {
		case kindByte:
			p.Int = uint32(inner.byte())
		case kindTwoByte:
			p.Int = uint32(inner.uint16())
		case kindFourByte:
			p.Int = inner.uint32()
		case kindVarInt:
			p.Int = inner.varint()
		case kindString:
			p.Str = inner.string()
		case kindBinary:
			p.Data = inner.binary()
		case kindPair:
			p.Str = inner.string()
			p.Value = inner.string()
		}
		switch {
		case inner.err != nil:
		case p.ID == TopicAlias && p.Int == 0:
			// 5.0 §3.3.4: "A Topic Alias value of 0 [...] is a Protocol
			// Error", with a reason of its own.
			inner.err = fmt.Errorf("%w: Topic Alias 0", ErrTopicAlias)
		case propBad(p.ID, p.Int):
			inner.protocol(fmt.Sprintf("property 0x%02X with value %d", p.ID, p.Int))
		}
		ps = append(ps, p)
	}
	if inner.err != nil {
		d.err = fmt.Errorf("properties: %w", inner.err)
	}
	return ps
}
