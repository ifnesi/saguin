package packet

import (
	"bytes"
	"encoding/binary"
	"iter"
	"math"
	"slices"
)

// MQTT 5 property identifiers from Table 2-4.
const (
	PayloadFormatIndicator          byte = 0x01
	MessageExpiryInterval           byte = 0x02
	ContentType                     byte = 0x03
	ResponseTopic                   byte = 0x08
	CorrelationData                 byte = 0x09
	SubscriptionIdentifier          byte = 0x0b
	SessionExpiryInterval           byte = 0x11
	AssignedClientIdentifier        byte = 0x12
	ServerKeepAlive                 byte = 0x13
	AuthenticationMethod            byte = 0x15
	AuthenticationData              byte = 0x16
	RequestProblemInformation       byte = 0x17
	WillDelayInterval               byte = 0x18
	RequestResponseInformation      byte = 0x19
	ResponseInformation             byte = 0x1a
	ServerReference                 byte = 0x1c
	ReasonString                    byte = 0x1f
	ReceiveMaximum                  byte = 0x21
	TopicAliasMaximum               byte = 0x22
	TopicAlias                      byte = 0x23
	MaximumQoS                      byte = 0x24
	RetainAvailable                 byte = 0x25
	UserProperty                    byte = 0x26
	MaximumPacketSize               byte = 0x27
	WildcardSubscriptionAvailable   byte = 0x28
	SubscriptionIdentifierAvailable byte = 0x29
	SharedSubscriptionAvailable     byte = 0x2a
)

// IsSaguinProperty reports whether a User Property name uses the reserved
// saguin- prefix. The name is checked in place, without allocating a string.
func IsSaguinProperty(name []byte) bool {
	return bytes.HasPrefix(name, []byte("saguin-"))
}

// Property holds the value of one MQTT 5 property. Its identifier determines
// which one of Int, String, Binary, or Pair is used.
type Property struct {
	ID     byte
	Int    uint32
	String string
	Binary []byte
	Pair   StringPair
}

// PropertyView is one property read in place: an integer kind as Int;
// a UTF-8 Encoded String or Binary Data as Value; a UTF-8 String Pair
// as Name and Value. Name and Value are slices of the section's bytes.
type PropertyView struct {
	ID     byte
	Int    uint32
	Name   []byte
	Value  []byte
	Offset int // byte offset in the property section
}

// Properties is a checked MQTT 5 property section in wire order, without
// its Property Length. Packet decoders and NewProperties establish the
// packet's property value rules; Without preserves the property checks.
// ReadStoredProperties trusts the original checks and checks structure alone.
// Broker-made delivery additions are written directly with valid wire values.
// Packet encoders using NewProperties trust those checks. Callers must not
// construct unchecked sections or change their bytes. Decoded properties
// share the source buffer, which must not change while the properties are in use.
type Properties []byte

// Stored adds the Property Length to checked properties and optional checked
// additions. An absent section is NULL in the store, rather than a zero length.
// A combined length beyond MQTT's limit returns ErrCannotEncode.
func (p Properties) Stored(additions Properties) ([]byte, error) {
	if len(p) > maxVarInt || len(additions) > maxVarInt-len(p) {
		return nil, ErrCannotEncode
	}
	n := len(p) + len(additions)
	if n == 0 {
		return nil, nil
	}
	return appendCheckedProperties(make([]byte, 0, varIntSize(uint32(n))+n), p, additions), nil
}

// All reads each property in wire order, including every repeat. The section
// must have been checked when made. Strings are copied only when read;
// Binary Data shares the section's bytes.
func (p Properties) All() iter.Seq[Property] {
	return func(yield func(Property) bool) {
		for view := range p.Views() {
			property := Property{ID: view.ID, Int: view.Int}
			switch propertyDefinitions[view.ID].kind {
			case propertyString:
				property.String = string(view.Value)
			case propertyBinary:
				property.Binary = view.Value
			case propertyStringPair:
				property.Pair = StringPair{Name: string(view.Name), Value: string(view.Value)}
			}
			if !yield(property) {
				return
			}
		}
	}
}

// Views reads each property in wire order, including every repeat, without
// copying. The section must have been checked when made. Name and Value
// share the section's bytes, which must not be changed while views are in use.
func (p Properties) Views() iter.Seq[PropertyView] {
	return func(yield func(PropertyView) bool) {
		for body := []byte(p); len(body) != 0; {
			property := PropertyView{ID: body[0], Offset: len(p) - len(body)}
			body = body[1:]
			switch propertyDefinitions[property.ID].kind {
			case propertyByte:
				property.Int, body = uint32(body[0]), body[1:]
			case propertyUint16:
				property.Int, body = uint32(binary.BigEndian.Uint16(body)), body[2:]
			case propertyUint32:
				property.Int, body = binary.BigEndian.Uint32(body), body[4:]
			case propertyVarInt:
				value, n := binary.Uvarint(body)
				property.Int, body = uint32(value), body[n:]
			case propertyString, propertyBinary:
				property.Value, body = propertyField(body)
			case propertyStringPair:
				property.Name, body = propertyField(body)
				property.Value, body = propertyField(body)
			}
			if !yield(property) {
				return
			}
		}
	}
}

// propertyField reads a length-prefixed field from already checked wire bytes,
// shared by property and subscription list iterators.
func propertyField(src []byte) (value, rest []byte) {
	n := 2 + int(binary.BigEndian.Uint16(src))
	return src[2:n], src[n:]
}

// Without omits the given identifiers from an already checked section,
// preserving the wire bytes and order of the remaining properties. It shares
// a prefix of p where possible and otherwise copies the remaining bytes.
// Removing properties preserves their types, places, repeats and value rules.
func (p Properties) Without(ids ...byte) Properties {
	return p.without(func(id byte, _ []byte) bool { return slices.Contains(ids, id) })
}

// without preserves checked bytes while sharing omission and prefix copying.
func (p Properties) without(omit func(byte, []byte) bool) Properties {
	kept := p[:0:0]
	for body := []byte(p); len(body) != 0; {
		id := body[0]
		rest := propertyRest(body[1:], propertyDefinitions[id].kind)
		n := len(body) - len(rest)
		if !omit(id, body[1:n]) {
			kept = keepProperty(kept, p, len(p)-len(body), len(p)-len(rest))
		}
		body = rest
	}
	return kept
}

// keepProperty shares an unchanged prefix. After an omission, its clipped
// capacity makes appending copy rather than overwrite the original bytes.
func keepProperty(kept, original Properties, from, before int) Properties {
	if len(kept) == from {
		return original[:before:before]
	}
	return append(kept, original[from:before]...)
}

// propertyRest skips a value in an already checked section, without copying
// strings or unpacking a Property when only its wire span is needed.
func propertyRest(src []byte, kind propertyKind) []byte {
	switch kind {
	case propertyByte:
		return src[1:]
	case propertyUint16:
		return src[2:]
	case propertyUint32:
		return src[4:]
	case propertyVarInt:
		_, n := binary.Uvarint(src)
		return src[n:]
	case propertyString, propertyBinary:
		_, rest := propertyField(src)
		return rest
	case propertyStringPair:
		_, rest := propertyField(src)
		_, rest = propertyField(rest)
		return rest
	default:
		panic("packet: unknown property kind")
	}
}

type propertyKind byte

const (
	propertyByte propertyKind = iota
	propertyUint16
	propertyUint32
	propertyVarInt
	propertyString
	propertyBinary
	propertyStringPair
)

type locations uint32

const (
	atCONNECT     locations = 1 << CONNECT
	atCONNACK     locations = 1 << CONNACK
	atPUBLISH     locations = 1 << PUBLISH
	atPUBACK      locations = 1 << PUBACK
	atPUBREC      locations = 1 << PUBREC
	atPUBREL      locations = 1 << PUBREL
	atPUBCOMP     locations = 1 << PUBCOMP
	atSUBSCRIBE   locations = 1 << SUBSCRIBE
	atSUBACK      locations = 1 << SUBACK
	atUNSUBSCRIBE locations = 1 << UNSUBSCRIBE
	atUNSUBACK    locations = 1 << UNSUBACK
	atDISCONNECT  locations = 1 << DISCONNECT
	atAUTH        locations = 1 << AUTH
	atWill        locations = 1 << 16
)

type propertyDefinition struct {
	kind    propertyKind
	allowed locations
}

// propertyDefinitions mirrors MQTT 5 Table 2-4. Keeping placement beside
// wire type makes a decoder unable to recognize a property without also
// checking where it is.
var propertyDefinitions = [...]propertyDefinition{
	PayloadFormatIndicator:          {propertyByte, atPUBLISH | atWill},
	MessageExpiryInterval:           {propertyUint32, atPUBLISH | atWill},
	ContentType:                     {propertyString, atPUBLISH | atWill},
	ResponseTopic:                   {propertyString, atPUBLISH | atWill},
	CorrelationData:                 {propertyBinary, atPUBLISH | atWill},
	SubscriptionIdentifier:          {propertyVarInt, atPUBLISH | atSUBSCRIBE},
	SessionExpiryInterval:           {propertyUint32, atCONNECT | atCONNACK | atDISCONNECT},
	AssignedClientIdentifier:        {propertyString, atCONNACK},
	ServerKeepAlive:                 {propertyUint16, atCONNACK},
	AuthenticationMethod:            {propertyString, atCONNECT | atCONNACK | atAUTH},
	AuthenticationData:              {propertyBinary, atCONNECT | atCONNACK | atAUTH},
	RequestProblemInformation:       {propertyByte, atCONNECT},
	WillDelayInterval:               {propertyUint32, atWill},
	RequestResponseInformation:      {propertyByte, atCONNECT},
	ResponseInformation:             {propertyString, atCONNACK},
	ServerReference:                 {propertyString, atCONNACK | atDISCONNECT},
	ReasonString:                    {propertyString, atCONNACK | atPUBACK | atPUBREC | atPUBREL | atPUBCOMP | atSUBACK | atUNSUBACK | atDISCONNECT | atAUTH},
	ReceiveMaximum:                  {propertyUint16, atCONNECT | atCONNACK},
	TopicAliasMaximum:               {propertyUint16, atCONNECT | atCONNACK},
	TopicAlias:                      {propertyUint16, atPUBLISH},
	MaximumQoS:                      {propertyByte, atCONNACK},
	RetainAvailable:                 {propertyByte, atCONNACK},
	UserProperty:                    {propertyStringPair, atCONNECT | atCONNACK | atPUBLISH | atWill | atPUBACK | atPUBREC | atPUBREL | atPUBCOMP | atSUBSCRIBE | atSUBACK | atUNSUBSCRIBE | atUNSUBACK | atDISCONNECT | atAUTH},
	MaximumPacketSize:               {propertyUint32, atCONNECT | atCONNACK},
	WildcardSubscriptionAvailable:   {propertyByte, atCONNACK},
	SubscriptionIdentifierAvailable: {propertyByte, atCONNACK},
	SharedSubscriptionAvailable:     {propertyByte, atCONNACK},
}

func definition(id byte) (propertyDefinition, bool) {
	if int(id) >= len(propertyDefinitions) {
		return propertyDefinition{}, false
	}
	def := propertyDefinitions[id]
	return def, def.allowed != 0
}

// DecodeProperties checks the property section carried by typ and returns
// its bytes in place, without the Property Length, and the following bytes.
func DecodeProperties(src []byte, typ Type) (Properties, []byte, error) {
	if typ < CONNECT || typ > AUTH {
		return nil, nil, ErrMalformed
	}
	return decodeProperties(src, locations(1)<<typ, nil)
}

// DecodeWillProperties decodes a CONNECT payload's Will Properties.
func DecodeWillProperties(src []byte) (Properties, []byte, error) {
	return decodeProperties(src, atWill, nil)
}

// ReadStoredProperties reads a complete stored section without rechecking
// publish policy, values or UTF-8. Only checked sections enter the store;
// damaged wire structure is returned to the core as a fatal store error.
// visit shares the structural traversal, for example to locate Message Expiry.
func ReadStoredProperties(src []byte, visit func(PropertyView) error) (Properties, error) {
	return readStoredProperties(src, visit, nil)
}

// FilterStoredProperties shares the stored section's structural traversal
// with omission. Offset gives each property's position in the kept section.
// Unchanged bytes share the source as in Without.
func FilterStoredProperties(src []byte, omit func(PropertyView) bool) (Properties, error) {
	return readStoredProperties(src, nil, omit)
}

func readStoredProperties(src []byte, visit func(PropertyView) error, omit func(PropertyView) bool) (Properties, error) {
	if len(src) == 0 {
		return nil, nil
	}
	p, rest, err := filterProperties(src, 0, visit, omit)
	if err == nil && len(rest) != 0 {
		err = ErrMalformed
	}
	return p, err
}

// A packet may supply its own numeric value check, applied while decoding
// each property. No Property values or strings are allocated while checking.
// The public property decoders check only types, places and repeats.
func decodeProperties(src []byte, location locations, check func(PropertyView) error) (Properties, []byte, error) {
	return filterProperties(src, location, check, nil)
}

func filterProperties(src []byte, location locations, check func(PropertyView) error, omit func(PropertyView) bool) (Properties, []byte, error) {
	length, rest, err := DecodeVarInt(src)
	if err != nil || int(length) > len(rest) {
		return nil, nil, ErrMalformed
	}
	body, rest := rest[:length], rest[length:]
	p, err := checkFilteredProperties(body, location, check, omit)
	if err != nil {
		return nil, nil, err
	}
	return p, rest, nil
}

// checkProperties is shared by decoders and packet encoders carrying raw
// properties. It checks wire types, placement, repeats and packet value rules.
func checkProperties(body []byte, location locations, check func(PropertyView) error) error {
	_, err := checkFilteredProperties(body, location, check, nil)
	return err
}

func checkFilteredProperties(body []byte, location locations, check func(PropertyView) error, omit func(PropertyView) bool) (Properties, error) {
	if len(body) > maxVarInt {
		return nil, ErrMalformed
	}
	original := Properties(body)
	kept := original
	if omit != nil {
		kept = original[:0:0]
	}
	var seen [len(propertyDefinitions)]bool
	length := len(body)
	for len(body) != 0 {
		// Table 2-4 has only one-byte identifiers. A continued or unknown
		// identifier cannot name a property, so it is rejected directly.
		id := body[0]
		def, known := definition(id)
		if !known || (location != 0 && def.allowed&location == 0) {
			return nil, ErrMalformed
		}
		if location != 0 && seen[id] && !repeatable(id, location) {
			return nil, ErrProtocol
		}

		value, rest, err := checkPropertyValue(body[1:], def.kind, location == 0)
		if err != nil {
			return nil, ErrMalformed
		}
		value.ID, value.Offset = id, length-len(body)
		if check != nil {
			if err := check(value); err != nil {
				return nil, err
			}
		}
		if omit != nil {
			value.Offset = len(kept)
			if !omit(value) {
				kept = keepProperty(kept, original, length-len(body), length-len(rest))
			}
		}
		seen[id] = true
		body = rest
	}
	return kept, nil
}

func repeatable(id byte, location locations) bool {
	// User Property repeats everywhere it is allowed; Subscription Identifier
	// repeats only on PUBLISH. Every other repeat is a Protocol Error.
	return id == UserProperty || (id == SubscriptionIdentifier && location == atPUBLISH)
}

func checkPropertyValue(src []byte, kind propertyKind, stored bool) (view PropertyView, rest []byte, err error) {
	readString := decodeStringBytes
	if stored {
		readString = DecodeBinary // structural length check, without validating UTF-8 again
	}
	switch kind {
	case propertyByte:
		if len(src) == 0 {
			return view, nil, ErrMalformed
		}
		view.Int, rest = uint32(src[0]), src[1:]
	case propertyUint16:
		var value uint16
		value, rest, err = DecodeUint16(src)
		view.Int = uint32(value)
	case propertyUint32:
		view.Int, rest, err = DecodeUint32(src)
	case propertyVarInt:
		view.Int, rest, err = DecodeVarInt(src)
	case propertyString:
		view.Value, rest, err = readString(src)
	case propertyBinary:
		view.Value, rest, err = DecodeBinary(src)
	case propertyStringPair:
		view.Name, rest, err = readString(src)
		if err != nil {
			return view, nil, err
		}
		view.Value, rest, err = readString(rest)
	default:
		return view, nil, ErrMalformed
	}
	return view, rest, err
}

// propertyValueRule adapts numeric packet rules to the property reader, so
// connection checks can use string and binary views in that same decode.
func propertyValueRule(valid func(byte, uint32) bool) func(PropertyView) error {
	return func(p PropertyView) error {
		if !valid(p.ID, p.Int) {
			return ErrProtocol
		}
		return nil
	}
}

// propertySectionSize adds a checked section and its Property Length to the
// body size without examining its bytes. The caller supplies bounded lengths.
func propertySectionSize(bodySize int, properties Properties) int {
	propertySize := len(properties)
	return bodySize + varIntSize(uint32(propertySize)) + propertySize
}

// appendCheckedProperties copies an already checked section verbatim, adding
// its Property Length. The packet encoder has grown dst for the whole packet.
func appendCheckedProperties(dst []byte, properties Properties, additions ...Properties) []byte {
	n := len(properties)
	for _, p := range additions {
		n += len(p)
	}
	dst = binaryAppendVarInt(dst, uint32(n))
	dst = append(dst, properties...)
	for _, p := range additions {
		dst = append(dst, p...)
	}
	return dst
}

// AppendProperties appends properties valid for typ in their supplied order.
func AppendProperties(dst []byte, typ Type, properties []Property) ([]byte, error) {
	return appendProperties(dst, locations(1)<<typ, properties)
}

// AppendWillProperties appends a CONNECT payload's Will Properties.
func AppendWillProperties(dst []byte, properties []Property) ([]byte, error) {
	return appendProperties(dst, atWill, properties)
}

// NewProperties makes a property section for CONNACK, PUBLISH, PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK or DISCONNECT in the
// supplied order, without its Property Length. It checks identifiers, places,
// types, repeats and the packet's value rules as that packet's decoder does.
// It allocates the section, copying the values, or
// returns ErrCannotEncode and no section if any property is invalid.
func NewProperties(typ Type, properties []Property) (Properties, error) {
	var validValue func(byte, uint32) bool
	switch typ {
	case CONNACK:
		validValue = validConnackProperty
	case PUBLISH:
		validValue = validPublishProperty
	case SUBSCRIBE:
		validValue = validSubscribeProperty
	}
	bodySize, err := propertiesSize(locations(1)<<typ, properties, validValue)
	if err != nil {
		return nil, err
	}
	return appendPropertyValues(make(Properties, 0, bodySize), properties), nil
}

func appendProperties(dst []byte, location locations, properties []Property) ([]byte, error) {
	bodySize, err := propertiesSize(location, properties, nil)
	if err != nil {
		return dst, err
	}
	dst = slices.Grow(dst, varIntSize(uint32(bodySize))+bodySize)
	dst = binaryAppendVarInt(dst, uint32(bodySize))
	return appendPropertyValues(dst, properties), nil
}

// propertiesSize validates and sizes supplied properties in one pass,
// including packet value rules when the caller supplies them.
func propertiesSize(location locations, properties []Property, validValue func(byte, uint32) bool) (int, error) {
	bodySize := 0
	var seen [len(propertyDefinitions)]bool
	for i := range properties {
		property := &properties[i]
		def, known := definition(property.ID)
		if !known || def.allowed&location == 0 || (seen[property.ID] && !repeatable(property.ID, location)) {
			return 0, ErrCannotEncode
		}
		size, valid := propertySize(property, def.kind)
		if !valid || (validValue != nil && !validValue(property.ID, property.Int)) || size > maxVarInt-bodySize {
			return 0, ErrCannotEncode
		}
		bodySize += size
		seen[property.ID] = true
	}
	return bodySize, nil
}

// appendPropertyValues writes values already checked by propertiesSize,
// without a Property Length. The caller has grown dst for the section.
func appendPropertyValues(dst []byte, properties []Property) []byte {
	for i := range properties {
		property := &properties[i]
		dst = appendProperty(dst, property, propertyDefinitions[property.ID].kind)
	}
	return dst
}

func appendProperty(dst []byte, property *Property, kind propertyKind) []byte {
	dst = append(dst, property.ID) // Every identifier in Table 2-4 is one byte.
	switch kind {
	case propertyByte:
		return append(dst, byte(property.Int))
	case propertyUint16:
		return AppendUint16(dst, uint16(property.Int))
	case propertyUint32:
		return AppendUint32(dst, property.Int)
	case propertyVarInt:
		return binaryAppendVarInt(dst, property.Int)
	case propertyString:
		dst = AppendUint16(dst, uint16(len(property.String)))
		return append(dst, property.String...)
	case propertyBinary:
		dst = AppendUint16(dst, uint16(len(property.Binary)))
		return append(dst, property.Binary...)
	case propertyStringPair:
		dst = AppendUint16(dst, uint16(len(property.Pair.Name)))
		dst = append(dst, property.Pair.Name...)
		dst = AppendUint16(dst, uint16(len(property.Pair.Value)))
		return append(dst, property.Pair.Value...)
	default:
		panic("packet: unknown property kind")
	}
}

func propertySize(property *Property, kind propertyKind) (int, bool) {
	switch kind {
	case propertyByte:
		return 2, property.Int <= math.MaxUint8
	case propertyUint16:
		return 3, property.Int <= math.MaxUint16
	case propertyUint32:
		return 5, true
	case propertyVarInt:
		return 1 + varIntSize(property.Int), property.Int <= maxVarInt
	case propertyString:
		if !validString(property.String) {
			return 0, false
		}
		return 3 + len(property.String), true
	case propertyBinary:
		return 3 + len(property.Binary), len(property.Binary) <= math.MaxUint16
	case propertyStringPair:
		if !validString(property.Pair.Name) || !validString(property.Pair.Value) {
			return 0, false
		}
		return 5 + len(property.Pair.Name) + len(property.Pair.Value), true
	default:
		return 0, false
	}
}

// binaryAppendVarInt is used only after the MQTT range has been checked.
func binaryAppendVarInt(dst []byte, value uint32) []byte {
	return binary.AppendUvarint(dst, uint64(value))
}
