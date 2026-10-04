// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/ifnesi/saguin/internal/mqtt/mempool"
)

const (
	PropPayloadFormat          byte = 1
	PropMessageExpiryInterval  byte = 2
	PropContentType            byte = 3
	PropResponseTopic          byte = 8
	PropCorrelationData        byte = 9
	PropSubscriptionIdentifier byte = 11
	PropSessionExpiryInterval  byte = 17
	PropAssignedClientID       byte = 18
	PropServerKeepAlive        byte = 19
	PropAuthenticationMethod   byte = 21
	PropAuthenticationData     byte = 22
	PropRequestProblemInfo     byte = 23
	PropWillDelayInterval      byte = 24
	PropRequestResponseInfo    byte = 25
	PropResponseInfo           byte = 26
	PropServerReference        byte = 28
	PropReasonString           byte = 31
	PropReceiveMaximum         byte = 33
	PropTopicAliasMaximum      byte = 34
	PropTopicAlias             byte = 35
	PropMaximumQos             byte = 36
	PropRetainAvailable        byte = 37
	PropUser                   byte = 38
	PropMaximumPacketSize      byte = 39
	PropWildcardSubAvailable   byte = 40
	PropSubIDAvailable         byte = 41
	PropSharedSubAvailable     byte = 42
)

// propKind is the data type of a property's value, as MQTT 5 section 2.2.2.2
// names it.
type propKind byte

const (
	kByte   propKind = iota // Byte
	kU16                    // Two Byte Integer
	kU32                    // Four Byte Integer
	kVBI                    // Variable Byte Integer
	kString                 // UTF-8 Encoded String
	kBinary                 // Binary Data
	kPair                   // UTF-8 String Pair
)

func (k propKind) String() string {
	return [...]string{"byte", "u16", "u32", "vbi", "str", "bin", "pair"}[k]
}

// propRule is one row of MQTT 5 section 2.2.2.2, Table 2-4, with what each
// property's own section adds to it.
type propRule struct {
	name string
	kind propKind
	// on holds a bit per packet type (pktBit) that may carry the property.
	on uint32
	// repeat holds the packet types on which it may appear more than once.
	// Zero for nearly every property: "It is a Protocol Error to include
	// the X more than once".
	repeat uint32
	// ranged says lo..hi are the values it may take, and bad is what a
	// value outside them is refused with. Only numeric kinds are ranged.
	ranged bool
	lo, hi uint32
	bad    Code
	// with is a property that must accompany this one on the packet types
	// in withOn, and required says on which packet types this one must be
	// present at all.
	with     byte
	withOn   uint32
	required uint32
}

// pktBit is the bit a packet type has in propRule.on: one per control
// packet type, and bit 16 for the Will Properties of a CONNECT.
func pktBit(pkt byte) uint32 {
	switch {
	case pkt <= Auth:
		return 1 << pkt
	case pkt == WillProperties:
		return 1 << 16
	}
	return 0
}

func pkts(types ...byte) uint32 {
	var m uint32
	for _, t := range types {
		m |= pktBit(t)
	}
	return m
}

// validPacketProperties is MQTT 5 section 2.2.2.2, Table 2-4: **the one
// statement of which properties exist, which packets may carry each, whether
// it may repeat, and what values it may take.** Properties.Decode refuses a
// property block that disagrees with it, and canEncode writes none that does.
// It is indexed by property identifier; a row with no packets is an
// identifier the specification does not define.
//
// The packets come from the table. The rest comes from each property's own
// section, quoted here so a row can be read against the text:
//
//   - every property but two: "It is a Protocol Error to include the X more
//     than once". User Property "is allowed to appear multiple times"
//     (3.1.2.11.8) and a PUBLISH may carry several Subscription Identifiers
//     (3.3.2.3.8), though a SUBSCRIBE only one (3.8.2.1.2).
//   - Payload Format Indicator: 0 or 1 (3.3.2.3.2), a value of 2 or more
//     being refused on section 1.2's definition of a Protocol Error, since
//     3.3.2.3.2 says no more than that. Request Response
//     Information, Request Problem Information, Maximum QoS, Retain
//     Available, Wildcard Subscription Available, Subscription Identifier
//     Available and Shared Subscription Available: "a value other than 0 or
//     1" is a Protocol Error (3.1.2.11.6, 3.1.2.11.7, 3.2.2.3.4, 3.2.2.3.5,
//     3.2.2.3.11, 3.2.2.3.12, 3.2.2.3.13).
//   - Receive Maximum and Maximum Packet Size: not 0 (3.1.2.11.3,
//     3.1.2.11.4, 3.2.2.3.3, 3.2.2.3.9).
//   - Subscription Identifier: 1 to 268,435,455 (3.3.2.3.8, 3.8.2.1.2).
//   - Topic Alias: not 0, and a 0 is answered with 0x94 (3.3.4).
//   - Authentication Data needs an Authentication Method on a CONNECT
//     (3.1.2.11.10), and an AUTH must carry one (3.15.2.2.2).
//
// An identifier a packet may not carry, and one that is not defined, are a
// Malformed Packet (2.2.2.2); the rest are Protocol Errors.
var validPacketProperties = [PropSharedSubAvailable + 1]propRule{
	PropPayloadFormat:          {name: "Payload Format Indicator", kind: kByte, on: pkts(Publish, WillProperties), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropMessageExpiryInterval:  {name: "Message Expiry Interval", kind: kU32, on: pkts(Publish, WillProperties)},
	PropContentType:            {name: "Content Type", kind: kString, on: pkts(Publish, WillProperties)},
	PropResponseTopic:          {name: "Response Topic", kind: kString, on: pkts(Publish, WillProperties)},
	PropCorrelationData:        {name: "Correlation Data", kind: kBinary, on: pkts(Publish, WillProperties)},
	PropSubscriptionIdentifier: {name: "Subscription Identifier", kind: kVBI, on: pkts(Publish, Subscribe), repeat: pkts(Publish), ranged: true, lo: 1, hi: 268435455, bad: ErrProtocolViolationZeroSubID},
	PropSessionExpiryInterval:  {name: "Session Expiry Interval", kind: kU32, on: pkts(Connect, Connack, Disconnect)},
	PropAssignedClientID:       {name: "Assigned Client Identifier", kind: kString, on: pkts(Connack)},
	PropServerKeepAlive:        {name: "Server Keep Alive", kind: kU16, on: pkts(Connack)},
	PropAuthenticationMethod:   {name: "Authentication Method", kind: kString, on: pkts(Connect, Connack, Auth), required: pkts(Auth)},
	PropAuthenticationData:     {name: "Authentication Data", kind: kBinary, on: pkts(Connect, Connack, Auth), with: PropAuthenticationMethod, withOn: pkts(Connect)},
	PropRequestProblemInfo:     {name: "Request Problem Information", kind: kByte, on: pkts(Connect), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropWillDelayInterval:      {name: "Will Delay Interval", kind: kU32, on: pkts(WillProperties)},
	PropRequestResponseInfo:    {name: "Request Response Information", kind: kByte, on: pkts(Connect), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropResponseInfo:           {name: "Response Information", kind: kString, on: pkts(Connack)},
	PropServerReference:        {name: "Server Reference", kind: kString, on: pkts(Connack, Disconnect)},
	PropReasonString:           {name: "Reason String", kind: kString, on: pkts(Connack, Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback, Disconnect, Auth)},
	PropReceiveMaximum:         {name: "Receive Maximum", kind: kU16, on: pkts(Connect, Connack), ranged: true, lo: 1, hi: math.MaxUint16, bad: ErrProtocolViolationZeroReceiveMaximum},
	PropTopicAliasMaximum:      {name: "Topic Alias Maximum", kind: kU16, on: pkts(Connect, Connack)},
	PropTopicAlias:             {name: "Topic Alias", kind: kU16, on: pkts(Publish), ranged: true, lo: 1, hi: math.MaxUint16, bad: ErrTopicAliasInvalid},
	PropMaximumQos:             {name: "Maximum QoS", kind: kByte, on: pkts(Connack), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropRetainAvailable:        {name: "Retain Available", kind: kByte, on: pkts(Connack), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropUser:                   {name: "User Property", kind: kPair, on: pkts(Connect, Connack, Publish, Puback, Pubrec, Pubrel, Pubcomp, Subscribe, Suback, Unsubscribe, Unsuback, Disconnect, Auth, WillProperties), repeat: math.MaxUint32},
	PropMaximumPacketSize:      {name: "Maximum Packet Size", kind: kU32, on: pkts(Connect, Connack), ranged: true, lo: 1, hi: math.MaxUint32, bad: ErrProtocolViolationZeroMaximumPacketSize},
	PropWildcardSubAvailable:   {name: "Wildcard Subscription Available", kind: kByte, on: pkts(Connack), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropSubIDAvailable:         {name: "Subscription Identifier Available", kind: kByte, on: pkts(Connack), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
	PropSharedSubAvailable:     {name: "Shared Subscription Available", kind: kByte, on: pkts(Connack), ranged: true, lo: 0, hi: 1, bad: ErrProtocolViolationPropertyValue},
}

// constrainedProperties are the identifiers whose row says more than "this
// packet may carry it": one that another must accompany, or one a packet must
// carry. Found once, so a packet's properties are not swept for them.
var constrainedProperties = func() (ids []byte) {
	for id := range validPacketProperties {
		if r := &validPacketProperties[id]; r.with != 0 || r.required != 0 {
			ids = append(ids, byte(id))
		}
	}
	return ids
}()

// UserProperty is an arbitrary key-value pair for a packet user properties array.
type UserProperty struct { // [MQTT-1.5.7-1]
	Key string
	Val string
}

// Properties contains all mqtt v5 properties available for a packet.
// Some properties have valid values of 0 or not-present. In this case, we opt for
// property flags to indicate the usage of property.
// Refer to mqtt v5 2.2.2.2 Property spec for more information.
type Properties struct {
	CorrelationData           []byte
	SubscriptionIdentifier    []int
	AuthenticationData        []byte
	User                      []UserProperty
	ContentType               string
	ResponseTopic             string
	AssignedClientID          string
	AuthenticationMethod      string
	ResponseInfo              string
	ServerReference           string
	ReasonString              string
	MessageExpiryInterval     uint32
	SessionExpiryInterval     uint32
	WillDelayInterval         uint32
	MaximumPacketSize         uint32
	ServerKeepAlive           uint16
	ReceiveMaximum            uint16
	TopicAliasMaximum         uint16
	TopicAlias                uint16
	PayloadFormat             byte
	PayloadFormatFlag         bool
	SessionExpiryIntervalFlag bool
	ServerKeepAliveFlag       bool
	ReceiveMaximumFlag        bool
	MaximumPacketSizeFlag     bool
	RequestProblemInfo        byte
	RequestProblemInfoFlag    bool
	RequestResponseInfo       byte
	TopicAliasFlag            bool
	MaximumQos                byte
	MaximumQosFlag            bool
	RetainAvailable           byte
	RetainAvailableFlag       bool
	WildcardSubAvailable      byte
	WildcardSubAvailableFlag  bool
	SubIDAvailable            byte
	SubIDAvailableFlag        bool
	SharedSubAvailable        byte
	SharedSubAvailableFlag    bool

	// **Present is not the same as non-empty.** A UTF-8 Encoded String and
	// Binary Data may have a length of zero, and a property sent that way is
	// there: [MQTT-3.3.2-15], [MQTT-3.3.2-16] and [MQTT-3.3.2-20] have the
	// server send the Response Topic, Correlation Data and Content Type
	// unaltered. Decode sets the flag when the value is empty, so what came
	// in goes out as it came (the way PayloadFormatFlag does for the one
	// byte property) and a property with bytes decodes as it always did; a
	// packet built in code sets only the value, and a non-empty value is
	// written whatever the flag says.
	CorrelationDataFlag      bool
	AuthenticationDataFlag   bool
	ContentTypeFlag          bool
	ResponseTopicFlag        bool
	AssignedClientIDFlag     bool
	AuthenticationMethodFlag bool
	ResponseInfoFlag         bool
	ServerReferenceFlag      bool
	ReasonStringFlag         bool
}

// Copy creates a new Properties struct with copies of the values.
func (p *Properties) Copy(allowTransfer bool) Properties {
	pr := Properties{
		PayloadFormat:             p.PayloadFormat, // [MQTT-3.3.2-4]
		PayloadFormatFlag:         p.PayloadFormatFlag,
		MessageExpiryInterval:     p.MessageExpiryInterval,
		ContentType:               p.ContentType, // [MQTT-3.3.2-20]
		ContentTypeFlag:           p.ContentTypeFlag,
		ResponseTopic:             p.ResponseTopic, // [MQTT-3.3.2-15]
		ResponseTopicFlag:         p.ResponseTopicFlag,
		AssignedClientIDFlag:      p.AssignedClientIDFlag,
		AuthenticationMethodFlag:  p.AuthenticationMethodFlag,
		ResponseInfoFlag:          p.ResponseInfoFlag,
		ServerReferenceFlag:       p.ServerReferenceFlag,
		ReasonStringFlag:          p.ReasonStringFlag,
		SessionExpiryInterval:     p.SessionExpiryInterval,
		SessionExpiryIntervalFlag: p.SessionExpiryIntervalFlag,
		AssignedClientID:          p.AssignedClientID,
		ServerKeepAlive:           p.ServerKeepAlive,
		ServerKeepAliveFlag:       p.ServerKeepAliveFlag,
		AuthenticationMethod:      p.AuthenticationMethod,
		RequestProblemInfo:        p.RequestProblemInfo,
		RequestProblemInfoFlag:    p.RequestProblemInfoFlag,
		WillDelayInterval:         p.WillDelayInterval,
		RequestResponseInfo:       p.RequestResponseInfo,
		ResponseInfo:              p.ResponseInfo,
		ServerReference:           p.ServerReference,
		ReasonString:              p.ReasonString,
		ReceiveMaximum:            p.ReceiveMaximum,
		ReceiveMaximumFlag:        p.ReceiveMaximumFlag,
		TopicAliasMaximum:         p.TopicAliasMaximum,
		TopicAlias:                0, // NB; do not copy topic alias [MQTT-3.3.2-7] + we do not send to clients (currently) [MQTT-3.1.2-26] [MQTT-3.1.2-27]
		MaximumQos:                p.MaximumQos,
		MaximumQosFlag:            p.MaximumQosFlag,
		RetainAvailable:           p.RetainAvailable,
		RetainAvailableFlag:       p.RetainAvailableFlag,
		MaximumPacketSize:         p.MaximumPacketSize,
		MaximumPacketSizeFlag:     p.MaximumPacketSizeFlag,
		WildcardSubAvailable:      p.WildcardSubAvailable,
		WildcardSubAvailableFlag:  p.WildcardSubAvailableFlag,
		SubIDAvailable:            p.SubIDAvailable,
		SubIDAvailableFlag:        p.SubIDAvailableFlag,
		SharedSubAvailable:        p.SharedSubAvailable,
		SharedSubAvailableFlag:    p.SharedSubAvailableFlag,
	}

	if allowTransfer {
		pr.TopicAlias = p.TopicAlias
		pr.TopicAliasFlag = p.TopicAliasFlag
	}

	if p.CorrelationDataFlag || len(p.CorrelationData) > 0 {
		pr.CorrelationData = append([]byte{}, p.CorrelationData...) // [MQTT-3.3.2-16]
		pr.CorrelationDataFlag = p.CorrelationDataFlag
	}

	if len(p.SubscriptionIdentifier) > 0 {
		pr.SubscriptionIdentifier = append([]int{}, p.SubscriptionIdentifier...)
	}

	if p.AuthenticationDataFlag || len(p.AuthenticationData) > 0 {
		pr.AuthenticationData = append([]byte{}, p.AuthenticationData...)
		pr.AuthenticationDataFlag = p.AuthenticationDataFlag
	}

	if len(p.User) > 0 {
		pr.User = []UserProperty{}
		for _, v := range p.User {
			pr.User = append(pr.User, UserProperty{ // [MQTT-3.3.2-17]
				Key: v.Key,
				Val: v.Val,
			})
		}
	}

	return pr
}

// canEncode returns true if the property type is valid for the packet type.
func (p *Properties) canEncode(pkt byte, k byte) bool {
	return encodable[k][pkt]
}

// encodable is validPacketProperties as a table indexed by property and
// packet type, which is what canEncode reads.
//
// **An array because canEncode runs for every property on every packet
// written**, whatever the packet carries, and as two map lookups each time
// it was 2.3 µs of a QoS 1 message's broker CPU at 10,000 msg/s: a PUBACK
// took 889 ns to encode, and takes 172 ns from this table. Every byte pair
// has an entry, so no index falls outside it. validPacketProperties stays
// the one statement of which property goes on which packet; this is built
// from it and holds nothing of its own.
var encodable = encodableFrom(&validPacketProperties)

func encodableFrom(rules *[PropSharedSubAvailable + 1]propRule) *[256][256]bool {
	t := new([256][256]bool)
	for k := range rules {
		for pkt := range 256 {
			t[k][pkt] = rules[k].on&pktBit(byte(pkt)) != 0
		}
	}
	return t
}

// Encode encodes properties into a bytes buffer.
func (p *Properties) Encode(pkt byte, mods Mods, b *bytes.Buffer, n int) {
	if p == nil {
		return
	}

	buf := mempool.GetBuffer()
	defer mempool.PutBuffer(buf)
	if p.canEncode(pkt, PropPayloadFormat) && p.PayloadFormatFlag {
		buf.WriteByte(PropPayloadFormat)
		buf.WriteByte(p.PayloadFormat)
	}

	if p.canEncode(pkt, PropMessageExpiryInterval) && p.MessageExpiryInterval > 0 {
		buf.WriteByte(PropMessageExpiryInterval)
		buf.Write(encodeUint32(p.MessageExpiryInterval))
	}

	if p.canEncode(pkt, PropContentType) && (p.ContentTypeFlag || p.ContentType != "") {
		buf.WriteByte(PropContentType)
		buf.Write(encodeString(p.ContentType)) // [MQTT-3.3.2-19]
	}

	if mods.AllowResponseInfo && p.canEncode(pkt, PropResponseTopic) &&
		(p.ResponseTopicFlag || p.ResponseTopic != "") && !strings.ContainsAny(p.ResponseTopic, "+#") { // [MQTT-3.3.2-14]
		buf.WriteByte(PropResponseTopic)
		buf.Write(encodeString(p.ResponseTopic)) // [MQTT-3.3.2-13]
	}

	if mods.AllowResponseInfo && p.canEncode(pkt, PropCorrelationData) && (p.CorrelationDataFlag || len(p.CorrelationData) > 0) { // [MQTT-3.3.2-16]
		buf.WriteByte(PropCorrelationData)
		buf.Write(encodeBytes(p.CorrelationData))
	}

	if p.canEncode(pkt, PropSubscriptionIdentifier) && len(p.SubscriptionIdentifier) > 0 {
		for _, v := range p.SubscriptionIdentifier {
			if v > 0 {
				buf.WriteByte(PropSubscriptionIdentifier)
				encodeLength(buf, int64(v))
			}
		}
	}

	if p.canEncode(pkt, PropSessionExpiryInterval) && p.SessionExpiryIntervalFlag { // [MQTT-3.14.2-2]
		buf.WriteByte(PropSessionExpiryInterval)
		buf.Write(encodeUint32(p.SessionExpiryInterval))
	}

	if p.canEncode(pkt, PropAssignedClientID) && (p.AssignedClientIDFlag || p.AssignedClientID != "") {
		buf.WriteByte(PropAssignedClientID)
		buf.Write(encodeString(p.AssignedClientID))
	}

	if p.canEncode(pkt, PropServerKeepAlive) && p.ServerKeepAliveFlag {
		buf.WriteByte(PropServerKeepAlive)
		buf.Write(encodeUint16(p.ServerKeepAlive))
	}

	if p.canEncode(pkt, PropAuthenticationMethod) && (p.AuthenticationMethodFlag || p.AuthenticationMethod != "") {
		buf.WriteByte(PropAuthenticationMethod)
		buf.Write(encodeString(p.AuthenticationMethod))
	}

	if p.canEncode(pkt, PropAuthenticationData) && (p.AuthenticationDataFlag || len(p.AuthenticationData) > 0) {
		buf.WriteByte(PropAuthenticationData)
		buf.Write(encodeBytes(p.AuthenticationData))
	}

	if p.canEncode(pkt, PropRequestProblemInfo) && p.RequestProblemInfoFlag {
		buf.WriteByte(PropRequestProblemInfo)
		buf.WriteByte(p.RequestProblemInfo)
	}

	if p.canEncode(pkt, PropWillDelayInterval) && p.WillDelayInterval > 0 {
		buf.WriteByte(PropWillDelayInterval)
		buf.Write(encodeUint32(p.WillDelayInterval))
	}

	if p.canEncode(pkt, PropRequestResponseInfo) && p.RequestResponseInfo > 0 {
		buf.WriteByte(PropRequestResponseInfo)
		buf.WriteByte(p.RequestResponseInfo)
	}

	if mods.AllowResponseInfo && p.canEncode(pkt, PropResponseInfo) && (p.ResponseInfoFlag || len(p.ResponseInfo) > 0) { // [MQTT-3.1.2-28]
		buf.WriteByte(PropResponseInfo)
		buf.Write(encodeString(p.ResponseInfo))
	}

	if p.canEncode(pkt, PropServerReference) && (p.ServerReferenceFlag || len(p.ServerReference) > 0) {
		buf.WriteByte(PropServerReference)
		buf.Write(encodeString(p.ServerReference))
	}

	// The Reason String and User Properties are held aside and placed after
	// everything else is encoded, in the order they have always been
	// written, because whether they fit depends on what follows them.
	rb := mempool.GetBuffer()
	defer mempool.PutBuffer(rb)
	if !mods.DisallowProblemInfo && p.canEncode(pkt, PropReasonString) && (p.ReasonStringFlag || p.ReasonString != "") {
		rb.WriteByte(PropReasonString)
		rb.Write(encodeString(p.ReasonString))
	}

	mid := mempool.GetBuffer()
	defer mempool.PutBuffer(mid)
	if p.canEncode(pkt, PropReceiveMaximum) && p.ReceiveMaximum > 0 {
		mid.WriteByte(PropReceiveMaximum)
		mid.Write(encodeUint16(p.ReceiveMaximum))
	}

	if p.canEncode(pkt, PropTopicAliasMaximum) && p.TopicAliasMaximum > 0 {
		mid.WriteByte(PropTopicAliasMaximum)
		mid.Write(encodeUint16(p.TopicAliasMaximum))
	}

	if p.canEncode(pkt, PropTopicAlias) && p.TopicAliasFlag && p.TopicAlias > 0 { // [MQTT-3.3.2-8]
		mid.WriteByte(PropTopicAlias)
		mid.Write(encodeUint16(p.TopicAlias))
	}

	if p.canEncode(pkt, PropMaximumQos) && p.MaximumQosFlag && p.MaximumQos < 2 {
		mid.WriteByte(PropMaximumQos)
		mid.WriteByte(p.MaximumQos)
	}

	if p.canEncode(pkt, PropRetainAvailable) && p.RetainAvailableFlag {
		mid.WriteByte(PropRetainAvailable)
		mid.WriteByte(p.RetainAvailable)
	}

	ub := mempool.GetBuffer()
	defer mempool.PutBuffer(ub)
	if !mods.DisallowProblemInfo && p.canEncode(pkt, PropUser) {
		for _, v := range p.User {
			ub.WriteByte(PropUser)
			ub.Write(encodeString(v.Key))
			ub.Write(encodeString(v.Val))
		}
	}

	tail := mempool.GetBuffer()
	defer mempool.PutBuffer(tail)
	if p.canEncode(pkt, PropMaximumPacketSize) && p.MaximumPacketSize > 0 {
		tail.WriteByte(PropMaximumPacketSize)
		tail.Write(encodeUint32(p.MaximumPacketSize))
	}

	if p.canEncode(pkt, PropWildcardSubAvailable) && p.WildcardSubAvailableFlag {
		tail.WriteByte(PropWildcardSubAvailable)
		tail.WriteByte(p.WildcardSubAvailable)
	}

	if p.canEncode(pkt, PropSubIDAvailable) && p.SubIDAvailableFlag {
		tail.WriteByte(PropSubIDAvailable)
		tail.WriteByte(p.SubIDAvailable)
	}

	if p.canEncode(pkt, PropSharedSubAvailable) && p.SharedSubAvailableFlag {
		tail.WriteByte(PropSharedSubAvailable)
		tail.WriteByte(p.SharedSubAvailable)
	}

	// **An optional property goes in only if the whole packet still fits**:
	// [MQTT-3.2.2-19] [MQTT-3.14.2-3] [MQTT-3.4.2-2] [MQTT-3.5.2-2]
	// [MQTT-3.6.2-2] [MQTT-3.9.2-1] [MQTT-3.11.2-1] [MQTT-3.15.2-2] for the
	// Reason String, and [MQTT-3.2.2-20] [MQTT-3.14.2-4] [MQTT-3.4.2-3]
	// [MQTT-3.5.2-3] [MQTT-3.6.2-3] [MQTT-3.9.2-2] [MQTT-3.11.2-2]
	// [MQTT-3.15.2-3] for User Properties - "if it would increase the size of
	// the packet beyond the Maximum Packet Size". The size is the whole
	// packet (§3.1.2.11.4): the fixed header byte, the Remaining Length's own
	// bytes, the n bytes of the packet that are not properties, the property
	// length's own bytes, and every property - including the ones encoded
	// after these two, which a check made where each was written could not
	// see. mosquitto's packet__check_oversize counts the same.
	//
	// The Reason String is offered first, as it always was. Each is decided
	// on its own, so a User Property that fits is still sent when the Reason
	// String did not.
	//
	// **A PUBLISH is never trimmed, and neither is a CONNECT.** Their User
	// Properties are the sender's data (MQTT-3.3.2-18), so a PUBLISH too
	// large for a client is discarded whole (MQTT-3.1.2-25) by the writer's
	// check on the finished packet, rather than delivered without them.
	props := buf.Len() + mid.Len() + tail.Len()
	fits := func(with int) bool {
		if mods.MaxSize == 0 || !trimsOptionalProperties(pkt) {
			return true
		}
		remaining := n + lengthBytes(props+with) + props + with
		return uint64(1+lengthBytes(remaining)+remaining) <= uint64(mods.MaxSize)
	}
	keepReason := rb.Len() > 0 && fits(rb.Len())
	if keepReason {
		props += rb.Len()
	}
	keepUser := ub.Len() > 0 && fits(ub.Len())
	if keepUser {
		props += ub.Len()
	}

	encodeLength(b, int64(props))
	b.Write(buf.Bytes()) // [MQTT-3.1.3-10]
	if keepReason {
		b.Write(rb.Bytes())
	}
	b.Write(mid.Bytes())
	if keepUser {
		b.Write(ub.Bytes())
	}
	b.Write(tail.Bytes())
}

// trimsOptionalProperties is whether a packet of this type drops its Reason
// String and User Properties to fit the receiver's Maximum Packet Size: the
// acknowledgements, DISCONNECT and AUTH, which MQTT 5 lets do so. Written as
// the closed set, so a packet type added later has to be considered rather
// than inherited.
func trimsOptionalProperties(pkt byte) bool {
	switch pkt {
	case Connack, Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback, Disconnect, Auth:
		return true
	}
	return false
}

// lengthBytes is how many bytes a Variable Byte Integer takes for n.
func lengthBytes(n int) int {
	switch {
	case n < 128:
		return 1
	case n < 16384:
		return 2
	case n < 2097152:
		return 3
	}
	return 4
}

// Decode decodes property bytes into a properties struct, and refuses a block
// that disagrees with validPacketProperties: an identifier that is not
// defined or not for this packet type (Malformed Packet, 2.2.2.2), one
// repeated that may not be, a value outside what its property allows, one
// that must have another beside it or must be present, and a property that
// runs past the Property Length that bounds it.
//
// **Every property is checked as it is read, against one table**, with a
// 64-bit mask of those seen so far: a repeat costs a bit test, and a
// property's rule is one array index.
//
// A refusal that is a Protocol Error is returned as its Code and not
// wrapped, which is what lets a caller answer with the code the specification
// names (propertiesFailure).
func (p *Properties) Decode(pkt byte, b *bytes.Buffer) (n int, err error) {
	if p == nil {
		return 0, nil
	}

	var bu int
	n, bu, err = DecodeLength(b)
	if err != nil {
		return n + bu, err
	}

	pb := pktBit(pkt)
	var seen uint64
	if n > 0 {
		// **The block is the Property Length and nothing past it.** A
		// property read from the bytes beyond it is the next field of the
		// packet read as a property, and the field is then lost to its own
		// decoder.
		bt := b.Bytes()
		if n > len(bt) {
			return n + bu, ErrMalformedOffsetByteOutOfRange
		}
		bt = bt[:n]

		var k byte
		for offset := 0; offset < n; {
			k, offset, err = decodeByte(bt, offset)
			if err != nil {
				return n + bu, err
			}

			if int(k) >= len(validPacketProperties) || validPacketProperties[k].on&pb == 0 {
				return n + bu, fmt.Errorf("property type %v not valid for packet type %v: %w", k, pkt, ErrMalformedUnsupportedProperty)
			}

			r := &validPacketProperties[k]
			bit := uint64(1) << k
			if seen&bit != 0 && r.repeat&pb == 0 {
				return n + bu, ErrProtocolViolationRepeatedProperty
			}
			seen |= bit

			// Every number is read once, here, and checked against its range.
			var v uint32
			switch r.kind {
			case kByte:
				var x byte
				x, offset, err = decodeByte(bt, offset)
				v = uint32(x)
			case kU16:
				var x uint16
				x, offset, err = decodeUint16(bt, offset)
				v = uint32(x)
			case kU32:
				v, offset, err = decodeUint32(bt, offset)
			case kVBI:
				var bv int
				var x int
				x, bv, err = DecodeLength(bytes.NewBuffer(bt[offset:]))
				v = uint32(x)
				offset += bv
			}
			if err != nil {
				return n + bu, err
			}
			if r.ranged && (v < r.lo || v > r.hi) {
				return n + bu, r.bad
			}

			switch k {
			case PropPayloadFormat:
				p.PayloadFormat = byte(v)
				p.PayloadFormatFlag = true
			case PropMessageExpiryInterval:
				p.MessageExpiryInterval = v
			case PropContentType:
				p.ContentType, offset, err = decodeString(bt, offset)
				p.ContentTypeFlag = len(p.ContentType) == 0
			case PropResponseTopic:
				p.ResponseTopic, offset, err = decodeString(bt, offset)
				p.ResponseTopicFlag = len(p.ResponseTopic) == 0
			case PropCorrelationData:
				p.CorrelationData, offset, err = decodeBytes(bt, offset)
				p.CorrelationDataFlag = len(p.CorrelationData) == 0
			case PropSubscriptionIdentifier:
				if p.SubscriptionIdentifier == nil {
					p.SubscriptionIdentifier = []int{}
				}
				p.SubscriptionIdentifier = append(p.SubscriptionIdentifier, int(v))
			case PropSessionExpiryInterval:
				p.SessionExpiryInterval = v
				p.SessionExpiryIntervalFlag = true
			case PropAssignedClientID:
				p.AssignedClientID, offset, err = decodeString(bt, offset)
				p.AssignedClientIDFlag = len(p.AssignedClientID) == 0
			case PropServerKeepAlive:
				p.ServerKeepAlive = uint16(v)
				p.ServerKeepAliveFlag = true
			case PropAuthenticationMethod:
				p.AuthenticationMethod, offset, err = decodeString(bt, offset)
				p.AuthenticationMethodFlag = len(p.AuthenticationMethod) == 0
			case PropAuthenticationData:
				p.AuthenticationData, offset, err = decodeBytes(bt, offset)
				p.AuthenticationDataFlag = len(p.AuthenticationData) == 0
			case PropRequestProblemInfo:
				p.RequestProblemInfo = byte(v)
				p.RequestProblemInfoFlag = true
			case PropWillDelayInterval:
				p.WillDelayInterval = v
			case PropRequestResponseInfo:
				p.RequestResponseInfo = byte(v)
			case PropResponseInfo:
				p.ResponseInfo, offset, err = decodeString(bt, offset)
				p.ResponseInfoFlag = len(p.ResponseInfo) == 0
			case PropServerReference:
				p.ServerReference, offset, err = decodeString(bt, offset)
				p.ServerReferenceFlag = len(p.ServerReference) == 0
			case PropReasonString:
				p.ReasonString, offset, err = decodeString(bt, offset)
				p.ReasonStringFlag = len(p.ReasonString) == 0
			case PropReceiveMaximum:
				p.ReceiveMaximum = uint16(v)
				p.ReceiveMaximumFlag = true
			case PropTopicAliasMaximum:
				p.TopicAliasMaximum = uint16(v)
			case PropTopicAlias:
				p.TopicAlias = uint16(v)
				p.TopicAliasFlag = true
			case PropMaximumQos:
				p.MaximumQos = byte(v)
				p.MaximumQosFlag = true
			case PropRetainAvailable:
				p.RetainAvailable = byte(v)
				p.RetainAvailableFlag = true
			case PropUser:
				var k, v string
				k, offset, err = decodeString(bt, offset)
				if err != nil {
					return n + bu, err
				}
				v, offset, err = decodeString(bt, offset)
				p.User = append(p.User, UserProperty{Key: k, Val: v})
			case PropMaximumPacketSize:
				p.MaximumPacketSize = v
				p.MaximumPacketSizeFlag = true
			case PropWildcardSubAvailable:
				p.WildcardSubAvailable = byte(v)
				p.WildcardSubAvailableFlag = true
			case PropSubIDAvailable:
				p.SubIDAvailable = byte(v)
				p.SubIDAvailableFlag = true
			case PropSharedSubAvailable:
				p.SharedSubAvailable = byte(v)
				p.SharedSubAvailableFlag = true
			}

			if err != nil {
				return n + bu, err
			}
		}
	}

	for _, id := range constrainedProperties {
		r := &validPacketProperties[id]
		has := seen&(uint64(1)<<id) != 0
		if has && r.withOn&pb != 0 && seen&(uint64(1)<<r.with) == 0 {
			return n + bu, ErrProtocolViolationMissingProperty
		}
		if !has && r.required&pb != 0 {
			return n + bu, ErrProtocolViolationMissingProperty
		}
	}

	return n + bu, nil
}

// propertiesFailure is the error a packet decoder returns for a failure of
// Properties.Decode. A Protocol Error or other code the table chose is
// returned as it is, because it is the reason code the specification names
// for that mistake; anything else is a Malformed Packet and is wrapped in
// the given code, as it always was.
func propertiesFailure(err error, malformed Code) error {
	var c Code
	if errors.As(err, &c) && c.Code != ErrMalformedPacket.Code {
		return c
	}
	return fmt.Errorf("%s: %w", err, malformed)
}
