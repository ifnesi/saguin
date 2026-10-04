// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co
// SPDX-FileContributor: ChrisJr404

package packets

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/ifnesi/saguin/internal/mqtt/mempool"
)

// All valid packet types and their packet identifiers.
const (
	Reserved       byte = iota // 0 - we use this in packet tests to indicate special-test or all packets.
	Connect                    // 1
	Connack                    // 2
	Publish                    // 3
	Puback                     // 4
	Pubrec                     // 5
	Pubrel                     // 6
	Pubcomp                    // 7
	Subscribe                  // 8
	Suback                     // 9
	Unsubscribe                // 10
	Unsuback                   // 11
	Pingreq                    // 12
	Pingresp                   // 13
	Disconnect                 // 14
	Auth                       // 15
	WillProperties byte = 99   // Special byte for validating Will Properties.
)

var (
	// ErrNoValidPacketAvailable indicates the packet type byte provided does not exist in the mqtt specification.
	ErrNoValidPacketAvailable = errors.New("no valid packet available")

	// PacketNames is a map of packet bytes to human-readable names, for easier debugging.
	PacketNames = map[byte]string{
		0:  "Reserved",
		1:  "Connect",
		2:  "Connack",
		3:  "Publish",
		4:  "Puback",
		5:  "Pubrec",
		6:  "Pubrel",
		7:  "Pubcomp",
		8:  "Subscribe",
		9:  "Suback",
		10: "Unsubscribe",
		11: "Unsuback",
		12: "Pingreq",
		13: "Pingresp",
		14: "Disconnect",
		15: "Auth",
	}
)

// Packets is a concurrency safe map of packets.
type Packets struct {
	internal map[string]Packet
	sync.RWMutex
}

// NewPackets returns a new instance of Packets.
func NewPackets() *Packets {
	return &Packets{
		internal: map[string]Packet{},
	}
}

// Add adds a new packet to the map.
func (p *Packets) Add(id string, val Packet) {
	p.Lock()
	defer p.Unlock()
	p.internal[id] = val
}

// GetAll returns all packets in the map.
func (p *Packets) GetAll() map[string]Packet {
	p.RLock()
	defer p.RUnlock()
	m := map[string]Packet{}
	for k, v := range p.internal {
		m[k] = v
	}
	return m
}

// Each calls fn for every packet under the read lock, without copying the
// map. fn must be quick and must not call back into p.
func (p *Packets) Each(fn func(id string, pk Packet)) {
	p.RLock()
	defer p.RUnlock()
	for id, pk := range p.internal {
		fn(id, pk)
	}
}

// Get returns a specific packet in the map by packet id.
func (p *Packets) Get(id string) (val Packet, ok bool) {
	p.RLock()
	defer p.RUnlock()
	val, ok = p.internal[id]
	return val, ok
}

// Len returns the number of packets in the map.
func (p *Packets) Len() int {
	p.RLock()
	defer p.RUnlock()
	val := len(p.internal)
	return val
}

// Delete removes a packet from the map by packet id.
func (p *Packets) Delete(id string) {
	p.Lock()
	defer p.Unlock()
	delete(p.internal, id)
}

// Packet represents an MQTT packet. Instead of providing a packet interface
// variant packet structs, this is a single concrete packet type to cover all packet
// types, which allows us to take advantage of various compiler optimizations. It
// contains a combination of mqtt spec values and internal broker control codes.
//
// **No json tags on this type, on Properties, or on FixedHeader**, and the
// question was asked rather than inherited. mochi carried sixty of them and
// nothing marshalled any of these types there either - its storage hooks
// marshalled their own mirror types and its debug hook rendered a packet
// with fmt. Nothing marshals one here: every marshal site in saguin was
// checked, including the one that takes `any`, and all of them touch
// saguin's own types. This package is inside `internal/`, so nothing
// outside the module can marshal these even in principle.
//
// This is tidiness and not a defect removed, said plainly: a tag nothing
// reads is inert, unlike a counter nothing writes, which answers wrongly.
// What it buys is an engine that reads as code with one consumer instead
// of a library shaped for somebody else's serialisation.
type Packet struct {
	Connect      *ConnectParams // a CONNECT's own fields; nil on every other packet (saguin)
	Properties   Properties     // all mqtt v5 packet properties
	Payload      []byte         // a message/payload for publish packets
	ReasonCodes  []byte         // one or more reason codes for multi-reason responses (suback, etc)
	Filters      Subscriptions  // a list of subscription filters and their properties (subscribe, unsubscribe)
	TopicName    string         // the topic a payload is being published to
	Origin       string         // client id of the client who is issuing the packet (mostly internal use)
	Acknowledged bool           // taken out of flight by its client's answer on the connection that owned it (ownsInflight); never encoded (saguin)
	Bounded      bool           // counted against its session's bound by whoever wrote it, so not by the in-flight table; never encoded (saguin)
	LogOffset    uint64         // where saguin's broadcast log kept this publish, for its selection of recipients to count; never encoded (saguin)
	FirstSend    bool           // queued beside its in-flight entry, whose first send the write claims (mqtt.Inflight.ClaimQueued); never encoded (saguin)
	// Acknowledged, Bounded and LogOffset describe one delivery or one
	// stored publish, and **Copy does not carry them**: a copy is another
	// subscriber's delivery, and inheriting them would count it against the
	// wrong session or take it out of flight unanswered. Whoever needs one on
	// a copy sets it there.
	FixedHeader     FixedHeader // -
	Created         int64       // unix timestamp indicating time packet was created/received on the server
	Expiry          int64       // unix timestamp indicating when the packet will expire and should be deleted
	Mods            Mods        // internal broker control values for controlling certain mqtt v5 compliance
	PacketID        uint16      // packet id for the packet (publish, qos, etc)
	ProtocolVersion byte        // protocol version of the client the packet belongs to
	SessionPresent  bool        // session existed for connack
	ReasonCode      byte        // reason code for a packet response (acks, etc)
	ReservedBit     byte        // reserved, do not use (except in testing)
	Ignore          bool        // if true, do not perform any message forwarding operations
}

// Mods specifies certain values required for certain mqtt v5 compliance within packet encoding/decoding.
type Mods struct {
	MaxSize             uint32 // the maximum packet size specified by the client / server
	DisallowProblemInfo bool   // if problem info is disallowed
	AllowResponseInfo   bool   // if response info is disallowed
}

// ConnectParams contains packet values which are specifically related to connect packets.
//
// **Behind a pointer on Packet, and nil on every packet but a CONNECT**
// (saguin). It is 392 of a Packet's 816 bytes, meaningful on one packet a
// connection sends, and a Packet is copied by value on every step of every
// publish - into each hook, each subscriber's delivery, the in-flight
// table - so every one of those copies moved and zeroed it for nothing.
// A CONNECT's is made when it is decoded (ConnectDecode) or built
// (readConnectPrefix); anything that may be handed some other packet reads
// it nil-safely - a refused connection's first packet need not be a
// CONNECT (OnConnectRefused) - and Copy makes a copy its own. Nothing
// writes one after it is made, so the copies of one packet a hook is given
// sharing it is sharing something read-only.
type ConnectParams struct {
	WillProperties   Properties // -
	Password         []byte     // -
	Username         []byte     // -
	ProtocolName     []byte     // -
	WillPayload      []byte     // -
	ClientIdentifier string     // -
	WillTopic        string     // -
	Keepalive        uint16     // -
	PasswordFlag     bool       // -
	UsernameFlag     bool       // -
	WillQos          byte       // -
	WillFlag         bool       // -
	WillRetain       bool       // -
	Clean            bool       // CleanSession in v3.1.1, CleanStart in v5
}

// Subscriptions is a slice of Subscription.
type Subscriptions []Subscription // must be a slice to retain order.

// Subscription contains details about a client subscription to a topic filter.
type Subscription struct {
	ShareName         []string
	Filter            string
	Identifier        int
	Identifiers       map[string]int
	RetainHandling    byte
	Qos               byte
	RetainAsPublished bool
	NoLocal           bool
	FwdRetainedFlag   bool // true if the subscription forms part of a publish response to a client subscription and packet is retained.
}

// Copy creates a new instance of a packet, but with an empty header for inheriting new QoS flags, etc.
func (pk *Packet) Copy(allowTransfer bool) Packet {
	p := Packet{
		FixedHeader: FixedHeader{
			Remaining: pk.FixedHeader.Remaining,
			Type:      pk.FixedHeader.Type,
			Retain:    pk.FixedHeader.Retain,
			Dup:       false, // [MQTT-4.3.1-1] [MQTT-4.3.2-2]
			Qos:       pk.FixedHeader.Qos,
		},
		Mods: Mods{
			MaxSize: pk.Mods.MaxSize,
		},
		ReservedBit:     pk.ReservedBit,
		ProtocolVersion: pk.ProtocolVersion,
		TopicName:       pk.TopicName,
		Properties:      pk.Properties.Copy(allowTransfer),
		SessionPresent:  pk.SessionPresent,
		ReasonCode:      pk.ReasonCode,
		Filters:         pk.Filters,
		Created:         pk.Created,
		Expiry:          pk.Expiry,
		Origin:          pk.Origin,
	}

	if allowTransfer {
		p.PacketID = pk.PacketID
	}

	if c := pk.Connect; c != nil {
		p.Connect = &ConnectParams{
			ClientIdentifier: c.ClientIdentifier,
			Keepalive:        c.Keepalive,
			WillQos:          c.WillQos,
			WillTopic:        c.WillTopic,
			WillFlag:         c.WillFlag,
			WillRetain:       c.WillRetain,
			WillProperties:   c.WillProperties.Copy(allowTransfer),
			Clean:            c.Clean,
		}

		if len(c.ProtocolName) > 0 {
			p.Connect.ProtocolName = append([]byte{}, c.ProtocolName...)
		}

		if len(c.Password) > 0 {
			p.Connect.PasswordFlag = true
			p.Connect.Password = append([]byte{}, c.Password...)
		}

		if len(c.Username) > 0 {
			p.Connect.UsernameFlag = true
			p.Connect.Username = append([]byte{}, c.Username...)
		}

		if len(c.WillPayload) > 0 {
			p.Connect.WillPayload = append([]byte{}, c.WillPayload...)
		}
	}

	if len(pk.Payload) > 0 {
		p.Payload = append([]byte{}, pk.Payload...)
	}

	if len(pk.ReasonCodes) > 0 {
		p.ReasonCodes = append([]byte{}, pk.ReasonCodes...)
	}

	return p
}

// CopySharingPayload is Copy(false) for one subscriber's delivery of a
// publish, with the payload shared rather than duplicated.
//
// **A payload is immutable once it has been read**, and that is the whole of
// why sharing it is safe. Nothing writes into a payload's bytes -
// TestNothingWritesIntoAPayload walks the tree for it - and the buffer a
// payload is read into is never recycled, which is the contract at
// ReadPacket's allocation. Every other field is copied as Copy copies it,
// because a delivery does change those: its retain flag, packet identifier,
// subscription identifiers, topic alias and expiry are each subscriber's own.
//
// **Measured on 2026-09-23**: 100 publishers of 16KB into 15 wide broadcast
// subscribers, the broker pinned to four physical cores. Copy duplicated the
// payload once per subscriber - 9.2s of 111s of CPU, and most of the garbage
// that tripled the collector's work against append, which already shares one
// payload across every consumer. Copy itself is left as it is: its other
// callers run once per publish and hand the packet to stores.
func (pk *Packet) CopySharingPayload() Packet {
	shallow := *pk
	shallow.Payload = nil
	p := shallow.Copy(false)
	p.Payload = pk.Payload
	return p
}

// Merge merges a new subscription with a base subscription, preserving the highest
// qos value, matched identifiers and any special properties.
func (s Subscription) Merge(n Subscription) Subscription {
	if s.Identifiers == nil {
		s.Identifiers = map[string]int{
			s.Filter: s.Identifier,
		}
	}

	if n.Identifier > 0 {
		s.Identifiers[n.Filter] = n.Identifier
	}

	if n.Qos > s.Qos {
		s.Qos = n.Qos // [MQTT-3.3.4-2]
	}

	if n.NoLocal {
		s.NoLocal = true // [MQTT-3.8.3-3]
	}

	return s
}

// encode encodes a subscription and properties into bytes.
func (s Subscription) encode() byte {
	var flag byte
	flag |= s.Qos

	if s.NoLocal {
		flag |= 1 << 2
	}

	if s.RetainAsPublished {
		flag |= 1 << 3
	}

	flag |= s.RetainHandling << 4
	return flag
}

// decode decodes subscription bytes into a subscription struct.
func (s *Subscription) decode(b byte) {
	s.Qos = b & 3                      // byte
	s.NoLocal = 1&(b>>2) > 0           // bool
	s.RetainAsPublished = 1&(b>>3) > 0 // bool
	s.RetainHandling = 3 & (b >> 4)    // byte
}

// ConnectEncode encodes a connect packet.
func (pk *Packet) ConnectEncode(buf *bytes.Buffer) error {
	c := pk.Connect
	if c == nil {
		c = new(ConnectParams) // encoded as the zero CONNECT it always was
	}
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeBytes(c.ProtocolName))
	nb.WriteByte(pk.ProtocolVersion)

	nb.WriteByte(
		encodeBool(c.Clean)<<1 |
			encodeBool(c.WillFlag)<<2 |
			c.WillQos<<3 |
			encodeBool(c.WillRetain)<<5 |
			encodeBool(c.PasswordFlag)<<6 |
			encodeBool(c.UsernameFlag)<<7 |
			0, // [MQTT-2.1.3-1]
	)

	nb.Write(encodeUint16(c.Keepalive))

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		(&pk.Properties).Encode(pk.FixedHeader.Type, pk.Mods, pb, 0)
		nb.Write(pb.Bytes())
	}

	nb.Write(encodeString(c.ClientIdentifier))

	if c.WillFlag {
		if pk.ProtocolVersion == 5 {
			pb := mempool.GetBuffer()
			defer mempool.PutBuffer(pb)
			c.WillProperties.Encode(WillProperties, pk.Mods, pb, 0)
			nb.Write(pb.Bytes())
		}

		nb.Write(encodeString(c.WillTopic))
		nb.Write(encodeBytes(c.WillPayload))
	}

	if c.UsernameFlag {
		nb.Write(encodeBytes(c.Username))
	}

	if c.PasswordFlag {
		nb.Write(encodeBytes(c.Password))
	}

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// ConnectDecode decodes a connect packet.
func (pk *Packet) ConnectDecode(buf []byte) error {
	var offset int
	var err error
	pk.Connect = new(ConnectParams)

	pk.Connect.ProtocolName, offset, err = decodeBytes(buf, 0)
	if err != nil {
		return ErrMalformedProtocolName
	}

	pk.ProtocolVersion, offset, err = decodeByte(buf, offset)
	if err != nil {
		return ErrMalformedProtocolVersion
	}

	flags, offset, err := decodeByte(buf, offset)
	if err != nil {
		return ErrMalformedFlags
	}

	pk.ReservedBit = 1 & flags
	pk.Connect.Clean = 1&(flags>>1) > 0
	pk.Connect.WillFlag = 1&(flags>>2) > 0
	pk.Connect.WillQos = 3 & (flags >> 3) // this one is not a bool
	pk.Connect.WillRetain = 1&(flags>>5) > 0
	pk.Connect.PasswordFlag = 1&(flags>>6) > 0
	pk.Connect.UsernameFlag = 1&(flags>>7) > 0

	pk.Connect.Keepalive, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return ErrMalformedKeepalive
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}
		offset += n
	}

	pk.Connect.ClientIdentifier, offset, err = decodeString(buf, offset) // [MQTT-3.1.3-1] [MQTT-3.1.3-2] [MQTT-3.1.3-3] [MQTT-3.1.3-4]
	if err != nil {
		return ErrClientIdentifierNotValid // [MQTT-3.1.3-8]
	}

	if pk.Connect.WillFlag { // [MQTT-3.1.2-7]
		if pk.ProtocolVersion == 5 {
			n, err := pk.Connect.WillProperties.Decode(WillProperties, bytes.NewBuffer(buf[offset:]))
			if err != nil {
				return propertiesFailure(err, ErrMalformedWillProperties)
			}
			offset += n
		}

		pk.Connect.WillTopic, offset, err = decodeString(buf, offset)
		if err != nil {
			return ErrMalformedWillTopic
		}

		pk.Connect.WillPayload, offset, err = decodeBytes(buf, offset)
		if err != nil {
			return ErrMalformedWillPayload
		}
	}

	if pk.Connect.UsernameFlag { // [MQTT-3.1.3-12]
		if offset >= len(buf) { // we are at the end of the packet
			return ErrProtocolViolationFlagNoUsername // [MQTT-3.1.2-17]
		}

		// **A UTF-8 Encoded String, as the client id is** [MQTT-3.1.3-12]:
		// read as bytes, a user name holding U+0000 or invalid UTF-8 was
		// accepted where MQTT has the server treat it as a malformed packet
		// [MQTT-1.5.4-1] [MQTT-1.5.4-2]. The password is Binary Data and
		// stays bytes.
		var username string
		username, offset, err = decodeString(buf, offset)
		if err != nil {
			return ErrMalformedUsername
		}
		pk.Connect.Username = []byte(username)
	}

	if pk.Connect.PasswordFlag {
		pk.Connect.Password, offset, err = decodeBytes(buf, offset)
		if err != nil {
			return ErrMalformedPassword
		}
	}

	// **The payload holds what the flags describe and nothing else.**
	//
	// §3.1.3: "The Payload of the CONNECT packet contains one or more
	// length-prefixed fields... The presence of these fields is determined
	// by flags in the Variable Header", and the order is fixed. Bytes left
	// over are fields the client sent without declaring them, which
	// [MQTT-3.1.2-16] and [MQTT-3.1.2-18] make a protocol error for a user
	// name and a password respectively.
	//
	// **ConnectValidate carries both of those rules and could not reach
	// them.** It asks whether a field is set while its flag is 0, and the
	// loop above only reads a field when its flag is 1, so the field it
	// asks about is always empty and the check always passed. The rule had
	// to move to where the bytes are, which is here.
	//
	// Stated once, over the whole payload, rather than once per field: a
	// surplus user name and a surplus password are the same defect, and so
	// is any third thing a later version of the protocol puts after them.
	if offset < len(buf) {
		return ErrProtocolViolationSurplusPayload // [MQTT-3.1.2-16] [MQTT-3.1.2-18]
	}

	return nil
}

// ConnectValidate ensures the connect packet is compliant.
func (pk *Packet) ConnectValidate() Code {
	c := pk.Connect
	if c == nil {
		c = new(ConnectParams) // a CONNECT with no fields, refused for its protocol name
	}
	if !bytes.Equal(c.ProtocolName, []byte{'M', 'Q', 'I', 's', 'd', 'p'}) && !bytes.Equal(c.ProtocolName, []byte{'M', 'Q', 'T', 'T'}) {
		return ErrProtocolViolationProtocolName // [MQTT-3.1.2-1]
	}

	if (bytes.Equal(c.ProtocolName, []byte{'M', 'Q', 'I', 's', 'd', 'p'}) && pk.ProtocolVersion != 3) ||
		(bytes.Equal(c.ProtocolName, []byte{'M', 'Q', 'T', 'T'}) && pk.ProtocolVersion != 4 && pk.ProtocolVersion != 5) {
		return ErrProtocolViolationProtocolVersion // [MQTT-3.1.2-2]
	}

	if pk.ReservedBit != 0 {
		return ErrMalformedReservedBit // [MQTT-3.1.2-3]
	}

	if len(c.Password) > math.MaxUint16 {
		return ErrProtocolViolationPasswordTooLong
	}

	if len(c.Username) > math.MaxUint16 {
		return ErrProtocolViolationUsernameTooLong
	}

	if !c.UsernameFlag && len(c.Username) > 0 {
		return ErrProtocolViolationUsernameNoFlag // [MQTT-3.1.2-16]
	}

	if c.PasswordFlag && len(c.Password) == 0 {
		return ErrProtocolViolationFlagNoPassword // [MQTT-3.1.2-19]
	}

	if !c.PasswordFlag && len(c.Password) > 0 {
		return ErrProtocolViolationPasswordNoFlag // [MQTT-3.1.2-18]
	}

	if len(c.ClientIdentifier) > math.MaxUint16 {
		return ErrClientIdentifierNotValid
	}

	// **A Will QoS of 3 is a Malformed Packet whatever the Will Flag says**
	// (MQTT 5 section 3.1.2.6; [MQTT-3.1.2-14] in 3.1.1), and with the flag
	// clear there is no Will for any QoS to belong to: "the Will QoS MUST be
	// set to 0" ([MQTT-3.1.2-11] in MQTT 5, [MQTT-3.1.2-13] in 3.1.1). The
	// two are neighbouring bits of one byte, so a client that sets the QoS
	// and clears the flag has contradicted itself.
	if c.WillQos > 2 {
		return ErrMalformedQos // [MQTT-3.1.2-14]
	}
	if !c.WillFlag && c.WillQos != 0 {
		return ErrMalformedFlags // [MQTT-3.1.2-11] [MQTT-3.1.2-13]
	}

	// **MQTT 3.1.1 allows a password only with a user name**: "If the User
	// Name Flag is set to 0, the Password Flag MUST be set to 0"
	// ([MQTT-3.1.2-22]). MQTT 5 dropped the rule (section 3.1.2.9) and
	// permits a password alone, so the protocol version decides.
	if pk.ProtocolVersion < 5 && c.PasswordFlag && !c.UsernameFlag {
		return ErrProtocolViolationFlagNoUsername // [MQTT-3.1.2-22]
	}

	if c.WillFlag {
		// **The topic, and not the payload.** [MQTT-3.1.2-9] asks for the
		// Will Topic and Will Payload fields to be present, and a field of
		// Binary Data is present at zero bytes: MQTT 5 section 3.1.3.4 and
		// 3.1.1 section 3.1.3.3 ("zero or more bytes") both allow an empty
		// Will Payload, which is how a device has its `latest` value deleted
		// when it dies (RFC 0003). A payload missing altogether never gets
		// here: the decoder refuses the packet as malformed. This refused
		// the empty one, from upstream mochi's v5 rewrite.
		if c.WillTopic == "" {
			return ErrProtocolViolationWillFlagNoPayload // [MQTT-3.1.2-9]
		}

		// The same rule as a PUBLISH's, because a Will becomes one. The
		// Will Properties carry their own Response Topic, so a wildcard
		// refused on the publish path would otherwise arrive through
		// CONNECT and be dropped at the moment the Will is sent, when
		// there is no longer a client to tell. Refusing the CONNECT is
		// the only point at which anybody can be told.
		if strings.ContainsAny(c.WillProperties.ResponseTopic, "+#") {
			return ErrProtocolViolationWildcardResponseTopic // [MQTT-3.3.2-14]
		}
	}

	if !c.WillFlag && c.WillRetain {
		return ErrProtocolViolationWillFlagSurplusRetain // [MQTT-3.1.2-13]
	}

	// A Receive Maximum or Maximum Packet Size of 0, or either twice
	// (3.1.2.11.3, 3.1.2.11.4), is refused as the properties are read, by the
	// table in properties.go: it is the one home of those rules.

	return CodeSuccess
}

// ConnackEncode encodes a Connack packet.
func (pk *Packet) ConnackEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.WriteByte(encodeBool(pk.SessionPresent))
	nb.WriteByte(pk.ReasonCode)

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		// nb already holds the session-present flag and the reason code.
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len())
		nb.Write(pb.Bytes())
	}

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// endOfPacket is a packet whose fields have all been read: the Remaining
// Length is "the number of bytes remaining within the current packet,
// including data in the variable header and the payload" (MQTT 5 section
// 2.1.4, 3.1.1 section 2.2.3), so bytes beyond the last field are bytes the
// packet does not describe, and a packet that cannot be parsed to its end is
// a Malformed Packet. Written once for every packet whose layout is fixed:
// one that ends in a list or a payload (PUBLISH, SUBSCRIBE, UNSUBSCRIBE, SUBACK)
// is read to its end by construction.
func endOfPacket(buf []byte, offset int) error {
	if offset != len(buf) {
		return fmt.Errorf("%d bytes after the last field: %w", len(buf)-offset, ErrMalformedPacket)
	}
	return nil
}

// ConnackDecode decodes a Connack packet.
func (pk *Packet) ConnackDecode(buf []byte) error {
	var offset int
	var err error

	pk.SessionPresent, offset, err = decodeByteBool(buf, 0)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedSessionPresent)
	}

	// "Bits 7-1 are reserved and MUST be set to 0" (MQTT 5 [MQTT-3.2.2-1],
	// 3.1.1 section 3.2.2.1).
	if buf[0]&0xFE != 0 {
		return ErrMalformedSessionPresent
	}

	pk.ReasonCode, offset, err = decodeByte(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedReasonCode)
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}
		offset += n
	}

	return endOfPacket(buf, offset)
}

// DisconnectEncode encodes a Disconnect packet.
func (pk *Packet) DisconnectEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)

	if pk.ProtocolVersion == 5 {
		nb.WriteByte(pk.ReasonCode)

		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len())
		nb.Write(pb.Bytes())
	}

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// DisconnectDecode decodes a Disconnect packet.
//
// **A Remaining Length of 1 is a Reason Code and no Property Length**
// (MQTT 5 section 3.14.2.1: only a Remaining Length of 0 means 0x00 with no
// Properties), so a client that ends with 0x04, Disconnect with Will
// Message, in one byte is asking for its Will, and reading it as 0x00 would
// have the Will discarded.
func (pk *Packet) DisconnectDecode(buf []byte) error {
	var offset int
	if pk.ProtocolVersion == 5 && pk.FixedHeader.Remaining > 0 {
		var err error
		pk.ReasonCode, offset, err = decodeByte(buf, offset)
		if err != nil {
			return fmt.Errorf("%s: %w", err, ErrMalformedReasonCode)
		}

		if pk.FixedHeader.Remaining > 1 {
			n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
			if err != nil {
				return propertiesFailure(err, ErrMalformedProperties)
			}
			offset += n
		}
	}

	return endOfPacket(buf, offset)
}

// PingreqEncode encodes a Pingreq packet.
func (pk *Packet) PingreqEncode(buf *bytes.Buffer) error {
	pk.FixedHeader.Encode(buf)
	return nil
}

// PingreqDecode decodes a Pingreq packet.
func (pk *Packet) PingreqDecode(buf []byte) error {
	return endOfPacket(buf, 0) // no variable header and no payload (3.12.2, 3.12.3)
}

// PingrespEncode encodes a Pingresp packet.
func (pk *Packet) PingrespEncode(buf *bytes.Buffer) error {
	pk.FixedHeader.Encode(buf)
	return nil
}

// PingrespDecode decodes a Pingres packet.
func (pk *Packet) PingrespDecode(buf []byte) error {
	return endOfPacket(buf, 0) // no variable header and no payload (3.13.2, 3.13.3)
}

// PublishEncode encodes a Publish packet.
func (pk *Packet) PublishEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)

	nb.Write(encodeString(pk.TopicName)) // [MQTT-3.3.2-1]

	if pk.FixedHeader.Qos > 0 {
		if pk.PacketID == 0 {
			return ErrProtocolViolationNoPacketID // [MQTT-2.2.1-3] [MQTT-2.2.1-4]
		}
		nb.Write(encodeUint16(pk.PacketID))
	}

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+len(pk.Payload))
		nb.Write(pb.Bytes())
	}

	pk.FixedHeader.Remaining = nb.Len() + len(pk.Payload)
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())
	buf.Write(pk.Payload)

	return nil
}

// PublishDecode extracts the data values from the packet.
func (pk *Packet) PublishDecode(buf []byte) error {
	var offset int
	var err error

	pk.TopicName, offset, err = decodeString(buf, 0) // [MQTT-3.3.2-1]
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedTopic)
	}

	if pk.FixedHeader.Qos > 0 {
		pk.PacketID, offset, err = decodeUint16(buf, offset)
		if err != nil {
			return fmt.Errorf("%s: %w", err, ErrMalformedPacketID)
		}
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}

		offset += n
	}

	pk.Payload = buf[offset:]

	return nil
}

// PublishValidate validates a publish packet.
func (pk *Packet) PublishValidate(topicAliasMaximum uint16) Code {
	if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
		return ErrProtocolViolationNoPacketID // [MQTT-2.2.1-3] [MQTT-2.2.1-4]
	}

	if pk.FixedHeader.Qos == 0 && pk.PacketID > 0 {
		return ErrProtocolViolationSurplusPacketID // [MQTT-2.2.1-2]
	}

	if strings.ContainsAny(pk.TopicName, "+#") {
		return ErrProtocolViolationSurplusWildcard // [MQTT-3.3.2-2]
	}

	// **A Response Topic naming a wildcard is refused rather than quietly
	// dropped**, which is what happened before this line existed.
	//
	// [MQTT-3.3.2-14] forbids the wildcard, and [MQTT-3.3.2-15] requires
	// the server to send the Response Topic *unaltered* to every
	// subscriber. Only the encoder knew about the rule, and its answer was
	// to leave the property out: the publisher was told 0x00, the message
	// went out without its reply route, and a request/response client had
	// no way to learn that any of that had happened. Breaking the second
	// rule is not a way to enforce the first.
	//
	// Found by FuzzDecodeSurvivesAReencode, not by any conformance suite.
	// mosquitto 2.0.22 answers the same packet with DISCONNECT 0x82.
	if strings.ContainsAny(pk.Properties.ResponseTopic, "+#") {
		return ErrProtocolViolationWildcardResponseTopic // [MQTT-3.3.2-14]
	}

	if pk.Properties.TopicAlias > topicAliasMaximum {
		return ErrTopicAliasInvalid // [MQTT-3.2.2-17] [MQTT-3.3.2-9] ~[MQTT-3.3.2-10] [MQTT-3.3.2-12]
	}

	if pk.TopicName == "" && pk.Properties.TopicAlias == 0 {
		return ErrProtocolViolationNoTopic // ~[MQTT-3.3.2-8]
	}

	// A Topic Alias of 0 [MQTT-3.3.2-8] is refused as the properties are
	// read, by the table in properties.go.

	if len(pk.Properties.SubscriptionIdentifier) > 0 {
		return ErrProtocolViolationSurplusSubID // [MQTT-3.3.4-6]
	}

	return CodeSuccess
}

// encodePubAckRelRecComp encodes a Puback, Pubrel, Pubrec, or Pubcomp packet.
func (pk *Packet) encodePubAckRelRecComp(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeUint16(pk.PacketID))

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		// +1 for the reason code, which is written whenever there are
		// properties - the only case in which their size is weighed.
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+1)
		if pk.ReasonCode >= ErrUnspecifiedError.Code || pb.Len() > 1 {
			nb.WriteByte(pk.ReasonCode)
		}

		if pb.Len() > 1 {
			nb.Write(pb.Bytes())
		}
	}

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())
	return nil
}

// decodePubAckRelRecComp extracts the data values from a Puback, Pubrel, Pubrec, or Pubcomp packet.
func (pk *Packet) decodePubAckRelRecComp(buf []byte) error {
	var offset int
	var err error
	pk.PacketID, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedPacketID)
	}

	if pk.ProtocolVersion == 5 && pk.FixedHeader.Remaining > 2 {
		pk.ReasonCode, offset, err = decodeByte(buf, offset)
		if err != nil {
			return fmt.Errorf("%s: %w", err, ErrMalformedReasonCode)
		}

		if pk.FixedHeader.Remaining > 3 {
			var n int
			n, err = pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
			if err != nil {
				return propertiesFailure(err, ErrMalformedProperties)
			}
			offset += n
		}
	}

	return endOfPacket(buf, offset)
}

// PubackEncode encodes a Puback packet.
func (pk *Packet) PubackEncode(buf *bytes.Buffer) error {
	return pk.encodePubAckRelRecComp(buf)
}

// PubackDecode decodes a Puback packet.
func (pk *Packet) PubackDecode(buf []byte) error {
	return pk.decodePubAckRelRecComp(buf)
}

// PubcompEncode encodes a Pubcomp packet.
func (pk *Packet) PubcompEncode(buf *bytes.Buffer) error {
	return pk.encodePubAckRelRecComp(buf)
}

// PubcompDecode decodes a Pubcomp packet.
func (pk *Packet) PubcompDecode(buf []byte) error {
	return pk.decodePubAckRelRecComp(buf)
}

// PubrecEncode encodes a Pubrec packet.
func (pk *Packet) PubrecEncode(buf *bytes.Buffer) error {
	return pk.encodePubAckRelRecComp(buf)
}

// PubrecDecode decodes a Pubrec packet.
func (pk *Packet) PubrecDecode(buf []byte) error {
	return pk.decodePubAckRelRecComp(buf)
}

// PubrelEncode encodes a Pubrel packet.
func (pk *Packet) PubrelEncode(buf *bytes.Buffer) error {
	return pk.encodePubAckRelRecComp(buf)
}

// PubrelDecode decodes a Pubrel packet.
func (pk *Packet) PubrelDecode(buf []byte) error {
	return pk.decodePubAckRelRecComp(buf)
}

// ReasonCodeValid returns true if the provided reason code is valid for the packet type.
func (pk *Packet) ReasonCodeValid() bool {
	switch pk.FixedHeader.Type {
	case Pubrec:
		return bytes.Contains([]byte{
			CodeSuccess.Code,
			CodeNoMatchingSubscribers.Code,
			ErrUnspecifiedError.Code,
			ErrImplementationSpecificError.Code,
			ErrNotAuthorized.Code,
			ErrTopicNameInvalid.Code,
			ErrPacketIdentifierInUse.Code,
			ErrQuotaExceeded.Code,
			ErrPayloadFormatInvalid.Code,
		}, []byte{pk.ReasonCode})
	case Pubrel:
		fallthrough
	case Pubcomp:
		return bytes.Contains([]byte{
			CodeSuccess.Code,
			ErrPacketIdentifierNotFound.Code,
		}, []byte{pk.ReasonCode})
	case Suback:
		return bytes.Contains([]byte{
			CodeGrantedQos0.Code,
			CodeGrantedQos1.Code,
			CodeGrantedQos2.Code,
			ErrUnspecifiedError.Code,
			ErrImplementationSpecificError.Code,
			ErrNotAuthorized.Code,
			ErrTopicFilterInvalid.Code,
			ErrPacketIdentifierInUse.Code,
			ErrQuotaExceeded.Code,
			ErrSharedSubscriptionsNotSupported.Code,
			ErrSubscriptionIdentifiersNotSupported.Code,
			ErrWildcardSubscriptionsNotSupported.Code,
		}, []byte{pk.ReasonCode})
	case Unsuback:
		return bytes.Contains([]byte{
			CodeSuccess.Code,
			CodeNoSubscriptionExisted.Code,
			ErrUnspecifiedError.Code,
			ErrImplementationSpecificError.Code,
			ErrNotAuthorized.Code,
			ErrTopicFilterInvalid.Code,
			ErrPacketIdentifierInUse.Code,
		}, []byte{pk.ReasonCode})
	}

	return true
}

// SubackEncode encodes a Suback packet.
func (pk *Packet) SubackEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeUint16(pk.PacketID))

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+len(pk.ReasonCodes))
		nb.Write(pb.Bytes())
	}

	nb.Write(pk.ReasonCodes)

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// SubackDecode decodes a Suback packet.
func (pk *Packet) SubackDecode(buf []byte) error {
	var offset int
	var err error

	pk.PacketID, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedPacketID)
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}
		offset += n
	}

	pk.ReasonCodes = buf[offset:]

	return nil
}

// SubscribeEncode encodes a Subscribe packet.
func (pk *Packet) SubscribeEncode(buf *bytes.Buffer) error {
	if pk.PacketID == 0 {
		return ErrProtocolViolationNoPacketID
	}

	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeUint16(pk.PacketID))

	xb := mempool.GetBuffer() // capture and write filters after length checks
	defer mempool.PutBuffer(xb)
	for _, opts := range pk.Filters {
		xb.Write(encodeString(opts.Filter)) // [MQTT-3.8.3-1]
		if pk.ProtocolVersion == 5 {
			xb.WriteByte(opts.encode())
		} else {
			xb.WriteByte(opts.Qos)
		}
	}

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+xb.Len())
		nb.Write(pb.Bytes())
	}

	nb.Write(xb.Bytes())

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// SubscribeDecode decodes a Subscribe packet.
func (pk *Packet) SubscribeDecode(buf []byte) error {
	var offset int
	var err error

	pk.PacketID, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return ErrMalformedPacketID
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}
		offset += n
	}

	var filter string
	pk.Filters = Subscriptions{}
	for offset < len(buf) {
		filter, offset, err = decodeString(buf, offset) // [MQTT-3.8.3-1]
		if err != nil {
			return ErrMalformedTopic
		}

		var option byte
		sub := &Subscription{
			Filter: filter,
		}

		option, offset, err = decodeByte(buf, offset)
		if err != nil {
			return ErrMalformedQos
		}
		if pk.ProtocolVersion == 5 {
			// **Bits 6 and 7 of the Subscription Options are reserved and
			// the server must refuse a packet that sets them**, which is
			// [MQTT-3.8.3-5]: "the Server MUST treat a SUBSCRIBE packet as
			// malformed if any of Reserved bits in the Payload are non-zero".
			//
			// Subscription.decode reads bits 0 to 5 and silently discards
			// the rest, so a client setting them was answered SUBACK 0x00
			// and subscribed as though it had not. The bits carry no
			// meaning today, and that is the point: a client setting them
			// is speaking a protocol this broker does not know, and
			// guessing what it meant is worse than saying so.
			if option&0xC0 != 0 {
				return ErrMalformedSurplusSubscriptionBits // [MQTT-3.8.3-5]
			}
			sub.decode(option)

			// §3.8.3.1: "It is a Protocol Error to send a Retain Handling
			// value of 3." No identifier attaches to that sentence, so the
			// section is what is cited. It is a Protocol Error and not a
			// Malformed Packet because a two-bit field holding 3 parses:
			// section 1.2 keeps "malformed" for a packet that cannot be
			// parsed, and section 4.13.1 answers the two 0x81 and 0x82.
			if sub.RetainHandling > 2 {
				return ErrProtocolViolationInvalidRetainHandling
			}
		} else {
			sub.Qos = option
		}

		if len(pk.Properties.SubscriptionIdentifier) > 0 {
			sub.Identifier = pk.Properties.SubscriptionIdentifier[0]
		}

		if sub.Qos > 2 {
			return ErrProtocolViolationQosOutOfRange
		}

		pk.Filters = append(pk.Filters, *sub)
	}

	return nil
}

// SubscribeValidate ensures the packet is compliant.
func (pk *Packet) SubscribeValidate() Code {
	if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
		return ErrProtocolViolationNoPacketID // [MQTT-2.2.1-3] [MQTT-2.2.1-4]
	}

	if len(pk.Filters) == 0 {
		return ErrProtocolViolationNoFilters // [MQTT-3.8.3-2]
	}

	// A Subscription Identifier outside 1 to 268,435,455 (3.8.2.1.2), or
	// sent more than once, is refused as the properties are read, by the
	// table in properties.go.

	return CodeSuccess
}

// UnsubackEncode encodes an Unsuback packet.
func (pk *Packet) UnsubackEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeUint16(pk.PacketID))

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+len(pk.ReasonCodes))
		nb.Write(pb.Bytes())
		nb.Write(pk.ReasonCodes)
	}

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// UnsubackDecode decodes an Unsuback packet.
func (pk *Packet) UnsubackDecode(buf []byte) error {
	var offset int
	var err error

	pk.PacketID, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedPacketID)
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}

		offset += n

		pk.ReasonCodes = buf[offset:]
		return nil
	}

	return endOfPacket(buf, offset) // MQTT 3.1.1: the packet identifier and nothing else
}

// UnsubscribeEncode encodes an Unsubscribe packet.
func (pk *Packet) UnsubscribeEncode(buf *bytes.Buffer) error {
	if pk.PacketID == 0 {
		return ErrProtocolViolationNoPacketID
	}

	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.Write(encodeUint16(pk.PacketID))

	xb := mempool.GetBuffer() // capture filters and write after length checks
	defer mempool.PutBuffer(xb)
	for _, sub := range pk.Filters {
		xb.Write(encodeString(sub.Filter)) // [MQTT-3.10.3-1]
	}

	if pk.ProtocolVersion == 5 {
		pb := mempool.GetBuffer()
		defer mempool.PutBuffer(pb)
		pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len()+xb.Len())
		nb.Write(pb.Bytes())
	}

	nb.Write(xb.Bytes())

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())

	return nil
}

// UnsubscribeDecode decodes an Unsubscribe packet.
func (pk *Packet) UnsubscribeDecode(buf []byte) error {
	var offset int
	var err error

	pk.PacketID, offset, err = decodeUint16(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedPacketID)
	}

	if pk.ProtocolVersion == 5 {
		n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
		if err != nil {
			return propertiesFailure(err, ErrMalformedProperties)
		}
		offset += n
	}

	var filter string
	pk.Filters = Subscriptions{}
	for offset < len(buf) {
		filter, offset, err = decodeString(buf, offset) // [MQTT-3.10.3-1]
		if err != nil {
			return fmt.Errorf("%s: %w", err, ErrMalformedTopic)
		}
		pk.Filters = append(pk.Filters, Subscription{Filter: filter})
	}

	return nil
}

// UnsubscribeValidate validates an Unsubscribe packet.
func (pk *Packet) UnsubscribeValidate() Code {
	if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
		return ErrProtocolViolationNoPacketID // [MQTT-2.2.1-3] [MQTT-2.2.1-4]
	}

	if len(pk.Filters) == 0 {
		return ErrProtocolViolationNoFilters // [MQTT-3.10.3-2]
	}

	return CodeSuccess
}

// AuthEncode encodes an Auth packet.
func (pk *Packet) AuthEncode(buf *bytes.Buffer) error {
	nb := mempool.GetBuffer()
	defer mempool.PutBuffer(nb)
	nb.WriteByte(pk.ReasonCode)

	pb := mempool.GetBuffer()
	defer mempool.PutBuffer(pb)
	pk.Properties.Encode(pk.FixedHeader.Type, pk.Mods, pb, nb.Len())
	nb.Write(pb.Bytes())

	pk.FixedHeader.Remaining = nb.Len()
	pk.FixedHeader.Encode(buf)
	buf.Write(nb.Bytes())
	return nil
}

// AuthDecode decodes an Auth packet.
func (pk *Packet) AuthDecode(buf []byte) error {
	var offset int
	var err error

	// Packet type 15 is "Reserved - Forbidden" in MQTT 3.1.1 (section 2.2.1).
	if pk.ProtocolVersion != 5 {
		return ErrProtocolViolation
	}

	// "The Reason Code and Property Length can be omitted if the Reason Code
	// is 0x00 (Success) and there are no Properties. In this case the AUTH has
	// a Remaining Length of 0" (3.15.2.1): no property is read, so none is
	// required either.
	if pk.FixedHeader.Remaining == 0 {
		pk.ReasonCode = CodeSuccess.Code
		return endOfPacket(buf, 0)
	}

	pk.ReasonCode, offset, err = decodeByte(buf, offset)
	if err != nil {
		return fmt.Errorf("%s: %w", err, ErrMalformedReasonCode)
	}

	n, err := pk.Properties.Decode(pk.FixedHeader.Type, bytes.NewBuffer(buf[offset:]))
	if err != nil {
		return propertiesFailure(err, ErrMalformedProperties)
	}

	return endOfPacket(buf, offset+n)
}

// AuthValidate returns success if the auth packet is valid.
func (pk *Packet) AuthValidate() Code {
	if pk.ReasonCode != CodeSuccess.Code &&
		pk.ReasonCode != CodeContinueAuthentication.Code &&
		pk.ReasonCode != CodeReAuthenticate.Code {
		return ErrProtocolViolationInvalidReason // [MQTT-3.15.2-1]
	}

	return CodeSuccess
}

// FormatID returns the PacketID field as a decimal integer.
func (pk *Packet) FormatID() string {
	return strconv.FormatUint(uint64(pk.PacketID), 10)
}
