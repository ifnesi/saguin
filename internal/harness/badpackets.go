package harness

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
)

// BadPacket is one packet that MQTT, or the design where it chooses, makes
// a receiver refuse. BadPackets drives two tests: the conformance test
// sends each row the broker reads and checks the broker's answer, and the
// harness's own test checks that the harness, decoding the row, refuses it
// as well, so a fault the broker sends cannot pass the harness unseen.
type BadPacket struct {
	Name    string // what is wrong, and the rule that says so
	Version byte   // the connection's version, V311 or V5
	Stage   Stage
	Packet  []byte
	// Err is the refusal's kind: ErrMalformed, ErrProtocol, ErrTopicAlias or
	// ErrTooLarge, MQTT 5's reasons 0x81, 0x82, 0x94 and 0x95.
	Err error
	// Reads marks a packet the harness decodes without error, as it must:
	// what makes it wrong is the connection's state (a second CONNECT, a
	// raised expiry, which end sent it), a choice of the design's beyond
	// the standard (design.md, Choices MQTT leaves open), or a Topic
	// Filter's own syntax (MQTT 5 4.7, MQTT 3.1.1 4.7), which a client
	// never reads and the harness does not check.
	Reads bool
}

// Stage is when a BadPacket is sent, which decides the broker's answer.
type Stage uint8

const (
	// First is the connection's first packet: the broker closes the
	// connection with nothing sent (design.md, CONNECT: "Any other
	// Protocol Name, a malformed CONNECT [...], or one that breaks the
	// protocol closes the connection without a CONNACK (MQTT-3.1.2-1)").
	First Stage = iota + 1
	// AfterConnack is a packet after a successful CONNACK: the broker sends
	// MQTT 5 a DISCONNECT with Reason, and closes an MQTT 3.1.1 connection
	// bare (design.md, DISCONNECT, "Reasons Sagüin sends").
	AfterConnack
	// HarnessOnly is a packet the broker does not read yet: the harness's
	// own test alone sends it.
	HarnessOnly
)

// Reason is the reason code of the MQTT 5 DISCONNECT that refuses b.
func (b *BadPacket) Reason() byte {
	switch {
	case errors.Is(b.Err, ErrTopicAlias):
		return 0x94
	case errors.Is(b.Err, ErrMalformed):
		return 0x81
	case errors.Is(b.Err, ErrProtocol):
		return 0x82
	}
	return 0x95
}

// The receiver's limits the size rows assume: broker.limits.max_packet_size
// for a packet after the CONNACK and broker.limits.max_connect_size for the
// first; a harness reading a row takes the same limit. max_packet_size
// holds a payload over 4 KiB, which the broker compresses (design.md,
// Store, Compression), so the end-to-end fuzzer, whose broker the rows
// assume, can send one.
const (
	BadMaxPacketSize  = 6 << 10
	BadMaxConnectSize = 96
	// BadMaxTopicLevels is broker.limits.max_topic_levels, which a
	// malformed filter of more levels passes, so its refusal shows that
	// a malformed filter is refused before any limit is counted.
	BadMaxTopicLevels = 4
)

// Limit is the receiver's size limit for b's stage.
func (b *BadPacket) Limit() int {
	if b.Stage == First {
		return BadMaxConnectSize
	}
	return BadMaxPacketSize
}

// pkt is a packet: its first byte, the Remaining Length of its body in the
// shortest form, and the body.
func pkt(first byte, body ...[]byte) []byte {
	b := bytes.Join(body, nil)
	return append(AppendVarInt([]byte{first}, uint32(len(b))), b...)
}

// longForm writes p's one-byte Remaining Length in two bytes, a longer form
// than it needs.
func longForm(p []byte) []byte {
	return append([]byte{p[0], p[1] | 0x80, 0x00}, p[2:]...)
}

// str is a UTF-8 Encoded String's length and bytes, as given.
func str(s string) []byte { return AppendString(nil, s) }

// props is a property section: its Property Length, then the properties.
func props(ps ...[]byte) []byte {
	b := bytes.Join(ps, nil)
	return append(AppendVarInt(nil, uint32(len(b))), b...)
}

// prop is one property: its identifier and its value's bytes.
func prop(id byte, value ...byte) []byte { return append([]byte{id}, value...) }

// connect is a CONNECT with the Protocol Name "MQTT", the level, the
// Connect Flags, a Keep Alive of 0, then rest: in MQTT 5, the property
// section first.
func connect(level, flags byte, rest ...[]byte) []byte {
	return pkt(0x10, str("MQTT"), []byte{level, flags, 0, 0}, bytes.Join(rest, nil))
}

// Connect Flags (5.0 §3.1.2.3, 3.1.1 §3.1.2.3).
const (
	cleanFlag    = 0x02
	willFlag     = 0x04
	willQoS1     = 0x08
	willQoS3     = 0x18
	willRetain   = 0x20
	passwordFlag = 0x40
	userNameFlag = 0x80
)

// BadPackets is the table: every rule the broker's readers enforce today
// on CONNECT, PINGREQ, DISCONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, the four
// acknowledgements of a publish and the packet types they refuse unread,
// and the harness's own decoding rules for the packets the broker does not
// read yet.
var BadPackets = slices.Concat(requestRows, badAcks(PUBACK, 0x80), badAcks(PUBREC, 0x80), badAcks(PUBREL, 0x92),
	badAcks(PUBCOMP, 0x92), harnessRows)

// requestRows are BadPackets' rows of the packets but the acknowledgements
// a client sends.
var requestRows = []BadPacket{
	// The first packet: the CONNECT's Protocol Name and flags.
	{Name: "a Protocol Name other than MQTT (5.0 [MQTT-3.1.2-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: pkt(0x10, str("MQTX"), []byte{V5, cleanFlag, 0, 0}, props(), str("c"))},
	{Name: "a Protocol Name other than MQTT (3.1.1 §3.1.2.1, [MQTT-3.1.2-1])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: pkt(0x10, str("MQTX"), []byte{V311, cleanFlag, 0, 0}, str("c"))},
	{Name: "the CONNECT's reserved flag set (5.0 [MQTT-3.1.2-3])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|1, props(), str("c"))},
	{Name: "the CONNECT's reserved flag set (3.1.1 [MQTT-3.1.2-3])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag|1, str("c"))},
	{Name: "a Will QoS of 3 (5.0 §3.1.2.6: \"a Malformed Packet\")", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag|willQoS3, props(), str("c"), props(), str("w"), str("p"))},
	{Name: "a Will QoS of 3 (3.1.1 [MQTT-3.1.2-14])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag|willFlag|willQoS3, str("c"), str("w"), str("p"))},
	{Name: "a Will QoS without the Will Flag (5.0 [MQTT-3.1.2-11])", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag|willQoS1, props(), str("c"))},
	{Name: "a Will Retain without the Will Flag (5.0 [MQTT-3.1.2-13])", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag|willRetain, props(), str("c"))},
	{Name: "a Will QoS without the Will Flag (3.1.1 [MQTT-3.1.2-13])", Version: V311, Stage: First, Err: ErrProtocol,
		Packet: connect(V311, cleanFlag|willQoS1, str("c"))},
	{Name: "a Will Retain without the Will Flag (3.1.1 [MQTT-3.1.2-15])", Version: V311, Stage: First, Err: ErrProtocol,
		Packet: connect(V311, cleanFlag|willRetain, str("c"))},
	{Name: "a Password Flag without the User Name Flag (3.1.1 [MQTT-3.1.2-22])", Version: V311, Stage: First, Err: ErrProtocol,
		Packet: connect(V311, cleanFlag|passwordFlag, str("c"), str("p"))},
	{Name: "the CONNECT's fixed header flags 0001 (5.0 §2.1.3, Table 2-2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: pkt(0x11, str("MQTT"), []byte{V5, cleanFlag, 0, 0}, props(), str("c"))},

	// The first packet: a CONNECT cut short by its Remaining Length, or
	// longer than its fields.
	{Name: "a CONNECT cut short before its Keep Alive (5.0 §3.1.2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: pkt(0x10, str("MQTT"), []byte{V5, cleanFlag, 0})},
	{Name: "a Client Identifier longer than the packet (5.0 §1.5.4)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(), []byte{0, 5, 'c'})},
	{Name: "a Client Identifier longer than the packet (3.1.1 §1.5.3)", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag, []byte{0, 5, 'c'})},
	{Name: "a Will Flag with no Will in the payload (5.0 [MQTT-3.1.2-9])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"))},
	{Name: "a Will Flag with no Will in the payload (3.1.1 [MQTT-3.1.2-9])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag|willFlag, str("c"))},
	{Name: "a User Name Flag with no User Name (5.0 [MQTT-3.1.2-17])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|userNameFlag, props(), str("c"))},
	{Name: "a Password Flag with no Password (5.0 [MQTT-3.1.2-19])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|passwordFlag, props(), str("c"))},
	{Name: "a User Name with no User Name Flag (5.0 [MQTT-3.1.2-16])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(), str("c"), str("u"))},
	{Name: "a User Name with no User Name Flag (3.1.1 [MQTT-3.1.2-18])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag, str("c"), str("u"))},

	// The first packet: the CONNECT's strings.
	{Name: "a Client Identifier not well-formed UTF-8 (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(), str("\xFF"))},
	{Name: "a Client Identifier holding an encoded surrogate (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(), str("\xED\xA0\x80"))},
	{Name: "a Client Identifier holding U+0000 (5.0 [MQTT-1.5.4-2])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(), str("a\x00b"))},
	{Name: "a Will Topic not well-formed UTF-8 (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), props(), str("\xFF"), str("p"))},
	{Name: "a User Name not well-formed UTF-8 (3.1.1 [MQTT-1.5.3-1])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag|userNameFlag, str("c"), str("\xFF"))},
	{Name: "a Client Identifier in an overlong encoding (3.1.1 [MQTT-1.5.3-1])", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: connect(V311, cleanFlag, str("\xC0\x80"))},

	// The first packet: the CONNECT's properties.
	{Name: "an unknown property identifier, 0x7F (5.0 §2.2.2.2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(prop(0x7F, 0)), str("c"))},
	{Name: "a property identifier of two bytes (5.0 §2.2.2.2, Table 2-4)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props([]byte{0x91, 0x00}, []byte{0, 0, 0, 1}), str("c"))},
	{Name: "a CONNACK's property on a CONNECT, Server Keep Alive (5.0 §2.2.2.2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(prop(ServerKeepAlive, 0, 10)), str("c"))},
	{Name: "Session Expiry Interval twice (5.0 §3.1.2.11.2)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(SessionExpiryInterval, 0, 0, 0, 1), prop(SessionExpiryInterval, 0, 0, 0, 1)), str("c"))},
	{Name: "a Receive Maximum of 0 (5.0 §3.1.2.11.3)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(ReceiveMaximum, 0, 0)), str("c"))},
	{Name: "a Maximum Packet Size of 0 (5.0 §3.1.2.11.4)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(MaximumPacketSize, 0, 0, 0, 0)), str("c"))},
	{Name: "a Request Problem Information of 2 (5.0 §3.1.2.11.7)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(RequestProblemInformation, 2)), str("c"))},
	{Name: "a Request Response Information of 2 (5.0 §3.1.2.11.6)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(RequestResponseInformation, 2)), str("c"))},
	{Name: "Authentication Data without an Authentication Method (5.0 §3.1.2.11.10)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag, props(prop(AuthenticationData, 0, 1, 'd')), str("c"))},
	{Name: "a Property Length past the packet (5.0 §2.2.2.1)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, []byte{0x20}, str("c"))},
	{Name: "a property value cut short by its Property Length (5.0 §1.5.3)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(prop(SessionExpiryInterval, 0, 0)), str("c"))},
	{Name: "a User Property not well-formed UTF-8 (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, props(prop(UserProperty, bytes.Join([][]byte{str("k"), str("\xFF")}, nil)...)), str("c"))},
	{Name: "a Property Length not in its shortest form (5.0 [MQTT-1.5.5-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag, []byte{0x80, 0x00}, str("c"))},

	// The first packet: the Will Properties.
	{Name: "a CONNECT's property in the Will Properties, Session Expiry Interval (5.0 §2.2.2.2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), props(prop(SessionExpiryInterval, 0, 0, 0, 1)), str("w"), str("p"))},
	{Name: "a Will Payload Format Indicator of 2 (5.0 §3.1.3.2.3)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), props(prop(PayloadFormatIndicator, 2)), str("w"), str("p"))},
	{Name: "Will Delay Interval twice (5.0 §3.1.3.2.2)", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"),
			props(prop(WillDelayInterval, 0, 0, 0, 1), prop(WillDelayInterval, 0, 0, 0, 1)), str("w"), str("p"))},
	{Name: "a Will Response Topic holding + (design.md, CONNECT: \"a Will's Response Topic holding + or # included\")", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), props(prop(ResponseTopic, str("r/+")...)), str("w"), str("p"))},
	{Name: "a Will Response Topic holding # (design.md, CONNECT: \"a Will's Response Topic holding + or # included\")", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), props(prop(ResponseTopic, str("r/#")...)), str("w"), str("p"))},
	{Name: "a Will Property Length past the packet (5.0 §3.1.3.2.1)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: connect(V5, cleanFlag|willFlag, props(), str("c"), []byte{0x20}, str("w"), str("p"))},

	// The first packet: its Remaining Length, its size and its type.
	{Name: "a CONNECT's Remaining Length not in its shortest form (5.0 [MQTT-1.5.5-1])", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: longForm(connect(V5, cleanFlag, props(), str("c")))},
	{Name: "a CONNECT's Remaining Length not in its shortest form, refused in MQTT 3.1.1 too (design.md, Choices MQTT leaves open)",
		Version: V311, Stage: First, Err: ErrMalformed, Reads: true,
		Packet: longForm(connect(V311, cleanFlag, str("c")))},
	{Name: "a Remaining Length of five bytes (5.0 §1.5.5: \"at most four\")", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: []byte{0x10, 0x80, 0x80, 0x80, 0x80, 0x01}},
	{Name: "a Remaining Length of five bytes (3.1.1 §2.2.3)", Version: V311, Stage: First, Err: ErrMalformed,
		Packet: []byte{0x10, 0x80, 0x80, 0x80, 0x80, 0x01}},
	{Name: "a CONNECT over max_connect_size with another Protocol Name (design.md, R6; [MQTT-3.1.2-1])", Version: V5, Stage: First, Err: ErrTooLarge,
		Packet: pkt(0x10, str("MQTX"), []byte{V5, cleanFlag, 0, 0}, props(), str(string(make([]byte, BadMaxConnectSize))))},
	{Name: "a PINGREQ before the CONNECT (5.0 [MQTT-3.1.0-1])", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: []byte{0xC0, 0x00}},
	{Name: "a PINGREQ before the CONNECT (3.1.1 [MQTT-3.1.0-1])", Version: V311, Stage: First, Err: ErrProtocol,
		Packet: []byte{0xC0, 0x00}},
	{Name: "an AUTH before the CONNECT (5.0 [MQTT-3.1.0-1])", Version: V5, Stage: First, Err: ErrProtocol,
		Packet: []byte{0xF0, 0x00}},
	{Name: "the reserved packet type 0 first (5.0 §2.1.2)", Version: V5, Stage: First, Err: ErrMalformed,
		Packet: []byte{0x00, 0x00}},

	// After the CONNACK: PINGREQ, the fixed header and the other types.
	{Name: "a PINGREQ with a body (5.0 §3.12.1: Remaining Length 0)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC0, 0x01, 0x00}},
	{Name: "a PINGREQ with a body (3.1.1 §3.12.1)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC0, 0x01, 0x00}},
	{Name: "a PINGREQ with flags 0001 (5.0 §2.1.3, Table 2-2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC1, 0x00}},
	{Name: "a PINGREQ with flags 0001 (3.1.1 §2.2.2, Table 2.2)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC1, 0x00}},
	{Name: "the reserved packet type 0 (5.0 §2.1.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0x00, 0x00}},
	{Name: "the reserved packet type 0 (3.1.1 §2.2.1)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0x00, 0x00}},
	{Name: "type 15, AUTH, reserved in MQTT 3.1.1 (3.1.1 §2.2.1)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xF0, 0x00}},
	{Name: "an AUTH where the CONNECT named no Authentication Method (5.0 §3.15)", Version: V5, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: []byte{0xF0, 0x00}},
	{Name: "a second CONNECT (5.0 [MQTT-3.1.0-2])", Version: V5, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: connect(V5, cleanFlag, props(), str("again"))},
	{Name: "a second CONNECT (3.1.1 [MQTT-3.1.0-2])", Version: V311, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: connect(V311, cleanFlag, str("again"))},
	{Name: "a CONNACK, which only a server sends (5.0 §2.1.2, Table 2-1)", Version: V5, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: []byte{0x20, 0x03, 0x00, 0x00, 0x00}},
	{Name: "a PINGRESP, which only a server sends (3.1.1 §2.2.1, Table 2.1)", Version: V311, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: []byte{0xD0, 0x00}},

	// After the CONNACK: SUBSCRIBE and UNSUBSCRIBE. MQTT 5 4.13.1 and 3.1.1
	// [MQTT-4.8.0-1], as for every row of this stage; design.md, Flows,
	// SUBSCRIBE: "A malformed filter is a malformed packet, with 0x81" and
	// "Packet validity precedes per-filter policy".
	{Name: "a SUBSCRIBE with packet identifier 0 (5.0 [MQTT-2.2.1-3])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 0}, props(), str("a"), []byte{0})},
	{Name: "a SUBSCRIBE with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 0}, str("a"), []byte{0})},
	{Name: "an UNSUBSCRIBE with packet identifier 0 (5.0 [MQTT-2.2.1-3])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0xA2, []byte{0, 0}, props(), str("a"))},
	{Name: "an UNSUBSCRIBE with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0xA2, []byte{0, 0}, str("a"))},
	{Name: "SUBSCRIBE with flags 0000 (5.0 [MQTT-3.8.1-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x80, []byte{0, 1}, props(), str("a"), []byte{0})},
	{Name: "SUBSCRIBE with flags 0000 (3.1.1 [MQTT-3.8.1-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x80, []byte{0, 1}, str("a"), []byte{0})},
	{Name: "UNSUBSCRIBE with flags 0000 (3.1.1 [MQTT-3.10.1-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xA0, []byte{0, 1}, str("a"))},
	{Name: "a SUBSCRIBE with no filter (5.0 [MQTT-3.8.3-2])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: []byte{0x82, 0x03, 0x00, 0x01, 0x00}},
	{Name: "a SUBSCRIBE with no filter (3.1.1 [MQTT-3.8.3-3])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: []byte{0x82, 0x02, 0x00, 0x01}},
	{Name: "an UNSUBSCRIBE with no filter (5.0 [MQTT-3.10.3-2])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: []byte{0xA2, 0x03, 0x00, 0x01, 0x00}},
	{Name: "an UNSUBSCRIBE with no filter (3.1.1 [MQTT-3.10.3-2])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: []byte{0xA2, 0x02, 0x00, 0x01}},
	{Name: "a subscription at QoS 3 (3.1.1 [MQTT-3-8.3-4], so spelled)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, str("a"), []byte{3})},
	{Name: "3.1.1 subscription option bits 2-7 set (3.1.1 [MQTT-3-8.3-4], so spelled)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, str("a"), []byte{0x04})},
	{Name: "a Maximum QoS of 3 (5.0 §3.8.3.1)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{0x03})},
	{Name: "5.0 subscription option bit 6 set (5.0 [MQTT-3.8.3-5])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{0x40})},
	{Name: "5.0 subscription option bit 7 set on the second filter (5.0 [MQTT-3.8.3-5])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{1}, str("b"), []byte{0x80})},
	{Name: "a Retain Handling of 3 (5.0 §3.8.3.1)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{0x30})},
	{Name: "two Subscription Identifiers on a SUBSCRIBE (5.0 §3.8.2.1.2)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 1}, props(prop(SubscriptionIdentifier, 1), prop(SubscriptionIdentifier, 2)), str("a"), []byte{0})},
	{Name: "a Subscription Identifier of 0 on a SUBSCRIBE (5.0 §3.8.2.1.2)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x82, []byte{0, 1}, props(prop(SubscriptionIdentifier, 0)), str("a"), []byte{0})},
	{Name: "a PUBLISH's property on a SUBSCRIBE, Topic Alias (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, props(prop(TopicAlias, 0, 1)), str("a"), []byte{0})},
	{Name: "a SUBSCRIBE's property on an UNSUBSCRIBE, Subscription Identifier (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xA2, []byte{0, 1}, props(prop(SubscriptionIdentifier, 1)), str("a"))},
	{Name: "a SUBSCRIBE's Property Length past the packet (5.0 §3.8.2.1.1)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0x82, 0x05, 0x00, 0x01, 0x05, 0x26, 0x00}},
	{Name: "a SUBSCRIBE's Topic Filter cut short (5.0 §1.5.4)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, props(), []byte{0, 5, 'a'})},
	{Name: "a SUBSCRIBE's Topic Filter without its options byte (3.1.1 §3.8.3)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, str("a"), []byte{1}, str("b"))},
	{Name: "an UNSUBSCRIBE's Topic Filter cut short (3.1.1 §1.5.3)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xA2, []byte{0, 1}, []byte{0, 5, 'a'})},
	{Name: "a Topic Filter not well-formed UTF-8 (5.0 [MQTT-3.8.3-1], [MQTT-1.5.4-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{0}, str("\xFF"), []byte{0})},
	{Name: "a Topic Filter holding U+0000 (3.1.1 [MQTT-3.8.3-1], [MQTT-1.5.3-2])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x82, []byte{0, 1}, str("a\x00"), []byte{0})},
	{Name: "an UNSUBSCRIBE's Topic Filter holding a surrogate (5.0 [MQTT-3.10.3-1], [MQTT-1.5.4-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xA2, []byte{0, 1}, props(), str("\xED\xA0\x80"))},
	// A Topic Filter's syntax: "All Topic Names and Topic Filters MUST be at
	// least one character long" [MQTT-4.7.3-1]; # "MUST be specified either
	// on its own or following a topic level separator. In either case it
	// MUST be the last character specified in the Topic Filter" (5.0
	// [MQTT-4.7.1-1]; 3.1.1 [MQTT-4.7.1-2]); + "MUST occupy an entire level
	// of the filter" (5.0 [MQTT-4.7.1-2]; 3.1.1 [MQTT-4.7.1-3]); 4.7.1.2's
	// "sport/tennis#" and "sport/tennis/#/ranking" and 4.7.1.3's "sport+"
	// "not valid".
	{Name: "an empty Topic Filter (5.0 [MQTT-4.7.3-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, props(), str(""), []byte{0})},
	{Name: "an empty Topic Filter (3.1.1 [MQTT-4.7.3-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, str(""), []byte{0})},
	{Name: "a # ending a level it does not fill, sport/tennis# (5.0 [MQTT-4.7.1-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("sport/tennis#"), []byte{1})},
	{Name: "a # before the last level, sport/tennis/#/ranking (3.1.1 [MQTT-4.7.1-2])", Version: V311, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, str("sport/tennis/#/ranking"), []byte{1})},
	{Name: "a + sharing its level, sport+ (3.1.1 [MQTT-4.7.1-3])", Version: V311, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, str("sport+"), []byte{0})},
	{Name: "a malformed Topic Filter after a well-formed one, a/+b (5.0 [MQTT-4.7.1-2])", Version: V5, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a"), []byte{1}, str("a/+b"), []byte{1})},
	{Name: "an UNSUBSCRIBE's Topic Filter with a + sharing its level, sport+ (5.0 [MQTT-4.7.1-2])", Version: V5, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0xA2, []byte{0, 1}, props(), str("sport+"))},
	{Name: "an UNSUBSCRIBE's empty Topic Filter (3.1.1 [MQTT-4.7.3-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0xA2, []byte{0, 1}, str(""))},
	// design.md, Flows: "Checks proceed through framing, protocol
	// recognition and packet validity, admission policy, then storage. [...]
	// A later stage never replaces an earlier failure"; MQTT 5 3.9.3: 0x8F is
	// for a Topic Filter "correctly formed but [...] not allowed".
	{Name: "a malformed Topic Filter over max_topic_levels, a/b/c/d/#/e (design.md, Flows: malformed before a limit)", Version: V5, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: pkt(0x82, []byte{0, 1}, props(), str("a/b/c/d/#/e"), []byte{0})},

	// After the CONNACK: the Remaining Length and max_packet_size.
	{Name: "a Remaining Length not in its shortest form (5.0 [MQTT-1.5.5-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC0, 0x80, 0x00}},
	{Name: "a Remaining Length not in its shortest form, refused in MQTT 3.1.1 too (design.md, Choices MQTT leaves open)",
		Version: V311, Stage: AfterConnack, Err: ErrMalformed, Reads: true,
		Packet: []byte{0xC0, 0x80, 0x00}},
	{Name: "a Remaining Length of five bytes (5.0 §1.5.5)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC0, 0x80, 0x80, 0x80, 0x80, 0x00}},
	{Name: "a Remaining Length of five bytes (3.1.1 §2.2.3)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xC0, 0x80, 0x80, 0x80, 0x80, 0x00}},
	{Name: "a packet over max_packet_size, counted whole (design.md, R6; 5.0 §3.1.2.11.4)", Version: V5, Stage: AfterConnack, Err: ErrTooLarge,
		Packet: pkt(0x30, str("a"), []byte{0}, make([]byte, BadMaxPacketSize-5))},
	{Name: "a packet over max_packet_size (design.md, R6)", Version: V311, Stage: AfterConnack, Err: ErrTooLarge,
		Packet: pkt(0x30, str("a"), make([]byte, BadMaxPacketSize-4))},

	// After the CONNACK: DISCONNECT.
	{Name: "a DISCONNECT reason code not in Table 3-10, 0x01 (5.0 [MQTT-3.14.2-1])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: []byte{0xE0, 0x01, 0x01}},
	{Name: "a DISCONNECT reason Table 2-6 gives but Table 3-10 does not, 0x8C (design.md, Choices MQTT leaves open; [MQTT-3.14.2-1])",
		Version: V5, Stage: AfterConnack, Err: ErrProtocol, Packet: []byte{0xE0, 0x01, 0x8C}},
	{Name: "a DISCONNECT reason only a server sends, 0x8E (5.0 §3.14.2.1, Table 3-10)", Version: V5, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: []byte{0xE0, 0x01, 0x8E}},
	{Name: "a CONNECT's property on a DISCONNECT, Receive Maximum (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xE0, []byte{0}, props(prop(ReceiveMaximum, 0, 1)))},
	{Name: "Session Expiry Interval twice on a DISCONNECT (5.0 §3.14.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0xE0, []byte{0}, props(prop(SessionExpiryInterval, 0, 0, 0, 0), prop(SessionExpiryInterval, 0, 0, 0, 0)))},
	{Name: "a DISCONNECT's Property Length past the packet (5.0 §3.14.2.2.1)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xE0, 0x02, 0x00, 0x05}},
	{Name: "a DISCONNECT's Reason String not well-formed UTF-8 (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0xE0, []byte{0}, props(prop(ReasonString, str("\xFF")...)))},
	{Name: "a DISCONNECT raising the session expiry from 0 (5.0 §3.14.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrProtocol, Reads: true,
		Packet: pkt(0xE0, []byte{0}, props(prop(SessionExpiryInterval, 0, 0, 0, 1)))},
	{Name: "a DISCONNECT with flags 0001 (5.0 §2.1.3, Table 2-2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xE1, 0x00}},
	{Name: "a DISCONNECT with a body (3.1.1 §3.14.1: Remaining Length 0)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: []byte{0xE0, 0x01, 0x00}},

	// After the CONNACK: PUBLISH. design.md, PUBLISH: "A Topic Name holding +
	// or # is a malformed packet (MQTT-3.3.2-2). A Response Topic holding
	// them is also malformed (MQTT-3.3.2-14). An MQTT 3.1.1 empty Topic Name
	// is malformed too (MQTT-4.7.3-1). These follow the same rule [...] at
	// any QoS (SUBSCRIBE). The response is DISCONNECT 0x81. An MQTT 3.1.1
	// connection just closes."
	{Name: "a Topic Name holding # (5.0 [MQTT-3.3.2-2]; design.md, PUBLISH: malformed)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x32, str("a/#"), []byte{0, 1}, props())},
	{Name: "a Topic Name holding + (3.1.1 [MQTT-3.3.2-2]; design.md, PUBLISH: malformed)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("+/a"))},
	{Name: "a Topic Name holding # (3.1.1 [MQTT-3.3.2-2]; design.md, PUBLISH: malformed)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x32, str("#"), []byte{0, 1})},
	{Name: "a Response Topic holding + (5.0 [MQTT-3.3.2-14]; design.md, PUBLISH: malformed)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("a"), props(prop(ResponseTopic, str("r/+")...)))},
	{Name: "a Response Topic holding # (5.0 [MQTT-3.3.2-14]; design.md, PUBLISH: malformed)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x32, str("a"), []byte{0, 1}, props(prop(ResponseTopic, str("r/#")...)))},
	// 5.0 §3.3.4: "A Topic Alias value of 0 or greater than the Maximum
	// Topic Alias is a Protocol Error, the receiver uses DISCONNECT with
	// Reason Code of 0x94 (Topic Alias invalid)"; design.md, CONNECT, 8:
	// the CONNACK's "Topic Alias Maximum (16)". The harness reads the row
	// above the maximum: only the CONNACK it answers makes it wrong.
	{Name: "a Topic Alias over the CONNACK's Topic Alias Maximum of 16 (5.0 §3.3.4)", Version: V5, Stage: AfterConnack, Err: ErrTopicAlias, Reads: true,
		Packet: pkt(0x30, str("a"), props(prop(TopicAlias, 0, 17)))},
	// The rules of the PUBLISH the harness's decoder keeps, which the
	// broker's reader keeps as well.
	{Name: "a PUBLISH at QoS 1 with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x32, str("a"), []byte{0, 0})},
	{Name: "a PUBLISH at QoS 3 (3.1.1 [MQTT-3.3.1-4])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x36, str("a"), []byte{0, 1})},
	{Name: "a PUBLISH at QoS 0 with DUP (3.1.1 [MQTT-3.3.1-2])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x38, str("a"))},
	{Name: "a Topic Name holding a wildcard (5.0 [MQTT-3.3.2-2]; design.md, PUBLISH: malformed)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("a/+"), props())},
	{Name: "an empty Topic Name (3.1.1 [MQTT-4.7.3-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str(""))},
	{Name: "an empty Topic Name with no Topic Alias (5.0 §3.3.2.1)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x30, str(""), props())},
	{Name: "a Topic Name not well-formed UTF-8 (3.1.1 [MQTT-1.5.3-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("\xFF"))},
	{Name: "a Topic Name holding U+0000 (3.1.1 [MQTT-1.5.3-2])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("\x00"))},
	{Name: "a Topic Alias of 0 (5.0 §3.3.4)", Version: V5, Stage: AfterConnack, Err: ErrTopicAlias,
		Packet: pkt(0x30, str("a"), props(prop(TopicAlias, 0, 0)))},
	{Name: "a Subscription Identifier of 0 (5.0 §3.3.2.3.8)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
		Packet: pkt(0x30, str("a"), props(prop(SubscriptionIdentifier, 0)))},
	{Name: "a Subscription Identifier not in its shortest form (5.0 [MQTT-1.5.5-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("a"), props(prop(SubscriptionIdentifier, 0xFF, 0x80, 0x00)))},
	{Name: "a Subscription Identifier of five bytes (5.0 §1.5.5)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("a"), props(prop(SubscriptionIdentifier, 0x80, 0x80, 0x80, 0x80, 0x01)))},
	{Name: "a CONNECT's property on a PUBLISH, Session Expiry Interval (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
		Packet: pkt(0x30, str("a"), props(prop(SessionExpiryInterval, 0, 0, 0, 1)))},
}

// badAcks are BadPackets' rows of the acknowledgement kind, PUBACK, PUBREC,
// PUBREL or PUBCOMP (5.0 §3.4 to §3.7, 3.1.1 §3.4 to §3.7), sent after the
// CONNACK: the broker reads each whether or not it names an exchange, as
// packet validity comes before any state (design.md, Flows: "Checks proceed
// through framing, protocol recognition and packet validity"). The four
// share one layout, each section numbered by its type (§3.4 for PUBACK, 4)
// and its reason codes in Table 3-4 to 3-7; failure is a code of 0x80 or
// above from that table. A PUBREL also has reserved flags 0010
// ([MQTT-3.6.1-1]).
func badAcks(kind, failure byte) []BadPacket {
	first := kind<<4 | requiredFlags[kind]
	name := Name(kind)
	section := fmt.Sprintf("3.%d", kind)
	rows := []BadPacket{
		{Name: "a " + name + " with packet identifier 0 (5.0 [MQTT-2.2.1-3])", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
			Packet: []byte{first, 0x02, 0x00, 0x00}},
		{Name: "a " + name + " with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: AfterConnack, Err: ErrProtocol,
			Packet: []byte{first, 0x02, 0x00, 0x00}},
		{Name: fmt.Sprintf("a %s reason code not in Table 3-%d, 0x01 (5.0 [MQTT-%s.2-1])", name, kind, section), Version: V5, Stage: AfterConnack,
			Err: ErrProtocol, Packet: []byte{first, 0x03, 0x00, 0x01, 0x01}},
		{Name: "a " + name + " cut short by its Remaining Length (3.1.1 §" + section + ".1)", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
			Packet: []byte{first, 0x01, 0x00}},
		{Name: "a 3.1.1 " + name + " with a reason code (3.1.1 §" + section + ".1: Remaining Length 2)", Version: V311, Stage: AfterConnack,
			Err: ErrMalformed, Packet: []byte{first, 0x03, 0x00, 0x01, 0x00}},
		{Name: "a byte after a " + name + "'s empty property section (5.0 §" + section + ".3: no Payload)", Version: V5, Stage: AfterConnack,
			Err: ErrMalformed, Packet: []byte{first, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00}},
		{Name: "an unknown property identifier on a " + name + ", 0x7F (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
			Packet: []byte{first, 0x05, 0x00, 0x01, 0x00, 0x01, 0x7F}},
		{Name: "a PUBLISH's property on a " + name + ", Topic Alias (5.0 §2.2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
			Packet: pkt(first, []byte{0, 1, 0}, props(prop(TopicAlias, 0, 1)))},
		{Name: "a " + name + "'s Reason String twice (5.0 §" + section + ".2.2.2)", Version: V5, Stage: AfterConnack, Err: ErrProtocol,
			Packet: pkt(first, []byte{0, 1, failure}, props(prop(ReasonString, str("a")...), prop(ReasonString, str("b")...)))},
		{Name: "a " + name + "'s property value cut short (5.0 §1.5.4)", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
			Packet: []byte{first, 0x06, 0x00, 0x01, 0x00, 0x02, ReasonString, 0x00}},
		{Name: "a " + name + "'s User Property value not well-formed UTF-8 (5.0 [MQTT-1.5.4-1])", Version: V5, Stage: AfterConnack, Err: ErrMalformed,
			Packet: pkt(first, []byte{0, 1, 0}, props(prop(UserProperty, bytes.Join([][]byte{str("k"), str("\xFF")}, nil)...)))},
	}
	if kind == PUBREL {
		rows = append(rows, BadPacket{Name: "PUBREL with flags 0000 (3.1.1 [MQTT-3.6.1-1])", Version: V311, Stage: AfterConnack, Err: ErrMalformed,
			Packet: []byte{0x60, 0x02, 0x00, 0x01}})
	}
	return rows
}

// harnessRows are BadPackets' rows of the packets the broker does not
// read, which the harness alone sends.
var harnessRows = []BadPacket{
	{Name: "a SUBACK with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: HarnessOnly, Err: ErrProtocol,
		Packet: []byte{0x90, 0x03, 0x00, 0x00, 0x00}},
	{Name: "an UNSUBACK with packet identifier 0 (3.1.1 [MQTT-2.3.1-1])", Version: V311, Stage: HarnessOnly, Err: ErrProtocol,
		Packet: []byte{0xB0, 0x02, 0x00, 0x00}},
	{Name: "PINGRESP with flags 0001 (3.1.1 §2.2.2, Table 2.2)", Version: V311, Stage: HarnessOnly, Err: ErrMalformed,
		Packet: []byte{0xD1, 0x00}},
	{Name: "PINGRESP with a body (3.1.1 §3.13.1: Remaining Length 0)", Version: V311, Stage: HarnessOnly, Err: ErrMalformed,
		Packet: []byte{0xD0, 0x01, 0x00}},
	{Name: "CONNACK reserved flags set (5.0 [MQTT-3.2.2-1])", Version: V5, Stage: HarnessOnly, Err: ErrMalformed,
		Packet: []byte{0x20, 0x03, 0x02, 0x00, 0x00}},
	{Name: "a 3.1.1 CONNACK carrying properties (3.1.1 §3.2.1: Remaining Length 2)", Version: V311, Stage: HarnessOnly, Err: ErrMalformed,
		Packet: []byte{0x20, 0x03, 0x00, 0x00, 0x00}},
	{Name: "a CONNACK's Maximum QoS of 2 (5.0 §3.2.2.3.4)", Version: V5, Stage: HarnessOnly, Err: ErrProtocol,
		Packet: pkt(0x20, []byte{0, 0}, props(prop(MaximumQoS, 2)))},
	{Name: "a CONNACK's Receive Maximum of 0 (5.0 §3.2.2.3.3)", Version: V5, Stage: HarnessOnly, Err: ErrProtocol,
		Packet: pkt(0x20, []byte{0, 0}, props(prop(ReceiveMaximum, 0, 0)))},
}
