package packet_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// The connection handshake's answer, CONNACK (MQTT 5 3.2, MQTT 3.1.1 3.2).
// A Connack's ReasonCode is MQTT 5's Connect Reason Code in both versions;
// for MQTT 3.1.1 the encoder and the decoder map it to and from the Connect
// Return code. Error classes follow connect_test.go. The broker writes a
// CONNACK to each client; only a bridge reads one, from its peer, and what
// these tests checked of reading it is checked from the wire, where a
// bridge's peer answers it (item 0019b; design.md, Bridges): the tests
// here hold the encoder (connackCodec.writer), the fuzz target and the
// allocation guard the decoder.

// connectReasonCodes is MQTT 5 Table 3-1, the Connect Reason Codes, as
// reasonTables holds it. "The Server sending the CONNACK packet MUST use one
// of the Connect Reason Code values" [MQTT-3.2.2-8].
var connectReasonCodes = codesOf(packet.CONNACK)

// returnCodes maps each MQTT 5 Connect Reason Code that has an MQTT 3.1.1
// Connect Return code to it, for the encoder. design.md, Flows, CONNECT,
// the refusal table's "MQTT 3.1.1 return code" column: 0x84 to 1; 0x85 to
// 2; 0x86 to 4; 0x87, 0x90, 0x9A and 0x9B to 5; 0x83 and 0x97 to 3;
// 0x95 "none: the connection closes" and 0x8C "not in MQTT 3.1.1". The
// table leaves out Success and Server unavailable, which pair by name
// (MQTT 5 Table 3-1, MQTT 3.1.1 Table 3.1): 0x00 Success to 0 Connection
// Accepted, 0x88 Server unavailable to 3 Server unavailable. Every other
// reason code has no return code: MQTT 3.1.1 3.2.2.3, "If none of the
// return codes listed in Table 3.1 ... are deemed applicable, then the
// Server MUST close the Network Connection without sending a CONNACK
// [MQTT-3.2.2-6]", so the encoder refuses it.
var returnCodes = map[byte]byte{
	0x00: 0, 0x84: 1, 0x85: 2, 0x86: 4,
	0x87: 5, 0x90: 5, 0x9A: 5, 0x9B: 5,
	0x88: 3, 0x83: 3, 0x97: 3,
}

// sameConnack reports whether two CONNACKs hold the same fields.
func sameConnack(a, b *packet.Connack) bool {
	return a.SessionPresent == b.SessionPresent && a.ReasonCode == b.ReasonCode && sameValues(a.Properties, b.Properties)
}

// connackOK is the oracle for a Connack the encoder writes and the decoder
// returns under version: a reason code the version can carry, Session
// Present 0 with any non-zero code (MQTT 5 [MQTT-3.2.2-6], MQTT 3.1.1
// [MQTT-3.2.2-4]: "If a Server sends a CONNACK packet containing a non-zero
// Reason Code it MUST set Session Present to 0"), and properties only in
// MQTT 5, within Table 2-4 and valueRules.
func connackOK(version packet.Version, c *packet.Connack) bool {
	if c.SessionPresent && c.ReasonCode != 0 {
		return false
	}
	switch version {
	case packet.V5:
		props, ok := valuesOf(c.Properties)
		return ok && slices.Contains(connectReasonCodes, c.ReasonCode) &&
			encodable(placeNamed["CONNACK"], props) && valuesOK("CONNACK", props)
	case packet.V311:
		_, mapped := returnCodes[c.ReasonCode]
		return mapped && len(c.Properties) == 0
	}
	return false
}

// connackCodec describes CONNACK's codec. Its fuzz oracle is connackOK; its
// encoder checks its Properties, and NewProperties makes them for the core
// (TestConnackProperties). Fixed header:
// flags 0 (MQTT 5 3.2.1, MQTT 3.1.1 3.2.1).
var connackCodec = codec[packet.Connack]{
	name: "CONNACK", typ: packet.CONNACK, first: byte(packet.CONNACK) << 4,
	read: packet.DecodeConnack, write: packet.AppendConnack, same: sameConnack, ok: connackOK,
	newProperties: true, checksProperties: true,
}

// connackCase is a CONNACK body under version and the Connack it decodes to.
func connackCase(name string, version packet.Version, body []byte, want packet.Connack) packetCase[packet.Connack] {
	return packetCase[packet.Connack]{name, &connackCodec, version, body, want}
}

// everyConnackProperty sets every property 3.2.2.3 lists, each at a value
// its section allows.
var everyConnackProperty = []wired{
	intProp(0x11, 0xFFFFFFFF), intProp(0x21, 65_535), intProp(0x24, 0), intProp(0x25, 0),
	intProp(0x27, 1), stringProp(0x12, "auto-1"), intProp(0x22, 0), stringProp(0x1F, "ok"),
	userProp("n", "v"), userProp("n", "v"), intProp(0x28, 0), intProp(0x29, 1), intProp(0x2A, 0),
	intProp(0x13, 0), stringProp(0x1A, "r/"), stringProp(0x1C, "other:1883"),
	stringProp(0x15, "M"), binaryProp(0x16, []byte{0x02}),
}

// validConnacks are CONNACKs the standards allow. MQTT 5 3.2.2: "Connect
// Acknowledge Flags, Connect Reason Code, and Properties"; Session Present
// is bit 0 (3.2.2.1.1). MQTT 3.1.1 3.2.2: the flags, then the Connect
// Return code, a Remaining Length of 2 (3.2.1).
var validConnacks = []packetCase[packet.Connack]{
	connackCase("MQTT 3.1.1, accepted", packet.V311, []byte{0x00, 0x00}, packet.Connack{}),
	connackCase("MQTT 3.1.1, accepted, Session Present", packet.V311, []byte{0x01, 0x00}, packet.Connack{SessionPresent: true}),
	connackCase("MQTT 3.1.1, not authorized", packet.V311, []byte{0x00, 0x05}, packet.Connack{ReasonCode: 0x87}),
	connackCase("MQTT 5, success", packet.V5, []byte{0x00, 0x00, 0x00}, packet.Connack{}),
	connackCase("MQTT 5, success, Session Present", packet.V5, []byte{0x01, 0x00, 0x00}, packet.Connack{SessionPresent: true}),
	connackCase("MQTT 5, Banned", packet.V5, []byte{0x00, 0x8A, 0x00}, packet.Connack{ReasonCode: 0x8A}),
	connackCase(
		"MQTT 5, every CONNACK property", packet.V5,
		append([]byte{0x00, 0x00}, setOf(everyConnackProperty)...),
		packet.Connack{Properties: propsOf(everyConnackProperty)},
	),
}

// TestConnack encodes each of validConnacks: the encoder writes the whole
// packet, after what dst holds.
func TestConnack(t *testing.T) {
	testRoundTrips(t, writing(validConnacks))
}

// TestConnackReasonCodes tries every reason code byte, with Session Present
// 0 and 1, in MQTT 5. A code of Table 3-1 [MQTT-3.2.2-8] is written,
// Session Present with it only for 0x00 [MQTT-3.2.2-6]; the rest are
// refused with ErrCannotEncode.
func TestConnackReasonCodes(t *testing.T) {
	for code := range 256 {
		for _, sp := range []bool{false, true} {
			flags := byte(0)
			if sp {
				flags = 1
			}
			body := []byte{flags, byte(code), 0x00}
			want := packet.Connack{SessionPresent: sp, ReasonCode: byte(code)}
			out, encErr := packet.AppendConnack(nil, packet.V5, &want)
			if slices.Contains(connectReasonCodes, byte(code)) && (!sp || code == 0) {
				if encErr != nil || !bytes.Equal(out, connackCodec.wire(body)) {
					t.Errorf("MQTT 5, %+v: AppendConnack = % X, %v; want % X", want, out, encErr, connackCodec.wire(body))
				}
				continue
			}
			if !cannotEncode(encErr) {
				t.Errorf("MQTT 5, %+v: AppendConnack = % X, %v; want ErrCannotEncode", want, out, encErr)
			}
		}
	}
}

// TestConnackReturnCodes maps MQTT 5 reason codes to MQTT 3.1.1 return
// codes. Every reason code byte is tried with Session Present 0 and 1: one
// with a return code (returnCodes) is written as it, Session Present only
// with 0 [MQTT-3.2.2-4]; any other is refused.
func TestConnackReturnCodes(t *testing.T) {
	for b := range 256 {
		for _, sp := range []bool{false, true} {
			flags := byte(0)
			if sp {
				flags = 1
			}
			c := packet.Connack{SessionPresent: sp, ReasonCode: byte(b)}
			out, err := packet.AppendConnack(nil, packet.V311, &c)
			if rc, mapped := returnCodes[byte(b)]; mapped && (!sp || b == 0) {
				if want := connackCodec.wire([]byte{flags, rc}); err != nil || !bytes.Equal(out, want) {
					t.Errorf("MQTT 3.1.1, %+v: AppendConnack = % X, %v; want % X", c, out, err, want)
				}
			} else if !cannotEncode(err) {
				t.Errorf("MQTT 3.1.1, %+v: AppendConnack = % X, %v; want ErrCannotEncode", c, out, err)
			}
		}
	}
}

// refusedConnackProps are CONNACK property sets the standard forbids,
// beyond valueRules.
var refusedConnackProps = []propertyCase{
	// 3.2.2.3.14: "It is a Protocol Error to include the Server Keep Alive
	// more than once."
	{"Server Keep Alive twice", []wired{intProp(0x13, 1), intProp(0x13, 1)}, packet.ErrProtocol},
	// 3.2.2.3.7: "It is a Protocol Error to include the Assigned Client
	// Identifier more than once."
	{"Assigned Client Identifier twice", []wired{stringProp(0x12, "a"), stringProp(0x12, "a")}, packet.ErrProtocol},
	// 2.2.2.2: "A Control Packet which contains an Identifier which is not
	// valid for its packet type ... is a Malformed Packet."
	{"Request Problem Information, a CONNECT property", []wired{intProp(0x17, 1)}, packet.ErrMalformed},
	{"Will Delay Interval, a Will property", []wired{intProp(0x18, 1)}, packet.ErrMalformed},
	{"Subscription Identifier, a PUBLISH property", []wired{intProp(0x0B, 1)}, packet.ErrMalformed},
}

// TestConnackProperties checks the value rules of 3.2.2.3 on the writing
// side (valueRules, refusedConnackProps): NewProperties and the encoder
// refuse with ErrCannotEncode what the rules forbid; the values the rules
// allow are made by NewProperties as their wire bytes, and written.
func TestConnackProperties(t *testing.T) {
	testProperties(t, connackCodec.writer(), propsCases(rulesFor("CONNACK"), refusedConnackProps), func(c *propsCase) ([]byte, packet.Connack) {
		return append([]byte{0x00, 0x00}, setOf(c.props)...), packet.Connack{Properties: propsOf(c.props)}
	})
}

// FuzzDecodeConnack feeds random bytes to DecodeConnack under both versions,
// through the shared body (codec.fuzzDecode). It must never panic; it refuses
// with a packet error; what it accepts obeys connackOK and writes back as
// exactly the packet it came from (design.md, Testing: "Code that reads bytes
// or text from outside Sagüin is fuzzed, whether the input comes from
// clients, a bridge's peer or the operator's files").
func FuzzDecodeConnack(f *testing.F) {
	for _, c := range validConnacks {
		f.Add(c.body, c.version == packet.V5)
	}
	f.Add([]byte{0x01, 0x05}, false)
	f.Add([]byte{0x00, 0x06}, false)
	f.Add([]byte{0x02, 0x00, 0x00}, true)
	f.Add([]byte{0x01, 0x80, 0x00}, true)
	f.Add([]byte{0x00, 0x8B, 0x00}, true)
	f.Add([]byte{0x00, 0x00, 0x02, 0x24, 0x02}, true)
	f.Fuzz(func(t *testing.T, data []byte, v5 bool) {
		connackCodec.fuzzDecode(t, versionOf(v5), data)
	})
}
