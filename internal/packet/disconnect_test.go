package packet_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// The end of a connection, DISCONNECT (MQTT 5 3.14, MQTT 3.1.1 3.14). A
// Disconnect's ReasonCode is MQTT 5's Disconnect Reason Code. MQTT 3.1.1's
// DISCONNECT carries no reason and no properties: it reads as 0x00, Normal
// disconnection, which is what an MQTT 5 DISCONNECT with a Remaining Length
// of 0 means (3.14.2.1). Error classes follow connect_test.go.

// disconnectReasonCodes is MQTT 5 Table 3-10, the Disconnect Reason Codes,
// as reasonTables holds it: without 0x8C, which Table 2-6 alone gives
// DISCONNECT (design.md, Choices MQTT leaves open). "The Client or Server
// sending the DISCONNECT packet MUST use one of the DISCONNECT Reason Code
// values [MQTT-3.14.2-1]."
var disconnectReasonCodes = codesOf(packet.DISCONNECT)

// sameDisconnect reports whether two DISCONNECTs hold the same fields.
func sameDisconnect(a, b *packet.Disconnect) bool {
	return a.ReasonCode == b.ReasonCode && sameValues(a.Properties, b.Properties)
}

// disconnectOK is the oracle for a Disconnect the encoder writes and the
// decoder returns under version. MQTT 5: a code of Table 3-10
// [MQTT-3.14.2-1] and properties within Table 2-4 and their repeat rules
// (3.14.2.2). MQTT 3.1.1: nothing at all (3.14.2, 3.14.3).
func disconnectOK(version packet.Version, d *packet.Disconnect) bool {
	switch version {
	case packet.V5:
		props, ok := valuesOf(d.Properties)
		return ok && slices.Contains(disconnectReasonCodes, d.ReasonCode) && encodable(placeNamed["DISCONNECT"], props)
	case packet.V311:
		return d.ReasonCode == 0 && len(d.Properties) == 0
	}
	return false
}

// shortestDisconnect is the body the encoder writes for an MQTT 5 DISCONNECT
// with no properties: the shortest the standard allows. 3.14.2.1: "The
// Reason Code and Property Length can be omitted if the Reason Code is 0x00
// (Normal disconnecton) and there are no Properties. In this case the
// DISCONNECT has a Remaining Length of 0." 3.14.2.2.1: "If the Remaining
// Length is less than 2, a value of 0 is used", so a reason code without
// properties needs no Property Length.
func shortestDisconnect(code byte) []byte {
	if code == 0 {
		return nil
	}
	return []byte{code}
}

// disconnectCodec describes DISCONNECT's codec. Its fuzz oracle is
// disconnectOK; for a body of two bytes or fewer it writes the shortest
// form (shortestDisconnect); its encoder checks its Properties, and
// NewProperties makes them for the core (TestPropertyTable). Fixed
// header: flags 0 (MQTT 5 3.14.1, MQTT
// 3.1.1 3.14.1).
var disconnectCodec = codec[packet.Disconnect]{
	name: "DISCONNECT", typ: packet.DISCONNECT, first: byte(packet.DISCONNECT) << 4,
	read: packet.DecodeDisconnect, write: packet.AppendDisconnect, same: sameDisconnect, ok: disconnectOK,
	shortest: func(data []byte, d *packet.Disconnect) []byte {
		if len(data) <= 2 {
			return shortestDisconnect(d.ReasonCode)
		}
		return data
	},
	newProperties: true, checksProperties: true,
}

// disconnectCase is a DISCONNECT body under version and the Disconnect it
// decodes to.
func disconnectCase(name string, version packet.Version, body []byte, want packet.Disconnect) packetCase[packet.Disconnect] {
	return packetCase[packet.Disconnect]{name, &disconnectCodec, version, body, want}
}

// everyDisconnectProperty sets every property 3.14.2.2 lists, each at a
// value its section allows, the User Property twice under one name
// (3.14.2.2.4: "The same name is allowed to appear more than once").
var everyDisconnectProperty = []wired{
	intProp(0x11, 0xFFFFFFFF), stringProp(0x1F, "bye"), userProp("n", "v"), userProp("n", "w"),
	stringProp(0x1C, "other:1883"),
}

// validDisconnects are DISCONNECTs the standards allow, each in the form the
// encoder writes. MQTT 5 3.14.2: "Disconnect Reason Code, and Properties".
// MQTT 3.1.1 3.14.2: "The DISCONNECT Packet has no variable header"; 3.14.3:
// "The DISCONNECT Packet has no payload."
var validDisconnects = []packetCase[packet.Disconnect]{
	disconnectCase("MQTT 3.1.1", packet.V311, nil, packet.Disconnect{}),
	// 3.14.2.1: "If the Remaining Length is less than 1 the value of 0x00
	// (Normal disconnection) is used."
	disconnectCase("MQTT 5, Remaining Length 0", packet.V5, nil, packet.Disconnect{}),
	// 3.14.2.2.1: "If the Remaining Length is less than 2, a value of 0 is
	// used." design.md, Flows, CONNECT, Take over: "DISCONNECT 0x8E".
	disconnectCase("MQTT 5, Session taken over, Remaining Length 1", packet.V5, []byte{0x8E}, packet.Disconnect{ReasonCode: 0x8E}),
	// Figure 3-24, the non-normative example: Reason Code 0, a Property
	// Length of 5, a Session Expiry Interval of 0.
	disconnectCase(
		"MQTT 5, the example of Figure 3-24", packet.V5, []byte{0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00},
		packet.Disconnect{Properties: propsOf([]wired{intProp(0x11, 0)})},
	),
	// design.md, Flows, DISCONNECT: "0x04 keeps it for publication at the
	// earlier of Will Delay ending and the session ending. The client may
	// change its session expiry".
	disconnectCase(
		"MQTT 5, Disconnect with Will Message and a Session Expiry Interval", packet.V5,
		append([]byte{0x04}, setOf([]wired{intProp(0x11, 3600)})...),
		packet.Disconnect{ReasonCode: 0x04, Properties: propsOf([]wired{intProp(0x11, 3600)})},
	),
	// 3.14.2.2.5: "The Server sends DISCONNECT including a Server Reference
	// and Reason Code 0x9C (Use another server) or 0x9D (Server moved)".
	disconnectCase(
		"MQTT 5, Server moved, a Server Reference", packet.V5,
		append([]byte{0x9D}, setOf([]wired{stringProp(0x1C, "other:1883")})...),
		packet.Disconnect{ReasonCode: 0x9D, Properties: propsOf([]wired{stringProp(0x1C, "other:1883")})},
	),
	disconnectCase(
		"MQTT 5, every DISCONNECT property", packet.V5,
		append([]byte{0x80}, setOf(everyDisconnectProperty)...),
		packet.Disconnect{ReasonCode: 0x80, Properties: propsOf(everyDisconnectProperty)},
	),
}

// TestDisconnect decodes and encodes each of validDisconnects: the decoder
// returns every field; the encoder writes the whole packet, fixed header
// included (flags 0, MQTT 5 3.14.1, MQTT 3.1.1 3.14.1), after what dst
// holds.
func TestDisconnect(t *testing.T) {
	testRoundTrips(t, validDisconnects)
}

// TestDisconnectLongerForms: an MQTT 5 DISCONNECT may carry a Reason Code
// or a Property Length it could have left out (3.14.2.1, 3.14.2.2.1:
// either "can be omitted"). The decoder reads the longer forms; the encoder
// writes the shortest (shortestDisconnect).
func TestDisconnectLongerForms(t *testing.T) {
	cases := []struct {
		body []byte
		want packet.Disconnect
	}{
		{[]byte{0x00}, packet.Disconnect{}},
		{[]byte{0x00, 0x00}, packet.Disconnect{}},
		{[]byte{0x8E, 0x00}, packet.Disconnect{ReasonCode: 0x8E}},
	}
	for _, c := range cases {
		got, err := packet.DecodeDisconnect(c.body, packet.V5)
		if err != nil || !sameDisconnect(&got, &c.want) {
			t.Errorf("DecodeDisconnect(% X) = %+v, %v; want %+v", c.body, got, err, c.want)
		}
		wire := disconnectCodec.wire(shortestDisconnect(c.want.ReasonCode))
		if out, err := packet.AppendDisconnect(nil, packet.V5, &c.want); err != nil || !bytes.Equal(out, wire) {
			t.Errorf("%+v: AppendDisconnect = % X, %v; want % X", c.want, out, err, wire)
		}
	}
}

// TestDisconnectReasonCodes tries every reason code byte in MQTT 5, with
// and without a Property Length. A code of Table 3-10 [MQTT-3.14.2-1] is
// decoded and written; the rest are refused both ways. The sentence names
// no class.
func TestDisconnectReasonCodes(t *testing.T) {
	for code := range 256 {
		want := packet.Disconnect{ReasonCode: byte(code)}
		known := slices.Contains(disconnectReasonCodes, byte(code))
		for _, body := range [][]byte{{byte(code)}, {byte(code), 0x00}} {
			got, err := packet.DecodeDisconnect(body, packet.V5)
			switch {
			case known && (err != nil || !sameDisconnect(&got, &want)):
				t.Errorf("% X: got %+v, %v; want %+v", body, got, err, want)
			case !known && !isKind(err):
				t.Errorf("% X: got %+v, %v; want ErrMalformed or ErrProtocol", body, got, err)
			}
		}
		if !known {
			refusedToWrite(t, fmt.Sprintf("AppendDisconnect, MQTT 5, reason code %#02X", code), func(dst []byte) ([]byte, error) {
				return packet.AppendDisconnect(dst, packet.V5, &want)
			})
			continue
		}
		wire := disconnectCodec.wire(shortestDisconnect(byte(code)))
		if out, err := packet.AppendDisconnect(nil, packet.V5, &want); err != nil || !bytes.Equal(out, wire) {
			t.Errorf("%+v: AppendDisconnect = % X, %v; want % X", want, out, err, wire)
		}
	}
}

// FuzzDecodeDisconnect feeds random bytes to DecodeDisconnect under both
// versions, through the shared body (codec.fuzzDecode). It must never panic; it
// refuses with a packet error; what it accepts obeys disconnectOK and writes
// back in the shortest form: the bytes it came from, unless they carried a
// Reason Code or Property Length the encoder leaves out
// (TestDisconnectLongerForms). That form reads back as the same Disconnect
// (design.md, Testing: "Code that reads bytes or text from outside Sagüin is
// fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files").
func FuzzDecodeDisconnect(f *testing.F) {
	for _, c := range validDisconnects {
		f.Add(c.body, c.version == packet.V5)
	}
	f.Add([]byte{0x00}, false)
	f.Add([]byte{0x00, 0x00}, true)
	f.Add([]byte{0x01}, true)
	f.Add([]byte{0x00, 0x02, 0x15, 0x00}, true)
	f.Add([]byte{0x00, 0x05, 0x11, 0x00}, true)
	f.Fuzz(func(t *testing.T, data []byte, v5 bool) {
		disconnectCodec.fuzzDecode(t, versionOf(v5), data)
	})
}
