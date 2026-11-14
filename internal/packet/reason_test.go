package packet_test

import (
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Reason Codes (MQTT 5 2.4): "The CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP,
// DISCONNECT and AUTH Control Packets have a single Reason Code as part of
// the Variable Header. The SUBACK and UNSUBACK packets contain a list of one
// or more Reason Codes in the Payload." Each of these packets has its own
// table of the codes it may carry, and a sentence binding the sender to it.
//
// Table 2-6 gives the same codes with the packets that use them, and
// disagrees once: it gives 0x8C "Bad authentication method" to DISCONNECT,
// which Table 3-10 does not list. design.md, Choices MQTT leaves open: "the
// packet's own table: DISCONNECT never carries 0x8C (MQTT-3.14.2-1)". So the
// oracle below is each packet's own table, and Table 2-6 serves only for a
// name the packet's table misspells (reasonTables).
//
// The API these tests fix:
//
//	func ReasonName(t Type, code byte) string
//
// ReasonName gives the MQTT 5 name of code on a packet of type t, or "" when
// that packet cannot carry it. The codecs check a reason code against the
// same table, and metrics takes its reason labels from it.

// reasonCode is one row of a packet's reason-code table: a code and its name.
type reasonCode struct {
	code byte
	name string
}

// reasonTables holds each packet's own reason-code table, row for row, in
// the standard's order. A name is spelled as its table spells it, with two
// exceptions where the table departs from every other spelling of the same
// code, Table 2-6's included: Table 3-4 writes 0x91 "Packet identifier in
// use" (lower-case i), and Table 3-5 ends 0x10 with a full stop ("No
// matching subscribers."). Both are written here as Table 2-6 writes them.
var reasonTables = map[packet.Type][]reasonCode{
	// Table 3-1 - Connect Reason Code values (3.2.2.2): "The Server sending
	// the CONNACK packet MUST use one of the Connect Reason Code values"
	// [MQTT-3.2.2-8].
	packet.CONNACK: {
		{0x00, "Success"},
		{0x80, "Unspecified error"},
		{0x81, "Malformed Packet"},
		{0x82, "Protocol Error"},
		{0x83, "Implementation specific error"},
		{0x84, "Unsupported Protocol Version"},
		{0x85, "Client Identifier not valid"},
		{0x86, "Bad User Name or Password"},
		{0x87, "Not authorized"},
		{0x88, "Server unavailable"},
		{0x89, "Server busy"},
		{0x8A, "Banned"},
		{0x8C, "Bad authentication method"},
		{0x90, "Topic Name invalid"},
		{0x95, "Packet too large"},
		{0x97, "Quota exceeded"},
		{0x99, "Payload format invalid"},
		{0x9A, "Retain not supported"},
		{0x9B, "QoS not supported"},
		{0x9C, "Use another server"},
		{0x9D, "Server moved"},
		{0x9F, "Connection rate exceeded"},
	},
	// Table 3-4 - PUBACK Reason Codes (3.4.2.1): "The Client or Server
	// sending the PUBACK packet MUST use one of the PUBACK Reason Codes"
	// [MQTT-3.4.2-1].
	packet.PUBACK: {
		{0x00, "Success"},
		{0x10, "No matching subscribers"},
		{0x80, "Unspecified error"},
		{0x83, "Implementation specific error"},
		{0x87, "Not authorized"},
		{0x90, "Topic Name invalid"},
		{0x91, "Packet Identifier in use"},
		{0x97, "Quota exceeded"},
		{0x99, "Payload format invalid"},
	},
	// Table 3-5 - PUBREC Reason Codes (3.5.2.1): "The Client or Server
	// sending the PUBREC packet MUST use one of the PUBREC Reason Code
	// values" [MQTT-3.5.2-1].
	packet.PUBREC: {
		{0x00, "Success"},
		{0x10, "No matching subscribers"},
		{0x80, "Unspecified error"},
		{0x83, "Implementation specific error"},
		{0x87, "Not authorized"},
		{0x90, "Topic Name invalid"},
		{0x91, "Packet Identifier in use"},
		{0x97, "Quota exceeded"},
		{0x99, "Payload format invalid"},
	},
	// Table 3-6 - PUBREL Reason Codes (3.6.2.1): "The Client or Server
	// sending the PUBREL packet MUST use one of the PUBREL Reason Code
	// values" [MQTT-3.6.2-1].
	packet.PUBREL: {
		{0x00, "Success"},
		{0x92, "Packet Identifier not found"},
	},
	// Table 3-7 - PUBCOMP Reason Codes (3.7.2.1): "The Client or Server
	// sending the PUBCOMP packet MUST use one of the PUBCOMP Reason Code
	// values" [MQTT-3.7.2-1].
	packet.PUBCOMP: {
		{0x00, "Success"},
		{0x92, "Packet Identifier not found"},
	},
	// Table 3-8 - Subscribe Reason Codes (3.9.3): "The Server sending a
	// SUBACK packet MUST use one of the Subscribe Reason Codes for each
	// Topic Filter received" [MQTT-3.9.3-2].
	packet.SUBACK: {
		{0x00, "Granted QoS 0"},
		{0x01, "Granted QoS 1"},
		{0x02, "Granted QoS 2"},
		{0x80, "Unspecified error"},
		{0x83, "Implementation specific error"},
		{0x87, "Not authorized"},
		{0x8F, "Topic Filter invalid"},
		{0x91, "Packet Identifier in use"},
		{0x97, "Quota exceeded"},
		{0x9E, "Shared Subscriptions not supported"},
		{0xA1, "Subscription Identifiers not supported"},
		{0xA2, "Wildcard Subscriptions not supported"},
	},
	// Table 3-9 - Unsubscribe Reason Codes (3.11.3): "The Server sending an
	// UNSUBACK packet MUST use one of the Unsubscribe Reason Code values for
	// each Topic Filter received" [MQTT-3.11.3-2].
	packet.UNSUBACK: {
		{0x00, "Success"},
		{0x11, "No subscription existed"},
		{0x80, "Unspecified error"},
		{0x83, "Implementation specific error"},
		{0x87, "Not authorized"},
		{0x8F, "Topic Filter invalid"},
		{0x91, "Packet Identifier in use"},
	},
	// Table 3-10 - Disconnect Reason Code values (3.14.2.1): "The Client or
	// Server sending the DISCONNECT packet MUST use one of the DISCONNECT
	// Reason Code values" [MQTT-3.14.2-1]. No 0x8C (design.md, Choices MQTT
	// leaves open).
	packet.DISCONNECT: {
		{0x00, "Normal disconnection"},
		{0x04, "Disconnect with Will Message"},
		{0x80, "Unspecified error"},
		{0x81, "Malformed Packet"},
		{0x82, "Protocol Error"},
		{0x83, "Implementation specific error"},
		{0x87, "Not authorized"},
		{0x89, "Server busy"},
		{0x8B, "Server shutting down"},
		{0x8D, "Keep Alive timeout"},
		{0x8E, "Session taken over"},
		{0x8F, "Topic Filter invalid"},
		{0x90, "Topic Name invalid"},
		{0x93, "Receive Maximum exceeded"},
		{0x94, "Topic Alias invalid"},
		{0x95, "Packet too large"},
		{0x96, "Message rate too high"},
		{0x97, "Quota exceeded"},
		{0x98, "Administrative action"},
		{0x99, "Payload format invalid"},
		{0x9A, "Retain not supported"},
		{0x9B, "QoS not supported"},
		{0x9C, "Use another server"},
		{0x9D, "Server moved"},
		{0x9E, "Shared Subscriptions not supported"},
		{0x9F, "Connection rate exceeded"},
		{0xA0, "Maximum connect time"},
		{0xA1, "Subscription Identifiers not supported"},
		{0xA2, "Wildcard Subscriptions not supported"},
	},
	// Table 3-11 - Authenticate Reason Codes (3.15.2.1): "The sender of the
	// AUTH Packet MUST use one of the Authenticate Reason Codes"
	// [MQTT-3.15.2-1].
	packet.AUTH: {
		{0x00, "Success"},
		{0x18, "Continue authentication"},
		{0x19, "Re-authenticate"},
	},
}

// codesOf returns the codes of t's reason-code table, in its order: the set
// the codec tests try every byte against.
func codesOf(t packet.Type) []byte {
	codes := make([]byte, len(reasonTables[t]))
	for i, row := range reasonTables[t] {
		codes[i] = row.code
	}
	return codes
}

// TestReasonName asks ReasonName for every code on every packet type, the
// ones that carry no Reason Code and values beyond the four bits of a type
// included: it gives the name in the packet's own table, and "" for a code
// the table does not list.
func TestReasonName(t *testing.T) {
	for typ, rows := range reasonTables {
		// Each table is in strictly ascending order, as the standard prints
		// it, so a row copied twice cannot hide.
		for i := 1; i < len(rows); i++ {
			if rows[i-1].code >= rows[i].code {
				t.Fatalf("reasonTables[%d]: 0x%02X follows 0x%02X", typ, rows[i].code, rows[i-1].code)
			}
		}
	}
	for typ := range 256 {
		rows := reasonTables[packet.Type(typ)]
		for code := range 256 {
			want := ""
			if i := slices.IndexFunc(rows, func(r reasonCode) bool { return r.code == byte(code) }); i >= 0 {
				want = rows[i].name
			}
			if got := packet.ReasonName(packet.Type(typ), byte(code)); got != want {
				t.Errorf("ReasonName(%d, 0x%02X) = %q, want %q", typ, code, got, want)
			}
		}
	}
}

// reasonNameSink keeps ReasonName's result in use, so the call cannot be
// dropped.
var reasonNameSink string

// TestReasonNameDoesNotAllocate: a reason code is looked up per packet, the
// busy path, which allocates nothing it can avoid (alloc_test.go).
func TestReasonNameDoesNotAllocate(t *testing.T) {
	testkit.NoAllocs(t, "ReasonName", func() { reasonNameSink = packet.ReasonName(packet.DISCONNECT, 0x97) })
}
