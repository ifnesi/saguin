package metrics_test

import (
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
)

// metrics.md, Rules: "For an MQTT reason code, the *reason* label is its MQTT
// 5 reason name in lower case, with each space or hyphen replaced by an
// underscore. Examples are quota_exceeded and re_authenticate". Which codes a
// packet may carry, and their MQTT 5 names, are one table in internal/packet
// (packet.ReasonName), held there to each packet's own reason-code table;
// metrics keeps no list of its own and takes its labels from it. A label
// depends on the packet as well as the code: 0x00 has three names, and a
// packet may not carry a code at all.

// underscores turns each space or hyphen into an underscore.
var underscores = strings.NewReplacer(" ", "_", "-", "_")

// label is metrics.md's rule applied to an MQTT 5 reason name.
func label(name string) string {
	return strings.ToLower(underscores.Replace(name))
}

// TestReasonLabelFollowsPacketTable asks ReasonLabel for every code on every
// packet type, values beyond the four bits of a type included: it is the
// packet table's name under metrics.md's rule, and "" where that packet
// cannot carry the code.
func TestReasonLabelFollowsPacketTable(t *testing.T) {
	for typ := range 256 {
		for code := range 256 {
			name := packet.ReasonName(packet.Type(typ), byte(code))
			if got, want := metrics.ReasonLabel(packet.Type(typ), byte(code)), label(name); got != want {
				t.Errorf("ReasonLabel(%d, 0x%02X) = %q, want %q (from %q)", typ, code, got, want, name)
			}
		}
	}
}

// TestReasonLabelsDocumented holds the labels metrics.md spells out, each
// with the packet and code the design gives it, so the rule is checked
// against the document as well as against packet's table.
func TestReasonLabelsDocumented(t *testing.T) {
	for _, c := range []struct {
		typ   packet.Type
		code  byte
		label string
	}{
		// Rules: the examples. 0x19 is AUTH's alone (MQTT 5 Table 3-11).
		{packet.PUBACK, 0x97, "quota_exceeded"},
		{packet.AUTH, 0x19, "re_authenticate"},
		// saguin_connections_refused_total: the CONNACK reasons of design.md,
		// Flows, CONNECT, the refusal table.
		{packet.CONNACK, 0x84, "unsupported_protocol_version"},
		{packet.CONNACK, 0x85, "client_identifier_not_valid"},
		{packet.CONNACK, 0x86, "bad_user_name_or_password"},
		{packet.CONNACK, 0x87, "not_authorized"},
		{packet.CONNACK, 0x90, "topic_name_invalid"},
		{packet.CONNACK, 0x9A, "retain_not_supported"},
		{packet.CONNACK, 0x9B, "qos_not_supported"},
		{packet.CONNACK, 0x95, "packet_too_large"},
		{packet.CONNACK, 0x83, "implementation_specific_error"},
		{packet.CONNACK, 0x8C, "bad_authentication_method"},
		{packet.CONNACK, 0x97, "quota_exceeded"},
		// saguin_disconnects_total: design.md, Flows, DISCONNECT, "Reasons
		// Sagüin sends", and the MQTT 3.1.1 publish refusals (Flows,
		// PUBLISH), counted under their MQTT 5 names.
		{packet.DISCONNECT, 0x81, "malformed_packet"},
		{packet.DISCONNECT, 0x82, "protocol_error"},
		{packet.DISCONNECT, 0x8B, "server_shutting_down"},
		{packet.DISCONNECT, 0x8D, "keep_alive_timeout"},
		{packet.DISCONNECT, 0x8E, "session_taken_over"},
		{packet.DISCONNECT, 0x93, "receive_maximum_exceeded"},
		{packet.DISCONNECT, 0x94, "topic_alias_invalid"},
		{packet.DISCONNECT, 0x95, "packet_too_large"},
		{packet.DISCONNECT, 0x98, "administrative_action"},
		{packet.DISCONNECT, 0x9A, "retain_not_supported"},
		{packet.DISCONNECT, 0x9B, "qos_not_supported"},
		{packet.DISCONNECT, 0x9E, "shared_subscriptions_not_supported"},
		{packet.DISCONNECT, 0x87, "not_authorized"},
		{packet.DISCONNECT, 0x90, "topic_name_invalid"},
		{packet.DISCONNECT, 0x97, "quota_exceeded"},
		{packet.DISCONNECT, 0x83, "implementation_specific_error"},
		// design.md, Choices MQTT leaves open: "DISCONNECT never carries
		// 0x8C", though Table 2-6 lists it there.
		{packet.DISCONNECT, 0x8C, ""},
	} {
		if got := metrics.ReasonLabel(c.typ, c.code); got != c.label {
			t.Errorf("ReasonLabel(%d, 0x%02X) = %q, want %q", c.typ, c.code, got, c.label)
		}
	}
}
