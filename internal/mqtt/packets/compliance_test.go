// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package packets

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rules this engine enforces that mochi did not, each stated as the
// specification states it and checked at the point the broker decides.
//
// They are here rather than in packets_test.go because that file is
// mochi's, driven by the fixture table, and these are saguin's: a reader
// asking "what did we change about compliance" should find one file.

// A Response Topic naming a wildcard is a protocol error.
//
// [MQTT-3.3.2-14] forbids the wildcard. [MQTT-3.3.2-15] requires the
// Response Topic to reach every subscriber unaltered. Before this rule the
// broker did neither: it accepted the packet, answered 0x00, and left the
// property out when forwarding, so a request/response client lost its reply
// route with nothing reported anywhere.
func TestAWildcardResponseTopicIsRefused(t *testing.T) {
	for _, rt := range []string{"reply/#", "reply/+/here", "#", "+"} {
		t.Run(rt, func(t *testing.T) {
			pk := Packet{
				FixedHeader: FixedHeader{Type: Publish},
				TopicName:   "a/b/c",
				Properties:  Properties{ResponseTopic: rt},
			}
			require.Equal(t, ErrProtocolViolationWildcardResponseTopic,
				pk.PublishValidate(16))
		})
	}
}

// And one without a wildcard is accepted, so the rule refuses the packets it
// is about rather than every packet carrying the property.
func TestAnOrdinaryResponseTopicIsAccepted(t *testing.T) {
	pk := Packet{
		FixedHeader: FixedHeader{Type: Publish},
		TopicName:   "a/b/c",
		Properties:  Properties{ResponseTopic: "reply/to/me"},
	}
	require.Equal(t, CodeSuccess, pk.PublishValidate(16))
}

// A Will carries its own Response Topic, and the same rule applies to it.
//
// The Will becomes a PUBLISH when the session ends, which is the moment
// there is no client left to refuse. CONNECT is the only point at which
// anybody can be told.
func TestAWildcardResponseTopicInAWillIsRefused(t *testing.T) {
	pk := Packet{
		ProtocolVersion: 5,
		FixedHeader:     FixedHeader{Type: Connect},
		Connect: &ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "c",
			WillFlag:         true,
			WillTopic:        "will/topic",
			WillPayload:      []byte("bye"),
			WillProperties:   Properties{ResponseTopic: "reply/#"},
		},
	}
	require.Equal(t, ErrProtocolViolationWildcardResponseTopic, pk.ConnectValidate())

	pk.Connect.WillProperties.ResponseTopic = "reply/to/me"
	require.Equal(t, CodeSuccess, pk.ConnectValidate())
}

// A Variable Byte Integer must use the fewest bytes that can hold its value.
//
// [MQTT-1.5.5-1]. A longer encoding of the same number is a malformed packet
// rather than a second spelling of it, and DecodeLength reads every one the
// broker decodes: a packet's Remaining Length, a property length, and a
// Subscription Identifier. The conformance suite probes three of those and
// names them as three failures; they were one.
func TestANonMinimalVariableByteIntegerIsRefused(t *testing.T) {
	minimal := map[string][]byte{
		"zero":             {0x00},
		"one":              {0x01},
		"127, one byte":    {0x7F},
		"128, two bytes":   {0x80, 0x01},
		"16383, two bytes": {0xFF, 0x7F},
		"16384, three":     {0x80, 0x80, 0x01},
		"268435455, four":  {0xFF, 0xFF, 0xFF, 0x7F},
	}
	for name, b := range minimal {
		t.Run("accepts "+name, func(t *testing.T) {
			_, _, err := DecodeLength(bytes.NewBuffer(b))
			require.NoError(t, err)
		})
	}

	// Each of these encodes the same value as a shorter one above.
	nonMinimal := map[string][]byte{
		"zero in two bytes":   {0x80, 0x00},
		"one in two bytes":    {0x81, 0x00},
		"127 in two bytes":    {0xFF, 0x00},
		"zero in three bytes": {0x80, 0x80, 0x00},
		"128 in three bytes":  {0x80, 0x81, 0x00},
		"zero in four bytes":  {0x80, 0x80, 0x80, 0x00},
	}
	for name, b := range nonMinimal {
		t.Run("refuses "+name, func(t *testing.T) {
			_, _, err := DecodeLength(bytes.NewBuffer(b))
			require.ErrorIs(t, err, ErrMalformedVariableByteInteger)
		})
	}
}

// A CONNECT payload holds the fields its flags describe and nothing else.
//
// §3.1.3 fixes which fields are present and in what order; bytes after them
// are fields the client sent without declaring, which [MQTT-3.1.2-16] and
// [MQTT-3.1.2-18] make a protocol error for a user name and a password.
//
// **ConnectValidate carried both rules and could never reach them**: it asks
// whether a field is set while its flag is 0, and the decoder only reads a
// field when its flag is 1. The check is in the decoder now, over the whole
// payload, because a surplus user name and a surplus password are the same
// defect.
func TestAConnectCarryingUndeclaredPayloadIsRefused(t *testing.T) {
	// MQTT, v5, flags 0x02 (clean start, no username, no password),
	// keepalive 60, properties 0, client id "c".
	base := []byte{
		0, 4, 'M', 'Q', 'T', 'T',
		5,
		0x02,
		0, 60,
		0,
		0, 1, 'c',
	}

	t.Run("accepts a payload that stops where the flags say", func(t *testing.T) {
		pk := Packet{FixedHeader: FixedHeader{Type: Connect, Remaining: len(base)}}
		require.NoError(t, pk.ConnectDecode(base))
	})

	for name, extra := range map[string][]byte{
		"an undeclared user name": {0, 4, 'u', 's', 'e', 'r'},
		"an undeclared password":  {0, 4, 'p', 'a', 's', 's'},
		"a stray byte":            {0x00},
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			buf := append(append([]byte{}, base...), extra...)
			pk := Packet{FixedHeader: FixedHeader{Type: Connect, Remaining: len(buf)}}
			require.ErrorIs(t, pk.ConnectDecode(buf), ErrProtocolViolationSurplusPayload)
		})
	}
}

// The reserved bits of a SUBSCRIBE options byte must be zero.
//
// [MQTT-3.8.3-5] calls a packet setting them malformed. Subscription.decode
// reads bits 0 to 5 and dropped the rest, so a client setting them was
// answered SUBACK 0x00 and subscribed as though it had not.
func TestReservedSubscriptionOptionBitsAreRefused(t *testing.T) {
	// packet id 1, no properties, filter "a/b", then the options byte.
	body := func(option byte) []byte {
		return []byte{0, 1, 0, 0, 3, 'a', '/', 'b', option}
	}

	t.Run("accepts qos 1 with no reserved bits", func(t *testing.T) {
		pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Subscribe}}
		require.NoError(t, pk.SubscribeDecode(body(0x01)))
	})

	for name, option := range map[string]byte{
		"bit 6":     0x40,
		"bit 7":     0x80,
		"both bits": 0xC0,
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Subscribe}}
			require.ErrorIs(t, pk.SubscribeDecode(body(option)),
				ErrMalformedSurplusSubscriptionBits)
		})
	}

	// Retain Handling is two bits and 3 is not one of its values. The byte
	// is asserted rather than the constant, which would follow any byte.
	t.Run("refuses retain handling 3", func(t *testing.T) {
		pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Subscribe}}
		var c Code
		require.ErrorAs(t, pk.SubscribeDecode(body(0x30)), &c)
		require.Equal(t, byte(0x82), c.Code, "3.8.3.1 makes it a Protocol Error: %v", c)
	})

	// 3.1.1 has no subscription options and no property length: the byte
	// after the filter is the QoS and nothing else, so none of the above
	// applies to it. Its body is laid out differently for that reason.
	t.Run("leaves 3.1.1 alone", func(t *testing.T) {
		v311 := []byte{0, 1, 0, 3, 'a', '/', 'b', 0x01}
		pk := Packet{ProtocolVersion: 4, FixedHeader: FixedHeader{Type: Subscribe}}
		require.NoError(t, pk.SubscribeDecode(v311))
		require.Len(t, pk.Filters, 1)
		require.Equal(t, byte(1), pk.Filters[0].Qos)
	})
}

// Each refusal carries the code its condition is given. Section 1.2 keeps a
// Malformed Packet for one that "cannot be parsed" and a Protocol Error for
// one parsed and "found to contain data that is not allowed", and section
// 4.13.1 answers them 0x81 and 0x82. Where the specification labels the
// condition, that label is the oracle, and the byte is what is asserted:
// the fixture table compares constants, which follow any byte they carry.
//
// The last two cases were right already; they are here so the test is
// seen telling the two codes apart rather than expecting one of them.
func TestEachRefusalCarriesTheCodeItsConditionIsGiven(t *testing.T) {
	subscribe := func(option byte) error {
		pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Subscribe}}
		return pk.SubscribeDecode([]byte{0, 1, 0, 0, 3, 'a', '/', 'b', option})
	}
	connect := func(mod func(*Packet)) error {
		pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Connect},
			Connect: &ConnectParams{ProtocolName: []byte("MQTT"), Clean: true, ClientIdentifier: "c"}}
		mod(&pk)
		return pk.ConnectValidate()
	}
	for _, tc := range []struct {
		name, oracle string
		err          error
		want         byte
	}{
		{"retain handling 3", "3.8.3.1: a Protocol Error", subscribe(0x30), 0x82},
		{"a PUBLISH with both QoS bits set", "3.3.1.2: a Malformed Packet",
			new(FixedHeader).Decode(Publish<<4 | 0x06), 0x81},
		{"a CONNECT with its reserved flag set", "3.1.2.3: a Malformed Packet",
			connect(func(pk *Packet) { pk.ReservedBit = 1 }), 0x81},
		{"a Will QoS of 3", "3.1.2.6: a Malformed Packet", connect(func(pk *Packet) {
			pk.Connect.WillFlag, pk.Connect.WillQos = true, 3
			pk.Connect.WillTopic, pk.Connect.WillPayload = "w/t", []byte("bye")
		}), 0x81},
		{"a Topic Alias in a CONNECT", "2.2.2.2: a Malformed Packet", func() error {
			_, err := new(Properties).Decode(Connect, bytes.NewBuffer([]byte{3, 0x23, 0, 1}))
			return err
		}(), 0x81},
		{"reserved subscription option bits", "3.8.3-5: malformed", subscribe(0x40), 0x81},
		{"a subscription QoS of 3", "3.8.3.1: a Protocol Error", subscribe(0x03), 0x82},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c Code
			require.ErrorAs(t, tc.err, &c, "%s: refused with no code at all", tc.oracle)
			require.Equal(t, tc.want, c.Code, "%s: answered 0x%02X (%s)", tc.oracle, c.Code, c.Reason)
		})
	}
}
