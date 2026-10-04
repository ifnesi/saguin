// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	propertiesStruct = Properties{
		PayloadFormat:             byte(1), // UTF-8 Format
		PayloadFormatFlag:         true,
		MessageExpiryInterval:     uint32(2),
		ContentType:               "text/plain",
		ResponseTopic:             "a/b/c",
		CorrelationData:           []byte("data"),
		SubscriptionIdentifier:    []int{322122},
		SessionExpiryInterval:     uint32(120),
		SessionExpiryIntervalFlag: true,
		AssignedClientID:          "mochi-v5",
		ServerKeepAlive:           uint16(20),
		ServerKeepAliveFlag:       true,
		AuthenticationMethod:      "SHA-1",
		AuthenticationData:        []byte("auth-data"),
		RequestProblemInfo:        byte(1),
		RequestProblemInfoFlag:    true,
		WillDelayInterval:         uint32(600),
		RequestResponseInfo:       byte(1),
		ResponseInfo:              "response",
		ServerReference:           "mochi-2",
		ReasonString:              "reason",
		ReceiveMaximum:            uint16(500),
		ReceiveMaximumFlag:        true,
		TopicAliasMaximum:         uint16(999),
		TopicAlias:                uint16(3),
		TopicAliasFlag:            true,
		MaximumQos:                byte(1),
		MaximumQosFlag:            true,
		RetainAvailable:           byte(1),
		RetainAvailableFlag:       true,
		User: []UserProperty{
			{
				Key: "hello",
				Val: "世界",
			},
			{
				Key: "key2",
				Val: "value2",
			},
		},
		MaximumPacketSize:        uint32(32000),
		MaximumPacketSizeFlag:    true,
		WildcardSubAvailable:     byte(1),
		WildcardSubAvailableFlag: true,
		SubIDAvailable:           byte(1),
		SubIDAvailableFlag:       true,
		SharedSubAvailable:       byte(1),
		SharedSubAvailableFlag:   true,
	}

	propertiesBytes = []byte{
		172, 1, // VBI

		// Payload Format (1) (vbi:2)
		1, 1,

		// Message Expiry (2) (vbi:7)
		2, 0, 0, 0, 2,

		// Content Type (3) (vbi:20)
		3,
		0, 10, 't', 'e', 'x', 't', '/', 'p', 'l', 'a', 'i', 'n',

		// Response Topic (8) (vbi:28)
		8,
		0, 5, 'a', '/', 'b', '/', 'c',

		// Correlations Data (9) (vbi:35)
		9,
		0, 4, 'd', 'a', 't', 'a',

		// Subscription Identifier (11) (vbi:39)
		11,
		202, 212, 19,

		// Session Expiry Interval (17) (vbi:43)
		17,
		0, 0, 0, 120,

		// Assigned Client ID (18) (vbi:55)
		18,
		0, 8, 'm', 'o', 'c', 'h', 'i', '-', 'v', '5',

		// Server Keep Alive (19) (vbi:58)
		19,
		0, 20,

		// Authentication Method (21) (vbi:66)
		21,
		0, 5, 'S', 'H', 'A', '-', '1',

		// Authentication Data (22) (vbi:78)
		22,
		0, 9, 'a', 'u', 't', 'h', '-', 'd', 'a', 't', 'a',

		// Request Problem Info (23) (vbi:80)
		23, 1,

		// Will Delay Interval (24) (vbi:85)
		24,
		0, 0, 2, 88,

		// Request Response Info (25) (vbi:87)
		25, 1,

		// Response Info (26) (vbi:98)
		26,
		0, 8, 'r', 'e', 's', 'p', 'o', 'n', 's', 'e',

		// Server Reference (28) (vbi:108)
		28,
		0, 7, 'm', 'o', 'c', 'h', 'i', '-', '2',

		// Reason String (31) (vbi:117)
		31,
		0, 6, 'r', 'e', 'a', 's', 'o', 'n',

		// Receive Maximum (33) (vbi:120)
		33,
		1, 244,

		// Topic Alias Maximum (34) (vbi:123)
		34,
		3, 231,

		// Topic Alias (35) (vbi:126)
		35,
		0, 3,

		// Maximum Qos (36) (vbi:128)
		36, 1,

		// Retain Available (37) (vbi: 130)
		37, 1,

		// User Properties (38) (vbi:161)
		38,
		0, 5, 'h', 'e', 'l', 'l', 'o',
		0, 6, 228, 184, 150, 231, 149, 140,
		38,
		0, 4, 'k', 'e', 'y', '2',
		0, 6, 'v', 'a', 'l', 'u', 'e', '2',

		// Maximum Packet Size (39) (vbi:166)
		39,
		0, 0, 125, 0,

		// Wildcard Subscriptions Available (40)  (vbi:168)
		40, 1,

		// Subscription ID Available (41) (vbi:170)
		41, 1,

		// Shared Subscriptions Available (42) (vbi:172)
		42, 1,
	}
)

func init() {
	// The fixture below decodes under the reserved packet type, which
	// carries every property and may repeat all of them.
	for k := range validPacketProperties {
		if validPacketProperties[k].on != 0 {
			validPacketProperties[k].on |= pktBit(Reserved)
			validPacketProperties[k].repeat |= pktBit(Reserved)
		}
	}
	encodable = encodableFrom(&validPacketProperties)
}

// TestEncodableIsValidPacketProperties holds canEncode's table to the map it
// is built from, for every property and packet type a byte can name: a
// property is encoded on a packet exactly where validPacketProperties says it
// may go, and nowhere else.
func TestEncodableIsValidPacketProperties(t *testing.T) {
	var p Properties
	examined, want := 0, 0
	for k := range 256 {
		for pkt := range 256 {
			examined++
			v := k < len(validPacketProperties) && validPacketProperties[k].on&pktBit(byte(pkt)) != 0
			if v {
				want++
			}
			if got := p.canEncode(byte(pkt), byte(k)); got != v {
				t.Errorf("property %d on packet type %d: canEncode says %v, validPacketProperties %v",
					k, pkt, got, v)
			}
		}
	}
	require.Equal(t, 256*256, examined)
	// Every property is allowed somewhere, so an empty map cannot pass as
	// an empty table.
	require.GreaterOrEqual(t, want, 27)
}

func TestEncodeProperties(t *testing.T) {
	props := propertiesStruct
	b := bytes.NewBuffer([]byte{})
	props.Encode(Reserved, Mods{AllowResponseInfo: true}, b, 0)
	require.Equal(t, propertiesBytes, b.Bytes())
}

func TestEncodePropertiesDisallowProblemInfo(t *testing.T) {
	props := propertiesStruct
	b := bytes.NewBuffer([]byte{})
	props.Encode(Reserved, Mods{DisallowProblemInfo: true}, b, 0)
	require.NotEqual(t, propertiesBytes, b.Bytes())
	require.False(t, bytes.Contains(b.Bytes(), []byte{31, 0, 6}))
	require.False(t, bytes.Contains(b.Bytes(), []byte{38, 0, 5}))
	require.False(t, bytes.Contains(b.Bytes(), []byte{26, 0, 8}))
}

func TestEncodePropertiesDisallowResponseInfo(t *testing.T) {
	props := propertiesStruct
	b := bytes.NewBuffer([]byte{})
	props.Encode(Reserved, Mods{AllowResponseInfo: false}, b, 0)
	require.NotEqual(t, propertiesBytes, b.Bytes())
	require.NotContains(t, b.Bytes(), []byte{8, 0, 5})
	require.NotContains(t, b.Bytes(), []byte{9, 0, 4})
}

func TestEncodePropertiesNil(t *testing.T) {
	type tmp struct {
		p *Properties
	}

	pr := tmp{}
	b := bytes.NewBuffer([]byte{})
	pr.p.Encode(Reserved, Mods{}, b, 0)
	require.Equal(t, []byte{}, b.Bytes())
}

func TestEncodeZeroProperties(t *testing.T) {
	// [MQTT-2.2.2-1] If there are no properties, this MUST be indicated by including a Property Length of zero.
	props := new(Properties)
	b := bytes.NewBuffer([]byte{})
	props.Encode(Reserved, Mods{AllowResponseInfo: true}, b, 0)
	require.Equal(t, []byte{0x00}, b.Bytes())
}

func TestDecodeProperties(t *testing.T) {
	b := bytes.NewBuffer(propertiesBytes)

	props := new(Properties)
	n, err := props.Decode(Reserved, b)
	require.NoError(t, err)
	require.Equal(t, 172+2, n)
	require.EqualValues(t, propertiesStruct, *props)
}

func TestDecodePropertiesNil(t *testing.T) {
	b := bytes.NewBuffer(propertiesBytes)

	type tmp struct {
		p *Properties
	}

	pr := tmp{}
	n, err := pr.p.Decode(Reserved, b)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestDecodePropertiesBadInitialVBI(t *testing.T) {
	b := bytes.NewBuffer([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
	require.ErrorIs(t, ErrMalformedVariableByteInteger, err)
}

func TestDecodePropertiesZeroLengthVBI(t *testing.T) {
	b := bytes.NewBuffer([]byte{0})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.NoError(t, err)
	require.Equal(t, props, new(Properties))
}

func TestDecodePropertiesBadKeyByte(t *testing.T) {
	b := bytes.NewBuffer([]byte{64, 1})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMalformedOffsetByteOutOfRange)
}

func TestDecodePropertiesInvalidForPacket(t *testing.T) {
	b := bytes.NewBuffer([]byte{1, 99})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMalformedUnsupportedProperty)
}

func TestDecodePropertiesGeneralFailure(t *testing.T) {
	b := bytes.NewBuffer([]byte{10, 11, 202, 212, 19})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
}

func TestDecodePropertiesBadSubscriptionID(t *testing.T) {
	b := bytes.NewBuffer([]byte{10, 11, 255, 255, 255, 255, 255, 255, 255, 255})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
}

func TestDecodePropertiesBadUserProps(t *testing.T) {
	b := bytes.NewBuffer([]byte{10, 38, 255, 255, 255, 255, 255, 255, 255, 255})
	props := new(Properties)
	_, err := props.Decode(Reserved, b)
	require.Error(t, err)
}

func TestCopyProperties(t *testing.T) {
	require.EqualValues(t, propertiesStruct, propertiesStruct.Copy(true))
}

func TestCopyPropertiesNoTransfer(t *testing.T) {
	pkA := propertiesStruct
	pkB := pkA.Copy(false)

	// Properties which should never be transferred from one connection to another
	require.Equal(t, uint16(0), pkB.TopicAlias)
}

// **An optional property is left out only when the whole packet would not
// otherwise fit the client's Maximum Packet Size.** MQTT 5 names the Reason
// String and User Properties on the acknowledgements, DISCONNECT and AUTH as
// the ones a server "MUST NOT send ... if it would increase the size of the
// packet beyond the Maximum Packet Size" (MQTT-3.2.2-19, -20 and the same
// rule on each packet), and the size is the whole packet (§3.1.2.11.4).
//
// Tried at every maximum from one byte under the packet with neither
// property to one byte over the packet with both, so each boundary is
// crossed:
//
//   - whenever the packet without them fits, what is sent fits;
//   - whenever the packet with both fits, both are sent;
//   - whenever it fits with the Reason String, the Reason String is sent,
//     and likewise for User Properties alone.
//
// The sizes are the encoder's own output for the same packet with the
// properties set or not, so the oracle is the rule and not a byte count
// written here. CONNACK carries properties after both optional ones, which
// is where a check made partway through encoding undercounts.
func TestOptionalPropertiesAreDroppedOnlyToFitTheWholePacket(t *testing.T) {
	reason := "a reason string long enough to matter"
	user := []UserProperty{{Key: "why", Val: "a user property long enough to matter"}}

	for _, tc := range []struct {
		name   string
		packet func() *Packet
		encode func(*Packet, *bytes.Buffer) error
	}{
		{"CONNACK", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Connack},
				Properties: Properties{ServerReference: "srv", ReceiveMaximum: 10,
					MaximumPacketSize: 1 << 20, SharedSubAvailableFlag: true}}
		}, (*Packet).ConnackEncode},
		{"DISCONNECT", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Disconnect},
				ReasonCode: ErrUnspecifiedError.Code, Properties: Properties{ServerReference: "srv"}}
		}, (*Packet).DisconnectEncode},
		{"PUBACK", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Puback},
				PacketID: 7, ReasonCode: CodeNoMatchingSubscribers.Code}
		}, (*Packet).PubackEncode},
		{"PUBREC", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Pubrec},
				PacketID: 7, ReasonCode: CodeNoMatchingSubscribers.Code}
		}, (*Packet).PubrecEncode},
		{"SUBACK", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Suback},
				PacketID: 7, ReasonCodes: []byte{0x00, 0x01}}
		}, (*Packet).SubackEncode},
		{"UNSUBACK", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Unsuback},
				PacketID: 7, ReasonCodes: []byte{0x00}}
		}, (*Packet).UnsubackEncode},
		{"AUTH", func() *Packet {
			return &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Auth},
				ReasonCode: CodeContinueAuthentication.Code,
				Properties: Properties{AuthenticationMethod: "m"}}
		}, (*Packet).AuthEncode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encode := func(withReason, withUser bool, max int) []byte {
				t.Helper()
				pk := tc.packet()
				if withReason {
					pk.Properties.ReasonString = reason
				}
				if withUser {
					pk.Properties.User = user
				}
				pk.Mods.MaxSize = uint32(max)
				var b bytes.Buffer
				if err := tc.encode(pk, &b); err != nil {
					t.Fatalf("encode: %v", err)
				}
				return b.Bytes()
			}
			bare := len(encode(false, false, 0))
			reasonOnly := len(encode(true, false, 0))
			userOnly := len(encode(false, true, 0))
			both := len(encode(true, true, 0))

			for max := bare - 1; max <= both+1; max++ {
				got := encode(true, true, max)
				keptReason := bytes.Contains(got, []byte(reason))
				keptUser := bytes.Contains(got, []byte(user[0].Val))
				switch {
				case max >= bare && len(got) > max:
					t.Errorf("against a maximum of %d the packet is %d bytes, though it fits in %d "+
						"without the optional properties", max, len(got), bare)
				case max >= both && !(keptReason && keptUser):
					t.Errorf("against a maximum of %d a property was dropped, though the packet "+
						"with both is %d bytes", max, both)
				case max >= reasonOnly && !keptReason:
					t.Errorf("against a maximum of %d the Reason String was dropped, though the "+
						"packet with it is %d bytes", max, reasonOnly)
				// Only when the Reason String was not sent: with it sent, the
				// two together may not fit though each would alone.
				case max >= userOnly && !keptReason && !keptUser:
					t.Errorf("against a maximum of %d nothing optional was sent, though the "+
						"packet with the User Properties alone is %d bytes", max, userOnly)
				}
			}
		})
	}

	// **A PUBLISH is never trimmed.** Its User Properties are the
	// publisher's data, carried to the subscriber unaltered (MQTT-3.3.2-18),
	// so a PUBLISH too large for a client is discarded whole (MQTT-3.1.2-25)
	// rather than delivered without them.
	t.Run("PUBLISH keeps its user properties", func(t *testing.T) {
		// The maximum is the PUBLISH without them, which is as tight as a
		// trimming encoder could be asked to fit.
		bare := &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Publish},
			TopicName: "a/b", Payload: []byte("x")}
		var without bytes.Buffer
		if err := bare.PublishEncode(&without); err != nil {
			t.Fatalf("encode: %v", err)
		}
		pk := &Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Publish},
			TopicName: "a/b", Payload: []byte("x"), Properties: Properties{User: user}}
		pk.Mods.MaxSize = uint32(without.Len())
		var b bytes.Buffer
		if err := pk.PublishEncode(&b); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if !bytes.Contains(b.Bytes(), []byte(user[0].Val)) {
			t.Error("a PUBLISH lost its User Properties to fit a client's maximum: the " +
				"subscriber would receive a message the publisher never sent")
		}
	})
}
