// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package packets

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// A string or binary property sent with no bytes is present, and "present"
// is a thing a receiver can see: [MQTT-3.3.2-15], [MQTT-3.3.2-16] and
// [MQTT-3.3.2-20] have the server send the Response Topic, Correlation Data
// and Content Type unaltered, and Binary Data and a UTF-8 Encoded String
// both allow a length of zero (MQTT 5 sections 1.5.4, 1.5.6). A decoder that
// reads one as present and an encoder that writes it as absent change the
// message in between.
//
// The oracle is the wire: the property block that came in is the one that
// goes out.
func TestAnEmptyPropertyIsStillThere(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   byte
		pkt  byte
	}{
		{"Content Type", PropContentType, Publish},
		{"Response Topic", PropResponseTopic, Publish},
		{"Correlation Data", PropCorrelationData, Publish},
		{"Reason String", PropReasonString, Puback},
		{"Assigned Client Identifier", PropAssignedClientID, Connack},
		{"Authentication Method", PropAuthenticationMethod, Connack},
		{"Authentication Data", PropAuthenticationData, Connack},
		{"Response Information", PropResponseInfo, Connack},
		{"Server Reference", PropServerReference, Disconnect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := []byte{3, tc.id, 0, 0}

			var p Properties
			_, err := p.Decode(tc.pkt, bytes.NewBuffer(block))
			require.NoError(t, err)

			for stage, props := range map[string]Properties{"decoded": p, "copied": p.Copy(false)} {
				out := new(bytes.Buffer)
				props.Encode(tc.pkt, Mods{AllowResponseInfo: true}, out, 0)
				require.Equal(t, block, out.Bytes(), "%s, then written", stage)

				var again Properties
				_, err = again.Decode(tc.pkt, bytes.NewBuffer(out.Bytes()))
				require.NoError(t, err)
				require.Equal(t, p, again, "%s, written and read again", stage)
			}

			// And a block with the property absent is not the same one.
			var none Properties
			_, err = none.Decode(tc.pkt, bytes.NewBuffer([]byte{0}))
			require.NoError(t, err)
			require.NotEqual(t, none, p)
			out := new(bytes.Buffer)
			none.Encode(tc.pkt, Mods{AllowResponseInfo: true}, out, 0)
			require.Equal(t, []byte{0}, out.Bytes())
		})
	}
}

// Section 3.14.2.1: "The Reason Code and Property Length can be omitted if
// the Reason Code is 0x00 (Success) and there are no Properties. In this case
// the DISCONNECT has a Remaining Length of 0." So a Remaining Length of 1 is
// a Reason Code and no Property Length, and 0x04 (Disconnect with Will
// Message) in it is what a client asked for. The acknowledgements are the
// same shape (3.4.2.1, 3.5.2.1, 3.6.2.1, 3.7.2.1), after the packet
// identifier.
func TestAReasonCodeWithNoPropertiesIsRead(t *testing.T) {
	for _, reason := range []byte{0x00, 0x04, 0x81, 0x8E} {
		pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Disconnect, Remaining: 1}}
		require.NoError(t, pk.DisconnectDecode([]byte{reason}))
		require.Equal(t, reason, pk.ReasonCode, "DISCONNECT, Remaining Length 1")
	}
	pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: Disconnect}}
	require.NoError(t, pk.DisconnectDecode(nil))
	require.Equal(t, byte(0), pk.ReasonCode, "Remaining Length 0 is 0x00")

	for _, typ := range []byte{Puback, Pubrec, Pubrel, Pubcomp} {
		for _, reason := range []byte{0x10, 0x80, 0x92} {
			pk := Packet{ProtocolVersion: 5, FixedHeader: FixedHeader{Type: typ, Remaining: 3}}
			require.NoError(t, pk.decodePubAckRelRecComp([]byte{0, 7, reason}))
			require.Equal(t, reason, pk.ReasonCode, "%s, Remaining Length 3", PacketNames[typ])
			require.Equal(t, uint16(7), pk.PacketID)
		}
	}
}
