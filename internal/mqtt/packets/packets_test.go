// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"
)

const pkInfo = "packet type %v, %s"

var packetList = []byte{
	Connect,
	Connack,
	Publish,
	Puback,
	Pubrec,
	Pubrel,
	Pubcomp,
	Subscribe,
	Suback,
	Unsubscribe,
	Unsuback,
	Pingreq,
	Pingresp,
	Disconnect,
	Auth,
}

var pkTable = []TPacketCase{
	TPacketData[Connect].Get(TConnectMqtt311),
	TPacketData[Connect].Get(TConnectMqtt5),
	TPacketData[Connect].Get(TConnectUserPassLWT),
	TPacketData[Connack].Get(TConnackAcceptedMqtt5),
	TPacketData[Connack].Get(TConnackAcceptedNoSession),
	TPacketData[Publish].Get(TPublishBasic),
	TPacketData[Publish].Get(TPublishMqtt5),
	TPacketData[Puback].Get(TPuback),
	TPacketData[Pubrec].Get(TPubrec),
	TPacketData[Pubrel].Get(TPubrel),
	TPacketData[Pubcomp].Get(TPubcomp),
	TPacketData[Subscribe].Get(TSubscribe),
	TPacketData[Subscribe].Get(TSubscribeMqtt5),
	TPacketData[Suback].Get(TSuback),
	TPacketData[Unsubscribe].Get(TUnsubscribe),
	TPacketData[Unsubscribe].Get(TUnsubscribeMqtt5),
	TPacketData[Pingreq].Get(TPingreq),
	TPacketData[Pingresp].Get(TPingresp),
	TPacketData[Disconnect].Get(TDisconnect),
	TPacketData[Disconnect].Get(TDisconnectMqtt5),
}

func TestNewPackets(t *testing.T) {
	s := NewPackets()
	require.NotNil(t, s.internal)
}

func TestPacketsAdd(t *testing.T) {
	s := NewPackets()
	s.Add("cl1", Packet{})
	require.Contains(t, s.internal, "cl1")
}

func TestPacketsGet(t *testing.T) {
	s := NewPackets()
	s.Add("cl1", Packet{TopicName: "a1"})
	s.Add("cl2", Packet{TopicName: "a2"})
	require.Contains(t, s.internal, "cl1")
	require.Contains(t, s.internal, "cl2")

	pk, ok := s.Get("cl1")
	require.True(t, ok)
	require.Equal(t, "a1", pk.TopicName)
}

func TestPacketsGetAll(t *testing.T) {
	s := NewPackets()
	s.Add("cl1", Packet{TopicName: "a1"})
	s.Add("cl2", Packet{TopicName: "a2"})
	s.Add("cl3", Packet{TopicName: "a3"})
	require.Contains(t, s.internal, "cl1")
	require.Contains(t, s.internal, "cl2")
	require.Contains(t, s.internal, "cl3")

	subs := s.GetAll()
	require.Len(t, subs, 3)
}

func TestPacketsLen(t *testing.T) {
	s := NewPackets()
	s.Add("cl1", Packet{TopicName: "a1"})
	s.Add("cl2", Packet{TopicName: "a2"})
	require.Contains(t, s.internal, "cl1")
	require.Contains(t, s.internal, "cl2")
	require.Equal(t, 2, s.Len())
}

func TestSPacketsDelete(t *testing.T) {
	s := NewPackets()
	s.Add("cl1", Packet{TopicName: "a1"})
	require.Contains(t, s.internal, "cl1")

	s.Delete("cl1")
	_, ok := s.Get("cl1")
	require.False(t, ok)
}

func TestFormatPacketID(t *testing.T) {
	for _, id := range []uint16{0, 7, 0x100, 0xffff} {
		packet := &Packet{PacketID: id}
		require.Equal(t, fmt.Sprint(id), packet.FormatID())
	}
}

func TestSubscriptionOptionsEncodeDecode(t *testing.T) {
	p := &Subscription{
		Qos:               2,
		NoLocal:           true,
		RetainAsPublished: true,
		RetainHandling:    2,
	}
	x := new(Subscription)
	x.decode(p.encode())
	require.Equal(t, *p, *x)

	p = &Subscription{
		Qos:               1,
		NoLocal:           false,
		RetainAsPublished: false,
		RetainHandling:    1,
	}
	x = new(Subscription)
	x.decode(p.encode())
	require.Equal(t, *p, *x)
}

func TestPacketEncode(t *testing.T) {
	for _, pkt := range packetList {
		require.Contains(t, TPacketData, pkt)
		for _, wanted := range TPacketData[pkt] {
			t.Run(fmt.Sprintf("%s %s", PacketNames[pkt], wanted.Desc), func(t *testing.T) {
				if !encodeTestOK(wanted) {
					return
				}

				pk := new(Packet)
				_ = copier.Copy(pk, wanted.Packet)
				require.Equal(t, pkt, pk.FixedHeader.Type, pkInfo, pkt, wanted.Desc)

				pk.Mods.AllowResponseInfo = true

				buf := new(bytes.Buffer)
				var err error
				switch pkt {
				case Connect:
					err = pk.ConnectEncode(buf)
				case Connack:
					err = pk.ConnackEncode(buf)
				case Publish:
					err = pk.PublishEncode(buf)
				case Puback:
					err = pk.PubackEncode(buf)
				case Pubrec:
					err = pk.PubrecEncode(buf)
				case Pubrel:
					err = pk.PubrelEncode(buf)
				case Pubcomp:
					err = pk.PubcompEncode(buf)
				case Subscribe:
					err = pk.SubscribeEncode(buf)
				case Suback:
					err = pk.SubackEncode(buf)
				case Unsubscribe:
					err = pk.UnsubscribeEncode(buf)
				case Unsuback:
					err = pk.UnsubackEncode(buf)
				case Pingreq:
					err = pk.PingreqEncode(buf)
				case Pingresp:
					err = pk.PingrespEncode(buf)
				case Disconnect:
					err = pk.DisconnectEncode(buf)
				case Auth:
					err = pk.AuthEncode(buf)
				}
				if wanted.Expect != nil {
					require.Error(t, err, pkInfo, pkt, wanted.Desc)
					return
				}

				require.NoError(t, err, pkInfo, pkt, wanted.Desc)
				encoded := buf.Bytes()

				// If ActualBytes is set, compare mutated version of byte string instead (to avoid length mismatches, etc).
				if len(wanted.ActualBytes) > 0 {
					wanted.RawBytes = wanted.ActualBytes
				}
				require.EqualValues(t, wanted.RawBytes, encoded, pkInfo, pkt, wanted.Desc)
			})
		}
	}
}

func TestPacketDecode(t *testing.T) {
	for _, pkt := range packetList {
		require.Contains(t, TPacketData, pkt)
		for _, wanted := range TPacketData[pkt] {
			t.Run(fmt.Sprintf("%s %s", PacketNames[pkt], wanted.Desc), func(t *testing.T) {
				if !decodeTestOK(wanted) {
					return
				}

				pk := &Packet{FixedHeader: FixedHeader{Type: pkt}}
				pk.Mods.AllowResponseInfo = true
				_ = pk.FixedHeader.Decode(wanted.RawBytes[0])
				if len(wanted.RawBytes) > 0 {
					pk.FixedHeader.Remaining = int(wanted.RawBytes[1])
				}

				if wanted.Packet != nil && wanted.Packet.ProtocolVersion != 0 {
					pk.ProtocolVersion = wanted.Packet.ProtocolVersion
				}

				buf := wanted.RawBytes[2:]
				var err error
				switch pkt {
				case Connect:
					err = pk.ConnectDecode(buf)
				case Connack:
					err = pk.ConnackDecode(buf)
				case Publish:
					err = pk.PublishDecode(buf)
				case Puback:
					err = pk.PubackDecode(buf)
				case Pubrec:
					err = pk.PubrecDecode(buf)
				case Pubrel:
					err = pk.PubrelDecode(buf)
				case Pubcomp:
					err = pk.PubcompDecode(buf)
				case Subscribe:
					err = pk.SubscribeDecode(buf)
				case Suback:
					err = pk.SubackDecode(buf)
				case Unsubscribe:
					err = pk.UnsubscribeDecode(buf)
				case Unsuback:
					err = pk.UnsubackDecode(buf)
				case Pingreq:
					err = pk.PingreqDecode(buf)
				case Pingresp:
					err = pk.PingrespDecode(buf)
				case Disconnect:
					err = pk.DisconnectDecode(buf)
				case Auth:
					err = pk.AuthDecode(buf)
				}

				if wanted.FailFirst != nil {
					require.Error(t, err, pkInfo, pkt, wanted.Desc)
					require.ErrorIs(t, err, wanted.FailFirst, pkInfo, pkt, wanted.Desc)
					return
				}

				require.NoError(t, err, pkInfo, pkt, wanted.Desc)

				require.EqualValues(t, wanted.Packet.Filters, pk.Filters, pkInfo, pkt, wanted.Desc)

				require.Equal(t, wanted.Packet.FixedHeader.Type, pk.FixedHeader.Type, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.FixedHeader.Dup, pk.FixedHeader.Dup, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.FixedHeader.Qos, pk.FixedHeader.Qos, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.FixedHeader.Retain, pk.FixedHeader.Retain, pkInfo, pkt, wanted.Desc)

				if pkt == Connect {
					// we use ProtocolVersion for controlling packet encoding, but we don't need to test
					// against it unless it's a connect packet.
					require.Equal(t, wanted.Packet.ProtocolVersion, pk.ProtocolVersion, pkInfo, pkt, wanted.Desc)
				}
				// A CONNECT's own fields are made by decoding a CONNECT and by
				// nothing else, so every other packet carries none.
				require.Equal(t, pkt == Connect, pk.Connect != nil, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).ProtocolName, cp(pk.Connect).ProtocolName, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).Clean, cp(pk.Connect).Clean, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).ClientIdentifier, cp(pk.Connect).ClientIdentifier, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).Keepalive, cp(pk.Connect).Keepalive, pkInfo, pkt, wanted.Desc)

				require.Equal(t, cp(wanted.Packet.Connect).UsernameFlag, cp(pk.Connect).UsernameFlag, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).Username, cp(pk.Connect).Username, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).PasswordFlag, cp(pk.Connect).PasswordFlag, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).Password, cp(pk.Connect).Password, pkInfo, pkt, wanted.Desc)

				require.Equal(t, cp(wanted.Packet.Connect).WillFlag, cp(pk.Connect).WillFlag, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).WillTopic, cp(pk.Connect).WillTopic, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).WillPayload, cp(pk.Connect).WillPayload, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).WillQos, cp(pk.Connect).WillQos, pkInfo, pkt, wanted.Desc)
				require.Equal(t, cp(wanted.Packet.Connect).WillRetain, cp(pk.Connect).WillRetain, pkInfo, pkt, wanted.Desc)

				require.Equal(t, wanted.Packet.ReasonCodes, pk.ReasonCodes, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.ReasonCode, pk.ReasonCode, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.SessionPresent, pk.SessionPresent, pkInfo, pkt, wanted.Desc)
				require.Equal(t, wanted.Packet.PacketID, pk.PacketID, pkInfo, pkt, wanted.Desc)

				require.EqualValues(t, wanted.Packet.Properties, pk.Properties)
				require.EqualValues(t, cp(wanted.Packet.Connect).WillProperties, cp(pk.Connect).WillProperties)
			})
		}
	}
}

func TestValidate(t *testing.T) {
	for _, pkt := range packetList {
		require.Contains(t, TPacketData, pkt)
		for _, wanted := range TPacketData[pkt] {
			t.Run(fmt.Sprintf("%s %s", PacketNames[pkt], wanted.Desc), func(t *testing.T) {
				if wanted.Group == "validate" || wanted.Primary {
					pk := wanted.Packet
					var err error
					switch pkt {
					case Connect:
						err = pk.ConnectValidate()
					case Publish:
						err = pk.PublishValidate(1024)
					case Subscribe:
						err = pk.SubscribeValidate()
					case Unsubscribe:
						err = pk.UnsubscribeValidate()
					case Auth:
						err = pk.AuthValidate()
					}

					if wanted.Expect != nil {
						require.Error(t, err, pkInfo, pkt, wanted.Desc)
						require.ErrorIs(t, wanted.Expect, err, pkInfo, pkt, wanted.Desc)
					}
				}
			})
		}
	}
}

func TestAckValidatePubrec(t *testing.T) {
	for _, b := range []byte{
		CodeSuccess.Code,
		CodeNoMatchingSubscribers.Code,
		ErrUnspecifiedError.Code,
		ErrImplementationSpecificError.Code,
		ErrNotAuthorized.Code,
		ErrTopicNameInvalid.Code,
		ErrPacketIdentifierInUse.Code,
		ErrQuotaExceeded.Code,
		ErrPayloadFormatInvalid.Code,
	} {
		pk := Packet{FixedHeader: FixedHeader{Type: Pubrec}, ReasonCode: b}
		require.True(t, pk.ReasonCodeValid())
	}
	pk := Packet{FixedHeader: FixedHeader{Type: Pubrec}, ReasonCode: ErrClientIdentifierTooLong.Code}
	require.False(t, pk.ReasonCodeValid())
}

func TestAckValidatePubrel(t *testing.T) {
	for _, b := range []byte{
		CodeSuccess.Code,
		ErrPacketIdentifierNotFound.Code,
	} {
		pk := Packet{FixedHeader: FixedHeader{Type: Pubrel}, ReasonCode: b}
		require.True(t, pk.ReasonCodeValid())
	}
	pk := Packet{FixedHeader: FixedHeader{Type: Pubrel}, ReasonCode: ErrClientIdentifierTooLong.Code}
	require.False(t, pk.ReasonCodeValid())
}

func TestAckValidatePubcomp(t *testing.T) {
	for _, b := range []byte{
		CodeSuccess.Code,
		ErrPacketIdentifierNotFound.Code,
	} {
		pk := Packet{FixedHeader: FixedHeader{Type: Pubcomp}, ReasonCode: b}
		require.True(t, pk.ReasonCodeValid())
	}
	pk := Packet{FixedHeader: FixedHeader{Type: Pubrel}, ReasonCode: ErrClientIdentifierTooLong.Code}
	require.False(t, pk.ReasonCodeValid())
}

func TestAckValidateSuback(t *testing.T) {
	for _, b := range []byte{
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
	} {
		pk := Packet{FixedHeader: FixedHeader{Type: Suback}, ReasonCode: b}
		require.True(t, pk.ReasonCodeValid())
	}

	pk := Packet{FixedHeader: FixedHeader{Type: Suback}, ReasonCode: ErrClientIdentifierTooLong.Code}
	require.False(t, pk.ReasonCodeValid())
}

func TestAckValidateUnsuback(t *testing.T) {
	for _, b := range []byte{
		CodeSuccess.Code,
		CodeNoSubscriptionExisted.Code,
		ErrUnspecifiedError.Code,
		ErrImplementationSpecificError.Code,
		ErrNotAuthorized.Code,
		ErrTopicFilterInvalid.Code,
		ErrPacketIdentifierInUse.Code,
	} {
		pk := Packet{FixedHeader: FixedHeader{Type: Unsuback}, ReasonCode: b}
		require.True(t, pk.ReasonCodeValid())
	}

	pk := Packet{FixedHeader: FixedHeader{Type: Unsuback}, ReasonCode: ErrClientIdentifierTooLong.Code}
	require.False(t, pk.ReasonCodeValid())
}

func TestReasonCodeValidMisc(t *testing.T) {
	pk := Packet{FixedHeader: FixedHeader{Type: Connack}, ReasonCode: CodeSuccess.Code}
	require.True(t, pk.ReasonCodeValid())
}

func TestCopy(t *testing.T) {
	for _, tt := range pkTable {
		pkc := tt.Packet.Copy(true)

		require.Equal(t, tt.Packet.FixedHeader.Qos, pkc.FixedHeader.Qos, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, false, pkc.FixedHeader.Dup, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, false, pkc.FixedHeader.Retain, pkInfo, tt.Case, tt.Desc)

		require.Equal(t, tt.Packet.TopicName, pkc.TopicName, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).ClientIdentifier, cp(pkc.Connect).ClientIdentifier, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).Keepalive, cp(pkc.Connect).Keepalive, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.ProtocolVersion, pkc.ProtocolVersion, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).PasswordFlag, cp(pkc.Connect).PasswordFlag, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).UsernameFlag, cp(pkc.Connect).UsernameFlag, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillQos, cp(pkc.Connect).WillQos, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillTopic, cp(pkc.Connect).WillTopic, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillFlag, cp(pkc.Connect).WillFlag, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillRetain, cp(pkc.Connect).WillRetain, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillProperties, cp(pkc.Connect).WillProperties, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.Properties, pkc.Properties, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).Clean, cp(pkc.Connect).Clean, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.SessionPresent, pkc.SessionPresent, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.ReasonCode, pkc.ReasonCode, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.PacketID, pkc.PacketID, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.Filters, pkc.Filters, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.Payload, pkc.Payload, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).Password, cp(pkc.Connect).Password, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).Username, cp(pkc.Connect).Username, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).ProtocolName, cp(pkc.Connect).ProtocolName, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, cp(tt.Packet.Connect).WillPayload, cp(pkc.Connect).WillPayload, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.ReasonCodes, pkc.ReasonCodes, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.Created, pkc.Created, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, tt.Packet.Origin, pkc.Origin, pkInfo, tt.Case, tt.Desc)
		require.EqualValues(t, pkc.Properties, tt.Packet.Properties)

		// A copy has CONNECT fields where its packet has them, and its own.
		require.Equal(t, tt.Packet.Connect == nil, pkc.Connect == nil, pkInfo, tt.Case, tt.Desc)
		if pkc.Connect != nil {
			require.NotSame(t, tt.Packet.Connect, pkc.Connect, pkInfo, tt.Case, tt.Desc)
		}

		pkcc := tt.Packet.Copy(false)
		require.Equal(t, uint16(0), pkcc.PacketID, pkInfo, tt.Case, tt.Desc)
	}
}

// TestCopySharingPayload holds the one difference CopySharingPayload has from
// Copy(false): the payload is the publish's own, not a duplicate. Everything
// else must come out exactly as Copy makes it, because every delivery
// changes those fields for its own subscriber.
func TestCopySharingPayload(t *testing.T) {
	shared := 0
	for _, tt := range pkTable {
		before := append([]byte(nil), tt.Packet.Payload...)
		got := tt.Packet.CopySharingPayload()
		want := tt.Packet.Copy(false)
		require.Equal(t, want, got, pkInfo, tt.Case, tt.Desc)
		require.Equal(t, before, tt.Packet.Payload, "the source packet was changed: "+pkInfo, tt.Case, tt.Desc)
		if len(tt.Packet.Payload) == 0 {
			continue
		}
		shared++
		require.Same(t, &tt.Packet.Payload[0], &got.Payload[0], "payload was duplicated: "+pkInfo, tt.Case, tt.Desc)
		// The control: Copy does duplicate, so the check above can tell the two apart.
		require.NotSame(t, &tt.Packet.Payload[0], &want.Payload[0], "Copy shared its payload: "+pkInfo, tt.Case, tt.Desc)
	}
	require.Greater(t, shared, 0, "no packet in the table has a payload, so sharing was never checked")
	t.Logf("%d packets with a payload checked shared", shared)
}

func TestMergeSubscription(t *testing.T) {
	sub := Subscription{
		Filter:            "a/b/c",
		RetainHandling:    0,
		Qos:               0,
		RetainAsPublished: false,
		NoLocal:           false,
		Identifier:        1,
	}

	sub2 := Subscription{
		Filter:            "a/b/d",
		RetainHandling:    0,
		Qos:               2,
		RetainAsPublished: false,
		NoLocal:           true,
		Identifier:        2,
	}

	expect := Subscription{
		Filter:            "a/b/c",
		RetainHandling:    0,
		Qos:               2,
		RetainAsPublished: false,
		NoLocal:           true,
		Identifier:        1,
		Identifiers: map[string]int{
			"a/b/c": 1,
			"a/b/d": 2,
		},
	}
	require.Equal(t, expect, sub.Merge(sub2))
}

// cp is a packet's CONNECT fields, or none where it has none, for comparing
// packets field by field.
func cp(c *ConnectParams) *ConnectParams {
	if c == nil {
		return new(ConnectParams)
	}
	return c
}

// A copy of a CONNECT has CONNECT fields of its own: they sit behind a
// pointer (ConnectParams), so a copy that shared them would have every
// change to one reach the other. Every field of the copy, and every byte it
// holds, is changed, and the original must be as it was.
func TestACopyOfACONNECTIsItsOwn(t *testing.T) {
	pk := Packet{FixedHeader: FixedHeader{Type: Connect}, ProtocolVersion: 5, Connect: &ConnectParams{
		ProtocolName: []byte("MQTT"), ClientIdentifier: "id", Keepalive: 30, Clean: true,
		Username: []byte("user"), UsernameFlag: true, Password: []byte("pass"), PasswordFlag: true,
		WillFlag: true, WillQos: 1, WillRetain: true, WillTopic: "w/t", WillPayload: []byte("bye"),
		WillProperties: Properties{ResponseTopic: "r", User: []UserProperty{{Key: "k", Val: "v"}}},
	}}
	want := *pk.Connect
	want.ProtocolName, want.Username = []byte("MQTT"), []byte("user")
	want.Password, want.WillPayload = []byte("pass"), []byte("bye")
	want.WillProperties.User = []UserProperty{{Key: "k", Val: "v"}}

	for _, transfer := range []bool{true, false} {
		c := pk.Copy(transfer)
		require.NotNil(t, c.Connect)
		require.NotSame(t, pk.Connect, c.Connect)
		c.Connect.ClientIdentifier, c.Connect.Keepalive, c.Connect.Clean = "other", 1, false
		c.Connect.WillTopic, c.Connect.WillQos, c.Connect.WillRetain = "x", 2, false
		c.Connect.WillProperties.ResponseTopic = "x"
		for _, b := range [][]byte{c.Connect.ProtocolName, c.Connect.Username, c.Connect.Password, c.Connect.WillPayload} {
			for i := range b {
				b[i] = '!'
			}
		}
		if len(c.Connect.WillProperties.User) > 0 {
			c.Connect.WillProperties.User[0].Key = "x"
		}
		require.Equal(t, want, *pk.Connect, "changing a copy changed the CONNECT it was copied from")
	}
}
