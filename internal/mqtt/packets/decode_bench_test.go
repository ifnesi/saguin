// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package packets

import (
	"bytes"
	"testing"
)

// Decoding is what every packet a client sends goes through before any other
// work, so what the property rules cost per packet is measured here rather
// than argued. The bodies are built by the encoders from packets a client
// really sends, so the benchmark decodes what the broker decodes.

// benchBody encodes pk and returns the bytes after the fixed header.
func benchBody(b *testing.B, pk Packet, enc func(*Packet, *bytes.Buffer) error) []byte {
	b.Helper()
	buf := new(bytes.Buffer)
	if err := enc(&pk, buf); err != nil {
		b.Fatal(err)
	}
	_, bu, err := DecodeLength(bytes.NewBuffer(buf.Bytes()[1:]))
	if err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()[1+bu:]
}

func BenchmarkDecode(b *testing.B) {
	pub := func(v byte) []byte {
		pk := Packet{
			ProtocolVersion: v,
			FixedHeader:     FixedHeader{Type: Publish, Qos: 1},
			TopicName:       "devices/alpha/telemetry",
			PacketID:        7,
			Payload:         bytes.Repeat([]byte{'x'}, 256),
		}
		if v == 5 {
			pk.Properties = Properties{
				PayloadFormat: 1, PayloadFormatFlag: true,
				MessageExpiryInterval: 60,
				ContentType:           "application/json",
				ResponseTopic:         "replies/alpha",
				CorrelationData:       []byte("corr-1"),
				User:                  []UserProperty{{Key: "a", Val: "b"}, {Key: "c", Val: "d"}},
			}
		}
		pk.Mods.AllowResponseInfo = true
		return benchBody(b, pk, (*Packet).PublishEncode)
	}
	sub := func(v byte) []byte {
		pk := Packet{
			ProtocolVersion: v,
			FixedHeader:     FixedHeader{Type: Subscribe, Qos: 1},
			PacketID:        9,
			Filters:         Subscriptions{{Filter: "devices/+/telemetry", Qos: 1}, {Filter: "alerts/#", Qos: 2}},
		}
		if v == 5 {
			pk.Properties = Properties{SubscriptionIdentifier: []int{42}, User: []UserProperty{{Key: "a", Val: "b"}}}
		}
		return benchBody(b, pk, (*Packet).SubscribeEncode)
	}
	ack := func(v byte) []byte {
		pk := Packet{ProtocolVersion: v, FixedHeader: FixedHeader{Type: Puback}, PacketID: 7}
		if v == 5 {
			pk.ReasonCode = 0x10
			pk.Properties = Properties{ReasonString: "no matching subscribers"}
		}
		return benchBody(b, pk, (*Packet).PubackEncode)
	}
	// A CONNECT is written by hand: the encoder is a stub in this engine.
	conn := func(v byte) []byte {
		var c []byte
		if v == 5 {
			c = []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0xCE, 0, 60}
			c = append(c, 0x11, 0, 0, 0, 30, 0x21, 0, 10, 0x27, 0, 0, 0x10, 0, 0x26, 0, 1, 'a', 0, 1, 'b')
			c = append(c[:10], append([]byte{byte(len(c) - 10)}, c[10:]...)...)
		} else {
			c = []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xCE, 0, 60}
		}
		c = append(c, 0, 5, 'c', 'l', 'i', 'e', 'n')
		if v == 5 {
			c = append(c, 0) // will properties
		}
		c = append(c, 0, 3, 'w', '/', 't', 0, 2, 'h', 'i', 0, 4, 'u', 's', 'e', 'r', 0, 4, 'p', 'a', 's', 's')
		return c
	}

	cases := []struct {
		name string
		ver  byte
		typ  byte
		qos  byte
		body []byte
		dec  func(*Packet, []byte) error
	}{
		{"publish/v5", 5, Publish, 1, pub(5), (*Packet).PublishDecode},
		{"publish/v4", 4, Publish, 1, pub(4), (*Packet).PublishDecode},
		{"subscribe/v5", 5, Subscribe, 1, sub(5), (*Packet).SubscribeDecode},
		{"subscribe/v4", 4, Subscribe, 1, sub(4), (*Packet).SubscribeDecode},
		{"puback/v5", 5, Puback, 0, ack(5), (*Packet).PubackDecode},
		{"connect/v5", 5, Connect, 0, conn(5), (*Packet).ConnectDecode},
		{"connect/v4", 4, Connect, 0, conn(4), (*Packet).ConnectDecode},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			var pk Packet
			pk.ProtocolVersion = c.ver
			pk.FixedHeader = FixedHeader{Type: c.typ, Qos: c.qos, Remaining: len(c.body)}
			if err := c.dec(&pk, c.body); err != nil {
				b.Fatalf("the benchmark body does not decode, so it would measure a refusal: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var pk Packet
				pk.ProtocolVersion = c.ver
				pk.FixedHeader = FixedHeader{Type: c.typ, Qos: c.qos, Remaining: len(c.body)}
				if err := c.dec(&pk, c.body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
