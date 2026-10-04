// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package packets

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

// The decoder is the first code in saguin to touch bytes from anybody who
// can open a socket, and until the engine moved into this repository there
// was nowhere to put a fuzz target for it: saguin had ten, all of them over
// its own topic rules and stores, and none over the thing that parses the
// wire.
//
// **Five of the eight known compliance defects are this code accepting
// something malformed**, and all five were found by a conformance suite,
// which tests the rules somebody wrote down. These ask the questions
// nobody wrote down.

// saguinTopicAliasMaximum is the cap saguin configures on the engine, kept
// here so PublishValidate is called with the number the broker uses rather
// than a number this file invented.
const saguinTopicAliasMaximum = 16

// decodeWire runs the bytes a client would send through the same steps the
// server does: the header byte, the remaining length, then the body handed
// to the decoder for that type.
//
// It is a copy of Client.ReadPacket's dispatch rather than a call to it,
// because that method needs a connection, a client and a server, and what
// is under test here is the decoding and nothing else.
func decodeWire(data []byte, version byte) (Packet, error) {
	r := bufio.NewReader(bytes.NewReader(data))

	hb, err := r.ReadByte()
	if err != nil {
		return Packet{}, err
	}

	var pk Packet
	pk.ProtocolVersion = version
	if err := pk.FixedHeader.Decode(hb); err != nil {
		return pk, err
	}

	n, _, err := DecodeLength(r)
	if err != nil {
		return pk, err
	}
	pk.FixedHeader.Remaining = n

	// The server reads exactly Remaining bytes with io.ReadFull and fails
	// the connection if they are not there, so a body shorter than the
	// header claims never reaches a decoder. Same here.
	body := make([]byte, n)
	if _, err := readFullFrom(r, body); err != nil {
		return pk, err
	}

	switch pk.FixedHeader.Type {
	case Connect:
		err = pk.ConnectDecode(body)
	case Connack:
		err = pk.ConnackDecode(body)
	case Publish:
		err = pk.PublishDecode(body)
	case Puback:
		err = pk.PubackDecode(body)
	case Pubrec:
		err = pk.PubrecDecode(body)
	case Pubrel:
		err = pk.PubrelDecode(body)
	case Pubcomp:
		err = pk.PubcompDecode(body)
	case Subscribe:
		err = pk.SubscribeDecode(body)
	case Suback:
		err = pk.SubackDecode(body)
	case Unsubscribe:
		err = pk.UnsubscribeDecode(body)
	case Unsuback:
		err = pk.UnsubackDecode(body)
	case Pingreq:
		err = pk.PingreqDecode(body)
	case Pingresp:
		err = pk.PingrespDecode(body)
	case Disconnect:
		err = pk.DisconnectDecode(body)
	case Auth:
		err = pk.AuthDecode(body)
	default:
		return pk, ErrProtocolViolation
	}
	if err != nil {
		return pk, err
	}

	// **The validators, because the server runs them and a harness that
	// does not is measuring something the broker never does.** Decoding and
	// validating are two steps in this package, and several rules the
	// specification puts on a received packet are enforced in the second:
	// the CONNECT reserved bit is [MQTT-3.1.2-3] and it is checked in
	// ConnectValidate, not in ConnectDecode.
	//
	// Leaving them out made the first run of the round-trip target fail on
	// its own seed corpus, against a CONNECT with that bit set - a packet
	// the broker refuses and this file had accepted. The finding was the
	// harness.
	//
	// PublishValidate takes the server's configured topic alias maximum
	// rather than being a property of the packet, so the harness supplies
	// saguin's, 16 (internal/broker's topicAliasMaximum). That number gates
	// one of its seven rules; the other six, including the packet
	// identifier rules of [MQTT-2.2.1-3] and [MQTT-2.2.1-2], do not depend
	// on it.
	var code Code
	switch pk.FixedHeader.Type {
	case Connect:
		code = pk.ConnectValidate()
	case Publish:
		code = pk.PublishValidate(saguinTopicAliasMaximum)
	case Subscribe:
		code = pk.SubscribeValidate()
	case Unsubscribe:
		code = pk.UnsubscribeValidate()
	case Auth:
		code = pk.AuthValidate()
	default:
		return pk, nil
	}
	if code != CodeSuccess {
		return pk, code
	}
	return pk, nil
}

// readFullFrom fills b or reports why it could not, which is io.ReadFull's
// contract without the import.
func readFullFrom(r *bufio.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := r.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// encodeWire puts a packet back on the wire the way the server would.
func encodeWire(pk *Packet) ([]byte, error) {
	buf := new(bytes.Buffer)
	var err error
	switch pk.FixedHeader.Type {
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
	default:
		return nil, ErrProtocolViolation
	}
	return buf.Bytes(), err
}

// seedFromFixtures adds every packet the test table describes, at both
// protocol versions.
//
// **A fuzz target seeded with nothing is a fuzz target that measures the
// rejection of random bytes.** Almost every random string fails at the
// header byte, so without a corpus of real packets the fuzzer never reaches
// a topic name, a property block or a payload, and reports success having
// tested the first `switch` in this file. The table already holds the wire
// bytes of every packet type this broker speaks; they are the corpus.
func seedFromFixtures(f *testing.F) int {
	seeded := 0
	for _, cases := range TPacketData {
		for _, c := range cases {
			if len(c.RawBytes) == 0 {
				continue
			}
			f.Add(c.RawBytes, byte(5))
			f.Add(c.RawBytes, byte(4))
			seeded += 2
		}
	}
	return seeded
}

// Decoding bytes off a socket must not panic, whatever the bytes are.
//
// This is the weakest property here and the one worth the most: a panic in
// this package is reached before authentication, before an ACL, before a
// channel, by anybody who can complete a TCP handshake, and it takes the
// process with it. saguin has seen exactly that once - a v5 SUBSCRIBE
// missing its subscription-options byte panicked in ReadPacket, twelve
// bytes and no credentials, fixed upstream in mochi#524 and pinned in
// the fuzz corpus. Nothing has been looking for the next one.
func FuzzDecoderNeverPanics(f *testing.F) {
	if n := seedFromFixtures(f); n < 100 {
		f.Fatalf("only %d seeds from the packet table, which cannot be all of them: "+
			"this target would be fuzzing random bytes against the header byte", n)
	}

	f.Fuzz(func(t *testing.T, data []byte, version byte) {
		// Any byte is a legal thing for a client to claim, and the decoder
		// reads the version off the connection rather than validating it
		// here, so it is fuzzed rather than fixed.
		_, _ = decodeWire(data, version)
	})
}

// forwarded is the packet type this broker reads in and writes back out.
//
// **A round trip is a property only where the broker actually performs
// one.** PUBLISH is that packet and the only one: it arrives from a
// publisher and goes out to every subscriber, carrying a topic, a payload,
// a Response Topic and Correlation Data that [MQTT-3.3.2-15] and
// [MQTT-3.3.2-16] say must reach them unaltered. If the decoder loses a
// field, a subscriber loses it.
//
// Everything else fails the property for reasons that are not defects, and
// each was reported as one before this set was narrowed:
//
//   - CONNACK, SUBACK, PINGRESP: the server sends them and never reads one,
//     so the decoder is test-only code. Worse, the encoders apply rules
//     meant to lose information - a CONNACK carries Response Information
//     only where the client asked ([MQTT-3.1.2-28]).
//   - CONNECT, SUBSCRIBE, PINGREQ: the mirror image, the encoder is
//     test-only.
//   - PUBACK, PUBREC, PUBREL, PUBCOMP: the broker both sends and receives
//     these, but it never re-emits one it received. It reads the fields,
//     acts on them, and builds its own answer in buildAck. Decode and
//     encode are not inverses there and are not required to be: a PUBREL
//     arriving with reason code 0x30 re-encodes without it, because
//     encodePubAckRelRecComp writes that byte only for 0x80-and-above,
//     which is exactly the two values §3.6.2.1 defines.
//
// The panic target above covers all of them. This one is about the packet
// whose contents the broker is responsible for carrying.
var forwarded = map[byte]bool{Publish: true}

// A packet the decoder accepts must survive a round trip through the
// encoder unchanged.
//
// The property is about what the decoder is entitled to accept: if it
// returns a Packet, the broker will act on that Packet and may forward it,
// so the Packet has to be a faithful reading of the bytes. Re-encoding is
// how that is checked without asking the decoder to report what it
// consumed - a field it dropped, a length it misread or a section it
// stopped short of comes back different on the second decode.
//
// **Structural equality rather than byte equality**, because two encodings
// of the same packet are both legal: MQTT 5 fixes no order for properties,
// and an acknowledgement with reason 0x00 and no properties may be written
// with or without its reason byte. Comparing bytes would report those as
// defects. Comparing what the decoder made of them does not.
func FuzzDecodeSurvivesAReencode(f *testing.F) {
	if n := seedFromFixtures(f); n < 100 {
		f.Fatalf("only %d seeds from the packet table, which cannot be all of them", n)
	}

	f.Fuzz(func(t *testing.T, data []byte, version byte) {
		if version != 4 && version != 5 {
			return // not a version any client can connect with
		}

		first, err := decodeWire(data, version)
		if err != nil {
			return // rejected, which is the decoder's right
		}
		if !forwarded[first.FixedHeader.Type] {
			return
		}

		// **The Mods the server sets before it writes.** Client.WritePacket
		// sets AllowResponseInfo on every packet that is not a CONNACK,
		// which is what lets a PUBLISH carry the Response Topic and
		// Correlation Data that [MQTT-3.3.2-15] and [MQTT-3.3.2-16] say
		// must reach subscribers unaltered. Encoding with a zero Mods
		// instead drops both, and the first run of this target reported
		// that as the decoder losing them. It was this harness.
		first.Mods.AllowResponseInfo = true

		wire, err := encodeWire(&first)
		if err != nil {
			// The encoder refusing a packet the decoder accepted is worth
			// knowing about on its own: the broker cannot answer or
			// forward what it has just taken in.
			t.Fatalf("decoded a packet the encoder will not write back: %v\n"+
				"\ttype=%d version=%d input=%x", err, first.FixedHeader.Type, version, data)
		}

		second, err := decodeWire(wire, version)
		if err != nil {
			t.Fatalf("re-encoding a packet made it undecodable: %v\n"+
				"\ttype=%d version=%d input=%x reencoded=%x",
				err, first.FixedHeader.Type, version, data, wire)
		}

		// Remaining is the encoder's arithmetic rather than the decoder's
		// reading, and the two are allowed to differ where a legal
		// encoding is not the canonical one. Everything else must match.
		first.FixedHeader.Remaining = 0
		second.FixedHeader.Remaining = 0
		if !packetsEqual(first, second) {
			t.Fatalf("a decoded packet changed when it was written back and read again\n"+
				"\ttype=%d version=%d\n\tinput=%x\n\treencoded=%x\n\tfirst=%+v\n\tsecond=%+v",
				first.FixedHeader.Type, version, data, wire, first, second)
		}
	})
}

// packetsEqual compares the fields a decode fills, leaving out the ones
// that are the server's own bookkeeping rather than anything read off the
// wire.
func packetsEqual(a, b Packet) bool {
	a.Origin, b.Origin = "", ""
	a.Created, b.Created = 0, 0
	a.Expiry, b.Expiry = 0, 0
	a.Ignore, b.Ignore = false, false
	a.Mods, b.Mods = Mods{}, Mods{}
	// The CONNECT reserved bit is read and never written: the server does
	// not send CONNECTs, so its encoder has no reason to carry it. A packet
	// that got this far has it at 0 anyway, because ConnectValidate refuses
	// anything else.
	a.ReservedBit, b.ReservedBit = 0, 0
	return reflect.DeepEqual(a, b)
}
