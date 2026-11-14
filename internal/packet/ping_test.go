package packet_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// Keeping alive, PINGREQ and PINGRESP (MQTT 5 3.12 and 3.13, MQTT 3.1.1
// 3.12 and 3.13). Each is its fixed header alone, the same in both
// versions: "no Variable Header" and "no Payload" (MQTT 5 3.12.2, 3.12.3,
// 3.13.2, 3.13.3; MQTT 3.1.1 3.12.2, 3.12.3, 3.13.2, 3.13.3), a Remaining
// Length of 0 (Figures 3.33 and 3.34). The broker reads PINGREQ and writes
// PINGRESP to its clients [MQTT-3.12.4-1]; as a bridge's client it writes
// PINGREQ and reads PINGRESP. A decoder takes the body ReadHeader leaves
// behind, an encoder writes the whole packet. Their reserved flags are
// ReadHeader's to check (the conformance tests' TestBadPacketsAreRefused), so a decoder never
// sees them. Error classes follow connect_test.go.

// ping is what PINGREQ and PINGRESP carry: nothing.
type ping struct{}

// newPingCodec describes one of the two packets' codecs, over ping: its
// decoder and encoder, and the first byte of its figure. Every ping is the
// same.
func newPingCodec(name string, typ packet.Type, first byte,
	decode func([]byte, packet.Version) error, write func([]byte, packet.Version) ([]byte, error),
) codec[ping] {
	return codec[ping]{
		name: name, typ: typ, first: first,
		read:  func(src []byte, version packet.Version) (ping, error) { return ping{}, decode(src, version) },
		write: func(dst []byte, version packet.Version, _ *ping) ([]byte, error) { return write(dst, version) },
		same:  func(_, _ *ping) bool { return true },
	}
}

// pingresp is PINGRESP: MQTT 5 Figure 3.34, byte 1 "MQTT Control Packet
// type (13)" over 1101 and Reserved 0000, byte 2 "Remaining Length (0)";
// MQTT 3.1.1's Figure 3.34 draws the same bytes, a whole packet of
// wire(nil). The broker reads PINGREQ with its fixed header alone (the
// conformance tests' TestBadPacketsAreRefused); PINGRESP it writes, and
// reads as a bridge's client.
var pingresp = newPingCodec("PINGRESP", packet.PINGRESP, 0xD0, packet.DecodePingresp, packet.AppendPingresp)

// TestPing encodes PINGRESP in both versions: the encoder writes the whole
// packet of its figure, after what dst holds. Reading a PINGRESP, a
// bridge's alone, is checked from the wire, where a bridge's peer answers
// its PINGREQ (item 0019b; design.md, Bridges), its fuzz target and
// allocation guard staying here. AppendPingreq, a bridge's, writes MQTT 5
// Figure 3.33's two bytes, type 12 over 1100 and a Remaining Length of 0,
// after dst.
func TestPing(t *testing.T) {
	cases := make([]packetCase[ping], 0, 2*len(versions))
	for _, ver := range versions {
		for _, body := range [][]byte{nil, {}} {
			cases = append(cases, packetCase[ping]{fmt.Sprintf("PINGRESP, MQTT %s, body % X", ver.name, body), &pingresp, ver.v, body, ping{}})
		}
		if out, err := packet.AppendPingreq([]byte{0xAA}, ver.v); err != nil || !bytes.Equal(out, []byte{0xAA, 0xC0, 0x00}) {
			t.Errorf("AppendPingreq after AA, MQTT %s = % X, %v; want AA C0 00", ver.name, out, err)
		}
	}
	testRoundTrips(t, writing(cases))
}

// TestPingDoesNotAllocate, by alloc_test.go's standard: a PINGREQ arrives
// once per keep-alive interval on every idle connection, and each gets a
// PINGRESP [MQTT-3.12.4-1]. PINGRESP keeps nothing, so decoding allocates
// nothing, and nor does writing into a buffer with room.
func TestPingDoesNotAllocate(t *testing.T) {
	cases := make([]packetCase[ping], 0, len(versions))
	for _, ver := range versions {
		cases = append(cases, packetCase[ping]{"PINGRESP, MQTT " + ver.name, &pingresp, ver.v, nil, ping{}})
	}
	noAllocsDecoding(t, cases, func(ping) {})
	noAllocsWriting(t, cases)
}

// fuzzPing feeds random bodies to p's decoder under random protocol levels
// (design.md, Testing: "Code that reads bytes or text from outside Sagüin is
// fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files"). The level is a byte, not a choice of the two versions,
// so the fuzzer also reaches the refusal of any other. The oracle is exact:
// under 3.1.1 or 5, only the empty body is accepted, any other is a Malformed
// Packet (MQTT 5 1.2: bytes left over cannot be parsed), and what is
// accepted writes back as the
// packet of p's figure; under any other level, every body is refused with a
// packet error (the decoders' contract).
func fuzzPing(f *testing.F, p *codec[ping]) {
	f.Add([]byte{}, byte(packet.V311))
	f.Add([]byte{}, byte(packet.V5))
	f.Add([]byte{0x00}, byte(packet.V5))
	f.Add([]byte{0x00, 0x00}, byte(packet.V311))
	f.Add(p.wire(nil), byte(packet.V5))
	f.Add([]byte{}, byte(3))
	f.Fuzz(func(t *testing.T, data []byte, level byte) {
		version := packet.Version(level)
		_, err := p.read(data, version)
		switch {
		case version != packet.V311 && version != packet.V5:
			if !isKind(err) {
				t.Fatalf("%s, protocol level %d, % X: %v; want ErrMalformed or ErrProtocol", p.name, level, data, err)
			}
			return
		case len(data) > 0:
			if !errors.Is(err, packet.ErrMalformed) {
				t.Fatalf("%s, protocol level %d, % X: %v; want ErrMalformed", p.name, level, data, err)
			}
			return
		case err != nil:
			t.Fatalf("%s, protocol level %d: the empty body refused: %v", p.name, level, err)
		}
		if out, err := p.write(nil, version, &ping{}); err != nil || !bytes.Equal(out, p.wire(nil)) {
			t.Fatalf("%s, protocol level %d: accepted, then wrote % X, %v; want % X", p.name, level, out, err, p.wire(nil))
		}
	})
}

// FuzzDecodePingresp fuzzes DecodePingresp (fuzzPing).
func FuzzDecodePingresp(f *testing.F) {
	fuzzPing(f, &pingresp)
}
