package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The checks every packet's codec shares. Each packet's file keeps its own
// specs, oracles, cases and the sentences they cite, and hands them to the
// loops here, which hold one shape each: a round trip, Packet Identifier 0,
// another version, a property set, a missing payload, a cut body, the
// decoders' fuzz body and the allocation loops. A codec is described once,
// generic over the Go type its packet decodes to, as replyType first
// described SUBACK and UNSUBACK.

// codec describes one packet's codec: its name and type, the first byte of
// its fixed header, its decoder and encoder over P, how two P compare, and
// what the shared checks need to know of it.
type codec[P any] struct {
	name  string
	typ   packet.Type
	first byte // the fixed header's first byte: the type, then its flags
	// read and write are the decoder and the encoder, each nil where the
	// checks leave it out: Sagüin has none, or only a bridge uses it and
	// its piece tests moved to the wire (writer, reader). Every shared
	// check takes the halves it is given.
	read  func(body []byte, version packet.Version) (P, error)
	write func(dst []byte, version packet.Version, p *P) ([]byte, error)
	same  func(a, b *P) bool
	// ok is the fuzz oracle: whether the standards let the decoder accept p
	// under version.
	ok func(version packet.Version, p *P) bool
	// packetID points at p's Packet Identifier, for a packet whose body
	// begins with it; nil for any other.
	packetID func(p *P) *uint16
	// shortest returns the body the encoder writes for p, decoded from
	// data, where the standard lets data carry fields the encoder leaves
	// out; nil for a packet with one form, whose encoder writes data back.
	shortest func(data []byte, p *P) []byte
	// brief formats p for a failure message; nil prints every field.
	brief func(p *P) string
	// newProperties: NewProperties makes this packet's property section.
	newProperties bool
	// checksProperties: the encoder refuses a property section the decoder
	// refuses. Otherwise it writes the section as given, checked when made
	// (the maintainer's decision on 261013-024a).
	checksProperties bool
}

// writer is k without its decoder, and reader k without its encoder: the
// half only a bridge uses left out, its piece tests moved to tests from
// outside, a bridge's peer on the wire (item 0019b; design.md, Bridges).
// k whole stays with the fuzz targets of outside input and the allocation
// guards of the busy path.
func (k codec[P]) writer() *codec[P] {
	k.read = nil
	return &k
}

// reader is k without its encoder (writer).
func (k codec[P]) reader() *codec[P] {
	k.write = nil
	return &k
}

// writing returns cases, each on its codec's writer, and reading each on
// its codec's reader.
func writing[P any](cases []packetCase[P]) []packetCase[P] {
	return onHalf(cases, codec[P].writer)
}

// reading returns cases, each on its codec's reader (writing).
func reading[P any](cases []packetCase[P]) []packetCase[P] {
	return onHalf(cases, codec[P].reader)
}

// onHalf returns cases, each on the codec half makes of its own.
func onHalf[P any](cases []packetCase[P], half func(codec[P]) *codec[P]) []packetCase[P] {
	out := slices.Clone(cases)
	for i := range out {
		out[i].codec = half(*out[i].codec)
	}
	return out
}

// wire returns the whole packet of body: the first byte, the Remaining
// Length, then body (packetWith).
func (k *codec[P]) wire(body []byte) []byte {
	return packetWith(k.first, body)
}

// show formats p for a failure message.
func (k *codec[P]) show(p *P) string {
	if k.brief != nil {
		return k.brief(p)
	}
	return fmt.Sprintf("%+v", *p)
}

// readRefused fails the test unless the decoder refuses body under version
// with class, or with a packet error (isKind) where class is nil: the
// standard requires the refusal without naming its class. A codec without
// a decoder checks nothing.
func (k *codec[P]) readRefused(t *testing.T, what string, version packet.Version, body []byte, class error) {
	t.Helper()
	if k.read == nil {
		return
	}
	got, err := k.read(body, version)
	if !refusedAs(err, class) {
		t.Errorf("%s, %s, protocol level %d, %s: got %s, %v; want %s", k.name, what, version, shown(body), k.show(&got), err, classOf(class))
	}
}

// writeRefused fails the test unless the encoder refuses p under version
// with ErrCannotEncode, leaving dst as it was (refusedToWrite). A codec
// without an encoder checks nothing.
func (k *codec[P]) writeRefused(t *testing.T, what string, version packet.Version, p *P) {
	t.Helper()
	if k.write == nil {
		return
	}
	refusedToWrite(t, fmt.Sprintf("%s, %s, protocol level %d", k.name, what, version), func(dst []byte) ([]byte, error) {
		return k.write(dst, version, p)
	})
}

// refused fails the test unless the decoder refuses body under version
// with a packet error and the encoder refuses p, which stands for the same
// packet, with ErrCannotEncode, leaving dst as it was.
func (k *codec[P]) refused(t *testing.T, what string, version packet.Version, body []byte, p *P) {
	t.Helper()
	k.readRefused(t, what, version, body, nil)
	k.writeRefused(t, what, version, p)
}

// refusedToWrite fails the test unless an encoder, called with dst holding
// AA, returns ErrCannotEncode and dst as it was: no part of a packet the
// standard forbids is ever written.
func refusedToWrite(t *testing.T, what string, write func(dst []byte) ([]byte, error)) {
	t.Helper()
	if out, err := write([]byte{0xAA}); !cannotEncode(err) || !bytes.Equal(out, []byte{0xAA}) {
		t.Errorf("%s: wrote %s, %v; want AA, ErrCannotEncode", what, shown(out), err)
	}
}

// refusedAs reports whether err is the refusal class names, or, where
// class is nil, either packet error (isKind).
func refusedAs(err, class error) bool {
	if class == nil {
		return isKind(err)
	}
	return errors.Is(err, class)
}

// classOf names the refusal refusedAs looks for.
func classOf(class error) string {
	if class == nil {
		return "ErrMalformed or ErrProtocol"
	}
	return class.Error()
}

// shown formats bytes for a failure message: all of them, or for a long
// packet the first 64 and the length.
func shown(b []byte) string {
	if len(b) <= 64 {
		return fmt.Sprintf("% X", b)
	}
	return fmt.Sprintf("% X... (%d bytes)", b[:64], len(b))
}

// packetWith returns a whole packet whose fixed header's first byte is
// first: the Remaining Length as a Variable Byte Integer (the standard
// library's unsigned varint), then body. A packet whose flags are 0, as
// CONNECT's and CONNACK's are (MQTT 5 3.1.1, 3.2.1), has byte(type)<<4.
func packetWith(first byte, body []byte) []byte {
	return slices.Concat([]byte{first}, binary.AppendUvarint(nil, uint64(len(body))), body)
}

// bothVersions are the versions every codec reads and writes, as versions
// (header_test.go) names them.
var bothVersions = func() []packet.Version {
	vs := make([]packet.Version, len(versions))
	for i := range versions {
		vs[i] = versions[i].v
	}
	return vs
}()

// versionOf returns the version a fuzz target's flag picks.
func versionOf(v5 bool) packet.Version {
	if v5 {
		return packet.V5
	}
	return packet.V311
}

// packetCase is one packet a test hands its codec: its name, the version,
// the body the decoder reads (the Remaining Length bytes ReadHeader leaves
// behind) and the value it decodes to, in the one form the encoder writes.
type packetCase[P any] struct {
	name    string
	codec   *codec[P]
	version packet.Version
	body    []byte
	want    P
}

// casesOf returns the case caseOf makes of each spec.
func casesOf[S, P any](specs []S, caseOf func(*S) packetCase[P]) []packetCase[P] {
	cases := make([]packetCase[P], len(specs))
	for i := range specs {
		cases[i] = caseOf(&specs[i])
	}
	return cases
}

// testRoundTrips decodes and encodes each case, through the halves its
// codec has: the decoder returns every field; the encoder writes the whole
// packet, the fixed header's first byte as the case's codec gives it, into
// nil and after what dst holds.
func testRoundTrips[P any](t *testing.T, cases []packetCase[P]) {
	t.Helper()
	for i := range cases {
		c := &cases[i]
		k := c.codec
		if k.read != nil {
			got, err := k.read(c.body, c.version)
			if err != nil || !k.same(&got, &c.want) {
				t.Errorf("%s: decoded %s as %s, %v; want %s", c.name, shown(c.body), k.show(&got), err, k.show(&c.want))
			}
		}
		if k.write == nil {
			continue
		}
		wire := k.wire(c.body)
		if out, err := k.write(nil, c.version, &c.want); err != nil || !bytes.Equal(out, wire) {
			t.Errorf("%s: wrote %s, %v; want %s", c.name, shown(out), err, shown(wire))
		}
		if out, err := k.write([]byte{0xAA}, c.version, &c.want); err != nil || !bytes.Equal(out, append([]byte{0xAA}, wire...)) {
			t.Errorf("%s: wrote after AA %s, %v; want AA %s", c.name, shown(out), err, shown(wire))
		}
	}
}

// testPacketIdentifierZero sets each case's Packet Identifier, the first
// two bytes of its body, to 0: the decoder refuses the body with a packet
// error, and the encoder refuses the value with ErrCannotEncode, leaving
// dst as it was.
func testPacketIdentifierZero[P any](t *testing.T, cases []packetCase[P]) {
	t.Helper()
	for _, c := range cases {
		body := slices.Concat([]byte{0x00, 0x00}, c.body[2:])
		*c.codec.packetID(&c.want) = 0
		c.codec.refused(t, c.name+", Packet Identifier 0", c.version, body, &c.want)
	}
}

// testNoPayload checks a packet whose payload is a list of one or more,
// under each of versions: the decoder refuses the Packet Identifier alone
// with a packet error, which under MQTT 5 also lacks the Property Length;
// under MQTT 5 it refuses the Packet Identifier and an empty property set,
// or the set props, with no payload after them, with class (a packet error
// where class is nil). The encoder refuses the packet empty makes of a nil
// and of an empty payload, leaving dst as it was.
func testNoPayload[P any](t *testing.T, k *codec[P], versions []packet.Version, class error, props []wired, empty func(payload []byte) P) {
	t.Helper()
	for _, version := range versions {
		k.readRefused(t, "no payload", version, []byte{0x00, 0x01}, nil)
		if version == packet.V5 {
			k.readRefused(t, "no payload", version, []byte{0x00, 0x01, 0x00}, class)
			k.readRefused(t, "no payload", version, slices.Concat([]byte{0x00, 0x01}, setOf(props)), class)
		}
		for _, payload := range [][]byte{nil, {}} {
			p := empty(payload)
			k.writeRefused(t, "no payload", version, &p)
		}
	}
}

// cutsOf returns full cut short at every length from from on, leaving out
// the lengths in whole: those cut where a field ends, which leaves a body
// that is not cut inside a field.
func cutsOf(full []byte, from int, whole ...int) [][]byte {
	var cuts [][]byte
	for n := from; n < len(full); n++ {
		if !slices.Contains(whole, n) {
			cuts = append(cuts, full[:n])
		}
	}
	return cuts
}

// propsCase is a property set, the class the standard gives the decoder's
// refusal of it (nil where it names none), and whether the standard allows
// it. place says which section takes it, for a packet with two (CONNECT's
// Will Properties); empty for the packet's own.
type propsCase struct {
	name  string
	place string
	props []wired
	class error
	ok    bool
}

// propsCases returns a case for each value each of rules tries, good and
// bad, then a refused case for each of refused.
func propsCases(rules []valueRule, refused []propertyCase) []propsCase {
	var cases []propsCase
	for _, r := range rules {
		for _, v := range slices.Concat(r.good, r.bad) {
			cases = append(cases, propsCase{fmt.Sprintf("%s of %d", r.name, v), r.place, []wired{intProp(r.id, v)}, r.class, r.ok(v)})
		}
	}
	for _, p := range refused {
		cases = append(cases, propsCase{name: p.name, props: p.props, class: p.class})
	}
	return cases
}

// rulesFor returns the valueRules of place.
func rulesFor(place string) []valueRule {
	return slices.DeleteFunc(slices.Clone(valueRules), func(r valueRule) bool { return r.place != place })
}

// testProperties runs each case through k under MQTT 5, in the packet
// caseOf builds around its set, through the halves k has. An allowed set
// decodes, NewProperties makes exactly its wire bytes (where
// k.newProperties), and the packet writes back. A refused set the decoder
// refuses with the case's class (a packet error where it names none);
// NewProperties refuses to make it, with ErrCannotEncode and no section
// (where k.newProperties); and the encoder refuses it, leaving dst as it
// was (where k.checksProperties).
func testProperties[P any](t *testing.T, k *codec[P], cases []propsCase, caseOf func(c *propsCase) (body []byte, want P)) {
	t.Helper()
	for i := range cases {
		c := &cases[i]
		body, want := caseOf(c)
		var got P
		var err error
		if k.read != nil {
			got, err = k.read(body, packet.V5)
		}
		var section packet.Properties
		var newErr error
		if k.newProperties {
			section, newErr = packet.NewProperties(k.typ, listOf(c.props))
		}
		if c.ok {
			if k.read != nil && (err != nil || !k.same(&got, &want)) {
				t.Errorf("%s, %s: decoded %s as %s, %v; want %s", k.name, c.name, shown(body), k.show(&got), err, k.show(&want))
			}
			if k.newProperties && (newErr != nil || !bytes.Equal(section, propsOf(c.props))) {
				t.Errorf("%s, %s: NewProperties = % X, %v; want % X", k.name, c.name, []byte(section), newErr, []byte(propsOf(c.props)))
			}
			if k.write == nil {
				continue
			}
			if out, err := k.write(nil, packet.V5, &want); err != nil || !bytes.Equal(out, k.wire(body)) {
				t.Errorf("%s, %s: wrote %s, %v; want %s", k.name, c.name, shown(out), err, shown(k.wire(body)))
			}
			continue
		}
		if k.read != nil && !refusedAs(err, c.class) {
			t.Errorf("%s, %s: decoded %s as %s, %v; want %s", k.name, c.name, shown(body), k.show(&got), err, classOf(c.class))
		}
		if k.newProperties && (!cannotEncode(newErr) || len(section) != 0) {
			t.Errorf("%s, %s: NewProperties = % X, %v; want no section, ErrCannotEncode", k.name, c.name, []byte(section), newErr)
		}
		if k.checksProperties {
			k.writeRefused(t, c.name, packet.V5, &want)
		}
	}
}

// fuzzAccepted is the first half of the body every decoder's fuzz target shares
// (design.md, Testing: "Code that reads bytes or text from outside Sagüin is
// fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files"): the decoder must never panic; it refuses with a packet
// error; what it accepts obeys k.ok. It returns what the decoder accepted,
// and whether it did.
func (k *codec[P]) fuzzAccepted(t *testing.T, version packet.Version, data []byte) (P, bool) {
	t.Helper()
	p, err := k.read(data, version)
	if err != nil {
		if !isKind(err) {
			t.Fatalf("%s, protocol level %d, % X: err = %v, not a packet error", k.name, version, data, err)
		}
		return p, false
	}
	if !k.ok(version, &p) {
		t.Fatalf("%s, protocol level %d, % X: accepted %s, which the standard forbids", k.name, version, data, k.show(&p))
	}
	return p, true
}

// fuzzDecode is the whole body: what the decoder accepts writes back as
// exactly the packet it came from, or, where the encoder writes a shorter
// form (k.shortest), as that form, which reads back as the same value. A
// packet Sagüin only decodes, its codec without an encoder, is checked by
// fuzzAccepted alone.
func (k *codec[P]) fuzzDecode(t *testing.T, version packet.Version, data []byte) {
	t.Helper()
	p, ok := k.fuzzAccepted(t, version, data)
	if !ok || k.write == nil {
		return
	}
	want := data
	if k.shortest != nil {
		want = k.shortest(data, &p)
	}
	if out, err := k.write(nil, version, &p); err != nil || !bytes.Equal(out, k.wire(want)) {
		t.Fatalf("%s, protocol level %d: read % X as %s, which writes as % X (err %v); want % X", k.name, version, data, k.show(&p), out, err, k.wire(want))
	}
	if k.shortest == nil {
		return
	}
	if back, err := k.read(want, version); err != nil || !k.same(&back, &p) {
		t.Fatalf("%s, protocol level %d: % X reads back as %s, %v; want %s", k.name, version, want, k.show(&back), err, k.show(&p))
	}
}

// noAllocsDecoding holds each case's decoder, and pass over what it
// returns, to no allocation (alloc_test.go's standard); a case whose codec
// has no decoder is left out.
func noAllocsDecoding[P any](t *testing.T, cases []packetCase[P], pass func(p P)) {
	t.Helper()
	for i := range cases {
		c := &cases[i]
		if c.codec.read == nil {
			continue
		}
		testkit.NoAllocs(t, "decoding and one pass, "+c.name, func() {
			p, err := c.codec.read(c.body, c.version)
			if err != nil {
				t.Fatal(err)
			}
			pass(p)
		})
	}
}

// noAllocsWriting holds each case's encoder, writing into a buffer with
// exactly the packet's room, to no allocation (alloc_test.go's standard);
// a case whose codec has no encoder is left out.
func noAllocsWriting[P any](t *testing.T, cases []packetCase[P]) {
	t.Helper()
	for i := range cases {
		c := &cases[i]
		if c.codec.write == nil {
			continue
		}
		buf := make([]byte, 0, len(c.codec.wire(c.body)))
		testkit.NoAllocs(t, "writing into a buffer with room, "+c.name, func() {
			if _, err := c.codec.write(buf, c.version, &c.want); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// stopsWhenAsked checks the contract of iter.Seq: "If yield returns false,
// the iterator should stop" (package iter); a range loop's break is that
// false, and the Go specification makes a further call a run-time panic.
// seq yields n values; stopped after each count from 1 to n, it calls
// yield exactly that many times.
func stopsWhenAsked[V any](t *testing.T, what string, seq iter.Seq[V], n int) {
	t.Helper()
	for stop := 1; stop <= n; stop++ {
		calls := 0
		seq(func(V) bool {
			calls++
			return calls < stop
		})
		if calls != stop {
			t.Errorf("%s, of %d values, stopped after %d: yield called %d times; want %d", what, n, stop, calls, stop)
		}
	}
}
