package packet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Publishing, PUBLISH (MQTT 5 3.3, MQTT 3.1.1 3.3). A PUBLISH's fixed
// header carries live flags, DUP, QoS and RETAIN (MQTT 5 3.3.1 Figure 3-8,
// MQTT 3.1.1 3.3.1 Figure 3.10), which ReadHeader leaves to the packet's
// own rules, so the decoder takes the flags with the body. Error classes
// follow connect_test.go. Whether a Topic Name is a valid topic
// (wildcards, `$`, limits) is item 0003e's: these tests leave it out.
//
// The API these tests fix:
//
//	type Publish struct {
//		Dup        bool
//		QoS        byte
//		Retain     bool
//		Topic      []byte
//		PacketID   uint16
//		Properties Properties
//		Payload    []byte
//	}
//	func DecodePublish(src []byte, flags byte, version Version) (Publish, error)
//	func AppendPublish(dst []byte, version Version, p *Publish) ([]byte, error)
//
// DecodePublish decodes a PUBLISH body, without its fixed header, given the
// fixed header's four flag bits; Topic, Properties and Payload share src.
// AppendPublish writes the whole packet, fixed header included, after what
// dst holds: what the decoder reads writes back as itself (TestPublish,
// FuzzDecodePublish), without allocating (alloc_test.go).
//
// AppendPublish trusts its Topic and Properties (the maintainer's decision
// on 261013-024a): the Topic comes from a checked decode or a stored record,
// the Properties are checked when made (DecodePublish, Properties.Without,
// NewProperties), and an empty Topic Name with a Topic Alias is the
// writer's choice (design R1: outgoing topic aliases are the writer's). It
// writes both as given (TestAppendPublishWritesAsGiven). The PUBLISHes the
// broker sends are checked where a client reads them, on the wire
// (internal/harness/conformance, delivery_test.go), and the writer's own
// busy path inside it (internal/connection, delivery_seam_test.go).

// PUBLISH fixed-header flags: DUP is bit 3, QoS bits 2-1, RETAIN bit 0
// (MQTT 5 3.3.1.1-3.3.1.3; MQTT 3.1.1 3.3.1.1-3.3.1.3).
const (
	dupFlag    = 0x08
	qosShift   = 1
	retainFlag = 0x01
)

// publishCodecs describe PUBLISH's codec for each value of the fixed
// header's four flag bits, which the decoder takes with the body. The fuzz
// oracle is publishOK, and failure messages cut long fields (brief). The
// encoder writes its Topic Name and Properties as given
// (TestAppendPublishWritesAsGiven), the Properties made by NewProperties.
var publishCodecs = func() [16]codec[packet.Publish] {
	var codecs [16]codec[packet.Publish]
	for flags := range byte(16) {
		codecs[flags] = codec[packet.Publish]{
			name: "PUBLISH", typ: packet.PUBLISH, first: byte(packet.PUBLISH)<<4 | flags,
			read: func(body []byte, version packet.Version) (packet.Publish, error) {
				return packet.DecodePublish(body, flags, version)
			},
			write: packet.AppendPublish, same: samePublish, ok: publishOK, brief: brief,
			newProperties: true,
		}
	}
	return codecs
}()

// publishSpec describes a PUBLISH field by field. body lays it out as the
// standard orders it; want is the Publish it decodes to.
type publishSpec struct {
	name    string
	version packet.Version
	dup     bool
	qos     byte
	retain  bool
	topic   string
	id      uint16
	props   []wired // MQTT 5 only
	payload []byte
}

// flags returns the fixed header's low four bits.
func (s *publishSpec) flags() byte {
	f := s.qos << qosShift
	if s.dup {
		f |= dupFlag
	}
	if s.retain {
		f |= retainFlag
	}
	return f
}

// body returns the variable header and payload. MQTT 5 3.3.2: "Topic Name,
// Packet Identifier, and Properties"; MQTT 3.1.1 3.3.2: "Topic Name, Packet
// Identifier". 3.3.2.2 (both): "The Packet Identifier field is only present
// in PUBLISH packets where the QoS level is 1 or 2." The payload is the
// rest (3.3.3).
func (s *publishSpec) body() []byte {
	b := prefixed([]byte(s.topic))
	if s.qos != 0 {
		b = binary.BigEndian.AppendUint16(b, s.id)
	}
	if s.version == packet.V5 {
		b = append(b, setOf(s.props)...)
	}
	return append(b, s.payload...)
}

// codec returns the codec of the spec's flags.
func (s *publishSpec) codec() *codec[packet.Publish] {
	return &publishCodecs[s.flags()]
}

// caseOf returns the spec as the case its codec takes.
func (s *publishSpec) caseOf() packetCase[packet.Publish] {
	return packetCase[packet.Publish]{s.name, s.codec(), s.version, s.body(), s.want()}
}

// want returns the Publish the spec decodes to.
func (s *publishSpec) want() packet.Publish {
	return packet.Publish{
		Dup:        s.dup,
		QoS:        s.qos,
		Retain:     s.retain,
		Topic:      []byte(s.topic),
		PacketID:   s.id,
		Properties: propsOf(s.props),
		Payload:    s.payload,
	}
}

// samePublish reports whether two PUBLISHes hold the same fields.
func samePublish(a, b *packet.Publish) bool {
	return a.Dup == b.Dup && a.QoS == b.QoS && a.Retain == b.Retain && bytes.Equal(a.Topic, b.Topic) &&
		a.PacketID == b.PacketID && sameValues(a.Properties, b.Properties) && bytes.Equal(a.Payload, b.Payload)
}

// brief formats a Publish for a failure message, its Topic Name, Properties
// and Payload cut to 32 bytes.
func brief(p *packet.Publish) string {
	return fmt.Sprintf("{Dup:%v QoS:%d Retain:%v Topic:%.32q (%d bytes) PacketID:%d Properties:% .32X Payload:% .32X (%d bytes)}",
		p.Dup, p.QoS, p.Retain, p.Topic, len(p.Topic), p.PacketID, []byte(p.Properties), p.Payload, len(p.Payload))
}

// publishValueRules are the value rules of MQTT 5 3.3.2.3, beyond the type
// Table 2-4 gives each property.
var publishValueRules = []valueRule{
	// 3.3.2.3.2: "Followed by the value of the Payload Format Indicator,
	// either of: 0 (0x00) ... 1 (0x01)". No class is named.
	{"PUBLISH", "Payload Format Indicator", 0x01, zeroOrOne, []uint32{0, 1}, []uint32{2, 255}, nil},
	// 3.3.2.3.4: "A Topic Alias of 0 is not permitted. A sender MUST NOT
	// send a PUBLISH packet containing a Topic Alias which has the value 0
	// [MQTT-3.3.2-8]." No class is named.
	{"PUBLISH", "Topic Alias", 0x23, nonZero, []uint32{1, 65_535}, []uint32{0}, nil},
	// 3.3.2.3.8: "The Subscription Identifier can have the value of 1 to
	// 268,435,455. It is a Protocol Error if the Subscription Identifier
	// has a value of 0."
	{"PUBLISH", "Subscription Identifier", 0x0B, nonZero, []uint32{1, maxVarInt}, []uint32{0}, packet.ErrProtocol},
}

// publishValuesOK reports whether props keep publishValueRules.
func publishValuesOK(props []packet.Property) bool {
	for i := range props {
		for _, r := range publishValueRules {
			if r.id == props[i].ID && !r.ok(props[i].Int) {
				return false
			}
		}
	}
	return true
}

// hasTopicAlias reports whether props hold a Topic Alias.
func hasTopicAlias(props []packet.Property) bool {
	return slices.ContainsFunc(props, func(p packet.Property) bool { return p.ID == 0x23 })
}

// publishOK is the oracle for a Publish the decoder returns under version:
// everything MQTT 5 3.3 or MQTT 3.1.1 3.3 asks of a PUBLISH, Topic Name
// validity aside (item 0003e).
func publishOK(version packet.Version, p *packet.Publish) bool {
	// [MQTT-3.3.1-4]: not both QoS bits; [MQTT-3.3.1-2]: DUP 0 at QoS 0;
	// MQTT 5 [MQTT-2.2.1-2], [MQTT-2.2.1-3], [MQTT-2.2.1-4], MQTT 3.1.1
	// [MQTT-2.3.1-1], [MQTT-2.3.1-5]: a non-zero Packet Identifier exactly
	// at QoS 1 and 2; [MQTT-3.3.2-1]: the Topic Name a UTF-8 Encoded String.
	if p.QoS > 2 || (p.Dup && p.QoS == 0) || (p.QoS == 0) != (p.PacketID == 0) || !testkit.UTF8StringOK(string(p.Topic)) {
		return false
	}
	switch version {
	case packet.V311:
		return len(p.Properties) == 0 // 3.3.2: no Properties
	case packet.V5:
		// 3.3.2.1: "It is a Protocol Error if the Topic Name is zero length
		// and there is no Topic Alias."
		props, ok := valuesOf(p.Properties)
		return ok && encodable(placeNamed["PUBLISH"], props) && publishValuesOK(props) &&
			(len(p.Topic) != 0 || hasTopicAlias(props))
	}
	return false
}

// everyPublishProperty sets every property 3.3.2.3 lists, each at a value
// its section allows: the User Property twice under one name (3.3.2.3.7:
// "The same name is allowed to appear more than once") and the Subscription
// Identifier twice (3.3.2.3.8: "Multiple Subscription Identifiers will be
// included if the publication is the result of a match to more than one
// subscription"), at its smallest and largest.
var everyPublishProperty = []wired{
	intProp(0x01, 1), intProp(0x02, 0xFFFFFFFF), intProp(0x23, 65_535), stringProp(0x08, "reply/to"),
	binaryProp(0x09, []byte{0x00, 0xFF}), userProp("n", "v"), userProp("n", "w"),
	intProp(0x0B, 1), intProp(0x0B, maxVarInt), stringProp(0x03, "text/plain"),
}

// validPublishes are PUBLISHes the standards allow.
var validPublishes = []publishSpec{
	// MQTT 5 Figure 3-9, the non-normative example: "the Topic Name set to
	// "a/b", the Packet Identifier set to 10, and having no properties".
	// 3.3.3: "It is valid for a PUBLISH packet to contain a zero length
	// Payload."
	{name: "MQTT 5, the example of Figure 3-9", version: packet.V5, qos: 1, topic: "a/b", id: 10},
	// MQTT 3.1.1 Table 3.3 and Figure 3.11: Topic Name "a/b", Packet
	// Identifier 10. 3.3.3: "It is valid for a PUBLISH Packet to contain a
	// zero length payload."
	{name: "MQTT 3.1.1, the example of Figure 3.11", version: packet.V311, qos: 1, topic: "a/b", id: 10},
	// 3.3.2.2: no Packet Identifier at QoS 0; 3.3.1.3: RETAIN.
	{name: "MQTT 5, QoS 0, RETAIN", version: packet.V5, retain: true, topic: "t", payload: []byte("hello")},
	{name: "MQTT 3.1.1, QoS 0, RETAIN", version: packet.V311, retain: true, topic: "t", payload: []byte("hello")},
	{name: "MQTT 5, QoS 1, DUP, RETAIN, every PUBLISH property", version: packet.V5, dup: true, qos: 1, retain: true, topic: "a/b", id: 1, props: everyPublishProperty, payload: []byte("{}")},
	// 3.3.2.3.4: "a sender can send a PUBLISH packet that contains that
	// Topic Alias and a zero length Topic Name."
	{name: "MQTT 5, QoS 0, an empty Topic Name with a Topic Alias", version: packet.V5, props: []wired{intProp(0x23, 1)}, payload: []byte("p")},
	{name: "MQTT 5, QoS 1, an empty Topic Name with a Topic Alias", version: packet.V5, qos: 1, id: 7, props: []wired{intProp(0x23, 9)}},
	// 3.3.2.2: at QoS 0 no Packet Identifier is read, so the bytes after
	// the Topic Name are the Properties (MQTT 5) or the payload (MQTT 3.1.1).
	{name: "MQTT 5, QoS 0, a payload shaped like a Packet Identifier", version: packet.V5, topic: "t", payload: []byte{0x00, 0x0A}},
	{name: "MQTT 3.1.1, QoS 0, a payload shaped like a Packet Identifier", version: packet.V311, topic: "t", payload: []byte{0x00, 0x0A}},
	// MQTT 5 [MQTT-1.5.4-3], MQTT 3.1.1 [MQTT-1.5.3-3]: "A UTF-8 encoded
	// sequence 0xEF 0xBB 0xBF is always interpreted as U+FEFF ... and MUST
	// NOT be skipped over or stripped off by a packet receiver." MQTT 5
	// Figure 1-2's string, "A𪛔", follows it.
	{name: "MQTT 5, a Topic Name opening with U+FEFF", version: packet.V5, topic: "\xEF\xBB\xBFA\U0002A6D4", payload: []byte("p")},
	{name: "MQTT 3.1.1, a Topic Name opening with U+FEFF", version: packet.V311, qos: 1, topic: "\xEF\xBB\xBFA\U0002A6D4", id: 2},
	// MQTT 5 1.5.4: "the maximum size of a UTF-8 Encoded String is 65,535
	// bytes"; MQTT 3.1.1 1.5.3 likewise.
	{name: "MQTT 5, a Topic Name of 65,535 bytes", version: packet.V5, qos: 1, topic: strings.Repeat("a", 65_535), id: 3, payload: []byte("p")},
	{name: "MQTT 3.1.1, a Topic Name of 65,535 bytes", version: packet.V311, topic: strings.Repeat("a", 65_535), payload: []byte("p")},
	// design.md, Choices MQTT leaves open: "A payload whose Payload Format
	// Indicator is 1 but which is not valid UTF-8 (0x99 is a MAY, MQTT 5
	// 3.3.2.3.2) | accepted and delivered unaltered: the payload is never
	// checked".
	{name: "MQTT 5, Payload Format Indicator 1, an ill-formed payload", version: packet.V5, topic: "t", props: []wired{intProp(0x01, 1)}, payload: []byte{0xC0, 0x80, 0xFF}},
	{name: "MQTT 5, Payload Format Indicator 1, a surrogate in the payload", version: packet.V5, qos: 1, topic: "t", id: 4, props: []wired{intProp(0x01, 1)}, payload: []byte{0xED, 0xA0, 0x80}},
	{name: "MQTT 5, Payload Format Indicator 1, U+0000 in the payload", version: packet.V5, qos: 1, topic: "t", id: 5, props: []wired{intProp(0x01, 1)}, payload: []byte{'a', 0x00, 'b'}},
}

// TestPublish decodes and encodes each of validPublishes: the decoder
// returns every field; the encoder writes the whole packet, its flags in
// the fixed header, after what dst holds.
func TestPublish(t *testing.T) {
	testRoundTrips(t, casesOf(validPublishes, (*publishSpec).caseOf))
}

// TestPublishFlags tries the flag values in both versions, the body holding
// the fields the QoS announces; QoS 2's are checked on the wire, where a
// client sends them (conformance: the tests of incoming QoS 2, qos2_test.go).
// Flags that break no rule decode to their fields. Flags that break one are
// refused by the decoder:
//   - QoS 3, MQTT 5 3.3.1.2: "A PUBLISH Packet MUST NOT have both QoS bits
//     set to 1 [MQTT-3.3.1-4]. If a Server or Client receives a PUBLISH
//     packet which has both QoS bits set to 1 it is a Malformed Packet."
//     MQTT 3.1.1 [MQTT-3.3.1-4]: "it MUST close the Network Connection".
//   - DUP at QoS 0, both versions, 3.3.1.1: "The DUP flag MUST be set to 0
//     for all QoS 0 messages [MQTT-3.3.1-2]." No class is named.
func TestPublishFlags(t *testing.T) {
	for _, version := range bothVersions {
		for flags := range byte(16) {
			s := publishSpec{
				version: version, dup: flags&dupFlag != 0, qos: flags >> qosShift & 3, retain: flags&retainFlag != 0,
				topic: "a/b", payload: []byte("p"),
			}
			if s.qos == 2 {
				continue
			}
			if s.qos != 0 {
				s.id = 10
			}
			body, want := s.body(), s.want()
			got, err := packet.DecodePublish(body, flags, version)
			qos3, dup0 := s.qos == 3, s.dup && s.qos == 0
			if !qos3 && !dup0 {
				if err != nil || !samePublish(&got, &want) {
					t.Errorf("MQTT %v, flags %04b: got %+v, %v; want %+v", version, flags, got, err, want)
				}
				continue
			}
			switch {
			case err == nil:
				t.Errorf("MQTT %v, flags %04b: accepted as %+v", version, flags, got)
			case version == packet.V5 && qos3 && !errors.Is(err, packet.ErrMalformed):
				t.Errorf("MQTT 5, flags %04b: err = %v, want ErrMalformed", flags, err)
			case !isKind(err):
				t.Errorf("MQTT %v, flags %04b: err = %v, want ErrMalformed or ErrProtocol", version, flags, err)
			}
		}
	}
}

// TestPublishPacketIdentifier: at QoS 1 and 2 the Packet Identifier is not
// zero. MQTT 5 2.2.1: a new PUBLISH (QoS > 0) "MUST assign it a non-zero
// Packet Identifier" [MQTT-2.2.1-3] (Client), [MQTT-2.2.1-4] (Server); MQTT
// 3.1.1 2.3.1: "PUBLISH (in cases where QoS > 0) Control Packets MUST
// contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]". No class is
// named. At QoS 0 there is none: "A PUBLISH packet MUST NOT contain a
// Packet Identifier if its QoS value is set to 0" (MQTT 5 [MQTT-2.2.1-2],
// MQTT 3.1.1 [MQTT-2.3.1-5]); validPublishes holds the QoS 0 bodies the
// decoder reads without one.
func TestPublishPacketIdentifier(t *testing.T) {
	for _, version := range bothVersions {
		for _, qos := range []byte{1, 2} {
			s := publishSpec{version: version, qos: qos, topic: "a/b", payload: []byte("p")}
			s.codec().readRefused(t, fmt.Sprintf("QoS %d, Packet Identifier 0", qos), version, s.body(), nil)
		}
	}
}

// TestPublishTopicNameString: the Topic Name "MUST be a UTF-8 Encoded
// String as defined in section 1.5.4 [MQTT-3.3.2-1]" (MQTT 3.1.1: 1.5.3).
// MQTT 5 1.5.4: ill-formed UTF-8 or an encoded surrogate [MQTT-1.5.4-1], or
// U+0000 [MQTT-1.5.4-2], "is a Malformed Packet"; MQTT 3.1.1 [MQTT-1.5.3-1],
// [MQTT-1.5.3-2]: "it MUST close the Network Connection". The encoder
// writes the Topic Name it is given, from a checked decode or a stored
// record (TestAppendPublishWritesAsGiven).
func TestPublishTopicNameString(t *testing.T) {
	bad := slices.Concat(illFormed, surrogates, nulls)
	for _, version := range bothVersions {
		for _, topic := range bad {
			for _, qos := range []byte{0, 1} {
				s := publishSpec{version: version, qos: qos, topic: string(topic), id: uint16(qos), payload: []byte("p")}
				body := s.body()
				got, err := packet.DecodePublish(body, s.flags(), version)
				switch {
				case version == packet.V5 && !errors.Is(err, packet.ErrMalformed):
					t.Errorf("MQTT 5, QoS %d, Topic Name % X: got %+v, %v; want ErrMalformed", qos, topic, got, err)
				case !isKind(err):
					t.Errorf("MQTT %v, QoS %d, Topic Name % X: got %+v, %v; want ErrMalformed or ErrProtocol", version, qos, topic, got, err)
				}
			}
		}
	}
}

// refusedPublishProps are PUBLISH property sets the standard forbids,
// beyond publishValueRules.
var refusedPublishProps = []propertyCase{
	// design.md, Choices MQTT leaves open: "Payload Format Indicator or
	// Message Expiry Interval twice in one PUBLISH, which MQTT 5 does not
	// forbid | a Protocol Error, as for every other property that may
	// appear once".
	{"Payload Format Indicator twice", []wired{intProp(0x01, 0), intProp(0x01, 0)}, packet.ErrProtocol},
	{"Message Expiry Interval twice", []wired{intProp(0x02, 1), intProp(0x02, 1)}, packet.ErrProtocol},
	// 3.3.2.3.4: "It is a Protocol Error to include the Topic Alias value
	// more than once."
	{"Topic Alias twice", []wired{intProp(0x23, 1), intProp(0x23, 2)}, packet.ErrProtocol},
	// 3.3.2.3.5: "It is a Protocol Error to include the Response Topic more
	// than once."
	{"Response Topic twice", []wired{stringProp(0x08, "r"), stringProp(0x08, "r")}, packet.ErrProtocol},
	// 3.3.2.3.6: "It is a Protocol Error to include Correlation Data more
	// than once."
	{"Correlation Data twice", []wired{binaryProp(0x09, []byte{1}), binaryProp(0x09, []byte{1})}, packet.ErrProtocol},
	// 3.3.2.3.9: "It is a Protocol Error to include the Content Type more
	// than once."
	{"Content Type twice", []wired{stringProp(0x03, "a"), stringProp(0x03, "b")}, packet.ErrProtocol},
	// 2.2.2.2: "A Control Packet which contains an Identifier which is not
	// valid for its packet type ... is a Malformed Packet." Table 2-4 gives
	// Reason String to the acknowledgements, CONNACK, SUBACK, UNSUBACK,
	// DISCONNECT and AUTH; Session Expiry Interval to CONNECT, CONNACK and
	// DISCONNECT; Will Delay Interval to the Will Properties alone.
	{"Reason String, an acknowledgement property", []wired{stringProp(0x1F, "r")}, packet.ErrMalformed},
	{"Session Expiry Interval, a CONNECT property", []wired{intProp(0x11, 1)}, packet.ErrMalformed},
	{"Will Delay Interval, a Will property", []wired{intProp(0x18, 1)}, packet.ErrMalformed},
	// [MQTT-3.3.2-13]: "The Response Topic MUST be a UTF-8 Encoded String
	// as defined in section 1.5.4"; [MQTT-3.3.2-19], the Content Type
	// likewise; a User Property's strings [MQTT-1.5.7-1]. 1.5.4: U+0000
	// [MQTT-1.5.4-2], ill-formed UTF-8 or a surrogate [MQTT-1.5.4-1] "is a
	// Malformed Packet".
	{"a Response Topic holding U+0000", []wired{stringProp(0x08, "a\x00")}, packet.ErrMalformed},
	{"a Content Type, ill-formed", []wired{stringProp(0x03, "\xc0\x80")}, packet.ErrMalformed},
	{"a User Property value holding a surrogate", []wired{userProp("n", "\xed\xa0\x80")}, packet.ErrMalformed},
}

// TestPublishProperties checks the rules of 3.3.2.3 in both directions
// (publishValueRules, refusedPublishProps): the decoder refuses with the
// class the section names (isKind where it names none), and NewProperties
// refuses to make the section, with ErrCannotEncode and no section. The
// values the rules allow decode, are made into exactly their wire bytes,
// and write back. The encoder writes the section it is given, checked when
// made (TestAppendPublishWritesAsGiven).
func TestPublishProperties(t *testing.T) {
	base := publishSpec{version: packet.V5, qos: 1, topic: "a/b", id: 1, payload: []byte("p")}
	testProperties(t, base.codec(), propsCases(publishValueRules, refusedPublishProps), func(c *propsCase) ([]byte, packet.Publish) {
		s := base
		s.props = c.props
		return s.body(), s.want()
	})
}

// TestAppendPublishWritesAsGiven guards the busy path's standard
// (design.md, Testing: "It does no needless work. It makes no second pass
// over the same data"; the maintainer's decision on 261013-024a). A Topic
// Name comes from a checked decode or a stored record, and Properties are
// checked when made, so AppendPublish writes both byte for byte without
// examining them. Each Topic Name and section here is one only a check would
// refuse: an empty Topic Name without a Topic Alias (3.3.2.1), an ill-formed
// one ([MQTT-3.3.2-1]), value rules broken (publishValueRules), a property
// repeated, misplaced, unknown or cut short. Such values can reach the
// encoder only through a bug elsewhere, which is where they are caught.
func TestAppendPublishWritesAsGiven(t *testing.T) {
	topics := []string{"", "\xC0\x80", "a\x00", "\xED\xA0\x80"}
	sections := [][]byte{
		nil,
		{0x01, 0x02, 0x23, 0x00, 0x00, 0x0B, 0x00}, // Payload Format Indicator 2, Topic Alias 0, Subscription Identifier 0
		{0x23, 0x00, 0x01, 0x23, 0x00, 0x02},       // Topic Alias twice
		{0x1F, 0x00, 0x01, 'r'},                    // Reason String, not a PUBLISH property
		{0x03, 0x00, 0x02, 0xC0, 0x80},             // Content Type, ill-formed
		{0x7F},                                     // an identifier Table 2-4 lacks
		{0x26, 0x00},                               // a User Property cut short
	}
	payload := []byte("p")
	for _, topic := range topics {
		for _, qos := range []byte{0, 1} {
			head := prefixed([]byte(topic))
			if qos != 0 {
				head = binary.BigEndian.AppendUint16(head, 1)
			}
			first := byte(packet.PUBLISH)<<4 | qos<<qosShift
			for _, section := range sections {
				p := packet.Publish{QoS: qos, Topic: []byte(topic), PacketID: uint16(qos), Properties: section, Payload: payload}
				wire := packetWith(first, slices.Concat(head, binary.AppendUvarint(nil, uint64(len(section))), section, payload))
				if out, err := packet.AppendPublish(nil, packet.V5, &p); err != nil || !bytes.Equal(out, wire) {
					t.Errorf("AppendPublish, MQTT 5, QoS %d, Topic Name % X, Properties % X: wrote % X, %v; want % X", qos, topic, section, out, err, wire)
				}
			}
			p := packet.Publish{QoS: qos, Topic: []byte(topic), PacketID: uint16(qos), Payload: payload}
			wire := packetWith(first, slices.Concat(head, payload))
			if out, err := packet.AppendPublish(nil, packet.V311, &p); err != nil || !bytes.Equal(out, wire) {
				t.Errorf("AppendPublish, MQTT 3.1.1, QoS %d, Topic Name % X: wrote % X, %v; want % X", qos, topic, out, err, wire)
			}
		}
	}
}

// TestPublishTruncated cuts the variable header of each of validPublishes
// short at every byte: the rest cannot be parsed into the fields of 3.3.2,
// a Malformed Packet by MQTT 5 1.2's definition, as connect_test.go
// applies it to both versions. The payload is whatever follows the
// variable header (3.3.3), so a body cut there is still whole. A Property
// Length of five bytes cannot be a Variable Byte Integer (1.5.5).
func TestPublishTruncated(t *testing.T) {
	type cut struct {
		name    string
		version packet.Version
		flags   byte
		body    []byte
	}
	cuts := []cut{
		{"a five-byte Property Length", packet.V5, 0, []byte{0x00, 0x01, 't', 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
		{"a Topic Name length past the end", packet.V311, 0, []byte{0x00, 0x05, 't'}},
	}
	for i := range validPublishes {
		s := &validPublishes[i]
		if len(s.topic) >= 256 {
			continue // a long Topic Name is cut as a short one is
		}
		body := s.body()
		for _, short := range cutsOf(body[:len(body)-len(s.payload)], 0) {
			cuts = append(cuts, cut{fmt.Sprintf("%s, cut to %d bytes", s.name, len(short)), s.version, s.flags(), short})
		}
	}
	for _, c := range cuts {
		publishCodecs[c.flags].readRefused(t, c.name, c.version, c.body, packet.ErrMalformed)
	}
}

// FuzzDecodePublish feeds random bytes and flags to DecodePublish under both
// versions, through the shared body (codec.fuzzDecode). It must never panic; it
// refuses with a packet error; what it accepts obeys publishOK and writes back
// as exactly the packet it came from, a PUBLISH having one form (design.md,
// Testing: "Code that reads bytes or text from outside Sagüin is fuzzed,
// whether the input comes from clients, a bridge's peer or the operator's
// files").
func FuzzDecodePublish(f *testing.F) {
	for i := range validPublishes {
		s := &validPublishes[i]
		if len(s.topic) < 256 {
			f.Add(s.body(), s.flags(), s.version == packet.V5)
		}
	}
	f.Add([]byte{0x00, 0x01, 't', 0x00, 0x0A}, byte(0x08), false)
	f.Add([]byte{0x00, 0x01, 't', 0x00, 0x0A, 0x00}, byte(0x06), true)
	f.Add([]byte{0x00, 0x01, 't', 0x00, 0x00, 0x00}, byte(0x02), true)
	f.Add([]byte{0x00, 0x00, 0x00}, byte(0x00), true)
	f.Add([]byte{0x00, 0x00, 0x03, 0x23, 0x00, 0x00}, byte(0x00), true)
	f.Add([]byte{0x00, 0x01, 't', 0x02, 0x0B, 0x00}, byte(0x00), true)
	f.Add([]byte{0x00, 0x01, 't', 0x02, 0x01, 0x02}, byte(0x00), true)
	f.Add([]byte{0x00, 0x01, 't', 0x04, 0x01, 0x00, 0x01, 0x00}, byte(0x01), true)
	f.Add([]byte{0x00, 0x02, 0xC0, 0x80, 0x00}, byte(0x00), true)
	f.Fuzz(func(t *testing.T, data []byte, flags byte, v5 bool) {
		publishCodecs[flags&0x0F].fuzzDecode(t, versionOf(v5), data)
	})
}
