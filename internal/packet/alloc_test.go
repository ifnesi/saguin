package packet_test

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Testing: "The busy path is anything run per packet, per message
// or per connection. It does no needless work. It makes no second pass over
// the same data and avoids every copy or allocation it can." Every packet is
// read through ReadHeader and the integer decoders and written through the
// encoders, so none of them allocates: a decoder reads in place; an encoder
// writes into the caller's buffer when it has room.

func TestDecodersDoNotAllocate(t *testing.T) {
	header := []byte{0x3B, 0xC1, 0x02}
	r := bytes.NewReader(header)
	testkit.NoAllocs(t, "ReadHeader with a reused reader", func() {
		r.Reset(header)
		if _, err := packet.ReadHeader(r, packet.V5, math.MaxInt); err != nil {
			t.Fatal(err)
		}
	})
	two := []byte{0x12, 0x34}
	testkit.NoAllocs(t, "DecodeUint16", func() {
		if _, _, err := packet.DecodeUint16(two); err != nil {
			t.Fatal(err)
		}
	})
	four := []byte{0x01, 0x02, 0x03, 0x04}
	testkit.NoAllocs(t, "DecodeUint32", func() {
		if _, _, err := packet.DecodeUint32(four); err != nil {
			t.Fatal(err)
		}
	})
	varInt := []byte{0xFF, 0xFF, 0xFF, 0x7F}
	testkit.NoAllocs(t, "DecodeVarInt", func() {
		if _, _, err := packet.DecodeVarInt(varInt); err != nil {
			t.Fatal(err)
		}
	})
	none := []byte{0x00, 0xAA}
	testkit.NoAllocs(t, "DecodeProperties with no properties", func() {
		if _, _, err := packet.DecodeProperties(none, packet.PUBLISH); err != nil {
			t.Fatal(err)
		}
	})
	// A CONNECT is decoded once per connection, a CONNACK once per bridge
	// connection. With nothing to keep, an empty client id and no
	// properties, neither allocates.
	for _, level := range []byte{4, 5} {
		connect := (&connectSpec{level: level, flags: cleanStart, keepAlive: 60}).body()
		testkit.NoAllocs(t, "DecodeConnect with nothing to keep", func() {
			if _, err := packet.DecodeConnect(connect); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, c := range validConnacks[:5] {
		testkit.NoAllocs(t, "DecodeConnack, "+c.name, func() {
			if _, err := packet.DecodeConnack(c.body, c.version); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestDecodeConnectAllocations: decoding a CONNECT allocates only what the
// Connect keeps beyond the read buffer: each non-empty string (a string
// cannot share the buffer, which the next packet reuses) and at most one
// for each property section, kept as its wire bytes. Binary Data (the Will
// Payload, the password) is read in place, as DecodeBinary reads it. This
// CONNECT keeps three strings (client id, Will Topic, user name) and two
// property sections: five allocations at most.
//
// The bound holds for the decode and the reading of the property values it
// records (Connect's Session Expiry Interval through HasSaguinProperty)
// together: the decode reads each property once, recording those values
// as it checks the section, so reading them is reading fields, with no
// later pass over the properties. Reading a property section copies
// nothing unless a caller asks for a copy (the maintainer's decision on
// 261013-021a), so the decode allocates nothing per property, however many
// User Properties the CONNECT holds: MQTT 5 3.1.2.11.8, "The User Property
// is allowed to appear multiple times to represent multiple name, value
// pairs." The values are checked once, outside the measure, against the
// properties sent (MQTT 5 3.1.2.11.2 to 3.1.2.11.7, 3.1.2.11.9), so the
// measure is of a decode that records them.
func TestDecodeConnectAllocations(t *testing.T) {
	for _, n := range []int{1, 100, 10_000} {
		users := slices.Repeat([]wired{userProp("key", "value")}, n)
		props := slices.Concat([]wired{
			intProp(0x11, 3600), intProp(0x21, 20), intProp(0x27, 1024), intProp(0x22, 16),
			intProp(0x17, 0), intProp(0x19, 1), stringProp(0x15, "method"), binaryProp(0x16, []byte{0x00, 0xFF}),
			userProp("saguin-unknown", "v"),
		}, users)
		willProps := slices.Concat([]wired{
			intProp(0x01, 1), intProp(0x02, 60), intProp(0x18, 5), stringProp(0x03, "text/plain"),
			stringProp(0x08, "reply"), binaryProp(0x09, []byte("id")),
		}, users)
		s := connectSpec{
			level: 5, flags: userNameFlag | passwordFlag | willFlag | cleanStart, keepAlive: 60,
			props: props, clientID: "client-1",
			willProps: willProps, willTopic: "will/topic", willPayload: []byte("gone"),
			userName: "user", password: []byte("secret"),
		}
		body := s.body()
		c, err := packet.DecodeConnect(body)
		if err != nil {
			t.Fatal(err)
		}
		// Request Problem Information and Request Response Information are
		// sent at the opposite of their defaults (1 and 0: MQTT 5
		// 3.1.2.11.7, 3.1.2.11.6), and a saguin- User Property is sent, so
		// a value the decode failed to record reads as its default and is
		// told apart from the value sent.
		want := recordedValues{3600, 20, 1024, 16, false, true, true, true}
		if got := recordedOf(&c); got != want {
			t.Fatalf("DecodeConnect recorded %+v, want %+v, the properties sent", got, want)
		}
		allocs := testing.AllocsPerRun(100, func() {
			c, err := packet.DecodeConnect(body)
			if err != nil {
				t.Fatal(err)
			}
			recordedSink = recordedOf(&c)
		})
		if allocs > 5 {
			t.Errorf("DecodeConnect and reading the values it records (User Properties: %d in each section) allocate %v times; want at most 5", n, allocs)
		}
	}
}

// recordedValues are the property values a CONNECT's decode records, in
// Connect's order: Session Expiry Interval, Receive Maximum, Maximum Packet
// Size, Topic Alias Maximum, Request Problem Information, Request Response
// Information, an Authentication Method's presence and a saguin- User
// Property's.
type recordedValues struct {
	expiry, receive, packetSize, aliases uint32
	problem, response, auth, saguin      bool
}

// recordedOf reads the values c's decode recorded.
func recordedOf(c *packet.Connect) recordedValues {
	return recordedValues{c.SessionExpiryInterval, uint32(c.ReceiveMaximum), c.MaximumPacketSize, uint32(c.TopicAliasMaximum),
		c.RequestProblemInformation, c.RequestResponseInformation, c.HasAuthenticationMethod, c.HasSaguinProperty}
}

// recordedSink keeps the values the measured decode recorded, as a
// connection keeps those it needs, so the compiler cannot drop the reads.
var recordedSink recordedValues

// propertySink keeps the last value a pass reads, and reservedNames counts
// the saguin- names it finds, so the compiler cannot drop the reads.
var (
	propertySink  packet.PropertyView
	reservedNames int
)

// readEveryProperty is generic coverage of the property iterator: one pass
// over a decoded property section through Properties.Views, each property's
// value read once and each User Property's name checked for the reserved
// saguin- prefix (design, Sagüin properties). It is not the path a reader
// takes, which checks each property through a callback while decoding;
// it holds the iterator any later reader of a kept section uses to the
// same bound.
func readEveryProperty(props packet.Properties) {
	for view := range props.Views() {
		if view.ID == packet.UserProperty && bytes.HasPrefix(view.Name, reservedPrefix) {
			reservedNames++
		}
		propertySink = view
	}
}

// reservedPrefix is the prefix Sagüin reserves on User Property names.
var reservedPrefix = []byte("saguin-")

func TestEncodersDoNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 256)
	h := packet.Header{Type: packet.PUBLISH, Flags: 0b1011, Length: 321}
	testkit.NoAllocs(t, "AppendHeader into a buffer with room", func() {
		if _, err := packet.AppendHeader(buf[:0], h); err != nil {
			t.Fatal(err)
		}
	})
	testkit.NoAllocs(t, "AppendVarInt into a buffer with room", func() {
		if _, err := packet.AppendVarInt(buf[:0], 268_435_455); err != nil {
			t.Fatal(err)
		}
	})
	testkit.NoAllocs(t, "AppendString into a buffer with room", func() {
		if _, err := packet.AppendString(buf[:0], "A\U0002A6D4"); err != nil {
			t.Fatal(err)
		}
	})
	props := []packet.Property{
		{ID: 0x01, Int: 1},
		{ID: 0x02, Int: 0x01020304},
		{ID: 0x03, String: "text/plain"},
		{ID: 0x09, Binary: []byte{0x00, 0xFF}},
		{ID: 0x23, Int: 7},
		{ID: 0x26, Pair: packet.StringPair{Name: "n", Value: "v"}},
		{ID: 0x26, Pair: packet.StringPair{Name: "n", Value: "w"}},
		{ID: 0x0B, Int: 321},
	}
	testkit.NoAllocs(t, "AppendProperties into a buffer with room", func() {
		if _, err := packet.AppendProperties(buf[:0], packet.PUBLISH, props); err != nil {
			t.Fatal(err)
		}
	})
	noAllocsWriting(t, casesOf(validConnects, (*connectSpec).caseOf))
	noAllocsWriting(t, validConnacks)
}

// bytesPerRun returns the heap bytes f allocates, on average over runs
// calls, measured as testing.AllocsPerRun measures allocations: on one
// processor, after a first call that warms up.
func bytesPerRun(runs int, f func()) uint64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	f()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// decodeSlack is what a decode may allocate beyond the bytes it reads: the
// handful of small strings a CONNECT keeps (TestDecodeConnectAllocations),
// whatever the number of properties.
const decodeSlack = 256

// emptyUserProps returns n User Properties with an empty name and value, the
// smallest property a section can repeat: five bytes each (MQTT 5 2.2.2.2,
// 1.5.7).
func emptyUserProps(n int) []wired {
	ws := make([]wired, n)
	for i := range ws {
		ws[i] = userProp("", "")
	}
	return ws
}

// TestDecodedPropertiesCostTheirWireBytes: a decoded packet keeps its
// properties as the bytes they arrived in, checked once while decoding and
// read in place when needed, never unpacked into a value per property (the
// maintainer's decision on 261013-06-GPT, observation 2). What the design
// bounds is wire bytes: a packet's size is checked before its body is read
// (R6) and message bytes in transit are bounded by its two budgets,
// limits.max_incoming_memory and limits.max_outgoing_memory. So
// decoding a legal CONNECT, or a PUBLISH's properties, allocates at most the
// packet's own size and decodeSlack, however many User Properties it holds:
// MQTT 5 2.2.2.2, "The User Property is allowed to appear multiple times".
func TestDecodedPropertiesCostTheirWireBytes(t *testing.T) {
	for _, n := range []int{1, 100, 10_000} {
		s := connectSpec{
			level: 5, flags: cleanStart | willFlag, keepAlive: 60, props: emptyUserProps(n),
			clientID: "c", willProps: emptyUserProps(n), willTopic: "t", willPayload: []byte("p"),
		}
		body := s.body()
		got := bytesPerRun(20, func() {
			if _, err := packet.DecodeConnect(body); err != nil {
				t.Fatal(err)
			}
		})
		if got > uint64(len(body)+decodeSlack) {
			t.Errorf("DecodeConnect of %d bytes, %d User Properties in each section, allocates %d bytes; want at most %d",
				len(body), n, got, len(body)+decodeSlack)
		}

		section := setOf(emptyUserProps(n))
		got = bytesPerRun(20, func() {
			if _, _, err := packet.DecodeProperties(section, packet.PUBLISH); err != nil {
				t.Fatal(err)
			}
		})
		if got > uint64(len(section)+decodeSlack) {
			t.Errorf("DecodeProperties of a PUBLISH section of %d bytes, %d User Properties, allocates %d bytes; want at most %d",
				len(section), n, got, len(section)+decodeSlack)
		}
	}
}

// TestDecodePublishDoesNotAllocate: a PUBLISH is decoded per message, the
// busy path, and keeps nothing beyond the read buffer: its Topic Name,
// Properties and Payload are the buffer's own bytes, so decoding it
// allocates nothing. That holds however many properties it carries
// (the maintainer's decision on 261011-01, 0003c's notes): User Properties
// ride every message, "allowed to appear multiple times" (MQTT 5
// 3.3.2.3.7), and a pass over the decoded section through the property
// iterator, checking each name for the reserved saguin- prefix
// (readEveryProperty), allocates nothing either. Every one of
// validPublishes is held to it too.
func TestDecodePublishDoesNotAllocate(t *testing.T) {
	counts := []int{1, 100, 10_000}
	many := make([]publishSpec, 0, len(counts))
	for _, n := range counts {
		props := slices.Concat(everyPublishProperty, slices.Repeat([]wired{userProp("key", "value")}, n))
		many = append(many, publishSpec{
			name: fmt.Sprintf("%d more User Properties", n), version: packet.V5, qos: 1, topic: "sensors/a/temp", id: 7, props: props, payload: []byte("21.5"),
		})
	}
	noAllocsDecoding(t, casesOf(many, (*publishSpec).caseOf), func(p packet.Publish) { readEveryProperty(p.Properties) })
	noAllocsDecoding(t, casesOf(validPublishes, (*publishSpec).caseOf), func(packet.Publish) {})
}

// TestAppendPublishDoesNotAllocate writes every valid PUBLISH, and one with
// a hundred User Properties, into a buffer with exactly the packet's room.
func TestAppendPublishDoesNotAllocate(t *testing.T) {
	specs := slices.Clone(validPublishes)
	specs = append(specs, publishSpec{
		name: "a hundred User Properties", version: packet.V5, qos: 2, topic: "a/b", id: 1,
		props: slices.Repeat([]wired{userProp("key", "value")}, 100), payload: []byte("p"),
	})
	noAllocsWriting(t, casesOf(specs, (*publishSpec).caseOf))
}

// TestAckCodecDoesNotAllocate: an acknowledgement is decoded and written
// per message at QoS 1 and 2. Decoding keeps nothing beyond the read
// buffer, its properties included, so it allocates nothing; writing into a
// buffer with room allocates nothing. Every one of validAcks, in each form
// TestAckLongerForms reads, is held to it.
func TestAckCodecDoesNotAllocate(t *testing.T) {
	noAllocsDecoding(t, validAcks, func(packet.Ack) {})
	noAllocsWriting(t, validAcks)
	for _, a := range ackTypes {
		for _, body := range [][]byte{{0x00, 0x0A, 0x00}, {0x00, 0x0A, 0x00, 0x00}, {0x00, 0x0A, a.failure, 0x00}} {
			testkit.NoAllocs(t, fmt.Sprintf("DecodeAck, %s, % X", a.name, body), func() {
				if _, err := packet.DecodeAck(body, a.typ, packet.V5); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
