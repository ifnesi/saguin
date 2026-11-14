package harness

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// cat joins byte pieces, so a vector reads as the standard's figures do.
func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// TestPacketGolden encodes packets built from the standards' non-normative
// examples and compares every byte; then decodes those bytes back to the same
// packet. Where a figure gives only the variable header or the payload, the
// fixed header is the type's first byte (3.1.1 §2.2, 5.0 §2.1) and the
// Remaining Length counted by hand.
func TestPacketGolden(t *testing.T) {
	mqtt := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}
	// Connect Flags 0xCE: User Name, Password, Will QoS 1, Will Flag, Clean
	// Start (3.1.1 Figure 3.6, 5.0 Figure 3-6).
	will := &Will{QoS: 1, Topic: "w", Payload: []byte("p")}
	payload := cat([]byte{0, 1, 'c'}, []byte{0, 1, 'w'}, []byte{0, 1, 'p'}, []byte{0, 1, 'u'}, []byte{0, 1, 'p'})
	ab := []byte{0x00, 0x03, 'a', '/', 'b'}
	cd := []byte{0x00, 0x03, 'c', '/', 'd'}

	cases := []struct {
		name    string
		version byte
		p       Packet
		want    []byte
	}{
		{"CONNECT 3.1.1 §3.1.2 Figure 3.6", V311,
			&Connect{ProtocolName: "MQTT", Version: V311, CleanStart: true, KeepAlive: 10, ClientID: "c", Will: will,
				HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")},
			cat([]byte{0x10, 25}, mqtt, []byte{0x04, 0xCE, 0x00, 0x0A}, payload)},
		{"CONNECT 5.0 §3.1.2 Figure 3-6, Session Expiry Interval 10", V5,
			&Connect{ProtocolName: "MQTT", Version: V5, CleanStart: true, KeepAlive: 10,
				Props:    Properties{{ID: SessionExpiryInterval, Int: 10}},
				ClientID: "c", Will: will, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")},
			// The Will Properties' length, 0, comes before the Will Topic (5.0 §3.1.3.2).
			cat([]byte{0x10, 32}, mqtt, []byte{0x05, 0xCE, 0x00, 0x0A, 0x05, 0x11, 0x00, 0x00, 0x00, 0x0A},
				payload[:3], []byte{0x00}, payload[3:])},
		{"PUBLISH 3.1.1 §3.3.2 Figure 3.11, a/b id 10", V311,
			&Publish{QoS: 1, Topic: "a/b", ID: 10},
			cat([]byte{0x32, 7}, ab, []byte{0x00, 0x0A})},
		{"PUBLISH 5.0 §3.3.2 Figure 3-9, no properties", V5,
			&Publish{QoS: 1, Topic: "a/b", ID: 10},
			cat([]byte{0x32, 8}, ab, []byte{0x00, 0x0A, 0x00})},
		{"SUBSCRIBE 3.1.1 §3.8.2-3 Figures 3.21, 3.23", V311,
			&Subscribe{ID: 10, Subs: []Subscription{{Filter: "a/b", QoS: 1}, {Filter: "c/d", QoS: 2}}},
			cat([]byte{0x82, 14, 0x00, 0x0A}, ab, []byte{1}, cd, []byte{2})},
		{"SUBSCRIBE 5.0 §3.8.2-3 Figures 3-19, 3-21", V5,
			&Subscribe{ID: 10, Subs: []Subscription{{Filter: "a/b", QoS: 1}, {Filter: "c/d", QoS: 2}}},
			cat([]byte{0x82, 15, 0x00, 0x0A, 0x00}, ab, []byte{1}, cd, []byte{2})},
		{"SUBACK 3.1.1 §3.9.3 Figure 3.27: QoS 0, QoS 2, failure", V311,
			&Suback{ID: 10, Reasons: []byte{0x00, 0x02, 0x80}},
			[]byte{0x90, 5, 0x00, 0x0A, 0x00, 0x02, 0x80}},
		{"UNSUBSCRIBE 3.1.1 §3.10.3 Figure 3.30", V311,
			&Unsubscribe{ID: 10, Filters: []string{"a/b", "c/d"}},
			cat([]byte{0xA2, 12, 0x00, 0x0A}, ab, cd)},
		{"CONNACK 3.1.1 §3.2.1: 0x20, length 2", V311, &Connack{}, []byte{0x20, 0x02, 0x00, 0x00}},
		{"PUBACK 3.1.1 §3.4.1: 0x40, length 2", V311, &Ack{Kind: PUBACK, ID: 10}, []byte{0x40, 0x02, 0x00, 0x0A}},
		{"PUBREL 3.1.1 §3.6.1: flags 0010", V311, &Ack{Kind: PUBREL, ID: 10}, []byte{0x62, 0x02, 0x00, 0x0A}},
		{"PINGREQ 3.1.1 §3.12.1", V311, &Pingreq{}, []byte{0xC0, 0x00}},
		{"PINGRESP 3.1.1 §3.13.1", V311, &Pingresp{}, []byte{0xD0, 0x00}},
		{"DISCONNECT 3.1.1 §3.14.1", V311, &Disconnect{}, []byte{0xE0, 0x00}},
		{"PUBACK 5.0 §3.4.2.1: success, no properties, length 2", V5, &Ack{Kind: PUBACK, ID: 10}, []byte{0x40, 0x02, 0x00, 0x0A}},
		{"PUBACK 5.0 §3.4.2.2.1: a reason, no Property Length, length 3", V5,
			&Ack{Kind: PUBACK, ID: 10, Reason: 0x10}, []byte{0x40, 0x03, 0x00, 0x0A, 0x10}},
		{"DISCONNECT 5.0 §3.14.2.1: 0x00, no properties, length 0", V5, &Disconnect{}, []byte{0xE0, 0x00}},
		// Figure 3-24 labels the Property Length "Length (5)" but draws its
		// bits as 00000111 (7). 5 is right: the property is its identifier
		// byte and a Four Byte Integer (§2.2.2.2, Table 2-4).
		{"DISCONNECT 5.0 §3.14.2 Figure 3-24, Session Expiry Interval 0", V5,
			&Disconnect{Props: Properties{{ID: SessionExpiryInterval, Int: 0}}},
			[]byte{0xE0, 7, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, c := range cases {
		got, err := Encode(c.p, c.version)
		if err != nil {
			t.Errorf("%s: encode: %v", c.name, err)
			continue
		}
		if !bytes.Equal(got, c.want) {
			t.Errorf("%s:\nencoded % X\nwant    % X", c.name, got, c.want)
		}
		back, err := Decode(c.want, c.version)
		if err != nil {
			t.Errorf("%s: decode: %v", c.name, err)
			continue
		}
		if !reflect.DeepEqual(back, c.p) {
			t.Errorf("%s: decoded %+v, want %+v", c.name, back, c.p)
		}
	}

	// The Variable Byte Integer against the standards' own bytes: the
	// boundaries of MQTT 5.0 §1.5.5 Table 1-1 (3.1.1 §2.2.3 Table 2.4, the
	// same numbers) and 3.1.1 §2.2.3's worked examples, 64 and 321.
	for _, c := range []struct {
		v    uint32
		want []byte
	}{
		{0, []byte{0x00}},                                     // Table 1-1, one byte, from
		{127, []byte{0x7F}},                                   // one byte, to
		{128, []byte{0x80, 0x01}},                             // two bytes, from
		{16_383, []byte{0xFF, 0x7F}},                          // two bytes, to
		{16_384, []byte{0x80, 0x80, 0x01}},                    // three bytes, from
		{2_097_151, []byte{0xFF, 0xFF, 0x7F}},                 // three bytes, to
		{2_097_152, []byte{0x80, 0x80, 0x80, 0x01}},           // four bytes, from
		{268_435_455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},         // four bytes, to
		{64, []byte{0x40}},                                    // 3.1.1 §2.2.3: "encoded as a single byte ... 0x40"
		{321, []byte{0xC1, 0x02}},                             // 3.1.1 §2.2.3: 65+128 = 193, then 2
		{MaxVarInt + 1, []byte{0x80, 0x80, 0x80, 0x80, 0x01}}, // the forbidden five-byte form, for malformed tests
	} {
		if got := AppendVarInt(nil, c.v); !bytes.Equal(got, c.want) {
			t.Errorf("AppendVarInt(%d) = % X, want % X", c.v, got, c.want)
		}
		if c.v > MaxVarInt {
			continue
		}
		if v, err := ReadVarInt(bytes.NewReader(c.want)); err != nil || v != c.v {
			t.Errorf("ReadVarInt(% X) = %d, %v; want %d", c.want, v, err, c.v)
		}
	}

	// The UTF-8 Encoded String example: "A" followed by U+2A6D4 (MQTT 5.0
	// §1.5.4, Figure 1-2; MQTT 3.1.1 §1.5.3.1, Figure 1.2).
	example := []byte{0x00, 0x05, 0x41, 0xF0, 0xAA, 0x9B, 0x94}
	if got := AppendString(nil, "A\U0002A6D4"); !bytes.Equal(got, example) {
		t.Errorf("AppendString = % X, want % X", got, example)
	}
	d := &decoder{b: example}
	if got := d.string(); got != "A\U0002A6D4" || d.err != nil || len(d.b) != 0 {
		t.Errorf("decoded %q, %v, %d left; want %q", got, d.err, len(d.b), "A\U0002A6D4")
	}
}

// everyProperty holds one property of each data type (5.0 §2.2.2.2), User
// Properties repeated, with a name used twice, in the order written.
var everyProperty = Properties{
	{ID: PayloadFormatIndicator, Int: 1},
	{ID: MessageExpiryInterval, Int: 0xFFFFFFFF},
	{ID: ContentType, Str: "text/plain"},
	{ID: ResponseTopic, Str: "reply/to"},
	{ID: CorrelationData, Data: []byte{0, 1, 2}},
	{ID: SubscriptionIdentifier, Int: 268_435_455},
	{ID: TopicAlias, Int: 0xFFFF},
	User("saguin-id", "0192"),
	User("k", "v1"),
	User("k", "v2"),
}

// TestRoundTrip encodes each packet and decodes it back, in each version
// where it exists.
func TestRoundTrip(t *testing.T) {
	both := []Packet{
		&Connack{SessionPresent: true, Reason: 0x05},
		&Publish{Topic: "t", Payload: []byte("q0")},
		&Publish{Dup: true, QoS: 2, Retain: true, Topic: "a/b/c", ID: 65535, Payload: bytes.Repeat([]byte{0xAB}, 200)},
		&Ack{Kind: PUBREC, ID: 1},
		&Ack{Kind: PUBCOMP, ID: 2},
		&Subscribe{ID: 3, Subs: []Subscription{{Filter: "#", QoS: 0}, {Filter: "a/+/b", QoS: 2}}},
		&Suback{ID: 3, Reasons: []byte{0, 1, 2, 0x80}},
		&Unsubscribe{ID: 4, Filters: []string{"x"}},
		&Unsuback{ID: 4},
		&Pingreq{}, &Pingresp{}, &Disconnect{},
	}
	v5 := []Packet{
		&Connect{ProtocolName: "MQTT", Version: V5, KeepAlive: 60, ClientID: "",
			Props: Properties{{ID: ReceiveMaximum, Int: 20}, {ID: MaximumPacketSize, Int: 1 << 20}, User("a", "b")},
			Will: &Will{QoS: 2, Retain: true, Topic: "will",
				Props: Properties{{ID: WillDelayInterval, Int: 30}, User("w", "x")}},
			HasPassword: true, Password: []byte{0xFF}},
		&Connack{Reason: 0x00, Props: Properties{{ID: AssignedClientIdentifier, Str: "auto-1"},
			{ID: ServerKeepAlive, Int: 30}, {ID: MaximumQoS, Int: 1}, {ID: TopicAliasMaximum, Int: 16}}},
		&Publish{QoS: 1, Topic: "a", ID: 9, Props: everyProperty, Payload: []byte("x")},
		&Ack{Kind: PUBACK, ID: 7, Reason: 0x97, Props: Properties{{ID: ReasonString, Str: "full"}}},
		&Ack{Kind: PUBREL, ID: 8, Reason: 0x92},
		&Subscribe{ID: 5, Props: Properties{{ID: SubscriptionIdentifier, Int: 42}, User("saguin-filter", "topic_hash(4, 1)")},
			Subs: []Subscription{{Filter: "$share/g/a/#", QoS: 1, NoLocal: true, RetainAsPublished: true, RetainHandling: 2}}},
		&Suback{ID: 5, Props: Properties{{ID: ReasonString, Str: "r"}}, Reasons: []byte{0x83}},
		&Unsuback{ID: 6, Props: Properties{User("saguin-unread", "saguin-x")}, Reasons: []byte{0x00, 0x11}},
		&Disconnect{Reason: 0x8E},
		&Disconnect{Reason: 0x04, Props: Properties{{ID: SessionExpiryInterval, Int: 5}, {ID: ServerReference, Str: "other"}}},
		&Auth{}, &Auth{Reason: 0x18, Props: Properties{{ID: AuthenticationMethod, Str: "m"}, {ID: AuthenticationData, Data: []byte("d")}}},
	}
	// A UTF-8 Encoded String and Binary Data each hold at most 65,535
	// bytes (MQTT 5.0 §1.5.4, §1.5.6; 3.1.1 §1.5.3, §3.1.3.5): the longest
	// is written, not refused.
	longest := strings.Repeat("x", 0xFFFF)
	v311 := []Packet{
		&Connect{ProtocolName: "MQTT", Version: V311, CleanStart: true, ClientID: "id", HasUsername: true, Username: "user"},
		&Connect{ProtocolName: "MQTT", Version: V311, ClientID: "id",
			HasUsername: true, Username: longest, HasPassword: true, Password: []byte(longest)},
	}
	run := func(version byte, ps []Packet) {
		for _, p := range ps {
			b, err := Encode(p, version)
			if err != nil {
				t.Errorf("v%d %T: encode: %v", version, p, err)
				continue
			}
			back, err := Decode(b, version)
			if err != nil {
				t.Errorf("v%d %T: decode % X: %v", version, p, b, err)
				continue
			}
			if !reflect.DeepEqual(back, p) {
				t.Errorf("v%d: round trip\n got %+v\nwant %+v", version, back, p)
			}
		}
	}
	run(V311, both)
	run(V5, both)
	run(V5, v5)
	run(V311, v311)
}

// TestBadPacketsRefused: the harness refuses each packet of BadPackets it
// can judge alone, with the row's kind (MQTT 5 4.13.1; 3.1.1 [MQTT-4.8.0-1]),
// and reads each row marked Reads, which only the connection's state or the
// design makes wrong. A packet over the receiver's limit is refused before
// its body is read (5.0 §3.1.2.11.4, sizes counted whole as §2.1.4 does).
// A packet that is not CONNECT, sent first, is checked as the answer to the
// harness's own CONNECT: "The first packet sent from the Server to the
// Client MUST be a CONNACK Packet" (3.1.1 [MQTT-3.2.0-1]).
func TestBadPacketsRefused(t *testing.T) {
	for _, row := range BadPackets {
		if row.Stage == First && row.Packet[0]>>4 != CONNECT {
			c, wait := pipe(t, row.Version, func(peer *Client) error {
				if _, err := peer.Recv(); err != nil {
					return err
				}
				_, err := peer.Conn.Write(row.Packet)
				return err
			})
			if _, err := c.Connect(&Connect{ClientID: "c", CleanStart: true}); !errors.Is(err, row.Err) {
				t.Errorf("%s: Connect returned %v; want %v", row.Name, err, row.Err)
			}
			wait()
			continue
		}
		r := bytes.NewReader(row.Packet)
		p, err := ReadPacket(r, row.Version, row.Limit())
		switch {
		case row.Reads && (err != nil || r.Len() != 0):
			t.Errorf("%s: read %+v, %v, %d bytes left; want it read whole", row.Name, p, err, r.Len())
		case row.Reads:
		case !errors.Is(err, row.Err):
			t.Errorf("%s: read %+v, %v; want %v", row.Name, p, err, row.Err)
		case errors.Is(row.Err, ErrTooLarge) && r.Len() != len(row.Packet)-headerSize(row.Packet):
			t.Errorf("%s: %d bytes of the body read before the refusal", row.Name, len(row.Packet)-headerSize(row.Packet)-r.Len())
		}
	}
}

// headerSize is the length of p's fixed header: its first byte and a
// Remaining Length ending at the first byte below 0x80.
func headerSize(p []byte) int {
	n := 2
	for p[n-1]&0x80 != 0 {
		n++
	}
	return n
}

// TestEncodeRefuses: what cannot be written at all is an error, not a
// silently wrong packet.
func TestEncodeRefuses(t *testing.T) {
	long := strings.Repeat("x", 0x10000)
	for name, c := range map[string]struct {
		p       Packet
		version byte
	}{
		"a topic over 65,535 bytes": {&Publish{Topic: long}, V311},
		"AUTH in 3.1.1":             {&Auth{}, V311},
		"an Ack of kind PUBLISH":    {&Ack{Kind: PUBLISH, ID: 1}, V311},
		"a byte property of 256":    {&Publish{Topic: "t", Props: Properties{{ID: MaximumQoS, Int: 256}}}, V5},
		"an unknown property":       {&Publish{Topic: "t", Props: Properties{{ID: 0x7F}}}, V5},
		// Binary Data is "0 to 65,535 bytes" (MQTT 5.0 §1.5.6; 3.1.1
		// §3.1.3.5, the Password).
		"a password over 65,535 bytes": {&Connect{ProtocolName: "MQTT", Version: V311, ClientID: "c",
			HasUsername: true, Username: "u", HasPassword: true, Password: []byte(long)}, V311},
	} {
		if _, err := Encode(c.p, c.version); err == nil {
			t.Errorf("%s: encoded without error", name)
		}
	}
}

// TestEncodePermits: the encoder writes what a broker must refuse, since
// conformance tests need to send it.
func TestEncodePermits(t *testing.T) {
	b, err := Encode(&Publish{QoS: 3, Topic: "a", ID: 0}, V311)
	if err != nil || b[0] != 0x36 {
		t.Errorf("QoS 3 PUBLISH: % X, %v; want first byte 36", b, err)
	}
	b, err = Encode(&Subscribe{ID: 1, Subs: []Subscription{{Filter: "a", NoLocal: true}}}, V311)
	if err != nil || b[len(b)-1] != 0x04 {
		t.Errorf("3.1.1 SUBSCRIBE with No Local: % X, %v; want option byte 04", b, err)
	}
	if _, err := Decode(b, V311); !errors.Is(err, ErrMalformed) {
		t.Errorf("the same bytes decode as %v, want ErrMalformed", err)
	}
}
