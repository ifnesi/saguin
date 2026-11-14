package conformance

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// clientID is a client id of the test's own: its name, which no other test
// shares, and suffix.
func clientID(t *testing.T, suffix string) string {
	return strings.ReplaceAll(t.Name(), "/", "-") + suffix
}

// pingreq is a PINGREQ's bytes (5.0 §3.12; 3.1.1 §3.12).
var pingreq = []byte{0xC0, 0x00}

// design.md, Flows, CONNECT, step 8: "Once the changes are stored, the broker
// sends CONNACK with every value it capped or chose. These are Server Keep
// Alive, Session Expiry Interval, Receive Maximum
// (qos2.max_inflight_per_client), Assigned Client Identifier, Maximum Packet
// Size (limits.max_packet_size), Topic Alias Maximum (16), and the Maximum
// QoS, Retain Available and Shared Subscription Available the acl_file
// allows". With no acl_file everything is allowed (Access and reload), which
// MQTT 5 says by leaving Retain Available and Shared Subscription Available
// out or 1 (3.2.2.3.5, 3.2.2.3.13), and Maximum QoS out (3.2.2.3.4: "If the
// Maximum QoS is absent, the Client uses a Maximum QoS of 2"; a value other
// than 0 or 1 is "a Protocol Error"). A value the broker kept as
// asked may be left out: "If the Session Expiry Interval is absent the value
// in the CONNECT Packet used" (3.2.2.3.2), and a Server Keep Alive only
// replaces the client's (3.2.2.3.14). The Assigned Client Identifier is
// TestAssignedClientIdentifier's. MQTT 3.1.1's CONNACK has no properties:
// "The Remaining Length field [...] is 2" (3.1.1 §3.2.1).
func TestConnackCarriesEveryValueTheBrokerCappedOrChose(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxInflightPerClient: 7, MaxPacketSize: 4096,
		MaxKeepalive: 30 * time.Second, MaxSessionExpiry: time.Hour})
	chosen := map[byte]uint32{harness.ReceiveMaximum: 7, harness.MaximumPacketSize: 4096, harness.TopicAliasMaximum: 16}
	cases := []struct {
		name      string
		version   byte
		keepAlive uint16
		props     harness.Properties
		capped    map[byte]uint32 // beside chosen: what the broker capped
		kept      map[byte]uint32 // what the CONNACK may repeat as asked
	}{
		{"both over the caps", harness.V5, 60, support.SessionExpiry(0xFFFFFFFF),
			map[byte]uint32{harness.ServerKeepAlive: 30, harness.SessionExpiryInterval: 3600}, nil},
		{"keep-alive 0, capped", harness.V5, 0, nil, map[byte]uint32{harness.ServerKeepAlive: 30}, nil},
		{"both within the caps", harness.V5, 10, support.SessionExpiry(60), nil,
			map[byte]uint32{harness.ServerKeepAlive: 10, harness.SessionExpiryInterval: 60}},
		{"MQTT 3.1.1", harness.V311, 60, nil, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ack := harness.Connected(t, b.Addr, c.version, &harness.Connect{ClientID: clientID(t, ""),
				CleanStart: true, KeepAlive: c.keepAlive, Props: c.props})
			want := maps.Clone(c.capped)
			if want == nil {
				want = map[byte]uint32{}
			}
			if c.version == harness.V5 {
				maps.Copy(want, chosen)
			}
			may := map[byte]uint32{harness.RetainAvailable: 1, harness.SharedSubscriptionAvailable: 1}
			maps.Copy(may, c.kept)
			for _, p := range ack.Props {
				if v, ok := want[p.ID]; ok && v == p.Int {
					delete(want, p.ID)
					continue
				}
				if v, ok := may[p.ID]; !ok || v != p.Int {
					t.Errorf("CONNACK carries property 0x%02X = %d %q, which is neither capped nor chosen", p.ID, p.Int, p.Str)
				}
			}
			for id, v := range want {
				t.Errorf("CONNACK lacks property 0x%02X = %d", id, v)
			}
		})
	}
}

// assigned is an Assigned Client Identifier's form (design.md, CONNECT,
// step 8): twelve digits, YYMMDDHHMMSS, then six letters or digits.
var assigned = regexp.MustCompile(`^\d{12}[0-9A-Za-z]{6}$`)

// design.md, Flows, CONNECT, step 8: "An Assigned Client Identifier is the
// CONNECT's time in UTC as YYMMDDHHMMSS followed by six random letters or
// digits. If a connected or stored session already holds that identifier, it
// is replaced by another before the CONNACK." MQTT 5 3.2.2.3.7: "If the
// Client connects using a zero length Client Identifier, the Server MUST
// respond with a CONNACK containing an Assigned Client Identifier. The
// Assigned Client Identifier MUST be a new Client Identifier not used by any
// other Session currently in the Server [MQTT-3.2.2-16]." MQTT 3.1.1
// [MQTT-3.1.3-6]: a zero-byte ClientId gets "a unique ClientId", which the
// client cannot see; two such clients both stay connected. Ids made in one
// second differ only by their random part, which no client can force onto one
// already held: that case stays inside, in the core's
// TestAssignedClientIdentifierIsTheConnectsTime.
func TestAssignedClientIdentifier(t *testing.T) {
	const many = 32
	clients, ids := make([]*harness.Client, 0, many+2), make([]string, 0, many)
	for range many {
		before := time.Now().UTC().Truncate(time.Second)
		c, ack := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{CleanStart: true, Props: support.Durable})
		after := time.Now().UTC()
		p, ok := ack.Props.Get(harness.AssignedClientIdentifier)
		if !ok {
			t.Fatal("CONNACK to an empty client id carries no Assigned Client Identifier")
		}
		if !assigned.MatchString(p.Str) {
			t.Fatalf("Assigned Client Identifier %q is not YYMMDDHHMMSS and six letters or digits", p.Str)
		}
		at, err := time.Parse("060102150405", p.Str[:12])
		if err != nil || at.Before(before) || at.After(after) {
			t.Errorf("Assigned Client Identifier %q: time %v, %v; want the CONNECT's, between %v and %v", p.Str, at, err, before, after)
		}
		if slices.Contains(ids, p.Str) {
			t.Errorf("Assigned Client Identifier %q given twice", p.Str)
		}
		clients, ids = append(clients, c), append(ids, p.Str)
	}
	for range 2 {
		c, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{CleanStart: true})
		clients = append(clients, c)
	}
	// None took another's session over: each is still connected.
	for i, c := range clients {
		if err := c.Send(&harness.Pingreq{}); err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		harness.Expect[*harness.Pingresp](t, c)
	}
}

// design.md, Flows, CONNECT, step 2: "A failure gets a CONNACK with its
// reason, then the connection closes", with the refusal table's codes, MQTT
// 3.1.1's return codes beside them; the row of 0x95: "none: the connection
// closes" in MQTT 3.1.1. MQTT 5 [MQTT-3.2.2-6] and 3.1.1 [MQTT-3.2.2-4]: a
// refusing CONNACK has Session Present 0. MQTT 5 3.1.4: "If the Server
// rejects the CONNECT, it MUST NOT process any data sent by the Client after
// the CONNECT packet except AUTH packets [MQTT-3.1.4-6]" (3.1.1
// [MQTT-3.1.4-5]): a PINGREQ in the same write is never answered. The
// refusals Sagüin makes today: the acl_file and passwords are not built yet,
// and a full disk (0x97) is the store's (0023c). The Will topic's rows are
// 0x90's: "a Will topic that is not a valid topic name: holding + or #,
// empty, over limits.max_topic_length or limits.max_topic_levels, or under
// $share/ or $saguin/", MQTT 3.1.1's return code 5. They are not the
// malformed CONNECT below the table, which names only "Will's Response Topic
// holds + or #". A Will topic at both limits is accepted:
// TestPingreqBehindTheConnectIsAnsweredAfterTheConnack.
func TestConnectRefusals(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxConnectSize: 64, MaxClientIDLength: 23,
		MaxTopicLength: 16, MaxTopicLevels: 3})
	v5only := support.Launch(t, &harness.Options{MinProtocolVersion: "5"})
	long := strings.Repeat("i", 24)
	big := []byte(strings.Repeat("p", 64))
	// will is a CONNECT in version whose Will topic is topic.
	will := func(version byte, topic string) *harness.Connect {
		return &harness.Connect{Version: version, ClientID: "c", CleanStart: true, Will: &harness.Will{Topic: topic}}
	}
	// Each of 0x90's forms, against b's limits: 16 bytes and 3 levels.
	invalid := []struct{ form, topic string }{
		{"holding +", "a+"},
		{"holding #", "a/#"},
		{"empty", ""},
		{"over max_topic_length", strings.Repeat("t", 17)},
		{"over max_topic_levels", "a/b/c/d"},
		{"under $share/", "$share/g/t"},
		{"under $saguin/", "$saguin/x"},
	}
	type refusal struct {
		name    string
		broker  *harness.Broker
		version byte // the CONNACK's form
		connect *harness.Connect
		reason  byte // 0: no CONNACK, only the close
	}
	// The table's fixed rows, then two per invalid Will topic below.
	cases := slices.Grow([]refusal{
		{"0x84: MQTT 3.1.1 below min_protocol_version", v5only, harness.V311,
			&harness.Connect{Version: harness.V311, ClientID: "c", CleanStart: true}, 1},
		{"an unknown level with the Protocol Name MQTT", b, harness.V311,
			&harness.Connect{Version: 6, ClientID: "c", CleanStart: true}, 1},
		{"MQTT 3.1: MQIsdp, level 3", b, harness.V311,
			&harness.Connect{ProtocolName: "MQIsdp", Version: 3, ClientID: "c", CleanStart: true}, 1},
		{"0x85: a client id over max_client_id_length", b, harness.V5,
			&harness.Connect{Version: harness.V5, ClientID: long, CleanStart: true}, 0x85},
		{"0x85: a client id over max_client_id_length, MQTT 3.1.1", b, harness.V311,
			&harness.Connect{Version: harness.V311, ClientID: long, CleanStart: true}, 2},
		{"0x85: a client id holding a control character", b, harness.V5,
			&harness.Connect{Version: harness.V5, ClientID: "a\x01b", CleanStart: true}, 0x85},
		{"0x85: a client id holding a control character, MQTT 3.1.1", b, harness.V311,
			&harness.Connect{Version: harness.V311, ClientID: "a\x7Fb", CleanStart: true}, 2},
		{"0x85: an empty client id with Clean Start 0", b, harness.V5,
			&harness.Connect{Version: harness.V5}, 0x85},
		{"0x85: an empty client id with Clean Session 0, MQTT 3.1.1 [MQTT-3.1.3-8]", b, harness.V311,
			&harness.Connect{Version: harness.V311}, 2},
		{"0x95: a CONNECT over max_connect_size", b, harness.V5,
			&harness.Connect{Version: harness.V5, ClientID: "c", CleanStart: true, HasPassword: true, Password: big}, 0x95},
		{"0x95: a CONNECT over max_connect_size, MQTT 3.1.1", b, harness.V311,
			&harness.Connect{Version: harness.V311, ClientID: "c", CleanStart: true, HasUsername: true, Username: "u",
				HasPassword: true, Password: big}, 0},
		{"0x83: an unknown saguin- property", b, harness.V5,
			&harness.Connect{Version: harness.V5, ClientID: "c", CleanStart: true,
				Props: harness.Properties{harness.User("saguin-unknown", "v")}}, 0x83},
		{"0x8C: an Authentication Method", b, harness.V5,
			&harness.Connect{Version: harness.V5, ClientID: "c", CleanStart: true,
				Props: harness.Properties{{ID: harness.AuthenticationMethod, Str: "m"}}}, 0x8C},
	}, 2*len(invalid))
	for _, w := range invalid {
		cases = append(cases,
			refusal{"0x90: a Will topic " + w.form, b, harness.V5, will(harness.V5, w.topic), 0x90},
			refusal{"0x90: a Will topic " + w.form + ", MQTT 3.1.1", b, harness.V311, will(harness.V311, w.topic), 5})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.connect.ProtocolName == "" {
				c.connect.ProtocolName = "MQTT"
			}
			refusedConnect(t, c.broker.Addr, c.version, harness.Encoded(t, c.connect, c.version), c.reason)
		})
	}
}

// refusedConnect dials addr in version and writes wire, a CONNECT, with a
// PINGREQ behind it in the same write, then ends its sending; it expects
// CONNACK reason with Session Present 0 (none where reason is 0), the
// PINGREQ unanswered, and the close (TestConnectRefusals).
func refusedConnect(t *testing.T, addr string, version byte, wire []byte, reason byte) {
	t.Helper()
	cl, err := harness.Dial(addr, version)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() }) // the test is over
	if err := cl.SendRaw(append(wire, pingreq...)); err != nil {
		t.Fatal(err)
	}
	if err := cl.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if reason != 0 {
		ack := harness.Expect[*harness.Connack](t, cl)
		if ack.Reason != reason || ack.SessionPresent {
			t.Errorf("CONNACK 0x%02X, Session Present %v; want 0x%02X, Session Present false", ack.Reason, ack.SessionPresent, reason)
		}
	}
	harness.ExpectClosed(t, cl)
}

// connackRefuses dials addr in version and connects with p, keeping its
// sending open, and fails the test unless the CONNACK carries reason with
// Session Present 0 and the connection then closes (design.md, Flows,
// CONNECT, 2: "A failure gets a CONNACK with its reason, then the connection
// closes"; MQTT 5 [MQTT-3.2.2-6], 3.1.1 [MQTT-3.2.2-4]). Unlike
// refusedConnect, the client does not end its sending first, so a CONNECT the
// broker would accept is answered rather than ended as a connection dropped
// before its CONNACK (CONNECT: "an ordinary disconnect").
func connackRefuses(t *testing.T, addr string, version byte, p *harness.Connect, reason byte) {
	t.Helper()
	c, err := harness.Dial(addr, version)
	if err != nil {
		t.Fatal(err)
	}
	connackRefusesOver(t, c, p, reason)
}

// connackRefusesOver is connackRefuses on c, already dialled, over TCP or
// TLS.
func connackRefusesOver(t *testing.T, c *harness.Client, p *harness.Connect, reason byte) {
	t.Helper()
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	ack, err := c.Connect(p)
	if err != nil {
		t.Fatalf("connecting %q: %v", p.ClientID, err)
	}
	if ack.Reason != reason || ack.SessionPresent {
		t.Fatalf("connecting %q: CONNACK 0x%02X, Session Present %v; want 0x%02X, Session Present false",
			p.ClientID, ack.Reason, ack.SessionPresent, reason)
	}
	harness.ExpectClosed(t, c)
}

// design.md, Flows: "A failure ends checking at its stage. A later stage
// never replaces an earlier failure. Policy checks select the first
// applicable reason in the stated order, independently of property order."
// CONNECT: "The order is: [...]; admission reasons 0x85, 0x86, 0x87, 0x90,
// 0x9A, 0x9B, 0x83, 0x8C; then storage refusal 0x97", with the refusal
// table's MQTT 3.1.1 return codes. Each CONNECT fails two admission checks
// and gets CONNACK with the earlier reason, then the close. Only the pair
// carried without properties has an MQTT 3.1.1 form: 3.1.1 has none. The
// package's broker has config.md's max_client_id_length, 256.
func TestConnectAdmissionOrder(t *testing.T) {
	long := strings.Repeat("i", 257)
	reserved := harness.User("saguin-x", "v")
	method := harness.Property{ID: harness.AuthenticationMethod, Str: "m"}
	cases := []struct {
		name    string
		version byte
		connect *harness.Connect
		reason  byte
	}{
		{"0x85 before 0x8C: a client id over max_client_id_length with an Authentication Method", harness.V5,
			&harness.Connect{ClientID: long, CleanStart: true, Props: harness.Properties{method}}, 0x85},
		{"0x85 before 0x90: a client id over max_client_id_length with a Will topic holding #", harness.V5,
			&harness.Connect{ClientID: long, CleanStart: true, Will: &harness.Will{Topic: "w/#"}}, 0x85},
		{"0x85 before 0x90, MQTT 3.1.1: a client id over max_client_id_length with a Will topic holding #", harness.V311,
			&harness.Connect{ClientID: long, CleanStart: true, Will: &harness.Will{Topic: "w/#"}}, 2},
		{"0x90 before 0x83: a Will topic holding # with a saguin- property", harness.V5,
			&harness.Connect{ClientID: "c", CleanStart: true, Props: harness.Properties{reserved},
				Will: &harness.Will{Topic: "w/#"}}, 0x90},
		{"0x83 before 0x8C: a saguin- property, then an Authentication Method", harness.V5,
			&harness.Connect{ClientID: "c", CleanStart: true, Props: harness.Properties{reserved, method}}, 0x83},
		{"0x83 before 0x8C: an Authentication Method, then a saguin- property", harness.V5,
			&harness.Connect{ClientID: "c", CleanStart: true, Props: harness.Properties{method, reserved}}, 0x83},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.connect.ProtocolName, c.connect.Version = "MQTT", c.version
			refusedConnect(t, brokerAddr, c.version, harness.Encoded(t, c.connect, c.version), c.reason)
		})
	}
}

// design.md, Flows, CONNECT: "The broker checks CONNECT in this order: 1. It
// checks for a malformed fixed header or Remaining Length. 2. It checks
// max_connect_size. 3. It checks Protocol Name. 4. It checks Protocol Level
// and min_protocol_version. 5. It checks complete packet validity"; "0x84 |
// an MQTT 3.1.1 client below min_protocol_version | 1"; Flows: "A later stage
// never replaces an earlier failure." MQTT 3.1.1 [MQTT-3.1.2-2]: a Protocol
// Level the Server does not support gets "CONNACK return code 0x01
// (unacceptable protocol level)" and then the disconnect. With
// min_protocol_version 5, an MQTT 3.1.1 CONNECT whose fixed header, Protocol
// Name and level are sound but whose Connect Flags set the reserved bit,
// which makes it malformed (MQTT-3.1.2-3), a failure of complete packet
// validity, gets CONNACK return code 1 and the close, not the close alone.
func TestConnectVersionBeforeValidity(t *testing.T) {
	b := support.Launch(t, &harness.Options{MinProtocolVersion: "5"})
	wire := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V311, ClientID: "c", CleanStart: true}, harness.V311)
	level := bytes.Index(wire, []byte("\x00\x04MQTT\x04"))
	if level < 0 {
		t.Fatalf("an MQTT 3.1.1 CONNECT % X without its Protocol Name and level", wire)
	}
	wire[level+7] |= 0x01 // the Connect Flags' reserved bit, after the level
	refusedConnect(t, b.Addr, harness.V311, wire, 1)
}

// MQTT 5 3.1.4: "Clients are allowed to send further MQTT Control Packets
// immediately after sending a CONNECT packet; Clients need not wait for a
// CONNACK packet to arrive from the Server"; 3.2: "The Server MUST send a
// CONNACK with a 0x00 (Success) Reason Code before sending any Packet other
// than AUTH [MQTT-3.2.0-1]"; MQTT 3.1.1 3.2: "The first packet sent from the
// Server to the Client MUST be a CONNACK Packet [MQTT-3.2.0-1]"; 3.12.4:
// "The Server MUST send a PINGRESP packet in response to a PINGREQ packet"
// [MQTT-3.12.4-1]. A durable session's CONNACK waits for the store
// (design.md, CONNECT, step 8: "Once the changes are stored, the broker sends
// CONNACK"), and the PINGREQ with it. A Will topic at both limits is a valid
// topic name, CONNECT's 0x90 refusing only one "over limits.max_topic_length
// or limits.max_topic_levels": config.md's defaults, 1024 bytes and 200
// levels, on the broker TestMain started.
func TestPingreqBehindTheConnectIsAnsweredAfterTheConnack(t *testing.T) {
	atLimits := strings.Repeat("w/", 199) + strings.Repeat("w", 1024-2*199)
	cases := []struct {
		name    string
		version byte
		connect harness.Connect
	}{
		{"MQTT 5, a session ending with its connection", harness.V5, harness.Connect{CleanStart: true}},
		{"MQTT 5, a durable session", harness.V5, harness.Connect{CleanStart: true, Props: support.Durable}},
		{"MQTT 3.1.1, Clean Session 1", harness.V311, harness.Connect{CleanStart: true}},
		{"MQTT 3.1.1, Clean Session 0", harness.V311, harness.Connect{}},
		{"MQTT 5, a Will topic at max_topic_length and max_topic_levels", harness.V5,
			harness.Connect{CleanStart: true, Will: &harness.Will{Topic: atLimits}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl, err := harness.Dial(brokerAddr, c.version)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cl.Close() }) // the test is over
			c.connect.ProtocolName, c.connect.Version, c.connect.ClientID = "MQTT", c.version, clientID(t, "")
			if err := cl.SendRaw(append(harness.Encoded(t, &c.connect, c.version), pingreq...)); err != nil {
				t.Fatal(err)
			}
			if ack := harness.Expect[*harness.Connack](t, cl); ack.Reason != 0 {
				t.Fatalf("CONNACK 0x%02X, want 0x00", ack.Reason)
			}
			harness.Expect[*harness.Pingresp](t, cl)
		})
	}
}

// design.md, Flows, CONNECT: "Session Present is 1 when the CONNECT keeps its
// session and a session exists for the client id, either away or connected
// with a session expiry above 0. Keeping the session means Clean Start 0, or
// MQTT 3.1.1 Clean Session 0. A takeover of a session whose expiry is 0 ends
// it and starts a new one. Otherwise Session Present is 0." Words: "Durable
// when its expiry is above zero"; an MQTT 3.1.1 Clean Session 0 takes
// max_session_expiry (config.md), Clean Session 1 none. MQTT 5 [MQTT-3.2.2-2]
// and [MQTT-3.2.2-3], and 3.1.2.11.2: with an expiry of 0 "the Session ends
// when the Network Connection is closed"; 3.1.1 [MQTT-3.2.2-1] and
// [MQTT-3.2.2-2], and 3.1.2.4: such a session's state "MUST NOT be reused in
// any subsequent Session" [MQTT-3.1.2-6]. A session that ends with its
// connection cannot be away, and a CONNECT taking it over never keeps it.
func TestSessionPresent(t *testing.T) {
	type before int
	const (
		none             before = iota // no session for the client id
		connected                      // one ending with its connection, connected
		durableAway                    // a durable one, its connection gone
		durableConnected               // a durable one, connected
	)
	cases := []struct {
		version byte
		before  before
		clean   bool
		want    bool
	}{
		{harness.V5, none, false, false}, {harness.V5, none, true, false},
		{harness.V5, connected, false, false}, {harness.V5, connected, true, false},
		{harness.V5, durableAway, false, true}, {harness.V5, durableAway, true, false},
		{harness.V5, durableConnected, false, true}, {harness.V5, durableConnected, true, false},
		{harness.V311, none, false, false}, {harness.V311, none, true, false},
		{harness.V311, connected, false, false}, {harness.V311, connected, true, false},
		{harness.V311, durableAway, false, true}, {harness.V311, durableAway, true, false},
		{harness.V311, durableConnected, false, true}, {harness.V311, durableConnected, true, false},
	}
	names := [...]string{"no session", "a connected session ending with it", "a durable session away", "a durable session connected"}
	for _, c := range cases {
		t.Run(fmt.Sprintf("v%d, %s, clean %v", c.version, names[c.before], c.clean), func(t *testing.T) {
			id := clientID(t, "")
			// A durable session: MQTT 5 by its expiry, MQTT 3.1.1 by Clean
			// Session 0.
			first := &harness.Connect{ClientID: id, CleanStart: true}
			if c.before == durableAway || c.before == durableConnected {
				first.CleanStart = c.version == harness.V5
				if c.version == harness.V5 {
					first.Props = support.Durable
				}
			}
			if c.before != none {
				old, _ := harness.Connected(t, brokerAddr, c.version, first)
				if c.before == durableAway {
					if err := old.Send(&harness.Disconnect{}); err != nil {
						t.Fatal(err)
					}
					harness.ExpectClosed(t, old)
				}
			}
			again := &harness.Connect{ClientID: id, CleanStart: c.clean}
			if c.version == harness.V5 {
				again.Props = support.Durable
			}
			if _, ack := harness.Connected(t, brokerAddr, c.version, again); ack.SessionPresent != c.want {
				t.Errorf("Session Present %v, want %v", ack.SessionPresent, c.want)
			}
		})
	}
}

// MQTT 5 3.1.2.11.4: "The Server MUST NOT send packets exceeding Maximum
// Packet Size to the Client [MQTT-3.1.2-24]"; "Where a Packet is too large
// to send, the Server MUST discard it without sending it [MQTT-3.1.2-25]".
// design.md, DISCONNECT, "Reasons Sagüin sends": "0x80 for a packet it cannot
// build, which is never sent". A CONNACK carries at least Receive Maximum,
// Maximum Packet Size and Topic Alias Maximum (CONNECT, step 8): 16 bytes,
// which a client accepting 10 cannot take; with no CONNACK written, no
// DISCONNECT may follow ("The Server MUST NOT send a DISCONNECT until after
// it has sent a CONNACK with Reason Code of less than 0x80" [MQTT-3.14.0-1]),
// and the connection closes.
func TestConnackTooLargeForTheClientIsNotSent(t *testing.T) {
	c, err := harness.Dial(brokerAddr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	if err := c.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: 10}}}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
}

// design.md, Flows, CONNECT, 7: "The core stores step 6's changes, the
// CONNECT's Will and the session record in one transaction, unless nothing
// changed. Nothing changed when there was no earlier session, there is no
// Will, and the new session ends with its connection"; 8: "Once the changes
// are stored, the broker sends CONNACK"; R4: "A reply confirming a change
// waits for that change's commit"; Words: Session, "what the broker keeps for
// a client id: [...] Will". storage.md, wills: "This table holds a session's
// Will, whether the session is durable or not. The Will is written at its
// CONNECT (R4)": session "the session's id"; user "” for none"; topic, qos
// and retain its Will Topic, QoS and Retain; delay "0 for none"; due "NULL
// while connected"; payload "the Will Payload"; properties "its Will
// Properties", in "their wire form [...] NULL for none" (storage.md, A
// message's properties). MQTT 5 [MQTT-3.1.2-7]: "If the Will Flag is set to 1
// this indicates that a Will Message MUST be stored on the Server and
// associated with the Session"; MQTT 3.1.1 [MQTT-3.1.2-8]: "if the Connect
// request is accepted, a Will Message MUST be stored on the Server".
//
// The broker runs under strace, which notes its locks on the -shm file.
// Clients of both versions, with no user name, their sessions ending with
// their connections or durable, connect with a Will and no Will Delay
// Interval while the test holds the database's write lock, as another
// program on the file may. Once the store writer has met the lock, the
// CONNECT's write is at the store, and the client has read nothing: a
// broker that answered before its commit has written its CONNACK by then
// (alreadySent). Once the lock is released, CONNACK 0x00 comes, and the
// database holds the Will as the CONNECT gave it, a durable session's
// under that session's id.
func TestWillIsStoredBeforeTheConnack(t *testing.T) {
	// alreadySent bounds the read that finds no CONNACK once the store
	// writer has met the lock: a CONNACK handed to the writer before the
	// write was sent is on the loopback socket long before the store writer
	// reaches the lock, strace notes it and the test reads that note.
	const alreadySent = 10 * time.Millisecond
	b := launchTracingLocks(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	props := harness.Properties{{ID: harness.ContentType, Str: "text/plain"}, harness.User("k", "2"), harness.User("k", "1")}
	payload := []byte{0, 1, 0xFE, 0xFF}
	cases := []struct {
		name    string
		version byte
		connect harness.Connect
		durable bool
	}{
		{"MQTT 5, Session Expiry 0", harness.V5,
			harness.Connect{CleanStart: true, Will: &harness.Will{QoS: 1, Topic: "stored/5", Payload: payload, Props: props}}, false},
		{"MQTT 5, durable", harness.V5,
			harness.Connect{CleanStart: true, Props: support.Durable, Will: &harness.Will{Retain: true, Topic: "stored/5/durable", Payload: payload}}, true},
		{"MQTT 3.1.1, Clean Session 1", harness.V311,
			harness.Connect{CleanStart: true, Will: &harness.Will{QoS: 1, Topic: "stored/3.1.1", Payload: payload}}, false},
		{"MQTT 3.1.1, Clean Session 0", harness.V311,
			harness.Connect{Will: &harness.Will{Retain: true, Topic: "stored/3.1.1/durable", Payload: payload}}, true},
	}
	parent := t
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.connect.ProtocolName, c.connect.Version, c.connect.ClientID = "MQTT", c.version, clientID(t, "")
			cl, err := harness.Dial(b.Addr, c.version)
			if err != nil {
				t.Fatal(err)
			}
			// Each client stays connected until the whole test is over: a
			// departure, its Will published, would be a write of its own that
			// the next case's store writer could meet the lock with.
			parent.Cleanup(func() { _ = cl.Close() })
			lock := holdWriteLock(t, b.DB)
			before := len(traced(t, b.trace))
			if err := cl.Send(&c.connect); err != nil {
				t.Fatal(err)
			}
			awaitLockMet(t, b.trace, before, "the CONNECT with a Will")
			nothingWithin(t, cl, alreadySent, "with the write lock held after the CONNECT with a Will")
			lock.release(t)
			if ack := harness.Expect[*harness.Connack](t, cl); ack.Reason != 0 {
				t.Fatalf("CONNACK 0x%02X once the lock is released, want 0x00", ack.Reason)
			}
			got, ok := support.StoredWill(t, db, c.connect.ClientID)
			if !ok {
				t.Fatal("after the CONNACK, its connection open, the database holds no Will for the client")
			}
			w := c.connect.Will
			want := support.Will{Session: got.Session, Topic: w.Topic, Qos: int(w.QoS), Payload: w.Payload, Properties: propertiesOf(t, w.Props)}
			if w.Retain {
				want.Retain = 1
			}
			if c.durable {
				want.Session = sessionID(t, db, c.connect.ClientID)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("the stored Will is %+v; want %+v", got, want)
			}
		})
	}
}

// design.md, Store, A full disk: "a write is answered "full" only when
// SQLite reports lack of space and rollback confirms that the transaction
// did not commit. The broker continues after that answer"; "Changes made by
// CONNECT, SUBSCRIBE or UNSUBSCRIBE are refused with that packet's code";
// Flows, CONNECT: "0x97 | a full disk | 3", and 7: the CONNECT's Will is
// stored, so a session ending with its connection still has a change to store
// when its CONNECT carries a Will. MQTT 5 3.2.2.2, Table 3-1: 0x97, Quota
// exceeded; MQTT 3.1.1 3.2.2.3, Table 3.1: return code 3, Server unavailable.
// MQTT 5 [MQTT-3.1.2-7] and 3.1.1 [MQTT-3.1.2-8]: the Will is stored if the
// CONNECT is accepted.
//
// On a filesystem of its own, with a ballast taking every byte left, an
// MQTT 5 client of Session Expiry 0 and an MQTT 3.1.1 client of Clean
// Session 1 each connect with a Will: CONNACK 0x97, return code 3, and the
// close. Once the ballast is emptied, the database holds
// no Will for either, and the same CONNECTs are accepted.
func TestFullDiskRefusesAConnectWithAWill(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	connects := []struct {
		version byte
		refused byte
	}{{harness.V5, 0x97}, {harness.V311, 3}}
	// connect is version's CONNECT with a Will, its session ending with
	// its connection.
	connect := func(version byte) *harness.Connect {
		return &harness.Connect{ClientID: "will-" + harness.VersionName(version),
			CleanStart: true, Will: &harness.Will{Topic: "full/will", Payload: support.WillPayload}}
	}
	for _, c := range connects {
		connackRefuses(t, b.Addr, c.version, connect(c.version), c.refused)
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	db := support.OpenDB(t, b.DB)
	for _, c := range connects {
		p := connect(c.version)
		if _, ok := support.StoredWill(t, db, p.ClientID); ok {
			t.Errorf("%s: the database holds the Will of the CONNECT refused on a full disk; want nothing stored", harness.VersionName(c.version))
		}
		harness.Connected(t, b.Addr, c.version, p)
	}
}

// design.md, Flows, CONNECT, 5: "A delayed Will is cancelled only by
// reconnection to the still-existing session before its due time, judged when
// the core takes the CONNECT [...] A Will whose client reconnects in time
// waits while that CONNECT is stored. If the store refuses it, the Will keeps
// its due time. It is due at once if that time has passed"; the refusals:
// "0x97 | a full disk", "It applies storage refusal 0x97"; Store, A full
// disk: "Changes made by CONNECT [...] are refused with that packet's code".
// MQTT 5 3.2.2.2, Table 3-1: 0x97, Quota exceeded; 3.1.2.5: the Will "MUST be
// published after the Network Connection is subsequently closed and either
// the Will Delay Interval has elapsed or the Session ends" [MQTT-3.1.2-8].
//
// On a filesystem of its own, an MQTT 5 client of a durable session
// connects with a Will of Will Delay Interval 1 second on a topic a
// subscriber holds, and drops its socket. Once the database gives the Will
// its due time, a ballast takes every byte left, and the client reconnects
// with Clean Start 0 before that time: CONNACK 0x97 and the close. The
// ballast is emptied, still before the due time; the subscriber receives
// the Will no sooner than that time, and the database then holds it no
// more.
func TestRefusedReconnectLeavesTheWillItsDueTime(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	db := support.OpenDB(t, b.DB)
	topic := clientID(t, "/will")
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
	id := clientID(t, "")
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
		Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(1)}})
	support.DropSocket(t, c)
	due := support.AwaitWillDue(t, db, id)
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable}, 0x97)
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if !time.Now().Before(due) {
		t.Fatal("the ballast was emptied after the Will's due time: the test was too slow to show the Will keeping it")
	}
	if arrived := willArrives(t, sub, topic); arrived.Before(due.Add(-tick)) {
		t.Errorf("the Will came %v before its due time; want it at that time, kept by the refused reconnect", due.Sub(arrived))
	}
	awaitWillGone(t, db, id, "its publication")
}

// design.md, Flows, CONNECT: "After a takeover whose CONNECT is then refused,
// the old session stays away with its delayed Will running. The old
// connection's departure is saved on its own, after the refusal. If a full
// disk refuses that save too, the departure is lost, and its Will is due its
// Will Delay after the next start, as for a client connected at a stop"; 5: "An old connection for the id gets DISCONNECT
// 0x8E on MQTT 5 and is closed"; the refusals: "0x97 | a full disk";
// Restart, Restored: "A Will whose client was connected at the stop is due
// its Will Delay Interval after the start, with its session now away
// (MQTT-3.1.2-8)"; Graceful stop: "Once the stop begins, the core's timer
// starts no housekeeping"; Store, Writes: "The core keeps at most one write
// transaction outstanding", and "Sealed groups dispatch in order".
// storage.md, wills: due, "when it is published, once its client is gone;
// NULL while connected". MQTT 5 3.1.2.5: "In the case of a Server shutdown
// or failure, the Server MAY defer publication of Will Messages until a
// subsequent restart"; the Will "MUST be published after the Network
// Connection is subsequently closed and either the Will Delay Interval has
// elapsed or the Session ends" [MQTT-3.1.2-8].
//
// On a filesystem of its own, a subscriber's durable session holds a topic,
// and an MQTT 5 client of a durable session connects with a Will of Will
// Delay Interval 3 seconds on it, and stays connected. A ballast takes every
// byte left, and a takeover of that session, Clean Start 0, is refused,
// CONNACK 0x97; a new client's durable CONNECT, refused the same way after
// it, has every write before it meet the full disk. The ballast is emptied
// and the broker stopped (SIGTERM, exit status 0) before the Will's delay
// has run from the takeover: the database still holds the Will with no due
// time. The broker starts again on the same files, and the subscriber
// resumes: it receives the Will, no sooner than its Will Delay Interval
// after that start began, and the database then holds it no more.
func TestFullDiskTakeoverLeavesTheWillToTheNextStart(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const delay = 3 * time.Second
	o := &harness.Options{FilePath: filepath.Join(disk, "saguin.db")}
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	topic := clientID(t, "/will")
	sub := &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable}
	c := support.Subscribed(t, b.Addr, harness.V5, sub, support.At(0, topic))
	id := clientID(t, "")
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
		Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(uint32(delay / time.Second))}})

	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	took := time.Now() // before the core takes the takeover
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable}, 0x97)
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-after"), CleanStart: true, Props: support.Durable}, 0x97)
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if !time.Now().Before(took.Add(delay)) {
		t.Fatal("the stop came after the Will's delay had run from the takeover: the test was too slow to leave the Will to the next start")
	}
	stopped(t, b)
	shutDown(t, c)
	if w, held := support.StoredWill(t, db, id); !held || w.Due.Valid {
		t.Fatalf("after the stop, the database holds the Will: %v, with due %v; want it held with no due time, the departure's own save refused too",
			held, w.Due)
	}

	restarted := time.Now()
	again := support.Launch(t, o)
	c = resumed(t, again.Addr, harness.V5, sub)
	if arrived := willArrives(t, c, topic); arrived.Before(restarted.Add(delay - tick)) {
		t.Errorf("the Will came %v after the restart began; want its whole Will Delay Interval, %v, after the start", arrived.Sub(restarted), delay)
	}
	awaitWillGone(t, db, id, "its publication")
}

// design.md, Flows, CONNECT: "0x97 | a retained Will on $broadcast that
// alone exceeds retained.max_bytes | 5"; Channels, $broadcast: retained
// values "have their own size limit"; storage.md, A message's bytes: "A
// message's bytes are its topic's, payload's and properties' bytes, the
// payload counted at its size as received. Retention and the size limits
// count those bytes." MQTT 5 3.2.2.2, Table 3-1: 0x97, Quota exceeded; MQTT
// 3.1.1 3.2.2.3, Table 3.1: return code 5, Not authorized. A Will that is not
// retained is no retained value, and one at the limit does not exceed it.
//
// With retained.max_bytes 64, a retained Will of 65 bytes, topic and
// payload, is refused, CONNACK 0x97 and return code 5, then closed; a
// retained Will of 64 bytes and one of 128 not
// retained are accepted.
func TestRetainedWillAloneOverTheRetainedLimitIsRefused(t *testing.T) {
	const limit = 64
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: limit})
	const topic = "big/will"
	// sized is a Will on topic of size bytes, topic and payload together.
	sized := func(retain bool, size int) *harness.Will {
		return &harness.Will{Retain: retain, Topic: topic, Payload: bytes.Repeat([]byte{'w'}, size-len(topic))}
	}
	cases := []struct {
		name    string
		version byte
		will    *harness.Will
		reason  byte // 0: accepted
	}{
		{"MQTT 5, retained, one byte over", harness.V5, sized(true, limit+1), 0x97},
		{"MQTT 3.1.1, retained, one byte over", harness.V311, sized(true, limit+1), 5},
		{"MQTT 5, retained, at the limit", harness.V5, sized(true, limit), 0},
		{"MQTT 3.1.1, not retained, twice the limit", harness.V311, sized(false, 2*limit), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, Will: c.will}
			if c.reason == 0 {
				harness.Connected(t, b.Addr, c.version, p)
				return
			}
			connackRefuses(t, b.Addr, c.version, p, c.reason)
		})
	}
}
