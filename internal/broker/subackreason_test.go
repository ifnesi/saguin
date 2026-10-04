package broker_test

// RFC 0003 "Client-declared partitioning" promises a refused subscriber a
// sentence, not just a code: "Anything else is `0x83` with the reason
// naming the property and stating the form", and "An argument that is not
// a number and one that is too large get different sentences, because they
// send a reader looking in different places."
//
// **Those sentences went only to the broker's log, which the author of the
// refused client cannot read**. A refused
// PUBLISH already carried its sentence on the wire - a catalogue request
// with no Response Topic is answered "a catalogue request needs a Response
// Topic to answer on" - and the difference was an accident of which packet
// the substrate builds rather than a decision.
//
// So these hold the promise as a wire fact. They are written against what
// a client can read, never against the log.

import (
	"bytes"
	"context"
	"maps"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// subSaying subscribes with these User Properties and hands back the SUBACK,
// which is where the client's whole answer is.
//
// **Through connectAsking rather than connect**, because MQTT-3.1.2-29 lets
// a server send no Reason String at all to a client that did not ask for
// problem information - and the ordinary harness client does not ask. A
// test of these sentences written on `connect` reads every one of them as
// absent and blames the broker.
func subSaying(t testing.TB, c *paho.Client, filter string,
	user ...paho.UserProperty) *paho.Suback {
	t.Helper()
	props := &paho.SubscribeProperties{}
	props.User = append(props.User, user...)
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
		Properties:    props,
	})
	if sa == nil {
		t.Fatalf("no SUBACK for %s: %v", filter, err)
	}
	return sa
}

func subackReason(sa *paho.Suback) string {
	if sa.Properties == nil {
		return ""
	}
	return sa.Properties.ReasonString
}

// Every shape of refused SUBSCRIBE says why, and the sentences tell each
// other apart - which is the half of the promise that matters. A single
// sentence for every refusal would satisfy "carries a reason" and send a
// reader looking in the wrong place, which is what the RFC's own paragraph
// about "not a number" and "too large" is written against.
func TestARefusedSubscribeSaysWhyOnTheWire(t *testing.T) {
	h := start(t)
	const events = "events/#"

	for _, tc := range []struct {
		name   string
		filter string
		user   []paho.UserProperty
		code   byte
		says   []string
	}{
		{"a declaration that is not a call", events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "$topic_hash % 3 == 0"}},
			0x83, []string{"saguin-filter", "topic_hash(<partitions>, <index>)"}},
		{"the function name in the wrong case", events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "TOPIC_HASH(3, 0)"}},
			0x83, []string{"saguin-filter", "topic_hash(<partitions>, <index>)"}},
		{"an index at the count", events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "topic_hash(3, 3)"}},
			0x83, []string{"0 to 2"}},
		{"a count above the bound", events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "topic_hash(10000000000000000000000, 0)"}},
			0x83, []string{"2147483647"}},
		{"an argument that is not a number", events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "topic_hash(abc, 0)"}},
			0x83, []string{"saguin-filter"}},
		{"two calls naming different counts", events,
			[]paho.UserProperty{
				{Key: "saguin-filter", Value: "topic_hash(3, 0)"},
				{Key: "saguin-filter", Value: "topic_hash(4, 1)"}},
			0x83, []string{"saguin-filter"}},
		{"a slice on a shared subscription", "$share/g/" + events,
			[]paho.UserProperty{{Key: "saguin-filter", Value: "topic_hash(2, 0)"}},
			0x83, []string{"shared subscription", "saguin-filter"}},
		{"a slice on a queue", "$saguin/queue/jobs",
			[]paho.UserProperty{{Key: "saguin-filter", Value: "topic_hash(2, 0)"}},
			0x83, []string{"queue", "saguin-filter"}},
		{"a spelling in the queue space naming no queue", "$saguin/queue/nope", nil,
			0x8F, []string{"$saguin/queue/"}},
		{"a reserved property saguin does not read", events,
			[]paho.UserProperty{{Key: "saguin-nonesuch", Value: "x"}},
			0x83, []string{"saguin-nonesuch", "reserved"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connectAsking(t, h, "why-"+strings.ReplaceAll(tc.name, " ", "-"))
			sa := subSaying(t, c, tc.filter, tc.user...)
			if sa.Reasons[0] != tc.code {
				t.Fatalf("SUBACK 0x%02X, want 0x%02X", sa.Reasons[0], tc.code)
			}
			said := subackReason(sa)
			if said == "" {
				t.Fatalf("refused 0x%02X with no Reason String: the sentence RFC "+
					"0003 promises reached the broker's log and not the client, "+
					"whose author cannot read it", sa.Reasons[0])
			}
			for _, want := range tc.says {
				if !strings.Contains(said, want) {
					t.Errorf("the reason %q does not name %q", said, want)
				}
			}
		})
	}
}

// **The sentences are different from each other**, counted rather than
// eyeballed: a refusal path that answered every shape with one sentence
// would pass every assertion above.
func TestTheRefusalSentencesTellEachOtherApart(t *testing.T) {
	h := start(t)
	values := []string{
		"$topic_hash % 3 == 0", "topic_hash(3, 3)",
		"topic_hash(10000000000000000000000, 0)", "topic_hash(abc, 0)",
		"topic_hash()", "",
	}
	said := map[string]bool{}
	for i, v := range values {
		c := connectAsking(t, h, "apart-"+string(rune('a'+i)))
		sa := subSaying(t, c, "events/#", paho.UserProperty{Key: "saguin-filter", Value: v})
		if sa.Reasons[0] != 0x83 {
			t.Fatalf("%q answered 0x%02X, want 0x83", v, sa.Reasons[0])
		}
		said[subackReason(sa)] = true
	}
	if len(said) < 4 {
		t.Errorf("%d refusals produced %d distinct sentences: a reader told the "+
			"same thing whatever they got wrong goes looking in the wrong place, "+
			"which is what RFC 0003's \"different sentences\" paragraph is for",
			len(values), len(said))
		for one := range said {
			t.Logf("  %q", one)
		}
	}
}

// **A client cannot dictate the text of its own refusal.** The carrier is a
// reserved User Property, and a SUBSCRIBE carrying one saguin does not read
// is itself refused - so the one packet where a client could plant a value
// is exactly the packet that answers with one. Appending rather than
// replacing would echo it back as the broker's explanation.
func TestAClientCannotWriteItsOwnRefusalSentence(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "forger")
	sa := subSaying(t, c, "events/#",
		paho.UserProperty{Key: "saguin-reason", Value: "granted, obviously"})
	if sa.Reasons[0] != 0x83 {
		t.Fatalf("SUBACK 0x%02X, want 0x83", sa.Reasons[0])
	}
	said := subackReason(sa)
	if strings.Contains(said, "granted, obviously") {
		t.Errorf("the refusal reads %q: the client's own text came back as the "+
			"broker's explanation of why it was refused", said)
	}
	if !strings.Contains(said, "saguin-reason") || !strings.Contains(said, "reserved") {
		t.Errorf("the refusal reads %q and should say the property is reserved", said)
	}
}

// **The carrier never reaches a client.** It rides the SUBACK from the hook
// to the encoder and is stripped there with every other reserved name.
func TestTheRefusalCarrierIsNotOnTheWire(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "no-carrier")
	sa := subSaying(t, c, "events/#",
		paho.UserProperty{Key: "saguin-filter", Value: "topic_hash(3, 3)"})
	if sa.Properties == nil {
		t.Fatal("no properties at all on a refusal that should carry a reason")
	}
	for _, u := range sa.Properties.User {
		if u.Key == "saguin-reason" {
			t.Errorf("the SUBACK carries %s=%q: the carrier was meant to become "+
				"the Reason String and be stripped", u.Key, u.Value)
		}
	}
	if subackReason(sa) == "" {
		t.Error("and no Reason String either, so it was stripped without being lifted")
	}
}

// A granted subscription explains nothing, because there is nothing to
// explain - and a Reason String on a success would be saguin talking about
// a refusal that did not happen.
func TestAGrantedSubscribeCarriesNoReason(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "granted")
	sa := subSaying(t, c, "events/#",
		paho.UserProperty{Key: "saguin-filter", Value: "topic_hash(3, 0)"})
	if sa.Reasons[0] > 2 {
		t.Fatalf("SUBACK 0x%02X, want a grant", sa.Reasons[0])
	}
	if said := subackReason(sa); said != "" {
		t.Errorf("a granted subscription carries the reason %q", said)
	}
}

// MQTT-3.1.2-29: a client that asked for no problem information gets none,
// and that rule stays the substrate's rather than being copied into saguin.
// It sets Mods.DisallowProblemInfo before the encode hook and the encoder
// honours it after, so this is a check that saguin did not step around it.
func TestARefusalIsSilentWhenProblemInformationIsOff(t *testing.T) {
	h := start(t)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	if _, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: "quiet", CleanStart: true, KeepAlive: 0,
		Properties: &paho.ConnectProperties{RequestProblemInfo: false},
	}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
		Properties: &paho.SubscribeProperties{User: paho.UserProperties{
			{Key: "saguin-filter", Value: "topic_hash(3, 3)"}}},
	})
	if sa == nil {
		t.Fatalf("no SUBACK: %v", err)
	}
	if sa.Reasons[0] != 0x83 {
		t.Fatalf("SUBACK 0x%02X, want 0x83", sa.Reasons[0])
	}
	if said := subackReason(sa); said != "" {
		t.Errorf("a client that asked for no problem information was sent %q", said)
	}
}

// **A SUBSCRIBE naming one bad filter and two good ones keeps its
// connection**, and this is here because carrying the sentence broke it.
// The carrier is a reserved User Property, and OnSubscribed re-reads the
// packet for reserved names a client sent - so saguin's own answer read
// as a client's violation and dropped the connection on every partly
// refused SUBSCRIBE. MQTT answers each filter separately precisely so
// that this case works.
func TestAPartlyRefusedSubscribeKeepsItsConnection(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "partly")
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: "$saguin/queue/nope", QoS: 1},
			{Topic: "events/#", QoS: 1},
			{Topic: "other/#", QoS: 1},
		},
	})
	if sa == nil {
		t.Fatalf("the connection went away instead of answering: %v", err)
	}
	if len(sa.Reasons) != 3 {
		t.Fatalf("%d reason codes for three filters: %v", len(sa.Reasons), sa.Reasons)
	}
	if sa.Reasons[0] != 0x8F {
		t.Errorf("the queue-space filter answered 0x%02X, want 0x8F", sa.Reasons[0])
	}
	for i, code := range sa.Reasons[1:] {
		if code > 2 {
			t.Errorf("filter %d answered 0x%02X and should have been granted",
				i+1, code)
		}
	}
	if said := subackReason(sa); said == "" {
		t.Error("the refused filter's sentence did not travel")
	}

	// **Still connected**, which is the whole point: a client that
	// auto-resubscribes met a reconnect loop rather than a reason code.
	if sa := subSaying(t, c, "events/#"); sa.Reasons[0] > 2 {
		t.Errorf("a second subscribe on the same connection answered 0x%02X",
			sa.Reasons[0])
	}
}

// subscribeAll sends one SUBSCRIBE of these filters and hands back its
// reason codes, one a filter.
func subscribeAll(t *testing.T, c *paho.Client, filters ...string) []byte {
	t.Helper()
	var opts []paho.SubscribeOptions
	for _, f := range filters {
		opts = append(opts, paho.SubscribeOptions{Topic: f, QoS: 1})
	}
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: opts})
	if sa == nil {
		t.Fatalf("no SUBACK for %v: %v", filters, err)
	}
	if len(sa.Reasons) != len(filters) {
		t.Fatalf("%d reason codes for %d filters", len(sa.Reasons), len(filters))
	}
	return sa.Reasons
}

// RFC 0002 limits.max_subscriptions: a client holds at most this many topic
// filters, and a SUBSCRIBE that would take it past them has the excess
// refused with 0x97 (Quota exceeded), filter by filter, and keeps its
// connection. What counts is what the client would hold: a filter it already
// has is replaced and adds nothing, a shared subscription is one filter like
// any other, and a filter it gave up makes room. Without the bound a filter
// was about 2.3KB of memory the store charged 20 bytes, and one connection
// granted 100,000 took 201MiB.
func TestAClientHoldsNoMoreThanMaxSubscriptionsFilters(t *testing.T) {
	brokertest.MaxSubscriptions = 3
	t.Cleanup(func() { brokertest.MaxSubscriptions = 0 })
	h := start(t)
	c := connectAsking(t, h, "many")
	granted := func(code byte) bool { return code <= 2 }

	if got := subscribeAll(t, c, "a/1", "a/2", "a/3"); !granted(got[0]) || !granted(got[1]) || !granted(got[2]) {
		t.Fatalf("three filters at a bound of three answered % x", got)
	}
	sa := subSaying(t, c, "a/4")
	if sa.Reasons[0] != 0x97 {
		t.Fatalf("a fourth filter at a bound of three answered 0x%02X, want 0x97", sa.Reasons[0])
	}
	if why := subackReason(sa); !strings.Contains(why, "limits.max_subscriptions") {
		t.Errorf("the refusal said %q, which does not name limits.max_subscriptions", why)
	}
	// A filter already held is replaced, and counts nothing.
	if got := subscribeAll(t, c, "a/1"); !granted(got[0]) {
		t.Errorf("subscribing again to a filter it holds answered 0x%02X: a replacement was counted", got[0])
	}
	// Filter by filter within one packet: the held one is granted, the new
	// one refused.
	if got := subscribeAll(t, c, "a/2", "b/1"); !granted(got[0]) || got[1] != 0x97 {
		t.Errorf("a held filter and a new one at the bound answered % x, want granted then 0x97", got)
	}
	// Giving one up makes room, and a shared subscription is one filter.
	if _, err := c.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"a/1"}}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if got := subscribeAll(t, c, "$share/g/s/1"); !granted(got[0]) {
		t.Errorf("a shared subscription into the room one UNSUBSCRIBE made answered 0x%02X", got[0])
	}
	if got := subscribeAll(t, c, "b/2"); got[0] != 0x97 {
		t.Errorf("past the bound again, a filter answered 0x%02X: the shared subscription was not counted", got[0])
	}
	if got := subscribeAll(t, c, "$share/g/s/2"); got[0] != 0x97 {
		t.Errorf("a shared subscription past the bound answered 0x%02X: it was not counted as a filter", got[0])
	}
	cl, ok := h.Srv.Clients.Get("many")
	if !ok {
		t.Fatal("refusing filters ended the connection")
	}
	if n := cl.State.Subscriptions.Len(); n != 3 {
		t.Errorf("the client holds %d filters at a bound of three", n)
	}
}

// MQTT 3.1.1 has one failure code in a SUBACK, 0x80, so a client on it is
// refused past limits.max_subscriptions with that (RFC 0002 says where 0x80
// is sent). Written byte by byte: the ordinary clients speak MQTT 5.
func TestA311ClientPastMaxSubscriptionsIsRefusedWith0x80(t *testing.T) {
	brokertest.MaxSubscriptions = 1
	t.Cleanup(func() { brokertest.MaxSubscriptions = 0 })
	h := startAdmitting311(t)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	send := func(pk packets.Packet) {
		var buf bytes.Buffer
		pk.ProtocolVersion = 4
		switch pk.FixedHeader.Type {
		case packets.Connect:
			err = pk.ConnectEncode(&buf)
		case packets.Subscribe:
			err = pk.SubscribeEncode(&buf)
		}
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := conn.Write(buf.Bytes()); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	answer := func() (byte, []byte) {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		first, body, err := brokertest.ReadRawPacket(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return first >> 4, body
	}
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "old", Keepalive: 60, Clean: true}})
	if typ, body := answer(); typ != packets.Connack || len(body) < 2 || body[1] != 0 {
		t.Fatalf("connect: type %d, % x", typ, body)
	}
	codes := func(id uint16, filter string) []byte {
		send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: id,
			Filters: packets.Subscriptions{{Filter: filter, Qos: 1}}})
		typ, body := answer()
		if typ != packets.Suback || len(body) < 3 {
			t.Fatalf("subscribe %s: type %d, % x", filter, typ, body)
		}
		return body[2:] // the packet identifier, then a code a filter: 3.1.1 has no properties
	}
	if got := codes(1, "a/1"); !bytes.Equal(got, []byte{1}) {
		t.Fatalf("the first filter answered % x, want 01", got)
	}
	if got := codes(2, "a/2"); !bytes.Equal(got, []byte{0x80}) {
		t.Errorf("a 3.1.1 client past its bound was answered % x, want 80", got)
	}
}

// The refusal is said at WARN once an episode - the first refusal since the
// client last added a filter - so a client subscribing in a loop
// cannot choose how much the log holds, and a client that makes room and
// goes past its bound again is said again.
func TestRefusalsAtMaxSubscriptionsAreSaidOnceAnEpisode(t *testing.T) {
	brokertest.MaxSubscriptions = 1
	t.Cleanup(func() { brokertest.MaxSubscriptions = 0 })
	var warned atomic.Int64
	h := brokertest.StartLogging(t, func(line string) {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "limits.max_subscriptions") {
			warned.Add(1)
		}
	})
	c := connectAsking(t, h, "looping")
	if got := subscribeAll(t, c, "x/1"); got[0] > 2 {
		t.Fatalf("the first filter answered 0x%02X", got[0])
	}
	for _, f := range []string{"x/2", "x/3", "x/4"} {
		if got := subscribeAll(t, c, f); got[0] != 0x97 {
			t.Fatalf("%s past the bound answered 0x%02X", f, got[0])
		}
	}
	if n := warned.Load(); n != 1 {
		t.Errorf("three refusals in one episode were said %d times, want once", n)
	}
	// Room made and a filter accepted ends the episode; the next refusal
	// starts another, and is said.
	if _, err := c.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"x/1"}}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if got := subscribeAll(t, c, "x/2"); got[0] > 2 {
		t.Fatalf("a filter into the room answered 0x%02X", got[0])
	}
	if got := subscribeAll(t, c, "x/3"); got[0] != 0x97 {
		t.Fatalf("x/3 past the bound answered 0x%02X", got[0])
	}
	if n := warned.Load(); n != 2 {
		t.Errorf("a second episode left %d warnings in all, want 2", n)
	}
}

// A client at its bound that re-subscribes a filter it already holds,
// between refused SUBSCRIBEs, never leaves the bound, so its refusals are one
// episode and are said once. A re-subscribe used to end the episode, and a
// client alternating the two packets chose one WARN for every two. Kept as a probe.
func TestResubscribingAHeldFilterDoesNotEndTheEpisode(t *testing.T) {
	brokertest.MaxSubscriptions = 1
	t.Cleanup(func() { brokertest.MaxSubscriptions = 0 })
	var warned atomic.Int64
	h := brokertest.StartLogging(t, func(line string) {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "limits.max_subscriptions") {
			warned.Add(1)
		}
	})
	c := connectAsking(t, h, "looping")
	if got := subscribeAll(t, c, "x/1"); got[0] > 2 {
		t.Fatalf("the first filter answered 0x%02X", got[0])
	}
	for i := range 5 {
		if got := subscribeAll(t, c, "x/1"); got[0] > 2 {
			t.Fatalf("re-subscribing the held filter answered 0x%02X", got[0])
		}
		if got := subscribeAll(t, c, "x/new"); got[0] != 0x97 {
			t.Fatalf("round %d: past the bound answered 0x%02X", i, got[0])
		}
	}
	if n := warned.Load(); n != 1 {
		t.Errorf("a client that never left its bound was warned %d times, want once", n)
	}
}

// levelsDeep is a topic of n levels, the first of them first.
func levelsDeep(first string, n int) string { return first + strings.Repeat("/x", n-1) }

// RFC 0002 "How deep a topic may be": limits.max_topic_levels, 200 by default,
// holds every topic name and filter wherever one enters, at 200 levels granted
// and at 201 refused, each with the code MQTT names for what it is. Without
// it one filter of 30,000 levels took 17MiB of topic index and two seconds of
// CPU, and a client could hold max_subscriptions of them.
func TestEveryTopicAndFilterIsHeldToMaxTopicLevels(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "deep")
	ctx := context.Background()
	at, over := 200, 201

	// SUBSCRIBE: a plain filter and a shared one, measured after $share/g/.
	got := subscribeAll(t, c, levelsDeep("d", at), levelsDeep("d", over),
		"$share/g/"+levelsDeep("s", at), "$share/g/"+levelsDeep("s", over))
	if got[0] > 2 || got[2] > 2 {
		t.Errorf("filters of %d levels answered 0x%02X and 0x%02X, want granted", at, got[0], got[2])
	}
	if got[1] != 0x8F || got[3] != 0x8F {
		t.Errorf("filters of %d levels answered 0x%02X and 0x%02X, want 0x8F", over, got[1], got[3])
	}
	if cl, ok := h.Srv.Clients.Get("deep"); !ok || cl.State.Subscriptions.Len() != 2 {
		t.Errorf("the client holds %v filters, want the 2 granted", cl.State.Subscriptions.Len())
	}

	// UNSUBSCRIBE is not held to it: it allocates nothing, and it is how a
	// client gives up a filter it holds over a lowered bound. The one never
	// granted was simply not held.
	ua, err := c.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{levelsDeep("d", at), levelsDeep("d", over)}})
	if ua == nil || len(ua.Reasons) != 2 {
		t.Fatalf("unsubscribe: %v (%+v)", err, ua)
	}
	if ua.Reasons[0] != 0x00 || ua.Reasons[1] != 0x11 {
		t.Errorf("an UNSUBSCRIBE of filters of %d and %d levels answered % x, want 00 11", at, over, ua.Reasons)
	}

	// PUBLISH, and a point read's key, which is a topic name too.
	publish := func(topic string, payload []byte, props *paho.PublishProperties) byte {
		t.Helper()
		pa, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: payload, Properties: props})
		if pa == nil {
			t.Fatalf("publish to %.20s…: no PUBACK: %v", topic, err)
		}
		return pa.ReasonCode
	}
	if code := publish(levelsDeep("p", at), []byte("x"), nil); code != 0 {
		t.Errorf("a publish to a topic of %d levels answered 0x%02X, want 0x00", at, code)
	}
	if code := publish(levelsDeep("p", over), []byte("x"), nil); code != 0x90 {
		t.Errorf("a publish to a topic of %d levels answered 0x%02X, want 0x90", over, code)
	}
	read := &paho.PublishProperties{ResponseTopic: "reply/deep"}
	if code := publish("$saguin/kv/get", []byte(levelsDeep("state", at)), read); code != 0 {
		t.Errorf("a point read of a key of %d levels answered 0x%02X, want 0x00", at, code)
	}
	if code := publish("$saguin/kv/get", []byte(levelsDeep("state", over)), read); code != 0x90 {
		t.Errorf("a point read of a key of %d levels answered 0x%02X, want 0x90", over, code)
	}

	// A Will, refused at CONNECT.
	if _, code := connectNamedWithWill(t, h, "will-at", "", levelsDeep("w", at), true, 0, 0); code != 0 {
		t.Errorf("a Will on a topic of %d levels answered CONNACK 0x%02X, want 0x00", at, code)
	}
	if _, code := connectNamedWithWill(t, h, "will-over", "", levelsDeep("w", over), true, 0, 0); code != 0x90 {
		t.Errorf("a Will on a topic of %d levels answered CONNACK 0x%02X, want 0x90", over, code)
	}

	// A record a bridge brings in.
	link := h.B.NewBridgeClient("deep")
	if err := link.Bounded(levelsDeep("b", at), []byte("x"), nil, store.Props{}); err != nil {
		t.Errorf("a bridged record on a topic of %d levels was refused: %v", at, err)
	}
	if err := link.Bounded(levelsDeep("b", over), []byte("x"), nil, store.Props{}); err == nil {
		t.Errorf("a bridged record on a topic of %d levels was accepted", over)
	}
}

// MQTT 3.1.1 has one failure code in a SUBACK, so a filter past
// limits.max_topic_levels reaches a 3.1.1 client as 0x80 (RFC 0002).
func TestA311FilterPastMaxTopicLevelsIsRefusedWith0x80(t *testing.T) {
	h := startAdmitting311(t)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	send := func(pk packets.Packet, encode func(*packets.Packet, *bytes.Buffer) error) {
		t.Helper()
		var buf bytes.Buffer
		pk.ProtocolVersion = 4
		if err := encode(&pk, &buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := conn.Write(buf.Bytes()); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	answer := func() (byte, []byte) {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		first, body, err := brokertest.ReadRawPacket(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return first >> 4, body
	}
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "old", Keepalive: 60, Clean: true}},
		(*packets.Packet).ConnectEncode)
	if typ, body := answer(); typ != packets.Connack || len(body) < 2 || body[1] != 0 {
		t.Fatalf("connect: type %d, % x", typ, body)
	}
	for id, tc := range []struct {
		levels int
		want   byte
	}{{200, 1}, {201, 0x80}} {
		send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: uint16(id + 1),
			Filters: packets.Subscriptions{{Filter: levelsDeep("old", tc.levels), Qos: 1}}},
			(*packets.Packet).SubscribeEncode)
		typ, body := answer()
		if typ != packets.Suback || len(body) != 3 {
			t.Fatalf("subscribe: type %d, % x", typ, body)
		}
		if body[2] != tc.want {
			t.Errorf("a 3.1.1 filter of %d levels answered 0x%02X, want 0x%02X", tc.levels, body[2], tc.want)
		}
	}
}

// RFC 0002: a lowered limits.max_topic_levels or limits.max_subscriptions
// bounds what is added, not what is held. A session restored holding a
// filter deeper than the new bound, and more filters than the new count,
// keeps them all - ending it would drop what it is owed - and its client can
// still UNSUBSCRIBE the deep one. A new filter over either bound is refused.
func TestALoweredBoundKeepsWhatARestoredSessionHolds(t *testing.T) {
	dir := t.TempDir()
	deep, shallow := levelsDeep("deep", 5), "shallow/a"
	h := startDurable(t, dir)
	c := connectRx(t, h, "keeper", false, false, 10)
	if got := subscribeAll(t, c.C, deep, shallow); got[0] > 2 || got[1] > 2 {
		t.Fatalf("subscribing under the default bounds answered % x", got)
	}
	c.Close()
	h.Stop()

	levels, subs := brokertest.Limits.MaxTopicLevels, brokertest.Limits.MaxSubscriptions
	brokertest.Limits.MaxTopicLevels, brokertest.Limits.MaxSubscriptions = 3, 1
	t.Cleanup(func() { brokertest.Limits.MaxTopicLevels, brokertest.Limits.MaxSubscriptions = levels, subs })
	h2 := startDurable(t, dir)
	again := connectRx(t, h2, "keeper", false, false, 10)
	if !again.SessionPresent {
		t.Fatal("the session was not kept across a start that lowered max_topic_levels and max_subscriptions")
	}
	cl, ok := h2.Srv.Clients.Get("keeper")
	if !ok {
		t.Fatal("the resumed client is not registered")
	}
	for _, f := range []string{deep, shallow} {
		if _, held := cl.State.Subscriptions.Get(f); !held {
			t.Errorf("the restored session lost %q across the lowered bounds", f)
		}
	}
	if got := subscribeAll(t, again.C, levelsDeep("other", 4)); got[0] != 0x8F {
		t.Errorf("a new filter of 4 levels under a bound of 3 answered 0x%02X, want 0x8F", got[0])
	}
	if got := subscribeAll(t, again.C, "shallow/b"); got[0] != 0x97 {
		t.Errorf("a third filter under a max_subscriptions of 1 answered 0x%02X, want 0x97", got[0])
	}
	ua, err := again.C.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{deep}})
	if ua == nil || len(ua.Reasons) != 1 || ua.Reasons[0] != 0 {
		t.Fatalf("unsubscribing the filter held over the lowered bound: %v (%+v)", err, ua)
	}
	if _, held := cl.State.Subscriptions.Get(deep); held {
		t.Error("the UNSUBSCRIBE answered 0x00 and the filter is still held")
	}
}

// RFC 0005 saguin_subscriptions_refused_total{reason}: every filter a SUBACK
// refuses is counted once, labelled with the specification's name for the
// code decided for it - the one an MQTT 5 client is sent, and for a 3.1.1
// client the one decided before it is written as 0x80. No series exists
// before the first refusal. Each reason is driven through the part of Sagüin
// that decides it, and the scrape is held to what the SUBACKs carried.
func TestEverySubackRefusalIsCountedByTheCodeDecided(t *testing.T) {
	brokertest.MaxSubscriptions = 3
	t.Cleanup(func() { brokertest.MaxSubscriptions = 0 })
	h := startAdmitting311(t)
	ops := operationsAt(t, h)
	const series = `saguin_subscriptions_refused_total{reason="`
	counted := func() map[string]float64 {
		got := map[string]float64{}
		for k, v := range scrapeGauges(t, ops) {
			if name, ok := strings.CutPrefix(k, series); ok {
				got[strings.TrimSuffix(name, `"}`)] = v
			}
		}
		return got
	}
	if got := counted(); len(got) != 0 {
		t.Fatalf("series before any refusal: %v", got)
	}
	names := map[byte]string{0x83: "implementation specific error", 0x87: "not authorized",
		0x8F: "topic filter invalid", 0x91: "packet identifier in use", 0x97: "quota exceeded"}
	want := map[string]float64{}
	saw := func(how string, codes ...byte) {
		t.Helper()
		refused := false
		for _, c := range codes {
			if c >= 0x80 {
				name, ok := names[c]
				if !ok {
					t.Fatalf("%s was refused 0x%02X, which this test does not name", how, c)
				}
				want[name]++
				refused = true
			}
		}
		if !refused {
			t.Fatalf("%s was granted % x, so it drove no refusal", how, codes)
		}
	}

	c := connectAsking(t, h, "asker")
	// topic filter invalid: a filter that is not one, and one too deep.
	saw("a malformed filter and a deep one", subscribeAll(t, c, "a+b/x", levelsDeep("d", 201))...)
	// implementation specific error: a reserved property.
	saw("a reserved property", subSaying(t, c, "r/x", paho.UserProperty{Key: "saguin-nope", Value: "1"}).Reasons...)
	// quota exceeded: past limits.max_subscriptions of 3.
	saw("a fourth filter", subscribeAll(t, c, "q/1", "q/2", "q/3", "q/4")[3])
	// not authorized: a channel the roles do not grant (DenyFeatures grants
	// broadcast, events and state, and not jobs).
	brokertest.DenyFeatures(t, h, "")
	d := connectAsking(t, h, "denied")
	saw("the jobs queue", subscribeAll(t, d, "$saguin/queue/jobs")...)

	// packet identifier in use, and the 3.1.1 downgrade: over the raw
	// protocol, a SUBSCRIBE reusing the identifier of a QoS 2 publish still
	// waiting for its PUBREL, then a filter too deep. A 3.1.1 client is sent
	// 0x80 for both; the counts are by the codes decided.
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	send := func(pk packets.Packet, encode func(*packets.Packet, *bytes.Buffer) error) {
		t.Helper()
		var buf bytes.Buffer
		pk.ProtocolVersion = 4
		if err := encode(&pk, &buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := conn.Write(buf.Bytes()); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	answer := func(want byte) []byte {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		first, body, err := brokertest.ReadRawPacket(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if first>>4 != want {
			t.Fatalf("the broker answered a packet of type %d, want %d: % x", first>>4, want, body)
		}
		return body
	}
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "old", Keepalive: 60, Clean: true}},
		(*packets.Packet).ConnectEncode)
	answer(packets.Connack)
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}, PacketID: 9,
		TopicName: "held/x", Payload: []byte("x")}, (*packets.Packet).PublishEncode)
	answer(packets.Pubrec)
	for _, tc := range []struct {
		filter  string
		id      uint16
		decided byte
	}{{"s/x", 9, 0x91}, {levelsDeep("old", 201), 10, 0x8F}} {
		send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: tc.id,
			Filters: packets.Subscriptions{{Filter: tc.filter, Qos: 1}}}, (*packets.Packet).SubscribeEncode)
		body := answer(packets.Suback)
		if len(body) != 3 || body[2] != 0x80 {
			t.Errorf("a 3.1.1 SUBACK for a filter decided 0x%02X carried % x, want 0x80: 3.1.1 has no "+
				"other failure code", tc.decided, body)
		}
		want[names[tc.decided]]++
	}

	for _, reason := range names {
		if want[reason] == 0 {
			t.Fatalf("nothing drove %q, so this does not cover it", reason)
		}
	}
	if got := counted(); !maps.Equal(got, want) {
		t.Errorf("saguin_subscriptions_refused_total is %v, want %v", got, want)
	}
}
