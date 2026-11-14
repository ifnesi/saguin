package conformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The tests of the $saguin requests beside the seek, from the wire:
// kv/get, catalogue/<channel> and sessions/disconnect (design.md, $saguin
// topics). Requests are made as seek_test.go makes them (requestOf, asked,
// answerOn).

// kvGetTopic is the kv/get request's topic, disconnectTopic the operator's
// disconnect's, and catalogueTopic the catalogue's of a channel (design.md,
// $saguin topics, the table).
const (
	kvGetTopic      = "$saguin/kv/get"
	disconnectTopic = "$saguin/sessions/disconnect"
)

func catalogueTopic(channel string) string { return "$saguin/catalogue/" + channel }

// requestAnswers is the Response Topic of these tests' requests.
const requestAnswers = "answers/request"

// kvGetOf is a kv/get of topic at qos, its Response Topic requestAnswers, with
// correlation as requestOf sends it.
func kvGetOf(qos byte, topic string, correlation []byte) *harness.Publish {
	return requestOf(kvGetTopic, qos, topic, requestAnswers, correlation)
}

// kvGot sends from c, an MQTT 5 client, a QoS 1 kv/get of topic carrying
// Correlation Data of its own, fails the test unless its PUBACK is 0x00
// and its answer arrives, and returns the answer's payload.
func kvGot(t *testing.T, c *harness.Client, topic string) []byte {
	t.Helper()
	correlation := []byte("get " + topic)
	return asked(t, c, kvGetOf(1, topic, correlation), 0x00, requestAnswers, correlation).Payload
}

// kvRig launches a broker holding kvChannel beside o's channels, with o's
// other keys, and stores ps there, if any, from a publisher of its own, at
// QoS 1.
func kvRig(t *testing.T, o *harness.Options, ps ...*harness.Publish) *harness.Broker {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Channels = append(slices.Clone(own.Channels), kvChannel)
	b := support.Launch(t, &own)
	if len(ps) != 0 {
		support.PublishAll(t, asker(t, b, "kv-pub"), ps)
	}
	return b
}

// asker connects a new MQTT 5 session for client to b.
func asker(t *testing.T, b *harness.Broker, client string) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true})
	return c
}

// design.md, $saguin topics, the kv/get row: "The request reads one topic's
// current value on a latest channel. [...] The payload is the topic. An
// empty answer means no value."; Channels, latest: "It holds one value per
// topic, replaced in place [...] An empty payload deletes the value and
// leaves a tombstone".
//
// kv holds kv/a "one" then "two", and kv/c "gone" then deleted. An MQTT 5
// client's kv/gets, each at QoS 1 with a Response Topic: kv/a answers
// "two"; kv/b, never set, and kv/c, deleted, answer empty; each PUBACK 0x00.
func TestKVGetReturnsTheCurrentValue(t *testing.T) {
	b := kvRig(t, &harness.Options{}, value("kv/a", "one"), value("kv/a", "two"), value("kv/c", "gone"), value("kv/c", ""))
	c := asker(t, b, "kv-asker")
	for _, want := range []struct{ topic, value string }{{"kv/a", "two"}, {"kv/b", ""}, {"kv/c", ""}} {
		if got := kvGot(t, c, want.topic); string(got) != want.value {
			t.Errorf("kv/get of %s answered %q; want %q", want.topic, got, want.value)
		}
	}
}

// design.md, $saguin topics: the kv/get row, "It needs read on that topic
// (else 0x87). [...] The broker refuses a topic on no latest channel, with
// a wildcard, under $, or over max_topic_length or max_topic_levels with
// 0x90. It refuses an empty payload with 0x83."; above the table, "A
// request without a Response Topic is refused with 0x83, except for a seek
// or a queue response". metrics.md, saguin_requests_total: "request is
// seek, kv_get, catalogue or sessions_disconnect; result is refused: a
// request refused on its PUBACK or PUBREC".
//
// A broker whose acl_file gives asker read on kv/open/# alone: its QoS 1
// kv/get of kv/closed/x gets PUBACK 0x87, of kv/open/x 0x00. A broker with
// no acl_file, max_topic_length 32 and max_topic_levels 4, holding events
// (append) and kv: kv/gets of events/x, plain/x ($broadcast), kv/+, kv/#,
// $data/kv, kv/ and 30 more letters (33 bytes) and kv/a/b/c/d (5 levels)
// each get 0x90; an empty payload 0x83; kv/a without a Response Topic
// 0x83. On each broker /metrics counts every refusal under kv_get,
// refused, and none under seek.
func TestKVGetRefusals(t *testing.T) {
	acl := `roles:
  open:
    - channel: kv
      filter: kv/open/#
      allow: [read]
users:
  asker: [open]
`
	o := aclOptions(t, acl, "asker")
	o.Operations = true
	b := kvRig(t, o)
	c := loggedIn(t, b, "asker")
	ps := []*harness.Publish{kvGetOf(1, "kv/closed/x", []byte("closed")), kvGetOf(1, "kv/open/x", []byte("open"))}
	if got := pubackReasons(t, c, ps); !bytes.Equal(got, []byte{0x87, 0x00}) {
		t.Errorf("kv/gets of kv/closed/x and kv/open/x by a user reading kv/open/# alone: PUBACKs % X; want 87 00", got)
	}
	awaitSeries(t, b, map[string]float64{requestSeries("kv_get", "refused"): 1, requestSeries("seek", "refused"): 0})

	b = kvRig(t, &harness.Options{Operations: true, MaxTopicLength: 32, MaxTopicLevels: 4, Channels: []harness.Channel{seekChannel}})
	c = asker(t, b, "kv-limits")
	bad := []string{"events/x", "plain/x", "kv/+", "kv/#", "$data/kv", "kv/" + strings.Repeat("x", 30), "kv/a/b/c/d"}
	ps = make([]*harness.Publish, 0, len(bad)+2)
	want := make([]byte, 0, len(bad)+2)
	for _, topic := range bad {
		ps = append(ps, kvGetOf(1, topic, []byte(topic)))
		want = append(want, 0x90)
	}
	ps = append(ps, kvGetOf(1, "", []byte("empty")), requestOf(kvGetTopic, 1, "kv/a", "", nil))
	want = append(want, 0x83, 0x83)
	if got := pubackReasons(t, c, ps); !bytes.Equal(got, want) {
		t.Errorf("kv/gets of %q, then of an empty payload and of kv/a without a Response Topic: PUBACKs % X; want % X", bad, got, want)
	}
	awaitSeries(t, b, map[string]float64{requestSeries("kv_get", "refused"): float64(len(want)), requestSeries("seek", "refused"): 0})
}

// design.md, $saguin topics, the kv/get row: "The answer carries the
// request's Correlation Data, or the topic where it set none or an empty
// one."
//
// kv holds kv/a. An MQTT 5 client's QoS 1 kv/gets of kv/a: with
// Correlation Data 00 FF 63, the answer carries it; with none, and with
// empty Correlation Data, the answer carries kv/a.
func TestKVGetCorrelationData(t *testing.T) {
	b := kvRig(t, &harness.Options{}, value("kv/a", "v"))
	c := asker(t, b, "kv-correlation")
	for _, ask := range []struct{ sent, want []byte }{{[]byte{0x00, 0xFF, 'c'}, []byte{0x00, 0xFF, 'c'}}, {nil, []byte("kv/a")}, {[]byte{}, []byte("kv/a")}} {
		asked(t, c, kvGetOf(1, "kv/a", ask.sent), 0x00, requestAnswers, ask.want)
	}
}

// design.md, $saguin topics, the kv/get row: "At QoS 0, the request is
// dropped."; metrics.md, saguin_requests_total: "dropped: a discarded QoS 0
// request".
//
// kv holds kv/a. An MQTT 5 client sends a QoS 0 kv/get of kv/a, then a QoS
// 1 one: the first answer on its Response Topic is the QoS 1 one's, by its
// Correlation Data, and /metrics counts kv_get dropped 1 and answered 1.
func TestKVGetAtQoS0IsDropped(t *testing.T) {
	b := kvRig(t, &harness.Options{Operations: true}, value("kv/a", "v"))
	c := asker(t, b, "kv-qos0")
	if err := c.Send(kvGetOf(0, "kv/a", []byte("zero"))); err != nil {
		t.Fatalf("a QoS 0 kv/get: %v", err)
	}
	asked(t, c, kvGetOf(1, "kv/a", []byte("one")), 0x00, requestAnswers, []byte("one"))
	awaitSeries(t, b, map[string]float64{requestSeries("kv_get", "dropped"): 1, requestSeries("kv_get", "answered"): 1})
}

// design.md, $saguin topics: "A direct answer reserves outgoing memory and
// delivery-queue space before loading its body [...] Any other answer
// exceeding it is discarded. [...] A lost or discarded answer never undoes
// a completed request"; Message memory, Two queues: "A delivery of a
// compressed payload also reserves the stored copy's size [...] A delivery
// whose reservation (its encoded size and any stored copy) exceeds
// max_outgoing_memory is discarded for that subscriber, counted as missed
// and logged, as one over its Maximum Packet Size"; metrics.md,
// saguin_requests_total: "answer_too_large: an otherwise completed request
// whose answer exceeds Maximum Packet Size; otherwise answered".
//
// With max_packet_size 64 KiB and max_outgoing_memory 64 KiB and the
// largest S2 copy of it (leastIncoming), kv holds kv/small "s", kv/mid of
// 300 bytes and kv/big, sensor JSON filling a PUBLISH of max_packet_size,
// stored compressed. An MQTT 5 client advertising a Maximum Packet Size of
// 200 asks for kv/mid: PUBACK 0x00, then asks for kv/small, whose answer is
// the first it receives. Another, advertising none, asks for kv/big with
// Correlation Data filling the request to max_packet_size, so that the
// answer's encoded size and the stored copy's exceed max_outgoing_memory:
// PUBACK 0x00, then asks for kv/small, whose answer is the first it
// receives. /metrics counts kv_get answer_too_large 1 and answered 3.
func TestKVGetAnswerTooLargeIsDiscarded(t *testing.T) {
	const packet = 64 << 10
	memory := leastIncoming(packet)
	b := kvRig(t, &harness.Options{Operations: true, MaxPacketSize: packet, MaxOutgoingMemory: memory},
		value("kv/small", "s"), &harness.Publish{QoS: 1, Topic: "kv/mid", Payload: support.Noise(300)})
	pub := asker(t, b, "kv-big-pub")
	big := &harness.Publish{QoS: 1, ID: pub.NextID(), Topic: "kv/big"}
	big.Payload = sensorJSON(packet - len(harness.Encoded(t, big, harness.V5)) - 2) // its Remaining Length takes 2 bytes more
	if n := len(harness.Encoded(t, big, harness.V5)); n != packet {
		t.Fatalf("the PUBLISH of kv/big is %d bytes; want max_packet_size, %d", n, packet)
	}
	stored := shrinks(t, big.Payload)
	support.Acknowledged(t, pub, big)

	small, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-small", CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: 200}}})
	support.Acknowledged(t, small, kvGetOf(1, "kv/mid", []byte("mid")))
	asked(t, small, kvGetOf(1, "kv/small", []byte("after")), 0x00, requestAnswers, []byte("after"))

	c := asker(t, b, "kv-memory")
	request := kvGetOf(1, "kv/big", []byte{})
	request.ID = c.NextID()
	// The property section's length and the Remaining Length each take 2
	// bytes more once the Correlation Data fills the packet.
	correlation := bytes.Repeat([]byte("c"), packet-len(harness.Encoded(t, request, harness.V5))-4)
	request.Props[len(request.Props)-1].Data = correlation
	if n := len(harness.Encoded(t, request, harness.V5)); n != packet {
		t.Fatalf("the kv/get of kv/big is %d bytes; want max_packet_size, %d", n, packet)
	}
	answer := &harness.Publish{Topic: requestAnswers, Payload: big.Payload, Props: harness.Properties{{ID: harness.CorrelationData, Data: correlation}}}
	if n := len(harness.Encoded(t, answer, harness.V5)); n+stored <= memory {
		t.Fatalf("the answer of kv/big is %d bytes and its stored copy %d: together within max_outgoing_memory, %d; the test needs them over it", n, stored, memory)
	}
	support.Acknowledged(t, c, request)
	asked(t, c, kvGetOf(1, "kv/small", []byte("after")), 0x00, requestAnswers, []byte("after"))
	awaitSeries(t, b, map[string]float64{requestSeries("kv_get", "answer_too_large"): 1, requestSeries("kv_get", "answered"): 3})
}

// heldUnanswered is the most QoS 1 and QoS 2 PUBLISHes a client of these
// tests leaves unanswered at once: qos2.max_inflight_per_client's default,
// 20 (config.md, broker.qos2), the Receive Maximum the broker's CONNACK
// carries (design.md, CONNECT, 8) and the count a reader holds any client
// to (Flows, PUBLISH: "A reader counts its client's unanswered QoS 1 and 2
// PUBLISHes against qos2.max_inflight_per_client"). MQTT 5 3.3.4: "The
// Client MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH
// packets for which it has not received PUBACK, PUBCOMP, or PUBREC with a
// Reason Code of 128 or greater from the Server [MQTT-3.3.4-7]".
const heldUnanswered = 20

// acknowledgedWithin sends ps from c, QoS 1 PUBLISHes, at most
// heldUnanswered unanswered at once, and fails the test unless each gets
// PUBACK 0x00, an ordinary PUBACK over MQTT 3.1.1.
func acknowledgedWithin(t *testing.T, c *harness.Client, ps []*harness.Publish) {
	t.Helper()
	for start := 0; start < len(ps); start += heldUnanswered {
		batch := ps[start:min(start+heldUnanswered, len(ps))]
		for _, p := range batch {
			support.Sent(t, c, p)
		}
		for _, p := range batch {
			support.Puback(t, c, p)
		}
	}
}

// answerAllowances is a connection's answer allowances (design.md,
// Execution, Capacities: "Each connection has 64 reply slots and 64 answer
// allowances").
const answerAllowances = 64

// design.md, $saguin topics: "A connection has a bounded number of answer
// allowances ([Execution, Capacities](#capacities)). Before
// the broker carries out a request that has a direct answer, the request
// takes one of them, after its other admission checks. When none is free,
// the request is refused without being carried out: PUBACK or PUBREC 0x97
// on MQTT 5, and dropped at QoS 0. An MQTT 3.1.1 seek refused this way has
// no answer to carry the reason, so it is disconnected at QoS 1 or 2, as
// any refused publish is (PUBLISH)"; "An accepted request's
// acknowledgement becomes ready when its checks pass and any change it
// makes has committed (R4). A disconnect request's acknowledgement becomes
// ready when its action is taken. It never waits for the answer to be
// prepared or delivered"; "An answer's allowance ends when the answer is
// handed to the writer or discarded. A direct answer reserves outgoing
// memory and delivery-queue space before loading its body, and otherwise
// waits as the connection's deliveries do". Message memory, Two queues:
// "When the head of a FIFO does not fit in its budget, it waits with every
// younger request of that FIFO behind it". Flows, PUBLISH: "At QoS 1 or 2
// an MQTT 3.1.1 client has no refusal code and is disconnected". MQTT 5
// 3.4.2.1 and 3.5.2.1: 0x97, "Quota exceeded: An implementation or
// administrative imposed limit has been exceeded"; 4.3.3: "The Packet
// Identifier becomes available for reuse once the sender has received the
// PUBCOMP packet or a PUBREC with a Reason Code of 0x80 or greater".
//
// A broker holds events, with three records, and its max_outgoing_memory is
// stallBudget, which a subscriber that reads nothing then holds, the rest
// of its deliveries waiting for room (stallOutgoing). Client victim is
// connected. An MQTT 5 requester, its CONNACK's Receive Maximum 20, sends 64
// catalogue requests at QoS 1, each with Correlation Data of its own, and
// 16 ordinary QoS 1 publishes between them, never more than 20 unanswered:
// each gets PUBACK 0x00, though no answer can be delivered. It then asks to
// disconnect victim at QoS 1, refused PUBACK 0x97, and at QoS 2, refused
// PUBREC 0x97; an ordinary publish gets PUBACK 0x00; a PINGREQ is answered,
// no answer having come before it; victim, still connected, is answered
// PINGREQ. A durable MQTT 3.1.1 session sends 64 seeks of events' floor at
// QoS 1, never more than 20 unanswered, each acknowledged, then a seek of
// the next offset: the connection is closed, and its stored position is
// still the floor. The stalled subscriber closes, giving the outgoing
// budget back: the requester receives the 64 answers, one for each
// catalogue request's Correlation Data, and victim is still answered
// PINGREQ. /metrics counts catalogue answered 64 and sessions_disconnect
// refused 2.
func TestRequestsAcknowledgedWithoutWaitingForAnswers(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxOutgoingMemory: stallBudget, WriteTimeout: stallFor, Channels: []harness.Channel{seekChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "answers-pub", CleanStart: true})
	support.PublishAll(t, pub, eventsAt("held ", 3))
	floor := eventRecords(t, b.DB)[0].offset
	const victimID = "victim"
	victim, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: victimID, CleanStart: true})
	stalled, delivery := stallOutgoing(t, b, "")

	r, connack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "requester", CleanStart: true})
	if rm, ok := connack.Props.Get(harness.ReceiveMaximum); !ok || rm.Int != heldUnanswered {
		t.Fatalf("the requester's CONNACK carries Receive Maximum %d (present %v); want %d, qos2.max_inflight_per_client's default", rm.Int, ok, heldUnanswered)
	}
	correlation := func(i int) []byte { return fmt.Appendf(nil, "catalogue %d", i) }
	var ps []*harness.Publish
	for i := range answerAllowances {
		ps = append(ps, requestOf(catalogueTopic(seekChannel.Name), 1, "", requestAnswers, correlation(i)))
		if i%4 == 3 {
			ps = append(ps, &harness.Publish{QoS: 1, Topic: "requester/own", Payload: fmt.Appendf(nil, "own %d", i)})
		}
	}
	acknowledgedWithin(t, r, ps)
	atQoS1 := disconnectOf(1, victimID, []byte("refused at QoS 1"))
	support.Sent(t, r, atQoS1)
	support.PubackOf(t, r, atQoS1, 0x97)
	atQoS2 := disconnectOf(2, victimID, []byte("refused at QoS 2"))
	support.Sent(t, r, atQoS2)
	support.AnswerOf(t, r, harness.PUBREC, atQoS2, 0x97)
	support.Acknowledged(t, r, &harness.Publish{QoS: 1, Topic: "requester/own", Payload: []byte("after the refusals")})
	takenKeeping(t, r, "the requests refused for want of an answer allowance")
	for _, p := range r.Pending() {
		if a, ok := p.(*harness.Publish); ok {
			t.Fatalf("with the outgoing budget held, the requester was sent PUBLISH %q %q; want no answer before room returns", a.Topic, a.Payload)
		}
	}
	if err := victim.Ping(); err != nil {
		t.Fatalf("victim, after two refused requests to disconnect it: %v; want PINGRESP, neither carried out", err)
	}

	old, _ := harness.Connected(t, b.Addr, harness.V311, kept(harness.V311, "seeker-311"))
	seeks := make([]*harness.Publish, answerAllowances)
	for i := range seeks {
		seeks[i] = seekOf(seekChannel.Name, 1, "0", "", nil)
	}
	acknowledgedWithin(t, old, seeks)
	support.Sent(t, old, seekOf(seekChannel.Name, 1, "-1", "", nil))
	harness.ExpectClosed(t, old)
	if offset, _ := storedPosition(t, support.OpenDB(t, b.DB), "seeker-311", seekChannel.Name); offset != floor {
		t.Errorf("the MQTT 3.1.1 session's stored position on %s is %d after its refused seek of the next offset; want %d, the floor, the seek not carried out",
			seekChannel.Name, offset, floor)
	}
	if got := support.Sampled(t, b, inTransitOutgoing)[inTransitOutgoing]; got <= float64(stallBudget-delivery) {
		t.Fatalf("before the stalled subscriber closed, %s reads %v: it no longer held the outgoing budget, so nothing here tests the rule", inTransitOutgoing, got)
	}

	if err := stalled.Close(); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, answerAllowances)
	for len(got) < answerAllowances {
		p, err := r.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Publish); return ok && a.Topic == requestAnswers })
		if err != nil {
			t.Fatalf("with %d of the 64 answers, the outgoing budget given back: %v", len(got), err)
		}
		cd, _ := p.(*harness.Publish).Props.Get(harness.CorrelationData)
		if got[string(cd.Data)] {
			t.Fatalf("the answer carrying Correlation Data %q came twice", cd.Data)
		}
		got[string(cd.Data)] = true
	}
	for i := range answerAllowances {
		if !got[string(correlation(i))] {
			t.Errorf("no answer carries Correlation Data %q; got %v", correlation(i), got)
		}
	}
	if err := victim.Ping(); err != nil {
		t.Fatalf("victim, the answers delivered: %v; want PINGRESP, the refused requests never carried out", err)
	}
	awaitSeries(t, b, map[string]float64{requestSeries("catalogue", "answered"): answerAllowances, requestSeries("sessions_disconnect", "refused"): 2})
}

// allowanceACL is the acl_file of TestRequestChecksComeBeforeTheAnswerAllowance:
// operator may disconnect clients, read kv and plain and seek on events;
// limited may read kv/allowed/# and events, without seek; stall may write
// and read stalled/# (stallOutgoingAs).
const allowanceACL = `roles:
  operator:
    - broker: sessions
      allow: [disconnect]
    - channel: kv
      allow: [read]
    - channel: plain
      allow: [read]
    - channel: events
      allow: [read, seek]
  limited:
    - channel: kv
      filter: kv/allowed/#
      allow: [read]
    - channel: events
      allow: [read]
  stall:
    - topic: stalled/#
      allow: [write, read]
users:
  operator: [operator]
  limited: [limited]
  stall: [stall]
`

// design.md, $saguin topics: "Before the broker carries out a request that
// has a direct answer, the request takes one of them, after its other
// admission checks. When none is free, the request is refused without
// being carried out: PUBACK or PUBREC 0x97 on MQTT 5, and dropped at QoS
// 0"; "A request without a Response Topic is refused with 0x83, except for
// a seek or a queue response (PUBLISH). A publish under $saguin/ naming no
// request is refused with 0x90 without consulting the acl_file"; the
// kv/get row: "It needs read on that topic (else 0x87). [...] The broker
// refuses a topic on no latest channel, with a wildcard, under $, or over
// max_topic_length or max_topic_levels with 0x90. It refuses an empty
// payload with 0x83. At QoS 0, the request is dropped"; the seek row: "with
// seek on that channel (else 0x87)"; the sessions/disconnect row: "The
// request needs sessions: disconnect (else 0x87). [...] The broker refuses
// an empty payload or the asker's own id with 0x83. At QoS 0 it still acts.
// A refusal at QoS 0 is dropped". Flows, PUBLISH, the refusal table: "0x83
// | a saguin- property other than saguin-id; a $saguin request without a
// Response Topic". metrics.md, saguin_requests_total: "dropped: a discarded
// QoS 0 request".
//
// Under allowanceACL, with kv, plain (append, plain/#) and events, a
// subscriber that reads nothing holds the outgoing budget (stallOutgoingAs),
// and operator and limited each send 64 catalogue requests at QoS 1, each
// acknowledged, their answers waiting: no answer allowance is left on
// either connection. Each request below, at QoS 1, then gets its own code,
// never 0x97:
//   - operator: a disconnect with an empty payload, or of operator itself,
//     or without a Response Topic, 0x83; a kv/get with an empty payload, or
//     without a Response Topic, 0x83; a kv/get of plain/x, on no latest
//     channel, and of a kv topic over max_topic_length, 0x90; a catalogue
//     request without a Response Topic, or carrying the User Property
//     saguin-x, 0x83; a publish on $saguin/consumer/events/jump and on
//     $saguin/nothing, naming no request, 0x90;
//   - limited: a disconnect of another client, a kv/get of kv/denied/x and a
//     seek on events, each 0x87.
//
// operator then sends, at QoS 0, a kv/get with an empty payload, a
// disconnect with an empty payload and a well-formed catalogue request, and
// a QoS 1 catalogue request, which gets PUBACK 0x97: /metrics counts kv_get,
// sessions_disconnect and catalogue dropped 1 each.
//
// The wildcard and $ topics of the kv/get row, and a seek on a channel that
// does not exist, are left out: for operator's grants the design does not
// order their 0x90 against the read or seek check's 0x87.
func TestRequestChecksComeBeforeTheAnswerAllowance(t *testing.T) {
	o := aclOptions(t, allowanceACL, "operator", "limited", "stall")
	o.Operations, o.MaxOutgoingMemory, o.WriteTimeout = true, stallBudget, stallFor
	o.Channels = []harness.Channel{kvChannel, seekChannel, {Name: "plain", Type: "append", Filter: "plain/#"}}
	b := support.OpsLaunch(t, o)
	stalled, _ := stallOutgoingAs(t, b, "", "stall")
	defer func() { _ = stalled.Close() }() // the test is over
	operator, limited := loggedIn(t, b, "operator"), loggedIn(t, b, "limited")
	for _, c := range []*harness.Client{operator, limited} {
		full := make([]*harness.Publish, answerAllowances)
		for i := range full {
			full[i] = requestOf(catalogueTopic(kvChannel.Name), 1, "", requestAnswers, nil)
		}
		acknowledgedWithin(t, c, full)
	}
	reserved := requestOf(catalogueTopic(kvChannel.Name), 1, "", requestAnswers, nil)
	reserved.Props = append(reserved.Props, harness.User("saguin-x", "1"))
	for _, tc := range []struct {
		name   string
		c      *harness.Client
		p      *harness.Publish
		reason byte
	}{
		{"a disconnect with an empty payload", operator, disconnectOf(1, "", nil), 0x83},
		{"a disconnect of the asker", operator, disconnectOf(1, "operator", nil), 0x83},
		{"a disconnect without a Response Topic", operator, requestOf(disconnectTopic, 1, "limited", "", nil), 0x83},
		{"a kv/get with an empty payload", operator, kvGetOf(1, "", nil), 0x83},
		{"a kv/get without a Response Topic", operator, requestOf(kvGetTopic, 1, "kv/a", "", nil), 0x83},
		{"a kv/get on no latest channel", operator, kvGetOf(1, "plain/x", nil), 0x90},
		{"a kv/get over max_topic_length", operator, kvGetOf(1, "kv/"+strings.Repeat("a", 1024), nil), 0x90},
		{"a catalogue request without a Response Topic", operator, requestOf(catalogueTopic(kvChannel.Name), 1, "", "", nil), 0x83},
		{"a request carrying saguin-x", operator, reserved, 0x83},
		{"a publish under a channel naming no request", operator, requestOf("$saguin/consumer/events/jump", 1, "0", requestAnswers, nil), 0x90},
		{"a publish under $saguin/ naming no request", operator, requestOf("$saguin/nothing", 1, "", requestAnswers, nil), 0x90},
		{"a disconnect without sessions: disconnect", limited, disconnectOf(1, "operator", nil), 0x87},
		{"a kv/get without read", limited, kvGetOf(1, "kv/denied/x", nil), 0x87},
		{"a seek without seek", limited, seekOf(seekChannel.Name, 1, "0", answerTopic, nil), 0x87},
	} {
		support.Sent(t, tc.c, tc.p)
		ack, err := tc.c.AwaitAck(harness.PUBACK, tc.p.ID)
		switch {
		case err != nil:
			t.Fatalf("%s, no answer allowance left: %v", tc.name, err)
		case ack.Reason != tc.reason:
			t.Errorf("%s, no answer allowance left: PUBACK 0x%02X; want 0x%02X, its own refusal, taken before the allowance", tc.name, ack.Reason, tc.reason)
		}
	}
	for _, p := range []*harness.Publish{kvGetOf(0, "", nil), disconnectOf(0, "", nil), requestOf(catalogueTopic(kvChannel.Name), 0, "", requestAnswers, nil)} {
		support.Sent(t, operator, p)
	}
	wellFormed := requestOf(catalogueTopic(kvChannel.Name), 1, "", requestAnswers, nil)
	support.Sent(t, operator, wellFormed)
	support.PubackOf(t, operator, wellFormed, 0x97)
	awaitSeries(t, b, map[string]float64{requestSeries("kv_get", "dropped"): 1, requestSeries("sessions_disconnect", "dropped"): 1,
		requestSeries("catalogue", "dropped"): 1})
}

// design.md, $saguin topics, the kv/get row: "reads one topic's current
// value"; Store, Compression: "A payload over 4 KiB, SQLite's page size, is
// compressed with S2 and stored compressed only where that makes it
// shorter [...] A store reader decompresses it once per read".
//
// kv holds kv/json, 12 KiB of sensor JSON, which the database file holds
// compressed. An MQTT 5 client's QoS 1 kv/get of kv/json answers those 12
// KiB byte for byte.
func TestKVGetReturnsACompressedValue(t *testing.T) {
	p := &harness.Publish{QoS: 1, Topic: "kv/json", Payload: sensorJSON(3 * pageSize)}
	shrinks(t, p.Payload)
	b := kvRig(t, &harness.Options{}, p)
	expectCompressed(t, storedOn(t, support.OpenDB(t, b.DB), "records", p.Topic), p.Payload, "the record of "+p.Topic)
	if got := kvGot(t, asker(t, b, "kv-json"), p.Topic); !bytes.Equal(got, p.Payload) {
		t.Fatalf("kv/get of %s answered %d bytes, %.40q; want the %d bytes published, %.40q", p.Topic, len(got), got, len(p.Payload), p.Payload)
	}
}

// catalogued is a catalogue answer: design.md, $saguin topics, the
// catalogue row, `{"name":…,"type":…,"filter":…,"verbs":[…]}`, "A queue
// adds "pin"".
type catalogued struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Filter string   `json:"filter"`
	Verbs  []string `json:"verbs"`
	Pin    string   `json:"pin"`
}

// catalogueChannels are the channels of the catalogue tests: an append
// channel whose filter is written with braces, ordersQueue and kvChannel.
var catalogueChannels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/{a,b}/#"}, ordersQueue, kvChannel}

// catalogueACL gives asker write on events/a/# and read and seek on all of
// events, consume on orders and read on kv/open/#, and stranger read on
// elsewhere/# alone.
const catalogueACL = `roles:
  asking:
    - channel: events
      filter: events/a/#
      allow: [write]
    - channel: events
      allow: [read, seek]
    - channel: orders
      allow: [consume]
    - channel: kv
      filter: kv/open/#
      allow: [read]
  outside:
    - topic: elsewhere/#
      allow: [read]
users:
  asker: [asking]
  stranger: [outside]
`

// catalogueRig launches a broker holding catalogueChannels under
// catalogueACL, with /metrics.
func catalogueRig(t *testing.T) *harness.Broker {
	t.Helper()
	o := aclOptions(t, catalogueACL, "asker", "stranger")
	o.Operations, o.Channels = true, catalogueChannels
	return support.Launch(t, o)
}

// loggedIn connects to b an MQTT 5 client of user, its client id the
// user's name.
func loggedIn(t *testing.T, b *harness.Broker, user string) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(user, user))
	return c
}

// design.md, $saguin topics, the catalogue row: "The answer describes a
// channel: {"name":…,"type":…,"filter":…,"verbs":[…]}. The filter is as
// configured. The verbs are those of the asker's grants that name the
// channel. Each operation is still checked against its own topic. A queue
// adds "pin", its worker topic $saguin/queue/<channel>". The design gives
// the verbs no order: they are compared as sets.
//
// asker, under catalogueACL, asks at QoS 1 for the catalogue of events,
// orders and kv, each PUBACK 0x00: events is append, filter
// events/{a,b}/#, verbs read, seek and write; orders is queue, filter
// orders/#, verbs consume, pin $saguin/queue/orders; kv is latest, filter
// kv/#, verbs read.
func TestCatalogueDescribesTheChannel(t *testing.T) {
	c := loggedIn(t, catalogueRig(t), "asker")
	for _, want := range []catalogued{
		{Name: "events", Type: "append", Filter: "events/{a,b}/#", Verbs: []string{"read", "seek", "write"}},
		{Name: "orders", Type: "queue", Filter: "orders/#", Verbs: []string{"consume"}, Pin: "$saguin/queue/orders"},
		{Name: "kv", Type: "latest", Filter: "kv/#", Verbs: []string{"read"}},
	} {
		correlation := []byte("catalogue " + want.Name)
		a := asked(t, c, requestOf(catalogueTopic(want.Name), 1, "", requestAnswers, correlation), 0x00, requestAnswers, correlation)
		var got catalogued
		if err := json.Unmarshal(a.Payload, &got); err != nil {
			t.Fatalf("the catalogue of %s answered %q: %v", want.Name, a.Payload, err)
		}
		slices.Sort(got.Verbs)
		if want.Pin == "" {
			got.Pin = "" // only a queue's is described
		}
		if got.Name != want.Name || got.Type != want.Type || got.Filter != want.Filter || !slices.Equal(got.Verbs, want.Verbs) || got.Pin != want.Pin {
			t.Errorf("the catalogue of %s answered %s; want %+v", want.Name, a.Payload, want)
		}
	}
}

// design.md, $saguin topics, the catalogue row: "A channel that does not
// exist, or on which the asker holds no verb, gets an empty answer and
// PUBACK 0x00. Correlation Data follows kv/get, with the channel name
// standing for the topic. At QoS 0, the request is dropped."; the kv/get
// row, "The answer carries the request's Correlation Data, or the topic
// where it set none or an empty one"; above the table, "A request without a
// Response Topic is refused with 0x83". metrics.md, saguin_requests_total:
// "dropped: a discarded QoS 0 request".
//
// Under catalogueACL, stranger asks at QoS 1 for the catalogue of nosuch,
// and of events, where it holds no verb: each answered empty, PUBACK 0x00.
// asker asks for the catalogue of events at QoS 0 with Correlation Data
// "zero", then at QoS 1 with "one": the first answer it receives carries
// "one". It asks again with no Correlation Data, and with empty Correlation
// Data: each answer carries "events". Without a Response Topic: PUBACK
// 0x83. /metrics counts catalogue dropped 1 and refused 1.
func TestCatalogueEmptyAnswersAndCorrelation(t *testing.T) {
	b := catalogueRig(t)
	stranger := loggedIn(t, b, "stranger")
	for _, channel := range []string{"nosuch", "events"} {
		correlation := []byte("stranger " + channel)
		if a := asked(t, stranger, requestOf(catalogueTopic(channel), 1, "", requestAnswers, correlation), 0x00, requestAnswers, correlation); len(a.Payload) != 0 {
			t.Errorf("the catalogue of %s, asked by a user holding no verb there, answered %q; want an empty answer", channel, a.Payload)
		}
	}

	c := loggedIn(t, b, "asker")
	if err := c.Send(requestOf(catalogueTopic("events"), 0, "", requestAnswers, []byte("zero"))); err != nil {
		t.Fatalf("a QoS 0 catalogue request: %v", err)
	}
	asked(t, c, requestOf(catalogueTopic("events"), 1, "", requestAnswers, []byte("one")), 0x00, requestAnswers, []byte("one"))
	for _, sent := range [][]byte{nil, {}} {
		asked(t, c, requestOf(catalogueTopic("events"), 1, "", requestAnswers, sent), 0x00, requestAnswers, []byte("events"))
	}
	bare := requestOf(catalogueTopic("events"), 1, "", "", nil)
	support.Sent(t, c, bare)
	support.PubackOf(t, c, bare, 0x83)
	awaitSeries(t, b, map[string]float64{requestSeries("catalogue", "dropped"): 1, requestSeries("catalogue", "refused"): 1})
}

// operatorACL gives operator the right to disconnect sessions, and user
// read on elsewhere/# alone.
const operatorACL = `roles:
  operators:
    - broker: sessions
      allow: [disconnect]
  others:
    - topic: elsewhere/#
      allow: [read]
users:
  operator: [operators]
  user: [others]
`

// operatorRig launches a broker under operatorACL, with /metrics, and
// connects an MQTT 5 client of operator, client id "operator".
func operatorRig(t *testing.T) (*harness.Broker, *harness.Client) {
	t.Helper()
	o := aclOptions(t, operatorACL, "operator", "user")
	o.Operations = true
	b := support.Launch(t, o)
	return b, loggedIn(t, b, "operator")
}

// target is user's CONNECT, in version, of client's durable session (kept).
func target(version byte, client string) *harness.Connect {
	p := aclLogin(client, "user")
	if version == harness.V311 {
		p.CleanStart = false // Clean Session 0
	} else {
		p.Props = support.Durable
	}
	return p
}

// disconnectOf is a disconnect of client at qos, its Response Topic
// requestAnswers, with correlation as requestOf sends it.
func disconnectOf(qos byte, client string, correlation []byte) *harness.Publish {
	return requestOf(disconnectTopic, qos, client, requestAnswers, correlation)
}

// hungUp fails the test unless c, of version, is closed as an operator
// disconnect closes it: design.md, DISCONNECT, Reasons Sagüin sends,
// "0x98 for operator disconnect. [...] These reasons are MQTT 5 only. An
// MQTT 3.1.1 connection just closes."
func hungUp(t *testing.T, c *harness.Client, version byte) {
	t.Helper()
	if version == harness.V5 {
		if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x98 {
			t.Fatalf("DISCONNECT 0x%02X; want 0x98, operator disconnect", d.Reason)
		}
	}
	harness.ExpectClosed(t, c)
}

// design.md, $saguin topics, the sessions/disconnect row: "An operator
// disconnects the client whose id is the payload (0x98). Its session is
// kept. The request needs sessions: disconnect [...]. The answer is
// hung-up"; DISCONNECT, Reasons Sagüin sends: "0x98 for operator disconnect
// [...] These reasons are MQTT 5 only. An MQTT 3.1.1 connection just
// closes"; Flows, CONNECT: "Session Present is 1 when the CONNECT keeps its
// session and a session exists for the client id, either away or
// connected".
//
// Under operatorACL, a durable MQTT 5 session and a durable MQTT 3.1.1 one
// of user are connected. operator disconnects each at QoS 1: PUBACK 0x00
// and the answer hung-up; the MQTT 5 target receives DISCONNECT 0x98 and
// its connection is closed, the MQTT 3.1.1 target's is closed. Each
// reconnects with Clean Start 0: Session Present 1.
func TestOperatorDisconnectKeepsTheSession(t *testing.T) {
	b, op := operatorRig(t)
	for _, version := range bothVersions {
		connect := target(version, "target-"+harness.VersionName(version))
		c, _ := harness.Connected(t, b.Addr, version, connect)
		correlation := []byte(connect.ClientID)
		if a := asked(t, op, disconnectOf(1, connect.ClientID, correlation), 0x00, requestAnswers, correlation); string(a.Payload) != "hung-up" {
			t.Errorf("the disconnect of %s answered %q; want hung-up", connect.ClientID, a.Payload)
		}
		hungUp(t, c, version)
		resumed(t, b.Addr, version, connect)
	}
}

// design.md, $saguin topics, the sessions/disconnect row: "The request
// needs sessions: disconnect (else 0x87). The answer is hung-up, or
// no-such-client where no connection has that id or it is already closing.
// The broker refuses an empty payload or the asker's own id with 0x83. At
// QoS 0 it still acts. A refusal at QoS 0 is dropped."; above the table, "A
// request without a Response Topic is refused with 0x83". metrics.md,
// saguin_requests_total: "refused: a request refused on its PUBACK or
// PUBREC [...]; dropped: a discarded QoS 0 request [...]; otherwise
// answered".
//
// Under operatorACL, operator, at QoS 1 unless said:
//   - disconnects nobody, connected nowhere: PUBACK 0x00, no-such-client;
//   - disconnects a connected MQTT 5 session twice in one write: each
//     PUBACK 0x00, the answers hung-up then no-such-client, the second
//     finding it closing or gone; the session receives DISCONNECT 0x98;
//   - sends an empty payload, its own id, and a disconnect without a
//     Response Topic: PUBACK 0x83 each;
//   - at QoS 0, disconnects another connected session: it receives
//     DISCONNECT 0x98, and the operator the answer hung-up;
//   - at QoS 0, sends an empty payload, then at QoS 1 disconnects nobody:
//     the first answer it receives is no-such-client, the QoS 1 one's.
//
// user, without the right, disconnects a connected session: PUBACK 0x87.
// /metrics counts sessions_disconnect refused 4, dropped 1 and answered 5.
func TestOperatorDisconnectRefusals(t *testing.T) {
	b, op := operatorRig(t)
	answered := func(p *harness.Publish, want string) {
		t.Helper()
		cd, _ := p.Props.Get(harness.CorrelationData)
		if a := asked(t, op, p, 0x00, requestAnswers, cd.Data); string(a.Payload) != want {
			t.Errorf("the disconnect of %q answered %q; want %s", p.Payload, a.Payload, want)
		}
	}
	answered(disconnectOf(1, "nobody", []byte("nobody")), "no-such-client")

	twice, _ := harness.Connected(t, b.Addr, harness.V5, target(harness.V5, "twice"))
	ps := []*harness.Publish{disconnectOf(1, "twice", []byte("first")), disconnectOf(1, "twice", []byte("second"))}
	if got := pubackReasons(t, op, ps); !bytes.Equal(got, []byte{0x00, 0x00}) {
		t.Errorf("two disconnects of one session in one write: PUBACKs % X; want 00 00", got)
	}
	for _, want := range []struct{ correlation, answer string }{{"first", "hung-up"}, {"second", "no-such-client"}} {
		if a := answerOn(t, op, requestAnswers, []byte(want.correlation)); string(a.Payload) != want.answer {
			t.Errorf("the %s disconnect of a session answered %q; want %s", want.correlation, a.Payload, want.answer)
		}
	}
	hungUp(t, twice, harness.V5)

	ps = []*harness.Publish{disconnectOf(1, "", []byte("empty")), disconnectOf(1, "operator", []byte("self")), requestOf(disconnectTopic, 1, "nobody", "", nil)}
	if got := pubackReasons(t, op, ps); !bytes.Equal(got, []byte{0x83, 0x83, 0x83}) {
		t.Errorf("disconnects of an empty payload, of the asker's own id and without a Response Topic: PUBACKs % X; want 83 83 83", got)
	}

	quiet, _ := harness.Connected(t, b.Addr, harness.V5, target(harness.V5, "at-qos0"))
	if err := op.Send(disconnectOf(0, "at-qos0", []byte("zero"))); err != nil {
		t.Fatalf("a QoS 0 disconnect: %v", err)
	}
	if a := answerOn(t, op, requestAnswers, []byte("zero")); string(a.Payload) != "hung-up" {
		t.Errorf("the QoS 0 disconnect of a connected session answered %q; want hung-up", a.Payload)
	}
	hungUp(t, quiet, harness.V5)
	if err := op.Send(disconnectOf(0, "", []byte("dropped"))); err != nil {
		t.Fatalf("a QoS 0 disconnect: %v", err)
	}
	answered(disconnectOf(1, "nobody", []byte("after")), "no-such-client")

	harness.Connected(t, b.Addr, harness.V5, target(harness.V5, "stays")) // connected: only the right refuses the request
	refused := disconnectOf(1, "stays", []byte("no right"))
	u := loggedIn(t, b, "user")
	support.Sent(t, u, refused)
	support.PubackOf(t, u, refused, 0x87)
	awaitSeries(t, b, map[string]float64{
		requestSeries("sessions_disconnect", "refused"): 4, requestSeries("sessions_disconnect", "dropped"): 1, requestSeries("sessions_disconnect", "answered"): 5,
	})
}
