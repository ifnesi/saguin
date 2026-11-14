package conformance

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// refusedPublish is a publish design.md's PUBLISH refusal table refuses,
// as the package's broker, at config.md's default limits, meets it.
type refusedPublish struct {
	name   string
	topic  string
	props  harness.Properties // sent by MQTT 5 alone
	reason byte               // the MQTT 5 refusal code
}

// refusedPublishes are the refusals the reader makes from the PUBLISH
// alone. design.md, Flows, PUBLISH: "0x87 | [...] a topic under $SYS/";
// "0x90 | a topic over limits.max_topic_length or limits.max_topic_levels,
// under $share/, or under $saguin/ naming no request"; "0x97 | [...] over
// max_header_count"; "0x83 | a saguin- property other than saguin-id";
// Sagüin properties: "a publish carrying any saguin- property except
// saguin-id [...] is refused with 0x83". config.md, broker.limits:
// max_topic_length 1024, max_topic_levels 200 and max_header_count 32, "User
// Properties per publish", by default.
var refusedPublishes = []refusedPublish{
	{name: "a topic under $SYS/", topic: "$SYS/broker/load", reason: 0x87},
	{name: "a topic over max_topic_length", topic: strings.Repeat("a", 1025), reason: 0x90},
	{name: "a topic over max_topic_levels", topic: strings.Repeat("a/", 200) + "a", reason: 0x90},
	{name: "a topic under $share/", topic: "$share/group/t", reason: 0x90},
	{name: "a topic under $saguin/ naming no request", topic: "$saguin/no/such/request", reason: 0x90},
	{name: "a saguin- User Property other than saguin-id", topic: "reserved",
		props: harness.Properties{harness.User("saguin-offset", "7")}, reason: 0x83},
	{name: "more User Properties than max_header_count", topic: "headers",
		props: slices.Repeat(harness.Properties{harness.User("h", "v")}, 33), reason: 0x97},
}

// design.md, Flows, PUBLISH, the refusal table (refusedPublishes), "at QoS
// 1 [...]" the refusal riding the PUBACK; MQTT 5 4.13.2: "Acknowledgment
// packets PUBACK [...] with a Reason Code of 0x80 or greater indicate that
// the received packet [...] was in error. There are no consequences for
// other Sessions or other Packets flowing on the same Session"; MQTT 5
// 3.4.2.1, Table 3-4, lists 0x83, 0x87, 0x90 and 0x97. Each refusal, sent
// at QoS 1 by an MQTT 5 client, gets PUBACK with its code, and the
// connection then answers PINGREQ.
func TestRefusedPublishGetsItsCode(t *testing.T) {
	for _, row := range refusedPublishes {
		t.Run(row.name, func(t *testing.T) {
			c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			ack, err := c.Publish(&harness.Publish{QoS: 1, Topic: row.topic, Props: row.props, Payload: []byte("x")})
			if err != nil {
				t.Fatal(err)
			}
			if ack.Reason != row.reason {
				t.Errorf("PUBACK 0x%02X, want 0x%02X", ack.Reason, row.reason)
			}
			if err := c.Ping(); err != nil {
				t.Fatalf("after the refused PUBLISH: %v; want the connection to stay", err)
			}
		})
	}
}

// design.md, Flows: "Policy checks select the first applicable reason in
// the stated order, independently of property order." PUBLISH: "Policy
// reasons are taken in the order of the table above (PUBLISH). Within 0x97,
// header count precedes retained-value capacity, channel capacity and, at
// storage, store capacity." Each MQTT 5 PUBLISH at QoS 1, failing two of the
// table's checks, gets PUBACK with the earlier reason in the table's order,
// 0x87, 0x90, 0x97, 0x83; config.md, broker.limits: max_topic_length 1024
// and max_header_count 32 by default.
func TestPublishRefusalOrder(t *testing.T) {
	reserved := harness.User("saguin-x", "v")
	headers := slices.Repeat(harness.Properties{harness.User("h", "v")}, 33)
	cases := []struct {
		name   string
		topic  string
		props  harness.Properties
		reason byte
	}{
		{name: "0x87 before 0x83: a topic under $SYS/ with a saguin- property",
			topic: "$SYS/broker/load", props: harness.Properties{reserved}, reason: 0x87},
		{name: "0x87 before 0x90: a topic under $SYS/ over max_topic_length",
			topic: "$SYS/" + strings.Repeat("a", 1020), reason: 0x87},
		{name: "0x87 before 0x97: a topic under $SYS/ with more User Properties than max_header_count",
			topic: "$SYS/broker/load", props: headers, reason: 0x87},
		{name: "0x90 before 0x97: a topic over max_topic_length with more User Properties than max_header_count",
			topic: strings.Repeat("a", 1025), props: headers, reason: 0x90},
		{name: "0x90 before 0x83: a topic over max_topic_length with a saguin- property",
			topic: strings.Repeat("a", 1025), props: harness.Properties{reserved}, reason: 0x90},
		{name: "0x97 before 0x83: more User Properties than max_header_count, a saguin- one first",
			topic: "headers", props: append(harness.Properties{reserved}, headers...), reason: 0x97},
		{name: "0x97 before 0x83: more User Properties than max_header_count, a saguin- one last",
			topic: "headers", props: append(slices.Clone(headers), reserved), reason: 0x97},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			ack, err := c.Publish(&harness.Publish{QoS: 1, Topic: row.topic, Props: row.props, Payload: []byte("x")})
			if err != nil {
				t.Fatal(err)
			}
			if ack.Reason != row.reason {
				t.Errorf("PUBACK 0x%02X, want 0x%02X", ack.Reason, row.reason)
			}
		})
	}
}

// design.md, Flows, PUBLISH: "At QoS 0 a refused publish is dropped and
// counted"; "0 | on PUBLISH | never" (the publisher is never answered).
// Each refusal of refusedPublishes, sent at QoS 0 by an MQTT 5 client, and
// by an MQTT 3.1.1 client where it carries no property: nothing answers
// it, and the PINGREQ that follows gets its PINGRESP as the next packet,
// the connection staying. The count, saguin_publish_refused_total
// (metrics.md), is read once /metrics is scraped from outside (0020a).
func TestRefusedQoS0PublishIsDropped(t *testing.T) {
	for _, version := range []byte{harness.V5, harness.V311} {
		for _, row := range refusedPublishes {
			if version == harness.V311 && len(row.props) != 0 {
				continue // MQTT 3.1.1 has no properties to carry
			}
			t.Run(harness.VersionName(version)+", "+row.name, func(t *testing.T) {
				c, _ := harness.Connected(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
				if _, err := c.Publish(&harness.Publish{Topic: row.topic, Props: row.props, Payload: []byte("x")}); err != nil {
					t.Fatal(err)
				}
				if err := c.Ping(); err != nil {
					t.Fatalf("after the refused QoS 0 PUBLISH: %v; want the PINGRESP next, the publish dropped", err)
				}
			})
		}
	}
}

// design.md, Flows, PUBLISH: "At QoS 1 or 2 an MQTT 3.1.1 client has no
// refusal code and is disconnected"; MQTT 3.1.1 [MQTT-3.3.5-2]: "If a Server
// implementation does not authorize a PUBLISH to be performed by a Client; it
// has no way of informing that Client. It MUST either make a positive
// acknowledgement, according to the normal QoS rules, or close the Network
// Connection". Each refusal of refusedPublishes an MQTT 3.1.1 client can
// send, one carrying no property, at QoS 1: the connection closes, nothing
// sent first.
func TestRefusedMQTT311PublishCloses(t *testing.T) {
	for _, row := range refusedPublishes {
		if len(row.props) != 0 {
			continue // MQTT 3.1.1 has no properties to carry
		}
		t.Run(row.name, func(t *testing.T) {
			c, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			if err := c.Send(&harness.Publish{QoS: 1, ID: c.NextID(), Topic: row.topic, Payload: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			harness.ExpectClosed(t, c)
		})
	}
}

// design.md, Flows, CONNECT, 8: the CONNACK carries "Topic Alias Maximum
// (16)"; design.md, Execution, State ownership: readers do "everything that
// needs only their own connection. This includes decoding and checking
// packets". MQTT 5 3.3.2.3.4: "It sets a Topic Alias mapping by including a
// non-zero length Topic Name and a Topic Alias in the PUBLISH packet"; "a
// sender can send a PUBLISH packet that contains that Topic Alias and a zero
// length Topic Name. The receiver then treats the incoming PUBLISH as if it
// had contained the Topic Name of the Topic Alias"; "A Server MUST accept all
// Topic Alias values greater than 0 and less than or equal to the Topic Alias
// Maximum value that it returned in the CONNACK packet [MQTT-3.3.2-12]";
// "Topic Alias mappings exist only within a Network Connection and last only
// for the lifetime of that Network Connection. A receiver MUST NOT carry
// forward any Topic Alias mappings from one Network Connection to another
// [MQTT-3.3.2-7]". storage.md: a record's properties are "its MQTT 5
// properties in their wire form". (An alias of 0 or above 16 is
// harness.BadPackets', DISCONNECT 0x94.)
//
// MQTT 5 2.2.2.1: "The Property Length is encoded as a Variable Byte
// Integer"; 1.5.5: values from 128 to 16,383 take two bytes, below 128
// one.
//
// An MQTT 5 client publishes at QoS 1 with Topic Alias 16, its topic and a
// User Property, then with Topic Alias 16, an empty Topic Name and another
// User Property, then so again with a User Property long enough that the
// properties take 129 bytes with the alias and 126 without it, their
// Property Length one byte shorter, each answered PUBACK 0x00. After the
// stop, the database holds the three records under that topic, each with
// its PUBLISH's properties but its Topic Alias, which belongs to the
// connection, not the message, byte for byte, the Property Length theirs,
// and its PUBLISH's payload, byte for byte (MQTT 5 3.3.3: "The Payload
// contains the Application Message that is being published"; design.md,
// Sagüin properties: "Nothing a publisher sends is altered").
func TestInboundTopicAlias(t *testing.T) {
	const topic = "alias/sixteen"
	b := support.Launch(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "aliases", CleanStart: true})
	alias := harness.Property{ID: harness.TopicAlias, Int: 16}
	// A User Property takes 5 bytes beside its name and value; the alias 3.
	long := harness.User("step", strings.Repeat("v", 129-3-5-len("step")))
	kept := []harness.Properties{{harness.User("step", "set")}, {harness.User("step", "used")}, {long}}
	payloads := [][]byte{[]byte("set"), []byte("used"), []byte("long")}
	if with, without := propertiesOf(t, harness.Properties{alias, long}), propertiesOf(t, kept[2]); len(with) != 2+129 || len(without) != 1+126 {
		t.Fatalf("the long properties take %d bytes with the alias and %d without, want 131 and 127: Property Lengths of two bytes and one",
			len(with), len(without))
	}
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: topic, Props: harness.Properties{alias, kept[0][0]}, Payload: payloads[0]})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "", Props: harness.Properties{kept[1][0], alias}, Payload: payloads[1]})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "", Props: harness.Properties{alias, long}, Payload: payloads[2]})
	stopped(t, b)

	got := records(t, b.DB)
	if len(got) != len(kept) {
		t.Fatalf("the database holds %d records, want %d: %+v", len(got), len(kept), got)
	}
	for i := range got {
		r := &got[i]
		if r.topic != topic {
			t.Errorf("the record of payload %q has topic %q; want %q, the alias's", r.payload, r.topic, topic)
		}
		if want := propertiesOf(t, kept[i]); !bytes.Equal(r.properties, want) {
			t.Errorf("the record of payload %q has properties % X; want % X, its PUBLISH's without the Topic Alias", r.payload, r.properties, want)
		}
		if !bytes.Equal(r.payload, payloads[i]) {
			t.Errorf("record %d has payload % X; want % X, its PUBLISH's", i, r.payload, payloads[i])
		}
	}
}

// publishRate is the broker.limits.publish_rate of TestPublishRate and
// TestThrottledSecondsCount.
const publishRate = 5

// publishBytes is the broker.limits.publish_bytes of TestPublishBytes and
// TestRateWaitSparesKeepAlive, and bigPayload the payload of
// TestPublishBytes' message larger than a second's bytes: twice them, so
// its debt, whatever bytes of the PUBLISH the bucket counts beside its
// payload, takes at least a second to pay.
const (
	publishBytes = 1000
	bigPayload   = 2 * publishBytes
)

// least is the part of a wait the publish limits demand that a test holds
// the broker to, timed from the client's own write: the broker, receiving
// the bytes later, can only wait longer, and the tenth left out keeps the
// bound clear of the broker's own rounding of its buckets.
func least(d time.Duration) time.Duration {
	return d * 9 / 10
}

// paced writes ps from c in one write, as soon as c's CONNACK has come,
// each PUBLISH above QoS 0 under c's next packet identifier where it has
// none, and a PINGREQ after them. It returns how long after the write the
// PINGRESP was read, the moment the reader read the packet after the last
// PUBLISH (design.md, PUBLISH, Rate limits: "The wait holds every packet
// of that connection, acknowledgements and PINGREQ included"), and then
// the reason of each PUBLISH's first answer, in order: its PUBACK at QoS 1,
// its PUBREC at QoS 2, 0 at QoS 0, which nothing answers. A QoS 2
// exchange PUBREC accepts is finished, its PUBREL sent and its PUBCOMP
// required to be 0x00. A PINGRESP may pass an acknowledgement still
// awaiting storage (Flows, Replies: "Ready replies enter the connection's
// reply queue in that event order"), so the answers are read once it has
// come, from what was read past it.
func paced(t *testing.T, c *harness.Client, ps []*harness.Publish) (took time.Duration, reasons []byte) {
	t.Helper()
	wire := make([]byte, 0, 32*len(ps)+len(pingreq))
	for _, p := range ps {
		if p.QoS > 0 && p.ID == 0 {
			p.ID = c.NextID()
		}
		wire = append(wire, harness.Encoded(t, p, c.Version)...)
	}
	at := time.Now()
	harness.SendBytes(t, c, append(wire, pingreq...))
	if _, err := c.Await(func(p harness.Packet) bool { _, ok := p.(*harness.Pingresp); return ok }); err != nil {
		t.Fatalf("waiting for the PINGRESP after %d PUBLISHes written at once: %v", len(ps), err)
	}
	took = time.Since(at)
	reasons = make([]byte, len(ps))
	for i, p := range ps {
		if p.QoS == 0 {
			continue
		}
		ack, err := c.AwaitAck(harness.AnswerKind(p.QoS), p.ID)
		if err != nil {
			t.Fatalf("PUBLISH %d of %d written at once: %v", i+1, len(ps), err)
		}
		reasons[i] = ack.Reason
		if p.QoS == 2 && ack.Reason < 0x80 {
			if err := c.Send(&harness.Ack{Kind: harness.PUBREL, ID: p.ID}); err != nil {
				t.Fatal(err)
			}
			if comp, err := c.AwaitAck(harness.PUBCOMP, p.ID); err != nil || comp.Reason != 0 {
				t.Fatalf("PUBLISH %d of %d written at once, at QoS 2: PUBCOMP %+v, %v; want 0x00, the publication in its channel", i+1, len(ps), comp, err)
			}
		}
	}
	return took, reasons
}

// pacedEveryWay runs, on a broker of o's, one case for each MQTT version
// and QoS, side by side, each on a topic of its own: a subscriber of its
// own to the topic at QoS 2, connected first; then a publisher of the
// case's version writes a PUBLISH of each of payloads at the case's QoS on
// the topic, at once, a PINGREQ after them (paced). Every answer is 0x00,
// the PINGRESP comes no sooner than wait after the write, and the
// subscriber receives each payload, in order, at the case's QoS: nothing
// refused or lost (an MQTT 3.1.1 publisher at QoS 1 or 2 would have been
// disconnected for a refusal, and one at QoS 0 dropped).
func pacedEveryWay(t *testing.T, o *harness.Options, payloads [][]byte, wait time.Duration) {
	t.Helper()
	b := support.Launch(t, o)
	for _, version := range []byte{harness.V5, harness.V311} {
		for qos := range byte(3) {
			t.Run(fmt.Sprintf("%s, QoS %d", harness.VersionName(version), qos), func(t *testing.T) {
				t.Parallel()
				topic := "paced/" + clientID(t, "")
				sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(2, topic))
				pub, _ := harness.Connected(t, b.Addr, version, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
				ps := make([]*harness.Publish, len(payloads))
				for i, payload := range payloads {
					ps[i] = &harness.Publish{QoS: qos, Topic: topic, Payload: payload}
				}
				took, reasons := paced(t, pub, ps)
				if slices.ContainsFunc(reasons, func(r byte) bool { return r != 0 }) {
					t.Errorf("%d PUBLISHes written at once: answers % X; want 0x00 to each, nothing refused", len(ps), reasons)
				}
				if took < least(wait) {
					t.Errorf("the PINGRESP after the PUBLISHes came %v after the write; want no sooner than %v, the PINGREQ read only once the allowance refilled", took, least(wait))
				}
				for _, payload := range payloads {
					support.DeliveredAt(t, sub, qos, topic, payload)
				}
			})
		}
	}
}

// design.md, Flows, PUBLISH, Rate limits: "publish_rate and publish_bytes
// slow a publisher down. They never refuse a publish. The reader counts
// them in buckets holding one second's worth, full at CONNECT"; "When a
// charge leaves the message bucket below one message, or the byte bucket at
// or below zero, the reader reads nothing more from its connection until
// the message bucket holds one message again and the byte bucket is above
// zero. The client's socket buffer then fills, so the client slows down.
// Nothing is refused or lost, at any QoS or MQTT version"; config.md,
// broker.limits: publish_rate, "messages and bytes per second per
// connection; throttling, per-user overrides and accounting follow PUBLISH,
// Rate limits".
//
// With publish_rate publishRate, each case of pacedEveryWay writes
// publishRate+2 PUBLISHes: the full bucket takes publishRate, and each of
// the last two, like the PINGREQ after them, is read a 1/publishRate second
// after the one before, so the PINGRESP comes no sooner than
// 3/publishRate seconds after the write.
func TestPublishRate(t *testing.T) {
	payloads := make([][]byte, publishRate+2)
	for i := range payloads {
		payloads[i] = []byte(strconv.Itoa(i))
	}
	pacedEveryWay(t, &harness.Options{PublishRate: publishRate}, payloads, 3*time.Second/publishRate)
}

// design.md, Flows, PUBLISH, Rate limits: "The reader counts them in
// buckets holding one second's worth, full at CONNECT"; "A message larger
// than a second's bytes passes and leaves the byte bucket in debt. When a
// charge leaves [...] the byte bucket at or below zero, the reader reads
// nothing more from its connection until [...] the byte bucket is above
// zero"; "Nothing is refused or lost, at any QoS or MQTT version";
// config.md, broker.limits: publish_bytes, "messages and bytes per second
// per connection; throttling, per-user overrides and accounting follow
// PUBLISH, Rate limits".
//
// With publish_bytes publishBytes, each case of pacedEveryWay writes a
// message of bigPayload bytes, then one of a byte: the first passes, its
// debt of at least a second's bytes paid at publishBytes a second, so the
// second, like the PINGREQ after it, is read no sooner than a second after
// the write.
func TestPublishBytes(t *testing.T) {
	pacedEveryWay(t, &harness.Options{PublishBytes: publishBytes}, [][]byte{make([]byte, bigPayload), []byte("x")}, time.Second)
}

// throttledSeries is saguin_publish_throttled_seconds_total (metrics.md,
// Publishing), as testkit.SeriesKey names it.
const throttledSeries = "saguin_publish_throttled_seconds_total{}"

// metrics.md, Publishing: saguin_publish_throttled_seconds_total, "time
// readers spent waiting for their connection's publish allowance to refill
// (design.md, PUBLISH, Rate limits); added as each wait ends"; design.md,
// Flows, PUBLISH, Rate limits: "When a charge leaves the message bucket
// below one message, or the byte bucket at or below zero, the reader reads
// nothing more from its connection until the message bucket holds one
// message again and the byte bucket is above zero".
//
// On a broker of its own with publish_rate publishRate, an MQTT 5 client
// writes publishRate+2 PUBLISHes at QoS 0 at once, a PINGREQ after them
// (paced): its reader waits a 1/publishRate second three times before it
// reads the PINGREQ. The series rises from its value before the write by
// no less than those three waits, least's part of them, and by no more
// than the time from the write to the PINGRESP, which holds every wait,
// with room.
func TestThrottledSecondsCount(t *testing.T) {
	const waited = 3 * time.Second / publishRate
	b := support.OpsLaunch(t, &harness.Options{PublishRate: publishRate})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	before := support.Sampled(t, b, throttledSeries)[throttledSeries]
	ps := make([]*harness.Publish, publishRate+2)
	for i := range ps {
		ps[i] = &harness.Publish{Topic: "throttled", Payload: []byte("x")}
	}
	took, _ := paced(t, c, ps)
	var rise float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		rise = support.Sampled(t, b, throttledSeries)[throttledSeries] - before
		return rise >= least(waited).Seconds()
	}) {
		t.Fatalf("%s rose by %vs; want at least %v, the reader's three waits", throttledSeries, rise, least(waited))
	}
	if most := took + 250*time.Millisecond; rise > most.Seconds() {
		t.Errorf("%s rose by %vs; want at most %v, the %v from the write to the PINGRESP that held every wait, with room", throttledSeries, rise, most, took)
	}
}

// atOnce returns the bytes of n MQTT 5 PUBLISHes from c at QoS 1 on topic,
// each with c's next packet identifier, for one write: a client sending
// them all before any answer can come.
func atOnce(t *testing.T, c *harness.Client, n int, topic string) []byte {
	t.Helper()
	wire := make([]byte, 0, n*len(topic)+n*16)
	for range n {
		wire = append(wire, harness.Encoded(t, &harness.Publish{QoS: 1, ID: c.NextID(), Topic: topic, Payload: []byte("x")}, harness.V5)...)
	}
	return wire
}

// receiveMaximum is the qos2.max_inflight_per_client of
// TestReceiveMaximumExceeded, sent as the CONNACK's Receive Maximum.
const receiveMaximum = 2

// design.md, Flows, CONNECT, 8: the CONNACK carries "Receive Maximum
// (qos2.max_inflight_per_client)"; DISCONNECT, "Reasons Sagüin sends":
// "0x93 for Receive Maximum exceeded"; design.md, Flows, Replies: PUBACK
// "sent only once the store confirms"; Words: "Receive Maximum is counted per
// direction". MQTT 5 3.3.4: "If it receives more than Receive Maximum QoS 1
// and QoS 2 PUBLISH packets where it has not sent a PUBACK or PUBCOMP in
// response, the Server uses a DISCONNECT packet with Reason Code 0x93
// (Receive Maximum exceeded)".
//
// With qos2.max_inflight_per_client receiveMaximum, an MQTT 5 client's
// CONNACK carries Receive Maximum receiveMaximum. The test holds the
// database's write lock, as another program on the file may, so no PUBACK
// can come, and the client writes receiveMaximum+1 QoS 1 PUBLISHes at
// once: it reads DISCONNECT 0x93, then the end of stream.
func TestReceiveMaximumExceeded(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxInflightPerClient: receiveMaximum})
	c, ack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "eager", CleanStart: true})
	if rm, ok := ack.Props.Get(harness.ReceiveMaximum); !ok || rm.Int != receiveMaximum {
		t.Fatalf("CONNACK's Receive Maximum %+v (present %v), want %d", rm, ok, receiveMaximum)
	}
	lock := holdWriteLock(t, b.DB)
	harness.SendBytes(t, c, atOnce(t, c, receiveMaximum+1, "eager"))
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x93 {
		t.Fatalf("DISCONNECT 0x%02X, want 0x93 (Receive Maximum exceeded)", d.Reason)
	}
	harness.ExpectClosed(t, c)
	lock.release(t)
}

// maxPacket and incomingHeld are the broker.limits.max_packet_size and
// max_incoming_memory of TestIncomingMemoryHoldsReaders: two PUBLISHes of
// maxPacket bytes, each a body of maxPacket-3 behind a fixed header of 3,
// not over 4 KiB so asking no S2 room (design.md, Message memory, Asking),
// reserve 8186, leaving 11, less than the 12 of the test's small body.
const maxPacket, incomingHeld = 4096, 2*(maxPacket-3) + 11

// heldFor is how long TestIncomingMemoryHoldsReaders finds a PUBLISH still
// unanswered: what it checks is that nothing comes, so the wait's length
// is the behaviour; a broker answers a PUBLISH it holds nothing back from
// within milliseconds.
const heldFor = 500 * time.Millisecond

// design.md, Message memory: "the incoming budget,
// limits.max_incoming_memory, holds PUBLISH bodies from the start of their
// reading until they are stored or refused"; Asking, "Before allocating or
// reading a nonempty PUBLISH body, its reader requests the body's Remaining
// Length from the core. Every such body uses this request"; Two queues, "A
// request receives its whole reservation or waits"; Bytes back, "An
// incoming reservation follows its body through reading, handover and
// storage, and ends when the body is stored, refused or abandoned, at the
// reader's end if unread". design.md, Flows, Replies: PUBACK "sent only once
// the store confirms".
//
// With max_packet_size maxPacket and max_incoming_memory incomingHeld,
// clients A and C each write a PINGREQ and then the first half of a QoS 1
// PUBLISH of maxPacket bytes, and wait for the PINGRESP, which says the
// reader has read on to that PUBLISH's fixed header; their bodies, being
// read, hold all of max_incoming_memory but 11 bytes, the two clients
// silent for less than write_timeout. Client B then connects and publishes
// a small message at QoS 1, a body of 12: no PUBACK comes for heldFor.
// Then, in each case, B's PUBACK 0x00 comes within heldFor, A's body still
// being read:
//   - C sends the rest of its PUBLISH, which gets PUBACK 0x00 within
//     heldFor too: both bodies had their reservations, whole;
//   - C's connection ends, its client closing it, its body unread.
//
// Last, A sends the rest of its PUBLISH, which gets PUBACK 0x00.
func TestIncomingMemoryHoldsReaders(t *testing.T) {
	// A body of 12: the Topic Name's 7, the Packet Identifier's 2, an
	// empty property section's 1 and the payload's 2.
	small := &harness.Publish{QoS: 1, ID: 1, Topic: "small", Payload: []byte("xx")}
	if left, body := incomingHeld-2*(maxPacket-3), len(harness.Encoded(t, small, harness.V5))-2; body <= left {
		t.Fatalf("two large bodies leave %d bytes of max_incoming_memory's %d, room for the small body of %d; the test needs none", left, incomingHeld, body)
	}
	if incomingHeld < leastIncoming(maxPacket) {
		t.Fatalf("max_incoming_memory %d is below its least for max_packet_size %d, %d", incomingHeld, maxPacket, leastIncoming(maxPacket))
	}
	// finish sends c's rest and expects the PUBLISH's PUBACK 0x00.
	finish := func(t *testing.T, c *harness.Client, rest []byte, id uint16, what string) {
		t.Helper()
		harness.SendBytes(t, c, rest)
		if ack, err := c.AwaitAck(harness.PUBACK, id); err != nil || ack.Reason != 0 {
			t.Fatalf("%s, finished: %+v, %v; want PUBACK 0x00", what, ack, err)
		}
	}
	for _, tc := range []struct {
		name string
		end  func(t *testing.T, c *harness.Client, rest []byte, id uint16)
	}{
		{"the second large PUBLISH finishes", func(t *testing.T, c *harness.Client, rest []byte, id uint16) {
			// Within heldFor, long before A's silence reaches
			// write_timeout: A's body is still being read.
			c.Timeout = heldFor
			finish(t, c, rest, id, "C's PUBLISH, with A's body still being read")
		}},
		{"the second large connection ends", func(t *testing.T, c *harness.Client, _ []byte, _ uint16) {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A and C are silent mid-body while B's PUBLISH is held for
			// heldFor: write_timeout well above that keeps the
			// silent-sender rule from closing them meanwhile. A broker of
			// the case's own: no reservation of another case is left in it.
			b := support.Launch(t, &harness.Options{MaxIncomingMemory: incomingHeld, MaxPacketSize: maxPacket, WriteTimeout: 10 * heldFor})
			a, aRest, aID := halfSent(t, b, "-a")
			c, cRest, cID := halfSent(t, b, "-c")

			bc, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true})
			p := &harness.Publish{QoS: 1, ID: bc.NextID(), Topic: small.Topic, Payload: small.Payload}
			if err := bc.Send(p); err != nil {
				t.Fatal(err)
			}
			nothingWithin(t, bc, heldFor, "with all of max_incoming_memory but 11 bytes held by two PUBLISHes half sent")
			tc.end(t, c, cRest, cID)
			bc.Timeout = heldFor // long before A's silence reaches write_timeout
			if ack, err := bc.AwaitAck(harness.PUBACK, p.ID); err != nil || ack.Reason != 0 {
				t.Fatalf("once C's bytes are free, within %v: %+v, %v; want PUBACK 0x00", heldFor, ack, err)
			}
			finish(t, a, aRest, aID, "A's PUBLISH")
		})
	}
}

// halfSent has an MQTT 5 client of its own, named by suffix, connect to b
// and write a PINGREQ and then the first half of a QoS 1 PUBLISH of
// maxPacket bytes (a fixed header of 3), and wait for the PINGRESP, which
// says its reader has read on to that PUBLISH's fixed header; it returns
// the client, the PUBLISH's rest and its packet identifier.
func halfSent(t *testing.T, b *harness.Broker, suffix string) (c *harness.Client, rest []byte, id uint16) {
	t.Helper()
	const size = maxPacket
	c, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, suffix), CleanStart: true})
	id = c.NextID()
	// A fixed header of 3 (a Remaining Length of two bytes), the Topic
	// Name's 5, the Packet Identifier's 2, an empty property section's 1,
	// and the payload.
	big := harness.Encoded(t, &harness.Publish{QoS: 1, ID: id, Topic: "big", Payload: make([]byte, size-3-5-2-1)}, harness.V5)
	if len(big) != size {
		t.Fatalf("the large PUBLISH is %d bytes, want %d", len(big), size)
	}
	harness.SendBytes(t, c, append(slices.Clone(pingreq), big[:len(big)/2]...))
	harness.Expect[*harness.Pingresp](t, c)
	return c, big[len(big)/2:], id
}

// The series of message memory's two budgets (metrics.md, Store and
// memory): saguin_memory_in_transit_bytes{direction}, "outstanding
// reservations of each budget, incoming or outgoing (design.md, Message
// memory)", and saguin_memory_full_seconds_total{direction}, "each budget's
// full time (design.md, Message memory), by the events' times. Includes
// completed intervals only; an ongoing interval is added when it ends".
var (
	inTransitIncoming = testkit.SeriesKey("saguin_memory_in_transit_bytes", "direction", "incoming")
	inTransitOutgoing = testkit.SeriesKey("saguin_memory_in_transit_bytes", "direction", "outgoing")
	fullIncoming      = testkit.SeriesKey("saguin_memory_full_seconds_total", "direction", "incoming")
	fullOutgoing      = testkit.SeriesKey("saguin_memory_full_seconds_total", "direction", "outgoing")
)

// awaitAbove waits until series reads above least on b's /metrics, and
// returns what it read, failing the test where it does not within
// harness.DefaultTimeout, the watchdog; what names the state awaited.
func awaitAbove(t *testing.T, b *harness.Broker, series string, least float64, what string) float64 {
	t.Helper()
	var got float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = support.Sampled(t, b, series)[series]
		return got > least
	}) {
		t.Fatalf("watchdog: %s: %s reads %v after %v; want above %v", what, series, got, harness.DefaultTimeout, least)
	}
	return got
}

// stallPayload and stallBudget are the payload of each message a
// stalled subscriber is sent (stallOutgoing) and the max_outgoing_memory of
// the brokers it stalls: noise, so stored and delivered as received with no
// stored copy (design.md, Store, Compression), a delivery of a little over
// 512 KiB; and a budget of 2 MiB, room for three such deliveries and not a
// fourth. stallMessages of them, 10 MiB, outgrow what the broker's send
// buffer and the client's receive buffer take before the broker's writer
// blocks in its write, at most 4 MiB and a little more here
// (net.ipv4.tcp_wmem, smallBuffers), together with the budget, and fit in
// the window of limits.max_inflight's default, 20.
const (
	stallPayload  = 512 << 10
	stallBudget   = 2 << 20
	stallMessages = 20
)

// stallFor is the broker.limits.write_timeout of the brokers a stalled
// subscriber holds (stallOutgoing): longer than any of their tests, so no
// write of the stalled subscriber's is cut while a test runs (design.md,
// Closing a connection: "A write that has not finished limits.write_timeout
// after its first socket-write attempt closes the connection as
// writer_full").
const stallFor = time.Minute

// stallOutgoing has a client of its own, named by suffix, that reads
// nothing the broker writes (stuckClient) subscribe to stalled/# on b at
// QoS 0, and a publisher of its own store stallMessages messages of
// stallPayload on stalled/held at QoS 1, each acknowledged. Once the
// buffers between the broker and that client are full, the client's
// deliveries stay queued to its writer, which cannot finish its write, and
// hold b's outgoing budget, stallBudget: design.md, Message memory, "the
// outgoing budget, limits.max_outgoing_memory, holds deliveries queued to
// writers"; Bytes back, "A delivery reservation ends when the writer
// reports completion or discard". stallOutgoing waits until /metrics reads
// the outgoing budget with no room for another of them, the rest of them
// waiting for room, and returns the client; its Close gives the budget
// back, its writer discarding what it holds. It returns the size of one
// delivery too. b's write_timeout is stallFor, or long enough for the
// test to finish with the client stalled.
func stallOutgoing(t *testing.T, b *harness.Broker, suffix string) (stalled *harness.Client, delivery int) {
	t.Helper()
	return stallOutgoingAs(t, b, suffix, "")
}

// stallOutgoingAs is stallOutgoing, its two clients logging in as user
// (aclLogin) where user is set, as a broker with a password file needs.
func stallOutgoingAs(t *testing.T, b *harness.Broker, suffix, user string) (stalled *harness.Client, delivery int) {
	t.Helper()
	const topic = "stalled/held"
	connect := func(client string) *harness.Connect {
		if user == "" {
			return &harness.Connect{ClientID: client, CleanStart: true}
		}
		return aclLogin(client, user)
	}
	stalled = stuckClient(t, b.Addr, harness.V5)
	harness.ConnectedOver(t, stalled, connect(clientID(t, "-stalled"+suffix)))
	support.Suback(t, stalled, nil, support.At(0, "stalled/#"), 0)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, connect(clientID(t, "-stalls"+suffix)))
	payload := support.Noise(stallPayload)
	support.Unshrunk(t, payload)
	delivery = len(harness.Encoded(t, &harness.Publish{Topic: topic, Payload: payload}, harness.V5))
	if 3*delivery > stallBudget || 4*delivery <= stallBudget {
		t.Fatalf("deliveries of %d bytes against max_outgoing_memory's %d; the test needs room for three and not four", delivery, stallBudget)
	}
	for range stallMessages {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: topic, Payload: payload})
	}
	awaitAbove(t, b, inTransitOutgoing, float64(stallBudget-delivery), "a subscriber reading nothing, sent 10 MiB")
	return stalled, delivery
}

// design.md, R5: "Publishers do not wait for consumer progress. A
// publication is acknowledged after its channel stores it"; Message
// memory: "the incoming budget, limits.max_incoming_memory, holds PUBLISH
// bodies from the start of their reading until they are stored or refused";
// "the outgoing budget, limits.max_outgoing_memory, holds deliveries queued
// to writers"; "A reservation is taken from one budget and given back to
// that same budget. Neither budget lends to the other"; Two queues: "A head
// that waits holds back only its own FIFO: a delivery never waits for room
// in the incoming budget, and a body never waits for room in the outgoing
// one"; "Example: a subscriber stops reading, and its deliveries hold the
// whole outgoing budget. Publishers' bodies are still granted from the
// incoming budget, and their messages are stored and acknowledged."
// Delivery, Slow clients: "A slow client fills only its own writer".
//
// With max_outgoing_memory stallBudget and the default incoming budget, a
// subscriber of stalled/# stops reading, and its deliveries hold the
// outgoing budget, the rest of them waiting for room (stallOutgoing). Three
// publishers then each publish 20 messages at QoS 1, one after another,
// alternately on stalled/x, which the stalled subscriber is owed, and on a
// topic of their own: each is answered PUBACK 0x00, the outgoing budget
// still holding no room for another of the stalled subscriber's deliveries
// once they are all answered.
func TestStalledSubscriberNeverStallsPublishers(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxOutgoingMemory: stallBudget, WriteTimeout: stallFor})
	_, delivery := stallOutgoing(t, b, "")
	for i := range 3 {
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, fmt.Sprint("-pub", i)), CleanStart: true})
		for j := range 20 {
			topic := "stalled/x"
			if j%2 == 1 {
				topic = fmt.Sprintf("own/%d", i)
			}
			support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: topic, Payload: fmt.Appendf(nil, "%d-%d", i, j)})
		}
	}
	if got := support.Sampled(t, b, inTransitOutgoing)[inTransitOutgoing]; got <= float64(stallBudget-delivery) {
		t.Fatalf("with every publisher answered, %s reads %v: the stalled subscriber no longer held the outgoing budget, so nothing here tests the rule", inTransitOutgoing, got)
	}
}

// seriesIncoming is the max_incoming_memory of TestMemorySeriesAreEachBudgets:
// 3 MiB, room for the largest body the default max_packet_size, 1 MiB,
// asks for with its S2 copy (config.md, max_incoming_memory), and for
// stallOutgoing's bodies one at a time.
const seriesIncoming = 3 << 20

// metrics.md, Store and memory: saguin_memory_in_transit_bytes{direction},
// "outstanding reservations of each budget, incoming or outgoing (design.md,
// Message memory)"; saguin_memory_max_bytes{direction}, "each budget's
// size: limits.max_incoming_memory or limits.max_outgoing_memory";
// saguin_memory_full_seconds_total{direction}, "each budget's full time
// (design.md, Message memory), by the events' times. Includes completed
// intervals only; an ongoing interval is added when it ends". design.md,
// Message memory: "A reservation is taken from one budget and given back to
// that same budget. Neither budget lends to the other"; Asking: "Before
// allocating or reading a nonempty PUBLISH body, its reader requests the
// body's Remaining Length from the core"; Full, and an overload episode:
// "Each budget's full time is counted on its own".
//
// A broker has max_incoming_memory seriesIncoming and max_outgoing_memory
// stallBudget: /metrics reads each budget's size as its max_bytes, and
// nothing of either in transit. A client writes half of a QoS 1 PUBLISH of
// maxPacket bytes: the incoming budget holds its body's 4,093 bytes, the
// outgoing none. It finishes, PUBACK 0x00: incoming 0. A subscriber that
// reads nothing then holds the outgoing budget, the rest of its deliveries
// waiting for room (stallOutgoing), and once the bodies its publisher sent
// are stored, incoming is 0 again, its full time 0. The subscriber closes:
// outgoing in transit 0 and its full time risen, the incoming full time
// still 0.
func TestMemorySeriesAreEachBudgets(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxIncomingMemory: seriesIncoming, MaxOutgoingMemory: stallBudget, WriteTimeout: stallFor})
	maxBytes := func(direction string) string {
		return testkit.SeriesKey("saguin_memory_max_bytes", "direction", direction)
	}
	awaitSeries(t, b, map[string]float64{maxBytes("incoming"): seriesIncoming, maxBytes("outgoing"): stallBudget,
		inTransitIncoming: 0, inTransitOutgoing: 0, fullIncoming: 0, fullOutgoing: 0})
	c, rest, id := halfSent(t, b, "-half")
	awaitSeries(t, b, map[string]float64{inTransitIncoming: maxPacket - 3, inTransitOutgoing: 0})
	harness.SendBytes(t, c, rest)
	if ack, err := c.AwaitAck(harness.PUBACK, id); err != nil || ack.Reason != 0 {
		t.Fatalf("the half-sent PUBLISH, finished: %+v, %v; want PUBACK 0x00", ack, err)
	}
	awaitSeries(t, b, map[string]float64{inTransitIncoming: 0, inTransitOutgoing: 0})
	stalled, _ := stallOutgoing(t, b, "")
	awaitSeries(t, b, map[string]float64{inTransitIncoming: 0, fullIncoming: 0})
	if err := stalled.Close(); err != nil {
		t.Fatal(err)
	}
	awaitSeries(t, b, map[string]float64{inTransitOutgoing: 0})
	awaitAbove(t, b, fullOutgoing, 0, "the stalled subscriber closed")
	awaitSeries(t, b, map[string]float64{fullIncoming: 0})
}

// silentFor is the broker.limits.write_timeout of TestSilentPublisherCloses:
// short, so the test is quick, and long enough that a byte every half of it
// comes in time on a busy machine. config.md: write_timeout, like every
// duration, may be as short as 1ms.
const silentFor = 500 * time.Millisecond

// design.md, Flows, Message memory, A stalled body, as agreed for 0005b:
// "While reading a PUBLISH body, a reader that receives no byte for
// limits.write_timeout closes its connection, without a DISCONNECT";
// DISCONNECT, a lost connection: "a keep-alive of 0 is never idle-closed
// unless capped", so no keep-alive ends either connection here. design.md,
// Flows, Replies: PUBACK "sent only once the store confirms".
//
// With write_timeout silentFor and no max_keepalive, an MQTT 5 client with
// a keep-alive of 0 writes a QoS 1 PUBLISH's fixed header:
//   - with half its body, then nothing: the connection closes with nothing
//     sent, no sooner than silentFor after the last byte;
//   - then its body a byte at a time, each half of silentFor after the
//     last, a wait whose length is the behaviour: PUBACK 0x00 comes.
func TestSilentPublisherCloses(t *testing.T) {
	b := support.Launch(t, &harness.Options{WriteTimeout: silentFor})
	for _, tc := range []struct {
		name    string
		trickle bool
	}{
		{"silent with half its body sent", false},
		{"a byte every half write_timeout", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			id := c.NextID()
			// A body of 7 bytes, under 128: a fixed header of 2.
			wire := harness.Encoded(t, &harness.Publish{QoS: 1, ID: id, Topic: "t", Payload: []byte("x")}, harness.V5)
			header, body := wire[:2], wire[2:]
			if !tc.trickle {
				harness.SendBytes(t, c, append(slices.Clone(header), body[:len(body)/2]...))
				last := time.Now()
				c.Timeout = silentFor + support.Late
				harness.ExpectClosed(t, c)
				if waited := time.Since(last); waited < silentFor {
					t.Errorf("closed %v after the last byte, before write_timeout's %v", waited, silentFor)
				}
				return
			}
			harness.SendBytes(t, c, header)
			for i := range body {
				time.Sleep(silentFor / 2)
				harness.SendBytes(t, c, body[i:i+1])
			}
			if ack, err := c.AwaitAck(harness.PUBACK, id); err != nil || ack.Reason != 0 {
				t.Fatalf("the PUBLISH sent a byte every %v: %+v, %v; want PUBACK 0x00", silentFor/2, ack, err)
			}
		})
	}
}

// capped is a channel holding capped/# whose max_bytes is limit.
func capped(limit int) []harness.Channel {
	return []harness.Channel{{Name: "capped", Type: "append", Filter: "capped/#", MaxBytes: limit}}
}

// cappedRecords counts the records the database at path holds in capped
// and sums their bytes, each its topic's and its payload's (the
// publications of TestChannelMaxBytesIsExact carry no properties), the
// payload at its payload_size (storage.md, A message's bytes: "the payload
// counted at its size as received").
func cappedRecords(t *testing.T, path string) (n, size int) {
	t.Helper()
	rs := records(t, path)
	for i := range rs {
		if r := &rs[i]; r.name == "capped" {
			n, size = n+1, size+len(r.topic)+r.payloadSize+len(r.properties)
		}
	}
	return n, size
}

// design.md, Channels: "A hard byte limit counts committed charges and
// reservations for accepted writes"; config.md, channels.<name>: max_bytes
// (append), "This is the channel's hard byte limit"; storage.md, A message's
// bytes: "A message's bytes are its topic's, payload's and properties' bytes,
// the payload counted at its size as received. Retention and the size limits
// count those bytes.", properties "NULL for none".
// design.md, Flows, PUBLISH: "0x97 | over [...] a channel's max_bytes, or a
// full disk"; "Within 0x97, header count precedes retained-value capacity,
// channel capacity and, at storage, store capacity"; "At QoS 0 a
// refused publish is dropped and counted. At QoS 1 or 2 an MQTT 3.1.1 client
// has no refusal code and is disconnected"; Store, A full disk: "publishes
// are refused with 0x97"; Delivery, Message Expiry: on a configured channel
// an expired message "is still delivered [...]; only retention removes a
// record". MQTT 5 4.13.2: a PUBACK of 0x80 or above has "no consequences for
// other Sessions or other Packets flowing on the same Session".
//
// Each case runs on a broker of its own, with capped (capped/#) and an
// MQTT 5 client publishing at QoS 1 on capped/t, 8 bytes, unless it says
// otherwise:
//   - exact: max_bytes 100. A payload of 50 (58 bytes) is taken, 0x00; one
//     of 35 (43, one byte over) is refused, 0x97; one of 34 (42, the
//     limit) is taken; one of 1 is refused. capped then holds 100 bytes.
//   - competing publishers: max_bytes 200, a publish_commit_interval
//     holding accepted writes uncommitted. Eight clients each write four
//     publications of 20 bytes at once: exactly 10 are taken, the rest
//     refused 0x97, and capped holds those 10, 200 bytes.
//   - an expired publication: max_bytes 100. A payload of 42 (50 bytes),
//     expiring in a second, is taken. Once a second has passed since its
//     PUBACK, a payload of 43 (51 bytes, 101 with the 50 it holds) is
//     refused, and one of 34 (42) is taken: the expired one still holds
//     its bytes, whatever its properties add.
//   - per version and QoS: max_bytes 20, filled by a payload of 12. A
//     payload of 1 is refused: from MQTT 5 at QoS 1 with PUBACK 0x97, the
//     connection staying; at QoS 0 by nothing, the PINGRESP next; from
//     MQTT 3.1.1 at QoS 1 by the connection's close, and at QoS 0 by
//     nothing. A publication on a topic of $broadcast is taken from MQTT 5.
//     capped then holds the filler alone.
//   - a refusal at the store: on a filesystem of its own, max_bytes 50. A
//     ballast takes every byte left: a payload of 42 (50 bytes, the limit)
//     is refused 0x97, the disk full. Once the ballast is emptied, the same
//     publication is taken, the refused one holding nothing of the limit,
//     and a payload of 1 is then refused, the limit reached.
func TestChannelMaxBytesIsExact(t *testing.T) {
	t.Parallel()
	const topic = "capped/t"
	payload := func(n int) *harness.Publish { return &harness.Publish{QoS: 1, Topic: topic, Payload: make([]byte, n)} }
	t.Run("exact", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{Channels: capped(100)})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "exact", CleanStart: true})
		answered(t, c, payload(50), 0x00)
		answered(t, c, payload(35), 0x97)
		answered(t, c, payload(34), 0x00)
		answered(t, c, payload(1), 0x97)
		stopped(t, b)
		if n, size := cappedRecords(t, b.DB); n != 2 || size != 100 {
			t.Errorf("capped holds %d records of %d bytes; want the 2 taken, 100 bytes, its limit", n, size)
		}
	})
	t.Run("competing publishers", func(t *testing.T) {
		const clients, each, size, fits = 8, 4, 20, 10
		b := support.Launch(t, &harness.Options{Channels: capped(fits * size), PublishCommitInterval: 100 * time.Millisecond})
		cs := make([]*harness.Client, clients)
		ps := make([][]*harness.Publish, clients)
		for i := range cs {
			cs[i], _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, strconv.Itoa(i)), CleanStart: true})
		}
		for i, c := range cs {
			wire := make([]byte, 0, each*2*size) // room for the PUBLISHes, each under twice its message
			for range each {
				p := payload(size - len(topic))
				p.ID = c.NextID()
				ps[i] = append(ps[i], p)
				wire = append(wire, harness.Encoded(t, p, harness.V5)...)
			}
			harness.SendBytes(t, c, wire)
		}
		taken := 0
		for i, c := range cs {
			for _, p := range ps[i] {
				ack, err := c.AwaitAck(harness.PUBACK, p.ID)
				switch {
				case err != nil:
					t.Fatal(err)
				case ack.Reason == 0x00:
					taken++
				case ack.Reason != 0x97:
					t.Errorf("PUBACK 0x%02X; want 0x00 or 0x97", ack.Reason)
				}
			}
		}
		stopped(t, b)
		n, held := cappedRecords(t, b.DB)
		if taken != fits || n != fits || held != fits*size {
			t.Errorf("%d of %d publications of %d bytes taken, capped holding %d records of %d bytes; want exactly %d, %d bytes, its limit",
				taken, clients*each, size, n, held, fits, fits*size)
		}
	})
	t.Run("an expired publication", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{Channels: capped(100)})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "expired", CleanStart: true})
		short := payload(42)
		short.Props = expiry(1)
		support.Sent(t, c, short)
		expired := support.Puback(t, c, short).Add(time.Second + 10*time.Millisecond)
		time.Sleep(time.Until(expired)) // its Message Expiry runs out: the behaviour under test
		answered(t, c, payload(43), 0x97)
		answered(t, c, payload(34), 0x00)
	})
	t.Run("per version and QoS", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{Channels: capped(20)})
		v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-5", CleanStart: true})
		answered(t, v5, payload(12), 0x00)
		answered(t, v5, payload(1), 0x97)
		if err := v5.Ping(); err != nil {
			t.Fatalf("after a QoS 1 PUBLISH refused for the channel's limit: %v; want the connection to stay", err)
		}
		v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "full-3.1.1", CleanStart: true})
		for _, c := range []*harness.Client{v5, v311} {
			if _, err := c.Publish(&harness.Publish{Topic: topic, Payload: []byte{0}}); err != nil {
				t.Fatal(err)
			}
			if err := c.Ping(); err != nil {
				t.Fatalf("%s, after a QoS 0 PUBLISH over the channel's limit: %v; want the PINGRESP next, the publish dropped", harness.VersionName(c.Version), err)
			}
		}
		refused := payload(1)
		refused.ID = v311.NextID()
		if err := v311.Send(refused); err != nil {
			t.Fatal(err)
		}
		harness.ExpectClosed(t, v311)
		answered(t, v5, &harness.Publish{QoS: 1, Topic: "uncapped", Payload: []byte("x")}, 0x00)
		stopped(t, b)
		if n, size := cappedRecords(t, b.DB); n != 1 || size != 20 {
			t.Errorf("capped holds %d records of %d bytes; want the filler alone, 20 bytes", n, size)
		}
	})
	t.Run("a refusal at the store", func(t *testing.T) {
		disk := os.Getenv(support.SmallDisk)
		if disk == "" {
			testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
			return
		}
		b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), Channels: capped(50)})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "store-refused", CleanStart: true})
		ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
		answered(t, c, payload(42), 0x97)
		if err := ballast.Truncate(0); err != nil {
			t.Fatal(err)
		}
		answered(t, c, payload(42), 0x00)
		answered(t, c, payload(1), 0x97)
	})
}

// design.md, DISCONNECT, A lost connection: "The Will goes after its Will
// Delay [...] The Will is dropped and counted if [...] its channel or the
// retained values' limit is full"; "A Will is published and removed in one
// transaction". config.md, channels.<name>: max_bytes, "This is the
// channel's hard byte limit". MQTT 5 [MQTT-3.1.2-10]: "The Will Message MUST be
// removed from the stored Session State in the Server once it has been
// published". storage.md, wills: the Will a session holds, by client.
//
// A channel, capped (capped/#), has max_bytes 20, and roomy (roomy/#) none.
// A publisher fills capped with a payload of 12 on capped/t, exactly 20
// bytes. Two MQTT 5 clients, their sessions ending with their connections,
// connect with a Will, QoS 0 and no Will Delay Interval, of payload "w":
// one on capped/will, which no longer fits, and one on roomy/will. The
// database holds both Wills. Each client drops its socket: both Wills then
// leave the database. After the stop, roomy holds its Will, and capped
// holds the filler alone, the other Will published nowhere.
func TestWillOnAFullChannelIsDropped(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "capped", Type: "append", Filter: "capped/#", MaxBytes: 20},
		{Name: "roomy", Type: "append", Filter: "roomy/#"},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "filler", CleanStart: true})
	answered(t, pub, &harness.Publish{QoS: 1, Topic: "capped/t", Payload: make([]byte, 12)}, 0x00)
	wills := map[string]string{"will-capped": "capped/will", "will-roomy": "roomy/will"} // a topic by client id
	cs := make(map[string]*harness.Client, len(wills))
	for id, topic := range wills {
		cs[id], _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true,
			Will: &harness.Will{Topic: topic, Payload: []byte("w")}})
		if _, ok := support.StoredWill(t, db, id); !ok {
			t.Fatalf("after its CONNACK the database holds no Will for %q: there is nothing to fire", id)
		}
	}
	for id, c := range cs {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		awaitWillGone(t, db, id, "its socket dropped")
	}
	stopped(t, b)

	var roomy, cappedTopics []string
	for _, r := range records(t, b.DB) {
		switch r.name {
		case "roomy":
			roomy = append(roomy, r.topic)
		case "capped":
			cappedTopics = append(cappedTopics, r.topic)
		default:
			t.Errorf("%q, payload %q, is in %q; nothing was published there", r.topic, r.payload, r.name)
		}
	}
	if !slices.Equal(roomy, []string{"roomy/will"}) {
		t.Errorf("roomy holds %q; want its Will, published", roomy)
	}
	if !slices.Equal(cappedTopics, []string{"capped/t"}) {
		t.Errorf("capped holds %q; want the filler alone, the Will that did not fit dropped", cappedTopics)
	}
}
