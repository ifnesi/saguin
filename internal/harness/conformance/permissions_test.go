package conformance

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the acl_file applied per message (item 0018e): design.md,
// Access and reload, "Readers check the acl_file at CONNECT [...] They also
// check it at SUBSCRIBE and on every publish. The core checks it on every
// delivery and when a Will fires"; Flows, PUBLISH, SUBSCRIBE, Delivery and
// DISCONNECT. Every broker here reads an acl_file beside a client password
// file (aclOptions, acl_test.go).

// permissionRoles are roles beside aclFree and aclDenials (acl_test.go):
// reader may read pub/open/# on $broadcast and events/open/# on the
// channel events, and nothing else; writer may write out/# alone; inbox
// may write inbox/# and read nothing.
const permissionRoles = `  reader:
    - topic: "pub/open/#"
      allow: [read]
    - channel: events
      filter: "events/open/#"
      allow: [read]
  writer:
    - topic: "out/#"
      allow: [write]
  inbox:
    - topic: "inbox/#"
      allow: [write]
`

// permissionACL is the acl_file of the brokers of permissionOptions: pub
// may do everything its clients do; partial reads through reader; sender
// writes through writer, and rated too, at a publish_rate of 2
// (ratedRate); inboxer writes inbox/# alone; limited is denied retained,
// qos2 and share, single qos2 alone. stranger, whom the password file
// holds, matches no entry.
const permissionACL = "roles:\n" + aclFree + aclDenials + permissionRoles + `users:
  pub: [free]
  partial: [reader]
  sender: [writer]
  rated: {roles: [writer], limits: {publish_rate: 2}}
  inboxer: [inbox]
  limited: [free, noretained, noqos2, noshare]
  single: [free, noqos2]
`

// permissionUsers are the users of permissionACL's password file.
var permissionUsers = []string{"pub", "partial", "sender", "rated", "inboxer", "limited", "single", "stranger"}

// permissionOptions is the configuration of a broker reading permissionACL,
// with an append channel events holding events/#, keeping retentionBytes
// bytes where it is not 0, and an operations listener.
func permissionOptions(t *testing.T, retentionBytes int) *harness.Options {
	t.Helper()
	o := aclOptions(t, permissionACL, permissionUsers...)
	o.Channels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: retentionBytes}}
	return o
}

// publishRefused is saguin_publish_refused_total of cause (metrics.md,
// Publishing).
func publishRefused(cause string) string {
	return `saguin_publish_refused_total{cause="` + cause + `"}`
}

// heldSeries is saguin_subscriptions_held (metrics.md, Connections and
// sessions).
const heldSeries = "saguin_subscriptions_held{}"

// pubackReasons sends ps from c at QoS 1 in one write, each under c's next
// packet identifier where it has none, and returns their PUBACKs' reasons,
// in order: a client writing them all before any answer can come.
func pubackReasons(t *testing.T, c *harness.Client, ps []*harness.Publish) []byte {
	t.Helper()
	wire := make([]byte, 0, 32*len(ps))
	for _, p := range ps {
		if p.ID == 0 {
			p.ID = c.NextID()
		}
		wire = append(wire, harness.Encoded(t, p, c.Version)...)
	}
	harness.SendBytes(t, c, wire)
	reasons := make([]byte, len(ps))
	for i, p := range ps {
		ack, err := c.AwaitAck(harness.PUBACK, p.ID)
		if err != nil {
			t.Fatal(err)
		}
		reasons[i] = ack.Reason
	}
	return reasons
}

// design.md, Flows, PUBLISH: "0x87 | the acl_file refuses the topic"; "A
// QoS 2 refusal rides the PUBREC. At QoS 0 a refused publish is dropped
// and counted. At QoS 1 or 2 an MQTT 3.1.1 client has no refusal code and
// is disconnected"; CONNECT: "A user matched by no users: pattern is
// accepted with no rights. Every publish and subscribe it makes is refused
// with 0x87, except a subscription to …/seek/reply, which seek grants";
// Access and reload: "Readers check the acl_file [...] on every
// publish". metrics.md: saguin_publish_refused_total, "refused publishes,
// QoS 0 drops included", cause not_authorized; saguin_disconnects_total,
// "for an MQTT 3.1.1 publish refusal, not_authorized". MQTT 5 3.4.2.1 and
// 3.5.2.1: PUBACK and PUBREC 0x87, Not authorized; MQTT 3.1.1
// [MQTT-3.3.5-2]: the Server "MUST either make a positive acknowledgement
// [...] or close the Network Connection".
//
// sender may write out/# alone. A subscriber holds elsewhere/#. Each row
// publishes on a connection of its own: sender at QoS 1 on elsewhere/1 and
// on the channel topic events/x, and stranger on elsewhere/2, each get
// PUBACK 0x87, the connection staying; sender at QoS 0, MQTT 5 and 3.1.1,
// gets nothing, its PINGRESP next; at QoS 1 in MQTT 3.1.1, its connection
// closes; at QoS 2 its PUBREC carries 0x87; on out/x it gets 0x00. A publication of pub's on elsewhere/end is then
// the subscriber's next packet, none refused having gone in. The refusals
// count seven not_authorized, and the MQTT 3.1.1 close one disconnect.
func TestPublishNotAllowedIsRefused(t *testing.T) {
	b := support.OpsLaunch(t, permissionOptions(t, 0))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	watch := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, "-watch"), "pub"), support.At(0, "elsewhere/#"))
	refused := 0
	for _, r := range []struct {
		name, user, topic string
		version, qos      byte
		reason            byte // the PUBACK's or PUBREC's; 0 for accepted
	}{
		{"MQTT 5, QoS 1", "sender", "elsewhere/1", harness.V5, 1, 0x87},
		{"MQTT 5, QoS 1, a channel's topic", "sender", "events/x", harness.V5, 1, 0x87},
		{"MQTT 5, QoS 1, a user matched by no entry", "stranger", "elsewhere/2", harness.V5, 1, 0x87},
		{"MQTT 5, QoS 0", "sender", "elsewhere/3", harness.V5, 0, 0x87},
		{"MQTT 3.1.1, QoS 0", "sender", "elsewhere/4", harness.V311, 0, 0x87},
		{"MQTT 3.1.1, QoS 1", "sender", "elsewhere/5", harness.V311, 1, 0x87},
		{"MQTT 5, QoS 2", "sender", "elsewhere/6", harness.V5, 2, 0x87},
		{"MQTT 5, QoS 1, a topic it may write", "sender", "out/x", harness.V5, 1, 0},
	} {
		t.Run(r.name, func(t *testing.T) {
			c, _ := harness.Connected(t, b.Addr, r.version, aclLogin(clientID(t, ""), r.user))
			if r.reason != 0 {
				refused++
			}
			p := &harness.Publish{QoS: r.qos, Topic: r.topic, Payload: []byte("x")}
			if r.version == harness.V311 && r.qos > 0 {
				p.ID = c.NextID()
				if err := c.Send(p); err != nil {
					t.Fatal(err)
				}
				harness.ExpectClosed(t, c)
				return
			}
			got, err := c.Publish(p)
			if err != nil {
				t.Fatal(err)
			}
			if r.qos > 0 && got.Reason != r.reason {
				t.Errorf("%s on %s: %s 0x%02X; want 0x%02X", r.user, r.topic, harness.Name(got.Kind), got.Reason, r.reason)
			}
			if err := c.Ping(); err != nil {
				t.Fatalf("after the PUBLISH: %v; want the PINGRESP next, the connection staying", err)
			}
		})
	}
	thenLive(t, pub, watch, "elsewhere/end")
	awaitSeries(t, b, map[string]float64{publishRefused("not_authorized"): float64(refused), disconnectSeries("not_authorized"): 1})
}

// ratedRate is the publish_rate of rated's entry in permissionACL.
const ratedRate = 2

// design.md, Flows, PUBLISH: "Policy reasons are taken in the order of the
// table above", 0x87 first; Rate limits: "Every completely received
// publish is charged at its receipt time, including one refused for
// another policy reason"; "When a charge leaves the message bucket below
// one message [...] the reader reads nothing more from its connection
// until the message bucket holds one message again"; "The reader counts
// them in buckets holding one second's worth, full at CONNECT"; Flows:
// "Policy checks select the first applicable reason in the stated order,
// independently of property order"; Access and reload: "The limits are
// publish_rate, a whole number of messages per second, and publish_bytes".
//
// rated may write out/# alone, at a publish_rate of ratedRate. Each row
// writes its MQTT 5 PUBLISHes at QoS 1 at once on a connection of its own,
// its bucket full, a PINGREQ after them (paced): one not allowed, then two
// allowed, get 0x87, 0x00 and 0x00, and the PINGRESP comes no sooner than
// two 1/ratedRate seconds after the write, the refused one charged: the
// bucket of two, spent by it and the first allowed, refills one message
// before the second is read and one more before the PINGREQ is (charging
// the allowed ones alone would take one wait). One not allowed and over
// max_topic_length (1024 by default), one not allowed with more User
// Properties than max_header_count (32 by default) and one not allowed with
// a saguin- property each get 0x87.
func TestPublishNotAllowedComesFirstAndIsCharged(t *testing.T) {
	b := support.Launch(t, permissionOptions(t, 0))
	at1 := func(topic string, props ...harness.Property) *harness.Publish {
		return &harness.Publish{QoS: 1, Topic: topic, Props: props, Payload: []byte("x")}
	}
	for _, r := range []struct {
		name string
		ps   []*harness.Publish
		want []byte
		wait time.Duration // the least the PINGRESP after them waits
	}{
		{"a refused publish is charged", []*harness.Publish{at1("elsewhere/x"), at1("out/a"), at1("out/b")}, []byte{0x87, 0, 0}, 2 * time.Second / ratedRate},
		{"0x87 before 0x90", []*harness.Publish{at1("elsewhere/" + strings.Repeat("a", 1020))}, []byte{0x87}, 0},
		{"0x87 before max_header_count's 0x97", []*harness.Publish{at1("elsewhere/x", slices.Repeat([]harness.Property{harness.User("h", "v")}, 33)...)}, []byte{0x87}, 0},
		{"0x87 before 0x83", []*harness.Publish{at1("elsewhere/x", harness.User("saguin-x", "v"))}, []byte{0x87}, 0},
	} {
		t.Run(r.name, func(t *testing.T) {
			c, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, ""), "rated"))
			took, got := paced(t, c, r.ps)
			if !slices.Equal(got, r.want) {
				t.Errorf("PUBLISHes written at once get PUBACKs % X; want % X", got, r.want)
			}
			if took < least(r.wait) {
				t.Errorf("the PINGRESP after them came %v after the write; want no sooner than %v, every PUBLISH charged", took, least(r.wait))
			}
		})
	}
}

// design.md, DISCONNECT, Reasons Sagüin sends: "It uses 0x9A, 0x9B or 0x9E
// for retained messages, QoS 2 or shared subscriptions its CONNACK
// withheld. These reasons are MQTT 5 only. An MQTT 3.1.1 connection just
// closes"; CONNECT, 8: the CONNACK carries "the Maximum QoS, Retain
// Available and Shared Subscription Available the acl_file allows";
// Flows: "Checks proceed through framing, protocol recognition and packet
// validity, admission policy, then storage". MQTT 5 [MQTT-3.2.2-14]: "A
// Client receiving Retain Available set to 0 from the Server MUST NOT send
// a PUBLISH packet with the RETAIN flag set to 1 [...] If the Server
// receives such a packet, this is a Protocol Error. The Server SHOULD send a
// DISCONNECT with Reason Code of 0x9A", whatever the topic; 3.2.2.3.4: "It
// is a Protocol Error if the Server receives a PUBLISH packet with a QoS
// greater than the Maximum QoS it specified. In this case use DISCONNECT
// with Reason Code 0x9B"; 3.2.2.3.13: "If the Server receives a SUBSCRIBE
// packet containing Shared Subscriptions and it does not support Shared
// Subscriptions, this is a Protocol Error. The Server uses DISCONNECT with
// Reason Code 0x9E". metrics.md: saguin_disconnects_total,
// retain_not_supported, qos_not_supported,
// shared_subscriptions_not_supported, "including a closure without a
// sendable DISCONNECT". ($share is refused 0x80 to every MQTT 3.1.1 client,
// SUBSCRIBE's table.)
//
// limited's roles deny retained, qos2 and share. On connections of its
// own, it sends a retained PUBLISH on w/x and on the channel topic
// events/x, a QoS 2 PUBLISH, and a SUBSCRIBE to $share/g/x: MQTT 5 gets
// DISCONNECT 0x9A, 0x9A, 0x9B and 0x9E, then the end of stream; MQTT 3.1.1
// the PUBLISHes' close, nothing sent first. Each counts under its reason's
// name. Nothing was stored: a new subscription to w/# gets no retained
// value, and one to events/#, from the floor, no record, a publication on
// each being its next packet.
func TestBreakingTheConnackDisconnects(t *testing.T) {
	b := support.OpsLaunch(t, permissionOptions(t, 0))
	for _, r := range []struct {
		name   string
		send   harness.Packet
		reason byte
		only5  bool
	}{
		{"a retained PUBLISH", retain("w/x", []byte("x")), 0x9A, false},
		{"a retained PUBLISH on a channel's topic", retain("events/x", []byte("x")), 0x9A, false},
		{"a QoS 2 PUBLISH", &harness.Publish{QoS: 2, Topic: "w/q", Payload: []byte("x")}, 0x9B, false},
		{"a $share SUBSCRIBE", &harness.Subscribe{Subs: support.At(1, "$share/g/x")}, 0x9E, true},
	} {
		for _, version := range []byte{harness.V5, harness.V311} {
			if r.only5 && version != harness.V5 {
				continue
			}
			t.Run(r.name+", "+harness.VersionName(version), func(t *testing.T) {
				c, _ := harness.Connected(t, b.Addr, version, aclLogin(clientID(t, ""), "limited"))
				switch p := r.send.(type) {
				case *harness.Publish:
					p.ID = 1
				case *harness.Subscribe:
					p.ID = 1
				}
				if err := c.Send(r.send); err != nil {
					t.Fatal(err)
				}
				if version == harness.V5 {
					if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != r.reason {
						t.Fatalf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, r.reason)
					}
				}
				harness.ExpectClosed(t, c)
			})
		}
	}
	awaitSeries(t, b, map[string]float64{
		disconnectSeries("retain_not_supported"):               4,
		disconnectSeries("qos_not_supported"):                  2,
		disconnectSeries("shared_subscriptions_not_supported"): 1,
	})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	for _, filter := range []string{"w/#", "events/#"} {
		sub := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, "-"+filter[:1]), "pub"), support.At(0, filter))
		thenLive(t, pub, sub, strings.TrimSuffix(filter, "#")+"live")
	}
}

// design.md, Access and reload: "The broker refuses a filter with 0x87 only
// when the user may read nothing the filter can match. Otherwise it grants
// the filter"; Flows,
// SUBSCRIBE, Refusals, per filter: "the broker refuses a filter with 0x87
// where the acl_file refuses the client [...] MQTT 3.1.1 gets 0x80 for
// each"; CONNECT: a user matched by no entry has every subscribe "refused
// with 0x87, except a subscription to …/seek/reply, which seek grants";
// Access and reload: a features rule "naming any of [...]
// qos2". metrics.md: saguin_subscriptions_refused_total, "refused filters,
// one per filter: not_authorized". MQTT 5 [MQTT-3.8.4-7]: each Reason Code
// "MUST either show the maximum QoS that was granted for that Subscription
// or indicate that the subscription failed"; "The Server might grant a
// lower Maximum QoS than the subscriber requested"; [MQTT-3.8.4-5]: a
// SUBSCRIBE of several filters is handled "as if it had received a
// sequence of multiple SUBSCRIBE packets"; MQTT 3.1.1 3.9.3: 0x80, Failure.
//
// partial may read pub/open/# and events/open/#. Its MQTT 5 SUBSCRIBE at
// QoS 1 of other/#, pub/open/x, pub/#, events/secret/#, events/#,
// +/open/# and inbox/# gets 0x87, 0x01, 0x01, 0x87, 0x01, 0x01 and 0x87:
// refused only where it may read nothing the filter matches. inboxer, who
// may write inbox/# but not read it, gets 0x87 for inbox/#; stranger 0x87
// for #. partial in MQTT 3.1.1 gets 0x80 and 0x01 for other/# and
// pub/open/x. single, whose qos2 is denied, asks w/x at QoS 2: granted QoS
// 1, MQTT 5 and 3.1.1. The refused filters count six not_authorized.
func TestSubscribeNotAllowedIsRefusedPerFilter(t *testing.T) {
	b := support.OpsLaunch(t, permissionOptions(t, 0))
	for _, r := range []struct {
		name, user string
		version    byte
		subs       []harness.Subscription
		want       []byte
	}{
		{"partly allowed filters are granted", "partial", harness.V5,
			support.At(1, "other/#", "pub/open/x", "pub/#", "events/secret/#", "events/#", "+/open/#", "inbox/#"),
			[]byte{0x87, 1, 1, 0x87, 1, 1, 0x87}},
		{"write alone is no read", "inboxer", harness.V5, support.At(1, "inbox/#"), []byte{0x87}},
		{"a user matched by no entry", "stranger", harness.V5, support.At(1, "#"), []byte{0x87}},
		{"MQTT 3.1.1", "partial", harness.V311, support.At(1, "other/#", "pub/open/x"), []byte{0x80, 1}},
		{"QoS 2 with qos2 denied, MQTT 5", "single", harness.V5, support.At(2, "w/x"), []byte{1}},
		{"QoS 2 with qos2 denied, MQTT 3.1.1", "single", harness.V311, support.At(2, "w/x"), []byte{1}},
	} {
		t.Run(r.name, func(t *testing.T) {
			c, _ := harness.Connected(t, b.Addr, r.version, aclLogin(clientID(t, ""), r.user))
			support.Suback(t, c, nil, r.subs, r.want...)
		})
	}
	awaitSeries(t, b, map[string]float64{subscriptionsRefused("not_authorized"): 6})
}

// design.md, Access and reload: "Otherwise it grants the filter. A record
// whose topic the user may not read is then skipped for that subscription.
// The record is not owed to it, the position moves past it, and a connected
// consumer does not count it as missed"; "An away session's losses are
// counted without the acl_file"; Words: Missed, "An away session's losses are
// counted from its stored subscriptions alone, without the acl_file";
// Delivery, Order: on $broadcast and append channels
// deliveries follow offset order. metrics.md: saguin_missed_total, "records
// consumers were owed and never received".
//
// partial may read pub/open/# and events/open/#; events keeps 150 bytes,
// so each 100-byte record's commit deletes the one before.
//   - On $broadcast, connected: partial's durable session subscribes to
//     pub/# at QoS 1, granted. pub publishes pub/secret/1, pub/open/1,
//     pub/secret/2 and pub/open/2: partial receives pub/open/1 and
//     pub/open/2. It goes away; pub publishes pub/secret/3 and pub/open/3;
//     it resumes and receives pub/open/3, then a live pub/open/end: its
//     position passed the secret records, which never come.
//   - On events, away: partial's durable session subscribes to events/# at
//     QoS 1 and goes away. pub publishes events/open/1, events/secret/1,
//     events/secret/2 and events/open/last: retention deletes the first
//     three, and the missed count is 3: an away session's losses are
//     counted without the acl_file, so the two it may not read count too.
//     The session resumes and receives events/open/last, never a secret
//     record, then a live events/open/end.
func TestPartlyAllowedFilterDeliversOnlyWhatMayBeRead(t *testing.T) {
	b := support.OpsLaunch(t, permissionOptions(t, testkit.RecordBytes+testkit.RecordBytes/2))
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	durableLogin := func(client string) *harness.Connect {
		p := aclLogin(client, "partial")
		p.Props = support.Durable
		return p
	}
	t.Run("$broadcast, connected", func(t *testing.T) {
		login := durableLogin(clientID(t, ""))
		c := support.Subscribed(t, b.Addr, harness.V5, login, support.At(1, "pub/#"))
		ps := []*harness.Publish{
			{QoS: 1, Topic: "pub/secret/1", Payload: []byte("s1")}, {QoS: 1, Topic: "pub/open/1", Payload: []byte("o1")},
			{QoS: 1, Topic: "pub/secret/2", Payload: []byte("s2")}, {QoS: 1, Topic: "pub/open/2", Payload: []byte("o2")},
		}
		support.PublishAll(t, pub, ps)
		for _, i := range []int{1, 3} {
			acknowledge(t, c, support.DeliveredAt(t, c, 1, ps[i].Topic, ps[i].Payload).ID, 0)
		}
		support.Away(t, c)
		later := []*harness.Publish{{QoS: 1, Topic: "pub/secret/3", Payload: []byte("s3")}, {QoS: 1, Topic: "pub/open/3", Payload: []byte("o3")}}
		support.PublishAll(t, pub, later)
		c = resumed(t, b.Addr, harness.V5, login)
		acknowledge(t, c, support.DeliveredAt(t, c, 1, later[1].Topic, later[1].Payload).ID, 0)
		thenLiveAt(t, pub, c, 1, "pub/open/end")
	})
	t.Run("events, away", func(t *testing.T) {
		login := durableLogin(clientID(t, ""))
		support.Away(t, support.Subscribed(t, b.Addr, harness.V5, login, support.At(1, "events/#")))
		support.AwaitAway(t, db, login.ClientID)
		last := support.Sized("events/open/last", testkit.RecordBytes)
		support.PublishAll(t, pub, []*harness.Publish{
			support.Sized("events/open/1", testkit.RecordBytes), support.Sized("events/secret/1", testkit.RecordBytes),
			support.Sized("events/secret/2", testkit.RecordBytes), last,
		})
		awaitHeld(t, db, "events", []string{last.Topic}, "once events/open/last is stored")
		committed(t, pub, "barrier")
		expectSample(t, b, testkit.MissedSeries("events"), 3, "once retention has passed three records of an away session, counted without the acl_file")
		c := resumed(t, b.Addr, harness.V5, login)
		acknowledge(t, c, support.DeliveredAt(t, c, 1, last.Topic, last.Payload).ID, 0)
		thenLiveAt(t, pub, c, 1, "events/open/end")
	})
}

// design.md, Access and reload: "A record whose topic the user may not read
// is then skipped for that subscription"; Flows, SUBSCRIBE, the subscription
// table: $broadcast "Starts with | the matching retained values, as Retain
// Handling allows, then the next message"; The start point: "The captured
// boundary separates the initial pass, which reads retained or current
// values below it, from subsequent live records". (latest channels' current
// values are not built yet, 0015a: this covers $broadcast's retained
// values alone.)
//
// pub publishes retained values on pub/open/a and pub/secret/b. partial,
// who may read pub/open/#, subscribes to pub/#, granted: it receives the
// retained value of pub/open/a alone, then a live pub/open/live.
func TestInitialPassSendsOnlyWhatMayBeRead(t *testing.T) {
	b := support.Launch(t, permissionOptions(t, 0))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	support.Acknowledged(t, pub, retain("pub/open/a", []byte("open")))
	support.Acknowledged(t, pub, retain("pub/secret/b", []byte("secret")))
	c := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, ""), "partial"), support.At(0, "pub/#"))
	retainedPass(t, c, map[string][]byte{"pub/open/a": []byte("open")})
	thenLive(t, pub, c, "pub/open/live")
}

// heldACL is the acl_file of TestHeldSubscriptionKeepsItsPlace: watcher
// may read the channel events where allowed, and misc/# always.
func heldACL(allowed bool) string {
	watch := "  watch:\n    - topic: \"misc/#\"\n      allow: [read]\n"
	if allowed {
		watch += "    - channel: events\n      allow: [read]\n"
	}
	return "roles:\n" + aclFree + watch + "users:\n  pub: [free]\n  watcher: [watch]\n"
}

// design.md, Access and reload: "When the rules no longer allow a stored
// subscription at all, the subscription is held. It receives nothing and
// keeps its position until a rule allows it again"; Flows, SUBSCRIBE, Refusals: "A subscriber no
// longer allowed is held, not skipped"; One copy: "A permission hold does
// not advance the position past an owed record"; Restart, Restored: the
// broker restores "durable sessions with their subscriptions, positions".
// metrics.md: saguin_subscriptions_held, "subscriptions whose deliveries
// are held because the acl_file no longer allows them".
//
// watcher's durable session subscribes to events/# at QoS 1 and receives
// events/a/1; it goes away and the broker stops. It starts again with an
// acl_file allowing watcher misc/# alone: watcher resumes, its
// subscription held (saguin_subscriptions_held 1), and pub publishes two
// records on events/a/meanwhile, of which watcher receives nothing within
// a second. It goes away, and the broker starts again with the first
// acl_file: watcher resumes and receives the two, where it stopped, and
// nothing is held.
func TestHeldSubscriptionKeepsItsPlace(t *testing.T) {
	t.Parallel()
	o := aclOptions(t, heldACL(true), "pub", "watcher")
	o.Channels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}
	restart := func(b *harness.Broker, allowed bool) *harness.Broker {
		t.Helper()
		stopped(t, b)
		testkit.WriteFile(t, o.Dir, "acl.yaml", heldACL(allowed))
		return support.OpsLaunch(t, o)
	}
	login := aclLogin(clientID(t, ""), "watcher")
	login.Props = support.Durable
	b := support.OpsLaunch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	c := support.Subscribed(t, b.Addr, harness.V5, login, support.At(1, "events/#"))
	first := &harness.Publish{QoS: 1, Topic: "events/a/1", Payload: []byte("1")}
	support.Acknowledged(t, pub, first)
	acknowledge(t, c, support.DeliveredAt(t, c, 1, first.Topic, first.Payload).ID, 0)
	support.Away(t, c)

	b = restart(b, false)
	c = resumed(t, b.Addr, harness.V5, login)
	awaitSeries(t, b, map[string]float64{heldSeries: 1})
	pub, _ = harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	meanwhile := numbered("events/a/meanwhile", 2)
	support.PublishAll(t, pub, meanwhile)
	nothingWithin(t, c, time.Second, "with its one subscription no longer allowed")
	support.Away(t, c)

	b = restart(b, true)
	c = resumed(t, b.Addr, harness.V5, login)
	for _, p := range meanwhile {
		acknowledge(t, c, support.DeliveredAt(t, c, 1, p.Topic, p.Payload).ID, 0)
	}
	awaitSeries(t, b, map[string]float64{heldSeries: 0})
}

// willsACL is the acl_file of TestFiringWillIsCheckedAgainstItsUser: in
// the first, keep, lost and mute may write everywhere; in the second, lost
// may write elsewhere/# alone and mute's will is denied. pub and owner may
// write everywhere, and taker out/# alone, in both.
func willsACL(first bool) string {
	users := "  keep: [free]\n  lost: [free]\n  mute: [free]\n"
	if !first {
		users = "  keep: [free]\n  lost: [elsewhere]\n  mute: [free, nowill]\n"
	}
	return "roles:\n" + aclFree + aclDenials + permissionRoles + "  elsewhere:\n    - topic: \"elsewhere/#\"\n      allow: [write]\n" +
		"users:\n  pub: [free]\n  owner: [free]\n  taker: [writer]\n" + users
}

// design.md, DISCONNECT, A lost connection: "The Will is dropped and
// counted if the acl_file no longer allows its topic or the will feature";
// "It is read from the store when it goes"; Access and reload: "The core
// checks it [...] when a Will fires"; Restart: "A Will whose client was
// connected at the stop is due its Will Delay Interval after the start";
// CONNECT, 6: "Clean start ends an existing session and publishes its
// delayed Will". storage.md, wills: user, "the proved user name captured
// when the Will was accepted [...]: the acl_file is checked against it
// when the Will fires". metrics.md: saguin_wills_dropped_total,
// not_authorized; saguin_wills_published_total, restart.
//
// A durable subscriber holds wills/# at QoS 1 and goes away. keep, lost
// and mute connect, their sessions durable, each with a Will at QoS 1 on
// wills/<user> and a Will Delay Interval of a second, and are connected at
// a graceful stop. The broker starts again with an acl_file in which lost
// may no longer write wills/# and mute's will is denied: keep's Will is
// published, the other two dropped and counted, and the store holds none.
// The subscriber resumes and receives keep's Will, then a live wills/end.
// owner then connects, durable, with a Will on wills/owner delayed an
// hour, and taker, who may not write wills/#, takes its client id over
// with Clean Start: owner's Will, checked against owner, is the
// subscriber's next packet.
func TestFiringWillIsCheckedAgainstItsUser(t *testing.T) {
	t.Parallel()
	o := aclOptions(t, willsACL(true), "pub", "owner", "taker", "keep", "lost", "mute")
	o.Channels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}
	b := support.OpsLaunch(t, o)
	subLogin := aclLogin(clientID(t, "-sub"), "pub")
	subLogin.Props = support.Durable
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, subLogin, support.At(1, "wills/#")))
	withWill := func(user, client string, delay uint32) *harness.Connect {
		p := aclLogin(client, user)
		p.Props = support.Durable
		p.Will = &harness.Will{QoS: 1, Topic: "wills/" + user, Payload: support.WillPayload, Props: support.WillDelay(delay)}
		return p
	}
	users := []string{"keep", "lost", "mute"}
	for _, user := range users {
		harness.Connected(t, b.Addr, harness.V5, withWill(user, clientID(t, "-"+user), 1))
	}
	stopped(t, b)
	testkit.WriteFile(t, o.Dir, "acl.yaml", willsACL(false))
	b = support.OpsLaunch(t, o)
	awaitSeries(t, b, map[string]float64{willsDropped("not_authorized"): 2, willsPublished("restart"): 1})
	db := support.OpenDB(t, b.DB)
	for _, user := range users {
		awaitWillGone(t, db, clientID(t, "-"+user), "the restart")
	}
	sub := resumed(t, b.Addr, harness.V5, subLogin)
	acknowledge(t, sub, support.DeliveredAt(t, sub, 1, "wills/keep", support.WillPayload).ID, 0)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	thenLiveAt(t, pub, sub, 1, "wills/end")

	owner, _ := harness.Connected(t, b.Addr, harness.V5, withWill("owner", clientID(t, "-owned"), support.Hour))
	harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-owned"), "taker"))
	if d := harness.Expect[*harness.Disconnect](t, owner); d.Reason != 0x8E {
		t.Fatalf("owner, taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	support.DeliveredAt(t, sub, 1, "wills/owner", support.WillPayload)
}

// design.md, Access and reload: "%u (the proved user name) and %c (the
// client id) are substituted in filters and topics. A value holding +, #
// or / makes that rule grant nothing"; "A channel rule may add filter: to
// narrow its reach within the channel"; Flows, PUBLISH: "0x87 | the
// acl_file refuses the topic"; SUBSCRIBE: 0x87 "where the acl_file refuses
// the client"; config.md, password_file: a user name "does not start with
// #".
//
// The one role grants write and read on home/%u/# and dev/%c/#, and write
// on the channel events within events/%u/#. Each row is one MQTT 5
// connection, its user and client id, publishing at QoS 1 or subscribing
// at QoS 1:
//   - alice as dev-1 writes home/alice/x, dev/dev-1/x and events/alice/x
//     (0x00), not home/bob/x, dev/dev-2/x or events/bob/x (0x87), and
//     subscribes to home/alice/#, home/bob/#, dev/dev-1/# and dev/+/x:
//     0x01, 0x87, 0x01, 0x01;
//   - alice as +, as # and as a/b: dev/%c/# grants nothing, so dev/other/x
//     and dev/a/b/x are refused 0x87, a subscription to dev/+/x 0x87,
//     while home/alice/x stays allowed;
//   - + and a/b, as plus-dev and ab-dev: home/%u/# grants nothing, so
//     home/bob/x and home/a/b/x are refused 0x87 and a subscription to
//     home/+/x 0x87, while dev/plus-dev/x stays allowed.
func TestSubstitutedRules(t *testing.T) {
	acl := `roles:
  own:
    - topic: "home/%u/#"
      allow: [write, read]
    - topic: "dev/%c/#"
      allow: [write, read]
    - channel: events
      filter: "events/%u/#"
      allow: [write]
users:
  "*": [own]
`
	o := aclOptions(t, acl, "alice", "+", "a/b")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	type publish struct {
		topic  string
		reason byte
	}
	for _, r := range []struct {
		user, client string
		publishes    []publish
		subs         []string
		granted      []byte
	}{
		{"alice", "dev-1", []publish{{"home/alice/x", 0}, {"home/bob/x", 0x87}, {"dev/dev-1/x", 0}, {"dev/dev-2/x", 0x87},
			{"events/alice/x", 0}, {"events/bob/x", 0x87}},
			[]string{"home/alice/#", "home/bob/#", "dev/dev-1/#", "dev/+/x"}, []byte{1, 0x87, 1, 1}},
		{"alice", "+", []publish{{"dev/other/x", 0x87}, {"home/alice/x", 0}}, []string{"dev/+/x"}, []byte{0x87}},
		{"alice", "#", []publish{{"dev/other/x", 0x87}, {"home/alice/x", 0}}, []string{"dev/+/x"}, []byte{0x87}},
		{"alice", "a/b", []publish{{"dev/a/b/x", 0x87}, {"home/alice/x", 0}}, []string{"dev/+/x"}, []byte{0x87}},
		{"+", "plus-dev", []publish{{"home/bob/x", 0x87}, {"dev/plus-dev/x", 0}}, []string{"home/+/x"}, []byte{0x87}},
		{"a/b", "ab-dev", []publish{{"home/a/b/x", 0x87}, {"dev/ab-dev/x", 0}}, []string{"home/+/x"}, []byte{0x87}},
	} {
		t.Run(fmt.Sprintf("%s as %s", r.user, r.client), func(t *testing.T) {
			c, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(r.client, r.user))
			for _, p := range r.publishes {
				ack, err := c.Publish(&harness.Publish{QoS: 1, Topic: p.topic, Payload: []byte("x")})
				if err != nil {
					t.Fatal(err)
				}
				if ack.Reason != p.reason {
					t.Errorf("PUBLISH %q: PUBACK 0x%02X, want 0x%02X", p.topic, ack.Reason, p.reason)
				}
			}
			support.Suback(t, c, nil, support.At(1, r.subs...), r.granted...)
		})
	}
}
