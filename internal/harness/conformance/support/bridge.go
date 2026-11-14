package support

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Quiet is the CONNACK accepting a bridge with a Server Keep Alive of
// 65,535 seconds: its first PINGREQ is due after half that, so none comes
// in the waits of a test that is not about keep-alive (design.md, Bridges:
// "A Server Keep Alive replaces it").
var Quiet = &harness.Connack{Props: harness.Properties{{ID: harness.ServerKeepAlive, Int: 65_535}}}

// QuietResumed is Quiet with Session Present 1: the peer kept the bridge's
// session from an earlier link (MQTT 5 3.2.2.1.1).
var QuietResumed = &harness.Connack{SessionPresent: true, Props: Quiet.Props}

// BridgeTo is a bridge named name to peer, with Sagüin's client id at the
// peer bridge-<name>, and rules; every other key at config.md's default.
func BridgeTo(name string, peer *harness.Peer, rules ...harness.BridgeRule) harness.Bridge {
	return harness.Bridge{Name: name, Peer: peer.URL(), ClientID: "bridge-" + name, Topics: rules}
}

// In is an in rule for filter at qos, whose topic template takes each of
// the filter's levels again under from/: a + level as $1, $2 and so on, the
// # as $# (design.md, Bridges: "In a topic template, $1, $2 and so on stand
// for the filter's + levels. [...] The $# stands for the filter's # and
// must be the topic's whole last level").
func In(filter string, qos byte) harness.BridgeRule {
	levels := strings.Split(filter, "/")
	plus := 0
	for i, level := range levels {
		switch level {
		case "+":
			plus++
			levels[i] = "$" + strconv.Itoa(plus)
		case "#":
			levels[i] = "$#"
		}
	}
	return harness.BridgeRule{Filter: filter, Topic: "from/" + strings.Join(levels, "/"), Direction: "in", QoS: qos}
}

// Grants is the SUBACK granting each subscription of s the QoS it asked for
// (MQTT 5 3.9.3: "Granted QoS 0", "Granted QoS 1", "Granted QoS 2").
func Grants(s *harness.Subscribe) *harness.Suback {
	reasons := make([]byte, len(s.Subs))
	for i, sub := range s.Subs {
		reasons[i] = sub.QoS
	}
	return &harness.Suback{ID: s.ID, Reasons: reasons}
}

// LinkUp takes the next link that reaches peer, answers its CONNECT with
// ack and the SUBSCRIBE that follows with Grants, and returns the link, the
// CONNECT and the SUBSCRIBE, failing the test where either does not come
// first or within harness.DefaultTimeout: a link up with every inbound
// filter granted (design.md, Bridges: "Every new link subscribes all its
// filters afresh").
func LinkUp(t *testing.T, peer *harness.Peer, ack *harness.Connack) (*harness.PeerLink, *harness.Connect, *harness.Subscribe) {
	t.Helper()
	l := peer.Link(t)
	c, _, _ := l.Accept(t, ack)
	s, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	if _, err := l.Send(Grants(s)); err != nil {
		t.Fatalf("granting the bridge's SUBSCRIBE: %v", err)
	}
	return l, c, s
}

// Filters is the filters a SUBSCRIBE carries, in its order.
func Filters(s *harness.Subscribe) []string {
	filters := make([]string, len(s.Subs))
	for i, sub := range s.Subs {
		filters[i] = sub.Filter
	}
	return filters
}

// Between fails the test unless what took took, a wait the design bounds,
// lies within least and most, room included by the caller: a bound read
// from the peer's own receipt times.
func Between(t *testing.T, what string, took, least, most time.Duration) {
	t.Helper()
	if took < least || took > most {
		t.Errorf("%s took %v; want %v to %v", what, took, least, most)
	}
}

// Ending reads l's arrivals within limit until the link's end, failing the
// test unless the end comes next, after at most one DISCONNECT, and returns
// the first of the two, when the bridge ended the link, and the DISCONNECT,
// nil where none came: a bridge ending a link may first tell its peer why.
func Ending(t *testing.T, l *harness.PeerLink, limit time.Duration) (harness.Arrival, *harness.Disconnect) {
	t.Helper()
	first, err := l.Next(limit)
	if err != nil {
		t.Fatalf("waiting for the bridge to end the link: %v", err)
	}
	d, ok := first.Packet.(*harness.Disconnect)
	switch {
	case ok:
		l.Ended(t, limit)
	case first.Packet != nil:
		t.Fatalf("the peer received %s, want the link ended", first)
	}
	return first, d
}

// Inbound launches a broker of the test's own with o and one bridge, named
// name, to a peer of the test's own, with rules (BridgeTo), and brings the
// bridge's first link up (LinkUp, its CONNACK Quiet): the peer can then
// send what its own clients published to the filters the bridge asked for.
// It returns the broker, the peer and the link.
func Inbound(t *testing.T, o *harness.Options, name string, rules ...harness.BridgeRule) (*harness.Broker, *harness.Peer, *harness.PeerLink) {
	t.Helper()
	b, peer := bridgedTo(t, o, name, rules)
	l, _, _ := LinkUp(t, peer, Quiet)
	return b, peer, l
}

// Outbound is Inbound for a bridge whose rules are all out: the link up
// once the peer has answered its CONNECT with ack, no SUBSCRIBE following,
// as the bridge has no inbound filter to ask for (design.md, Bridges, In:
// "the bridge subscribes at the peer in one SUBSCRIBE carrying every
// inbound filter"). Local clients then publish what crosses to the peer.
func Outbound(t *testing.T, o *harness.Options, name string, ack *harness.Connack, rules ...harness.BridgeRule) (*harness.Broker, *harness.Peer, *harness.PeerLink) {
	t.Helper()
	b, peer := bridgedTo(t, o, name, rules)
	return b, peer, Accepted(t, peer, ack)
}

// bridgedTo launches a broker of the test's own with o and one bridge,
// named name, to a peer of the test's own, with rules (BridgeTo), and
// returns the broker and the peer.
func bridgedTo(t *testing.T, o *harness.Options, name string, rules []harness.BridgeRule) (*harness.Broker, *harness.Peer) {
	t.Helper()
	peer := harness.ListenedPeer(t)
	own := *o // the caller's Options stay as they were
	own.Bridges = []harness.Bridge{BridgeTo(name, peer, rules...)}
	return Launch(t, &own), peer
}

// Accepted takes the next link that reaches peer and answers its CONNECT
// with ack, failing the test where no CONNECT comes first: the link of a
// bridge whose rules are all out is up then.
func Accepted(t *testing.T, peer *harness.Peer, ack *harness.Connack) *harness.PeerLink {
	t.Helper()
	l := peer.Link(t)
	l.Accept(t, ack)
	return l
}

// Out is an out rule: the records filter matches locally cross to the
// peer under the topic template topic, at qos (config.md, bridges.<name>:
// "A topic template is required for in and out").
func Out(filter, topic string, qos byte) harness.BridgeRule {
	return harness.BridgeRule{Filter: filter, Topic: topic, Direction: "out", QoS: qos}
}

// QuietWith is Quiet carrying props too, with Session Present 1 where
// resumed: the peer's CONNACK capping what the bridge sends it (MQTT 5
// 3.2.2.3: Receive Maximum, Maximum QoS, Retain Available, Maximum Packet
// Size).
func QuietWith(resumed bool, props ...harness.Property) *harness.Connack {
	return &harness.Connack{SessionPresent: resumed, Props: append(slices.Clone(Quiet.Props), props...)}
}

// PeerTakes reads l's arrivals, within harness.DefaultTimeout each, until n
// PUBLISHes have come and each of their QoS 2 exchanges is complete,
// failing the test on any other arrival, and answers each as it comes, as
// a live peer does: a QoS 1 PUBLISH with PUBACK 0x00 (MQTT 5 4.3.2: the
// receiver "MUST respond with a PUBACK packet containing the Packet
// Identifier from the incoming PUBLISH packet" [MQTT-4.3.2-4]); a QoS 2
// one with PUBREC 0x00 [MQTT-4.3.3-8], then the bridge's PUBREL 0x00 under
// its packet identifier, which must follow [MQTT-4.3.3-4], with PUBCOMP
// 0x00 [MQTT-4.3.3-11]. It returns the PUBLISHes in the order they came.
func PeerTakes(t *testing.T, l *harness.PeerLink, n int) []*harness.Publish {
	t.Helper()
	got := make([]*harness.Publish, 0, n)
	owed := map[uint16]bool{} // the QoS 2 exchanges awaiting the bridge's PUBREL
	for len(got) < n || len(owed) > 0 {
		a, err := l.Next(harness.DefaultTimeout)
		if err != nil {
			t.Fatalf("the peer awaiting %d PUBLISHes and the PUBRELs of %d QoS 2 exchanges: %v", n-len(got), len(owed), err)
		}
		switch p := a.Packet.(type) {
		case *harness.Publish:
			if len(got) == n {
				t.Fatalf("the peer received %s; want the PUBRELs of %d QoS 2 exchanges first", a, len(owed))
			}
			switch p.QoS {
			case 1:
				PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: p.ID})
			case 2:
				PeerSends(t, l, &harness.Ack{Kind: harness.PUBREC, ID: p.ID})
				owed[p.ID] = true
			}
			got = append(got, p)
		case *harness.Ack:
			if p.Kind != harness.PUBREL || !owed[p.ID] || p.Reason != 0x00 {
				t.Fatalf("the peer received %s; want PUBREL 0x00 under the packet identifier of a QoS 2 PUBLISH it answered with PUBREC 0x00", a)
			}
			delete(owed, p.ID)
			PeerSends(t, l, &harness.Ack{Kind: harness.PUBCOMP, ID: p.ID})
		default:
			t.Fatalf("the peer received %s; want a PUBLISH, or PUBREL to a QoS 2 PUBLISH it answered", a)
		}
	}
	return got
}

// Crossed fails the test unless got, a PUBLISH the peer received from the
// bridge, is a first sending (DUP 0 [MQTT-3.3.1-1]) on topic at qos of
// payload.
func Crossed(t *testing.T, got *harness.Publish, topic string, qos byte, payload []byte) {
	t.Helper()
	if got.Topic != topic || got.QoS != qos || got.Dup || !bytes.Equal(got.Payload, payload) {
		t.Errorf("the peer received PUBLISH %q at QoS %d, DUP %v, of %.40q; want %q at QoS %d, DUP 0, of %.40q",
			got.Topic, got.QoS, got.Dup, got.Payload, topic, qos, payload)
	}
}

// PeerSends has the peer send p to the bridge on l, and returns when the
// sending began.
func PeerSends(t *testing.T, l *harness.PeerLink, p harness.Packet) time.Time {
	t.Helper()
	at, err := l.Send(p)
	if err != nil {
		t.Fatalf("the peer sending %s: %v", harness.Name(p.Type()), err)
	}
	return at
}

// PeerSendsBytes has the peer write b to the bridge on l as it is, part of
// a packet or several, and returns when the sending began.
func PeerSendsBytes(t *testing.T, l *harness.PeerLink, b []byte) time.Time {
	t.Helper()
	at, err := l.SendRaw(b)
	if err != nil {
		t.Fatalf("the peer sending %d bytes: %v", len(b), err)
	}
	return at
}

// PeerAck reads l's next arrival, within harness.DefaultTimeout, failing
// the test unless it is the bridge's acknowledgement of kind for packet
// identifier id carrying reason, and returns its arrival.
func PeerAck(t *testing.T, l *harness.PeerLink, kind byte, id uint16, reason byte) harness.Arrival {
	t.Helper()
	ack, a := harness.Arrive[*harness.Ack](t, l, harness.DefaultTimeout)
	if ack.Kind != kind || ack.ID != id || ack.Reason != reason {
		t.Fatalf("the peer received %s; want %s %d 0x%02X", a, harness.Name(kind), id, reason)
	}
	return a
}

// PeerAnswered has the peer send p, at QoS 1 or 2, and fails the test
// unless the bridge's first answer to it carries reason: PUBACK at QoS 1,
// PUBREC at QoS 2 (MQTT 5 4.3.2: the receiver "MUST respond with a PUBACK
// packet containing the Packet Identifier from the incoming PUBLISH packet
// [MQTT-4.3.2-4]"; 4.3.3: it "MUST respond with a PUBREC containing the
// Packet Identifier from the incoming PUBLISH packet [MQTT-4.3.3-8]"). It
// returns the answer's arrival.
func PeerAnswered(t *testing.T, l *harness.PeerLink, p *harness.Publish, reason byte) harness.Arrival {
	t.Helper()
	PeerSends(t, l, p)
	return PeerAck(t, l, harness.AnswerKind(p.QoS), p.ID, reason)
}

// PeerReleases has the peer send PUBREL for packet identifier id, and fails
// the test unless the bridge's PUBCOMP to it carries reason (MQTT 5 4.3.3:
// the receiver "MUST respond to a PUBREL packet by sending a PUBCOMP packet
// containing the same Packet Identifier as the PUBREL [MQTT-4.3.3-11]").
func PeerReleases(t *testing.T, l *harness.PeerLink, id uint16, reason byte) harness.Arrival {
	t.Helper()
	PeerSends(t, l, &harness.Ack{Kind: harness.PUBREL, ID: id})
	return PeerAck(t, l, harness.PUBCOMP, id, reason)
}

// Bridged has the peer send p and complete its exchange, failing the test
// unless every answer is a success: nothing answers QoS 0; QoS 1 gets
// PUBACK 0x00; QoS 2 gets PUBREC 0x00, then, to the peer's PUBREL, PUBCOMP
// 0x00 (design.md, Flows, PUBLISH: "1 | on PUBLISH | PUBACK once stored";
// QoS 2 "PUBREC, then PUBCOMP once in the channel").
func Bridged(t *testing.T, l *harness.PeerLink, p *harness.Publish) {
	t.Helper()
	if p.QoS == 0 {
		PeerSends(t, l, p)
		return
	}
	PeerAnswered(t, l, p, 0x00)
	if p.QoS == 2 {
		PeerReleases(t, l, p.ID, 0x00)
	}
}

// AwaitAcknowledged waits until b's bridge has counted n records its peer
// acknowledged since b started: metrics.md, Bridges,
// saguin_bridge_sent_total{bridge}, counter, "records the peer
// acknowledged, or sent at QoS 0"; Rules: "Counters start at 0." b must
// run its operations listener. A peer's PUBACK sent is not one the broker
// has handled: one still unread when b stops leaves its record in flight,
// sent again with DUP 1 on the next link, as MQTT 5 4.4 has a session
// resend what is unacknowledged on reconnecting [MQTT-4.4.0-1].
func AwaitAcknowledged(t *testing.T, b *harness.Broker, bridge string, n int) {
	t.Helper()
	AwaitSampled(t, b, fmt.Sprintf("bridge %s acknowledged %d records", bridge, n),
		map[string]float64{testkit.SeriesKey("saguin_bridge_sent_total", "bridge", bridge): float64(n)})
}
