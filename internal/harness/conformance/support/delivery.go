package support

import (
	"bytes"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// Subscribed connects a client of its own to addr in version with p, and
// subscribes it to subs, failing the test unless each is granted the QoS
// it asks (design.md, Flows, SUBSCRIBE: $broadcast, "Granted for QoS 0 / 1
// / 2 | 0 / 1 / 2"; MQTT 3.1.1's return codes are the same values).
func Subscribed(t *testing.T, addr string, version byte, p *harness.Connect, subs []harness.Subscription) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, addr, version, p)
	granted := make([]byte, len(subs))
	for i := range subs {
		granted[i] = subs[i].QoS
	}
	Suback(t, c, nil, subs, granted...)
	return c
}

// DeliveredAt reads c's next packet and fails the test unless it is a
// first delivery at qos of topic and payload, which it returns: a PUBLISH
// whose DUP is 0, as a QoS 1 PUBLISH sent for a new message has it
// [MQTT-4.3.2-2] (the harness decoder has refused DUP at QoS 0 already
// [MQTT-3.3.1-2]).
func DeliveredAt(t *testing.T, c *harness.Client, qos byte, topic string, payload []byte) *harness.Publish {
	t.Helper()
	p := harness.Expect[*harness.Publish](t, c)
	switch {
	case p.QoS != qos:
		t.Fatalf("PUBLISH %q at QoS %d; want QoS %d, the lower of the publication's QoS and the highest grant", p.Topic, p.QoS, qos)
	case p.Dup:
		t.Fatalf("PUBLISH %q at QoS %d with DUP 1; want DUP 0 on a first delivery [MQTT-4.3.2-2]", p.Topic, p.QoS)
	case p.Topic != topic || !bytes.Equal(p.Payload, payload):
		t.Fatalf("PUBLISH %q of payload %.40q; want %q of %.40q", p.Topic, p.Payload, topic, payload)
	}
	return p
}

// PublishAll sends ps from c in order, without waiting between them, then
// waits for the PUBACK 0x00 of each at QoS 1: one connection's packets
// enter the core in their order (design.md, Execution, Decisions: "Each
// connection's packet order is preserved"), so their offsets follow it.
func PublishAll(t *testing.T, c *harness.Client, ps []*harness.Publish) {
	t.Helper()
	for _, p := range ps {
		if p.QoS > 0 {
			Sent(t, c, p)
		} else if err := c.Send(p); err != nil {
			t.Fatalf("PUBLISH %q at QoS 0: %v", p.Topic, err)
		}
	}
	for _, p := range ps {
		if p.QoS > 0 {
			Puback(t, c, p)
		}
	}
}
