package support

import (
	"bytes"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// At returns a subscription to each of filters At qos, its other options
// 0.
func At(qos byte, filters ...string) []harness.Subscription {
	subs := make([]harness.Subscription, len(filters))
	for i, f := range filters {
		subs[i] = harness.Subscription{Filter: f, QoS: qos}
	}
	return subs
}

// Suback sends from c a SUBSCRIBE of subs carrying props, under the next
// packet identifier, and fails the test unless the SUBACK with that
// identifier carries want: "The SUBACK packet MUST have the same Packet
// Identifier as the SUBSCRIBE packet" (MQTT 5 [MQTT-3.8.4-2]; 3.1.1
// [MQTT-3.8.4-2]), "a Reason Code for each Topic Filter" [MQTT-3.8.4-6]
// (3.1.1: a return code [MQTT-3.8.4-5]), in "the order of Topic Filters in
// the SUBSCRIBE packet" [MQTT-3.9.3-1]. Deliveries read on the way are kept
// for Recv.
func Suback(t *testing.T, c *harness.Client, props harness.Properties, subs []harness.Subscription, want ...byte) {
	t.Helper()
	s := &harness.Subscribe{ID: c.NextID(), Props: props, Subs: subs}
	if err := c.Send(s); err != nil {
		t.Fatalf("SUBSCRIBE %.40q: %v", FiltersOf(subs), err)
	}
	p, err := c.Await(func(p harness.Packet) bool {
		a, ok := p.(*harness.Suback)
		return ok && a.ID == s.ID
	})
	if err != nil {
		t.Fatalf("SUBSCRIBE %.40q: waiting for its SUBACK: %v", FiltersOf(subs), err)
	}
	if got := p.(*harness.Suback).Reasons; !bytes.Equal(got, want) {
		t.Fatalf("SUBSCRIBE %.40q: SUBACK % X, want % X", FiltersOf(subs), got, want)
	}
}

// FiltersOf returns the filters of subs, for messages.
func FiltersOf(subs []harness.Subscription) []string {
	filters := make([]string, len(subs))
	for i := range subs {
		filters[i] = subs[i].Filter
	}
	return filters
}
