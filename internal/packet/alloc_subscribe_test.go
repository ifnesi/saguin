package packet_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
)

// Allocations of the subscribing codecs, by alloc_test.go's standard: a
// decoder allocates only what the packet keeps beyond the read buffer, and
// an encoder writing into a buffer with room allocates nothing. A
// SUBSCRIBE, UNSUBSCRIBE, SUBACK or UNSUBACK keeps nothing beyond the read
// buffer: its properties, filters and reason codes are the buffer's own
// bytes, read in place. Each is handled per packet, the busy path.

// Sinks keep the last value a pass reads, so the compiler cannot drop the
// reads, as the reader keeps what it hands to the core.
var (
	subscriptionSink packet.Subscription
	filterSink       []byte
)

// manySubscriptions returns n subscriptions to distinct filters, each with
// options, for a SUBSCRIBE carrying a long list.
func manySubscriptions(n int) []packet.Subscription {
	subs := make([]packet.Subscription, n)
	for i := range subs {
		subs[i] = sub(fmt.Sprintf("sensors/%d/#", i), byte(i%3), i%2 == 0, i%3 == 0, byte(i%3))
	}
	return subs
}

// TestDecodeSubscribingDoesNotAllocate: decoding a SUBSCRIBE or an
// UNSUBSCRIBE, and a pass over its properties through the property
// iterator (readEveryProperty) and over its filters through All, allocate
// nothing, however many filters and User Properties it carries: MQTT 5
// 3.8.3, "a list of Topic Filters"; 3.8.2.1.3, "The User Property is
// allowed to appear multiple times". Decoding a SUBACK allocates nothing
// either. Every valid case of each decoder is held to it.
func TestDecodeSubscribingDoesNotAllocate(t *testing.T) {
	specs := slices.Clone(validSubscribes)
	unsubs := slices.Clone(validUnsubscribes)
	for _, n := range []int{100, 10_000} {
		users := slices.Repeat([]wired{userProp("key", "value")}, n)
		subs := manySubscriptions(n)
		specs = append(specs, subscribeSpec{
			name: fmt.Sprintf("%d filters and User Properties", n), version: packet.V5, id: 1,
			props: slices.Concat([]wired{intProp(0x0B, 7)}, users), subs: subs,
		})
		filters := make([][]byte, n)
		for i := range subs {
			filters[i] = subs[i].Filter
		}
		unsubs = append(unsubs, unsubscribeSpec{
			name: fmt.Sprintf("%d filters and User Properties", n), version: packet.V5, id: 1, props: users, filters: filters,
		})
	}
	noAllocsDecoding(t, casesOf(specs, (*subscribeSpec).caseOf), func(s packet.Subscribe) {
		readEveryProperty(s.Properties)
		for subscription := range s.Subscriptions.All() {
			subscriptionSink = subscription
		}
	})
	noAllocsDecoding(t, casesOf(unsubs, (*unsubscribeSpec).caseOf), func(u packet.Unsubscribe) {
		readEveryProperty(u.Properties)
		for filter := range u.Filters.All() {
			filterSink = filter
		}
	})
	noAllocsDecoding(t, validReplies, func(reply) {})
}

// TestAppendSubscribingDoesNotAllocate writes every valid SUBSCRIBE, SUBACK
// and UNSUBACK into a buffer with exactly the packet's room.
func TestAppendSubscribingDoesNotAllocate(t *testing.T) {
	noAllocsWriting(t, casesOf(validSubscribes, (*subscribeSpec).caseOf))
	noAllocsWriting(t, validReplies)
}
