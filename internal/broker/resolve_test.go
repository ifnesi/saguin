package broker

// The recipient resolver, held to a naive walk of the source of truth.
//
// **The oracle is `subs`, not the resolver's own previous shape.** A record's
// recipients are, by definition, the clients holding a subscription on that
// channel whose filter matches the topic - that is the rule, and `subs` is
// where saguin keeps it. `byTopic` is a projection of `subs` maintained by
// `reindex`, and this asks whether the projection can be trusted to answer
// the question directly rather than as a hint to be re-checked.
//
// It exists because the walk is being changed to stop re-deriving what the
// index already knows, and the only honest way to make that change is to
// have a test that fails on the semantics rather than on the numbers. It
// outlives the change: it is the standing statement of what the index owes
// `subs`, which is the obligation the index's one-writer rule creates.
//
// Randomised, with the seed printed and SAGUIN_RANDOM_SEED honoured.

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
)

// resolverSeed is the run's seed, printed either way so a failure can be
// repeated exactly.
func resolverSeed(t *testing.T) int64 {
	t.Helper()
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q: %v", v, err)
		}
		seed = n
	}
	t.Logf("seed %d", seed)
	return seed
}

// naiveRecipients is the oracle: every client holding a subscription on this
// channel whose filter matches, found by walking subs and nothing else.
func naiveRecipients(b *Broker, c *channel.Channel, topic string) []string {
	var out []string
	for id, subs := range b.subs {
		for _, s := range subs {
			if s.channel.Name == c.Name && channel.Matches(s.filter, topic) {
				out = append(out, id)
				break
			}
		}
	}
	slices.Sort(out)
	return out
}

// resolverBroker is a broker with a channel and nothing running, so the
// index and the map can be driven directly.
func resolverBroker(t *testing.T) (*Broker, *channel.Channel) {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append, Filter: "events/#"},
		{Name: "other", Type: channel.Append, Filter: "other/#"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return b, reg.Get("events")
}

// **Every shape that has ever caused a delivery bug here**: overlapping
// filters on one client, single-level and multi-level wildcards, a filter
// on a different channel that must not match, and clients holding several
// subscriptions of which only one matches.
func TestTheIndexAnswersTheSameRecipientsAsAWalkOfSubs(t *testing.T) {
	rng := rand.New(rand.NewSource(resolverSeed(t)))
	b, c := resolverBroker(t)
	other := b.reg.Get("other")

	filters := []string{
		"events/#", "events/+/reading", "events/line/+/reading",
		"events/line/1/#", "events/line/1/reading", "events/+/+/+",
		"events/line/2/reading", "events/#",
	}
	otherFilters := []string{"other/#", "other/line/+/reading"}

	// A hundred clients, each holding one to three subscriptions drawn from
	// the set, some on a channel this record is not for.
	b.mu.Lock()
	for i := range 100 {
		id := fmt.Sprintf("c-%d", i)
		for range 1 + rng.Intn(3) {
			ch, f := c, filters[rng.Intn(len(filters))]
			if rng.Intn(5) == 0 {
				ch, f = other, otherFilters[rng.Intn(len(otherFilters))]
			}
			b.subs[id] = append(b.subs[id], subscription{
				filter: f, channel: ch, qos: byte(rng.Intn(2)),
			})
		}
		b.reindex(id)
	}
	b.mu.Unlock()

	topics := []string{
		"events/line/1/reading", "events/line/2/reading", "events/line/3/reading",
		"events/line/1/status", "events/other/reading", "events/a/b/c",
		"events/deep/a/b/c/d", "other/line/1/reading",
	}
	checked := 0
	for _, topic := range topics {
		b.mu.Lock()
		got := slices.Clone(b.matchingLocked(c, topic))
		b.mu.Unlock()
		slices.Sort(got)
		got = slices.Compact(got)

		want := naiveRecipients(b, c, topic)
		checked++
		if !slices.Equal(got, want) {
			t.Errorf("topic %q: the index answers %d recipients and a walk of subs answers "+
				"%d.\n\tindex: %v\n\t subs: %v", topic, len(got), len(want), got, want)
		}
	}
	if checked == 0 {
		t.Fatal("no topic was compared, so this test proved nothing")
	}
	t.Logf("%d topics compared against a walk of subs, over %d clients", checked, len(b.subs))
}

// **A client with two matching filters is one recipient, not two.** This is
// the regression the change most plausibly introduces: the index yields per
// subscription, so a walk that forgets to fold them gives one client two
// deliveries - which MQTT 5 forbids for overlapping subscriptions, and
// which no throughput number would ever show.
func TestAClientWithOverlappingFiltersIsOneRecipient(t *testing.T) {
	b, c := resolverBroker(t)
	b.mu.Lock()
	b.subs["dup"] = []subscription{
		{filter: "events/#", channel: c, qos: 0},
		{filter: "events/line/+/reading", channel: c, qos: 1},
		{filter: "events/line/1/reading", channel: c, qos: 1},
	}
	b.reindex("dup")
	got := slices.Clone(b.matchingLocked(c, "events/line/1/reading"))
	b.mu.Unlock()

	if len(got) != 1 {
		t.Fatalf("a client holding three filters that all match one topic was answered as "+
			"%d recipients: %v.\n\tOverlapping subscriptions are one delivery, at the "+
			"merged QoS - two deliveries is a duplicate no rate figure would show", len(got), got)
	}
	if got[0] != "dup" {
		t.Fatalf("the recipient is %q", got[0])
	}
}
