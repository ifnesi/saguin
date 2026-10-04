package broker

import (
	"io"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// The turned-round subscription indexes, byTopic and members, against the
// one they are derived from.
//
// **A disagreement is silent.** A client missing from byTopic is connected,
// subscribed, holding its position, and never sent another record; nothing
// errors and nothing is counted. So the oracle is subs itself, read the slow
// way - every client, every filter, channel.Matches - after every change a
// random sequence of subscribes, unsubscribes, disconnects and session ends
// makes, and the two answers must be the same set.
//
// Topics are never `$`-leading here. A channel's filter cannot begin with
// `$` (channel.ValidFilter), so no record topic can, and the trie's
// [MQTT-4.7.2-1] rule for them is one the oracle would otherwise have to
// restate for a case that cannot happen.
//
// Order is never compared: which order the candidates come back in was
// never a promise, and the map walk before this was random too.
func TestTheTopicIndexesAgreeWithTheSubscriptions(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q is not a number", v)
		}
		seed = n
	}
	t.Logf("seed %d; replay with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append},
		{Name: "state", Type: channel.Latest},
		{Name: "jobs", Type: channel.Queue},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	channels := []*channel.Channel{reg.Get("events"), reg.Get("state"), reg.Get("jobs")}

	// Overlapping filters on purpose: two of a client's filters can reach the
	// same channel, one filter can reach two, a pin reaches a queue by name,
	// and bcast/# reaches none.
	filters := []string{
		"events/#", "events/+", "events/a/#", "events/a/b", "events/+/b", "events",
		"state/#", "state/x", "state/+/z", "#", "+/a/#",
		"$saguin/queue/jobs", "bcast/#",
	}
	topics := []string{
		"events", "events/a", "events/a/b", "events/z/b", "events/a/b/c",
		"state/x", "state/y/z", "state", "jobs/1",
	}
	clients := []string{"c0", "c1", "c2", "c3", "c4", "c5"}

	counts := map[string]int{}
	check := func(op string) {
		t.Helper()
		b.mu.Lock()
		defer b.mu.Unlock()
		// The plans, a third copy: what each client's subscriptions ask of
		// each append channel, rebuilt the slow way from subs and partitions.
		for _, id := range clients {
			want := map[string][]planFilter{}
			wantQoS := map[string]byte{}
			for _, s := range b.subs[id] {
				if s.channel.Type != channel.Append {
					continue
				}
				want[s.channel.Name] = append(want[s.channel.Name], planFilter{
					pumpFilter:        pumpFilter{s.filter, b.partitions[id][s.filter], s.noLocal},
					retainAsPublished: s.retainAsPublished, identifier: s.identifier, spans: s.spans,
				})
				wantQoS[s.channel.Name] = max(wantQoS[s.channel.Name], s.qos)
			}
			var got map[string]*channelPlan
			if con := b.lookupConsumer(id); con != nil {
				con.cmu.Lock()
				got = con.plans
				con.cmu.Unlock()
			}
			if len(got) != len(want) {
				t.Fatalf("after %s: %s has plans for %d channels, subs says %d", op, id, len(got), len(want))
			}
			for ch, fs := range want {
				p := got[ch]
				if p == nil || p.qos != wantQoS[ch] || !samePlanFilters(p.filters, fs) {
					t.Fatalf("after %s: %s's plan for %s is %+v, subs and partitions say %+v (qos %d) - "+
						"a batch would be matched against filters it no longer holds", op, id, ch, p, fs, wantQoS[ch])
				}
				counts["plans"]++
			}
		}
		for _, c := range channels {
			var want []string
			for id, subs := range b.subs {
				for _, s := range subs {
					if s.channel.Name == c.Name {
						want = append(want, id)
						break
					}
				}
			}
			var got []string
			for id := range b.members[c.Name] {
				got = append(got, id)
			}
			if !sameSet(got, want) {
				t.Fatalf("after %s: members of %s are %v, subs says %v", op, c.Name, sortedIDs(got), sortedIDs(want))
			}
			for _, topic := range topics {
				var want []string
				for id, subs := range b.subs {
					for _, s := range subs {
						if s.channel.Name == c.Name && channel.Matches(s.filter, topic) {
							want = append(want, id)
							break
						}
					}
				}
				got := b.matchingLocked(c, topic)
				if !sameSet(got, want) {
					t.Fatalf("after %s: %s on %s matches %v in the trie, subs says %v - "+
						"a subscriber the trie misses is never sent this record",
						op, topic, c.Name, sortedIDs(got), sortedIDs(want))
				}
				counts["matches"] += len(want)
			}
		}
	}

	for range 3000 {
		id := clients[rng.Intn(len(clients))]
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4:
			f := filters[rng.Intn(len(filters))]
			b.track(id, f, byte(rng.Intn(2)), false, false, false, 0)
			counts["track"]++
			check("track " + id + " " + f)
		case 5, 6:
			f := filters[rng.Intn(len(filters))]
			b.mu.Lock()
			b.forgetFilters(id, []packets.Subscription{{Filter: f}})
			b.mu.Unlock()
			counts["forgetFilters"]++
			check("forgetFilters " + id + " " + f)
		case 7, 8:
			b.mu.Lock()
			b.forgetSubscriptions(id)
			b.mu.Unlock()
			counts["forgetSubscriptions"]++
			check("forgetSubscriptions " + id)
		case 9:
			b.mu.Lock()
			b.forgetClient(id)
			b.mu.Unlock()
			counts["forgetClient"]++
			check("forgetClient " + id)
		}
		// A partition declared or withdrawn on one of the filters, which
		// changes the plans without changing subs.
		if rng.Intn(4) == 0 {
			f := filters[rng.Intn(len(filters))]
			part := partition{}
			if rng.Intn(2) == 0 {
				part = partition{count: 2, indices: []int{rng.Intn(2)}}
			}
			b.declare(id, f, part)
			counts["declare"]++
			check("declare " + id + " " + f)
		}
	}

	// Every client gone, so every trie is empty and every node it grew has
	// been trimmed back to nothing a topic can reach (invariant 13).
	b.mu.Lock()
	for _, id := range clients {
		b.forgetClient(id)
	}
	b.mu.Unlock()
	check("forgetClient everyone")
	if len(b.members) != 0 || len(b.indexed) != 0 {
		t.Errorf("with every client gone, members holds %d channels and indexed %d clients",
			len(b.members), len(b.indexed))
	}

	t.Logf("drove %v", counts)
	for _, op := range []string{"track", "forgetFilters", "forgetSubscriptions", "forgetClient", "declare", "plans"} {
		if counts[op] == 0 {
			t.Errorf("the sequence never called %s, so what it says about that writer is nothing", op)
		}
	}
	if counts["matches"] == 0 {
		t.Error("no topic ever matched a subscription, so every comparison was of two empty sets")
	}
}

func sameSet(a, b []string) bool {
	return slices.Equal(sortedIDs(a), sortedIDs(b))
}

func sortedIDs(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func samePlanFilters(a, b []planFilter) bool {
	return slices.EqualFunc(a, b, func(x, y planFilter) bool {
		return samePumpFilters([]pumpFilter{x.pumpFilter}, []pumpFilter{y.pumpFilter}) &&
			x.retainAsPublished == y.retainAsPublished && x.identifier == y.identifier && x.spans == y.spans
	})
}

// A filter lying inside a queue's own filter is never recorded as a worker.
// Nothing sends one to track today - the SUBACK refuses it, and so does a
// restore - which is how a branch there that recorded one as a worker went
// unnoticed: the failure the branch after it calls invariant 4's, an ordinary
// subscriber offered jobs. Asked directly, it meets the queue as every
// intersecting filter does. The queue's pin, the control, is the one form
// that makes a worker.
func TestAFilterInsideAQueueIsNeverAWorker(t *testing.T) {
	b := metricsBroker(t)
	jobs := b.reg.Get("jobs")
	for _, f := range []string{"jobs/x", "jobs/+", "jobs/#"} {
		b.track("subscriber", f, 1, false, false, false, 0)
	}
	if ids := b.workersOf(jobs); len(ids) != 0 {
		t.Fatalf("filters inside the queue's own made %v workers of it", ids)
	}
	b.track("worker", "$saguin/queue/jobs", 1, false, false, false, 0)
	if ids := b.workersOf(jobs); len(ids) != 1 || ids[0] != "worker" {
		t.Fatalf("the queue's pin made %v its workers, want [worker], so this proves nothing", ids)
	}
}
