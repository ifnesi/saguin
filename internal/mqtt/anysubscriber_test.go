package mqtt

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// AnySubscriber against the question it replaced: the broadcast log's
// "is anybody here owed this publish" (bdrain.wanted), asked through the
// selection Subscribers builds.
//
// **A disagreement is silent either way.** A false no leaves a publish out
// of the log that a durable session or a shared group was owed, so it is
// lost to them with nothing counted (invariants 11, 15 and 18: durable
// broadcast owes exactly the sessions routing found). A false yes writes to
// the log what nobody will read. So over random filter sets - wildcards,
// `$`-leading filters and topics, shared subscriptions with the share
// prefix spelt three ways, inline subscriptions that must not count, and
// unsubscribes that trim the trie - and random choices of which clients the
// drain holds and which groups have a cursor, the two answers must be the
// same for every topic asked.
//
// A partition slice is not in the index (the broker keeps it with the
// session), so a partitioned subscriber is an ordinary one here, and
// served less by the fan-out after this question is answered.
func TestAnySubscriberAnswersAsTheSelectionDoes(t *testing.T) {
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

	levels := []string{"a", "b", "c", "", "$SYS", "$x"}
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	topic := func() string {
		n := 1 + rng.Intn(4)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = pick(levels)
		}
		return strings.Join(parts, "/")
	}
	filter := func() string {
		if rng.Intn(8) == 0 {
			return "#"
		}
		n := 1 + rng.Intn(4)
		parts := make([]string, n)
		for i := range parts {
			if rng.Intn(3) == 0 {
				parts[i] = "+"
			} else {
				parts[i] = pick(levels)
			}
		}
		if rng.Intn(3) == 0 {
			parts = append(parts, "#")
		}
		return strings.Join(parts, "/")
	}
	clients := []string{"c0", "c1", "c2", "c3", "c4"}

	// What the drain holds, as bdrain.wanted asks it. The shared filters a
	// group can have a cursor on are the ones subscribed, so the choice is
	// made over those.
	var held map[string]bool
	var cursors map[string]bool
	ordinary := func(id string, sub packets.Subscription) bool { return sub.Qos > 0 && held[id] }
	shared := func(filter string) bool { return cursors[filter] }

	// Today's computation, as bdrain.wanted made it before AnySubscriber.
	bySelection := func(x *TopicsIndex, topic string) (bool, string) {
		subs := x.Subscribers(topic)
		for id, sub := range subs.Subscriptions {
			if sub.Qos > 0 && held[id] {
				return true, "ordinary"
			}
		}
		for f := range subs.Shared {
			if cursors[f] {
				return true, "shared"
			}
		}
		return false, ""
	}

	asked, yes := 0, map[string]int{}
	for round := 0; round < 3000; round++ {
		x := NewTopicsIndex()
		var sharedFilters []string
		type subscription struct{ client, filter string }
		var subscribed []subscription
		for i, n := 0, 1+rng.Intn(10); i < n; i++ {
			id := pick(clients)
			f := filter()
			switch rng.Intn(5) {
			case 0: // shared
				f = pick([]string{"$share", "$SHARE", "$Share"}) + "/" + pick([]string{"g1", "g2"}) + "/" + f
				sharedFilters = append(sharedFilters, f)
			case 1: // inline: never asked about
				x.InlineSubscribe(InlineSubscription{Subscription: packets.Subscription{Filter: f, Qos: 2, Identifier: 1 + rng.Intn(3)}})
				continue
			}
			x.Subscribe(id, packets.Subscription{Filter: f, Qos: byte(rng.Intn(3)), NoLocal: rng.Intn(2) == 0})
			subscribed = append(subscribed, subscription{id, f})
		}
		for i := 0; i < len(subscribed)/3; i++ {
			s := subscribed[rng.Intn(len(subscribed))]
			x.Unsubscribe(s.filter, s.client)
		}
		held, cursors = map[string]bool{}, map[string]bool{}
		for _, id := range clients {
			held[id] = rng.Intn(2) == 0
		}
		for _, f := range sharedFilters {
			cursors[f] = rng.Intn(2) == 0
		}

		for i := 0; i < 20; i++ {
			tp := topic()
			if rng.Intn(50) == 0 {
				tp = ""
			}
			want, why := bySelection(x, tp)
			got := x.AnySubscriber(tp, ordinary, shared)
			asked++
			if got != want {
				t.Fatalf("round %d topic %q: AnySubscriber says %v, the selection says %v (%s)\nheld %v\ncursors %v\nselection %+v",
					round, tp, got, want, why, held, cursors, x.Subscribers(tp))
			}
			if want {
				yes[why]++
				if tp[0] == '$' {
					yes["$-topic"]++
				}
			} else {
				yes["no"]++
			}
		}
	}

	// **Every class was asked about and answered yes for**, so a scan that
	// dropped one would have disagreed somewhere above.
	t.Logf("asked %d: %v", asked, yes)
	for _, k := range []string{"ordinary", "shared", "$-topic", "no"} {
		if yes[k] < 100 {
			t.Fatalf("only %d answers of kind %q in %d; the sweep did not exercise it: %v", yes[k], k, asked, yes)
		}
	}
}

// Subscriptions holds one subscription inline and makes a map only for a
// second, so every method has two shapes to answer from and the moves
// between them - to a map on a second Add, back inline when a Delete leaves
// one - are where an entry could be dropped or kept twice. Against a plain
// map, over random sequences of adds (new ids and replacements) and
// deletes (held ids and absent ones), every method must answer as the map
// does after every step, and each shape and each move must have happened.
func TestSubscriptionsAnswerAsAMapDoes(t *testing.T) {
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

	ids := []string{"a", "b", "c", ""}
	seen := map[string]int{}
	for round := 0; round < 2000; round++ {
		s, model := NewSubscriptions(), map[string]packets.Subscription{}
		for step := 0; step < 12; step++ {
			id := ids[rng.Intn(len(ids))]
			wasInline := s.internal == nil
			if rng.Intn(3) > 0 {
				sub := packets.Subscription{Filter: id + "/f", Qos: byte(rng.Intn(3)), Identifier: rng.Intn(9)}
				s.Add(id, sub)
				model[id] = sub
			} else {
				_, held := model[id]
				if got := s.Delete(id); got != held {
					t.Fatalf("round %d step %d: Delete(%q) answered %v with the id held %v", round, step, id, got, held)
				}
				delete(model, id)
			}
			switch {
			case wasInline && s.internal != nil:
				seen["to a map"]++
			case !wasInline && s.internal == nil:
				seen["back inline"]++
			}
			if s.internal == nil && s.hasOne {
				seen["inline"]++
			}

			if s.Len() != len(model) {
				t.Fatalf("round %d step %d: Len %d, the map holds %d", round, step, s.Len(), len(model))
			}
			if got := s.GetAll(); !reflect.DeepEqual(got, model) {
				t.Fatalf("round %d step %d: GetAll %v, the map holds %v", round, step, got, model)
			}
			each := map[string]packets.Subscription{}
			s.Each(func(id string, sub packets.Subscription) { each[id] = sub })
			if !reflect.DeepEqual(each, model) {
				t.Fatalf("round %d step %d: Each gave %v, the map holds %v", round, step, each, model)
			}
			for _, id := range ids {
				got, ok := s.Get(id)
				want, held := model[id]
				if ok != held || !reflect.DeepEqual(got, want) {
					t.Fatalf("round %d step %d: Get(%q) is %v %v, the map has %v %v", round, step, id, got, ok, want, held)
				}
			}
			if s.internal != nil && len(s.internal) < 2 {
				t.Fatalf("round %d step %d: a map of %d was kept where the struct holds it", round, step, len(s.internal))
			}
		}
	}
	t.Logf("%v", seen)
	for _, k := range []string{"inline", "to a map", "back inline"} {
		if seen[k] < 100 {
			t.Fatalf("only %d steps of %q: the sweep did not exercise it: %v", seen[k], k, seen)
		}
	}
}
