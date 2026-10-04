package broker_test

// Scenarios nobody wrote.
//
// **Every other test in this repository drives a case somebody thought of.**
// That is what a table test is for and it is not nothing - but it can only
// ever prove the cases that were imagined, and the defects that survive
// review are by definition in the cases that were not. This test generates
// a topology instead: channels whose filters overlap in ways nobody chose,
// topics that land on the seams between them, and consumers whose filters
// cross several channels at once. Then it checks the result against the
// rules rather than against an expected answer.
//
// **The model here is written from RFC 0002's prose and does not call into
// `internal/channel`.** That is the whole discipline: a probe that asks the
// code under test what the answer should be is asserting that the code
// agrees with itself. Where the two disagree, one of them is wrong and the
// test says which topic and which filters, so the disagreement can be read.
//
// **The second oracle is the other storage provider.** Every scenario runs
// twice - the same channels, consumers and writes on a broker holding them
// in memory and again on one holding them on SQLite - and what came back
// is compared. The RFC model can say which records must arrive; it has no
// opinion about most of what a record carries, and enumerating that by
// hand is how a column gets forgotten: the sqlite upsert that kept a
// replaced value's content type passed every listed assertion, because
// nobody had listed content type. Against the other provider nothing needs
// listing - a disagreement is a defect in one of them by definition, and
// the memory store, which replaces records whole, is usually the one that
// is right.
//
// **Reproducible from its output.** A random failure that cannot be replayed
// is a rumour. The seed is printed on every run and taken from
// SAGUIN_RANDOM_SEED, so a failure here is re-run exactly:
//
//	SAGUIN_RANDOM_SEED=1724764800 go test ./internal/broker/ -run Random
//
// **No build tag and no skip**, for the reason the link-churn soak has
// none: a randomised test excluded at compile time stops building and says
// nothing, and one that skips compiles and then quietly asserts nothing.
// Short in the ordinary suite, longer when SAGUIN_RANDOM_ROUNDS asks.
//
// **And it proves it drove something.** The failure this shape is prone to
// is not a wrong assertion but a silent one: a generator that produced one
// channel and no overlap, or a publisher whose topics reached nobody,
// passes every check below while testing none of them. The counters at the
// end are asserted, and they are what stops this becoming a green tick over
// work nobody did.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// The vocabulary a scenario is built from. It is small on purpose: dense
// overlap is what stresses the rule that decides which channel holds a
// topic, and a wide vocabulary would mostly generate filters that meet
// nothing.
var (
	randSites = []string{"alpha", "beta"}
	randKinds = []string{"events", "state", "work", "health"}
	randIDs   = []string{"d1", "d2"}
)

// randomTopic is `iot/<site>/<kind>/<id>` - four levels, the shape RFC 0002's
// own examples use, so a generated filter can differ from another at any of
// the three that vary.
func randomTopic(rng *rand.Rand) string {
	return strings.Join([]string{
		"iot",
		randSites[rng.Intn(len(randSites))],
		randKinds[rng.Intn(len(randKinds))],
		randIDs[rng.Intn(len(randIDs))],
	}, "/")
}

// randomFilter is a topic with some levels blurred: a literal level, `+`, or
// a `#` tail that ends the filter. Level 0 is always `iot`, so every filter
// meets every topic somewhere and the interesting comparisons actually
// happen.
func randomFilter(rng *rand.Rand) string {
	levels := []string{"iot"}
	for _, choices := range [][]string{randSites, randKinds, randIDs} {
		switch rng.Intn(4) {
		case 0:
			// A `#` tail ends the filter and covers everything below.
			return strings.Join(append(levels, "#"), "/")
		case 1, 2:
			levels = append(levels, "+")
		default:
			levels = append(levels, choices[rng.Intn(len(choices))])
		}
	}
	return strings.Join(levels, "/")
}

// modelMatches is MQTT topic matching, written from the specification rather
// than taken from internal/channel - which is the point of it existing.
//
// `#` matches the level it sits at and every level below, and only appears
// last. `+` matches exactly one level. Everything else is literal.
func modelMatches(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := range f {
		if f[i] == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if f[i] != "+" && f[i] != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// modelRank is how exactly one filter level spells a topic out: a literal
// level beats `+`, and `+` beats `#`. RFC 0002 "Which channel a topic
// belongs to".
func modelRank(level string) int {
	switch level {
	case "#":
		return 0
	case "+":
		return 1
	default:
		return 2
	}
}

// modelMoreExact compares two filters the way RFC 0002 says an operator can
// read off their own configuration: the first level at which they differ
// decides, and a filter that has run out of levels is inside a `#` and
// ranks lowest from there on.
//
// Positive means a is the more exact, negative b, zero neither - and zero
// for two filters that both match a topic is the case the registry's
// duplicate-filter refusal rests on, so it is asserted rather than assumed.
func modelMoreExact(a, b string) int {
	al, bl := strings.Split(a, "/"), strings.Split(b, "/")
	n := len(al)
	if len(bl) > n {
		n = len(bl)
	}
	for i := 0; i < n; i++ {
		ar, br := 0, 0
		if i < len(al) {
			ar = modelRank(al[i])
		}
		if i < len(bl) {
			br = modelRank(bl[i])
		}
		if ar != br {
			return ar - br
		}
	}
	return 0
}

// modelHolder is the channel a published topic belongs to, or "" for
// broadcast: of every filter that matches, the one that spells the topic out
// most exactly.
func modelHolder(chans []*channel.Channel, topic string) *channel.Channel {
	var best *channel.Channel
	for _, c := range chans {
		if !modelMatches(c.Filter, topic) {
			continue
		}
		if best == nil || modelMoreExact(c.Filter, best.Filter) > 0 {
			best = c
		}
	}
	return best
}

// randomChannels builds a channel set whose filters overlap and which the
// registry will accept.
//
// **Duplicate filters are dropped rather than retried**, because two
// channels carrying the same filter is a startup error naming both
// (invariant 12) and generating one would be testing the registry's
// refusal, which has a test of its own. What this wants is a topology that
// starts.
//
// A queue is included only sometimes: its filter must be reachable by no
// wildcard subscription, so a scenario with one asserts a different rule
// from a scenario without, and both are worth generating.
func randomChannels(rng *rand.Rand) []*channel.Channel {
	seen := map[string]bool{}
	var chans []*channel.Channel
	names := 0
	for len(chans) < 2+rng.Intn(4) {
		f := randomFilter(rng)
		if seen[f] {
			continue
		}
		seen[f] = true
		names++
		c := &channel.Channel{Name: fmt.Sprintf("ch%d", names), Filter: f}
		switch rng.Intn(5) {
		case 0:
			c.Type = channel.Queue
			c.VisibilityTimeout = int64(visibility / time.Second)
			c.MaxAttempts = maxAttempts
			c.JobExpiresAfter = brokertest.JobExpiry
		case 1, 2:
			c.Type = channel.Latest
		default:
			c.Type = channel.Append
		}
		chans = append(chans, c)
	}
	return chans
}

// deliverable reports whether a scenario can put a record in front of the
// consumers this test attaches, which is not the same as whether the broker
// works.
//
// **A queue's records go to a worker's pin, not to an ordinary subscriber**
// (invariant 11, and the scope note above TestRandomScenariosHoldTheRules
// says so in as many words). So a round whose published topics all land on
// queues delivers nothing here by construction, however correct the broker
// is. The generator makes one channel in five a queue, so such a topology
// is uncommon and reachable, and it was: a run on 2026-09-08 drew seven
// queues, delivered nothing, and failed as though the round had been
// unlucky rather than inapplicable.
//
// A declared slice is modelled too, because a consumer taking one receives
// only its share and a scenario whose writes all fall outside it is the
// same kind of non-scenario. Without that this would report a slice that
// excluded everything as loss.
func deliverable(chans []*channel.Channel, s *randScript) bool {
	for _, w := range s.writes {
		if h := modelHolder(chans, w.topic); h != nil && h.Type == channel.Queue {
			continue
		}
		for ci, filters := range s.consumers {
			if sl := s.slices[ci]; sl.count > 0 && ownerOf(w.topic, sl.count) != sl.index {
				continue
			}
			for _, f := range filters {
				if modelMatches(f, w.topic) {
					return true
				}
			}
		}
	}
	return false
}

// randomRounds is how many scenarios to run: a couple in the ordinary
// suite, or SAGUIN_RANDOM_ROUNDS when somebody wants a longer hunt. The
// short figure is chosen to reach the paths rather than to hammer them.
func randomRounds(t *testing.T) int {
	t.Helper()
	v := os.Getenv("SAGUIN_RANDOM_ROUNDS")
	if v == "" {
		return 3
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("SAGUIN_RANDOM_ROUNDS=%q is not a positive number of rounds", v)
	}
	t.Logf("random: %d rounds", n)
	return n
}

// randomSeed is the run's seed, taken from SAGUIN_RANDOM_SEED when a
// failure is being replayed and from the clock otherwise. It is logged
// either way, so the line that reproduces a failure is in the output of the
// run that found it rather than something to work out afterwards.
func randomSeed(t *testing.T) int64 {
	t.Helper()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q is not a number", v)
		}
		return n
	}
	return time.Now().UnixNano()
}

// randCounts is what the run proves it drove, added up across rounds and
// both providers. The shape's failure mode is silence - see the counter
// assertions at the end of the test.
type randCounts struct {
	deliveries int // records delivered to any consumer
	appends    int // append records checked for arrival
	latests    int // latest values checked against the model
	props      int // deliveries carrying a content type at all
	compared   int // records and values compared across the two providers
	sliced     int // append records owed to a consumer that declared a slice
}

// boolToInt is one where a condition holds, for the counters above.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// randWrite is one publish a scenario will make, drawn before either
// broker starts so both are driven with exactly the same one.
type randWrite struct {
	topic, payload string
	props          *paho.PublishProperties
}

// randScript is a whole scenario, drawn up front for the same reason: the
// comparison at the end is only about the providers if everything else -
// consumers, filters, writes, properties - is the same on both.
type randScript struct {
	consumers [][]string // the filters each consumer will offer

	// slices is the partition each consumer declares, one per consumer and
	// mostly the zero value - a count of 0 meaning it declared nothing,
	// which is what almost every client does.
	//
	// **Drawn with the rest of the script rather than per broker**, for the
	// reason everything else here is: the comparison at the end is only
	// about the two providers if the consumers ask them the same questions.
	slices []randSlice
	writes []randWrite
	// latestTopics is every written topic a latest channel holds, in first-
	// write order, and lastLatest the model's final value for each - what a
	// fresh subscriber must be served.
	latestTopics []string
	lastLatest   map[string]string
}

// randSlice is one consumer's declared partition, or the zero value for a
// consumer that declared none.
type randSlice struct {
	count int
	index int
}

// holds is whether a topic belongs to this consumer's slice, computed
// from RFC 0003's algorithm rather than from the broker's function - the
// same independence the end-to-end tests keep, for the same reason: taking
// the broker's own hash would make this agree with whatever it does.
func (r randSlice) holds(topic string) bool {
	if r.count == 0 {
		return true
	}
	const offset64, prime64 = uint64(14695981039346656037), uint64(1099511628211)
	h := offset64
	for i := 0; i < len(topic); i++ {
		h ^= uint64(topic[i])
		h *= prime64
	}
	// The RFC's second block, which the slice is actually taken from.
	h ^= h >> 30
	h *= 13787848793156543929
	h ^= h >> 27
	h *= 10723151780598845931
	h ^= h >> 31
	return int(h%uint64(r.count)) == r.index
}

// makeScript draws the consumers and writes for one round.
func makeScript(rng *rand.Rand, chans []*channel.Channel) *randScript {
	s := &randScript{lastLatest: map[string]string{}}
	for i := 0; i < 2+rng.Intn(2); i++ {
		var filters []string
		for n := 0; n < 1+rng.Intn(2); n++ {
			filters = append(filters, randomFilter(rng))
		}
		s.consumers = append(s.consumers, filters)

		// **One consumer in three takes a slice.** Rarely enough that most
		// rounds still drive the undeclared path, which is the one every
		// existing client uses, and often enough that a run of any length
		// crosses partitioning with generated filters and a generated
		// topology - which is the combination no table test reaches.
		var sl randSlice
		if rng.Intn(3) == 0 {
			sl.count = 2 + rng.Intn(3)
			sl.index = rng.Intn(sl.count)
		}
		s.slices = append(s.slices, sl)
	}
	for i := 0; i < 12; i++ {
		w := randWrite{topic: randomTopic(rng), payload: fmt.Sprintf("p%d", i),
			props: randProps(rng, i)}
		s.writes = append(s.writes, w)
		if h := modelHolder(chans, w.topic); h != nil && h.Type == channel.Latest {
			if _, seen := s.lastLatest[w.topic]; !seen {
				s.latestTopics = append(s.latestTopics, w.topic)
			}
			s.lastLatest[w.topic] = w.payload
		}
	}
	return s
}

// randProps is the publish properties write i carries. Each value is
// unique to its write, because uniqueness is what makes a kept old value
// visible: a store that leaves a replaced record's content type in place
// is then serving a value provably not this write's. Each is also
// sometimes absent, because absent-after-present is the other way a
// replace can go stale - an old value kept where the new write carried
// nothing.
//
// Message Expiry is never set: it is delivered decremented by the time the
// record has waited, so two sequential runs read two clocks and every
// comparison would be about scheduling rather than about the stores.
func randProps(rng *rand.Rand, i int) *paho.PublishProperties {
	p := &paho.PublishProperties{}
	if rng.Intn(3) > 0 {
		p.ContentType = fmt.Sprintf("app/c%d", i)
	}
	if rng.Intn(3) == 0 {
		pf := byte(rng.Intn(2))
		p.PayloadFormat = &pf
	}
	if rng.Intn(2) == 0 {
		p.ResponseTopic = fmt.Sprintf("reply/w%d", i)
	}
	if rng.Intn(2) == 0 {
		p.CorrelationData = []byte(fmt.Sprintf("corr%d", i))
	}
	if rng.Intn(2) == 0 {
		p.User = append(p.User, paho.UserProperty{Key: "tag", Value: fmt.Sprintf("t%d", i)})
	}
	return p
}

// scenarioAnswer is what one provider's broker answered a scenario with:
// which filters each consumer was granted, everything each consumer
// received, and what a fresh subscriber is served as each latest topic's
// current value - nil where it was served nothing.
type scenarioAnswer struct {
	filters   [][]string
	delivered [][]received
	latest    map[string]*received
}

func (a *scenarioAnswer) subscribedAnything() bool {
	for _, f := range a.filters {
		if len(f) > 0 {
			return true
		}
	}
	return false
}

// driveScenario runs one scenario against one broker and records what it
// answered. It asserts nothing itself: the rules are checked against the
// answer once per provider, and then the two answers are compared - which
// is the reason driving and checking are separate at all.
func driveScenario(t *testing.T, prov string, h *harness, round int, script *randScript) *scenarioAnswer {
	t.Helper()
	ans := &scenarioAnswer{latest: map[string]*received{}}

	var cons []*client
	for i, filters := range script.consumers {
		cl := connect(t, h, fmt.Sprintf("r%d-%s-c%d", round, prov, i), true, false)
		sl := script.slices[i]
		var accepted []string
		for _, f := range filters {
			// A declared slice goes on its own SUBSCRIBE, because the
			// properties are packet-level: sending several filters in one
			// packet would declare the same slice for all of them, which is
			// a different scenario from the one drawn.
			if sl.count > 0 {
				if sa := cl.SubSliced(t, f, sl.count, sl.index); sa != nil && sa.Reasons[0] > 2 {
					continue
				}
				accepted = append(accepted, f)
				continue
			}
			// A filter wholly inside a queue is refused 0x8F, which is
			// invariant 11 working; such a subscription is not part of this
			// scenario. The refusal comes from the registry, not the store,
			// so the comparison below holds the two providers to refusing
			// alike.
			if sa := cl.Sub(t, f, 1); sa != nil && sa.Reasons[0] > 2 {
				continue
			}
			accepted = append(accepted, f)
		}
		cons = append(cons, cl)
		ans.filters = append(ans.filters, accepted)
	}

	pub := connect(t, h, fmt.Sprintf("r%d-%s-pub", round, prov), true, false)
	for _, w := range script.writes {
		pub.PubProps(t, w.topic, w.payload, w.props)
	}
	settle(t, pub)
	time.Sleep(400 * time.Millisecond)

	for _, cl := range cons {
		var got []received
		for _, r := range cl.All() {
			if r.Topic == "settle/barrier" {
				continue
			}
			got = append(got, r)
		}
		ans.delivered = append(ans.delivered, got)
	}

	// A latest channel's current value, asked for by a fresh subscriber
	// rather than inferred from what the consumers above saw - a subscriber
	// arriving afterwards is the reader "current state" is a promise to.
	for i, topic := range script.latestTopics {
		late := connect(t, h, fmt.Sprintf("r%d-%s-late-%d", round, prov, i), true, false)
		late.Sub(t, topic, 1)
		if got, ok := late.Await(t, 3*time.Second); ok {
			g := got
			ans.latest[topic] = &g
		} else {
			ans.latest[topic] = nil
		}
	}
	return ans
}

// **A generated topology, checked against the rules rather than against an
// expected answer.**
//
// Each round builds a channel set nobody wrote, attaches consumers whose
// filters cross it in ways nobody chose, publishes topics that land on the
// seams, and then holds the broker to four things that must be true of any
// topology at all:
//
//   - **Nothing arrives that the consumer did not ask for.** Every delivered
//     topic matches one of that consumer's own filters.
//
//   - **A wildcard never reaches a queue** (invariant 11). No ordinary
//     subscriber receives a record whose topic a queue holds, whatever its
//     filter crosses.
//
//   - **Nothing the consumer asked for goes missing.** Every append record
//     whose topic one of its filters matches arrives - duplicates are
//     at-least-once keeping its promise, an absence is loss (invariant 1).
//
//     **Not offset contiguity**, which is what this first checked and is
//     not a consumer-visible rule: an offset is channel-wide, so a consumer
//     whose filter is narrower than the channel's is served a subset and
//     sees 1 then 3 with 2 belonging to a topic it never asked for. The
//     link-churn soak can assert contiguity because its consumer covers the
//     whole channel; a generated one usually does not. That mistake was the
//     first thing this test found, and it found it about itself.
//
//   - **A latest channel's value is the last one written.** A subscriber
//     arriving afterwards is served current state, and current means the
//     model's last write to that topic.
//
//   - **The two providers answer alike.** The same scenario is driven on a
//     broker holding its channels in memory and again on one holding them
//     on SQLite, and the answers are compared: the SUBACKs, the append
//     records each consumer received - properties included - and the
//     current value a fresh subscriber is served. The model rules above
//     run on both.
//
// Not asserted: how many records arrive, in what order, or how quickly.
// Those are the broker's to decide and a test that fixes them is a coin
// flip rather than an assertion. And so not compared either: live
// deliveries on latest and broadcast topics, whose count is timing's; the
// queue's records, which go to a worker's pin and not to these consumers
// at all; and the value of the two properties a clock stamps - see
// canonicalRecord.
func TestRandomScenariosHoldTheRules(t *testing.T) {
	seed := randomSeed(t)
	t.Logf("random: seed %d - reproduce with SAGUIN_RANDOM_SEED=%d", seed, seed)

	var counts randCounts
	var (
		roundsRun   int
		redrawn     int // draws discarded: refused, or could reach nobody
		overlapSeen int // rounds where two filters both matched one topic
		queueSeen   int // queue channels drawn, summed over rounds
	)

	for round := 0; round < randomRounds(t); round++ {
		// **A draw this test cannot measure costs a redraw, not a round.**
		// A topology the registry refuses is not a scenario, and neither is
		// one whose published records could reach no consumer (see
		// deliverable). Losing the round to either wastes the run, and when
		// every round is lost the suite fails for a generator outcome rather
		// than a broker fault. That is what happened on 2026-09-08: three
		// queue-heavy draws, nothing delivered, and a message that read as
		// bad luck.
		//
		// Attempt 0 keeps the original offset, so a seed that needed no
		// redraw still reproduces exactly what it always did.
		var chans []*channel.Channel
		var script *randScript
		for attempt := 0; attempt < 16; attempt++ {
			off := seed + int64(round)
			if attempt > 0 {
				off += int64(attempt) * 1000003
			}
			rng := rand.New(rand.NewSource(off))
			chans = randomChannels(rng)
			if _, err := channel.NewRegistry(cloneChans(chans)); err != nil {
				redrawn++
				continue
			}
			s := makeScript(rng, chans)
			if !deliverable(chans, s) {
				redrawn++
				continue
			}
			script = s
			break
		}
		if script == nil {
			t.Fatalf("round %d: sixteen draws and not one that the registry "+
				"accepted and that could reach a consumer (seed %d), so the "+
				"generator is broken rather than unlucky", round, seed)
		}
		roundsRun++

		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			for _, c := range chans {
				t.Logf("  channel %s %-6s %s", c.Name, c.Type, c.Filter)
				if c.Type == channel.Queue {
					queueSeen++
				}
			}

			// Did any two filters actually meet over one published topic?
			// Without that this round proved nothing about the rule it is
			// here to test.
			for _, w := range script.writes {
				matched := 0
				for _, c := range chans {
					if modelMatches(c.Filter, w.topic) {
						matched++
					}
				}
				if matched > 1 {
					overlapSeen++
					break
				}
			}

			// Memory first, sqlite second - the order is arbitrary but the
			// clones are not: startWith writes the storage name onto the
			// channels it is given, so each broker gets its own copies and
			// the model keeps the originals.
			answers := map[string]*scenarioAnswer{}
			for _, prov := range []struct {
				name  string
				start func() *harness
			}{
				{"memory", func() *harness { return startRandom(t, cloneChans(chans)) }},
				{"sqlite", func() *harness {
					return startRandomSQLite(t, cloneChans(chans),
						filepath.Join(t.TempDir(), "random.db"))
				}},
			} {
				ans := driveScenario(t, prov.name, prov.start(), round, script)
				if prov.name == "memory" && !ans.subscribedAnything() {
					t.Skip("every generated filter was refused: no scenario here")
				}
				holdTheRules(t, prov.name, chans, script, ans, &counts)
				answers[prov.name] = ans
			}
			compareAnswers(t, chans, script, answers["memory"], answers["sqlite"],
				&counts, seed)
		})
	}

	// **The counters are asserted, because this shape fails silently.** A
	// generator that never produced an overlap, a publisher whose topics
	// reached nobody, or a comparison fed nothing comparable passes every
	// check above while testing none of them.
	if overlapSeen == 0 {
		t.Errorf("no round produced two filters matching one topic, so the rule "+
			"that decides between them was never exercised (seed %d)", seed)
	}
	// **A vacuity backstop, and only that.** Loss is named by the per-record
	// append rule above, which gives the topic, the channel that holds it and
	// the filter that matched, so a broker dropping records a consumer was
	// owed is caught there rather than here. Live deliveries on latest and
	// broadcast topics are not asserted at all - their count is timing's, as
	// the scope note says - so this cannot be read as loss either.
	//
	// What it catches is a run where nothing arrived anywhere. Before the
	// redraw above that meant a topology which could never have delivered,
	// and the message said "passed by vacuum": true, and it read as bad luck
	// rather than as a scenario outside what this test measures. Those are
	// now discarded at the draw, so reaching here means the model said
	// something was owed and nothing came, which is worth a look whatever
	// the cause.
	if counts.deliveries == 0 {
		t.Errorf("nothing arrived at any consumer across %d deliverable rounds, "+
			"so every check above ran on nothing: read the per-record failures "+
			"above first if there are any (seed %d)", roundsRun, seed)
	}

	if counts.compared == 0 && counts.appends > 0 {
		t.Errorf("%d append records were checked and none was compared across "+
			"the providers, so the agreement above was asserted about nothing "+
			"(seed %d)", counts.appends, seed)
	}
	// **Whether a partitioned consumer was owed anything is reported, not
	// required**, and the difference matters. One consumer in three declares
	// a slice, but a declaration tests something only where an append
	// channel exists, a filter matches a written topic, and the hash puts
	// that topic in the slice taken - three draws, in three rounds. Seed
	// 1788725847035412040 produced 66 deliveries and no append records at
	// all, so nothing could be owed to anybody.
	//
	// Requiring it made this suite fail on an unlucky seed, which is a coin
	// flip rather than an assertion - the thing this file's own header warns
	// against. Partitioning is asserted deterministically elsewhere: eight
	// end-to-end tests, a fuzz target, and the link-churn soak. What this
	// suite adds is generated topologies nobody wrote, and it adds that
	// whether or not one round happened to draw a slice.
	if counts.sliced == 0 {
		t.Logf("no record was owed to a consumer declaring a partition slice in %d "+
			"rounds: the two partitioning rules held vacuously here. Not a failure - "+
			"re-run with more rounds, or SAGUIN_RANDOM_SEED, to exercise them",
			roundsRun)
	}
	t.Logf("random: %d rounds (%d draws discarded), %d deliveries, "+
		"%d overlaps, %d append checks "+
		"(%d owed to a declared slice), %d latest checks, %d queues, %d with "+
		"properties, %d compared across providers",
		roundsRun, redrawn, counts.deliveries, overlapSeen, counts.appends,
		counts.sliced,
		counts.latests, queueSeen, counts.props, counts.compared)
}

// holdTheRules checks one provider's answer against the model - the four
// rules in the comment above TestRandomScenariosHoldTheRules.
func holdTheRules(t *testing.T, prov string, chans []*channel.Channel,
	script *randScript, ans *scenarioAnswer, counts *randCounts) {
	t.Helper()

	for i, filters := range ans.filters {
		t.Logf("  %s consumer %d filters %v", prov, i, filters)
		seen := map[string]bool{} // topic\x00payload actually delivered
		for _, got := range ans.delivered[i] {
			counts.deliveries++
			if got.ContentType != "" {
				counts.props++
			}

			// Nothing arrives that this consumer did not ask for.
			asked := false
			for _, f := range filters {
				if modelMatches(f, got.Topic) {
					asked = true
				}
			}
			if !asked {
				t.Errorf("%s consumer %d received %q, which matches none of its "+
					"filters %v", prov, i, got.Topic, filters)
				continue
			}

			// **And nothing outside the slice it declared.** A filter
			// matching is no longer the whole of "asked for": a consumer
			// that took 1 of 3 asked for a third of what its filters reach,
			// and a record from another third is another member's.
			if !script.slices[i].holds(got.Topic) {
				t.Errorf("%s consumer %d declared slice %d of %d and received %q, "+
					"which the hash in RFC 0003 puts elsewhere: two members hold "+
					"one record", prov, i, script.slices[i].index,
					script.slices[i].count, got.Topic)
				continue
			}

			// A wildcard never reaches a queue.
			holder := modelHolder(chans, got.Topic)
			if holder != nil && holder.Type == channel.Queue {
				t.Errorf("%s consumer %d received %q, which queue %q holds - "+
					"invariant 11", prov, i, got.Topic, holder.Name)
				continue
			}
			seen[got.Topic+"\x00"+got.Payload] = true
		}

		// Nothing the consumer asked for went missing. By topic and
		// payload rather than by offset, because an offset is
		// channel-wide and this consumer is served a subset of the
		// channel.
		for _, w := range script.writes {
			holder := modelHolder(chans, w.topic)
			if holder == nil || holder.Type != channel.Append {
				continue
			}
			asked := false
			for _, f := range filters {
				if modelMatches(f, w.topic) {
					asked = true
				}
			}
			if !asked {
				continue
			}
			// **A record outside the declared slice is owed to nobody
			// here.** Counting it as missing would make every partitioned
			// round fail, and skipping the *whole* rule for a partitioned
			// consumer would remove the loss check exactly where a new
			// predicate could cause loss - so the model narrows what is
			// owed rather than dropping the question.
			if !script.slices[i].holds(w.topic) {
				continue
			}
			counts.appends++
			counts.sliced += boolToInt(script.slices[i].count > 0)
			if !seen[w.topic+"\x00"+w.payload] {
				t.Errorf("%s consumer %d never received %q %q, which channel %q "+
					"holds, its filters %v match, and its declared slice %d of %d "+
					"takes - that is loss, not backpressure",
					prov, i, w.topic, w.payload, holder.Name, filters,
					script.slices[i].index, script.slices[i].count)
			}
		}
	}

	// A latest channel's value is the last one written.
	for _, topic := range script.latestTopics {
		got := ans.latest[topic]
		if got == nil {
			t.Errorf("on %s, a subscriber to %q was served no current value, and "+
				"channel %q holds one", prov, topic, modelHolder(chans, topic).Name)
			continue
		}
		counts.latests++
		if want := script.lastLatest[topic]; got.Payload != want {
			t.Errorf("on %s, current value of %q is %q, want %q - the last write",
				prov, topic, got.Payload, want)
		}
	}
}

// compareAnswers holds the two providers to answering alike. It compares
// only what is deterministic given the script: the SUBACKs, each
// consumer's append records deduplicated by offset - at-least-once may
// deliver a record twice, on either provider, without either being wrong -
// and the current value served for each latest topic.
func compareAnswers(t *testing.T, chans []*channel.Channel, script *randScript,
	mem, sq *scenarioAnswer, counts *randCounts, seed int64) {
	t.Helper()

	for i := range script.consumers {
		if !slices.Equal(mem.filters[i], sq.filters[i]) {
			t.Errorf("consumer %d was granted %v on memory and %v on sqlite - "+
				"the two providers answered the same SUBSCRIBE differently (seed %d)",
				i, mem.filters[i], sq.filters[i], seed)
		}
	}

	for i := range script.consumers {
		m := appendHeld(chans, mem.delivered[i])
		s := appendHeld(chans, sq.delivered[i])
		topics := map[string]bool{}
		for tp := range m {
			topics[tp] = true
		}
		for tp := range s {
			topics[tp] = true
		}
		ordered := make([]string, 0, len(topics))
		for tp := range topics {
			ordered = append(ordered, tp)
		}
		sort.Strings(ordered)
		for _, tp := range ordered {
			a, b := m[tp], s[tp]
			if len(a) != len(b) {
				t.Errorf("consumer %d: %q delivered %d distinct records on memory "+
					"and %d on sqlite (seed %d)", i, tp, len(a), len(b), seed)
			}
			for j := 0; j < len(a) && j < len(b); j++ {
				counts.compared++
				if ca, cb := canonicalRecord(a[j]), canonicalRecord(b[j]); ca != cb {
					t.Errorf("consumer %d: record %d of %q differs between the "+
						"providers (seed %d)\n  memory: %s\n  sqlite: %s",
						i, j, tp, seed, ca, cb)
				}
			}
		}
	}

	for _, tp := range script.latestTopics {
		a, b := mem.latest[tp], sq.latest[tp]
		if (a == nil) != (b == nil) {
			t.Errorf("a subscriber to %q was served a current value on one "+
				"provider and nothing on the other - memory %v, sqlite %v (seed %d)",
				tp, a != nil, b != nil, seed)
			continue
		}
		if a == nil {
			continue
		}
		counts.compared++
		if ca, cb := canonicalRecord(*a), canonicalRecord(*b); ca != cb {
			t.Errorf("the current value of %q differs between the providers "+
				"(seed %d)\n  memory: %s\n  sqlite: %s", tp, seed, ca, cb)
		}
	}
}

// appendHeld is one consumer's deliveries on topics an append channel
// holds, keyed by topic, deduplicated by the delivery's saguin-offset and
// sorted by it - the log's content as this consumer's filters select it,
// which is the deterministic thing two providers must agree on.
func appendHeld(chans []*channel.Channel, recs []received) map[string][]received {
	byOffset := map[string]map[string]received{}
	for _, r := range recs {
		h := modelHolder(chans, r.Topic)
		if h == nil || h.Type != channel.Append {
			continue
		}
		m, ok := byOffset[r.Topic]
		if !ok {
			m = map[string]received{}
			byOffset[r.Topic] = m
		}
		off := r.User["saguin-offset"]
		if _, dup := m[off]; !dup {
			m[off] = r
		}
	}
	out := map[string][]received{}
	for topic, m := range byOffset {
		offs := make([]string, 0, len(m))
		for off := range m {
			offs = append(offs, off)
		}
		sort.Slice(offs, func(i, j int) bool {
			a, aerr := strconv.ParseUint(offs[i], 10, 64)
			b, berr := strconv.ParseUint(offs[j], 10, 64)
			if aerr != nil || berr != nil {
				return offs[i] < offs[j]
			}
			return a < b
		})
		for _, off := range offs {
			out[topic] = append(out[topic], m[off])
		}
	}
	return out
}

// canonicalRecord renders a delivery as the string the comparison is over:
// payload, the stored publish properties, and every user property. Two of
// those are compared by presence rather than value - saguin-id is a fresh
// UUID and saguin-timestamp the broker's clock, so their values differ
// between two runs of the same script with neither provider wrong.
func canonicalRecord(r received) string {
	var b strings.Builder
	fmt.Fprintf(&b, "payload=%q content-type=%q payload-format=%q "+
		"response-topic=%q correlation=%q retain=%v",
		r.Payload, r.ContentType, r.PayloadFormat, r.RespTopic, r.CorrData, r.Retain)
	keys := make([]string, 0, len(r.User))
	for k := range r.User {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := r.User[k]
		if k == "saguin-id" || k == "saguin-timestamp" {
			v = "present"
		}
		fmt.Fprintf(&b, " %s=%q", k, v)
	}
	return b.String()
}

// cloneChans copies a generated set so that a trial registry build cannot
// mutate the channels the harness is about to be given - NewRegistry fills
// in derived fields, and handing it the same pointers twice would leave the
// second build looking at the first one's work.
func cloneChans(in []*channel.Channel) []*channel.Channel {
	out := make([]*channel.Channel, 0, len(in))
	for _, c := range in {
		cp := *c
		out = append(out, &cp)
	}
	return out
}

// crashRounds is how many crash scenarios to run: two in the ordinary
// suite, or SAGUIN_CRASH_ROUNDS when somebody wants a longer hunt.
func crashRounds(t *testing.T) int {
	t.Helper()
	v := os.Getenv("SAGUIN_CRASH_ROUNDS")
	if v == "" {
		return 2
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("SAGUIN_CRASH_ROUNDS=%q is not a positive number of rounds", v)
	}
	t.Logf("crash: %d rounds", n)
	return n
}

// **A crash at a moment nobody chose, held to the promises that survive
// one.**
//
// The suite's other crash tests die at a moment somebody picked, after
// writes somebody listed. This one runs live traffic against a broker on
// sqlite and kills it - no shutdown, no snapshot, no database close, the
// same abandonment a dying process is - at a random moment mid-write.
// Then it restarts on the same file and checks what MUST be true whatever
// the moment was:
//
//   - **Nothing acknowledged is lost.** A PUBACK is the broker's word that
//     the record is durable (invariant 2); a record acked before the crash
//     and absent after it is the worst class of defect, a loss that
//     reported success.
//   - **Nothing is stored twice.** The same payload at two different
//     offsets is a duplicate the store created; at one offset delivered
//     twice it is only at-least-once redelivering, and is not counted.
//   - **Nothing appears that nobody sent**, and the one record a stream
//     had in flight when the lights went out may be present or absent -
//     both are the truth about an unacknowledged publish.
//   - **A latest topic's current value is not older than the last
//     acknowledged write** to it.
//   - **The first offset assigned after the restart is above every stored
//     one** (invariant 9).
//
// The publishers record an acknowledgement only after Publish returns, so
// the race at the crash instant resolves conservatively: a PUBACK the
// client never read is a record the checks allow either way.
//
// What this cannot simulate, honestly: the operating system losing writes
// the process believed synced - the crash is in-process, so file pages
// already handed to the kernel survive. What it does exercise is
// transaction-level durability: whatever the store had not committed when
// it was abandoned must roll back to a state the restart reads whole.
func TestACrashAtARandomMomentLosesNothingAcknowledged(t *testing.T) {
	seed := randomSeed(t)
	t.Logf("crash: seed %d - reproduce with SAGUIN_RANDOM_SEED=%d", seed, seed)

	var (
		roundsRun   int
		ackedTotal  int // publishes acknowledged before their crash
		recovered   int // records read back after the restarts
		interrupted int // streams the crash caught mid-publish
		latestSeen  int // rounds whose latest value was checked
	)

	for round := 0; round < crashRounds(t); round++ {
		rng := rand.New(rand.NewSource(seed + int64(round)))
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "crash.db")
			h := startDurableSQLite(t, path)

			// stream is one publisher's bookkeeping. acked holds what the
			// broker took responsibility for; inflight the one publish the
			// crash may have caught, which the checks allow either way.
			type stream struct {
				mu       sync.Mutex
				acked    []string
				inflight string
			}
			const appendStreams = 3
			streams := make([]*stream, appendStreams)
			state := &stream{} // one more, writing a latest topic
			stop := make(chan struct{})
			var wg sync.WaitGroup

			// **qos is a parameter so half the streams run exactly-once**,
			// and the promise being checked is the same sentence at either
			// level: a publish the broker acknowledged is still there after
			// the crash. What differs is which packet the acknowledgement
			// is - a PUBACK at QoS 1, a PUBCOMP at QoS 2 - and Paho returns
			// from Publish only when that packet has arrived, so the
			// bookkeeping below needs no change.
			//
			// It is worth running here rather than only in the e2e tests
			// because the crash lands at a moment nobody chose: between the
			// receipt and the release is a window that exists only at QoS 2,
			// and a record stored inside it is the defect
			// `broker.qos2` was built to remove.
			publishUntilStopped := func(cl *client, topic string, st *stream, label string, qos byte) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					payload := fmt.Sprintf("%s-%d", label, i)
					st.mu.Lock()
					st.inflight = payload
					st.mu.Unlock()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, err := cl.C.Publish(ctx, &paho.Publish{
						Topic: topic, QoS: qos, Payload: []byte(payload)})
					cancel()
					if err != nil {
						return // the crash reached this publish mid-flight
					}
					st.mu.Lock()
					st.acked = append(st.acked, payload)
					st.mu.Unlock()
				}
			}

			for g := 0; g < appendStreams; g++ {
				streams[g] = &stream{}
				cl := connect(t, h, fmt.Sprintf("cr%d-p%d", round, g), true, false)
				wg.Add(1)
				// Alternating rather than random, so every run covers both
				// and a seed cannot produce a round that is all one level.
				qos := byte(1)
				if g%2 == 1 {
					qos = 2
				}
				go publishUntilStopped(cl, fmt.Sprintf("events/hunt/g%d", g),
					streams[g], fmt.Sprintf("r%dg%d", round, g), qos)
			}
			scl := connect(t, h, fmt.Sprintf("cr%d-state", round), true, false)
			wg.Add(1)
			// The `latest` stream runs exactly-once too: a channel keyed by
			// topic is where a duplicate is hardest to see, because the
			// second copy replaces the first rather than sitting beside it.
			go publishUntilStopped(scl, "state/hunt", state, fmt.Sprintf("r%ds", round), 2)

			// The moment nobody chose.
			time.Sleep(time.Duration(5+rng.Intn(115)) * time.Millisecond)
			h.Crash()
			close(stop)
			wg.Wait()

			for _, st := range append(append([]*stream{}, streams...), state) {
				ackedTotal += len(st.acked)
				if st.inflight != "" && (len(st.acked) == 0 ||
					st.acked[len(st.acked)-1] != st.inflight) {
					interrupted++
				}
			}

			// The restart an operator would do: a new broker on the same file.
			h2 := startDurableSQLite(t, path)
			reader := connect(t, h2, fmt.Sprintf("cr%d-reader", round), true, false)
			reader.Sub(t, "events/#", 1)

			// Read from the client's own record of what arrived, never by
			// draining its channel: the channel holds 256 and drops the
			// rest, so a hunter reading it reports the buffer as a loss.
			// That was this test's first failure, and it was about itself.
			deadline := time.Now().Add(15 * time.Second)
			for last := -1; ; {
				n := reader.Count()
				if n == last || time.Now().After(deadline) {
					break
				}
				last = n
				time.Sleep(300 * time.Millisecond)
			}

			// Payload -> the distinct offsets it was stored at. Distinct,
			// because the same offset twice is at-least-once redelivering to
			// this reader, and only a second offset is a duplicate the store
			// created.
			stored := map[string]map[string]bool{}
			var maxOffset uint64
			for _, r := range reader.All() {
				recovered++
				off := r.User["saguin-offset"]
				if stored[r.Payload] == nil {
					stored[r.Payload] = map[string]bool{}
				}
				stored[r.Payload][off] = true
				if n, err := strconv.ParseUint(off, 10, 64); err == nil && n > maxOffset {
					maxOffset = n
				}
			}

			allowed := map[string]bool{}
			for _, st := range streams {
				for _, p := range st.acked {
					if len(stored[p]) == 0 {
						t.Errorf("acknowledged %q is gone after the crash - a loss "+
							"that reported success (seed %d)", p, seed)
					}
					allowed[p] = true
				}
				if st.inflight != "" {
					allowed[st.inflight] = true
				}
			}
			for p, offs := range stored {
				if len(offs) > 1 {
					t.Errorf("%q is stored at %d offsets - the crash duplicated a "+
						"record (seed %d)", p, len(offs), seed)
				}
				if !allowed[p] {
					t.Errorf("%q came back and nobody's acknowledged or in-flight "+
						"publishes contain it (seed %d)", p, seed)
				}
			}

			// The latest topic's current value must not be older than its
			// last acknowledged write.
			state.mu.Lock()
			lastAcked, inflight := "", state.inflight
			if len(state.acked) > 0 {
				lastAcked = state.acked[len(state.acked)-1]
			}
			state.mu.Unlock()
			if lastAcked != "" {
				late := connect(t, h2, fmt.Sprintf("cr%d-late", round), true, false)
				late.Sub(t, "state/hunt", 1)
				got, ok := late.Await(t, 3*time.Second)
				if !ok {
					t.Errorf("the current value of state/hunt is gone after the "+
						"crash, and %q was acknowledged (seed %d)", lastAcked, seed)
				} else {
					latestSeen++
					if got.Payload != lastAcked && got.Payload != inflight {
						t.Errorf("the current value of state/hunt is %q after the "+
							"crash; the last acknowledged write was %q - the store "+
							"went backwards (seed %d)", got.Payload, lastAcked, seed)
					}
				}
			}

			// Invariant 9: the first offset assigned after the restart sits
			// above every stored one.
			p2 := connect(t, h2, fmt.Sprintf("cr%d-after", round), true, false)
			marker := fmt.Sprintf("after-%d", round)
			p2.Pub(t, "events/hunt/after", marker)
			found := false
			for waited := time.Now(); !found && time.Since(waited) < 5*time.Second; {
				for _, r := range reader.All() {
					if r.Payload != marker {
						continue
					}
					found = true
					n, err := strconv.ParseUint(r.User["saguin-offset"], 10, 64)
					if err != nil || n <= maxOffset {
						t.Errorf("the first offset after the restart is %q with %d "+
							"already stored - an offset reused across a crash "+
							"(invariant 9, seed %d)", r.User["saguin-offset"], maxOffset, seed)
					}
					break
				}
				if !found {
					time.Sleep(100 * time.Millisecond)
				}
			}
			if !found {
				t.Errorf("the record published after the restart never arrived "+
					"(seed %d)", seed)
			}
			roundsRun++
		})
	}

	// The counters are asserted, because a hunter whose crash always landed
	// before the first acknowledgement, or whose reader read nothing, passes
	// every check above while hunting nothing.
	if roundsRun == 0 {
		t.Fatal("no crash round ran at all")
	}
	if ackedTotal == 0 {
		t.Errorf("no publish was acknowledged before any crash, so the loss "+
			"check compared nothing (seed %d)", seed)
	}
	if recovered == 0 {
		t.Errorf("no record was read back after any restart, so every check "+
			"passed by vacuum (seed %d)", seed)
	}
	if interrupted == 0 {
		t.Errorf("no crash caught a publish mid-flight in %d rounds, so the "+
			"moment was never actually random with respect to a write (seed %d)",
			roundsRun, seed)
	}
	t.Logf("crash: %d rounds, %d acked, %d recovered, %d interrupted streams, "+
		"%d latest checks", roundsRun, ackedTotal, recovered, interrupted, latestSeen)
}

// Capacity at a moment nobody chose.
//
// **The deterministic capacity tests each fill the store one way.** They pick
// a bound and who is away, and they prove that case. The closest to this one
// is TestAnOfflineSessionKeepsTheNewestWithinItsBound: an offline session
// keeps the newest. What it does not cross is a session whose client is
// **connected and never acknowledging**, and it runs on one provider at one
// bound with one payload size.
//
// So this draws the combination - bound, payload size, how many are published,
// and whether the subscriber is away or merely deaf - and holds whatever comes
// out to the rules rather than to an expected answer:
//
//   - **The publisher is never refused.** A session's bound and a full session
//     store are the session's problem, never the publisher's (RFC 0002,
//     invariant 16).
//   - **The bound bounds**, whatever the policy and whoever is away
//     (invariant 13).
//   - **What went is counted.** The deliveries a session gave up equal what it
//     was owed less what it holds, under session_queue_full. A message that is
//     neither held nor counted went silently, which is the class this item
//     exists for.
//   - **The oldest goes**: a session keeps the newest it was sent.
//
// **And it proves it drove the fill.** A draw that never reaches the bound
// satisfies all four vacuously, so the run fails if no round crossed it, and
// separately if nothing was ever given up or no surviving end was ever
// compared. Those are three different silences and each has its own counter.
//
// **One of the four has no mutation behind it, and that is worth saying.** A
// mutation exists for the bound, for the counting and for which end goes, and each
// fails this test by name. None exists for "the publisher is never refused":
// the publish path does not consult a session's bound or its store at all, so
// refusing a publisher is not a line to flip but a different broker. The rule
// is asserted here every draw; it is held structurally rather than proven by
// mutation, and reading its presence in the list as the latter would be wrong.
func TestRandomCapacityScenariosHoldTheRules(t *testing.T) {
	seed := randomSeed(t)
	t.Logf("capacity: seed %d - reproduce with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	const series = `saguin_session_deliveries_dropped_total{cause="session_queue_full"}`
	// crossed counts draws that reached the bound; policyChecked counts those
	// where the surviving end was actually identified and compared. They are
	// not the same number, and asserting the first while meaning the second is
	// how this shape passes without checking anything.
	var crossed, policyChecked, gaveSome, deafDraws, drawsRun int

	for round := 1; round <= randomRounds(t); round++ {
		for _, provider := range []string{"memory", "sqlite"} {
			var (
				bound     = int64(8<<10) * int64(1+rng.Intn(3)) // 8, 16 or 24 KiB
				size      = 256 * (1 + rng.Intn(4))             // 256B to 1KiB
				published = 24 + rng.Intn(24)
				away      = rng.Intn(2) == 1
			)
			name := fmt.Sprintf("r%d/%s/%dB/bound%d/away%v", round, provider, size, bound, away)

			t.Run(name, func(t *testing.T) {
				brokertest.SessionQueueBytes = bound
				t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })

				var h *harness
				if provider == "memory" {
					h = startDurable(t, t.TempDir())
				} else {
					h = startDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))
				}
				ops := operationsAt(t, h)
				lg := attachDrain(t, h)

				sub, _ := windowDial(t, h.Addr, "cap", false, 65535)
				sub.Subscribe("cap/room", 1)
				if away {
					sub.Close()
					sessionGone(t, h, "cap")
				} else {
					// Connected, reading its socket, acknowledging nothing: the
					// deliveries stay in flight and fill the bound.
					//
					// **The channel is drained.** deafReader sends every
					// delivery into it, so a full one blocks its goroutine and
					// the subscriber stops reading - which is a different case
					// from this one, and would pass under this name.
					got := make(chan delivered, 64)
					go func() {
						for range got { //nolint:revive // drained on purpose
						}
					}()
					deafReader(sub, got)
				}

				before := scrapeGauges(t, ops)[series]
				p := connect(t, h, "cap-pub", true, false)
				for i := range published {
					pa, err := p.C.Publish(context.Background(), &paho.Publish{
						Topic:   "cap/room",
						QoS:     1,
						Payload: []byte(fmt.Sprintf("%04d-%s", i, strings.Repeat("x", size))),
					})
					if err != nil || pa.ReasonCode != 0 {
						t.Fatalf("publish %d was not accepted (%v, reason %v): a publisher is never "+
							"refused for a session's bound or a full session store", i, err, pa)
					}
				}
				settle(t, p)

				// What the session holds is what it is owed from the broadcast
				// log, on the wire or waiting (RFC 0003 "Broadcast").
				owed, bytes := h.B.OwedBroadcast("cap")
				kept := len(owed)
				if bytes > bound {
					t.Errorf("the session holds %d bytes against a bound of %d: the bound stopped bounding",
						bytes, bound)
				}
				gave := int(scrapeGauges(t, ops)[series] - before)
				if gave > 0 {
					gaveSome++
				}
				if gave != published-kept {
					t.Errorf("%s moved by %d, and the session was owed %d and holds %d: "+
						"%d deliveries are neither held nor counted",
						series, gave, published, kept, published-kept-gave)
				}
				if kept >= published {
					return // this draw never reached the bound; the rules above still held
				}
				crossed++

				// Which end went. The payloads are numbered, so what survived
				// says which end the bound gave up.
				msgs, err := lg.ReadAt(owed...)
				if err != nil {
					t.Fatalf("read what the session holds: %v", err)
				}
				lowest, highest := -1, -1
				for _, m := range msgs {
					n, convErr := strconv.Atoi(strings.SplitN(string(m.Payload), "-", 2)[0])
					if convErr != nil {
						continue
					}
					if lowest < 0 || n < lowest {
						lowest = n
					}
					if n > highest {
						highest = n
					}
				}
				if len(msgs) == 0 || lowest < 0 {
					return // nothing identifiable held; the counted identity above still applied
				}
				policyChecked++
				if highest != published-1 {
					t.Errorf("the session holds %d..%d of %d: it gave up the newest, and a full "+
						"session keeps the newest", lowest, highest, published)
				}
			})
			drawsRun++
			if !away {
				deafDraws++
			}
		}
	}

	if crossed == 0 {
		t.Errorf("no draw reached the session bound in %d draws (seed %d), so every rule above held "+
			"vacuously: raise SAGUIN_RANDOM_ROUNDS, or lower the bounds drawn", drawsRun, seed)
	}
	if gaveSome == 0 {
		t.Errorf("no draw gave up a delivery in %d draws (seed %d), so the counted identity was "+
			"only ever 0 == 0", drawsRun, seed)
	}
	if policyChecked == 0 {
		t.Errorf("no draw compared which end survived in %d draws (seed %d), so a bound giving up "+
			"the newest would have passed", drawsRun, seed)
	}
	t.Logf("capacity: %d draws (%d with a connected subscriber that never acknowledges, %d with one away), "+
		"%d crossed the bound, %d gave something up, %d had their surviving end compared",
		drawsRun, deafDraws, drawsRun-deafDraws, crossed, gaveSome, policyChecked)
}

// A provider crossing its ceiling in the middle of live traffic.
//
// **The scenario above fills a session; this one fills the provider.** They
// are different bounds with different outcomes: `limits.session_queue_bytes`
// is what one session may hold and gives up under session_queue_full, while
// `max_bytes` is what the storage may hold and refuses under storage_full
// (RFC 0002). The deterministic test for the second fills the store to its
// ceiling *before* the publish under test. Here the filling runs alongside
// the traffic, so the ceiling is crossed at a moment nobody chose, and the
// bound, the payload size and how much is published are drawn.
//
// Whatever the draw:
//
//   - **The publisher is never refused**, which is the rule a full session
//     store exists to keep off the publisher (RFC 0002, invariant 16).
//   - **Nothing vanishes unaccounted.** The subscriber is away, so nothing is
//     delivered and the arithmetic is exact: what was published is what the
//     session holds plus what it gave up, under storage_full and
//     session_queue_full between them.
//   - **Nothing is held half-written.** Every message the store hands back
//     carries the whole payload it was published with; a truncated one is a
//     provider that ran out of room in the middle of a write.
//
// **And it proves it drove the fill**: a run where the provider never refused
// anything fails rather than passing on a store that was never full.
func TestRandomStoreFillingMidTrafficHoldsTheRules(t *testing.T) {
	seed := randomSeed(t)
	t.Logf("store-fill: seed %d - reproduce with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	const (
		storageFull = `saguin_session_deliveries_dropped_total{cause="storage_full"}`
		queueFull   = `saguin_session_deliveries_dropped_total{cause="session_queue_full"}`
	)
	var filledRuns, gaveSome, drawsRun int

	for round := 1; round <= randomRounds(t); round++ {
		for _, provider := range []string{"memory", "sqlite"} {
			var (
				room      = int64(48<<10) * int64(1+rng.Intn(3)) // 48, 96 or 144 KiB
				size      = 256 * (1 + rng.Intn(3))              // 256B to 768B
				published = 20 + rng.Intn(20)
			)
			name := fmt.Sprintf("r%d/%s/%dB/room%d", round, provider, size, room)

			t.Run(name, func(t *testing.T) {
				var h *harness
				if provider == "memory" {
					h = startDurable(t, t.TempDir())
					h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(room, 0)})
				} else {
					brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+room, "16KiB")
					h = startDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))
				}
				s := brokertest.HarnessSessions
				lg := attachDrain(t, h)
				ops := operationsAt(t, h)
				body := strings.Repeat("x", size)

				away, _ := windowDial(t, h.Addr, "away", true, 10)
				away.Subscribe("fill/away", 1)
				away.Close()
				sessionGone(t, h, "away")

				beforeStorage := scrapeGauges(t, ops)[storageFull]
				beforeQueue := scrapeGauges(t, ops)[queueFull]

				// **The filler runs alongside the traffic**: sessions saved on
				// the same provider, each a filter the size of a delivery, so
				// the log's next message finds the room taken by another
				// writer and gives up its oldest (RFC 0002's full-store table).
				var (
					wg      sync.WaitGroup
					refused atomic.Bool
					fillHit atomic.Bool
				)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for id := 1; id <= 60000; id++ {
						err := s.Save(store.Session{Client: fmt.Sprintf("filler-%06d", id), ExpiryInterval: 300,
							Subscriptions: []store.SessionSubscription{{Filter: "fill/" + body, QoS: 1}}})
						if errors.Is(err, store.ErrFull) {
							fillHit.Store(true)
							return
						}
						if err != nil {
							return // any other storage failure is the broker's to report, not this loop's
						}
					}
				}()

				pub := connect(t, h, "fill-pub", true, false)
				for i := range published {
					pa, err := pub.C.Publish(context.Background(), &paho.Publish{
						Topic:   "fill/away",
						QoS:     1,
						Payload: []byte(fmt.Sprintf("%04d:%s", i, body)),
					})
					if err != nil || pa.ReasonCode != 0 {
						refused.Store(true)
						t.Errorf("publish %d was not accepted (%v, reason %v): a publisher is never "+
							"refused for a full session store", i, err, pa)
						break
					}
				}
				wg.Wait()
				settle(t, pub)

				if fillHit.Load() {
					filledRuns++
				}
				if refused.Load() {
					return // already reported; the arithmetic below would only echo it
				}

				// What the away session holds is what it is owed from the
				// broadcast log (RFC 0003 "Broadcast").
				owed, _ := h.B.OwedBroadcast("away")
				held, err := lg.ReadAt(owed...)
				if err != nil {
					t.Fatalf("read what the away session holds: %v", err)
				}
				gave := int(scrapeGauges(t, ops)[storageFull]-beforeStorage) +
					int(scrapeGauges(t, ops)[queueFull]-beforeQueue)
				if gave > 0 {
					gaveSome++
				}
				if len(held)+gave != published {
					t.Errorf("%d published, %d held and %d given up: %d deliveries are neither held "+
						"nor counted, so they went silently",
						published, len(held), gave, published-len(held)-gave)
				}

				want := len(fmt.Sprintf("%04d:%s", 0, body))
				for _, m := range held {
					if got := len(m.Payload); got != want {
						t.Errorf("the store holds a %d-byte payload where %d was published: "+
							"a message was kept half-written", got, want)
						break
					}
				}
			})
			drawsRun++
		}
	}

	if filledRuns == 0 {
		t.Errorf("the provider never refused a write in %d draws (seed %d), so every rule above held "+
			"on a store that was never full: lower the room drawn, or raise SAGUIN_RANDOM_ROUNDS",
			drawsRun, seed)
	}
	if gaveSome == 0 {
		t.Errorf("no draw lost a delivery to a full store in %d draws (seed %d), so the arithmetic "+
			"above only ever compared a full set with itself", drawsRun, seed)
	}
	t.Logf("store-fill: %d draws, %d filled the provider, %d gave something up", drawsRun, filledRuns, gaveSome)
}
