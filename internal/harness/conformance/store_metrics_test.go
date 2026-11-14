package conformance

import (
	"fmt"
	"maps"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the series only the store knows, re-read every
// min_scrape_interval; of the channel series the core keeps; and of the
// store's commits by cause (metrics.md, Rules; Channels and retained values;
// Store and memory; design.md, Store, Writes), each read from /metrics as an
// operator reads it.

// keptChannel is an append channel, kept, holding kept/#, which keeps two
// records and a half of testkit.RecordBytes: a third record's commit
// brings the size pass that deletes the first (TestSizeRetentionKeepsTheNewest).
var keptChannel = harness.Channel{Name: "kept", Type: "append", Filter: "kept/#",
	RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2}

// recordsSeries is saguin_channel_records of channel.
func recordsSeries(channel string) string {
	return support.ChannelSeries("saguin_channel_records", channel)
}

// metrics.md, saguin_channel_records: "records held: counted from the store
// at start, then kept by the core from each committed change that adds or
// removes records". design.md, Words: Retention, "Every committed change
// that affects size schedules a size pass for its channel"; R4: a reply
// confirming a change "waits for that change's commit"; Store, Writes: "The
// core applies every member's result before taking another event"; Store, full
// mode: "A restart recovers committed state".
//
// A publisher stores kept/1 and kept/2: after each PUBACK, before any
// refresh (refreshInterval), kept counts what the database holds, 1 then 2.
// kept/3 brings the size pass that deletes kept/1; once the database shows
// it, a PUBACK for a record on $broadcast follows its answer, and kept
// counts 2, $broadcast 1. After a graceful stop, the next start's first
// scrape counts the same, as the database holds them. Dead-letter moves
// and latest replacements are not built (items 0017a and 0015a): their
// channels have no records to count yet.
func TestChannelRecordsFollowEachCommit(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{keptChannel}}
	b := support.OpsLaunch(t, o)
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "records-pub", CleanStart: true})
	for i, topic := range []string{"kept/1", "kept/2"} {
		support.Acknowledged(t, pub, support.Sized(topic, testkit.RecordBytes))
		expectSample(t, b, recordsSeries("kept"), float64(i+1), "after the PUBACK of "+topic)
	}
	support.Acknowledged(t, pub, support.Sized("kept/3", testkit.RecordBytes))
	awaitHeld(t, db, "kept", []string{"kept/2", "kept/3"}, "once kept/3 has made 300 bytes")
	committed(t, pub, "records/barrier")
	expectSample(t, b, recordsSeries("kept"), 2, "after the size pass")
	expectSample(t, b, recordsSeries("$broadcast"), 1, "after the PUBACK of records/barrier")
	stopped(t, b)

	again := support.OpsLaunch(t, o)
	for channel, n := range map[string]int{"kept": 2, "$broadcast": 1} {
		if got := len(held(t, db, channel)); got != n {
			t.Fatalf("at the next start the database holds %d records on %s; want %d", got, channel, n)
		}
		expectSample(t, again, recordsSeries(channel), float64(n), "at the next start")
	}
}

// metrics.md, Channels and retained values: saguin_channel_info{channel,
// type,filter}, "always 1; $broadcast and a dead-letter channel are append,
// with filter # and the derived filter"; saguin_channel_next_offset, "the
// tail"; saguin_channel_floor_offset, "the floor"; Rules: "A channel is a
// configured channel, its <channel>__dlq or $broadcast"; "the labels of the
// _info series" are "as configured". design.md, Words: Retention, "The
// floor is the oldest record retention has not passed. The tail is the next
// offset"; Offset: "The core assigns it when a publication enters the
// channel".
//
// The broker has kept (keptChannel) alone: /metrics holds channel_info for
// kept, append, kept/#, and $broadcast, append, #, each 1, and no other.
// A publisher stores kept/1 and kept/2: after each PUBACK, kept's tail is
// one past the offset the database gives that record, and its floor
// kept/1's offset. kept/3's commit brings the size pass that deletes kept/1;
// once the database shows it, and a PUBACK on $broadcast follows its
// answer, the floor is kept/2's offset and the tail one past kept/3's.
func TestChannelIdentityAndOffsets(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{keptChannel}})
	db := support.OpenDB(t, b.DB)
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	info := []string{
		testkit.SeriesKey("saguin_channel_info", "channel", "kept", "type", "append", "filter", "kept/#"),
		testkit.SeriesKey("saguin_channel_info", "channel", "$broadcast", "type", "append", "filter", "#"),
	}
	for _, s := range e.Samples {
		if key := testkit.SeriesKey(s.Name, flatten(s.Labels)...); s.Name == "saguin_channel_info" && !slices.Contains(info, key) {
			t.Errorf("/metrics holds %s, a channel the broker does not have, or not as configured", key)
		}
	}
	for _, key := range info {
		if v, ok := e.Values[key]; !ok || v != 1 {
			t.Errorf("%s = %v (held %v); want 1", key, v, ok)
		}
	}

	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "offsets-pub", CleanStart: true})
	tail := support.ChannelSeries("saguin_channel_next_offset", "kept")
	for _, topic := range []string{"kept/1", "kept/2"} {
		support.Acknowledged(t, pub, support.Sized(topic, testkit.RecordBytes))
		rs := held(t, db, "kept")
		expectSample(t, b, tail, float64(rs[len(rs)-1].offset+1), "after the PUBACK of "+topic)
		expectSample(t, b, floorSeries("kept"), float64(rs[0].offset), "after the PUBACK of "+topic)
	}
	support.Acknowledged(t, pub, support.Sized("kept/3", testkit.RecordBytes))
	rs := awaitHeld(t, db, "kept", []string{"kept/2", "kept/3"}, "once kept/3 has made 300 bytes")
	committed(t, pub, "offsets/barrier")
	expectSample(t, b, floorSeries("kept"), float64(rs[0].offset), "after the size pass")
	expectSample(t, b, tail, float64(rs[1].offset+1), "after the size pass")
}

// commitSeries are saguin_store_commits_total of each cause, and
// saguin_store_committed_records_total under "records" (metrics.md, Store
// and memory).
var commitSeries = map[string]string{
	"event":       `saguin_store_commits_total{closed_by="event"}`,
	"interval":    `saguin_store_commits_total{closed_by="interval"}`,
	"max_records": `saguin_store_commits_total{closed_by="max_records"}`,
	"barrier":     `saguin_store_commits_total{closed_by="barrier"}`,
	"stop":        `saguin_store_commits_total{closed_by="stop"}`,
	"records":     "saguin_store_committed_records_total{}",
}

// commitCounts reads commitSeries on b's /metrics, by their keys.
func commitCounts(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	got := support.Sampled(t, b, slices.Collect(maps.Values(commitSeries))...)
	counts := make(map[string]float64, len(commitSeries))
	for k, s := range commitSeries {
		counts[k] = got[s]
	}
	return counts
}

// expectCommits reads commitSeries on b's /metrics at once and fails the
// test unless each has moved from base by what want gives it, 0 where want
// gives none; it returns what it read. A commit's counts are added as its
// answer is applied, in the core event that makes its replies ready
// (design.md, Flows, Replies), so the test reads them once it has the
// replies of what it sent. when says what was done.
func expectCommits(t *testing.T, b *harness.Broker, base, want map[string]float64, when string) map[string]float64 {
	t.Helper()
	got := commitCounts(t, b)
	for _, k := range slices.Sorted(maps.Keys(commitSeries)) {
		if got[k]-base[k] != want[k] {
			t.Errorf("%s: %s moved by %v; want %v", when, commitSeries[k], got[k]-base[k], want[k])
		}
	}
	return got
}

// pinged sends c a PINGREQ and waits for its PINGRESP, keeping what comes
// before it for Recv. A reply becomes ready "when the core processes the
// event establishing its result" and ready replies "enter the connection's
// reply queue in that event order" (design.md, Flows, Replies), the reply
// queue holding PINGRESP; and "Each connection's packet order is preserved"
// (Execution, Decisions): once the PINGRESP is in, the core has taken every
// packet c sent before the PINGREQ.
func pinged(t *testing.T, c *harness.Client) {
	t.Helper()
	if err := c.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(func(p harness.Packet) bool { _, ok := p.(*harness.Pingresp); return ok }); err != nil {
		t.Fatalf("waiting for the PINGRESP: %v", err)
	}
}

// design.md, Store, Writes: "The first event making a group due or sealing
// it fixes its commit cause: event for dispatch without a collection delay,
// interval for the collection deadline, or max_records, barrier or stop for
// those boundaries"; "With a configured interval [...] The group is due at
// that deadline, or at the end of the event in which a write that is not a
// publication joins it"; "A batch closes before either of its limits would
// be exceeded. One otherwise valid publication larger than the byte limit
// forms a batch alone, fully charged. Reaching a group's limit seals it at
// once"; "A Store-read barrier or a stop also seals it at once"; Store
// reads: "Every other read used for a broker decision has a place in the
// core's store-request order. Earlier accepted writes are resolved before
// its view is fixed"; Flows, CONNECT, 4: "Unless the id is connected, the
// core reads its session from the store"; Execution, Capacities: "A
// publication batch holds publish_commit_max_records records or 4 MiB of
// message bytes". metrics.md, Store and memory: saguin_store_commits_total
// {closed_by}, "successfully committed transactions, by their fixed commit
// cause"; saguin_store_committed_records_total, "records carried by
// successfully committed transactions". This test takes the place of the store's piece tests of
// grouping (0020c.md, 261019 065527).
//
// The broker collects for publish_commit_interval 1s, at most 3 records,
// with max_packet_size 5 MiB. Each step reads the series once its replies
// are in, and every series it does not name moves by 0:
//   - event: a durable session's SUBSCRIBE, a write of no record, commits
//     at once: event 1, records 0; the session then leaves, before the
//     next step reads the series;
//   - interval: one publication waits out the interval: interval 1,
//     records 1;
//   - max_records: six publications sent together make two groups of 3:
//     max_records 2, records 6;
//   - an oversized publication, of 4 MiB and one byte, sent between two
//     small ones, commits alone: 3 commits in all, records 3;
//   - barrier: a publication the core has taken (pinged), then another
//     client's CONNECT, whose session read seals it: barrier 1, records 1;
//   - stop: a publication the core has taken, then SIGTERM: stop 1,
//     records 1, read before the broker exits, the publisher's connection
//     held open within the write timeout.
//
// The collecting steps rely on the core taking the PINGREQ's reply, the
// CONNECT or the signal well within the interval, 1s, the most config.md
// allows.
func TestCommitCauses(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{PublishCommitInterval: time.Second, PublishCommitMaxRecords: 3,
		MaxPacketSize: 5 << 20, WriteTimeout: 5 * time.Second})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "commits-pub", CleanStart: true})
	durable, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "commits-sub", CleanStart: true,
		Props: support.SessionExpiry(support.Hour)})
	base := commitCounts(t, b)
	support.Suback(t, durable, nil, support.At(1, "commits/sub"), 1)
	expectCommits(t, b, base, map[string]float64{"event": 1}, "a durable session's SUBSCRIBE")
	// Its DISCONNECT changes its session expiry, which "is stored before the
	// connection closes" (design.md, DISCONNECT): no write of it is left
	// for the steps that follow.
	support.Disconnected(t, durable, &harness.Disconnect{Props: support.SessionExpiry(60)})
	base = commitCounts(t, b)

	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "commits/1", Payload: []byte("1")})
	base = expectCommits(t, b, base, map[string]float64{"interval": 1, "records": 1}, "one publication")

	six := make([]*harness.Publish, 6)
	for i := range six {
		six[i] = &harness.Publish{QoS: 1, Topic: fmt.Sprintf("commits/six/%d", i), Payload: []byte("6")}
	}
	support.PublishAll(t, pub, six)
	base = expectCommits(t, b, base, map[string]float64{"max_records": 2, "records": 6}, "six publications sent together")

	before := base
	support.PublishAll(t, pub, []*harness.Publish{
		{QoS: 1, Topic: "commits/small/1", Payload: []byte("s")},
		{QoS: 1, Topic: "commits/large", Payload: make([]byte, 4<<20+1)},
		{QoS: 1, Topic: "commits/small/2", Payload: []byte("s")},
	})
	base = commitCounts(t, b)
	commits := 0.0
	for k := range commitSeries {
		if k != "records" {
			commits += base[k] - before[k]
		}
	}
	if commits != 3 || base["records"]-before["records"] != 3 {
		t.Errorf("an oversized publication between two small ones: %v commits of %v records; want 3 of 3, the large one alone",
			commits, base["records"]-before["records"])
	}

	other, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	p := &harness.Publish{QoS: 1, Topic: "commits/barrier", Payload: []byte("b")}
	support.Sent(t, pub, p)
	pinged(t, pub)
	harness.ConnectedOver(t, other, &harness.Connect{ClientID: "commits-other", CleanStart: true})
	support.Puback(t, pub, p)
	base = expectCommits(t, b, base, map[string]float64{"barrier": 1, "records": 1}, "a publication, then another client's CONNECT")

	p = &harness.Publish{QoS: 1, Topic: "commits/stop", Payload: []byte("s")}
	support.Sent(t, pub, p)
	pinged(t, pub)
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	support.Puback(t, pub, p)
	expectCommits(t, b, base, map[string]float64{"stop": 1, "records": 1}, "a publication, then the stop")
	for _, c := range []*harness.Client{pub, other} {
		_ = c.Close() // the broker has stopped serving them
	}
	if state := support.Exited(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
}

// batchedChannel is the configured append channel batched/#, whose
// publications TestPendingBatchGrowsPastOtherRequests has wait their turn
// behind an outstanding batch of their own channel.
var batchedChannel = harness.Channel{Name: "batched", Type: "append", Filter: "batched/#"}

// pendingPublishers is how many publications the core takes while one
// batch of their channel is at the store
// (TestPendingBatchGrowsPastOtherRequests): design.md, Execution, Capacities, "Accepted, unresolved
// publication batches are limited to 4 per channel", so with that one
// outstanding there is room for three batches more, and every body is
// granted however the core cuts them.
const pendingPublishers = 3

// design.md, Store, Writes: "A batch closes before either of its limits
// would be exceeded"; "Reaching a group's limit seals it at once"; "A
// Store-read barrier or a stop also seals it at once"; "A write awaiting
// its owner's earlier request or another required predecessor stays
// pending. It does not hold back unrelated eligible writes"; "With no
// publish_commit_interval, the core seals eligible pending work at the end
// of the current core event whenever no write transaction is outstanding";
// "A publication batch closes only at its record or byte limit, a
// Store-read barrier or a stop. Other requests queued between its
// publications do not close it"; "A read of channel records is no
// barrier"; Requests: "Each [...]
// channel keeps at most one request of its own in flight, plus bounded
// pending work"; "Applying a predecessor's result schedules its dependent
// request"; Delivery, Positions: an acknowledgement "settles its own
// delivery", "Deleting a durable session's settled exchange row must
// commit with its position beyond that record". metrics.md, Store and
// memory: saguin_store_commits_total{closed_by}, "successfully committed
// transactions, by their fixed commit cause". storage.md,
// session_inflight: "messages in flight or prepared for a durable session,
// by packet identifier".
//
// Publications of one channel that arrive while its batch is at the store
// wait as one pending batch, which only its limits, a barrier or a stop
// close: delivery bookkeeping of other sessions, taken between them, is
// neither. The broker's store's log syncs are slowed (launchStallable), no
// interval set. Three durable sessions, subscribed at QoS 1 to chatter/<i>,
// each hold one delivery unacknowledged. With the stall on, a publisher's
// batched/0 goes to the store alone; while it is there (its PUBACK not
// yet in when a later PINGREQ of the same publisher is answered), three
// other publishers each send batched/<i>, and after each, a subscriber
// acknowledges its delivery, a settlement write of its own session taken
// between the publications. Nothing seals a batch between them, so
// batched/1 to batched/3 wait as one batch, and once the stall is off the
// next transaction carries that batch with the settlements: from before
// the stall to the last PUBACK, saguin_store_commits_total moves by 2,
// both event, and the transactions carry 4 records. The settlements were
// taken, so the store shows them: once the last PUBACK is in, no
// session_inflight row is left for the three sessions, and each one's
// position on $broadcast is past its chatter record. A broker cutting a
// batch at each other request carries batched/1 to batched/3 in a
// transaction each, the channel's one request in flight at a time: 4
// commits or more.
func TestPendingBatchGrowsPastOtherRequests(t *testing.T) {
	s := launchStallable(t, &harness.Options{Channels: []harness.Channel{batchedChannel}})
	first, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub0"), CleanStart: true})
	pubs := make([]*harness.Client, pendingPublishers)
	subs := make([]*harness.Client, pendingPublishers)
	unacked := make([]*harness.Publish, pendingPublishers)
	for i := range pendingPublishers {
		pubs[i], _ = harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, fmt.Sprintf("-pub%d", i+1)), CleanStart: true})
		topic := fmt.Sprintf("chatter/%d", i+1)
		subs[i] = support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, fmt.Sprintf("-sub%d", i+1)), CleanStart: true,
			Props: support.SessionExpiry(support.Hour)}, support.At(1, topic))
		payload := []byte(topic) // a payload of its own, to find its record by (offsetsByPayload)
		support.Acknowledged(t, first, &harness.Publish{QoS: 1, Topic: topic, Payload: payload})
		unacked[i] = support.DeliveredAt(t, subs[i], 1, topic, payload)
	}
	// A publication of the channel stored last, so that no write enqueued
	// before it is left for the counts that follow ("Eligible writes join
	// in their store-request order").
	support.Acknowledged(t, first, &harness.Publish{QoS: 1, Topic: "batched/before", Payload: []byte("b")})
	base := commitCounts(t, s.Broker)

	s.stall(t, true)
	outstanding := &harness.Publish{QoS: 1, Topic: "batched/0", Payload: []byte("0")}
	support.Sent(t, first, outstanding)
	pinged(t, first)
	pending := make([]*harness.Publish, pendingPublishers)
	for i := range pendingPublishers {
		pending[i] = &harness.Publish{QoS: 1, Topic: fmt.Sprintf("batched/%d", i+1), Payload: []byte("p")}
		support.Sent(t, pubs[i], pending[i])
		pinged(t, pubs[i])
		if err := subs[i].Acknowledge(unacked[i]); err != nil {
			t.Fatal(err)
		}
		pinged(t, subs[i])
	}
	pinged(t, first)
	for _, p := range first.Pending() {
		if a, ok := p.(*harness.Ack); ok && a.Kind == harness.PUBACK && a.ID == outstanding.ID {
			t.Fatalf("batched/0's PUBACK came before the core took the last of the other publications: its transaction was not outstanding throughout, the stall (%v) too short to show anything", stallSync)
		}
	}
	s.stall(t, false)

	support.Puback(t, first, outstanding)
	for i, p := range pending {
		support.Puback(t, pubs[i], p)
	}
	expectCommits(t, s.Broker, base, map[string]float64{"event": 2, "records": 1 + pendingPublishers},
		fmt.Sprintf("batched/0 at the store, then %d publications of its channel, a settlement after each", pendingPublishers))

	// The settlements went in the transaction batched/1 to batched/3's
	// PUBACKs followed, or one before it.
	db := support.OpenDB(t, s.DB)
	offsets := offsetsByPayload(t, s.DB)
	for i, d := range unacked {
		client := clientID(t, fmt.Sprintf("-sub%d", i+1))
		if rows := storedInflight(t, db, client); len(rows) != 0 {
			t.Errorf("after every PUBACK, session_inflight holds %+v for %s; want none, its delivery of %q acknowledged", rows, client, d.Topic)
		}
		positions := storedSubscriptions(t, db, client).positions
		if len(positions) != 1 || positions[0].offset <= offsets[d.Topic] {
			t.Errorf("after every PUBACK, positions holds %+v for %s; want one row, past %d, the offset of its acknowledged %q", positions, client, offsets[d.Topic], d.Topic)
		}
	}
}
