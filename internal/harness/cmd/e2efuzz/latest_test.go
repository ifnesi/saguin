package main

import (
	"cmp"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// The fake broker's latest channels, written from design.md's Channels,
// latest ("It holds one value per topic, replaced in place at a new offset,
// in one write [...] An empty payload deletes the value and leaves a
// tombstone"), Flows, SUBSCRIBE (the subscription table: "latest | the
// current values, as Retain Handling allows, then each change") and
// Delivery ("On latest, they follow offset order of changes and skip
// replaced values"; "A recovered exchange whose latest value a newer one
// replaced is settled as if sent, and not counted"), and storage.md,
// records, not from the runner's model: the fake keeps each topic's
// current record (fake.latest), deletes the one a new value replaces
// (replace), sends a pass the current values but tombstones
// (currentMatching), skips a replaced value its turn reaches (deliverLive)
// and settles a recovered exchange of one (resend).

// The faults of latest channels the fake broker can be given, each
// breaking one rule the runner checks.
const (
	missingPassIdentifier fault = "missing initial-pass identifier"
	// keptHistory leaves a replaced value's record where it was.
	keptHistory fault = "replaced value kept"
	// lostTombstone sends no subscriber a deletion's empty payload.
	lostTombstone fault = "tombstone never sent"
	// noLatestPass sends a latest channel's current values to no initial
	// pass.
	noLatestPass fault = "no current values for a pass"
	// replayedHistory sends a value a newer one replaced before its turn
	// came, to a session coming back included.
	replayedHistory fault = "replaced values sent"
	// resentReplaced resends a recovered exchange whose value a newer one
	// replaced.
	resentReplaced fault = "replaced value resent"
)

// fakeLatest reports whether the channel id is a latest channel.
func fakeLatest(id int64) bool {
	return id > 1 && fakeConfigured[id-2].Type == "latest"
}

// replace deletes the record of topic's current value on the latest
// channel id, which a new one is replacing in the same step (storage.md,
// records: "A new value updates the topic's record in place"), unless
// keptHistory. It runs under the lock.
func (b *fake) replace(id int64, topic string) {
	old := b.latest[topic]
	if old == nil || b.fault == keptHistory {
		return
	}
	b.records = slices.DeleteFunc(b.records, func(r *fakeRecord) bool { return r == old })
	_, charge := b.room(old.topic, old.payload, old.props)
	b.bytes[id] -= charge
	_, _ = b.db.Exec(`DELETE FROM records WHERE channel = ? AND "offset" = ?`, id, old.offset)
	b.seen.latestReplaced++
}

// replaced reports whether r is a latest channel's record a newer value of
// its topic has replaced. It runs under the lock.
func (b *fake) replaced(r *fakeRecord) bool {
	return fakeLatest(r.channel) && b.latest[r.topic] != r
}

// currentMatching is the current values of the latest channels' topics
// filter matches, in offset order: design.md, SUBSCRIBE: "the current
// values"; Channels, latest: "An empty payload deletes the value". With
// noLatestPass, none. It runs under the lock.
func (b *fake) currentMatching(filter string) []*fakeRecord {
	if b.fault == noLatestPass {
		return nil
	}
	var rs []*fakeRecord
	for topic, r := range b.latest {
		if len(r.payload) > 0 && topicMatches(filter, topic) {
			rs = append(rs, r)
		}
	}
	slices.SortFunc(rs, func(x, y *fakeRecord) int { return cmp.Compare(x.offset, y.offset) })
	return rs
}

// TestLatestReplacedRecordIsCaught: storage.md, records: "A latest channel
// holds one record per topic. A new value updates the topic's record in
// place"; design.md, Channels: "It holds one value per topic, replaced in
// place at a new offset, in one write". A broker leaving a replaced value's
// record where it was fails the run's record check, naming the topic's two
// records: kv/a takes two values, then the records are checked.
func TestLatestReplacedRecordIsCaught(t *testing.T) {
	caughtInScene(t, keptHistory, sceneQueue, "holds two records of", replacedScene)
}

// TestLatestTombstoneOwedIsCaught: design.md, Channels, latest: "An empty
// payload deletes the value and leaves a tombstone"; Flows, SUBSCRIBE, the
// subscription table: "latest | the current values, as Retain Handling
// allows, then each change"; Delivery, Order: "On latest, they follow
// offset order of changes and skip replaced values". A broker that sends
// no subscriber a deletion's empty payload fails the run at a checkpoint,
// the deletion its topic's current change, still owed: a subscriber to
// kv/# waits while kv/d is deleted, then a checkpoint.
func TestLatestTombstoneOwedIsCaught(t *testing.T) {
	caughtInScene(t, lostTombstone, sceneQueue, "still owed at checkpoint", tombstoneScene)
}

// TestLatestPassOwedIsCaught: design.md, Flows, SUBSCRIBE, the
// subscription table: "latest | the current values, as Retain Handling
// allows"; The start point: "The captured boundary separates the initial
// pass, which reads retained or current values below it, from subsequent
// live records". A broker whose initial passes send no current value fails
// the run at a checkpoint, a value current before the SUBSCRIBE still
// owed: kv/b takes a value, then a client subscribes to kv/# with Retain
// Handling 0, then a checkpoint.
func TestLatestPassOwedIsCaught(t *testing.T) {
	caughtInScene(t, noLatestPass, sceneQueue, "still owed at checkpoint", passScene)
}

// TestLatestPassReplacedWhileQueuedIsOneCopy: design.md, SUBSCRIBE, One
// copy: "a record matching several of a session's non-shared subscriptions
// is sent once"; "Only subscriptions eligible for the record contribute";
// "The delivery uses the lower of the publication's QoS and the highest
// contributing grant"; The start point: "A SUBSCRIBE repeating an existing
// filter replaces its options". The fake broker passes the runner's checks
// when a SUBSCRIBE replaces a subscription whose pass still has a value
// waiting: kv/a and kv/b take values; a subscriber with Receive Maximum 1,
// acknowledging nothing, subscribes to kv/# at QoS 1, kv/a filling its
// window and kv/b waiting; it subscribes again to kv/# at QoS 0 and kv/+ at
// QoS 1, then acknowledges. kv/b goes once, at QoS 1 through kv/+, and
// never at QoS 0 as the replaced kv/# alone would give it.
func TestLatestPassReplacedWhileQueuedIsOneCopy(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		s, p := sc.f.slots[0], sc.f.slots[1]
		window := func(c *harness.Connect) {
			c.Props = append(c.Props, harness.Property{ID: harness.ReceiveMaximum, Int: 1})
		}
		return do(func() error { return sc.connect(p, "ann", nil) },
			func() error { return sc.publish(p, "kv/a", false) },
			func() error { return sc.publish(p, "kv/b", false) },
			func() error { return sc.connect(s, "ann", window) },
			func() error { return sc.subscribe(s, harness.Subscription{Filter: "kv/#", QoS: 1}) },
			func() error { return sc.read(s) },
			func() error {
				return sc.subscribe(s, harness.Subscription{Filter: "kv/#", QoS: 0},
					harness.Subscription{Filter: "kv/+", QoS: 1})
			},
			func() error { return sc.read(s) },
			func() error { return s.releaseAcks() },
			func() error { return sc.read(s) },
			func() error { return s.releaseAcks() },
			func() error { return sc.read(s) },
			func() error { return s.releaseAcks() },
			func() error { return sc.read(s) },
			sc.checkpoint)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestLatestHistoryOnReturnIsCaught: design.md, Delivery, Order: "On
// latest, they follow offset order of changes and skip replaced values";
// Channels, latest: values are "replaced in place at a new offset",
// "Returning consumers receive surviving tombstones". A broker sending values
// replaced before their turn came fails the run when a session coming back
// receives one replaced while it was away: a subscriber to kv/# holding a
// durable session drops its connection, kv/a takes two values, and it comes
// back keeping its session.
func TestLatestHistoryOnReturnIsCaught(t *testing.T) {
	caughtInScene(t, replayedHistory, sceneQueue, "not the history", latestReturnScene(false))
}

// TestLatestReplacedResendIsCaught: design.md, Delivery: "A recovered
// exchange whose latest value a newer one replaced is settled as if sent,
// and not counted"; Flows, CONNECT, 9: resent messages have DUP set. A
// broker resending such an exchange on the session's return fails the run,
// naming the exchange settled as if sent: a subscriber to kv/# holding a
// durable session leaves kv/a's first value unacknowledged, drops its
// connection, and comes back keeping its session after kv/a takes another.
func TestLatestReplacedResendIsCaught(t *testing.T) {
	caughtInScene(t, resentReplaced, sceneQueue, "settled as if sent", latestReturnScene(true))
}

// TestLatestPassIdentifiersAreCaught: design.md, SUBSCRIBE: "Only
// subscriptions eligible for the record contribute"; "Subscription
// Identifiers appear in complete filter-byte order, once for each
// contributing subscription having one". MQTT 5 3.3.4: "the PUBLISH packet
// will carry multiple identical Subscription Identifiers". RH2 sends no
// initial value [MQTT-3.3.1-11]. The two eligible filters share one ID;
// omitting either copy must fail, and the RH2 filter contributes none.
func TestLatestPassIdentifiersAreCaught(t *testing.T) {
	caughtInScene(t, missingPassIdentifier, sceneQueue, "Subscription Identifier", func(sc *scene) error {
		p, err := sc.client(1, "ann", false, false)
		if err != nil {
			return err
		}
		if err := sc.publish(p, "kv/a", false); err != nil {
			return err
		}
		s, err := sc.client(0, "ann", false, false)
		if err != nil {
			return err
		}
		if err := sc.subscribeID(s, 37,
			harness.Subscription{Filter: "#", QoS: 1, RetainHandling: 2},
			harness.Subscription{Filter: "kv/+", QoS: 1, RetainHandling: 0},
			harness.Subscription{Filter: "kv/#", QoS: 1, RetainHandling: 1}); err != nil {
			return err
		}
		return sc.checkpoint()
	})
}
