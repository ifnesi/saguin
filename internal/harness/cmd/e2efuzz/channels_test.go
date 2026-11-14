package main

import (
	"database/sql"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// The fake broker's channels, read from the runner's configuration
// (options), written from design.md's Words: Channel and Channels, not from
// the runner's model: $broadcast is channel 1, the configured ones 2 on in
// the order configured (storage.md, channels: "The $broadcast channel is
// channel 1").

// fakeConfigured is the runner's configured channels, in the order
// configured, read once.
var fakeConfigured = withDeadLetters(options("", 1).Channels)

// fakeChannelSchema is storage.md's channels, as far as the runner reads it.
const fakeChannelSchema = `CREATE TABLE channels (id INTEGER PRIMARY KEY, name TEXT, bytes INTEGER)`

// fakeChannels writes the channels' rows.
func fakeChannels(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(fakeChannelSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO channels VALUES (1, '$broadcast', 0)`); err != nil {
		t.Fatal(err)
	}
	for i := range fakeConfigured {
		if _, err := db.Exec(`INSERT INTO channels VALUES (?, ?, 0)`, i+2, fakeConfigured[i].Name); err != nil {
			t.Fatal(err)
		}
	}
}

// fakeRoute is the channel topic lands in, its id and max_bytes: of the
// configured filters matching it, {a,b} expanded, the most exact; else
// $broadcast. With firstMatch, the first matching in configuration order.
func (b *fake) fakeRoute(topic string) (id int64, maxBytes int) {
	id, best := 1, ""
	for i := range fakeConfigured {
		c := &fakeConfigured[i]
		filters := []string{c.Filter}
		if open := strings.Index(c.Filter, "{"); open >= 0 {
			end := strings.Index(c.Filter, "}")
			filters = nil
			for alt := range strings.SplitSeq(c.Filter[open+1:end], ",") {
				filters = append(filters, c.Filter[:open]+alt+c.Filter[end+1:])
			}
		}
		for _, f := range filters {
			if !topicMatches(f, topic) {
				continue
			}
			if b.fault == firstMatch && id == 1 {
				return int64(i + 2), c.MaxBytes
			}
			if best != "" {
				if more, _ := harness.MoreExact(f, best); !more {
					continue
				}
			}
			id, best, maxBytes = int64(i+2), f, c.MaxBytes
		}
	}
	return id, maxBytes
}

// room is 0x97 where a publication of topic, payload and props would pass
// its channel's max_bytes, its bytes "its topic's, payload's and properties'
// bytes" (storage.md), with those of held QoS 2 publications (design.md,
// Channels: "A hard byte limit counts committed charges and reservations for
// accepted writes"), else 0, with the bytes it would charge.
func (b *fake) room(topic string, payload []byte, props harness.Properties) (reason byte, bytes int64) {
	id, limit := b.fakeRoute(topic)
	bytes = int64(len(topic) + len(payload))
	props = slices.DeleteFunc(slices.Clone(props), func(q harness.Property) bool { return q.ID == harness.TopicAlias })
	if len(props) > 0 {
		wire, _ := harness.AppendProperties(nil, props) // drawn valid by the runner
		bytes += int64(len(wire))
	}
	if limit > 0 && b.bytes[id]+b.reserved[id]+bytes > int64(limit) {
		return 0x97, bytes
	}
	return 0, bytes
}

// retain deletes the oldest records of the channel id while its bytes pass
// its retention_bytes (config.md, channels.<name>: "removes the oldest
// beyond it"; design.md, Words: Retention: "retention deletes a channel's
// oldest records by age and size"), each with the deliveries queued for it
// and not yet handed over: one handed over still goes (Words: Missed,
// "Deliveries already settled or handed to a writer are excluded"). With
// wrongBoundary it keeps only the newest. It runs under the lock.
func (b *fake) retain(id int64) {
	if id == 1 || fakeConfigured[id-2].RetentionBytes == 0 || fakeConfigured[id-2].Type == "queue" {
		return // a queue's retention moves its unfinished jobs (queue_test.go, sweep)
	}
	limit := int64(fakeConfigured[id-2].RetentionBytes)
	for {
		var on []int // where the channel's records are in b.records
		for i, r := range b.records {
			if r.channel == id {
				on = append(on, i)
			}
		}
		if len(on) == 0 || (b.bytes[id] <= limit && (b.fault != wrongBoundary || len(on) == 1)) {
			return
		}
		r := b.records[on[0]]
		b.records = slices.Delete(b.records, on[0], on[0]+1)
		_, charge := b.room(r.topic, r.payload, r.props)
		b.bytes[id] -= charge
		_, _ = b.db.Exec(`DELETE FROM records WHERE channel = ? AND "offset" = ?`, id, r.offset)
		_, _ = b.db.Exec(`UPDATE channels SET bytes = ? WHERE id = ?`, b.bytes[id], id)
		for _, s := range b.sessions {
			s.queue = slices.DeleteFunc(s.queue, func(d *delivery) bool { return d.rec == r })
		}
		b.seen.retention++
	}
}

// TestRoute: design.md, Words: Channel, "the topic goes to the filter that
// spells it out most exactly. Filters are compared level by level. The first
// level where they differ decides. A written level beats + and #, and + beats
// #. A filter ending there beats one going on with #. A {a,b} level is
// expanded first"; Channels, $broadcast takes "every topic no configured
// channel claims".
func TestRoute(t *testing.T) {
	for topic, want := range map[string]string{
		"ca":       "ca",    // # matches its parent
		"ca/x":     "ca",    //
		"ca/x/b":   "cab",   // + beats #
		"ca/s/b":   "small", // written s beats +
		"ca/s":     "small", // ca/s/# matches its parent, s beats #
		"ca/z/b":   "cz",    // written z beats +
		"cb/z":     "cz",    // {ca,cb} expanded
		"cb/z/q/r": "cz",
		"cb/x":     broadcast,
		"cc/z":     broadcast,
		"ca/b/b":   "cab",
	} {
		if got := route(topic); got != want {
			t.Errorf("route(%q) = %s, want %s", topic, got, want)
		}
	}
}

// fakePartition is a subscription's saguin-filter declarations (design.md,
// SUBSCRIBE, Partitioning).
type fakePartition struct {
	count   uint64
	indices map[uint64]bool
}

// fakePartitionOf reads ps's saguin-filter declarations, each by
// harness.PartitionDeclaration: none, nil; "Repeated declarations use one
// count and their indices form a set"; otherwise bad.
func fakePartitionOf(ps harness.Properties) (p *fakePartition, bad bool) {
	for _, d := range ps.User("saguin-filter") {
		count, index, ok := harness.PartitionDeclaration(d)
		if !ok || (p != nil && p.count != count) {
			return nil, true
		}
		if p == nil {
			p = &fakePartition{count: count, indices: map[uint64]bool{}}
		}
		p.indices[index] = true
	}
	return p, false
}

// allows reports whether p delivers topic: "topic_hash(topic) mod
// partitions == index", topic_hash being FNV-1a-64 then a mix, "a gives
// 198367012849983736".
func (p *fakePartition) allows(topic string) bool {
	if p == nil {
		return true
	}
	return p.indices[harness.TopicHash(topic)%p.count]
}

// fakeReach is the ids of the channels filter reaches, $broadcast 1 among
// them (design.md, Words: Channel): those of the topics it matches
// (fakeTopics).
// The answer is kept, by filter (fake.reached). It runs under the lock.
func (b *fake) fakeReach(filter string) []int64 {
	if r, ok := b.reached[filter]; ok {
		return r
	}
	set := map[int64]bool{}
	fakeTopics(filter, maxTopicLevels, func(topic string) bool {
		id, _ := b.fakeRoute(topic)
		set[id] = true
		return true
	})
	r := slices.Sorted(maps.Keys(set))
	b.reached[filter] = r
	return r
}

// fakeTopics calls visit with each topic of up to levels levels filter
// matches, until visit returns false: each standing for every topic its
// levels, those the configured filters or filter write out or another,
// route and match alike.
func fakeTopics(filter string, levels int, visit func(topic string) bool) {
	words := []string{"o"}
	written := strings.Split(filter, "/")
	for _, l := range written {
		if l != "+" && l != "#" && !slices.Contains(words, l) {
			words = append(words, l)
		}
	}
	for i := range fakeConfigured {
		for l := range strings.SplitSeq(strings.NewReplacer("{", "", "}", "", ",", "/").Replace(fakeConfigured[i].Filter), "/") {
			if l != "+" && l != "#" && !slices.Contains(words, l) {
				words = append(words, l)
			}
		}
	}
	topics := []string{""}
	for depth := range levels {
		// Below a written level of filter, only that level can match.
		at := words
		switch {
		case depth < len(written) && written[depth] != "+" && written[depth] != "#":
			at = written[depth : depth+1]
		case depth >= len(written) && written[len(written)-1] != "#":
			return
		}
		next := make([]string, 0, len(at)*len(topics))
		for _, t := range topics {
			for _, l := range at {
				topic := strings.TrimPrefix(t+"/"+l, "/")
				if topicMatches(filter, topic) && !visit(topic) {
					return
				}
				next = append(next, topic)
			}
		}
		topics = next
	}
}

// fakeMay reports whether the runner's acl_file (writeACL) lets user write
// topic (write) or read it: design.md, Access and reload, "A topic: names a
// $broadcast filter"; a channel: rule covers its channel's topics, "filter:
// to narrow its reach within the channel"; Words: Channel, each topic in
// the one channel it lands in. With ignoredACL, everything is allowed.
func (b *fake) fakeMay(user, topic string, write bool) bool {
	if b.fault == ignoredACL {
		return true
	}
	id, _ := b.fakeRoute(topic)
	for _, g := range grantsOf(user) {
		switch {
		case (write && !g.write) || (!write && !g.read):
		case g.channel == "" && id == 1 && topicMatches(g.topic, topic):
			return true
		case g.channel != "" && id > 1 && fakeConfigured[id-2].Name == g.channel && (g.filter == "" || topicMatches(g.filter, topic)):
			return true
		}
	}
	return false
}

// fakeReads reports whether user may read some topic filter matches:
// design.md, Access and reload, "The broker refuses a filter with 0x87 only
// when the user may read nothing the filter can match", over the topics of up to one
// level more than max_topic_levels, a filter's longest.
// The answer is kept, by user and filter (fake.reads). It runs under the
// lock.
func (b *fake) fakeReads(user, filter string) bool {
	key := [2]string{user, filter}
	if r, ok := b.reads[key]; ok {
		return r
	}
	found := false
	fakeTopics(filter, maxTopicLevels+1, func(topic string) bool {
		found = b.fakeMay(user, topic, false)
		return !found
	})
	b.reads[key] = found
	return found
}

// position gives s a position on each append channel a subscription of it
// reaches where it has none: at the floor, its whole history queued in
// offset order, or at the tail (design.md, SUBSCRIBE: "append [...] the
// floor, or the tail where the channel sets start: tail"). A position it
// kept with no subscription reads on from where it was kept, every record
// from there on that a subscription matches queued again in offset order,
// those still awaiting their acknowledgement included (UNSUBSCRIBE: "The
// next subscription reaching the channel reads from the kept position, so
// a record at or beyond it may be delivered again"). It runs under the
// lock.
func (b *fake) position(s *fakeSession) {
	for _, f := range slices.Sorted(maps.Keys(s.subs)) {
		for _, id := range s.subs[f].reach {
			if from, kept := s.kept[id]; kept {
				delete(s.kept, id)
				b.seen.kept++
				for _, r := range b.records {
					if r.channel != id || r.offset < from || !matchesAny(s, r.topic) {
						continue
					}
					again := slices.ContainsFunc(s.inflight, func(d *delivery) bool { return d.rec == r && d.attempt == nil })
					if again {
						b.seen.keptAgain++
					}
					s.queue = append(s.queue, &delivery{rec: r, again: again})
				}
				continue
			}
			if id == 1 || s.positions[id] {
				continue
			}
			s.positions[id] = true
			if fakeLatest(id) {
				continue // read from each subscription's live-start boundary (SUBSCRIBE, The start point)
			}
			b.seen.positions++
			if fakeConfigured[id-2].Start == "tail" {
				continue
			}
			for _, r := range b.records {
				if r.channel == id && matchesAny(s, r.topic) {
					s.queue = append(s.queue, &delivery{rec: r})
				}
			}
		}
	}
}

// unposition ends s's positions no subscription reaches any more, with the
// deliveries they had not yet sent, but, for a durable session, keeps its
// position on an append channel or a queue at its first record unsettled,
// sent and awaiting its acknowledgement or not yet sent, a worker's offers
// aside, or past every record where none is (design.md, UNSUBSCRIBE: "When the last subscription
// reaching an append channel or a queue is removed, a durable session keeps
// its position there. If a delivery was prepared but never handed to the
// writer, the core first sets the position back to the earliest such
// delivery's offset"; "A volatile session's positions, and positions on
// latest and $broadcast, still end with the last subscription reaching the
// channel"; "A delivery prepared but not yet handed to the writer is
// cancelled and never sent when no remaining subscription is owed it"). Its
// deliveries sent finish as before. It runs under the lock.
func (b *fake) unposition(s *fakeSession) {
	for id := range s.positions {
		if slices.ContainsFunc(slices.Collect(maps.Values(s.subs)), func(sub *fakeSub) bool { return slices.Contains(sub.reach, id) }) {
			continue
		}
		if _, kept := s.kept[id]; !kept && s.durable && !fakeLatest(id) && b.fault != endedKept {
			from := b.offset + 1
			for _, d := range slices.Concat(s.inflight, s.queue) {
				if d.rec.channel == id && d.share == "" && d.pass == nil && d.attempt == nil {
					from = min(from, d.rec.offset)
				}
			}
			s.kept[id] = from
		}
		if _, kept := s.kept[id]; !kept {
			delete(s.positions, id)
		}
		s.queue = slices.DeleteFunc(s.queue, func(d *delivery) bool { return d.rec.channel == id })
	}
}

// saguin is the Sagüin properties d carries, a delivery from a configured
// channel to MQTT 5 (design.md, Sagüin properties): saguin-id,
// saguin-offset, saguin-timestamp, saguin-expires where the publisher set
// an expiry, saguin-channel where a filter it went through reaches more
// than one channel.
func (d *delivery) saguin() harness.Properties {
	r := d.rec
	ps := harness.Properties{harness.User("saguin-id", r.id), harness.User("saguin-offset", strconv.FormatInt(r.offset, 10)),
		harness.User("saguin-timestamp", strconv.FormatInt(r.ms, 10))}
	if mei, ok := r.props.Get(harness.MessageExpiryInterval); ok {
		ps = append(ps, harness.User("saguin-expires", strconv.FormatInt(r.ms+int64(mei.Int)*1000, 10)))
	}
	if d.multi {
		ps = append(ps, harness.User("saguin-channel", fakeConfigured[r.channel-2].Name))
	}
	return ps
}
