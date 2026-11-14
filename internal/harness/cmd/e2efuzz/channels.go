package main

import (
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/harness"
)

// The configured channels the run's broker holds (config.md,
// channels.<name>), all append but one latest (latest.go) and one queue
// (queues.go), with the queue's dead-letter channel, whose filters overlap
// one another and
// $broadcast: design.md, Words: Channel, "Where filters overlap, the topic
// goes to the filter that spells it out most exactly"; "A {a,b} level is
// expanded first, with each alternative treated as a written level";
// Channels, $broadcast: "the reserved append channel for every topic no
// configured channel claims". Two have a small max_bytes, "the channel's
// hard byte limit" (config.md), a publish over it getting "0x97 | over [...]
// a channel's max_bytes" (design.md, Flows, PUBLISH), counting "committed
// charges and reservations for accepted writes" (design.md, Channels). One, cr, has a
// small retention_bytes, which config.md's channels.<name> says "removes the
// oldest beyond it": retention deletes its records while the run goes on
// (checkRetention). "A retain flag on a configured channel topic is ignored
// (MQTT-3.3.1-5)". Their records are delivered from consumer positions
// (appends.go), three channels starting new readers at the floor, two at
// the tail.
//
// Topics on them are drawn now and then (channelTopic), and Wills too
// (willTopic), so every route, $broadcast's included, is taken; no Will
// topic is drawn on the latest channel's levels, nor on the queue's or its
// dead-letter channel's.

// broadcast is $broadcast's name (storage.md, channels).
const broadcast = "$broadcast"

// smallMaxBytes is the small channel's max_bytes, and czMaxBytes cz's: a
// few dozen of the run's publications fill each early in a run, mostly
// within a window of many publishers, and it stays full, retention never
// coming due within a run (harness: default_retention_period 1d).
const (
	smallMaxBytes = 4096
	czMaxBytes    = 8192
)

// crRetentionBytes is cr's retention_bytes: two or three of the run's
// records, each within max_packet_size, so the size pass each committed
// record schedules deletes the oldest every few records (design.md, Words:
// Retention: "Every committed change that affects size schedules a size
// pass for its channel"). Its retention_period is the harness's day, so
// size alone deletes within a run.
const crRetentionBytes = 256

// channelConfig is one configured channel.
type channelConfig struct {
	name, filter   string
	maxBytes       int  // 0 for none
	retentionBytes int  // 0 for the harness's default, which no run reaches
	tail           bool // start: tail, a new reader starting at the next offset; else the floor
	// latest: a latest channel, of one value per topic (design.md,
	// Channels), which takes none of the keys above (config.md,
	// channels.<name>: max_bytes, retention_bytes and start are append's);
	// else append. Its retention_period and tombstone_retention_period are
	// the harness's day and config.md's, so retention never comes due
	// within a run.
	latest bool
	// queue: a queue channel's own settings, nil for any other (queues.go);
	// deadLetter: the queue whose dead-letter channel this is, "" for any
	// other, which the broker derives and the configuration never names
	// (design.md, Channels, queue: "It moves the job to the append channel
	// <channel>__dlq [...] The dead-letter channel's filter is the queue's
	// filter with the same prefix").
	queue      *queueConfig
	deadLetter string
}

// channels are the run's configured channels, and the dead-letter channel
// the broker derives from the queue.
var channels = []channelConfig{
	// The busy channels start new readers at the tail; the quiet ones at
	// the floor, so a new position replays their whole history.
	{name: "ca", filter: "ca/#", tail: true},
	{name: "cab", filter: "ca/+/b"},
	{name: "cz", filter: "{ca,cb}/z/#", maxBytes: czMaxBytes, tail: true},
	{name: "small", filter: "ca/s/#", maxBytes: smallMaxBytes},
	{name: "cr", filter: "ca/r/#", retentionBytes: crRetentionBytes},
	{name: "kv", filter: "kv/#", latest: true},
	{name: queueName, filter: queueName + "/#", retentionBytes: queueRetentionBytes, queue: &theQueue},
	{name: queueName + deadLetterSuffix, filter: deadLetterPrefix + queueName + "/#", deadLetter: queueName},
}

// channelOptions is channels as the harness writes them.
func channelOptions() []harness.Channel {
	cs := make([]harness.Channel, 0, len(channels))
	for _, c := range channels {
		switch {
		case c.deadLetter != "":
			continue // derived by the broker from its queue
		case c.latest:
			cs = append(cs, harness.Channel{Name: c.name, Type: "latest", Filter: c.filter})
			continue
		case c.queue != nil:
			q := c.queue
			cs = append(cs, harness.Channel{Name: c.name, Type: "queue", Filter: c.filter, RetentionPeriod: q.retentionPeriod,
				RetentionBytes: c.retentionBytes, VisibilityTimeout: q.visibility, RetryMode: q.retryMode, RetryDelay: q.retryDelay,
				RetryMaxAttempts: q.maxAttempts})
			continue
		}
		start := "floor"
		if c.tail {
			start = "tail"
		}
		cs = append(cs, harness.Channel{Name: c.name, Type: "append", Filter: c.filter, MaxBytes: c.maxBytes, RetentionBytes: c.retentionBytes, Start: start})
	}
	return cs
}

// isLatest reports whether the channel named is a latest channel.
func isLatest(name string) bool {
	return slices.ContainsFunc(channels, func(c channelConfig) bool { return c.name == name && c.latest })
}

// queueOf is the settings of the queue named, nil where name is no queue.
func queueOf(name string) *queueConfig {
	for _, c := range channels {
		if c.name == name {
			return c.queue
		}
	}
	return nil
}

// isDeadLetter reports whether the channel named is a queue's dead-letter
// channel.
func isDeadLetter(name string) bool {
	return slices.ContainsFunc(channels, func(c channelConfig) bool { return c.name == name && c.deadLetter != "" })
}

// expand is filter with its {a,b} level expanded, each alternative a
// written level (config.md: "a level holding { or } is exactly one {a,b}").
func expand(filter string) []string {
	levels := strings.Split(filter, "/")
	for i, l := range levels {
		if strings.HasPrefix(l, "{") && strings.HasSuffix(l, "}") {
			var out []string
			for alt := range strings.SplitSeq(l[1:len(l)-1], ",") {
				levels[i] = alt
				out = append(out, strings.Join(levels, "/"))
			}
			return out
		}
	}
	return []string{filter}
}

// route is the channel topic lands in: the one whose expanded filter
// matching it spells it out most exactly, else $broadcast.
func route(topic string) string {
	best, bestFilter := broadcast, ""
	for _, c := range channels {
		for _, filter := range expand(c.filter) {
			if !matchesFilter(filter, topic) {
				continue
			}
			if bestFilter != "" {
				if more, _ := harness.MoreExact(filter, bestFilter); !more {
					continue
				}
			}
			best, bestFilter = c.name, filter
		}
	}
	return best
}

// limitOf is the max_bytes of the channel named, 0 for none.
func limitOf(name string) int {
	for _, c := range channels {
		if c.name == name {
			return c.maxBytes
		}
	}
	return 0
}

// channelTopic draws a topic on the channels' levels: ca/s (small), ca/r
// (cr), ca, cb, cc, which no channel claims, kv (the latest channel), q
// (the queue) or __dlq/q (its dead-letter channel, which only the broker
// writes), then up to two levels more, mostly those the filters write out,
// so a latest channel's few topics are replaced often.
func (f *fuzzer) channelTopic() string {
	r := f.rng
	levels := strings.Split([]string{"ca/s", "ca/r", "ca", "cb", "cc", "kv", queueName, deadLetterPrefix + queueName}[r.IntN(8)], "/")
	for range r.IntN(maxTopicLevels - 1) {
		if n := r.IntN(6); n < 4 {
			levels = append(levels, []string{"z", "s", "b", "r"}[n])
		} else {
			levels = append(levels, f.word(1, 3))
		}
	}
	return strings.Join(levels, "/")
}

// limitedTopic draws a topic of a channel with a max_bytes: small's ca/s,
// or cz's ca/z and cb/z, perhaps with a level more.
func (f *fuzzer) limitedTopic() string {
	t := []string{"ca/s", "ca/z", "cb/z"}[f.rng.IntN(3)]
	if f.rng.IntN(2) == 0 {
		t += "/" + f.word(1, 3)
	}
	return t
}

// pubTopic draws a PUBLISH's topic: a third on the channels' levels.
func (f *fuzzer) pubTopic() string {
	if f.rng.IntN(3) == 0 {
		return f.channelTopic()
	}
	return f.topic(1 + f.rng.IntN(maxTopicLevels))
}

// messageBytes is what a message charges its channel: design.md's
// storage.md, "A message's bytes are its topic's, payload's and properties'
// bytes", its properties "in their wire form. This is a Property Length
// followed by the properties [...] NULL for none".
func messageBytes(topic string, payload int, props harness.Properties) int64 {
	n := int64(len(topic) + payload)
	if len(props) > 0 {
		wire, _ := harness.AppendProperties(nil, props) // drawn valid
		n += int64(len(wire))
	}
	return n
}

// limited is what the model knows of a channel with a max_bytes.
type limited struct {
	max int64
	// upper is the bytes of every publication and Will the run has sent
	// to it and not seen refused: no more can be committed or reserved.
	// Bursts' PUBACKs lower it on their own goroutines.
	upper atomic.Int64
}

// lowerBytes is the bytes the database holds committed for the channel
// named, which only grow while no retention comes due: "-1" when there is
// no database to read.
func (f *fuzzer) lowerBytes(name string) (int64, error) {
	if f.dbPath == "" {
		return -1, nil
	}
	if err := f.openDB(); err != nil {
		return 0, err
	}
	var n sql.NullInt64
	err := f.db.QueryRow(`SELECT SUM(length(CAST(r.topic AS BLOB)) + r.payload_size + COALESCE(length(r.properties), 0))
		FROM records r JOIN channels c ON c.id = r.channel WHERE c.name = ?`, name).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("reading the bytes of channel %s: %w", name, err)
	}
	return n.Int64, nil
}

// admission is what the model predicts of a publication of b bytes to a
// channel with a max_bytes, upper already counting it: refused for certain
// where the bytes committed with it pass the limit (design.md, Flows,
// PUBLISH: "0x97 | over [...] a channel's max_bytes"); taken for certain where every publication the run
// sent to it, it included, fits (no earlier one can have reserved more);
// else either.
type admission uint8

const (
	admitted admission = iota
	refusedFull
	eitherWay
)

func (f *fuzzer) admit(name string, b int64) (admission, error) {
	lim := f.limits[name]
	lower, err := f.lowerBytes(name)
	switch {
	case err != nil:
		return eitherWay, err
	case lower >= 0 && lower+b > lim.max:
		return refusedFull, nil
	case lim.upper.Load() <= lim.max:
		return admitted, nil
	}
	return eitherWay, nil
}

// placeRefused gives o, refused with 0x83, the 0x97 its channel's max_bytes
// may give it first: design.md, PUBLISH, "Policy reasons are taken in the
// order of the table above", 0x97 before 0x83. It charges nothing.
func (f *fuzzer) placeRefused(o *out) error {
	name := route(o.p.Topic)
	if len(o.refusals) != 1 || o.refusals[0] != 0x83 || f.limits[name] == nil {
		return nil
	}
	o.limited = true
	b := messageBytes(o.p.Topic, len(o.p.Payload), slices.DeleteFunc(slices.Clone(o.p.Props), func(p harness.Property) bool { return p.ID == harness.TopicAlias }))
	f.limits[name].upper.Add(b) // as if it were taken: "would pass it" counts it
	a, err := f.admit(name, b)
	f.limits[name].upper.Add(-b)
	switch {
	case err != nil:
		return err
	case a == refusedFull:
		o.refusals = []byte{0x97}
	case a == eitherWay:
		o.refusals = []byte{0x83, 0x97}
	}
	return nil
}

// placeAll gives each message of batch the run may have stored its channel
// and, on a channel with a max_bytes, its admission: the whole batch is
// counted first, as its PUBLISHes may reach the core in any order. A
// certain refusal makes the PUBLISH refused with 0x97 (design.md, PUBLISH);
// a possible one, either answer. MQTT 3.1.1 QoS 1 PUBLISHes never reach a
// limited channel (newPublish), so no close is left to chance.
func (f *fuzzer) placeAll(batch []*out) error {
	var charged []*out
	for _, o := range batch {
		m := o.m
		if m == nil {
			if err := f.placeRefused(o); err != nil {
				return err
			}
			continue
		}
		m.channel = route(m.topic)
		if m.channel != broadcast {
			m.retain = false // "A retain flag on a configured channel topic is ignored"
			f.channelRecords++
		}
		if lim := f.limits[m.channel]; lim != nil {
			m.bytes = messageBytes(m.topic, len(m.payload), slices.DeleteFunc(slices.Clone(o.p.Props), func(p harness.Property) bool { return p.ID == harness.TopicAlias }))
			lim.upper.Add(m.bytes)
			charged = append(charged, o)
			o.limited = true
		}
	}
	for _, o := range charged {
		a, err := f.admit(o.m.channel, o.m.bytes)
		if err != nil {
			return err
		}
		switch a {
		case refusedFull:
			f.limits[o.m.channel].upper.Add(-o.m.bytes)
			o.refusals, o.m = []byte{0x97}, nil
			f.forRoom.Add(1)
		case eitherWay:
			o.full = true
		}
	}
	return nil
}

// refusedForRoom records that m, which might find its channel full, was
// refused with 0x97: no record of it may be stored, and its bytes are no
// longer charged. It runs on m's publisher's goroutine.
func (f *fuzzer) refusedForRoom(m *message) {
	m.setFate(full)
	f.limits[m.channel].upper.Add(-m.bytes)
	f.forRoom.Add(1)
}

// openDB opens the broker's database, read-only, once.
func (f *fuzzer) openDB() error {
	if f.db != nil {
		return nil
	}
	db, err := sql.Open("sqlite3", "file:"+f.dbPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("opening %s: %w", f.dbPath, err)
	}
	f.db = db
	return nil
}

// readFrom has the run's record checks read the database at path from the
// next one on: a memory-mode broker's file_path once it has stopped, which
// then holds what it stored (design.md, Store, memory mode: "Every
// snapshot_interval and at a graceful stop, a snapshot copies the RAM copy's
// database file [...] renames it over file_path"; "A clean stop deletes the
// RAM copy once the final snapshot succeeds").
func (f *fuzzer) readFrom(path string) {
	if f.db != nil {
		_ = f.db.Close() // opened read-only: nothing is lost
		f.db = nil
	}
	f.dbPath = path
}

// channelNames reads the channels' names by id (storage.md, channels).
func (f *fuzzer) channelNames() (map[int64]string, error) {
	rows, err := f.db.Query(`SELECT id, name FROM channels`)
	if err != nil {
		return nil, fmt.Errorf("reading the channels of %s: %w", f.dbPath, err)
	}
	defer func() { _ = rows.Close() }() // read whole, or the check has failed
	names := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}
	return names, rows.Err()
}

// checkLimits checks, in one read of the database, that no channel with a
// max_bytes holds more: "A hard byte limit counts committed charges and
// reservations for accepted writes" (design.md, Channels), so what is
// committed alone never passes it; and that the channel's bytes are its
// records' (storage.md, channels: "the bytes of its records").
func (f *fuzzer) checkLimits() []error {
	var errs []error
	for _, c := range channels {
		if c.maxBytes == 0 {
			continue
		}
		var stored int64
		var sum sql.NullInt64
		err := f.db.QueryRow(`SELECT c.bytes, (SELECT SUM(length(CAST(r.topic AS BLOB)) + r.payload_size + COALESCE(length(r.properties), 0))
			FROM records r WHERE r.channel = c.id) FROM channels c WHERE c.name = ?`, c.name).Scan(&stored, &sum)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("reading channel %s's bytes: %w", c.name, err))
		case sum.Int64 > int64(c.maxBytes):
			errs = append(errs, fmt.Errorf("channel %s holds records of %d bytes, over its max_bytes %d (design.md, Channels: a hard byte limit)", c.name, sum.Int64, c.maxBytes))
		case stored != sum.Int64:
			errs = append(errs, fmt.Errorf("channel %s's bytes are %d, its records' %d (storage.md, channels)", c.name, stored, sum.Int64))
		}
	}
	return errs
}

// startsAtTail reports whether the channel named starts a new reader at
// its tail (config.md, channels.<name>.start).
func startsAtTail(name string) bool {
	for _, c := range channels {
		if c.name == name {
			return c.tail
		}
	}
	return false
}

// reachCache holds reach's answers, by filter.
var reachCache = map[string][]string{}

// reach is the channels filter reaches, sorted: design.md, Words: Channel,
// "A subscription's filter reaches the channels its topics land in. Each
// topic belongs to its own channel under that rule. The filter also reaches
// $broadcast where a matching topic lands in no configured channel". Every
// topic of up to max_topic_levels levels topicsOf visits stands for them
// all. It is read on bursts' goroutines only once filled.
func reach(filter string) []string {
	if r, ok := reachCache[filter]; ok {
		return r
	}
	set := map[string]bool{}
	topicsOf(filter, maxTopicLevels, func(topic string) bool {
		set[route(topic)] = true
		return true
	})
	r := slices.Sorted(maps.Keys(set))
	reachCache[filter] = r
	return r
}

// topicsOf calls visit with each topic filter matches of up to levels
// levels, each level drawn from those the channel filters and the run's
// acl_file (rights) write out, filter's own and one other, until visit
// returns false. Routing, matching and the rights tell a level only by
// whether it is one of those written out, so these stand for every topic.
// Below a written level of filter only that level is walked, as no other
// can lead to a topic filter matches.
func topicsOf(filter string, levels int, visit func(topic string) bool) {
	words := []string{"ca", "cb", "cc", "kv", queueName, strings.TrimSuffix(deadLetterPrefix, "/"), "s", "z", "b", "r", "x"}
	written := strings.Split(filter, "/")
	for _, l := range written {
		if l != "+" && l != "#" && !slices.Contains(words, l) {
			words = append(words, l) // the filter's own written levels
		}
	}
	// next is the levels a topic's level depth may take and still match.
	next := func(depth int) []string {
		switch {
		case depth < len(written) && written[depth] != "+" && written[depth] != "#":
			return written[depth : depth+1]
		case depth < len(written) || written[len(written)-1] == "#":
			return words
		}
		return nil
	}
	var walk func(prefix string, depth int) bool
	walk = func(prefix string, depth int) bool {
		if prefix != "" && matchesFilter(filter, prefix) && !visit(prefix) {
			return false
		}
		if depth == levels {
			return true
		}
		for _, w := range next(depth) {
			next := w
			if prefix != "" {
				next = prefix + "/" + w
			}
			if !walk(next, depth+1) {
				return false
			}
		}
		return true
	}
	walk("", 0)
}
