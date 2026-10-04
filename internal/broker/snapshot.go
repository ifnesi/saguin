package broker

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// SetQuotas attaches the bound each memory provider is held to across
// every channel it owns, by provider name.
//
// Only a memory provider needs one. A sqlite provider bounds its own file
// with max_page_count and refuses the write itself, which costs nothing
// per publish and is why the two are not one mechanism.
func (b *Broker) SetQuotas(q map[string]*store.Quota) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.quotas = q
	b.applyBounds()
}

// ProviderMeasure is a storage provider that can say what it holds and what
// bounds it: *store.Quota for a memory provider, *sqlite.DB for one that
// keeps its own file.
//
// The two answer the same questions by different means - one counts what
// saguin published, the other asks SQLite for the size of the file - which
// is exactly why the collector asks rather than computing. A provider
// measured by anything other than the thing that refuses its writes would
// be graphed against a bound that does not govern it.
type ProviderMeasure interface {
	// Bytes is what this provider is holding now.
	Bytes() int64
	// MaxBytes is the bound it is held to, or zero for none.
	MaxBytes() int64
}

// ReaderDropper is a storage provider that can forget one reader's stored
// positions across every channel it holds, in one operation.
//
// **Why the provider and not the channel.** A Clean Start discards whatever
// the client had stored, and a channel can only answer for itself - so
// asking every channel is a transaction per channel on a provider that
// keeps its own records, however few positions the client actually had. At
// ten thousand channels that is the cost of connecting, paid by every
// device on its first connect, when the answer is almost always "it had
// none". A provider whose positions live in one table answers the same
// question with one statement.
//
// It is separate from ProviderMeasure for the reason CommitCounter is: a
// memory provider has no such table and no provider-level object to ask,
// and folding this in would make it answer a question about a mechanism it
// does not have. Not implementing it is how a provider says "ask my
// channels one at a time", which for memory costs a map delete each.
type ReaderDropper interface {
	DropReader(reader string) (int, error)
}

// CommitCounter is a storage provider that collects publishes into shared
// transactions and can say how that went. A provider that commits one
// publish at a time does not implement it and gets no such series.
//
// It is separate from ProviderMeasure rather than added to it because a
// memory provider has no transaction to collect into: folding this in would
// make every memory provider answer a question about a mechanism it does
// not have, with a zero that reads like a measurement.
type CommitCounter interface {
	CommitStats() store.CommitStats
}

// SetProviderMeasures attaches what answers saguin_provider_bytes and
// saguin_provider_max_bytes, by provider name.
//
// Called after the databases are open, because a sqlite provider cannot say
// how large its file is until there is one.
func (b *Broker) SetProviderMeasures(m map[string]ProviderMeasure) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.measures = m
}

// SetSnapshotDirs attaches the directory each storage provider writes to.
// A provider with nothing here keeps nothing across a restart.
func (b *Broker) SetSnapshotDirs(dirs map[string]*store.Dir) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dirs = dirs
}

// Stores are the stores holding one or more channels, by channel name -
// and, by provider name, the providers themselves where they can answer a
// question no single channel can.
type Stores struct {
	Logs   map[string]LogStore
	Latest map[string]LatestStore
	Queues map[string]QueueStore

	// Providers is the provider behind those stores, for the operations
	// that are the provider's rather than a channel's - forgetting one
	// reader everywhere, today.
	//
	// **Attached with the stores rather than beside the metrics.** The
	// first shape of this read the capability off the map that answers
	// saguin_provider_bytes, because a sqlite provider is already in there:
	// so a caller that attached stores and no metrics got the per-channel
	// walk instead, silently and with nothing to say so. A store capability
	// belongs where stores are attached, or "is this on" depends on whether
	// something unrelated was configured.
	Providers map[string]ReaderDropper
}

// SetStores attaches the store for every channel a provider keeps itself,
// replacing the memory one New built.
//
// New gives every channel a memory store, because a configuration with no
// storage block is a real one and that is exactly what it means. A
// provider that holds its own records says so here, before any listener
// opens.
func (b *Broker) SetStores(s Stores) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Wrapped as they are attached, so that every call the broker makes to a
	// store is counted against its provider whoever makes it. See
	// storageerrors.go for why this is not wired at the sites that log a
	// failure instead.
	for name, lg := range s.Logs {
		b.attachLog(name, countingLog{LogStore: lg, on: b.countStorage(name)})
		b.durable = true
	}
	for name, lt := range s.Latest {
		b.latest[name] = countingLatest{LatestStore: lt, on: b.countStorage(name)}
		b.durable = true
	}
	for name, q := range s.Queues {
		b.queues[name] = countingQueue{QueueStore: q, on: b.countStorage(name)}
		b.durable = true
	}
	b.waiters = nil
	for provider, d := range s.Providers {
		if w, ok := d.(clientWaiter); ok {
			b.waiters = append(b.waiters, w)
		}
		if b.providers == nil {
			b.providers = map[string]ReaderDropper{}
		}
		// Wrapped as it is attached, the way every store above is, so a
		// failure here reaches saguin_storage_errors_total whoever calls it.
		b.providers[provider] = countingDropper{ReaderDropper: d, on: b.countStorageOn(provider)}
	}
	b.wireClientWaiting()
	// The stores just attached replaced the ones New built, and a bound set
	// on those went with them.
	b.applyBounds()
}

// Durable reports whether any channel survives a restart. A broker where
// nothing does must say so at startup rather than let an operator find
// out at the wrong moment (invariant 14).
//
// Two different things make it true, and they promise different amounts. A
// snapshot directory means a memory channel survives a graceful shutdown
// and nothing else. A store that keeps its own records means the channel
// survives a crash. Neither is nothing, which is all this answers.
func (b *Broker) Durable() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.durable || len(b.dirs) > 0
}

// snapshotChannels returns the channels that name a snapshot file, by
// provider, in a fixed order.
//
// A dead-letter channel is not among them: it rides in its queue's file,
// so that a record leaving the queue and arriving in the DLQ cannot be
// recorded by half (invariant 5).
func (b *Broker) snapshotChannels() map[string][]*channel.Channel {
	out := map[string][]*channel.Channel{}
	for name, c := range b.reg.All() {
		if b.dirs[c.Storage] == nil {
			continue
		}
		// A dead-letter channel is refused as a configured name, so the
		// suffix identifies exactly the derived companions.
		if strings.HasSuffix(name, channel.DLQSuffix) {
			continue
		}
		out[c.Storage] = append(out[c.Storage], c)
	}
	for _, cs := range out {
		sort.Slice(cs, func(i, j int) bool { return cs[i].Name < cs[j].Name })
	}
	return out
}

// SaveSnapshots writes every durable channel to its provider's directory.
//
// It runs after the listeners are closed and every client is gone, the
// Will timers stopped and the drains waited for, so nothing is publishing
// while the state is read. Shutdown is its only caller, and
// store.Sessions.Snapshot, which copies the sessions and the broadcast log
// in two steps, is whole only because of that. Memory durability is
// exactly this: what is written here, and nothing else (invariant 14).
func (b *Broker) SaveSnapshots() error {
	// Positions are written on the broker's tick, so the last one to move
	// may not be stored yet. It is stored now, before the state is read: a
	// snapshot with a position a tick behind is a consumer that resumes a
	// tick behind, which nothing in the file itself would ever reveal.
	b.flushPositions()

	b.mu.Lock()
	byProvider := b.snapshotChannels()
	work := map[string][]*store.Snapshot{}
	for provider, chans := range byProvider {
		for _, c := range chans {
			if s, ok := b.snapshot(c); ok {
				work[provider] = append(work[provider], s)
			}
		}
	}
	// The retained store is not a channel and the loop above cannot see
	// it, but its durability is the same promise: what is in this file,
	// and nothing else. One file for the whole store rather than one per
	// topic - ten thousand retained topics must not mean ten thousand
	// files and ten thousand fsyncs at shutdown.
	if s, ok := b.retainedSnapshot(); ok && b.dirs[b.retainedProvider] != nil {
		work[b.retainedProvider] = append(work[b.retainedProvider], s)
	}
	dirs := b.dirs
	// The session store is the third thing here that is not a channel, and
	// it writes a file of its own rather than joining the snapshots above:
	// what it holds is sessions and their in-flight tables, which is
	// not a channel's shape. A sqlite session store exports nothing and is
	// already on its own file.
	sessions, sessionsProvider := b.sessions, b.sessionsProvider
	sessionsDir := b.dirs[sessionsProvider]
	b.mu.Unlock()

	var failures []error
	for provider, snaps := range work {
		if err := dirs[provider].Save(snaps); err != nil {
			failures = append(failures, fmt.Errorf("storage %q: %w", provider, err))
		}
	}
	if ex, ok := sessions.(exportableSessions); ok && sessionsDir != nil {
		if err := sessionsDir.SaveSessions(ex.Snapshot()); err != nil {
			failures = append(failures, fmt.Errorf("storage %q sessions: %w", sessionsProvider, err))
		}
	}
	if len(failures) == 1 {
		return failures[0]
	}
	if len(failures) > 1 {
		return fmt.Errorf("%d storage providers could not be written: %v", len(failures), failures)
	}
	return nil
}

// A snapshot is how a store that keeps its records in memory survives a
// shutdown, so only such a store has anything to hand over. A store that
// is already durable implements none of these, and a channel it holds is
// not snapshotted at all - there is nothing to write and nothing that
// could be lost by not writing it.
type exportableLog interface {
	Export() (next, floor uint64, records []store.Record)
	Positions() []store.Position
}

type exportableLatest interface {
	Export() (next uint64, records []store.Record)
}

type exportableQueue interface {
	Export() (next uint64, items []store.Item)
}

// exportableSessions is a session store that keeps its sessions in memory,
// so a file is the whole of what they survive. Its snapshot carries the
// broadcast log the sessions' cursors point into, because the two are written
// and read as one pair. Loaded again by store.RestoreSessions, where the
// broker starts.
type exportableSessions interface {
	Snapshot() *store.SessionsSnapshot
}

// snapshot copies one channel's state, and its dead-letter channel's with
// it when it is a queue. It reports false when the channel's store keeps
// nothing to export. Callers hold b.mu.
func (b *Broker) snapshot(c *channel.Channel) (*store.Snapshot, bool) {
	cs, ok := b.channelState(c)
	if !ok {
		return nil, false
	}
	s := &store.Snapshot{Channels: []store.ChannelState{cs}}
	if c.Type == channel.Queue && c.DLQ != nil {
		// A queue and its dead-letter channel share a provider, so either
		// both export or neither does.
		dlq, ok := b.channelState(c.DLQ)
		if !ok {
			return nil, false
		}
		s.Channels = append(s.Channels, dlq)
	}
	return s, true
}

func (b *Broker) channelState(c *channel.Channel) (store.ChannelState, bool) {
	cs := store.ChannelState{Name: c.Name, Floor: 1}

	var held HoldStore
	switch c.Type {
	case channel.Append:
		lg, ok := storeBehind(b.logs[c.Name]).(exportableLog)
		if !ok {
			return cs, false
		}
		cs.Kind = store.KindAppend
		cs.Next, cs.Floor, cs.Records = lg.Export()
		cs.Positions = lg.Positions()
		held = b.logs[c.Name]
	case channel.Latest:
		lt, ok := storeBehind(b.latest[c.Name]).(exportableLatest)
		if !ok {
			return cs, false
		}
		cs.Kind = store.KindLatest
		cs.Next, cs.Records = lt.Export()
		held = b.latest[c.Name]
	case channel.Queue:
		q, ok := storeBehind(b.queues[c.Name]).(exportableQueue)
		if !ok {
			return cs, false
		}
		cs.Kind = store.KindQueue
		cs.Next, cs.Items = q.Export()
		held = b.queues[c.Name]
	}
	// The exactly-once publishes it holds for their release, which the
	// publisher was told at its PUBREC the broker had taken. A memory store
	// answers from memory, and never fails to.
	cs.Holds, _ = held.Holds()
	// Only an append channel has positions. A latest channel has no
	// history to resume from, and a queue tracks its work by the state of
	// each record rather than by where anybody has got to.
	return cs, true
}

// retainedSnapshot copies the retained store's state, ready to be written
// to a file of its own. It reports false when there is no store, or when
// the store keeps its own records and has nothing to hand over.
//
// The name it is written under holds a `/`, which a channel name may not,
// so the file it reaches can never be the file of a channel however a
// configuration is written. Callers hold b.mu.
func (b *Broker) retainedSnapshot() (*store.Snapshot, bool) {
	if b.retained == nil {
		return nil, false
	}
	lt, ok := storeBehind(b.retained).(exportableLatest)
	if !ok {
		return nil, false
	}
	next, records := lt.Export()
	return &store.Snapshot{Channels: []store.ChannelState{{
		Name:    RetainedStoreName,
		Kind:    store.KindLatest,
		Next:    next,
		Floor:   1,
		Records: records,
	}}}, true
}

// SettleRetained empties the retained store of every topic a channel now
// claims, so that the two can never both hold one (RFC 0002 "Retained
// messages on a broadcast topic").
//
// It runs at startup, after the stores are attached and the snapshots are
// loaded, and before any listener opens - configuration changes only at
// startup, so that is the one moment the question can be asked and the one
// moment nothing is publishing while it is answered.
//
// A stored topic reaches a channel exactly when an operator names a
// channel for a prefix their fleet was already publishing to, which is the
// migration RFC 0003 recommends. What happens to the value depends on what
// the channel is, and the difference is not a detail:
//
//   - **`latest`**: moved. The two are the same thing under different
//     names - one current value per topic - so nothing is lost and
//     nothing changes meaning.
//   - **anything else**: discarded, and said out loud with a count. In an
//     append channel a current value would become an event that never
//     happened, carrying a timestamp older than the records already in the
//     log, which is the ordering every consumer and every seek by time
//     depends on. In a queue it would become work nobody sent. A
//     dead-letter channel takes records from its queue and from nothing
//     else, and this is nothing else.
//
// A move the destination will not take is a startup error naming both,
// rather than a broker that starts having quietly dropped state an
// operator asked it to keep.
func (b *Broker) SettleRetained() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retained == nil {
		return nil
	}

	claimed, err := b.retained.Match(func(topic string) bool {
		return b.reg.Resolve(topic) != nil
	})
	if err != nil {
		return fmt.Errorf("reading the retained store: %w", err)
	}
	if len(claimed) == 0 {
		return nil
	}

	moved := map[string]int{}
	dropped := map[string]int{}
	now := time.Now()
	for _, r := range claimed {
		c := b.reg.Resolve(r.Topic)
		if c == nil {
			continue // resolved a moment ago; nothing changes it here
		}

		// A value whose publisher's Message Expiry has run out stopped
		// being state at the expiry (MQTT-3.3.2-5); it is still here only
		// because the sweep had not ticked before the last shutdown.
		// Moving it into a `latest` channel would resurrect it - a channel
		// deletes on the operator's clock alone - so it is deleted now,
		// silently, as the sweep would have deleted it.
		if r.Expired(now) {
			if _, err := b.retained.Delete(r.Topic); err != nil {
				return fmt.Errorf("cannot clear an expired retained message that channel %q "+
					"now claims: %w", c.Name, err)
			}
			continue
		}

		// **The move is where a value becomes a record, so it is where the
		// record rule applies.** The retained store keeps what a publisher
		// sent verbatim, reserved names included, because a broadcast
		// message reaches its subscribers as the packet it arrived as and
		// the substrate's own fan-out carries those names too - a stored
		// copy that differed from the live one would be the defect, not the
		// fix. A record is held to a different rule: the reserved prefix is
		// the broker's, and a publisher may not hand a consumer metadata it
		// will read as the broker's word (RFC 0003).
		//
		// So on the way in: lift a publisher's `saguin-id` into the Message
		// ID, exactly as the publish path does, drop every other reserved
		// name, and mint an identity only where there is none. Without this
		// a retained value carrying `saguin-offset=999` and
		// `saguin-dlq-reason=…` arrived in the channel with those beside the
		// broker's own - one record with two offsets, two timestamps, two
		// ids, and a dead-letter reason on a value that was never
		// dead-lettered. A consumer reading properties into a map keeps
		// whichever came last, which is the forged one.
		kept := r.Headers[:0]
		for _, hd := range r.Headers {
			switch {
			case hd.Key == "saguin-id":
				if r.MessageID == "" {
					r.MessageID = hd.Value
				}
			case strings.HasPrefix(hd.Key, "saguin-"):
				// Dropped: the broker stamps its own below.
			default:
				kept = append(kept, hd)
			}
		}
		r.Headers = kept
		if r.MessageID == "" {
			r.MessageID = uuidv7()
		}
		if c.Type == channel.Latest {
			lt := b.latest[c.Name]
			if lt == nil {
				return fmt.Errorf("channel %q is configured as latest but has no store to move "+
					"%d retained message(s) into", c.Name, len(claimed))
			}
			if _, err := lt.Set(r); err != nil {
				// Refused rather than dropped. The value is still in the
				// retained store, so an operator who raises the bound and
				// starts again loses nothing.
				return fmt.Errorf("cannot move a retained message into channel %q on storage %q: %w. "+
					"Raise that provider's max_bytes, or remove broker.retained if the values are "+
					"no longer wanted", c.Name, c.Storage, err)
			}
			moved[c.Name]++
		} else {
			dropped[c.Name]++
		}

		if _, err := b.retained.Delete(r.Topic); err != nil {
			return fmt.Errorf("cannot clear a retained message that channel %q now claims: %w",
				c.Name, err)
		}
	}

	// The count is pulled out rather than logged as moved[name], and not
	// for readability: the source check that keeps client strings out of
	// the log judges the last word of an expression, and a map index ends
	// in the key rather than the value. `count` says what it is to the
	// check and to the next reader at the same time.
	for _, name := range sortedKeys(moved) {
		count := moved[name]
		b.log.Info("retained messages moved into a channel that now claims their topics",
			"channel", name, "topics", count)
	}
	for _, name := range sortedKeys(dropped) {
		count := dropped[name]
		// Warn, and counted: this is state going away, and the only place
		// an operator can learn it did.
		b.log.Warn("retained messages discarded: a channel now claims their topics and cannot hold "+
			"a current value",
			"channel", name, "topics", count,
			"why", "only a latest channel holds one value per topic; an append channel would gain "+
				"events that never happened and a queue work nobody sent")
	}
	return nil
}

// CountStranded logs, per `latest` channel, how many of its stored values
// have topics that channel's filter no longer matches.
//
// **A filter change moves nothing that is already stored.** A record is
// served from the channel it landed in, which is the right answer - but it
// means narrowing a filter, or moving a subtree back to broadcast, leaves
// values behind that no subscriber can reach and no counter reports as
// missing. `$saguin/kv/get` answers that the topic names no latest channel,
// about a value that exists. Nothing is lost and nothing is wrong; it is
// simply invisible, which is the class of surprise this project writes down
// rather than leaves to be discovered.
//
// **Only `latest` channels are counted, and that is a decision rather than
// an oversight.** One value per topic makes the scan a walk over what a
// channel already holds in memory. An append channel would mean reading its
// whole log at every start, on a broker that may hold a fortnight of
// telemetry - so append is not checked, RFC 0002 says so in those words,
// and `saguin --route` on the old topic answers the question for either
// kind at the moment somebody asks it.
//
// It reports rather than repairs. Moving records between channels on a
// filter edit would be a broker rewriting an operator's data on a
// configuration change, which is a far worse surprise than the one this
// closes.
func (b *Broker) CountStranded() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, name := range sortedKeys(b.latest) {
		c := b.reg.Get(name)
		if c == nil || c.Type != channel.Latest {
			continue
		}
		vals, err := b.latest[name].Match(func(topic string) bool {
			return b.reg.Resolve(topic) != c
		})
		if err != nil {
			// A store that cannot be read is the startup path's problem and
			// it will say so with its own error. This is a count, and a
			// count that cannot be taken is not a reason to refuse to start.
			b.log.Warn("cannot count the values a filter change stranded",
				"channel", name, "error", err)
			continue
		}
		if len(vals) == 0 {
			continue
		}
		count := len(vals)
		b.log.Warn("stored values are no longer reachable: this channel's filter does not match "+
			"the topics they were stored under, so no subscriber can be served them",
			"channel", name, "topics", count, "filter", c.Filter,
			"why", "a filter change moves nothing already stored; records stay in the channel "+
				"they landed in. Use `saguin --route` on one of those topics to see where it "+
				"goes now. Append channels are not counted, because it would mean reading the "+
				"whole log at every start")
	}
}

// LoadSnapshots restores every durable channel from its provider's
// directory, and returns what an operator has to be told about a
// directory the broker is starting from anyway.
//
// It runs before any listener opens. An unreadable snapshot is an error
// and the broker does not start: an empty start is indistinguishable from
// a fresh install and destroys the evidence (invariant 14).
func (b *Broker) LoadSnapshots() ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var warnings []string
	providers := make([]string, 0, len(b.dirs))
	for p := range b.dirs {
		providers = append(providers, p)
	}
	sort.Strings(providers)

	for _, provider := range providers {
		chans := b.snapshotChannels()[provider]
		names := make([]string, 0, len(chans))
		for _, c := range chans {
			names = append(names, c.Name)
		}
		// Its file is asked for by name like a channel's, and restore
		// knows the name is not one.
		if b.retained != nil && provider == b.retainedProvider {
			names = append(names, RetainedStoreName)
		}

		loaded, err := b.dirs[provider].Load(names)
		if err != nil {
			return nil, fmt.Errorf("storage %q: %w", provider, err)
		}
		warnings = append(warnings, loaded.Warnings...)

		for _, name := range names {
			s, ok := loaded.Snapshots[name]
			if !ok {
				continue
			}
			// A position whose session would have expired while the broker
			// was down belongs to nobody, and keeping it is how the map
			// grows for ever (invariant 13).
			if n := s.DropExpiredPositions(time.Now()); n > 0 {
				warnings = append(warnings, fmt.Sprintf(
					"%d stored position(s) in %q were dropped: the sessions holding them expired while the broker was down",
					n, name))
			}
			// **A file older than the last shutdown comes back without its
			// exactly-once holds.** The broker has run since without them -
			// the channel was not configured, or its write never landed - and
			// answered their releases 0x92, so a publisher may have sent the
			// message again. Released now, it would be a second copy.
			if loaded.Stale[name] {
				n := 0
				for i := range s.Channels {
					n += len(s.Channels[i].Holds)
					s.Channels[i].Holds = nil
				}
				if n > 0 {
					b.counted.qos2Abandoned.Add(uint64(n))
					warnings = append(warnings, fmt.Sprintf(
						"%d unfinished exactly-once publish(es) in %q were dropped: its snapshot is older than the last shutdown, which answered their releases without them",
						n, name))
				}
			}
			for i := range s.Channels {
				if err := b.restore(&s.Channels[i]); err != nil {
					return nil, fmt.Errorf("storage %q, snapshot for %q: %w", provider, name, err)
				}
			}
		}
	}
	return warnings, nil
}

// restore puts one channel's state back. Callers hold b.mu.
func (b *Broker) restore(cs *store.ChannelState) error {
	// The retained store is not in the registry and never will be: it
	// claims no prefix and holds only topics no channel claims.
	if cs.Name == RetainedStoreName {
		if cs.Kind != store.KindLatest {
			return fmt.Errorf("the retained store was snapshotted as %s", cs.Kind)
		}
		if b.retained == nil {
			// The file is here and the configuration no longer asks for a
			// retained store. Loading it would keep values no client can
			// reach, and refusing to start over it would be worse: the
			// operator removed the block on purpose.
			return nil
		}
		b.retained = store.RestoreLatest(cs.Next, cs.Records)
		b.applyBounds()
		return nil
	}

	c := b.reg.Get(cs.Name)
	if c == nil {
		// A queue's file also holds its dead-letter channel, and every
		// channel in a file was configured when it was written. Reaching
		// here means the configuration changed underneath the snapshot.
		return fmt.Errorf("channel %q is in the snapshot but not in the configuration", cs.Name)
	}

	// A channel that changed type would otherwise load append records
	// into a latest map, or queue work into a log where nothing can
	// acknowledge it. Neither reports a problem afterwards.
	want := map[channel.Type]store.Kind{
		channel.Append: store.KindAppend,
		channel.Latest: store.KindLatest,
		channel.Queue:  store.KindQueue,
	}[c.Type]
	if cs.Kind != want {
		return fmt.Errorf("channel %q is configured as %s but was snapshotted as %s", cs.Name, c.Type, cs.Kind)
	}

	switch c.Type {
	case channel.Append:
		lg := store.RestoreLog(cs.Next, cs.Floor, cs.Records, cs.Positions)
		lg.RestoreHeld(cs.Holds)
		b.attachLog(cs.Name, lg)
	case channel.Latest:
		lt := store.RestoreLatest(cs.Next, cs.Records)
		lt.RestoreHeld(cs.Holds)
		b.latest[cs.Name] = lt
	case channel.Queue:
		// Every item comes back available, with no delivery, no Delivery
		// ID and no deadline (invariant 15). RestoreQueue enforces it.
		q := store.RestoreQueue(cs.Next, cs.Items)
		q.RestoreHeld(cs.Holds)
		b.queues[cs.Name] = q
	}
	// A restored store is a new one, so it has no bound until it is given
	// one. A channel that came back from a snapshot already over its bound
	// refuses the next publish, which is the bound working: the records are
	// there and the operator lowered the number.
	b.applyBounds()
	return nil
}
