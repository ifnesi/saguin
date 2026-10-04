package broker

import (
	"cmp"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// What the external test package needs from inside this one, and nothing
// else.

// SetPumpBetweenReadAndCommit installs the pumpBatch test seam and returns
// what puts the previous one back. See pumpBetweenReadAndCommit.
func SetPumpBetweenReadAndCommit(f func(clientID string)) (restore func()) {
	old := pumpBetweenReadAndCommit.Swap(&f)
	return func() { pumpBetweenReadAndCommit.Store(old) }
}

// SetPumpBeforeTakeoverStop installs the drainPump test seam and returns
// what puts the previous one back. See pumpBeforeTakeoverStop.
func SetPumpBeforeTakeoverStop(f func(clientID string)) (restore func()) {
	old := pumpBeforeTakeoverStop.Swap(&f)
	return func() { pumpBeforeTakeoverStop.Store(old) }
}

// SetSubackBeforeServed installs the OnPacketSent test seam and returns what
// puts the previous one back. See subackBeforeServed.
func SetSubackBeforeServed(f func(clientID string)) (restore func()) {
	old := subackBeforeServed.Swap(&f)
	return func() { subackBeforeServed.Store(old) }
}

// SetDrainBeforeLookAgain installs the bdrain.run test seam and returns what
// puts the previous one back. See drainBeforeLookAgain.
func SetDrainBeforeLookAgain(f func(client string)) (restore func()) {
	old := drainBeforeLookAgain.Swap(&f)
	return func() { drainBeforeLookAgain.Store(old) }
}

// SetSeekBeforeReply installs the handleSeek test seam and returns what puts
// the previous one back. See seekBeforeReply.
func SetSeekBeforeReply(f func(clientID string)) (restore func()) {
	old := seekBeforeReply.Swap(&f)
	return func() { seekBeforeReply.Store(old) }
}

// SetAfterBusyFlushCheck installs the bdrain.run test seam and returns what
// puts the previous one back. See afterBusyFlushCheck.
func SetAfterBusyFlushCheck(f func(client string)) (restore func()) {
	old := afterBusyFlushCheck.Swap(&f)
	return func() { afterBusyFlushCheck.Store(old) }
}

// SetGroupPass installs the runGroup test seam and returns what puts the
// previous one back. See groupPass.
func SetGroupPass(f func(b *Broker, group string, begun bool)) (restore func()) {
	old := groupPass.Swap(&f)
	return func() { groupPass.Store(old) }
}

// SetGroupCreating installs the createGroups test seam and returns what puts
// the previous one back. See groupCreating.
func SetGroupCreating(f func(b *Broker, groups []string)) (restore func()) {
	old := groupCreating.Swap(&f)
	return func() { groupCreating.Store(old) }
}

// SetAfterGiveUpAsked installs the sayGaveUp test seam and returns what puts
// the previous one back. See afterGiveUpAsked.
func SetAfterGiveUpAsked(f func(b *Broker, now time.Time)) (restore func()) {
	old := afterGiveUpAsked.Swap(&f)
	return func() { afterGiveUpAsked.Store(old) }
}

// WakeGroup asks a shared group's drain to look at its list again, as a
// member's arrival or acknowledgement does. See bdrain.wakeGroup.
func (b *Broker) WakeGroup(group string) {
	d := b.broadcastDrain()
	if g := d.group(group); g != nil {
		d.wakeGroup(g)
	}
}

// SetAfterBatchWritten installs the bdrain.batch test seam and returns what
// puts the previous one back. See afterBatchWritten.
func SetAfterBatchWritten(f func(client string, sent int)) (restore func()) {
	old := afterBatchWritten.Swap(&f)
	return func() { afterBatchWritten.Store(old) }
}

// SetStoredBeforeAnnounced installs the storeAndDeliver test seam and
// returns what puts the previous one back. See storedBeforeAnnounced.
func SetStoredBeforeAnnounced(f func(channel string, offset uint64)) (restore func()) {
	old := storedBeforeAnnounced.Swap(&f)
	return func() { storedBeforeAnnounced.Store(old) }
}

// SetAfterWillTaken installs the firePendingWill test seam and returns what
// puts the previous one back. See afterWillTaken.
func SetAfterWillTaken(f func(clientID string)) (restore func()) {
	old := afterWillTaken.Swap(&f)
	return func() { afterWillTaken.Store(old) }
}

// CountSizeSweeps installs the Run test seam and answers how many size sweeps
// b has finished since, and what puts the previous seam back. See
// afterSizeSweep.
func CountSizeSweeps(b *Broker) (count func() int64, restore func()) {
	var n atomic.Int64
	f := func(swept *Broker) {
		if swept == b {
			n.Add(1)
		}
	}
	old := afterSizeSweep.Swap(&f)
	return n.Load, func() { afterSizeSweep.Store(old) }
}

// CountQueueTicks installs the Run tick test seam and answers how many
// queuePeriod ticks b has finished since, and what puts the previous seam
// back. See afterQueueTick.
func CountQueueTicks(b *Broker) (count func() int64, restore func()) {
	var n atomic.Int64
	f := func(ticked *Broker) {
		if ticked == b {
			n.Add(1)
		}
	}
	old := afterQueueTick.Swap(&f)
	return n.Load, func() { afterQueueTick.Store(old) }
}

// CountOwedRetries is CountSizeSweeps for the owed-write retry. See
// afterOwedRetry.
func CountOwedRetries(b *Broker) (count func() int64, restore func()) {
	return countPasses(&afterOwedRetry, b)
}

// CountPositionFlushes is CountSizeSweeps for the position flush. See
// afterPositionsFlush.
func CountPositionFlushes(b *Broker) (count func() int64, restore func()) {
	return countPasses(&afterPositionsFlush, b)
}

func countPasses(seam *atomic.Pointer[func(*Broker)], b *Broker) (count func() int64, restore func()) {
	var n atomic.Int64
	f := func(passed *Broker) {
		if passed == b {
			n.Add(1)
		}
	}
	old := seam.Swap(&f)
	return n.Load, func() { seam.Store(old) }
}

// EndingOwed reports whether the store still owes part of an ending for id
// (oweEnding).
func (b *Broker) EndingOwed(id string) bool { return b.endingOwed(id) }

// LatestMissed is the lowest offset of a latest channel recorded missed for a
// client, or 0 (missedLatest).
func (b *Broker) LatestMissed(client, channel string) uint64 {
	l := b.latestSeen
	l.posMu.Lock()
	defer l.posMu.Unlock()
	return l.by[client][channel].missed
}

// ClientLockWait is how long taking one client's own lock took, which is
// what its deliveries and acknowledgements wait for. Zero when the client
// has no delivery state. See consumer.
func (b *Broker) ClientLockWait(id string) time.Duration {
	con := b.lookupConsumer(id)
	if con == nil {
		return 0
	}
	start := time.Now()
	con.cmu.Lock()
	con.cmu.Unlock()
	return time.Since(start)
}

// SetLatestBeforeWrite installs the publishLatest test seam and returns what
// puts the previous one back. See latestBeforeWrite.
func SetLatestBeforeWrite(f func(clientID string)) (restore func()) {
	old := latestBeforeWrite.Swap(&f)
	return func() { latestBeforeWrite.Store(old) }
}

// SetLatestAfterState installs the deliverLatest merge seam and returns what
// puts the previous one back. See latestAfterState.
func SetLatestAfterState(f func(clientID string)) (restore func()) {
	old := latestAfterState.Swap(&f)
	return func() { latestAfterState.Store(old) }
}

// SetLatestBeforeQueue installs the publishLatest test seam and returns what
// puts the previous one back. See latestBeforeQueue.
func SetLatestBeforeQueue(f func(topic string, offset uint64)) (restore func()) {
	old := latestBeforeQueue.Swap(&f)
	return func() { latestBeforeQueue.Store(old) }
}

// SetLatestAfterSet installs the storeAndDeliver test seam and returns what
// puts the previous one back. See latestAfterSet.
func SetLatestAfterSet(f func(topic string, offset uint64)) (restore func()) {
	old := latestAfterSet.Swap(&f)
	return func() { latestAfterSet.Store(old) }
}

// SetLatestAfterNoRoom installs the runLatest test seam and returns what puts
// the previous one back. See latestAfterNoRoom.
func SetLatestAfterNoRoom(f func(clientID string)) (restore func()) {
	old := latestAfterNoRoom.Swap(&f)
	return func() { latestAfterNoRoom.Store(old) }
}

// SetLatestBeforeState installs the deliverLatest test seam and returns what
// puts the previous one back. See latestBeforeState.
func SetLatestBeforeState(f func(clientID string)) (restore func()) {
	old := latestBeforeState.Swap(&f)
	return func() { latestBeforeState.Store(old) }
}

// ConsumerRecorded reports whether the registry holds a delivery record for
// this client. A record is tidied away once it holds nothing, which is the
// state the latest-path race test has to prove it reached.
func (b *Broker) ConsumerRecorded(id string) bool {
	return b.lookupConsumer(id) != nil
}

// ConsumerHolding says what a client's delivery record holds, field by
// field - what keeps it from being tidied. Empty string when there is no
// record.
func (b *Broker) ConsumerHolding(id string) string {
	con := b.lookupConsumer(id)
	if con == nil {
		return ""
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	return fmt.Sprintf("cursors=%d inflight=%d plans=%d hasQueues=%v latest=%d draining=%d gone=%v",
		len(con.cursors), len(con.inflight), len(con.plans), con.hasQueues, len(con.latest),
		len(con.draining), con.gone)
}

// The three readers below are for tests only, which is why they are here:
// production takes a cursor through cursorC, under one hold of the
// consumer's cmu, and a read-modify-write split across two holds is the
// mistake these would make easy.

// cursor returns a consumer's cursor for one channel, seeded from its
// stored position as cursorC seeds it. The caller holds b.mu.
func (b *Broker) cursor(clientID, chName string, fromTail bool) *cursor {
	con := b.consumerLocked(clientID)
	con.cmu.Lock()
	defer con.cmu.Unlock()
	return b.cursorC(con, clientID, chName, fromTail)
}

// cursorLocked is a client's cursor on a channel, or nil. The caller holds
// b.mu, and not the consumer's cmu, which this takes.
func (b *Broker) cursorLocked(id, chName string) *cursor {
	con := b.lookupConsumer(id)
	if con == nil {
		return nil
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	return con.cursors[chName]
}

// cursorsOfLocked is every cursor a client has, by channel, copied; nil for
// none. The caller holds b.mu, and not the consumer's cmu.
func (b *Broker) cursorsOfLocked(id string) map[string]*cursor {
	con := b.lookupConsumer(id)
	if con == nil {
		return nil
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	if len(con.cursors) == 0 {
		return nil
	}
	return maps.Clone(con.cursors)
}

// LatestSuperseded is a latest channel's values a subscriber was not sent
// because a newer one for its topic took their place while it waited - what
// saguin_latest_superseded_total reports.
func LatestSuperseded(b *Broker, channel string) uint64 {
	if cc := b.counted.forChannel(channel); cc != nil {
		return cc.latestSuperseded.Load()
	}
	return 0
}

// LogOf is an append channel's log behind every wrapper, read under the
// broker's lock. It only reads: a log is replaced before the broker serves
// (brokertest.WrapLogs), never on a running one, whose pumps read the log
// map without a lock.
func LogOf(b *Broker, name string) LogStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	lg, _ := storeBehind(b.logs[name]).(LogStore)
	return lg
}

// UseBroadcastLog attaches a session provider's broadcast log, and the store
// that owns it, to the drain, counting again who owes what it holds. See
// useBroadcastLog.
func (b *Broker) UseBroadcastLog(lg broadcastLog, st broadcastSessions) error {
	return b.useBroadcastLog(lg, st)
}

// AttachBroadcast gives a durable session an owed list on the log. See
// bdrain.attach.
func (b *Broker) AttachBroadcast(client string) { b.broadcastDrain().attach(client) }

// CountBroadcast puts a message the log has kept on the owed lists of the
// sessions given. See bdrain.count.
func (b *Broker) CountBroadcast(rec store.Record, recipients ...string) {
	b.broadcastDrain().count(rec, recipients, nil)
}

// KeepBroadcast appends a message to the log and counts it for the sessions
// given, making room at the provider's bound. See bdrain.keep.
func (b *Broker) KeepBroadcast(rec store.Record, recipients ...string) (store.Record, bool, error) {
	return b.broadcastDrain().keep(rec, recipients, nil)
}

// WithRoom runs a write the broadcast log gives way to on its provider. See
// withRoom.
func (b *Broker) WithRoom(provider string, need int64, writer string, write func() error) error {
	return b.withRoom(provider, need, writer, write)
}

// SetGiveUpReportEvery sets how often a full log says what it gave up, and
// returns what puts it back. See giveUpReportEvery.
func SetGiveUpReportEvery(d time.Duration) (restore func()) {
	old := giveUpReportEvery
	giveUpReportEvery = d
	return func() { giveUpReportEvery = old }
}

// EndBroadcast lets go of everything a session was owed. See bdrain.end.
func (b *Broker) EndBroadcast(client string) { b.broadcastDrain().end(client) }

// WakeBroadcast starts a session's drain, as its client connecting does.
func (b *Broker) WakeBroadcast(client string) {
	d := b.broadcastDrain()
	if o := d.session(client); o != nil {
		d.wake(o, nil)
	}
}

// BroadcastList is a session's owed list itself, or nil where the drain holds
// none: what a batch that began before the session ended still holds after
// it. See bdrain.session.
func (b *Broker) BroadcastList(client string) any {
	if o := b.broadcastDrain().session(client); o != nil {
		return o
	}
	return nil
}

// BroadcastFlight is the drain's entry for a session's delivery on the wire
// under id: how far its exchange has got, and the connection it is on.
func (b *Broker) BroadcastFlight(client string, id uint16) (store.MessageState, *mqtt.Client, bool) {
	o := b.broadcastDrain().session(client)
	if o == nil {
		return 0, nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	f, ok := o.flying[id]
	return f.entry.State, f.conn, ok
}

// LetGoOnList lets go of the message at off on a list BroadcastList answered,
// the way a batch does when it cannot record what it picked (drop), when it
// has written it at QoS 0 (passed), or when it is larger than its client
// takes (discard).
func (b *Broker) LetGoOnList(list any, off uint64, how string) {
	d, o := b.broadcastDrain(), list.(*owed)
	switch how {
	case "drop":
		d.drop(o, []uint64{off}, 0)
	case "passed":
		d.passed(o, off)
	case "discard":
		d.discard(nil, o, packets.Packet{}, off)
	default:
		panic("LetGoOnList: " + how)
	}
}

// OwedBroadcast is a session's owed list and what it comes to against its
// limits.session_queue_bytes.
func (b *Broker) OwedBroadcast(client string) (offsets []uint64, bytes int64) {
	o := b.broadcastDrain().session(client)
	if o == nil {
		return nil, 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range o.list {
		offsets = append(offsets, e.offset)
	}
	return offsets, o.bytes
}

// PendingWillFire is what the timer of client's delayed Will runs, for a
// test to run at a moment of its choosing; false where none is pending.
func (b *Broker) PendingWillFire(client string) (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.pendingWills[client]
	if !ok {
		return nil, false
	}
	return func() { b.firePendingWill(client, p) }, true
}

// WillsPublished is how many Wills this broker has published for cause.
func (b *Broker) WillsPublished(cause string) uint64 {
	b.counted.willsMu.Lock()
	defer b.counted.willsMu.Unlock()
	return b.counted.willsPub[cause]
}

// Stopping reports whether Shutdown has begun.
func (b *Broker) Stopping() bool { return b.stopping.Load() }

// PublishesRefused is saguin_publish_refused_total for one reason.
func (b *Broker) PublishesRefused(reason string) uint64 {
	b.counted.refusedMu.Lock()
	defer b.counted.refusedMu.Unlock()
	return b.counted.refused[reason]
}

// AfterDrainsWait has Shutdown run f once it has waited for the drains, for
// the length of one test.
func AfterDrainsWait(t testing.TB, f func()) {
	afterDrainsWait = f
	t.Cleanup(func() { afterDrainsWait = nil })
}

// OwedTotals is what a session's owed list says it holds against its bound
// and on the wire, beside the same two summed from its entries as they stand.
// They differ only where a change of an entry's state moved the totals by
// the wrong amount.
func (b *Broker) OwedTotals(client string) (bytes, wire, entryBytes, entryWire int64) {
	o := b.broadcastDrain().session(client)
	if o == nil {
		return 0, 0, 0, 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range o.list {
		entryBytes += e.cost()
		if !e.waiting() {
			entryWire += e.cost()
		}
	}
	if entryWire > 0 {
		entryBytes += wireTableCost // once, while anything is on the wire (tableLocked)
	}
	return o.bytes, o.wire, entryBytes, entryWire
}

// FlyingBroadcast is the drain's own copy of a session's in-flight table, in
// packet identifier order.
func (b *Broker) FlyingBroadcast(client string) []store.InFlight {
	o := b.broadcastDrain().session(client)
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []store.InFlight
	for _, f := range o.flying {
		out = append(out, f.entry)
	}
	slices.SortFunc(out, func(a, b store.InFlight) int { return cmp.Compare(a.PacketID, b.PacketID) })
	return out
}

// PickedBroadcast is what a session's drain has picked to write and not yet
// sent or put back, in offset order.
func (b *Broker) PickedBroadcast(client string) []uint64 {
	o := b.broadcastDrain().session(client)
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []uint64
	for _, e := range o.list {
		if e.picked {
			out = append(out, e.offset)
		}
	}
	return out
}

// BroadcastOwners is how many owed lists hold the message at an offset.
func (b *Broker) BroadcastOwners(offset uint64) uint32 {
	d := b.broadcastDrain()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.owners[offset]
}

// OwedEntryCost is what one entry on an owed list costs beside its message.
const OwedEntryCost = owedEntryCost

// WireTableCost is what a session with anything on the wire is charged once.
const WireTableCost = wireTableCost

// ReleaseBatch is how many released messages end a release's wait. See
// bdrain.release.
const ReleaseBatch = releaseBatch

// WireEntryCost is what an owed entry costs besides while it is on the wire.
const WireEntryCost = wireEntryCost

// OwedHeap builds an owed list of n waiting entries, then puts min(n, 65535)
// of them on the wire as the drain records them (flying and stored), and
// answers the heap each entry took on the list and each wire entry took in
// those two maps. The in-flight PUBLISH the substrate holds for a wire entry
// is mqtt.InflightEntryOverhead's to measure, not this.
func OwedHeap(n int) (perEntry, perFlight float64) {
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	o := &owed{flying: map[uint16]flight{}, stored: map[uint16]uint64{}}
	for i := range n {
		o.insertLocked(owedEntry{offset: uint64(i + 1), charge: 136})
	}
	mid := heap()
	flights := min(n, 65535)
	for i := range flights {
		id := uint16(i + 1)
		o.flying[id] = flight{entry: store.InFlight{Offset: uint64(i + 1), PacketID: id, QoS: 1}}
		o.stored[id] = uint64(i + 1)
	}
	after := heap()
	runtime.KeepAlive(o)
	return float64(mid-before) / float64(n), float64(after-mid) / float64(flights)
}

// BroadcastTableHeap gives each of tables sessions n broadcast deliveries on
// the wire as the drain and the substrate hold them - on the session's owed
// list and marked sent, in its flying and stored maps, and Bounded in the
// client's in-flight table - and answers the heap a session's took, with
// each session's list and table made beforehand as they are with the
// session, and what a session is charged for them: its list's bytes and its
// table's.
func BroadcastTableHeap(tables, n int) (perTable, charged float64) {
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	src := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName: "sensors/device-0001/temperature", Payload: make([]byte, 20)}
	charge := store.RecordSize(store.Record{Topic: src.TopicName, Payload: src.Payload, QoS: 1}) + owedEntryCost
	ins := make([]*mqtt.Inflight, tables)
	os := make([]*owed, tables)
	for k := range tables {
		ins[k] = mqtt.NewInflights()
		os[k] = &owed{flying: map[uint16]flight{}, stored: map[uint16]uint64{}}
	}
	before := heap()
	for k := range tables {
		o, in := os[k], ins[k]
		for i := range n {
			id, off := uint16(i+1), uint64(i+1)
			o.insertLocked(owedEntry{offset: off, charge: charge})
			o.pickLocked([]uint64{off})
			o.markSentLocked(off)
			o.flying[id] = flight{entry: store.InFlight{Offset: off, PacketID: id, QoS: 1}}
			o.stored[id] = off
			pk := src.Copy(false)
			pk.PacketID, pk.Bounded = id, true
			in.Set(pk)
		}
	}
	after := heap()
	var sum int64
	for k := range tables {
		sum += os[k].bytes + ins[k].Bytes()
	}
	runtime.KeepAlive(ins)
	runtime.KeepAlive(os)
	return (float64(after) - float64(before)) / float64(tables), float64(sum) / float64(tables)
}

// UnwrittenDisconnects is how many disconnects the session store refused to
// record and the broker still remembers (recordDisconnect).
func (b *Broker) UnwrittenDisconnects() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.unwritten)
}

// PumpBatchCap is the most a drain or a pump sends in one batch.
const PumpBatchCap = pumpBatchCap

// SubackPending reports whether what a connection's SUBSCRIBE earned is still
// waiting for its SUBACK to reach the wire.
func (b *Broker) SubackPending(cl *mqtt.Client) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.afterSuback[cl]
	return ok
}

// BroadcastSince is where the log was when each of a session's subscriptions
// was made, as the drain holds it, or nil for a session it holds none for.
func (b *Broker) BroadcastSince(client string) map[string]uint64 {
	d := b.broadcastDrain()
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.sinces[client])
}

// holdIn holds an exactly-once publish in a channel's store and records
// where, as holdForRelease does for a connected publisher.
func (b *Broker) holdIn(channel string, e store.Exchange, r store.Record, at time.Time) error {
	if err := b.holdsFor(channel).Hold(e, r, at); err != nil {
		return err
	}
	b.heldMu.Lock()
	defer b.heldMu.Unlock()
	if b.held == nil {
		b.held, b.heldBy = map[store.Exchange]heldExchange{}, map[string]int{}
	}
	if _, ok := b.held[e]; !ok {
		b.held[e] = heldExchange{channel: channel, at: at}
		b.heldBy[e.Client]++
	}
	return nil
}

// heldCount is how many exactly-once publishes the broker records a client
// as holding.
func (b *Broker) heldCount(client string) int {
	b.heldMu.Lock()
	defer b.heldMu.Unlock()
	return b.heldBy[client]
}

// storeHolds reports whether a channel's store holds an exchange.
func (b *Broker) storeHolds(channel string, e store.Exchange) bool {
	held, _ := b.holdsFor(channel).Holds()
	for _, h := range held {
		if h.Exchange == e {
			return true
		}
	}
	return false
}

// HeldBy is heldCount, for the tests outside the package.
func (b *Broker) HeldBy(client string) int { return b.heldCount(client) }

// QoS2Abandoned is saguin_qos2_abandoned_total as the broker counts it.
func (b *Broker) QoS2Abandoned() uint64 { return b.counted.qos2Abandoned.Load() }

// VerifiedName is the MQTT listener's name for a certificate, for the tests
// that compare it with the operations listener's.
func VerifiedName(conn net.Conn) (string, bool) { return verifiedName(conn) }

// CertificateName is the operations listener's.
func CertificateName(cert *x509.Certificate) string { return certificateName(cert) }

// GroupEntry is one entry of a shared group's list: its offset, and whether
// it is returned, lent to a clean member, or picked by the group's drain.
type GroupEntry struct {
	Offset                 uint64
	Returned, Lent, Picked bool
}

// GroupList is a shared group's list, in offset order, and whether the group
// has one. See bgroup.go.
func (b *Broker) GroupList(group string) ([]GroupEntry, bool) {
	g := b.broadcastDrain().group(group)
	if g == nil {
		return nil, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]GroupEntry, 0, len(g.list))
	for _, e := range g.list {
		out = append(out, GroupEntry{Offset: e.offset, Returned: e.returned, Lent: e.lent != nil, Picked: e.picked})
	}
	return out, true
}

// Unhand undoes the hand-over of the group delivery client holds under pid,
// as a write that never reached the connection does, and reports whether the
// client held one. See bdrain.unhand.
func (b *Broker) Unhand(client string, pid uint16) bool {
	d := b.broadcastDrain()
	o := d.session(client)
	cl, ok := b.srv.Clients.Get(client)
	if o == nil || !ok {
		return false
	}
	o.mu.Lock()
	f, held := o.flying[pid]
	o.mu.Unlock()
	g := d.group(f.entry.Group)
	if !held || g == nil {
		return false
	}
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: f.entry.QoS}, PacketID: pid}
	d.unhand(g, cl, o, pk, f.entry)
	return true
}

// EndWith ends a durable session's owed list and settles what its groups had
// handed it with the store's answer given, as endStoredSession does. See
// bdrain.ended.
func (b *Broker) EndWith(client string, dropped store.Dropped) {
	d := b.broadcastDrain()
	d.ended(d.end(client), dropped, nil)
}

// BroadcastLogForTest is the log the drain reads, to ask what it still holds.
func (b *Broker) BroadcastLogForTest() interface {
	ReadAt(offsets ...uint64) ([]store.Record, error)
} {
	return b.broadcastDrain().log
}

// ChannelPosition is what channel keeps as client's position, and whether it
// keeps one.
func (b *Broker) ChannelPosition(channel, client string) (uint64, bool, error) {
	b.mu.Lock()
	lg := b.logs[channel]
	b.mu.Unlock()
	p, ok, err := lg.Position(store.MQTTReader(client))
	return p.Offset, ok, err
}

// MeasureSharedFitWith makes fitsShared measure with margin, and answers what
// puts it back: a margin far below zero has every member fit whatever its
// Maximum Packet Size.
func MeasureSharedFitWith(margin int) (restore func()) {
	was := sharedFitMargin
	sharedFitMargin = margin
	return func() { sharedFitMargin = was }
}

// EndGroupForTest ends a group's list in memory, as its last member's ending
// does, leaving the store to the test.
func (b *Broker) EndGroupForTest(group string) {
	b.broadcastDrain().endGroup(group)
}

// UnhandIntoEndedGroup undoes a hand-over, as Unhand does, into a group whose
// list ends between the store's return of the delivery and its going back
// on the list: the list's own reference, taken before it ended.
func (b *Broker) UnhandIntoEndedGroup(client string, pid uint16) bool {
	d := b.broadcastDrain()
	o := d.session(client)
	cl, ok := b.srv.Clients.Get(client)
	if o == nil || !ok {
		return false
	}
	o.mu.Lock()
	f, held := o.flying[pid]
	o.mu.Unlock()
	g := d.group(f.entry.Group)
	if !held || g == nil {
		return false
	}
	d.endGroup(f.entry.Group)
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: f.entry.QoS}, PacketID: pid}
	d.unhand(g, cl, o, pk, f.entry)
	return true
}

// AppendUncounted appends a broadcast owed to owedTo to the log as a publish
// does, and leaves it uncounted until count is called: a publish held between
// its append and its selection (bdrain.published, bdrain.chosen).
func (b *Broker) AppendUncounted(topic, payload, owedTo string) (count func()) {
	d := b.broadcastDrain()
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, TopicName: topic,
		Payload: []byte(payload)}
	off, err := d.published(b.record(pk, nil, "", owedTo))
	if err != nil {
		panic(err)
	}
	return func() { d.chosen(off, []string{owedTo}, nil) }
}

// CountedThrough is how far the broadcast drain has counted: every offset up
// to it is on every list it belongs on.
func (b *Broker) CountedThrough() uint64 { return b.broadcastDrain().counted.through.Load() }

// PendingValues is how many values wait on a client's pending list for a
// channel or the retained store (consumer.latest), or -1 where the client has
// no delivery state.
func PendingValues(b *Broker, id, ch string) int {
	con := b.lookupConsumer(id)
	if con == nil {
		return -1
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	return len(con.latest[ch])
}

// StartPeak starts a broadcast drain over lg and st as a broker bounded at
// bound per session does, and reports the most the named session's owed list
// - or the shared group's, where group is set - held while the start was
// still reading the log, sampled before each batch it read; how many batches
// it read; and what the list holds once the start is done.
func StartPeak(lg *store.Log, st *store.Sessions, bound int64, list string, group bool) (peak int64, reads int, after int64, err error) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		return 0, 0, 0, err
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.limits.SessionQueueBytes = bound
	var d *bdrain
	held := func() int64 {
		m := &d.sessions
		if group {
			m = &d.groups
		}
		v, ok := m.Load(list)
		if !ok {
			return 0
		}
		o := v.(*owed)
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.bytes
	}
	p := &peekLog{Log: lg, peek: func() {
		reads++
		if d != nil {
			peak = max(peak, held())
		}
	}}
	d = newBroadcastDrain(b, p, st)
	if err := d.rebuild(); err != nil {
		return 0, 0, 0, err
	}
	return peak, reads, held(), nil
}

// peekLog is a broadcast log that calls peek before each batch it is read.
type peekLog struct {
	*store.Log
	peek func()
}

func (p *peekLog) ReadFromN(offset uint64, max int) ([]store.Record, error) {
	p.peek()
	return p.Log.ReadFromN(offset, max)
}

// LatestPending is how many values wait on a client's pending list for a
// latest channel (consumer.latest), or -1 where the client has no delivery
// state.
func LatestPending(b *Broker, id, ch string) int {
	con := b.lookupConsumer(id)
	if con == nil {
		return -1
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	return len(con.latest[ch])
}
