package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"math/bits"
	"slices"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/timestamp"
	"github.com/ifnesi/saguin/internal/topic"
)

// RetainedCharge loads only the committed charge of a topic with an active
// replacement. The core never loads a catalogue of retained topics at rest.
type RetainedCharge struct {
	Channel int64 // zero selects $broadcast; latest charges exclude incoming holds
	Offset  int64 // current value, independent of a larger held replacement
	Topic   string
	Bytes   int64
}

// ReadConn reads one topic through the unique current-value index.
func (r *RetainedCharge) ReadConn(conn *Conn) error {
	return conn.Prepared(retainedCharge).QueryRowContext(context.Background(), r.Topic, r.Channel).Scan(&r.Bytes, &r.Offset)
}

// ClearRetained removes the previous flag and returns its offset and bytes
// released from the retained charge. Below the floor the old value is deleted
// here; otherwise its bytes become ordinary log bytes. The caller stores the
// replacement and channel accounting in this transaction.
func (r *Record) ClearRetained(tx *Tx, floor int64) (offset, size int64, err error) {
	err = tx.Prepared(clearRetainedTopic).QueryRowContext(context.Background(), r.Topic).Scan(&offset, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err == nil && offset < floor {
		err = deletePassedRetained(tx, offset)
	}
	return offset, size, err
}

// deletePassedRetained reclaims a flag that ended below the floor in the
// transaction ending that flag. Passing already counted its losses.
func deletePassedRetained(tx *Tx, offset int64) error {
	e := Retention{Channel: 1}
	return e.deleteRecords(tx, offset, offset)
}

// RetentionPolicy names a channel's metric registration. Record deadlines
// come from the retention walk; records and retained metadata stay in SQLite.
type RetentionPolicy struct {
	Channel     int64
	Bytes       *metrics.Gauge // immutable registration, shared by the periodic store read
	Consumers   *metrics.Gauge
	PositionMin *metrics.OptionalGauge
}

// RetentionConsumer is the core's immutable progress at the deletion's ordered
// boundary. Scan covers examined records; Pending names selected, never-sent
// live deliveries below it. Handed-off and settled deliveries are excluded.
// Filters and their partition sets are immutable after subscription admission.
type RetentionConsumer struct {
	Rule              int64 // bridge rule; Scan zero reads its away cursor at this write boundary
	Share             int64
	Access            *acl.Client // immutable connected-client grants; away losses never consult access
	Subject           string      // empty for broadcast
	Session, Scan     int64
	Client            string
	Filters           []Subscription
	Pending, Excluded []int64
	MarkersBefore     int64 // settled inherited records below this bound are not losses
	Recovering        bool
	Missed            uint64
}

// Retention extends retained-value expiry with log age and size retention.
// A transaction examines at most 512 records in offset order, then deletes
// the passed append prefix or the due latest values within that page.
// Results are applied only after commit.
type Retention struct {
	page                                  []retentionRecord // one selected queue page, charged until its write answers
	Held                                  []int64           // active queue holds, in offset order at the write boundary
	Latest                                bool              // current values and tombstones have independent age retention
	DeadLetters                           *DeadLetters      // queue destination and committed move metadata, never payloads
	Job                                   *QueueChange      // a failed attempt moving this one job, in the same transaction
	TombstonePeriod                       int64
	Deleted                               []int64 // latest offsets removed, for prepared-delivery cancellation
	TrackCharges                          bool    // retained limit needs topic maxima, including incoming holds
	ChargeReleased                        int64
	Channel, At, Period, LogPeriod, Limit int64
	Floor, Tail, Size                     int64
	After, Last                           int64
	Next                                  int64 // earliest future deadline seen in this page, or zero
	Consumers                             []RetentionConsumer
	Named                                 map[int64]struct{}
	Bytes                                 int64 // retained bytes released by flag expiry
	Count                                 int   // retained flags cleared
	Removed                               int
	Delta                                 int64 // change to the log's unflagged byte charge
	Missed                                uint64
	More                                  bool
	Expire                                bool // an age deadline or startup, not a size-only pass
}

// retentionPage bounds record examination and deletion independently of the
// smaller metadata pages used for subscriptions and retained-value expiry.
const retentionPage = 512

// Records returns zero: retention introduces no publications.
func (*Retention) Records() int { return 0 }

// WriteTx clears due flags, determines losses before removing their metadata,
// and advances the floor and byte charge in the same transaction.
func (e *Retention) WriteTx(tx *Tx) error {
	if e.Job != nil {
		if err := e.Job.WriteTx(tx); err != nil {
			return err
		}
	}
	if e.page == nil {
		e.After = max(e.After, e.Floor-1)
	}
	if e.Channel == 1 && e.Expire {
		if err := e.expireValues(tx); err != nil {
			return err
		}
	}
	if e.page == nil {
		limit := retentionPage - e.Removed
		page, err := e.readPage(tx.Prepared(readRetentionRecords), limit)
		if err != nil {
			return err
		}
		e.selectRecords(page, limit)
	}
	if err := e.losses(tx, e.page); err != nil {
		return err
	}
	if d := e.DeadLetters; d != nil {
		waiting := false
		for i := range d.Moves {
			if err := e.moveJob(tx, &d.Moves[i]); err != nil {
				return err
			}
			waiting = waiting || d.Moves[i].Waiting
		}
		if waiting {
			var err error
			d.RetryAt, err = queueRetryAt(tx.Prepared(nextQueueRetry), e.Channel)
			if err != nil {
				return err
			}
		}
	}
	return e.deleteRun(tx, e.After+1)
}

// ReadRetention selects one queue metadata page under the joint channel
// reservation. It is a decision read, following earlier accepted writes.
// The core assigns move offsets before handing this same page to WriteTx.
type ReadRetention struct{ Retention *Retention }

// ReadConn keeps the selected page within the shared metadata credit.
func (r *ReadRetention) ReadConn(conn *Conn) error {
	e := r.Retention
	e.After = max(e.After, e.Floor-1)
	limit := MetadataPage
	if e.Job != nil {
		limit = 1
	}
	page, err := e.readPage(conn.Prepared(readRetentionRecords), limit)
	e.page = page
	return err
}

// Select applies the shared retention walk to the queue's already-read page.
// The caller first supplies the current held jobs and effective byte charge.
func (e *Retention) Select() { e.selectRecords(e.page, MetadataPage) }

// ReleasePage returns the bounded metadata after commit, refusal or abandonment.
func (e *Retention) ReleasePage() { e.page = nil }

// selectRecords determines the due subset once. Queue selection runs in the
// core; other channels use it inside their existing retention transaction.
func (e *Retention) selectRecords(page []retentionRecord, limit int) {
	examined, due := len(page), 0
	var through int64
	if examined != 0 {
		through = page[examined-1].offset
	}
	for i := range page {
		r := &page[i]
		if e.Job != nil && e.Job.Spend && r.spent.Valid {
			r.spent.Int64++
			if !r.first.Valid {
				r.first = sql.NullInt64{Int64: e.Job.Began, Valid: true}
			}
			r.last = sql.NullInt64{Int64: e.Job.Began, Valid: true}
		}
		period := e.LogPeriod
		if e.Latest {
			e.Last = r.offset // continuation follows examined records, including survivors
			if r.payloadSize == 0 {
				period = e.TombstonePeriod
			}
		}
		if _, held := slices.BinarySearch(e.Held, r.offset); held {
			e.Last = r.offset
			continue
		}
		deadline := timestamp.AfterMilli(r.time, period)
		if e.Job == nil && (period == 0 || deadline > e.At) {
			if e.Latest {
				if period != 0 && (e.Next == 0 || deadline < e.Next) {
					e.Next = deadline
				}
				continue
			}
			if e.Limit == 0 || e.Size+e.Delta <= e.Limit {
				if period != 0 {
					e.Next = deadline
				}
				break
			}
		}
		e.Last = r.offset
		if !e.Latest {
			e.Floor = r.offset + 1
		}
		if !r.retained || e.Latest {
			e.Delta -= r.bytes
			e.Removed++
			if e.Latest {
				e.Deleted = append(e.Deleted, r.offset)
			}
		}
		// Compact due rows for the shared loss-counting path, skipping holds.
		// The examined offset boundary remains separate from this subset.
		if due != i {
			page[due] = *r
		}
		if d := e.DeadLetters; d != nil && r.spent.Valid {
			d.Moves = append(d.Moves, DeadLetter{Source: r.offset, record: &page[due], Expired: period != 0 && deadline <= d.At, Exhausted: e.Job != nil, Waiting: r.nextTry.Valid})
		}
		due++
	}
	e.page = page[:due]
	e.More = e.Job == nil && (e.More || (examined == limit && (e.Latest || due == examined || (e.DeadLetters != nil && e.Last == through))))
	if !e.Latest && e.DeadLetters == nil && due == examined && examined < limit {
		// Gaps are unused offsets, never losses. All surviving log records
		// were passed; retained values below the floor remain initial-only.
		e.Floor = max(e.Floor, e.Tail)
	}
}

// deleteRun preserves broadcast retained values and removes only the examined
// prefix or due latest records.
// Every consumer's loss count is determined before the range is deleted.
func (e *Retention) deleteRun(tx *Tx, from int64) error {
	if e.Removed != 0 {
		for _, held := range e.Held {
			if held < from {
				continue
			}
			if held > e.Last {
				break
			}
			if err := e.deleteRecords(tx, from, held-1); err != nil {
				return err
			}
			from = held + 1
		}
		if err := e.deleteRecords(tx, from, e.Last); err != nil {
			return err
		}
	}
	return e.advance(tx)
}

// recordArgs binds the range deletion to the same age cut used by the walk.
func (e *Retention) recordArgs(from, bound int64) []any {
	return []any{e.Channel, from, bound, e.Latest, e.LogPeriod, e.At,
		timestamp.AfterMilli(0, e.LogPeriod), e.TombstonePeriod, timestamp.AfterMilli(0, e.TombstonePeriod)}
}

func (e *Retention) deleteRecords(tx *Tx, from, through int64) error {
	_, err := tx.Prepared(deleteRetentionRecords).ExecContext(context.Background(), e.recordArgs(from, through)...)
	return err
}

func (e *Retention) expireValues(tx *Tx) error {
	// Keep the Message Expiry cutoff at e.At; only the age branch uses
	// the adjusted retention period.
	period := timestamp.AfterMilli(0, e.Period)
	rows, err := tx.Prepared(expireRetained).QueryContext(context.Background(), 0, e.At, e.Period, e.At, period)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var page [MetadataPage]struct {
		offset, size int64
		topic        string
	}
	n := 0
	for rows.Next() {
		p := &page[n]
		if err := rows.Scan(&p.offset, &p.size, &p.topic); err != nil {
			return err
		}
		e.Bytes += p.size
		e.Count++
		n++
		if p.offset >= e.Floor {
			e.Delta += p.size
		}
	}
	if err := finishRows(rows); err != nil {
		return err
	}
	for _, p := range page[:n] {
		if e.TrackCharges {
			charge := RetainedCharge{Topic: p.topic}
			if err := charge.ReadTx(tx); err != nil {
				return err
			}
			e.ChargeReleased += max(0, p.size-charge.Bytes)
		}
		if p.offset < e.Floor {
			if err := deletePassedRetained(tx, p.offset); err != nil {
				return err
			}
			e.Removed++
		}
	}
	e.More = e.Count == MetadataPage
	return nil
}

type retentionRecord struct {
	offset, time, bytes         int64
	bridge                      int64
	topic                       []byte
	publisher                   string
	retained                    bool
	payloadSize                 int
	spent, first, last, nextTry sql.NullInt64
	propertyBytes               int // original Property Length, excluding its prefix
}

func (e *Retention) readPage(statement *sql.Stmt, limit int) ([]retentionRecord, error) {
	rows, err := statement.QueryContext(context.Background(), e.Channel, e.After, e.Tail, limit, e.DeadLetters != nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	page := make([]retentionRecord, 0, limit)
	for rows.Next() {
		var r retentionRecord
		var prefix []byte
		var size int
		if err := rows.Scan(&r.offset, &r.time, &r.topic, &r.publisher, &r.retained, &r.bytes, &r.payloadSize,
			&r.spent, &r.first, &r.last, &r.nextTry, &prefix, &size, &r.bridge); err != nil {
			return nil, err
		}
		if size != 0 {
			n, rest, err := packet.DecodeVarInt(prefix)
			if err != nil || int(n)+len(prefix)-len(rest) != size {
				return nil, packet.ErrMalformed
			}
			r.propertyBytes = int(n)
		}
		page = append(page, r)
	}
	return page, finishRows(rows)
}

func (e *Retention) advance(tx *Tx) error {
	if d := e.DeadLetters; d != nil && d.Expired+d.Evicted+d.Exhausted != 0 {
		if err := d.Channel.WriteTx(tx); err != nil {
			return err
		}
	}
	return tx.Prepared(advanceRetention).QueryRowContext(context.Background(), e.Floor, e.Delta, e.Channel).Scan(&e.Floor)
}

// losses counts each consumer-record pair once, even when several filters
// contribute. No payload or loss history leaves this transaction.
func (e *Retention) losses(tx *Tx, records []retentionRecord) error {
	if len(records) != 0 {
		for i := range e.Consumers {
			if u := &e.Consumers[i]; u.Share != 0 {
				if err := e.sharedLossOffsets(tx, u, records); err != nil {
					return err
				}
			}
		}
	}
	for i := range e.Consumers {
		u := &e.Consumers[i]
		if u.Rule != 0 && u.Scan == 0 {
			if err := tx.Prepared(retentionBridgeCursor).QueryRowContext(context.Background(), u.Rule, e.Channel).Scan(&u.Scan); err != nil {
				return err
			}
		}
	}
	for i := range records {
		r := &records[i]
		for i := range e.Consumers {
			u := &e.Consumers[i]
			if u.Rule != 0 && r.bridge != 0 {
				continue
			}
			pending := slices.Contains(u.Pending, r.offset)
			if (r.offset < u.Scan && !pending) || slices.Contains(u.Excluded, r.offset) {
				continue
			}
			if !pending && (u.Recovering || r.offset < u.MarkersBefore) {
				var held bool
				statement, args := retentionHasDelivery, []any{u.Session, e.Channel, r.offset, u.Recovering}
				if u.Rule != 0 {
					statement, args = retentionBridgeDelivery, []any{u.Rule, e.Channel, r.offset}
				}
				if err := tx.Prepared(statement).QueryRowContext(context.Background(), args...).Scan(&held); err != nil {
					return err
				}
				if held {
					continue
				}
			}
			match := e.match(r, r.publisher == u.Client)
			for j := range u.Filters {
				sub := &u.Filters[j]
				if match.Eligible(sub, false) && (sub.ReadAccess != acl.Partial || u.Access.Allows(acl.Read, u.Subject, string(r.topic))) {
					u.Missed++
					e.Missed++
					break
				}
			}
		}
	}
	if err := e.awayLosses(tx, records); err != nil {
		return err
	}
	return e.sharedLosses(tx, records)
}

// match supplies the channel's start-boundary rule to the common eligibility
// check for connected consumers, away sessions and shared groups alike.
func (e *Retention) match(r *retentionRecord, local bool) SubscriptionMatch {
	m := SubscriptionMatch{Topic: r.topic, Offset: r.offset, Channel: e.Channel, Local: local}
	if e.Latest {
		m.Type = Latest
	}
	return m
}

// sharedLossOffsets puts returned work back among the group's owed records.
// Stored exchanges and core-named hand-offs are excluded by the same bounded
// retention snapshot used for ordinary consumers.
func (e *Retention) sharedLossOffsets(tx *Tx, u *RetentionConsumer, records []retentionRecord) error {
	read := func(statement Statement, visit func(int64)) error {
		after := records[0].offset - 1
		for {
			rows, err := tx.Prepared(statement).QueryContext(context.Background(), u.Share, e.Channel, after, e.Last)
			if err != nil {
				return err
			}
			n := 0
			for rows.Next() {
				if err := rows.Scan(&after); err != nil {
					_ = rows.Close()
					return err
				}
				visit(after)
				n++
			}
			err = finishRows(rows)
			if err != nil || n < MetadataPage {
				return err
			}
		}
	}
	if err := read(retentionSharedDeliveries, func(offset int64) {
		if !slices.Contains(u.Pending, offset) {
			u.Excluded = append(u.Excluded, offset)
		}
	}); err != nil {
		return err
	}
	return read(retentionSharedReturned, func(offset int64) { u.Pending = append(u.Pending, offset) })
}

// sharedLosses streams groups, counting one owed copy per away group. Each
// group uses the same eligibility and examined/handed-off boundaries as a
// connected consumer; returned keys can precede its saved cursor. Once its
// losses are known, cleanup probes only the passed records' complete keys.
func (e *Retention) sharedLosses(tx *Tx, records []retentionRecord) error {
	if len(records) == 0 {
		return nil
	}
	named := make(map[int64]bool)
	for i := range e.Consumers {
		u := &e.Consumers[i]
		if u.Share != 0 {
			named[u.Share] = true
		}
	}
	var keys [retentionPage / MetadataPage][MetadataPage + 2]any
	for i := range records {
		keys[i/MetadataPage][i%MetadataPage+2] = records[i].offset
	}
	var after int64
	for {
		rows, err := tx.Prepared(retentionSharedGroups).QueryContext(context.Background(), e.Channel, e.Last, after, records[0].offset)
		if err != nil {
			return err
		}
		var page [MetadataPage]struct {
			id, position int64
			filter       string
		}
		n := 0
		for rows.Next() {
			g := &page[n]
			if err := rows.Scan(&g.id, &g.position, &g.filter); err != nil {
				_ = rows.Close()
				return err
			}
			after = g.id
			n++
		}
		err = finishRows(rows)
		if err != nil {
			return err
		}
		for _, g := range page[:n] {
			if !named[g.id] {
				_, part, err := topic.SplitShared(g.filter)
				if err != nil {
					return err
				}
				u := RetentionConsumer{Share: g.id, Scan: g.position,
					Filters: []Subscription{{Share: g.id, Filter: g.filter, MatchFilter: part}}}
				if err := e.sharedLossOffsets(tx, &u, records); err != nil {
					return err
				}
				for i := range records {
					record := &records[i]
					if (record.offset < u.Scan && !slices.Contains(u.Pending, record.offset)) || slices.Contains(u.Excluded, record.offset) {
						continue
					}
					match := e.match(record, false)
					if match.Eligible(&u.Filters[0], false) {
						e.Missed++
					}
				}
			}
			for first := 0; first < len(records); first += MetadataPage {
				args := &keys[first/MetadataPage]
				args[0], args[1] = g.id, e.Channel
				shape := bits.Len(uint(min(MetadataPage, len(records)-first) - 1))
				statement := retainSharedReturned
				if shape != 0 {
					statement = retainSharedReturnedPages + Statement(shape) - 1
				}
				if _, err := tx.Prepared(statement).ExecContext(context.Background(), args[:2+(1<<shape)]...); err != nil {
					return err
				}
			}
		}
		if n < MetadataPage {
			return nil
		}
	}
}

// awayLosses pages the stored filters in complete session/filter order. An
// exchange already handed off is excluded here and a missing recovered record
// is counted separately at resume. Named connected sessions use only the core
// snapshot, including progress whose save has not committed.
func (e *Retention) awayLosses(tx *Tx, records []retentionRecord) error {
	if len(records) == 0 {
		return nil
	}
	var session, previous int64
	var excluded [retentionPage]bool // held or already counted for this session
	remaining := 0
	var filter string
	for {
		rows, err := tx.Prepared(retentionSubscriptions).QueryContext(context.Background(), e.Channel, e.Last, session, filter)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var client string
			var sub Subscription
			var start sql.NullInt64
			var position int64
			var held bool
			if err := rows.Scan(&session, &client, &sub.Filter, &sub.NoLocal, &sub.Partitions, &start, &position, &held); err != nil {
				_ = rows.Close()
				return err
			}
			n++
			filter = sub.Filter
			if _, named := e.Named[session]; named {
				continue
			}
			if session != previous {
				clear(excluded[:])
				previous = session
				remaining = len(records)
				if held {
					n, err := e.excludeDeliveries(tx, session, records, excluded[:])
					if err != nil {
						_ = rows.Close()
						return err
					}
					remaining -= n
				}
			}
			if remaining == 0 {
				continue
			}
			if start.Valid {
				sub.Starts = []SubscriptionStart{{Channel: e.Channel, Start: start.Int64}}
			}
			if err := sub.parsePartitions(); err != nil {
				_ = rows.Close()
				return err
			}
			for i := range records {
				r := &records[i]
				if excluded[i] {
					continue
				}
				if r.offset < position {
					excluded[i] = true
					remaining--
					continue
				}
				match := e.match(r, client == r.publisher)
				if match.Eligible(&sub, false) {
					e.Missed++
					excluded[i] = true
					remaining--
				}
			}
		}
		err = finishRows(rows)
		if err != nil || n < MetadataPage {
			return err
		}
	}
}

// excludeDeliveries streams distinct held offsets for one away session. Its
// bitmap is bounded by this deletion's records, even when offsets have gaps or
// the session's window spans many metadata pages. It survives filter pages.
func (e *Retention) excludeDeliveries(tx *Tx, session int64, records []retentionRecord, excluded []bool) (int, error) {
	after := records[0].offset - 1
	held := 0
	for {
		rows, err := tx.Prepared(retentionDeliveries).QueryContext(context.Background(), session, e.Channel, after, e.Last)
		if err != nil {
			return 0, err
		}
		n := 0
		for rows.Next() {
			if err := rows.Scan(&after); err != nil {
				_ = rows.Close()
				return 0, err
			}
			n++
			if i, found := slices.BinarySearchFunc(records, after, func(r retentionRecord, offset int64) int {
				return cmp.Compare(r.offset, offset)
			}); found {
				excluded[i] = true
				held++
			}
		}
		err = finishRows(rows)
		if err != nil || n < MetadataPage {
			return held, err
		}
	}
}

// RetentionDeadline reads retained-value deadlines for the shared pass.
// Log deadlines are returned by the bounded record walk itself.
// Overdue work on refused channels belongs to the one-second full-store retry.
type RetentionDeadline struct {
	At, Period int64
	Refused    map[int64]bool
	Next       sql.NullInt64
}

// ReadConn reads scalar aggregates, never a catalogue of retained metadata.
func (r *RetentionDeadline) ReadConn(conn *Conn) error {
	var after int64
	if r.Refused[1] {
		after = r.At
	}
	period := timestamp.AfterMilli(0, r.Period)
	return conn.Prepared(nextRetainedDeadline).QueryRowContext(context.Background(),
		after, period, r.Period, period, after).Scan(&r.Next)
}

// readChannels extends the channel-byte refresh with stored positions. Every
// bounded page uses the refresh's one view; the core holds no result pages.
func (r *MetricRefresh) readChannels(conn *Conn) error {
	var after int64
	index := 0
	for {
		rows, err := conn.Prepared(readChannelBytes).QueryContext(context.Background(), after)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var id, size int64
			if err := rows.Scan(&id, &size); err != nil {
				_ = rows.Close()
				return err
			}
			n++
			after = id
			for index < len(r.Channels) && r.Channels[index].Channel < id {
				index++
			}
			if index < len(r.Channels) && r.Channels[index].Channel == id {
				r.Channels[index].Bytes.Set(size)
			}
		}
		err = finishRows(rows)
		if err != nil {
			return err
		}
		if n < MetadataPage {
			break
		}
	}
	// Positions are keyed by session, not channel. Walk that index once,
	// retaining only one count and minimum per configured channel, instead
	// of rescanning the entire positions table for each channel.
	counts := make([]struct{ count, lowest int64 }, len(r.Channels))
	if err := walkMetricPositions(conn, func(channel, offset int64) {
		if i, found := slices.BinarySearchFunc(r.Channels, channel, func(p RetentionPolicy, id int64) int {
			return cmp.Compare(p.Channel, id)
		}); found {
			p := &counts[i]
			p.count++
			if p.lowest == 0 || offset < p.lowest {
				p.lowest = offset
			}
		}
	}); err != nil {
		return err
	}
	for i := range counts {
		p, gauges := &counts[i], &r.Channels[i]
		gauges.Consumers.Set(p.count)
		var lowest *int64
		if p.count != 0 {
			lowest = &p.lowest
		}
		gauges.PositionMin.Set(lowest)
	}
	return nil
}
