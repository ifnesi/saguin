package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"slices"
)

// ReadSessionDeliveries reads only the next bounded resend page. Unoffered
// exchanges stay in the store; record payloads require separate reservations.
type ReadSessionDeliveries struct {
	Bridge                  int64 // nonzero selects the bridge exchange owner
	Session, After, Through int64
	ReleasesOnly            bool  // look past PUBLISHes blocked by the new connection's quota
	PublishesOnly           bool  // PUBLISH capacity remains when control capacity is spent
	ReleasedThrough         int64 // control cursor; the answer advances it through empty sequence ranges
	Limit                   int
	Rows                    []RecoveredDelivery
}

// RecoveredDelivery combines an exchange's first-delivery state with the
// original record's metadata, without loading its payload.
type RecoveredDelivery struct {
	SessionDelivery
	RuleFilter, RuleTopic string // stored template, also available after a rule's cursor is removed
	Record                RecordMetadata
	Missing               bool // retention deleted the record; the exchange still must settle
}

// discardOrphanDeliveries ends connection-lifetime exchanges left by a crash.
// A continuing session can retain inherited rows without a durable session row
// while it pages them; they survive only for that connection's lifetime.
type discardOrphanDeliveries struct{}

func (*discardOrphanDeliveries) Records() int { return 0 }

func (*discardOrphanDeliveries) WriteTx(tx *Tx) error {
	_, err := tx.Prepared(deleteOrphanDeliveries).ExecContext(context.Background())
	return err
}

// ReadConn offers outstanding exchanges in their original sending order.
func (r *ReadSessionDeliveries) ReadConn(conn *Conn) error {
	statement, owner := readSessionDeliveries, r.Session
	if r.Bridge != 0 {
		statement, owner = readBridgeDeliveries, r.Bridge
	}
	rows, err := conn.Prepared(statement).QueryContext(context.Background(), owner, r.Through,
		r.After, r.ReleasesOnly, r.ReleasedThrough, r.PublishesOnly, r.Limit)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	r.Rows = make([]RecoveredDelivery, r.Limit)
	n := 0
	for rows.Next() {
		d := &r.Rows[n]
		m := &d.Record
		var moves bool
		var attempt sql.NullString
		var number, began sql.NullInt64
		fields := [...]any{&d.PacketID, &d.Seq, &d.Channel, &d.Offset, &d.QoS, &d.Released, &moves, &d.Share, &attempt, &number, &began, &d.Retain, &d.Initial, &d.Properties,
			&d.Missing, &m.Topic, &m.Properties, &m.Expires, &m.PayloadSize, &m.StoredPayloadSize, &d.Rule, &m.ID, &d.RuleFilter, &d.RuleTopic, &m.QoS}
		columns := len(fields) - 5
		values := fields[:columns]
		if r.Bridge != 0 {
			values = fields[:]
		}
		if err := rows.Scan(values...); err != nil {
			return err
		}
		if attempt.Valid {
			d.Attempt = &DeliveryAttempt{ID: attempt.String, Number: number.Int64, Began: began.Int64}
		}
		m.Offset = d.Offset
		d.NoMove = !moves
		n++
	}
	r.Rows = r.Rows[:n]
	if err := finishRows(rows); err != nil {
		return err
	}
	after := r.ReleasedThrough
	if !r.PublishesOnly {
		through := r.Through
		if n != 0 && n == r.Limit {
			through = r.Rows[n-1].Seq
		}
		after = max(after, through)
	}
	if after >= r.Through {
		r.ReleasedThrough = r.Through
		return nil
	}
	// One scalar probe, in this same ordered view, locates the next control
	// not admitted. A PUBLISH before it need not wait for a prior PUBCOMP;
	// a PUBLISH beyond it cannot overtake that unread control.
	var next int64
	statement = nextSessionRelease
	if r.Bridge != 0 {
		statement = nextBridgeRelease
	}
	if err := conn.Prepared(statement).QueryRowContext(context.Background(), owner, after, r.Through).Scan(&next); err != nil {
		return err
	}
	r.ReleasedThrough = r.Through
	if next != 0 {
		r.ReleasedThrough = next - 1
	}
	return nil
}

// SessionDelivery is an outgoing QoS 1 or 2 exchange. Delivery properties are
// broker additions only; record properties are read from the original record.
// Alias and expiry belong to the current connection and transmission.
type SessionDelivery struct {
	Reset       bool  // replace a forgotten peer exchange with a new bridge preparation
	Rule        int64 // bridge rule owning this exchange, zero for a client
	Attempt     *DeliveryAttempt
	Returned    bool // returned key reclaimed with this newly stored exchange
	PacketID    uint16
	Seq, Offset int64
	Channel     int64
	Share       int64 // independent shared cursor; never moves a session position
	QoS         byte
	Released    bool // QoS 2 PUBREC received; only PUBREL remains to be sent
	Retain      bool
	Initial     bool // the first delivery belonged to a subscription's initial pass
	NoMove      bool
	Properties  []byte
}

// DeliveryAttempt preserves the identity and metadata of a broker worker offer.
type DeliveryAttempt struct {
	ID            string
	Number, Began int64
}

// DeliveryChanges keeps a consumer's new exchanges, received PUBREC phases,
// settled exchanges and latest progress in one transaction. An acknowledged
// row is deleted only after progress passes its record; an inherited exchange
// of an expiry-zero session instead becomes a settled marker ahead of progress.
// A cancelled row goes with progress bounded before it or with the subscriptions
// that owed it. Saving progress supersedes movement by earlier stored exchanges,
// without settling those exchanges themselves.
// The core deletes only previously committed exchanges and reserves their
// identifiers through the settlement's successful answer. Save and Delete thus
// never name the same key in a transaction, including cancelled preparations.
type DeliveryChanges struct {
	Session, Channel int64
	Position, Seen   int64             // zero Position leaves progress unchanged
	Rewind           bool              // departure cancels known never-sent preparations at Position
	Restore          []uint16          // surviving live exchanges; a rewind restores movement at/after Position
	Save             []SessionDelivery // Released updates an existing row; other entries prepare PUBLISH
	Delete           []uint16
	Markers          *DeliveryMarkers // optional persistence context: owner identity and terminal evidence
}

// DeliveryMarkers holds an optional bridge cursor identity or client terminal
// evidence. Inherited settled markers protect an expiry-zero session's scan;
// shared terminals belong to their group and survive the member's session.
type DeliveryMarkers struct {
	Bridge   *BridgeDeliveryOwner // independent bridge cursor identity; other marker fields stay empty
	Return   []SharedReturn       // cancelled shared preparations, committed with deletion
	IDs      []uint16
	Shared   []uint16 // group-owned terminal evidence, committed with exchange deletion
	Retired  []uint16 // terminal evidence of an ended group, reclaimed with its MQTT exchange
	Position int64
	Groups   []SharedGroup // result: exact group keys touched by shared settlements
}

// Records returns zero: delivery state contains no new channel records.
func (*DeliveryChanges) Records() int { return 0 }

// WriteTx saves exactly the bounded changes selected by the core.
func (d *DeliveryChanges) WriteTx(tx *Tx) error {
	return tx.writeDeliveryChanges([]Write{d})
}

// WriteDeliveryChanges writes the longest independent delivery prefix and
// returns its member count. A repeated session ends the run: its next member
// may supersede an earlier save or rewrite its progress. Any other
// operation is also a boundary, preserving session/subscription/publication
// dependencies without knowing that operation's implementation. Only SQL inside
// this transaction is combined; request ownership and result order do not change.
func (t *Tx) WriteDeliveryChanges(writes []Write) (int, error) {
	if t.deliverySessions == nil {
		// The writer reuses Tx for life. Reserve the bounded membership table
		// once, not an object for each row or each transaction.
		t.deliverySessions = make(map[int64]struct{}, Credit)
	}
	defer clear(t.deliverySessions)
	n := 0
	bridge := writes[0].(*DeliveryChanges).bridgeOwner() != nil
	for _, w := range writes {
		d, ok := w.(*DeliveryChanges)
		if !ok || n == Credit || (d.bridgeOwner() != nil) != bridge {
			break
		}
		key := d.Session
		if bridge {
			key = d.bridgeOwner().Rule
		}
		if _, found := t.deliverySessions[key]; found {
			break
		}
		t.deliverySessions[key] = struct{}{}
		n++
	}
	return n, t.writeDeliveryChanges(writes[:n])
}

// writeDeliveryChanges combines rows across distinct sessions, visiting each
// Save/Delete list once. Reclaim settled rows before any insert, so a later
// member's allocations cannot overtake an earlier member's reclamation at full
// storage. The core selects Delete from committed exchanges (recordSettlement
// and cancelPreparation), separately from pending Save rows; identifiers remain
// reserved until savedDeliveryChanges applies a successful commit. Deletion SQL
// uses only those keys, never the position or moves. Moving a deletion before
// its position/restore/supersede therefore only omits updates to a disappearing
// row: progress and surviving exchanges still commit together or all roll back.
// Repeated sessions and other operation types remain member-order boundaries.
func (t *Tx) writeDeliveryChanges(writes []Write) error {
	for _, w := range writes {
		d := w.(*DeliveryChanges)
		markers := d.Markers
		if markers == nil {
			continue
		}
		markers.Groups = markers.Groups[:0]
		// Capture exact group keys before deleting their exchanges. The core
		// checks live membership and schedules their ending on this commit.
		for _, ids := range [2][]uint16{markers.Shared, markers.Retired} {
			for _, id := range ids {
				var g SharedGroup
				err := t.Prepared(settlingSharedGroup).QueryRowContext(context.Background(), d.Session, id).Scan(&g.ID, &g.Filter)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				markers.Groups = append(markers.Groups, g)
			}
		}
		slices.SortFunc(markers.Groups, func(a, b SharedGroup) int { return cmp.Compare(a.ID, b.ID) })
		markers.Groups = slices.CompactFunc(markers.Groups, func(a, b SharedGroup) bool { return a.ID == b.ID })
		for _, row := range markers.Return {
			if err := (&SharedChange{ID: row.Share, Return: []SharedRecord{row.SharedRecord}}).WriteTx(t); err != nil {
				return err
			}
		}
	}
	if err := t.settleDeliveries(writes); err != nil {
		return err
	}
	if err := t.saveDeliveries(writes); err != nil {
		return err
	}
	for _, w := range writes {
		for _, row := range w.(*DeliveryChanges).Save {
			if row.Returned && !row.Released {
				if _, err := t.Prepared(settleSharedRecord).ExecContext(context.Background(), row.Share, row.Channel, row.Offset); err != nil {
					return err
				}
			}
		}
	}
	for _, w := range writes {
		if err := w.(*DeliveryChanges).movePosition(t); err != nil {
			return err
		}
	}
	for _, w := range writes {
		d := w.(*DeliveryChanges)
		if d.bridgeOwner() != nil && len(d.Delete) != 0 {
			if _, err := t.Prepared(pruneBridgeRule).ExecContext(context.Background(), d.bridgeOwner().Rule); err != nil {
				return err
			}
		}
		if d.Position != 0 && d.bridgeOwner() == nil {
			if _, err := t.Prepared(supersedeSessionDeliveries).ExecContext(context.Background(), d.Session, d.Channel, d.Position); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *DeliveryChanges) movePosition(tx *Tx) error {
	if d.bridgeOwner() == nil && d.Markers != nil && d.Markers.Position != 0 {
		if _, err := tx.Prepared(pruneSessionMarkers).ExecContext(context.Background(), d.Session, d.Channel, d.Markers.Position); err != nil {
			return err
		}
	}
	if d.Position != 0 {
		statement := moveSessionPosition
		args := []any{d.Position, d.Seen, d.Session, d.Channel, d.Position}
		if d.bridgeOwner() != nil {
			statement, args[2] = moveBridgeCursor, d.bridgeOwner().Rule
		}
		if d.Rewind {
			statement, args = rewindSessionPosition, args[:4]
			if d.bridgeOwner() != nil {
				statement = rewindBridgeCursor
			}
		}
		if _, err := tx.Prepared(statement).ExecContext(context.Background(), args...); err != nil {
			return err
		}
		if d.Rewind && d.bridgeOwner() == nil {
			// Restore only live rows named by the core. Retained-pass rows also
			// have moves=0, but must never become live recovery protections.
			for _, id := range d.Restore {
				if _, err := tx.Prepared(restoreSessionDelivery).ExecContext(context.Background(), d.Session, d.Channel, id, d.Position); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

const (
	deliveryChunk   = 16
	deliveryColumns = 14
	settleColumns   = 2
)

// saveDeliveries uses the same full-chunk/single-remainder pattern as records
// for preparations; a received PUBREC changes only its existing row's phase.
// A new row below this member's progress already has its final moves=0. Restore
// only affects offsets at/after that progress, so the two predicates are disjoint.
func (t *Tx) saveDeliveries(writes []Write) error {
	columns, add, chunk, release, releaseChunk := deliveryColumns, addSessionDelivery, addDeliveryChunk, releaseSessionDelivery, releaseDeliveryChunk
	if writes[0].(*DeliveryChanges).bridgeOwner() != nil {
		columns, add, chunk, release, releaseChunk = 7, addBridgeDelivery, addBridgeDeliveryChunk, releaseBridgeDelivery, releaseBridgeDeliveryChunk
	}
	args := t.recordArgs[:0]
	keys := t.deliveryKeys[:0]
	defer clear(t.recordArgs[:deliveryChunk*deliveryColumns])
	defer clear(t.deliveryKeys[:])
	for _, w := range writes {
		d := w.(*DeliveryChanges)
		for _, row := range d.Save {
			if row.Reset {
				if _, err := t.Prepared(settleBridgeDelivery).ExecContext(context.Background(), d.bridgeOwner().ID, row.PacketID); err != nil {
					return err
				}
			}
			if row.Released {
				keys = append(keys, d.owner(), int64(row.PacketID))
				if len(keys) == len(t.deliveryKeys) {
					if err := t.writeDeliveryKeys(keys, release, releaseChunk); err != nil {
						return err
					}
					keys = keys[:0]
				}
				continue
			}
			var attempt sql.NullString
			var number, began sql.NullInt64
			if a := row.Attempt; a != nil {
				attempt = sql.NullString{String: a.ID, Valid: true}
				number = sql.NullInt64{Int64: a.Number, Valid: true}
				began = sql.NullInt64{Int64: a.Began, Valid: true}
			}
			moves := !row.NoMove && (d.Position == 0 || row.Offset >= d.Position)
			args = append(args, d.owner(), int64(row.PacketID), row.Seq, d.Channel, row.Offset, int64(row.QoS))
			if d.bridgeOwner() != nil {
				args = append(args, d.bridgeOwner().Rule)
			} else {
				args = append(args, moves, row.Share, attempt, number, began, row.Retain, row.Initial, row.Properties)
			}
			if len(args) == deliveryChunk*columns {
				if _, err := t.Prepared(chunk).ExecContext(context.Background(), args...); err != nil {
					return err
				}
				args = args[:0]
			}
		}
	}
	for len(args) != 0 {
		if _, err := t.Prepared(add).ExecContext(context.Background(), args[:columns]...); err != nil {
			return err
		}
		args = args[columns:]
	}
	return t.writeDeliveryKeys(keys, release, releaseChunk)
}

// settleDeliveries commits shared terminal evidence before deleting exchanges,
// and batches complete keys for deletions and inherited scan markers. A reused
// identifier cannot affect either kind of marker.
func (t *Tx) settleDeliveries(writes []Write) error {
	args := t.deliveryKeys[:0]
	defer clear(t.deliveryKeys[:])
	statements := []struct{ single, chunk Statement }{
		{retireSharedDelivery, retireSharedChunk},
		{terminalSharedDelivery, terminalSharedChunk},
		{settleSessionDelivery, settleDeliveryChunk},
		{markSessionDelivery, markDeliveryChunk},
	}
	if writes[0].(*DeliveryChanges).bridgeOwner() != nil {
		statements = []struct{ single, chunk Statement }{{settleBridgeDelivery, settleBridgeDeliveryChunk}}
	}
	for _, statements := range statements {
		for _, w := range writes {
			d := w.(*DeliveryChanges)
			ids := d.Delete
			if statements.single == retireSharedDelivery || statements.single == terminalSharedDelivery {
				if d.Markers == nil {
					continue
				}
				ids = d.Markers.Shared
				if statements.single == retireSharedDelivery {
					ids = d.Markers.Retired
				}
			}
			if statements.single == markSessionDelivery {
				if d.Markers == nil {
					continue
				}
				ids = d.Markers.IDs
			}
			for _, id := range ids {
				args = append(args, d.owner(), int64(id))
				if len(args) == deliveryChunk*settleColumns {
					if err := t.writeDeliveryKeys(args, statements.single, statements.chunk); err != nil {
						return err
					}
					args = args[:0]
				}
			}
		}
		if err := t.writeDeliveryKeys(args, statements.single, statements.chunk); err != nil {
			return err
		}
		args = args[:0]
	}
	return nil
}

// writeDeliveryKeys shares full-chunk and single-remainder execution without
// copying keys or revisiting either member's Save/Delete list.
func (t *Tx) writeDeliveryKeys(args []any, single, chunk Statement) error {
	if len(args) == deliveryChunk*settleColumns {
		_, err := t.Prepared(chunk).ExecContext(context.Background(), args...)
		return err
	}
	for len(args) != 0 {
		if _, err := t.Prepared(single).ExecContext(context.Background(), args[:settleColumns]...); err != nil {
			return err
		}
		args = args[settleColumns:]
	}
	return nil
}

// owner selects independent identifier spaces without changing the common
// batching and settlement implementation.
func (d *DeliveryChanges) owner() int64 {
	if d.bridgeOwner() != nil {
		return d.bridgeOwner().ID
	}
	return d.Session
}

// BridgeDeliveryOwner supplies the durable bridge and rule identities to the
// shared exchange writer. Client requests need no additional owner allocation.
type BridgeDeliveryOwner struct{ ID, Rule int64 }

// bridgeOwner selects the optional bridge cursor identity. Client marker and
// shared-group evidence use the same persistence context without an identity.
func (d *DeliveryChanges) bridgeOwner() *BridgeDeliveryOwner {
	if d.Markers != nil {
		return d.Markers.Bridge
	}
	return nil
}
