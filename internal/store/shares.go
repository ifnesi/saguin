package store

import (
	"context"
	"database/sql"
	"errors"
)

// SharedGroup is the metadata loaded when a group gains a connected member.
// Away members and returned records stay in SQLite; only cursor scalars leave
// the read. The core supplies the identity of a group not yet stored.
type SharedGroup struct {
	ID, Members  int64
	Filter, Last string
	Cursors      []SubscriptionStart
}

// LoadSharedGroup fixes the group's existing state before a subscription
// changes its membership. It never reads a returned record or a payload.
type LoadSharedGroup struct {
	SharedGroup
	Found bool
}

// ReadConn loads the group's identity, durable membership count and cursors.
func (l *LoadSharedGroup) ReadConn(conn *Conn) error {
	var last sql.NullString
	err := conn.Prepared(loadSharedGroup).QueryRowContext(context.Background(), l.Filter).Scan(&l.ID, &last, &l.Members)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	l.Found, l.Last = true, last.String
	rows, err := conn.Prepared(loadSharedCursors).QueryContext(context.Background(), l.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cursor SubscriptionStart
		if err := rows.Scan(&cursor.Channel, &cursor.Start); err != nil {
			return err
		}
		l.Cursors = append(l.Cursors, cursor)
	}
	return finishRows(rows)
}

// SharedRecord names returned work. Ordering is by channel, then offset.
type SharedRecord struct{ Channel, Offset int64 }

// Shared evidence belongs to the group, independently of packet identifiers.
const (
	SharedReturned byte = iota + 1
	SharedAssigned
	SharedTerminal
)

// SharedEvidence records an assignment, return or terminal disposition.
// A returned durable assignment also names the exchange to delete atomically;
// only an assignment stores its session and delivery QoS in share_returned.
type SharedEvidence struct {
	SharedRecord
	Session    int64
	PacketID   uint16
	State, QoS byte
}

// ReadSharedReturned pages only keys. Selected records use the ordinary
// metadata and payload reads and the member's existing delivery window.
type ReadSharedReturned struct {
	Share int64
	After SharedRecord
	Limit int
	Rows  []SharedRecord
}

// ReadConn reads the next bounded page of returned record keys.
func (r *ReadSharedReturned) ReadConn(conn *Conn) error {
	rows, err := conn.Prepared(readSharedReturned).QueryContext(context.Background(), r.Share, r.After.Channel, r.After.Offset, r.Limit)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row SharedRecord
		if err := rows.Scan(&row.Channel, &row.Offset); err != nil {
			return err
		}
		r.Rows = append(r.Rows, row)
	}
	return finishRows(rows)
}

// SharedChange is one bounded group transition. Its caller commits it in the
// same transaction as the subscription, exchange or session change it names.
type SharedChange struct {
	ID                 int64
	Filter             string
	New, End, Volatile bool
	Last               *string
	Cursors            []SubscriptionStart
	Return, Settle     []SharedRecord
	Evidence           []SharedEvidence
	Prune              []SubscriptionStart // live progress also reclaims terminal rows of volatile-only groups
}

// Records returns zero: a group transition contains only metadata.
func (*SharedChange) Records() int { return 0 }

// WriteTx applies the group transition in its enclosing transaction.
func (s *SharedChange) WriteTx(tx *Tx) error {
	if s.New {
		if _, err := tx.Prepared(addSharedGroup).ExecContext(context.Background(), s.ID, s.Filter); err != nil {
			return err
		}
	}
	if s.Last != nil {
		if _, err := tx.Prepared(selectSharedMember).ExecContext(context.Background(), *s.Last, s.ID); err != nil {
			return err
		}
	}
	for _, cursor := range s.Cursors {
		if err := (&AdvanceChannel{ID: cursor.Channel, Next: cursor.Start}).WriteTx(tx); err != nil {
			return err
		}
		if _, err := tx.Prepared(saveSharedCursor).ExecContext(context.Background(), s.ID, cursor.Channel, cursor.Start); err != nil {
			return err
		}
	}
	for _, row := range s.Return {
		if _, err := tx.Prepared(returnSharedRecord).ExecContext(context.Background(), s.ID, row.Channel, row.Offset); err != nil {
			return err
		}
	}
	for _, row := range s.Settle {
		if _, err := tx.Prepared(settleSharedRecord).ExecContext(context.Background(), s.ID, row.Channel, row.Offset); err != nil {
			return err
		}
	}
	for _, row := range s.Evidence {
		if row.State == SharedReturned {
			if row.PacketID != 0 {
				change := DeliveryChanges{Session: row.Session, Delete: []uint16{row.PacketID}}
				if err := change.WriteTx(tx); err != nil {
					return err
				}
			}
			if _, err := tx.Prepared(returnSharedRecord).ExecContext(context.Background(), s.ID, row.Channel, row.Offset); err != nil {
				return err
			}
			continue
		}
		var session, qos any
		if row.State == SharedAssigned {
			session, qos = row.Session, int64(row.QoS)
		}
		if _, err := tx.Prepared(saveSharedEvidence).ExecContext(context.Background(), s.ID, row.Channel, row.Offset, int64(row.State), session, qos); err != nil {
			return err
		}
	}
	for _, cursor := range s.Cursors {
		if _, err := tx.Prepared(pruneSharedEvidence).ExecContext(context.Background(), s.ID, cursor.Channel, cursor.Start); err != nil {
			return err
		}
	}
	for _, cursor := range s.Prune {
		if _, err := tx.Prepared(pruneSharedEvidence).ExecContext(context.Background(), s.ID, cursor.Channel, cursor.Start); err != nil {
			return err
		}
	}
	if s.End || s.Volatile {
		if _, err := tx.Prepared(endSharedCursors).ExecContext(context.Background(), s.ID); err != nil {
			return err
		}
	}
	if s.End {
		// Membership ends; an already-started exchange keeps its provenance
		// and a terminal disposition, never work for a later membership.
		if _, err := tx.Prepared(terminalGroupShares).ExecContext(context.Background(), s.ID); err != nil {
			return err
		}
		if _, err := tx.Prepared(selectSharedMember).ExecContext(context.Background(), nil, s.ID); err != nil {
			return err
		}
		if _, err := tx.Prepared(endSharedReturned).ExecContext(context.Background(), s.ID); err != nil {
			return err
		}
		if _, err := tx.Prepared(endSharedGroup).ExecContext(context.Background(), s.ID); err != nil {
			return err
		}
	}
	return nil
}

// LoadEndingShares identifies the groups reserved with a session ending.
// It reads keys in bounded pages, without loading exchanges or returned work.
type LoadEndingShares struct {
	Session int64
	Groups  []SharedGroup
}

// ReadConn discovers group keys without loading their deliveries or payloads.
func (l *LoadEndingShares) ReadConn(conn *Conn) error {
	var after int64
	for {
		rows, err := conn.Prepared(loadEndingShares).QueryContext(context.Background(), l.Session, after)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var g SharedGroup
			if err := rows.Scan(&g.ID, &g.Filter); err != nil {
				_ = rows.Close()
				return err
			}
			l.Groups = append(l.Groups, g)
			after, n = g.ID, n+1
		}
		err = finishRows(rows)
		if err != nil || n < MetadataPage {
			return err
		}
	}
}

// SharedEnding carries only the ending's group keys and live-owner exceptions.
// Counts are aggregate results, applied once after the transaction commits.
type SharedEnding struct {
	Progress []*SharedChange
	Members  map[int64]int64
	Groups   []SharedGroup
	Live     map[int64]bool
	Return   []SharedReturn
	Cancel   []uint16
	Dropped  map[int64]uint64
}

// SharedReturn names a shared delivery returned with its cancelling transition.
type SharedReturn struct {
	Share int64
	SharedRecord
}

// discardOrphanShares reclaims one bounded page of connection-lifetime group
// state at startup. The ordinary due-session sweep rediscovers refusals.
type discardOrphanShares struct{ more bool }

func (*discardOrphanShares) Records() int { return 0 }

func (d *discardOrphanShares) WriteTx(tx *Tx) error {
	rows, err := tx.Prepared(orphanSharedGroups).QueryContext(context.Background())
	if err != nil {
		return err
	}
	var ids [MetadataPage]int64
	n := 0
	for rows.Next() {
		if err := rows.Scan(&ids[n]); err != nil {
			_ = rows.Close()
			return err
		}
		n++
	}
	err = finishRows(rows)
	if err != nil {
		return err
	}
	for _, id := range ids[:n] {
		if err := (&SharedChange{ID: id, End: true}).WriteTx(tx); err != nil {
			return err
		}
	}
	d.more = n == MetadataPage
	return nil
}

func (e *EndSession) returnShares(tx *Tx) error {
	if e.Shared == nil || e.KeepHeld || e.KeepDeliveries {
		return nil
	}
	for _, id := range e.Shared.Cancel {
		if _, err := tx.Prepared(settleSessionDelivery).ExecContext(context.Background(), e.ID, id); err != nil {
			return err
		}
	}
	// A known never-handed preparation is returned at either QoS. Convert
	// it before counting unfinished QoS 2 assignments as session-end drops.
	for _, row := range e.Shared.Return {
		if _, err := tx.Prepared(returnSharedRecord).ExecContext(context.Background(), row.Share, row.Channel, row.Offset); err != nil {
			return err
		}
	}
	rows, err := tx.Prepared(countEndingShares).QueryContext(context.Background(), e.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var channel int64
		var count uint64
		if err := rows.Scan(&channel, &count); err != nil {
			_ = rows.Close()
			return err
		}
		if e.Shared.Dropped == nil {
			e.Shared.Dropped = make(map[int64]uint64)
		}
		e.Shared.Dropped[channel] += count
	}
	err = finishRows(rows)
	if err != nil {
		return err
	}
	if _, err := tx.Prepared(returnSessionShares).ExecContext(context.Background(), e.ID); err != nil {
		return err
	}
	for _, statement := range [...]Statement{terminalSessionShares, returnVolatileShares} {
		if _, err := tx.Prepared(statement).ExecContext(context.Background(), e.ID); err != nil {
			return err
		}
	}
	return nil
}

// recoverSharedAssignments ends a bounded page of volatile assignments after
// a crash. Packet identifiers never leave their former connection's memory.
type recoverSharedAssignments struct {
	more    bool
	after   SharedReturn
	dropped map[int64]uint64
}

func (*recoverSharedAssignments) Records() int { return 0 }

func (r *recoverSharedAssignments) WriteTx(tx *Tx) error {
	rows, err := tx.Prepared(readVolatileShares).QueryContext(context.Background(), r.after.Share, r.after.Channel, r.after.Offset)
	if err != nil {
		return err
	}
	var page [MetadataPage]struct {
		share    int64
		evidence SharedEvidence
		present  bool
		terminal bool  // a group ended; this backing row retains only MQTT provenance
		session  int64 // inherited backing row, whose owner ended at restart
		packetID uint16
	}
	n := 0
	for rows.Next() {
		p := &page[n]
		if err := rows.Scan(&p.share, &p.evidence.Channel, &p.evidence.Offset, &p.evidence.QoS, &p.present, &p.session, &p.packetID, &p.terminal); err != nil {
			_ = rows.Close()
			return err
		}
		n++
	}
	err = finishRows(rows)
	if err != nil {
		return err
	}
	for i := range n {
		p := &page[i]
		p.evidence.State = SharedTerminal
		if !p.terminal && p.evidence.QoS == 1 && p.present {
			p.evidence.State = SharedReturned
		}
		if !p.terminal && p.evidence.QoS == 2 {
			if r.dropped == nil {
				r.dropped = make(map[int64]uint64)
			}
			r.dropped[p.evidence.Channel]++
		}
		if err := (&SharedChange{ID: p.share, Evidence: []SharedEvidence{p.evidence}}).WriteTx(tx); err != nil {
			return err
		}
		if p.session != 0 {
			if _, err := tx.Prepared(settleSessionDelivery).ExecContext(context.Background(), p.session, p.packetID); err != nil {
				return err
			}
		}
		if _, err := tx.Prepared(pruneStoredSharedEvidence).ExecContext(context.Background(), p.share, p.evidence.Channel); err != nil {
			return err
		}
		r.after = SharedReturn{Share: p.share, SharedRecord: p.evidence.SharedRecord}
	}
	r.more = n == MetadataPage
	return nil
}

func (e *EndSession) endShares(tx *Tx) error {
	if e.Shared == nil {
		return nil
	}
	for _, g := range e.Shared.Groups {
		if _, err := tx.Prepared(pruneEndedSharedEvidence).ExecContext(context.Background(), g.ID); err != nil {
			return err
		}
		var members int64
		err := tx.Prepared(cleanupSharedGroup).QueryRowContext(context.Background(), g.ID).Scan(&members)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if e.Shared.Members == nil {
			e.Shared.Members = make(map[int64]int64)
		}
		e.Shared.Members[g.ID] = members
		if members == 0 {
			if err := (&SharedChange{ID: g.ID, Volatile: true, End: !e.Shared.Live[g.ID]}).WriteTx(tx); err != nil {
				return err
			}
		} else {
			for _, progress := range e.Shared.Progress {
				if progress.ID == g.ID {
					if err := progress.WriteTx(tx); err != nil {
						return err
					}
					break
				}
			}
		}
	}
	return nil
}
