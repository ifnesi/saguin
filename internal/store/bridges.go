package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ifnesi/saguin/internal/config"
)

// Bridge is a stable stored origin, independent of any particular link.
type Bridge struct {
	ID         int64
	Name       string
	ReplayOwed bool
}

func readBridges(conn *Conn) ([]Bridge, error) {
	rows, err := conn.Prepared(loadBridges).QueryContext(context.Background())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var bridges []Bridge
	for rows.Next() {
		var b Bridge
		if err := rows.Scan(&b.ID, &b.Name, &b.ReplayOwed); err != nil {
			return nil, err
		}
		bridges = append(bridges, b)
	}
	return bridges, finishRows(rows)
}

// AddBridges persists identities allocated by the core before links start.
type AddBridges struct {
	Bridges []Bridge
	Rules   []*BridgeRule
}

// BridgeRule carries startup identity and the first position on each reached
// channel. Results return with the startup commit before any link starts.
type BridgeRule struct {
	ID, Bridge    int64
	Filter, Topic string
	Cursors       []ChannelPosition
}

// Records returns zero for these origin metadata rows.
func (*AddBridges) Records() int { return 0 }

// WriteTx stores every new identity in the startup transaction.
func (a *AddBridges) WriteTx(tx *Tx) error {
	for _, b := range a.Bridges {
		if _, err := tx.Prepared(addBridge).ExecContext(context.Background(), b.ID, b.Name); err != nil {
			return err
		}
	}
	for _, r := range a.Rules {
		if _, err := tx.Prepared(addBridgeRule).ExecContext(context.Background(), r.Bridge, r.Filter, r.Topic); err != nil {
			return err
		}
		if err := tx.Prepared(loadBridgeRule).QueryRowContext(context.Background(), r.Bridge, r.Filter, r.Topic).Scan(&r.ID); err != nil {
			return err
		}
		for _, p := range r.Cursors {
			if _, err := tx.Prepared(addBridgeCursor).ExecContext(context.Background(), r.ID, p.Channel, p.Offset); err != nil {
				return err
			}
			if err := (&AdvanceChannel{ID: p.Channel, Next: p.Offset}).WriteTx(tx); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadBridgeHeld restores only incoming identifiers and their channel owners.
// Its ordered view follows every hold accepted on an earlier link.
type LoadBridgeHeld struct {
	Bridge int64
	Held   []HeldIdentifier
}

// ReadConn reuses session recovery's metadata-only held-publication read.
func (l *LoadBridgeHeld) ReadConn(conn *Conn) (err error) {
	l.Held, err = loadHeld(conn, 0, l.Bridge)
	return err
}

// LoadBridgeOutbound establishes a new link's ordered view after its former
// link's bookkeeping. Only configured rule/channel positions live in memory.
type LoadBridgeOutbound struct {
	Bridge, Sequence int64
	Count            int
	Identifiers      map[uint16]struct{} // every surviving exchange, including unread recovery pages
	Cursors          []BridgeCursor
}

// BridgeCursor is a rule's independent durable consumer position.
type BridgeCursor struct {
	Rule int64
	ChannelPosition
}

// ReadConn loads cursor metadata and the fixed recovery boundary.
func (l *LoadBridgeOutbound) ReadConn(conn *Conn) error {
	if err := conn.Prepared(loadBridgeSequence).QueryRowContext(context.Background(), l.Bridge).Scan(&l.Sequence, &l.Count); err != nil {
		return err
	}
	if l.Count != 0 {
		if err := l.loadIdentifiers(conn); err != nil {
			return err
		}
	}
	rows, err := conn.Prepared(loadBridgeCursors).QueryContext(context.Background(), l.Bridge)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var p BridgeCursor
		if err := rows.Scan(&p.Rule, &p.Channel, &p.Offset, &p.Seen); err != nil {
			return err
		}
		l.Cursors = append(l.Cursors, p)
	}
	return finishRows(rows)
}

// loadIdentifiers reserves unread exchanges too, including removed rules.
// The map transfers to the core with the fixed recovery boundary.
func (l *LoadBridgeOutbound) loadIdentifiers(conn *Conn) error {
	ids, err := conn.Prepared(loadBridgeIdentifier).QueryContext(context.Background(), l.Bridge)
	if err != nil {
		return err
	}
	l.Identifiers = make(map[uint16]struct{}, l.Count)
	return scanRows(ids, func(row *sql.Rows) error {
		var id uint16
		if err := row.Scan(&id); err != nil {
			return err
		}
		l.Identifiers[id] = struct{}{}
		return nil
	})
}

// BridgeReplay marks a lost peer session before either direction is replayed.
// Its final write prepares outbound exchanges through the ordinary writer and
// clears the mark atomically, after all inbound moves have committed.
type BridgeReplay struct {
	Bridge int64
	Owed   bool
}

// Records returns zero for protocol bookkeeping.
func (*BridgeReplay) Records() int { return 0 }

// WriteTx keeps an interrupted replay owed until its final step commits.
func (r *BridgeReplay) WriteTx(tx *Tx) error {
	if !r.Owed {
		if err := r.prepareOutbound(tx); err != nil {
			return err
		}
	}
	_, err := tx.Prepared(setBridgeReplay).ExecContext(context.Background(), r.Owed, r.Bridge)
	return err
}

// prepareOutbound replaces every phase together using bounded metadata pages
// and the ordinary preparation writer, for either QoS.
func (r *BridgeReplay) prepareOutbound(tx *Tx) error {
	var after int64
	var save [1]SessionDelivery
	var remove [1]uint16
	owner := BridgeDeliveryOwner{ID: r.Bridge}
	change := DeliveryChanges{Markers: &DeliveryMarkers{Bridge: &owner}}
	for {
		rows, err := tx.Prepared(readBridgePreparations).QueryContext(context.Background(), r.Bridge, after)
		if err != nil {
			return err
		}
		var page [MetadataPage]struct {
			SessionDelivery
			active bool
		}
		n := 0
		err = scanRows(rows, func(row *sql.Rows) error {
			p := &page[n]
			if err := row.Scan(&p.PacketID, &p.Seq, &p.Channel, &p.Offset, &p.QoS, &p.Rule, &p.active); err != nil {
				return err
			}
			n++
			return nil
		})
		if err != nil {
			return err
		}
		for _, p := range page[:n] {
			owner.Rule, change.Channel = p.Rule, p.Channel
			change.Save, change.Delete = nil, nil
			if p.active {
				save[0] = p.SessionDelivery
				save[0].Reset = true
				change.Save = save[:]
			} else {
				remove[0] = p.PacketID
				change.Delete = remove[:]
			}
			if err := change.WriteTx(tx); err != nil {
				return err
			}
		}
		if n < len(page) {
			break
		}
		after = page[n-1].Seq
	}
	_, err := tx.Prepared(pruneBridgeRules).ExecContext(context.Background(), r.Bridge)
	return err
}

// ReconcileBridges removes obsolete startup state in one transaction. Rules
// of a surviving bridge keep their MQTT exchanges until CONNACK decides their
// fate. Removed bridges retain only identities still named by stored records.
type ReconcileBridges struct {
	Configured []config.Bridge
	Bridges    []Bridge
	Held       *HeldChanges
	Notices    []string // caller-owned bounded output, returned only after commit
	Omitted    uint64
}

// Records returns zero for startup protocol and cursor cleanup.
func (*ReconcileBridges) Records() int { return 0 }

// WriteTx removes obsolete state and returns its notices and released charges.
func (r *ReconcileBridges) WriteTx(tx *Tx) error {
	configured := make(map[string]*config.Bridge, len(r.Configured))
	for i := range r.Configured {
		configured[r.Configured[i].Name] = &r.Configured[i]
	}
	for _, b := range r.Bridges {
		cfg := configured[b.Name]
		kept := make(map[[2]string]struct{})
		if cfg != nil {
			for _, rule := range cfg.Topics {
				if rule.Direction != config.In {
					kept[[2]string{rule.Filter, rule.Topic}] = struct{}{}
				}
			}
		} else {
			// A bridge's exchanges and held sources disappear with its
			// cursors in this same transaction. Its answer releases charges.
			if _, err := tx.Prepared(deleteBridgeDeliveries).ExecContext(context.Background(), b.ID); err != nil {
				return err
			}
			if err := removeHeld(tx, 0, b.ID, &r.Held); err != nil {
				return err
			}
		}
		if err := r.pruneRules(tx, b, kept); err != nil {
			return err
		}
		if _, err := tx.Prepared(pruneBridgeRules).ExecContext(context.Background(), b.ID); err != nil {
			return err
		}
		if cfg == nil {
			if _, err := tx.Prepared(pruneBridge).ExecContext(context.Background(), b.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// pruneRules pages metadata even when many removed rules survive on disk.
// Only the caller's bounded notice buffer leaves startup; excess is counted.
func (r *ReconcileBridges) pruneRules(tx *Tx, b Bridge, kept map[[2]string]struct{}) error {
	var after int64
	for {
		rows, err := tx.Prepared(loadBridgeRules).QueryContext(context.Background(), b.ID, after)
		if err != nil {
			return err
		}
		var page [MetadataPage]struct {
			id            int64
			filter, topic string
			cursor        bool
		}
		n := 0
		err = scanRows(rows, func(row *sql.Rows) error {
			p := &page[n]
			if err := row.Scan(&p.id, &p.filter, &p.topic, &p.cursor); err != nil {
				return err
			}
			n++
			return nil
		})
		if err != nil {
			return err
		}
		for _, p := range page[:n] {
			if _, ok := kept[[2]string{p.filter, p.topic}]; ok {
				continue
			}
			if _, err := tx.Prepared(removeBridgeCursors).ExecContext(context.Background(), p.id); err != nil {
				return err
			}
			if p.cursor {
				if len(r.Notices) < cap(r.Notices) {
					r.Notices = append(r.Notices, fmt.Sprintf("bridge %s: removed rule %s: cursors deleted", b.Name, p.filter))
				} else {
					r.Omitted++
				}
			}
		}
		if n < len(page) {
			return nil
		}
		after = page[n-1].id
	}
}

// scanRows closes a metadata view on every exit, preserving its first error.
// Writers finish these views before mutating any of the scanned rows.
func scanRows(rows *sql.Rows, scan func(*sql.Rows) error) error {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return finishRows(rows)
}
