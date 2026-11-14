package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/topic"
)

// Subscription holds a filter's options and its channel live-start boundaries.
// Starts is immutable after handover, including across repeated subscriptions.
type Subscription struct {
	Share             int64        // resolved group identity, never stored in this table
	MatchFilter       string       // shared filter's topic part; empty for ordinary subscriptions
	ReadAccess        acl.Coverage // compiled by the reader/core, never stored
	ReadAccessFile    *acl.File    // immutable generation of ReadAccess, never stored
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	Identifier        uint32
	Partitions        string
	Partition         topic.Partitions
	Channels          []int64 // resolved reach, core-owned and not stored
	Starts            []SubscriptionStart
}

// SubscriptionStart names a live-start boundary, or a newly created position.
type SubscriptionStart struct {
	Channel int64
	Start   int64
}

func loadSubscriptions(conn *Conn, session int64) (map[string]Subscription, error) {
	rows, err := conn.Prepared(loadSessionSubscriptions).QueryContext(context.Background(), session)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var subscriptions map[string]Subscription
	for rows.Next() {
		var s Subscription
		var channel, start sql.NullInt64
		if err := rows.Scan(&s.Filter, &s.QoS, &s.NoLocal, &s.RetainAsPublished,
			&s.RetainHandling, &s.Identifier, &s.Partitions, &channel, &start); err != nil {
			return nil, err
		}
		if subscriptions == nil {
			subscriptions = make(map[string]Subscription)
		}
		previous, held := subscriptions[s.Filter]
		s.Partition = previous.Partition
		if !held {
			if err := s.parsePartitions(); err != nil {
				return nil, err
			}
		}
		s.Starts = previous.Starts
		if channel.Valid {
			s.Starts = append(s.Starts, SubscriptionStart{Channel: channel.Int64, Start: start.Int64})
		}
		subscriptions[s.Filter] = s
	}
	return subscriptions, finishRows(rows)
}

// SubscriptionUpdate distinguishes a new filter from replacement options;
// a replacement writes neither its boundaries nor its consumer's position.
type SubscriptionUpdate struct {
	Subscription
	New bool
}

// SaveSubscriptions writes one SUBSCRIBE's accepted filters atomically with
// their boundaries and the new positions selected by the core.
type SaveSubscriptions struct {
	Volatile  bool // shared changes may need a commit for a connection-lifetime member
	Shared    []*SharedChange
	ReachOnly bool // CONNECT adds newly reached positions/boundaries after a configuration change
	Session   int64
	Updates   []SubscriptionUpdate
	Positions []SubscriptionStart // one per channel newly reached by the session
}

// Records returns zero: subscriptions are session metadata.
func (*SaveSubscriptions) Records() int { return 0 }

// WriteTx inserts new filters and updates only options on repeated filters.
// The core names exactly which positions to insert; an unexpected reused key
// is a store error rather than a policy decision made by SQL. Each saved
// start raises its channel's counter in the same transaction.
func (s *SaveSubscriptions) WriteTx(tx *Tx) error {
	for _, change := range s.Shared {
		if err := change.WriteTx(tx); err != nil {
			return err
		}
	}
	if s.Volatile {
		return nil
	}
	var raised AdvanceChannel // the last raise, which a packet's other new filters share
	for i := range s.Updates {
		u := &s.Updates[i]
		statement := updateSubscription
		if u.New {
			statement = addSubscription
		}
		if !s.ReachOnly {
			if _, err := tx.Prepared(statement).ExecContext(context.Background(), u.QoS, u.NoLocal, u.RetainAsPublished, u.RetainHandling,
				u.Identifier, u.Partitions, s.Session, u.Filter); err != nil {
				return err
			}
		}
		if !u.New && !s.ReachOnly {
			continue
		}
		for _, start := range u.Starts {
			if _, err := tx.Prepared(addSubscriptionStart).ExecContext(context.Background(), s.Session, u.Filter, start.Channel, start.Start); err != nil {
				return err
			}
			if raise := (AdvanceChannel{ID: start.Channel, Next: start.Start}); raise != raised {
				if err := raise.WriteTx(tx); err != nil {
					return err
				}
				raised = raise
			}
		}
	}
	for _, position := range s.Positions {
		if err := (&AdvanceChannel{ID: position.Channel, Next: position.Start}).WriteTx(tx); err != nil {
			return err
		}
		if _, err := tx.Prepared(addSubscriptionPosition).ExecContext(context.Background(), s.Session, position.Channel, position.Start); err != nil {
			return err
		}
	}
	return nil
}

// RemoveSubscriptions deletes only the held filters named by one UNSUBSCRIBE.
// Channels names exactly the positions the core decided to remove, each once.
// Kept fixes the surviving positions and supersedes their ordinary exchanges,
// including exchanges that the core has not yet loaded during recovery.
type RemoveSubscriptions struct {
	Volatile bool
	Shared   []*SharedChange
	Session  int64
	Filters  []string
	Channels []int64
	Kept     []ChannelPosition // authoritative positions selected by the core
	Cancel   []uint16          // known never-sent exchanges, removed with their subscriptions
}

// Records returns zero: removing subscriptions stores no message records.
func (*RemoveSubscriptions) Records() int { return 0 }

// WriteTx commits preparation cancellation and the core's explicit position
// decisions with the removed filters. It applies no channel lifetime policy.
func (u *RemoveSubscriptions) WriteTx(tx *Tx) error {
	for _, change := range u.Shared {
		if err := change.WriteTx(tx); err != nil {
			return err
		}
	}
	for _, channel := range u.Channels {
		if _, err := tx.Prepared(deleteSessionMarkers).ExecContext(context.Background(), u.Session, channel); err != nil {
			return err
		}
	}
	for _, position := range u.Kept {
		if _, err := tx.Prepared(rewindSessionPosition).ExecContext(context.Background(), position.Offset, position.Seen, u.Session, position.Channel); err != nil {
			return err
		}
		if _, err := tx.Prepared(supersedeSubscriptionDeliveries).ExecContext(context.Background(), u.Session, position.Channel); err != nil {
			return err
		}
	}
	if len(u.Cancel) != 0 {
		if err := (&DeliveryChanges{Session: u.Session, Delete: u.Cancel}).WriteTx(tx); err != nil {
			return err
		}
	}
	if u.Volatile {
		return nil
	}
	for _, filter := range u.Filters {
		for _, statement := range [...]Statement{removeSubscription, removeSubscriptionStarts} {
			if _, err := tx.Prepared(statement).ExecContext(context.Background(), u.Session, filter); err != nil {
				return err
			}
		}
	}
	for _, channel := range u.Channels {
		if _, err := tx.Prepared(removeSubscriptionPosition).ExecContext(context.Background(), u.Session, channel); err != nil {
			return err
		}
	}
	return nil
}

// SubscriptionMatch shares eligibility between delivery, subscription changes
// and retention. A topic's partition hash is computed at most once.
type SubscriptionMatch struct {
	Topic           []byte
	Offset, Channel int64
	Type            ChannelType
	Local, hashed   bool
	hash            uint64
}

// Eligible applies the common topic, start, No Local and partition rules.
func (m *SubscriptionMatch) Eligible(sub *Subscription, initial bool) bool {
	if !initial && (m.Channel == 1 || m.Type == Latest) && sub.Share == 0 {
		live := false
		for _, start := range sub.Starts {
			if start.Channel == m.Channel {
				live = start.Start <= m.Offset
				break
			}
		}
		if !live {
			return false
		}
	}
	filter := sub.Filter
	if sub.MatchFilter != "" {
		filter = sub.MatchFilter
	}
	if (sub.NoLocal && m.Local) || !topic.Match(filter, string(m.Topic)) {
		return false
	}
	if sub.Partition.Count != 0 {
		if !m.hashed {
			m.hash, m.hashed = topic.Hash(m.Topic), true
		}
		_, eligible := sub.Partition.Indices[uint32(m.hash%uint64(sub.Partition.Count))]
		return eligible
	}
	return true
}

// parsePartitions is shared by session recovery and retention's stored filters.
func (s *Subscription) parsePartitions() error {
	if s.Partitions == "" {
		return nil
	}
	for declaration := range strings.SplitSeq(s.Partitions, "\n") {
		if !s.Partition.Add(declaration) {
			return fmt.Errorf("store: damaged partitions for %q", s.Filter)
		}
	}
	return nil
}
