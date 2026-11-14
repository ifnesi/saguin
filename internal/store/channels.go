package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/topic"
)

// ChannelType is a channel's stored log type (storage.md).
type ChannelType uint8

// Stored channel types.
const (
	Append ChannelType = iota + 1
	Latest
	Queue
)

// Channel holds a stored channel row. The core assigns IDs and offsets.
type Channel struct {
	ID          int64
	Name        string
	Type        ChannelType
	Next        int64
	Floor       int64
	Bytes       int64
	HeldBytes   int64 // incoming QoS 2 reservations, separate from records
	Jobs        int64 // startup unfinished-job count, independent of reader records
	RetryJobs   int64 // jobs waiting for the retry deadline, including overdue retries
	RetryAt     sql.NullInt64
	Records     int64 // startup count; subsequent committed changes are counted by the core
	Consumers   int64
	PositionMin sql.NullInt64
}

// AddChannels adds the rows the core assigns at startup, without replacing
// an existing ID or name. It stores no message records.
type AddChannels struct {
	Channels []Channel
}

// Records returns zero: channel metadata is not a message record.
func (*AddChannels) Records() int { return 0 }

// WriteTx inserts the channels in the writer's open transaction, using its
// prepared connection statement.
func (a *AddChannels) WriteTx(tx *Tx) error {
	for i := range a.Channels {
		c := &a.Channels[i]
		if _, err := tx.Prepared(AddChannel).ExecContext(context.Background(), c.ID, c.Name, c.Type, c.Next, c.Floor, c.Bytes); err != nil {
			return err
		}
	}
	return nil
}

// CountUnmatchedValues reports values a changed latest filter leaves behind.
// It shares the bounded current-value read, keeping no catalogue in the core.
type CountUnmatchedValues struct {
	Channel, Before int64
	Filters         []string
	Count           int64
}

// ReadConn counts in one startup view, releasing each metadata page in turn.
func (r *CountUnmatchedValues) ReadConn(conn *Conn) error {
	page := ReadRecordMetadata{Channel: r.Channel, From: 1, Before: r.Before, Retained: true, Limit: MetadataPage}
	for {
		if err := page.ReadConn(conn); err != nil {
			return err
		}
		for i := range page.Rows {
			name := string(page.Rows[i].Topic)
			if !slices.ContainsFunc(r.Filters, func(filter string) bool { return topic.Match(filter, name) }) {
				r.Count++
			}
		}
		if len(page.Rows) < page.Limit {
			return nil
		}
		page.From = page.Rows[len(page.Rows)-1].Offset + 1
	}
}

func loadChannels(conn *Conn) (*State, error) {
	stmt := conn.Prepared(LoadChannels)
	rows, err := stmt.QueryContext(context.Background())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	state := new(State)
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.Next, &c.Floor, &c.Bytes); err != nil {
			return nil, err
		}
		state.Channels = append(state.Channels, c)
	}
	if err := finishRows(rows); err != nil {
		return nil, err
	}
	for i := range state.Channels {
		c := &state.Channels[i]
		if c.Type == Queue {
			if err := conn.Prepared(countQueueJobs).QueryRowContext(context.Background(), c.ID).Scan(&c.Jobs, &c.RetryJobs, &c.RetryAt); err != nil {
				return nil, err
			}
		}
		if err := conn.Prepared(countChannelRecords).QueryRowContext(context.Background(), c.ID).Scan(&c.Records); err != nil {
			return nil, err
		}
	}
	if err := walkMetricPositions(conn, func(channel, offset int64) {
		if i, found := slices.BinarySearchFunc(state.Channels, channel, func(c Channel, id int64) int {
			return cmp.Compare(c.ID, id)
		}); found {
			c := &state.Channels[i]
			c.Consumers++
			if !c.PositionMin.Valid || offset < c.PositionMin.Int64 {
				c.PositionMin = sql.NullInt64{Int64: offset, Valid: true}
			}
		}
	}); err != nil {
		return nil, err
	}
	// LoadChannels orders by ID, and IDs start at one. The schema's
	// creation transaction always stores this row; its absence is damage,
	// checked on the read-only validation connection before writable open.
	if len(state.Channels) == 0 || state.Channels[0].ID != 1 {
		return nil, errors.New("damaged file: missing $broadcast channel (id 1)")
	}
	// Numbering resumes from next, which every saved record and subscription
	// start raised; a counter below them is damage, never repaired here.
	var name string
	var next int64
	err = conn.Prepared(channelBelowSaved).QueryRowContext(context.Background()).Scan(&name, &next)
	switch {
	case err == nil:
		return nil, fmt.Errorf("damaged file: channel %s next %d is below a saved record or subscription start", name, next)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	return state, nil
}

// DropChannel removes a stored channel and all rows owned by its log while
// holding the broker's storage lock. Subscriptions survive and resolve their
// reach under the next startup configuration. No catalogue object is changed.
func DropChannel(cfg *config.Config, name string) (err error) {
	lock, err := Lock(cfg)
	if err != nil {
		return err
	}
	opened := startup{locks: []*FileLock{lock}, full: true}
	defer func() {
		if closeErr := opened.close(); err == nil {
			err = closeErr
		}
	}()
	state, initialized, err := existingState(cfg.Broker.Storage.FilePath)
	if err != nil {
		return err
	}
	if !initialized {
		return fmt.Errorf("stored channel %s does not exist", name)
	}
	var dropped *Channel
	for i := range state.Channels {
		if state.Channels[i].Name == name {
			dropped = &state.Channels[i]
			break
		}
	}
	if dropped == nil {
		return fmt.Errorf("stored channel %s does not exist", name)
	}
	if dropped.ID == 1 {
		return errors.New("cannot drop $broadcast")
	}
	// The command is for an obsolete or retyped channel, not an online
	// purge. Use resolved routing so generated dead-letter logs agree too.
	policy := config.NewRouting(cfg.Channels)
	for i := range policy.Channels {
		configured := &policy.Channels[i]
		if configured.Name == name && ChannelType(configured.Type)+1 == dropped.Type {
			return fmt.Errorf("channel %s is still configured with the stored type", name)
		}
	}
	conn, err := opened.open(cfg.Broker.Storage.FilePath, "rw")
	if err != nil {
		return err
	}
	if err := conn.prepare(true); err != nil {
		return err
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	t := &Tx{Tx: tx, statements: &conn.statements}
	for i := range state.Channels {
		ch := &state.Channels[i]
		if ch.ID != dropped.ID && (dropped.Type != Queue || ch.Name != name+"__dlq") {
			continue
		}
		for _, statement := range dropChannelStatements {
			if _, err := t.Prepared(statement).ExecContext(context.Background(), ch.ID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
