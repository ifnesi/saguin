package store

import (
	"context"
	"database/sql"
	"maps"
	"slices"

	"github.com/ifnesi/saguin/internal/operations/views"
)

// SessionView shares the scalar projection between live and stored sessions.
func SessionView(s *Session, subscriptions int, connected bool) *views.Session {
	protocol := "5"
	if s.Protocol == 4 {
		protocol = "3.1.1"
	}
	return &views.Session{ID: s.ID, ClientID: s.Client, User: s.User, Protocol: protocol,
		Connected: connected, Listener: s.Listener, Remote: s.Remote, Keepalive: s.KeepAlive,
		ExpiresAfter: s.Expiry, CleanStart: s.CleanStart, Subscriptions: subscriptions}
}

// PartitionView returns the declared partition set in stable index order.
func (s *Subscription) PartitionView() views.Partition {
	return views.Partition{Filter: s.Filter, Count: s.Partition.Count, Indices: slices.Sorted(maps.Keys(s.Partition.Indices))}
}

// ReadSessionsView streams rows outside the later core capture as away,
// without loading any session. The capture alone decides which clients are
// live: a stored left marker may still precede a reconnect's confirmation.
// Live client ids exclude all backing rows, even after a session replacement.
type ReadSessionsView struct {
	Observation
	Live map[string]struct{}
	View views.Sessions
}

// ReadConn keeps the catalogue and declared partitions in one fixed view.
func (r *ReadSessionsView) ReadConn(conn *Conn) error {
	after := ""
	for {
		if r.Abandoned() {
			return nil
		}
		rows, err := conn.Prepared(readOperationsSessions).QueryContext(context.Background(), after)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			if r.Abandoned() {
				return finishRows(rows)
			}
			var s Session
			var subscriptions int
			if err := rows.Scan(&s.ID, &s.Client, &s.User, &s.Protocol, &s.Expiry, &s.Listener,
				&s.Remote, &s.KeepAlive, &s.CleanStart, &subscriptions); err != nil {
				_ = rows.Close()
				return err
			}
			after, n = s.Client, n+1
			if _, live := r.Live[s.Client]; !live {
				r.View.Away++
				// The query already orders away client ids. Later rows count
				// toward the total but cannot displace the first bounded set.
				if r.View.Away <= views.Limit {
					r.View.Sessions = views.Include(r.View.Sessions, SessionView(&s, subscriptions, false), views.CompareSessions)
				}
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
	for i := range r.View.Sessions {
		s := r.View.Sessions[i]
		if s.Connected {
			continue
		}
		if err := r.readPartitions(conn, s); err != nil {
			return err
		}
	}
	r.View.Total = r.View.Connected + r.View.Away
	r.View.Returned = len(r.View.Sessions)
	return nil
}

func (r *ReadSessionsView) readPartitions(conn *Conn, s *views.Session) error {
	if r.Abandoned() {
		return nil
	}
	rows, err := conn.Prepared(readOperationsPartitions).QueryContext(context.Background(), s.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if r.Abandoned() {
			return finishRows(rows)
		}
		var sub Subscription
		if err := rows.Scan(&sub.Filter, &sub.Partitions); err != nil {
			return err
		}
		if err := sub.parsePartitions(); err != nil {
			return err
		}
		s.Partitions = append(s.Partitions, sub.PartitionView())
	}
	return finishRows(rows)
}

// ReadConsumersView adds stored away positions to the core's bounded rows.
// Live client ids and bridge rule ids exclude positions held by the core;
// channel next offsets come only from the core's capture.
type ReadConsumersView struct {
	Observation
	Live        map[string]struct{}
	ActiveRules map[int64]struct{}
	View        views.Consumers
}

// ReadConn reads all selected channels at the same Store-read cut.
func (r *ReadConsumersView) ReadConn(conn *Conn) error {
	for i := range r.View.Channels {
		ch := &r.View.Channels[i]
		if err := r.readPositions(conn, ch, false); err != nil {
			return err
		}
		if err := r.readPositions(conn, ch, true); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReadConsumersView) readPositions(conn *Conn, ch *views.Channel, bridge bool) error {
	statement := readOperationsPositions
	if bridge {
		statement = readOperationsCursors
	}
	var after int64
	for {
		if r.Abandoned() {
			return nil
		}
		rows, err := conn.Prepared(statement).QueryContext(context.Background(), ch.ID, after)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			if r.Abandoned() {
				return finishRows(rows)
			}
			var id, offset int64
			var name, filter, topic string
			var seen sql.NullInt64
			if err := rows.Scan(&id, &name, &offset, &seen, &filter, &topic); err != nil {
				_ = rows.Close()
				return err
			}
			after, n = id, n+1
			var active bool
			if bridge {
				_, active = r.ActiveRules[id]
			} else {
				_, active = r.Live[name]
			}
			if active {
				continue
			}
			p := views.Position{Reader: "mqtt:" + name, Offset: offset, LastSeen: views.Millis(seen.Int64)}
			if bridge {
				p.Reader, p.Rule = "bridge:"+name, &views.Rule{Filter: filter, Topic: topic}
			}
			ch.Add(p)
		}
		err = finishRows(rows)
		if err != nil || n < MetadataPage {
			return err
		}
	}
}

// ReadQueueView carries only live attempt metadata. Unassigned jobs and their
// attempt history remain in SQLite; the returned page never reads a payload.
type ReadQueueView struct {
	Observation
	Channel int64
	Live    map[int64]views.Job
	View    views.Queue
}

// ReadConn counts every unfinished job, then returns its oldest bounded page.
func (r *ReadQueueView) ReadConn(conn *Conn) error {
	if r.Abandoned() {
		return nil
	}
	var waiting int
	var next sql.NullInt64
	if err := conn.Prepared(countQueueJobs).QueryRowContext(context.Background(), r.Channel).Scan(&r.View.Total, &waiting, &next); err != nil {
		return err
	}
	if r.Abandoned() {
		return nil
	}
	rows, err := conn.Prepared(readOperationsJobs).QueryContext(context.Background(), r.Channel)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if r.Abandoned() {
			return finishRows(rows)
		}
		var job views.Job
		var first, last, retry sql.NullInt64
		if err := rows.Scan(&job.Offset, &job.Topic, &job.Attempts, &first, &last, &retry); err != nil {
			return err
		}
		job.State = "ready"
		if retry.Valid {
			job.State = "backoff"
		}
		if active, ok := r.Live[job.Offset]; ok {
			job.State, job.Holder, job.HeldUntil = active.State, active.Holder, active.HeldUntil
		}
		job.FirstAttempt, job.LastAttempt = views.Millis(first.Int64), views.Millis(last.Int64)
		r.View.Records = append(r.View.Records, job)
	}
	r.View.Returned = len(r.View.Records)
	return finishRows(rows)
}
