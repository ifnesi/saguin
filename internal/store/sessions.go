package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// NeverExpires is the MQTT Session Expiry Interval that has no deadline.
// Message Expiry does not give this value any special meaning.
const NeverExpires = math.MaxUint32

// Session holds a durable session's sessions row (storage.md). The core
// assigns its ID and decides the expiry, in seconds; Left is Unix milliseconds,
// or zero while connected.
type Session struct {
	ID             int64
	Client         string
	User           string
	Protocol       uint8
	Expiry         uint32
	Left           int64
	ReceiveMaximum uint16
	Listener       string
	Remote         string
	KeepAlive      uint16
	CleanStart     bool
}

// LoadSession reads a client's durable session row at CONNECT. A missing
// client is a successful read with Found false.
type LoadSession struct {
	Client        string
	Session       Session
	Found         bool
	Subscriptions map[string]Subscription
	Position      int64             // fixed channel 1 position, held inline
	Positions     []ChannelPosition // configured-channel positions, in channel id order
	Seen          sql.NullInt64
	Sequence      int64 // last outstanding delivery's sending order
	Deliveries    int   // stored exchanges; recovery pages obey the new connection's bounds
	Will          WillInfo
	Held          []HeldIdentifier // incoming identifiers only; publications remain stored
}

// ReadConn loads the row through a reader's prepared statement.
func (l *LoadSession) ReadConn(conn *Conn) error {
	s := &l.Session
	err := conn.Prepared(loadSession).QueryRowContext(context.Background(), l.Client).Scan(
		&s.ID, &s.Client, &s.User, &s.Protocol, &s.Expiry, &s.Left, &s.ReceiveMaximum,
		&s.Listener, &s.Remote, &s.KeepAlive, &s.CleanStart,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	l.Found = true
	l.Subscriptions, err = loadSubscriptions(conn, s.ID)
	if err != nil {
		return err
	}
	if err := l.loadPositions(conn); err != nil {
		return err
	}
	if err := conn.Prepared(loadDeliverySequence).QueryRowContext(context.Background(), s.ID).Scan(&l.Sequence, &l.Deliveries); err != nil {
		return err
	}
	l.Held, err = loadHeld(conn, s.ID, 0)
	if err != nil {
		return err
	}
	return readWillInfo(conn, s.ID, &l.Will)
}

// ChannelPosition holds a channel's saved progress at CONNECT.
type ChannelPosition struct {
	Channel, Offset int64
	Seen            sql.NullInt64
}

func (l *LoadSession) loadPositions(conn *Conn) error {
	rows, err := conn.Prepared(loadSessionPosition).QueryContext(context.Background(), l.Session.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var p ChannelPosition
		if err := rows.Scan(&p.Channel, &p.Offset, &p.Seen); err != nil {
			return err
		}
		if p.Channel == 1 {
			l.Position, l.Seen = p.Offset, p.Seen
		} else {
			l.Positions = append(l.Positions, p)
		}
	}
	return finishRows(rows)
}

// SaveSession inserts a new durable session when New, or updates every
// column of its existing row at a reconnect. A reused key is never replaced.
type SaveSession struct {
	Reach   *SaveSubscriptions // newly reached channels, committed with the reconnect
	Session Session
	New     bool
	Will    *Will
}

// Records returns zero: a session row is metadata, not a message record.
func (*SaveSession) Records() int { return 0 }

// WriteTx stores the row in the writer's open transaction.
func (s *SaveSession) WriteTx(tx *Tx) error {
	statement := updateSession
	if s.New {
		statement = addSession
	}
	c := &s.Session
	_, err := tx.Prepared(statement).ExecContext(context.Background(), c.Client, c.User, c.Protocol, c.Expiry, c.Left, c.ReceiveMaximum,
		c.Listener, c.Remote, c.KeepAlive, c.CleanStart, c.ID,
	)
	if err != nil {
		return err
	}
	if s.Reach != nil {
		if err := s.Reach.WriteTx(tx); err != nil {
			return err
		}
	}
	return (&SaveWill{Session: c.ID, Will: s.Will}).WriteTx(tx)
}

// Will holds a wills row in transit, never in a connected session's state.
// Properties includes its Property Length, as in every stored message.
type Will struct {
	Client, User, Topic string
	QoS                 byte
	Retain              bool
	Delay               uint32
	Payload, Properties []byte
	StoredPayload       []byte // reader-prepared S2 copy, reused when publishing the Will
	PayloadSize         int    // original length when Payload has been released
}

// SaveWill replaces a CONNECT's Will, or removes it when Will is nil.
// It also serves sessions with no durable sessions row.
type SaveWill struct {
	Session int64
	Will    *Will
}

// Records returns zero: a Will is not yet a channel record.
func (*SaveWill) Records() int { return 0 }

// WriteTx changes the Will within its CONNECT or publication transaction.
func (s *SaveWill) WriteTx(tx *Tx) error {
	if _, err := tx.Prepared(deleteWill).ExecContext(context.Background(), s.Session); err != nil {
		return err
	}
	if w := s.Will; w != nil {
		_, err := tx.Prepared(addWill).ExecContext(context.Background(), s.Session, w.Client, w.User,
			w.Topic, w.QoS, w.Retain, w.Delay, payload.Stored(w.Payload, w.StoredPayload), max(len(w.Payload), w.PayloadSize), w.Properties)
		return err
	}
	return nil
}

// WillInfo is bounded metadata used to order a Will with its session. The
// payload remains in the store until the firing has reserved message memory.
type WillInfo struct {
	Session int64
	Bytes   int64 // reservation for the original body and any stored S2 source
	Delay   uint32
	Due     sql.NullInt64
	Left    int64
}

// LoadWillInfo reads only the old connected owner's Will metadata at takeover.
// Its subscriptions and delivery state already belong to the core.
type LoadWillInfo struct {
	Session int64
	Will    WillInfo
}

// ReadConn shares the metadata lookup used by an away session's CONNECT.
func (l *LoadWillInfo) ReadConn(conn *Conn) error {
	return readWillInfo(conn, l.Session, &l.Will)
}

func readWillInfo(conn *Conn, id int64, w *WillInfo) error {
	err := conn.Prepared(loadWillInfo).QueryRowContext(context.Background(), id).Scan(
		&w.Session, &w.Bytes, &w.Delay, &w.Due, &w.Left)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// LoadWill reads a session's Will only when it fires, after the
// core has reserved memory for its bytes and ordered its session lane.
type LoadWill struct {
	Session  int64
	Expected WillInfo // validates that a due-page decision still owns this Will
	Will     Will
	Found    bool
}

// ReadConn transfers the row through the reader's prepared statement.
func (l *LoadWill) ReadConn(conn *Conn) error {
	w := &l.Will
	e := &l.Expected
	rows, err := conn.Prepared(loadWill).QueryContext(context.Background(), l.Session, e.Bytes, e.Delay, e.Due, e.Left)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return finishRows(rows)
	}
	var stored sql.RawBytes
	// RawBytes avoids database/sql's copy. The pinned go-sqlite3 driver
	// gives each BLOB its own slice, never reused after Next or Close;
	// database/sql's RawBytes contract alone does not promise that lifetime.
	if err := rows.Scan(&w.Client, &w.User, &w.Topic, &w.QoS, &w.Retain, &w.Delay, &stored, &w.PayloadSize, &w.Properties); err != nil {
		return err
	}
	if len(stored) < w.PayloadSize {
		w.StoredPayload = stored
	}
	w.Payload, err = payload.Decode(stored, w.PayloadSize)
	if err != nil {
		return err
	}
	if err := finishRows(rows); err != nil {
		return err
	}
	l.Found = true
	return nil
}

// LeaveSession stores a durable session's expiry and the time its connection
// ended. All other columns stay as they were at its last CONNECT.
type LeaveSession struct {
	ID          int64
	Expiry      uint32
	Left        int64
	DiscardWill bool
	WillDue     int64 // nonzero schedules its Will; a stop leaves it to restart
}

// Records returns zero: going away stores no message record.
func (*LeaveSession) Records() int { return 0 }

// WriteTx updates the row in the writer's open transaction.
func (l *LeaveSession) WriteTx(tx *Tx) error {
	_, err := tx.Prepared(leaveSession).ExecContext(context.Background(), l.Expiry, l.Left, l.ID)
	if err != nil {
		return err
	}
	if l.DiscardWill {
		return (&SaveWill{Session: l.ID}).WriteTx(tx)
	}
	if l.WillDue != 0 {
		_, err = tx.Prepared(scheduleWill).ExecContext(context.Background(), l.WillDue, l.ID)
	}
	return err
}

// EndSession deletes a session and its rows in one transaction. KeepWill
// preserves a Will awaiting publication, including one deferred by a stop.
// Non-durable sessions have no sessions row, but can own Wills and held QoS 2
// publishes, which end through the same operation.
type EndSession struct {
	Shared         *SharedEnding
	Held           *HeldChanges
	KeepHeld       bool // durable state removed while the same session continues in memory
	KeepDeliveries bool // inherited exchanges stay stored for bounded recovery on this connection
	ID             int64
	KeepWill       bool
}

// Records returns zero: deleting session state stores no message record.
func (*EndSession) Records() int { return 0 }

// WriteTx removes the session's rows in the writer's open transaction.
func (e *EndSession) WriteTx(tx *Tx) error {
	if err := e.returnShares(tx); err != nil {
		return err
	}
	if !e.KeepHeld {
		if err := removeHeld(tx, e.ID, 0, &e.Held); err != nil {
			return err
		}
	}
	for _, statement := range [...]Statement{
		deleteSession, deleteSubscriptions, deleteSubscriptionStarts, deleteWill, deletePositions, deleteSessionInflight,
	} {
		if statement == deleteWill && e.KeepWill {
			continue
		}
		if statement == deleteSessionInflight && e.KeepDeliveries {
			continue
		}
		if _, err := tx.Prepared(statement).ExecContext(context.Background(), e.ID); err != nil {
			return err
		}
	}
	return e.endShares(tx)
}

// DueBatch bounds the sessions returned by one due read, at start and on
// each expiry timer fire. The core keeps only one batch at a time (R6).
const DueBatch = 256

// DueSession identifies a due session and its client, so the core can order
// its ending against a CONNECT (R2). Expiry and ID locate it within a pass.
type DueSession struct {
	Shared   bool // an abandoned connection-lifetime shared group
	HeldOnly bool // abandoned incoming or outgoing rows, without ending a delayed Will
	ID       int64
	Client   string
	Expiry   int64 // the pass's deadline, Unix milliseconds
	Will     WillInfo
}

// DueSessions reads at most DueBatch sessions whose expiry has run out by At,
// strictly after After, in expiry, then id, order. After is zero at the start
// of a pass. Next is the earliest expiry after After omitted, due or not, or
// zero when no other away session remains. Times are Unix milliseconds.
type DueSessions struct {
	Retry    bool // also rediscover non-durable endings refused for room
	At       int64
	After    DueSession
	Sessions []DueSession
	Next     int64
}

// ReadConn follows sessions_expiry in one ordered, bounded read. One extra
// row supplies Next even when the batch is full and more sessions are due.
func (d *DueSessions) ReadConn(conn *Conn) error {
	after := d.After
	if d.Retry && (after.ID == 0 || after.Shared) {
		if err := d.readDue(conn, dueSharedGroups, after.ID); err != nil {
			return err
		}
		if d.Next != 0 {
			return nil
		}
		after = DueSession{}
	}
	if d.Retry && after.Expiry <= 1 {
		if err := d.readDue(conn, dueHeld, after.ID); err != nil {
			return err
		}
		if d.Next != 0 {
			return nil
		}
		after = DueSession{}
	}
	return d.readDue(conn, dueSessions, after.Expiry, after.Expiry, after.ID)
}

// DueWills uses the same bounded pass and continuation as session expiry.
type DueWills struct {
	DueSessions
	Retry bool // also rediscover immediate firings refused with due still NULL
}

// ReadConn reads Will metadata, never its payload.
func (d *DueWills) ReadConn(conn *Conn) error {
	after := d.After
	if d.Retry && !after.Will.Due.Valid {
		if err := d.readDue(conn, retryWills, after.ID); err != nil {
			return err
		}
		if d.Next != 0 {
			return nil
		}
		// The NULL candidates precede timed Wills. Their continuation has
		// no timed deadline; start that part at its earliest entry.
		after = DueSession{}
	}
	return d.readDue(conn, dueWills, after.Expiry, after.Expiry, after.ID)
}

func (d *DueSessions) readDue(conn *Conn, statement Statement, args ...any) error {
	args = append(args, DueBatch-len(d.Sessions)+1)
	rows, err := conn.Prepared(statement).QueryContext(context.Background(), args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		s := DueSession{HeldOnly: statement == dueHeld, Shared: statement == dueSharedGroups}
		if err := rows.Scan(&s.ID, &s.Client, &s.Expiry, &s.Will.Session, &s.Will.Bytes, &s.Will.Delay, &s.Will.Due, &s.Will.Left); err != nil {
			return err
		}
		if s.Expiry > d.At || len(d.Sessions) == DueBatch {
			d.Next = s.Expiry
			break
		}
		if d.Sessions == nil {
			d.Sessions = make([]DueSession, 0, DueBatch)
		}
		d.Sessions = append(d.Sessions, s)
	}
	return finishRows(rows)
}

// restoreSessions makes clients left connected away from this start, once.
// Expired rows remain for the core to end, together with any delayed Will;
// only sessions kept beyond this start count as restored. A full disk loses
// the departure write, leaving those clients stored as connected until the
// next start; it never stops startup (Store).
func restoreSessions(writer, reader *Conn, at time.Time, reg *metrics.Registry) (uint64, error) {
	left := timestamp.CeilMilli(at)
	if _, err := writer.Prepared(restoreWills).ExecContext(context.Background(), left, left); err != nil && !diskFull(err) {
		return 0, err
	}
	if _, err := writer.Prepared(leaveConnectedSessions).ExecContext(context.Background(), left); err != nil && !diskFull(err) {
		return 0, err
	}
	var restored uint64
	err := reader.Prepared(countRestoredSessions).QueryRowContext(context.Background(), at.UnixMilli()).Scan(&restored)
	if err != nil {
		return 0, err
	}
	reg.Counter("saguin_sessions_restored_total").Add(restored)
	return restored, nil
}
