// Package store owns Sagüin's SQLite connections and commits requests from
// the core before answering them.
package store

import (
	"database/sql"
	"errors"
)

// Credit bounds outstanding requests, reads and writes together. Request
// queues have this much room; answers have twice as much for ordered reads'
// two answers, so neither side waits while working within credit.
const Credit = 1024

// Write changes rows in the writer's transaction, or requests a sync or
// checkpoint outside a transaction in that same lane. Records counts stored
// messages, not changed rows; a metadata-only write returns zero.
type Write interface {
	WriteTx(*Tx) error
	Records() int
}

// ErrFull answers every write of a transaction that failed with SQLITE_FULL,
// which guarantees rollback, without stopping the store (R8).
var ErrFull = errors.New("store: full")

// Read runs on a reader's lifetime connection. Ordered reads keep a fixed
// transaction view; RecordRead operations name already committed records.
type Read interface {
	ReadConn(*Conn) error
}

// RecordRead names records within a boundary already seen committed by the
// core. It has no ordered view gate and returns only its final answer.
type RecordRead interface {
	Read
	RecordRead() bool // false when the operation selects retained values
}

// ObservationRead inspects state without making a broker decision. It keeps
// a reader transaction and sends view confirmation, but neither joins the
// decision-read order nor gates writes while establishing that snapshot.
type ObservationRead interface {
	Read
	ObservationRead()
	Abandoned() bool
	Capture()
	AwaitCapture() bool
}

// Observation supplies the common classification and caller lifetime for
// operations views. The reader confirms its snapshot before the core supplies
// its projection, then waits for that handover without holding back writes.
// Cancellation skips queued work, the handover wait and page processing; it
// never cancels a SQLite call or replaces the reader's lifetime connection.
type Observation struct {
	Done     <-chan struct{}
	captured chan struct{}
}

// NewObservation prepares the one-use projection handover within a read.
// Outstanding read credit bounds these notification channels too.
func NewObservation(done <-chan struct{}) Observation {
	return Observation{Done: done, captured: make(chan struct{})}
}

// ObservationRead marks an inspection as independent of broker decisions.
func (*Observation) ObservationRead() {}

// Capture transfers the projection from the core back to the waiting reader.
// The core alone calls it, once, after the view-established confirmation.
func (o *Observation) Capture() { close(o.captured) }

// AwaitCapture keeps the fixed snapshot open until its projection arrives.
func (o *Observation) AwaitCapture() bool {
	select {
	case <-o.captured:
		return !o.Abandoned()
	case <-o.Done:
		return false
	}
}

// Abandoned reports that the HTTP caller has already stopped waiting.
func (o *Observation) Abandoned() bool {
	select {
	case <-o.Done:
		return true
	default:
		return false
	}
}

// RequestClass distinguishes ordered work from reads that may proceed
// independently. The zero value preserves order for writes and internal stages.
type RequestClass uint8

// Store scheduling classes.
const (
	OrderedRequest RequestClass = iota
	RecordRequest
	ObservationRequest
)

// Classify determines an operation's request order once. Retained and job
// pages keep an ordered view; only ordinary committed-record reads bypass it.
// Observations establish a view without participating in the decision order.
func Classify(op any) RequestClass {
	if r, ok := op.(RecordRead); ok && r.RecordRead() {
		return RecordRequest
	}
	if _, observation := op.(ObservationRead); observation {
		return ObservationRequest
	}
	return OrderedRequest
}

// Answer confirms a read's view, or returns ownership and the result to the
// core. A successful write result means its transaction has committed.
type Answer struct {
	Op   any
	View bool // an ordered read's view is established; its result follows
	Err  error
}

// State is the state loaded at startup for the core to own.
type State struct {
	SessionsRestored uint64 // sessions retained beyond this startup's boundary
	Abandoned        uint64 // non-durable holds deleted before loading their charges
	RetryAt          int64  // refused startup housekeeping, on the ordinary retry deadline
	Channels         []Channel
	Bridges          []Bridge
	SnapshotDirty    bool             // startup committed changes after making the RAM copy
	Started          int64            // this start's Unix milliseconds
	Wills            int64            // number of stored Wills
	NextWill         int64            // earliest recovered Will due time
	HighestSession   int64            // Highest session ID named by any table, including non-durable sessions.
	HighestShare     int64            // highest group identity still named by stored state
	SharedDropped    map[int64]uint64 // volatile QoS 2 assignments ended at startup, by configured channel
	RetainedValues   int64
	RetainedCharge   int64 // current values and held replacements, by topic maximum
	RetainedBytes    int64
	RetainedExpiry   sql.NullInt64
	RetainedTime     sql.NullInt64
	MetricCounts     MetricCounts
	Measure          Read // immutable file-size observation shared with periodic refreshes
}

// Store passes requests between the core and the connection owners. Only
// the core sends requests, within Credit, and it always drains Answers.
// An operation and its buffers belong to the store from send until answer.
// An observation's view confirmation returns its projection fields to the
// core until Capture hands them back; the reader touches none while waiting.
type Store struct {
	writes      chan Write
	reads       chan Read
	answers     chan Answer
	stop        chan struct{}
	done        chan error
	failures    chan Answer // failed reads, including the request carrying their error
	readersDone chan error  // each reader's connection-close result
	databases   []*sql.DB
	locks       []*FileLock
	startupCopy string // main arms cleanup for a refused start or a safe final snapshot
	memoryPath  string // immutable RAM path; empty outside memory mode
	measurement measurement
}

// Writes returns the bounded writer queue.
func (s *Store) Writes() chan<- Write { return s.writes }

// Reads returns the bounded queue served only by store readers.
func (s *Store) Reads() chan<- Read { return s.reads }

// Answers returns one result, plus a view confirmation for ordered reads.
// Its capacity is 2*Credit; only a result returns the request's credit.
func (s *Store) Answers() <-chan Answer { return s.answers }

// Started completes the startup resource handoff once every listener is open.
// Main alone calls this and Stop. Until then, Stop cleans a refused start's
// RAM copy after all connections close and before releasing the live locks.
func (s *Store) Started() { s.startupCopy = "" }

// Snapshotted arms RAM cleanup after the core has completed its final snapshot.
// Main calls it only after the core has ended, with no newer committed work.
func (s *Store) Snapshotted() { s.startupCopy = s.memoryPath }

// Stop ends all connection owners, closes their databases and releases the
// storage lock without deleting its file. Main calls it
// exactly once, after the core has received every read and write answer.
// It reports only errors no answer carried, including errors found at close.
// Request and answer channels have multiple users and are never closed (R3).
func (s *Store) Stop() error {
	close(s.stop)
	err := <-s.done
	for _, db := range s.databases {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}
	if s.startupCopy != "" {
		err = errors.Join(err, removeDatabase(s.startupCopy))
	}
	if closeErr := releaseLocks(s.locks); err == nil {
		err = closeErr
	}
	return err
}
