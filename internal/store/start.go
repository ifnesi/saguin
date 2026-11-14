package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"

	_ "github.com/mattn/go-sqlite3" // Registers the SQLite driver.

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/timestamp"
)

var openDSN = databaseDSN // the connection string of every connection Start keeps

// Start takes ownership of lock, validates or creates the disk store, loads
// the core's state, and starts exactly one writer and read_connections readers
// (R1). Opening and preparation complete before any goroutine starts.
// The caller takes the lock and writes pid_file before calling Start. A
// refused start closes connections, removes its RAM copy and releases locks;
// after success, Stop releases the locks. Lock files are never deleted.
func Start(cfg *config.Config, reg *metrics.Registry, lock *FileLock) (_ *Store, _ *State, err error) {
	at := time.Now()
	cfgStore := &cfg.Broker.Storage
	opened := startup{locks: []*FileLock{lock}, full: cfgStore.Mode != config.Normal}
	defer func() {
		if err != nil {
			err = errors.Join(err, opened.close())
			err = fmt.Errorf("open store %s: %w", cfgStore.FilePath, err)
		}
	}()
	if cfgStore.Mode == config.Memory {
		if err := opened.memoryLock(cfg); err != nil {
			return nil, nil, err
		}
	}
	state, initialized, err := existingState(cfgStore.FilePath)
	if err != nil {
		return nil, nil, err
	}
	mode := "rwc"
	if initialized {
		mode = "rw"
	}
	conn, err := opened.open(cfgStore.FilePath, mode)
	if err != nil {
		return nil, nil, err
	}
	if !initialized {
		if err := createSchema(conn.Conn); err != nil {
			return nil, nil, err
		}
	}
	// SQLite follows file_path's link. Keep that same real database path for
	// every derived file, including snapshots, measurements and log syncs.
	path, err := filepath.EvalSymlinks(cfgStore.FilePath)
	if err != nil {
		return nil, nil, err
	}
	cfgStore.FilePath = path
	if err := conn.prepare(true); err != nil {
		return nil, nil, err
	}
	if cfgStore.Mode == config.Memory {
		if err := checkSnapshotGroup(cfgStore.FilePath); err != nil {
			return nil, nil, err
		}
		path = cfg.MemoryPath()
		if err := checkpoint(conn.Prepared(checkpointLog)); err != nil {
			return nil, nil, err
		}
		if err := opened.copyDatabase(cfgStore.FilePath, path); err != nil {
			return nil, nil, err
		}
		if err := opened.closeConnections(); err != nil {
			return nil, nil, err
		}
		conn, err = opened.open(path, "rw")
		if err != nil {
			return nil, nil, err
		}
		if err := conn.prepare(true); err != nil {
			return nil, nil, err
		}
	}
	readers, err := opened.readers(path, cfgStore.ReadConnections)
	if err != nil {
		return nil, nil, err
	}
	if !initialized {
		state, err = loadState(readers[0])
		if err != nil {
			return nil, nil, err
		}
	}
	changesBefore, err := conn.totalChanges()
	if err != nil {
		return nil, nil, err
	}
	state.SessionsRestored, err = restoreSessions(conn, readers[0], at, reg)
	if err != nil {
		return nil, nil, err
	}
	if err := state.MetricCounts.read(readers[0]); err != nil {
		return nil, nil, err
	}
	s := newStore(opened.databases, len(readers))
	files := config.FilesForDatabase(path)
	w := &writer{s: s, conn: conn, tx: Tx{statements: &conn.statements, walPath: files.Data[1]}, full: reg.Gauge("saguin_store_full")}
	cleanup := &discardOrphanHeld{}
	if err := w.write(cleanup); err != nil {
		if !errors.Is(err, ErrFull) {
			return nil, nil, err
		}
		state.RetryAt = at.Add(time.Second).UnixMilli()
	} else {
		state.Abandoned = cleanup.count
	}
	if err := state.loadHeldCharges(readers[0]); err != nil {
		return nil, nil, err
	}
	for after := (SharedReturn{}); ; {
		recovery := &recoverSharedAssignments{after: after}
		if err := w.write(recovery); err != nil {
			if !errors.Is(err, ErrFull) {
				return nil, nil, err
			}
			state.RetryAt = at.Add(time.Second).UnixMilli()
			break
		}
		if len(recovery.dropped) != 0 && state.SharedDropped == nil {
			state.SharedDropped = make(map[int64]uint64)
		}
		for channel, count := range recovery.dropped {
			state.SharedDropped[channel] += count
		}
		if !recovery.more {
			break
		}
		after = recovery.after
	}
	if err := w.write(&discardOrphanDeliveries{}); err != nil {
		if !errors.Is(err, ErrFull) {
			return nil, nil, err
		}
		state.RetryAt = at.Add(time.Second).UnixMilli()
	}
	for {
		cleanup := &discardOrphanShares{}
		if err := w.write(cleanup); err != nil {
			if !errors.Is(err, ErrFull) {
				return nil, nil, err
			}
			state.RetryAt = at.Add(time.Second).UnixMilli()
			break
		}
		if !cleanup.more {
			break
		}
	}
	changesAfter, err := conn.totalChanges()
	if err != nil {
		return nil, nil, err
	}
	// The disk copy already holds everything before startup housekeeping.
	// Count rows on the same writer connection, including every cleanup,
	// so transactions that changed nothing do not cause a whole-file copy.
	state.SnapshotDirty = cfgStore.Mode == config.Memory && changesAfter != changesBefore
	state.Started = timestamp.CeilMilli(at)
	if err := readers[0].Prepared(nextWill).QueryRowContext(context.Background()).Scan(&state.NextWill, &state.Wills); err != nil {
		return nil, nil, err
	}
	reg.Info("saguin_store_info", "mode", cfgStore.Mode.String())
	s.measurement = measurement{
		path: files.Data[0], wal: files.Data[1],
		bytes: reg.Gauge("saguin_store_bytes"), walBytes: reg.Gauge("saguin_store_wal_bytes"),
	}
	if err := s.measurement.read(); err != nil {
		return nil, nil, err
	}
	state.Measure = s.Measure()
	s.locks, s.startupCopy = opened.locks, opened.memoryPath
	s.memoryPath = opened.memoryPath
	writers, readerCount := reg.Goroutine("store_writer"), reg.Goroutine("store_reader")
	go func() {
		writers.Add(1)
		defer writers.Add(-1)
		w.run()
	}()
	for _, reader := range readers {
		go func() {
			readerCount.Add(1)
			defer readerCount.Add(-1)
			s.runReader(reader)
		}()
	}
	return s, state, nil
}

func (c *Conn) totalChanges() (int64, error) {
	var changes int64
	err := c.Prepared(totalChanges).QueryRowContext(context.Background()).Scan(&changes)
	if err != nil {
		return 0, fmt.Errorf("read total changes: %w", err)
	}
	return changes, nil
}

func newStore(databases []*sql.DB, readers int) *Store {
	return &Store{
		writes:      make(chan Write, Credit),
		reads:       make(chan Read, Credit),
		answers:     make(chan Answer, 2*Credit),
		stop:        make(chan struct{}),
		done:        make(chan error, 1),
		failures:    make(chan Answer, readers),
		readersDone: make(chan error, readers),
		databases:   databases,
	}
}

// databaseDSN escapes a path as a URI, including any literal '?' or '#'.
// Read-only connections leave the persistent journal mode alone: changing
// a refused database's rollback journal would change it before refusal.
// The writer establishes WAL before opening the working readers.
func databaseDSN(path, mode string, full bool) string {
	u := url.URL{Scheme: "file", Path: path}
	synchronous := "FULL"
	if !full {
		synchronous = "NORMAL"
	}
	params := url.Values{"mode": {mode}, "_synchronous": {synchronous}}
	if mode != "ro" {
		// Write callbacks may read before changing rows. Take the write lock
		// at BEGIN so they never have to upgrade an established read view.
		params.Set("_txlock", "immediate")
		params.Set("_journal_mode", "WAL")
	}
	u.RawQuery = params.Encode()
	return u.String()
}

// existingState validates through a temporary read-only connection before
// any writable open. SQLite checks the header and parses the catalogue;
// loading state touches only the state this engine restores for now.
// The boolean reports an initialized schema: a missing file or an empty
// version-0 database both need creation.
func existingState(path string) (state *State, initialized bool, err error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	db, err := sql.Open("sqlite3", databaseDSN(path, "ro", true))
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, true, err
	}
	c := &Conn{Conn: conn}
	defer func() { _ = c.close() }()
	var version int
	err = conn.QueryRowContext(context.Background(), `PRAGMA user_version`).Scan(&version)
	if err != nil {
		return nil, true, err
	}
	if version == 0 {
		var objects int
		err = conn.QueryRowContext(context.Background(), `SELECT count(*) FROM sqlite_schema`).Scan(&objects)
		if err != nil {
			return nil, true, err
		}
		if objects == 0 {
			return nil, false, nil
		}
	}
	if version != schemaVersion {
		return nil, true, fmt.Errorf("schema version %d, expected %d", version, schemaVersion)
	}
	if err := checkSchema(conn); err != nil {
		return nil, true, err
	}
	if err := c.prepare(false); err != nil {
		return nil, true, err
	}
	state, err = loadState(c)
	return state, true, err
}

// loadState serves both a validated existing file and a newly created one.
// Session rows stay in the store; only the highest referenced ID is loaded.
func loadState(conn *Conn) (*State, error) {
	state, err := loadChannels(conn)
	if err != nil {
		return nil, err
	}
	err = conn.Prepared(highestSession).QueryRowContext(context.Background()).Scan(&state.HighestSession)
	if err == nil {
		err = conn.Prepared(highestSharedGroup).QueryRowContext(context.Background()).Scan(&state.HighestShare)
	}
	if err == nil {
		err = conn.Prepared(retainedTotals).QueryRowContext(context.Background()).Scan(
			&state.RetainedValues, &state.RetainedBytes, &state.RetainedExpiry, &state.RetainedTime)
	}
	if err != nil {
		return nil, err
	}
	state.Bridges, err = readBridges(conn)
	if err != nil {
		return nil, err
	}
	return state, err
}

// startup owns resources until Start hands them to the connection owners.
type startup struct {
	databases  []*sql.DB
	conns      []*Conn
	locks      []*FileLock
	full       bool
	memoryPath string // RAM copy owned by this start once its destination was created
}

// docs/design.md, Store: "SQLite manages its own log and folds it into the
// database by its own default rule. Sagüin only sets journal_size_limit to 4
// MiB at open, so the log shrinks back after a burst".
const journalSizeLimit = 4 << 20

func (s *startup) open(path, mode string) (*Conn, error) {
	if mode == "rwc" {
		f, err := createPrivateFile(path)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if err == nil {
			if err := f.Close(); err != nil {
				return nil, err
			}
		}
		// Creation belongs here so SQLite cannot create a database with its
		// default permissions, including if the path disappears meanwhile.
		mode = "rw"
	}
	db, err := sql.Open("sqlite3", openDSN(path, mode, s.full))
	if err != nil {
		return nil, err
	}
	// A released lifetime connection must close SQLite itself, not wait in
	// database/sql's idle pool until after another owner has closed.
	db.SetMaxIdleConns(0)
	s.databases = append(s.databases, db)
	conn, err := s.connect(db)
	if err != nil {
		return nil, err
	}
	if mode != "ro" {
		// The driver has no connection-string option for this pragma.
		pragma := fmt.Sprintf("PRAGMA journal_size_limit = %d", journalSizeLimit)
		if _, err := conn.ExecContext(context.Background(), pragma); err != nil {
			return nil, fmt.Errorf("set %s: %w", pragma, err)
		}
	}
	return conn, nil
}

func (s *startup) connect(db *sql.DB) (*Conn, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("connect with journal_mode=WAL: %w", err)
	}
	c := &Conn{Conn: conn}
	s.conns = append(s.conns, c)
	if err := checkSettings(conn, s.full); err != nil {
		return nil, err
	}
	return c, nil
}

func (s *startup) readers(path string, count int) ([]*Conn, error) {
	conns := make([]*Conn, count)
	conn, err := s.open(path, "ro")
	if err != nil {
		return nil, err
	}
	if err := conn.prepare(false); err != nil {
		return nil, err
	}
	conns[0] = conn
	db := s.databases[len(s.databases)-1]
	for i := 1; i < count; i++ {
		conn, err := s.connect(db)
		if err != nil {
			return nil, err
		}
		if err := conn.prepare(false); err != nil {
			return nil, err
		}
		conns[i] = conn
	}
	return conns, nil
}

// closeConnections also serves the memory-mode handoff to the RAM copy.
func (s *startup) closeConnections() (err error) {
	for _, conn := range slices.Backward(s.conns) {
		if closeErr := conn.close(); err == nil {
			err = closeErr
		}
	}
	for _, db := range s.databases {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}
	s.conns, s.databases = nil, nil
	return err
}

func (s *startup) close() error {
	err := s.closeConnections()
	if s.memoryPath != "" {
		err = errors.Join(err, removeDatabase(s.memoryPath))
	}
	return errors.Join(err, releaseLocks(s.locks))
}

const (
	synchronousNormal = 1
	synchronousFull   = 2
)

// checkSettings confirms the driver applied the connection string. SQLite
// may silently keep a different journal mode when WAL is unavailable.
func checkSettings(conn *sql.Conn, full bool) error {
	var journal string
	err := conn.QueryRowContext(context.Background(), `PRAGMA journal_mode`).Scan(&journal)
	if err != nil {
		return fmt.Errorf("read journal_mode: %w", err)
	}
	if journal != "wal" {
		return fmt.Errorf("journal_mode is %q, expected WAL", journal)
	}
	var synchronous int
	err = conn.QueryRowContext(context.Background(), `PRAGMA synchronous`).Scan(&synchronous)
	if err != nil {
		return fmt.Errorf("read synchronous: %w", err)
	}
	want, name := synchronousFull, "FULL"
	if !full {
		want, name = synchronousNormal, "NORMAL"
	}
	if synchronous != want {
		return fmt.Errorf("synchronous is %d, expected %s (%d)", synchronous, name, want)
	}
	return nil
}
