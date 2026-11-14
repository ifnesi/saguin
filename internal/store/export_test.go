package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"testing"

	"github.com/mattn/go-sqlite3"
)

// The seam these tests need in the code:
//
//	var openDSN = databaseDSN // the connection string of every connection Start keeps
//
// startup.open builds its connection string with openDSN, so a test can have
// a connection ask for other settings, which no file_path selects, while the
// driver still opens it, and only Start's read-back of journal_mode and
// synchronous stands between that connection and the store.

// AskFor makes the connections Start keeps, for the rest of t, ask in their
// connection string for value of the driver's setting key: the readers'
// connections where readers is set, else the writer's. An empty value asks
// for nothing, leaving SQLite's own default.
func AskFor(t *testing.T, readers bool, key, value string) {
	t.Helper()
	saved := openDSN
	t.Cleanup(func() { openDSN = saved })
	openDSN = func(path, mode string, wal bool) string {
		dsn := saved(path, mode, wal)
		if (mode == "ro") != readers {
			return dsn
		}
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("the store's connection string %q: %v", dsn, err)
		}
		q := u.Query()
		if value == "" {
			q.Del(key)
		} else {
			q.Set(key, value)
		}
		u.RawQuery = q.Encode()
		return u.String()
	}
}

// StatementSQL returns the SQL the store prepares for the statement its
// table names name, and whether the table holds one: a test explains the
// store's own statements, never a query of its own.
func StatementSQL(name string) (string, bool) {
	for _, spec := range &statementSQL {
		if spec.name == name {
			return spec.sql, true
		}
	}
	return "", false
}

// Counted runs w in one transaction of its own on the database at path, as
// the store writer runs a write: on a connection with the writer's settings
// (databaseDSN) and its prepared statements (Conn.prepare). The connection
// is the test's own, through countingConnector, which counts each
// statement the write runs and each row they return; Counted commits and
// returns those counts. No store may be running on path.
func Counted(t *testing.T, path string, w Write) (runs, rows int) {
	t.Helper()
	counts := &statementCounts{}
	db := sql.OpenDB(countingConnector{dsn: databaseDSN(path, "rw", true), counts: counts})
	defer func() { _ = db.Close() }() // the counts are read before
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = conn.Close() }()
	c := &Conn{Conn: conn}
	if err := c.prepare(true); err != nil {
		t.Fatalf("preparing the writer's statements on %s: %v", path, err)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	*counts = statementCounts{} // preparing ran nothing of the write's
	if err := w.WriteTx(&Tx{Tx: tx, statements: &c.statements}); err != nil {
		_ = tx.Rollback() // the error is the test's failure
		t.Fatalf("%T on %s: %v", w, path, err)
	}
	runs, rows = counts.runs, counts.rows
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing %T on %s: %v", w, path, err)
	}
	return runs, rows
}

// statementCounts is what countingConnector's connections count: the
// statements run, queries and executions alike, and the rows queries
// returned. Where fault is set, they also fail at fault in the faulted'th
// statement run (from 0), and fired notes that the failure was given.
type statementCounts struct {
	runs, rows int
	faulted    int
	fault      ReadFault
	fired      bool
}

// ErrInjected is the failure Faulted's connection gives.
var ErrInjected = errors.New("the driver failed, as the test injected")

// ReadFault is where Faulted's connection fails, in one statement a read
// runs: as the statement starts, at a call of Next, in a row's values,
// which no Scan can take, or as its rows close, before or at their end.
// database/sql keeps a close error at their end too, for Rows.Err.
type ReadFault uint8

// The faults Faulted injects; NoFault injects none.
const (
	NoFault ReadFault = iota
	FaultQuery
	FaultNext
	FaultScan
	FaultClose
)

// unscannable is a row's value no Scan can store: FaultScan's.
type unscannable struct{}

// query runs a statement through run, counting it and wrapping its rows,
// and fails it where it is the faulted statement.
func (c *statementCounts) query(run func() (driver.Rows, error)) (driver.Rows, error) {
	faulted := c.fault != NoFault && c.runs == c.faulted
	c.runs++
	if faulted && c.fault == FaultQuery {
		c.fired = true
		return nil, ErrInjected
	}
	rows, err := run()
	if err != nil {
		return nil, err
	}
	r := &countingRows{Rows: rows, counts: c}
	if faulted {
		r.fault = c.fault
	}
	return r, nil
}

// Faulted runs read op as a store reader runs it (design.md, Store), on a
// connection of its own to the database at path, with a reader's settings
// (databaseDSN, read-only) and the readers' prepared statements
// (Conn.prepare), whose driver fails at fault in the query'th statement op
// runs, counted from 0. It returns how many statements op ran, whether the
// fault was given, and op's error.
func Faulted(t *testing.T, path string, op Read, query int, fault ReadFault) (queries int, fired bool, err error) {
	t.Helper()
	counts := &statementCounts{}
	db := sql.OpenDB(countingConnector{dsn: databaseDSN(path, "ro", true), counts: counts})
	defer func() { _ = db.Close() }() // the counts are read before
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = conn.Close() }()
	c := &Conn{Conn: conn}
	if err := c.prepare(false); err != nil {
		t.Fatalf("preparing the readers' statements on %s: %v", path, err)
	}
	*counts = statementCounts{faulted: query, fault: fault} // preparing ran nothing of the read's
	err = op.ReadConn(c)
	return counts.runs, counts.fired, err
}

// countingConnector opens SQLite connections, as the driver the store
// registers does, whose prepared statements count into counts.
type countingConnector struct {
	dsn    string
	counts *statementCounts
}

func (c countingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return countingConn{conn.(*sqlite3.SQLiteConn), c.counts}, nil
}

func (countingConnector) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }

// countingConn is a SQLite connection counting the statements it runs,
// prepared or not.
type countingConn struct {
	*sqlite3.SQLiteConn
	counts *statementCounts
}

func (c countingConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.SQLiteConn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return countingStmt{stmt.(*sqlite3.SQLiteStmt), c.counts}, nil
}

func (c countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.counts.runs++
	return c.SQLiteConn.ExecContext(ctx, query, args)
}

func (c countingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.counts.query(func() (driver.Rows, error) { return c.SQLiteConn.QueryContext(ctx, query, args) })
}

// countingStmt is a prepared statement counting its runs and its rows.
type countingStmt struct {
	*sqlite3.SQLiteStmt
	counts *statementCounts
}

func (s countingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.counts.runs++
	return s.SQLiteStmt.ExecContext(ctx, args)
}

func (s countingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.counts.query(func() (driver.Rows, error) { return s.SQLiteStmt.QueryContext(ctx, args) })
}

// countingRows counts each row a query returns, and fails at fault, the
// faulted statement's, where it is set.
type countingRows struct {
	driver.Rows
	counts *statementCounts
	fault  ReadFault
}

func (r *countingRows) Next(dest []driver.Value) error {
	if r.fault == FaultNext {
		r.counts.fired = true
		return ErrInjected
	}
	err := r.Rows.Next(dest)
	if err == nil {
		r.counts.rows++
		if r.fault == FaultScan {
			r.counts.fired = true
			for i := range dest {
				dest[i] = unscannable{}
			}
		}
	}
	return err
}

func (r *countingRows) Close() error {
	err := r.Rows.Close()
	if r.fault == FaultClose {
		r.counts.fired = true
		return errors.Join(ErrInjected, err)
	}
	return err
}
