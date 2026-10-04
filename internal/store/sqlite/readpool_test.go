package sqlite

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// fiveRecords is a channel holding offsets 1 to 5, a minute apart, on a
// provider with the default read pool.
func fiveRecords(t *testing.T) (*DB, *Log, time.Time) {
	t.Helper()
	return fiveRecordsWith(t, store.SQLiteReadConnections)
}

// fiveRecordsWith is fiveRecords on a provider opened with read_connections
// n.
func fiveRecordsWith(t *testing.T, n int) (*DB, *Log, time.Time) {
	t.Helper()
	db, err := OpenBoundedWithReads(tempPath(t), "0.1.0-test", 0, 0, n)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	now := time.Unix(1770000000, 0).UTC()
	for i := range 5 {
		if _, err := lg.Append(at(now.Add(time.Duration(i)*time.Minute), "0123456789")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return db, lg, now
}

// **A read is one snapshot** (readTx). A trim commits in the middle of a read,
// between the floor it checks and the rows it fetches. Read from one snapshot,
// the rows are the ones that were there with that floor - offsets 1 to 5, the
// read served in full. Read from two, the rows would be the survivors, 3 to 5,
// handed to a consumer at offset 1 as though it had missed nothing: the
// failure invariant 1 refuses. The next read, a new snapshot, is refused.
func TestAReadIsOneSnapshot(t *testing.T) {
	_, lg, now := fiveRecords(t)
	trimmed := false
	betweenFloorAndRows = func() {
		if trimmed {
			return
		}
		trimmed = true
		if removed, _, err := lg.Trim(now.Add(2*time.Minute), 0); err != nil || removed != 2 {
			t.Errorf("the trim inside the read removed %d, %v; want 2", removed, err)
		}
	}
	t.Cleanup(func() { betweenFloorAndRows = nil })

	recs, err := lg.ReadFromN(1, 0)
	if !trimmed {
		t.Fatal("the trim never ran inside the read, so this proves nothing")
	}
	if err != nil {
		t.Fatalf("the read around the trim returned %v", err)
	}
	var offs []uint64
	for _, r := range recs {
		offs = append(offs, r.Offset)
	}
	if len(offs) != 5 || offs[0] != 1 {
		t.Errorf("a read that checked floor 1 returned offsets %v, want 1 to 5: the rows came from "+
			"a different snapshot than the floor", offs)
	}
	if _, err := lg.ReadFromN(1, 0); !errors.Is(err, store.ErrBelowFloor) {
		t.Errorf("the next read from offset 1 returned %v, want ErrBelowFloor: the trim did commit", err)
	}
}

// **Every read sees every commit before it** - a fresh snapshot each time,
// never one held across reads - so a consumer woken by an append finds it,
// and a writer reading back its own write finds that.
func TestEveryReadSeesEveryCommitBeforeIt(t *testing.T) {
	_, lg, now := fiveRecords(t)
	for i := 0; i < 20; i++ {
		recs, err := lg.ReadFromN(1, 0)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if want := 5 + i; len(recs) != want {
			t.Fatalf("read %d returned %d records, want %d: it did not see the append before it", i, len(recs), want)
		}
		if _, err := lg.Append(at(now.Add(time.Hour+time.Duration(i)*time.Second), "x")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

// **The point of the pool**: a channel read proceeds while a write
// transaction holds the write connection and SQLite's write lock. On the one
// connection it waited for the writer to finish.
func TestAChannelReadDoesNotWaitForTheWriter(t *testing.T) {
	db, lg, _ := fiveRecords(t)
	hold, err := db.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := hold.Exec(`UPDATE meta SET value = value WHERE key = 'writer'`); err != nil {
		t.Fatalf("take the write lock: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		recs, err := lg.ReadFromN(1, 0)
		if err == nil && len(recs) != 5 {
			err = errors.New("wrong count")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("the read beside the writer: %v", err)
		}
		_ = hold.Rollback()
	case <-time.After(5 * time.Second):
		t.Error("a channel read waited 5s for a write transaction it has no part in")
		_ = hold.Rollback()
		<-done
	}
}

// **No pure read runs inside a write transaction**, by syntax, in the style
// of the write-lock sweep: a read that feeds a write stays on the write
// connection. Every function literal handed to DB.tx, reliefTx or runBatch
// is walked for a call of readTx or of a read-pool method (ReadFromN,
// ReadFrom); so is every function taking a *sql.Tx. And readTx is called from
// exactly the functions this test names, so a new caller is a decision
// somebody makes here rather than one that happens.
func TestNoPureReadRunsInsideAWriteTransaction(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package: %v", err)
	}
	pool := map[string]bool{"readTx": true, "ReadFromN": true, "ReadFrom": true}
	callers := map[string]bool{}
	var walked int
	check := func(where string, body ast.Node) {
		walked++
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && pool[sel.Sel.Name] {
				t.Errorf("%s: %s runs on the read pool inside a write transaction",
					fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			for _, p := range fd.Type.Params.List {
				if star, ok := p.Type.(*ast.StarExpr); ok {
					if sel, ok := star.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "Tx" && fd.Name.Name != "readTx" {
						check(fd.Name.Name, fd.Body)
					}
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "tx", "reliefTx", "runBatch":
					for _, a := range call.Args {
						if lit, ok := a.(*ast.FuncLit); ok {
							check(fd.Name.Name, lit.Body)
						}
					}
				case "readTx":
					callers[fd.Name.Name] = true
				}
				return true
			})
		}
	}
	t.Logf("%d write transactions and *sql.Tx functions walked; readTx called from %v", walked, callers)
	if walked < 30 {
		t.Fatalf("only %d write transactions walked: the sweep is not finding them", walked)
	}
	if want := map[string]bool{"ReadFromN": true, "Get": true, "All": true, "ShareReturned": true, "Position": true}; !maps.Equal(callers, want) {
		t.Errorf("readTx is called from %v, want %v: a new pure read is named here", callers, want)
	}
}

// read_connections 0 opens no pool - not one connection, not even to
// prepare on - and a channel read runs on the write connection, behind a
// writer: what a small box chooses.
func TestNoReadConnectionsPutsReadsBehindTheWriter(t *testing.T) {
	db, lg, _ := fiveRecordsWith(t, 0)
	if db.reads != nil {
		t.Fatal("read_connections 0 opened a read pool")
	}
	hold, err := db.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := hold.Exec(`UPDATE meta SET value = value WHERE key = 'writer'`); err != nil {
		t.Fatalf("take the write lock: %v", err)
	}
	// **Queued for the write connection, seen rather than waited out.** The
	// write pool has one connection and the transaction above holds it, so
	// a read that uses it waits inside database/sql, which counts the wait
	// (WaitCount); one that returns first, or never waits, read beside the
	// writer.
	waits := db.db.Stats().WaitCount
	done := make(chan error, 1)
	go func() {
		recs, err := lg.ReadFromN(1, 0)
		if err == nil && len(recs) != 5 {
			err = errors.New("wrong count")
		}
		done <- err
	}()
	for deadline := time.Now().Add(5 * time.Second); db.db.Stats().WaitCount == waits; time.Sleep(time.Millisecond) {
		select {
		case err := <-done:
			_ = hold.Rollback()
			t.Fatalf("with no read connections a read returned beside the writer (%v): it did not use the write connection", err)
		default:
		}
		if time.Now().After(deadline) {
			_ = hold.Rollback()
			t.Fatal("with no read connections a read never queued for the write connection in 5s")
		}
	}
	_ = hold.Rollback()
	if err := <-done; err != nil {
		t.Fatalf("the read, once the writer was done: %v", err)
	}
}

// read_connections n is the most reads that run at once: with n held inside
// their snapshots, one more waits for a connection, and with one to spare it
// does not.
func TestReadConnectionsIsTheMostReadsAtOnce(t *testing.T) {
	for _, n := range []int{1, 3} {
		db, lg, _ := fiveRecordsWith(t, n)
		release := make(chan struct{})
		inside := make(chan struct{}, 8)
		betweenFloorAndRows = func() {
			inside <- struct{}{}
			<-release
		}
		t.Cleanup(func() { betweenFloorAndRows = nil })
		errs := make(chan error, n+1)
		for range n {
			go func() { _, err := lg.ReadFromN(1, 0); errs <- err }()
		}
		for range n {
			select {
			case <-inside:
			case <-time.After(5 * time.Second):
				t.Fatalf("read_connections %d: fewer than %d reads got a connection", n, n)
			}
		}
		// The one more queues for a connection, which database/sql counts
		// (WaitCount), rather than reaching the read: seen, not waited out.
		waits := db.reads.Stats().WaitCount
		go func() { _, err := lg.ReadFromN(1, 0); errs <- err }()
		for deadline := time.Now().Add(5 * time.Second); db.reads.Stats().WaitCount == waits; time.Sleep(time.Millisecond) {
			select {
			case <-inside:
				t.Fatalf("read_connections %d: read %d got a connection while %d were held", n, n+1, n)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("read_connections %d: read %d never queued for a connection in 5s", n, n+1)
			}
		}
		select {
		case <-inside:
			t.Fatalf("read_connections %d: read %d got a connection while %d were held", n, n+1, n)
		default:
		}
		close(release)
		for range n + 1 {
			if err := <-errs; err != nil {
				t.Fatalf("read_connections %d: %v", n, err)
			}
		}
		betweenFloorAndRows = nil
	}
}

// **Every statement a read runs on the pool was prepared there at open**
// (pureReads), by syntax. readTx hands its function the statements the pool
// prepared, looked up by their text; a text nobody added to pureReads has no
// statement, and the read fails on the delivery path - found by a consumer
// rather than by a test. Preparing it there instead would ask the pool for a
// second connection while the read holds one, which with every connection in
// a read waits for ever. So every call of the statement argument inside a
// function literal given to readTx must name a constant pureReads lists.
func TestEveryPureReadIsNamed(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package: %v", err)
	}
	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files = append(files, f)
	}
	named := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "pureReads" || len(vs.Values) != 1 {
				return true
			}
			if lit, ok := vs.Values[0].(*ast.CompositeLit); ok {
				for _, el := range lit.Elts {
					if id, ok := el.(*ast.Ident); ok {
						named[id.Name] = true
					}
				}
			}
			return true
		})
	}
	if len(named) == 0 {
		t.Fatal("found no pureReads list, so nothing below is checked against it")
	}
	var bodies, calls int
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "readTx" || len(call.Args) != 1 {
				return true
			}
			lit, ok := call.Args[0].(*ast.FuncLit)
			if !ok {
				t.Errorf("%s: readTx is given something other than a function literal, which this "+
					"sweep cannot read", fset.Position(call.Pos()))
				return true
			}
			bodies++
			params := lit.Type.Params.List
			if len(params) != 2 || len(params[1].Names) != 1 {
				t.Errorf("%s: readTx's function does not take (tx, stmt)", fset.Position(lit.Pos()))
				return true
			}
			stmt := params[1].Names[0].Name
			ast.Inspect(lit.Body, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if id, ok := c.Fun.(*ast.Ident); !ok || id.Name != stmt {
					return true
				}
				calls++
				arg, ok := c.Args[0].(*ast.Ident)
				if !ok || !named[arg.Name] {
					t.Errorf("%s: a read on the pool runs %s, which pureReads does not list: it has no "+
						"statement there", fset.Position(c.Pos()), exprName(c.Args[0]))
				}
				return true
			})
			return false
		})
	}
	t.Logf("%d readTx bodies, %d statements run in them, %d texts in pureReads", bodies, calls, len(named))
	if bodies == 0 || calls < len(named) {
		t.Fatalf("%d readTx bodies and %d statements found: the sweep is not finding the reads", bodies, calls)
	}
}

// exprName names an expression for a failure message.
func exprName(e ast.Expr) string {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return "an expression rather than a named constant"
}

// **Nothing sent down a read connection can write** (query_only, in the
// connection string as the writer's pragmas are). The pool is for reads a
// client is sent; a write reaching it would be a second writer beside the
// one every transaction in this package assumes is alone.
func TestNothingOnAReadConnectionCanWrite(t *testing.T) {
	db, _, _ := fiveRecords(t)
	if db.reads == nil {
		t.Fatal("the provider opened no read pool, so nothing below is asked of one")
	}
	var n int
	if err := db.reads.QueryRow(`SELECT count(*) FROM records`).Scan(&n); err != nil || n != 5 {
		t.Fatalf("a read on the pool: %d records, %v; want 5 - the pool is not reading this file", n, err)
	}
	_, err := db.reads.Exec(`UPDATE meta SET value = value WHERE key = 'writer'`)
	if err == nil {
		t.Fatal("an UPDATE on a read connection succeeded: the pool can write")
	}
	if !strings.Contains(err.Error(), "readonly") && !strings.Contains(err.Error(), "query_only") {
		t.Errorf("an UPDATE on a read connection failed for another reason than being read-only: %v", err)
	}
}

// **Close closes the read connections too**, before its TRUNCATE checkpoint.
// SQLite removes the write-ahead log and its shared-memory index when the
// last connection to the file closes, so both still on disk after Close is a
// connection left open on a provider the broker has let go of - and its lock
// released, so a second process could take the file while this one held it.
func TestCloseLeavesNoConnectionOnTheFile(t *testing.T) {
	db, lg, _ := fiveRecords(t)
	if _, err := lg.ReadFromN(1, 0); err != nil {
		t.Fatalf("a read, so the pool has a connection open: %v", err)
	}
	path := db.path
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatalf("no write-ahead log while the provider is open (%v), so its absence below proves nothing", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err == nil {
			t.Errorf("%s is still on disk after Close: a connection to the file is still open", path+suffix)
		}
	}
}

// **A read racing Close fails, and does not panic.** The broker closes its
// providers at shutdown with consumers perhaps still being fed; a read then
// must fail as a write does - "database is closed" - and never find the
// pool half taken apart under it. Run under -race it is also the check that
// Close writes nothing a read reads unlocked.
func TestAReadRacingCloseFailsCleanly(t *testing.T) {
	for trial := range 20 {
		db, lg, _ := fiveRecords(t)
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-start
			for range 200 {
				if _, err := lg.ReadFromN(1, 0); err != nil {
					return
				}
			}
		}()
		close(start)
		if err := db.Close(); err != nil {
			t.Fatalf("trial %d: close: %v", trial, err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("trial %d: a read racing Close never returned", trial)
		}
		if _, err := lg.ReadFromN(1, 0); err == nil {
			t.Fatalf("trial %d: a read after Close succeeded", trial)
		}
	}
}
