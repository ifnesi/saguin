package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	msqlite "modernc.org/sqlite"

	"github.com/ifnesi/saguin/internal/store"
)

// countedPrepares is what the counting driver has prepared since it was
// registered.
var countedPrepares atomic.Int64

var registerCounting sync.Once

// countingDriver is modernc's driver with every statement preparation
// counted. Its connection does not offer database/sql the ExecerContext and
// QueryerContext shortcuts, so a text run as text has to be prepared through
// PrepareContext too, and is counted there - which is the cost the cache
// exists to remove.
type countingDriver struct{ d driver.Driver }

type fullConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
}

func (c countingDriver) Open(name string) (driver.Conn, error) {
	conn, err := c.d.Open(name)
	if err != nil {
		return nil, err
	}
	return countingConn{conn.(fullConn)}, nil
}

type countingConn struct{ c fullConn }

func (c countingConn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}
func (c countingConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	countedPrepares.Add(1)
	return c.c.PrepareContext(ctx, q)
}
func (c countingConn) Close() error { return c.c.Close() }
func (c countingConn) Begin() (driver.Tx, error) {
	return c.c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c countingConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.c.BeginTx(ctx, o)
}

// A constant SQL text is prepared once for the provider's life, whatever runs
// it and however often: inside a transaction through tx.Stmt, or on its own
// (DB.stmt). A second round of the same operations, a hundred times over,
// prepares nothing more.
//
// **This is the claim the cache rests on**, and it is database/sql's rather
// than this package's: tx.Stmt answers with the driver statement already
// prepared on the transaction's connection. Measured before the cache: the
// same text through tx.Exec was prepared on every call, 100 for 100.
func TestAStatementIsPreparedOnceWhateverRunsIt(t *testing.T) {
	registerCounting.Do(func() { sql.Register("sqlite-counting", countingDriver{&msqlite.Driver{}}) })
	driverName = "sqlite-counting"
	t.Cleanup(func() { driverName = "sqlite" })
	db := open(t, filepath.Join(t.TempDir(), "s.db"))
	t.Cleanup(func() { _ = db.Close() })
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	s, err := db.Sessions()
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	round := func(n int) {
		t.Helper()
		for i := range n {
			if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/a", Payload: []byte("x")}); err != nil {
				t.Fatalf("append: %v", err)
			}
			if _, err := lg.ReadFromN(1, 10); err != nil {
				t.Fatalf("read: %v", err)
			}
			if err := lg.SavePosition(store.Position{Reader: store.MQTTReader("c"), Offset: 1, LastSeen: time.Now()}); err != nil {
				t.Fatalf("save position: %v", err)
			}
			if err := s.Save(store.Session{Client: "c", ExpiryInterval: uint32(60 + i)}); err != nil {
				t.Fatalf("save session: %v", err)
			}
			if _, _, err := s.Get("c"); err != nil {
				t.Fatalf("get session: %v", err)
			}
		}
	}
	round(3) // every text met, and prepared by the transaction after it
	before := countedPrepares.Load()
	round(100)
	if got := countedPrepares.Load() - before; got != 0 {
		t.Errorf("a hundred rounds of operations already run prepared %d statements more, want 0: "+
			"a text is being parsed again", got)
	}
	if before == 0 {
		t.Fatal("the counting driver counted nothing, so the provider did not open through it and this proves nothing")
	}
}

// Every SQL text in this package runs through the statement cache when it
// is a constant, and cannot when it is not (DB.stmt):
//
//   - a call of DB.exec, query or queryRow passes a compile-time constant,
//     so the cache holds one entry per text written here and cannot grow
//     with what the broker is sent (invariant 13);
//   - no constant single-statement text is handed to database/sql as text,
//     where it would be parsed again on every call. A text holding more
//     than one statement is the exception by what it is - Prepare takes a
//     text's first statement only - and so is SQL built at runtime, which
//     is counted and named rather than cached.
func TestEverySQLTextIsConstantOrBuilt(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
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
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("github.com/ifnesi/saguin/internal/store/sqlite", fset, files, info); err != nil {
		t.Fatalf("type-check this package: %v", err)
	}
	const self = "*github.com/ifnesi/saguin/internal/store/sqlite.DB"
	constantText := func(e ast.Expr) (string, bool) {
		if v, ok := info.Types[e]; ok && v.Value != nil && v.Value.Kind() == constant.String {
			return constant.StringVal(v.Value), true
		}
		return "", false
	}
	var cached, built, multi int
	var builtAt []string
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				tv, ok := info.Types[sel.X]
				if !ok || tv.Type == nil {
					return true
				}
				recv, at := tv.Type.String(), fset.Position(call.Pos())
				switch {
				case recv == self && (sel.Sel.Name == "exec" || sel.Sel.Name == "query" || sel.Sel.Name == "queryRow"):
					if _, ok := constantText(call.Args[1]); !ok {
						t.Errorf("%s: %s passes the statement cache a text that is not a constant, so "+
							"the cache could grow with it", at, fd.Name.Name)
					}
					cached++
				case recv == "*database/sql.Tx" || recv == "*database/sql.DB":
					switch sel.Sel.Name {
					case "Exec", "Query", "QueryRow", "ExecContext", "QueryContext", "QueryRowContext":
					default:
						return true
					}
					arg := call.Args[0]
					if strings.HasSuffix(sel.Sel.Name, "Context") && len(call.Args) > 1 {
						arg = call.Args[1]
					}
					text, ok := constantText(arg)
					switch {
					case !ok:
						built++
						builtAt = append(builtAt, fd.Name.Name)
					case strings.Count(strings.TrimRight(strings.TrimSpace(text), ";"), ";") > 0:
						multi++
					default:
						t.Errorf("%s: %s runs a constant SQL text as text, so it is parsed again on "+
							"every call: put it through d.exec, d.query or d.queryRow", at, fd.Name.Name)
					}
				}
				return true
			})
		}
	}
	t.Logf("%d calls through the statement cache; %d SQL texts built at runtime (%s); %d multi-statement texts",
		cached, built, strings.Join(builtAt, ", "), multi)
	if cached < 80 {
		t.Fatalf("found %d calls through the statement cache, and this package has over 80: the walk "+
			"has stopped seeing them", cached)
	}
}

// **Each of Remove's four statements is prepared once**, however often it
// runs: a text parsed on every call is the cost the statements replaced.
func TestEveryRemoveStatementIsPreparedOnce(t *testing.T) {
	registerCounting.Do(func() { sql.Register("sqlite-counting", countingDriver{&msqlite.Driver{}}) })
	driverName = "sqlite-counting"
	t.Cleanup(func() { driverName = "sqlite" })
	db := open(t, filepath.Join(t.TempDir(), "s.db"))
	t.Cleanup(func() { _ = db.Close() })
	lg, err := db.Broadcast()
	if err != nil {
		t.Fatalf("broadcast log: %v", err)
	}
	round := func() {
		t.Helper()
		for _, n := range []int{1, 7, 60, 400} { // one for each statement
			var offs []uint64
			for i := range n {
				r, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", n, "-", i), Topic: "events/a", Payload: []byte("x"), QoS: 1})
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				offs = append(offs, r.Offset)
			}
			if removed, _, err := lg.Remove(offs...); err != nil || removed != n {
				t.Fatalf("remove %d: %d (%v)", n, removed, err)
			}
		}
	}
	round()
	round() // every text met, and prepared by the transaction after it
	before := countedPrepares.Load()
	for range 5 {
		round()
	}
	if got := countedPrepares.Load() - before; got != 0 {
		t.Errorf("five more rounds of removals at every statement size prepared %d statements, want 0", got)
	}
	if before == 0 {
		t.Fatal("the counting driver counted nothing, so the provider did not open through it and this proves nothing")
	}
}
