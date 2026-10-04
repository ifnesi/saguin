package sqlite

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"
	"testing"
)

// packageDecls parses this package's non-test files and indexes every
// function and method by name: a name borne by several is all of them, so a
// walk through a call follows each it could be.
func packageDecls(t *testing.T) (*token.FileSet, []*ast.File, map[string][]*ast.FuncDecl) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package: %v", err)
	}
	var files []*ast.File
	decls := map[string][]*ast.FuncDecl{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files = append(files, f)
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				decls[fd.Name.Name] = append(decls[fd.Name.Name], fd)
			}
		}
	}
	return fset, files, decls
}

// callName is the name a call is made by: the function's, or the method's.
func callName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// typeCheckThisPackage parses and type-checks this package's non-test files,
// so that a call is the function the type checker resolved rather than a
// name: rows.Close is not DB.Close, and holders.Remove is not Log.Remove.
func typeCheckThisPackage(t *testing.T) (*token.FileSet, []*ast.File, *types.Info) {
	t.Helper()
	fset, files, _ := packageDecls(t)
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	var typeErrs []string
	conf := types.Config{
		Importer: importer.ForCompiler(fset, "source", nil),
		Error:    func(err error) { typeErrs = append(typeErrs, err.Error()) },
	}
	if _, err := conf.Check("github.com/ifnesi/saguin/internal/store/sqlite", fset, files, info); err != nil {
		t.Fatalf("type-check this package: %v\n\t%s", err, strings.Join(typeErrs, "\n\t"))
	}
	return fset, files, info
}

// calleeOf is the function or method a call resolves to, nil for a call of a
// function value.
func calleeOf(call *ast.CallExpr, info *types.Info) *types.Func {
	var id *ast.Ident
	switch f := call.Fun.(type) {
	case *ast.Ident:
		id = f
	case *ast.SelectorExpr:
		id = f.Sel
	default:
		return nil
	}
	fn, _ := info.Uses[id].(*types.Func)
	return fn
}

// **What a client write runs, it runs holding the write lock, the session
// store's lock and the write connection** (runWrites) - three things nothing
// else may take while waiting for another of them. So nothing a write's
// check, run, apply or again reaches may:
//
//   - take a lock: the two above are not re-entrant, and any other is a
//     second order to meet;
//   - reach DB.Log, DB.Broadcast, EnsureChannel, logNext or DB.Sessions,
//     which take the provider's d.mu or its write lock - the broadcast log's
//     next offset is read by the caller before it joins;
//   - join a group, lead one, open a transaction, or read on the read pool;
//   - run a statement on no transaction - exec, query or queryRow given nil,
//     or a *sql.DB itself - which asks for the one write connection the
//     leader is holding, and waits for ever.
//
// Every composite literal of write is found, each of its functions followed
// through every function of this package it calls, as the type checker
// resolves them, and what the walk examined counted. The statement helpers
// are judged by the transaction they are given rather than walked.
func TestNoClientWriteTakesALockOrReadsOffItsTransaction(t *testing.T) {
	fset, files, info := typeCheckThisPackage(t)
	decls := map[*types.Func]*ast.FuncDecl{}
	locals := map[types.Object]*ast.FuncLit{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			if fn, ok := info.Defs[fd.Name].(*types.Func); ok {
				decls[fn] = fd
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Lhs) != len(as.Rhs) {
				return true
			}
			for i, l := range as.Lhs {
				id, ok := l.(*ast.Ident)
				lit, isLit := as.Rhs[i].(*ast.FuncLit)
				if !ok || !isLit {
					continue
				}
				if obj := info.Defs[id]; obj != nil {
					locals[obj] = lit
				} else if obj := info.Uses[id]; obj != nil {
					locals[obj] = lit
				}
			}
			return true
		})
	}
	forbidden := map[string]string{
		"Log": "reaches DB.Log, which takes d.mu and the write lock", "Broadcast": "reaches DB.Broadcast",
		"EnsureChannel": "takes the write lock", "logNext": "reaches DB.Log",
		"Sessions":  "reaches DB.Sessions, which takes d.mu",
		"joinWrite": "joins a group from inside one", "runWrites": "leads a group from inside one",
		"alone": "opens a transaction", "attempt": "opens a transaction", "tx": "opens a transaction",
		"reliefTx": "opens a transaction", "runBatch": "opens a transaction", "store": "stores a publish",
		"storeBehind": "stores a publish", "readTx": "reads on the read pool",
		"Get": "reads off its transaction", "All": "reads off its transaction",
		"InFlight": "reads off its transaction", "get": "reads off its transaction",
		"all": "reads off its transaction", "Begin": "begins a session from inside a write",
		"Drop": "ends a session from inside a write", "Save": "saves a session from inside a write",
	}
	judged := map[string]bool{"exec": true, "query": true, "queryRow": true, "note": true, "notedOn": true,
		"stmt": true, "failure": true, "setCeiling": true, "prepareText": true}
	here := "github.com/ifnesi/saguin/internal/store/sqlite"

	var members, walked int
	seen := map[*ast.FuncDecl]bool{}
	var walk func(where string, body ast.Node)
	walk = func(where string, body ast.Node) {
		walked++
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok {
				if lit := locals[info.Uses[id]]; lit != nil {
					walk(where+" → "+id.Name, lit.Body)
				}
			}
			fn := calleeOf(call, info)
			if fn == nil || fn.Pkg() == nil {
				return true
			}
			switch path := fn.Pkg().Path(); {
			case path == "sync" && strings.HasSuffix(fn.Name(), "Lock"):
				t.Errorf("%s: %s takes a lock (%s)", fset.Position(call.Pos()), where, fn.FullName())
			case path == "database/sql" && strings.HasPrefix(fn.FullName(), "(*database/sql.DB)."):
				t.Errorf("%s: %s runs %s, a statement on no transaction", fset.Position(call.Pos()), where,
					fn.FullName())
			case path == here:
				if why, bad := forbidden[fn.Name()]; bad {
					t.Errorf("%s: %s calls %s, which %s", fset.Position(call.Pos()), where, fn.FullName(), why)
					return true
				}
				if fn.Name() == "exec" || fn.Name() == "query" || fn.Name() == "queryRow" {
					if id, ok := call.Args[0].(*ast.Ident); ok && id.Name == "nil" {
						t.Errorf("%s: %s runs a statement on no transaction", fset.Position(call.Pos()), where)
					}
				}
				if judged[fn.Name()] {
					return true
				}
				if fd := decls[fn]; fd != nil && !seen[fd] {
					seen[fd] = true
					walk(where+" → "+fn.Name(), fd.Body)
				}
			}
			return true
		})
	}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if id, ok := lit.Type.(*ast.Ident); !ok || id.Name != "write" {
				return true
			}
			members++
			for _, el := range lit.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key := kv.Key.(*ast.Ident).Name
				// Data, not code: the store, and what the write is and whose.
				if key == "s" || key == "relief" || key == "answers" || key == "client" || key == "departed" {
					continue
				}
				where := fset.Position(lit.Pos()).String() + " " + key
				switch v := kv.Value.(type) {
				case *ast.FuncLit:
					walk(where, v.Body)
				case *ast.Ident:
					lit := locals[info.Uses[v]]
					if lit == nil {
						t.Errorf("%s: %s is %s, which this walk cannot follow", fset.Position(v.Pos()), where, v.Name)
						continue
					}
					walk(where, lit.Body)
				case *ast.CallExpr:
					walk(where, v)
				default:
					t.Errorf("%s: %s is an expression this walk cannot follow", fset.Position(v.Pos()), where)
				}
			}
			return true
		})
	}
	t.Logf("%d client writes found, %d bodies walked", members, walked)
	if members < 18 || walked < 30 { // the session store's thirteen, and holds and positions
		t.Fatalf("found %d client writes and walked %d bodies: the sweep is not finding them", members, walked)
	}
}

// **The session store's lock is taken by a group's leader, or around memory
// alone** (runWrites). Anything else holding it while it asks for the write
// connection - which the leader holds, and waits for this lock under - is a
// cycle. So every function taking it is either runWrites, or makes no call
// that reaches the database.
func TestTheSessionStoresLockIsTakenOnlyByTheLeaderOrAroundMemory(t *testing.T) {
	fset, files, _ := packageDecls(t)
	db := map[string]bool{"exec": true, "query": true, "queryRow": true, "tx": true, "reliefTx": true,
		"joinWrite": true, "readTx": true, "Begin": true, "get": true, "all": true, "InFlight": true,
		"attempt": true, "Exec": true, "Query": true, "QueryRow": true}
	var sites []string
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			takes := false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || callName(call) != "Lock" {
					return true
				}
				sel := call.Fun.(*ast.SelectorExpr)
				mu, ok := sel.X.(*ast.SelectorExpr)
				if !ok || mu.Sel.Name != "mu" {
					return true
				}
				// s.mu on a *Sessions receiver, or w.s.mu in a group's leader.
				if x, ok := mu.X.(*ast.SelectorExpr); ok && x.Sel.Name == "s" {
					takes = true
				}
				if id, ok := mu.X.(*ast.Ident); ok && fd.Recv != nil && id.Name == fd.Recv.List[0].Names[0].Name {
					if star, ok := fd.Recv.List[0].Type.(*ast.StarExpr); ok {
						if tn, ok := star.X.(*ast.Ident); ok && tn.Name == "Sessions" {
							takes = true
						}
					}
				}
				return true
			})
			if !takes {
				continue
			}
			sites = append(sites, fd.Name.Name)
			if fd.Name.Name == "runWrites" {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && db[callName(call)] {
					t.Errorf("%s: %s takes the session store's lock and calls %s, which reaches the "+
						"database", fset.Position(call.Pos()), fd.Name.Name, callName(call))
				}
				return true
			})
		}
	}
	sort.Strings(sites)
	t.Logf("the session store's lock is taken in %v", sites)
	if len(sites) < 2 || sort.SearchStrings(sites, "runWrites") == len(sites) || sites[sort.SearchStrings(sites, "runWrites")] != "runWrites" {
		t.Fatalf("found the lock taken in %v, without runWrites: the sweep is not finding it", sites)
	}
}
