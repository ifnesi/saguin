package sqlite

// The rule about this package, in the shape the two rules about the whole
// tree already take: a check over the source rather than a list of the
// sites that were right on the day somebody looked.
//
// **What it is about.** A channel's `next` and `bytes` live in two places -
// the channels row, and a copy in memory that every publish reads to decide
// the next offset and whether the channel is full. Two writers of that row
// that do not exclude each other will write one another's totals: a
// retention sweep that removes 40 records between a batch reserving its
// offsets and the transaction storing them writes its decrement, and the
// batch then writes a total it worked out before the sweep ran. Nothing
// about the records looks wrong afterwards. The byte count is a column, so
// the channel refuses publishes against room it is not using for the rest
// of that database's life, and no restart recomputes it.
//
// **Why this is a check and not a test.** The window is microseconds wide.
// A test that publishes into a running sweep hits it seldom enough that it
// passed five runs out of five with the lock removed - which is a test that
// passes against its own defect, and worse than none. The rule is a
// property of the source, so it is checked in the source.
//
// It is an ordinary test rather than a Makefile target so that it cannot be
// skipped by somebody who runs the suite and not the Makefile.

import (
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// SQL that writes the channels table - the row holding a channel's next
// offset, floor and byte count.
//
// **Which prepared statements those are is discovered rather than listed.**
// An earlier version of this named `bump` and `sweep`, which is the list its
// own header says it must not be: a third statement prepared tomorrow, or a
// raw tx.Exec, wrote that row invisibly. Now anything whose SQL matches this
// counts, wherever it is written.
var writesChannelRow = regexp.MustCompile(`(?is)(update|insert|delete)[^;]*\bchannels\b`)

func TestEveryTransactionWritingAChannelsCountersHoldsTheWriteLock(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}

	// Pass one: the fields holding a statement that writes the channels
	// row, found by reading the SQL they are prepared from. Both shapes in
	// this package are covered - `l.bump, err = d.db.Prepare(...)` and the
	// `{&l.sweep, "..."}` entries of the table it prepares in a loop -
	// because both put the field and its text in one node.
	counterStatements := map[string]bool{}
	sqlWritesRow := func(e ast.Expr) bool {
		lit, ok := e.(*ast.BasicLit)
		return ok && lit.Kind == token.STRING && writesChannelRow.MatchString(lit.Value)
	}
	// Named on the left of the assignment, or beside the text in the table.
	// Deliberately narrow: an earlier attempt took every selector anywhere
	// inside a node holding such a string, and the statement table is one
	// composite literal, so one channels row entry made `insert`, `remove`,
	// `count` and `trim` counter statements too and three innocent
	// functions were reported.
	nameFields := func(exprs []ast.Expr) {
		for _, e := range exprs {
			if u, ok := e.(*ast.UnaryExpr); ok {
				e = u.X
			}
			if sel, ok := e.(*ast.SelectorExpr); ok {
				counterStatements[sel.Sel.Name] = true
			}
		}
	}
	collect := func(n ast.Node) {
		switch v := n.(type) {
		case *ast.AssignStmt:
			for _, r := range v.Rhs {
				if call, ok := r.(*ast.CallExpr); ok {
					for _, a := range call.Args {
						if sqlWritesRow(a) {
							nameFields(v.Lhs)
						}
					}
				}
			}
		case *ast.CompositeLit:
			for _, e := range v.Elts {
				if sqlWritesRow(e) {
					nameFields(v.Elts)
				}
			}
		}
	}

	type fn struct {
		name    string
		where   string
		writes  bool   // executes bump or sweep itself
		opaque  string // a place it runs SQL this check cannot read; "" when there is none
		opensTx bool   // calls db.tx or reliefTx, so the transaction is its own
		holds   bool   // takes DB.writing
		takesTx bool   // is handed a *sql.Tx, so it writes inside somebody else's
		calls   map[string]bool
	}
	var funcs []*fn

	// **Every file parsed before any function is judged.** The statement
	// set has to be complete first: collecting it during the same walk that
	// uses it means a function read before the statement it runs was
	// defined is measured against a set that does not hold it yet. It
	// happened to work here only because of the order the files and the
	// declarations in them happen to be in.
	var parsed []*ast.File
	files := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files++
		parsed = append(parsed, file)
	}

	for _, file := range parsed {
		// A const or a var holding such SQL, which is the ordinary way to
		// hold it in Go and so the likeliest shape of the three. A function
		// naming the identifier writes the row as surely as one with the
		// text inline.
		for _, decl := range file.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && (gen.Tok == token.CONST || gen.Tok == token.VAR) {
				for _, spec := range gen.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, v := range vs.Values {
						if sqlWritesRow(v) {
							for _, name := range vs.Names {
								counterStatements[name.Name] = true
							}
						}
					}
				}
			}
		}
		// And the prepared statements, wherever in the package they are.
		ast.Inspect(file, func(n ast.Node) bool { collect(n); return true })
	}

	// **Pass one and a half: what the SQL actually says, where it is
	// assembled rather than written.**
	//
	// The passes above read string literals, so `tx.Exec("UPDATE " + table +
	// " SET next = 1")` is invisible to them - the fifth shape found in this
	// check, and the one left open longest because the obvious fix was
	// worse. Flagging any assembled SQL inside a transaction would flag
	// Latest.Trim, which builds a DELETE against latest_values and has
	// nothing to do with a channel's counters; exempting it would put back
	// the exemption list this check exists not to be.
	//
	// **Type information answers it without an exemption**, two ways at
	// once. Go folds a constant expression, so where the pieces are
	// constants - which is what Latest.Trim's are - the checker hands over
	// the finished SQL and it can simply be read: it says latest_values, so
	// there is nothing to flag and nothing to excuse. And where the pieces
	// are not constant the SQL is genuinely unknowable here, so the
	// function holding it has to answer for it rather than be trusted.
	//
	// It also settles which arguments are SQL at all. Without types
	// `tx.Exec(sql, args…)` and `stmt.Exec(args…)` are the same shape and a
	// check would read a device id as a statement; with them, the receiver
	// says which is which. That is why this needs a type-checked package
	// rather than a parse, and it is done with the standard library only -
	// go/importer in source mode resolves this package's imports without
	// adding a dependency to the module.
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	var typeErrs []string
	conf := types.Config{
		Importer: importer.ForCompiler(fset, "source", nil),
		Error:    func(err error) { typeErrs = append(typeErrs, err.Error()) },
	}
	if _, err := conf.Check("github.com/ifnesi/saguin/internal/store/sqlite", fset, parsed, info); err != nil {
		t.Fatalf("type-check this package: %v\n\t%s", err, strings.Join(typeErrs, "\n\t"))
	}
	// **The third counter this check owes.** A type check that resolved
	// nothing would leave every receiver unknown, every SQL argument
	// unreadable and - because unknown SQL is what the rule below is about
	// - either flag the whole package or, if the lookups silently miss,
	// flag none of it. Neither is a result.
	if len(info.Types) < 1000 {
		t.Fatalf("the type checker returned %d typed expressions for this package, which cannot be "+
			"right: the pass below reads receivers and constants out of this and would be "+
			"measuring nothing", len(info.Types))
	}

	// A call that runs SQL through a *sql.Tx or a *sql.DB, and what the SQL
	// says. Returns whether this is such a call at all, the folded SQL, and
	// whether the checker could fold it.
	sqlThrough := func(call *ast.CallExpr) (isSQL bool, text string, known bool) {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || len(call.Args) == 0 {
			return false, "", false
		}
		tv, ok := info.Types[sel.X]
		if !ok || tv.Type == nil {
			return false, "", false
		}
		recv := tv.Type.String()
		var arg ast.Expr
		switch {
		// **The provider's own statement cache runs most of its SQL**
		// (DB.exec, query, queryRow: the SQL second, after the transaction or
		// nil), so a pass reading only database/sql's methods would read
		// almost none of it and pass.
		case recv == "*github.com/ifnesi/saguin/internal/store/sqlite.DB" &&
			(sel.Sel.Name == "exec" || sel.Sel.Name == "query" || sel.Sel.Name == "queryRow") && len(call.Args) > 1:
			arg = call.Args[1]
		case recv == "*database/sql.Tx" || recv == "*database/sql.DB":
			switch sel.Sel.Name {
			case "Exec", "ExecContext", "Query", "QueryContext", "QueryRow", "QueryRowContext", "Prepare", "PrepareContext":
			default:
				return false, "", false
			}
			// The SQL is the first argument, or the second where a Context
			// comes first.
			arg = call.Args[0]
			if strings.HasSuffix(sel.Sel.Name, "Context") && len(call.Args) > 1 {
				arg = call.Args[1]
			}
		default:
			return false, "", false
		}
		if v, ok := info.Types[arg]; ok && v.Value != nil && v.Value.Kind() == constant.String {
			return true, constant.StringVal(v.Value), true
		}
		return true, "", false
	}

	for _, file := range parsed {
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			f := &fn{
				name:  fd.Name.Name,
				where: fset.Position(fd.Pos()).String(),
				calls: map[string]bool{},
			}
			for _, p := range fd.Type.Params.List {
				star, ok := p.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if sel, ok := star.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "Tx" {
					f.takesTx = true
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.Ident:
					// A const or var holding channel-row SQL, named.
					if counterStatements[v.Name] {
						f.writes = true
					}
					// **Named is enough; it does not have to be called.**
					// Propagation used to read only the function of a call,
					// so a writer handed to db.tx *by value* - `d.tx(l.someTx)`,
					// which is the next thing somebody writes when the
					// closure is one line - propagated to nobody. A mention
					// is a reference, and a reference is enough to have to
					// answer for it.
					f.calls[v.Name] = true
				case *ast.SelectorExpr:
					f.calls[v.Sel.Name] = true
				case *ast.BasicLit:
					// Raw SQL inside a function, prepared or not: if it
					// writes the channels row then this function writes it,
					// whatever it runs the statement through.
					if v.Kind == token.STRING && writesChannelRow.MatchString(v.Value) {
						f.writes = true
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				// Assembled SQL, read for what it says rather than guessed
				// at. Where the checker folded it, the text is exact -
				// better evidence than the literal passes above, which see
				// only one piece of it. Where it could not, this function
				// runs SQL nobody here can read.
				if isSQL, text, known := sqlThrough(call); isSQL {
					switch {
					case known && writesChannelRow.MatchString(text):
						f.writes = true
					case !known && f.opaque == "":
						f.opaque = fset.Position(call.Pos()).String()
					}
				}
				// **A call with no receiver is a call.** This returned
				// here when the function was not a method, so
				// `importChannel(tx, …)` was never recorded and DB.Import,
				// which writes the channels row through it, was invisible.
				// Propagation that follows only methods follows half a
				// package.
				if id, ok := call.Fun.(*ast.Ident); ok {
					f.calls[id.Name] = true
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "Stmt":
					// tx.Stmt(l.bump) - which statement is being run.
					for _, a := range call.Args {
						if s, ok := a.(*ast.SelectorExpr); ok && counterStatements[s.Sel.Name] {
							f.writes = true
						}
					}
				case "tx", "reliefTx":
					f.opensTx = true
				case "Lock":
					// d.writing.Lock(), however the DB is reached.
					if s, ok := sel.X.(*ast.SelectorExpr); ok && s.Sel.Name == "writing" {
						f.holds = true
					}
				default:
					f.calls[sel.Sel.Name] = true
				}
				return true
			})
			funcs = append(funcs, f)
		}
	}

	// **The check has to prove it looked.** The first version of the log
	// guard elsewhere in this tree walked the wrong directory, scanned two
	// files, found nothing and passed - a green tick over a rule it never
	// applied. This package has four source files and has never had fewer.
	if files < 4 {
		t.Fatalf("read %d source files in this package, which cannot be right", files)
	}

	// A helper that writes a counter row inside a transaction it was handed
	// makes its callers writers too: the transaction is theirs, so the lock
	// is theirs to take. **Taking a *sql.Tx is what identifies such a
	// helper**, rather than its name - an earlier version of this matched
	// names, and `Queue.Release` made the provider lock's own `Release` a
	// writer, which made `Close` one, which made every function that closes
	// a row of results one. A parameter type cannot collide that way.
	// **To a fixed point, not for one hop.** The first version marked the
	// callers of a writer that takes a *sql.Tx and stopped, so a helper one
	// step further along the chain - a function taking a *sql.Tx that calls
	// appendTx - was itself marked but never made anybody else a writer. A
	// walk went straight through the gap. The chain is short today and
	// this loop is cheap; what matters is that its length is not assumed.
	for round := 0; ; round++ {
		handed := map[string]bool{}
		for _, f := range funcs {
			if f.writes && f.takesTx {
				handed[f.name] = true
			}
		}
		grew := false
		for _, f := range funcs {
			if f.writes {
				continue
			}
			for name := range f.calls {
				if handed[name] {
					f.writes = true
					grew = true
					break
				}
			}
		}
		if !grew {
			break
		}
		if round > len(funcs) {
			t.Fatal("the propagation did not settle, which cannot happen unless it is wrong")
		}
	}

	// **The second counter this check owes.** If the SQL ever moves out of
	// this package's source - into a generated file, or a constant block
	// this walk does not reach - the discovery above finds nothing, every
	// function looks innocent, and the whole check passes over a rule it
	// never applied.
	if len(counterStatements) < 2 {
		t.Fatalf("found %d prepared statements writing the channels row (%v), and this package has "+
			"always had at least two: the check is not finding them, so nothing below means anything",
			len(counterStatements), counterStatements)
	}
	t.Logf("statements writing the channels row, discovered from their SQL: %v", counterStatements)

	// runBatch is the one transaction that writes counter rows without
	// naming the statements itself: the publishes it carries brought their
	// own. It is checked by name because its role is what makes it
	// exceptional, and a rule with an exception nobody asserts is a rule
	// with a hole in it.
	var batch *fn
	for _, f := range funcs {
		if f.name == "runBatch" {
			batch = f
		}
	}
	switch {
	case batch == nil:
		t.Error("runBatch is gone: the check below no longer covers the batch write path")
	case !batch.holds:
		t.Errorf("%s: runBatch runs a transaction full of publishes and does not take DB.writing",
			batch.where)
	}

	// **SQL this check could not read, inside a transaction of this
	// function's own.** It may write a channel's counters and there is no
	// way to tell from here, so it answers the same question as one that
	// provably does: take the lock, or stop assembling the statement out of
	// something the source does not settle. No site in this package is in
	// this position today - the one that assembles SQL inside a transaction
	// builds it from constants, so it was read exactly and cleared - and
	// this exists so that the next one has to be argued for rather than
	// merely written.
	opaqueInTx := 0
	for _, f := range funcs {
		if f.opaque == "" || !f.opensTx || f.holds {
			continue
		}
		opaqueInTx++
		t.Errorf("%s: %s opens a transaction and runs SQL this check cannot read (%s), so it may be "+
			"writing a channel's counter row.\n\tEither take DB.writing first, or build the "+
			"statement from constants so that what it does is settled by the source rather than "+
			"by whoever calls it.", f.where, f.name, f.opaque)
	}
	if opaqueInTx == 0 {
		t.Logf("no transaction in this package runs SQL that cannot be read from the source")
	}

	checked := 0
	for _, f := range funcs {
		if !f.writes || !f.opensTx {
			continue
		}
		checked++
		if !f.holds {
			t.Errorf("%s: %s opens a transaction that writes a channel's counter row and does not take "+
				"DB.writing first.\n\tTake it before the channel's own lock. Two writers of that row "+
				"write one another's totals, and the byte count is a column: the channel then refuses "+
				"publishes against room it is not using, for the life of the database, with nothing "+
				"anywhere saying why.", f.where, f.name)
		}
	}
	// The floor is what stops this passing on a sweep that has stopped
	// finding anything: a check that matched nothing would otherwise report
	// success. It was 7 while `AppendAt` and `SetAt` existed - the two
	// offset-addressed writes a channel holding a copy of another broker's
	// used - and moves with them rather than being left to catch a real
	// disappearance months later.
	if checked < 5 {
		t.Fatalf("found %d transactions writing a channel's counter row across %d files, which is fewer "+
			"than this package has ever had - the check is not finding them", checked, files)
	}
	t.Logf("%d transactions write a channel's counter row, across %d source files", checked, files)
}
