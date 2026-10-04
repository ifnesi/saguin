package broker

// The order of saguin's broker-wide lock and a client's own lock, checked
// in the source.
//
// **b.positions, then b.mu, then a client's cmu - never b.mu while cmu is
// held.** A client's lock was split out of b.mu so that one consumer's
// pumping and acknowledgements stop queueing behind every other consumer's.
// That makes two locks where there was one, and two locks taken in opposite
// orders by two goroutines is a broker that stops, for good, with nothing
// logged. The rule is that b.mu may be held when cmu is taken, and never
// the other way round.
//
// **Why the source and not a test that runs it.** A deadlock needs two
// goroutines to meet in exactly the wrong place, which a test reaches by
// luck. What has to be true is that no critical section of cmu can reach
// b.mu at all, however it is scheduled, and only the source can say that:
// every function in the package that takes b.mu, directly or by calling one
// that does, is found, and every section holding cmu is checked for a call
// to any of them.
//
// **It asks the type checker rather than matching names, and that is the
// whole of this file's history.** Written on 2026-09-22 alongside the lock
// split itself, it indexed functions by bare name - `funcs[fn.Name.Name]`,
// one entry per name - and a package with two functions of a name kept only
// the second. Measured on 2026-09-23: nine names in this package are borne
// by two functions each, and three of them hid a b.mu-taker outright -
// BridgeClient.Position, BridgeClient.ReadFrom and BridgeClient.SavePosition
// each lost their slot to countingLog's methods of the same name, because
// storageerrors.go sorts after broker.go in the directory read. The guard's
// inventory was wrong, which of two duplicates won was decided by filename
// ordering, and the test stayed green throughout. It was caught by diffing
// the printed count against a baseline after an unrelated change took it
// from 150 to 148 - which is not a thing anybody does routinely, and is why
// a green tick here had been overstating what it covered for a day.
//
// Two shapes were tried against that and both died on measurement. Treating
// a name as reaching b.mu if ANY function of that name does took the set
// from 150 to 168 and raised five violations in a clean tree, every one an
// artefact of a name shared with an unrelated method; and the fix for those
// would be renames, which is not available because the colliding names -
// Position, ReadFrom, SavePosition, Set, Trim - are interface methods that
// LogStore, LatestStore and bridge.Reader require. Failing the build on a
// collision dies on the same fact.
//
// So a function is the *types.Func the type checker resolved, a call is the
// callee it resolved to, and the locks are the fields Broker.mu and
// consumer.cmu rather than anything spelled `mu` or `cmu`. Collisions
// cannot exist. It also retires a rule nobody should have had to follow: a
// method here used to need a name and a mutex field no other type had used,
// which is how `add` came not to be `record` and `rowsMu` not to be `mu`.
//
// **A failure to type-check fails this test rather than skipping it.** An
// instrument that cannot resolve the package cannot judge it, and one that
// says nothing in that case is the blind guard this file exists to stop
// being.
//
// It walks syntax trees, not text, and counts what it examined.

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

func TestNoClientLockSectionReachesTheBrokerLock(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)

	brokerMu := lockField(t, pkg, "Broker", "mu")
	consumerCmu := lockField(t, pkg, "consumer", "cmu")

	// Every function in the package, by the identity the type checker gives
	// it rather than by its name.
	funcs := map[*types.Func]*ast.FuncDecl{}
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if obj, ok := info.Defs[fn.Name].(*types.Func); ok {
				funcs[obj] = fn
			}
		}
	}

	// Which functions take b.mu, directly or through a function that does.
	takes := map[*types.Func]bool{}
	for obj, fn := range funcs {
		if locksField(fn.Body, info, brokerMu) {
			takes[obj] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for obj, fn := range funcs {
			if takes[obj] {
				continue
			}
			for _, callee := range calleesIn(fn.Body, info) {
				if takes[callee] {
					takes[obj], changed = true, true
					break
				}
			}
		}
	}

	sections := 0
	for obj, fn := range funcs {
		walkSections(fn.Body, info, consumerCmu, func(section []ast.Stmt, at token.Pos) {
			sections++
			for _, s := range section {
				if locksField(s, info, brokerMu) {
					t.Errorf("%s: %s takes b.mu while holding a client's cmu",
						fset.Position(at), obj.Name())
				}
				for _, callee := range calleesIn(s, info) {
					if takes[callee] {
						t.Errorf("%s: %s calls %s while holding a client's cmu, and %s takes b.mu",
							fset.Position(at), obj.Name(), callee.FullName(), callee.FullName())
					}
				}
			}
		})
	}

	t.Logf("%d functions take b.mu; %d sections holding cmu examined; %d functions indexed",
		len(takes), sections, len(funcs))
	if len(takes) < 20 || sections == 0 {
		t.Fatalf("found %d functions taking b.mu and %d sections holding cmu, which cannot be "+
			"this package: the walk has stopped matching the code it guards", len(takes), sections)
	}
}

// **The property the rewrite bought, asserted rather than described.** Two
// functions of one name must both be in the index; under the bare-name map
// this file used to keep, the second silently replaced the first and the
// walk stopped knowing the name reached anything.
//
// The pair is found in the package rather than written down, so this cannot
// rot into a check about two functions somebody later renames. It fails
// either way it can go wrong: if no name is shared any more, it says so
// rather than passing over a package it no longer describes.
func TestTheLockWalkHoldsBothFunctionsOfADuplicatedName(t *testing.T) {
	_, info, _, files := typeCheckPackage(t)

	byName := map[string][]*types.Func{}
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if obj, ok := info.Defs[fn.Name].(*types.Func); ok {
				byName[obj.Name()] = append(byName[obj.Name()], obj)
			}
		}
	}
	shared := 0
	for name, objs := range byName {
		if len(objs) < 2 {
			continue
		}
		shared++
		seen := map[*types.Func]bool{}
		for _, o := range objs {
			if seen[o] {
				t.Errorf("two declarations of %q resolved to one function, so the index "+
					"cannot tell them apart", name)
			}
			seen[o] = true
		}
	}
	if shared == 0 {
		t.Fatal("no name in this package is borne by two functions, so this test compared " +
			"nothing. It exists because nine such names hid three b.mu-takers from the walk " +
			"above; if that is genuinely no longer true, this test has no subject and should " +
			"be removed rather than left passing over an empty set")
	}
	t.Logf("%d names are borne by more than one function, and the walk holds each separately", shared)
}

// typeCheckPackage resolves the package's non-test files. **A failure here
// is fatal rather than a skip**: an instrument that cannot resolve what it
// guards is blind, and a blind instrument reporting success is what this
// file was rewritten to stop.
func typeCheckPackage(t *testing.T) (*types.Package, *types.Info, *token.FileSet, []*ast.File) {
	t.Helper()
	return typeCheckDir(t, ".", "github.com/ifnesi/saguin/internal/broker")
}

// typeCheckDir is typeCheckPackage for the package in dir, whose import path
// is path.
func typeCheckDir(t *testing.T, dir, path string) (*types.Package, *types.Info, *token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatal("no non-test source file was read, so this walk examined nothing")
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	pkg, err := conf.Check(path, fset, files, info)
	if err != nil {
		t.Fatalf("type-checking the package failed, so this guard cannot judge it: %v", err)
	}
	return pkg, info, fset, files
}

// qualifiedFuncName is a function's receiver type and its name, which is
// what tells two same-named methods apart without type-checking a package.
//
// **Nine names in this package are borne by two functions.** Any check that
// keys a map by `fn.Name.Name` alone therefore has one entry where it
// believes it has two - silently, and with which one survives decided by
// the order the directory is read. This file's own walk resolves that
// properly with the type checker because it has to follow calls; a check
// that only needs to tell declarations apart can have the same safety for
// the cost of a string.
func qualifiedFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var b strings.Builder
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			b.WriteString("(*" + id.Name + ")")
		}
	case *ast.Ident:
		b.WriteString("(" + t.Name + ")")
	}
	if b.Len() == 0 {
		return fn.Name.Name
	}
	return b.String() + "." + fn.Name.Name
}

// lockField is one named type's mutex field, as the type checker sees it.
// Asked for by type and field rather than by the field's name alone,
// because three types in this package have a field called `mu` - Broker,
// publishAllowance and BridgeClient - and only one of them is the lock this
// rule is about.
func lockField(t *testing.T, pkg *types.Package, typeName, field string) *types.Var {
	t.Helper()
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		t.Fatalf("%s is not in this package, so the walk cannot find the lock it guards", typeName)
	}
	st, ok := obj.Type().Underlying().(*types.Struct)
	if !ok {
		t.Fatalf("%s is not a struct", typeName)
	}
	for i := range st.NumFields() {
		if f := st.Field(i); f.Name() == field {
			return f
		}
	}
	t.Fatalf("%s has no field %q: this guard is watching a lock that has moved", typeName, field)
	return nil
}

// locksField reports whether a node takes the given mutex field itself -
// `<expr>.<field>.Lock()` resolved to that exact field - outside any
// function literal it contains, since a goroutine started there does not
// hold what its creator holds.
func locksField(n ast.Node, info *types.Info, field *types.Var) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if isFieldLock(n, info, field, "Lock") {
			found = true
		}
		return !found
	})
	return found
}

// locksFieldHeld is locksField for what a node does while its caller still
// holds what it holds: the function literals run in place are looked in too -
// a callback such as sync.Map's Range runs before the caller returns - and
// the ones calleesHeld leaves out are left out here: a goroutine's, and one
// handed to time.AfterFunc or AfterSessionUnlock.
func locksFieldHeld(n ast.Node, info *types.Info, field *types.Var) bool {
	found := false
	after := map[*ast.FuncLit]bool{}
	ast.Inspect(n, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			return false
		case *ast.FuncLit:
			return !after[x]
		case *ast.CallExpr:
			if c := calleeOf(x, info); c != nil && (c.Name() == "AfterSessionUnlock" ||
				c.Name() == "afterSessionUnlock" || c.FullName() == "time.AfterFunc") {
				for _, a := range x.Args {
					if lit, ok := a.(*ast.FuncLit); ok {
						after[lit] = true
					}
				}
			}
		}
		if isFieldLock(n, info, field, "Lock") {
			found = true
		}
		return !found
	})
	return found
}

// isFieldLock reports whether n is a call of method on the given field.
func isFieldLock(n ast.Node, info *types.Info, field *types.Var, method string) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != method {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	s := info.Selections[inner]
	return s != nil && s.Obj() == field
}

// calleesIn is the functions a node calls, as the type checker resolved
// them, outside any function literal. A call through an interface resolves
// to the interface's method, which is the right answer: it is what the
// caller can reach, and reading it as every concrete implementation of that
// name is the over-approximation that raised five false violations when it
// was tried.
func calleesIn(n ast.Node, info *types.Info) []*types.Func {
	var out []*types.Func
	ast.Inspect(n, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var id *ast.Ident
		switch f := call.Fun.(type) {
		case *ast.Ident:
			id = f
		case *ast.SelectorExpr:
			id = f.Sel
		}
		if id != nil {
			if fn, ok := info.Uses[id].(*types.Func); ok {
				out = append(out, fn)
			}
		}
		return true
	})
	return out
}

// walkSections calls visit with every run of statements that holds a
// client's cmu: from `x.cmu.Lock()` to the matching `x.cmu.Unlock()` in the
// same block, or to the end of the block when the unlock is deferred or
// sits inside a nested one.
func walkSections(body *ast.BlockStmt, info *types.Info, field *types.Var,
	visit func([]ast.Stmt, token.Pos)) {
	ast.Inspect(body, func(n ast.Node) bool {
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, s := range block.List {
			es, ok := s.(*ast.ExprStmt)
			if !ok || !isFieldLock(es.X, info, field, "Lock") {
				continue
			}
			end := len(block.List)
			for j := i + 1; j < len(block.List); j++ {
				if es2, ok := block.List[j].(*ast.ExprStmt); ok &&
					isFieldLock(es2.X, info, field, "Unlock") {
					end = j
					break
				}
			}
			visit(block.List[i+1:end], s.Pos())
		}
		return true
	})
}

// TestLatestPositionsAreReachedOnlyThroughTheirMethods holds the property
// latestPositions was moved off b.mu on: every read-modify-write of a
// position is one hold of posMu, because the three fields of a position are
// advanced by different paths and two of them updating it under different
// locks would lose one update.
//
// **Go does not enforce that on its own.** The map is unexported, but this
// file is in the same package, and so is every site that used to reach it
// under b.mu - `b.latestSeen.by[id]` compiles anywhere here. So the source is
// walked: the map, the lock and the unlocked accessor `at` may be named only
// inside a method of latestPositions.
func TestLatestPositionsAreReachedOnlyThroughTheirMethods(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	obj := pkg.Scope().Lookup("latestPositions")
	if obj == nil {
		t.Fatal("latestPositions is not in this package, so there is nothing to guard - " +
			"if it has been removed, remove this test with it")
	}
	named := obj.Type().(*types.Named)
	guarded := map[types.Object]bool{}
	st := named.Underlying().(*types.Struct)
	for i := range st.NumFields() {
		guarded[st.Field(i)] = true
	}
	for i := range named.NumMethods() {
		if m := named.Method(i); m.Name() == "at" {
			guarded[m] = true
		}
	}
	if len(guarded) != 3 {
		t.Fatalf("expected the map, its lock and at to be guarded, found %d; the type has "+
			"changed shape and this test must be re-read against it", len(guarded))
	}

	isOwn := func(fn *ast.FuncDecl) bool {
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return false
		}
		rt := info.Defs[fn.Name].(*types.Func).Type().(*types.Signature).Recv().Type()
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}
		return types.Identical(rt, named)
	}

	inside, outside := 0, 0
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			own := isOwn(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				s := info.Selections[sel]
				if s == nil || !guarded[s.Obj()] {
					return true
				}
				if own {
					inside++
					return true
				}
				outside++
				t.Errorf("%s: %s names latestPositions.%s directly, outside the type's own "+
					"methods - a position updated that way is not one hold of posMu",
					fset.Position(sel.Pos()), fn.Name.Name, sel.Sel.Name)
				return true
			})
		}
	}
	// The methods themselves reach the map and the lock, so an inventory of
	// zero means the walk resolved nothing and its silence above is blindness.
	if inside == 0 {
		t.Fatal("no use of the map, the lock or at was found inside the type's own " +
			"methods, so this walk resolved nothing and cannot vouch for the rest")
	}
	t.Logf("%d uses inside latestPositions' methods, %d outside", inside, outside)
}

// TestLatestPositionsLockIsALeaf holds the other half of what the type's
// comment claims: posMu may be taken under b.mu, under a consumer's cmu or
// under neither only because nothing is acquired while it is held.
//
// **So a method may call nothing but the lock itself, its own type's methods,
// latestPos's, and builtins.** Another lock taken inside would be an order
// this file's other walk does not see; a function value or a logger called
// inside could take anything. And no method but the unlocked accessor may
// hand back something that refers into the map - a caller iterating a map or
// a pointer returned from under posMu reads it with posMu released, which the
// test above cannot see because the selection happened inside a method.
func TestLatestPositionsLockIsALeaf(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	named := pkg.Scope().Lookup("latestPositions").Type().(*types.Named)
	pos := pkg.Scope().Lookup("latestPos").Type().(*types.Named)
	posMu := lockField(t, pkg, "latestPositions", "posMu")

	recvOf := func(f *types.Func) types.Type {
		r := f.Type().(*types.Signature).Recv()
		if r == nil {
			return nil
		}
		rt := r.Type()
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}
		return rt
	}

	methods, calls := 0, 0
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			obj := info.Defs[fn.Name].(*types.Func)
			if rt := recvOf(obj); rt == nil || !types.Identical(rt, named) {
				continue
			}
			methods++
			if fn.Name.Name != "at" {
				res := obj.Type().(*types.Signature).Results()
				for i := range res.Len() {
					if _, basic := res.At(i).Type().Underlying().(*types.Basic); !basic {
						t.Errorf("%s: latestPositions.%s returns %s, which can refer into the "+
							"map after posMu is released - return a copy of a plain value",
							fset.Position(fn.Pos()), fn.Name.Name, res.At(i).Type())
					}
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				calls++
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if _, ok := info.Uses[fun].(*types.Builtin); ok {
						return true
					}
				case *ast.SelectorExpr:
					if s := info.Selections[fun]; s != nil {
						if callee, ok := s.Obj().(*types.Func); ok {
							if isFieldLock(call, info, posMu, "Lock") || isFieldLock(call, info, posMu, "Unlock") {
								return true
							}
							if rt := recvOf(callee); rt != nil &&
								(types.Identical(rt, named) || types.Identical(rt, pos)) {
								return true
							}
						}
					}
				}
				t.Errorf("%s: latestPositions.%s calls %s while it may hold posMu - the lock "+
					"is taken under b.mu and under cmu on the promise that nothing is acquired "+
					"inside it", fset.Position(call.Pos()), fn.Name.Name, types.ExprString(call.Fun))
				return true
			})
		}
	}
	if methods == 0 || calls == 0 {
		t.Fatalf("examined %d methods and %d calls, so this walk judged nothing", methods, calls)
	}
	t.Logf("%d methods of latestPositions and %d calls inside them examined", methods, calls)
}

// Nothing publishes while holding a client id's session lock.
//
// **A publish can wait on another id's session lock.** A fan-out that reaches
// a connection being taken over waits for that connection's lock until the
// takeover is done (the engine's deliveryTarget), so a publish made holding
// one id's lock can wait on another's - and two ids each publishing under
// their own lock to a connection of the other's wait on each other for ever,
// with nothing logged. Nothing that holds one publishes, then: a hook the
// engine calls under it, or a section of the broker's own that takes it,
// hands what it owes to AfterSessionUnlock.
//
// **Which hooks run under it is read from the engine, not written here**:
// every section of the engine holding the lock is walked for the hooks it
// reaches, so a hook moved under the lock is checked without anybody
// remembering to add it. A publish is a call of the engine's InjectPacket,
// Publish or PublishToSubscribers; the engine's own sections are
// checked for its deliveryTarget.
//
// Two sites publish under the lock and are named, each for what it is. They
// are counted, and an entry that no longer matches fails. **Neither closes the
// cycle, by ordering**: each publishes before its holder marks any connection
// taken over - expireSession marks the expired connection after
// OnClientExpired returns, and inheritClientSession marks the replaced one
// after the CONNACK confirmClaim publishes from - so no delivery is waiting on
// that holder's lock for a takeover it is making. The wait deliveryTarget
// makes is for a takeover in progress, and at the moment of the publish its
// holder has none in progress yet. Moving either publish after the marking
// would break that.
//
// A function literal a holder runs in place - deferred, or called back before
// it returns - is followed like any call; one a goroutine, time.AfterFunc or
// AfterSessionUnlock runs is not (calleesHeld).
var publishesUnderASessionLock = map[string]string{
	"(*Broker).OnClientExpired -> (*Broker).publishEndedWill": "an expiry publishes its Will before its record " +
		"goes (RFC 0003 \"Last Will\"). At the sweep that is safe by what an expiring session is: its " +
		"client is gone and nothing is taking the id over, so no delivery waits on its lock. An expiry " +
		"found as the client comes back is on the connect path, safe by the order above: expireSession " +
		"marks the expired connection after this returns",
	"(*Broker).OnPacketSent -> (*Broker).confirmClaim": "a predecessor's Will a claim was holding, published " +
		"as the CONNACK is written, where RFC 0003 moves ownership; safe by the order above: " +
		"inheritClientSession marks the replaced connection after the CONNACK",
}

func TestNothingPublishesUnderASessionLock(t *testing.T) {
	// The engine: the hooks it calls holding the lock.
	epkg, einfo, efset, efiles := typeCheckDir(t, "../mqtt", "github.com/ifnesi/saguin/internal/mqtt")
	efuncs := funcIndex(efiles, einfo)
	takeLock := methodOf(t, epkg, "sessionLocks", "lock")
	takeConnectLock := methodOf(t, epkg, "sessionLocks", "lockConnect")
	deliveryTarget := methodOf(t, epkg, "Server", "deliveryTarget")
	hooks := epkg.Scope().Lookup("Hooks")
	if hooks == nil {
		t.Fatal("the engine has no Hooks type, so the walk cannot tell which hooks it calls")
	}
	isHook := func(fn *types.Func) bool {
		recv := fn.Type().(*types.Signature).Recv()
		if recv == nil {
			return false
		}
		ptr, ok := recv.Type().(*types.Pointer)
		return ok && ptr.Elem() == hooks.Type()
	}

	underLock := map[string]bool{}
	engineSections := 0
	for obj, fn := range efuncs {
		sessionSections(fn.Body, einfo, func(c *types.Func) bool { return c == takeLock || c == takeConnectLock },
			func(section []ast.Stmt, at token.Pos) {
				engineSections++
				walkReach(section, einfo, efuncs, isHook, func(path []*types.Func) {
					last := path[len(path)-1]
					if isHook(last) {
						underLock[last.Name()] = true
					}
					if last == deliveryTarget {
						t.Errorf("%s: %s reaches the engine's deliveryTarget while holding a session "+
							"lock: %s", efset.Position(at), obj.Name(), describePath(path))
					}
				})
			})
	}
	if engineSections < 3 || !underLock["OnSessionEstablish"] || !underLock["OnClientExpired"] ||
		!underLock["OnSessionRegistered"] {
		t.Fatalf("found %d sections of the engine's session lock reaching hooks %v, which cannot be the "+
			"engine: establishClient calls OnSessionEstablish, OnClientExpired and OnSessionRegistered "+
			"holding it", engineSections, sortedKeys(underLock))
	}

	// The broker: every hook of those it answers, and every section of its
	// own holding the lock.
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	publishes := map[string]bool{}
	for _, m := range []string{"InjectPacket", "Publish", "PublishToSubscribers"} {
		publishes["(*github.com/ifnesi/saguin/internal/mqtt.Server)."+m] = true
	}
	isPublish := func(fn *types.Func) bool { return publishes[fn.FullName()] }
	takesLock := func(fn *types.Func) bool {
		return fn.FullName() == "(*github.com/ifnesi/saguin/internal/mqtt.Server).LockSession" ||
			fn == methodOf(t, pkg, "Broker", "lockSession")
	}

	// Every function's path to a publish, where it has one. **Per function
	// and not per walk**: a walk that stops at a function it has already
	// expanded reports one route through it, so a second call from the same
	// place - a new publish behind a named one - would pass unseen.
	toPublish := pathsTo(funcs, info, isPublish)

	// Each call a hook or section makes is checked on its own, and named by
	// the caller and what it calls.
	found := map[string]string{}
	check := func(entry string, stmts []ast.Stmt, at token.Position) {
		for _, s := range stmts {
			for _, c := range calleesHeld(s, info) {
				if path := toPublish[c]; path != nil {
					key := entry + " -> " + qualified(c)
					if _, seen := found[key]; !seen {
						found[key] = fmt.Sprintf("%s: %s -> %s", at, entry, describePath(path))
					}
				}
			}
		}
	}
	entries := 0
	ownSections := map[string]bool{}
	for obj, fn := range funcs {
		if underLock[obj.Name()] && isBrokerMethod(obj) {
			entries++
			check(qualified(obj), fn.Body.List, fset.Position(fn.Pos()))
		}
		sessionSections(fn.Body, info, takesLock, func(section []ast.Stmt, at token.Pos) {
			entries++
			ownSections[qualified(obj)] = true
			check(qualified(obj), section, fset.Position(at))
		})
	}

	for key, where := range found {
		if _, named := publishesUnderASessionLock[key]; !named {
			t.Errorf("%s\npublishes while holding a client id's session lock: hand it to "+
				"AfterSessionUnlock, or name the site here for what it is", where)
		}
	}
	for key := range publishesUnderASessionLock {
		if _, still := found[key]; !still {
			t.Errorf("%q is named as publishing under a session lock and no longer does: remove it", key)
		}
	}
	t.Logf("%d sections of the engine's session lock reach hooks %v; the broker takes it in %v; "+
		"%d hooks and sections examined; %d publishing under the lock, all named", engineSections,
		sortedKeys(underLock), sortedKeys(ownSections), entries, len(found))

	// **What is handed to AfterSessionUnlock is handed by the lock's holder.**
	// Its queue is guarded by the lock itself, so a call from anywhere else
	// races the holder's release and can lose what it queued. A call site
	// holds the lock when it sits in a section taking it, or in a function
	// that only ever runs holding it: a hook the engine calls only under the
	// lock, or a function every caller of which calls it holding it.
	engineHeld := heldOnly(efuncs, einfo, func(c *types.Func) bool { return c == takeLock || c == takeConnectLock }, nil)
	hookEntries := map[*types.Func]bool{}
	var heldHooks []string
	for obj := range efuncs {
		if isHook(obj) && engineHeld[obj] {
			heldHooks = append(heldHooks, obj.Name())
			if m := pkg.Scope().Lookup("Broker"); m != nil {
				if b, _, _ := types.LookupFieldOrMethod(types.NewPointer(m.Type()), true, pkg, obj.Name()); b != nil {
					if fn, ok := b.(*types.Func); ok && isBrokerMethod(fn) {
						hookEntries[fn] = true
					}
				}
			}
		}
	}
	sort.Strings(heldHooks)
	for _, want := range []string{"OnClientExpired", "OnSessionEstablish", "OnSessionRegistered",
		"OnSubscribe", "OnSubscribed", "OnUnsubscribe", "OnUnsubscribed", "OnDisconnecting"} {
		if !slices.Contains(heldHooks, want) {
			t.Errorf("the engine calls %s only under the session lock, and the walk found it called "+
				"otherwise: it has stopped matching the code (hooks found: %v)", want, heldHooks)
		}
	}
	brokerHeld := heldOnly(funcs, info, takesLock, hookEntries)
	// The engine's UnsubscribeClient reaches OnUnsubscribed, so the broker's
	// own calls of it hold the lock too.
	unsubscribes := 0
	for obj, fn := range funcs {
		inSection := callsInSections(fn.Body, info, takesLock)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if c := calleeOf(call, info); c == nil ||
				c.FullName() != "(*github.com/ifnesi/saguin/internal/mqtt.Server).UnsubscribeClient" {
				return true
			}
			unsubscribes++
			if !inSection[call] && !brokerHeld[obj] {
				t.Errorf("%s: %s unsubscribes a client without holding its session lock",
					fset.Position(call.Pos()), qualified(obj))
			}
			return true
		})
	}
	if unsubscribes == 0 {
		t.Error("found no broker call of UnsubscribeClient, which the retention-floor discard makes: " +
			"the walk has stopped matching the code")
	}
	helper := methodOf(t, pkg, "Broker", "afterSessionUnlock")
	queues := 0
	for obj, fn := range funcs {
		if obj == helper {
			continue // the wrapper, checked at each of its callers
		}
		inSection := callsInSections(fn.Body, info, takesLock)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			c := calleeOf(call, info)
			if c == nil || (c != helper &&
				c.FullName() != "(*github.com/ifnesi/saguin/internal/mqtt.Server).AfterSessionUnlock") {
				return true
			}
			queues++
			if !inSection[call] && !brokerHeld[obj] {
				t.Errorf("%s: %s queues for after a session lock it may not hold: only the lock's "+
					"holder may call AfterSessionUnlock", fset.Position(call.Pos()), qualified(obj))
			}
			return true
		})
	}
	t.Logf("the engine calls %v only under the lock; %d call sites queue for after it",
		heldHooks, queues)
	if queues == 0 {
		t.Fatal("found no call queueing for after a session lock, which endSession makes: the walk " +
			"has stopped matching the code")
	}
	for _, want := range []string{"(*Broker).OnDisconnect", "(*Broker).firePendingWill", "(*Broker).OnWill"} {
		if !ownSections[want] {
			t.Errorf("found no section of %s holding the session lock, which it takes: the walk has "+
				"stopped matching the code", want)
		}
	}
}

// funcIndex is every function of a package with a body, by the identity the
// type checker gives it.
func funcIndex(files []*ast.File, info *types.Info) map[*types.Func]*ast.FuncDecl {
	funcs := map[*types.Func]*ast.FuncDecl{}
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if obj, ok := info.Defs[fn.Name].(*types.Func); ok {
				funcs[obj] = fn
			}
		}
	}
	return funcs
}

// methodOf is a named type's method, as the type checker sees it.
func methodOf(t *testing.T, pkg *types.Package, typeName, method string) *types.Func {
	t.Helper()
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		t.Fatalf("%s is not in %s", typeName, pkg.Path())
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, pkg, method)
	fn, ok := m.(*types.Func)
	if !ok {
		t.Fatalf("%s has no method %s: this guard is watching a lock that has moved", typeName, method)
	}
	return fn
}

func isBrokerMethod(fn *types.Func) bool {
	recv := fn.Type().(*types.Signature).Recv()
	if recv == nil {
		return false
	}
	ptr, ok := recv.Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	return ok && named.Obj().Name() == "Broker"
}

// sessionSections calls visit with every run of statements holding a
// session lock: from `unlock := <takes the lock>(...)` to the statement
// calling unlock() in the same block, or to the end of the block where the
// release is deferred or sits in a nested one - which counts a branch that
// releases early as held, erring towards finding more, except for the two
// shapes unheldBranches cuts, whose counts it answers. A lock taken again
// inside a section is a section of its own, found in the code as written.
func sessionSections(body *ast.BlockStmt, info *types.Info, takes func(*types.Func) bool,
	visit func([]ast.Stmt, token.Pos)) (nilChecks, released int) {
	ast.Inspect(body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, s := range block.List {
			as, ok := s.(*ast.AssignStmt)
			if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
				continue
			}
			call, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok {
				continue
			}
			callees := calleesIn(call.Fun, info)
			var fn *types.Func
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				fn, _ = info.Uses[sel.Sel].(*types.Func)
			} else if len(callees) == 1 {
				fn = callees[0]
			}
			if fn == nil || !takes(fn) {
				continue
			}
			lhs, ok := as.Lhs[0].(*ast.Ident)
			if !ok {
				continue
			}
			release := info.Defs[lhs]
			if release == nil {
				release = info.Uses[lhs]
			}
			end := len(block.List)
			for j := i + 1; j < len(block.List); j++ {
				es, ok := block.List[j].(*ast.ExprStmt)
				if !ok {
					continue
				}
				if c, ok := es.X.(*ast.CallExpr); ok {
					if id, ok := c.Fun.(*ast.Ident); ok && info.Uses[id] == release {
						end = j
						break
					}
				}
			}
			section, n, r := unheldBranches(block.List[i+1:end], info, release)
			nilChecks, released = nilChecks+n, released+r
			visit(section, s.Pos())
		}
		return true
	})
	return nilChecks, released
}

// unheldBranches is section without what an if statement in it runs holding
// no lock, keyed on release, the variable the lock call bound: the body of
// `if release == nil`, since a nil release is a lock never taken; and, in a
// body calling release(...) at its own level, the statements after that call.
// The condition, and whatever a body runs before its release, stay held.
func unheldBranches(section []ast.Stmt, info *types.Info, release types.Object) (out []ast.Stmt, nilChecks, released int) {
	isRelease := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && info.Uses[id] == release
	}
	for _, s := range section {
		ifs, ok := s.(*ast.IfStmt)
		if !ok {
			out = append(out, s)
			continue
		}
		if b, ok := ifs.Cond.(*ast.BinaryExpr); ok && b.Op == token.EQL && isRelease(b.X) {
			if id, ok := b.Y.(*ast.Ident); ok && info.Uses[id] == types.Universe.Lookup("nil") {
				cut := *ifs
				cut.Body = &ast.BlockStmt{}
				out, nilChecks = append(out, &cut), nilChecks+1
				continue
			}
		}
		for j, st := range ifs.Body.List {
			if es, ok := st.(*ast.ExprStmt); ok {
				if c, ok := es.X.(*ast.CallExpr); ok && isRelease(c.Fun) {
					cut := *ifs
					cut.Body = &ast.BlockStmt{List: ifs.Body.List[:j]}
					s, released = &cut, released+1
					break
				}
			}
		}
		out = append(out, s)
	}
	return out, nilChecks, released
}

// walkReach follows every call from stmts through the package's functions,
// outside function literals, calling found with the path to each function
// reached. It does not follow past a function stop says to stop at.
func walkReach(stmts []ast.Stmt, info *types.Info, funcs map[*types.Func]*ast.FuncDecl,
	stop func(*types.Func) bool, found func([]*types.Func)) {
	seen := map[*types.Func]bool{}
	var visit func(fn *types.Func, path []*types.Func)
	visit = func(fn *types.Func, path []*types.Func) {
		path = append(path[:len(path):len(path)], fn)
		found(path)
		if seen[fn] || stop(fn) {
			return
		}
		seen[fn] = true
		if decl, ok := funcs[fn]; ok {
			for _, c := range calleesHeld(decl.Body, info) {
				visit(c, path)
			}
		}
	}
	for _, s := range stmts {
		for _, c := range calleesHeld(s, info) {
			visit(c, nil)
		}
	}
}

// heldOnly is, for every function of a package, whether it only ever runs
// holding a session lock: every call of it sits in a section taking one, or in
// a function that itself only runs holding one. entries run holding it by
// what calls them - hooks the engine calls only under the lock - and are
// judged by their calls here too. A function nothing here calls is not
// entered holding the lock, and a call made inside a function literal runs
// whenever that literal does, so it holds nothing.
func heldOnly(funcs map[*types.Func]*ast.FuncDecl, info *types.Info, takes func(*types.Func) bool,
	entries map[*types.Func]bool) map[*types.Func]bool {
	type site struct {
		caller *types.Func // nil for a call inside a function literal
		held   bool
	}
	sites := map[*types.Func][]site{}
	for obj, fn := range funcs {
		inSection := callsInSections(fn.Body, info, takes)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if lit, ok := n.(*ast.FuncLit); ok {
				ast.Inspect(lit.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if c := calleeOf(call, info); c != nil {
							sites[c] = append(sites[c], site{nil, false})
						}
					}
					return true
				})
				return false
			}
			if call, ok := n.(*ast.CallExpr); ok {
				if c := calleeOf(call, info); c != nil {
					sites[c] = append(sites[c], site{obj, inSection[call]})
				}
			}
			return true
		})
	}
	held := map[*types.Func]bool{}
	for obj := range funcs {
		held[obj] = true
	}
	for changed := true; changed; {
		changed = false
		for obj := range funcs {
			if !held[obj] {
				continue
			}
			if len(sites[obj]) == 0 && !entries[obj] {
				held[obj], changed = false, true
				continue
			}
			for _, s := range sites[obj] {
				if !s.held && (s.caller == nil || !held[s.caller]) {
					held[obj], changed = false, true
					break
				}
			}
		}
	}
	return held
}

// callsInSections is every call a function makes inside a section holding a
// session lock, outside function literals.
func callsInSections(body *ast.BlockStmt, info *types.Info, takes func(*types.Func) bool) map[*ast.CallExpr]bool {
	in := map[*ast.CallExpr]bool{}
	sessionSections(body, info, takes, func(section []ast.Stmt, _ token.Pos) {
		for _, s := range section {
			ast.Inspect(s, func(n ast.Node) bool {
				if _, ok := n.(*ast.FuncLit); ok {
					return false
				}
				if call, ok := n.(*ast.CallExpr); ok {
					in[call] = true
				}
				return true
			})
		}
	})
	return in
}

// calleesHeld is the functions a node calls while its caller still holds
// what it holds: calleesIn, and the function literals run in place - a
// deferred call, a callback called before the caller returns. A literal a
// goroutine runs, one time.AfterFunc runs on its own goroutine, or one handed
// to AfterSessionUnlock runs without the lock and is left out. **Any other
// literal is kept in, stored and called later or not**, which errs towards
// finding more.
func calleesHeld(n ast.Node, info *types.Info) []*types.Func {
	var out []*types.Func
	after := map[*ast.FuncLit]bool{}
	ast.Inspect(n, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			return false
		case *ast.FuncLit:
			return !after[x]
		case *ast.CallExpr:
			c := calleeOf(x, info)
			if c == nil {
				return true
			}
			if c.Name() == "AfterSessionUnlock" || c.Name() == "afterSessionUnlock" ||
				c.FullName() == "time.AfterFunc" {
				for _, a := range x.Args {
					if lit, ok := a.(*ast.FuncLit); ok {
						after[lit] = true
					}
				}
			}
			out = append(out, c)
		}
		return true
	})
	return out
}

// calleeOf is the function a call resolves to, or nil for a call through a
// function value.
//
// **That nil is a blind spot every walk here shares**: a call through a
// function value - an inline subscription's Handler, a closure called by the
// name of the variable holding it - resolves to nothing, so no walk sees what
// it calls.
func calleeOf(call *ast.CallExpr, info *types.Info) *types.Func {
	var id *ast.Ident
	switch f := call.Fun.(type) {
	case *ast.Ident:
		id = f
	case *ast.SelectorExpr:
		id = f.Sel
	}
	if id == nil {
		return nil
	}
	fn, _ := info.Uses[id].(*types.Func)
	return fn
}

// pathsTo is, for every function that reaches one sink, a path to it: the
// function, the calls between, and the sink. A sink is its own path.
func pathsTo(funcs map[*types.Func]*ast.FuncDecl, info *types.Info,
	sink func(*types.Func) bool) map[*types.Func][]*types.Func {
	paths := map[*types.Func][]*types.Func{}
	callees := map[*types.Func][]*types.Func{}
	for obj, fn := range funcs {
		callees[obj] = calleesHeld(fn.Body, info)
		for _, c := range callees[obj] {
			if sink(c) {
				paths[c] = []*types.Func{c}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for obj := range funcs {
			if paths[obj] != nil {
				continue
			}
			for _, c := range callees[obj] {
				if p := paths[c]; p != nil {
					paths[obj], changed = append([]*types.Func{obj}, p...), true
					break
				}
			}
		}
	}
	return paths
}

func qualified(fn *types.Func) string {
	recv := fn.Type().(*types.Signature).Recv()
	if recv == nil {
		return fn.Name()
	}
	switch r := recv.Type().(type) {
	case *types.Pointer:
		if n, ok := r.Elem().(*types.Named); ok {
			return "(*" + n.Obj().Name() + ")." + fn.Name()
		}
	case *types.Named:
		return "(" + r.Obj().Name() + ")." + fn.Name()
	}
	return fn.Name()
}

func describePath(path []*types.Func) string {
	names := make([]string, len(path))
	for i, fn := range path {
		names[i] = qualified(fn)
	}
	return strings.Join(names, " -> ")
}

// A client id's session lock comes before b.mu and a client's cmu, and is
// never taken holding either.
//
// **The engine holds it around the hooks it calls while a connection claims
// an id, and those take b.mu.** So a section of b.mu or cmu that reached
// anything taking the session lock would be the two orders a deadlock needs:
// a connection holding the id's lock waiting for b.mu, and b.mu's holder
// waiting for the id's lock.
func TestTheSessionLockIsNeverTakenUnderTheBrokerLock(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	brokerMu := lockField(t, pkg, "Broker", "mu")
	consumerCmu := lockField(t, pkg, "consumer", "cmu")

	// Which functions take the session lock, directly or through one that does.
	takes := map[*types.Func]bool{}
	for obj, fn := range funcs {
		for _, c := range calleesHeld(fn.Body, info) {
			if c.FullName() == "(*github.com/ifnesi/saguin/internal/mqtt.Server).LockSession" {
				takes[obj] = true
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for obj, fn := range funcs {
			if takes[obj] {
				continue
			}
			for _, c := range calleesHeld(fn.Body, info) {
				if takes[c] {
					takes[obj], changed = true, true
					break
				}
			}
		}
	}

	sections := 0
	for obj, fn := range funcs {
		for _, field := range []*types.Var{brokerMu, consumerCmu} {
			walkSections(fn.Body, info, field, func(section []ast.Stmt, at token.Pos) {
				sections++
				for _, s := range section {
					for _, c := range calleesHeld(s, info) {
						if takes[c] {
							t.Errorf("%s: %s calls %s holding %s, and %s takes a client id's session lock",
								fset.Position(at), obj.Name(), qualified(c), field.Name(), qualified(c))
						}
					}
				}
			})
		}
	}
	t.Logf("%d functions take the session lock; %d sections of b.mu and cmu examined", len(takes), sections)
	for _, want := range []string{"lockSession", "OnDisconnect", "firePendingWill", "OnWill"} {
		if !takes[methodOf(t, pkg, "Broker", want)] {
			t.Errorf("%s is not found taking the session lock, which it does: the walk has stopped "+
				"matching the code", want)
		}
	}
	if sections < 100 {
		t.Fatalf("examined %d sections of b.mu and cmu, which cannot be this package", sections)
	}
}

// No session lock is taken while one is held: not the same id's, which is not
// reentrant and waits for itself for good, and not another id's, which is two
// of them taken in an order nothing fixes.
//
// **The engine takes it at more places than it used to** - around a
// SUBSCRIBE's and an UNSUBSCRIBE's hooks, and around the unsubscribe at a
// disconnect - and the takeover, expiry and retention-floor paths reach the
// same hooks with the lock already held. So both halves are walked: every
// section of the engine's lock and every hook the engine calls holding it,
// and every section of the broker's, for any call that reaches a function
// taking one - in the engine, or across into the engine through one of its
// exported methods that does.
func TestNoSessionLockIsTakenWhileOneIsHeld(t *testing.T) {
	epkg, einfo, efset, efiles := typeCheckDir(t, "../mqtt", "github.com/ifnesi/saguin/internal/mqtt")
	efuncs := funcIndex(efiles, einfo)
	takeLock := methodOf(t, epkg, "sessionLocks", "lock")
	takeConnectLock := methodOf(t, epkg, "sessionLocks", "lockConnect")
	isTakeLock := func(c *types.Func) bool { return c == takeLock || c == takeConnectLock }
	// **A publish takes one in deliveryTarget, for a connection being taken
	// over, and that is TestNothingPublishesUnderASessionLock's question**,
	// which names the sites that may. Here the lock taken is any other, so
	// the walk does not go through it.
	delete(efuncs, methodOf(t, epkg, "Server", "deliveryTarget"))
	engineTakes := pathsTo(efuncs, einfo, isTakeLock)
	hooks := epkg.Scope().Lookup("Hooks")
	isHook := func(fn *types.Func) bool {
		recv := fn.Type().(*types.Signature).Recv()
		if recv == nil {
			return false
		}
		ptr, ok := recv.Type().(*types.Pointer)
		return ok && ptr.Elem() == hooks.Type()
	}

	underLock := map[string]bool{}
	engineSections := 0
	for obj, fn := range efuncs {
		sessionSections(fn.Body, einfo, isTakeLock, func(section []ast.Stmt, at token.Pos) {
			engineSections++
			for _, s := range section {
				for _, c := range calleesHeld(s, einfo) {
					if p := engineTakes[c]; p != nil {
						t.Errorf("%s: %s holds a session lock and calls %s, which takes one",
							efset.Position(at), qualified(obj), describePath(p))
					}
				}
			}
			walkReach(section, einfo, efuncs, isHook, func(path []*types.Func) {
				if last := path[len(path)-1]; isHook(last) {
					underLock[last.Name()] = true
				}
			})
		})
	}

	// Across into the engine: its exported methods that take the lock. **The
	// four a publish goes through are the publish walk's**, for the reason
	// deliveryTarget is. InjectPacket also dispatches a SUBSCRIBE or an
	// UNSUBSCRIBE, which take the lock - so the broker building either is
	// checked below rather than assumed never to happen. That check reads
	// composite literals of packets.FixedHeader only: a Type set by assigning
	// the field afterwards is a blind spot it does not see.
	publishing := map[string]bool{}
	for _, m := range []string{"InjectPacket", "Publish", "PublishToSubscribers"} {
		publishing["(*github.com/ifnesi/saguin/internal/mqtt.Server)."+m] = true
	}
	crossing := map[string]bool{}
	for fn := range engineTakes {
		if fn.Exported() && fn.Type().(*types.Signature).Recv() != nil && !publishing[fn.FullName()] {
			crossing[fn.FullName()] = true
		}
	}
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	headers := 0
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if tv, ok := info.Types[lit]; !ok || tv.Type.String() !=
				"github.com/ifnesi/saguin/internal/mqtt/packets.FixedHeader" {
				return true
			}
			headers++
			for _, e := range lit.Elts {
				kv, ok := e.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if k, ok := kv.Key.(*ast.Ident); !ok || k.Name != "Type" {
					continue
				}
				if sel, ok := kv.Value.(*ast.SelectorExpr); ok &&
					(sel.Sel.Name == "Subscribe" || sel.Sel.Name == "Unsubscribe") {
					t.Errorf("%s: the broker builds a %s packet, which InjectPacket would handle "+
						"holding a session lock", fset.Position(lit.Pos()), sel.Sel.Name)
				}
			}
			return true
		})
	}
	if headers == 0 {
		t.Error("found no packet header the broker builds, which publishWill does: the walk has " +
			"stopped matching the code")
	}
	brokerTakes := pathsTo(funcs, info, func(c *types.Func) bool { return crossing[c.FullName()] })
	takesLock := func(fn *types.Func) bool {
		return fn.FullName() == "(*github.com/ifnesi/saguin/internal/mqtt.Server).LockSession" ||
			fn == methodOf(t, pkg, "Broker", "lockSession")
	}
	check := func(entry string, stmts []ast.Stmt, at token.Position) {
		for _, s := range stmts {
			for _, c := range calleesHeld(s, info) {
				if p := brokerTakes[c]; p != nil {
					t.Errorf("%s: %s holds a session lock and calls %s, which takes one", at, entry,
						describePath(p))
				}
			}
		}
	}
	entries := 0
	for obj, fn := range funcs {
		if underLock[obj.Name()] && isBrokerMethod(obj) {
			entries++
			check(qualified(obj), fn.Body.List, fset.Position(fn.Pos()))
		}
		sessionSections(fn.Body, info, takesLock, func(section []ast.Stmt, at token.Pos) {
			entries++
			check(qualified(obj), section, fset.Position(at))
		})
	}
	t.Logf("%d sections of the engine's session lock; %d engine functions take it, %d of them "+
		"exported methods outside a publish; %d broker hooks and sections examined; %d packet "+
		"headers the broker builds, none a SUBSCRIBE or UNSUBSCRIBE", engineSections, len(engineTakes),
		len(crossing), entries, headers)
	if !crossing["(*github.com/ifnesi/saguin/internal/mqtt.Server).LockSession"] || engineSections < 7 {
		t.Fatalf("found %d sections and exported lock-takers %v, which cannot be the engine: the walk "+
			"has stopped matching the code", engineSections, sortedKeys(crossing))
	}
}

// hooksUnderTheSessionLock is every hook the engine calls holding a client
// id's session lock, read from the engine's own sections rather than written
// down, less the ones the walk over-counts.
func hooksUnderTheSessionLock(t *testing.T) map[string]bool {
	t.Helper()
	epkg, einfo, _, efiles := typeCheckDir(t, "../mqtt", "github.com/ifnesi/saguin/internal/mqtt")
	efuncs := funcIndex(efiles, einfo)
	takeLock := methodOf(t, epkg, "sessionLocks", "lock")
	takeConnectLock := methodOf(t, epkg, "sessionLocks", "lockConnect")
	hooks := epkg.Scope().Lookup("Hooks")
	isHook := func(fn *types.Func) bool {
		recv := fn.Type().(*types.Signature).Recv()
		if recv == nil {
			return false
		}
		ptr, ok := recv.Type().(*types.Pointer)
		return ok && ptr.Elem() == hooks.Type()
	}
	under := map[string]bool{}
	sections, nilChecks, released := 0, 0, 0
	for _, fn := range efuncs {
		n, r := sessionSections(fn.Body, einfo, func(c *types.Func) bool { return c == takeLock || c == takeConnectLock },
			func(section []ast.Stmt, _ token.Pos) {
				sections++
				walkReach(section, einfo, efuncs, isHook, func(path []*types.Func) {
					if last := path[len(path)-1]; isHook(last) {
						under[last.Name()] = true
					}
				})
			})
		nilChecks, released = nilChecks+n, released+r
	}
	// establishClient leaves by both shapes: superseded, with no lock, and
	// refused, having let it go. A walk that cut neither has stopped seeing
	// them, and would count what those branches call as held.
	t.Logf("%d sections of the engine's session lock; %d branches cut for a lock never taken, "+
		"%d for a lock let go", sections, nilChecks, released)
	if nilChecks == 0 || released == 0 {
		t.Fatalf("the walk cut %d branches for a lock never taken and %d for one let go, and "+
			"establishClient has one of each", nilChecks, released)
	}
	if !under["OnSessionEstablish"] || !under["OnSubscribed"] {
		t.Fatalf("found hooks %v under the session lock, which cannot be the engine: the walk has "+
			"stopped matching the code", sortedKeys(under))
	}
	return under
}

// The Hook interface says which hooks run holding the session lock, and it
// says what the engine does: every hook the walk finds is named there, and
// every hook named there is one the walk finds.
func TestTheHookInterfaceNamesTheHooksCalledUnderTheSessionLock(t *testing.T) {
	under := hooksUnderTheSessionLock(t)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../mqtt/hooks.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse hooks.go: %v", err)
	}
	var doc string
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE || gd.Doc == nil {
			continue
		}
		for _, sp := range gd.Specs {
			if ts, ok := sp.(*ast.TypeSpec); ok && ts.Name.Name == "Hook" {
				doc = gd.Doc.Text()
			}
		}
	}
	start := strings.Index(doc, "Some hooks are called holding")
	end := strings.Index(doc, "Such a hook must never call")
	if start < 0 || end < start {
		t.Fatalf("the Hook interface's comment no longer says which hooks run holding the session "+
			"lock:\n%s", doc)
	}
	named := map[string]bool{}
	for _, w := range strings.FieldsFunc(doc[start:end], func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
	}) {
		if strings.HasPrefix(w, "On") && len(w) > 2 && w[2] >= 'A' && w[2] <= 'Z' {
			named[w] = true
		}
	}
	for name := range under {
		if !named[name] {
			t.Errorf("the engine calls %s holding the session lock, and the Hook interface does not say so", name)
		}
	}
	for name := range named {
		if !under[name] {
			t.Errorf("the Hook interface says %s is called holding the session lock, and the engine does not", name)
		}
	}
	t.Logf("%d hooks called under the session lock, %d named on the Hook interface", len(under), len(named))
}

// No test's hook takes the lock the hook is called holding.
//
// **A test's hooks are not saguin's code, so no walk above sees them**, and
// one that called the broker's OnDisconnect from OnSubscribed - once the
// engine held the id's session lock across OnSubscribed - hung a package run
// for forty-five minutes, waiting on itself. Every method of a test type
// named after a hook the engine calls holding the lock is read for a call
// that takes it, or that publishes: LockSession, OnDisconnect, OnWill, the
// Will timer, and the four publish entry points. Read by name, since the
// tests are several packages the type checker is not given here.
func TestNoTestHookTakesTheLockItsHookHolds(t *testing.T) {
	// A hook named in the rule and called for more than one kind of packet
	// may act on one the engine handles without the lock; each is named here
	// for what it acts on, and an entry that no longer matches fails.
	exempt := map[string]string{
		"(*staleTeardownHook).OnPacketEncode": "it acts on a SUBACK only, and the engine encodes a " +
			"SUBACK after processSubscribe has let the lock go",
	}
	used := map[string]bool{}
	under := hooksUnderTheSessionLock(t)
	forbidden := map[string]bool{"LockSession": true, "lockSession": true, "OnDisconnect": true,
		"OnWill": true, "firePendingWill": true, "InjectPacket": true, "Publish": true,
		"PublishToSubscribers": true}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var files, methods int
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", path, err)
			return nil
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || !under[fn.Name.Name] {
				continue
			}
			methods++
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if _, ok := n.(*ast.GoStmt); ok {
					return false
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && forbidden[sel.Sel.Name] {
					if _, ok := exempt[qualifiedFuncName(fn)]; ok {
						used[qualifiedFuncName(fn)] = true
						return true
					}
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d: %s calls %s, and the engine calls %s holding the client id's "+
						"session lock", rel, fset.Position(call.Pos()).Line, qualifiedFuncName(fn),
						sel.Sel.Name, fn.Name.Name)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range exempt {
		if !used[name] {
			t.Errorf("%s is exempted and calls nothing that takes the lock any more: remove it", name)
		}
	}
	t.Logf("%d test files read; %d methods named after the %d hooks called under the lock examined",
		files, methods, len(under))
	if files < 20 || methods == 0 {
		t.Fatalf("read %d test files and %d hook methods, which cannot be this repository", files, methods)
	}
}

// TestTheDrainsFlushLockIsTakenUnderNoOtherLock holds the order owed.fmu is
// documented with. Every store write of a session's drain holds it across
// the store call (flush, record), a clean DISCONNECT waits for it on the
// client's read loop (storeBeforeDisconnectCloses), and a session's ending
// waits for it (end), so it is taken under no other lock: not b.mu, not a
// client's cmu, not the drain's bdrain.mu or gmu, and not owed.mu, which only
// ever comes after it. Every function that takes it, directly or through a
// call, is found, and every section holding one of the others is checked for
// a call to any of them.
func TestTheDrainsFlushLockIsTakenUnderNoOtherLock(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	flushMu := lockField(t, pkg, "owed", "fmu")
	others := map[*types.Var]string{
		lockField(t, pkg, "Broker", "mu"):    "b.mu",
		lockField(t, pkg, "consumer", "cmu"): "a client's cmu",
		lockField(t, pkg, "bdrain", "mu"):    "bdrain.mu",
		lockField(t, pkg, "bdrain", "gmu"):   "bdrain.gmu",
		lockField(t, pkg, "owed", "mu"):      "owed.mu",
	}
	funcs := funcIndex(files, info)

	takes := map[*types.Func]bool{}
	for obj, fn := range funcs {
		if locksField(fn.Body, info, flushMu) {
			takes[obj] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for obj, fn := range funcs {
			if takes[obj] {
				continue
			}
			for _, callee := range calleesIn(fn.Body, info) {
				if takes[callee] {
					takes[obj], changed = true, true
					break
				}
			}
		}
	}
	for _, m := range [][2]string{
		{"bdrain", "flush"}, {"Broker", "storeBeforeDisconnectCloses"},
		{"bdrain", "record"}, {"bdrain", "batch"}, {"bdrain", "received"},
		{"bdrain", "end"}, {"Broker", "endStoredSession"}, {"Broker", "confirmClaim"},
	} {
		if fn := methodOf(t, pkg, m[0], m[1]); !takes[fn] {
			t.Fatalf("the walk does not find that %s takes owed.fmu, so it is not following the code it guards",
				fn.FullName())
		}
	}

	sections := map[string]int{}
	for obj, fn := range funcs {
		for lock, name := range others {
			walkSections(fn.Body, info, lock, func(section []ast.Stmt, at token.Pos) {
				sections[name]++
				for _, s := range section {
					if locksField(s, info, flushMu) {
						t.Errorf("%s: %s takes owed.fmu while holding %s", fset.Position(at), obj.Name(), name)
					}
					for _, callee := range calleesIn(s, info) {
						if takes[callee] {
							t.Errorf("%s: %s calls %s while holding %s, and %s takes owed.fmu",
								fset.Position(at), obj.Name(), callee.FullName(), name, callee.FullName())
						}
					}
				}
			})
		}
	}
	t.Logf("%d functions take owed.fmu; sections examined: %v", len(takes), sections)
	for _, name := range others {
		if sections[name] == 0 {
			t.Fatalf("no section holding %s was found, so this walk examined nothing of it", name)
		}
	}
}

// interfaceOf is the named interface a method belongs to, or nil for a
// method of a concrete type or a plain function.
func interfaceOf(fn *types.Func) *types.Named {
	recv := fn.Type().(*types.Signature).Recv()
	if recv == nil {
		return nil
	}
	named, ok := recv.Type().(*types.Named)
	if !ok {
		return nil
	}
	if _, ok := named.Underlying().(*types.Interface); !ok {
		return nil
	}
	return named
}

// RFC 0002 "Every session's state": the broadcast log gives way to every
// other writer on its provider, and the give-up runs inside whatever store
// call ran out of room - a channel's append, a session's Will.
//
// **So giveUp reaches only the log, the owed lists and what counts and says
// what went**: the broadcast log's own methods, the drain's and the lists'
// locks, the counters and the logger. It never calls the session store the
// drain writes through (broadcastSessions), any other store the broker holds,
// a store's concrete type, or anything that takes b.mu or a client's cmu.
// Each of those is a lock or a transaction the writer that ran out of room
// may be in the middle of. Walked from giveUp through every call, the
// closures it runs in place included (calleesHeld).
//
// **Nor does it take a session's flush lock, owed.fmu**, which is what makes
// holding one across giveUp safe: a session's drain writes its in-flight
// table under its own fmu (record), and that write is one that runs out of
// room. What giveUp does take - gmu, bdrain.mu, any list's owed.mu, the
// report's lock and the log's store call - is never held where an fmu is
// taken (TestTheDrainsFlushLockIsTakenUnderNoOtherLock), so a give-up holding
// one session's fmu waits on nothing that waits for it. A flush it starts
// for an away session runs on a goroutine of its own (settle), which the walk
// does not follow, and rightly: nothing here waits for it.
func TestAGiveUpReachesOnlyTheLogAndTheLists(t *testing.T) {
	pkg, info, _, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	start := methodOf(t, pkg, "bdrain", "giveUp")

	locks := map[*types.Var]string{
		lockField(t, pkg, "Broker", "mu"):    "b.mu",
		lockField(t, pkg, "consumer", "cmu"): "a client's cmu",
		lockField(t, pkg, "owed", "fmu"):     "a session's owed.fmu",
	}
	reached := map[*types.Func]bool{start: true}
	queue := []*types.Func{start}
	var calls []*types.Func
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		decl, ok := funcs[fn]
		if !ok {
			continue
		}
		for lock, name := range locks {
			if locksFieldHeld(decl.Body, info, lock) {
				t.Errorf("giveUp reaches %s, which takes %s", qualified(fn), name)
			}
		}
		for _, callee := range calleesHeld(decl.Body, info) {
			calls = append(calls, callee)
			if !reached[callee] {
				reached[callee] = true
				queue = append(queue, callee)
			}
		}
	}

	brokerPath := pkg.Path()
	var logCalls []string
	for _, fn := range calls {
		if iface := interfaceOf(fn); iface != nil && iface.Obj().Pkg() != nil && iface.Obj().Pkg().Path() == brokerPath {
			if iface.Obj().Name() == "broadcastLog" {
				logCalls = append(logCalls, fn.Name())
				continue
			}
			t.Errorf("giveUp reaches %s.%s: a store other than the broadcast log", iface.Obj().Name(), fn.Name())
			continue
		}
		if fn.Pkg() == nil || !strings.HasPrefix(fn.Pkg().Path(), "github.com/ifnesi/saguin/internal/store") {
			continue
		}
		if fn.Type().(*types.Signature).Recv() != nil {
			t.Errorf("giveUp reaches %s, a store's own method, not through the broadcast log", qualified(fn))
		}
	}
	for _, want := range []string{"ReadFromN", "Remove"} {
		if !slices.Contains(logCalls, want) {
			t.Fatalf("the walk does not see giveUp call the log's %s (it saw %v), so it is not following "+
				"the code it guards", want, logCalls)
		}
	}
	for _, m := range [][2]string{{"owed", "dropLocked"}, {"owed", "onWireLocked"}, {"bdrain", "noteGaveUp"}} {
		if !reached[methodOf(t, pkg, m[0], m[1])] {
			t.Fatalf("the walk does not reach %s.%s from giveUp, so it is not following the closures", m[0], m[1])
		}
	}
	if !locksFieldHeld(funcs[methodOf(t, pkg, "bdrain", "giveUpOf")].Body, info, lockField(t, pkg, "owed", "mu")) {
		t.Fatal("the walk does not see giveUpOf take owed.mu in its Range callback, so it would not see a " +
			"lock taken there")
	}
	t.Logf("giveUp reaches %d functions and makes %d calls; the log's methods it calls: %v",
		len(reached), len(calls), logCalls)
}

// RFC 0002 "Every session's state", and the drain's lock order
//
// **Nothing reaches giveUp holding a lock giveUp could end up waiting on**:
// b.mu or a client's cmu, which the drain's locks are never held with; the
// drain's bdrain.mu or an owed.mu, which it takes; its gmu, which it takes
// and is not reentrant; the share cursors' and the claims' b.shareMu and
// b.claimMu, whose writes are not wrapped; or the held exchanges' b.heldMu,
// taken after the store call it records. The writer that runs out of room
// holds none of them when it calls withRoom. A session's flush lock owed.fmu
// is held across giveUp by the drain's own in-flight writes (record), and
// that is safe because giveUp takes no fmu and nothing it takes is held
// where one is taken (TestAGiveUpReachesOnlyTheLogAndTheLists). The engine's
// per-id session lock may be held -
// a CONNECT's writes are made under it - and that is safe by order: giveUp
// takes no session lock and fans nothing out, so nothing it waits for waits
// on one. Every function that reaches giveUp while it runs - the closures it
// runs in place included, and not one a timer or a goroutine runs later
// (calleesHeld) - is found, and every section holding one of the locks is
// checked for a call to any of them.
func TestAGiveUpIsReachedUnderNoLockItCouldWaitOn(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	giveUp := methodOf(t, pkg, "bdrain", "giveUp")
	held := map[*types.Var]string{
		lockField(t, pkg, "Broker", "mu"):      "b.mu",
		lockField(t, pkg, "consumer", "cmu"):   "a client's cmu",
		lockField(t, pkg, "bdrain", "mu"):      "bdrain.mu",
		lockField(t, pkg, "bdrain", "gmu"):     "bdrain.gmu",
		lockField(t, pkg, "owed", "mu"):        "owed.mu",
		lockField(t, pkg, "Broker", "shareMu"): "b.shareMu",
		lockField(t, pkg, "Broker", "claimMu"): "b.claimMu",
		lockField(t, pkg, "Broker", "heldMu"):  "b.heldMu",
	}

	reaches := map[*types.Func]bool{giveUp: true}
	for changed := true; changed; {
		changed = false
		for obj, fn := range funcs {
			if reaches[obj] {
				continue
			}
			for _, callee := range calleesHeld(fn.Body, info) {
				if reaches[callee] {
					reaches[obj], changed = true, true
					break
				}
			}
		}
	}
	for _, m := range [][2]string{{"bdrain", "keep"}, {"Broker", "withRoom"}} {
		if !reaches[methodOf(t, pkg, m[0], m[1])] {
			t.Fatalf("the walk does not find that %s.%s reaches giveUp, so it is not following the code "+
				"it guards", m[0], m[1])
		}
	}

	sections := map[string]int{}
	for obj, fn := range funcs {
		for lock, name := range held {
			walkSections(fn.Body, info, lock, func(section []ast.Stmt, at token.Pos) {
				sections[name]++
				for _, s := range section {
					for _, callee := range calleesHeld(s, info) {
						if reaches[callee] {
							t.Errorf("%s: %s calls %s while holding %s, and %s reaches giveUp",
								fset.Position(at), obj.Name(), callee.FullName(), name, callee.FullName())
						}
					}
				}
			})
		}
	}
	t.Logf("%d functions reach giveUp; sections examined: %v", len(reaches), sections)
	for _, name := range held {
		if sections[name] == 0 {
			t.Fatalf("no section holding %s was found, so this walk examined nothing of it", name)
		}
	}
}

// RFC 0003 "Exactly once, and where the unfinished ones wait", and the order
// Broker.heldMu's comment states: the client id's session lock, then the
// store, then heldMu.
//
// **So heldMu is a leaf**: nothing reached while it is held calls a store -
// a method of an interface this package declares, or of any type in the
// store packages - takes b.mu, takes a session lock, or takes heldMu again.
// A store call under it would hold every other client's hold, release and
// expiry behind one client's transaction, and the gauge's read behind both;
// a session lock under it would invert the order. Walked from every section
// holding it, through every call, the closures run in place included
// (calleesHeld).
func TestTheHeldExchangesLockIsALeaf(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	heldMu := lockField(t, pkg, "Broker", "heldMu")
	brokerMu := lockField(t, pkg, "Broker", "mu")
	lockSession := methodOf(t, pkg, "Broker", "lockSession")
	brokerPath := pkg.Path()
	forbidden := func(fn *types.Func) string {
		if fn == lockSession || fn.FullName() == "(*github.com/ifnesi/saguin/internal/mqtt.Server).LockSession" {
			return "takes a session lock"
		}
		if iface := interfaceOf(fn); iface != nil && iface.Obj().Pkg() != nil && iface.Obj().Pkg().Path() == brokerPath {
			return "calls " + iface.Obj().Name() + "." + fn.Name() + ", a store"
		}
		if fn.Pkg() != nil && strings.HasPrefix(fn.Pkg().Path(), "github.com/ifnesi/saguin/internal/store") &&
			fn.Type().(*types.Signature).Recv() != nil {
			return "calls a store's own method"
		}
		if decl, ok := funcs[fn]; ok {
			if locksField(decl.Body, info, brokerMu) {
				return "takes b.mu"
			}
			if locksField(decl.Body, info, heldMu) {
				return "takes heldMu again"
			}
		}
		return ""
	}

	sections, examined := 0, map[string]bool{}
	for obj, fn := range funcs {
		walkSections(fn.Body, info, heldMu, func(section []ast.Stmt, at token.Pos) {
			sections++
			examined[obj.Name()] = true
			seen := map[*types.Func]bool{}
			var queue []*types.Func
			for _, st := range section {
				queue = append(queue, calleesHeld(st, info)...)
			}
			for len(queue) > 0 {
				c := queue[0]
				queue = queue[1:]
				if seen[c] {
					continue
				}
				seen[c] = true
				if why := forbidden(c); why != "" {
					t.Errorf("%s: %s holds b.heldMu and reaches %s, which %s", fset.Position(at),
						obj.Name(), qualified(c), why)
					continue
				}
				if decl, ok := funcs[c]; ok {
					queue = append(queue, calleesHeld(decl.Body, info)...)
				}
			}
		})
	}
	for _, want := range []string{"forgetHeld", "holdForRelease", "releaseHeld", "sweepHeldPublishes",
		"dropHeldOf", "unreleasedBySession", "collect"} {
		if !examined[want] {
			t.Fatalf("no section of %s holding b.heldMu was found (examined %v), so this walk is not "+
				"following the code it guards", want, sortedKeys(examined))
		}
	}
	t.Logf("%d sections holding b.heldMu examined, in %v", sections, sortedKeys(examined))
}

// TestNothingATakeoverRunsUnderItsWriteLockTakesTheBrokerOrAClientLock is
// the other half of the order TestAnInFlightTableIsChangedOnlyWhileItsConnectionOwnsIt
// holds. A channel's pump takes a connection's handover lock for reading
// before its cmu (Client.Own), the engine takes it for reading before b.mu
// (its delivery hooks), and a shared group's hand-over before a member's
// owed.fmu, so a takeover holding it for writing must never wait for any of
// them: it would wait on a reader waiting on it. Every section of
// the engine that holds the lock for writing is found, what it calls is
// followed through the engine to the hooks it calls, and each hook saguin
// implements is followed through this package - the closures it runs in
// place included - to any lock it takes.
func TestNothingATakeoverRunsUnderItsWriteLockTakesTheBrokerOrAClientLock(t *testing.T) {
	mpkg, minfo, _, mfiles := typeCheckDir(t, "../mqtt", "github.com/ifnesi/saguin/internal/mqtt")
	handover := lockField(t, mpkg, "ClientState", "handover")
	mfuncs := funcIndex(mfiles, minfo)
	const hooksType = "(*github.com/ifnesi/saguin/internal/mqtt.Hooks)."

	hooks := map[string]bool{}
	sections := 0
	for _, fn := range mfuncs {
		walkSections(fn.Body, minfo, handover, func(section []ast.Stmt, at token.Pos) {
			sections++
			seen := map[*types.Func]bool{}
			var queue []*types.Func
			for _, s := range section {
				queue = append(queue, calleesHeld(s, minfo)...)
			}
			for len(queue) > 0 {
				f := queue[0]
				queue = queue[1:]
				if seen[f] {
					continue
				}
				seen[f] = true
				if strings.HasPrefix(f.FullName(), hooksType) {
					hooks[f.Name()] = true
					continue
				}
				if decl, ok := mfuncs[f]; ok {
					queue = append(queue, calleesHeld(decl.Body, minfo)...)
				}
			}
		})
	}
	if sections < 3 || !hooks["OnDeliveryDone"] || !hooks["OnQosDropped"] {
		t.Fatalf("%d sections holding the handover lock for writing, calling hooks %v: fewer than the "+
			"takeover, the expiry and EndTakenOver - which a takeover that ends the session calls - have, "+
			"so the walk is not following the engine", sections, slices.Sorted(maps.Keys(hooks)))
	}

	pkg, info, _, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	// **And no session's or group's flush lock, owed.fmu**, the twin of the
	// order a shared group's hand-over takes: the connection's read lock,
	// then the member's fmu (bdrain.handToMember). A takeover's write-lock
	// section waiting on an fmu that a hand-over holding the read lock
	// waits under would be each waiting on the other.
	locks := map[*types.Var]string{
		lockField(t, pkg, "Broker", "mu"):    "b.mu",
		lockField(t, pkg, "consumer", "cmu"): "a client's cmu",
		lockField(t, pkg, "owed", "fmu"):     "a session's or a group's owed.fmu",
	}
	owedMu := lockField(t, pkg, "owed", "mu")
	reachesOwed := false
	examined := 0
	for name := range hooks {
		obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(pkg.Scope().Lookup("Broker").Type()), true, pkg, name)
		start, ok := obj.(*types.Func)
		if !ok {
			continue // a hook saguin does not implement
		}
		reached := map[*types.Func]bool{start: true}
		queue := []*types.Func{start}
		for len(queue) > 0 {
			fn := queue[0]
			queue = queue[1:]
			decl, ok := funcs[fn]
			if !ok {
				continue
			}
			examined++
			for lock, lname := range locks {
				if locksFieldHeld(decl.Body, info, lock) {
					t.Errorf("%s, which a takeover calls under the handover lock, reaches %s, which takes %s",
						name, qualified(fn), lname)
				}
			}
			if locksFieldHeld(decl.Body, info, owedMu) {
				reachesOwed = true
			}
			for _, callee := range calleesHeld(decl.Body, info) {
				if !reached[callee] {
					reached[callee] = true
					queue = append(queue, callee)
				}
			}
		}
	}
	t.Logf("%d engine sections hold the handover lock for writing and call hooks %v; %d functions of "+
		"this package examined from them", sections, slices.Sorted(maps.Keys(hooks)), examined)
	if !reachesOwed {
		t.Fatal("the walk does not find the hooks a takeover calls reaching owed.mu, which OnDeliveryDone " +
			"takes, so it is not following the code it guards")
	}
}

// TestAnInFlightTableIsChangedOnlyWhileItsConnectionOwnsIt holds the rule
// WhileOwned exists for. saguin registers and retires deliveries on a
// connection from goroutines of its own - the broadcast drain, a channel's
// pump, a latest value's drain - and a takeover copies that connection's
// in-flight table to the new one under its handover lock: one registered
// after the copy is on no connection's table, and one retired after it is
// sent again by the new connection as well as put back. So every call in
// this package that adds an entry to a Client's in-flight table or takes one
// out is inside a function WhileOwned runs, which holds that lock for
// reading, or follows a Client.Own in the same function, which holds it the
// same way until it is let go.
//
// Claim and Unclaim are not among them, by what they are: a reservation
// belongs to one table and a takeover never copies it (Inflight.Clone).
//
// **And the order the lock takes.** The engine holds the read lock while its
// hooks take b.mu (OnQosPublish), and b.mu comes before a client's cmu; a
// takeover's clearing of a table calls DeliveryKeeper hooks under the write
// lock, and those reach owed.mu. A caller waiting for the
// read lock while a takeover waits to write holds up everything behind it, so
// neither WhileOwned nor Own is reached with b.mu, a client's cmu, bdrain.mu,
// owed.mu or owed.fmu held - the read lock comes first - and nothing
// WhileOwned runs takes any of them.
func TestAnInFlightTableIsChangedOnlyWhileItsConnectionOwnsIt(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	const (
		inflight   = "(*github.com/ifnesi/saguin/internal/mqtt.Inflight)."
		whileOwned = "(*github.com/ifnesi/saguin/internal/mqtt.Client).WhileOwned"
		own        = "(*github.com/ifnesi/saguin/internal/mqtt.Client).Own"
	)
	mutating := map[string]bool{}
	for _, m := range []string{"Set", "Retire", "RetireExpired", "Delete", "Withhold",
		"WithholdAgain", "TakeImmediate", "TakeWithheld", "DropOldest"} {
		mutating[inflight+m] = true
	}

	// What WhileOwned runs, by where it lies; and where each Own is called.
	var owned []*ast.FuncLit
	var owns []token.Pos
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if fn := calleeOf(call, info); fn != nil {
					switch fn.FullName() {
					case whileOwned:
						for _, a := range call.Args {
							if lit, ok := a.(*ast.FuncLit); ok {
								owned = append(owned, lit)
							}
						}
					case own:
						owns = append(owns, call.Pos())
					}
				}
			}
			return true
		})
	}
	within := func(decl *ast.FuncDecl, p token.Pos) string {
		for _, lit := range owned {
			if p >= lit.Pos() && p < lit.End() {
				return "WhileOwned"
			}
		}
		for _, o := range owns {
			if o >= decl.Body.Pos() && o < p {
				return "Own"
			}
		}
		return ""
	}
	examined := map[string]int{}
	byOwn := 0
	for _, f := range files {
		file := filepath.Base(fset.Position(f.Pos()).Filename)
		for _, d := range f.Decls {
			decl, ok := d.(*ast.FuncDecl)
			if !ok || decl.Body == nil {
				continue
			}
			ast.Inspect(decl.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if fn := calleeOf(call, info); fn != nil && mutating[fn.FullName()] {
					examined[file]++
					switch within(decl, call.Pos()) {
					case "":
						t.Errorf("%s: %s changes a connection's in-flight table outside WhileOwned or Own",
							fset.Position(call.Pos()), fn.Name())
					case "Own":
						byOwn++
					}
				}
				return true
			})
		}
	}

	// The locks: which functions take each, directly or through a call.
	funcs := funcIndex(files, info)
	locks := map[string]*types.Var{
		"b.mu":           lockField(t, pkg, "Broker", "mu"),
		"a client's cmu": lockField(t, pkg, "consumer", "cmu"),
		"bdrain.mu":      lockField(t, pkg, "bdrain", "mu"),
		"owed.mu":        lockField(t, pkg, "owed", "mu"),
		"owed.fmu":       lockField(t, pkg, "owed", "fmu"),
	}
	closure := func(direct func(*ast.FuncDecl) bool) map[*types.Func]bool {
		takes := map[*types.Func]bool{}
		for obj, fn := range funcs {
			if direct(fn) {
				takes[obj] = true
			}
		}
		for changed := true; changed; {
			changed = false
			for obj, fn := range funcs {
				if takes[obj] {
					continue
				}
				for _, callee := range calleesIn(fn.Body, info) {
					if takes[callee] {
						takes[obj], changed = true, true
						break
					}
				}
			}
		}
		return takes
	}
	// Every function of the engine that takes a connection's handover lock
	// for reading, directly or through a call: WhileOwned and Own, and the
	// engine's own delivery paths saguin can call (PublishToSubscribers among them).
	mpkg, minfo, _, mfiles := typeCheckDir(t, "../mqtt", "github.com/ifnesi/saguin/internal/mqtt")
	handover := lockField(t, mpkg, "ClientState", "handover")
	mfuncs := funcIndex(mfiles, minfo)
	readers := map[string]bool{}
	for obj, fn := range mfuncs {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if isFieldLock(n, minfo, handover, "RLock") {
				readers[obj.FullName()] = true
			}
			return !readers[obj.FullName()]
		})
	}
	for changed := true; changed; {
		changed = false
		for obj, fn := range mfuncs {
			if readers[obj.FullName()] {
				continue
			}
			for _, callee := range calleesIn(fn.Body, minfo) {
				if readers[callee.FullName()] {
					readers[obj.FullName()], changed = true, true
					break
				}
			}
		}
	}
	for _, name := range []string{whileOwned, own, "(*github.com/ifnesi/saguin/internal/mqtt.Server).PublishToSubscribers"} {
		if !readers[name] {
			t.Fatalf("the walk does not find that %s takes the handover lock for reading, so it is not "+
				"following the engine", name)
		}
	}
	acquires := func(f *types.Func) bool { return readers[f.FullName()] }
	reachesOwned := closure(func(fn *ast.FuncDecl) bool {
		return slices.ContainsFunc(calleesHeld(fn.Body, info), acquires)
	})
	for _, m := range [][2]string{{"Broker", "pumpBatch"}, {"Broker", "drainPump"}, {"bdrain", "batch"}} {
		if !reachesOwned[methodOf(t, pkg, m[0], m[1])] {
			t.Fatalf("the walk does not find that %s.%s reaches WhileOwned or Own, so it is not following "+
				"the code it guards", m[0], m[1])
		}
	}
	sections := map[string]int{}
	for name, lock := range locks {
		takes := closure(func(fn *ast.FuncDecl) bool { return locksField(fn.Body, info, lock) })
		for _, lit := range owned {
			if locksField(lit.Body, info, lock) {
				t.Errorf("%s: what WhileOwned runs takes %s", fset.Position(lit.Pos()), name)
			}
			for _, callee := range calleesIn(lit.Body, info) {
				if takes[callee] {
					t.Errorf("%s: what WhileOwned runs calls %s, which takes %s",
						fset.Position(lit.Pos()), callee.FullName(), name)
				}
			}
		}
		for obj, fn := range funcs {
			walkSections(fn.Body, info, lock, func(section []ast.Stmt, at token.Pos) {
				sections[name]++
				// What runs while the lock is held: not a timer's or a
				// goroutine's closure (calleesHeld).
				for _, s := range section {
					for _, f := range calleesHeld(s, info) {
						if acquires(f) || reachesOwned[f] {
							t.Errorf("%s: %s reaches %s, which takes a connection's handover lock, "+
								"while holding %s", fset.Position(at), obj.Name(), f.FullName(), name)
						}
					}
				}
			})
		}
	}
	total := 0
	for _, n := range examined {
		total += n
	}
	t.Logf("%d in-flight table changes examined (%v), %d of them after an Own; %d functions WhileOwned "+
		"runs; %d Own calls; %d engine functions take the handover lock for reading; sections examined: %v",
		total, examined, byOwn, len(owned), len(owns), len(readers), sections)
	if examined["bcast.go"] < 4 || examined["broker.go"] < 4 || examined["session.go"] < 1 ||
		len(owned) < 8 || byOwn < 1 || len(owns) < 1 {
		t.Fatalf("fewer changes, WhileOwned functions or Own calls than the drain, the pumps and the " +
			"resume have: the walk has stopped matching the code it guards")
	}
	for name := range locks {
		if sections[name] == 0 {
			t.Fatalf("no section holding %s was found, so this walk examined nothing of it", name)
		}
	}
}

// **No write to the session store is waited for under b.mu.** A sqlite
// provider collects every client's session writes behind the commit before
// (RFC 0004 "Group commit"), so a call to the session store can wait out
// other clients' transactions - and b.mu held across that wait would stop
// every connection, publish and delivery that takes it behind a stranger's
// commit. So nothing reached while b.mu is held calls a method of the
// session store's interfaces, or a session store's own: walked from every
// section holding b.mu, through every call, the closures run in place
// included (calleesHeld).
func TestNoSessionStoreCallIsMadeUnderTheBrokerLock(t *testing.T) {
	pkg, info, fset, files := typeCheckPackage(t)
	funcs := funcIndex(files, info)
	brokerMu := lockField(t, pkg, "Broker", "mu")
	stores := map[string]bool{"SessionStore": true, "broadcastSessions": true}
	forbidden := func(fn *types.Func) string {
		if iface := interfaceOf(fn); iface != nil && iface.Obj().Pkg() == pkg && stores[iface.Obj().Name()] {
			return "calls " + iface.Obj().Name() + "." + fn.Name()
		}
		recv := fn.Type().(*types.Signature).Recv()
		if recv != nil {
			for name := range stores {
				iface := pkg.Scope().Lookup(name).Type().Underlying().(*types.Interface)
				if types.Implements(recv.Type(), iface) && iface.NumMethods() > 0 {
					for i := range iface.NumMethods() {
						if iface.Method(i).Name() == fn.Name() {
							return "calls " + recv.Type().String() + "." + fn.Name() + ", a " + name
						}
					}
				}
			}
		}
		if recv != nil && fn.Pkg() != nil &&
			strings.HasPrefix(fn.Pkg().Path(), "github.com/ifnesi/saguin/internal/store") {
			rt := recv.Type()
			if p, ok := rt.(*types.Pointer); ok {
				rt = p.Elem()
			}
			if n, ok := rt.(*types.Named); ok && n.Obj().Name() == "Sessions" {
				return "calls a session store's own " + fn.Name()
			}
		}
		return ""
	}
	sections, calls := 0, 0
	for obj, fn := range funcs {
		walkSections(fn.Body, info, brokerMu, func(section []ast.Stmt, at token.Pos) {
			sections++
			seen := map[*types.Func]bool{}
			var queue []*types.Func
			for _, st := range section {
				queue = append(queue, calleesHeld(st, info)...)
			}
			for len(queue) > 0 {
				c := queue[0]
				queue = queue[1:]
				if seen[c] {
					continue
				}
				seen[c] = true
				calls++
				if why := forbidden(c); why != "" {
					t.Errorf("%s: %s holds b.mu and reaches %s, which %s", fset.Position(at), obj.Name(),
						qualified(c), why)
					continue
				}
				if decl, ok := funcs[c]; ok {
					queue = append(queue, calleesHeld(decl.Body, info)...)
				}
			}
		})
	}
	t.Logf("%d sections holding b.mu examined, %d calls followed", sections, calls)
	if sections < 100 || calls == 0 {
		t.Fatalf("%d sections holding b.mu found and %d calls followed: the walk is not finding them", sections, calls)
	}
	if forbidden(methodOf(t, pkg, "countingSessions", "Save")) != "" {
		return
	}
	t.Fatal("the rule does not recognise countingSessions.Save as a session store call: it would pass " +
		"whatever b.mu reached")
}
