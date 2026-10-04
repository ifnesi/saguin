package broker

// The rule about saguin's two subscription indexes, in the shape the tree's
// other source-level rules already take: a check over the source rather
// than a list of the sites that were right on the day somebody looked.
//
// **What it is about.** `subs` holds a client's channel subscriptions and
// `partitions` holds what that client declared about which slice of a
// channel it wants. They are two maps because a broadcast subscriber has an
// entry in the second and none in the first, and two maps are a thing to
// keep in step. A removal from one that forgets the other leaves a
// partition declaration behind, and the next SUBSCRIBE on that filter
// inherits a predicate the client never sent: it is served a slice of the
// channel while believing it asked for all of it, and nothing reports that
// - it looks exactly like a channel carrying less traffic than expected.
//
// **Why a source check and not a behavioural one.** The bug is a *missed
// site*, not a wrong delete. A test that disconnects a client and finds the
// maps empty passes while exercising one departure path out of four, and
// goes on passing when somebody adds a fifth. What has to be true is that
// there is nowhere to add one: every removal lives in forgetClient or
// forgetFilters, so a new departure path cannot forget the second map
// because there is nothing to remember. Only the source can say that.
//
// It is an ordinary test rather than a Makefile target so that it cannot be
// skipped by somebody who runs the suite and not the Makefile.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The functions a removal from either index may appear in.
//
// **declare is the third and it is not a weakening.** It is the only writer
// of b.partitions, and clearing one filter's declaration is the other half
// of writing one: a client re-subscribing without a saguin-filter is
// dropping its slice, and leaving the old one would serve it a slice it no
// longer asks for. It does not touch b.subs, because the subscription is
// still live - only the declaration is gone - which is why it cannot be
// forgetFilters.
//
// **The receiver is part of every entry, and that is not decoration.** This
// list is a permission, and keyed by bare name it grants that permission to
// any function of that name - including one added later by somebody who
// never read this file. `declare` is the live example: both
// `(*Broker).declare` and `(*metricWriter).declare` exist today, and only
// the first is meant. The lock-order walk was found to be wrong for the
// mirror of this on 2026-09-23, where a duplicate name hid three functions
// that take the broker lock.
var mayRemove = map[string]bool{
	"(*Broker).forgetClient": true, "(*Broker).forgetFilters": true,
	"(*Broker).declare": true, "(*Broker).forgetSubscriptions": true,
}

// The maps the rule is about.
var guarded = map[string]bool{"subs": true, "partitions": true}

func TestEveryRemovalFromTheSubscriptionIndexesGoesThroughOnePlace(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}

	removals, outside := 0, []string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") ||
			strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				where, what := "", ""
				switch x := n.(type) {
				case *ast.CallExpr:
					// delete(b.<map>, ...) - a whole entry going.
					id, ok := x.Fun.(*ast.Ident)
					if !ok || id.Name != "delete" || len(x.Args) == 0 {
						return true
					}
					what = guardedName(x.Args[0])
					where = "delete"
				case *ast.AssignStmt:
					// b.<map>[k] = ... where the value neither grows the
					// entry nor creates it. An append grows it and a
					// composite literal creates an empty one; anything else
					// can shrink or replace what is there.
					//
					// **The creation case is here because this check flagged
					// its own author.** `b.partitions[id] = map[...]{}` in
					// declare() is a client's first declaration, not a
					// removal, and a rule that calls it one teaches the next
					// reader to reach for the exemption list rather than to
					// believe the check.
					if len(x.Lhs) != 1 || len(x.Rhs) != 1 {
						return true
					}
					ix, ok := x.Lhs[0].(*ast.IndexExpr)
					if !ok {
						return true
					}
					what = guardedName(ix.X)
					if what == "" || isAppendTo(x.Rhs[0], what) || isCreation(x.Rhs[0]) {
						return true
					}
					where = "assignment"
				default:
					return true
				}
				if what == "" {
					return true
				}
				removals++
				if !mayRemove[qualifiedFuncName(fn)] {
					outside = append(outside,
						fset.Position(n.Pos()).String()+": "+where+" on b."+what+
							" in "+qualifiedFuncName(fn)+"()")
				}
				return true
			})
		}
	}

	// **Counted, because a sweep that matched nothing would say ok.** The
	// named functions hold a removal each at least - two deletes in
	// forgetClient, a prune and a delete in forgetFilters, a delete in
	// declare - so a total below that many means this walk has stopped
	// seeing the shape it is written against and is reporting on work it
	// did not do.
	//
	// A floor rather than an exact number: adding a removal inside one of
	// those functions is ordinary, and only a *fall* says the walk has gone
	// blind.
	if want := len(mayRemove); removals < want {
		t.Fatalf("found %d removals from b.subs / b.partitions across the package, "+
			"and %d functions are named as their only homes: this check has stopped "+
			"matching the code it guards and would pass over anything",
			removals, want)
	}
	for _, o := range outside {
		t.Errorf("%s\n  every removal from these two indexes belongs in forgetClient "+
			"or forgetFilters. They are two maps that must stay in step, and a "+
			"removal written anywhere else is one a later departure path copies "+
			"without the second map - which leaves a partition declaration behind "+
			"for the next SUBSCRIBE on that filter to inherit silently", o)
	}
	// **Said only when it is true.** This line reported "all inside" beside
	// its own failures in the first draft, which is a sweep describing work
	// it did not do - the thing the count above exists to prevent, one line
	// further on.
	if len(outside) == 0 {
		t.Logf("%d removals from b.subs / b.partitions, all inside %v",
			removals, keys(mayRemove))
	}
}

// guardedName is the name of the guarded map an expression selects on the
// broker, or "" for anything else. It matches `b.subs` and `b.partitions`
// however the receiver is spelled, so a rename of the receiver cannot
// quietly take the rule with it.
func guardedName(e ast.Expr) string {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || !guarded[sel.Sel.Name] {
		return ""
	}
	if _, ok := sel.X.(*ast.Ident); !ok {
		return ""
	}
	return sel.Sel.Name
}

// isAppendTo reports whether an expression is append(b.<name>[...], ...) -
// the one shape that adds to an entry rather than removing from it.
func isAppendTo(e ast.Expr, name string) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "append" {
		return false
	}
	if len(call.Args) == 0 {
		return false
	}
	ix, ok := call.Args[0].(*ast.IndexExpr)
	return ok && guardedName(ix.X) == name
}

// isCreation reports whether an expression makes a fresh empty container -
// `map[k]v{}` or `make(...)` - which adds an entry rather than removing
// from one.
func isCreation(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.CompositeLit:
		return true
	case *ast.CallExpr:
		id, ok := x.Fun.(*ast.Ident)
		return ok && id.Name == "make"
	}
	return false
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The rule for the two indexes derived from subs: byTopic and members,
// with indexed recording what was last written to them.
//
// **They are subs turned round, and a writer of subs that does not reindex
// leaves them saying something subs does not.** The cost is silent: a
// client missing from byTopic is subscribed, holds its position, and is
// never sent another record. TestTheTopicIndexesAgreeWithTheSubscriptions
// holds that the four writers there are today keep them in step; this holds
// that a fifth cannot be added without doing so, and that nothing but
// reindex writes them at all.
//
// byTopic is held to more than "no writes": its trie is changed through
// method calls on whatever the map hands back, so a writer could take the
// value into a local and change it there. It may therefore be named at all
// only in reindex, which writes it, and matchingLocked, which reads it.
var (
	derived        = map[string]bool{"byTopic": true, "members": true, "indexed": true}
	mayNameByTopic = map[string]bool{"reindex": true, "matchingLocked": true}
)

func TestEveryWriterOfTheSubscriptionIndexReindexes(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}

	var subsWriters, derivedWrites int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") ||
			strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			writesSubs, reindexes := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "reindex" {
						if _, ok := sel.X.(*ast.Ident); ok {
							reindexes = true
						}
					}
					if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "delete" && len(x.Args) > 0 {
						switch root := brokerField(x.Args[0]); {
						case root == "subs":
							writesSubs = true
						case derived[root]:
							derivedWrites++
							if name != "reindex" {
								t.Errorf("%s: delete on b.%s in %s(); only reindex writes it",
									fset.Position(x.Pos()), root, name)
							}
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						switch root := brokerField(lhs); {
						case root == "subs":
							writesSubs = true
						case derived[root]:
							derivedWrites++
							if name != "reindex" {
								t.Errorf("%s: assignment to b.%s in %s(); only reindex writes it",
									fset.Position(x.Pos()), root, name)
							}
						}
					}
				case *ast.SelectorExpr:
					if x.Sel.Name == "byTopic" && !mayNameByTopic[name] {
						if _, ok := x.X.(*ast.Ident); ok {
							t.Errorf("%s: b.byTopic named in %s(); only reindex and matchingLocked "+
								"may, because its trie is changed through whatever the map hands back",
								fset.Position(x.Pos()), name)
						}
					}
				}
				return true
			})
			if writesSubs {
				subsWriters++
				if !reindexes {
					t.Errorf("%s() writes b.subs and never calls reindex: byTopic and members "+
						"then disagree with it, and a subscriber they miss is never sent a record",
						name)
				}
			}
		}
	}

	// Counted, so a walk gone blind cannot pass. Four functions write subs
	// today (track, forgetSubscriptions, forgetFilters, forgetClient), and
	// reindex writes the derived maps in several places.
	t.Logf("%d functions write b.subs; %d writes to the derived indexes", subsWriters, derivedWrites)
	if subsWriters < 4 || derivedWrites == 0 {
		t.Fatalf("found %d writers of b.subs and %d writes to the derived indexes, which "+
			"cannot be this package: the walk has stopped matching the code it guards",
			subsWriters, derivedWrites)
	}
}

// brokerField is the broker field an expression writes through - `subs`
// for b.subs, b.subs[id] or b.subs[id][i] - or "" for anything else.
func brokerField(e ast.Expr) string {
	for {
		ix, ok := e.(*ast.IndexExpr)
		if !ok {
			break
		}
		e = ix.X
	}
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	if _, ok := sel.X.(*ast.Ident); !ok {
		return ""
	}
	return sel.Sel.Name
}
