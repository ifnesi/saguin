package broker

// What ending a session removes, as a rule over the source.
//
// RFC 0003 "Sessions": a session ends in one place, and all of it ends there.
// That place is endSession, and every way a session ends calls it - an expiry
// (OnClientExpired), a disconnect that discards the session (OnDisconnect), a
// clean start (OnSessionRegistered, from what confirmClaim hands it) and a
// resume retention passed (OnSessionEstablish).
// Each of those paths used to remove its own subset, and each forgot something
// the others remembered: the expiry path the partition declarations, the clean
// start the held exactly-once publishes, all but the expiry a shared group's
// backlog, and the retention-floor discard a Will still waiting.
//
// A test driving one path passes while the next path added forgets, so this
// walks the source instead. It finds every call of every removal primitive and
// requires each to be in endSession or on a named list, justified by what the
// function around it is; endSession has to call every primitive, and every
// ending has to call endSession.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// removalPrimitives are the calls that remove a piece of a session's state.
// latestSeen.drop is spelled with its receiver: `drop` alone names other
// things. A shared group's backlog, and what its groups had handed the
// session, go with its record (dropSessionRecord, through endStoredSession);
// endLent is what its groups had lent it as a clean member.
var removalPrimitives = []string{"latestSeen.drop", "forgetSessionDeliveriesLocked", "forgetClient",
	"dropStoredPositions", "DropPosition", "DropReader", "dropHeldPublishes",
	"endLent", "takeWillAtSessionEnd", "dropSessionRecord", "dropRecordAfterItsWill"}

// endSessionRemoves is what endSession itself calls: every primitive but the
// two store operations dropStoredPositions makes for it.
var endSessionRemoves = []string{"latestSeen.drop", "forgetSessionDeliveriesLocked", "forgetClient",
	"dropStoredPositions", "dropHeldPublishes", "endLent",
	"takeWillAtSessionEnd", "dropSessionRecord"}

// notEndings may call a removal primitive without being endSession, each for
// what the function is rather than for who calls it.
var notEndings = map[string]map[string]string{
	"(*Broker).dropStoredPositions": {
		"DropPosition": "the primitive's own body: it drops one channel's position",
		"DropReader":   "the primitive's own body: it drops every position a provider holds for the reader",
	},
	"(countingLog).DropPosition": {
		"DropPosition": "a wrapper passing the call to the channel store it counts failures for",
	},
	"(countingDropper).DropReader": {
		"DropReader": "a wrapper passing the call to the provider it counts failures for",
	},
	"(*Broker).OnClientExpired": {
		"dropRecordAfterItsWill": "an expiry is the ending that drops a record still holding its Will: " +
			"endSession hands the Will back, and the expiry publishes it and drops the record straight after",
	},
	"(*Broker).PublishDueWills": {
		"dropRecordAfterItsWill": "a start's ending, kept until its Will is published: the record goes " +
			"straight after the Will, as at an expiry, and nothing else of the session was restored",
	},
	"(*Broker).dropRecordAfterItsWill": {
		"dropSessionRecord": "the primitive's own body: it drops the record, remembering a refused drop",
	},
}

// sessionEndings are the ways a session ends, and each has to reach endSession.
var sessionEndings = []string{"(*Broker).OnClientExpired", "(*Broker).disconnectLocked",
	"(*Broker).OnSessionRegistered", "(*Broker).OnSessionEstablish"}

func TestEveryPathThatEndsASessionDropsTheSameState(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	isPrimitive := map[string]bool{}
	for _, p := range removalPrimitives {
		isPrimitive[p] = true
	}

	type site struct {
		fn, primitive string
		at            token.Position
	}
	var sites []site
	callsEndSession := map[string]bool{}
	files := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files++
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// **Keyed by receiver and name, not by name.** Nine names in this
			// package are borne by two functions, and keyed by name alone the
			// second would silently stand for the first.
			name := qualifiedFuncName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				called := sel.Sel.Name
				if called == "endSession" {
					callsEndSession[name] = true
				}
				// `b.latestSeen.drop(id)` is the only spelling that can
				// remove a client's latest positions: latestSeen is a type
				// with its own lock, and its map is behind methods.
				if called == "drop" {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "latestSeen" {
						called = "latestSeen.drop"
					}
				}
				if isPrimitive[called] {
					sites = append(sites, site{name, called, fset.Position(call.Pos())})
				}
				return true
			})
		}
	}

	// Counted, so a walk that has stopped seeing the shape cannot pass on
	// nothing: at the least endSession's own calls and the named sites.
	if files == 0 || len(sites) < len(endSessionRemoves)+len(notEndings) {
		t.Fatalf("walked %d files and found %d call sites of the removal primitives, fewer than "+
			"endSession and the named functions make on their own: this check has stopped "+
			"matching the code it guards", files, len(sites))
	}

	inEndSession := map[string]bool{}
	namedSeen := map[string]bool{}
	for _, s := range sites {
		if s.fn == "(*Broker).endSession" {
			inEndSession[s.primitive] = true
			continue
		}
		if why := notEndings[s.fn][s.primitive]; why != "" {
			namedSeen[s.fn+" "+s.primitive] = true
			continue
		}
		t.Errorf("%s: %s calls %s outside endSession: a session's state is removed in one "+
			"place (RFC 0003 \"Sessions\"), or the path that removes it here is one the next "+
			"ending will not match - end it through endSession, or name this function in "+
			"notEndings by what it is", s.at, s.fn, s.primitive)
	}
	for _, p := range endSessionRemoves {
		if !inEndSession[p] {
			t.Errorf("endSession does not call %s, so no ending removes that piece of a session", p)
		}
	}
	// A named exception that no longer matches anything is a list that has
	// stopped describing the code.
	for fn, prims := range notEndings {
		for p := range prims {
			if !namedSeen[fn+" "+p] {
				t.Errorf("notEndings names %s calling %s, and the walk found no such call", fn, p)
			}
		}
	}
	for _, e := range sessionEndings {
		if !callsEndSession[e] {
			t.Errorf("%s ends a session and does not call endSession", e)
		}
	}

	var byFn []string
	seen := map[string]bool{}
	for _, s := range sites {
		if !seen[s.fn] {
			seen[s.fn] = true
			byFn = append(byFn, s.fn)
		}
	}
	sort.Strings(byFn)
	t.Logf("%d call sites of %d removal primitives in %d files, in %v", len(sites),
		len(removalPrimitives), files, byFn)
}

// refusesWills is a session store with no room for any Will.
type refusesWills struct{ *store.Sessions }

func (s refusesWills) Save(sess store.Session) error {
	if sess.Will != nil {
		return store.ErrFull
	}
	return s.Sessions.Save(sess)
}

// Begin keeps a new session's record as Save does, and has no room for a Will
// either.
func (s refusesWills) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if next != nil && next.Will != nil {
		return store.Dropped{}, store.ErrFull
	}
	return s.Sessions.Begin(client, held, next)
}

// A connection refused at OnSessionEstablish leaves nothing behind: not its
// Will's properties, kept by the connection and otherwise kept for as long as
// the broker runs (invariant 13), not its claim on the id, which would leave
// the live session unable to end, and not a count as an accepted connection.
// Measured before: a hundred refusals left a hundred of each.
func TestAConnectionRefusedAtItsSessionLeavesNothingBehind(t *testing.T) {
	b := metricsBroker(t)
	b.SetServer(mqtt.New(&mqtt.Options{}))
	b.SetSessions("local", refusesWills{store.NewSessions()})
	live := &mqtt.Client{ID: "dev"}
	b.mu.Lock()
	b.owner["dev"] = live
	b.mu.Unlock()

	for i := 0; i < 100; i++ {
		cl := &mqtt.Client{ID: "dev"}
		cl.Properties.ProtocolVersion = 5
		pk := packets.Packet{ProtocolVersion: 5, Connect: &packets.ConnectParams{
			ClientIdentifier: "dev", Clean: true,
			WillFlag: true, WillTopic: "wills/dev", WillPayload: []byte("gone"),
		}}
		if err := b.OnSessionEstablish(cl, pk); err == nil {
			t.Fatal("a CONNECT whose Will the store had no room for was accepted, so this proves nothing")
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.willProps); n != 0 {
		t.Errorf("%d refused connections left their Will properties behind", n)
	}
	if n := len(b.claims); n != 0 {
		t.Errorf("%d refused connections left a claim on the id", n)
	}
	if b.owner["dev"] != live {
		t.Error("the id is no longer the live session's after connections under it were refused")
	}
	if n := b.counted.connections.Load(); n != 0 {
		t.Errorf("%d refused connections were counted as accepted", n)
	}
}

// RFC 0003 "Sessions": a CONNECT for a client id no session is held for
// begins a new session, whichever Clean Start it asked for, and nothing the
// store holds under the id is read for it. The substrate holds every session
// kept, so what the store has there is an ended session's, which an ending
// the store refused left: a Clean Start 0 session read it and was given its
// subscriptions, and a read that failed refused the CONNECT with 0x83.
//
// So keepSession reads the session store only where a session is held for
// the id, inside `if predecessor`. The store is what b.sessionStore()
// answered; any method called on it that reads is a read.
func TestAConnectForAnIDNoSessionIsHeldForReadsNothingOfTheStore(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "session.go", nil, 0)
	if err != nil {
		t.Fatalf("parse session.go: %v", err)
	}
	var keep *ast.FuncDecl
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && qualifiedFuncName(fn) == "(*Broker).keepSession" {
			keep = fn
		}
	}
	if keep == nil {
		t.Fatal("session.go has no (*Broker).keepSession: this check has stopped matching the code it guards")
	}
	// The store's name, from `<name> := b.sessionStore()`.
	var storeVar string
	ast.Inspect(keep.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if call, ok := as.Rhs[0].(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "sessionStore" {
				storeVar = as.Lhs[0].(*ast.Ident).Name
			}
		}
		return true
	})
	if storeVar == "" {
		t.Fatal("keepSession takes no store from b.sessionStore(): this check has stopped matching the code it guards")
	}
	// The body of `if predecessor { ... }`, where a session is held.
	var held *ast.BlockStmt
	ast.Inspect(keep.Body, func(n ast.Node) bool {
		if is, ok := n.(*ast.IfStmt); ok {
			if id, ok := is.Cond.(*ast.Ident); ok && id.Name == "predecessor" && held == nil {
				held = is.Body
			}
		}
		return true
	})
	if held == nil {
		t.Fatal("keepSession has no `if predecessor` block: this check has stopped matching the code it guards")
	}
	reads := map[string]bool{"Get": true, "All": true, "InFlight": true}
	var inside, outside []string
	ast.Inspect(keep.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// A read of the store directly, or through b.getSession(store, id),
		// which puts back a record owed to it first.
		switch {
		case sel.Sel.Name == "getSession" && len(call.Args) > 0:
			if id, ok := call.Args[0].(*ast.Ident); !ok || id.Name != storeVar {
				return true
			}
		case reads[sel.Sel.Name]:
			if id, ok := sel.X.(*ast.Ident); !ok || id.Name != storeVar {
				return true
			}
		default:
			return true
		}
		at := fmt.Sprintf("%s.%s at %s", storeVar, sel.Sel.Name, fset.Position(call.Pos()))
		if call.Pos() >= held.Pos() && call.End() <= held.End() {
			inside = append(inside, at)
		} else {
			outside = append(outside, at)
		}
		return true
	})
	// Counted: the resume's read of what it resumes is the one expected.
	if len(inside) == 0 {
		t.Fatalf("keepSession reads nothing of the store where a session is held, where it reads the " +
			"record a resume keeps: this check has stopped matching the code it guards")
	}
	for _, at := range outside {
		t.Errorf("keepSession reads the session store where no session is held for the id: %s", at)
	}
	t.Logf("store reads where a session is held: %v", inside)
}

// Invariants 17 and 18: what the store owes a client id - a disconnect, a
// record put back, an ending - is settled by the CONNECT for it before that
// CONNECT claims the id (settleOwed), so no later write of it can land on
// the session the CONNECT begins. That holds only while every way a
// connection becomes the id's owner passes through settleOwed first, so it
// is pinned here over the source: the owner map is written only where a
// claim is made (claimID) or given back (settleClaim), claimID is called in
// one place, and there an unconditional settleOwed, whose failure returns,
// comes before it.
func TestEveryClaimOfAClientIDSettlesWhatIsOwedFirst(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	writers := map[string]int{}
	var claims []string
	var establish *ast.FuncDecl
	files := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files++
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := qualifiedFuncName(fn)
			if name == "(*Broker).OnSessionEstablish" {
				establish = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.AssignStmt:
					for _, l := range v.Lhs {
						ix, ok := l.(*ast.IndexExpr)
						if !ok {
							continue
						}
						if sel, ok := ix.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "owner" {
							writers[name]++
						}
					}
				case *ast.CallExpr:
					if sel, ok := v.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "claimID" {
						claims = append(claims, name)
					}
				}
				return true
			})
		}
	}
	if files == 0 || establish == nil || len(writers) == 0 {
		t.Fatalf("walked %d files, found OnSessionEstablish=%v and %d writers of the owner map: this "+
			"check has stopped matching the code it guards", files, establish != nil, len(writers))
	}
	for fn, n := range writers {
		if fn != "(*Broker).claimID" && fn != "(*Broker).settleClaim" {
			t.Errorf("%s writes the owner map %d time(s): a connection becomes the id's owner only by a "+
				"claim (claimID), which settles what is owed first, or by one given back (settleClaim)", fn, n)
		}
	}
	if len(claims) != 1 || claims[0] != "(*Broker).OnSessionEstablish" {
		t.Fatalf("claimID is called from %v, want only OnSessionEstablish, where settleOwed precedes it", claims)
	}
	// In OnSessionEstablish's own statement list: an `if err := b.settleOwed(..);
	// err != nil { ... return ... }` before the statement that calls claimID.
	settleAt, claimAt := -1, -1
	for i, st := range establish.Body.List {
		if is, ok := st.(*ast.IfStmt); ok && settleAt < 0 {
			if as, ok := is.Init.(*ast.AssignStmt); ok && len(as.Rhs) == 1 && len(as.Lhs) == 1 {
				if call, ok := as.Rhs[0].(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "settleOwed" {
						// The condition is exactly `<v> != nil`, v being what
						// receives settleOwed's answer: any other condition
						// can let a failed settle go on to claim the id.
						v, _ := as.Lhs[0].(*ast.Ident)
						cond, _ := is.Cond.(*ast.BinaryExpr)
						var x, y *ast.Ident
						if cond != nil {
							x, _ = cond.X.(*ast.Ident)
							y, _ = cond.Y.(*ast.Ident)
						}
						if v == nil || cond == nil || cond.Op != token.NEQ || x == nil || x.Name != v.Name ||
							y == nil || y.Name != "nil" {
							t.Errorf("OnSessionEstablish tests settleOwed's answer with %q, want `%s != nil`",
								types.ExprString(is.Cond), types.ExprString(as.Lhs[0]))
						}
						returns := false
						for _, b := range is.Body.List {
							if _, ok := b.(*ast.ReturnStmt); ok {
								returns = true
							}
						}
						if !returns {
							t.Errorf("OnSessionEstablish goes on where settleOwed fails: a CONNECT would claim " +
								"an id the store still owes a write for")
						}
						settleAt = i
					}
				}
			}
		}
		ast.Inspect(st, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "claimID" && claimAt < 0 {
					claimAt = i
				}
			}
			return true
		})
	}
	if settleAt < 0 || claimAt < 0 || settleAt >= claimAt {
		t.Errorf("OnSessionEstablish settles what is owed at statement %d and claims the id at %d: "+
			"settleOwed has to come first, unconditionally", settleAt, claimAt)
	}
	t.Logf("owner map written in %v; claimID called from %v; settleOwed at statement %d, claimID at %d",
		writers, claims, settleAt, claimAt)
}
