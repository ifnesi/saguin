package mqtt

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/sourcetree"
)

// endSource is one non-test Go file of the repository, parsed.
type endSource struct {
	rel  string
	fset *token.FileSet
	f    *ast.File
}

// parseRepository parses every non-test Go file in the repository.
func parseRepository(t *testing.T) (string, []endSource) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var out []endSource
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
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		out = append(out, endSource{rel: filepath.ToSlash(rel), fset: fset, f: f})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return root, out
}

// endSite is one place that decides a connection's end, or touches what
// records it: the call, the function it is in, and what kind of place it is.
type endSite struct {
	src  endSource
	fn   *ast.FuncDecl
	node ast.Node
	kind string
}

func (s endSite) key() string { return s.src.rel + ":" + s.fn.Name.Name + ":" + s.kind }

func (s endSite) line() int { return s.src.fset.Position(s.node.Pos()).Line }

func isPacketsSel(e ast.Expr, names ...string) (string, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "packets" {
		return "", false
	}
	for _, n := range names {
		if sel.Sel.Name == n {
			return n, true
		}
	}
	return "", false
}

// socketNames are what the engine and its listeners call a connection's
// socket: a Close on one of them closes a connection.
var socketNames = map[string]bool{"c": true, "conn": true, "nc": true, "netConn": true, "raw": true}

// findEndSites returns every place in the repository's non-test code that
// decides, claims or finishes a connection's end (Client.claimEnd): a call to
// beginEnd, which decides one; to claimEnd, which claims one, and to
// finishEnd, awaitEnd and disconnectOwned, which its owner and the others
// call after; a close of a client's connection; a packet built as a
// DISCONNECT or a CONNACK, the two that end a connection when written; a
// SendConnack whose code is not Success; every change to the state the end
// is kept in; and a recover, which could carry a goroutine past the close an
// owner owes.
func findEndSites(srcs []endSource) []endSite {
	var sites []endSite
	for _, src := range srcs {
		for _, decl := range src.f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			add := func(n ast.Node, kind string) { sites = append(sites, endSite{src, fn, n, kind}) }
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "recover" {
						add(n, "recover")
					}
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "beginEnd", "claimEnd", "claimEndForConnack", "finishEnd", "awaitEnd", "disconnectOwned":
						add(n, sel.Sel.Name)
					case "Close":
						if c, ok := sel.X.(*ast.SelectorExpr); ok && c.Sel.Name == "Conn" {
							if nt, ok := c.X.(*ast.SelectorExpr); ok && nt.Sel.Name == "Net" {
								add(n, "close")
							}
						}
						// Any other socket: a connection, by what it is named
						// in the engine and its listeners.
						if id, ok := sel.X.(*ast.Ident); ok && len(n.Args) == 0 && socketNames[id.Name] &&
							strings.HasPrefix(src.rel, "internal/mqtt/") {
							add(n, "close socket")
						}
					case "SendConnack":
						if len(n.Args) > 1 {
							if _, ok := isPacketsSel(n.Args[1], "CodeSuccess"); !ok {
								add(n, "refusing CONNACK")
							}
						}
					case "Store", "CompareAndSwap", "Swap", "Add":
						if io, ok := sel.X.(*ast.SelectorExpr); ok && io.Sel.Name == "io" {
							if st, ok := io.X.(*ast.SelectorExpr); ok && st.Sel.Name == "State" {
								add(n, "io")
							}
						}
					}
				case *ast.KeyValueExpr:
					if k, ok := n.Key.(*ast.Ident); ok && k.Name == "Type" {
						if name, ok := isPacketsSel(n.Value, "Disconnect", "Connack"); ok {
							add(n, name)
						}
					}
				case *ast.AssignStmt:
					for i, l := range n.Lhs {
						if sel, ok := l.(*ast.SelectorExpr); ok && sel.Sel.Name == "Type" && i < len(n.Rhs) {
							if name, ok := isPacketsSel(n.Rhs[i], "Disconnect", "Connack"); ok {
								add(n, name)
							}
						}
					}
				}
				return true
			})
		}
	}
	return sites
}

// TestEveryDecidedEndIsClosed: **an end has one owner, which closes the
// connection, and everything else that would end it waits for that close**
// (Client.claimEnd) - a read loop among them, with no bound of its own, so an
// end claimed and never closed would hold that loop, its connection and its
// session for good. Walks the syntax tree of every non-test Go file for every
// place that decides, claims or finishes an end, and holds each to the claim
// it makes and the line that closes the connection after it. A place not in
// the table fails, as does one in it that is gone, until somebody has said
// what it claims and what closes it.
func TestEveryDecidedEndIsClosed(t *testing.T) {
	_, srcs := parseRepository(t)
	sites := findEndSites(srcs)

	type closer struct {
		n   int
		how string
		// check holds the claim in how to the code, where the syntax tree
		// can show it: nil where how names the row that does.
		check func(endSite) error
	}
	stops := func(s endSite) error { return callFollows(s, "Stop") }
	table := map[string]closer{
		// Claims, and what the owner and the others then do.
		"internal/mqtt/clients.go:stop:claimEnd": {1,
			"Stop: claims; the owner closes (finishEnd), the others wait for that close", ownerFinishes},
		"internal/mqtt/server.go:DisconnectClient:claimEnd": {1,
			"claims; the owner writes the DISCONNECT and closes (disconnectOwned), the others wait",
			loserWaits("disconnectOwned")},
		"internal/mqtt/server.go:refuse:claimEnd": {1,
			"refuseConnection: claims; the owner counts the refusal, writes it and closes, the others wait",
			loserWaits("disconnectOwned")},
		"internal/mqtt/clients.go:stop:finishEnd": {1, "the owner's close, in the claiming branch", nil},
		"internal/mqtt/server.go:disconnectOwned:finishEnd": {1,
			"the owner's close, after its DISCONNECT whatever the write returned: the Disconnect row", nil},
		"internal/mqtt/server.go:DisconnectClient:disconnectOwned": {1, "only past a claim won", afterClaimWon},
		"internal/mqtt/server.go:refuse:disconnectOwned":           {1, "only past a claim won", afterClaimWon},
		"internal/mqtt/clients.go:stop:awaitEnd":                   {1, "a claim lost: waits, closes nothing", nil},
		"internal/mqtt/server.go:DisconnectClient:awaitEnd":        {1, "a claim lost: waits, closes nothing", nil},
		"internal/mqtt/server.go:refuse:awaitEnd":                  {1, "a claim lost: waits, closes nothing", nil},
		"internal/mqtt/clients.go:finishEnd:close": {1,
			"the one close of a client's connection, the owner's", nil},
		"internal/mqtt/server.go:disconnectOwned:Disconnect": {1,
			"the one DISCONNECT, built by the owner and written only on an end claimed (WritePacket)",
			func(s endSite) error { return callFollows(s, "finishEnd") }},

		// Ends decided without a claim, and who claims them.
		"internal/mqtt/clients.go:WritePacket:claimEndForConnack": {1,
			"a refusing CONNACK, written before the connection is registered: the goroutine refusing it " +
				"owns the end, and its own Stop finishes it (the refusing CONNACK rows); a shutdown waits",
			inConnackBranch},
		"internal/mqtt/server.go:processDisconnect:claimEnd": {1,
			"a clean DISCONNECT claims under the id's session lock, before anything it changes is stored, " +
				"and holds the end through the store and the close; a claim lost stores nothing and waits",
			func(s endSite) error {
				if !sessionLockHeld(s.fn.Body, s.node.Pos()) {
					return fmt.Errorf("claimed outside the session lock, which a takeover waiting for this end holds")
				}
				return loserWaits("finishEnd")(s)
			}},
		"internal/mqtt/server.go:processDisconnect:awaitEnd":  {1, "a claim lost: waits, closes nothing", nil},
		"internal/mqtt/server.go:processDisconnect:finishEnd": {2, "the owner's close, after the store", nil},
		"internal/mqtt/server.go:closeGoverned:claimEnd": {1,
			"a shutdown reaching a socket its client governs: claims; a claim lost cuts an unbounded write " +
				"and waits", loserWaits("finishEnd")},
		"internal/mqtt/server.go:closeGoverned:awaitEnd":  {1, "a claim lost: waits, closes nothing", nil},
		"internal/mqtt/server.go:closeGoverned:finishEnd": {1, "the owner's close", nil},
		"internal/mqtt/clients.go:claimEndForConnack:io":  {1, "claims, for the refusing goroutine", nil},
		"internal/mqtt/listeners/listeners.go:CloseAll:close socket": {1,
			"only a socket no client governs yet (Govern); a governed one is ended through its client",
			inNilEndBranch},
		"internal/mqtt/listeners/listeners.go:Establish:close socket": {1,
			"refused as shutdown began, before any client exists", nil},
		"internal/mqtt/listeners/admission.go:Accept:close socket": {1,
			"refused at accept, before any client exists", nil},
		"internal/mqtt/listeners/net.go:Serve:close socket": {1,
			"accepted as the door shut, before any client exists", nil},
		"internal/mqtt/listeners/tcp.go:Serve:close socket": {1,
			"accepted as the door shut, before any client exists", nil},
		"internal/mqtt/listeners/unixsock.go:Serve:close socket": {1,
			"accepted as the door shut, before any client exists", nil},
		"internal/mqtt/listeners/websocket.go:handler:close socket": {1,
			"deferred: runs once Establish has returned, its client's end finished", nil},
		"internal/mqtt/server.go:attachClient:beginEnd": {1,
			"no read loop runs: attachClient returns, and its deferred endAttach claims with Stop", endAttachCloses},
		"internal/mqtt/server.go:detachClient:beginEnd": {1,
			"the read loop has returned: refuse or stop claims, or endAttach's Stop", endAttachCloses},
		"internal/mqtt/server.go:processDisconnect:beginEnd": {1,
			"on the read loop: returns an error, which the loop returns, or Stop claims before nil",
			func(s endSite) error { return nilReturnsAfterStop(s.fn) }},
		"internal/mqtt/server.go:SendConnack:Connack": {2,
			"refusing only at a CONNECT, before the read loop: the refusing CONNACK rows", nil},
		"internal/mqtt/server.go:refuseBusy:refusing CONNACK": {1,
			"before the CONNECT is read: admit fails, attachClient returns, endAttach's Stop claims",
			preReadOnly(srcs)},
		"internal/mqtt/server.go:refuseUnreadConnect:refusing CONNACK": {1,
			"establishClient returns false, attachClient returns, endAttach's Stop claims", preReadOnly(srcs)},
		"internal/mqtt/server.go:establishClient:refusing CONNACK": {4,
			"three refusals each followed by return false, so attachClient returns and endAttach's Stop " +
				"claims; the fourth writes validateConnect's code once any but Success has returned",
			establishRefuses},
		"internal/broker/auth.go:OnConnectAuthenticate:Connack":        {1, "cl.Stop claims after the write", stops},
		"internal/broker/auth.go:sessionCapabilities:refusing CONNACK": {4, "cl.Stop claims after the write", stops},
		"internal/broker/admission.go:OnConnect:refusing CONNACK": {6,
			"OnConnect returns the code, establishClient returns false, endAttach's Stop claims",
			returnsRefusal(srcs)},

		// The state the end is kept in.
		"internal/mqtt/clients.go:beginEnd:io":   {2, "decides", nil},
		"internal/mqtt/clients.go:claimEnd:io":   {4, "claims", nil},
		"internal/mqtt/clients.go:beginWrite:io": {4, "a write's own state; an ended connection stays ended", nil},
		"internal/mqtt/clients.go:endWrite:io":   {1, "a write's own state; an ended connection stays ended", nil},
	}

	found := map[string][]endSite{}
	for _, s := range sites {
		found[s.key()] = append(found[s.key()], s)
	}
	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var lines []string
		for _, s := range found[k] {
			lines = append(lines, fmt.Sprint(s.line()))
		}
		c, ok := table[k]
		t.Logf("%s at line %s: %s", k, strings.Join(lines, ", "), c.how)
		if !ok {
			t.Errorf("%s at line %s decides, claims or finishes a connection's end and nothing here says "+
				"what it claims or what closes it: a read loop that finds it decided waits for that close",
				k, strings.Join(lines, ", "))
			continue
		}
		if len(found[k]) != c.n {
			t.Errorf("%s: %d sites, the table has %d", k, len(found[k]), c.n)
		}
		if c.check != nil {
			for _, s := range found[k] {
				if err := c.check(s); err != nil {
					t.Errorf("%s at line %d: %v", k, s.line(), err)
				}
			}
		}
	}
	for k := range table {
		if _, ok := found[k]; !ok {
			t.Errorf("%s is in the table and the walk no longer finds it", k)
		}
	}
	t.Logf("%d files walked, %d sites", len(srcs), len(sites))
	for _, rel := range []string{"internal/mqtt/clients.go", "internal/mqtt/server.go", "internal/broker/broker.go"} {
		if srcFile(srcs, rel).f == nil {
			t.Fatalf("the walk did not reach %s: it is not walking the repository", rel)
		}
	}
}

// TestNothingWaitsForAnEndHoldingWhatItsOwnerNeeds: **an owner, between its
// claim and its close, takes the client's lock and nothing else** - for the
// DISCONNECT it writes - so a goroutine that waits for an end (Stop,
// DisconnectClient, refuseConnection, and the saguin calls that reach them)
// holding the client's lock would wait for ever. Walks the syntax tree of
// every non-test Go file for every call that can wait for an end, and fails
// on one made while its function holds a client's lock it took; and holds
// the owner's own path to the client's lock alone.
func TestNothingWaitsForAnEndHoldingWhatItsOwnerNeeds(t *testing.T) {
	_, srcs := parseRepository(t)
	waiters := map[string]bool{"Stop": true, "stop": true, "DisconnectClient": true, "refuseConnection": true,
		"refuse": true, "awaitEnd": true, "endedElsewhere": true, "endConnection": true, "disconnect": true}
	clients := map[string]bool{"cl": true, "existing": true, "client": true, "c": true}
	var examined int
	// A wait under a client id's session lock is safe because no owner takes
	// one (below), and each is named here so that a new one is looked at.
	// The takeovers that wait under one - inheritClientSession, and saguin's
	// OnSessionEstablish - hold it from their caller, establishClient, which
	// a walk of one function does not see; the owner's side covers them.
	underSession := map[string][]int{}
	allowedUnderSession := map[string]string{}
	for _, src := range srcs {
		for _, decl := range src.f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := c.Fun.(*ast.SelectorExpr)
				if !ok || !waiters[sel.Sel.Name] {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok && (x.Name == "t" || x.Name == "timer" || x.Name == "hook") {
					return true
				}
				examined++
				if held := clientLockHeld(fn.Body, c.Pos(), clients); held != "" {
					t.Errorf("%s:%d in %s: %s waits for an end holding %s.Lock()", src.rel,
						src.fset.Position(c.Pos()).Line, fn.Name.Name, sel.Sel.Name, held)
				}
				if sessionLockHeld(fn.Body, c.Pos()) {
					key := src.rel + ":" + fn.Name.Name
					underSession[key] = append(underSession[key], src.fset.Position(c.Pos()).Line)
				}
				return true
			})
		}
	}
	for key, lines := range underSession {
		why, ok := allowedUnderSession[key]
		t.Logf("%s at %v waits for an end under a session lock: %s", key, lines, why)
		if !ok {
			t.Errorf("%s at %v waits for an end holding a client id's session lock, and nothing here says why "+
				"that is safe", key, lines)
		}
	}
	for key := range allowedUnderSession {
		if _, ok := underSession[key]; !ok {
			t.Errorf("%s is allowed to wait under a session lock and the walk no longer finds it doing so", key)
		}
	}
	t.Logf("%d calls that can wait for an end examined", examined)
	if examined < 30 {
		t.Fatalf("only %d calls that can wait for an end found: the walk is not reaching them", examined)
	}

	// The owner's path: the claim's winner calls disconnectOwned or
	// finishEnd, and the DISCONNECT goes through WritePacket.
	for _, name := range []string{"disconnectOwned", "refuse", "finishEnd", "WritePacket", "writePacketAs"} {
		var fn *ast.FuncDecl
		for _, rel := range []string{"internal/mqtt/clients.go", "internal/mqtt/server.go"} {
			if f := findFunc(srcs, rel, name); f != nil {
				fn = f
			}
		}
		if fn == nil {
			t.Fatalf("%s is not in internal/mqtt", name)
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var callee string
			switch f := c.Fun.(type) {
			case *ast.SelectorExpr:
				callee = f.Sel.Name
				if (callee == "Lock" || callee == "RLock") && !isIdent(f.X, "cl") {
					t.Errorf("%s takes a lock other than the client's on the owner's path", name)
				}
			case *ast.Ident:
				callee = f.Name
			}
			if callee == "lock" || callee == "LockSession" || callee == "lockSession" {
				t.Errorf("%s takes a session lock on the owner's path", name)
			}
			return true
		})
	}
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// sessionLockHeld is whether body has taken a client id's session lock before
// pos - `unlock := ....lock(...)`, lockSession or LockSession - and not
// called that unlock between them other than deferred.
func sessionLockHeld(body *ast.BlockStmt, pos token.Pos) bool {
	held := map[string]bool{}
	deferred := map[*ast.CallExpr]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok {
			deferred[d.Call] = true
		}
		return true
	})
	var nodes []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if _, lit := n.(*ast.FuncLit); lit && (pos < n.Pos() || n.End() < pos) {
			return false
		}
		// A branch that returns, and does not hold pos, releases its lock
		// on its own way out: the path to pos does not run it.
		if ifs, ok := n.(*ast.IfStmt); ok && (pos < ifs.Body.Pos() || ifs.Body.End() < pos) &&
			ifs.Else == nil && returns(ifs.Body) {
			if n.Pos() < pos {
				nodes = append(nodes, ifs.Cond)
			}
			return false
		}
		if n != nil && n.Pos() < pos {
			switch n.(type) {
			case *ast.AssignStmt, *ast.CallExpr:
				nodes = append(nodes, n)
			}
		}
		return true
	})
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Pos() < nodes[j].Pos() })
	for _, n := range nodes {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if len(n.Lhs) != 1 || len(n.Rhs) != 1 {
				continue
			}
			c, ok := n.Rhs[0].(*ast.CallExpr)
			id, isID := n.Lhs[0].(*ast.Ident)
			if ok && isID && (isCallTo(c, "lock") || isCallTo(c, "lockSession") || isCallTo(c, "LockSession")) {
				held[id.Name] = true
			}
		case *ast.CallExpr:
			if id, ok := n.Fun.(*ast.Ident); ok && held[id.Name] && !deferred[n] {
				held[id.Name] = false
			}
		}
	}
	for _, h := range held {
		if h {
			return true
		}
	}
	return false
}

// clientLockHeld names the client whose lock body has taken before pos and
// not released, where one has: a Lock call on one of clients, with no Unlock
// on the same client between it and pos that is not deferred.
func clientLockHeld(body *ast.BlockStmt, pos token.Pos, clients map[string]bool) string {
	held := map[string]bool{}
	deferred := map[*ast.CallExpr]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok {
			deferred[d.Call] = true
		}
		return true
	})
	var calls []*ast.CallExpr
	ast.Inspect(body, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok && c.Pos() < pos {
			calls = append(calls, c)
		}
		// A function literal runs on its own: one that does not hold pos
		// takes and releases its locks within itself.
		if _, lit := n.(*ast.FuncLit); lit && (pos < n.Pos() || n.End() < pos) {
			return false
		}
		return true
	})
	sort.Slice(calls, func(i, j int) bool { return calls[i].Pos() < calls[j].Pos() })
	for _, c := range calls {
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok || !clients[x.Name] {
			continue
		}
		switch sel.Sel.Name {
		case "Lock", "RLock":
			held[x.Name] = true
		case "Unlock", "RUnlock":
			if !deferred[c] {
				held[x.Name] = false
			}
		}
	}
	for name, h := range held {
		if h {
			return name
		}
	}
	return ""
}

// ownerFinishes reports an error unless the claim is the condition of an if
// whose body finishes the end and returns, and nothing after that if
// finishes it or closes the connection.
func ownerFinishes(s endSite) error {
	b, i := innermost(s)
	if b == nil {
		return fmt.Errorf("no block holds the claim")
	}
	ifs, ok := b.List[i].(*ast.IfStmt)
	cond := ifs.Cond
	if be, isOr := cond.(*ast.BinaryExpr); ok && isOr && be.Op == token.LOR && be.X == s.node {
		// claimEnd() || connackOwnsEnd(): the refusing goroutine's own end.
		if c, isCall := be.Y.(*ast.CallExpr); !isCall || !isCallTo(c, "connackOwnsEnd") {
			return fmt.Errorf("the claim is or'ed with something other than connackOwnsEnd")
		}
		cond = be.X
	}
	if !ok || cond != s.node {
		return fmt.Errorf("the claim is not the condition of an if")
	}
	if calls(&ast.FuncDecl{Name: s.fn.Name, Body: ifs.Body}, "finishEnd") != nil || !returns(ifs.Body) {
		return fmt.Errorf("the claiming branch does not finish the end and return")
	}
	for _, st := range b.List[i+1:] {
		if callsAny(st, "finishEnd", "Close", "disconnectOwned") {
			return fmt.Errorf("a claim lost goes on to finish or close the end")
		}
	}
	return nil
}

// loserWaits reports an error unless the claim, negated, is the condition of
// an if whose body waits for the close, returns, and finishes and closes
// nothing, and the statements after it call owner.
func loserWaits(owner string) func(endSite) error {
	return func(s endSite) error {
		b, i := innermost(s)
		if b == nil {
			return fmt.Errorf("no block holds the claim")
		}
		ifs, ok := b.List[i].(*ast.IfStmt)
		if !ok {
			return fmt.Errorf("the claim is not the condition of an if")
		}
		if u, ok := ifs.Cond.(*ast.UnaryExpr); !ok || u.Op != token.NOT || u.X != s.node {
			return fmt.Errorf("the if is not on the claim lost")
		}
		if !callsAny(ifs.Body, "awaitEnd") || !returns(ifs.Body) {
			return fmt.Errorf("a claim lost does not wait for the close and return")
		}
		if callsAny(ifs.Body, "finishEnd", "Close", "disconnectOwned", "WritePacket") {
			return fmt.Errorf("a claim lost writes, finishes or closes")
		}
		for _, st := range b.List[i+1:] {
			if callsAny(st, owner) {
				return nil
			}
		}
		return fmt.Errorf("the claim won does not reach %s", owner)
	}
}

// afterClaimWon reports an error unless the site comes after an
// `if !cl.claimEnd() { ... return }` in its function's body.
func afterClaimWon(s endSite) error {
	for _, st := range s.fn.Body.List {
		if st.Pos() > s.node.Pos() {
			break
		}
		ifs, ok := st.(*ast.IfStmt)
		if !ok {
			continue
		}
		if u, ok := ifs.Cond.(*ast.UnaryExpr); ok && u.Op == token.NOT {
			if c, ok := u.X.(*ast.CallExpr); ok && isCallTo(c, "claimEnd") && returns(ifs.Body) {
				return nil
			}
		}
	}
	return fmt.Errorf("not past a claim won")
}

// inConnackBranch reports an error unless the site is inside an if whose
// condition names packets.Connack.
func inConnackBranch(s endSite) error {
	found := false
	ast.Inspect(s.fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Body.Pos() > s.node.Pos() || s.node.End() > ifs.Body.End() {
			return true
		}
		ast.Inspect(ifs.Cond, func(m ast.Node) bool {
			if e, ok := m.(ast.Expr); ok {
				if _, ok := isPacketsSel(e, "Connack"); ok {
					found = true
				}
			}
			return true
		})
		return true
	})
	if !found {
		return fmt.Errorf("decided outside the refusing CONNACK's branch")
	}
	return nil
}

// inNilEndBranch reports an error unless the site is in the body of an if
// whose condition is `end == nil`.
func inNilEndBranch(s endSite) error {
	found := false
	ast.Inspect(s.fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Body.Pos() > s.node.Pos() || s.node.End() > ifs.Body.End() {
			return true
		}
		if be, ok := ifs.Cond.(*ast.BinaryExpr); ok && be.Op == token.EQL && isIdent(be.X, "end") && isIdent(be.Y, "nil") {
			found = true
		}
		return true
	})
	if !found {
		return fmt.Errorf("a socket a client may govern is closed outright")
	}
	return nil
}

// callsAny is whether n holds a call to any of names.
func callsAny(n ast.Node, names ...string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if c, ok := m.(*ast.CallExpr); ok {
			for _, name := range names {
				if isCallTo(c, name) {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// calls reports an error unless fn calls every one of names.
func calls(fn *ast.FuncDecl, names ...string) error {
	seen := map[string]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			switch f := c.Fun.(type) {
			case *ast.SelectorExpr:
				seen[f.Sel.Name] = true
			case *ast.Ident:
				seen[f.Name] = true
			}
		}
		return true
	})
	for _, n := range names {
		if !seen[n] {
			return fmt.Errorf("%s does not call %s", fn.Name.Name, n)
		}
	}
	return nil
}

// callFollows reports an error unless, in the innermost block holding the
// site, a statement after it is a call to name, and none between them can
// return.
func callFollows(s endSite, name string) error {
	b, i := innermost(s)
	if b == nil {
		return fmt.Errorf("no block holds the site")
	}
	for _, next := range b.List[i+1:] {
		if isCallStmt(next, name) {
			return nil
		}
		if returns(next) {
			return fmt.Errorf("a statement at line %d can return before %s",
				s.src.fset.Position(next.Pos()).Line, name)
		}
	}
	return fmt.Errorf("nothing after the site calls %s", name)
}

// innermost finds the innermost block holding the site, and the index of the
// statement in it that holds the site.
func innermost(s endSite) (*ast.BlockStmt, int) {
	var best *ast.BlockStmt
	idx := -1
	ast.Inspect(s.fn.Body, func(n ast.Node) bool {
		b, ok := n.(*ast.BlockStmt)
		if !ok || b.Pos() > s.node.Pos() || s.node.End() > b.End() {
			return true
		}
		for i, st := range b.List {
			if st.Pos() <= s.node.Pos() && s.node.End() <= st.End() {
				best, idx = b, i
			}
		}
		return true
	})
	return best, idx
}

// returns is whether st holds a return.
func returns(st ast.Stmt) bool {
	found := false
	ast.Inspect(st, func(n ast.Node) bool {
		if _, ok := n.(*ast.ReturnStmt); ok {
			found = true
		}
		_, lit := n.(*ast.FuncLit)
		return !found && !lit
	})
	return found
}

// stopsClient is whether st is a call to Stop.
func stopsClient(st ast.Stmt) bool { return isCallStmt(st, "Stop") }

// isCallStmt is whether st is a call to name.
func isCallStmt(st ast.Stmt, name string) bool {
	e, ok := st.(*ast.ExprStmt)
	if !ok {
		return false
	}
	c, ok := e.X.(*ast.CallExpr)
	return ok && isCallTo(c, name)
}

// nilReturnsAfterStop reports an error unless every `return nil` in fn comes
// straight after a statement calling Stop.
func nilReturnsAfterStop(fn *ast.FuncDecl) error {
	var err error
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		b, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, st := range b.List {
			r, ok := st.(*ast.ReturnStmt)
			if !ok || len(r.Results) != 1 {
				continue
			}
			if id, ok := r.Results[0].(*ast.Ident); !ok || id.Name != "nil" {
				continue
			}
			if i == 0 || !stopsClient(b.List[i-1]) && !isCallStmt(b.List[i-1], "finishEnd") {
				err = fmt.Errorf("a return of nil not straight after a Stop or the owner's close")
			}
		}
		return true
	})
	return err
}

// endAttachCloses reports an error unless the site's function is attachClient
// or detachClient, attachClient defers endAttach, and endAttach stops the
// client.
func endAttachCloses(s endSite) error {
	fns := funcsIn(s.src.f)
	attach, end := fns["attachClient"], fns["endAttach"]
	if attach == nil || end == nil {
		return fmt.Errorf("attachClient or endAttach is not in %s", s.src.rel)
	}
	deferred := false
	for _, st := range attach.Body.List {
		if d, ok := st.(*ast.DeferStmt); ok {
			if sel, ok := d.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "endAttach" {
				deferred = true
			}
		}
	}
	if !deferred {
		return fmt.Errorf("attachClient does not defer endAttach")
	}
	for _, st := range end.Body.List {
		if stopsClient(st) {
			return nil
		}
	}
	return fmt.Errorf("endAttach does not stop the client")
}

// preReadOnly reports an error unless every caller of the site's function in
// its package is admit or establishClient, which attachClient calls before
// it reads, and attachClient closes through endAttach.
func preReadOnly(srcs []endSource) func(endSite) error {
	return func(s endSite) error {
		allowed := map[string]bool{"admit": true, "establishClient": true}
		dir := filepath.Dir(s.src.rel)
		n := 0
		for _, src := range srcs {
			if filepath.Dir(src.rel) != dir {
				continue
			}
			for _, caller := range callersIn(src.f, s.fn.Name.Name) {
				n++
				if !allowed[caller] {
					return fmt.Errorf("%s is called from %s, which is not before the read loop", s.fn.Name.Name, caller)
				}
			}
		}
		if n == 0 {
			return fmt.Errorf("nothing calls %s", s.fn.Name.Name)
		}
		return endAttachCloses(endSite{src: srcFile(srcs, "internal/mqtt/server.go"), fn: s.fn})
	}
}

// firstReturn is the first statement after the site, in its innermost block,
// that is a return, where no statement before it holds one.
func firstReturn(s endSite) (*ast.ReturnStmt, error) {
	b, i := innermost(s)
	if b == nil {
		return nil, fmt.Errorf("no block holds the site")
	}
	for _, next := range b.List[i+1:] {
		if r, ok := next.(*ast.ReturnStmt); ok {
			return r, nil
		}
		if returns(next) {
			return nil, fmt.Errorf("a statement at line %d may return before the refusal is answered",
				s.src.fset.Position(next.Pos()).Line)
		}
	}
	return nil, fmt.Errorf("the refusal is not answered by a return")
}

// returnsRefusal reports an error unless the hook returns an error, not nil,
// after the refusal it wrote, and establishClient answers that error by
// returning false, so that attachClient never reads and endAttach stops the
// client.
func returnsRefusal(srcs []endSource) func(endSite) error {
	return func(s endSite) error {
		r, err := firstReturn(s)
		if err != nil {
			return err
		}
		if len(r.Results) == 0 {
			return fmt.Errorf("the hook returns nothing after refusing")
		}
		if id, ok := r.Results[len(r.Results)-1].(*ast.Ident); ok && id.Name == "nil" {
			return fmt.Errorf("the hook returns nil after refusing")
		}
		est := findFunc(srcs, "internal/mqtt/server.go", "establishClient")
		if est == nil {
			return fmt.Errorf("establishClient is not in internal/mqtt/server.go")
		}
		// err = s.hooks.OnConnect(cl, pk); if err != nil { return false, err }
		for i, st := range est.Body.List {
			a, ok := st.(*ast.AssignStmt)
			if !ok || len(a.Rhs) != 1 || i+1 >= len(est.Body.List) {
				continue
			}
			c, ok := a.Rhs[0].(*ast.CallExpr)
			if !ok {
				continue
			}
			if sel, ok := c.Fun.(*ast.SelectorExpr); !ok || sel.Sel.Name != s.fn.Name.Name {
				continue
			}
			if ifs, ok := est.Body.List[i+1].(*ast.IfStmt); ok && len(ifs.Body.List) > 0 {
				if ret, ok := ifs.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 2 {
					if id, ok := ret.Results[0].(*ast.Ident); ok && id.Name == "false" {
						return endAttachCloses(endSite{src: srcFile(srcs, "internal/mqtt/server.go"), fn: est})
					}
				}
			}
		}
		return fmt.Errorf("establishClient does not return false on the error %s returns", s.fn.Name.Name)
	}
}

// establishRefuses reports an error unless the CONNACK the site writes in
// establishClient is followed by its `return false`, or is the one written
// with the code validateConnect returned, after establishClient has returned
// on any code but Success.
func establishRefuses(s endSite) error {
	if refusesAfter(s) {
		return endAttachCloses(s)
	}
	call, ok := s.node.(*ast.CallExpr)
	if !ok {
		return fmt.Errorf("the site is not a call")
	}
	if id, ok := call.Args[1].(*ast.Ident); !ok || id.Name != "code" {
		return fmt.Errorf("a CONNACK not followed by return false, with a code other than validateConnect's")
	}
	for _, st := range s.fn.Body.List {
		if st.Pos() > s.node.Pos() {
			break
		}
		a, ok := st.(*ast.AssignStmt)
		if !ok || len(a.Lhs) != 1 || len(a.Rhs) != 1 {
			continue
		}
		if id, ok := a.Lhs[0].(*ast.Ident); !ok || id.Name != "code" || a.Tok != token.DEFINE {
			continue
		}
		if c, ok := a.Rhs[0].(*ast.CallExpr); !ok || !isCallTo(c, "validateConnect") {
			return fmt.Errorf("code is not validateConnect's")
		}
		return guardedOnSuccess(s.fn, a.End(), s.node.Pos())
	}
	return fmt.Errorf("no code := validateConnect(...) before the CONNACK")
}

// refusesAfter is whether, after the site in its innermost block, every
// return up to and including the first unconditional one returns false.
func refusesAfter(s endSite) bool {
	b, i := innermost(s)
	if b == nil {
		return false
	}
	for _, next := range b.List[i+1:] {
		ok := true
		ast.Inspect(next, func(n ast.Node) bool {
			if r, isRet := n.(*ast.ReturnStmt); isRet {
				if len(r.Results) == 0 {
					ok = false
				} else if id, isID := r.Results[0].(*ast.Ident); !isID || id.Name != "false" {
					ok = false
				}
			}
			_, lit := n.(*ast.FuncLit)
			return !lit
		})
		if !ok {
			return false
		}
		if _, isRet := next.(*ast.ReturnStmt); isRet {
			return true
		}
	}
	return false
}

// guardedOnSuccess reports an error unless, between from and to, fn's body
// holds `if code != packets.CodeSuccess { ... return ... }`, and nothing
// between assigns code.
func guardedOnSuccess(fn *ast.FuncDecl, from, to token.Pos) error {
	guarded := false
	for _, st := range fn.Body.List {
		if st.Pos() < from || st.Pos() > to {
			continue
		}
		if a, ok := st.(*ast.AssignStmt); ok {
			for _, l := range a.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == "code" {
					return fmt.Errorf("code is assigned again at %v", a.Pos())
				}
			}
		}
		ifs, ok := st.(*ast.IfStmt)
		if !ok {
			continue
		}
		be, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok || be.Op != token.NEQ {
			continue
		}
		if id, ok := be.X.(*ast.Ident); !ok || id.Name != "code" {
			continue
		}
		if _, ok := isPacketsSel(be.Y, "CodeSuccess"); ok && returns(ifs) {
			guarded = true
		}
	}
	if !guarded {
		return fmt.Errorf("establishClient does not return on a code other than Success before this CONNACK")
	}
	return nil
}

func isCallTo(c *ast.CallExpr, name string) bool {
	switch f := c.Fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name == name
	case *ast.Ident:
		return f.Name == name
	}
	return false
}

func srcFile(srcs []endSource, rel string) endSource {
	for _, src := range srcs {
		if src.rel == rel {
			return src
		}
	}
	return endSource{}
}

func findFunc(srcs []endSource, rel, name string) *ast.FuncDecl {
	src := srcFile(srcs, rel)
	if src.f == nil {
		return nil
	}
	return funcsIn(src.f)[name]
}

func funcsIn(f *ast.File) map[string]*ast.FuncDecl {
	out := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
			out[fn.Name.Name] = fn
		}
	}
	return out
}

// callersIn names the functions of f that call name.
func callersIn(f *ast.File, name string) []string {
	var out []string
	for _, fn := range funcsIn(f) {
		if calls(fn, name) == nil && fn.Name.Name != name {
			out = append(out, fn.Name.Name)
		}
	}
	sort.Strings(out)
	return out
}

// **An owner writes no DISCONNECT after a write that failed** (Client.endWrite):
// the write may have put part of its packet on the wire, so the DISCONNECT
// would land inside it and the client would parse the two as one. The end is
// claimed while a PUBLISH is on the socket, the PUBLISH's write then fails,
// and the owner's DISCONNECT is refused rather than written after it.
func TestAnOwnerWritesNoDisconnectAfterAWriteThatFailed(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		cl, peer, released := pipeClient(t)
		pub := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
			TopicName: "t", Payload: []byte("under way")}
		wrote := make(chan error, 1)
		go func() { wrote <- cl.WritePacket(pub) }()
		// One byte read: the write is on the socket, and blocked for the rest.
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		first := make([]byte, 1)
		_, err := io.ReadFull(peer, first)
		require.NoError(t, err)
		require.Equal(t, byte(0x30), first[0])

		require.True(t, cl.claimEnd(), "trial %d: the end had an owner already", trial)
		require.Zero(t, released.Load(), "trial %d: the slot went back while a write was on the socket", trial)

		// The write fails part way: its packet is cut on the wire. Its writer
		// stops the client, which waits for the owner here.
		require.NoError(t, cl.Net.Conn.SetWriteDeadline(time.Now()))
		require.Eventually(t, func() bool { return released.Load() == 1 }, 5*time.Second, time.Millisecond,
			"trial %d: the failed write did not finish", trial)

		got := make(chan []byte, 1)
		go func() {
			_ = peer.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			b := make([]byte, 64)
			n, _ := peer.Read(b)
			got <- b[:n]
		}()
		dis := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Disconnect}}
		require.Error(t, cl.WritePacket(dis), "trial %d: the owner wrote a DISCONNECT after a cut packet", trial)
		require.Empty(t, <-got, "trial %d: bytes followed the cut packet", trial)

		cl.finishEnd(errClientStop)
		require.Error(t, <-wrote, "trial %d: the cut write reported success", trial)
		require.ErrorIs(t, cl.StopCause(), os.ErrDeadlineExceeded,
			"trial %d: the end's reason is not the write that failed", trial)
	}
}

// stallingConn is one end of a pipe whose writes stall until something cuts
// them - a write deadline that has passed - and that records, as the wire
// would show it, which writes began, how each ended, and whether the socket
// was closed under one.
type stallingConn struct {
	net.Conn
	stall      func(b []byte) bool // whether a write of b stalls
	mu         sync.Mutex
	events     []string
	writing    int
	began      chan byte
	cut        chan struct{}
	closed     chan struct{}
	cutOnce    sync.Once
	closeOnce  sync.Once
	underWrite atomic.Bool
}

func newStallingConn(c net.Conn, stall func([]byte) bool) *stallingConn {
	return &stallingConn{Conn: c, stall: stall, began: make(chan byte, 16), cut: make(chan struct{}),
		closed: make(chan struct{})}
}

func (c *stallingConn) note(format string, a ...any) {
	c.mu.Lock()
	c.events = append(c.events, fmt.Sprintf(format, a...))
	c.mu.Unlock()
}

func (c *stallingConn) Events() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.events, ", ")
}

func (c *stallingConn) Write(b []byte) (int, error) {
	if len(b) == 0 || !c.stall(b) {
		return c.Conn.Write(b)
	}
	c.mu.Lock()
	c.writing++
	c.events = append(c.events, fmt.Sprintf("write %#x began", b[0]))
	c.mu.Unlock()
	c.began <- b[0]
	defer func() { c.mu.Lock(); c.writing--; c.mu.Unlock() }()
	select {
	case <-c.cut:
		c.note("write %#x cut by its deadline", b[0])
		return 0, os.ErrDeadlineExceeded
	case <-c.closed:
		c.note("write %#x failed: socket closed under it", b[0])
		return 0, net.ErrClosed
	}
}

func (c *stallingConn) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() && !t.After(time.Now()) {
		c.cutOnce.Do(func() { close(c.cut) })
	}
	return c.Conn.SetWriteDeadline(t)
}

func (c *stallingConn) Close() error {
	c.mu.Lock()
	if c.writing > 0 {
		c.underWrite.Store(true)
	}
	c.events = append(c.events, "close")
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// refuseAll refuses every CONNECT's credentials.
type refuseAll struct{ HookBase }

func (h *refuseAll) ID() string                                         { return "refuse-all" }
func (h *refuseAll) Provides(b byte) bool                               { return b == OnConnectAuthenticate }
func (h *refuseAll) OnConnectAuthenticate(*Client, packets.Packet) bool { return false }

// **A shutdown does not close a socket under the CONNACK refusing it**
// (Listeners.CloseAll, Client.claimEndForConnack): the goroutine refusing the
// CONNECT owns that connection's end, and a shutdown reaching the socket ends
// it through that owner - cutting its write where nothing bounds it, here no
// write_timeout and a keepalive of 0, and waiting for it to finish.
func TestAShutdownDoesNotCloseTheSocketUnderARefusingConnack(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		s := New(&Options{Logger: logger})
		require.NoError(t, s.AddHook(new(refuseAll), nil))
		srv, peer := net.Pipe()
		conn := newStallingConn(srv, func(b []byte) bool { return b[0] == 0x20 })
		done := make(chan error, 1)
		go func() { done <- s.EstablishConnection("t", conn) }()
		body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x02, 0, 0, 0, 0, 7, 'r', 'e', 'f', 'u', 's', 'e', 'd'}
		go func() { _, _ = peer.Write(append([]byte{0x10, byte(len(body))}, body...)) }()
		select {
		case b := <-conn.began:
			require.Equal(t, byte(0x20), b)
		case <-time.After(5 * time.Second):
			t.Fatal("the refusing CONNACK was never written")
		}

		stopped := make(chan struct{})
		go func() { s.Listeners.CloseAll(s.closeListenerClients); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatalf("trial %d: the shutdown never finished: %s", trial, conn.Events())
		}
		<-done
		t.Logf("trial %d: %s", trial, conn.Events())
		require.False(t, conn.underWrite.Load(),
			"trial %d: the shutdown closed the socket under the refusing CONNACK: %s", trial, conn.Events())
		_ = peer.Close()
	}
}

// **A shutdown and a takeover never wait on an unbounded write** (invariant
// 16; Client.cutWrites): with no write_timeout and a keepalive of 0 nothing
// bounds a write to a client that has stopped reading, so one holding the
// connection's end - a DISCONNECT its owner is writing - or the client's lock
// would hold the shutdown or the takeover for good. Each cuts that write, the
// owner finishes, and the wait ends.
func TestAShutdownOrTakeoverCutsAWriteNothingBounds(t *testing.T) {
	for _, ender := range []packets.Code{packets.ErrServerShuttingDown, packets.ErrSessionTakenOver} {
		for _, stuck := range []string{"the owner's DISCONNECT", "a delivery holding the lock"} {
			t.Run(ender.Reason+"/"+stuck, func(t *testing.T) {
				s := New(&Options{Logger: logger})
				srv, peer := net.Pipe()
				t.Cleanup(func() { _ = peer.Close() })
				conn := newStallingConn(srv, func([]byte) bool { return true })
				cl := s.NewClient(conn, "t", "stuck", false)
				cl.Properties.ProtocolVersion = 5
				cl.State.Keepalive = 0
				require.True(t, cl.writeDeadline().IsZero(), "the write is bounded, so this proves nothing")

				first := make(chan error, 1)
				if stuck == "the owner's DISCONNECT" {
					go func() { first <- s.DisconnectClient(cl, packets.ErrPacketTooLarge) }()
				} else {
					pub := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
						TopicName: "t", Payload: []byte("stuck")}
					go func() { first <- cl.WritePacket(pub) }()
				}
				<-conn.began

				ended := make(chan error, 1)
				go func() { ended <- s.DisconnectClient(cl, ender) }()
				select {
				case <-ended:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s waited on %s for good: %s", ender.Reason, stuck, conn.Events())
				}
				require.Error(t, <-first, "the stuck write reported success")
				require.True(t, cl.Closed(), "the connection was not closed")
				t.Logf("%s", conn.Events())
				require.False(t, conn.underWrite.Load(), "the socket was closed under a write: %s", conn.Events())
			})
		}
	}
}
