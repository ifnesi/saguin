package main

// A rule about the whole tree, beside the payload one: a delivery's packet
// identifier is freed in the engine and cleared from its state in the broker,
// so neither package alone can hold it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// **A packet identifier the server gave out is not freed until everything
// keyed by it is cleared.** A delivery's session store row and the broker's
// record of what it carries are keyed by client and identifier, and are
// cleared after the delivery leaves the in-flight table. An identifier freed
// first could be claimed by a new delivery in between, and the clearing meant
// for the old one then removed the new one's row - a delivery never written
// and never re-sent (TestAnIdentifierIsNotReusedWhileItsReleaseIsPending).
//
// So a delivery the server sent leaves the table only by Retire,
// RetireExpired or DropOldest, which keep its identifier reserved, and any
// function that calls one of them also calls Unclaim once it has cleared the
// rest. Inflight.Delete, which frees at once, is left to the functions that
// handle an inbound exchange: its identifier is the one the client chose for
// its own publish or our acknowledgement of it, and nothing of ours is keyed
// by it. They are named rather than detected, and the list is the claim.
func TestAPacketIdentifierOutlivesWhatIsKeyedByIt(t *testing.T) {
	inbound := map[string]string{
		"processPubrel":  "the client's PUBREL for its own QoS 2 publish",
		"forgetExchange": "a QoS 2 publish from the client, swept before its PUBREL",
		"resendInflight": "our PUBACK or PUBCOMP to the client, re-sent on resume",
	}
	retiring := map[string]bool{"Retire": true, "RetireExpired": true, "DropOldest": true}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	onInflight := func(call *ast.CallExpr) (string, bool) {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		x, ok := sel.X.(*ast.SelectorExpr)
		if !ok || x.Sel.Name != "Inflight" {
			return "", false
		}
		return sel.Sel.Name, true
	}

	var files, retirers int
	deletes := map[string]bool{}
	err = filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			if sourcetree.Outside(root, path, fi.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		// Tests set tables up however they need to; the rule is about the
		// broker.
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not ours to report; the compiler says so first
		}
		files++
		rel, _ := filepath.Rel(root, path)
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var retires []string
			unclaims := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				m, ok := onInflight(call)
				switch {
				case !ok:
				case retiring[m]:
					retires = append(retires, m)
				case m == "Unclaim":
					unclaims = true
				case m == "Delete":
					deletes[fn.Name.Name] = true
					if _, ok := inbound[fn.Name.Name]; !ok {
						t.Errorf("%s:%d: %s frees a packet identifier with Inflight.Delete. Unless "+
							"it is an inbound exchange's, use Retire and Unclaim once the session "+
							"store and the broker have cleared what is keyed by it.",
							rel, fset.Position(call.Pos()).Line, fn.Name.Name)
					}
				}
				return true
			})
			if len(retires) > 0 {
				retirers++
				if !unclaims {
					t.Errorf("%s: %s calls %s and never Unclaim, so the identifiers it retires "+
						"stay reserved for the life of the session",
						rel, fn.Name.Name, strings.Join(retires, ", "))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	// Counted, and each named exemption must still exist: a list that names
	// a function nothing calls Delete from any more is claiming a reason
	// nobody is using.
	for name, why := range inbound {
		if !deletes[name] {
			t.Errorf("%s is exempt (%s) but no longer calls Inflight.Delete: remove it from the list",
				name, why)
		}
	}
	if files < 50 || retirers < 9 {
		t.Fatalf("%d Go files and %d functions that retire an identifier examined, which is "+
			"too few - the walk is not reaching them", files, retirers)
	}
	names := make([]string, 0, len(deletes))
	for n := range deletes {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("%d Go files, %d functions that retire an identifier, Inflight.Delete in %v",
		files, retirers, names)
}
