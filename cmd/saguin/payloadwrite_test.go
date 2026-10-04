package main

// A rule about the whole tree, beside the atomic-alignment one and for the
// same reason: a payload crosses every package, so no one package can hold it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// **A payload is immutable once it has been read.** One publish's payload is
// shared, not copied, by every subscriber's delivery of it, by the session
// store's hold of it for a subscriber who is away, and by the retained store
// - so a write into its bytes anywhere changes a message other clients were
// already promised. Sharing it is what made a broadcast delivery stop costing
// a copy of its payload per subscriber (packets.Packet.CopySharingPayload).
//
// So nothing may write into one: no assignment to an element of a Payload, no
// copy() into one, and no append() onto one, which writes into whatever spare
// capacity the shared array has. Every field named Payload is held to it,
// with no exemptions, because in this tree the name means a message's bytes
// whoever declares it; reading a payload, slicing it or replacing the field
// with other bytes are all fine.
//
// **What this cannot see** is a write through another name - `b :=
// pk.Payload; b[0] = 1` - or a read buffer being recycled underneath a payload.
// The second is the contract at ReadPacket's allocation. The first is why
// payload bytes are handed around by field rather than stashed in locals.
func TestNothingWritesIntoAPayload(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}

	// isPayload is whether an expression is a Payload field, or a slice or
	// parenthesis of one - each of which shares its backing array.
	var isPayload func(ast.Expr) bool
	isPayload = func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.ParenExpr:
			return isPayload(x.X)
		case *ast.SliceExpr:
			return isPayload(x.X)
		case *ast.SelectorExpr:
			return x.Sel.Name == "Payload"
		}
		return false
	}
	intoPayload := func(e ast.Expr) bool {
		ix, ok := e.(*ast.IndexExpr)
		return ok && isPayload(ix.X)
	}

	var files, seen, found int
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
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not ours to report; the compiler says so first
		}
		files++
		rel, _ := filepath.Rel(root, path)
		bad := func(n ast.Node, what string) {
			found++
			t.Errorf("%s:%d: %s. A payload is shared by every delivery of its "+
				"message and by the stores holding it, so this changes messages "+
				"other clients were promised. Build new bytes instead.",
				rel, fset.Position(n.Pos()).Line, what)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if x.Sel.Name == "Payload" {
					seen++
				}
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					if intoPayload(l) {
						bad(x, "writes an element of a payload")
					}
				}
			case *ast.IncDecStmt:
				if intoPayload(x.X) {
					bad(x, "writes an element of a payload")
				}
			case *ast.CallExpr:
				id, ok := x.Fun.(*ast.Ident)
				if !ok || len(x.Args) == 0 {
					return true
				}
				switch {
				case id.Name == "copy" && isPayload(x.Args[0]):
					bad(x, "copies into a payload")
				case id.Name == "append" && isPayload(x.Args[0]):
					bad(x, "appends onto a payload, writing into its spare capacity")
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	// **Counted, because a sweep that matched nothing must not pass for the
	// same reason as a sweep that found nothing wrong.**
	if files < 100 || seen < 100 {
		t.Fatalf("%d Go files and %d uses of a Payload field examined, which is too "+
			"few for this tree - the walk is not reaching them", files, seen)
	}
	t.Logf("%d Go files and %d uses of a Payload field examined, %d writes into one found",
		files, seen, found)
}
