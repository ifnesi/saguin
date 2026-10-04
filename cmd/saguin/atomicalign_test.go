package main

// The third rule about the whole tree, beside the doc-comment and log-bounds
// ones and for the same reason: it is about every package rather than any
// one of them.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// **A 64-bit atomic on a field Go has not promised to align panics on a
// 32-bit platform**, and saguin runs on 32-bit platforms. Go guarantees only
// that the first word of an allocated struct is 64-bit aligned; a raw int64
// or uint64 sitting behind pointer-width fields lands on a 4-byte boundary
// where pointers are four bytes, and `atomic.AddInt64` on it does not fail
// quietly - it panics, on the first connection or the first retained
// publish.
//
// Two such fields existed and both were found by running the suite on 386
// for the first time: mqtt.Server.slots at offset 52 and Broker.retaken at
// 188. Neither was a subtle bug; both were fatal on arrival, on the hardware
// this project names in its own README. Nothing caught them because nothing
// built for that width.
//
// **Fixed by making the mistake unrepresentable rather than by moving the
// fields.** The sync/atomic struct types carry align64, so the alignment is
// the type's business wherever the struct lands. Reordering the fields would
// have worked too, for exactly as long as nobody inserted a narrower field
// above them - and the failure would have come back on hardware no test
// runs on.
//
// This is what stops the raw form returning. An exemption list would defeat
// it: the typed form is available everywhere the raw one is, costs nothing
// on 64-bit - the layout is byte-identical and the instructions are the same
// - and a value that genuinely cannot be a struct field is still better
// spelled with the type.
func TestNo64BitAtomicUsesTheRawPackageFunction(t *testing.T) {
	banned := map[string]bool{}
	for _, op := range []string{"Add", "Load", "Store", "Swap", "CompareAndSwap"} {
		for _, w := range []string{"Int64", "Uint64"} {
			banned["atomic."+op+w] = true
		}
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	var files, found int
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
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if banned[types.ExprString(call.Fun)] {
				found++
				t.Errorf("%s:%d: %s takes the address of its target, so whether it is "+
					"legal depends on where that value happens to sit - and on a "+
					"32-bit platform a 64-bit atomic off an 8-byte boundary panics.\n"+
					"\tUse the sync/atomic struct type instead: a field of type "+
					"atomic.Int64 or atomic.Uint64, called as x.Add(1), x.Load() "+
					"or x.Store(v). It carries align64, costs nothing on 64-bit, "+
					"and cannot be got wrong by a later field insertion.",
					rel, fset.Position(call.Pos()).Line, types.ExprString(call.Fun))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	// **Counted, because a sweep that matched nothing must not pass for the
	// same reason as a sweep that found nothing wrong.** A walk that silently
	// stopped finding Go files would report success for ever.
	if files < 100 {
		t.Fatalf("only %d Go files examined, which is too few for this tree - "+
			"the walk is not reaching them", files)
	}
	t.Logf("%d Go files examined, %d raw 64-bit atomic calls found", files, found)
}
