package main

// Another rule about the whole tree, beside the atomic-alignment, doc-comment
// and log-bounds ones, and for the same reason: it is about every package
// rather than any one of them.

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

// **go test discovers benchmarks only in _test.go files.** One declared
// anywhere else compiles, is called by nothing, and says nothing: make bench
// reports ok over it for ever.
//
// Five were lost that way when the end-to-end harness moved into its own
// package as harness.go - among them BenchmarkPublishWithConsumers, the one
// RFC 0004's consumer figures come from. Nobody noticed until a scale run
// wedged and those figures were wanted. The Makefile's bench target records
// the same class once before, for BenchmarkTrimOnALongChannel.
//
// What makes a function a benchmark is its shape - a top-level func named
// Benchmark* taking one *testing.B - not who calls it, so that is what this
// matches, and there is no exemption list.
func TestNoBenchmarkOutsideTestFiles(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	var files, inTests int
	var misplaced []string
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
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Errorf("%s does not parse, so it was not examined: %v", path, err)
			return nil
		}
		files++
		rel, _ := filepath.Rel(root, path)
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Benchmark") || !takesB(fn) {
				continue
			}
			if strings.HasSuffix(path, "_test.go") {
				inTests++
			} else {
				misplaced = append(misplaced, rel+": "+fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
	t.Logf("examined %d files; %d benchmarks in _test.go files, %d elsewhere", files, inTests, len(misplaced))
	if files == 0 || inTests == 0 {
		t.Fatalf("examined %d files and found %d benchmarks in test files, which cannot be the tree: "+
			"the sweep matched nothing and would pass by vacuum", files, inTests)
	}
	for _, m := range misplaced {
		t.Errorf("%s is a benchmark outside a _test.go file; go test will never run it", m)
	}
}

// takesB reports whether fn's parameters are exactly one *testing.B.
func takesB(fn *ast.FuncDecl) bool {
	ps := fn.Type.Params.List
	if len(ps) != 1 || len(ps[0].Names) > 1 {
		return false
	}
	star, ok := ps[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && sel.Sel.Name == "B"
}
