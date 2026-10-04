package main

// One rule about the whole tree, which is why it lives beside the binary
// rather than in any package it is about.

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

// Go attaches a doc comment to whatever declaration follows it. A function
// inserted directly beneath an existing comment therefore takes that
// comment over, and the orphaned text goes on describing a function it was
// never about - while the compiler, `go vet` and `gofmt` all stay silent.
//
// It is not a hypothetical. It happened five times in one week here, twice
// in the very commit whose message described the reasoning those comments
// carry, and the two worst cases were found by this check after two people
// had read the diff. What makes it worth guarding is the same thing that
// makes it invisible: the comments in this repository exist so a rule is
// not rewritten back into the bug, and a rule attached to the wrong
// function has stopped doing that while looking exactly as if it still is.
//
// The signal is narrow on purpose. A doc comment opening with a capitalised
// word is Go's own convention for naming what it documents, so a comment
// whose first word names *some other declaration in the same package* is
// almost certainly one that has been taken over. Anything else - a comment
// starting "Every", "RFC 0002", "Invariant 4" - says nothing about which
// declaration it belongs to and is left alone.
func TestNoDocCommentSitsOnTheWrongDeclaration(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}

	var checked int
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
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil // not ours to report; the compiler already has
		}

		// Every name this file declares, so that a first word which is not
		// an identifier here cannot raise anything.
		declared := map[string]bool{}
		for _, decl := range f.Decls {
			for _, name := range declaredNames(decl) {
				declared[name] = true
			}
		}

		rel, _ := filepath.Rel(root, path)
		for _, decl := range f.Decls {
			doc := docOf(decl)
			if doc == nil || len(doc.List) == 0 {
				continue
			}
			head := firstWord(doc.List[0].Text)
			if head == "" || !declared[head] {
				continue
			}
			names := declaredNames(decl)
			checked++
			if len(names) == 0 || contains(names, head) {
				continue
			}
			t.Errorf("%s:%d: the doc comment opens %q but sits on %q - "+
				"it was written about something else and has been taken over",
				rel, fset.Position(doc.Pos()).Line, head, strings.Join(names, ", "))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
	// A check that silently examined nothing is worse than none: it would
	// pass for ever after a refactor moved the files.
	if checked < 20 {
		t.Fatalf("only %d doc comments named a declaration, which is too few to "+
			"believe - this check is no longer looking where it thinks it is", checked)
	}
}

func docOf(decl ast.Decl) *ast.CommentGroup {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return d.Doc
	case *ast.GenDecl:
		return d.Doc
	}
	return nil
}

// declaredNames is what a declaration is called: one name for a function,
// and possibly several for a `var (...)` or `type (...)` block, any of
// which a doc comment above it may legitimately open with.
func declaredNames(decl ast.Decl) []string {
	var out []string
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Name != nil {
			out = append(out, d.Name.Name)
		}
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				out = append(out, s.Name.Name)
			case *ast.ValueSpec:
				for _, n := range s.Names {
					out = append(out, n.Name)
				}
			}
		}
	}
	return out
}

// firstWord is the identifier a doc comment opens with, or "" when it does
// not open with one.
func firstWord(line string) string {
	line = strings.TrimPrefix(line, "//")
	line = strings.TrimPrefix(line, "/*")
	line = strings.TrimSpace(line)
	word, _, _ := strings.Cut(line, " ")
	for _, r := range word {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return ""
		}
	}
	return word
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}
