package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Every `saguin --<flag>` the repository writes down - in a document, a
// comment, a log line an operator is told to follow - is a flag the binary
// defines.
//
// **A flag renamed leaves its old name behind in prose**, and nothing about
// prose fails: `--explain` became `--route` and a log line went on telling
// operators to run the old one, which answers "flag provided but not
// defined". The flags are read off main.go's syntax tree, so this asks the
// binary rather than a list somebody keeps.
func TestEveryFlagTheRepositoryNamesIsOneTheBinaryHas(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	defined := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "flag" {
			return true
		}
		if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if name, err := strconv.Unquote(lit.Value); err == nil {
				defined[name] = true
			}
		}
		return true
	})
	// The flag package answers these two itself, with the usage text.
	defined["help"], defined["h"] = true, true
	if len(defined) < 7 {
		t.Fatalf("read %d flags off main.go, and the binary has more: the walk is reading nothing", len(defined))
	}

	out, err := exec.Command("git", "-C", "../..", "ls-files").Output()
	if err != nil {
		t.Skipf("no git to list the repository's files: %v", err)
	}
	named := regexp.MustCompile(`(?:^|[^\w-])saguin (--[a-z][a-z0-9-]*)`)
	files, mentions := 0, 0
	for _, path := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		switch ext := filepath.Ext(path); {
		case ext == ".go", ext == ".md", ext == ".yaml", ext == ".yml", ext == ".sh",
			filepath.Base(path) == "Makefile":
		default:
			continue
		}
		body, err := os.ReadFile(filepath.Join("../..", path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		files++
		for _, m := range named.FindAllStringSubmatch(string(body), -1) {
			mentions++
			if name := strings.TrimPrefix(m[1], "--"); !defined[name] {
				t.Errorf("%s names `saguin %s`, which the binary does not define", path, m[1])
			}
		}
	}
	if mentions == 0 {
		t.Fatalf("read %d files and found no `saguin --<flag>` in any, so this checked nothing", files)
	}
	t.Logf("%d flags defined; %d mentions across %d files", len(defined), mentions, files)
}
