package sourcetree

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// **A walk from the root reaches this module's files and nothing else.** A
// tree is built with a Go file in each place a sweep has met one: the
// module's own package, a session worktree under .claude, a .venv, bin,
// node_modules, a testdata directory, a directory the go tool ignores for
// its leading underscore, and a nested module. Only the module's own file
// may be reached.
func TestOnlyTheModulesOwnTreeIsWalked(t *testing.T) {
	root := t.TempDir()
	for _, f := range []string{
		"go.mod",
		"internal/pkg/own.go",
		".claude/worktrees/session-1/internal/pkg/own.go",
		".venv/lib/x.go",
		"bin/x.go",
		"node_modules/x/x.go",
		"internal/pkg/testdata/x.go",
		"_scratch/x.go",
		"examples/other/go.mod",
		"examples/other/x.go",
	} {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("package x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var reached []string
	var dirs int
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs++
			if Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) == ".go" {
			rel, _ := filepath.Rel(root, path)
			reached = append(reached, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if want := []string{"internal/pkg/own.go"}; !slices.Equal(reached, want) {
		t.Errorf("the walk reached %v, want %v", reached, want)
	}
	if dirs < 9 {
		t.Fatalf("the walk met %d directories, too few to have been offered the ones it must skip", dirs)
	}
}
