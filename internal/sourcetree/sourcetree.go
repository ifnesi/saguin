// Package sourcetree says which directories under the module root are this
// module's own source, for the tests that walk it.
//
// **One rule for every sweep.** Each source-level test used to keep its own
// list of directories to skip - .git, bin, .venv, node_modules, in whichever
// combination its author remembered - and none named .claude, where a
// session's worktree is a second copy of the tree at another commit. Every
// sweep that walked from the root counted and judged that copy as well: a
// check could fail on code that is not this tree's, or pass on counts
// inflated by code that is not this tree's. CI has no such directory, so the
// two disagreed without anybody seeing it.
package sourcetree

import (
	"os"
	"path/filepath"
	"strings"
)

// Outside reports whether the directory at path, named name and met by a
// walk from the module root, is outside this module's own tree, so the walk
// returns filepath.SkipDir for it.
//
// The rule is the go tool's, which is what decides what a package is: a
// directory whose name begins with "." or "_", or is named testdata, holds
// no package of this module - .git, .claude, .venv among them. Two more hold
// no source of this module whatever their names: bin, where binaries are
// built, and node_modules. A directory holding its own go.mod is another
// module. The root itself is never outside.
func Outside(root, path, name string) bool {
	if filepath.Clean(path) == filepath.Clean(root) {
		return false
	}
	switch {
	case strings.HasPrefix(name, "."), strings.HasPrefix(name, "_"), name == "testdata":
		return true
	case name == "bin", name == "node_modules":
		return true
	}
	if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
		return true
	}
	return false
}
