// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// No log line this engine writes names mochi.
//
// The engine came from mochi-mqtt/server and its lifecycle messages came
// with it. An earlier rename changed the two in Serve and missed the one in Close,
// so a broker's logs opened as saguin and closed as mochi, and an operator
// grepping a unit for the engine by either name got half its lifecycle.
// A soak log showed it; nothing in the tree could have.
//
// **It is a rule about the source rather than a test of behaviour**,
// because the defect is a string nobody reads until something has gone
// wrong, and the only place it appears is an operator's terminal. There is
// no assertion about saguin's behaviour that can fail when this is wrong.
//
// It judges the message and not the whole call: a log line may legitimately
// carry a value that says mochi - a client id, a topic, a payload - and
// what an operator reads as the broker's own voice is the message.
//
// Copyright headers, comments and the test fixtures in tpackets.go are
// untouched by this. Attribution is required to stay, and a fixture's
// client id is data.
func TestNoLogMessageInTheEngineNamesMochi(t *testing.T) {
	// **Every package under internal/mqtt, not just this one.** The first
	// version of this walked `.` alone, and internal/mqtt/listeners writes
	// six messages of its own that it never saw. None of them named mochi,
	// so the gap was empty - but a guard whose doc comment says "this
	// engine" and whose walk says "this directory" is one rename away from
	// being wrong and green at the same time. This is the walk catching up with the claim.
	fset := token.NewFileSet()
	noTests := func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}

	var dirs []string
	require.NoError(t, filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() != "testdata" {
			dirs = append(dirs, path)
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		return nil
	}), "walking the engine tree")
	require.GreaterOrEqual(t, len(dirs), 6,
		"only %d directories under the engine, which cannot be all of them", len(dirs))

	var files []*ast.File
	for _, dir := range dirs {
		pkgs, err := parser.ParseDir(fset, dir, noTests, 0)
		require.NoError(t, err, "parsing %s", dir)
		for _, pkg := range pkgs {
			for _, f := range pkg.Files {
				files = append(files, f)
			}
		}
	}

	logMethods := map[string]bool{
		"Info": true, "Warn": true, "Error": true, "Debug": true,
	}

	var messages, offenders int
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !logMethods[sel.Sel.Name] {
				return true
			}
			// The receiver must read as a logger, because Error is also
			// how an error reports itself and Info is a common method
			// name. A logger here is `s.Log`, `h.Log`, `cl.ops.log`.
			recv := strings.ToLower(types.ExprString(sel.X))
			if !strings.HasSuffix(recv, "log") {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			msg, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			messages++
			if strings.Contains(strings.ToLower(msg), "mochi") {
				offenders++
				t.Errorf("%s: a log message names mochi: %q\n"+
					"\tThe engine is saguin's. An operator reading this line reads "+
					"the name of a project that does not ship here.",
					fset.Position(lit.Pos()), msg)
			}
			return true
		})
	}

	// A check that silently examined nothing is worse than none. The engine
	// writes thirty log lines; a number far below that means the
	// receiver test above has stopped recognising this package's loggers,
	// and every assertion would pass by never running.
	t.Logf("%d log messages examined, %d naming mochi", messages, offenders)
	require.GreaterOrEqual(t, messages, 25,
		"only %d log messages found in the engine, which cannot be all of them: "+
			"this check is no longer reading the calls it thinks it is", messages)
}
