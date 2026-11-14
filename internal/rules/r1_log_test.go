package rules

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// design.md, R1, the goroutine table's logger row: it owns "the log output
// and its write results; lines come from the core, and from main before the
// core starts and after it ends (Logging)"; Flows, Logging: "Runtime
// log-worthy events reach the core through their existing event or failure
// reports". Only the core (internal/core) and main (cmd/saguin), and the
// logger's own package, may hold the logger, make a line or admit one: any
// other package names, of the logger's package, the log level alone, which
// the configuration reads (config.md, log_level).

// logSenders are the packages, below the module's path, allowed every
// member of the logger's package.
var logSenders = []string{"internal/core", "cmd/saguin", "internal/logger"}

// levelMembers are the members of the logger's package that any package may
// name: the log level and its values.
var levelMembers = map[string]bool{"Level": true, "Debug": true, "Info": true, "Warn": true, "Error": true}

// kindLogSender is a finding of a package other than the core and main
// sending log lines.
const kindLogSender = "log sender"

// scanLogSenders returns each use, in one file, of a member of the logger's
// package, loggerPath, other than the log level, by whatever name the file
// gives the package; a dot import of it, which would hide those uses, is a
// finding itself.
func scanLogSenders(fset *token.FileSet, f *ast.File, loggerPath string) []finding {
	var found []finding
	for _, im := range fileImports(f) {
		if im.path == loggerPath && im.name == "." {
			found = append(found, finding{fset.Position(im.spec.Pos()), kindLogSender,
				"dot-imports the logger's package, which hides its uses from the log senders' scan"})
		}
	}
	names := importNames(f)
	ast.Inspect(f, func(n ast.Node) bool {
		e, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		if name, ok := strings.CutPrefix(selectorOf(e, names), loggerPath+"."); ok && !levelMembers[name] {
			found = append(found, finding{fset.Position(n.Pos()), kindLogSender,
				"logger." + name + ": lines come from the core, and from main before the core starts and after it ends (design.md, R1)"})
		}
		return true
	})
	return found
}

// TestOnlyTheCoreAndMainSendLogLines applies scanLogSenders to every file of
// the broker's code outside logSenders.
func TestOnlyTheCoreAndMainSendLogLines(t *testing.T) {
	fset, module, files := brokerCode(t)
	allowed := map[string]bool{}
	for _, p := range logSenders {
		allowed[module+"/"+p] = true
	}
	for _, pf := range files {
		if allowed[pf.pkg] {
			continue
		}
		for _, f := range scanLogSenders(fset, pf.f, module+"/internal/logger") {
			t.Error(f)
		}
	}
}
