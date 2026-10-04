package broker

// A source-level rule, like the log-bounds and doc-comment ones: it is about
// the relationship between two places in this package rather than about any
// behaviour one test can drive.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every hook method the broker implements must be in the set it declares.
//
// The engine asks Provides before it dispatches, so a method left out of
// `provided` is never called. **Nothing else notices**: the method compiles,
// its tests pass when they call it directly, and the behaviour it was
// written for simply stops happening on a running broker. That is the same
// silence as a counter reading zero, and it is one line of a twenty-three
// line list away at any time.
//
// It reads the syntax tree rather than calling Provides for each name,
// because the question is about what this package *declares* and there is
// no way to go from a method's name back to the mqtt constant at run time -
// the constants are iota bytes whose values move whenever the engine's hook
// list changes, which is exactly the change this is here to survive.
//
// The other direction is checked too. An entry with no method behind it is
// not a defect the way a missing one is - HookBase answers it with a no-op -
// but it is a dispatch this broker pays for on every packet and gets
// nothing from, and it usually means a method was deleted and its entry was
// not.
func TestProvidesNamesEveryHookTheBrokerImplements(t *testing.T) {
	fset := token.NewFileSet()
	pkg, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err, "parsing the broker package")
	require.Contains(t, pkg, "broker")

	implemented := map[string]bool{}
	declared := map[string]bool{}

	for _, f := range pkg["broker"].Files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if ok && fn.Recv != nil && len(fn.Recv.List) == 1 {
				if types.ExprString(fn.Recv.List[0].Type) == "*Broker" &&
					strings.HasPrefix(fn.Name.Name, "On") {
					implemented[fn.Name.Name] = true
				}
				continue
			}
			// The `provided` table: every mqtt.OnX named anywhere inside it.
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "provided" {
					continue
				}
				ast.Inspect(vs, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok &&
						types.ExprString(sel.X) == "mqtt" &&
						strings.HasPrefix(sel.Sel.Name, "On") {
						declared[sel.Sel.Name] = true
					}
					return true
				})
			}
		}
	}

	// A check that silently compared nothing is worse than none. Both sides
	// are asserted, because either one coming back empty - a renamed
	// receiver, a moved table - would make every comparison below pass.
	require.GreaterOrEqual(t, len(implemented), 20,
		"only %d hook methods found on *Broker, which cannot be all of them: "+
			"this check is no longer reading the package it thinks it is", len(implemented))
	require.GreaterOrEqual(t, len(declared), 20,
		"only %d events found in the provided table, which cannot be all of them", len(declared))

	for name := range implemented {
		require.True(t, declared[name],
			"*Broker implements %s and `provided` does not name it, so the engine "+
				"will never call it and nothing else will say so", name)
	}
	for name := range declared {
		require.True(t, implemented[name],
			"`provided` names %s and *Broker does not implement it, so every packet "+
				"pays for a dispatch that reaches HookBase's no-op", name)
	}

	t.Logf("%d hook methods, %d declared events", len(implemented), len(declared))
}
