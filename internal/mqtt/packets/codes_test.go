// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package packets

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodesString(t *testing.T) {
	c := Code{
		Reason: "test",
		Code:   0x1,
	}

	require.Equal(t, "test", c.String())
}

func TestCodesError(t *testing.T) {
	c := Code{
		Reason: "error",
		Code:   0x1,
	}

	require.Equal(t, "error", error(c).Error())
}

// **A code's name says which kind of error it is, and its value agrees**:
// every ErrProtocolViolation* is 0x82 (Protocol Error) and every
// ErrMalformed* is 0x81 (Malformed Packet). Three carried 0x81 under a
// protocol-violation name - one moved from 0x82 when strand W checked the
// codes against the specification, and the names were not part of that
// check - so a reader choosing by name chose the other error. Walked in
// the syntax tree of codes.go, and counted.
func TestACodesNameAgreesWithItsValue(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "codes.go", nil, 0)
	require.NoError(t, err)
	var examined int
	ast.Inspect(f, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return true
		}
		name := spec.Names[0].Name
		var want string
		switch {
		case strings.HasPrefix(name, "ErrProtocolViolation"):
			want = "0x82"
		case strings.HasPrefix(name, "ErrMalformed"):
			want = "0x81"
		default:
			return true
		}
		lit, ok := spec.Values[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, el := range lit.Elts {
			kv, ok := el.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Code" {
				examined++
				if v, ok := kv.Value.(*ast.BasicLit); !ok || v.Value != want {
					t.Errorf("%s: %s is %s, and its name says %s", fset.Position(kv.Pos()), name,
						types.ExprString(kv.Value), want)
				}
			}
		}
		return true
	})
	t.Logf("%d protocol-violation and malformed codes examined", examined)
	require.GreaterOrEqual(t, examined, 50, "the walk is not finding the codes")
}
