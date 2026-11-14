package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// The design fuzzes "code that reads bytes or text from outside Sagüin
// (clients, a bridge's peer, the operator's files)" (docs/design.md,
// Testing). An encoder reads nothing from outside: it writes Sagüin's own
// values as bytes and trusts its caller, so no fuzz target is meant to run
// it, and the gap report leaves its lines out. Target selection still
// counts them: a seed that runs an encoder selects its target as before.
//
// A function is an encoder by what it is, with no list to keep:
//
//   - its name begins with Append or append, its first parameter is a
//     []byte and its first result a []byte: Go's form for a function that
//     appends an encoding to dst, as strconv.AppendInt has; or
//   - it is unexported, its package's non-test code names it, and only
//     inside encoders: a helper of encoders alone.
//
// Every other function, a decoder above all, keeps its lines in the report
// when no seed reaches them. A name is matched wherever it appears, as a
// call, a value or a selector, which can only count more uses and so keep
// more lines in the report, never fewer. An exported function other than
// an Append one may be used from any package, so the second rule never
// makes it an encoder.

// markEncoders sets encoder on every function that owns a change, reading
// the non-test Go files of the change's package; byDir holds each package
// by its directory relative to root.
func markEncoders(root string, byDir map[string]packageInfo, changes []codeChange) error {
	found := make(map[string]map[location]bool) // by package directory
	for _, c := range changes {
		dir := filepath.ToSlash(filepath.Dir(c.file))
		encoders, done := found[dir]
		if !done {
			var err error
			pkg := byDir[dir]
			if encoders, err = packageEncoders(root, dir, slices.Concat(pkg.GoFiles, pkg.CgoFiles)); err != nil {
				return err
			}
			found[dir] = encoders
		}
		for _, fn := range c.funcs {
			fn.encoder = encoders[location{file: c.file, line: fn.first}]
		}
	}
	return nil
}

// inEncoders reports whether every function owning c is an encoder.
func (c codeChange) inEncoders() bool {
	return !slices.ContainsFunc(c.funcs, func(fn *sourceFunction) bool { return !fn.encoder })
}

// packageEncoders returns where each encoder of the package in dir, relative
// to root, begins: its file and the line of its func keyword, as parseSource
// gives a function's first line. files are its non-test Go files, by name.
func packageEncoders(root, dir string, files []string) (map[location]bool, error) {
	type function struct {
		decl *ast.FuncDecl
		at   location
	}
	var funcs []function
	// The functions each name is used in; -1 is outside every function.
	uses := make(map[string][]int)
	fset := token.NewFileSet()
	for _, name := range files {
		file := path.Join(dir, name)
		f, err := parser.ParseFile(fset, filepath.Join(root, file), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			owner, body := -1, ast.Node(decl)
			if fd, ok := decl.(*ast.FuncDecl); ok {
				owner, body = len(funcs), fd.Body
				funcs = append(funcs, function{decl: fd, at: location{file: file, line: fset.Position(fd.Pos()).Line}})
				if fd.Body == nil {
					continue // declared here, written in assembly
				}
			}
			ast.Inspect(body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					uses[id.Name] = append(uses[id.Name], owner)
				}
				return true
			})
		}
	}
	encoder := make([]bool, len(funcs))
	for i, fn := range funcs {
		encoder[i] = appendsEncoding(fn.decl)
	}
	// onlyInEncoders reports whether function i is used, apart from by
	// itself, and only inside encoders.
	onlyInEncoders := func(i int) bool {
		used := false
		for _, owner := range uses[funcs[i].decl.Name.Name] {
			if owner == i {
				continue
			}
			if owner < 0 || !encoder[owner] {
				return false
			}
			used = true
		}
		return used
	}
	// Each pass can make a helper an encoder, and so the helpers it alone
	// uses on the next; nothing changing ends it.
	for changed := true; changed; {
		changed = false
		for i, fn := range funcs {
			if !encoder[i] && !fn.decl.Name.IsExported() && onlyInEncoders(i) {
				encoder[i], changed = true, true
			}
		}
	}
	found := make(map[location]bool)
	for i, fn := range funcs {
		if encoder[i] {
			found[fn.at] = true
		}
	}
	return found, nil
}

// appendsEncoding reports whether fd has Go's form for appending an
// encoding: named Append or append, a []byte first in and first out.
func appendsEncoding(fd *ast.FuncDecl) bool {
	name, params, results := fd.Name.Name, fd.Type.Params.List, fd.Type.Results
	return (strings.HasPrefix(name, "Append") || strings.HasPrefix(name, "append")) &&
		len(params) != 0 && isBytes(params[0].Type) &&
		results != nil && len(results.List) != 0 && isBytes(results.List[0].Type)
}

// isBytes reports whether e spells the type []byte.
func isBytes(e ast.Expr) bool {
	slice, ok := e.(*ast.ArrayType)
	if !ok || slice.Len != nil {
		return false
	}
	elem, ok := slice.Elt.(*ast.Ident)
	return ok && elem.Name == "byte"
}
