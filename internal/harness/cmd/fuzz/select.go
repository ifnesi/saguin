package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
)

// A codeChange is a current line, or the two lines beside a deletion.
// Deletions are reported at their following line (or preceding line at EOF).
type codeChange struct {
	file        string
	first, last int
	funcs       []*sourceFunction

	instrumented    bool // a coverage block holds a neighboring or changed line
	reached         bool // any seed executes such a block
	functionReached bool // any seed executes code in an owning function
}

// newCodeChange records the functions owning the changed line or either
// neighbor of a deletion. Spans, rather than names, distinguish methods.
func newCodeChange(source sourceLines, path string, first, last int) codeChange {
	c := codeChange{file: path, first: first, last: last}
	for line := first; line <= last && line <= len(source.funcs); line++ {
		if fn := source.funcs[line-1]; fn != nil && !slices.Contains(c.funcs, fn) {
			c.funcs = append(c.funcs, fn)
		}
	}
	return c
}

// selectSince measures ordinary seed runs, not fuzzing or Go's cached fuzz
// inputs. Coverage includes every module package, so callers in other
// packages can select a target too. Profiles live only in a temporary folder.
func selectSince(dir, since string, targets []target, report io.Writer) ([]target, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	commit, err := gitOutput(root, "rev-parse", "--verify", "--end-of-options", since+"^{commit}")
	if err != nil {
		return nil, err
	}
	since = strings.TrimSpace(string(commit))
	paths, err := changedPaths(root, since)
	if err != nil {
		return nil, err
	}
	dependenciesChanged := false
	for _, path := range paths {
		if path == "go.mod" || path == "go.sum" {
			dependenciesChanged = true
		}
	}
	if len(paths) == 0 {
		return nil, nil
	}
	packages, err := modulePackages(root)
	if err != nil {
		return nil, err
	}
	byDir := make(map[string]packageInfo, len(packages))
	byImport := make(map[string]string, len(packages))
	for _, pkg := range packages {
		rel, err := filepath.Rel(root, pkg.Dir)
		if err != nil {
			return nil, err
		}
		byDir[filepath.ToSlash(rel)] = pkg
		byImport[pkg.ImportPath] = filepath.ToSlash(rel)
	}
	prefix, err := gitOutput(root, "rev-parse", "--show-prefix")
	if err != nil {
		return nil, err
	}
	selected := make(map[target]bool)
	if dependenciesChanged {
		for _, t := range targets {
			selected[t] = true
		}
	}
	var changes []codeChange
	for _, path := range paths {
		pkg := byDir[filepath.ToSlash(filepath.Dir(path))].ImportPath
		// Corpus changes belong to their target, including deleted inputs.
		if parent, rest, ok := strings.Cut(path, "testdata/fuzz/"); ok {
			name, _, _ := strings.Cut(rest, "/")
			selected[target{pkg: byDir[filepath.ToSlash(filepath.Clean(parent))].ImportPath, name: name}] = true
			continue
		}
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		old, current, hunks, err := fileChanges(root, since, strings.TrimSpace(string(prefix)), path)
		if err != nil {
			return nil, err
		}
		for _, h := range hunks {
			// Comparing tokens also ignores edits to inline comments and
			// whitespace on lines that contain code.
			if old.tokens(h.oldFirst, h.oldCount) == current.tokens(h.newFirst, h.newCount) {
				continue
			}
			for line := h.newFirst; line < h.newFirst+h.newCount; line++ {
				if current.code(line) {
					if !selectDeclaration(current, path, line, pkg, targets, selected) {
						changes = append(changes, newCodeChange(current, path, line, line))
					}
				}
			}
			deletedCode := false
			for line := h.oldFirst; line < h.oldFirst+h.oldCount; line++ {
				if old.code(line) && !selectDeclaration(old, path, line, pkg, targets, selected) {
					deletedCode = true
				}
			}
			if deletedCode && current.tokens(h.newFirst, h.newCount) == "" {
				// In a zero-length hunk Git gives the line before the
				// deletion, unlike its first-line notation for insertions.
				before := h.newFirst
				if h.newCount != 0 {
					before--
				}
				after := min(before+1, len(current.lines))
				// A deletion can leave a neighbor outside every function.
				// That line selects the package, as a declaration does.
				for line := max(1, before); line <= after; line++ {
					selectDeclaration(current, path, line, pkg, targets, selected)
				}
				changes = append(changes, newCodeChange(current, path, max(1, before), max(1, after)))
			}
		}
	}
	if len(changes) != 0 {
		if err := measureSeeds(root, targets, byImport, changes, selected); err != nil {
			return nil, err
		}
		if err := markEncoders(root, byDir, changes); err != nil {
			return nil, err
		}
		reportGaps(report, changes)
	}
	var result []target
	for _, t := range targets {
		if selected[t] {
			result = append(result, t)
		}
	}
	return result, nil
}

// sourceLines keeps tokens by physical line and the function owning each
// line. Empty token strings denote comments or blank lines. The parser and
// scanner handle block comments and multiline literals without heuristics.
type sourceLines struct {
	lines []string
	funcs []*sourceFunction
}

// sourceFunction includes the opening line and closing brace, which need
// not belong to a coverage block. Its name identifies test fuzz functions.
type sourceFunction struct {
	name        string
	first, last int
	encoder     bool // writes Sagüin's own values; see markEncoders
}

func parseSource(path string, data []byte) (sourceLines, error) {
	if len(data) == 0 {
		return sourceLines{}, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, data, 0)
	if err != nil {
		return sourceLines{}, err
	}
	file := fset.File(f.Pos())
	source := sourceLines{lines: make([]string, file.LineCount()), funcs: make([]*sourceFunction, file.LineCount())}
	var scan scanner.Scanner
	scan.Init(file, data, nil, 0)
	for {
		pos, tok, lit := scan.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON && lit == "\n" {
			continue
		}
		for i, part := range strings.Split(lit, "\n") {
			line := file.Line(pos) - 1 + i
			source.lines[line] += tok.String() + ":" + strconv.Quote(part) + " "
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		var name string
		switch fn := n.(type) {
		case *ast.FuncDecl:
			name = fn.Name.Name
		case *ast.FuncLit:
		default:
			return true
		}
		fn := &sourceFunction{name: name, first: file.Line(n.Pos()), last: file.Line(n.End())}
		for line := fn.first; line <= fn.last; line++ {
			if source.funcs[line-1] == nil {
				source.funcs[line-1] = fn
			}
		}
		// Literals inside this function stay with the enclosing function,
		// including the callback of a test's Fuzz function.
		return false
	})
	return source, nil
}

func (s sourceLines) code(line int) bool {
	return line > 0 && line <= len(s.lines) && s.lines[line-1] != ""
}

func (s sourceLines) tokens(first, count int) string {
	if count == 0 {
		return ""
	}
	return strings.Join(s.lines[first-1:first-1+count], "")
}

// Test code selects its owning Fuzz function, or every target of the
// package for helpers and declarations. Non-test declarations outside
// functions also select the package: line coverage cannot observe them.
func selectDeclaration(source sourceLines, path string, line int, pkg string, targets []target, selected map[target]bool) bool {
	fn := source.funcs[line-1]
	if strings.HasSuffix(path, "_test.go") {
		if fn != nil {
			owner := target{pkg: pkg, name: fn.name}
			if slices.Contains(targets, owner) {
				selected[owner] = true
				return true
			}
		}
	} else if fn != nil {
		return false
	}
	for _, t := range targets {
		if t.pkg == pkg {
			selected[t] = true
		}
	}
	return true
}

type diffHunk struct {
	oldFirst, oldCount int
	newFirst, newCount int
}

func fileChanges(root, since, prefix, path string) (old, current sourceLines, hunks []diffHunk, err error) {
	diff, err := gitOutput(root, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--unified=0", since, "--", path)
	if err != nil {
		return old, current, nil, err
	}
	data, err := os.ReadFile(filepath.Join(root, path))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return old, current, nil, err
	}
	current, err = parseSource(path, data)
	if err != nil {
		return old, current, nil, err
	}
	if len(diff) == 0 {
		// An untracked file has no Git diff: all of it is new.
		return old, current, []diffHunk{{newFirst: 1, newCount: len(current.lines)}}, nil
	}
	if !bytes.Contains(diff, []byte("\n--- /dev/null\n")) {
		data, err = gitOutput(root, "show", since+":"+prefix+path)
		if err != nil {
			return old, current, nil, err
		}
		old, err = parseSource(path, data)
		if err != nil {
			return old, current, nil, err
		}
	}
	for line := range strings.SplitSeq(string(diff), "\n") {
		if !strings.HasPrefix(line, "@@ ") {
			continue
		}
		fields := strings.Fields(line)
		var h diffHunk
		h.oldFirst, h.oldCount, err = diffRange(fields[1][1:])
		if err != nil {
			return old, current, nil, err
		}
		h.newFirst, h.newCount, err = diffRange(fields[2][1:])
		if err != nil {
			return old, current, nil, err
		}
		hunks = append(hunks, h)
	}
	return old, current, hunks, nil
}

func diffRange(text string) (first, count int, err error) {
	start, length, ok := strings.Cut(text, ",")
	first, err = strconv.Atoi(start)
	if err != nil {
		return 0, 0, err
	}
	count = 1
	if ok {
		count, err = strconv.Atoi(length)
	}
	return first, count, err
}

func gitOutput(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func measureSeeds(root string, targets []target, packages map[string]string, changes []codeChange, selected map[target]bool) (err error) {
	// Index once: each profile block needs only changes in its own file.
	byFile := make(map[string][]*codeChange)
	for i := range changes {
		c := &changes[i]
		byFile[c.file] = append(byFile[c.file], c)
	}
	tmp, err := os.MkdirTemp("", "fuzz-coverage-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(tmp)) }()
	profile := filepath.Join(tmp, "seeds.cover")
	for _, t := range targets {
		cmd := exec.Command("go", "test", "-count=1", "-run=^"+t.name+"$", "-coverpkg=./...", "-coverprofile="+profile, t.pkg)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("seeds for %s %s: %w\n%s", t.pkg, t.name, err, out)
		}
		data, err := os.ReadFile(profile)
		if err != nil {
			return err
		}
		if err := applyCoverage(data, packages, byFile, t, selected); err != nil {
			return err
		}
	}
	return nil
}

// applyCoverage selects by executed functions, but records line coverage
// separately for the gap report. Zero-count blocks still hold statements.
func applyCoverage(data []byte, packages map[string]string, changes map[string][]*codeChange, t target, selected map[target]bool) error {
	for line := range strings.SplitSeq(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return fmt.Errorf("invalid coverage line: %s", line)
		}
		count, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return err
		}
		colon := strings.LastIndexByte(fields[0], ':')
		if colon < 0 {
			return fmt.Errorf("invalid coverage location: %s", fields[0])
		}
		name, coords := fields[0][:colon], fields[0][colon+1:]
		pkg, file := filepath.Split(name)
		dir, ok := packages[strings.TrimSuffix(pkg, "/")]
		if !ok {
			continue
		}
		path := filepath.ToSlash(filepath.Join(dir, file))
		var first, firstColumn, last, lastColumn int
		if _, err := fmt.Sscanf(coords, "%d.%d,%d.%d", &first, &firstColumn, &last, &lastColumn); err != nil {
			return err
		}
		for _, c := range changes[path] {
			if c.first <= last && c.last >= first {
				c.instrumented = true
				if count != 0 {
					c.reached = true
				}
			}
			if count != 0 {
				for _, fn := range c.funcs {
					if fn.first <= last && fn.last >= first {
						c.functionReached = true
						selected[t] = true
					}
				}
			}
		}
	}
	return nil
}

// A location is a line of a file, the file relative to the module.
type location struct {
	file string
	line int
}

// reportGaps groups adjacent unreached statements and uninstrumented lines
// whose functions no seed runs. Test-support code is tooling, and an
// encoder is not fuzzed by design (see markEncoders): neither is a gap.
// A deletion uses a neighboring line; duplicate locations collapse.
func reportGaps(w io.Writer, changes []codeChange) {
	var gaps []location
	for _, c := range changes {
		if len(c.funcs) == 0 || harness.InTestSupport(c.file) || c.inEncoders() {
			continue
		}
		if !c.reached && (c.instrumented || !c.functionReached) {
			gaps = append(gaps, location{file: c.file, line: c.last})
		}
	}
	slices.SortFunc(gaps, func(a, b location) int {
		if a.file != b.file {
			return strings.Compare(a.file, b.file)
		}
		return a.line - b.line
	})
	gaps = slices.Compact(gaps)
	for i := 0; i < len(gaps); {
		first, last := gaps[i], gaps[i]
		i++
		for i < len(gaps) && gaps[i].file == last.file && gaps[i].line == last.line+1 {
			last = gaps[i]
			i++
		}
		if first.line == last.line {
			harness.Say(w, "fuzz: no seed reaches %s:%d\n", first.file, first.line)
		} else {
			harness.Say(w, "fuzz: no seed reaches %s:%d-%d\n", first.file, first.line, last.line)
		}
	}
}
