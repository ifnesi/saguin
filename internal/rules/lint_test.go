package rules

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The standard for code and tests (saguin-sprint skill): "idiomatic,
// efficient Go, as Effective Go and Go's Code Review Comments describe it".
// The maintainer's decision: a .golangci.yml in the repository root sets
// golangci-lint, which `make check` runs, to hold the code to it.

// requiredLinters are the linters the decisions name beyond golangci-lint's
// standard set, which is required too and read from golangci-lint itself.
// Item 0022a adds dupl: "Turn on golangci-lint's dupl detector in
// .golangci.yml for code and tests, so make ci refuses a copy at the
// commit".
var requiredLinters = []string{
	"gocritic", "modernize", "revive", "perfsprint", "prealloc", "unconvert",
	"errorlint", "nilerr", "unparam", "intrange", "usestdlibvars", "bodyclose",
	"dupl",
}

// lintFlags are golangci-lint's flags that shape which linters run, with
// whether each takes a value: the ones `make check` may give `run` that
// `linters` takes too.
var lintFlags = map[string]bool{
	"-c": true, "--config": true, "--no-config": false, "--default": true,
	"-E": true, "--enable": true, "-D": true, "--disable": true,
	"--enable-only": true, "--fast-only": false,
}

// lintCommand returns the golangci-lint `make check` runs and the flags it
// gives it that shape which linters run, so the checks below ask the same
// golangci-lint the same question.
func lintCommand(t *testing.T, root string) (path string, runFlags []string) {
	t.Helper()
	cmds, plan := planIn(t, root, "check")
	for _, c := range cmds {
		_, _, argv := parts(c)
		if len(argv) < 2 || filepath.Base(argv[0]) != "golangci-lint" || argv[1] != "run" {
			continue
		}
		for i := 2; i < len(argv); i++ {
			name, _, inline := strings.Cut(argv[i], "=")
			takesValue, ok := lintFlags[name]
			if !ok {
				continue
			}
			runFlags = append(runFlags, argv[i])
			if takesValue && !inline && i+1 < len(argv) {
				i++
				runFlags = append(runFlags, argv[i])
			}
		}
		return argv[0], runFlags
	}
	t.Fatalf("`make check` runs no `golangci-lint run`; it would run:\n%s", plan)
	return "", nil
}

// lint runs golangci-lint with args in dir and returns all it printed and
// how it ended.
func lint(t *testing.T, dir string, env []string, bin string, args ...string) (string, error) {
	t.Helper()
	out, err := command(t, dir, append(os.Environ(), env...), 0, bin, args...).CombinedOutput()
	return string(out), err
}

// lintProbe writes files, keyed by their paths, into a new module of their
// own and runs golangci-lint over it as `make check` runs it, with env added
// to the environment, reading the repository's .golangci.yml where `make
// check` names no file. It returns all golangci-lint printed, issues and log
// alike, whatever its exit status: a probe asks what golangci-lint does.
func lintProbe(t *testing.T, root string, env []string, files map[string]string) string {
	t.Helper()
	bin, flags := lintCommand(t, root)
	probe := t.TempDir()
	module := map[string]string{"go.mod": "module example.com/lintprobe\n\ngo 1.25.0\n"}
	maps.Copy(module, files)
	for name, content := range module {
		path := filepath.Join(probe, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A probe's module is its own, so it shares no cache entry with any
	// other run: it may run beside `make check`'s own golangci-lint, which
	// holds golangci-lint's lock while it runs, and beside the other
	// probes.
	args := append([]string{"run", "--allow-parallel-runners"}, flags...)
	if !slices.ContainsFunc(flags, func(f string) bool { return f == "-c" || strings.HasPrefix(f, "--config") }) {
		args = append(args, "-c", filepath.Join(root, ".golangci.yml"))
	}
	out, _ := lint(t, probe, env, bin, append(args, "./...")...)
	return out
}

// reasoned reports whether name appears in the configuration's lines
// outside comments and, each time, carries a reason: a comment on its line,
// or a comment line right above it.
func reasoned(lines []string, name string) (mentioned, ok bool) {
	word := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`)
	ok = true
	for i, line := range lines {
		code, comment, _ := strings.Cut(line, "#")
		if !word.MatchString(code) {
			continue
		}
		mentioned = true
		above := ""
		if i > 0 {
			above = strings.TrimSpace(lines[i-1])
		}
		if strings.TrimSpace(comment) == "" && (!strings.HasPrefix(above, "#") || strings.TrimSpace(strings.TrimLeft(above, "#")) == "") {
			ok = false
		}
	}
	return mentioned, ok
}

// gocriticChecks reads the checks gocritic reports using, and not using,
// from golangci-lint's debug log (GL_DEBUG=gocritic): "Final used checks
// (N): a, b" and "Final not used checks (N): c".
func gocriticChecks(log string) (used, unused []string, found bool) {
	list := func(label string) ([]string, bool) {
		m := regexp.MustCompile(regexp.QuoteMeta(label) + ` \(\d+\):([^"\n]*)`).FindStringSubmatch(log)
		if m == nil {
			return nil, false
		}
		var names []string
		for name := range strings.SplitSeq(m[1], ",") {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, name)
			}
		}
		return names, true
	}
	used, found = list("Final used checks")
	unused, _ = list("Final not used checks")
	return used, unused, found
}

// TestLintConfig asks golangci-lint, as `make check` runs it, what it will
// run: it reads the repository's .golangci.yml; it enables the standard set
// and every linter of requiredLinters; gocritic runs all its checks; and
// every linter the file names, whether it enables, sets or switches it off,
// and any gocritic check switched off, carries a written reason there (the
// maintainer's decision; item 0022a: "each kept linter's reasoning goes in
// .golangci.yml, as lint_test.go requires", and "The settings' reasoning
// goes in .golangci.yml's comments"). golangci-lint answers itself, through
// `linters --json` and gocritic's debug log over a one-file module: the
// module has no YAML library, and none may be added (R9).
func TestLintConfig(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	bin, flags := lintCommand(t, root)
	if slices.Contains(flags, "--no-config") {
		t.Fatalf("`make check` runs golangci-lint with --no-config: %q", flags)
	}

	// `config path` takes only the flags that choose the file.
	var fileFlags []string
	for i := 0; i < len(flags); i++ {
		if flags[i] == "-c" || flags[i] == "--config" {
			fileFlags = append(fileFlags, flags[i:i+2]...)
			i++
		} else if strings.HasPrefix(flags[i], "--config=") {
			fileFlags = append(fileFlags, flags[i])
		}
	}
	want := filepath.Join(root, ".golangci.yml")
	out, err := lint(t, root, nil, bin, append([]string{"config", "path"}, fileFlags...)...)
	path := strings.TrimSpace(out)
	if path != "" && !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	if err != nil {
		t.Fatalf("golangci-lint, as `make check` runs it, reads no configuration (%v); want %s:\n%s", err, want, out)
	}
	if filepath.Clean(path) != want {
		t.Fatalf("golangci-lint, as `make check` runs it, reads %s; want %s", path, want)
	}
	text, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(text), "\n")

	out, err = lint(t, root, nil, bin, append([]string{"linters", "--json"}, flags...)...)
	if err != nil {
		t.Fatalf("golangci-lint linters --json: %v\n%s", err, out)
	}
	type linter struct {
		Name   string   `json:"name"`
		Groups []string `json:"groups"`
	}
	var set struct{ Enabled, Disabled []linter }
	if err := json.Unmarshal([]byte(out), &set); err != nil {
		t.Fatalf("reading golangci-lint linters --json: %v\n%s", err, out)
	}
	enabled := map[string]bool{}
	for _, l := range set.Enabled {
		enabled[l.Name] = true
	}
	linters := slices.Concat(set.Enabled, set.Disabled)
	required := slices.Clone(requiredLinters)
	for _, l := range linters {
		if slices.Contains(l.Groups, "standard") {
			required = append(required, l.Name)
		}
	}
	for _, name := range required {
		if !enabled[name] {
			t.Errorf("golangci-lint, as `make check` runs it, does not run %s", name)
		}
	}
	for _, l := range linters {
		if mentioned, ok := reasoned(lines, l.Name); mentioned && !ok {
			t.Errorf("%s names %s without a reason: a comment on its line, or a comment line above", want, l.Name)
		}
	}

	// gocritic's checks, from its debug log over a one-file module.
	out = lintProbe(t, root, []string{"GL_DEBUG=gocritic"}, map[string]string{
		"probe.go": "// Package probe gives gocritic a package to load.\npackage probe\n",
	})
	used, unused, found := gocriticChecks(out)
	if !found || len(used) == 0 {
		t.Fatalf("golangci-lint ran no gocritic check over a one-file module:\n%s", out)
	}
	for _, check := range unused {
		if mentioned, ok := reasoned(lines, check); !mentioned || !ok {
			t.Errorf("gocritic's %s is switched off without a reason in %s: name it, with a comment on its line or a comment line above", check, want)
		}
	}
}

// twoCopies returns a Go file of package pkg declaring one function twice,
// under two names. golangci-lint 2.14.0's dupl, enabled alone over it,
// reports the pair at every threshold up to 108 tokens and at none from 109
// (measured by bisection over thresholds 1 to 400; nothing at 150): the
// threshold of 100 catches it, and one raised to hide copies does not.
func twoCopies(pkg string) string {
	const copied = `
// %[1]s splits each line of text at its first '=' into a key and a value,
// both trimmed, skipping blank lines and comments, which start with '#' or
// ';'. It returns the pairs found and the lines holding no key, or a key
// already found.
func %[1]s(text string) (pairs map[string]string, bad []string) {
	pairs = map[string]string{}
	for line := range strings.Lines(text) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		key, value, _ := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if _, seen := pairs[key]; key == "" || seen {
			bad = append(bad, line)
			continue
		}
		pairs[key] = strings.TrimSpace(value)
	}
	return pairs, bad
}
`
	return "// Package " + pkg + " holds a planted copy.\npackage " + pkg + "\n\nimport \"strings\"\n" +
		fmt.Sprintf(copied, "First") + fmt.Sprintf(copied, "Second")
}

// TestLintFindsCopies plants one function twice in a probe module, in
// ordinary files and, in another package, in test files, and requires
// golangci-lint, as `make check` runs it with the repository's
// .golangci.yml, to report each copy through dupl (item 0022a: "Turn on
// golangci-lint's dupl detector in .golangci.yml for code and tests, so make
// ci refuses a copy at the commit"; its threshold is 100, by measurement,
// and each copy it finds "is fixed by sharing one function, never by tuning
// the threshold to hide it").
func TestLintFindsCopies(t *testing.T) {
	t.Parallel()
	files := []string{"code/code.go", "tests/tests_test.go"}
	out := lintProbe(t, moduleRoot(t), nil, map[string]string{
		files[0]: twoCopies("code"),
		files[1]: twoCopies("tests"),
	})
	for _, file := range files {
		// golangci-lint prints each issue as path:line[:column]: text (linter),
		// the path relative to the configuration's folder.
		issue := regexp.MustCompile(`(?m)^(\S*/)?` + regexp.QuoteMeta(file) + `:\d+(:\d+)?: .*\(dupl\)$`)
		if !issue.MatchString(out) {
			t.Errorf("golangci-lint, as `make check` runs it, does not report the copy planted in %s through dupl:\n%s", file, out)
		}
	}
}
