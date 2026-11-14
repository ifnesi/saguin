package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// mod is a small module of its own (testdata is outside ./...), holding a
// package with no fuzz target, one whose target passes, and one whose first
// target fails before a second.
const mod = "testdata/mod"

// fuzz runs the harness on mod, each target 20 runs, and returns its exit
// code and everything it printed, in order.
func fuzz(t *testing.T, args ...string) (exit int, output string) {
	t.Helper()
	exit, output, _ = testkit.RunCommand(run, testkit.Combined, append([]string{"-dir", mod, "-fuzztime", "20x"}, args...)...)
	return exit, output
}

func TestNoTargetsSucceeds(t *testing.T) {
	code, out := fuzz(t, "./nofuzz")
	if code != 0 || !strings.Contains(out, "no fuzz targets") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
}

func TestRunsEachTarget(t *testing.T) {
	code, out := fuzz(t, "./pass")
	if code != 0 || !strings.Contains(out, "FuzzPasses for 20x") || !strings.Contains(out, "1 targets passed") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
}

// TestStopsAtFirstFailure also checks the failure left nothing behind: a
// failing seed is not written to the module's testdata.
func TestStopsAtFirstFailure(t *testing.T) {
	code, out := fuzz(t, "./...")
	if code != 1 || !strings.Contains(out, "FAIL example.com/fuzzmod/fail FuzzFails") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
	if strings.Contains(out, "FuzzNeverReached for") || strings.Contains(out, "FuzzPasses for") {
		t.Errorf("ran on after the first failure:\n%s", out)
	}
	left, err := exec.Command("git", "status", "--porcelain", "--untracked-files=all", "--", filepath.Join(mod, "fail")).Output()
	if err == nil && bytes.Contains(left, []byte("testdata/fuzz")) {
		t.Errorf("the failure wrote a corpus file:\n%s", left)
	}
}

// TestKeepsWhatFuzzingFound: when fuzzing, not a seed, finds a failing
// input, Go writes it to testdata/fuzz/<Target>/ to replay; it is the
// evidence, and once the bug is fixed it stays as a regression seed. The run
// fails, the file stays, and the report names it. The module is a throwaway
// one: FuzzLong passes its one short seed and fails on any input longer than
// four bytes, which the fuzzer finds within moments.
func TestKeepsWhatFuzzingFound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for path, content := range map[string]string{
		"go.mod": "module example.com/finds\n\ngo 1.25.0\n",
		"long/long_test.go": `package long

import "testing"

func FuzzLong(f *testing.F) {
	f.Add([]byte("a"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4 {
			t.Fatalf("an input of %d bytes", len(b))
		}
	})
}
`,
	} {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	// The time is a ceiling: go test stops at the first failure.
	code := run([]string{"-dir", dir, "-fuzztime", "30s"}, &out, &out)
	if code != 1 {
		t.Errorf("exit %d, want 1: the fuzzer's find is a failure; output:\n%s", code, out.String())
	}
	corpus := filepath.Join(dir, "long", "testdata", "fuzz", "FuzzLong")
	kept, err := os.ReadDir(corpus)
	if err != nil || len(kept) == 0 {
		t.Fatalf("the failing input is gone from %s (%v); output:\n%s", corpus, err, out.String())
	}
	for _, e := range kept {
		if !strings.Contains(out.String(), e.Name()) {
			t.Errorf("the report does not name %s, the failing input kept; output:\n%s", e.Name(), out.String())
		}
	}
}

// TestMinimizeTimeReachesGoTest: the per-commit fuzz caps Go's input
// minimisation at 1 second a target, since an audit found a target spending
// its whole 5 seconds minimising one large input; the long fuzz keeps Go's
// default (the maintainer's decision). The runner hands its
// -fuzzminimizetime to each go test that fuzzes, and without it hands none.
// A stand-in go, first in PATH, records the arguments of each command the
// runner starts, then runs the real go; PATH is the process's own, so this
// test is not parallel.
func TestMinimizeTimeReachesGoTest(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	calls := filepath.Join(bin, "calls")
	// Each call is one line, its arguments each ended by a NUL.
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\0' \"$@\" >> '%[1]s'\nprintf '\\n' >> '%[1]s'\nexec '%[2]s' \"$@\"\n", calls, goBin)
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	for _, c := range []struct {
		args []string
		want string // the -fuzzminimizetime each fuzzing go test gets; "" for none
	}{
		{[]string{"-fuzzminimizetime", "1s"}, "1s"},
		{nil, ""},
	} {
		if err := os.Remove(calls); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		code, out := fuzz(t, append(c.args, "./pass")...)
		if code != 0 {
			t.Errorf("runner given %q: exit %d, output:\n%s", c.args, code, out)
			continue
		}
		recorded, err := os.ReadFile(calls)
		if err != nil {
			t.Fatal(err)
		}
		fuzzing := 0
		for call := range strings.SplitSeq(strings.TrimSuffix(string(recorded), "\n"), "\n") {
			argv := strings.Split(strings.TrimSuffix(call, "\x00"), "\x00")
			if argv[0] != "test" {
				continue
			}
			if _, ok := goFlag(argv, "fuzz"); !ok {
				continue
			}
			fuzzing++
			got, given := goFlag(argv, "fuzzminimizetime")
			if got != c.want || given != (c.want != "") {
				t.Errorf("runner given %q starts `go %s`; want -fuzzminimizetime %q (\"\": not given)", c.args, strings.Join(argv, " "), c.want)
			}
		}
		if fuzzing == 0 {
			t.Errorf("runner given %q fuzzed nothing; it started:\n%s", c.args, bytes.ReplaceAll(recorded, []byte{0}, []byte{' '}))
		}
	}
}

// goFlag returns the value of go's flag called name in argv, given as
// -name=value or -name value, and whether it is given at all.
func goFlag(argv []string, name string) (value string, given bool) {
	for i, w := range argv {
		f, v, hasValue := strings.Cut(w, "=")
		if f != "-"+name && f != "--"+name {
			continue
		}
		if hasValue {
			return v, true
		}
		if i+1 < len(argv) {
			return argv[i+1], true
		}
		return "", true
	}
	return "", false
}

// The maintainer's decision: each audit round's long fuzz runs only what
// changed. Given -since <commit>, the runner fuzzes a target when
//
//  1. running it over its seed corpus (its f.Add seeds and its
//     testdata/fuzz/<Target>/ files) runs any code of a function holding a
//     non-test Go line changed since the commit, or holding a line either
//     side of a deletion; untracked files count, comments and blank lines
//     do not, in any file. A changed statement the seeds never reach still
//     selects the targets that run its function: fuzzing is what finds the
//     input that reaches it;
//  2. its Fuzz function or its corpus changed, or a _test.go file of its
//     package changed outside every Fuzz function, or a line outside every
//     function (a constant, type, variable, import or package line) changed
//     in its package; or
//  3. go.mod or go.sum changed, which selects every target.
//
// Changed broker code that no target's seeds reach is listed, by file and
// lines (a deletion by the line it sits next to), so the missing seed is
// seen and added: a statement no seed reaches, or a line no coverage block
// holds (a function's opening line, a case or default label, a closing
// brace) in a function no seed runs. Code under a test-support directory,
// one of harness.TestSupportDirs (the harness and testkit), is never listed:
// it is test tooling no fuzz target runs. Nor is an encoder, which writes
// Sagüin's own values rather than reading bytes from outside, and so is not
// fuzzed by design (design.md, Testing): a function named Append… taking
// and returning a []byte first, or an unexported one used only inside
// encoders. A decoder is listed. A gap never fails the run.
// With nothing selected the runner says so, without claiming the module has
// no fuzz targets, and passes; without -since it runs every target; an
// unknown commit fails.

// gapReport begins each line of the runner's report that names changed code
// no seed reaches, followed by "<file>:<line>" or "<file>:<first>-<last>",
// the file relative to the module.
const gapReport = "fuzz: no seed reaches "

// sinceRepo is a throwaway module in a git repository of its own, under
// TMPDIR (the repository's .tmp/ when run through make).
type sinceRepo struct {
	t   *testing.T
	dir string
}

// sinceFiles are the module's files at its first commit. In package p,
// FuzzKind's seed reaches the start of Kind and its last return, and in
// letter its switch and the late case, never the vowel case or the default;
// FuzzSize's reaches Size but for its cap; no seed reaches Spare. FuzzQ, in
// package q, has no f.Add seed: its corpus file, the empty input, reaches
// Kind's first return through q, never letter.
var sinceFiles = map[string]string{
	"go.mod":    "module example.com/lines\n\ngo 1.25.0\n",
	"README.md": "A module for the fuzz runner's tests.\n",
	"p/p.go": `// Package p holds the code the tests change.
package p

// limit is the most Size counts.
const limit = 4

// Kind names the class of b.
func Kind(b []byte) string {
	// An empty input is a class of its own.
	if len(b) == 0 {
		return "empty"
	}
	return letter(b[0])
}

// letter names the class of the letter c.
func letter(c byte) string {
	switch c {
	case 'a', 'e', 'i', 'o':
		return "vowel"
	case 'x', 'y', 'z':
		return "late"
	default:
		return "other"
	}
}

// Size returns how long b is, up to limit.
func Size(b []byte) int {
	n := len(b)
	if n > limit {
		n = limit
	}
	return n
}

// Spare counts b twice, and one more; no seed reaches it.
func Spare(b []byte) int {
	n := 2 * len(b)
	n++
	return n
}
`,
	"p/p_test.go": `package p

import "testing"

// seed gives a target its first input.
func seed(f *testing.F, s string) {
	f.Add([]byte(s))
}

func FuzzKind(f *testing.F) {
	seed(f, "x")
	f.Fuzz(func(t *testing.T, b []byte) { Kind(b) })
}

func FuzzSize(f *testing.F) {
	seed(f, "a")
	f.Fuzz(func(t *testing.T, b []byte) { Size(b) })
}
`,
	"q/q.go": `// Package q reaches p's code from another package.
package q

import "example.com/lines/p"

// Q names the class of b, as p does.
func Q(b []byte) string { return p.Kind(b) }
`,
	"q/q_test.go": `package q

import "testing"

func FuzzQ(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) { Q(b) })
}
`,
	"q/testdata/fuzz/FuzzQ/empty": "go test fuzz v1\n[]byte(\"\")\n",
}

// newSinceRepo writes sinceFiles and commits them.
func newSinceRepo(t *testing.T) sinceRepo {
	t.Helper()
	r := sinceRepo{t: t, dir: t.TempDir()}
	if out, err := exec.Command("git", "init", "--quiet", r.dir).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	for path, content := range sinceFiles {
		r.write(path, content)
	}
	r.commit()
	return r
}

// git runs git in the throwaway repository and nowhere else: GIT_DIR and
// GIT_WORK_TREE name it, so a failed init can never reach the repository
// around TMPDIR; no user or system configuration applies.
func (r sinceRepo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = r.dir
	cmd.Env = append(os.Environ(),
		"GIT_DIR="+filepath.Join(r.dir, ".git"), "GIT_WORK_TREE="+r.dir,
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// write puts content at path in the repository's working tree.
func (r sinceRepo) write(path, content string) {
	r.t.Helper()
	testkit.WriteFile(r.t, r.dir, path, content)
}

// edit replaces the one occurrence of from in the file at path with to.
func (r sinceRepo) edit(path, from, to string) {
	r.t.Helper()
	data, err := os.ReadFile(filepath.Join(r.dir, path))
	if err != nil {
		r.t.Fatal(err)
	}
	if n := strings.Count(string(data), from); n != 1 {
		r.t.Fatalf("%s holds %q %d times; want once", path, from, n)
	}
	r.write(path, strings.Replace(string(data), from, to, 1))
}

// commit commits every change.
func (r sinceRepo) commit() {
	r.t.Helper()
	r.git("add", "-A")
	r.git("commit", "--quiet", "--no-gpg-sign", "--no-verify", "-m", "change")
}

// fuzzSince runs the runner on the repository with -since and returns its
// exit code, its output, and the targets it fuzzed.
func (r sinceRepo) fuzzSince(since string) (exit int, output string, targets []string) {
	r.t.Helper()
	var out bytes.Buffer
	code := run([]string{"-dir", r.dir, "-fuzztime", "1x", "-since", since}, &out, &out)
	return code, out.String(), fuzzed(out.String())
}

// fuzzed returns the targets a run's report says it fuzzed, sorted: each is
// announced as "fuzz: <package> <target> for <time>".
func fuzzed(out string) []string {
	var names []string
	for line := range strings.SplitSeq(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 5 && f[0] == "fuzz:" && f[3] == "for" {
			names = append(names, f[2])
		}
	}
	slices.Sort(names)
	return names
}

// gaps returns the spans of changed code a run's report says no seed
// reaches, each as "<file>:<line>" or "<file>:<first>-<last>", sorted.
func gaps(out string) []string {
	var spans []string
	for line := range strings.SplitSeq(out, "\n") {
		if span, ok := strings.CutPrefix(line, gapReport); ok {
			spans = append(spans, strings.TrimSpace(span))
		}
	}
	slices.Sort(spans)
	return spans
}

// lineOf returns the number, from 1, of the line of content that holds text.
func lineOf(t *testing.T, content, text string) int {
	t.Helper()
	before, _, found := strings.Cut(content, text)
	if !found {
		t.Fatalf("no %q in:\n%s", text, content)
	}
	return strings.Count(before, "\n") + 1
}

// TestSinceSelectsByChangedLines makes one change in a repository of its own
// for each case, commits it, and runs the runner -since the first commit.
// Each case also checks that the run left the repository as it found it.
func TestSinceSelectsByChangedLines(t *testing.T) {
	t.Parallel()
	// at names the line of p.go that holds text, as a gap is reported.
	at := func(text string) string {
		return fmt.Sprintf("p/p.go:%d", lineOf(t, sinceFiles["p/p.go"], text))
	}
	// Spare's lines: the first, n++, and the line after n++ once it is gone.
	spareFirst, spareIncrement := at("n := 2 * len(b)"), at("\tn++")
	// letter's vowel case, a statement no seed reaches.
	vowel := at(`return "vowel"`)
	cases := []struct {
		name   string
		change func(r sinceRepo) // nil: no change at all
		want   []string          // the targets fuzzed; nil: none, and the runner says so
		gaps   [][]string        // the spans reported as reached by no seed, as any one of these lists; nil: none
	}{
		{name: "nothing changed"},
		{
			name:   "a line one target's seeds reach, in a file other targets share",
			change: func(r sinceRepo) { r.edit("p/p.go", "n := len(b)\n", "n := len(b) + 1\n") },
			want:   []string{"FuzzSize"},
		},
		{
			// Only FuzzQ's corpus file reaches this return, but FuzzKind's
			// seed runs Kind too, and a change selects every target that
			// runs its function (rule 1).
			name:   "a line in a function another package's target runs, from its corpus file",
			change: func(r sinceRepo) { r.edit("p/p.go", `return "empty"`, `return "none"`) },
			want:   []string{"FuzzKind", "FuzzQ"},
		},
		{
			name:   "a line no seed reaches",
			change: func(r sinceRepo) { r.edit("p/p.go", "n := 2 * len(b)", "n := 3 * len(b)") },
			gaps:   [][]string{{spareFirst}},
		},
		// Audit finding 261011-001a: "Go's blocks begin at a statement and
		// end at a statement or a closing brace, so a function's own line, a
		// `case` or `default` label and many closing braces lie in no block
		// at all", and "when a change touches only such a line, no target is
		// selected, though every seed runs the function". FuzzKind's seed
		// runs letter; FuzzSize, of the same package, and FuzzQ never do.
		{
			// "Reason codes in this project are `case` lists [...], so
			// adding or removing a code is exactly this change."
			name:   "a value added to a case list the seed passes over",
			change: func(r sinceRepo) { r.edit("p/p.go", "case 'a', 'e', 'i', 'o':", "case 'a', 'e', 'i', 'o', 'u':") },
			want:   []string{"FuzzKind"},
		},
		{
			name:   "a value removed from the case list the seed takes",
			change: func(r sinceRepo) { r.edit("p/p.go", "case 'x', 'y', 'z':", "case 'x', 'y':") },
			want:   []string{"FuzzKind"},
		},
		{
			// "A test for [...] a function-line-only change."
			name: "a function's opening line alone",
			change: func(r sinceRepo) {
				r.edit("p/p.go", "func letter(c byte) string {", "func letter(c byte) (class string) {")
			},
			want: []string{"FuzzKind"},
		},
		{
			// The statement that uses the parameter already selects
			// FuzzKind; the opening line beside it is "no seed reaches"
			// every time it changes, which it must not be.
			name: "a parameter renamed, on the opening line and where it is used",
			change: func(r sinceRepo) {
				r.edit("p/p.go", "func letter(c byte) string {\n\tswitch c {", "func letter(r byte) string {\n\tswitch r {")
			},
			want: []string{"FuzzKind"},
		},
		{
			// The maintainer's decision on the round's observation: a new
			// case arm for a reason no seed sends "prints its lines as a gap
			// and selects nothing, though every seed runs that function".
			// The statement no seed reaches selects the target that runs its
			// function, so fuzzing can find the input, and is still listed
			// so a seed is added. "a line no seed reaches" above is the same
			// in a function no seed runs: listed, and nothing selected.
			name:   "a statement on a branch no seed takes, in a function a seed runs",
			change: func(r sinceRepo) { r.edit("p/p.go", `return "vowel"`, `return "a vowel"`) },
			want:   []string{"FuzzKind"},
			gaps:   [][]string{{vowel}},
		},
		{
			// A line charged to its function is reached only when its
			// function is: the opening line of Spare, which no seed runs,
			// stays a gap, so the change is seen rather than lost.
			name:   "a function's opening line alone, in a function no seed runs",
			change: func(r sinceRepo) { r.edit("p/p.go", "func Spare(b []byte) int {", "func Spare(b []byte) (size int) {") },
			gaps:   [][]string{{at("func Spare(b []byte) int {")}},
		},
		{
			// "adding or removing a code is exactly this change": a whole
			// case clause removed is a deletion beside the switch and the
			// next label, both charged to letter, which FuzzKind's seed runs.
			name:   "a whole case clause deleted, in a function a seed runs",
			change: func(r sinceRepo) { r.edit("p/p.go", "\tcase 'a', 'e', 'i', 'o':\n\t\treturn \"vowel\"\n", "") },
			want:   []string{"FuzzKind"},
		},
		{
			name:   "a deletion between lines one target's seeds reach",
			change: func(r sinceRepo) { r.edit("p/p.go", "\tif n > limit {\n\t\tn = limit\n\t}\n", "") },
			want:   []string{"FuzzSize"},
		},
		{
			// The deletion sits between Spare's first line and the one that
			// now follows it; either may name it.
			name:   "a deletion between lines no seed reaches",
			change: func(r sinceRepo) { r.edit("p/p.go", "\tn++\n", "") },
			gaps:   [][]string{{spareFirst}, {spareIncrement}},
		},
		{
			name:   "a constant, which coverage cannot see",
			change: func(r sinceRepo) { r.edit("p/p.go", "const limit = 4", "const limit = 5") },
			want:   []string{"FuzzKind", "FuzzSize"},
		},
		{
			name: "a new type, which coverage cannot see",
			change: func(r sinceRepo) {
				r.edit("p/p.go", "const limit = 4\n", "const limit = 4\n\n// pair is a type nothing uses yet.\ntype pair struct {\n\ta, b int\n}\n")
			},
			want: []string{"FuzzKind", "FuzzSize"},
		},
		{
			name: "comments, a blank line inside code a seed reaches, and prose",
			change: func(r sinceRepo) {
				r.edit("p/p.go", "// Kind names the class of b.", "// Kind names the class of b: empty or some.")
				r.edit("p/p.go", "\t// An empty input is a class of its own.\n", "\t// An empty input is a class of its own,\n\t// apart from every other.\n\n")
				r.write("README.md", "Changed.\n")
			},
		},
		{
			name: "comments in a test file, inside a fuzz function and out",
			change: func(r sinceRepo) {
				r.edit("p/p_test.go", "// seed gives a target its first input.", "// seed gives a target its first input, as f.Add does.")
				r.edit("p/p_test.go", "\tseed(f, \"x\")\n", "\t// Any input but the empty one.\n\tseed(f, \"x\")\n")
			},
		},
		{
			name:   "a fuzz function, in a file other targets share",
			change: func(r sinceRepo) { r.edit("p/p_test.go", "seed(f, \"a\")\n", "seed(f, \"a\")\n\tseed(f, \"bb\")\n") },
			want:   []string{"FuzzSize"},
		},
		{
			name:   "a corpus file",
			change: func(r sinceRepo) { r.write("q/testdata/fuzz/FuzzQ/empty", "go test fuzz v1\n[]byte(\"q\")\n") },
			want:   []string{"FuzzQ"},
		},
		{
			name:   "a test helper, outside every fuzz function",
			change: func(r sinceRepo) { r.edit("p/p_test.go", "\tf.Add([]byte(s))\n", "\tf.Helper()\n\tf.Add([]byte(s))\n") },
			want:   []string{"FuzzKind", "FuzzSize"},
		},
		{
			name:   "go.mod",
			change: func(r sinceRepo) { r.edit("go.mod", "go 1.25.0\n", "go 1.25.0\n\nretract v0.0.1\n") },
			want:   []string{"FuzzKind", "FuzzQ", "FuzzSize"},
		},
		{
			name: "go.sum",
			change: func(r sinceRepo) {
				r.write("go.sum", "golang.org/x/text v0.3.0/go.mod h1:NqM8EUOU14njkJ3fqMW+pc6Ldnwhi/IjpwHt7yyuwOQ=\n")
			},
			want: []string{"FuzzKind", "FuzzQ", "FuzzSize"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newSinceRepo(t)
			base := r.git("rev-parse", "HEAD")
			if c.change != nil {
				c.change(r)
				r.commit()
			}
			code, out, got := r.fuzzSince(base)
			if code != 0 {
				t.Errorf("exit %d; output:\n%s", code, out)
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("fuzzed %q, want %q; output:\n%s", got, c.want, out)
			}
			if c.want == nil && !strings.Contains(out, "nothing to run") {
				t.Errorf("fuzzed nothing without saying so (\"nothing to run\"); output:\n%s", out)
			}
			// The round's observation: "no fuzz targets in ./..." "reads as
			// the module having none"; this one has three.
			if strings.Contains(out, "no fuzz targets") {
				t.Errorf("said the module has no fuzz targets; output:\n%s", out)
			}
			wantGaps := c.gaps
			if wantGaps == nil {
				wantGaps = [][]string{nil}
			}
			if spans := gaps(out); !slices.ContainsFunc(wantGaps, func(want []string) bool { return slices.Equal(spans, want) }) {
				t.Errorf("reported %q as reached by no seed (%q…), want one of %q; output:\n%s", spans, gapReport, wantGaps, out)
			}
			if left := r.git("status", "--porcelain", "--untracked-files=all"); left != "" {
				t.Errorf("the run left files behind:\n%s", left)
			}
		})
	}
}

// TestSinceUntrackedFilesCount: files Git does not track yet are changes
// too. A new corpus file in p selects its target; a new file in q selects
// q's target by its package line, and its code no seed reaches is listed.
func TestSinceUntrackedFilesCount(t *testing.T) {
	t.Parallel()
	const extra = `package q

// Extra is new, and no seed reaches it.
func Extra(b []byte) int {
	return len(b) + 2
}
`
	r := newSinceRepo(t)
	r.write("p/testdata/fuzz/FuzzSize/more", "go test fuzz v1\n[]byte(\"more\")\n")
	r.write("q/extra.go", extra)
	code, out, got := r.fuzzSince(r.git("rev-parse", "HEAD"))
	if code != 0 || !slices.Equal(got, []string{"FuzzQ", "FuzzSize"}) {
		t.Errorf("exit %d, fuzzed %q; want FuzzQ and FuzzSize; output:\n%s", code, got, out)
	}
	ret := lineOf(t, extra, "return len(b) + 2")
	reported := false
	for _, span := range gaps(out) {
		file, lines, _ := strings.Cut(span, ":")
		first, last, isRange := strings.Cut(lines, "-")
		if !isRange {
			last = first
		}
		from, errFrom := strconv.Atoi(first)
		to, errTo := strconv.Atoi(last)
		if file != "q/extra.go" || errFrom != nil || errTo != nil {
			t.Errorf("reported %q as reached by no seed; only q/extra.go is", span)
			continue
		}
		reported = reported || from <= ret && ret <= to
	}
	if !reported {
		t.Errorf("q/extra.go:%d, new and reached by no seed, is not reported (%q…); output:\n%s", ret, gapReport, out)
	}
	if left := r.git("status", "--porcelain", "--untracked-files=all"); left != "?? p/testdata/fuzz/FuzzSize/more\n?? q/extra.go" {
		t.Errorf("the run left files behind:\n%s", left)
	}
}

// TestSinceLeavesOutTestSupport: audit finding 261015-007a, "the long
// fuzz's gap report lists internal/testkit, test support that no fuzz target
// can reach, as unreached broker code". Code under the harness and under
// testkit is test tooling and never a gap, though no seed reaches it; one
// change in broker code no seed reaches is still listed, so the report is
// seen to run. The test names both directories itself rather than taking
// them from harness.TestSupportDirs (audit finding 261015-009a): a list
// that loses one would write nothing there and expect nothing of it, so the
// list is checked to hold both, and the support code is written under each.
func TestSinceLeavesOutTestSupport(t *testing.T) {
	t.Parallel()
	supportDirs := []string{"internal/harness/", "internal/testkit/"}
	for _, dir := range supportDirs {
		if !slices.Contains(harness.TestSupportDirs, dir) {
			t.Errorf("harness.TestSupportDirs is %q, without %q", harness.TestSupportDirs, dir)
		}
	}
	const support = `// Package support stands for test-support code, which no fuzz target runs.
package support

// Triple returns three times n; no seed reaches it.
func Triple(n int) int {
	return 3 * n
}
`
	r := newSinceRepo(t)
	base := r.git("rev-parse", "HEAD")
	for _, dir := range supportDirs {
		r.write(dir+"support/support.go", support)
	}
	r.edit("p/p.go", "n := 2 * len(b)", "n := 3 * len(b)")
	r.commit()
	code, out, _ := r.fuzzSince(base)
	if code != 0 {
		t.Errorf("exit %d; output:\n%s", code, out)
	}
	spans := gaps(out)
	for _, span := range spans {
		if slices.ContainsFunc(supportDirs, func(dir string) bool { return strings.HasPrefix(span, dir) }) {
			t.Errorf("reported %s, test support, as reached by no seed (%q…); output:\n%s", span, gapReport, out)
		}
	}
	spare := fmt.Sprintf("p/p.go:%d", lineOf(t, sinceFiles["p/p.go"], "n := 2 * len(b)"))
	if !slices.Contains(spans, spare) {
		t.Errorf("%s, changed and reached by no seed, is not reported (%q…); output:\n%s", spare, gapReport, out)
	}
}

// codecFile is a codec added to package p: encoders that write the module's
// own values and decoders that read bytes from outside. No seed reaches any
// of it.
const codecFile = `package p

// AppendPair appends the pair a, b to dst, after its width: an encoder.
func AppendPair(dst []byte, a, b byte) []byte {
	if !valid(a) {
		a = 0
	}
	dst = append(dst, byte(width(a, b)))
	return append(dst, a, b)
}

// width is how many bytes a pair takes: used by AppendPair alone.
func width(a, b byte) int {
	return 2
}

// valid reports whether c may be sent: used by AppendPair and DecodePair.
func valid(c byte) bool {
	return c != 0xff
}

// DecodePair reads a pair from src: a decoder.
func DecodePair(src []byte) (a, b byte, ok bool) {
	if len(src) < 3 || !valid(src[1]) {
		return 0, 0, false
	}
	return src[1], src[2], true
}

// appendKinds appends the kind of each byte of src to kinds: a decoder,
// though its name begins with append.
func appendKinds(kinds []string, src []byte) []string {
	for _, c := range src {
		kinds = append(kinds, letter(c))
	}
	return kinds
}
`

// TestSinceLeavesOutEncoders: audit report 261018-01, observation 4, "the
// long fuzz's gap report now lists the three encoder lines whose targets
// 0023a deleted, and will every round". The design fuzzes only "code that
// reads bytes or text from outside Sagüin" (design.md, Testing). One
// statement changes in each function of codecFile, and no seed reaches any:
// the encoder's and its own helper's are not listed; the decoder's, the
// helper it shares with the encoder, and the append-named function that
// reads input are.
func TestSinceLeavesOutEncoders(t *testing.T) {
	t.Parallel()
	r := newSinceRepo(t)
	r.write("p/codec.go", codecFile)
	r.commit()
	base := r.git("rev-parse", "HEAD")
	edits := []struct {
		from, to string
		listed   bool // the change is listed as reached by no seed
	}{
		{"return append(dst, a, b)", "return append(dst, b, a)", false},                      // AppendPair, an encoder
		{"return 2", "return 1 + 1", false},                                                  // width, its helper alone
		{"return c != 0xff", "return c < 0xff", true},                                        // valid, shared with a decoder
		{"return src[1], src[2], true", "return src[2], src[1], true", true},                 // DecodePair, a decoder
		{"kinds = append(kinds, letter(c))", "kinds = append(kinds, \"?\"+letter(c))", true}, // appendKinds, a decoder
	}
	for _, e := range edits {
		r.edit("p/codec.go", e.from, e.to)
	}
	r.commit()
	code, out, _ := r.fuzzSince(base)
	if code != 0 {
		t.Errorf("exit %d; output:\n%s", code, out)
	}
	var want []string
	for _, e := range edits {
		if e.listed {
			want = append(want, fmt.Sprintf("p/codec.go:%d", lineOf(t, codecFile, e.from)))
		}
	}
	slices.Sort(want)
	if spans := gaps(out); !slices.Equal(spans, want) {
		t.Errorf("reported %q as reached by no seed (%q…), want %q: the decoder's lines and never the encoder's; output:\n%s", spans, gapReport, want, out)
	}
}

// TestSinceUnknownCommitFails: a -since the repository does not know is a
// mistake to report, not an empty change set to pass on.
func TestSinceUnknownCommitFails(t *testing.T) {
	t.Parallel()
	r := newSinceRepo(t)
	code, out, got := r.fuzzSince("0123456789abcdef0123456789abcdef01234567")
	if code == 0 || len(got) != 0 {
		t.Errorf("exit %d, fuzzed %q; want a failure and nothing fuzzed; output:\n%s", code, got, out)
	}
}

// TestEmptySinceRunsEverything: -since given empty, as a Makefile passes an
// unset SINCE, runs every target, as without it.
func TestEmptySinceRunsEverything(t *testing.T) {
	code, out := fuzz(t, "-since=", "./pass")
	if code != 0 || !strings.Contains(out, "FuzzPasses for 20x") || !strings.Contains(out, "1 targets passed") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
}
