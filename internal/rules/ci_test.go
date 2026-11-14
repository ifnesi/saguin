package rules

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode"

	"github.com/ifnesi/saguin/internal/testkit"
)

// The checks here read commands, never text: a comment or an echo that
// mentions `make check` or -race runs nothing, so it must not pass.

// simpleCommands splits shell lines into simple commands, each a list of
// words, as the shell reads them: a # at the start of a word begins a comment
// that runs to the end of its line; a line ending in a backslash continues on
// the next; ;, &&, || and | separate commands; and a quote, ' or ", holds its
// text in one word, spaces and separators included, and is removed. So
// -ldflags="-linkmode=external -extldflags=-static" is one word. Escapes and
// expansions are not parsed; the commands checked here need none.
func simpleCommands(lines []string) [][]string {
	var cmds [][]string
	var cmd []string
	var word strings.Builder
	inWord := false // a word has begun, though it may still be empty ("")
	var quote rune  // the quote open, or 0
	endWord := func() {
		if inWord {
			cmd = append(cmd, word.String())
			word.Reset()
			inWord = false
		}
	}
	endCommand := func() {
		endWord()
		if len(cmd) > 0 {
			cmds = append(cmds, cmd)
			cmd = nil
		}
	}
	for _, line := range lines {
		rs := []rune(strings.TrimSpace(line))
		continued := false
	scan:
		for i := 0; i < len(rs); i++ {
			switch r := rs[i]; {
			case quote != 0:
				if r == quote {
					quote = 0
				} else {
					word.WriteRune(r)
				}
			case r == '"' || r == '\'':
				quote, inWord = r, true
			case r == '#' && !inWord:
				break scan
			case r == '\\' && i == len(rs)-1:
				continued = true
			case r == ';' || r == '|':
				endCommand()
			case r == '&' && i+1 < len(rs) && rs[i+1] == '&':
				endCommand()
				i++
			case unicode.IsSpace(r):
				endWord()
			default:
				word.WriteRune(r)
				inWord = true
			}
		}
		switch {
		case quote != 0:
			word.WriteByte(' ') // the quote goes on past the end of the line
		case continued:
			endWord()
		default:
			endCommand()
		}
	}
	endCommand()
	return cmds
}

// parts splits a simple command into its leading variable settings, a
// `systemd-run … --` prefix if it has one, and the program it runs with its
// arguments, so `echo make check` runs echo, not make.
func parts(cmd []string) (env, prefix, argv []string) {
	i := 0
	for i < len(cmd) && strings.Contains(cmd[i], "=") && !strings.HasPrefix(cmd[i], "-") {
		i++
	}
	env, argv = cmd[:i], cmd[i:]
	if len(argv) > 0 && argv[0] == "systemd-run" {
		if j := slices.Index(argv, "--"); j >= 0 {
			prefix, argv = argv[:j+1], argv[j+1:]
		}
	}
	return env, prefix, argv
}

// TestMakeCIRunsEverything: CI runs locally as `make ci`, before every
// commit. It must run all that `make check`, `make conformance`, `make
// fuzz`, `make soak` and `make e2efuzz` run (design, Rules: "Each rule has a
// test or CI check from the first commit"; conformance, fuzz and a short
// soak run on every change; item 0023d: the end-to-end fuzzer, 30 seconds in
// make ci). It compares the plans make prints, so it runs nothing.
func TestMakeCIRunsEverything(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	ci, plan := planIn(t, root, "ci")
	have := map[string]bool{}
	for _, c := range ci {
		have[strings.Join(c, " ")] = true
	}
	for _, target := range []string{"check", "conformance", "fuzz", "soak", "e2efuzz"} {
		cmds, _ := planIn(t, root, target)
		for _, c := range cmds {
			if !have[strings.Join(c, " ")] {
				t.Errorf("`make ci` does not run what `make %s` runs (%s); it would run:\n%s", target, strings.Join(c, " "), plan)
				break
			}
		}
	}
}

// TestMakeCheckUsesRaceDetector reads what `make check` and `make build`
// would run, without running them. The design: "the race detector runs on a
// native glibc build" (Testing). So `make check` runs a `go test` of every
// package with -race (every package, because the rules' own tests run only
// through it), and each such run is the native glibc build: cgo on, for
// without it no C library is linked at all; CC=gcc, the system's own C
// compiler, which links the system's glibc; and none of the static link's
// flags, which the shipped build's -ldflags hold (R9), so the race detector
// runs on the build the machine links natively, not on the shipped one.
func TestMakeCheckUsesRaceDetector(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	// linkerFlags returns the flags a go command's -ldflags give the linker,
	// one word each, with any package pattern before them (`all=`) dropped.
	// Given more than once, the last -ldflags is the one go uses.
	linkerFlags := func(argv []string) []string {
		given := flagValues(argv, "ldflags")
		if len(given) == 0 {
			return nil
		}
		v := given[len(given)-1]
		if !strings.HasPrefix(v, "-") {
			if _, rest, ok := strings.Cut(v, "="); ok {
				v = rest
			}
		}
		return strings.Fields(v)
	}
	builds, buildPlan := planIn(t, root, "build")
	var static []string
	for _, c := range builds {
		_, _, argv := parts(c)
		if len(argv) > 0 && argv[0] == "go" && slices.Contains(argv, "build") {
			static = append(static, linkerFlags(argv)...)
		}
	}
	if len(static) == 0 {
		t.Fatalf("`make build` gives the linker no -ldflags, which hold the static link's flags (R9); it would run:\n%s", buildPlan)
	}

	cmds, plan := planIn(t, root, "check")
	found := false
	for _, c := range cmds {
		env, _, argv := parts(c)
		if len(argv) < 2 || argv[0] != "go" || argv[1] != "test" {
			continue
		}
		race := slices.ContainsFunc(argv, func(w string) bool { return w == "-race" || strings.HasPrefix(w, "-race=") })
		if !race || !slices.Contains(argv, "./...") {
			continue
		}
		found = true
		if !slices.Contains(env, "CGO_ENABLED=1") || !slices.Contains(env, "CC=gcc") {
			t.Errorf("`make check` runs `go test -race ./...` with %q: the native glibc build needs CGO_ENABLED=1 and CC=gcc", env)
		}
		for _, f := range linkerFlags(argv) {
			if slices.Contains(static, f) {
				t.Errorf("`make check` runs `go test -race ./...` with the static link's flag %s: the race detector runs on the native glibc build", f)
			}
		}
	}
	if !found {
		t.Errorf("`make check` runs no `go test -race ./...`; it would run:\n%s", plan)
	}
}

// runners are the Makefile targets run on every change (conformance, fuzz
// and a short soak, from the first commit), the end-to-end fuzzer (item
// 0023d), and fuzz-full, the long fuzz of every audit round (the
// maintainer's decision), each with the harness package it must run (item
// 0001c) and whether it runs under the memory bound ("Long runs go under
// systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=0").
var runners = []struct {
	target string
	pkg    string
	bound  bool
}{
	{"conformance", "internal/harness/conformance", false},
	{"fuzz", "internal/harness/cmd/fuzz", true},
	{"soak", "internal/harness/cmd/soak", true},
	{"e2efuzz", "internal/harness/cmd/e2efuzz", true},
	{"fuzz-full", "internal/harness/cmd/fuzz", true},
}

// TestMakeHasRunners reads what `make conformance`, `make fuzz` and `make
// soak` would run, without running them: each must run its harness package
// with go (go test or go run), and fuzz and soak must do it inside a
// `systemd-run … --` scope with MemoryMax and MemorySwapMax=0. The plan is
// read with the Makefile's defaults; make -n starts nothing, so this needs no
// systemd.
func TestMakeHasRunners(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	for _, r := range runners {
		t.Run(r.target, func(t *testing.T) {
			t.Parallel()
			cmds, plan := planIn(t, root, r.target)
			for _, c := range cmds {
				_, prefix, argv := parts(c)
				if len(argv) == 0 || argv[0] != "go" || !slices.ContainsFunc(argv, func(w string) bool { return strings.Contains(w, r.pkg) }) {
					continue
				}
				if !r.bound {
					return
				}
				bounded := len(prefix) > 0 &&
					slices.ContainsFunc(prefix, func(w string) bool { return strings.HasPrefix(w, "MemoryMax=") }) &&
					slices.Contains(prefix, "MemorySwapMax=0")
				if bounded {
					return
				}
				t.Errorf("`make %s` runs %s outside the memory bound (systemd-run with MemoryMax and MemorySwapMax=0):\n%s", r.target, r.pkg, strings.Join(c, " "))
				return
			}
			t.Errorf("`make %s` does not run %s; it would run:\n%s", r.target, r.pkg, plan)
		})
	}
}

// The maintainer's decision: `make ci` runs before every commit, so its cost
// is capped. The whole run fails past a budget of 12 minutes, saying so
// (raised from 5 by the maintainer's decision of 261027, when the suite
// reached it with 0007b, and to 12 by the decision of 261031, when it
// reached 10 with QoS 2 and shared subscriptions); in it each fuzz target
// runs 5 seconds; and `make fuzz-full`, run in every audit round beside
// `make soak-full`, runs each selected fuzz target for 1 minute (the
// maintainer's decision of 261011, after a round of 17 targets took 85
// minutes at 5).
const (
	ciBudgetMax  = 720 // seconds, the most CI_BUDGET may be
	ciFuzzMax    = 5 * time.Second
	fullFuzzTime = 1 * time.Minute
	fuzzHarness  = "internal/harness/cmd/fuzz"
)

// ciFuzzMinimize caps Go's input minimisation in `make fuzz`, run by `make
// ci` (the maintainer's decision, after an audit found a target spending its
// whole 5 seconds minimising one large input).
const ciFuzzMinimize = time.Second

// makeEnv is the environment for a make these tests start: the caller's,
// less what a surrounding make passes down (a test run by `make check`
// inherits its MAKEFLAGS) and less the variables a caller could set to
// override the values under test.
func makeEnv() []string {
	all := os.Environ()
	env := make([]string, 0, len(all))
	for _, kv := range all {
		switch name, _, _ := strings.Cut(kv, "="); name {
		case "MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKELEVEL", "MAKEOVERRIDES", "FUZZTIME", "CI_BUDGET", "SINCE":
			continue
		}
		env = append(env, kv)
	}
	return env
}

// makeIn runs make with args in dir, within a minute and the test's life,
// and returns all it printed and how it ended.
func makeIn(t *testing.T, dir string, args ...string) (string, error) {
	out, err := command(t, dir, makeEnv(), time.Minute, "make", args...).CombinedOutput()
	return string(out), err
}

// planIn returns the simple commands `make -n target` would run in dir.
func planIn(t *testing.T, dir, target string) (cmds [][]string, text string) {
	t.Helper()
	out, err := makeIn(t, dir, "-n", target)
	if err != nil {
		t.Fatalf("make -n %s: %v\n%s", target, err, out)
	}
	return simpleCommands(strings.Split(out, "\n")), out
}

// harnessFlag returns the value of the flag called name given to each
// command in cmds that runs the fuzz harness with go; "" where it is not
// given.
func harnessFlag(cmds [][]string, name string) []string {
	var values []string
	for _, c := range cmds {
		_, _, argv := parts(c)
		if len(argv) == 0 || argv[0] != "go" || !slices.ContainsFunc(argv, func(w string) bool { return strings.Contains(w, fuzzHarness) }) {
			continue
		}
		value := ""
		if given := flagValues(argv, name); len(given) > 0 {
			value = given[len(given)-1]
		}
		values = append(values, value)
	}
	return values
}

// flagValues returns the values the flag called name is given in argv, a
// command's words, in their order: as -name=value, --name=value or -name
// value.
func flagValues(argv []string, name string) []string {
	var values []string
	for i, w := range argv {
		rest, ok := strings.CutPrefix(w, "-")
		if !ok {
			continue
		}
		flag, v, hasValue := strings.Cut(rest, "=")
		if flag != "-"+name && flag != name {
			continue
		}
		if !hasValue {
			if i+1 >= len(argv) {
				continue
			}
			v = argv[i+1]
		}
		values = append(values, v)
	}
	return values
}

// TestMakeCIFuzzTime reads what `make ci` would run: the fuzz harness, with
// each target given at most 5 seconds (the maintainer's decision). The
// harness's own default is 30 seconds, so the time must be given.
func TestMakeCIFuzzTime(t *testing.T) {
	t.Parallel()
	cmds, plan := planIn(t, moduleRoot(t), "ci")
	times := harnessFlag(cmds, "fuzztime")
	if len(times) == 0 {
		t.Fatalf("`make ci` does not run the fuzz harness; it would run:\n%s", plan)
	}
	for _, v := range times {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > ciFuzzMax {
			t.Errorf("`make ci` fuzzes each target for %q; want a duration of at most %v", v, ciFuzzMax)
		}
	}
}

// TestMakeFuzzFull reads what `make fuzz-full` would run: the fuzz harness,
// each target for 1 minute (the maintainer's decision, and of 261011).
// TestMakeHasRunners checks it runs under the memory bound.
func TestMakeFuzzFull(t *testing.T) {
	t.Parallel()
	cmds, plan := planIn(t, moduleRoot(t), "fuzz-full")
	times := harnessFlag(cmds, "fuzztime")
	if len(times) == 0 {
		t.Fatalf("`make fuzz-full` does not run the fuzz harness; it would run:\n%s", plan)
	}
	for _, v := range times {
		if d, err := time.ParseDuration(v); err != nil || d != fullFuzzTime {
			t.Errorf("`make fuzz-full` fuzzes each target for %q; want %v", v, fullFuzzTime)
		}
	}
}

// TestMakeFuzzMinimizeTime reads what `make fuzz` and `make fuzz-full` would
// run: the fuzz harness, given -fuzzminimizetime of ciFuzzMinimize in `make
// fuzz`, and none in `make fuzz-full`, which keeps Go's default.
func TestMakeFuzzMinimizeTime(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	for _, c := range []struct {
		target string
		want   time.Duration // 0: not given
	}{
		{"fuzz", ciFuzzMinimize},
		{"fuzz-full", 0},
	} {
		cmds, plan := planIn(t, root, c.target)
		limits := harnessFlag(cmds, "fuzzminimizetime")
		if len(limits) == 0 {
			t.Errorf("`make %s` does not run the fuzz harness; it would run:\n%s", c.target, plan)
		}
		for _, v := range limits {
			switch d, err := time.ParseDuration(v); {
			case c.want == 0 && v != "":
				t.Errorf("`make %s` caps minimisation at %q; want Go's default, no -fuzzminimizetime", c.target, v)
			case c.want != 0 && (err != nil || d != c.want):
				t.Errorf("`make %s` gives the fuzz harness -fuzzminimizetime %q; want %v", c.target, v, c.want)
			}
		}
	}
}

// standInSleep is how long each of standIn's five steps sleeps, in seconds:
// under the 1-second budget alone, over it all five together.
const standInSleep = "0.3"

// standIn is a throwaway Makefile: the real one, with the five steps of
// `make ci` replaced by stand-ins that each sleep standInSleep, the build
// they depend on by one that does nothing (standInBuild), and a target that
// prints the budget. make warns that the recipes are overridden; the later
// recipe is the one it runs.
const standIn = `include %s

check conformance fuzz soak e2efuzz:
	sleep ` + standInSleep + `
` + standInBuild + `

standin-budget:
	@echo 'CI_BUDGET=$(CI_BUDGET)'
`

// standInBuild replaces the real build in a stand-in Makefile. check,
// conformance, soak and e2efuzz depend on build, and a stand-in's recipe
// replaces a step's recipe, never its prerequisites: without it, a stand-in
// `make ci` builds the broker for real, into the repository's bin/saguin,
// before its first step, and its budget then times that build, which takes
// as long as the machine's load makes it, or fails where the broker does
// not compile. design.md, Testing: "A deadline test controls or observes
// the event starting that deadline"; the budget starts with the ci recipe,
// and with this the stand-in's first step follows it with nothing heavy
// between them.
const standInBuild = `
build:
	true
`

// heavy reports whether a simple command does the work of `make ci`'s real
// steps: go build, go test or go vet (go -C <dir> included), a step's
// harness, the linter or the formatter, or anything under the memory bound.
func heavy(c []string) bool {
	_, prefix, argv := parts(c)
	if len(prefix) > 0 {
		return true
	}
	if len(argv) > 1 && argv[0] == "go" {
		sub := argv[1:]
		if len(sub) > 2 && sub[0] == "-C" {
			sub = sub[2:]
		}
		if slices.Contains([]string{"build", "test", "vet"}, sub[0]) {
			return true
		}
	}
	return slices.ContainsFunc(c, func(w string) bool {
		for _, s := range []string{fuzzHarness, "internal/harness/cmd/soak", "internal/harness/cmd/e2efuzz", "internal/harness/conformance", "golangci-lint", "gofmt"} {
			if strings.Contains(w, s) {
				return true
			}
		}
		return false
	})
}

// writeStandIn writes makefile, a throwaway Makefile whose first line
// includes the real one ("include %s"), into a new folder under TMPDIR (the
// repository's .tmp/ when run through make), with the .tmp/ the real one
// sets as TMPDIR made in advance, so make itself never falls back to /tmp.
// Before anything runs there, it checks that `make ci` would run nothing
// heavy: a recursive make that escaped the stand-ins would run the whole
// suite, this test included. It returns the folder and the plan.
func writeStandIn(t *testing.T, root, makefile string) (dir string, cmds [][]string, plan string) {
	t.Helper()
	dir = t.TempDir()
	makefile = strings.Replace(makefile, "%s", filepath.Join(root, "Makefile"), 1)
	if err := os.WriteFile(filepath.Join(dir, "Makefile"), []byte(makefile), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ".tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmds, plan = planIn(t, dir, "ci")
	for _, c := range cmds {
		if heavy(c) {
			t.Fatalf("`make ci` with its steps replaced would still run %q: it must run its steps as targets of its own Makefile. Not running it; it would run:\n%s", strings.Join(c, " "), plan)
		}
	}
	return dir, cmds, plan
}

// standInDir writes standIn and returns its folder, once it has checked that
// `make ci` would run the five stand-ins.
func standInDir(t *testing.T, root string) string {
	t.Helper()
	dir, cmds, plan := writeStandIn(t, root, standIn)
	sleeps := 0
	for _, c := range cmds {
		if _, _, argv := parts(c); slices.Equal(argv, []string{"sleep", standInSleep}) {
			sleeps++
		}
	}
	if sleeps != 5 {
		t.Fatalf("`make ci` with its steps replaced runs %d of the 5 stand-ins; it would run:\n%s", sleeps, plan)
	}
	return dir
}

// TestMakeCIBudget checks the budget on `make ci` (the maintainer's
// decision): CI_BUDGET, in seconds, is at most 300 by default; and the
// mechanism works, run for real on stand-in steps that take about 1.5
// seconds in all. Given a budget of 1 second, `make ci` fails and says it is
// over budget; given 60, the same run passes. Each stand-in alone takes
// under a second, so only a budget on the whole run catches the overrun.
func TestMakeCIBudget(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	dir := standInDir(t, root)

	out, err := makeIn(t, dir, "-s", "--no-print-directory", "standin-budget")
	if err != nil {
		t.Fatalf("make standin-budget: %v\n%s", err, out)
	}
	// make also prints warnings about the overridden recipes; the value is on
	// its own line.
	value := ""
	for line := range strings.SplitSeq(out, "\n") {
		if v, ok := strings.CutPrefix(line, "CI_BUDGET="); ok {
			value = strings.TrimSpace(v)
		}
	}
	if budget, err := strconv.Atoi(value); err != nil || budget <= 0 || budget > ciBudgetMax {
		t.Errorf("CI_BUDGET defaults to %q; want whole seconds, at most %d", value, ciBudgetMax)
	}

	t.Run("overrun fails", func(t *testing.T) {
		t.Parallel()
		dir := standInDir(t, root)
		out, err := makeIn(t, dir, "ci", "CI_BUDGET=1")
		// The folder's name holds the test's, budget included: only the
		// words around it can say so.
		said := strings.Contains(strings.ToLower(strings.ReplaceAll(out, dir, "")), "budget")
		if err == nil {
			t.Errorf("`make ci CI_BUDGET=1` took about 1.5 s and passed; want it to fail over its budget:\n%s", out)
		} else if !said {
			t.Errorf("`make ci CI_BUDGET=1` failed (%v) without saying it is over budget:\n%s", err, out)
		}
	})
	t.Run("within budget passes", func(t *testing.T) {
		t.Parallel()
		if out, err := makeIn(t, standInDir(t, root), "ci", "CI_BUDGET=60"); err != nil {
			t.Errorf("`make ci CI_BUDGET=60` took about 1.5 s and failed: %v\n%s", err, out)
		}
	})
}

// lingerStandIn is a throwaway Makefile whose check step starts a program
// that outlives its parent, as `go run` starts the fuzz and soak harnesses:
// a shell backgrounds a sleep and waits for it (finding 261010-006a's rig).
// A shell without job control starts a background job with SIGINT ignored
// (POSIX, Shell Command Language 2.11), so neither a signal to the step nor
// the terminal's Ctrl-C ends the sleep: only an end that reaches every
// process `make ci` started does. The sleep's length is the mark that finds
// it. Once the sleep has started, the shell writes its process id to the
// file lingerStarted, in make's folder. The other steps do nothing, and so
// does the build (standInBuild).
const lingerStandIn = `include %s

check:
	sh -c 'sleep MARK & echo $$! > ` + lingerStarted + `; wait'

conformance fuzz soak e2efuzz:
	true
` + standInBuild

// lingerStarted is the file lingerStandIn's shell writes once its sleep has
// started.
const lingerStarted = "stand-in.started"

// lingerDir writes lingerStandIn with mark, a number unique to one run, and
// returns its folder.
func lingerDir(t *testing.T, root, mark string) string {
	t.Helper()
	dir, _, plan := writeStandIn(t, root, strings.Replace(lingerStandIn, "MARK", mark, 1))
	if !strings.Contains(plan, "sleep "+mark) {
		t.Fatalf("`make ci` with its steps replaced does not run the stand-in; it would run:\n%s", plan)
	}
	return dir
}

// newMark returns a sleep length no other process on the machine has: 31
// seconds, longer than any wait here, and a fraction unique to this run.
func newMark(i int) string {
	return fmt.Sprintf("31.%06d%d", time.Now().UnixNano()%1_000_000, i)
}

// overBudgetMax is the largest CI_BUDGET, in seconds, that
// TestMakeCILeavesNothingRunning runs over: its stand-in's sleep (newMark)
// outlives that budget, the supervisor's half-second grace and survivors'
// wait, so a survivor is still there to find.
const overBudgetMax = 16

// processesWith returns the live processes any of whose arguments holds
// mark: the stand-in's sleep and the shells around it. A process that has
// ended, a zombie included, has no arguments left to read.
func processesWith(mark string) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		args, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err == nil && bytes.Contains(args, []byte(mark)) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// startMake starts make with args in dir, in a process group of its own, as
// a shell starts a job, its output to a file: a pipe would keep the wait
// open as long as any survivor holds it. It returns the command and a
// channel that is closed when make has returned. Cleanup ends whatever is
// left: make's group, then every process carrying mark.
func startMake(t *testing.T, dir, mark string, args ...string) (started *exec.Cmd, returned <-chan struct{}) {
	t.Helper()
	log, err := os.Create(filepath.Join(dir, "make.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("make", args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = dir, makeEnv(), log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		for _, pid := range processesWith(mark) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		<-done
		_ = log.Close()
	})
	return cmd, done
}

// survivors fails the test if any process carrying mark is still alive a
// short while after make has returned, naming them and make's last words.
func survivors(t *testing.T, dir, mark, how string) {
	t.Helper()
	if testkit.Within(3*time.Second, func() bool { return len(processesWith(mark)) == 0 }) {
		return
	}
	var left []string
	for _, pid := range processesWith(mark) {
		args, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
		left = append(left, fmt.Sprintf("%d %s", pid, bytes.ReplaceAll(args, []byte{0}, []byte{' '})))
	}
	out, _ := os.ReadFile(filepath.Join(dir, "make.log"))
	t.Errorf("`make ci` returned %s, and what it started is still running:\n%s\nmake said:\n%s", how, strings.Join(left, "\n"), out)
}

// TestMakeCILeavesNothingRunning: when `make ci` returns, nothing it started
// is still running, whether it ended over its budget or at a Ctrl-C (the
// maintainer's decision after audit round 261010-07; finding 261010-006a).
// The check step's stand-in outlives its parent and ignores SIGINT
// (lingerStandIn). Ctrl-C is SIGINT to make's process group, as a terminal
// sends it to its foreground job; make must then return within 5 seconds,
// not run on to the budget.
//
// The budget runs from the ci recipe's start, and the stand-in starts only
// once the supervisor has been built and has started make (design.md,
// Testing: "A deadline test controls or observes the event starting that
// deadline"): a budget that ends first leaves nothing running to find. Each
// run over budget is checked for survivors, then for its stand-in having
// started (lingerStarted); one whose stand-in never started tested nothing,
// and is run again with twice the budget, up to overBudgetMax.
func TestMakeCILeavesNothingRunning(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	t.Run("over budget", func(t *testing.T) {
		t.Parallel()
		for budget := 1; ; budget *= 2 {
			mark := newMark(1)
			dir := lingerDir(t, root, mark)
			_, done := startMake(t, dir, mark, "ci", fmt.Sprintf("CI_BUDGET=%d", budget))
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("watchdog: `make ci CI_BUDGET=%d` still running after 30 s", budget)
			}
			survivors(t, dir, mark, "over its budget")
			if _, err := os.Stat(filepath.Join(dir, lingerStarted)); err == nil {
				return
			}
			out, _ := os.ReadFile(filepath.Join(dir, "make.log"))
			if budget >= overBudgetMax {
				t.Fatalf("test premise broken: the stand-in never started within CI_BUDGET=%d, nor any budget before it from 1 s; make said:\n%s", budget, out)
			}
			t.Logf("the stand-in never started within CI_BUDGET=%d, which tested nothing; trying twice the budget. make said:\n%s", budget, out)
		}
	})
	t.Run("Ctrl-C", func(t *testing.T) {
		t.Parallel()
		mark := newMark(2)
		dir := lingerDir(t, root, mark)
		cmd, done := startMake(t, dir, mark, "ci", "CI_BUDGET=120")
		if !testkit.Within(10*time.Second, func() bool { return len(processesWith(mark)) > 0 }) {
			t.Fatal("the stand-in never started")
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGINT); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			out, _ := os.ReadFile(filepath.Join(dir, "make.log"))
			t.Fatalf("`make ci` still running 5 s after Ctrl-C; make said:\n%s", out)
		}
		survivors(t, dir, mark, "after Ctrl-C")
	})
}

// TestMakeFuzzFullSince reads what `make fuzz-full SINCE=<commit>` would
// run: the fuzz harness, 1 minute a target, given the commit, to fuzz only
// what changed since it (the maintainer's decision after audit round
// 261010-07; the harness's own tests pin what it selects). Without SINCE,
// fuzz-full fuzzes every target; and `make fuzz`, as in `make ci`, fuzzes
// every target whatever SINCE says.
func TestMakeFuzzFullSince(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	const commit = "0123abcd"
	cases := []struct {
		args  []string
		since string // the -since the harness must get; "" is every target
	}{
		{[]string{"fuzz-full", "SINCE=" + commit}, commit},
		{[]string{"fuzz-full"}, ""},
		{[]string{"fuzz", "SINCE=" + commit}, ""},
		{[]string{"ci", "SINCE=" + commit}, ""},
	}
	for _, c := range cases {
		out, err := makeIn(t, root, append([]string{"-n"}, c.args...)...)
		if err != nil {
			t.Errorf("make -n %s: %v\n%s", strings.Join(c.args, " "), err, out)
			continue
		}
		cmds := simpleCommands(strings.Split(out, "\n"))
		since := harnessFlag(cmds, "since")
		if len(since) == 0 {
			t.Errorf("`make %s` does not run the fuzz harness; it would run:\n%s", strings.Join(c.args, " "), out)
		}
		for _, v := range since {
			if v != c.since {
				t.Errorf("`make %s` gives the fuzz harness -since %q; want %q", strings.Join(c.args, " "), v, c.since)
			}
		}
		if c.args[0] == "fuzz-full" {
			for _, v := range harnessFlag(cmds, "fuzztime") {
				if d, err := time.ParseDuration(v); err != nil || d != fullFuzzTime {
					t.Errorf("`make %s` fuzzes each target for %q; want %v", strings.Join(c.args, " "), v, fullFuzzTime)
				}
			}
		}
	}
}
