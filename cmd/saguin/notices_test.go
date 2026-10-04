package main

// The third rule about the whole binary, beside the doc-comment and
// log-bounds ones and for the same reason: it is about what ships rather
// than about any package.

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// engineNotice is the module-list line for the MQTT engine, which `go list`
// cannot see: it used to be a module and is now a stripped copy under
// `internal/mqtt`, reported as saguin's own code and filtered out with it.
//
// It is named here rather than skipped by pattern so that the two checks
// below can be about it: that the line is present at all, and that the
// licence underneath it is the one sitting beside the code.
const engineNotice = "github.com/mochi-mqtt/server/v2 v2.7.9 (stripped into internal/mqtt)"

// enginePath is the engine's directory, relative to this package.
const enginePath = "../../internal/mqtt"

// The attribution that ships must name every module that ships.
//
// saguin is one binary and a recipient has no go.mod, so `saguin
// --licenses` is the only place they can learn whose code is inside. A
// module added next month reaches the binary the moment it is imported and
// reaches this file only when somebody runs `make notices` - and nothing
// about a missing paragraph looks wrong. That is the same silence as a
// counter reading zero.
//
// **It asks the embedded string rather than the file on disk**, because the
// file is what the repository holds and the string is what the binary
// carries, and it is the second one a recipient gets.
//
// The set is `go list -deps ./cmd/saguin`: the packages that reach the
// binary. Not go.mod, which names two modules that never do, and not `go
// list -m all`, which drags in dependencies' test dependencies - attribution
// for code that is not there is its own kind of wrong.
//
// **Asked of every platform Go builds for, not of this one**, and that is
// the difference between a check and a check that is green on one machine.
// The set is not the same everywhere: `modernc.org/sqlite` reaches
// `github.com/mattn/go-isatty` and `github.com/ncruces/go-strftime` on
// Darwin, Windows and the BSDs and not on Linux. So a file generated on a
// Mac names two modules a Linux build does not contain, and one generated on
// Linux leaves two out of a Mac build - the second being the wrong that
// matters, because a recipient is then not told whose code they are running.
// This failed exactly that way: regenerated on a Mac, red on Linux, which is
// CI.
//
// One file ships from one source tree, so what it must name is the **union**
// - every module that can reach a saguin binary anywhere. `go tool dist list`
// is where the platform list comes from rather than a list written here, so
// there is nothing to keep in step with `make notices`, which takes the same
// union the same way. It costs about five seconds and it is the whole reason
// the answer is trustworthy.
func TestTheNoticesCoverEveryModuleInTheBinary(t *testing.T) {
	listed := map[string]bool{}
	head, _, ok := strings.Cut(notices, "<!-- end of module list -->")
	if !ok {
		t.Fatal("the embedded notices carry no module list; `make notices` writes one, " +
			"and without it this check has nothing to compare")
	}
	engineListed := false
	for _, line := range strings.Split(head, "\n") {
		if !strings.HasPrefix(line, "- ") {
			continue
		}
		entry := strings.TrimPrefix(line, "- ")
		// The engine is in the binary and not in any module set, so it
		// belongs in neither direction of the comparison below. Taken out
		// here rather than tolerated there, because "in the notices and not
		// in the binary" is a real failure for everything else.
		if entry == engineNotice {
			engineListed = true
			continue
		}
		listed[entry] = true
	}
	if !engineListed {
		t.Errorf("the module list does not name the MQTT engine (%q): its code is in the "+
			"binary and `go list` cannot see it, so nothing else would notice.\n\t"+
			"Run `make notices`.", engineNotice)
	}

	// The version travels with the path, and a replacement travels with
	// both. **A fork revision is what moves without the module list
	// changing**, and it moved once with this check reporting ok: the
	// notices went on naming the commit before the one in the binary, which
	// is attribution for code that is not there - the failure this exists to
	// stop, arrived at from the one direction a name comparison cannot see.
	platforms, err := exec.Command("go", "tool", "dist", "list").Output()
	if err != nil {
		t.Fatalf("asking the toolchain which platforms it builds for: %v", err)
	}

	// Which platform each module was first seen on, so a failure says why a
	// module nobody on this machine can see belongs in the file.
	inBinary := map[string]string{}
	answered := map[string]bool{}
	for _, target := range strings.Fields(string(platforms)) {
		goos, goarch, ok := strings.Cut(target, "/")
		if !ok {
			continue
		}
		cmd := exec.Command("go", "list", "-deps", "-f",
			"{{if .Module}}{{.Module.Path}} {{.Module.Version}}"+
				"{{with .Module.Replace}} ({{.Path}}@{{.Version}}){{end}}{{end}}", "./")
		cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)
		out, err := cmd.Output()
		if err != nil {
			// A platform saguin does not build for contributes nothing, and
			// is not a failure: js/wasm has no sockets, and the binary that
			// cannot exist there ships no attribution either.
			continue
		}
		answered[target] = true
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			line = strings.TrimSpace(line)
			// The main module has no version of its own here, so it is
			// recognised by its path rather than by the whole line.
			if path, _, _ := strings.Cut(line, " "); line == "" || path == "github.com/ifnesi/saguin" {
				continue // saguin's own code, which its own licence covers
			}
			if _, seen := inBinary[line]; !seen {
				inBinary[line] = target
			}
		}
	}
	// **A check whose subject is every platform has to prove it asked them**,
	// and the two to name are the two that disagree: the defect this exists
	// to catch is a file generated where `modernc.org/sqlite` reaches two
	// modules and checked where it does not, so a union that skipped either
	// end of that is a union of one answer. A count alone would not say it -
	// twenty-five of the toolchain's platforms build saguin today, and which
	// twenty-five is the toolchain's business rather than this file's.
	for _, target := range []string{"linux/amd64", "darwin/arm64"} {
		if !answered[target] {
			t.Fatalf("%s never answered, so this is not a union - it is one platform's "+
				"module set, which is the failure this check exists to stop", target)
		}
	}
	if len(answered) < 10 {
		t.Fatalf("only %d of the toolchain's platforms answered, which is too few to call "+
			"this a union - the module set was taken from almost nowhere", len(answered))
	}

	for what, where := range inBinary {
		if !listed[what] {
			t.Errorf("%s is in the binary on %s and not in the notices: a recipient of "+
				"that binary is not told whose code they are running.\n\tRun `make notices`.",
				what, where)
		}
	}
	for what := range listed {
		if _, ok := inBinary[what]; !ok {
			t.Errorf("%s is in the notices and no longer in the binary: attribution for "+
				"code that is not there.\n\tRun `make notices`.", what)
		}
	}

	// A check that silently compared nothing is worse than none.
	t.Logf("%d modules across %d platforms, %d in the notices",
		len(inBinary), len(answered), len(listed))
	if len(inBinary) < 10 {
		t.Fatalf("only %d modules reported as reaching the binary, which is too few to "+
			"believe - this check is no longer asking about the binary it thinks it is",
			len(inBinary))
	}
}

// The engine's licence must ship, and must be the one beside its code.
//
// `go list` reports `internal/mqtt` as saguin's own package, so the check
// above cannot ask about it and `make notices` cannot generate it from the
// module cache. Both take it from `internal/mqtt/LICENSE.md` instead, which
// moves the question from "did the generator find it" to "is the file still
// there and still what shipped" - and that is what this asks.
//
// MIT requires the copyright and permission notice to travel with the
// software. The software is this binary.
func TestTheNoticesCarryTheInTreeEngineLicence(t *testing.T) {
	licence, err := os.ReadFile(filepath.Join(enginePath, "LICENSE.md"))
	if err != nil {
		t.Fatalf("reading the engine's licence: %v\n\tIt sits beside the code it covers, "+
			"and `make notices` copies it into what ships.", err)
	}

	// A truncated or emptied file would otherwise pass the containment check
	// below against an equally truncated notices file.
	if n := len(licence); n < 500 {
		t.Fatalf("the engine's licence is %d bytes, which is too short to be the MIT "+
			"text - this check would then be comparing nothing to nothing", n)
	}
	if !strings.Contains(string(licence), "Permission is hereby granted") {
		t.Fatal("the engine's licence does not read as the MIT text, so what ships " +
			"under its name is something else")
	}

	if !strings.Contains(notices, string(licence)) {
		t.Errorf("the licence in %s is not in the notices the binary carries: a recipient "+
			"of that binary gets the engine's code without the notice MIT asks to travel "+
			"with it.\n\tRun `make notices`.", enginePath)
	}

	// **And the entry must not outlive the code.** Attribution for code that
	// is not there is the other half of the failure this file exists for, and
	// a licence copied into the notices would keep shipping unchallenged after
	// the engine left the tree.
	engineFiles := 0
	err = filepath.WalkDir(enginePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".go") {
			engineFiles++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", enginePath, err)
	}
	if engineFiles < 10 {
		t.Fatalf("%s holds %d Go files, which is too few to be the engine: the notices "+
			"name it and the binary may no longer carry it", enginePath, engineFiles)
	}
	t.Logf("%d Go files under %s, licence %d bytes", engineFiles, enginePath, len(licence))
}
