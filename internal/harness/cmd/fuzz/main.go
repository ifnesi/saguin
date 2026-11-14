// Command fuzz runs every fuzz target in the module, one after another, each
// for -fuzztime, and stops at the first failure. With no target it says so
// and succeeds.
// Inputs found by fuzzing stay in Go's corpus; Go reports their replay path.
//
// It is a program rather than a test because `go test -fuzz` takes exactly
// one target per run: something has to list the targets and run each in
// turn. Run it under a memory bound: the fuzzing workers are the one part of
// the suite whose memory is not ours to decide.
//
//	go run ./internal/harness/cmd/fuzz [-fuzztime 30s] [-dir .] [-since commit] [packages]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// target is one fuzz function and the package that holds it.
type target struct{ pkg, name string }

// run is the whole command, apart from os.Exit, so tests can call it.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("fuzz", flag.ContinueOnError)
	flags.SetOutput(stderr)
	fuzztime := flags.String("fuzztime", "30s", "how long each target runs, as go test -fuzztime takes it: a duration, or Nx for N inputs")
	minimize := flags.String("fuzzminimizetime", "", "limit input minimization, as go test -fuzzminimizetime takes it; empty keeps Go's default")
	dir := flags.String("dir", ".", "the module to search and run in")
	since := flags.String("since", "", "fuzz targets whose seeds run functions changed since this Git commit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	patterns := flags.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	targets, err := list(*dir, patterns)
	if err != nil {
		harness.Say(stderr, "fuzz: listing targets: %v\n", err)
		return 1
	}
	if *since != "" {
		targets, err = selectSince(*dir, *since, targets, stdout)
		if err != nil {
			harness.Say(stderr, "fuzz: selecting targets changed since %s: %v\n", *since, err)
			return 1
		}
	}
	if len(targets) == 0 {
		if *since != "" {
			harness.Say(stdout, "fuzz: no targets selected since %s; nothing to run\n", *since)
		} else {
			harness.Say(stdout, "fuzz: no fuzz targets in %s; nothing to run\n", strings.Join(patterns, " "))
		}
		return 0
	}
	for _, t := range targets {
		harness.Say(stdout, "fuzz: %s %s for %s\n", t.pkg, t.name, *fuzztime)
		// -run=^$ skips the package's ordinary tests; they run elsewhere.
		testArgs := []string{"test", "-run=^$", "-fuzz=^" + t.name + "$", "-fuzztime=" + *fuzztime}
		if *minimize != "" {
			testArgs = append(testArgs, "-fuzzminimizetime="+*minimize)
		}
		cmd := exec.Command("go", append(testArgs, t.pkg)...)
		cmd.Dir = *dir
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Run(); err != nil {
			harness.Say(stdout, "fuzz: FAIL %s %s: %v; stopping\n", t.pkg, t.name, err)
			return 1
		}
	}
	harness.Say(stdout, "fuzz: %d targets passed\n", len(targets))
	return 0
}

// packageInfo is what go list says of a package: its non-test Go files are
// named relative to Dir.
type packageInfo struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
}

func changedPaths(dir, since string) ([]string, error) {
	cmd := exec.Command("git", "diff", "--no-renames", "--relative", "--name-only", "-z", since, "--", ".")
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git diff: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	paths := splitNUL(out)

	// git diff omits untracked files, but they are changes since every commit
	// and can introduce a new fuzzable package.
	cmd = exec.Command("git", "ls-files", "--others", "--exclude-standard", "-z", "--", ".")
	cmd.Dir = dir
	stderr.Reset()
	cmd.Stderr = &stderr
	out, err = cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return append(paths, splitNUL(out)...), nil
}

func splitNUL(data []byte) []string {
	var paths []string
	for field := range bytes.SplitSeq(data, []byte{0}) {
		if len(field) != 0 {
			paths = append(paths, string(field))
		}
	}
	return paths
}

func modulePackages(dir string) ([]packageInfo, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = dir
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list: %w\n%s%s", err, out.String(), stderr.String())
	}
	var packages []packageInfo
	dec := json.NewDecoder(&out)
	for dec.More() {
		var pkg packageInfo
		if err := dec.Decode(&pkg); err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
	}
	return packages, nil
}

// list asks go test for the fuzz functions in patterns, sorted by package and
// in source order within one. -json tags each listed name with its package;
// packages are listed concurrently, hence the sort.
func list(dir string, patterns []string) ([]target, error) {
	cmd := exec.Command("go", append([]string{"test", "-json", "-list", "^Fuzz"}, patterns...)...)
	cmd.Dir = dir
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w\n%s%s", err, out.String(), errOut.String())
	}
	var targets []target
	dec := json.NewDecoder(&out)
	for dec.More() {
		var ev struct{ Action, Package, Output string }
		if err := dec.Decode(&ev); err != nil {
			return nil, err
		}
		name := strings.TrimSpace(ev.Output)
		// -list prints one name per line among summary lines ("ok ...");
		// a name has no spaces.
		if ev.Action == "output" && strings.HasPrefix(name, "Fuzz") && !strings.ContainsAny(name, " \t") {
			targets = append(targets, target{pkg: ev.Package, name: name})
		}
	}
	sort.SliceStable(targets, func(i, j int) bool { return targets[i].pkg < targets[j].pkg })
	return targets, nil
}
