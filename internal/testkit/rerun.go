package testkit

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// RunAlone runs the calling test again, alone, in a run of the test binary
// of its own, and returns everything the run printed and how it ended: nil
// for an exit of 0, an *exec.ExitError for another, and an error wrapping
// context.DeadlineExceeded where it outlasted limit and was killed. The
// run's environment is this one's plus env, each "NAME=value", which tells
// the run it is the inner one; under, where given, is the command, with its
// arguments, that the binary runs under (bwrap and its options, say), the
// binary's path and flags following them. The run is given this one's own
// flags, those of the package rather than of go test (a seed to replay,
// say), so it makes the same test.
func RunAlone(t *testing.T, limit time.Duration, env []string, under ...string) ([]byte, error) {
	t.Helper()
	// -test.run matches each level of a test's name on its own, as a
	// pattern: each is anchored and quoted, so only this test runs.
	parts := strings.Split(t.Name(), "/")
	for i, part := range parts {
		parts[i] = "^" + regexp.QuoteMeta(part) + "$"
	}
	own := slices.DeleteFunc(slices.Clone(os.Args[1:]), func(arg string) bool { return strings.HasPrefix(arg, "-test.") })
	args := slices.Concat(under, []string{os.Args[0], "-test.run=" + strings.Join(parts, "/"), "-test.count=1", "-test.v"}, own)
	ctx, cancel := context.WithTimeout(t.Context(), limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	// A process the run started and left holding its output does not keep
	// the wait going past the kill.
	cmd.Env, cmd.WaitDelay = append(os.Environ(), env...), 5*time.Second
	out, err := cmd.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out, ctx.Err()
	}
	return out, err
}

// Rerun runs the calling test again, alone, as RunAlone does, and fails the
// test where that run fails, outlasts limit, or does not pass the test.
// what names the run in the messages ("the run including /dev/zero"); each
// carries everything the run printed.
func Rerun(t *testing.T, what string, limit time.Duration, env []string, under ...string) {
	t.Helper()
	out, err := RunAlone(t, limit, env, under...)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		t.Fatalf("%s did not end within %v:\n%s", what, limit, out)
	case err != nil:
		t.Fatalf("%s: %v\n%s", what, err, out)
	case !strings.Contains(string(out), "--- PASS: "+t.Name()+" "):
		t.Fatalf("%s did not pass %s:\n%s", what, t.Name(), out)
	}
}
