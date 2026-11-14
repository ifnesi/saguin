// Package rules holds the checks that keep the design's rules true: the size
// and dependency rule (R9) and, in 0001b, the goroutine and lock rules (R1,
// R3). It has no code of its own: it inspects the module around it, so a
// rule breaks a test, not a review.
package rules

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// moduleRoot returns the directory holding go.mod. A repository without one
// has no frame yet, which is itself a failure.
func moduleRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == os.DevNull {
		t.Fatal("no go.mod: the module does not exist yet")
	}
	return filepath.Dir(gomod)
}

// command is name with args, to run in dir with the environment env (nil:
// the test's own), within limit where it is above 0, and within the test's
// life: a command still running then is killed, and what it printed is
// taken as it stands 5 seconds later. Every command the rules run is made
// here; each caller runs it and reads its output as it needs.
func command(t *testing.T, dir string, env []string, limit time.Duration, name string, args ...string) *exec.Cmd {
	t.Helper()
	ctx := t.Context()
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		t.Cleanup(cancel)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir, cmd.Env, cmd.WaitDelay = dir, env, 5*time.Second
	return cmd
}

// run runs a command in dir and returns its standard output. A command that
// fails, or outlasts limit, fails the test with everything it printed.
func run(t *testing.T, dir string, limit time.Duration, name string, args ...string) string {
	t.Helper()
	cmd := command(t, dir, nil, limit, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %s: %v\n%s%s", name, strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String()
}
