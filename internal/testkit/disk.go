package testkit

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// OnSmallDisk runs the calling test again, alone, as Rerun does, with a
// tmpfs of size bytes mounted on a new directory of the test's, which the
// run finds in its environment variable env: the test fills that
// filesystem, never the machine's disk (OnSmallDisks).
func OnSmallDisk(t *testing.T, env string, size int, limit time.Duration) {
	t.Helper()
	OnSmallDisks(t, limit, SmallDisk{Env: env, Size: size})
}

// SmallDisk is one filesystem OnSmallDisks mounts: a tmpfs of Size bytes,
// whose directory the run finds in its environment variable Env.
type SmallDisk struct {
	Env  string
	Size int
}

// OnSmallDisks runs the calling test again, alone, as Rerun does, with each
// of disks mounted on a new directory of the test's. bwrap (bubblewrap)
// gives the run a private user and mount namespace, with no privilege: the
// mounts are the run's alone, go with it, and are seen by every process the
// run starts.
func OnSmallDisks(t *testing.T, limit time.Duration, disks ...SmallDisk) {
	t.Helper()
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		t.Fatalf("a full disk is tested on a small filesystem mounted by bubblewrap: %v", err)
	}
	under := make([]string, 0, 4+4*len(disks)+2)
	under = append(under, bwrap, "--dev-bind", "/", "/")
	env := make([]string, 0, len(disks))
	sizes := make([]string, 0, len(disks))
	for _, d := range disks {
		dir := t.TempDir()
		under = append(under, "--size", strconv.Itoa(d.Size), "--tmpfs", dir)
		env = append(env, d.Env+"="+dir)
		sizes = append(sizes, strconv.Itoa(d.Size))
	}
	Rerun(t, fmt.Sprintf("the run on a %s-byte filesystem", strings.Join(sizes, "-byte and a ")), limit, env,
		append(under, "--die-with-parent", "--")...)
}

// FillDisk writes a file at path until its filesystem has no room left,
// and returns it open: truncating it gives the room back.
func FillDisk(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	block := make([]byte, 4096)
	for {
		if _, err := f.Write(block); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			return f
		}
	}
}

// failingCalls are the system calls each kind FailingIO and SlowIO take
// names, as strace names them.
var failingCalls = map[string]string{
	"sync":  "fsync,fdatasync",
	"write": "pwrite64",
	"read":  "pread64",
}

// FailingIO returns the command, with its arguments, that a program runs
// under (as Launch's or Rerun's under) so that every call of kind ("sync":
// fsync and fdatasync; "write": pwrite64) on the file at path fails with
// errno ("ENOSPC": no room on the device; "EDQUOT": a disk quota met;
// "EIO": an I/O error, not for room), each failed call noted in the file
// trace (Injected). The command is strace, which fails the calls of the
// program and every thread and process it starts. A call's file is named
// when the call is made, from the descriptor: a file the program opened
// under another name fails once a rename gives it path, and stops failing
// once it is renamed back, so a test turns the failure on and off by
// renaming the file's directory. A test that kills strace kills the program
// with it.
func FailingIO(t *testing.T, kind, errno, path, trace string) []string {
	t.Helper()
	command, err := injectedIO(kind, "error="+errno, path, trace)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

// SlowIO returns the command, with its arguments, that a program runs
// under so that every call of kind on the file at path, as FailingIO names
// them or "read" (pread64), waits delay before it is made, then succeeds as
// it would have;
// each delayed call is noted in the file trace (Injected). Only the thread
// making the call waits: the program's other threads go on. The file is
// named when the call is made, as for FailingIO.
func SlowIO(t *testing.T, kind string, delay time.Duration, path, trace string) []string {
	t.Helper()
	command, err := SlowIOCommand(kind, delay, path, trace)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

// SlowIOCommand builds SlowIO's command for callers outside a test. It
// returns an error if strace cannot be found or kind is unknown.
func SlowIOCommand(kind string, delay time.Duration, path, trace string) ([]string, error) {
	if delay < 0 {
		return nil, errors.New("I/O delay must be at least 0")
	}
	return injectedIO(kind, fmt.Sprintf("delay_enter=%dns", delay.Nanoseconds()), path, trace)
}

// injectedIO is FailingIO and SlowIO: strace, injecting into every call of
// kind on the file at path what inject says, in strace's --inject terms.
func injectedIO(kind, inject, path, trace string) ([]string, error) {
	calls, ok := failingCalls[kind]
	if !ok {
		return nil, fmt.Errorf("no injected call of kind %q", kind)
	}
	return straced(trace, "--trace="+calls, "--inject="+calls+":"+inject, "--trace-path="+path)
}

// straced is strace's command, with its arguments, noting in the file trace
// the calls of the program and of every thread and process it starts that
// options select, then "--": the program and its flags follow. A test that
// kills strace kills the program with it.
func straced(trace string, options ...string) ([]string, error) {
	strace, err := exec.LookPath("strace")
	if err != nil {
		return nil, fmt.Errorf("system calls are traced by strace: %w", err)
	}
	return slices.Concat([]string{strace, "--follow-forks", "--quiet=all", "--seccomp-bpf", "--kill-on-exit", "--output=" + trace},
		options, []string{"--"}), nil
}

// TracedCalls returns the command, with its arguments, that a program runs
// under (as Launch's or Rerun's under) so that every call it makes of
// calls, system calls as strace names them, is noted in the file trace in
// the order made, on any of its threads; Traced reads them back.
func TracedCalls(t *testing.T, trace string, calls ...string) []string {
	t.Helper()
	command, err := straced(trace, "--trace="+strings.Join(calls, ","))
	if err != nil {
		t.Fatal(err)
	}
	return command
}

// Traced returns the calls the trace file TracedCalls named notes, one
// line each in the order they were made, each as strace writes it: the
// thread's id, the call's name and its arguments (a call another thread
// interrupted is noted where it began, "<unfinished ...>", and again where
// it ended, "<... name resumed>").
func Traced(t *testing.T, trace string) []string {
	t.Helper()
	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// Injected reports whether the trace file FailingIO or SlowIO named notes
// a call it failed or delayed.
func Injected(t *testing.T, trace string) bool {
	t.Helper()
	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(b), "(INJECTED)") || strings.Contains(string(b), "(DELAYED)")
}
