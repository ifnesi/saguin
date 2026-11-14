package testkit

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Output says where RunCommand puts what the command prints.
type Output int

const (
	// Separate keeps standard output and standard error apart.
	Separate Output = iota
	// Combined writes both into one stream, in the order printed, returned
	// as stdout.
	Combined
)

// RunCommand runs a command's whole body apart from os.Exit, run(args,
// stdout, stderr), and returns its exit code and what it printed.
func RunCommand(run func(args []string, stdout, stderr io.Writer) int, output Output, args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	if output == Combined {
		code = run(args, &out, &out)
		return code, out.String(), ""
	}
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// WriteFile writes content to the file name, a slash-separated path under
// dir whose directories it makes, and returns the file's path. The file is
// the owner's alone (0600), as a configuration may require.
func WriteFile(t testing.TB, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// FileSize returns the size of the file at path, failing the test where
// there is none.
func FileSize(t testing.TB, path string) int64 {
	t.Helper()
	size, ok := sizeOf(t, path)
	if !ok {
		t.Fatalf("%s: no such file", path)
	}
	return size
}

// FileSizeOrZero returns the size of the file at path, 0 where there is
// none: SQLite's -wal, say, which comes and goes.
func FileSizeOrZero(t testing.TB, path string) int64 {
	t.Helper()
	size, _ := sizeOf(t, path)
	return size
}

// sizeOf returns the size of the file at path and true, or 0 and false
// where there is none, failing the test on any other error.
func sizeOf(t testing.TB, path string) (int64, bool) {
	t.Helper()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return info.Size(), true
}

// ShortDir makes a new folder of the test's own, removed when the test
// ends, with a name short enough to hold a unix socket: config.md, A
// listener, path, "the socket file, at most 107 bytes", which t.TempDir's
// names, holding the test's, can pass. It is made in TMPDIR, as t.TempDir's
// are: make's .tmp of the repository, never /tmp.
func ShortDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "u")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) }) // the test is over
	return dir
}
