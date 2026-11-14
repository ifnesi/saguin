package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of `saguin --passwd` (item 0018a), as the operator runs it.
// design.md, Operations: "saguin --passwd list <file>, add <file> <user>
// [password], delete <file> <user> [...] manages a password file: list
// prints one user per line in file order, or (no users); add for a listed
// user replaces that line's password, every other line and comment kept;
// delete of a user not listed says so and exits 0; add asks twice, without
// echo, when no password is given, and writes $7$ with 1,000 iterations and
// a 64-byte salt; an empty password, or a user name a password file cannot
// hold (config.md, password_file), is refused; a change is written to a
// new file in the same folder, synced to disk, then renamed over the old
// one, so a crash leaves the old file or the new one whole; the file keeps
// its mode and owner, and the change is refused where the owner cannot be
// kept; a file the command creates is readable by its owner alone (0600)".

// passwd runs `saguin --passwd` with args to its end, as execute does.
func passwd(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return execute(t, support.BrokerBinary, append([]string{"--passwd"}, args...)...)
}

// The Operations row above, add. With the password given, add makes a new
// file holding alice's $7$ line, 1,000 iterations and a 64-byte salt, which
// a broker started on the file then accepts. With none given, it asks
// twice on its terminal, without echo: two equal answers write that line;
// two that differ are refused, no file made. An empty password given is
// refused, no file made, and so is a user name config.md's password_file
// rules out: "a user name is 1 to 65,535 bytes of valid UTF-8 with no
// control character, holds no :, does not start with # and has no leading
// or trailing space". A refusal exits 1, as a refused configuration
// does, the command line being one saguin takes (a command line it does
// not take exits 2, with its usage: cmd/saguin, TestConfigRefused).
func TestPasswdAdd(t *testing.T) {
	t.Run("with the password given", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "passwd")
		if code, stdout, stderr := passwd(t, "add", file, "alice", "alice-pw"); code != 0 {
			t.Fatalf("add: exit %d, stdout %q, stderr %q; want 0", code, stdout, stderr)
		}
		lines := entries(t, file)
		if len(lines) != 1 {
			t.Fatalf("the new file holds %q; want alice's line alone", lines)
		}
		sevenEntry(t, lines[0], "alice", "alice-pw")
		b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file}})
		harness.Connected(t, b.Addr, harness.V5, credentials(clientID(t, ""), "alice", "alice-pw"))
	})
	t.Run("asked twice without echo", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "passwd")
		code, shown := typed(t, file, "typed-pw", "typed-pw")
		if code != 0 {
			t.Fatalf("add with two equal answers: exit %d; want 0. Its terminal showed:\n%s", code, shown)
		}
		lines := entries(t, file)
		if len(lines) != 1 {
			t.Fatalf("the new file holds %q; want alice's line alone", lines)
		}
		sevenEntry(t, lines[0], "alice", "typed-pw")
	})
	t.Run("two answers that differ", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "passwd")
		if code, shown := typed(t, file, "first-pw", "second-pw"); code != 1 {
			t.Errorf("add with two answers that differ: exit %d; want it refused, exit 1. Its terminal showed:\n%s", code, shown)
		}
		absentFile(t, file)
	})
	t.Run("an empty password", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "passwd")
		if code, stdout, stderr := passwd(t, "add", file, "alice", ""); code != 1 {
			t.Errorf("add with an empty password: exit %d, stdout %q, stderr %q; want it refused, exit 1", code, stdout, stderr)
		}
		absentFile(t, file)
	})
	t.Run("a user name a password file cannot hold", func(t *testing.T) {
		for _, c := range []struct{ what, user string }{
			{"empty", ""},
			{"a control character", "al\x01ice"},
			{"a tab", "al\tice"},
			{"a line feed", "al\nice"},
			{"not valid UTF-8", "\xffalice"},
			{"of 65,536 bytes", strings.Repeat("u", 65536)},
			{"holding a colon", "al:ice"},
			{"starting with #", "#alice"},
			{"a leading space", " alice"},
			{"a trailing space", "alice "},
		} {
			file := filepath.Join(t.TempDir(), "passwd")
			if code, stdout, stderr := passwd(t, "add", file, c.user, "alice-pw"); code != 1 {
				t.Errorf("add for a user name %s: exit %d, stdout %.200q, stderr %.200q; want it refused, exit 1", c.what, code, stdout, stderr)
			}
			absentFile(t, file)
		}
	})
}

// openPTY opens a pseudo-terminal through /dev/ptmx, as pts(4) describes:
// the controlling side, and the terminal a program reads and writes, its
// slave unlocked (TIOCSPTLCK) and found by number (TIOCGPTN). Both close
// when the test ends.
func openPTY(t *testing.T) (master, slave *os.File) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = master.Close() }) // the test is over
	var unlock int32
	if err := ioctl(master.Fd(), syscall.TIOCSPTLCK, unsafe.Pointer(&unlock)); err != nil {
		t.Fatalf("unlocking the pseudo-terminal: %v", err)
	}
	var n uint32
	if err := ioctl(master.Fd(), syscall.TIOCGPTN, unsafe.Pointer(&n)); err != nil {
		t.Fatalf("numbering the pseudo-terminal: %v", err)
	}
	slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slave.Close() }) // closed already where the test read to the end
	return master, slave
}

// ioctl is the ioctl(2) request req on the descriptor fd, its argument at
// arg.
func ioctl(fd uintptr, req uint, arg unsafe.Pointer) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(req), uintptr(arg)); errno != 0 {
		return errno
	}
	return nil
}

// echoing reports whether the terminal slave echoes what is typed: its
// ECHO local mode (termios(3), read with TCGETS).
func echoing(t *testing.T, slave *os.File) bool {
	t.Helper()
	var mode syscall.Termios
	if err := ioctl(slave.Fd(), syscall.TCGETS, unsafe.Pointer(&mode)); err != nil {
		t.Fatalf("reading the terminal's modes: %v", err)
	}
	return mode.Lflag&syscall.ECHO != 0
}

// typed runs `saguin --passwd add file alice`, no password given, on a
// pseudo-terminal of its own as its controlling terminal and its standard
// input, output and error, and types each answer, followed by a line feed,
// once the command has printed something since the last answer (its
// question) and its terminal does not echo. It fails the test where the
// command does not ask so, or where what its terminal showed holds any
// answer: what is typed without echo is never shown. It returns the
// command's exit code and what its terminal showed.
func typed(t *testing.T, file string, answers ...string) (code int, shown string) {
	t.Helper()
	master, slave := openPTY(t)
	ctx, cancel := context.WithTimeout(t.Context(), startLimit)
	defer cancel()
	cmd := exec.CommandContext(ctx, support.BrokerBinary, "--passwd", "add", file, "alice")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true} // its controlling terminal: Ctty 0, its standard input
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// The terminal's output is read as it comes, until the terminal ends.
	chunks := make(chan []byte, 64)
	go func() {
		defer close(chunks)
		for {
			buf := make([]byte, 4096)
			n, err := master.Read(buf)
			if n > 0 {
				chunks <- buf[:n]
			}
			if err != nil {
				return
			}
		}
	}()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	var out bytes.Buffer
	drain := func() {
		for {
			select {
			case c, ok := <-chunks:
				if !ok {
					return
				}
				out.Write(c)
			default:
				return
			}
		}
	}
	seen, ended := 0, false
	for i, answer := range answers {
		asked := testkit.Within(startLimit, func() bool {
			drain()
			select {
			case err := <-exited:
				exited <- err // kept for the wait below
				ended = true
				return true
			default:
			}
			return out.Len() > seen && !echoing(t, slave)
		})
		if !asked || ended {
			t.Fatalf("the command did not ask, without echo, for answer %d of %d (ended: %v). Its terminal showed:\n%s", i+1, len(answers), ended, out.String())
		}
		seen = out.Len()
		if _, err := master.WriteString(answer + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	err := <-exited
	if _, isExit := errors.AsType[*exec.ExitError](err); err != nil && !isExit {
		t.Fatalf("the command: %v", err)
	}
	_ = slave.Close() // the terminal ends: the reader reads to its end
	for c := range chunks {
		out.Write(c)
	}
	shown = out.String()
	for _, answer := range answers {
		if strings.Contains(shown, answer) {
			t.Errorf("the terminal showed the answer %q: it was echoed or printed. It showed:\n%s", answer, shown)
		}
	}
	return cmd.ProcessState.ExitCode(), shown
}

// clientFile is a client password file of comments, a blank line and three
// users, alice, bob and carol, in that order, in both schemes.
func clientFile(t *testing.T) string {
	return strings.Join([]string{
		"# Sagüin's clients",
		"alice:" + sixHash("alice-pw", salt12),
		"",
		"# bob is the dashboard",
		"bob:" + sixHash("bob-pw", salt12),
		"carol:" + sevenHash(t, "carol-pw", 1000, salt64),
		"# the end",
	}, "\n") + "\n"
}

// The Operations row above, the file's handling. A change (delete bob)
// writes a new file and renames it over the old one: the path names
// another inode, the old file, still open, holds its bytes as they were,
// and the folder holds the one file; the file keeps its mode, 0640, and
// its owner and group, the group another of the operator's where the
// operator has one. A file add creates is 0600. add for a listed user
// replaces that line alone, every other line and comment kept byte for
// byte; delete of a user not listed says so and exits 0, the file as it
// was; list prints the users in file order, or (no users).
func TestPasswdFileHandling(t *testing.T) {
	t.Run("a change replaces the file whole, keeping mode and owner", func(t *testing.T) {
		content := clientFile(t)
		want := strings.Replace(content, "bob:"+sixHash("bob-pw", salt12)+"\n", "", 1)
		if got := changedInOneStep(t, content, "delete", "bob"); got != want {
			t.Errorf("after delete bob the file holds\n%s\nwant\n%s", got, want)
		}
	})
	t.Run("a file the command creates is 0600", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "passwd")
		if code, stdout, stderr := passwd(t, "add", file, "alice", "alice-pw"); code != 0 {
			t.Fatalf("add: exit %d, stdout %q, stderr %q; want 0", code, stdout, stderr)
		}
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Errorf("the file add created has mode %04o; want 0600", mode)
		}
	})
	t.Run("add for a listed user replaces that line alone", func(t *testing.T) {
		content := clientFile(t)
		file := testkit.WriteFile(t, t.TempDir(), "passwd", content)
		if code, stdout, stderr := passwd(t, "add", file, "bob", "new-bob-pw"); code != 0 {
			t.Fatalf("add bob: exit %d, stdout %q, stderr %q; want 0", code, stdout, stderr)
		}
		was := strings.Split(strings.TrimSuffix(content, "\n"), "\n")
		is := entries(t, file)
		if len(is) != len(was) {
			t.Fatalf("after add bob the file holds\n%s\nwant bob's line replaced and every other kept", readFile(t, file))
		}
		bob := slices.Index(was, "bob:"+sixHash("bob-pw", salt12))
		for i := range was {
			if i != bob && is[i] != was[i] {
				t.Errorf("line %d is %q after add bob; want %q, as it was", i+1, is[i], was[i])
			}
		}
		sevenEntry(t, is[bob], "bob", "new-bob-pw")
		if !strings.HasSuffix(readFile(t, file), "\n") {
			t.Error("the file lost its last line feed")
		}
	})
	t.Run("delete of a user not listed", func(t *testing.T) {
		content := clientFile(t)
		file := testkit.WriteFile(t, t.TempDir(), "passwd", content)
		code, stdout, stderr := passwd(t, "delete", file, "dave")
		if code != 0 || !strings.Contains(stdout+stderr, "dave") {
			t.Errorf("delete dave: exit %d, stdout %q, stderr %q; want 0, saying dave is not listed", code, stdout, stderr)
		}
		if got := readFile(t, file); got != content {
			t.Errorf("after delete dave the file holds\n%s\nwant it as it was", got)
		}
	})
	t.Run("list", func(t *testing.T) {
		dir := t.TempDir()
		listed := passwordFile(t, dir, "listed", "# not sorted", "zed:"+sixHash("z", salt12), "",
			"alice:"+sixHash("a", salt12), "# between", "mike:"+sevenHash(t, "m", 1000, salt64))
		empty := passwordFile(t, dir, "empty", "# no one yet", "")
		for _, c := range []struct{ file, want string }{{listed, "zed\nalice\nmike\n"}, {empty, "(no users)\n"}} {
			if code, stdout, stderr := passwd(t, "list", c.file); code != 0 || stdout != c.want {
				t.Errorf("list %s: exit %d, stdout %q, stderr %q; want 0 and %q", filepath.Base(c.file), code, stdout, stderr, c.want)
			}
		}
	})
}

// changedInOneStep writes content to a file of mode 0640 in a folder of its
// own, its group another of the operator's where the operator has one, runs
// saguin --passwd verb on it with args, which must exit 0, and returns what
// the file then holds. It fails the test unless the change was written to a
// new file renamed over the old one: the path names another inode, the old
// file, still open, holds its bytes as they were, and the folder holds the
// one file; and the file kept its mode and its owner and group.
func changedInOneStep(t *testing.T, content, verb string, args ...string) string {
	t.Helper()
	dir := t.TempDir()
	file := testkit.WriteFile(t, dir, "passwd", content)
	if err := os.Chmod(file, 0o640); err != nil {
		t.Fatal(err)
	}
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(groups, func(g int) bool { return g != os.Getgid() }); i >= 0 {
		if err := os.Chown(file, -1, groups[i]); err != nil {
			t.Fatal(err)
		}
	}
	old, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Close() }) // read only
	before, err := old.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if code, stdout, stderr := passwd(t, append([]string{verb, file}, args...)...); code != 0 {
		t.Fatalf("%s %v: exit %d, stdout %q, stderr %q; want 0", verb, args, code, stdout, stderr)
	}
	after, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Errorf("%s rewrote the file in place; want a new file renamed over it", verb)
	}
	if mode := after.Mode().Perm(); mode != 0o640 {
		t.Errorf("the file's mode is %04o after %s; want 0640, as it was", mode, verb)
	}
	was, is := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
	if is.Uid != was.Uid || is.Gid != was.Gid {
		t.Errorf("the file's owner and group are %d:%d after %s; want %d:%d, as they were", is.Uid, is.Gid, verb, was.Uid, was.Gid)
	}
	if held := readAll(t, old); held != content {
		t.Errorf("the old file, still open, holds %q after %s; want its bytes as they were", held, verb)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*")); !slices.Equal(left, []string{file}) {
		t.Errorf("the folder holds %v after %s; want the file alone", left, verb)
	}
	return readFile(t, file)
}

// design.md, Operations: "saguin --passwd [...] scope <file> <user> <routes
// or all> | manages a password file: [...] a change is written to a new file
// in the same folder, synced to disk, then renamed over the old one, so a
// crash leaves the old file or the new one whole; the file keeps its mode
// and owner [...]; scope is for the operators' file only"; Who may call: "A
// line of the operators' password file may end in a third field containing
// route prefixes. These are separated by commas and matched at a /
// boundary (prometheus:$7$…:/metrics). Without that field, the user reaches
// every route".
//
// An operators' file of comments, a blank line and three users, alice, bob
// and carol. scope bob /metrics,/v1/operations/acl ends bob's line, his
// hash as it was, with that third field; scope bob all, on the result, ends
// it without one, the file as it first was; scope bob /health, on the same
// result, replaces his third field. Each change keeps every other line and
// comment byte for byte and replaces the file whole, keeping its mode and
// owner (changedInOneStep).
func TestPasswdScope(t *testing.T) {
	content := clientFile(t)
	bob := "bob:" + sixHash("bob-pw", salt12)
	scoped := strings.Replace(content, bob+"\n", bob+":/metrics,/v1/operations/acl\n", 1)
	for _, c := range []struct {
		name, from, user, routes, want string
	}{
		{"a scope set", content, "bob", "/metrics,/v1/operations/acl", scoped},
		{"a scope cleared by all", scoped, "bob", "all", content},
		{"a scope replaced", scoped, "bob", "/health", strings.Replace(content, bob+"\n", bob+":/health\n", 1)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := changedInOneStep(t, c.from, "scope", c.user, c.routes); got != c.want {
				t.Errorf("after scope %s %s the file holds\n%s\nwant\n%s", c.user, c.routes, got, c.want)
			}
		})
	}
}

// readFile returns the content of the file at path.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// readAll returns what f holds from its start.
func readAll(t *testing.T, f *os.File) string {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
