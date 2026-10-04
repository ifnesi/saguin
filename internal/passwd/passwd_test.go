package passwd_test

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/passwd"
)

// **These are real hashes, written by mosquitto_passwd 2.1.2**, not
// examples of the format. That is the whole assertion: an operator's
// existing file works, and the only way to know is to read one somebody
// else's tool wrote.
//
//	mosquitto_passwd -c -b file alice hunter2
//	mosquitto_passwd    -b file bob   's3cret pass'
//	mosquitto_passwd -H sha512 -c -b old dave hunter2
const (
	alice = "alice:$7$1000$aZgkP6cb2b3ixQwdoDGK6SrcanFw8qhZ/v+HHbVtNxshn3zpp4U2+KZGND/8mzSrDJk6iwVa6ooQocRmZvXH2g==$NOyrZjiDdda7tcek5aun0S+c26WBmt++DYoQxMLSW9pFqIseyqKye88cHlNurOf6utRY2Zz24/+9a6ZGiuDrZg=="
	bob   = "bob:$7$1000$yKoF2KDLkeo8qvGpC6RZnHYSOtWoR5Ss2zkFQEVpL5ssJ62d436/PwoXCR4/sCZAksqHj5XaAy8ljPacU/c2MQ==$Oz35vXEhLkvg+Rm9m2m3fQXPLD1wRD/2E7lbeKemRe5caFUuhwMAEgKmudtZ1O5O4NB/rj54HV/ML9igvKcVaQ=="
	dave  = "dave:$6$3CT9isteD+dG9RdACdWtIilHbQcwYRV+lc3susyrq12coJXccvmbYaPzAqoqDbtYPYOdt/CaWT288xLqQPQd7Q==$feT2JsIRKYYW54dDq1wJXxJ7rUG8hBF+RplhMJQe237Iwj0sUbmVS9fAKlDFG8YcPhQT+1oOMTLRLDy22y8TLg=="
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "saguin.passwd")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// A file Mosquitto wrote, read by saguin, with the passwords those users
// actually have.
func TestAMosquittoPasswordFileWorksUnchanged(t *testing.T) {
	f, err := passwd.Load(write(t, strings.Join([]string{
		"# a comment mosquitto_passwd would leave alone",
		alice, bob, dave, "",
	}, "\n")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	for _, tc := range []struct {
		user, password string
		want           bool
	}{
		{"alice", "hunter2", true},   // $7$, PBKDF2-SHA512
		{"bob", "s3cret pass", true}, // and a password with a space in it
		{"dave", "hunter2", true},    // $6$, the 1.6-era scheme
		{"alice", "hunter3", false},  // one character out
		{"alice", "", false},         // and nothing at all
		{"bob", "hunter2", false},    // alice's password, bob's account
		{"nobody", "hunter2", false}, // a user the file does not have
	} {
		if got := f.Verify(tc.user, tc.password); got != tc.want {
			t.Errorf("Verify(%q, %q) = %v, want %v", tc.user, tc.password, got, tc.want)
		}
	}

	if got := f.Users(); len(got) != 3 {
		t.Errorf("Users() = %v, want the three the file names", got)
	}
}

// **An unreadable hash is an error, never a failed login.** An operator
// migrating from Mosquitto 2.1 has exactly this file, and the alternative
// behaviour - every user quietly failing to authenticate - looks like a
// fleet that has forgotten its passwords, which sends somebody to debug the
// wrong thing entirely.
func TestAnUnreadableHashIsRefusedByName(t *testing.T) {
	for _, tc := range []struct{ name, line, want string }{
		{"argon2id, which Mosquitto 2.1 documents as its default",
			"erin:$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA", "argon2id"},
		{"a plain text password, which mosquitto_passwd -U converts",
			"erin:hunter2", "plain text"},
		{"a scheme nobody has heard of", "erin:$9$x$y", "unknown hash scheme"},
		{"an iteration count that is not one", "erin:$7$zero$c2FsdA==$aGFzaA==", "not a positive number"},
		{"a salt that is not base64", "erin:$7$1000$not-base-64-!!$aGFzaA==", "not base64"},
		{"a $6$ hash missing a field", "erin:$6$c2FsdA==", "a $6$ hash is $6$<salt>$<hash>"},
		{"a $7$ hash missing a field", "erin:$7$1000$c2FsdA==",
			"a $7$ hash is $7$<iterations>$<salt>$<hash>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := passwd.Load(write(t, tc.line+"\n"))
			if err == nil {
				t.Fatal("accepted, so every login against this file fails and nothing says why")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not say what is wrong (%q): %v", tc.want, err)
			}
			// The line and the user, because a file with two hundred users
			// in it needs to say which one.
			if !strings.Contains(err.Error(), ":1:") || !strings.Contains(err.Error(), "erin") {
				t.Errorf("the error names neither the line nor the user: %v", err)
			}
		})
	}
}

// **A name on two lines is refused, naming both lines**, as mosquitto
// refuses it. Every reader acts on the first match, so the second line's
// password was invisible until a delete removed the first - and then it
// admitted the device the operator had just withdrawn. The control, the
// same file without the second line, loads: a refusal of every file with
// bob in it would pass the first half alone.
func TestAUserOnTwoLinesIsRefusedNamingBoth(t *testing.T) {
	second := "bob:" + strings.TrimPrefix(dave, "dave:")
	_, err := passwd.Load(write(t, bob+"\n"+alice+"\n"+second+"\n"))
	if err == nil {
		t.Fatal("a file naming bob twice loaded, so a delete leaves the second password " +
			"admitting him")
	}
	for _, want := range []string{":3:", `"bob"`, "line 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %s: %v", want, err)
		}
	}
	if _, err := passwd.Load(write(t, bob+"\n"+alice+"\n")); err != nil {
		t.Errorf("the same file with bob once was refused: %v", err)
	}
}

// **A line with no colon is refused by its line**, not read as a user
// with no password or skipped: it is neither a user nor a comment, and a
// file somebody mangled should say where.
func TestALineWithNoColonIsRefusedByItsLine(t *testing.T) {
	_, err := passwd.Load(write(t, alice+"\nerin\n"))
	if err == nil {
		t.Fatal("a line with no colon loaded")
	}
	for _, want := range []string{":2:", "no colon"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// What saguin writes is what Mosquitto reads. A file saguin has touched has
// not left Mosquitto's world, which is the other half of the migration
// promise: it goes both ways until the operator is sure.
func TestWhatSaguinWritesIsTheFormatMosquittoWrites(t *testing.T) {
	path := write(t, "# a file with a comment and a user\n"+alice+"\n")
	f, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := f.Set("carol", "a new password"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.Set("alice", "changed"); err != nil {
		t.Fatalf("set alice: %v", err)
	}
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.HasPrefix(string(body), "# a file with a comment") {
		t.Errorf("the comment was lost; saguin is not the only thing that reads this "+
			"file:\n%s", body)
	}
	for _, want := range []string{"alice:$7$1000$", "carol:$7$1000$"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("no %q in what was written:\n%s", want, body)
		}
	}

	again, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("what saguin wrote does not load again: %v", err)
	}
	if !again.Verify("carol", "a new password") {
		t.Error("the user saguin added cannot authenticate")
	}
	if !again.Verify("alice", "changed") {
		t.Error("the password saguin changed was not changed")
	}
	if again.Verify("alice", "hunter2") {
		t.Error("the old password still works, so the change did nothing")
	}

	if !again.Delete("alice") {
		t.Error("deleting a user that exists reported nothing to delete")
	}
	if again.Delete("nobody") {
		t.Error("deleting a user that does not exist reported a deletion")
	}
}

// The file is the whole of the defence for a hash cheap enough to check on
// every CONNECT, so saguin does not create one the rest of the machine can
// read.
func TestAFileSaguinWritesIsNotReadableByEverybody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.passwd")
	f := passwd.New(path)
	if err := f.Set("alice", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("the password file is %v, want 0600: whoever can read it can guess "+
			"against it for as long as they like", got)
	}
}

// **A file that exists keeps its mode and its owner** across a rewrite, as
// mosquitto_passwd's in-place rewrite keeps them. The rename put a new
// 0600 inode in place, so an operator's 0640 - a group the broker runs in -
// came back 0600, and the SIGUSR1 meant to admit a new device found a file
// the broker could not read. The owner is asserted unchanged; a test not
// running as root cannot make the file somebody else's.
func TestAFileThatExistsKeepsItsModeAndOwner(t *testing.T) {
	path := write(t, alice+"\n")
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	f, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := f.Set("carol", "a new password"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := after.Mode().Perm(); got != 0o640 {
		t.Errorf("a 0640 file came back %v: the broker reading it through its group can "+
			"no longer read it", got)
	}
	b, a := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
	if b.Uid != a.Uid || b.Gid != a.Gid {
		t.Errorf("the owner went from %d:%d to %d:%d", b.Uid, b.Gid, a.Uid, a.Gid)
	}
}

// **A refusal takes as long whether the name is real or not**, in the file
// the migration exists for: every Mosquitto 1.6 file is $6$, one SHA-512
// round, checked in a quarter of a microsecond. The unknown name was checked
// at $7$ 1000 iterations instead - over a thousand times longer, which lists
// the fleet's accounts to anybody timing CONNACKs. Now it is checked against
// the file's first user. The fastest of five trials of 200 each, so one
// preemption cannot decide it; the bound is ten times, where the defect is
// three orders of magnitude.
func TestAnUnknownNameIsRefusedInTheTimeAKnownOneIs(t *testing.T) {
	f, err := passwd.Load(write(t, dave+"\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	fastest := func(user string) time.Duration {
		best := time.Duration(1 << 62)
		for trial := 0; trial < 5; trial++ {
			start := time.Now()
			for i := 0; i < 200; i++ {
				if f.Verify(user, "wrong") {
					t.Fatalf("%s was admitted with a wrong password", user)
				}
			}
			best = min(best, time.Since(start)/200)
		}
		return best
	}
	known, unknown := fastest("dave"), fastest("nobody")
	t.Logf("$6$: a known name is refused in %v, an unknown one in %v", known, unknown)
	if unknown > 10*known+time.Microsecond {
		t.Errorf("on a $6$ file a known name is refused in %v and an unknown one in %v: the "+
			"time says which names are real", known, unknown)
	}
}

// RFC 0002 "--passwd": the file is consulted on every CONNECT, so this is
// what one is charged for - a $7$ entry as saguin writes it, at Mosquitto's
// own 1000 iterations, verified against the right password. What a higher
// count would cost a fleet reconnecting after a link drop is this number
// multiplied.
func BenchmarkVerify(b *testing.B) {
	// Written and read back, which is the path a deployment takes: saguin
	// --passwd writes the file and the broker loads it at startup.
	path := filepath.Join(b.TempDir(), "clients.passwd")
	w := passwd.New(path)
	if err := w.Set("alice", "hunter2"); err != nil {
		b.Fatal(err)
	}
	if err := w.Save(); err != nil {
		b.Fatal(err)
	}
	f, err := passwd.Load(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if !f.Verify("alice", "hunter2") {
			b.Fatal("the right password was refused")
		}
	}
}

// **A third field names the routes a user may reach**, and the whole design
// rests on the colon: a user name may not contain one and a stored hash is
// `$`-prefixed base64, whose alphabet has none - so the second colon ends
// the hash unambiguously.
//
// **The delimiter had to be the colon rather than the `@` first proposed**,
// and this is the case that decides it: `alice@example.com` is a name this
// file accepts today, and an `@` delimiter would silently read it as user
// `alice` scoped to `example.com`. The file would load, the user would
// exist, and their access would be wrong.
func TestAThirdFieldNamesTheRoutesAUserMayReach(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "operations.passwd")

	f := passwd.New(path)
	for _, u := range []string{"prometheus", "alice@example.com", "unscoped"} {
		if err := f.Set(u, "hunter2"); err != nil {
			t.Fatalf("set %s: %v", u, err)
		}
	}
	if !f.SetScopes("prometheus", []string{"/metrics"}) {
		t.Fatal("SetScopes found no such user")
	}
	f.SetScopes("alice@example.com", []string{"/metrics", "/v1/operations"})
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	back, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := back.Scopes("alice@example.com"); len(got) != 2 {
		t.Fatalf("a name holding @ read back with scopes %v - the delimiter is eating "+
			"the domain, so this user's access is not what the file says", got)
	}

	for _, tc := range []struct {
		user, path string
		want       bool
		why        string
	}{
		{"prometheus", "/metrics", true, "the route it names"},
		{"prometheus", "/v1/operations/consumers", false, "a route it does not"},
		{"alice@example.com", "/metrics", true, "the first of two"},
		{"alice@example.com", "/v1/operations/queues/jobs", true, "under the second, at a segment boundary"},
		{"unscoped", "/metrics", true, "no third field: every route, which is what every file written before this means"},
		{"unscoped", "/v1/operations/consumers", true, "the same"},
		{"nobody", "/metrics", false, "a user the file does not hold reaches nothing"},

		// **The one a string prefix would get wrong.** `/v1/operations`
		// must not grant a route that merely starts the same way, which is
		// a grant nobody wrote reached through a name that looks alike.
		{"alice@example.com", "/v1/operationsomething", false, "not a segment boundary"},
	} {
		if got := back.Reaches(tc.user, tc.path); got != tc.want {
			t.Errorf("Reaches(%q, %q) = %v, want %v - %s",
				tc.user, tc.path, got, tc.want, tc.why)
		}
	}
}

// **A password change leaves the routes alone.** They are separate commands
// precisely so that rotating a credential does not silently widen a user
// who had been narrowed - at the moment somebody is thinking about a
// password rather than about access.
func TestChangingAPasswordKeepsTheRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.passwd")
	f := passwd.New(path)
	if err := f.Set("alice", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	f.SetScopes("alice", []string{"/v1/operations"})
	if err := f.Set("alice", "different"); err != nil {
		t.Fatalf("change: %v", err)
	}
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// **Read back from the file, not asked of the object.** Set rebuilds the
	// line it writes; the in-memory field is a separate copy, so asking the
	// object answers a question about the wrong one of the two. A mutation
	// that dropped the scope from the written line and left the field alone
	// passed the first version of this test.
	back, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := back.Scopes("alice"); len(got) != 1 || got[0] != "/v1/operations" {
		t.Fatalf("after a password change the file gives alice %v - a rotation widened "+
			"a user who had been narrowed", got)
	}
	if !back.Verify("alice", "different") {
		t.Error("the new password does not verify, so keeping the scope cost the hash")
	}
	if back.Verify("alice", "hunter2") {
		t.Error("the old password still verifies")
	}
}

// A file nobody has narrowed writes two fields, so it is byte for byte the
// file it always was - which is what keeps an MQTT password file Mosquitto's
// format hash for hash.
func TestAnUnscopedFileIsUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clients.passwd")
	f := passwd.New(path)
	if err := f.Set("demo", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := f.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n := strings.Count(strings.TrimSpace(string(body)), ":"); n != 1 {
		t.Errorf("the line holds %d colons, want 1: a file nobody scoped must not grow a "+
			"field, or it stops being the format Mosquitto writes\n%s", n, body)
	}
	if f.AnyScoped() {
		t.Error("a file nobody scoped reports that it has scopes")
	}
}

// **A scope that names no route denies that user everything**, from a file
// that reads as though it granted something.
func TestAScopeNamingNoRouteIsAFinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.passwd")
	f := passwd.New(path)
	if err := f.Set("alice", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	f.SetScopes("alice", []string{"/metric"})
	findings := f.CheckScopes()
	if len(findings) != 1 {
		t.Fatalf("a route saguin does not serve produced %d findings, want 1: %v",
			len(findings), findings)
	}
	for _, want := range []string{"/metric", "does not serve", "/metrics"} {
		if !strings.Contains(findings[0], want) {
			t.Errorf("the finding does not say %q, so an operator cannot see what to "+
				"write instead: %s", want, findings[0])
		}
	}
}

// A field that is present and empty reaches nothing, which is a locked-out
// user rather than an unrestricted one, so the file is refused rather than
// read either way round.
func TestAnEmptyScopeFieldIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.passwd")
	if err := os.WriteFile(path,
		[]byte("alice:$7$1000$c2FsdA==$c3Vt:\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := passwd.Load(path)
	if err == nil {
		t.Fatal("a present-but-empty scope field was accepted: it reaches no route, so " +
			"the user is locked out by a trailing colon nobody would notice")
	}
	if !strings.Contains(err.Error(), "reach") {
		t.Errorf("the refusal does not say what is wrong: %v", err)
	}
}

// bigFile is a file of n users sharing one real hash (every user's password
// is "hunter2"), so a file of tens of thousands is built without hashing
// tens of thousands of times.
func bigFile(tb testing.TB, n int) (*passwd.File, string) {
	tb.Helper()
	_, hash, _ := strings.Cut(alice, ":")
	var sb strings.Builder
	for i := range n {
		sb.WriteString("user" + strconv.Itoa(i) + ":" + hash + "\n")
	}
	path := filepath.Join(tb.TempDir(), "big.passwd")
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		tb.Fatal(err)
	}
	f, err := passwd.Load(path)
	if err != nil {
		tb.Fatal(err)
	}
	return f, path
}

// BenchmarkVerifyLargeFile is Verify against a file of 21,210 users, for the
// first, a middle and the last user and for one the file does not hold, which
// is checked against the first user's hash. The spread between them is what
// finding the user costs; the hash is the same in every case.
func BenchmarkVerifyLargeFile(b *testing.B) {
	const n = 21210
	f, _ := bigFile(b, n)
	for _, tc := range []struct {
		name, user string
		want       bool
	}{
		{"first", "user0", true},
		{"middle", "user" + strconv.Itoa(n/2), true},
		{"last", "user" + strconv.Itoa(n-1), true},
		{"unknown", "nobody", false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if f.Verify(tc.user, "hunter2") != tc.want {
					b.Fatal("Verify gave the wrong answer")
				}
			}
		})
	}
}

// TestFileMemoryAt21210Users prints what a loaded file of 21,210 users holds.
func TestFileMemoryAt21210Users(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f, _ := bigFile(t, 21210)
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("live heap for the file: %d bytes (%d users)", int64(after.HeapAlloc)-int64(before.HeapAlloc), len(f.Users()))
}

// **Every per-user lookup agrees with a scan of the file's lines**, across
// loads, sets, scope changes and deletes - the first user's included, which is
// the one an unknown name is timed against. The model is a plain slice of
// users searched linearly, the way the file itself was before it had an index.
func TestEveryLookupAgreesWithAScanAfterEveryMutation(t *testing.T) {
	type user struct {
		name, pw string
		scopes   []string
	}
	type step struct {
		op, name string
	}
	names := []string{"a", "b", "c", "alice@example.com"}
	pwOf := func(n string, gen int) string { return "pw-" + n + strconv.Itoa(gen) }
	tests := []struct {
		name  string
		start string // file body, "" for New
		steps []step
	}{
		{"new then sets", "", []step{{"set", "a"}, {"set", "b"}, {"set", "a"}, {"set", "c"}}},
		{"delete the first", "", []step{{"set", "a"}, {"set", "b"}, {"set", "c"}, {"del", "a"}, {"set", "a"}, {"del", "b"}, {"del", "c"}, {"del", "a"}, {"set", "b"}}},
		{"delete the last and a missing one", "", []step{{"set", "a"}, {"set", "b"}, {"del", "b"}, {"del", "zz"}, {"set", "c"}}},
		{"scopes survive a set", "", []step{{"set", "a"}, {"scope", "a"}, {"set", "a"}, {"scope", "b"}, {"del", "a"}, {"set", "a"}}},
		{"loaded with comments and blanks", "# top\n\n" + alice + "\n  # mid\n" + bob + "\n\n", []step{{"set", "bob"}, {"del", "alice"}, {"set", "alice"}, {"del", "bob"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var f *passwd.File
			var model []user
			if tc.start == "" {
				f = passwd.New(filepath.Join(t.TempDir(), "p"))
			} else {
				var err error
				if f, err = passwd.Load(write(t, tc.start)); err != nil {
					t.Fatal(err)
				}
				for _, u := range f.Users() {
					pw := map[string]string{"alice": "hunter2", "bob": "s3cret pass"}[u]
					model = append(model, user{name: u, pw: pw})
				}
			}
			find := func(n string) int {
				for i, u := range model {
					if u.name == n {
						return i
					}
				}
				return -1
			}
			check := func(when string) {
				t.Helper()
				probe := append([]string{"", "nobody", "alice", "bob"}, names...)
				for _, n := range probe {
					i := find(n)
					if got := f.Known(n); got != (i >= 0) {
						t.Errorf("%s: Known(%q) = %v, a scan says %v", when, n, got, i >= 0)
					}
					var want []string
					if i >= 0 {
						want = model[i].scopes
					}
					if got := f.Scopes(n); !slices.Equal(got, want) || (got == nil) != (want == nil) {
						t.Errorf("%s: Scopes(%q) = %#v, a scan says %#v", when, n, got, want)
					}
					for _, path := range []string{"/v1/operations/users", "/v1/other"} {
						w := false
						if i >= 0 {
							w = model[i].scopes == nil || slices.Contains(model[i].scopes, path)
						}
						if got := f.Reaches(n, path); got != w {
							t.Errorf("%s: Reaches(%q, %q) = %v, a scan says %v", when, n, path, got, w)
						}
					}
					if i >= 0 {
						if !f.Verify(n, model[i].pw) || f.Verify(n, model[i].pw+"x") {
							t.Errorf("%s: Verify(%q) disagrees with the password the model holds", when, n)
						}
					} else if f.Verify(n, "anything") {
						t.Errorf("%s: Verify admitted %q, who is not in the file", when, n)
					}
				}
				var want []string
				for _, u := range model {
					want = append(want, u.name)
				}
				if got := f.Users(); !slices.Equal(got, want) {
					t.Errorf("%s: Users = %v, want %v", when, got, want)
				}
			}
			check("start")
			for k, s := range tc.steps {
				when := s.op + " " + s.name + " (step " + strconv.Itoa(k) + ")"
				i := find(s.name)
				switch s.op {
				case "set":
					pw := pwOf(s.name, k)
					if err := f.Set(s.name, pw); err != nil {
						t.Fatal(err)
					}
					if i >= 0 {
						model[i].pw = pw
					} else {
						model = append(model, user{name: s.name, pw: pw})
					}
				case "del":
					if got := f.Delete(s.name); got != (i >= 0) {
						t.Errorf("%s: Delete = %v, a scan says %v", when, got, i >= 0)
					}
					if i >= 0 {
						model = append(model[:i], model[i+1:]...)
					}
				case "scope":
					scopes := []string{"/v1/operations/users"}
					if got := f.SetScopes(s.name, scopes); got != (i >= 0) {
						t.Errorf("%s: SetScopes = %v, a scan says %v", when, got, i >= 0)
					}
					if i >= 0 {
						model[i].scopes = scopes
					}
				}
				check(when)
			}
		})
	}
}

// The first user is what an unknown name is timed against, so the file must
// find it through comments, must move it when it is deleted, and must set it
// when a file emptied of users is filled again. Checked by what Verify costs
// rather than by reading the field: a $7$ line costs a third of a millisecond
// and a $6$ line a quarter of a microsecond.
func TestAnUnknownNameIsTimedAgainstTheCurrentFirstUser(t *testing.T) {
	cost := func(f *passwd.File) time.Duration {
		best := time.Hour
		for range 20 {
			t0 := time.Now()
			f.Verify("nobody", "x")
			best = min(best, time.Since(t0))
		}
		return best
	}
	const slow, fast = "slow", "fast"
	expect := func(t *testing.T, f *passwd.File, when, want string) {
		t.Helper()
		c := cost(f)
		if (want == slow) != (c > 100*time.Microsecond) {
			t.Errorf("%s: an unknown name cost %v; the first user's hash should be %s", when, c, want)
		}
	}
	t.Run("comments before the first user", func(t *testing.T) {
		f, err := passwd.Load(write(t, "# c\n\n"+alice+"\n"+dave+"\n"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, f, "loaded", slow)
		f.Delete("alice") // dave, a $6$ line, is now first
		expect(t, f, "first deleted", fast)
	})
	t.Run("emptied and refilled behind comments", func(t *testing.T) {
		f, err := passwd.Load(write(t, dave+"\n# c\n# d\n"))
		if err != nil {
			t.Fatal(err)
		}
		expect(t, f, "loaded", fast)
		f.Delete("dave")
		if err := f.Set("x", "pw"); err != nil { // a $7$ line, after two comments
			t.Fatal(err)
		}
		expect(t, f, "refilled", slow)
	})
}
