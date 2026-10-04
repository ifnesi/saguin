package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
)

// The commands an operator runs, driven through the function the flag
// calls rather than by inspecting what it would have done.
func TestThePasswordCommandsManageAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.passwd")

	// A file that does not exist yet is the first user, which is how one of
	// these always starts.
	if code := managePasswd([]string{"add", path, "alice", "hunter2"}, false, false); code != 0 {
		t.Fatalf("adding the first user exited %d", code)
	}
	f, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !f.Verify("alice", "hunter2") {
		t.Error("the user that was just added cannot authenticate")
	}

	// Adding an existing user changes their password rather than adding a
	// second line, which is what mosquitto_passwd does and what anybody
	// typing it expects.
	if code := managePasswd([]string{"add", path, "alice", "changed"}, false, false); code != 0 {
		t.Fatalf("changing a password exited %d", code)
	}
	f, _ = passwd.Load(path)
	if f.Verify("alice", "hunter2") {
		t.Error("the old password still works")
	}
	if !f.Verify("alice", "changed") {
		t.Error("the new password does not")
	}
	if got := f.Users(); len(got) != 1 {
		t.Errorf("the file now names %v, want one alice rather than two", got)
	}

	if code := managePasswd([]string{"delete", path, "alice"}, false, false); code != 0 {
		t.Fatalf("delete exited %d", code)
	}
	f, _ = passwd.Load(path)
	if len(f.Users()) != 0 {
		t.Errorf("the file still names %v", f.Users())
	}

	// Deleting a user that is not there is not a failure: the file ends up
	// as asked either way, and a script that runs twice has not broken.
	if code := managePasswd([]string{"delete", path, "alice"}, false, false); code != 0 {
		t.Errorf("deleting an absent user exited %d, which fails a script that runs twice", code)
	}
}

// **`--passwd delete` on a file naming a user twice refuses and changes
// nothing**, where it used to report the user deleted and leave the second
// line admitting them (RFC 0002 makes the delete the first step of
// withdrawing a device). Every verb reads through passwd.Load, which is
// where the refusal is; this is the verb the finding was about.
func TestDeletingFromAFileNamingAUserTwiceChangesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.passwd")
	a, err := passwd.Hash("first")
	if err != nil {
		t.Fatal(err)
	}
	b, err := passwd.Hash("second")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("bob:" + a + "\nalice:" + a + "\nbob:" + b + "\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if code := managePasswd([]string{"delete", path, "bob"}, false, false); code == 0 {
		t.Error("delete reported success on a file naming bob twice")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(after) != string(body) {
		t.Errorf("the refused delete rewrote the file:\n%s", after)
	}
}

// The same rules a migration follows, and for the same reason: this reads
// and writes the file it is named, so a configuration it does not consult
// must not be accepted as though it did.
func TestThePasswordCommandsRefuseWhatTheyCannotHonour(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.passwd")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, tc := range []struct {
		name       string
		args       []string
		cfg, check bool
	}{
		{"a configuration it does not read", []string{"list", path}, true, false},
		{"the configuration checker", []string{"list", path}, false, true},
		{"no action at all", nil, false, false},
		{"an action nobody has", []string{"rotate", path}, false, false},
		{"add with no user", []string{"add", path}, false, false},
		{"delete with no user", []string{"delete", path}, false, false},
		{"list with an extra argument", []string{"list", path, "alice"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := managePasswd(tc.args, tc.cfg, tc.check); code == 0 {
				t.Error("accepted")
			}
		})
	}

	// A colon is what separates a name from its hash, so a name holding one
	// writes a file that reads back as a different user entirely.
	if code := managePasswd([]string{"add", path, "al:ice", "hunter2"}, false, false); code == 0 {
		t.Error("a user name with a colon in it was accepted")
	}

	// An empty password hashes perfectly well, which is exactly why it is
	// refused here rather than left to surprise somebody.
	if code := managePasswd([]string{"add", path, "alice", ""}, false, false); code == 0 {
		t.Error("an empty password was accepted")
	}
}

// An unreadable file is reported rather than treated as an empty one: a
// typo in the path would otherwise start a new file and leave the operator
// wondering where their users went.
func TestListingAFileThatIsNotThereSaysSo(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nowhere.passwd")
	if code := managePasswd([]string{"list", missing}, false, false); code == 0 {
		t.Error("listing a file that does not exist reported success")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Error("listing created the file")
	}
}

// The usage text names all three verbs, because a wrong one is when
// somebody needs to be told what the right ones are.
func TestTheUsageTextNamesEveryVerb(t *testing.T) {
	for _, verb := range []string{"list", "add", "delete"} {
		if !strings.Contains(passwdUsage, verb) {
			t.Errorf("the usage text does not mention %q", verb)
		}
	}
}

// A user name holding a NUL or a control character can never authenticate
// at either door (broker.ValidName), so `--passwd add` refuses it with the
// reason rather than writing a user nobody can be.
func TestPasswdAddRefusesANameNoDoorTakes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.passwd")
	if code := managePasswd([]string{"add", path, "dev\x1bice", "hunter2"}, false, false); code == 0 {
		t.Error("a user name holding ESC was added")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("the refused add wrote the file anyway")
	}
	if code := managePasswd([]string{"add", path, "device-7", "hunter2"}, false, false); code != 0 {
		t.Fatalf("an ordinary user name was refused (%d), so this proves nothing", code)
	}
}

// An entry of a credential file whose name holds a control character is
// said once when the files are read - at the start and at each SIGUSR1 -
// rather than refusing them: the rest of each file is still good, and the
// entry is one nobody can authenticate as.
func TestACredentialEntryNobodyCanBeIsListedWhenTheFilesAreRead(t *testing.T) {
	dir := t.TempDir()
	pwPath := filepath.Join(dir, "clients.passwd")
	pf := passwd.New(pwPath)
	for _, u := range []string{"device-7", "dev\x1bice"} {
		if err := pf.Set(u, "hunter2"); err != nil {
			t.Fatalf("set %q: %v", u, err)
		}
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  r:
    - topic: "#"
      allow: [read]
users:
  device-7: [r]
  "ops\x7f": [r]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
broker:
  id: t
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: `+pwPath+`
    acl_file: `+aclPath+`
channels: {}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, reg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	auth, findings := loadAuthorization(cfg, reg)
	if len(findings) > 0 || auth == nil {
		t.Fatalf("the files were refused (%v): an unusable entry is a warning, not a startup failure", findings)
	}
	got := strings.Join(auth.unnamable, "\n")
	for _, want := range []string{`user "dev\x1bice"`, `users "ops\x7f"`} {
		if !strings.Contains(got, want) {
			t.Errorf("the entries listed are %q, want one naming %s", auth.unnamable, want)
		}
	}
	if len(auth.unnamable) != 2 {
		t.Errorf("%d entries listed, want the 2 unusable ones: %q", len(auth.unnamable), auth.unnamable)
	}
}
