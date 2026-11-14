package support

import (
	"errors"
	"os"
	"os/exec"
	"os/user"
	"slices"
	"strconv"
	"syscall"
	"testing"
)

// The helpers of the tests of the read access an operator grants on
// file_path, which a memory-mode snapshot keeps (design.md, Store, memory
// mode: "It gives that file file_path's mode, owning group and access
// control list (ACL), so the read access an operator granted on file_path
// survives the snapshot"). A user namespace maps none of a user's
// supplementary groups nor any other user (bubblewrap, testkit.OnSmallDisk,
// shows them as 65534 and refuses them), so these tests run outside one,
// their memory_dir on the user's own runtime tmpfs (RuntimeMemoryDir).

// RuntimeMemoryDir returns a new directory of the test's own on the tmpfs
// at $XDG_RUNTIME_DIR, the user's runtime directory, as a memory-mode
// broker's memory_dir ("The memory_dir must be a tmpfs, which startup
// checks"), removed when the test ends; the test is skipped where there is
// none. A test whose files keep groups and ACLs the operator grants runs
// there, outside any user namespace; its database is a few pages.
func RuntimeMemoryDir(t *testing.T) string {
	t.Helper()
	runtime := os.Getenv("XDG_RUNTIME_DIR")
	var fs syscall.Statfs_t
	if runtime == "" || syscall.Statfs(runtime, &fs) != nil || fs.Type != tmpfsMagic {
		t.Skipf("no tmpfs at $XDG_RUNTIME_DIR (%q) for memory_dir outside a user namespace, which this test needs to grant groups and ACLs", runtime)
	}
	dir, err := os.MkdirTemp(runtime, "saguin-grants-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) }) // the test is over
	return dir
}

// tmpfsMagic is a tmpfs's f_type (statfs(2): TMPFS_MAGIC).
const tmpfsMagic = 0x01021994

// Group is a group as these tests name it: its id and, where it has one,
// its name.
type Group struct {
	ID   int
	Name string
}

// Names are the ways a log line or a refusal may name g: its name, where it
// has one, and its id.
func (g Group) Names() []string {
	names := []string{strconv.Itoa(g.ID)}
	if g.Name != "" {
		names = append(names, g.Name)
	}
	return names
}

// groupOf is the group of id, named where /etc/group or its like names it.
func groupOf(id int) Group {
	g := Group{ID: id}
	if found, err := user.LookupGroupId(strconv.Itoa(id)); err == nil {
		g.Name = found.Name
	}
	return g
}

// OwnGroups are the groups the test's process, and so the brokers it
// starts, belongs to: its effective group and its supplementary groups.
func OwnGroups(t *testing.T) []int {
	t.Helper()
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	return append(groups, os.Getegid())
}

// OtherGroup returns a group the test's user belongs to beside its own,
// effective one: the brokers the test starts can give a file that group
// ("A process can give a file only a group its user belongs to"). The test
// is skipped where the user has none.
func OtherGroup(t *testing.T) Group {
	t.Helper()
	for _, id := range OwnGroups(t) {
		if id != os.Getegid() {
			return groupOf(id)
		}
	}
	t.Skip("the test's user belongs to no group but its own; the test needs a second to grant read access by group")
	return Group{}
}

// GiveForeignGroup gives the file at path a group the test's user does not
// belong to, so no broker the test starts can give a file that group, and
// returns it. Only a process with the power to change a file's group
// beyond its own groups (root, or CAP_CHOWN) can; the test is skipped
// elsewhere.
func GiveForeignGroup(t *testing.T, path string) Group {
	t.Helper()
	own := OwnGroups(t)
	for id := 1; id < 65534; id++ {
		if slices.Contains(own, id) {
			continue
		}
		switch err := os.Chown(path, -1, id); {
		case err == nil:
			return groupOf(id)
		case errors.Is(err, syscall.EPERM):
			t.Skipf("this machine's test user cannot give a file a group it does not belong to (%v): the test needs root or CAP_CHOWN", err)
		}
	}
	t.Skip("no group id is free of the test's user's groups")
	return Group{}
}

// GrantRead gives the file at path the read access an operator grants
// (design.md, Store, The lock: "an operator grants others read access
// through the file's own permissions or ACLs"): mode 0640, the owning
// group group, and, with setfacl, an ACL entry letting user nobody read
// it. The test is skipped where setfacl is missing or nobody is unknown.
func GrantRead(t *testing.T, path string, group Group) {
	t.Helper()
	setfacl, err := exec.LookPath("setfacl")
	if err != nil {
		t.Skipf("the test grants read access by ACL with setfacl: %v", err)
	}
	if _, err := user.Lookup("nobody"); err != nil {
		t.Skipf("the test grants user nobody read access by ACL: %v", err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, -1, group.ID); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(setfacl, "-m", "u:nobody:r", path).CombinedOutput(); err != nil {
		t.Fatalf("setfacl -m u:nobody:r %s: %v\n%s", path, err, out)
	}
}

// Grant is what the read access to a file is made of: its mode, its owning
// group and its ACL, as the file's system.posix_acl_access extended
// attribute holds it (nil for none), and the file's inode, which a file
// replaced by another changes.
type Grant struct {
	Mode  os.FileMode
	Group int
	ACL   []byte
	Inode uint64
}

// GrantOf reads the Grant of the file at path.
func GrantOf(t *testing.T, path string) Grant {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	g := Grant{Mode: info.Mode(), Group: int(st.Gid), Inode: st.Ino}
	buf := make([]byte, 1024)
	switch n, err := syscall.Getxattr(path, aclAttribute, buf); {
	case err == nil:
		g.ACL = buf[:n]
	case !errors.Is(err, syscall.ENODATA):
		t.Fatalf("reading %s's %s: %v", path, aclAttribute, err)
	}
	return g
}

// aclAttribute is the extended attribute holding a file's access ACL
// (acl(5); xattr(7)).
const aclAttribute = "system.posix_acl_access"
