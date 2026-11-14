package support

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// SlowSync is how long each sync of file_path.new waits in a broker under
// SlowSnapshots: a snapshot then lasts at least that long, whatever the size
// of the database it copies.
const SlowSync = 2 * time.Second

// MemoryAndFileDisks returns memory_dir, a tmpfs of diskSize bytes, and the
// directory of a filesystem of fileDiskSize bytes for file_path, each
// mounted for the test alone by testkit.OnSmallDisks; "" for both where the
// test has instead been run again, alone, on them, and has nothing left to
// do. limit bounds that run.
func MemoryAndFileDisks(t *testing.T, limit time.Duration) (ram, disk string) {
	t.Helper()
	ram, disk = os.Getenv(SmallDisk), os.Getenv(FileDisk)
	if ram == "" {
		testkit.OnSmallDisks(t, limit, testkit.SmallDisk{Env: SmallDisk, Size: DiskSize}, testkit.SmallDisk{Env: FileDisk, Size: FileDiskSize})
	}
	return ram, disk
}

// SlowSnapshots is o, its folder a new one of the test's where o names
// none, with the broker run under testkit.SlowIO: every sync of
// file_path.new, Dir/saguin.db.new, waits slowSync, so each snapshot lasts
// at least that long (design.md, Store, memory mode: a snapshot "copies the
// RAM copy's database file [...] to file_path.new [...] It then syncs that file,
// renames it over file_path"). The broker's process is then strace's, and
// tracee names the broker's own.
func SlowSnapshots(t *testing.T, o *harness.Options) *harness.Options {
	t.Helper()
	own := *o // the caller's Options stay as they were
	if own.Dir == "" {
		own.Dir = t.TempDir()
	}
	own.Under = testkit.SlowIO(t, "sync", SlowSync, filepath.Join(own.Dir, "saguin.db.new"), filepath.Join(own.Dir, "strace.out"))
	return &own
}

// FileDisk names the environment variable that tells the test binary, run
// again by testkit.OnSmallDisks beside smallDisk's memory_dir, the directory
// of a second small filesystem: the one holding file_path, which a test
// fills, never the machine's disk nor memory_dir.
const FileDisk = "SAGUIN_CONFORMANCE_FILE_DISK"

// FileDiskSize is room for a new database, its sidecars, its lock and a
// snapshot of it, and little more, so a ballast fills it at once.
const FileDiskSize = 2 << 20
