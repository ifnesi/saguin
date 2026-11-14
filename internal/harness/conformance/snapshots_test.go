package conformance

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of memory mode's snapshots (design.md, Store, memory mode;
// Restart, Graceful stop; config.md, broker.storage: snapshot_interval;
// metrics.md, Store and memory: the snapshot series), each driving a broker
// in memory mode from outside: the wire, /health, /metrics, its exit and
// the files it leaves. Its memory_dir is a tmpfs of the test's own, mounted
// by testkit.OnSmallDisk (memoryDir), never the machine's /dev/shm.

// The snapshot series (metrics.md, Store and memory), as testkit.SeriesKey
// names them.

// tracee returns the process id of the broker b runs under strace
// (slowSnapshots): strace's one child. strace blocks the signals that would
// end it, so the broker is signalled by its own id.
func tracee(t *testing.T, b *harness.Broker) int {
	t.Helper()
	children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", b.PID(), b.PID()))
	if err != nil {
		t.Fatal(err)
	}
	ids := strings.Fields(string(children))
	if len(ids) != 1 {
		t.Fatalf("strace, process %d, has the children %q; want one, the broker", b.PID(), ids)
	}
	pid, err := strconv.Atoi(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// design.md, Store, memory mode: "A failed or interrupted snapshot leaves
// file_path holding either the previous complete snapshot or its complete
// replacement. A leftover file_path.new is removed before the next
// attempt"; "at a graceful stop, a snapshot copies the RAM copy's database
// file, byte for byte, to file_path.new, beside file_path. [...] It then
// syncs that file, renames it over file_path". storage.md, The schema
// version: a file SQLite finds damaged is refused, so file_path must pass
// SQLite's own integrity_check.
//
// A broker in full mode stores a durable session and stops. A broker in
// memory mode, memory_dir on a tmpfs, starts on its file_path and
// acknowledges left/1; then, as a crash may leave it, a file_path.new lies
// beside file_path: a file of bytes that are no database, or a symbolic
// link to file_path itself. The broker stops gracefully, exit status 0.
// file_path is then a regular file, passes integrity_check, and holds the
// session and left/1.
func TestLeftoverSnapshotFileNeverDamagesFilePath(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	for _, tc := range []struct {
		name     string
		leftover func(t *testing.T, path string) // makes file_path.new, given file_path
	}{
		{"a file of garbage", func(t *testing.T, path string) { garbageFile(t, path+".new") }},
		{"a link to file_path", func(t *testing.T, path string) {
			if err := os.Symlink(path, path+".new"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			full := support.Launch(t, &harness.Options{Dir: dir})
			support.Stored(t, full, "left-session")
			stopped(t, full)

			b := support.Launch(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram})
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "left-pub", CleanStart: true})
			support.Acknowledged(t, pub, support.Sized("left/1", 64))
			tc.leftover(t, b.DB)
			stopped(t, b)

			info, err := os.Lstat(b.DB)
			if err != nil {
				t.Fatal(err)
			}
			if !info.Mode().IsRegular() {
				t.Fatalf("after the stop, file_path %s has mode %v; want a regular file", b.DB, info.Mode())
			}
			db := support.FileAlone(t, b.DB)
			if check := pragma[string](t, db, "integrity_check"); check != "ok" {
				t.Errorf("file_path's integrity_check: %q; want ok", check)
			}
			if held, err := support.HoldsInFile(db, "left/1"); err != nil || !held {
				t.Errorf("file_path does not hold left/1 (%v)", err)
			}
			if n := sessionRows(t, db, "left-session"); n != 1 {
				t.Errorf("file_path holds %d sessions rows of left-session; want 1", n)
			}
		})
	}
}

// design.md, Store, memory mode: "A failed or interrupted snapshot leaves
// file_path holding either the previous complete snapshot or its complete
// replacement"; "Success is reported only once the rename and the directory
// are safe on disk"; "A restart in memory mode recovers the whole store
// [...] from file_path"; Restart, Graceful stop: "Memory mode takes its
// final snapshot, except after a store error".
//
// A broker in full mode stores a durable session and stops. A broker in
// memory mode, memory_dir on a tmpfs, under slowSnapshots, starts on its
// file_path, which is then the previous complete snapshot, and acknowledges
// mid/1. It is sent SIGTERM; once file_path.new exists, the final snapshot
// has begun copying, its sync waiting slowSync, and the broker is killed
// (SIGKILL). file_path is byte for byte the previous snapshot, and a broker
// in full mode starting on it serves the session, Session Present 1.
func TestKilledMidSnapshotRestartsFromThePreviousOne(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	full := support.Launch(t, &harness.Options{Dir: dir})
	support.Stored(t, full, "mid-session")
	stopped(t, full)

	b := support.Launch(t, support.SlowSnapshots(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram}))
	previous, err := os.ReadFile(b.DB)
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "mid-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized("mid/1", 64))
	broker := tracee(t, b)
	if err := syscall.Kill(broker, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if !testkit.Within(support.StopLimit, func() bool {
		_, err := os.Lstat(b.DB + ".new")
		return err == nil
	}) {
		t.Fatalf("no %s.new within %v of SIGTERM: the final snapshot did not begin", b.DB, support.StopLimit)
	}
	if err := syscall.Kill(broker, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	support.Exited(t, b) // strace ends once the broker has

	if now, err := os.ReadFile(b.DB); err != nil || !bytes.Equal(now, previous) {
		t.Errorf("after the kill mid-snapshot, file_path %s is not the previous snapshot (%v)", b.DB, err)
	}
	restarted := support.Launch(t, &harness.Options{Dir: dir})
	if !present(t, restarted.Addr, "mid-session") {
		t.Error("the restart from the previous snapshot has Session Present 0 for the session it holds")
	}
}

// design.md, Restart, Graceful stop: "The core waits until every store
// request it sent, read or write, is answered. Memory mode takes its final
// snapshot, except after a store error"; A stop for a failure: "The exit status is 0 after a stop
// requested by SIGINT or SIGTERM"; Store, memory mode: "A clean stop deletes
// the RAM copy once the final snapshot succeeds"; "An existing database may
// switch between modes"; full mode: "A restart recovers committed state".
// Flows, CONNECT, 9. Resume: "Consumers read on"; MQTT 5 4.1: a durable
// session's QoS 1 messages are delivered on return.
//
// A broker in memory mode, memory_dir on a tmpfs, holds a durable session
// subscribed to final/# at QoS 1, away, and acknowledges a publisher's
// three publications there. SIGTERM: the exit status is 0, and the RAM copy
// is gone. A broker in full mode on the same file_path serves the session,
// Session Present 1, which receives the three, in order.
func TestGracefulStopTakesTheFinalSnapshot(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram})
	subscriber := harness.Connect{ClientID: "final-sub", CleanStart: true, Props: support.Durable}
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, &subscriber, support.At(1, "final/#")))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "final-pub", CleanStart: true})
	ps := []*harness.Publish{support.Sized("final/1", 64), support.Sized("final/2", 64), support.Sized("final/3", 64)}
	support.PublishAll(t, pub, ps)
	stopped(t, b)
	if _, err := os.Stat(b.RAM); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("after the clean stop, the RAM copy %s: %v; want it deleted", b.RAM, err)
	}

	full := support.Launch(t, &harness.Options{Dir: dir})
	c := resumed(t, full.Addr, harness.V5, &subscriber)
	for _, p := range ps {
		d := support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
		if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
			t.Fatal(err)
		}
	}
}

// design.md, Store, memory mode: "A snapshot failure at a graceful stop
// exits with an error naming the RAM copy, and leaves it in place"; Restart:
// "The exit status is [...] 1 after a stop caused by a failure".
//
// A broker in memory mode, memory_dir on a tmpfs and file_path on a small
// filesystem of its own, acknowledges kept/1. A ballast takes every byte
// left on file_path's filesystem, so the final snapshot has no room for
// file_path.new. SIGTERM: the exit status is 1, the log names the RAM copy,
// and the RAM copy is still there, holding kept/1.
func TestFailedFinalSnapshotLeavesTheRAMCopy(t *testing.T) {
	t.Parallel()
	ram, disk := support.MemoryAndFileDisks(t, support.AloneLimit)
	if ram == "" {
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), StorageMode: "memory", MemoryDir: ram})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized("kept/1", 64))
	testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	if state := stop(t, b); state.ExitCode() != 1 {
		t.Errorf("the stop whose final snapshot had no room ended %v; want exit status 1", state)
	}
	if !strings.Contains(b.Log(), b.RAM) {
		t.Errorf("the log does not name the RAM copy %s:\n%s", b.RAM, b.Log())
	}
	if got := topicsOf(held(t, support.OpenDB(t, b.RAM), "$broadcast")); !slices.Equal(got, []string{"kept/1"}) {
		t.Errorf("after the failed final snapshot, the RAM copy holds the records of %q; want kept/1's", got)
	}
}
