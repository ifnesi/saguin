package conformance

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the store's normal and memory modes (design.md, Store;
// config.md, broker.storage: mode, sync_interval and memory_dir), each
// driving the broker from outside: from the wire, its exit and the files it
// writes. A memory-mode broker's memory_dir is a tmpfs of the test's own,
// mounted by testkit.OnSmallDisk, never the machine's /dev/shm. Its RAM copy
// is harness.Broker.RAM: broker.id harness's, harness.db.

// syncInterval leaves time to observe dirty log pages after a PUBACK,
// before the next timer sync (design.md, Store, normal mode).
const syncInterval = time.Second

// unsynced returns how many pages of the file at path the page cache holds
// with writes the disk has not yet taken: dirty, or being written back, as
// cachestat(2) counts them. A file fdatasync has just synced has none until
// it is written again; the kernel writes a dirty page back of its own only
// once it has been dirty for vm.dirty_expire_centisecs, 30 s by default.
func unsynced(t *testing.T, path string) uint64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }() // opened to read: nothing to lose
	var whole unix.CachestatRange    // a length of 0 reaches the file's end
	var stat unix.Cachestat_t
	if err := unix.Cachestat(uint(f.Fd()), &whole, &stat, 0); err != nil {
		t.Fatalf("cachestat(2) of %s: %v", path, err)
	}
	return stat.Dirty + stat.Writeback
}

// design.md, Store, normal mode: "Every sync_interval, counted from the end
// of the previous sync, the core's timer asks the writer for a sync if
// anything was committed since"; "The writer syncs the log file itself
// (fdatasync), which writes to the disk only the commits since the previous
// sync, whatever reads are open". config.md, broker.storage: sync_interval,
// "normal mode: how often committed changes are synced to the disk, at
// least 100ms".
//
// A broker in normal mode, with sync_interval one second, serves. The test begins
// a read of its database, which stays open, and the broker then
// acknowledges a QoS 1 publication, then a second, the read still open.
// Each PUBACK leaves dirty log pages: the commit did not wait for disk.
// Within sync_interval of each PUBACK, and late for the scheduling, no page
// of the -wal holds a write the disk has not taken (unsynced). Nothing but
// the broker's sync can do that within the wait: synchronous=NORMAL writes a
// commit's log frames without a sync; the test's read keeps SQLite from
// folding the log, and so from the sync a fold begins with; and the kernel
// writes a dirty page back of its own only after 30 s by default. The second
// publication is what a sync made by folding the log fails: a fold may still
// sync the first commit's frames, the read not yet holding them back, but
// never the second's. A filesystem showing no unsynced page would prove
// nothing: the test first sees its own write unsynced beside the database.
// When the sync comes is the core's to test (internal/core, sync_test.go).
func TestNormalModeSyncsTheLogWhateverReadsAreOpen(t *testing.T) {
	b := support.Launch(t, &harness.Options{StorageMode: "normal", SyncInterval: syncInterval})
	if own := testkit.WriteFile(t, filepath.Dir(b.DB), "unsynced", strings.Repeat("x", 4096)); unsynced(t, own) == 0 {
		t.Fatalf("the test's own write to %s shows no unsynced page: this filesystem cannot show a sync", own)
	}
	conn, err := support.OpenDB(t, b.DB).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // the read ends with the test
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // a read: nothing to keep
	sessionRows(t, tx, "synced-pub")        // the read begins, and holds its view

	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "synced-pub", CleanStart: true})
	wal := b.DB + "-wal"
	for _, topic := range []string{"synced/1", "synced/2"} {
		support.Acknowledged(t, pub, support.Sized(topic, 64))
		if dirty := unsynced(t, wal); dirty == 0 {
			t.Fatalf("PUBACK on %s left no unsynced log pages: the commit waited for disk, as in full mode", topic)
		}
		var left uint64
		if !testkit.Within(syncInterval+support.Late, func() bool { left = unsynced(t, wal); return left == 0 }) {
			t.Errorf("%v after the PUBACK on %s, the test's read open, %d pages of %s hold writes the disk has not taken; want the log synced",
				syncInterval+support.Late, topic, left, wal)
		}
	}
}

// design.md, Store, memory mode: "It then checkpoints the disk database and
// copies its file, byte for byte, to <id>.db in memory_dir, named after
// broker.id. This RAM copy is readable and writable by the broker's user
// alone (0600)"; "Startup closes every connection it opened to the disk
// database and runs on the RAM copy"; "Every snapshot_interval and at a
// graceful stop, a snapshot copies the RAM copy's database file [...] to
// file_path.new"; config.md, broker.storage: snapshot_interval, default 30m.
//
// A broker in memory mode, memory_dir on a tmpfs, starts on a new
// file_path. Once it listens, a durable session subscribes to ram/# at QoS
// 1, a publisher's publication there is acknowledged and delivered, and a
// second durable session connects. Throughout, file_path stays byte for
// byte as it was when the broker began to listen, no snapshot being due for
// half an hour. The RAM copy, harness.db in memory_dir, is a file of mode
// 0600 and holds the record and both sessions.
func TestMemoryModeServesFromItsRAMCopy(t *testing.T) {
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	b := support.Launch(t, &harness.Options{StorageMode: "memory", MemoryDir: ram})
	before, err := os.ReadFile(b.DB)
	if err != nil {
		t.Fatalf("the database on disk, once the broker listens: %v", err)
	}
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ram-sub", CleanStart: true, Props: support.Durable}, support.At(1, "ram/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ram-pub", CleanStart: true})
	p := support.Sized("ram/1", 64)
	support.Acknowledged(t, pub, p)
	d := support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	if err := sub.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	support.Stored(t, b, "ram-second")

	if after, err := os.ReadFile(b.DB); err != nil || !bytes.Equal(after, before) {
		t.Errorf("serving clients in memory mode changed file_path %s (%v)", b.DB, err)
	}
	info, err := os.Stat(b.RAM)
	if err != nil {
		t.Fatalf("the RAM copy: %v", err)
	}
	if mode := info.Mode(); !mode.IsRegular() || mode.Perm() != 0o600 {
		t.Errorf("the RAM copy %s has mode %v; want a file of mode 0600", b.RAM, mode)
	}
	db := support.OpenDB(t, b.RAM)
	if got := topicsOf(held(t, db, "$broadcast")); !slices.Equal(got, []string{p.Topic}) {
		t.Errorf("the RAM copy holds the records of %q; want %s's", got, p.Topic)
	}
	for _, id := range []string{"ram-sub", "ram-second"} {
		if n := sessionRows(t, db, id); n != 1 {
			t.Errorf("the RAM copy holds %d sessions rows of %s; want 1", n, id)
		}
	}
}

// design.md, Store, memory mode: "memory mode validates file_path at start
// and lets SQLite recover its committed log. It then checkpoints the disk
// database and copies its file, byte for byte, to <id>.db in memory_dir";
// "runs on the RAM copy". Flows, CONNECT, 9. Resume: "Consumers read on";
// MQTT 5 4.1: a durable session's QoS 1 messages are delivered on return.
//
// A broker in full mode holds a durable session subscribed to log/# at QoS
// 1, away, and acknowledges a publisher's three publications there; killed
// (SIGKILL), it leaves them committed in file_path's -wal alone. A broker in
// memory mode, memory_dir on a tmpfs, starts on the same file_path: the
// session is there, Session Present 1, and receives the three, in order, and
// the RAM copy holds their records.
func TestMemoryModeRecoversTheDiskLog(t *testing.T) {
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	full := support.Launch(t, &harness.Options{Dir: dir})
	subscriber := harness.Connect{ClientID: "log-sub", CleanStart: true, Props: support.Durable}
	support.Away(t, support.Subscribed(t, full.Addr, harness.V5, &subscriber, support.At(1, "log/#")))
	pub, _ := harness.Connected(t, full.Addr, harness.V5, &harness.Connect{ClientID: "log-pub", CleanStart: true})
	ps := []*harness.Publish{support.Sized("log/1", 64), support.Sized("log/2", 64), support.Sized("log/3", 64)}
	support.PublishAll(t, pub, ps)
	support.Crash(t, full)
	if wal, ok := fileIfThere(t, full.DB+"-wal"); !ok || len(wal) == 0 {
		t.Fatalf("the crash left no -wal holding commits beside %s", full.DB)
	}

	b := support.Launch(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram})
	c := resumed(t, b.Addr, harness.V5, &subscriber)
	topics := make([]string, len(ps))
	for i, p := range ps {
		d := support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
		if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
			t.Fatal(err)
		}
		topics[i] = p.Topic
	}
	if got := topicsOf(held(t, support.OpenDB(t, b.RAM), "$broadcast")); !slices.Equal(got, topics) {
		t.Errorf("the RAM copy holds the records of %q; want %q", got, topics)
	}
}

// design.md, Store, memory mode: "It then checkpoints the disk database and
// copies its file"; "An incomplete checkpoint refuses startup. Sagüin never
// deletes a log holding commits not yet checkpointed"; Restart: "Listeners
// and bridges start last"; "The exit status is [...] 1 after [...] a refused
// start". SQLite folds a frame of the log into the database file only where
// no open read still needs the page the file holds in its place: a read
// begun before a commit holds that commit's frames in the log.
//
// A broker in full mode holds a durable session. The test begins a read
// on file_path, which stays open, and the broker then acknowledges a
// publisher's three publications; killed (SIGKILL), it leaves them committed
// in the -wal. A broker in memory mode, memory_dir on a tmpfs, starting on
// the same file_path cannot fold those frames past the test's read: its
// start is refused, exit status 1, before any listener opens, and the -wal
// is still there, byte for byte as the crash left it. Once the test's read
// ends, the same start serves, its RAM copy holding the three records: the
// read alone refused it.
func TestMemoryStartRefusedByAnIncompleteFold(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	full := support.Launch(t, &harness.Options{Dir: dir})
	support.Stored(t, full, "fold-before")
	conn, err := support.OpenDB(t, full.DB).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // the read ends with the test
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // a read: nothing to keep
	if n := sessionRows(t, tx, "fold-before"); n != 1 {
		t.Fatalf("the test's read finds %d sessions rows of fold-before; want 1", n)
	}
	pub, _ := harness.Connected(t, full.Addr, harness.V5, &harness.Connect{ClientID: "fold-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("fold/1", 64), support.Sized("fold/2", 64), support.Sized("fold/3", 64)})
	support.Crash(t, full)
	wal, ok := fileIfThere(t, full.DB+"-wal")
	if !ok || len(wal) == 0 {
		t.Fatalf("the crash left no -wal holding commits beside %s", full.DB)
	}

	o := &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram}
	err = support.Started(t, o).Listening()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Errorf("the start in memory mode with the log's commits held by a read: %v; want it refused, exit status 1, before listening", err)
	}
	if after, ok := fileIfThere(t, full.DB+"-wal"); !ok || !bytes.Equal(after, wal) {
		t.Error("the refused start removed or changed the -wal holding commits not yet checkpointed")
	}

	// The read ends: the same start folds the log, and its RAM copy holds
	// the three records.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	b := support.Launch(t, o)
	if got := topicsOf(held(t, support.OpenDB(t, b.RAM), "$broadcast")); !slices.Equal(got, []string{"fold/1", "fold/2", "fold/3"}) {
		t.Errorf("once the test's read ended, the start in memory mode holds the records of %q in its RAM copy; want fold/1 to fold/3", got)
	}
}

// design.md, Store, memory mode: "It then checkpoints the disk database and
// copies its file, byte for byte, to <id>.db in memory_dir"; "A refused
// start deletes the RAM copy it made, then releases its locks"; Restart:
// "The exit status is [...] 1 after [...] a refused start".
//
// A broker in full mode stores a durable session and stops, file_path
// holding it. A ballast takes all but half file_path's size of a memory_dir
// on a tmpfs of its own: room to begin the copy, never to end it. A broker
// in memory mode starting on the same file_path is refused, exit status 1,
// and leaves in memory_dir no RAM copy, harness.db, and none of its -wal,
// -shm and -journal. Once the ballast is emptied, the same start serves,
// the session there: room alone refused it.
func TestRefusedMemoryStartDeletesItsRAMCopy(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	full := support.Launch(t, &harness.Options{Dir: dir})
	support.Stored(t, full, "refused-start")
	stopped(t, full)
	ballast := testkit.FillDisk(t, filepath.Join(ram, "ballast"))
	filled, err := ballast.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := ballast.Truncate(filled.Size() - testkit.FileSize(t, full.DB)/2); err != nil {
		t.Fatal(err)
	}

	o := &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram}
	err = support.Started(t, o).Listening()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Fatalf("the start in memory mode with no room for the RAM copy: %v; want it refused, exit status 1", err)
	}
	for _, name := range []string{"harness.db", "harness.db-wal", "harness.db-shm", "harness.db-journal"} {
		if _, err := os.Lstat(filepath.Join(ram, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the refused start left %s in memory_dir %s (%v); want the RAM copy it made deleted", name, ram, err)
		}
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	b := support.Launch(t, o)
	if !present(t, b.Addr, "refused-start") {
		t.Error("once memory_dir had room, CONNECT with Clean Start 0 has Session Present 0: the session file_path holds is gone")
	}
}

// tmpfsMagic is the type statfs(2) gives a tmpfs (TMPFS_MAGIC).
const tmpfsMagic = 0x01021994

// design.md, Store, memory mode: "A missing memory_dir is created, readable
// only by the broker's user (0700). The memory_dir must be a tmpfs, which
// startup checks"; Restart: "The exit status is [...] 1 after [...] a
// refused start".
//
// A broker in memory mode whose memory_dir is a folder on the disk, not a
// tmpfs, is refused, exit status 1, before listening. One whose memory_dir
// is missing, below a tmpfs, starts and serves a client; memory_dir is then
// a folder of mode 0700.
func TestMemoryDirMustBeATmpfs(t *testing.T) {
	t.Run("a folder on the disk", func(t *testing.T) {
		onDisk := t.TempDir()
		var fs syscall.Statfs_t
		if err := syscall.Statfs(onDisk, &fs); err != nil {
			t.Fatal(err)
		}
		if fs.Type == tmpfsMagic {
			t.Fatalf("the test's folder %s is on a tmpfs: it cannot stand for a folder on the disk", onDisk)
		}
		err := support.Started(t, &harness.Options{StorageMode: "memory", MemoryDir: onDisk}).Listening()
		if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
			t.Errorf("the start with memory_dir %s on the disk: %v; want it refused, exit status 1", onDisk, err)
		}
	})
	t.Run("a missing folder", func(t *testing.T) {
		ram := support.MemoryDir(t, support.DiskSize)
		if ram == "" {
			return
		}
		missing := filepath.Join(ram, "saguin")
		b := support.Launch(t, &harness.Options{StorageMode: "memory", MemoryDir: missing})
		harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "made-dir", CleanStart: true})
		info, err := os.Stat(missing)
		if err != nil {
			t.Fatalf("memory_dir once the broker started: %v", err)
		}
		if mode := info.Mode(); !mode.IsDir() || mode.Perm() != 0o700 {
			t.Errorf("memory_dir %s has mode %v; want a folder of mode 0700", missing, mode)
		}
	})
}

// design.md, Store, memory mode: "A missing file_path is first created as
// in full mode, then copied like any other"; "It then checkpoints the disk
// database and copies its file"; "Startup closes every connection it opened
// to the disk database". storage.md, The schema version: "A missing file
// [...] is new and is created with the schema below"; "The schema version is
// SQLite's own user_version: 21".
//
// A broker in memory mode, memory_dir on a tmpfs, starts on a file_path that
// does not exist. Once it listens, the file itself (fileAlone) holds
// storage.md's tables at version 21.
func TestMemoryModeCreatesAMissingFile(t *testing.T) {
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	path := filepath.Join(t.TempDir(), "saguin.db")
	support.Launch(t, &harness.Options{FilePath: path, StorageMode: "memory", MemoryDir: ram})
	db := support.FileAlone(t, path)
	if n := schemaObjects(t, db, "table"); n != len(testkit.Tables) {
		t.Errorf("file_path holds %d tables; want storage.md's %d", n, len(testkit.Tables))
	}
	if v := pragma[int](t, db, "user_version"); v != 21 {
		t.Errorf("file_path's user_version: got %d, want 21", v)
	}
}

// design.md, Store, memory mode: "A start replaces any RAM copy left there,
// since a restart recovers from file_path"; "copies its file, byte for byte,
// to <id>.db in memory_dir".
//
// A broker in full mode stores a durable session and stops. A file of bytes
// that are no database lies at the RAM copy's name, harness.db, in a
// memory_dir on a tmpfs. A broker in memory mode starting on the same
// file_path serves: the session is there, Session Present 1, and the RAM
// copy holds its row.
func TestMemoryStartReplacesALeftoverRAMCopy(t *testing.T) {
	t.Parallel()
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	dir := t.TempDir()
	full := support.Launch(t, &harness.Options{Dir: dir})
	support.Stored(t, full, "leftover")
	stopped(t, full)
	garbageFile(t, filepath.Join(ram, "harness.db"))

	b := support.Launch(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram})
	if !present(t, b.Addr, "leftover") {
		t.Error("after the start in memory mode, CONNECT with Clean Start 0 has Session Present 0: the session file_path holds is gone")
	}
	if n := sessionRows(t, support.OpenDB(t, b.RAM), "leftover"); n != 1 {
		t.Errorf("the RAM copy holds %d sessions rows of leftover; want 1", n)
	}
}

// design.md, Store, memory mode: "Startup first takes a live exclusive lock
// on <id>.db.lock beside it, as the storage lock is held. If another process
// holds it, startup is refused, naming the lock"; "A start replaces any RAM
// copy left there"; Restart: "The exit status is [...] 1 after [...] a
// refused start".
//
// A broker in memory mode, memory_dir on a tmpfs, holds a durable session,
// connected. A second broker in memory mode, with the same broker.id and
// memory_dir but a file_path and a listener of its own, so that only the
// RAM copy's lock can refuse it, exits with status 1, naming harness.db.lock
// in memory_dir. The first broker still answers its client's PINGREQ and
// stores another durable session, and its RAM copy, at the same path, holds
// both sessions.
func TestASecondMemoryBrokerIsRefusedByTheRAMLock(t *testing.T) {
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	first := support.Launch(t, &harness.Options{StorageMode: "memory", MemoryDir: ram})
	c := support.Stored(t, first, "ram-held")

	lock := filepath.Join(ram, "harness.db.lock")
	err := support.Started(t, &harness.Options{StorageMode: "memory", MemoryDir: ram}).Listening()
	switch exit, ok := errors.AsType[*exec.ExitError](err); {
	case err == nil:
		t.Errorf("a second broker started on memory_dir %s, which a running broker of the same broker.id holds", ram)
	case !ok || exit.ExitCode() != 1:
		t.Errorf("the second broker's start: %v; want it refused, exit status 1", err)
	case !strings.Contains(err.Error(), lock):
		t.Errorf("the refusal does not name the lock %s:\n%v", lock, err)
	}

	harness.SendBytes(t, c, pingreq)
	harness.Expect[*harness.Pingresp](t, c)
	support.Stored(t, first, "ram-after")
	db := support.OpenDB(t, first.RAM)
	for _, id := range []string{"ram-held", "ram-after"} {
		if n := sessionRows(t, db, id); n != 1 {
			t.Errorf("the first broker's RAM copy holds %d sessions rows of %s; want 1", n, id)
		}
	}
}

// design.md, Store, A full disk: "On a full disk or a full memory_dir, a
// write is answered "full" only when SQLite reports lack of space and
// rollback confirms that the transaction did not commit. The broker
// continues after that answer"; "Publishes are refused with 0x97"; Flows,
// PUBLISH: "0x97 | [...] a full disk".
//
// A broker in memory mode, its memory_dir a tmpfs of its own and file_path
// on the disk, serves a publisher and a durable session, connected; a
// ballast then takes every byte left in memory_dir. A noisy publication of
// fullRAMRecord bytes, whose frames the log has no room for, gets PUBACK
// 0x97, and the connected client's PINGREQ is answered. Once the ballast is
// emptied, the same publication gets PUBACK 0x00.
func TestFullMemoryDirWhileServing(t *testing.T) {
	const fullRAMRecord = 64 << 10
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	b := support.Launch(t, &harness.Options{StorageMode: "memory", MemoryDir: ram})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ram-full-pub", CleanStart: true})
	stays := support.Stored(t, b, "ram-full-stays")
	ballast := testkit.FillDisk(t, filepath.Join(ram, "ballast"))

	answered(t, pub, support.Noisy(t, "ram/full", fullRAMRecord), 0x97)
	if err := stays.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, stays)

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.Acknowledged(t, pub, support.Noisy(t, "ram/full", fullRAMRecord))
}
