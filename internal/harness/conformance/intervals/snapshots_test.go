// Package intervals holds the conformance tests that wait the design's
// one-minute intervals: memory mode's snapshot_interval, at least 1m
// (config.md, broker.storage), and the store-held metric values' refresh,
// every min_scrape_interval, at least 60s (config.md, broker.operations);
// and the bridges' waits of seconds and more: redials, keep-alive,
// subscription retries and the 30 seconds a SUBSCRIBE may go unanswered
// (design.md, Bridges).
// They run as a test package of their own, beside the conformance
// package's, so their waits pass while its tests run; they share its
// helpers through package support.
package intervals

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of memory mode's snapshots (design.md, Store, memory mode;
// Restart, Graceful stop; config.md, broker.storage: snapshot_interval;
// metrics.md, Store and memory: the snapshot series), each driving a broker
// in memory mode from outside: the wire, /health, /metrics, its exit and
// the files it leaves. Its memory_dir is a tmpfs of the test's own, mounted
// by testkit.OnSmallDisk (support.MemoryDir), never the machine's /dev/shm.

// snapshotInterval is broker.storage.snapshot_interval at its least
// (config.md: "memory mode's snapshot period, at least 1m"): a test that
// needs a periodic snapshot waits one, and runs beside the others that do
// (t.Parallel).
const snapshotInterval = time.Minute

// snapshotRAM is the size of the tmpfs a test publishing all through its
// snapshot interval has as memory_dir: room for the RAM copy and a
// write-ahead log of SQLite's 1,000 pages before it folds them in.
const snapshotRAM = 16 << 20

var (
	okSnapshots     = testkit.SeriesKey("saguin_snapshots_total", "result", "ok")
	failedSnapshots = testkit.SeriesKey("saguin_snapshots_total", "result", "failed")
	lastSuccess     = testkit.SeriesKey("saguin_snapshot_last_success_timestamp_seconds")
	lastDuration    = testkit.SeriesKey("saguin_snapshot_last_duration_seconds")
)

// snapshotChannel is an append channel holding snap/#: a new subscription to
// it starts at the floor (design.md, SUBSCRIBE: append "Starts with the
// floor"), so it is sent every record the channel holds, in offset order.
var snapshotChannel = harness.Channel{Name: "snapped", Type: "append", Filter: "snap/#"}

// unixSeconds is the time a metrics.md timestamp in seconds stands for.
func unixSeconds(s float64) time.Time {
	return time.Unix(0, int64(s*float64(time.Second)))
}

// seconds is a metrics.md duration in seconds as a time.Duration.
func seconds(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}

// awaitSnapshot waits until b's /metrics counts a snapshot in the series
// counted (okSnapshots or failedSnapshots), failing the test where it does
// not within limit, and returns the four snapshot series as that scrape
// read them.
func awaitSnapshot(t *testing.T, b *harness.Broker, counted string, limit time.Duration) map[string]float64 {
	t.Helper()
	var got map[string]float64
	if !testkit.Within(limit, func() bool {
		got = support.Sampled(t, b, okSnapshots, failedSnapshots, lastSuccess, lastDuration)
		return got[counted] >= 1
	}) {
		t.Fatalf("after %v, /metrics reads %v; want %s at 1", limit, got, counted)
	}
	return got
}

// awaitHealth waits until GET /health on b answers status, failing the test
// where it does not within harness.DefaultTimeout, the test's watchdog: the
// core answers it from the state it holds (design.md, Operations, HTTP),
// which a snapshot's answer changes as a later event than the metrics it
// moves (metrics.md, Rules). The answer's body is then checked
// (expectHealth).
func awaitHealth(t *testing.T, b *harness.Broker, status int, state string) {
	t.Helper()
	var a harness.OpsAnswer
	var err error
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		a, err = support.OpsTry(b, http.MethodGet, "/health", "")
		return err == nil && a.Status == status
	}) {
		t.Fatalf("GET /health answers %d %q (%v) after %v; want %d", a.Status, a.Body, err, harness.DefaultTimeout, status)
	}
	support.ExpectHealth(t, http.MethodGet, a, status, state)
}

// failedLimit bounds the run of a test waiting two snapshot intervals on a
// filesystem of its own: a backstop over its brokers' starts and stops and
// its own bounded waits.
const failedLimit = 2*snapshotInterval + support.AloneLimit

// design.md, Store, memory mode: "Every snapshot_interval [...] a snapshot
// copies the RAM copy's database file, byte for byte, to file_path.new
// [...] renames it over file_path"; "It holds exactly the commits before
// it"; "A restart in memory mode recovers the whole store, offsets included,
// from file_path"; "A start replaces any RAM copy left there, since a
// restart recovers from file_path". metrics.md, Store and memory:
// saguin_snapshot_last_success_timestamp_seconds, "when the last good
// snapshot finished"; saguin_snapshot_last_duration_seconds, "how long the
// last snapshot took". SUBSCRIBE: append "Starts with the floor"; Delivery,
// Order: deliveries follow "a channel's offset order".
//
// A broker in memory mode, memory_dir on a tmpfs, snapshot_interval 1m,
// acknowledges snap/before; its first snapshot then succeeds, having begun
// (its end less its duration, as /metrics reads them) after that PUBACK.
// Once /metrics counts it, snap/after is acknowledged, and the broker is
// killed (SIGKILL). A broker in memory mode on the same files acknowledges
// snap/restarted; a new subscription to snap/# is sent snap/before, then
// snap/restarted: snap/after, in the RAM copy the kill left, is gone.
func TestSnapshotHoldsWhatCameBeforeIt(t *testing.T) {
	t.Parallel() // it waits a snapshot interval beside the other tests that do
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	o := &harness.Options{Dir: t.TempDir(), StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval,
		Channels: []harness.Channel{snapshotChannel}}
	b := support.OpsLaunch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "snap-pub", CleanStart: true})
	before := support.Sized("snap/before", 64)
	support.Sent(t, pub, before)
	acked := support.Puback(t, pub, before)
	got := awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.Late)
	if began := unixSeconds(got[lastSuccess]).Add(-seconds(got[lastDuration])); !acked.Before(began) {
		t.Fatalf("the snapshot began at %v, before the PUBACK of snap/before at %v: the test cannot tell what it holds", began, acked)
	}
	support.Acknowledged(t, pub, support.Sized("snap/after", 64))
	support.Crash(t, b)

	b = support.Launch(t, o)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "snap-pub", CleanStart: true})
	restarted := support.Sized("snap/restarted", 64)
	support.Acknowledged(t, pub, restarted)
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "snap-sub", CleanStart: true}, support.At(1, "snap/#"))
	for _, p := range []*harness.Publish{before, restarted} {
		d := support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
		if err := sub.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
			t.Fatal(err)
		}
	}
}

// design.md, Store, memory mode: "Writes then resume and collect in the log
// while that store reader copies the file. Later writes are not held: they
// belong to a later snapshot"; Flows, PUBLISH: "1 | on PUBLISH | PUBACK once
// stored". metrics.md, Store and memory:
// saguin_snapshot_last_success_timestamp_seconds, "when the last good
// snapshot finished"; saguin_snapshot_last_duration_seconds, "how long the
// last snapshot took".
//
// A broker in memory mode, memory_dir on a tmpfs, snapshot_interval 1m, runs
// under slowSnapshots, so its snapshot lasts at least slowSync. A publisher
// sends one QoS 1 publication after another, each once the last is
// acknowledged, until /metrics counts the first snapshot. The snapshot,
// from its end less its duration to its end as /metrics reads them, lasted
// at least slowSync, and one publication sent after it began was
// acknowledged before it ended.
func TestPublishingGoesOnDuringASnapshot(t *testing.T) {
	t.Parallel() // it waits a snapshot interval beside the other tests that do
	ram := support.MemoryDir(t, snapshotRAM)
	if ram == "" {
		return
	}
	b := support.OpsLaunch(t, support.SlowSnapshots(t, &harness.Options{StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval}))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "during-pub", CleanStart: true})
	type exchange struct{ sent, acked time.Time }
	var exchanges []exchange
	var got map[string]float64
	limit := snapshotInterval + support.SlowSync + support.Late
	if !testkit.Within(limit, func() bool {
		p := support.Sized(fmt.Sprintf("during/%d", len(exchanges)), 64)
		e := exchange{sent: support.Sent(t, pub, p)}
		e.acked = support.Puback(t, pub, p)
		exchanges = append(exchanges, e)
		got = support.Sampled(t, b, okSnapshots, lastSuccess, lastDuration)
		return got[okSnapshots] == 1
	}) {
		t.Fatalf("after %v, /metrics reads %v; want %s at 1", limit, got, okSnapshots)
	}
	took := seconds(got[lastDuration])
	ended := unixSeconds(got[lastSuccess])
	began := ended.Add(-took)
	if took < support.SlowSync {
		t.Fatalf("the snapshot took %v, less than the %v its sync of file_path.new waited: the test cannot tell a publication sent during it", took, support.SlowSync)
	}
	if !slices.ContainsFunc(exchanges, func(e exchange) bool { return e.sent.After(began) && e.acked.Before(ended) }) {
		t.Errorf("of %d publications, none sent after the snapshot began, at %v, was acknowledged before it ended, at %v: writes waited for the snapshot",
			len(exchanges), began, ended)
	}
}

// design.md, Store, memory mode: "A failed or interrupted snapshot leaves
// file_path holding either the previous complete snapshot or its complete
// replacement. A leftover file_path.new is removed before the next attempt";
// "A failed snapshot is logged and counted, and makes /health unhealthy";
// R8: "A store error stops the broker, except a failed memory-mode
// snapshot"; Operations, HTTP: /health "returns 200 {"status":"ok"} [...]
// while [...] the last snapshot, if any yet, succeeded. Otherwise it returns
// 503 {"status":"unavailable"}". metrics.md, Store and memory:
// saguin_snapshots_total{result}, "ok or failed".
//
// A broker in memory mode, memory_dir on a tmpfs and file_path on a small
// filesystem of its own, snapshot_interval 1m, acknowledges failed/1. A
// ballast takes every byte left on file_path's filesystem: the first
// snapshot, with no room for file_path.new, counts failed, and /health
// answers 503; a connected client's PINGREQ is answered, and file_path is
// byte for byte as it was. Once the ballast is emptied, failed/2 is
// acknowledged: the next snapshot counts ok, /health answers 200, and
// file_path itself holds failed/1 and failed/2.
func TestFailedSnapshotKeepsThePreviousOne(t *testing.T) {
	t.Parallel() // it waits two snapshot intervals beside the other tests that wait
	ram, disk := support.MemoryAndFileDisks(t, failedLimit)
	if ram == "" {
		return
	}
	path := filepath.Join(disk, "saguin.db")
	b := support.OpsLaunch(t, &harness.Options{FilePath: path, StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval})
	stays, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "failed-stays", CleanStart: true})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "failed-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized("failed/1", 64))
	previous, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	awaitSnapshot(t, b, failedSnapshots, snapshotInterval+support.Late)
	awaitHealth(t, b, http.StatusServiceUnavailable, "unavailable")
	if err := stays.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, stays)
	if now, err := os.ReadFile(path); err != nil || !bytes.Equal(now, previous) {
		t.Errorf("after the failed snapshot, file_path %s is not as it was (%v); want the previous complete snapshot", path, err)
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.Acknowledged(t, pub, support.Sized("failed/2", 64))
	awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.Late)
	awaitHealth(t, b, http.StatusOK, "ok")
	db := support.FileAlone(t, path)
	for _, topic := range []string{"failed/1", "failed/2"} {
		if held, err := support.HoldsInFile(db, topic); err != nil || !held {
			t.Errorf("after the snapshot that succeeded, file_path itself does not hold %s (%v)", topic, err)
		}
	}
}

// metrics.md, Store and memory: saguin_snapshots_total{result}, "In memory
// mode, counts snapshots with result ok or failed.";
// saguin_snapshot_last_success_timestamp_seconds, "In memory mode, records
// when the last good snapshot finished.";
// saguin_snapshot_last_duration_seconds, "In memory mode, records how long
// the last snapshot took, failed or not."; Rules: "Every series exists from
// start [...] Counters start at 0". design.md, Store, memory mode: "Every
// snapshot_interval [...] a snapshot copies the RAM copy's database file",
// a snapshot being "skipped when nothing was committed since the last
// successful one".
//
// A broker in memory mode, memory_dir on a tmpfs, snapshot_interval 1m,
// under slowSnapshots, so its snapshot lasts at least slowSync, stores a
// durable session: /metrics then counts no snapshot, ok or failed. Once it
// counts the first, ok is 1 and failed 0, the last duration is at least
// slowSync, and the last success is at least that duration after the
// broker's start, and no later than the scrape that counted it.
func TestSnapshotSeriesFollowEachSnapshot(t *testing.T) {
	t.Parallel() // it waits a snapshot interval beside the other tests that do
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	launched := time.Now()
	b := support.OpsLaunch(t, support.SlowSnapshots(t, &harness.Options{StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval}))
	support.Stored(t, b, "series-session")
	if got := support.Sampled(t, b, okSnapshots, failedSnapshots); got[okSnapshots] != 0 || got[failedSnapshots] != 0 {
		t.Fatalf("before any snapshot, /metrics reads %v; want both counts 0", got)
	}
	got := awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.SlowSync+support.Late)
	counted := time.Now()
	took, ended := seconds(got[lastDuration]), unixSeconds(got[lastSuccess])
	switch {
	case got[okSnapshots] != 1 || got[failedSnapshots] != 0:
		t.Errorf("after the first snapshot, /metrics reads %v; want ok 1, failed 0", got)
	case took < support.SlowSync:
		t.Errorf("%s reads %v, less than the %v the snapshot's sync of file_path.new waited", lastDuration, took, support.SlowSync)
	case ended.Add(-took).Before(launched) || ended.After(counted):
		t.Errorf("%s reads %v, and %s %v: want a snapshot that began after the broker's start, %v, and ended by the scrape that counted it, %v",
			lastSuccess, ended, lastDuration, took, launched, counted)
	}
}

// design.md, Store, memory mode: "A snapshot, the final one included, is
// skipped when nothing was committed since the last successful one."
// Restart: "Sessions that expired while the broker was down end at start,
// each in one transaction"; "A Will without a surviving session is due
// immediately." Each case starts from a database made through MQTT. Only
// startup's work can dirty the RAM copy before the first interval.
func TestFirstSnapshotDependsOnStartupChanges(t *testing.T) {
	for _, kind := range []string{"idle", "expired session", "Will"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ram := support.MemoryDir(t, support.DiskSize)
			if ram == "" {
				return
			}
			dir := t.TempDir()
			full := support.Launch(t, &harness.Options{Dir: dir})
			var expired time.Time
			switch kind {
			case "expired session":
				c, _ := harness.Connected(t, full.Addr, harness.V5, &harness.Connect{ClientID: "startup-session", CleanStart: true,
					Props: support.SessionExpiry(1)})
				support.Away(t, c)
				db := support.OpenDB(t, full.DB)
				expired = support.AwaitAway(t, db, "startup-session").Add(time.Second)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			case "Will":
				harness.Connected(t, full.Addr, harness.V5, &harness.Connect{ClientID: "startup-will", CleanStart: true,
					Will: &harness.Will{Topic: "startup/will", Payload: support.WillPayload, Props: support.WillDelay(support.Hour)}})
			}
			if err := full.Stop(); err != nil {
				t.Fatal(err)
			}
			if !expired.IsZero() && !time.Now().Before(expired) {
				t.Fatal("the fixture stopped after its session expired; startup must be what ends it")
			}
			if !expired.IsZero() && !testkit.Within(harness.DefaultTimeout, func() bool { return !time.Now().Before(expired) }) {
				t.Fatal("the stored session's expiry did not arrive while the broker was down")
			}
			before, err := os.Stat(full.DB)
			if err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(full.DB)
			if err != nil {
				t.Fatal(err)
			}
			b := support.OpsLaunch(t, &harness.Options{Dir: dir, StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval})
			if kind == "idle" {
				// Watch for an unwanted snapshot through the first interval,
				// including scheduling allowance, rather than sleeping then
				// sampling once and possibly missing a failure.
				if testkit.Within(snapshotInterval+support.Late, func() bool {
					got := support.Sampled(t, b, okSnapshots, failedSnapshots)
					return got[okSnapshots]+got[failedSnapshots] != 0
				}) {
					t.Fatal("the first interval counted a snapshot with no committed change")
				}
				after, err := os.Stat(b.DB)
				if err != nil {
					t.Fatal(err)
				}
				now, err := os.ReadFile(b.DB)
				if err != nil || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) || !bytes.Equal(contents, now) {
					t.Fatalf("idle first interval rewrote file_path (%v)", err)
				}
				return
			}
			got := awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.Late)
			if got[okSnapshots] != 1 || got[failedSnapshots] != 0 {
				t.Fatalf("startup's %s: snapshot counts %v; want one success", kind, got)
			}
			disk := support.FileAlone(t, b.DB)
			if kind == "Will" {
				if held, err := support.HoldsInFile(disk, "startup/will"); err != nil || !held {
					t.Fatalf("snapshot lost startup's Will (%v)", err)
				}
			} else {
				var count int
				if err := disk.QueryRow(`SELECT count(*) FROM sessions WHERE client = 'startup-session'`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("snapshot has %d expired sessions (%v); want none", count, err)
				}
			}
		})
	}
}

// grantsKept fails the test unless the file at path, named what, is a new
// file since was read, its inode another, with was's mode and ACL, and its
// group too where group is set; it returns the file's Grant.
func grantsKept(t *testing.T, path string, was support.Grant, group bool, what string) support.Grant {
	t.Helper()
	now := support.GrantOf(t, path)
	switch {
	case now.Inode == was.Inode:
		t.Fatalf("after %s, %s is the same file as before it; want the snapshot renamed over it", what, path)
	case now.Mode != was.Mode:
		t.Errorf("after %s, %s has mode %v; want %v, the mode granted on it", what, path, now.Mode, was.Mode)
	case !bytes.Equal(now.ACL, was.ACL):
		t.Errorf("after %s, %s's ACL is %x; want %x, the ACL granted on it", what, path, now.ACL, was.ACL)
	case group && now.Group != was.Group:
		t.Errorf("after %s, %s's group is %d; want %d, the group granted on it", what, path, now.Group, was.Group)
	}
	return now
}

// design.md, Store, memory mode: "Every snapshot_interval and at a graceful
// stop, a snapshot copies the RAM copy's database file, byte for byte, to
// file_path.new, beside file_path. It gives that file file_path's mode,
// owning group and access control list (ACL), so the read access an
// operator granted on file_path survives the snapshot. It then syncs that
// file, renames it over file_path and syncs the directory"; The lock: "The
// broker never changes the mode, group or ACL of a file that exists, so an
// operator grants others read access through the file's own permissions or
// ACLs. A memory-mode snapshot replaces file_path with a new file that keeps
// all three, as memory mode above describes".
//
// A broker in memory mode, its memory_dir on the user's runtime tmpfs
// (support.RuntimeMemoryDir: no user namespace, so groups and users keep
// their ids), snapshot_interval 1m, makes its database at file_path. The
// operator grants read access on it (support.GrantRead): mode 0640, a group
// of the test's user's other than its own, and an ACL entry letting user
// nobody read it. The broker acknowledges a QoS 1 publish, and its first
// timed snapshot succeeds: file_path is a new file, with the mode, group and
// ACL granted. It acknowledges another and stops, exit status 0, taking its
// final snapshot: file_path is a new file again, with the same mode, group
// and ACL.
func TestSnapshotKeepsTheReadAccessGranted(t *testing.T) {
	t.Parallel() // it waits a snapshot interval beside the other tests that do
	group := support.OtherGroup(t)
	ram := support.RuntimeMemoryDir(t)
	dir := t.TempDir()
	db := filepath.Join(dir, "saguin.db")
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, FilePath: db, StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval})
	support.GrantRead(t, db, group)
	granted := support.GrantOf(t, db)
	if granted.ACL == nil {
		t.Fatalf("%s holds no ACL after setfacl; the test cannot see one kept", db)
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "grants-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized("snap/timed", 64))
	awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.Late)
	timed := grantsKept(t, db, granted, true, "the timed snapshot")
	support.Acknowledged(t, pub, support.Sized("snap/stopped", 64))
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	grantsKept(t, db, timed, true, "the stop's snapshot")
}

// design.md, Store, memory mode: "A process can give a file only a group
// its user belongs to"; "When file_path's group changes while the broker
// runs to one it cannot give, the snapshot still replaces file_path, with
// its mode and ACL, and the broker logs one WARN line naming file_path and
// the group it could not keep. The new file then has the broker's own
// group, so later snapshots keep that group and log nothing about it";
// "Example: an operator gives file_path group backup while the broker runs;
// the next snapshot logs that it could not keep backup, and the snapshots
// after it are silent"; "Every snapshot_interval and at a graceful stop, a
// snapshot copies the RAM copy's database file". Logging: lines are written
// "on standard error".
//
// A broker in memory mode, memory_dir on the user's runtime tmpfs,
// snapshot_interval 1m, makes its database at file_path. While it runs,
// file_path is given a group the broker's user does not belong to
// (support.GiveForeignGroup), mode 0640 and an ACL entry letting user
// nobody read it. The broker acknowledges a QoS 1 publish, and its first
// timed snapshot succeeds: file_path is a new file, with the mode and ACL
// granted and a group the broker's user belongs to, and the log holds one
// WARN line naming file_path, which names the group too. It acknowledges
// another and stops, exit status 0, taking its final snapshot: file_path
// is a new file again, with that mode and ACL and a group of the broker's
// user's, and still one WARN line names file_path, the stop's snapshot
// logging nothing about the group.
//
// Only a process with the power to give a file a group beyond its own can
// make that change, and the broker it starts must lack it: the test runs
// where its own process has CAP_CHOWN and is not root.
func TestSnapshotOfAGroupItCannotGive(t *testing.T) {
	t.Parallel() // it waits a snapshot interval beside the other tests that do
	if os.Geteuid() == 0 {
		t.Skip("a broker running as root can give a file any group, so no group is one it cannot give; the test needs a test process with CAP_CHOWN that is not root")
	}
	ram := support.RuntimeMemoryDir(t)
	dir := t.TempDir()
	db := filepath.Join(dir, "saguin.db")
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, FilePath: db, StorageMode: "memory", MemoryDir: ram, SnapshotInterval: snapshotInterval})
	foreign := support.GiveForeignGroup(t, db)
	support.GrantRead(t, db, foreign)
	granted := support.GrantOf(t, db)
	// warned waits until n WARN lines name db, failing the test unless each
	// names the group too.
	warned := func(n int, after string) {
		t.Helper()
		var lines []string
		if !testkit.Within(harness.DefaultTimeout, func() bool {
			lines = testkit.LinesNaming(b.Log(), "WARN", db)
			return len(lines) >= n
		}) || len(lines) != n {
			t.Fatalf("after %s, WARN lines naming %s: %q; want %d; the log:\n%s", after, db, lines, n, b.Log())
		}
		for _, line := range lines {
			if !slices.ContainsFunc(foreign.Names(), func(name string) bool { return strings.Contains(line, name) }) {
				t.Errorf("the WARN line %q names %s but not the group it could not keep, %v", line, db, foreign.Names())
			}
		}
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "foreign-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized("snap/timed", 64))
	awaitSnapshot(t, b, okSnapshots, snapshotInterval+support.Late)
	// own fails the test unless the file a snapshot left is in a group the
	// broker's user, the test's own, belongs to.
	own := func(g support.Grant, after string) {
		t.Helper()
		if !slices.Contains(support.OwnGroups(t), g.Group) {
			t.Errorf("after %s, %s's group is %d; want one of the broker's user's, %v", after, db, g.Group, support.OwnGroups(t))
		}
	}
	timed := grantsKept(t, db, granted, false, "the timed snapshot")
	own(timed, "the timed snapshot")
	warned(1, "the timed snapshot")
	support.Acknowledged(t, pub, support.Sized("snap/stopped", 64))
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	own(grantsKept(t, db, timed, false, "the stop's snapshot"), "the stop's snapshot")
	warned(1, "the stop's snapshot")
}
