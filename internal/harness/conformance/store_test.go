package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// present connects id with Clean Start 0 to the broker at addr, as MQTT 5,
// and returns the CONNACK's Session Present: MQTT 5 3.2.2.1.1, with Clean
// Start 0 and Session State for the ClientID, "it MUST set Session Present
// to 1" [MQTT-3.2.2-3], and otherwise 0 [MQTT-3.2.2-2].
func present(t *testing.T, addr, id string) bool {
	t.Helper()
	_, ack := harness.Connected(t, addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable})
	return ack.SessionPresent
}

// design.md, Flows, Replies: "CONNACK (when it changes a session) [...] sent
// only once the store confirms"; PUBLISH: "1 | on PUBLISH | PUBACK once
// stored"; Store, full mode: "Everything acknowledged survives a crash or a
// power cut. A restart recovers committed state"; normal mode: "A crash of
// the broker loses nothing acknowledged"; "A restart recovers what reached
// the disk". Flows, CONNECT, 9. Resume: "Consumers read on"; Choices MQTT
// leaves open: QoS 0 messages for away sessions are "delivered on return",
// and so, MQTT 5 4.1, are those of QoS 1 a durable session is owed.
//
// In full mode, and in normal mode with a sync_interval of an hour, which
// no sync reaches before the crash, a durable session subscribes to
// acknowledged/# at QoS 1 and goes away, and a publisher's three
// publications there are acknowledged. The broker is killed (SIGKILL) at
// once, and started again on the same files. The session is there, Session
// Present 1, and receives the three, in order.
func TestAcknowledgedSessionSurvivesACrash(t *testing.T) {
	for _, o := range []harness.Options{
		{StorageMode: "full"},
		{StorageMode: "normal", SyncInterval: time.Hour},
	} {
		t.Run(o.StorageMode, func(t *testing.T) {
			o.Dir = t.TempDir()
			b := support.Launch(t, &o)
			subscriber := harness.Connect{ClientID: "acknowledged", CleanStart: true, Props: support.Durable}
			support.Away(t, support.Subscribed(t, b.Addr, harness.V5, &subscriber, support.At(1, "acknowledged/#")))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "acknowledged-pub", CleanStart: true})
			ps := []*harness.Publish{support.Sized("acknowledged/1", 64), support.Sized("acknowledged/2", 64), support.Sized("acknowledged/3", 64)}
			support.PublishAll(t, pub, ps)
			support.Crash(t, b)

			again := support.Launch(t, &o)
			c := resumed(t, again.Addr, harness.V5, &subscriber)
			for _, p := range ps {
				d := support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
				if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// design.md, R8: "Recoverable full responses, fatal errors and recovery are
// defined in Store"; Store, A full disk: "a write is answered "full" only
// when SQLite reports lack of space and rollback confirms that the
// transaction did not commit. The broker continues after that answer"; "Group
// refusal follows Writes. [...] Changes made by
// CONNECT, SUBSCRIBE or UNSUBSCRIBE are refused with that packet's code. A
// write request answered "full" is discarded. The core does not retain its
// body for retry."; CONNECT's refusals: "0x97 | a full
// disk | 3"; DISCONNECT: the session expiry a client changes "is stored
// before the connection closes (on a full disk, the change is lost, and the
// session stays as stored until the next start)".
//
// On a filesystem of its own, a broker holds a durable session, connected;
// a ballast then takes every byte left. A CONNECT of a new durable session
// is refused, 0x97 for MQTT 5 and return code 3 for MQTT 3.1.1, and closed
// (CONNECT, 2: "A failure gets a CONNACK with its reason, then the connection
// closes"); the broker still answers the connected client's PINGREQ. That
// client's DISCONNECT changing its session expiry to 0, which would end the
// session, is lost. Once the ballast is emptied, the same CONNECTs are
// accepted, and the session is still there.
func TestFullDiskWhileServing(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	stays := support.Stored(t, b, "stays")
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	connects := []struct {
		name    string
		version byte
		connect harness.Connect // a new durable session: MQTT 3.1.1's Clean Session 0 is one
		refused byte
	}{
		{"MQTT 5", harness.V5, harness.Connect{ClientID: "new-5", CleanStart: true, Props: support.Durable}, 0x97},
		{"MQTT 3.1.1", harness.V311, harness.Connect{ClientID: "new-3.1.1"}, 3},
	}
	for _, tc := range connects {
		connect := tc.connect
		connackRefuses(t, b.Addr, tc.version, &connect, tc.refused)
	}
	if err := stays.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, stays)
	if err := stays.Send(&harness.Disconnect{Props: support.SessionExpiry(0)}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, stays)

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	for _, tc := range connects {
		connect := tc.connect
		harness.Connected(t, b.Addr, tc.version, &connect)
	}
	if !present(t, b.Addr, "stays") {
		t.Error("the DISCONNECT that changed the session expiry to 0 on a full disk ended the session: the change was not lost")
	}
}

// spilling is the size of a publication's record larger than SQLite's page
// cache, which the broker leaves at SQLite's default, 2,000 KiB (design.md,
// Store: "Sagüin only sets journal_size_limit"): SQLite writes some of its
// pages to the log while its statements run, before any commit. Its
// publications are noisy, so stored at that size, compression unable to
// make them shorter (storage.md, A stored payload).
const spilling = 3 << 20

// spillingDiskSize is room for a new database and its log with a few
// publications of spilling bytes.
const spillingDiskSize = 8 << 20

// leftRoom is the room a ballast leaves: frames of the log enough for a
// session's change of a few pages, never for a publication of spilling
// bytes.
const leftRoom = 128 << 10

// design.md, Store, Writes: "A full refusal rolls back the whole group and
// answers every member "full", including members not executed"; "The group
// is due at that deadline, or at the end of the event in which a write that
// is not a publication joins it"; A full disk: "a write is answered "full"
// only when SQLite reports lack of space and rollback confirms that the
// transaction did not commit. The broker continues after that answer";
// "Group refusal follows Writes. Publishes are
// refused with 0x97. Changes made by CONNECT, SUBSCRIBE or UNSUBSCRIBE are
// refused with that packet's code"; Flows, PUBLISH: "0x97 | [...] a full
// disk"; UNSUBSCRIBE: "A full disk refuses it with 0x80 on each filter";
// R4: "A reply confirming a change waits for that change's commit"; Store, full
// mode: "A restart recovers committed state". metrics.md:
// saguin_store_full, "1 while the disk is full: from a write SQLite could
// not save for room until the next saved one".
//
// On a filesystem of its own, a broker collects for publish_commit_interval
// 1s. A publisher is connected, and another client holds a durable session
// subscribed to full/other; a ballast then takes every byte left but
// leftRoom. The publisher publishes spilling random bytes at QoS 1 and pings: the
// PINGRESP says the core has taken the PUBLISH (pinged), which waits in the
// collecting group. The other client's UNSUBSCRIBE of full/other joins that
// group and sends it. SQLite meets the full disk writing the publication's
// pages to the log: the PUBACK is 0x97 and the UNSUBACK 0x80, the
// database holds no record, and saguin_store_full is 1. A publication of the
// same size, alone, gets 0x97 again, saguin_store_full still 1. The same
// UNSUBSCRIBE, alone, then gets 0x00: the room left holds it, so the group,
// not its own size, refused it; saguin_store_full is 0. Once the ballast is
// emptied, a third publication of that size gets PUBACK 0x00,
// saguin_store_full still 0. Killed and started again on the same files,
// the broker's database holds that record and no other.
//
// Like TestCommitCauses, the grouping relies on the core taking the
// UNSUBSCRIBE well within the interval, 1s, the most config.md allows,
// after the PINGRESP.
func TestPublishMeetingAFullDisk(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, spillingDiskSize, support.AloneLimit)
		return
	}
	o := &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), PublishCommitInterval: time.Second, MaxPacketSize: 2 * spilling}
	b := support.OpsLaunch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-pub", CleanStart: true})
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-other", CleanStart: true, Props: support.Durable}, support.At(0, "full/other"))
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	filled, err := ballast.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := ballast.Truncate(filled.Size() - leftRoom); err != nil {
		t.Fatal(err)
	}
	db := support.OpenDB(t, b.DB)

	refused := support.Noisy(t, "full/refused", spilling)
	support.Sent(t, pub, refused)
	pinged(t, pub)
	unsuback(t, other, nil, []string{"full/other"}, 0x80)
	support.PubackOf(t, pub, refused, 0x97)
	if got := topicsOf(held(t, db, "$broadcast")); len(got) != 0 {
		t.Errorf("after the group answered full, the database holds the records of %q; want none", got)
	}
	expectSample(t, b, "saguin_store_full", 1, "after the group answered full")

	answered(t, pub, support.Noisy(t, "full/again", spilling), 0x97)
	expectSample(t, b, "saguin_store_full", 1, "after a second publication answered full")
	unsuback(t, other, nil, []string{"full/other"}, 0x00)
	expectSample(t, b, "saguin_store_full", 0, "after the UNSUBSCRIBE, alone, was saved")

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.Acknowledged(t, pub, support.Noisy(t, "full/stored", spilling))
	expectSample(t, b, "saguin_store_full", 0, "after a publication saved once the ballast was emptied")

	support.Crash(t, b)
	again := support.Launch(t, o)
	if got := topicsOf(held(t, support.OpenDB(t, again.DB), "$broadcast")); !slices.Equal(got, []string{"full/stored"}) {
		t.Errorf("after a restart, the database holds the records of %q; want only the acknowledged one, full/stored", got)
	}
}

// design.md, Store, A full disk: "A write request answered "full" is
// discarded. The core does not retain its body for retry. It may retain
// bounded bookkeeping under the following rules."; "The following applies
// when no bounded bookkeeping preserves the change. What a lost write would
// have changed comes back only from what the store still holds, from the
// client's own resend, or at the next start. The next sweep or retention pass
// may rediscover a due Will or session, or a record not yet deleted."; "A
// housekeeping write refused for room sets a retry deadline one second after
// that answer"; "At the deadline, the core rediscovers due Wills, session
// endings and retention work from the store"; "Another full refusal schedules
// the next retry"; "Failed write bodies are not retained". DISCONNECT: "When
// the expiry runs out, [...] the session deleted"; A clean DISCONNECT: the
// session's expiry "is stored before the connection closes". MQTT 5
// 3.1.2.11.2: the Session Expiry Interval is in seconds, the state kept after
// the connection closes while it is above 0 [MQTT-3.1.2-23]. storage.md,
// sessions: "One row per durable session".
//
// On a filesystem of its own, an MQTT 5 client of Session Expiry 1 s sends
// DISCONNECT, and its connection closes, its going away stored; a ballast
// then takes every byte left. Past its expiry, the session's ending refused
// for room, the database still holds its row. Once the ballast is emptied,
// with no client acting, the row is gone, the broker still running.
func TestEndingRefusedForRoomEndsOnceRoomIsFreed(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const expiry = time.Second
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "expires", CleanStart: true,
		Props: support.SessionExpiry(uint32(expiry / time.Second))})
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
	gone := time.Now() // the session's expiry runs out no later than expiry from here
	db := support.OpenDB(t, b.DB)
	if n := sessionRows(t, db, "expires"); n != 1 {
		t.Fatalf("once the session went away, the database holds %d sessions rows for it, want 1", n)
	}
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	if filled := time.Since(gone); filled >= expiry {
		t.Fatalf("the disk took %v to fill, the session's expiry %v: its ending may have found room", filled, expiry)
	}
	time.Sleep(time.Until(gone.Add(expiry + support.Late)))
	if n := sessionRows(t, db, "expires"); n != 1 {
		t.Fatalf("past the session's expiry on a full disk, the database holds %d sessions rows for it; want 1, its ending refused for room", n)
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool { return sessionRows(t, db, "expires") == 0 }) {
		t.Errorf("room freed, the database still holds the expired session's row after %v; want the retry to have ended it", harness.DefaultTimeout)
	}
}

// design.md, DISCONNECT, A lost connection: "The Will goes after its Will
// Delay [...] It is read from the store when it goes"; "If the session ends
// first, the Will goes early"; "On a full disk it stays in the store and is
// published once there is room. A delayed Will whose session outlives its
// connection is timed from the saved departure"; Store, A full disk: "What a
// lost write would have changed comes back only from what the store still
// holds [...] The next sweep or retention pass may rediscover a due Will or
// session"; "A housekeeping write refused for room sets a retry deadline one
// second after that answer"; "At the deadline, the core rediscovers due Wills
// [...] from the store"; "Another full refusal schedules the next retry".
// storage.md, wills: wills_due leaves out the Wills of connected clients,
// and "Only the full-store retry reads them, to find a Will whose
// publication a full disk refused". MQTT 5 3.1.2.5: the Will "MUST be
// published after the Network Connection is subsequently closed and either
// the Will Delay Interval has elapsed or the Session ends" [MQTT-3.1.2-8].
//
// On a filesystem of its own, an MQTT 5 client connects with a Will on a
// topic a subscriber holds, and drops its socket, in three shapes. A durable
// session's Will of Will Delay Interval 1 second: once the database gives
// the Will its due time, its departure saved, a ballast takes every byte
// left. A durable session's Will of no delay, and a Will of an hour's delay
// on a session that ends with its connection, due as the session ends: a
// ballast takes every byte left before the socket drops, so the Will's
// firing itself is refused for room. Until a second past the Will's due
// time, its publication refused for room, the subscriber receives nothing
// and the database still holds the Will. Once the ballast is emptied, with
// no client acting, the subscriber receives the Will, and the database then
// holds it no more.
func TestWillMeetingAFullDiskGoesOnceThereIsRoom(t *testing.T) {
	for _, tc := range []struct {
		name     string
		session  harness.Properties // the CONNECT's properties: durable, or a session ending with its connection
		will     harness.Properties // the Will's properties: its delay
		departed bool               // the departure saved before the disk fills, else the firing refused
	}{
		{"delayed, departure saved", support.Durable, support.WillDelay(1), true},
		{"undelayed, durable session", support.Durable, nil, false},
		{"delayed, session ending with its connection", nil, support.WillDelay(support.Hour), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := os.Getenv(support.SmallDisk)
			if disk == "" {
				testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
				return
			}
			b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
			db := support.OpenDB(t, b.DB)
			topic := clientID(t, "/will")
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
			id := clientID(t, "")
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: tc.session,
				Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: tc.will}})
			var ballast *os.File
			var due time.Time
			if tc.departed {
				support.DropSocket(t, c)
				due = support.AwaitWillDue(t, db, id)
				ballast = testkit.FillDisk(t, filepath.Join(disk, "ballast"))
				if !time.Now().Before(due) {
					t.Fatal("the disk filled after the Will's due time: the test was too slow to have the Will meet a full disk")
				}
			} else {
				ballast = testkit.FillDisk(t, filepath.Join(disk, "ballast"))
				support.DropSocket(t, c)
				due = time.Now() // the Will is due as its connection ends
			}
			nothingWithin(t, sub, time.Until(due.Add(support.Late)), "until a second past the Will's due time on a full disk")
			if _, held := support.StoredWill(t, db, id); !held {
				t.Fatal("past the Will's due time on a full disk, the database holds it no more; want it kept in the store, its publication refused for room")
			}

			if err := ballast.Truncate(0); err != nil {
				t.Fatal(err)
			}
			live(t, sub, topic, support.WillPayload)
			awaitWillGone(t, db, id, "the ballast was emptied")
		})
	}
}

// design.md, R8: "A store error stops the broker, except a failed memory-mode
// snapshot"; Store, A full disk: "a write is answered "full" only when SQLite
// reports lack of space and rollback confirms that the transaction did not
// commit"; "A failure whose transaction outcome or database integrity is
// uncertain is a store error. It stops the broker, even when lack of space
// caused it"; Restart: "A stop for a failure, whether a store error or a
// failed accept, follows the graceful stop" ("every client gets 0x8B"), and
// "The exit status is [...] It is 1 after a stop caused by a failure";
// design.md, Flows, Logging: "The core logs its first fatal store error as
// one ERROR line naming the error. It also logs each reply a writer could not
// build as one ERROR line naming the error, from that writer's close report.
// These lines follow the admission rules above" (decided from items 0008a and
// 0026d). The maintainer's decision on audit finding 261013-004a: "full" only
// where SQLite guarantees the rollback, SQLITE_FULL; any other error, an
// fsync failing for room included, is a store error; on 261013-019a: a quota
// met at a write is a full disk; no other failure of that write is.
// design.md, Flows, Logging: "Runtime log-worthy events reach the core
// through their existing event or failure reports".
//
// Each row fails one kind of system call on one of the database's files
// while a client's packet is stored: on the write-ahead log, a CONNECT of a
// new durable session; on the log's index, the database's -shm, a
// publication of indexGrowth bytes at QoS 1. A full disk refuses that
// CONNECT with 0x97, and the broker serves on: a connected client's PINGREQ
// is answered, and once the failure ends the same CONNECT is accepted. A
// store error stops the broker in order: the connected client gets
// DISCONNECT 0x8B, exactly one ERROR line names the error, SQLite's "disk
// I/O error" (the name sqlite3_errstr gives SQLITE_IOERR), and the exit
// status is 1.
func TestDiskFailures(t *testing.T) {
	const ioError = "disk I/O error"
	// touches connects a new durable session.
	touches := func(t *testing.T, b *harness.Broker) *harness.Client {
		t.Helper()
		return touch(t, b, "touches-the-store")
	}
	for _, tc := range []struct {
		name        string
		file        string // the database's file whose calls fail
		kind, errno string // the failing calls, as testkit.FailingIO takes them
		full        bool   // a full disk, else a store error
		// meets sends, from a client of its own, the packet the failure
		// meets, and returns that client.
		meets func(t *testing.T, b *harness.Broker) *harness.Client
	}{
		// SQLite writes every frame of a commit into the log, then syncs it:
		// a sync that fails leaves the frames there, so a restart may
		// recover the write, and its outcome is unknown.
		{"the log's sync failing for room", "saguin.db-wal", "sync", "ENOSPC", false, touches},
		// EIO is the device failing, not a lack of room.
		{"the log's writes failing not for room", "saguin.db-wal", "write", "EIO", false, touches},
		// A quota met is a lack of room; the frame is not written.
		{"a quota met at the log's writes", "saguin.db-wal", "write", "EDQUOT", true, touches},
		// SQLite grows the index after it has written and synced a commit's
		// frames to the log, so a restart may recover the write, and its
		// outcome is unknown; SQLite reports the growth failing as an I/O
		// error, SQLITE_IOERR_SHMSIZE, not as lack of space.
		{"the log's index failing to grow for room", "saguin.db-shm", "write", "ENOSPC", false, growsTheIndex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir, failing := filepath.Join(root, "store"), filepath.Join(root, "failing")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			trace := filepath.Join(root, "trace")
			under := testkit.FailingIO(t, tc.kind, tc.errno, filepath.Join(failing, tc.file), trace)
			b := support.Launch(t, &harness.Options{FilePath: filepath.Join(dir, "saguin.db"), Under: under, MaxPacketSize: 2 * indexGrowth, MaxIncomingMemory: leastIncoming(2 * indexGrowth)})
			connected, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "connected", CleanStart: true})

			// The failure begins: the database's folder takes the name
			// whose files fail.
			if err := os.Rename(dir, failing); err != nil {
				t.Fatal(err)
			}
			c := tc.meets(t, b)
			if tc.full {
				if ack := harness.Expect[*harness.Connack](t, c); ack.Reason != 0x97 {
					t.Errorf("CONNACK 0x%02X, want 0x97 (Quota exceeded)", ack.Reason)
				}
				if err := connected.Send(&harness.Pingreq{}); err != nil {
					t.Fatal(err)
				}
				harness.Expect[*harness.Pingresp](t, connected)
			} else {
				shutDown(t, connected)
				_, _ = connected.Close(), c.Close() // the broker has ended its end
				if state := support.Exited(t, b); state.ExitCode() != 1 {
					t.Errorf("the stop on a store error ended %v, want exit status 1", state)
				}
				if lines := testkit.LinesNaming(b.Log(), "ERROR", ioError); len(lines) != 1 {
					t.Errorf("%d ERROR lines name the store error (%q), want exactly one:\n%s", len(lines), ioError, b.Log())
				}
			}
			if !testkit.Injected(t, trace) {
				t.Fatalf("no %s failed with %s: the test injected nothing", tc.kind, tc.errno)
			}
			if !tc.full {
				return
			}
			// The failure ends.
			if err := os.Rename(failing, dir); err != nil {
				t.Fatal(err)
			}
			harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "touches-the-store", CleanStart: true, Props: support.Durable})
		})
	}
}

// walIndexFirstRegion is the frames of the write-ahead log that the first
// 32 KiB region of its index, the database's -shm, maps (SQLite's
// HASHTABLE_NPAGE_ONE): a log past them grows the -shm.
const walIndexFirstRegion = 4062

// indexGrowth is the size of a publication's record whose pages take a new
// database's log just past walIndexFirstRegion frames, at its commit:
// SQLite writes to the log, while the statements run, the pages its cache
// cannot hold, and at the commit the last of them, the ones its cache of
// 2,000 KiB still holds (spilling). Its publication is noisy, as
// spilling's are.
const indexGrowth = (walIndexFirstRegion + 128) * 4096

// growsTheIndex connects a client to b, which publishes indexGrowth random bytes
// at QoS 1, and returns it.
func growsTheIndex(t *testing.T, b *harness.Broker) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "grows-the-index", CleanStart: true})
	support.Sent(t, c, support.Noisy(t, "grows/the/index", indexGrowth))
	return c
}

// design.md, Store, A full disk: "a write is answered "full" only when
// SQLite reports lack of space and rollback confirms that the transaction
// did not commit. The broker continues after that answer"; "A start
// with no room for SQLite's -wal and -shm files is refused, naming the
// cause"; Restart: "The exit status is [...] 1 after [...] a refused
// start". storage.md, sessions: "A session left connected by a crash is away
// from the start that follows, except in the full-disk case (Store)".
//
// On a filesystem of its own, a broker holds a durable session, connected,
// and ends; a ballast then takes every byte left, and the broker starts
// again on the same files. After a crash, which leaves the -wal and -shm
// files, it starts; once the ballast is emptied, the session is there.
// After a clean stop, which leaves the database alone
// (TestCleanStopLeavesOneFile), the start is refused, with exit status 1,
// naming the full disk in the system's own words for it.
func TestStartOnAFullDisk(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		end    func(t *testing.T, b *harness.Broker)
		starts bool
	}{
		{"after a crash", support.Crash, true},
		{"after a clean stop", func(t *testing.T, b *harness.Broker) {
			t.Helper()
			if state := stop(t, b); !state.Success() {
				t.Fatalf("the graceful stop ended %v, want exit status 0", state)
			}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := os.Getenv(support.SmallDisk)
			if disk == "" {
				testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
				return
			}
			o := &harness.Options{Dir: t.TempDir(), FilePath: filepath.Join(disk, "saguin.db")}
			b := support.Launch(t, o)
			support.Stored(t, b, "restored")
			tc.end(t, b)
			ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

			if !tc.starts {
				config, err := harness.WriteConfig(o)
				if err != nil {
					t.Fatal(err)
				}
				code, _, stderr := execute(t, support.BrokerBinary, "--config", config)
				if code != 1 {
					t.Errorf("exit %d on a full disk after a clean stop; want the start refused, exit 1", code)
				}
				// ENOSPC's text, as strerror gives it.
				if !strings.Contains(stderr, "no space left on device") {
					t.Errorf("the refusal does not name the full disk:\n%s", stderr)
				}
				return
			}
			again := support.Launch(t, o)
			if err := ballast.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if !present(t, again.Addr, "restored") {
				t.Error("after a start on a full disk, CONNACK with Clean Start 0 has Session Present 0: the session is gone")
			}
		})
	}
}

// design.md, Restart: "Sessions that expired while the broker was down end
// at start, each in one transaction"; Start: "The core loads [...] It ends
// expired sessions. Listeners and bridges start last". DISCONNECT: "The
// session goes away until its expiry". storage.md, sessions: "One row per
// durable session".
//
// A durable session of a 1-second expiry goes away with its client's
// DISCONNECT, and the broker stops; once the expiry has passed, it starts
// again. When its listener accepts, the database holds no row for the
// session, and its client's CONNECT with Clean Start 0 finds Session
// Present 0.
func TestSessionsExpiredWhileDownEndAtStart(t *testing.T) {
	const expiry = time.Second
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{
		ClientID: "expires", CleanStart: true,
		Props: support.SessionExpiry(uint32(expiry / time.Second)),
	})
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
	// The session went away no later than now: its expiry has run out once
	// expiry has passed from here.
	gone := time.Now()
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	time.Sleep(time.Until(gone.Add(expiry)))

	again := support.Launch(t, &harness.Options{Dir: dir})
	if n := sessionRows(t, support.OpenDB(t, again.DB), "expires"); n != 0 {
		t.Errorf("once the broker listens, the database holds %d sessions rows for the expired session, want none", n)
	}
	if present(t, again.Addr, "expires") {
		t.Error("CONNACK with Clean Start 0 has Session Present 1: the session that expired while the broker was down is there")
	}
}

// design.md, Restart, Restored: "Wills with their due time"; "Sessions that
// expired while the broker was down end at start, each in one transaction. A
// Will with a due time is published if due. A Will whose client was connected
// at the stop is due its Will Delay Interval after the start, with its
// session now away (MQTT-3.1.2-8). A Will without a surviving session is due
// immediately"; Start: the core loads "the next session expiry and Will due
// time still to come"; DISCONNECT, A lost connection: the Will "If the
// session ends first, the Will goes early". storage.md, sessions: "A session
// left connected by a crash is away from the start that follows"; What is
// stored: non-durable sessions stay in memory, "A Will is also stored,
// including a non-durable session's Will". MQTT 5 3.1.2.5: "In the case of a
// Server shutdown or failure, the Server MAY defer publication of Will
// Messages until a subsequent restart"; the Will "MUST be published after the
// Network Connection is subsequently closed and either the Will Delay
// Interval has elapsed or the Session ends" [MQTT-3.1.2-8].
//
// A subscriber's durable session holds the test's topics. MQTT 5 clients
// connect with Wills on topics of their own. Three drop their sockets: due,
// of a durable session, with Will Delay Interval 1 second; expired, of
// Session Expiry 1 second, with Will Delay Interval an hour; waiting, of a
// durable session, with Will Delay Interval 3 seconds. Two stay connected:
// connected, of a durable session, with Will Delay Interval 1 second; and
// lost, of Session Expiry 0, with Will Delay Interval an hour. Once the
// database holds each going away, the broker is killed, before any of
// those Wills is due; once due's Will and expired's session are due, it
// starts again on the same files, and the subscriber resumes. It receives
// each Will once: due's, expired's and lost's; waiting's no sooner than
// its due time, kept from before the restart, and sooner than its Will
// Delay Interval after the restart began; connected's no sooner than its
// Will Delay Interval after the restart began. The database then holds
// none of those Wills, nor expired's session.
//
// Graceful stop: "It acts on a client's DISCONNECT by storing its session
// expiry change and keeping or discarding its Will as the DISCONNECT says";
// "once the stop begins, the core's timer starts no housekeeping (expired
// sessions and due Wills are handled at the next start)". On the restarted
// broker, stopped, of a durable session, connects with a Will of Will Delay
// Interval 1 second and stays connected while the broker is stopped
// (SIGTERM): the subscriber's next packet is DISCONNECT 0x8B, exit status 0,
// and the database still holds the Will, never published at the stop. Once
// the broker starts again and the subscriber resumes, it receives the Will no
// sooner than its Will Delay Interval after that start began.
func TestRestartRecoversWills(t *testing.T) {
	const delay, waits = time.Second, 3 * time.Second
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	db := support.OpenDB(t, b.DB)
	prefix := clientID(t, "")
	sub := &harness.Connect{ClientID: prefix + "-sub", CleanStart: true, Props: support.Durable}
	support.Subscribed(t, b.Addr, harness.V5, sub, support.At(0, prefix+"/#"))
	clients := []struct {
		name  string
		props harness.Properties // the CONNECT's
		delay time.Duration      // its Will's Will Delay Interval
		stays bool               // connected when the broker is killed
	}{
		{"due", support.Durable, delay, false},
		{"expired", support.SessionExpiry(1), support.Hour * time.Second, false},
		{"waiting", support.Durable, waits, false},
		{"connected", support.Durable, delay, true},
		{"lost", nil, support.Hour * time.Second, true},
	}
	for _, w := range clients {
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: prefix + "-" + w.name, CleanStart: true, Props: w.props,
			Will: &harness.Will{Topic: prefix + "/" + w.name, Payload: support.WillPayload, Props: support.WillDelay(uint32(w.delay / time.Second))}})
		if !w.stays {
			support.DropSocket(t, c)
		}
	}
	due := support.AwaitWillDue(t, db, prefix+"-due")
	expires := support.AwaitAway(t, db, prefix+"-expired").Add(time.Second) // its Session Expiry
	waiting := support.AwaitWillDue(t, db, prefix+"-waiting")
	support.Crash(t, b)
	if crashed := time.Now(); !crashed.Before(due) || !crashed.Before(expires) {
		t.Fatal("the broker was killed after a Will was due: the test was too slow to have it due while the broker was down")
	}
	// Both are due once the later of the two has passed.
	if expires.After(due) {
		time.Sleep(time.Until(expires.Add(tick)))
	} else {
		time.Sleep(time.Until(due.Add(tick)))
	}

	restarted := time.Now()
	again := support.Launch(t, &harness.Options{Dir: dir})
	c := resumed(t, again.Addr, harness.V5, sub)
	arrived := make(map[string]time.Time, len(clients))
	for range clients {
		p := harness.Expect[*harness.Publish](t, c)
		name, ours := strings.CutPrefix(p.Topic, prefix+"/")
		if _, seen := arrived[name]; seen || !ours || !bytes.Equal(p.Payload, support.WillPayload) {
			t.Fatalf("after the Wills of %v, PUBLISH %q of %q; want another client's Will, each once", slices.Sorted(maps.Keys(arrived)), p.Topic, p.Payload)
		}
		arrived[name] = time.Now()
	}
	if at := arrived["waiting"]; at.Before(waiting.Add(-tick)) || !at.Before(restarted.Add(waits)) {
		t.Errorf("waiting's Will came %v after its due time and %v after the restart began; want it at its due time, kept, sooner than its delay, %v, after the restart",
			at.Sub(waiting), at.Sub(restarted), waits)
	}
	if at := arrived["connected"]; at.Before(restarted.Add(delay - tick)) {
		t.Errorf("connected's Will came %v after the restart began; want its whole Will Delay Interval, %v, after the start", at.Sub(restarted), delay)
	}
	for _, w := range clients {
		awaitWillGone(t, db, prefix+"-"+w.name, "its publication")
	}
	if n := sessionRows(t, db, prefix+"-expired"); n != 0 {
		t.Errorf("after the restart, the database holds %d sessions rows for the expired session; want none", n)
	}

	stopped := prefix + "-stopped"
	harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: stopped, CleanStart: true, Props: support.Durable,
		Will: &harness.Will{Topic: prefix + "/stopped", Payload: support.WillPayload, Props: support.WillDelay(uint32(delay / time.Second))}})
	if state := stop(t, again); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	shutDown(t, c)
	if _, held := support.StoredWill(t, db, stopped); !held {
		t.Fatal("after the graceful stop, the database holds no Will for the client connected at the stop; want it kept for the next start")
	}
	restarted = time.Now()
	last := support.Launch(t, &harness.Options{Dir: dir})
	c = resumed(t, last.Addr, harness.V5, sub)
	if at := willArrives(t, c, prefix+"/stopped"); at.Before(restarted.Add(delay - tick)) {
		t.Errorf("stopped's Will came %v after the restart began; want its whole Will Delay Interval, %v, after the start", at.Sub(restarted), delay)
	}
	awaitWillGone(t, db, stopped, "its publication")
}

// sessionRows counts the sessions rows of client in q's database
// (storage.md, sessions: client "unique: the client id").
func sessionRows(t *testing.T, q support.RowQuerier, client string) int {
	t.Helper()
	var n int
	if err := q.QueryRowContext(context.Background(), `SELECT count(*) FROM sessions WHERE client = ?`, client).Scan(&n); err != nil {
		t.Fatalf("reading the sessions rows of %q: %v", client, err)
	}
	return n
}

// fileSizeLimit lets SQLite make a new file in WAL mode, but not commit the
// whole schema's pages, so a creation under it is cut short.
const fileSizeLimit = 64 << 10

// A database file as a row of TestStartChecksTheDatabaseFile finds it: made
// at path, a file in a folder of its own, before the broker starts on it.
type databaseFile func(t *testing.T, path string)

// sqliteFile makes a database through SQLite alone, in its default rollback
// journal, holding ddl's objects and the given user_version.
func sqliteFile(version int, ddl string) databaseFile {
	return func(t *testing.T, path string) {
		t.Helper()
		db := support.OpenDB(t, path)
		for _, stmt := range []string{ddl, `PRAGMA user_version = ` + strconv.Itoa(version)} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil { // the file is left as its maker leaves it
			t.Fatal(err)
		}
	}
}

// saguinFile has a broker create the database at path, and stops it
// cleanly, which leaves the database one file (TestCleanStopLeavesOneFile).
func saguinFile(t *testing.T, path string) {
	t.Helper()
	if state := stop(t, support.Launch(t, &harness.Options{FilePath: path})); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
}

// alteredFile makes a Sagüin file, then runs ddl on it and folds SQLite's
// log into it, leaving its version as it was.
func alteredFile(ddl string) databaseFile {
	return func(t *testing.T, path string) {
		t.Helper()
		saguinFile(t, path)
		db := support.OpenDB(t, path)
		for _, stmt := range []string{ddl, `PRAGMA wal_checkpoint(TRUNCATE)`} {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil { // its -wal and -shm go with it
			t.Fatal(err)
		}
	}
}

// damagedFile makes a Sagüin file and overwrites the b-tree header of
// table's root page, or with "" the first page's, which holds the schema,
// after the 100-byte file header.
func damagedFile(table string) databaseFile {
	return func(t *testing.T, path string) {
		t.Helper()
		saguinFile(t, path)
		at := int64(100)
		if table != "" {
			db := support.OpenDB(t, path)
			var root, pageSize int64
			if err := db.QueryRow(`SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&root); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			at = (root - 1) * pageSize
		}
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteAt(bytes.Repeat([]byte{0xff}, 16), at)
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// rollbackFile makes a Sagüin file and returns it to SQLite's rollback
// journal; with readOnly, its folder then takes no new file, so SQLite
// cannot make the WAL's -wal and -shm files beside it.
func rollbackFile(readOnly bool) databaseFile {
	return func(t *testing.T, path string) {
		t.Helper()
		saguinFile(t, path)
		db := support.OpenDB(t, path)
		var mode string
		if err := db.QueryRow(`PRAGMA journal_mode = DELETE`).Scan(&mode); err != nil || mode != "delete" {
			t.Fatalf("setting the rollback journal: %q, %v", mode, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if !readOnly {
			return
		}
		if os.Geteuid() == 0 {
			t.Skip("root makes files in a read-only folder")
		}
		dir := filepath.Dir(path)
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) }) // so the test's folder can go
	}
}

// emptyFile makes a file of no bytes.
func emptyFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// garbageFile writes two pages of bytes that are no database.
func garbageFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, bytes.Repeat([]byte("this is not a database file\n"), 300), 0o600); err != nil {
		t.Fatal(err)
	}
}

// cutShort has a broker create the database at path under a file size
// limit, set by prlimit for the process alone, that stops the creation part
// way: the start is refused, and the file it leaves holds no table and no
// version (storage.md: "When the file is created, the tables, the $broadcast
// row in channels and the version are written in one transaction").
func cutShort(t *testing.T, path string) {
	t.Helper()
	config, err := harness.WriteConfig(&harness.Options{Dir: t.TempDir(), FilePath: path})
	if err != nil {
		t.Fatal(err)
	}
	limit := "--fsize=" + strconv.Itoa(fileSizeLimit)
	if code, _, stderr := execute(t, "prlimit", limit, support.BrokerBinary, "--config", config); code != 1 {
		t.Fatalf("the creation under a %d-byte file size limit ended with exit %d, want a refused start, 1:\n%s", fileSizeLimit, code, stderr)
	}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the creation cut short left no file at %s to start on", path)
	}
	db := support.OpenDB(t, path)
	if n := schemaObjects(t, db, ""); n != 0 {
		t.Errorf("the creation cut short left %d tables and indexes", n)
	}
	if v := pragma[int](t, db, "user_version"); v != 0 {
		t.Errorf("the creation cut short left user_version %d", v)
	}
}

// pragma reads one of SQLite's settings of db.
func pragma[T any](t *testing.T, db *sql.DB, name string) T {
	t.Helper()
	var v T
	if err := db.QueryRow(`PRAGMA ` + name).Scan(&v); err != nil {
		t.Fatalf("PRAGMA %s: %v", name, err)
	}
	return v
}

// schemaObjects counts db's schema objects of type ("table", "index"), or
// of every type with "", SQLite's own aside.
func schemaObjects(t *testing.T, db *sql.DB, typ string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\' AND (? = '' OR type = ?)`, typ, typ).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// crashedFile makes a Sagüin file whose -wal holds committed changes, as a
// crash leaves it: a broker stores a durable session, ddl is committed
// beside it through SQLite, and the broker is killed, so no stop folds the
// log into the database.
func crashedFile(ddl string) databaseFile {
	return func(t *testing.T, path string) {
		t.Helper()
		b := support.Launch(t, &harness.Options{FilePath: path})
		support.Stored(t, b, "crashed")
		db := support.OpenDB(t, path)
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
		// The broker's connections stay open: closing this one does not
		// fold the log in.
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		support.Crash(t, b)
		if wal, ok := fileIfThere(t, path+"-wal"); !ok || len(wal) == 0 {
			t.Fatalf("the crash left no -wal holding changes beside %s", path)
		}
	}
}

// fileIfThere returns the bytes of the file at path, and whether it exists.
func fileIfThere(t *testing.T, path string) ([]byte, bool) {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return b, true
}

// storage.md, The schema version: "A missing file, or one holding no schema
// object at version 0, is new and is created with the schema below (Logs). A
// file holding any other version stops startup, naming both versions. The
// broker never upgrades it" (the new file
// decided from item 0006a); "Every SQLite table [...] with its columns, keys
// and indexes": a file whose schema is another is damaged. design.md, Store:
// "A damaged file_path stops the broker. If the damage is in the header,
// schema, version or state the core loads, it stops the broker at startup";
// "It runs in WAL mode"; Restart: "The exit status is [...] 1 after [...] a
// refused start". An empty file, and one a first creation cut short leaves,
// hold no schema object at version 0, so are new (storage.md: "When the file
// is created, the tables, the $broadcast row in channels and the version are
// written in one transaction"); "a file without that row is damaged (Store)".
//
// design.md, Store: "A start that refuses the database file leaves it, and a
// -wal file beside it, as it found them; SQLite may create an empty -wal and
// its -shm."
//
// The broker starts on a database file each row makes. A file it refuses
// ends the start with exit status 1, the refusal naming the file and what
// the row names; the file is byte for byte as it was before the start, and
// so is a -wal that was beside it, a crash's holding committed changes
// included; a -wal that was not there is absent or empty; a -shm may appear
// or change. A file it takes it starts on, and, once stopped, the file
// holds storage.md's tables, version 21, in WAL mode.
func TestStartChecksTheDatabaseFile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		make  databaseFile
		takes bool     // the start goes on, else it is refused
		names []string // what a refusal names besides the file, each a word of its own
	}{
		{"version 20", sqliteFile(20, `CREATE TABLE channels (id INTEGER)`), false, []string{"20", "21"}},
		{"a later version 22", sqliteFile(22, `CREATE TABLE channels (id INTEGER)`), false, []string{"22", "21"}},
		{"another program's database", sqliteFile(0, `CREATE TABLE notes (body TEXT)`), false, []string{"0", "21"}},
		{"no schema object, version 7", sqliteFile(7, `CREATE TABLE notes (body TEXT); DROP TABLE notes`), false, []string{"7", "21"}},
		{"garbage bytes", garbageFile, false, nil},
		{"a Sagüin file whose schema page is damaged", damagedFile(""), false, nil},
		{"a Sagüin file whose channels page is damaged", damagedFile("channels"), false, nil},
		{"a Sagüin file whose bridges table lacks its CHECK", alteredFile(`DROP TABLE bridges;
			CREATE TABLE bridges (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT`), false, nil},
		{"a Sagüin file missing the index records_retained", alteredFile(`DROP INDEX records_retained`), false, nil},
		{"a Sagüin file missing the index sessions_expiry", alteredFile(`DROP INDEX sessions_expiry`), false, nil},
		{"a Sagüin file missing the index wills_due", alteredFile(`DROP INDEX wills_due`), false, nil},
		// storage.md: "A file is accepted when its catalogue matches these
		// tables"; the maintainer's decision C2 (design review 261019): a
		// file the build wrote before storage.md's sessions_expiry left out
		// the expiry that never runs out is refused.
		{"a Sagüin file holding the earlier build's sessions_expiry", alteredFile(`DROP INDEX sessions_expiry;
			CREATE INDEX sessions_expiry ON sessions (left + expiry * 1000) WHERE left > 0`), false, nil},
		{"a Sagüin file with an index storage.md does not list", alteredFile(`CREATE INDEX sessions_left ON sessions (left)`), false, nil},
		{"a Sagüin file without $broadcast's channels row", alteredFile(`DELETE FROM channels WHERE id = 1`), false, []string{"broadcast"}},
		{"a crash's -wal missing the index records_retained", crashedFile(`DROP INDEX records_retained`), false, nil},
		{"a Sagüin file in the rollback journal, in a read-only folder", rollbackFile(true), false, []string{"journal_mode"}},
		{"an empty file", emptyFile, true, nil},
		{"a database whose only table was dropped", sqliteFile(0, `CREATE TABLE notes (body TEXT); DROP TABLE notes`), true, nil},
		{"a creation cut short", cutShort, true, nil},
		{"a Sagüin file in the rollback journal", rollbackFile(false), true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "store", "saguin.db")
			if err := os.Mkdir(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			tc.make(t, path)
			o := &harness.Options{Dir: dir, FilePath: path}
			if tc.takes {
				if state := stop(t, support.Launch(t, o)); !state.Success() {
					t.Fatalf("the graceful stop ended %v, want exit status 0", state)
				}
				db := support.OpenDB(t, path)
				if n := schemaObjects(t, db, "table"); n != len(testkit.Tables) {
					t.Errorf("tables: got %d, want storage.md's %d", n, len(testkit.Tables))
				}
				if v := pragma[int](t, db, "user_version"); v != 21 {
					t.Errorf("user_version: got %d, want 21", v)
				}
				if mode := pragma[string](t, db, "journal_mode"); mode != "wal" {
					t.Errorf("journal_mode: got %q, want wal", mode)
				}
				return
			}
			config, err := harness.WriteConfig(o)
			if err != nil {
				t.Fatal(err)
			}
			database, _ := fileIfThere(t, path)
			wal, hadWAL := fileIfThere(t, path+"-wal")
			code, _, stderr := execute(t, support.BrokerBinary, "--config", config)
			if code != 1 {
				t.Errorf("exit %d; want the start refused, exit 1:\n%s", code, stderr)
			}
			if after, _ := fileIfThere(t, path); !bytes.Equal(after, database) {
				t.Error("the refused start changed the database file")
			}
			switch after, has := fileIfThere(t, path+"-wal"); {
			case hadWAL && (!has || !bytes.Equal(after, wal)):
				t.Error("the refused start changed or removed the -wal beside the database")
			case !hadWAL && len(after) != 0:
				t.Errorf("the refused start left a -wal of %d bytes where there was none; SQLite may make only an empty one", len(after))
			}
			if !strings.Contains(stderr, path) {
				t.Errorf("the refusal does not name the file %s:\n%s", path, stderr)
			}
			// The file's path may hold any of the words; they are looked
			// for in the rest.
			rest := strings.ReplaceAll(stderr, path, "")
			for _, word := range tc.names {
				if !regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`).MatchString(rest) {
					t.Errorf("the refusal does not name %s:\n%s", word, stderr)
				}
			}
		})
	}
}

// storage.md: "The schema version is SQLite's own user_version: 21"; "Every
// table is STRICT", each WITHOUT ROWID where its row says so; every column
// of every table, in the order its table lists them, with its type, NOT
// NULL "unless its row says nullable", and its primary key; the unique keys, the index records_retained,
// "The unique index records_retained is on (channel, topic) WHERE retained =
// 1. It allows at most one current value per topic, and finds it", and the index
// sessions_expiry "on (left + expiry
// * 1000) WHERE left > 0 AND expiry <> 4294967295. It orders away sessions by
// the instant their expiry runs out. An expiry of 4,294,967,295 never runs
// out, so it is excluded from the index and from every due-expiry query",
// and the index wills_due "on (due) WHERE due IS NOT NULL", whose use by
// the Will pass internal/store's TestWillReadsSeekTheirKeys checks; "The
// index held_publishes_retained is on (topic) WHERE channel = 1 AND retain =
// 1. It finds a topic's held retained replacements ... without reading every
// held row"; and, under session_inflight, "The index
// session_inflight_shared is on (share, channel, offset) WHERE share > 0. It
// finds a shared group's assignments of a record without reading every
// exchange. A query uses it only when it repeats share > 0"; each table's
// CHECK. storage.md lists "Every SQLite table
// Sagüin creates", so the file holds no other table, index, view or trigger. design.md, Store:
// "It runs in WAL mode", which the file records. storage.md: "When the file
// is created, the tables, the $broadcast row in channels and the version are
// written in one transaction"; "The $broadcast channel is channel 1";
// channels: a dead-letter channel "and $broadcast are 1" (append), next is
// "the offset the next record takes", floor "the oldest offset retention has
// not passed", and bytes "the bytes of its records", none in a new file.
//
// A broker makes a new file and stops; the file holds one channels row,
// $broadcast's: id 1, type 1, next 1, floor 1, bytes 0. The file, read
// through SQLite's own
// catalogue (sqlite_schema, pragma_table_list, pragma_table_info,
// pragma_index_list, pragma_index_info), holds that schema. SQLite uses an
// index on an expression only for a query holding that same expression, and
// a partial index only where the query's WHERE implies the index's: a query
// for the away sessions by that instant, in its order, leaving out the
// expiry that never runs out as every due-expiry query does, is answered
// from sessions_expiry, with no sort of its own, and a query for one topic's
// held retained replacements is answered from held_publishes_retained, and a
// query for a shared group's assignments of one record, repeating share > 0,
// from session_inflight_shared. Each
// table takes its valid
// rows and refuses each change that breaks one clause of its CHECK and a
// NULL in each column that is not nullable (an INTEGER PRIMARY KEY of a
// rowid table is SQLite's rowid, which a NULL assigns rather than
// refuses); a second current retained value on a topic is refused, at any
// QoS, and records that are not one share topics freely. Each probe is
// rolled back, from tables holding no row: $broadcast's is deleted once
// read.
func TestNewFileHasTheSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.db")
	saguinFile(t, path)
	db := support.OpenDB(t, path)
	if v := pragma[int](t, db, "user_version"); v != 21 {
		t.Errorf("user_version: got %d, want 21", v)
	}
	if mode := pragma[string](t, db, "journal_mode"); mode != "wal" {
		t.Errorf("journal_mode: got %q, want wal", mode)
	}
	checkObjects(t, db)
	checkBroadcastRow(t, db)
	for i := range testkit.Tables {
		table := &testkit.Tables[i]
		t.Run(table.Name, func(t *testing.T) {
			checkTable(t, db, table)
			checkRows(t, db, table)
		})
	}
	for index, want := range map[string]string{
		"records_retained": "records", "sessions_expiry": "sessions", "wills_due": "wills",
		"held_publishes_retained": "held_publishes", "session_inflight_shared": "session_inflight",
	} {
		var table string
		if err := db.QueryRow(`SELECT tbl_name FROM sqlite_schema WHERE type = 'index' AND name = ?`, index).Scan(&table); err != nil {
			t.Errorf("index %s: %v", index, err)
		} else if table != want {
			t.Errorf("index %s is on %s, want %s", index, table, want)
		}
	}
	plan := testkit.QueryPlan(t, db, `SELECT id, client FROM sessions
		WHERE left > 0 AND expiry <> 4294967295 AND left + expiry * 1000 <= 1 ORDER BY left + expiry * 1000`)
	if !slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "USING INDEX sessions_expiry") }) ||
		slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "TEMP B-TREE") }) {
		t.Errorf("the due sessions' query plan, %q, does not read sessions_expiry in its order", plan)
	}
	plan = testkit.QueryPlan(t, db, `SELECT packet_id FROM held_publishes WHERE channel = 1 AND retain = 1 AND topic = ?`, "a")
	if !slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "USING INDEX held_publishes_retained") }) {
		t.Errorf("a topic's held retained replacements' query plan, %q, does not read held_publishes_retained", plan)
	}
	plan = testkit.QueryPlan(t, db, `SELECT session, packet_id FROM session_inflight
		WHERE share > 0 AND share = ? AND channel = ? AND offset = ?`, 1, 2, 1)
	if !slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "INDEX session_inflight_shared") }) {
		t.Errorf("a shared group's assignments of a record: query plan %q does not read session_inflight_shared", plan)
	}
	checkOneRetainedValue(t, db)
}

// checkBroadcastRow checks that db's channels table holds $broadcast's row
// alone, as a new file holds it, then deletes it, so the tables' probes
// start from no row.
func checkBroadcastRow(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT id, name, type, next, floor, bytes FROM channels ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got [][6]any
	for rows.Next() {
		var id, typ, next, floor, size int64
		var name string
		if err := rows.Scan(&id, &name, &typ, &next, &floor, &size); err != nil {
			t.Fatal(err)
		}
		got = append(got, [6]any{id, name, typ, next, floor, size})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][6]any{{int64(1), "$broadcast", int64(1), int64(1), int64(1), int64(0)}}
	if !slices.Equal(got, want) {
		t.Errorf("a new file's channels rows: got %v, want $broadcast's alone, %v", got, want)
	}
	if _, err := db.Exec(`DELETE FROM channels`); err != nil {
		t.Fatal(err)
	}
}

// checkObjects checks that db's schema objects are storage.md's tables and
// indexes only, each table there.
func checkObjects(t *testing.T, db *sql.DB) {
	t.Helper()
	want := map[string]bool{}
	for _, table := range testkit.Tables {
		want[table.Name] = true
	}
	objects, err := db.Query(`SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = objects.Close() }()
	found := map[string]bool{}
	for objects.Next() {
		var typ, name string
		if err := objects.Scan(&typ, &name); err != nil {
			t.Fatal(err)
		}
		switch {
		case typ == "table" && want[name]:
			found[name] = true
		case typ != "index": // each index is checked with its table
			t.Errorf("%s %s is not in storage.md", typ, name)
		}
	}
	if err := objects.Err(); err != nil {
		t.Fatal(err)
	}
	for name := range want {
		if !found[name] {
			t.Errorf("table %s is missing", name)
		}
	}
}

// checkTable compares one table's catalogue entries with table.
func checkTable(t *testing.T, db *sql.DB, table *testkit.Table) {
	t.Helper()
	var withoutRowID, strict bool
	if err := db.QueryRow(`SELECT wr, strict FROM pragma_table_list WHERE schema = 'main' AND name = ?`, table.Name).Scan(&withoutRowID, &strict); err != nil {
		t.Fatal(err)
	}
	if !strict {
		t.Error("not STRICT")
	}
	if withoutRowID != table.WithoutRowID {
		t.Errorf("WITHOUT ROWID: got %v, want %v", withoutRowID, table.WithoutRowID)
	}

	cols, err := db.Query(`SELECT name, upper(type), "notnull", pk FROM pragma_table_info(?) ORDER BY cid`, table.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cols.Close() }()
	got := map[string]testkit.Column{}
	var order []string // the columns' names in the table's order
	for cols.Next() {
		var c testkit.Column
		var notNull bool
		if err := cols.Scan(&c.Name, &c.Type, &notNull, &c.PK); err != nil {
			t.Fatal(err)
		}
		c.Nullable = !notNull
		got[c.Name] = c
		order = append(order, c.Name)
	}
	if err := cols.Err(); err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		want[i] = c.Name
	}
	if !slices.Equal(order, want) {
		t.Errorf("columns in order: got %q, want storage.md's %q", order, want)
	}
	for _, want := range table.Columns {
		c, ok := got[want.Name]
		switch {
		case !ok:
			t.Errorf("column %s is missing", want.Name)
		case c != want:
			t.Errorf("column %s: got %+v, want %+v", want.Name, c, want)
		}
		delete(got, want.Name)
	}
	for _, name := range slices.Sorted(maps.Keys(got)) {
		t.Errorf("column %s is not in storage.md", name)
	}

	if got, want := indexes(t, db, table.Name), slices.Sorted(slices.Values(table.Unique)); !slices.Equal(got, want) {
		t.Errorf("indexes besides the primary key's: got %q, want %q", got, want)
	}
}

// indexes lists a table's indexes but its primary key's, sorted, as
// testkit.Table's Unique gives them.
func indexes(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	list, err := db.Query(`SELECT name, "unique", partial FROM pragma_index_list(?) WHERE origin <> 'pk'`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = list.Close() }()
	var keys []string
	for list.Next() {
		var name string
		var unique, partial bool
		if err := list.Scan(&name, &unique, &partial); err != nil {
			t.Fatal(err)
		}
		key := strings.Join(indexColumns(t, db, name), ",")
		if partial {
			key += " partial"
		}
		if !unique {
			key = "index on " + key
		}
		keys = append(keys, key)
	}
	if err := list.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// indexColumns lists an index's columns in its order, "an expression" for
// a column that is one, which the catalogue does not name.
func indexColumns(t *testing.T, db *sql.DB, index string) []string {
	t.Helper()
	cols, err := db.Query(`SELECT coalesce(name, 'an expression') FROM pragma_index_info(?) ORDER BY seqno`, index)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cols.Close() }()
	var names []string
	for cols.Next() {
		var name string
		if err := cols.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := cols.Err(); err != nil {
		t.Fatal(err)
	}
	return names
}

// checkRows checks that table takes its valid rows and refuses each of its
// refused changes and a NULL in each column that is not nullable.
func checkRows(t *testing.T, db *sql.DB, table *testkit.Table) {
	t.Helper()
	base := table.Valid[0]
	for _, change := range table.Valid {
		if err := insert(db, table.Name, testkit.With(base, change)); err != nil {
			t.Errorf("refused %v: %v", testkit.With(base, change), err)
		}
	}
	for _, change := range table.Refused {
		isConstraint(t, testkit.With(base, change), insert(db, table.Name, testkit.With(base, change)))
	}
	for _, c := range table.Columns {
		if c.Nullable || (!table.WithoutRowID && c.PK == 1 && c.Type == "INTEGER" && len(table.PrimaryKey()) == 1) {
			continue
		}
		r := testkit.With(base, testkit.Row{c.Name: nil})
		isConstraint(t, r, insert(db, table.Name, r))
	}
}

// checkOneRetainedValue probes records_retained, at each QoS: a second
// current retained value on a topic is refused; a record on the topic that
// is not its retained value, one published retained that lost its flag, a
// retained value on another topic and records that are not retained values
// are taken.
func checkOneRetainedValue(t *testing.T, db *sql.DB) {
	t.Helper()
	retained := testkit.Row{"published_retained": 1, "retained": 1}
	plain := testkit.Row{"published_retained": 0, "retained": 0}
	for qos := range 3 {
		base := testkit.With(testkit.TableNamed(t, "records").Valid[0], testkit.Row{"channel": 1, "topic": "status/a", "qos": qos})
		for _, tc := range []struct {
			name          string
			first, second testkit.Row // the records at offsets 1 and 2
			taken         bool
		}{
			{"a second retained value on the topic", retained, retained, false},
			{"a record on the topic that is not its retained value", retained, plain, true},
			{"a published retained record that lost its flag", retained, testkit.Row{"published_retained": 1, "retained": 0}, true},
			{"a retained value on another topic", retained, testkit.With(retained, testkit.Row{"topic": "status/b"}), true},
			{"records that are not retained values", plain, plain, true},
		} {
			second := testkit.With(testkit.With(base, tc.second), testkit.Row{"offset": 2})
			err := insert(db, "records", testkit.With(base, tc.first), second)
			switch {
			case tc.taken && err != nil:
				t.Errorf("%s at QoS %d: refused: %v", tc.name, qos, err)
			case !tc.taken:
				isConstraint(t, second, err)
			}
		}
	}
}

// insert inserts rows into table in one transaction, which it rolls back,
// so every probe starts from the same file.
func insert(db *sql.DB, table string, rows ...testkit.Row) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, r := range rows {
		if err := testkit.InsertRow(tx, table, r); err != nil {
			return err
		}
	}
	return nil
}

// isConstraint checks that err, inserting r, is SQLite's constraint error.
func isConstraint(t *testing.T, r testkit.Row, err error) {
	t.Helper()
	if se, ok := errors.AsType[sqlite3.Error](err); !ok || se.Code != sqlite3.ErrConstraint {
		t.Errorf("inserting %v: got %v, want a constraint error", r, err)
	}
}
