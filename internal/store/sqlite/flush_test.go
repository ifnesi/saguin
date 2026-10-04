package sqlite

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// steppedFlusher replaces the flusher's clock with one the test turns: each
// step hands the flusher one tick and returns once that pass has finished
// and the flusher is waiting again, with no other pass begun. An idle
// flusher that syncs is then a count of passes, not a bet on how many ticks
// a real clock delivered in some number of milliseconds, and what a test
// reads after a step is what that pass left.
func steppedFlusher(t *testing.T) (step func(n int)) {
	t.Helper()
	tick := make(chan time.Time)
	// One slot: the flusher announces each wait once, and a step takes the
	// announcement before the next tick, so it never blocks the flusher -
	// and a Close with no step taken finds the flusher in its select.
	waiting := make(chan struct{}, 1)
	oldTicker, oldWaiting := flushTicker, flushWaiting
	flushTicker = func(time.Duration) (<-chan time.Time, func()) { return tick, func() {} }
	flushWaiting = func() { waiting <- struct{}{} }
	t.Cleanup(func() { flushTicker, flushWaiting = oldTicker, oldWaiting })
	await := func() {
		t.Helper()
		select {
		case <-waiting:
		case <-time.After(5 * time.Second):
			t.Fatal("the flusher did not come back to wait for a tick within 5s")
		}
	}
	primed := false
	return func(n int) {
		t.Helper()
		if !primed {
			await() // the wait before the first tick
			primed = true
		}
		for range n {
			select {
			case tick <- time.Now():
			case <-time.After(5 * time.Second):
				t.Fatal("the flusher took no tick for 5s: it is not running")
			}
			await()
		}
	}
}

// stepUntil steps the flusher until ok holds; at most a thousand passes.
func stepUntil(t *testing.T, step func(int), what string, ok func() bool) {
	t.Helper()
	for i := 0; !ok(); i++ {
		if i == 1000 {
			t.Fatalf("%s within 1000 passes", what)
		}
		step(1)
	}
}

// RFC 0004 "WAL, and synchronous=NORMAL": the flusher forces the log to disk
// after commits and never while idle, and Close stops it before the final
// checkpoint. Counted through the walSync seam, so a flusher that fsyncs
// nothing cannot pass.
func TestTheFlusherForcesTheLogOnlyAfterCommitsAndStopsAtClose(t *testing.T) {
	var syncs atomic.Int64
	old := walSync
	walSync = func(f *os.File) error { syncs.Add(1); return f.Sync() }
	t.Cleanup(func() { walSync = old })

	db, err := Open(tempPath(t), "0.1.0-test")
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	step := steppedFlusher(t)
	db.SetFlushInterval(10*time.Millisecond, nil)
	log, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}

	// Creating the channel committed; let that flush, then be idle: many
	// passes, no commit, no fsync.
	stepUntil(t, step, "the channel's creation was never flushed", func() bool { return syncs.Load() > 0 })
	base := syncs.Load()
	step(20)
	if n := syncs.Load(); n != base {
		t.Fatalf("fsyncs went from %d to %d in 20 passes while nothing was committed", base, n)
	}

	// A commit is flushed.
	if _, err := log.Append(store.Record{Topic: "t", Payload: []byte("a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	stepUntil(t, step, "a commit was never flushed", func() bool { return syncs.Load() > base })

	// Flushed once, then idle again: the count stays.
	settled := syncs.Load()
	step(20)
	if n := syncs.Load(); n != settled {
		t.Fatalf("fsyncs went from %d to %d in 20 passes with nothing committed", settled, n)
	}

	// Stopped at Close: the flusher has returned by the time Close does, so
	// nothing is left to take a tick, and no fsync can follow.
	closed = true
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-db.flush.done:
	default:
		t.Fatal("Close returned with the flusher still running")
	}
}

// A failed fsync is reported, every failure, with the first of a streak
// marked, and the flusher keeps trying and recovers.
func TestAFailedFlushIsReportedAndRetried(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var syncs atomic.Int64
	old := walSync
	walSync = func(f *os.File) error {
		syncs.Add(1)
		if fail.Load() {
			return errors.New("input/output error")
		}
		return f.Sync()
	}
	t.Cleanup(func() { walSync = old })

	db, err := Open(tempPath(t), "0.1.0-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var failures, firsts atomic.Int64
	step := steppedFlusher(t)
	db.SetFlushInterval(10*time.Millisecond, func(err error, first bool) {
		if err == nil || !strings.Contains(err.Error(), "input/output error") {
			t.Errorf("reported %v, want the fsync's error", err)
		}
		failures.Add(1)
		if first {
			firsts.Add(1)
		}
	})
	log, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(store.Record{Topic: "t", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, step, "fewer than 3 failures reported", func() bool { return failures.Load() >= 3 })
	if n := firsts.Load(); n != 1 {
		t.Fatalf("%d failures marked first in one streak, want 1", n)
	}
	// Recovery ends the streak: the next failure is a first again.
	fail.Store(false)
	before := syncs.Load()
	stepUntil(t, step, "the flusher never synced again", func() bool { return syncs.Load() > before })
	fail.Store(true)
	if _, err := log.Append(store.Record{Topic: "t", Payload: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	stepUntil(t, step, "a new streak was never marked first", func() bool { return firsts.Load() >= 2 })
}

// RFC 0004 "A flush that fails is never silent": an error finding or opening
// the log is a failed flush too, whatever it is, except a log that does not
// exist. After an acknowledged commit, the injected error is reported with
// the first of a streak marked, the flusher retries every tick, and a
// successful sync ends the streak, so the next failure is a first again.
func TestAnErrorFindingOrOpeningTheLogIsReportedAndRetried(t *testing.T) {
	denied := &os.PathError{Op: "open", Path: "wal", Err: os.ErrPermission}
	// The seams are installed once, before the flusher starts, and consult
	// a flag, so the test never writes them while the flusher reads them.
	var failOpen, failStat atomic.Bool
	oldOpen, oldStat := walOpen, walStat
	walOpen = func(n string) (*os.File, error) {
		if failOpen.Load() {
			return nil, denied
		}
		return oldOpen(n)
	}
	walStat = func(n string) (os.FileInfo, error) {
		if failStat.Load() {
			return nil, denied
		}
		return oldStat(n)
	}
	t.Cleanup(func() { walOpen, walStat = oldOpen, oldStat })
	cases := []struct {
		name string
		// flag is the failure; the open case must fail before the flusher
		// holds a handle, the stat case after.
		flag    *atomic.Bool
		upFront bool
		streaks int64
		// reports the failures to wait for: an open that keeps failing is
		// retried every tick; a stat error drops the handle, and the next
		// tick opens a new one, so it is reported once and then recovers.
		reports int64
	}{
		{"open", &failOpen, true, 1, 3},
		{"stat", &failStat, false, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var syncs atomic.Int64
			oldSync := walSync
			walSync = func(f *os.File) error { syncs.Add(1); return f.Sync() }
			t.Cleanup(func() { walSync = oldSync })

			db, err := Open(tempPath(t), "0.1.0-test")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			log, err := db.Log("events")
			if err != nil {
				t.Fatal(err)
			}
			var failures, firsts atomic.Int64
			var bad atomic.Value
			step := steppedFlusher(t)
			wait := func(what string, ok func() bool) {
				t.Helper()
				stepUntil(t, step, fmt.Sprintf("%s (when it began: failures %d, firsts %d, syncs %d)",
					what, failures.Load(), firsts.Load(), syncs.Load()), ok)
			}
			commit := func() {
				t.Helper()
				if _, err := log.Append(store.Record{Topic: "t", Payload: []byte("x")}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			t.Cleanup(func() { tc.flag.Store(false) })
			if tc.upFront {
				tc.flag.Store(true)
			}
			db.SetFlushInterval(10*time.Millisecond, func(err error, first bool) {
				if !errors.Is(err, os.ErrPermission) {
					bad.Store(err)
				}
				failures.Add(1)
				if first {
					firsts.Add(1)
				}
			})
			if !tc.upFront {
				// Let the flusher open and sync the log first.
				wait("the log was never synced", func() bool { return syncs.Load() > 0 })
			}
			for streak := int64(1); streak <= tc.streaks; streak++ {
				tc.flag.Store(true)
				commit()
				wait("the error was not reported", func() bool { return failures.Load() >= tc.reports*streak })
				if n := firsts.Load(); n != streak {
					t.Fatalf("%d failures marked first, want %d", n, streak)
				}
				tc.flag.Store(false)
				before := syncs.Load()
				wait("the flusher never synced again", func() bool { return syncs.Load() > before })
			}
			if e := bad.Load(); e != nil {
				t.Fatalf("reported %v, want an error wrapping the injected one", e)
			}
		})
	}
}

// A log that does not exist is nothing to flush, not a failure.
func TestAMissingLogIsNotAFailedFlush(t *testing.T) {
	old := walOpen
	var opens atomic.Int64
	walOpen = func(n string) (*os.File, error) {
		opens.Add(1)
		return nil, &os.PathError{Op: "open", Path: n, Err: os.ErrNotExist}
	}
	t.Cleanup(func() { walOpen = old })
	db, err := Open(tempPath(t), "0.1.0-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var failures atomic.Int64
	const every = 10 * time.Millisecond
	db.SetFlushInterval(every, func(error, bool) { failures.Add(1) })
	log, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(store.Record{Topic: "t", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for opens.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatal("the flusher never tried to open the log")
		}
		time.Sleep(every)
	}
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d failures reported for a log that does not exist", n)
	}
}
