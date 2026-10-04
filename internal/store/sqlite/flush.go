package sqlite

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// The provider runs with synchronous=NORMAL, so a commit reaches the
// write-ahead log without waiting for the disk: fast, and a power cut can
// lose what was committed since the log last reached it (RFC 0004 "WAL, and
// synchronous=NORMAL"). SetFlushInterval bounds that: every interval a
// goroutine fsyncs the log, so what a power cut can lose is about one
// interval plus one fsync, never a hole below a surviving record and never
// the file.
//
// The fsync is on a read-only handle of the `-wal` file, which flushes the
// file's data whoever wrote it, so it takes no lock the writer needs. It is
// skipped when nothing has committed since the last one, so an idle broker
// writes nothing, and the handle is opened again when the log is a
// different file (removed by the last connection closing, or recreated).

// walSync is what forces an open log to disk. A variable only so a test can
// count the calls.
var walSync = func(f *os.File) error { return f.Sync() }

// walStat and walOpen are how the flusher finds and opens the log by path.
// Variables only so a test can make them fail.
var (
	walStat = os.Stat
	walOpen = func(name string) (*os.File, error) { return os.Open(name) }
)

// flushTicker is the flusher's clock, and flushWaiting, where set, is called
// each time the flusher is about to wait for a tick - once before the first,
// and after every pass. Variables only so a test can step the flusher one
// pass at a time and know each has finished: an idle flusher syncing
// nothing is then a count of passes rather than of milliseconds.
var (
	flushTicker = func(d time.Duration) (<-chan time.Time, func()) {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	flushWaiting func()
)

type flusher struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// SetFlushInterval starts forcing the write-ahead log to disk every
// interval; an interval of zero or less starts nothing. Called once, before
// any listener opens, and stopped by Close.
//
// **A failed fsync is not silent**, because it can mean commits that were
// acknowledged did not reach the disk. onFail is called with each failure,
// and first is true for the first of a streak of them, so the caller can
// count every one and say it once. The flusher keeps trying every tick.
func (d *DB) SetFlushInterval(interval time.Duration, onFail func(err error, first bool)) {
	if interval <= 0 || d.flush != nil {
		return
	}
	f := &flusher{stop: make(chan struct{}), done: make(chan struct{})}
	d.flush = f
	go d.flushLoop(f, interval, onFail)
}

func (d *DB) flushLoop(f *flusher, interval time.Duration, onFail func(error, bool)) {
	defer close(f.done)
	tick, stop := flushTicker(interval)
	defer stop()
	var wal *os.File
	defer func() {
		if wal != nil {
			_ = wal.Close()
		}
	}()
	// Read before the fsync, not after: a commit that lands during it makes
	// the next tick sync again rather than being counted as flushed.
	var flushed int64
	failing := false
	for {
		if flushWaiting != nil {
			flushWaiting()
		}
		select {
		case <-f.stop:
			return
		case <-tick:
		}
		c := d.committed.Load()
		if c == flushed {
			continue
		}
		var err error
		if wal != nil {
			// A log that is no longer the file at this path is one the
			// commits are not in. A missing path is that; any other stat
			// error is a failure to look, and is reported below.
			var cur, old os.FileInfo
			cur, err = walStat(d.path + "-wal")
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
			if err != nil {
				_ = wal.Close()
				wal = nil
			} else if old, err = wal.Stat(); err != nil {
				_ = wal.Close()
				wal = nil
			} else if cur == nil || !os.SameFile(cur, old) {
				_ = wal.Close()
				wal = nil
			}
		}
		if wal == nil && err == nil {
			var w *os.File
			w, err = walOpen(d.path + "-wal")
			if errors.Is(err, os.ErrNotExist) {
				// Nothing written yet, or checkpointed away: nothing to
				// flush, and a streak in progress stays open.
				continue
			}
			if err == nil {
				wal = w
			}
		}
		if err == nil {
			err = walSync(wal)
		}
		if err != nil {
			// Every other error, an open refused or out of descriptors
			// included, can mean acknowledged commits are not on disk.
			if onFail != nil {
				onFail(fmt.Errorf("storage %s: forcing the write-ahead log to disk: %w", d.path, err), !failing)
			}
			failing = true
			continue
		}
		flushed, failing = c, false
	}
}

// stopFlusher ends the flusher and waits for it, so that no fsync is in
// flight when the final checkpoint runs.
func (d *DB) stopFlusher() {
	f := d.flush
	if f == nil {
		return
	}
	f.once.Do(func() { close(f.stop) })
	<-f.done
}
