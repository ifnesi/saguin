package store

import (
	"context"

	"github.com/ifnesi/saguin/internal/metrics"
)

// writer owns its lifetime connection and executes each received write as
// one transaction, or standalone work outside it. Membership and ordering
// belong to the core.
type writer struct {
	s       *Store
	conn    *Conn
	tx      Tx
	failure Answer // first runtime failure; Op names an answer carrying it, if any
	full    *metrics.Gauge
}

func (w *writer) run() {
	defer w.finish()
	// A blocking receive only wakes the loop. Retain what it took so the
	// same fixed order applies even if several channels became ready.
	var write Write
	var failure Answer
	for {
		// Store's order is stop, reader failure, then writes.
		select {
		case <-w.s.stop:
			w.fail(failure)
			return
		default:
		}
		w.fail(failure)
		failure = Answer{}
		w.checkFailures()
		if write == nil {
			select {
			case write = <-w.s.writes:
			default:
			}
		}
		if write != nil {
			err := w.failure.Err
			if err == nil {
				err = w.write(write)
			}
			w.s.answers <- Answer{Op: write, Err: err}
			if w.failure.Err != nil {
				w.failure.Op = write
			}
			write = nil
			continue
		}
		select {
		case <-w.s.stop:
		case failure = <-w.s.failures:
		case write = <-w.s.writes:
		}
	}
}

func (w *writer) write(op Write) error {
	_, standalone := op.(interface{ standalone() })
	if !standalone {
		var err error
		w.tx.Tx, err = w.conn.BeginTx(context.Background(), nil)
		if err != nil {
			return w.writeFailed(err)
		}
	}
	if err := op.WriteTx(&w.tx); err != nil {
		if standalone {
			if SnapshotFailed(err) {
				return err
			}
			// Work following committed writes cannot refuse a transaction.
			w.fail(Answer{Err: err})
			return w.failure.Err
		}
		return w.writeFailed(err)
	}
	// A reader failure already reported wins over committing this write.
	w.checkFailures()
	if w.failure.Err != nil {
		return w.failure.Err
	}
	if standalone {
		return nil
	}
	err := w.tx.Commit()
	// The driver attempts rollback after a failed commit before database/sql
	// releases the transaction. diskFull accepts only guaranteed rollbacks.
	w.tx.Tx = nil
	if err != nil {
		return w.writeFailed(err)
	}
	w.full.Set(0)
	return nil
}

// fail latches the first store error and rolls back the current transaction.
// Later writes are answered with it without executing their callbacks.
func (w *writer) fail(failure Answer) {
	if failure.Err == nil || w.failure.Err != nil {
		return
	}
	w.failure = failure
	w.rollback()
}

func (w *writer) rollback() {
	if w.tx.Tx != nil {
		// SQLITE_FULL may already have rolled SQLite's transaction back.
		// Rollback still releases database/sql's transaction ownership.
		_ = w.tx.Rollback()
		w.tx.Tx = nil
	}
}

func (w *writer) checkFailures() {
	for {
		select {
		case failure := <-w.s.failures:
			w.fail(failure)
		default:
			return
		}
	}
}

func (w *writer) finish() {
	// Main has drained every request. Readers release their actual SQLite
	// connections before the writer, whose close folds WAL into the file.
	var err error
	for range cap(w.s.readersDone) {
		if closeErr := <-w.s.readersDone; err == nil {
			err = closeErr
		}
	}
	w.checkFailures()
	if w.failure.Op == nil && w.failure.Err != nil {
		err = w.failure.Err
	}
	if closeErr := w.conn.close(); err == nil {
		err = closeErr
	}
	w.s.done <- err
}
