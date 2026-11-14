package store

import (
	"errors"
	"syscall"

	"github.com/mattn/go-sqlite3"
)

// writeFailed rolls back and classifies the whole transaction's failure.
// ErrFull refuses it without latching an error, so the next write can run.
func (w *writer) writeFailed(err error) error {
	if !diskFull(err) {
		w.fail(Answer{Err: err})
		return w.failure.Err
	}
	w.rollback()
	w.full.Set(1)
	return ErrFull
}

// diskFull recognizes SQLITE_FULL and SQLITE_IOERR_WRITE with EDQUOT.
// SQLite's unixWrite maps ENOSPC to SQLITE_FULL and EDQUOT to SQLITE_IOERR_WRITE;
// either leaves the frame unwritten and guarantees rollback. Fsync and -shm
// growth errors can follow commit frames, so their outcome is unknown and
// they must stop the broker whatever their errno (R8).
func diskFull(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code == sqlite3.ErrFull ||
		(sqliteErr.ExtendedCode == sqlite3.ErrIoErrWrite && sqliteErr.SystemErrno == syscall.EDQUOT)
}
