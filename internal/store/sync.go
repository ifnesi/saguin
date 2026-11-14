package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// Sync takes a place in the writer's request order, outside a transaction.
// It syncs the log itself even when an open read prevents SQLite folding it.
type Sync struct{}

func (*Sync) standalone() {}

// Records is zero because a sync commits no transaction or record.
func (*Sync) Records() int { return 0 }

// WriteTx runs on the writer without a SQLite transaction. Opening the WAL
// only for this request leaves no extra lifetime file or close-time owner.
// File-sync errors are ordinary I/O errors, never SQLite full refusals.
func (*Sync) WriteTx(tx *Tx) (err error) {
	wal, err := os.OpenFile(tx.walPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, wal.Close()) }()
	if err := syscall.Fdatasync(int(wal.Fd())); err != nil {
		return &os.PathError{Op: "fdatasync", Path: tx.walPath, Err: err}
	}
	return nil
}

// checkpoint completes the log before a memory-mode byte copy.
var errIncompleteCheckpoint = errors.New("incomplete checkpoint")

func checkpoint(stmt *sql.Stmt) error {
	var busy, frames, folded int
	if err := stmt.QueryRowContext(context.Background()).Scan(&busy, &frames, &folded); err != nil {
		return err
	}
	if busy != 0 || frames != folded {
		return fmt.Errorf("%w: busy %d, folded %d of %d log frames", errIncompleteCheckpoint, busy, folded, frames)
	}
	return nil
}
