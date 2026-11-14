package store

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/ifnesi/saguin/internal/config"
)

// snapshotError is an unsuccessful snapshot with a usable working store.
// Connection errors remain ordinary store errors and take precedence
// over a failed copy when ending its read transaction.
type snapshotError struct{ error }

func (e *snapshotError) Unwrap() error { return e.error }

// SnapshotFailed distinguishes a failed snapshot from a broken store.
func SnapshotFailed(err error) bool {
	var failure *snapshotError
	return errors.As(err, &failure)
}

// SnapshotFold runs alone in the write lane, outside a transaction. Its
// answer precedes the ordered read which pins the file for SnapshotCopy.
type SnapshotFold struct{}

func (*SnapshotFold) standalone() {}

// Records is zero: a checkpoint commits no transaction or record.
func (*SnapshotFold) Records() int { return 0 }

// WriteTx folds the whole WAL without deleting any uncheckpointed commit.
func (*SnapshotFold) WriteTx(tx *Tx) error {
	err := checkpoint(tx.Prepared(checkpointLog))
	if diskFull(err) || errors.Is(err, errIncompleteCheckpoint) {
		return &snapshotError{err}
	}
	return err
}

// SnapshotCopy holds the folded file's read view until its byte copy has
// replaced the disk database and that rename is safe on disk.
type SnapshotCopy struct {
	Source, Destination string
	GroupWarning        error // reported through the core, even when the snapshot succeeds
}

// ReadConn runs inside the reader's established view. File errors fail the
// snapshot, without making the SQLite connection or the working store fail.
func (s *SnapshotCopy) ReadConn(*Conn) error {
	if err := s.copy(); err != nil {
		return &snapshotError{err}
	}
	return nil
}

func (s *SnapshotCopy) copy() (err error) {
	staged := config.FilesForDatabase(s.Destination).Snapshot
	if err := os.Remove(staged); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	access, err := readDatabaseAccess(s.Destination)
	if err != nil {
		return err
	}
	s.GroupWarning, err = copyDatabase(s.Source, staged, access, s.Destination)
	if err != nil {
		return err
	}
	if err := os.Rename(staged, s.Destination); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.Destination))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	return dir.Sync()
}
