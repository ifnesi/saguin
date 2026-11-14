package store

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/ifnesi/saguin/internal/config"
)

// FileLock holds the live storage lock until released or handed to Start.
// Its file is separate from SQLite's files and is never deleted.
type FileLock struct {
	file *os.File
}

// Lock takes cfg's storage lock exclusively without waiting. Opening does
// not truncate a stale lock file. A new file is readable and writable by
// its owner alone. The caller owns the lock until Start.
func Lock(cfg *config.Config) (*FileLock, error) {
	if err := cfg.ResolveStoragePaths(); err != nil {
		return nil, err
	}
	return LockFile(cfg.Broker.Storage.LockPath())
}

// LockFile takes a live exclusive lock on path without waiting. Database
// files and Unix sockets share this lock; releasing it never removes its file.
func LockFile(path string) (*FileLock, error) {
	f, err := createPrivateFile(path)
	if errors.Is(err, os.ErrExist) {
		f, err = os.OpenFile(path, os.O_RDWR, 0)
	}
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("storage lock %s is held by another process: %w", path, err)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return &FileLock{file: f}, nil
}

// Release releases the lock without deleting its file. Callers use it only
// before handing the lock to Start; afterwards, the store releases it.
// Closing the descriptor releases the lock, including on a process crash.
func (l *FileLock) Release() error {
	return l.file.Close()
}

// releaseLocks releases every lock even when an earlier close fails.
func releaseLocks(locks []*FileLock) (err error) {
	for _, lock := range locks {
		err = errors.Join(err, lock.Release())
	}
	return err
}
