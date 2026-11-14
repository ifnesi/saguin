package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ifnesi/saguin/internal/config"
)

// memoryLock protects the RAM copy before any leftover file can be replaced.
func (s *startup) memoryLock(cfg *config.Config) error {
	dir := cfg.Broker.Storage.MemoryDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	cfg.Broker.Storage.MemoryDir = dir
	if err := checkMemoryDir(dir); err != nil {
		return err
	}
	lock, err := LockFile(config.FilesForDatabase(cfg.MemoryPath()).Lock)
	if err != nil {
		return err
	}
	s.locks = append(s.locks, lock)
	return nil
}

// copyDatabase copies only a completely checkpointed database. The RAM lock
// is held, and no running connection uses the destination. Unlinking the
// abandoned files also prevents following a leftover link when creating it.
func (s *startup) copyDatabase(source, destination string) error {
	if err := removeDatabase(destination); err != nil {
		return err
	}
	// A refused start owns even a partially written copy.
	s.memoryPath = destination
	_, err := copyDatabase(source, destination, nil, "")
	return err
}

// copyDatabase creates a private, exclusive byte copy, applies its final access
// and syncs it before closing. Callers remove leftovers while holding the
// destination's lock. Only a snapshot passes the grant on the file it replaces.
func copyDatabase(source, destination string, access *databaseAccess, path string) (warning, err error) {
	src, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	dst, err := createPrivateFile(destination)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, dst.Close()) }()
	if _, err := io.Copy(dst, src); err != nil {
		return nil, fmt.Errorf("copy database to %s: %w", destination, err)
	}
	// Writing can clear special permission bits, so restore the mode only
	// after the copy is complete, before syncing and installing it.
	warning, err = access.apply(dst, path)
	if err != nil {
		return warning, err
	}
	return warning, dst.Sync()
}

// removeDatabase removes data files only, retaining the live lock's inode.
// Every file is attempted so one failure cannot leave unrelated sidecars.
func removeDatabase(path string) (err error) {
	for _, path := range config.FilesForDatabase(path).Data {
		if removeErr := os.Remove(path); !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}
