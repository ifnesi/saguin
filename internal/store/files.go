package store

import (
	"errors"
	"os"
)

// createPrivateFile creates a new file exclusively. Setting its mode through
// the new descriptor restores owner access even under a restrictive umask,
// without ever changing the permissions of an existing file.
func createPrivateFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(0o600); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}
