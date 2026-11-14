package passwd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// Save syncs a complete replacement beside the original and renames it over
// that file. Failures before rename leave the original untouched. Syncing the
// directory also makes the installed name durable. Temporary files are removed
// on every failure. An existing file's owner and mode must both survive.
func (f *File) Save() error {
	info, err := os.Stat(f.name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	dir, err := os.Open(filepath.Dir(f.name))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }() // read-only directory handle
	tmp, err := os.CreateTemp(filepath.Dir(f.name), ".passwd-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()           // already checked before rename on success
		_ = os.Remove(tmp.Name()) // absent after a successful rename
	}()
	if _, err := tmp.WriteString(f.text); err != nil {
		return err
	}
	// Writing can clear special permission bits, so set metadata only once
	// the replacement is complete, before syncing and installing it.
	if err := preserveMetadata(tmp, info); err != nil {
		return fmt.Errorf("%s: preserve password file metadata: %w", f.name, err)
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), f.name); err != nil {
		return err
	}
	return dir.Sync()
}

func preserveMetadata(file *os.File, previous fs.FileInfo) error {
	mode := fs.FileMode(0o600)
	if previous != nil {
		owner, ok := previous.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("cannot determine the original owner")
		}
		current, err := file.Stat()
		if err != nil {
			return err
		}
		now, ok := current.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("cannot determine the replacement's owner")
		}
		if now.Uid != owner.Uid || now.Gid != owner.Gid {
			if err := file.Chown(int(owner.Uid), int(owner.Gid)); err != nil {
				return err
			}
		}
		mode = previous.Mode()
	}
	// Chown can clear special permission bits, so restore the mode last.
	return file.Chmod(mode)
}
