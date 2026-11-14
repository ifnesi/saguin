package store

import (
	"fmt"
	"syscall"
)

// checkMemoryDir refuses disk-backed working copies in memory mode.
func checkMemoryDir(path string) error {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return err
	}
	const tmpfsMagic = 0x01021994
	if fs.Type != tmpfsMagic {
		return fmt.Errorf("memory_dir %s must be a tmpfs", path)
	}
	return nil
}
