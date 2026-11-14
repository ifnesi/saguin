//go:build !linux

package store

import "fmt"

func checkMemoryDir(path string) error {
	return fmt.Errorf("memory_dir %s: memory storage requires a tmpfs on Linux", path)
}
