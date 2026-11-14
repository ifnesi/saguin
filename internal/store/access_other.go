//go:build !linux

package store

import "os"

// Memory storage is refused by checkMemoryDir on these platforms, before
// any snapshot access or copying can run.
type databaseAccess struct{}

func readDatabaseAccess(string) (*databaseAccess, error) { return nil, nil }
func checkSnapshotGroup(string) error                    { return nil }
func (*databaseAccess) apply(*os.File, string) (error, error) {
	return nil, nil
}
