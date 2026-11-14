package support

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
)

// FileAlone opens the database file at path through SQLite with its
// immutable flag, which has SQLite read that file alone, never a -wal or
// -shm beside it, and take no lock: what it finds is in the file itself.
// SQLite keeps the pages an immutable file's connection has read, assuming
// they cannot change, so each query is given a connection of its own.
func FileAlone(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { _ = db.Close() }) // the test is over
	return db
}

// HoldsInFile reports whether the database file read by db (fileAlone)
// holds a record of topic, and the error of a read that could not tell:
// the broker may be writing the pages the read meets.
func HoldsInFile(db *sql.DB, topic string) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM records WHERE topic = ?`, topic).Scan(&n)
	return n > 0, err
}

// MemoryDir returns the tmpfs a memory-mode test's broker has as its
// memory_dir, a filesystem of size bytes mounted for the test alone by
// testkit.OnSmallDisk; "" where the test has instead been run again, alone,
// on one, and has nothing left to do.
func MemoryDir(t *testing.T, size int) string {
	t.Helper()
	ram := os.Getenv(SmallDisk)
	if ram == "" {
		testkit.OnSmallDisk(t, SmallDisk, size, AloneLimit)
	}
	return ram
}
