package store_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Store: "It runs in WAL mode, with synchronous=FULL except in
// normal mode"; "normal mode runs on file_path with synchronous=NORMAL".
// Start reads each connection's journal_mode and synchronous back and
// refuses to run otherwise, naming the file and the setting. SQLite can bring
// a connection up without what its connection string asked, silently, where
// shared memory is missing, which no file_path selects; here the connection
// string itself asks for SQLite's default rollback journal on a new file, or
// for the synchronous level of the other modes, NORMAL in full mode and FULL
// in normal mode, so the driver opens the connection without complaint and
// only the read-back can refuse it.
func TestStartReadsTheSettingsBack(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string // broker.storage.mode, "" for config.md's default, full
		readers    int    // read_connections
		onReaders  bool   // the readers' connections ask otherwise, else the writer's
		key, value string // the driver's setting asked for, "" for none
		setting    string // the setting the refusal names
	}{
		{"the writer in the rollback journal", "", 2, false, "_journal_mode", "", "journal_mode"},
		{"the writer at synchronous NORMAL", "", 2, false, "_synchronous", "NORMAL", "synchronous"},
		{"the readers at synchronous NORMAL", "", 2, true, "_synchronous", "NORMAL", "synchronous"},
		{"normal mode, the writer at synchronous FULL", "normal", 2, false, "_synchronous", "FULL", "synchronous"},
		{"normal mode, the readers at synchronous FULL", "normal", 2, true, "_synchronous", "FULL", "synchronous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := inMode(t, tc.readers, tc.mode, "")
			path := cfg.Broker.Storage.FilePath
			store.AskFor(t, tc.onReaders, tc.key, tc.value)
			id := testkit.Goid()
			s, _, err := lockAndStart(t, cfg, metrics.New())
			if err == nil {
				_ = s.Stop()
				t.Fatalf("Start ran with %s=%q asked", tc.key, tc.value)
			}
			msg := err.Error()
			if !strings.Contains(msg, path) || !strings.Contains(strings.ReplaceAll(msg, path, ""), tc.setting) {
				t.Errorf("the error does not name the file %s and the setting %s: %v", path, tc.setting, err)
			}
			waitFamily(t, id)
		})
	}
}

// design.md, Restart: "then opens the store. The core loads channels,
// bridges, and the counts and next deadlines without loading every retained
// value or unfinished job. The counts and deadlines are defined in
// Execution, Resource bounds";
// Store, full mode: "A restart recovers committed state". A new file loads
// $broadcast's row alone (storage.md: "When the file is created, the tables,
// the $broadcast row in channels and the version are written in one
// transaction"; Ids: "The $broadcast channel is channel 1"); the core adds
// the other channels rows (ids the core assigns), and a reopened version-21
// file loads them all back as stored, whatever read_connections (config.md:
// "2 to 8").
func TestStartLoadsTheChannels(t *testing.T) {
	want := []store.Channel{
		broadcastChannel,
		{ID: 2, Name: "orders", Type: store.Queue, Next: 9, Floor: 4, Bytes: 120},
		{ID: 3, Name: "orders__dlq", Type: store.Append, Next: 1, Floor: 1},
		{ID: 4, Name: "prices", Type: store.Latest, Next: 2, Floor: 1, Bytes: 7},
	}
	for _, readers := range []int{2, 8} {
		t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
			cfg := newConfig(t, readers)
			r := start(t, cfg)
			if !slices.Equal(r.state.Channels, want[:1]) {
				t.Errorf("a new file loads channels %+v, want $broadcast's alone, %+v", r.state.Channels, want[:1])
			}
			if err := write(t, r, &store.AddChannels{Channels: slices.Clone(want[1:])}); err != nil {
				t.Fatal(err)
			}
			stop(t, r)

			r = start(t, cfg)
			stop(t, r)
			if !slices.Equal(r.state.Channels, want) {
				t.Errorf("reopened, the channels load as %+v, want %+v", r.state.Channels, want)
			}

			path := cfg.Broker.Storage.FilePath
			if got := userVersion(t, openFile(t, path)); got != 21 {
				t.Errorf("user_version after reopening: got %d, want 21", got)
			}
			wantRows := [][6]any{
				broadcastRow,
				{int64(2), "orders", int64(3), int64(9), int64(4), int64(120)},
				{int64(3), "orders__dlq", int64(1), int64(1), int64(1), int64(0)},
				{int64(4), "prices", int64(2), int64(2), int64(1), int64(7)},
			}
			if got := storedChannels(t, path); !slices.Equal(got, wantRows) {
				t.Errorf("channels rows: got %v, want %v", got, wantRows)
			}
		})
	}
}
