package broker

// What a periodic snapshot would cost, which is the number to have before
// anybody builds one.
//
// The question is not how long a snapshot takes. It is how long publishes
// stop, because the broker has one lock on the publish path and everything
// waits behind it. So the interesting figure is the part of SaveSnapshots
// that runs holding that lock, and the shutdown path deliberately keeps the
// file write outside it: state is copied under the lock, the lock goes
// back, and only then is anything written.
//
// These are internal benchmarks because the locked section is internal.

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

func benchBroker(tb testing.TB, chans []*channel.Channel, dir string) *Broker {
	tb.Helper()
	reg, err := channel.NewRegistry(chans)
	if err != nil {
		tb.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if dir != "" {
		b.SetSnapshotDirs(map[string]*store.Dir{"local": store.NewDir(dir, "0.1.0-bench")})
	}
	return b
}

// BenchmarkSnapshotCopyUnderLock is the pause a periodic snapshot would
// impose on every publisher: one channel's state copied while the broker
// lock is held. 128-byte payloads, the size every published figure for
// this broker uses.
func BenchmarkSnapshotCopyUnderLock(b *testing.B) {
	payload := []byte(strings.Repeat("x", 128))

	for _, records := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d-records", records), func(b *testing.B) {
			br := benchBroker(b, []*channel.Channel{
				{Name: "events", Type: channel.Append, Storage: "local"},
			}, b.TempDir())
			lg := br.logs["events"]
			for i := range records {
				if _, err := lg.Append(store.Record{
					MessageID: "m", Topic: "events/x", Payload: payload, Offset: uint64(i),
				}); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
			c := br.reg.Get("events")

			b.ResetTimer()
			for range b.N {
				br.mu.Lock()
				if _, ok := br.snapshot(c); !ok {
					b.Fatal("nothing to snapshot")
				}
				br.mu.Unlock()
			}
		})
	}
}

// BenchmarkSnapshotChannelSelection is the other term in the locked
// section, and the one that does not depend on how much data there is:
// snapshotChannels walks every channel and sorts the result, on every call.
func BenchmarkSnapshotChannelSelection(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d-channels", n), func(b *testing.B) {
			chans := make([]*channel.Channel, 0, n)
			for i := range n {
				chans = append(chans, &channel.Channel{
					Name: fmt.Sprintf("c%06d", i), Type: channel.Append, Storage: "local",
				})
			}
			br := benchBroker(b, chans, b.TempDir())

			b.ResetTimer()
			for range b.N {
				br.mu.Lock()
				_ = br.snapshotChannels()
				br.mu.Unlock()
			}
		})
	}
}

// BenchmarkSnapshotWrite is what happens after the lock is released: the
// encode and the write to disk, with its fsyncs. It is here to be compared
// against the pause above rather than added to it - nothing waits on this
// today, and a periodic snapshot that changed that is the thing to refuse.
func BenchmarkSnapshotWrite(b *testing.B) {
	payload := []byte(strings.Repeat("x", 128))

	for _, records := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("%d-records", records), func(b *testing.B) {
			dir := b.TempDir()
			br := benchBroker(b, []*channel.Channel{
				{Name: "events", Type: channel.Append, Storage: "local"},
			}, dir)
			lg := br.logs["events"]
			for i := range records {
				if _, err := lg.Append(store.Record{
					MessageID: "m", Topic: "events/x", Payload: payload, Offset: uint64(i),
				}); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
			c := br.reg.Get("events")
			br.mu.Lock()
			snap, ok := br.snapshot(c)
			br.mu.Unlock()
			if !ok {
				b.Fatal("nothing to snapshot")
			}
			d := br.dirs["local"]

			b.ResetTimer()
			for range b.N {
				if err := d.Save([]*store.Snapshot{snap}); err != nil {
					b.Fatalf("save: %v", err)
				}
			}
		})
	}
}

// snapshotBenchRecords is what each channel holds in the benchmark below.
//
// **Small on purpose.** The benchmarks above vary what one channel holds
// and are dominated by the encode; this one holds every channel to a
// handful so that the only thing moving is the number of files. A few
// records rather than none, because a channel with nothing in it is not
// snapshotted at all and ten thousand empty ones would measure a loop that
// writes no files.
const snapshotBenchRecords = 8

// BenchmarkShutdownSnapshotAcrossChannels is what an operator waits through
// at shutdown as the number of channels rises: the state copied under the
// lock, then one file per channel each ending in an fsync, plus the
// manifest and two directory syncs.
//
// **This is the whole of SaveSnapshots rather than either half**, unlike
// the three above, because the question here is not where the time goes but
// how long the shutdown takes. Nothing is publishing by then - the
// listeners are closed and every client is gone - so the locked section
// costs an operator exactly what the write does.
//
// **The figure is worthless without its filesystem, and the default is the
// wrong one.** `b.TempDir()` follows TMPDIR, and on a machine where /tmp is
// tmpfs every fsync in here is free - which is most of what this benchmark
// exists to measure. Take it with TMPDIR set to a directory on the disk the
// deployment would use, and record which filesystem that was. That is the
// rule every SQLite figure in this project already carries, and it applies
// here for the same reason.
func BenchmarkShutdownSnapshotAcrossChannels(b *testing.B) {
	payload := []byte(strings.Repeat("x", 128))

	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d-channels", n), func(b *testing.B) {
			chans := make([]*channel.Channel, 0, n)
			for i := range n {
				chans = append(chans, &channel.Channel{
					Name: fmt.Sprintf("c%06d", i), Type: channel.Append, Storage: "local",
				})
			}
			br := benchBroker(b, chans, b.TempDir())
			for _, c := range chans {
				lg := br.logs[c.Name]
				for j := range snapshotBenchRecords {
					if _, err := lg.Append(store.Record{
						MessageID: "m", Topic: c.Name + "/x",
						Payload: payload, Offset: uint64(j),
					}); err != nil {
						b.Fatalf("append: %v", err)
					}
				}
			}

			b.ResetTimer()
			for range b.N {
				if err := br.SaveSnapshots(); err != nil {
					b.Fatalf("save snapshots: %v", err)
				}
			}
		})
	}
}
