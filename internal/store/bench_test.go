package store_test

import (
	"strconv"
	"testing"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/store"
)

// BenchmarkWrite measures records committed per second through the store's
// own write path, as the core shapes it: one write group outstanding at a
// time (design.md, Store, Writes: "The core keeps at most one write
// transaction outstanding at the store"), each a core.Group of members
// single-record writes, each through the store's prepared AddRecord, on disk
// with synchronous=FULL. A group of one is a lone publisher's; a larger one,
// what waited while the group before it was committed. Each iteration is one
// record. It runs only with -bench, never in make check.
func BenchmarkWrite(b *testing.B) {
	for _, payload := range []int{100, 1 << 10, 64 << 10} {
		for _, members := range []int{1, 256} {
			b.Run("payload="+strconv.Itoa(payload)+"/members="+strconv.Itoa(members), func(b *testing.B) {
				benchmarkWrite(b, payload, members)
			})
		}
	}
}

func benchmarkWrite(b *testing.B, payload, members int) {
	s, _, err := lockAndStart(b, newConfig(b, 2), metrics.New())
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := s.Stop(); err != nil {
			b.Error(err)
		}
	}()
	body := make([]byte, payload)
	b.SetBytes(int64(payload))
	// commit sends g, a group never sent before, and waits for its answer.
	commit := func(g *core.Group) {
		s.Writes() <- g
		if a := <-s.Answers(); a.Err != nil {
			b.Fatal(a.Err)
		}
	}
	g, next := &core.Group{Writes: make([]store.Write, 0, members)}, 1
	for b.Loop() {
		g.Writes = append(g.Writes, &putRecords{first: next, n: 1, payload: body})
		next++
		if len(g.Writes) == members {
			commit(g)
			g = &core.Group{Writes: make([]store.Write, 0, members)}
		}
	}
	if len(g.Writes) != 0 {
		commit(g)
	}
	b.ReportMetric(float64(next-1)/b.Elapsed().Seconds(), "records/s")
}
