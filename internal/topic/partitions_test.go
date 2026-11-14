package topic_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/topic"
)

// FuzzPartitions compares Partitions.Add, which reads the saguin-filter
// declarations a client's SUBSCRIBE carries, with design.md, Flows,
// SUBSCRIBE, Partitioning: a declaration is accepted exactly when the
// design's rule accepts it (harness.PartitionDeclaration); "Repeated
// declarations use one count and their indices form a set. Invalid syntax
// or inconsistent counts refuse each affected filter with 0x83". A second
// declaration after an accepted first is accepted exactly when it is valid
// with the same count, and the two accepted leave that count and the set of
// their indices.
func FuzzPartitions(f *testing.F) {
	// Each shape is tried first; after a valid first, the second tries
	// the same count, another index, the same index, another count and an
	// index at the count.
	for _, first := range []string{
		"topic_hash(4,1)", "topic_hash( 4 , 1 )", "topic_hash(\t1\t,\t0\t)", "topic_hash(4294967295,4294967294)",
		"topic_hash(4294967296,0)", "topic_hash(18446744073709551616,0)", "topic_hash(+4,1)", "topic_hash(-4,1)",
		"topic_hash(04,1)", "topic_hash(4,01)", "topic_hash(0,0)", "topic_hash(4,4)", "topic_hash(4,1) ",
		" topic_hash(4,1)", "topic_hash (4,1)", "TOPIC_HASH(4,1)", "topic_hash(4 1)", "topic_hash(4,,1)",
		"topic_hash(4,1", "topic_hash(4,1))", "topic_hash(4\n,1)", "topic_hash(4\u00a0,1)", "topic_hash(\u0664,1)",
		"topic_hash(4,1)(4,1)", "topic_hash(,1)", "topic_hash(4,)", "topic_hash()", "",
	} {
		f.Add(first, "topic_hash(4,3)")
	}
	for _, second := range []string{"topic_hash(4,3)", "topic_hash(4,1)", "topic_hash(5,1)", "topic_hash(4,4)"} {
		f.Add("topic_hash(4,1)", second)
	}
	f.Fuzz(func(t *testing.T, first, second string) {
		var p topic.Partitions
		count, index, valid := harness.PartitionDeclaration(first)
		if got := p.Add(first); got != valid {
			t.Fatalf("Add(%q) = %v, want %v", first, got, valid)
		}
		if !valid {
			return // the filter is refused, whatever follows
		}
		secondCount, secondIndex, secondValid := harness.PartitionDeclaration(second)
		secondValid = secondValid && secondCount == count
		if got := p.Add(second); got != secondValid {
			t.Fatalf("after Add(%q), Add(%q) = %v, want %v", first, second, got, secondValid)
		}
		if !secondValid {
			return // the filter is refused
		}
		indices := map[uint32]struct{}{uint32(index): {}, uint32(secondIndex): {}}
		if uint64(p.Count) != count || !maps.Equal(p.Indices, indices) {
			t.Fatalf("after Add(%q) and Add(%q), count %d, indices %v; want %d, %v",
				first, second, p.Count, slices.Sorted(maps.Keys(p.Indices)), count, slices.Sorted(maps.Keys(indices)))
		}
	})
}
