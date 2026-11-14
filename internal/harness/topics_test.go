package harness

import "testing"

// TestTopicHash: design.md, Flows, SUBSCRIBE, Partitioning: "The topic a
// gives 198367012849983736".
func TestTopicHash(t *testing.T) {
	if got := TopicHash("a"); got != 198367012849983736 {
		t.Errorf("TopicHash(a) = %d, want 198367012849983736", got)
	}
}

// TestMoreExact: design.md, Words: Channel, "Filters are compared level by
// level. The first level where they differ decides. A written level beats +
// and #, and + beats #. A filter ending there beats one going on with #".
// Filters the rule does not compare are unordered.
func TestMoreExact(t *testing.T) {
	for _, tc := range []struct {
		a, b          string
		more, ordered bool
	}{
		{"a/b", "a/#", true, true}, // ending there beats going on with #
		{"a/+", "a/#", true, true}, // + beats #
		{"a/b", "a/+", true, true}, // a written level beats +
		{"a/b/#", "a/+/#", true, true},
		{"a/#", "a/b/#", false, true},
		{"a/#", "a/b", false, true},
		{"a/b", "a/c", false, false}, // two written levels that differ
		{"a", "a/b", false, false},   // going on with a level that is not #
		{"a/b", "a/b", false, false},
	} {
		if more, ordered := MoreExact(tc.a, tc.b); more != tc.more || ordered != tc.ordered {
			t.Errorf("MoreExact(%q, %q) = %v, %v; want %v, %v", tc.a, tc.b, more, ordered, tc.more, tc.ordered)
		}
	}
}

// TestPartitionDeclaration: design.md, Flows, SUBSCRIBE, Partitioning: "A
// declaration is topic_hash(count,index), with ASCII spaces or tabs allowed
// around the integers and comma. Integers are unsigned decimal without signs
// or leading zeroes, except zero itself. Count is 1 through 4,294,967,295.
// Index is 0 through count minus one."
func TestPartitionDeclaration(t *testing.T) {
	for _, tc := range []struct {
		declaration  string
		count, index uint64
		ok           bool
	}{
		{"topic_hash(4,1)", 4, 1, true},
		{"topic_hash( 4\t,\t0 )", 4, 0, true},
		{"topic_hash(4294967295,4294967294)", 4294967295, 4294967294, true},
		{"topic_hash(4294967296,0)", 0, 0, false},
		{"topic_hash(0,0)", 0, 0, false},
		{"topic_hash(4,4)", 0, 0, false},
		{"topic_hash(+4,1)", 0, 0, false},
		{"topic_hash(04,1)", 0, 0, false},
		{"topic_hash(4,1) ", 0, 0, false},
		{"topic_hash(4\n,1)", 0, 0, false},
	} {
		count, index, ok := PartitionDeclaration(tc.declaration)
		if count != tc.count || index != tc.index || ok != tc.ok {
			t.Errorf("PartitionDeclaration(%q) = %d, %d, %v; want %d, %d, %v", tc.declaration, count, index, ok, tc.count, tc.index, tc.ok)
		}
	}
}
