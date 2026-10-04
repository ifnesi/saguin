package mqtt

import (
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// Invariant 13: limits.session_queue_bytes holds a session's in-flight table
// to what it says only if what an entry is charged (inflightSize) is at
// least the heap it takes: the entry, its packet, its map slot and its
// place in the order, and the bytes it carries. Measured as the original
// figure was, at 8,192 entries of a publish copied as a delivery is, with a
// small, a middling and a large payload, the least of three tries each,
// since a collection cannot be made to count only this.
func TestWhatAnInflightEntryCostsIsWhatItIsCharged(t *testing.T) {
	const n = 8192
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	for _, size := range []int{20, 256, 4096} {
		src := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			TopicName: "sensors/device-0001/temperature", Payload: []byte(strings.Repeat("x", size)),
			Properties: packets.Properties{User: []packets.UserProperty{{Key: "k", Val: "v"}}}}
		charged := inflightSize(src)
		took := math.Inf(1)
		for range 3 {
			before := heap()
			i := NewInflights()
			for id := range uint16(n) {
				pk := src.Copy(false)
				pk.PacketID = id + 1
				i.Set(pk)
			}
			after := heap()
			if got := i.Len(); got != n {
				t.Fatalf("the table holds %d entries of %d set", got, n)
			}
			took = min(took, (float64(after)-float64(before))/n)
			runtime.KeepAlive(i)
		}
		t.Logf("a %d-byte payload: %.0f bytes an entry, charged %d", size, took, charged)
		if took <= float64(size) {
			t.Fatalf("an entry of a %d-byte payload took %.0f bytes, so this measured nothing", size, took)
		}
		if took > float64(charged) {
			t.Errorf("an in-flight entry of a %d-byte payload took %.0f bytes and is charged %d: "+
				"a session's bound holds more memory than it says", size, took, charged)
		}
	}
}

// A table's first entries carry what the table itself takes once it holds
// anything - its map and its order - and a session with one delivery in
// flight is the commonest table there is. So a thousand tables of one, two
// and four entries of a 20-byte payload each, against what the table is
// charged in all (Bytes), the least of three tries.
func TestWhatASmallInflightTableCostsIsWhatItIsCharged(t *testing.T) {
	const tables = 2000
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	src := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName: "sensors/device-0001/temperature", Payload: []byte(strings.Repeat("x", 20)),
		Properties: packets.Properties{User: []packets.UserProperty{{Key: "k", Val: "v"}}}}
	for _, n := range []int{1, 2, 4, 8} {
		took, charged := math.Inf(1), int64(0)
		for range 3 {
			ts := make([]*Inflight, tables)
			for k := range ts {
				ts[k] = NewInflights()
			}
			before := heap()
			for _, i := range ts {
				for id := range uint16(n) {
					pk := src.Copy(false)
					pk.PacketID = id + 1
					i.Set(pk)
				}
			}
			after := heap()
			took = min(took, (float64(after)-float64(before))/tables)
			charged = ts[0].Bytes()
			runtime.KeepAlive(ts)
		}
		t.Logf("%d entries: %.0f bytes a table, charged %d", n, took, charged)
		if took <= float64(n*20) {
			t.Fatalf("a table of %d took %.0f bytes, so this measured nothing", n, took)
		}
		if took > float64(charged) {
			t.Errorf("an in-flight table of %d entries took %.0f bytes and is charged %d: "+
				"a session's bound holds more memory than it says", n, took, charged)
		}
	}
}
