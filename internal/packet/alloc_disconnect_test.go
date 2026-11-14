package packet_test

import (
	"fmt"
	"testing"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Allocations of the DISCONNECT codec, by alloc_test.go's
// standard: a decoder allocates only what the packet keeps beyond the read
// buffer, and an encoder writing into a buffer with room allocates nothing.
// Every connection ends with a DISCONNECT or a close, and the broker sends
// one to every client at a graceful stop (design.md, Restart: "every client
// gets 0x8B").

// TestDecodeDisconnectAllocations: a DISCONNECT without properties, in
// either version and any of its short forms, keeps nothing and allocates
// nothing. One with properties keeps one list: the example of Figure 3-24,
// whose one property is an integer, allocates at most once.
func TestDecodeDisconnectAllocations(t *testing.T) {
	bare := []struct {
		version packet.Version
		body    []byte
	}{
		{packet.V311, nil},
		{packet.V5, nil},
		{packet.V5, []byte{0x8E}},
		{packet.V5, []byte{0x8E, 0x00}},
	}
	for _, c := range bare {
		testkit.NoAllocs(t, fmt.Sprintf("DecodeDisconnect(% X), MQTT %v", c.body, c.version), func() {
			if _, err := packet.DecodeDisconnect(c.body, c.version); err != nil {
				t.Fatal(err)
			}
		})
	}
	atMost := func(what string, limit float64, f func() error) {
		t.Helper()
		n := testing.AllocsPerRun(100, func() {
			if err := f(); err != nil {
				t.Fatal(err)
			}
		})
		if n > limit {
			t.Errorf("%s allocates %v times a call; want at most %v", what, n, limit)
		}
	}
	example := []byte{0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00}
	atMost("DecodeDisconnect, the example of Figure 3-24", 1, func() error {
		_, err := packet.DecodeDisconnect(example, packet.V5)
		return err
	})
}

// TestAppendDisconnectDoesNotAllocate writes every valid DISCONNECT into a
// buffer with room.
func TestAppendDisconnectDoesNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 256)
	for _, c := range validDisconnects {
		testkit.NoAllocs(t, "AppendDisconnect into a buffer with room, "+c.name, func() {
			if _, err := packet.AppendDisconnect(buf[:0], c.version, &c.want); err != nil {
				t.Fatal(err)
			}
		})
	}
}
