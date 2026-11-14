package support

import (
	"math/rand/v2"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/klauspost/compress/s2"
)

// Noisy is a QoS 1 publication on topic whose record is n bytes, as
// sized's is, its payload noise: stored as received, n bytes in the
// database too.
func Noisy(t *testing.T, topic string, n int) *harness.Publish {
	t.Helper()
	p := &harness.Publish{QoS: 1, Topic: topic, Payload: Noise(n - len(topic))}
	Unshrunk(t, p.Payload)
	return p
}

// Noise is n random bytes from a fixed seed: a payload no compression makes
// shorter.
func Noise(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{'s', 'a', 'g', 'u', 'i', 'n'}).Read(b) // ChaCha8's Read never fails
	return b
}

// Unshrunk fails the test if S2's block encoding makes p shorter: a payload
// stored as received however large (design.md, Store, Compression: "A payload
// over 4 KiB, SQLite's page size, is compressed with S2 and stored compressed
// only where that makes it shorter"), so a test sizing the database by it
// holds.
func Unshrunk(t *testing.T, p []byte) {
	t.Helper()
	if n := len(s2.Encode(nil, p)); n < len(p) {
		t.Fatalf("S2 encodes the %d-byte payload in %d; the test needs a payload it cannot make shorter", len(p), n)
	}
}
