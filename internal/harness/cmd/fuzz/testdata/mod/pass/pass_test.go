// Package pass has a fuzz target that never fails.
package pass

import "testing"

func FuzzPasses(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) {})
}
