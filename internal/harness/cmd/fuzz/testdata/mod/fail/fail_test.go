// Package fail has a target that fails on its seed, then one the runner must
// never reach.
package fail

import "testing"

func FuzzFails(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) { t.Fatal("this target always fails") })
}

func FuzzNeverReached(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {})
}
