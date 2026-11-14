// Package support holds the helpers of Sagüin's conformance tests that two
// test packages share: conformance, and conformance/intervals, whose tests
// wait the design's one-minute intervals in a run of their own beside it.
// Each drives a broker from outside, through the harness, as the
// conformance package's own helpers did; none is a test.
package support

import (
	"cmp"
	"os"
	"path/filepath"
)

// BrokerBinary is the binary every broker of a test package is started from.
var BrokerBinary = cmp.Or(os.Getenv(BinaryEnv), Binary)

// Binary is the broker `make build` writes: bin/saguin at the root of the
// module, found up from the directory a test package runs in, its own.
var Binary = moduleFile("bin", "saguin")

// BinaryEnv names the environment variable that, set, names the broker
// binary in Binary's place: TestNoBrokerFails points it at a file that does
// not exist.
const BinaryEnv = "SAGUIN_CONFORMANCE_BINARY"

// moduleFile is the path of elem under the module's root, the nearest
// directory up from the working directory that holds go.mod; elem alone
// where none does.
func moduleFile(elem ...string) string {
	dir, err := os.Getwd()
	for err == nil {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(append([]string{dir}, elem...)...)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return filepath.Join(elem...)
}
