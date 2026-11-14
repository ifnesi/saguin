// Package conformance holds the tests that drive a running broker with the
// harness client, over a real socket, and check what comes back against MQTT
// 3.1.1, MQTT 5 and the design, and the tests that drive the broker's
// process from outside where they need the harness to reach it.
//
// TestMain starts one broker for the package through harness.StartBroker,
// the seam the soak runner shares; a test that only talks to a broker dials
// brokerAddr. A test that stops or restarts its broker starts one of its
// own with launch. A test with a broker of its own whose time goes mostly
// on waiting (a timer, a restart, a graceful stop's write_timeout) calls
// t.Parallel, so the waits overlap; one that loads the broker, or fails
// when a step comes late, stays serial, so other tests never slow it.
// A broker that cannot start fails the package: design.md,
// Testing, "A test tool that finds no broker to drive fails, naming the
// binary; a run that tested nothing never passes" (decided from item 0008a).
package conformance

import (
	"fmt"
	"os"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// brokerAddr is the MQTT address of the broker TestMain started.
var brokerAddr string

func TestMain(m *testing.M) {
	addr, stop, err := harness.StartBroker(support.BrokerBinary, &harness.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "conformance: starting %s: %v\n", support.BrokerBinary, err)
		os.Exit(1)
	}
	brokerAddr = addr
	code := m.Run()
	if err := stop(); err != nil {
		fmt.Fprintf(os.Stderr, "conformance: stopping the broker: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
