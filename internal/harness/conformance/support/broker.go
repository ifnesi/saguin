package support

import (
	"cmp"
	"os"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// SessionExpiry is the property section of an MQTT 5 CONNECT or
// DISCONNECT holding one property, a Session Expiry Interval of seconds.
func SessionExpiry(seconds uint32) harness.Properties {
	return harness.Properties{{ID: harness.SessionExpiryInterval, Int: seconds}}
}

// Launch starts a broker of the test's own (started) and waits until it
// listens.
func Launch(t *testing.T, o *harness.Options) *harness.Broker {
	t.Helper()
	b := Started(t, o)
	if err := b.Listening(); err != nil {
		t.Fatalf("starting %s: %v", BrokerBinary, err)
	}
	return b
}

// Durable is the property section of an MQTT 5 CONNECT asking for a
// session that outlives its connection by an hour (design.md, Words:
// "Durable when its expiry is above zero").
var Durable = SessionExpiry(3600)

// Started starts a broker of the test's own, from brokerBinary, with o
// (its folder a new one of the test's where o names none, and its write
// timeout writeTimeout where o sets none), and returns at once, before it
// has read its configuration. It is killed when the test ends if it still
// runs.
func Started(t *testing.T, o *harness.Options) *harness.Broker {
	t.Helper()
	return StartedFrom(t, BrokerBinary, o)
}

// Exited waits for b's exit, failing the test where it does not come within
// StopLimit.
func Exited(t *testing.T, b *harness.Broker) *os.ProcessState {
	t.Helper()
	state, err := b.Wait(StopLimit)
	if err != nil {
		t.Fatalf("the broker did not exit within %v: %v", StopLimit, err)
	}
	return state
}

// StopLimit bounds a graceful stop. design.md, Restart, Graceful stop:
// "after limits.write_timeout from when the core accepts the stop, the
// remaining sockets are cut", and then "the
// logger writes the lines queued until limits.write_timeout": two write
// timeouts and the margin.
const StopLimit = 2*WriteTimeout + Margin

// StartedFrom is started, from binary.
func StartedFrom(t *testing.T, binary string, o *harness.Options) *harness.Broker {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Dir = cmp.Or(own.Dir, t.TempDir())
	own.WriteTimeout = cmp.Or(own.WriteTimeout, WriteTimeout)
	b, err := harness.Start(binary, &own)
	if err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	t.Cleanup(func() {
		// A broker the test left running is killed; how it ends no longer
		// matters.
		_ = b.Signal(os.Kill)
		_, _ = b.Wait(StopLimit)
		b.Release() // the test is over: no dial of it reaches another broker's
	})
	return b
}

// WriteTimeout is broker.limits.write_timeout for every broker a test
// launches: short, so a stop that waits it out costs little.
const WriteTimeout = time.Second

// Margin is what a broker's exit may take beyond the waits the design
// names: the store closing and the process ending.
const Margin = 5 * time.Second
