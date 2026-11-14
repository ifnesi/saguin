package support

import (
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Late is how long past a deadline the design names the broker may take to
// act on it, or to notice a client's end: a scheduling margin, never part
// of what is checked.
const Late = time.Second

// WillDelay is the Will Properties of an MQTT 5 CONNECT holding one
// property, a Will Delay Interval of seconds (MQTT 5 3.1.3.2.2).
func WillDelay(seconds uint32) harness.Properties {
	return harness.Properties{{ID: harness.WillDelayInterval, Int: seconds}}
}

// Hour is a Will Delay Interval, in seconds, that no test outlives: a Will
// of it goes before its delay or not at all.
const Hour = 3600

// DropSocket closes c's connection with no DISCONNECT: a lost connection
// (design.md, DISCONNECT, A lost connection; MQTT 5 3.1.2.5, "The Client
// closes the Network Connection without first sending a DISCONNECT
// packet").
func DropSocket(t *testing.T, c *harness.Client) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// AwaitWillDue waits until q's database gives client's Will a due time,
// which it returns, failing the test where it has none
// harness.DefaultTimeout after the client's connection ended: storage.md,
// wills: due, "when it is published, once its client is gone; NULL while
// connected".
func AwaitWillDue(t *testing.T, q RowQuerier, client string) time.Time {
	t.Helper()
	var due int64
	given := func() bool {
		w, held := StoredWill(t, q, client)
		due = w.Due.Int64
		return held && w.Due.Valid
	}
	if !testkit.Within(harness.DefaultTimeout, given) {
		t.Fatalf("%v after its connection ended, the database gives the Will of %q no due time; want it timed once its client is gone",
			harness.DefaultTimeout, client)
	}
	return time.UnixMilli(due)
}
