package support

import (
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// Away sends c's DISCONNECT, Normal disconnection, as disconnected does: a
// durable session then goes Away, keeping what it is owed (design.md,
// DISCONNECT: "The session goes Away until its expiry, keeping its
// in-flight messages").
func Away(t *testing.T, c *harness.Client) {
	t.Helper()
	Disconnected(t, c, &harness.Disconnect{})
}

// Disconnected sends d, c's DISCONNECT, ends its sending as MQTT 5 has the
// sender do after it ("MUST close the Network Connection"
// [MQTT-3.14.4-2]), and waits for the end of its connection. Once the
// client has read the end, the core has handed that connection its final
// entry, so nothing published later is sent on it.
func Disconnected(t *testing.T, c *harness.Client, d *harness.Disconnect) {
	t.Helper()
	disconnect(t, c, d)
	harness.ExpectClosed(t, c)
}

// AwayAllowing is Away for a client that may still be sent deliveries the
// test permits before its connection ends: each PUBLISH that arrives first
// must satisfy allowed, and nothing else may come before the end.
func AwayAllowing(t *testing.T, c *harness.Client, allowed func(*harness.Publish) bool) {
	t.Helper()
	disconnect(t, c, &harness.Disconnect{})
	p, err := c.Recv()
	for err == nil {
		pub, ok := p.(*harness.Publish)
		if !ok || !allowed(pub) {
			break
		}
		p, err = c.Recv()
	}
	if err := c.Closed(p, err); err != nil {
		t.Fatal(err)
	}
}

// disconnect sends d and ends c's sending, as MQTT 5 has the sender do.
func disconnect(t *testing.T, c *harness.Client, d *harness.Disconnect) {
	t.Helper()
	if err := c.Send(d); err != nil {
		t.Fatalf("DISCONNECT 0x%02X: %v", d.Reason, err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatalf("ending the client's sending after its DISCONNECT: %v", err)
	}
}
