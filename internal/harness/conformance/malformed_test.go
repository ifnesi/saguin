package conformance

import (
	"fmt"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// Every row of harness.BadPackets the broker reads, sent to a broker with
// the table's limits. Before a CONNACK: design.md, CONNECT: "Any other
// Protocol Name, a malformed CONNECT [...], or one that breaks the protocol
// closes the connection without a CONNACK (MQTT-3.1.2-1)"; a first packet
// other than CONNECT breaks [MQTT-3.1.0-1]. After it: MQTT 5 4.13.1: "When
// a Server detects a Malformed Packet or Protocol Error, and a Reason Code
// is given in the specification, it MUST close the Network Connection
// [MQTT-4.13.1-1]. [...] it SHOULD send a DISCONNECT packet containing the
// Reason Code", which DISCONNECT, "Reasons Sagüin sends", makes 0x81
// malformed packet, 0x82 protocol error or 0x95 packet too large: "These
// reasons are MQTT 5 only. An MQTT 3.1.1 connection just closes" (3.1.1
// [MQTT-4.8.0-1]). design.md, Flows, Packet buffers: "A packet's size is
// checked before its body is read." Each close is orderly: the broker's end
// of stream comes first (R2).
func TestBadPacketsAreRefused(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxPacketSize: harness.BadMaxPacketSize, MaxConnectSize: harness.BadMaxConnectSize,
		MaxTopicLevels: harness.BadMaxTopicLevels})
	for i, row := range harness.BadPackets {
		if row.Stage == harness.HarnessOnly {
			continue
		}
		t.Run(row.Name, func(t *testing.T) {
			var c *harness.Client
			if row.Stage == harness.First {
				var err error
				if c, err = harness.Dial(b.Addr, row.Version); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() }) // the test is over
			} else {
				c, _ = harness.Connected(t, b.Addr, row.Version, &harness.Connect{ClientID: fmt.Sprintf("bad-%d", i), CleanStart: true})
			}
			// The client keeps its stream open: an end of stream would let
			// the broker drop a CONNECT it should answer, as a lost client.
			harness.SendBytes(t, c, row.Packet)
			if row.Stage == harness.AfterConnack && row.Version == harness.V5 {
				if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != row.Reason() {
					t.Errorf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, row.Reason())
				}
			}
			harness.ExpectClosed(t, c)
		})
	}
}
