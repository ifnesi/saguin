package conformance

import (
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// design.md, Operations: "saguin --config <file> | runs the broker", the
// broker TestMain started. MQTT 5 3.2.2.1.1: with Clean Start 1 "the Server
// MUST set Session Present to 0 in the CONNACK packet in addition to setting
// a 0x00 (Success) Reason Code" [MQTT-3.2.2-2]; 3.12.4: "The Server MUST
// send a PINGRESP packet in response to a PINGREQ packet" [MQTT-3.12.4-1];
// 3.14.4: "On receipt of DISCONNECT, the receiver: SHOULD close the Network
// Connection", which design.md, Flows, Closing a connection makes an
// orderly close ("The writer handles the DISCONNECT and orderly close").
func TestFirstLight(t *testing.T) {
	c, ack := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: "first-light", CleanStart: true, KeepAlive: 60})
	if ack.SessionPresent {
		t.Error("CONNACK with Clean Start 1 has Session Present 1")
	}
	if err := c.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, c)
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
}
