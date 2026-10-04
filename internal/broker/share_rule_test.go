package broker

import (
	"io"
	"log/slog"
	"testing"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// **A member being hung up is chosen for nothing** while it closes
// (Broker.hangUp): until then it is still connected, and a delivery handed to
// it goes with the connection rather than to a member that would have read
// it.
func TestAMemberBeingHungUpOnCannotTakeADelivery(t *testing.T) {
	b := testBroker(t)
	srv := mqtt.New(&mqtt.Options{InlineClient: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)
	member := srv.NewClient(nil, "test", "member", false)
	member.Properties.ProtocolVersion = 5
	srv.Clients.Add(member)
	sub := packets.Subscription{Filter: "$share/g/alerts/#", Qos: 1}
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName: "alerts/door", Payload: []byte("open")}
	if !b.canTakeShared("member", sub, pk) {
		t.Fatal("a connected member with room could not take a delivery, so the case is not " +
			"the one named")
	}
	if !member.BeginHangUp() {
		t.Fatal("the hang-up was not begun")
	}
	if b.canTakeShared("member", sub, pk) {
		t.Error("a member being hung up on was offered a delivery")
	}
}
