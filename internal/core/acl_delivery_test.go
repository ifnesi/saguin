package core_test

import (
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
)

// The guard of the busy path for the acl_file (item 0018e), renamed without
// its leading underscore once the seam of _export_acl_test.go exists. It
// drives the core with scripted events, a scripted store and fake times,
// as positions_test.go does.

// guardACL is the acl_file of TestWholeFilterSkipsTheDeliveryCheck: reader
// may read open/# alone, writer write and read everything.
const guardACL = `roles:
  open:
    - topic: "open/#"
      allow: [read]
  all:
    - topic: "#"
      allow: [write, read]
users:
  reader: [open]
  writer: [all]
`

// design.md, Access and reload: "The core checks it on every delivery
// [...] A subscription whose whole filter is allowed skips the delivery
// check. This is worked out at SUBSCRIBE"; Testing: "The busy path is
// anything run per packet, per message or per connection. It does no
// needless work".
//
// A core holds guardACL. reader's client whole subscribes to open/#, its
// whole filter allowed, and writer publishes open/1: whole receives it.
// whole's compiled rules are then replaced by rules granting nothing, its
// subscription kept as SUBSCRIBE worked it out, and writer publishes
// open/2 and open/3: whole receives them too, so the core checked none of
// the three deliveries against its rules. That a partly allowed
// subscription is checked, a record it may not read skipped, is the
// conformance suite's to show from the wire.
func TestWholeFilterSkipsTheDeliveryCheck(t *testing.T) {
	s := newPositionScriptOf(t, newConfig(t), 0)
	if err := s.core.UseACL([]byte(guardACL)); err != nil {
		t.Fatalf("the test's acl_file: %v", err)
	}
	login := func(conn *connection.Conn, client, user string) {
		t.Helper()
		s.connect(conn, &packet.Connect{Version: packet.V5, ClientID: client, UserNameFlag: true, UserName: user, ReceiveMaximum: 65535})
	}
	writer, whole := new(connection.Conn), new(connection.Conn)
	login(writer, "writer", "writer")
	login(whole, "whole", "reader")
	s.subscribe(whole, 1, "open/#")
	publish := func(topics ...string) {
		t.Helper()
		for _, topic := range topics {
			s.publish(writer, &packet.Publish{Topic: []byte(topic), Payload: []byte("x")})
		}
	}
	received := func() []string {
		var topics []string
		for _, d := range s.deliveries {
			if d.conn == whole {
				topics = append(topics, d.topic)
			}
		}
		return topics
	}
	publish("open/1")
	if got, want := received(), []string{"open/1"}; !slices.Equal(got, want) {
		t.Fatalf("whole, subscribed to open/#, received %q; want %q", got, want)
	}

	s.core.RevokeAccess(whole)
	publish("open/2", "open/3")
	if got, want := received(), []string{"open/1", "open/2", "open/3"}; !slices.Equal(got, want) {
		t.Errorf("whole, its whole filter allowed at SUBSCRIBE, received %q once its rules granted nothing; "+
			"want %q: the core checked a delivery it was to skip the check for", got, want)
	}
}
