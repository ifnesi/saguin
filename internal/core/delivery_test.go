package core_test

import (
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// The tests of delivery scheduling driven inside: which store requests are
// outstanding when a consumer wakes, and which of a connection's consumers
// the core serves first, are orderings no client can force (design.md,
// Testing: "A test inside the code checks only what neither can see.
// These checks cover [...] orderings no client can force"). They drive the
// core with scripted events, a scripted store and fake times, as
// positions_test.go does.

// design.md, Execution, Resource bounds: a metadata page "is read for a
// channel and serves every consumer of that channel whose scan it covers. A
// consumer it does not cover waits for that page's answer"; Store,
// Requests: "A consumer keeps at most one request of its own, either a read
// or its session's subscription change"; Delivery, Order: "Channels
// supplying one connection are visited round-robin by channel id"; Words:
// Channel, a filter "reaches $broadcast where a matching topic lands in no
// configured channel"; Flows, SUBSCRIBE: an append channel's subscription
// starts with "the floor", a $broadcast one with "the next message", every
// publication that entered before the SUBSCRIBE falling below its
// live-start boundary; "Adding a filter does not move an existing consumer
// position".
//
// A configured append channel a holds a/#. Subscriber T holds b/#, on
// $broadcast; a publication b/1 has T's metadata page on $broadcast read,
// and the store holds that read. Subscriber S, of a durable session, then
// subscribes to #, reaching a and $broadcast. A publication a/1 is
// selected for S, and the store holds its payload read. S subscribes to
// a/# too, a change that waits for that read, and a publication c/1 lands
// on $broadcast, past T's page: the core asks the store for nothing more.
// The store answers S's payload read; the subscription change is stored,
// and in that event S's consumer on $broadcast, which T's page does not
// cover, waits for it while S's consumer on a hands a/1 to its writer. The
// store then answers T's page. The core goes on serving: T receives b/1,
// and S receives a/1 and c/1, each once.
func TestConsumerWaitingForAPageWhileItsConnectionHandsOff(t *testing.T) {
	cfg, err := config.Parse([]byte(baseYAML + "channels:\n  a:\n    type: append\n    filter: \"a/#\"\n"))
	if err != nil {
		t.Fatalf("the test's configuration: %v", err)
	}
	s := newPositionScriptOf(t, cfg, 0)
	publisher := new(connection.Conn)
	s.connect(publisher, &packet.Connect{Version: packet.V5, ClientID: "publisher"})
	tc := new(connection.Conn)
	s.connect(tc, &packet.Connect{Version: packet.V5, ClientID: "t"})
	s.subscribe(tc, 1, "b/#")
	publish := func(topic string) {
		t.Helper()
		s.publish(publisher, &packet.Publish{Topic: []byte(topic), Payload: []byte("x")})
	}

	s.holdRead = func(r store.Read) bool {
		m, ok := r.(*store.ReadRecordMetadata)
		return ok && m.Channel == 1 && !m.Retained
	}
	publish("b/1")
	if len(s.heldReads) != 1 {
		t.Fatalf("after b/1, the store holds %d reads; want T's metadata page", len(s.heldReads))
	}
	s.holdRead = nil
	page := s.heldReads[0]

	sc := new(connection.Conn)
	s.connect(sc, &packet.Connect{Version: packet.V5, ClientID: "s", CleanStart: true, SessionExpiryInterval: 3600})
	s.subscribe(sc, 1, "#")
	s.holdRead = func(r store.Read) bool { return len(core.PayloadsOf(r)) != 0 }
	publish("a/1")
	s.holdRead = nil
	if len(s.heldReads) != 2 {
		t.Fatalf("after a/1, the store holds %d reads; want T's page and S's payload read", len(s.heldReads))
	}
	s.subscribe(sc, 2, "a/#")
	publish("c/1")
	if len(s.pending) != 0 || len(s.heldReads) != 2 {
		t.Fatalf("after c/1, the core asks for %d more requests, the store holding %d reads; want none and the 2 held",
			len(s.pending), len(s.heldReads))
	}
	s.releaseRead(s.heldReads[1])
	s.releaseRead(page)

	received := func(conn *connection.Conn) []string {
		var topics []string
		for _, d := range s.deliveries {
			if d.conn == conn {
				topics = append(topics, d.topic)
			}
		}
		slices.Sort(topics)
		return topics
	}
	if got := received(tc); !slices.Equal(got, []string{"b/1"}) {
		t.Errorf("T received %q; want b/1", got)
	}
	if got := received(sc); !slices.Equal(got, []string{"a/1", "c/1"}) {
		t.Errorf("S received %q; want a/1 and c/1", got)
	}
}
