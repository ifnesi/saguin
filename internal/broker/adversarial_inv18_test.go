package broker_test

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// An UNSUBSCRIBE whose packet identifier is still in use must not be applied
// to the durable record before the engine refuses it with 0x91.
func TestAdversarialUnsubscribePacketIdentifierInUseDoesNotChangeStoredSession(t *testing.T) {
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner = s
		return s
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })

	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	w, _ := qos2Dial(t, h.Addr, "dev", true, 3600)
	w.Subscribe("news/#", 1)

	const id = 7
	w.Publish(id, "events/x", "held", false)
	if rc := w.Pubrec(id); rc != 0 {
		t.Fatalf("PUBREC 0x%02x", rc)
	}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Unsubscribe, Qos: 1},
		PacketID:        id,
		Filters:         packets.Subscriptions{{Filter: "news/#"}},
		ProtocolVersion: 5,
	}
	var out bytes.Buffer
	if err := pk.UnsubscribeEncode(&out); err != nil {
		t.Fatal(err)
	}
	if _, err := w.C.Write(out.Bytes()); err != nil {
		t.Fatal(err)
	}
	first, body, err := brokertest.ReadRawPacket(w.C)
	if err != nil {
		t.Fatal(err)
	}
	if first>>4 != packets.Unsuback || len(body) < 4 || body[len(body)-1] != packets.ErrPacketIdentifierInUse.Code {
		t.Fatalf("UNSUBACK first=%#x body=%x, want reason 0x91", first, body)
	}

	if got := storedFilters(t, inner, "dev"); len(got) != 1 || got[0] != "news/#" {
		t.Fatalf("after UNSUBACK 0x91 stored filters are %v; the refused operation changed durable state", got)
	}

	// It also remains live: the engine did not apply the operation.
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	pub.Pub(t, "news/x", "still-live")
	if _, err := w.Read(2 * time.Second); err != nil {
		t.Fatalf("the live subscription was also removed: %v", err)
	}
}

type holdsSessionSave struct {
	broker.SessionStore
	client  string
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (s *holdsSessionSave) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *holdsSessionSave) Save(sess store.Session) error {
	if sess.Client == s.client && s.armed.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	return s.SessionStore.Save(sess)
}

// The ACL is deliberately reloaded while OnSubscribe is storing the session,
// after the read rule was asked for this SUBSCRIBE and before its SUBACK. The
// durable record must agree with the SUBACK even across this supported live
// reload, and the reload must still govern what is delivered.
func TestAdversarialACLReloadDuringSubscribeDoesNotStoreARefusedFilter(t *testing.T) {
	gate := &holdsSessionSave{client: "dev", reached: make(chan struct{}), release: make(chan struct{})}
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner, gate.SessionStore = s, s
		return gate
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })

	h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	dev := brokertest.Dial(t, h, "dev", true, false, 10, 3600, 0)
	gate.armed.Store(true)
	type answer struct {
		ack *paho.Suback
		err error
	}
	done := make(chan answer, 1)
	go func() {
		ack, err := dev.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
			{Topic: "news/#", QoS: 1},
		}})
		done <- answer{ack: ack, err: err}
	}()
	select {
	case <-gate.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the pre-SUBACK session Save was never reached")
	}
	brokertest.WriteACL(t, h, `
roles:
  other:
    - topic: "other/#"
      allow: [read]
    - topic: "news/#"
      allow: [write]
users:
  "": [other]
`)
	close(gate.release)
	a := <-done
	// The read rule was asked once, before the hook and before the reload:
	// the SUBACK grants, and the record holds exactly what it granted.
	if a.ack == nil || len(a.ack.Reasons) != 1 || a.ack.Reasons[0] != 0x01 {
		t.Fatalf("SUBACK=%+v err=%v, want [0x01]: the grant decided before the reload", a.ack, a.err)
	}
	if got := storedFilters(t, inner, "dev"); !slices.Equal(got, []string{"news/#"}) {
		t.Fatalf("SUBACK 0x01 but the store holds %v, want [news/#]", got)
	}
	// And a grant made before the reload serves nothing the reloaded file
	// denies: the read rule is asked again for every delivery (invariant 16).
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	pub.Pub(t, "news/x", "after-the-reload")
	if r, ok := dev.Await(t, 1500*time.Millisecond); ok {
		t.Fatalf("after the reload denied news/# the subscriber was sent %q", r.Payload)
	}
}

// Reloaded before the SUBSCRIBE, the read rule denies it: the SUBACK refuses
// 0x87 and the store holds nothing.
func TestAdversarialACLReloadBeforeSubscribeRefusesAndStoresNothing(t *testing.T) {
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner = s
		return s
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })

	h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	dev := brokertest.Dial(t, h, "dev", true, false, 10, 3600, 0)
	brokertest.WriteACL(t, h, `
roles:
  other:
    - topic: "other/#"
      allow: [read]
    - topic: "news/#"
      allow: [write]
users:
  "": [other]
`)
	ack, err := dev.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "news/#", QoS: 1},
	}})
	if ack == nil || len(ack.Reasons) != 1 || ack.Reasons[0] != 0x87 {
		t.Fatalf("SUBACK=%+v err=%v, want [0x87] after the reload", ack, err)
	}
	if got := storedFilters(t, inner, "dev"); len(got) != 0 {
		t.Fatalf("SUBACK 0x87 but the store holds %v", got)
	}
}
