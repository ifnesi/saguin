package broker_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// blockingSaves holds every Save for one client until released, as a
// session write queued behind other clients' commits waits.
type blockingSaves struct {
	broker.SessionStore
	client  string
	release chan struct{}
}

func (s *blockingSaves) Save(sess store.Session) error {
	if sess.Client == s.client {
		<-s.release
	}
	return s.SessionStore.Save(sess)
}

// **Invariant 18 at the broker: the SUBACK waits for the session write.** A
// sqlite provider collects session writes behind other clients' commits
// (RFC 0004 "Group commit"), and the store answers each only once its
// transaction has committed (TestAWriteReturnsOnlyOnceItsTransactionCommitted
// holds that). This is the other half: a SUBSCRIBE whose write has not
// returned is not answered, and once it returns the SUBACK comes and the
// record holds the filter.
func TestASubackWaitsForTheSessionWrite(t *testing.T) {
	bs := &blockingSaves{client: "held", release: make(chan struct{})}
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		bs.SessionStore = s
		return bs
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))
	raw, ack := connectRawWith(t, h, "held", 10, true, 3600)
	if ack.FixedHeader.Type != packets.Connack || ack.ReasonCode != 0 {
		t.Fatalf("connect: %+v", ack)
	}
	raw.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 1,
		Filters: packets.Subscriptions{{Filter: "held/x", Qos: 1}}})
	_ = raw.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if first, body, err := brokertest.ReadRawPacket(raw.conn); err == nil {
		t.Fatalf("while the session write had not returned, the broker sent type %d % x", first>>4, body)
	}
	close(bs.release)
	if pk := raw.read(); pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) != 1 || pk.ReasonCodes[0] != 1 {
		t.Fatalf("once the write returned, the SUBSCRIBE was answered %+v", pk)
	}
	sess, had, err := bs.SessionStore.Get("held")
	if err != nil || !had || len(sess.Subscriptions) != 1 || sess.Subscriptions[0].Filter != "held/x" {
		t.Fatalf("after the SUBACK the record is %+v (had %v, err %v), want it holding held/x", sess, had, err)
	}
}

// **One client id's writes, each made under its session lock, stay in the
// order they were made, across takeovers and a restart** (RFC 0004 "Group
// commit"). A session taken over twenty times by
// its own client id, each new connection subscribing to one more filter while
// QoS 1 messages arrive for the filters it holds and the connection before it
// is sometimes cut rather than taken over; then the broker restarts from its
// file. The session comes back with every filter it was granted, and every
// message published to it is delivered at least once - none lost to a write
// that landed out of order, a record overwritten by an older one or an
// in-flight table the new session was not given.
func TestOneClientsWritesStayInOrderAcrossTakeoversAndARestart(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q is not a number", v)
		}
		seed = n
	}
	t.Logf("seed %d; replay with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	for trial := range 3 {
		path := filepath.Join(t.TempDir(), "s.db")
		h := brokertest.StartDurableSQLite(t, path)
		pub := brokertest.Connect(t, h, "pub", true, false)
		if _, ok := brokertest.HarnessSessions.(*sqlite.Sessions); !ok {
			t.Fatalf("the harness's sessions are a %T, not a sqlite provider's: nothing here is collected",
				brokertest.HarnessSessions)
		}
		var (
			granted   []string
			published []string
			clients   []*brokertest.Client
			cuts      int
		)
		for round := range 20 {
			c := brokertest.Connect(t, h, "tk", false, false)
			clients = append(clients, c)
			f := fmt.Sprintf("tk/%d/#", round)
			c.Sub(t, f, 1)
			granted = append(granted, f)
			for i := range 5 {
				payload := fmt.Sprintf("%d-%d-%d", trial, round, i)
				pub.Pub(t, fmt.Sprintf("tk/%d/m", rng.Intn(round+1)), payload)
				published = append(published, payload)
			}
			if rng.Intn(3) == 0 {
				_ = c.Conn.Close() // cut rather than taken over
				cuts++
			}
		}
		pub.Close()
		h.Stop()

		h = brokertest.StartDurableSQLite(t, path)
		c := brokertest.Connect(t, h, "tk", false, false)
		clients = append(clients, c)
		if !c.SessionPresent {
			t.Fatalf("trial %d: the session did not come back after the restart", trial)
		}
		sess, had, err := brokertest.HarnessSessions.Get("tk")
		if err != nil || !had {
			t.Fatalf("trial %d: the record after the restart: had %v, err %v", trial, had, err)
		}
		var filters []string
		for _, sub := range sess.Subscriptions {
			filters = append(filters, sub.Filter)
		}
		slices.Sort(filters)
		slices.Sort(granted)
		if !slices.Equal(filters, granted) {
			t.Fatalf("trial %d: the record holds %v after the restart, want every filter granted: %v",
				trial, filters, granted)
		}
		missing := func() []string {
			got := map[string]bool{}
			for _, cl := range clients {
				for _, p := range cl.Payloads() {
					got[p] = true
				}
			}
			var out []string
			for _, p := range published {
				if !got[p] {
					out = append(out, p)
				}
			}
			return out
		}
		for deadline := time.Now().Add(10 * time.Second); len(missing()) > 0 && time.Now().Before(deadline); {
			time.Sleep(20 * time.Millisecond)
		}
		if m := missing(); len(m) > 0 {
			t.Fatalf("trial %d: %d of %d messages published to the session were never delivered: %v",
				trial, len(m), len(published), m)
		}
		delivered := 0
		for _, cl := range clients {
			delivered += cl.Count()
		}
		t.Logf("trial %d: %d connections, %d cut, %d filters, %d published, %d deliveries", trial,
			len(clients), cuts, len(granted), len(published), delivered)
		h.Stop()
	}
}
