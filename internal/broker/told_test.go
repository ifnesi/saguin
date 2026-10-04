package broker_test

// Invariant 18: nothing is told to a client before the state it describes is
// stored. Each test here makes the store refuse the write a packet (or a
// close) describes, then crashes the broker and reads what the next start
// serves: what the client was told has to still be true.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// refusesSaves refuses one client's Session Saves while armed, after letting
// `allow` of them through, and counts what it refused and what it let pass.
type refusesSaves struct {
	broker.SessionStore
	client  string
	armed   atomic.Bool
	allow   atomic.Int32
	refused atomic.Int32
	saved   atomic.Int32
}

func (s *refusesSaves) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *refusesSaves) Save(sess store.Session) error {
	if sess.Client == s.client && s.armed.Load() {
		if s.allow.Add(-1) < 0 {
			s.refused.Add(1)
			return errors.New("disk I/O error (refused for the test)")
		}
	}
	if sess.Client == s.client {
		s.saved.Add(1)
	}
	return s.SessionStore.Save(sess)
}

// refuseSavesOf wraps the next harness's session store in a refusesSaves for
// client, and answers it and the store beneath.
func refuseSavesOf(t *testing.T, client string) (*refusesSaves, func() broker.SessionStore) {
	t.Helper()
	rs := &refusesSaves{client: client}
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner, rs.SessionStore = s, s
		return rs
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	return rs, func() broker.SessionStore { return inner }
}

func storedFilters(t *testing.T, s broker.SessionStore, client string) []string {
	t.Helper()
	rec, ok, err := s.Get(client)
	if err != nil {
		t.Fatalf("read %s's record: %v", client, err)
	}
	if !ok {
		return nil
	}
	var out []string
	for _, f := range rec.Subscriptions {
		out = append(out, f.Filter)
	}
	slices.Sort(out)
	return out
}

// An UNSUBACK says the subscription is gone. Where the store refuses to
// forget it, the client is answered 0x83 for that filter and stays
// subscribed - live, and after a crash - rather than told it is gone and
// served it again after the restart.
func TestAnUnsubscribeTheStoreRefusedIsAnswered0x83(t *testing.T) {
	for _, refuse := range []bool{true, false} {
		name := "stored"
		if refuse {
			name = "refused"
		}
		t.Run(name, func(t *testing.T) {
			rs, inner := refuseSavesOf(t, "dev")
			path := filepath.Join(t.TempDir(), "saguin.db")
			h := startDurableSQLite(t, path)
			dev := brokertest.Dial(t, h, "dev", true, false, 10, 3600, 0)
			dev.Sub(t, "news/#", 1)
			rs.armed.Store(refuse)
			ua, err := dev.C.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"news/#"}})
			if ua == nil {
				t.Fatalf("unsubscribe: %v", err)
			}
			rs.armed.Store(false)
			if refuse && rs.refused.Load() == 0 {
				t.Fatal("the store was never asked to forget the subscription, so this proves nothing")
			}
			stored := storedFilters(t, inner(), "dev")
			t.Logf("UNSUBACK %v; saves refused %d; stored after it %v", ua.Reasons, rs.refused.Load(), stored)
			want := byte(0x00)
			if refuse {
				want = 0x83
			}
			if len(ua.Reasons) != 1 || ua.Reasons[0] != want {
				t.Fatalf("UNSUBACK reasons %v, want [0x%02x]", ua.Reasons, want)
			}
			if subscribed := len(stored) == 1; subscribed != refuse {
				t.Fatalf("stored %v after an UNSUBACK of 0x%02x", stored, ua.Reasons[0])
			}
			// Live: a refused unsubscribe leaves the subscription working.
			pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
			pub.Pub(t, "news/live", "live")
			_, got := dev.Await(t, 1500*time.Millisecond)
			if got != refuse {
				t.Fatalf("dev received a publish after the UNSUBACK: %v, want %v", got, refuse)
			}
			dev.Close()
			h.Crash()
			h = startDurableSQLite(t, path)
			dev2 := brokertest.Dial(t, h, "dev", false, false, 10, 3600, 0)
			pub = brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
			pub.Pub(t, "news/x", "after the crash")
			_, got = dev2.Await(t, 1500*time.Millisecond)
			if got != refuse {
				t.Fatalf("after the crash dev received a publish: %v; told 0x%02x", got, want)
			}
		})
	}
}

// A 3.1.1 UNSUBACK has no reason code to refuse with, so an unsubscribe the
// store refused is answered by closing the connection, never by an UNSUBACK.
func TestA311UnsubscribeTheStoreRefusedClosesTheConnection(t *testing.T) {
	rs, inner := refuseSavesOf(t, "old")
	h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	send := func(pk packets.Packet, encode func(*packets.Packet, *bytes.Buffer) error) {
		t.Helper()
		var buf bytes.Buffer
		pk.ProtocolVersion = 4
		if err := encode(&pk, &buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if _, err := conn.Write(buf.Bytes()); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	read := func() (byte, error) {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		first, _, err := brokertest.ReadRawPacket(conn)
		return first >> 4, err
	}
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "old", Keepalive: 60}},
		(*packets.Packet).ConnectEncode)
	if typ, err := read(); err != nil || typ != packets.Connack {
		t.Fatalf("CONNACK: type %d, %v", typ, err)
	}
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 1,
		Filters: packets.Subscriptions{{Filter: "news/#", Qos: 1}}}, (*packets.Packet).SubscribeEncode)
	if typ, err := read(); err != nil || typ != packets.Suback {
		t.Fatalf("SUBACK: type %d, %v", typ, err)
	}
	rs.armed.Store(true)
	send(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe, Qos: 1}, PacketID: 2,
		Filters: packets.Subscriptions{{Filter: "news/#"}}}, (*packets.Packet).UnsubscribeEncode)
	typ, err := read()
	if rs.refused.Load() == 0 {
		t.Fatal("the store was never asked to forget the subscription, so this proves nothing")
	}
	if err == nil {
		t.Fatalf("the broker answered a refused unsubscribe with a packet of type %d; stored %v",
			typ, storedFilters(t, inner(), "old"))
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the broker neither answered nor closed the connection")
	}
	if got := storedFilters(t, inner(), "old"); len(got) != 1 {
		t.Fatalf("stored %v, want the subscription kept", got)
	}
}

// A SUBACK refusing a filter says the session does not hold it, so the store
// never holds it either: not for a moment, and so not after a crash that
// comes while a later write is refused. `$SYS/#` is refused by the broker's
// read rule, which the substrate asks after the session store has been.
func TestASubscriptionTheBrokerRefusedIsNeverStored(t *testing.T) {
	rs, inner := refuseSavesOf(t, "dev")
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	dev := brokertest.Dial(t, h, "dev", true, false, 10, 3600, 0)
	// Any save after the first refused: the one that keeps what is granted
	// passes, and any write meant to take a refused filter back out fails.
	rs.allow.Store(1)
	rs.armed.Store(true)
	sa, err := dev.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "news/#", QoS: 1}, {Topic: "$SYS/#", QoS: 1}}})
	if sa == nil {
		t.Fatalf("subscribe: %v", err)
	}
	rs.armed.Store(false)
	if len(sa.Reasons) != 2 || sa.Reasons[0] != 1 || sa.Reasons[1] != 0x87 {
		t.Fatalf("SUBACK %v, want [1 0x87]", sa.Reasons)
	}
	stored := storedFilters(t, inner(), "dev")
	t.Logf("SUBACK %v; saves %d, refused %d; stored %v", sa.Reasons, rs.saved.Load(), rs.refused.Load(), stored)
	if !slices.Equal(stored, []string{"news/#"}) {
		t.Fatalf("the store holds %v after a SUBACK granting only news/#", stored)
	}
	dev.Close()
	h.Crash()
	h = startDurableSQLite(t, path)
	dev2 := brokertest.Dial(t, h, "dev", false, false, 10, 3600, 0)
	if !dev2.SessionPresent {
		t.Fatal("the session did not come back after the crash")
	}
	if got := storedFilters(t, inner(), "dev"); !slices.Equal(got, []string{"news/#"}) {
		t.Fatalf("after the crash the store holds %v", got)
	}
}

// holdsDisconnected holds one client's Sessions.Disconnected, once armed,
// until released, and says when it was reached.
type holdsDisconnected struct {
	broker.SessionStore
	client  string
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	// dead makes every write of the client's disconnect from now on fail
	// without reaching the store, as a process that has died writes nothing.
	dead atomic.Bool
}

func (s *holdsDisconnected) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *holdsDisconnected) Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error {
	if client == s.client && s.armed.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	if client == s.client && s.dead.Load() {
		return errors.New("the process is gone (for the test)")
	}
	return s.SessionStore.Disconnected(client, at, dropWill, expiry)
}

// Drop is held and killed the same way: a session ending with its connection
// is written by its drop, where one that outlives it is by Disconnected.
func (s *holdsDisconnected) Drop(client string, held []string) (store.Dropped, error) {
	if client == s.client && s.armed.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	if client == s.client && s.dead.Load() {
		return store.Dropped{}, errors.New("the process is gone (for the test)")
	}
	return s.SessionStore.Drop(client, held)
}

// closedWithin reports whether the broker closed conn within d.
func closedWithin(conn net.Conn, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 64)
	for {
		if _, err := conn.Read(buf); err != nil {
			var ne net.Error
			return !(errors.As(err, &ne) && ne.Timeout())
		}
	}
}

// The close that answers a clean DISCONNECT says the Will is withdrawn and
// the expiry it carried is in force, so both are stored before it. The
// store's write is held; if the connection closes while it is held, the
// broker is crashed in that window, and the next start shows what a crash
// there costs: a Will published after a clean DISCONNECT, or a session kept
// past the expiry its DISCONNECT set.
func TestACleanDisconnectIsStoredBeforeItsClose(t *testing.T) {
	for _, tc := range []struct {
		name          string
		connectExpiry uint32
		disconnect    []byte
	}{
		// Will withdrawn by a clean DISCONNECT; the session ends a second
		// after it, which is when a Will left on its record would go out.
		{"will withdrawn", 1, []byte{0xE0, 0x00}},
		// CONNECT 60s, DISCONNECT lowers it to 1s.
		{"expiry lowered", 60, []byte{0xE0, 0x07, 0x00, 0x05, 0x11, 0, 0, 0, 1}},
		// CONNECT 60s, DISCONNECT ends the session with the connection.
		{"expiry zero", 60, []byte{0xE0, 0x07, 0x00, 0x05, 0x11, 0, 0, 0, 0}},
		// A session that ends with its connection from its CONNECT: the
		// start ends such a session and publishes no Will a clean
		// DISCONNECT withdrew, so a crash costs nothing and nothing need be
		// stored before the close.
		{"ends with its connection", 0, []byte{0xE0, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hd := &holdsDisconnected{client: "dev", reached: make(chan struct{}), release: make(chan struct{})}
			var inner broker.SessionStore
			// The first broker's store is held and killed; the one the
			// next start opens is the store as it is.
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				inner = s
				if hd.SessionStore == nil {
					hd.SessionStore = s
					return hd
				}
				return s
			}
			t.Cleanup(func() { brokertest.WrapSessions = nil })
			path := filepath.Join(t.TempDir(), "saguin.db")
			h := startDurableSQLite(t, path)
			conn, code := connectWithWillExpiring(t, h, "dev", "wills/dev", false, 0, 1, tc.connectExpiry)
			if code != 0 {
				t.Fatalf("connect: %#x", code)
			}
			hd.armed.Store(true)
			if _, err := conn.Write(tc.disconnect); err != nil {
				t.Fatal(err)
			}
			select {
			case <-hd.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("the disconnect was never written, so this proves nothing")
			}
			early := closedWithin(conn, 1500*time.Millisecond)
			if early {
				// The window: told, and nothing stored. The held write dies
				// with the process, and so does every one after it.
				hd.dead.Store(true)
				close(hd.release)
				h.Crash()
			} else {
				close(hd.release)
				if !closedWithin(conn, 5*time.Second) {
					t.Fatal("the connection was not closed once the disconnect was stored")
				}
				h.Crash()
			}
			t.Logf("closed while the write was held: %v", early)
			h = startDurableSQLite(t, path)
			w := connect(t, h, "watcher", true, false)
			w.Sub(t, "wills/#", 1)
			time.Sleep(3 * time.Second)
			_, still, _ := inner.Get("dev")
			if w.Count() != 0 {
				t.Errorf("a Will was published after a clean DISCONNECT: %v", w.Payloads())
			}
			if still {
				t.Errorf("the session outlived the expiry its connection left with")
			}
		})
	}
}

// A DISCONNECT whose write the store refused is written again once the store
// takes writes, while the broker runs - not only as it stops - so a crash
// after the store has recovered keeps what the client was told: its Will
// withdrawn, its expiry the one it left with.
func TestARefusedDisconnectIsWrittenOnceTheStoreRecovers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		connectExpiry uint32
		disconnect    []byte
	}{
		{"will withdrawn", 4, []byte{0xE0, 0x00}},
		{"expiry lowered", 60, []byte{0xE0, 0x07, 0x00, 0x05, 0x11, 0, 0, 0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rd, inner := refuseDisconnectOf(t, "dev")
			path := filepath.Join(t.TempDir(), "saguin.db")
			h := startDurableSQLite(t, path)
			conn, code := connectWithWillExpiring(t, h, "dev", "wills/dev", false, 0, 1, tc.connectExpiry)
			if code != 0 {
				t.Fatalf("connect: %#x", code)
			}
			rd.always.Store(true)
			left := time.Now()
			if _, err := conn.Write(tc.disconnect); err != nil {
				t.Fatal(err)
			}
			if !closedWithin(conn, 5*time.Second) {
				t.Fatal("the connection was not closed")
			}
			// Refused before the close, and again by the teardown after it.
			eventually(t, "the store refusing the disconnect", 5*time.Second, func() bool {
				return rd.failed.Load() >= 2
			})
			// And by a retry while the broker runs, not only at the close.
			eventually(t, "a retry of the disconnect refused", 5*time.Second, func() bool {
				return rd.failed.Load() >= 3
			})
			rd.always.Store(false)
			// The store has recovered, and the broker runs on and writes it.
			eventually(t, "the disconnect written once the store took writes", 5*time.Second, func() bool {
				rec, kept, err := inner().Get("dev")
				return err == nil && kept && !rec.DisconnectedAt.IsZero()
			})
			rec, kept, _ := inner().Get("dev")
			t.Logf("refused %d; before the crash: kept=%v disconnected=%v expiry=%d Will=%v",
				rd.failed.Load(), kept, rec.DisconnectedAt, rec.ExpiryInterval, rec.Will != nil)
			h.Crash()
			h = startDurableSQLite(t, path)
			w := connect(t, h, "watcher", true, false)
			w.Sub(t, "wills/#", 1)
			// **Read when the session has ended, by the bound the sleep this
			// replaces gave it**: its expiry runs from the DISCONNECT, and the
			// check came seven and a half seconds after it. A Will is published
			// before its session's record goes (RFC 0003), so a marker
			// published once the record has gone arrives after any Will it
			// published, and first and alone if it published none.
			deadline := left.Add(7500 * time.Millisecond)
			for _, still, _ := inner().Get("dev"); still; _, still, _ = inner().Get("dev") {
				if time.Now().After(deadline) {
					t.Fatalf("the session outlived the expiry its connection left with")
				}
				time.Sleep(10 * time.Millisecond)
			}
			connect(t, h, "marker", true, false).Pub(t, "wills/marker", "marker")
			if r, ok := w.Await(t, 5*time.Second); !ok || r.Payload != "marker" {
				t.Errorf("a Will was published after a clean DISCONNECT: %v", w.Payloads())
			}
		})
	}
}

// gatesDisconnected refuses one client's disconnect while refusing is set,
// and holds the first one asked for after hold is set until released.
type gatesDisconnected struct {
	broker.SessionStore
	client   string
	refusing atomic.Bool
	failed   atomic.Int32
	hold     atomic.Bool
	reached  chan struct{}
	release  chan struct{}
}

func (s *gatesDisconnected) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *gatesDisconnected) Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error {
	if client == s.client && s.refusing.Load() {
		s.failed.Add(1)
		return errors.New("refused for the test")
	}
	if client == s.client && s.hold.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	return s.SessionStore.Disconnected(client, at, dropWill, expiry)
}

// Invariant 17: writing a refused disconnect again never changes a session a
// newer connection owns. The retry's write, once the store takes writes again,
// is held while the client comes back: the CONNECT waits for it rather than
// being written over by it, and the new connection's record is its own -
// connected, holding the Will it armed.
func TestARetriedDisconnectNeverWritesOverTheNextConnection(t *testing.T) {
	for i := range 10 {
		g := &gatesDisconnected{client: "dev", reached: make(chan struct{}), release: make(chan struct{})}
		var inner broker.SessionStore
		brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
			inner, g.SessionStore = s, s
			return g
		}
		t.Cleanup(func() { brokertest.WrapSessions = nil })
		h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
		conn, code := connectWithWillExpiring(t, h, "dev", "wills/dev", false, 0, 1, 60)
		if code != 0 {
			t.Fatalf("round %d: connect: %#x", i, code)
		}
		g.refusing.Store(true)
		if _, err := conn.Write([]byte{0xE0, 0x00}); err != nil {
			t.Fatal(err)
		}
		closedWithin(conn, 5*time.Second)
		eventually(t, "the store refusing the disconnect", 5*time.Second, func() bool {
			return g.failed.Load() >= 2
		})
		// The store recovers, and the retry's write is held.
		g.hold.Store(true)
		g.refusing.Store(false)
		select {
		case <-g.reached:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the refused disconnect was never written again, so this proves nothing", i)
		}
		type result struct {
			conn net.Conn
			code byte
		}
		back := make(chan result, 1)
		go func() {
			c, code := connectWithWillExpiring(t, h, "dev", "wills/again", false, 0, 1, 60)
			back <- result{c, code}
		}()
		lockWaitedOn(t, h, "dev", func() bool { return len(back) > 0 }) // the CONNECT under way while the retry is held
		close(g.release)
		r := <-back
		if r.code != 0 {
			t.Fatalf("round %d: connect again: %#x", i, r.code)
		}
		retries, restore := broker.CountOwedRetries(h.B)
		awaitPasses(t, "the owed-write retry", retries, 1) // past another retry
		restore()
		rec, ok, err := inner.Get("dev")
		if err != nil || !ok || !rec.DisconnectedAt.IsZero() || rec.Will == nil || rec.Will.Topic != "wills/again" {
			var will any
			if rec.Will != nil {
				will = rec.Will.Topic
			}
			t.Fatalf("round %d: the connected session's record reads kept=%v disconnected=%v Will=%v (%v): "+
				"a retry wrote the old connection's disconnect over it", i, ok, rec.DisconnectedAt, will, err)
		}
		_ = r.conn.Close()
		h.Stop()
	}
}

// A clean DISCONNECT whose acknowledgements of the broadcast log the store
// refused closes all the same (it has no answer to refuse with), and they
// are written again once the store takes writes, while the session is away,
// so a crash after that sends the session none of them again.
func TestRefusedBroadcastAcknowledgementsAreWrittenWhileTheSessionIsAway(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	st := new(refusesAcknowledgements)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	// Another session owes the same messages and never reads them, so the
	// log keeps them and only raw's stored cursor says raw is done with them.
	other := dialRaw(t, h, "other", "news/#")
	_ = other.conn.Close()
	c := dialRaw(t, h, "raw", "news/#")
	st.armed.Store(true)
	for i := range 3 {
		owe(t, h, lg, news("a", "m-"+string(rune('0'+i))), "raw", "other")
		c.ack(c.read())
	}
	eventually(t, "the store refusing the acknowledgements", 5*time.Second, func() bool {
		return st.refused.Load() > 0
	})
	if _, err := c.conn.Write([]byte{0xE0, 0x00}); err != nil {
		t.Fatal(err)
	}
	if !closedWithin(c.conn, 5*time.Second) {
		t.Fatal("the broker did not close the connection after the DISCONNECT")
	}
	// Refused again by a retry while the session is away, not only by the
	// DISCONNECT's own write.
	atClose := st.refused.Load()
	eventually(t, "a retry of the acknowledgements refused", 5*time.Second, func() bool {
		return st.refused.Load() > atClose
	})
	refused := st.refused.Load()
	st.armed.Store(false)
	// The store has recovered, the session is away, and the cursor is written.
	eventually(t, "the session's cursor written once the store took writes", 5*time.Second, func() bool {
		pos, ok, err := lg.Position(store.MQTTReader("raw"))
		return err == nil && ok && pos.Offset == lg.Next()
	})
	pos, ok, _ := lg.Position(store.MQTTReader("raw"))
	t.Logf("refused %d; stored cursor before the crash: ok=%v offset=%d next=%d", refused, ok, pos.Offset, lg.Next())
	h.Crash()
	h = startDurableSQLite(t, path)
	attachDrain(t, h)
	again, present := connectRaw(t, h, "raw", 10)
	if !present {
		t.Fatal("the session did not survive the restart")
	}
	got := 0
	for {
		_ = again.conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		first, _, err := brokertest.ReadRawPacket(again.conn)
		if err != nil {
			break
		}
		if first>>4 == packets.Publish {
			got++
		}
	}
	if got != 0 {
		t.Errorf("after the crash the session was sent %d of the 3 messages it acknowledged", got)
	}
}

// gatesAcknowledgements refuses the drain's acknowledgement writes while
// refusing is set, and holds the first one asked for after hold is set until
// released.
type gatesAcknowledgements struct {
	drainSessions
	refusing atomic.Bool
	refused  atomic.Int32
	hold     atomic.Bool
	reached  chan struct{}
	release  chan struct{}
}

func (s *gatesAcknowledgements) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	if s.refusing.Load() {
		s.refused.Add(1)
		return 0, errors.New("disk I/O error (refused for the test)")
	}
	if s.hold.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	return s.drainSessions.Acknowledge(client, cursor, done)
}

// Invariant 17 for the acknowledgements retried while a session is away: the
// retry's write is held while its client comes back with Clean Start, and the
// session that begins is not given the old one's cursor or table - after a
// crash it is sent nothing the old session was owed.
func TestARetriedAcknowledgementNeverWritesOverTheNextSession(t *testing.T) {
	for i := range 10 {
		path := filepath.Join(t.TempDir(), "saguin.db")
		h := startDurableSQLite(t, path)
		g := &gatesAcknowledgements{reached: make(chan struct{}), release: make(chan struct{})}
		lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
			g.drainSessions = s
			return g
		})
		other := dialRaw(t, h, "other", "news/#")
		_ = other.conn.Close()
		c := dialRaw(t, h, "raw", "news/#")
		g.refusing.Store(true)
		for j := range 3 {
			owe(t, h, lg, news("a", "m-"+string(rune('0'+j))), "raw", "other")
			c.ack(c.read())
		}
		if _, err := c.conn.Write([]byte{0xE0, 0x00}); err != nil {
			t.Fatal(err)
		}
		closedWithin(c.conn, 5*time.Second)
		if g.refused.Load() == 0 {
			t.Fatalf("round %d: no acknowledgement was refused, so this proves nothing", i)
		}
		g.hold.Store(true)
		g.refusing.Store(false)
		select {
		case <-g.reached:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the refused acknowledgements were never written again", i)
		}
		back := make(chan *rawClient, 1)
		go func() {
			n, _ := connectRawAs(t, h, "raw", 10, true)
			back <- n
		}()
		lockWaitedOn(t, h, "raw", func() bool { return len(back) > 0 }) // the CONNECT under way while the retry is held
		close(g.release)
		n := <-back
		_ = n.conn.Close()
		sessionGone(t, h, "raw")
		retries, restore := broker.CountOwedRetries(h.B)
		awaitPasses(t, "the owed-write retry", retries, 1) // past another retry
		restore()
		h.Crash()
		h = startDurableSQLite(t, path)
		attachDrain(t, h)
		again, _ := connectRaw(t, h, "raw", 10)
		got := 0
		for {
			_ = again.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			first, _, err := brokertest.ReadRawPacket(again.conn)
			if err != nil {
				break
			}
			if first>>4 == packets.Publish {
				got++
			}
		}
		if got != 0 {
			t.Fatalf("round %d: the session begun new was sent %d messages its predecessor was owed", i, got)
		}
		h.Stop()
	}
}

// refusesPositions refuses a channel log's SavePosition while armed, and
// counts what it refused.
type refusesPositions struct {
	broker.LogStore
	armed   atomic.Bool
	refused atomic.Int32
}

func (l *refusesPositions) Unwrap() broker.LogStore { return l.LogStore }

func (l *refusesPositions) SavePosition(p store.Position) error {
	if l.armed.Load() {
		l.refused.Add(1)
		return errors.New("disk I/O error (refused for the test)")
	}
	return l.LogStore.SavePosition(p)
}

// replayed counts the channel records a resumed consumer is sent within a
// quiet second and a half.
func replayed(t *testing.T, c *brokertest.Client) []string {
	t.Helper()
	var got []string
	for {
		r, ok := c.Await(t, 1500*time.Millisecond)
		if !ok {
			return got
		}
		got = append(got, r.Payload)
	}
}

// A consumer's position its clean DISCONNECT could not store is written
// again once the store takes writes, while the consumer is away, so a crash
// after that replays nothing it acknowledged.
func TestARefusedPositionIsWrittenWhileTheConsumerIsAway(t *testing.T) {
	g := new(refusesPositions)
	gateLog(t, "events", func(lg broker.LogStore) broker.LogStore {
		if g.LogStore == nil {
			g.LogStore = lg
			return g
		}
		return lg
	})
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	cons := brokertest.Dial(t, h, "cons", true, false, 10, 3600, 0)
	cons.Sub(t, "events/#", 1)
	g.armed.Store(true)
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	for _, p := range []string{"r1", "r2", "r3"} {
		pub.Pub(t, "events/x", p)
	}
	awaitCount(t, cons, 3, 5*time.Second)
	acknowledged(t, cons) // the PUBACKs
	before := h.Disconnects.Snapshot("cons")
	cons.Close()
	sessionGoneAfter(t, h, "cons", before)
	// Refused while the consumer is away, by a write after its DISCONNECT's.
	atClose := g.refused.Load()
	eventually(t, "a position write refused while the consumer is away", 5*time.Second, func() bool {
		return g.refused.Load() > atClose
	})
	refused := g.refused.Load()
	if atClose == 0 {
		t.Fatal("no position write was refused before the close, so this proves nothing")
	}
	g.armed.Store(false)
	awaitStoredPosition(t, h, "events", 4, 5*time.Second) // written once the store took writes
	t.Logf("position writes refused: %d", refused)
	h.Crash()
	h = startDurableSQLite(t, path)
	again := brokertest.Dial(t, h, "cons", false, false, 10, 3600, 0)
	if !again.SessionPresent {
		t.Fatal("the consumer's session did not survive the restart")
	}
	if got := replayed(t, again); len(got) != 0 {
		t.Errorf("after the crash the consumer was sent %v again", got)
	}
}

// A subscription with Retain Handling 2 asks an append channel for nothing
// older than now, and its position at the head is written as it is made;
// where the store refused that write it is written again, not marked as
// stored, so the clean DISCONNECT and a crash after it do not replay what
// the client asked not to be sent.
func TestAPositionAtTheHeadTheStoreRefusedIsWrittenAgain(t *testing.T) {
	g := new(refusesPositions)
	gateLog(t, "events", func(lg broker.LogStore) broker.LogStore {
		if g.LogStore == nil {
			g.LogStore = lg
			return g
		}
		return lg
	})
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	for _, p := range []string{"old1", "old2", "old3"} {
		pub.Pub(t, "events/x", p)
	}
	cons := brokertest.Dial(t, h, "cons", true, false, 10, 3600, 0)
	g.armed.Store(true)
	sa, err := cons.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "events/#", QoS: 1, RetainHandling: 2}}})
	if err != nil || len(sa.Reasons) != 1 || sa.Reasons[0] != 1 {
		t.Fatalf("subscribe: %v %+v", err, sa)
	}
	eventually(t, "the head's position refused", 5*time.Second, func() bool { return g.refused.Load() > 0 })
	g.armed.Store(false)
	if got := replayed(t, cons); len(got) != 0 {
		t.Fatalf("a subscription asking for nothing older than now was sent %v", got)
	}
	cons.Close()
	h.Crash()
	h = startDurableSQLite(t, path)
	again := brokertest.Dial(t, h, "cons", false, false, 10, 3600, 0)
	if !again.SessionPresent {
		t.Fatal("the consumer's session did not survive the restart")
	}
	if got := replayed(t, again); len(got) != 0 {
		t.Errorf("after the crash the consumer was sent %v, which its subscription asked not to be", got)
	}
}

// A CONNECT for an id whose last disconnect the store still owes writes it
// first, and is refused 0x83 while the store refuses it (RFC 0003
// "Sessions"): the session it would claim reads as its client never said.
// Once the store takes writes, the CONNECT goes ahead.
func TestAConnectWritesTheDisconnectItsIdIsOwedFirst(t *testing.T) {
	g := &gatesDisconnected{client: "dev"}
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner, g.SessionStore = s, s
		return g
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	conn, code := connectWithWillExpiring(t, h, "dev", "wills/dev", false, 0, 1, 60)
	if code != 0 {
		t.Fatalf("connect: %#x", code)
	}
	g.refusing.Store(true)
	if _, err := conn.Write([]byte{0xE0, 0x00}); err != nil {
		t.Fatal(err)
	}
	closedWithin(conn, 5*time.Second)
	eventually(t, "the store refusing the disconnect", 5*time.Second, func() bool { return g.failed.Load() >= 2 })
	before := g.failed.Load()
	refused, code := connectWithWillExpiring(t, h, "dev", "wills/again", false, 0, 1, 60)
	_ = refused.Close()
	if code != 0x83 {
		t.Fatalf("a CONNECT while its id's disconnect is owed and refused was answered %#x, want 0x83", code)
	}
	if g.failed.Load() == before {
		t.Fatal("the CONNECT did not try the owed write, so this proves nothing")
	}
	g.refusing.Store(false)
	again, code := connectWithWillExpiring(t, h, "dev", "wills/again", false, 0, 1, 60)
	if code != 0 {
		t.Fatalf("a CONNECT once the store takes writes was answered %#x", code)
	}
	defer again.Close()
	rec, ok, err := inner.Get("dev")
	if err != nil || !ok || !rec.DisconnectedAt.IsZero() || rec.Will == nil || rec.Will.Topic != "wills/again" {
		t.Fatalf("the new connection's record reads kept=%v disconnected=%v Will=%+v (%v)",
			ok, rec.DisconnectedAt, rec.Will, err)
	}
}

// refusesReaderDrops refuses a provider's drop of a reader's positions while
// armed, and counts what it refused.
type refusesReaderDrops struct {
	broker.ReaderDropper
	armed   atomic.Bool
	refused atomic.Int32
	calls   atomic.Int32
}

func (d *refusesReaderDrops) DropReader(reader string) (int, error) {
	d.calls.Add(1)
	if d.armed.Load() {
		d.refused.Add(1)
		return 0, errors.New("disk I/O error (refused for the test)")
	}
	return d.ReaderDropper.DropReader(reader)
}

// A new persistent session accepted while an old ending is owed must settle
// that ending before it is claimed. Once the new session has gone away there
// is no owner entry to protect it, so this also proves the old debt itself was
// removed rather than merely deferred by the ownership check.
func TestAnEndingSettledByConnectCannotDropThatSessionAfterItGoesAway(t *testing.T) {
	d := new(refusesReaderDrops)
	brokertest.WrapProviders = func(_ string, rd broker.ReaderDropper) broker.ReaderDropper {
		if d.ReaderDropper == nil {
			d.ReaderDropper = rd
			return d
		}
		return rd
	}
	t.Cleanup(func() { brokertest.WrapProviders = nil })
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := startDurableSQLite(t, path)
	cons := brokertest.Dial(t, h, "cons", true, false, 10, 1, 0)
	cons.Sub(t, "events/#", 1)
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	for _, p := range []string{"r1", "r2", "r3"} {
		pub.Pub(t, "events/x", p)
	}
	awaitCount(t, cons, 3, 5*time.Second)
	awaitStoredPosition(t, h, "events", 4, 5*time.Second)

	d.armed.Store(true)
	cons.Close()
	eventually(t, "the old ending was owed", 10*time.Second, func() bool {
		return d.refused.Load() > 0
	})
	d.armed.Store(false)

	// CONNECT itself must finish the old ending before claiming the id. It
	// starts a fresh durable session, consumes to a new position, then leaves.
	next := brokertest.Dial(t, h, "cons", false, false, 10, 3600, 0)
	if next.SessionPresent {
		t.Fatal("the ended session was resumed")
	}
	next.Sub(t, "events/#", 1)
	if got := replayed(t, next); len(got) != 3 {
		t.Fatalf("new session received %v, want the channel from its start", got)
	}
	acknowledged(t, next)
	before := h.Disconnects.Snapshot("cons")
	next.Close()
	sessionGoneAfter(t, h, "cons", before)
	retries, restore := broker.CountOwedRetries(h.B)
	defer restore()
	afterDisconnect := d.calls.Load()

	// Owed-write passes run with no owner. A stale ending would now be able
	// to remove the new session's record and position.
	awaitPasses(t, "the owed-write retry", retries, 2)
	if got := d.calls.Load(); got != afterDisconnect {
		t.Fatalf("DropReader ran again after the new session went away: %d -> %d; the old ending stayed owed", afterDisconnect, got)
	}
	h.Crash()
	h = startDurableSQLite(t, path)
	again := brokertest.Dial(t, h, "cons", false, false, 10, 3600, 0)
	if !again.SessionPresent {
		t.Fatal("the newer persistent session's record was dropped after it went away")
	}
	if got := replayed(t, again); len(got) != 0 {
		t.Fatalf("the newer session's position was dropped; it replayed %v", got)
	}
}

// A session that ended takes its positions with it (RFC 0003 "Sessions"),
// and where the store refused to drop them the next session under the client
// id - told Session Present 0 - is not resumed at the ended one's offset: it
// is served the channel from where a new subscription starts.
func TestAPositionItsEndedSessionLeftIsNotTheNextSessions(t *testing.T) {
	for _, when := range []string{"by the next CONNECT", "while the broker runs", "at a clean start"} {
		t.Run(when, func(t *testing.T) {
			d := new(refusesReaderDrops)
			brokertest.WrapProviders = func(_ string, rd broker.ReaderDropper) broker.ReaderDropper {
				if d.ReaderDropper == nil {
					d.ReaderDropper = rd
					return d
				}
				return rd
			}
			t.Cleanup(func() { brokertest.WrapProviders = nil })
			var events broker.LogStore
			gateLog(t, "events", func(lg broker.LogStore) broker.LogStore { events = lg; return lg })
			h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
			// A second's expiry ends the session by itself; a clean start
			// ends one that would have lasted.
			expiry := uint32(1)
			if when == "at a clean start" {
				expiry = 3600
			}
			cons := brokertest.Dial(t, h, "cons", true, false, 10, expiry, 0)
			cons.Sub(t, "events/#", 1)
			pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
			for _, p := range []string{"r1", "r2", "r3"} {
				pub.Pub(t, "events/x", p)
			}
			awaitCount(t, cons, 3, 5*time.Second)
			awaitStoredPosition(t, h, "events", 4, 5*time.Second)
			d.armed.Store(true)
			cons.Close()
			var first *brokertest.Client
			if when == "at a clean start" {
				first = brokertest.Dial(t, h, "cons", true, false, 10, 3600, 0)
			}
			eventually(t, "the ended session's positions refused", 10*time.Second, func() bool {
				return d.refused.Load() > 0
			})
			eventually(t, "the refused ending owed", 5*time.Second, func() bool {
				return h.B.EndingOwed("cons")
			})
			if first != nil {
				// The clean start that owes it is ended with it, 0x83, and not
				// built on (RFC 0003 "Sessions"): waited for, so the select
				// below reads the outcome rather than a moment before it.
				select {
				case <-first.C.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("a clean start whose ending the store refused was kept and built on")
				}
			}
			d.armed.Store(false)
			if first != nil {
				// Told Session Present 0: if it is still connected it is the
				// session to serve; if the broker ended it, the client comes
				// back, as a client does.
				select {
				case <-first.C.Done():
				default:
					first.Sub(t, "events/#", 1)
					if got := replayed(t, first); len(got) != 3 {
						t.Errorf("a clean start was sent %v, want the channel from its start: it was "+
							"resumed at the ended session's position", got)
					}
					return
				}
				again := brokertest.Dial(t, h, "cons", true, false, 10, 3600, 0)
				again.Sub(t, "events/#", 1)
				if got := replayed(t, again); len(got) != 3 {
					t.Errorf("a clean start after the refusal was sent %v, want the channel from its start", got)
				}
				return
			}
			if when == "while the broker runs" {
				eventually(t, "the position dropped once the store took writes", 5*time.Second, func() bool {
					_, had, err := events.Position(store.MQTTReader("cons"))
					return err == nil && !had
				})
			}
			next := brokertest.Dial(t, h, "cons", false, false, 10, 60, 0)
			if next.SessionPresent {
				t.Fatal("the session did not end, so this proves nothing")
			}
			next.Sub(t, "events/#", 1)
			if got := replayed(t, next); len(got) != 3 {
				t.Errorf("a session begun new under the id was sent %v, want the channel from its start: "+
					"it was resumed at the ended session's position", got)
			}
		})
	}
}

// refusesHoldDrops refuses a channel's DropHold while armed.
type refusesHoldDrops struct {
	broker.HoldStore
	armed   atomic.Bool
	refused atomic.Int32
}

func (h *refusesHoldDrops) DropHold(e store.Exchange) (bool, error) {
	if h.armed.Load() {
		h.refused.Add(1)
		return false, errors.New("disk I/O error (refused for the test)")
	}
	return h.HoldStore.DropHold(e)
}

// An exactly-once publish its ended session left unreleased goes with it,
// and where the store refused to drop it, a session begun new under the
// client id that reuses the packet identifier is not answered from the old
// one: its own message is the one kept, and the old one is not released in
// its place.
func TestAnExactlyOncePublishItsEndedSessionHeldIsNotTheNextSessions(t *testing.T) {
	for _, when := range []string{"by the next CONNECT", "while the broker runs"} {
		t.Run(when, func(t *testing.T) {
			g := new(refusesHoldDrops)
			brokertest.WrapHolds = func(name string, hs broker.HoldStore) broker.HoldStore {
				if name != "events" || g.HoldStore != nil {
					return hs
				}
				g.HoldStore = hs
				return g
			}
			t.Cleanup(func() { brokertest.WrapHolds = nil })
			h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
			w, _ := qos2Dial(t, h.Addr, "dev", false, 1)
			w.Publish(1, "events/x", "old", false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("PUBREC 0x%02X, want the publish held", rc)
			}
			g.armed.Store(true)
			w.Close()
			eventually(t, "the ended session's held publish refused", 10*time.Second, func() bool {
				return g.refused.Load() > 0
			})
			eventually(t, "the refused ending owed", 5*time.Second, func() bool {
				return h.B.EndingOwed("dev")
			})
			g.armed.Store(false)
			if when == "while the broker runs" {
				eventually(t, "the held publish dropped once the store took writes", 5*time.Second, func() bool {
					held, err := g.HoldStore.Holds()
					return err == nil && len(held) == 0
				})
			}
			w2, ca := qos2Dial(t, h.Addr, "dev", false, 60)
			if ca.SessionPresent {
				t.Fatal("the session did not end, so this proves nothing")
			}
			w2.Publish(1, "events/x", "new", false)
			if rc := w2.Pubrec(1); rc != 0 {
				t.Fatalf("PUBREC 0x%02X for the new session's publish", rc)
			}
			w2.Pubrel(1)
			if rc := w2.Pubcomp(1); rc != 0 {
				t.Fatalf("PUBCOMP 0x%02X for the new session's publish", rc)
			}
			newN, oldN := inChannel(t, h, "reader-new", "new"), inChannel(t, h, "reader-old", "old")
			if newN != 1 || oldN != 0 {
				t.Errorf("the channel holds the new session's message %d times and the ended one's %d, "+
					"want 1 and 0: PUBCOMP 0x00 answered a message that was not kept", newN, oldN)
			}
		})
	}
}

// holdsReaderDrops holds a provider's next DropReader once hold is set,
// until released, after refusing while refusing is set; left closes once the
// held drop has been made.
type holdsReaderDrops struct {
	broker.ReaderDropper
	refusing atomic.Bool
	refused  atomic.Int32
	hold     atomic.Bool
	reached  chan struct{}
	release  chan struct{}
	left     chan struct{}
}

func (d *holdsReaderDrops) DropReader(reader string) (int, error) {
	if d.refusing.Load() {
		d.refused.Add(1)
		return 0, errors.New("disk I/O error (refused for the test)")
	}
	if d.hold.CompareAndSwap(true, false) {
		close(d.reached)
		<-d.release
		defer close(d.left)
	}
	return d.ReaderDropper.DropReader(reader)
}

// Invariant 17 for an ending retried while the broker runs: its drop of the
// ended session's positions, held once the store takes writes, is not made
// under a session a CONNECT has begun meanwhile. The CONNECT waits for it; had
// it not, the drop would take the new session's own position, and a restart
// would send it again what it had acknowledged.
func TestARetriedEndingNeverDropsTheNextSessionsPosition(t *testing.T) {
	for i := range 5 {
		d := &holdsReaderDrops{reached: make(chan struct{}), release: make(chan struct{}),
			left: make(chan struct{})}
		brokertest.WrapProviders = func(_ string, rd broker.ReaderDropper) broker.ReaderDropper {
			if d.ReaderDropper == nil {
				d.ReaderDropper = rd
				return d
			}
			return rd
		}
		t.Cleanup(func() { brokertest.WrapProviders = nil })
		path := filepath.Join(t.TempDir(), "saguin.db")
		h := startDurableSQLite(t, path)
		// Let go on the way out as well - after the broker's own cleanup was
		// registered, so before it runs - so that a failure below stops the
		// broker rather than leaving its drop held for good.
		var once sync.Once
		let := func() { once.Do(func() { close(d.release) }) }
		t.Cleanup(let)
		cons := brokertest.Dial(t, h, "cons", true, false, 10, 1, 0)
		cons.Sub(t, "events/#", 1)
		pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
		for _, p := range []string{"r1", "r2", "r3"} {
			pub.Pub(t, "events/x", p)
		}
		awaitCount(t, cons, 3, 5*time.Second)
		awaitStoredPosition(t, h, "events", 4, 5*time.Second)
		d.refusing.Store(true)
		cons.Close()
		eventually(t, "the ended session's positions refused", 10*time.Second, func() bool {
			return d.refused.Load() > 0
		})
		d.hold.Store(true)
		d.refusing.Store(false)
		select {
		case <-d.reached:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the refused drop was never made again", i)
		}
		back := make(chan *brokertest.Client, 1)
		go func() { back <- brokertest.Dial(t, h, "cons", false, false, 10, 60, 0) }()
		// **Which of the two comes first is the broker's to decide**, and
		// both are read rather than waited out: the CONNECT answered, or
		// queued for the id's session lock behind the held drop.
		var next *brokertest.Client
		for deadline := time.Now().Add(5 * time.Second); next == nil; time.Sleep(time.Millisecond) {
			select {
			case next = <-back:
			default:
			}
			if next != nil || h.Srv.SessionLockWaiters("cons") >= 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: the CONNECT was neither answered nor waiting for the held drop", i)
			}
		}
		// leave reads, acknowledges and leaves cleanly, which stores the
		// position, and answers once that DISCONNECT has been handled: the
		// teardown is finished, or queued behind the held drop.
		leave := func(next *brokertest.Client) brokertest.Snapshot {
			next.Sub(t, "events/#", 1)
			awaitCount(t, next, 3, 5*time.Second)
			acknowledged(t, next) // its PUBACKs
			before := h.Disconnects.Snapshot("cons")
			next.Close()
			eventually(t, "the DISCONNECT handled", 5*time.Second, func() bool {
				return h.Disconnects.FinishedSince(before) || h.Srv.SessionLockWaiters("cons") >= 2
			})
			return before
		}
		var before brokertest.Snapshot
		if next != nil {
			// Connected while the drop is held: the session it began reads,
			// acknowledges and leaves cleanly - which stores its position -
			// before the drop lands. The retry runs on the broker's own loop,
			// which is what flushes a position on its tick, so only a
			// DISCONNECT stores one while it is held.
			t.Logf("round %d: connected while the drop was held", i)
			before = leave(next)
			let()
		} else {
			let()
			before = leave(<-back)
		}
		<-d.left // the held drop made
		sessionGoneAfter(t, h, "cons", before)
		h.Crash()
		h = startDurableSQLite(t, path)
		again := brokertest.Dial(t, h, "cons", false, false, 10, 60, 0)
		if !again.SessionPresent {
			t.Fatalf("round %d: the session begun after the ending did not survive the crash, so this "+
				"proves nothing", i)
		}
		if got := replayed(t, again); len(got) != 0 {
			t.Fatalf("round %d: after a crash the session was sent %v again: a drop meant for the ended "+
				"session took its position", i, got)
		}
		h.Stop()
	}
}

// refusesPositionDrops refuses a channel log's DropPosition while armed.
type refusesPositionDrops struct {
	broker.LogStore
	armed   atomic.Bool
	refused atomic.Int32
}

func (l *refusesPositionDrops) Unwrap() broker.LogStore { return l.LogStore }

func (l *refusesPositionDrops) DropPosition(reader string) (bool, error) {
	if l.armed.Load() {
		l.refused.Add(1)
		return false, errors.New("disk I/O error (refused for the test)")
	}
	return l.LogStore.DropPosition(reader)
}

// A session retention passed ends at the CONNECT that finds it (RFC 0003
// "Resuming below the retention floor"), dropping the positions retention
// passed. Where the store refuses that drop, the CONNECT is refused 0x83
// rather than answered Session Present 0 and built on (RFC 0003 "Sessions"):
// the drop still owed would otherwise be made once that connection had gone,
// under the session it began, taking the position it had stored - and its
// next resume would be sent again what it had acknowledged.
func TestAConnectWhoseRetentionEndingTheStoreRefusedIsRefused(t *testing.T) {
	g := new(refusesPositionDrops)
	gateLog(t, "events", func(lg broker.LogStore) broker.LogStore { g.LogStore = lg; return g })
	h := startTrimming(t, map[string]int64{"events": 200})
	p := connect(t, h, "producer", true, false)
	c := connect(t, h, "device", false, false)
	c.Sub(t, "events/#", 1)
	p.Pub(t, "events/a/1", "first")
	if _, ok := c.Await(t, 3*time.Second); !ok {
		t.Fatal("no delivery, so no position is stored")
	}
	awaitStoredPosition(t, h, "events", 2, 5*time.Second) // its position stored
	linkCut(t, c)
	for i := range 30 {
		p.Pub(t, "events/a/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
	}
	awaitFloorPast(t, h, "events", 2, 5*time.Second) // the size sweep past the position

	// The CONNECT that finds the floor passed; its drop is refused.
	g.armed.Store(true)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got := make(chan string, 64)
	var lastOffset atomic.Uint64
	pc := paho.NewClient(paho.ClientConfig{Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(r paho.PublishReceived) (bool, error) {
			if r.Packet.Properties != nil {
				for _, u := range r.Packet.Properties.User {
					if off, err := strconv.ParseUint(u.Value, 10, 64); u.Key == "saguin-offset" && err == nil {
						lastOffset.Store(off)
					}
				}
			}
			got <- string(r.Packet.Payload)
			return true, nil
		}}})
	expiry := uint32(3600)
	ca, _ := pc.Connect(context.Background(), &paho.Connect{ClientID: "device", CleanStart: false, KeepAlive: 60,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}})
	if ca == nil {
		t.Fatal("no CONNACK")
	}
	if g.refused.Load() == 0 {
		t.Fatal("the retention ending's drop was never refused, so this proves nothing")
	}
	g.armed.Store(false)
	var session *brokertest.Client
	if ca.ReasonCode == 0 {
		// Accepted, told Session Present 0: this connection is the session.
		// It subscribes, is served, acknowledges, and goes.
		t.Logf("the CONNECT was accepted, Session Present %v", ca.SessionPresent)
		if _, err := pc.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
			{Topic: "events/#", QoS: 1}}}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		p.Pub(t, "events/a/last", "after-the-gap")
		for deadline := time.After(5 * time.Second); ; {
			select {
			case m := <-got:
				if m != "after-the-gap" {
					continue
				}
			case <-deadline:
				t.Fatal("the accepted session was served nothing")
			}
			break
		}
		// Its PUBACKs read, and its position stored.
		awaitStoredPosition(t, h, "events", lastOffset.Load()+1, 5*time.Second)
		before := h.Disconnects.Snapshot("device")
		_ = conn.Close()
		sessionGoneAfter(t, h, "device", before)
	} else {
		// Refused: the client connects again, and that CONNECT drops what is
		// owed before it is answered.
		t.Logf("the CONNECT was refused 0x%02x", ca.ReasonCode)
		session = connect(t, h, "device", false, false)
		session.Sub(t, "events/#", 1)
		p.Pub(t, "events/a/last", "after-the-gap")
		awaitCount(t, session, 3, 5*time.Second)
		acknowledged(t, session)
		all := session.All()
		last, err := strconv.ParseUint(all[len(all)-1].User["saguin-offset"], 10, 64)
		if err != nil {
			t.Fatalf("the last record carried no offset: %v", all[len(all)-1].User)
		}
		awaitStoredPosition(t, h, "events", last+1, 5*time.Second)
		linkCut(t, session)
	}
	retries, restore := broker.CountOwedRetries(h.B)
	awaitPasses(t, "the owed-write retry", retries, 2) // past a retry of anything owed
	restore()

	back := connect(t, h, "device", false, false)
	if !back.SessionPresent {
		t.Fatal("the session begun after the refusal was not kept, so this proves nothing")
	}
	if again := replayed(t, back); len(again) != 0 {
		t.Errorf("resumed, the session was sent %v again: the drop owed from the ended session took "+
			"the position the session after it had stored", again)
	}
}

// resumeCode sends a CONNECT with Clean Start 0 and a day's expiry for id, and
// answers the CONNACK's reason code; the connection is closed.
func resumeCode(t *testing.T, h *brokertest.Harness, id string) byte {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	expiry := uint32(3600)
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	ca, err := c.Connect(context.Background(), &paho.Connect{ClientID: id, CleanStart: false, KeepAlive: 60,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}})
	if ca == nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	return ca.ReasonCode
}

// awaitPasses waits until count, from a CountOwedRetries or the like
// installed just before, shows n whole passes of the broker's loop: the first
// it counts may have begun before it was installed, so n+1.
//
// It stands where a sleep "past another retry" stood. How long a pass takes
// to come round is a fact about the machine; what the test needs is that one
// has run, start to finish, after the thing it is checking.
func awaitPasses(t *testing.T, what string, count func() int64, n int64) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); count() < n+1; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s came round %d times in 10s, want %d", what, count(), n+1)
		}
	}
}

// lockWaitedOn waits until the client id's session lock is held and asked for
// by one more - the CONNECT a test started is waiting its turn behind a held
// write - or until answered says it was not made to wait at all. Either is
// read rather than presumed after a pause.
func lockWaitedOn(t *testing.T, h *brokertest.Harness, id string, answered func() bool) {
	t.Helper()
	eventually(t, "a CONNECT waiting for "+id+"'s session lock, or answered", 5*time.Second, func() bool {
		return h.Srv.SessionLockWaiters(id) >= 2 || answered()
	})
}
