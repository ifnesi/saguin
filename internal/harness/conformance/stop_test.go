package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Access and reload: "SIGINT and SIGTERM stop gracefully";
// Restart, Graceful stop: "every client gets 0x8B"; Flows, DISCONNECT:
// "These reasons are MQTT 5 only. An MQTT 3.1.1 connection just closes";
// Restart, A stop for a failure: "The exit status is 0 after a stop requested
// by SIGINT or SIGTERM".
func TestSIGTERMStopsGracefully(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "stop-v5", CleanStart: true})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "stop-v311", CleanStart: true})
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	shutDown(t, v5)
	harness.ExpectClosed(t, v311)
	// Each closes its end once the broker has ended its own.
	_, _ = v5.Close(), v311.Close()
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, Restart, Graceful stop: "after limits.write_timeout from when
// the core accepts the stop, the remaining sockets are cut [...]; main
// waits until every reader, writer and dialer has ended". A client that neither reads nor closes holds its
// connection until the write timeout, and no longer: the broker exits no
// sooner than write_timeout after SIGTERM, and within stopLimit.
func TestStopCutsAClientThatDoesNotClose(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "never-closes", CleanStart: true})
	sent := time.Now()
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	support.Exited(t, b)
	if took := time.Since(sent); took < support.WriteTimeout {
		t.Errorf("the broker exited %v after SIGTERM, with a client still open: before write_timeout (%v)", took, support.WriteTimeout)
	}
}

// design.md, Restart, Graceful stop: "the core waits until every store
// request it sent, read or write, is answered"; Restart: "the broker restores
// durable sessions". A durable session the store confirmed (design.md, Flows,
// Replies: "CONNACK (when it changes a session) [...] sent only once the
// store confirms") just before SIGTERM is there after a restart: MQTT 5
// 3.2.2.1.1, with Clean Start 0 and Session State for the ClientID, "it MUST
// set Session Present to 1" [MQTT-3.2.2-3].
func TestDurableSessionSurvivesTheStop(t *testing.T) {
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "durable", CleanStart: true, Props: support.Durable})
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	_ = c.Close() // the broker is gone

	again := support.Launch(t, &harness.Options{Dir: dir})
	_, ack := harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: "durable", Props: support.Durable})
	if !ack.SessionPresent {
		t.Error("after the restart, CONNACK with Clean Start 0 has Session Present 0: the session saved before SIGTERM is gone")
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop whose
// changes the stop stores (R4)"; "At a graceful stop, every message Sagüin
// stores is therefore acknowledged"; Restart, A stop for a failure: "The exit
// status is 0 after a stop requested by SIGINT or SIGTERM"; Flows, Replies:
// "Closing may supersede pending replies under the closing rules (Closing a
// connection). A graceful stop sends them first (Restart, Graceful stop)";
// Store, Writes: "Stop ends collection delays and drains accepted work under
// the shutdown rules"; Restart: "the broker restores durable sessions". MQTT
// 5 4.4: reconnecting with Clean Start 0 to a session present, the Client
// "MUST resend any unacknowledged PUBLISH packets (where QoS > 0) [...] using
// their original Packet Identifiers" [MQTT-4.4.0-1]; a PUBLISH whose PUBACK
// came is acknowledged and is not resent. Audit 261022-001a: the stop sent
// 0x8B before the PUBACK of what it stored, and the client's resend after the
// restart stored that publication twice.
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 5 client holding a durable session publishes at QoS 1,
// then pings: the PINGRESP, read first, says the core has taken the
// PUBLISH (design.md, Execution, Decisions: "Each connection's packet order
// is preserved"), whose PUBACK waits in the collecting group. SIGTERM comes
// within the interval. The client reads PUBACK 0x00, then DISCONNECT 0x8B
// and the end of stream, and the broker exits 0. Started again on the same
// file, the client reconnects with Clean Start 0 and finds its session; its
// PUBLISH was acknowledged, so it holds nothing unacknowledged to resend. A
// PINGREQ, answered with nothing queued ahead of its PINGRESP, shows the
// broker resends it nothing either. After that broker's stop, the channel
// holds the publication's record once.
func TestStopAcknowledgesWhatItStores(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords}
	b := support.Launch(t, o)
	connect := &harness.Connect{ClientID: "acknowledged", Props: support.Durable}
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	p := &harness.Publish{QoS: 1, Topic: "stop/acknowledged", Payload: []byte("x")}
	published := support.Sent(t, c, p)
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBLISH: %v; want the PINGRESP first, the PUBACK waiting in the collecting group", err)
	}
	if since := time.Since(published); since >= commitInterval {
		t.Fatalf("SIGTERM %v after the PUBLISH, past publish_commit_interval (%v): its group may have gone at its deadline, so nothing here tests the stop",
			since, commitInterval)
	}
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// The stop stores the collecting group's one publication, so its PUBACK
	// precedes the connection's DISCONNECT 0x8B.
	if ack, err := c.Recv(); err != nil {
		t.Fatalf("after SIGTERM: %v; want PUBACK 0x00 to the PUBLISH, then DISCONNECT 0x8B", err)
	} else if a, ok := ack.(*harness.Ack); !ok || a.Kind != harness.PUBACK || a.ID != p.ID || a.Reason != 0 {
		t.Fatalf("after SIGTERM the client read %s %+v first; want PUBACK 0x00 to its PUBLISH, then DISCONNECT 0x8B", harness.Name(ack.Type()), ack)
	}
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}

	again := support.Launch(t, o)
	c, connack := harness.Connected(t, again.Addr, harness.V5, connect)
	if !connack.SessionPresent {
		t.Fatal("after the restart, CONNACK with Clean Start 0 has Session Present 0: the durable session is gone")
	}
	// Nothing unacknowledged to resend (MQTT-4.4.0-1), and nothing owed back:
	// the PINGRESP is the first and only packet.
	if err := c.Ping(); err != nil {
		t.Fatalf("after the restart: %v; want the PINGRESP alone, the acknowledged PUBLISH neither resent nor redelivered", err)
	}
	stopped(t, again)
	n := 0
	for _, r := range records(t, again.DB) {
		if r.topic == p.Topic {
			n++
		}
	}
	if n != 1 {
		t.Errorf("after the restart, the channel holds %d records of the publication, want 1", n)
	}
}

// pubacksPending is how many QoS 1 PUBLISHes TestStopPendingAcknowledgementsAreInPacketOrder
// pipelines at the stop: several, within config.md's default
// qos2.max_inflight_per_client (20), the Receive Maximum the broker counts
// its client's unanswered QoS 1 PUBLISHes against.
const pubacksPending = 4

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop whose
// changes the stop stores (R4)"; design.md, Flows, Replies: "Acknowledgements
// of stored changes preserve their packet order", and PUBACK "sent only once
// the store confirms". A client may have several QoS 1 PUBACKs pending at the
// stop, not one: the stop stores the publications and sends every PUBACK, in
// packet order, before the connection's DISCONNECT 0x8B.
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 5 client pipelines pubacksPending QoS 1 PUBLISHes within
// one interval, not waiting for a PUBACK between them, so all collect in one
// group. It then pings: the PINGRESP, read first (the PUBACKs wait in the
// collecting group), says the core has taken all pubacksPending PUBLISHes
// (design.md, Execution, Decisions: "Each connection's packet order is
// preserved"), so none is a packet taken after the stop. Only then does
// SIGTERM go, within the interval. The client reads PUBACK 0x00 for each,
// in the order it sent them, then DISCONNECT 0x8B and the end of stream;
// the broker exits 0.
func TestStopPendingAcknowledgementsAreInPacketOrder(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pipelined", CleanStart: true})
	ids := make([]uint16, pubacksPending)
	start := time.Now()
	for i := range ids {
		p := &harness.Publish{QoS: 1, ID: c.NextID(), Topic: "stop/pipelined", Payload: []byte(strconv.Itoa(i))}
		ids[i] = p.ID
		if err := c.Send(p); err != nil {
			t.Fatalf("pipelining PUBLISH %d: %v", p.ID, err)
		}
	}
	// The fence: the PINGRESP comes only once the core has taken every
	// PUBLISH before the PINGREQ, so the stop cannot interleave before one is
	// taken and leave it unacknowledged (a packet taken after the stop).
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the %d PUBLISHes: %v; want the PINGRESP first, the PUBACKs waiting in the collecting group", pubacksPending, err)
	}
	if since := time.Since(start); since >= commitInterval {
		t.Fatalf("the %d PUBLISHes and the fencing PINGREQ took %v, past publish_commit_interval (%v): their group may have gone at its deadline, so nothing here tests the stop",
			pubacksPending, since, commitInterval)
	}
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		p, err := c.Recv()
		if err != nil {
			t.Fatalf("waiting for PUBACK %d (the %d of %d pending): %v; want every PUBACK in packet order before DISCONNECT 0x8B", id, i+1, len(ids), err)
		}
		if a, ok := p.(*harness.Ack); !ok || a.Kind != harness.PUBACK || a.ID != id || a.Reason != 0 {
			t.Fatalf("the %d reply after SIGTERM was %s %+v; want PUBACK 0x00 for packet %d: the pending acknowledgements come in packet order", i+1, harness.Name(p.Type()), p, id)
		}
	}
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK [...] of
// packets taken before the stop whose changes the stop stores (R4)";
// DISCONNECT, "Reasons Sagüin sends": "These reasons are MQTT 5 only. An MQTT
// 3.1.1 connection just closes"; design.md, Flows, Replies: PUBACK "sent only
// once the store confirms". An MQTT 3.1.1 publisher's owed PUBACK is sent
// before its connection closes, and it gets no server DISCONNECT.
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 3.1.1 client publishes at QoS 1, then pings: the
// PINGRESP, read first, says the core has taken the PUBLISH, whose PUBACK
// waits in the collecting group. SIGTERM comes within the interval. The
// client reads PUBACK 0x00, then the end of stream, with no DISCONNECT; the
// broker exits 0.
func TestStopAcknowledgesAnMQTT311Publisher(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "legacy", CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "stop/legacy", Payload: []byte("x")}
	published := support.Sent(t, c, p)
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBLISH: %v; want the PINGRESP first, the PUBACK waiting in the collecting group", err)
	}
	if since := time.Since(published); since >= commitInterval {
		t.Fatalf("SIGTERM %v after the PUBLISH, past publish_commit_interval (%v): its group may have gone at its deadline, so nothing here tests the stop",
			since, commitInterval)
	}
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// MQTT 3.1.1 PUBACK carries no reason code; the harness leaves it 0.
	if ack, err := c.Recv(); err != nil {
		t.Fatalf("after SIGTERM: %v; want PUBACK 0x00, then the end of stream with no DISCONNECT", err)
	} else if a, ok := ack.(*harness.Ack); !ok || a.Kind != harness.PUBACK || a.ID != p.ID || a.Reason != 0 {
		t.Fatalf("after SIGTERM the MQTT 3.1.1 client read %s %+v; want PUBACK 0x00 to its PUBLISH, then the end of stream, no DISCONNECT", harness.Name(ack.Type()), ack)
	}
	harness.ExpectClosed(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, R8: "A store error stops the broker, except a failed memory-mode
// snapshot"; Store: "A damaged file_path stops the broker [...] Otherwise it
// does so when first touched"; Restart: "A stop for a failure, whether a
// store error or a failed accept, follows the graceful stop" ("every client
// gets 0x8B"), and "The exit status is [...] It is 1 after a stop caused by a
// failure"; design.md, Flows, Logging: "The core logs its first fatal store
// error as one ERROR line naming the error. It also logs each reply a writer
// could not build as one ERROR line naming the error, from that writer's
// close report. These lines follow the admission rules above" (decided from
// items 0008a and 0026d). The database overwritten whole is damaged: SQLite
// names it "file is not a database" (SQLITE_NOTADB) or "database disk image
// is malformed" (SQLITE_CORRUPT), as sqlite3_errstr gives them. Exactly one
// ERROR line names that error.
func TestDamagedDatabaseStopsInOrder(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "connected", CleanStart: true})
	damage(t, b.DB)
	touch(t, b, "touches-the-store")
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b); state.ExitCode() != 1 {
		t.Errorf("the stop on a store error ended %v, want exit status 1", state)
	}
	damaged := []string{"file is not a database", "database disk image is malformed"}
	if lines := testkit.LinesNaming(b.Log(), "ERROR", damaged...); len(lines) != 1 {
		t.Errorf("%d ERROR lines name the damage (%q), want exactly one:\n%s", len(lines), damaged, b.Log())
	}
}

// design.md, CONNECT, Connection permits: "Permits bound connections to
// limits.max_connections. [...] An MQTT listener accepts a connection, then
// takes a permit before serving it. While no permit is left, the listener
// holds that connection unserved and accepts no more. More connections wait
// in the accept queue. The core puts the permit back when the writer reports
// the close"; config.md, max_connections: "connections served at once". With
// max_connections at 2 and two clients connected, a third client's CONNECT is
// not answered while both stay; once one disconnects, the third is accepted
// and connected.
func TestFullListenerWaitsForAClose(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxConnections: 2})
	first, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "first", CleanStart: true})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "second", CleanStart: true})
	third, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatalf("a connection waiting in the accept queue: %v", err)
	}
	t.Cleanup(func() { _ = third.Close() }) // the test is over
	if err := third.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "third", CleanStart: true}); err != nil {
		t.Fatal(err)
	}
	// The window is write_timeout, the longest the design gives a reply the
	// broker has decided to reach the client (design.md, Flows, Closing a
	// connection: "A write that has not finished limits.write_timeout after
	// its first socket-write attempt closes the connection as
	// writer_full"): a broker that had
	// accepted the third connection would have answered its CONNECT, or
	// closed it, within it.
	nothingWithin(t, third, support.WriteTimeout, "with max_connections 2 and two clients connected, the third unserved")

	if err := first.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, first)
	_ = first.Close() // its permit goes back with the writer's close report
	if ack := harness.Expect[*harness.Connack](t, third); ack.Reason != 0 {
		t.Errorf("the third client, once one of two closed: CONNACK 0x%02X, want 0x00", ack.Reason)
	}
}

// design.md, Flows, CONNECT, Connection permits: "Permits bound connections
// to limits.max_connections. One bounded channel holds that many permits. An
// MQTT listener accepts a connection, then takes a permit before serving it";
// it takes none while no connection is accepted. With two MQTT listeners and
// max_connections at 2, a listener no client connects to holds no permit: two
// clients connecting one after the other to the other listener are both
// accepted and connected. The first listener takes only Launch's probe, which
// closes without a CONNECT.
func TestIdleListenerHoldsNoPermit(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxConnections: 2, TCPListeners: 2})
	busy := b.Addrs[1]
	harness.Connected(t, busy, harness.V5, &harness.Connect{ClientID: "first", CleanStart: true})
	harness.Connected(t, busy, harness.V5, &harness.Connect{ClientID: "second", CleanStart: true})
}

// floodClients is how many clients end their sessions around the SIGTERM
// in TestDisconnectFloodAroundTheStop: audit 261018-01's probe, whose 300
// clients met every order a DISCONNECT and the stop can meet in (finding
// 261018-001a), well within config.md's max_connections (10000) and
// max_connect_rate's burst (twice 500).
const floodClients = 300

// floodPublishers is how many clients publish around the SIGTERM in
// TestDisconnectFloodAroundTheStop.
const floodPublishers = 30

// endSession is an MQTT 5 client's DISCONNECT ending its session: Session
// Expiry Interval 0 (MQTT 5 3.14.2.2.2).
var endSession = &harness.Disconnect{Props: support.SessionExpiry(0)}

// floodOutcome is what one client of the flood read after its DISCONNECT.
type floodOutcome struct {
	bareClose bool  // the end of stream alone
	err       error // anything the design does not allow; nil otherwise
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken"; "It acts on a client's DISCONNECT by
// storing its session expiry change"; Restart, A stop for a failure: "The
// exit status is 0 after a stop requested by SIGINT or SIGTERM"; DISCONNECT:
// the expiry change "is stored before the connection closes"; Restart: "the
// broker restores durable sessions". Audit 261018-01's probe, kept as a guard
// of the stop under load (Observation 1). floodClients MQTT 5 clients, each
// holding a durable session, write DISCONNECT with Session Expiry Interval 0
// together, then end their sending (MQTT-3.14.4-2), and the test sends
// SIGTERM once half of them have written theirs. Then:
//   - the broker exits 0 within stopLimit;
//   - each client reads one of the two outcomes the design allows: the end
//     of stream alone (the core took its DISCONNECT and the clean close was
//     the client's), or DISCONNECT 0x8B and then the end of stream;
//   - after a restart on the same files, each client that read the end of
//     stream alone finds no session: its DISCONNECT was taken, so its
//     expiry of 0 was stored before its connection closed (MQTT 5
//     3.2.2.1.1: Session Present 0 where the Server has no Session State);
//   - the restarted broker serves every client.
//
// A client that read 0x8B is not checked after the restart: whether its
// reader read its DISCONNECT before the writer stopped it (R2) is not
// something a client can see, and a DISCONNECT never read leaves the
// session, rightly. This test passed on a broker with finding 001a; the
// core's tests in internal/core/stop_test.go hold the orders it cannot
// force.
//
// Publish traffic runs through it (0009a.md, 261018 195854): design.md,
// Flows, Replies, PUBACK "sent only once the store confirms"; Graceful
// stop: every client gets 0x8B "The 0x8B follows the PUBACK, PUBREC, PUBREL,
// PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop whose changes
// the stop stores (R4)", "any other packet it takes is neither stored nor
// answered", "At a graceful stop, every message Sagüin stores is therefore
// acknowledged, and none it leaves unacknowledged is stored, except where the
// cutoff comes before the store confirms, the store fails, or the client's
// own DISCONNECT has ended its connection", and "the core waits until every
// store request it sent [...] is answered". floodPublishers MQTT 5 clients
// publish at QoS 1, one message at a time, from before the DISCONNECTs until
// the stop ends their connections; the DISCONNECTs start once each has had a
// PUBACK. No publisher sends a DISCONNECT and the store does not fail, so a
// publisher reads PUBACK 0x00 for each publish and then either DISCONNECT
// 0x8B and the end of stream, or — where the cutoff closed it before the
// store confirmed its last publish (which the design permits) — a closed
// socket. After the exit, each publisher's acknowledged messages are in the
// database, each exactly once: stored before acknowledged (R4), and never
// twice (finding 001a). A message stored without a PUBACK is allowed only for
// a publisher the cutoff cut: a publisher that read 0x8B had every stored
// message acknowledged ahead of it.
func TestDisconnectFloodAroundTheStop(t *testing.T) {
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	clients := make([]*harness.Client, floodClients)
	ids := make([]string, floodClients)
	for i := range clients {
		ids[i] = "flood-" + strconv.Itoa(i)
		clients[i], _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: ids[i], CleanStart: true, Props: support.Durable})
		// A client reads until the broker has ended its connection, which
		// the graceful stop bounds.
		clients[i].Timeout = support.StopLimit
	}

	publishers := make([]*harness.Client, floodPublishers)
	for i := range publishers {
		publishers[i], _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "flood-publisher-" + strconv.Itoa(i), CleanStart: true})
		publishers[i].Timeout = support.StopLimit
	}

	outcomes := make([]floodOutcome, floodClients)
	published := make([]publishOutcome, floodPublishers)
	release, wrote, first := make(chan struct{}), make(chan struct{}, floodClients), make(chan struct{}, floodPublishers)
	var wg sync.WaitGroup
	for i, c := range publishers {
		wg.Go(func() { published[i] = publishUntilStopped(c, strconv.Itoa(i), first) })
	}
	for i, c := range clients {
		wg.Go(func() {
			<-release
			outcomes[i] = disconnectAndRead(c, wrote)
		})
	}
	for range floodPublishers {
		<-first
	}
	close(release)
	for range floodClients / 2 {
		<-wrote
	}
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if state := support.Exited(t, b); !state.Success() {
		t.Fatalf("the graceful stop under a flood of DISCONNECTs ended %v, want exit status 0", state)
	}
	for i, o := range outcomes {
		if o.err != nil {
			t.Errorf("client %s: %v", ids[i], o.err)
		}
	}
	stored := records(t, b.DB)
	for i, o := range published {
		id := "flood-publisher-" + strconv.Itoa(i)
		if o.err != nil {
			t.Errorf("publisher %s: %v", id, o.err)
		}
		missing, doubled, unacknowledged := storedByPublisher(o.acked, stored, id)
		if len(missing) != 0 {
			t.Errorf("publisher %s: %d of the %d messages whose PUBACK 0x00 came are not in the database after the exit (stored before acknowledged, R4): %q",
				id, len(missing), len(o.acked), missing)
		}
		if len(doubled) != 0 {
			t.Errorf("publisher %s: %d messages are in the database more than once (finding 001a): %q", id, len(doubled), doubled)
		}
		// A message stored without its PUBACK is allowed only where the
		// cutoff cut the connection before the store confirmed; a publisher
		// that read DISCONNECT 0x8B had every stored message acknowledged
		// ahead of it.
		if !o.cut && len(unacknowledged) != 0 {
			t.Errorf("publisher %s read DISCONNECT 0x8B, yet %d of its stored messages never had their PUBACK, the 0x8B coming first: %q",
				id, len(unacknowledged), unacknowledged)
		}
	}

	again := support.Launch(t, &harness.Options{Dir: dir})
	for i, id := range ids {
		_, ack := harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable})
		if outcomes[i].bareClose && ack.SessionPresent {
			t.Errorf("client %s read the end of stream alone after its DISCONNECT with Session Expiry Interval 0, and after the restart its session is still present", id)
		}
	}
}

// publishOutcome is what one publisher of the flood read.
type publishOutcome struct {
	acked map[string]bool // the payloads whose PUBACK 0x00 came
	cut   bool            // the connection was cut (the cutoff), not closed after DISCONNECT 0x8B
	err   error           // anything the design does not allow; nil otherwise
}

// publishUntilStopped publishes from c at QoS 1, one message at a time,
// each with a payload of its own, name and a count, reading each one's
// PUBACK, until the broker's DISCONNECT 0x8B comes; it then ends its
// sending and reads to the end of stream. A closed socket while it awaits a
// PUBACK is the cutoff closing it before the store confirmed (Graceful
// stop): it records that (cut) and returns, no error. It says so on first
// once, when its first PUBACK has come or it has ended, whichever is sooner.
func publishUntilStopped(c *harness.Client, name string, first chan<- struct{}) (out publishOutcome) {
	out.acked = map[string]bool{}
	signalled := false
	signal := func() {
		if !signalled {
			first <- struct{}{}
			signalled = true
		}
	}
	defer signal()
	for n := 0; ; n++ {
		p := &harness.Publish{QoS: 1, ID: c.NextID(), Topic: "flood/publish", Payload: []byte(name + "-" + strconv.Itoa(n))}
		if err := c.Send(p); err != nil {
			out.err = fmt.Errorf("writing PUBLISH %d: %w", p.ID, err)
			return out
		}
		got, err := c.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
				out.cut = true // the cutoff, before the store confirmed this publish
				return out
			}
			out.err = fmt.Errorf("waiting for PUBACK %d: %w, want it, DISCONNECT 0x8B or the cutoff", p.ID, err)
			return out
		}
		switch got := got.(type) {
		case *harness.Ack:
			if got.Kind != harness.PUBACK || got.ID != p.ID || got.Reason != 0 {
				out.err = fmt.Errorf("read %s %+v after PUBLISH %d, want PUBACK 0x00 for it", harness.Name(got.Kind), got, p.ID)
				return out
			}
			out.acked[string(p.Payload)] = true
			signal()
		case *harness.Disconnect:
			if got.Reason != 0x8B {
				out.err = fmt.Errorf("DISCONNECT 0x%02X, want 0x8B (Server shutting down)", got.Reason)
				return out
			}
			if err := c.CloseWrite(); err != nil {
				out.err = fmt.Errorf("ending its sending: %w", err)
				return out
			}
			if err := c.AwaitClose(); err != nil {
				out.err = fmt.Errorf("after DISCONNECT 0x8B: %w", err)
			}
			return out
		default:
			out.err = fmt.Errorf("read %s %+v after PUBLISH %d, want its PUBACK or DISCONNECT 0x8B", harness.Name(got.Type()), got, p.ID)
			return out
		}
	}
}

// storedByPublisher counts publisher's records in stored and returns, each
// sorted: missing, the payloads in acked that no record holds (acknowledged
// but not stored, against R4); doubled, the payloads a record holds more
// than once (stored twice, finding 001a); and unacknowledged, the payloads
// a record holds that acked lacks (stored without a PUBACK).
func storedByPublisher(acked map[string]bool, stored []record, publisher string) (missing, doubled, unacknowledged []string) {
	counts := map[string]int{}
	for i := range stored {
		if stored[i].publisher == publisher {
			counts[string(stored[i].payload)]++
		}
	}
	for payload, n := range counts {
		if n > 1 {
			doubled = append(doubled, payload)
		}
		if !acked[payload] {
			unacknowledged = append(unacknowledged, payload)
		}
	}
	for payload := range acked {
		if counts[payload] == 0 {
			missing = append(missing, payload)
		}
	}
	slices.Sort(missing)
	slices.Sort(doubled)
	slices.Sort(unacknowledged)
	return missing, doubled, unacknowledged
}

// disconnectAndRead writes c's DISCONNECT ending its session, says so on
// wrote, ends its sending, and reads what the broker sends until the end
// of stream, returning what it read.
func disconnectAndRead(c *harness.Client, wrote chan<- struct{}) floodOutcome {
	err := c.Send(endSession)
	wrote <- struct{}{}
	if err != nil {
		return floodOutcome{err: fmt.Errorf("writing DISCONNECT: %w", err)}
	}
	if err := c.CloseWrite(); err != nil {
		return floodOutcome{err: fmt.Errorf("ending its sending: %w", err)}
	}
	p, err := c.Recv()
	switch {
	case errors.Is(err, io.EOF):
		return floodOutcome{bareClose: true}
	case err != nil:
		return floodOutcome{err: fmt.Errorf("reading after DISCONNECT: %w, want the end of stream or DISCONNECT 0x8B", err)}
	}
	if d, ok := p.(*harness.Disconnect); !ok || d.Reason != 0x8B {
		return floodOutcome{err: fmt.Errorf("read %s %+v after DISCONNECT, want the end of stream or DISCONNECT 0x8B", harness.Name(p.Type()), p)}
	}
	if err := c.AwaitClose(); err != nil {
		return floodOutcome{err: fmt.Errorf("after DISCONNECT 0x8B: %w", err)}
	}
	return floodOutcome{}
}

// awayExpiry is the Session Expiry Interval, in seconds, that the
// DISCONNECT of TestStopWaitsForADisconnectStillAtTheStore sets: neither
// 0 nor its CONNECT's (durable's 3600), so the stored row tells which
// was saved.
const awayExpiry = 120

// heldPast is how long past limits.write_timeout that test still finds its
// connection open: the time the stop takes to reach the connections after
// SIGTERM. The test holds SQLite's write lock this long after the broker
// first meets it, which must stay well within SQLite's own wait for the
// lock (go-sqlite3's busy timeout, 5 s by default): past it, the write
// fails, a store error (R8).
const heldPast = support.WriteTimeout

// writeLockRefused is the line strace writes for SQLite's attempt to take
// a WAL database's write lock while another connection holds it: a write
// lock on byte 120 of the -shm file (wal.c: WALINDEX_LOCK_OFFSET, plus
// WAL_WRITE_LOCK, 0), refused at once.
var writeLockRefused = regexp.MustCompile(`F_WRLCK, l_whence=SEEK_SET, l_start=120, l_len=1\}\) = -1 EAGAIN`)

// design.md, Restart, Graceful stop: "After limits.write_timeout from when
// the core accepts the stop, the remaining sockets are cut, including those
// with replies still pending. A connection whose DISCONNECT change is still
// at the store is the exception. It closes once the store confirms that
// change"; "It acts on a client's DISCONNECT by storing its session expiry
// change"; "The exit status is 0 after a stop requested by SIGINT or
// SIGTERM"; Flows, DISCONNECT: "The client may change its session expiry
// [...] The change is stored before the connection closes".
//
// An MQTT 5 client holding a durable session writes DISCONNECT with
// Session Expiry Interval awayExpiry, and keeps its connection open. The
// test holds the database's write lock, as another program on the file
// may, so the store cannot save that change; the broker runs under strace,
// which notes its fcntl calls on the -shm file, and SIGTERM goes once its
// store writer has met the lock: the core took the DISCONNECT and sent its
// change. Then:
//   - the connection is still open write_timeout and heldPast after
//     SIGTERM, nothing read on it, a DISCONNECT 0x8B included, and the
//     stored expiry is still the CONNECT's;
//   - once the lock is released, the connection closes in order;
//   - the broker exits 0, and the stored expiry is the DISCONNECT's.
func TestStopWaitsForADisconnectStillAtTheStore(t *testing.T) {
	t.Parallel()
	b := launchTracingLocks(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pending", CleanStart: true, Props: support.Durable})

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	if err := c.Send(&harness.Disconnect{Props: support.SessionExpiry(awayExpiry)}); err != nil {
		t.Fatal(err)
	}
	awaitLockMet(t, b.trace, before, "the DISCONNECT")
	b.terminate(t)

	nothingWithin(t, c, support.WriteTimeout+heldPast, "with the DISCONNECT's change still at the store, after SIGTERM")
	if got := storedExpiry(t, lock.conn, "pending"); got != support.Durable[0].Int {
		t.Fatalf("with the write lock held, the stored expiry is %d; want the CONNECT's, %d: the store could not have confirmed the change", got, support.Durable[0].Int)
	}
	lock.release(t)

	harness.ExpectClosed(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	if got := storedExpiry(t, support.OpenDB(t, b.DB), "pending"); got != awayExpiry {
		t.Errorf("after the stop, the stored expiry is %d; want the DISCONNECT's, %d", got, awayExpiry)
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop whose
// changes the stop stores (R4)"; "After limits.write_timeout from when the
// core accepts the stop, the remaining sockets are cut, including those with
// replies still pending"; "where the cutoff comes before the store confirms";
// design.md, Flows, Replies: PUBACK "sent only once the store confirms";
// Closing a connection: "at the deadline the socket is closed whatever is
// left, which may reset it".
//
// The test holds the database's write lock, as another program on the file
// may, while an MQTT 5 client publishes at QoS 1; the broker runs under
// strace, and SIGTERM goes once its store writer has met the lock. The
// PUBACK waits for the store, which cannot confirm, and the client's 0x8B
// for the PUBACK. The client reads once, with a deadline late past
// write_timeout after SIGTERM, and the read is judged by when it ended:
// nothing is read, neither the PUBACK nor DISCONNECT 0x8B, and the
// connection is closed by the cutoff, no sooner than write_timeout after
// SIGTERM. Once the lock is released, the broker exits 0.
func TestStopCutsAPublisherStillAtTheStore(t *testing.T) {
	b, c, lock := publisherAtTheStore(t, &harness.Options{})
	terminated := b.terminate(t)
	// terminated is stamped before SIGTERM is sent, and the core accepts the
	// stop only after it, so the cutoff cannot close the socket before
	// cutoff. The read's deadline lies late past it, and the instant the read
	// ends decides, not which of two near-simultaneous wake-ups wins.
	cutoff := terminated.Add(support.WriteTimeout)
	c.Timeout = time.Until(cutoff.Add(support.Late))
	p, err := c.Recv()
	ended := time.Now()
	switch {
	case err == nil:
		t.Errorf("%v after SIGTERM the client read %s %+v; want nothing, its connection closed by the cutoff, neither the PUBACK nor DISCONNECT 0x8B",
			ended.Sub(terminated), harness.Name(p.Type()), p)
	case harness.IsTimeout(err):
		t.Errorf("the connection is still open %v past write_timeout after SIGTERM; want it closed by the cutoff", support.Late)
	case !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET):
		t.Errorf("reading after SIGTERM: %v; want the socket closed by the cutoff, an end of stream or a reset", err)
	case ended.Before(cutoff):
		t.Errorf("the connection closed %v after SIGTERM, before write_timeout (%v); want it open, nothing read, until the cutoff", ended.Sub(terminated), support.WriteTimeout)
	}
	lock.release(t)
	_ = c.Close() // the broker has cut its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop
// whose changes the stop stores (R4)"; "After limits.write_timeout from when
// the core accepts the stop, the remaining sockets are cut, including those
// with replies still pending"; "The core waits until every store request it
// sent, read or write, is answered". Closing a connection: "A write that has
// not finished limits.write_timeout after its first socket-write attempt
// closes the connection as writer_full. Partial writes do not extend that
// deadline"; "When the core cuts a connection, the reason it selected is the
// closure's cause. The failed write or read the cut causes records no cause
// of its own". SUBSCRIBE: an append channel "Starts with the floor".
// Delivery: "The window is the lower of the client's Receive Maximum and
// limits.max_inflight"; "A message is in flight from its hand-off to the
// writer". Message memory: "An outgoing delivery reserves its encoded packet
// size before loading its payload"; "A delivery reservation ends when the
// writer reports completion or discard". DISCONNECT, Reasons Sagüin sends:
// "0x8B for server shutting down". metrics.md, saguin_disconnects_total:
// "the reason Sagüin selected for a closure, including a closure without a
// sendable DISCONNECT (design.md, DISCONNECT): [...] server_shutting_down";
// saguin_memory_in_transit_bytes{direction}: "outstanding reservations of
// each budget, incoming or outgoing (design.md, Message memory)", here
// outgoing; saguin_ready: "1 while listeners and bridges
// have started, the last snapshot, if any yet, succeeded and the broker is
// not stopping".
//
// A publisher stores a window's worth of records on an append channel,
// flood, each stopCutPayload bytes, and leaves. A client that takes nothing
// the broker writes until it reads (stalledReader) subscribes to them at
// QoS 0: once the first has begun on the socket and all of them hold their
// message memory, the broker's writer is in a write it cannot finish, begun
// before the stop, with the rest behind it. The client publishes at QoS 1
// while the test holds the database's write lock (publishAtTheStore): its
// PUBACK, and its 0x8B behind it, wait for the store. The broker is
// stopped. stopCutLead after SIGTERM, the client reads half the window,
// more than every buffer between the broker and it holds, and then nothing:
// the write in progress began after the stop, so its own write_timeout ends
// after the cutoff, and the cutoff cuts the connection while the writer is
// in it. Once a closure is counted, it is server_shutting_down. Once the
// lock is released, the broker exits 0.
func TestStopCutCountsServerShuttingDown(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{Operations: true, WriteTimeout: stopCutWriteTimeout, MaxInflight: stopCutWindow,
		MaxPacketSize: 2 * stopCutPayload, Channels: []harness.Channel{{Name: "flood", Type: "append", Filter: "stop/flood"}}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "cut-pub", CleanStart: true})
	flood := bytes.Repeat([]byte{'f'}, stopCutPayload)
	for range stopCutWindow {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "stop/flood", Payload: flood})
	}
	left := baselineClosures(t, b.Broker)
	support.Away(t, pub)
	closuresSince(t, b.Broker, left) // the publisher's own, counted before the stop's

	c, first := stalledReader(t, b.Addr, harness.V5)
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: "cut", CleanStart: true})
	support.Suback(t, c, nil, support.At(0, "stop/flood"), 0)
	if _, err := first(); err != nil {
		t.Fatalf("watchdog: the first delivery's header on the client: %v", err)
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		return testkit.Sample(t, b.Metrics(), inTransitOutgoing) >= stopCutWindow*stopCutPayload
	}) {
		t.Fatalf("watchdog: %s never held the window's %d deliveries", inTransitOutgoing, stopCutWindow)
	}
	lock := publishAtTheStore(t, b, c)
	before := baselineClosures(t, b.Broker)
	terminated := b.terminate(t)
	if !testkit.Within(harness.DefaultTimeout, func() bool { return testkit.Sample(t, b.Metrics(), "saguin_ready") == 0 }) {
		t.Fatal("watchdog: saguin_ready never read 0 after SIGTERM")
	}
	// The lead waits for no event: it only places the first socket-write
	// attempt of every write the client's reads then let begin that much
	// after the stop's beginning, which the cutoff follows.
	time.Sleep(time.Until(terminated.Add(stopCutLead)))
	for range stopCutWindow / 2 {
		if p := harness.Expect[*harness.Publish](t, c); len(p.Payload) != stopCutPayload {
			t.Fatalf("a delivery of %d bytes; want %d", len(p.Payload), stopCutPayload)
		}
	}
	if got, want := closuresSince(t, b.Broker, before), map[string]float64{disconnectSeries("server_shutting_down"): 1}; !maps.Equal(got, want) {
		t.Errorf("the connection the graceful-stop cutoff cut, its writer in a write begun after the stop, is counted as %v; want %v, the reason the broker selected", got, want)
	}
	lock.release(t)
	_ = c.Close() // the broker has cut its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// TestStopCutCountsServerShuttingDown's broker and client: its
// write_timeout; stopCutLead, how long after SIGTERM the client reads,
// within the write_timeout of the write begun before the stop; the
// window, limits.max_inflight; and each record's payload, so that half
// the window's bytes exceed the broker's send buffer and the client's
// receive buffer together.
const (
	stopCutWriteTimeout = 3 * time.Second
	stopCutLead         = 500 * time.Millisecond
	stopCutWindow       = 16
	stopCutPayload      = 2 << 20
)

// publisherAtTheStore launches a broker of the test's own under strace
// (launchTracingLocks) with o's keys, connects client "cut", and has it
// publish at the store (publishAtTheStore); it returns the broker, the
// client and the lock.
func publisherAtTheStore(t *testing.T, o *harness.Options) (*lockTraced, *harness.Client, *writeLock) {
	t.Helper()
	b := launchTracingLocks(t, o)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "cut", CleanStart: true})
	return b, c, publishAtTheStore(t, b, c)
}

// publishAtTheStore takes b's database's write lock (holdWriteLock), has c
// publish at QoS 1, and returns the lock once the store writer has met it:
// the PUBLISH's change is at the store, and the store cannot confirm it.
func publishAtTheStore(t *testing.T, b *lockTraced, c *harness.Client) *writeLock {
	t.Helper()
	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	support.Sent(t, c, &harness.Publish{QoS: 1, Topic: "stop/cut", Payload: []byte("x")})
	awaitLockMet(t, b.trace, before, "the PUBLISH")
	return lock
}

// lockTraced is a broker of the test's own that runs under strace
// (tracingLocks), which notes in the file trace its locks on its database's
// -shm file.
type lockTraced struct {
	*harness.Broker
	trace string
	pid   int // the broker's own process id: Signal would reach strace
}

// launchTracingLocks launches a broker of the test's own under strace, with
// o's keys, in a folder of its own, its pid_file there telling its own
// process id.
func launchTracingLocks(t *testing.T, o *harness.Options) *lockTraced {
	t.Helper()
	dir := t.TempDir()
	db, pidFile, trace := filepath.Join(dir, "saguin.db"), filepath.Join(dir, "saguin.pid"), filepath.Join(dir, "trace")
	own := *o // the caller's Options stay as they were
	own.Dir, own.FilePath, own.PIDFile, own.Under = dir, db, pidFile, tracingLocks(t, db+"-shm", trace)
	b := support.Launch(t, &own)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("pid_file holds %q: %v", data, err)
	}
	return &lockTraced{Broker: b, trace: trace, pid: pid}
}

// terminate sends SIGTERM to the broker itself, and returns when it was
// sent.
func (b *lockTraced) terminate(t *testing.T) time.Time {
	t.Helper()
	at := time.Now()
	if err := syscall.Kill(b.pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	return at
}

// tracingLocks returns the command, with its arguments, that a broker runs
// under (Options.Under) so that strace notes in the file trace every fcntl
// call, SQLite's locks, that it and its threads make on the file at path.
func tracingLocks(t *testing.T, path, trace string) []string {
	t.Helper()
	strace, err := exec.LookPath("strace")
	if err != nil {
		t.Fatalf("the broker's locks are traced by strace: %v", err)
	}
	return []string{
		strace, "--follow-forks", "--quiet=all", "--seccomp-bpf", "--kill-on-exit", "--output=" + trace,
		"--trace=fcntl", "--trace-path=" + path, "--",
	}
}

// writeLock is SQLite's write lock on a broker's database, held by a test
// as another program on the file may hold it: while it is held, the store
// can save nothing. A write waits for it within SQLite's own wait for the
// lock (go-sqlite3's busy timeout, 5 s by default), past which it fails, a
// store error (R8), so a test holds it well within that.
type writeLock struct {
	db   *sql.DB
	conn *sql.Conn // the connection holding the lock, in its open transaction
}

// holdWriteLock takes the write lock of the database at path, beginning an
// IMMEDIATE transaction on a connection of the test's own. A test that ends
// still holding it, failed before its release, gives it back, so a later
// test on the same database can take it.
func holdWriteLock(t *testing.T, path string) *writeLock {
	t.Helper()
	db := support.OpenDB(t, path)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("taking the database's write lock: %v", err)
	}
	// After a release, both fail on the closed connection: nothing is held.
	t.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`); _ = conn.Close() })
	return &writeLock{db: db, conn: conn}
}

// release gives the write lock back and closes the test's connection: a
// broker's clean stop folds its log into the file once no other connection
// holds it.
func (l *writeLock) release(t *testing.T) {
	t.Helper()
	if _, err := l.conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("releasing the database's write lock: %v", err)
	}
	_, _ = l.conn.Close(), l.db.Close()
}

// awaitLockMet waits until strace has noted in the file trace, past its
// first from bytes, the broker's store writer meeting the held write lock
// (writeLockRefused), failing the test where it has not within
// harness.DefaultTimeout of what the test sent, named by after.
func awaitLockMet(t *testing.T, trace string, from int, after string) {
	t.Helper()
	met := func() bool { return writeLockRefused.MatchString(traced(t, trace)[from:]) }
	if !testkit.Within(harness.DefaultTimeout, met) {
		t.Fatalf("the store writer never met the held write lock after %s; strace noted:\n%s", after, traced(t, trace)[from:])
	}
}

// traced returns what strace has noted in the file trace so far.
func traced(t *testing.T, trace string) string {
	t.Helper()
	b, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// storedExpiry reads the session expiry of client that q's database holds
// (storage.md, sessions: expiry "the session expiry granted, in seconds").
func storedExpiry(t *testing.T, q support.RowQuerier, client string) uint32 {
	t.Helper()
	var expiry uint32
	if err := q.QueryRowContext(context.Background(), `SELECT expiry FROM sessions WHERE client = ?`, client).Scan(&expiry); err != nil {
		t.Fatalf("reading the stored session expiry: %v", err)
	}
	return expiry
}
