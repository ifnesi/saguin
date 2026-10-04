package sessiontest_test

// Sessions that outlive their connection: what an expired one takes with
// it, who a shared group's member may be, and what one may hold while its
// client is away.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	mqttv3 "github.com/eclipse/paho.mqtt.golang"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// awaitSessionExpired waits until the substrate has dropped a client id's
// session, which its expiry sweep does on a one-second ticker.
func awaitSessionExpired(t testing.TB, h *harness, clientID string, within time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		if _, ok := h.Srv.Clients.Get(clientID); !ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the session for %q had not expired after %s, so nothing below is about "+
		"an expired member", clientID, within)
}

// groupTotal is how many deliveries a shared group's members received
// between them.
func groupTotal(members ...*client) int {
	n := 0
	for _, m := range members {
		n += m.Count()
	}
	return n
}

// RFC 0002 "Shared subscriptions" - a group is split across its members, so
// every publish reaches exactly one of them.
//
// **A member whose session has expired is not a member.** The expiry sweep
// dropped the client from the substrate's table and left its subscriptions
// in the topic index, so the group kept selecting it and the substrate
// found nobody to hand the message to. Counted across the group, because a
// member that received nothing proves nothing on its own.
func TestAnExpiredMemberLeavesItsSharedGroup(t *testing.T) {
	h := start(t)

	a := connect(t, h, "member-a", true, false)
	b := connect(t, h, "member-b", true, false)
	// A one-second session, so the sweep takes it while the test waits.
	c := dial(t, h, "member-c", true, false, 0, 1, 0)
	for _, m := range []*client{a, b, c} {
		if sa := m.Sub(t, "$share/g/fleet/+", 1); sa.Reasons[0] > 1 {
			t.Fatalf("a member was refused the group: 0x%02x", sa.Reasons[0])
		}
	}
	// A declaration on the member that expires, so there is something of
	// saguin's own to leak beside the substrate's subscription.
	if sa := c.SubSliced(t, "fleet-sliced/+", 2, 0); sa.Reasons[0] > 1 {
		t.Fatalf("the declaring subscription was refused: 0x%02x", sa.Reasons[0])
	}
	// Read until it is there rather than after a pause: the declaration is
	// the broker's own state, and the SUBACK says nothing about when it lands.
	waitUntil(5*time.Second, func() bool { return len(h.B.DeclaredSlices("member-c")) == 1 })
	if got := h.B.DeclaredSlices("member-c"); len(got) != 1 {
		t.Fatalf("the broker holds %v for a member that declared one slice: this test "+
			"cannot show a leak it never created", got)
	}

	gone(t, c)
	awaitSessionExpired(t, h, "member-c", 5*time.Second)

	if got := h.B.DeclaredSlices("member-c"); got != nil {
		t.Errorf("the broker still holds %v for a session that has expired", got)
	}

	const n = 60
	p := connect(t, h, "producer", true, false)
	for i := range n {
		p.Pub(t, fmt.Sprintf("fleet/%d", i), "x")
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if groupTotal(a, b) >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := groupTotal(a, b); got != n {
		t.Errorf("the group's two live members received %d of %d publishes between "+
			"them: the rest went to a member whose session had expired", got, n)
	}
}

// MQTT 5 section 4.9: a server does not send more QoS 1 and 2 publishes than
// the client's Receive Maximum before they are acknowledged [MQTT-3.3.4-9].
//
// **A resumed session was the one place that was not held to it.** Its
// queued messages were all written back to back at the CONNECT, whatever the
// window. And a message written once the window freed left the session's
// in-flight set, so its acknowledgement returned no quota and the client
// stopped being sent anything at about twice its window.
//
// Proved from what the broker sent: nothing is acknowledged until the first
// burst is counted, and then one at a time.
func TestAResumedSessionIsSentNoMoreThanItsReceiveMaximum(t *testing.T) {
	const window, backlog = 2, 10
	h := start(t)

	first, _ := windowDial(t, h.Addr, "slow-reader", false, window)
	first.Subscribe("backlog/+", 1)
	first.Close()
	sessionGone(t, h, "slow-reader")

	p := connect(t, h, "producer", true, false)
	for i := range backlog {
		p.Pub(t, fmt.Sprintf("backlog/%d", i), fmt.Sprintf("m-%d", i))
	}
	// Every publish owed to the session before it resumes, in the broadcast
	// log (RFC 0003 "Broadcast"), so the resume has the whole backlog to send.
	awaitOwed(t, operationsAt(t, h), "the backlog, before the resume", backlog)

	w, ca := windowDial(t, h.Addr, "slow-reader", false, window)
	if !ca.SessionPresent {
		t.Fatal("the session was not resumed")
	}

	seen := map[string]int{}
	var unacked []uint16
	readBurst := func() {
		for {
			cp, err := w.Read(700 * time.Millisecond)
			if err != nil {
				return
			}
			if pub, ok := cp.Content.(*pahopackets.Publish); ok {
				seen[string(pub.Payload)]++
				unacked = append(unacked, pub.PacketID)
				if len(unacked) > window {
					t.Fatalf("the broker had %d publishes unacknowledged with a Receive Maximum "+
						"of %d", len(unacked), window)
				}
			}
		}
	}
	readBurst()
	if len(unacked) == 0 {
		t.Fatal("the resumed session was sent nothing, so this proves nothing about a window")
	}
	for deadline := time.Now().Add(10 * time.Second); len(seen) < backlog && time.Now().Before(deadline); {
		if len(unacked) == 0 {
			readBurst()
			if len(unacked) == 0 {
				break
			}
		}
		w.Puback(unacked[0])
		unacked = unacked[1:]
		readBurst()
	}
	if len(seen) != backlog {
		t.Errorf("the resumed session received %d of %d queued messages, then nothing: %v",
			len(seen), backlog, seen)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Errorf("%s arrived %d times on one connection", payload, n)
		}
	}
}

// RFC 0003 "Exactly once": the record is written when the PUBREL arrives,
// and a shared subscriber is served records as they are written.
//
// **So a held publish reaches nobody until its release.** A shared group
// over an append or latest channel is fed by the substrate's fan-out, and
// that ran at the PUBLISH: the member was handed a message that was not yet
// a record, and would never become one if the exchange was abandoned.
func TestAHeldPublishReachesNoSharedMemberBeforeItsRelease(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		for _, topic := range []string{"events/held/1", "state/held/1"} {
			t.Run(topic, func(t *testing.T) {
				member := connect(t, h, "member-"+topic, true, false)
				if sa := member.Sub(t, "$share/g/"+topic, 1); sa.Reasons[0] > 1 {
					t.Fatalf("the member was refused: 0x%02x", sa.Reasons[0])
				}

				w, _ := qos2Dial(t, h.Addr, "publisher-"+topic, true, 0)
				defer w.Close()
				w.Publish(5, topic, "held", false)
				if rc := w.Pubrec(5); rc != 0 {
					t.Fatalf("the receipt carried 0x%02X, want success", rc)
				}
				// **An end marker rather than a quiet period.** A record
				// published after the PUBREC is served after anything the
				// PUBLISH fanned out, so the marker arriving first and alone
				// says the held publish reached nobody.
				marker := connect(t, h, "marker-"+topic, true, false)
				next := func(want, why string) {
					t.Helper()
					r, ok := member.Await(t, 3*time.Second)
					if !ok || r.Payload != want {
						t.Fatalf("the shared member was served %q (%v), want %q: %s",
							r.Payload, ok, want, why)
					}
				}
				marker.Pub(t, topic, "marker-1")
				next("marker-1", "it is not a record before its release, and an exchange "+
					"never finished never becomes one")

				w.Pubrel(5)
				if rc := w.Pubcomp(5); rc != 0 {
					t.Fatalf("the completion carried 0x%02X, want success", rc)
				}
				next("held", "the record its release wrote")
				marker.Pub(t, topic, "marker-2")
				next("marker-2", "anything before it is the record served a second time")
			})
		}
	})
}

// RFC 0003 "Last Will": a Will is delivered through the ordinary publish
// path, at whatever QoS the client armed it with.
//
// **An exactly-once Will is held for a release nobody sends.** The publish
// path holds a QoS 2 publish until its PUBREL, and a Will is published in
// process by saguin, which sends none. Two at once collide as well: both are
// held under the Will client's one identifier.
func TestAnExactlyOnceWillIsStored(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		first, code := connectWithWill(t, h, "dying-1", "events/wills/1", false, 0, 2)
		if code != 0 {
			t.Fatalf("a QoS 2 Will was refused 0x%02X on a broker offering exactly-once", code)
		}
		second, code := connectWithWill(t, h, "dying-2", "events/wills/2", false, 0, 2)
		if code != 0 {
			t.Fatalf("the second QoS 2 Will was refused 0x%02X", code)
		}
		kill(t, first)
		kill(t, second)

		// Read until both are there, each read to its end marker, so a third
		// copy is counted too; the deadline bounds a broker that stores fewer.
		n := 0
		for i, deadline := 0, time.Now().Add(5*time.Second); n < 2 && time.Now().Before(deadline); i++ {
			n = inChannel(t, h, fmt.Sprintf("auditor-%d", i), "i-died")
		}
		if n != 2 {
			t.Errorf("the channel holds %d of the 2 exactly-once Wills that fired", n)
		}
	})
}

// RFC 0002 `broker.qos2`: "A message nobody releases is not a record. It
// leaves when its session ends."
//
// **A clean start ends the session it replaces.** The unreleased publishes
// of the discarded session stayed held, where they counted against the
// client's `max_inflight_per_client` until `expires_after` - so the new
// session was refused publishes it had never left unfinished.
func TestACleanStartDropsTheHeldPublishesOfTheSessionItDiscards(t *testing.T) {
	brokertest.QoS2Inflight = 1
	t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
	eachProvider(t, func(t *testing.T, h *harness) {
		old, _ := qos2Dial(t, h.Addr, "device", false, 300)
		old.Publish(1, "events/clean/1", "abandoned", false)
		if rc := old.Pubrec(1); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}
		old.Close()
		sessionGone(t, h, "device")

		fresh, ca := qos2Dial(t, h.Addr, "device", true, 300)
		defer fresh.Close()
		if ca.SessionPresent {
			t.Fatal("a clean start was told a session was present")
		}
		fresh.Publish(2, "events/clean/2", "new", false)
		if rc := fresh.Pubrec(2); rc != 0 {
			t.Errorf("the first exactly-once publish of a fresh session was refused 0x%02X: the "+
				"discarded session's unreleased publish still counts against the allowance", rc)
		}
	})
}

// RFC 0002 "Shared subscriptions": a delivery goes to a member that can take
// it now - a session that is connected, with room for it.
//
// **An offline member was picked as often as a live one**, and a connected
// member that had stopped acknowledging kept being picked after its window
// was full, so both piled the group's messages into sessions nobody was
// reading. Counted across the group: what the live members received, plus
// what the deaf member holds, is every publish.
func TestASharedGroupSkipsAMemberThatCannotTakeADelivery(t *testing.T) {
	const n = 40
	h := start(t)

	a := connect(t, h, "member-a", true, false)
	b := connect(t, h, "member-b", true, false)
	off := connect(t, h, "member-off", false, false)
	for _, m := range []*client{a, b, off} {
		if sa := m.Sub(t, "$share/g/fleet/+", 1); sa.Reasons[0] > 1 {
			t.Fatalf("a member was refused the group: 0x%02x", sa.Reasons[0])
		}
	}
	gone(t, off)
	if _, ok := h.Srv.Clients.Get("member-off"); !ok {
		t.Fatal("the offline member's session did not outlive its connection, so it " +
			"cannot show what an offline member is handed")
	}

	// Connected, and never reading: one delivery fills its window.
	deaf, _ := windowDial(t, h.Addr, "member-deaf", true, 1)
	deaf.Subscribe("$share/g/fleet/+", 1)

	p := connect(t, h, "producer", true, false)
	for i := range n {
		p.Pub(t, fmt.Sprintf("fleet/%d", i), fmt.Sprintf("m-%d", i))
	}

	held := func(id string) int {
		cl, ok := h.Srv.Clients.Get(id)
		if !ok {
			t.Fatalf("the session for %s is gone", id)
		}
		return cl.State.Inflight.Len()
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if groupTotal(a, b)+held("member-deaf")+held("member-off") >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// **No pause before the checks.** The sum counts each publish once it is
	// placed anywhere - received by a live member, or held by the deaf one or
	// the offline one - so at n nothing is left to place.

	if got := held("member-off"); got != 0 {
		t.Errorf("the offline member's session was handed %d of the group's %d publishes", got, n)
	}
	if got := held("member-deaf"); got > 1 {
		t.Errorf("a member with a window of 1 that acknowledged nothing holds %d deliveries", got)
	}
	if got := groupTotal(a, b) + held("member-deaf"); got != n {
		t.Errorf("the group received %d of %d publishes: the live members %d and %d, the "+
			"deaf member %d, the offline member %d", got, n, a.Count(), b.Count(),
			held("member-deaf"), held("member-off"))
	}
	if a.Count() == 0 || b.Count() == 0 {
		t.Errorf("the group was not split: member-a %d, member-b %d", a.Count(), b.Count())
	}
}

// RFC 0003 "Broadcast": a shared group with no member that can take a
// delivery, and members whose sessions outlive their connections, holds it
// for them - a broadcast and a channel's record alike, since a group over a
// channel keeps no position on it and is served from its backlog as a
// broadcast group is. Nothing is dropped as having no member, and no offline
// member is handed anything.
func TestASharedGroupWithNobodyToTakeItHoldsBroadcastAndChannelRecords(t *testing.T) {
	const n = 10
	h := start(t)
	ops := operationsAt(t, h)

	first := connect(t, h, "away-1", false, false)
	second := connect(t, h, "away-2", false, false)
	for _, m := range []*client{first, second} {
		m.Sub(t, "$share/g/fleet/+", 1)
		m.Sub(t, "$share/g/events/fleet/+", 1)
	}
	gone(t, first)
	gone(t, second)

	const series = `saguin_session_deliveries_dropped_total{cause="no_shared_member"}`
	const heldSeries = `saguin_shares_held_total`
	before := scrapeGauges(t, ops)[series]
	heldBefore := scrapeGauges(t, ops)[heldSeries]

	p := connect(t, h, "producer", true, false)
	for i := range n {
		p.Pub(t, fmt.Sprintf("fleet/%d", i), "x")
		p.Pub(t, fmt.Sprintf("events/fleet/%d", i), "x")
	}
	settle(t, p)
	// Each delivery is held or dropped, so once all are held none can be
	// dropped: read until they are, rather than after a pause.
	waitUntil(5*time.Second, func() bool { return scrapeGauges(t, ops)[heldSeries]-heldBefore >= 2*n })

	if got := scrapeGauges(t, ops)[series] - before; got != 0 {
		t.Errorf("%s moved by %v: a group whose members are coming back holds what it is owed", series, got)
	}
	if got := scrapeGauges(t, ops)[heldSeries] - heldBefore; got != 2*n {
		t.Errorf("%s moved by %v for %d deliveries to groups whose members' sessions outlive "+
			"their connections, want each held", heldSeries, got, 2*n)
	}
	for _, group := range []string{"$share/g/fleet/+", "$share/g/events/fleet/+"} {
		backlog, err := brokertest.HarnessShares.Backlog(group)
		if err != nil {
			t.Fatal(err)
		}
		if len(backlog) != n {
			t.Errorf("%s holds %d deliveries, want %d", group, len(backlog), n)
		}
	}
	for _, id := range []string{"away-1", "away-2"} {
		cl, ok := h.Srv.Clients.Get(id)
		if !ok {
			t.Fatalf("the session for %s is gone, so nothing here is about an offline member", id)
		}
		if held := cl.State.Inflight.Len(); held != 0 {
			t.Errorf("offline member %s was handed %d deliveries", id, held)
		}
	}
}

// RFC 0002 `limits.session_queue_bytes`: a session holds no more than its
// bound of deliveries it has not acknowledged. Full means the oldest goes,
// and the publisher is never refused for it.
//
// **The bound was a count of 8,192 and nothing else**, so a session nobody
// was reading held that many messages of any size. Proved from the wire: the
// session is resumed holding only the newest, in order, and the counter moved
// by exactly the ones it gave up.
func TestAnOfflineSessionKeepsTheNewestWithinItsBound(t *testing.T) {
	const published = 30
	// Each message here counts about a kilobyte - its payload, topic and
	// message id, and the 80 bytes of its entry on the session's list (RFC
	// 0002) - so eight is room for seven and the bound is reached well inside
	// the backlog.
	pad := strings.Repeat("x", 1000)
	brokertest.SessionQueueBytes = 8 << 10
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)
	ops := operationsAt(t, h)

	away, _ := windowDial(t, h.Addr, "away", false, 100)
	away.Subscribe("backlog/+", 1)
	away.Close()
	sessionGone(t, h, "away")

	const series = `saguin_session_deliveries_dropped_total{cause="session_queue_full"}`
	before := scrapeGauges(t, ops)[series]

	p := connect(t, h, "producer", true, false)
	for i := range published {
		pa, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: fmt.Sprintf("backlog/%d", i), QoS: 1, Payload: []byte(fmt.Sprintf("m-%02d %s", i, pad))})
		if err != nil || pa.ReasonCode != 0 {
			t.Fatalf("publish %d was not accepted: %v (reason %v) - the publisher is never "+
				"refused for a session's bound", i, err, pa)
		}
	}
	settle(t, p)

	// What the session holds is what it is owed from the broadcast log (RFC
	// 0005 saguin_session_queue_messages and _bytes).
	kept := int(metricValue(t, ops, "saguin_session_queue_messages"))
	bytes := metricValue(t, ops, "saguin_session_queue_bytes")
	if kept == 0 || kept >= published {
		t.Fatalf("the session holds %d of %d messages with an 8KiB bound", kept, published)
	}
	if bytes > 8<<10 {
		t.Errorf("the session holds %v bytes against a bound of %d", bytes, 8<<10)
	}
	after := scrapeGauges(t, ops)[series]
	if got := int(after - before); got != published-kept {
		t.Errorf("%s moved by %d, and the session gave up %d", series, got, published-kept)
	}
	// And the log let them go: it holds what the session holds, the newest,
	// and nothing it gave up, since nobody else is owed those.
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		held := logged(t)
		newest := len(held) == kept
		for i := 0; newest && i < len(held); i++ {
			newest = strings.HasPrefix(string(held[i].Payload), fmt.Sprintf("m-%02d ", published-kept+i))
		}
		if newest {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the log holds %d messages, want the newest %d the session holds", len(held), kept)
		}
	}

	w, ca := windowDial(t, h.Addr, "away", false, 100)
	if !ca.SessionPresent {
		t.Fatal("the session was not resumed")
	}
	var got []string
	for {
		cp, err := w.Read(700 * time.Millisecond)
		if err != nil {
			break
		}
		if pub, ok := cp.Content.(*pahopackets.Publish); ok {
			got = append(got, strings.Fields(string(pub.Payload))[0])
			w.Puback(pub.PacketID)
		}
	}
	var want []string
	for i := published - kept; i < published; i++ {
		want = append(want, fmt.Sprintf("m-%02d", i))
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the resumed session received %v, want the newest %d in order: %v", got, kept, want)
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - a Will the client's
// roles deny refuses the connection `0x87`, and a CONNECT without one is
// admitted.
func TestADeniedWillRefusesTheConnection(t *testing.T) {
	h := start(t)
	denyFeatures(t, h, "    - broker: features\n      deny: [will]\n")

	conn, code := connectWithWill(t, h, "dying", "events/wills/1", false, 0, 1)
	defer conn.Close()
	if code != 0x87 {
		t.Errorf("a CONNECT carrying a denied Will was answered 0x%02X, want 0x87", code)
	}

	// A 3.1.1 client is told in the one code its CONNACK carries for it.
	legacy, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer legacy.Close()
	var vh []byte
	vh = append(vh, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02|0x04|0x08, 0x00, 0x3c)
	for _, s := range []string{"dying-311", "events/wills/2", "i-died"} {
		vh = append(vh, byte(len(s)>>8), byte(len(s)))
		vh = append(vh, s...)
	}
	if _, err := legacy.Write(mqttPacket(0x10, vh)); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = legacy.SetReadDeadline(time.Now().Add(3 * time.Second))
	ack := make([]byte, 4)
	if _, err := io.ReadFull(legacy, ack); err != nil {
		t.Fatalf("no CONNACK for the 3.1.1 client: %v", err)
	}
	if ack[3] != 0x05 {
		t.Errorf("a 3.1.1 CONNECT carrying a denied Will was answered 0x%02X, want 0x05", ack[3])
	}

	// The control: the same client without a Will connects.
	plain := connect(t, h, "no-will", true, false)
	plain.Pub(t, "events/fine/1", "ok")
}

// A persistent session the client's roles deny is accepted, told a Session
// Expiry Interval of 0 in its CONNACK, and ends with the connection.
func TestADeniedPersistentSessionEndsWithItsConnection(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied=%v", denied), func(t *testing.T) {
			h := start(t)
			rules := ""
			if denied {
				rules = "    - broker: features\n      deny: [persistent]\n"
			}
			denyFeatures(t, h, rules)

			w, ca := windowDial(t, h.Addr, "keeper", false, 10)
			told := ca.Properties != nil && ca.Properties.SessionExpiryInterval != nil &&
				*ca.Properties.SessionExpiryInterval == 0
			if told != denied {
				t.Errorf("the CONNACK carried Session Expiry Interval 0: %v, want %v", told, denied)
			}
			w.Subscribe("keeper/+", 1)
			w.Close()
			sessionGone(t, h, "keeper")
			// A session that ends with its connection leaves the client table
			// after the disconnect hooks, so its going is waited for; one kept
			// is there already, and stays.
			if denied {
				waitUntil(5*time.Second, func() bool { _, ok := h.Srv.Clients.Get("keeper"); return !ok })
			}

			_, held := h.Srv.Clients.Get("keeper")
			if held == denied {
				t.Errorf("the session outlived its connection: %v, want %v", held, !denied)
			}
			_, again := windowDial(t, h.Addr, "keeper", false, 10)
			if again.SessionPresent == denied {
				t.Errorf("the next connection was told Session Present %v", again.SessionPresent)
			}
		})
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - `retained` is refused
// on a broadcast topic, with the 0x9A MQTT gives a broker without retained
// messages, and nowhere else.
//
// **The harness keeps a retained store**, so a refusal the denied client meets
// is the denial's and not the store's absence: the undenied run is the control
// that proves the store is there to be refused.
func TestADeniedRetainedValueIsRefusedOnBroadcastOnly(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied=%v", denied), func(t *testing.T) {
			h := startRetaining(t, 0)
			rules := ""
			if denied {
				rules = "    - broker: features\n      deny: [retained]\n"
			}
			denyFeatures(t, h, rules)

			// **Retain Available stays 1**, denied or not: the flag is still
			// honoured on a channel's topic, and a 0 would make every retained
			// publish a Protocol Error on every topic (MQTT-3.2.2-14).
			_, ca := windowDial(t, h.Addr, "asks", true, 10)
			if ra := ca.Properties.RetainAvailable; ra == nil || *ra != 1 {
				t.Errorf("the CONNACK carried Retain Available %v, want 1", ra)
			}

			c := connect(t, h, "retainer", true, false)
			// A latest channel's topic: the channel is the store, and the flag
			// keeps nothing extra there.
			pr, err := c.C.Publish(context.Background(), &paho.Publish{
				Topic: "state/door", QoS: 1, Retain: true, Payload: []byte("shut"),
			})
			if err != nil || pr.ReasonCode != 0 {
				t.Fatalf("a retained publish to a latest channel was answered %v, %v", pr, err)
			}

			pr, err = c.C.Publish(context.Background(), &paho.Publish{
				Topic: "loose/lamp", QoS: 1, Retain: true, Payload: []byte("on"),
			})
			select {
			case d := <-c.Gone:
				if !denied {
					t.Errorf("an undenied retained broadcast publish was disconnected 0x%02X",
						d.ReasonCode)
				} else if d.ReasonCode != 0x9A {
					t.Errorf("a denied retained broadcast publish was disconnected 0x%02X, want 0x9A",
						d.ReasonCode)
				}
			case <-time.After(time.Second):
				if denied {
					t.Errorf("a denied retained broadcast publish was not disconnected: %v, %v", pr, err)
				} else if err != nil || pr.ReasonCode != 0 {
					t.Errorf("an undenied retained broadcast publish was answered %v, %v", pr, err)
				}
			}

			// A retained Will: refused only where it aims at broadcast.
			for _, tc := range []struct {
				id, topic string
				broadcast bool
			}{
				{"will-loose", "loose/wills/1", true},
				{"will-events", "events/wills/1", false},
			} {
				conn, code := connectWithWill(t, h, tc.id, tc.topic, true, 0, 1)
				_ = conn.Close()
				want := byte(0x00)
				if denied && tc.broadcast {
					want = 0x9A
				}
				if code != want {
					t.Errorf("a retained Will on %s was answered 0x%02X, want 0x%02X", tc.topic, code, want)
				}
			}

			if !denied {
				return
			}
			// A 3.1.1 client is told in the one code its CONNACK carries for it.
			legacy, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer legacy.Close()
			var vh []byte
			vh = append(vh, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02|0x04|0x08|0x20, 0x00, 0x3c)
			for _, s := range []string{"will-311", "loose/wills/2", "i-died"} {
				vh = append(vh, byte(len(s)>>8), byte(len(s)))
				vh = append(vh, s...)
			}
			if _, err := legacy.Write(mqttPacket(0x10, vh)); err != nil {
				t.Fatalf("write connect: %v", err)
			}
			_ = legacy.SetReadDeadline(time.Now().Add(3 * time.Second))
			ack := make([]byte, 4)
			if _, err := io.ReadFull(legacy, ack); err != nil {
				t.Fatalf("no CONNACK for the 3.1.1 client: %v", err)
			}
			if ack[3] != 0x05 {
				t.Errorf("a 3.1.1 CONNECT carrying a denied retained Will was answered 0x%02X, want 0x05",
					ack[3])
			}
		})
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - a denial is asked of
// the client that connects, never of saguin's own Will client, which publishes
// every Will on its client's behalf and carries no user name.
//
// **The case is a `"*"` entry denying `retained`**, which matches that empty
// name, beside a more exact entry that does not. `device-1` is allowed its
// retained Will at CONNECT, dies, and the Will must be kept: a subscriber that
// arrives afterwards can only be sent it by the retained store, with the
// RETAIN flag set.
func TestARetainedDenialNeverReachesAWillFiredOnAClientsBehalf(t *testing.T) {
	h := startRetaining(t, 0)
	acl := filepath.Join(t.TempDir(), "acl.yaml")
	body := "roles:\n  everyone:\n    - topic: \"#\"\n      allow: [write, read]\n" +
		"  noretain:\n    - broker: features\n      deny: [retained]\n" +
		"users:\n  \"*\": [everyone, noretain]\n  device-1: [everyone]\n"
	if err := os.WriteFile(acl, []byte(body), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	f, err := authz.Load(acl, h.B.Registry(), 0)
	if err != nil {
		t.Fatalf("load acl: %v", err)
	}
	h.B.Authorize(authz.New(f, h.B.Registry()))

	// CONNECT by hand, because the harness's client carries no Will: MQTT 5,
	// clean start, a retained QoS 1 Will, and a user name with its password -
	// a name nothing checks is not the identity device-1's entry is about.
	password := h.Authenticate(t, "device-1")
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	const topic = "loose/wills/device-1"
	flags := byte(0x80 | 0x40 | 0x20 | 0x08 | 0x04 | 0x02)
	vh := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, flags, 0x00, 0x3c, 0x00}
	field := func(s string) {
		vh = append(vh, byte(len(s)>>8), byte(len(s)))
		vh = append(vh, s...)
	}
	field("device-1")
	vh = append(vh, 0x00) // no Will properties
	field(topic)
	field("i-died")
	field("device-1")
	field(password)
	if _, err := conn.Write(mqttPacket(0x10, vh)); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	fh := make([]byte, 2)
	if _, err := io.ReadFull(conn, fh); err != nil {
		t.Fatalf("no CONNACK: %v", err)
	}
	rest := make([]byte, int(fh[1]))
	if _, err := io.ReadFull(conn, rest); err != nil || len(rest) < 2 {
		t.Fatalf("short CONNACK: % 02x, %v", rest, err)
	}
	if rest[1] != 0x00 {
		t.Fatalf("device-1's retained Will was refused 0x%02X, so its own entry is being denied "+
			"and this test proves nothing about the Will client", rest[1])
	}

	// Gone without a DISCONNECT, which is what fires a Will - and published,
	// counted once the publish path took it, before a reader arrives: one
	// subscribed sooner is sent it live, without RETAIN.
	const published = `saguin_wills_published_total{cause="immediate"}`
	ops := operationsAt(t, h)
	_ = conn.Close()
	sessionGone(t, h, "device-1")
	// A Will refused is never counted, and the reader below says so.
	waitUntil(5*time.Second, func() bool { return scrapeGauges(t, ops)[published] == 1 })

	sub := connect(t, h, "late-reader", true, false)
	sub.Sub(t, "loose/wills/#", 1)
	r, ok := sub.Await(t, 2*time.Second)
	if !ok {
		t.Fatal("a subscriber arriving after the Will fired was sent nothing: the retained Will " +
			"device-1 was allowed was refused when saguin's Will client published it")
	}
	if r.Topic != topic || r.Payload != "i-died" || !r.Retain {
		t.Errorf("the late subscriber was sent %q = %q with RETAIN %v, want the stored Will",
			r.Topic, r.Payload, r.Retain)
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - a client denied
// `share` is told at CONNECT, Shared Subscription Available 0, and a SUBSCRIBE
// asking for one anyway is a Protocol Error the connection does not survive,
// `0x9E` (MQTT 5 section 3.2.2.3.13).
//
// **Read off the wire**, so what is asserted is what the broker sent rather
// than a client library's reading of a closed socket.
func TestADeniedShareIsToldAtConnectAndDisconnectedOnSubscribe(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied=%v", denied), func(t *testing.T) {
			h := start(t)
			rules := ""
			if denied {
				rules = "    - broker: features\n      deny: [share]\n"
			}
			denyFeatures(t, h, rules)

			w, ca := windowDial(t, h.Addr, "sharer", true, 10)
			told := ca.Properties.SharedSubAvailable != nil && *ca.Properties.SharedSubAvailable == 0
			if told != denied {
				t.Errorf("the CONNACK said Shared Subscription Available 0: %v, want %v", told, denied)
			}

			// A shared filter beside an ordinary one: the denial takes the
			// connection, not the one filter.
			cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
			s := cp.Content.(*pahopackets.Subscribe)
			s.PacketID = 1
			s.Subscriptions = []pahopackets.SubOptions{
				{Topic: "$share/g/events/#", QoS: 1},
				{Topic: "events/#", QoS: 1},
			}
			if _, err := cp.WriteTo(w.C); err != nil {
				t.Fatalf("write SUBSCRIBE: %v", err)
			}
			switch p := w.Next(3 * time.Second).Content.(type) {
			case *pahopackets.Disconnect:
				if !denied || p.ReasonCode != 0x9E {
					t.Errorf("the SUBSCRIBE was answered DISCONNECT 0x%02X with denied=%v; a denial "+
						"is answered 0x9E and nothing else is", p.ReasonCode, denied)
				}
			case *pahopackets.Suback:
				if denied {
					t.Errorf("a denied shared subscription was answered SUBACK %v, want DISCONNECT 0x9E",
						p.Reasons)
				}
				for i, r := range p.Reasons {
					if r > 2 {
						t.Errorf("filter %d was refused 0x%02X on a client nothing denies", i, r)
					}
				}
			default:
				t.Errorf("the SUBSCRIBE was answered %T", p)
			}

			// Only shared subscriptions go: an ordinary one is still granted.
			plain, _ := windowDial(t, h.Addr, "plain", true, 10)
			plain.Subscribe("events/#", 1)
		})
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - a client denied
// `qos2` is told Maximum QoS 1 at CONNECT, a SUBSCRIBE asking for QoS 2 is
// granted 1, and a CONNECT carrying a QoS 2 Will is refused `0x9B`, `0x05` to
// a 3.1.1 client. The publish refusal, and deliveries held to the grant, are
// TestAPublishAboveTheAdvertisedMaximumQoSIsRefused and
// TestNothingIsDeliveredAboveTheAdvertisedMaximumQos on the same denial.
//
// **Read off the wire**, and run undenied as the control, so every answer
// the denied client gets is shown to be the denial's rather than the
// broker's.
func TestADeniedQoS2IsToldMaximumQoS1AndGrantedOne(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied=%v", denied), func(t *testing.T) {
			h := start(t)
			rules := ""
			if denied {
				rules = "    - broker: features\n      deny: [qos2]\n"
			}
			denyFeatures(t, h, rules)
			ceiling := byte(2)
			if denied {
				ceiling = 1
			}

			w, ca := windowDial(t, h.Addr, "exact", true, 10)
			told := byte(2) // absent is 2 (MQTT 5 section 3.2.2.3.4)
			if ca.Properties.MaximumQOS != nil {
				told = *ca.Properties.MaximumQOS
			}
			if told != ceiling {
				t.Errorf("the CONNACK said Maximum QoS %d with denied=%v, want %d", told, denied, ceiling)
			}

			cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
			s := cp.Content.(*pahopackets.Subscribe)
			s.PacketID = 1
			s.Subscriptions = []pahopackets.SubOptions{{Topic: "events/#", QoS: 2}}
			if _, err := cp.WriteTo(w.C); err != nil {
				t.Fatalf("write SUBSCRIBE: %v", err)
			}
			sa, ok := w.Next(3 * time.Second).Content.(*pahopackets.Suback)
			if !ok || len(sa.Reasons) != 1 {
				t.Fatalf("the SUBSCRIBE was not answered with one SUBACK code: %+v", sa)
			}
			if sa.Reasons[0] != ceiling {
				t.Errorf("a QoS 2 subscription was granted 0x%02X with denied=%v, want 0x%02X",
					sa.Reasons[0], denied, ceiling)
			}

			wantWill := byte(0x00)
			if denied {
				wantWill = 0x9B
			}
			conn, code := connectWithWill(t, h, "dying", "events/wills/1", false, 0, 2)
			conn.Close()
			if code != wantWill {
				t.Errorf("a CONNECT carrying a QoS 2 Will was answered 0x%02X with denied=%v, want 0x%02X",
					code, denied, wantWill)
			}

			// A 3.1.1 client is told in the one code its CONNACK carries for it.
			legacy, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer legacy.Close()
			var vh []byte
			vh = append(vh, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02|0x04|(2<<3), 0x00, 0x3c)
			for _, s := range []string{"dying-311", "events/wills/2", "i-died"} {
				vh = append(vh, byte(len(s)>>8), byte(len(s)))
				vh = append(vh, s...)
			}
			if _, err := legacy.Write(mqttPacket(0x10, vh)); err != nil {
				t.Fatalf("write connect: %v", err)
			}
			_ = legacy.SetReadDeadline(time.Now().Add(3 * time.Second))
			ack := make([]byte, 4)
			if _, err := io.ReadFull(legacy, ack); err != nil {
				t.Fatalf("no CONNACK for the 3.1.1 client: %v", err)
			}
			want311 := byte(0x00)
			if denied {
				want311 = 0x05
			}
			if ack[3] != want311 {
				t.Errorf("a 3.1.1 CONNECT carrying a QoS 2 Will was answered 0x%02X with denied=%v, want 0x%02X",
					ack[3], denied, want311)
			}
		})
	}
}

// aliasDial is windowDial for a client that allows the broker topic aliases,
// which is the one thing an outbound alias needs from a subscriber.
func aliasDial(t *testing.T, addr, id string, clean bool, rxMax uint16) (*qos2Wire, *pahopackets.Connack) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	w := &qos2Wire{T: t, C: c}
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	conn := cp.Content.(*pahopackets.Connect)
	conn.ClientID, conn.CleanStart, conn.KeepAlive = id, clean, 0
	expiry, aliases := uint32(300), uint16(16)
	conn.Properties.SessionExpiryInterval = &expiry
	conn.Properties.ReceiveMaximum = &rxMax
	conn.Properties.TopicAliasMaximum = &aliases
	if _, err := cp.WriteTo(c); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	ca, ok := w.Next(5 * time.Second).Content.(*pahopackets.Connack)
	if !ok || ca.ReasonCode != 0 {
		t.Fatalf("the CONNECT was not accepted: %+v", ca)
	}
	return w, ca
}

// **No topic alias is sent to a subscriber**, because a packet kept for a
// client outlives the connection that would have registered it. Both ways it
// went wrong, measured on bin/saguin before the fix:
//
//   - a QoS 1 delivery sent alias-only and not acknowledged was re-sent to
//     the resumed session as `topic="" alias=1`, on a connection that had
//     registered no alias at all;
//   - with the window full, a withheld QoS 1 delivery took alias 2, and a
//     QoS 0 publish to the same topic went out as `topic="" alias=2` before
//     the packet that named the topic.
//
// A client meeting either must treat it as a Protocol Error (MQTT 5 section
// 3.3.2.3.4). mosquitto 2.0.22 sends no outbound alias. Read off the wire,
// from a subscriber that allows sixteen.
func TestNoTopicAliasIsSentToASubscriber(t *testing.T) {
	t.Run("a delivery re-sent to a resumed session", func(t *testing.T) {
		h := start(t)
		s, _ := aliasDial(t, h.Addr, "alias-resume", true, 10)
		s.Subscribe("loose/a", 1)
		pub := connect(t, h, "alias-resume-pub", true, false)

		pub.Pub(t, "loose/a", "m1")
		s.Puback(named(t, s.Next(3*time.Second), "loose/a", "m1"))
		pub.Pub(t, "loose/a", "m2")
		named(t, s.Next(3*time.Second), "loose/a", "m2") // not acknowledged
		s.Close()
		sessionGone(t, h, "alias-resume")

		again, ca := aliasDial(t, h.Addr, "alias-resume", false, 10)
		if !ca.SessionPresent {
			t.Fatal("the session was not resumed, so nothing is re-sent and this proves nothing")
		}
		named(t, again.Next(3*time.Second), "loose/a", "m2")
	})

	t.Run("a QoS 0 publish while a delivery to its topic is withheld", func(t *testing.T) {
		h := start(t)
		s, _ := aliasDial(t, h.Addr, "alias-window", true, 1)
		s.Subscribe("loose/b/#", 1)
		pub := connect(t, h, "alias-window-pub", true, false)

		pub.Pub(t, "loose/b/x", "m1")
		first := named(t, s.Next(3*time.Second), "loose/b/x", "m1") // fills the window of 1
		pub.Pub(t, "loose/b/y", "m2")                               // withheld
		if _, err := pub.C.Publish(context.Background(), &paho.Publish{
			Topic: "loose/b/y", QoS: 0, Payload: []byte("m3"),
		}); err != nil {
			t.Fatalf("publish at QoS 0: %v", err)
		}
		named(t, s.Next(3*time.Second), "loose/b/y", "m3")
		s.Puback(first)
		named(t, s.Next(3*time.Second), "loose/b/y", "m2")
	})
}

// RFC 0002 "Every session's state: `broker.session`" - what the session store
// keeps follows the connection: a persistent session is saved with the
// interval it was granted, kept with the moment its client went away, resumed,
// ended by a clean start with everything it held, never written for a session
// that ends with its connection, and gone when it expires.
func TestASessionRecordFollowsItsConnection(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		s := brokertest.HarnessSessions

		// Saved at CONNECT, connected, with the interval it asked for.
		w, _ := windowDial(t, h.Addr, "keeper", true, 10)
		got, ok, err := s.Get("keeper")
		if err != nil || !ok {
			t.Fatalf("a persistent session was not kept at CONNECT: %v, %v", ok, err)
		}
		if got.ExpiryInterval != 300 || !got.DisconnectedAt.IsZero() {
			t.Errorf("kept with interval %d and disconnected at %v, want 300 and connected",
				got.ExpiryInterval, got.DisconnectedAt)
		}

		// Kept with the moment its client went away.
		before := time.Now()
		w.Close()
		sessionGone(t, h, "keeper")
		got, ok, _ = s.Get("keeper")
		if !ok || got.DisconnectedAt.Before(before.Add(-time.Second)) || got.DisconnectedAt.After(time.Now()) {
			t.Fatalf("after its client went away the session is %+v, %v - want it kept with that moment", got, ok)
		}

		// Resumed: connected again.
		again, ca := windowDial(t, h.Addr, "keeper", false, 10)
		if !ca.SessionPresent {
			t.Fatal("the session was not resumed, so the rest of this proves nothing about a resume")
		}
		if got, _, _ = s.Get("keeper"); !got.DisconnectedAt.IsZero() {
			t.Errorf("a resumed session still reads disconnected at %v", got.DisconnectedAt)
		}
		// Something on the wire for it to hold: a delivery it is sent and
		// does not acknowledge.
		again.Subscribe("loose/#", 1)
		connect(t, h, "keeper-pub", true, false).Pub(t, "loose/x", "x")
		if cp := again.Next(3 * time.Second); cp.PacketType() != "PUBLISH" {
			t.Fatalf("the resumed session was sent a %s, want the delivery", cp.PacketType())
		}
		tables := s.(interface {
			InFlight(string) (uint16, []store.InFlight, error)
		})
		var table []store.InFlight
		if !waitUntil(3*time.Second, func() bool {
			_, table, _ = tables.InFlight("keeper")
			return len(table) == 1
		}) {
			t.Fatalf("the session holds %+v in flight, want the one delivery, so a clean start "+
				"ending it proves nothing", table)
		}
		again.Close()
		sessionGone(t, h, "keeper")

		// A clean start ends it, with what it held.
		// **Waited for rather than read once**: the session a clean start
		// replaces is ended once its CONNACK is written - a connection whose
		// CONNACK never arrives replaces nothing - so the CONNACK can reach
		// this client a moment before the end is written, and before the
		// broker reads anything from it.
		fresh, _ := windowDial(t, h.Addr, "keeper", true, 10)
		if !waitUntil(3*time.Second, func() bool {
			_, table, _ = tables.InFlight("keeper")
			return len(table) == 0
		}) {
			t.Errorf("a clean start left %+v in flight of the session it replaced", table)
		}
		fresh.Close()
		sessionGone(t, h, "keeper")

		// A session that ends with its connection is never written at its
		// CONNECT, and a client asking for one ends whatever its id held.
		dial(t, h, "passing", true, false, 0, 0, 0)
		if _, ok, _ := s.Get("passing"); ok {
			t.Error("a session that ends with its connection was written to the store")
		}
		if _, ok, _ := s.Get("keeper"); !ok {
			t.Fatal("the keeper's session is gone before the case that ends it")
		}
		// Ended as its CONNACK is written, as the clean start above is.
		dial(t, h, "keeper", true, false, 0, 0, 0)
		if !waitUntil(3*time.Second, func() bool {
			_, ok, _ := s.Get("keeper")
			return !ok
		}) {
			t.Error("a clean start with no expiry left the session the id held before")
		}

		// A client asking for longer than limits.max_session_expiry is kept
		// with the cap, which is what its expiry is judged against.
		long := dial(t, h, "long", true, false, 0, 4294967295, 0)
		got, ok, _ = s.Get("long")
		if !ok || got.ExpiryInterval == 0 || got.ExpiryInterval == 4294967295 {
			t.Errorf("a session asking for 136 years was kept as %+v, %v - want the cap", got, ok)
		}
		_ = long.C.Disconnect(&paho.Disconnect{})

		// Gone when it expires.
		short := dial(t, h, "short", true, false, 0, 1, 0)
		if _, ok, _ := s.Get("short"); !ok {
			t.Fatal("a one-second session was not kept, so its expiry proves nothing")
		}
		gone(t, short)
		awaitSessionExpired(t, h, "short", 5*time.Second)
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if _, ok, _ := s.Get("short"); !ok {
				return
			}
		}
		t.Error("an expired session is still in the store")
	})
}

// RFC 0004 "Enforcing a size bound" and RFC 0002 "Every session's state":
// a memory provider whose snapshot comes back over a bound the operator
// lowered holds all of it and is charged for all of it, so every publish into
// it is refused 0x97 until room frees - and a client still connects, as a
// full session store lets one: the broadcast log gives way first, and where
// it has nothing to give, the session is accepted and told it ends with its
// connection, and only a Will it cannot hold is refused.
//
// **Charged at the start, where it used to be refused and left uncounted**:
// the channel held 42,720 bytes, the provider counted 3, and the next
// publish was answered 0x00.
func TestAMemoryProviderRestoredOverALoweredBoundRefusesPublishesAndStillTakesClients(t *testing.T) {
	payload := strings.Repeat("x", 1024)
	// restart stops h, which held `held` bytes, and starts it again under
	// max, which the test asks to be below what it held.
	restart := func(t *testing.T, dir string, h *harness, held, max int64) (*harness, *store.Quota) {
		t.Helper()
		if held <= max {
			t.Fatalf("the provider held %d bytes at the stop, not over the bound of %d, so this proves nothing", held, max)
		}
		h.Stop()
		h = startDurable(t, dir)
		q := store.NewQuota(max, 0)
		h.B.SetQuotas(map[string]*store.Quota{"local": q})
		if q.Bytes() < held || !q.OverBound() {
			t.Errorf("the provider came back counting %d of the %d bytes it held (bound %d, over it: %t)",
				q.Bytes(), held, max, q.OverBound())
		}
		return h, q
	}

	t.Run("nothing in the broadcast log to give", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)
		first := store.NewQuota(64<<10, 0)
		h.B.SetQuotas(map[string]*store.Quota{"local": first})
		pub := connect(t, h, "pub", true, false)
		for range 40 {
			pub.Pub(t, "events/a", payload)
		}
		settle(t, pub)
		pub.Close()

		h, _ = restart(t, dir, h, first.Bytes(), 32<<10)
		pub = connect(t, h, "pub", true, false)
		resp, err := pub.C.Publish(context.Background(), &paho.Publish{Topic: "events/a", QoS: 1, Payload: []byte(payload)})
		if resp == nil || resp.ReasonCode != 0x97 {
			t.Errorf("a publish into a provider over its bound was answered %v (%v), want PUBACK 0x97", resp, err)
		}
		_, ca := windowDial(t, h.Addr, "late", false, 10)
		if ca.ReasonCode != 0 || ca.Properties.SessionExpiryInterval == nil || *ca.Properties.SessionExpiryInterval != 0 {
			t.Errorf("a durable CONNECT was answered 0x%02X with expiry %v, want 0x00 with 0: accepted, and told "+
				"its session ends with its connection", ca.ReasonCode, ca.Properties.SessionExpiryInterval)
		}
		will := rawCONNECT(t, h, connectFor(t, "will", true, 3600, "wills/will"))
		if got := readAnswers(t, will, 1); got[0] != "CONNACK 0x97 present=false" {
			t.Errorf("a CONNECT with a Will the provider cannot hold was answered %v, want CONNACK 0x97", got)
		}
	})

	t.Run("the broadcast log gives way", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)
		first := store.NewQuota(96<<10, 0)
		h.B.SetQuotas(map[string]*store.Quota{"local": first})
		away := connect(t, h, "away", false, false)
		away.Sub(t, "loose/owed", 1)
		away.Close()
		sessionGone(t, h, "away")
		pub := connect(t, h, "pub", true, false)
		for range 20 {
			pub.Pub(t, "events/a", payload)
		}
		for range 30 {
			pub.Pub(t, "loose/owed", payload)
		}
		settle(t, pub)
		pub.Close()

		h, q := restart(t, dir, h, first.Bytes(), 48<<10)
		ops := operationsAt(t, h)
		before := scrapeGauges(t, ops)[`saguin_session_deliveries_dropped_total{cause="storage_full"}`]
		_, ca := windowDial(t, h.Addr, "late", false, 10)
		if ca.ReasonCode != 0 || (ca.Properties.SessionExpiryInterval != nil && *ca.Properties.SessionExpiryInterval == 0) {
			t.Errorf("a durable CONNECT was answered 0x%02X with expiry %v, want its session kept: the broadcast "+
				"log had room to give", ca.ReasonCode, ca.Properties.SessionExpiryInterval)
		}
		if got := scrapeGauges(t, ops)[`saguin_session_deliveries_dropped_total{cause="storage_full"}`] - before; got <= 0 {
			t.Errorf("session_deliveries_dropped_total{cause=storage_full} went up %v: the log gave nothing way", got)
		}
		if publish, _ := q.Effective(); q.Bytes() > publish {
			t.Errorf("the provider counts %d after the log gave way, over the %d a write may take it to", q.Bytes(), publish)
		}
	})
}

// RFC 0002 "Every session's state: `broker.session`" - a
// persistent session the store has no room for is accepted, told in its
// CONNACK that it ends with the connection, counted, and leaves nothing to be
// resumed; the broker goes on serving everybody else. Run on both providers,
// with a control saved before the store fills, so what the client is told is
// the full store's doing.
func TestASessionTheStoreHasNoRoomForEndsWithItsConnection(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			if provider == "memory" {
				h = startDurable(t, t.TempDir())
				h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(64<<10, 0)})
			} else {
				brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+64<<10, "16KiB")
				h = startDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))
			}
			s := brokertest.HarnessSessions
			ops := operationsAt(t, h)

			// Subscribed before the store fills, so a broadcast has somebody to
			// reach once it is full: with the store full a new subscription is
			// state the store cannot keep, and is refused.
			listener := connect(t, h, "listener", true, false)
			listener.Sub(t, "loose/after-full", 1)

			control, ca := windowDial(t, h.Addr, "control-1", true, 10)
			if ca.Properties.SessionExpiryInterval != nil && *ca.Properties.SessionExpiryInterval == 0 {
				t.Fatal("the control was told its session ends with the connection before the store filled")
			}
			control.Close()
			sessionGone(t, h, "control-1")

			// Fill the store with sessions of the same shape, one character
			// shorter in the client id than the one that will be refused - a
			// premise the test asks rather than assumes, since a refused
			// session no larger than the last filler can fit the room that
			// filler could not, by bytes, and the test then proves nothing.
			const refused = "refused-000001"
			if len(refused) <= len(fmt.Sprintf("filler-%06d", 0)) {
				t.Fatalf("%q is no longer than a filler's id, so it may fit where the last filler did not", refused)
			}
			filled := 0
			for i := 0; ; i++ {
				err := s.Save(store.Session{Client: fmt.Sprintf("filler-%06d", i), ExpiryInterval: 300})
				if errors.Is(err, store.ErrFull) {
					break
				}
				if err != nil {
					t.Fatalf("filling the store: %v", err)
				}
				if filled++; filled > 100000 {
					t.Fatal("100,000 sessions fitted, so the provider is not bounded and this proves nothing")
				}
			}
			if filled == 0 {
				t.Fatal("the store refused the first session, so it was full before this test filled it")
			}

			w, ca := windowDial(t, h.Addr, refused, true, 10)
			if ca.Properties.SessionExpiryInterval == nil || *ca.Properties.SessionExpiryInterval != 0 {
				t.Errorf("a session the store had no room for was told expiry %v, want 0", ca.Properties.SessionExpiryInterval)
			}
			if _, ok, _ := s.Get(refused); ok {
				t.Error("a session the store had no room for is in the store")
			}
			if got := scrapeGauges(t, ops)[`saguin_sessions_dropped_total{cause="storage_full"}`]; got != 1 {
				t.Errorf("saguin_sessions_dropped_total{cause=storage_full} is %v, want 1", got)
			}
			w.Close()
			sessionGone(t, h, refused)

			// Nothing to resume: it ended with its connection.
			back, ca := windowDial(t, h.Addr, refused, false, 10)
			if ca.SessionPresent {
				t.Error("a session the store could not keep was resumed")
			}
			back.Close()

			// And the broker serves everybody else: the control resumes, and a
			// broadcast reaches a subscriber.
			if _, ca := windowDial(t, h.Addr, "control-1", false, 10); !ca.SessionPresent {
				t.Error("the session kept before the store filled did not resume")
			}
			// At QoS 0, which holds nothing in the store: a QoS 1 delivery
			// with the store full follows its own row of the table, and
			// TestADeliveryTheStoreHasNoRoomForGivesUpTheOldest has it.
			pubAtMostOnce(t, connect(t, h, "talker", true, false), "loose/after-full", "still here")
			if got, ok := listener.Await(t, 3*time.Second); !ok || got.Payload != "still here" {
				t.Errorf("with the session store full a broadcast arrived as %+v, %v", got, ok)
			}
			late, _ := windowDial(t, h.Addr, "late-sub", true, 10)
			if got := rawSubscribe(t, late, 1, 0, pahopackets.SubOptions{Topic: "loose/late", QoS: 1}); got[0] != 0x97 {
				t.Errorf("a new subscription with the session store full was answered 0x%02X, want 0x97", got[0])
			}
		})
	}
}

// rawSubscribe sends one SUBSCRIBE and returns its SUBACK codes, reading what
// the broker wrote.
func rawSubscribe(t *testing.T, w *qos2Wire, id uint16, ident int, subs ...pahopackets.SubOptions) []byte {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
	s := cp.Content.(*pahopackets.Subscribe)
	s.PacketID = id
	s.Subscriptions = subs
	if ident > 0 {
		s.Properties.SubscriptionIdentifier = &ident
	}
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	sa, ok := w.Next(3 * time.Second).Content.(*pahopackets.Suback)
	if !ok {
		t.Fatal("the SUBSCRIBE was not answered with a SUBACK")
	}
	return sa.Reasons
}

func rawUnsubscribe(t *testing.T, w *qos2Wire, id uint16, filters ...string) {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.UNSUBSCRIBE)
	u := cp.Content.(*pahopackets.Unsubscribe)
	u.PacketID = id
	u.Topics = filters
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatalf("write UNSUBSCRIBE: %v", err)
	}
	if _, ok := w.Next(3 * time.Second).Content.(*pahopackets.Unsuback); !ok {
		t.Fatal("the UNSUBSCRIBE was not answered with an UNSUBACK")
	}
}

func storedSubscription(t *testing.T, client, filter string) (store.SessionSubscription, bool) {
	t.Helper()
	sess, ok, err := brokertest.HarnessSessions.Get(client)
	if err != nil || !ok {
		t.Fatalf("no session kept for %s: %v, %v", client, ok, err)
	}
	for _, sub := range sess.Subscriptions {
		if sub.Filter == filter {
			return sub, true
		}
	}
	return store.SessionSubscription{}, false
}

// RFC 0002 "Every session's state: `broker.session`" - a session's
// subscriptions are kept in the session store with every option the SUBSCRIBE
// carried, follow an UNSUBSCRIBE and a change of options, leave out what the
// broker refused, and for a session that ends with its connection last only as
// long as it does.
func TestSubscriptionsAreKeptWithTheirSession(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		w, _ := windowDial(t, h.Addr, "subs", true, 10)
		if got := rawSubscribe(t, w, 1, 7, pahopackets.SubOptions{Topic: "loose/a", QoS: 1,
			NoLocal: true, RetainAsPublished: true, RetainHandling: 2}); got[0] != 1 {
			t.Fatalf("loose/a was answered 0x%02X", got[0])
		}
		// With the broadcast log's offset when it was made (RFC 0004): the
		// first, on a log nothing has been written to.
		want := store.SessionSubscription{Filter: "loose/a", QoS: 1, NoLocal: true,
			RetainAsPublished: true, RetainHandling: 2, Identifier: 7, Since: 1}
		if got, ok := storedSubscription(t, "subs", "loose/a"); !ok || !reflect.DeepEqual(got, want) {
			t.Errorf("kept as %+v, %v, want %+v", got, ok, want)
		}

		// A change of options replaces what was kept.
		rawSubscribe(t, w, 2, 0, pahopackets.SubOptions{Topic: "loose/a", QoS: 0})
		if got, _ := storedSubscription(t, "subs", "loose/a"); got.QoS != 0 || got.NoLocal || got.Identifier != 0 {
			t.Errorf("a changed subscription was kept as %+v", got)
		}

		// A refused filter is not kept.
		if got := rawSubscribe(t, w, 3, 0, pahopackets.SubOptions{Topic: "$SYS/#", QoS: 1}); got[0] < 0x80 {
			t.Fatalf("$SYS/# was granted 0x%02X, so this cannot show a refusal is left out", got[0])
		}
		if _, ok := storedSubscription(t, "subs", "$SYS/#"); ok {
			t.Error("a refused filter is in the session store")
		}

		rawUnsubscribe(t, w, 4, "loose/a")
		if _, ok := storedSubscription(t, "subs", "loose/a"); ok {
			t.Error("an unsubscribed filter is still kept")
		}

		// A partition declaration is kept with its filter.
		sliced := connect(t, h, "sliced", true, false)
		if sa := sliced.SubSliced(t, "iot/+/events/+", 4, 1); sa.Reasons[0] > 1 {
			t.Fatalf("the declaring subscription was refused 0x%02x", sa.Reasons[0])
		}
		for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			got, ok := storedSubscription(t, "sliced", "iot/+/events/+")
			if ok && got.PartitionCount == 4 && reflect.DeepEqual(got.PartitionIndices, []int{1}) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the declaration was kept as %+v, %v, want slice 1 of 4", got, ok)
			}
		}

		// A session ending with its connection keeps its subscriptions while
		// it lasts, and nothing after.
		np := dial(t, h, "passing-sub", true, false, 0, 0, 0)
		np.Sub(t, "loose/np", 1)
		sess, ok, _ := brokertest.HarnessSessions.Get("passing-sub")
		if !ok || sess.ExpiryInterval != 0 || len(sess.Subscriptions) != 1 {
			t.Errorf("a connected non-persistent session is kept as %+v, %v", sess, ok)
		}
		gone(t, np)
		sessionGone(t, h, "passing-sub")
		if _, ok, _ := brokertest.HarnessSessions.Get("passing-sub"); ok {
			t.Error("a session that ended with its connection left its subscriptions behind")
		}
	})
}

// RFC 0002 - a SUBSCRIBE the session store has no room for
// is refused `0x97` in its SUBACK for every filter it would add or change, the
// session is kept exactly as it was, and an existing subscription the client
// tried to change goes on being served as it was. On both providers, and the
// broker serves everybody else.
func TestASubscriptionTheStoreHasNoRoomForIsRefused(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			if provider == "memory" {
				h = startDurable(t, t.TempDir())
				h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(64<<10, 0)})
			} else {
				brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+64<<10, "16KiB")
				h = startDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))
			}
			s := brokertest.HarnessSessions

			listener := connect(t, h, "full-listener", true, false)
			listener.Sub(t, "loose/after", 1)

			w, _ := windowDial(t, h.Addr, "full-sub", true, 10)
			if got := rawSubscribe(t, w, 1, 0, pahopackets.SubOptions{Topic: "loose/old", QoS: 1}); got[0] != 1 {
				t.Fatalf("loose/old was answered 0x%02X before the store filled", got[0])
			}
			before, _, _ := s.Get("full-sub")

			filled := 0
			for i := 0; ; i++ {
				err := s.Save(store.Session{Client: fmt.Sprintf("filler-%06d", i), ExpiryInterval: 300,
					Subscriptions: []store.SessionSubscription{{Filter: "a/long/enough/filter/to/fill/rows", QoS: 1}}})
				if errors.Is(err, store.ErrFull) {
					break
				}
				if err != nil {
					t.Fatalf("filling the store: %v", err)
				}
				if filled++; filled > 100000 {
					t.Fatal("100,000 sessions fitted, so the provider is not bounded")
				}
			}

			got := rawSubscribe(t, w, 2, 0,
				pahopackets.SubOptions{Topic: "loose/new-and-rather-longer-than-any-filter-before", QoS: 1},
				pahopackets.SubOptions{Topic: "loose/old", QoS: 0})
			if len(got) != 2 || got[0] != 0x97 || got[1] != 0x97 {
				t.Fatalf("with the store full the SUBACK was % 02x, want 97 97", got)
			}
			after, _, _ := s.Get("full-sub")
			if !reflect.DeepEqual(after, before) {
				t.Errorf("a refused SUBSCRIBE changed the session kept:\n%+v\nwas\n%+v", after, before)
			}

			// The existing subscription is served as it was: QoS 1. A QoS 1
			// delivery needs room in the store, so some is made first; the
			// refusal above was taken with the store full.
			for i := 0; i < filled && i < 1000; i++ {
				if _, err := s.Drop(fmt.Sprintf("filler-%06d", i), nil); err != nil {
					t.Fatalf("making room: %v", err)
				}
			}
			connect(t, h, "full-talker", true, false).Pub(t, "loose/old", "still")
			for {
				p := w.Next(3 * time.Second)
				if pub, ok := p.Content.(*pahopackets.Publish); ok {
					if pub.Topic != "loose/old" || pub.QoS != 1 {
						t.Errorf("delivered %s at QoS %d, want loose/old at the QoS it was granted before, 1", pub.Topic, pub.QoS)
					}
					break
				}
			}

			// And everybody subscribed is served.
			pubAtMostOnce(t, connect(t, h, "full-talker-2", true, false), "loose/after", "fine")
			if m, ok := listener.Await(t, 3*time.Second); !ok || m.Payload != "fine" {
				t.Errorf("with the session store full a broadcast arrived as %+v, %v", m, ok)
			}
		})
	}
}

// pubAtMostOnce publishes at QoS 0, which the session store holds nothing of.
func pubAtMostOnce(t testing.TB, c *client, topic, payload string) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: 0, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// broadcastLog is the broadcast log of the provider the harness keeps its
// sessions in: where a message a session that outlives its connection is owed
// waits for it (RFC 0003 "Broadcast").
func broadcastLog(t *testing.T) interface {
	Next() uint64
	ReadAt(offsets ...uint64) ([]store.Record, error)
} {
	t.Helper()
	switch s := brokertest.HarnessSessions.(type) {
	case *store.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg
	case *sqlite.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg
	}
	t.Fatalf("the harness keeps its sessions in a %T", brokertest.HarnessSessions)
	return nil
}

// logged is every message the broadcast log holds, in the order it holds
// them.
func logged(t *testing.T) []store.Record {
	t.Helper()
	lg := broadcastLog(t)
	var offs []uint64
	for o := uint64(1); o < lg.Next(); o++ {
		offs = append(offs, o)
	}
	recs, err := lg.ReadAt(offs...)
	if err != nil {
		t.Fatalf("read the broadcast log: %v", err)
	}
	return recs
}

// awaitOwed waits until the sessions the broker holds are owed n messages
// taking bytes, as saguin_session_queue_messages and saguin_session_queue_bytes
// say (RFC 0005), and answers them; bytes below zero is not asked. In these
// tests one session is owed anything.
func awaitOwed(t *testing.T, ops, what string, n int) (messages, bytes float64) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		messages = metricValue(t, ops, "saguin_session_queue_messages")
		bytes = metricValue(t, ops, "saguin_session_queue_bytes")
		if messages == float64(n) {
			return messages, bytes
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the sessions are owed %v messages, want %d", what, messages, n)
		}
	}
}

// nextPublish reads until the broker writes a PUBLISH, and returns it.
func nextPublish(t *testing.T, w *qos2Wire) *pahopackets.Publish {
	t.Helper()
	for {
		if p, ok := w.Next(3 * time.Second).Content.(*pahopackets.Publish); ok {
			return p
		}
	}
}

// wireAck writes a PUBREC or a PUBCOMP as a subscriber answering a delivery.
func wireAck(t *testing.T, w *qos2Wire, kind byte, id uint16) {
	t.Helper()
	cp := pahopackets.NewControlPacket(kind)
	switch a := cp.Content.(type) {
	case *pahopackets.Pubrec:
		a.PacketID = id
	case *pahopackets.Pubcomp:
		a.PacketID = id
	default:
		t.Fatalf("wireAck writes a PUBREC or a PUBCOMP, not packet type %d", kind)
	}
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatalf("write acknowledgement: %v", err)
	}
}

// A durable session taken over by its own client id, again and again while QoS 1
// messages are published to it, loses none of them, and every connection is
// answered with its CONNACK first [MQTT-3.2.0-1] [MQTT-4.3.2]. On both
// providers.
//
// **Measured losing them before**: a delivery that chose the old connection
// and recorded on it after the takeover copied its in-flight table was
// cleared with it - 92 to 160 of 100,000 deliveries with 50 clients on a
// memory session store, 356 to 463 on sqlite, none of it counted.
func TestATakenOverSessionLosesNoQoS1Delivery(t *testing.T) {
	const (
		subs      = 20
		published = 600
		every     = 200 * time.Millisecond
	)
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		type subscriber struct {
			mu  sync.Mutex
			got map[int]bool
		}
		var takeovers, notConnackFirst sync.Map
		resume := func(id string, clean bool, s *subscriber) net.Conn {
			c, err := net.DialTimeout("tcp", h.Addr, 3*time.Second)
			if err != nil {
				return nil
			}
			cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
			conn := cp.Content.(*pahopackets.Connect)
			conn.ClientID, conn.CleanStart, conn.KeepAlive = id, clean, 0
			expiry, rx := uint32(300), uint16(65535)
			conn.Properties.SessionExpiryInterval, conn.Properties.ReceiveMaximum = &expiry, &rx
			if _, err := cp.WriteTo(c); err != nil {
				_ = c.Close()
				return nil
			}
			var once sync.Once
			go func() {
				first := true
				for {
					_ = c.SetReadDeadline(time.Now().Add(120 * time.Second))
					p, err := pahopackets.ReadPacket(c)
					if err != nil {
						return
					}
					if first {
						first = false
						if p.FixedHeader.Type != pahopackets.CONNACK {
							once.Do(func() { notConnackFirst.Store(id+strconv.FormatInt(time.Now().UnixNano(), 10), p.FixedHeader.Type) })
						}
					}
					if pub, ok := p.Content.(*pahopackets.Publish); ok {
						if n, err := strconv.Atoi(string(pub.Payload)); err == nil {
							s.mu.Lock()
							s.got[n] = true
							s.mu.Unlock()
						}
						ack := pahopackets.NewControlPacket(pahopackets.PUBACK)
						ack.Content.(*pahopackets.Puback).PacketID = pub.PacketID
						_, _ = ack.WriteTo(c)
					}
				}
			}()
			return c
		}

		all := make([]*subscriber, subs)
		conns := make([]net.Conn, subs)
		for i := range all {
			all[i] = &subscriber{got: map[int]bool{}}
			id := fmt.Sprintf("taker-%02d", i)
			c := resume(id, true, all[i])
			if c == nil {
				t.Fatalf("%s could not connect", id)
			}
			cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
			sp := cp.Content.(*pahopackets.Subscribe)
			sp.PacketID = 1
			sp.Subscriptions = []pahopackets.SubOptions{{Topic: "loose/takeover/#", QoS: 1}}
			if _, err := cp.WriteTo(c); err != nil {
				t.Fatal(err)
			}
			conns[i] = c
		}
		// Every taker holds its subscription before anything is published.
		// Their SUBACKs are read by the readers above, so the broker is asked.
		if !waitUntil(5*time.Second, func() bool {
			for i := range all {
				cl, ok := h.Srv.Clients.Get(fmt.Sprintf("taker-%02d", i))
				if !ok {
					return false
				}
				if _, ok := cl.State.Subscriptions.Get("loose/takeover/#"); !ok {
					return false
				}
			}
			return true
		}) {
			t.Fatal("the takers' subscriptions were never all held")
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := range all {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("taker-%02d", i)
				tick := time.NewTicker(every + time.Duration(i)*time.Millisecond)
				defer tick.Stop()
				n := 0
				for {
					select {
					case <-stop:
						takeovers.Store(i, n)
						return
					case <-tick.C:
					}
					if c := resume(id, false, all[i]); c != nil {
						old := conns[i]
						conns[i] = c
						_ = old.Close()
						n++
					}
				}
			}(i)
		}

		p := connect(t, h, "takeover-pub", true, false)
		for n := 1; n <= published; n++ {
			if _, err := p.C.Publish(context.Background(), &paho.Publish{
				Topic: "loose/takeover/x", QoS: 1, Payload: []byte(strconv.Itoa(n))}); err != nil {
				t.Fatalf("publish %d: %v", n, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
		close(stop)
		wg.Wait()
		total := 0
		takeovers.Range(func(_, v any) bool { total += v.(int); return true })
		if total < subs*5 {
			t.Fatalf("only %d takeovers happened while publishing, so this says little about a takeover", total)
		}

		// Every session is taken over once more and re-sent what it is owed.
		for i := range all {
			if c := resume(fmt.Sprintf("taker-%02d", i), false, all[i]); c != nil {
				_ = conns[i].Close()
				conns[i] = c
			}
		}
		missing := func() (int, int) {
			lost, who := 0, 0
			for _, s := range all {
				s.mu.Lock()
				miss := 0
				for n := 1; n <= published; n++ {
					if !s.got[n] {
						miss++
					}
				}
				s.mu.Unlock()
				if miss > 0 {
					lost, who = lost+miss, who+1
				}
			}
			return lost, who
		}
		// **Wait for the condition, with the timeout only as a backstop.**
		// What this test asserts is that a takeover loses nothing, not that a
		// drain finishes inside a particular clock. The fixed ten seconds it
		// used to wait was calibrated to a broker that drained in about seven;
		// one that keeps a waiting delivery in its session store and reads it
		// back once when its turn comes drains in about ten, so the clock
		// rather than the broker decided whether this passed - 5 of 20 runs,
		// with one to eleven deliveries of twelve thousand still arriving. The
		// backstop is generous because it is here to fail a stall, not to time
		// a drain, and what the drain actually cost is logged rather than
		// hidden by the wait.
		settleStart := time.Now()
		lost, who := missing()
		for deadline := time.Now().Add(90 * time.Second); lost > 0 && time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			lost, who = missing()
		}
		t.Logf("%d takeovers during %d QoS 1 publishes to %d subscribers; what they were owed drained in %s",
			total, published, subs, time.Since(settleStart).Round(time.Millisecond))
		if lost > 0 {
			t.Errorf("%d QoS 1 deliveries never arrived, across %d of %d sessions taken over", lost, who, subs)
		}
		notConnackFirst.Range(func(k, v any) bool {
			t.Errorf("a connection was sent packet type %v before its CONNACK", v)
			return true
		})
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

// drainDelivered collects deliveries until none has arrived for quiet.
func drainDelivered(got <-chan delivered, quiet time.Duration) []delivered {
	var all []delivered
	for {
		select {
		case d, ok := <-got:
			if !ok {
				return all
			}
			all = append(all, d)
		case <-time.After(quiet):
			return all
		}
	}
}

// RFC 0002 `limits.session_queue_bytes` - a client that reads every delivery
// and acknowledges none holds no more than its bound, is written no more than
// half of it, and once it acknowledges is sent the newest of what it missed.
// Every publish is accepted, and every delivery it never receives is counted.
//
// **This was bounded only by a count**, Mochi's 8,192 in-flight entries:
// measured on bin/saguin with 4 KiB messages, one such client grew the broker
// by 109 MiB with that count and by 719 MiB without it, against a bound of
// 1 MiB. Refusing past the bound kept it, and kept the oldest.
func TestAClientThatNeverAcknowledgesHoldsItsBoundAndKeepsTheNewest(t *testing.T) {
	const (
		published = 200
		size      = 4 << 10
		bound     = 64 << 10
	)
	brokertest.SessionQueueBytes = bound
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)
	ops := operationsAt(t, h)

	deaf, _ := windowDial(t, h.Addr, "deaf", true, 65535)
	deaf.Subscribe("loose/deaf", 1)
	got := make(chan delivered, published+10)
	deafReader(deaf, got)

	p := connect(t, h, "deaf-pub", true, false)
	for i := 1; i <= published; i++ {
		pa, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "loose/deaf", QoS: 1, Payload: numbered(i, size)})
		if err != nil || pa.ReasonCode != 0 {
			t.Fatalf("publish %d was not accepted: %v %v - a publisher is never refused for a session's bound", i, err, pa)
		}
	}
	written := drainDelivered(got, time.Second)
	if len(written) == 0 {
		t.Fatal("the client was sent nothing, so nothing here is about what it holds")
	}
	for k, d := range written {
		if d.N != k+1 {
			t.Fatalf("delivery %d carried message %d: what a client is written first is the oldest, in order", k, d.N)
		}
	}
	// One is written whatever its size, so it is the ones after it that
	// must fit within half.
	if written := int64(len(written)-1) * size; written > bound/2 {
		t.Errorf("%d bytes of payload beyond the first delivery were written unacknowledged, past half of a %d-byte bound", written, bound)
	}

	// What the session holds is what it is owed from the broadcast log, on
	// the wire or waiting (RFC 0005 saguin_session_queue_messages and _bytes).
	kept := int(metricValue(t, ops, "saguin_session_queue_messages"))
	if held := metricValue(t, ops, "saguin_session_queue_bytes"); held > bound {
		t.Errorf("the session holds %v bytes of unacknowledged deliveries against a bound of %d", held, bound)
	}
	if kept <= len(written) {
		t.Fatalf("the session keeps %d deliveries and wrote %d, so none waits and the share on the wire was never reached", kept, len(written))
	}
	if got, want := scrapeGauges(t, ops)[`saguin_session_deliveries_dropped_total{cause="session_queue_full"}`], float64(published-kept); got != want {
		t.Errorf("session_queue_full is %v, and %v of %d deliveries are not in the session", got, want, published)
	}

	// Recovered: it acknowledges what it reads, and is sent what the session
	// kept for it, which is the newest.
	var rest []delivered
	for batch := written; len(batch) > 0; batch = drainDelivered(got, time.Second) {
		for _, d := range batch {
			deaf.Puback(d.ID)
		}
		if &batch[0] != &written[0] {
			rest = append(rest, batch...)
		}
	}
	if len(written)+len(rest) != kept {
		t.Errorf("after acknowledging, the client was sent %d more, and the session held %d beyond the %d written", len(rest), kept-len(written), len(written))
	}
	for k, d := range rest {
		if want := published - len(rest) + 1 + k; d.N != want {
			t.Fatalf("after acknowledging, delivery %d carried message %d, want %d: the session keeps the newest", k, d.N, want)
		}
	}
}

// RFC 0002 `limits.session_queue_bytes` - a client that acknowledges every
// delivery, but more slowly than they are published, loses only what its
// bound could not keep and never a delivery uncounted: what it receives, in
// order and once each, and what was given up add up to what was published,
// and the last publish reaches it.
func TestASlowClientReceivesInOrderAndLosesOnlyWhatIsCounted(t *testing.T) {
	const (
		published = 300
		size      = 4 << 10
		bound     = 64 << 10
	)
	brokertest.SessionQueueBytes = bound
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)
	ops := operationsAt(t, h)

	slow, _ := windowDial(t, h.Addr, "slow", true, 65535)
	slow.Subscribe("loose/slow", 1)
	got := make(chan delivered, published+10)
	deafReader(slow, got)

	var mu sync.Mutex
	var received []int
	go func() {
		for d := range got {
			time.Sleep(5 * time.Millisecond)
			ack := pahopackets.NewControlPacket(pahopackets.PUBACK)
			ack.Content.(*pahopackets.Puback).PacketID = d.ID
			if _, err := ack.WriteTo(slow.C); err != nil {
				return
			}
			mu.Lock()
			received = append(received, d.N)
			mu.Unlock()
		}
	}()

	cl, ok := h.Srv.Clients.Get("slow")
	if !ok {
		t.Fatal("the slow client is not connected")
	}
	p := connect(t, h, "slow-pub", true, false)
	for i := 1; i <= published; i++ {
		pa, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "loose/slow", QoS: 1, Payload: numbered(i, size)})
		if err != nil || pa.ReasonCode != 0 {
			t.Fatalf("publish %d was not accepted: %v %v", i, err, pa)
		}
		time.Sleep(time.Millisecond) // faster than the client acknowledges, and not so fast it only ever sees a deaf one
		// A delivery being queued counts before the oldest it replaces is
		// given up, so a session seen in between is past its bound by that
		// one delivery and no more: its payload, and about a kilobyte beside.
		if held := cl.State.Inflight.Bytes(); held > bound+size+2<<10 {
			t.Fatalf("after publish %d the session holds %d bytes against a bound of %d and one %d-byte delivery", i, held, bound, size)
		}
	}
	// **Read to the last publish, not to a quiet second.** The client is sent
	// in order, and a session at its bound gives up its oldest and never the
	// newest, so the last message published is the last it is sent: once it
	// has that, everything it will be sent has arrived. The deadline bounds
	// a broker that never sends it, which the checks below then report.
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		mu.Lock()
		done := len(received) > 0 && received[len(received)-1] == published
		mu.Unlock()
		if done {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()
	dropped := scrapeGauges(t, ops)[`saguin_session_deliveries_dropped_total{cause="session_queue_full"}`]
	if dropped == 0 {
		t.Fatal("nothing was given up, so the client kept up and this says nothing about a slow one")
	}
	if float64(len(received))+dropped != published {
		t.Errorf("the client received %d and %v were given up, of %d published: a delivery was lost uncounted, or counted twice", len(received), dropped, published)
	}
	for k := 1; k < len(received); k++ {
		if received[k] <= received[k-1] {
			t.Fatalf("message %d arrived after %d: a slow client is sent in order, and once", received[k], received[k-1])
		}
	}
	if len(received) == 0 || received[len(received)-1] != published {
		t.Errorf("the last message the slow client received is not the last published, %d", published)
	}
	t.Logf("received %d of %d, %v given up", len(received), published, dropped)
}

// A client that acknowledges nothing is sent as much as its bound allows,
// however many messages that is: there is no count of unacknowledged
// deliveries beside the bytes. Mochi's was 8,192, and this sends more.
func TestAClientIsSentMoreThanEightThousandUnacknowledged(t *testing.T) {
	const published = 9000
	brokertest.SessionQueueBytes = 64 << 20
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)

	deaf, _ := windowDial(t, h.Addr, "counted", true, 65535)
	deaf.Subscribe("loose/counted", 1)
	got := make(chan delivered, published+10)
	deafReader(deaf, got)

	p := connect(t, h, "counted-pub", true, false)
	for i := range published {
		if _, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "loose/counted", QoS: 1, Payload: []byte("x")}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if all := drainDelivered(got, 2*time.Second); len(all) != published {
		t.Errorf("a client acknowledging nothing was sent %d of %d small deliveries within a 64 MiB bound", len(all), published)
	}
}

// RFC 0002 "Shared subscriptions": each message goes to a member whose
// outbound queue is not full. A member that stops reading fills its queue to
// half its session's bound in bytes, long before the queue's 8,192 packets,
// and is passed over from then on rather than handed messages it can only
// shed.
//
// **Every message is accounted for, three ways**: received by the live
// member, held by the deaf one - its queue and what its socket buffers - or
// counted `no_shared_member` because neither member had room. The publisher
// is QoS 0 and unthrottled, so the live member's queue fills too, as often
// as the broker reads faster than the test's reader decodes: 300 to 1,000
// of 2,000 were `no_shared_member` in runs of this test, more as reading a
// publish got cheaper. That is the rule working, not the deaf member keeping
// its turns - so the deaf member's turns are measured by what it holds,
// which is what having its turns would show: about half.
func TestASharedMemberWhoseQueueIsFullByBytesIsPassedOver(t *testing.T) {
	const published = 2000
	brokertest.SessionQueueBytes = 1 << 20
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)

	deaf, _ := windowDial(t, h.Addr, "share-deaf", true, 65535)
	deaf.Subscribe("$share/g/loose/shared/#", 0)
	live, _ := windowDial(t, h.Addr, "share-live", true, 65535)
	live.Subscribe("$share/g/loose/shared/#", 0)
	got := make(chan delivered, published+10)
	deafReader(live, got) // reads everything; the deaf member reads nothing

	// Counted by saguin_deliveries_dropped_total, which is every delivery the
	// engine shed at a full queue (OnPublishDropped), read the way an
	// operator reads it: recomputed on every scrape, so before and after are
	// two readings rather than one cached one.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	ops := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(ops), time.Nanosecond, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()
	droppedNow := func() float64 {
		t.Helper()
		v, ok := scrapeGauges(t, ops)["saguin_deliveries_dropped_total"]
		if !ok {
			t.Fatal("the scrape has no saguin_deliveries_dropped_total, so a zero below would mean nothing")
		}
		return v
	}
	noMemberNow := func() float64 {
		t.Helper()
		const series = `saguin_session_deliveries_dropped_total{cause="no_shared_member"}`
		v, ok := scrapeGauges(t, ops)[series]
		if !ok {
			t.Fatalf("the scrape has no %s, so a zero below would mean nothing", series)
		}
		return v
	}
	droppedBefore, noMemberBefore := droppedNow(), noMemberNow()
	p := connect(t, h, "share-pub", true, false)
	for i := range published {
		if _, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "loose/shared/x", QoS: 0, Payload: numbered(i+1, 32<<10)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	received := len(drainDelivered(got, 2*time.Second))
	dropped := droppedNow() - droppedBefore
	noMember := noMemberNow() - noMemberBefore
	// What the deaf member was handed, read now: its queue and its socket's
	// buffers, which is all a member that reads nothing can hold.
	held := make(chan delivered, published+10)
	deafReader(deaf, held)
	deafHeld := len(drainDelivered(held, 2*time.Second))
	t.Logf("the live member received %d of %d, the deaf member held %d, %v with no member able to take "+
		"them; %v dropped", received, published, deafHeld, noMember, dropped)

	if float64(received+deafHeld)+noMember != published {
		t.Errorf("%d received, %d held and %v counted no_shared_member of %d published: a message was "+
			"lost without being counted, or counted and delivered", received, deafHeld, noMember, published)
	}
	// Proof the deaf member was passed over once its queue filled: with its
	// turns it would hold about half of what was published.
	if deafHeld == 0 || deafHeld >= published/4 {
		t.Errorf("the deaf member held %d of %d: it kept its turns (or held nothing, and this measured "+
			"nothing)", deafHeld, published)
	}
	if received <= deafHeld {
		t.Errorf("the live member received %d and the deaf member held %d: the live member was not "+
			"handed the deaf member's turns", received, deafHeld)
	}
	if dropped != 0 {
		t.Errorf("%v shared deliveries were handed to a member whose queue was full and dropped there", dropped)
	}
}

// A QoS 1 delivery that finds the queue for its client's socket full waits in
// its session and is written once the queue has room, rather than being
// dropped: the publisher was told it was accepted, and the session owes it
// [MQTT-4.3.2]. QoS 0 at a full queue is still shed. Within a bound large
// enough that the session gives nothing up, a slow client receives every
// QoS 1 delivery however much QoS 0 was shed around them.
func TestAFullSocketQueueLosesNoQoS1Delivery(t *testing.T) {
	const published = 5000
	brokertest.SessionQueueBytes = 256 << 20
	// **No write deadline inside the hold below.** A socket that took nothing
	// for limits.write_timeout ends its connection, which is another rule;
	// the hold stands in for a socket slower than the flood, not a dead one.
	brokertest.WriteDeadline = 10 * time.Minute
	t.Cleanup(func() { brokertest.SessionQueueBytes, brokertest.WriteDeadline = 0, 0 })
	h := start(t)

	// **The queue is made full by holding the broker's writes, not the
	// client's reads.** Holding reads left the kernel's socket buffers to
	// fill before the queue could, and on a two-core runner the queue never
	// filled: 15,069 QoS 0 in 60s and none shed. The subscriber's server-side
	// connection is the test's, handed to the broker as an embedding caller
	// hands one (EstablishConnection), and its Write waits while held - so
	// nothing leaves the queue for the socket, whatever the kernel would
	// buffer.
	door, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = door.Close() })
	accepted := make(chan *heldConn, 1)
	go func() {
		c, err := door.Accept()
		if err != nil {
			close(accepted)
			return
		}
		hc := &heldConn{Conn: c, release: make(chan struct{})}
		accepted <- hc
		_ = h.Srv.EstablishConnection("t", hc)
	}()
	slow, _ := windowDial(t, door.Addr().String(), "slow", true, 65535)
	held, ok := <-accepted
	if !ok {
		t.Fatal("the subscriber's connection was never accepted")
	}
	t.Cleanup(held.let)
	slow.Subscribe("loose/slow/#", 1)
	held.hold()

	// **QoS 0 is what fills the queue**: QoS 0 at a full queue is shed and
	// counted, which is what proves the queue full. QoS 1 at a full queue
	// waits in the session, which is the case under test.
	//
	// **A slow reader, not a deaf one**, once the writes are let go: 4 KiB a
	// millisecond at most, against publishers several times faster.
	got := make(chan delivered, 1<<20)
	pr, pw := io.Pipe()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	ops := ln.Addr().String()
	_ = ln.Close()
	stopOps, err := h.B.ServeOperations(tcpOnly(ops), time.Nanosecond, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stopOps()
	droppedNow := func() float64 {
		t.Helper()
		v, ok := scrapeGauges(t, ops)["saguin_deliveries_dropped_total"]
		if !ok {
			t.Fatal("the scrape has no saguin_deliveries_dropped_total, so a zero would mean nothing")
		}
		return v
	}
	droppedBefore := droppedNow()

	go func() {
		<-held.release
		buf := make([]byte, 4096)
		for {
			_ = slow.C.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := slow.C.Read(buf)
			if _, werr := pw.Write(buf[:n]); werr != nil || err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer close(got)
		r := bufio.NewReader(pr)
		for {
			cp, err := pahopackets.ReadPacket(r)
			if err != nil {
				return
			}
			if p, ok := cp.Content.(*pahopackets.Publish); ok {
				number, _, _ := strings.Cut(string(p.Payload), " ")
				n, _ := strconv.Atoi(number)
				got <- delivered{ID: p.PacketID, N: n}
			}
		}
	}()

	// The QoS 0 flood runs for as long as the QoS 1 publishes do.
	stop, flooded := make(chan struct{}), make(chan int, 1)
	var floodCount atomic.Int64
	flood := connect(t, h, "slow-flood", true, false)
	go func() {
		sent := 0
		for {
			select {
			case <-stop:
				flooded <- sent
				return
			default:
			}
			if _, err := flood.C.Publish(context.Background(), &paho.Publish{
				Topic: "loose/slow/0", QoS: 0, Payload: []byte("q0 " + strings.Repeat("x", 512))}); err != nil {
				flooded <- sent
				return
			}
			sent++
			floodCount.Store(int64(sent))
			// **Paused now and then, so the flood fills the queue without
			// starving the publisher whose deliveries this is about.**
			// Unthrottled, a QoS 1 publish waited past paho's ten-second
			// default on a CI runner under -race and the test failed there
			// while passing here.
			if sent%256 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Held until the broker reports a QoS 0 delivery shed; failing with the
	// counts if it never does, rather than hanging.
	filled := false
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if droppedNow()-droppedBefore >= 1 {
			filled = true
			break
		}
	}
	if !filled {
		close(stop)
		held.let()
		t.Fatalf("the broker shed no QoS 0 delivery in 60s with its writes to the subscriber held "+
			"(%d flooded, %v dropped, %d writes waiting): the queue never filled",
			floodCount.Load(), droppedNow()-droppedBefore, held.waiting.Load())
	}

	// **Half the QoS 1 publishes are made while the queue is still full**, so
	// each of them finds it full - nothing leaves it while the writes are
	// held - and the other half while the slow reader drains it.
	p := connect(t, h, "slow-pub", true, false)
	publish := func(from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			if _, err := p.C.Publish(context.Background(), &paho.Publish{
				Topic: "loose/slow/1", QoS: 1, Payload: numbered(i+1, 512)}); err != nil {
				close(stop)
				held.let()
				t.Fatalf("publish %d: %v", i, err)
			}
		}
	}
	publish(0, published/2)
	// The instrument proves it held what it claims: the broker's writer was
	// waiting on the hold, and the QoS 1 deliveries made meanwhile are in the
	// subscriber's session rather than lost or written.
	if held.waiting.Load() == 0 {
		t.Fatal("no write to the subscriber was waiting on the hold, so the queue was not held")
	}
	// saguin_session_queue_messages: what sessions hold unacknowledged, on
	// the wire or waiting. The subscriber's is the only session owed anything.
	if v, ok := scrapeGauges(t, ops)["saguin_session_queue_messages"]; !ok || v < published/2 {
		t.Fatalf("sessions hold %v deliveries (scraped: %v) after %d QoS 1 publishes made with the "+
			"subscriber's queue full, want at least that many", v, ok, published/2)
	}
	held.let()
	publish(published/2, published)
	close(stop)
	floodSent := <-flooded

	seen, floodGot := map[int]bool{}, 0
	for _, m := range drainDelivered(got, 3*time.Second) {
		if m.ID == 0 {
			floodGot++
			continue
		}
		seen[m.N] = true
	}
	t.Logf("QoS 0: %d of %d received; QoS 1: %d of %d distinct received", floodGot, floodSent, len(seen), published)
	// Proof the case was reached: QoS 0 is shed only at a full queue.
	if floodGot >= floodSent {
		t.Fatalf("no QoS 0 delivery was shed (%d of %d received; the broker counted %v dropped): the queue never filled, so this test did not reach the case",
			floodGot, floodSent, droppedNow()-droppedBefore)
	}
	if len(seen) != published {
		t.Errorf("a slow client received %d of %d QoS 1 deliveries around a full socket queue, within a bound that gives nothing up",
			len(seen), published)
	}
}

// RFC 0002 `broker.session` and RFC 0004 "Memory, with a snapshot": a session
// and the messages it is owed survive exactly what its provider survives. A
// memory provider keeps them in the file it writes on the way out and nowhere
// else, so a graceful stop keeps them and a crash does not; a sqlite provider
// keeps them in its database, which survives both.
//
// Driven from the wire, so what is being restarted is a session a client
// made rather than rows a test wrote.
func TestSessionsSurviveWhatTheirProviderSurvives(t *testing.T) {
	// leave puts a persistent session on the broker with a subscription and
	// one message owed to it, and its client away.
	leave := func(t *testing.T, h *harness) {
		t.Helper()
		away, _ := windowDial(t, h.Addr, "keeper", false, 100)
		away.Subscribe("backlog/+", 1)
		away.Close()
		sessionGone(t, h, "keeper")

		p := connect(t, h, "producer", true, false)
		pa, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "backlog/1", QoS: 1, Payload: []byte("owed")})
		if err != nil || pa.ReasonCode != 0 {
			t.Fatalf("the publish was not accepted: %v (reason %v)", err, pa)
		}
		settle(t, p)
		awaitOwed(t, operationsAt(t, h), "the one message the offline session is owed", 1)
	}
	// expect reads the session store the restarted broker is using, and what
	// that broker says the session is owed.
	expect := func(t *testing.T, h *harness, kept bool) {
		t.Helper()
		sess, ok, err := brokertest.HarnessSessions.Get("keeper")
		if err != nil {
			t.Fatalf("read the session store after the restart: %v", err)
		}
		if ok != kept {
			t.Fatalf("the session is there: %v after the restart, want %v", ok, kept)
		}
		msgs := logged(t)
		if !kept {
			if len(msgs) != 0 {
				t.Errorf("%d messages survived a restart that keeps nothing", len(msgs))
			}
			awaitOwed(t, operationsAt(t, h), "nothing, after a restart that keeps nothing", 0)
			return
		}
		awaitOwed(t, operationsAt(t, h), "the one message, after the restart", 1)
		if len(sess.Subscriptions) != 1 || sess.Subscriptions[0].Filter != "backlog/+" ||
			sess.Subscriptions[0].QoS != 1 {
			t.Errorf("the session came back with %+v, want its one subscription to backlog/+ at QoS 1",
				sess.Subscriptions)
		}
		if len(msgs) != 1 || string(msgs[0].Payload) != "owed" {
			t.Fatalf("the log came back holding %d messages (%+v), want the one the session was owed", len(msgs), msgs)
		}
		if msgs[0].QoS != 1 || msgs[0].Topic != "backlog/1" {
			t.Errorf("the message came back as %+v, want the QoS 1 publish to backlog/1", msgs[0])
		}
	}

	t.Run("memory keeps them across a graceful stop", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)
		leave(t, h)
		h.Stop()
		expect(t, startDurable(t, dir), true)
	})
	t.Run("memory loses them in a crash", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)
		leave(t, h)
		h.Crash()
		expect(t, startDurable(t, dir), false)
	})
	t.Run("sqlite keeps them across a graceful stop", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		h := startDurableSQLite(t, path)
		leave(t, h)
		h.Stop()
		expect(t, startDurableSQLite(t, path), true)
	})
	t.Run("sqlite keeps them across a crash", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		h := startDurableSQLite(t, path)
		leave(t, h)
		h.Crash()
		expect(t, startDurableSQLite(t, path), true)
	})
}

// RFC 0002 `broker.session`: a session the store kept while the broker was
// stopped is put back as a session the broker holds and nothing is connected
// to. Its subscriptions are live while it is away, so a broadcast published
// after the restart is held for it; and the client that comes back is
// answered Session Present 1 and sent everything it was owed, in order,
// whichever side of the restart it was published on.
func TestARestoredSessionIsResumedWithItsBacklog(t *testing.T) {
	restart := func(t *testing.T, provider string) (*harness, func() *harness) {
		t.Helper()
		switch provider {
		case "memory":
			dir := t.TempDir()
			return startDurable(t, dir), func() *harness { return startDurable(t, dir) }
		default:
			path := filepath.Join(t.TempDir(), "s.db")
			return startDurableSQLite(t, path), func() *harness { return startDurableSQLite(t, path) }
		}
	}
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			h, again := restart(t, provider)

			w, _ := windowDial(t, h.Addr, "keeper", false, 100)
			w.Subscribe("backlog/+", 1)
			w.Close()
			sessionGone(t, h, "keeper")

			p := connect(t, h, "producer", true, false)
			for i := 1; i <= 3; i++ {
				pa, err := p.C.Publish(context.Background(), &paho.Publish{
					Topic: fmt.Sprintf("backlog/%d", i), QoS: 1, Payload: []byte(fmt.Sprintf("before-%d", i))})
				if err != nil || pa.ReasonCode != 0 {
					t.Fatalf("publish %d was not accepted: %v (%v)", i, err, pa)
				}
			}
			settle(t, p)
			awaitOwed(t, operationsAt(t, h), "the three published before the restart", 3)

			h.Stop()
			h2 := again()
			ops2 := operationsAt(t, h2)

			// The session is back, and nothing is connected to it.
			cl, ok := h2.Srv.Clients.Get("keeper")
			if !ok {
				t.Fatal("the session was not restored, so nothing below is about a restored one")
			}
			if !cl.Closed() {
				t.Error("the restored session reads as connected, and no client is")
			}
			awaitOwed(t, ops2, "the 3 the restored session was owed", 3)

			// Its subscription is live while it is still away.
			p2 := connect(t, h2, "producer", true, false)
			pa, err := p2.C.Publish(context.Background(), &paho.Publish{
				Topic: "backlog/4", QoS: 1, Payload: []byte("after-4")})
			if err != nil || pa.ReasonCode != 0 {
				t.Fatalf("the publish after the restart was not accepted: %v (%v)", err, pa)
			}
			settle(t, p2)
			awaitOwed(t, ops2, "a fourth, published after the restart", 4)

			// And the client comes back to Session Present 1 and its backlog.
			w2, ca := windowDial(t, h2.Addr, "keeper", false, 100)
			if !ca.SessionPresent {
				t.Fatal("the client that came back was told Session Present 0, so its session did not survive")
			}
			var got []string
			for {
				cp, err := w2.Read(2 * time.Second)
				if err != nil {
					break
				}
				if pub, ok := cp.Content.(*pahopackets.Publish); ok {
					got = append(got, string(pub.Payload))
					w2.Puback(pub.PacketID)
				}
			}
			want := []string{"before-1", "before-2", "before-3", "after-4"}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("the resumed session received %v, want %v", got, want)
			}
			awaitOwed(t, ops2, "nothing, now that every delivery is acknowledged", 0)
		})
	}
}

// RFC 0002 `broker.session` and RFC 0003 "`append` - Durable consumers",
// docs/invariants.md 1: a consumer whose session the store kept across a
// restart comes back without subscribing and is sent what is published
// after it did.
//
// **The test above cannot see this half.** Its backlog is replayed from
// the stored position, which works whether or not the restored consumer is
// indexed for new records - measured: it passed with reindex removed from
// track. A record published after the consumer comes back reaches it only
// if pumpAll finds it through byTopic, and byTopic only holds it if the
// subscriptions the store kept went back into the substrate at restore and
// out again through OnSessionEstablished into track. Break that chain on
// the restore side and the consumer is told Session Present 1, holds its
// position, and is never sent another record.
func TestARestoredConsumerIsSentWhatIsPublishedAfterItComesBack(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			var again func() *harness
			if provider == "memory" {
				dir := t.TempDir()
				h, again = startDurable(t, dir), func() *harness { return startDurable(t, dir) }
			} else {
				path := filepath.Join(t.TempDir(), "s.db")
				h, again = startDurableSQLite(t, path), func() *harness { return startDurableSQLite(t, path) }
			}

			c := connect(t, h, "keeper", false, false)
			c.Sub(t, "events/#", 1)
			c.Close()
			sessionGone(t, h, "keeper")
			h.Stop()

			h2 := again()
			if _, ok := h2.Srv.Clients.Get("keeper"); !ok {
				t.Fatal("the session was not restored, so nothing below is about a restored one")
			}
			c2 := connect(t, h2, "keeper", false, false)
			if !c2.SessionPresent {
				t.Fatal("the consumer that came back was told Session Present 0, so its session did not survive")
			}

			p := connect(t, h2, "producer", true, false)
			p.Pub(t, "events/after", "published after it came back")
			r, ok := c2.Await(t, 3*time.Second)
			if !ok {
				t.Fatal("the restored consumer was sent nothing published after it came back: it was " +
					"told its session was present, and it will never be sent another record")
			}
			if r.Topic != "events/after" || r.Payload != "published after it came back" {
				t.Fatalf("the restored consumer was sent %s %q, want events/after", r.Topic, r.Payload)
			}
		})
	}
}

// RFC 0003 "`append` - Durable consumers", docs/invariants.md 1: a consumer
// on a narrow filter that was away across a restart is sent every record
// its filter matched while it was away, and nothing else.
//
// **What this guards is where a consumer may step over records unread.**
// A pending list says which records a consumer's filters matched, and
// everything else in the range it covers is stepped over. The list is not
// stored, so after a restart it covers nothing below where the broker
// started - and a list that claimed to would step over the whole backlog,
// matched records included, in silence.
func TestARestartedNarrowConsumerIsSentWhatItMatchedWhileAway(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			var again func() *harness
			if provider == "memory" {
				dir := t.TempDir()
				h, again = startDurable(t, dir), func() *harness { return startDurable(t, dir) }
			} else {
				path := filepath.Join(t.TempDir(), "s.db")
				h, again = startDurableSQLite(t, path), func() *harness { return startDurableSQLite(t, path) }
			}

			c := connect(t, h, "narrow", false, false)
			c.Sub(t, "events/a/#", 1)
			p := connect(t, h, "producer", true, false)
			p.Pub(t, "events/a/0", "before")
			if r, ok := c.Await(t, 3*time.Second); !ok || r.Payload != "before" {
				t.Fatalf("before going away the consumer was sent %v %v, want before", r, ok)
			}
			// **Gone once its acknowledgement is stored, not once it has the
			// record.** The client acknowledges after its handler returns, so
			// a DISCONNECT sent the moment the record arrives can reach the
			// broker first - and then the record is rightly sent again after
			// the restart, at least once. What this test is about is what
			// the consumer was sent *while away*, so it leaves having
			// acknowledged what it had: offset 1, stored as position 2.
			awaitStoredPosition(t, h, "events", 2, 5*time.Second)
			c.Close()
			sessionGone(t, h, "narrow")

			// Away: mostly records it declines, a few it matches.
			var want []string
			for i := 1; i <= 60; i++ {
				if i%15 == 0 {
					payload := fmt.Sprintf("away-%d", i)
					p.Pub(t, fmt.Sprintf("events/a/%d", i), payload)
					want = append(want, payload)
				} else {
					p.Pub(t, fmt.Sprintf("events/b/%d", i), "declined")
				}
			}
			h.Stop()

			h2 := again()
			c2 := connect(t, h2, "narrow", false, false)
			if !c2.SessionPresent {
				t.Fatal("the consumer that came back was told Session Present 0, so nothing below is about a resumed one")
			}
			p2 := connect(t, h2, "producer", true, false)
			p2.Pub(t, "events/b/after", "declined")
			p2.Pub(t, "events/a/after", "after")
			want = append(want, "after")

			var got []string
			for {
				r, ok := c2.Await(t, 2*time.Second)
				if !ok {
					break
				}
				got = append(got, r.Payload)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("after the restart the consumer was sent %v, want %v: records its filter "+
					"matched while it was away were stepped over, or something else was sent",
					got, want)
			}
		})
	}
}

// RFC 0003 "Broadcast", the cursor-and-window rule: a restored session is held to
// the Receive Maximum of the connection that comes back for it, exactly as a
// resumed one is. Its backlog was written by a broker that is gone, so the
// window it is sent under is the new connection's own - there is no earlier
// window to inherit, and a restart must not be the moment a device is handed
// its whole backlog at once.
func TestARestoredSessionIsSentNoMoreThanItsReceiveMaximum(t *testing.T) {
	const window, backlog = 2, 10
	dir := t.TempDir()
	h := startDurable(t, dir)

	first, _ := windowDial(t, h.Addr, "slow-reader", false, window)
	first.Subscribe("backlog/+", 1)
	first.Close()
	sessionGone(t, h, "slow-reader")

	p := connect(t, h, "producer", true, false)
	for i := range backlog {
		p.Pub(t, fmt.Sprintf("backlog/%d", i), fmt.Sprintf("m-%d", i))
	}
	awaitOwed(t, operationsAt(t, h), "the whole backlog, so the restart has one to restore", backlog)

	h.Stop()
	h2 := startDurable(t, dir)
	awaitOwed(t, operationsAt(t, h2), "the backlog the restored session is owed", backlog)

	w, ca := windowDial(t, h2.Addr, "slow-reader", false, window)
	if !ca.SessionPresent {
		t.Fatal("the client that came back was told Session Present 0")
	}
	seen := map[string]int{}
	var unacked []uint16
	readBurst := func() {
		for {
			cp, err := w.Read(700 * time.Millisecond)
			if err != nil {
				return
			}
			if pub, ok := cp.Content.(*pahopackets.Publish); ok {
				seen[string(pub.Payload)]++
				unacked = append(unacked, pub.PacketID)
				if len(unacked) > window {
					t.Fatalf("the broker had %d publishes unacknowledged with a Receive Maximum of %d",
						len(unacked), window)
				}
			}
		}
	}
	readBurst()
	if len(unacked) == 0 {
		t.Fatal("the restored session was sent nothing, so this proves nothing about a window")
	}
	for deadline := time.Now().Add(10 * time.Second); len(seen) < backlog && time.Now().Before(deadline); {
		if len(unacked) == 0 {
			readBurst()
			if len(unacked) == 0 {
				break
			}
		}
		w.Puback(unacked[0])
		unacked = unacked[1:]
		readBurst()
	}
	if len(seen) != backlog {
		t.Errorf("the restored session received %d of %d restored deliveries, then nothing: %v",
			len(seen), backlog, seen)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Errorf("%s arrived %d times on one connection", payload, n)
		}
	}
}

// RFC 0003 "How a stored position is dropped": a `latest` consumer's position
// is held in memory and a restart takes it, so a restored session is served
// the whole of current state when it comes back rather than left stale. That
// is what the document already promises a client after a restart; what has
// changed is only how it gets there - it is answered Session Present 1 and
// served on the resume, rather than Session Present 0 and served on the
// SUBSCRIBE it then sends.
//
// **Stale is the failure this guards.** A restored session sends no
// SUBSCRIBE, so nothing else would ever serve it: a viewer would come back
// holding a subscription and showing whatever it had before the restart,
// with nothing to say the state had moved.
func TestARestoredLatestConsumerIsServedCurrentState(t *testing.T) {
	dir := t.TempDir()
	h := startDurable(t, dir)

	viewer := connect(t, h, "viewer", false, false)
	viewer.Sub(t, "state/+", 1)
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "state/a", "a-1")
	p.Pub(t, "state/b", "b-1")
	first := map[string]string{}
	for range 2 {
		r, ok := viewer.Await(t, 3*time.Second)
		if !ok {
			t.Fatalf("the viewer was served %v before the restart, want both values", first)
		}
		first[r.Topic] = r.Payload
	}
	viewer.Close()

	h.Stop()
	h2 := startDurable(t, dir)

	// Moved while it was away, and one of the values it had already seen
	// changed: what it is served has to be current rather than what it left.
	p2 := connect(t, h2, "producer", true, false)
	p2.Pub(t, "state/a", "a-2")
	p2.Pub(t, "state/c", "c-1")

	again := connect(t, h2, "viewer", false, false)
	got := map[string]string{}
	for {
		r, ok := again.Await(t, 1*time.Second)
		if !ok {
			break
		}
		got[r.Topic] = r.Payload
	}
	want := map[string]string{"state/a": "a-2", "state/b": "b-1", "state/c": "c-1"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the restored viewer was served %v on its resume, want the whole of current "+
			"state %v: it holds its subscription and sends no SUBSCRIBE, so this is the only "+
			"moment anything serves it", got, want)
	}
}

// Invariant 15, "Restart never resurrects a delivery as live", now that
// sessions do come back: what comes back is a session's own messages, and a
// channel's records are not among them. A channel keeps its records and the
// session holds a position in them; a queue keeps its work and hands it out
// under a lease. Neither is session state, so neither is restored - the
// consumer resumes from its stored position and the queue offers its work
// again, both by their own paths.
//
// **The failure this guards is a delivery coming back twice over.** A record
// restored into a session and also replayed from a position is the same
// record delivered from two places, with a lease that expired on a broker
// that no longer exists.
func TestARestartRestoresNoChannelDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	h := startDurableSQLite(t, path)

	consumer := connect(t, h, "consumer", false, true)
	consumer.Sub(t, "events/#", 1)
	worker := connect(t, h, "worker", false, true)
	worker.Sub(t, "$saguin/queue/jobs", 1)

	p := connect(t, h, "producer", true, false)
	p.Pub(t, "events/orders/1", "record-1")
	p.Pub(t, "jobs/a", "job-1")
	if r, ok := consumer.Await(t, 3*time.Second); !ok || r.Payload != "record-1" {
		t.Fatalf("the consumer was served %v (%v), want the record it is about to leave unacknowledged", r, ok)
	}
	if r, ok := worker.Await(t, 3*time.Second); !ok || r.Payload != "job-1" {
		t.Fatalf("the worker was offered %v (%v), want the job it is about to leave unacknowledged", r, ok)
	}
	consumer.Close()
	worker.Close()
	sessionGone(t, h, "consumer")
	sessionGone(t, h, "worker")

	h.Stop()
	h2 := startDurableSQLite(t, path)

	for _, id := range []string{"consumer", "worker"} {
		cl, ok := h2.Srv.Clients.Get(id)
		if !ok {
			t.Fatalf("the session for %s did not come back, so this says nothing about what it holds", id)
		}
		if n := cl.State.Inflight.Len(); n != 0 {
			t.Errorf("the restored session for %s holds %d deliveries in flight, want none: a "+
				"channel's records and a queue's work are not session state", id, n)
		}
		tables := brokertest.HarnessSessions.(interface {
			InFlight(string) (uint16, []store.InFlight, error)
		})
		if _, table, err := tables.InFlight(id); err != nil || len(table) != 0 {
			t.Errorf("the session store holds %+v in flight for %s (%v), want none", table, id, err)
		}
	}

	// And nothing is lost: each comes back by its own path, once.
	back := connect(t, h2, "consumer", false, true)
	if r, ok := back.Await(t, 5*time.Second); !ok || r.Payload != "record-1" {
		t.Errorf("the consumer was served %v (%v) after the restart, want its record replayed "+
			"from the position it never acknowledged past", r, ok)
	}
	again := connect(t, h2, "worker", false, true)
	if r, ok := again.Await(t, 10*time.Second); !ok || r.Payload != "job-1" {
		t.Errorf("the worker was offered %v (%v) after the restart, want the job it left "+
			"unacknowledged, offered again by the queue", r, ok)
	}
}

// RFC 0005 `saguin_sessions_restored_total` and `saguin_sessions_offline`:
// what the last start did to a fleet's sessions, and what it is holding now.
//
// **The gauge's own words are what is checked here.** It says "sessions this
// broker holds that nothing is connected to", and it is derived by walking
// the client table - a restored session enters that table by a path that did
// not exist when the gauge was written, so whether it counts was worth
// running rather than assuming. It does, which is what the sentence promises:
// a device that has not come back yet is a session held, however this broker
// came to hold it.
func TestARestartIsVisibleInTheSessionMetrics(t *testing.T) {
	dir := t.TempDir()
	h := startDurable(t, dir)

	away, _ := windowDial(t, h.Addr, "counted", false, 100)
	away.Subscribe("backlog/+", 1)
	away.Close()
	sessionGone(t, h, "counted")

	h.Stop()
	h2 := startDurable(t, dir)
	ops := operationsAt(t, h2)

	got := scrapeGauges(t, ops)
	if n := got["saguin_sessions_restored_total"]; n != 1 {
		t.Errorf("saguin_sessions_restored_total is %v after a start that put one session back, want 1", n)
	}
	if n := got["saguin_sessions_offline"]; n != 1 {
		t.Errorf("saguin_sessions_offline is %v, want 1: a restored session nothing is connected "+
			"to is a session held, which is what the series says it counts", n)
	}
	if n := got[`saguin_sessions_dropped_total{cause="subscription_refused"}`]; n != 0 {
		t.Errorf("subscription_refused counts %v on a start that refused nothing", n)
	}
}

// RFC 0002 `broker.session`: a Will is session state, so it is kept where the
// operator said session state is kept - on both providers, counted against
// that provider's bound, and gone when the session that armed it ends.
//
// **A session that ends with its connection is written for a Will**, where it
// is written for nothing else until its first SUBSCRIBE: a Will is something
// the session holds from its CONNECT, and an operator who named a provider
// for session state named it for this.
func TestAWillIsKeptWithItsSession(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		conn := connectWithWillProps(t, h, "armed", "loose/armed/gone", 0)

		sess, ok, err := brokertest.HarnessSessions.Get("armed")
		if err != nil || !ok {
			t.Fatalf("a client that armed a Will has no session in the store: %v, %v", ok, err)
		}
		w := sess.Will
		if w == nil {
			t.Fatal("the session holds no Will, so the broker is keeping it in memory alone " +
				"and a restart inside its delay would lose it")
		}
		if w.Topic != "loose/armed/gone" {
			t.Errorf("the Will was kept for topic %q, want loose/armed/gone", w.Topic)
		}
		if len(w.Payload) == 0 {
			t.Error("the Will was kept with no payload")
		}
		// The properties MQTT lets a Will carry are kept with it: they are
		// what the subscriber is served, and the connection that armed them
		// is gone by the time the Will fires.
		if w.Props.ContentType == "" || w.Props.ResponseTopic == "" || len(w.Props.CorrelationData) == 0 {
			t.Errorf("the Will was kept without its properties: %+v", w.Props)
		}
		if !w.DueAt.IsZero() {
			t.Errorf("the Will is due at %v while its client is connected, want no moment at all", w.DueAt)
		}

		// It is gone with the session it belonged to. This connection ends
		// without a DISCONNECT, so the Will fires - and a Will fires once.
		kill(t, conn)
		sessionGone(t, h, "armed")
		if sess, ok, _ := brokertest.HarnessSessions.Get("armed"); ok && sess.Will != nil {
			t.Errorf("the Will is still held after it fired: %+v", sess.Will)
		}
	})
}

// A Will belongs to the session that armed it, so a session that ends takes
// it: MQTT publishes no Will for a client that said goodbye
// (MQTT-3.1.2-10), and one whose connection never completed leaves nothing
// behind at all.
func TestAWillGoesWithTheSessionThatArmedIt(t *testing.T) {
	h := startDurable(t, t.TempDir())

	// A connection that never completes: the CONNECT is written and the
	// socket is cut before the broker can answer it. Nothing it wrote may
	// outlive it - the client was never told it was connected, so it has no
	// session and no Will.
	//
	// **Driven until it is observed rather than once**, because whether the
	// CONNACK write loses the race is timing: a run where it won proves
	// nothing, and one that is never run proves less.
	tried, never := 0, 0
	for ; tried < 200; tried++ {
		id := fmt.Sprintf("half-connected-%03d", tried)
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if _, err := conn.Write(willConnectBytes(id, "loose/half/"+id, 0)); err != nil {
			_ = conn.Close()
			continue
		}
		kill(t, conn)
		// **Polled rather than read once, and polled on the store itself.**
		// A connection whose CONNACK never went out was never in the client
		// table, so its absence says nothing about whether the broker has
		// finished with it - and the disconnect hook is the thing under
		// test here, so waiting on that would be waiting on the answer.
		var left store.Session
		var held bool
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
			var err error
			left, held, err = brokertest.HarnessSessions.Get(id)
			if err != nil {
				t.Fatalf("read the session store: %v", err)
			}
			if !held {
				never++
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s left a session behind: %+v - a connection that never completed "+
					"must leave nothing, or every half-open connect fills the store "+
					"with a Will nobody will ever publish", id, left)
			}
		}
	}
	if never != tried {
		t.Fatalf("%d of %d cut connections left nothing", never, tried)
	}
	t.Logf("%d connections cut before their CONNACK, none left a session or a Will", tried)
}

// RFC 0002 `broker.session`: a Will the session store has no room for
// refuses the connection with `0x97`, where a *session* it has no room for is
// accepted and told it ends with its connection.
//
// **The difference is what MQTT lets a server say.** A session ending with
// its connection is stated in the CONNACK's Session Expiry Interval, so the
// client knows; there is no property for "your Will is not held", so a device
// that armed one and was accepted would go on believing the broker would
// speak for it if it died. Refusing it is the only honest answer, and it is
// one a client can act on - it knows its Will matters, the broker does not.
//
// **The store is made to refuse rather than filled**, and the two are not the
// same test: when a provider is full is the provider's arithmetic, tested
// where each provider is tested, and a sqlite one is bounded by pages - so a
// store that has just refused one row may still have room for another shape,
// and a test that filled it would be asking the broker about a store with
// room. What is under test here is what the broker does with the refusal.
func TestAWillTheStoreHasNoRoomForRefusesTheConnection(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		ops := operationsAt(t, h)

		// A Will accepted while there is room, so the refusal below is about
		// the room rather than about the Will.
		first := connectWithWillProps(t, h, "armed-early", "loose/armed/early", 0)
		if sess, ok, _ := brokertest.HarnessSessions.Get("armed-early"); !ok || sess.Will == nil {
			t.Fatal("a Will was not kept while the store had room, so this test cannot " +
				"show one being refused for want of it")
		}
		kill(t, first)

		h.B.SetSessions("local", fullSessions{brokertest.HarnessSessions})

		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		if _, err := conn.Write(willConnectBytes("armed-late", "loose/armed/late", 0)); err != nil {
			t.Fatalf("write connect: %v", err)
		}
		// Read as a packet rather than as two bytes and a body: this CONNACK
		// carries a Reason String saying what filled, so its remaining length
		// runs past 127 and is two bytes wide. A reader that assumes one
		// reads the refusal as a success.
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		cp, err := pahopackets.ReadPacket(conn)
		if err != nil {
			t.Fatalf("read connack: %v", err)
		}
		ca, ok := cp.Content.(*pahopackets.Connack)
		if !ok {
			t.Fatalf("the broker answered a %s, want a CONNACK", cp.PacketType())
		}
		if ca.ReasonCode != 0x97 {
			t.Fatalf("a Will the store had no room for was answered 0x%02X, want 0x97 "+
				"(Quota exceeded): the client believes the broker will speak for it", ca.ReasonCode)
		}
		// And it is told which quota, in the one place MQTT leaves for saying
		// so: the client cannot act on "0x97" alone.
		if ca.Properties == nil || !strings.Contains(ca.Properties.ReasonString, "broker.session.storage") {
			t.Errorf("the refusal carries no reason naming what filled: %+v", ca.Properties)
		}
		if _, ok, _ := brokertest.HarnessSessions.Get("armed-late"); ok {
			t.Error("a refused connection left a session in the store")
		}
		// And the operator is told which of the four things 0x97 means on
		// this broker, rather than an undifferentiated quota series.
		if got := scrapeGauges(t, ops)[`saguin_connections_refused_total{reason="session store full"}`]; got != 1 {
			t.Errorf(`saguin_connections_refused_total{reason="session store full"} is %v, want 1`, got)
		}
	})
}

// fullSessions is a session store with no room for anything new, which is
// what a provider at its max_bytes answers. Everything else is the store it
// wraps, so what a test reads back is what the broker actually kept.
type fullSessions struct{ broker.SessionStore }

func (fullSessions) Save(store.Session) error { return store.ErrFull }

// Begin keeps a new session's record as Save does, so it has no room either;
// an ending that keeps nothing still goes through.
func (s fullSessions) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if next != nil {
		return store.Dropped{}, store.ErrFull
	}
	return s.SessionStore.Begin(client, held, nil)
}

// RFC 0002 `broker.session`: a Will waiting out its delay lives in the
// provider the operator named and nowhere else. What the broker keeps in
// memory is a client id, the moment the Will is due, and a timer; the message
// itself is read back once, when that moment arrives.
//
// **The second copy is what this proves gone**, and it proves it the only way
// a test can: by taking the Will out of the store while it waits. A broker
// holding one in memory publishes anyway; one that does not, publishes
// nothing. The control beside it is the same wait left alone, so a silent
// test cannot pass by never having armed anything.
func TestAWaitingWillIsHeldOnlyInTheStore(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		watch := connect(t, h, "watcher", true, false)
		watch.Sub(t, "loose/#", 1)
		subscribed(t, h, watch, "loose/subscribed")

		// Taken out of the store while it waits: nothing is published.
		gone := willConnectWithExpiry(t, h, "vanishing", "loose/will/vanishing", 3, 300)
		awaitRegistered(t, h, gone, "vanishing")
		kill(t, gone)
		waitForStoredWill(t, "vanishing")
		sess, _, err := brokertest.HarnessSessions.Get("vanishing")
		if err != nil {
			t.Fatal(err)
		}
		sess.Will = nil
		if err := brokertest.HarnessSessions.Save(sess); err != nil {
			t.Fatal(err)
		}
		if r, ok := watch.Await(t, 6*time.Second); ok {
			t.Errorf("%q was published for a Will the store no longer holds: the broker is "+
				"keeping a second copy of a waiting Will in memory", r.Topic)
		}

		// And the control: the same wait, left where it lives, arrives.
		kept := willConnectWithExpiry(t, h, "dying", "loose/will/dying", 3, 300)
		awaitRegistered(t, h, kept, "dying")
		kill(t, kept)
		due := waitForStoredWill(t, "dying")
		if until := time.Until(due); until < time.Second || until > 4*time.Second {
			t.Errorf("the Will is due in %v, want about three seconds: the moment on the "+
				"record is what a restart reads", until.Round(time.Millisecond))
		}
		r, ok := watch.Await(t, 8*time.Second)
		if !ok {
			t.Fatal("the Will that was left in the store was never published")
		}
		if r.Topic != "loose/will/dying" || r.Payload != "i-died" {
			t.Errorf("published %q %q, want the Will", r.Topic, r.Payload)
		}
		// A Will fires once, so the record no longer holds it - and the
		// session it belonged to is still there, because a Will firing is not
		// what ends one.
		if sess := awaitWillOffRecord(t, "dying"); sess == nil {
			t.Fatal("the session is gone after its Will fired")
		}
	})
}

// [MQTT-3.1.3-9]: a client that comes back inside the delay, resuming its
// session, is not announced dead. The timer goes, and so does the Will on its
// session record - which is where it lives, so leaving it would be a Will with
// a moment in the past for the next start to find and publish. A clean start
// is not this: it ends the session, and a waiting Will is published (RFC 0003
// "Sessions"; TestAWillIsItsOwnSessionsAcrossATakeover).
//
// **And the connection that cancels it may arm a Will of its own**, which is
// the ordering this pins: the cancel must not take the Will the new CONNECT
// has just written, or a device that flaps and re-arms comes back with no
// Will at all and nothing saying so. It came back with none, on a resume,
// while this reconnected with a clean start and could not see it.
func TestAReconnectTakesAWaitingWillOffItsSession(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		watch := connect(t, h, "watcher", true, false)
		watch.Sub(t, "loose/#", 1)
		subscribed(t, h, watch, "loose/subscribed")

		first := willConnectWithExpiry(t, h, "flapper", "loose/will/first", 30, 300)
		awaitRegistered(t, h, first, "flapper")
		kill(t, first)
		waitForStoredWill(t, "flapper")

		// Back inside the delay, resuming, and arming a Will of its own.
		second := willResumeWithExpiry(t, h, "flapper", "loose/will/second", 30, 300)
		defer second.Close()
		// The resume's CONNECT has written its own Will by the time it is read.
		waitUntil(5*time.Second, func() bool {
			sess, ok, err := brokertest.HarnessSessions.Get("flapper")
			return err == nil && ok && sess.Will != nil && sess.Will.Topic == "loose/will/second"
		})

		sess, ok, err := brokertest.HarnessSessions.Get("flapper")
		if err != nil || !ok {
			t.Fatalf("the returning client has no session: %v, %v", ok, err)
		}
		if sess.Will == nil {
			t.Fatal("the connection that came back armed a Will and its session holds none: " +
				"the cancel took the Will the new CONNECT had just written")
		}
		if sess.Will.Topic != "loose/will/second" {
			t.Errorf("the session holds a Will for %q, want the one this connection armed",
				sess.Will.Topic)
		}
		if !sess.Will.DueAt.IsZero() {
			t.Errorf("the new Will is due at %v while its client is connected", sess.Will.DueAt)
		}
		if r, ok := watch.Await(t, 2*time.Second); ok {
			t.Errorf("%q was published for a device that had already reconnected", r.Topic)
		}
	})
}

// awaitFloorPast waits until retention has moved channel's floor - the oldest
// offset still readable - past pos, the position a consumer stored. The size
// sweep runs on the broker's own second; what a test needs is what it
// removed, read off the floor rather than slept through.
func awaitFloorPast(t *testing.T, ops, channel string, pos float64) {
	t.Helper()
	series := fmt.Sprintf("saguin_channel_floor_offset{channel=%q}", channel)
	if !waitUntil(5*time.Second, func() bool { return scrapeGauges(t, ops)[series] > pos }) {
		t.Fatalf("%s is %v five seconds after the records that push it, want past %v: retention "+
			"never passed the position, so nothing below is about a consumer below the floor",
			series, scrapeGauges(t, ops)[series], pos)
	}
}

// awaitRegistered waits until the server holds conn as id's connection.
//
// **A CONNACK is written before the connection is registered**, on purpose
// (MQTT-3.2.0-1, and brokertest's Dial says the rest), so a socket cut or a
// stop straight after one can reach a connection the broker has not finished
// taking on. A pause after the CONNACK stood in for this.
func awaitRegistered(t *testing.T, h *harness, conn net.Conn, id string) {
	t.Helper()
	local := conn.LocalAddr().String()
	if !waitUntil(5*time.Second, func() bool {
		cl, ok := h.Srv.Clients.Get(id)
		return ok && cl.Net.Remote == local
	}) {
		t.Fatalf("%s from %s was not registered 5s after its CONNACK", id, local)
	}
}

// subscribed proves a watcher's subscription is live by a marker published
// to topic, which it must be sent, rather than by a pause after its SUBACK.
func subscribed(t *testing.T, h *harness, watch *client, topic string) {
	t.Helper()
	connect(t, h, "subscribed-"+watch.ID, true, false).Pub(t, topic, "subscribed")
	if r, ok := watch.Await(t, 5*time.Second); !ok || r.Payload != "subscribed" {
		t.Fatalf("the watcher was sent %q (%v), want the marker that says it is subscribed",
			r.Payload, ok)
	}
}

// awaitInFlight waits until the session store holds at least n in-flight
// entries for client: a delivery is stored by the session's drain after it
// is written, so a client having read it says nothing about the store.
func awaitInFlight(t *testing.T, client string, n int) {
	t.Helper()
	s, ok := brokertest.HarnessSessions.(interface {
		InFlight(string) (uint16, []store.InFlight, error)
	})
	if !ok {
		t.Fatalf("the harness's session store is a %T, which keeps no in-flight table",
			brokertest.HarnessSessions)
	}
	if !waitUntil(5*time.Second, func() bool {
		_, table, err := s.InFlight(client)
		return err == nil && len(table) >= n
	}) {
		_, table, err := s.InFlight(client)
		t.Fatalf("the store holds %d in-flight entries for %s (%v), want %d", len(table), client, err, n)
	}
}

// waitForStoredWill waits until a session's Will carries the moment it is
// due, and returns it. A test that read the record before the disconnect had
// finished would read the Will as its client armed it, with no moment on it.
func waitForStoredWill(t *testing.T, client string) time.Time {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		sess, ok, err := brokertest.HarnessSessions.Get(client)
		if err != nil {
			t.Fatalf("read the session store: %v", err)
		}
		if ok && sess.Will != nil && !sess.Will.DueAt.IsZero() {
			return sess.Will.DueAt
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has no waiting Will on its session five seconds after its link was cut "+
				"(session: %v, will: %+v)", client, ok, sess.Will)
		}
	}
}

// RFC 0003 "The delay is Sagüin's": a Will waiting out its delay when the
// broker stops is published when the broker comes back, if its moment passed
// while it was down, and goes on waiting for what is left of it if not. Nothing read the moment back, so a restart inside
// a delay meant the announcement was never made at all - durable and mute.
//
// **The three cases are one rule at three moments**, and the third is the one
// that says the broker stopping is not a device dying.
func TestARestartPublishesTheWillsItFindsOwed(t *testing.T) {
	t.Run("a delay that passed while the broker was down", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		h := startDurableSQLite(t, path)

		gone := willConnectWithExpiry(t, h, "died-while-down", "events/will/down", 2, 300)
		awaitRegistered(t, h, gone, "died-while-down")
		kill(t, gone)
		waitForStoredWill(t, "died-while-down")
		h.Stop()

		// Down for longer than the delay.
		time.Sleep(2500 * time.Millisecond)
		h2 := startDurableSQLite(t, path)

		// The Will is published at the start, before any client can connect,
		// so what proves it is the record it left in the channel it claims -
		// which is what a subscriber arriving afterwards is served.
		// **A channel topic, deliberately.** The Will is published before any
		// listener opens, so a broadcast one would reach nobody and prove
		// nothing; a channel keeps the record, and what a consumer reads
		// afterwards is what the start actually published.
		watch := connect(t, h2, "watcher", true, false)
		watch.Sub(t, "events/#", 1)
		r, ok := watch.Await(t, 3*time.Second)
		if !ok {
			t.Fatal("the Will whose delay passed while the broker was down was never " +
				"published: the device is gone and nothing ever said so")
		}
		if r.Topic != "events/will/down" || r.Payload != "i-died" {
			t.Errorf("received %q %q, want the Will", r.Topic, r.Payload)
		}
		// Published once: the record no longer carries it, so a second start
		// does not announce the same death again.
		if sess, ok, _ := brokertest.HarnessSessions.Get("died-while-down"); ok && sess.Will != nil {
			t.Errorf("the Will is still on the record after the start published it: %+v", sess.Will)
		}
	})

	t.Run("a delay still running when the broker comes back", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		h := startDurableSQLite(t, path)

		gone := willConnectWithExpiry(t, h, "still-waiting", "loose/will/waiting", 6, 300)
		awaitRegistered(t, h, gone, "still-waiting")
		kill(t, gone)
		due := waitForStoredWill(t, "still-waiting")
		h.Stop()

		h2 := startDurableSQLite(t, path)
		watch := connect(t, h2, "watcher", true, false)
		watch.Sub(t, "loose/#", 1)

		// Not yet: what is left of the wait is what is left, rather than the
		// whole interval starting again.
		if r, ok := watch.Await(t, 2*time.Second); ok {
			t.Fatalf("%q was published %v before it was due: the restart published a Will "+
				"whose delay had not passed", r.Topic, time.Until(due).Round(time.Millisecond))
		}
		r, ok := watch.Await(t, 8*time.Second)
		if !ok {
			t.Fatal("the Will never arrived, so the wait the restart resumed never ended")
		}
		if r.Topic != "loose/will/waiting" {
			t.Errorf("received %q, want the Will", r.Topic)
		}
		if late := time.Since(due); late > 3*time.Second {
			t.Errorf("the Will arrived %v after it was due: the restart restarted the "+
				"interval rather than resuming it", late.Round(time.Millisecond))
		}
	})

	t.Run("a client that was connected when the broker stopped", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		h := startDurableSQLite(t, path)

		// Connected, armed, and never disconnected: the broker stops under it.
		conn := willConnectWithExpiry(t, h, "connected-at-stop", "loose/will/connected", 2, 300)
		defer conn.Close()
		awaitRegistered(t, h, conn, "connected-at-stop")
		if sess, ok, _ := brokertest.HarnessSessions.Get("connected-at-stop"); !ok || sess.Will == nil {
			t.Fatal("the connected client's Will was not kept, so this case is not set up")
		}
		stopBegan := time.Now()
		h.Stop()
		stopEnded := time.Now()

		h2 := startDurableSQLite(t, path)
		watch := connect(t, h2, "watcher", true, false)
		watch.Sub(t, "loose/#", 1)
		if r, ok := watch.Await(t, 4*time.Second); ok {
			t.Errorf("%q was published for a client that was connected when the broker "+
				"stopped: the broker stopping is not that device dying", r.Topic)
		}
		// **And the Will stays where it is**, with no moment on it: nothing
		// has happened yet to make it due. What makes it due is the client
		// returning, which replaces it, or this session expiring here, which
		// publishes it (TestARestoredWillIsPublishedWhenItsSessionExpires).
		// Stripping it would take Will protection away from every device that
		// was connected at a stop until each happened to reconnect.
		sess, ok, _ := brokertest.HarnessSessions.Get("connected-at-stop")
		if !ok {
			t.Fatal("the session did not come back")
		}
		// **Its expiry runs from the stop, not from this start**, which is
		// when the client actually became unreachable: measuring from the
		// start would hand every session a free extension the length of the
		// outage. A graceful stop ends every connection through the ordinary
		// disconnect path, so the moment is recorded there - and a session
		// that comes back with *no* moment is one nothing ended, which is a
		// crash, where this start is all anyone can honestly measure from.
		if sess.DisconnectedAt.Before(stopBegan) || sess.DisconnectedAt.After(stopEnded) {
			t.Errorf("the session came back disconnected at %v, and the stop ran from %v to "+
				"%v: its expiry is running from the wrong moment",
				sess.DisconnectedAt.Format("15:04:05.000"), stopBegan.Format("15:04:05.000"),
				stopEnded.Format("15:04:05.000"))
		}
		if sess.Will == nil {
			t.Error("the restart stripped the Will of a client that was connected when the " +
				"broker stopped: it has no Will protection now until it reconnects")
		} else if !sess.Will.DueAt.IsZero() {
			t.Errorf("the restored Will is due at %v: nothing has happened to make it due, "+
				"and a moment on it would publish it without the client having gone anywhere",
				sess.Will.DueAt)
		}
	})
}

// The epic's rule, and MQTT's own reading of a Will: nothing dies because the
// broker stopped. Closing the listeners closes every client, and a connection
// that ends without a DISCONNECT is one whose Will is due - so a stop
// announced every connected device as dead.
//
// **It was true before and invisible**, because the announcement
// went out as the broker went down: nothing was left connected to hear it,
// and only a channel keeps the record for afterwards. Measured that way here,
// which is the only way it shows.
func TestAStopAnnouncesNobodyAsDead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	h := startDurableSQLite(t, path)

	// One with no delay, which used to be published as the broker stopped,
	// and one with a delay, whose moment the record now carries.
	immediate, code := connectWithWill(t, h, "connected-now", "events/stop/immediate", false, 0, 0)
	if code != 0 {
		t.Fatalf("refused 0x%02X", code)
	}
	defer immediate.Close()
	delayed := willConnectWithExpiry(t, h, "connected-delayed", "events/stop/delayed", 2, 300)
	defer delayed.Close()
	awaitRegistered(t, h, immediate, "connected-now")
	awaitRegistered(t, h, delayed, "connected-delayed")

	h.Stop()
	h2 := startDurableSQLite(t, path)

	// **The one with no delay is read to an end marker.** Published as the
	// broker went down, it would be in the channel ahead of anything
	// published now, so the marker read first and alone says it is not.
	reader := connect(t, h2, "reader", true, false)
	reader.Sub(t, "events/#", 1)
	connect(t, h2, "marker", true, false).Pub(t, "events/stop/marker", "marker")
	if r, ok := reader.Await(t, 5*time.Second); !ok || r.Payload != "marker" {
		t.Errorf("the channel holds %q %q ahead of the end marker (%v): stopping the broker "+
			"announced a device that was connected to it as dead, and every restart would do "+
			"it to the whole fleet", r.Topic, r.Payload, ok)
	}
	// **The one with a delay is read off its record.** A Will is due at the
	// moment its record carries, and a stop that armed it would have put one
	// there; with none, nothing will ever publish it. Its delay is what a
	// quiet period here had to wait out.
	sess, ok, err := brokertest.HarnessSessions.Get("connected-delayed")
	if err != nil || !ok || sess.Will == nil {
		t.Fatalf("the delayed Will is not on its record after the restart (%v, %v)", ok, err)
	}
	if !sess.Will.DueAt.IsZero() {
		t.Errorf("the delayed Will is due at %v: stopping the broker armed it, and the start "+
			"will announce a device that was connected as dead", sess.Will.DueAt)
	}
}

// Every way a start ends a session, asked the same question: what happens to
// the Will on the record it is about to drop? Ending a session is what makes
// a waiting Will due (MQTT 5 3.1.3.2.2), so a start that drops the record
// first loses an announcement nobody else will ever make.
//
// **A Will with no moment on it is the other half of the rule** and is
// discarded: it belongs to a client that was connected when this broker
// stopped, and a broker stopping is not a device dying.
func TestAStartPublishesTheWillOfEverySessionItEnds(t *testing.T) {
	t.Run("a session that expired while the broker was stopped", func(t *testing.T) {
		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// One second of session, and a delay longer than it: the Will is
			// due when the session ends, which is while the broker is down.
			gone := willConnectWithExpiry(t, h, "expired-away", "events/will/expired", 30, 1)
			awaitRegistered(t, h, gone, "expired-away")
			kill(t, gone)
			waitForStoredWill(t, "expired-away")
			h.Stop()

			time.Sleep(1500 * time.Millisecond) // past its session's own life
			h2 := restart()

			watch := connect(t, h2, "watcher", true, false)
			watch.Sub(t, "events/#", 1)
			r, ok := watch.Await(t, 3*time.Second)
			if !ok {
				t.Fatal("the Will of a session that expired while the broker was stopped was " +
					"never published: the record was dropped with the announcement on it")
			}
			if r.Topic != "events/will/expired" {
				t.Errorf("received %q, want the Will", r.Topic)
			}
			if _, ok, _ := brokertest.HarnessSessions.Get("expired-away"); ok {
				t.Error("the expired session is still in the store after the start ended it")
			}
		})
	})

	t.Run("a session that ended with the connection a stop cut", func(t *testing.T) {
		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// Clean start, so the session ends with its connection, and
			// connected when the broker stops: its Will has no due moment.
			conn, code := connectWithWill(t, h, "cut-by-the-stop", "events/will/cut", false, 0, 0)
			if code != 0 {
				t.Fatalf("refused 0x%02X", code)
			}
			defer conn.Close()
			awaitRegistered(t, h, conn, "cut-by-the-stop")
			h.Stop()

			h2 := restart()
			watch := connect(t, h2, "watcher", true, false)
			watch.Sub(t, "events/#", 1)
			if r, ok := watch.Await(t, 3*time.Second); ok {
				t.Errorf("%q was published for a client that was connected when the broker "+
					"stopped: a broker stopping is not that device dying", r.Topic)
			}
			if _, ok, _ := brokertest.HarnessSessions.Get("cut-by-the-stop"); ok {
				t.Error("a session that ends with its connection survived the start")
			}
		})
	})
}

// eachSessionProviderRestart runs one case against both providers, handing it
// a broker and the way to restart it on the same storage.
func eachSessionProviderRestart(t *testing.T, run func(t *testing.T, h *harness, restart func() *harness)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		dir := t.TempDir()
		run(t, startDurable(t, dir), func() *harness { return startDurable(t, dir) })
	})
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.db")
		run(t, startDurableSQLite(t, path), func() *harness { return startDurableSQLite(t, path) })
	})
}

// awaitWillOffRecord waits until client's record no longer holds its Will -
// taken off it, or the record gone with its session - and returns the record,
// nil where it is gone.
//
// **Read until it holds, not once.** A Will is published and then taken off
// its record, in that order and never the other way round (RFC 0003 "Last
// Will"): taken off first, a crash between the two would lose the
// announcement, where published first it can only repeat it. So a watcher can
// be sent the Will before the record lets go of it, and a single read at that
// moment reports a Will the broker is about to take off. Five seconds, and
// what was last read if it never does.
func awaitWillOffRecord(t *testing.T, client string) *store.Session {
	t.Helper()
	var last store.Session
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		sess, ok, err := brokertest.HarnessSessions.Get(client)
		if err != nil {
			t.Fatalf("read %s's record: %v", client, err)
		}
		if !ok {
			return nil
		}
		if sess.Will == nil {
			return &sess
		}
		last = sess
		if time.Now().After(deadline) {
			t.Fatalf("the Will is still on %s's record five seconds after it was published: %+v",
				client, last.Will)
		}
	}
}

// The last door, and it is the running broker's: a session restored after a
// stop carries the Will of a client that was connected at the moment, with no
// due moment on it, and that client never comes back. The expiry sweep ends
// the session - and a session ending is a Will falling due.
//
// **This is not a Will firing because the broker stopped.** The broker
// stopping only started the clock; what publishes this one is the broker
// watching that client stay away for the whole interval it was granted, which
// is a device that went and did not return. Discarding it instead would mean
// every restart quietly took Will protection away from every connected
// client until each happened to reconnect.
func TestARestoredWillIsPublishedWhenItsSessionExpires(t *testing.T) {
	brokertest.MaxSessionExpiry = 3
	t.Cleanup(func() { brokertest.MaxSessionExpiry = 0 })
	eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
		// Connected when the broker stops, so its Will is on the record with
		// no moment: nothing has made it due yet.
		conn := willConnectWithExpiry(t, h, "never-returns", "events/will/expiring", 0, 300)
		defer conn.Close()
		awaitRegistered(t, h, conn, "never-returns")
		if sess, ok, _ := brokertest.HarnessSessions.Get("never-returns"); !ok || sess.Will == nil ||
			!sess.Will.DueAt.IsZero() {
			t.Fatalf("the connected client's Will is not on its record without a moment: %v", ok)
		}
		h.Stop()

		h2 := restart()
		if sess, ok, _ := brokertest.HarnessSessions.Get("never-returns"); !ok || sess.Will == nil {
			t.Fatal("the restart stripped the Will of a client that was connected when the " +
				"broker stopped: that client now has no Will protection at all until it " +
				"happens to reconnect")
		}
		watch := connect(t, h2, "watcher", true, false)
		watch.Sub(t, "events/#", 1)

		// Its granted session is three seconds, and the client never comes
		// back: the sweep ends the session, and the Will goes with it.
		r, ok := watch.Await(t, 10*time.Second)
		if !ok {
			t.Fatal("the session expired at a running broker and its Will was never published: " +
				"a device that went away and never came back was announced by nobody")
		}
		if r.Topic != "events/will/expiring" || r.Payload != "i-died" {
			t.Errorf("published %q %q, want the Will", r.Topic, r.Payload)
		}
		awaitWillOffRecord(t, "never-returns")
	})
}

// TestWhatAStartDoesWithAGroupsBacklog is the start's side of step 12, the
// same rule seen at three moments (internal/broker/share.go).
//
// **Both providers and a real restart**, because what a backlog survives is
// the provider's own promise and the three answers differ by what the start
// finds left of the sessions that could collect it.
func TestWhatAStartDoesWithAGroupsBacklog(t *testing.T) {
	const group = "$share/fleet/alerts/#"

	t.Run("member sessions survive, so the backlog does", func(t *testing.T) {
		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// An hour of session, so it is still there after the restart.
			member := dial(t, h, "worker", false, false, 0, 3600, 0)
			member.Sub(t, group, 1)
			member.Close()
			// Gone before the publish, or it goes live to the member leaving.
			sessionGone(t, h, "worker")

			p := connect(t, h, "producer", true, false)
			p.Pub(t, "alerts/door", "owed")
			if held := waitForBacklog(t, group, 1); len(held) != 1 {
				t.Fatalf("the group holds %d deliveries before the stop, want 1", len(held))
			}
			h.Stop()

			restart()
			held, err := brokertest.HarnessShares.Backlog(group)
			if err != nil {
				t.Fatal(err)
			}
			if len(held) != 1 {
				t.Errorf("the group holds %d deliveries after the restart, want the one its "+
					"member is still owed: a persistent session is the broker's own record "+
					"that its holder is coming back, and the start took the outage out on it",
					len(held))
			}
		})
	})

	t.Run("no member session is left, so the backlog goes", func(t *testing.T) {
		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// Ten seconds of session: long enough that the publish and the
			// backlog check below finish while it lives, which is when the
			// group holds the record at all, and gone by the time the restart
			// looks. One second was the margin this ran inside, and a loaded
			// machine once missed it.
			member := dial(t, h, "worker", false, false, 0, 10, 0)
			member.Sub(t, group, 1)
			member.Close()
			// Gone before the publish, or it goes live to the member leaving.
			sessionGone(t, h, "worker")

			p := connect(t, h, "producer", true, false)
			p.Pub(t, "alerts/door", "owed to nobody")
			if held := waitForBacklog(t, group, 1); len(held) != 1 {
				t.Fatalf("the group holds %d deliveries before the stop, want 1", len(held))
			}
			h.Stop()
			time.Sleep(10500 * time.Millisecond) // the member's session expires while it is down

			restart()
			held, err := brokertest.HarnessShares.Backlog(group)
			if err != nil {
				t.Fatal(err)
			}
			if len(held) != 0 {
				t.Errorf("the group still holds %d deliveries after every session that could "+
					"collect them expired: a backlog nobody is owed is a provider filling up "+
					"with work for ghosts", len(held))
			}
		})
	})

	t.Run("held past its expiry, so the start drops it", func(t *testing.T) {
		brokertest.ShareExpiresAfter = time.Second
		t.Cleanup(func() { brokertest.ShareExpiresAfter = 0 })

		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// A session that outlives the outage, so the only thing that can
			// remove this backlog is the expiry itself.
			member := dial(t, h, "worker", false, false, 0, 3600, 0)
			member.Sub(t, group, 1)
			member.Close()
			// Gone before the publish, or it goes live to the member leaving.
			sessionGone(t, h, "worker")

			p := connect(t, h, "producer", true, false)
			p.Pub(t, "alerts/door", "stale")
			if held := waitForBacklog(t, group, 1); len(held) != 1 {
				t.Fatalf("the group holds %d deliveries before the stop, want 1", len(held))
			}
			h.Stop()
			time.Sleep(1500 * time.Millisecond) // longer than broker.share.expires_after

			restart()
			held, err := brokertest.HarnessShares.Backlog(group)
			if err != nil {
				t.Fatal(err)
			}
			if len(held) != 0 {
				t.Errorf("the group still holds %d deliveries past broker.share.expires_after: "+
					"an outage that extends what an operator bounded is the bound not applying "+
					"exactly when it matters", len(held))
			}
		})
	})
}

// TestABacklogDroppedByItsExpiryIsCountedAsItsOwnCause is about the label an
// operator is sent to a knob by. `broker.share.expires_after` dropping work
// and `limits.session_queue_bytes` dropping work are two different
// operators' decisions, and counting the first as the second tells whoever
// is watching a fleet lose backlogged deliveries to grow a queue that was
// never full.
//
// **Both doors, in one test**, because they are one rule seen at two
// moments and were wrong in the same way: the running broker's sweep and
// the pass a start makes before any listener opens.
//
// The control is `backlog_full` itself: the queue bound is untouched here
// and nothing goes near it, so a run where it moves is a run where the
// cause is still being read off the wrong counter.
func TestABacklogDroppedByItsExpiryIsCountedAsItsOwnCause(t *testing.T) {
	const (
		group   = "$share/fleet/alerts/#"
		expired = `saguin_shares_dropped_total{cause="expired"}`
		full    = `saguin_shares_dropped_total{cause="backlog_full"}`
		held    = 5
	)

	t.Run("the running broker's sweep", func(t *testing.T) {
		brokertest.ShareExpiresAfter = time.Second
		t.Cleanup(func() { brokertest.ShareExpiresAfter = 0 })

		eachSessionProvider(t, func(t *testing.T, h *harness) {
			ops := operationsAt(t, h)

			// **Every cause from the start**, which the catalogue promises
			// and a series that appears only once it has fired would break:
			// an alert on a rate cannot be written against a name that is
			// not there yet.
			if _, ok := scrapeGauges(t, ops)[expired]; !ok {
				t.Fatalf("%s is not served before anything expires, and RFC 0005 says every "+
					"cause is served from the start", expired)
			}

			member := dial(t, h, "worker-expiring", false, false, 0, 3600, 0)
			member.Sub(t, group, 1)
			member.Close()
			sessionGone(t, h, "worker-expiring")

			p := connect(t, h, "producer-expiring", true, false)
			for i := range held {
				p.Pub(t, "alerts/door", fmt.Sprintf("stale-%d", i))
			}
			if backlog := waitForBacklog(t, group, held); len(backlog) != held {
				t.Fatalf("the group holds %d deliveries, want %d: nothing below is about a "+
					"backlog reaching its expiry", len(backlog), held)
			}
			if backlog := waitForBacklog(t, group, 0); len(backlog) != 0 {
				t.Fatalf("the group still holds %d deliveries past its expiry, so nothing was "+
					"dropped and no cause has been counted", len(backlog))
			}

			g := scrapeGauges(t, ops)
			if g[expired] != held {
				t.Errorf("%s is %v after the sweep dropped %d deliveries past "+
					"broker.share.expires_after, want %d", expired, g[expired], held, held)
			}
			if g[full] != 0 {
				t.Errorf("%s is %v with the queue bound untouched: an operator watching a "+
					"fleet lose backlogged work is told to grow a queue that was never full",
					full, g[full])
			}
		})
	})

	t.Run("the pass a start makes", func(t *testing.T) {
		brokertest.ShareExpiresAfter = time.Second
		t.Cleanup(func() { brokertest.ShareExpiresAfter = 0 })

		eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
			// A session that outlives the outage, so the expiry is the only
			// thing that can remove this backlog.
			member := dial(t, h, "worker-restart", false, false, 0, 3600, 0)
			member.Sub(t, group, 1)
			member.Close()
			// Gone before the publish, or it goes live to the member leaving.
			sessionGone(t, h, "worker-restart")

			p := connect(t, h, "producer-restart", true, false)
			for i := range held {
				p.Pub(t, "alerts/door", fmt.Sprintf("stale-%d", i))
			}
			if backlog := waitForBacklog(t, group, held); len(backlog) != held {
				t.Fatalf("the group holds %d deliveries before the stop, want %d",
					len(backlog), held)
			}
			h.Stop()
			time.Sleep(1500 * time.Millisecond) // longer than broker.share.expires_after

			ops := operationsAt(t, restart())
			g := scrapeGauges(t, ops)
			if g[expired] != held {
				t.Errorf("%s is %v after a start dropped %d deliveries held past "+
					"broker.share.expires_after, want %d", expired, g[expired], held, held)
			}
			if g[full] != 0 {
				t.Errorf("%s is %v at a start where no queue bound was reached", full, g[full])
			}
		})
	})
}

// writeFailing is a connection whose CONNECT arrives and whose every write
// fails: a device reconnecting over a link that drops again before the
// broker's answer reaches it. The CONNACK is the first thing the broker
// writes, so it is the write that fails - every time, where cutting a real
// socket makes it a matter of timing.
type writeFailing struct{ r *bytes.Reader }

func (c *writeFailing) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *writeFailing) Write([]byte) (int, error) {
	return 0, errors.New("connection reset by peer")
}
func (c *writeFailing) Close() error { return nil }
func (c *writeFailing) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1883}
}
func (c *writeFailing) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50000}
}
func (c *writeFailing) SetDeadline(time.Time) error      { return nil }
func (c *writeFailing) SetReadDeadline(time.Time) error  { return nil }
func (c *writeFailing) SetWriteDeadline(time.Time) error { return nil }

// connectFor is an MQTT 5 CONNECT, with a Will when willTopic is set.
func connectFor(t *testing.T, id string, clean bool, expiry uint32, willTopic string) []byte {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	c := cp.Content.(*pahopackets.Connect)
	c.ClientID, c.CleanStart, c.KeepAlive = id, clean, 0
	c.Properties = &pahopackets.Properties{SessionExpiryInterval: &expiry}
	if willTopic != "" {
		c.WillFlag, c.WillTopic, c.WillMessage = true, willTopic, []byte("gone")
		c.WillProperties = &pahopackets.Properties{}
	}
	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}
	return buf.Bytes()
}

// connackNeverArrives puts one CONNECT through the broker's own accept path
// on a connection whose CONNACK cannot be written, and returns once the
// broker has finished with it.
//
// **It fails unless the connection ended at its CONNACK**, which is the
// engine's own "ack connection packet" error: a connection refused before
// that, or one whose write somehow worked, is not the case under test and
// would pass for the wrong reason.
func connackNeverArrives(t *testing.T, h *harness, connect []byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.Srv.EstablishConnection("t", &writeFailing{r: bytes.NewReader(connect)}) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "ack connection packet") {
			t.Fatalf("the connection ended with %v rather than at its CONNACK, so this proves nothing", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the broker never finished with a connection whose CONNACK could not be written")
	}
}

// A connection that claims a live session's client id and never gets its
// CONNACK has not taken that session over: the engine keeps the live one
// connected and registered, so nothing saguin keeps for it may change.
//
// It used to be torn down in full, because the engine tells saguin about
// the failed connection as a disconnect of a session ending, and saguin had
// recorded the failed connection as the id's owner. The live session's held
// exactly-once publish was dropped, and its PUBREL was then answered
// PUBCOMP 0x00 for a message the channel never received.
func TestAConnectionThatNeverGetsItsConnackLeavesTheLiveSessionAlone(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean start %t", clean), func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				w, _ := qos2Dial(t, h.Addr, "dev", false, 3600)
				w.Publish(1, "events/held", "held", false)
				if rc := w.Pubrec(1); rc != 0 {
					t.Fatalf("PUBREC 0x%02X, want the publish held", rc)
				}

				connackNeverArrives(t, h, connectFor(t, "dev", clean, 3600, ""))

				w.Pubrel(1)
				if rc := w.Pubcomp(1); rc != 0 {
					t.Fatalf("PUBCOMP 0x%02X for a publish the live session still held", rc)
				}
				if n := inChannel(t, h, "reader", "held"); n != 1 {
					t.Fatalf("the channel holds the released publish %d times after PUBCOMP 0x00, want 1: "+
						"a connection that never got its CONNACK dropped the live session's held publish", n)
				}
			})
		})
	}
}

// And a live subscriber goes on being served. The teardown the failed
// connection ran used to take the live session's place in every channel
// with it, so the subscriber - still connected, still subscribed - was sent
// nothing more, with nothing said to it.
func TestALiveSubscriberIsServedAfterAConnectionUnderItsIDFails(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		dev := dial(t, h, "dev", false, false, 0, 3600, 0)
		dev.Sub(t, "events/#", 1)
		p := connect(t, h, "publisher", true, false)
		p.Pub(t, "events/x", "one")
		if r, ok := dev.Await(t, 3*time.Second); !ok || r.Payload != "one" {
			t.Fatalf("the subscriber was not served before anything happened (got %q, %v)", r.Payload, ok)
		}

		connackNeverArrives(t, h, connectFor(t, "dev", false, 3600, ""))

		p.Pub(t, "events/x", "two")
		if r, ok := dev.Await(t, 3*time.Second); !ok || r.Payload != "two" {
			t.Fatalf("the live subscriber was not served after a connection under its id failed "+
				"(got %q, %v): its place in the channel went with a connection that never existed", r.Payload, ok)
		}
	})
}

// **A connection that never gets its CONNACK leaves no record where the live
// session had none.** It claimed the id of a live connection the store had
// no room to keep a session for - accepted, and told its session ends with
// it - and wrote a record of its own before the CONNACK. The live
// connection keeps the id and is still the session, so the record goes:
// kept, a restart would restore a session that never was.
func TestAConnectionThatNeverGetsItsConnackLeavesNoRecordWhereThereWasNone(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &refuseKeepingWhen{SessionStore: s, armed: &armed}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		armed.Store(true)
		dial(t, h, "dev", false, false, 0, 3600, 0)
		armed.Store(false)
		if _, ok, err := brokertest.HarnessSessions.Get("dev"); err != nil || ok {
			t.Fatalf("the store holds a record for the live session it had no room for (%v, %v), "+
				"so this proves nothing", ok, err)
		}
		connackNeverArrives(t, h, connectFor(t, "dev", false, 3600, ""))
		if sess, ok, err := brokertest.HarnessSessions.Get("dev"); err != nil || ok {
			t.Errorf("the store holds %+v for dev (%v, %v), written by a connection whose CONNACK "+
				"never arrived: a restart would restore a session that never was", sess, ok, err)
		}
	})
}

// refuseKeepingWhen has no room for a session record while armed: a Save, or
// a Begin that would keep one, fails full. Endings pass through.
type refuseKeepingWhen struct {
	broker.SessionStore
	armed *atomic.Bool
}

func (s *refuseKeepingWhen) Save(sess store.Session) error {
	if s.armed.Load() {
		return store.ErrFull
	}
	return s.SessionStore.Save(sess)
}

func (s *refuseKeepingWhen) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if s.armed.Load() && next != nil {
		return store.Dropped{}, store.ErrFull
	}
	return s.SessionStore.Begin(client, held, next)
}

// A delayed Will stays owed when the connection that would have cancelled it
// never completes: [MQTT-3.1.3-9] cancels a Will for a new Network
// Connection to the session, and one whose CONNACK never arrived is not one.
func TestADelayedWillIsNotCancelledByAConnectionThatNeverCompletes(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		watcher := connect(t, h, "watcher", true, false)
		watcher.Sub(t, "wills/dev", 0)
		// A session that outlives the delay, so the Will waits the whole two
		// seconds rather than firing as the session ends.
		conn, rc := connectWithWillExpiring(t, h, "dev", "wills/dev", false, 2, 0, 3600)
		if rc != 0 {
			t.Fatalf("CONNECT with a Will refused 0x%02X", rc)
		}
		kill(t, conn)
		sessionGone(t, h, "dev")

		connackNeverArrives(t, h, connectFor(t, "dev", false, 3600, ""))
		if n := watcher.Count(); n != 0 {
			t.Fatalf("the Will arrived before its delay ran out (%d), so this proves nothing", n)
		}

		if r, ok := watcher.Await(t, 5*time.Second); !ok || r.Topic != "wills/dev" {
			t.Fatalf("the Will was not published (got %q, %v): a connection that never got its "+
				"CONNACK cancelled it", r.Topic, ok)
		}
	})
}

// refuseWillsWhen fails a Save carrying a Will while armed, and passes
// every other call through.
type refuseWillsWhen struct {
	broker.SessionStore
	armed *atomic.Bool
	err   error
}

func (s *refuseWillsWhen) Save(sess store.Session) error {
	if s.armed.Load() && sess.Will != nil {
		return s.err
	}
	return s.SessionStore.Save(sess)
}

// Begin keeps a new session's record as Save does, and refuses its Will the
// same way.
func (s *refuseWillsWhen) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if s.armed.Load() && next != nil && next.Will != nil {
		return store.Dropped{}, s.err
	}
	return s.SessionStore.Begin(client, held, next)
}

// A connection refused for a Will the store cannot keep leaves the live
// session under its id exactly as it was: its record, and its end.
//
// It used to delete the live session's record on its way to the refusal -
// a clean start ends the old session first - so a broker restarted after
// the live client left brought it back to Session Present 0 and nothing.
// And it kept its claim on the id, so the live session's own disconnect and
// expiry each found somebody else owning it and dropped nothing, leaving
// its record for ever.
func TestARefusedTakeoverLeavesTheLiveSessionItsRecordAndItsEnd(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &refuseWillsWhen{SessionStore: s, armed: &armed, err: store.ErrFull}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		armed.Store(false)
		ops := operationsAt(t, h)
		dev := dial(t, h, "dev", false, false, 0, 1, 0)
		dev.Sub(t, "events/#", 1)
		accepted := metricValue(t, ops, "saguin_connections_total")

		armed.Store(true)
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write(connectFor(t, "dev", true, 3600, "wills/dev")); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		ack, err := pahopackets.ReadPacket(conn)
		armed.Store(false)
		if err != nil {
			t.Fatalf("no answer to the CONNECT: %v", err)
		}
		if ca, ok := ack.Content.(*pahopackets.Connack); !ok || ca.ReasonCode != 0x97 {
			t.Fatalf("the CONNECT was answered %s, want CONNACK 0x97, so this proves nothing", ack)
		}

		sess, ok, err := brokertest.HarnessSessions.Get("dev")
		if err != nil || !ok {
			t.Fatalf("the live session's record is gone (%v, %v) after a connection under its id was refused", ok, err)
		}
		if len(sess.Subscriptions) != 1 {
			t.Fatalf("the live session's record holds %d subscriptions after the refusal, want 1", len(sess.Subscriptions))
		}
		if got := metricValue(t, ops, "saguin_connections_total"); got != accepted {
			t.Errorf("saguin_connections_total went from %v to %v for a connection that was refused", accepted, got)
		}

		// And it is still the live session's to end: it goes, it expires,
		// and its record goes with it.
		_ = dev.C.Disconnect(&paho.Disconnect{})
		for deadline := time.Now().Add(6 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if _, held, _ := brokertest.HarnessSessions.Get("dev"); !held {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the live session's record outlived its expiry: its disconnect and its expiry each " +
					"found a refused connection owning the id, and dropped nothing")
			}
		}
	})
}

// RFC 0003 "Sessions": a CONNECT that would only resume the session and be
// taken over by the next CONNECT for its client id is answered with what that
// would have sent it - its CONNACK, then DISCONNECT 0x8E - without taking the
// session over, and its Will goes as a taken-over connection's does: at once
// with no delay, when the CONNECT after it starts clean, and not at all when
// that one resumes inside the delay [MQTT-3.1.3-9]. On both providers.
//
// The CONNECTs wait behind the id's session lock, held here, so the order they
// wait in is the order they were sent. **The count of CONNECTs that read the
// session's record as they claimed it is what shows which ran**: on the wire
// a superseded connection and one taken over after running look much the
// same. Much, not quite: the session holds a delivery its owner never
// acknowledged, which a connection resuming it is sent again after its
// CONNACK, so a superseded one is shown to be sent nothing between its
// CONNACK and its DISCONNECT; and the first CONNECT has a PUBLISH written
// straight after it, which a superseded connection never reads.
//
// **And the superseded ones are answered only once a newer one owns the
// session.** Where the newest is refused after the lock, the ones it passed
// are judged again among themselves, and the last of them owns the session -
// not the connection they were all going to take it from.
func TestACONNECTANewerOneSupersedesIsAnsweredWithoutTheTakeover(t *testing.T) {
	var claims sync.Map // client id -> *atomic.Int64
	var refuse atomic.Bool
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &refuseWillsWhen{SessionStore: &claimsCounted{SessionStore: s, n: &claims},
			armed: &refuse, err: store.ErrFull}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	claimed := func(id string) int64 {
		n, _ := claims.LoadOrStore(id, &atomic.Int64{})
		return n.(*atomic.Int64).Load()
	}
	none := uint32(1 << 31)
	arms := []struct {
		name      string
		willDelay uint32 // of the first CONNECT; none for no Will
		nextClean bool   // whether the CONNECT after it starts clean
		refuseNew bool   // the newest carries a Will the store refuses
		// what the watcher is sent, the counter that shows why, and how many
		// CONNECTs claimed the session
		wills  []string
		series string
		claims int64
	}{
		{name: "no Will", willDelay: none, claims: 1},
		{name: "a Will with no delay", willDelay: 0, wills: []string{"first"},
			series: `saguin_wills_published_total{cause="immediate"}`, claims: 1},
		{name: "a delayed Will, the next resuming", willDelay: 60,
			series: "saguin_wills_cancelled_total", claims: 1},
		{name: "a delayed Will, the next starting clean", willDelay: 60, nextClean: true,
			wills: []string{"first"}, series: `saguin_wills_published_total{cause="session_ended"}`, claims: 2},
		{name: "the newest refused", willDelay: none, refuseNew: true, claims: 2},
	}
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		ops := operationsAt(t, h)
		for i, arm := range arms {
			t.Run(arm.name, func(t *testing.T) {
				id := fmt.Sprintf("dev-%d", i)
				watch := connect(t, h, "watcher-"+id, true, false)
				watch.Sub(t, "status/#", 1)
				owner := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
					t.Fatalf("the owner was answered %v", p)
				}
				sub := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
				sub.Content.(*pahopackets.Subscribe).PacketID = 1
				sub.Content.(*pahopackets.Subscribe).Subscriptions = []pahopackets.SubOptions{{Topic: "loose/" + id, QoS: 1}}
				if _, err := sub.WriteTo(owner); err != nil {
					t.Fatalf("write SUBSCRIBE: %v", err)
				}
				if p := readAnswers(t, owner, 1); !strings.HasPrefix(p[0], "SUBACK") {
					t.Fatalf("the owner's SUBSCRIBE was answered %v", p)
				}
				connect(t, h, "pub-"+id, true, false).Pub(t, "loose/"+id, "owed")
				if p := readAnswers(t, owner, 1); !strings.HasPrefix(p[0], "PUBLISH") {
					t.Fatalf("the owner was sent %v, want the delivery it will not acknowledge", p)
				}

				unlock := h.Srv.LockSession(id)
				first := connectFor(t, id, false, 3600, "")
				if arm.willDelay != none {
					first = connectWithDelayedWill(t, id, false, 3600, "status/"+id, "first", arm.willDelay)
				}
				newest := connectFor(t, id, false, 3600, "")
				if arm.refuseNew {
					newest = connectFor(t, id, false, 3600, "status/"+id)
				}
				pipelined := pahopackets.NewControlPacket(pahopackets.PUBLISH)
				pp := pipelined.Content.(*pahopackets.Publish)
				pp.Topic, pp.QoS, pp.PacketID, pp.Payload = "status/"+id, 1, 7, []byte("pipelined")
				var buf bytes.Buffer
				if _, err := pipelined.WriteTo(&buf); err != nil {
					t.Fatalf("encode PUBLISH: %v", err)
				}
				first = append(first[:len(first):len(first)], buf.Bytes()...)
				var conns []net.Conn
				for n, pk := range [][]byte{first, connectFor(t, id, arm.nextClean, 3600, ""), newest} {
					conns = append(conns, rawCONNECT(t, h, pk))
					for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < n+2; time.Sleep(time.Millisecond) {
						if time.Now().After(deadline) {
							unlock()
							t.Fatalf("CONNECT %d never waited for the id's session lock, so the order is not known", n+1)
						}
					}
				}
				before, claimsBefore := scrapeGauges(t, ops), claimed(id)
				refuse.Store(arm.refuseNew)
				unlock()

				wantFirst := []string{"CONNACK 0x00 present=true", "DISCONNECT 0x8E"}
				if got := readAnswers(t, conns[0], 2); !slices.Equal(got, wantFirst) {
					t.Errorf("the first CONNECT was answered %v, want %v", got, wantFirst)
				}
				if arm.refuseNew {
					if got := readAnswers(t, conns[2], 1); got[0] != "CONNACK 0x97 present=false" {
						t.Fatalf("the newest was answered %v, want CONNACK 0x97, so this proves nothing", got)
					}
					refuse.Store(false)
					if got := readAnswers(t, conns[1], 1); got[0] != "CONNACK 0x00 present=true" {
						t.Errorf("the second was answered %v, want its CONNACK", got)
					}
					if got := readAnswers(t, owner, 1); got[0] != "DISCONNECT 0x8E" {
						t.Errorf("the owner was sent %v, want DISCONNECT 0x8E: the CONNECTs the refused "+
							"one had passed never took their turns, so the session stayed with it", got)
					}
					stillConnected(t, conns[1], "the second CONNECT, which should own the session")
				} else {
					if got := readAnswers(t, owner, 1); got[0] != "DISCONNECT 0x8E" {
						t.Errorf("the owner was sent %v, want DISCONNECT 0x8E", got)
					}
					second := wantFirst
					if arm.nextClean {
						second = []string{"CONNACK 0x00 present=false", "DISCONNECT 0x8E"}
					}
					if got := readAnswers(t, conns[1], 2); !slices.Equal(got, second) {
						t.Errorf("the second CONNECT was answered %v, want %v", got, second)
					}
					if got := readAnswers(t, conns[2], 1); got[0] != "CONNACK 0x00 present=true" {
						t.Errorf("the newest was answered %v, want its CONNACK", got)
					}
					stillConnected(t, conns[2], "the newest CONNECT, which should own the session")
				}

				accepted := 3.0
				if arm.refuseNew {
					accepted = 2
				}
				// **Read once it has caught up.** A superseded CONNECT is
				// counted after its CONNACK and DISCONNECT are written
				// (answerSuperseded), so the answers read above can come
				// first: 7 in 27 runs on two loaded cores under -race. An
				// extra count still shows, as a total above accepted.
				var d float64
				waitUntil(5*time.Second, func() bool {
					d = scrapeGauges(t, ops)["saguin_connections_total"] - before["saguin_connections_total"]
					return d >= accepted
				})
				if d != accepted {
					t.Errorf("saguin_connections_total went up %v for %v CONNECTs answered with a CONNACK 0x00", d, accepted)
				}
				if got := awaitWills(t, h, watch, "after-"+id); !slices.Equal(got, arm.wills) {
					t.Errorf("the watcher was sent %q, want %q", got, arm.wills)
				}
				if arm.series != "" {
					if d := scrapeGauges(t, ops)[arm.series] - before[arm.series]; d != 1 {
						t.Errorf("%s went up %v, want 1", arm.series, d)
					}
				}
				if got := claimed(id) - claimsBefore; got != arm.claims {
					t.Errorf("%d CONNECTs claimed the session, want %d", got, arm.claims)
				}
				for _, c := range append(conns, owner) {
					_ = c.Close()
				}
			})
		}

		// **A delayed Will falling due behind a passed resume is still
		// cancelled by it.** The owner is gone with a Will due in a second;
		// a resume waits, then the Will falling due, then a newer resume.
		// In arrival order the first resume cancels the Will before it is
		// due, so no Will is published for a device that came back inside
		// its delay [MQTT-3.1.3-9] - and passing that resume over must not
		// let the Will run first.
		t.Run("a Will falling due behind a passed resume", func(t *testing.T) {
			id := "dev-due"
			watch := connect(t, h, "watcher-"+id, true, false)
			watch.Sub(t, "status/#", 1)
			gone := h.Disconnects.Snapshot(id)
			owner := rawCONNECT(t, h, connectWithDelayedWill(t, id, false, 3600, "status/"+id, "owner", 1))
			if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
				t.Fatalf("the owner was answered %v", p)
			}
			_ = owner.Close()
			sessionGoneAfter(t, h, id, gone)

			unlock := h.Srv.LockSession(id)
			waitFor := func(n int, what string) {
				t.Helper()
				for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < n; time.Sleep(time.Millisecond) {
					if time.Now().After(deadline) {
						unlock()
						t.Fatalf("%s never waited for the id's session lock, so the order is not known", what)
					}
				}
			}
			first := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
			waitFor(2, "the first resume")
			waitFor(3, "the Will falling due")
			newest := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
			waitFor(4, "the newest resume")
			before, claimsBefore := scrapeGauges(t, ops), claimed(id)
			unlock()

			if got := readAnswers(t, first, 2); !slices.Equal(got, []string{"CONNACK 0x00 present=true", "DISCONNECT 0x8E"}) {
				t.Errorf("the first resume was answered %v", got)
			}
			if got := readAnswers(t, newest, 1); got[0] != "CONNACK 0x00 present=true" {
				t.Errorf("the newest resume was answered %v", got)
			}
			if got := awaitWills(t, h, watch, "after-"+id); len(got) != 0 {
				t.Errorf("the watcher was sent %q for a device that resumed inside its Will's delay", got)
			}
			if d := scrapeGauges(t, ops)["saguin_wills_cancelled_total"] - before["saguin_wills_cancelled_total"]; d != 1 {
				t.Errorf("saguin_wills_cancelled_total went up %v, want 1", d)
			}
			if got := claimed(id) - claimsBefore; got != 1 {
				t.Errorf("%d CONNECTs claimed the session, want 1: the first resume was not passed over, "+
					"so this is not the case under test", got)
			}
			_, _ = first.Close(), newest.Close()
		})
	})
}

// payloadsOf collects what a Paho v3 client is sent, for a test that reads
// it to an end marker.
type payloadsOf struct {
	mu  sync.Mutex
	got []string
}

func (p *payloadsOf) record(_ mqttv3.Client, m mqttv3.Message) {
	p.mu.Lock()
	p.got = append(p.got, string(m.Payload()))
	p.mu.Unlock()
}

// until waits for the marker and returns everything received up to it.
func (p *payloadsOf) until(t *testing.T, marker string) []string {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		p.mu.Lock()
		got := slices.Clone(p.got)
		p.mu.Unlock()
		if i := slices.Index(got, marker); i >= 0 {
			return got[:i+1]
		}
		if time.Now().After(deadline) {
			t.Fatalf("the end marker %q never arrived; received %q", marker, got)
		}
	}
}

// RFC 0003 "Sessions", MQTT 5 section 3.1.2.11.2: a session that ends with
// its connection ends at a takeover too, since a takeover closes that
// connection. A CONNECT taking it over begins a new session, whatever its
// Clean Start says: Session Present 0, and none of the old session's
// deliveries, subscriptions or channel consumers. Its Will goes at once, as a
// session ending with its connection has it (section 3.1.3.2.2), where a
// resume inside the delay would have cancelled it. An MQTT 5 predecessor with
// no Session Expiry Interval used to be
// resumed, and what it held had never been kept, so a restart then lost it
// from a session its client had been told was present. A predecessor whose
// session outlives its connection is the control: resumed, and what it was
// sent comes back after a restart. On both providers.
func TestATakeoverOfASessionThatEndsWithItsConnectionBeginsANewOne(t *testing.T) {
	arms := []struct {
		name    string
		expiry  uint32 // the predecessor's Session Expiry Interval
		present bool
	}{
		{"no Session Expiry Interval", 0, false},
		{"a Session Expiry Interval (control)", 3600, true},
	}
	eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
		ops := operationsAt(t, h)
		var nexts []net.Conn
		for i, arm := range arms {
			id := fmt.Sprintf("dev%d", i)
			t.Run(arm.name, func(t *testing.T) {
				watch := connect(t, h, "watcher-"+id, true, false)
				watch.Sub(t, "status/#", 1)
				prev := rawCONNECT(t, h, connectWithDelayedWill(t, id, false, arm.expiry, "status/"+id, "first", 60))
				if got := readAnswers(t, prev, 1); got[0] != "CONNACK 0x00 present=false" {
					t.Fatalf("the predecessor was answered %v", got)
				}
				subscribeConn(t, prev, "loose/"+id, "iot/"+id+"/events/+")
				pub := connect(t, h, "pub-"+id, true, false)
				pub.Pub(t, "loose/"+id, "owed")
				if got := readAnswers(t, prev, 1); !strings.HasPrefix(got[0], "PUBLISH") {
					t.Fatalf("the predecessor was sent %v, want the delivery it will not acknowledge", got)
				}
				before := scrapeGauges(t, ops)

				next := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				nexts = append(nexts, next)
				want := fmt.Sprintf("CONNACK 0x00 present=%t", arm.present)
				if got := readAnswers(t, next, 1); got[0] != want {
					t.Errorf("the takeover was answered %v, want %s", got, want)
				}
				if got := readAnswers(t, prev, 1); got[0] != "DISCONNECT 0x8E" {
					t.Errorf("the predecessor was sent %v, want DISCONNECT 0x8E", got)
				}
				// And the record the store keeps: a restart before the new
				// session next subscribes restores what it says.
				var filters []string
				if rec, _, err := brokertest.HarnessSessions.Get(id); err == nil {
					for _, sub := range rec.Subscriptions {
						filters = append(filters, sub.Filter)
					}
				}
				var wantFilters []string
				if arm.present {
					wantFilters = []string{"iot/" + id + "/events/+", "loose/" + id}
				}
				if !slices.Equal(filters, wantFilters) {
					t.Errorf("after the takeover the session's record holds %q, want %q", filters, wantFilters)
				}
				pub.Pub(t, "loose/"+id, "late")
				pub.Pub(t, "iot/"+id+"/events/e", "record")
				var wantSent []string
				if arm.present {
					wantSent = []string{"late", "owed", "record"}
				}
				if got := publishesUntilQuiet(t, next); !slices.Equal(got, wantSent) {
					t.Errorf("the new connection was sent %q, want %q", got, wantSent)
				}
				stillConnected(t, next, "the connection that took the id over")

				wills, series := []string{"first"}, `saguin_wills_published_total{cause="immediate"}`
				if arm.present {
					wills, series = nil, "saguin_wills_cancelled_total"
				}
				if got := awaitWills(t, h, watch, "after-"+id); !slices.Equal(got, wills) {
					t.Errorf("the watcher was sent %q, want %q", got, wills)
				}
				if d := scrapeGauges(t, ops)[series] - before[series]; d != 1 {
					t.Errorf("%s went up %v, want 1", series, d)
				}
			})
		}

		// A 3.1.1 predecessor with Clean Session set, whose session the
		// engine already ended at a takeover: nothing the broker kept for it
		// under the id - its channel consumer, its subscriptions on the
		// record - reaches the new connection, before a restart or after.
		var next311 mqttv3.Client
		t.Run("a 3.1.1 Clean Session", func(t *testing.T) {
			const id = "dev311"
			prev, _ := legacy(t, h, id, true)
			var had atomic.Int64
			if tok := prev.Subscribe("iot/"+id+"/events/+", 1, func(mqttv3.Client, mqttv3.Message) { had.Add(1) }); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe: %v", tok.Error())
			}
			pub := connect(t, h, "pub-"+id, true, false)
			pub.Pub(t, "iot/"+id+"/events/e", "first")
			for deadline := time.Now().Add(3 * time.Second); had.Load() == 0; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the predecessor was never sent the channel's record, so its consumer proves nothing")
				}
			}
			var sent payloadsOf
			next311, _ = legacyWithDefault(t, h, id, false, sent.record)
			// **An end marker on the same channel**, subscribed by the new
			// connection itself: one consumer per session and channel serves
			// in offset order, so a record its predecessor's consumer still
			// owed it comes ahead of the marker.
			if tok := next311.Subscribe("iot/"+id+"/events/marker", 1, nil); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe the end marker: %v", tok.Error())
			}
			pub.Pub(t, "iot/"+id+"/events/e", "record")
			pub.Pub(t, "iot/"+id+"/events/marker", "marker")
			if got := sent.until(t, "marker"); !slices.Equal(got, []string{"marker"}) {
				t.Errorf("the new connection was sent %q, want only its own end marker: the rest are "+
					"records of a channel only its predecessor subscribed to", got)
			}
		})

		for _, c := range nexts {
			_ = c.Close()
		}
		for i := range arms {
			sessionGone(t, h, fmt.Sprintf("dev%d", i))
		}
		if next311 != nil {
			next311.Disconnect(100)
			sessionGone(t, h, "dev311")
		}
		// A database's promise is a crash's; memory's is a graceful stop's.
		if strings.HasSuffix(t.Name(), "/sqlite") {
			h.Crash()
		} else {
			h.Stop()
		}
		h = restart()
		pub := connect(t, h, "pub-after", true, false)
		for i, arm := range arms {
			id := fmt.Sprintf("dev%d", i)
			back := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
			if got := readAnswers(t, back, 1); got[0] != "CONNACK 0x00 present=true" {
				t.Fatalf("%s: after the restart the session was answered %v", arm.name, got)
			}
			got := publishesUntilQuiet(t, back)
			if arm.present && !slices.Contains(got, "owed") {
				t.Errorf("%s: after the restart the session was sent %q: the delivery it resumed with is lost", arm.name, got)
			}
			if !arm.present && len(got) != 0 {
				t.Errorf("%s: after the restart the session was sent %q, which it was never owed", arm.name, got)
			}
			// What its record says it subscribes to: the predecessor's
			// subscriptions only where the session was resumed.
			pub.Pub(t, "loose/"+id, "again")
			pub.Pub(t, "iot/"+id+"/events/e", "again")
			var want []string
			if arm.present {
				want = []string{"again", "again"}
			}
			if got := publishesUntilQuiet(t, back); !slices.Equal(got, want) {
				t.Errorf("%s: after the restart the session was sent %q from its predecessor's subscriptions, want %q",
					arm.name, got, want)
			}
		}
		// The session the takeover began kept its own end-marker subscription,
		// and nothing of its predecessor's.
		var sent311 payloadsOf
		legacyWithDefault(t, h, "dev311", false, sent311.record)
		pub.Pub(t, "iot/dev311/events/e", "again")
		pub.Pub(t, "iot/dev311/events/marker", "marker-after")
		if got := sent311.until(t, "marker-after"); !slices.Equal(got, []string{"marker-after"}) {
			t.Errorf("a 3.1.1 Clean Session: after the restart the session was sent %q, want only its own "+
				"end marker: the rest are records of a channel only its predecessor subscribed to", got)
		}
	})
}

// RFC 0003 "Sessions": a session that outlives its connection, resumed by a
// connection with no Session Expiry Interval, ends with that connection - and
// so at a takeover, which begins a new session holding nothing the store kept
// for the old one. The resume stores nothing, the session not being kept, so
// the store still has the durable session's in-flight table under the id:
// a takeover that saved its own record over it and ended nothing else left
// that table to the new session, and a restart sent it what it was never
// owed. On both providers.
func TestATakeoverOfADurableSessionResumedWithNoExpiryKeepsNothingOfIt(t *testing.T) {
	eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
		const id = "dev"
		d := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
		if got := readAnswers(t, d, 1); got[0] != "CONNACK 0x00 present=false" {
			t.Fatalf("the first connection was answered %v", got)
		}
		subscribeConn(t, d, "loose/"+id)
		connect(t, h, "pub", true, false).Pub(t, "loose/"+id, "owed")
		if got := readAnswers(t, d, 1); !strings.HasPrefix(got[0], "PUBLISH") {
			t.Fatalf("the durable session was sent %v, want the delivery it will not acknowledge", got)
		}
		awaitInFlight(t, id, 1) // its in-flight entry stored
		_ = d.Close()
		sessionGone(t, h, id)

		e := rawCONNECT(t, h, connectFor(t, id, false, 0, ""))
		if got := readAnswers(t, e, 2); got[0] != "CONNACK 0x00 present=true" || !strings.HasPrefix(got[1], "PUBLISH") {
			t.Fatalf("the resume with no expiry was answered %v, want its session and the delivery again", got)
		}
		f := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
		if got := readAnswers(t, f, 1); got[0] != "CONNACK 0x00 present=false" {
			t.Fatalf("the takeover was answered %v, want a new session", got)
		}
		// The old session ends as the CONNACK is written, so a moment after it.
		var table []store.InFlight
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			var err error
			if _, table, err = brokertest.HarnessSessions.(interface {
				InFlight(string) (uint16, []store.InFlight, error)
			}).InFlight(id); err != nil {
				t.Fatalf("read the in-flight table: %v", err)
			}
			if len(table) == 0 || time.Now().After(deadline) {
				break
			}
		}
		if len(table) != 0 {
			t.Errorf("after the takeover the store keeps %d in-flight entries under the id, want none", len(table))
		}
		if got := publishesUntilQuiet(t, f); len(got) != 0 {
			t.Errorf("the new session was sent %q", got)
		}

		_ = f.Close()
		sessionGone(t, h, id)
		if strings.HasSuffix(t.Name(), "/sqlite") {
			h.Crash()
		} else {
			h.Stop()
		}
		h = restart()
		back := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
		if got := readAnswers(t, back, 1); got[0] != "CONNACK 0x00 present=true" {
			t.Fatalf("after the restart the new session was answered %v", got)
		}
		if got := publishesUntilQuiet(t, back); len(got) != 0 {
			t.Errorf("after the restart the new session was sent %q, which only the session it replaced was owed", got)
		}
	})
}

// RFC 0003 "Sessions": a CONNECT passed over for a newer one is answered what
// its turn would have answered it, and the session ends with the connection
// its turn would have left it with. Only a resume that outlives its
// connection is passed over, since only that one leaves the session to the
// next CONNECT as it found it: one that ends with its connection ends the
// session when the next takes it over. And the one passed over is answered
// whether the session was there to resume when the newer one claimed it -
// asked of the newer one itself, a newer CONNECT ending with its connection
// read as no session, and the older one took the id back from it.
func TestAPassedOverCONNECTIsAnsweredAsItsTurnWouldHaveBeen(t *testing.T) {
	var claims sync.Map // client id -> *atomic.Int64
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &claimsCounted{SessionStore: s, n: &claims}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	claimed := func(id string) int64 {
		n, _ := claims.LoadOrStore(id, &atomic.Int64{})
		return n.(*atomic.Int64).Load()
	}
	arms := []struct {
		name          string
		first, newest uint32 // Session Expiry Intervals
		answers       []string
		newestPresent bool
		claims        int64
	}{
		{name: "a durable resume behind one that ends with its connection", first: 3600, newest: 0,
			answers: []string{"CONNACK 0x00 present=true", "DISCONNECT 0x8E"}, newestPresent: true, claims: 1},
		{name: "a resume ending with its connection behind a durable one", first: 0, newest: 3600,
			answers: []string{"CONNACK 0x00 present=true", "DISCONNECT 0x8E"}, newestPresent: false, claims: 2},
	}
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		for i, arm := range arms {
			t.Run(arm.name, func(t *testing.T) {
				id := fmt.Sprintf("dev-%d", i)
				owner := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
					t.Fatalf("the owner was answered %v", p)
				}
				unlock := h.Srv.LockSession(id)
				var conns []net.Conn
				for n, expiry := range []uint32{arm.first, arm.newest} {
					conns = append(conns, rawCONNECT(t, h, connectFor(t, id, false, expiry, "")))
					for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < n+2; time.Sleep(time.Millisecond) {
						if time.Now().After(deadline) {
							unlock()
							t.Fatalf("CONNECT %d never waited for the id's session lock, so the order is not known", n+1)
						}
					}
				}
				claimsBefore := claimed(id)
				unlock()

				if got := readAnswers(t, conns[0], 2); !slices.Equal(got, arm.answers) {
					t.Errorf("the first CONNECT was answered %v, want %v", got, arm.answers)
				}
				want := fmt.Sprintf("CONNACK 0x00 present=%t", arm.newestPresent)
				if got := readAnswers(t, conns[1], 1); got[0] != want {
					t.Errorf("the newest was answered %v, want %s", got, want)
				}
				if got := readAnswers(t, owner, 1); got[0] != "DISCONNECT 0x8E" {
					t.Errorf("the owner was sent %v, want DISCONNECT 0x8E", got)
				}
				stillConnected(t, conns[1], "the newest CONNECT, which should own the session")
				if got := claimed(id) - claimsBefore; got != arm.claims {
					t.Errorf("%d CONNECTs claimed the session, want %d", got, arm.claims)
				}
				for _, c := range append(conns, owner) {
					_ = c.Close()
				}
			})
		}
	})
}

// RFC 0003 "Sessions": a resume passed over for a newer CONNECT is answered
// what the newer one found - here, no session to resume: an owner whose
// session ends with its connection, or no owner at all - and the newer one
// keeps the id. A passed-over resume told there was nothing to resume went
// back to the queue instead, ran after the newer CONNECT and took the id
// from it: answered Session Present 1, and the newer one 0x8E. On both
// providers, with the newer one resuming and starting clean.
func TestAPassedOverCONNECTIsAnsweredWhenItsSuccessorFindsNoSession(t *testing.T) {
	var claims sync.Map // client id -> *atomic.Int64
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &claimsCounted{SessionStore: s, n: &claims}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	claimed := func(id string) int64 {
		n, _ := claims.LoadOrStore(id, &atomic.Int64{})
		return n.(*atomic.Int64).Load()
	}
	arms := []struct {
		name        string
		owner       bool // one whose session ends with its connection
		newestClean bool
	}{
		{"an owner ending with its connection, the newest resuming", true, false},
		{"no owner, the newest resuming", false, false},
		{"an owner ending with its connection, the newest starting clean", true, true},
	}
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		for i, arm := range arms {
			t.Run(arm.name, func(t *testing.T) {
				id := fmt.Sprintf("nobody-%d", i)
				var owner net.Conn
				holders := 1 // the lock taken below
				if arm.owner {
					owner = rawCONNECT(t, h, connectFor(t, id, false, 0, ""))
					if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
						t.Fatalf("the owner was answered %v", p)
					}
				}
				unlock := h.Srv.LockSession(id)
				var conns []net.Conn
				for n, clean := range []bool{false, arm.newestClean} {
					conns = append(conns, rawCONNECT(t, h, connectFor(t, id, clean, 3600, "")))
					for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < holders+n+1; time.Sleep(time.Millisecond) {
						if time.Now().After(deadline) {
							unlock()
							t.Fatalf("CONNECT %d never waited for the id's session lock, so the order is not known", n+1)
						}
					}
				}
				claimsBefore := claimed(id)
				unlock()

				want := []string{"CONNACK 0x00 present=false", "DISCONNECT 0x8E"}
				if got := readAnswers(t, conns[0], 2); !slices.Equal(got, want) {
					t.Errorf("the passed-over resume was answered %v, want %v", got, want)
				}
				if got := readAnswers(t, conns[1], 1); got[0] != "CONNACK 0x00 present=false" {
					t.Errorf("the newest was answered %v, want a new session", got)
				}
				if owner != nil {
					if got := readAnswers(t, owner, 1); got[0] != "DISCONNECT 0x8E" {
						t.Errorf("the owner was sent %v, want DISCONNECT 0x8E", got)
					}
				}
				stillConnected(t, conns[1], "the newest CONNECT, which should own the session")
				// A claim reads the record of a session it takes over, so with no
				// owner there is none to count, and a second claim is the
				// passed-over CONNECT taking the id back from the newest.
				if got := claimed(id) - claimsBefore; arm.owner && got != 1 || !arm.owner && got != 0 {
					t.Errorf("%d CONNECTs read a session's record as they claimed it, want %d", got,
						map[bool]int{true: 1, false: 0}[arm.owner])
				}
				for _, c := range conns {
					_ = c.Close()
				}
			})
		}
	})
}

// sessionsHeldAtClaim holds keepSession's read of the predecessor's record
// while armed, and says when it has: the window between the broker's claim
// and the engine's takeover, both under the id's session lock.
type sessionsHeldAtClaim struct {
	broker.SessionStore
	armed   *atomic.Bool
	inGet   chan struct{}
	release chan struct{}
	// disconnects counts the disconnects written, so a test can tell a
	// DISCONNECT read and written from one still waiting.
	disconnects *atomic.Int64
}

func (s *sessionsHeldAtClaim) Disconnected(id string, at time.Time, dropWill bool, expiry uint32) error {
	if s.disconnects != nil {
		s.disconnects.Add(1)
	}
	return s.SessionStore.Disconnected(id, at, dropWill, expiry)
}

// awaitDisconnectRead waits until a DISCONNECT the owner sent inside a held
// claim has been read: waiting for the id's session lock the claim holds, or
// - read without that lock, the defect - written already.
func awaitDisconnectRead(t *testing.T, h *harness, id string, disconnects *atomic.Int64, before int64) {
	t.Helper()
	if !waitUntil(5*time.Second, func() bool {
		return h.Srv.SessionLockWaiters(id) >= 2 || disconnects.Load() > before
	}) {
		t.Fatal("the owner's DISCONNECT was never read, so it did not meet the held claim")
	}
}

func (s *sessionsHeldAtClaim) Snapshot() *store.SessionsSnapshot {
	if ex, ok := s.SessionStore.(interface {
		Snapshot() *store.SessionsSnapshot
	}); ok {
		return ex.Snapshot()
	}
	return nil
}

func (s *sessionsHeldAtClaim) InFlight(client string) (uint16, []store.InFlight, error) {
	return s.SessionStore.(interface {
		InFlight(string) (uint16, []store.InFlight, error)
	}).InFlight(client)
}

func (s *sessionsHeldAtClaim) Get(client string) (store.Session, bool, error) {
	if s.armed.Load() && onStack("keepSession") {
		s.armed.Store(false)
		s.inGet <- struct{}{}
		<-s.release
	}
	return s.SessionStore.Get(client)
}

// RFC 0003 "Sessions", invariant 17: a DISCONNECT's Session Expiry Interval is
// wholly before a takeover or not read at all. A durable owner, holding a
// delivery it has not acknowledged, sends DISCONNECT with an interval of 0:
//   - inside a CONNECT's claim, held there at keepSession's read: it waits
//     for the takeover and, from a connection taken over, changes nothing -
//     the session is resumed, whole, its delivery sent again;
//   - before the CONNECT: the session ends with that connection, and the
//     CONNECT begins a new one holding nothing of it.
//
// Written from the owner's read loop without the lock, the interval landed
// inside the claim: the broker resumed the session while the engine began a
// new one, answering Session Present 0 with the old session's in-flight entry
// left in the store, and after a restart sending it to the new session. On
// both providers, each order through a restart.
func TestADisconnectsExpiryIsWhollyBeforeATakeoverOrNotReadAtAll(t *testing.T) {
	var armed atomic.Bool
	var disconnects atomic.Int64
	inGet, release := make(chan struct{}, 1), make(chan struct{})
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &sessionsHeldAtClaim{SessionStore: s, armed: &armed, inGet: inGet, release: release,
			disconnects: &disconnects}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	disconnectEnding := func(t *testing.T, owner net.Conn) {
		t.Helper()
		d := pahopackets.NewControlPacket(pahopackets.DISCONNECT)
		zero := uint32(0)
		d.Content.(*pahopackets.Disconnect).Properties = &pahopackets.Properties{SessionExpiryInterval: &zero}
		if _, err := d.WriteTo(owner); err != nil {
			t.Fatalf("write DISCONNECT: %v", err)
		}
	}
	for _, during := range []bool{true, false} {
		name := map[bool]string{true: "inside the claim", false: "before the CONNECT"}[during]
		t.Run(name, func(t *testing.T) {
			eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
				const id = "dev"
				owner := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
					t.Fatalf("the owner was answered %v", p)
				}
				subscribeConn(t, owner, "loose/"+id)
				connect(t, h, "pub", true, false).Pub(t, "loose/"+id, "owed")
				if got := readAnswers(t, owner, 1); !strings.HasPrefix(got[0], "PUBLISH") {
					t.Fatalf("the owner was sent %v, want the delivery it will not acknowledge", got)
				}
				awaitInFlight(t, id, 1) // its in-flight entry stored

				var next net.Conn
				if during {
					armed.Store(true)
					next = rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
					select {
					case <-inGet:
					case <-time.After(5 * time.Second):
						t.Fatal("keepSession never read the predecessor's record, so the window was not held")
					}
					read := disconnects.Load()
					disconnectEnding(t, owner)
					awaitDisconnectRead(t, h, id, &disconnects, read)
					release <- struct{}{}
				} else {
					gone := h.Disconnects.Snapshot(id)
					disconnectEnding(t, owner)
					sessionGoneAfter(t, h, id, gone)
					next = rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				}

				want, wantSent := "CONNACK 0x00 present=true", []string{"owed"}
				if !during {
					want, wantSent = "CONNACK 0x00 present=false", nil
				}
				if got := readAnswers(t, next, 1); got[0] != want {
					t.Fatalf("the CONNECT was answered %v, want %s", got, want)
				}
				if got := publishesUntilQuiet(t, next); !slices.Equal(got, wantSent) {
					t.Errorf("the new connection was sent %q, want %q", got, wantSent)
				}
				stillConnected(t, next, "the connection that took the id")
				if !during {
					var table []store.InFlight
					for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
						var err error
						if _, table, err = brokertest.HarnessSessions.(interface {
							InFlight(string) (uint16, []store.InFlight, error)
						}).InFlight(id); err != nil {
							t.Fatalf("read the in-flight table: %v", err)
						}
						if len(table) == 0 || time.Now().After(deadline) {
							break
						}
					}
					if len(table) != 0 {
						t.Errorf("the new session's store keeps %d in-flight entries of the ended one", len(table))
					}
				}

				gone := h.Disconnects.Snapshot(id)
				_ = next.Close()
				sessionGoneAfter(t, h, id, gone)
				if strings.HasSuffix(t.Name(), "/sqlite") {
					h.Crash()
				} else {
					h.Stop()
				}
				h = restart()
				back := rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				if got := readAnswers(t, back, 1); got[0] != "CONNACK 0x00 present=true" {
					t.Fatalf("after the restart the session was answered %v", got)
				}
				if got := publishesUntilQuiet(t, back); !slices.Equal(got, wantSent) {
					t.Errorf("after the restart the session was sent %q, want %q", got, wantSent)
				}
			})
		})
	}
}

// RFC 0003 "Last Will", invariant 17: a clean DISCONNECT withdraws its Will
// only from a connection that still has the session. An owner with a Will
// and no delay sends DISCONNECT 0x00:
//   - inside a CONNECT's claim: it waits for the takeover and is not read,
//     so the Will goes as a taken-over connection's does - at once, its
//     delay being 0 - as mosquitto, reading nothing after a takeover, has it;
//   - before the CONNECT: the Will is withdrawn, and nothing is published.
//
// Read without the lock, the DISCONNECT inside the claim withdrew the Will of
// a connection whose session had already moved on. On both providers.
func TestADisconnectWithdrawsItsWillOnlyBeforeATakeover(t *testing.T) {
	var armed atomic.Bool
	var disconnects atomic.Int64
	inGet, release := make(chan struct{}, 1), make(chan struct{})
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &sessionsHeldAtClaim{SessionStore: s, armed: &armed, inGet: inGet, release: release,
			disconnects: &disconnects}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	for _, during := range []bool{true, false} {
		name := map[bool]string{true: "inside the claim", false: "before the CONNECT"}[during]
		t.Run(name, func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				const id = "dev"
				watch := connect(t, h, "watcher", true, false)
				watch.Sub(t, "status/#", 1)
				owner := rawCONNECT(t, h, connectWithDelayedWill(t, id, false, 3600, "status/"+id, "gone", 0))
				if p := readAnswers(t, owner, 1); p[0] != "CONNACK 0x00 present=false" {
					t.Fatalf("the owner was answered %v", p)
				}
				clean := func() {
					if _, err := pahopackets.NewControlPacket(pahopackets.DISCONNECT).WriteTo(owner); err != nil {
						t.Fatalf("write DISCONNECT: %v", err)
					}
				}
				var next net.Conn
				// The owner's teardown, which publishes a Will it still has.
				ownerGone := h.Disconnects.Snapshot(id)
				if during {
					armed.Store(true)
					next = rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
					select {
					case <-inGet:
					case <-time.After(5 * time.Second):
						t.Fatal("keepSession never read the predecessor's record, so the window was not held")
					}
					read := disconnects.Load()
					clean()
					awaitDisconnectRead(t, h, id, &disconnects, read)
					release <- struct{}{}
				} else {
					gone := h.Disconnects.Snapshot(id)
					clean()
					sessionGoneAfter(t, h, id, gone)
					next = rawCONNECT(t, h, connectFor(t, id, false, 3600, ""))
				}
				if got := readAnswers(t, next, 1); got[0] != "CONNACK 0x00 present=true" {
					t.Fatalf("the CONNECT was answered %v, want the session resumed", got)
				}
				var want []string
				if during {
					want = []string{"gone"}
					// **Ended before the marker is published.** The taken-over
					// connection's Will goes as it closes, after the CONNACK
					// that took it over: a marker published at the CONNACK
					// could arrive first, and a Will that was sent read as
					// none - 72 times in 300 on two loaded cores.
					sessionGoneAfter(t, h, id, ownerGone)
				}
				if got := awaitWills(t, h, watch, "after"); !slices.Equal(got, want) {
					t.Errorf("the watcher was sent %q, want %q", got, want)
				}
				_ = next.Close()
			})
		})
	}
}

// subscribeConn subscribes conn to each filter at QoS 1 and reads its SUBACK.
func subscribeConn(t *testing.T, conn net.Conn, filters ...string) {
	t.Helper()
	sub := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
	sub.Content.(*pahopackets.Subscribe).PacketID = 1
	for _, f := range filters {
		sub.Content.(*pahopackets.Subscribe).Subscriptions = append(sub.Content.(*pahopackets.Subscribe).Subscriptions,
			pahopackets.SubOptions{Topic: f, QoS: 1})
	}
	if _, err := sub.WriteTo(conn); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	if got := readAnswers(t, conn, 1); !strings.HasPrefix(got[0], "SUBACK") {
		t.Fatalf("the SUBSCRIBE was answered %v", got)
	}
}

// publishesUntilQuiet reads what conn is sent until it has been sent nothing
// for 700ms, and answers the payloads of its PUBLISHes, sorted: what arrives
// on a channel and on broadcast has no order between them.
func publishesUntilQuiet(t *testing.T, conn net.Conn) []string {
	t.Helper()
	var got []string
	for {
		_ = conn.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
		p, err := pahopackets.ReadPacket(conn)
		if err != nil {
			var ne net.Error
			if !errors.As(err, &ne) || !ne.Timeout() {
				t.Errorf("reading what the connection was sent: %v", err)
			}
			_ = conn.SetReadDeadline(time.Time{})
			slices.Sort(got)
			return got
		}
		if pub, ok := p.Content.(*pahopackets.Publish); ok {
			got = append(got, string(pub.Payload))
			continue
		}
		t.Errorf("the connection was sent %s", p)
	}
}

// claimsCounted counts, per client id, the CONNECTs that read the session's
// record as they claimed it (keepSession): every claim with a connection to
// take the id from reads it first.
type claimsCounted struct {
	broker.SessionStore
	n *sync.Map
}

func (s *claimsCounted) Get(client string) (store.Session, bool, error) {
	if onStack("keepSession") {
		n, _ := s.n.LoadOrStore(client, &atomic.Int64{})
		n.(*atomic.Int64).Add(1)
	}
	return s.SessionStore.Get(client)
}

// rawCONNECT dials the broker and writes connect, reading nothing.
func rawCONNECT(t *testing.T, h *harness, connect []byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(connect); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	return conn
}

// readAnswers reads n packets from conn, each described as the test compares
// it, or what ended the read in its place.
func readAnswers(t *testing.T, conn net.Conn, n int) []string {
	t.Helper()
	var got []string
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for len(got) < n {
		p, err := pahopackets.ReadPacket(conn)
		if err != nil {
			return append(got, "read: "+err.Error())
		}
		switch c := p.Content.(type) {
		case *pahopackets.Connack:
			got = append(got, fmt.Sprintf("CONNACK 0x%02X present=%t", c.ReasonCode, c.SessionPresent))
		case *pahopackets.Disconnect:
			got = append(got, fmt.Sprintf("DISCONNECT 0x%02X", c.ReasonCode))
		default:
			got = append(got, p.String())
		}
	}
	return got
}

// stillConnected fails unless conn answers a PINGREQ, past any deliveries
// sent to it first.
func stillConnected(t *testing.T, conn net.Conn, what string) {
	t.Helper()
	if _, err := pahopackets.NewControlPacket(pahopackets.PINGREQ).WriteTo(conn); err != nil {
		t.Errorf("%s: write PINGREQ: %v", what, err)
		return
	}
	for range 10 {
		got := readAnswers(t, conn, 1)
		if strings.HasPrefix(got[0], "PUBLISH") {
			continue
		}
		if !strings.HasPrefix(got[0], "PINGRESP") {
			t.Errorf("%s answered a PINGREQ with %v, so it is not connected", what, got)
		}
		return
	}
	t.Errorf("%s was sent ten deliveries and no PINGRESP", what)
}

// getFailsOnce fails one Get while armed.
type getFailsOnce struct {
	broker.SessionStore
	armed *atomic.Bool
}

func (s *getFailsOnce) Get(id string) (store.Session, bool, error) {
	if s.armed.CompareAndSwap(true, false) {
		return store.Session{}, false, errors.New("disk I/O error")
	}
	return s.SessionStore.Get(id)
}

// A resume the session store cannot read is refused, rather than saved over
// with nothing where its subscriptions were.
//
// It used to be logged and saved anyway, with the subscriptions left empty,
// so one transient read error erased them: after a restart the client was
// told Session Present 1 and sent nothing, which is refusedNow's own
// description of the worst outcome - "a client that believes it is
// subscribed to something the broker will never send it".
func TestAResumeTheStoreCannotReadIsRefusedRatherThanSavedOver(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &getFailsOnce{SessionStore: s, armed: &armed}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		armed.Store(false)
		dev := dial(t, h, "dev", false, false, 0, 3600, 0)
		dev.Sub(t, "events/#", 1)
		_ = dev.C.Disconnect(&paho.Disconnect{})
		sessionGone(t, h, "dev")

		armed.Store(true)
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write(connectFor(t, "dev", false, 3600, "")); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		ack, err := pahopackets.ReadPacket(conn)
		if armed.Load() {
			t.Fatal("the session store was never read at the resume, so this proves nothing")
		}
		if err != nil {
			t.Fatalf("no answer to the CONNECT: %v", err)
		}
		if ca, ok := ack.Content.(*pahopackets.Connack); !ok || ca.ReasonCode != 0x83 {
			t.Errorf("a resume the store could not read was answered %s, want CONNACK 0x83", ack)
		}

		sess, ok, err := brokertest.HarnessSessions.Get("dev")
		if err != nil || !ok || len(sess.Subscriptions) != 1 {
			t.Fatalf("after one failed read the stored session holds %d subscriptions (%v, %v), want 1: "+
				"a resume saved over what it could not read", len(sess.Subscriptions), ok, err)
		}
	})
}

// A client that comes back after its session expired, before the sweep has
// reached it, is given nothing that session left: not its unreleased
// exactly-once publish in place of a new one, and not its stored position in
// place of the floor. It is told Session Present 0, and it has to be true.
//
// The engine used to end such a session only as it was inherited, after
// saguin had already recorded the new connection as the id's owner, so
// saguin's expiry work found somebody else owning the id and did nothing.
// The new QoS 2 publish was acknowledged PUBREC and PUBCOMP 0x00, and the
// old one written in its place; the new subscription was served from the
// expired session's position.
//
// **Each trial comes back 30ms after the expiry**, inside the second before
// the sweep, and the engine's own line for a session ended as its client
// came back is what proves a trial landed there: one that did not is the
// sweep's case, which was never broken.
func TestAClientBackAfterItsSessionExpiredStartsFromNothing(t *testing.T) {
	run := func(t *testing.T, start func(onLine func(string)) *harness) {
		var hit atomic.Int64
		h := start(func(line string) {
			if strings.Contains(line, "a session expired as its client came back") {
				hit.Add(1)
			}
		})
		p := connect(t, h, "publisher", true, false)
		for _, v := range []string{"one", "two", "three"} {
			p.Pub(t, "events/x", v)
		}
		const trials = 4
		for i := 0; i < trials; i++ {
			id := fmt.Sprintf("back-%d", i)
			w, _ := qos2Dial(t, h.Addr, id, true, 1)
			w.Publish(1, "events/x", fmt.Sprintf("OLD-%d", i), false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("PUBREC 0x%02X for the publish the session is to leave unreleased", rc)
			}
			rawSubscribe(t, w, 2, 0, pahopackets.SubOptions{Topic: "events/#", QoS: 1})
			// Everything the channel serves is read, and only one and two
			// are acknowledged, so the stored position is at three.
			sawThree := false
			for {
				cp, err := w.Read(400 * time.Millisecond)
				if err != nil {
					break
				}
				pub, ok := cp.Content.(*pahopackets.Publish)
				if !ok {
					continue
				}
				switch string(pub.Payload) {
				case "one", "two":
					ack := pahopackets.NewControlPacket(pahopackets.PUBACK)
					ack.Content.(*pahopackets.Puback).PacketID = pub.PacketID
					if _, err := ack.WriteTo(w.C); err != nil {
						t.Fatalf("write PUBACK: %v", err)
					}
				case "three":
					sawThree = true
				}
			}
			if !sawThree {
				t.Fatalf("%s was never served three, so its position is not where this needs it", id)
			}
			kill(t, w.C)
			sessionGone(t, h, id)
			time.Sleep(1030 * time.Millisecond)

			back, ca := qos2Dial(t, h.Addr, id, false, 3600)
			if ca.SessionPresent {
				t.Fatalf("%s came back after its session expired and was told Session Present 1", id)
			}
			back.Publish(1, "events/x", fmt.Sprintf("NEW-%d", i), false)
			if rc := back.Pubrec(1); rc != 0 {
				t.Fatalf("PUBREC 0x%02X for the new session's publish", rc)
			}
			back.Pubrel(1)
			if rc := back.Pubcomp(1); rc != 0 {
				t.Fatalf("PUBCOMP 0x%02X for the new session's publish", rc)
			}
			rawSubscribe(t, back, 3, 0, pahopackets.SubOptions{Topic: "events/#", QoS: 1})
			if first := nextPublish(t, back); string(first.Payload) != "one" {
				t.Errorf("%s, told Session Present 0, was served from %q rather than the floor: "+
					"the expired session's stored position outlived it", id, first.Payload)
			}
			back.Close()
		}
		if hit.Load() == 0 {
			t.Fatalf("none of %d trials came back before the sweep ended the session, so this proves nothing", trials)
		}
		for i := 0; i < trials; i++ {
			if n := inChannel(t, h, fmt.Sprintf("reader-%d", i), fmt.Sprintf("NEW-%d", i)); n != 1 {
				t.Errorf("NEW-%d is in the channel %d times after PUBCOMP 0x00, want 1", i, n)
			}
			if n := inChannel(t, h, fmt.Sprintf("reader-old-%d", i), fmt.Sprintf("OLD-%d", i)); n != 0 {
				t.Errorf("OLD-%d, the expired session's unreleased publish, is in the channel %d times", i, n)
			}
		}
		t.Logf("%d of %d trials came back before the sweep", hit.Load(), trials)
	}
	t.Run("memory", func(t *testing.T) {
		run(t, func(onLine func(string)) *harness { return startLoggingDebug(t, onLine) })
	})
	t.Run("sqlite", func(t *testing.T) {
		run(t, func(onLine func(string)) *harness {
			return startLoggingDebugSQLite(t, onLine, filepath.Join(t.TempDir(), "s.db"))
		})
	})
}

// A Will the session store could not keep refuses the connection with the
// code for what went wrong, and is counted as the refusal it is.
//
// **Two causes, two answers**: 0x97 says the provider has no room, which is
// true of a full store and false of a disk error, so a write that failed for
// another reason is 0x83 with a storage error counted - it was 0x97 "has no
// room" either way, which sends an operator looking for space that is there.
// And **neither is a dropped session**: saguin_sessions_dropped_total counts
// clients that were accepted and told their session ends with the
// connection (RFC 0005), which a refused client was not. It was counted there
// as well as among the refusals.
func TestAWillTheStoreCouldNotKeepIsRefusedForWhatWentWrong(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		code    byte
		refused string
		storage bool
	}{
		{"full", store.ErrFull, 0x97, `saguin_connections_refused_total{reason="session store full"}`, false},
		{"disk error", errors.New("disk I/O error"), 0x83, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var armed atomic.Bool
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				return &refuseWillsWhen{SessionStore: s, armed: &armed, err: tc.err}
			}
			t.Cleanup(func() { brokertest.WrapSessions = nil })
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				ops := operationsAt(t, h)
				storageErrors := func() float64 {
					n := 0.0
					for k, v := range scrapeGauges(t, ops) {
						if strings.HasPrefix(k, "saguin_storage_errors_total") {
							n += v
						}
					}
					return n
				}
				before := storageErrors()
				armed.Store(true)
				conn, err := net.Dial("tcp", h.Addr)
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				if _, err := conn.Write(connectFor(t, "armed", true, 0, "wills/armed")); err != nil {
					t.Fatalf("write CONNECT: %v", err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				ack, err := pahopackets.ReadPacket(conn)
				armed.Store(false)
				if err != nil {
					t.Fatalf("no answer to the CONNECT: %v", err)
				}
				ca, ok := ack.Content.(*pahopackets.Connack)
				if !ok || ca.ReasonCode != tc.code {
					t.Fatalf("a Will the store failed with %q was answered %s, want CONNACK 0x%02X", tc.err, ack, tc.code)
				}
				if tc.code != 0x97 && ca.Properties != nil && strings.Contains(ca.Properties.ReasonString, "no room") {
					t.Errorf("a disk error was explained as %q", ca.Properties.ReasonString)
				}
				// Waited for, because a scrape answered within a millisecond of
				// the last is that one's body again; the zeros below are read
				// from the scrape that shows the count.
				gauges := scrapeGauges(t, ops)
				if tc.refused != "" && !waitUntil(3*time.Second, func() bool {
					gauges = scrapeGauges(t, ops)
					return gauges[tc.refused] == 1
				}) {
					t.Errorf("%s is %v, want 1", tc.refused, gauges[tc.refused])
				}
				if tc.storage && !waitUntil(3*time.Second, func() bool { return storageErrors() > before }) {
					t.Errorf("a disk error writing a Will was not counted as a storage error")
				}
				gauges = scrapeGauges(t, ops)
				if n := gauges[`saguin_sessions_dropped_total{cause="storage_full"}`]; n != 0 {
					t.Errorf("a refused client was counted as a dropped session (%v): that counter is for "+
						"clients that were accepted", n)
				}
				if got := storageErrors() - before; !tc.storage && got != 0 {
					t.Errorf("a full store was counted as %v storage errors", got)
				}
			})
		})
	}
}

// releaseFailsOnce fails one release of a held publish while armed, leaving
// the hold where it was, as a store whose write failed does.
type releaseFailsOnce struct {
	broker.HoldStore
	armed *atomic.Bool
}

func (p releaseFailsOnce) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	if p.armed.CompareAndSwap(true, false) {
		return store.Record{}, true, errors.New("disk I/O error")
	}
	return p.HoldStore.ReleaseHold(e)
}

// A release the channel's store cannot write is refused rather than
// completed: no PUBCOMP tells the client a message was delivered that nothing
// holds - the connection is closed with 0x83, which says so - and the publish
// stays held, so the client's next PUBREL completes it, once.
func TestAReleaseTheStoreCannotReadIsNotCompleted(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapHolds = func(_ string, h broker.HoldStore) broker.HoldStore {
		return releaseFailsOnce{HoldStore: h, armed: &armed}
	}
	t.Cleanup(func() { brokertest.WrapHolds = nil })
	eachProvider(t, func(t *testing.T, h *harness) {
		armed.Store(false)
		w, _ := qos2Dial(t, h.Addr, "publisher", false, 3600)
		w.Publish(1, "events/once", "once", false)
		if rc := w.Pubrec(1); rc != 0 {
			t.Fatalf("PUBREC 0x%02X", rc)
		}

		armed.Store(true)
		w.Pubrel(1)
		if cp, err := w.Read(time.Second); err == nil {
			d, ok := cp.Content.(*pahopackets.Disconnect)
			if !ok || d.ReasonCode != 0x83 {
				t.Fatalf("a PUBREL the store could not write was answered %s: want DISCONNECT 0x83, and "+
					"never a completion for a message nothing holds", cp)
			}
			if cp, err := w.Read(time.Second); err == nil {
				t.Fatalf("after the DISCONNECT the connection sent %s, want it closed", cp.PacketType())
			}
		}
		if armed.Load() {
			t.Fatal("the store was never asked to release the publish, so this proves nothing")
		}

		// The connection is closed on a rejected PUBREL, so the retry is
		// the one MQTT gives a client: resume, and send the PUBREL again
		// under the same identifier (4.4.0-1).
		w.Close()
		sessionGone(t, h, "publisher")
		again, ca := qos2Dial(t, h.Addr, "publisher", false, 3600)
		if !ca.SessionPresent {
			t.Fatal("the session holding the unreleased publish was not resumed")
		}
		// The resumed session is sent its PUBREC again first: the exchange
		// is still waiting for the PUBREL.
		if rc := again.Pubrec(1); rc != 0 {
			t.Fatalf("the resumed session's PUBREC is 0x%02X", rc)
		}
		again.Pubrel(1)
		if rc := again.Pubcomp(1); rc != 0 {
			t.Fatalf("the retried PUBREL was answered PUBCOMP 0x%02X", rc)
		}
		if n := inChannel(t, h, "reader", "once"); n != 1 {
			t.Fatalf("the publish is in the channel %d times after its retried release, want 1", n)
		}
	})
}

// A delivery a session was owed comes back after a restart with what it
// carried: its User Properties, and a Message Expiry Interval that has gone
// on counting down (invariant 15 - a session's own messages come back with
// it). The session store keeps both, and the packet the returning client is
// sent is rebuilt from what it kept.
func TestARestoredDeliveryKeepsItsPropertiesAndItsExpiry(t *testing.T) {
	eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
		dev := dial(t, h, "dev", false, false, 0, 3600, 0)
		dev.Sub(t, "loose/#", 1)
		_ = dev.C.Disconnect(&paho.Disconnect{})
		sessionGone(t, h, "dev")

		expiry := uint32(600)
		p := connect(t, h, "publisher", true, false)
		p.PubProps(t, "loose/x", "owed", &paho.PublishProperties{
			User:          paho.UserProperties{{Key: "tenant", Value: "north"}},
			MessageExpiry: &expiry,
		})
		awaitOwed(t, operationsAt(t, h), "the delivery owed to the away session", 1)
		h.Stop()

		h2 := restart()
		back := dial(t, h2, "dev", false, false, 0, 3600, 0)
		r, ok := back.Await(t, 3*time.Second)
		if !ok || r.Payload != "owed" {
			t.Fatalf("the restored session was not sent what it was owed (%q, %v)", r.Payload, ok)
		}
		if r.User["tenant"] != "north" {
			t.Errorf("the restored delivery carries User Properties %v, want tenant=north", r.User)
		}
		if r.Expiry == nil || *r.Expiry == 0 || *r.Expiry > expiry {
			t.Errorf("the restored delivery's Message Expiry is %v, want the publisher's 600 counted down", r.Expiry)
		}
	})
}

// heldConnack is a connection whose CONNECT arrives and whose first write -
// the CONNACK - waits until the test lets it go, then fails or goes through.
// Later writes are discarded, and reads block until it is closed, so a
// connection whose CONNACK went through stays connected.
type heldConnack struct {
	connect  *bytes.Reader
	entered  chan struct{}
	release  chan bool // true: the CONNACK is written; false: the write fails
	first    sync.Once
	closed   chan struct{}
	closeOne sync.Once
}

func newHeldConnack(connect []byte) *heldConnack {
	return &heldConnack{connect: bytes.NewReader(connect), entered: make(chan struct{}),
		release: make(chan bool, 1), closed: make(chan struct{})}
}

func (c *heldConnack) Read(p []byte) (int, error) {
	if c.connect.Len() > 0 {
		return c.connect.Read(p)
	}
	<-c.closed
	return 0, io.EOF
}

func (c *heldConnack) Write(p []byte) (int, error) {
	ok := true
	c.first.Do(func() {
		close(c.entered)
		ok = <-c.release
	})
	if !ok {
		return 0, errors.New("connection reset by peer")
	}
	return len(p), nil
}

func (c *heldConnack) Close() error {
	c.closeOne.Do(func() { close(c.closed) })
	return nil
}
func (c *heldConnack) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1883}
}
func (c *heldConnack) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50001}
}
func (c *heldConnack) SetDeadline(time.Time) error      { return nil }
func (c *heldConnack) SetReadDeadline(time.Time) error  { return nil }
func (c *heldConnack) SetWriteDeadline(time.Time) error { return nil }

// RFC 0003 "Sessions: one owner, one ending" - the Will row, while the
// connection taking over is not yet answered
//
// **A Will whose connection closes while another connection's claim on its
// client id is still pending waits for that claim to settle.** Ownership
// moves after the CONNACK, so until then the session is still the closing
// connection's: a CONNACK that never arrives gives the id back and the Will
// waits out its delay as if nothing had happened; a resume cancels it
// [MQTT-3.1.3-9]; a clean start publishes it, because its session ended.
//
// The new connection's CONNACK is held at its write while the old connection
// is cut, and the Will is seen waiting for the claim, so a row where it did
// not cannot pass: on the id's session lock, which the new connection holds
// until it is registered or its CONNACK has failed, or - in the moment
// between a failed CONNACK letting that lock go and its claim being given
// back - on the claim itself, which the broker's own line says.
func TestAWillWaitsForTheClaimOnItsClientID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		written bool // the new connection's CONNACK goes through
		clean   bool
		want    bool // the old Will is published
		early   bool // at once, rather than after its delay
	}{
		{"the CONNACK never arrives", false, false, true, false},
		{"a resume", true, false, false, false},
		{"a clean start", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var waited atomic.Int64
			h := startLoggingDebug(t, func(line string) {
				if strings.Contains(line, "a Will waits for the connection that took its client id") {
					waited.Add(1)
				}
			})
			watch := connect(t, h, "watcher", true, false)
			watch.Sub(t, "loose/#", 1)
			old, code := connectWithWillExpiring(t, h, "dev", "loose/will", false, 2, 0, 3600)
			if code != 0 {
				t.Fatalf("the old connection was refused 0x%02X", code)
			}

			// With a Will of its own, on a topic the watcher does not read:
			// it is the new session's, and must not be given the old one's
			// due moment.
			fresh := newHeldConnack(connectFor(t, "dev", tc.clean, 3600, "elsewhere/new"))
			// A row that fails before letting the CONNACK go must not leave
			// the broker's stop waiting on it: the write is let go first.
			t.Cleanup(func() {
				select {
				case fresh.release <- false:
				default:
				}
				_ = fresh.Close()
			})
			done := make(chan error, 1)
			go func() { done <- h.Srv.EstablishConnection("t", fresh) }()
			select {
			case <-fresh.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("the new connection never reached its CONNACK")
			}
			_ = old.Close()
			if !waitUntil(3*time.Second, func() bool {
				return waited.Load() > 0 || h.Srv.SessionLockWaiters("dev") == 2
			}) {
				t.Fatal("the old Will never waited for the claim, so this proves nothing")
			}
			began := time.Now()
			fresh.release <- tc.written

			r, got := watch.Await(t, 4*time.Second)
			if got != tc.want {
				t.Fatalf("the old Will was published %v, want %v (%v)", got, tc.want, r.Payload)
			}
			if got && tc.early != (time.Since(began) < time.Second) {
				t.Errorf("the old Will arrived after %v: want it at once %v", time.Since(began), tc.early)
			}
			if tc.written {
				sess, ok, err := brokertest.HarnessSessions.Get("dev")
				switch {
				case err != nil || !ok || sess.Will == nil || sess.Will.Topic != "elsewhere/new":
					t.Errorf("the new session holds %+v (%v, %v), want its own Will", sess.Will, ok, err)
				case !sess.Will.DueAt.IsZero():
					t.Errorf("the new session's Will is due at %v, with its client connected: a restart "+
						"would announce it", sess.Will.DueAt)
				}
			}
			if !tc.written {
				select {
				case err := <-done:
					if err == nil || !strings.Contains(err.Error(), "ack connection packet") {
						t.Errorf("the new connection ended with %v rather than at its CONNACK", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("the broker never finished with the connection whose CONNACK failed")
				}
			}
		})
	}
}

// **A connection's teardown that finds another connection's claim on its
// client id still pending waits with that claim, and runs when the claim is
// given back.** The old connection closes while the new one's CONNACK is
// held at its write, so its teardown queues on the id's session lock behind
// the new connection and has the lock first once the CONNACK fails - before
// the failed connection's own disconnect gives the claim back. The id is the
// old session's again, and the teardown it deferred is what records that
// session's disconnect; lost, the record keeps no disconnect time, and the
// session's expiry is never counted from anything.
func TestATeardownDeferredOntoAClaimRunsWhenTheClaimIsGivenBack(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		old := dialRaw(t, h, connectFor(t, "dev", false, 3600, ""))
		if sess, ok, err := brokertest.HarnessSessions.Get("dev"); err != nil || !ok || !sess.DisconnectedAt.IsZero() {
			t.Fatalf("the connected session's record is %+v (%v, %v), want one with no disconnect time", sess, ok, err)
		}

		fresh := newHeldConnack(connectFor(t, "dev", false, 3600, ""))
		t.Cleanup(func() {
			select {
			case fresh.release <- false:
			default:
			}
			_ = fresh.Close()
		})
		done := make(chan error, 1)
		go func() { done <- h.Srv.EstablishConnection("t", fresh) }()
		select {
		case <-fresh.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("the new connection never reached its CONNACK")
		}
		_ = old.Close()
		if !waitUntil(3*time.Second, func() bool { return h.Srv.SessionLockWaiters("dev") == 2 }) {
			t.Fatal("the old connection's teardown never queued behind the claim, so this proves nothing")
		}
		fresh.release <- false
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "ack connection packet") {
				t.Fatalf("the new connection ended with %v rather than at its CONNACK", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the broker never finished with the connection whose CONNACK failed")
		}

		var sess store.Session
		if !waitUntil(3*time.Second, func() bool {
			got, ok, err := brokertest.HarnessSessions.Get("dev")
			sess = got
			return err == nil && ok && !got.DisconnectedAt.IsZero()
		}) {
			t.Errorf("the session's record is %+v after its connection closed and the claim was given back: "+
				"no disconnect time, so its expiry is never counted", sess)
		}
	})
}

// heldCall holds one store call for one client id open, from the moment it is
// armed until it is let go: the moment a session's ending, or a delayed Will,
// has decided the id is still its own and is about to write to it.
type heldCall struct {
	id string
	// on, where set, holds only a call made from that function, so a hold
	// can be armed early for a call that comes after others on the same id.
	on      string
	armed   atomic.Bool
	entered chan struct{} // closed when the held call arrives
	release chan struct{} // closed to let it go
	left    chan struct{} // closed when it has returned
	once    sync.Once
}

func newHeldCall(id string) *heldCall {
	return &heldCall{id: id, entered: make(chan struct{}), release: make(chan struct{}),
		left: make(chan struct{})}
}

// at runs call, holding it first if it is the call the hold is armed for.
func (h *heldCall) at(id string, call func()) {
	if id != h.id || (h.on != "" && !onStack(h.on)) || !h.armed.CompareAndSwap(true, false) {
		call()
		return
	}
	close(h.entered)
	<-h.release
	defer close(h.left)
	call()
}

func (h *heldCall) let() { h.once.Do(func() { close(h.release) }) }

// await waits for one of the hold's moments, failing with what was expected.
func (h *heldCall) await(t *testing.T, moment chan struct{}, what string) {
	t.Helper()
	select {
	case <-moment:
	case <-time.After(10 * time.Second):
		t.Fatalf("the held call never %s, so nothing here was exercised", what)
	}
}

// The wrappers read the hold when called rather than when built, because a
// harness wraps its stores as it starts and a hold is made for each one - and
// through an atomic pointer, because the harness's own goroutines call through
// a wrapper while an arm stores its hold: its start's, and a previous arm's
// teardown still running. Read and written plainly, that raced.
type holdsHeldAtDrop struct {
	broker.HoldStore
	hold *atomic.Pointer[heldCall]
}

func (s holdsHeldAtDrop) DropHold(e store.Exchange) (gone bool, err error) {
	s.hold.Load().at(e.Client, func() { gone, err = s.HoldStore.DropHold(e) })
	return gone, err
}

type sessionsHeldAtGet struct {
	broker.SessionStore
	hold *atomic.Pointer[heldCall]
}

func (s *sessionsHeldAtGet) Get(id string) (sess store.Session, ok bool, err error) {
	s.hold.Load().at(id, func() { sess, ok, err = s.SessionStore.Get(id) })
	return sess, ok, err
}

// sessionsHeldAtDisconnect holds the session store's record of a disconnect,
// the one write recordDisconnect makes (SessionStore.Disconnected).
type sessionsHeldAtDisconnect struct {
	broker.SessionStore
	hold *atomic.Pointer[heldCall]
}

func (s *sessionsHeldAtDisconnect) Disconnected(id string, at time.Time, dropWill bool, expiry uint32) (err error) {
	s.hold.Load().at(id, func() { err = s.SessionStore.Disconnected(id, at, dropWill, expiry) })
	return err
}

// connectWithDelayedWill is an MQTT 5 CONNECT carrying a Will with a delay.
func connectWithDelayedWill(t *testing.T, id string, clean bool, expiry uint32,
	topic, payload string, delay uint32) []byte {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	c := cp.Content.(*pahopackets.Connect)
	c.ClientID, c.CleanStart, c.KeepAlive = id, clean, 0
	c.Properties = &pahopackets.Properties{SessionExpiryInterval: &expiry}
	c.WillFlag, c.WillTopic, c.WillMessage = true, topic, []byte(payload)
	c.WillProperties = &pahopackets.Properties{WillDelayInterval: &delay}
	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}
	return buf.Bytes()
}

// dialRaw writes a CONNECT and returns the connection once it is accepted.
func dialRaw(t *testing.T, h *harness, connect []byte) net.Conn {
	t.Helper()
	conn, ca := connectPastHold(t, h, nil, connect)
	if ca.ReasonCode != 0 {
		t.Fatalf("the CONNECT was refused 0x%02X", ca.ReasonCode)
	}
	return conn
}

// connectPastHold connects under a client id whose ending hold is holding,
// and returns once the connection is answered.
//
// **It works whichever side of the fix it runs on**, which is what lets it
// show the defect and its absence alike. Where the ending holds the id's
// session lock, the connection waits for it - and the hold is let go the
// moment the broker has it waiting, which it says rather than a clock
// guessing. Where nothing holds that lock, the connection is answered inside
// the hold, and whatever it then does lands before the ending's writes.
func connectPastHold(t *testing.T, h *harness, hold *heldCall, connect []byte) (net.Conn, *pahopackets.Connack) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", h.Addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(connect); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	type answer struct {
		cp  *pahopackets.ControlPacket
		err error
	}
	got := make(chan answer, 1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		cp, err := pahopackets.ReadPacket(conn)
		got <- answer{cp, err}
	}()
	for {
		select {
		case a := <-got:
			if a.err != nil {
				t.Fatalf("no answer to the CONNECT: %v", a.err)
			}
			ca, ok := a.cp.Content.(*pahopackets.Connack)
			if !ok {
				t.Fatalf("the CONNECT was answered %s, want a CONNACK", a.cp.PacketType())
			}
			_ = conn.SetReadDeadline(time.Time{})
			return conn, ca
		case <-time.After(5 * time.Millisecond):
			if hold != nil && h.Srv.SessionLockWaiters(hold.id) >= 2 {
				hold.let()
			}
		}
	}
}

// awaitWills reads what the watcher is sent until the marker a producer
// publishes after it, and returns the Wills in between.
//
// **The marker goes at QoS 0, as these Wills do.** A durable watcher is sent
// QoS 1 from the broadcast log, written as its drain reaches it, and QoS 0
// through its connection's queue, and nothing orders the two (MQTT orders
// one publisher's messages on one topic at one QoS): a QoS 1 marker was
// written 11us ahead of a Will queued 2 ms before it, once in 194 runs on two
// cores. At QoS 0 the marker joins the queue behind any Will already on it.
func awaitWills(t *testing.T, h *harness, watch *client, marker string) []string {
	t.Helper()
	p := connect(t, h, "marker-"+marker, true, false)
	p.PubPlainAt(t, "status/marker", marker, 0)
	var wills []string
	for {
		r, ok := watch.Await(t, 5*time.Second)
		if !ok {
			t.Fatalf("the marker %q never arrived, so what arrived before it is not known", marker)
		}
		if r.Payload == marker {
			return wills
		}
		wills = append(wills, r.Payload)
	}
}

// invariant 17, and RFC 0003 "Sessions": only the connection that owns a
// client id changes what the broker keeps for it.
//
// **Each of these decides the id is still its own, and then writes to what
// the id keeps**: a session ending with its connection, a clean start ending
// the session it replaced, a delayed Will falling due, a delayed Will being
// held, and a disconnect keeping its session. The decision was made under
// b.mu and the writes after it, since they are store writes, so a connection
// could claim the id in between - or claim it and leave, which empties the
// owner entry each of them reads as nobody's. That connection then lost what
// the writes removed or overwrote: a publish it had been sent PUBREC for was
// answered PUBCOMP and never reached its channel, its session record went,
// and its own Will was published while it was connected and never when it
// went. Each arm holds the write open, connects under the id, and says what
// that connection kept. The id's session lock is what orders them now.
func TestASessionThatNoLongerOwnsItsIDChangesNothingTheIDKeeps(t *testing.T) {
	const id = "lockgap"
	for _, arm := range []struct {
		name string
		// atDrop holds the removal of an exactly-once publish the ending
		// session held, atDisconnect the session store's record of a
		// disconnect, and otherwise the session store's read of the id.
		atDrop, atDisconnect bool
		// on is the function whose call alone the hold holds (heldCall.on).
		on  string
		run func(t *testing.T, h *harness, hold *heldCall)
	}{{
		name:   "a disconnect that ends its session",
		atDrop: true,
		run: func(t *testing.T, h *harness, hold *heldCall) {
			first, _ := qos2Dial(t, h.Addr, id, false, 0)
			// An exchange of its own for the ending to drop.
			first.Publish(3, "events/lockgap/0", "ended", false)
			if rc := first.Pubrec(3); rc != 0 {
				t.Fatalf("the ending session's publish was answered 0x%02X", rc)
			}
			hold.armed.Store(true)
			first.Close()
			hold.await(t, hold.entered, "arrived: the session did not end at its disconnect")
			claimsTheID(t, h, hold, false)
		},
	}, {
		name:   "a clean start ending the session it replaced",
		atDrop: true,
		run: func(t *testing.T, h *harness, hold *heldCall) {
			// The session the clean start replaces, holding an exchange for
			// the ending to drop.
			zero, _ := qos2Dial(t, h.Addr, id, false, 300)
			zero.Publish(3, "events/lockgap/0", "replaced", false)
			if rc := zero.Pubrec(3); rc != 0 {
				t.Fatalf("the replaced session's publish was answered 0x%02X", rc)
			}
			before := h.Disconnects.Snapshot(id)
			zero.Close()
			sessionGoneAfter(t, h, id, before)
			hold.armed.Store(true)
			first, _ := qos2Dial(t, h.Addr, id, true, 300)
			defer first.Close()
			hold.await(t, hold.entered, "arrived: the clean start ended nothing")
			claimsTheID(t, h, hold, true)
		},
	}, {
		name: "a delayed Will falling due",
		on:   "firePendingWill",
		run: func(t *testing.T, h *harness, hold *heldCall) {
			watch := connect(t, h, "watcher", true, false)
			watch.Sub(t, "status/#", 1)
			first := dialRaw(t, h, connectWithDelayedWill(t, id, true, 300, "status/"+id, "first-gone", 1))
			// **Armed before the connection goes, for the timer's read
			// alone** (heldCall.on): the read that stamps when the Will is due
			// passes, and the one its timer makes a second later is held,
			// however late this goroutine runs. Armed once a poll had seen
			// the Will waiting, a poll slower than the one-second delay found
			// it already fired.
			hold.armed.Store(true)
			_ = first.Close()
			hold.await(t, hold.entered, "arrived: the Will's timer never read the record")
			ops := operationsAt(t, h)
			second, _ := connectPastHold(t, h, hold,
				connectWithDelayedWill(t, id, false, 300, "status/"+id, "second-gone", 1))
			hold.let()
			hold.await(t, hold.left, "returned")
			// **Published before the marker is.** The timer publishes after
			// the read it was held at returns, so a marker sent at that return
			// could arrive first and read a Will that went as none. A Will
			// published is counted, and one that is not is what the marker
			// then reports.
			const delayed = `saguin_wills_published_total{cause="delayed"}`
			waitUntil(5*time.Second, func() bool { return scrapeGauges(t, ops)[delayed] >= 1 })
			if got := awaitWills(t, h, watch, "while-connected"); !slices.Equal(got, []string{"first-gone"}) {
				t.Fatalf("while the second connection was up the watcher was sent %q, want only the "+
					"Will that fell due, [first-gone]", got)
			}
			_ = second.Close()
			if r, ok := watch.Await(t, 5*time.Second); !ok || r.Payload != "second-gone" {
				t.Fatalf("the second connection went and its Will was not published (got %q, %v)",
					r.Payload, ok)
			}
		},
	}, {
		name: "a delayed Will being held",
		run: func(t *testing.T, h *harness, hold *heldCall) {
			ops := operationsAt(t, h)
			watch := connect(t, h, "watcher", true, false)
			watch.Sub(t, "status/#", 1)
			first := dialRaw(t, h, connectWithDelayedWill(t, id, true, 300, "status/"+id, "first-gone", 1))
			hold.armed.Store(true)
			_ = first.Close()
			hold.await(t, hold.entered, "arrived: the Will was not held")
			// A resume inside the delay cancels the first Will.
			second, _ := connectPastHold(t, h, hold,
				connectWithDelayedWill(t, id, false, 300, "status/"+id, "second-gone", 1))
			hold.let()
			hold.await(t, hold.left, "returned")
			// Past the moment the first Will's timer would fire, whichever
			// way it went: cancelled by the resume, or fired.
			for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				g := scrapeGauges(t, ops)
				if g["saguin_wills_cancelled_total"]+g[`saguin_wills_published_total{cause="delayed"}`] > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the first Will was neither cancelled nor published, so what follows proves nothing")
				}
			}
			if got := awaitWills(t, h, watch, "while-connected"); len(got) != 0 {
				t.Fatalf("while the second connection was up the watcher was sent %q, want nothing: "+
					"the first Will was cancelled by the resume", got)
			}
			_ = second.Close()
			if r, ok := watch.Await(t, 5*time.Second); !ok || r.Payload != "second-gone" {
				t.Fatalf("the second connection went and its Will was not published (got %q, %v)",
					r.Payload, ok)
			}
		},
	}, {
		name:         "a disconnect that keeps its session",
		atDisconnect: true,
		run: func(t *testing.T, h *harness, hold *heldCall) {
			watch := connect(t, h, "watcher", true, false)
			watch.Sub(t, "status/#", 1)
			first := dialRaw(t, h, connectWithDelayedWill(t, id, true, 300, "status/"+id, "first-gone", 1))
			hold.armed.Store(true)
			if _, err := first.Write([]byte{0xE0, 0x00}); err != nil { // DISCONNECT 0x00
				t.Fatalf("write DISCONNECT: %v", err)
			}
			hold.await(t, hold.entered, "arrived: the disconnect recorded nothing")
			second, _ := connectPastHold(t, h, hold,
				connectWithDelayedWill(t, id, false, 300, "status/"+id, "second-gone", 1))
			hold.let()
			hold.await(t, hold.left, "returned")
			sess, ok, err := brokertest.HarnessSessions.Get(id)
			if err != nil || !ok {
				t.Fatalf("the connected session has no record (%v, %v)", ok, err)
			}
			if !sess.DisconnectedAt.IsZero() || sess.Will == nil {
				t.Errorf("the connected session's record says it went at %v and holds Will %v: "+
					"the first connection's disconnect was recorded on it", sess.DisconnectedAt, sess.Will)
			}
			_ = second.Close()
			if r, ok := watch.Await(t, 5*time.Second); !ok || r.Payload != "second-gone" {
				t.Fatalf("the second connection went and its Will was not published (got %q, %v)",
					r.Payload, ok)
			}
		},
	}} {
		t.Run(arm.name, func(t *testing.T) {
			var holdAt atomic.Pointer[heldCall]
			holdAt.Store(newHeldCall(id))
			if arm.atDrop {
				brokertest.WrapHolds = func(_ string, s broker.HoldStore) broker.HoldStore {
					return holdsHeldAtDrop{HoldStore: s, hold: &holdAt}
				}
			} else if arm.atDisconnect {
				brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
					return &sessionsHeldAtDisconnect{SessionStore: s, hold: &holdAt}
				}
			} else {
				brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
					return &sessionsHeldAtGet{SessionStore: s, hold: &holdAt}
				}
			}
			t.Cleanup(func() { brokertest.WrapHolds, brokertest.WrapSessions = nil, nil })
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				hold := newHeldCall(id)
				hold.on = arm.on
				holdAt.Store(hold)
				t.Cleanup(hold.let)
				arm.run(t, h, hold)
			})
		})
	}
}

// claimsTheID connects under the held id, holds an exactly-once publish and
// leaves with its session kept; then, once the ending is done, resumes and
// releases the publish, and says what the session had kept.
func claimsTheID(t *testing.T, h *harness, hold *heldCall, cleanEnding bool) {
	t.Helper()
	conn, _ := connectPastHold(t, h, hold, connectFor(t, hold.id, false, 300, ""))
	w := &qos2Wire{T: t, C: conn}
	w.Publish(7, "events/lockgap/1", "held", false)
	if rc := w.Pubrec(7); rc != 0 {
		t.Fatalf("the exactly-once publish was answered 0x%02X, want success", rc)
	}
	before := h.Disconnects.Snapshot(hold.id)
	w.Close()
	sessionGoneAfter(t, h, hold.id, before)
	hold.let()
	hold.await(t, hold.left, "returned")

	if !cleanEnding {
		if _, ok, err := brokertest.HarnessSessions.Get(hold.id); err != nil || !ok {
			t.Errorf("the session the second connection left has no record (%v, %v): the ending "+
				"of the session before it dropped it", ok, err)
		}
	}
	back, ca := qos2Dial(t, h.Addr, hold.id, false, 300)
	defer back.Close()
	if !ca.SessionPresent {
		t.Fatal("the session the second connection kept was not present when it came back")
	}
	// The resumed session re-sends the receipt it still owes; the release
	// is answered after it.
	back.Pubrel(7)
	for {
		cp := back.Next(3 * time.Second)
		if rec, ok := cp.Content.(*pahopackets.Pubrec); ok && rec.PacketID == 7 {
			continue
		}
		comp, ok := cp.Content.(*pahopackets.Pubcomp)
		if !ok || comp.PacketID != 7 {
			t.Fatalf("the release was answered %s, want PUBCOMP for 7", cp)
		}
		if comp.ReasonCode != 0 {
			t.Fatalf("the release was answered 0x%02X, want success", comp.ReasonCode)
		}
		break
	}
	if n := inChannel(t, h, "reader", "held"); n != 1 {
		t.Errorf("the channel holds %d copies of the publish the second connection was sent "+
			"PUBREC and PUBCOMP for, want 1", n)
	}
}

// RFC 0003 "Last Will": a Will still waiting when its session ends is
// published then - and published once, however the connection that ended it
// fares.
//
// **A retention-floor discard now publishes its Will once the client id's
// session lock is let go**, never under it, because a publish can wait on
// another id's session lock. The discard runs before the CONNACK, so the one
// release that must not skip it is the one after a CONNACK that could not be
// written: the session has still ended, and its Will is still owed.
func TestAWillEndedByAFloorDiscardIsPublishedOnceWhenTheCONNACKFails(t *testing.T) {
	h := startTrimming(t, map[string]int64{"events": 200})
	ops := operationsAt(t, h)
	p := connect(t, h, "producer", true, false)
	watch := connect(t, h, "watcher", true, false)
	watch.Sub(t, "status/#", 1)

	dev := &qos2Wire{T: t, C: dialRaw(t, h,
		connectWithDelayedWill(t, "device", true, 3600, "status/device", "OLD", 60))}
	dev.Subscribe("events/#", 1)
	p.Pub(t, "events/a/1", "first")
	for {
		cp := dev.Next(3 * time.Second)
		if pub, ok := cp.Content.(*pahopackets.Publish); ok {
			dev.Puback(pub.PacketID)
			break
		}
	}
	awaitStoredPosition(t, h, "events", 2, 5*time.Second)

	before := h.Disconnects.Snapshot("device")
	dev.Close()
	sessionGoneAfter(t, h, "device", before)
	if got := scrapeGauges(t, ops)["saguin_wills_waiting"]; got != 1 {
		t.Fatalf("%v Wills waiting after the device went, want 1, so this proves nothing", got)
	}
	for i := range 30 {
		p.Pub(t, "events/a/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
	}
	awaitFloorPast(t, ops, "events", 2)

	connackNeverArrives(t, h, connectWithDelayedWill(t, "device", false, 3600, "status/device", "NEW", 60))
	g := scrapeGauges(t, ops)
	if lost := g[`saguin_channel_position_lost_total{channel="events"}`]; lost != 1 {
		t.Fatalf("position lost %v, want 1: the connection did not find retention past its "+
			"position, so its session was not discarded and this proves nothing", lost)
	}
	if got := awaitWills(t, h, watch, "after-the-discard"); !slices.Equal(got, []string{"OLD"}) {
		t.Fatalf("the watcher was sent %q, want the discarded session's Will once", got)
	}
	if got := scrapeGauges(t, ops)[`saguin_wills_published_total{cause="session_ended"}`]; got != 1 {
		t.Errorf("wills published for an ended session: %v, want 1", got)
	}
}

type holdsHeldAtHold struct {
	broker.HoldStore
	hold *atomic.Pointer[heldCall]
}

func (s holdsHeldAtHold) Hold(e store.Exchange, r store.Record, now time.Time) (err error) {
	s.hold.Load().at(e.Client, func() { err = s.HoldStore.Hold(e, r, now) })
	return err
}

// packetGate holds one packet of a kind at its read, before the engine
// handles it: a packet read before a takeover and handled after it.
type packetGate struct {
	mqtt.HookBase
	kind    byte
	id      uint16
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPacketGate(kind byte, id uint16) *packetGate {
	return &packetGate{kind: kind, id: id, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *packetGate) ID() string           { return fmt.Sprintf("packet-gate-%d", g.kind) }
func (g *packetGate) Provides(b byte) bool { return b == mqtt.OnPacketRead }
func (g *packetGate) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == g.kind && pk.PacketID == g.id && cl.ID == "dev" &&
		g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
	return pk, nil
}

func (g *packetGate) let() { g.once.Do(func() { close(g.release) }) }

// connect311 is an MQTT 3.1.1 CONNECT keeping its session, and publish311 an
// exactly-once 3.1.1 PUBLISH: 3.1.1 carries no properties, which the client
// library this suite uses always writes.
func connect311(id string) []byte {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x00, 0x00, 0x00, 0x00, byte(len(id))}
	body = append(body, id...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

func publish311(id uint16, topic, payload string) []byte {
	body := []byte{0x00, byte(len(topic))}
	body = append(body, topic...)
	body = append(body, byte(id>>8), byte(id))
	body = append(body, payload...)
	return append([]byte{0x34, byte(len(body))}, body...)
}

// neverToldHeld reads what a connection is sent until it closes, and fails if
// any of it is a PUBREC saying a publish was held.
func neverToldHeld(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		cp, err := pahopackets.ReadPacket(conn)
		if err != nil {
			return
		}
		if rec, ok := cp.Content.(*pahopackets.Pubrec); ok && rec.ReasonCode < 0x80 {
			t.Errorf("the old connection was told its exactly-once publish was held: %s", cp)
		}
	}
}

// RFC 0002 `broker.qos2` and invariant 17: an exactly-once publish belongs to
// the session that sent it, and leaves when that session ends.
//
// **The old connection's publish meets a clean start that takes its id**, and
// the new session then publishes under the same packet identifier. What the
// channel receives says whose publish the release completed. Held at its store
// write, the old publish lands inside the id's session lock, before the new
// connection can claim the id, and the clean start's ending drops it. Held at
// its read, it is handled after the new connection holds the id, and is
// refused: with a reason code on an MQTT 5 PUBREC, and by closing a 3.1.1
// connection, whose PUBREC cannot carry one. The refusal is read off the
// broker's counter, so a run where the owner was never asked cannot pass.
func TestAnExactlyOncePublishFromAnEndedSessionTakesNothingFromItsSuccessor(t *testing.T) {
	for _, arm := range []struct {
		name      string
		atRead    bool
		legacy    bool
		refusedBy float64
	}{
		{"held at its store write", false, false, 0},
		{"read before the takeover, from an MQTT 5 client", true, false, 1},
		{"read before the takeover, from an MQTT 3.1.1 client", true, true, 1},
	} {
		t.Run(arm.name, func(t *testing.T) {
			var holdAt atomic.Pointer[heldCall]
			holdAt.Store(newHeldCall("dev"))
			brokertest.WrapHolds = func(_ string, s broker.HoldStore) broker.HoldStore {
				return holdsHeldAtHold{HoldStore: s, hold: &holdAt}
			}
			t.Cleanup(func() { brokertest.WrapHolds = nil })
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				hold := newHeldCall("dev")
				holdAt.Store(hold)
				t.Cleanup(hold.let)
				ops := operationsAt(t, h)
				const refused = `saguin_publish_refused_total{reason="unspecified error"}`
				before := scrapeGauges(t, ops)[refused]
				gate := newPacketGate(packets.Publish, 5)
				if err := h.Srv.AddHook(gate, nil); err != nil {
					t.Fatalf("add the PUBLISH gate: %v", err)
				}
				t.Cleanup(gate.let)

				var old net.Conn
				if arm.legacy {
					old = dialLegacy(t, h, connect311("dev"))
				} else {
					old = dialRaw(t, h, connectFor(t, "dev", false, 300, ""))
				}
				var publish []byte
				if arm.legacy {
					publish = publish311(5, "events/lockgap/old", "from-the-ended-session")
				} else {
					var buf bytes.Buffer
					cp := pahopackets.NewControlPacket(pahopackets.PUBLISH)
					pp := cp.Content.(*pahopackets.Publish)
					pp.Topic, pp.Payload, pp.QoS, pp.PacketID = "events/lockgap/old",
						[]byte("from-the-ended-session"), 2, 5
					if _, err := cp.WriteTo(&buf); err != nil {
						t.Fatalf("encode PUBLISH: %v", err)
					}
					publish = buf.Bytes()
				}
				if arm.atRead {
					gate.armed.Store(true)
				} else {
					hold.armed.Store(true)
				}
				if _, err := old.Write(publish); err != nil {
					t.Fatalf("write PUBLISH: %v", err)
				}

				var conn net.Conn
				if arm.atRead {
					select {
					case <-gate.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("the old publish was never read, so this proves nothing")
					}
					conn, _ = connectPastHold(t, h, nil, connectFor(t, "dev", true, 300, ""))
					if !waitUntil(3*time.Second, func() bool { return h.Srv.SessionLockWaiters("dev") == 0 }) {
						t.Fatal("the clean start never let go of the id's session lock")
					}
					gate.let()
					if !waitUntil(5*time.Second, func() bool {
						return scrapeGauges(t, ops)[refused]-before >= arm.refusedBy
					}) {
						t.Fatalf("the old publish was never refused: %s moved by %v", refused,
							scrapeGauges(t, ops)[refused]-before)
					}
				} else {
					hold.await(t, hold.entered, "arrived: the old publish was never held")
					conn, _ = connectPastHold(t, h, hold, connectFor(t, "dev", true, 300, ""))
					// Let go once the clean start has ended the old session,
					// which it does after its CONNACK and before it lets go
					// of the id's lock.
					if !waitUntil(3*time.Second, func() bool { return h.Srv.SessionLockWaiters("dev") == 0 }) {
						t.Fatal("the clean start never let go of the id's session lock")
					}
					hold.let()
					hold.await(t, hold.left, "returned")
				}
				// Held at its store write, the old publish was held while its
				// connection still owned the id, and told so rightly; read
				// before the takeover, it is handled for an id another
				// connection holds, and must not be.
				if arm.atRead {
					neverToldHeld(t, old)
				}

				fresh := &qos2Wire{T: t, C: conn}
				fresh.Publish(5, "events/lockgap/new", "from-the-new-session", false)
				if rc := fresh.Pubrec(5); rc != 0 {
					t.Fatalf("the new session's publish was answered 0x%02X, want success", rc)
				}
				fresh.Pubrel(5)
				if rc := fresh.Pubcomp(5); rc != 0 {
					t.Fatalf("the new session's release was answered 0x%02X, want success", rc)
				}
				if n := inChannel(t, h, "reader-new", "from-the-new-session"); n != 1 {
					t.Errorf("the channel holds %d copies of the publish the new session was sent PUBREC "+
						"and PUBCOMP for, want 1", n)
				}
				if n := inChannel(t, h, "reader-old", "from-the-ended-session"); n != 0 {
					t.Errorf("the channel holds %d copies of the publish whose session ended unfinished, want 0", n)
				}
				if got := scrapeGauges(t, ops)[refused] - before; got != arm.refusedBy {
					t.Errorf("%s moved by %v, want %v", refused, got, arm.refusedBy)
				}
			})
		})
	}
}

// dialLegacy writes a 3.1.1 CONNECT and returns the connection once it is
// accepted.
func dialLegacy(t *testing.T, h *harness, connect []byte) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", h.Addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(connect); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	ack := make([]byte, 4)
	if _, err := io.ReadFull(conn, ack); err != nil || ack[0] != 0x20 || ack[3] != 0 {
		t.Fatalf("the 3.1.1 CONNECT was not accepted: %v % x", err, ack)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn
}

// onStack reports whether the function of that name is on the caller's stack,
// so a hold on a store method many paths call holds only the one it is for.
func onStack(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, "."+name) {
			return true
		}
		if !more {
			return false
		}
	}
}

type sessionsHeldAtKeep struct {
	broker.SessionStore
	hold *atomic.Pointer[heldCall]
}

// Snapshot passes the memory store's export through, which a graceful stop
// writes the sessions from: without it a wrapped memory store is saved as
// nothing, and a restart finds no session for a reason that is the wrapper's.
func (s *sessionsHeldAtKeep) Snapshot() *store.SessionsSnapshot {
	if ex, ok := s.SessionStore.(interface {
		Snapshot() *store.SessionsSnapshot
	}); ok {
		return ex.Snapshot()
	}
	return nil
}

func (s *sessionsHeldAtKeep) Save(sess store.Session) (err error) {
	if !onStack("keepSubscriptions") {
		return s.SessionStore.Save(sess)
	}
	s.hold.Load().at(sess.Client, func() { err = s.SessionStore.Save(sess) })
	return err
}

// subscribeFor is an MQTT 5 SUBSCRIBE for one filter at QoS 1.
func subscribeFor(t *testing.T, id uint16, filter string) []byte {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
	s := cp.Content.(*pahopackets.Subscribe)
	s.PacketID = id
	s.Properties = &pahopackets.Properties{}
	s.Subscriptions = []pahopackets.SubOptions{{Topic: filter, QoS: 1}}
	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		t.Fatalf("encode SUBSCRIBE: %v", err)
	}
	return buf.Bytes()
}

// invariant 17: only the connection that owns a client id changes what the
// broker keeps for it - here, the subscriptions its session record keeps and
// the topic index keeps under its id.
//
// **The old connection's SUBSCRIBE meets a clean start that takes the id**,
// and the new session subscribes. Held at its write to the session record,
// the old SUBSCRIBE is inside the id's session lock and lands before the new
// connection can claim the id. Held at its read, it is handled after the new
// session has subscribed, on a connection taken over for good, and changes
// nothing. The record is what a restart restores the session from, so what it
// says after both is read, then what the new session is sent, then the
// restored session is sent a record on its own subscription.
func TestASubscribeFromASupersededConnectionLeavesItsSuccessorsRecordAlone(t *testing.T) {
	for _, arm := range []struct {
		name   string
		atRead bool
	}{
		{"held at its record write", false},
		{"read before the takeover", true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			var holdAt atomic.Pointer[heldCall]
			holdAt.Store(newHeldCall("dev"))
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				return &sessionsHeldAtKeep{SessionStore: s, hold: &holdAt}
			}
			t.Cleanup(func() { brokertest.WrapSessions = nil })
			eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
				hold := newHeldCall("dev")
				holdAt.Store(hold)
				t.Cleanup(hold.let)
				gate := newPacketGate(packets.Subscribe, 1)
				if err := h.Srv.AddHook(gate, nil); err != nil {
					t.Fatalf("add the SUBSCRIBE gate: %v", err)
				}
				t.Cleanup(gate.let)

				before := h.Disconnects.Snapshot("dev")
				old := dialRaw(t, h, connectFor(t, "dev", false, 300, ""))
				if arm.atRead {
					gate.armed.Store(true)
				} else {
					hold.armed.Store(true)
				}
				if _, err := old.Write(subscribeFor(t, 1, "events/old/#")); err != nil {
					t.Fatalf("write SUBSCRIBE: %v", err)
				}
				var fresh *qos2Wire
				if arm.atRead {
					select {
					case <-gate.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("the old SUBSCRIBE was never read, so this proves nothing")
					}
					conn, _ := connectPastHold(t, h, nil, connectFor(t, "dev", true, 300, ""))
					fresh = &qos2Wire{T: t, C: conn}
					fresh.Subscribe("events/new/#", 1)
					gate.let()
					sessionGoneAfter(t, h, "dev", before) // the old connection is done with it
				} else {
					hold.await(t, hold.entered, "arrived: the old SUBSCRIBE never wrote the session record")
					conn, _ := connectPastHold(t, h, hold, connectFor(t, "dev", true, 300, ""))
					fresh = &qos2Wire{T: t, C: conn}
					fresh.Subscribe("events/new/#", 1)
					hold.let()
					hold.await(t, hold.left, "returned")
				}

				sess, ok, err := brokertest.HarnessSessions.Get("dev")
				if err != nil || !ok {
					t.Fatalf("the new session has no record (%v, %v)", ok, err)
				}
				var filters []string
				for _, sub := range sess.Subscriptions {
					filters = append(filters, sub.Filter)
				}
				if !slices.Equal(filters, []string{"events/new/#"}) {
					t.Errorf("the new session's record keeps subscriptions %q, want only the one it made", filters)
				}
				// And it is served only what it subscribed to: a filter the
				// old connection asked for went into the topic index under
				// the id.
				p0 := connect(t, h, "producer-0", true, false)
				p0.Pub(t, "events/old/1", "not-asked-for")
				p0.Pub(t, "events/new/1", "marker")
				for {
					cp, err := fresh.Read(3 * time.Second)
					if err != nil {
						t.Fatalf("the new session was not sent the marker on its own subscription: %v", err)
					}
					pub, ok := cp.Content.(*pahopackets.Publish)
					if !ok {
						continue
					}
					fresh.Puback(pub.PacketID)
					if string(pub.Payload) == "not-asked-for" {
						t.Error("the new session was sent a record on a filter only the old connection subscribed to")
					}
					if string(pub.Payload) == "marker" {
						break
					}
				}

				fresh.Settle()
				fresh.Close()
				h.Stop()
				h2 := restart()
				back, ca := qos2Dial(t, h2.Addr, "dev", false, 300)
				defer back.Close()
				if !ca.SessionPresent {
					t.Fatal("the new session was not restored")
				}
				p := connect(t, h2, "producer", true, false)
				p.Pub(t, "events/new/2", "owed")
				for {
					cp, err := back.Read(3 * time.Second)
					if err != nil {
						t.Fatalf("the restored session was sent nothing on the subscription it made: %v", err)
					}
					if pub, ok := cp.Content.(*pahopackets.Publish); ok {
						if string(pub.Payload) != "owed" {
							t.Fatalf("the restored session was sent %q, want the record on its subscription",
								pub.Payload)
						}
						break
					}
				}
			})
		})
	}
}

// subackGate holds one SUBACK at its encode, before it is written, which is
// before the hook that runs once it has been.
type subackGate struct {
	mqtt.HookBase
	id      uint16
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *subackGate) ID() string           { return "suback-gate" }
func (g *subackGate) Provides(b byte) bool { return b == mqtt.OnPacketEncode }
func (g *subackGate) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Suback && pk.PacketID == g.id {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	return pk
}

// RFC 0003 "Replay": a SUBSCRIBE is served what it earned once its SUBACK is
// written - invariant 17, for what is waiting on that SUBACK.
//
// **What a subscription earned waited under the client id**, so a SUBSCRIBE
// handled on a connection another had since taken the id from replaced what
// the new session's own SUBSCRIBE was waiting to be sent: the new session's
// SUBACK then ran the old connection's catch-up, to a socket already closed,
// and the new session was granted its subscription and sent none of the
// backlog.
//
// **Held at the two SUBACKs, so the order is decided rather than raced.** The
// id's session lock means the old SUBSCRIBE finishes its hooks before the new
// connection can take the id over, so what can still go wrong is the old
// connection's SUBACK being written - and answered in OnPacketSent - after the
// new session's SUBSCRIBE has registered what it earned. So the old SUBACK is
// held at its encode, past the lock; the new connection takes the id over and
// subscribes, its SUBACK held too; the old one is let go and the old
// connection is waited out; then the new one. Keyed by client id, the old
// SUBACK takes the new session's catch-up every time.
func TestASubscribeFromASupersededConnectionTakesNothingItsSuccessorIsOwed(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		p := connect(t, h, "producer", true, false)
		for i := 1; i <= 3; i++ {
			p.Pub(t, "events/x", fmt.Sprintf("r%d", i))
		}
		oldGate := &subackGate{id: 1, entered: make(chan struct{}), release: make(chan struct{})}
		gate := &subackGate{id: 77, entered: make(chan struct{}), release: make(chan struct{})}
		for _, g := range []*subackGate{oldGate, gate} {
			if err := h.Srv.AddHook(g, nil); err != nil {
				t.Fatalf("add a SUBACK gate: %v", err)
			}
		}
		var letOld, letNew sync.Once
		t.Cleanup(func() {
			letOld.Do(func() { close(oldGate.release) })
			letNew.Do(func() { close(gate.release) })
		})

		before := h.Disconnects.Snapshot("dev")
		old := dialRaw(t, h, connectFor(t, "dev", false, 300, ""))
		if _, err := old.Write(subscribeFor(t, 1, "events/#")); err != nil {
			t.Fatalf("write SUBSCRIBE: %v", err)
		}
		select {
		case <-oldGate.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the old SUBSCRIBE's SUBACK was never encoded, so this proves nothing")
		}

		conn, _ := connectPastHold(t, h, nil, connectFor(t, "dev", true, 300, ""))
		fresh := &qos2Wire{T: t, C: conn}
		if _, err := conn.Write(subscribeFor(t, 77, "events/#")); err != nil {
			t.Fatalf("write SUBSCRIBE: %v", err)
		}
		select {
		case <-gate.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the new session's SUBACK was never encoded, so this proves nothing")
		}
		letOld.Do(func() { close(oldGate.release) })
		sessionGoneAfter(t, h, "dev", before) // the old SUBACK, then the takeover's DISCONNECT
		letNew.Do(func() { close(gate.release) })

		var got []string
		for len(got) < 3 {
			cp, err := fresh.Read(3 * time.Second)
			if err != nil {
				t.Fatalf("the new session was granted its subscription and sent %v of the backlog: %v", got, err)
			}
			if pub, ok := cp.Content.(*pahopackets.Publish); ok {
				got = append(got, string(pub.Payload))
				fresh.Puback(pub.PacketID)
			}
		}
		if want := []string{"r1", "r2", "r3"}; !slices.Equal(got, want) {
			t.Errorf("the new session was sent %v, want %v", got, want)
		}
	})
}

// RFC 0003 "Sessions": a session retention passed ends, and its client is told
// Session Present 0 - so the record kept for the session that follows holds
// none of the ended one's subscriptions.
//
// **The engine's unsubscribe of the ended session's connection is what empties
// them**, writing that connection's now-empty subscriptions over the record the
// resume carried them into. It is a write from a connection that no longer
// owns the id, made on purpose under the id's session lock, and a check that
// refused every such write kept the ended session's subscriptions on a record
// whose client had been told it has none.
func TestASessionRetentionPassedKeepsNoSubscriptionsInTheRecordAfterIt(t *testing.T) {
	h := startTrimming(t, map[string]int64{"events": 200})
	ops := operationsAt(t, h)
	p := connect(t, h, "producer", true, false)
	dev := &qos2Wire{T: t, C: dialRaw(t, h, connectFor(t, "device", true, 3600, ""))}
	dev.Subscribe("events/#", 1)
	p.Pub(t, "events/a/1", "first")
	for {
		cp := dev.Next(3 * time.Second)
		if pub, ok := cp.Content.(*pahopackets.Publish); ok {
			dev.Puback(pub.PacketID)
			break
		}
	}
	awaitStoredPosition(t, h, "events", 2, 5*time.Second)
	before := h.Disconnects.Snapshot("device")
	dev.Close()
	sessionGoneAfter(t, h, "device", before)
	for i := range 30 {
		p.Pub(t, "events/a/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
	}
	awaitFloorPast(t, ops, "events", 2)

	back, ca := qos2Dial(t, h.Addr, "device", false, 3600)
	defer back.Close()
	if ca.SessionPresent {
		t.Fatal("the client was told its session is present, so retention passed nothing and this proves nothing")
	}
	const lost = `saguin_channel_position_lost_total{channel="events"}`
	if got := scrapeGauges(t, ops)[lost]; got != 1 {
		t.Fatalf("%s is %v, want 1: the session was not discarded, so this proves nothing", lost, got)
	}
	sess, ok, err := brokertest.HarnessSessions.Get("device")
	if err != nil || !ok {
		t.Fatalf("the session that followed has no record (%v, %v)", ok, err)
	}
	if len(sess.Subscriptions) != 0 {
		var filters []string
		for _, s := range sess.Subscriptions {
			filters = append(filters, s.Filter)
		}
		t.Errorf("the record keeps subscriptions %q for a client told Session Present 0", filters)
	}
}

// unsubscribeFor is an MQTT 5 UNSUBSCRIBE for one filter.
func unsubscribeFor(t *testing.T, id uint16, filter string) []byte {
	t.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.UNSUBSCRIBE)
	u := cp.Content.(*pahopackets.Unsubscribe)
	u.PacketID = id
	u.Properties = &pahopackets.Properties{}
	u.Topics = []string{filter}
	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		t.Fatalf("encode UNSUBSCRIBE: %v", err)
	}
	return buf.Bytes()
}

// invariant 17, and RFC 0003 "Sessions": a session retention passed ends when
// its client comes back, and the connection it was still open on goes with it.
//
// **That connection is taken over, and a packet it read before the resume
// changes nothing after it.** The discard ended it without marking it, so a
// SUBSCRIBE or UNSUBSCRIBE it had read and not yet handled passed the
// engine's check for a connection taken over. A SUBSCRIBE wrote its filter
// into the new session's record and the topic index under the id, and the
// new session was served a filter it never asked for. An UNSUBSCRIBE took the
// new session's own subscription out of the index, and its record, so it was
// told it was subscribed and sent nothing.
func TestAConnectionARetentionDiscardEndedChangesNothingAfterIt(t *testing.T) {
	brokertest.Retain = map[string]int64{"events": 200}
	t.Cleanup(func() { brokertest.Retain = nil })
	for _, arm := range []struct {
		name string
		kind byte
	}{
		{"a SUBSCRIBE", packets.Subscribe},
		{"an UNSUBSCRIBE", packets.Unsubscribe},
	} {
		t.Run(arm.name, func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				ops := operationsAt(t, h)
				p := connect(t, h, "producer", true, false)

				// Durable, connected, and holding a stored position the floor
				// will pass. Its filter is narrow, so the padding is not
				// delivered to it.
				before := h.Disconnects.Snapshot("dev")
				dev := &qos2Wire{T: t, C: dialRaw(t, h, connectFor(t, "dev", true, 3600, ""))}
				dev.Subscribe("events/seed/#", 1)
				p.Pub(t, "events/seed/1", "seed")
				for {
					cp := dev.Next(3 * time.Second)
					if pub, ok := cp.Content.(*pahopackets.Publish); ok {
						dev.Puback(pub.PacketID)
						break
					}
				}
				awaitStoredPosition(t, h, "events", 2, 5*time.Second)
				for i := range 30 {
					p.Pub(t, "events/pad/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
				}
				awaitFloorPast(t, ops, "events", 2)

				// The old connection's packet, read and held before the resume.
				gate := newPacketGate(arm.kind, 9)
				if err := h.Srv.AddHook(gate, nil); err != nil {
					t.Fatalf("add the gate: %v", err)
				}
				t.Cleanup(gate.let)
				gate.armed.Store(true)
				packet := subscribeFor(t, 9, "events/stale/#")
				if arm.kind == packets.Unsubscribe {
					packet = unsubscribeFor(t, 9, "events/seed/#")
				}
				if _, err := dev.C.Write(packet); err != nil {
					t.Fatalf("write the packet: %v", err)
				}
				select {
				case <-gate.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("the old connection's packet was never read, so this proves nothing")
				}

				// The device comes back resuming, and retention has passed its
				// position: the discard runs against the old connection.
				back, ca := qos2Dial(t, h.Addr, "dev", false, 3600)
				defer back.Close()
				if ca.SessionPresent {
					t.Fatal("the session was present, so nothing was discarded and this proves nothing")
				}
				const lost = `saguin_channel_position_lost_total{channel="events"}`
				if got := scrapeGauges(t, ops)[lost]; got != 1 {
					t.Fatalf("%s is %v, want 1: the discard never ran, so this proves nothing", lost, got)
				}
				// The new session's own subscription, which a stale
				// UNSUBSCRIBE would take and a stale SUBSCRIBE sits beside.
				back.Subscribe("events/seed/#", 1)
				gate.let()
				sessionGoneAfter(t, h, "dev", before) // the old connection is done with its packet

				sess, ok, err := brokertest.HarnessSessions.Get("dev")
				if err != nil || !ok {
					t.Fatalf("the new session has no record (%v, %v)", ok, err)
				}
				var filters []string
				for _, sub := range sess.Subscriptions {
					filters = append(filters, sub.Filter)
				}
				if !slices.Equal(filters, []string{"events/seed/#"}) {
					t.Errorf("the new session's record keeps subscriptions %q, want only the one it made", filters)
				}
				p.Pub(t, "events/stale/1", "not-asked-for")
				p.Pub(t, "events/seed/2", "owed")
				for {
					cp, err := back.Read(3 * time.Second)
					if err != nil {
						t.Fatalf("the new session was not sent the record on its own subscription: %v", err)
					}
					pub, ok := cp.Content.(*pahopackets.Publish)
					if !ok {
						continue
					}
					back.Puback(pub.PacketID)
					if string(pub.Payload) == "not-asked-for" {
						t.Error("the new session was sent a record on a filter only the old connection subscribed to")
					}
					if string(pub.Payload) == "owed" {
						break
					}
				}
			})
		})
	}
}

// RFC 0003 "Exactly once, and where the unfinished ones wait": an expiry and
// a release of the same exchange never interleave, so a PUBREL racing the
// expiry is answered 0x92, never completed.
//
// The expiry is held inside its store call, after it has decided the
// exchange is due. The PUBREL is sent then, and waits - the broker says so,
// by the id's session lock having a waiter. Let go, the expiry finishes and
// the PUBREL is answered: the message is in no channel, and the answer says
// the exchange is unknown rather than complete.
func TestAPubrelRacingItsExpiryIsAnsweredNotFound(t *testing.T) {
	const id = "racer"
	brokertest.QoS2ExpiresAfter = time.Second
	var holdAt atomic.Pointer[heldCall]
	brokertest.WrapHolds = func(_ string, s broker.HoldStore) broker.HoldStore {
		return holdsHeldAtDrop{HoldStore: s, hold: &holdAt}
	}
	t.Cleanup(func() { brokertest.QoS2ExpiresAfter, brokertest.WrapHolds = 0, nil })
	eachProvider(t, func(t *testing.T, h *harness) {
		hold := newHeldCall(id)
		holdAt.Store(hold)
		t.Cleanup(hold.let)
		w, _ := qos2Dial(t, h.Addr, id, false, 300)
		defer w.Close()
		w.Publish(1, "events/race/1", "raced", false)
		if rc := w.Pubrec(1); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}
		hold.armed.Store(true)
		hold.await(t, hold.entered, "arrived: the sweep never dropped the expired exchange")

		w.Pubrel(1)
		for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < 2; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the PUBREL never waited for the id's session lock while the expiry held it, so " +
					"the two were not raced")
			}
		}
		hold.let()
		hold.await(t, hold.left, "returned")

		if rc := w.Pubcomp(1); rc != 0x92 {
			t.Errorf("the PUBREL that raced its expiry was answered 0x%02X, want 0x92: the message was "+
				"dropped, so nothing completed it", rc)
		}
		if n := inChannel(t, h, "auditor", "raced"); n != 0 {
			t.Errorf("the expired message is in the channel %d time(s), want 0", n)
		}
	})
}

// sessionsHeldAtConnect holds the write a CONNECT makes to its session
// record, which it makes inside the id's session lock once it has claimed the
// id.
type sessionsHeldAtConnect struct {
	broker.SessionStore
	hold *atomic.Pointer[heldCall]
}

func (s *sessionsHeldAtConnect) Save(sess store.Session) (err error) {
	if !onStack("keepSession") {
		return s.SessionStore.Save(sess)
	}
	s.hold.Load().at(sess.Client, func() { err = s.SessionStore.Save(sess) })
	return err
}

// invariant 17, and RFC 0003 "Exactly once, and where the unfinished ones
// wait": a PUBREL read on a connection that no longer owns its client id
// releases nothing and is answered no PUBCOMP. The connection that took the
// id over releases the message, once.
//
// The takeover is held inside the id's session lock, once it has claimed the
// id; the old connection's PUBREL is sent then, and waits for the lock. Let
// go, the takeover finishes and the PUBREL is handled for a connection that
// owns nothing. **The old connection is closed by then**, so a PUBCOMP it
// might have been sent cannot be read on the wire; the broker's own line
// saying it refused the PUBREL is what shows the refusal ran, rather than
// the release being lost with the connection.
func TestAPubrelOnATakenOverConnectionReleasesNothing(t *testing.T) {
	const id = "taken"
	var holdAt atomic.Pointer[heldCall]
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return &sessionsHeldAtConnect{SessionStore: s, hold: &holdAt}
	}
	var refused atomic.Int32
	brokertest.LogTo = func(l string) {
		if strings.Contains(l, "refused a PUBREL: another connection holds its client id") {
			refused.Add(1)
		}
	}
	t.Cleanup(func() { brokertest.WrapSessions, brokertest.LogTo = nil, nil })
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		hold := newHeldCall(id)
		holdAt.Store(hold)
		t.Cleanup(hold.let)
		refused.Store(0)
		old, _ := qos2Dial(t, h.Addr, id, false, 300)
		defer old.Close()
		old.Publish(1, "events/taken/1", "taken-once", false)
		if rc := old.Pubrec(1); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}

		hold.armed.Store(true)
		conn, err := net.DialTimeout("tcp", h.Addr, 3*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write(connectFor(t, id, false, 300, "")); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		hold.await(t, hold.entered, "arrived: the takeover never wrote its session record")

		old.Pubrel(1)
		for deadline := time.Now().Add(5 * time.Second); h.Srv.SessionLockWaiters(id) < 2; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the old connection's PUBREL never waited for the id's session lock the takeover " +
					"holds, so the two were not raced")
			}
		}
		hold.let()
		hold.await(t, hold.left, "returned")

		fresh := &qos2Wire{T: t, C: conn}
		cp := fresh.Next(5 * time.Second)
		if ca, ok := cp.Content.(*pahopackets.Connack); !ok || ca.ReasonCode != 0 || !ca.SessionPresent {
			t.Fatalf("the takeover was answered %s, want CONNACK 0x00 with its session present", cp)
		}
		for {
			cp, err := old.Read(2 * time.Second)
			if err != nil {
				break
			}
			if _, ok := cp.Content.(*pahopackets.Pubcomp); ok {
				t.Fatalf("the taken-over connection was sent %s for its PUBREL", cp)
			}
		}
		for deadline := time.Now().Add(5 * time.Second); refused.Load() == 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the broker never said it refused the old connection's PUBREL: it was released, or " +
					"lost with its connection, and neither is the refusal under test")
			}
		}

		fresh.Pubrel(1)
		for {
			cp := fresh.Next(3 * time.Second)
			if rec, ok := cp.Content.(*pahopackets.Pubrec); ok && rec.PacketID == 1 {
				continue // the receipt the resumed session still owes
			}
			comp, ok := cp.Content.(*pahopackets.Pubcomp)
			if !ok || comp.PacketID != 1 || comp.ReasonCode != 0 {
				t.Fatalf("the new connection's PUBREL was answered %s, want PUBCOMP 0x00 for 1", cp)
			}
			break
		}
		fresh.Close()
		if n := inChannel(t, h, "auditor", "taken-once"); n != 1 {
			t.Errorf("the message is in the channel %d time(s), want 1", n)
		}
	})
}

// holdsBegin holds, once armed, the session store's Begin for one client id
// until it is let go - the write keepSession makes before the CONNACK, so a
// connection held there has begun and is not yet registered - and refuses a
// Will for another id, as a full store does.
type holdsBegin struct {
	broker.SessionStore
	held, refused    string
	armed            atomic.Bool
	entered, release chan struct{}
}

func (s *holdsBegin) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if client == s.held && s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	if client == s.refused && next != nil && next.Will != nil {
		return store.Dropped{}, store.ErrFull
	}
	return s.SessionStore.Begin(client, held, next)
}

// sessionGone, the harness's wait for the broker to have finished with a
// client id, waits for a connection under the id that has begun and is not
// yet registered, and not for one whose CONNECT was refused.
//
// The substrate writes the CONNACK before it registers the connection, and
// sessionGone took an id absent from the table as finished with: a client
// that closed on reading its CONNACK was waited for before it was
// registered, and the wait returned before its disconnect had run
// (TestASessionRecordFollowsItsConnection, 7 of 216 under load). A CONNECT
// refused is never registered, so its absence has to go on counting.
func TestSessionGoneWaitsForAConnectionNotYetRegistered(t *testing.T) {
	hb := &holdsBegin{held: "late", refused: "refused"}
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		hb.SessionStore = s
		return hb
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		hb.entered, hb.release = make(chan struct{}), make(chan struct{})
		// Let go however the test ends, or the held CONNECT holds the stop.
		let := sync.OnceFunc(func() { close(hb.release) })
		t.Cleanup(let)
		hb.armed.Store(true)
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write(connectFor(t, "late", true, 300, "")); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		select {
		case <-hb.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the CONNECT never reached the session store, so this proves nothing")
		}
		if _, registered := h.Srv.Clients.Get("late"); registered {
			t.Fatal("the connection is registered while its CONNECT is held, so this proves nothing")
		}
		gone := make(chan struct{})
		go func() {
			sessionGone(t, h, "late")
			close(gone)
		}()
		select {
		case <-gone:
			t.Fatal("sessionGone returned for a connection that had begun and was not yet registered")
		case <-time.After(300 * time.Millisecond):
		}
		let()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		ack, err := pahopackets.ReadPacket(conn)
		if ca, ok := ack.Content.(*pahopackets.Connack); err != nil || !ok || ca.ReasonCode != 0 {
			t.Fatalf("the CONNECT was answered %v (%v), want an accepting CONNACK", ack, err)
		}
		_ = conn.Close()
		select {
		case <-gone:
		case <-time.After(10 * time.Second):
			t.Fatal("sessionGone did not return after the connection ended")
		}
		if n := h.Disconnects.Count("late"); n != 1 {
			t.Errorf("sessionGone returned with %d disconnects run for the id, want 1", n)
		}
		if sess, ok, _ := brokertest.HarnessSessions.Get("late"); !ok || sess.DisconnectedAt.IsZero() {
			t.Errorf("sessionGone returned before the disconnect was recorded: %+v, %v", sess, ok)
		}

		// Refused, after the hook saw it begin: never registered, and
		// finished with as it is refused.
		refused, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = refused.Close() })
		if _, err := refused.Write(connectFor(t, "refused", true, 300, "wills/refused")); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		_ = refused.SetReadDeadline(time.Now().Add(3 * time.Second))
		ack, err = pahopackets.ReadPacket(refused)
		if ca, ok := ack.Content.(*pahopackets.Connack); err != nil || !ok || ca.ReasonCode != 0x97 {
			t.Fatalf("the CONNECT was answered %v (%v), want CONNACK 0x97, so this proves nothing", ack, err)
		}
		start := time.Now()
		sessionGone(t, h, "refused")
		if took := time.Since(start); took > time.Second {
			t.Errorf("sessionGone took %v for a refused CONNECT, want it done within a second", took)
		}
		if n := h.Disconnects.Count("refused"); n != 0 {
			t.Errorf("%d disconnects ran for a refused CONNECT, so this was not one", n)
		}
	})
}

// **The Session Expiry Interval a DISCONNECT carries is the one a start
// keeps** (MQTT 5 3.14.2.2.2). The engine ended a
// running session on the DISCONNECT's value, and the store kept the
// CONNECT's, so a client that lengthened its session on the way out lost it
// - and every message it was owed - to the next start, and one that
// shortened it was kept past what it asked. Both directions cross a restart
// on both stores: raised from 2 s to 3600, the session is resumed with its
// owed message; lowered from 3600 to 2, the start ends it. What the store
// holds is checked before the stop, so the instrument is the record itself.
func TestTheExpiryADisconnectCarriesIsTheOneAStartKeeps(t *testing.T) {
	for _, tc := range []struct {
		name                string
		connect, disconnect uint32
		kept                bool
	}{
		{"raised at the DISCONNECT", 2, 3600, true},
		{"lowered at the DISCONNECT", 3600, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
				dev := dial(t, h, "device", true, false, 0, tc.connect, 0)
				dev.Sub(t, "owed/#", 1)
				v := tc.disconnect
				if err := dev.C.Disconnect(&paho.Disconnect{ReasonCode: 0,
					Properties: &paho.DisconnectProperties{SessionExpiryInterval: &v}}); err != nil {
					t.Fatalf("disconnect: %v", err)
				}
				for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
					s, ok, _ := brokertest.HarnessSessions.Get("device")
					if ok && !s.DisconnectedAt.IsZero() && s.ExpiryInterval == tc.disconnect {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("the store holds %+v (%v) for the device, want it disconnected with "+
							"the DISCONNECT's expiry %d", s.ExpiryInterval, ok, tc.disconnect)
					}
				}
				p := connect(t, h, "publisher", true, false)
				p.Pub(t, "owed/x", "owed")
				awaitOwed(t, operationsAt(t, h), "the message owed to the device", 1)
				h.Stop()

				time.Sleep(3 * time.Second) // past 2 s, well inside 3600
				h2 := restart()
				back := dial(t, h2, "device", false, false, 0, tc.connect, 0)
				if back.SessionPresent != tc.kept {
					t.Fatalf("after the restart the device was told Session Present %v, want %v: the "+
						"start ended the session by an expiry the client had replaced",
						back.SessionPresent, tc.kept)
				}
				r, got := back.Await(t, 2*time.Second)
				if tc.kept && (!got || r.Payload != "owed") {
					t.Errorf("the resumed session was not sent what it was owed (%q, %v)", r.Payload, got)
				}
				if !tc.kept && got {
					t.Errorf("a session its client shortened to 2 s was kept across a 3 s stop, and sent %q",
						r.Payload)
				}
			})
		})
	}
}

// **A DISCONNECT is held to the cap a CONNECT is held to**
// (limits.max_session_expiry). The engine took the
// DISCONNECT's value as sent, so any MQTT 5 client kept its session past the
// cap with one property, while saguin_max_session_expiry_seconds said the
// cap held. The kept record must carry the cap, as EMQX clamps it.
func TestADisconnectIsHeldToTheSessionExpiryCap(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *harness) {
		dev := dial(t, h, "device", true, false, 0, 60, 0)
		dev.Sub(t, "owed/#", 1)
		v := uint32(4_000_000_000)
		if err := dev.C.Disconnect(&paho.Disconnect{ReasonCode: 0,
			Properties: &paho.DisconnectProperties{SessionExpiryInterval: &v}}); err != nil {
			t.Fatalf("disconnect: %v", err)
		}
		cap := uint32(brokertest.Limits.MaxSessionExpiry)
		if cap == 0 || cap >= v {
			t.Fatalf("the harness cap is %d, which cannot tell a capped value from an uncapped one", cap)
		}
		for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			s, ok, _ := brokertest.HarnessSessions.Get("device")
			if ok && !s.DisconnectedAt.IsZero() {
				if s.ExpiryInterval != cap {
					t.Fatalf("the DISCONNECT's %d was kept as %d, want the cap %d", v, s.ExpiryInterval, cap)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the disconnect was never recorded, so nothing above was checked")
			}
		}
		cl, ok := h.Srv.Clients.Get("device")
		if !ok || cl.Properties.Props.SessionExpiryInterval != cap {
			t.Errorf("the engine holds the session's expiry at %d, want the cap %d", func() uint32 {
				if ok {
					return cl.Properties.Props.SessionExpiryInterval
				}
				return 0
			}(), cap)
		}
	})
}

// slowDisconnects records a disconnect only after a pause, so a wait that
// returns before the broker has finished with a connection returns before
// the record says so.
type slowDisconnects struct {
	broker.SessionStore
	pause time.Duration
}

func (s slowDisconnects) Disconnected(id string, at time.Time, dropWill bool, expiry uint32) error {
	time.Sleep(s.pause)
	return s.SessionStore.Disconnected(id, at, dropWill, expiry)
}

// **SessionGone waits for the connection it was called after, whatever the
// id has had before**. It waited for a count of
// disconnects above zero, so on an id closed once already - or taken over -
// it returned at once while the connection it was called for was still
// being torn down: 47 returns at 8 sites in an instrumented run.
// Here the second disconnect is recorded half a second late; when the wait
// returns, the store must already hold it.
func TestSessionGoneWaitsForTheConnectionItWasCalledAfter(t *testing.T) {
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return slowDisconnects{SessionStore: s, pause: 500 * time.Millisecond}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	h := start(t)

	first := dial(t, h, "device", false, false, 0, 3600, 0)
	first.Close()
	sessionGone(t, h, "device")
	if h.Disconnects.Count("device") != 1 {
		t.Fatalf("the first disconnect was counted %d times, so the id does not carry an earlier "+
			"one and nothing below is about the class", h.Disconnects.Count("device"))
	}
	firstAt := func() time.Time {
		s, _, _ := brokertest.HarnessSessions.Get("device")
		return s.DisconnectedAt
	}()

	second := dial(t, h, "device", false, false, 0, 3600, 0)
	time.Sleep(10 * time.Millisecond) // the clock moves, so the two records differ
	second.Close()
	sessionGone(t, h, "device")
	s, ok, _ := brokertest.HarnessSessions.Get("device")
	if !ok || !s.DisconnectedAt.After(firstAt) {
		t.Errorf("SessionGone returned with the store still holding the first disconnect (%v, "+
			"then %v): it waited for an earlier connection's teardown, not this one's",
			firstAt, s.DisconnectedAt)
	}
}

// **A wait taken just after a takeover waits for its own connection, not the
// one taken over.** LinkCut, Gone and every SessionGoneAfter caller counted
// disconnects before closing and returned on the next one - which, straight
// after a takeover, can be the taken-over connection's, counted while the
// connection the test just ended is still being torn down: 6 of 100 in the
// validator's probe. Here that teardown's record is written a quarter of a
// second late, and the taken-over connection writes none; when LinkCut returns, the
// store must hold the new connection's disconnect.
//
// **The interleaving is built, not waited for.** The taken-over connection's
// teardown is held in the harness's disconnect hook (HoldDisconnects) until
// the cut connection's late record has begun, so every attempt begins its
// wait with the takeover uncounted and has it counted mid-teardown. Left to
// the scheduler, one P never produced that order.
func TestAWaitAfterATakeoverWaitsForItsOwnConnection(t *testing.T) {
	recording := make(chan string, 64)
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		return signalledDisconnects{slowDisconnects{SessionStore: s, pause: 250 * time.Millisecond}, recording}
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	h := start(t)
	t.Cleanup(func() { h.Disconnects.HoldDisconnects(nil) })

	const attempts = 5
	raced := 0
	for i := range attempts {
		id := fmt.Sprintf("device-%d", i)
		_ = dial(t, h, id, false, false, 0, 3600, 0)
		first, ok := h.Srv.Clients.Get(id)
		if !ok {
			t.Fatalf("attempt %d: the first connection is not in the server's table", i)
		}
		held, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		free := func() { once.Do(func() { close(release) }) }
		t.Cleanup(free) // a failed attempt must not leave a teardown held
		h.Disconnects.HoldDisconnects(func(cl *mqtt.Client) {
			if cl == first {
				close(held)
				<-release
			}
		})
		// **Returned at its CONNACK, not at its registration.** dial waits
		// until the server holds the connection, and a takeover counts the
		// connection it took over before that, so the wait below would never
		// begin ahead of the count it must not mistake for its own.
		second := brokertest.DialAt(t, h.Addr, h.Network, id, false, false, 0, 3600, 0, nil, nil) // takes the first over
		second.H, second.ID = h, id
		select {
		case <-held:
		case <-time.After(10 * time.Second):
			t.Fatalf("attempt %d: the taken-over connection's teardown never reached the hook", i)
		}
		if n := h.Disconnects.Count(id); n != 0 {
			t.Fatalf("attempt %d: the takeover was counted (%d) while its teardown was held, so the "+
				"hold is not where the count is taken and nothing here tests the class", i, n)
		}
		raced++
		// The taken-over teardown is let go once the cut connection's
		// record has begun - its quarter-second pause - so it is counted
		// while the connection the wait is for is still being torn down.
		go func() {
			for got := range recording {
				if got == id {
					break
				}
			}
			free()
		}()
		cutAt := time.Now()
		brokertest.LinkCut(t, second)
		s, ok, _ := brokertest.HarnessSessions.Get(id)
		if !ok || s.DisconnectedAt.Before(cutAt) {
			t.Errorf("attempt %d: LinkCut returned with the store holding a disconnect from %v, "+
				"before the cut at %v: it waited for the taken-over connection's teardown, not "+
				"its own", i, s.DisconnectedAt, cutAt)
		}
		h.Disconnects.HoldDisconnects(nil)
	}
	t.Logf("%d of %d attempts began the wait before the takeover was counted", raced, attempts)
	if raced != attempts {
		t.Fatalf("%d of %d attempts began the wait before the takeover was counted, so not every "+
			"attempt tested the class", raced, attempts)
	}
}

// signalledDisconnects says which client's disconnect is being recorded as
// the record begins, before slowDisconnects' pause. A full channel drops the
// name: its reader skips every other id, and one not read is waited on by
// nobody.
type signalledDisconnects struct {
	slowDisconnects
	recording chan<- string
}

func (s signalledDisconnects) Disconnected(id string, at time.Time, dropWill bool, expiry uint32) error {
	select {
	case s.recording <- id:
	default:
	}
	return s.slowDisconnects.Disconnected(id, at, dropWill, expiry)
}

// heldConn is the broker's side of one subscriber's connection, whose writes
// wait while it is held: a socket that takes nothing, whatever the kernel's
// buffers would have taken. Reads pass through.
type heldConn struct {
	net.Conn
	held    atomic.Bool
	waiting atomic.Int64 // writes that found it held
	release chan struct{}
	once    sync.Once
}

func (c *heldConn) hold() { c.held.Store(true) }

// let releases every write waiting and every one after.
func (c *heldConn) let() { c.once.Do(func() { c.held.Store(false); close(c.release) }) }

func (c *heldConn) Write(b []byte) (int, error) {
	if c.held.Load() {
		c.waiting.Add(1)
		<-c.release
	}
	return c.Conn.Write(b)
}
