package conformance

import (
	"context"
	"database/sql"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of bridge links over TCP (design.md, Bridges; config.md,
// bridges.<name>; metrics.md, Bridges) whose waits are short. Each starts a
// broker of its own with a bridge to a test peer, harness.Peer, on
// loopback: the peer records what the bridge sends, with its own receipt
// times, and answers as the test scripts. The tests whose waits run to
// seconds and more are in the intervals package.

// success is the CONNACK accepting a bridge, with nothing the peer caps or
// chooses.
var success = &harness.Connack{}

// TestBridgeConnectsAsConfigured: design.md, Bridges: "A bridge uses MQTT 5,
// Clean Start 0, its configured client id, Session Expiry and Receive
// Maximum. Its CONNECT also carries Maximum Packet Size
// limits.max_packet_size, so the peer skips a larger record instead of
// sending it (MQTT-3.1.2-25)."; "A bridge advertises Keep Alive 30
// seconds."; config.md, bridges.<name>: username, "Sagüin's user name at the
// peer"; password_file, "The file holds the password alone. One trailing
// line ending (\n or \r\n) is not counted. This key requires username.".
//
// A bridge configured with each of those keys dials the peer, whose first
// arrival is a CONNECT carrying each as configured: protocol level 5, Clean
// Start 0, the client id, a Session Expiry Interval of session_expiry's
// seconds, its Receive Maximum, a Maximum Packet Size of max_packet_size,
// Keep Alive 30, the user name and the password the file holds, its
// closing line feed not counted, and no Will.
func TestBridgeConnectsAsConfigured(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	password := testkit.WriteFile(t, t.TempDir(), "password", "s3cret\n")
	bridge := support.BridgeTo("configured", peer, support.In("a/#", 1))
	bridge.ClientID = "edge-7"
	bridge.Username = "edge"
	bridge.PasswordFile = password
	bridge.SessionExpiry = 2 * time.Hour
	bridge.ReceiveMaximum = 7
	support.Launch(t, &harness.Options{MaxPacketSize: 65_536, Bridges: []harness.Bridge{bridge}})

	c, _ := harness.Arrive[*harness.Connect](t, peer.Link(t), harness.DefaultTimeout)
	if c.ProtocolName != "MQTT" || c.Version != harness.V5 {
		t.Errorf("the CONNECT names %q level %d; want MQTT 5: \"MQTT\" level 5", c.ProtocolName, c.Version)
	}
	if c.CleanStart {
		t.Error("the CONNECT has Clean Start 1; want 0")
	}
	if c.ClientID != "edge-7" {
		t.Errorf("the CONNECT's client id is %q; want the configured edge-7", c.ClientID)
	}
	if c.KeepAlive != 30 {
		t.Errorf("the CONNECT's Keep Alive is %d; want 30 seconds", c.KeepAlive)
	}
	for _, p := range []struct {
		name string
		id   byte
		want uint32
	}{
		{"Session Expiry Interval", harness.SessionExpiryInterval, 7200},
		{"Receive Maximum", harness.ReceiveMaximum, 7},
		{"Maximum Packet Size", harness.MaximumPacketSize, 65_536},
	} {
		if got, ok := c.Props.Get(p.id); !ok || got.Int != p.want {
			t.Errorf("the CONNECT's %s is %v (present %v); want %d", p.name, got.Int, ok, p.want)
		}
	}
	if !c.HasUsername || c.Username != "edge" {
		t.Errorf("the CONNECT's user name is %q (flag %v); want edge", c.Username, c.HasUsername)
	}
	if !c.HasPassword || string(c.Password) != "s3cret" {
		t.Errorf("the CONNECT's password is %q (flag %v); want the password file's s3cret", c.Password, c.HasPassword)
	}
	if c.Will != nil {
		t.Errorf("the CONNECT carries a Will %+v; want none", c.Will)
	}
}

// TestBridgeConnectTimeoutCoversTheAttempt: design.md, Bridges: "One
// connect_timeout deadline covers each establishment attempt, from starting
// the dial through TLS, WebSocket upgrade and CONNACK."
//
// A peer accepts the bridge's connection, reads its CONNECT and never
// answers. The bridge ends the link connect_timeout after its dial began:
// no earlier than connect_timeout after the peer accepted it, less the
// little the dial took, and within a second more.
func TestBridgeConnectTimeoutCoversTheAttempt(t *testing.T) {
	t.Parallel()
	const connectTimeout = 2 * time.Second
	peer := harness.ListenedPeer(t)
	support.Launch(t, &harness.Options{ConnectTimeout: connectTimeout,
		Bridges: []harness.Bridge{support.BridgeTo("timed", peer, support.In("a/#", 1))}})

	l := peer.Link(t)
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	end, _ := support.Ending(t, l, connectTimeout+harness.DefaultTimeout)
	support.Between(t, "the unanswered attempt, from the accept to its end", end.At.Sub(l.Accepted),
		connectTimeout-250*time.Millisecond, connectTimeout+time.Second)
}

// TestBridgeSubscribesEveryFilterInOnePacket: design.md, Bridges, In: "the
// bridge subscribes at the peer in one SUBSCRIBE carrying every inbound
// filter. Each filter asks for the lower of its rule's QoS and the peer's
// Maximum QoS, with No Local, Retain As Published 1 and Retain Handling 1,
// so the peer sends retained values only for a subscription new to it.";
// "Every new link subscribes all its filters afresh."; MQTT 5 3.2.2.3.4: "If
// the Maximum QoS is absent, the Client uses a Maximum QoS of 2."
//
// A bridge has in rules on a/# at QoS 2 and b/+ at QoS 0, a both rule on c
// at QoS 1 and an out rule on d/#. On a first link, whose CONNACK carries
// Maximum QoS 1, the packet after the CONNECT is one SUBSCRIBE carrying
// a/#, b/+ and c, the inbound filters, asking QoS 1, 0 and 1, each with No
// Local, Retain As Published 1 and Retain Handling 1. The peer grants them
// and drops the link; on the next link, whose CONNACK carries no Maximum
// QoS, the SUBSCRIBE carries the three again, asking QoS 2, 0 and 1.
func TestBridgeSubscribesEveryFilterInOnePacket(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	rules := []harness.BridgeRule{
		support.In("a/#", 2), support.In("b/+", 0),
		{Filter: "c", Direction: "both", QoS: 1},
		{Filter: "d/#", Topic: "to/d/$#", Direction: "out", QoS: 1},
	}
	support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("subscribing", peer, rules...)}})

	for _, link := range []struct {
		name string
		ack  *harness.Connack
		want map[string]byte // each filter's QoS asked
	}{
		{"Maximum QoS 1", &harness.Connack{Props: harness.Properties{{ID: harness.MaximumQoS, Int: 1}}}, map[string]byte{"a/#": 1, "b/+": 0, "c": 1}},
		{"no Maximum QoS", success, map[string]byte{"a/#": 2, "b/+": 0, "c": 1}},
	} {
		l, _, s := support.LinkUp(t, peer, link.ack)
		got := make(map[string]byte, len(s.Subs))
		for _, sub := range s.Subs {
			got[sub.Filter] = sub.QoS
			if !sub.NoLocal || !sub.RetainAsPublished || sub.RetainHandling != 1 {
				t.Errorf("%s: %s is asked with No Local %v, Retain As Published %v, Retain Handling %d; want 1, 1 and 1",
					link.name, sub.Filter, sub.NoLocal, sub.RetainAsPublished, sub.RetainHandling)
			}
		}
		if len(got) != len(s.Subs) || !maps.Equal(got, link.want) {
			t.Errorf("%s: the SUBSCRIBE asks %v; want one each of %v", link.name, s.Subs, link.want)
		}
		_ = l.Close() // the peer drops the link: the next one subscribes afresh
	}
}

// TestBridgeStopDisconnectsALiveLinkAndAbandonsADial: design.md, Restart,
// Graceful stop: "A bridge whose link is up sends DISCONNECT 0x00 after the
// acknowledgements it owes the peer, and closes within its close deadline.
// A dial in progress is abandoned at once."; Flows, Closing a connection:
// "When the close begins, it fixes one deadline limits.write_timeout
// later"; "The exit status is 0 after a stop requested by SIGINT or
// SIGTERM."
//
// A broker has two bridges: up, whose link the peer accepts and grants,
// and dialling, whose peer reads the CONNECT and never answers, under a
// connect_timeout of 30 seconds. At SIGTERM, up's peer receives DISCONNECT
// 0x00, then, though it never closes its side, the link's end within the
// write timeout; dialling's link ends at once, its CONNECT followed by the
// end of the stream; and the broker exits 0 within the graceful stop's
// bound, long before the dial's connect_timeout.
func TestBridgeStopDisconnectsALiveLinkAndAbandonsADial(t *testing.T) {
	t.Parallel()
	up, dialling := harness.ListenedPeer(t), harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{ConnectTimeout: 30 * time.Second, Bridges: []harness.Bridge{
		support.BridgeTo("up", up, support.In("a/#", 1)),
		support.BridgeTo("dialling", dialling, support.In("a/#", 1)),
	}})
	live, _, _ := support.LinkUp(t, up, success)
	silent := dialling.Link(t)
	harness.Arrive[*harness.Connect](t, silent, harness.DefaultTimeout)

	stopped := time.Now()
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	d, said := harness.Arrive[*harness.Disconnect](t, live, support.StopLimit)
	if d.Reason != 0x00 {
		t.Errorf("the live link's DISCONNECT has reason 0x%02X; want 0x00", d.Reason)
	}
	end := live.Ended(t, support.StopLimit)
	support.Between(t, "the live link's close after its DISCONNECT", end.At.Sub(said.At), 0, support.WriteTimeout+500*time.Millisecond)
	abandoned := silent.Ended(t, support.StopLimit)
	support.Between(t, "the dial in progress, from SIGTERM to its end", abandoned.At.Sub(stopped), 0, support.WriteTimeout)
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the broker exited with %v; want status 0\n%s", state, b.Log())
	}
}

// TestBridgeSeries: metrics.md, Bridges: saguin_bridge_info{bridge,peer},
// gauge, "always 1"; saguin_bridge_connected{bridge}, gauge, "1 while the
// link is up"; saguin_bridge_rule_up{bridge,direction,filter,topic}, gauge,
// "in: 1 while the peer grants the subscription; out: 1 while sending; a
// both rule has one series each way, its topic empty";
// saguin_bridge_connection_attempts_total{bridge,result}, counter, "dials:
// connected or failed"; saguin_bridge_reconnects_total{bridge}, counter,
// "links re-established"; Rules: "Every series exists from start, for every
// value of its labels that applies"; "A bridge rule's filter and topic are
// as configured, as are the labels of the _info series"; "Counters start at
// 0. The _info series start at 1."
//
// A bridge with in rules on a/# and b/# and a both rule on c dials a peer
// that holds its first CONNECT unanswered: /metrics then has each series,
// of the type metrics.md gives, the info at 1, the rest at 0, both of the
// both rule's present. The peer refuses that CONNECT (CONNACK 0x87): one
// failed dial. It accepts the next and grants a/# and c but refuses b/#:
// one connected dial, the link up, a/# and c's in series at 1, b/#'s at 0,
// no reconnect. It drops the link: the link down. It accepts the next and
// grants all three: two connected dials, one reconnect, every in series at
// 1.
func TestBridgeSeries(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	both := harness.BridgeRule{Filter: "c", Direction: "both", QoS: 1}
	a, bRule := support.In("a/#", 1), support.In("b/#", 1)
	b := support.OpsLaunch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("metered", peer, a, bRule, both)}})

	const bridge = "metered"
	info := testkit.SeriesKey("saguin_bridge_info", "bridge", bridge, "peer", peer.URL())
	connected := testkit.SeriesKey("saguin_bridge_connected", "bridge", bridge)
	ruleUp := func(direction string, r harness.BridgeRule) string {
		return testkit.SeriesKey("saguin_bridge_rule_up", "bridge", bridge, "direction", direction, "filter", r.Filter, "topic", r.Topic)
	}
	upA, upB, upC, outC := ruleUp("in", a), ruleUp("in", bRule), ruleUp("in", both), ruleUp("out", both)
	dials := func(result string) string {
		return testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", bridge, "result", result)
	}
	reconnects := testkit.SeriesKey("saguin_bridge_reconnects_total", "bridge", bridge)
	types := map[string]string{
		"saguin_bridge_info": "gauge", "saguin_bridge_connected": "gauge", "saguin_bridge_rule_up": "gauge",
		"saguin_bridge_connection_attempts_total": "counter", "saguin_bridge_reconnects_total": "counter",
	}

	held := peer.Link(t)
	harness.Arrive[*harness.Connect](t, held, harness.DefaultTimeout)
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	for name, kind := range types {
		if e.Types[name] != kind {
			t.Errorf("%s is of type %q; want %s", name, e.Types[name], kind)
		}
	}
	if _, ok := e.Values[outC]; !ok {
		t.Errorf("the scrape has no %s: a both rule has one series each way", outC)
	}
	support.AwaitSampled(t, b, "from start, the first CONNECT unanswered", map[string]float64{
		info: 1, connected: 0, upA: 0, upB: 0, upC: 0, dials("connected"): 0, dials("failed"): 0, reconnects: 0,
	})

	if _, err := held.Send(&harness.Connack{Reason: 0x87}); err != nil {
		t.Fatalf("refusing the first CONNECT: %v", err)
	}
	_ = held.Close() // a server closes after a refusing CONNACK
	support.AwaitSampled(t, b, "the first dial refused", map[string]float64{connected: 0, dials("connected"): 0, dials("failed"): 1, reconnects: 0})

	l := peer.Link(t)
	l.Accept(t, success)
	s, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	reasons := make([]byte, len(s.Subs))
	for i, sub := range s.Subs {
		reasons[i] = sub.QoS
		if sub.Filter == bRule.Filter {
			reasons[i] = 0x87
		}
	}
	if _, err := l.Send(&harness.Suback{ID: s.ID, Reasons: reasons}); err != nil {
		t.Fatalf("answering the SUBSCRIBE: %v", err)
	}
	support.AwaitSampled(t, b, "the link up, b/# refused", map[string]float64{
		connected: 1, upA: 1, upB: 0, upC: 1, dials("connected"): 1, dials("failed"): 1, reconnects: 0,
	})

	_ = l.Close()
	support.AwaitSampled(t, b, "the link dropped", map[string]float64{connected: 0})
	support.LinkUp(t, peer, success)
	support.AwaitSampled(t, b, "the link re-established, every filter granted", map[string]float64{
		connected: 1, upA: 1, upB: 1, upC: 1, dials("connected"): 2, dials("failed"): 1, reconnects: 1,
	})
}

// TestReadyWhileTheBridgesPeerIsDown: design.md, Bridges: "A bridge has
// started once its first dial begins. Readiness never waits for its link
// (Operations, HTTP)."; Operations, HTTP: "/health returns 200
// {"status":"ok"} once listeners and bridges have started"; metrics.md,
// saguin_ready, "1 while listeners and bridges have started".
//
// A broker has two bridges: one to a loopback port nothing listens on, one
// to a peer that reads the CONNECT and never answers, under a
// connect_timeout of 30 seconds. Once that peer holds the CONNECT, the
// bridge's first dial begun, /health comes to answer 200 {"status":"ok"}
// and saguin_ready to read 1, while neither link is up.
func TestReadyWhileTheBridgesPeerIsDown(t *testing.T) {
	t.Parallel()
	silent := harness.ListenedPeer(t)
	down := "tcp://" + harness.DownPeer(t)
	b := support.Launch(t, &harness.Options{Operations: true, ConnectTimeout: 30 * time.Second, Bridges: []harness.Bridge{
		{Name: "down", Peer: down, ClientID: "bridge-down", Topics: []harness.BridgeRule{support.In("a/#", 1)}},
		support.BridgeTo("silent", silent, support.In("a/#", 1)),
	}})
	harness.Arrive[*harness.Connect](t, silent.Link(t), harness.DefaultTimeout)

	support.AwaitReady(t, b)
	a, err := support.OpsTry(b, http.MethodGet, "/health", "")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	support.ExpectHealth(t, http.MethodGet, a, http.StatusOK, "ok")
	support.AwaitSampled(t, b, "ready, neither link up", map[string]float64{
		testkit.SeriesKey("saguin_ready"):                                1,
		testkit.SeriesKey("saguin_bridge_connected", "bridge", "down"):   0,
		testkit.SeriesKey("saguin_bridge_connected", "bridge", "silent"): 0,
	})
}

// TestBridgeKeepsTheClientsSide: MQTT 5 3.14.0: "A Server MUST NOT send a
// DISCONNECT until after it has sent a CONNACK with Reason Code of less than
// 0x80 [MQTT-3.14.0-1]"; 3.2.2.3.6: "The Client MUST NOT send packets
// exceeding Maximum Packet Size to the Server [MQTT-3.2.2-15]"; 3.14.2.2.3
// and 3.14.2.2.4: the sender of a DISCONNECT MUST NOT send a Reason String
// or a User Property "if it would increase the size of the DISCONNECT
// packet beyond the Maximum Packet Size specified by the receiver
// [MQTT-3.14.2-3]", "[MQTT-3.14.2-4]"; design.md, Bridges: "On that
// connection Sagüin is an ordinary MQTT 5 client of the peer."; "The first
// redial after failed establishment or link loss is immediate."; Bridges,
// In: "Where one SUBSCRIBE would exceed the Maximum Packet Size the peer's
// CONNACK allows (MQTT-3.2.2-15), the filters go in as few SUBSCRIBEs as
// fit. A filter too large to fit alone is never asked for. It is one ERROR
// line on each link, and its rule is not up. The same applies to a filter
// holding + or # on a link whose CONNACK has Wildcard Subscription Available
// 0 (MQTT 5, 3.2.2.3.11)."; "Every new link subscribes all its filters afresh.";
// Logging: "Every bridge line names its bridge."; MQTT 5 3.2.2.3.11: "If not
// present, then Wildcard Subscriptions are supported."; "If the Server
// receives a SUBSCRIBE packet containing a Wildcard Subscription and it does
// not support Wildcard Subscriptions, this is a Protocol Error."; metrics.md,
// Bridges: saguin_bridge_connected, "1 while the link is up";
// saguin_bridge_connection_attempts_total, "dials: connected or failed";
// saguin_bridge_reconnects_total, "links re-established";
// saguin_bridge_rule_up, "in: 1 while the peer grants the subscription".
func TestBridgeKeepsTheClientsSide(t *testing.T) {
	t.Parallel()
	// A peer answers the bridge's CONNECT with a DISCONNECT, which no server
	// may send before its CONNACK: the link never came up. The bridge dials
	// again within a second, and the peer accepts that dial: /metrics then
	// counts one failed dial, one connected, and no reconnect, the link up.
	t.Run("a DISCONNECT before CONNACK fails the dial", func(t *testing.T) {
		t.Parallel()
		peer := harness.ListenedPeer(t)
		b := support.Launch(t, &harness.Options{Operations: true,
			Bridges: []harness.Bridge{support.BridgeTo("early", peer, support.In("a/#", 1))}})
		first := peer.Link(t)
		harness.Arrive[*harness.Connect](t, first, harness.DefaultTimeout)
		sent, err := first.Send(&harness.Disconnect{Reason: 0x87})
		if err != nil {
			t.Fatalf("sending DISCONNECT: %v", err)
		}
		_ = first.Close()
		next := peer.Link(t)
		support.Between(t, "the redial after the DISCONNECT", next.Accepted.Sub(sent), 0, time.Second)
		next.Accept(t, success)
		support.AwaitSampled(t, b, "the first dial failed, the next connected", map[string]float64{
			testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", "early", "result", "failed"):    1,
			testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", "early", "result", "connected"): 1,
			testkit.SeriesKey("saguin_bridge_reconnects_total", "bridge", "early"):                                 0,
			testkit.SeriesKey("saguin_bridge_connected", "bridge", "early"):                                        1,
		})
	})

	// A peer's CONNACK carries Maximum Packet Size 64 and Server Keep Alive
	// 2: the link's SUBSCRIBE, its PINGREQs and, at SIGTERM, its DISCONNECT
	// each reach the peer at 64 bytes or fewer.
	t.Run("the peer's Maximum Packet Size", func(t *testing.T) {
		t.Parallel()
		const peerMaximum = 64
		peer := harness.ListenedPeer(t)
		b := support.Launch(t, &harness.Options{
			Bridges: []harness.Bridge{support.BridgeTo("bounded", peer, support.In("in/#", 1))}})
		l := peer.Link(t)
		l.Accept(t, &harness.Connack{Props: harness.Properties{
			{ID: harness.MaximumPacketSize, Int: peerMaximum}, {ID: harness.ServerKeepAlive, Int: 2},
		}})
		l.AnswerPings(true)
		s, subscribed := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
		if _, err := l.Send(support.Grants(s)); err != nil {
			t.Fatalf("granting the SUBSCRIBE: %v", err)
		}
		_, pinged := harness.Arrive[*harness.Pingreq](t, l, harness.DefaultTimeout)
		if err := b.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM: %v", err)
		}
		sent := []harness.Arrival{subscribed, pinged}
		for disconnected := false; !disconnected; {
			a, err := l.Next(support.StopLimit)
			if err != nil || a.Packet == nil {
				t.Fatalf("after SIGTERM, %v %v; want the link's DISCONNECT", a, err)
			}
			sent = append(sent, a)
			_, disconnected = a.Packet.(*harness.Disconnect)
		}
		for _, a := range sent {
			if a.Size > peerMaximum {
				t.Errorf("the peer received %s, over its Maximum Packet Size of %d", a, peerMaximum)
			}
		}
		support.Exited(t, b)
	})

	// A peer's CONNACK carries Maximum Packet Size 32. The bridge has three
	// in rules whose filters take 13 bytes each in a SUBSCRIBE, whose
	// fixed header, Packet Identifier and empty property section take 5: two
	// fit in one SUBSCRIBE (31 bytes), three do not (44). A fourth filter,
	// of 31 bytes, takes 34, 39 in a SUBSCRIBE alone: too large to fit. The link's
	// SUBSCRIBEs, each granted, are two, each within 32 bytes, asking for
	// the three small filters once each, the large one in none. The log
	// holds one ERROR line naming the bridge and the large filter; on
	// /metrics its rule is 0, the others 1.
	t.Run("the filters split to fit the peer's Maximum Packet Size", func(t *testing.T) {
		t.Parallel()
		const peerMaximum = 32
		const name = "split"
		peer := harness.ListenedPeer(t)
		rules := []harness.BridgeRule{
			support.In("f/aaaaaaaa", 1), support.In("f/bbbbbbbb", 1), support.In("f/cccccccc", 1),
			support.In("large/"+strings.Repeat("x", 25), 1),
		}
		small, large := rules[:3], rules[3]
		b := support.Launch(t, &harness.Options{Operations: true,
			Bridges: []harness.Bridge{support.BridgeTo(name, peer, rules...)}})
		l := peer.Link(t)
		l.Accept(t, &harness.Connack{Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: peerMaximum}}})
		asked := map[string]int{}
		subscribes := 0
		for len(asked) < len(small) {
			s, a := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
			subscribes++
			if a.Size > peerMaximum {
				t.Errorf("the peer received %s, over its Maximum Packet Size of %d", a, peerMaximum)
			}
			for _, f := range support.Filters(s) {
				asked[f]++
			}
			if _, err := l.Send(support.Grants(s)); err != nil {
				t.Fatalf("granting the SUBSCRIBE: %v", err)
			}
		}
		want := map[string]int{}
		for _, r := range small {
			want[r.Filter] = 1
		}
		if subscribes != 2 || !maps.Equal(asked, want) {
			t.Errorf("%d SUBSCRIBEs asked %v; want 2, asking %v, each once, %s in none", subscribes, asked, want, large.Filter)
		}
		oneErrorLine(t, b, name, large.Filter)
		up := map[string]float64{inRuleUp(name, large): 0}
		for _, r := range small {
			up[inRuleUp(name, r)] = 1
		}
		support.AwaitSampled(t, b, "the small filters granted, the large one never asked", up)
	})

	// A peer's CONNACK carries Wildcard Subscription Available 0. The
	// bridge has in rules on p/+ and h/#, each holding a wildcard, and on
	// the exact topic e/x. The link's SUBSCRIBE asks for e/x alone, and the
	// peer grants it. The log holds, for each of p/+ and h/#, one ERROR
	// line naming the bridge and the filter; on /metrics their rules are 0,
	// e/x's 1. The link stays up: for three seconds nothing more reaches
	// the peer, neither the link's end nor a new link, and /metrics then
	// reads the link up, one connected dial, none failed and no reconnect.
	// The peer drops the link; the next, whose CONNACK has no Wildcard
	// Subscription Available, asks for all three, and once they are
	// granted each rule is 1.
	t.Run("no wildcard filter where the peer allows none", func(t *testing.T) {
		t.Parallel()
		const name = "exact-only"
		const stays = 3 * time.Second // a redial after link loss is immediate
		peer := harness.ListenedPeer(t)
		plus, hash, exact := support.In("p/+", 1), support.In("h/#", 1), support.In("e/x", 1)
		b := support.Launch(t, &harness.Options{Operations: true,
			Bridges: []harness.Bridge{support.BridgeTo(name, peer, plus, hash, exact)}})
		connected := testkit.SeriesKey("saguin_bridge_connected", "bridge", name)
		dials := func(result string) string {
			return testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", name, "result", result)
		}

		l := peer.Link(t)
		l.Accept(t, support.QuietWith(false, harness.Property{ID: harness.WildcardSubscriptionAvailable, Int: 0}))
		s, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
		if got := support.Filters(s); !slices.Equal(got, []string{exact.Filter}) {
			t.Fatalf("the SUBSCRIBE asks for %q; want %s alone, the peer allowing no wildcard", got, exact.Filter)
		}
		support.PeerSends(t, l, support.Grants(s))
		support.AwaitSampled(t, b, "e/x granted, p/+ and h/# never asked", map[string]float64{
			inRuleUp(name, plus): 0, inRuleUp(name, hash): 0, inRuleUp(name, exact): 1, connected: 1,
		})
		if a, err := l.Next(stays); err == nil {
			t.Errorf("the peer received %s; want the link up, nothing more sent", a)
		}
		if _, err := peer.Next(10 * time.Millisecond); err == nil {
			t.Error("a new link reached the peer; want the first one to stay up")
		}
		support.AwaitSampled(t, b, "the link still up, never redialled", map[string]float64{
			connected: 1, dials("connected"): 1, dials("failed"): 0,
			testkit.SeriesKey("saguin_bridge_reconnects_total", "bridge", name): 0,
		})
		for _, r := range []harness.BridgeRule{plus, hash} {
			oneErrorLine(t, b, name, r.Filter)
		}

		_ = l.Close() // the peer drops the link
		_, _, again := support.LinkUp(t, peer, support.Quiet)
		all := []string{exact.Filter, hash.Filter, plus.Filter}
		if got := slices.Sorted(slices.Values(support.Filters(again))); !slices.Equal(got, all) {
			t.Errorf("the next link's SUBSCRIBE asks for %q; want each of %q once", got, all)
		}
		support.AwaitSampled(t, b, "every filter granted on the next link", map[string]float64{
			inRuleUp(name, plus): 1, inRuleUp(name, hash): 1, inRuleUp(name, exact): 1,
		})
	})
}

// inRuleUp names, as testkit.SeriesKey does, the saguin_bridge_rule_up series
// of the bridge named bridge for r's in direction.
func inRuleUp(bridge string, r harness.BridgeRule) string {
	return testkit.SeriesKey("saguin_bridge_rule_up", "bridge", bridge, "direction", "in", "filter", r.Filter, "topic", r.Topic)
}

// oneErrorLine fails the test unless b's log comes to hold, within
// harness.DefaultTimeout, an ERROR line naming filter, the one ERROR line
// naming it, and that line names bridge too: a filter never asked for
// (design.md, Bridges, In: "It is one ERROR line on each link"; Logging:
// "Every bridge line names its bridge.").
func oneErrorLine(t *testing.T, b *harness.Broker, bridge, filter string) {
	t.Helper()
	var lines []string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		lines = testkit.LinesNaming(b.Log(), "ERROR", filter)
		return len(lines) > 0
	}) || len(lines) != 1 || !strings.Contains(lines[0], bridge) {
		t.Errorf("the ERROR lines naming %s: %q; want one, naming the bridge %s", filter, lines, bridge)
	}
}

// bridgeLeft is what the database holds of a bridge's state: whether its
// row is there, its held publishes' packet identifiers, its in-flight
// rows, and one rule's cursor on $broadcast, 0 for none.
type bridgeLeft struct {
	row      bool
	held     []uint16
	inflight []bridgeInflight
	cursor   int64
}

// leftOf reads from db what it holds of the bridge named's state, rule's
// cursor among it (storage.md, bridges: "name | unique: the bridge's
// name").
func leftOf(t *testing.T, db *sql.DB, bridge string, rule int64) bridgeLeft {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM bridges WHERE name = ?`, bridge).Scan(&n); err != nil {
		t.Fatalf("reading the row of bridge %s: %v", bridge, err)
	}
	return bridgeLeft{n == 1, heldOf(t, db, bridge), inflightTo(t, db, bridge), cursorOf(t, db, rule)}
}

// TestRemovedBridgeIsClearedAtStart: storage.md, bridges: "This table holds
// each configured bridge and each removed bridge still named by a record,
// rule or held publish. A removed bridge's exchanges, held publishes and
// cursors are deleted at start, in one transaction. Its row is deleted only
// when nothing names it any more."; records: "bridge | the id of the bridge
// it arrived over, 0 for none"; design.md, Restart, A changed
// configuration: "The cursors of a removed bridge or rule are deleted at
// start and logged."
//
// A bridge, removed, has an in rule on a/# at QoS 2 and an out rule taking
// w/# to w/$# at QoS 2. Its peer forwards a/kept at QoS 1, stored, a record
// naming the bridge, and a/held at QoS 2 under packet identifier 2,
// answered PUBREC and never released. A client stores w/0, which crosses
// and completes, and the out rule's cursor on $broadcast comes to w/0's
// offset plus one; it stores w/1, which reaches the peer unanswered. Once
// the broker has stopped, its database holds the bridge's row, its held
// publish 2, an in-flight row and the rule's cursor. Started again without
// the bridge, the database comes to hold the bridge's row, still named by
// a/kept's record, and none of its held publishes, in-flight rows or
// cursors.
func TestRemovedBridgeIsClearedAtStart(t *testing.T) {
	t.Parallel()
	const name = "removed"
	out := support.Out("w/#", "w/$#", 2)
	peer := harness.ListenedPeer(t)
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{elsewhere},
		Bridges: []harness.Bridge{support.BridgeTo(name, peer, support.In("a/#", 2), out)}}
	b := support.Launch(t, o)
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	support.Bridged(t, l, fromPeer(1, 1, "a/kept", "names the bridge"))
	support.PeerAnswered(t, l, fromPeer(2, 2, "a/held", "held"), 0x00)
	pub := localClient(t, b, "removed-pub")
	settled := &harness.Publish{QoS: 2, Topic: "w/0", Payload: []byte("w/0")}
	published(t, pub, settled)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "w/0", 2, settled.Payload)
	db := support.OpenDB(t, b.DB)
	rule := ruleOf(t, db, name, out)
	cursorComesTo(t, db, pub, rule, offsetsByPayload(t, b.DB)[string(settled.Payload)]+1, "w/0 settled")
	published(t, pub, &harness.Publish{QoS: 2, Topic: "w/1", Payload: []byte("w/1")})
	harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	stopped(t, b)
	if before := leftOf(t, db, name, rule); !before.row || !slices.Equal(before.held, []uint16{2}) || len(before.inflight) != 1 || before.cursor == 0 {
		t.Fatalf("before the restart, the database holds %+v of %s; want its row, held publish 2, one in-flight row and a cursor", before, name)
	}

	o.Bridges = nil
	b = support.Launch(t, o)
	db = support.OpenDB(t, b.DB)
	awaitState(t, name+"'s row, still named by a record, and none of its held publishes, in-flight rows or cursors", func() bridgeLeft {
		return leftOf(t, db, name, rule)
	}, func(left bridgeLeft) bool {
		return left.row && len(left.held) == 0 && len(left.inflight) == 0 && left.cursor == 0
	})
}

// replayInterrupted fills the small disk at disk (testkit.FillDisk) and
// has the peer drop l. The peer answers the bridge's next CONNECT with
// Session Present 0, which starts a replay whose mark the full disk
// refuses, and the bridge must end that link (design.md, Bridges: "When the
// mark cannot be stored, the link ends. The bridge keeps the mark in memory
// alone until it is stored."). It returns the bridge's next link,
// that link's CONNECT, unanswered, and the ballast filling the disk, which
// the caller truncates to give room back.
func replayInterrupted(t *testing.T, disk string, peer *harness.Peer, l *harness.PeerLink) (*harness.PeerLink, *harness.Connect, *os.File) {
	t.Helper()
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	_ = l.Close() // the peer drops the link
	support.Ending(t, support.Accepted(t, peer, support.Quiet), harness.DefaultTimeout)
	next := peer.Link(t)
	c, _ := harness.Arrive[*harness.Connect](t, next, harness.DefaultTimeout)
	return next, c, ballast
}

// replayOwed reads from db whether the bridge named owes a replay
// (storage.md, bridges: replay_owed, "1 from a CONNACK with Session Present
// 0 until its replay commits").
func replayOwed(t *testing.T, db *sql.DB, bridge string) bool {
	t.Helper()
	var owed int
	if err := db.QueryRowContext(context.Background(), `SELECT replay_owed FROM bridges WHERE name = ?`, bridge).Scan(&owed); err != nil {
		t.Fatalf("reading whether bridge %s owes a replay: %v", bridge, err)
	}
	return owed == 1
}

// TestBridgeTakesSessionPresentWithoutState: design.md, Bridges: "A CONNACK
// with Session Present 1 is accepted even when the bridge holds no state for
// the peer. The records the peer kept for it still arrive. This departs from
// MQTT-3.2.2-4."; Bridges, In: "Every new link subscribes all its
// filters afresh."; Flows, PUBLISH: "1 | on PUBLISH | PUBACK once stored";
// MQTT 5 3.2.2.1.1: "If the Client does not have Session State and receives
// Session Present set to 1 it MUST close the Network Connection
// [MQTT-3.2.2-4]."; metrics.md, Bridges: saguin_bridge_connected, "1 while
// the link is up"; saguin_bridge_connection_attempts_total, "dials:
// connected or failed"; saguin_bridge_received_total, "records from the
// peer that were stored".
//
// A broker with a fresh database, so its bridge holds no state for the
// peer, has a bridge with an in rule on a/# at QoS 1, and a local client
// subscribes to from/# at QoS 1. The bridge's first CONNACK has Session
// Present 1: the link stays, its SUBSCRIBE comes and is granted, and
// /metrics reads the link up, one connected dial and none failed. The peer
// then sends a/kept at QoS 1 with DUP 1, as a peer resending a message it
// kept for the bridge's session does (MQTT-4.4.0-1): it gets PUBACK 0x00,
// the subscriber's next packet is from/a/kept, and /metrics counts one
// record received over the bridge.
func TestBridgeTakesSessionPresentWithoutState(t *testing.T) {
	t.Parallel()
	const name = "resumed"
	peer := harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{Operations: true,
		Bridges: []harness.Bridge{support.BridgeTo(name, peer, support.In("a/#", 1))}})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "resumed-sub", CleanStart: true}, support.At(1, "from/#"))

	l, _, _ := support.LinkUp(t, peer, support.QuietResumed)
	support.AwaitSampled(t, b, "the link up on Session Present 1", map[string]float64{
		testkit.SeriesKey("saguin_bridge_connected", "bridge", name):                                        1,
		testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", name, "result", "connected"): 1,
		testkit.SeriesKey("saguin_bridge_connection_attempts_total", "bridge", name, "result", "failed"):    0,
	})
	kept := &harness.Publish{QoS: 1, ID: 1, Dup: true, Topic: "a/kept", Payload: []byte("kept for the bridge")}
	support.Bridged(t, l, kept)
	support.DeliveredAt(t, sub, 1, "from/a/kept", kept.Payload)
	support.AwaitSampled(t, b, "the kept record stored", map[string]float64{
		testkit.SeriesKey("saguin_bridge_received_total", "bridge", name): 1,
	})
}
