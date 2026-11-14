package conformance

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of live reload by SIGUSR1 (item 0018g): design.md, Access and
// reload, "SIGUSR1 reloads the following in steps. Each step is
// all-or-nothing. An invalid file is logged and the old one kept", its three
// steps (the log level and WebSocket origins; the password files and the
// acl_file; every TLS certificate, key and client CA) and what follows
// them; Flows, PUBLISH, Rate limits, for a reload's publish limits;
// metrics.md, Process, saguin_reloads_total. Each test changes the files on
// disk, as an operator does, then sends the broker SIGUSR1 (reload).

// reloadParts are saguin_reloads_total's parts, one per step: "part is
// log_and_origins, access (password files and acl_file) or tls"
// (metrics.md, Process).
var reloadParts = []string{"log_and_origins", "access", "tls"}

// reloadSeries is saguin_reloads_total of part and result, "applied or
// refused" (metrics.md, Process).
func reloadSeries(part, result string) string {
	return testkit.SeriesKey("saguin_reloads_total", "part", part, "result", result)
}

// reload sends b, which has an operations listener, SIGUSR1, and waits
// until /metrics has counted each of its steps once, refused for the parts
// refused names and applied for the others, failing the test where it has
// not within harness.DefaultTimeout, the test's watchdog (design.md, Access
// and reload: "Each step is logged and counted once"). A step is counted
// once its copy is swapped in, so what the test does next comes after the
// swap.
func reload(t *testing.T, b *harness.Broker, refused ...string) {
	t.Helper()
	series := make([]string, 0, 2*len(reloadParts))
	for _, part := range reloadParts {
		series = append(series, reloadSeries(part, "applied"), reloadSeries(part, "refused"))
	}
	want := support.Sampled(t, b, series...)
	for _, part := range reloadParts {
		result := "applied"
		if slices.Contains(refused, part) {
			result = "refused"
		}
		want[reloadSeries(part, result)]++
	}
	if err := b.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("SIGUSR1: %v", err)
	}
	awaitSeries(t, b, want)
}

// edited replaces the one occurrence of old in the file at path with with,
// as an operator edits a file before a SIGUSR1, failing the test unless old
// occurs there exactly once.
func edited(t *testing.T, path, old, with string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), old); n != 1 {
		t.Fatalf("%s holds %q %d times; want it once", path, old, n)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), old, with, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The origins the reload tests' ws listeners allow, one before a reload
// and the other after it.
const (
	firstOrigin  = "https://first.example"
	secondOrigin = "https://second.example"
)

// originsChecked fails the test unless a WebSocket upgrade to addr from
// the origin allowed passes the origin check and one from the origin
// refused gets HTTP 403 (design.md, Flows, Transport setup: "the origin
// must pass same_origin or appear in allowed_origins"; "A refused origin
// gets HTTP 403 and is logged"). Neither connects: the upgrade is what is
// checked.
func originsChecked(t *testing.T, addr, allowed, refused string) {
	t.Helper()
	upgrade := func(origin string) error {
		c, err := harness.DialWebSocket(addr, harness.V5, &harness.WebSocketDial{Subprotocols: []string{"mqtt"},
			Header: http.Header{"Origin": {origin}}})
		if err == nil {
			_ = c.Close() // upgraded: what the check asks
		}
		return err
	}
	if err := upgrade(allowed); err != nil {
		t.Errorf("an upgrade from %s: %v; want it upgraded, %s being allowed", allowed, err, allowed)
	}
	var answered *harness.UpgradeError
	switch err := upgrade(refused); {
	case err == nil:
		t.Errorf("an upgrade from %s was upgraded; want 403, %s being no allowed origin", refused, refused)
	case !errors.As(err, &answered) || answered.StatusCode != http.StatusForbidden:
		t.Errorf("an upgrade from %s: %v; want it answered 403", refused, err)
	}
}

// design.md, Access and reload: "SIGUSR1 reloads the following in steps.
// Each step is all-or-nothing. An invalid file is logged and the old one
// kept"; "Each step is logged and counted once"; "A step applied is one
// INFO line naming it. A step refused is one ERROR line naming the file,
// the entry and what is wrong, worded as a refused start or --check-config
// words it". metrics.md, Process: saguin_reloads_total, "part is
// log_and_origins, access (password files and acl_file) or tls; result is
// applied or refused". config.md, tls: a start refused for a certificate
// and key that do not load as a pair "names the listener, the file and the
// error" (TestStartRefusesTLSFilesThatDoNotLoad).
//
// A broker has log_level info, a ws listener allowing firstOrigin alone, a
// password file holding alice, an acl_file naming alice and carol, and a
// TLS listener, tcp2, serving the test's server certificate. Every step's
// files change: the configuration's allowed origin becomes secondOrigin;
// the password file adds carol; the server's certificate and key are
// replaced by a second certificate of the same authority. Each case breaks
// one step's files besides: the configuration's log_level becomes loud;
// the acl_file's entry alice gains an unknown key, colour; or the key file
// holds another certificate's key. After the SIGUSR1, the broken step
// counts refused and the others applied; the log gains an INFO line for
// each applied step and one ERROR line, naming the broken file and its
// entry: log_level; alice and colour, as a refused start names them; or
// tcp2, both files and Go's error for the pair. The broken step keeps all of its old copy, though one
// of its files was good: the origins allowed stay firstOrigin's, carol is
// refused with 0x86, or tcp2 still presents the first certificate. The
// other steps take their new copies.
func TestReloadStepIsAllOrNothing(t *testing.T) {
	for _, broken := range reloadParts {
		t.Run(broken, func(t *testing.T) {
			certs := newPKI(t)
			second := issue(t, certs.ca, &testkit.Leaf{CommonName: "saguin-2", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
			other := issue(t, certs.ca, &testkit.Leaf{CommonName: "other", Server: true})
			acl := "roles:\n" + aclFree + "users:\n  alice: [free]\n  carol: [free]\n"
			o := aclOptions(t, acl, "alice")
			o.Channels = aclChannels
			o.LogLevel = "info"
			o.TCPListeners = 2
			o.ListenerTLS = []harness.TLS{{}, certs.listener(&harness.TLS{})}
			o.WS = []harness.WS{{SameOrigin: "false", AllowedOrigins: []string{firstOrigin}}}
			b := support.OpsLaunch(t, o)

			edited(t, b.Config, strconv.Quote(firstOrigin), strconv.Quote(secondOrigin))
			usersFile(t, o.Dir, "passwd", "alice", "carol")
			testkit.WriteFile(t, certs.dir, "server.crt", string(second.CertPEM))
			testkit.WriteFile(t, certs.dir, "server.key", string(second.KeyPEM))
			var names []string
			switch broken {
			case "log_and_origins":
				edited(t, b.Config, "log_level: info", "log_level: loud")
				names = []string{b.Config, "log_level"}
			case "access":
				testkit.WriteFile(t, o.Dir, "acl.yaml", strings.Replace(acl, "alice: [free]", "alice: {roles: [free], colour: blue}", 1))
				names = []string{o.ACLFile, "alice", "colour"}
			case "tls":
				testkit.WriteFile(t, certs.dir, "server.key", string(other.KeyPEM))
				_, err := tls.LoadX509KeyPair(certs.certFile, certs.keyFile)
				if err == nil {
					t.Fatal("the second certificate and another's key load as a pair")
				}
				names = []string{"tcp2", certs.certFile, certs.keyFile, err.Error()}
			}
			logged := len(b.Log())
			reload(t, b, broken)

			var since string
			if !testkit.Within(harness.DefaultTimeout, func() bool {
				since = b.Log()[logged:]
				return len(testkit.LogLines(since, "INFO")) >= len(reloadParts)-1 && len(testkit.LogLines(since, "ERROR")) >= 1
			}) {
				t.Errorf("watchdog: %v after the reload, the log gained %q; want an INFO line for each of the %d steps applied and an ERROR line for the one refused",
					harness.DefaultTimeout, since, len(reloadParts)-1)
			}
			if lines := testkit.LogLines(since, "ERROR"); len(lines) != 1 || !namesAll(lines[0], names...) {
				t.Errorf("ERROR lines since the reload: %q; want one, naming each of %q", lines, names)
			}

			allowed, refused := secondOrigin, firstOrigin
			if broken == "log_and_origins" {
				allowed, refused = firstOrigin, secondOrigin
			}
			originsChecked(t, b.WS[0], allowed, refused)
			if broken == "access" {
				connackRefuses(t, b.Addr, harness.V5, aclLogin(clientID(t, "-carol"), "carol"), 0x86)
			} else {
				harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-carol"), "carol"))
			}
			served := second
			if broken == "tls" {
				served = certs.server
			}
			presented(t, dialTLS(t, b.Addrs[1], harness.V5, certs.client(nil)).Conn, served)
		})
	}
}

// design.md, Access and reload: "It reloads the log level and WebSocket
// origins"; "It also sends the core each new copy the core uses, the log
// level included, as an event. The core applies a new log level from the
// next line it admits"; Flows, Logging: "A failed TLS handshake [...] [is]
// one WARN line"; "The level is configured by broker.log_level"; config.md,
// broker: log_level, "debug, info, warn or error".
//
// A broker has log_level error, a TLS listener and a ws listener with
// same_origin false allowing firstOrigin alone: an upgrade from firstOrigin
// passes and one from secondOrigin gets 403. A plaintext CONNECT to the TLS
// listener fails its handshake (tls_handshake rises by one). The
// configuration then has log_level warn and allows secondOrigin alone, and
// the broker gets SIGUSR1. A second plaintext CONNECT fails its handshake:
// the log then holds one WARN line naming the TLS error, the second's, the
// first's having come at level error. An upgrade from secondOrigin now
// passes and one from firstOrigin gets 403.
func TestReloadTakesTheLogLevelAndOriginsLive(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{Dir: t.TempDir(), LogLevel: "error",
		ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})},
		WS:          []harness.WS{{SameOrigin: "false", AllowedOrigins: []string{firstOrigin}}}})
	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	want := handshakeError(t, &certs.server.Certificate, connect)
	before := baseline(t, b, tlsRefused)[tlsRefused]
	sentToClose(t, "tcp", b.Addr, connect)
	awaitSeries(t, b, map[string]float64{tlsRefused: before + 1})
	originsChecked(t, b.WS[0], firstOrigin, secondOrigin)

	edited(t, b.Config, "log_level: error", "log_level: warn")
	edited(t, b.Config, strconv.Quote(firstOrigin), strconv.Quote(secondOrigin))
	reload(t, b)
	sentToClose(t, "tcp", b.Addr, connect)
	awaitSeries(t, b, map[string]float64{tlsRefused: before + 2})
	var lines []string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		lines = testkit.LinesNaming(b.Log(), "WARN", want)
		return len(lines) > 0
	}) {
		t.Errorf("watchdog: %v after a failed handshake at log_level warn, no WARN line names its TLS error %q; the log:\n%s", harness.DefaultTimeout, want, b.Log())
	}
	if len(lines) > 1 {
		t.Errorf("WARN lines naming the TLS error %q: %q; want one, the handshake failed at log_level error writing none", want, lines)
	}
	originsChecked(t, b.WS[0], secondOrigin, firstOrigin)
}

// design.md, Access and reload: "Nothing else reloads"; Flows, CONNECT, 8:
// the CONNACK carries "Receive Maximum (qos2.max_inflight_per_client)" and
// "Maximum Packet Size (limits.max_packet_size)".
//
// A broker has qos2.max_inflight_per_client 5 and limits.max_packet_size
// 4096. Its configuration then has 7 and 8192, and it gets SIGUSR1, every
// step applied. A new MQTT 5 client's CONNACK still carries Receive Maximum
// 5 and Maximum Packet Size 4096.
func TestReloadLeavesOtherKeysAlone(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Dir: t.TempDir(), MaxInflightPerClient: 5, MaxPacketSize: 4096})
	edited(t, b.Config, "max_inflight_per_client: 5", "max_inflight_per_client: 7")
	edited(t, b.Config, "max_packet_size: 4096", "max_packet_size: 8192")
	reload(t, b)
	_, ack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	for _, w := range []struct {
		id   byte
		name string
		want uint32
	}{{harness.ReceiveMaximum, "Receive Maximum", 5}, {harness.MaximumPacketSize, "Maximum Packet Size", 4096}} {
		if v, ok := connackSays(ack, w.id); !ok || v != w.want {
			t.Errorf("after the reload, CONNACK's %s is %d (present %v); want %d, the value the broker started with", w.name, v, ok, w.want)
		}
	}
}

// design.md, Access and reload: "It reloads the password files for new
// connections"; "An MQTT client's password is checked against the new
// password file from its next CONNECT"; CONNECT's refusal table: 0x86, "a
// wrong or missing credential". config.md, A listener: password_file, "the
// listener's own file".
//
// Listener tcp1 reads the broker's password file, holding alice and bob;
// tcp2 its own, holding dave. alice is connected on tcp1 and dave on tcp2.
// The broker's file then holds bob and carol, tcp2's erin, and the broker
// gets SIGUSR1. carol and bob connect on tcp1 and erin on tcp2, CONNACK
// 0x00; alice's new CONNECT on tcp1 and dave's on tcp2 are refused with
// 0x86. alice's and dave's connections, open across the reload, still
// answer PINGREQ.
func TestReloadTakesPasswordFilesForNewConnections(t *testing.T) {
	dir := t.TempDir()
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, TCPListeners: 2,
		Login:         harness.Login{PasswordFile: usersFile(t, dir, "passwd", "alice", "bob")},
		ListenerLogin: []harness.Login{{}, {PasswordFile: usersFile(t, dir, "tcp2-passwd", "dave")}}})
	alice, _ := harness.Connected(t, b.Addrs[0], harness.V5, aclLogin(clientID(t, "-alice"), "alice"))
	dave, _ := harness.Connected(t, b.Addrs[1], harness.V5, aclLogin(clientID(t, "-dave"), "dave"))

	usersFile(t, dir, "passwd", "bob", "carol")
	usersFile(t, dir, "tcp2-passwd", "erin")
	reload(t, b)
	for _, c := range []struct{ user, addr string }{{"carol", b.Addrs[0]}, {"bob", b.Addrs[0]}, {"erin", b.Addrs[1]}} {
		harness.Connected(t, c.addr, harness.V5, aclLogin(clientID(t, "-new-"+c.user), c.user))
	}
	connackRefuses(t, b.Addrs[0], harness.V5, aclLogin(clientID(t, "-new-alice"), "alice"), 0x86)
	connackRefuses(t, b.Addrs[1], harness.V5, aclLogin(clientID(t, "-new-dave"), "dave"), 0x86)
	for name, c := range map[string]*harness.Client{"alice": alice, "dave": dave} {
		if err := c.Ping(); err != nil {
			t.Errorf("%s's connection, open across the reload: %v; want it to stay", name, err)
		}
	}
}

// reloadACL is the acl_file of TestReloadReachesOpenConnections, its rules
// granting where granted: pub may do everything (aclFree); watcher may
// read misc/# and, granted, the channel events; sender may write out/# and,
// granted, shut/# and wills/#.
func reloadACL(granted bool) string {
	watch := "  watch:\n    - topic: \"misc/#\"\n      allow: [read]\n"
	speak := "  speak:\n    - topic: \"out/#\"\n      allow: [write]\n"
	if granted {
		watch += "    - channel: events\n      allow: [read]\n"
		speak += "    - topic: \"shut/#\"\n      allow: [write]\n    - topic: \"wills/#\"\n      allow: [write]\n"
	}
	return "roles:\n" + aclFree + watch + speak + "users:\n  pub: [free]\n  watcher: [watch]\n  sender: [speak]\n"
}

// design.md, Access and reload: "It reloads [...] the acl_file for every
// check after the swap, including open connections and waiting Wills";
// "Every check after the swap uses the new acl_file. A connection rebuilds
// its compiled rules at its first check after the swap. The core wakes a
// subscription held by the old rules as soon as it takes the new copy, if
// the new rules allow it, without waiting for its next record"; "When the
// rules no longer allow a stored subscription at all, the subscription is
// held. It receives nothing and keeps its position until a rule allows it
// again"; DISCONNECT, A lost connection: "The Will is dropped and counted
// if the acl_file no longer allows its topic or the will feature"; CONNECT,
// 6: "Clean start ends an existing session and publishes its delayed
// Will"; Flows, PUBLISH: "0x87 | the acl_file refuses the topic".
// metrics.md: saguin_subscriptions_held; saguin_wills_dropped_total,
// not_authorized.
//
// Under reloadACL(true), on a broker with the append channel events:
// watcher subscribes to events/# at QoS 1; a subscriber of pub's to
// wills/#; sender's connection publishes shut/a, PUBACK 0x00; and a
// durable session of sender's client willer holds a Will on wills/willer,
// delayed an hour, its connection dropped. The acl_file then becomes
// reloadACL(false), and the broker gets SIGUSR1. sender's open connection
// publishes shut/a again: PUBACK 0x87, and it still answers PINGREQ. pub
// publishes events/a/1: watcher's subscription is held
// (saguin_subscriptions_held 1). willer connects again with Clean Start 1,
// which publishes its Will: it is dropped and counted not_authorized, the
// store no longer holds it, and the wills subscriber's next packet is a
// live wills/end. The acl_file becomes reloadACL(true) again, and the broker
// gets SIGUSR1: watcher is sent events/a/1, though nothing was published
// since, and nothing is held.
func TestReloadReachesOpenConnections(t *testing.T) {
	o := aclOptions(t, reloadACL(true), "pub", "watcher", "sender")
	o.Channels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}
	b := support.OpsLaunch(t, o)
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	watcher := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, "-watcher"), "watcher"), support.At(1, "events/#"))
	wills := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, "-wills"), "pub"), support.At(1, "wills/#"))
	sender, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-sender"), "sender"))
	support.Acknowledged(t, sender, &harness.Publish{QoS: 1, Topic: "shut/a", Payload: []byte("before")})
	willer := aclLogin(clientID(t, "-willer"), "sender")
	willer.Props = support.Durable
	willer.Will = &harness.Will{QoS: 1, Topic: "wills/willer", Payload: support.WillPayload, Props: support.WillDelay(support.Hour)}
	w, _ := harness.Connected(t, b.Addr, harness.V5, willer)
	support.DropSocket(t, w)
	support.AwaitWillDue(t, db, willer.ClientID)

	testkit.WriteFile(t, o.Dir, "acl.yaml", reloadACL(false))
	reload(t, b)
	refused := &harness.Publish{QoS: 1, Topic: "shut/a", Payload: []byte("after")}
	support.Sent(t, sender, refused)
	support.PubackOf(t, sender, refused, 0x87)
	if err := sender.Ping(); err != nil {
		t.Fatalf("sender's connection, after a publish the reload forbade: %v; want it to stay", err)
	}
	held := &harness.Publish{QoS: 1, Topic: "events/a/1", Payload: []byte("held")}
	support.Acknowledged(t, pub, held)
	awaitSeries(t, b, map[string]float64{heldSeries: 1})
	taker := aclLogin(willer.ClientID, "sender")
	harness.Connected(t, b.Addr, harness.V5, taker)
	awaitSeries(t, b, map[string]float64{willsDropped("not_authorized"): 1})
	awaitWillGone(t, db, willer.ClientID, "a clean start of its session")
	thenLiveAt(t, pub, wills, 1, "wills/end")

	testkit.WriteFile(t, o.Dir, "acl.yaml", reloadACL(true))
	reload(t, b)
	support.DeliveredAt(t, watcher, 1, held.Topic, held.Payload)
	awaitSeries(t, b, map[string]float64{heldSeries: 0})
}

// featuresACL is the acl_file of TestReloadDeniesFeaturesPerMessage: feat
// may do everything (aclFree), and, denied, its role narrowed denies it
// persistent, retained, qos2 and share.
func featuresACL(denied bool) string {
	if !denied {
		return "roles:\n" + aclFree + "users:\n  feat: [free]\n"
	}
	return "roles:\n" + aclFree + "  narrowed:\n    - broker: features\n      deny: [persistent, retained, qos2, share]\n" +
		"users:\n  feat: [free, narrowed]\n"
}

// design.md, Access and reload: "A feature a reload denies is refused per
// message on open connections, with 0x87 on PUBACK, PUBREC or SUBACK. A
// reload itself never disconnects a client. [...] The next CONNACK reflects
// the denial. The persistent feature applies from the next connection"; Flows, CONNECT:
// "A client whose persistent is denied is accepted with Session Expiry 0";
// 8: the CONNACK carries "the Maximum QoS, Retain Available and Shared
// Subscription Available the acl_file allows"; Session Present "is 1 when
// the CONNECT keeps its session and a session exists for the client id".
//
// feat connects a durable MQTT 5 session; its CONNACK withholds nothing.
// The acl_file then denies feat persistent, retained, qos2 and share, and
// the broker gets SIGUSR1. On the open connection a retained PUBLISH at
// QoS 1 gets PUBACK 0x87, a QoS 2 PUBLISH PUBREC 0x87, and a SUBSCRIBE to
// $share/g/feat/# at QoS 1 SUBACK 0x87; a plain QoS 1 PUBLISH still gets
// PUBACK 0x00, and the connection answers PINGREQ. feat disconnects and
// connects again with Clean Start 0: Session Present 1, the session kept
// past the connection it was made on, and a CONNACK of Session Expiry
// Interval 0, Maximum QoS 1, Retain Available 0 and Shared Subscription
// Available 0.
func TestReloadDeniesFeaturesPerMessage(t *testing.T) {
	o := aclOptions(t, featuresACL(false), "feat")
	o.Channels = aclChannels
	b := support.OpsLaunch(t, o)
	p := aclLogin(clientID(t, ""), "feat")
	p.Props = support.Durable
	c, ack := harness.Connected(t, b.Addr, harness.V5, p)
	for _, id := range []byte{harness.RetainAvailable, harness.SharedSubscriptionAvailable, harness.MaximumQoS} {
		if withholds(ack, id) {
			t.Fatalf("before the reload, CONNACK withholds property 0x%02X's feature: %+v; want everything allowed", id, ack.Props)
		}
	}

	testkit.WriteFile(t, o.Dir, "acl.yaml", featuresACL(true))
	reload(t, b)
	retained := retain("feat/r", []byte("r"))
	support.Sent(t, c, retained)
	support.PubackOf(t, c, retained, 0x87)
	exactly := &harness.Publish{QoS: 2, Topic: "feat/q", Payload: []byte("q")}
	support.Sent(t, c, exactly)
	support.AnswerOf(t, c, harness.PUBREC, exactly, 0x87)
	support.Suback(t, c, nil, support.At(1, "$share/g/feat/#"), 0x87)
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "feat/plain", Payload: []byte("p")})
	if err := c.Ping(); err != nil {
		t.Fatalf("after the features the reload denied: %v; want the connection to stay", err)
	}
	support.Away(t, c)

	p.CleanStart = false
	_, ack = harness.Connected(t, b.Addr, harness.V5, p)
	if !ack.SessionPresent {
		t.Error("the reconnect with Clean Start 0 has Session Present 0; want 1: persistent applies from the next connection, so the session outlived the one it was made on")
	}
	for _, w := range []struct {
		id   byte
		name string
		want uint32
	}{
		{harness.SessionExpiryInterval, "Session Expiry Interval", 0},
		{harness.MaximumQoS, "Maximum QoS", 1},
		{harness.RetainAvailable, "Retain Available", 0},
		{harness.SharedSubscriptionAvailable, "Shared Subscription Available", 0},
	} {
		if v, ok := connackSays(ack, w.id); !ok || v != w.want {
			t.Errorf("the next CONNACK's %s is %d (present %v); want %d, reflecting the denial", w.name, v, ok, w.want)
		}
	}
}

// design.md, Access and reload: "It reloads every TLS certificate, key,
// client CA and bridge ca_file for new connections"; "Each TLS handshake takes the current
// certificate and client CA. Every reload makes new session-ticket keys";
// "Each step is all-or-nothing. An invalid file is logged and the old one
// kept"; config.md, tls: require_certificate false, "a client may present
// no certificate, but a certificate it does present must verify, or the
// handshake fails"; broker.operations, listen: tls "as above".
//
// An MQTT listener and an operations listener share one tls block: the
// test's server certificate, its first authority's, and a client CA file
// holding that authority, certificates not required. Every client trusts
// both the first authority and a second one. A client's second handshake
// resumes the session its first was given; a client presenting a
// certificate of the first authority connects; another stays connected.
// The certificate, key and client CA files are then the second authority's
// server certificate, its key and that authority, and the broker gets
// SIGUSR1. A new handshake with the session the first two shared resumes
// nothing, and both listeners present the second authority's certificate;
// a client presenting the second authority's certificate connects, one
// presenting the first's is refused at the handshake; the client connected
// before still answers PINGREQ. The files then hold a third certificate of
// the second authority with another certificate's key, and a client CA
// file of the first authority, and the broker gets SIGUSR1, its tls step
// refused: both listeners still present the second authority's
// certificate, and the second authority's client certificate still
// connects while the first's is still refused.
func TestReloadTakesNewTLSMaterial(t *testing.T) {
	certs := newPKI(t)
	clientCA := testkit.WriteFile(t, certs.dir, "clients.pem", string(certs.ca.PEM))
	block := certs.listener(&harness.TLS{ClientCAFile: clientCA, RequireCertificate: "false"})
	b := support.OpsLaunch(t, &harness.Options{Dir: t.TempDir(), ListenerTLS: []harness.TLS{block},
		OpsListeners: []harness.OpsListener{{TLS: &block}}})
	ca2, err := testkit.NewAuthority("Sagüin second test authority")
	if err != nil {
		t.Fatal(err)
	}
	loopback := []net.IP{net.IPv4(127, 0, 0, 1)}
	server2 := issue(t, ca2, &testkit.Leaf{CommonName: "saguin-2", IPAddresses: loopback, Server: true})
	oldDevice := issue(t, certs.ca, &testkit.Leaf{CommonName: "old-device"})
	newDevice := issue(t, ca2, &testkit.Leaf{CommonName: "new-device"})
	roots := x509.NewCertPool()
	roots.AddCert(certs.ca.Cert)
	roots.AddCert(ca2.Cert)
	// trusting is a client's configuration, trusting both authorities,
	// presenting cert where it is not nil, keeping its sessions in cache
	// where that is not nil.
	trusting := func(cert *testkit.Issued, cache tls.ClientSessionCache) *tls.Config {
		config := certs.client(cert)
		config.RootCAs, config.ClientSessionCache = roots, cache
		return config
	}
	// connected is a client connected over TLS with config, CONNACK 0x00.
	connected := func(name string, config *tls.Config) *harness.Client {
		t.Helper()
		c := dialTLS(t, b.Addr, harness.V5, config)
		harness.ConnectedOver(t, c, &harness.Connect{ClientID: clientID(t, "-"+name), CleanStart: true})
		return c
	}
	// opsPresents fails the test unless the operations listener presents
	// want.
	opsPresents := func(want *testkit.Issued) {
		t.Helper()
		conn, err := harness.OpsTarget{Network: "tcp", Address: b.Ops[0], TLS: trusting(nil, nil)}.Dial()
		if err != nil {
			t.Fatalf("the TLS handshake with the operations listener: %v", err)
		}
		defer func() { _ = conn.Close() }() // only its handshake is checked
		presented(t, conn, want)
	}
	cache := tls.NewLRUClientSessionCache(1)
	_ = connected("first", trusting(nil, cache)).Close() // the session it was given is in cache
	if !connected("resumed", trusting(nil, cache)).Conn.(*tls.Conn).ConnectionState().DidResume {
		t.Fatal("before the reload, a second handshake did not resume the session the first was given; the test cannot see new session-ticket keys")
	}
	connected("old-device", trusting(oldDevice, nil))
	open := connected("open", trusting(nil, nil))

	testkit.WriteFile(t, certs.dir, "server.crt", string(server2.CertPEM))
	testkit.WriteFile(t, certs.dir, "server.key", string(server2.KeyPEM))
	testkit.WriteFile(t, certs.dir, "clients.pem", string(ca2.PEM))
	reload(t, b)
	after := connected("after", trusting(nil, cache))
	if after.Conn.(*tls.Conn).ConnectionState().DidResume {
		t.Error("after the reload, a handshake resumed a session given before it; want new session-ticket keys, which no earlier ticket opens")
	}
	presented(t, after.Conn, server2)
	opsPresents(server2)
	connected("new-device", trusting(newDevice, nil))
	handshakeRefused(t, b.Addr, trusting(oldDevice, nil), "after the reload, a client presenting the first authority's certificate")
	if err := open.Ping(); err != nil {
		t.Errorf("the connection open across the reload: %v; want it to stay", err)
	}

	third := issue(t, ca2, &testkit.Leaf{CommonName: "saguin-3", IPAddresses: loopback, Server: true})
	other := issue(t, ca2, &testkit.Leaf{CommonName: "other", Server: true})
	testkit.WriteFile(t, certs.dir, "server.crt", string(third.CertPEM))
	testkit.WriteFile(t, certs.dir, "server.key", string(other.KeyPEM))
	testkit.WriteFile(t, certs.dir, "clients.pem", string(certs.ca.PEM))
	reload(t, b, "tls")
	presented(t, connected("kept", trusting(nil, nil)).Conn, server2)
	opsPresents(server2)
	connected("new-device-kept", trusting(newDevice, nil))
	handshakeRefused(t, b.Addr, trusting(oldDevice, nil), "after a refused reload, a client presenting the first authority's certificate")
}

// askedOn sends a GET of path on conn, an open HTTP connection to b's
// operations listener read through r, with user's HTTP Basic credentials
// (RFC 7617), and returns the answer's status, failing the test where no
// answer comes within harness.DefaultTimeout or the listener closes the
// connection after it.
func askedOn(t *testing.T, b *harness.Broker, conn net.Conn, r *bufio.Reader, path, user, password string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+b.OpsAddr+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, password)
	if err := conn.SetDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
		t.Fatal(err)
	}
	if err := req.Write(conn); err != nil {
		t.Fatalf("GET %s as %s: %v", path, user, err)
	}
	resp, err := http.ReadResponse(r, req)
	if err != nil {
		t.Fatalf("GET %s as %s: %v", path, user, err)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err = errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatalf("GET %s as %s: %v", path, user, err)
	}
	if resp.Close {
		t.Fatalf("GET %s as %s: %s, and the listener closed the connection; want it kept open", path, user, resp.Status)
	}
	return resp.StatusCode
}

// design.md, Access and reload: "Each operations request is checked
// against the operators' password file current when the request arrives.
// So a removed operator, or a narrower scope, is refused from the next
// request, even on an open HTTP connection"; Who may call: "A rejected
// credential gets 401 with WWW-Authenticate. A good credential outside its
// scope gets 403"; "Without that field, the user reaches every route".
//
// The operators' file holds root, reaching every route, and auditor,
// scoped to /v1/operations. On two open HTTP connections to the operations
// listener, root and auditor each GET /v1/operations/acl?user=alice: 200.
// The file then holds auditor scoped to /metrics, and newbie, reaching
// every route; root is gone. The broker gets SIGUSR1. On the same
// connections, root's next GET of that route gets 401, and auditor's 403;
// auditor's GET of /metrics gets 200. newbie's GET of the route gets 200.
func TestReloadChecksEachOperationsRequestAgainstTheCurrentFile(t *testing.T) {
	dir := t.TempDir()
	line := func(user, scope string) string { return user + ":" + sixHash(user+"-pw", salt12) + scope }
	file := passwordFile(t, dir, "operators", line("root", ""), line("auditor", ":/v1/operations"))
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, OpsPasswordFile: file})
	const route = "/v1/operations/acl?user=alice"
	type open struct {
		conn net.Conn
		r    *bufio.Reader
	}
	opened := make(map[string]open, 2)
	for _, user := range []string{"root", "auditor"} {
		conn := opsDial(t, support.PlainOps(b))
		opened[user] = open{conn, bufio.NewReader(conn)}
		if status := askedOn(t, b, conn, opened[user].r, route, user, user+"-pw"); status != http.StatusOK {
			t.Fatalf("before the reload, %s's GET %s: %d; want 200", user, route, status)
		}
	}

	passwordFile(t, dir, "operators", line("auditor", ":/metrics"), line("newbie", ""))
	reload(t, b)
	for _, c := range []struct {
		user, path string
		want       int
	}{{"root", route, http.StatusUnauthorized}, {"auditor", route, http.StatusForbidden}, {"auditor", "/metrics", http.StatusOK}} {
		o := opened[c.user]
		if status := askedOn(t, b, o.conn, o.r, c.path, c.user, c.user+"-pw"); status != c.want {
			t.Errorf("after the reload, %s's GET %s on the connection open across it: %d; want %d", c.user, c.path, status, c.want)
		}
	}
	if a := opsCall(t, support.PlainOps(b), get(route, "newbie", "newbie-pw")); a.Status != http.StatusOK {
		t.Errorf("after the reload, newbie's GET %s: %d %q; want 200", route, a.Status, a.Body)
	}
}

// ratedACL is the acl_file of TestReloadChangesPublishLimitsOnOpenConnections:
// rated may do everything (aclFree), at a publish_rate of rate, or with
// no limit when rate is zero.
func ratedACL(rate int) string {
	if rate == 0 {
		return "roles:\n" + aclFree + "users:\n  rated: [free]\n"
	}
	return "roles:\n" + aclFree + fmt.Sprintf("users:\n  rated: {roles: [free], limits: {publish_rate: %d}}\n", rate)
}

// The waits TestReloadChangesPublishLimitsOnOpenConnections holds the
// broker to, each below the least the design allows, so that a client
// measuring from its own writes, before the broker's receipt, never sees
// less. Clamped: a bucket of 2 messages, refilling 2 a second, reads a
// third message half a second after the second. Kept: a bucket of 2,
// refilling 2 a second, spent at once, reads its next message no sooner
// than half a second later, and the three after it at 4 a second, a
// second in all, however the reload falls between them.
const (
	clampedWait = 400 * time.Millisecond
	keptWait    = 900 * time.Millisecond
)

// design.md, Flows, PUBLISH, Rate limits: "A reload that changes a
// connection's limits applies from its next publish. The buckets keep what
// was spent or owed, refill at the old rate up to that publish, and are then
// clamped to the new capacity. A reload never fills them"; "A connection
// that had no limit on a unit before the reload starts that unit's bucket
// full, as at CONNECT"; "The reader
// counts them in buckets holding one second's worth, full at CONNECT";
// "When a charge leaves the message bucket below one message [...] the
// reader reads nothing more from its connection until the message bucket
// holds one message again"; "Nothing is refused or lost"; Access and
// reload: "A connection's publish limits follow PUBLISH, Rate limits".
//
// Each case is an MQTT 5 client of rated's, its entry's publish_rate the
// first rate, connected before the acl_file sets the second and the broker
// gets SIGUSR1. Each PUBLISH is at QoS 1, those of one write sent before
// any answer, and every PUBACK is 0x00:
//   - clamped, 100 then 2: after the reload, three PUBLISHes in one write:
//     the third's PUBACK comes at least clampedWait after they were
//     written, the full bucket of 100 clamped to 2, where a bucket of 100
//     would take them at once;
//   - kept, 2 then 4: before the reload, two PUBLISHes in one write spend
//     the bucket; after it, four in one write: the last PUBACK comes at
//     least keptWait after the first write, the bucket keeping what was
//     spent, where one filled by the reload would take the four at once;
//   - newly limited, none then 2: eight PUBLISHes before the reload; after
//     it, four in one write: a full bucket answers the last after one
//     second. The bounds are 900 ms to 1.4 s: 100 ms below the required
//     wait and 400 ms for receipt, storage and acknowledgement, less than
//     the half second a missing token would add. This row measures from
//     the first write after the reload, excluding the reload's own time.
func TestReloadChangesPublishLimitsOnOpenConnections(t *testing.T) {
	for _, c := range []struct {
		name        string
		from, to    int
		spent, sent int
		least, most time.Duration // most zero means no upper bound
		why         string
	}{
		{"clamped", 100, 2, 0, 3, clampedWait, 0, "the full bucket clamped to the new rate's 2"},
		{"kept", 2, 4, 2, 4, keptWait, 0, "the spent bucket kept, never filled"},
		{"newly limited", 0, 2, 8, 4, keptWait, time.Second + clampedWait, "a previously unlimited bucket starts full"},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := aclOptions(t, ratedACL(c.from), "rated")
			o.Channels = aclChannels
			b := support.OpsLaunch(t, o)
			cl, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, ""), "rated"))
			allAccepted := func(got []byte) bool { return !slices.ContainsFunc(got, func(r byte) bool { return r != 0 }) }
			at := time.Now()
			if c.spent > 0 {
				if got := ackReasons(t, cl, c.spent, "rated/spent"); !allAccepted(got) {
					t.Fatalf("PUBLISHes before the reload: PUBACKs % X; want 0x00 each, nothing refused", got)
				}
			}
			testkit.WriteFile(t, o.Dir, "acl.yaml", ratedACL(c.to))
			reload(t, b)
			if c.spent == 0 || c.from == 0 {
				at = time.Now()
			}
			got := ackReasons(t, cl, c.sent, "rated/after")
			took := time.Since(at)
			if !allAccepted(got) {
				t.Errorf("%d PUBLISHes after the reload: PUBACKs % X; want 0x00 each, nothing refused", c.sent, got)
			}
			if c.most != 0 {
				support.Between(t, "the last PUBACK after the first write, "+c.why, took, c.least, c.most)
			} else if took < c.least {
				t.Errorf("the PUBLISHes were all answered within %v of the first write; want at least %v, %s", took, c.least, c.why)
			}
		})
	}
}

// design.md, Access and reload: "A SIGUSR1 that arrives while the broker is
// starting is carried out once startup has finished. One that arrives
// after stopping has begun is ignored"; "A step applied is one INFO line
// naming it as that series' part does. A step refused is one ERROR line
// naming the file, the entry and what is wrong"; Restart, Graceful stop: "Every
// client gets 0x8B"; "/health answers 503 until exit"; "The exit status is
// 0 after a stop requested by SIGINT or SIGTERM".
//
// While starting: a SIGUSR1 sent while the broker reads its configuration
// (readingConfig) is carried out once it serves: /health answers 200 and
// /metrics counts each step applied once. While stopping: an MQTT 5
// client, connected, holds the graceful stop open until write_timeout;
// SIGTERM gives it DISCONNECT 0x8B, so the stop has begun, and the broker
// then gets SIGUSR1. Until the broker exits, /metrics counts no step, and
// it exits 0. Its complete log then holds no applied or refused reload
// line, even if /metrics became unavailable before the first scrape.
func TestReloadWaitsForStartAndIsIgnoredOnceStopping(t *testing.T) {
	t.Run("while starting", func(t *testing.T) {
		b, feed := readingConfig(t, &harness.Options{Operations: true})
		if err := b.Signal(syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
		if err := feed(); err != nil {
			t.Fatalf("SIGUSR1, sent while the broker read its configuration: writing the configuration: %v; the broker ended %v", err, support.Exited(t, b))
		}
		if err := b.Listening(); err != nil {
			t.Fatalf("SIGUSR1, sent while the broker read its configuration: %v", err)
		}
		support.AwaitReady(t, b)
		want := make(map[string]float64, 2*len(reloadParts))
		for _, part := range reloadParts {
			want[reloadSeries(part, "applied")], want[reloadSeries(part, "refused")] = 1, 0
		}
		awaitSeries(t, b, want)
	})
	t.Run("while stopping", func(t *testing.T) {
		b := support.OpsLaunch(t, &harness.Options{})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		if err := b.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8B {
			t.Fatalf("SIGTERM: DISCONNECT 0x%02X; want 0x8B, the stop begun", d.Reason)
		}
		if err := b.Signal(syscall.SIGUSR1); err != nil {
			t.Fatalf("SIGUSR1, once the stop had begun: %v", err)
		}
		var counted string
		testkit.Within(support.StopLimit, func() bool {
			var page strings.Builder
			if _, err := b.Metrics().WriteTo(&page); err != nil {
				return true // the broker has ended
			}
			e, err := testkit.ParseExposition(page.String())
			if err != nil {
				return false
			}
			for _, part := range reloadParts {
				for _, result := range []string{"applied", "refused"} {
					if v := e.Values[reloadSeries(part, result)]; v != 0 {
						counted = fmt.Sprintf("%s = %v", reloadSeries(part, result), v)
						return true
					}
				}
			}
			return false
		})
		if counted != "" {
			t.Errorf("SIGUSR1, sent once the stop had begun: /metrics counts %s; want it ignored", counted)
		}
		_ = c.Close() // the stop needs it no longer
		if state := support.Exited(t, b); !state.Success() {
			t.Errorf("SIGUSR1, sent once the stop had begun: the broker ended %v; want exit status 0", state)
		}
		// An applied step's INFO line names its part; a refused step's
		// ERROR line names the file, here the configuration, the broker's
		// one file. The start's INFO line names that file too, so the
		// file names refused steps alone.
		for level, names := range map[string][]string{"INFO": reloadParts, "ERROR": {b.Config}} {
			if lines := testkit.LinesNaming(b.Log(), level, names...); len(lines) != 0 {
				t.Errorf("SIGUSR1, sent once the stop had begun: the complete log holds reload lines at %s: %q; want it ignored", level, lines)
			}
		}
	})
}
