package conformance

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of what the broker logs of its own life (design.md, Flows,
// Logging, "What the broker logs of its own life"). A line is judged by its
// level, how many times it comes and the fields the design names, never by
// its wording.

// awaitLogged waits until lines, given b's log, returns at least n lines,
// and returns them, failing the test where it has not within
// harness.DefaultTimeout, the watchdog; what names the event logged.
func awaitLogged(t *testing.T, b *harness.Broker, n int, what string, lines func(log string) []string) []string {
	t.Helper()
	var got []string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = lines(b.Log())
		return len(got) >= n
	}) {
		t.Fatalf("watchdog: %v after %s, its lines are %q; want %d; the log:\n%s", harness.DefaultTimeout, what, got, n, b.Log())
	}
	return got
}

// at returns a function giving the lines of a log at level, for
// awaitLogged.
func at(level string) func(log string) []string {
	return func(log string) []string { return testkit.LogLines(log, level) }
}

// infosAfter returns the INFO lines of log written after the line first:
// the room come back after the WARN line of its lack.
func infosAfter(log, first string) []string {
	_, rest, _ := strings.Cut(log, first+"\n")
	return testkit.LogLines(rest, "INFO")
}

// design.md, Logging, What the broker logs of its own life: "A refused
// credential: one WARN line for each CONNECT refused with 0x86, naming the
// listener, the client id, the user name and the remote address"; CONNECT's
// refusal table: 0x86, "a wrong or missing credential, or a client id the
// acl_file refuses"; Access and reload: "Any other client id is refused at
// CONNECT with 0x86". config.md, A listener: two tcp listeners are named,
// here tcp1 and tcp2.
//
// On listener tcp2, alice's CONNECT with a wrong password and bob's with a
// client id his acl_file entry does not allow are each refused 0x86. Once
// the broker has stopped, its log holds one WARN line naming each client
// id, which also names tcp2, the user name and the client's own address.
func TestRefusedCredentialIsLogged(t *testing.T) {
	acl := "roles:\n" + aclFree + `users:
  alice: [free]
  bob:
    roles: [free]
    client_ids: "bob-*"
`
	o := aclOptions(t, acl, "alice", "bob")
	o.Channels = aclChannels
	o.TCPListeners = 2
	b := support.Launch(t, o)
	refusals := []struct {
		user    string
		connect *harness.Connect
	}{
		{"alice", credentials(clientID(t, "-wrong-password"), "alice", "not-alice-pw")},
		{"bob", aclLogin(clientID(t, "-stranger"), "bob")},
	}
	remotes := make([]string, len(refusals))
	for i, r := range refusals {
		c, err := harness.Dial(b.Addrs[1], harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		remotes[i] = c.Conn.LocalAddr().String()
		connackRefusesOver(t, c, r.connect, 0x86)
	}
	stopped(t, b)
	log := b.Log()
	for i, r := range refusals {
		names := []string{"tcp2", r.connect.ClientID, r.user, remotes[i]}
		if lines := testkit.LinesNaming(log, "WARN", r.connect.ClientID); len(lines) != 1 || !namesAll(lines[0], names...) {
			t.Errorf("WARN lines naming %q: %q; want one, naming each of %q; the log:\n%s", r.connect.ClientID, lines, names, log)
		}
	}
}

// design.md, Logging, What the broker logs of its own life: "An operator's
// credential refused at the operations endpoint, a 401: one WARN line
// naming the route and the remote address"; "An operator's credential
// outside its scope at the operations endpoint, a 403: one WARN line saying
// the credential is not allowed that route, naming the user, the route and
// the remote address"; Operations, Who may call: "A rejected credential
// gets 401 with WWW-Authenticate. A good credential outside its scope gets
// 403".
//
// The operators' file (operatorsFile) holds root, reaching every route, and
// prom, scoped to /metrics. On an HTTP connection of its own, prom's GET of
// /v1/operations/sessions, with its right password, gets 403; on another,
// root's GET of /metrics with a wrong password gets 401. Once the broker has
// stopped, its log holds one WARN line naming each route: the 403's names
// prom and its connection's address too, and says the credential is "not
// allowed", never that it is "refused"; the 401's names its own
// connection's address, and says the credential is "refused", never that it
// is "not allowed" (operatorLineSays). A line is otherwise judged by its
// fields, but these two are told apart by what the design says each says.
func TestOperatorOutsideItsScopeIsLogged(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t)})
	refusals := []struct {
		user, password, route string
		status                int
		says, never           string
		remote                string
	}{
		{user: "prom", password: "prom-pw", route: "/v1/operations/sessions", status: http.StatusForbidden, says: "not allowed", never: "refused"},
		{user: "root", password: "not-root-pw", route: "/metrics", status: http.StatusUnauthorized, says: "refused", never: "not allowed"},
	}
	for i := range refusals {
		r := &refusals[i]
		conn := opsDial(t, support.PlainOps(b))
		r.remote = conn.LocalAddr().String()
		if status := askedOn(t, b, conn, bufio.NewReader(conn), r.route, r.user, r.password); status != r.status {
			t.Fatalf("%s's GET %s: %d; want %d", r.user, r.route, status, r.status)
		}
	}
	stopped(t, b)
	log := b.Log()
	for _, r := range refusals {
		names := []string{r.route, r.remote}
		if r.status == http.StatusForbidden {
			names = append(names, r.user)
		}
		lines := testkit.LinesNaming(log, "WARN", r.route)
		if len(lines) != 1 || !namesAll(lines[0], names...) {
			t.Fatalf("WARN lines naming %s, refused %d: %q; want one, naming each of %q; the log:\n%s", r.route, r.status, lines, names, log)
		}
		if !operatorLineSays(lines[0], r.says, r.never) {
			t.Errorf("the %d's WARN line %q; want it to say %q, never %q", r.status, lines[0], r.says, r.never)
		}
	}
}

// operatorLineSays reports whether line, an operations listener's WARN
// line, says says and never says never, case aside.
func operatorLineSays(line, says, never string) bool {
	line = strings.ToLower(line)
	return strings.Contains(line, says) && !strings.Contains(line, never)
}

// design.md, Logging, What the broker logs of its own life: "A full disk:
// one WARN line when the store first refuses for lack of room, and one INFO
// line when it has room again"; Store, A full disk: "a write is answered
// "full" only when SQLite reports lack of space"; "Publishes are refused
// with 0x97".
//
// On a filesystem of its own, a ballast takes every byte left. Three
// publications of spilling bytes at QoS 1 are each answered PUBACK 0x97.
// Once the ballast is emptied, two more are answered 0x00. The log holds
// one WARN line, after the first refusal, and one INFO line after it, once
// the first publication is saved.
func TestFullDiskIsLoggedOnceEachWay(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, spillingDiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), MaxPacketSize: 2 * spilling})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	answered(t, pub, support.Noisy(t, "full/refused/1", spilling), 0x97)
	warned := awaitLogged(t, b, 1, "the store first refused for lack of room", at("WARN"))[0]
	answered(t, pub, support.Noisy(t, "full/refused/2", spilling), 0x97)
	answered(t, pub, support.Noisy(t, "full/refused/3", spilling), 0x97)

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.Acknowledged(t, pub, support.Noisy(t, "full/stored/1", spilling))
	awaitLogged(t, b, 1, "the store had room again", func(log string) []string { return infosAfter(log, warned) })
	support.Acknowledged(t, pub, support.Noisy(t, "full/stored/2", spilling))

	log := b.Log()
	if lines := testkit.LogLines(log, "WARN"); len(lines) != 1 {
		t.Errorf("WARN lines: %q; want one, the store's first refusal for room, none for the two refused after it; the log:\n%s", lines, log)
	}
	if lines := infosAfter(log, warned); len(lines) != 1 {
		t.Errorf("INFO lines after the full disk's WARN line: %q; want one, the room come back; the log:\n%s", lines, log)
	}
}

// loggedAt is when line, a line of a broker's log, says its event happened:
// design.md, Logging, "It contains the time the event happened, in UTC with
// milliseconds (2026-10-10T09:00:00.123Z)", its first field.
func loggedAt(t *testing.T, line string) time.Time {
	t.Helper()
	field, _, _ := strings.Cut(line, " ")
	at, err := time.Parse(time.RFC3339, field)
	if err != nil {
		t.Fatalf("the log line %q: %v", line, err)
	}
	return at
}

// overloadBudget drives one message-memory budget from the wire for
// TestOverloadEpisodesAreLoggedOnce: options are the broker's; full and
// other are the budget's saguin_memory_full_seconds_total and the other
// budget's; hold readies a broker, launched with options, for waits; wait
// has request n of the budget wait for room, returning release, which gives
// the room back and returns when it began to; and waits is how many waits
// the test makes, one after another. ahead says that wait n+1 is readied
// while wait n still waits, so that its requests wait behind wait n's and
// go on waiting as soon as wait n's room is given, by the broker's own work:
// readying it takes the test longer than the second an episode allows
// between waits, on a slow machine or a slow disk.
type overloadBudget struct {
	name        string
	options     harness.Options
	full, other string
	hold        func(t *testing.T, b *harness.Broker)
	wait        func(t *testing.T, b *harness.Broker, n int) (release func() time.Time)
	waits       int
	ahead       bool
}

// overloadBudgets are the two budgets as the overload tests drive them:
//   - incoming: with max_packet_size maxPacket and max_incoming_memory
//     incomingHeld, as in TestIncomingMemoryHoldsReaders, client A has half
//     a PUBLISH of maxPacket bytes read throughout (hold). Each wait has a
//     client of its own write half of another such PUBLISH, holding with
//     A's all of the budget but 11 bytes, and another client publish a body
//     of 12 at QoS 1 behind a PINGREQ: its PINGRESP says the reader has
//     read on to the PUBLISH, whose body then waits for room, given askTaken
//     to ask (no client sees the ask itself). Its release sends the rest of
//     the half-sent PUBLISH, and the two PUBLISHes get PUBACK 0x00.
//   - outgoing: with max_outgoing_memory stallBudget, each wait is a
//     subscriber that reads nothing holding the outgoing budget, the rest
//     of its deliveries waiting for room (stallOutgoing); its release closes
//     it, its writer discarding what it holds. Each wait stores 10 MiB, so
//     it makes fewer, and each is readied ahead: the next wait's subscriber
//     is owed its 10 MiB, its deliveries waiting for room behind this
//     wait's, before this wait's room is given back.
var overloadBudgets = []overloadBudget{
	{
		name:    "incoming",
		options: harness.Options{MaxIncomingMemory: incomingHeld, MaxPacketSize: maxPacket},
		full:    fullIncoming, other: fullOutgoing, waits: 10,
		hold: func(t *testing.T, b *harness.Broker) {
			t.Helper()
			halfSent(t, b, "-a")
		},
		wait: func(t *testing.T, b *harness.Broker, n int) func() time.Time {
			t.Helper()
			h, rest, id := halfSent(t, b, fmt.Sprintf("-h%d", n))
			s, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, fmt.Sprintf("-s%d", n)), CleanStart: true})
			small := &harness.Publish{QoS: 1, ID: s.NextID(), Topic: "small", Payload: []byte("xx")}
			harness.SendBytes(t, s, append(slices.Clone(pingreq), harness.Encoded(t, small, harness.V5)...))
			harness.Expect[*harness.Pingresp](t, s)
			time.Sleep(askTaken)
			return func() time.Time {
				t.Helper()
				began := time.Now()
				harness.SendBytes(t, h, rest)
				if ack, err := h.AwaitAck(harness.PUBACK, id); err != nil || ack.Reason != 0 {
					t.Fatalf("the half-sent PUBLISH %d, finished: %+v, %v; want PUBACK 0x00", n, ack, err)
				}
				support.Puback(t, s, small)
				return began
			}
		},
	},
	{
		name:    "outgoing",
		options: harness.Options{MaxOutgoingMemory: stallBudget},
		full:    fullOutgoing, other: fullIncoming, waits: 4, ahead: true,
		hold: func(*testing.T, *harness.Broker) {},
		wait: func(t *testing.T, b *harness.Broker, n int) func() time.Time {
			t.Helper()
			stalled, _ := stallOutgoing(t, b, strconv.Itoa(n))
			return func() time.Time {
				t.Helper()
				began := time.Now()
				if err := stalled.Close(); err != nil {
					t.Fatal(err)
				}
				return began
			}
		},
	},
}

// askTaken is how long a test gives a reader, its PINGRESP read, to have
// asked for the body whose fixed header follows the PINGREQ in the same
// write, where the ask is not granted: no client sees the ask itself, and
// a waiting ask moves no series of metrics.md until its wait ends.
const askTaken = 100 * time.Millisecond

// design.md, Logging, What the broker logs of its own life: "Overload: for
// each budget, one WARN line when an overload episode begins and one INFO
// line when it ends (Message memory)"; "The stop: one INFO line when a
// graceful stop begins, naming what asked for it, and one when it is done";
// Message memory, Full, and an overload episode: "A budget is full from the
// event at which its eligible FIFO head does not fit to the first event at
// which that head fits or no head remains"; "Each budget's full time is
// counted on its own"; "An overload episode begins when a budget first
// becomes full. It ends once no request of that budget has waited for room
// for one uninterrupted second, counted after the core's grants for an
// event. When the budget is full again within that second, the episode
// goes on, and the second counts again from the next time it has room";
// "A stop ends an episode without its end being logged. Each budget has its
// own episodes"; "Example: under a steady overload, publishers' bodies wait
// one after another, each granted a moment later. The incoming budget logs
// one WARN line at the first wait. Its INFO line comes one second after the
// last wait, not after each grant." metrics.md,
// saguin_memory_full_seconds_total{direction}: "each budget's full time
// [...] Includes completed intervals only".
//
// For each budget (overloadBudgets), each case on a broker of its own:
//   - waits: waits requests wait for room one after another, each given
//     room before the next waits: the first wait's WARN line is logged
//     while it waits, and each wait is seen to have happened by its
//     budget's full time rising once its room is given back. Each wait
//     begins less than a second after the room before it, as the test
//     bounds from outside: from when it began to give that room, to when it
//     read the full time risen by the next wait's interval, less that
//     interval; a wait that began later breaks the test's premise, the
//     episode having rightly ended. The broker logs one INFO line after the
//     WARN line, no sooner than a second after the last wait's room began
//     to be given; then its log holds that WARN line alone and that INFO
//     line alone after it, and the other budget's full time is still 0.
//   - a stop: write_timeout 5 s. A request waits for room and the WARN line
//     is logged; the room is given back and the broker at once gets
//     SIGTERM, its stop beginning less than a second after that room, and
//     lasting longer while its clients keep their connections open. It
//     exits 0, and its log holds two INFO lines after the WARN line, the
//     stop's own, none for the episode the stop ended.
func TestOverloadEpisodesAreLoggedOnce(t *testing.T) {
	for _, budget := range overloadBudgets {
		t.Run(budget.name+"/waits", func(t *testing.T) {
			o := budget.options
			o.WriteTimeout = stallFor
			b := support.OpsLaunch(t, &o)
			budget.hold(t, b)
			var warned string
			var released time.Time       // when the last wait's room began to be given
			var readied func() time.Time // the next wait's release, where it is readied ahead
			for n := range budget.waits {
				before := support.Sampled(t, b, budget.full)[budget.full]
				release := readied
				if release == nil {
					release = budget.wait(t, b, n)
				}
				if n == 0 {
					warned = awaitLogged(t, b, 1, "the "+budget.name+" budget's first wait", at("WARN"))[0]
				}
				if readied = nil; budget.ahead && n+1 < budget.waits {
					readied = budget.wait(t, b, n+1)
				}
				began := release()
				after := awaitAbove(t, b, budget.full, before, fmt.Sprintf("wait %d of the %s budget, its room given back", n+1, budget.name))
				if n != 0 {
					waited := time.Duration((after - before) * float64(time.Second))
					if gap := time.Since(released) - waited; gap >= time.Second {
						t.Fatalf("test premise broken: wait %d of the %s budget began as much as %v after the room before it; the test needs each within a second", n+1, budget.name, gap)
					}
				}
				released = began
			}
			ended := awaitLogged(t, b, 1, "the "+budget.name+" budget's last wait", func(log string) []string { return infosAfter(log, warned) })[0]
			if when, least := loggedAt(t, ended), released.Add(time.Second).Truncate(time.Millisecond); when.Before(least) {
				t.Errorf("the %s budget's INFO line %q came at %v, before %v: want it a second after the last wait's room, no sooner", budget.name, ended, when, least)
			}
			log := b.Log()
			if lines := testkit.LogLines(log, "WARN"); len(lines) != 1 {
				t.Errorf("WARN lines: %q; want one, the %s budget's first wait, none for the %d waits after it; the log:\n%s", lines, budget.name, budget.waits-1, log)
			}
			if lines := infosAfter(log, warned); len(lines) != 1 {
				t.Errorf("INFO lines after the %s budget's WARN line: %q; want one, the episode's end; the log:\n%s", budget.name, lines, log)
			}
			if got := support.Sampled(t, b, budget.other)[budget.other]; got != 0 {
				t.Errorf("%s reads %v; want 0, only the %s budget waited", budget.other, got, budget.name)
			}
		})
		t.Run(budget.name+"/a stop", func(t *testing.T) {
			o := budget.options
			o.WriteTimeout = 5 * time.Second
			b := support.OpsLaunch(t, &o)
			budget.hold(t, b)
			release := budget.wait(t, b, 0)
			warned := awaitLogged(t, b, 1, "the "+budget.name+" budget's wait", at("WARN"))[0]
			released := release()
			stopped(t, b)
			log := b.Log()
			lines := infosAfter(log, warned)
			if len(lines) != 0 && !loggedAt(t, lines[0]).Before(released.Add(time.Second).Truncate(time.Millisecond)) {
				t.Fatalf("test premise broken: the first INFO line after the %s budget's WARN line, %q, came a second or more after its room; the test needs the stop to begin sooner", budget.name, lines[0])
			}
			if len(lines) != 2 {
				t.Errorf("INFO lines after the %s budget's WARN line: %q; want two, the stop's beginning and its end, none for the episode the stop ended; the log:\n%s", budget.name, lines, log)
			}
		})
	}
}

// design.md, Logging, What the broker logs of its own life: "A TLS
// certificate near its end: at start and at each reload, one WARN line for
// each listener's or bridge's certificate that expires within 30 days, and
// one ERROR line for one already expired, each naming the certificate and
// its expiry"; Access and reload: SIGUSR1 reloads.
//
// Listener tcp1 serves a certificate that ends in 10 days, tcp2 one that
// ended a day ago, each in a file of its own. Once the broker is ready, its
// log holds one WARN line naming the first's file and one ERROR line naming
// the second's, each naming its end's date, in UTC; after a SIGUSR1 reload,
// two of each. Once the broker has stopped, there are still two of each,
// and no WARN line names the expired one's file.
func TestCertificateNearItsEndIsLogged(t *testing.T) {
	certs := newPKI(t)
	ends := []struct {
		level, name string
		ends        time.Duration // from the certificate's making
		end         time.Time     // as the certificate holds it, to the second
		cert, key   string
	}{
		{level: "WARN", name: "near", ends: 10 * 24 * time.Hour},
		{level: "ERROR", name: "expired", ends: -24 * time.Hour},
	}
	listeners := make([]harness.TLS, len(ends))
	for i := range ends {
		e := &ends[i]
		issued := issue(t, certs.ca, &testkit.Leaf{CommonName: "saguin-" + e.name, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true, Ends: e.ends})
		e.end = issued.Certificate.Leaf.NotAfter
		e.cert = testkit.WriteFile(t, certs.dir, e.name+".crt", string(issued.CertPEM))
		e.key = testkit.WriteFile(t, certs.dir, e.name+".key", string(issued.KeyPEM))
		listeners[i] = harness.TLS{CertFile: e.cert, KeyFile: e.key}
	}
	b := support.OpsLaunch(t, &harness.Options{Dir: t.TempDir(), TCPListeners: len(listeners), ListenerTLS: listeners})
	for _, e := range ends {
		awaitLogged(t, b, 1, "the start, with "+e.name+" certificate "+e.cert, func(log string) []string { return testkit.LinesNaming(log, e.level, e.cert) })
	}
	reload(t, b)
	for _, e := range ends {
		awaitLogged(t, b, 2, "a reload, with "+e.name+" certificate "+e.cert, func(log string) []string { return testkit.LinesNaming(log, e.level, e.cert) })
	}
	stopped(t, b)
	log := b.Log()
	for _, e := range ends {
		date := e.end.UTC().Format(time.DateOnly)
		lines := testkit.LinesNaming(log, e.level, e.cert)
		if len(lines) != 2 {
			t.Errorf("%s lines naming the %s certificate %s: %q; want two, the start's and the reload's; the log:\n%s", e.level, e.name, e.cert, lines, log)
		}
		for _, line := range lines {
			if !strings.Contains(line, date) {
				t.Errorf("%s line %q names the %s certificate but not its expiry, %s", e.level, line, e.name, date)
			}
		}
	}
	if lines := testkit.LinesNaming(log, "WARN", ends[1].cert); len(lines) != 0 {
		t.Errorf("WARN lines naming the expired certificate %s: %q; want none, its lines being ERROR", ends[1].cert, lines)
	}
}
