package broker_test

// A bridge driven against a real mosquitto, rather than against the
// substrate saguin itself runs on.
//
// **Everything else in this suite bridges saguin to mochi**, which is the
// same MQTT server saguin is built on. That proves the bridge talks to
// itself. The three figures the bridge's own defaults are argued from -
// mosquitto's 1000-record session-queue cut-off, its Receive Maximum
// backpressure, and the 5ms-against-50ms acknowledgement table in
// internal/config/bridge.go - were all measured against mosquitto 2.1.2 by
// hand, once, and nothing re-runs them.
//
// **The usual warning about mosquitto's command-line tools does not apply
// here.** `mosquitto_pub` and `mosquitto_sub` validate topics before they
// reach the socket, which is why this suite is driven by Paho - but that is
// about a *client* hiding what a broker does. Here mosquitto is the server
// and saguin is the client, so nothing is being validated away: what is
// under test is the packets saguin sends and what a foreign broker makes of
// them.
//
// **It is not in `make check`.** `make mosquitto` runs it, and it is gated
// on SAGUIN_MOSQUITTO for one reason: a test that quietly skipped wherever
// the binary was absent would be a green tick over work nobody did, and the
// CI runner has no mosquitto on it.
// So the ordinary suite skips it *saying so*, and the target runs it and
// **fails** when the binary is missing - a target invoked by name has
// already said what was wanted.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/bridge"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/config"
)

// wantMosquitto reports whether this run asked for a real broker, and says
// how to ask when it did not. The skip is loud on purpose: a silent one is
// indistinguishable from a pass.
func wantMosquitto(t *testing.T) {
	t.Helper()
	if os.Getenv("SAGUIN_MOSQUITTO") == "" {
		t.Skip("not run: this drives a real mosquitto, which the ordinary suite does " +
			"not install. `make mosquitto` runs it, or set SAGUIN_MOSQUITTO=1.")
	}
}

// lockedBuffer collects what a subprocess printed, safely enough to read
// while it is still running.
//
// Guarded because os/exec copies a process's output on a goroutine of its
// own whenever the destination is not a file, so a plain bytes.Buffer read
// from the test's goroutine is a data race - and everything here runs under
// -race.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// spoke renders what mosquitto printed, for a failure message that would
// otherwise be a timeout with no cause in it.
//
// **A broker that cannot start says why on stderr and writes no log file at
// all**, so the log-reading diagnostic below fails for the same reason as
// the thing it is trying to explain - and what reached the operator was ten
// seconds of silence and then "never accepted a connection". That is not
// hypothetical: the first run of this target on a machine whose OS confines
// mosquitto to its own directories printed "Error: Unable to open config
// file" and this test threw it away, so the report named neither the file
// nor the reason.
//
// Nothing here knows which OS or which confinement mechanism: it prints
// what the process said. That is what makes it the fix rather than a list
// of the platforms somebody has met so far.
func spoke(said *lockedBuffer) string {
	if s := strings.TrimSpace(said.String()); s != "" {
		return "\n\tmosquitto printed: " + s
	}
	return "\n\tmosquitto printed nothing at all, so it was stopped before it could say why"
}

// startMosquitto runs one, on a port nothing else has, and returns its
// address. It **fails** rather than skips when the binary is absent: by the
// time this is called the run has already said it wants a real broker.
func startMosquitto(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("mosquitto")
	if err != nil {
		t.Fatalf("mosquitto is not on PATH, and this test is the one that needs it: %v\n"+
			"\tInstall the broker itself - several distributions package it apart from "+
			"its clients - or do not ask for this target.", err)
	}

	// A port the kernel just handed out, released again immediately. The
	// window between is a race in principle; in practice a bind that loses
	// it makes mosquitto exit, which the wait below reports as exactly that
	// rather than as a hang.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	addr := probe.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	if err := probe.Close(); err != nil {
		t.Fatalf("release the probe port: %v", err)
	}

	dir := t.TempDir()
	conf := filepath.Join(dir, "mosquitto.conf")
	// persistence false so nothing outlives the test, and a log to a file so
	// a failure here can quote what mosquitto said rather than guessing.
	logFile := filepath.Join(dir, "mosquitto.log")
	body := fmt.Sprintf("listener %s 127.0.0.1\nallow_anonymous true\n"+
		"persistence false\nlog_dest file %s\nlog_type all\n", port, logFile)
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatalf("write the mosquitto configuration: %v", err)
	}

	var said lockedBuffer
	// Said once. The wait below reports what mosquitto printed when it
	// never binds, and the cleanup reports it when there is no log to quote
	// - which is the same failure reached twice, and a report that repeats
	// itself buries the line above it.
	reported := false
	cmd := exec.Command(bin, "-c", conf)
	cmd.Stdout, cmd.Stderr = &said, &said
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mosquitto: %v", err)
	}
	t.Cleanup(func() {
		// **Interrupt rather than kill**, and that is not tidiness. The log
		// below is the only window into a broker this test cannot otherwise
		// inspect, and mosquitto buffers it: killed outright it flushes
		// nothing, so the one run that needed the log printed "mosquitto
		// said:" and then nothing at all. Killing is kept as the fallback
		// for a broker that will not go.
		_ = cmd.Process.Signal(os.Interrupt)
		gone := make(chan struct{})
		go func() { defer close(gone); _, _ = cmd.Process.Wait() }()
		select {
		case <-gone:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-gone
		}
		if t.Failed() {
			also := func() string {
				if reported {
					return ""
				}
				return spoke(&said)
			}
			b, err := os.ReadFile(logFile)
			switch {
			case err != nil:
				t.Logf("mosquitto's log could not be read: %v.%s", err, also())
			case len(b) == 0:
				t.Logf("mosquitto's log is empty, so it wrote nothing before it "+
					"stopped.%s", also())
			default:
				t.Logf("mosquitto said:\n%s", b)
			}
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			// Version reported rather than assumed: the figures this test
			// exists to keep honest were taken against 2.1.2, and a run
			// against something else is still a run - it just has to say so.
			out, _ := exec.Command(bin, "-h").CombinedOutput()
			t.Logf("peer: %s at %s", versionLine(string(out)), addr)
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	reported = true
	t.Fatalf("mosquitto never accepted a connection on %s within 10s.%s", addr, spoke(&said))
	return ""
}

// reasonOf is the reason code a PUBACK carried, or 0xFF when there was no
// PUBACK at all - so a failure message can tell "the broker refused it" from
// "nothing came back", which are different problems with the same symptom.
func reasonOf(ack *paho.PublishResponse) byte {
	if ack == nil {
		return 0xFF
	}
	return ack.ReasonCode
}

// versionLine is mosquitto's version out of what `-h` prints.
//
// Not the first line, which is what this reached for first and got wrong:
// `mosquitto -h` starts a broker and stops it again, so its output opens
// with a timestamped "terminating" line and the version is further down.
// A banner that reports the wrong thing is worse than none, because it is
// the line somebody will quote when a figure stops reproducing.
func versionLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "mosquitto version ") {
			return line
		}
	}
	return "an unidentified mosquitto"
}

// **An inbound bridge, end to end through a broker that has never heard of
// a channel.**
//
// Two halves, and the second is the one mochi cannot give honestly. First
// the crossing: records published at mosquitto reach a saguin `append`
// channel, at the topics the rule's template builds, in order.
//
// Then the outage. The bridge is stopped, records are published while it is
// gone, and it is started again under the same client id - so what is being
// asked is whether mosquitto held the session and handed over the backlog,
// which is the whole promise an inbound bridge makes and the one place
// saguin depends on somebody else's implementation. mochi cannot stand in
// for it: it stops delivering once a subscriber's Receive Maximum is used
// up and does not resume (see startUpstream), so a backlog test against it
// measures that defect instead.
func TestAnInboundBridgeCarriesRecordsThroughRealMosquitto(t *testing.T) {
	wantMosquitto(t)
	addr := startMosquitto(t)

	h := start(t)
	cfg := bridgeConfig(t, addr)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// run starts a bridge and returns the stop, so the outage below is one
	// call rather than a second copy of this block.
	run := func() func() {
		br := bridge.New(cfg, h.B.NewBridgeClient(cfg.Name), limits, log, nil)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); _ = br.Run(ctx) }()
		return func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("the bridge did not stop")
			}
		}
	}

	reader := connect(t, h, "reader", true, false)
	reader.Sub(t, "events/#", 1)

	prod := dialUpstream(t, addr, "vessel-sensor")
	send := func(t testing.TB, from, n int) {
		t.Helper()
		for i := from; i < from+n; i++ {
			ack, err := prod.Publish(context.Background(), &paho.Publish{
				Topic:   "fleet/vessel-07/telemetry/hold/psi",
				QoS:     1,
				Payload: fmt.Appendf(nil, "reading-%d", i),
			})
			// **0x10 is a success**, not a failure: "No matching
			// subscribers" is what a broker answers a publish nobody is
			// listening for, and it is exactly what the first one here gets
			// - the bridge has not subscribed yet. Only 0x80 and above are
			// refusals. Reading it as an error cost this test its first run,
			// which is the reason the reason code is printed below rather
			// than only the error.
			if err != nil || ack == nil || ack.ReasonCode >= 0x80 {
				t.Fatalf("publish %d at mosquitto: reason 0x%02X, err %v",
					i, reasonOf(ack), err)
			}
		}
	}

	stop := run()
	// **The bridge's SUBSCRIBE has to have landed before anything is
	// published**, or a record was never sent to it and the test is flaky
	// about the wrong thing. There is no way to ask mosquitto what it holds,
	// so this asks the only end that can answer: publish one and wait for it
	// to come out at saguin, retrying until it does.
	ready := time.Now().Add(20 * time.Second)
	var crossed bool
	for time.Now().Before(ready) && !crossed {
		send(t, 0, 1)
		crossed = len(collect(t, reader, 1, time.Second)) == 1
	}
	if !crossed {
		stop()
		t.Fatal("nothing crossed the link in 20s: the bridge never subscribed at mosquitto, " +
			"or mosquitto never delivered - and no assertion below means anything without it")
	}
	// **Every warm-up record out of the way before anything is counted.**
	// The loop above publishes once per attempt and reads one, so a slow
	// subscribe leaves the rest of them queued - and a later phase would
	// then reach its count on records it did not cause, which is a pass for
	// the wrong reason rather than a wrong answer. They are all
	// `reading-0`, so the payload checks below would catch it too; this
	// makes it not arise.
	collect(t, reader, 64, 2*time.Second)

	// The crossing proper: at the topics the template built, and carrying
	// the payloads that were sent rather than merely the right number of
	// them.
	const carried = 10
	send(t, 1, carried)
	got := collect(t, reader, carried, 15*time.Second)
	if len(got) != carried {
		stop()
		t.Fatalf("%d of %d records crossed a real mosquitto", len(got), carried)
	}
	seen := map[string]bool{}
	for i, r := range got {
		if want := "events/telemetry/vessel-07/hold/psi"; r.Topic != want {
			t.Errorf("record %d arrived at %q, want %q - the rule's template is not what "+
				"reached the channel", i, r.Topic, want)
		}
		seen[r.Payload] = true
	}
	for i := 1; i <= carried; i++ {
		if want := fmt.Sprintf("reading-%d", i); !seen[want] {
			t.Errorf("%q never crossed, so the count above was made up of other records",
				want)
		}
	}

	// The outage. Stopped, not disconnected politely into a fresh session:
	// what is being asked is whether the far end kept one.
	stop()

	const backlog = 5
	send(t, 1+carried, backlog)

	stop = run()
	defer stop()

	// **What comes back first is not the backlog, and expecting it to be was
	// this test's own defect.** Stopping the bridge mid-flight leaves records
	// it had received but not yet acknowledged upstream - the acknowledgement
	// rides a ticker - so mosquitto resends *those* on resume, ahead of
	// anything published during the outage. RFC 0002 says what that costs
	// and it is not a fault: a record redelivered on the link carries no
	// saguin identity, so it becomes a genuinely separate record at its own
	// offset, and no consumer can tell it from a real one.
	//
	// The first version of this took the first five records after the
	// restart and asserted they were `reading-11` upwards. It got
	// `reading-2` to `reading-6` - the unacknowledged tail - and failed six
	// times out of six once the timing settled. So the assertion is about
	// the *set* arriving rather than about position in it: the outage
	// records must all cross, and duplicates alongside them are the
	// documented behaviour rather than a surprise.
	want := map[string]bool{}
	for i := 1 + carried; i <= carried+backlog; i++ {
		want[fmt.Sprintf("reading-%d", i)] = true
	}
	after := map[string]bool{}
	deadline := time.Now().Add(45 * time.Second)
	for len(want) > 0 && time.Now().Before(deadline) {
		got := collect(t, reader, 1, 3*time.Second)
		if len(got) == 0 {
			break // nothing more is arriving; the loop below says what is missing
		}
		for _, r := range got {
			after[r.Payload] = true
			delete(want, r.Payload)
		}
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for p := range want {
			missing = append(missing, p)
		}
		sort.Strings(missing)
		t.Fatalf("%v never came back after the outage, so the session mosquitto was asked "+
			"to hold either was not held or was not drained - this is the guarantee an "+
			"inbound bridge is entirely built on. What did arrive: %v",
			missing, len(after))
	}
}

// slowLink forwards TCP with a fixed one-way delay, so an outbound bridge has
// records in flight when it is cut - on loopback every PUBACK is back before
// a cut can land - and can be cut and restored.
type slowLink struct {
	ln    net.Listener
	to    string
	delay time.Duration
	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

func newSlowLink(t *testing.T, to string, delay time.Duration) *slowLink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	l := &slowLink{ln: ln, to: to, delay: delay}
	t.Cleanup(func() { _ = ln.Close(); l.cut() })
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			l.mu.Lock()
			down := l.down
			l.mu.Unlock()
			if down {
				_ = in.Close()
				continue
			}
			out, err := net.Dial("tcp", to)
			if err != nil {
				_ = in.Close()
				continue
			}
			l.mu.Lock()
			l.conns = append(l.conns, in, out)
			l.mu.Unlock()
			go l.pipe(in, out)
			go l.pipe(out, in)
		}
	}()
	return l
}

func (l *slowLink) pipe(src, dst net.Conn) {
	type chunk struct {
		at time.Time
		b  []byte
	}
	q := make(chan chunk, 8192)
	go func() {
		defer dst.Close()
		for c := range q {
			time.Sleep(time.Until(c.at))
			if _, err := dst.Write(c.b); err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			q <- chunk{at: time.Now().Add(l.delay), b: append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			close(q)
			return
		}
	}
}

func (l *slowLink) cut() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = true
	for _, c := range l.conns {
		_ = c.Close()
	}
	l.conns = nil
}

func (l *slowLink) restore() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = false
}

// RFC 0002 "Bridges": what may be sent twice is what was in flight. An
// outage longer than paho's old ten-second packet timeout put every record in
// flight at the drop on the peer three times - paho's DUP resend on the new
// connection, and the rule's own resend of the publish that had timed out
// (440 deliveries for 400 records at
// mosquitto 2.0.22, twenty of them three times). One resend mechanism now,
// the session's: each record reaches the peer at most twice, and none is
// missing.
func TestAnOutboundRecordInFlightAtALongOutageReachesMosquittoAtMostTwice(t *testing.T) {
	wantMosquitto(t)
	peer := startMosquitto(t)
	link := newSlowLink(t, peer, 40*time.Millisecond)

	// Copies per record, by the saguin-id every crossing record carries.
	var mu sync.Mutex
	copies := map[string]int{}
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		t.Fatalf("dial mosquitto: %v", err)
	}
	sub := paho.NewClient(paho.ClientConfig{
		Conn: packets.NewThreadSafeConn(conn), ClientID: "far-reader",
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			copies[string(pr.Packet.Payload)]++
			return true, nil
		}},
	})
	if _, err := sub.Connect(context.Background(), &paho.Connect{ClientID: "far-reader", KeepAlive: 30, CleanStart: true}); err != nil {
		t.Fatalf("connect at mosquitto: %v", err)
	}
	t.Cleanup(func() { _ = sub.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	if _, err := sub.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "mirror/#", QoS: 1}}}); err != nil {
		t.Fatalf("subscribe at mosquitto: %v", err)
	}
	distinct := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(copies)
	}

	h := start(t)
	yaml := fmt.Sprintf(`broker:
  id: t
`+memStorage+`channels:
  events:
    type: append
bridges:
  export:
    peer: tcp://%s
    client_id: export
    topics:
      - filter: events/#
        topic: mirror/$#
        direction: out
`, link.ln.Addr())
	path := filepath.Join(t.TempDir(), "export.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own bridge configuration does not load: %v", err)
	}
	cfg := f.BridgeSet()[0]
	bc := h.B.NewBridgeClient(cfg.Name)
	br := bridge.New(cfg, bc, limits, slog.New(slog.NewTextHandler(io.Discard, nil)), bc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = br.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	const n = 400
	pub := connect(t, h, "producer", true, false)
	go func() {
		for i := range n {
			if _, err := pub.C.Publish(context.Background(), &paho.Publish{
				Topic: fmt.Sprintf("events/t%d/%d", i%8, i), QoS: 1, Payload: fmt.Appendf(nil, "r%d", i),
			}); err != nil {
				return
			}
		}
	}()

	// Cut once a window's worth has crossed, for longer than the old packet
	// timeout, with the window full of records in flight.
	if !eventuallyTrue(60*time.Second, func() bool { return distinct() >= 80 }) {
		t.Fatalf("only %d records crossed before the cut: the bridge never carried", distinct())
	}
	link.cut()
	time.Sleep(12 * time.Second)
	link.restore()
	if !eventuallyTrue(120*time.Second, func() bool { return distinct() == n }) {
		t.Fatalf("%d of %d records reached mosquitto after the outage", distinct(), n)
	}
	time.Sleep(3 * time.Second) // for a late copy, if there were going to be one

	mu.Lock()
	defer mu.Unlock()
	again, most := 0, 0
	for _, c := range copies {
		if c > 1 {
			again++
		}
		most = max(most, c)
	}
	// The instrument's own proof: something was in flight at the cut, or
	// "at most twice" held over nothing.
	if again == 0 {
		t.Fatal("no record reached mosquitto more than once, so nothing was in flight at the cut and this proves nothing")
	}
	if most > 2 {
		t.Errorf("a record in flight at the outage reached mosquitto %d times, want at most twice", most)
	}
	t.Logf("%d records, %d of them more than once (in flight at the cut), at most %d times", len(copies), again, most)
}

// eventuallyTrue polls until want holds or the time runs out.
func eventuallyTrue(within time.Duration, want func() bool) bool {
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if want() {
			return true
		}
	}
	return want()
}

// A link that drops while a channel is refusing what the bridge brings in is
// reported down at once. The refusal was retried on paho's read loop, and
// autopaho notices a lost connection only once that loop returns: measured
// 21 seconds late against mosquitto 2.0.22, `connected` at 1 and no
// reconnect meanwhile. The bridge now
// stores on a worker of its own, so the read loop sees the link go.
func TestALinkLostWhileAChannelRefusesIsNoticedAtOnce(t *testing.T) {
	wantMosquitto(t)
	peer := startMosquitto(t)
	link := newSlowLink(t, peer, 0)

	brokertest.Bound = map[string]int64{"events": 4 << 10}
	t.Cleanup(func() { brokertest.Bound = nil })
	h := start(t)

	yaml := fmt.Sprintf(`broker:
  id: t
`+memStorage+`channels:
  events:
    type: append
bridges:
  import:
    peer: tcp://%s
    client_id: import
    topics:
      - filter: fleet/#
        topic: events/$#
        direction: in
`, link.ln.Addr())
	path := filepath.Join(t.TempDir(), "import.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own bridge configuration does not load: %v", err)
	}
	cfg := f.BridgeSet()[0]
	var said lockedBuffer
	bc := h.B.NewBridgeClient(cfg.Name)
	br := bridge.New(cfg, bc, limits, slog.New(slog.NewTextHandler(&said, nil)), bc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = br.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	if !eventuallyTrue(20*time.Second, func() bool { return br.Connected() }) {
		t.Fatalf("the bridge never connected:\n%s", said.String())
	}

	// Fill the channel until it refuses what crosses.
	prod := dialUpstream(t, peer, "vessel")
	payload := bytes.Repeat([]byte("x"), 1<<10)
	for i := 0; i < 12 && !strings.Contains(said.String(), "a channel refused a bridged record"); i++ {
		if _, err := prod.Publish(context.Background(), &paho.Publish{
			Topic: fmt.Sprintf("fleet/x/%d", i), QoS: 1, Payload: payload}); err != nil {
			t.Fatalf("publish at mosquitto: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !eventuallyTrue(10*time.Second, func() bool {
		return strings.Contains(said.String(), "a channel refused a bridged record")
	}) {
		t.Fatalf("the channel never refused, so the link was never held behind it and this proves nothing:\n%s",
			said.String())
	}

	link.cut()
	cut := time.Now()
	if !eventuallyTrue(5*time.Second, func() bool {
		return strings.Contains(said.String(), "bridge link down") && !br.Connected()
	}) {
		t.Fatalf("the link was cut %v ago while the channel refused, and the bridge still reports "+
			"it up (connected=%v): a refusal is holding the connection's read loop\n%s",
			time.Since(cut).Round(time.Millisecond), br.Connected(), said.String())
	}
	t.Logf("the link down was noticed %v after the cut, while the channel refused",
		time.Since(cut).Round(time.Millisecond))
}
