package main

// **What main hands the broker, driven through the binary.**
//
// Everything else tests the broker directly: a test calls SetCredentials,
// or sets the connect limits, and asserts what the broker does with it. Nothing
// drove the lines in main that read those values out of the configuration
// file - so a setting could be parsed correctly, enforced correctly, and
// never connected, and every test in the tree would pass.
//
// That is not hypothetical. Two defects of exactly this shape shipped on
// 2026-08-21: a retention period nothing advanced, and a bridge that set a
// flag and cancelled nothing. Both were a value that existed and a wire
// that did not.
//
// The settings driven here are the ones whose failure is silent: a log
// level nobody sees is not, but a connect limit that was parsed and never
// reached a listener is a bound an operator wrote and does not have.

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/gorilla/websocket"

	"github.com/ifnesi/saguin/internal/passwd"

	"go/ast"
	"go/parser"
	"go/token"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/store"
)

var listenerAddr = regexp.MustCompile(`address=(127\.0\.0\.1:\d+)`)

// **The configured log level reaches the handler**, which is the half a
// config test cannot see: `Level()` returning the right constant proves
// nothing about a broker whose logger was built before the file was read.
//
// It is built that way on purpose - the configuration naming the level has
// not been parsed when the logger is made, so the level is a `slog.LevelVar`
// set afterwards. A LevelVar that nobody sets is a broker running at Info
// whatever the file says, and every line below Info stays where it has
// always been: unreachable. That is the defect this closes, so it is the
// one worth a test.
//
// Asserted as "fewer lines, and none of the Info ones", rather than by
// naming a line: which lines the broker writes at startup is not this
// test's business, and pinning one would make it fail the next time
// somebody rephrased a log message.
func TestTheConfiguredLogLevelReachesTheLogger(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	run := func(t *testing.T, level string) string {
		t.Helper()
		dir := t.TempDir()
		cfg := filepath.Join(dir, "saguin.yaml")
		body := "broker:\n  id: t\n" + memStorage
		if level != "" {
			body += "  log_level: " + level + "\n"
		}
		body += `  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels:
  events:
    type: append
`
		if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, "--config", cfg)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("stdout: %v", err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()

		// Read until the listener is up, then a moment more so the startup
		// lines are all out, and stop.
		var seen strings.Builder
		lines := bufio.NewScanner(out)
		deadline := time.Now().Add(15 * time.Second)
		for lines.Scan() {
			seen.WriteString(lines.Text())
			seen.WriteString("\n")
			if strings.Contains(lines.Text(), "mqtt server started") || time.Now().After(deadline) {
				break
			}
		}
		return seen.String()
	}

	// A broker at `error` says nothing at startup: every line it writes
	// getting there is Info.
	quiet := run(t, "error")
	if strings.Contains(quiet, "level=INFO") {
		t.Errorf("log_level: error still wrote INFO lines, so the level never reached "+
			"the handler:\n%s", quiet)
	}

	// And the default still does, so the check above is about the level
	// rather than about a broker that printed nothing either way.
	loud := run(t, "")
	if !strings.Contains(loud, "level=INFO") {
		t.Fatalf("the default wrote no INFO lines at all, so the quiet run proves "+
			"nothing:\n%s", loud)
	}
}

// broker.session.ack_commit_interval reaches the broker that stores the
// acknowledgements, and the startup line says what the broker holds rather
// than what was asked for - so an interval read and never handed on shows as
// the default here. Absent, it is the default.
func TestTheAckCommitIntervalReachesTheBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	for _, tc := range []struct{ written, want string }{
		{"", "ack_commit_interval=200ms"},
		{"50ms", "ack_commit_interval=50ms"},
	} {
		t.Run("written "+tc.written, func(t *testing.T) {
			dir := t.TempDir()
			cfg := filepath.Join(dir, "saguin.yaml")
			body := "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:0\n"
			if tc.written != "" {
				body += "  session:\n    ack_commit_interval: " + tc.written + "\n"
			}
			body += "channels:\n  events:\n    type: append\n"
			if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, "--config", cfg)
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatalf("stdout: %v", err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
			var seen strings.Builder
			var kept string
			for lines := bufio.NewScanner(out); kept == "" && lines.Scan(); {
				seen.WriteString(lines.Text() + "\n")
				if strings.Contains(lines.Text(), `msg="sessions are kept"`) {
					kept = lines.Text()
				}
			}
			if kept == "" {
				t.Fatalf("no \"sessions are kept\" line at startup, so this proves nothing:\n%s", seen.String())
			}
			if !strings.Contains(kept, tc.want) {
				t.Errorf("the broker holds another interval than %s: %s", tc.want, kept)
			}
		})
	}
}

// RFC 0002 "What a shared group is owed": broker.share.expires_after reaches
// the broker main starts. The config test sees the key parse; only the
// binary shows main handing it on, which the startup line reports from what
// the broker holds.
func TestTheShareExpiryReachesTheBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	for _, tc := range []struct{ written, want string }{
		{"", "expires_after=0s"},
		{"90s", "expires_after=1m30s"},
	} {
		t.Run("written "+tc.written, func(t *testing.T) {
			dir := t.TempDir()
			cfg := filepath.Join(dir, "saguin.yaml")
			body := "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:0\n"
			if tc.written != "" {
				body += "  share:\n    expires_after: " + tc.written + "\n"
			}
			body += "channels:\n  events:\n    type: append\n"
			if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, "--config", cfg)
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatalf("stdout: %v", err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
			var seen strings.Builder
			var kept string
			for lines := bufio.NewScanner(out); kept == "" && lines.Scan(); {
				seen.WriteString(lines.Text() + "\n")
				if strings.Contains(lines.Text(), `msg="what shared groups are owed is kept"`) {
					kept = lines.Text()
				}
			}
			if kept == "" {
				t.Fatalf("no \"what shared groups are owed is kept\" line at startup, so this proves nothing:\n%s", seen.String())
			}
			if !strings.Contains(kept, tc.want) {
				t.Errorf("the broker holds another expiry than %s: %s", tc.want, kept)
			}
		})
	}
}

// freePort is a port the kernel has just handed out and let go again. The
// window between is a race in principle; a bind that loses it fails saying
// so, which is a clearer report than a test that guessed a busy port.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release the probe port: %v", err)
	}
	return addr
}

// **A provider says what it holds and what bounds it, whichever kind it
// is.** RFC 0005: saguin_provider_bytes is "Bytes the provider holds" and
// saguin_provider_max_bytes is "The provider's bound; zero for none",
// published so a dashboard can read what a fleet asks for against what it
// gets.
//
// It could not. The collector read the memory quotas, which are the only
// thing main built - so a sqlite provider bounded by max_page_count had no
// quota and reported holding nothing and having no bound, while it was
// refusing publishes with 0x97 for being full. An unbounded memory provider
// reported zero bytes for the same reason: a quota was built only where
// there was something to enforce, and counting rode on enforcement.
//
// A fault that renders as health is the worst shape this repository has, so
// the check is here rather than at the collector: the defect was main's
// wiring, and a broker-level test would have passed against it.
//
// Driven to the refusal on purpose. The gauges are wrong on an idle
// provider too, but "reports no bound while refusing for being full" is the
// sentence somebody has to be unable to write again - and it takes in the
// log line beside it, which named the channel's absent bound rather than
// the provider bound that did the refusing.
func TestABoundedProviderReportsItsBytesAndItsBound(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	dir := t.TempDir()
	mqttAddr, opsAddr := freePort(t), freePort(t)
	const bound = 3 * 1024 * 1024
	cfg := filepath.Join(dir, "saguin.yaml")
	body := fmt.Sprintf(`broker:
  id: bounded
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
        max_bytes: 3MiB
      mem:
        type: memory
        snapshot_dir: none
channels:
  events:
    type: append
    storage: disk
  scratch:
    type: append
    storage: mem
`, mqttAddr, opsAddr, filepath.Join(dir, "c.db"))
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	var said lockedLog
	run.Stdout, run.Stderr = &said, &said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	// Up when it answers, rather than after a sleep somebody tuned once.
	health := "http://" + opsAddr + "/health"
	var up bool
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if resp, err := http.Get(health); err == nil {
			_ = resp.Body.Close()
			up = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !up {
		t.Fatalf("the broker never answered /health on %s:\n%s", opsAddr, said.String())
	}

	conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", mqttAddr, err)
	}
	defer conn.Close()
	cl := paho.NewClient(paho.ClientConfig{Conn: conn})
	ca, err := cl.Connect(context.Background(), &paho.Connect{
		ClientID: "filler", CleanStart: true, KeepAlive: 0,
	})
	if err != nil || ca == nil || ca.ReasonCode != 0 {
		t.Fatalf("connect: %v (%v)", err, ca)
	}

	// One record onto the memory provider, which is all its half needs: the
	// question there is whether an unbounded provider counts at all.
	payload := make([]byte, 8192)
	if ack, err := cl.Publish(context.Background(), &paho.Publish{
		Topic: "scratch/x", QoS: 1, Payload: payload,
	}); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the memory provider refused a record: %v (%v)", err, ack)
	}

	// And onto the bounded one until it says no. The count is a ceiling
	// rather than an expectation: what matters is that a refusal happened,
	// and a test asserting how many fit would be asserting SQLite's page
	// accounting.
	var refused bool
	for i := 0; i < 2000 && !refused; i++ {
		ack, err := cl.Publish(context.Background(), &paho.Publish{
			Topic: "events/fill", QoS: 1, Payload: payload,
		})
		if ack == nil {
			t.Fatalf("publish %d was never acknowledged: %v", i, err)
		}
		if ack.ReasonCode == 0x97 {
			refused = true
		} else if ack.ReasonCode != 0 {
			t.Fatalf("publish %d answered 0x%02X, want 0x00 or 0x97", i, ack.ReasonCode)
		}
	}
	if !refused {
		t.Fatalf("a provider bounded at %d bytes never refused a publish, so this test "+
			"never reached the state it is about", bound)
	}

	scrape := scrapeMetrics(t, "http://"+opsAddr+"/metrics")
	for _, want := range []struct {
		line string
		zero bool
	}{
		{`saguin_provider_max_bytes{provider="disk"}`, false},
		{`saguin_provider_bytes{provider="disk"}`, false},
		{`saguin_provider_bytes{provider="mem"}`, false},
	} {
		got, ok := scrape[want.line]
		if !ok {
			t.Errorf("the scrape has no %s at all", want.line)
			continue
		}
		if got == 0 {
			t.Errorf("%s is 0 on a provider that is holding records and, for disk, "+
				"refusing publishes for being full: a bound nothing reports is a bound "+
				"no dashboard can show, and zero means \"no bound\"", want.line)
		}
	}
	if got := scrape[`saguin_provider_max_bytes{provider="disk"}`]; got != bound {
		t.Errorf("saguin_provider_max_bytes{disk} is %v, want the configured %d", got, bound)
	}
	// Zero here is right and is the other half of the rule: an unbounded
	// provider says so, rather than being absent.
	if got, ok := scrape[`saguin_provider_max_bytes{provider="mem"}`]; !ok || got != 0 {
		t.Errorf("saguin_provider_max_bytes{mem} is %v (present=%v), want 0 for a provider "+
			"with no bound", got, ok)
	}

	// The log line beside it. It named the channel's own max_bytes, which is
	// absent here, so it said the store was at a bound of zero.
	log := said.String()
	if !strings.Contains(log, "the store is at its size bound") {
		t.Fatalf("nothing was logged about the refusal:\n%s", log)
	}
	if !strings.Contains(log, fmt.Sprintf("provider_max_bytes=%d", bound)) {
		t.Errorf("the refusal did not name the bound that refused it (want "+
			"provider_max_bytes=%d):\n%s", bound, log)
	}
}

// lockedLog collects a subprocess's output while the test reads it. os/exec
// copies on a goroutine of its own, so a plain buffer read here is a data
// race and -race is on.

// The four lines in main.go that hand each sqlite provider to the broker as
// a ReaderDropper, which is the only place the binary turns the
// one-statement discard on.
//
// **The broker's own test cannot see them.** It wires the provider through
// its harness and asserts the broker does the right thing when it is given
// one; whether the binary gives it one is a different claim, and there was
// nothing making it. Removing the assignment leaves a loop that compiles and
// a broker that walks every channel on every Clean Start - correctly, and
// silently, and about a hundred times slower on a large configuration. That
// is a defect nothing would report: no line says the capability is off, and
// the only symptom is a clock.
//
// So this asserts the line the one-statement path writes and the absence of
// the line the walk writes. Making
// exactly that mutation exposed the gap: and watching `go test ./cmd/saguin` answer ok.
func TestTheBinaryHandsEachDatabaseToTheBrokerAsAReaderDropper(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	dir := t.TempDir()
	mqttAddr, opsAddr := freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	body := fmt.Sprintf(`broker:
  id: dropper
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
channels:
  events:
    type: append
    storage: disk
`, mqttAddr, opsAddr, filepath.Join(dir, "c.db"))
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	var said lockedLog
	run.Stdout, run.Stderr = &said, &said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	health := "http://" + opsAddr + "/health"
	var up bool
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if resp, err := http.Get(health); err == nil {
			_ = resp.Body.Close()
			up = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !up {
		t.Fatalf("the broker never answered /health on %s:\n%s", opsAddr, said.String())
	}

	dial := func(t *testing.T, id string, clean bool) *paho.Client {
		t.Helper()
		conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", mqttAddr, err)
		}
		cl := paho.NewClient(paho.ClientConfig{Conn: conn})
		expiry := uint32(300)
		ca, err := cl.Connect(context.Background(), &paho.Connect{
			ClientID: id, CleanStart: clean, KeepAlive: 0,
			Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
		})
		if err != nil || ca == nil || ca.ReasonCode != 0 {
			t.Fatalf("connect %s: %v (%v)", id, err, ca)
		}
		return cl
	}

	// A record, and a durable consumer that reads it - which is what puts a
	// row in the positions table for this reader to discard.
	got := make(chan struct{}, 4)
	conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	reader := paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(paho.PublishReceived) (bool, error) {
				select {
				case got <- struct{}{}:
				default:
				}
				return true, nil
			},
		},
	})
	expiry := uint32(300)
	if ca, err := reader.Connect(context.Background(), &paho.Connect{
		ClientID: "consumer", CleanStart: false, KeepAlive: 0,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	}); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect consumer: %v (%v)", err, ca)
	}
	if _, err := reader.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	p := dial(t, "producer", true)
	if ack, err := p.Publish(context.Background(), &paho.Publish{
		Topic: "events/x", QoS: 1, Payload: []byte("one"),
	}); err != nil || ack.ReasonCode > 0 {
		t.Fatalf("publish: %v (%v)", err, ack)
	}
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatalf("the consumer never received the record:\n%s", said.String())
	}

	// **The row is read back from the file**, which stays readable while
	// the broker runs: the position is stored on the broker's tick, not on
	// the acknowledgement, and the discard below logs only a row it found.
	// Waited for rather than slept through.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "c.db")+"?mode=ro")
	if err != nil {
		t.Fatalf("open the database to read: %v", err)
	}
	defer db.Close()
	rows := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM positions WHERE reader = ?`,
			store.MQTTReader("consumer")).Scan(&n); err != nil {
			t.Fatalf("read the consumer's positions: %v", err)
		}
		return n
	}
	awaitRows := func(want int, what string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); rows() != want; time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: the file holds %d position rows for the consumer, want %d:\n%s",
					what, rows(), want, said.String())
			}
		}
	}
	awaitRows(1, "the consumer's position was never stored")
	_ = reader.Disconnect(&paho.Disconnect{ReasonCode: 0})

	// The same id, now asking for a clean start: the discard runs here, and
	// is stored before the CONNACK says the session is new (invariant 18).
	_ = dial(t, "consumer", true)
	awaitRows(0, "a clean start left the ended session's position")

	// **An end marker in the log**: the line a SIGUSR1 writes comes after
	// every line the discard wrote, so what the discard said is all there.
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(said.String(), "SIGUSR1: the log level was re-read"); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the broker never answered SIGUSR1:\n%s", said.String())
		}
	}

	out := said.String()
	if !strings.Contains(out, "a session's stored positions went with it") {
		t.Errorf("the binary did not hand its sqlite provider to the broker as a "+
			"ReaderDropper: the clean start took the per-channel walk, which is "+
			"correct and silent and a transaction per channel. Log:\n%s", out)
	}
	if strings.Contains(out, "dropped a position with its session") {
		t.Errorf("the walk ran as well as, or instead of, the one statement. Log:\n%s", out)
	}
}

type lockedLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// scrapeMetrics reads /metrics into a map from the whole labelled name to
// its value, so an assertion names the series exactly as the scrape does.
func scrapeMetrics(t testing.TB, url string) map[string]float64 {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("scrape %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the scrape: %v", err)
	}
	out := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		out[line[:i]] = v
	}
	if len(out) == 0 {
		t.Fatalf("the scrape parsed to nothing, so every assertion below would pass by "+
			"vacuum:\n%s", body)
	}
	return out
}

// **Every way of being refused says so on standard error; every answer goes
// to standard output.** Both halves are asserted for every entry point,
// including the stream that must be empty: checking only that the right one
// carries something passes just as well when the wrong one carries it too,
// which is the state two of these were in.
//
// The rule rather than a list of the ones that were wrong. Go's flag package
// writes an error line and the usage to whichever stream SetOutput last
// named, and a sub-command handed one writer for both uses it for both - so
// this is a shape that arrives by inheritance, one entry point at a time,
// and asserting the three that were wrong yesterday would have missed
// `--acl explain` today.
//
// **The table is held to the flags the binary declares**, at the bottom, so
// a flag added tomorrow fails here until somebody says which stream it
// answers on. That is what stops this becoming the list it is trying not to
// be.
func TestEachAnswerGoesToItsOwnStream(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	dir := t.TempDir()
	good := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(good, []byte(`broker:
  id: streams
`+memStorage+`channels:
  events:
    type: append
`), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	missing := filepath.Join(dir, "nowhere.yaml")

	cases := []struct {
		why  string
		args []string
		// A zero exit normally means standard error stays empty. One case
		// deliberately splits the two: with --output, stdout carries the
		// resolved configuration and the summary goes to stderr, because a
		// redirect has to capture the document and nothing else or what it
		// captures is not a configuration. The exception is stated here
		// rather than the rule being weakened for everybody.
		summaryOnStderr bool
	}{
		// Answers. A zero exit must put its output on standard output and
		// leave standard error empty.
		{why: "the usage somebody asked for", args: []string{"--help"}},
		{why: "the version", args: []string{"--version"}},
		{why: "the licences inside the binary", args: []string{"--licenses"}},
		{why: "a configuration that is valid", args: []string{"--check-config", good}},
		{why: "the same, resolved", args: []string{"--check-config", good, "--output"}, summaryOnStderr: true},

		{why: "a configuration naming no acl_file", args: []string{"--acl", good, "device-7"}},
		{why: "where a topic lands", args: []string{"--route", good, "events/orders/1"}},
		{why: "what a filter reaches", args: []string{"--route", good, "#"}},

		// --json answers on the same stream the columns do. It is written
		// after the operand on purpose: a flag written after the
		// configuration is still a flag, and this is the command an
		// operator will type it on.
		{why: "where a topic lands, in JSON",
			args: []string{"--route", good, "events/orders/1", "--json"}},
		{why: "what a client may do, in JSON", args: []string{"--acl", good, "device-7", "--json"}},

		// Refusals. A non-zero exit must put its output on standard error
		// and leave standard output empty.
		{why: "a flag that does not exist", args: []string{"--bogus"}},
		{why: "no arguments at all", args: nil},
		{why: "--check-config with no configuration named", args: []string{"--check-config"}},
		{why: "--check-config alongside --config", args: []string{"--check-config", good, "--config", good}},
		{why: "a configuration that is not there", args: []string{"--check-config", missing}},
		{why: "--passwd with no operands", args: []string{"--passwd"}},
		{why: "--passwd with an unknown operand", args: []string{"--passwd", "bogus"}},
		{why: "--passwd add with no file", args: []string{"--passwd", "add"}},
		{why: "--acl with no operands", args: []string{"--acl"}},
		{why: "--acl naming a file that is not there", args: []string{"--acl", missing, "device-7"}},
		{why: "--acl with too many operands", args: []string{"--acl", good, "a", "b"}},

		{why: "--route with no operands", args: []string{"--route"}},
		// --json is how the two inspect commands answer rather than a thing
		// of its own, so alone it is a refusal and not a broker starting.
		{why: "--json with neither --acl nor --route", args: []string{"--json"}},

		{why: "--route naming a file that is not there",
			args: []string{"--route", missing, "events/orders/1"}},
		{why: "--snapshots-to-sqlite with no operands", args: []string{"--snapshots-to-sqlite"}},
		{why: "--sqlite-to-snapshots with no operands", args: []string{"--sqlite-to-snapshots"}},
	}
	for _, tc := range cases {
		t.Run(tc.why, func(t *testing.T) {
			var out, errOut strings.Builder
			cmd := exec.Command(bin, tc.args...)
			cmd.Stdout, cmd.Stderr = &out, &errOut
			err := cmd.Run()

			exit := 0
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				exit = ee.ExitCode()
			} else if err != nil {
				t.Fatalf("running %v: %v", tc.args, err)
			}

			// Which stream is right follows from the exit code, so this
			// asserts the rule rather than a remembered answer per case.
			wantOn, wantEmpty, got := "stdout", "stderr", out.String()
			if exit != 0 {
				wantOn, wantEmpty, got = "stderr", "stdout", errOut.String()
			}
			empty := map[string]string{"stdout": out.String(), "stderr": errOut.String()}[wantEmpty]

			if got == "" {
				t.Errorf("%v exited %d and put nothing on %s, which is where a %s belongs",
					tc.args, exit, wantOn,
					map[bool]string{true: "refusal", false: "answer"}[exit != 0])
			}
			if empty != "" && !(exit == 0 && tc.summaryOnStderr && wantEmpty == "stderr") {
				t.Errorf("%v exited %d and put %d bytes on %s, which should be empty - "+
					"beginning %q. A refusal on the stream somebody pipes is read as an "+
					"answer, and an answer on the stream a script watches is read as a "+
					"failure", tc.args, exit, len(empty), wantEmpty,
					strings.SplitN(empty, "\n", 2)[0])
			}
		})
	}

	// **Held to the flags the binary declares.** Every flag must appear in
	// some case above, so adding one without saying which stream it answers
	// on fails here rather than being found a day later. The
	// list is read out of --help, which is the binary's own account of what
	// it takes.
	var help strings.Builder
	helpCmd := exec.Command(bin, "--help")
	helpCmd.Stdout = &help
	if err := helpCmd.Run(); err != nil {
		t.Fatalf("--help: %v", err)
	}
	declared := regexp.MustCompile(`(?m)^\s+-([a-z][a-z0-9-]*)`).FindAllStringSubmatch(help.String(), -1)
	if len(declared) < 5 {
		t.Fatalf("only %d flags were read out of --help, which is too few to be the whole "+
			"list: this check read something it did not understand and would pass by "+
			"vacuum:\n%s", len(declared), help.String())
	}
	//
	// **Read off the cases above rather than listed again here.** It was a
	// second literal list, and the message below - "no case above runs it" -
	// was then a claim this check did not make: a flag named in that list
	// and driven by nothing satisfied it. A list of what somebody thought of
	// is the shape this whole check exists to replace, so it must not be one
	// itself.
	covered := map[string]bool{}
	for _, tc := range cases {
		for _, a := range tc.args {
			if strings.HasPrefix(a, "-") {
				covered[strings.TrimLeft(a, "-")] = true
			}
		}
	}
	for _, m := range declared {
		if !covered[m[1]] {
			t.Errorf("--%s is a flag the binary declares and no case above runs it: say "+
				"which stream it answers on, because the one it inherits is whichever "+
				"the code around it happened to use", m[1])
		}
	}
}

// **A flag written after the configuration is still a flag.**
//
// Go's flag package stops parsing at the first non-flag argument, so once
// these commands took the configuration as an operand rather than through
// `--config`, everything after it silently became an operand too:
// `saguin --check-config saguin.yaml --output` reached the dispatch as two
// operands and answered with a usage message about a file it had just been
// handed. Writing the flag first worked, which makes it a rule an operator
// cannot see and has no reason to guess.
//
// **TestEachAnswerGoesToItsOwnStream does not catch this and cannot**, which
// is why this is a test of its own rather than another row there. That one
// derives the stream it expects *from the exit code* - deliberately, so it
// asserts the rule rather than a remembered answer per case - so a refusal
// on stderr with a non-zero exit satisfies it exactly as an answer on stdout
// does. It is a test about streams; this is a test about the command working.
func TestAFlagAfterTheConfigurationIsStillAFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	good := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(good, []byte(`broker:
  id: flagorder
`+memStorage+`channels:
  events:
    type: append
`), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	for _, args := range [][]string{
		{"--check-config", good, "--output"},
		{"--check-config", "--output", good},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut strings.Builder
			cmd := exec.Command(bin, args...)
			cmd.Stdout, cmd.Stderr = &out, &errOut
			if err := cmd.Run(); err != nil {
				t.Fatalf("%v exited non-zero: %v\n%s", args, err, errOut.String())
			}
			// The resolved document, which is the whole point of --output
			// and the thing a usage message is not.
			if !strings.Contains(out.String(), "channels:") {
				t.Errorf("%v printed no resolved configuration on stdout; it began %q",
					args, strings.SplitN(out.String(), "\n", 2)[0])
			}
		})
	}
}

// **A pid file, and a signal that changes the log level without a restart.**
//
// Both answer the same operator question - how do I manage this process -
// and both are checked here rather than in a unit test because neither
// exists until a real process runs: a pid file is written by a running
// broker, and a signal is delivered to one.
//
// **The signal is the one that matters.** Getting `debug` out of a
// misbehaving bridge otherwise costs a restart, and a restart costs every
// connection and every in-flight record on a broker holding durable
// sessions.
func TestThePIDFileAndTheLogLevelSignal(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	pid := filepath.Join(dir, "saguin.pid")
	cfg := filepath.Join(dir, "saguin.yaml")
	write := func(level string) {
		t.Helper()
		if err := os.WriteFile(cfg, []byte(`broker:
  id: t
`+memStorage+`  log_level: `+level+`
  pid_file: `+pid+`
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels:
  events:
    type: append
`), 0o600); err != nil {
			t.Fatalf("write the configuration: %v", err)
		}
	}
	write("info")

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	said := make(chan string, 256)
	go func() {
		lines := bufio.NewScanner(out)
		for lines.Scan() {
			said <- lines.Text()
		}
		close(said)
	}()
	// await reads the broker's own output until a line matches, so nothing
	// here sleeps for a duration somebody guessed.
	var seen []string
	await := func(what string, within time.Duration) string {
		t.Helper()
		deadline := time.After(within)
		for {
			select {
			case line, ok := <-said:
				if !ok {
					t.Fatalf("the broker's output ended before it said %q", what)
				}
				seen = append(seen, line)
				if strings.Contains(line, what) {
					return line
				}
			case <-deadline:
				t.Fatalf("the broker never said %q within %s; it said:\n%s",
					what, within, strings.Join(seen, "\n"))
			}
		}
	}

	await("pid file written", 20*time.Second)
	body, err := os.ReadFile(pid)
	if err != nil {
		t.Fatalf("the broker said it wrote a pid file and there is none: %v", err)
	}
	got, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil {
		t.Fatalf("the pid file holds %q, which is not a process id", body)
	}
	// **The number, not merely a number.** A file holding somebody else's
	// pid is worse than none: a supervisor signals a process it did not
	// start.
	if got != run.Process.Pid {
		t.Fatalf("the pid file holds %d and the process is %d", got, run.Process.Pid)
	}

	// The signal, with the file edited underneath it. Nothing restarts.
	write("debug")
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	line := await("SIGUSR1", 10*time.Second)
	for _, want := range []string{"was=INFO", "now=DEBUG", "startup-only"} {
		if !strings.Contains(line, want) {
			t.Errorf("the signal's line does not say %q: %s", want, line)
		}
	}

	// **Raising the level must still announce itself**, and that is the
	// direction that was broken: the line was written after the switch and
	// at Info, so moving to `warn` or `error` suppressed the only
	// acknowledgement the operator gets. Quietening a log is exactly when a
	// signal that silently did nothing is indistinguishable from one that
	// worked.
	write("warn")
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	line = await("now=WARN", 10*time.Second)
	if !strings.Contains(line, "was=DEBUG") {
		t.Errorf("the line does not say what it moved from: %s", line)
	}
	if !strings.Contains(line, "level=WARN") {
		t.Errorf("the announcement was written at a level its own change hides; "+
			"it has to be at the stricter of the two: %s", line)
	}

	// **An invalid file changes nothing and does not stop the broker**, which
	// is the case an operator meets mid-edit. A signal that took a running
	// broker down over a half-saved file would be worse than no signal.
	if err := os.WriteFile(cfg, []byte("broker:\n  id: [this is not a broker id\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	line = await("not valid", 10*time.Second)
	if !strings.Contains(line, "nothing changed") {
		t.Errorf("the refusal does not say the settings were kept: %s", line)
	}
	if err := run.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the broker died on an invalid configuration it was only asked to re-read: %v", err)
	}

	// **SIGHUP does nothing, and doing nothing has to be something the
	// broker does rather than something it omits.** Left out of
	// signal.Notify it is not inert: the default disposition terminates the
	// process with no shutdown, no snapshot and no pid file removed, so a
	// memory-backed channel loses every record acknowledged since its last
	// snapshot. Driving exactly that showed three publishes answered RC:0
	// and gone after a restart - and it is reachable twice by accident,
	// since mosquitto reloads its configuration on HUP and a closing
	// terminal sends one to a broker started by hand.
	if err := run.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("hup: %v", err)
	}
	line = await("SIGHUP", 10*time.Second)
	if !strings.Contains(line, "no configuration reload") {
		t.Errorf("the line does not say why nothing happened: %s", line)
	}
	if err := run.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the broker died on SIGHUP, which the documentation says does nothing: %v", err)
	}
	if _, err := os.Stat(pid); err != nil {
		t.Errorf("SIGHUP took the pid file with it and left the broker running, "+
			"which is the stale-pid case inverted: %v", err)
	}

	// And a clean shutdown takes the file with it, so a supervisor does not
	// find a pid for a process that has gone.
	if err := run.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("term: %v", err)
	}
	_ = run.Wait()
	if _, err := os.Stat(pid); !os.IsNotExist(err) {
		t.Errorf("the pid file outlived the process: %v", err)
	}
}

// **A flag's help is a claim about behaviour, and two went stale
// in one file.**
//
// `--passwd` grew a `scope` subcommand and its help did not list it;
// `--acl` changed its argument from a client id to a user name and grew two
// columns, and its help still said `<client-id>` and "five columns out". Running
// the commands the help describes found both. Neither is
// caught by anything else: help text compiles, `vet` is quiet, and the next
// person to read it believes it - which is the entire purpose of help text.
//
// So the two claims that can be stated about the source are stated here.
// Both ask the code what is true rather than holding a second copy of the
// answer, because a list in a test is a place to keep in step and the
// second copy is where the two drift apart.
func TestTheHelpTextDescribesWhatTheCommandsActuallyDo(t *testing.T) {
	main, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	help := string(main)

	t.Run("--passwd lists every subcommand it accepts", func(t *testing.T) {
		src, err := os.ReadFile("passwd.go")
		if err != nil {
			t.Fatalf("read passwd.go: %v", err)
		}
		got := regexp.MustCompile(`(?m)^\tcase "([a-z]+)":`).
			FindAllStringSubmatch(string(src), -1)
		if len(got) < 3 {
			t.Fatalf("found %d subcommands in passwd.go, which cannot be all of "+
				"them: the pattern has stopped matching", len(got))
		}
		for _, m := range got {
			if !strings.Contains(help, "saguin --passwd "+m[1]) {
				t.Errorf("--passwd accepts %q and its help does not mention it: an "+
					"operator reading the help does not know it exists", m[1])
			}
		}
	})

	// The batch form's own header row is the count, so this cannot be
	// answered by reading the help twice.
	t.Run("--acl says how many columns it prints", func(t *testing.T) {
		bin := filepath.Join(t.TempDir(), "saguin")
		if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
			t.Skipf("the help is UNCHECKED: the binary would not build here: %v\n%s", err, out)
		}
		cfg := filepath.Join(t.TempDir(), "saguin.yaml")
		acl := filepath.Join(t.TempDir(), "acl.yaml")
		if err := os.WriteFile(acl, []byte(
			"roles:\n  r:\n    - channel: events\n      allow: [write]\n"+
				"users:\n  \"someone\": [r]\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		pw := filepath.Join(t.TempDir(), "p.passwd")
		if out, err := exec.Command(bin, "--passwd", "add", pw, "someone", "x").
			CombinedOutput(); err != nil {
			t.Fatalf("make a password file: %v\n%s", err, out)
		}
		if err := os.WriteFile(cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n"+
			"      tcp:\n        address: 127.0.0.1:0\n"+
			"    password_file: "+pw+"\n    acl_file: "+acl+"\n"+
			"channels:\n  events:\n    type: append\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		run := exec.Command(bin, "--acl", cfg)
		run.Stdin = strings.NewReader("someone\n")
		out, err := run.CombinedOutput()
		if err != nil {
			t.Fatalf("--acl in its batch form: %v\n%s", err, out)
		}
		header, _, _ := strings.Cut(string(out), "\n")
		if !strings.HasPrefix(header, "#") {
			t.Fatalf("the batch form no longer starts with a header row: %q", header)
		}
		columns := len(strings.Fields(strings.TrimPrefix(header, "#")))

		claimed := regexp.MustCompile(`([a-z]+) columns out`).FindStringSubmatch(help)
		if claimed == nil {
			t.Fatal("the --acl help no longer says how many columns it prints")
		}
		words := map[string]int{"three": 3, "four": 4, "five": 5, "six": 6,
			"seven": 7, "eight": 8, "nine": 9, "ten": 10}
		if words[claimed[1]] != columns {
			t.Errorf("the help says %s columns and the batch form prints %d: %q",
				claimed[1], columns, header)
		}
	})
}

// RFC 0005 "/v1/operations/config" and "/v1/operations/acl".
//
// **The handful of lines that make those routes real live in main and
// nowhere else.** The broker's own tests hand it a document and assert what it does
// with one, which is a different claim from whether the binary gives it
// one - and with those lines removed the handler answers 500, every broker
// test still passes, and `go test ./cmd/saguin` says ok. That is the exact
// shape of the sqlite ReaderDropper this file already guards one function
// above, and the reason this is here rather than beside the handler.
//
// It asserts the resolved values rather than the presence of a body: the
// channel below writes no filter, so `events/#` can only have come from the
// registry the broker is serving from, and a route wired to the file rather
// than to the resolution would answer without it.
func TestTheBinaryHandsTheBrokerWhatTheOperationsRoutesNeed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	dir := t.TempDir()
	mqttAddr, opsAddr := freePort(t), freePort(t)
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`roles:
  sensor:
    - channel: events
      allow: [write]
users:
  "device-*":
    roles: [sensor]
`), 0o600); err != nil {
		t.Fatalf("write the acl: %v", err)
	}
	// An acl_file names what a client may do, so the configuration must
	// also say who a client is: saguin refuses one without the other rather
	// than starting with rules about an identity nothing proves.
	pwPath := filepath.Join(dir, "clients.passwd")
	pf := passwd.New(pwPath)
	if err := pf.Set("device-7", "a password"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	// **The door requires a client certificate**, so the guard below has a
	// state to assert that only the loop in main can produce. With a plain
	// door the only answerable assertion was "not empty", and the route
	// used to fill an untold door with `none` - so the guard passed with
	// the loop deleted, which is how it came to compare nothing. A
	// self-signed certificate is its own authority, which is all
	// `client_ca_file` needs here.
	pair := writePair(t, dir)
	cfg := filepath.Join(dir, "saguin.yaml")
	body := fmt.Sprintf(`broker:
  id: wired
  mqtt:
    acl_file: %s
    password_file: %s
    listen:
      tcp:
        address: %s
        tls:
          cert_file: %s
          key_file: %s
          client_ca_file: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
channels:
  events:
    type: append
`, aclPath, pwPath, mqttAddr, pair[0], pair[1], pair[0], opsAddr)
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	var said lockedLog
	run.Stdout, run.Stderr = &said, &said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	var up bool
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if resp, err := http.Get("http://" + opsAddr + "/health"); err == nil {
			_ = resp.Body.Close()
			up = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !up {
		t.Fatalf("the broker never answered /health on %s:\n%s", opsAddr, said.String())
	}

	resp, err := http.Get("http://" + opsAddr + "/v1/operations/config")
	if err != nil {
		t.Fatalf("get the configuration: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the running binary answered /v1/operations/config with %d and %q: it was "+
			"started with a configuration and never handed the broker one\n%s",
			resp.StatusCode, raw, said.String())
	}
	var doc struct {
		Channels map[string]struct {
			Type    string `json:"type"`
			Filter  string `json:"filter"`
			Storage string `json:"storage"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the response is not JSON: %v\n%s", err, raw)
	}
	events, ok := doc.Channels["events"]
	if !ok {
		t.Fatalf("the running binary's configuration names no channel `events`: %s", raw)
	}
	if events.Filter != "events/#" {
		t.Errorf("the channel's filter came back as %q, want events/# - the file writes none, "+
			"so a route answering with the written values rather than the resolved ones is "+
			"answering a question nobody asked: %s", events.Filter, raw)
	}
	if events.Storage != "mem" {
		t.Errorf("the channel's storage came back as %q, want mem - the file writes none, so "+
			"this is the broker-wide default the broker actually resolved", events.Storage)
	}

	// **And the authorization file, which is the second thing main hands
	// over and the second thing nothing else would notice going.** A broker
	// never told about its acl_file answers this 500; one told about it
	// resolves a name against the patterns, which is the whole of the route.
	resp, err = http.Get("http://" + opsAddr + "/v1/operations/acl?user=device-7")
	if err != nil {
		t.Fatalf("get the acl: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the running binary answered /v1/operations/acl with %d and %q: it was "+
			"started with an acl_file and never handed the broker one\n%s",
			resp.StatusCode, raw, said.String())
	}
	var acl struct {
		ACLFile        *string `json:"acl_file"`
		PatternApplied *string `json:"pattern_applied"`
	}
	if err := json.Unmarshal(raw, &acl); err != nil {
		t.Fatalf("the acl response is not JSON: %v\n%s", err, raw)
	}
	if acl.ACLFile == nil || *acl.ACLFile != aclPath {
		t.Errorf("the acl response names %v as its file, want %q - a broker answering about "+
			"a file it was not given is answering about nothing", acl.ACLFile, aclPath)
	}
	if acl.PatternApplied == nil || *acl.PatternApplied != "device-*" {
		t.Errorf("pattern_applied is %v, want device-*: the running broker did not resolve "+
			"the name against the patterns it was started with", acl.PatternApplied)
	}

	// **And who may connect**, which needs no hand-off of its own - the
	// route reads the credentials the broker was already given - so what
	// this adds is the end-to-end proof that a name written into a password
	// file reaches the answer, and that its hash does not.
	resp, err = http.Get("http://" + opsAddr + "/v1/operations/users")
	if err != nil {
		t.Fatalf("get the users: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the running binary answered /v1/operations/users with %d and %q\n%s",
			resp.StatusCode, raw, said.String())
	}
	var who struct {
		Users     []string `json:"users"`
		Listeners map[string]struct {
			Users            []string `json:"users"`
			AnonymousAllowed bool     `json:"anonymous_allowed"`
			Certificate      string   `json:"certificate"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(raw, &who); err != nil {
		t.Fatalf("the users response is not JSON: %v\n%s", err, raw)
	}
	if len(who.Users) != 1 || who.Users[0] != "device-7" {
		t.Errorf("users came back as %v, want the one name the password file holds", who.Users)
	}
	// **Every configured door, each with its certificate state**, which is
	// a second loop in main and the second thing nothing else would notice
	// going. The route's own test wires one door in process and cannot see
	// either loop; without them a door is missing from the body, or present
	// with an empty certificate field - and an empty field on a door
	// requiring one is the misreading the field was added to prevent.
	tcp, ok := who.Listeners["tcp"]
	if !ok {
		t.Fatalf("the configured tcp door is not in the body: %s", raw)
	}
	if tcp.Certificate != "required" {
		t.Errorf("the tcp door writes a client_ca_file and came back as %q, want "+
			"\"required\": the binary is not telling the broker what its doors do about "+
			"client certificates, and a door reported as examining none is the "+
			"misreading the field exists to prevent: %s", tcp.Certificate, raw)
	}
	if len(tcp.Users) != 1 || tcp.Users[0] != "device-7" {
		t.Errorf("the tcp door has no file of its own and should still carry the broker's "+
			"list, got %v", tcp.Users)
	}
	stored, err := os.ReadFile(pwPath)
	if err != nil {
		t.Fatalf("read the password file: %v", err)
	}
	if _, hash, ok := strings.Cut(strings.TrimSpace(string(stored)), ":"); ok && len(hash) > 16 {
		if strings.Contains(string(raw), hash) {
			t.Errorf("the running binary put a password hash on /v1/operations/users")
		}
	}
}

// **limits.max_connect_size and limits.max_connect_rate reach the
// listeners** (RFC 0002, "The largest CONNECT before authentication" and "How
// fast connections are accepted"), driven through the real binary: the unit
// tests prove the substrate enforces each, and only this proves the
// configuration file is what sets them.
//
//   - a CONNECT larger than max_connect_size is answered 0x95, and one inside
//     it is admitted, which is the control;
//   - 30 connections arriving together at max_connect_rate: 5, whose burst is
//     10, are all admitted and take at least two seconds to be.
func TestTheConnectLimitsReachTheListeners(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n"+
		"      tcp:\n        address: 127.0.0.1:0\n  limits:\n"+
		"    max_connect_size: 1KiB\n    max_connect_rate: 5\n"+
		"channels:\n  events:\n    type: append\n"), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	lines := bufio.NewScanner(out)
	var addr string
	deadline := time.After(20 * time.Second)
	found := make(chan string, 1)
	go func() {
		for lines.Scan() {
			if m := listenerAddr.FindStringSubmatch(lines.Text()); m != nil {
				found <- m[1]
				break
			}
		}
		for lines.Scan() {
		}
	}()
	select {
	case addr = <-found:
	case <-deadline:
		t.Fatal("the broker never said which address it bound")
	}

	connect := func(id string) []byte {
		body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00}
		body = append(body, byte(len(id)>>8), byte(len(id)))
		body = append(body, id...)
		n := len(body)
		var length []byte
		for {
			b := byte(n % 128)
			n /= 128
			if n > 0 {
				b |= 0x80
			}
			length = append(length, b)
			if n == 0 {
				break
			}
		}
		return append(append([]byte{0x10}, length...), body...)
	}
	reason := func(c net.Conn) byte {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		h := make([]byte, 2)
		if _, err := io.ReadFull(c, h); err != nil || h[0] != 0x20 {
			return 0xFF
		}
		body := make([]byte, h[1])
		if _, err := io.ReadFull(c, body); err != nil || len(body) < 2 {
			return 0xFF
		}
		return body[1]
	}
	dialConnect := func(pkt []byte) byte {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		_, _ = c.Write(pkt)
		return reason(c)
	}

	// Let the burst refill before measuring anything.
	time.Sleep(2500 * time.Millisecond)
	if got := dialConnect(connect(strings.Repeat("i", 2000))); got != 0x95 {
		t.Errorf("a CONNECT of about 2 KiB against max_connect_size: 1KiB answered 0x%02X, want 0x95", got)
	}
	if got := dialConnect(connect("small")); got != 0x00 {
		t.Fatalf("a small CONNECT answered 0x%02X, so the refusal above proves nothing", got)
	}

	time.Sleep(2500 * time.Millisecond)
	const n = 30
	codes := make([]byte, n)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = dialConnect(connect(fmt.Sprintf("device-%d", i)))
		}(i)
	}
	wg.Wait()
	took := time.Since(start)
	for i, c := range codes {
		if c != 0x00 {
			t.Errorf("connection %d over max_connect_rate answered 0x%02X; it should have waited and been admitted", i, c)
		}
	}
	if took < 2*time.Second {
		t.Errorf("30 connections at max_connect_rate: 5 (burst 10) were all admitted in %v, so the rate did not reach the listener", took)
	}
}

// **same_origin and allowed_origins are re-read on SIGUSR1** (RFC 0002,
// "Which web pages may connect: same_origin and allowed_origins"), driven through the real binary and a real
// signal: a site added to the file is admitted without a restart, a site
// removed is refused, and a file that cannot be used changes nothing.
func TestSIGUSR1RereadsAllowedOrigins(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	// A site on another origin is admitted only with same_origin off, because
	// with it on a page must also be the listener's own site.
	writeConfig := func(sameOrigin, origin string) {
		t.Helper()
		if err := os.WriteFile(cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n"+
			"      ws:\n        address: 127.0.0.1:0\n        same_origin: "+sameOrigin+"\n"+
			"        allowed_origins:\n"+
			"          - \""+origin+"\"\nchannels:\n  events:\n    type: append\n"), 0o600); err != nil {
			t.Fatalf("write the configuration: %v", err)
		}
	}
	writeConfig("false", "https://one.example")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	said := make(chan string, 256)
	go func() {
		lines := bufio.NewScanner(out)
		for lines.Scan() {
			said <- lines.Text()
		}
		close(said)
	}()
	var seen []string
	// A wait from a mark taken after draining, for the reason the credential
	// test below gives: a mark behind queued lines matches the previous
	// signal's answer.
	awaitFrom := func(from int, what string, within time.Duration) string {
		t.Helper()
		for _, line := range seen[from:] {
			if strings.Contains(line, what) {
				return line
			}
		}
		deadline := time.After(within)
		for {
			select {
			case line, ok := <-said:
				if !ok {
					t.Fatalf("the broker's output ended before it said %q; it said:\n%s",
						what, strings.Join(seen, "\n"))
				}
				seen = append(seen, line)
				if strings.Contains(line, what) {
					return line
				}
			case <-deadline:
				t.Fatalf("the broker never said %q within %s; it said:\n%s",
					what, within, strings.Join(seen, "\n"))
			}
		}
	}
	drain := func() int {
		for {
			select {
			case line, ok := <-said:
				if !ok {
					return len(seen)
				}
				seen = append(seen, line)
			default:
				return len(seen)
			}
		}
	}

	m := listenerAddr.FindStringSubmatch(awaitFrom(0, "protocol=ws", 20*time.Second))
	if m == nil {
		t.Fatal("the broker never said which address the ws listener bound")
	}
	url := "ws://" + m[1] + "/"
	// upgrades reports the HTTP status the broker answered an upgrade from a
	// page on origin with - the broker's answer, not the library's.
	upgrades := func(origin string) int {
		t.Helper()
		ws, resp, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {origin}})
		if err == nil {
			_ = ws.Close()
		}
		if resp == nil {
			t.Fatalf("no HTTP response to an upgrade from %s: %v", origin, err)
		}
		return resp.StatusCode
	}
	signal := func(expect string) {
		t.Helper()
		from := drain()
		if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
			t.Fatalf("signal: %v", err)
		}
		awaitFrom(from, expect, 10*time.Second)
	}

	if got := upgrades("https://one.example"); got != http.StatusSwitchingProtocols {
		t.Fatalf("a listed site answered %d at startup, so what follows proves nothing", got)
	}
	if got := upgrades("https://two.example"); got != http.StatusForbidden {
		t.Fatalf("an unlisted site answered %d at startup, want 403", got)
	}

	writeConfig("false", "https://two.example")
	// The whole line the re-read writes. "allowed_origins" alone also
	// matches the warning the refused upgrade above wrote, which can still
	// be on its way through the pipe when the mark is taken - and then this
	// returned before the signal had changed anything.
	signal("same_origin and allowed_origins were re-read")
	if got := upgrades("https://two.example"); got != http.StatusSwitchingProtocols {
		t.Errorf("a site added to allowed_origins answered %d after SIGUSR1, want 101", got)
	}
	if got := upgrades("https://one.example"); got != http.StatusForbidden {
		t.Errorf("a site removed from allowed_origins answered %d after SIGUSR1, want 403", got)
	}

	writeConfig("false", "https://three.example/app")
	signal("nothing changed")
	if got := upgrades("https://two.example"); got != http.StatusSwitchingProtocols {
		t.Errorf("after a file that cannot be used, the listed site answered %d, want 101: "+
			"a signal mid-edit must leave the broker as it was", got)
	}

	// same_origin is re-read too: turned on, the listed site is no longer the
	// listener's own and is refused.
	writeConfig("true", "https://two.example")
	signal("same_origin")
	if got := upgrades("https://two.example"); got != http.StatusForbidden {
		t.Errorf("with same_origin turned on by SIGUSR1, another site answered %d, want 403", got)
	}
}

// **The credential files are re-read on SIGUSR1, and a withdrawn credential
// is then refused** (RFC 0002, "Withdrawing a device's access").
//
// This is the test the document did not have, and the defect it closes is
// the worst kind there is: an authorization failure reporting success.
// Both files were read once at startup, so an operator who deleted a
// password, narrowed a role and hung the device up got `hung-up` from the
// broker and a device that reconnected a second later with every grant it
// had. Nothing said otherwise.
//
// **Driven through the real binary and a real signal**, because that is
// where the defect lived: the loading was correct and ran once. A unit test
// of the loader would have passed against the broken broker.
//
// Three claims, and the third is the one an operator's edit depends on:
//
//   - the acl_file takes effect on the connection that is already open -
//     the next publish is refused, with nothing reconnecting;
//   - the password file takes effect at the next CONNECT, which is what
//     makes the hang-up the second half of the act rather than the whole of
//     it;
//   - a file that cannot be used changes nothing at all, because a signal
//     arrives in the middle of an edit as often as after one.
func TestSIGUSR1RereadsTheCredentialFilesAndAWithdrawnPasswordIsRefused(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	passwd := filepath.Join(dir, "clients.passwd")
	aclPath := filepath.Join(dir, "acl.yaml")
	opsPasswd := filepath.Join(dir, "operations.passwd")
	cfg := filepath.Join(dir, "saguin.yaml")

	// The binary's own password command, rather than a hash written here: a
	// test that wrote its own would be asserting against a format nothing
	// else uses.
	for _, who := range [][2]string{{"wide", "devpass"}, {"oncall", "opspass"}} {
		if out, err := exec.Command(bin, "--passwd", "add", passwd, who[0], who[1]).
			CombinedOutput(); err != nil {
			t.Fatalf("--passwd add %s: %v\n%s", who[0], err, out)
		}
	}
	// The operators' file is a third file of the same kind, on a door of its
	// own, and it is re-read by the same signal - so it is in this test
	// rather than assumed from the two above.
	for _, who := range [][2]string{{"scraper", "scrapepass"}, {"kept", "keptpass"}} {
		if out, err := exec.Command(bin, "--passwd", "add", opsPasswd, who[0], who[1]).
			CombinedOutput(); err != nil {
			t.Fatalf("--passwd add %s to the operations file: %v\n%s", who[0], err, out)
		}
	}
	writeACL := func(body string) {
		t.Helper()
		if err := os.WriteFile(aclPath, []byte(body), 0o600); err != nil {
			t.Fatalf("write the acl_file: %v", err)
		}
	}
	const granted = `roles:
  device:
    - channel: events
      allow: [write, read]
users:
  wide: [device]
  oncall: [device]
`
	writeACL(granted)
	// **A port asked for and given back**, because the operations listener
	// logs the address it was configured with rather than the one it bound:
	// `:0` there is a line saying `:0`, and nothing to connect to. The
	// window between closing this and the broker binding it is small and
	// this test does not run beside anything that would take it.
	spare, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port for the operations listener: %v", err)
	}
	opsAddr := spare.Addr().String()
	if err := spare.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	if err := os.WriteFile(cfg, []byte(`broker:
  id: t
`+memStorage+`  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: `+passwd+`
    acl_file: `+aclPath+`
  operations:
    listen:
      tcp:
        address: `+opsAddr+`
    password_file: `+opsPasswd+`
channels:
  events:
    type: append
    filter: events/+
`), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()

	said := make(chan string, 256)
	go func() {
		lines := bufio.NewScanner(out)
		for lines.Scan() {
			said <- lines.Text()
		}
		close(said)
	}()
	var seen []string
	// **Lines already read count, and which ones is the caller's business.**
	// Everything the broker says at startup arrives before the first thing
	// this test waits for, so a wait that only watched the channel would
	// miss a line it had itself consumed. But a wait that searched the whole
	// backlog would match the *previous* signal's line and return before the
	// one being waited for had happened - which is how this test came to
	// scrape a broker mid-re-read and report the re-read broken. So a caller
	// waiting for something that has happened before takes `len(seen)` first
	// and passes it.
	awaitFrom := func(from int, what string, within time.Duration) string {
		t.Helper()
		for _, line := range seen[from:] {
			if strings.Contains(line, what) {
				return line
			}
		}
		deadline := time.After(within)
		for {
			select {
			case line, ok := <-said:
				if !ok {
					t.Fatalf("the broker's output ended before it said %q; it said:\n%s",
						what, strings.Join(seen, "\n"))
				}
				seen = append(seen, line)
				if strings.Contains(line, what) {
					return line
				}
			case <-deadline:
				t.Fatalf("the broker never said %q within %s; it said:\n%s",
					what, within, strings.Join(seen, "\n"))
			}
		}
	}
	await := func(what string, within time.Duration) string {
		t.Helper()
		return awaitFrom(0, what, within)
	}
	// **Everything said so far, read now.** `len(seen)` marks the lines this
	// test has consumed, not the lines the broker has written - so a mark
	// taken before a signal still sits behind whatever is queued in the
	// pipe, and a wait from it matches the *previous* signal's line and
	// returns before this one has happened. That cost an hour: the test
	// scraped a broker mid-re-read, got the answer from before it, and
	// reported the re-read broken.
	drain := func() int {
		t.Helper()
		for {
			select {
			case line, ok := <-said:
				if !ok {
					return len(seen)
				}
				seen = append(seen, line)
			default:
				return len(seen)
			}
		}
	}

	line := await("listener", 20*time.Second)
	m := listenerAddr.FindStringSubmatch(line)
	if m == nil {
		t.Fatalf("the broker never said which address it bound: %s", line)
	}
	addr := m[1]

	// dial connects as a named user and reports what the CONNACK said.
	dial := func(id, user, password string) (*paho.Client, byte) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		cl := paho.NewClient(paho.ClientConfig{Conn: conn})
		ca, err := cl.Connect(context.Background(), &paho.Connect{
			ClientID: id, CleanStart: true, KeepAlive: 0,
			Username: user, Password: []byte(password),
			// Paho sends the name only when told to, and a CONNECT without
			// the flag is what an anonymous client is.
			UsernameFlag: true, PasswordFlag: true,
		})
		if ca == nil {
			t.Fatalf("no CONNACK for %s: %v", id, err)
		}
		return cl, ca.ReasonCode
	}
	// publishes says what the broker answered, which is the question - a
	// client library's own return code says only that it managed to write.
	publishesTo := func(cl *paho.Client, topic string) byte {
		t.Helper()
		ack, err := cl.Publish(context.Background(), &paho.Publish{
			Topic: topic, QoS: 1, Payload: []byte("hello"),
		})
		if ack == nil {
			t.Fatalf("no PUBACK for %s: %v", topic, err)
		}
		return ack.ReasonCode
	}
	publishes := func(cl *paho.Client) byte {
		t.Helper()
		return publishesTo(cl, "events/x")
	}

	device, code := dial("wide-device", "wide", "devpass")
	if code != 0x00 {
		t.Fatalf("the device was refused 0x%02X before anything was withdrawn", code)
	}
	if got := publishes(device); got != 0x00 {
		t.Fatalf("the device's publish was refused 0x%02X before anything was "+
			"withdrawn, so this test proves nothing about what follows", got)
	}

	// **A file the broker cannot use changes nothing**, tested first so
	// that the withdrawal below cannot be credited to it.
	writeACL("roles:\n  device:\n    - channel: events\n      allow: [wri")
	from := drain()
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	refused := awaitFrom(from, "nothing was re-read", 10*time.Second)
	if !strings.Contains(refused, "acl_file") {
		t.Errorf("the refusal does not name the file that could not be used: %s", refused)
	}
	if got := publishes(device); got != 0x00 {
		t.Errorf("a half-saved acl_file narrowed a live client to 0x%02X: a signal that "+
			"arrives mid-edit must leave the broker exactly as it was", got)
	}
	// **And the other direction, which is the one that would be a hole**: a
	// refused file must not clear the rules either. Applying nothing and
	// applying an empty authorizer are the same line in a log and opposite
	// answers on the wire.
	if got := publishesTo(device, "somewhere/else"); got != 0x87 {
		t.Errorf("after a half-saved acl_file was refused, a topic the file never granted "+
			"answered 0x%02X, want 0x87: the re-read cleared the rules it could not "+
			"replace, which grants every client everything", got)
	}

	// Now the withdrawal: the role narrowed and the password gone.
	writeACL(`roles:
  device:
    - channel: events
      allow: [write, read]
users:
  oncall: [device]
`)
	if out, err := exec.Command(bin, "--passwd", "delete", passwd, "wide").
		CombinedOutput(); err != nil {
		t.Fatalf("--passwd delete: %v\n%s", err, out)
	}
	from = drain()
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	applied := awaitFrom(from, "were re-read", 10*time.Second)
	for _, want := range []string{"users=1", "roles=1"} {
		if !strings.Contains(applied, want) {
			t.Errorf("the re-read line does not say what the files now hold (%s): %s",
				want, applied)
		}
	}

	// **The acl_file, on the connection that never went away.**
	if got := publishes(device); got != 0x87 {
		t.Errorf("after the role was withdrawn and re-read, the device's publish answered "+
			"0x%02X, want 0x87: an acl_file that only takes effect at the next restart is "+
			"the defect this signal exists to close", got)
	}

	// **The password file, at the next CONNECT** - which is why hanging the
	// client up is the second half of withdrawing its access.
	if _, code := dial("wide-device", "wide", "devpass"); code != 0x86 {
		t.Errorf("the withdrawn credential connected again with 0x%02X, want 0x86: the "+
			"password file was not re-read", code)
	}
	// And the credential that was left alone still works, so the test above
	// is about the withdrawal rather than about a broker that stopped
	// admitting anybody.
	if _, code := dial("still-here", "oncall", "opspass"); code != 0x00 {
		t.Errorf("a credential nobody touched was refused 0x%02X after the re-read", code)
	}

	// **The operators' door, which is the third file of this kind.** It is
	// re-read by the same signal, on a listener of its own, and a claim
	// about it is a thing to run rather than one to infer from the two
	// above.
	await("operations listening", 10*time.Second)
	metrics := "http://" + opsAddr + "/metrics"
	// **Its own client, keeping its connections**, which is the shape a
	// scraper has: Prometheus holds one open and reuses it. A withdrawal
	// that only reached the next connection would be a hole this test would
	// miss with keep-alives off.
	scraper := &http.Client{Transport: &http.Transport{}}
	scrapes := func(user, password string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, metrics, nil)
		if err != nil {
			t.Fatalf("build the request: %v", err)
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		resp, err := scraper.Do(req)
		if err != nil {
			t.Fatalf("GET /metrics as %s: %v", user, err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if got := scrapes("scraper", "scrapepass"); got != http.StatusOK {
		t.Fatalf("the scraper was answered %d before it was withdrawn, so what follows "+
			"would prove nothing", got)
	}
	// **And that the door is shut to everybody else**, because a door with
	// no password file answers every caller 200 - so the line above cannot
	// tell "this credential works" from "this door asks for none", and a
	// withdrawal tested against an open door proves nothing at all.
	if got := scrapes("", ""); got != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated scrape was answered %d, want 401: this door is not "+
			"asking for a credential, so nothing below is about withdrawing one", got)
	}
	if out, err := exec.Command(bin, "--passwd", "delete", opsPasswd, "scraper").
		CombinedOutput(); err != nil {
		t.Fatalf("--passwd delete from the operations file: %v\n%s", err, out)
	}
	// **From here**, because the signal above already logged this line once
	// and matching that one would scrape before this re-read had happened.
	mark := drain()
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	awaitFrom(mark, "operations routes was re-read", 10*time.Second)
	if got := scrapes("scraper", "scrapepass"); got != http.StatusUnauthorized {
		t.Errorf("a withdrawn operator read /metrics with %d after the re-read, want 401: "+
			"the operations password file is the same kind of file as the other two and "+
			"has the same problem when it is only read at startup", got)
	}
	if got := scrapes("kept", "keptpass"); got != http.StatusOK {
		t.Errorf("an operator nobody touched was answered %d after the re-read", got)
	}
}

// memStorage is the one provider every configuration must define, for the
// tests whose subject is something else. It goes straight after `id:`.
const memStorage = "  storage:\n    default: mem\n    default_retention_period: none\n" +
	"    default_retention_bytes: none\n    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"

// **Retained messages and QoS 2 are on with no block for either**, which is
// main's wiring rather than the broker's: the broker only offers what main
// attaches, and a config test can see that the file resolves to a store
// without seeing that anything was ever given one. So this drives the
// binary with a configuration that names storage and nothing else, and
// asks the wire.
func TestTheBinaryOffersRetainedAndQoS2WithNoBlockForEither(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(`broker:
  id: t
`+memStorage+`  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels: {}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(memStorage, "retained") || strings.Contains(memStorage, "qos2") {
		t.Fatal("the configuration names a retained or qos2 block, so this proves nothing about their absence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Kill(); _ = run.Wait() }()
	var addr, said string
	lines := bufio.NewScanner(out)
	for lines.Scan() {
		said += lines.Text() + "\n"
		if m := listenerAddr.FindStringSubmatch(lines.Text()); m != nil && strings.Contains(lines.Text(), "listener") {
			addr = m[1]
			break
		}
	}
	if addr == "" {
		t.Fatalf("the broker never said which address it bound:\n%s", said)
	}
	go func() { _, _ = io.Copy(io.Discard, out) }()

	dial := func(t *testing.T, id string, onPublish func(paho.PublishReceived) (bool, error)) (*paho.Client, *paho.Connack) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ccfg := paho.ClientConfig{Conn: conn}
		if onPublish != nil {
			ccfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPublish}
		}
		cl := paho.NewClient(ccfg)
		ca, err := cl.Connect(ctx, &paho.Connect{ClientID: id, CleanStart: true})
		if err != nil || ca == nil || ca.ReasonCode != 0 {
			t.Fatalf("connect %s: %v (%v)", id, err, ca)
		}
		return cl, ca
	}

	producer, ca := dial(t, "producer", nil)
	if ca.Properties == nil {
		t.Fatal("the CONNACK carried no properties")
	}
	if q := ca.Properties.MaximumQoS; q != nil && *q != 2 {
		t.Errorf("CONNACK Maximum QoS %d with no qos2 block, want the property absent (2)", *q)
	}
	if !ca.Properties.RetainAvailable {
		t.Error("CONNACK Retain Available 0 with no retained block, want 1")
	}

	// A QoS 2 publish to a broadcast topic completes its exchange, which a
	// broker with no store would have disconnected 0x9B.
	if ack, err := producer.Publish(ctx, &paho.Publish{Topic: "loose/x", QoS: 2, Payload: []byte("once")}); err != nil || ack == nil || ack.ReasonCode >= 0x80 {
		t.Fatalf("QoS 2 publish to a broadcast topic: %v (%+v)", err, ack)
	}

	// A retained publish to a broadcast topic is kept and handed to a
	// subscriber that arrives afterwards, which a broker with no store would
	// have disconnected 0x9A.
	if ack, err := producer.Publish(ctx, &paho.Publish{Topic: "state/lamp", QoS: 1, Retain: true, Payload: []byte("on")}); err != nil || ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("retained publish to a broadcast topic: %v (%+v)", err, ack)
	}
	got := make(chan *paho.Publish, 4)
	reader, _ := dial(t, "reader", func(p paho.PublishReceived) (bool, error) {
		got <- p.Packet
		return true, nil
	})
	sa, err := reader.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "state/#", QoS: 1},
		{Topic: "loose/#", QoS: 2},
	}})
	if err != nil || sa == nil || len(sa.Reasons) != 2 {
		t.Fatalf("subscribe: %v (%+v)", err, sa)
	}
	if sa.Reasons[1] != 2 {
		t.Errorf("a QoS 2 subscription to a broadcast topic was granted 0x%02X, want 0x02", sa.Reasons[1])
	}
	select {
	case p := <-got:
		if p.Topic != "state/lamp" || string(p.Payload) != "on" || !p.Retain {
			t.Errorf("got %s %q retain=%v, want the retained state/lamp \"on\"", p.Topic, p.Payload, p.Retain)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retained value was never handed to a new subscriber")
	}
}

// RFC 0002 `broker.session`: when the broker starts it ends a session whose
// expiry passed while it was stopped, and one that ended with a connection the
// stop cut, and leaves a persistent session still inside its interval. On a
// sqlite provider, which keeps all three across a crash.
//
// **This is main's wiring**: the broker only ends what it is asked to, so the
// binary is what has to be run twice over one database.
func TestSessionsThatExpiredWhileTheBrokerWasStoppedAreEndedAtStart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	mqttAddr, opsAddr := freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`broker:
  id: restart
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
channels: {}
`, mqttAddr, opsAddr, filepath.Join(dir, "s.db"))), 0o600); err != nil {
		t.Fatal(err)
	}

	var crash func()
	start := func(t *testing.T) (*lockedLog, func()) {
		t.Helper()
		run := exec.Command(bin, "--config", cfg)
		said := &lockedLog{}
		run.Stdout, run.Stderr = said, said
		if err := run.Start(); err != nil {
			t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
		}
		stop := func() { _ = run.Process.Signal(os.Interrupt); _ = run.Wait() }
		crash = func() { _ = run.Process.Kill(); _ = run.Wait() }
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if resp, err := http.Get("http://" + opsAddr + "/health"); err == nil {
				_ = resp.Body.Close()
				return said, stop
			}
			if time.Now().After(deadline) {
				stop()
				t.Fatalf("the broker never answered /health:\n%s", said.String())
			}
		}
	}
	keep := func(t *testing.T, id string, expiry uint32) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		cl := paho.NewClient(paho.ClientConfig{Conn: conn})
		ca, err := cl.Connect(context.Background(), &paho.Connect{ClientID: id, CleanStart: true,
			Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}})
		if err != nil || ca == nil || ca.ReasonCode != 0 {
			t.Fatalf("connect %s: %v (%v)", id, err, ca)
		}
		_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}

	said, stop := start(t)
	keep(t, "short", 1)
	keep(t, "long", 300)
	// A session that ends with its connection, subscribed - which is what
	// writes it - and connected when the broker dies.
	conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	passing := paho.NewClient(paho.ClientConfig{Conn: conn})
	if ca, err := passing.Connect(context.Background(), &paho.Connect{ClientID: "passing", CleanStart: true}); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect passing: %v (%v)", err, ca)
	}
	if _, err := passing.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "loose/passing", QoS: 1}}}); err != nil {
		t.Fatalf("subscribe passing: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	crash()
	if strings.Contains(said.String(), "ended the sessions left from before this start") {
		t.Fatalf("a first start over an empty store ended sessions:\n%s", said.String())
	}

	time.Sleep(2 * time.Second) // past the short session's interval, with the broker stopped
	said, stop = start(t)
	defer stop()
	out := said.String()
	if !strings.Contains(out, "ended the sessions left from before this start") ||
		!strings.Contains(out, "expired_while_stopped=1") || !strings.Contains(out, "ended_with_their_connection=1") {
		t.Errorf("the second start did not end exactly the expired session and the one the crash cut:\n%s", out)
	}
}

// RFC 0004 "Memory, with a snapshot": a memory provider's sessions are in the
// file it writes on the way out, and a start reads them back. Through the
// binary, because the wiring is main's: the broker writes the file and main
// reads it before the store is attached, and neither half is exercised by a
// test that builds the store itself.
func TestAMemoryProvidersSessionsAreWrittenAtShutdownAndReadAtStart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	snaps := filepath.Join(dir, "snapshots")
	mqttAddr, opsAddr := freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`broker:
  id: sessions-file
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: %s
channels: {}
`, mqttAddr, opsAddr, snaps)), 0o600); err != nil {
		t.Fatal(err)
	}

	start := func(t *testing.T) (*lockedLog, func()) {
		t.Helper()
		run := exec.Command(bin, "--config", cfg)
		said := &lockedLog{}
		run.Stdout, run.Stderr = said, said
		if err := run.Start(); err != nil {
			t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
		}
		stop := func() { _ = run.Process.Signal(os.Interrupt); _ = run.Wait() }
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if resp, err := http.Get("http://" + opsAddr + "/health"); err == nil {
				_ = resp.Body.Close()
				return said, stop
			}
			if time.Now().After(deadline) {
				stop()
				t.Fatalf("the broker never answered /health:\n%s", said.String())
			}
		}
	}

	said, stop := start(t)
	if strings.Contains(said.String(), "sessions read from the last shutdown") {
		t.Fatalf("a first start over an empty directory read sessions:\n%s", said.String())
	}
	conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cl := paho.NewClient(paho.ClientConfig{Conn: conn})
	expiry := uint32(300)
	if ca, err := cl.Connect(context.Background(), &paho.Connect{ClientID: "keeper", CleanStart: true,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}}); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect keeper: %v (%v)", err, ca)
	}
	if _, err := cl.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "loose/keeper", QoS: 1}}}); err != nil {
		t.Fatalf("subscribe keeper: %v", err)
	}
	_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	_ = conn.Close()
	stop()

	// The file is where the provider's snapshots are, written by the same
	// shutdown that writes them.
	file := filepath.Join(snaps, "saguin.sessions")
	if st, err := os.Stat(file); err != nil || st.Size() == 0 {
		t.Fatalf("the sessions file is %v (%v) after a graceful stop, want one with the session in it", st, err)
	}

	said, stop = start(t)
	defer stop()
	out := said.String()
	if !strings.Contains(out, "sessions read from the last shutdown") || !strings.Contains(out, "sessions=1") {
		t.Errorf("the second start did not read the session left by the first:\n%s", out)
	}
	if strings.Contains(out, "ended the sessions left from before this start") {
		t.Errorf("a session still inside its interval was ended at the start:\n%s", out)
	}
}

// RFC 0004 "Enforcing a size bound": a memory provider whose snapshots come
// back over a max_bytes the operator lowered holds them all and refuses every
// publish until room frees, and the start says so once, in the words it uses
// for a sqlite provider over its bound - rather than leaving an operator to
// work it out from a broker that answers every publish 0x97.
//
// **Sessions can be what fills it**, since a session is charged the memory it
// holds (RFC 0002 "Every session's state: `broker.session`"), and then what
// frees room is not retention: the start says what is refused and the
// remedy. In that case the channel's records alone fit the lowered bound, so
// a start that asked before the sessions were read would say nothing.
func TestAMemoryProviderRestoredOverALoweredBoundSaysSoAtStart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	for _, c := range []struct {
		name              string
		records, sessions int
		lowered           string
	}{
		{"records", 32, 0, "16KiB"},
		{"sessions", 4, 4, "16KiB"},
	} {
		t.Run(c.name, func(t *testing.T) {
			restoredOverBound(t, bin, c.records, c.sessions, c.lowered)
		})
	}
}

// restoredOverBound starts bin on a memory provider bounded at 64KiB, leaves
// records 1KiB records in a channel and sessions durable sessions with a
// filter each, stops it, and starts it again under the lowered bound.
func restoredOverBound(t *testing.T, bin string, records, sessions int, lowered string) {
	dir := t.TempDir()
	snaps := filepath.Join(dir, "snapshots")
	mqttAddr, opsAddr := freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	write := func(maxBytes string) {
		t.Helper()
		if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`broker:
  id: over-bound
  limits:
    max_message_size: 2KiB
    max_header_bytes: 1KiB
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: %s
        max_bytes: %s
channels:
  events:
    type: append
`, mqttAddr, opsAddr, snaps, maxBytes)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	start := func(t *testing.T) (*lockedLog, func()) {
		t.Helper()
		run := exec.Command(bin, "--config", cfg)
		said := &lockedLog{}
		run.Stdout, run.Stderr = said, said
		if err := run.Start(); err != nil {
			t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
		}
		stop := func() { _ = run.Process.Signal(os.Interrupt); _ = run.Wait() }
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if resp, err := http.Get("http://" + opsAddr + "/health"); err == nil {
				_ = resp.Body.Close()
				return said, stop
			}
			if time.Now().After(deadline) {
				stop()
				t.Fatalf("the broker never answered /health:\n%s", said.String())
			}
		}
	}
	const warning = "storage already holds more than its max_bytes"
	const sessionsFill = "sessions expire"

	write("64KiB")
	said, stop := start(t)
	if strings.Contains(said.String(), warning) {
		t.Fatalf("a first start over an empty directory said it was over its bound:\n%s", said.String())
	}
	dial := func(id string, clean bool, expiry uint32) *paho.Client {
		t.Helper()
		conn, err := net.DialTimeout("tcp", mqttAddr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		cl := paho.NewClient(paho.ClientConfig{Conn: conn})
		cp := &paho.Connect{ClientID: id, CleanStart: clean, Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}}
		if ca, err := cl.Connect(context.Background(), cp); err != nil || ca.ReasonCode != 0 {
			t.Fatalf("connect %s: %v (%v)", id, err, ca)
		}
		return cl
	}
	cl := dial("pub", true, 0)
	payload := []byte(strings.Repeat("x", 1024))
	for i := range records {
		if pa, err := cl.Publish(context.Background(), &paho.Publish{Topic: "events/a", QoS: 1, Payload: payload}); err != nil || pa.ReasonCode != 0 {
			t.Fatalf("publish %d: %v (%v)", i, err, pa)
		}
	}
	_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	for i := range sessions {
		s := dial(fmt.Sprintf("kept-%d", i), false, 3600)
		if sa, err := s.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
			{Topic: fmt.Sprintf("news/%d/cmd", i), QoS: 1}}}); err != nil || sa.Reasons[0] != 1 {
			t.Fatalf("subscribe kept-%d: %v (%v)", i, err, sa)
		}
		_ = s.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}
	stop()

	write(lowered)
	said, stop = start(t)
	defer stop()
	out := said.String()
	if !strings.Contains(out, warning) || !strings.Contains(out, "provider=mem") {
		t.Errorf("a start holding more than its lowered max_bytes of %s did not say it was over its bound:\n%s",
			lowered, out)
	}
	if got := strings.Contains(out, sessionsFill); got != (sessions > 0) {
		t.Errorf("sessions filled the provider: %v; the start said sessions are what to wait for or move: %v:\n%s",
			sessions > 0, got, out)
	}
}

// **A door admitting a client nothing identifies beside an acl_file is named
// by --check-config and at the start**, one line per such door (RFC 0002
// "Authorization"). config's own test decides which doors; this one proves
// both places print what it decided, and that a door requiring a certificate
// is not named - so a loop printing every door fails here as well as one
// printing none.
func TestADoorNothingIdentifiesBesideAnACLFileIsNamedAtCheckAndAtStart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(acl, []byte(
		"roles:\n  device:\n    - channel: events\n      allow: [write, read]\n"+
			"users:\n  \"sensor-*\": [device]\n"), 0o600); err != nil {
		t.Fatalf("write the acl file: %v", err)
	}
	// A self-signed certificate is its own authority, which is all
	// client_ca_file needs here.
	pair := writePair(t, dir)
	mqttAddr, wsAddr, opsAddr := freePort(t), freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`broker:
  id: doors
  mqtt:
    acl_file: %s
    listen:
      tcp:
        address: %s
        tls:
          cert_file: %s
          key_file: %s
          client_ca_file: %s
      ws:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
    min_scrape_interval: 60s
`+memStorage+`channels:
  events:
    type: append
`, acl, mqttAddr, pair[0], pair[1], pair[0], wsAddr, opsAddr)), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	const named = "admits a client with no certificate and no password beside the acl_file"
	// Each door named, from the lines that carry the sentence.
	doors := func(said string) []string {
		var out []string
		for _, line := range strings.Split(said, "\n") {
			if !strings.Contains(line, named) {
				continue
			}
			for _, id := range []string{"tcp", "ws", "unix"} {
				if strings.Contains(line, "listener "+id+" ") {
					out = append(out, id)
				}
			}
		}
		return out
	}

	out, err := exec.Command(bin, "--check-config", cfg).CombinedOutput()
	if err != nil {
		t.Fatalf("--check-config exited non-zero over a warning, so a deploy gating on it "+
			"would stop: %v\n%s", err, out)
	}
	if got := doors(string(out)); !reflect.DeepEqual(got, []string{"ws"}) {
		t.Errorf("--check-config named %q, want the ws door alone:\n%s", got, out)
	}

	run := exec.Command(bin, "--config", cfg)
	said := &lockedLog{}
	run.Stdout, run.Stderr = said, said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	defer func() { _ = run.Process.Signal(os.Interrupt); _ = run.Wait() }()
	// The warning follows the listening line, which follows /health.
	for deadline := time.Now().Add(30 * time.Second); !strings.Contains(said.String(), "saguin listening"); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the broker never said it was listening:\n%s", said.String())
		}
	}
	// Written straight after it by the same goroutine; five seconds is room
	// for a loaded machine, and the wait a mutant that says nothing costs.
	for deadline := time.Now().Add(5 * time.Second); !strings.Contains(said.String(), named) && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}
	if got := doors(said.String()); !reflect.DeepEqual(got, []string{"ws"}) {
		t.Errorf("the start named %q, want the ws door alone:\n%s", got, said.String())
	}
}

// RFC 0002 `broker.qos2`, and RFC 0003 "Exactly once, and where the
// unfinished ones wait": an exactly-once publish is held in the store of the
// channel it is for, so there is no store of its own to name. A
// configuration that names one - here the pairing that once let sessions
// outlive a restart their exchanges did not - is refused by name at the
// start, never accepted and ignored, which would leave the operator believing
// the publishes wait where they said.
func TestAStartRefusesAStoreNamedForExactlyOncePublishes(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	mqttAddr, opsAddr := freePort(t), freePort(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`broker:
  id: mixed-durability
  mqtt:
    listen:
      tcp:
        address: %s
  operations:
    listen:
      tcp:
        address: %s
  qos2:
    storage: scratch
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
      scratch:
        type: memory
        snapshot_dir: none
channels: {}
`, mqttAddr, opsAddr, filepath.Join(dir, "s.db"))), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--config", cfg).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the broker started with broker.qos2.storage set and ran until it was stopped:\n%s", out)
	}
	if err == nil {
		t.Fatalf("the broker exited 0 with broker.qos2.storage set:\n%s", out)
	}
	if !strings.Contains(string(out), "broker.qos2.storage does not apply") {
		t.Errorf("the refusal does not name the key:\n%s", out)
	}
	if !strings.Contains(string(out), "held in the store of the channel it is for") {
		t.Errorf("the refusal does not say where an exactly-once publish is held instead:\n%s", out)
	}
}

// The startup line's client_authentication and metrics_authentication say
// what each door asks, as the doors resolve it, not what the broker-wide
// keys say. Each case is a
// configuration beside what the wire answered: a door with its
// own password file refuses an anonymous CONNECT 0x86 and was said to admit
// every client; RFC 0002's own example admits anonymous on its unix door
// and read "password file"; an operations door requiring a client
// certificate read "none".
func TestTheStartupLineSaysWhatEachDoorAsks(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name string
		m    config.MQTT
		want string
	}{
		{"a tcp door with its own password file, none broker-wide",
			config.MQTT{Listen: config.Listen{TCP: []config.TCPDoor{{Name: "tcp",
				Address: config.Address{Auth: config.Auth{PasswordFile: "/etc/saguin/p"}}}}}},
			"password file"},
		{"RFC 0002's example: the broker's file, and a unix door admitting anonymous",
			config.MQTT{PasswordFile: "/etc/saguin/p", AllowAnonymous: &no, Listen: config.Listen{
				TCP: []config.TCPDoor{{Name: "tcp"}},
				Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{
					Path: "/run/saguin.sock", Auth: config.Auth{AllowAnonymous: &yes}}}}}},
			"tcp=password file | unix=password file, anonymous also admitted"},
		{"the broker's file, and a tcp door admitting anonymous",
			config.MQTT{PasswordFile: "/etc/saguin/p", Listen: config.Listen{TCP: []config.TCPDoor{{Name: "tcp",
				Address: config.Address{Auth: config.Auth{AllowAnonymous: &yes}}}}}},
			"password file, anonymous also admitted"},
		{"every door the same is said once",
			config.MQTT{PasswordFile: "/etc/saguin/p", Listen: config.Listen{
				TCP:  []config.TCPDoor{{Name: "tcp"}},
				Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: "/run/saguin.sock"}}}}},
			"password file"},
		{"no password file anywhere",
			config.MQTT{Listen: config.Listen{TCP: []config.TCPDoor{{Name: "tcp"}}}},
			"none (every client admitted)"},
		{"a door requiring a certificate, beside one that does not",
			config.MQTT{PasswordFile: "/etc/saguin/p", Listen: config.Listen{
				TCP: []config.TCPDoor{{Name: "tcp", Address: config.Address{
					TLS: &config.TLS{ClientCAFile: "/etc/saguin/ca.pem"}}}},
				WS: []config.WSDoor{{Name: "ws"}}}},
			"tcp=client certificate required; password file | ws=password file"},
	} {
		if got := clientAuthState(tc.m); got != tc.want {
			t.Errorf("%s: client_authentication=%q, want %q", tc.name, got, tc.want)
		}
	}

	for _, tc := range []struct {
		name string
		o    config.Operations
		want string
	}{
		{"an operations tcp door requiring a client certificate, no password file",
			config.Operations{Listen: config.OperationsListen{TCP: []config.TCPDoor{{Name: "tcp",
				Address: config.Address{TLS: &config.TLS{ClientCAFile: "/etc/saguin/ca.pem"}}}}}},
			"tcp=client certificate"},
		{"both, beside a unix door with neither",
			config.Operations{PasswordFile: "/etc/saguin/ops", Listen: config.OperationsListen{
				TCP: []config.TCPDoor{{Name: "tcp", Address: config.Address{
					TLS: &config.TLS{ClientCAFile: "/etc/saguin/ca.pem"}}}},
				Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{
					Path: "/run/ops.sock", Auth: config.Auth{PasswordFile: ""}}}}}},
			"tcp=basic+client certificate unix=basic"},
		{"a certificate accepted, not required, is no credential",
			config.Operations{Listen: config.OperationsListen{TCP: []config.TCPDoor{{Name: "tcp",
				Address: config.Address{
					TLS: &config.TLS{ClientCAFile: "/etc/saguin/ca.pem", RequireCertificate: &no}}}}}},
			"tcp=none"},
	} {
		if got := authenticationState(&tc.o); got != tc.want {
			t.Errorf("%s: metrics_authentication=%q, want %q", tc.name, got, tc.want)
		}
	}
}

// RFC 0002 "Validation": an abstract Unix socket name, beginning with "@", has
// no file to leave behind and takes no lock - and none to set a mode on. The
// start set the mode on the name itself, so a configuration --check-config
// passed stopped at "chmod @...: no such file or directory". It starts, and answers MQTT on the socket.
func TestAnAbstractUnixSocketStartsAndAnswers(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	name := fmt.Sprintf("@saguin-test-abstract-%d", os.Getpid())
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  id: abstract\n"+memStorage+
		"  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:0\n"+
		"      unix:\n        path: \""+name+"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "--check-config", cfg).CombinedOutput(); err != nil {
		t.Fatalf("--check-config refused it, so this proves nothing about the start: %v\n%s", err, out)
	}
	run := exec.Command(bin, "--config", cfg)
	said := &lockedLog{}
	run.Stdout, run.Stderr = said, said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- run.Wait() }()
	defer func() { _ = run.Process.Signal(os.Interrupt); <-exited }()
	for deadline := time.Now().Add(30 * time.Second); !strings.Contains(said.String(), "saguin listening"); time.Sleep(50 * time.Millisecond) {
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("the broker exited with an abstract socket configured (%v):\n%s", err, said.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker never said it was listening:\n%s", said.String())
		}
	}
	c, err := net.Dial("unix", name)
	if err != nil {
		t.Fatalf("dial the abstract socket: %v", err)
	}
	defer c.Close()
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00, 0x00, 0x01, 'a'}
	if _, err := c.Write(append([]byte{0x10, byte(len(body))}, body...)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	ack := make([]byte, 4)
	if _, err := io.ReadFull(c, ack); err != nil || ack[0] != 0x20 || ack[3] != 0x00 {
		t.Fatalf("the abstract socket answered % x (%v), want a CONNACK 0x00", ack, err)
	}
}

// RFC 0002 "Who may connect": a unix door's socket takes the `mode` the
// configuration gives it, and the socket is the access control - whoever can
// open the file can speak MQTT to the broker. main hands the mode to the
// listener, and nothing checked that it does: a binary that stopped passing
// it left the socket at 0775 with every test green. 0600 rather than the default 0660, so a mode
// that fell back to the default fails too.
func TestAPlainUnixDoorTakesItsMode(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir, err := os.MkdirTemp("/tmp", "sgn") // short: a socket path is bounded
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "saguin.sock")
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  id: mode\n"+memStorage+
		"  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:0\n"+
		"      unix:\n        path: "+sock+"\n        mode: \"0600\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := exec.Command(bin, "--config", cfg)
	said := &lockedLog{}
	run.Stdout, run.Stderr = said, said
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- run.Wait() }()
	defer func() { _ = run.Process.Signal(os.Interrupt); <-exited }()
	for deadline := time.Now().Add(30 * time.Second); !strings.Contains(said.String(), "saguin listening"); time.Sleep(50 * time.Millisecond) {
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("the broker exited (%v):\n%s", err, said.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker never said it was listening:\n%s", said.String())
		}
	}
	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("the unix door left no socket at %s: %v", sock, err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("the unix door's socket is %#o, want the configured 0600: whoever the mode "+
			"lets open it can speak MQTT to the broker", got)
	}
}

// The signals are handled before anything answers, and the pid file is
// written before the MQTT listeners serve. Installed after Serve and after "saguin listening", a SIGTERM or a
// SIGUSR1 in the window between met Go's default disposition and ended the
// process with no shutdown and no snapshot. Timing that window is a race a
// test would lose on a fast machine, so this asks main's syntax tree for
// the order instead: signal.Notify, the operations listener, the pid file,
// the MQTT listeners' Serve.
func TestTheSignalsAreHandledBeforeAnythingAnswers(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	var main *ast.FuncDecl
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "main" {
			main = fn
		}
	}
	if main == nil {
		t.Fatal("main.go has no func main, so nothing below was checked")
	}
	order := []string{"signal.Notify", "b.ServeOperations", "writePIDFile", "srv.Serve"}
	first := map[string]token.Pos{}
	ast.Inspect(main.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			name = fn.Name
		case *ast.SelectorExpr:
			if x, ok := fn.X.(*ast.Ident); ok {
				name = x.Name + "." + fn.Sel.Name
			}
		}
		if _, seen := first[name]; !seen && slices.Contains(order, name) {
			first[name] = call.Pos()
		}
		return true
	})
	for _, name := range order {
		if _, ok := first[name]; !ok {
			t.Fatalf("main calls no %s, so this check has stopped matching the code it guards", name)
		}
	}
	for i := 1; i < len(order); i++ {
		if first[order[i-1]] > first[order[i]] {
			t.Errorf("main calls %s (%s) after %s (%s), want %s",
				order[i-1], fset.Position(first[order[i-1]]), order[i], fset.Position(first[order[i]]),
				strings.Join(order, " before "))
		}
	}
}

// RFC 0002 `flush_interval`: a value outside 10ms to 1s is refused by name at
// --check-config and at start alike, and an accepted one is said at start.
func TestAFlushIntervalOutsideItsBoundsIsRefusedAtCheckAndAtStart(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	write := func(interval string) string {
		cfg := filepath.Join(dir, "saguin-"+interval+".yaml")
		body := fmt.Sprintf(`broker:
  id: flush
  mqtt:
    listen:
      tcp:
        address: %s
  storage:
    default: p
    default_retention_period: none
    default_retention_bytes: none
    providers:
      p:
        type: sqlite
        file_path: %s
        flush_interval: %s
channels:
  events:
    type: append
`, freePort(t), filepath.Join(dir, interval+".db"), interval)
		if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	for _, tc := range []struct {
		interval string
		refused  bool
	}{{"9ms", true}, {"10ms", false}, {"1s", false}, {"1001ms", true}} {
		cfg := write(tc.interval)
		out, err := exec.Command(bin, "--check-config", cfg).CombinedOutput()
		if tc.refused != (err != nil) || (tc.refused && !strings.Contains(string(out), "flush_interval \""+tc.interval+"\"")) {
			t.Errorf("--check-config with %s: err=%v, refused wanted %v:\n%s", tc.interval, err, tc.refused, out)
		}
		if !tc.refused {
			continue
		}
		out, err = exec.Command(bin, "--config", cfg).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "flush_interval \""+tc.interval+"\"") {
			t.Errorf("start with %s: err=%v, want a refusal naming flush_interval:\n%s", tc.interval, err, out)
		}
	}
}
