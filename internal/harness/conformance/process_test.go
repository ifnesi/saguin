package conformance

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// config.md, broker: "pid_file | none | the file the process writes its id
// to"; design.md, Restart, Start: "It writes pid_file where set", before the
// listeners, which start last. While the broker serves, the file holds its
// process id, in decimal.
func TestPIDFileHoldsTheProcessID(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "saguin.pid")
	checkPIDFile(t, pidFile, support.Launch(t, &harness.Options{PIDFile: pidFile}))
}

// checkPIDFile checks that pidFile holds the process id of b, which is
// running, in decimal.
func checkPIDFile(t *testing.T, pidFile string, b *harness.Broker) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("pid_file while the broker runs: %v", err)
	}
	if got, want := strings.TrimSpace(string(data)), strconv.Itoa(b.PID()); got != want {
		t.Errorf("pid_file holds %q, want the process id %s", data, want)
	}
}

// design.md, Store: "The store closes its readers first and its writer last.
// At a stop in full or normal mode with no store error, SQLite folds the log
// into the database as the last connection closes. No -wal or -shm file remains"
// (decided from item 0008a). config.md, broker.storage: "file_path | required
// | the database on disk", written, as config.md, Values, says, as "file_path
// with its -wal, -shm and -journal files". The durable session gives the log
// something to fold in.
func TestCleanStopLeavesOneFile(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "durable", CleanStart: true, Props: support.Durable})
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(b.DB + suffix); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("after a clean stop, %s%s: %v; want no such file", b.DB, suffix, err)
		}
	}
}

// design.md, Access and reload: "SIGHUP and SIGPIPE are ignored: a log
// output that closes loses its lines, counted as refused, and never stops
// the broker"; Delivery: "A message over the client's Maximum Packet Size
// is discarded for that subscriber as if sent, counted as missed and
// logged"; metrics.md, Process: saguin_log_lines_dropped_total, "refused
// (the log output refused it)". With standard error on a pipe whose reader
// is gone (item 0008a, 261010 115117) and a SIGHUP, the broker still
// serves: a message over a subscriber's Maximum Packet Size of 64 makes a
// line, which the closed output refuses and the series counts. The broker
// stops cleanly on SIGTERM: a broker either signal ended would not exit 0.
func TestSIGHUPAndAClosedLogDoNotStopIt(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	b := support.Launch(t, &harness.Options{Stderr: w, Operations: true})
	if err := w.Close(); err != nil { // the broker holds its own copy
		t.Fatal(err)
	}
	if err := b.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "after-sighup", CleanStart: true, Props: support.Durable})
	if err := c.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, c)
	support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "small", CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: 64}}}, support.At(0, "big"))
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "big", Payload: bytes.Repeat([]byte{'x'}, 128)})
	refused := `saguin_log_lines_dropped_total{cause="refused"}`
	if !testkit.Within(harness.DefaultTimeout, func() bool { return testkit.Sample(t, b.Metrics(), refused) >= 1 }) {
		t.Errorf("%s = %v %v after a delivery over a Maximum Packet Size, its line written to a closed output; want at least 1",
			refused, testkit.Sample(t, b.Metrics(), refused), harness.DefaultTimeout)
	}
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
	if state := stop(t, b); !state.Success() {
		t.Errorf("after SIGHUP, with its log output closed, the broker ended %v, want exit status 0 on SIGTERM", state)
	}
}

// readingConfig starts a broker on o's configuration (o.Dir and o.ConfigPath
// its own, o left as it was) whose configuration file is a named pipe,
// and returns once the broker has opened it to read and before it has read
// a byte of it, the path then holding the configuration as a regular file:
// the broker is running its own code, at the very start of reading its
// configuration (design.md, Restart, Start: "startup checks the
// configuration", the start's first step). This pins, from outside, the
// instant audit 261018-002a's first millisecond stands for. A signal sent
// earlier, before the program's own code runs, ends any program by the
// signal's default action, which no broker can prevent. feed then writes the
// configuration into the pipe and closes it, returning the write's error: a
// broker that has ended takes nothing.
func readingConfig(t *testing.T, o *harness.Options) (b *harness.Broker, feed func() error) {
	t.Helper()
	dir := t.TempDir()
	pipe := filepath.Join(dir, "saguin.pipe")
	if err := syscall.Mkfifo(pipe, 0o600); err != nil {
		t.Fatal(err)
	}
	own := *o
	own.Dir, own.ConfigPath = dir, pipe
	b = support.Started(t, &own)
	config, err := os.ReadFile(filepath.Join(dir, "saguin.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// Opening a pipe to write waits until a reader opens it: the broker.
	// The open runs on a goroutine of the test's so that a broker that
	// never opens it fails the test within margin, the bound this package
	// gives a process's own start and end; opening the reading end then
	// releases it.
	type opened struct {
		f   *os.File
		err error
	}
	open := make(chan opened, 1)
	go func() {
		f, err := os.OpenFile(pipe, os.O_WRONLY, 0)
		open <- opened{f, err}
	}()
	var w opened
	select {
	case w = <-open:
	case <-time.After(support.Margin):
		if r, err := os.OpenFile(pipe, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			if w = <-open; w.f != nil {
				_ = w.f.Close()
			}
			_ = r.Close()
		}
		t.Fatalf("the broker did not open its configuration within %v of its start:\n%s", support.Margin, b.Log())
	}
	if w.err != nil {
		t.Fatal(w.err)
	}
	t.Cleanup(func() { _ = w.f.Close() }) // a second close changes nothing
	// The broker reads its start's copy through the pipe it holds open. The
	// path becomes a regular file holding the same configuration, as an
	// operator's is, so a later read by path (a reload) finds it.
	if err := os.Rename(filepath.Join(dir, "saguin.yaml"), pipe); err != nil {
		t.Fatal(err)
	}
	return b, func() error {
		_, err := w.f.Write(config)
		if closeErr := w.f.Close(); err == nil {
			err = closeErr
		}
		return err
	}
}

// design.md, Access and reload: "SIGINT and SIGTERM stop gracefully";
// Restart: "The exit status is 0 after a stop requested by SIGINT or
// SIGTERM". Audit 261018-002a: either signal, sent in the first millisecond,
// ended the broker by the signal's default action, exit status 143 or 130
// (128 and the signal's number). Sent while the broker reads its
// configuration (readingConfig), each still stops it gracefully: exit status
// 0, whether it stops before or after it has read the rest. The pipe pins the
// instant, so one trial of each is certain to meet it.
func TestStopAskedWhileStartingIsGraceful(t *testing.T) {
	for _, tc := range []struct {
		name string
		sig  syscall.Signal
	}{{"SIGTERM", syscall.SIGTERM}, {"SIGINT", syscall.SIGINT}} {
		t.Run(tc.name, func(t *testing.T) {
			b, feed := readingConfig(t, &harness.Options{})
			if err := b.Signal(tc.sig); err != nil {
				t.Fatal(err)
			}
			_ = feed() // a broker that stopped before reading it takes nothing
			if state := support.Exited(t, b); !state.Success() {
				t.Errorf("%s, sent while the broker read its configuration, ended it %v; want a graceful stop, exit status 0", tc.name, state)
			}
		})
	}
}

// design.md, Access and reload: "SIGHUP and SIGPIPE are ignored". A SIGHUP
// sent while the broker reads its configuration (readingConfig; audit
// 261018-002a: "SIGHUP and SIGPIPE are ignored at the same line, so in
// that window they end the process too") changes nothing: the broker goes
// on to serve a client, and stops gracefully on SIGTERM, exit status 0
// (Restart).
func TestSIGHUPWhileStartingIsIgnored(t *testing.T) {
	b, feed := readingConfig(t, &harness.Options{})
	if err := b.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	if err := feed(); err != nil {
		t.Fatalf("SIGHUP, sent while the broker read its configuration: writing the configuration: %v; the broker ended %v", err, support.Exited(t, b))
	}
	if err := b.Listening(); err != nil {
		t.Fatalf("SIGHUP, sent while the broker read its configuration: %v", err)
	}
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "after-sighup", CleanStart: true})
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
	if state := stop(t, b); !state.Success() {
		t.Errorf("after a SIGHUP while starting, the broker ended %v on SIGTERM, want exit status 0", state)
	}
}

// design.md, Restart, Graceful stop: "The logger writes the lines queued
// until limits.write_timeout after main requests its drain. Main waits no
// longer. The logger ignores any later line. The process then exits". With
// standard error on a pipe that never drains, a stop that logs its cause (a
// store error: R8, and Flows, Logging, "The core logs its first fatal store
// error as one ERROR line naming the error", decided from item 0008a) still
// exits within stopLimit of the stop's start, its clients gone.
func TestUndrainedLogDoesNotHoldTheExit(t *testing.T) {
	b := support.Launch(t, &harness.Options{Stderr: neverDrained(t)})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "connected", CleanStart: true})
	damage(t, b.DB)
	toucher := touch(t, b, "touches-the-store")
	shutDown(t, c)
	_, _ = c.Close(), toucher.Close() // no client holds the stop
	support.Exited(t, b)
}

// noBrokerLimit bounds a run of the package with no broker to start: it
// fails at TestMain, before any test runs.
const noBrokerLimit = time.Minute

// design.md, Testing: "A test tool that finds no broker to drive fails,
// naming the binary; a run that tested nothing never passes" (decided from
// item 0008a). A run of the package whose broker binary does not exist
// fails, naming it.
func TestNoBrokerFails(t *testing.T) {
	if os.Getenv(support.BinaryEnv) != "" {
		t.Fatalf("the package ran its tests with no broker: %s", support.BrokerBinary)
	}
	missing := filepath.Join(t.TempDir(), "saguin")
	out, err := testkit.RunAlone(t, noBrokerLimit, []string{support.BinaryEnv + "=" + missing})
	if _, failed := errors.AsType[*exec.ExitError](err); !failed {
		t.Fatalf("the run with no broker at %s ended %v, want a failure:\n%s", missing, err, out)
	}
	if !strings.Contains(string(out), missing) {
		t.Errorf("the run with no broker does not name %s:\n%s", missing, out)
	}
}
