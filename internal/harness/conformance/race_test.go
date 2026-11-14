package conformance

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// raceLoad is the stream that once let a store reader write a delivery's
// payload fields while the core read them (0013d's comment of 261103
// 110831: store.ReadRecordPayloads against core.offerTurn): QoS 0 PUBLISHes
// from one client to one subscriber over TCP, so that the core reads
// payloads for one delivery while it offers the next. It is the smallest
// tried that caught that race every time, 25 runs of 25 on the code before
// its fix; 100 or 300 PUBLISHes, or 100 of 8 or 32 KiB, missed it now and
// then.
const (
	racePublishes = 1000
	racePayload   = 1 << 10
)

// design.md, R1: "The core alone owns shared broker state"; R3: "The core
// talks to other goroutines only through bounded Go channels"; Execution,
// Communication: "Once a buffer is sent, its sender never touches it
// again"; Testing: "The race detector runs on a native glibc build". The
// broker is built with Go's race detector, which reports every access two
// goroutines make to the same memory without one ordering them, and makes
// the process exit with status 66 when it found any.
//
// The race-detecting broker serves raceLoad: a subscriber to load at QoS 0
// receives every PUBLISH whole, then both clients leave and the broker
// stops gracefully. It exits with status 0, its log holding no "DATA RACE".
func TestNoDataRaceUnderLoad(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "saguin-race")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-buildvcs=false", "-tags=netgo,osusergo",
		"-o", binary, "github.com/ifnesi/saguin/cmd/saguin")
	tmp := filepath.Join(filepath.Dir(filepath.Dir(support.Binary)), ".tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	build.Env = append(os.Environ(), "TMPDIR="+tmp)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the broker with the race detector: %v\n%s", err, out)
	}
	b := support.StartedFrom(t, binary, &harness.Options{})
	if err := b.Listening(); err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true})
	t.Cleanup(func() { _ = sub.Close() }) // the test is over
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "load", QoS: 0}}, 0)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	t.Cleanup(func() { _ = pub.Close() }) // the test is over
	payload := bytes.Repeat([]byte("x"), racePayload)
	sent := make(chan error, 1)
	go func() {
		for range racePublishes {
			if err := pub.Send(&harness.Publish{Topic: "load", Payload: payload}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	for i := range racePublishes {
		if p := harness.Expect[*harness.Publish](t, sub); !bytes.Equal(p.Payload, payload) {
			t.Fatalf("PUBLISH %d: %d bytes, not the %d published", i, len(p.Payload), len(payload))
		}
	}
	if err := <-sent; err != nil {
		t.Fatalf("publishing: %v", err)
	}
	_ = sub.Close()
	_ = pub.Close()
	state := stop(t, b)
	if log := b.Log(); state.ExitCode() != 0 || strings.Contains(log, "DATA RACE") {
		t.Fatalf("the race-detecting broker exited with status %d; want 0, no race found:\n%s", state.ExitCode(), log)
	}
}
