package support

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// OpsLaunch launches a broker of the test's own with an operations listener,
// and waits until it is ready.
func OpsLaunch(t *testing.T, o *harness.Options) *harness.Broker {
	t.Helper()
	own := *o
	own.Operations = true
	b := Launch(t, &own)
	AwaitReady(t, b)
	return b
}

// OpsTry sends one request to b's operations listener, method on path with
// no credential, its Host header host ("" for the listener's address, an IP
// literal), and returns the answer, or the error that kept it from coming
// within harness.DefaultTimeout (harness.OpsTarget.Do).
func OpsTry(b *harness.Broker, method, path, host string) (harness.OpsAnswer, error) {
	return PlainOps(b).Do(&harness.OpsCall{Method: method, Path: path, Host: host})
}

// AwaitReady waits until b's /health answers 200, failing the test where it
// has not within harness.DefaultTimeout of the MQTT listener's accepting
// (design.md, Restart, Start: "Listeners and bridges start last, and
// /health reports ready only then"). The watchdog is the test's, not a
// deadline of the broker's.
func AwaitReady(t *testing.T, b *harness.Broker) {
	t.Helper()
	var last string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		a, err := OpsTry(b, http.MethodGet, "/health", "")
		last = fmt.Sprint(a, err)
		return err == nil && a.Status == http.StatusOK
	}) {
		t.Fatalf("watchdog: /health has not answered 200 within %v of the broker listening; last: %s", harness.DefaultTimeout, last)
	}
}

// ExpectHealth fails the test unless a is /health's answer status with the
// JSON object {"status": state} (design.md, Operations, HTTP: "Answers are
// JSON"; /health "returns 200 {"status":"ok"} [...] Otherwise it returns 503
// {"status":"unavailable"}"). A HEAD's answer has no body.
func ExpectHealth(t *testing.T, method string, a harness.OpsAnswer, status int, state string) {
	t.Helper()
	if a.Status != status {
		t.Errorf("%s /health: %d %q, want %d", method, a.Status, a.Body, status)
		return
	}
	if method == http.MethodHead {
		if a.Body != "" {
			t.Errorf("HEAD /health: a body %q, want none", a.Body)
		}
		return
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(a.Body), &got); err != nil || a.MediaType != "application/json" {
		t.Errorf("GET /health: %s %q (%v), want JSON", a.MediaType, a.Body, err)
		return
	}
	if want := map[string]any{"status": state}; !maps.Equal(got, want) {
		t.Errorf("GET /health: %v, want %v", got, want)
	}
}

// PlainOps is b's operations listener of Options.Operations, plain TCP.
func PlainOps(b *harness.Broker) harness.OpsTarget {
	return harness.OpsTarget{Network: "tcp", Address: b.OpsAddr}
}
