package broker

// RFC 0005 "The health endpoint".
//
// The 503 is what this file is for, and it is why these tests are inside
// the package: the condition being reported is the broker's own lock being
// unavailable, and there is no way to hold that from outside. The rest of
// the contract is checked here too rather than split across two files for
// the sake of it.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/passwd"
)

func healthBroker(t *testing.T) *Broker {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHealthAnswersOK(t *testing.T) {
	h := healthBroker(t).operationsHandler(healthLockTimeout, time.Minute, nil)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/health", nil))

		if w.Code != http.StatusOK {
			t.Errorf("%s /health answered %d, want 200", method, w.Code)
		}
		if got := w.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("%s /health content type %q, want application/json", method, got)
		}
		body := w.Body.String()
		switch method {
		case http.MethodGet:
			if body != "{\"status\":\"ok\"}\n" {
				t.Errorf("GET /health body %q", body)
			}
		case http.MethodHead:
			// A HEAD carries the status and no body, which is what some
			// probes send.
			if body != "" {
				t.Errorf("HEAD /health returned a body: %q", body)
			}
		}
	}
}

func TestHealthRefusesOtherPathsAndMethods(t *testing.T) {
	h := healthBroker(t).operationsHandler(healthLockTimeout, time.Minute, nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("GET / answered %d, want 404", w.Code)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/health", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /health answered %d, want 405", w.Code)
	}
	if got := w.Header().Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow header %q, want \"GET, HEAD\"", got)
	}
}

// The whole point of the endpoint: a broker that cannot take its own lock
// has stopped publishing, and that is the failure a restart fixes. Asserted
// through the handler rather than through Responsive alone, because the
// status code and the body are the contract RFC 0005 states.
func TestHealthAnswers503WhenTheBrokerIsWedged(t *testing.T) {
	b := healthBroker(t)
	h := b.operationsHandler(150*time.Millisecond, time.Minute, nil)

	// Wedged: something holds the broker's lock and does not give it back.
	b.mu.Lock()
	defer b.mu.Unlock()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("a wedged broker answered %d, want 503", w.Code)
	}
	if got := w.Body.String(); got != "{\"status\":\"unavailable\"}\n" {
		t.Errorf("body %q", got)
	}
}

// And it says yes again once the lock comes back, so the 503 above is the
// lock rather than the timer.
func TestHealthRecoversWhenTheLockIsReleased(t *testing.T) {
	b := healthBroker(t)

	b.mu.Lock()
	if b.Responsive(150 * time.Millisecond) {
		t.Fatal("Responsive said yes while the broker's lock was held")
	}
	b.mu.Unlock()

	if !b.Responsive(150 * time.Millisecond) {
		t.Error("Responsive still says no after the lock was released")
	}
}

// A busy broker is not a wedged one. The lock is taken and released
// constantly under load, and a probe that reported those as failures would
// have a restart loop waiting at exactly the wrong moment - which is why
// the wait is a blocking Lock that joins the mutex's queue rather than a
// TryLock that can lose the race for ever.
func TestHealthIsNotFooledByContention(t *testing.T) {
	b := healthBroker(t)

	stop := make(chan struct{})
	defer close(stop)

	// Each contender reports that it has taken the lock at least once, so
	// the trials below cannot run before there is anything to contend with -
	// a test that measured an idle broker would pass without testing.
	spinning := make(chan struct{}, 8)
	for range 8 {
		go func() {
			first := true
			for {
				select {
				case <-stop:
					return
				default:
				}
				b.mu.Lock()
				b.mu.Unlock() //nolint:staticcheck // held and released, as the publish path does
				if first {
					first = false
					spinning <- struct{}{}
				}
			}
		}()
	}
	for range 8 {
		<-spinning
	}

	// One trial would be a coin flip; the claim is that it holds every time.
	for i := range 50 {
		if !b.Responsive(2 * time.Second) {
			t.Fatalf("trial %d: a busy broker was reported wedged", i)
		}
	}
}

// **Who you are, then what you may reach**, and the second answered only
// once the first has been.
//
// **403 rather than 401 is the half that matters operationally.** A 401
// says "try again with a credential", so Prometheus retries and records an
// authentication failure - sending an operator to check a password that is
// correct, while the actual answer is that this credential does not reach
// this route. Getting that wrong turns a five-second fix into an afternoon.
func TestACredentialReachesOnlyTheRoutesItNames(t *testing.T) {
	b := healthBroker(t)
	dir := t.TempDir()

	ops := passwd.New(filepath.Join(dir, "operations.passwd"))
	for _, u := range []string{"scraper", "engineer", "everything"} {
		if err := ops.Set(u, "hunter2"); err != nil {
			t.Fatalf("set %s: %v", u, err)
		}
	}
	ops.SetScopes("scraper", []string{"/metrics"})
	ops.SetScopes("engineer", []string{"/v1/operations"})

	h := b.operationsHandler(healthLockTimeout, time.Minute,
		&Operators{byDoor: map[string]*passwd.File{"tcp": ops}})

	ask := func(user, path string) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.SetBasicAuth(user, "hunter2")
		r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, "tcp"))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	for _, tc := range []struct {
		user, path string
		want       int
		why        string
	}{
		{"scraper", "/metrics", http.StatusOK, "the route it names"},
		{"scraper", "/v1/operations/consumers", http.StatusForbidden,
			"a good credential that does not reach here - 401 would send somebody to check a correct password"},
		{"engineer", "/v1/operations/consumers", http.StatusOK, "under the prefix it names"},
		{"engineer", "/metrics", http.StatusForbidden, "and not the scrape path"},
		{"everything", "/metrics", http.StatusOK, "no routes named: every one"},
		{"everything", "/v1/operations/consumers", http.StatusOK, "the same"},
	} {
		if got := ask(tc.user, tc.path); got != tc.want {
			t.Errorf("%s GET %s answered %d, want %d - %s",
				tc.user, tc.path, got, tc.want, tc.why)
		}
	}

	// **A 403 says why, in the body.** Whoever meets this is looking at a
	// browser, not at the broker's log - and HTTP Basic gives them no way to
	// offer a different credential, so an empty page is a dead end at the
	// one moment somebody needs to be told something.
	r403 := httptest.NewRequest(http.MethodGet, "/v1/operations/consumers", nil)
	r403.SetBasicAuth("scraper", "hunter2")
	r403 = r403.WithContext(context.WithValue(r403.Context(), doorNameKey{}, "tcp"))
	w403 := httptest.NewRecorder()
	h.ServeHTTP(w403, r403)
	for _, want := range []string{"does not reach", "--passwd scope", "private window"} {
		if !strings.Contains(w403.Body.String(), want) {
			t.Errorf("the 403 body does not say %q, so a browser shows a blank page: %q",
				want, w403.Body.String())
		}
	}

	// A wrong password is still 401 wherever it is sent: the scope question
	// is only asked once the credential is good.
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.SetBasicAuth("scraper", "wrong")
	r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, "tcp"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("a wrong password answered %d, want 401", w.Code)
	}

	// And /health has no credential at all, so no scope can narrow it.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Errorf("/health answered %d with no credential, want 200: a probe cannot hold "+
			"one, which is the whole reason it is outside this", w.Code)
	}
}

// RFC 0005 "What the `/v1` routes answer with"
//
// **The version in the path is a promise, and a promise nothing checks is a
// hope.** `/v1` says these field names may gain company and may not change
// meaning or vanish - that is what a `/v2` would be for. The
// document named the routes' semantics in full and their shape nowhere, so
// a dashboard written against `behind` had nothing holding the next release
// to it.
//
// So the shape is written there and this is the tie. It reads the field
// names out of the document rather than repeating them here: a list in a
// test is a second place to keep in step, and the second copy is where the
// two drift apart.
func TestTheV1ShapeInRFC0005IsWhatTheBrokerSends(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs", "0005-operations.md"))
	if err != nil {
		t.Fatalf("read RFC 0005: %v", err)
	}
	src, err := os.ReadFile("health.go")
	if err != nil {
		t.Fatalf("read health.go: %v", err)
	}

	// The field names in the document's two JSON samples, which sit under
	// the heading that says they are the contract.
	section := string(md)
	if i := strings.Index(section, "### What the `/v1` routes answer with"); i >= 0 {
		section = section[i:]
	} else {
		t.Fatal("RFC 0005 no longer has the section that states the /v1 shape")
	}
	if j := strings.Index(section, "\n### "); j > 0 {
		section = section[:j]
	}
	// **Two of the four samples are cut out and held to their own source of
	// truth**, because they are different kinds of body. `consumers` and
	// `queues` are fixed shapes this package builds out of structs, so
	// their fields are json tags in health.go and the comparison below is
	// the right one. The other two are not: `/v1/operations/config` answers
	// with the configuration, whose keys are the schema's, and
	// `/v1/operations/acl` answers with a shape that lives in
	// internal/authz because two tools render it. Checking either against
	// this package would demand json tags for fields that are not here, and
	// cutting them out without checking them would leave the two newest
	// samples the only ones nothing holds to anything - which is how a
	// document comes to promise a field nobody sends.
	cut := func(from, to string) string {
		i := strings.Index(section, from)
		if i < 0 {
			return ""
		}
		rest := section[i:]
		j := strings.Index(rest, to)
		if j <= 0 {
			return ""
		}
		section = section[:i] + rest[j:]
		return rest[:j]
	}
	aclSample := cut("`GET /v1/operations/acl?user=<name>`", "`GET /v1/operations/config`")
	configSample := cut("`GET /v1/operations/config`", "`GET /v1/operations/consumers`")
	if aclSample == "" || configSample == "" {
		t.Fatal("RFC 0005 no longer carries both the /v1/operations/acl and the " +
			"/v1/operations/config samples under the section that states the /v1 shape")
	}
	checkConfigSample(t, configSample)
	checkACLSample(t, aclSample)

	// **Field names, not the names in the examples.** A body carrying a map
	// keyed by something the operator chose - a listener id, a channel
	// name - puts that name in the JSON exactly where a field goes, and a
	// pattern reading `"([a-z_]+)":` cannot tell them apart. It reported
	// `unix` as a promised field the first time a sample carried a
	// per-listener block. So the samples are decoded and walked, with the
	// levels that hold names skipped by position.
	documented := map[string]bool{}
	for _, key := range sampleFields(t, section, map[string]bool{"listeners": true}) {
		documented[key] = true
	}

	// The json tags the two handlers actually send.
	sent := map[string]bool{}
	tag := regexp.MustCompile("`" + `json:"([a-z_]+)`)
	for _, m := range tag.FindAllStringSubmatch(string(src), -1) {
		sent[m[1]] = true
	}

	// The counter this shape owes: a pattern that stopped matching passes
	// every comparison below by comparing nothing.
	if len(documented) < 10 || len(sent) < 10 {
		t.Fatalf("read %d documented fields and %d sent ones; the routes carry more "+
			"than that, so this test read the wrong file or the pattern broke",
			len(documented), len(sent))
	}

	for name := range sent {
		if !documented[name] {
			t.Errorf("the broker sends %q and RFC 0005 does not name it: a field "+
				"outside the written shape is one nothing holds the next release to",
				name)
		}
	}
	for name := range documented {
		if !sent[name] {
			t.Errorf("RFC 0005 promises %q and no handler sends it: a reader writing "+
				"a dashboard against this document gets nothing back", name)
		}
	}
	t.Logf("%d fields documented, %d sent", len(documented), len(sent))
}

// RFC 0005 "Who saguin thinks you are"
//
// **Three ways to be named, and one of them is only believed at the right
// door.** A password the broker checked; a client certificate an authority
// it names signed; and a header a reverse proxy set after doing the
// checking itself. The third is the one with a trust boundary: a header is
// whatever the caller typed, so it counts on a Unix socket or a loopback
// port - doors nothing off this machine can open - and is not read at all
// on a routable one, where it would be a bypass anybody could write.
//
// **And a name with no entry in the password file reaches /metrics and
// nothing else.** That is the point of naming a principal rather than
// letting a certificate be a skeleton key: being scraped needs no entry,
// and reading which device is behind does.
// An operations listener with no credential answers to an address and not to
// a name, and every listener that has one is untouched.
//
// **The hole is a browser, not the network.** Validation already confines an
// unauthenticated listener to loopback or a socket, which is unreachable from
// the network - and that is not the same as being unreachable from the
// internet. A page on another site whose domain re-resolves to 127.0.0.1 is
// same-origin to this listener and reads it through whatever browser has it
// open: the catalogue, the sessions, the ACL, the user list, the resolved
// configuration. The one thing that page cannot forge is the Host header,
// because a browser sends the name it was loaded by.
//
// **The controls are the point of this test.** A refusal proves nothing on
// its own: the same name must be served on a listener that has a credential,
// on the socket door, and on /health - because if the check reached any of
// those it would have broken a scraper, a reverse proxy or a liveness probe,
// each of which is a name arriving legitimately.
func TestAnUnauthenticatedListenerAnswersToAnAddressAndNotToAName(t *testing.T) {
	b := healthBroker(t)
	open := b.operationsHandler(healthLockTimeout, time.Minute, nil)

	ask := func(h http.Handler, host, path string, gated bool, cred [2]string) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		// httptest.NewRequest fills in a name of its own, so every case
		// here says which host it means rather than inheriting one.
		r.Host = host
		if cred[0] != "" {
			r.SetBasicAuth(cred[0], cred[1])
		}
		r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, gated))
		door := "tcp"
		if gated {
			door = "unix"
		}
		r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, door))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	none := [2]string{}

	for _, tc := range []struct {
		host, path string
		want       int
		why        string
	}{
		{"127.0.0.1:9090", "/metrics", http.StatusOK, "an address, which is what a caller on loopback sends"},
		{"localhost:9090", "/metrics", http.StatusOK, "the one name nobody else's DNS can move"},
		{"localhost", "/metrics", http.StatusOK, "the same, with the port left off"},
		{"[::1]:9090", "/metrics", http.StatusOK, "an address, bracketed as v6 arrives"},
		{"[::1]", "/metrics", http.StatusOK, "bracketed and portless, which SplitHostPort refuses"},
		{"", "/metrics", http.StatusOK, "no Host at all is not a browser: every browser sends one"},
		{"saguin.example", "/metrics", http.StatusForbidden, "a name, which is how a re-resolved domain arrives"},
		{"evil.example:9090", "/metrics", http.StatusForbidden, "the same, with a port"},
		{"saguin.example", "/v1/operations/sessions", http.StatusForbidden, "the routes that name devices, above all"},
		{"saguin.example", "/v1/operations/config", http.StatusForbidden, "and the resolved configuration"},
		{"saguin.example", "/health", http.StatusOK,
			"never authenticated, so never behind this - a probe cannot hold a credential " +
				"and carries nothing about this broker's contents"},
	} {
		if got := ask(open, tc.host, tc.path, false, none); got != tc.want {
			t.Errorf("Host %q asking %s answered %d, want %d - %s",
				tc.host, tc.path, got, tc.want, tc.why)
		}
	}

	// **The socket door is exempt**, for the reason it is exempt everywhere
	// else here: no browser can open one, and the proxy in front of it sends
	// whatever name it was asked for. Checking it would refuse the one
	// arrangement this broker offers for being reached by a real name.
	if got := ask(open, "saguin.example", "/v1/operations/consumers", true, none); got != http.StatusOK {
		t.Errorf("a name on the socket door answered %d, want 200 - a proxy in front "+
			"of a socket sends the name it was asked for, and refusing it would "+
			"break the one arrangement that exists for a real hostname", got)
	}

	// **The control that matters most.** A deployment with a credential is
	// reached by name every day - the viewer's backend on another machine,
	// Prometheus scraping by hostname - and none of it comes near this
	// branch. If this case ever fails, the check has escaped the one place
	// it belongs.
	ops := passwd.New(filepath.Join(t.TempDir(), "operations.passwd"))
	if err := ops.Set("oncall", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	closed := b.operationsHandler(healthLockTimeout, time.Minute,
		&Operators{byDoor: map[string]*passwd.File{"tcp": ops, "unix": ops}})
	for _, tc := range []struct {
		path string
		cred [2]string
		want int
		why  string
	}{
		{"/metrics", [2]string{"oncall", "hunter2"}, http.StatusOK,
			"a scraper calling a real deployment by its hostname"},
		{"/v1/operations/consumers", [2]string{"oncall", "hunter2"}, http.StatusOK,
			"the viewer's backend, on another machine, by name"},
		{"/metrics", none, http.StatusUnauthorized,
			"no credential where one is configured is still 401, not this 403"},
		{"/metrics", [2]string{"oncall", "wrong"}, http.StatusUnauthorized,
			"and a wrong one is still 401: the name is not what was wrong"},
	} {
		if got := ask(closed, "saguin.example", tc.path, false, tc.cred); got != tc.want {
			t.Errorf("Host %q asking %s with %q answered %d, want %d - %s",
				"saguin.example", tc.path, tc.cred[0], got, tc.want, tc.why)
		}
	}
}

// RFC 0005 "A scope naming no route saguin serves is a startup error".
//
// **Every route the operations listener serves can be named in a scope, and
// every route a scope may name is served.** The two lists lived apart - the
// handlers here, the vocabulary in internal/passwd so `--passwd` can check a
// file without a configuration - and two routes were added to one and not
// the other: `/v1/operations/sessions` and the refusal record's route could
// not be granted on their own, so an operator could not be given least
// privilege over either.
//
// Both directions, from the table the listener is built from: a route in
// the table must be in the vocabulary, and a route in the vocabulary must
// answer something other than 404 through the real handler. The one
// vocabulary entry that is not a route is `/v1/operations`, which scopes
// everything under it and must still reach something.
func TestEveryOperationsRouteCanBeScoped(t *testing.T) {
	b := healthBroker(t)
	h := b.operationsHandler(healthLockTimeout, time.Minute, nil)

	known := map[string]bool{}
	for _, r := range passwd.KnownRoutes {
		known[r] = true
	}

	served := map[string]bool{}
	for _, r := range b.operationsRoutes(nil, time.Minute) {
		path := strings.TrimSuffix(r.path, "/")
		served[path] = true
		if !known[path] {
			t.Errorf("%s is served and is not in passwd.KnownRoutes, so no operator can be "+
				"scoped to it on its own", path)
		}
	}
	if len(served) == 0 {
		t.Fatal("the route table is empty, so this compared nothing")
	}

	for r := range known {
		if r == "/v1/operations" {
			var under int
			for s := range served {
				if strings.HasPrefix(s, r+"/") {
					under++
				}
			}
			if under == 0 {
				t.Errorf("%s is a scope and no served route is under it", r)
			}
			continue
		}
		if !served[r] {
			t.Errorf("%s is in passwd.KnownRoutes and is not in the route table", r)
		}
		path := r
		if path == "/v1/operations/queues" {
			path += "/jobs" // the one route that names something after it
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code == http.StatusNotFound && !strings.Contains(w.Body.String(), "channel") {
			t.Errorf("%s may be named in a scope and the listener answers it 404: %s",
				path, w.Body.String())
		}
	}
	t.Logf("%d routes served, %d names a scope may use", len(served), len(known))
}

func TestAPrincipalIsBelievedOnlyWhereItCannotBeForged(t *testing.T) {
	b := healthBroker(t)
	ops := passwd.New(filepath.Join(t.TempDir(), "operations.passwd"))
	if err := ops.Set("oncall", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	ops.SetScopes("oncall", []string{"/metrics", "/v1/operations"})
	// **Both doors, one file**, because that is what a deployment writing
	// only broker.operations.password_file resolves to - and because the
	// cases below drive the socket and the port against the same operators,
	// which is the whole point of the header being believed at one and not
	// the other.
	h := b.operationsHandler(healthLockTimeout, time.Minute,
		&Operators{byDoor: map[string]*passwd.File{"tcp": ops, "unix": ops}})

	// local says the request arrived somewhere off-machine callers cannot
	// reach, which is what ServeOperations decides from the listener.
	ask := func(name, path string, local bool) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if name != "" {
			r.Header.Set(principalHeader, name)
		}
		r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, local))
		door := "tcp"
		if local {
			door = "unix"
		}
		r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, door))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	for _, tc := range []struct {
		name, path string
		local      bool
		want       int
		why        string
	}{
		{"oncall", "/metrics", true, http.StatusOK,
			"a proxy named an operator the file scopes"},
		{"oncall", "/v1/operations/consumers", true, http.StatusOK, "the same"},
		{"stranger", "/metrics", true, http.StatusOK,
			"named, no entry: being scraped needs none"},
		{"stranger", "/v1/operations/consumers", true, http.StatusForbidden,
			"named, no entry: which device is behind is not a thing an unscoped name reads"},
		{"oncall", "/metrics", false, http.StatusUnauthorized,
			"the same header on a routable port is a bypass anybody could write"},
		{"oncall", "/v1/operations/consumers", false, http.StatusUnauthorized, "the same"},
		{"", "/metrics", true, http.StatusUnauthorized,
			"no name at all, and a password file that wants one"},
		{"oncall\u00a0", "/v1/operations/consumers", true, http.StatusForbidden,
			"a name is taken exactly as the proxy states it, as the MQTT listener takes one: " +
				"\"oncall\u00a0\" is not oncall, and has no entry"},
	} {
		got := ask(tc.name, tc.path, tc.local)
		if got != tc.want {
			where := "a routable port"
			if tc.local {
				where = "a local door"
			}
			t.Errorf("principal %q on %s asking %s answered %d, want %d - %s",
				tc.name, where, tc.path, got, tc.want, tc.why)
		}
	}

	// **A credential that was offered and failed ends there**, rather than
	// falling through to whatever else names the caller. The sharp form is
	// a wrong password beside a header: it answered 200, so a rejected
	// credential was not rejected - reachable by accident by a scraper with
	// a stale password behind a proxy that also names it.
	t.Run("a failed password does not fall through to the header", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/operations/consumers", nil)
		r.SetBasicAuth("oncall", "wrong")
		r.Header.Set(principalHeader, "oncall")
		// The socket, where the header is believed: on the port it is
		// never read, so a 401 there would prove nothing about the order.
		r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, true))
		r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, "unix"))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("a wrong password beside a header answered %d, want 401: the "+
				"header outranked a credential the broker had just refused", w.Code)
		}
	})

	// **A certificate this door proved but could not name gets what a named
	// stranger gets, never more.**
	//
	// Treating it as an anonymous local reader gave it *every* route while
	// `CN=device-7` reached only /metrics - being unnamed was worth more
	// than being named. A certificate with a subject
	// alternative name and no Common Name, which is what many authorities
	// issue now, read the queue routes.
	t.Run("a verified certificate with no name is not nobody", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			cert *x509.Certificate
			want int
		}{
			{"a Common Name", &x509.Certificate{
				Subject: pkix.Name{CommonName: "oncall"}}, http.StatusOK},
			{"a DNS name and no Common Name", &x509.Certificate{
				DNSNames: []string{"nocn.example"}}, http.StatusForbidden},
			{"no name at all", &x509.Certificate{}, http.StatusForbidden},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodGet, "/v1/operations/consumers", nil)
				r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{tc.cert}}
				// The TLS port, whose file holds oncall: a certificate is
				// what names a caller there.
				r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, false))
				r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, "tcp"))
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != tc.want {
					t.Errorf("a certificate with %s answered %d, want %d",
						tc.name, w.Code, tc.want)
				}
			})
		}
	})

	// **The name itself, not only what it is allowed to do.** The subtests
	// above assert the outcome, and the outcome is 403 whether a SAN-only
	// certificate is named from its SAN or not named at all - so they
	// cannot tell the two apart, and deleting the fallback would pass them.
	// A principal that reaches a log line, an acl_file or a scope needs the
	// name to be right, not merely restrictive.
	t.Run("the name taken out of a certificate", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			cert *x509.Certificate
			want string
		}{
			{"the Common Name when there is one", &x509.Certificate{
				Subject:  pkix.Name{CommonName: "oncall"},
				DNSNames: []string{"ignored.example"}}, "oncall"},
			{"the first DNS name when there is not", &x509.Certificate{
				DNSNames: []string{"first.example", "second.example"}}, "first.example"},
			{"nothing when it carries neither", &x509.Certificate{}, ""},
			{"whitespace kept, as the MQTT listener keeps it", &x509.Certificate{
				Subject: pkix.Name{CommonName: "   "}}, "   "},
			{"edge spaces kept", &x509.Certificate{
				Subject: pkix.Name{CommonName: " ops "}}, " ops "},
			{"a DNS name's edge spaces kept", &x509.Certificate{
				DNSNames: []string{" first.example "}}, " first.example "},
		} {
			if got := certificateName(tc.cert); got != tc.want {
				t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
			}
		}
	})

	// **/health is never any of this.** A supervisor deciding whether to
	// restart the process cannot hold a credential, and it is the one route
	// that must answer while everything else is refusing.
	for _, local := range []bool{true, false} {
		if got := ask("", "/health", local); got != http.StatusOK {
			t.Errorf("/health answered %d with no credential (local=%v), want 200",
				got, local)
		}
	}
}

// **The trust boundary itself, rather than the rule that rests on it.**
//
// TestAPrincipalIsBelievedOnlyWhereItCannotBeForged puts the answer into
// the request and checks what the handler does with it. That leaves the
// question it depends on untested: whether *this* connection arrived
// somewhere off-machine callers cannot reach. Get that wrong and the rule
// above is exactly as strong as its input, which is to say not at all -
// the header becomes readable on a routable port and anyone who reaches it
// can name themselves.
//
// **The local address, not the remote one**, and that is the whole of it.
// A request proxied to a loopback listener has a loopback *remote* address
// as well, so asking the remote would answer yes for a caller from
// anywhere in the world. What cannot be chosen by the caller is which
// socket the broker opened.
func TestOnlyAGatedDoorIsTrusted(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local net.Addr
		want  bool
	}{
		{"a unix socket", &net.UnixAddr{Name: "/run/saguin/ops.sock", Net: "unix"}, true},
		// **A loopback port is not a gated door**, and these two rows are
		// the finding: it is unreachable from the network, which is not the
		// same as being reachable only by whoever the operator chose. Every
		// process on the machine may connect, so a header there named any
		// operator without their password.
		{"loopback v4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9090}, false},
		{"loopback v6", &net.TCPAddr{IP: net.IPv6loopback, Port: 9090}, false},
		{"a routable address", &net.TCPAddr{IP: net.IPv4(192, 168, 0, 134), Port: 9090}, false},
		{"the wildcard, which is reachable", &net.TCPAddr{IP: net.IPv4zero, Port: 9090}, false},
		{"a public address", &net.TCPAddr{IP: net.IPv4(203, 0, 113, 7), Port: 9090}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gatedDoor(fakeConn{local: tc.local}); got != tc.want {
				t.Errorf("gatedDoor(%s) = %v, want %v - a wrong answer here makes "+
					"X-Saguin-Principal readable on a door the operator does not "+
					"control who knocks at", tc.local, got, tc.want)
			}
		})
	}

	// A connection whose address is of no kind this knows is not local.
	// **Unknown is refused rather than assumed**, because the cost of the
	// two mistakes is not the same: a local reader wrongly asked for a
	// credential is an inconvenience, and a routable port wrongly trusting
	// a header is a bypass.
	if gatedDoor(fakeConn{local: &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)}}) {
		t.Error("an address kind this does not recognise was treated as gated")
	}
}

// fakeConn is a net.Conn that answers both addresses, and answers them
// differently on purpose.
//
// **The remote is always loopback and the local is what varies**, which is
// the arrangement a reverse proxy produces: it dials the broker from this
// machine, so every proxied caller looks local from the remote side
// whatever their real origin. A fake that left RemoteAddr unset would let
// gatedDoor be rewritten to ask the wrong one and fail with a nil
// dereference rather than with the reason.
type fakeConn struct {
	net.Conn
	local net.Addr
}

func (c fakeConn) LocalAddr() net.Addr { return c.local }
func (c fakeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000}
}

// checkConfigSample holds RFC 0005's `/v1/operations/config` sample to the
// configuration schema, which is the only thing that can hold it.
//
// **Every key in that sample is either a configuration key or a name.**
// The keys come from `yaml:"…"` tags in internal/config, plus the one field
// Report adds; the names are what the operator called a channel or a
// provider, and they are skipped by position - one level under `channels`
// and under `providers` - rather than by being listed here, because a list
// of the example's own values is the second copy this file's other check
// exists to avoid.
//
// What it catches is a sample naming a key the schema does not have, which
// is a reader writing a configuration against this document and having it
// refused at startup.
func checkConfigSample(t *testing.T, sample string) {
	t.Helper()

	blocks := jsonBlocks(sample)
	if len(blocks) == 0 {
		t.Fatal("the /v1/operations/config sample carries no JSON block")
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(blocks[0]), &doc); err != nil {
		t.Fatalf("the /v1/operations/config sample is not JSON, so a reader copying it "+
			"into a test gets a parse error: %v", err)
	}

	schema, err := os.ReadFile(filepath.Join("..", "config", "config.go"))
	if err != nil {
		t.Fatalf("read the configuration schema: %v", err)
	}
	bridge, err := os.ReadFile(filepath.Join("..", "config", "bridge.go"))
	if err != nil {
		t.Fatalf("read the bridge schema: %v", err)
	}
	keys := map[string]bool{
		// The one field Report adds that no yaml tag carries, because the
		// provenance is a comment in the document a configuration round
		// trips through and a field only in the report.
		"configured_in": true,
	}
	tag := regexp.MustCompile("`" + `yaml:"([a-z_]+)`)
	for _, m := range tag.FindAllStringSubmatch(string(schema)+string(bridge), -1) {
		keys[m[1]] = true
	}
	if len(keys) < 30 {
		t.Fatalf("read %d configuration keys out of the schema, which is too few to be "+
			"checking anything: the pattern broke or the file moved", len(keys))
	}

	checked := 0
	var walk func(v any, named bool)
	walk = func(v any, named bool) {
		m, ok := v.(map[string]any)
		if !ok {
			return
		}
		for k, child := range m {
			if named {
				// A channel or provider name, which is the operator's word
				// and not the schema's. Its contents are the schema's
				// again.
				walk(child, false)
				continue
			}
			checked++
			if !keys[k] {
				t.Errorf("the /v1/operations/config sample names %q, which is not a key the "+
					"configuration schema has: a reader building a file from this document "+
					"has it refused at startup", k)
			}
			walk(child, k == "channels" || k == "providers")
		}
	}
	walk(doc, false)

	if checked < 8 {
		t.Fatalf("only %d keys in the sample were checked, which is fewer than it carries: "+
			"this walk is not reaching into it", checked)
	}

	// **Every section the route serves is named in the document.** The
	// vocabulary is closed - a name outside it is a 400 - so a section
	// added to the table and not written down here is one no operator can
	// discover, and the refusal that lists them is the only other place it
	// appears.
	for name := range configSections {
		if !strings.Contains(sample, "`"+name+"`") {
			t.Errorf("`?section=%s` is served and RFC 0005 does not name it: the vocabulary "+
				"is closed, so a section nobody documents is one nobody can ask for", name)
		}
	}
	t.Logf("%d keys in the /v1/operations/config sample checked against the schema, "+
		"and %d section names against the document", checked, len(configSections))
}

// checkACLSample holds RFC 0005's `/v1/operations/acl` sample to the json
// tags in internal/authz, which is where that body's shape lives.
//
// **It is there rather than here because two doors answer with it** -
// `saguin --acl --output json` and this route - and a shape rendered in two
// places is two answers to "what may this client do", which is the one
// question an operator must never get two of. So the check follows the
// shape rather than the handler.
func checkACLSample(t *testing.T, sample string) {
	t.Helper()

	src, err := os.ReadFile(filepath.Join("..", "authz", "authz.go"))
	if err != nil {
		t.Fatalf("read the authorization package: %v", err)
	}
	sent := map[string]bool{}
	for _, m := range regexp.MustCompile("`"+`json:"([a-z_]+)`).FindAllStringSubmatch(string(src), -1) {
		sent[m[1]] = true
	}
	if len(sent) < 6 {
		t.Fatalf("read %d json fields out of internal/authz, which is fewer than the "+
			"explanation carries: the pattern broke or the file moved", len(sent))
	}

	documented := map[string]bool{}
	for _, m := range regexp.MustCompile(`"([a-z_]+)":`).FindAllStringSubmatch(sample, -1) {
		documented[m[1]] = true
	}
	if len(documented) < 6 {
		t.Fatalf("read %d fields out of the /v1/operations/acl sample, which is fewer than "+
			"it carries: this check is not reading the sample it thinks it is", len(documented))
	}
	for name := range documented {
		if !sent[name] {
			t.Errorf("RFC 0005 promises %q on /v1/operations/acl and internal/authz renders "+
				"no such field: a reader writing against this document gets nothing back", name)
		}
	}
	for name := range sent {
		if !documented[name] {
			t.Errorf("internal/authz renders %q and RFC 0005 does not name it on "+
				"/v1/operations/acl: a field outside the written shape is one nothing holds "+
				"the next release to", name)
		}
	}
	t.Logf("%d fields in the /v1/operations/acl sample checked against internal/authz", len(documented))
}

// jsonBlocks is every fenced JSON block in a slice of the document.
func jsonBlocks(md string) []string {
	var out []string
	rest := md
	for {
		open := strings.Index(rest, "```json")
		if open < 0 {
			return out
		}
		rest = rest[open+len("```json"):]
		end := strings.Index(rest, "```")
		if end < 0 {
			return out
		}
		out = append(out, rest[:end])
		rest = rest[end:]
	}
}

// sampleFields is every field name in a slice of the document's JSON
// samples, with one level under each of `named` treated as data.
//
// **A sample is decoded rather than pattern-matched**, because the two
// things that look identical in JSON text - a field and a map key somebody
// chose - are told apart by position and by nothing else. `"unix":` under
// `listeners` is a listener id; `"users":` beside it is a field.
func sampleFields(t *testing.T, md string, named map[string]bool) []string {
	t.Helper()
	var out []string
	blocks := jsonBlocks(md)
	if len(blocks) == 0 {
		t.Fatal("no JSON samples found where the /v1 shape is stated")
	}
	for _, b := range blocks {
		var doc any
		if err := json.Unmarshal([]byte(b), &doc); err != nil {
			t.Fatalf("a /v1 sample is not JSON, so a reader copying it into a test gets a "+
				"parse error: %v\n%s", err, b)
		}
		var walk func(v any, isName bool)
		walk = func(v any, isName bool) {
			switch x := v.(type) {
			case map[string]any:
				for k, child := range x {
					if !isName {
						out = append(out, k)
					}
					walk(child, named[k])
				}
			case []any:
				for _, e := range x {
					walk(e, isName)
				}
			}
		}
		walk(doc, false)
	}
	return out
}

// ValidName is the one rule every source of an identity asks: valid UTF-8,
// no U+0000, and none of U+0001-U+001F or U+007F-U+009F - the set mosquitto's
// mosquitto_validate_utf8 and EMQX's is_mqtt_safe_utf8 agree on. The edges
// of each range are in the table, so a range narrowed by one fails it.
func TestValidNameRefusesExactlyTheControlCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"device-7", true},
		{"dispositivo-ñ", true},
		{"设备", true},
		{"dev ice", true},     // U+0020, the first printable
		{"dev~ice", true},     // U+007E, the last before DEL
		{"dev ice", true},     // U+00A0, the first after C1
		{"dev\x00ice", false}, // U+0000
		{"dev\x01ice", false}, // U+0001, the first C0
		{"dev\x1fice", false}, // U+001F, the last C0
		{"dev\r\nice", false},
		{"dev\x7fice", false},   // DEL
		{"dev\u0080ice", false}, // U+0080, the first C1
		{"dev\u009fice", false}, // U+009F, the last C1
		{"dev\xffice", false},   // not UTF-8
	} {
		if got := ValidName(tc.name); got != tc.want {
			t.Errorf("ValidName(%q) = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// **No door name means refuse**, however the request context was built -
// not only when connContext marked it. A context built anywhere else
// carries no marker at all, and a check that refused only a marked one
// served such a request as an unauthenticated door. The configuration is
// the most permissive there is - no password file, a gated socket - so
// anything short of refusing a nameless request answers 200 here.
func TestARequestWithNoDoorNameIsRefused(t *testing.T) {
	b := healthBroker(t)
	h := b.operationsHandler(healthLockTimeout, time.Minute, nil)
	ask := func(path string, door *string) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, true))
		if door != nil {
			r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, *door))
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	empty, unix := "", "unix"
	asked := 0
	for _, path := range []string{"/v1/operations/consumers", "/metrics"} {
		for _, tc := range []struct {
			why  string
			door *string
		}{{"no door name", nil}, {"an empty door name", &empty}} {
			asked++
			if got := ask(path, tc.door); got != http.StatusUnauthorized {
				t.Errorf("%s with %s answered %d, want 401", path, tc.why, got)
			}
		}
		// Control: the same request named as the socket is served, so the
		// 401 above is the missing name and not the route or the door.
		if got := ask(path, &unix); got != http.StatusOK {
			t.Errorf("%s on the socket answered %d, want 200", path, got)
		}
	}
	if got := ask("/health", nil); got != http.StatusOK {
		t.Errorf("/health with no door name answered %d, want 200", got)
	}
	if asked != 4 {
		t.Fatalf("asked %d nameless requests, want 4", asked)
	}
}

// A served connection whose door tag is lost is refused on every route but
// /health, rather than answered as a door with no credential.
func TestUntaggedOperationsConnectionFailsClosed(t *testing.T) {
	b := healthBroker(t)
	h := b.operationsHandler(healthLockTimeout, time.Minute, nil)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctx := connContext(context.Background(), c1) // no namedListener: no tag
	for path, want := range map[string]int{"/metrics": http.StatusUnauthorized, "/health": http.StatusOK} {
		r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("untagged %s: %d, want %d", path, w.Code, want)
		}
	}
	// Control: the same request on a tagged connection is served.
	ctx = connContext(context.Background(), &namedConn{Conn: c1, name: "tcp"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	r.Host = "127.0.0.1:9090"
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("tagged /metrics: %d, want 200", w.Code)
	}
}
