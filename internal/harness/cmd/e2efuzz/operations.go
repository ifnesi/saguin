package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"maps"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The run's operations calls (design.md, Operations, HTTP and its Who may
// call; Flows, Transport setup). A broker the run starts has three
// operations listeners (options): plain TCP on a loopback address, TLS
// asking for client certificates without requiring them, and a unix socket
// with proxy_protocol, each with the run's operators' file. At each
// checkpoint (checked) the run makes opsPerCheck calls, one listener after
// another, each drawn from a generator of its own, so the clients' draws
// stay as they were: a route, a method, a Host, and, but on /health, a
// certificate or a verified PROXY name and HTTP Basic credentials, right or
// wrong. Each answer is checked against the status expectOps predicts from
// the design: the route first, then the credential by precedence and its
// scope, then the method, and then the answer itself.

// operator is a user of the run's operators' file, its password, and its
// scope, the line's third field, "" for none.
type operator struct{ user, password, scope string }

// operators are the users of the run's operators' file: root, reaching
// every route; prom, scoped to /metrics; and audit, to /v1/operations,
// which reaches /v1/operations/acl at its / boundary.
var operators = []operator{{"root", "r7", ""}, {"prom", "p4", "/metrics"}, {"audit", "a2", "/v1/operations"}}

// operatorsFileName is the run's operators' file and opsSocketName its
// unix operations listener, in the broker's folder.
const (
	operatorsFileName = "operators"
	opsSocketName     = "ops.sock"
)

// writeOperators writes the run's operators' file in dir: one line per
// operator, user:hash, then :scope where it has one (design.md, Who may
// call: "prometheus:$7$…:/metrics").
func writeOperators(dir string) error {
	var lines strings.Builder
	for _, o := range operators {
		salt := make([]byte, 64)
		if _, err := rand.Read(salt); err != nil {
			return err
		}
		hash, err := harness.SevenHash(o.password, loginIterations, salt)
		if err != nil {
			return err
		}
		fmt.Fprintf(&lines, "%s:%s", o.user, hash)
		if o.scope != "" {
			fmt.Fprintf(&lines, ":%s", o.scope)
		}
		lines.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(dir, operatorsFileName), []byte(lines.String()), 0o600)
}

// opsNames are the names a call's client certificate or verified PROXY
// header carries: each operator's, stranger, in no entry, and one the
// certificate-name rule refuses, holding a control character.
var opsNames = []string{"root", "prom", "audit", "stranger", "bad\x01name"}

// The ways an operations call reaches the broker, one after another.
const (
	opsPlain = iota
	opsSecure
	opsProxied
	opsWays
)

// opsPaths are the paths a call asks for: the routes this build serves
// (/health, /metrics, the acl route and the state views' statePaths) and
// three that are no route.
var opsPaths = append([]string{"/health", "/metrics", "/v1/operations/acl?user=ann", "/nowhere", "/v1/operations/nowhere", "/metricsx"},
	statePaths...)

// opsHosts are a call's Host headers: "" for the target's own (the
// listener's address, or localhost on a unix socket), IP literals and
// localhost, and names.
var opsHosts = []string{"", "localhost", "127.0.0.1:9", "[::1]", "example.com", "localhost.example.com"}

// opsMethods are the methods a call uses other than GET.
var opsMethods = []string{http.MethodHead, http.MethodPost, http.MethodDelete}

// opsPerCheck is how many operations calls a checkpoint makes.
const opsPerCheck = 6

// operations is what the run needs to call a broker's operations
// listeners, and what it counts of the calls.
type operations struct {
	rng     *mathrand.Rand
	targets [opsWays]harness.OpsTarget // each way's listener, without credentials
	secure  *tls.Config                // trusting the run's authority
	certs   map[string]*testkit.Issued // a client certificate per opsNames
	// calls counts the calls by way, and statuses the answers by status.
	calls    [opsWays]int
	statuses map[int]int
	// config is the broker's configuration file, which config's
	// configured_in names (views.go).
	config string
}

// newOperations is the calls of the run seeded with seed to b's operations
// listeners, the run's authority issuing a certificate for each of
// opsNames, secure trusting it.
func newOperations(seed uint64, b *harness.Broker, authority *testkit.Authority, secure *tls.Config) (*operations, error) {
	o := &operations{rng: mathrand.New(mathrand.NewPCG(^seed, seed)), secure: secure,
		certs: map[string]*testkit.Issued{}, statuses: map[int]int{}, config: b.Config}
	o.targets[opsPlain] = harness.OpsTarget{Network: "tcp", Address: b.OpsAddr}
	o.targets[opsSecure] = harness.OpsTarget{Network: "tcp", Address: b.Ops[0]}
	o.targets[opsProxied] = harness.OpsTarget{Network: "unix", Address: b.Ops[1]}
	for _, name := range opsNames {
		issued, err := authority.Issue(&testkit.Leaf{CommonName: name})
		if err != nil {
			return nil, err
		}
		o.certs[name] = issued
	}
	return o, nil
}

// opsDraw is one call: its way, the name its certificate or PROXY header
// carries, "" for none, its Basic credentials, user "" for none, its Host,
// method and path.
type opsDraw struct {
	way                  int
	name, user, password string
	host, method, path   string
}

func (d *opsDraw) String() string {
	return fmt.Sprintf("%s %s over %s, Host %q, name %q, Basic %q:%q", d.method, d.path,
		[...]string{"TCP", "TLS", "a unix socket behind a PROXY header"}[d.way], d.host, d.name, d.user, d.password)
}

// route is d's path without its query.
func (d *opsDraw) route() string {
	route, _, _ := strings.Cut(d.path, "?")
	return route
}

// draw is the next call, on way.
func (o *operations) draw(way int) opsDraw {
	r := o.rng
	d := opsDraw{way: way, method: http.MethodGet, path: opsPaths[r.IntN(len(opsPaths))], host: opsHosts[r.IntN(len(opsHosts))]}
	if r.IntN(4) == 0 {
		d.method = opsMethods[r.IntN(len(opsMethods))]
	}
	if d.route() == "/health" {
		return d // the design leaves a credential on /health open
	}
	if way != opsPlain && r.IntN(2) == 0 {
		d.name = opsNames[r.IntN(len(opsNames))]
	}
	if r.IntN(2) == 0 {
		op := operators[r.IntN(len(operators))]
		d.user, d.password = op.user, op.password
		switch r.IntN(6) {
		case 0:
			d.password += "x"
		case 1:
			d.user = "nobody"
		}
	}
	return d
}

// target is d's way, carrying d's name: a certificate presented over TLS,
// or a PROXY header's verified Common Name. A PROXY header names a loopback
// client, so that a call with no credential is a local one however the
// header's address is read.
func (o *operations) target(d *opsDraw) harness.OpsTarget {
	t := o.targets[d.way]
	switch d.way {
	case opsSecure:
		t.TLS = o.secure.Clone()
		if cert := o.certs[d.name]; cert != nil {
			t.TLS.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert.Certificate, nil }
		}
	case opsProxied:
		var records [][]byte
		if d.name != "" {
			records = append(records, harness.ProxyVerifiedCN(d.name))
		}
		t.Proxy = harness.ProxyHeader(harness.ProxyProxy, harness.ProxyTCP4,
			netip.MustParseAddrPort("127.0.0.1:40000"), netip.MustParseAddrPort("127.0.0.1:9090"), records...)
	}
	return t
}

// expectOps is the statuses d may get (design.md, Operations, HTTP: "The
// broker decides the route first. An unknown route gets 404, whatever the
// credential. For a known route, the broker checks the credential, then the
// method. Any other method gets 405. Credential checks follow Who may
// call."; Who may call: "/health needs no credential on any operations
// listener"; "Every route takes GET, and /health also takes HEAD").
func expectOps(d *opsDraw) []int {
	route := d.route()
	if !slices.Contains([]string{"/health", "/metrics", "/v1/operations/acl"}, route) && !stateRoute(route) {
		return []int{http.StatusNotFound}
	}
	if route != "/health" {
		scope, refused := identify(d)
		if refused != nil {
			return refused
		}
		if scope != nil && !slices.ContainsFunc(scope, func(p string) bool { return route == p || strings.HasPrefix(route, p+"/") }) {
			return []int{http.StatusForbidden} // "A good credential outside its scope gets 403"
		}
	}
	switch {
	case d.method == http.MethodGet && stateRoute(route):
		return []int{stateAnswer(d.path)}
	case d.method == http.MethodGet || (route == "/health" && d.method == http.MethodHead):
		return []int{http.StatusOK}
	}
	return []int{http.StatusMethodNotAllowed}
}

// identify is the scope of who d proves to be, nil for every route, or the
// statuses it gets where it is refused (design.md, Who may call):
// "Identity sources are tried by precedence: verified client certificate,
// verified PROXY certificate identity, then HTTP Basic. The selected source
// is used alone. Failure does not fall back"; a name the certificate-name
// rule refuses "gets 403"; a certificate or PROXY identity "must name an
// entry in that file, or it gets 403. It receives that entry's scope"; "A
// rejected credential gets 401"; and "A
// request with no credential reaches every route only on a loopback
// address or Unix socket, and only when its Host is an IP literal or
// localhost. Otherwise it gets 403": every listener of the run is on a
// loopback address or a unix socket.
func identify(d *opsDraw) (scope []string, refused []int) {
	switch {
	case d.name != "":
		if strings.ContainsFunc(d.name, unicode.IsControl) {
			return nil, []int{http.StatusForbidden}
		}
		i := slices.IndexFunc(operators, func(o operator) bool { return o.user == d.name })
		if i < 0 {
			return nil, []int{http.StatusForbidden}
		}
		return operators[i].prefixes(), nil
	case d.user != "":
		i := slices.IndexFunc(operators, func(o operator) bool { return o.user == d.user && o.password == d.password })
		if i < 0 {
			return nil, []int{http.StatusUnauthorized}
		}
		return operators[i].prefixes(), nil
	case localHost(d.host):
		return nil, nil
	}
	return nil, []int{http.StatusForbidden}
}

// prefixes is o's scope, its route prefixes, nil for none: every route.
func (o operator) prefixes() []string {
	if o.scope == "" {
		return nil
	}
	return strings.Split(o.scope, ",")
}

// localHost reports whether host, a Host header, "" for the target's own,
// is an IP literal or localhost, with or without a port.
func localHost(host string) bool {
	if host == "" {
		return true // the listener's address, an IP literal, or localhost on a unix socket
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if host == "localhost" {
		return true
	}
	_, err := netip.ParseAddr(host)
	return err == nil
}

// operate makes a checkpoint's operations calls, each listener in turn,
// checking each answer (checkOps), then reads and checks every state view
// (checkViews); a broker given with -addr has none.
func (f *fuzzer) operate() error {
	o := f.ops
	if o == nil {
		return nil
	}
	for range opsPerCheck {
		d := o.draw((o.calls[opsPlain] + o.calls[opsSecure] + o.calls[opsProxied]) % opsWays)
		want := expectOps(&d)
		f.note("operations call: %s: want %v", &d, want)
		a, err := o.target(&d).Do(&harness.OpsCall{Method: d.method, Path: d.path, Host: d.host, User: d.user, Password: d.password})
		if err != nil {
			return fmt.Errorf("operations call %s: %w", &d, err)
		}
		if err := checkOps(&d, want, &a); err != nil {
			return err
		}
		o.calls[d.way]++
		o.statuses[a.Status]++
	}
	return f.checkViews()
}

// checkOps checks a, d's answer: its status one of want; an error in plain
// text and a 401 with WWW-Authenticate (design.md, Operations, HTTP:
// "Errors are plain text"; Who may call: "A rejected credential gets 401
// with WWW-Authenticate"); and a 200 the route's answer: /health's
// {"status":"ok"} (but for HEAD, which has no body), /metrics' Prometheus
// text, the acl route's JSON ("Answers are JSON, except for /metrics,
// which returns Prometheus text").
func checkOps(d *opsDraw, want []int, a *harness.OpsAnswer) error {
	switch {
	case !slices.Contains(want, a.Status):
		return fmt.Errorf("operations call %s: %d %q; want %v", d, a.Status, a.Body, want)
	case a.Status >= http.StatusBadRequest && a.MediaType != "text/plain":
		return fmt.Errorf("operations call %s: %d as %q; want an error in plain text", d, a.Status, a.MediaType)
	case a.Status == http.StatusUnauthorized && a.Challenge == "":
		return fmt.Errorf("operations call %s: 401 with no WWW-Authenticate header", d)
	case a.Status != http.StatusOK || d.method == http.MethodHead:
		return nil
	}
	switch d.route() {
	case "/health":
		var got map[string]any
		if err := json.Unmarshal([]byte(a.Body), &got); err != nil || a.MediaType != "application/json" || !maps.Equal(got, map[string]any{"status": "ok"}) {
			return fmt.Errorf("operations call %s: 200 %s %q; want {\"status\":\"ok\"}", d, a.MediaType, a.Body)
		}
	case "/metrics":
		if a.MediaType != "text/plain" || !strings.Contains(a.Body, "saguin_ready") {
			return fmt.Errorf("operations call %s: 200 %s of %d bytes; want the Prometheus text of every series", d, a.MediaType, len(a.Body))
		}
	default:
		if a.MediaType != "application/json" {
			return fmt.Errorf("operations call %s: 200 %s; want application/json", d, a.MediaType)
		}
		var view map[string]json.RawMessage
		if err := json.Unmarshal([]byte(a.Body), &view); err != nil {
			return fmt.Errorf("operations call %s: 200 %q; want a JSON object: %w", d, a.Body, err)
		}
		if view == nil {
			return fmt.Errorf("operations call %s: 200 %q; want a JSON object", d, a.Body)
		}
		if d.route() != "/v1/operations/acl" {
			return nil // a state view, read whole at each checkpoint (views.go)
		}
		if err := harness.CheckACLAnswer([]byte(a.Body)); err != nil {
			return fmt.Errorf("operations call %s: 200 %q: %w", d, a.Body, err)
		}
	}
	return nil
}
