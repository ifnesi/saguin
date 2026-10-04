package main_test

// RFC 0005's nginx block, extracted and driven.
//
// **Three times in two days the block that was driven and the block that
// was published differed by the thing the driving had found**: once with
// the door open (no `ssl_verify_client`, so `/v1` served anybody), once
// shut (`proxy_pass …:/`, so `/v1` answered 404 to everybody), and once a
// missing `password_file` that let a nameless caller through. Each was
// found by running the fence rather than by anything here, and
// each was fixed by editing prose - which is the shape this project's own
// rule says to stop with a check.
//
// Two halves, and the split is deliberate. **The first needs no Docker and
// runs for everybody**: it proves that what a driver runs differs from what
// a reader copies only in paths and ports. That is the half the three
// defects would have failed. **The second needs Docker** and drives the
// rows RFC 0005 claims, behind `make nginx`, because a container and a
// two-second start are not something `make check` should pay on every run.

import (
	"crypto/tls"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rfc0005 is the document the block is published in.
var rfc0005 = filepath.Join("..", "..", "docs", "rfcs", "0005-operations.md")

// substitutions turn the published block into one this machine can run.
//
// **Paths and ports only.** Nothing here may change what a directive means:
// that is the whole distinction the three defects fell on, and the reversal
// below is what enforces it rather than good intentions.
var substitutions = [][2]string{
	{"/etc/saguin/tls/cert.pem", "CERTS/broker.pem"},
	{"/etc/saguin/tls/key.pem", "CERTS/broker-key.pem"},
	{"/etc/saguin/tls/clients-ca.pem", "CERTS/clients-ca.pem"},
	{"/etc/saguin/tls/operators-ca.pem", "CERTS/operators-ca.pem"},
	{"/etc/saguin/metrics.htpasswd", "CERTS/metrics.htpasswd"},
	{"unix:/run/saguin/saguin.sock", "unix:/sock/mqtt.sock"},
	{"unix:/run/saguin/operations.sock", "unix:/sock/ops.sock"},
	{"listen 8883 ssl;", "listen 28883 ssl;"},
	{"listen 8443 ssl;", "listen 28443 ssl;"},
	{"listen 9443 ssl;", "listen 29443 ssl;"},
	{"http://127.0.0.1:9090", "http://127.0.0.1:29100"},
	{"http://127.0.0.1:8083", "http://127.0.0.1:28083"},
}

// nginxBlock is the fenced nginx configuration in RFC 0005, under
// "Behind a reverse proxy". It lived in README.md until the README was
// shortened to an introduction; the RFC is where the arrangement is
// specified, so the RFC is what is driven.
func nginxBlock(t *testing.T) string {
	t.Helper()
	doc, err := os.ReadFile(rfc0005)
	if err != nil {
		t.Fatalf("read RFC 0005: %v", err)
	}
	s := string(doc)
	i := strings.Index(s, "\nstream {")
	if i < 0 {
		t.Fatal("RFC 0005 has no nginx stream block; this test is about one")
	}
	j := strings.Index(s[i:], "\n```")
	if j < 0 {
		t.Fatal("the nginx block in RFC 0005 is not closed")
	}
	return s[i+1 : i+j]
}

// runnable applies the substitutions, and proves it applied nothing else.
func runnable(t *testing.T, block, certs string) string {
	t.Helper()
	out := block
	for _, s := range substitutions {
		out = strings.ReplaceAll(out, s[0], s[1])
	}
	// **Reverse every substitution and require the original back.** A
	// change that is not one of these - a directive edited to make a run
	// succeed, which is what happened three times - survives the reversal
	// and shows up here rather than in a deployment.
	back := out
	for _, s := range substitutions {
		back = strings.ReplaceAll(back, s[1], s[0])
	}
	if back != block {
		t.Fatalf("what would be run is not what RFC 0005 publishes.\n"+
			"Only paths and ports may be substituted; something changed what a "+
			"directive means.\n--- published ---\n%s\n--- reversed ---\n%s",
			block, back)
	}
	return strings.ReplaceAll(out, "CERTS", certs)
}

// **The half that needs nothing but the repository**, so every run of the
// suite pays it and every one of the three defects would have met it.
func TestTheNginxBlockIsFitToRun(t *testing.T) {
	block := nginxBlock(t)
	if got := runnable(t, block, "/certs"); strings.Contains(got, "/etc/saguin") {
		t.Errorf("a deployment path survived substitution, so a run would read a "+
			"file this machine does not have: %s", got)
	}

	// **The directives the three defects were, asserted in the server that
	// needs them.** A block can be syntactically perfect and open, or
	// perfect and shut, and `nginx -t` cannot tell either.
	//
	// **Asked of the right server, not of the block.** Looking for
	// `ssl_verify_client` anywhere passed while the server that needs it had
	// none, because the stream block has one too - a check matching
	// something else, which is the failure this shape is prone to rather
	// than a wrong answer.
	principalServer := ""
	for _, srv := range strings.Split(block, "server {") {
		if strings.Contains(srv, "X-Saguin-Principal") {
			principalServer = srv
		}
	}
	if principalServer == "" {
		t.Fatal("no server in the nginx block sets X-Saguin-Principal, so nothing " +
			"there names the caller to saguin at all")
	}
	for _, want := range []struct{ directive, why string }{
		{"ssl_verify_client", "without it nginx asks this caller for no certificate, " +
			"sends an empty principal header, and /v1 serves somebody it cannot name"},
		{"ssl_client_certificate", "the authority those certificates are checked against"},
		{"operations.sock:/v1/", "a proxy_pass carrying a URI replaces the matched " +
			"prefix, so a bare / sends /v1/operations/consumers on as " +
			"/operations/consumers and saguin answers 404 to everything"},
	} {
		if !strings.Contains(principalServer, want.directive) {
			t.Errorf("the server that names the caller does not carry %q: %s",
				want.directive, want.why)
		}
	}

	// **The configuration printed beneath the block**, which is the other
	// half of the arrangement: the socket the block proxies to, and the
	// operators file that refuses a nameless caller. Published without the
	// second, /v1 answered 200 to a request carrying nothing at all.
	//
	// Read out of the yaml fence that follows, not out of the rest of the
	// document: "password_file" occurs in prose further down, and looking
	// for it anywhere after the block passed while the fence had none.
	doc, err := os.ReadFile(rfc0005)
	if err != nil {
		t.Fatal(err)
	}
	rest := string(doc)[strings.Index(string(doc), "\nstream {"):]
	open := strings.Index(rest, "```yaml")
	if open < 0 {
		t.Fatal("no configuration is printed beneath the nginx block, so a reader " +
			"has the proxy and no broker to point it at")
	}
	fence := rest[open:]
	fence = fence[:strings.Index(fence[len("```yaml"):], "```")+len("```yaml")]
	for _, want := range []struct{ key, why string }{
		{"operations.sock", "the block proxies /v1 to a socket, and this is where " +
			"the broker is told to open one"},
		{"password_file", "without it a request carrying no name is read as a local " +
			"reader with no credential to give, and reaches every route - which " +
			"is what a caller presenting no certificate to nginx becomes"},
	} {
		if !strings.Contains(fence, want.key) {
			t.Errorf("the configuration beneath the nginx block does not name %q: %s",
				want.key, want.why)
		}
	}
}

// **The half that drives it**, behind `make nginx`, because it wants Docker
// and about ten seconds.
//
// It asserts the rows RFC 0005 prints beside the block. Those rows are
// the ones the three defects got wrong, and every one of them looked fine
// in the file.
func TestTheNginxBlockAnswersWhatRFC0005Says(t *testing.T) {
	if os.Getenv("SAGUIN_NGINX") == "" {
		t.Skip("set SAGUIN_NGINX=1, or run `make nginx`: this drives a container")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not on PATH, and it is the whole point of this test")
	}

	// A socket path short enough to bind: sun_path is 108 bytes, and
	// t.TempDir() embeds the test's name.
	dir, err := os.MkdirTemp("/tmp", "sgn")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// **nginx's workers do not run as root**, and MkdirTemp makes 0700. The
	// master reads the configuration as root and a worker reads the
	// password file, so a private directory answers /metrics with 500 while
	// every route needing no file passes - which is exactly how this first
	// presented.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "sock")
	if err := os.MkdirAll(sock, 0o777); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(sock, 0o777)

	bin := filepath.Join(dir, "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").
		CombinedOutput(); err != nil {
		t.Skipf("the broker would not build here: %v\n%s", err, out)
	}

	certs, err := filepath.Abs(filepath.Join("..", "..", "examples", "support", "certs"))
	if err != nil {
		t.Fatal(err)
	}
	copyIn := func(from, to string) {
		t.Helper()
		b, err := os.ReadFile(from)
		if err != nil {
			t.Skipf("the shipped certificates are not here: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, to), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"broker.pem", "broker-key.pem", "ca.pem",
		"device-7.pem", "device-7-key.pem"} {
		copyIn(filepath.Join(certs, f), f)
	}
	// The block names two client authorities; both are the shipped one here.
	for _, f := range []string{"clients-ca.pem", "operators-ca.pem"} {
		copyIn(filepath.Join(dir, "ca.pem"), f)
	}
	// nginx reads {PLAIN}, which needs no hashing tool on the machine.
	if err := os.WriteFile(filepath.Join(dir, "metrics.htpasswd"),
		[]byte("prometheus:{PLAIN}prom\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The operators file RFC 0005 says is not optional: the scraper, and
	// the certificate Common Name nginx forwards.
	ops := filepath.Join(dir, "operations.passwd")
	for _, a := range [][]string{
		{"--passwd", "add", ops, "prometheus", "prom"},
		{"--passwd", "scope", ops, "prometheus", "/metrics"},
		{"--passwd", "add", ops, "device-7", "unused"},
		{"--passwd", "scope", ops, "device-7", "/metrics,/v1/operations"},
	} {
		if out, err := exec.Command(bin, a...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v\n%s", a, err, out)
		}
	}

	cfg := filepath.Join(dir, "saguin.yaml")
	// **An MQTT listener on loopback, named rather than left out.**
	//
	// Leaving the block out is not the same as having no listener: RFC
	// 0002's default applies and the broker binds `*:1883` on every
	// interface, anonymous. This broker can outlive the test - one that
	// dies on its timeout runs no cleanups - so the difference between the
	// two spellings is whether a leaked process is reachable from the
	// network. Named for that reason and not because anything here speaks
	// MQTT: nothing does.
	if err := os.WriteFile(cfg, []byte(`broker:
  id: nginx-readme
`+memStorage+`  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
  operations:
    listen:
      tcp:
        address: 127.0.0.1:29100
      unix:
        path: `+filepath.Join(sock, "ops.sock")+`
        mode: "0666"
    password_file: `+ops+`
channels:
  events:
    type: append
`), 0o644); err != nil {
		t.Fatal(err)
	}

	broker := exec.Command(bin, "-config", cfg)
	brokerLog := &strings.Builder{}
	broker.Stdout, broker.Stderr = brokerLog, brokerLog
	if err := broker.Start(); err != nil {
		t.Fatalf("start the broker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Process.Kill(); _ = broker.Wait() })
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(sock, "ops.sock")); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(sock, "ops.sock")); err != nil {
		t.Fatalf("the broker never opened its socket:\n%s", brokerLog)
	}

	conf := filepath.Join(dir, "nginx.conf")
	if err := os.WriteFile(conf,
		[]byte("events {}\n\n"+runnable(t, nginxBlock(t), "/certs")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	name := "saguin-readme-nginx"
	// **A container this test left behind holds the port**, and the next
	// run then waits on an nginx that never binds. Removing it first is the
	// same preflight `make conformance` grew for the same reason, and a
	// killed test does not run its cleanup.
	_ = exec.Command("docker", "rm", "-f", name).Run()
	if out, err := exec.Command("docker", "run", "-d", "--name", name,
		"--network", "host",
		"-v", conf+":/etc/nginx/nginx.conf:ro",
		"-v", dir+":/certs:ro",
		"-v", sock+":/sock",
		"nginx:alpine").CombinedOutput(); err != nil {
		t.Skipf("could not start nginx: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	client := &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // the certificate names a host this is not
		}},
	}
	withCert := func() *http.Client {
		pair, err := tls.LoadX509KeyPair(filepath.Join(dir, "device-7.pem"),
			filepath.Join(dir, "device-7-key.pem"))
		if err != nil {
			t.Fatalf("the client certificate: %v", err)
		}
		return &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, Certificates: []tls.Certificate{pair},
			}}}
	}()

	// nginx needs a moment, and a container that never came up should say so
	// rather than fail every row below.
	up := false
	for deadline = time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		if r, err := client.Get("https://127.0.0.1:29443/v1/operations/consumers"); err == nil {
			_ = r.Body.Close()
			up = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !up {
		out, _ := exec.Command("docker", "logs", name).CombinedOutput()
		t.Fatalf("nginx never answered:\n%s", out)
	}
	// **What nginx said, when a row fails.** A 500 from a proxy is nginx's
	// answer rather than the broker's, and reading it out of the container
	// is the difference between a finding and an afternoon.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		// **`docker logs`, never `tail` inside the container.** nginx's
		// image symlinks its logs to /dev/stderr, so `tail` on them never
		// returns - inside a failure-path cleanup, which then hangs until
		// the test times out. A timeout panic runs no further cleanups,
		// which is how a failing run came to leave the container and the
		// broker behind.
		if out, err := exec.Command("docker", "logs", "--tail", "10", name).
			CombinedOutput(); err == nil {
			t.Logf("nginx said:\n%s", out)
		}
		t.Logf("the broker said:\n%s", brokerLog)
	})

	ask := func(t *testing.T, c *http.Client, path, user, pass, header string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "https://127.0.0.1:29443"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		if header != "" {
			req.Header.Set("X-Saguin-Principal", header)
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	for _, tc := range []struct {
		name, path, user, pass, header string
		cert                           bool
		want                           int
	}{
		{"/metrics with the scraper's password", "/metrics", "prometheus", "prom", "", false, 200},
		{"/metrics with a wrong password", "/metrics", "prometheus", "no", "", false, 401},
		{"/v1 by a certificate the operators file names", "/v1/operations/consumers",
			"", "", "", true, 200},
		// **The row to re-check after any change here.** An earlier block
		// answered 200 to this, and one after that answered 404 to
		// everything; both loaded.
		{"/v1 with nothing at all", "/v1/operations/consumers", "", "", "", false, 401},
		{"/v1 with a header the client wrote", "/v1/operations/consumers",
			"", "", "operator", false, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := client
			if tc.cert {
				c = withCert
			}
			if got := ask(t, c, tc.path, tc.user, tc.pass, tc.header); got != tc.want {
				t.Errorf("answered %d, want %d - RFC 0005 prints this row beside "+
					"the block", got, tc.want)
			}
		})
	}
}

// memStorage is the one provider every configuration must define, for the
// tests whose subject is something else. It goes straight after `id:`.
const memStorage = "  storage:\n    default: mem\n    default_retention_period: none\n" +
	"    default_retention_bytes: none\n    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"
