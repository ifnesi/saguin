package main

// RFC 0002 "Several listeners of a kind" / RFC 0005 "The operations
// listener": broker.operations.listen.tcp and .unix take the same
// list-or-map shape the MQTT listener's kinds do. These tests drive the
// real binary because what is asserted is main's wiring: that each
// operations door keeps its own credential, TLS certificate and the
// loopback-needs-a-credential rule, and that --check-config and a plain
// start refuse the same routable door with none.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// getStatus does an HTTP GET against addr (over TLS when conf is not nil),
// with optional basic auth, and returns the status code.
func getStatus(t *testing.T, addr, path string, conf *tls.Config, user, pass string) int {
	t.Helper()
	scheme := "http"
	if conf != nil {
		scheme = "https"
	}
	req, err := http.NewRequest(http.MethodGet, scheme+"://"+addr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: conf}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s%s: %v", addr, path, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// A loopback probe door needs no login on /health or /metrics, beside a
// routable door that refuses /metrics without its own credential and
// admits it with one - each door's rule asked of its own answer (RFC 0002
// "The operations listener").
func TestOperationsLoopbackProbeBesideRoutableDoor(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	fleetPasswd := filepath.Join(dir, "fleet.passwd")
	if out, err := exec.Command(bin, "--passwd", "add", fleetPasswd, "prometheus", "s3cret").
		CombinedOutput(); err != nil {
		t.Fatalf("--passwd add: %v\n%s", err, out)
	}

	probeAddr, fleetAddr := freeAddr(t), freeAddr(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n  operations:\n    listen:\n      tcp:\n"+
		"        - name: probe\n          address: "+probeAddr+"\n"+
		"        - name: fleet\n          address: "+fleetAddr+"\n          password_file: "+fleetPasswd+
		"\nchannels:\n  events:\n    type: append\n"))

	_, await := startBinary(t, cfg)
	await("operations listening", 20*time.Second)

	if got := getStatus(t, probeAddr, "/health", nil, "", ""); got != http.StatusOK {
		t.Errorf("probe /health: %d, want 200", got)
	}
	if got := getStatus(t, probeAddr, "/metrics", nil, "", ""); got != http.StatusOK {
		t.Errorf("probe /metrics with no login: %d, want 200 - it names no credential and is loopback", got)
	}
	if got := getStatus(t, fleetAddr, "/health", nil, "", ""); got != http.StatusOK {
		t.Errorf("fleet /health: %d, want 200 - /health is never authenticated", got)
	}
	if got := getStatus(t, fleetAddr, "/metrics", nil, "", ""); got != http.StatusUnauthorized {
		t.Errorf("fleet /metrics with no credential: %d, want 401 - it names its own password_file", got)
	}
	if got := getStatus(t, fleetAddr, "/metrics", nil, "prometheus", "s3cret"); got != http.StatusOK {
		t.Errorf("fleet /metrics with its own credential: %d, want 200", got)
	}
	// The probe door's own answer must not have leaked onto the fleet
	// door: a mutant sharing one door's Operators entry with the other
	// would pass the two checks above and fail this one, admitting the
	// probe's "nobody" answer on the routable port.
	if got := getStatus(t, fleetAddr, "/metrics", nil, "prometheus", "wrong"); got != http.StatusUnauthorized {
		t.Errorf("fleet /metrics with a wrong password: %d, want 401", got)
	}
}

// A routable operations TCP door with no credential anywhere is refused at
// --check-config and at a plain start, naming the door (RFC 0002 "The
// operations listener": a routable address must authenticate somebody).
func TestRoutableOperationsDoorWithNoCredentialIsRefused(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	addr := freeAddr(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n  operations:\n    listen:\n      tcp:\n"+
		"        - name: fleet\n          address: 0.0.0.0"+addr[len("127.0.0.1"):]+
		"\nchannels:\n  events:\n    type: append\n"))

	checkExit, _, checkErr := runBinary(t, bin, "--check-config", cfg)
	if checkExit == 0 {
		t.Fatalf("--check-config accepted a routable door with no credential")
	}
	if !strings.Contains(checkErr, "broker.operations.listen.tcp[fleet]") {
		t.Fatalf("--check-config's refusal does not name the door: %s", checkErr)
	}

	startExit, _, startErr := runBinary(t, bin, "--config", cfg)
	if startExit == 0 {
		t.Fatalf("a plain start accepted a routable door with no credential")
	}
	if !strings.Contains(startErr, "broker.operations.listen.tcp[fleet]") {
		t.Fatalf("a plain start's refusal does not name the door: %s", startErr)
	}
}

// SIGUSR1 reloads every operations TCP door's own certificate and password
// file, each independently: a renewal or a new user on one door must not
// touch the other's.
func TestSIGUSR1ReloadsEachOperationsDoorsCertificateAndPasswordFile(t *testing.T) {
	dir := t.TempDir()
	ca := newAuthority(t, "ops-doors-ca")

	type door struct {
		name, addr, certFile, keyFile, passwdFile string
	}
	doors := []*door{
		{name: "d1", addr: freeAddr(t), certFile: filepath.Join(dir, "d1-cert.pem"),
			keyFile: filepath.Join(dir, "d1-key.pem"), passwdFile: filepath.Join(dir, "d1.passwd")},
		{name: "d2", addr: freeAddr(t), certFile: filepath.Join(dir, "d2-cert.pem"),
			keyFile: filepath.Join(dir, "d2-key.pem"), passwdFile: filepath.Join(dir, "d2.passwd")},
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	listen := "  operations:\n    listen:\n      tcp:\n"
	for _, d := range doors {
		cert, key := ca.issue(t, d.name+"-ops-a", x509.ExtKeyUsageServerAuth)
		writeFile(t, d.certFile, cert)
		writeFile(t, d.keyFile, key)
		if out, err := exec.Command(bin, "--passwd", "add", d.passwdFile, "u1", "pw1").CombinedOutput(); err != nil {
			t.Fatalf("--passwd add: %v\n%s", err, out)
		}
		listen += "        - name: " + d.name + "\n          address: " + d.addr +
			"\n          password_file: " + d.passwdFile +
			"\n          tls:\n            cert_file: " + d.certFile + "\n            key_file: " + d.keyFile + "\n"
	}

	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n"+listen+"channels:\n  events:\n    type: append\n"))

	run, await := startBinary(t, cfg)
	await("operations listening", 20*time.Second)

	tlsConf := &tls.Config{InsecureSkipVerify: true}
	for _, d := range doors {
		if got := getStatus(t, d.addr, "/metrics", tlsConf, "u1", "pw1"); got != http.StatusOK {
			t.Fatalf("%s before any reload: %d, want 200", d.name, got)
		}
		if got, err := tlsPeerCN(t, d.addr); err != nil || got != d.name+"-ops-a" {
			t.Fatalf("%s before any reload served %q (%v), want %s-ops-a", d.name, got, err, d.name)
		}
	}

	for _, d := range doors {
		cert, key := ca.issue(t, d.name+"-ops-b", x509.ExtKeyUsageServerAuth)
		writeFile(t, d.certFile, cert)
		writeFile(t, d.keyFile, key)
		if out, err := exec.Command(bin, "--passwd", "add", d.passwdFile, "u2", "pw2").CombinedOutput(); err != nil {
			t.Fatalf("--passwd add: %v\n%s", err, out)
		}
	}
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	await("SIGUSR1: who may read the operations routes was re-read", 10*time.Second)
	await("SIGUSR1: who may read the operations routes was re-read", 10*time.Second)
	await("a TLS certificate was re-read", 10*time.Second)
	await("a TLS certificate was re-read", 10*time.Second)

	for _, d := range doors {
		if got, err := tlsPeerCN(t, d.addr); err != nil || got != d.name+"-ops-b" {
			t.Errorf("%s after SIGUSR1 served %q (%v), want %s-ops-b", d.name, got, err, d.name)
		}
		if got := getStatus(t, d.addr, "/metrics", tlsConf, "u2", "pw2"); got != http.StatusOK {
			t.Errorf("%s: the user added before the signal was refused: %d, want 200", d.name, got)
		}
	}

	// **Independence, not only success.** Each door's new certificate must
	// be its own: d1 serving d2's certificate (or vice versa) would still
	// pass "a certificate was re-read" above, so this checks the name
	// each door actually presents rather than that renewal happened at
	// all - the mutant this test exists for is one door's reload wired to
	// the other's file.
	if doors[0].name == doors[1].name {
		t.Fatal("test setup: two doors must have distinct names to prove independence")
	}
	for i, d := range doors {
		other := doors[1-i]
		if got, err := tlsPeerCN(t, d.addr); err == nil && got == other.name+"-ops-b" {
			t.Errorf("%s served %q, which is %s's certificate", d.name, got, other.name)
		}
	}
}

// tryGet is one GET that reports the failure instead of failing the test:
// a refused TLS handshake is the answer some cases expect.
func tryGet(client *http.Client, url, user, pass string) (int, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func buildSaguin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	return bin
}

func addOperator(t *testing.T, bin, file, user, pass string) {
	t.Helper()
	if out, err := exec.Command(bin, "--passwd", "add", file, user, pass).CombinedOutput(); err != nil {
		t.Fatalf("--passwd add: %v\n%s", err, out)
	}
}

// Two named operations unix doors, one behind a PROXY-protocol front with
// its own password_file: the request that arrives through the proxied door
// is judged by that door's file - not by no file (the kind's default name)
// and not by the other door's. The other door keeps its own rule.
func TestProxiedOperationsUnixDoorKeepsItsOwnCredential(t *testing.T) {
	bin := buildSaguin(t)
	dir := t.TempDir()
	sock := shortSocketDir(t)
	proxPath, plainPath := filepath.Join(sock, "p.sock"), filepath.Join(sock, "q.sock")
	pf := filepath.Join(dir, "prox.passwd")
	addOperator(t, bin, pf, "proxuser", "pw")
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n  operations:\n    listen:\n      unix:\n"+
		"        - name: prox\n          path: "+proxPath+"\n          proxy_protocol: true\n          password_file: "+pf+"\n"+
		"        - name: plain\n          path: "+plainPath+"\n"+
		"channels:\n  events:\n    type: append\n"))
	_, await := startBinary(t, cfg)
	await("operations listening", 20*time.Second)

	get := func(path string, proxied bool, user, pass string) int {
		t.Helper()
		client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
				if err == nil && proxied {
					_, err = c.Write(proxyV2Header(12345, 9090))
				}
				return c, err
			}}}
		got, err := tryGet(client, "http://saguin/metrics", user, pass)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return got
	}
	if got := get(proxPath, true, "", ""); got != http.StatusUnauthorized {
		t.Errorf("proxied door, no credential: %d, want 401", got)
	}
	if got := get(proxPath, true, "proxuser", "wrong"); got != http.StatusUnauthorized {
		t.Errorf("proxied door, wrong password: %d, want 401", got)
	}
	if got := get(proxPath, true, "proxuser", "pw"); got != http.StatusOK {
		t.Errorf("proxied door, its own user: %d, want 200", got)
	}
	if got := get(plainPath, false, "", ""); got != http.StatusOK {
		t.Errorf("the other door names no credential and must keep that: %d, want 200", got)
	}
}

// A routable operations door with client_ca_file and no password file is
// authenticated by the certificate alone: a certificate from its CA is
// admitted; none, or one from another CA, is refused.
func TestRoutableOperationsDoorAuthenticatesByClientCertificate(t *testing.T) {
	_ = buildSaguin(t)
	dir := t.TempDir()
	ca, other := newAuthority(t, "ops-ca"), newAuthority(t, "other-ca")
	srvCert, srvKey := ca.issue(t, "ops", x509.ExtKeyUsageServerAuth)
	writeFile(t, filepath.Join(dir, "srv.pem"), srvCert)
	writeFile(t, filepath.Join(dir, "srv-key.pem"), srvKey)
	writeFile(t, filepath.Join(dir, "ca.pem"), ca.pem)
	addr := freeAddr(t)
	routable := "0.0.0.0" + addr[len("127.0.0.1"):]
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n  operations:\n    listen:\n      tcp:\n"+
		"        - name: fleet\n          address: "+routable+"\n          tls:\n"+
		"            cert_file: "+filepath.Join(dir, "srv.pem")+"\n"+
		"            key_file: "+filepath.Join(dir, "srv-key.pem")+"\n"+
		"            client_ca_file: "+filepath.Join(dir, "ca.pem")+"\n"+
		"channels:\n  events:\n    type: append\n"))
	_, await := startBinary(t, cfg)
	await("operations listening", 20*time.Second)

	clientWith := func(a *authority) *http.Client {
		conf := &tls.Config{InsecureSkipVerify: true}
		if a != nil {
			c, k := a.issue(t, "operator", x509.ExtKeyUsageClientAuth)
			pair, err := tls.X509KeyPair(c, k)
			if err != nil {
				t.Fatal(err)
			}
			conf.Certificates = []tls.Certificate{pair}
		}
		return &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: conf}}
	}
	url := "https://" + addr + "/metrics"
	if got, err := tryGet(clientWith(&ca), url, "", ""); err != nil || got != http.StatusOK {
		t.Errorf("certificate from its CA: %d, %v; want 200", got, err)
	}
	if got, err := tryGet(clientWith(nil), url, "", ""); err == nil {
		t.Errorf("no certificate was admitted: %d", got)
	}
	if got, err := tryGet(clientWith(&other), url, "", ""); err == nil {
		t.Errorf("a certificate from another CA was admitted: %d", got)
	}
}

// Two operations TCP doors, two password files: a user of one is refused
// on the other, in both directions.
func TestOperationsDoorsDoNotShareCredentials(t *testing.T) {
	bin := buildSaguin(t)
	dir := t.TempDir()
	fa, fb := filepath.Join(dir, "a.passwd"), filepath.Join(dir, "b.passwd")
	addOperator(t, bin, fa, "alice", "pa")
	addOperator(t, bin, fb, "bob", "pb")
	a, b := freeAddr(t), freeAddr(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n  operations:\n    listen:\n      tcp:\n"+
		"        - name: a\n          address: "+a+"\n          password_file: "+fa+"\n"+
		"        - name: b\n          address: "+b+"\n          password_file: "+fb+"\n"+
		"channels:\n  events:\n    type: append\n"))
	_, await := startBinary(t, cfg)
	await("operations listening", 20*time.Second)
	for _, c := range []struct {
		addr, user, pass string
		want             int
	}{
		{a, "alice", "pa", 200}, {b, "bob", "pb", 200},
		{b, "alice", "pa", 401}, {a, "bob", "pb", 401},
	} {
		if got := getStatus(t, c.addr, "/metrics", nil, c.user, c.pass); got != c.want {
			t.Errorf("%s as %s: %d, want %d", c.addr, c.user, got, c.want)
		}
	}
}
