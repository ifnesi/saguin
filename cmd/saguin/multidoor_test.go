package main

// RFC 0002 "TLS on a listener", where "Several listeners of a kind" says
// that a kind with two or more doors is a list, each with its own name,
// auth and TLS. These tests drive the real binary because what is asserted
// is main's wiring -
// that two configured tcp doors are actually started, each keeping its own
// answer to who may connect, and that max_connections and SIGUSR1 both
// reach across the list rather than stopping at the first entry.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// dialReason dials addr - over TLS when conf is not nil - and sends a bare
// CONNECT with the given credentials, returning the CONNACK reason code. An
// error means the connection never got that far: a TLS handshake a door
// does not speak, or a socket the other side closed.
func dialReason(t *testing.T, addr string, conf *tls.Config, user, pass string) (byte, error) {
	t.Helper()
	var conn net.Conn
	var err error
	if conf != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, conf)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 3*time.Second)
	}
	if err != nil {
		return 0, err
	}
	connect := &paho.Connect{
		ClientID: fmt.Sprintf("c-%d", time.Now().UnixNano()), CleanStart: true, KeepAlive: 60,
	}
	if user != "" {
		connect.UsernameFlag = true
		connect.Username = user
		if pass != "" {
			connect.PasswordFlag = true
			connect.Password = []byte(pass)
		}
	}
	c := paho.NewClient(paho.ClientConfig{Conn: packets.NewThreadSafeConn(conn)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, connect)
	// **ca is filled even when Connect also returns an error**: a refused
	// CONNACK (ReasonCode >= 0x80) is reported both ways, so the reason
	// code is read off ca whenever the broker actually answered, and only
	// a nil ca - the handshake or the network failing before any CONNACK -
	// is a bare error with no reason to give.
	if ca == nil {
		return 0, err
	}
	if ca.ReasonCode == 0 {
		t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
	}
	return ca.ReasonCode, nil
}

// startBinary builds saguin once for the test, writes cfg and starts it,
// and returns the running process and a function that waits for a line
// containing what within the given time.
func startBinary(t *testing.T, cfg string) (proc *exec.Cmd, await func(string, time.Duration) string) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	run := exec.CommandContext(ctx, bin, "--config", cfg)
	out, err := run.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run.Stderr = run.Stdout
	if err := run.Start(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not start here: %v", err)
	}
	t.Cleanup(func() { _ = run.Process.Kill(); _ = run.Wait() })

	said := make(chan string, 256)
	go func() {
		lines := bufio.NewScanner(out)
		for lines.Scan() {
			said <- lines.Text()
		}
		close(said)
	}()
	var seen []string
	await = func(what string, within time.Duration) string {
		t.Helper()
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
	return run, await
}

// freeAddr is a loopback address nothing is listening on yet.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// A plain tcp door beside a TLS one, each with its own name and its own
// answer to who may connect: RFC 0002 "TLS on a listener", where "Several
// listeners of a kind" sits, beside "Who may connect". Proven on the wire
// rather than read off the configuration, and each assertion is the one a
// bug sharing one door's settings with the other would flip:
//   - the open door admits anonymous over plain TCP;
//   - the open door speaks no TLS at all;
//   - the fleet door refuses a plain-TCP CONNECT outright (it never gets a
//     CONNACK - the handshake itself fails);
//   - the fleet door refuses an anonymous TLS CONNECT (0x86) - which fails
//     under a mutant that let the open door's allow_anonymous leak across;
//   - the fleet door admits its own password, over TLS.
func TestTwoTCPDoorsPlainAndTLSKeepDistinctAuth(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	ca := newAuthority(t, "fleet-ca")
	cert, key := ca.issue(t, "fleet-broker", x509.ExtKeyUsageServerAuth)
	writeFile(t, certFile, cert)
	writeFile(t, keyFile, key)

	passwd := filepath.Join(dir, "fleet.passwd")
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	if out, err := exec.Command(bin, "--passwd", "add", passwd, "device-7", "s3cret").CombinedOutput(); err != nil {
		t.Fatalf("--passwd add: %v\n%s", err, out)
	}

	openAddr, fleetAddr := freeAddr(t), freeAddr(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        - name: open\n          address: "+openAddr+"\n          allow_anonymous: true\n"+
		"        - name: fleet\n          address: "+fleetAddr+"\n          password_file: "+passwd+
		"\n          tls:\n            cert_file: "+certFile+"\n            key_file: "+keyFile+
		"\nchannels:\n  events:\n    type: append\n"))

	_, await := startBinary(t, cfg)
	await("saguin listening", 20*time.Second)

	if reason, err := dialReason(t, openAddr, nil, "", ""); err != nil || reason != 0 {
		t.Errorf("open (plain, anonymous): reason=%d err=%v, want an admitted anonymous client", reason, err)
	}
	if _, err := dialReason(t, openAddr, &tls.Config{InsecureSkipVerify: true}, "", ""); err == nil {
		t.Error("the open door completed a TLS handshake; it was configured with no tls block at all")
	}
	if _, err := dialReason(t, fleetAddr, nil, "", ""); err == nil {
		t.Error("the fleet door accepted a plain-TCP CONNECT; it is configured for TLS only")
	}
	tlsConf := &tls.Config{InsecureSkipVerify: true}
	if reason, _ := dialReason(t, fleetAddr, tlsConf, "", ""); reason != 0x86 {
		t.Errorf("fleet (TLS, anonymous): reason=%d, want 0x86 - its own password_file admits "+
			"only its own users, and this must not have inherited the open door's allow_anonymous", reason)
	}
	if reason, err := dialReason(t, fleetAddr, tlsConf, "device-7", "s3cret"); err != nil || reason != 0 {
		t.Errorf("fleet (TLS, its own credential): reason=%d err=%v, want an admitted client", reason, err)
	}
}

// limits.max_connections is one count across every door (RFC 0002 "TLS on a
// listener"). Proven with an uneven split - two connections on one tcp door
// and one on the other, against a bound of three - because a mutant that
// counted per door would still have slack on the lighter door and admit a
// fourth this test refuses.
func TestMaxConnectionsIsSharedAcrossDoors(t *testing.T) {
	dir := t.TempDir()
	addrA, addrB := freeAddr(t), freeAddr(t)
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        - name: a\n          address: "+addrA+"\n          allow_anonymous: true\n"+
		"        - name: b\n          address: "+addrB+"\n          allow_anonymous: true\n"+
		"  limits:\n    max_connections: 3\nchannels:\n  events:\n    type: append\n"))

	_, await := startBinary(t, cfg)
	await("saguin listening", 20*time.Second)

	// Two on door a, one on door b: the bound of three is reached with door
	// b holding only one of it.
	for i := 0; i < 2; i++ {
		if reason, err := dialReason(t, addrA, nil, "", ""); err != nil || reason != 0 {
			t.Fatalf("connection %d on door a: reason=%d err=%v", i, reason, err)
		}
	}
	if reason, err := dialReason(t, addrB, nil, "", ""); err != nil || reason != 0 {
		t.Fatalf("connection on door b: reason=%d err=%v", reason, err)
	}

	// A fourth, on door b - which by itself holds only one of the three -
	// must be refused if the bound is the broker's rather than the door's.
	reason, err := dialReason(t, addrB, nil, "", "")
	if err == nil && reason == 0 {
		t.Error("a fourth connection was admitted on door b, which alone had not reached the bound: " +
			"max_connections is being counted per door rather than once across all of them")
	}
}

// SIGUSR1 re-reads every TLS listener's certificate and every listener's
// password file - both doors, not only the first one configured (RFC 0002
// "TLS on a listener", "Who may connect").
func TestSIGUSR1ReloadsEveryDoorsCertificateAndPasswordFile(t *testing.T) {
	dir := t.TempDir()
	ca := newAuthority(t, "doors-ca")

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
	listen := "  mqtt:\n    listen:\n      tcp:\n"
	for _, d := range doors {
		cert, key := ca.issue(t, d.name+"-broker-a", x509.ExtKeyUsageServerAuth)
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
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+listen+"channels:\n  events:\n    type: append\n"))

	run, await := startBinary(t, cfg)
	await("saguin listening", 20*time.Second)

	tlsConf := &tls.Config{InsecureSkipVerify: true}
	for _, d := range doors {
		if reason, err := dialReason(t, d.addr, tlsConf, "u1", "pw1"); err != nil || reason != 0 {
			t.Fatalf("%s before any reload: reason=%d err=%v", d.name, reason, err)
		}
		if got, err := tlsPeerCN(t, d.addr); err != nil || got != d.name+"-broker-a" {
			t.Fatalf("%s before any reload served %q (%v), want %s-broker-a", d.name, got, err, d.name)
		}
	}

	// Renew both certificates and add a second user to both password
	// files, then signal once.
	for _, d := range doors {
		cert, key := ca.issue(t, d.name+"-broker-b", x509.ExtKeyUsageServerAuth)
		writeFile(t, d.certFile, cert)
		writeFile(t, d.keyFile, key)
		if out, err := exec.Command(bin, "--passwd", "add", d.passwdFile, "u2", "pw2").CombinedOutput(); err != nil {
			t.Fatalf("--passwd add: %v\n%s", err, out)
		}
	}
	if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}
	await("who may connect was re-read from each listener's own password_file", 10*time.Second)
	// Two certificates re-read, one line each.
	await("a TLS certificate was re-read", 10*time.Second)
	await("a TLS certificate was re-read", 10*time.Second)

	for _, d := range doors {
		if got, err := tlsPeerCN(t, d.addr); err != nil || got != d.name+"-broker-b" {
			t.Errorf("%s after SIGUSR1 served %q (%v), want %s-broker-b", d.name, got, err, d.name)
		}
		if reason, err := dialReason(t, d.addr, tlsConf, "u2", "pw2"); err != nil || reason != 0 {
			t.Errorf("%s: the user added before the signal was refused: reason=%d err=%v", d.name, reason, err)
		}
	}
}

// tlsPeerCN dials addr over TLS and returns the server certificate's Common
// Name, without completing an MQTT handshake - which certificate a door
// serves does not depend on what comes after it.
func tlsPeerCN(t *testing.T, addr string) (string, error) {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr,
		&tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].Subject.CommonName, nil
}
