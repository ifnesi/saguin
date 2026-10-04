package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// authority is a CA a test issues certificates from.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newAuthority(t *testing.T, name string) authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	cert, _ := x509.ParseCertificate(der)
	return authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a leaf named name, and returns it and its key as PEM.
func (a authority) issue(t *testing.T, name string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatalf("issue %s: %v", name, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// RFC 0002 "TLS on a listener": SIGUSR1 re-reads every TLS listener's
// certificate and client CA from the paths the broker started with.
//
// Driven through the binary, because what is asserted is main's wiring: a new
// handshake is served the renewed certificate on the MQTT port and on the
// operations port, a connection made before the signal carries on, a key that
// does not match leaves every listener serving what it had, and a replaced
// client CA refuses a certificate only the old one trusted.
func TestSIGUSR1RereadsTLSCertificatesAndClientCAs(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"),
		filepath.Join(dir, "clients.pem")

	servers := newAuthority(t, "server-ca")
	oldClients, newClients := newAuthority(t, "old-clients"), newAuthority(t, "new-clients")
	certA, keyA := servers.issue(t, "broker-a", x509.ExtKeyUsageServerAuth)
	writeFile(t, certFile, certA)
	writeFile(t, keyFile, keyA)
	writeFile(t, caFile, oldClients.pem)

	oldCert, oldKey := oldClients.issue(t, "device-old", x509.ExtKeyUsageClientAuth)
	newCert, newKey := newClients.issue(t, "device-new", x509.ExtKeyUsageClientAuth)
	oldPair, err := tls.X509KeyPair(oldCert, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	newPair, err := tls.X509KeyPair(newCert, newKey)
	if err != nil {
		t.Fatal(err)
	}

	// A free port for the operations listener, whose startup line names the
	// configured address rather than the bound one.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opsAddr := probe.Addr().String()
	_ = probe.Close()

	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:0\n        tls:\n          cert_file: "+certFile+
		"\n          key_file: "+keyFile+"\n          client_ca_file: "+caFile+
		"\n  operations:\n    listen:\n      tcp:\n        address: "+opsAddr+
		"\n        tls:\n          cert_file: "+certFile+"\n          key_file: "+keyFile+
		"\nchannels:\n  events:\n    type: append\n"))

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
	await := func(what string, within time.Duration) string {
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
	m := listenerAddr.FindStringSubmatch(await("protocol=tcp", 20*time.Second))
	if m == nil {
		t.Fatal("the broker never said which address the tcp listener bound")
	}
	mqttAddr := m[1]
	await("operations listening", 20*time.Second)

	// served is the Common Name a new MQTT handshake is shown, or the error
	// the handshake ended with - read from a round trip, because in TLS 1.3 a
	// server refuses a client certificate after the client's side is done.
	type answer struct {
		name string
		conn *tls.Conn
		err  error
	}
	dialed := 0
	dialMQTT := func(pair tls.Certificate) answer {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", mqttAddr,
			&tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{pair}})
		if err != nil {
			return answer{err: err}
		}
		// A bare MQTT 5 CONNECT under a client id of its own - one id for
		// every dial would take the earlier connection over - and the CONNACK.
		dialed++
		id := fmt.Sprintf("dev-%d", dialed)
		connect := append([]byte{0x10, 0x00, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x00,
			0x00, 0x00, byte(len(id))}, id...)
		connect[1] = byte(len(connect) - 2)
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Write(connect); err != nil {
			_ = c.Close()
			return answer{err: err}
		}
		ack := make([]byte, 2)
		if _, err := io.ReadFull(c, ack); err != nil {
			_ = c.Close()
			return answer{err: err}
		}
		if ack[0] != 0x20 {
			_ = c.Close()
			return answer{err: fmt.Errorf("the first packet back was 0x%02x, not a CONNACK", ack[0])}
		}
		rest := make([]byte, ack[1])
		if _, err := io.ReadFull(c, rest); err != nil {
			_ = c.Close()
			return answer{err: err}
		}
		_ = c.SetDeadline(time.Time{})
		return answer{name: c.ConnectionState().PeerCertificates[0].Subject.CommonName, conn: c}
	}
	opsName := func() string {
		t.Helper()
		client := http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}}
		resp, err := client.Get("https://" + opsAddr + "/health")
		if err != nil {
			t.Fatalf("the operations listener did not answer over TLS: %v", err)
		}
		defer resp.Body.Close()
		return resp.TLS.PeerCertificates[0].Subject.CommonName
	}
	signal := func() {
		t.Helper()
		if err := run.Process.Signal(syscall.SIGUSR1); err != nil {
			t.Fatalf("signal: %v", err)
		}
	}

	before := dialMQTT(oldPair)
	if before.err != nil || before.name != "broker-a" {
		t.Fatalf("before any renewal a client was served %q (%v), want broker-a", before.name, before.err)
	}
	defer before.conn.Close()
	if got := opsName(); got != "broker-a" {
		t.Fatalf("the operations listener was serving %q before any renewal", got)
	}

	// Renewed, and signalled.
	certB, keyB := servers.issue(t, "broker-b", x509.ExtKeyUsageServerAuth)
	writeFile(t, certFile, certB)
	writeFile(t, keyFile, keyB)
	signal()
	await(`listener=broker.operations.listen.tcp`, 10*time.Second)

	if got := dialMQTT(oldPair); got.err != nil || got.name != "broker-b" {
		t.Errorf("after SIGUSR1 a new MQTT handshake was served %q (%v), want broker-b", got.name, got.err)
	} else {
		got.conn.Close()
	}
	if got := opsName(); got != "broker-b" {
		t.Errorf("after SIGUSR1 the operations listener served %q, want broker-b", got)
	}
	// The connection made before the signal is still up: a PINGREQ is answered.
	_ = before.conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := before.conn.Write([]byte{0xC0, 0x00}); err != nil {
		t.Errorf("the connection made before the renewal could not be written to: %v", err)
	}
	pong := make([]byte, 2)
	if _, err := io.ReadFull(before.conn, pong); err != nil || pong[0] != 0xD0 {
		t.Errorf("the connection made before the renewal did not answer a PINGREQ: % x %v", pong, err)
	}

	// Half a renewal: a certificate whose key is still the old one.
	certC, _ := servers.issue(t, "broker-c", x509.ExtKeyUsageServerAuth)
	writeFile(t, certFile, certC)
	signal()
	line := await("cannot be used", 10*time.Second)
	if !strings.Contains(line, "every listener is serving what it had") {
		t.Errorf("the refusal does not say the certificates were kept: %s", line)
	}
	if got := dialMQTT(oldPair); got.err != nil || got.name != "broker-b" {
		t.Errorf("after a mismatched pair a new handshake was served %q (%v), want the kept broker-b",
			got.name, got.err)
	} else {
		got.conn.Close()
	}

	// The key put right, and the client authorities replaced.
	writeFile(t, certFile, certB)
	writeFile(t, caFile, newClients.pem)
	signal()
	await(`listener=broker.mqtt.listen.tcp`, 10*time.Second)
	await(`listener=broker.operations.listen.tcp`, 10*time.Second)

	if got := dialMQTT(oldPair); got.err == nil {
		got.conn.Close()
		t.Error("a certificate only the replaced client CA trusted was still admitted")
	}
	if got := dialMQTT(newPair); got.err != nil {
		t.Errorf("a certificate the new client CA issued was refused: %v", got.err)
	} else {
		got.conn.Close()
	}
}
