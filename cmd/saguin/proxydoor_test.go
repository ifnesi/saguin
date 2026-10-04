package main

// RFC 0002 "Several listeners of a kind": a Unix door behind a proxy is
// still one door among several, and the listener id the broker keys its
// per-door credentials on (SetListenerCredentials, "the id the substrate
// puts on a connection as cl.Net.Listener") has to be the door's own name,
// not a word fixed at every proxied socket regardless of which one it is.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// shortSocketDir is a short-enough base for a Unix socket path: t.TempDir()
// embeds the test's own name and can overrun the ~104-byte sun_path a
// socket address is limited to, where /tmp itself does not.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sgn")
	if err != nil {
		t.Fatalf("a short-enough temp dir for a socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// proxyV2Header is a minimal PROXY protocol v2 header carrying only
// addresses - no SSL TLV, so no Common Name - which is enough for the
// listener to accept the connection and read what follows as the client's
// own bytes. Real values, not the fields under test: what is asserted here
// is which door's credentials answer the CONNECT that follows, not the
// address the header carries.
func proxyV2Header(srcPort, dstPort uint16) []byte {
	b := []byte{0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a,
		0x21, 0x11, // version 2, PROXY; AF_INET, STREAM
		0x00, 0x0c, // 12 bytes of addresses follow
		127, 0, 0, 1, 127, 0, 0, 1,
		byte(srcPort >> 8), byte(srcPort), byte(dstPort >> 8), byte(dstPort)}
	return b
}

// dialProxiedUnix sends a bare PROXY v2 header over a Unix socket and then
// a CONNECT with the given credentials, returning the CONNACK reason code.
func dialProxiedUnix(t *testing.T, path, user, pass string) (byte, error) {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 3*time.Second)
	if err != nil {
		return 0, err
	}
	if _, err := conn.Write(proxyV2Header(12345, 1883)); err != nil {
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
	if ca == nil {
		return 0, err
	}
	if ca.ReasonCode == 0 {
		t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
	}
	return ca.ReasonCode, nil
}

// A named, proxied Unix door keeps its own password_file rather than being
// judged by the broker-wide pair. The defect: proxyproto.NewUnixSock was handed a fixed "unix" rather than
// the door's own name, so a connection through it carried the wrong
// listener id and was resolved against the broker-wide pair - which here
// has no password_file at all, so it answered anonymous instead of 0x86.
func TestProxiedUnixDoorKeepsItsOwnAuth(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	passwd := filepath.Join(dir, "edge.passwd")
	if out, err := exec.Command(bin, "--passwd", "add", passwd, "dev", "s3cret").CombinedOutput(); err != nil {
		t.Fatalf("--passwd add: %v\n%s", err, out)
	}
	sock := filepath.Join(shortSocketDir(t), "edge.sock")
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      unix:\n"+
		"        - name: edge\n          path: "+sock+"\n          proxy_protocol: true\n"+
		"          password_file: "+passwd+"\nchannels:\n  events:\n    type: append\n"))

	_, await := startBinary(t, cfg)
	await("saguin listening", 20*time.Second)

	if reason, err := dialProxiedUnix(t, sock, "", ""); err != nil || reason != 0x86 {
		t.Errorf("anonymous through the proxied edge door: reason=%d err=%v, want 0x86 - "+
			"its own password_file admits only its own users, no broker-wide file admits "+
			"anybody, and this must not have been resolved as if it were", reason, err)
	}
	if reason, err := dialProxiedUnix(t, sock, "dev", "s3cret"); err != nil || reason != 0 {
		t.Errorf("edge's own credential through its own proxied door: reason=%d err=%v, want admitted",
			reason, err)
	}
}

// Two proxied Unix doors both start: each is a distinct listener id (its
// own name), not both fighting over one fixed "unix" - which is what the
// same defect made the second door's AddListener fail on, naming a door
// already attached.
func TestTwoProxiedUnixDoorsBothStart(t *testing.T) {
	short := shortSocketDir(t)
	sockA, sockB := filepath.Join(short, "a.sock"), filepath.Join(short, "b.sock")
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      unix:\n"+
		"        - name: a\n          path: "+sockA+"\n          proxy_protocol: true\n"+
		"          allow_anonymous: true\n"+
		"        - name: b\n          path: "+sockB+"\n          proxy_protocol: true\n"+
		"          allow_anonymous: true\nchannels:\n  events:\n    type: append\n"))

	_, await := startBinary(t, cfg)
	await("saguin listening", 20*time.Second)

	if reason, err := dialProxiedUnix(t, sockA, "", ""); err != nil || reason != 0 {
		t.Errorf("door a: reason=%d err=%v, want admitted", reason, err)
	}
	if reason, err := dialProxiedUnix(t, sockB, "", ""); err != nil || reason != 0 {
		t.Errorf("door b: reason=%d err=%v, want admitted - if the broker started at all; "+
			"a second door sharing the first's hard-coded id fails to bind and the broker "+
			"never says \"saguin listening\"", reason, err)
	}
}
