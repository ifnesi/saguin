package main

import (
	"crypto/tls"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, R1: "They also fail if the live count of Sagüin's goroutines
// differs from the table". The table: "reader | 1 per connection, client or
// bridge | the connection"; "writer | 1 per connection, client or bridge |
// the connection"; "dialer | 1 per bridge connection attempt | the
// attempt"; and "A WebSocket connection accepted by a listener has as its
// reader the goroutine net/http starts for it", which a bridge's link,
// dialled and not accepted, is not.
//
// With read_connections at its default, 2 (config.md), one MQTT TCP
// listener, no operations listener and one bridge to a wss:// peer, its
// link up, the table gives besides main: core 1, store writer 1, store
// reader 2, logger 1, listener 1, and the bridge connection's reader and
// writer, 8 in all; the dialer has ended with its attempt, and no goroutine
// net/http starts is Sagüin's (testkit.ServedConnection). The reader and
// writer outlive the dialer that started them, so they are counted with
// testkit.Running.
func TestBridgeOverWebSocketGoroutinesMatchR1(t *testing.T) {
	if !alone(t) {
		return
	}
	ca, err := testkit.NewAuthority("Sagüin R1 test authority")
	if err != nil {
		t.Fatal(err)
	}
	server, err := ca.Issue(&testkit.Leaf{CommonName: "peer", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "wss", Path: "/mqtt",
		TLS: &tls.Config{Certificates: []tls.Certificate{server.Certificate}}})
	b := startInProcess(t, &harness.Options{Bridges: []harness.Bridge{{Name: "r1-wss", Peer: peer.URL(), ClientID: "r1-wss",
		TLS:    &harness.BridgeTLS{CAFile: testkit.WriteFile(t, t.TempDir(), "ca.pem", string(ca.PEM))},
		Topics: []harness.BridgeRule{{Filter: "a/#", Topic: "b/$#", Direction: "out", QoS: 1}}}}})
	// A Server Keep Alive of 65,535 seconds: no PINGREQ comes while the
	// test counts (design.md, Bridges: "A Server Keep Alive replaces it").
	peer.Link(t).Accept(t, &harness.Connack{Props: harness.Properties{{ID: harness.ServerKeepAlive, Int: 65_535}}})

	const want = 1 + 1 + 2 + 1 + 1 + 2
	running := func() (int, map[string]int) {
		byCreator := testkit.Running(b.main, module)
		total := 0
		for _, n := range byCreator {
			total += n
		}
		return total, byCreator
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		n, byCreator := running()
		return n == want && byCreator[testkit.ServedConnection] == 0
	}) {
		n, byCreator := running()
		t.Errorf("with a wss:// bridge's link up, Sagüin runs %d goroutines besides main, by the function starting them %v; R1's table gives %d, none of them started by net/http (%s)",
			n, byCreator, want, testkit.ServedConnection)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	b.returned(t, "after SIGTERM")
}
