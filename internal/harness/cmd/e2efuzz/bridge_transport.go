package main

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The bridge's transport (config.md, bridges.<name>, peer: "the peer's URL:
// tcp://, tls://, ws:// or wss://, [...] ws:// and wss:// may add a path";
// tls (tls, wss): "ca_file, the authority for the peer's certificate"):
// each run's seed draws the scheme its bridge's peer serves, so the run's
// links all travel over it, and runs together carry the bridge over every
// scheme. A tls or wss peer presents a certificate an authority of its own
// issued for the loopback address, which the bridge's tls block names as
// its ca_file; a ws or wss peer serves its WebSocket on bridgePath. Every
// check of the link is the same over each (bridges.go).

// bridgeSchemes are the schemes a run's seed draws its bridge's from.
var bridgeSchemes = []string{"tcp", "tls", "ws", "wss"}

// bridgePath is the path a ws or wss peer serves its WebSocket on.
const bridgePath = "/e2efuzz/bridge"

// bridgeAuthorityFileName is the file in the run's folder holding the
// authority of a tls or wss peer's certificate: the bridge's ca_file.
const bridgeAuthorityFileName = "bridge-ca.pem"

// bridgeScheme is the scheme seed draws for the bridge's peer, apart from
// the storage mode it draws (storageMode), so the two vary independently.
func bridgeScheme(seed uint64) string {
	n := uint64(len(bridgeSchemes))
	return bridgeSchemes[seed/uint64(len(storageModes))%n]
}

// listenBridgePeer listens for the bridge over the scheme seed draws, and
// returns the peer and the bridge's tls block, nil on tcp and ws; a tls or
// wss peer's authority is written to dir.
func listenBridgePeer(seed uint64, dir string) (*harness.Peer, *harness.BridgeTLS, error) {
	o := harness.PeerOptions{Scheme: bridgeScheme(seed)}
	if o.Scheme == "ws" || o.Scheme == "wss" {
		o.Path = bridgePath
	}
	var block *harness.BridgeTLS
	if o.Scheme == "tls" || o.Scheme == "wss" {
		ca, err := testkit.NewAuthority("e2efuzz bridge peer authority")
		if err != nil {
			return nil, nil, err
		}
		server, err := ca.Issue(&testkit.Leaf{CommonName: "e2efuzz peer", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
		if err != nil {
			return nil, nil, err
		}
		block = &harness.BridgeTLS{CAFile: filepath.Join(dir, bridgeAuthorityFileName)}
		if err := os.WriteFile(block.CAFile, ca.PEM, 0o600); err != nil {
			return nil, nil, err
		}
		o.TLS = &tls.Config{Certificates: []tls.Certificate{server.Certificate}}
	}
	p, err := harness.ListenPeerWith(o)
	return p, block, err
}
