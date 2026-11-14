package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// bridgeClient identifies a client's session independently of transport
// scheme and WebSocket path. DNS names compare without case; IPs by value.
type bridgeClient struct {
	host string
	port uint16
	id   string
}

// parsePeer uses URL parsing for the envelope and the listener's address
// checks for its authority, including bracketed IPv6 and link-local zones.
// The original URL spelling is kept, with only the default WS path added.
func (b *Bridge) parsePeer() (resolved, host string, port uint16, err error) {
	value := b.Peer
	u, err := url.Parse(value)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid peer URL: %w", err)
	}
	websocket := u.Scheme == "ws" || u.Scheme == "wss"
	if (!websocket && u.Scheme != "tcp" && u.Scheme != "tls") ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(value, "#") ||
		(!websocket && u.Path != "") {
		return "", "", 0, errors.New("expected tcp://, tls://, ws:// or wss://, with no user, query or fragment; a path only on ws or wss")
	}
	host, port, err = splitAddress(u.Host)
	if err != nil {
		return "", "", 0, err
	}
	if host == "" || port == 0 {
		return "", "", 0, errors.New("expected a nonempty host and port from 1 to 65535")
	}
	host, _, _ = canonicalHost(host)
	host = strings.ToLower(host)
	if websocket && u.Path == "" {
		value += "/"
		u.Path = "/"
	}
	b.peerURL = u
	return value, host, port, nil
}
