package config

import (
	"net/url"
	"time"
)

// Direction selects which way a bridge rule carries records.
type Direction uint8

// Supported bridge directions.
const (
	In Direction = iota
	Out
	Both
)

// Bridge holds one peer connection's resolved settings, in file order.
// PasswordFile names the password file; parsing never reads its contents.
type Bridge struct {
	Name           string
	Peer           string
	ClientID       string
	Username       string
	PasswordFile   string
	Password       []byte // immutable startup credential, absent from the resolved YAML
	SessionExpiry  time.Duration
	ReceiveMaximum int
	AckInterval    time.Duration
	TLS            *BridgeTLS
	Topics         []BridgeRule
	peerURL        *url.URL // parsed once at load; immutable after validation
}

// PeerURL returns the checked URL, including its case-normalized scheme.
// Callers must not change the returned URL.
func (b *Bridge) PeerURL() *url.URL { return b.peerURL }

// UsesTLS reports whether the checked peer transport requires TLS.
func (b *Bridge) UsesTLS() bool { return b.peerURL.Scheme == "tls" || b.peerURL.Scheme == "wss" }

// BridgeTLS names peer trust and optional client certificate files.
// A nil block uses system roots, no client certificate and TLS 1.2 or newer.
type BridgeTLS struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	MinVersion uint16
}

// BridgeRule preserves the configured filter and topic template. Filters
// contains the ordered expansions of every alternatives level in Filter.
type BridgeRule struct {
	Filter    string
	Filters   []string
	Topic     string
	Direction Direction
	QoS       byte
	template  *bridgeTopic
	captures  int
}

func defaultBridge(name string) Bridge {
	return Bridge{Name: name, SessionExpiry: day, ReceiveMaximum: 20, AckInterval: 5 * time.Millisecond}
}
