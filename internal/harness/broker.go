package harness

import (
	"cmp"
	"crypto/pbkdf2"
	"crypto/sha512"
	"encoding/base64"
	byteorder "encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/config"
)

// Options describes the process and configuration used by every broker test.
// Dir is required and is retained across launches to test restarts.
type Options struct {
	Dir            string
	WriteTimeout   time.Duration
	MaxConnections int
	PIDFile        string
	Stderr         *os.File
	// LogLevel is broker.log_level, written where set: debug, info, warn
	// or error (config.md, broker).
	LogLevel string
	// DeliveryAdmissionDelay and DeliveryPeerTimeout are broker's
	// delivery_admission_delay and delivery_peer_timeout, written in
	// milliseconds where set; NoDeliveryAdmission writes the delay none
	// (config.md, broker).
	DeliveryAdmissionDelay time.Duration
	DeliveryPeerTimeout    time.Duration
	// The keys below are written where set; zero leaves config.md's
	// default. Durations but WriteTimeout are written in whole seconds.
	MinProtocolVersion   string        // broker.mqtt.min_protocol_version: "3.1.1" or "5"
	MaxInflightPerClient int           // broker.qos2.max_inflight_per_client
	MaxPacketSize        int           // broker.limits.max_packet_size, in bytes
	MaxConnectSize       int           // broker.limits.max_connect_size, in bytes
	MaxClientIDLength    int           // broker.limits.max_client_id_length
	MaxKeepalive         time.Duration // broker.limits.max_keepalive
	MaxSessionExpiry     time.Duration // broker.limits.max_session_expiry
	ConnectTimeout       time.Duration // broker.limits.connect_timeout
	MaxConnectRate       int           // broker.limits.max_connect_rate; NoConnectRate writes none
	MaxTopicLength       int           // broker.limits.max_topic_length
	MaxTopicLevels       int           // broker.limits.max_topic_levels
	MaxSubscriptions     int           // broker.limits.max_subscriptions
	MaxIncomingMemory    int           // broker.limits.max_incoming_memory, in bytes
	MaxOutgoingMemory    int           // broker.limits.max_outgoing_memory, in bytes
	MaxInflight          int           // broker.limits.max_inflight
	PublishRate          int           // broker.limits.publish_rate
	PublishBytes         int           // broker.limits.publish_bytes, in bytes
	// RetainedMaxBytes and RetainedRetentionPeriod are broker.retained's
	// max_bytes, in bytes, and retention_period.
	RetainedMaxBytes        int
	RetainedRetentionPeriod time.Duration
	// BroadcastRetentionPeriod and BroadcastRetentionBytes are
	// broker.broadcast's retention_period, in whole seconds, and
	// retention_bytes, in bytes: $broadcast's retention (config.md).
	BroadcastRetentionPeriod time.Duration
	BroadcastRetentionBytes  int
	// Operations adds broker.operations with one loopback tcp listener,
	// whose address Broker.OpsAddr holds: the operator's /metrics
	// (design.md, Operations, HTTP).
	Operations bool
	// OpsListeners are broker.operations' listeners beside Operations'
	// one, in configuration order (OpsListener), each one's address or
	// path in Broker.Ops; OpsPasswordFile is broker.operations'
	// password_file, the operators' file, written where set.
	OpsListeners    []OpsListener
	OpsPasswordFile string
	// PublishCommitInterval is broker.storage.publish_commit_interval,
	// written in milliseconds, and PublishCommitMaxRecords its
	// publish_commit_max_records; each is written where set, without the
	// other.
	PublishCommitInterval   time.Duration
	PublishCommitMaxRecords int
	// ReadConnections is broker.storage.read_connections, written where
	// set; zero leaves config.md's default.
	ReadConnections int
	// TCPListeners is how many loopback TCP listeners
	// broker.mqtt.listen.tcp holds, named tcp1, tcp2 and so on; zero or
	// one writes one, unnamed.
	TCPListeners int
	// FilePath is broker.storage.file_path; "" for Dir/saguin.db.
	FilePath string
	// StorageMode is broker.storage.mode: "full", "normal" or "memory";
	// SyncInterval its sync_interval, written in milliseconds, MemoryDir
	// its memory_dir, and SnapshotInterval its snapshot_interval, written
	// in whole seconds; each is written where set (config.md,
	// broker.storage).
	StorageMode      string
	SyncInterval     time.Duration
	MemoryDir        string
	SnapshotInterval time.Duration
	// ConfigPath is the path the binary is given for its configuration;
	// "" for the file WriteConfig writes. A test that names another path,
	// a named pipe say, makes it hold WriteConfig's configuration itself.
	ConfigPath string
	// Under is a command, with its arguments, that the binary runs under
	// (strace and its options, say), the binary and its flags following
	// them: PID, Signal and Wait are then that command's.
	Under []string
	// Channels is the channels: mapping (config.md, channels.<name>), in
	// the order given; none leaves the key out.
	Channels []Channel
	// IncludedChannels, where set, is a group of channels written to
	// IncludedChannelsName in Dir and included by that path: channels: is
	// then a list of Channels' group, where set, then the !include
	// (config.md: "A group is written in place or as !include <file>").
	IncludedChannels []Channel
	// Login is broker.mqtt's password_file and allow_anonymous, and
	// ListenerLogin each MQTT TCP listener's own, in configuration order
	// (config.md, broker.mqtt and A listener); each key is written where
	// set.
	Login         Login
	ListenerLogin []Login
	// ACLFile is broker.mqtt.acl_file, written where set (config.md,
	// broker.mqtt; design.md, Access and reload).
	ACLFile string
	// ListenerTLS is each MQTT TCP listener's tls block, in configuration
	// order (config.md, A listener, tls); one with no CertFile, or none
	// where ListenerTLS stops short, writes none.
	ListenerTLS []TLS
	// Unix is broker.mqtt.listen.unix's listeners, in configuration order,
	// none leaving the key out; two or more are named unix1, unix2 and so on
	// (config.md, A listener). The TCP listeners stay beside them.
	Unix []Unix
	// WS is broker.mqtt.listen.ws's listeners, in configuration order,
	// none leaving the key out, each on a loopback address of its own
	// (Broker.WS); two or more are named ws1, ws2 and so on (config.md, A
	// listener). The TCP listeners stay beside them.
	WS []WS
	// Bridges is the bridges: mapping (config.md, bridges.<name>), in the
	// order given; none leaves the key out.
	Bridges []Bridge
}

// Bridge is one bridge of Options.Bridges: its name, peer, client_id and
// rules, and each other key where set, "" and zero leaving config.md's
// default.
type Bridge struct {
	Name, Peer, ClientID   string
	Username, PasswordFile string
	SessionExpiry          time.Duration // session_expiry, in whole seconds
	ReceiveMaximum         int           // receive_maximum
	AckInterval            time.Duration // ack_interval, in milliseconds
	TLS                    *BridgeTLS    // tls, none where nil
	Topics                 []BridgeRule
}

// BridgeTLS is a bridge's tls block (config.md, bridges.<name>, tls):
// ca_file, min_version, and cert_file and key_file, each written where set.
// A block with nothing set is written empty, which config.md refuses
// ("Neither bridges: nor a tls block is empty").
type BridgeTLS struct {
	CAFile, CertFile, KeyFile string
	MinVersion                string // "1.2" or "1.3"
}

// BridgeRule is one rule of a bridge's topics: its filter, topic (none
// where ""), direction (in, out or both) and qos, always written.
type BridgeRule struct {
	Filter, Topic, Direction string
	QoS                      byte
}

// appendBridges appends the bridges: mapping for bs, nothing for none.
func appendBridges(data []byte, bs []Bridge) []byte {
	if len(bs) == 0 {
		return data
	}
	data = append(data, "bridges:\n"...)
	for i := range bs {
		b := &bs[i]
		data = fmt.Appendf(data, "  %s:\n    peer: %q\n    client_id: %q\n", b.Name, b.Peer, b.ClientID)
		if b.Username != "" {
			data = fmt.Appendf(data, "    username: %q\n", b.Username)
		}
		if b.PasswordFile != "" {
			data = fmt.Appendf(data, "    password_file: %q\n", b.PasswordFile)
		}
		if b.SessionExpiry != 0 {
			data = fmt.Appendf(data, "    session_expiry: %ds\n", b.SessionExpiry/time.Second)
		}
		if b.ReceiveMaximum != 0 {
			data = fmt.Appendf(data, "    receive_maximum: %d\n", b.ReceiveMaximum)
		}
		if b.AckInterval != 0 {
			data = fmt.Appendf(data, "    ack_interval: %dms\n", b.AckInterval/time.Millisecond)
		}
		if t := b.TLS; t != nil {
			data = append(data, "    tls:\n"...)
			for _, key := range [][2]string{{"ca_file", t.CAFile}, {"cert_file", t.CertFile}, {"key_file", t.KeyFile}, {"min_version", t.MinVersion}} {
				if key[1] != "" {
					data = fmt.Appendf(data, "      %s: %q\n", key[0], key[1])
				}
			}
		}
		data = append(data, "    topics:\n"...)
		for _, r := range b.Topics {
			data = fmt.Appendf(data, "      - filter: %q\n", r.Filter)
			if r.Topic != "" {
				data = fmt.Appendf(data, "        topic: %q\n", r.Topic)
			}
			data = fmt.Appendf(data, "        direction: %s\n        qos: %d\n", r.Direction, r.QoS)
		}
	}
	return data
}

// WS is a ws listener: its tls block, none where TLS is nil or names no
// CertFile; same_origin as written, "true" or "false", "" leaving
// config.md's default; and allowed_origins, none leaving the key out.
type WS struct {
	TLS            *TLS
	SameOrigin     string
	AllowedOrigins []string
}

// appendWS appends broker.mqtt.listen.ws for ls, at addrs, nothing for
// none.
func appendWS(data []byte, ls []WS, addrs []string) []byte {
	if len(ls) == 0 {
		return data
	}
	data = append(data, "      ws:\n"...)
	indent := "        "
	for i, l := range ls {
		if len(ls) > 1 {
			data = fmt.Appendf(data, "        - name: ws%d\n", i+1)
			indent = "          "
		}
		data = fmt.Appendf(data, "%saddress: %q\n", indent, addrs[i])
		data = appendTLS(data, indent, l.TLS)
		if l.SameOrigin != "" {
			data = fmt.Appendf(data, "%ssame_origin: %s\n", indent, l.SameOrigin)
		}
		if len(l.AllowedOrigins) > 0 {
			data = fmt.Appendf(data, "%sallowed_origins:\n", indent)
			for _, o := range l.AllowedOrigins {
				data = fmt.Appendf(data, "%s  - %q\n", indent, o)
			}
		}
	}
	return data
}

// Unix is a unix listener: its path, at most 107 bytes (config.md: "the
// socket file, at most 107 bytes"), so a test keeps it in a short folder
// (testkit.ShortDir); its mode, "" for config.md's default, "0660"; and
// proxy_protocol.
type Unix struct {
	Path, Mode    string
	ProxyProtocol bool
}

// appendUnix appends broker.mqtt.listen.unix for ls, nothing for none.
func appendUnix(data []byte, ls []Unix) []byte {
	if len(ls) == 0 {
		return data
	}
	data = append(data, "      unix:\n"...)
	indent := "        "
	for i, l := range ls {
		if len(ls) > 1 {
			data = fmt.Appendf(data, "        - name: unix%d\n", i+1)
			indent = "          "
		}
		data = fmt.Appendf(data, "%spath: %q\n", indent, l.Path)
		if l.Mode != "" {
			data = fmt.Appendf(data, "%smode: %q\n", indent, l.Mode)
		}
		if l.ProxyProtocol {
			data = fmt.Appendf(data, "%sproxy_protocol: true\n", indent)
		}
	}
	return data
}

// TLS is a listener's tls block: cert_file and key_file, written together
// where CertFile is set, and each other key where set, "" leaving
// config.md's default; RequireCertificate is written as it stands, "true"
// or "false", and MinVersion as "1.2" or "1.3".
type TLS struct {
	CertFile, KeyFile  string
	MinVersion         string
	ClientCAFile       string
	RequireCertificate string
}

// appendTLS appends t, a listener's tls block where t is not nil and
// t.CertFile is set, indented by indent.
func appendTLS(data []byte, indent string, t *TLS) []byte {
	if t == nil || t.CertFile == "" {
		return data
	}
	data = fmt.Appendf(data, "%stls:\n%s  cert_file: %q\n%s  key_file: %q\n", indent, indent, t.CertFile, indent, t.KeyFile)
	if t.MinVersion != "" {
		data = fmt.Appendf(data, "%s  min_version: %q\n", indent, t.MinVersion)
	}
	if t.ClientCAFile != "" {
		data = fmt.Appendf(data, "%s  client_ca_file: %q\n", indent, t.ClientCAFile)
	}
	if t.RequireCertificate != "" {
		data = fmt.Appendf(data, "%s  require_certificate: %s\n", indent, t.RequireCertificate)
	}
	return data
}

// Login is a password_file and an allow_anonymous, broker.mqtt's or a
// listener's: "" leaves either key out, so it takes config.md's default;
// AllowAnonymous is written as it stands, "true" or "false".
type Login struct {
	PasswordFile   string
	AllowAnonymous string
}

// appendLogin appends a's keys, each where set, indented by indent.
func appendLogin(data []byte, indent string, a Login) []byte {
	if a.PasswordFile != "" {
		data = fmt.Appendf(data, "%spassword_file: %q\n", indent, a.PasswordFile)
	}
	if a.AllowAnonymous != "" {
		data = fmt.Appendf(data, "%sallow_anonymous: %s\n", indent, a.AllowAnonymous)
	}
	return data
}

// SevenHash is config.md's $7$ hash of password, as a password_file line
// holds it after the user name: PBKDF2-HMAC-SHA512 over iterations, with
// salt, its 64-byte output, salt and output in standard base64. It is made
// from config.md's definition with Go's crypto, never the broker's code.
func SevenHash(password string, iterations int, salt []byte) (string, error) {
	key, err := pbkdf2.Key(sha512.New, password, salt, iterations, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$7$%d$%s$%s", iterations, base64.StdEncoding.EncodeToString(salt), base64.StdEncoding.EncodeToString(key)), nil
}

// Channel is one channel of Options.Channels: its name and type, and its
// filter, start, max_bytes and retention, a queue's visibility_timeout,
// retry and its dead-letter channel's retention, where set; "" and zero
// leave config.md's default.
type Channel struct {
	Name, Type, Filter       string
	Start                    string        // start: floor or tail
	MaxBytes                 int           // max_bytes, in bytes
	RetentionPeriod          time.Duration // retention_period, in whole seconds
	NoRetentionPeriod        bool          // writes retention_period: none, RetentionPeriod then left unset
	RetentionBytes           int           // retention_bytes, in bytes
	TombstoneRetentionPeriod time.Duration // tombstone_retention_period (latest), in whole seconds
	VisibilityTimeout        time.Duration // visibility_timeout (queue), in whole seconds
	RetryMode                string        // retry.mode (queue): fixed, linear or exponential
	RetryDelay               time.Duration // retry.delay (queue), in milliseconds
	RetryMaxAttempts         int           // retry.max_attempts (queue)
	DLQRetentionPeriod       time.Duration // dlq_retention_period (queue), in whole seconds
	DLQRetentionBytes        int           // dlq_retention_bytes (queue), in bytes
}

// WriteConfig writes the one configuration shared by process and in-process
// tests: a store in o.StorageMode, config.md's default full where none, and
// one loopback TCP listener (o.TCPListeners for more), with all other
// defaults. Its ports stay reserved (reserveAddrs) for the life of the
// process: whatever runs on the configuration binds them, and nothing else
// is given them.
func WriteConfig(o *Options) (string, error) {
	path, _, err := writeConfig(o)
	return path, err
}

// writeConfig is WriteConfig, returning what releases its ports'
// reservations, which Start hands to its Broker.
func writeConfig(o *Options) (path string, release func(), err error) {
	if o.Dir == "" {
		return "", nil, errors.New("broker configuration requires Dir")
	}
	// The MQTT TCP listeners' addresses, then the ws listeners', then the
	// operations listener's where asked.
	listeners := max(o.TCPListeners, 1)
	n := listeners + len(o.WS)
	if o.Operations {
		n++
	}
	all, release, err := reserveAddrs(n + opsTCP(o))
	if err != nil {
		return "", nil, err
	}
	addrs := all[:listeners]
	ops, err := opsAddresses(o, all[n:])
	if err != nil {
		release()
		return "", nil, err
	}
	data := fmt.Appendf(nil, `broker:
  id: harness
  storage:
    file_path: %q
    default_retention_period: 1d
    default_retention_bytes: 1GiB
`, cmp.Or(o.FilePath, filepath.Join(o.Dir, "saguin.db")))
	if o.PublishCommitInterval != 0 {
		data = fmt.Appendf(data, "    publish_commit_interval: %dms\n", o.PublishCommitInterval/time.Millisecond)
	}
	if o.PublishCommitMaxRecords != 0 {
		data = fmt.Appendf(data, "    publish_commit_max_records: %d\n", o.PublishCommitMaxRecords)
	}
	if o.ReadConnections != 0 {
		data = fmt.Appendf(data, "    read_connections: %d\n", o.ReadConnections)
	}
	if o.StorageMode != "" {
		data = fmt.Appendf(data, "    mode: %s\n", o.StorageMode)
	}
	if o.SyncInterval != 0 {
		data = fmt.Appendf(data, "    sync_interval: %dms\n", o.SyncInterval/time.Millisecond)
	}
	if o.MemoryDir != "" {
		data = fmt.Appendf(data, "    memory_dir: %q\n", o.MemoryDir)
	}
	if o.SnapshotInterval != 0 {
		data = fmt.Appendf(data, "    snapshot_interval: %ds\n", o.SnapshotInterval/time.Second)
	}
	data = append(data, "  mqtt:\n    listen:\n      tcp:\n"...)
	// own is the i-th listener's own keys, none where ListenerLogin stops
	// short.
	own := func(i int) Login {
		if i < len(o.ListenerLogin) {
			return o.ListenerLogin[i]
		}
		return Login{}
	}
	// secured is the i-th listener's tls block, nil where ListenerTLS
	// stops short.
	secured := func(i int) *TLS {
		if i < len(o.ListenerTLS) {
			return &o.ListenerTLS[i]
		}
		return nil
	}
	if len(addrs) == 1 {
		data = fmt.Appendf(data, "        address: %q\n", addrs[0])
		data = appendLogin(data, "        ", own(0))
		data = appendTLS(data, "        ", secured(0))
	} else {
		for i, addr := range addrs {
			data = fmt.Appendf(data, "        - name: tcp%d\n          address: %q\n", i+1, addr)
			data = appendLogin(data, "          ", own(i))
			data = appendTLS(data, "          ", secured(i))
		}
	}
	data = appendUnix(data, o.Unix)
	data = appendWS(data, o.WS, all[listeners:listeners+len(o.WS)])
	if o.MinProtocolVersion != "" {
		data = fmt.Appendf(data, "    min_protocol_version: %q\n", o.MinProtocolVersion)
	}
	data = appendLogin(data, "    ", o.Login)
	if o.ACLFile != "" {
		data = fmt.Appendf(data, "    acl_file: %q\n", o.ACLFile)
	}
	if o.Operations || len(o.OpsListeners) > 0 {
		data = appendOperations(data, o, all[n-1], ops)
	}
	if o.PIDFile != "" {
		data = fmt.Appendf(data, "  pid_file: %q\n", o.PIDFile)
	}
	if o.LogLevel != "" {
		data = fmt.Appendf(data, "  log_level: %s\n", o.LogLevel)
	}
	switch {
	case o.DeliveryAdmissionDelay == NoDeliveryAdmission:
		data = append(data, "  delivery_admission_delay: none\n"...)
	case o.DeliveryAdmissionDelay != 0:
		data = fmt.Appendf(data, "  delivery_admission_delay: %dms\n", o.DeliveryAdmissionDelay/time.Millisecond)
	}
	if o.DeliveryPeerTimeout != 0 {
		data = fmt.Appendf(data, "  delivery_peer_timeout: %dms\n", o.DeliveryPeerTimeout/time.Millisecond)
	}
	if o.MaxInflightPerClient != 0 {
		data = fmt.Appendf(data, "  qos2:\n    max_inflight_per_client: %d\n", o.MaxInflightPerClient)
	}
	if o.RetainedMaxBytes != 0 || o.RetainedRetentionPeriod != 0 {
		data = append(data, "  retained:\n"...)
		if o.RetainedMaxBytes != 0 {
			data = fmt.Appendf(data, "    max_bytes: %d\n", o.RetainedMaxBytes)
		}
		if o.RetainedRetentionPeriod != 0 {
			data = fmt.Appendf(data, "    retention_period: %ds\n", o.RetainedRetentionPeriod/time.Second)
		}
	}
	if o.BroadcastRetentionPeriod != 0 || o.BroadcastRetentionBytes != 0 {
		data = append(data, "  broadcast:\n"...)
		data = appendRetention(data, "    ", o.BroadcastRetentionPeriod, o.BroadcastRetentionBytes)
	}
	var limits []byte
	for _, l := range []struct {
		key   string
		value int64
		unit  string
	}{
		{"write_timeout", int64(o.WriteTimeout / time.Millisecond), "ms"},
		{"max_connections", int64(o.MaxConnections), ""},
		{"max_packet_size", int64(o.MaxPacketSize), ""},
		{"max_connect_size", int64(o.MaxConnectSize), ""},
		{"max_client_id_length", int64(o.MaxClientIDLength), ""},
		{"max_keepalive", int64(o.MaxKeepalive / time.Second), "s"},
		{"max_session_expiry", int64(o.MaxSessionExpiry / time.Second), "s"},
		{"connect_timeout", int64(o.ConnectTimeout / time.Second), "s"},
		{"max_connect_rate", int64(max(o.MaxConnectRate, 0)), ""},
		{"max_topic_length", int64(o.MaxTopicLength), ""},
		{"max_topic_levels", int64(o.MaxTopicLevels), ""},
		{"max_subscriptions", int64(o.MaxSubscriptions), ""},
		{"max_incoming_memory", int64(o.MaxIncomingMemory), ""},
		{"max_outgoing_memory", int64(o.MaxOutgoingMemory), ""},
		{"max_inflight", int64(o.MaxInflight), ""},
		{"publish_rate", int64(o.PublishRate), ""},
		{"publish_bytes", int64(o.PublishBytes), ""},
	} {
		if l.value != 0 {
			limits = fmt.Appendf(limits, "    %s: %d%s\n", l.key, l.value, l.unit)
		}
	}
	if o.MaxConnectRate == NoConnectRate {
		limits = append(limits, "    max_connect_rate: none\n"...)
	}
	if len(limits) != 0 {
		data = append(append(data, "  limits:\n"...), limits...)
	}
	switch {
	case len(o.IncludedChannels) != 0:
		included := filepath.Join(o.Dir, IncludedChannelsName)
		if err := os.WriteFile(included, appendChannels(nil, "", o.IncludedChannels), 0o600); err != nil {
			release()
			return "", nil, err
		}
		data = append(data, "channels:\n"...)
		if len(o.Channels) != 0 {
			data = appendChannels(append(data, "  -\n"...), "    ", o.Channels)
		}
		data = fmt.Appendf(data, "  - !include %s\n", included)
	case len(o.Channels) != 0:
		data = appendChannels(append(data, "channels:\n"...), "  ", o.Channels)
	}
	data = appendBridges(data, o.Bridges)
	path = filepath.Join(o.Dir, "saguin.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		release()
		return "", nil, err
	}
	return path, release, nil
}

// IncludedChannelsName is the file, in Options.Dir, that holds
// Options.IncludedChannels.
const IncludedChannelsName = "channels.yaml"

// appendChannels appends cs as one group of channels, each name indented
// by indent and its keys two spaces further.
func appendChannels(data []byte, indent string, cs []Channel) []byte {
	keys := indent + "  "
	for i := range cs {
		c := &cs[i]
		data = fmt.Appendf(data, "%s%s:\n%stype: %s\n", indent, c.Name, keys, c.Type)
		if c.Filter != "" {
			data = fmt.Appendf(data, "%sfilter: %q\n", keys, c.Filter)
		}
		if c.Start != "" {
			data = fmt.Appendf(data, "%sstart: %s\n", keys, c.Start)
		}
		if c.MaxBytes != 0 {
			data = fmt.Appendf(data, "%smax_bytes: %d\n", keys, c.MaxBytes)
		}
		if c.NoRetentionPeriod {
			data = fmt.Appendf(data, "%sretention_period: none\n", keys)
		}
		data = appendRetention(data, keys, c.RetentionPeriod, c.RetentionBytes)
		if c.TombstoneRetentionPeriod != 0 {
			data = fmt.Appendf(data, "%stombstone_retention_period: %ds\n", keys, c.TombstoneRetentionPeriod/time.Second)
		}
		if c.VisibilityTimeout != 0 {
			data = fmt.Appendf(data, "%svisibility_timeout: %ds\n", keys, c.VisibilityTimeout/time.Second)
		}
		data = appendRetry(data, keys, c)
		if c.DLQRetentionPeriod != 0 {
			data = fmt.Appendf(data, "%sdlq_retention_period: %ds\n", keys, c.DLQRetentionPeriod/time.Second)
		}
		if c.DLQRetentionBytes != 0 {
			data = fmt.Appendf(data, "%sdlq_retention_bytes: %d\n", keys, c.DLQRetentionBytes)
		}
	}
	return data
}

// appendRetry appends c's retry block, indented by indent, holding each of
// mode, delay, in milliseconds, and max_attempts where set, and nothing
// where none is.
func appendRetry(data []byte, indent string, c *Channel) []byte {
	if c.RetryMode == "" && c.RetryDelay == 0 && c.RetryMaxAttempts == 0 {
		return data
	}
	data = fmt.Appendf(data, "%sretry:\n", indent)
	if c.RetryMode != "" {
		data = fmt.Appendf(data, "%s  mode: %s\n", indent, c.RetryMode)
	}
	if c.RetryDelay != 0 {
		data = fmt.Appendf(data, "%s  delay: %dms\n", indent, c.RetryDelay/time.Millisecond)
	}
	if c.RetryMaxAttempts != 0 {
		data = fmt.Appendf(data, "%s  max_attempts: %d\n", indent, c.RetryMaxAttempts)
	}
	return data
}

// appendRetention appends a log's retention_period, in whole seconds, and
// retention_bytes, each where set, indented by indent.
func appendRetention(data []byte, indent string, period time.Duration, bytes int) []byte {
	if period != 0 {
		data = fmt.Appendf(data, "%sretention_period: %ds\n", indent, period/time.Second)
	}
	if bytes != 0 {
		data = fmt.Appendf(data, "%sretention_bytes: %d\n", indent, bytes)
	}
	return data
}

// reserveAddrs hands out n loopback ports, none of them handed out before
// while its reservation holds, and returns them with what releases the
// reservations. A reservation is a socket bound to its port with
// SO_REUSEADDR and never listening. While it is held, the kernel gives that
// port to no socket asking for any port, in this process or another: no
// other reservation, bridge peer or client connection gets it, so a
// broker's port is never another listener's. The broker still binds it by
// its number, as Go's listeners set SO_REUSEADDR too: only a socket naming
// the port can take it.
func reserveAddrs(n int) (addrs []string, release func(), err error) {
	fds := make([]int, 0, n)
	release = func() {
		for _, fd := range fds {
			_ = syscall.Close(fd) // a bound socket never listening: nothing is lost
		}
	}
	for range n {
		fd, port, err := reservePort()
		if err != nil {
			release()
			return nil, nil, err
		}
		fds = append(fds, fd)
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", port))
	}
	return addrs, sync.OnceFunc(release), nil
}

// reservePort binds a new socket to a loopback port the kernel chooses, with
// SO_REUSEADDR, and returns the socket and the port. The socket closes on
// exec, so no broker inherits it.
func reservePort() (fd, port int, err error) {
	fd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, 0, err
	}
	if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err == nil {
		err = syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}})
	}
	var bound syscall.Sockaddr
	if err == nil {
		bound, err = syscall.Getsockname(fd)
	}
	if err != nil {
		_ = syscall.Close(fd)
		return -1, 0, fmt.Errorf("reserving a loopback port: %w", err)
	}
	return fd, bound.(*syscall.SockaddrInet4).Port, nil
}

// Broker holds a child process and its captured log. Process state and waitErr
// are published by done, so callers need no lock when waiting more than once.
type Broker struct {
	Addr         string   // the first MQTT TCP listener's address
	Addrs        []string // every MQTT TCP listener's address, in configuration order
	Unix         []Unix   // every MQTT unix listener, in configuration order (Options.Unix)
	WS           []string // every ws listener's address, in configuration order (Options.WS)
	OpsAddr      string   // the operations listener's address, "" for none (Options.Operations)
	Ops          []string // each of Options.OpsListeners' address or socket path, in order
	DB           string
	RAM          string // memory mode's RAM copy, <id>.db in memory_dir (design.md, Store); "" in the other modes
	Config       string // the configuration WriteConfig wrote, which an operator edits before a SIGUSR1 (design.md, Access and reload)
	cmd          *exec.Cmd
	binary       string
	own          []string // the broker's own command line, the binary on its configuration, under Options.Under's command where set
	done         chan struct{}
	waitErr      error
	logPath      string
	writeTimeout time.Duration
	release      func() // releases the reservations of the ports WriteConfig chose (Release)
}

// Launch is Start and Listening: it starts binary on WriteConfig's
// configuration and waits until its listener accepts a probe, or the
// process exits. An early exit retains its *exec.ExitError and diagnostic
// output.
func Launch(binary string, o *Options) (*Broker, error) {
	b, err := Start(binary, o)
	if err != nil {
		return nil, err
	}
	if err := b.Listening(); err != nil {
		b.Release() // the broker has exited or been killed, and no caller holds it
		return nil, err
	}
	return b, nil
}

// Start writes WriteConfig's configuration and starts binary on it (or on
// o.ConfigPath), returning as soon as the process runs the binary, without
// waiting for anything of its own.
func Start(binary string, o *Options) (*Broker, error) {
	path, release, err := writeConfig(o)
	if err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			release()
		}
	}()
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	own := []string{binary, "--config", cmp.Or(o.ConfigPath, path)}
	args := slices.Concat(o.Under, own)
	b := &Broker{
		Addr: cfg.Broker.MQTT.Listen.TCP[0].Address, DB: cfg.Broker.Storage.FilePath, Config: path,
		cmd: exec.Command(args[0], args[1:]...), done: make(chan struct{}),
		binary: binary, own: own, writeTimeout: cfg.Broker.Limits.WriteTimeout, release: release,
	}
	for _, l := range cfg.Broker.MQTT.Listen.TCP {
		b.Addrs = append(b.Addrs, l.Address)
	}
	b.Unix = o.Unix
	for _, l := range cfg.Broker.MQTT.Listen.WS {
		b.WS = append(b.WS, l.Address)
	}
	if o.StorageMode == "memory" {
		b.RAM = filepath.Join(cfg.Broker.Storage.MemoryDir, cfg.Broker.ID+".db")
	}
	if ops := cfg.Broker.Operations; ops != nil && o.Operations {
		b.OpsAddr = ops.Listen.TCP[0].Address
	}
	// Operations' own tcp listener, where there is one, comes first.
	tcp, unix := 0, 0
	if o.Operations {
		tcp = 1
	}
	for _, l := range o.OpsListeners {
		if l.Unix != nil {
			b.Ops = append(b.Ops, cfg.Broker.Operations.Listen.Unix[unix].Path)
			unix++
		} else {
			b.Ops = append(b.Ops, cfg.Broker.Operations.Listen.TCP[tcp].Address)
			tcp++
		}
	}
	var log *os.File
	if o.Stderr != nil {
		b.cmd.Stderr = o.Stderr
	} else {
		b.logPath = filepath.Join(o.Dir, "saguin.log")
		log, err = os.Create(b.logPath)
		if err != nil {
			return nil, err
		}
		b.cmd.Stderr = log
	}
	err = b.cmd.Start()
	if log != nil {
		_ = log.Close() // only the child retains the writing descriptor
	}
	if err != nil {
		return nil, err
	}
	go func() {
		b.waitErr = b.cmd.Wait()
		close(b.done)
	}()
	started = true
	return b, nil
}

// Release gives back the reservations of the ports WriteConfig chose for b,
// once nothing of the test still uses them: after b has exited, and after
// the test's last dial to an address of b's, so that no later broker or
// peer answers there for b. Until then no other broker or peer is given
// them. Calling it again does nothing.
func (b *Broker) Release() { b.release() }

// Listening waits until each of b's listeners is b's own and accepts a
// probe, for at most 10 seconds in all. A listener is b's own when the
// socket listening on its address is open in b's process (listensOn): a
// listener of another process's on the address is never taken for b's,
// nor sent a probe. A broker that exits first is an error that wraps the
// *exec.ExitError of its exit and carries what it printed; one that does
// not listen in time is killed. A probe closes before sending a byte, but
// on a unix listener with proxy_protocol it first sends a LOCAL header, a
// proxy's own health check, which "closes the connection quietly, unserved:
// it is neither logged nor counted" (design.md, Flows, Transport setup).
func (b *Broker) Listening() error {
	deadline := time.Now().Add(10 * time.Second)
	for _, addr := range slices.Concat(b.Addrs, b.WS) {
		if err := b.listening("tcp", addr, nil, deadline); err != nil {
			return err
		}
	}
	for _, l := range b.Unix {
		var local []byte
		if l.ProxyProtocol {
			local = ProxyHeader(ProxyLocal, ProxyUnspec, netip.AddrPort{}, netip.AddrPort{})
		}
		if err := b.listening("unix", l.Path, local, deadline); err != nil {
			return err
		}
	}
	return nil
}

// listening is Listening for the listener at addr on network, its probe
// sending first, until deadline.
func (b *Broker) listening(network, addr string, first []byte, deadline time.Time) error {
	for {
		select {
		case <-b.done:
			return fmt.Errorf("%s exited before listening: %w\n%s", b.binary, b.exitError(), b.Log())
		default:
		}
		own, err := b.listensOn(network, addr)
		var conn net.Conn
		switch {
		case err != nil:
			return fmt.Errorf("finding the socket listening on %s: %w", addr, err)
		case !own:
			err = fmt.Errorf("no socket of the broker's listens on %s", addr)
		default:
			conn, err = net.DialTimeout(network, addr, 10*time.Millisecond)
		}
		if err == nil {
			// A probe whose LOCAL header is lost has still found the
			// listener.
			if first != nil && conn.SetWriteDeadline(deadline) == nil {
				_, _ = conn.Write(first)
			}
			_ = conn.Close()
			return nil
		}
		if !time.Now().Before(deadline) {
			_ = b.Signal(os.Kill)
			_, _ = b.Wait(time.Second)
			return fmt.Errorf("%s did not listen on %s within 10s: %w\n%s", b.binary, addr, err, b.Log())
		}
		select {
		case <-b.done:
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// listensOn reports whether a socket listening on addr, on network tcp (an
// IPv4 address) or unix (a path), is open in b's process or in one it
// started, as a command of Options.Under starts the broker. The kernel's
// socket tables, /proc/net/tcp and /proc/net/unix, give each listening
// socket's inode; /proc/<pid>/fd links each socket a process holds open to
// its inode.
func (b *Broker) listensOn(network, addr string) (bool, error) {
	listeners, err := listeningSockets(network, addr)
	if err != nil || len(listeners) == 0 {
		return false, err
	}
	for _, pid := range processTree(b.PID()) {
		dir := fmt.Sprintf("/proc/%d/fd", pid)
		fds, err := os.ReadDir(dir)
		if err != nil {
			continue // the process has ended
		}
		for _, fd := range fds {
			if link, err := os.Readlink(filepath.Join(dir, fd.Name())); err == nil && listeners[link] {
				return true, nil
			}
		}
	}
	return false, nil
}

// listeningSockets returns the sockets listening on addr, each as
// /proc/<pid>/fd links it: socket:[inode].
func listeningSockets(network, addr string) (map[string]bool, error) {
	// /proc/net/tcp: sl, local_address, rem_address, st (0A for LISTEN),
	// tx_queue:rx_queue, tr:tm->when, retrnsmt, uid, timeout, inode.
	// /proc/net/unix: Num, RefCount, Protocol, Flags (00010000 for a
	// listening socket), Type, St, Inode, Path.
	var table, local string
	var at, listening, inode, matched int
	switch network {
	case "tcp":
		ap, err := netip.ParseAddrPort(addr)
		if err != nil || !ap.Addr().Is4() {
			return nil, fmt.Errorf("%q is not an IPv4 address and port", addr)
		}
		ip := ap.Addr().As4()
		// The table writes the address as the kernel holds it, in network
		// order read as a native integer, and the port in hexadecimal.
		local = fmt.Sprintf("%08X:%04X", byteorder.NativeEndian.Uint32(ip[:]), ap.Port())
		table, at, listening, inode, matched = "/proc/net/tcp", 1, 3, 9, 10
	case "unix":
		local = addr
		table, at, listening, inode, matched = "/proc/net/unix", 7, 3, 6, 8
	default:
		return nil, fmt.Errorf("no socket table for network %q", network)
	}
	data, err := os.ReadFile(table)
	if err != nil {
		return nil, err
	}
	state := map[string]string{"tcp": "0A", "unix": "00010000"}[network]
	found := map[string]bool{}
	for line := range strings.Lines(string(data)) {
		f := strings.Fields(line)
		if len(f) >= matched && f[at] == local && f[listening] == state {
			found["socket:["+f[inode]+"]"] = true
		}
	}
	return found, nil
}

// processTree returns pid and every process it started that is still
// running, each named in /proc/<pid>/task/<tid>/children of its parent.
func processTree(pid int) []int {
	tree := []int{pid}
	for i := 0; i < len(tree); i++ {
		tasks, _ := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", tree[i]))
		for _, task := range tasks {
			data, err := os.ReadFile(task)
			if err != nil {
				continue // the task has ended
			}
			for child := range strings.FieldsSeq(string(data)) {
				if n, err := strconv.Atoi(child); err == nil {
					tree = append(tree, n)
				}
			}
		}
	}
	return tree
}

// PID is the process id while running and after exit.
func (b *Broker) PID() int { return b.cmd.Process.Pid }

// Signal sends a signal to the broker process.
func (b *Broker) Signal(sig os.Signal) error { return b.cmd.Process.Signal(sig) }

// ownProcess is the id of the process running the binary itself: the one in
// the launched process's tree (processTree) whose command line is the
// broker's own, which under Options.Under is the wrapper's child, or the
// launched process itself where it has exec'd the binary in its place.
// Where none runs it yet, or any more, it is the launched process.
func (b *Broker) ownProcess() int {
	own := strings.Join(b.own, "\x00") + "\x00"
	for _, pid := range processTree(b.PID()) {
		if cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil && string(cmdline) == own {
			return pid
		}
	}
	return b.PID()
}

// Wait returns the same state on repeated calls. A non-zero exit is conveyed
// by the state; a process that exceeds limit is a wait error carrying its
// log. It may be hung, so it is first sent SIGQUIT, on which Go's runtime
// writes every goroutine's stack to its standard error and ends it; it is
// killed only where it has not ended within quitWait. Both go to its own
// process (ownProcess): a wrapper of Options.Under, such as strace, may
// pass neither on. The kill goes to the launched process too.
func (b *Broker) Wait(limit time.Duration) (*os.ProcessState, error) {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-b.done:
		return b.cmd.ProcessState, nil
	default:
	}
	select {
	case <-b.done:
		return b.cmd.ProcessState, nil
	case <-timer.C:
		own := b.ownProcess()
		_ = syscall.Kill(own, syscall.SIGQUIT)
		timer.Reset(quitWait)
		select {
		case <-b.done:
		case <-timer.C:
			_ = syscall.Kill(own, syscall.SIGKILL)
			_ = b.Signal(os.Kill)
			<-b.done
		}
		return b.cmd.ProcessState, fmt.Errorf("broker exceeded wait limit %s; SIGQUIT asked for its goroutines' stacks, in its log\n%s", limit, b.Log())
	}
}

// quitWait bounds the wait for a broker sent SIGQUIT to write its stacks
// and end: a backstop, the end of the process being what Wait waits for.
const quitWait = 5 * time.Second

// Log returns the captured standard error, complete after Wait has returned.
func (b *Broker) Log() string {
	if b.logPath == "" {
		return ""
	}
	data, err := os.ReadFile(b.logPath)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (b *Broker) exitError() error {
	if b.waitErr != nil {
		return b.waitErr
	}
	return errors.New("broker exited without serving")
}

// StopBound is the longest a graceful stop may take (design.md, Restart,
// Graceful stop): limits.write_timeout until the remaining sockets are cut,
// limits.write_timeout more for the logger's queued lines, and 5 seconds of
// margin for the process to end.
func (b *Broker) StopBound() time.Duration {
	return 2*b.writeTimeout + 5*time.Second
}

// Stop asks for a graceful stop and requires exit status zero.
func (b *Broker) Stop() error {
	if err := b.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return b.AwaitStop()
}

// AwaitStop waits, within StopBound, for the graceful stop a SIGTERM has
// begun, and requires exit status zero: Stop's wait, for a caller that
// signals the broker itself, such as one whose launched command does not
// forward the signal.
func (b *Broker) AwaitStop() error {
	state, err := b.Wait(b.StopBound())
	if err != nil {
		return err
	}
	if !state.Success() {
		return fmt.Errorf("broker stop: %w\n%s", b.exitError(), b.Log())
	}
	return nil
}

// Metrics is b's /metrics, as an operator scrapes it from its operations
// listener (Options.Operations; design.md, Operations, HTTP: "/metrics
// returns every series in metrics.md"), each WriteTo a scrape of its own:
// what testkit.Sample and testkit.AwaitSample read.
func (b *Broker) Metrics() io.WriterTo { return metricsPage(b.OpsAddr) }

// metricsPage is the operations listener at its address, scraped by WriteTo.
type metricsPage string

// WriteTo writes one GET of /metrics to w, failing unless it answers 200
// within DefaultTimeout.
func (addr metricsPage) WriteTo(w io.Writer) (int64, error) {
	if addr == "" {
		return 0, errors.New("the broker was started without an operations listener (Options.Operations)")
	}
	client := http.Client{Timeout: DefaultTimeout}
	resp, err := client.Get("http://" + string(addr) + "/metrics")
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }() // read whole, or the scrape has failed
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET /metrics: %s", resp.Status)
	}
	return io.Copy(w, resp.Body)
}

// NoConnectRate is Options.MaxConnectRate for broker.limits.max_connect_rate
// none: no MQTT listener waits for the rate (config.md).
const NoConnectRate = -1

// NoDeliveryAdmission is Options.DeliveryAdmissionDelay for
// broker.delivery_admission_delay none: the delivery admission gate is
// disabled (config.md, broker).
const NoDeliveryAdmission time.Duration = -1

// StartBroker gives conformance and soak the same Launch lifecycle, in a
// fresh directory under make's TMPDIR, removed after the broker has stopped,
// with o's configuration, o.Dir set to that directory.
func StartBroker(binary string, o *Options) (addr string, stop func() error, err error) {
	dir, err := os.MkdirTemp("", "saguin-")
	if err != nil {
		return "", nil, err
	}
	o.Dir = dir
	b, err := Launch(binary, o)
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	return b.Addr, func() error {
		defer b.Release()
		return errors.Join(b.Stop(), os.RemoveAll(dir))
	}, nil
}
