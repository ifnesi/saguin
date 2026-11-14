// Package config reads and resolves Sagüin's configuration before startup.
package config

import (
	"crypto/tls"
	"io/fs"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/passwd"
)

// Size is a size in bytes. Zero means none where the key allows it.
type Size int64

// StorageMode selects the database's working location.
type StorageMode uint8

// Supported storage modes.
const (
	Full StorageMode = iota
	Normal
	Memory
)

// Config holds the resolved configuration, including every default.
type Config struct {
	Live      *Reloads // immutable reload generations; absent from YAML
	Broker    Broker
	Channels  []Channel
	Bridges   []Bridge
	Passwords map[string]*passwd.File // immutable startup copies, keyed by file name; absent from YAML
	ACL       *acl.File               // immutable startup policy; nil without acl_file; absent from YAML
}

// Broker holds the broker's identity and its shared settings.
type Broker struct {
	ID                     string
	LogLevel               logger.Level
	PIDFile                string
	DeliveryAdmissionDelay time.Duration
	DeliveryPeerTimeout    time.Duration
	MQTT                   MQTT
	Operations             *Operations
	Storage                Storage
	Broadcast              Broadcast
	Retained               Retained
	QoS2                   QoS2
	Limits                 Limits
}

// MQTT configures client listeners and their shared access settings.
type MQTT struct {
	Listen             Listen
	PasswordFile       string
	AllowAnonymous     bool
	MinProtocolVersion packet.Version
	ACLFile            string
}

// Operations configures HTTP listeners and the operators' access settings.
// A nil Operations means that no operations listener is configured.
type Operations struct {
	Listen            Listen
	PasswordFile      string
	MinScrapeInterval time.Duration
}

// Listen groups listeners by transport, preserving each kind's file order.
type Listen struct {
	TCP, WS, Unix []Listener
}

// Listener holds resolved transport and access settings. Fields belonging
// to another transport, or MQTT-only fields on operations, remain zero.
type Listener struct {
	Name           string
	Address        string
	Path           string
	Mode           fs.FileMode
	ProxyProtocol  bool
	PasswordFile   string
	AllowAnonymous bool
	TLS            *TLS
	SameOrigin     bool
	AllowedOrigins []string
}

// TLS names certificate files and bounds the listener's TLS handshakes.
// Parsing checks settings only; it does not read certificates or keys.
type TLS struct {
	CertFile           string
	KeyFile            string
	MinVersion         uint16
	ClientCAFile       string
	RequireCertificate bool
}

// Storage configures the single SQLite store and its retention defaults.
type Storage struct {
	FilePath                string
	Mode                    StorageMode
	DefaultRetentionPeriod  time.Duration
	DefaultRetentionBytes   Size
	PublishCommitInterval   time.Duration
	PublishCommitMaxRecords int
	ReadConnections         int
	SyncInterval            time.Duration
	MemoryDir               string
	SnapshotInterval        time.Duration
}

// Broadcast configures retention of unflagged records on $broadcast.
type Broadcast struct {
	RetentionPeriod time.Duration
	RetentionBytes  Size
}

// Retained configures the lifetime and size of current retained values.
type Retained struct {
	RetentionPeriod time.Duration
	MaxBytes        Size
}

// QoS2 configures the Receive Maximum sent in CONNACK.
type QoS2 struct {
	MaxInflightPerClient int
}

// Limits bounds clients, packets and message bytes in transit.
type Limits struct {
	MaxPacketSize     Size
	MaxTopicLength    int
	MaxClientIDLength int
	MaxHeaderCount    int
	MaxConnections    int
	MaxSubscriptions  int
	MaxTopicLevels    int
	MaxSessionExpiry  time.Duration
	MaxKeepalive      time.Duration
	PublishRate       int
	PublishBytes      Size
	WriteTimeout      time.Duration
	ConnectTimeout    time.Duration
	MaxConnectSize    Size
	MaxConnectRate    int
	MaxInflight       int
	MaxOutgoingMemory Size
	MaxIncomingMemory Size
}

const (
	day                   = 24 * time.Hour
	defaultMaxConnectSize = Size(100 << 10)
	maxMQTTPacketSize     = 268435460
	maxMQTTTwoByteInteger = 65535
	maxMQTTExpiry         = 4294967295 * time.Second
	minClientIDLength     = 23
	maxReadConnections    = 8
	defaultCommitRecords  = 256
	maxUnixPathBytes      = 107
	portBits              = 16
	decimalBase           = 10
	octalBase             = 8
	modeBits              = 9
	minScrapeInterval     = time.Minute
)

// defaults is the one source of static defaults. Defaults that depend on
// supplied keys are resolved after parsing, in resolve.
func defaults() *Config {
	return &Config{Broker: Broker{
		LogLevel:               logger.Info,
		DeliveryAdmissionDelay: 100 * time.Millisecond,
		DeliveryPeerTimeout:    time.Second,
		MQTT: MQTT{
			Listen:             Listen{TCP: []Listener{defaultListener("tcp", ":1883")}},
			AllowAnonymous:     true,
			MinProtocolVersion: packet.V311,
		},
		Storage: Storage{
			Mode:             Full,
			ReadConnections:  2,
			SyncInterval:     time.Second,
			MemoryDir:        "/dev/shm/saguin",
			SnapshotInterval: 30 * time.Minute,
		},
		QoS2: QoS2{MaxInflightPerClient: 20},
		Limits: Limits{
			MaxPacketSize:     1 << 20,
			MaxTopicLength:    1024,
			MaxClientIDLength: 256,
			MaxHeaderCount:    32,
			MaxConnections:    10000,
			MaxSubscriptions:  10000,
			MaxTopicLevels:    200,
			MaxSessionExpiry:  30 * day,
			WriteTimeout:      5 * time.Second,
			ConnectTimeout:    10 * time.Second,
			MaxConnectRate:    500,
			MaxInflight:       20,
			MaxOutgoingMemory: 224 << 20,
			MaxIncomingMemory: 32 << 20,
		},
	}}
}

// Transport defaults are shared by explicit listeners and the default TCP
// listener. Access defaults are filled by resolve from the owning section.
func defaultListener(kind, address string) Listener {
	l := Listener{Name: kind, Address: address}
	switch kind {
	case "unix":
		l.Mode = 0o660
	case "ws":
		l.SameOrigin = true
	}
	return l
}

func defaultOperations() *Operations {
	return &Operations{MinScrapeInterval: minScrapeInterval}
}

func defaultTLS() *TLS {
	return &TLS{MinVersion: tls.VersionTLS12}
}
