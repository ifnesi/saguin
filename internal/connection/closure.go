package connection

import (
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
)

// Closure identifies a counted history event. Its zero value describes no
// history observation.
type Closure struct {
	typ  packet.Type
	code byte
}

// MQTTClosure identifies the reason selected for a refusing CONNACK or a
// server DISCONNECT, including when a socket cut prevents that packet's write.
func MQTTClosure(typ packet.Type, code byte) Closure {
	return Closure{typ: typ, code: code}
}

// Non-MQTT reasons use an absent packet type, separate from MQTT reason codes.
const (
	closureConnectTimeout byte = iota + 1
	closureTLSHandshake
	closureProxyHeader
	closureWriterFull
	closurePublishStalled
)

// Label uses the same immutable reason names as the connection series.
func (c Closure) Label() string {
	if c.typ != 0 {
		return metrics.ReasonLabel(c.typ, c.code)
	}
	switch c.code {
	case closureConnectTimeout:
		return "connect_timeout"
	case closureTLSHandshake:
		return "tls_handshake"
	case closureProxyHeader:
		return "proxy_header"
	case closureWriterFull:
		return "writer_full"
	case closurePublishStalled:
		return "publish_stalled"
	}
	return ""
}
