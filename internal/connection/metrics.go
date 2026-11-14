package connection

import (
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
)

// Metrics holds the series moved by connections. All listeners share one
// instance, registered during startup before any connection or scrape.
type Metrics struct {
	listeners, readers, writers *metrics.Gauge
	open                        *metrics.Gauge
	accepted                    *metrics.Counter
	receivedBytes               *metrics.Counter
	sentBytes                   *metrics.Counter
	refused                     [256]*metrics.Counter
	disconnected                [256]*metrics.Counter
	connectTimeout              *metrics.Counter
	tlsHandshake                *metrics.Counter
	proxyHeader                 *metrics.Counter
	client                      *metrics.Counter
	lost                        *metrics.Counter
	writerFull                  *metrics.Counter
	publishStalled              *metrics.Counter
	publishes                   [3]*metrics.Counter
	publishRefused              map[string]*metrics.Counter
	publishThrottled            *metrics.DurationCounter
	closureReasons              map[*metrics.Counter]Closure // immutable identities of counted history events
}

// NewMetrics registers the connection series, including every reason at zero
// (metrics.md, Rules). Call it once for a broker's registry.
func NewMetrics(reg *metrics.Registry) *Metrics {
	m := &Metrics{
		closureReasons: make(map[*metrics.Counter]Closure),
		listeners:      reg.Goroutine("listener"), readers: reg.Goroutine("reader"), writers: reg.Goroutine("writer"),
		open:             reg.Gauge("saguin_connections_open"),
		accepted:         reg.Counter("saguin_connections_accepted_total"),
		receivedBytes:    reg.Counter("saguin_bytes_received_total"),
		sentBytes:        reg.Counter("saguin_bytes_sent_total"),
		publishThrottled: reg.DurationCounter("saguin_publish_throttled_seconds_total"),
	}
	for _, code := range [...]byte{0x84, 0x85, 0x86, 0x87, 0x90, 0x9A, 0x9B, 0x95, 0x83, 0x8C, 0x97, 0x81, 0x82} {
		reason := metrics.ReasonLabel(packet.CONNACK, code)
		m.refused[code] = reg.Counter("saguin_connections_refused_total", "reason", reason)
		m.closureReasons[m.refused[code]] = Closure{typ: packet.CONNACK, code: code}
	}
	m.connectTimeout = reg.Counter("saguin_connections_refused_total", "reason", "connect_timeout")
	m.tlsHandshake = reg.Counter("saguin_connections_refused_total", "reason", "tls_handshake")
	m.proxyHeader = reg.Counter("saguin_connections_refused_total", "reason", "proxy_header")
	m.closureReasons[m.connectTimeout] = Closure{code: closureConnectTimeout}
	m.closureReasons[m.tlsHandshake] = Closure{code: closureTLSHandshake}
	m.closureReasons[m.proxyHeader] = Closure{code: closureProxyHeader}
	for _, code := range [...]byte{0x80, 0x81, 0x82, 0x8B, 0x8D, 0x8E, 0x93, 0x94, 0x95, 0x98, 0x9A, 0x9B, 0x9E, 0x87, 0x90, 0x97, 0x83} {
		reason := metrics.ReasonLabel(packet.DISCONNECT, code)
		m.disconnected[code] = reg.Counter("saguin_disconnects_total", "reason", reason)
		m.closureReasons[m.disconnected[code]] = Closure{typ: packet.DISCONNECT, code: code}
	}
	m.client = reg.Counter("saguin_disconnects_total", "reason", "client")
	m.lost = reg.Counter("saguin_disconnects_total", "reason", "connection_lost")
	m.writerFull = reg.Counter("saguin_disconnects_total", "reason", "writer_full")
	m.publishStalled = reg.Counter("saguin_disconnects_total", "reason", "publish_stalled")
	m.closureReasons[m.writerFull] = Closure{code: closureWriterFull}
	m.closureReasons[m.publishStalled] = Closure{code: closurePublishStalled}
	for i, qos := range [...]string{"0", "1", "2"} {
		m.publishes[i] = reg.Counter("saguin_publishes_received_total", "qos", qos)
	}
	m.publishRefused = make(map[string]*metrics.Counter)
	for _, cause := range [...]string{"not_authorized", "topic_invalid", "headers"} {
		m.publishRefused[cause] = reg.Counter("saguin_publish_refused_total", "cause", cause)
	}
	return m
}
