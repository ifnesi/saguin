package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/topic"
)

// DialOptions is immutable establishment input. The core retains bridge
// policy; the dialer owns only one attempt and hands its socket to Conn.
type DialOptions struct {
	CleanStart bool
	Bridge     config.Bridge
	Limits     config.Limits
	Routing    *config.Routing
	Metrics    *Metrics
	Dialers    *metrics.Gauge
	TLS        *tls.Config // immutable material selected by the core for this attempt
}

// DialResult precedes all events from a new link. A TCP failure has no Conn;
// a connected socket still awaits its MQTT CONNACK to finish establishment.
type DialResult struct {
	Name string
	Err  error
}

// Dial runs on the one dialer goroutine the core starts for an attempt.
// Only it starts this link's reader and writer (R1). The common workers own
// CONNECT, CONNACK and all later packets, including establishment deadlines.
func Dial(ctx context.Context, options *DialOptions, inbound chan<- Event, clock Clock, workers *sync.WaitGroup) {
	defer workers.Done()
	options.Dialers.Add(1)
	defer options.Dialers.Add(-1)
	inbound <- Event{Kind: DialStarted, Packet: options.Bridge.Name}
	deadline := clock.Now().Add(options.Limits.ConnectTimeout)
	peer := options.Bridge.PeerURL()
	var err error
	secure := options.TLS
	if options.Bridge.UsesTLS() && secure == nil {
		err = errors.New("bridge TLS material has not been loaded")
	}
	var raw net.Conn
	if err == nil {
		dialer := net.Dialer{Deadline: deadline}
		raw, err = dialer.DialContext(ctx, "tcp", peer.Host)
	}
	result := DialResult{Name: options.Bridge.Name, Err: err}
	if err != nil {
		inbound <- Event{Kind: Dialed, Packet: result}
		return
	}
	limits := options.Limits
	limits.MaxInflight = options.Bridge.ReceiveMaximum
	established := make(chan struct{})
	var transport socket = raw.(*net.TCPConn)
	if secure != nil {
		secure = secure.Clone()
		secure.ServerName = peer.Hostname()
		if peer.Scheme == "wss" {
			secure.NextProtos = []string{"http/1.1"}
		}
		transport = tls.Client(&tlsSocket{Conn: raw, closing: make(chan struct{}),
			establishment: deadline, established: established}, secure)
	}
	// Publish the raw socket before setup so the core can cut an unfinished
	// TLS handshake or upgrade immediately at stop, as it can a TCP link.
	c := newConn(transport, clock, &limits)
	c.client = true
	c.established = established
	c.pongs = make(chan time.Time, 1)
	r := &reader{conn: c, limits: options.Limits, inbound: inbound, metrics: options.Metrics,
		version: packet.V5, deadline: deadline, problemInfo: true, expiry: uint32(options.Bridge.SessionExpiry.Seconds()),
		receiveMaximum: uint32(options.Bridge.ReceiveMaximum), routing: options.Routing}
	r.bridgeRules = options.Bridge.Topics
	if peer.Scheme == "ws" || peer.Scheme == "wss" {
		r.websocketURL = peer.String()
	}
	captures := 0
	for i := range r.bridgeRules {
		rule := &r.bridgeRules[i]
		captures = max(captures, rule.CaptureCount())
		if rule.Direction == config.In && rule.BuildsTopic() {
			r.topicRoom = int64(options.Limits.MaxTopicLength)
		}
	}
	r.captures.Plus = make([]topic.Capture, captures)
	w := &writer{conn: c, inbound: inbound, metrics: options.Metrics, timeout: options.Limits.WriteTimeout,
		peer: peerSettings{version: packet.V5}, establishment: deadline}
	properties, err := packet.NewProperties(packet.CONNECT, []packet.Property{
		{ID: packet.SessionExpiryInterval, Int: uint32(options.Bridge.SessionExpiry.Seconds())},
		{ID: packet.ReceiveMaximum, Int: uint32(options.Bridge.ReceiveMaximum)},
		{ID: packet.MaximumPacketSize, Int: uint32(options.Limits.MaxPacketSize)},
	})
	r.bridgeConnect = &packet.Connect{Version: packet.V5, CleanStart: options.CleanStart, KeepAlive: 30, ClientID: options.Bridge.ClientID,
		Properties: properties, UserNameFlag: options.Bridge.Username != "", UserName: options.Bridge.Username,
		PasswordFlag: options.Bridge.PasswordFile != "", Password: options.Bridge.Password}
	if err == nil {
		err = transport.SetWriteDeadline(deadline)
	}
	workers.Add(2)
	inbound <- Event{Kind: Dialed, Conn: c, Packet: result}
	go r.serve(err, workers)
	go w.serve(workers)
}
