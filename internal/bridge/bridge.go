// Package bridge is saguin as a client of somebody else's broker.
//
// It subscribes upstream and re-publishes what arrives through the broker's
// own publish path, so channel resolution, the size bounds, the header
// limits and every reason code apply to a bridged record exactly as they do
// to one from a client on the wire. There is no second way into a channel,
// which is why a bridge puts no restriction on channel type (RFC 0002
// "Bridges").
//
// **A bridge is a translator rather than a copy.** The far end has never
// heard of a channel or an offset, so a record arriving from one carries no
// saguin identity and becomes a new record here, at an offset this broker
// assigns. Nothing about the two ends' positions is expected to line up,
// and disaster recovery is the SQLite copy in RFC 0004 rather than anything
// a bridge does.
//
// # What an inbound bridge does not promise
//
// **It is only as durable as the broker it reads from.** saguin connects
// with a persistent session so the upstream holds messages while the link is
// down, which inverts the property the rest of saguin is built on:
// everywhere else the log is the buffer and an outage costs nothing until
// retention catches up. Here the buffer is somebody else's broker, bounded
// by their rules, and one whose session queue overflows discards. Measured
// against mosquitto 2.1.2 on 2026-08-13: with the default
// max_queued_messages of 1000, a session offline while 1200 were published
// came back with exactly 1000. The upstream logs it. saguin cannot see it,
// and nothing about the 200 that went reaches a consumer here.
//
// **A duplicate on the link cannot be deduplicated.** MQTT is at-least-once,
// so a lost acknowledgement makes the upstream re-send; a record from a
// foreign broker carries no saguin identity, so saguin mints a fresh Message
// ID and it becomes a genuinely separate record at its own offset. On an
// append channel that is a duplicate a consumer cannot tell from a real
// record. On a queue it is two jobs, done twice - none of the queue's
// protections apply, because they are about one record reaching two workers
// rather than about two records.
package bridge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/store"

	"log/slog"
)

// Publisher is the way into the broker. It is an interface so that a test
// can watch what a bridge decided without standing up a second broker to
// hear it, and it is deliberately narrow: publishing and asking what would
// be refused outright are the only two things a bridge may do to saguin.
type Publisher interface {
	// Publish puts one record through the broker's own publish path,
	// returning nil once the store has taken it, or the reason code a
	// client on the wire would have been given.
	Publish(topic string, payload []byte, headers []packets.UserProperty, props store.Props) error
	// Bounded reports what is wrong with the record itself, and nil when
	// nothing is. A record refused here can never be accepted, so it is
	// dropped rather than retried. It refuses a payload over
	// max_message_size, which enqueue relies on: such a record is decided
	// on the connection's read loop, and must never reach Publish there.
	Bounded(topic string, payload []byte, headers []packets.UserProperty, props store.Props) error
}

// Reader is the other half, and it exists only for `out`: a rule that sends
// records to the peer reads them from this broker first.
//
// It is separate from Publisher because the two are opposite directions and
// a test of one should not have to answer for the other - and because a
// bridge with no outbound rule never calls any of it.
type Reader interface {
	// Sources are the channels a filter reads from: every append and latest
	// channel it matches, and never a queue (invariant 11).
	Sources(filter string) []channel.Source

	// ReadFrom is up to max records at or after offset, with the channel's
	// floor as it was when they were read.
	ReadFrom(name string, offset uint64, max int) ([]store.Record, uint64, error)
	// Head is where the next record written will go, which is where a rule
	// reading a `start: tail` channel begins.
	Head(name string) uint64

	Position(name, reader string) (uint64, bool, error)
	SavePosition(name, reader string, offset uint64) error
	// PositionLost reports a stored position retention had already passed.
	// It carries the reader and the two offsets because the broker both
	// counts this and names it on /v1/operations/position-lost, and "which
	// rule lost how much" cannot be recovered from the channel name alone.
	PositionLost(name, reader string, position, floor uint64)

	// Watch signals when a record lands in an append channel; WatchLatest
	// hands over each value a latest channel takes, as it is published; and
	// WatchBroadcast each record published to a topic no channel claims. All
	// three return a function that stops watching. A handed-over record must
	// not be held up: fn runs on the publishing goroutine.
	Watch(name string) (<-chan struct{}, func())
	WatchLatest(name string, fn func(store.Record)) func()
	WatchBroadcast(fn func(store.Record)) func()
}

// presentedCertificate is the certificate a bridge presents when the far end
// asks for one, read from its files at each handshake. A pair renewed on disk
// is therefore what the next reconnect presents, with no signal to send.
func presentedCertificate(certFile, keyFile string) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("cert_file and key_file: %w", err)
		}
		return &pair, nil
	}
}

// subscribeTimeout bounds the SUBSCRIBE a bridge sends on every connection.
// An upstream that accepts a connection and never answers would otherwise
// hold the callback for ever, and the bridge would sit there connected,
// carrying nothing, with nothing in the log to say why.
const subscribeTimeout = 30 * time.Second

// settled remembers the records this bridge stored and acknowledged, so that
// an upstream redelivering one can be told from an upstream sending a new
// one.
//
// **Why it has to exist at all.** MQTT *requires* an upstream to re-send a
// QoS 1 publish whose PUBACK it never saw, and a link that drops with an
// acknowledgement in flight produces exactly that. Republished here, the
// record is given a local offset where it lands and carries no `saguin-id`,
// so downstream it is a new record that nothing can recognise as the one it
// already has - invariant 8's failure arriving by way of a rule MQTT insists
// on.
//
// **Bounded by what the upstream may have outstanding toward us**, which is
// the Receive Maximum this bridge advertises in its CONNECT and nothing
// larger: a record it may redeliver is one whose acknowledgement it has not
// seen, and it may not hold more of those than the number we gave it. Oldest
// overwritten, so the memory is fixed however long the link lives.
//
// **It is only ever consulted for a packet carrying DUP**, which is what
// makes a reused packet identifier safe: an identical reading republished
// under a recycled identifier arrives as a first delivery, where MQTT
// forbids DUP (MQTT-3.3.1-1), so it is stored rather than matched. A peer
// that sets DUP on a first delivery is breaking that rule, and the cost is
// one record of an identical topic and payload.
//
// **What it does not survive is a restart of this process.** The ring is
// held in memory, so a bridge that comes back has forgotten what it
// acknowledged and a redelivery then crosses as it does today. RFC 0002
// says so rather than promising more.
type settled struct {
	mu   sync.Mutex
	ring []settledRecord
	at   int
}

type settledRecord struct {
	id   uint16
	sum  uint64
	full bool
}

// settledDigest is FNV-1a over the topic and the payload, which is the hash
// RFC 0003 already uses where a number has to mean the same thing on every
// build. The topic is in it because two topics can carry one payload under
// one identifier, and the pair is what identifies a record rather than
// either half.
func settledDigest(topic string, payload []byte) uint64 {
	const offset64, prime64 = 14695981039346656037, 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(topic); i++ {
		h ^= uint64(topic[i])
		h *= prime64
	}
	h ^= 0 // the separator, so "ab"+"c" and "a"+"bc" are not one key
	h *= prime64
	for _, c := range payload {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

func newSettled(receiveMaximum int) *settled {
	if receiveMaximum < 1 {
		receiveMaximum = 1
	}
	return &settled{ring: make([]settledRecord, receiveMaximum)}
}

// remember records that this bridge has stored and acknowledged one record.
func (s *settled) remember(id uint16, topic string, payload []byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ring[s.at] = settledRecord{id: id, sum: settledDigest(topic, payload), full: true}
	s.at = (s.at + 1) % len(s.ring)
}

// holds reports whether this record is one already stored and acknowledged.
func (s *settled) holds(id uint16, topic string, payload []byte) bool {
	if s == nil {
		return false
	}
	sum := settledDigest(topic, payload)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.ring {
		if r.full && r.id == id && r.sum == sum {
			return true
		}
	}
	return false
}

// Bridge is one configured bridge, running.
type Bridge struct {
	cfg config.Bridge
	pub Publisher
	log *slog.Logger
	lim config.Resolved

	// down is when the link was last seen to go, for the one log line that
	// says how long it was gone. Written by OnConnectionDown and read by
	// OnConnectionUp, which autopaho calls from different goroutines.
	mu   sync.Mutex
	down time.Time
	// up says a connection has been established at least once, so that the
	// first one does not report itself as a recovery from an outage that
	// never happened.
	up bool

	// halt ends this bridge's own context, which is what stopping it means:
	// the connection manager shuts down and Run returns. Set once in Run
	// before any record can arrive, and read by the record path, so it is
	// under the mutex above rather than passed down through four calls that
	// have no other use for it.
	halt context.CancelFunc

	// seen is the redelivery guard described on `settled` above. Built in
	// Run, so a bridge that never ran holds nothing.
	seen *settled

	// firstRetry is how long store waits before publishing a refused record
	// again, doubling up to mostRetry. A field only so that a test can make
	// a retry happen without waiting for one; nothing configures it.
	firstRetry time.Duration

	// reconnectBackoff is how long autopaho waits between connection
	// attempts. A field for the same reason as firstRetry and configured by
	// nothing: a test about what the *second* attempt logs would otherwise
	// have to sit through the real five seconds to see one.
	//
	// New is the only place a Bridge is built, so this is never nil - and
	// the reason to keep it that way is that autopaho does not fail on nil.
	// It substitutes a flat ten seconds, which would replace the curve
	// below with a constant and say nothing about having done it.
	reconnectBackoff autopaho.Backoff

	// What the operations listener reports about this bridge (RFC 0005).
	// Atomic rather than under the mutex above, because they are touched on
	// the record path and read by a scrape that must not wait for it.
	//
	// `linkUp` is a link state rather than a health verdict: an inbound
	// bridge whose far end is down is a real thing to know and a terrible
	// thing to fail a liveness probe on, since the restart cannot fix
	// somebody else's broker. It belongs here and not in /health.
	linkUp atomic.Bool

	// stopped says this bridge has halted for good and will not reconnect,
	// which happens when a channel holding a copy refuses a record (RFC
	// 0002 *When the far end refuses*). It is reported separately from
	// linkUp because the two need different actions: a link that is down
	// may come back on its own, and one that stopped never will until
	// somebody looks at why.
	stopped atomic.Bool

	// lastConnectError is the reason the last connection attempt failed, so
	// that a reason which has not changed is not written again. Nil until
	// one has failed.
	lastConnectError atomic.Pointer[string]
	received         atomic.Uint64
	reconnects       atomic.Uint64

	// The outbound half's own numbers (saguin_bridge_sent_total,
	// _loops_skipped_total and _unsent_total). The unsent ones are kept apart
	// by cause because each asks an operator for something different:
	// peerRefused is a record the peer refused for good (its ACL, its
	// limits), broadcastRefused a broadcast record refused when there was no
	// store to retry from (the shed is the answer), unmappable a record the
	// rule could build no topic for (the bridge's own configuration), and
	// liveDropped a broadcast record dropped for a full queue.
	forwarded        atomic.Uint64
	loopsSkipped     atomic.Uint64
	peerRefused      atomic.Uint64
	broadcastRefused atomic.Uint64
	unmappable       atomic.Uint64
	liveDropped      atomic.Uint64
	// superseded is latest values replaced by a newer one for their topic
	// while they waited to cross, which is not a loss: the newer crosses.
	superseded atomic.Uint64

	// refusing says a run of refused publishes is in progress, so the line
	// about it is written once when it starts and once when it clears
	// rather than once per record.
	refusing atomic.Bool

	// inbox is what the peer sent, waiting for the worker to store it
	// (enqueue): three Receive Maximums, one of them open to QoS 0.
	inbox chan paho.PublishReceived
	// unstoredQueueFull counts QoS 0 records from the peer dropped because
	// the queue was full, and droppingQoS0 says a run of them is in
	// progress, so the line about it is written once when it starts and once
	// when it ends.
	unstoredQueueFull atomic.Uint64
	droppingQoS0      atomic.Bool
	// And the records from the peer dropped with a warning, each finished
	// with at the peer and so lost at this hop, by the same causes the
	// warnings name (saguin_bridge_unstored_total): no inbound rule covers
	// the topic; a rule covers it and builds no topic, or one in the
	// reserved $ space - unmappable, the word the outbound half uses for the
	// same two; and a record saguin would refuse whatever the channel.
	unstoredNoRule        atomic.Uint64
	unstoredUnmappable    atomic.Uint64
	unstoredNeverAccepted atomic.Uint64

	// session is what every outbound publish waits under beside its rule's
	// own context, and it ends when the peer keeps no session for this
	// bridge (connected). paho then drops the publishes it held in flight
	// without answering them, so without this they would wait for ever.
	session atomic.Pointer[pubSession]

	// unackedAfter is how long a record waits unacknowledged on a link that
	// is up before its rule says so (outRule.waiting).
	unackedAfter time.Duration

	// peerRxMax is the Receive Maximum the peer sent in its CONNACK, or
	// zero where it sent none. Read on every window, written on every
	// connection, so it is atomic rather than under the mutex above.
	peerRxMax atomic.Uint32

	// peerNoRetain says the peer advertised Retain Available 0, so a
	// crossing publish carries the flag down however the record was
	// published here.
	//
	// **A record would otherwise stop the link for good.** paho refuses a
	// retained publish to such a peer client-side rather than sending it,
	// so the write fails, the rule's position never advances past that
	// record, and it is retried on every reconnect for ever - one retained
	// value at the source, and the link never moves again. Clearing the
	// flag ships the value live instead: the peer's subscribers get it, and
	// only the retained promise is lost, which is the degradation RFC 0002
	// states.
	//
	// Written on every connection, because a reconnect may reach a peer
	// that has been reconfigured, and read on every crossing publish.
	peerNoRetain atomic.Bool

	// retainCleared says the line about clearing the flag has been written
	// for this connection, so it is one line per link rather than one per
	// record - the same shape `refusing` above keeps for a run of refusals.
	retainCleared atomic.Bool

	// rd is how an outbound rule reads this broker, and it is nil for a
	// bridge with no `out` rule - which is every bridge that only reads.
	rd Reader
}

// Name and Peer identify this bridge in the metric catalogue, and both
// are what the operator wrote in the configuration file - which is what
// makes them labels the catalogue may carry.
func (b *Bridge) Name() string { return b.cfg.Name }
func (b *Bridge) Peer() string { return b.cfg.Peer }

// Connected reports whether the link to the peer is up right now.
func (b *Bridge) Connected() bool { return b.linkUp.Load() }

// Received is how many records have arrived from the upstream, and
// Reconnects how many times the link has come back after going away.
//
// Reconnects counts recoveries rather than connections, so the first
// connection is not one: a bridge that has never lost its link reports
// zero, which is the number an operator is looking for.
func (b *Bridge) Received() uint64   { return b.received.Load() }
func (b *Bridge) Reconnects() uint64 { return b.reconnects.Load() }

// How long a refused record waits before it is published again. The first
// wait is short because the common refusal - a channel at its max_bytes with
// a consumer draining it - clears in about the time it takes one consumer to
// read one record. The ceiling is where it settles for the other case, a
// channel nobody is draining, which clears when an operator does something.
const (
	firstRetry = 100 * time.Millisecond
	mostRetry  = 30 * time.Second
)

// New builds a bridge. It does not connect; Run does.
//
// The three tuning values are defaulted again here, and they should never
// need it: config.Load fills them in for every bridge it validates, so a
// zero can only reach this from a config.Bridge somebody built by hand. It
// is done anyway because of what a zero would mean rather than because it is
// expected - a Session Expiry Interval of 0 tells the upstream to discard
// the session the moment the link drops, so the bridge would hold no backlog
// at all and lose every record published during an outage, silently. A
// Receive Maximum of 0 is a subscriber that may receive nothing, and an
// acknowledgement interval of 0 is a ticker that never fires.
// rd is how an outbound rule reads this broker, and may be nil for a bridge
// whose rules are all `in` - which is what New's callers pass where nothing
// goes out. A bridge with an `out` rule and no Reader refuses to start
// rather than running half of itself.
func New(cfg config.Bridge, pub Publisher, lim config.Resolved, log *slog.Logger, rd Reader) *Bridge {
	if cfg.SessionExpiry <= 0 {
		cfg.SessionExpiry = config.DefaultSessionExpiry
	}
	if cfg.ReceiveMaximum <= 0 {
		cfg.ReceiveMaximum = config.DefaultReceiveMaximum
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = config.DefaultAckInterval
	}
	b := &Bridge{
		cfg:        cfg,
		pub:        pub,
		rd:         rd,
		lim:        lim,
		log:        log.With("bridge", cfg.Name, "peer", cfg.Peer),
		seen:       newSettled(cfg.ReceiveMaximum),
		firstRetry: firstRetry,
		reconnectBackoff: autopaho.NewExponentialBackoff(
			time.Second, time.Minute, 5*time.Second, 2),
		unackedAfter: unackedAfter,
		inbox:        make(chan paho.PublishReceived, 3*cfg.ReceiveMaximum),
	}
	b.session.Store(newPubSession())
	return b
}

// pubSession is one session at the peer, as far as the outbound publishes
// are concerned: ended when the peer keeps no session for this bridge.
type pubSession struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newPubSession() *pubSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &pubSession{ctx: ctx, cancel: cancel}
}

// publishWait is how long an outbound publish may wait for its PUBACK: as
// long as the bridge runs, whose context ends the wait.
//
// **One resend mechanism, the session's.** paho keeps an unacknowledged
// publish in its session and sends it again, marked DUP, when the link
// comes back; mosquitto's bridge does the same and nothing more. With
// paho's default of ten seconds the publish also returned an error when an
// outage outlasted it, and carry then sent the record again as a new
// publish - a third copy of every record in flight at the drop, where MQTT
// and RFC 0002 promise two. The
// connect is bounded by autopaho's own ConnectTimeout and a SUBSCRIBE by
// subscribeTimeout, so nothing but a publish waits longer.
const publishWait = 100 * 365 * 24 * time.Hour

// unackedAfter is how long a record may wait for its PUBACK on a link that is
// up before the bridge says so. Two keepalives: a link that answers PINGREQ
// for that long is up, so the wait is the peer's.
const unackedAfter = time.Minute

// Run connects, subscribes, and carries records until ctx is cancelled. It
// returns only when the connection manager has finished shutting down, so
// that a caller stopping a bridge knows the link is closed.
//
// Reconnection is autopaho's, and every attempt after the first is silent.
// One line when the link goes and one when it returns is what an operator
// can act on; one per attempt is a log that scrolls a bad connection out of
// the record of everything else that happened.
func (b *Bridge) Run(ctx context.Context) error {
	u, err := url.Parse(b.cfg.Peer)
	if err != nil {
		return fmt.Errorf("bridge %q: peer %q: %w", b.cfg.Name, b.cfg.Peer, err)
	}

	// This bridge's own context, so that it can stop itself without taking
	// the caller's down with it: every other bridge on this broker goes on
	// running, which is the point of stopping one.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	b.mu.Lock()
	b.halt = cancel
	b.mu.Unlock()

	// The TLS both directions: the authority the upstream's certificate is
	// checked against, and the certificate saguin presents to it. Nil is
	// autopaho's own default, which crypto/tls reads as the system roots
	// with the host name inferred from the address and no certificate of our
	// own - right for a public certificate, useless for the private
	// authority an estate usually runs, and never "verify nothing".
	var certs *tls.Config
	if b.cfg.CAFile != "" || b.cfg.CertFile != "" {
		certs = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if b.cfg.CAFile != "" {
		pem, err := os.ReadFile(b.cfg.CAFile)
		if err != nil {
			return fmt.Errorf("bridge %q: ca_file: %w", b.cfg.Name, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("bridge %q: ca_file %s holds no PEM certificates",
				b.cfg.Name, b.cfg.CAFile)
		}
		certs.RootCAs = pool
	}

	// The other half: the certificate saguin presents when the upstream asks
	// for one, which is what saguin's own listener does with `client_ca_file`
	// set. Read once here, so a pair that does not load stops the bridge
	// naming itself rather than becoming a link that never comes up.
	if b.cfg.CertFile != "" {
		if _, err := tls.LoadX509KeyPair(b.cfg.CertFile, b.cfg.KeyFile); err != nil {
			return fmt.Errorf("bridge %q: cert_file and key_file: %w", b.cfg.Name, err)
		}
		// **GetClientCertificate rather than Certificates**, and the
		// difference is which mistake stays visible. crypto/tls matches
		// Certificates against the authorities the far end advertises and
		// sends nothing when none of them match - silently, with no error -
		// so a pair issued by an authority the upstream does not know becomes
		// an ordinary no-certificate connection: refused at the handshake
		// where the upstream requires one, and accepted as an unauthenticated
		// link where it does not, which is the worse of the two. This hook is
		// asked on every handshake and sends back what it returns, so saguin
		// presents what the operator configured and the far end is the one
		// that says no.
		//
		// **Read again at every handshake**, so a renewed pair is what the
		// next reconnect presents, with no signal to send. The read above
		// stays: it is what stops a bridge whose files never loaded.
		certs.GetClientCertificate = presentedCertificate(b.cfg.CertFile, b.cfg.KeyFile)
	}

	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		TlsCfg:     certs,
		KeepAlive:  30,
		// False, and on the first connection too. A bridge that cleared the
		// session on startup would discard the backlog the upstream held
		// across a saguin restart, which is the one thing this session is
		// for.
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         uint32(b.cfg.SessionExpiry / time.Second),
		ReconnectBackoff:              b.reconnectBackoff,
		ConnectPacketBuilder: func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if cp.Properties == nil {
				cp.Properties = &paho.ConnectProperties{}
			}
			rx := uint16(b.cfg.ReceiveMaximum)
			cp.Properties.ReceiveMaximum = &rx
			// The bound on a record's size, applied by the upstream on
			// saguin's behalf - which is the only place it can be applied
			// before the bytes are read (invariant 13). A conforming broker
			// will not send a larger packet; the guard for one that does is
			// in enqueue, where paho has already read it and before it is
			// queued.
			max := b.lim.MaxMessageSize
			cp.Properties.MaximumPacketSize = &max
			return cp, nil
		},
		OnConnectionUp:   b.connected,
		OnConnectionDown: b.disconnected,
		OnConnectError: func(err error) {
			// **The first attempt, and every attempt whose reason has
			// changed, at Warn; the repeats at Debug.**
			//
			// This was Debug for everything, on the reasoning that a bad
			// link writes one per attempt and the two lines that matter are
			// written by connected and disconnected. That is true of a link
			// that was up and went away. It is false of one that has never
			// been up - connected has never fired, so disconnected never
			// does either, and a bridge that has never reached its upstream
			// says nothing at any level an operator runs at. A certificate
			// this broker cannot verify is exactly that: it never heals by
			// retrying, and the operator has to be able to tell it apart
			// from a cable somebody pulled.
			//
			// Keyed on the reason rather than counted, so a link failing the
			// same way for a day writes one line, and the day its reason
			// changes - from "connection refused" to "unknown authority" -
			// writes another.
			reason := err.Error()
			previous := b.lastConnectError.Swap(&reason)
			if previous == nil || *previous != reason {
				// The bridge and its upstream are on this logger already
				// (New builds it with them), so naming them here printed
				// each twice.
				b.log.Warn("bridge cannot reach its peer", "error", err)
				return
			}
			b.log.Debug("bridge connection attempt failed", "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID:      b.cfg.ClientID,
			PacketTimeout: publishWait,
			// The acknowledgement is saguin's to send, and it sends it only
			// once its own store has the record. Left automatic, Paho
			// acknowledges when the handler returns, which would acknowledge
			// a record that was refused.
			OnServerDisconnect:         b.serverGone,
			EnableManualAcknowledgment: true,
			SendAcksInterval:           b.cfg.AckInterval,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					b.enqueue(ctx, pr)
					return true, nil
				},
			},
		},
	}

	// **Stored off paho's read loop, in order, by one worker.** The handler
	// above is called on the connection's routing goroutine, and autopaho
	// notices a dropped link only once that goroutine returns - so a channel
	// refusing a record, retried in the handler, left a link that had gone
	// reported as up, reconnected nothing and held every outbound rule on a
	// dead client: measured 21 seconds late with `connected` at 1. The worker does what the handler did, the
	// redelivery ring and the acknowledgement included, and the handler only
	// queues. Started once the connection manager exists - anything that
	// arrives first waits in the queue - and waited for below, so no store
	// runs after Run returns.
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return fmt.Errorf("bridge %q: %w", b.cfg.Name, err)
	}
	var work sync.WaitGroup
	work.Add(1)
	go func() { defer work.Done(); b.work(ctx) }()

	// **The outbound half runs beside the link rather than inside a
	// connection.** autopaho reconnects underneath it. A publish made while
	// there is no connection returns at once and its rule retries on its
	// backoff; one already in flight when the link drops waits across the
	// reconnect for the PUBACK to paho's own resend (publishWait), so a link
	// that drops mid window leaves the position where it was and sends each
	// record in flight twice at most. Started once the connection manager
	// exists and stopped with the same context, which is what makes a
	// bridge's shutdown one event rather than two.
	var out sync.WaitGroup
	if b.hasOut() {
		out.Add(1)
		go func() { defer out.Done(); b.runOut(ctx, cm) }()
	}

	<-cm.Done()
	out.Wait()
	work.Wait()
	return nil
}

// enqueue hands a record from the peer to the worker and returns, so the
// connection's read loop never waits on a channel.
//
// **The queue is bounded by what the peer may send.** A QoS 1 or 2 record is
// acknowledged only once stored, and the peer may hold no more than this
// bridge's Receive Maximum unacknowledged - twice that across a reconnect,
// when the old connection's are still queued and the peer sends them again -
// so they always fit and are never dropped; past that, which only repeated
// reconnects reach, the handler waits as it used to. A QoS 0 record has no
// such bound, so it may take one Receive Maximum of the queue and is dropped
// and counted beyond it: at-most-once is what its publisher asked for, and
// it is mosquitto's rule for QoS 0 over a full queue. Holding the read loop
// for it instead is the stall this queue exists to end.
func (b *Bridge) enqueue(ctx context.Context, pr paho.PublishReceived) {
	// **A record over max_message_size is decided here, not queued.** Only a
	// peer that ignores the Maximum Packet Size this bridge sent can send
	// one, and paho has read the whole of it by now, so this is the first
	// place saguin can refuse it. The worker would drop it anyway, but only
	// once it reached the front: until then up to three Receive Maximums of
	// them waited in the queue, each as large as the peer chose. So it goes
	// through handle here, on the read loop, which is safe for this record
	// alone: BridgeClient.Bounded refuses it before handle could wait on a
	// channel. Everything else handle decides is decided as the worker would
	// - counted as never_accepted, or no_rule, or left unacknowledged by a
	// stopped bridge - and paho keeps acknowledgements in the order the
	// records arrived.
	if uint32(len(pr.Packet.Payload)) > b.lim.MaxMessageSize {
		if b.handle(ctx, pr.Packet.Topic, pr.Packet.Payload,
			headersOf(pr.Packet), propsOf(pr.Packet)) == finished {
			b.ack(pr)
		}
		return
	}
	if pr.Packet.QoS == 0 && len(b.inbox) >= b.cfg.ReceiveMaximum {
		b.unstoredQueueFull.Add(1)
		if !b.droppingQoS0.Swap(true) {
			b.log.Warn("dropping QoS 0 records from the peer: the bridge's queue is full "+
				"behind a channel that is not taking what it is given",
				"peer_topic", b.lim.Loggable(pr.Packet.Topic))
		}
		return
	}
	select {
	case b.inbox <- pr:
	case <-ctx.Done():
	}
}

// work stores what the peer sent, in the order it arrived, for as long as
// the bridge runs. A record still queued when it stops is left
// unacknowledged, and the peer sends it again.
func (b *Bridge) work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pr := <-b.inbox:
			b.receive(ctx, pr)
			if len(b.inbox) < b.cfg.ReceiveMaximum && b.droppingQoS0.Swap(false) {
				b.log.Warn("the bridge's queue has room again, so QoS 0 records from the " +
					"peer are being stored")
			}
		}
	}
}

// hasOut reports whether any rule sends records to the peer.
//
// **A bridge with an outbound rule and no Reader is a configuration that
// cannot do what it says**, so it says so rather than running the half it
// can. Nothing in the tree builds one - main hands the same client to both
// halves - which is exactly why it is worth a line here: the next caller is
// the one that will.
func (b *Bridge) hasOut() bool {
	for _, r := range b.cfg.Topics {
		if !r.Out {
			continue
		}
		if b.rd == nil {
			b.log.Error("this bridge has an outbound rule and no way to read this broker, " +
				"so nothing will be sent to the peer; the inbound rules still run")
			return false
		}
		return true
	}
	return false
}

// stop halts this bridge for good: the link goes down and does not come
// back until somebody restarts the broker.
//
// **It is the answer RFC 0002 gives when a copy refuses a record**, and the
// opposite of what an ordinary inbound bridge does. Inbound drops a record
// that can never be accepted, because holding the link blocks a broker other
// people are using and one lost record from a foreign feed is one record.
// Into a copy, a dropped record is a permanent hole in a disaster-recovery
// copy, and a hole nothing reports is the failure the whole mechanism exists
// to prevent. A stopped bridge is loud, the records are still at the
// upstream - nothing it refused was acknowledged - and an operator who fixes
// the far end loses nothing.
func (b *Bridge) stop() {
	b.stopped.Store(true)
	// **Down, said here rather than left to the disconnect callback.**
	// Cancelling the context shuts autopaho down from the inside, and
	// OnConnectionDown does not fire for a shutdown it was asked for - so a
	// stopped bridge went on reporting its link as up, and a scrape carried
	// `connected 1` beside `stopped 1`. A dashboard watching the ordinary
	// signal would have seen a healthy link that was dead.
	b.linkUp.Store(false)
	b.mu.Lock()
	halt := b.halt
	b.mu.Unlock()
	if halt != nil {
		halt()
	}
}

// Stopped reports whether this bridge halted itself rather than merely
// losing its link (RFC 0005).
func (b *Bridge) Stopped() bool { return b.stopped.Load() }

// connected subscribes to every rule's filter, and says the link is back.
//
// The subscription is made on every connection rather than only the first.
// A session the upstream still holds already carries them, so re-sending is
// redundant there and harmless - and the case that matters is the one where
// it does not: a session the upstream expired, or a broker that restarted
// without persistence. A bridge that subscribed once would sit connected to
// that broker for ever, receiving nothing, with nothing in its log to say
// why.
func (b *Bridge) connected(cm *autopaho.ConnectionManager, ca *paho.Connack) {
	// **What the peer said it will take at once.** MQTT reads an absent
	// Receive Maximum as 65,535, which is no bound at all - so this is one
	// half of the outbound window and the bridge's own `receive_maximum` is
	// the other, with the smaller winning. Stored on every connection
	// because a reconnect may reach a peer that has been reconfigured.
	if ca != nil && ca.Properties != nil && ca.Properties.ReceiveMaximum != nil {
		b.peerRxMax.Store(uint32(*ca.Properties.ReceiveMaximum))
	} else {
		b.peerRxMax.Store(0)
	}

	// **Whether the peer takes retained messages at all.** MQTT reads an
	// absent Retain Available as "yes", which is what paho resolves it to,
	// so this is only ever false for a peer that said so. Read on every
	// crossing publish; the line about it is written once per link.
	b.peerNoRetain.Store(ca != nil && !ca.Properties.RetainAvailable)
	b.retainCleared.Store(false)

	// **A peer that kept no session has lost what was in flight**, and so
	// has paho: it empties its own in-flight table on Session Present 0
	// without answering the publishes waiting on it. Ending the session they
	// wait under returns them, and their rules send them again - the peer
	// never acknowledged them, so that is the at-least-once resend rather
	// than a copy. A first connection reads the same and has nothing in
	// flight.
	if ca != nil && !ca.SessionPresent {
		b.session.Swap(newPubSession()).cancel()
	}

	// A connection already in flight when the bridge stopped still lands
	// here. Subscribing on it would fail against the cancelled context and
	// write "bridge cannot subscribe upstream" underneath the line that says
	// why the bridge stopped - a second, misleading failure for one event.
	if b.stopped.Load() {
		return
	}

	// **Two slices kept in step**, because the SUBACK below is read by
	// position: MQTT answers one reason code per filter in the order they
	// were sent, so a refusal has to be named against the rule that asked
	// for it. Indexing the configuration instead would have been right only
	// while every rule subscribed, and it is not a mistake that shows up in
	// anything but the log line after a refusal.
	subs := make([]paho.SubscribeOptions, 0, len(b.cfg.Topics))
	asked := make([]config.Rule, 0, len(b.cfg.Topics))
	for _, in := range b.cfg.Topics {
		// Only the rules that read. An `out`-only rule's filter selects
		// local topics and means nothing at the peer; subscribing to it
		// would pull records nobody asked for across the link, which on the
		// metered connection this exists for is exactly backwards.
		if !in.In {
			continue
		}
		asked = append(asked, in)
		subs = append(subs, paho.SubscribeOptions{
			Topic: in.Filter,
			QoS:   1,
			// **saguin's own publishes must not come back to it**, and this
			// is load-bearing now rather than the belt and braces it was
			// while a bridge only read. An `out` rule publishes on this very
			// connection, and a peer echoing that back to the subscription
			// beside it is the shortest loop there is: MQTT 5's No Local is
			// what closes it, in one round trip saved rather than caught
			// later by the mark on the record.
			//
			// Verified honoured by mosquitto 2.0.22, since a rule this
			// important should not rest on the specification alone: one
			// connection subscribing with it and publishing on itself, a
			// second client publishing the same topic, only the second
			// arrived.
			NoLocal: true,
			// **What the publisher set, not what the delivery is.** MQTT
			// puts RETAIN down on a live delivery unless the subscriber
			// asked otherwise [MQTT-3.3.1-12], so without this a value
			// retained at the peer reaches the bridge looking ordinary and
			// is stored here as ordinary: the flag would survive the hop
			// only for values that happened to be delivered from the
			// peer's retained store, which is a bridge that carries
			// retained-ness sometimes.
			//
			// It is the same question the crossing publish answers in the
			// other direction, asked of the subscription rather than of
			// the packet, and it is why the inbound half needs a line of
			// its own rather than following from the outbound one.
			RetainAsPublished: true,
			// **The peer's retained set is not asked for again on a
			// recovery** [MQTT-3.3.1-10, Retain Handling 1: "send retained
			// messages at subscribe only if the subscription does not
			// currently exist"]. `connected` re-subscribes on every link
			// recovery, and the option's zero value asks for the stored
			// pass every time - so each reconnect earned the peer's whole
			// retained set under the rule any broker honours for any
			// subscriber, and RFC 0003 promises the opposite: "a link
			// restart re-ships nothing".
			//
			// What it costs is silence. The pass arrives as ordinary
			// publishes carrying no `saguin-id`, so a consumer
			// deduplicating on identity (invariant 8) cannot recognise
			// them, and a rule landing on an `append` channel appends the
			// peer's retained set again on every recovery - a flapping
			// link, which is the deployment bridges exist for,
			// multiplying it for the life of the deployment. Measured
			// against a saguin peer, and worse against mosquitto 2.0.22,
			// where the origin's own exported values come back too.
			//
			// **1 rather than 2**, which would be the over-fix. RFC 0002
			// documents the re-import that a reconnect finding Session
			// Present = 0 produces, and names the mitigation an operator
			// chooses for it; a session that is genuinely gone has no
			// subscription, so 1 leaves that case exactly as documented
			// and closes only the one where the session was held.
			RetainHandling: 1,
		})
	}

	b.mu.Lock()
	first, since := !b.up, b.down
	b.up = true
	b.mu.Unlock()

	b.linkUp.Store(true)
	// A recovery rather than a connection, so a bridge that has never lost
	// its link reports zero - which is the number an operator is looking
	// for. Counting the first connection would make every healthy bridge
	// read as having reconnected once.
	if !first {
		b.reconnects.Add(1)
	}

	// **Only where there is something to read.** A SUBSCRIBE must carry at
	// least one filter (MQTT-3.8.3-2), so an out-only bridge that sent one
	// anyway sent an empty packet and the peer was right to hang up:
	// measured against saguin ("protocol violation: must contain at least
	// one filter") and against mosquitto 2.0.22 ("disconnected due to
	// malformed packet"), each looping every ten seconds for ever while the
	// link never came up and nothing was ever exported - the pure exporter
	// RFC 0002 recommends, dead on arrival.
	//
	// The link is up either way, which is the point: an `out` rule needs a
	// connection to publish on and nothing else. What hid this is that a
	// bridge used only to read, so "connected" and "subscribed" were one
	// moment; they are two now, and only the second is conditional. The
	// line that says the link is up is below and shared, so an out-only
	// bridge still retracts its own "link down" at the level it was made.
	if len(subs) > 0 {
		b.subscribeUpstream(cm, subs, asked)
	}
	// **A line that retracts another is written at the level of the line it
	// retracts.** A log level is a severity filter rather than a subject
	// one: an operator at `warn` asked to be told about problems, and when
	// a problem ended is the same fact as that it started. Left at Info,
	// the pair said "bridge link down" at `warn` and never took it back, so
	// the log asserted a broken link for the life of the process - a week
	// after the link returned in the first minute.
	//
	// The first connection is not a retraction and stays at Info: nothing
	// was claimed to be wrong, so there is nothing to take back.
	switch {
	case first:
		b.log.Info("bridge link up", "session_present", ca.SessionPresent, "subscribed", len(subs))
	default:
		b.log.Warn("bridge link up again", "down_for", time.Since(since).Round(time.Second),
			"session_present", ca.SessionPresent, "subscribed", len(subs))
	}
	if !ca.SessionPresent {
		// The upstream is not holding saguin's session, so whatever it had
		// queued for it is gone. Everywhere else in saguin an outage costs
		// nothing until retention catches up; here it costs whatever the far
		// end decided to stop keeping, and **this connection is the only
		// moment saguin can tell**.
		//
		// It is said on a first connection too, and that is the whole point
		// of the line. A bridge cannot distinguish its first connection ever
		// from the first after a restart - nothing about it is stored - so
		// suppressing this when `first` is true suppressed it in exactly the
		// case worth reporting: a saguin down for longer than session_expiry,
		// coming back to a session the upstream has discarded. That was the
		// behaviour until it was run and found silent.
		//
		// So the line states the fact and names the ambiguity rather than
		// guessing which of the two happened. A first start is the one an
		// operator can dismiss, and they are the only one who can tell.
		b.log.Warn("the peer is not holding a session for this bridge, so anything it "+
			"queued is gone: either this is the bridge's first connection, or the link was "+
			"down for longer than session_expiry. saguin cannot tell which",
			"session_expiry", b.cfg.SessionExpiry, "first_connection", first)
	}
}

// serverGone reads the DISCONNECT the upstream sent, for the one reason
// that never heals by reconnecting.
//
// **A record at the source larger than this broker's `max_message_size`
// never reaches the record path at all.** The bridge declares that bound as
// MQTT's Maximum Packet Size in its CONNECT, so the *source* refuses to
// deliver and disconnects the consumer - and the bridge reconnects,
// resubscribes, and is disconnected on the same record, for ever. Nothing is
// lost: the position never advances past it. But the copy stops moving, the
// cause is logged only at the source, and until this the box whose copy was
// stalled said nothing at all.
//
// Every other disconnect reason is left to the ordinary reconnect: a link
// that dropped is a link that comes back.
func (b *Bridge) serverGone(d *paho.Disconnect) {
	if d == nil || d.ReasonCode != byte(packets.ErrPacketTooLarge.Code) {
		return
	}
	b.log.Error("stopping this bridge: the peer holds a record larger than this "+
		"broker's max_message_size, so it disconnects rather than delivering and no "+
		"reconnection can help. Raise max_message_size here to at least the peer's "+
		"and restart; nothing is lost meanwhile, because the position never advances "+
		"past the record",
		// The bridge and its upstream are on this logger already, so naming
		// them here prints each twice - which this file has been caught by
		// before, and was again.
		"max_message_size", b.lim.MaxMessageSize)
	b.stop()
}

// subscribeUpstream asks the peer for everything the reading rules cover,
// and reports a refusal per filter rather than as one number.
//
// Split out of connected so that the SUBSCRIBE can be skipped without the
// link-up line going with it - see the call site for why an out-only bridge
// must not send one at all.
func (b *Bridge) subscribeUpstream(cm *autopaho.ConnectionManager,
	subs []paho.SubscribeOptions, asked []config.Rule) {
	// With a deadline. An upstream that accepts the connection and never
	// answers the SUBSCRIBE would otherwise hold this callback for ever,
	// leaving a bridge that is connected, carrying nothing, and silent.
	ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
	defer cancel()

	// **Ask for deletions.** A saguin upstream leaves them out of the state
	// a new subscription is sent, because to every other reader a deleted
	// topic is one there is nothing to say about - so without this a topic
	// deleted while the link was down is simply absent from what arrives,
	// and this broker keeps it for ever (RFC 0003).
	//
	// Asked on every link rather than only where a channel is a copy: an
	// upstream that deleted a topic means it deleted a topic, whichever kind
	// of channel this end lands it in. A broker that has never heard of the
	// property ignores it, which is what MQTT says to do with a User
	// Property, so it costs a foreign upstream nothing.
	sa, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: subs,
		Properties: &paho.SubscribeProperties{
			User: paho.UserProperties{{Key: "saguin-deletions", Value: "1"}},
		},
	})
	if err != nil {
		// autopaho reconnects, and connected runs again on the next
		// connection, so there is nothing to do here but say so.
		b.log.Error("bridge cannot subscribe at its peer", "error", err)
		return
	}
	// Indexed against the rules, so a broker answering with more reason codes
	// than there were filters cannot walk off the end of them. MQTT says
	// there is one per filter in order; this does not depend on it.
	for i, code := range sa.Reasons {
		if code <= 2 { // 0, 1 and 2 are granted QoS; anything else is a refusal
			continue
		}
		filter := "?"
		if i < len(asked) {
			filter = asked[i].Filter
		}
		b.log.Error("the peer refused a bridge subscription, so nothing will "+
			"arrive for it", "filter", filter, "reason", fmt.Sprintf("0x%02X", code))
	}
}

func (b *Bridge) disconnected() bool {
	b.linkUp.Store(false)
	b.mu.Lock()
	b.down = time.Now()
	b.mu.Unlock()
	b.log.Warn("bridge link down")
	return true // keep trying
}

// disposition is what became of one upstream record.
type disposition int

const (
	// finished: the upstream may be told saguin is done with it - stored, or
	// dropped on purpose.
	finished disposition = iota
	// keep: leave it with the upstream. It was refused, or the bridge has
	// stopped, and the record is still theirs.
	keep
)

// receive takes one record off the link and acknowledges it where saguin is
// finished with it.
//
// **Records arrive in the upstream's order and are taken in that order**,
// which is all the ordering a bridge needs now that nothing here places a
// record at an offset somebody else chose. A record this broker refuses for
// a reason that can stop being true holds the link until it is taken, which
// is backpressure the upstream can see; one it will never take is dropped
// with a line saying so.
func (b *Bridge) receive(ctx context.Context, pr paho.PublishReceived) {
	// **A record this bridge already took, offered again.** The upstream
	// re-sends a QoS 1 publish whose acknowledgement it never saw, which
	// MQTT requires of it; handing that to `handle` a second time stores it
	// again under a new local offset, and nothing downstream can tell the
	// two apart. Acknowledged again instead, because the acknowledgement is
	// what the upstream is missing, and dropped.
	//
	// Asked only of a packet carrying DUP: see `settled`.
	if pr.Packet.Duplicate() &&
		b.seen.holds(pr.Packet.PacketID, pr.Packet.Topic, pr.Packet.Payload) {
		b.log.Debug("an upstream redelivery this bridge had already stored was "+
			"acknowledged again rather than crossed twice",
			"topic", b.lim.Loggable(pr.Packet.Topic), "packet_id", pr.Packet.PacketID)
		b.ack(pr)
		return
	}
	if b.handle(ctx, pr.Packet.Topic, pr.Packet.Payload,
		headersOf(pr.Packet), propsOf(pr.Packet)) == finished {
		// Remembered before the acknowledgement rather than after: the
		// redelivery this guards against is caused by an acknowledgement
		// that never arrived, so the window where the record is settled
		// here and not there must be one this already covers.
		b.seen.remember(pr.Packet.PacketID, pr.Packet.Topic, pr.Packet.Payload)
		b.ack(pr)
	}
}

// handle decides what becomes of one upstream record, and reports whether
// the upstream may now be told saguin is finished with it.
//
// Every path through here ends in an acknowledgement except the one that
// cannot: a record saguin stored is acknowledged because it is safe, a
// record saguin will never accept is acknowledged because holding it would
// stop the link for ever behind something that cannot move, and a record
// saguin merely cannot take *yet* is not acknowledged until it can.
//
// It takes the record rather than the packet so that what a bridge decides
// can be tested without a broker at either end. The acknowledgement itself
// is the caller's, which is what keeps the decision and the MQTT plumbing
// from being the same function.
func (b *Bridge) handle(ctx context.Context, upstream string, payload []byte,
	headers []packets.UserProperty, props store.Props) disposition {
	// **A stopped bridge takes nothing, and this is the first thing asked.**
	// Cancelling the context ends the link, but records already inside the
	// in-flight window are still handed here, and a bridge that stopped
	// because the upstream holds a record this broker can never accept
	// should not go on storing the ones that followed it.
	//
	// Not acknowledged, so everything the upstream still holds stays its.
	if b.stopped.Load() {
		return keep
	}

	// Counted on arrival rather than on storage: what this answers is
	// whether the upstream is sending anything, which is the question asked
	// when a bridge looks idle. What became of each record is the publish
	// and refusal counters' business.
	b.received.Add(1)
	topic, ok := b.route(upstream)
	if !ok {
		return finished // dropped with a warning, and finished with
	}

	if err := b.pub.Bounded(topic, payload, headers, props); err != nil {
		// Dropped rather than retried: nothing about this record will
		// change, so a retry would hold the link behind it for ever.
		b.unstoredNeverAccepted.Add(1)
		b.log.Warn("dropping a bridged record saguin would never accept",
			"peer_topic", b.lim.Loggable(upstream), "topic", b.lim.Loggable(topic), "reason", err,
			"payload_bytes", len(payload), "headers", len(headers))
		return finished
	}

	return b.store(ctx, upstream, topic, payload, headers, props)
}

// store publishes into the broker, retrying for as long as the broker keeps
// refusing, and reports whether it got in. False means only that ctx ended
// first: the record is then left unacknowledged, so the upstream still holds
// it and delivers it again.
//
// The retry has no limit, and that is the design rather than an oversight.
// A refusal here is a full channel or a storage that is not working, and
// both can stop being true. Giving up would mean discarding a record the
// upstream believes it delivered, which is the failure RFC 0002 refuses
// everywhere else; holding the link instead is backpressure, and it is what
// an operator can see. Records that could *never* be accepted have already
// been dropped by the caller, so nothing here retries something that cannot
// change its mind.
func (b *Bridge) store(ctx context.Context, upstream, topic string,
	payload []byte, headers []packets.UserProperty, props store.Props) disposition {
	wait := b.firstRetry
	for attempt := 1; ; attempt++ {
		err := b.pub.Publish(topic, payload, headers, props)
		if err == nil {
			if attempt > 1 {
				// Warn, because it retracts the Warn below - see the rule
				// at "bridge link up again".
				b.log.Warn("the channel took a bridged record that had been refused",
					"topic", b.lim.Loggable(topic), "attempts", attempt)
			}
			return finished
		}

		// Anything not on the short list of refusals that can stop being
		// true is permanent, and permanent means dropped rather than held:
		// nothing about this record will change, so retrying it would hold
		// the link for ever behind a record that cannot move.
		if !transient(err) {
			b.log.Error("dropping a bridged record the broker will never accept: "+
				"retrying it would hold this link for ever behind a record that cannot move",
				"peer_topic", b.lim.Loggable(upstream),
				"topic", b.lim.Loggable(topic), "reason", err,
				"attempts", attempt)
			return finished
		}

		// One line when it starts, not one per attempt: a channel that stays
		// full stays full for as long as nobody drains it.
		if attempt == 1 {
			b.log.Warn("a channel refused a bridged record, so the peer will not be "+
				"acknowledged until it takes it; the subscription is now backpressure",
				"peer_topic", b.lim.Loggable(upstream), "topic", b.lim.Loggable(topic), "reason", err)
		}

		select {
		case <-ctx.Done():
			return keep
		case <-time.After(jitter(wait)):
		}
		if wait < mostRetry {
			wait *= 2
		}
	}
}

// transient reports whether a refusal is one that can stop being true, and
// so whether the record is worth publishing again.
//
// **The list is of what may be retried, not of what may not**, and that is
// the whole design of it. A bridge holds a record it cannot place at the
// head of its link, unacknowledged, and every record behind it waits - so a
// refusal nobody classified must default to "give up and say so", never to
// "hold the link for ever". It defaulted the other way once, and one
// mistyped topic template stopped a bridge dead: 0 of 40 records crossed,
// with a single warning to show for it.
//
// Two codes qualify, and RFC 0002 names the reason for both: a full channel
// empties as consumers drain it, and a storage that is not working can start
// working. Everything else is about *this record* - a topic the broker will
// not publish, more headers than it allows - and no amount of waiting
// changes it.
func transient(err error) bool {
	var code packets.Code
	if !errors.As(err, &code) {
		// Not a reason code at all: an error from the injection itself
		// rather than from the broker's answer. Retried, because it says
		// nothing about the record.
		return true
	}
	switch code.Code {
	case packets.ErrQuotaExceeded.Code:
		// The channel is at its max_bytes. Records leave as consumers read
		// them and as retention trims, so this one clears itself. A quota
		// refusal for the record's own shape - too many headers - is caught
		// by Bounded before it can reach here and be mistaken for this.
		return true
	case packets.ErrImplementationSpecificError.Code:
		// The store did not work. It may again, and dropping records because
		// a disk is unhappy is the failure this whole path exists to avoid.
		return true
	}
	return false
}

// route is the first rule that covers this topic, and what it makes of it.
//
// Match's three-way answer is the whole of it: false is traffic no rule
// covers, true with an empty topic is traffic a rule covers and cannot
// place. The three warnings RFC 0002 asks for are the two ways of reaching
// the second, plus the first.
//
// The first is defensive rather than routine, which is worth saying so that
// nobody goes looking for it. Every rule's filter goes on the wire as
// saguin's SUBSCRIBE, so a record only arrives because some rule's filter
// asked for it, and that rule matches it here. What is left for this branch
// is an upstream that sends what was not subscribed to, and MQTT's rule that
// a wildcard does not match a topic beginning with `$` - which the upstream
// applies and this matcher does not. Both are the other broker misbehaving,
// which is exactly when a line in the log is worth having.
func (b *Bridge) route(upstream string) (topic string, ok bool) {
	for _, in := range b.cfg.Topics {
		// Only the rules that read. An `out` rule's filter is about local
		// topics, so matching an arriving record against it would place
		// records by a rule that describes the other direction.
		if !in.In {
			continue
		}
		topic, matched := in.Match(upstream)
		if !matched {
			continue
		}
		if topic == "" {
			// A rule covered it and produced nothing publishable. Match
			// answers the same way for two different reasons, and RFC 0002
			// lists them as two different drops - so the reason is worked out
			// here rather than reported as whichever one was written first.
			// An operator told the tail was empty, when what actually
			// happened is that the record aimed at the reserved space, goes
			// looking for a `#` that matched nothing and finds a healthy rule.
			b.unstoredUnmappable.Add(1)
			if in.Reserved(upstream) {
				b.log.Warn("dropping a bridged record: it would land in the reserved `$` space, "+
					"where saguin keeps its own control topics and a bridge may not forge them",
					"peer_topic", b.lim.Loggable(upstream), "filter", in.Filter, "template", in.Topic)
			} else {
				b.log.Warn("dropping a bridged record: the rule that covers it produced no topic",
					"peer_topic", b.lim.Loggable(upstream), "filter", in.Filter, "template", in.Topic)
			}
			return "", false
		}

		// **Where it lands is the topic's business and nobody else's.** The
		// rule built a topic; the broker resolves it exactly as it resolves
		// any other publish, so a bridge needs no opinion about which
		// channel that is and no way to hold a wrong one. That is
		// invariant 11 read from the writing side: a client never has to
		// know where the channels are, and a bridge is a client.
		return topic, true
	}
	b.unstoredNoRule.Add(1)
	b.log.Warn("dropping a bridged record: no rule covers it", "peer_topic", b.lim.Loggable(upstream))
	return "", false
}

// ack tells the upstream saguin is finished with the record, and it is the
// only place that does. Everything above decides whether to call it.
func (b *Bridge) ack(pr paho.PublishReceived) {
	// **No client is no link.** A record held for its predecessors can be
	// placed after the connection that carried it has gone, and the client
	// on it is then nil - acknowledging into that is a panic where the right
	// answer is silence: the upstream still holds the record and offers it
	// again, which is the duplicate this design already accepts.
	if pr.Client == nil {
		return
	}
	if err := pr.Client.Ack(pr.Packet); err != nil {
		// The link went between receiving and acknowledging. The upstream
		// still holds the record and will deliver it again, which is a
		// duplicate rather than a loss - the one this design accepts.
		b.log.Warn("cannot acknowledge a bridged record at its peer",
			"peer_topic", b.lim.Loggable(pr.Packet.Topic), "error", err)
	}
}

// propsOf reads them off an upstream publish.
//
// **Message Expiry is taken as the upstream sent it**, which is already
// decremented by the time it waited there - that is what a conforming server
// sends. Storing that is right: the record's clock starts again at this
// broker, and what crosses is how long the publisher's ceiling has left
// rather than what it originally was.
func propsOf(pk *paho.Publish) store.Props {
	// **The retain flag is on the fixed header, not among the properties**,
	// so it is read before the early return below - a peer sending a
	// retained value and no properties at all is the ordinary case for a
	// deletion, and losing the flag there would turn a delete into an empty
	// live publish that clears nothing.
	if pk.Properties == nil {
		return store.Props{Retain: pk.Retain}
	}
	out := store.Props{
		ContentType:     pk.Properties.ContentType,
		ResponseTopic:   pk.Properties.ResponseTopic,
		CorrelationData: pk.Properties.CorrelationData,
		// paho hands an empty Correlation Data over as an empty slice, and
		// an absent one as nil.
		CorrelationDataEmpty: pk.Properties.CorrelationData != nil && len(pk.Properties.CorrelationData) == 0,
		Retain:               pk.Retain,
	}
	if pk.Properties.PayloadFormat != nil {
		out.PayloadFormat = *pk.Properties.PayloadFormat
		out.PayloadFormatFlag = true
	}
	if pk.Properties.MessageExpiry != nil {
		out.MessageExpiry = *pk.Properties.MessageExpiry
	}
	return out
}

// headersOf carries the upstream's User Properties across as saguin
// headers.
//
// The `saguin-` prefix is reserved and the broker strips it on the way in,
// so a foreign broker cannot forge a Message ID, an offset or dead-letter
// metadata through a bridge. Nothing is filtered here - doing it in two
// places is how one of them stops matching the other.
func headersOf(pk *paho.Publish) []packets.UserProperty {
	if pk.Properties == nil || len(pk.Properties.User) == 0 {
		return nil
	}
	out := make([]packets.UserProperty, 0, len(pk.Properties.User))
	for _, u := range pk.Properties.User {
		out = append(out, packets.UserProperty{Key: u.Key, Val: u.Value})
	}
	return out
}

// jitter spreads a retry so that several bridges refused by one full channel
// do not come back at the same instant for ever after.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// Set runs every configured bridge until ctx is cancelled, and returns once
// they have all stopped.
//
// A bridge that cannot start is reported and the others still run: one
// unreachable upstream must not take a broker down, because the channels a
// bridge feeds are not the only channels there are.
func Set(ctx context.Context, bridges []*Bridge) error {
	var wg sync.WaitGroup
	errs := make([]error, len(bridges))
	for i, br := range bridges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = br.Run(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Names is what a caller logs at startup, so that "the bridges are running"
// says which.
func Names(bridges []*Bridge) string {
	names := make([]string, 0, len(bridges))
	for _, b := range bridges {
		names = append(names, b.cfg.Name)
	}
	return strings.Join(names, ", ")
}
