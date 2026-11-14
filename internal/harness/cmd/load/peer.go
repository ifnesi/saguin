package main

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// peerReceiveMaximum is the Receive Maximum the peer's CONNACK grants, and
// the peer's own in-flight window toward each subscriber: the broker's
// defaults (config.md, qos2.max_inflight_per_client and
// limits.max_inflight), so the clients' windows are the same.
const peerReceiveMaximum = 20

// A peer is a fast in-process MQTT test peer standing in for the broker,
// to show the load tool's own headroom: the same clients, concurrency,
// fan-out and checks, against a peer that stores nothing and routes each
// publication straight to its group's subscribers. It speaks through the
// harness client playing the server, and speaks only what the load tool
// sends: CONNECT, SUBSCRIBE to load/<group>/+, PUBLISH at QoS 0 or 1,
// PUBACK and DISCONNECT. A subscriber's queue full holds its publishers,
// so the peer never drops what the checks would count lost.
type peer struct {
	ln     net.Listener
	mu     sync.Mutex
	groups map[string][]*peerSubscriber // by topic prefix, load/<group>/
	served sync.WaitGroup
}

// A peerSubscriber is one subscriber connection: its queue of deliveries,
// written by its own writer, which holds at most peerReceiveMaximum QoS 1
// deliveries unacknowledged.
type peerSubscriber struct {
	c      *harness.Client
	out    chan *harness.Publish
	tokens chan struct{}
	done   chan struct{}
}

// startPeer listens on a loopback port and serves each connection.
func startPeer() (*peer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &peer{ln: ln, groups: map[string][]*peerSubscriber{}}
	p.served.Go(p.accept)
	return p, nil
}

// addr is the peer's address.
func (p *peer) addr() string { return p.ln.Addr().String() }

// stop closes the listener and waits for every connection to end, which
// the clients' closes end.
func (p *peer) stop() error {
	err := p.ln.Close()
	p.served.Wait()
	return err
}

// accept serves each connection until the listener closes.
func (p *peer) accept() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.served.Go(func() {
			defer func() { _ = conn.Close() }() // the client's close ends it
			p.serve(buffered(conn, 0))
		})
	}
}

// serve answers a CONNECT, then serves a publisher or a subscriber, by
// its client id.
func (p *peer) serve(c *harness.Client) {
	c.Timeout = time.Hour // idle clients during a drain are no failure
	got, err := c.Recv()
	connect, ok := got.(*harness.Connect)
	if err != nil || !ok {
		return
	}
	ack := &harness.Connack{Props: harness.Properties{{ID: harness.ReceiveMaximum, Int: peerReceiveMaximum}}}
	if c.Send(ack) != nil {
		return
	}
	if strings.HasPrefix(connect.ClientID, "load-pub-") {
		p.publisher(c)
	} else {
		p.subscriber(c)
	}
}

// publisher acknowledges each QoS 1 PUBLISH and routes every one to the
// subscribers of its group, at the publication's QoS.
func (p *peer) publisher(c *harness.Client) {
	for {
		got, err := c.Recv()
		pub, ok := got.(*harness.Publish)
		if err != nil || !ok {
			return
		}
		if pub.QoS == 1 && c.Send(&harness.Ack{Kind: harness.PUBACK, ID: pub.ID}) != nil {
			return
		}
		prefix := pub.Topic[:strings.LastIndexByte(pub.Topic, '/')+1]
		p.mu.Lock()
		subs := p.groups[prefix]
		p.mu.Unlock()
		for _, s := range subs {
			delivery := *pub // each subscriber's own packet identifier
			delivery.ID = 0
			select {
			case s.out <- &delivery:
			case <-s.done:
			}
		}
	}
}

// subscriber takes the SUBSCRIBE, grants QoS 1, starts the writer and
// reads PUBACKs, each freeing one delivery of the window, until the
// connection ends.
func (p *peer) subscriber(c *harness.Client) {
	got, err := c.Recv()
	sub, ok := got.(*harness.Subscribe)
	if err != nil || !ok || len(sub.Subs) != 1 {
		return
	}
	s := &peerSubscriber{c: c, out: make(chan *harness.Publish, 1024),
		tokens: make(chan struct{}, peerReceiveMaximum), done: make(chan struct{})}
	prefix := strings.TrimSuffix(sub.Subs[0].Filter, "+")
	// Registered before the SUBACK, so a publication sent once the client
	// has it is routed to this subscriber.
	p.mu.Lock()
	p.groups[prefix] = append(p.groups[prefix], s)
	p.mu.Unlock()
	if c.Send(&harness.Suback{ID: sub.ID, Reasons: []byte{1}}) != nil {
		p.mu.Lock()
		p.groups[prefix] = removed(p.groups[prefix], s)
		p.mu.Unlock()
		return
	}
	var writer sync.WaitGroup
	writer.Go(s.write)
	for {
		got, err := c.Recv()
		if a, ok := got.(*harness.Ack); err != nil || !ok || a.Kind != harness.PUBACK {
			break
		}
		<-s.tokens
	}
	close(s.done)
	_ = c.Close() // ends a writer blocked on the socket
	writer.Wait()
	p.mu.Lock()
	p.groups[prefix] = removed(p.groups[prefix], s)
	p.mu.Unlock()
}

// write sends the subscriber's deliveries until it is done.
func (s *peerSubscriber) write() {
	for {
		var d *harness.Publish
		select {
		case d = <-s.out:
		case <-s.done:
			return
		}
		if d.QoS == 1 {
			select {
			case s.tokens <- struct{}{}:
			case <-s.done:
				return
			}
			d.ID = s.c.NextID()
		}
		if err := s.c.Send(d); err != nil && !errors.Is(err, net.ErrClosed) {
			return
		}
	}
}

// removed is subs without s.
func removed(subs []*peerSubscriber, s *peerSubscriber) []*peerSubscriber {
	out := subs[:0:0]
	for _, x := range subs {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}
