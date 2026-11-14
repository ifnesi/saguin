package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// delivery is one packet and its receipt time, before any wait for its
// subscriber to process it. Only the reader touches the client's decoder.
type delivery struct {
	packet harness.Packet
	at     time.Duration
	err    error
}

// subscriberReader keeps reading independently of acknowledgement delays.
// Its unbuffered handover holds at most one extra packet, and closing it
// joins the reader before the next connection starts delivering.
type subscriberReader struct {
	c     *harness.Client
	input chan delivery
	stop  chan struct{}
	done  chan struct{}
}

// readSubscriber starts c's reader. A read that times out is a quiet
// subscriber until the run is closing; then it is the connection's end,
// so a broker that keeps the connection open after the run's DISCONNECT
// holds the run no longer than the client's read deadline (answerTimeout),
// as receive's own read does.
func readSubscriber(c *harness.Client, r *run, closing *atomic.Bool) *subscriberReader {
	reader := &subscriberReader{c: c, input: make(chan delivery), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(reader.done)
		for {
			packet, err := c.Recv()
			at := time.Since(r.epoch)
			if harness.IsTimeout(err) && !closing.Load() {
				continue // a quiet subscriber is measured, not failed
			}
			select {
			case reader.input <- delivery{packet: packet, at: at, err: err}:
			case <-reader.stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return reader
}

func (reader *subscriberReader) close() {
	close(reader.stop)
	_ = reader.c.Close() // releases a read still on the socket
	<-reader.done
}

// delayedAck keeps only an identifier and its earliest send time. The
// broker's negotiated window bounds the outstanding acknowledgements.
type delayedAck struct {
	id  uint16
	due time.Time
}

// reconnectPlan is owned by the receive loop, just as the connection and
// its pending acknowledgements are. Kind 0 is takeover, kind 1 return.
type reconnectPlan struct {
	addr  string
	stop  <-chan struct{}
	every time.Duration
	kind  int
	next  time.Time // next reconnect, measured from the last CONNACK
}

// receiveConnections owns delivery accounting and every subscriber write.
// At a reconnect boundary it discards the old connection's pending ACKs
// and drains its last deliveries without sending anything more on it.
func (s *subscriber) receiveConnections(r *run, closing *atomic.Bool, plan *reconnectPlan) error {
	reader := readSubscriber(s.c, r, closing)
	defer func() { reader.close() }()
	ackTimer := time.NewTimer(time.Hour)
	defer ackTimer.Stop()
	ackTimer.Stop()
	var pending []delayedAck
	head := 0
	var stop <-chan struct{}
	var next <-chan time.Time
	var reconnectTimer *time.Timer
	stableSent := false
	stable := func() {
		if s.stable != nil && !stableSent {
			close(s.stable)
			stableSent = true
		}
	}
	if plan != nil {
		stop = plan.stop
		phase := time.Duration(s.index) * plan.every / time.Duration(max(r.subscribers, 1))
		reconnectTimer = time.NewTimer(time.Until(r.epoch.Add(plan.every + phase)))
		defer reconnectTimer.Stop()
		defer stable()
		next = reconnectTimer.C
	}
	for {
		// Stop has priority over a simultaneously due reconnect, and also
		// publishes a connection whose CONNECT was already in progress.
		select {
		case <-stop:
			reconnectTimer.Stop()
			stop, next = nil, nil
			stable()
		default:
		}
		var due <-chan time.Time
		if head < len(pending) {
			ackTimer.Reset(time.Until(pending[head].due))
			due = ackTimer.C
		}
		select {
		case <-stop:
			continue
		case <-next:
			select {
			case <-stop:
				continue
			default:
			}
			newReader, err := s.reconnect(r, reader, plan, closing)
			if err != nil {
				return err
			}
			reader = newReader
			pending, head = pending[:0], 0
			ackTimer.Stop()
			reconnectTimer.Reset(time.Until(plan.next))
		case <-due:
			ack := pending[head]
			head++
			if !closing.Load() && !r.afterCutoff(time.Since(r.epoch)) {
				if err := s.c.Send(&harness.Ack{Kind: harness.PUBACK, ID: ack.id}); err != nil {
					if !closing.Load() {
						return fmt.Errorf("subscriber %d: acknowledging: %w", s.index, err)
					}
				}
			}
			if head == len(pending) {
				pending, head = pending[:0], 0
			}
		case d := <-reader.input:
			if d.err != nil {
				if closing.Load() {
					return nil
				}
				return fmt.Errorf("subscriber %d: reading: %w", s.index, d.err)
			}
			pub, after, err := s.delivered(r, d.packet, d.at)
			if err != nil {
				return err
			}
			if pub.QoS == 1 && !after && !closing.Load() {
				// Compact only after at least half has been consumed, so
				// queue maintenance is amortized rather than per-window.
				if head > 0 && head >= len(pending)/2 {
					pending = pending[:copy(pending, pending[head:])]
					head = 0
				}
				pending = append(pending, delayedAck{id: pub.ID, due: r.epoch.Add(d.at).Add(ackDelay)})
			}
		}
	}
}

// drainConnection reads the old connection to its orderly end, still
// counting deliveries, with no acknowledgements after the transition.
func (s *subscriber) drainConnection(r *run, reader *subscriberReader, takeover bool) error {
	limit := time.NewTimer(answerTimeout)
	defer limit.Stop()
	disconnected := false
	for {
		select {
		case <-limit.C:
			return fmt.Errorf("subscriber %d: the old connection did not end within %v", s.index, answerTimeout)
		case d := <-reader.input:
			if d.err != nil {
				if err := reader.c.Closed(nil, d.err); err != nil {
					return err
				}
				if takeover && !disconnected {
					return fmt.Errorf("subscriber %d: takeover ended without DISCONNECT 0x8E", s.index)
				}
				return nil
			}
			if disconnect, ok := d.packet.(*harness.Disconnect); ok && takeover && !disconnected && disconnect.Reason == 0x8E {
				disconnected = true
				continue
			}
			if disconnected {
				return fmt.Errorf("subscriber %d: %s after takeover DISCONNECT", s.index, harness.Name(d.packet.Type()))
			}
			if _, _, err := s.delivered(r, d.packet, d.at); err != nil {
				return err
			}
		}
	}
}

// A connection is a reconnect's new connection, as dial made it: its client
// and CONNACK, its CONNACK delay, when its CONNECT began, since the run's
// epoch, and why it failed.
type connection struct {
	c     *harness.Client
	ack   *harness.Connack
	delay time.Duration
	at    time.Duration
	err   error
}

// dial makes s's next connection to addr, as every reconnect makes it.
func (s *subscriber) dial(r *run, addr string) connection {
	c, ack, delay, err := connectSubscriber(addr, s.index)
	return connection{c: c, ack: ack, delay: delay, at: time.Since(r.epoch) - delay, err: err}
}

// reconnect replaces the connection after one takeover or orderly return.
// A takeover keeps reading the old connection while its CONNECT awaits
// the CONNACK, so the old socket never backs up for want of a reader, and
// each delivery on it is stamped as it arrives. Only reconnects whose
// CONNECT is in the measured window are counted, as every other figure.
func (s *subscriber) reconnect(r *run, old *subscriberReader, plan *reconnectPlan, closing *atomic.Bool) (*subscriberReader, error) {
	takeover := plan.kind == 0
	var next connection
	if takeover {
		dialed := make(chan connection, 1)
		go func() {
			made := s.dial(r, plan.addr)
			if made.err != nil {
				_ = old.c.Close() // no takeover comes: the drain ends now; the dial's error is the one reported
			}
			dialed <- made
		}()
		drained := s.drainConnection(r, old, true)
		next = <-dialed
		if drained != nil && next.err == nil {
			_ = next.c.Close() // the old connection's failure is the one reported
			return nil, drained
		}
	} else {
		if err := leave([]*subscriber{s}, func(s *subscriber) error {
			return s.drainConnection(r, old, false)
		}); err != nil {
			return nil, err
		}
		next = s.dial(r, plan.addr)
	}
	if next.err != nil {
		return nil, next.err
	}
	acked := next.at + next.delay
	plan.next = r.epoch.Add(acked + plan.every)
	if !next.ack.SessionPresent {
		_ = next.c.Close()
		return nil, fmt.Errorf("subscriber %d: its session is not present", s.index)
	}
	if r.inWindow(next.at) {
		s.connack[plan.kind].add(next.delay)
	}
	old.close()
	s.c, s.reconnected = next.c, acked
	for k := range s.resumed {
		s.resumed[k] = s.resumed[k][:0] // the new connection has received nothing yet
	}
	plan.kind = 1 - plan.kind
	return readSubscriber(next.c, r, closing), nil
}
