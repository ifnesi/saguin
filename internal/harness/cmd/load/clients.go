package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// bufferedConn reads its connection through a buffer. The harness reads a
// packet's fixed header a byte at a time, which, unbuffered, is a system
// call per byte: a cost of the load tool, not of the broker.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

// Read reads through the buffer; deadlines set on the connection apply.
func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// buffered wraps conn in a harness client reading through a buffer.
func buffered(conn net.Conn, version byte) *harness.Client {
	return harness.NewClient(&bufferedConn{Conn: conn, r: bufio.NewReader(conn)}, version)
}

// connect dials addr and connects as id over MQTT 5, keep-alive off so no
// ping competes with the load, and returns the client and its CONNACK,
// which must accept. expiry above zero asks for a durable session (Clean
// Start 0), zero for a clean one (Clean Start 1).
func connect(addr, id string, expiry uint32) (*harness.Client, *harness.Connack, error) {
	c, ack, _, err := connectWithProperties(addr, id, expiry, nil)
	return c, ack, err
}

// connectSubscriber uses the same CONNECT on setup, drain and reconnect,
// measuring only CONNECT through CONNACK, without the TCP dial.
func connectSubscriber(addr string, index int) (*harness.Client, *harness.Connack, time.Duration, error) {
	var props harness.Properties
	if receiveMaximum != 0 {
		props = append(props, harness.Property{ID: harness.ReceiveMaximum, Int: uint32(receiveMaximum)})
	}
	return connectWithProperties(addr, "load-sub-"+strconv.Itoa(index), sessionExpiry, props)
}

// connectWithProperties adds caller properties to the session's CONNECT.
func connectWithProperties(addr, id string, expiry uint32, props harness.Properties) (*harness.Client, *harness.Connack, time.Duration, error) {
	conn, err := net.DialTimeout("tcp", addr, harness.DefaultTimeout)
	if err != nil {
		return nil, nil, 0, err
	}
	c := buffered(conn, harness.V5)
	c.Timeout = answerTimeout
	p := &harness.Connect{ClientID: id, CleanStart: expiry == 0, Props: props}
	if expiry > 0 {
		p.Props = append(p.Props, harness.Property{ID: harness.SessionExpiryInterval, Int: expiry})
	}
	start := time.Now()
	ack, err := c.Connect(p)
	delay := time.Since(start)
	if err == nil && ack.Reason != 0 {
		err = fmt.Errorf("CONNACK 0x%02X", ack.Reason)
	}
	if err != nil {
		_ = c.Close() // refused: nothing more to learn from it
		return nil, nil, 0, fmt.Errorf("connecting %s: %w", id, err)
	}
	return c, ack, delay, nil
}

// A stamp is what a payload carries for the subscriber to measure and
// check a delivery: when the publication was due and when it was sent, as
// times since the run's epoch, which publisher sent it, and its sequence
// number in that publisher's publications. In the maximum mode a
// publication is due when its publisher starts on it, before any wait for
// its Receive Maximum. The payload's first stampSize bytes hold it,
// followed by the payload's own length, so a payload cut short is caught;
// every payload is at least stampSize bytes (payload.go).
type stamp struct {
	due, sent time.Duration
	publisher int
	seq       uint64
}

// stampSize is the bytes a stamp takes at the start of a payload, its
// length included.
const stampSize = 32

// put writes s and the payload's length into payload, which is at least
// stampSize bytes; the rest of the payload stays as it is.
func (s stamp) put(payload []byte) {
	binary.BigEndian.PutUint64(payload[0:], uint64(s.due))
	binary.BigEndian.PutUint64(payload[8:], uint64(s.sent))
	binary.BigEndian.PutUint32(payload[16:], uint32(s.publisher))
	binary.BigEndian.PutUint64(payload[20:], s.seq)
	binary.BigEndian.PutUint32(payload[28:], uint32(len(payload)))
}

// readStamp reads what put wrote.
func readStamp(payload []byte) (stamp, error) {
	if len(payload) < stampSize {
		return stamp{}, fmt.Errorf("a payload of %d bytes, shorter than its stamp's %d", len(payload), stampSize)
	}
	if n := binary.BigEndian.Uint32(payload[28:]); int(n) != len(payload) {
		return stamp{}, fmt.Errorf("a payload of %d bytes, sent with %d", len(payload), n)
	}
	return stamp{
		due:       time.Duration(binary.BigEndian.Uint64(payload[0:])),
		sent:      time.Duration(binary.BigEndian.Uint64(payload[8:])),
		publisher: int(binary.BigEndian.Uint32(payload[16:])),
		seq:       binary.BigEndian.Uint64(payload[20:]),
	}, nil
}

// A publisher is one publishing client, at one QoS throughout: a writer,
// its publish loop, and, at QoS 1, a reader, its PUBACK loop, which share
// the client, the writer only sending and the reader only receiving.
type publisher struct {
	index int
	qos   byte
	c     *harness.Client
	// idle keeps the publisher connected but publishing nothing, and limit,
	// where above zero, stops it after that many publications: for the
	// experiments (experiment.go), never the recipe.
	idle  bool
	limit uint64
	// window holds one token per unacknowledged publication; its capacity
	// is the CONNACK's Receive Maximum.
	window chan struct{}
	// outstanding maps the packet identifiers awaiting their PUBACK to
	// their publications' sequence numbers.
	mu          sync.Mutex
	outstanding map[uint16]sentAt
	// sent is how many publications the writer has written, numbered from
	// 0; read while it runs, for the backlog.
	sent atomic.Uint64
	// done is set once publishing and settlement end. writeCut lets the
	// reader recognize the close caused by our own publication deadline.
	done, writeCut atomic.Bool
	cut            int64 // writes cut at the run's end; never counted as sent
	// The writer's own record, read once it has ended: cohort is the
	// sequence numbers sent in the measured window, [cohort[0],
	// cohort[1]); in the fixed mode, scheduled counts the slots due in the
	// window, missed those skipped for lateness, unsent those due but not
	// begun when the publishers stopped, and delay holds each sent
	// publication's delay from its due time.
	cohort                    [2]uint64
	scheduled, missed, unsent int64
	delay                     histogram
	// The counts in the measured window: publications begun (attempted),
	// written to the socket, their payload bytes, and acknowledged.
	attempted, written atomic.Int64
	writtenBytes       atomic.Int64
	acked              atomic.Int64
	// ackedSeqs holds the sequence numbers acknowledged, ackedLate how
	// many of them after the cutoff; the reader's, read once it has ended.
	ackedSeqs              seqSet
	ackedBefore, ackedLate int64
	// ackLatency holds, for each publication sent in the measured window,
	// the time from its send to its PUBACK; the reader's.
	ackLatency histogram
}

// sentAt is an outstanding publication: its sequence number and when it
// was sent, since the run's epoch.
type sentAt struct {
	seq  uint64
	sent time.Duration
}

// due is when slot of this publisher is due in the fixed mode: its phase
// plus slot periods, the publishers' phases spread evenly over one period.
func (p *publisher) due(r *run, slot uint64) time.Duration {
	return time.Duration(p.index)*r.period/time.Duration(len(r.topics)) + time.Duration(slot)*r.period
}

// publish sends publications until stop closes, paced by r, then waits
// until the cutoff for every QoS 1 one to be acknowledged.
//
// The fixed mode's missed-slot policy: each slot is due at its own time
// and a publication is never sent before it. One begun so late that its
// next slot is already due takes the latest slot due instead, and the
// slots passed are missed, counted and never sent, so no burst catches
// up. A publication's delay is from its slot's due time to its send.
func (p *publisher) publish(r *run, stop <-chan struct{}, cutoff time.Time) error {
	defer p.done.Store(true)
	buf := make([]byte, payloads.largest())
	sizes := payloads.source(p.index)
	pkt := &harness.Publish{QoS: p.qos, Topic: r.topics[p.index]}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var slot uint64
	for seq := uint64(0); ; seq++ {
		if p.idle || (p.limit > 0 && seq >= p.limit) {
			return p.settle(r, slot, cutoff)
		}
		due := time.Since(r.epoch)
		if r.period > 0 {
			due = p.due(r, slot)
			timer.Reset(due - time.Since(r.epoch))
			select {
			case <-timer.C:
			case <-stop:
				return p.settle(r, slot, cutoff)
			}
		}
		select {
		case <-stop:
			return p.settle(r, slot, cutoff)
		default:
		}
		if p.qos == 1 {
			select {
			case p.window <- struct{}{}:
			case <-stop:
				return p.settle(r, slot, cutoff)
			}
		}
		if r.period > 0 {
			slot, due = p.skip(r, slot)
		}
		if r.inWindow(due) {
			p.attempted.Add(1)
		}
		s := stamp{due: due, sent: time.Since(r.epoch), publisher: p.index, seq: seq}
		if p.qos == 1 {
			pkt.ID = p.c.NextID()
			p.mu.Lock()
			p.outstanding[pkt.ID] = sentAt{seq: seq, sent: s.sent}
			p.mu.Unlock()
		}
		pkt.Payload = buf[:payloads.draw(sizes)]
		s.put(pkt.Payload)
		deadline := time.Now().Add(answerTimeout)
		if cutoff.Before(deadline) {
			deadline = cutoff
		}
		if err := p.c.SendUntil(pkt, deadline); err != nil {
			if deadline.Equal(cutoff) && errors.Is(err, os.ErrDeadlineExceeded) {
				p.cut++
				if p.qos == 1 {
					p.mu.Lock()
					delete(p.outstanding, pkt.ID)
					p.mu.Unlock()
					<-p.window
				}
				p.writeCut.Store(true)
				_ = p.c.Close() // a partial packet cannot be followed by DISCONNECT
				return p.settle(r, slot+1, cutoff)
			}
			return fmt.Errorf("publisher %d: sending: %w", p.index, err)
		}
		// Capture completion once, before updating any accounting. Totals
		// and second buckets use this same timestamp even if a later
		// counter update is delayed. A write begun in the window but
		// completed after it contributes to neither window count.
		writtenAt := time.Since(r.epoch)
		p.sent.Store(seq + 1)
		if r.inWindow(s.sent) {
			if p.cohort[1] == 0 {
				p.cohort[0] = seq
			}
			p.cohort[1] = seq + 1
		}
		if r.writes != nil {
			r.writes[p.index].add(writtenAt, r.windowLength)
		}
		if r.inWindow(writtenAt) {
			p.written.Add(1)
			p.writtenBytes.Add(int64(len(pkt.Payload)))
		}
		if r.period > 0 && r.inWindow(due) {
			p.delay.add(s.sent - due)
		}
		slot++
	}
}

// skip applies the missed-slot policy to a publication about to take
// slot: while the next slot is due already, slot is missed and the next
// taken. It returns the slot taken and its due time, counting the slots
// due in the window, missed or not.
func (p *publisher) skip(r *run, slot uint64) (uint64, time.Duration) {
	now := time.Since(r.epoch)
	for p.due(r, slot+1) <= now {
		if r.inWindow(p.due(r, slot)) {
			p.scheduled++
			p.missed++
			r.skipped[p.index].add(p.due(r, slot), r.windowLength)
		}
		slot++
	}
	if r.inWindow(p.due(r, slot)) {
		p.scheduled++
	}
	return slot, p.due(r, slot)
}

// settle records, in the fixed mode, the slots due in the window and not
// begun when the publishers stopped, from slot on; then waits until the
// cutoff for every QoS 1 publication to be acknowledged, by taking every
// token of the window, and leaves the window as it found it.
func (p *publisher) settle(r *run, slot uint64, cutoff time.Time) error {
	if r.period > 0 {
		for ; p.due(r, slot) < warmUp+measuredWindow; slot++ {
			if r.inWindow(p.due(r, slot)) {
				p.scheduled++
				p.unsent++
				r.skipped[p.index].add(p.due(r, slot), r.windowLength)
			}
		}
	}
	limit := time.NewTimer(time.Until(cutoff))
	defer limit.Stop()
	taken := 0
	defer func() {
		for range taken {
			<-p.window
		}
	}()
	for range cap(p.window) {
		select {
		case p.window <- struct{}{}:
			taken++
		case <-limit.C:
			return nil // the unacknowledged are counted at the cutoff
		}
	}
	return nil
}

// unacknowledged is how many QoS 1 publications await their PUBACK.
func (p *publisher) unacknowledged() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.outstanding)
}

// acks reads the publisher's PUBACKs until the connection ends after the
// run's DISCONNECT: each must accept (0x00) and answer an outstanding
// packet identifier, which it frees with one token of the window.
func (p *publisher) acks(r *run, closing *atomic.Bool) error {
	defer func() { _ = p.c.Close() }() // read to its end
	for {
		got, err := p.c.Recv()
		switch {
		case err != nil && (closing.Load() || p.writeCut.Load()):
			return nil
		case err != nil && harness.IsTimeout(err) && len(p.window) == 0:
			continue // nothing is awaited: a quiet connection is no loss
		case err != nil:
			return fmt.Errorf("publisher %d: reading: %w", p.index, err)
		}
		a, ok := got.(*harness.Ack)
		if !ok || a.Kind != harness.PUBACK {
			return fmt.Errorf("publisher %d: %s, want PUBACK", p.index, harness.Name(got.Type()))
		}
		// MQTT 5, PUBACK reason codes: 0x00 Success and 0x10 No matching
		// subscribers both accept the message.
		if a.Reason != 0x00 && a.Reason != 0x10 {
			return fmt.Errorf("publisher %d: PUBACK 0x%02X, want 0x00 or 0x10", p.index, a.Reason)
		}
		at := time.Since(r.epoch)
		after := r.afterCutoff(at)
		p.mu.Lock()
		out, awaited := p.outstanding[a.ID]
		delete(p.outstanding, a.ID)
		p.mu.Unlock()
		if !awaited {
			return fmt.Errorf("publisher %d: PUBACK %d, an identifier no publication awaits", p.index, a.ID)
		}
		p.ackedSeqs.add(out.seq)
		if r.inWindow(out.sent) && !after {
			p.ackLatency.add(at - out.sent)
		}
		if after {
			p.ackedLate++
		} else {
			p.ackedBefore++
		}
		if r.inWindow(time.Since(r.epoch)) {
			p.acked.Add(1)
		}
		select {
		case <-p.window:
		default: // the window's token was taken by settle, which gives it back
		}
	}
}

// A subscriber is one subscribing client, with what it has received; its
// receive loop owns every field but unique.
type subscriber struct {
	index int
	group int
	c     *harness.Client
	// stable closes once reconnects can no longer replace c. Shutdown
	// waits for it before accessing the last connection.
	stable chan struct{}
	// connack records the CONNECT-to-CONNACK time of the reconnect
	// experiment's reconnects whose CONNECT is in the measured window,
	// takeovers then returns; resends counts its QoS 1 deliveries with DUP
	// that are the broker's resends after a reconnect. reconnected is when
	// the current connection's CONNACK came, since the run's epoch, where
	// it replaced another; zero for the first connection. resumed holds,
	// for each publisher of the group, the sequence numbers of the QoS 1
	// publications sent before that CONNACK received on the current
	// connection, so each is excused as a resend at most once there.
	connack     [2]histogram
	resends     int64
	reconnected time.Duration
	resumed     [groupSize]seqSet
	// seen holds, for each publisher of the group by its place in the
	// group, the sequence numbers received before the cutoff; late, those
	// received after it, while the connection closes; last, the highest
	// received of each, plus one.
	seen, late [groupSize]seqSet
	last       [groupSize]uint64
	// unique counts the distinct publications received before the cutoff,
	// read by the drain while the loop runs; byQoS, the same by QoS.
	unique atomic.Int64
	byQoS  [2]atomic.Int64
	// duplicates counts publications received more than once; reordered,
	// those received after a later one of the same publisher.
	duplicates, reordered int64
	// arrived counts the distinct publications received in the measured
	// window, and arrivedBytes their payload bytes.
	arrived, arrivedBytes int64
	// cohort counts the distinct publications sent in the measured window
	// received before the cutoff; sendLatency and dueLatency hold their
	// latencies from send and from due time.
	cohort                  int64
	sendLatency, dueLatency histogram
}

// receive reads deliveries until the connection ends after the run's
// DISCONNECT. Without ackDelay it reads, accounts and acknowledges each
// delivery directly, preserving the recipe's workload. Delayed ACKs use
// the diagnostic reader; reconnects enters that path separately.
func (s *subscriber) receive(r *run, closing *atomic.Bool) error {
	if ackDelay != 0 {
		return s.receiveConnections(r, closing, nil)
	}
	defer func() { _ = s.c.Close() }() // read to its end
	for {
		got, err := s.c.Recv()
		switch {
		case err != nil && closing.Load():
			return nil
		case err != nil && harness.IsTimeout(err):
			continue // a quiet subscriber is measured, not failed
		case err != nil:
			return fmt.Errorf("subscriber %d: reading: %w", s.index, err)
		}
		pub, after, err := s.delivered(r, got, time.Since(r.epoch))
		if err != nil {
			return err
		}
		if !after {
			if err := s.c.Acknowledge(pub); err != nil && !closing.Load() {
				return fmt.Errorf("subscriber %d: acknowledging: %w", s.index, err)
			}
		}
	}
}

// delivered checks and records a packet through the common accounting,
// returning its cutoff classification for the acknowledgement decision.
func (s *subscriber) delivered(r *run, packet harness.Packet, at time.Duration) (*harness.Publish, bool, error) {
	pub, ok := packet.(*harness.Publish)
	if !ok {
		return nil, false, fmt.Errorf("subscriber %d: %s, want PUBLISH", s.index, harness.Name(packet.Type()))
	}
	after := r.afterCutoff(at)
	if err := s.record(r, pub, at, after); err != nil {
		return nil, false, fmt.Errorf("subscriber %d: %w", s.index, err)
	}
	return pub, after, nil
}

// record checks one delivery, received at time at (after the cutoff where
// after), against the recipe (a publisher of its group, on that
// publisher's topic, at the publication's QoS, the lower of it and the
// QoS 1 grant: design.md, SUBSCRIBE, One copy) and counts it.
func (s *subscriber) record(r *run, pub *harness.Publish, at time.Duration, after bool) error {
	st, err := readStamp(pub.Payload)
	switch {
	case err != nil:
		return err
	case st.publisher < 0 || st.publisher >= len(r.topics) || st.publisher/groupSize != s.group:
		return fmt.Errorf("a publication of publisher %d, outside group %d", st.publisher, s.group)
	case pub.Topic != r.topics[st.publisher]:
		return fmt.Errorf("publisher %d's publication on %q, want %q", st.publisher, pub.Topic, r.topics[st.publisher])
	case pub.QoS != r.qos[st.publisher]:
		return fmt.Errorf("publisher %d's publication %d at QoS %d, want %d", st.publisher, st.seq, pub.QoS, r.qos[st.publisher])
	}
	k := st.publisher % groupSize
	duplicate := s.seen[k].has(st.seq) || s.late[k].has(st.seq)
	// A QoS 1 delivery with DUP is the broker's resend only where it
	// follows a reconnect, its publication was sent before that
	// reconnect's CONNACK, so the broker could hold it unacknowledged then
	// (design.md, CONNECT, Resume; MQTT-4.4.0-1), and it is the
	// publication's first receipt on this connection: while connected,
	// the broker starts no resend (design.md, Delivery). Any other repeat
	// is a duplicate.
	resend := false
	if pub.QoS == 1 && st.sent < s.reconnected {
		first := s.resumed[k].add(st.seq)
		resend = pub.Dup && first
	}
	if resend {
		s.resends++
	}
	if duplicate {
		if !resend {
			s.duplicates++
		}
		return nil
	}
	if !resend && st.seq+1 < s.last[k] {
		s.reordered++
	}
	s.last[k] = max(s.last[k], st.seq+1)
	if after {
		s.late[k].add(st.seq)
		return nil
	}
	s.seen[k].add(st.seq)
	s.unique.Add(1)
	s.byQoS[pub.QoS].Add(1)
	// Rates, series and backlog all count the same distinct receipts, in
	// every mode. MQTT resends and unexpected duplicates are reported
	// separately.
	if r.inWindow(at) {
		s.arrived++
		s.arrivedBytes += int64(len(pub.Payload))
	}
	if r.receipts != nil {
		r.receipts[s.index].add(at, r.windowLength)
	}
	if r.inWindow(st.sent) {
		s.cohort++
		s.sendLatency.add(at - st.sent)
		s.dueLatency.add(at - st.due)
	}
	return nil
}

// A seqSet is a set of sequence numbers, one bit each.
type seqSet []uint64

// add adds n and reports whether it was new.
func (s *seqSet) add(n uint64) bool {
	word, bit := n/64, uint64(1)<<(n%64)
	for uint64(len(*s)) <= word {
		*s = append(*s, 0)
	}
	if (*s)[word]&bit != 0 {
		return false
	}
	(*s)[word] |= bit
	return true
}

// has reports whether n is in the set.
func (s seqSet) has(n uint64) bool {
	word := n / 64
	return word < uint64(len(s)) && s[word]&(1<<(n%64)) != 0
}

// last is the highest number in the set, and whether there is one.
func (s seqSet) last() (uint64, bool) {
	for word, set := range slices.Backward(s) {
		if set != 0 {
			return uint64(word)*64 + uint64(bits.Len64(set)) - 1, true
		}
	}
	return 0, false
}
