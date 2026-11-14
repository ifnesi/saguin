package core

import "time"

// peerTime removes this connection's body-withholding intervals. Its bounded
// exchange heap uses this time base, so pausing shifts one indexed connection
// deadline rather than revisiting every exchange.
func (s *connectionState) peerTime(now time.Time) time.Time {
	if !s.peerPausedAt.IsZero() {
		now = s.peerPausedAt
	}
	return now.Add(-s.peerPaused)
}

func (c *Core) indexPeer(s *connectionState) {
	var at time.Time
	if s.peerObligations.Len() != 0 && s.peerPausedAt.IsZero() {
		at = s.peerObligations.items[0].at.Add(s.peerPaused)
	}
	c.admission.peers.set(&s.peerDeadline, at)
}

// requirePeer starts only on the successful packet-write report. An ACK
// already applied by the core creates no obligation, even if that report was
// delayed. Duplicate and unrelated acknowledgements never touch this clock.
func (c *Core) requirePeer(d *outgoing, now time.Time) {
	if c.admission.delay == 0 || d.acknowledged {
		return
	}
	s := d.consumer.conn
	p := &s.peerWindow[d.packet.Slot]
	p.owner, p.conn, p.delivery = d.consumer, s, d
	s.peerObligations.set(p, s.peerTime(now).Add(c.admission.peerTimeout))
	c.indexPeer(s)
}

func (c *Core) endPeer(d *outgoing) {
	if c.admission.delay == 0 || d.consumer.service {
		return
	}
	s := d.consumer.conn
	p := &s.peerWindow[d.packet.Slot]
	if p.overdue {
		d.consumer.admission.overdue--
	}
	if p.index != 0 {
		s.peerObligations.set(p, time.Time{})
		c.indexPeer(s)
	}
	*p = coreDeadline{}
}

// expireConnectionPeer applies due obligations before a withholding interval
// starts as well as at the indexed timer. Overdue entries remain exchanges,
// but leave the deadline heap until their valid acknowledgement ends them.
func (c *Core) expireConnectionPeer(s *connectionState, now time.Time) {
	at := s.peerTime(now)
	for s.peerObligations.Len() != 0 {
		p := s.peerObligations.items[0]
		if at.Before(p.at) {
			break
		}
		d := p.delivery
		s.peerObligations.set(p, time.Time{})
		p.overdue = true
		d.consumer.admission.overdue++
		c.touchAdmission(d.consumer)
	}
	c.indexPeer(s)
}

func (c *Core) expirePeers(now time.Time) {
	q := &c.admission.peers
	for q.Len() != 0 && !now.Before(q.items[0].at) {
		c.expireConnectionPeer(q.items[0].conn, now)
	}
}

func (c *Core) pausePeer(s *connectionState, now time.Time) {
	if c.admission.delay == 0 || !s.peerPausedAt.IsZero() {
		return
	}
	c.expireConnectionPeer(s, now)
	s.peerPausedAt = now
	c.indexPeer(s)
}

func (c *Core) resumePeer(s *connectionState, now time.Time) {
	if c.admission.delay == 0 || s.peerPausedAt.IsZero() {
		return
	}
	s.peerPaused += now.Sub(s.peerPausedAt)
	s.peerPausedAt = time.Time{}
	c.indexPeer(s)
}
