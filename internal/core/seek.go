package core

import (
	"database/sql"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// channelRequest keeps a request body only until its session reaches execution.
// Afterwards only the bounded answer metadata remains, never a loaded answer.
type channelRequest struct {
	reserved bool          // ordinary PUBLISH property policy still precedes execution
	channel  *channelState // bound in packet order once CONNECT has resolved
	publish  *packet.Publish
	bytes    int64
	serviceRequest
	offset, at int64
	beforeNow  bool
	byTime     bool
	answer     string
	properties []packet.Property
}

func (c *Core) requestPublish(acts []Action, ev connection.Event, p *connection.RequestPublish, charge int64, identity publishIdentity, now time.Time) []Action {
	s := c.connections[ev.Conn]
	q := &channelRequest{reserved: ev.Publish.Reserved, publish: &p.Publish, bytes: charge}
	if p.Channel >= 0 && p.Kind != connection.CatalogueRequest {
		q.channel = c.routed[p.Channel]
	}
	q.kind, q.quota, q.qos, q.id = requestService(p.Kind), ev.Quota, p.QoS, p.PacketID
	q.publishIdentity = identity
	q.topic, q.correlation = p.ResponseTopic, p.Correlation
	// Until its lane permits checking, the request keeps its incoming body
	// reservation. It takes no answer allowance before those checks, and a
	// QoS 0 packet needs no reply slot to bound its waiting body.
	r := &storeRequest{kind: executeRequest, conn: s, request: q,
		service: &q.serviceRequest}
	if q.qos != 0 {
		r.replySlot = s.storedReplies.PushBack(nil)
	}
	if s.phase != connecting {
		r.channel = q.channel
	}
	s.pendingAcks++
	if q.kind == seekService {
		s.pendingSubscriptions++
		s.subscriptionRevision++
		c.queueConsumers(s)
	}
	if q.kind == queueResponseService && s.phase == connected {
		// Identifier ownership is already fixed. The application result uses
		// the queue's owners, independently of takeover's session lane.
		return c.queueResponse(acts, r, now)
	}
	r.lane = c.lane(s.clientID)
	c.enqueue(r)
	return acts
}

// parseSeek distinguishes decimal offsets from time selectors. Duration
// parsing uses the standard library; decimal days are the one extra unit.
func parseSeek(text string, now time.Time) (offset int64, at time.Time, byTime, valid bool) {
	text = strings.TrimSpace(text)
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, time.Time{}, false, n >= -1
	}
	if t, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return 0, t, true, true
	}
	duration := strings.TrimPrefix(text, "-")
	if duration == "" || strings.HasPrefix(duration, "+") || strings.HasPrefix(duration, "-") {
		return
	}
	var d time.Duration
	var err error
	if decimal, daysUnit := strings.CutSuffix(duration, "d"); daysUnit {
		for _, digit := range decimal {
			if digit != '.' && (digit < '0' || digit > '9') {
				return
			}
		}
		days, e := strconv.ParseFloat(decimal, 64)
		if e != nil || math.IsNaN(days) || days < 0 || days > 106751 {
			return
		}
		d = time.Duration(days * float64(24*time.Hour))
	} else {
		d, err = time.ParseDuration(duration)
	}
	if err != nil || d < 0 || d > 106751*24*time.Hour {
		return
	}
	return 0, now.Add(-d), true, true
}

// startRequest executes after CONNECT and earlier session packets, reserving the
// consumer with the session and channel before it establishes its ordered view.
func (c *Core) startRequest(acts []Action, r *storeRequest, now time.Time) ([]Action, bool) {
	s, q := r.conn, r.request
	if s.end.seen || s.phase >= refused {
		c.cancelChannelRequest(r, now)
		return acts, true
	}
	if h := q.repeat; h != nil {
		c.returnBytes(q.bytes, now)
		q.bytes, q.publish = 0, nil
		if q.kind == seekService {
			c.completeSubscription(s)
		}
		r.request = nil // this packet only repeats the earlier request's exchange
		c.releaseSharedRequest(r)
		return c.repeatIncoming(acts, s, q.id, h, r.replySlot), true
	}
	p := q.publish
	if q.kind == queueResponseService {
		c.releaseSharedRequest(r)
		c.releaseChannel(r)
		lane := r.lane
		r.lane = nil
		c.endLane(lane, r)
		return c.queueResponse(acts, r, now), false
	}
	if q.kind != seekService {
		return c.startServiceRequest(acts, r, now)
	}
	if q.topic == "" {
		q.topic = string(p.Topic) + "/reply"
	}
	offset, at, byTime, valid := parseSeek(string(p.Payload), now)
	c.returnBytes(q.bytes, now)
	q.bytes, q.publish = 0, nil
	q.offset, q.at, q.byTime, q.beforeNow = offset, at.UnixMilli(), byTime, at.Before(now)
	if q.reserved {
		c.retained.reservedProperty.Inc()
		return c.finishChannelRequest(acts, r, 0x83, now), true
	}
	switch {
	case s.session.Expiry == 0:
		q.answer = "no-session"
	case !valid:
		q.answer = "malformed"
	}
	if q.answer != "" {
		return c.finishChannelRequest(acts, r, 0x83, now), true
	}
	r.kind, r.op = readSeek, &store.ReadSeekPosition{Channel: r.channel.row.ID, Time: sql.NullInt64{Int64: timestamp.CeilMilli(at), Valid: byTime}}
	if u := s.consumer(r.channel.row.ID); u != nil {
		c.joinConsumerRead(r, u)
		c.queueConsumer(u)
	}
	c.continueRequest(r)
	return acts, false
}

func (c *Core) readSeek(acts []Action, r *storeRequest, now time.Time) []Action {
	s, q, view := r.conn, r.request, r.op.(*store.ReadSeekPosition)
	if s.end.seen || s.phase != connected || c.failure != nil {
		return c.finishChannelRequest(acts, r, 0, now)
	}
	offset := q.offset
	if q.byTime {
		offset = view.Next
		if view.Found.Valid {
			offset = view.Found.Int64
		}
		if view.Floor == 1 {
			if !view.First.Valid || q.at < view.First.Int64 {
				offset = view.Floor
			}
		} else if (view.First.Valid && q.at < view.First.Int64) || (!view.First.Valid && q.beforeNow) {
			offset = view.Floor - 1
		}
	} else {
		switch offset {
		case 0:
			offset = view.Floor
		case -1:
			offset = view.Next
		}
	}
	switch {
	case offset < view.Floor:
		q.answer = "below-floor"
		q.properties = append(q.properties, packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-floor", Value: strconv.FormatInt(view.Floor, 10)}})
	case offset > view.Next:
		q.answer = "beyond-end"
		q.properties = append(q.properties, packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-next", Value: strconv.FormatInt(view.Next, 10)}})
	}
	if q.answer != "" {
		return c.finishChannelRequest(acts, r, 0x83, now)
	}
	q.offset = offset
	var admitted bool
	acts, admitted = c.admitServiceRequest(acts, r, now)
	if !admitted {
		return acts
	}
	op := &store.SeekPosition{Offset: offset, At: now.UnixMilli()}
	op.Session, op.Channel = s.session.ID, r.channel.row.ID
	// Capture every never-sent preparation once. The seek owns its cancellation
	// through commit, including a connection ending while the store owns it.
	for i := range s.window {
		d := &s.window[i]
		if d.consumer == nil || d.consumer.channel != r.channel || d.exchange.Attempt != nil || d.attempted || d.settled {
			continue
		}
		d.saving = true
		if d.consumer.sharedCursor != nil && d.consumer.share.active {
			if op.Markers == nil {
				op.Markers = new(store.DeliveryMarkers)
			}
			op.Markers.Return = append(op.Markers.Return, store.SharedReturn{Share: d.consumer.share.id, SharedRecord: d.sharedRecord()})
		}
		r.cancellations = append(r.cancellations, d)
		if d.saved {
			op.Delete = append(op.Delete, d.exchange.PacketID)
		}
	}
	r.kind, r.op = saveSeek, op
	c.continueRequest(r)
	return acts
}

func (c *Core) savedSeek(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	s, q := r.conn, r.request
	c.finishCancellations(r, err, now)
	if err != nil {
		return c.finishChannelRequest(acts, r, 0x97, now)
	}
	// A committed seek supersedes earlier progress even if departure has
	// begun. Only its reply is connection-local; departure must keep its
	// position and must not restore movement by the earlier exchanges.
	u := s.consumer(r.channel.row.ID)
	if u == nil {
		u = c.newConsumer(s, r.channel, q.offset)
		s.addConsumer(u)
	}
	u.persisted = q.offset
	u.supersedePosition(q.offset, now.UnixMilli())
	u.markersBefore = 0
	c.queueConsumer(u)
	return c.finishChannelRequest(acts, r, 0, now)
}

// finishChannelRequest releases the request's owners before completing it.
// Its acknowledgement is ready with the checks or committed change, while
// its separately bounded answer follows the ordinary delivery path.
func (c *Core) finishChannelRequest(acts []Action, r *storeRequest, reason byte, now time.Time) []Action {
	c.releaseSharedRequest(r)
	s, q := r.conn, r.request
	r.request = nil // completion owns the reply; connection cancellation cannot end it again
	for _, u := range c.finishConsumerRead(r) {
		c.queueConsumer(u)
	}
	if q.kind == seekService {
		c.completeSubscription(s)
	}
	var body serviceBody = q
	if q.kind == queueResponseService {
		body = nil
	}
	if q.answer == "" && reason != 0 {
		body = nil
	}
	return c.answerService(acts, r, body, reason, q.properties, now)
}

func (q *channelRequest) Size() int {
	if q.answer != "" {
		return len(q.answer)
	}
	var buf [20]byte
	return len(strconv.AppendInt(buf[:0], q.offset, 10))
}

func (q *channelRequest) Load(into *[]byte) store.Read {
	if q.answer != "" {
		*into = []byte(q.answer)
	} else {
		*into = strconv.AppendInt(make([]byte, 0, q.Size()), q.offset, 10)
	}
	return nil
}

// cancelChannelRequest supersedes an undecided request. An already dispatched
// read returns only its credit; a stored result still completes normally.
func (c *Core) cancelChannelRequest(r *storeRequest, now time.Time) {
	s, q := r.conn, r.request
	if q == nil {
		return
	}
	c.returnBytes(q.bytes, now)
	q.bytes, q.publish = 0, nil
	for _, u := range c.finishConsumerRead(r) {
		c.queueConsumer(u)
	}
	if q.kind == seekService {
		c.completeSubscription(s)
	}
	c.cancelServiceRequest(r)
	if r.channel != nil {
		c.releaseChannel(r)
		if r.channel.pending == r {
			r.channel.pending = nil
		}
	}
	r.request = nil
	c.cancelPendingRequest(r)
}

func (c *Core) cancelChannelRequests(s *connectionState, now time.Time) {
	lane := c.lanes[s.clientID]
	if lane == nil || lane.request == nil {
		return
	}
	r := lane.request
	if r.conn != s || r.request == nil || (r.kind != executeRequest && r.kind != readSeek && r.kind != readKVSize) {
		return
	}
	c.cancelChannelRequest(r, now)
	c.endLane(lane, r)
	r.lane = nil
}
