package core

import (
	"errors"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

type serviceKind uint8

const (
	seekService serviceKind = iota
	kvGetService
	catalogueService
	disconnectService
	serviceKinds
)

const queueResponseService = serviceKinds

const (
	requestRefused = iota
	requestDropped
	answerTooLarge
	requestAnswered
	requestResults
)

var serviceNames = [...]string{"seek", "kv_get", "catalogue", "sessions_disconnect"}
var requestResultNames = [...]string{"refused", "dropped", "answer_too_large", "answered"}

func requestService(kind connection.RequestKind) serviceKind {
	switch kind {
	case connection.QueueResponse:
		return queueResponseService
	case connection.KVGetRequest:
		return kvGetService
	case connection.CatalogueRequest:
		return catalogueService
	case connection.DisconnectRequest:
		return disconnectService
	default:
		return seekService
	}
}

// serviceRequest is the common connection-local completion of a request.
// Its QoS 2 identifier is transient, independently of any durable result.
type serviceRequest struct {
	publishIdentity
	kind        serviceKind
	qos         byte
	id          uint16
	quota       uint32
	topic       string
	correlation []byte
	response    *serviceAnswer // reserved after admission checks, transferred to delivery or released
	completed   bool           // acknowledgement readiness is independent of answer loading
}

// serviceBody describes an answer without loading it. Load runs only after
// both delivery capacity and encoded message memory have been reserved. Size
// is exact except for a catalogue's conservative bound. A
// stored answer can return a read; its result is applied at the final answer.
type serviceBody interface {
	Size() int
	Load(*[]byte) store.Read
}

// serviceAnswer is a connection's automatic exact-topic service subscription.
// It never changes explicit subscriptions or starts a retained pass. Only its
// bounded metadata waits for a fixed delivery descriptor; bodies use the same
// memory FIFO and writer hand-off as every other delivery.
type serviceAnswer struct {
	conn             *connectionState // owns one of the connection's answers until hand-off or discard
	body             serviceBody
	packet           connection.Delivery
	size             int64 // encoded packet and stored-copy reservation
	result, tooLarge *metrics.Counter
}

func (a *serviceAnswer) count(large bool) {
	if a.result == nil {
		return
	}
	if large {
		a.tooLarge.Inc()
	} else {
		a.result.Inc()
	}
	a.result, a.tooLarge = nil, nil
}

// reserveServiceAnswer bounds answer metadata independently of packet replies.
// Requests call it only after checking admission, before any action. A seek
// refusal may also reserve its explanatory answer, without changing its code
// if no allowance remains.
func (c *Core) reserveServiceAnswer(r *storeRequest) bool {
	s, q := r.conn, r.service
	if s.answerCount == connection.ReplySlots {
		return false
	}
	q.response = &serviceAnswer{conn: s}
	s.answerCount++
	return true
}

func (c *Core) admitServiceRequest(acts []Action, r *storeRequest, now time.Time) ([]Action, bool) {
	if c.reserveServiceAnswer(r) {
		return acts, true
	}
	return c.finishChannelRequest(acts, r, 0x97, now), false
}

// acknowledgeServiceRequest follows ordinary publication reply ownership.
// Checks and committed changes decide readiness; loading an answer never does.
func (c *Core) acknowledgeServiceRequest(acts []Action, r *storeRequest, reason byte) []Action {
	q := r.service
	if q.completed {
		return acts
	}
	var reply connection.Reply
	if q.qos != 0 {
		typ := packet.PUBACK
		if q.qos == 2 {
			typ = packet.PUBREC
		}
		code := reason
		if r.conn.version == packet.V311 {
			code = 0
		}
		reply = &connection.Ack{Type: typ, PacketID: q.id, ReasonCode: code, Quota: q.quota}
	}
	return c.finishServiceRequest(acts, r, reply, reason)
}

// finishServiceAnswer returns the connection's answer allowance at hand-off
// or discard. A payload read can retain its independent buffer ownership.
func (c *Core) finishServiceAnswer(a *serviceAnswer) {
	if a.conn != nil {
		a.conn.answerCount--
		a.conn = nil
	}
	a.body = nil
}

// finishServiceRequest completes the original packet and every repeat of its
// transient QoS 2 identifier together. A nil reply cancels the whole exchange.
func (c *Core) finishServiceRequest(acts []Action, r *storeRequest, reply connection.Reply, reason byte) []Action {
	s, q := r.conn, r.service
	if q.completed {
		return acts
	}
	q.completed = true
	if h := q.incoming; h != nil {
		h.pending = false
		h.reason = reason
		if reply == nil || (reason >= 0x80 && s.version == packet.V5) {
			c.forgetIncoming(s, q.id)
		}
		p := &publication{conn: s, packetID: q.id, publishIdentity: q.publishIdentity, pendingReply: r.replySlot}
		return c.answerIncoming(acts, p, reply, reason == 0)
	}
	c.completeAcknowledgement(s)
	return c.completeStoredReply(acts, s, r.replySlot, reply, reason == 0)
}

// cancelServiceRequest drops only this request's completion and identifiers.
// Cancellation is a discarded QoS 0 request; no refusal or packet-size failure
// occurred for other QoS levels, whose terminal result is otherwise answered.
func (c *Core) cancelServiceRequest(r *storeRequest) {
	result := requestAnswered
	if r.service.qos == 0 {
		result = requestDropped
	}
	if r.service.kind < serviceKinds {
		c.requestsTotal[r.service.kind][result].Inc()
	}
	c.finishServiceRequest(nil, r, nil, 0)
	if a := r.service.response; a != nil {
		c.finishServiceAnswer(a)
		r.service.response = nil
	}
}

// answerService completes every request through one direct-delivery path.
// Payload refusals are request results; policy refusals are counted by their
// admission path as well. A completed change is never undone by answer loss.
func (c *Core) answerService(acts []Action, r *storeRequest, body serviceBody, reason byte, properties []packet.Property, now time.Time) []Action {
	s, q := r.conn, r.service
	if q.kind == queueResponseService {
		return c.acknowledgeServiceRequest(acts, r, reason)
	}
	if q.qos == 0 && reason != 0 {
		c.requestsTotal[q.kind][requestDropped].Inc()
		if a := q.response; a != nil {
			c.finishServiceAnswer(a)
			q.response = nil
		}
		return c.acknowledgeServiceRequest(acts, r, reason)
	}
	if reason != 0 {
		c.requestsTotal[q.kind][requestRefused].Inc()
	}
	if body != nil && q.response == nil && !c.reserveServiceAnswer(r) {
		// A refused seek keeps its own reason. Without an allowance its
		// explanatory answer is discarded; MQTT 3.1.1 then cannot carry
		// that reason and follows the ordinary publish refusal below.
		body = nil
	}
	if reason != 0 && body == nil && q.qos != 0 && s.version == packet.V311 {
		// Payload refusals have an answer; a failed change follows PUBLISH's
		// MQTT 3.1.1 refusal rule, without confirming a change that failed.
		acts = c.finish(acts, s, reason, now)
	}
	acts = c.acknowledgeServiceRequest(acts, r, reason)
	a := q.response
	q.response = nil
	if a == nil {
		return acts // refusal without an answer; no allowance was taken
	}
	a.body = body
	if reason == 0 {
		a.result, a.tooLarge = c.requestsTotal[q.kind][requestAnswered], c.requestsTotal[q.kind][answerTooLarge]
	}
	if body == nil || s.phase != connected || s.end.seen || c.failure != nil || c.phase >= quiescing {
		a.count(false)
		c.finishServiceAnswer(a)
		return acts
	}
	a.packet = connection.Delivery{Channel: 1, Topic: []byte(q.topic)}
	if s.version == packet.V5 {
		if q.correlation != nil {
			properties = append(properties, packet.Property{ID: packet.CorrelationData, Binary: q.correlation})
		}
		var err error
		a.packet.Additions, err = packet.AppendProperties(nil, packet.PUBLISH, properties)
		if err != nil {
			c.finishServiceAnswer(a)
			return c.fail(acts, err, now)
		}
	}
	large, discard, err := c.sizeServiceAnswer(s, a)
	if err != nil {
		c.finishServiceAnswer(a)
		return c.fail(acts, err, now)
	}
	if discard {
		a.count(large)
		c.finishServiceAnswer(a)
		return c.logDeliveryDiscard(acts, s, q.topic, 0, now)
	}
	s.answers = append(s.answers, a)
	return c.admitServiceAnswers(acts, s)
}

// sizeServiceAnswer applies the same packet and memory limits initially and
// after a current-value load reports a larger replacement. Source bytes are
// charged only while the store reader may need them for decompression.
func (c *Core) sizeServiceAnswer(s *connectionState, a *serviceAnswer) (large, discard bool, err error) {
	bodySize := a.body.Size()
	_, bounded := a.body.(*catalogueBody)
	payloadSize := bodySize
	if bounded {
		// Only the envelope's size is known before marshalling. The body
		// bound must not decide whether the actual packet fits the peer.
		payloadSize = 0
	}
	size, large, err := servicePacketSize(s, &a.packet, payloadSize)
	if err != nil || large {
		return large, large, err
	}
	a.size = size
	if bounded {
		// Remaining Length can grow from one byte to at most four as the
		// bounded body is added. This reservation also covers a body whose
		// bound exceeds MQTT's encodable size; its exact check follows Load.
		a.size += int64(bodySize) + 3
	}
	if value, ok := a.body.(*valueBody); ok {
		a.size += int64(value.StoredSize())
		var alias uint16
		if s.topicAliases != 0 {
			alias = 1
		}
		p, err := a.packet.Publish(a.packet.Topic, alias)
		if err != nil {
			return false, false, err
		}
		value.value.Publication, value.value.Version = &p, s.version
		value.value.Budget = a.size
	}
	return false, a.size > int64(c.limits.MaxOutgoingMemory), nil
}

// servicePacketSize checks a known payload length against Maximum Packet Size
// and returns the reservation covering every writer alias choice.
func servicePacketSize(s *connectionState, p *connection.Delivery, payloadSize int) (reservation int64, large bool, err error) {
	size, err := minimumDeliverySize(s, p, payloadSize)
	if err != nil && !errors.Is(err, packet.ErrCannotEncode) {
		return 0, false, err
	}
	if errors.Is(err, packet.ErrCannotEncode) || (s.maximumPacket != 0 && int64(size) > int64(s.maximumPacket)) {
		return 0, true, nil
	}
	if s.topicAliases != 0 {
		// The exact unaliased size also bounds memory when aliases are
		// disabled. Otherwise reserve for the writer's largest encoding.
		var encodable bool
		size, encodable, err = deliverySize(s, p, payloadSize)
		if err != nil {
			return 0, false, err
		}
		if !encodable {
			return 0, true, nil
		}
	}
	return int64(size), false, nil
}

func (c *Core) admitServiceAnswers(acts []Action, s *connectionState) []Action {
	if len(s.answers) != 0 && s.service == nil {
		c.initWindow(s)
		s.service = c.newConsumer(s, &c.broadcast, 0)
		s.service.service = true
	}
	for len(s.answers) != 0 {
		a := s.answers[0]
		if s.phase != connected || s.end.seen || c.failure != nil || c.phase >= quiescing {
			s.answers = slices.Delete(s.answers, 0, 1)
			a.count(false)
			c.finishServiceAnswer(a)
			continue
		}
		if c.preparationRoom(s.service) <= 0 {
			break
		}
		s.answers = slices.Delete(s.answers, 0, 1)
		d := c.selectDelivery(s.service, &a.packet, 0, a.size, true)
		d.answer = a
		if value, ok := a.body.(*valueBody); ok {
			d.storedBytes = int64(value.StoredSize())
		}
		c.queueConsumer(s.service)
	}
	c.queueConsumer(s.service)
	return acts
}

// serviceRoom reserves the writer's delivery queue without borrowing any
// QoS send quota. Ordinary payload reservations share that same queue bound.
func (c *Core) serviceRoom(s *connectionState) bool {
	return s.writingPublishes+s.payloadSlots+s.serviceSlots < c.deliveryWindow(s)
}

func (c *Core) loadServiceAnswers(acts []Action, u *consumerState, now time.Time) []Action {
	for d := u.load; d != nil && d.stage == deliveryReserved; d = u.load {
		u.load = d.next
		u.reserved--
		body := d.answer.body
		op := body.Load(&d.payloadBody)
		if _, current := body.(*valueBody); !current {
			d.answer.body = nil
		}
		if op != nil {
			r := &u.read
			r.kind, r.op = readDeliveryPayload, op
			r.payloads.members = append(r.payloads.members, d)
			d.stage = deliveryLoading
			c.joinConsumerRead(r, u)
			c.enqueue(r)
			break
		}
		d.applyPayload()
		d.stage = deliveryLoaded
		if _, bounded := body.(*catalogueBody); bounded {
			var err error
			acts, err = c.trimServiceAnswer(acts, d, now)
			if err != nil {
				return c.fail(acts, err, now)
			}
		}
	}
	return c.offerLoadedDeliveries(acts, u)
}

// loadedServiceValue settles the newest fixed view, or releases the entire
// old grant before the larger size joins the ordinary outgoing FIFO again.
// The descriptor and writer-queue slot remain this answer's throughout.
func (c *Core) loadedServiceValue(acts []Action, d *outgoing, now time.Time) ([]Action, bool) {
	a, u := d.answer, d.consumer
	b := a.body.(*valueBody)
	if b.value.Retry {
		large, discard, err := c.sizeServiceAnswer(u.conn, a)
		if err != nil {
			return c.fail(acts, err, now), false
		}
		if !discard {
			c.returnDeliveryBytes(d, now)
			d.bytes, d.size, d.stage = 0, a.size, deliveryWaiting
			u.load = d
			d.storedBytes = int64(b.StoredSize())
			c.queueMemory(&d.memory, d.size)
			return acts, true
		}
		a.count(large)
		acts = c.logDeliveryDiscard(acts, u.conn, string(a.packet.Topic), 0, now)
		d.removed = true
	} else {
		var err error
		acts, err = c.trimServiceAnswer(acts, d, now)
		if err != nil {
			return c.fail(acts, err, now), false
		}
	}
	a.body = nil
	return acts, false
}

// trimServiceAnswer checks the real body after loading and releases any unused
// reservation. A catalogue was bounded; a current value may have shrunk or
// lost its compressed source. The same exact check precedes either hand-off.
func (c *Core) trimServiceAnswer(acts []Action, d *outgoing, now time.Time) ([]Action, error) {
	s := d.consumer.conn
	size, large, err := servicePacketSize(s, &d.packet, len(d.packet.Payload))
	if err != nil {
		return acts, err
	}
	if large {
		d.answer.count(true)
		d.removed = true
		return c.logDeliveryDiscard(acts, s, string(d.packet.Topic), 0, now), nil
	}
	back := d.bytes - size
	if c.admission.delay != 0 {
		// The loaded packet replaces the original peer-held share. Its
		// decoded body can be larger even when the whole grant suffices,
		// because the replacement needed a smaller compressed source.
		c.memoryPeer += size - (d.bytes - d.storedBytes)
	}
	c.returnMemory(1, back, now)
	d.bytes, d.size, d.storedBytes = size, size, 0
	return acts, nil
}
