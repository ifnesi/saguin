package connection

import (
	"bytes"
	"errors"
	"math"
	"net"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/topic"
)

// TopicAliasMaximum is the connection-local inbound table advertised by
// CONNACK. Aliases never form part of the message handed to the core.
const TopicAliasMaximum = 16

// publishChecks is confined to one decode. Broker refusals do not stop the
// decoder: the complete packet must still be checked for malformed input.
type publishChecks struct {
	server           bool // PUBLISH received from a bridge peer
	alias            uint32
	aliasOffset      int
	headers          int
	maximumHeaders   int
	notAuthorized    bool
	topicInvalid     bool
	payloadInvalid   bool // request payload refusal, without a publication counter
	reservedProperty bool
	message          PublishMetadata
	responseTopic    []byte
	correlation      []byte
	id               []byte // borrowed until the complete PUBLISH is accepted
}

// refuse selects policy in the design's order after packet validity is known.
func (p *publishChecks) refuse() (reason byte, cause string) {
	switch {
	case p.notAuthorized:
		return 0x87, "not_authorized"
	case p.topicInvalid:
		return 0x90, "topic_invalid"
	case p.payloadInvalid:
		return 0x90, ""
	case p.headers > p.maximumHeaders:
		return 0x97, "headers"
	default:
		return 0, ""
	}
}

func (p *publishChecks) property(v packet.PropertyView) error {
	switch v.ID {
	case packet.MessageExpiryInterval:
		p.message.Expiry, p.message.Expires = v.Int, true
	case packet.TopicAlias:
		if p.server || v.Int > TopicAliasMaximum {
			return packet.ErrTopicAlias
		}
		p.alias, p.aliasOffset = v.Int, v.Offset
	case packet.SubscriptionIdentifier:
		if !p.server {
			return packet.ErrProtocol // a client cannot supply one (MQTT-3.3.4-6)
		}
	case packet.CorrelationData:
		p.correlation = v.Value
	case packet.ResponseTopic:
		p.responseTopic = v.Value
		limits := topic.Limits{MaxLength: math.MaxUint16, MaxLevels: math.MaxUint16 + 1}
		if topic.CheckName(string(v.Value), limits) != nil {
			return packet.ErrMalformed
		}
	case packet.UserProperty:
		p.headers++
		switch {
		case bytes.Equal(v.Name, []byte("saguin-id")):
			if p.id == nil {
				p.id = v.Value
			}
		case packet.IsSaguinProperty(v.Name):
			p.reservedProperty = true
		}
	}
	return nil
}

func (r *reader) publish(body []byte, flags byte, received time.Time) (Event, error) {
	checks := publishChecks{maximumHeaders: r.limits.MaxHeaderCount, server: r.conn.client,
		message: PublishMetadata{Received: received.UnixMilli()}}
	p, err := packet.DecodePublishProperties(body, flags, r.version, checks.property)
	if err != nil {
		return Event{}, err
	}
	r.metrics.publishes[p.QoS].Inc()
	r.refreshAccess()
	// Charge at receipt, including policy refusals and held QoS 2 repeats.
	// The reader waits only after handing this packet to the core.
	if !r.conn.client {
		r.rate.charge(r, len(body), received)
	}
	if checks.alias != 0 && len(p.Topic) == 0 {
		p.Topic = r.aliases[checks.alias-1]
		if len(p.Topic) == 0 {
			return Event{}, packet.ErrProtocol
		}
	}
	limits := topic.Limits{MaxLength: r.limits.MaxTopicLength, MaxLevels: r.limits.MaxTopicLevels}
	name := string(p.Topic)
	err = topic.CheckName(name, limits)
	if errors.Is(err, topic.ErrMalformed) {
		return Event{}, packet.ErrMalformed
	}
	unmatched := ""
	bridgeName := ""
	var topicBytes int64
	if r.conn.client {
		var rule *config.BridgeRule
		bridgeName, rule, err = r.bridgeTopic(name, limits)
		if rule == nil {
			unmatched = string(p.Topic)
		} else {
			if rule.Direction != config.Both {
				name = bridgeName
			}
			if rule.BuildsTopic() {
				topicBytes = int64(len(bridgeName))
			}
		}
	}
	index, request := r.routing.SeekChannel(name, false)
	kind := SeekRequest
	if !request {
		index, request = r.routing.QueueResponseChannel(name)
		kind = QueueResponse
	}
	if !request {
		switch {
		case name == "$saguin/kv/get":
			kind, request, index = KVGetRequest, true, -1
		case strings.HasPrefix(name, "$saguin/catalogue/"):
			channel := strings.TrimPrefix(name, "$saguin/catalogue/")
			if channel != "" && !strings.Contains(channel, "/") {
				kind, request, index = CatalogueRequest, true, -1
				if i, found := r.routing.Named(channel); found {
					index = i
				}
			}
		case name == "$saguin/sessions/disconnect":
			kind, request, index = DisconnectRequest, true, -1
		}
	}
	if r.conn.client {
		request = false // bridge templates cannot address broker service topics
	}
	reserved := strings.HasPrefix(name, "$saguin/")
	checks.topicInvalid = err != nil || unmatched != "" || (topic.HasReservedPrefix(name) && !request)
	if p.Retain && !r.conn.client && !r.connackWritten && !r.waitConnack() {
		return Event{}, net.ErrClosed
	}
	if p.Retain && r.negotiatedDenial(acl.Retained) {
		return Event{Kind: Ended, Conn: r.conn, Reason: 0x9A}, nil
	}
	switch {
	case (p.Retain && r.access.Denied(acl.Retained)) || (p.QoS == 2 && r.access.Denied(acl.QoS2)):
		checks.notAuthorized = true
	case request && kind == KVGetRequest:
		asked := string(p.Payload)
		if asked != "" {
			invalid := topic.CheckName(asked, limits) != nil
			channel := ""
			if !invalid && r.routing != nil {
				index = r.routing.Resolve(asked)
				if index >= 0 {
					channel = r.routing.Channels[index].Name
				}
			}
			checks.notAuthorized = !invalid && !r.access.Allows(acl.Read, channel, asked)
			checks.payloadInvalid = invalid || strings.HasPrefix(asked, "$") || index < 0 || r.routing.Channels[index].Type != config.Latest
		}
	case request && kind == CatalogueRequest:
		// The catalogue hides unknown and inaccessible channels with an empty answer.
	case request && kind == DisconnectRequest:
		checks.notAuthorized = !r.access.Allows(acl.Disconnect, "", "")
	case request:
		verb := acl.Seek
		if kind == QueueResponse {
			verb = acl.Consume
		}
		checks.notAuthorized = !r.access.AllowsChannel(verb, r.routing.Channels[index].Name)
	case reserved:
		// Unknown requests are topic errors, before consulting the ACL file.
	default:
		channel := ""
		if r.routing != nil {
			if i := r.routing.Resolve(name); i >= 0 {
				channel = r.routing.Channels[i].Name
			}
		}
		checks.notAuthorized = strings.HasPrefix(name, "$SYS/") || strings.HasSuffix(channel, "__dlq") ||
			(r.access != nil && !r.access.Allows(acl.Write, channel, name))
	}

	if checks.alias != 0 {
		if len(r.aliases[checks.alias-1]) == 0 || !bytes.Equal(r.aliases[checks.alias-1], p.Topic) {
			// Clone only on a new mapping. A previous message may still use
			// the old name; neither it nor a handed body is ever overwritten.
			r.aliases[checks.alias-1] = bytes.Clone(p.Topic)
		}
	}
	quota, admitted, err := r.admitPublish(p.PacketID, p.QoS)
	if err != nil {
		return Event{}, err
	}
	if !admitted {
		return Event{Kind: Ended, Conn: r.conn, Reason: 0x93}, nil
	}
	ev := Event{Kind: Received, Conn: r.conn, Bytes: int64(len(body)) + topicBytes}
	if reason, cause := checks.refuse(); reason != 0 {
		refusal := PublishRefusal{Kind: kind, QoS: p.QoS, PacketID: p.PacketID, Request: request, UnmatchedTopic: unmatched}
		if cause != "" && p.QoS != 0 {
			// The core may still be loading this session. Only it knows
			// whether this is a held repeat, which receives PUBREC again.
			refusal.Counter = r.metrics.publishRefused[cause]
		} else if cause != "" {
			r.metrics.publishRefused[cause].Inc()
		}
		ev.Kind, ev.Reason, ev.Packet = Refused, reason, refusal
	} else {
		ev.Publish = checks.message
		var service *RequestPublish
		if request {
			service = &RequestPublish{Kind: kind, Channel: index, ResponseTopic: string(checks.responseTopic), Correlation: bytes.Clone(checks.correlation)}
		}
		ev.Publish.Reserved = checks.reservedProperty
		if checks.id != nil {
			// Preserve the borrowed value before removing Topic Alias:
			// compacting the properties can overwrite its original span.
			ev.Publish.ID, ev.Publish.HasID = string(checks.id), true
		}
		if checks.alias != 0 {
			// Topic Alias is exactly three bytes. Its recorded offset
			// avoids another traversal or allocation; only this reader
			// still owns the packet's body (R3).
			start := len(body) - len(p.Payload) - len(p.Properties)
			i := checks.aliasOffset
			p.Properties = append(p.Properties[:i], p.Properties[i+3:]...)
			if len(p.Properties) != 0 {
				// Place the rewritten length immediately before the kept
				// bytes. A shorter prefix uses the old prefix's suffix,
				// without moving the properties a second time.
				var length [4]byte
				prefix, _ := packet.AppendVarInt(length[:0], uint32(len(p.Properties)))
				copy(body[start-len(prefix):start], prefix)
				p.StoredProperties = body[start-len(prefix) : start+len(p.Properties)]
			}
		} else if len(p.Properties) != 0 {
			start := 2 + len(p.Topic)
			if p.QoS != 0 {
				start += 2 // Packet Identifier
			}
			p.StoredProperties = body[start : len(body)-len(p.Payload)]
		}
		// Only an accepted message needs a packet object beyond this call.
		if service != nil {
			service.Publish = p
			ev.Packet = service
		} else {
			if encoded := payload.Compress(p.Payload); encoded != nil {
				p.PayloadSize = len(p.Payload)
				if checks.alias == 0 {
					p.Topic = bytes.Clone(p.Topic)
				} else {
					p.Topic = r.aliases[checks.alias-1]
				}
				p.StoredProperties = bytes.Clone(p.StoredProperties)
				p.Properties, p.Payload = nil, nil
				p.StoredPayload = payload.Compact(encoded)
				ev.Bytes += int64(len(p.StoredPayload) - p.PayloadSize)
			}
			if r.conn.client && bridgeName != "" {
				ev.Packet = &RoutedPublish{Publish: p, Topic: bridgeName}
			} else {
				message := new(packet.Publish)
				*message = p
				ev.Packet = message
			}
		}
	}
	ev.Quota = quota
	return ev, nil
}

// publishBuckets holds one second's allowance, initialized at CONNECT.
// Every received publication spends its allowance. A message larger than
// the byte capacity leaves debt to repay before the next packet is read.
type publishBuckets struct {
	at       time.Time
	messages float64
	bytes    float64
}

func (b *publishBuckets) charge(r *reader, size int, received time.Time) {
	rate, capacity := float64(r.limits.PublishRate), float64(r.limits.PublishBytes)
	if (rate == 0 && capacity == 0) && (r.live == nil || (r.publishRate == 0 && r.publishBytes == 0)) {
		return
	}
	b.refill(rate, capacity, received)
	// Refill at the old rate through this publish. Only a previously
	// unlimited unit starts full; existing buckets clamp without new credit.
	if r.live != nil {
		if rate == 0 {
			b.messages = float64(r.publishRate)
		}
		if capacity == 0 {
			b.bytes = float64(r.publishBytes)
		}
		rate, capacity = float64(r.publishRate), float64(r.publishBytes)
		b.messages, b.bytes = math.Min(rate, b.messages), math.Min(capacity, b.bytes)
		r.limits.PublishRate, r.limits.PublishBytes = r.publishRate, r.publishBytes
	}
	if rate != 0 {
		b.messages--
	}
	if capacity != 0 {
		b.bytes -= float64(size)
	}
}

func (b *publishBuckets) refill(rate, capacity float64, at time.Time) {
	elapsed := max(0, at.Sub(b.at).Seconds())
	b.at = at
	b.messages = math.Min(rate, b.messages+elapsed*rate)
	b.bytes = math.Min(capacity, b.bytes+elapsed*capacity)
}

// delay rounds up to a whole nanosecond with one message available and
// strictly positive byte credit. A disabled limit imposes no wait.
func (b *publishBuckets) delay(limits *config.Limits) time.Duration {
	var delay time.Duration
	if rate := float64(limits.PublishRate); rate != 0 && b.messages < 1 {
		delay = time.Duration(math.Ceil((1 - b.messages) / rate * float64(time.Second)))
	}
	if capacity := float64(limits.PublishBytes); capacity != 0 && b.bytes <= 0 {
		delay = max(delay, time.Duration(-b.bytes/capacity*float64(time.Second))+1)
	}
	return delay
}

// waitPublish holds all subsequent input while allowance refills. It runs
// before the next packet's keep-alive deadline starts, just like takeSlot.
// The first actual wait creates a timer, reused and stopped by this reader.
func (r *reader) waitPublish() bool {
	if !r.connected || r.conn.client || r.rate.delay(&r.limits) == 0 {
		return true
	}
	started := r.conn.clock.Now()
	rate, capacity := float64(r.limits.PublishRate), float64(r.limits.PublishBytes)
	r.rate.refill(rate, capacity, started)
	delay := r.rate.delay(&r.limits)
	if delay == 0 {
		return true // decoding or handover already allowed enough refill
	}
	if r.rateTimer == nil {
		r.rateTimer, r.resetRateTimer = r.conn.clock.Timer(started.Add(delay))
	} else {
		r.resetRateTimer(started.Add(delay))
	}
	defer func() {
		r.resetRateTimer(time.Time{})
		r.metrics.publishThrottled.Add(r.conn.clock.Now().Sub(started))
	}()
	for {
		select {
		case <-r.conn.stopping:
			return false
		case <-r.rateTimer:
		}
		if r.stopRequested() {
			return false
		}
		now := r.conn.clock.Now()
		r.rate.refill(rate, capacity, now)
		delay = r.rate.delay(&r.limits)
		if delay == 0 {
			return true
		}
		r.resetRateTimer(now.Add(delay))
	}
}
