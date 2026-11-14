package main

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/ifnesi/saguin/internal/harness"
)

// The fake broker's seeks (design.md, $saguin topics): it answers a seek as
// the design has the broker answer one, moving no position, as the
// runner's model allows a seek's replay and owes none (seeks.go). Any other
// publish under $saguin/ takes the PUBLISH table's path (publishRefusal).

// uncorrelated sends each answer without its request's Correlation Data.
const uncorrelated fault = "answer without its Correlation Data"

// fakeSeekChannel is the index, in the runner's configuration, of the
// channel topic seeks, if it is a seek: $saguin/consumer/<channel>/seek of
// a configured channel, all of them append channels.
func fakeSeekChannel(topic string) (int, bool) {
	rest, consumer := strings.CutPrefix(topic, "$saguin/consumer/")
	channel, seek := strings.CutSuffix(rest, "/seek")
	ch := slices.IndexFunc(fakeConfigured, func(o harness.Channel) bool { return o.Name == channel })
	return ch, consumer && seek && ch >= 0
}

// request answers p, a seek on topic of the channel of index ch, from c,
// the connection holding id's session, and reports whether the connection
// goes on. Flows, PUBLISH: "0x87 | the acl_file refuses the topic, $saguin
// requests included" (the restricted user holds no seek, model.go); "0x90
// | a topic over limits.max_topic_length". It runs under the lock (owns).
func (b *fake) request(c *fakeConn, id, topic string, ch int, p *harness.Publish) bool {
	channel := fakeConfigured[ch].Name
	switch {
	case b.fault != ignoredACL && c.user == restricted:
		b.seen.unauthorized++
		return b.acknowledgeRequest(c, p, 0x87)
	case len(topic) > maxTopicLength:
		return b.acknowledgeRequest(c, p, 0x90)
	}
	answer, refused, props := b.seek(b.sessions[id], int64(ch+2), string(p.Payload))
	if refused && p.QoS == 0 {
		return true // "Taken at QoS 0 too, a refused one dropped"
	}
	var reason byte
	if refused && c.version == harness.V5 {
		reason = 0x83 // "each also 0x83 on the PUBACK (MQTT 3.1.1: an ordinary PUBACK [...])"
	}
	if !b.acknowledgeRequest(c, p, reason) {
		return false
	}
	// "The answer goes only to the asking connection, on its Response
	// Topic with its Correlation Data"; "[the] reply topic, when no
	// Response Topic was set". "MQTT 3.1.1 clients receive none of these
	// properties".
	out := &harness.Publish{Topic: replyTopic(channel), Payload: []byte(answer)}
	if c.version == harness.V5 {
		if rt, ok := p.Props.Get(harness.ResponseTopic); ok {
			out.Topic = rt.Str
		}
		if cd, ok := p.Props.Get(harness.CorrelationData); ok && b.fault != uncorrelated {
			props = append(props, cd)
		}
		out.Props = props
	}
	b.answer(c, out)
	return true
}

// answer sends out, a request's answer, on c: "An answer exceeding the
// client's Maximum Packet Size is discarded". It runs under the lock.
func (b *fake) answer(c *fakeConn, out *harness.Publish) {
	wire, err := harness.Encode(out, c.version)
	if err != nil || (c.maxPacket > 0 && len(wire) > c.maxPacket) {
		return
	}
	b.seen.answered++
	c.queue(wire, false)
}

// acknowledgeRequest answers p, a request, with reason: nothing at QoS 0;
// at QoS 1 a PUBACK, at QoS 2 a PUBREC whose identifier, not refused, is
// held on c until its PUBREL ("The ordinary PUBREL and PUBCOMP follow");
// a refusal by code ends an MQTT 3.1.1 connection ("At QoS 1 or 2 an MQTT
// 3.1.1 client has no refusal code and is disconnected"). It reports
// whether the connection goes on.
func (b *fake) acknowledgeRequest(c *fakeConn, p *harness.Publish, reason byte) bool {
	switch {
	case p.QoS == 0:
	case reason >= 0x80 && c.version != harness.V5:
		return false
	case p.QoS == 2:
		c.send(&harness.Ack{Kind: harness.PUBREC, ID: p.ID, Reason: reason})
		if reason < 0x80 {
			if c.requests == nil {
				c.requests = map[uint16]bool{}
			}
			c.requests[p.ID] = true
		}
	default:
		c.send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID, Reason: reason})
	}
	return true
}

// seek is the answer to a seek of payload by s on the channel of id, and
// whether it refuses the seek, with the User Properties it carries in MQTT
// 5 (design.md, $saguin topics): "Surrounding Unicode whitespace is
// trimmed. A decimal integer in the signed 64-bit range denotes 0 for the
// floor, -1 for the tail, or a positive offset. [...] Values below -1 are
// malformed. A duration means ago, with or without a leading minus [...].
// The other time form is RFC 3339. Overflow or other syntax answers
// malformed"; "A time selects the lowest surviving offset whose timestamp
// is at or after it, or the tail"; "Explicit offsets outside floor through
// tail are refused, never clamped". The fake's offsets are counted across
// channels, so a channel's tail is the next of them all.
func (b *fake) seek(s *fakeSession, id int64, payload string) (answer string, refused bool, props harness.Properties) {
	if !s.durable {
		return "no-session", true, nil
	}
	var on []*fakeRecord
	for _, r := range b.records {
		if r.channel == id {
			on = append(on, r)
		}
	}
	tail := b.offset + 1
	floor := tail
	if len(on) > 0 {
		floor = on[0].offset
	}
	offset := func(v int64) (string, bool, harness.Properties) { return strconv.FormatInt(v, 10), false, nil }
	text := strings.TrimFunc(payload, unicode.IsSpace)
	v, err := strconv.ParseInt(text, 10, 64)
	switch {
	case errors.Is(err, strconv.ErrRange) || (err == nil && v < -1):
		return "malformed", true, nil
	case err == nil && v == 0:
		return offset(floor)
	case err == nil && v == -1:
		return offset(tail)
	case err == nil && v < floor:
		return "below-floor", true, harness.Properties{harness.User("saguin-floor", strconv.FormatInt(floor, 10))}
	case err == nil && v > tail:
		return "beyond-end", true, harness.Properties{harness.User("saguin-next", strconv.FormatInt(tail, 10))}
	case err == nil:
		return offset(v)
	}
	at, ok := fakeTime(text)
	if !ok {
		return "malformed", true, nil
	}
	if i := slices.IndexFunc(on, func(r *fakeRecord) bool { return r.ms >= at.UnixMilli() }); i >= 0 {
		return offset(on[i].offset)
	}
	return offset(tail)
}

// fakeTime reads text as a duration ago, of Go's units or of days, at most
// 106,751 days, a leading minus allowed, or as an RFC 3339 time (design.md,
// $saguin topics).
func fakeTime(text string) (time.Time, bool) {
	const most = 106751 * 24 * time.Hour
	ago := strings.TrimPrefix(text, "-")
	if days, ok := strings.CutSuffix(ago, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || n < 0 || n > 106751 || strings.ContainsAny(days, "+-eEnN") {
			return time.Time{}, false
		}
		return time.Now().Add(-time.Duration(n * float64(24*time.Hour))), true
	}
	if d, err := time.ParseDuration(ago); err == nil && d >= 0 && d <= most && !strings.HasPrefix(ago, "+") && !strings.HasPrefix(ago, "-") {
		return time.Now().Add(-d), true
	}
	at, err := time.Parse(time.RFC3339Nano, text)
	return at, err == nil
}
