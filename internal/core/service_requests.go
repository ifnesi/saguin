package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/store"
)

// startServiceRequest shares the seek's packet ordering and transient QoS 2
// ownership. Only seeks have a fallback reply topic or change subscriptions.
func (c *Core) startServiceRequest(acts []Action, r *storeRequest, now time.Time) ([]Action, bool) {
	s, q := r.conn, r.request
	p := q.publish
	subject := string(p.Payload)
	if q.kind == catalogueService {
		subject = strings.TrimPrefix(string(p.Topic), "$saguin/catalogue/")
	}
	c.returnBytes(q.bytes, now)
	q.bytes, q.publish = 0, nil
	if q.reserved {
		c.retained.reservedProperty.Inc()
		return c.finishChannelRequest(acts, r, 0x83, now), true
	}
	if q.topic == "" {
		c.noResponseTopic.Inc()
		return c.finishChannelRequest(acts, r, 0x83, now), true
	}
	if (q.kind == kvGetService && subject == "") ||
		(q.kind == disconnectService && (subject == "" || subject == s.clientID)) {
		return c.finishChannelRequest(acts, r, 0x83, now), true
	}
	if q.qos == 0 && (q.kind == kvGetService || q.kind == catalogueService) {
		c.requestsTotal[q.kind][requestDropped].Inc()
		r.request = nil
		c.releaseSharedRequest(r)
		return c.finishServiceRequest(acts, r, nil, 0), true
	}
	if len(q.correlation) == 0 && (q.kind == kvGetService || q.kind == catalogueService) {
		q.correlation = []byte(subject)
	}
	var admitted bool
	acts, admitted = c.admitServiceRequest(acts, r, now)
	if !admitted {
		return acts, true
	}
	switch q.kind {
	case kvGetService:
		acts = c.acknowledgeServiceRequest(acts, r, 0)
		r.kind, r.op = readKVSize, &store.ReadCurrentValue{Channel: q.channel.row.ID, Topic: subject}
		c.continueRequest(r)
		return acts, false
	case catalogueService:
		return c.finishServiceRequestBody(acts, r, c.catalogue(s.access, subject), now), true
	case disconnectService:
		q.answer = "no-such-client"
		if target := c.sessions[subject]; target != nil && target.phase == connected && !target.end.seen {
			acts = c.finish(acts, target, 0x98, now)
			if c.logCredits.Admit(logger.Info) {
				acts = append(acts, Action{Line: logger.Line{At: now, Level: logger.Info,
					Msg: fmt.Sprintf("operator client %q user %q disconnected client %q", s.clientID, s.accessUser, target.clientID)}})
			}
			q.answer = "hung-up"
		}
		return c.finishChannelRequest(acts, r, 0, now), true
	default:
		panic("core: unknown service request")
	}
}

func (c *Core) finishServiceRequestBody(acts []Action, r *storeRequest, body serviceBody, now time.Time) []Action {
	c.releaseSharedRequest(r)
	r.request = nil
	return c.answerService(acts, r, body, 0, nil, now)
}

func (c *Core) readKVSize(acts []Action, r *storeRequest, now time.Time) []Action {
	if r.request == nil {
		return acts
	}
	if r.conn.end.seen || r.conn.phase != connected || c.failure != nil || c.phase >= quiescing {
		c.cancelChannelRequest(r, now)
		return acts
	}
	value := r.op.(*store.ReadCurrentValue)
	return c.finishServiceRequestBody(acts, r, &valueBody{value: *value}, now)
}

// valueBody retains scalar size metadata only. Each load reselects the current
// value in a new ordered view, bounded by the granted payload/source budget.
type valueBody struct{ value store.ReadCurrentValue }

func (b *valueBody) Size() int       { return b.value.Size }
func (b *valueBody) StoredSize() int { return b.value.StoredSize }
func (b *valueBody) Load(into *[]byte) store.Read {
	b.value.Into = into
	return &b.value
}

type catalogueBody struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Filter string   `json:"filter"`
	Verbs  []string `json:"verbs"`
	Pin    string   `json:"pin,omitempty"`
}

// catalogue uses the compiled ACL evaluation, including narrowed and withheld
// grants. Channel verbs describe capabilities; record operations still check
// their own topic. The configured filter's spelling is kept intact.
func (c *Core) catalogue(access *acl.Client, name string) serviceBody {
	i, ok := c.routing.Named(name)
	if !ok {
		return emptyServiceBody{}
	}
	ch := &c.routing.Channels[i]
	b := &catalogueBody{Name: name, Filter: ch.Filter}
	verbs := []acl.Verb{acl.Write, acl.Read}
	switch ch.Type {
	case config.Append:
		b.Type = "append"
		verbs = append(verbs, acl.Seek)
	case config.Latest:
		b.Type = "latest"
	case config.Queue:
		b.Type, b.Pin = "queue", "$saguin/queue/"+name
		verbs = append(verbs, acl.Seek, acl.Consume)
	}
	for _, verb := range verbs {
		if (verb != acl.Write || !strings.HasSuffix(name, "__dlq")) && access.AllowsChannel(verb, name) {
			b.Verbs = append(b.Verbs, verb.String())
		}
	}
	if len(b.Verbs) == 0 {
		return emptyServiceBody{}
	}
	return b
}

type emptyServiceBody struct{}

func (emptyServiceBody) Size() int                    { return 0 }
func (emptyServiceBody) Load(into *[]byte) store.Read { *into = nil; return nil }

// Size bounds the body without encoding it. Each input byte can require at
// most six JSON bytes; encoding/json alone decides the actual escaping.
func (b *catalogueBody) Size() int {
	n := len(`{"name":,"type":,"filter":,"verbs":[]}`) +
		jsonStringSize(b.Name) + jsonStringSize(b.Type) + jsonStringSize(b.Filter)
	for i, verb := range b.Verbs {
		if i != 0 {
			n++ // comma between verbs
		}
		n += jsonStringSize(verb)
	}
	if b.Pin != "" {
		n += len(`,"pin":`) + jsonStringSize(b.Pin)
	}
	return n
}

// jsonStringSize bounds any JSON string by its quotes and at most six bytes
// per input byte. It neither examines characters nor implements escaping.
func jsonStringSize(s string) int { return 2 + 6*len(s) }

func (b *catalogueBody) Load(into *[]byte) store.Read {
	// The delivery descriptor, queue space and bounded encoding memory are
	// already reserved. This acyclic struct holds only strings and []string,
	// which encoding/json can always marshal.
	*into, _ = json.Marshal(b)
	return nil
}
