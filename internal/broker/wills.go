package broker

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// A Will: the announcement a client arms at CONNECT and never sends
// itself, and the moments at which this broker owes it.
//
// **A Will belongs to its session.** That is the rule the rest of this file
// follows from, settled with the storage design: a Will is armed on the
// session record, cancelled when its client comes back, published when the
// session ends, and discarded with the record when nothing is owed. It
// cannot outlast the session it was armed on, which is what a Will held in
// memory beside the session could always do.
//
// # The moments, and they are one rule seen at each
//
//   - **CONNECT.** A Will that could never be delivered is refused here,
//     where the device is still there to be told (RFC 0003 "Last Will").
//     What cannot be known until it fires - a full channel, a storage
//     failure - is not judged here, because by then there is nobody to
//     tell. The publish properties the substrate's own Will struct has
//     nowhere for are kept at this moment too, keyed by connection rather
//     than by client id, so a takeover cannot fire one connection's Will
//     with another's properties.
//   - **A reconnect under the same client id.** A Will waiting out its
//     delay is cancelled, in both places it lives: the timer here and the
//     Will on the session record. That is MQTT-3.1.3-9 - the client did not
//     die after all, which is the whole point of a Will Delay Interval.
//   - **The disconnect.** Published at once, or held for its delay. A delay
//     is only honoured for a session that outlives its connection, because
//     a session that ends at the disconnect has no absence for the delay to
//     wait inside.
//   - **The session ending before the delay does.** The Will is due at that
//     moment rather than dropped: the session ending is the other half of
//     "the Will belongs to the session", and it also closes the race
//     between an expiry and a delay falling due together.
//   - **A start.** Three cases, decided from the record: a delay that
//     passed while the broker was stopped is owed and published once the
//     channels are loaded; one still ahead resumes waiting out what is
//     left; one with no moment on it belongs to a client that was still
//     connected when this broker stopped, and is discarded with the record,
//     because a broker stopping is not a device dying.
//
// **One road out, and there is deliberately only one.** Every moment above
// ends at publishWill, which puts the Will through the ordinary publish
// path as saguin rather than as the device, and every caller empties the
// Will it returns to the substrate: the substrate's own path publishes
// straight to subscribers, past OnPublish and past every rule this broker
// has, so a Will left with a topic on it would take a second road that
// nothing guards.

// codeWillNotKept is the answer to a CONNECT whose Will the session store
// could not keep: `0x97 Quota exceeded`, with a Reason String naming what
// filled. It is the fourth meaning of that code on this broker and it is a
// value of its own for the reason codeTooManyInflight is: an operator seeing
// `quota exceeded` on a refused connection cannot otherwise tell a full
// session store from a client over its publish rate, and the two have
// nothing to do with each other.
var codeWillNotKept = packets.Code{
	Code: packets.ErrQuotaExceeded.Code,
	Reason: "the provider broker.session.storage names has no room for this " +
		"client's Will, and a Will this broker does not hold is one it could " +
		"never publish",
}

// codeWillNotWritten is the answer to a CONNECT whose Will the session store
// failed to write for a reason other than room: `0x83 Implementation specific
// error`. Answered 0x97, as it was, a disk error read as a full store and
// sent an operator looking for space that was there.
var codeWillNotWritten = packets.Code{
	Code: packets.ErrImplementationSpecificError.Code,
	Reason: "the session store could not write this client's Will, and a Will " +
		"this broker does not hold is one it could never publish",
}

// willRefusal judges a Will against what can be known at CONNECT: the
// topic it names, and whether the retain flag it carries could ever be
// honoured. It returns the code and a reason, or an empty reason when the
// Will is acceptable.
//
// Everything here is permanent for that client's configuration - a device
// aimed at the reserved space will never be aimed anywhere else by trying
// again - so refusing the connection is telling it the one thing it can
// act on. What cannot be known until the Will fires, a full channel or a
// storage failure, is not here and is logged instead: the device is gone
// by then and there is nobody to tell.
//
// Refusing costs the connection, because MQTT requires the server to close
// after a non-zero CONNACK. That is the point rather than a side effect: a
// device whose fleet monitoring can never arm learns it on its first
// connect instead of the day somebody asks why an alert never came.
func (b *Broker) willRefusal(pk packets.Packet) (packets.Code, string) {
	topic := pk.Connect.WillTopic

	switch {
	case len(topic) > b.limits.MaxTopicLength:
		// Nothing bounds a Will topic anywhere else: it arrives in CONNECT
		// and never goes through the publish path's checks until it fires,
		// by which time refusing it tells nobody.
		return packets.ErrTopicNameInvalid,
			"the Will topic is longer than max_topic_length"
	case channel.TooDeep(topic, b.limits.MaxTopicLevels) != nil:
		return packets.ErrTopicNameInvalid,
			"the Will topic is deeper than max_topic_levels"
	case strings.HasPrefix(topic, channel.ReservedRoot+"/"):
		// The reserved space holds the topics saguin defines for seeking
		// and for answering a delivery. A Will landing there would be a
		// dead device seeking on a live consumer's behalf.
		return packets.ErrTopicNameInvalid,
			"the Will topic is in the reserved $saguin/ space"
	case notATopicName(topic) != "":
		// Whatever a publish to this name would be refused for, the Will is
		// refused for here - a wildcard, the reserved `$SYS` tree, or
		// `$share/`. The publish path refuses it too, but only when the Will
		// fires, and by then the device is gone and the log is the only
		// place it goes. Asked here, the device is still connected to be
		// told.
		//
		// **The publish path's own question** (notATopicName) rather than a
		// list of the rules, because a list is a list to keep in step: this
		// one asked the substrate alone, which does not know `$share/`, and
		// a Will there was acknowledged and never published.
		return packets.ErrTopicNameInvalid, "the Will topic: " + notATopicName(topic)
	}

	c := b.reg.Resolve(topic)
	if c != nil && strings.HasSuffix(c.Name, channel.DLQSuffix) {
		// A dead-letter channel takes records from its queue and from
		// nothing else, which is the same rule a publish into one meets.
		return packets.ErrTopicNameInvalid,
			"the Will topic is in a dead-letter channel, which takes records only from its queue"
	}

	// A retained Will asks for a store the way a retained publish does, and
	// is refused in exactly the cases one is - which is the point of asking
	// here rather than inventing a second rule for Wills.
	//
	// **One case, not two.** A retained Will aimed at a queue used to be
	// refused here as well, and it is now accepted with the flag dropped,
	// because the publish path accepts one: two answers to one flag would
	// mean a device could publish to a queue all day and be refused at
	// CONNECT for saying the same thing in its Will. Aiming an availability
	// message at a queue is still a mistake - one worker takes it once and
	// no dashboard sees it - but it is the operator's configuration that
	// decided the topic is work, and refusing the connection is not how
	// they find out. saguin_queue_retain_ignored_total is.
	if pk.Connect.WillRetain && c == nil && b.retained == nil {
		return packets.ErrRetainNotSupported,
			"the Will is retained and this broker has no retained store attached for broadcast topics"
	}

	return packets.CodeSuccess, ""
}

// armedWill is the Will this CONNECT armed, as the session store keeps it, or
// nil where it armed none.
//
// **Its due time is not set here.** A Will becomes due when its client goes
// away, and while the client is connected there is no such moment - so the
// zero time is the truth about it, and recordDisconnect is what stamps the
// moment the wait began.
func (b *Broker) armedWill(cl *mqtt.Client, pk packets.Packet) *store.SessionWill {
	if !pk.Connect.WillFlag {
		return nil
	}
	b.mu.Lock()
	props := b.willProps[cl]
	b.mu.Unlock()
	var user []store.Header
	for _, u := range pk.Connect.WillProperties.User {
		user = append(user, store.Header{Key: u.Key, Value: u.Val})
	}
	return &store.SessionWill{
		Topic:    pk.Connect.WillTopic,
		Payload:  append([]byte(nil), pk.Connect.WillPayload...),
		QoS:      pk.Connect.WillQos,
		Delay:    pk.Connect.WillProperties.WillDelayInterval,
		Props:    props,
		User:     user,
		Identity: identity(cl),
	}
}

// OnWill delivers a Will through the ordinary publish path, which is the
// only way it can be delivered at all without stepping around every rule
// saguin has (RFC 0003 "Last Will").
//
// The substrate publishes a Will from sendLWT, which calls
// publishToSubscribers directly and never OnPublish - so left alone, a Will
// strips no `saguin-` prefix, is issued no Message ID or offset, reaches a
// channel's live subscribers while being stored nowhere, hands a queue
// worker something no timeout will ever reclaim, and skips every bound.
// Routing it here is what makes it an ordinary record instead.
//
// **What comes back matters as much as what is published, and it is always
// nothing.** Whatever this returns is what the substrate publishes itself,
// past OnPublish and past every rule - so returning a Will with a topic on
// it opens a second road that nothing on this one guards. It did: a Will of
// `#` came back for the substrate to fan out, and reached subscribers
// carrying a topic name MQTT forbids in a delivery. Emptying it always is
// what closes that, and publishWill is then the only way a Will is
// delivered, immediate or delayed.
//
// A refusal is logged rather than answered. The client is gone by the time
// a Will fires, so a full channel or a storage failure has nobody to tell
// - which is why everything that *can* be judged earlier is judged at
// CONNECT instead, where the device is still there. willRefusal is that
// judgement, and the topic name is part of it.
func (b *Broker) OnWill(cl *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	// A delay means this Will is not due yet, and the substrate must not
	// hold it: its own delayed path publishes past every hook, measured, so
	// a Will with a delay would reach subscribers with none of the above
	// having happened. saguin keeps it and empties this one.
	// **Taken either way, because the entry belongs to the connection.** An
	// immediate Will is published from what this returns; a delayed one is
	// published from the session record when its timer fires, and reads its
	// properties back from there. Leaving the entry for the delayed case
	// would be a map keyed by a connection that has gone, which is the bound
	// invariant 13 is about.
	props := b.takeWillProps(cl)
	// **A connection this broker is closing is not a device dying** (the
	// epic's rule: no Will fires because the broker stopped). Every client
	// goes at a shutdown, and each one reaching here would be announced dead
	// - the whole fleet, on every restart.
	if b.stopping.Load() {
		return emptyWill(will), nil
	}
	if d := b.willDelay(cl, will); d > 0 {
		// **Judged and held under the client id's session lock**, and
		// published after it. willAtTakeover asks whose the id is under b.mu
		// and holdWill writes the id's record without it, so without the
		// session lock a connection claiming the id between the two had this
		// Will's due moment stamped on its own record and a timer armed that
		// took its Will off that record and published it: the new connection
		// announced dead while connected, and nothing sent when it went.
		unlock := b.lockSession(cl.ID)
		outcome := b.willAtTakeover(cl, will, props, d)
		if outcome == willHeld {
			b.holdWill(cl.ID, will, d)
		}
		unlock()
		switch outcome {
		case willSessionEnded:
			b.log.Info("publishing a Will: another connection took its client id with a clean start, "+
				"so its session ended before the delay did", "client", b.limits.Loggable(cl.ID),
				"topic", b.limits.Loggable(will.TopicName))
			b.publishWill(cl.ID, identity(cl), "session_ended", will, props)
		case willResumed:
			b.counted.willsCancelled.Add(1)
			b.log.Info("a Will was not held: another connection resumed its session inside the delay",
				"client", b.limits.Loggable(cl.ID))
		case willWaitsForClaim:
			b.log.Debug("a Will waits for the connection that took its client id: its CONNACK is not written yet",
				"client", b.limits.Loggable(cl.ID))
		}
		return emptyWill(will), nil
	}
	// Spent, published or refused: the disconnect that follows takes it off
	// the record, whether or not a stop has begun by then (recordDisconnect).
	b.mu.Lock()
	b.willFired[cl] = true
	b.mu.Unlock()
	b.publishWill(cl.ID, identity(cl), "immediate", will, props)
	return emptyWill(will), nil
}

// What becomes of a delayed Will whose connection another has taken the
// client id from (willAtTakeover).
const (
	willHeld          = iota // its own: it waits out its delay
	willSessionEnded         // a clean start ended its session: published now
	willResumed              // its session was resumed: dropped (MQTT-3.1.3-9)
	willWaitsForClaim        // the taking connection's CONNACK is not written yet
)

// willAtTakeover decides a delayed Will's fate when its connection has been
// taken over, and keeps it on the claim when the taking is not final yet.
//
// **A connection taken over is not a device that died, and its Will is not
// the id's.** The Will waited by client id and was read back from the id's
// record when its delay ran out - and by then the id and its record were the
// connection that took over, so the broker announced the *new* connection
// dead two seconds after it arrived, still connected, with the new
// connection's own Will (measured, on a resume and on a clean start alike).
// MQTT 5 says what happens instead, in the non-normative note on taking a
// session over: a delay of 0 publishes at the close, which the caller has
// already done; a resume inside the delay drops it [MQTT-3.1.3-9]; and a
// clean start publishes it, because the session it belonged to has ended.
// mosquitto and EMQX do the same.
func (b *Broker) willAtTakeover(cl *mqtt.Client, will mqtt.Will, props store.Props, d time.Duration) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ended, taken := b.supersededWills[cl]; taken {
		delete(b.supersededWills, cl)
		if ended {
			return willSessionEnded
		}
		return willResumed
	}
	// Taken by a connection whose CONNACK has not been written: it may yet
	// give the id back, so the Will waits with its claim (confirmClaim,
	// settleClaim), as this connection's teardown does.
	if owner := b.owner[cl.ID]; owner != nil && owner != cl {
		if c, pending := b.claims[owner]; pending && c.prev == cl {
			c.prevWill = &deferredWill{will: will, props: props, delay: d, identity: identity(cl)}
			return willWaitsForClaim
		}
	}
	return willHeld
}

// deferredWill is a delayed Will that arrived while another connection's
// claim on its client id was still pending.
type deferredWill struct {
	will     mqtt.Will
	props    store.Props
	delay    time.Duration
	identity string
}

// willDelay is how long this Will waits before it is published: what the
// client asked for, and never longer than the session it belongs to.
//
// **"Whichever happens first" is the rule, and both halves of it are here**
// (MQTT 5 section 3.1.3.2.2): a server delays a Will "until the Will Delay
// Interval has passed or the Session ends, whichever happens first". A
// session that ends with its connection ends at this disconnect, so its Will
// is due now however long the interval says - and a session with an expiry
// shorter than the interval ends at that expiry, so its Will is due then.
//
// Without this a device that connects with Clean Start and a five-minute
// delay is announced dead five minutes after it goes, on a session the broker
// threw away at the disconnect: an announcement about a session that no
// longer exists, arriving long after anything could act on it. Measured
// before this: a 30-second delay on such a client, the connection cut, and
// nothing published in the five seconds that followed.
func (b *Broker) willDelay(cl *mqtt.Client, will mqtt.Will) time.Duration {
	asked := time.Duration(will.WillDelayInterval) * time.Second
	if asked <= 0 {
		return 0
	}
	// The session's own life, from what the client was granted. A session
	// that ends with its connection has none to outlive, so the Will is due
	// at once.
	if !persistentSession(cl) {
		// **Said out loud, because a device migrating from another broker
		// will behave differently here.** mosquitto waits out the delay for
		// a session like this; saguin publishes at the disconnect, which is
		// what the specification says and what EMQX does. An operator who
		// wants flap protection needs a session for the delay to wait in, and
		// the only place that can be said is this log line - nothing on the
		// wire carries it, and the device is gone by the time it matters.
		b.log.Info("a Will delay was not honoured: the session ends with its connection, "+
			"so the Will is published at the disconnect",
			"client", b.limits.Loggable(cl.ID), "topic", b.limits.Loggable(will.TopicName),
			"asked_s", will.WillDelayInterval,
			"fix", "ask for a session expiry at least as long as the delay")
		return 0
	}
	if ends := time.Duration(b.grantedExpiry(cl)) * time.Second; ends < asked {
		return ends
	}
	return asked
}

// willPropsOf is what a CONNECT's Will carries beside its topic and payload.
func willPropsOf(pk packets.Packet) store.Props {
	wp := pk.Connect.WillProperties
	return store.Props{
		ContentType:       wp.ContentType,
		ResponseTopic:     wp.ResponseTopic,
		CorrelationData:   append([]byte(nil), wp.CorrelationData...),
		PayloadFormat:     wp.PayloadFormat,
		PayloadFormatFlag: wp.PayloadFormatFlag,
		MessageExpiry:     wp.MessageExpiryInterval,
		Retain:            pk.Connect.WillRetain,
		// Present and empty, kept as such.
		ContentTypeEmpty:     wp.ContentTypeFlag,
		ResponseTopicEmpty:   wp.ResponseTopicFlag,
		CorrelationDataEmpty: wp.CorrelationDataFlag,
	}
}

// takeWillProps removes and returns a connection's Will properties, which is
// both halves of one operation on purpose: a Will fires once, so the entry
// has no reason to outlive the read, and taking it here is what keeps the
// delayed path from depending on state the disconnect is about to drop.
func (b *Broker) takeWillProps(cl *mqtt.Client) store.Props {
	b.mu.Lock()
	defer b.mu.Unlock()
	props := b.willProps[cl]
	delete(b.willProps, cl)
	return props
}

// publishWill puts one Will through the publish path, as saguin rather than
// as the device, and is the only way a Will is delivered.
//
// **One road, because two disagreed.** A Will that waited out a delay went
// through here and was refused a wildcard topic; an immediate one called
// routePublish directly and was not, so `events/+` was stored in an append
// channel and every conforming consumer of that channel stopped at it, for
// ever and across restarts. The half that was right was the half written
// second, which is the argument for there being one.
//
// Injecting runs everything a publish runs - the bounds, the topic name,
// the reserved space, channel resolution, storage, and the server's own
// fan-out for a broadcast topic. Returning the Will to the substrate
// instead ran none of it: `sendLWT` hands what this hook returns straight
// to `publishToSubscribers`, so a Will of `#` reached subscribers with a
// topic name MQTT forbids. Every caller of this therefore empties the Will
// afterwards, and that emptying is what closes that door.
//
// It is published as saguin because the device's session is being torn
// down as this runs. That costs nothing a subscriber can see - a Will
// carries no publisher identity on the wire - and it is what the delayed
// path has always done, since by then the session is gone entirely.
//
// A refusal is logged and not answered. The client is gone by the time a
// Will fires, so there is nobody to tell, which is exactly why everything
// that can be judged at CONNECT is judged there instead.
//
// **But the acl_file is asked again here, as the client that armed it**
// (ident, clientID). The inline client this publishes as is one the ACL never
// asks, and the rules can be reloaded between the CONNECT and the fire, which
// may be days apart on a session that outlives its connection: a role that
// lost `write` on a topic has lost it for its Will too. mosquitto asks at
// both moments. Refused, it is counted as the PUBLISH it would have been
// (saguin_publish_refused_total, `not authorized`), because that is what it
// is.
//
// cause is saguin_wills_published_total's label, counted here and only for
// a Will that was published: counted by each caller beforehand, a Will this
// refused read as announced.
func (b *Broker) publishWill(clientID, ident, cause string, will mqtt.Will, props store.Props) {
	if b.willClient == nil {
		b.log.Error("cannot deliver a Will: no client to publish it as",
			"client", clientID, "topic", b.limits.Loggable(will.TopicName))
		return
	}
	if !b.permitsAs(ident, clientID, will.TopicName, true) {
		b.counted.refuse(reasonNames[packets.ErrNotAuthorized.Code])
		b.log.Warn("refused a Will: the acl_file does not allow the client that armed it to publish here",
			"client", b.limits.Loggable(clientID), "user", b.limits.Loggable(ident),
			"topic", b.limits.Loggable(will.TopicName))
		return
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    will.Qos,
			Retain: will.Retain,
		},
		TopicName: will.TopicName,
		Payload:   will.Payload,
		// **A QoS 1 packet must carry a packet identifier to be valid**, and
		// the substrate refuses one that does not - `protocol violation:
		// missing packet id`. Without this every Will a client armed at QoS 1
		// was accepted at CONNECT, answered `0x00`, and then silently never
		// delivered when it fired: the client believed its death
		// announcement was armed, and the only trace was one WARN line on
		// the broker. QoS 0 Wills worked throughout, which is why the test
		// that drives four destinations was green over it.
		//
		// **Every publish property the Will carried, not only the User
		// Properties.** This forwarded `will.User` alone, because that is
		// all the substrate's Will struct has - so a Will declaring
		// `application/json` arrived as opaque bytes, one carrying a
		// Response Topic so a monitor could answer it arrived with nowhere
		// to answer, and one asking to expire in a minute became one that
		// never expires. None of it is visible from the wire, which is why
		// the loss lasted. willProps is where they were kept.
		Properties: packets.Properties{
			User:                  will.User,
			ContentType:           props.ContentType,
			ResponseTopic:         props.ResponseTopic,
			CorrelationData:       props.CorrelationData,
			PayloadFormat:         props.PayloadFormat,
			PayloadFormatFlag:     props.PayloadFormatFlag,
			MessageExpiryInterval: props.MessageExpiry,
			ContentTypeFlag:       props.ContentTypeEmpty,
			ResponseTopicFlag:     props.ResponseTopicEmpty,
			CorrelationDataFlag:   props.CorrelationDataEmpty,
		},
	}
	// **Only at QoS 1**, because MQTT forbids a packet identifier on a QoS 0
	// PUBLISH as firmly as it requires one above it - setting it on every
	// Will traded one silent failure for the other, and the widened test
	// caught that in the same run that caught the first.
	//
	// Any non-zero value, for the reason the bridge client's injected
	// publish gives: this packet is injected rather than sent to anybody, so
	// nothing ever reads the identifier back.
	if pk.FixedHeader.Qos > 0 {
		pk.PacketID = 1
	}

	c := b.reg.Resolve(will.TopicName)
	// **Published only where the publish path took it.** InjectPacket
	// returns nil for a publish that path refused - a full channel, a
	// storage failure, a topic nothing may publish to - because the server
	// answers the refusal with an acknowledgement to a client that has no
	// connection. Counting on that nil read those Wills as published. OnPublish records the answer instead,
	// and a Will it never saw counts as not taken.
	b.willMu.Lock()
	b.willOutcome = errWillNotDecided
	b.willArmer = clientID
	err := b.srv.InjectPacket(b.willClient, pk)
	refused := b.willOutcome
	b.willMu.Unlock()
	if err != nil {
		b.log.Warn("cannot deliver a Will", "client", clientID,
			"topic", b.limits.Loggable(will.TopicName), "error", err)
		return
	}
	if refused != nil {
		// RFC 0003: "A full channel or a storage failure is logged and no
		// more". The refusal itself is counted where the publish path
		// counts it, in saguin_publish_refused_total.
		//
		// The reason is bounded, not exempted: it is whatever error the
		// publish path returned, and nothing about the value keeps a
		// client's text out of it, only today's callers - the argument
		// OnPublish's legacy refusal line makes for its code.Reason.
		b.log.Warn("a Will was refused when it fired, and is not published",
			"client", b.limits.Loggable(clientID), "topic", b.limits.Loggable(will.TopicName),
			"reason", b.limits.Loggable(refused.Error()))
		return
	}
	b.counted.willsPublished(cause)
	b.log.Info("delivered a Will", "client", clientID,
		"topic", b.limits.Loggable(will.TopicName), "channel", channelName(c))
}

// errWillNotDecided is a Will's outcome until OnPublish records one: a
// Will the publish path never decided on is not one it took.
var errWillNotDecided = errors.New("the publish path never decided on it")

// emptyWill is a Will the substrate will publish to nobody. An empty topic
// matches no subscriber, and the retain flag goes down with it: a Will
// delivered to nobody has no state to record, and it is the one publish
// where leaving the flag up would ask the substrate to keep something under
// a topic name of "".
func emptyWill(will mqtt.Will) mqtt.Will {
	will.TopicName = ""
	will.Retain = false
	will.Payload = nil
	will.User = nil
	return will
}

// pendingWill is a Will waiting out its delay: the timer that will publish
// it, kept so a reconnect can stop it. When it is due is on the session
// record (stampWillDue), which is what a restart reads.
//
// **The Will itself is not here.** It is on the session record, in the
// provider `broker.session.storage` names, written by the CONNECT that armed
// it and read back once when the timer fires. Holding it here as well kept a
// second copy of a message for the whole of every delay, which is what
// keeping it in the operator's storage removes - the rule step C already
// applied to deliveries: memory holds what is being written, and nothing that
// is only waiting.
type pendingWill struct {
	timer *time.Timer
}

// holdWill keeps a Will that asked to be delayed, and delivers it when the
// delay expires unless the same client id connects again first.
//
// The delay is the whole reason saguin holds these rather than leaving
// them to the substrate: a device on a link that drops for four seconds
// has not died, and announcing that it has is the false alarm the interval
// exists to prevent. Measured against the substrate's own path as it was:
// OnWill fired at the disconnection and then nothing did - not OnPublish,
// not even the OnWillSent it then had - while the Will reached subscribers
// regardless. So a
// delayed Will there is one that skips every rule.
//
// It is one timer per client id whose Will is waiting, on a session the store
// keeps, so the session store's max_bytes bounds them (pendingWill). A
// restart does not clear them: the due moment is on the record, and
// restoreWill arms the timer again for what is left of the wait (RFC 0003
// "Last Will").
func (b *Broker) holdWill(clientID string, will mqtt.Will, d time.Duration) {
	// **The wait is written down before it is timed.** The Will itself is in
	// broker.session.storage - this is the moment it becomes due, stamped on
	// the record the operator's provider holds, so that the answer after a
	// restart is arithmetic rather than a guess. Written before the timer is
	// armed, or a delay short enough could fire against a record that does
	// not yet say when it was due.
	due := time.Now().Add(d)
	b.stampWillDue(clientID, due)

	b.mu.Lock()
	if prev, ok := b.pendingWills[clientID]; ok {
		// One pending Will per client id. A session taken over and dying
		// again must not leave two.
		prev.timer.Stop()
	}
	p := &pendingWill{}
	p.timer = time.AfterFunc(d, func() { b.firePendingWill(clientID, p) })
	b.pendingWills[clientID] = p
	b.mu.Unlock()

	// The wait, which is what the client asked for or its session's own life,
	// whichever is shorter: an operator reading "delay_s" wants the number
	// the broker is actually waiting.
	b.log.Info("holding a Will until its delay expires", "client", clientID,
		"topic", b.limits.Loggable(will.TopicName), "delay_s", int(d/time.Second),
		"asked_s", will.WillDelayInterval)
}

// firePendingWill delivers a held Will, unless something already cancelled
// or replaced it.
//
// **The Will comes from the store, because that is where it is.** What waits
// is not held in memory twice: the timer knows a client id and a moment, and
// the message it publishes is read once, here, from the provider
// `broker.session.storage` names. A read that finds nothing is a Will
// something else has already taken - a clean disconnect, an expiry, a
// takeover - and a read that fails is counted and logged rather than
// publishing an announcement the broker cannot stand behind.
//
// **The entry and the record are read under the client id's session lock**,
// and the Will published after it. Read without it, a connection that claimed
// the id between this timer taking its entry and reading the record had its
// own Will taken off its record and published - the device announced dead
// while connected, the Will that was due never sent, and nothing sent when
// the new connection did go.
func (b *Broker) firePendingWill(clientID string, p *pendingWill) {
	unlock := b.lockSession(clientID)
	b.mu.Lock()
	current, ok := b.pendingWills[clientID]
	if !ok || current != p || b.stopping.Load() {
		// Cancelled by a reconnect, or replaced by a later session's Will.
		// The identity check matters: comparing only the client id would
		// let this timer publish the *replacement's* Will early. Or the
		// broker is stopping, and the next start publishes it (Shutdown).
		b.mu.Unlock()
		unlock()
		return
	}
	delete(b.pendingWills, clientID)
	// Counted under b.mu, which Shutdown takes after marking itself
	// stopping: so this fire is either counted before Shutdown waits, or
	// refused above.
	b.drains.Add(1)
	defer b.drains.Done()
	b.mu.Unlock()

	sess, ok := b.storedWill(clientID)
	unlock()
	if !ok {
		return
	}
	// The same road an immediate Will takes. The "holding a Will" line above
	// is what tells an operator this one waited; there is no second message
	// for it, because two messages for one event is two things to keep true.
	//
	// **Published, then taken off its record**, the order every other Will
	// path keeps (RFC 0003 "Last Will"). Taken off first, a crash between the
	// two lost the Will for good; published first, it is published again by
	// the next start - at least once across a crash, never lost to one.
	w := *sess.Will
	will, props := willOf(w)
	b.publishWill(clientID, w.Identity, "delayed", will, props)
	unlock = b.lockSession(clientID)
	b.takePublishedWill(sess)
	unlock()
	if f := afterWillTaken.Load(); f != nil {
		(*f)(clientID)
	}
}

// afterWillTaken is a test seam, nil in production: when set it runs once a
// delayed Will has been published and taken off its record, so a test can
// read the record after the take rather than sleeping past it.
var afterWillTaken atomic.Pointer[func(clientID string)]

// endWaitingWill cancels a Will the client id's previous session left
// waiting, once a resume's claim on the id is confirmed.
//
// **A resume cancels it**, which is MQTT's rule and RFC 0003's: the delay
// exists because a device that comes back was not dead, so a session resumed
// inside it never announces its death [MQTT-3.1.3-9]. A clean start ends that
// session instead, and endSession publishes the Will: a Will is published
// when its session ends, whichever of that and the delay comes first.
//
// **Only where the new connection wrote nothing is the old Will still on the
// id's record**, and taken from it: otherwise the new connection has written
// its own session there, with its own Will, and taking from the record took
// the new connection's Will off its own session.
func (b *Broker) endWaitingWill(clientID string, c *claim) {
	b.mu.Lock()
	p, waiting := b.pendingWills[clientID]
	if waiting {
		delete(b.pendingWills, clientID)
	}
	b.mu.Unlock()
	if !waiting {
		return
	}
	p.timer.Stop()
	b.counted.willsCancelled.Add(1)
	if !c.wrote {
		b.takeStoredWill(clientID)
	}
	b.log.Info("a held Will was cancelled: the client connected again", "client", clientID)
}

// stampWillDue records, on the session that armed it, the moment its Will
// becomes due. A session with no record and no Will has nothing to stamp,
// which is every session that armed none.
func (b *Broker) stampWillDue(clientID string, due time.Time) {
	s := b.sessionStore()
	if s == nil {
		return
	}
	sess, ok, err := b.getSession(s, clientID)
	if err != nil {
		b.log.Error("cannot read a session to record when its Will is due",
			"client", b.limits.Loggable(clientID), "error", err)
		return
	}
	if !ok || sess.Will == nil {
		return
	}
	sess.Will.DueAt = due
	if err := b.saveSession(s, sess); err != nil {
		b.log.Error("cannot record when a Will is due: this run publishes it when its delay ends, "+
			"but after a restart it waits for its session to end instead",
			"client", b.limits.Loggable(clientID), "error", err)
	}
}

// takeStoredWill reads a session's Will and removes it in one step, and
// reports whether there was one: a Will fires once, so the read and the
// removal are halves of the same operation. Its session stays - what ends a
// session is a disconnect, an expiry or a clean start, never its Will firing.
func (b *Broker) takeStoredWill(clientID string) (store.SessionWill, bool) {
	s := b.sessionStore()
	if s == nil {
		return store.SessionWill{}, false
	}
	sess, ok, err := b.getSession(s, clientID)
	if err != nil {
		b.mu.Lock()
		provider := b.sessionsProvider
		b.mu.Unlock()
		b.log.Error("cannot read the Will a session was holding: it is not published",
			"client", b.limits.Loggable(clientID), "provider", provider, "error", err)
		return store.SessionWill{}, false
	}
	if !ok || sess.Will == nil {
		return store.SessionWill{}, false
	}
	w := *sess.Will
	sess.Will = nil
	if err := b.saveSession(s, sess); err != nil {
		b.willNotTakenOff(sess, err)
	}
	return w, true
}

// willNotTakenOff remembers a Will the store refused to take off its record.
//
// **Remembered, as a refused disconnect is** (recordDisconnect). The Will is
// published or cancelled all the same, and a record still holding it, with
// its due moment, is one the next start publishes again: once owed at the
// start, or once more when its session ends. So it is kept as a Will this
// record must drop - which the ending asks before it publishes
// (willWithdrawn) and the stop writes - with the moment the record already
// says its client went away, or the moment an earlier refused disconnect
// had.
func (b *Broker) willNotTakenOff(sess store.Session, err error) {
	b.log.Error("cannot take a Will off the session that armed it; it is tried again as the broker stops",
		"client", b.limits.Loggable(sess.Client), "error", err)
	b.mu.Lock()
	d, pending := b.unwritten[sess.Client]
	if !pending {
		// The record's own moment and expiry, which the write at the stop
		// keeps: written as zero, the expiry would end the session.
		d.at, d.expiry = sess.DisconnectedAt, sess.ExpiryInterval
	}
	d.dropWill = true
	b.unwritten[sess.Client] = d
	b.mu.Unlock()
}

// storedWill is the Will a session's record holds, read and left there:
// the delayed path publishes it before it takes it off (firePendingWill). A
// read that fails is counted and logged, and nothing is published - an
// announcement the broker cannot stand behind is not made.
func (b *Broker) storedWill(clientID string) (store.Session, bool) {
	s := b.sessionStore()
	if s == nil {
		return store.Session{}, false
	}
	sess, ok, err := b.getSession(s, clientID)
	if err != nil {
		b.mu.Lock()
		provider := b.sessionsProvider
		b.mu.Unlock()
		b.log.Error("cannot read the Will a session was holding: it is not published",
			"client", b.limits.Loggable(clientID), "provider", provider, "error", err)
		return store.Session{}, false
	}
	return sess, ok && sess.Will != nil
}

// takePublishedWill takes a Will off its record once it has been published,
// if the record is still the one it was published from.
//
// **Asked again, because the id's session lock was let go to publish**, and
// asked three ways, because a Will taken from the wrong record is an
// announcement lost:
//   - a connection holds the id now: it claimed it while the Will was being
//     published, and the record is its own, whatever it holds. A resume
//     that wrote nothing leaves the published Will on it, which the
//     connection's own disconnect drops and no start publishes: the Will
//     has a due moment, or it has none and a start discards it;
//   - the Will's due moment differs: another Will;
//   - the moment the record says its client went differs: another
//     connection came and went. This is what tells two Wills apart when
//     neither has a due moment - one whose stamp the store refused, and
//     one a connection armed - which the due moment alone took for the
//     same (with the store refusing the stamp).
//
// A write the store refuses is remembered (willNotTakenOff). A read it
// refuses is not: the record most likely still holds the published Will, and
// the next start publishing it again is the direction RFC 0003 states, where
// a mark made blind could take a Will armed since.
func (b *Broker) takePublishedWill(was store.Session) {
	s := b.sessionStore()
	if s == nil {
		return
	}
	b.mu.Lock()
	_, held := b.owner[was.Client]
	b.mu.Unlock()
	if held {
		return
	}
	now, ok, err := b.getSession(s, was.Client)
	if err != nil {
		b.log.Error("cannot read a session to take off the Will it published: the next start may publish it again",
			"client", b.limits.Loggable(was.Client), "error", err)
		return
	}
	if !ok || now.Will == nil || !now.Will.DueAt.Equal(was.Will.DueAt) || !now.DisconnectedAt.Equal(was.DisconnectedAt) {
		return
	}
	now.Will = nil
	if err := b.saveSession(s, now); err != nil {
		b.willNotTakenOff(now, err)
	}
}

// willOf is a stored Will as the engine's, with its properties. The one
// conversion, so every path that publishes from the record carries the same
// fields: a second copy of it was where the User Properties went missing.
func willOf(w store.SessionWill) (mqtt.Will, store.Props) {
	var user []packets.UserProperty
	for _, h := range w.User {
		user = append(user, packets.UserProperty{Key: h.Key, Val: h.Value})
	}
	return mqtt.Will{
		TopicName:         w.Topic,
		Payload:           w.Payload,
		User:              user,
		Qos:               w.QoS,
		Retain:            w.Props.Retain,
		WillDelayInterval: w.Delay,
		Flag:              1,
	}, w.Props
}

// takeWillAtSessionEnd is the Will a session still held when it ended, and
// stops the timer that would otherwise publish it a moment later. It
// publishes nothing: its caller does (publishEndedWill), before the record is
// dropped where the ending drops it - "published first" (RFC 0003
// "Sessions").
//
// **The Will is the ended record's, as endSession's caller read it**, never
// whatever the store holds for the id by now: an ending that re-read the
// store could find a successor's record there, and publish that session's
// Will for it (invariant 17).
//
// **Owed whether or not a timer was waiting**, because one kind of Will
// has no timer: a session restored after a stop, whose client was connected
// at the moment and has not come back. Its Will carries no due moment -
// nothing had happened to make it due - and the expiry passing is the thing
// that does: this broker has watched that client stay away for the whole
// interval it was granted, which is a device that went and did not return
// rather than a Will firing because a broker stopped (RFC 0003 "Last Will").
//
// **Except a Will with a due moment whose timer is already gone**: that timer
// fired first - firePendingWill takes the entry before it publishes - or the
// start published it as already due, and publishing it for the ending as well
// would be twice. A Will with a due moment always has a timer until then, from
// holdWill or from restoreWill - or the claim that ended the session stopped
// it, which the ended session says (willTimerTaken).
func (b *Broker) takeWillAtSessionEnd(clientID string, ended endedSession) (store.SessionWill, bool) {
	b.mu.Lock()
	p, waiting := b.pendingWills[clientID]
	if waiting {
		delete(b.pendingWills, clientID)
	}
	b.mu.Unlock()
	if waiting {
		p.timer.Stop()
	}
	waiting = waiting || ended.willTimerTaken
	rec := ended.record
	if rec == nil || rec.Will == nil {
		return store.SessionWill{}, false
	}
	// **Withdrawn by a clean DISCONNECT the store was not told about**
	// (recordDisconnect): the record still carries it, and publishing it
	// would announce a device dead that said goodbye (MQTT-3.1.2-10).
	if b.willWithdrawn(clientID) {
		return store.SessionWill{}, false
	}
	if !waiting && !rec.Will.DueAt.IsZero() {
		return store.SessionWill{}, false
	}
	return *rec.Will, true
}

// publishEndedWill publishes the Will a session's ending owed
// (takeWillAtSessionEnd).
//
// **Never under a client id's session lock, bar the one site that says
// why** (OnClientExpired). A publish that reaches a connection being taken
// over waits for that connection's session lock (the engine's
// deliveryTarget), so an ending publishing under one id's lock can wait on
// another's - and two ids ending at once, each subscribed to the other's Will
// topic, would wait on each other for ever. TestNothingPublishesUnderASessionLock
// holds that.
func (b *Broker) publishEndedWill(clientID string, w store.SessionWill) {
	b.log.Info("publishing a held Will: its session ended before the delay did",
		"client", clientID, "topic", b.limits.Loggable(w.Topic))
	will, props := willOf(w)
	b.publishWill(clientID, w.Identity, "session_ended", will, props)
}

// dueWill remembers a Will whose moment has passed, so that the pass
// which publishes them can run where publishing is safe.
//
// **Why it is remembered rather than published where it is found.** A Will
// goes through the ordinary publish path, which writes to a channel and
// delivers to the sessions subscribed to it - and at the point the sessions
// are read, none of them is restored yet, so a broadcast Will would reach none
// of the durable subscribers it is owed to. So the reading and the publishing
// are two passes with the restore between them (PublishDueWills).
type dueWill struct {
	client string
	will   store.SessionWill
	// end says the start is ending its session, whose record is kept until
	// the Will is published and dropped then, as the running expiry does
	// (OnClientExpired): dropped first, a start that stopped before it
	// published had ended the session and lost its Will. at and expiry are
	// the record's, for a drop the store refuses (dropRecordAfterItsWill).
	end    bool
	at     time.Time
	expiry uint32
	// why says which rule made it due, for the operator's line and the
	// metric: its delay passed while the broker was stopped, or its session
	// ended while the broker was stopped.
	why string
}

// restoreWill decides what a restored session's Will is now: published,
// waited for, or gone.
//
// **Three cases, and they are the same rule seen at three moments** (MQTT 5
// 3.1.3.2.2, "the Will Delay Interval has passed or the Session ends,
// whichever happens first"):
//
//   - **No moment on it.** Its client was connected when the broker stopped,
//     so nothing has happened yet that makes a Will due: a broker stopping is
//     not a device dying. The Will stays on the record and waits for
//     something that is - the client returning, which replaces it, or the
//     session expiring at this running broker, which publishes it. Stripping
//     it here was the other option and it is the worse one: every restart
//     would quietly take Will protection away from every client that was
//     connected, until each happened to reconnect.
//   - **A moment that has passed.** The device went away and its delay ran
//     out while the broker was down. It is owed, and it is published once the
//     channels are loaded - late, which is the honest cost of the outage and
//     is stated in RFC 0003.
//   - **A moment still ahead.** The wait resumes for what is left of it,
//     rather than starting again: the device has been gone the whole time,
//     and restarting the interval would hide exactly the outage the operator
//     wants to see through.
func (b *Broker) restoreWill(s SessionStore, sess store.Session, now time.Time) {
	w := sess.Will
	if w == nil {
		return
	}
	if w.DueAt.IsZero() {
		return
	}
	if !w.DueAt.After(now) {
		b.mu.Lock()
		b.dueWills = append(b.dueWills, dueWill{
			client: sess.Client, will: *w,
			why: "its delay passed while the broker was stopped",
		})
		b.mu.Unlock()
		return
	}
	left := w.DueAt.Sub(now)
	b.mu.Lock()
	p := &pendingWill{}
	p.timer = time.AfterFunc(left, func() { b.firePendingWill(sess.Client, p) })
	b.pendingWills[sess.Client] = p
	b.mu.Unlock()
	b.log.Info("a restored session is still waiting out its Will delay",
		"client", b.limits.Loggable(sess.Client), "topic", b.limits.Loggable(w.Topic),
		"left_s", int(left/time.Second))
}

// takeWillsDueAtStart remembers the Will of every session this start is about
// to end - one whose expiry passed while the broker was stopped - so that
// PublishDueWills can publish it after the channels are loaded, and answers
// the sessions whose Will is owed, whose records are kept until then. A
// session that ends unpublishes nothing else; its Will is the one thing it
// owes.
//
// **Only a Will with a moment on it.** A Will with none belongs to a session
// whose client was still connected when the broker stopped: the broker
// stopping is not that client dying, and the epic's rule is that no Will
// fires because the broker stopped. It is dropped with the record.
func (b *Broker) takeWillsDueAtStart(s SessionStore, now time.Time) (map[string]bool, error) {
	all, err := s.All()
	if err != nil {
		return nil, err
	}
	owed := map[string]bool{}
	for _, sess := range all {
		if store.SessionExpired(sess, now) && b.oweWill(sess, "its session expired while the broker was stopped") {
			owed[sess.Client] = true
		}
	}
	return owed, nil
}

// oweWill remembers the Will of a session that is being ended, so the start
// publishes it before the record goes, and reports whether it did: the caller
// leaves such a record for PublishDueWills to drop.
//
// **A Will with no moment on it is discarded rather than published**, and
// that is the decided rule rather than an omission: a Will with no moment
// belongs to a client that was still connected when this broker stopped, and
// a broker stopping is not a device dying. What it costs is stated in RFC
// 0003 - a device that really did die during an outage, while connected, is
// never announced, because nothing was left to notice.
func (b *Broker) oweWill(sess store.Session, why string) bool {
	w := sess.Will
	if w == nil || w.DueAt.IsZero() {
		return false
	}
	b.mu.Lock()
	b.dueWills = append(b.dueWills, dueWill{client: sess.Client, will: *w, why: why, end: true,
		at: sess.DisconnectedAt, expiry: sess.ExpiryInterval})
	b.mu.Unlock()
	return true
}

// endingAtStart is the sessions this start is ending once their Wills are
// published (oweWill), which are not restored meanwhile.
func (b *Broker) endingAtStart() map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ending := map[string]bool{}
	for _, d := range b.dueWills {
		if d.end {
			ending[d.client] = true
		}
	}
	return ending
}

// PublishDueWills publishes the Wills this start found already owed, and
// reports how many.
//
// **It runs after the snapshots are loaded and the sessions restored, and
// before any listener opens.** A Will is published through the ordinary
// publish path, so it becomes a record in whatever channel claims its topic -
// and a record written before a channel's snapshot loads is one the load
// replaces - and is delivered to the restored sessions subscribed to it.
// Before a listener opens, because a subscriber connecting first would be
// served a death announcement from before the restart as though it had just
// happened.
func (b *Broker) PublishDueWills() int {
	b.mu.Lock()
	due := b.dueWills
	b.dueWills = nil
	b.mu.Unlock()

	for _, d := range due {
		b.log.Info("publishing a Will this start found owed", "client", b.limits.Loggable(d.client),
			"topic", b.limits.Loggable(d.will.Topic), "reason", d.why)
		will, props := willOf(d.will)
		b.publishWill(d.client, d.will.Identity, "start", will, props)
		// Off the record, so a second start does not publish it again: the
		// session this start is ending goes with it, straight after its Will
		// as at a running expiry, and a Will whose delay passed is taken off
		// the session it leaves.
		if d.end {
			b.dropRecordAfterItsWill(d.client, d.why, nil, d.at, d.expiry)
		} else {
			b.takeStoredWill(d.client)
		}
	}
	return len(due)
}
