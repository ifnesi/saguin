package broker

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// A session and who owns it: what the broker keeps for a client id between
// its connections, which connection speaks for that id at any moment, and
// what goes when nobody does.
//
// **A client id has one owner and the newest connection is it.** MQTT lets
// a second CONNECT take an id from a live connection, so every piece of
// state saguin keys by a client id has a moment where two connections could
// both believe it is theirs. The rule is that the arriving connection
// becomes the owner before the superseded one is hung up, and a teardown
// asks whether it still owns the id before it removes anything: a
// superseded connection's teardown removes nothing, or it takes the state
// its successor has just arrived to use.
//
// **What a session holds is somebody else's state, and that is deliberate.**
// The hooks here are the moments - established, resumed, disconnected,
// expired - and each of them triggers work that belongs to another concern
// and lives with it:
//
//   - **Positions**, in the channel machinery: where a durable consumer had
//     read to, dropped when a session is discarded, and judged against the
//     retention floor when one comes back.
//   - **Owed deliveries**: the exactly-once exchanges a session was holding,
//     which go with it, and the queue's own held records, which go back to
//     the queue rather than with the worker.
//   - **The subscription index**, which is a cache of the substrate's
//     subscriptions rather than a second record, and is rebuilt on a resume
//     rather than carried.
//   - **A shared group's backlog**, in `share.go`: a session ending is what
//     asks whether any member of a group is still coming back.
//
// So these hooks call into four other files, and the sequence rather than
// the state is what lives here. A file that tried to hold all of it would
// be holding four concerns keyed by the same string.
//
// **A session's own record is the store's**, `broker.session.storage`,
// bounded and counted there like every other thing saguin keeps: what it
// holds, what a start does with one it finds, and when one expires are in
// RFC 0002. A Will armed on that record is in `wills.go`, which is the same
// rule at its own moments.

// persistentSession reports whether a connection's session is one that
// outlives it: a Session Expiry Interval above 0 on MQTT 5, Clean Session 0 on
// 3.1.1. A client the acl_file denies `persistent` has already had its
// interval set to 0 by the time this is asked.
func persistentSession(cl *mqtt.Client) bool {
	if legacyClient(cl) {
		return !cl.Properties.Clean
	}
	return cl.Properties.Props.SessionExpiryInterval > 0
}

// keepSession decides, at CONNECT, what the session store keeps for this
// client (RFC 0002 "Every session's state: `broker.session`").
//
//   - A clean start ends whatever session the id held, and what it had in
//     flight goes with it.
//   - A persistent session is saved with the interval it was granted, after
//     limits.max_session_expiry, and a resumed one keeps its subscriptions.
//   - A session the store has no room for is not kept: its interval becomes 0,
//     its CONNACK says so (OnPacketEncode), and it is counted. The connection
//     itself is accepted - MQTT gives a server a way to say "your session ends
//     with this connection", and refusing the client would cost it more than
//     the session it asked for.
//
// **A session that ends with its connection is written when it first holds
// something**, its first SUBSCRIBE (keepSubscriptions), not here. It can be
// sent nothing before then, so a record written at CONNECT would protect
// nothing, and it cost a write and a delete per connection: measured on a
// sqlite provider with no connect rate limit, 3,388-4,023 connections a second
// against 6,334-6,545 without.
func (b *Broker) keepSession(cl *mqtt.Client, pk packets.Packet, c *claim) error {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return nil
	}
	id := cl.ID
	b.mu.Lock()
	provider := b.sessionsProvider
	b.mu.Unlock()

	// **A session the substrate still holds for this id - connected, or away
	// and kept - is not this connection's until its CONNACK is written**
	// (claim). So its record is read first, to be put back if that never
	// happens, and a clean start's end of it waits for the CONNACK rather
	// than being done here.
	existing, held := b.srv.Clients.Get(id)
	predecessor := held && existing != cl
	if predecessor {
		prev, ok, err := b.getSession(s, id)
		if err != nil {
			return b.sessionUnreadable(id, provider, err)
		}
		c.record, c.hadRecord = prev, ok
	}
	if c.clean && predecessor {
		c.dropOnConfirm = true
	}
	will := b.armedWill(cl, pk)

	// **A Will is session state, so a session that armed one is written here
	// whatever its expiry.** A session that ends with its connection is
	// otherwise not written until it first holds something - its first
	// SUBSCRIBE - because a record for a session holding nothing protects
	// nothing and costs a write per connection. A Will is something it holds
	// from its CONNECT, kept where the operator said session state is kept
	// (RFC 0002 `broker.session`).
	sess := store.Session{Client: id, ExpiryInterval: b.grantedExpiry(cl), Will: will}
	keep := persistentSession(cl) || will != nil
	var err error
	if !predecessor {
		// **No session is held for the id, so this one begins new**,
		// whichever Clean Start it asked for. The substrate holds every kept
		// session (RestoreSessions), so what the store has under an id it
		// does not hold is an ended session's, which an ending the store
		// refused left behind. Begin ends it and keeps this session's record
		// in the same write. Nothing of it is read - its subscriptions were
		// carried into a Clean Start 0 session's record, and after a restart
		// restored, subscriptions of a session that had ended.
		var next *store.Session
		if keep {
			next = &sess
		}
		err = b.beginSession(s, id, nil, next)
		if err == nil {
			c.wrote = next != nil
			return nil
		}
		// **Nothing an ended session left is inherited, even when this one's
		// record cannot be kept**: the ending is made on its own, and where
		// the store refuses that too the connection is, since it would begin
		// inside what the ended session left.
		if next == nil {
			return b.sessionNotBegun(id, provider, err)
		}
		if endErr := b.beginSession(s, id, nil, nil); endErr != nil {
			return b.sessionNotBegun(id, provider, endErr)
		}
	} else {
		if !keep {
			return nil
		}
		// A resume keeps what the session it resumes held; what that is was
		// read at the claim, and a read that failed refused the connection.
		if !c.clean && c.hadRecord {
			sess.Subscriptions = c.record.Subscriptions
		}
		err = b.saveSession(s, sess)
		if err == nil {
			c.wrote = true
			if c.dropOnConfirm {
				c.resave = &sess
			}
			return nil
		}
	}

	// **A Will that cannot be kept refuses the connection**, where a session
	// that cannot be kept is accepted and told it ends with this connection.
	// The difference is what the client has been told: a session ending with
	// its connection is stated in the CONNACK's Session Expiry Interval and
	// MQTT gives a server that way of saying it, while MQTT has no way to
	// say "your Will is not held" - so a device that armed one and was
	// accepted would go on believing the broker would speak for it if it
	// died, and nothing would ever say otherwise.
	//
	// **Refused, it is counted as a refusal and not as a dropped session**:
	// saguin_sessions_dropped_total is for a client that was accepted
	// (RFC 0005), and the refusal is counted where every refused CONNECT is,
	// by the reason its code names. And **a full store and a failed write are
	// two answers**: 0x97 says the provider has no room, which is only true
	// of the first.
	if will != nil {
		if errors.Is(err, store.ErrFull) {
			return codeWillNotKept
		}
		b.log.Error("cannot keep a Will: the connection is refused",
			"client", b.limits.Loggable(id), "provider", provider, "error", err)
		return codeWillNotWritten
	}

	// Not kept, so nothing of it may be left to be resumed later either -
	// once this connection is the session, which a predecessor's record
	// waits for. With no predecessor it was ended above.
	if predecessor {
		c.dropOnConfirm, c.resave = true, nil
	}
	b.endWithConnection(cl)

	if errors.Is(err, store.ErrFull) {
		b.counted.sessionsStorageFull.Add(1)
		b.log.Warn("a session asked to outlive its connection and the session store is full: "+
			"it ends with the connection", "client", b.limits.Loggable(id), "provider", provider)
		return nil
	}
	b.log.Error("cannot keep a session: it ends with the connection",
		"client", b.limits.Loggable(id), "provider", provider, "error", err)
	return nil
}

// codeSessionUnreadable is the answer to a CONNECT resuming a session the
// store could not read: `0x83`. Resumed anyway, it would be saved over with
// nothing where its subscriptions were.
var codeSessionUnreadable = packets.Code{
	Code:   packets.ErrImplementationSpecificError.Code,
	Reason: "the session store could not read this client's session to resume it",
}

// codeGroupUnkept is the answer to a CONNECT resuming a durable member whose
// shared group has no cursor and the store could not keep one, other than for
// want of room: `0x83` (repairGroups).
var codeGroupUnkept = packets.Code{
	Code:   packets.ErrImplementationSpecificError.Code,
	Reason: "the session store could not keep this client's shared group's cursor",
}

// sessionUnreadable refuses a CONNECT whose session the store could not
// read, and says so to the operator: logged, where the client is told 0x83
// and connects again. The failed read is counted by the session store's
// wrapper (countingSessions).
func (b *Broker) sessionUnreadable(id, provider string, err error) error {
	b.log.Error("cannot read a session to resume it: the connection is refused",
		"client", b.limits.Loggable(id), "provider", provider, "error", err)
	return codeSessionUnreadable
}

// sessionNotBegun refuses a CONNECT for an id whose ended session the store
// would not end, and says so: logged, where the client is told 0x83 and
// connects again. The failed write is counted by countingSessions.
func (b *Broker) sessionNotBegun(id, provider string, err error) error {
	b.log.Error("cannot end what an ended session left under this client id: the connection is refused",
		"client", b.limits.Loggable(id), "provider", provider, "error", err)
	return codeSessionUnreadable
}

// DropSessionsLeftByAStop ends, when the broker starts, the sessions in the
// session store that no client can have any more, with the messages they
// were owed, and reports how many of each. Called once, before any listener
// opens.
//
//   - Expired: its client had been away for its whole expiry interval by now.
//     Counted as saguin_sessions_dropped_total{cause="expired_while_stopped"}.
//   - Ended: its interval is zero, so it ended with its connection, and that
//     connection ended with the broker. One whose client disconnected before
//     the stop is already gone; this is the one a crash left. Not counted: a
//     session that ends with its connection losing it is what it asked for.
//
// **Nothing else would remove either.** The expiry sweep and the disconnect
// end the sessions the running broker holds, and these are held by nothing
// but the store, so a sqlite store kept every one across every restart and
// its max_bytes filled with them until live clients were refused room. A
// persistent session still inside its interval is left for its client, and so
// is one whose client was connected when the broker stopped.
func (b *Broker) DropSessionsLeftByAStop(now time.Time) (expired, ended int, err error) {
	s := b.sessionStore()
	if s == nil {
		return 0, 0, nil
	}
	// **The Wills of sessions about to be dropped are taken first**, because
	// a session ending is what makes its Will due (MQTT 5 3.1.3.2.2). One
	// that was still waiting out its delay is owed: its client went away, its
	// session has now ended, and nobody else will ever say so.
	//
	// **And its record is kept until the Will is published**, which
	// PublishDueWills does before it drops it: the order the running expiry
	// keeps (OnClientExpired), and RFC 0003's "published before its record
	// goes". Dropped here, a start that stopped before it published - a
	// session store it could not restore from - had ended the session and
	// lost the Will. A read that fails stops the start for the same reason,
	// rather than ending sessions whose Wills nobody has read.
	owed, err := b.takeWillsDueAtStart(s, now)
	if err != nil {
		return 0, 0, err
	}

	gone, dropped, err := s.DropExpired(now, func(sess store.Session) bool { return owed[sess.Client] })
	expired = len(gone) + len(owed)
	b.counted.sessionsExpiredWhileStopped.Add(uint64(expired))
	// **And what they were owed from the broadcast log**, which this start
	// counted for every session the store held (rebuild): left, a list pins
	// its messages in the log and in the gauges until its client id connects
	// again. After the store's ending rather than before (endStoredSession),
	// since only the store knows which expired, and no drain write can land
	// after it: both stores refuse one to a session they do not hold, in the
	// same step, and nothing writes these records again.
	// What their groups had handed them is settled from the store's split of
	// it, and the groups whose last member expired end here.
	if d := b.broadcastDrain(); d != nil {
		var hs []handed
		for _, id := range gone {
			hs = append(hs, d.end(id)...)
		}
		d.ended(hs, dropped, nil)
	}
	if err != nil {
		return expired, 0, err
	}
	all, err := s.All()
	if err != nil {
		return expired, 0, err
	}
	for _, sess := range all {
		if sess.ExpiryInterval != 0 {
			continue
		}
		// **Its Will too, on the same rule as an expired one**, and in
		// practice there is never one to take: a session ending with its
		// connection ends at the disconnect, so its Will is published then
		// and its record goes with it. What reaches here is a record a crash
		// left behind, whose client was connected at the moment - and such a
		// Will carries no due moment, so oweWill discards it. Asked anyway,
		// because "this cannot happen" is how the one case that can is lost.
		if b.oweWill(sess, "its session ended with a connection the stop cut") {
			ended++
			continue
		}
		if err := b.endStoredSession(s, sess.Client, nil); err != nil {
			return expired, ended, err
		}
		ended++
	}
	return expired, ended, nil
}

// RestoreSessions puts back the sessions the store kept while the broker was
// stopped. Each becomes a client this broker knows and nothing is connected
// to, holding the subscriptions it had, so the client that comes back is
// answered Session Present 1 by the same path that answers one whose
// connection merely dropped. What it is owed is the broadcast log's, which
// StartBroadcast has already counted for it, and it is sent that on its
// return (bdrain.resumed).
//
// **Called once, after DropSessionsLeftByAStop and before any listener
// opens.** That call removes the sessions no client can have any more, so
// what is left here is exactly what may come back; and nothing can be
// connecting to an id while it is being built.
//
// **What is restored is what a disconnected session holds, and no more.** Its
// subscriptions go back into the substrate's index, so a broadcast published
// while it is still away is held for it; its partition declarations go back
// into saguin's, because nothing on the wire carries them again and a member
// that came back undeclared would be served every other member's slice
// (RFC 0003 "Client-declared partitioning"). What saguin's own channel index
// holds is not restored, because a disconnected session has none: a resume
// rebuilds it from the session's subscriptions (OnSessionEstablished), which
// is the same path a restored one takes.
//
// A store that cannot be read stops the start, rather than sessions being
// restored empty or left out: invariant 14 makes it an error naming what was
// wrong.
func (b *Broker) RestoreSessions() (sessions int, err error) {
	s := b.sessionStore()
	if s == nil || b.srv == nil {
		return 0, nil
	}
	now := time.Now()
	all, err := s.All()
	if err != nil {
		return 0, err
	}
	// The exactly-once publishes waiting for their release, by session. A
	// session that comes back takes its own with it; what is left when every
	// session has been judged belongs to nobody and is dropped below.
	unreleased, err := b.unreleasedBySession()
	if err != nil {
		return 0, err
	}
	// Ended by this start, and kept only until their Wills are published.
	ending := b.endingAtStart()
	for _, sess := range all {
		if ending[sess.Client] {
			continue
		}
		if why, filter := b.refusedNow(sess); why != "" {
			// **The session goes, not the subscription.** A session restored
			// without one of its filters is a client that believes it is
			// subscribed to something the broker will never send it, and
			// nothing on the wire would ever say otherwise: MQTT has no way
			// to tell a resuming session that one of its subscriptions is
			// gone. Ended here, its client comes back to Session Present 0,
			// re-subscribes, and is answered for each filter - including the
			// refusal, with its reason.
			b.log.Warn("ended a session at startup: it holds a subscription this broker refuses",
				"client", b.limits.Loggable(sess.Client), "filter", b.limits.Loggable(filter),
				"reason", b.limits.Loggable(why))
			// **Its Will is owed before its session goes.** Ending a session
			// is what makes a waiting Will due, and this start is ending it -
			// so a device that died, waited out its delay and is now refused
			// a subscription is still a device that died. Dropping the record
			// first would lose the announcement for a reason that has nothing
			// to do with it.
			if !b.oweWill(sess, "its session was ended at a start: it held a subscription this broker refuses") {
				if err := b.endStoredSession(s, sess.Client, nil); err != nil {
					return sessions, fmt.Errorf("end a session this broker refuses: %w", err)
				}
			}
			b.counted.sessionsRefusedAtStart.Add(1)
			continue
		}
		r := mqtt.RestoredSession{
			ClientID:       sess.Client,
			ExpiryInterval: sess.ExpiryInterval,
			DisconnectedAt: sess.DisconnectedAt,
		}
		for _, sub := range sess.Subscriptions {
			r.Subscriptions = append(r.Subscriptions, packets.Subscription{
				Filter: sub.Filter, Qos: sub.QoS, NoLocal: sub.NoLocal,
				RetainAsPublished: sub.RetainAsPublished, RetainHandling: sub.RetainHandling,
				Identifier: sub.Identifier,
			})
		}
		r.Receiving = unreleased[sess.Client]
		delete(unreleased, sess.Client)
		cl := b.srv.RestoreSession(r)
		b.restoreWill(s, sess, now)
		for _, sub := range sess.Subscriptions {
			if sub.PartitionCount > 0 {
				b.declare(cl.ID, sub.Filter, partition{
					count:   sub.PartitionCount,
					indices: append([]int(nil), sub.PartitionIndices...),
				})
			}
		}
		sessions++
		b.counted.sessionsRestored.Add(1)
	}
	// **What no restored session owns cannot be completed by anybody.** Its
	// publisher was told the message was taken in - a PUBREC - and the
	// session that would have released it is gone, so the message is
	// abandoned here rather than left to age out of a provider's bytes. It is
	// counted where every other abandoned exchange is counted
	// (saguin_qos2_abandoned_total): the publisher is told nothing, because
	// there is nobody to tell.
	for client, ids := range unreleased {
		n, refused := b.dropHeldOf(client)
		if refused {
			b.oweEnding(client, nil, "its session did not come back", false, nil, true)
		}
		b.counted.qos2Abandoned.Add(uint64(n))
		b.log.Info("dropped unreleased exactly-once publishes at startup: their session did not come back",
			"client", b.limits.Loggable(client), "publishes", len(ids))
	}
	return sessions, nil
}

// unreleasedBySession is the exactly-once publishes waiting for their
// release, by the session that sent each. Read once at the start, where each
// is decided with the session it belongs to.
//
// **It is also where the held maps come from**: every channel store is asked
// what it holds, and the maps say where each exchange is and how many each
// client has (holdForRelease). A held row naming a channel the configuration
// no longer has, in a provider this broker opened for others, is deleted
// and counted here: nothing could release it, and it would take that
// provider's room for good. A provider no channel names any more is not
// opened at all, so what it held goes with it unread.
func (b *Broker) unreleasedBySession() (map[string][]uint16, error) {
	type holder struct {
		channel string
		h       HoldStore
	}
	b.mu.Lock()
	var all []holder
	configured := map[string][]string{}
	for name, c := range b.reg.All() {
		configured[c.Storage] = append(configured[c.Storage], name)
		if h := b.holdsForLocked(name); h != nil {
			all = append(all, holder{name, h})
		}
	}
	// **And the broadcast log's**, which holds a QoS 2 broadcast for its
	// release as a channel's store holds a channel's publish. It is kept on
	// the session provider's list, since that provider's file holds it: left
	// off, every start would delete the held broadcasts as a removed
	// channel's.
	if h := b.holdsForLocked(store.BroadcastLog); h != nil {
		all = append(all, holder{store.BroadcastLog, h})
		configured[b.sessionsProvider] = append(configured[b.sessionsProvider], store.BroadcastLog)
	}
	providers := make(map[string]ReaderDropper, len(b.providers))
	for name, d := range b.providers {
		providers[name] = d
	}
	b.mu.Unlock()

	for provider, d := range providers {
		if c, ok := d.(countingDropper); ok {
			d = c.ReaderDropper
		}
		pruner, ok := d.(heldPruner)
		if !ok {
			continue
		}
		n, err := pruner.DropHeldExcept(configured[provider])
		if err != nil {
			return nil, fmt.Errorf("drop the exactly-once publishes held for channels no longer configured: %w", err)
		}
		if n > 0 {
			b.counted.qos2Abandoned.Add(uint64(n))
			b.log.Info("dropped unreleased exactly-once publishes at startup: their channel is no longer configured",
				"provider", provider, "publishes", n)
		}
	}

	held := map[store.Exchange]heldExchange{}
	by := map[string]int{}
	out := map[string][]uint16{}
	for _, c := range all {
		hs, err := c.h.Holds()
		if err != nil {
			return nil, fmt.Errorf("the unreleased publishes channel %q holds: %w", c.channel, err)
		}
		for _, p := range hs {
			held[p.Exchange] = heldExchange{channel: c.channel, at: p.HeldAt,
				retain: c.channel == store.BroadcastLog && p.Record.Retain}
			by[p.Exchange.Client]++
			out[p.Exchange.Client] = append(out[p.Exchange.Client], p.Exchange.PacketID)
		}
	}
	b.heldMu.Lock()
	b.held, b.heldBy = held, by
	b.heldMu.Unlock()
	return out, nil
}

// heldPruner is a provider that can delete, in one call, the held publishes
// of every channel not named: a sqlite provider, whose held rows share one
// table across every channel in the file.
type heldPruner interface {
	DropHeldExcept(channels []string) (int, error)
}

// refusedNow reports why the rules this broker started with refuse a
// subscription the session holds, and which filter, or "" where they refuse
// none.
//
// **Asked of what a filter is, never of who holds it.** The checks that need
// a client - what its roles deny, what its protocol version admits - are
// asked of a connection, and a session has none until one arrives; they are
// asked then, as they are of any other connection. What is asked here is the
// half that depends on this broker's configuration rather than on the client,
// and the case it exists for is a channel that has become a queue while the
// broker was stopped: a queue's records reach a worker and nobody else
// (invariant 4), so a session subscribed to its topics from before must not
// come back holding that subscription.
func (b *Broker) refusedNow(sess store.Session) (why, filter string) {
	for _, sub := range sess.Subscriptions {
		switch {
		case filterMalformed(sub.Filter) != "":
			return filterMalformed(sub.Filter), sub.Filter
		case sub.PartitionCount > 0 && partitionRefusal(sub.Filter) != "":
			return partitionRefusal(sub.Filter), sub.Filter
		case channel.MisspelledShare(sub.Filter):
			return "$share is spelled in lower case and no other", sub.Filter
		case channel.ShareMalformed(sub.Filter) != "":
			return channel.ShareMalformed(sub.Filter), sub.Filter
		}
		if _, refused := b.reg.QueueSubscriptionError(sub.Filter, sub.QoS); refused != "" {
			return refused, sub.Filter
		}
	}
	return "", ""
}

// grantedExpiry is the Session Expiry Interval a session is kept with: what
// the client asked for, capped by limits.max_session_expiry, and the cap
// itself for a 3.1.1 session, which states none. Zero for a session that ends
// with its connection.
func (b *Broker) grantedExpiry(cl *mqtt.Client) uint32 {
	if !persistentSession(cl) {
		return 0
	}
	expiry := cl.Properties.Props.SessionExpiryInterval
	if max := uint32(b.limits.MaxSessionExpiry); legacyClient(cl) || expiry > max {
		expiry = max
	}
	return expiry
}

// keepSubscriptions asks the session store to keep a SUBSCRIBE's filters,
// before the substrate applies it, and refuses them where it cannot.
//
// What is saved is the client's whole set as it will be: the substrate's
// current subscriptions with these filters added or replaced. **One save for
// the packet, so nothing is half kept**: where the store has no room every
// filter the packet would add or change is answered `0x97` and the session
// stays exactly as it was - and because a refused filter is one the substrate
// does not apply, an existing subscription the client tried to change goes on
// working as it was. A failure that is not a full store answers `0x83`.
//
// A session with no record yet - one that ends with its connection, or one
// the store had no room for at CONNECT - gets one here, with an interval of
// zero, since its subscriptions are state it holds while connected.
//
// Reports whether any filter was refused.
func (b *Broker) keepSubscriptions(cl *mqtt.Client, pk *packets.Packet, codes []byte) bool {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return false
	}
	var adding []packets.Subscription
	var at []int
	for i, f := range pk.Filters {
		if i < len(codes) && codes[i] < packets.ErrUnspecifiedError.Code {
			adding = append(adding, f)
			at = append(at, i)
		}
	}
	if len(adding) == 0 {
		return false
	}

	// Where each new filter was made is recorded as the record is built
	// (bdrain.since), and forgotten again if the write is refused: the
	// filter was never made, and subscribing to it later is new then.
	var restore func()
	if d := b.broadcastDrain(); d != nil {
		restore = d.keepSinces(cl.ID)
	}
	sess, ok, err := b.getSession(s, cl.ID)
	if err == nil {
		if !ok {
			sess = store.Session{Client: cl.ID, ExpiryInterval: b.grantedExpiry(cl)}
		}
		declared, _ := partitioning(*pk)
		ident := 0
		if len(pk.Properties.SubscriptionIdentifier) > 0 {
			ident = pk.Properties.SubscriptionIdentifier[0]
		}
		sess.Subscriptions = b.sessionSubscriptions(cl, adding, declared, ident, nil)
		err = b.saveSubscribing(s, cl, sess, adding)
	}
	if err == nil {
		return false
	}
	if restore != nil {
		restore()
	}

	b.mu.Lock()
	provider := b.sessionsProvider
	b.mu.Unlock()
	code := packets.ErrQuotaExceeded
	why := "the session store is full"
	if !errors.Is(err, store.ErrFull) {
		code = packets.ErrImplementationSpecificError
		why = "the session store could not keep the subscription"
		b.log.Error("cannot keep a subscription in the session store",
			"client", b.limits.Loggable(cl.ID), "provider", provider, "error", err)
	} else {
		b.log.Warn("refused a subscription: the session store is full",
			"client", b.limits.Loggable(cl.ID), "provider", provider)
	}
	for _, i := range at {
		codes[i] = code.Code
	}
	refusedBecause(pk, why)
	return true
}

// saveSubscribing keeps a SUBSCRIBE's record and, for a member whose session
// outlives its connection, the cursor of each shared group it joins that has
// none, in one store write (SaveWithShareCursors): **a cursor the store
// refuses refuses the record with it**, and so every filter the packet adds
// (keepSubscriptions). Kept apart, a refused cursor was a SUBACK of success
// while the group kept nothing, and a crash lost a publish its publisher was
// told was received; and refusing the SUBACK after the record was kept left
// the refused filter in it, the client's again after a restart.
func (b *Broker) saveSubscribing(s SessionStore, cl *mqtt.Client, sess store.Session,
	adding []packets.Subscription) error {
	d := b.broadcastDrain()
	var groups []string
	if d != nil && persistentSession(cl) {
		for _, f := range adding {
			if isShareFilter(f.Filter) {
				groups = append(groups, f.Filter)
			}
		}
	}
	if len(groups) == 0 {
		return b.saveSession(s, sess)
	}
	// **Every one, whether it has a cursor or not**: which have one is
	// decided under the gate (createGroups), with no ending of one between
	// that and this write. Decided before it, the last member leaving could
	// end a cursor this record was saved as joining, and the member was told
	// it was subscribed while its group kept nothing.
	return d.createGroups(groups, func(boundary uint64, missing []string) (int, error) {
		if len(missing) == 0 {
			return 0, b.saveSession(s, sess)
		}
		if err := b.saveWithCursors(s, sess, boundary, missing); err != nil {
			return 0, err
		}
		return len(missing), nil
	})
}

// saveWithCursors is saveSession with the cursors of groups kept in the same
// write, at boundary (SaveWithShareCursors).
func (b *Broker) saveWithCursors(s SessionStore, sess store.Session, boundary uint64, groups []string) error {
	if err := b.settleRecord(s, sess.Client); err != nil {
		return err
	}
	b.mu.Lock()
	provider := b.sessionsProvider
	b.mu.Unlock()
	need := store.SessionSize(sess)
	for _, g := range groups {
		need += store.ShareCursorSize(g)
	}
	return b.withRoom(provider, need, "the session store", func() error {
		return s.SaveWithShareCursors(sess, boundary, groups)
	})
}

// repairGroups gives a returning durable member's shared groups that have no
// cursor one, before the CONNECT claims anything: a member is never served
// while its group keeps nothing for it (RFC 0003 "Sessions"). Its SUBSCRIBE
// and every start make the cursor with the record (saveSubscribing,
// repairCursors), so this finds one missing only where something else ended
// it. **The cursor alone is written, one group at a time**, each given its
// list once its own cursor is kept: the record is the session's owner's to
// write, and this CONNECT does not own it yet (invariant 17). Nor is anything
// done for a CONNECT that begins a new session (beginsNew): the groups are
// the session it ends, which is not this connection's. Its error refuses the
// CONNECT.
func (b *Broker) repairGroups(cl *mqtt.Client, pk packets.Packet) error {
	d := b.broadcastDrain()
	if d == nil || b.srv == nil || cl.Net.Inline || !persistentSession(cl) || b.beginsNew(cl, pk) {
		return nil
	}
	// The session it resumes, as the substrate holds it: a CONNECT for an
	// id with none begins one, and reads nothing of the store.
	existing, held := b.srv.Clients.Get(cl.ID)
	if !held || existing == cl {
		return nil
	}
	groups := shareFiltersOf(existing)
	if len(groups) == 0 {
		return nil
	}
	// Which have none is decided under the gate, as a SUBSCRIBE decides it.
	return d.createGroups(groups, func(boundary uint64, missing []string) (int, error) {
		if len(missing) > 0 {
			b.log.Warn("a returning member's shared group has no cursor: it is given one",
				"client", b.limits.Loggable(cl.ID), "groups", len(missing))
		}
		for i, g := range missing {
			err := b.withRoom(d.provider, store.ShareCursorSize(g), "the session store", func() error {
				return d.st.CreateShareCursor(g, boundary)
			})
			if err != nil {
				return i, err
			}
		}
		return len(missing), nil
	})
}

// keepUnsubscribe stores a client's UNSUBSCRIBE before the substrate applies
// it: the session's record without the filters it gives up.
//
// **Stored first, because the UNSUBACK says they are gone** (invariant 18).
// Written after, from OnUnsubscribed, a write the store refused was logged and
// the UNSUBACK said Success all the same: after a crash the subscription was
// back, and the client was sent what it had unsubscribed from. So a filter
// that cannot be forgotten is refused `0x83` - the substrate leaves it
// subscribed, and a 3.1.1 client, whose UNSUBACK has no code, is disconnected
// instead - and the client is told what is true: it is still subscribed.
//
// A session with no record keeps no subscriptions, and a filter it does not
// hold changes nothing, so neither is a write. A record that cannot be read
// cannot be written without what it held, and is refused the same way.
func (b *Broker) keepUnsubscribe(cl *mqtt.Client, pk *packets.Packet) {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline || len(pk.Filters) == 0 {
		return
	}
	// Only what nothing has refused: the engine hands its own refusal in
	// (an identifier in use), and a filter it will answer so stays.
	decided := len(pk.ReasonCodes) == len(pk.Filters)
	gone := make(map[string]bool, len(pk.Filters))
	for i, f := range pk.Filters {
		if decided && pk.ReasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			continue
		}
		gone[f.Filter] = true
	}
	if len(gone) == 0 {
		return
	}
	sess, ok, err := b.getSession(s, cl.ID)
	if err == nil && ok {
		held := false
		for _, ss := range sess.Subscriptions {
			held = held || gone[ss.Filter]
		}
		if !held {
			return
		}
		// Where each filter was made is forgotten with it (bdrain.since),
		// and remembered again if the write is refused: the filter stays.
		var restore func()
		if d := b.broadcastDrain(); d != nil {
			restore = d.keepSinces(cl.ID)
		}
		sess.Subscriptions = b.sessionSubscriptions(cl, nil, partition{}, 0, gone)
		err = b.saveSession(s, sess)
		if err != nil && restore != nil {
			restore()
		}
	}
	if err == nil {
		return
	}
	b.log.Error("cannot forget a subscription in the session store: the unsubscribe is refused 0x83, "+
		"and the client stays subscribed", "client", b.limits.Loggable(cl.ID), "error", err)
	codes := make([]byte, len(pk.Filters))
	for i, f := range pk.Filters {
		if gone[f.Filter] {
			codes[i] = packets.ErrImplementationSpecificError.Code
		} else {
			codes[i] = pk.ReasonCodes[i]
		}
	}
	pk.ReasonCodes = codes
}

// syncSessionSubscriptions rewrites what the store keeps of a client's
// subscriptions from what the substrate actually holds, after the engine
// unsubscribes a session it is discarding - which tells its client nothing,
// the session ending with it. It only ever removes or keeps, so it cannot be
// what fills a store; a failure is logged.
func (b *Broker) syncSessionSubscriptions(cl *mqtt.Client) {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return
	}
	sess, ok, err := b.getSession(s, cl.ID)
	if err != nil {
		b.log.Error("cannot read a session to update its subscriptions", "client", b.limits.Loggable(cl.ID), "error", err)
		return
	}
	if !ok {
		return
	}
	sess.Subscriptions = b.sessionSubscriptions(cl, nil, partition{}, 0, nil)
	if err := b.saveSession(s, sess); err != nil {
		b.log.Error("cannot update a session's subscriptions", "client", b.limits.Loggable(cl.ID), "error", err)
	}
}

// sessionSubscriptions is what the store keeps of a client's subscriptions:
// the substrate's own set, with `adding` added or replacing a filter of the
// same name. Options come from the substrate's subscription; the partition
// declaration, which the substrate does not keep, from saguin's own records,
// or from the SUBSCRIBE being added. Sorted by filter, so one set is always
// saved the same way.
//
// **A request for deletions is not kept**: a resumed session asks for them
// again (OnSessionEstablished has why), so a stored one would be read by
// nothing.
func (b *Broker) sessionSubscriptions(cl *mqtt.Client, adding []packets.Subscription,
	declared partition, ident int, gone map[string]bool) []store.SessionSubscription {
	b.mu.Lock()
	parts := map[string]partition{}
	for filter, p := range b.partitions[cl.ID] {
		parts[filter] = p
	}
	b.mu.Unlock()

	byFilter := map[string]store.SessionSubscription{}
	put := func(f packets.Subscription, p partition, id int) {
		if id == 0 {
			id = f.Identifier
		}
		ss := store.SessionSubscription{
			Filter: f.Filter, QoS: f.Qos, NoLocal: f.NoLocal,
			RetainAsPublished: f.RetainAsPublished, RetainHandling: f.RetainHandling,
			Identifier: id,
		}
		if p.declared() {
			ss.PartitionCount = p.count
			ss.PartitionIndices = append([]int(nil), p.indices...)
		}
		byFilter[f.Filter] = ss
	}
	cl.State.Subscriptions.Each(func(_ string, f packets.Subscription) {
		put(f, parts[f.Filter], 0)
	})
	for _, f := range adding {
		put(f, declared, ident)
	}
	// Less what an UNSUBSCRIBE gives up, before the substrate has
	// (keepUnsubscribe).
	for f := range gone {
		delete(byFilter, f)
	}
	// Where the broadcast log was when each was made, so that a start owes a
	// session only what its subscriptions matched when it was published (RFC
	// 0003 "Broadcast"): kept for a filter the session held, and the log's
	// place now for one it did not (bdrain.since).
	if d := b.broadcastDrain(); d != nil {
		filters := make([]string, 0, len(byFilter))
		for f := range byFilter {
			filters = append(filters, f)
		}
		since := d.since(cl.ID, filters)
		for f, ss := range byFilter {
			ss.Since = since[f]
			byFilter[f] = ss
		}
	}
	out := make([]store.SessionSubscription, 0, len(byFilter))
	for _, ss := range byFilter {
		out = append(out, ss)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Filter < out[j].Filter })
	return out
}

// sessionWasNotKept reports whether this connection was told its session ends
// with it because the store had no room.
func (b *Broker) sessionWasNotKept(cl *mqtt.Client) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessionNotKept[cl]
}

// recordDisconnect keeps what the store knows about a session current when its
// connection ends and the session does not: it is kept with the moment its
// client went away, which is what its expiry is measured from. A session that
// ends with its connection is endSession's.
//
// **One write, with nothing read first** (SessionStore.Disconnected). It was
// a Get and then a Save, and a Get that failed returned before anything was
// written: the record went on saying its client was connected with its Will
// armed, and a Will a clean DISCONNECT had withdrawn was published when the
// session ended (MQTT-3.1.2-10). **A write that fails is remembered**
// (unwritten), so that ending does not publish it, and it is tried again every
// second until it lands (retryOwedWrites), and as the broker stops.
func (b *Broker) recordDisconnect(cl *mqtt.Client, fired bool) {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return
	}
	// **What the Will does now is decided here, because this is where the
	// wait begins.** A Will that has already fired, and one on a connection
	// that ended with a DISCONNECT, are gone: MQTT publishes no Will for a
	// client that said goodbye (MQTT-3.1.2-10), and one that fired fires
	// once, and holdWill has already stamped the moment a waiting one becomes
	// due on this same record.
	//
	// **A shutdown leaves it exactly where it is.** Closing the listeners
	// ends every connection, and each one arrives here - so clearing the Will
	// of a connection this broker is itself closing would take Will
	// protection away from the whole fleet at every stop, and give it back
	// only as each device happened to reconnect. What makes that Will due is
	// the client returning, which replaces it, or its session expiring at the
	// broker that comes back, which publishes it.
	//
	// **Except a Will this connection's own OnWill published**, which is gone
	// whether or not the broker has begun to stop since (fired). Reading
	// stopping here alone asked it a second time, after the teardown: a stop
	// beginning between OnWill's publish and this kept a published Will on
	// the record with no moment, and the next start published it again when
	// the session expired. And a Will a refused take-off left on the record
	// stays one to drop (takeStoredWill).
	b.mu.Lock()
	_, waiting := b.pendingWills[cl.ID]
	dropped := b.unwritten[cl.ID].dropWill
	b.mu.Unlock()
	// **With the expiry in force now, not the one the CONNECT was granted.**
	// A DISCONNECT may carry a new Session Expiry Interval (MQTT 5
	// 3.14.2.2.2), and the engine ends the session on it while this broker
	// runs; the record kept only the CONNECT's, so a start ended a session
	// its client had lengthened on the way out, with everything it was owed.
	d := unwrittenDisconnect{at: time.Now(), dropWill: fired || dropped || !waiting && !b.stopping.Load(),
		expiry: b.grantedExpiry(cl)}
	// **Not written twice**: what OnDisconnecting stored before the close
	// stands, unless the Will has fired since.
	b.mu.Lock()
	stored, before := b.disconnecting[cl]
	delete(b.disconnecting, cl)
	b.mu.Unlock()
	if before && stored.dropWill == d.dropWill && stored.expiry == d.expiry {
		return
	}
	if err := b.writeDisconnect(s, cl.ID, d); err != nil {
		b.log.Error("cannot record when a session's client went away; it is tried again every second until it lands",
			"client", b.limits.Loggable(cl.ID), "error", err)
	}
}

// OnDisconnecting stores what a client's DISCONNECT changes before the engine
// closes the connection that answers it: the moment its client went, its Will
// withdrawn where the DISCONNECT did not ask for it (0x04), and the expiry it
// carried. It runs holding the client id's session lock, so the owner asked
// here is the owner written for (invariant 17).
//
// **Before the close, because the close says it is done** (invariant 18).
// Written after it, from OnDisconnect once the read loop had returned, a
// crash in between published a Will a clean DISCONNECT had withdrawn, and
// kept a session for the CONNECT's expiry after its DISCONNECT had lowered
// it - or ended it, with 0. A session that ends with its connection and was
// never told otherwise is not written: a start ends it, and publishes no Will
// that carries no due moment, so a crash there costs nothing. Nor is a
// DISCONNECT asking for its Will and carrying no expiry: it changes nothing a
// crash would lose that an unclean end does not.
//
// **A write the store refuses does not hold the close.** A DISCONNECT has no
// answer to refuse it with, and its client closes the connection whether or
// not the broker does (MQTT-3.14.4-2), so holding the close would make nothing
// durable. The disconnect is remembered, as OnDisconnect's is
// (writeDisconnect), and written again until it lands.
func (b *Broker) OnDisconnecting(cl *mqtt.Client, pk packets.Packet) {
	b.storeBeforeDisconnectCloses(cl)
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return
	}
	withdraws := pk.ReasonCode != packets.CodeDisconnectWillMessage.Code
	expiry := b.grantedExpiry(cl)
	if !pk.Properties.SessionExpiryIntervalFlag && (expiry == 0 || !withdraws) {
		return
	}
	b.mu.Lock()
	owns := b.owner[cl.ID] == cl
	dropped := b.unwritten[cl.ID].dropWill
	b.mu.Unlock()
	if !owns {
		return
	}
	d := unwrittenDisconnect{at: time.Now(), dropWill: withdraws || dropped, expiry: expiry}
	if err := b.writeDisconnect(s, cl.ID, d); err != nil {
		b.log.Error("cannot record a client's DISCONNECT before its connection closes; "+
			"it is written again until it lands",
			"client", b.limits.Loggable(cl.ID), "error", err)
		return
	}
	b.mu.Lock()
	b.disconnecting[cl] = d
	b.mu.Unlock()
}

// unwrittenDisconnect is a disconnect the session store has not recorded:
// when the client went away, and whether its Will went with it - withdrawn by
// a clean DISCONNECT, or already published or cancelled by a broker that could
// not take it off the record (takeStoredWill).
type unwrittenDisconnect struct {
	at       time.Time
	dropWill bool
	expiry   uint32
}

// writeDisconnect records one, and remembers it where the store refuses. A
// session the store no longer holds has nothing left to record.
func (b *Broker) writeDisconnect(s SessionStore, id string, d unwrittenDisconnect) error {
	err := b.settleRecord(s, id)
	if err == nil {
		err = s.Disconnected(id, d.at, d.dropWill, d.expiry)
	}
	if errors.Is(err, store.ErrNoSession) {
		err = nil
	}
	b.mu.Lock()
	if err == nil {
		delete(b.unwritten, id)
	} else {
		b.unwritten[id] = d
	}
	b.mu.Unlock()
	return err
}

// forgetUnwritten drops what recordDisconnect remembered for an id, once its
// session is replaced or has ended: what was unwritten belonged to it.
func (b *Broker) forgetUnwritten(id string) {
	b.mu.Lock()
	delete(b.unwritten, id)
	b.mu.Unlock()
}

// willWithdrawn reports a Will the record still holds that is gone: withdrawn
// by a clean DISCONNECT, or published or cancelled already, where the store
// refused the write that said so (recordDisconnect, takeStoredWill).
func (b *Broker) willWithdrawn(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unwritten[id].dropWill
}

// owedLogEvery is how often retryOwedWrites says the store is still refusing
// what it owes: once every ten seconds of an outage, rather than once a pass.
const owedLogEvery = 10 * time.Second

// retryOwedWrites writes again, once a second while the broker runs, what the
// store refused and a client has already been told is done: a disconnect
// (recordDisconnect, OnDisconnecting) and an away session's acknowledgements of
// the broadcast log (bdrain.retryAway). Until now the first waited for the
// broker's stop and the second for the session's return, so a crash any time
// before them lost what the client was told - for as long as the broker ran
// (invariant 18).
//
// **Only for an id no connection owns** (invariant 17), asked under its
// session lock: a connection that has taken the id since wrote its own record
// when its CONNECT settled what was owed (settleOwed), and a write made now
// for the connection before it would be written over the new one's.
func (b *Broker) retryOwedWrites() {
	s := b.sessionStore()
	if s == nil {
		return
	}
	b.mu.Lock()
	ids := make(map[string]bool, len(b.unwritten)+len(b.owedRecords)+len(b.owedEndings))
	for id := range b.unwritten {
		ids[id] = true
	}
	for id := range b.owedRecords {
		ids[id] = true
	}
	for id := range b.owedEndings {
		ids[id] = true
	}
	b.mu.Unlock()
	// A count, and the last error the store answered with: the one is a
	// number and the other an error saguin or its store built, so neither
	// carries a string a client chose.
	refusedCount := 0
	var err error
	for id := range ids {
		if e := b.retryOwedOf(s, id); e != nil {
			refusedCount++
			err = e
		}
	}
	if d := b.broadcastDrain(); d != nil {
		n, e := d.retryAway()
		refusedCount += n
		if e != nil {
			err = e
		}
	}
	if refusedCount > 0 && time.Since(b.owedLogged) >= owedLogEvery {
		b.owedLogged = time.Now()
		b.log.Error("the session store still refuses what clients were told is stored; "+
			"it is written again every second, and a crash before it lands loses it",
			"writes", refusedCount, "error", err)
	}
}

// retryOwedOf is retryOwedWrites for one id, under its session lock: the
// record a failed connection wrote over, put back only while the id is held
// by the connection it was given back to, or by none; then the disconnect and
// what an ending left, only while no connection holds it.
func (b *Broker) retryOwedOf(s SessionStore, id string) error {
	unlock := b.lockSession(id)
	defer unlock()
	b.mu.Lock()
	rec := b.owedRecords[id]
	d, disconnect := b.unwritten[id]
	ending := b.owedEndings[id]
	owner, owned := b.owner[id]
	b.mu.Unlock()
	if rec != nil && (!owned || owner == rec.owner) {
		if err := b.settleRecord(s, id); err != nil {
			return err
		}
	}
	if owned {
		return nil
	}
	if disconnect {
		if err := b.writeDisconnect(s, id, d); err != nil {
			return err
		}
	}
	if ending != nil {
		return b.settleEnding(id)
	}
	return nil
}

// settleOwed writes what the store owes an id before a CONNECT for it goes
// on, answering what the store said: a disconnect it refused. A CONNECT is
// refused where it cannot be written (keepSession), as one is where an ending
// cannot (RFC 0003 "Sessions"): built on, the new session would begin from a
// record that says what its client was told is not so.
func (b *Broker) settleOwed(cl *mqtt.Client) error {
	s := b.sessionStore()
	if s == nil || cl.Net.Inline {
		return nil
	}
	id := cl.ID
	if err := b.settleRecord(s, id); err != nil {
		return err
	}
	b.mu.Lock()
	d, owed := b.unwritten[id]
	b.mu.Unlock()
	if owed {
		if err := b.writeDisconnect(s, id, d); err != nil {
			return err
		}
	}
	return b.settleEnding(id)
}

// owedRecord is a session record the store refused to put back after a
// connection that never completed wrote over it (settleClaim): the record, or
// none where the id had none, and the connection holding the id as it was
// given back, the only one whose session it is.
type owedRecord struct {
	rec   store.Session
	had   bool
	owner *mqtt.Client
}

// owedEnding is what of an ended session the store refused to drop: its
// positions (in the channels only names, or in every one where it is nil) and
// its unfinished exactly-once publishes. Left, the next session under the
// client id - told Session Present 0 - was resumed at the ended one's
// position, and a QoS 2 PUBLISH reusing an identifier the ended one held was
// answered from its exchange: PUBREC and PUBCOMP 0x00 for a message never
// kept, and the ended session's released in its place.
type owedEnding struct {
	cl        *mqtt.Client
	why       string
	positions bool
	only      map[string]bool
	holds     bool
}

// oweEnding remembers part of an ending the store refused.
func (b *Broker) oweEnding(id string, cl *mqtt.Client, why string, positions bool, only map[string]bool, holds bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.owedEndings[id]
	if e == nil {
		e = &owedEnding{why: why, only: map[string]bool{}}
		b.owedEndings[id] = e
	}
	if cl != nil {
		e.cl = cl
	}
	if positions {
		if only == nil || e.positions && e.only == nil {
			e.only = nil
		} else {
			if !e.positions {
				e.only = map[string]bool{}
			}
			maps.Copy(e.only, only)
		}
		e.positions = true
	}
	e.holds = e.holds || holds
}

// settleEnding finishes an ending the store refused part of, and answers an
// error while the store still refuses any of it. The caller holds the id's
// session lock, and no connection owns the id.
//
// **Through endSession, the one place a session ends** (RFC 0003
// "Sessions"). What is owed is part of an ending endSession began and the
// store refused, so it is finished by running that ending again, for the same
// connection and the same channels: whatever of it already went is gone and
// is removed again as nothing, and what the store still refuses endSession
// owes again (oweEnding). A second place that dropped only the positions
// would be a second list of what an ending removes, which the next piece
// added to endSession would not be on. Its record is dropped again too,
// which at worst removes what an ended session, or a CONNECT refused for
// this ending, left under the id - what a CONNECT beginning a new session
// would end anyway (keepSession).
//
// An ending with no connection is the start's own (RestoreSessions): a
// session that did not come back, whose unreleased publishes are all it left.
func (b *Broker) settleEnding(id string) error {
	b.mu.Lock()
	e := b.owedEndings[id]
	if e != nil {
		// endSession owes again what is still refused - unless another
		// connection holds the id, which it would leave untouched.
		if owner, held := b.owner[id]; held && e.cl != nil && owner != e.cl {
			b.mu.Unlock()
			return errOwedEnding
		}
		delete(b.owedEndings, id)
	}
	b.mu.Unlock()
	if e == nil {
		return nil
	}
	if e.cl != nil {
		b.endSession(e.cl, e.why, endedSession{retentionPassed: e.only})
	}
	if e.holds && e.cl == nil {
		n, refused := b.dropHeldOf(id)
		if n > 0 {
			b.counted.qos2Abandoned.Add(uint64(n))
		}
		if refused {
			b.oweEnding(id, nil, e.why, false, nil, true)
		}
	}
	if b.endingOwed(id) {
		return errOwedEnding
	}
	return nil
}

// errOwedEnding is settleEnding's answer while the store refuses what an
// ended session left.
var errOwedEnding = errors.New("saguin: the store still refuses to drop what an ended session left")

// settleRecord puts back a record owed to the store (owedRecord) before
// anything else writes the id's record, and answers the store's error while
// it refuses. The caller holds the id's session lock.
func (b *Broker) settleRecord(s SessionStore, id string) error {
	b.mu.Lock()
	e := b.owedRecords[id]
	b.mu.Unlock()
	if e == nil {
		return nil
	}
	var err error
	if e.had {
		err = s.Save(e.rec)
	} else {
		_, err = s.Drop(id, nil)
	}
	if err != nil {
		return err
	}
	b.forgetOwedRecord(id)
	return nil
}

// getSession reads an id's session record, putting back first a record owed
// to the store (settleRecord), so that nothing is built on the one a
// connection that never completed left there. A put-back the store refuses
// is answered as a read that failed.
func (b *Broker) getSession(s SessionStore, id string) (store.Session, bool, error) {
	if err := b.settleRecord(s, id); err != nil {
		return store.Session{}, false, err
	}
	return s.Get(id)
}

// forgetOwedRecord drops a record owed to the store, once the id's session
// has ended: what ends it is written in its place.
func (b *Broker) forgetOwedRecord(id string) {
	b.mu.Lock()
	delete(b.owedRecords, id)
	b.mu.Unlock()
}

// retryUnwrittenDisconnects writes, as the broker stops, the disconnects the
// store refused while it ran, so the next start reads each session as its
// client left it: gone at that moment, and its Will withdrawn if it was.
func (b *Broker) retryUnwrittenDisconnects() {
	s := b.sessionStore()
	if s == nil {
		return
	}
	b.mu.Lock()
	pending := make(map[string]unwrittenDisconnect, len(b.unwritten))
	maps.Copy(pending, b.unwritten)
	b.mu.Unlock()
	for id, d := range pending {
		if err := b.writeDisconnect(s, id, d); err != nil {
			b.log.Error("cannot record when a session's client went away, at the stop either: the next start "+
				"reads it as connected, and a Will its client withdrew, or one already published, can be "+
				"published when it ends",
				"client", b.limits.Loggable(id), "error", err)
		}
	}
}

// **The broker is a DeliveryKeeper**, which the broadcast log's drain answers:
// the engine tells it a QoS 2 delivery's PUBREC and a delivery's end other than
// by its acknowledgement, which the drain takes from OnQosComplete.
var _ mqtt.DeliveryKeeper = (*Broker)(nil)

// OnDeliveryReleased records that a QoS 2 delivery was received, so what the
// session owes is its PUBREL.
//
// **Waited for.** The PUBREL goes out after this, and the client answers it
// with PUBCOMP and forgets the identifier. A mark lost in a crash after that
// would have the session send the PUBLISH again after a restart, under an
// identifier the client has finished with - so it would take it as a new
// message: a QoS 2 message delivered twice. So a mark that cannot be stored
// is an error, and the engine sends no PUBREL (invariant 18). A delivery
// from the broadcast log keeps its exchange in the session's in-flight table
// (bdrain.received); any other is kept nowhere, and there is nothing to mark.
func (b *Broker) OnDeliveryReleased(cl *mqtt.Client, pk packets.Packet) error {
	if d := b.broadcastDrain(); d != nil {
		if mine, err := d.received(cl, pk); mine {
			return err
		}
	}
	return nil
}

// OnDeliveryDone lets go of a delivery from the broadcast log whose exchange
// ended in the engine's in-flight table - a PUBREC that refused it, or its
// connection's table cleared - as an acknowledged one is (bdrain.acked), and
// not for a connection its session was taken from.
func (b *Broker) OnDeliveryDone(cl *mqtt.Client, pk packets.Packet) {
	if d := b.broadcastDrain(); d != nil && releasedFor(cl, pk) {
		d.acked(cl, pk.PacketID)
	}
}

// releasedFor reports whether a delivery that has left cl's in-flight table
// is let go of by the session with it (OnDeliveryDone).
//
// **Not for a connection its session was taken from.** A resumed session's
// in-flight table is copied to the new connection and the old one's is then
// cleared, and those deliveries are still owed: the drain keys them by client
// id, so letting them go for the old connection would let them go for the new.
//
// **Except what its client acknowledged while the connection still owned
// it.** The engine retires an acknowledgement under the lock the takeover
// copies under, so the copy never carries it, and marks it Acknowledged
// because this runs after that lock is let go. Asked of IsTakenOver alone, a
// takeover landing in between kept the entry of a delivery nothing owed any
// more, and a restart sent it again.
func releasedFor(cl *mqtt.Client, pk packets.Packet) bool {
	return pk.Acknowledged || !cl.IsTakenOver()
}

// dropSessionRecord ends a session in the store, with its in-flight table.
// groups are the shared groups it held (endStoredSession). A refusal is
// logged and answered, for dropRecordAfterItsWill.
func (b *Broker) dropSessionRecord(id, why string, groups []string) error {
	s := b.sessionStore()
	if s == nil {
		return nil
	}
	err := b.endStoredSession(s, id, groups)
	if err != nil {
		b.log.Error("cannot end a session in the session store", "client", b.limits.Loggable(id), "why", why, "error", err)
	}
	return err
}

// dropRecordAfterItsWill is dropSessionRecord for a record whose Will has
// just been published: a running expiry's (OnClientExpired) and a start's
// (PublishDueWills), each of which publishes before the record goes.
//
// **A drop the store refuses is remembered, as a refused take-off is**
// (takeStoredWill). Kept as it stands, the record still holds the Will, and
// the next start would find it owed and publish it again. So it is an
// unwritten disconnect that drops the Will, at the record's own moment and
// expiry, which the stop writes (retryUnwrittenDisconnects), and the next
// start ends a session with no Will on it. A process that dies before the
// stop writes it publishes the Will again at the next start: the
// at-least-once RFC 0003 "Last Will" states for a crash, not a hole in this,
// which is about a refusal.
func (b *Broker) dropRecordAfterItsWill(id, why string, groups []string, at time.Time, expiry uint32) {
	if err := b.dropSessionRecord(id, why, groups); err != nil {
		b.mu.Lock()
		b.unwritten[id] = unwrittenDisconnect{at: at, dropWill: true, expiry: expiry}
		b.mu.Unlock()
	}
}

// endStoredSession ends a session in the store - its record, its in-flight
// table and its cursor - once the broadcast drain has let go
// of what it was owed and waited for any store write of its own already
// under way (bdrain.end). **Every Drop of a session is made here** but
// settleClaim's, which takes out a record written by a connection that never
// became the session, whose client id stays its predecessor's. The other way
// round, a drain write landing between the two was left under the id - at a
// clean start, in the in-flight table of the session that replaced it.
//
// **held is the shared groups the session held**, which its record may no
// longer say: a clean start writes the new session's record, holding
// nothing, before the one it replaces ends (confirmClaim). The store ends
// the cursor of each it was the last member of in the same transaction, and
// splits what its groups had handed it (store.Dropped), which the drain then
// settles (bdrain.ended).
func (b *Broker) endStoredSession(s SessionStore, id string, held []string) error {
	d := b.broadcastDrain()
	var hs []handed
	if d != nil {
		hs = d.end(id)
		// The cursors the store ends with it and the lists ended after, as
		// one step to a group's creation (bdrain.gate).
		d.gate.RLock()
		defer d.gate.RUnlock()
	}
	dropped, err := s.Drop(id, held)
	if d != nil {
		d.ended(hs, dropped, err)
	}
	return err
}

// beginSession ends whatever the store keeps under id and keeps next in its
// place, in one store write (SessionStore.Begin), with the drain let go of
// the id first and what the groups had handed it settled after, as
// endStoredSession does for an ending that keeps nothing.
//
// **Wherever a session begins new**: a clean start, a Clean Start 0 CONNECT
// for an id no session is held for, and a resume retention discarded. Each
// used to end what was kept and then save its own record - or, retention's,
// to keep the record and end nothing in the store - and an ending the store
// refused or never made left the ended session's in-flight table and cursor
// under the id: the new session's window full of entries it never had, so it
// was sent nothing, and after a restart it was sent them again as messages it
// was never owed. One write keeps both or neither.
func (b *Broker) beginSession(s SessionStore, id string, held []string, next *store.Session) error {
	d := b.broadcastDrain()
	var hs []handed
	if d != nil {
		hs = d.end(id)
		// As endStoredSession: the cursors Begin ends and their lists, one
		// step to a group's creation (bdrain.gate).
		d.gate.RLock()
		defer d.gate.RUnlock()
	}
	var dropped store.Dropped
	begin := func() (err error) {
		dropped, err = s.Begin(id, held, next)
		return err
	}
	var err error
	if next == nil {
		err = begin()
	} else {
		// The new record takes room as a save does, so the broadcast log
		// gives way to it as it does to a save (saveSession).
		b.mu.Lock()
		provider := b.sessionsProvider
		b.mu.Unlock()
		err = b.withRoom(provider, store.SessionSize(*next), "the session store", begin)
	}
	// A refused Begin is split in memory, as errEndedInMemory's ending is,
	// while the store keeps everything: what the groups handed the session
	// may be sent again, never lost.
	if d != nil {
		d.ended(hs, dropped, err)
	}
	return err
}

// endWithConnection makes a connection's session end when the connection
// does: expiry 0, so the substrate ends it at the disconnect, and marked not
// kept, so nothing of it is saved to be resumed. For a session the store has
// no room to keep, and for one whose clean start the store refused to begin.
func (b *Broker) endWithConnection(cl *mqtt.Client) {
	cl.Properties.Props.SessionExpiryInterval = 0
	cl.Properties.Props.SessionExpiryIntervalFlag = true
	cl.Properties.Clean = true
	b.mu.Lock()
	if b.sessionNotKept == nil {
		b.sessionNotKept = map[*mqtt.Client]bool{}
	}
	b.sessionNotKept[cl] = true
	b.mu.Unlock()
}

// claim is what a connection took over when it claimed a client id, kept
// until its CONNACK is written.
//
// **A claim is provisional because the connection is.** OnSessionEstablish
// runs before the substrate writes the CONNACK, and two things can happen
// after it in which the session under the id is still somebody else's: the
// hook refuses the connection itself (a Will the store cannot keep), or the
// CONNACK cannot be written - a device reconnecting over a link that drops
// again before the answer arrives. In both the substrate keeps the session it
// already held, live or away, and never takes it over. The second is then
// reported as a disconnect of a session ending, because nothing the failed
// connection wrote may outlive it.
//
// Taken as final, that claim tore the kept session down: its held
// exactly-once publish dropped and its PUBREL answered PUBCOMP 0x00 for a
// message no channel received, its place in every channel gone while it was
// still connected, its record deleted by a refused clean start, and a Will
// waiting out its delay cancelled by a device that never came back. So
// nothing that belongs to the session the id already has is changed before
// the CONNACK is out - what must wait, waits in here - and a connection that
// never gets one gives back what it took.
type claim struct {
	prev  *mqtt.Client // the id's owner before this connection, or nil
	clean bool         // the CONNECT begins a new session (beginsNew)

	// The record the CONNECT wrote over, put back if the connection never
	// becomes the session.
	record    store.Session
	hadRecord bool
	wrote     bool

	// A clean start's end of the session it replaces, which waits for the
	// CONNACK: the record and its in-flight table are dropped then, and the new
	// session's record written again after them when it keeps one.
	dropOnConfirm bool
	resave        *store.Session

	// The predecessor's own teardown, which found this claim holding the id,
	// and waits with it: run if the claim is given back.
	prevTeardown *pendingTeardown
	// And its delayed Will, which arrived first and waits the same way:
	// settled by confirmClaim, held again by settleClaim.
	prevWill *deferredWill
}

type pendingTeardown struct {
	err    error
	expire bool
}

// beginsNew reports whether a CONNECT begins a new session rather than
// resuming the one held for its client id: it asked for a clean start, or
// that session ends with the connection its takeover closes, which the
// engine answers Session Present 0 and inherits nothing from
// (mqtt.Client.EndsWithConnection). Asked under the id's session lock, before
// the takeover, so it is the engine's answer: a session begun new is ended
// here whole, as a clean start's is, and nothing of it is carried into the
// new one's record.
func (b *Broker) beginsNew(cl *mqtt.Client, pk packets.Packet) bool {
	if pk.Connect.Clean {
		return true
	}
	prev, held := b.srv.Clients.Get(cl.ID)
	return held && prev != cl && prev.EndsWithConnection()
}

// claimID records this connection as the owner of its client id, as a claim
// the CONNACK confirms.
//
// **A claim still pending from an earlier connection is one whose CONNACK was
// never written.** This hook runs under the id's session lock, which that
// connection held until its CONNACK had gone or failed, and a written one
// confirms its claim before the lock is let go. Its own disconnect may not
// have run yet, so it is settled here, before this connection reads what the
// id holds - or this one would take the failed connection's record for the
// session's.
func (b *Broker) claimID(cl *mqtt.Client, pk packets.Packet) *claim {
	b.mu.Lock()
	prior := b.owner[cl.ID]
	b.mu.Unlock()
	if prior != nil && prior != cl {
		b.settleClaim(prior, false)
	}
	b.mu.Lock()
	c := &claim{prev: b.owner[cl.ID], clean: b.beginsNew(cl, pk)}
	b.owner[cl.ID] = cl
	b.claims[cl] = c
	b.mu.Unlock()
	return c
}

// confirmClaim makes a connection's claim stand once its CONNACK is written,
// and does what had to wait for it. It runs from OnPacketSent, under the id's
// session lock, before the substrate takes the session over.
func (b *Broker) confirmClaim(cl *mqtt.Client) {
	b.claimMu.Lock()
	b.mu.Lock()
	c, ok := b.claims[cl]
	delete(b.claims, cl)
	b.mu.Unlock()
	b.claimMu.Unlock()
	if !ok {
		return
	}
	// A disconnect the store refused to record belonged to the connection
	// before this one: from its CONNACK the id is this connection's, and a
	// Will it arms is its own (recordDisconnect).
	b.forgetUnwritten(cl.ID)

	// **The previous session's Will is settled by a connection, and not by a
	// CONNECT**: one refused for its password is not the device coming back,
	// and cancelling on its word let anybody silence a device's death - and
	// nor is one whose CONNACK never arrived, which is why this waits for it.
	// A resume cancels a Will left waiting (endWaitingWill) and a clean start
	// publishes it, ending the session it replaces (endSession); a connection
	// still open when this one took its id has its Will decided when it
	// closes, by what is recorded here (willAtTakeover). A Will or a teardown
	// is left on a claim only once a failed CONNACK has let the id's session
	// lock go (settleClaim), and this connection has held that lock from its
	// claim to here, so both are nil on this path; prevWill is still settled
	// below rather than assumed away, because a Will dropped is silent.
	ended := c.clean
	if c.prev != nil && c.prev != cl {
		ended = ended || c.prev.EndsWithConnection()
		if c.prevTeardown == nil && c.prevWill == nil {
			b.mu.Lock()
			b.supersededWills[c.prev] = ended
			b.mu.Unlock()
		}
	}
	if w := c.prevWill; w != nil {
		if ended {
			b.publishWill(cl.ID, w.identity, "session_ended", w.will, w.props)
		} else {
			b.counted.willsCancelled.Add(1)
		}
	}
	// **A clean start ends the session it replaces**, in endSession as every
	// ending does - but not here. This runs as the CONNACK is written and
	// before the engine registers the connection, and on a database the
	// ending's store writes would hold every clean connect between the two.
	// What has to be read now is read now and handed on: the replaced
	// connection still holds its subscriptions, which the engine takes as it
	// takes over, and the timer waiting out its Will stops here, before it
	// can fire into the new session's record. OnSessionRegistered ends it.
	// The groups the replaced session held, read from its connection while
	// it still holds them: its record, in the store, is the new session's
	// by now (endStoredSession).
	var held []string
	if prev, ok := b.srv.Clients.Get(cl.ID); ok && prev != cl {
		held = shareFiltersOf(prev)
	}
	if c.clean {
		ended := b.endedByCleanStart(cl, c)
		b.mu.Lock()
		b.cleanStartEnds[cl] = ended
		b.mu.Unlock()
	} else {
		b.endWaitingWill(cl.ID, c)
	}

	// **The session it replaces ends, and this one's record is kept, in one
	// write** (beginSession). A refused ending used to be logged and the new
	// record saved over it, so the new session inherited the ended one's
	// in-flight table and cursor. The CONNACK is out by now, so a refusal is
	// answered by hanging up: nothing is inherited, and the client's next
	// CONNECT, finding no session held, begins it again (keepSession).
	//
	// **The hang-up ends the session, not only the connection.** hangUp ends
	// a connection, and the substrate keeps a session whose expiry is above
	// zero - so the next CONNECT with Clean Start 0, which a durable client's
	// library sends on every reconnect, resumed it with the ended session's
	// in-flight table under it, and a restart sent that session's messages to
	// the one that began new. Ending with its
	// connection, as a session the store had no room to keep does, the
	// substrate ends it at the disconnect and endSession asks the store to
	// end it again; if the store refuses that too, the next CONNECT still
	// finds no session held and begins one, whose Begin ends what the store
	// kept. What that leaves is a restart while the store is still refusing
	// every write: the record it kept is restored.
	if c.dropOnConfirm {
		if s := b.sessionStore(); s != nil {
			if err := b.beginSession(s, cl.ID, held, c.resave); err != nil {
				b.mu.Lock()
				provider := b.sessionsProvider
				b.mu.Unlock()
				b.log.Error("cannot end the session a clean start replaced: the connection is ended",
					"client", b.limits.Loggable(cl.ID), "provider", provider, "error", err)
				b.endWithConnection(cl)
				b.hangUp(cl, packets.ErrImplementationSpecificError)
			}
		}
	}

	// **What the resuming session is still holding and is no longer true
	// goes now, before the substrate clones it.** See dropSupersededLatest:
	// the clone at inheritClientSession is what carries the predecessor's
	// unacknowledged packets to this connection, and everything re-sent to
	// it comes from that copy. Taken out here, a superseded `latest` value
	// is never written, never owed and never allocated an identifier on
	// this connection - which is the difference between correcting the
	// client and crediting one value's acknowledgement to another. Here and
	// not at the claim: a predecessor that stays the session, because this
	// CONNACK was never written, keeps its table whole.
	//
	// The predecessor holds the table, so it is the client asked. Nothing
	// to do when there is none, which is every first connection.
	if !c.clean {
		if prev, held := b.srv.Clients.Get(cl.ID); held && prev != cl {
			b.dropSupersededLatest(prev)
		}
	}
}

// endedByCleanStart is the session a clean start ends, read from the claim
// that holds its record and from the connection it replaces - never from the
// store, which by now holds the new session's record (keepSession).
func (b *Broker) endedByCleanStart(cl *mqtt.Client, c *claim) endedSession {
	ended := endedSession{claimed: true}
	ended.record, ended.willTimerTaken = b.takeWaitingWill(cl.ID, c)
	if prev, held := b.srv.Clients.Get(cl.ID); held && prev != cl {
		ended.groups = shareFiltersOf(prev)
	}
	return ended
}

// takeWaitingWill stops the timer waiting out the ended session's Will, and
// returns that session's record as the claim holds it, with its Will kept
// only if a timer was waiting for it: the old session's client had gone and
// its delay was running, and the session ending makes it due. A connection
// still open when this one took its id has no timer - its Will is decided when
// it closes (willAtTakeover) - and passing the record's copy as well would
// publish it twice. The record is nil where the claim holds none.
//
// **The timer stops here, at the claim**, and not when the session is ended,
// because the claim has already written the new session's record under the
// id: a timer firing in between reads the id's record, and would take the new
// connection's Will off it and publish that.
func (b *Broker) takeWaitingWill(id string, c *claim) (*store.Session, bool) {
	b.mu.Lock()
	p, waiting := b.pendingWills[id]
	if waiting {
		delete(b.pendingWills, id)
	}
	b.mu.Unlock()
	if waiting {
		p.timer.Stop()
	}
	if !c.hadRecord {
		return nil, waiting
	}
	rec := c.record
	if !waiting {
		rec.Will = nil
	}
	return &rec, waiting
}

// settleClaim gives back what a connection claimed if its CONNACK was never
// written. It reports whether there was such a claim, and whether it was
// given back - to the session the substrate still holds under the id, or,
// for a connection refused by this broker's own hook, whatever the id held
// before.
//
// **A claim that nothing else holds the id against stands.** A first
// connection, or one whose predecessor it ended itself - a stored position
// retention had passed - replaced nobody the substrate kept, so the session
// under the id is this failed connection's and ends as any other does.
func (b *Broker) settleClaim(cl *mqtt.Client, refused bool) (pending, givenBack bool) {
	// Nearly every disconnect has no claim pending, so that is asked first,
	// before anything is read of the substrate.
	b.mu.Lock()
	_, pending = b.claims[cl]
	b.mu.Unlock()
	if !pending {
		return false, false
	}
	existing, held := b.srv.Clients.Get(cl.ID)
	kept := refused || (held && existing != cl)

	b.claimMu.Lock()
	b.mu.Lock()
	c, pending := b.claims[cl]
	if !pending || !kept {
		delete(b.claims, cl)
		b.mu.Unlock()
		b.claimMu.Unlock()
		return pending, false
	}
	delete(b.claims, cl)
	delete(b.willProps, cl)
	delete(b.sessionNotKept, cl)
	delete(b.subscriptionsCapped, cl)
	var teardown *pendingTeardown
	var deferred *deferredWill
	if b.owner[cl.ID] == cl {
		if c.prev != nil {
			b.owner[cl.ID] = c.prev
			deferred = c.prevWill
		} else {
			delete(b.owner, cl.ID)
		}
		teardown = c.prevTeardown
	}
	b.mu.Unlock()
	// The record goes back before the next connection can read it: claimID
	// settles under claimMu too, so it waits for this.
	if c.wrote {
		if s := b.sessionStore(); s != nil {
			var err error
			if c.hadRecord {
				err = s.Save(c.record)
			} else {
				_, err = s.Drop(cl.ID, nil)
			}
			if err != nil {
				// **Owed, and written again until it lands** (invariant 17):
				// the store holds what a connection that never owned the id
				// wrote, and a crash would restore it - a session told its
				// subscriptions were granted, and Session Present 1, served
				// from a record that has none of them. Every write of this
				// id's record puts it back first (settleRecord).
				b.mu.Lock()
				b.owedRecords[cl.ID] = &owedRecord{rec: c.record, had: c.hadRecord, owner: c.prev}
				b.mu.Unlock()
				b.log.Error("cannot put back the session a connection that never completed wrote over; "+
					"it is written again until it lands",
					"client", b.limits.Loggable(cl.ID), "error", err)
			}
		}
	}
	b.claimMu.Unlock()

	// The predecessor's delayed Will, which its connection left on this claim:
	// the id is its session's again, so it waits out its delay as it would
	// have. After the record is back, which is where it is read from.
	if deferred != nil {
		b.holdWill(c.prev.ID, deferred.will, deferred.delay)
	}

	// The predecessor's teardown, outside every lock this takes: it is the
	// whole of OnDisconnect's work on the id, and it may flush positions to a
	// store. Its held records went back before it found this claim. Every
	// caller of this holds the id's session lock, and a teardown taking it
	// again would wait for itself, so it is disconnectLocked.
	if teardown != nil {
		b.disconnectLocked(c.prev, teardown.err, teardown.expire)
	}
	return true, true
}

// OnSessionEstablish discards a session whose stored position retention has
// passed, so that the client is told rather than quietly moved forward.
//
// MQTT has no way to say "your position expired" mid-session, so saguin says
// it at the only moment the protocol provides: CONNACK with Session Present
// = 0 (RFC 0003 "Resuming below the retention floor"). The stored session is
// discarded, the client is told its session was not found, and it starts
// fresh at the channel's floor. Every client library already handles that
// signal, and every correctly written client already reads it as "I must
// resubscribe and I may have missed things" - which is exactly true.
//
// The alternative is what this replaces: serve the oldest surviving record
// as though nothing were missing. The consumer then processes the remainder
// in order and reports success over everything retention removed, and the
// only notice of it is a line in the broker's log, where the party that
// needs it is not looking. That is invariant 1's failure with the report
// going to the wrong party.
//
// **This is the last hook before mochi decides**, which is what makes it
// possible at all: mochi computes Session Present by asking whether it still
// holds a client under this id, so removing that client here means it finds
// nothing and answers 0. Everything below is what mochi's own clean-start
// branch does, less the unexported flag it sets - see the comment on the
// takeover case for what that costs and why it is safe.
func (b *Broker) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) error {
	// **The first hook an authenticated connection reaches**, so what a
	// CONNECT does to the broker starts here and not in OnConnect, which a
	// CONNECT that fails its password also reaches.

	// A Will's properties, **kept because the substrate is about to throw
	// them away**: its `mqtt.Will` has nowhere for a Will's Content Type,
	// Payload Format Indicator, Message Expiry Interval, Response Topic or
	// Correlation Data, so this is the last place they exist.
	//
	// **Keyed by the connection, not the client id.** A connection that
	// takes over an id arms its Will here, before the substrate hangs up the
	// one it supersedes - and that one's Will and teardown then run. Keyed by
	// id, the old connection's Will fired with the new one's properties and
	// its teardown dropped them, so the Will of the connection that replaced
	// it arrived bare. Keyed by the *mqtt.Client, each connection has only
	// its own, including a client the server had to name.
	if pk.Connect.WillFlag {
		b.mu.Lock()
		b.willProps[cl] = willPropsOf(pk)
		b.mu.Unlock()
	}

	// This connection now owns everything saguin keys by this client id, as
	// a claim its CONNACK confirms (claim has why).
	//
	// It is claimed here because this hook runs before the substrate
	// disconnects the connection being superseded, so the new owner is
	// recorded before the old teardown can possibly run - and a teardown
	// that finds it is not the owner deletes nothing. Claiming it any later
	// would leave the window this closes.
	//
	// A first connection has no predecessor and claims it just the same, so
	// the map always has an answer and forgetConsumer never has to guess
	// from an absent entry.
	// **What the store owes this id is written first** (settleOwed), under
	// the id's lock this CONNECT holds and before it claims anything, so
	// nothing retrying it later can write over the session this CONNECT
	// claims (invariant 17), and the session it claims is not built on what
	// its client was told is not so. Refused `0x83` while the store refuses,
	// as a CONNECT is whose ending the store refuses (RFC 0003 "Sessions").
	if err := b.settleOwed(cl); err != nil {
		b.mu.Lock()
		provider := b.sessionsProvider
		b.mu.Unlock()
		b.log.Error("cannot write what the store owes this client id from its last connection: "+
			"the connection is refused", "client", b.limits.Loggable(cl.ID), "provider", provider, "error", err)
		return codeSessionUnreadable
	}

	// **A returning member's groups have their cursors before it is claimed**
	// (repairGroups), and one the store cannot keep refuses the CONNECT:
	// `0x97` where the store is full, `0x83` otherwise. Accepted without it,
	// the member would be served while its group kept nothing.
	if err := b.repairGroups(cl, pk); err != nil {
		code := codeGroupUnkept
		if errors.Is(err, store.ErrFull) {
			code = packets.ErrQuotaExceeded
		}
		b.log.Error("cannot keep a returning member's shared group's cursor: the connection is refused",
			"client", b.limits.Loggable(cl.ID), "error", err)
		return code
	}

	c := b.claimID(cl, pk)

	// Before the substrate decides Session Present and writes the CONNACK, so
	// a session the store has no room for can still be told it ends with this
	// connection - and so that a client whose **Will** it has no room for is
	// refused rather than accepted, which is the difference between the two:
	// a session that ends with its connection has been told the truth, and a
	// client whose Will the broker does not hold has not. **A refusal gives
	// the claim back**: the session under the id is still whoever had it.
	if err := b.keepSession(cl, pk, c); err != nil {
		b.settleClaim(cl, true)
		return err
	}

	// Accepted: the password file has admitted it and the session store has
	// kept what it asked for, so this is RFC 0005's "accepted since start".
	// A CONNACK that then cannot be written is still an accepted connection
	// whose answer the network lost.
	b.counted.connections.Add(1)

	// **The shortening happens in the substrate rather than in a hook**, so
	// this counter is saguin's own arithmetic (RFC 0005). The client is told
	// in its CONNACK; this is what tells the operator, and nothing else
	// does - a fleet asking for a session that never expires and being given
	// a month is a thing to know before somebody asks why a device came back
	// to nothing.
	if asked := pk.Properties.SessionExpiryInterval; asked > uint32(b.limits.MaxSessionExpiry) {
		b.counted.sessionsShortened.Add(1)
	}

	// A session begun new has no stored position to have passed and nothing
	// to discard.
	if c.clean {
		return nil
	}

	gone := b.positionsBelowTheFloor(cl.ID)
	if len(gone) == 0 {
		return nil
	}

	// **The session retention passed ends here, whole, in endSession** - the
	// one place a session ends - and this is the table's one exception: only
	// the positions retention passed go, so the consumer restarts at the
	// floor rather than being handed its old offset on the next connect,
	// and a position below the floor is unreadable, so keeping it would
	// discard the session on every reconnect for ever. The positions it did
	// not pass are kept, since nothing their client had not read was removed
	// from them.
	//
	// **A Will still waiting on it is published first.** The session ends
	// here, and a Will is due when its session ends, whichever of that and
	// its delay comes first. Left out, as this path used to leave it, the
	// resume's confirmClaim then counted it cancelled - "the client
	// connected again" - and the device's death was never announced.
	ended := endedSession{retentionPassed: make(map[string]bool, len(gone)), claimed: true}
	ended.record, ended.willTimerTaken = b.takeWaitingWill(cl.ID, c)
	for name := range gone {
		ended.retentionPassed[name] = true
	}
	existing, held := b.srv.Clients.Get(cl.ID)
	if held && existing != cl {
		// The groups it was the last member of, while it still holds them:
		// the unsubscribe below takes them.
		ended.groups = shareFiltersOf(existing)
	}
	// **What the discarded session kept in the store ends here, and this
	// connection's record is kept in its place, in one write**
	// (beginSession), before anything else of it goes. The record stayed
	// and nothing ended its in-flight table or cursor, so the session that
	// came back to Session Present 0 was sent nothing into a window full of
	// entries it never had, and after a restart was sent them again. Its
	// record is the one keepSession has just written for this connection,
	// holding none of the discarded session's subscriptions.
	if s := b.sessionStore(); s != nil {
		var next *store.Session
		rec, kept, err := b.getSession(s, cl.ID)
		if err == nil && kept {
			rec.Subscriptions = nil
			next = &rec
		}
		if err == nil {
			err = b.beginSession(s, cl.ID, ended.groups, next)
		}
		if err != nil {
			b.mu.Lock()
			provider := b.sessionsProvider
			b.mu.Unlock()
			return b.sessionNotBegun(cl.ID, provider, err)
		}
	}
	if held && existing != cl {
		// What mochi does for a client that asked for a clean start, so that
		// the connection whose session is discarded goes with it.
		//
		// **Marked taken over, as that takeover marks it**, and only after
		// its unsubscribe: UnsubscribeClient skips a connection already
		// marked, and its hook is what empties the subscriptions the resume
		// carried into the new session's record. Left unmarked, a SUBSCRIBE
		// or UNSUBSCRIBE the old connection read before this and handled
		// after it passed the engine's check for a connection taken over,
		// and wrote its filters into the new session's record and the topic
		// index under the id.
		b.endConnection(existing, packets.ErrSessionTakenOver)
		b.srv.UnsubscribeClient(existing)
		existing.EndTakenOver()
		b.srv.Clients.DeleteIf(cl.ID, existing)
	}
	b.endSession(cl, "its session was discarded because retention passed its position", ended)
	// Refused rather than built on, as a clean start's is (OnSessionRegistered).
	if b.endingOwed(cl.ID) {
		b.mu.Lock()
		provider := b.sessionsProvider
		b.mu.Unlock()
		return b.sessionNotBegun(cl.ID, provider, errOwedEnding)
	}

	// **The accounting belongs to the loss, not to the session-discard
	// ceremony below it.** Every channel in `gone` has just had its stored
	// position dropped: those records are unreadable and this reader's claim
	// on them is gone, whatever the substrate turns out to hold.
	//
	// Whether there was a session to tear down is a different question, and
	// a stored position can outlive one.
	// `broker.session.storage` naming a memory provider while a channel sits
	// on a database is the pairing (RFC 0002), and a restart is the event:
	// the position survives, the session does not, and the reconnecting
	// device reaches here with nothing for mochi to report. Counted only
	// where there was a session, as it once was, the loss saguin exists to
	// report went to nobody - no line, no counter, and Session Present = 0
	// carrying no signal because there was no session to miss.
	//
	// So it is counted and said here, keyed off `gone`, which is the set
	// that lost something. Invariant 1 both ways: this consumer *is* told,
	// which the connected case cannot manage, and the operator is told how
	// many records went, which every case must.
	for name, at := range gone {
		if cc := b.counted.forChannel(name); cc != nil {
			cc.positionLost.Add(1)
		}
		// `reported` is true here, and that is the field an operator
		// triages on: this consumer is told with Session Present = 0, so it
		// knows to start fresh rather than carrying on over a hole.
		b.passed.add(store.MQTTReader(cl.ID), name, kindSession, true,
			at.position, at.floor, b.limits.Loggable)
		b.log.Warn("retention passed a stored position while its consumer was away; "+
			"it restarts at the floor and is told with Session Present = 0",
			"client", cl.ID, "channel", name, "position", at.position,
			"floor", at.floor, "records_missed", at.floor-at.position)
	}
	return nil
}

// OnSessionSuperseded answers for a connection a newer CONNECT for its
// client id superseded as it waited. It was accepted, with a CONNACK, as
// every connection counted in saguin_connections_total is, and claimed
// nothing.
//
// **Its Will is decided as a taken-over connection's** (OnWill,
// willAtTakeover), since that is what it would have been: published now if
// it has no delay, or if the CONNECT that would have taken its session asked
// for a clean start and so ended the session; dropped if that CONNECT
// resumed it inside the delay [MQTT-3.1.3-9]. Nothing is held, because the
// session it would have held on is the resuming connection's. Every check
// that can refuse a Will was made at its CONNECT, before it waited.
func (b *Broker) OnSessionSuperseded(cl *mqtt.Client, pk packets.Packet, successorClean bool) {
	b.counted.connections.Add(1)
	if !pk.Connect.WillFlag || b.stopping.Load() {
		return
	}
	will := cl.Properties.Will
	switch {
	case b.willDelay(cl, will) == 0:
		b.publishWill(cl.ID, identity(cl), "immediate", will, willPropsOf(pk))
	case successorClean:
		b.log.Info("publishing a Will: another connection took its client id with a clean start, "+
			"so its session ended before the delay did", "client", b.limits.Loggable(cl.ID),
			"topic", b.limits.Loggable(will.TopicName))
		b.publishWill(cl.ID, identity(cl), "session_ended", will, willPropsOf(pk))
	default:
		b.counted.willsCancelled.Add(1)
		b.log.Info("a Will was not held: another connection resumed its session inside the delay",
			"client", b.limits.Loggable(cl.ID))
	}
}

// OnSessionRegistered ends the session a clean start replaced, now the engine
// has registered this connection: what confirmClaim read of it is handed on
// (cleanStartEnds). It is still before any SUBSCRIBE, so no read can have used
// a position this drops, and the connection it replaced finds the id is not
// its own when its OnDisconnect has the lock, so it writes none back.
//
// **Here, under the engine's session lock, and not in OnSessionEstablished
// after it.** Ended once the lock was let go, a connection claiming the id in
// between - or claiming it and leaving, which empties the owner entry the
// ending's check reads as nobody - had its own stored positions and
// exactly-once publishes removed by the ending of the session before it: it
// resumed with Session Present 1 and was sent every record it had already
// acknowledged, and a publish it had been sent PUBREC for was answered PUBCOMP
// 0x00 and never reached its channel. After the registration rather than at
// the CONNACK (confirmClaim), because a clean connect would otherwise wait on
// these store writes between being answered and being registered.
func (b *Broker) OnSessionRegistered(cl *mqtt.Client, pk packets.Packet) {
	b.mu.Lock()
	ended, clean := b.cleanStartEnds[cl]
	delete(b.cleanStartEnds, cl)
	b.mu.Unlock()
	if clean {
		why := "the client asked for a clean start"
		if !pk.Connect.Clean {
			why = "its session ended with the connection taken over"
		}
		b.endSession(cl, why, ended)
		// **An ending the store refused is not built on** (RFC 0003
		// "Sessions"): this connection was told Session Present 0 and would
		// subscribe into the ended session's positions and exchanges. It is
		// ended with 0x83, and its session with it, so nothing is left for a
		// later drop of what is owed to take from, and the client's next
		// CONNECT drops what is owed before it is answered (settleOwed).
		if b.endingOwed(cl.ID) {
			b.log.Error("cannot drop what the session a clean start replaced left: the connection is ended",
				"client", b.limits.Loggable(cl.ID))
			b.endWithConnection(cl)
			b.hangUp(cl, packets.ErrImplementationSpecificError)
		}
	}
}

// endingOwed reports whether the store still owes an ending for id.
func (b *Broker) endingOwed(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.owedEndings[id] != nil
}

// OnSessionEstablished rebuilds saguin's subscription index for a resumed
// session.
//
// A client reconnecting with Clean Start = 0 has its subscriptions restored
// by the server, which fires no subscribe hook for them. Without this,
// saguin's index stays empty and the consumer is invisible to the broker:
// no replay and no live records either, with nothing reported anywhere.
// That is the reconnect every intermittent link depends on.
//
// The index is a cache of the server's subscriptions rather than a second
// record kept in parallel, which is what let the two drift apart.
func (b *Broker) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	// A session that outlives its connection is owed its broadcast from the
	// log, so the drain holds it (bdrain.attach). This runs after the CONNACK
	// and after the engine has let the client id's session lock go, but
	// before the read loop starts: the session is held before its first
	// SUBSCRIBE can be read, so no publish it subscribes to can miss it.
	//
	// A resumed one is then served what it is owed, what was on the wire
	// first (bdrain.resumed). Here since it starts a drain.
	if d := b.broadcastDrain(); d != nil {
		if persistentSession(cl) {
			d.attach(cl.ID)
		}
		if !pk.Connect.Clean {
			d.resumed(cl)
		}
	}
	restored := cl.State.Subscriptions.GetAll()
	if len(restored) == 0 {
		return
	}

	// **The subscription index is cleared and rebuilt below; what the
	// session declared is not.** This is a resume, so MQTT restores the
	// subscriptions and saguin rebuilds its own view of them from those -
	// but the partition properties arrived on a SUBSCRIBE packet that a
	// resuming client has no reason to send again, exactly as the comment
	// below says of user properties in general.
	//
	// **Deletions and partitioning make opposite choices here, and the
	// asymmetry is which way each fails.** A copy that must re-ask for
	// deletions and does not simply misses deletions - it is served less
	// than it wanted. A member that must re-declare and has not yet is
	// served *more* than it asked for: the whole channel, including every
	// other member's slice, for as long as the window lasts. One degrades
	// safely and the other duplicates records across a group.
	b.mu.Lock()
	b.forgetSubscriptions(cl.ID)
	b.mu.Unlock()

	resume := map[string]*channel.Channel{}
	for _, s := range restored {
		// A resumed session's stored subscriptions carry no *user*
		// properties: MQTT stores the filter and its options, not the
		// SUBSCRIBE that made it. So a copy asking for deletions asks again
		// when it reconnects, which it does - a bridge subscribes on every
		// connection.
		//
		// **The Subscription Identifier is not one of those**, and the
		// difference matters: it is an option of the subscription rather
		// than a property of the packet, the server stores it on the
		// subscription it keeps, and MQTT 5 requires it to survive a
		// resumed session. So it is restored here rather than waiting for a
		// SUBSCRIBE that a resuming client has no reason to send.
		// A filter reaching several channels resumes in every one of them,
		// which is why this is a map keyed by name: two restored filters
		// crossing one channel must not start two resumes of it.
		//
		// **Retain As Published is one of those options too**, so it is
		// restored from the stored subscription rather than defaulted off:
		// a subscriber that asked for the publisher's flag and then resumed
		// would otherwise start seeing 0 after a reconnect, with nothing it
		// did to explain the change.
		for _, c := range b.track(cl.ID, s.Filter, s.Qos, false, s.RetainAsPublished,
			s.NoLocal, s.Identifier) {
			if c.Type != channel.Queue {
				resume[c.Name] = c
			}
		}
	}
	for _, c := range resume {
		if c.Type == channel.Latest {
			// A session resumed without a new SUBSCRIBE is served only what
			// changed while it was away. MQTT says nothing about this
			// moment - a standard broker sends nothing at all here and
			// leaves the client stale until it subscribes again - and
			// sending everything costs a fleet with ten thousand state
			// topics ten thousand messages on every reconnect.
			since := b.latestSeen.resumeFrom(cl.ID, c.Name)
			// **Every restored filter, because this is a resume rather
			// than a subscribe.** There is no one filter that asked; the
			// session came back holding all of them.
			b.deliverLatest(cl, c, since, "")
			continue
		}
		b.pump(cl, c, nil)
	}
	// **The other way a member comes back.** A resumed session sends no
	// SUBSCRIBE, so the drain after the SUBACK never runs for it and the
	// group it belongs to would hold work for a member that is already
	// connected - until some other member happened to subscribe. Its groups
	// have their cursors by now (repairGroups), and are woken.
	b.wakeGroups(cl)
}

// OnDisconnect returns every record the departing worker held. saguin knows
// which records those are, so this needs no deadline to expire first
// (RFC 0003).
func (b *Broker) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	// Held records go back whatever else is true here: this connection is
	// gone, and a record in Delivering has no deadline to rescue it. The
	// worker can never acknowledge what it was holding, so the record is
	// invariant 7's stranded case and returns with its attempt unspent. A
	// session that was taken over may therefore see a job a second time,
	// which at-least-once permits and stranding it for ever does not.
	//
	// **This connection's, asked of the connection rather than of the id.**
	// A successor holds the same id, so `d.holder == cl.ID` answered yes
	// for a job the live worker had been handed since the takeover: it went
	// back while that worker was still doing it, the queue offered it
	// again, and invariant 3 then discarded whichever answer arrived second
	// as superseded. Two live deliveries of one record out of no
	// configuration and no client behaviour, which is invariant 4 - the
	// work done twice and one result thrown away, with nothing reporting a
	// problem. Measured on a live worker: offered `job-1`, a stale teardown
	// run against the id, and `job-1` offered to it again under a second
	// Delivery ID.
	//
	// Stopping at the ownership check instead would be the other failure.
	// A superseded connection's own deliveries are exactly the ones nobody
	// can rescue, so a teardown that returns nothing when it has lost the
	// id strands them until the session ends.
	b.mu.Lock()
	held := []*delivery{}
	for _, d := range b.deliveries {
		if d.conn == cl {
			held = append(held, d)
		}
	}
	b.mu.Unlock()

	for _, d := range held {
		b.releaseDelivery(d, "worker disconnected", false)
	}

	// **Everything else is keyed by client id, and is done holding the id's
	// session lock** (mqtt.Server.LockSession): the ownership checks below read
	// b.owner under b.mu, and the store writes after them run without it, so
	// only that lock keeps a connection from claiming the id in between. The
	// held records above are this connection's rather than the id's, so the
	// lock has nothing to guard in them - and returning one can dead-letter it
	// and hand it to that channel's consumers, which is not work to do while
	// another connection waits for the id.
	unlock := b.lockSession(cl.ID)
	defer unlock()
	b.disconnectLocked(cl, err, expire)
}

// disconnectLocked is OnDisconnect's work on what the client id holds, for a
// caller holding the id's session lock: OnDisconnect, and settleClaim running
// a predecessor's teardown under the lock of the connection that gave the id
// back.
func (b *Broker) disconnectLocked(cl *mqtt.Client, err error, expire bool) {
	// What confirmClaim recorded for this connection's Will is spent: the
	// engine asks OnWill before this, and a connection with no Will is never
	// asked (invariant 13).
	b.mu.Lock()
	delete(b.supersededWills, cl)
	delete(b.cleanStartEnds, cl)
	defer func() {
		// Whatever path the teardown took, what OnDisconnecting stored for
		// this connection is spent with it (invariant 13).
		b.mu.Lock()
		delete(b.disconnecting, cl)
		b.mu.Unlock()
	}()
	fired := b.willFired[cl]
	delete(b.willFired, cl)
	b.mu.Unlock()
	// **A connection whose CONNACK was never written is not the session**,
	// whatever the substrate says about expiring it: the substrate kept the
	// session it already held under this id, and the claim this connection
	// made on it goes back (claim). Nothing below is this connection's to
	// tear down.
	if _, givenBack := b.settleClaim(cl, false); givenBack {
		return
	}

	// The publish budget goes with the connection. A map holding one entry
	// per client id ever seen is unbounded state keyed by a string the
	// client chooses, which is the shape invariant 13 exists to refuse -
	// arriving, without this, through the mechanism meant to enforce it.
	//
	// **Keyed by client id and asked no owner, deliberately.** It is the
	// only one of those above the ownership guard that stays there, and it
	// stays because it deletes nothing: it stamps the budget as unheld, and
	// a successor's very next publish stamps it back - that stamp is what
	// makes a reconnect resume a budget rather than replace it. The sweep
	// that does delete takes an entry only once it has refilled, which is
	// once it says nothing a fresh one would not, so a superseded teardown
	// marking a live client's budget costs that client nothing. An
	// ownership check here would guard a write that cannot do harm, and
	// would read as though the ones below it were the same question.
	b.forgetAllowance(cl.ID)
	// And the Will properties of a client whose Will never fired - a clean
	// DISCONNECT, which MQTT says discards it. OnWill takes the entry when
	// one does fire, so this is the other half of the same bound rather
	// than a second owner of it.
	b.mu.Lock()
	delete(b.willProps, cl)
	delete(b.sessionNotKept, cl)
	delete(b.subscriptionsCapped, cl)
	// And a subscription's deliveries whose SUBACK never reached the wire.
	// OnPacketSent takes the entry when the write succeeds, so this is the
	// other half of the same bound: without it a failed SUBACK write leaves
	// a closure holding a client and its channels for as long as the broker
	// runs (invariant 13).
	//
	// **This connection's own, and asked of nobody**, because the entry is
	// keyed by the connection. While it was keyed by client id a stale
	// teardown deleted the catch-up a live session's SUBACK was about to be
	// given - the SUBACK said granted and nothing arrived, for ever, unless
	// the client subscribed again. Found by the link-churn soak: one `latest`
	// subscriber, once in ten minutes under -race across 217 cuts.
	delete(b.afterSuback, cl)
	b.mu.Unlock()

	// **The operator's line for a client hung up on by the write deadline,
	// and the only one.** Every route ends here, which is why it is here:
	// saguin writes append and latest deliveries itself, the substrate
	// writes a queue's offers and broadcast, and both call Stop with the
	// deadline error. Written in writeTo instead, it covered saguin's
	// writes only - so a queue worker was hung up on and an operator at
	// the default level was told nothing at all.
	//
	// The stop cause rather than err: err is whatever the read loop saw
	// once the connection went, which is an EOF or a closed socket, not
	// the reason it was closed.
	if isTimeout(cl.StopCause()) {
		b.log.Warn("disconnected a consumer that stopped reading",
			"client", b.limits.Loggable(cl.ID), "after", b.limits.WriteTimeout)
	}

	// Everything below is keyed by client id, and a new connection using the
	// same id has already inherited it. Tearing it down now would take the
	// live session's subscriptions and its place in every channel with it -
	// the consumer is told Session Present = 1 and then receives nothing,
	// for ever, with nothing said to it.
	//
	// **The question is asked of saguin's own ownership map, not of the
	// substrate.** Both substrate signals - IsTakenOver, and whether another
	// client now holds this id - are read before the lock protecting the
	// maps below is taken, and both are set at moments the substrate
	// chooses: a teardown reading them in the gap between its connection
	// being disconnected and being marked taken over passes both, and then
	// deletes what the new session installed while it was between the two.
	// Measured with a delay injected at exactly that point: 9 rounds in 10
	// failed, against 0 for the same build without it.
	//
	// **The id's session lock is what keeps this answer true below.** The
	// tail flushes positions to a store, which is I/O, and no path may hold
	// b.mu across that (RFC 0005), so the work cannot be one b.mu section -
	// and without the session lock this check went stale before the store
	// writes exactly as the substrate's signals do: a connection that claimed
	// the id in between, or claimed it and left, was torn down by this
	// teardown. forgetConsumer, endSession and the block below still ask again
	// under b.mu, for the callers that reach them without this check.
	b.mu.Lock()
	owner := b.owner[cl.ID]
	if owner != cl {
		// **A claim still pending took the id from this connection**, and may
		// yet give it back: its CONNACK has not been written. This teardown
		// waits with it rather than being lost - settleClaim runs it if the
		// id comes back, and a confirmed claim makes skipping it right.
		if c, pending := b.claims[owner]; pending && c.prev == cl {
			c.prevTeardown = &pendingTeardown{err: err, expire: expire}
		}
	}
	b.mu.Unlock()
	if owner != cl {
		return
	}

	// Its position is written before its cursor goes, or the advance since
	// the last tick goes with it and the consumer resumes further back than
	// it reached. An expiring session is the exception: its position is
	// about to be dropped below, so storing it first would be work to undo.
	if !expire {
		b.flushPositionsOf(cl.ID)
	}
	b.forgetConsumer(cl)
	// **A session that ends with its connection ends here, in endSession**,
	// before this connection lets go of the id below, so the ownership check
	// endSession makes and the removals it guards still see this connection
	// as the owner. Its subscriptions are still on it: the engine unsubscribes
	// a session that ends only after this returns, and the shared groups it
	// was the last member of are read from them.
	//
	// **No Will is owed from the record.** This connection's own Will was
	// decided at this disconnect, before this hook: published at once, since
	// a session ending with its connection has no delay to wait in
	// (willDelay), or discarded by a DISCONNECT with 0x00, or held back
	// because the broker is stopping. The record still carries the copy the
	// CONNECT wrote, and publishing that would be twice, or a Will the
	// client discarded, or one fired by a stop.
	if expire {
		b.endSession(cl, "its session was discarded at disconnect",
			endedSession{groups: shareFiltersOf(cl)})
	}

	b.mu.Lock()
	// Asked again, under the lock that protects everything below it. See
	// the fast path above for why once was not enough.
	if b.owner[cl.ID] != cl {
		b.mu.Unlock()
		return
	}
	delete(b.owner, cl.ID)
	// **A session that survives keeps what it declared**, and only a session
	// that is going takes it with it. MQTT restores a resumed session's
	// subscriptions, and saguin rebuilds its own index of them in
	// OnSessionEstablished - but nothing on the wire carries the partition
	// properties again, because there is no second SUBSCRIBE. Dropped here,
	// the consumer comes back subscribed and undeclared, and is served the
	// whole channel until it happens to subscribe again.
	//
	// Found by the link-churn soak on its first run: 480 of 480 records in
	// its own slice, and 37 from other slices, every one of them arriving
	// in the window after a resume.
	if !expire {
		b.forgetSubscriptions(cl.ID)
	}
	// **What this client has been told about its grant goes with it**, on
	// every disconnect rather than only an expiring one: it is a note about
	// whether a log line has been written, not session state, and a map
	// keyed by client id that nothing removes from is the bound invariant 13
	// is about.
	for key := range b.refusedRead {
		if id, _, ok := strings.Cut(key, "\x00"); ok && id == cl.ID {
			delete(b.refusedRead, key)
		}
	}
	b.mu.Unlock()

	// A kept session's record, outside b.mu because it is a store write: kept
	// with the moment its client went away. A session that ended has had its
	// record dropped by endSession. Still under the id's session lock, which
	// is what makes the record read here this session's: read after another
	// connection had claimed the id, it was that connection's live record
	// that was stamped disconnected and had its Will taken off.
	if !expire {
		b.recordDisconnect(cl, fired)
	}
}

// OnClientExpired drops what saguin holds for a session the server has
// just given up on.
//
// A `latest` channel's position deliberately outlives a disconnect - that
// is the whole of what it is for - so it cannot go in forgetConsumer,
// which runs every time a client goes away. What that leaves is a session
// that disconnects and never returns: the server drops it when its expiry
// interval passes, and without this hook saguin would keep one small map
// per client id that ever read a latest channel, for the life of the
// process. Bounded by the number of client ids a fleet has ever used is
// not bounded (invariant 13).
//
// **The stored `append` position goes here too, and this is the only path
// that reaches it.** OnDisconnect drops one, but only on its `expire`
// branch, and the substrate sets that flag solely for a client whose
// Session Expiry Interval is *zero* - a session that ends with the
// connection. Every client that asks to keep its session, which is every
// client `limits.max_session_expiry` was written for, disconnects with the
// flag clear and leaves its row behind. Nothing then removed it while the
// broker ran: the only other sweep is at startup, in each store's own
// recovery, so a broker up for months accumulated one row per client id
// that ever read a channel.
//
// It was measured rather than reasoned: a position with a two-second
// interval was still in the table twenty seconds later with the broker
// running, and gone after a restart - so the sweep worked and only the
// trigger was missing.
func (b *Broker) OnClientExpired(cl *mqtt.Client) {
	// Its own record, read before the session ends, because that is where a
	// Will still waiting on it is (takeWillAtSessionEnd). Under the engine's
	// session lock, so nothing else can have written the id's record since
	// the session was judged expired. A read that fails still ends the
	// session; the Will it could not read is not published, and says so.
	var ended *store.Session
	if s := b.sessionStore(); s != nil {
		sess, ok, err := b.getSession(s, cl.ID)
		switch {
		case err != nil:
			b.mu.Lock()
			provider := b.sessionsProvider
			b.mu.Unlock()
			b.log.Error("cannot read the Will a session was holding: it is not published",
				"client", b.limits.Loggable(cl.ID), "provider", provider, "error", err)
		case ok:
			ended = &sess
		}
	}
	// **Published immediately before its record goes** (RFC 0003 "Last
	// Will"): an expiry is the one ending that drops a record still holding
	// its Will, so endSession hands the Will back rather than dropping the
	// record, and it is published here and the record dropped straight
	// after. Published earlier, a crash in between left a published Will on
	// a record the next start would publish it from again. So this publishes
	// under the engine's session lock, and at the sweep that is safe by what
	// an expiring session is: its client is gone and no connection is taking
	// the id over, so no delivery waits on this lock. An expiry found as the
	// client comes back is on the connect path, and is safe by order instead:
	// expireSession marks the expired connection taken over only after this
	// returns, so no delivery is waiting on the lock for a takeover it is
	// making (TestNothingPublishesUnderASessionLock lists it).
	const why = "its session expired"
	groups := shareFiltersOf(cl)
	if will, owed := b.endSession(cl, why, endedSession{record: ended, groups: groups}); owed {
		b.publishEndedWill(cl.ID, will)
		b.dropRecordAfterItsWill(cl.ID, why, groups, ended.DisconnectedAt, ended.ExpiryInterval)
	}
}

// endedSession is the session an ending ends, as its caller holds it: the
// facts about that session endSession needs and must not read for itself,
// because by the time a session ends its client id may be another
// connection's (invariant 17).
type endedSession struct {
	// record is its record, read before it ended, or nil where it had none or
	// none is owed. The Will on it is the one published; a caller whose own
	// connection decides the Will at this ending passes the record without it.
	record *store.Session
	// groups are the shared groups it was a member of. A group it leaves with
	// no member loses its backlog with it.
	groups []string
	// retentionPassed names the channels whose stored positions retention
	// passed, where that is why it ends. Only those positions go: the ones
	// retention has not passed are kept, since nothing their client had not
	// read was removed from them - the one exception RFC 0003's table makes.
	// Nil for every other ending, which drops them all.
	retentionPassed map[string]bool
	// claimed says a CONNECT's claim on its client id holds its record: the
	// claim writes the successor's over it, and puts it back if the CONNACK
	// never reaches the client (settleClaim), so it is not dropped here.
	claimed bool
	// willTimerTaken says the timer waiting out its Will's delay was stopped
	// by the claim that ended it, so the Will on record is owed rather than
	// already published by that timer (takeWillAtSessionEnd).
	willTimerTaken bool
}

// endSession ends a session, and it is the one place a session ends: RFC 0003
// "Sessions" - "a session ends in one place, and all of it ends there".
// Everything the table lists goes, in this order:
//   - under b.mu, with the ownership check: the latest marks, the append
//     cursors and unacknowledged records, and the subscription and partition
//     indexes;
//   - outside it, because each is a store write and RFC 0005 forbids holding
//     b.mu across one: the stored positions, the held exactly-once publishes,
//     the shared groups' backlogs this was the last member of, the Will it
//     still held (published first), and the session record.
//
// **ended is the session's record as the caller read it**, and nil where it
// had none or none is owed. Its Will is the one published; nothing here reads
// the store for it, because by the time a session ends the id may belong to
// another connection (invariant 17). A caller whose own connection decided
// its Will at this ending passes a record without one.
//
// **Every caller holds the client id's session lock across the whole call**
// (mqtt.Server.LockSession): the engine holds it around OnClientExpired, from
// the expiry sweep and from a client returning to an expired session, around
// OnSessionEstablish for the retention-floor discard, and around
// OnSessionRegistered for a clean start; OnDisconnect takes it. The ownership
// check below reads b.owner under b.mu and the removals after it run without
// b.mu, and only that lock keeps a connection from claiming the id in between.
// Without it a connection that claimed the id there - or claimed it and left,
// which empties the owner entry this check reads as nobody - had its stored
// positions, its exactly-once publishes and its record removed by a session
// that had already ended.
//
// **A Will still owed is published once that lock is let go**, never under it
// (publishEndedWill has why), where the ending is claimed: its record was
// already written over by the connection that claimed the id, so there is
// nothing left to publish it before. An ending that drops a record still
// holding its Will - an expiry, and only an expiry - must publish it
// immediately before the record goes (RFC 0003 "Last Will"), so it is handed
// back instead, with the record left for the caller to drop once it has
// published it (OnClientExpired). Nothing is handed back where the session
// was not ended.
func (b *Broker) endSession(cl *mqtt.Client, why string, ended endedSession) (store.SessionWill, bool) {
	b.mu.Lock()
	// Not if a live connection now holds this id. An absent entry means
	// nobody holds it, which is the ordinary case at an expiry: OnDisconnect
	// removed it when the connection went, long before its session came due.
	if owner, held := b.owner[cl.ID]; held && owner != cl {
		b.mu.Unlock()
		return store.SessionWill{}, false
	}
	hadLatest := b.latestSeen.drop(cl.ID)
	// The cursors and unacknowledged append records, kept at a disconnect
	// that kept the session, and what the session declared: a resumed
	// session sends no SUBSCRIBE to carry its partition declarations again,
	// so they outlive a connection and go only here.
	b.forgetSessionDeliveriesLocked(cl.ID)
	b.forgetClient(cl.ID)
	b.mu.Unlock()

	if hadLatest {
		b.log.Info("dropped a latest position with its session", "client", cl.ID, "why", why)
	}
	// Outside b.mu, and this is the whole of why the ending is two halves: at
	// an expiry it runs on the substrate's single sweep, over every session
	// coming due in one tick, one store transaction per channel. Held under
	// the broker-wide lock it stalled publishes by 150-250ms against a 3-6ms
	// p99 at three hundred channels, and RFC 0005 calls a hold that long a
	// defect in so many words.
	if !b.dropStoredPositions(cl, why, ended.retentionPassed) {
		b.oweEnding(cl.ID, cl, why, true, ended.retentionPassed, false)
	}
	b.dropHeldPublishes(cl, why)
	// What its shared groups had lent it, if it was a clean member, is
	// settled at its ending (bdrain.endLent).
	//
	// What it was owed from the broadcast log is let go by the store's own
	// ending of the record (endStoredSession), which this one makes below
	// where it is its to make; where it is not - a claim holds the record -
	// it is let go here, and what its groups had handed it is split in memory,
	// as the store would have split it.
	//
	// The groups a discard at a position retention passed was the last
	// member of are ended by its own write, which Begins with them (the
	// retention path), and a clean start's with the record it replaced
	// (confirmClaim). endUnheld here asks the store again for those groups,
	// and on every path traced it finds nothing left to end; it
	// stays because a claimed ending no write reached would otherwise leave a
	// cursor, and asking costs one query per group on that discard. Likewise
	// d.end above answers nothing where beginSession has already let the
	// list go.
	if d := b.broadcastDrain(); d != nil {
		d.endLent(cl.ID)
		if ended.claimed {
			d.ended(d.end(cl.ID), store.Dropped{}, errEndedInMemory)
		}
		if ended.claimed && ended.retentionPassed != nil {
			d.endUnheld(ended.groups)
		}
	}
	// **A Will still waiting is due now.** The session ending is the other half
	// of "the Will Delay Interval has passed or the Session ends, whichever
	// happens first" (MQTT 5 section 3.1.3.2.2). It also closed a race this
	// broker had while the Will lived in memory: a session whose expiry and
	// whose Will fall due at the same moment had the sweep drop the record out
	// from under the timer, which then found nothing to publish (the arm of
	// TestAWillIsDueWhenItsSessionEndsIfThatIsSooner covering a session the
	// broker shortened).
	will, owed := b.takeWillAtSessionEnd(cl.ID, ended)
	// Whatever was unwritten about this session goes with it, so an id that
	// never comes back cannot keep an entry (recordDisconnect) - and a record
	// owed to be put back (settleClaim) would bring back what just ended.
	b.forgetUnwritten(cl.ID)
	b.forgetOwedRecord(cl.ID)
	switch {
	case owed && ended.claimed:
		b.afterSessionUnlock(cl.ID, func() { b.publishEndedWill(cl.ID, will) })
	case owed:
		return will, true
	}
	if !ended.claimed {
		b.dropSessionRecord(cl.ID, why, ended.groups)
	}
	return store.SessionWill{}, false
}

// errEndedInMemory is the answer bdrain.ended is given for an ending the
// store did not make: what the groups had handed the session is split by
// the same rule in memory.
var errEndedInMemory = errors.New("saguin: a session ended with its record held by a claim")

// lockSession takes a client id's session lock (mqtt.Server.LockSession), and
// nothing on a broker with no server, which only a test builds.
func (b *Broker) lockSession(id string) (unlock func()) {
	if b.srv == nil {
		return func() {}
	}
	return b.srv.LockSession(id)
}

// afterSessionUnlock runs fn once the caller lets go of id's session lock
// (mqtt.Server.AfterSessionUnlock), and at once on a broker with no server.
// Only the lock's holder may call it, which TestNothingPublishesUnderASessionLock
// checks of every caller.
func (b *Broker) afterSessionUnlock(id string, fn func()) {
	if b.srv == nil {
		fn()
		return
	}
	b.srv.AfterSessionUnlock(id, fn)
}

// forgetClient drops everything saguin keys by a client id that is going
// away, and is the only place a whole client leaves those indexes.
//
// **One function rather than a delete beside each departure**, and the
// reason is the shape of the bug it prevents rather than tidiness. There
// are four ways a client leaves - a discarded session, a taken-over id, an
// expiry, a disconnect - and a second index maintained by writing a delete
// next to each of them is one somebody forgets on the fifth path added
// next year. Nothing enforces beside-ness. There is nothing to remember
// here, because there is nowhere else to write it.
//
// TestEveryRemovalFromTheSubscriptionIndexesGoesThroughOnePlace holds that,
// by walking the source for removals rather than trusting this comment.
//
// The caller holds b.mu.
func (b *Broker) forgetClient(id string) {
	delete(b.subs, id)
	delete(b.partitions, id)
	b.reindex(id)
}

// dropSupersededLatest takes out of a departing session the `latest`
// deliveries whose topic has since been written again, before the successor
// inherits them.
//
// **A `latest` channel promises the current value, not every intermediate
// one**, and it already acts on that everywhere else: a live update for a
// subscriber with no room waits, and a newer one for its topic replaces it,
// because the value it would have missed is no longer current and it receives
// whatever is current when it catches up (RFC 0003). A resume is exactly that
// catching up - and it was the one path that did the opposite. MQTT restores
// the session's unacknowledged packets and re-sends them, so a value that
// stopped being true while the client was away went back on the wire as
// though it were current, and held a slot of the window until it was
// acknowledged.
//
// **What that cost is why this exists.** Those slots are never
// freed when the acknowledgement cannot arrive - a delivery re-sent into a
// socket a link cut has already killed reaches nobody, so the session keeps
// it and re-sends it on the next resume too. One is stranded per cut that
// lands in the wrong moment, and at the client's Receive Maximum the window
// is closed for good: the current-state pass waits in the `latest` list behind
// values that are both stale and immovable, and the device holds state
// nothing can correct. Measured on a ten-minute link-churn soak: four such
// packets against a Receive Maximum of four, window zero, one value queued,
// offsets 3042, 3429, 11928 and 381 - four different records accumulated
// across 259 cuts, every one long superseded.
//
// **Before the wire, and that is the whole of why it runs here.** This is
// OnSessionEstablish, which fires before the substrate clones the
// predecessor's in-flight table for the successor and therefore before
// anything is re-sent. Taking the values out afterwards was the first shape
// of this fix and it was unsound: the packet reaches a live client, the drop
// frees its identifier, and `Claim` hands a freed identifier straight back
// out - so the catch-up could send the *current* value under the identifier
// the client still owed an acknowledgement for, `processPuback` would match
// by identifier alone, and the stale value's acknowledgement would be
// credited to the current one. The mark then advances over a value the
// client may never have read. That is a false acknowledgement of current
// state, which is worse than the jam it was fixing. Removed before the
// clone, the value is never written, never owed, and its identifier never
// reaches the wire.
//
// **Strictly superseded only.** A packet carrying the value that is still
// current is left where it is and re-sent exactly as MQTT asks; only one the
// store has written past is taken out. So the session is never served less
// than it was owed - what replaces the dropped value is the newer value for
// that same topic, in the catch-up that follows the resume.
//
// **And the mark is held below the drop**, the same way a value undelivered
// at a disconnect records itself, so that catch-up provably reaches the
// topic rather than stepping over it on the strength of an offset that was
// sent and never acknowledged.
//
// Quotas need no repair. The clone carries entries, sizes and byte
// accounting and no quota counters, and inherit resets both from the
// CONNECT's Receive Maximum - so a packet removed here was never counted
// against the successor's window in the first place.
//
// `latest` alone. An `append` record is history and a queue record is a
// lease; neither is superseded by a later write to the same topic, and both
// re-send untouched.
func (b *Broker) dropSupersededLatest(cl *mqtt.Client) {
	type stale struct {
		packetID uint16
		channel  string
		offset   uint64
	}
	var drop []stale
	for _, pk := range cl.State.Inflight.GetAll(false) {
		if pk.FixedHeader.Type != packets.Publish {
			continue
		}
		b.mu.Lock()
		p, tracked := b.inflightLocked(cl.ID, pk.PacketID)
		lt := b.latest[p.channel]
		b.mu.Unlock()
		if !tracked || !p.latest || lt == nil {
			continue
		}
		// The store is asked rather than the packet: what makes a value
		// stale is that its topic has been written again, and only the
		// store knows that.
		cur, ok, err := lt.Get(pk.TopicName)
		if err != nil || !ok || cur.Offset <= p.offset {
			continue
		}
		drop = append(drop, stale{pk.PacketID, p.channel, p.offset})
	}
	if len(drop) == 0 {
		return
	}

	for _, d := range drop {
		// While cl still has its session (WhileOwned), as every change to a
		// connection's table saguin makes; it has, since the takeover that
		// copies it comes after this, from the connection running it.
		retired := false
		if !cl.WhileOwned(func() { retired = cl.State.Inflight.Retire(d.packetID) }) {
			continue
		}

		b.mu.Lock()
		b.deleteInflightLocked(cl.ID, d.packetID)
		b.latestSeen.lowerMissed(cl.ID, d.channel, d.offset)
		b.mu.Unlock()
		if retired {
			cl.State.Inflight.Unclaim(d.packetID) // its record is gone (Inflight.Retire)
		}
	}

	// One line per resume rather than one per value, for the reason
	// forgetConsumer gives: a fleet's worth of state topics would otherwise
	// be a page of log per reconnect.
	b.log.Info("took superseded values out of a resuming session; the catch-up "+
		"serves what is current",
		"client", b.limits.Loggable(cl.ID), "values", len(drop))
}
