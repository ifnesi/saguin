package broker

import (
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// A shared group's reading of the broadcast log (RFC 0003 "Broadcast").
//
// **A group with a member whose session outlives its connection has a cursor
// on the log**, kept with the sessions, and an owed list of what lies after
// it that it has not handed out: every QoS 1 or 2 broadcast its filter
// matches is counted for it as for a session (bdrain.count). One drain per
// group hands each, oldest first, to one member that can take it now, round
// robin: a durable member gets it in its window, as any delivery it is owed
// (HandOver), and a clean member in memory, with a returned row that a
// restart - which ends its session - gives back (Lend). What lies on the list
// while no member can take it is the group's backlog.
//
// **What a group handed a member and the member did not finish is the
// member's until its session ends**, and then split as MQTT 5 section 4.8.2
// splits it (store.Dropped): at QoS 1 it goes back to the group, which serves
// it again ahead of its cursor; at QoS 2 awaiting a PUBREC it goes to no other
// member (MQTT-4.8.2-5), dropped and counted as member_ended.
//
// **Lock order.** A group's list is an owed list, so its lock is owed.mu,
// taken as a session's is and never with another. A hand-over to a durable
// member takes the member's connection's read lock first (Client.Own), then
// the member's fmu, and each list's mu on its own after that, re-checking
// gone under it as record does: nothing takes the read lock under an fmu
// (TestAnInFlightTableIsChangedOnlyWhileItsConnectionOwnsIt). lmu, guarding
// the clean members' deliveries, is taken under nothing and takes nothing.
// Every write of a group's cursor is made on its drain's goroutine, so two
// never race.

// lent is a group's delivery in flight to a member whose session the store
// does not keep: the connection it went out on, under which identifier and at
// which QoS, and whether a QoS 2 exchange has had its PUBREC.
type lent struct {
	conn     *mqtt.Client
	pid      uint16
	qos      byte
	received bool
}

// lendRef is where a delivery in flight to a clean member is on its group's
// list, and the connection it went out on.
type lendRef struct {
	g    *owed
	off  uint64
	conn *mqtt.Client
}

// handed is an entry a group handed a durable member, taken off the member's
// list at its ending (bdrain.end): its in-flight row, and what it counts
// against a list that takes it back.
type handed struct {
	f      store.InFlight
	charge int64
}

// group is a shared group's list, or nil where it has no cursor.
func (d *bdrain) group(filter string) *owed {
	if v, ok := d.groups.Load(filter); ok {
		return v.(*owed)
	}
	return nil
}

// groupsOf is the list of each group among filters that has one, once each.
func (d *bdrain) groupsOf(filters []string) []*owed {
	var out []*owed
	for _, f := range filters {
		if g := d.group(f); g != nil && !slices.Contains(out, g) {
			out = append(out, g)
		}
	}
	return out
}

// newGroupList is an empty list for the group filter.
func (d *bdrain) newGroupList(filter string) *owed {
	return &owed{d: d, client: filter, isGroup: true, flying: map[uint16]flight{}, stored: map[uint16]uint64{}}
}

// createGroups gives each of groups that has none a cursor on the log, where
// the next message not yet counted is, and a list. write keeps the cursors of
// missing, at boundary, with whatever else it keeps, and answers how many of
// missing, from the first, it kept, and its error: those are given their
// lists, and no other. With missing empty, every group has one by now, and
// write keeps the rest alone.
//
// **Under the gate, from the boundary to the lists**, which the publish
// side's two readings of who has a group take shared (wanted,
// takeRecipients). So no publish decides between the boundary and the lists:
// one that decided before was counted before the boundary was taken, and is
// below it, the cursor's no more than the list's; one that decides after
// finds the list, and is written to the log for it and counted on it. A list
// put in before its cursor was kept is a group that may never get one, and a
// cursor kept before its list is a store write a publish could fall behind.
//
// **Which groups are missing is decided under it too**, and every store
// write that ends a cursor holds it until the cursor's list is gone
// (endUnheld, endStoredSession, beginSession). So a group seen here with a
// list still has its cursor when write lands, and one whose cursor is ending
// is seen without.
func (d *bdrain) createGroups(groups []string, write func(boundary uint64, missing []string) (int, error)) error {
	d.gate.Lock()
	defer d.gate.Unlock()
	var missing []string
	for _, f := range groups {
		if d.group(f) == nil && !slices.Contains(missing, f) {
			missing = append(missing, f)
		}
	}
	boundary := d.counted.through.Load() + 1
	kept, err := write(boundary, missing)
	if kept > 0 {
		if p := groupCreating.Load(); p != nil {
			(*p)(d.b, missing[:kept])
		}
	}
	for _, f := range missing[:kept] {
		d.groups.LoadOrStore(f, d.newGroupList(f))
	}
	return err
}

// groupCreating is a test seam, nil in production: when set it runs once a
// group's cursor is kept and before its list is put in, with the gate held
// (createGroups), so a test can publish into that moment.
var groupCreating atomic.Pointer[func(b *Broker, groups []string)]

// endGroup ends a group whose cursor the store has ended: what it holds goes,
// counted as no_member_left, since nobody who could take it is left (RFC 0003
// "Sessions").
func (d *bdrain) endGroup(filter string) {
	v, ok := d.groups.LoadAndDelete(filter)
	if !ok {
		return
	}
	d.dropGroup(v.(*owed), &d.b.counted.sharesDroppedNoMember)
}

// endUnheld ends each of groups no kept session holds the filter of any more,
// as the store decides it in one step (EndShareCursorIfUnheld).
//
// Under the gate, from the store's ending to the list's: a group's creation
// between the two would see the list and save a member against a cursor
// already gone (createGroups).
func (d *bdrain) endUnheld(groups []string) {
	d.gate.RLock()
	defer d.gate.RUnlock()
	for _, g := range groups {
		ended, err := d.st.EndShareCursorIfUnheld(g)
		if err != nil {
			d.b.log.Error("cannot end a shared group its last member left", "group", d.b.limits.Loggable(g),
				"error", err)
			continue
		}
		if ended {
			d.endGroup(g)
		}
	}
}

// dropGroup lets go of everything a group's list holds but what is in flight
// to a clean member, which its acknowledgement or its member's ending
// settles, counting what it let go under cause.
func (d *bdrain) dropGroup(g *owed, cause interface{ Add(uint64) uint64 }) {
	g.mu.Lock()
	g.gone = true
	var offs []uint64
	kept := g.list[:0]
	for _, e := range g.list {
		if e.lent != nil {
			kept = append(kept, e)
			continue
		}
		offs = append(offs, e.offset)
	}
	g.list = kept
	g.forget = nil
	g.mu.Unlock()
	if cause != nil && len(offs) > 0 {
		cause.Add(uint64(len(offs)))
		d.b.log.Info("dropped a shared group's backlog: its last member session ended",
			"group", d.b.limits.Loggable(g.client), "deliveries", len(offs))
	}
	d.letGo(offs...)
}

// wakeGroup starts a group's drain on a goroutine of its own, or asks the
// one running to look again: one drain per group, so it hands its messages
// out in the order its list holds them.
func (d *bdrain) wakeGroup(g *owed) {
	g.mu.Lock()
	if g.gone {
		g.mu.Unlock()
		return
	}
	if g.pumping {
		g.repump = true
		g.mu.Unlock()
		return
	}
	g.pumping = true
	g.mu.Unlock()
	d.b.drains.Add(1)
	go func() {
		defer d.b.drains.Done()
		d.runGroup(g)
	}()
}

// wakeGroups wakes every group cl is a member of: it has just subscribed, or
// come back. A durable member's groups have their cursors by then, made with
// its record (keepSubscriptions, repairGroups).
func (b *Broker) wakeGroups(cl *mqtt.Client) {
	if d := b.broadcastDrain(); d != nil {
		d.wakeGroupsOf(cl)
	}
}

// wakeGroupsOf wakes the drain of every group cl is a member of: its window
// or its connection has room now.
func (d *bdrain) wakeGroupsOf(cl *mqtt.Client) {
	for _, f := range shareFiltersOf(cl) {
		if g := d.group(f); g != nil {
			d.wakeGroup(g)
		}
	}
}

// groupFlushEvery is how many hand-outs a busy group's drain makes between
// writes of where its cursor stands.
const groupFlushEvery = 64

// runGroup hands out what a group is owed until no member can take the next,
// then writes where its cursor stands.
//
// **And while it stays busy, every groupFlushEvery hand-outs.** A hand-over
// to a member writes the cursor itself, but what a group lets go of
// otherwise - written at QoS 0, expired, refused by the acl_file - only marks
// it to be written: a drain that never ran out of work never wrote it, and a
// crash then sent all of that again.
func (d *bdrain) runGroup(g *owed) {
	for {
		groupPassed(d.b, g.client, true)
		for n := 1; d.handOut(g); n++ {
			if n%groupFlushEvery == 0 {
				d.flushGroup(g)
			}
		}
		groupPassed(d.b, g.client, false)
		d.flushGroup(g)
		g.mu.Lock()
		if !g.repump || g.gone {
			g.pumping = false
			g.mu.Unlock()
			return
		}
		g.repump = false
		g.mu.Unlock()
	}
}

// groupPass is a test seam, nil in production: when set it runs as each pass
// of a group's drain begins (begun) and once it has handed out all it could
// (not begun), so a test can wait for a pass that saw a state rather than
// sleeping until one probably has.
var groupPass atomic.Pointer[func(b *Broker, group string, begun bool)]

func groupPassed(b *Broker, group string, begun bool) {
	if f := groupPass.Load(); f != nil {
		(*f)(b, group, begun)
	}
}

// flushGroup writes a group's cursor where its list now puts it, and forgets
// the returned offsets it let go of otherwise than by handing them over, in
// one store call (SetShareCursor). A write the store refuses is kept for the
// next.
func (d *bdrain) flushGroup(g *owed) {
	through := d.counted.through.Load()
	g.mu.Lock()
	if g.gone || (!g.dirty && len(g.forget) == 0) {
		g.mu.Unlock()
		return
	}
	cursor := g.cursorLocked(through)
	forget := g.forget
	g.forget, g.dirty = nil, false
	g.mu.Unlock()
	err := d.st.SetShareCursor(g.client, cursor, forget...)
	if err == nil || errors.Is(err, store.ErrNoShareGroup) {
		return
	}
	d.b.log.Error("cannot record where a shared group's cursor stands", "group", d.b.limits.Loggable(g.client),
		"error", err)
	g.mu.Lock()
	g.forget = append(forget, g.forget...)
	g.dirty = true
	g.mu.Unlock()
}

// passOn takes a delivery off a group's list without handing it to anyone,
// counting it under cause where there is one, and lets go of it. Its returned
// row goes with the group's next write.
func (d *bdrain) passOn(g *owed, off uint64, cause interface{ Add(uint64) uint64 }) {
	g.mu.Lock()
	returned := false
	if i := g.findLocked(off); i >= 0 {
		returned = g.list[i].returned
	}
	took := g.removeLocked(off)
	if took {
		g.dirty = true
		if returned {
			g.forget = append(g.forget, off)
		}
	}
	g.mu.Unlock()
	if !took {
		return
	}
	if cause != nil {
		cause.Add(1)
	}
	d.letGo(off)
}

// handOut hands the group's oldest waiting delivery to one member that can
// take it now, and reports whether to go on.
//
// **A member can take it** as the live selection has it (canTakeShared):
// connected, room in its socket's queue and in its window. A durable member
// also needs room under half its bound on the wire, as its own deliveries
// do, and must not hold the same message already: its list holds one entry
// an offset, and its own copy comes first (MQTT 5 section 4.8.2: a second
// copy may follow). Round robin over the members sorted by client id, as the
// live selection chooses. None that can take it leaves it waiting: a
// member's arrival, acknowledgement or SUBSCRIBE wakes the group.
func (d *bdrain) handOut(g *owed) bool {
	if d.b.srv == nil {
		return false
	}
	g.mu.Lock()
	if g.gone {
		g.mu.Unlock()
		return false
	}
	var e owedEntry
	found := false
	for _, x := range g.list {
		if x.waiting() {
			e, found = x, true
			break
		}
	}
	if !found {
		g.mu.Unlock()
		return false
	}
	g.pickLocked([]uint64{e.offset})
	g.mu.Unlock()
	done := false
	defer func() {
		if !done {
			g.mu.Lock()
			g.unpickLocked([]uint64{e.offset})
			g.mu.Unlock()
		}
	}()

	recs, err := d.log.ReadAt(e.offset)
	if err != nil {
		d.b.log.Error("cannot read the broadcast log; this shared group is not being served",
			"group", d.b.limits.Loggable(g.client), "error", err)
		return false
	}
	if len(recs) == 0 {
		// The log no longer holds it: taken to make room before it was
		// picked, and counted then.
		done = true
		d.passOn(g, e.offset, nil)
		return true
	}
	r := recs[0]
	now := time.Now()
	if r.Expired(now) {
		// The publisher's own expiry, read rather than enforced by deleting
		// it (invariant 2): the group's, counted as its expired.
		done = true
		d.passOn(g, e.offset, &d.b.counted.sharesDroppedExpired)
		return true
	}

	members := d.b.srv.Topics.Subscribers(r.Topic).Shared[g.client]
	probe := broadcastPacket(r, r.QoS, broadcastOptions{}, false, now)
	var able []string
	for id, sub := range members {
		if !d.b.canTakeShared(id, sub, probe) || !d.b.fitsShared(id, probe) || !d.memberHasRoom(id, e) {
			continue
		}
		able = append(able, id)
	}
	if len(able) == 0 {
		return false
	}
	slices.Sort(able)
	var allowed []*mqtt.Client
	for _, id := range able {
		if cl, ok := d.b.srv.Clients.Get(id); ok && d.b.mayDeliver(cl, r.Topic) {
			allowed = append(allowed, cl)
		}
	}
	if len(allowed) == 0 {
		// Members could take it and the acl_file allows it to none of them:
		// not delivered, as the session's drain lets such a one go, and
		// counted as the group's, not_authorized, so what the group was
		// given and what it gave out still add up.
		d.b.log.Debug("did not deliver a shared group's broadcast the acl_file allows none of its members",
			"group", d.b.limits.Loggable(g.client), "topic", d.b.limits.Loggable(r.Topic))
		done = true
		d.passOn(g, e.offset, &d.b.counted.sharesDroppedNotAuthorized)
		return true
	}
	g.mu.Lock()
	cl := allowed[g.rr%uint64(len(allowed))]
	g.rr++
	g.mu.Unlock()

	sub := members[cl.ID]
	qos := min(r.QoS, sub.Qos)
	opt := broadcastOptions{qos: sub.Qos, rap: sub.RetainAsPublished}
	if sub.Identifier > 0 {
		opt.ids = []int{sub.Identifier}
	}
	pk := broadcastPacket(r, qos, opt, legacyClient(cl), now)
	var goOn bool
	switch o := d.session(cl.ID); {
	case qos == 0:
		done, goOn = d.handAtZero(g, e, cl, pk)
	case o != nil && persistentSession(cl):
		done, goOn = d.handToMember(g, e, cl, o, pk)
	default:
		done, goOn = d.lendTo(g, e, cl, pk)
	}
	return goOn
}

// memberHasRoom reports whether a durable member may be handed e: room in its
// in-flight table as the store holds it, under half its bound on the wire
// with it, and not holding the same message already. A member the drain does
// not hold has nothing to ask.
//
// **The store's table, not only the connection's.** An acknowledgement frees
// the connection's slot as it is read, and the store's row only at the
// member's next flush, so a member with room on its connection can have none
// in the store (Sessions.HandOver refuses a table larger than the window). A
// group asked the store and, refused, asked again at once: measured on one
// core, 9,000 refused hand-overs a delivery, each a store call, and the
// member's own flush, which would have ended it, waiting for the core. Its
// flush wakes its groups instead (flush).
func (d *bdrain) memberHasRoom(id string, e owedEntry) bool {
	o := d.session(id)
	if o == nil {
		return true
	}
	window := 65535
	if cl, ok := d.b.srv.Clients.Get(id); ok {
		window = int(receiveMaximum(cl))
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gone || o.findLocked(e.offset) >= 0 || len(o.stored) >= window {
		return false
	}
	half := d.b.limits.SessionQueueBytes / 2
	return half <= 0 || o.wire == 0 || o.wire+e.charge+wireEntryCost <= half
}

// handAtZero writes a group's delivery to a member at QoS 0, which needs
// nothing more: the group lets it go once it is written.
func (d *bdrain) handAtZero(g *owed, e owedEntry, cl *mqtt.Client, pk packets.Packet) (done, goOn bool) {
	if err := d.b.writeTo(cl, pk); err != nil {
		if errors.Is(err, packets.ErrPacketTooLarge) {
			d.refusedAfterFitting(g, cl, pk)
		}
		return false, true // not written: it stays the group's, for another member
	}
	d.b.counted.sharesDrained.Add(1)
	d.passOn(g, e.offset, nil)
	return true, true
}

// handToMember hands a group's delivery to a durable member: into its
// in-flight table under a packet identifier, with the group's cursor moved
// past it, in one store write (HandOver), then onto its list as sent, into
// its connection's in-flight table, and out on the wire. It is the member's
// from there, as any delivery it is owed: acknowledged, sent again on a
// resume, and split at its session's ending.
//
// **The order is the lock order**: the connection's read lock first, so a
// takeover comes wholly before or wholly after the row and the table entry;
// then the member's fmu, which its ending waits for; then each list's lock
// alone. A write that fails - the connection going - is undone to the group's
// returned list (unhand).
func (d *bdrain) handToMember(g *owed, e owedEntry, cl *mqtt.Client, o *owed, pk packets.Packet) (done, goOn bool) {
	id, err := cl.NextPacketID()
	if err != nil {
		return false, false // none free: an acknowledgement frees one and wakes the group
	}
	pk.PacketID = uint16(id)
	f := store.InFlight{Offset: e.offset, PacketID: pk.PacketID, QoS: pk.FixedHeader.Qos,
		State: store.MessageSent, Group: g.client}

	release, owned := cl.Own()
	if !owned {
		cl.State.Inflight.Unclaim(pk.PacketID)
		return false, true
	}
	o.fmu.Lock()
	o.mu.Lock()
	memberGone := o.gone
	o.mu.Unlock()
	g.mu.Lock()
	groupGone := g.gone
	cursor := g.cursorExceptLocked(d.counted.through.Load(), e.offset)
	g.mu.Unlock()
	if memberGone || groupGone {
		o.fmu.Unlock()
		release()
		cl.State.Inflight.Unclaim(pk.PacketID)
		return false, !groupGone
	}
	err = d.b.withRoom(d.provider, store.InFlightBytes(f), "the session store", func() error {
		return d.st.HandOver(g.client, cursor, o.client, receiveMaximum(cl), []store.InFlight{f})
	})
	if err != nil {
		o.fmu.Unlock()
		release()
		cl.State.Inflight.Unclaim(pk.PacketID)
		switch {
		case errors.Is(err, store.ErrNoShareGroup):
			return false, false // the group ended: endGroup settles it
		case errors.Is(err, store.ErrWindowFull):
			// The member's own write filled the table since memberHasRoom
			// asked: its flush clears the rows and wakes the group.
			return false, false
		case errors.Is(err, store.ErrNoSession):
			return false, true
		}
		d.b.log.Error("cannot hand a shared group's delivery to a member", "group", d.b.limits.Loggable(g.client),
			"client", d.b.limits.Loggable(o.client), "error", err)
		return false, false
	}
	o.mu.Lock()
	o.insertLocked(owedEntry{offset: e.offset, charge: e.charge, group: g.client})
	o.markSentLocked(e.offset)
	o.flying[pk.PacketID] = flight{entry: f, conn: cl}
	o.stored[pk.PacketID] = e.offset
	o.mu.Unlock()
	g.mu.Lock()
	g.removeLocked(e.offset) // its returned row, if it had one, went in the same write
	g.mu.Unlock()
	o.fmu.Unlock()
	cl.State.Inflight.Set(pk)
	cl.State.Inflight.DecreaseSendQuota()
	release()
	d.b.counted.sharesDrained.Add(1)

	if err := d.b.writeTo(cl, pk); err != nil {
		if errors.Is(err, packets.ErrPacketTooLarge) {
			d.refusedAfterFitting(g, cl, pk)
		}
		d.unhand(g, cl, o, pk, f)
	}
	return true, true
}

// refusedAfterFitting hangs up a member whose connection refused a group's
// delivery for its size although the member was chosen for fitting it
// (fitsShared), which the measure's margin is there to make impossible. Hung
// up, 0x95, it is not chosen again, and the caller gives the delivery back
// to the group: without that, the same member would be chosen for it for
// ever.
func (d *bdrain) refusedAfterFitting(g *owed, cl *mqtt.Client, pk packets.Packet) {
	size := deliverySize(pk)
	d.b.log.Error("disconnecting: a shared group's delivery chosen for fitting this member's Maximum "+
		"Packet Size was refused for its size", "group", d.b.limits.Loggable(g.client),
		"client", d.b.limits.Loggable(cl.ID), "size", size,
		"maximum_packet_size", cl.Properties.Props.MaximumPacketSize)
	d.b.hangUp(cl, packets.ErrPacketTooLarge)
}

// unhand undoes a hand-over whose delivery never reached the member's
// connection: out of the connection's in-flight table, and from the member's
// table to the group's returned list in one store write (Return), so the
// group serves it again, at any QoS - it was never sent.
//
// **Not one a takeover carried**, which the new connection sends again under
// its identifier and so finishes, as unsend leaves one.
func (d *bdrain) unhand(g *owed, cl *mqtt.Client, o *owed, pk packets.Packet, f store.InFlight) {
	id := pk.PacketID
	if !cl.WhileOwned(func() {
		if cl.State.Inflight.Retire(id) {
			cl.State.Inflight.IncreaseSendQuota()
		}
	}) {
		return
	}
	o.fmu.Lock()
	o.mu.Lock()
	gone := o.gone
	o.mu.Unlock()
	var err error
	if !gone {
		err = d.st.Return(o.client, g.client, []store.InFlight{f})
	}
	o.fmu.Unlock()
	switch {
	case gone:
		// Its ending took the entry, and split it (ended).
		cl.State.Inflight.Unclaim(id)
		return
	case errors.Is(err, store.ErrNoShareGroup):
		// Nobody left to return it to: the member lets it go as its own,
		// and its row leaves the table with its next write.
		o.mu.Lock()
		delete(o.flying, id)
		took := o.removeLocked(f.Offset)
		o.acked = append(o.acked, f)
		o.unclaims = append(o.unclaims, func() { cl.State.Inflight.Unclaim(id) })
		o.dirty = true
		o.mu.Unlock()
		if took {
			d.droppedHanded(&d.b.counted.sharesDroppedNoMember, 1)
			d.letGo(f.Offset)
		}
		return
	case err != nil:
		// Kept on the member's table, on no connection, to be sent again
		// when its session resumes.
		d.b.log.Error("cannot return an unsent delivery to its shared group; it waits for its member",
			"group", d.b.limits.Loggable(g.client), "client", d.b.limits.Loggable(o.client), "error", err)
		o.mu.Lock()
		if fl, ok := o.flying[id]; ok && fl.conn == cl {
			fl.conn = nil
			o.flying[id] = fl
		}
		o.mu.Unlock()
		cl.State.Inflight.Unclaim(id)
		return
	}
	o.mu.Lock()
	delete(o.flying, id)
	delete(o.stored, id)
	took := o.removeLocked(f.Offset)
	o.mu.Unlock()
	cl.State.Inflight.Unclaim(id)
	if took {
		d.returnToGroup(g, []uint64{f.Offset}, map[uint64]int64{f.Offset: d.chargeOf(f.Offset)})
	}
}

// chargeOf is what a message counts on a list, read from the log; the
// owed-entry cost alone where the log no longer holds it.
func (d *bdrain) chargeOf(off uint64) int64 {
	recs, err := d.log.ReadAt(off)
	if err != nil || len(recs) == 0 {
		return owedEntryCost
	}
	return store.RecordSize(recs[0]) + owedEntryCost
}

// returnToGroup puts deliveries back on a group's list as returned, waiting
// to be handed out again ahead of its cursor, each counted as held again.
// Ownership moves from the list that had them to this one, so the log's
// count of who is owed each does not change. A group that has ended lets
// them go, counted as no_member_left.
//
// **Not held to the group's bound as it goes back**, on purpose: what comes
// back was in a member's window, so the overshoot is bounded by the members'
// windows, and giving up the oldest here would give up exactly what MQTT
// says should go to another member. The next count bounds the list again.
func (d *bdrain) returnToGroup(g *owed, offs []uint64, charges map[uint64]int64) {
	if len(offs) == 0 {
		return
	}
	g.mu.Lock()
	if g.gone {
		g.mu.Unlock()
		d.droppedHanded(&d.b.counted.sharesDroppedNoMember, len(offs))
		d.letGo(offs...)
		return
	}
	for _, off := range offs {
		g.insertLocked(owedEntry{offset: off, charge: charges[off], returned: true})
		if i := g.findLocked(off); i >= 0 {
			g.list[i].returned = true
		}
	}
	g.mu.Unlock()
	d.b.counted.sharesHeld.Add(uint64(len(offs)))
	d.wakeGroup(g)
}

// lendTo hands a group's delivery to a member whose session the store does
// not keep. At QoS 1 its returned row is written and the group's cursor moved
// past it in one store write (Lend): a restart ends that session, and the
// row gives the delivery back. At QoS 2 the cursor alone moves: at its
// member's ending it goes to no other member. In memory it stays on the
// group's list, lent, until the member's acknowledgement lets it go, or its
// session's end settles it (endLent).
func (d *bdrain) lendTo(g *owed, e owedEntry, cl *mqtt.Client, pk packets.Packet) (done, goOn bool) {
	id, err := cl.NextPacketID()
	if err != nil {
		return false, false
	}
	pk.PacketID = uint16(id)
	qos := pk.FixedHeader.Qos
	g.mu.Lock()
	cursor := g.cursorExceptLocked(d.counted.through.Load(), e.offset)
	g.mu.Unlock()
	if qos == 1 {
		err = d.b.withRoom(d.provider, store.ShareReturnedSize(g.client), "the session store", func() error {
			return d.st.Lend(g.client, cursor, []uint64{e.offset})
		})
	} else {
		err = d.st.SetShareCursor(g.client, cursor)
	}
	if err != nil {
		cl.State.Inflight.Unclaim(pk.PacketID)
		if !errors.Is(err, store.ErrNoShareGroup) {
			d.b.log.Error("cannot hand a shared group's delivery to a member", "group", d.b.limits.Loggable(g.client),
				"client", d.b.limits.Loggable(cl.ID), "error", err)
		}
		return false, false
	}
	d.lmu.Lock()
	if d.lends == nil {
		d.lends = map[string]map[uint16]lendRef{}
	}
	if d.lends[cl.ID] == nil {
		d.lends[cl.ID] = map[uint16]lendRef{}
	}
	d.lends[cl.ID][pk.PacketID] = lendRef{g: g, off: e.offset, conn: cl}
	d.lmu.Unlock()
	g.mu.Lock()
	if i := g.findLocked(e.offset); i >= 0 {
		g.list[i].lent = &lent{conn: cl, pid: pk.PacketID, qos: qos}
		g.list[i].returned = g.list[i].returned || qos == 1
	}
	g.mu.Unlock()
	// **Drained as it is lent, before it is registered**: every way a lent
	// delivery comes back - its member's ending (endLent), a takeover before
	// the registration below (unlend) - counts it held again, so it is
	// counted handed out from the moment either can.
	d.b.counted.sharesDrained.Add(1)
	if !cl.WhileOwned(func() {
		cl.State.Inflight.Set(pk)
		cl.State.Inflight.DecreaseSendQuota()
	}) {
		// Taken over before it was registered: the connection's in-flight
		// table went to its successor without it. Never sent, it goes back.
		d.unlend(cl, pk.PacketID, true)
		return true, true
	}
	if err := d.b.writeTo(cl, pk); err != nil {
		if errors.Is(err, packets.ErrPacketTooLarge) {
			// Never sent: back to the group, at any QoS, before the hang-up
			// ends the session, whose ending would split it as one the
			// member had.
			if cl.WhileOwned(func() {
				if cl.State.Inflight.Retire(pk.PacketID) {
					cl.State.Inflight.IncreaseSendQuota()
				}
			}) {
				d.unlend(cl, pk.PacketID, true)
				cl.State.Inflight.Unclaim(pk.PacketID)
			}
			d.refusedAfterFitting(g, cl, pk)
		}
		// The connection is going: its session's ending settles it (endLent).
	}
	return true, true
}

// unlend takes back a delivery lent to a clean member that never reached it:
// to the group's list as waiting, returned (again), and held again, where
// back; let go where not.
func (d *bdrain) unlend(cl *mqtt.Client, pid uint16, back bool) {
	ref, ok := d.takeLend(cl, pid)
	if !ok {
		return
	}
	g := ref.g
	g.mu.Lock()
	i := g.findLocked(ref.off)
	if i < 0 {
		g.mu.Unlock()
		return
	}
	qos := g.list[i].lent.qos
	if !back || g.gone {
		if g.list[i].returned {
			g.forget = append(g.forget, ref.off)
		}
		g.removeLocked(ref.off)
		g.dirty = true
		g.mu.Unlock()
		d.letGo(ref.off)
		return
	}
	g.list[i].lent = nil
	g.unpickLocked([]uint64{ref.off})
	g.mu.Unlock()
	if qos == 2 {
		// Its cursor passed it with no row behind it: one is written now, so
		// a restart gives it back too.
		if err := d.st.Lend(g.client, g.cursorOf(d.counted.through.Load()), []uint64{ref.off}); err == nil {
			g.mu.Lock()
			if i := g.findLocked(ref.off); i >= 0 {
				g.list[i].returned = true
			}
			g.mu.Unlock()
		}
	}
	d.b.counted.sharesHeld.Add(1)
	d.wakeGroup(g)
}

// cursorOf is cursorLocked, taking the list's lock.
func (o *owed) cursorOf(through uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cursorLocked(through)
}

// lentAcked settles a clean member's acknowledgement of a delivery its group
// lent it - its PUBACK, or at QoS 2 its PUBCOMP - and reports whether it was
// one. Its returned row is forgotten by the group's next write.
func (d *bdrain) lentAcked(cl *mqtt.Client, pid uint16) bool {
	ref, ok := d.takeLend(cl, pid)
	if !ok {
		return false
	}
	g := ref.g
	g.mu.Lock()
	took := false
	if i := g.findLocked(ref.off); i >= 0 {
		if g.list[i].returned {
			g.forget = append(g.forget, ref.off)
		}
		took = g.removeLocked(ref.off)
		g.dirty = true
	}
	g.mu.Unlock()
	if took {
		d.letGo(ref.off)
	}
	d.wakeGroup(g)
	return true
}

// lentReceived records a clean member's PUBREC for a QoS 2 delivery its group
// lent it, and reports whether it was one: from here the member has it, and
// its session's ending is no loss.
func (d *bdrain) lentReceived(cl *mqtt.Client, pid uint16) bool {
	d.lmu.Lock()
	ref, ok := d.lends[cl.ID][pid]
	d.lmu.Unlock()
	if !ok || !d.takes(flight{conn: ref.conn}, cl) {
		return false
	}
	g := ref.g
	g.mu.Lock()
	if i := g.findLocked(ref.off); i >= 0 && g.list[i].lent != nil {
		g.list[i].lent.received = true
	}
	g.mu.Unlock()
	return true
}

// takeLend takes a clean member's lent delivery out of lends, where cl may
// settle it: the connection it went out on, or the one a takeover carried it
// to (takes).
//
// The connection is asked of the engine with lmu let go, which takes nothing.
func (d *bdrain) takeLend(cl *mqtt.Client, pid uint16) (lendRef, bool) {
	d.lmu.Lock()
	ref, ok := d.lends[cl.ID][pid]
	d.lmu.Unlock()
	if !ok || !d.takes(flight{conn: ref.conn}, cl) {
		return lendRef{}, false
	}
	d.lmu.Lock()
	defer d.lmu.Unlock()
	if now, still := d.lends[cl.ID][pid]; !still || now != ref {
		return lendRef{}, false
	}
	delete(d.lends[cl.ID], pid)
	if len(d.lends[cl.ID]) == 0 {
		delete(d.lends, cl.ID)
	}
	return ref, true
}

// endLent settles what groups lent to a clean member whose session has ended:
// at QoS 1 back to the group, whose returned row already holds it; at QoS 2
// awaiting its PUBREC let go and counted as member_ended (MQTT-4.8.2-5); at
// QoS 2 with its PUBREC let go, since the member has it. By client id, since
// a takeover that keeps the session carries these to its new connection,
// and only the session's end settles them.
func (d *bdrain) endLent(client string) {
	d.lmu.Lock()
	refs := d.lends[client]
	delete(d.lends, client)
	d.lmu.Unlock()
	for _, ref := range refs {
		g := ref.g
		g.mu.Lock()
		i := g.findLocked(ref.off)
		if i < 0 || g.list[i].lent == nil {
			g.mu.Unlock()
			continue
		}
		l := *g.list[i].lent
		if l.qos == 1 && !g.gone {
			g.list[i].lent = nil
			g.unpickLocked([]uint64{ref.off})
			g.mu.Unlock()
			d.b.counted.sharesHeld.Add(1)
			d.wakeGroup(g)
			continue
		}
		if g.list[i].returned {
			g.forget = append(g.forget, ref.off)
		}
		g.removeLocked(ref.off)
		g.dirty = true
		gone := g.gone
		g.mu.Unlock()
		switch {
		case l.qos == 1 && gone:
			d.droppedHanded(&d.b.counted.sharesDroppedNoMember, 1)
		case l.qos == 2 && !l.received:
			d.droppedHanded(&d.b.counted.sharesDroppedMemberEnded, 1)
		}
		d.letGo(ref.off)
		d.wakeGroup(g)
	}
}

// ended settles what the groups had handed a durable member whose session
// has ended, once the store's ending of it has split them (Dropped): the
// returned go back on their group's list, the lost and the unreturned are
// let go and counted, the orphaned with their group's backlog, and one the
// member had answered with a PUBREC is let go. The groups whose cursors ended
// with it end here. Where the store refused the ending (err), the same split
// is made in memory, from the entries themselves.
func (d *bdrain) ended(hs []handed, dropped store.Dropped, err error) {
	for _, g := range dropped.Groups {
		d.endGroup(g)
	}
	type key struct {
		group string
		off   uint64
	}
	returned, lost, orphaned, unreturned := map[key]bool{}, map[key]bool{}, map[key]bool{}, map[key]bool{}
	for g, offs := range dropped.Returned {
		for _, off := range offs {
			returned[key{g, off}] = true
		}
	}
	for _, f := range dropped.Lost {
		lost[key{f.Group, f.Offset}] = true
	}
	for _, f := range dropped.Orphaned {
		orphaned[key{f.Group, f.Offset}] = true
	}
	for _, f := range dropped.Unreturned {
		unreturned[key{f.Group, f.Offset}] = true
	}
	back := map[string][]uint64{}
	charges := map[uint64]int64{}
	var letGo []uint64
	for _, h := range hs {
		f, k := h.f, key{h.f.Group, h.f.Offset}
		g := d.group(f.Group)
		switch {
		case f.QoS == 2 && f.State == store.MessageReleased:
		case err == nil && returned[k], err != nil && f.QoS == 1 && g != nil:
			back[f.Group] = append(back[f.Group], f.Offset)
			charges[f.Offset] = h.charge
			continue
		case err == nil && lost[k], err != nil && f.QoS == 2:
			d.droppedHanded(&d.b.counted.sharesDroppedMemberEnded, 1)
		case err == nil && unreturned[k]:
			d.droppedHanded(&d.b.counted.sharesDroppedStorageFull, 1)
		case err == nil && orphaned[k], err != nil && g == nil:
			d.droppedHanded(&d.b.counted.sharesDroppedNoMember, 1)
		}
		letGo = append(letGo, f.Offset)
	}
	for group, offs := range back {
		if g := d.group(group); g != nil {
			d.returnToGroup(g, offs, charges)
			continue
		}
		d.droppedHanded(&d.b.counted.sharesDroppedNoMember, len(offs))
		letGo = append(letGo, offs...)
	}
	d.letGo(letGo...)
}

// droppedHanded counts n deliveries a group drops that are not on its list
// as held, and then as dropped with cause: one it had handed out, and so
// counted drained, and one it never held - refused at its bound, lost to a
// full log, or ended with its group at a start. **Both, or the identity
// breaks**: held less drained less dropped is what the groups hold (RFC
// 0005), and a delivery counted dropped and never held is subtracted from a
// sum it was never added to - the sum falls one short for each, and a panel
// of it goes negative.
func (d *bdrain) droppedHanded(cause *atomic.Uint64, n int) {
	d.b.counted.sharesHeld.Add(uint64(n))
	cause.Add(uint64(n))
}

// expireGroups drops, from every group's list, what has waited longer than
// broker.share.expires_after since it was published - counted as expired,
// the label naming the knob - and wakes each group that lost any, so its
// cursor is written (RFC 0002 `broker.share`), and answers how many went.
// What is in flight to a member is the member's, and stays.
func (d *bdrain) expireGroups(now time.Time, after time.Duration) int {
	if after <= 0 {
		return 0
	}
	expired := 0
	d.groups.Range(func(_, v any) bool {
		g := v.(*owed)
		g.mu.Lock()
		var offs []uint64
		for _, e := range g.list {
			if e.waiting() {
				offs = append(offs, e.offset)
			}
		}
		g.mu.Unlock()
		if len(offs) == 0 {
			return true
		}
		recs, err := d.log.ReadAt(offs...)
		if err != nil {
			d.b.log.Error("cannot read what a shared group holds to expire it", "group", d.b.limits.Loggable(g.client),
				"error", err)
			return true
		}
		var old []uint64
		for _, r := range recs {
			if now.Sub(r.Timestamp) > after {
				old = append(old, r.Offset)
			}
		}
		if len(old) == 0 {
			return true
		}
		g.mu.Lock()
		gone := g.dropLocked(old)
		g.mu.Unlock()
		if len(gone) > 0 {
			expired += len(gone)
			d.b.counted.sharesDroppedExpired.Add(uint64(len(gone)))
			d.b.log.Info("dropped what a shared group had held past its expiry",
				"group", d.b.limits.Loggable(g.client), "deliveries", len(gone), "expires_after", after)
			d.letGo(gone...)
			d.wakeGroup(g)
		}
		return true
	})
	return expired
}

// Backlog is what a shared group holds and has not handed out, oldest first,
// read from the broadcast log: its backlog (RFC 0003 "Broadcast"). What is in
// flight to a member is the member's, and is not here. Empty for a group with
// no cursor. For the test harness to read back (internal/brokertest), which,
// being another package, cannot reach an export_test.go: nothing else calls
// it, and no operations route serves it.
func (b *Broker) Backlog(group string) ([]store.ShareMessage, error) {
	d := b.broadcastDrain()
	if d == nil {
		return nil, nil
	}
	g := d.group(group)
	if g == nil {
		return nil, nil
	}
	g.mu.Lock()
	var offs []uint64
	for _, e := range g.list {
		if e.lent == nil {
			offs = append(offs, e.offset)
		}
	}
	g.mu.Unlock()
	if len(offs) == 0 {
		return nil, nil
	}
	recs, err := d.log.ReadAt(offs...)
	if err != nil {
		return nil, err
	}
	out := make([]store.ShareMessage, 0, len(recs))
	for _, r := range recs {
		out = append(out, store.ShareMessage{Seq: r.Offset, QoS: r.QoS, Record: r})
	}
	return out, nil
}

// shareTopicFilter is the topic filter of a shared group's `$share/<name>/`
// filter: what its members' messages match.
func shareTopicFilter(group string) string {
	parts := strings.SplitN(group, "/", 3)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// keepForGroups takes out of a channel's shared selection every group with a
// cursor on the broadcast log, at QoS 1 or 2, and keeps one copy of the
// record in the log owed to those groups (store.Record.ForGroups): from
// there each group's drain hands it to a member, returns it at a member's
// ending and holds it while its members are away, as it does a broadcast
// (RFC 0003 "Broadcast"). One write, before the publisher is answered.
//
// **A group over a channel follows the channel at subscribe**: its cursor is
// where the log was when it began, so it is owed what is published after,
// with no replay and no current-state pass, and it holds no position on the
// channel, whose consumers it never moves.
//
// A full log with nothing it can give up loses it for each group, counted
// storage_full (lostToFull). A store that fails otherwise puts the groups
// back, served live: the record is already the channel's, and its publisher
// is answered on that.
func (d *bdrain) keepForGroups(subs *mqtt.Subscribers, pk packets.Packet) {
	if pk.FixedHeader.Qos == 0 {
		return
	}
	taken := map[string]map[string]packets.Subscription{}
	var groups []string
	for filter, members := range subs.Shared {
		if d.group(filter) == nil {
			continue
		}
		taken[filter] = members
		groups = append(groups, filter)
		delete(subs.Shared, filter)
	}
	if len(groups) == 0 {
		return
	}
	rec := d.b.record(pk, nil, "", pk.Origin)
	rec.ForGroups = true
	if _, _, err := d.keep(rec, nil, groups); err != nil {
		for filter, members := range taken {
			subs.Shared[filter] = members
		}
		d.b.log.Error("cannot keep a channel's record for its shared groups in the broadcast log; it is served live",
			"topic", d.b.limits.Loggable(pk.TopicName), "error", err)
	}
}
