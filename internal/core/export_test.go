package core

import (
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// The seam the stop's tests drive (stop_test.go). design.md, Testing: "The
// core's decisions are a function of its state, one event and that event's
// time. They return what to do"; design.md, Execution, Event order: the core
// takes "stop, store answers, logger results, the core's timer, then the
// readers' channel". Handle, Answer and Fire are the decisions on the
// readers' channel, store answers and the timer; the production code defines
// the first, unexported:
//
//	// beginStop is the core's decision on the stop, which Run takes once
//	// main has called Stop (design.md, Restart, Graceful stop): it
//	// appends what the stop asks for at now, in the order it is to be
//	// done, and returns the extended slice, as Handle does. After it, the
//	// core takes every other event as the graceful stop says.
//	func (c *Core) beginStop(acts []Action, now time.Time) []Action

// BeginStop is beginStop, for the tests outside the package.
func (c *Core) BeginStop(acts []Action, now time.Time) []Action {
	return c.beginStop(acts, now)
}

// CopyGroup copies g's write list before applying the store's answer,
// which clears it. The writes themselves are shared; copy publication
// records with RecordsOf before applying the answer too.
func CopyGroup(g *Group) *Group {
	return &Group{Writes: slices.Clone(g.Writes)}
}

// RecordsOf copies the records w, a write of messages into a channel,
// holds, in the order the store writes them, each with the offset the core
// assigned it (publishBatch.records): the rows storage.md's records
// receives. Call it before applying the store's answer, which clears the
// batch's publications. Any other write holds none.
func RecordsOf(w store.Write) []store.Record {
	b, ok := w.(*publishBatch)
	if !ok {
		return nil
	}
	records := make([]store.Record, 0, b.count)
	for p, offset := range b.records {
		r := p.record
		r.Offset = offset
		records = append(records, r)
	}
	return records
}

// PayloadsOf returns the payload reads r, a read of the payloads of a
// consumer's reserved deliveries, holds, in order: each names one record
// and receives its payload (store.ReadRecordPayload). Any other read holds
// none.
func PayloadsOf(r store.Read) []*store.ReadRecordPayload {
	b, ok := r.(*payloadBatch)
	if !ok {
		return nil
	}
	var reads []*store.ReadRecordPayload
	for _, d := range b.members {
		reads = append(reads, &d.payload)
	}
	return reads
}

// ChargesOf returns the retained charges r, a publication batch's retained
// admission read, reads next: one page, as the store reads it
// (store.RetainedCharge), each topic to receive its current value's bytes;
// and done, which marks that page read. Any other read holds none.
func ChargesOf(r store.Read) (page []*store.RetainedCharge, done func()) {
	p, ok := r.(*publishCharges)
	if !ok {
		return nil, nil
	}
	end := min(p.after+store.MetadataPage, len(p.batch.charges))
	for i := p.after; i < end; i++ {
		page = append(page, &p.batch.charges[i])
	}
	return page, func() { p.after = end }
}

// Serve ends the startup as Run does (loop.go) once the startup's store
// requests are answered: design.md, Restart, Start, "Listeners and bridges
// start last", and Words: Retention, "Startup processes overdue retention
// before serving". A test driving the decisions without Run calls it once
// its scripted store has answered the startup's requests, so that the core
// then decides as a running broker's does; it reports whether the startup
// is over, false while a store request is outstanding.
func (c *Core) Serve() bool {
	if c.phase == starting && c.storeIdle() {
		c.phase = running
		close(c.initialized)
	}
	return c.phase != starting
}

// AdmissionRoom is the room the delivery admission gate's own indexes have
// taken (admission.go, admissionState): its queues of owners' ages, of
// progress waiting for payload memory and of connections' peer deadlines,
// and its list of owners waiting for a refresh, entries included. Each
// entry is an age or a deadline the gate keeps; 0 means it keeps none, nor
// room for any.
func (c *Core) AdmissionRoom() int {
	a := &c.admission
	return cap(a.ages.items) + cap(a.memory.items) + cap(a.peers.items) + cap(a.dirty)
}

// SettleThrough has the consumer of client's connected session on the
// channel with id channel take a settlement through offset, its position
// then advancing as a settlement moves it (advanceConsumer): past the
// records the session's stored exchanges represent, preparations included
// ("a durable session may also pass records represented by stored QoS 1 or
// QoS 2 exchanges", design.md, Delivery, Positions). No acknowledgement a
// client sends settles a delivery beyond one prepared and never handed to
// the writer, so a test reaching a position beyond such a preparation
// takes this settlement. It returns the position.
func (c *Core) SettleThrough(client string, channel, offset int64, now time.Time) int64 {
	u := c.sessions[client].consumer(channel)
	u.settledThrough = max(u.settledThrough, offset)
	c.advanceConsumer(u, now)
	return u.position
}
