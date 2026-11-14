package main

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/ifnesi/saguin/internal/payload"
	_ "github.com/mattn/go-sqlite3" // SQLite, as the operator's own tools read the broker's database
)

// An outcome is one run: its clients, and what they and the broker's
// database say once it has ended.
type outcome struct {
	run       *run
	size      size
	subs      []*subscriber
	pubs      []*publisher
	rate      float64   // the fixed mode's target, per second; zero for the maximum mode
	wallEpoch time.Time // the epoch, on the wall clock the records' times use
	config    string    // the broker's resolved configuration, defaults filled in
	// target is what the run drove: saguin, peer or mosquitto. Only
	// Sagüin's database is read; the others are accounted on the wire.
	target    string
	addr      string    // the target's address, for the reconnect experiment
	resources resources // the measured window's
	// observed is how long after the measured window the run observed,
	// up to observeLimit: until every QoS 1 publication was acknowledged
	// and every delivery received, or the cutoff.
	observed time.Duration
	// backlog is the deliveries owed and not yet received at the measured
	// window's start and end.
	backlog [2]int64
	// seconds are live samples of whole measured seconds; disk supplies
	// each sample's drive latency and phase when monitoring is enabled.
	seconds []second
	disk    *diskMonitor
	// peakRSS is the broker's peak resident memory and clientRSS the load
	// tool's, in bytes, over their lifetime through the observation.
	peakRSS, clientRSS uint64
	// brokerMemory and toolMemory describe their cgroups' memory after the
	// observation: peak, limits, out-of-memory kills.
	brokerMemory, toolMemory string

	// By QoS, in the measured window: publications begun (attempted),
	// written to the socket, acknowledged (QoS 1), and surviving records
	// whose intake time is in the window, read once the broker stopped
	// (-1 for the peer, which stores nothing).
	attempted, written, acked, recordsIn [2]int64
	// sent is the publications written over the whole run, by QoS;
	// unacknowledged, the QoS 1 ones still awaiting their PUBACK at the
	// cutoff, ackedLate those acknowledged after it.
	sent                      [2]int64
	unacknowledged, ackedLate int64
	// The fixed mode's schedule in the window: slots due, missed (skipped
	// for lateness), and due but unsent when the publishers stopped; and
	// each sent publication's delay from its due time.
	scheduled, missed, unsent int64
	cut                       int64 // publication writes cut at the run's end, unsent
	delay                     histogram
	// arrived counts the distinct deliveries received in the measured
	// window; writtenBytes and arrivedBytes, the payload bytes written to
	// the socket and received in it.
	arrived, writtenBytes, arrivedBytes int64
	// The cohort is the publications sent in the measured window, owed
	// once to each subscriber of their group; received counts those
	// received by the cutoff, worstCohort the lowest share any one
	// subscriber received.
	cohortOwed, cohortReceived int64
	worstCohort                float64
	// Latencies of the cohort's received deliveries, from send and from
	// due time.
	sendLatency, dueLatency histogram
	// ackLatency is the QoS 1 publications' time from send to PUBACK, for
	// those sent in the window.
	ackLatency histogram

	// Every delivery owed is in exactly one wire category for every target.
	// A gap precedes a later receipt from its publisher; a tail does not.
	owed, received, receivedLate, gap, tail [2]int64
	// Database evidence for missing deliveries, beside the wire categories.
	// notTaken counts an unstored tail; the name does not establish admission.
	pending, belowFloor, absentBelow, neverStored, notTaken, skipped, unknown [2]int64
	// Known gap causes and gaps for which the evidence establishes no cause.
	gapTooLarge, gapExpired, gapRetention, gapUnknown [2]int64
	// Publications provably never stored: after the publisher's last stored
	// one (admission unproven), or before a later stored one.
	unstoredTail, unstoredGap [2]int64
	// ackedUnstored counts QoS 1 publications acknowledged but provably not
	// stored (R4); duplicates and reordered, deliveries received twice or
	// after a later one of the same publisher.
	ackedUnstored, duplicates, reordered int64
	resends                              int64 // reconnect resends with DUP, counted apart
	// floor and next are $broadcast's floor and tail at the end: a floor
	// above 1 means retention has passed records.
	floor, next int64
}

// stored is what the broker's database holds of the run once the broker
// has stopped (storage.md): each publication's record offset, the records
// with intake in the measured window by QoS, $broadcast's floor and tail,
// and each subscriber's position and records in flight on $broadcast.
type stored struct {
	offsets     [][]int64 // by publisher and sequence number; 0 where no record
	inWindow    [2]int64
	floor, next int64
	positions   []int64          // by subscriber; 0 where none is stored
	inflight    []map[int64]bool // by subscriber: offsets of exchanges in flight
}

// readStore reads the database at path, read-only, after the broker has
// stopped.
func readStore(path string, o *outcome) (*stored, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { _ = db.Close() }() // read-only: nothing to lose
	st := &stored{offsets: make([][]int64, len(o.pubs)), positions: make([]int64, len(o.subs)),
		inflight: make([]map[int64]bool, len(o.subs))}
	for i, p := range o.pubs {
		st.offsets[i] = make([]int64, p.sent.Load())
	}
	var channel int64
	if err := db.QueryRow(`SELECT id, floor, next FROM channels WHERE name = '$broadcast'`).Scan(&channel, &st.floor, &st.next); err != nil {
		return nil, fmt.Errorf("reading $broadcast in %s: %w", path, err)
	}
	if err := st.records(db, channel, o); err != nil {
		return nil, fmt.Errorf("reading the records of %s: %w", path, err)
	}
	if err := st.sessions(db, channel, o); err != nil {
		return nil, fmt.Errorf("reading the sessions of %s: %w", path, err)
	}
	return st, nil
}

// records reads $broadcast's records, each one a publication of the run.
func (st *stored) records(db *sql.DB, channel int64, o *outcome) error {
	rows, err := db.Query(`SELECT "offset", qos, time, payload, payload_size FROM records WHERE channel = ?`, channel)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }() // read whole, or the run has failed
	from := o.wallEpoch.Add(warmUp).UnixMilli()
	to := o.wallEpoch.Add(warmUp + measuredWindow).UnixMilli()
	for rows.Next() {
		var offset, at int64
		var qos byte
		var body []byte
		var size int
		if err := rows.Scan(&offset, &qos, &at, &body, &size); err != nil {
			return err
		}
		body, err := payload.Decode(body, size)
		if err != nil {
			return fmt.Errorf("offset %d: %w", offset, err)
		}
		s, err := readStamp(body)
		switch {
		case err != nil:
			return fmt.Errorf("offset %d: %w", offset, err)
		case s.publisher < 0 || s.publisher >= len(o.pubs) || qos != o.run.qos[s.publisher] || s.seq >= uint64(len(st.offsets[s.publisher])):
			return fmt.Errorf("offset %d: publisher %d's publication %d at QoS %d, which the run never sent", offset, s.publisher, s.seq, qos)
		case st.offsets[s.publisher][s.seq] != 0:
			return fmt.Errorf("publisher %d's publication %d is stored twice", s.publisher, s.seq)
		}
		st.offsets[s.publisher][s.seq] = offset
		if at >= from && at < to {
			st.inWindow[qos]++
		}
	}
	return rows.Err()
}

// sessions reads each subscriber's position on $broadcast and the records
// of its exchanges in flight.
func (st *stored) sessions(db *sql.DB, channel int64, o *outcome) error {
	ids := map[int64]int{} // session id to subscriber
	rows, err := db.Query(`SELECT id, client FROM sessions`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var client string
		if err := rows.Scan(&id, &client); err != nil {
			return errors.Join(err, rows.Close())
		}
		var i int
		if _, err := fmt.Sscanf(client, "load-sub-%d", &i); err == nil && i < len(o.subs) && client == "load-sub-"+strconv.Itoa(i) {
			ids[id] = i
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for i := range st.inflight {
		st.inflight[i] = map[int64]bool{}
	}
	for _, q := range []struct {
		sql  string
		into func(sub int, offset int64)
	}{
		{`SELECT session, "offset" FROM positions WHERE channel = ?`, func(sub int, off int64) { st.positions[sub] = off }},
		{`SELECT session, "offset" FROM session_inflight WHERE channel = ?`, func(sub int, off int64) { st.inflight[sub][off] = true }},
	} {
		rows, err := db.Query(q.sql, channel)
		if err != nil {
			return err
		}
		for rows.Next() {
			var session, offset int64
			if err := rows.Scan(&session, &offset); err != nil {
				return errors.Join(err, rows.Close())
			}
			if sub, ok := ids[session]; ok {
				q.into(sub, offset)
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
	}
	return nil
}

// The causes of a publication without a record, decided from its
// neighbours of the same publisher (one publisher's records take offsets
// in its order).
const (
	// neverStored: had it been stored, its offset would be at or above the
	// floor, where retention has removed nothing; or retention never ran.
	neverStored byte = iota + 1
	// absentBelow: its offset would be below the floor: removed by
	// retention, or (QoS 0) never stored; which cannot be told apart.
	absentBelow
	// unknownCause: its offset could be on either side of the floor.
	unknownCause
	// notTaken: never stored, after the publisher's last stored one.
	// Neither admission nor the reason for absence is established.
	notTaken
)

// gapFacts are evidence of a permitted skip, never an inference from a
// missing receipt alone. The recipe sends no expiry and advertises no
// subscriber packet-size limit, so only database-proven retention can
// explain a gap in its runs.
type gapFacts struct{ tooLarge, expired, broadcast, retention bool }

// explain names the applicable permitted skip, or leaves its cause unknown.
func explain(target string, f gapFacts) string {
	switch {
	case f.tooLarge:
		return "too_large"
	case f.expired && (target == "mosquitto" || f.broadcast):
		return "expired"
	case f.retention && target != "mosquitto":
		return "retention"
	}
	return ""
}

// absentCauses gives, for each publication of a publisher without a
// record, its cause; zero where it has a record.
func absentCauses(offsets []int64, floor int64) []byte {
	causes := make([]byte, len(offsets))
	var below int64 // the nearest earlier record's offset
	for seq, off := range offsets {
		if off != 0 {
			below = off
			continue
		}
		if floor <= 1 || below+1 >= floor { // its offset is above below
			causes[seq] = neverStored
		} else {
			causes[seq] = unknownCause
		}
	}
	var above int64 // the nearest later record's offset
	for seq := len(offsets) - 1; seq >= 0; seq-- {
		if offsets[seq] != 0 {
			above = offsets[seq]
		} else if causes[seq] == unknownCause && above != 0 && above <= floor {
			causes[seq] = absentBelow
		}
	}
	return causes
}

// account counts what the run sent and what each subscriber received
// against st, what Sagüin stored (nil for other targets). Wire counts do
// not depend on database evidence. Duplicates and reordering fail every
// target; unexplained gaps fail except at Mosquitto's QoS 0. Database
// checks enforce Sagüin's stored-before-acknowledged and delivery promises.
func (o *outcome) account(st *stored) error {
	if o.target == "mosquitto" {
		st = nil // only Sagüin's database can establish its promises
	}
	o.floor, o.recordsIn = 1, [2]int64{-1, -1}
	causes := make([][]byte, len(o.pubs))
	if st != nil {
		o.floor, o.next, o.recordsIn = st.floor, st.next, st.inWindow
	}
	for i, p := range o.pubs {
		o.attempted[p.qos] += p.attempted.Load()
		o.written[p.qos] += p.written.Load()
		o.writtenBytes += p.writtenBytes.Load()
		o.acked[p.qos] += p.acked.Load()
		o.sent[p.qos] += int64(p.sent.Load())
		o.ackedLate += p.ackedLate
		if p.qos == 1 {
			o.unacknowledged += int64(p.sent.Load()) - p.ackedBefore
		}
		o.scheduled += p.scheduled
		o.missed += p.missed
		o.unsent += p.unsent
		o.cut += p.cut
		o.delay.merge(&p.delay)
		o.ackLatency.merge(&p.ackLatency)
		if st == nil {
			continue
		}
		causes[i] = absentCauses(st.offsets[i], st.floor)
		later := false // a stored record of p's after seq
		for seq, c := range slices.Backward(causes[i]) {
			if st.offsets[i][seq] != 0 {
				later = true
			}
			if c != neverStored {
				continue
			}
			if later {
				o.unstoredGap[p.qos]++
			} else {
				o.unstoredTail[p.qos]++
				causes[i][seq] = notTaken
			}
			if p.qos == 1 && p.ackedSeqs.has(uint64(seq)) {
				o.ackedUnstored++
			}
		}
	}
	o.worstCohort = 1
	for i, s := range o.subs {
		o.subscriber(i, s, st, causes)
	}
	var errs []error
	if n := o.skipped[0] + o.skipped[1]; n > 0 {
		errs = append(errs, fmt.Errorf("%d stored publications passed by a subscriber's position, above the floor, and never received", n))
	}
	if o.ackedUnstored > 0 {
		errs = append(errs, fmt.Errorf("%d QoS 1 publications acknowledged but not stored (R4)", o.ackedUnstored))
	}
	if o.duplicates > 0 {
		errs = append(errs, fmt.Errorf("%d deliveries received twice", o.duplicates))
	}
	if o.reordered > 0 {
		errs = append(errs, fmt.Errorf("%d deliveries out of their publisher's order", o.reordered))
	}
	n := o.gapUnknown[1]
	if o.target != "mosquitto" {
		n += o.gapUnknown[0]
	}
	if n > 0 {
		errs = append(errs, fmt.Errorf("%d deliveries missing before a later one of their publisher (gap, cause unknown)", n))
	}
	return errors.Join(errs...)
}

// subscriber adds subscriber i's deliveries to the counts, each
// publication of its group sent in the run classified once.
func (o *outcome) subscriber(i int, s *subscriber, st *stored, causes [][]byte) {
	o.arrived += s.arrived
	o.arrivedBytes += s.arrivedBytes
	o.cohortReceived += s.cohort
	o.duplicates += s.duplicates
	o.resends += s.resends
	o.reordered += s.reordered
	o.sendLatency.merge(&s.sendLatency)
	o.dueLatency.merge(&s.dueLatency)
	var cohortOwed int64
	for k := range groupSize {
		p := o.pubs[s.group*groupSize+k]
		q := p.qos
		cohortOwed += int64(p.cohort[1] - p.cohort[0])
		o.owed[q] += int64(p.sent.Load())
		var last uint64 // the last wire receipt, before or after the cutoff
		if seq, ok := s.seen[k].last(); ok {
			last = max(last, seq+1)
		}
		if seq, ok := s.late[k].last(); ok {
			last = max(last, seq+1)
		}
		for seq := range p.sent.Load() {
			switch {
			case s.seen[k].has(seq):
				o.received[q]++
			case s.late[k].has(seq):
				o.receivedLate[q]++
			default:
				var facts gapFacts
				if st != nil {
					offset, cause := st.offsets[p.index][seq], causes[p.index][seq]
					finding := o.classify(q, st, i, offset, cause)
					*finding++
					// A below-floor absence alone cannot distinguish QoS 0
					// never stored from retention. At QoS 1 a PUBACK proves storage.
					facts.retention = finding == &o.belowFloor[q] || (q == 1 && cause == absentBelow && p.ackedSeqs.has(seq))
				}
				if seq+1 < last {
					o.gap[q]++
					o.gapCause(q, facts)
				} else {
					o.tail[q]++
				}
			}
		}
	}
	o.cohortOwed += cohortOwed
	if cohortOwed > 0 {
		o.worstCohort = min(o.worstCohort, float64(s.cohort)/float64(cohortOwed))
	}
}

// gapCause counts one observed gap under its evidenced cause.
func (o *outcome) gapCause(q byte, facts gapFacts) {
	switch explain(o.target, facts) {
	case "too_large":
		o.gapTooLarge[q]++
	case "expired":
		o.gapExpired[q]++
	case "retention":
		o.gapRetention[q]++
	default:
		o.gapUnknown[q]++
	}
}

// classify is the count a publication at QoS q, not received by
// subscriber sub, belongs in: by its record's offset, or its cause where
// it has none.
func (o *outcome) classify(q byte, st *stored, sub int, offset int64, cause byte) *int64 {
	pos := st.positions[sub]
	switch {
	case offset == 0 && cause == notTaken:
		return &o.notTaken[q]
	case offset == 0 && cause == neverStored:
		return &o.neverStored[q]
	case offset == 0 && cause == absentBelow:
		return &o.absentBelow[q]
	case offset == 0:
		return &o.unknown[q]
	case st.inflight[sub][offset], pos != 0 && offset >= pos:
		return &o.pending[q] // its exchange or the position still delivers it
	case offset < st.floor:
		return &o.belowFloor[q] // retention passed it; kept for another's exchange
	case pos != 0:
		return &o.skipped[q]
	}
	return &o.unknown[q]
}

// coverage is the share of the deliveries owed that were received by the
// cutoff.
func (o *outcome) coverage() float64 {
	owed := o.owed[0] + o.owed[1]
	if owed == 0 {
		return 0
	}
	return float64(o.received[0]+o.received[1]) / float64(owed)
}

// cohortCoverage is the share of the measured cohort's deliveries
// received by the cutoff.
func (o *outcome) cohortCoverage() float64 {
	if o.cohortOwed == 0 {
		return 0
	}
	return float64(o.cohortReceived) / float64(o.cohortOwed)
}

// scheduledShare is the share of the fixed mode's slots due in the window
// that were sent; 1 for the maximum mode.
func (o *outcome) scheduledShare() float64 {
	if o.scheduled == 0 {
		return 1
	}
	return float64(o.scheduled-o.missed-o.unsent) / float64(o.scheduled)
}
