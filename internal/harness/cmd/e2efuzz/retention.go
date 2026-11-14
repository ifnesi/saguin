package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Retention on the configured channels with a retention_bytes (channels.go:
// cr). design.md, Words: Retention: "retention deletes a channel's oldest
// records by age and size, as Kafka does. A record past its channel's
// retention is gone, whoever still needed it: a slow or away consumer, a
// message sent and not yet acknowledged"; "Every committed change that
// affects size schedules a size pass for its channel"; config.md,
// channels.<name>, retention_bytes: "removes the oldest beyond it". When a
// pass runs is the broker's to choose, so the model never requires one: it
// checks what the passes did, at each record check (checkRetention), and
// owes no message a pass deleted (owed, awaitOwed), nor finds one missing
// (checkMissing). Store reads: "If retention removes a record before the read
// reaches it, the record is skipped for that consumer".

// retentionLog is what the model knows of one channel with a
// retention_bytes.
type retentionLog struct {
	max int64 // its retention_bytes
	// tracked are its messages that may be stored and are not known
	// deleted, in sending order; deleted counts those found deleted, for
	// the run's last lines.
	tracked []*message
	deleted int
}

// track keeps m, which the broker may store on a channel with a
// retention_bytes, with the bytes its record charges (messageBytes). Only
// PUBLISHes land there: no Will topic is drawn on cr's levels (willTopic).
func (f *fuzzer) track(m *message) {
	if l := f.retention[m.channel]; l != nil {
		m.bytes = messageBytes(m.topic, len(m.payload), m.props)
		l.tracked = append(l.tracked, m)
	}
}

// before reports whether a's record surely has a lower offset than b's:
// a's intake was confirmed, by its PUBACK or a later PINGRESP, before b was
// sent, and "the core assigns [an offset] when a publication enters the
// channel, in entry order" (design.md, Words: Offset).
func before(a, b *message) bool {
	c := a.confirmed.Load()
	return c != 0 && c < b.serial
}

// checkRetention checks each channel with a retention_bytes against what
// its records show since the last check. A message the model knows stored
// (acknowledged, or its record found) that the channel no longer holds was
// deleted. Two rules follow, exact as far as the run knows offsets (before):
//   - oldest first: no record deleted is newer than one held;
//   - only beyond the limit: the newest record deleted, with the records
//     newer than it then, made more than retention_bytes. Those newer
//     records are all still held, as anything deleted after it is newer
//     still, so its bytes and the bytes held now pass the limit. Which
//     deleted record is the newest is not always known: one of those that
//     may be must pass it.
func (f *fuzzer) checkRetention() []error {
	errs := make([]error, 0, len(f.retention))
	for _, name := range slices.Sorted(maps.Keys(f.retention)) {
		errs = append(errs, f.retention[name].check(f, name)...)
	}
	return errs
}

// check is checkRetention for the channel named, l.
func (l *retentionLog) check(f *fuzzer, name string) []error {
	held, total, err := f.heldTags(name)
	if err != nil {
		return []error{err}
	}
	var kept, present, deleted, unknown []*message
	for _, m := range l.tracked {
		switch {
		case m.fateIs(full):
			// Refused for room: never stored.
		case held[m.tag]:
			kept, present = append(kept, m), append(present, m)
		case m.fateIs(stored) || m.stored.Load() != nil:
			deleted = append(deleted, m)
		default:
			// Never acknowledged nor found: perhaps never stored, perhaps
			// stored and deleted since.
			kept, unknown = append(kept, m), append(unknown, m)
		}
	}
	l.tracked = kept
	if len(deleted) == 0 {
		return nil
	}
	var errs []error
	for _, d := range deleted {
		d.deleted = true
		if i := slices.IndexFunc(present, func(p *message) bool { return before(p, d) }); i >= 0 {
			p := present[i]
			errs = append(errs, fmt.Errorf("channel %s no longer holds %s (step %d) but still holds %s (step %d), stored before it: retention deletes the oldest records (design.md, Words: Retention)",
				name, d.tag, d.step, p.tag, p.step))
		}
	}
	l.deleted += len(deleted)
	beyond := slices.ContainsFunc(slices.Concat(deleted, unknown), func(c *message) bool {
		newest := !slices.ContainsFunc(deleted, func(d *message) bool { return before(c, d) })
		return newest && c.bytes+total > l.max
	})
	if !beyond {
		tags := make([]string, len(deleted))
		for i, d := range deleted {
			tags[i] = d.tag
		}
		errs = append(errs, fmt.Errorf("channel %s no longer holds %s, though each record it may have deleted last, with the %d bytes it holds, fits within its retention_bytes %d: retention removes only the oldest beyond it (config.md, channels.<name>)",
			name, strings.Join(tags, ", "), total, l.max))
	}
	return errs
}

// heldTags reads the records the channel named holds: each one's payload
// tag (fill), the run's own for each message, and the bytes they hold
// together (storage.md: "A message's bytes are its topic's, payload's and
// properties' bytes, the payload counted at its size as received").
func (f *fuzzer) heldTags(name string) (held map[string]bool, total int64, err error) {
	if openErr := f.openDB(); openErr != nil {
		return nil, 0, openErr
	}
	rows, err := f.db.Query(`SELECT r.payload, r.payload_size, length(CAST(r.topic AS BLOB)) + r.payload_size + COALESCE(length(r.properties), 0)
		FROM records r JOIN channels c ON c.id = r.channel WHERE c.name = ?`, name)
	if err != nil {
		return nil, 0, fmt.Errorf("reading the records of channel %s: %w", name, err)
	}
	defer func() { _ = rows.Close() }() // read whole, or the check has failed
	held = map[string]bool{}
	for rows.Next() {
		var stored []byte
		var size, bytes int64
		if err = rows.Scan(&stored, &size, &bytes); err != nil {
			return nil, 0, fmt.Errorf("reading the records of channel %s: %w", name, err)
		}
		payload, _, err := asReceived(stored, size)
		if err != nil {
			return nil, 0, fmt.Errorf("a record of channel %s: %w", name, err)
		}
		tag, _, _ := strings.Cut(string(payload), " ")
		held[tag] = true
		total += bytes
	}
	return held, total, rows.Err()
}

// goneByRetention reports whether m, owed and not come, is on a channel
// with a retention_bytes that no longer holds it: deleted since the last
// record check, so no longer owed, the next check holding the deletion to
// checkRetention's rules.
func (f *fuzzer) goneByRetention(m *message) (bool, error) {
	if f.retention[m.channel] == nil || f.dbPath == "" {
		return false, nil
	}
	held, _, err := f.heldTags(m.channel)
	return err == nil && !held[m.tag], err
}

// retentionDeleted counts the records found deleted by retention, every
// channel's, for the run's last lines.
func (f *fuzzer) retentionDeleted() int {
	n := 0
	for _, l := range f.retention {
		n += l.deleted
	}
	return n
}
