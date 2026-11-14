package main

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/s2"
	_ "github.com/mattn/go-sqlite3" // SQLite, as the operator's own tools read the broker's database
)

// uuidV7 is the form of a saguin-id the broker makes (design.md, Sagüin
// properties: "Otherwise the broker makes a UUIDv7"; RFC 9562: version 7,
// variant 10).
var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// mostErrors is how many record errors one check reports.
const mostErrors = 8

// pageSize is the size over which the broker compresses a payload:
// design.md, Store, Compression, "A payload over 4 KiB, SQLite's page
// size".
const pageSize = 4 << 10

// asReceived is a payload as received from its stored form and its
// payload_size: storage.md, A stored payload, "A stored payload is the
// payload as received or its S2 block encoding
// (github.com/klauspost/compress/s2)." design.md, Store, Compression defines
// the choice: "A payload over 4 KiB, SQLite's page size, is compressed with
// S2 and stored compressed only where that makes it shorter". storage.md, A
// stored payload: "A stored payload shorter than its payload_size is
// compressed; one of equal length is not." It says whether the stored
// form was compressed.
func asReceived(stored []byte, size int64) (payload []byte, compressed bool, err error) {
	switch n := int64(len(stored)); {
	case n == size:
		return stored, false, nil
	case n > size:
		return nil, false, fmt.Errorf("a stored payload of %d bytes over its payload_size %d (storage.md: CHECK (length(payload) <= payload_size))", n, size)
	case size <= pageSize:
		return nil, false, fmt.Errorf("a payload of %d bytes stored compressed, in %d: only one over 4 KiB is (design.md, Store, Compression)", size, n)
	}
	payload, err = s2.Decode(nil, stored)
	switch {
	case err != nil:
		return nil, true, fmt.Errorf("a stored payload of %d bytes, payload_size %d, is not S2's block encoding: %w", len(stored), size, err)
	case int64(len(payload)) != size:
		return nil, true, fmt.Errorf("a stored payload decodes to %d bytes; payload_size %d", len(payload), size)
	}
	return payload, true, nil
}

// checkRecords holds the records stored since the last check to the
// messages the run sent (storage.md, records): each record is one message
// the model expects may be stored, stored once, with its topic, QoS,
// publisher ("the publisher's client id", design.md, Words: the one
// assigned it where it connected with none, CONNECT, 8), RETAIN bit and
// saguin-id; and every message acknowledged with
// PUBACK 0x00 is stored (design.md, Flows, Replies: "PUBACK [...] sent only once the store
// confirms"). A message the model predicts refused is never one: its record
// is an error, as is a second record of one message (the run sends each
// once, so "each copy stored" makes one record per copy). A Will is one
// once published, and checked further (checkWillRecord); once the run has
// stopped the broker, its Wills are checked against the wills table too
// (checkWillsAfterStop). A latest channel's records are checked against
// its topics' current changes (latest.go), a queue's jobs and dead letters
// against the jobs the run sent (queues.go). Without a database to read, it
// checks nothing.
func (f *fuzzer) checkRecords() error {
	f.observeWills(time.Now())
	if f.dbPath == "" {
		return nil
	}
	if err := f.openDB(); err != nil {
		return err
	}
	names, err := f.channelNames()
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range slices.Sorted(maps.Keys(names)) {
		switch name := names[id]; {
		case isLatest(name):
			errs = append(errs, f.checkLatestRecords(id, name)...)
		case isDeadLetter(name):
			// Read with its queue's jobs, in one transaction (checkQueues).
		default:
			errs = append(errs, f.checkChannelRecords(id, name)...)
		}
	}
	errs = append(errs, f.checkRetention()...)
	errs = append(errs, f.checkQueues(names)...)
	return f.checkMissing(errs)
}

// checkChannelRecords checks the records stored on the channel id, named
// name, since the last check: each on the channel the model routes its
// topic to (design.md, Words: Channel).
func (f *fuzzer) checkChannelRecords(id int64, name string) []error {
	rows, err := f.db.Query(`SELECT r.channel, r."offset", r.id, r.time, r.topic, r.qos, r.publisher, coalesce(b.name, ''), r.expires, r.properties, r.payload,
		r.payload_size, r.published_retained FROM records r LEFT JOIN bridges b ON b.id = r.bridge WHERE r.channel = ? AND r."offset" > ? ORDER BY r."offset"`, id, f.read[id])
	if err != nil {
		return []error{fmt.Errorf("reading the records of %s: %w", f.dbPath, err)}
	}
	defer func() { _ = rows.Close() }() // read whole, or the check has failed
	var errs []error
	for rows.Next() {
		var r record
		var stored []byte
		var size int64
		if err := rows.Scan(&r.channel, &r.offset, &r.id, &r.time, &r.topic, &r.qos, &r.publisher, &r.bridge, &r.expires, &r.properties, &stored, &size, &r.retained); err != nil {
			return append(errs, fmt.Errorf("reading the records of %s: %w", f.dbPath, err))
		}
		r.channelName = name
		f.read[id] = r.offset
		payload, compressed, err := asReceived(stored, size)
		if err != nil {
			errs = append(errs, fmt.Errorf("the record at channel %d offset %d: %w", r.channel, r.offset, err))
			continue
		}
		r.payload = payload
		f.stored.count(size, compressed)
		if name != broadcast {
			f.onChannels++
		}
		key := recordKey(r.topic, r.payload)
		m := f.messages[key]
		delete(f.messages, key)
		if m != nil && m.hidden != 0 {
			if err := f.learn(m, r.publisher); err != nil {
				errs = append(errs, fmt.Errorf("the record at channel %d offset %d: %w", r.channel, r.offset, err))
			}
		}
		if err := r.matches(f, m); err != nil {
			errs = append(errs, fmt.Errorf("the record at channel %d offset %d: %w", r.channel, r.offset, err))
		}
		if m != nil && m.channel != "" && m.channel != broadcast {
			stored := r
			stored.payload, stored.properties = nil, nil // what compare reads is kept, no body
			m.stored.Store(&stored)
			if seen := m.seen.Load(); seen != nil {
				if err := r.compare(*seen, m.tag, "a subscriber"); err != nil {
					errs = append(errs, err)
				}
			}
		}
		if m != nil {
			if m.will != nil {
				m.will.found = fmt.Sprintf("channel %d offset %d", r.channel, r.offset)
			}
			m.payload = ""
		} // delivery checks retain metadata, never a body at rest
	}
	if err := rows.Err(); err != nil {
		errs = append(errs, fmt.Errorf("reading the records of %s: %w", f.dbPath, err))
	}
	return errs
}

// checkMissing adds to errs, the record checks', every message acknowledged
// but not found, but those retention deleted (checkRetention) and a queue's
// jobs, which checkQueues holds to account, the channels' limits and,
// after the stop, the Wills.
func (f *fuzzer) checkMissing(errs []error) error {
	errs = append(errs, f.checkLimits()...)
	missing := slices.SortedFunc(maps.Values(f.messages), func(a, b *message) int { return cmp.Compare(a.payload, b.payload) })
	for _, m := range missing {
		if m.fateIs(stored) && !m.deleted && m.job == nil {
			errs = append(errs, fmt.Errorf("%q, %q from %s at QoS %d (step %d), acknowledged with PUBACK 0x00, is not in the database (R4)",
				m.topic, m.payload, m.publisher, m.qos, m.step))
		}
	}
	if f.stopped {
		errs = append(errs, f.checkWillsAfterStop()...)
	}
	if len(errs) > mostErrors {
		errs = append(errs[:mostErrors], fmt.Errorf("and %d more", len(errs)-mostErrors))
	}
	return errors.Join(errs...)
}

// learn gives m, sent on an MQTT 3.1.1 connection with an empty client id,
// the identifier the broker assigned that connection, which its client
// never sees (MQTT-3.1.3-6): the publisher of the connection's first
// record found, got for m's own, which must be in an Assigned Client
// Identifier's form (design.md, CONNECT, 8: "the CONNECT's time in UTC as
// YYMMDDHHMMSS followed by six random letters or digits"). Every later
// record of the connection must name the same.
func (f *fuzzer) learn(m *message, got string) error {
	if id, ok := f.hiddenIDs[m.hidden]; ok {
		m.publisher = id
		return nil
	}
	f.hiddenIDs[m.hidden], m.publisher = got, got
	if !assignedForm.MatchString(got) {
		return fmt.Errorf("publisher %q of %q, sent at step %d by an MQTT 3.1.1 client connected with an empty client id: "+
			"want the identifier assigned it, YYMMDDHHMMSS and six letters or digits", got, m.payload, m.step)
	}
	return nil
}

// largeStored counts the records over 4 KiB the run read, as stored
// compressed and as received: a run whose large payloads were never stored
// both ways did not test compression (design.md, Testing: "A run that
// tested nothing never passes").
type largeStored struct{ compressed, asReceived int }

// count counts a record of size bytes as received, stored compressed or
// not.
func (l *largeStored) count(size int64, compressed bool) {
	switch {
	case compressed:
		l.compressed++
	case size > pageSize:
		l.asReceived++
	}
}

// tested returns an error unless l counted records over 4 KiB stored both
// ways: floods send sensor readings, which S2 makes shorter, and random
// bytes, which it does not (newPublish).
func (l *largeStored) tested() error {
	if l.compressed == 0 || l.asReceived == 0 {
		return fmt.Errorf("the run read %d records over 4 KiB stored compressed and %d stored as received; want some of each (design.md, Store, Compression)",
			l.compressed, l.asReceived)
	}
	return nil
}

// record is one row of storage.md's records, as far as the run reads it.
type record struct {
	channel, offset int64
	id, topic       string
	time            int64
	qos             byte
	publisher       string
	bridge          string // the name of the bridge it arrived over, from storage.md's bridges; "" for none
	expires         sql.NullInt64
	properties      []byte
	payload         []byte
	retained        bool // published_retained
	channelName     string
}

// matches checks r against m, the message whose key (recordKey) it holds,
// nil where the model expects none.
func (r *record) matches(f *fuzzer, m *message) error {
	if m == nil {
		return fmt.Errorf("topic %q, payload %q, publisher %q: %s", r.topic, r.payload, r.publisher, f.unexpected(r))
	}
	var errs []error
	switch {
	case r.topic == m.topic && r.qos == m.qos && r.publisher == m.publisher && r.retained == m.retain:
	case m.will != nil:
		errs = append(errs, fmt.Errorf("topic %q, QoS %d, publisher %q, published_retained %v; want %q, QoS %d, publisher %q, published_retained %v: %s, its client's record",
			r.topic, r.qos, r.publisher, r.retained, m.topic, m.qos, m.publisher, m.retain, m.will.describe()))
	default:
		errs = append(errs, fmt.Errorf("topic %q, QoS %d, publisher %q, published_retained %v; the PUBLISH of step %d sent %q, QoS %d, from %q, RETAIN %v",
			r.topic, r.qos, r.publisher, r.retained, m.step, m.topic, m.qos, m.publisher, m.retain))
	}
	if m.will != nil {
		errs = append(errs, r.checkWillRecord(m)...)
	}
	if r.bridge != m.bridge {
		// storage.md, records: bridge, "the id of the bridge it arrived over,
		// 0 for none: such a record never leaves over a bridge".
		errs = append(errs, fmt.Errorf("arrived over the bridge %q, want %q (storage.md, records: bridge)", r.bridge, m.bridge))
	}
	if m.channel != "" && r.channelName != m.channel {
		errs = append(errs, fmt.Errorf("on channel %s, want %s: the channel whose filter spells %q out most exactly, $broadcast where none matches (design.md, Words: Channel)",
			r.channelName, m.channel, m.topic))
	}
	if m.fateIs(full) {
		errs = append(errs, errors.New("stored, though refused with PUBACK 0x97 for its channel's max_bytes"))
	}
	switch {
	case m.id != "" && r.id != m.id:
		errs = append(errs, fmt.Errorf("id %q, want the PUBLISH's first saguin-id %q (design.md, Sagüin properties)", r.id, m.id))
	case m.id == "" && !uuidV7.MatchString(r.id):
		errs = append(errs, fmt.Errorf("id %q, want a UUIDv7 (design.md, Sagüin properties)", r.id))
	}
	return errors.Join(errs...)
}

// unexpected says what r, a record the model expects none of, is, and why
// it expects none.
func (f *fuzzer) unexpected(r *record) string {
	if tag := willTagOf(r.topic); tag != "" {
		if w := f.wills[tag]; w != nil {
			return fmt.Sprintf("%s, of which the model expects no record: %s", w.describe(), w.unexpected())
		}
		return "a Will topic no CONNECT of the run named"
	}
	tag, _, _ := strings.Cut(string(r.payload), " ")
	step, n, ok := strings.Cut(tag, ".")
	if ok && step != "" && n != "" && strings.Trim(step+n, "0123456789") == "" {
		return fmt.Sprintf("a PUBLISH sent at step %s, of which the model expects no record: refused, held at QoS 2 awaiting its PUBREL, or stored already", step)
	}
	return "no message the run sent"
}
