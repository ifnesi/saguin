package main

import (
	"bytes"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The tests of 0028k's accounting, against Sagüin and Mosquitto alike.
// The load tool is not in the design: their oracle is the maintainer's
// decisions on the load recipe, quoted where a test rests on one, with the
// MQTT and design sentences they rest on.
// Decision 1, on Astra's finding 1: "the same wire-observable loss
// categories for both brokers (received, received late, gap, unresolved
// tail); Sagüin's database findings reported separately; "unknown" stays
// unknown, never counted as lost".

// smallRun is a run of the small size against target, before it is
// accounted: no publisher has sent anything yet, and its one subscriber,
// of group 0, is owed every publication of the group.
func smallRun(target string) (*outcome, *subscriber) {
	r := newRun(sizes[0], 0)
	s := &subscriber{}
	o := &outcome{run: r, target: target, subs: []*subscriber{s}}
	for i := range sizes[0].publishers {
		o.pubs = append(o.pubs, &publisher{index: i, qos: r.qos[i]})
	}
	return o, s
}

// publisherAt is the first of r's publishers at QoS q: on the small size,
// a publisher of group 0, its index also its place in the group.
func publisherAt(r *run, q byte) int { return bytes.IndexByte(r.qos, q) }

// receivedBy records publications seqs of the publisher at place k of the
// group as s's receive loop records them: received before the cutoff, or
// after it where late.
func receivedBy(s *subscriber, k int, late bool, seqs ...uint64) {
	for _, seq := range seqs {
		if late {
			s.late[k].add(seq)
		} else {
			s.seen[k].add(seq)
		}
		s.last[k] = max(s.last[k], seq+1)
	}
}

// storedAt is Sagüin's database after o's run (storage.md): publisher i's
// publications at offsets[i], none where an offset is 0 or none is given;
// $broadcast's floor; the subscriber's position, none where 0; nothing in
// flight. It is nil for another target, accounted on the wire alone.
func storedAt(o *outcome, offsets map[int][]int64, floor, position int64) *stored {
	if o.target != "saguin" {
		return nil
	}
	st := &stored{floor: floor, next: 100, positions: []int64{position}, inflight: []map[int64]bool{{}}}
	for i, p := range o.pubs {
		off := make([]int64, p.sent.Load())
		copy(off, offsets[i])
		st.offsets = append(st.offsets, off)
	}
	return st
}

// TestUnknownNotLost checks the maintainer's test 3 for 0028k, "unknown
// never counted as lost", from decision 1's "unknown stays unknown, never
// counted as lost": a QoS 0 publication stored, which the subscriber never
// received, its position not stored, so the database cannot say whether it
// is owed still or was passed, counts as unknown, and the run's lost
// (results file, Lost) does not count it. On the wire it is the unresolved
// tail, after the last publication received, which fails no run.
func TestUnknownNotLost(t *testing.T) {
	o, s := smallRun("saguin")
	zero := publisherAt(o.run, 0)
	o.pubs[zero].sent.Store(2)
	receivedBy(s, zero, false, 0)
	if err := o.account(storedAt(o, map[int][]int64{zero: {1, 2}}, 1, 0)); err != nil {
		t.Fatalf("an unresolved tail failed the run: %v", err)
	}
	if o.unknown[0] != 1 || o.lost() != 0 {
		t.Fatalf("unknown %v, lost %d; want 1 unknown at QoS 0 and nothing lost", o.unknown, o.lost())
	}
}

// TestUnstoredTail checks the maintainer's test 4 for 0028k, on Astra's
// finding 2 ("A database absence cannot distinguish bytes still in TCP
// buffers from publications read and discarded before storage"):
// "Sagüin's unstored tail reported without a cause claimed, an
// acknowledged QoS 1 one still failing the run (R4)". The tail is the
// publications never stored after the publisher's last stored one; the
// report names no cause for it, neither TCP flow control nor publications
// the broker never took. design.md, R4: "Stored before acknowledged. A
// reply confirming a change waits for that change's commit."
func TestUnstoredTail(t *testing.T) {
	o, s := smallRun("saguin")
	one := publisherAt(o.run, 1)
	p := o.pubs[one]
	p.sent.Store(3)
	p.ackedSeqs.add(2)
	receivedBy(s, one, false, 0, 1)
	err := o.account(storedAt(o, map[int][]int64{one: {1, 2}}, 1, 3))
	report := o.String()
	switch {
	case err == nil:
		t.Fatal("a QoS 1 publication acknowledged and never stored passed the run; want it failed (R4)")
	case o.unstoredTail[1] != 1:
		t.Fatalf("unstored tail %v, want 1 at QoS 1", o.unstoredTail)
	case strings.Contains(report, "not taken") || strings.Contains(report, "flow control"):
		t.Fatalf("the report claims a cause for the unstored tail, which the database cannot show:\n%s", report)
	}
}

// TestDuplicatesAndReorderingFailBoth checks decision 2, on Astra's finding
// 6: "duplicates and out-of-order delivery fail a run for both brokers;
// only Sagüin's stored-before-acknowledged checks stay Sagüin-only". The
// recipe's subscribers never reconnect, so nothing is resent to them, and
// each publisher's publications share one topic and one QoS: MQTT 5 §4.6,
// "When a Server processes a message that has been published to an
// Ordered Topic, it MUST send PUBLISH packets to consumers (for the same
// Topic and QoS) in the order that they were received from any given
// Client [MQTT-4.6.0-5]".
func TestDuplicatesAndReorderingFailBoth(t *testing.T) {
	for _, target := range []string{"saguin", "mosquitto"} {
		for _, broken := range []struct {
			name string
			set  func(*subscriber) // as the receive loop counts it
		}{
			{"a delivery received twice", func(s *subscriber) { s.duplicates = 1 }},
			{"a delivery after a later one of its publisher", func(s *subscriber) { s.reordered = 1 }},
		} {
			o, s := smallRun(target)
			k := publisherAt(o.run, 1)
			o.pubs[k].sent.Store(3)
			receivedBy(s, k, false, 0, 1, 2)
			broken.set(s)
			if o.account(storedAt(o, map[int][]int64{k: {1, 2, 3}}, 1, 4)) == nil {
				t.Errorf("%s: %s passed the run; want it failed, for either broker", target, broken.name)
			}
		}
	}
}

// mixedDeliveries is a run against target in which a QoS 1 publisher, one,
// sent five publications, of which the subscriber received 0 and 2 before
// the cutoff and 4 after it, and a QoS 0 publisher, zero, sent four, of
// which it received 0 and 1. Sagüin's database holds them all, one's at
// offsets 1, 3, 5, 7 and 9 and zero's at 2, 4, 6 and 8.
func mixedDeliveries(target string) (o *outcome, offsets map[int][]int64) {
	o, s := smallRun(target)
	one, zero := publisherAt(o.run, 1), publisherAt(o.run, 0)
	o.pubs[one].sent.Store(5)
	o.pubs[zero].sent.Store(4)
	receivedBy(s, one, false, 0, 2)
	receivedBy(s, one, true, 4)
	receivedBy(s, zero, false, 0, 1)
	return o, map[int][]int64{one: {1, 3, 5, 7, 9}, zero: {2, 4, 6, 8}}
}

// wireCounts are o's wire counts, by QoS: received, received late, gap and
// unresolved tail.
func wireCounts(o *outcome) [4][2]int64 {
	return [4][2]int64{o.received, o.receivedLate, o.gap, o.tail}
}

// mixedWire is mixedDeliveries' wire counts, by QoS: received {2, 2};
// received late {0, 1}; a gap {0, 2}, one's 1 and 3, missing before a later
// one received; an unresolved tail {2, 0}, zero's 2 and 3, missing after
// the last received.
var mixedWire = [4][2]int64{{2, 2}, {0, 1}, {0, 2}, {2, 0}}

// TestWireCountsBothBrokers checks decision 1 and the maintainer's test 1
// for 0028k: "the same wire counts (received, received late, gap,
// unresolved tail) for the same deliveries, Sagüin or Mosquitto".
func TestWireCountsBothBrokers(t *testing.T) {
	for _, target := range []string{"saguin", "mosquitto"} {
		o, offsets := mixedDeliveries(target)
		_ = o.account(storedAt(o, offsets, 1, 3)) // whether a gap fails the run is TestGaps'
		if got := wireCounts(o); got != mixedWire {
			t.Errorf("%s: received, received late, gap and tail by QoS %v; want %v, as for the other broker", target, got, mixedWire)
		}
	}
}

// TestDatabaseBesideWire checks decision 1 and the maintainer's test 2 for
// 0028k: "Sagüin's database findings reported beside them, never changing
// them". For the same deliveries, two databases: the subscriber's position
// before the four missing publications, which it still owes (pending), and
// retention past them (below the floor). Each finding is counted, and the
// wire counts stay those of the wire.
func TestDatabaseBesideWire(t *testing.T) {
	for _, db := range []struct {
		name            string
		floor, position int64
		finding         func(*outcome) [2]int64
	}{
		{"pending", 1, 3, func(o *outcome) [2]int64 { return o.pending }},
		{"below the floor", 10, 10, func(o *outcome) [2]int64 { return o.belowFloor }},
	} {
		o, offsets := mixedDeliveries("saguin")
		_ = o.account(storedAt(o, offsets, db.floor, db.position)) // whether a gap fails the run is TestGaps'
		if got := wireCounts(o); got != mixedWire {
			t.Errorf("the missing ones %s in the database: received, received late, gap and tail by QoS %v; want %v, the wire's", db.name, got, mixedWire)
		}
		if got := db.finding(o); got != [2]int64{2, 2} {
			t.Errorf("the missing ones %s in the database: counted %v by QoS; want [2 2]", db.name, got)
		}
	}
}

// TestGaps checks the maintainer's test 12 for 0028k: "a gap fails a run
// unless explained, the explained causes reported with their cause: QoS 1
// for both brokers; QoS 0 counted for both, failing no Mosquitto run (MQTT
// allows dropping QoS 0) but failing a Sagüin run (its design delivers
// every stored record to a connected subscriber in order). Explained:
// too_large for both (MQTT-3.1.2-25); Message Expiry for both
// (MQTT-3.3.2-5), for Sagüin only where its design skips, on $broadcast;
// retention for Sagüin only."
func TestGaps(t *testing.T) {
	// MQTT 5 §4.3.1, QoS 0: "The message arrives at the receiver either
	// once or not at all"; §4.3.2, QoS 1: "This Quality of Service level
	// ensures that the message arrives at the receiver at least once".
	// design.md, Delivery, Positions: "The position advances only past the
	// contiguous settled or skipped prefix of examined records".
	t.Run("unexplained", func(t *testing.T) {
		for _, c := range []struct {
			target string
			qos    byte
			fails  bool
		}{
			{"saguin", 1, true}, {"mosquitto", 1, true},
			{"saguin", 0, true}, {"mosquitto", 0, false},
		} {
			o, s := smallRun(c.target)
			k := publisherAt(o.run, c.qos)
			o.pubs[k].sent.Store(3)
			receivedBy(s, k, false, 0, 2)
			// Publication 1 stored above the floor, the position past it:
			// nothing explains its gap.
			err := o.account(storedAt(o, map[int][]int64{k: {1, 2, 3}}, 1, 4))
			if o.gap[c.qos] != 1 || (err != nil) != c.fails {
				t.Errorf("%s, a QoS %d gap: counted %d, the run's error %v; want it counted, failing the run %v", c.target, c.qos, o.gap[c.qos], err, c.fails)
			}
		}
	})
	// MQTT 5: "Where a Packet is too large to send, the Server MUST discard
	// it without sending it and then behave as if it had completed sending
	// that Application Message [MQTT-3.1.2-25]"; "If the Message Expiry
	// Interval has passed and the Server has not managed to start onward
	// delivery to a matching subscriber, then it MUST delete the copy of
	// the message for that subscriber [MQTT-3.3.2-5]". design.md, Delivery,
	// Message Expiry: "On `$broadcast` an expired message is skipped for
	// that subscriber and counted ... On a configured channel it is still
	// delivered"; Words, Retention: "A channel's retention, `$broadcast`
	// included, may pass a record a session is owed".
	t.Run("explained", func(t *testing.T) {
		for _, c := range []struct {
			target string
			facts  gapFacts
			want   string
		}{
			{"saguin", gapFacts{tooLarge: true}, "too_large"},
			{"mosquitto", gapFacts{tooLarge: true}, "too_large"},
			{"saguin", gapFacts{expired: true, broadcast: true}, "expired"},
			{"saguin", gapFacts{expired: true}, ""},
			{"mosquitto", gapFacts{expired: true}, "expired"},
			{"saguin", gapFacts{retention: true}, "retention"},
			{"mosquitto", gapFacts{retention: true}, ""},
			{"saguin", gapFacts{broadcast: true}, ""},
			{"mosquitto", gapFacts{}, ""},
		} {
			if got := explain(c.target, c.facts); got != c.want {
				t.Errorf("%s, a gap %+v: explained by %q; want %q", c.target, c.facts, got, c.want)
			}
		}
	})
	t.Run("reported with its cause", func(t *testing.T) {
		o, s := smallRun("saguin")
		k := publisherAt(o.run, 1)
		o.pubs[k].sent.Store(4)
		receivedBy(s, k, false, 3)
		// Publications 0 to 2 stored below the floor, the position past
		// them: three gaps retention explains.
		err := o.account(storedAt(o, map[int][]int64{k: {10, 11, 12, 25}}, 20, 26))
		reported := slices.ContainsFunc(strings.Split(o.String(), "\n"), func(line string) bool {
			return strings.Contains(line, "retention") && strings.Contains(line, "3")
		})
		if err != nil || o.gap[1] != 3 || !reported {
			t.Fatalf("three QoS 1 gaps retention explains: the run's error %v, counted %d, reported with their cause %v:\n%s", err, o.gap[1], reported, o.String())
		}
	})
	// Maintainer, 0028k [261026 101053]: "a gap is an observed loss, so one
	// not explained by retention, Message Expiry or too_large is reported
	// "gap, cause unknown" and fails the run, at QoS 1 for both brokers, at
	// QoS 0 for Sagüin only". Sagüin's database cannot settle these two:
	// absent, the publisher's records around them at offsets 5 and 40,
	// either side of the floor at 20; Mosquitto has no database.
	t.Run("cause unknown", func(t *testing.T) {
		for _, c := range []struct {
			target string
			qos    byte
			fails  bool
		}{
			{"saguin", 1, true}, {"mosquitto", 1, true},
			{"saguin", 0, true}, {"mosquitto", 0, false},
		} {
			o, s := smallRun(c.target)
			k := publisherAt(o.run, c.qos)
			o.pubs[k].sent.Store(4)
			receivedBy(s, k, false, 0, 3)
			err := o.account(storedAt(o, map[int][]int64{k: {5, 0, 0, 40}}, 20, 41))
			reported := slices.ContainsFunc(strings.Split(o.String(), "\n"), func(line string) bool {
				return strings.Contains(line, "gap, cause unknown") && strings.Contains(line, "2")
			})
			if o.gap[c.qos] != 2 || (err != nil) != c.fails || !reported {
				t.Errorf("%s, two QoS %d gaps of unknown cause: counted %d, the run's error %v, reported as \"gap, cause unknown\" %v; want them counted and reported, failing the run %v:\n%s",
					c.target, c.qos, o.gap[c.qos], err, reported, c.fails, o.String())
			}
		}
	})
}

// TestCompressedPayloadAccounted checks the database evidence against a
// publication whose payload the broker stored compressed: 0008c's
// -payload mix run at medium failed reading the records ("offset 63323: a
// payload of 39 bytes, sent with 4186885"), the stored bytes read as if
// they were the payload. design.md, Store, Compression: "A payload over
// 4 KiB, SQLite's page size, is compressed with S2 and stored compressed
// only where that makes it shorter"; storage.md, A stored payload: "Every
// table holding a payload has payload_size, an INTEGER: the payload's size
// as received. A stored payload shorter than its payload_size is
// compressed; one of equal length is not."
//
// One publisher of a run sends two QoS 1 publications to the broker every
// run launches (startTarget), each stamped as the tool stamps them: 8 KiB,
// its stamp then zeros, which the broker stores compressed (the test
// confirms it in the database, or it would not test the case), then the
// recipe's 256 bytes, stored as received. The run's measured window opens
// as the first is sent and lasts the recipe's measured time, so both
// records' intake times fall in it (design.md, PUBLISH: "The record's
// timestamp is the core's intake time"). Once the broker has stopped,
// readStore reads both as it reads any record, by the stamp of the payload
// as received: it reports nothing malformed, finds each at its offset and
// counts both among the window's records at QoS 1, none at QoS 0; and the
// account reports neither unstored, and the window's records as stored.
// The interface is unchanged: readStore and account. Reading the stored
// bytes as the payload fails readStore on the first, its length not the
// one its stamp was sent with; a record read but not counted in the window
// leaves its measured count short.
func TestCompressedPayloadAccounted(t *testing.T) {
	tg := running(t)
	o, _ := smallRun("saguin")
	o.subs = nil // the evidence of the publications alone
	k := publisherAt(o.run, 1)
	p, err := connectPublisher(tg.addr, k, 1)
	if err != nil {
		t.Fatal(err)
	}
	o.pubs[k] = p
	// The window [wallEpoch+warmUp, wallEpoch+warmUp+measured) opens now,
	// before either publication is sent, in the records' milliseconds.
	o.wallEpoch = time.Now().Add(-warmUp)
	lengths := []int{8 << 10, payloadSize}
	for seq, size := range lengths {
		payload := make([]byte, size)
		stamp{publisher: k, seq: uint64(seq)}.put(payload)
		ack, err := p.c.Publish(&harness.Publish{QoS: 1, Topic: o.run.topics[k], Payload: payload})
		switch {
		case err != nil:
			t.Fatalf("publication %d, of %d bytes: %v", seq, size, err)
		case ack.Reason != 0:
			t.Fatalf("publication %d, of %d bytes, got PUBACK 0x%02X; want 0x00, stored", seq, size, ack.Reason)
		}
		p.sent.Add(1)
	}
	_ = p.c.Send(&harness.Disconnect{}) // its end is the broker's stop's business
	_ = p.c.Close()
	if err := tg.stop(); err != nil {
		t.Fatal(err)
	}
	storedCompressed(t, tg.broker.DB, lengths)
	st, err := readStore(tg.broker.DB, o)
	if err != nil {
		t.Fatalf("the run's database evidence, one payload stored compressed: %v", err)
	}
	for seq, offset := range st.offsets[k] {
		if offset == 0 {
			t.Errorf("publication %d, of %d bytes, acknowledged, has no record in the evidence; want its offset", seq, lengths[seq])
		}
	}
	window := [2]int64{0, int64(len(lengths))} // by QoS: both at QoS 1
	if st.inWindow != window {
		t.Errorf("the evidence counts %v records in the measured window, by QoS; want %v, both publications", st.inWindow, window)
	}
	if err := o.account(st); err != nil {
		t.Errorf("the account of two stored publications: %v", err)
	}
	if o.unstoredTail != ([2]int64{}) || o.unstoredGap != ([2]int64{}) || o.ackedUnstored != 0 {
		t.Errorf("unstored tail %v, gap %v, acknowledged unstored %d; want none, both stored", o.unstoredTail, o.unstoredGap, o.ackedUnstored)
	}
	if o.recordsIn != window {
		t.Errorf("the account reports %v records stored in the measured window, by QoS; want %v", o.recordsIn, window)
	}
}

// storedCompressed fails t unless the database at path holds, on
// $broadcast in offset order, one record per payload size of lengths, the
// first stored compressed and the others as received (storage.md, A
// stored payload): the case TestCompressedPayloadAccounted tests.
func storedCompressed(t *testing.T, path string, lengths []int) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }() // read-only
	rows, err := db.Query(`SELECT length(payload), payload_size FROM records
		WHERE channel = (SELECT id FROM channels WHERE name = '$broadcast') ORDER BY "offset"`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }() // read whole below
	var got [][2]int
	for rows.Next() {
		var length, size int
		if err := rows.Scan(&length, &size); err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]int{length, size})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	ok := len(got) == len(lengths)
	for i := 0; ok && i < len(got); i++ {
		ok = got[i][1] == lengths[i] && ((i == 0 && got[i][0] < lengths[i]) || (i > 0 && got[i][0] == lengths[i]))
	}
	if !ok {
		t.Fatalf("the broker stored (length, payload_size) %v; want payload lengths %v, the first stored compressed (shorter) and the rest as received", got, lengths)
	}
}
