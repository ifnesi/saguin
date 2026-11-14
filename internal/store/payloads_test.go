package store_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The store readers' payloads (design.md, Store; Compression; Testing, the
// busy path). These tests use the store's own record write and its three
// readers of a stored payload:
//
//	func (t *Tx) WriteRecords(records iter.Seq2[*Record, bool], queue bool) (int, error)
//	type Record struct{ Channel, Offset int64; ID string; Time int64; Topic string; QoS byte;
//		Publisher string; Payload, StoredPayload []byte; PayloadSize int; PublishedRetained, Retained bool; ... }
//	type ReadRecordPayload struct{ Channel, Offset int64; Into *[]byte; Missing bool }
//	func (b *ReadRecordPayloads) Add(r *ReadRecordPayload) bool // a page's rows, several recipients per row
//	type ReadCurrentValue struct{ Channel int64; Topic string; Budget int64; Into *[]byte;
//		Publication *packet.Publish; Version packet.Version; ... } // kv/get's read of a current value
//	type SaveWill struct{ Session int64; Will *Will }
//	type Will struct{ Client, Topic string; Payload, StoredPayload []byte; PayloadSize int; ... }
//	type LoadWillInfo struct{ Session int64; Will WillInfo }
//	type LoadWill struct{ Session int64; Expected WillInfo; Will Will; Found bool }
//
// A payload is stored as the connection's reader hands it on: its S2 copy
// and its size as received where that is shorter, else as received
// (Compression; storage.md, A stored payload). A reader hands back the
// payload as received: in a page read, in each recipient's Into; in a
// current value's read, in Into; in a Will's load, in Will.Payload, the S2
// copy it read in Will.StoredPayload.

// payloadsAlone names the environment variable set in the run of the test
// binary TestStoredPayloadReadsMakeOneBody makes for itself, alone; and
// payloadsAloneLimit bounds that run.
const (
	payloadsAlone      = "SAGUIN_STORE_PAYLOADS_MEASURED_ALONE"
	payloadsAloneLimit = 2 * time.Minute
)

// readSlack is what a store read may allocate for its own work, its
// statements, rows and answers, beside the payloads it hands back: far
// below the smallest payload the tests read, 64 KiB, and below the S2 copy
// of one. pageRound is what Go adds to each allocation of a payload or of
// an S2 copy, all of them over 32 KiB: it rounds a large allocation up to
// whole 8 KiB pages.
const readSlack, pageRound = 16 << 10, 8 << 10

// payloadRows is a test write storing records through the store's own
// record write, in one transaction.
type payloadRows struct{ records []store.Record }

func (p *payloadRows) Records() int { return len(p.records) }

func (p *payloadRows) WriteTx(tx *store.Tx) error {
	_, err := tx.WriteRecords(func(yield func(*store.Record, bool) bool) {
		for i := range p.records {
			if !yield(&p.records[i], i == len(p.records)-1) {
				return
			}
		}
	}, false)
	return err
}

// payloadOf is n bytes of seed's own, no two seeds alike: noise, which no
// compression makes shorter (design.md, Compression: "stored compressed
// only where that makes it shorter"), or, compressible, half noise and half
// zeros, which S2 makes about half as long.
func payloadOf(seed uint64, n int, compressible bool) []byte {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	b := make([]byte, n)
	noise := b
	if compressible {
		noise = b[:n/2]
	}
	_, _ = rand.NewChaCha8(key).Read(noise)
	return b
}

// recordOf is a record of channel at offset on topic, its payload body
// stored as its reader hands it on: the S2 copy where shorter
// (design.md, Compression), retained where it is a latest channel's current
// value.
func recordOf(channel, offset int64, topic string, body []byte, retained bool) store.Record {
	r := store.Record{Channel: channel, Offset: offset, ID: strconv.FormatInt(offset, 10), Time: 1760000000000,
		Topic: topic, QoS: 1, Publisher: "c1", Retained: retained}
	if encoded := payload.Compress(body); encoded != nil {
		r.StoredPayload, r.PayloadSize = payload.Compact(encoded), len(body)
	} else {
		r.Payload = body
	}
	return r
}

// willOf is a Will on topic will of payload body, stored as its reader
// hands it on, as recordOf stores a record's.
func willOf(body []byte) *store.Will {
	w := &store.Will{Client: "c1", Topic: "will"}
	if encoded := payload.Compress(body); encoded != nil {
		w.StoredPayload, w.PayloadSize = payload.Compact(encoded), len(body)
	} else {
		w.Payload = body
	}
	return w
}

// answered takes op's answers, its view's confirmation where it has one
// and its result, and returns the result's error. A read of channel
// records has its result alone (design.md, Store reads: it "is answered
// once").
func answered(t *testing.T, r *running, op store.Read) error {
	t.Helper()
	for {
		a := testkit.Recv(t, r.Answers(), backstop, "a read's answer")
		if a.Op != op {
			t.Fatalf("the store answered %T %p; want %T %p, the one read outstanding", a.Op, a.Op, op, op)
		}
		if !a.View || a.Err != nil {
			return a.Err
		}
	}
}

// allocatedBy has a reader carry out op and returns the bytes the whole
// process allocated from its send to its result, and the result's error.
func allocatedBy(t *testing.T, r *running, op store.Read) (uint64, error) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	testkit.Send(t, r.Reads(), op, backstop)
	err := answered(t, r, op)
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, err
}

// design.md, Testing: the busy path "makes no second pass over the same
// data and avoids every copy or allocation it can"; Store: "A store reader
// hands a stored payload on as the driver gives it, without copying it
// again. The driver gives each row's payload a copy of its own"; Store,
// Compression: "A store reader decompresses it once per read, whatever the
// number of recipients".
//
// For payloads of 64 KiB and 1 MiB, stored as received (noise) and, the
// control, stored compressed (half noise), the store holds three records
// of channel 2, a current value of channel 3 and a Will, each payload its
// own. Each of the three readers carries out its read three times, a page
// read of the three rows once with one recipient per row and once with
// four. The least a read allocated, over the whole process while it ran,
// is at most the payloads it hands back, one per distinct row whatever the
// recipients, and the S2 copies it reads, with readSlack beside and
// pageRound for each of them: a payload stored as received is the driver's
// copy alone, handed on; one stored compressed is the driver's copy of its
// S2 copy and its body, decompressed once. It is at least the payloads
// handed back, the measure seeing the reader's work. Each recipient is
// handed its row's payload.
func TestStoredPayloadReadsMakeOneBody(t *testing.T) {
	// What a read allocates is measured over the whole process, so the
	// test runs alone in a run of the test binary of its own, where no
	// other test allocates meanwhile.
	if os.Getenv(payloadsAlone) == "" {
		testkit.Rerun(t, "the run measuring the store's payload reads alone", payloadsAloneLimit, []string{payloadsAlone + "=1"})
		return
	}
	const rows, runs = 3, 3
	for _, size := range []int{64 << 10, 1 << 20} {
		for _, compressible := range []bool{false, true} {
			form := "stored as received"
			if compressible {
				form = "stored compressed"
			}
			t.Run(fmt.Sprintf("%d bytes %s", size, form), func(t *testing.T) {
				r := start(t, newConfig(t, 2))
				bodies := make([][]byte, rows)
				stored := &payloadRows{}
				var copies int // the S2 copies of the page's rows, in all
				for i := range bodies {
					bodies[i] = payloadOf(uint64(i+1), size, compressible)
					stored.records = append(stored.records, recordOf(2, int64(i+1), "orders/a", bodies[i], false))
					copies += len(stored.records[i].StoredPayload)
				}
				value := payloadOf(rows+1, size, compressible)
				stored.records = append(stored.records, recordOf(3, 1, "values/a", value, true))
				valueCopy := len(stored.records[rows].StoredPayload)
				body := payloadOf(rows+2, size, compressible)
				will := willOf(body)
				if (copies != 0) != compressible || (valueCopy != 0) != compressible || (will.StoredPayload != nil) != compressible {
					t.Fatalf("rig: the payloads are not all %s", form)
				}
				if err := write(t, r, stored); err != nil {
					t.Fatalf("storing the records: %v", err)
				}
				if err := write(t, r, &store.SaveWill{Session: 1, Will: will}); err != nil {
					t.Fatalf("storing the Will: %v", err)
				}

				// pieces is how many payloads and S2 copies a read of n
				// distinct payloads allocates: one each, and as many copies
				// where they are stored compressed.
				pieces := func(n int) int {
					if compressible {
						return 2 * n
					}
					return n
				}
				// within checks least, the least a read allocated, against
				// the payloads it handed back, handed bytes, and the S2
				// copies it read, copied bytes, n distinct payloads in all.
				within := func(what string, least uint64, handed, copied, n int) {
					t.Helper()
					t.Logf("%s: allocated %d bytes, handing back %d from S2 copies of %d", what, least, handed, copied)
					if least > uint64(handed+copied+readSlack+pieces(n)*pageRound) {
						t.Errorf("%s allocated %d bytes, handing back payloads of %d in all from S2 copies of %d: a payload copied again, or decompressed twice",
							what, least, handed, copied)
					}
					if least < uint64(handed) {
						t.Fatalf("rig: %s allocated %d bytes, handing back payloads of %d: the measure missed the reader's work", what, least, handed)
					}
				}

				for _, recipients := range []int{1, 4} {
					least := uint64(math.MaxUint64)
					for range runs {
						op := &store.ReadRecordPayloads{}
						into := make([][]byte, rows*recipients)
						for i := range into {
							if !op.Add(&store.ReadRecordPayload{Channel: 2, Offset: int64(i/recipients + 1), Into: &into[i]}) {
								t.Fatalf("rig: a page read of %d rows took no recipient %d", rows, i)
							}
						}
						n, err := allocatedBy(t, r, op)
						if err != nil {
							t.Fatalf("a page read: %v", err)
						}
						for i, got := range into {
							if !same(got, bodies[i/recipients]) {
								t.Fatalf("recipient %d of offset %d was handed %d bytes; want its record's payload, %d bytes", i%recipients, i/recipients+1, len(got), size)
							}
						}
						least = min(least, n)
					}
					within(fmt.Sprintf("a page read of %d rows, %d recipients each", rows, recipients), least, rows*size, copies, rows)
				}

				least := uint64(math.MaxUint64)
				for range runs {
					op, into := currentValue("values/a")
					n, err := allocatedBy(t, r, op)
					if err != nil {
						t.Fatalf("the current value's read: %v", err)
					}
					if op.Retry || !same(*into, value) {
						t.Fatalf("the current value's read handed back %d bytes, retry %v; want the value, %d bytes", len(*into), op.Retry, size)
					}
					least = min(least, n)
				}
				within("the current value's read", least, size, valueCopy, 1)

				least = uint64(math.MaxUint64)
				for range runs {
					info := &store.LoadWillInfo{Session: 1}
					if err := read(t, r, info); err != nil {
						t.Fatalf("the Will's metadata: %v", err)
					}
					op := &store.LoadWill{Session: 1, Expected: info.Will}
					n, err := allocatedBy(t, r, op)
					if err != nil {
						t.Fatalf("the Will's load: %v", err)
					}
					if !op.Found || !same(op.Will.Payload, body) {
						t.Fatalf("the Will's load found %v, handing back %d bytes; want the Will's payload, %d bytes", op.Found, len(op.Will.Payload), size)
					}
					least = min(least, n)
				}
				within("the Will's load", least, size, len(will.StoredPayload), 1)
			})
		}
	}
}

// same reports whether got holds want, byte for byte. It reads each byte in
// Go, as the race detector sees it, rather than through bytes.Equal, whose
// assembly the race detector does not see.
func same(got, want []byte) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// handed is a payload a read handed back, got, kept as handed and never
// copied, and want, what was stored; what names it.
type handed struct {
	what      string
	got, want []byte
}

// intact checks, when, that every payload of hs holds what was stored,
// byte for byte, read as the race detector sees it.
func intact(t *testing.T, when string, hs []handed) {
	t.Helper()
	changed := 0
	for _, h := range hs {
		if !same(h.got, h.want) {
			if changed == 0 {
				t.Errorf("%s: %s, handed back as stored, now differs from it", when, h.what)
			}
			changed++
		}
	}
	if changed > 1 {
		t.Errorf("%s: %d of %d payloads handed back differ from what was stored", when, changed, len(hs))
	}
}

// meanwhile sends ops to the readers at once and checks hs, when, while
// they carry them out, until their answers are waiting; it then takes
// those answers, each read's result and its view's confirmation where it
// has one, and returns their errors by op.
func meanwhile(t *testing.T, r *running, when string, hs []handed, ops ...store.Read) map[any]error {
	t.Helper()
	for _, op := range ops {
		testkit.Send(t, r.Reads(), op, backstop)
	}
	for pass := 0; pass < 100 && len(r.Answers()) < len(ops); pass++ {
		intact(t, when, hs)
	}
	return collect(t, r, len(ops))
}

// design.md, Store: "A store reader hands a stored payload on as the driver
// gives it, without copying it again. The driver gives each row's payload
// a copy of its own, which nothing reuses once the read moves on or ends.
// This holds for the pinned driver, not by database/sql's contract, and a
// test fails if a driver version reuses that copy"; Execution,
// Communication: "Once a buffer is sent, its sender never touches it
// again".
//
// The store holds six sets of payloads of 64 KiB, no two alike: each set
// sixteen records of channel 2, a current value of channel 3 and a Will,
// all stored as received, and a Will stored compressed. The reads of a set
// are a page read of its records, two recipients per row, the current
// value's read and the two Wills' loads; what they hand back is kept as
// handed, never copied: each recipient's Into, the current value's Into,
// and each Will's Payload and, compressed, the StoredPayload it read. With
// one reader held, the other, on its one connection, carries out the
// first set's reads one after another: each hands back its set's payloads,
// the page's rows intact after its later rows were read and the read
// ended. It then carries out the second set's reads, sent at once: while
// it does, and once it has, every payload the first set's reads handed
// back is as stored, byte for byte. The hold released, both readers carry
// out the four remaining sets' reads, sent at once: while they do, and
// once they have, every payload handed back is still as stored.
func TestHandedPayloadsStayIntact(t *testing.T) {
	const size, rows, recipients, sets = 64 << 10, 16, 2, 6
	r := start(t, newConfig(t, 2))
	bodies := make([][][]byte, sets)  // each set's records' payloads, by row
	values := make([][]byte, sets)    // each set's current value
	wills := make([][2][]byte, sets)  // each set's Wills' payloads, as received and compressed
	copies := make([][2][]byte, sets) // their S2 copies, nil for the one stored as received
	records, saves := &payloadRows{}, &core.Group{}
	for k := range sets {
		seed := uint64(k) << 8
		for i := range rows {
			body := payloadOf(seed+uint64(i), size, false)
			bodies[k] = append(bodies[k], body)
			records.records = append(records.records, recordOf(2, int64(k*rows+i+1), "orders/a", body, false))
		}
		values[k] = payloadOf(seed+rows, size, false)
		records.records = append(records.records, recordOf(3, int64(k+1), "values/"+strconv.Itoa(k), values[k], true))
		for j, compressible := range []bool{false, true} {
			wills[k][j] = payloadOf(seed+rows+1+uint64(j), size, compressible)
			w := willOf(wills[k][j])
			if copies[k][j] = w.StoredPayload; (w.StoredPayload != nil) != compressible {
				t.Fatalf("rig: set %d's Will %d is not stored as the test needs", k, j)
			}
			saves.Writes = append(saves.Writes, &store.SaveWill{Session: int64(2*k + j + 1), Will: w})
		}
	}
	for _, w := range []store.Write{records, saves} {
		if err := write(t, r, w); err != nil {
			t.Fatalf("storing the payloads: %v", err)
		}
	}
	infos := make([]store.WillInfo, 2*sets)
	for i := range infos {
		info := &store.LoadWillInfo{Session: int64(i + 1)}
		if err := read(t, r, info); err != nil {
			t.Fatalf("Will %d's metadata: %v", i+1, err)
		}
		infos[i] = info.Will
	}

	// readsOf returns set k's reads, and a function returning, once they
	// are answered, what they handed back, each with what was stored.
	readsOf := func(k int) ([]store.Read, func() []handed) {
		page := &store.ReadRecordPayloads{}
		into := make([][]byte, rows*recipients)
		for i := range into {
			if !page.Add(&store.ReadRecordPayload{Channel: 2, Offset: int64(k*rows + i/recipients + 1), Into: &into[i]}) {
				t.Fatalf("rig: a page read of %d rows took no recipient %d", rows, i)
			}
		}
		current, value := currentValue("values/" + strconv.Itoa(k))
		loads := [2]*store.LoadWill{{Session: int64(2*k + 1), Expected: infos[2*k]}, {Session: int64(2*k + 2), Expected: infos[2*k+1]}}
		return []store.Read{page, current, loads[0], loads[1]}, func() []handed {
			t.Helper()
			var hs []handed
			for i, got := range into {
				hs = append(hs, handed{fmt.Sprintf("set %d's record at offset %d, recipient %d", k, k*rows+i/recipients+1, i%recipients), got, bodies[k][i/recipients]})
			}
			if current.Retry {
				t.Fatalf("set %d's current value's read asked to be retried, its budget unbounded", k)
			}
			hs = append(hs, handed{fmt.Sprintf("set %d's current value", k), *value, values[k]})
			for j, load := range loads {
				if !load.Found {
					t.Fatalf("set %d's Will %d was not found", k, j)
				}
				hs = append(hs, handed{fmt.Sprintf("set %d's Will %d's payload", k, j), load.Will.Payload, wills[k][j]})
				if copies[k][j] != nil {
					hs = append(hs, handed{fmt.Sprintf("set %d's Will %d's S2 copy", k, j), load.Will.StoredPayload, copies[k][j]})
				}
			}
			return hs
		}
	}
	// answers checks that every one of ops was answered without error.
	answers := func(errs map[any]error, ops []store.Read) {
		t.Helper()
		for _, op := range ops {
			if err := errs[op]; err != nil {
				t.Fatalf("%T: %v", op, err)
			}
		}
	}

	held := holdReaders(t, r, 1)[0]
	first, firstHanded := readsOf(0)
	alone := &connOf{}
	if err := read(t, r, alone); err != nil {
		t.Fatalf("the free reader's connection: %v", err)
	}
	for _, op := range first {
		testkit.Send(t, r.Reads(), op, backstop)
		if err := answered(t, r, op); err != nil {
			t.Fatalf("set 0's %T: %v", op, err)
		}
	}
	kept := firstHanded()
	intact(t, "set 0's reads answered one after another", kept)

	second, secondHanded := readsOf(1)
	again := &connOf{}
	answers(meanwhile(t, r, "while the same reader carries out set 1's reads", kept, append(second, again)...), second)
	if alone.conn == held.conn || again.conn != alone.conn {
		t.Fatalf("rig: one reader held, sets 0 and 1 were read on more than one connection")
	}
	intact(t, "set 1's reads answered on the same connection", kept)
	kept = append(kept, secondHanded()...)

	held.free()
	if err := answerTo(t, r, held); err != nil {
		t.Fatalf("the held reader: %v", err)
	}
	var rest []store.Read
	var restHanded []func() []handed
	for k := 2; k < sets; k++ {
		ops, h := readsOf(k)
		rest, restHanded = append(rest, ops...), append(restHanded, h)
	}
	answers(meanwhile(t, r, "while both readers carry out the remaining sets' reads", kept, rest...), rest)
	for _, h := range restHanded {
		kept = append(kept, h()...)
	}
	intact(t, "every set's reads answered", kept)
	stop(t, r)
	intact(t, "the store stopped", kept)
}

// design.md, R8: "A store error stops the broker"; Store: "A broken
// connection is a store error". A stored read whose driver fails reports
// that failure as its error, and never a success, wherever in the read it
// comes: as a statement starts, at a call of Next, in a row's values (its
// Scan), or as its rows close, after a row, after the last or after none;
// and a payload whose stored form is damaged (storage.md, A stored
// payload) is its error too.
//
// The store holds, for each of the three readers of a stored payload, a
// row it reads, one it does not find, and a damaged one: a record of
// channel 2, a current value of channel 3 and a Will, each of 64 KiB
// stored as received, and each of a payload of 1 byte stored with
// payload_size 2. Each read is carried out on a connection whose driver
// fails at one of those places in one of the statements the read runs, for
// every statement it runs (store.Faulted): each failure given is the
// read's error. Without a failure, a found row is read whole, a missing
// one is no error, and a damaged one is an error; a failure on a row read
// without damage is reported as itself (a damaged row may report its
// damage instead).
func TestStoredReadFailuresAreErrors(t *testing.T) {
	const size = 64 << 10
	r := start(t, newConfig(t, 2))
	body, value, will := payloadOf(1, size, false), payloadOf(2, size, false), payloadOf(3, size, false)
	damaged := func(channel, offset int64, topic string, retained bool) store.Record {
		rec := recordOf(channel, offset, topic, nil, retained)
		rec.StoredPayload, rec.PayloadSize = []byte("x"), 2
		return rec
	}
	records := &payloadRows{records: []store.Record{
		recordOf(2, 1, "orders/a", body, false), damaged(2, 2, "orders/a", false),
		recordOf(3, 1, "values/a", value, true), damaged(3, 2, "values/damaged", true),
	}}
	wills := &core.Group{Writes: []store.Write{
		&store.SaveWill{Session: 1, Will: willOf(will)},
		&store.SaveWill{Session: 2, Will: &store.Will{Client: "c1", Topic: "will", StoredPayload: []byte("x"), PayloadSize: 2}},
	}}
	for _, w := range []store.Write{records, wills} {
		if err := write(t, r, w); err != nil {
			t.Fatalf("storing the payloads: %v", err)
		}
	}
	infos := make([]store.WillInfo, 2)
	for i := range infos {
		info := &store.LoadWillInfo{Session: int64(i + 1)}
		if err := read(t, r, info); err != nil {
			t.Fatalf("Will %d's metadata: %v", i+1, err)
		}
		infos[i] = info.Will
	}
	stop(t, r)
	path := r.cfg.Broker.Storage.FilePath

	// found, missing, damaged: what a read's row is.
	const (
		found = iota
		missing
		damage
	)
	for _, tc := range []struct {
		name string
		row  int
		// read returns a new read, and a check of what it handed back
		// once it succeeded.
		read func() (store.Read, func() bool)
	}{
		{"a page read of a stored record", found, func() (store.Read, func() bool) {
			var into []byte
			op := &store.ReadRecordPayloads{}
			op.Add(&store.ReadRecordPayload{Channel: 2, Offset: 1, Into: &into})
			return op, func() bool { return same(into, body) }
		}},
		{"a page read of a record not stored", missing, func() (store.Read, func() bool) {
			var into []byte
			op := &store.ReadRecordPayloads{}
			p := &store.ReadRecordPayload{Channel: 2, Offset: 9, Into: &into}
			op.Add(p)
			return op, func() bool { return p.Missing && into == nil }
		}},
		{"a page read of a damaged record", damage, func() (store.Read, func() bool) {
			var into []byte
			op := &store.ReadRecordPayloads{}
			op.Add(&store.ReadRecordPayload{Channel: 2, Offset: 2, Into: &into})
			return op, nil
		}},
		{"the read of a current value", found, func() (store.Read, func() bool) {
			op, into := currentValue("values/a")
			return op, func() bool { return !op.Retry && same(*into, value) }
		}},
		{"the read of no current value", missing, func() (store.Read, func() bool) {
			op, into := currentValue("values/none")
			return op, func() bool { return !op.Retry && *into == nil }
		}},
		{"the read of a damaged current value", damage, func() (store.Read, func() bool) {
			op, _ := currentValue("values/damaged")
			return op, nil
		}},
		{"the load of a Will", found, func() (store.Read, func() bool) {
			op := &store.LoadWill{Session: 1, Expected: infos[0]}
			return op, func() bool { return op.Found && same(op.Will.Payload, will) }
		}},
		{"the load of no Will", missing, func() (store.Read, func() bool) {
			op := &store.LoadWill{Session: 9}
			return op, func() bool { return !op.Found }
		}},
		{"the load of a damaged Will", damage, func() (store.Read, func() bool) {
			op := &store.LoadWill{Session: 2, Expected: infos[1]}
			return op, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op, ok := tc.read()
			queries, _, err := store.Faulted(t, path, op, 0, store.NoFault)
			switch {
			case tc.row == damage && err == nil:
				t.Errorf("without a failure, %s succeeded; want an error for its damaged payload", tc.name)
			case tc.row != damage && err != nil:
				t.Fatalf("without a failure, %s: %v", tc.name, err)
			case tc.row != damage && !ok():
				t.Fatalf("without a failure, %s handed back the wrong result", tc.name)
			}
			for query := range queries {
				for _, f := range []struct {
					name  string
					fault store.ReadFault
				}{
					{"as the statement starts", store.FaultQuery}, {"at Next", store.FaultNext},
					{"in a row's values", store.FaultScan}, {"as its rows close", store.FaultClose},
				} {
					// The read cannot avoid a failure as a statement it
					// runs starts or at Next, nor in a row it gets.
					must := f.fault == store.FaultQuery || f.fault == store.FaultNext || (f.fault == store.FaultScan && tc.row != missing)
					op, _ := tc.read()
					_, fired, err := store.Faulted(t, path, op, query, f.fault)
					switch {
					case !fired && must:
						t.Fatalf("rig: %s, failing %s in statement %d of %d, never met the failure", tc.name, f.name, query+1, queries)
					case fired && err == nil:
						t.Errorf("%s, its driver failing %s in statement %d of %d, succeeded; want the failure as its error", tc.name, f.name, query+1, queries)
					case fired && f.fault != store.FaultScan && tc.row != damage && !errors.Is(err, store.ErrInjected):
						t.Errorf("%s, its driver failing %s in statement %d of %d, reported %v; want the failure itself", tc.name, f.name, query+1, queries, err)
					}
				}
			}
		})
	}
}

// currentValue is a kv/get's read of topic's current value on channel 3,
// its budget unbounded, and where it hands the value back.
func currentValue(topic string) (op *store.ReadCurrentValue, into *[]byte) {
	into = new([]byte)
	op = &store.ReadCurrentValue{Channel: 3, Topic: topic, Into: into, Budget: math.MaxInt32,
		Publication: &packet.Publish{Topic: []byte("answers/kv")}, Version: packet.V5}
	return op, into
}
