package store_test

import (
	"fmt"
	"testing"

	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The test of a retention write's reads of away sessions, run inside: how
// often a write reads the sessions it counts losses for is work no client
// or operator can see (design.md, Testing: "A test inside the code checks
// only what neither can see. These checks cover [...] the busy path"). It
// builds the store's state as storage.md's tables give it and runs the
// write as the store writer does (store.Counted), counting the statements
// it runs and the rows they return.

// The state the test builds: an append channel, aged, keeping records for
// agedPeriodMs, holding agedRecords records of testkit.RecordBytes, and
// awaySessions durable sessions, away, each subscribed to aged/# at QoS 1
// with its position at the first record: more than two pages of 256
// (design.md, Capacities: "Due-work and metadata pages hold 256 rows").
const (
	agedChannel  = 2
	agedPeriodMs = 10_000
	agedRecords  = 600
	awaySessions = 600
	agedAt       = 1_760_000_000_000 // the write's time, Unix milliseconds
)

// awayState builds that state in a new store file and returns its path:
// the first due records are past their retention_period at agedAt, the
// rest not yet.
func awayState(t *testing.T, due int) string {
	t.Helper()
	cfg := newConfig(t, 1)
	stop(t, start(t, cfg)) // the schema, as the store creates it
	path := cfg.Broker.Storage.FilePath
	tx, err := openFile(t, path).Begin()
	if err != nil {
		t.Fatal(err)
	}
	insert := func(table string, change testkit.Row) {
		if err := testkit.InsertRow(tx, table, testkit.With(testkit.TableNamed(t, table).Valid[0], change)); err != nil {
			t.Fatalf("a row of %s: %v", table, err)
		}
	}
	insert("channels", testkit.Row{"id": agedChannel, "name": "aged", "type": 1, "next": agedRecords + 1, "floor": 1,
		"bytes": agedRecords * testkit.RecordBytes})
	for i := 1; i <= agedRecords; i++ {
		at := int64(agedAt)
		if i <= due {
			at -= 2 * agedPeriodMs
		}
		topic := fmt.Sprintf("aged/%d", i)
		payload := testkit.Filling(topic, testkit.RecordBytes)
		insert("records", testkit.Row{"channel": agedChannel, "offset": i, "id": fmt.Sprintf("record-%d", i), "time": at,
			"topic": topic, "publisher": "publisher", "payload": payload, "payload_size": len(payload)})
	}
	for i := 1; i <= awaySessions; i++ {
		insert("sessions", testkit.Row{"id": i, "client": fmt.Sprintf("away-%d", i), "expiry": 3600, "left": agedAt - agedPeriodMs})
		insert("subscriptions", testkit.Row{"session": i, "filter": "aged/#", "qos": 1})
		insert("positions", testkit.Row{"session": i, "channel": agedChannel, "offset": 1})
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return path
}

// design.md, Words: Missed, "Retention determines losses before removing
// the information needed to count them [...] Away sessions use their
// stored state"; "Processing uses bounded pages"; "A retention write
// deletes at most 512 records"; Words: Retention, "A pass reads the times
// of up to 512 records in one ordered read, deletes the run of them past
// retention in one range"; Testing: "The busy path [...] does no needless
// work. It makes no second pass over the same data". metrics.md,
// saguin_missed_total: "one record owed to two consumers contributes two".
//
// Over the same 600 records and 600 away sessions, two retention writes
// for aged, each on a state of its own: one with the first record due, one
// with the first 512. The 512-record write deletes 512 records and counts
// 512 × 600 missed, exactly; it reads the away sessions' pages once, as
// the one-record write does: it runs no more statements, and its
// statements return no more rows, than the one-record write's.
func TestRetentionReadsAwaySessionsOncePerWrite(t *testing.T) {
	type pass struct{ runs, rows int }
	passes := map[int]pass{}
	for _, due := range []int{1, 512} {
		path := awayState(t, due)
		op := &store.Retention{Channel: agedChannel, At: agedAt, LogPeriod: agedPeriodMs, Floor: 1, Tail: agedRecords + 1,
			Size: agedRecords * testkit.RecordBytes, Expire: true}
		runs, rows := store.Counted(t, path, op)
		if op.Removed != due || op.Missed != uint64(due*awaySessions) {
			t.Fatalf("with %d records due and %d away sessions owed each, the write removed %d and counted %d missed; want %d and %d",
				due, awaySessions, op.Removed, op.Missed, due, due*awaySessions)
		}
		passes[due] = pass{runs, rows}
	}
	if one, many := passes[1], passes[512]; many.runs > one.runs || many.rows > one.rows {
		t.Errorf("a retention write deleting 512 records ran %d statements returning %d rows; one deleting 1 ran %d returning %d: "+
			"want no more, the %d away sessions' pages read once a write, not once a record", many.runs, many.rows, one.runs, one.rows, awaySessions)
	}
}
