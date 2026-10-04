package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// These measure the store on its own, with no MQTT and no broker lock, so
// what they report is a ceiling rather than a throughput saguin delivers.
// The broker adds a packet parse, a broker-wide mutex and a write to a
// socket on top of every one of these.
//
// Run with:
//
//	TMPDIR=/somewhere/on/real/storage \
//	  go test ./internal/store/sqlite/ -bench . -benchtime 2s -run '^$'
//
// **`TMPDIR` is not optional for any figure quoted as a disk's.**
// benchDB puts the database under b.TempDir(), which follows `TMPDIR`, and
// on a machine whose /tmp is a tmpfs that is memory. BenchmarkAppend's
// sqlite row reads 22.8-23.4us there against 34.7-35.4us on ext4 over NVMe
// - a third faster, silently, and it is the same code either way. A figure
// taken without checking which of the two it measured is a figure about
// nothing in particular.
//
// Nothing here is a claim. The number that matters to an operator is the
// one measured through the wire, which is BenchmarkPublishConcurrently in
// internal/broker, and RFC 0002 carries its table.

func benchPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte('a' + i%26)
	}
	return p
}

func benchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "bench.db"), "bench")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// Appending one record: a memory store against a SQLite one, at two
// payload sizes. Every SQLite append here is its own transaction, which is
// what a provider does unless an operator sets publish_commit_interval (RFC 0002),
// so this is the cost of the default, stated.
func BenchmarkAppend(b *testing.B) {
	for _, size := range []int{128, 1024} {
		payload := benchPayload(size)
		rec := store.Record{MessageID: "m", Topic: "events/x", Payload: payload}

		b.Run(fmt.Sprintf("memory/%dB", size), func(b *testing.B) {
			lg := store.NewLog()
			b.ResetTimer()
			for range b.N {
				if _, err := lg.Append(rec); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("sqlite/%dB", size), func(b *testing.B) {
			lg, err := benchDB(b).Log("events")
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				if _, err := lg.Append(rec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Appending a record carrying headers, which is the common case for
// anything saguin generated: a dead-lettered record carries seven. They
// become JSON, so this is what that costs.
func BenchmarkAppendWithHeaders(b *testing.B) {
	rec := store.Record{
		MessageID: "m", Topic: "events/x", Payload: benchPayload(128),
		Headers: []store.Header{{Key: "saguin-dlq-channel", Value: "jobs"}, {Key: "saguin-dlq-offset", Value: "41"}, {Key: "saguin-dlq-attempts", Value: "5"}, {Key: "saguin-dlq-reason", Value: "attempts_exhausted"}, {Key: "saguin-dlq-at", Value: "2026-08-10T20:00:00Z"}, {Key: "saguin-dlq-first", Value: "2026-08-10T19:00:00Z"}, {Key: "saguin-dlq-last", Value: "2026-08-10T19:59:00Z"}},
	}
	b.Run("memory", func(b *testing.B) {
		lg := store.NewLog()
		for range b.N {
			if _, err := lg.Append(rec); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		lg, err := benchDB(b).Log("events")
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for range b.N {
			if _, err := lg.Append(rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// What a group commit is worth at the store: the same append, N rows to a
// transaction.
//
// A publish is its own transaction unless the provider collects - Log.Append
// opens one, writes two statements and commits - and at synchronous=NORMAL
// that commit does not sync, so what is being paid on every record is CPU
// inside SQLite rather than a wait on the disk. Batching amortises that
// cost; it does not change what durability the commit gives. This measures
// how much there is to amortise; what an operator gets from collecting is
// the wire figure in internal/broker, which waits for acknowledgements as a
// publisher does and is the smaller number.
//
// Two variants, because they are different amounts of work and the gap
// between them is exactly what a purpose-built batch would have to be
// written to collect:
//
//   - rowPerRecord - N appends through appendTx in one transaction, which
//     is today's code with nothing changed but where the commit falls. Each
//     record still rewrites the channel's next/held row, so the batch pays
//     N of those.
//   - rowPerBatch - the same N inserts with that row written once at the
//     end, which is what a group commit would do. It is spelled out here
//     rather than called, because no store method does this yet and the
//     point of a prototype is to size the design before writing it.
//
// Neither is a proposal, and neither is a rate saguin delivers: there is no
// MQTT, no broker lock and no socket here. A batch that acknowledges N
// publishers together still has to answer a refusal in the middle of one,
// share the provider's single connection with retention and the dead-letter
// move, and widen the window in which a crash leaves records stored and
// unacknowledged. None of that is measured here.
func BenchmarkAppendBatched(b *testing.B) {
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: benchPayload(128)}
	size := store.RecordSize(rec)

	for _, batch := range []int{1, 8, 64, 256, 1024} {
		// ns/op is per record in both variants, so the columns compare
		// directly against BenchmarkAppend/sqlite/128B at batch 1.
		b.Run(fmt.Sprintf("rowPerRecord/%d", batch), func(b *testing.B) {
			lg, err := benchDB(b).Log("events")
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i += batch {
				n := min(batch, b.N-i)
				if err := lg.db.tx(func(tx *sql.Tx) error {
					for range n {
						if _, err := lg.appendTx(tx, rec); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("rowPerBatch/%d", batch), func(b *testing.B) {
			lg, err := benchDB(b).Log("events")
			if err != nil {
				b.Fatal(err)
			}
			// Hoisted out of the loop for the same reason a real batch
			// would hoist them: they are per record, not per transaction,
			// and leaving them inside would measure JSON encoding of the
			// same two nils N times over.
			headers, err := encodeHeaders(rec.Headers)
			if err != nil {
				b.Fatal(err)
			}
			properties, err := encodeProps(rec)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i += batch {
				n := min(batch, b.N-i)
				// next and held are advanced without the lock and before
				// the commit returns, which no store method may do - a
				// failed commit would leave both one batch ahead of the
				// rows. b.Fatal ends the run instead, which is the licence
				// a prototype has and the store does not.
				if err := lg.db.tx(func(tx *sql.Tx) error {
					insert := tx.Stmt(lg.insert)
					for range n {
						if _, err := insert.Exec(
							lg.name, int64(lg.next), rec.MessageID, rec.Topic, rec.Payload,
							headers, encodeTime(rec.Timestamp), properties); err != nil {
							return err
						}
						lg.next++
						lg.held += size
					}
					_, err := tx.Stmt(lg.bump).Exec(int64(lg.next), lg.held, lg.name)
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Replacing a topic's value, at three channel sizes.
//
// This is the shape that matters, not the absolute number. A latest
// channel is a map, so the cost of replacing one value must not depend on
// how many other topics the channel holds. If these three columns are flat
// the table is keyed by what the channel is keyed by; if they climb with
// the topic count, something is finding the row by scanning.
func BenchmarkLatestSet(b *testing.B) {
	payload := benchPayload(128)

	for _, topics := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("memory/%dtopics", topics), func(b *testing.B) {
			lt := store.NewLatest()
			for i := range topics {
				if _, err := lt.Set(store.Record{Topic: fmt.Sprintf("state/d%d", i), Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := range b.N {
				rec := store.Record{Topic: fmt.Sprintf("state/d%d", i%topics), Payload: payload}
				if _, err := lt.Set(rec); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("sqlite/%dtopics", topics), func(b *testing.B) {
			lt, err := benchDB(b).Latest("state")
			if err != nil {
				b.Fatal(err)
			}
			for i := range topics {
				if _, err := lt.Set(store.Record{Topic: fmt.Sprintf("state/d%d", i), Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := range b.N {
				rec := store.Record{Topic: fmt.Sprintf("state/d%d", i%topics), Payload: payload}
				if _, err := lt.Set(rec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Reading a consumer's window: what pump asks for on every PUBACK. The
// bound is the client's Receive Maximum, so ten records is a realistic
// call and the whole channel is not.
func BenchmarkReadWindow(b *testing.B) {
	const backlog = 100_000
	payload := benchPayload(128)
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: payload}

	b.Run("memory", func(b *testing.B) {
		lg := store.NewLog()
		for range backlog {
			if _, err := lg.Append(rec); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := range b.N {
			if _, err := lg.ReadFromN(uint64(i%backlog)+1, 10); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("sqlite", func(b *testing.B) {
		lg, err := benchDB(b).Log("events")
		if err != nil {
			b.Fatal(err)
		}
		for range backlog {
			if _, err := lg.Append(rec); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := range b.N {
			if _, err := lg.ReadFromN(uint64(i%backlog)+1, 10); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Storing a consumer's position: one row per reader and channel, written
// on every PUBACK an append consumer sends. It is the write that rides
// alongside every record a durable consumer reads, so its cost is added to
// the read path rather than to the publish path.
func BenchmarkSavePosition(b *testing.B) {
	p := store.Position{Reader: store.MQTTReader("reader"), Offset: 1}

	b.Run("memory", func(b *testing.B) {
		lg := store.NewLog()
		held := positionsFor(b, lg)
		b.ResetTimer()
		for i := range b.N {
			p.Offset = uint64(i%held) + 1
			if err := lg.SavePosition(p); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		lg, err := benchDB(b).Log("events")
		if err != nil {
			b.Fatal(err)
		}
		held := positionsFor(b, lg)
		b.ResetTimer()
		for i := range b.N {
			p.Offset = uint64(i%held) + 1
			if err := lg.SavePosition(p); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// positionsFor appends the records a benchmark's positions point into, since
// a position past the log's next offset is refused (store.ErrPastNext), and
// answers how many, so that offsets 1 to that many are all positions the log
// takes. Each write still moves the position, which is what is measured.
func positionsFor(b *testing.B, lg interface {
	Append(store.Record) (store.Record, error)
}) int {
	b.Helper()
	const held = 16
	for i := range held {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/a", Payload: []byte("x")}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
	return held
}

// The same point read as BenchmarkLatestPointRead in the memory store, on
// the provider where the difference is not a map lookup against a map walk.
//
// Match selects every row of the channel, sorts it in the database, and
// decodes every payload and header into Go before discarding all but one.
// Get names both halves of PRIMARY KEY (channel, topic).
func BenchmarkLatestPointRead(b *testing.B) {
	for _, topics := range []int{100, 1000, 10000} {
		db := benchDB(b)
		lt, err := db.Latest("state")
		if err != nil {
			b.Fatalf("latest: %v", err)
		}
		payload := benchPayload(256)
		for i := range topics {
			if _, err := lt.Set(store.Record{
				MessageID: "m",
				Topic:     fmt.Sprintf("state/device/%d/level", i),
				Payload:   payload,
			}); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
		key := fmt.Sprintf("state/device/%d/level", topics-1)

		b.Run(fmt.Sprintf("get/%d", topics), func(b *testing.B) {
			for b.Loop() {
				if _, ok, err := lt.Get(key); err != nil || !ok {
					b.Fatalf("get: ok=%v err=%v", ok, err)
				}
			}
		})
		b.Run(fmt.Sprintf("match/%d", topics), func(b *testing.B) {
			got, err := lt.Match(func(t string) bool { return t == key })
			if err != nil || len(got) != 1 {
				b.Fatalf("match setup: %d records, err=%v", len(got), err)
			}
			for b.Loop() {
				if _, err := lt.Match(func(t string) bool { return t == key }); err != nil {
					b.Fatalf("match: %v", err)
				}
			}
		})
	}
}

// RFC 0004 "Removing a record": what a retention sweep costs on sqlite -
// one record removed from a channel of `held`, which is the shape the
// memory store's BenchmarkTrimOnALongChannel measures, so the two read side
// by side. The property is that it is flat in the channel's length; the
// number is what a sweep every second charges a provider.
func BenchmarkTrimOnALongChannel(b *testing.B) {
	now := time.Unix(1770000000, 0).UTC()
	for _, held := range []int{10000, 100000} {
		b.Run(fmt.Sprintf("%d-records", held), func(b *testing.B) {
			rec := store.Record{MessageID: "m", Topic: "events/x",
				Payload: benchPayload(128), Timestamp: now}
			size := store.RecordSize(rec)
			fill := func() *Log {
				lg, err := benchDB(b).Log("events")
				if err != nil {
					b.Fatal(err)
				}
				for range held {
					if _, err := lg.Append(rec); err != nil {
						b.Fatal(err)
					}
				}
				return lg
			}

			// Refilled rather than drained, one record removed per call,
			// for the reason the memory benchmark gives: the count comes
			// from how long one removal takes and is far larger than the
			// channel is long, and a target of zero bytes means no size
			// limit at all rather than an empty channel.
			lg, left := fill(), held
			b.ResetTimer()
			for range b.N {
				if left < 2 {
					b.StopTimer()
					lg, left = fill(), held
					b.StartTimer()
				}
				removed, _, err := lg.Trim(time.Time{}, int64(left-1)*size)
				if err != nil || removed != 1 {
					b.Fatalf("removed %d, %v", removed, err)
				}
				left--
			}
		})
	}
}

// What saguin_channel_consumer_position_min costs to answer, at three fleet
// sizes on one channel.
//
// **This is the number RFC 0005 asserted without measuring.** Its cost
// paragraph refuses "a scrape that reads a row per consumer" and had the
// sqlite store keep a field instead - a field that only ever fell, so the
// one alert the catalogue exists for fired on a healthy broker for ever.
// The field is gone and the query is back; this says what the query costs,
// so the next person to want a cache has a number to beat rather than a
// fear to act on.
//
// The memory store's walk is beside it because it is the same question
// asked of a map, and because it is the implementation that was always
// correct.
func BenchmarkLowestPosition(b *testing.B) {
	for _, consumers := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("memory/%d", consumers), func(b *testing.B) {
			lg := store.NewLog()
			held := positionsFor(b, lg)
			for i := range consumers {
				if err := lg.SavePosition(store.Position{
					Reader: store.MQTTReader(fmt.Sprintf("device-%06d", i)),
					Offset: uint64(i%held) + 1,
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for range b.N {
				if low, _ := lg.LowestPosition(); low != 1 {
					b.Fatal("wrong minimum: this benchmarked nothing")
				}
			}
		})
		b.Run(fmt.Sprintf("sqlite/%d", consumers), func(b *testing.B) {
			lg, err := benchDB(b).Log("events")
			if err != nil {
				b.Fatal(err)
			}
			held := positionsFor(b, lg)
			for i := range consumers {
				if err := lg.SavePosition(store.Position{
					Reader: store.MQTTReader(fmt.Sprintf("device-%06d", i)),
					Offset: uint64(i%held) + 1,
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for range b.N {
				if low, _ := lg.LowestPosition(); low != 1 {
					b.Fatal("wrong minimum: this benchmarked nothing")
				}
			}
		})
	}
}

// **Discarding one reader's positions, three ways.** A Clean Start connect
// has to forget whatever the client had stored, and saguin does it by
// walking every channel in the broker and asking each one - which is a
// transaction per channel however few positions the reader actually has.
// At ten thousand channels that is ten thousand transactions on the connect
// path, for a client that may have nothing stored at all.
//
// The two candidates replace the walk with one question to the provider,
// which is where the rows live: the positions table is keyed by channel and
// reader together, so a reader's rows across every channel are reachable in
// one statement.
//
// The control is per-channel, what saguin does now. bulk-delete is one
// DELETE for the reader across every channel. query-first is one SELECT for
// the channels the reader is in and then a DropPosition for each, which is
// the only one of the three that can still report which channels it cleared.
//
// **Both cases matter and they are not the same shape.** A client with
// nothing stored is the common one - every first connect of every device -
// and a client resuming has rows on the channels it reads. The control's
// cost is the same for both, which is the defect; a fix should be cheap for
// the first and proportional for the second.
func BenchmarkDiscardOneReadersPositions(b *testing.B) {
	const channels = 10_000

	for _, held := range []int{0, 180} {
		b.Run(fmt.Sprintf("%d-positions-held", held), func(b *testing.B) {
			db := benchDB(b)
			logs := make([]*Log, channels)
			for i := range channels {
				lg, err := db.Log(fmt.Sprintf("c%05d", i))
				if err != nil {
					b.Fatalf("log %d: %v", i, err)
				}
				logs[i] = lg
			}
			// Other readers' rows stay throughout, so a bulk delete has to
			// select this reader rather than empty the table.
			for i := range channels {
				if err := logs[i].SavePosition(store.Position{
					Reader: "other", Offset: 1, LastSeen: time.Now(),
				}); err != nil {
					b.Fatalf("seed other: %v", err)
				}
			}

			const reader = "mqtt:subscriber-1"
			seed := func() {
				for i := range held {
					// Offset 1, which an empty log's next is: what is
					// measured is the rows going, not where they pointed.
					if err := logs[i].SavePosition(store.Position{
						Reader: reader, Offset: 1, LastSeen: time.Now(),
					}); err != nil {
						b.Fatalf("seed: %v", err)
					}
				}
			}

			b.Run("per-channel", func(b *testing.B) {
				for range b.N {
					b.StopTimer()
					seed()
					b.StartTimer()
					for _, lg := range logs {
						if _, err := lg.DropPosition(reader); err != nil {
							b.Fatalf("drop: %v", err)
						}
					}
				}
			})

			b.Run("bulk-delete", func(b *testing.B) {
				for range b.N {
					b.StopTimer()
					seed()
					b.StartTimer()
					err := db.tx(func(tx *sql.Tx) error {
						_, err := tx.Exec(`DELETE FROM positions WHERE reader = ?`, reader)
						return err
					})
					if err != nil {
						b.Fatalf("bulk: %v", err)
					}
				}
			})

			b.Run("query-first", func(b *testing.B) {
				byName := map[string]*Log{}
				for i := range channels {
					byName[fmt.Sprintf("c%05d", i)] = logs[i]
				}
				for range b.N {
					b.StopTimer()
					seed()
					b.StartTimer()
					var names []string
					err := db.tx(func(tx *sql.Tx) error {
						rows, err := tx.Query(
							`SELECT channel FROM positions WHERE reader = ?`, reader)
						if err != nil {
							return err
						}
						defer rows.Close()
						for rows.Next() {
							var n string
							if err := rows.Scan(&n); err != nil {
								return err
							}
							names = append(names, n)
						}
						return rows.Err()
					})
					if err != nil {
						b.Fatalf("query: %v", err)
					}
					for _, n := range names {
						if _, err := byName[n].DropPosition(reader); err != nil {
							b.Fatalf("drop: %v", err)
						}
					}
				}
			})
		})
	}
}

// What the topic count costs a latest channel's write, which is the
// measurement that decided how Set is written.
//
// **A replacement and a first sighting are separate cases**, because they
// are what the store does in wildly different proportions: a fleet sees
// each of its topics once and then replaces that value for the rest of the
// channel's life. A benchmark averaging the two would report a number
// nothing experiences.
//
// The memory arm is here for the same reason it is in BenchmarkLatestSet:
// the count is the size of a map there, so this says what "free" looks
// like beside a store that has to be told.
func BenchmarkLatestSetCountsTopics(b *testing.B) {
	payload := benchPayload(128)
	const seeded = 1000

	for _, kind := range []string{"replace", "newtopic"} {
		b.Run("memory/"+kind, func(b *testing.B) {
			lt := store.NewLatest()
			for i := range seeded {
				if _, err := lt.Set(store.Record{
					Topic: fmt.Sprintf("state/d%d", i), Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := range b.N {
				topic := fmt.Sprintf("state/d%d", i%seeded)
				if kind == "newtopic" {
					topic = fmt.Sprintf("state/n%d", i)
				}
				if _, err := lt.Set(store.Record{Topic: topic, Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("sqlite/"+kind, func(b *testing.B) {
			lt, err := benchDB(b).Latest("state")
			if err != nil {
				b.Fatal(err)
			}
			for i := range seeded {
				if _, err := lt.Set(store.Record{
					Topic: fmt.Sprintf("state/d%d", i), Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := range b.N {
				topic := fmt.Sprintf("state/d%d", i%seeded)
				if kind == "newtopic" {
					topic = fmt.Sprintf("state/n%d", i)
				}
				if _, err := lt.Set(store.Record{Topic: topic, Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			// **A benchmark that kept a wrong count would still be fast.**
			// The rows are what the number claims to be about, so read them
			// once and refuse the reading if the two have parted.
			rows, err := lt.countRows()
			if err != nil {
				b.Fatal(err)
			}
			if int64(lt.Len()) != rows {
				b.Fatalf("the store reports %d topics and holds %d rows: this "+
					"measured a count that is wrong", lt.Len(), rows)
			}
		})
	}
}

// Seeding that count at startup, which is the one place this store counts
// rows - RFC 0005 permits it there and refuses it on a scrape.
func BenchmarkLatestCountAtOpen(b *testing.B) {
	payload := benchPayload(128)
	for _, topics := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("%dtopics", topics), func(b *testing.B) {
			lt, err := benchDB(b).Latest("state")
			if err != nil {
				b.Fatal(err)
			}
			for i := range topics {
				if _, err := lt.Set(store.Record{
					Topic: fmt.Sprintf("state/d%d", i), Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for range b.N {
				n, err := lt.countRows()
				if err != nil {
					b.Fatal(err)
				}
				if int(n) != topics {
					b.Fatalf("counted %d of %d", n, topics)
				}
			}
		})
	}
}
