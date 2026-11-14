package intervals

import (
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the series only the store knows, re-read every
// min_scrape_interval (metrics.md, Rules; Channels and retained values;
// Store and memory), each read from /metrics as an operator reads it.

// refreshInterval is config.md's broker.operations.min_scrape_interval at
// its default, which is also its least: "how often the store-held metric
// values are re-read [...]; at least 60s". No test can see the store-read
// values change sooner.
const refreshInterval = time.Minute

// storedPositions reads storage.md's positions in the database at path,
// by channel name: a channel with no row is not in the map.
func storedPositions(t *testing.T, path string) map[string]storedPosition {
	t.Helper()
	ps := make(map[string]storedPosition)
	support.Query(t, support.OpenDB(t, path), `SELECT c.name, count(*), min(p."offset") FROM positions p JOIN channels c ON c.id = p.channel
		GROUP BY c.name`, func(rows *sql.Rows) error {
		var name string
		var p storedPosition
		err := rows.Scan(&name, &p.count, &p.lowest)
		ps[name] = p
		return err
	})
	return ps
}

// mismatches returns, sorted, each series of want that e does not hold at
// its value, and each series of absent that e holds.
func mismatches(e testkit.Exposition, want map[string]float64, absent []string) []string {
	var wrong []string
	for _, key := range slices.Sorted(maps.Keys(want)) {
		if v, ok := e.Values[key]; !ok || v != want[key] {
			wrong = append(wrong, fmt.Sprintf("%s = %v (held %v); want %v", key, v, ok, want[key]))
		}
	}
	for _, key := range absent {
		if v, ok := e.Values[key]; ok {
			wrong = append(wrong, fmt.Sprintf("%s = %v; want no such series", key, v))
		}
	}
	return wrong
}

// storedPosition is what storage.md's positions holds on one channel: how
// many rows, and the lowest offset among them.
type storedPosition struct {
	count  int
	lowest int64
}

// metrics.md, Rules: "A value only the store knows, such as a size or
// count, is re-read every min_scrape_interval [...] The value is kept as an
// atomic until the next read, and a scrape returns the values held. These
// values are saguin_channel_bytes, saguin_channel_consumers,
// saguin_channel_position_min, saguin_retained_values,
// saguin_retained_bytes, saguin_store_bytes, saguin_store_wal_bytes,
// saguin_sessions_away, saguin_subscriptions and saguin_wills_waiting";
// "Store-derived gauges share the Store-read cut used for that refresh".
// The rows: saguin_channel_bytes, "bytes held, as retention counts them";
// saguin_channel_consumers, "stored positions"; saguin_channel_position_min,
// "the lowest stored position; absent while the channel has none";
// saguin_retained_values, "current retained values on $broadcast";
// saguin_retained_bytes, "their size"; saguin_store_bytes, "the database's
// size"; saguin_store_wal_bytes, "the write-ahead log's size";
// saguin_sessions_away, "away sessions"; saguin_subscriptions, "durable
// subscriptions from the store and non-durable subscriptions held by the
// core [...] Neither source counts the other's subscriptions";
// saguin_wills_waiting, "Wills waiting out their Will Delay". design.md,
// Channels, $broadcast: "Retention counts only unflagged records toward its
// size"; "A retained publish takes the flag"; Flows, SUBSCRIBE,
// UNSUBSCRIBE: "When the last subscription reaching an append channel or a
// queue is removed, a durable session keeps its position there";
// storage.md, positions: "a durable session's place in each channel it
// consumes"; "A place on an append channel or a queue stays until the
// session ends, also while no subscription reaches the channel"; "A
// message's bytes are its topic's, payload's and properties' bytes".
//
// The broker has two append channels, events and quiet. Every record is
// testkit.RecordBytes, or twice it, its PUBLISH carrying no property. Its
// first scrape, before any client, holds no channel's lowest position. Then:
//   - a, durable, subscribes to events/# while it is empty and goes away;
//   - a publisher stores events/1 to /3; retained values r/a and r/b, of
//     one and two records' bytes; r/a again, retained, which takes the flag
//     from the first; and p/x, plain;
//   - b, durable, subscribes to events/#, acknowledges the three records
//     and goes away;
//   - d, durable, subscribes to news/# and alerts/# and stays;
//   - n, whose session ends with its connection, subscribes to quiet/# and
//     stays;
//   - k, durable, subscribes to quiet/#, unsubscribes and stays, keeping
//     its position there;
//   - w, durable, with a Will of an hour's delay, loses its connection.
//
// Once a refresh has followed, and within min_scrape_interval and the
// watchdog after the last of that, /metrics reads: events 3 records' bytes,
// $broadcast 2 (the replaced r/a and p/x), quiet none; the stored positions
// of each channel and the lowest, as the database holds them, events among
// them and quiet's one, k's kept position, n's never stored; 2 retained
// values of 3 records' bytes; the database and its log at their files' sizes, the store idle;
// 3 away sessions (a, b and w); 5 subscriptions, 4 durable and n's; one
// Will waiting. Nothing writes once the clients are done, so what the
// database holds then is what the refresh reads.
//
// position_min: its row, "absent while the channel has none": none at the
// first scrape; after the refresh, present at the lowest stored position
// where the channel has one, absent on quiet.
func TestStoreReadSeries(t *testing.T) {
	t.Parallel() // its wait for the refresh runs beside the other parallel tests
	const rb = testkit.RecordBytes
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#"},
		{Name: "quiet", Type: "append", Filter: "quiet/#"},
	}})
	channels := []string{"$broadcast", "events", "quiet"}
	first := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	db := support.OpenDB(t, b.DB)
	durable := func(id string) *harness.Connect {
		return &harness.Connect{ClientID: id, CleanStart: true, Props: support.SessionExpiry(support.Hour)}
	}

	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, durable("refresh-a"), support.At(1, "events/#")))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "refresh-pub", CleanStart: true})
	retained := func(topic string, n int) *harness.Publish {
		p := support.Sized(topic, n)
		p.Retain = true
		return p
	}
	events := []*harness.Publish{support.Sized("events/1", rb), support.Sized("events/2", rb), support.Sized("events/3", rb)}
	support.PublishAll(t, pub, append(slices.Clone(events), retained("r/a", rb), retained("r/b", 2*rb), retained("r/a", rb), support.Sized("p/x", rb)))

	reader := support.Subscribed(t, b.Addr, harness.V5, durable("refresh-b"), support.At(1, "events/#"))
	for _, p := range events {
		d := support.DeliveredAt(t, reader, 1, p.Topic, p.Payload)
		if err := reader.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
			t.Fatal(err)
		}
	}
	support.Away(t, reader)
	support.Subscribed(t, b.Addr, harness.V5, durable("refresh-d"), support.At(0, "news/#", "alerts/#"))
	support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "refresh-n", CleanStart: true}, support.At(0, "quiet/#"))
	k := support.Subscribed(t, b.Addr, harness.V5, durable("refresh-k"), support.At(0, "quiet/#"))
	unsubscribe := &harness.Unsubscribe{ID: k.NextID(), Filters: []string{"quiet/#"}}
	if err := k.Send(unsubscribe); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Unsuback); return ok && a.ID == unsubscribe.ID }); err != nil {
		t.Fatalf("waiting for refresh-k's UNSUBACK: %v", err)
	}
	w := durable("refresh-w")
	w.Will = &harness.Will{Topic: "will/w", Payload: support.WillPayload, Props: support.WillDelay(support.Hour)}
	c, _ := harness.Connected(t, b.Addr, harness.V5, w)
	support.DropSocket(t, c)
	support.AwaitWillDue(t, db, "refresh-w")
	support.AwaitAway(t, db, "refresh-a")
	support.AwaitAway(t, db, "refresh-b")

	positions := storedPositions(t, b.DB)
	if positions["events"].count == 0 || positions["quiet"].count != 1 {
		t.Fatalf("the database holds positions %v; want events' durable consumers' and one on quiet, refresh-k's, kept with no subscription, "+
			"refresh-n's never stored (storage.md, positions: \"a durable session's place\"; \"A place on an append channel or a queue stays until the session ends\")", positions)
	}
	want := map[string]float64{
		support.ChannelSeries("saguin_channel_bytes", "events"):     3 * rb,
		support.ChannelSeries("saguin_channel_bytes", "$broadcast"): 2 * rb,
		support.ChannelSeries("saguin_channel_bytes", "quiet"):      0,
		"saguin_retained_values{}":                                  2,
		"saguin_retained_bytes{}":                                   3 * rb,
		"saguin_store_bytes{}":                                      float64(testkit.FileSizeOrZero(t, b.DB)),
		"saguin_store_wal_bytes{}":                                  float64(testkit.FileSizeOrZero(t, b.DB+"-wal")),
		"saguin_sessions_away{}":                                    3,
		"saguin_subscriptions{}":                                    5,
		"saguin_wills_waiting{}":                                    1,
	}
	present := make(map[string]float64) // the channels' lowest positions, where they have one
	var none []string                   // and those that must be absent
	for _, ch := range channels {
		p := positions[ch]
		want[support.ChannelSeries("saguin_channel_consumers", ch)] = float64(p.count)
		if key := support.ChannelSeries("saguin_channel_position_min", ch); p.count == 0 {
			none = append(none, key)
		} else {
			present[key] = float64(p.lowest)
		}
	}
	var e testkit.Exposition
	if !testkit.Within(refreshInterval+harness.DefaultTimeout, func() bool {
		e = testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
		return len(mismatches(e, want, nil))+len(mismatches(e, present, none)) == 0
	}) {
		t.Logf("no scrape within %v of the clients being done read what they did", refreshInterval+harness.DefaultTimeout)
	}

	t.Run("values", func(t *testing.T) {
		if want["saguin_store_bytes{}"] == 0 || want["saguin_store_wal_bytes{}"] == 0 {
			t.Errorf("the database's files are %v and %v bytes; want both above zero, the store having committed",
				want["saguin_store_bytes{}"], want["saguin_store_wal_bytes{}"])
		}
		for _, m := range mismatches(e, want, nil) {
			t.Error(m)
		}
	})
	t.Run("position_min", func(t *testing.T) {
		for _, ch := range channels {
			if v, ok := first.Values[support.ChannelSeries("saguin_channel_position_min", ch)]; ok {
				t.Errorf("the first scrape, before any client, holds %s's lowest position %v; want none", ch, v)
			}
		}
		for _, m := range mismatches(e, present, none) {
			t.Error(m)
		}
	})
}

// design.md, Store, memory mode: the broker "copies its file, byte for
// byte, to <id>.db in memory_dir" and "runs on the RAM copy"; "Every
// snapshot_interval [...] a snapshot copies the RAM copy's database file
// [...] to file_path.new", snapshot_interval 30m by default (config.md);
// Store: "SQLite manages its own log and folds it into the database by its
// own default rule", once the log holds 1,000 pages (SQLite's default
// wal_autocheckpoint, which Sagüin leaves alone). metrics.md, Store and
// memory: saguin_store_info{mode}, "always 1; full, normal or memory";
// saguin_store_bytes, "the database's size"; Rules: "The _info series start
// at 1"; "A value only the store knows, such as a size or count, is re-read
// every min_scrape_interval".
//
// A broker in each mode, memory_dir on a tmpfs in memory mode: /metrics
// holds one saguin_store_info, 1, labelled with that mode. In memory mode, a
// publisher's publications of ramGrowth bytes in all, noisy, take the log
// past 1,000 pages, which SQLite then folds into the RAM copy: the RAM copy
// outgrows file_path, whose byte copy it began as, no snapshot being due.
// Once a refresh has followed, saguin_store_bytes reads the RAM copy's size.
func TestStoreModeSeries(t *testing.T) {
	t.Parallel() // memory mode's wait for the refresh runs beside the other parallel tests
	const ramRecord = 900 << 10
	const ramGrowth = 6 * ramRecord
	for _, mode := range []string{"full", "normal", "memory"} {
		t.Run(mode, func(t *testing.T) {
			o := &harness.Options{StorageMode: mode}
			if mode == "memory" {
				if o.MemoryDir = support.MemoryDir(t, 4*ramGrowth); o.MemoryDir == "" {
					return
				}
			}
			b := support.OpsLaunch(t, o)
			var modes []string
			for _, s := range testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics())).Samples {
				if s.Name != "saguin_store_info" {
					continue
				}
				modes = append(modes, s.Labels["mode"])
				if s.Value != 1 {
					t.Errorf("saguin_store_info{mode=%q} = %v; want 1", s.Labels["mode"], s.Value)
				}
			}
			if !slices.Equal(modes, []string{mode}) {
				t.Errorf("/metrics holds saguin_store_info of the modes %q; want %q alone", modes, mode)
			}
			if mode != "memory" {
				return
			}

			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ram-grows", CleanStart: true})
			for i := range ramGrowth / ramRecord {
				support.Acknowledged(t, pub, support.Noisy(t, fmt.Sprintf("ram/%d", i), ramRecord))
			}
			ram := testkit.FileSize(t, b.RAM)
			if disk := testkit.FileSize(t, b.DB); ram <= disk {
				t.Fatalf("the RAM copy is %d bytes, file_path %d: the log was not folded into the RAM copy, so its size tells it from file_path's no more", ram, disk)
			}
			var got float64
			if !testkit.Within(refreshInterval+harness.DefaultTimeout, func() bool {
				got = testkit.Sample(t, b.Metrics(), "saguin_store_bytes")
				return got == float64(ram)
			}) {
				t.Errorf("saguin_store_bytes reads %v within %v of the RAM copy growing; want the RAM copy's size, %d bytes",
					got, refreshInterval+harness.DefaultTimeout, ram)
			}
		})
	}
}
