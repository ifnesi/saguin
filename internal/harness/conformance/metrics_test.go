package conformance

import (
	"cmp"
	"database/sql"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of /metrics as an operator scrapes it (design.md, Operations,
// HTTP: "/metrics returns every series in metrics.md"), metrics.md their
// oracle: its tables are read here, so what they list is what is checked.

// metricsDoc is metrics.md, from this package's directory.
const metricsDoc = "../../../docs/metrics.md"

// documented is one series name of a row of metrics.md's tables.
type documented struct {
	name    string
	labels  []string // the label names the row gives it, in its order
	kind    string   // its Type column: gauge or counter
	section string   // the heading it is listed under
	meaning string   // its Meaning column
}

// seriesCell matches one series a row's Series column names, with its
// label names.
var seriesCell = testkit.SeriesCell

// backticked matches what metrics.md writes as code.
var backticked = regexp.MustCompile("`([^`]+)`")

// labelValue matches a code span that is a plain label value: lower case,
// digits and underscores.
var labelValue = regexp.MustCompile(`^[a-z0-9_]+$`)

// metricsTables reads metrics.md's tables, and its Rules section's text.
func metricsTables(t *testing.T) (rows []documented, rules string) {
	t.Helper()
	doc, rules := testkit.MetricsDoc(t, metricsDoc)
	for _, r := range doc {
		rows = append(rows, documented{name: r.Name, labels: r.Labels, kind: r.Kind, section: r.Section, meaning: r.Meaning})
	}
	return rows, rules
}

// listedAfter returns the code spans of text that follow "`label` is ",
// up to the ";", line end or sentence-ending "." that ends that list
// outside a code span, and whether text has such a list. A code span inside
// parentheses explains a value rather than being one ("`access` (password
// files and `acl_file`)"), so it is left out.
func listedAfter(text, label string) ([]string, bool) {
	_, rest, ok := strings.Cut(text, "`"+label+"` is ")
	if !ok {
		return nil, false
	}
	var values []string
	code, depth, start := false, 0, 0
	for i, c := range rest {
		switch {
		case c == '`' && code:
			code = false
			if depth == 0 {
				values = append(values, rest[start:i])
			}
		case c == '`':
			code, start = true, i+1
		case code:
		case c == '(':
			depth++
		case c == ')' && depth > 0:
			depth--
		case c == ';' || c == '\n' || (c == '.' && (i+1 == len(rest) || rest[i+1] == ' ' || rest[i+1] == '\n')):
			return values, true
		}
	}
	return values, true
}

// labelValues returns the values of label that apply to d: those the
// configuration gives where configured has the label, else those d's
// meaning lists after "`label` is", else those Rules list so, else the
// plain label values d's meaning writes as code.
func labelValues(d *documented, label, rules string, configured map[string][]string) []string {
	if values, ok := configured[label]; ok {
		return values
	}
	if values, ok := listedAfter(d.meaning, label); ok {
		return values
	}
	if values, ok := listedAfter(rules, label); ok {
		return values
	}
	var values []string
	for _, m := range backticked.FindAllStringSubmatch(d.meaning, -1) {
		if labelValue.MatchString(m[1]) && !strings.HasPrefix(m[1], "saguin_") && !slices.Contains(values, m[1]) {
			values = append(values, m[1])
		}
	}
	return values
}

// expectedSeries returns the series of d, as testkit.SeriesKey names them:
// one for every combination of the values of its labels (metrics.md, Rules:
// "Every series exists from start, for every value of its labels that
// applies"). A label no value applies to gives none.
func expectedSeries(d *documented, rules string, configured map[string][]string) []string {
	combos := [][]string{nil}
	for _, label := range d.labels {
		var next [][]string
		for _, combo := range combos {
			for _, v := range labelValues(d, label, rules, configured) {
				next = append(next, append(slices.Clone(combo), label, v))
			}
		}
		combos = next
	}
	keys := make([]string, len(combos))
	for i, combo := range combos {
		keys[i] = testkit.SeriesKey(d.name, combo...)
	}
	return keys
}

// sectionValues returns, for each section of rows and each label, the plain
// values the rows of that section list for it, in their order: a row whose
// meaning lists none for a label it carries takes its section's. In Store
// and memory, saguin_memory_in_transit_bytes{direction} lists "each budget,
// incoming or outgoing", and saguin_memory_max_bytes{direction} and
// saguin_memory_full_seconds_total{direction}, each "each budget's", list
// no value of their own: metrics.md's Rules list no value of direction, so
// this is the test's reading of those rows.
func sectionValues(rows []documented, rules string) map[string]map[string][]string {
	shared := make(map[string]map[string][]string)
	for i := range rows {
		d := &rows[i]
		for _, label := range d.labels {
			for _, v := range labelValues(d, label, rules, nil) {
				if shared[d.section] == nil {
					shared[d.section] = make(map[string][]string)
				}
				if !slices.Contains(shared[d.section][label], v) {
					shared[d.section][label] = append(shared[d.section][label], v)
				}
			}
		}
	}
	return shared
}

// notBuilt names, by metric or by series, what metrics.md lists that an
// item still to come builds, with that item: it removes its lines here.
var notBuilt = map[string]string{}

// knownAtStart is the value a gauge holds at a fresh broker's start, before
// any client, where the document says what it is: nothing connected, in
// flight or in transit, each message-memory budget its default size, the
// disk not full, the gate open (metrics.md, Rules: "Other gauges start at
// their current value").
var knownAtStart = map[string]float64{
	`saguin_connections{protocol="3.1.1"}`:                 0,
	`saguin_connections{protocol="5"}`:                     0,
	"saguin_connections_open{}":                            0,
	"saguin_inflight{}":                                    0,
	"saguin_qos2_held{}":                                   0,
	`saguin_memory_in_transit_bytes{direction="incoming"}`: 0,
	`saguin_memory_in_transit_bytes{direction="outgoing"}`: 0,
	// config.md, broker.limits: max_incoming_memory's default, 32MiB, and
	// max_outgoing_memory's, 224MiB.
	`saguin_memory_max_bytes{direction="incoming"}`: 32 << 20,
	`saguin_memory_max_bytes{direction="outgoing"}`: 224 << 20,
	"saguin_store_full{}":                           0,
	"saguin_delivery_admission_gated{}":             0,
	// The store holds no record, position, retained value, session,
	// subscription or Will.
	`saguin_channel_records{channel="$broadcast"}`:   0,
	`saguin_channel_records{channel="events"}`:       0,
	`saguin_channel_bytes{channel="$broadcast"}`:     0,
	`saguin_channel_bytes{channel="events"}`:         0,
	`saguin_channel_consumers{channel="$broadcast"}`: 0,
	`saguin_channel_consumers{channel="events"}`:     0,
	"saguin_retained_values{}":                       0,
	"saguin_retained_bytes{}":                        0,
	"saguin_sessions_away{}":                         0,
	"saguin_subscriptions{}":                         0,
	"saguin_wills_waiting{}":                         0,
}

// absentWhileNone are the series whose row says they are absent while the
// store holds none of what they read, as at TestMetricsListEverySeriesFromStart's
// start: saguin_channel_position_min, "absent while the channel has none"
// (TestStoreReadSeries checks it).
var absentWhileNone = []string{"saguin_channel_position_min"}

// metrics.md, Rules: "Every series exists from start, for every value of
// its labels that applies. A floor exists only where a channel has one, and
// queue series exist only for queues. Counters start at 0. The _info series
// start at 1. Other gauges start at their current value"; "A channel is a
// configured channel, its <channel>__dlq or $broadcast"; the labels of the
// _info series "as configured"; design.md, Operations, HTTP: "/metrics
// returns every series in metrics.md"; Store: "full mode runs on file_path.
// The default mode is defined in config.md, broker.storage." The
// memory-mode series ("In memory mode,") do not apply to a full-mode store,
// nor do bridge series without a bridge, nor queue series without a queue.
//
// The broker has broker.id harness, a full-mode store, one append channel,
// events, and no bridge or queue. A broker's first start on a new database
// stores the configured channel (storage.md, channels: "one row per stored
// log: configured channels"), a write the store commits before any scrape,
// so a counter of commits rightly reads it then. Counters start at 0 when
// their series is made, so the start value is read where nothing happens
// before the first scrape: the broker is started once, stopped, and started
// again on the same database with the same configuration. That start has
// no channel to store, no record for overdue retention (Words: Retention,
// "Startup processes overdue retention before serving"), no session to end
// and no Will (Restart, Start: "It ends expired sessions"). Its first
// scrape, taken once its operations listener accepts and before any client
// has connected, holds
// every series metrics.md lists that is built (notBuilt), each of its
// table's Type, with its labels: the configuration's (channel: events and
// $broadcast, both with a floor; version: what --version prints;
// broker_id: harness; mode: full) or those its row or the Rules list, or,
// where its row lists none for a label, those its section's rows list
// (sectionValues: each message-memory budget, direction incoming and
// outgoing).
// Counters are 0, but the Go runtime's, which "each scrape reads", and the
// scrape's own request of /metrics, which may count itself; _info series
// are 1, and exactly those that apply; knownAtStart's gauges hold their
// value, and each channel's tail and floor are its stored next and floor
// (storage.md, channels: next, "the offset the next publication entering
// the channel takes at startup"; floor, "the oldest offset retention has
// not passed"). absentWhileNone's series apply to no channel. The
// channel_info series' filter label is a channel's, which this broker's
// configured labels cannot tell from a bridge rule's
// (TestChannelIdentityAndOffsets checks them).
func TestMetricsListEverySeriesFromStart(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Operations: true, Channels: []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}}
	stopped(t, support.Launch(t, o)) // the first start stores events
	b := support.Started(t, o)
	_ = opsDial(t, support.PlainOps(b)).Close() // the listener accepts; no request was made
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	rows, rules := metricsTables(t)
	configured := map[string][]string{
		"channel":   {"$broadcast", "events"},
		"version":   {versionOf(t, support.BrokerBinary)},
		"broker_id": {"harness"},
		"mode":      {"full"},
		// No bridge: no bridge, peer, rule filter or topic.
		"bridge": nil, "peer": nil, "filter": nil, "topic": nil,
	}
	queues := map[string][]string{"channel": nil} // no queue: no queue series
	// No bridge: no rule direction either; the direction of a message-memory
	// budget is no bridge's (sectionValues).
	bridges := maps.Clone(configured)
	bridges["direction"] = nil
	shared := sectionValues(rows, rules)
	scrapes := testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "200")
	known := maps.Clone(knownAtStart)
	support.Query(t, support.OpenDB(t, b.DB), `SELECT name, next, floor FROM channels`, func(rows *sql.Rows) error {
		var name string
		var next, floor float64
		err := rows.Scan(&name, &next, &floor)
		known[testkit.SeriesKey("saguin_channel_next_offset", "channel", name)] = next
		known[testkit.SeriesKey("saguin_channel_floor_offset", "channel", name)] = floor
		return err
	})
	for i := range rows {
		d := &rows[i]
		if item, ok := notBuilt[d.name]; ok {
			t.Logf("%s: built by %s", d.name, item)
			continue
		}
		if slices.Contains(absentWhileNone, d.name) {
			continue
		}
		// metrics.md, Store and memory: saguin_snapshots_total{result},
		// "In memory mode, counts snapshots with result ok or failed.";
		// saguin_snapshot_last_success_timestamp_seconds, "In memory mode,
		// records when the last good snapshot finished.";
		// saguin_snapshot_last_duration_seconds, "In memory mode, records
		// how long the last snapshot took, failed or not."
		if strings.HasPrefix(strings.TrimSpace(d.meaning), "In memory mode,") {
			continue
		}
		labels := configured
		switch d.section {
		case "Queues":
			labels = queues
		case "Bridges":
			labels = bridges
		default:
			for _, label := range d.labels {
				if _, set := labels[label]; !set && len(labelValues(d, label, rules, nil)) == 0 && shared[d.section][label] != nil {
					labels = maps.Clone(labels)
					labels[label] = shared[d.section][label]
				}
			}
		}
		want := slices.DeleteFunc(expectedSeries(d, rules, labels), func(key string) bool {
			_, later := notBuilt[key]
			return later
		})
		if len(want) == 0 {
			continue
		}
		if got := e.Types[d.name]; got != d.kind {
			t.Errorf("%s: TYPE %q, metrics.md's %q", d.name, got, d.kind)
		}
		for _, key := range want {
			v, ok := e.Values[key]
			start, isKnown := known[key]
			switch {
			case !ok:
				t.Errorf("the first scrape has no series %s", key)
			case strings.HasSuffix(d.name, "_info") && v != 1:
				t.Errorf("%s = %v at start, want 1", key, v)
			case d.kind == "counter" && d.section != "Go runtime" && key != scrapes && v != 0:
				t.Errorf("%s = %v at start, want 0", key, v)
			case isKnown && v != start:
				t.Errorf("%s = %v at start, want %v", key, v, start)
			}
		}
		if strings.HasSuffix(d.name, "_info") {
			for _, s := range e.Samples {
				if key := testkit.SeriesKey(s.Name, flatten(s.Labels)...); s.Name == d.name && !slices.Contains(want, key) {
					t.Errorf("%s: a series that does not apply, %s", d.name, key)
				}
			}
		}
	}
}

// flatten returns labels as name, value pairs.
func flatten(labels map[string]string) []string {
	pairs := make([]string, 0, 2*len(labels))
	for name, value := range labels {
		pairs = append(pairs, name, value)
	}
	return pairs
}

// TestEverySeriesServedCarriesItsMeaning: metrics.md, "Each series carries
// its Meaning below as its HELP line, as plain text." In the Prometheus
// text format a family's HELP line comes before its TYPE line, its text
// with a backslash written \\ and a line feed \n. Every family a scrape
// holds has that line, with its row's Meaning, just before its TYPE line.
func TestEverySeriesServedCarriesItsMeaning(t *testing.T) {
	b := support.Started(t, &harness.Options{Dir: t.TempDir(), Operations: true})
	_ = opsDial(t, support.PlainOps(b)).Close() // the listener accepts
	lines := strings.Split(testkit.Scrape(t, b.Metrics()), "\n")
	rows, _ := metricsTables(t)
	meaning := make(map[string]string, len(rows))
	for _, d := range rows {
		meaning[d.name] = testkit.PlainMeaning(d.meaning)
	}
	escape := strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	families := 0
	for i, line := range lines {
		typed, ok := strings.CutPrefix(line, "# TYPE ")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(typed, " ")
		families++
		want := "# HELP " + name + " " + escape.Replace(meaning[name])
		if i == 0 || lines[i-1] != want {
			t.Errorf("%s: the line before its TYPE line is %q; want %q", name, lines[max(i-1, 0)], want)
		}
	}
	if families < 40 {
		t.Fatalf("the scrape held %d families: too few to be the broker's page", families)
	}
}

// stalledScrape bounds a scrape taken while the store writer waits for a
// write lock the test holds. SQLite's wait for the lock lasts up to 5
// seconds (writeLock), and the test holds it until the scrape has
// answered, so a scrape waiting on the store would take that long.
const stalledScrape = time.Second

// metrics.md, Rules: "A scrape returns published values without waiting.
// In particular, it never waits on the core or the store"; design.md, R1's
// table: HTTP request, "metrics read atomic counters"; Rules: a value only
// the store knows "is kept as an atomic until the next read, and a scrape
// returns the values held", storeReadSeries naming those values. The test
// scrapes once an MQTT 5 client has connected, then holds the database's
// write lock (writeLock) while the client publishes at QoS 1; once the
// store writer has met the lock, the store can save nothing. A scrape then
// answers within stalledScrape, the series it read rendered as Prometheus
// text, every store-read series the first scrape held held again at its
// value, nothing having been saved since; only then is the lock released
// and the PUBACK read.
func TestScrapeDoesNotWaitForTheStore(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{Operations: true})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "stalled", CleanStart: true})
	_, rules := metricsTables(t)
	storeRead := storeReadSeries(t, rules)
	first := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	p := &harness.Publish{QoS: 1, Topic: "stalled", Payload: []byte("x")}
	support.Sent(t, c, p)
	awaitLockMet(t, b.trace, before, "the PUBLISH")
	text := testkit.Scrape(t, b.Metrics(), testkit.Backstop(stalledScrape))
	e := testkit.ParseScrape(t, text)
	e.Value(t, "saguin_connections", "protocol", "5")
	carried := 0
	for _, s := range first.Samples {
		if !slices.Contains(storeRead, s.Name) {
			continue
		}
		carried++
		if key := testkit.SeriesKey(s.Name, flatten(s.Labels)...); e.Values[key] != s.Value {
			v, ok := e.Values[key]
			t.Errorf("with the store stalled, %s = %v (held %v); want %v, the value held before", key, v, ok, s.Value)
		}
	}
	if carried == 0 {
		t.Errorf("the scrape before the stall holds none of the store-read series %q", storeRead)
	}
	lock.release(t)
	support.Puback(t, c, p)
}

// storeReadSeries returns the metrics Rules lists as the values only the
// store knows ("These values are [...]"), failing the test where it lists
// none.
func storeReadSeries(t *testing.T, rules string) []string {
	t.Helper()
	_, list, _ := strings.Cut(rules, "These values are ")
	list, _, _ = strings.Cut(list, ".\n")
	matches := seriesCell.FindAllStringSubmatch(list, -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, m[1])
	}
	if len(names) == 0 {
		t.Fatal("metrics.md's Rules list no value only the store knows")
	}
	return names
}

// awaitSeries waits until each series of want, named as testkit.SeriesKey
// names it, reads its value on b's /metrics, failing the test where they
// do not within harness.DefaultTimeout, the test's watchdog: a counter
// moves on the goroutine that sees its event, which no client can wait on
// (metrics.md, Rules).
func awaitSeries(t *testing.T, b *harness.Broker, want map[string]float64) {
	t.Helper()
	var got map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = support.Sampled(t, b, slices.Collect(maps.Keys(want))...)
		return maps.Equal(got, want)
	}) {
		t.Fatalf("after %v, /metrics reads %v; want %v", harness.DefaultTimeout, got, want)
	}
}

// connectionSeries are metrics.md's connection gauges and counter.
const (
	connected311    = `saguin_connections{protocol="3.1.1"}`
	connected5      = `saguin_connections{protocol="5"}`
	connectionsOpen = "saguin_connections_open{}"
	accepted        = "saguin_connections_accepted_total{}"
)

// metrics.md, Connections and sessions: saguin_connections{protocol},
// "connected clients, 3.1.1 or 5: the sessions the core holds connected";
// saguin_connections_open, "sockets served, from their permit to their
// close"; saguin_connections_accepted_total, "sockets accepted, refused ones
// included". design.md, CONNECT, 5: "An old connection for the id gets
// DISCONNECT 0x8E on MQTT 5 and is closed".
//
// Two sockets that send nothing are served and accepted; an MQTT 5 and an
// MQTT 3.1.1 client each count under their protocol; a CONNECT refused
// (0x85) counts no session, its socket accepted; an MQTT 3.1.1 client
// taking over the MQTT 5 client's id moves that session to 3.1.1. As each
// socket closes, open falls back, and with every client gone, nothing is
// connected.
func TestConnectionSeries(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	awaitSeries(t, b, map[string]float64{connectionsOpen: 0, connected5: 0, connected311: 0})
	base := support.Sampled(t, b, accepted)[accepted]
	raw := make([]*harness.Client, 0, 2)
	for range 2 {
		c, err := harness.Dial(b.Addr, harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, c)
	}
	awaitSeries(t, b, map[string]float64{connectionsOpen: 2, accepted: base + 2})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "series-a", CleanStart: true})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "series-b", CleanStart: true})
	awaitSeries(t, b, map[string]float64{connected5: 1, connected311: 1, connectionsOpen: 4, accepted: base + 4})
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: "series-\x01", CleanStart: true}, 0x85)
	awaitSeries(t, b, map[string]float64{connected5: 1, connected311: 1, connectionsOpen: 4, accepted: base + 5})
	taker, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "series-a", CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, v5); d.Reason != 0x8E {
		t.Fatalf("the client taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	harness.ExpectClosed(t, v5)
	awaitSeries(t, b, map[string]float64{connected5: 0, connected311: 2, accepted: base + 6})
	for _, c := range append(raw, v5, v311, taker) {
		_ = c.Close() // ending each socket; v5's has ended already
	}
	awaitSeries(t, b, map[string]float64{connected5: 0, connected311: 0, connectionsOpen: 0})
}

// refusedSeries is saguin_connections_refused_total of reason.
func refusedSeries(reason string) string {
	return `saguin_connections_refused_total{reason="` + reason + `"}`
}

// metrics.md, Connections and sessions: saguin_connections_refused_total,
// "the CONNACK's reason (design.md, CONNECT): unsupported_protocol_version
// (an unknown level included), client_identifier_not_valid, [...]
// implementation_specific_error, bad_authentication_method, [...];
// malformed_packet (another Protocol Name included) and protocol_error (a
// first packet other than CONNECT included), for a connection closed before
// its CONNACK; or connect_timeout (no CONNECT in time)"; Rules: "The
// reason is counted before an MQTT 3.1.1 client's code is mapped".
// design.md, CONNECT's refusal table: 0x84 "an MQTT 3.1.1 client below
// min_protocol_version", return code 1; "none | an unknown protocol level
// [...] | 1, sent in MQTT 3.1.1 form"; 0x85 "empty with Clean Start 0";
// 0x83 "an unknown saguin- property"; 0x8C "an Authentication Method";
// "Any other Protocol Name [...] closes the connection without a CONNACK";
// Accept: "If no CONNECT arrives within connect_timeout, the connection
// closes".
//
// With min_protocol_version 5 and connect_timeout 1 second, each refusal
// is sent once, on a connection of its own, and each reason counts it.
func TestRefusalSeries(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MinProtocolVersion: "5", ConnectTimeout: time.Second})
	reasons := []string{"unsupported_protocol_version", "client_identifier_not_valid", "implementation_specific_error",
		"bad_authentication_method", "malformed_packet", "protocol_error", "connect_timeout"}
	series := make([]string, len(reasons))
	for i, r := range reasons {
		series[i] = refusedSeries(r)
	}
	base := support.Sampled(t, b, series...)
	connect := func(version byte, p harness.Connect) []byte {
		p.ProtocolName = cmp.Or(p.ProtocolName, "MQTT")
		p.Version = cmp.Or(p.Version, version)
		return harness.Encoded(t, &p, version)
	}
	silent, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = silent.Close() }) // the test is over
	refusedConnect(t, b.Addr, harness.V311, connect(harness.V311, harness.Connect{ClientID: "below", CleanStart: true}), 1)
	refusedConnect(t, b.Addr, harness.V311, connect(harness.V311, harness.Connect{Version: 6, ClientID: "level-6", CleanStart: true}), 1)
	refusedConnect(t, b.Addr, harness.V5, connect(harness.V5, harness.Connect{}), 0x85)
	refusedConnect(t, b.Addr, harness.V5, connect(harness.V5, harness.Connect{ClientID: "reserved", CleanStart: true,
		Props: harness.Properties{harness.User("saguin-id", "x")}}), 0x83)
	refusedConnect(t, b.Addr, harness.V5, connect(harness.V5, harness.Connect{ClientID: "auth", CleanStart: true,
		Props: harness.Properties{{ID: harness.AuthenticationMethod, Str: "m"}}}), 0x8C)
	refusedConnect(t, b.Addr, harness.V5, connect(harness.V5, harness.Connect{ProtocolName: "MQTX", ClientID: "mqtx", CleanStart: true}), 0)
	refusedConnect(t, b.Addr, harness.V5, harness.Encoded(t, &harness.Pingreq{}, harness.V5), 0)
	harness.ExpectClosed(t, silent)
	want := map[string]float64{}
	for _, s := range series {
		want[s] = base[s] + 1
	}
	want[refusedSeries("unsupported_protocol_version")]++
	awaitSeries(t, b, want)
}

// disconnectSeries is saguin_disconnects_total of reason.
func disconnectSeries(reason string) string {
	return `saguin_disconnects_total{reason="` + reason + `"}`
}

// closuresSince waits until b's closure series (closures), summed, count
// one more than before, a reading of baselineClosures, under
// harness.DefaultTimeout as the watchdog, and returns each series that
// moved since, with by how much:
// a counter moves on the goroutine that sees its event, which no client
// can wait on (metrics.md, Rules), so only the series say a closure is
// counted.
func closuresSince(t *testing.T, b *harness.Broker, before map[string]float64) map[string]float64 {
	t.Helper()
	since := map[string]float64{}
	counted := 0.0
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		clear(since)
		counted = 0
		for series, v := range closures(t, b) {
			if d := v - before[series]; d != 0 {
				since[series] = d
				counted += d
			}
		}
		return counted == 1
	}) {
		t.Fatalf("watchdog: the closure series moved by %v since, %v closures; want 1", since, counted)
	}
	return since
}

// metrics.md, Connections and sessions: saguin_disconnects_total, "the
// reason Sagüin selected for a closure, including a closure without a
// sendable DISCONNECT"; protocol_error, malformed_packet, packet_too_large,
// keep_alive_timeout, "or client (the client's DISCONNECT)"; Rules: "An
// MQTT 3.1.1 connection closed where MQTT 5 would get a code is counted
// under that code's name". design.md, DISCONNECT, Reasons Sagüin sends:
// "0x81 for a malformed packet, 0x82 for a protocol error, 0x8D for
// keep-alive timeout, [...] 0x95 for packet too large"; "An MQTT 3.1.1
// connection just closes"; A lost connection: "a nonzero effective Keep
// Alive gives the reader 1.5 times that interval". MQTT 5 [MQTT-3.1.0-2]: a
// second CONNECT is a Protocol Error.
//
// With max_packet_size and max_connect_size at 1,024, one MQTT 5 and one
// MQTT 3.1.1 client each send a second CONNECT, a PINGREQ with a body, a
// PUBLISH header announcing 2,048 bytes, their DISCONNECT, or nothing past
// a Keep Alive of 1 second; MQTT 5's get their DISCONNECT reason, and each
// reason counts both.
func TestDisconnectSeries(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{MaxPacketSize: 1024, MaxConnectSize: 1024})
	type row struct {
		reason string
		code   byte // the MQTT 5 DISCONNECT's reason; 0 for none
		wire   func(version byte) []byte
	}
	rows := []row{
		{"keep_alive_timeout", 0x8D, nil},
		{"protocol_error", 0x82, func(v byte) []byte {
			return harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: v, CleanStart: true, ClientID: "again"}, v)
		}},
		{"malformed_packet", 0x81, func(byte) []byte { return []byte{0xC0, 0x01, 0x00} }},
		{"packet_too_large", 0x95, func(byte) []byte { return harness.AppendVarInt([]byte{0x30}, 2048) }},
		{"client", 0, func(v byte) []byte { return harness.Encoded(t, &harness.Disconnect{}, v) }},
	}
	series := make([]string, len(rows))
	for i, r := range rows {
		series[i] = disconnectSeries(r.reason)
	}
	base := support.Sampled(t, b, series...)
	type ending struct {
		c       *harness.Client
		version byte
		code    byte
	}
	endings := make([]ending, 0, 2*len(rows))
	for _, r := range rows {
		for _, version := range []byte{harness.V5, harness.V311} {
			p := &harness.Connect{ClientID: clientID(t, "-"+r.reason+"-"+harness.VersionName(version)), CleanStart: true}
			if r.wire == nil {
				p.KeepAlive = 1
			}
			c, _ := harness.Connected(t, b.Addr, version, p)
			if r.wire != nil {
				harness.SendBytes(t, c, r.wire(version))
			}
			endings = append(endings, ending{c, version, r.code})
		}
	}
	for _, e := range endings {
		if e.version == harness.V5 && e.code != 0 {
			if d := harness.Expect[*harness.Disconnect](t, e.c); d.Reason != e.code {
				t.Errorf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, e.code)
			}
		}
		harness.ExpectClosed(t, e.c)
	}
	want := make(map[string]float64, len(series))
	for _, s := range series {
		want[s] = base[s] + 2
	}
	awaitSeries(t, b, want)
}

// endedSeries is saguin_sessions_ended_total of cause.
func endedSeries(cause string) string {
	return `saguin_sessions_ended_total{cause="` + cause + `"}`
}

// goneExpiry is the session expiry, in seconds, of TestSessionSeries'
// client that leaves just before the stop: it runs out while the broker is
// down, or, if the stop takes longer, before it; either way its session is
// not kept.
const goneExpiry = 3

// The series TestSessionSeries reads.
const (
	expiryCapped = "saguin_session_expiry_capped_total{}"
	restored     = "saguin_sessions_restored_total{}"
)

// metrics.md, Connections and sessions: saguin_session_expiry_capped_total,
// "CONNECTs given max_session_expiry instead of what they asked";
// saguin_sessions_ended_total{cause}, "clean_start, expired, connection_end
// (expiry 0)"; saguin_sessions_restored_total, "durable sessions kept
// across the start, after those expired ended". config.md,
// max_session_expiry: "caps session expiry, and is MQTT 3.1.1 Clean
// Session 0's". design.md, CONNECT, 8: the CONNACK carries "every value it
// capped or chose", Session Expiry Interval among them; 6: "Clean start
// ends an existing session"; DISCONNECT: "A granted Session Expiry of
// 4,294,967,295 means never expires [...] A smaller configured cap
// replaces it with that finite duration"; Restart: "Sessions that expired
// while the broker was down end at start"; storage.md, sessions: expiry,
// "the session expiry granted, in seconds".
//
// With max_session_expiry at an hour: MQTT 5 CONNECTs asking 600 and 3,600
// seconds are not counted; one asking 3,601 and one asking never are, each
// granted 3,600 on its CONNACK and stored with it; MQTT 3.1.1 Clean Session
// 0 asks nothing and is not counted. A clean start over one of those
// sessions ends it (clean_start); a client with no session expiry ends its
// session with its DISCONNECT (connection_end); a session of 1 second
// expires (expired). After a graceful stop, and once the session of a
// client that left just before it has expired (goneExpiry), the next start
// keeps the five durable sessions.
func TestSessionSeries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := &harness.Options{Dir: dir, MaxSessionExpiry: time.Hour}
	b := support.OpsLaunch(t, o)
	causes := []string{endedSeries("clean_start"), endedSeries("expired"), endedSeries("connection_end")}
	awaitSeries(t, b, map[string]float64{expiryCapped: 0, causes[0]: 0, causes[1]: 0, causes[2]: 0})
	for _, tc := range []struct {
		client  string
		version byte
		expiry  uint32 // the Session Expiry Interval asked, MQTT 5
		want    float64
		granted uint32 // the CONNACK's Session Expiry Interval; 0 for none checked
	}{
		{"cap-a", harness.V5, 600, 0, 0},
		{"cap-b", harness.V5, 3600, 0, 0},
		{"cap-c", harness.V5, 3601, 1, 3600},
		{"cap-d", harness.V311, 0, 1, 0},
		{"cap-e", harness.V5, math.MaxUint32, 2, 3600},
	} {
		p := &harness.Connect{ClientID: tc.client}
		if tc.version == harness.V5 {
			p.Props = support.SessionExpiry(tc.expiry)
		}
		_, ack := harness.Connected(t, b.Addr, tc.version, p)
		if tc.granted != 0 {
			if got, ok := ack.Props.Get(harness.SessionExpiryInterval); !ok || got.Int != tc.granted {
				t.Errorf("%s: CONNACK Session Expiry Interval %v (%v), want %d", tc.client, got.Int, ok, tc.granted)
			}
			var stored uint32
			support.Query(t, support.OpenDB(t, b.DB), `SELECT expiry FROM sessions WHERE client = ?`, func(rows *sql.Rows) error {
				return rows.Scan(&stored)
			}, tc.client)
			if stored != tc.granted {
				t.Errorf("%s: stored expiry %d, want %d", tc.client, stored, tc.granted)
			}
		}
		awaitSeries(t, b, map[string]float64{expiryCapped: tc.want})
	}

	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "cap-b", CleanStart: true, Props: support.SessionExpiry(3600)})
	zero, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "zero", CleanStart: true})
	brief, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "brief", CleanStart: true, Props: support.SessionExpiry(1)})
	for _, c := range []*harness.Client{zero, brief} {
		if err := c.Send(&harness.Disconnect{}); err != nil {
			t.Fatal(err)
		}
		harness.ExpectClosed(t, c)
	}
	awaitSeries(t, b, map[string]float64{causes[0]: 1, causes[1]: 1, causes[2]: 1})

	gone, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "gone", CleanStart: true, Props: support.SessionExpiry(goneExpiry)})
	if err := gone.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, gone)
	left := time.Now()
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	time.Sleep(time.Until(left.Add(goneExpiry*time.Second + tick))) // gone's expiry, from its departure
	again := *o
	b = support.OpsLaunch(t, &again)
	awaitSeries(t, b, map[string]float64{restored: 5})
}

// subscriptionsRefused is saguin_subscriptions_refused_total of reason.
func subscriptionsRefused(reason string) string {
	return `saguin_subscriptions_refused_total{reason="` + reason + `"}`
}

// metrics.md, Connections and sessions: saguin_subscriptions_refused_total,
// "refused filters, one per filter: not_authorized, topic_filter_invalid,
// quota_exceeded, implementation_specific_error, or unspecified_error for
// what only MQTT 3.1.1 is refused (a worker, $share)". design.md, Flows,
// SUBSCRIBE, Refusals: "0x8F for a filter [...] over
// limits.max_topic_length"; "0x97 for a filter past
// limits.max_subscriptions"; "For an unknown saguin- property, it uses
// 0x83 on every otherwise eligible filter"; "Filters are processed in
// packet order"; the subscription table: $share, "MQTT 3.1.1: refused
// (0x80)"; Operations, HTTP: "A subscription to $SYS/… is refused with
// 0x87".
//
// With max_topic_length 64 and max_subscriptions 2, an MQTT 5 client
// subscribes to $SYS/x, a filter of 65 bytes, a/1, a/2 and a/3 (0x87,
// 0x8F, granted, granted, 0x97); another to b/1 with an unknown saguin-
// property (0x83); an MQTT 3.1.1 client to $share/g/x (0x80). Each reason
// counts one.
func TestSubscriptionRefusalSeries(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxTopicLength: 64, MaxSubscriptions: 2})
	long := "a/" + strings.Repeat("x", 63)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "refused-a", CleanStart: true})
	support.Suback(t, c, nil, support.At(0, "$SYS/x", long, "a/1", "a/2", "a/3"), 0x87, 0x8F, 0, 0, 0x97)
	c, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "refused-b", CleanStart: true})
	support.Suback(t, c, harness.Properties{harness.User("saguin-unknown", "x")}, support.At(0, "b/1"), 0x83)
	c, _ = harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "refused-c", CleanStart: true})
	support.Suback(t, c, nil, support.At(0, "$share/g/x"), 0x80)
	awaitSeries(t, b, map[string]float64{
		subscriptionsRefused("not_authorized"):                1,
		subscriptionsRefused("topic_filter_invalid"):          1,
		subscriptionsRefused("quota_exceeded"):                1,
		subscriptionsRefused("implementation_specific_error"): 1,
		subscriptionsRefused("unspecified_error"):             1,
	})
}

// The Will series TestWillSeries reads.
func willsPublished(cause string) string {
	return `saguin_wills_published_total{cause="` + cause + `"}`
}

func willsDropped(cause string) string {
	return `saguin_wills_dropped_total{cause="` + cause + `"}`
}

const willsCancelled = "saguin_wills_cancelled_total{}"

// metrics.md, Connections and sessions: saguin_wills_published_total,
// "disconnect (0x04), lost (after its Will Delay, if any [...]), takeover,
// session_end, restart: one cause per Will, restart for one published after
// a restart [...], otherwise the event that made it due";
// saguin_wills_cancelled_total, "This counts Wills that a reconnect cancelled
// in time";
// saguin_wills_dropped_total, "channel_full, retained_full (the retained
// values' limit)". design.md, DISCONNECT: "The reason 0x04 keeps it for
// publication"; "The Will goes after its Will Delay [...] A reconnect in
// time cancels it. If the session ends first, the Will goes early. The
// Will is dropped and counted if [...] its channel or the retained values'
// limit is full"; CONNECT, 5: an old connection's "Will is published if it
// has no delay"; 0x97, "a retained Will on $broadcast that alone exceeds
// retained.max_bytes" (so one that does not is accepted); Restart: "A Will
// whose client was connected at the stop is due its Will Delay Interval
// after the start".
//
// The broker has an append channel, small, of max_bytes testkit.RecordBytes, and
// retained.max_bytes 2 × testkit.RecordBytes. Wills with no delay go by a
// DISCONNECT 0x04 (disconnect), a dropped connection (lost) and a takeover
// (takeover); one with an hour's delay goes when its 1-second session ends
// (session_end); one with an hour's delay, its session an hour, is
// cancelled by its client's return. A Will of 2 × testkit.RecordBytes into small
// is dropped (channel_full); a retained Will of testkit.RecordBytes, after a
// retained value of 1.5 × testkit.RecordBytes, is dropped (retained_full). Then a
// client with a Will of 1 second's delay is connected at a graceful stop;
// after the start its Will goes (restart).
func TestWillSeries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := &harness.Options{Dir: dir, RetainedMaxBytes: 2 * testkit.RecordBytes,
		Channels: []harness.Channel{{Name: "small", Type: "append", Filter: "small/#", MaxBytes: testkit.RecordBytes}}}
	b := support.OpsLaunch(t, o)
	withWill := func(id string, w *harness.Will, props harness.Properties) *harness.Client {
		t.Helper()
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: props, Will: w})
		return c
	}
	plain := func(topic string) *harness.Will { return &harness.Will{Topic: topic, Payload: support.WillPayload} }

	c := withWill("will-disconnect", plain("will/disconnect"), nil)
	if err := c.Send(&harness.Disconnect{Reason: 0x04}); err != nil {
		t.Fatal(err)
	}
	support.DropSocket(t, withWill("will-lost", plain("will/lost"), nil))
	taken := withWill("will-takeover", plain("will/takeover"), nil)
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "will-takeover", CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, taken); d.Reason != 0x8E {
		t.Fatalf("the client taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	delayed := plain("will/session-end")
	delayed.Props = support.WillDelay(support.Hour)
	support.DropSocket(t, withWill("will-session-end", delayed, support.SessionExpiry(1)))

	cancelled := plain("will/cancelled")
	cancelled.Props = support.WillDelay(support.Hour)
	support.DropSocket(t, withWill("will-cancelled", cancelled, support.SessionExpiry(support.Hour)))
	support.AwaitWillDue(t, support.OpenDB(t, b.DB), "will-cancelled")
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "will-cancelled", Props: support.SessionExpiry(support.Hour)})

	support.DropSocket(t, withWill("will-channel-full", sizedWill("small/will", 2*testkit.RecordBytes), nil))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "will-retainer", CleanStart: true})
	value := support.Sized("retained/a", 3*testkit.RecordBytes/2)
	value.Retain = true
	support.Acknowledged(t, pub, value)
	retained := sizedWill("retained/b", testkit.RecordBytes)
	retained.Retain = true
	support.DropSocket(t, withWill("will-retained-full", retained, nil))

	awaitSeries(t, b, map[string]float64{
		willsPublished("disconnect"):  1,
		willsPublished("lost"):        1,
		willsPublished("takeover"):    1,
		willsPublished("session_end"): 1,
		willsPublished("restart"):     0,
		willsCancelled:                1,
		willsDropped("channel_full"):  1,
		willsDropped("retained_full"): 1,
	})

	atStop := plain("will/restart")
	atStop.Props = support.WillDelay(1)
	withWill("will-restart", atStop, support.SessionExpiry(support.Hour))
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
	again := *o
	b = support.OpsLaunch(t, &again)
	awaitSeries(t, b, map[string]float64{willsPublished("restart"): 1})
}
