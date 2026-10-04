package broker

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

func metricsBroker(t *testing.T) *Broker {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append, Storage: "local"},
		{Name: "state", Type: channel.Latest, Storage: "local"},
		{Name: "jobs", Type: channel.Queue, Storage: "local",
			VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// The bounds a publish is held to. Left at their zero values every
	// topic is longer than max_topic_length and the publish path refuses
	// the lot, which reads as a broken test rather than as a missing
	// setting.
	topics, headers, conns := 1024, 32, int64(10000)
	var lim config.Limits
	lim.MaxTopicLength, lim.MaxHeaderCount, lim.MaxConnections = &topics, &headers, &conns
	lim.MaxMessageSize, lim.MaxHeaderBytes = "1MiB", "8KiB"
	b.limits = lim.Resolve()
	b.SetIdentity("edge-1", "0.1.0-test")
	b.SetProviderKinds(map[string]string{"local": "memory"})
	return b
}

// scrapeOnce reads /metrics through the handler, as a scraper would.
func scrapeOnce(t *testing.T, b *Broker, minScrape time.Duration, header http.Header) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	for k, v := range header {
		r.Header[k] = v
	}
	w := httptest.NewRecorder()
	b.metricsHandler(minScrape).ServeHTTP(w, r)
	return w.Result()
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var r io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("gzip: %v", err)
		}
		r = zr
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(out)
}

// RFC 0005 "The catalogue"
//
// A scrape names the broker, its channels and its storage, and every name
// in it is one the catalogue promises. The point of asserting on the text
// rather than on a struct is that the text is the interface: a scraper
// reads these bytes, and a HELP or TYPE line missing makes the whole page
// unparseable rather than one metric absent.
func TestAScrapeCarriesTheCatalogue(t *testing.T) {
	b := metricsBroker(t)
	if _, err := b.logs["events"].Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got := body(t, scrapeOnce(t, b, time.Minute, nil))

	for _, want := range []string{
		`saguin_build_info{version="0.1.0-test",broker_id="edge-1"} 1`,
		`saguin_channel_info{channel="events",filter="events/#",type="append",provider="local"} 1`,
		`saguin_channel_info{channel="state",filter="state/#",type="latest",provider="local"} 1`,
		`saguin_channel_info{channel="jobs",filter="jobs/#",type="queue",provider="local"} 1`,
		`saguin_channel_next_offset{channel="events"} 2`,
		`saguin_channel_floor_offset{channel="events"} 1`,
		`saguin_channel_records{channel="events"} 1`,
		`saguin_provider_info{provider="local",type="memory"} 1`,
		"# TYPE saguin_uptime_seconds gauge",
		"# HELP saguin_connections",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the scrape does not carry %q\n%s", want, got)
		}
	}

	// Every name is declared exactly once. A repeated HELP or TYPE line is
	// a page a scraper rejects whole, so it takes out every metric rather
	// than the one that was written twice.
	seen := map[string]int{}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			seen[strings.Fields(line)[2]]++
		}
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("%s is declared %d times; a scraper rejects the whole page", name, n)
		}
	}
	if len(seen) == 0 {
		t.Fatal("the scrape declared no metrics at all: this asserted nothing")
	}
}

// RFC 0005 "What a metric is allowed to cost"
//
// **What a latest channel holds, and only where holding it is free.** Such
// a channel keeps one value per topic, so its topics are its records - and
// `next − floor` cannot say so, because a replacement takes a new offset
// and the subtraction would count writes instead. The memory store is
// keeping those values in a map and answers from its size.
//
// The pair below is one test in two halves on purpose: the number arrives
// from either provider, and it is the same number the store's own rows
// come to. The second half is what keeps a kept count honest - a counter
// advanced on the write path is a second copy of the truth, and the only
// way to know it has not drifted is to ask the rows it claims to be about.
func TestALatestChannelReportsItsHoldingsWhereTheStoreKnowsThem(t *testing.T) {
	b := metricsBroker(t)
	for _, topic := range []string{"state/a", "state/b", "state/c"} {
		if _, err := b.latest["state"].Set(store.Record{MessageID: "m", Topic: topic}); err != nil {
			t.Fatalf("set %s: %v", topic, err)
		}
	}
	// Twice to one topic that already has a value. The count must not move:
	// this is a replacement, and it is the case next − floor would have got
	// wrong - offsets would be at five with three values held.
	for range 2 {
		if _, err := b.latest["state"].Set(store.Record{MessageID: "m", Topic: "state/a"}); err != nil {
			t.Fatalf("replace: %v", err)
		}
	}

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	if want := `saguin_channel_records{channel="state"} 3`; !strings.Contains(got, want) {
		t.Errorf("the scrape does not carry %q, so a latest channel still reports "+
			"nothing about what it holds\n%s", want, got)
	}
	// The reading this replaces. Five writes, three values: a scrape saying
	// 5 would be the subtraction nobody should be doing here.
	if bad := `saguin_channel_records{channel="state"} 5`; strings.Contains(got, bad) {
		t.Errorf("the scrape carries %q, which counts writes rather than holdings", bad)
	}
}

func TestALatestChannelOnASqliteProviderReportsWhatItHolds(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "saguin.db"), "metrics-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	b := metricsBroker(t)
	b.SetStores(Stores{Latest: map[string]LatestStore{"state": lt}})
	for _, topic := range []string{"state/a", "state/b", "state/c"} {
		if _, err := b.latest["state"].Set(store.Record{MessageID: "m", Topic: topic}); err != nil {
			t.Fatalf("set %s: %v", topic, err)
		}
	}

	// Replacing a value must not move it here either, and this is the
	// provider where the count is kept rather than looked at.
	if _, err := b.latest["state"].Set(store.Record{MessageID: "m", Topic: "state/a"}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	// The counter this shape owes: the durable store is really the one
	// behind the channel, so what the scrape says below is about sqlite.
	if _, ok := storeBehind(b.latest["state"]).(*sqlite.Latest); !ok {
		t.Fatalf("the channel is not backed by the sqlite store: %T",
			storeBehind(b.latest["state"]))
	}

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	if want := `saguin_channel_records{channel="state"} 3`; !strings.Contains(got, want) {
		t.Errorf("the scrape does not carry %q, so a latest channel on a durable "+
			"provider still reports nothing about what it holds\n%s", want, got)
	}
	if bad := `saguin_channel_records{channel="state"} 4`; strings.Contains(got, bad) {
		t.Errorf("the scrape carries %q: the replacement was counted as a new topic", bad)
	}

	// **That the kept count matches the rows it describes is the sqlite
	// package's own check**, where countRows is reachable without
	// exporting anything for a test. This one is about the scrape.
	if want := `saguin_channel_info{channel="state",filter="state/#",type="latest",provider="local"} 1`; !strings.Contains(got, want) {
		t.Fatalf("the scrape does not carry %q, so nothing here was measured\n%s", want, got)
	}
}

// RFC 0005 "What the box moved"
//
// **The three numbers the substrate derives rather than counts**, and the
// reason they are worth a test of their own: each is read from somewhere
// different, and a name wired to the wrong somewhere reports a plausible
// number for ever.
//
// saguin_retained_messages especially. The substrate has a field of that
// name and it is wrong here - it counts a store saguin empties as fast as
// it fills - so this asserts against saguin's own store, which is a latest
// channel and answers from the count every latest channel now keeps.
func TestTheNumbersDerivedRatherThanCounted(t *testing.T) {
	b := metricsBroker(t)

	retained := store.NewLatest()
	retained.SetQuota(store.NewQuota(0, 0))
	b.SetRetained("local", retained, 0)
	for _, topic := range []string{"weather/roof", "weather/yard", "door/front"} {
		if _, err := retained.Set(store.Record{MessageID: "m", Topic: topic,
			Payload: []byte("v")}); err != nil {
			t.Fatalf("retain %s: %v", topic, err)
		}
	}

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	if want := "saguin_retained_messages 3"; !strings.Contains(got, want) {
		t.Errorf("the scrape does not carry %q. Nothing else in the catalogue says how "+
			"many retained messages this broker holds\n%s", want, got)
	}
	// The substrate's own field for this is zero on every saguin, because
	// the tree that maintained it is silenced. A 0 here is that field
	// being read by mistake.
	if bad := "saguin_retained_messages 0"; strings.Contains(got, bad) {
		t.Errorf("the scrape carries %q with three messages retained: this is reading "+
			"the substrate's count rather than saguin's own store", bad)
	}

	for _, name := range []string{
		"saguin_deliveries_refused_total", "saguin_sessions_offline",
	} {
		if !strings.Contains(got, "# TYPE "+name) {
			t.Errorf("the scrape does not declare %s", name)
		}
	}
}

// **A broker with no retained store carries no count**, rather than a zero
// that would read as a store which happens to be empty. Such a broker
// refuses a retained publish outright, so nothing could ever be held.
func TestABrokerWithNoRetainedStoreReportsNoCountForIt(t *testing.T) {
	got := body(t, scrapeOnce(t, metricsBroker(t), time.Minute, nil))
	if !strings.Contains(got, "# TYPE saguin_retained_messages gauge") {
		t.Fatal("the name is not declared, so a dashboard reading the catalogue would " +
			"not know it exists")
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "saguin_retained_messages ") {
			t.Errorf("the scrape carries %q on a broker with no retained store: a zero "+
				"there reads as a store that is empty rather than one that is absent", line)
		}
	}
}

// RFC 0005 "The observer does not set the cost"
//
// A scrape arriving sooner than min_scrape_interval is answered from the
// previous one, and **answered rather than refused**: Prometheus records a
// refused scrape as a failed one, which is a gap in the graph and usually
// an alert, so a defence that becomes the operator's incident is not one.
func TestAnEarlyScrapeIsAnsweredFromThePreviousOne(t *testing.T) {
	b := metricsBroker(t)
	h := b.metricsHandler(time.Hour)

	read := func() string {
		r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("a scrape inside the interval answered %d, want 200: a refused "+
				"scrape is a failed one to every scraper in common use", resp.StatusCode)
		}
		return body(t, resp)
	}

	first := read()
	// Something that would change the numbers if they were recomputed.
	if _, err := b.logs["events"].Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if second := read(); second != first {
		t.Error("a scrape inside min_scrape_interval was recomputed: the cost of being " +
			"observed is then set by whoever configures the scraper, and by how many of them")
	}

	// And a scrape past the interval does see the change.
	if fresh := body(t, scrapeOnce(t, b, time.Nanosecond, nil)); fresh == first {
		t.Error("a scrape past the interval was answered from the cache, so the numbers never move")
	}
}

// RFC 0005 "What it costs, measured": gzip when the client asks, which
// Prometheus does by default.
func TestAScrapeIsCompressedWhenTheScraperAsks(t *testing.T) {
	b := metricsBroker(t)
	h := http.Header{"Accept-Encoding": []string{"gzip"}}
	resp := scrapeOnce(t, b, time.Minute, h)
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding is %q, want gzip", got)
	}
	// Readable as gzip, which is the half a header alone does not prove.
	if !strings.Contains(body(t, resp), "saguin_build_info") {
		t.Error("the compressed body does not decompress to the catalogue")
	}

	if got := scrapeOnce(t, b, time.Minute, nil).Header.Get("Content-Encoding"); got != "" {
		t.Errorf("a scraper that did not ask for gzip was sent %q", got)
	}
}

// RFC 0005 "And the scrape does not hold the publish path"
//
// **This test is worth more than the code it guards.** The health probe
// sits on the same listener, so a scrape that formatted its output under
// the broker's lock would stall every publish for as long as it took and
// fail that probe - and the flapping would be telling the truth.
//
// It is asserted by holding the lock and scraping: the collect half must
// block, and once the lock is released the formatting must finish without
// it. Holding the lock for the whole scrape would pass a test that only
// checked the answer.
func TestAScrapeDoesNotFormatUnderTheBrokersLock(t *testing.T) {
	b := metricsBroker(t)

	// While the lock is held, no scrape can complete: the collect half
	// needs it. This is the control - without it the test below cannot
	// tell "released the lock" from "never took it".
	b.mu.Lock()
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		_ = b.catalogue()
	}()
	select {
	case <-blocked:
		b.mu.Unlock()
		t.Fatal("a scrape completed while the broker's lock was held, so it read " +
			"nothing under it: the numbers are not being copied under the lock at all")
	case <-time.After(100 * time.Millisecond):
	}
	b.mu.Unlock()

	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the scrape did not finish once the lock was free")
	}

	// And the lock is free for the whole of the formatting: a scrape is held
	// between its collect and its formatting (afterCollect), the lock is
	// taken here, and only then is the formatting let go. Formatting that
	// asked for the lock could not finish while it is held.
	entered, release := make(chan struct{}), make(chan struct{})
	seam := func(scraped *Broker) {
		if scraped == b {
			close(entered)
			<-release
		}
	}
	afterCollect.Store(&seam)
	t.Cleanup(func() { afterCollect.Store(nil) })
	formatted := make(chan []byte, 1)
	go func() { formatted <- b.catalogue() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scrape never reached the end of its collect, so nothing below is about the formatting")
	}
	afterCollect.Store(nil) // one scrape is held; any other runs through
	b.mu.Lock()
	close(release)
	select {
	case out := <-formatted:
		b.mu.Unlock()
		// The whole catalogue, so what finished was the formatting rather
		// than a scrape that gave up.
		if !strings.Contains(string(out), "saguin_build_info") {
			t.Fatalf("the held scrape finished with %d bytes and no saguin_build_info", len(out))
		}
	case <-time.After(5 * time.Second):
		b.mu.Unlock()
		<-formatted
		t.Fatal("the scrape could not format while the broker's lock was held: the formatting takes it")
	}
}

// RFC 0005 "The operations listener", and the methods each takes.
//
// **It was named for two paths and no third**, which stopped being true when
// the `/v1/operations` routes were added and stayed in the name - the fourth
// place that sentence was written, after the shipped configuration file and
// two comments. A test named after a claim the code has stopped making is
// believed by everyone who reads it next, and nothing catches it, because a
// name is not an assertion.
func TestTheOperationsListenerAnswersItsOwnRoutesAndNothingElse(t *testing.T) {
	h := metricsBroker(t).operationsHandler(time.Second, time.Minute, nil)

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodHead, "/health", http.StatusOK},
		{http.MethodPost, "/health", http.StatusMethodNotAllowed},
		{http.MethodGet, "/metrics", http.StatusOK},
		{http.MethodHead, "/metrics", http.StatusOK},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
		{http.MethodGet, "/v1/operations/consumers", http.StatusOK},
		// Nothing at the top level, and no user interface. The configuration
		// is a route, but it is `/v1/operations/config` - saguin's own, and
		// versioned because it will grow - and `/config` is not a route at
		// all.
		{http.MethodGet, "/", http.StatusNotFound},
		{http.MethodGet, "/config", http.StatusNotFound},
		{http.MethodGet, "/channels", http.StatusNotFound},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(tc.method, tc.path, nil)
		// **An address, because this handler has no credential configured**,
		// and such a listener answers to an address and not to a name.
		// httptest fills in a name of its own, which is the shape a
		// re-resolved domain arrives as rather than the shape a caller on
		// loopback sends.
		r.Host = "127.0.0.1:9090"
		// The loopback port, which is what a caller sending that address
		// arrived at.
		r = r.WithContext(context.WithValue(r.Context(), gatedDoorKey{}, false))
		r = r.WithContext(context.WithValue(r.Context(), doorNameKey{}, "tcp"))
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.method, tc.path, w.Code, tc.want)
		}
	}
}

// boundedLabels is every label the catalogue may carry, and why its values
// cannot be chosen by whoever connects.
//
// **It is written the way round that cannot rot.** A list of known-bad
// names - client id, topic, filter - stops being true the moment somebody
// adds a metric keyed by something nobody thought of, and the test would go
// on passing. This asks the opposite question of every label that actually
// appears: is this one provably bounded, and by what. A label with no entry
// fails until a person decides, which is the only kind of decision that
// survives the next contributor.
//
// The justification is what the value *is*, never which metric uses it: the
// first cannot rot and the second rots the moment somebody adds a metric.
var boundedLabels = map[string]string{
	"channel": "a channel name from the operator's configuration file, or the fixed word broadcast",
	// **A channel's own filter, not a client's.** The rule this list
	// enforces is about who chooses the value: a filter a client sends is
	// unbounded and must never be a label, and this one is written in the
	// configuration file beside the channel name above it - refused at
	// startup when it is longer than limits.max_topic_length leaves room
	// for. It adds no series either: the channel already has exactly one
	// saguin_channel_info, and this rides on it.
	"filter":    "a channel's topic filter, from the operator's file",
	"provider":  "a storage provider's name, from the operator's file",
	"bridge":    "a bridge's name, from the operator's file",
	"upstream":  "a bridge's upstream address, from the operator's file",
	"type":      "a channel type or provider type: a closed set saguin defines",
	"broker_id": "broker.id, from the operator's file",
	"version":   "saguin's own build stamp",
	"reason":    "the specification's name for an MQTT reason code, its number, or rejected",
	"cause":     "why a delivery never reached a session: four words saguin writes, a closed set",
}

// RFC 0005 "Labels, and where the catalogue stops"
//
// **Every label is bounded by something the operator typed, or the metric
// does not ship.** A client chooses its own id, so a metric keyed by one is
// a series count chosen by whoever connects: a fleet that reconnects with a
// fresh id per boot writes an unbounded number of series into the
// operator's monitoring system, which then falls over - some distance from
// saguin and long after the cause. The same is true of a topic, and of a
// filter a *client* sent - which is a different value from the filter a
// channel is configured with, and the reason the entry for that one says
// where it comes from rather than what it is called.
func TestEveryLabelInTheCatalogueIsBounded(t *testing.T) {
	b := metricsBroker(t)
	if _, err := b.logs["events"].Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got := body(t, scrapeOnce(t, b, time.Minute, nil))

	found := 0
	for _, line := range strings.Split(got, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		open := strings.IndexByte(line, '{')
		if open < 0 {
			continue // a sample with no labels at all
		}
		close := strings.LastIndexByte(line, '}')
		if close < open {
			t.Errorf("a sample's labels are not closed, so the page is unparseable: %q", line)
			continue
		}
		for _, pair := range strings.Split(line[open+1:close], ",") {
			name, _, ok := strings.Cut(pair, "=")
			if !ok {
				t.Errorf("a label is not a name=value pair: %q in %q", pair, line)
				continue
			}
			found++
			if _, bounded := boundedLabels[name]; !bounded {
				t.Errorf("the label %q in %q has no entry in boundedLabels.\n"+
					"\tSay what the value is and why nothing a client sends can reach it, "+
					"or the catalogue stops being closed: a label a client chooses is a "+
					"series count a client chooses.", name, line)
			}
		}
	}

	// The counter every check of this shape owes. A scrape that produced no
	// labelled samples would satisfy every assertion above by vacuum, which
	// is a green tick over a rule that never ran.
	if found < 10 {
		t.Fatalf("only %d labels were examined; this checked almost nothing", found)
	}
}

// RFC 0003 "Retained messages": a retained publish to a queue is taken as
// ordinary work with the flag dropped, and counted - the counter being the
// only place it is visible at all, since the publish is acknowledged like
// any other and nothing on the wire says a flag went missing.
//
// Driven through OnPublish rather than by adding to the counter, so a
// counter wired at the wrong site fails here: the question this answers is
// "did the publish path count it", and incrementing it by hand answers
// nothing.
func TestARetainedPublishToAQueueIsTakenAsWorkAndCounted(t *testing.T) {
	b := metricsBroker(t)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: true},
		TopicName:   "jobs/build/1",
		Payload:     []byte("x"),
	}
	out, err := b.OnPublish(&mqtt.Client{ID: "p"}, pk)
	if err != nil && !errors.Is(err, packets.CodeSuccessIgnore) {
		t.Fatalf("a retained publish to a queue was refused: %v", err)
	}
	// Dropped rather than carried on. Left up it reaches the substrate's
	// own retained store on the way past, where a wildcard at channel depth
	// can reach it - invariant 11, and the reason this is asserted on the
	// packet rather than taken on trust from the counter.
	if out.FixedHeader.Retain {
		t.Error("the retain flag survived the publish path on a queue")
	}

	got := body(t, scrapeOnce(t, b, 0, nil))
	if want := `saguin_queue_retain_ignored_total{channel="jobs"} 1`; !strings.Contains(got, want) {
		t.Errorf("the scrape does not carry %q\n%s", want, got)
	}
	// A queue series, so an append channel carrying one would read as a
	// queue nothing has happened on.
	if strings.Contains(got, `saguin_queue_retain_ignored_total{channel="events"}`) {
		t.Error("an append channel carries the queue's retain counter")
	}
}

// RFC 0005 "The catalogue" - the counters
//
// **Each one is asserted to move, and to move for the right reason.** A
// counter wired at the wrong site reads perfectly: it exists, it has a
// HELP line, and it counts something. What it does not do is answer the
// question its name asks, and nothing about the page says so.
//
// This drives the broker's own paths rather than adding to the counters
// directly, so a site that stops being reached fails it.
func TestTheCountersCountWhatTheyName(t *testing.T) {
	b := metricsBroker(t)

	// Three accepted records on one channel.
	for range 3 {
		if _, err := b.logs["events"].Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
			t.Fatalf("append: %v", err)
		}
		b.counted.forChannel("events").published.Add(1)
	}
	// A refusal, by the code saguin answered with.
	b.counted.refuse(refusalReason(packets.ErrQuotaExceeded))
	b.counted.refuse(refusalReason(packets.ErrQuotaExceeded))
	// A queue's life, one of each.
	q := b.counted.forChannel("jobs")
	q.delivered.Add(4)
	q.acknowledged.Add(1)
	q.returned.Add(1)
	q.redelivered.Add(1)
	q.deadLettered.Add(1)
	b.counted.forChannel("events").retentionRemoved.Add(7)
	b.counted.forChannel("events").positionLost.Add(1)
	b.counted.connections.Add(5)
	b.counted.sessionsShortened.Add(2)
	// Two numbers rather than one: a consumer that cannot keep up and a
	// publisher's expiry being carried out are not the same event, and an
	// operator does a different thing about each.
	b.counted.dropped.Add(3)
	b.counted.expired.Add(2)

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	for _, want := range []string{
		"saguin_connections_total 5",
		"saguin_session_expiry_shortened_total 2",
		`saguin_published_total{channel="events"} 3`,
		`saguin_publish_refused_total{reason="quota exceeded"} 2`,
		`saguin_channel_retention_removed_total{channel="events"} 7`,
		`saguin_channel_position_lost_total{channel="events"} 1`,
		`saguin_queue_delivered_total{channel="jobs"} 4`,
		`saguin_queue_acknowledged_total{channel="jobs"} 1`,
		`saguin_queue_returned_total{channel="jobs"} 1`,
		`saguin_queue_redelivered_total{channel="jobs"} 1`,
		`saguin_queue_dead_lettered_total{channel="jobs"} 1`,
		"saguin_deliveries_dropped_total 3",
		"saguin_deliveries_expired_total 2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the scrape does not carry %q\n%s", want, got)
		}
	}

	// The queue series carry queue channels only: a zero against an append
	// channel would read as a queue nothing has happened on, which is a
	// different statement from "not a queue".
	if strings.Contains(got, `saguin_queue_delivered_total{channel="events"}`) {
		t.Error("an append channel carries a queue counter")
	}

	// And a reason nothing has been refused with has no series at all,
	// which is what keeps the label closed rather than merely bounded.
	if strings.Contains(got, `reason="payload format invalid"`) {
		t.Error("a reason code nothing was refused with has a series")
	}
}

// RFC 0005 "The catalogue" - saguin_qos2_abandoned_total
//
// **The one series an abandoned exactly-once publish appears in.** RFC 0005
// says so in as many words: "Nothing else shows them: the publisher is told
// nothing, and a completed exchange is counted where its record lands." A
// publisher that opens an exchange and never finishes it has its message
// dropped, correctly and by design, and this counter is the whole of what
// an operator can see it by.
//
// It read 0 for ever. Both drop paths incremented the live counter and the
// snapshot builder, which copies every other tally into the struct the
// export reads, did not copy this one - so the series was declared, present
// and stuck, which is the shape a catalogue test accepts and a dashboard
// draws as a flat line at nothing.
//
// **Driven through the drop rather than through the counter**, so that the
// whole path is under test: two exchanges are held, the session that owns
// them ends, and the scrape is asked. Incrementing the counter by hand
// would have proved only the half that was broken, and left the half that
// works with no test at all - remove either the `Add` at the drop site or
// the copy in the snapshot and this fails.
func TestAnAbandonedExactlyOncePublishIsExported(t *testing.T) {
	b := metricsBroker(t)
	b.SetQoS2(20, time.Minute)

	// Two rather than one: a count exported as a constant, or as "has
	// anything been dropped at all", would satisfy a 1.
	now := time.Now()
	for _, id := range []uint16{9, 10} {
		if err := b.holdIn("events", store.Exchange{Client: "q2pub", PacketID: id},
			store.Record{MessageID: "m", Topic: "events/q2"}, now); err != nil {
			t.Fatalf("hold %d: %v", id, err)
		}
	}
	b.dropHeldPublishes(&mqtt.Client{ID: "q2pub"}, "its session was discarded at disconnect")

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	if !strings.Contains(got, "saguin_qos2_abandoned_total 2") {
		t.Errorf("the scrape does not carry %q, so exactly-once publishes the "+
			"broker dropped are invisible to an operator - the one place RFC 0005 "+
			"says they can be seen\n%s", "saguin_qos2_abandoned_total 2", got)
	}
}

// The class the test above is one instance of, and the reason it is a sweep
// rather than a second table of names: a counter is added to `counters` and
// exported in two separate places, and nothing but this connects them. The
// one that went missing was incremented at two sites, declared in the
// catalogue, drawn on a dashboard and named in an RFC, and every one of
// those was correct. What was absent was the line that copies it.
//
// So the rule is the narrow one that would have caught it: every tally the
// broker keeps must be read somewhere. A field that is only ever added to
// is a number that never leaves the process.
func TestEveryTallyTheBrokerKeepsIsRead(t *testing.T) {
	fset := token.NewFileSet()
	pkg, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// The fields of `counters` whose type is an atomic tally.
	var fields []string
	for _, f := range pkg["broker"].Files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "counters" {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, fld := range st.Fields.List {
				sel, ok := fld.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Uint64" {
					continue
				}
				if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "atomic" {
					continue
				}
				for _, name := range fld.Names {
					fields = append(fields, name.Name)
				}
			}
			return true
		})
	}

	// **Counted rather than assumed.** A sweep that found no fields would
	// report every one of them read, which is the failure this rule exists
	// to prevent (test rule 12). Seven at the time of writing; the bound is
	// deliberately loose because the number is allowed to grow, and zero is
	// the only answer that means the sweep broke.
	if len(fields) < 5 {
		t.Fatalf("found %d atomic tallies on `counters` (%v). The sweep is not "+
			"reading the struct it thinks it is", len(fields), fields)
	}

	// Every `.Load()` of one of them **on the counters struct itself**,
	// anywhere in the package: the shape is `<anything>.counted.<field>
	// .Load()`.
	//
	// **The receiver is checked and not only the field name**, which it was
	// not when this was written. Keyed on the name alone, a load of a
	// same-named field on any other struct in the package satisfied the
	// rule, so the sweep could report a tally read while the tally went on
	// reaching nobody - a check passing on evidence about something else,
	// which is the shape this repository refuses everywhere. Left as a
	// looseness rather than a finding.
	//
	// **What is deliberately still loose**: "read somewhere" is weaker than
	// "reaches a scrape", so a counter loaded only into a log line passes
	// here. That gap is held by the behavioural test above, and closing it
	// in a syntax walk would be rebuilding that test inside this one. The
	// division is the point: this is the class guard, and the test above is
	// the one that knows what an operator sees.
	loaded := map[string]bool{}
	for _, f := range pkg["broker"].Files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			load, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || load.Sel.Name != "Load" {
				return true
			}
			field, ok := load.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// The field hangs off `counted`, whatever holds that in turn.
			if on, ok := field.X.(*ast.SelectorExpr); !ok || on.Sel.Name != "counted" {
				return true
			}
			loaded[field.Sel.Name] = true
			return true
		})
	}

	// **Counted again, at the other end.** The walk above is now specific
	// enough to match nothing at all if the struct is ever reached by
	// another name, and a sweep that matched nothing would report every
	// tally unread rather than every tally read - noisy rather than silent,
	// but still a sweep reporting on work it did not do. Saying so here
	// names the cause instead of leaving seven identical failures to be
	// read as seven defects.
	if len(loaded) == 0 {
		t.Fatalf("the sweep found no `counted.<field>.Load()` anywhere in the "+
			"package, so it is not reading the struct it thinks it is. The %d "+
			"failures below would be about the sweep rather than the broker",
			len(fields))
	}

	for _, name := range fields {
		if !loaded[name] {
			t.Errorf("`counters.%s` is added to and never read, so whatever it "+
				"counts cannot reach a scrape. A tally the broker keeps and never "+
				"publishes is a number nobody can act on", name)
		}
	}
}

// The counters that are wired into the broker's own paths, driven through
// them rather than added to. A site that stops being reached fails here,
// which is the half the test above cannot see.
func TestPublishesAndRefusalsAreCountedAtTheirSites(t *testing.T) {
	b := metricsBroker(t)

	before := b.counted.forChannel("events").published.Load()
	// The publish funnel, reached the way a client reaches it.
	// CodeSuccessIgnore is how a record that *was* stored reports itself, so
	// it is success here for the same reason it is success in OnPublish.
	if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
		TopicName: "events/order/1", Payload: []byte("x"),
		FixedHeader: packets.FixedHeader{Qos: 1},
	}); err != nil && !errors.Is(err, packets.CodeSuccessIgnore) {
		t.Fatalf("publish: %v", err)
	}
	if got := b.counted.forChannel("events").published.Load(); got != before+1 {
		t.Errorf("an accepted publish did not reach saguin_published_total: %d, want %d",
			got, before+1)
	}

	// A topic in the reserved space is refused, and counted by the code it
	// was answered with rather than by a name this test invented.
	_, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
		TopicName: "$saguin/nothing/here", Payload: []byte("x"),
		FixedHeader: packets.FixedHeader{Qos: 1},
	})
	if err == nil {
		t.Fatal("a publish into the reserved space was accepted")
	}
	if n := len(b.counted.refusals()); n == 0 {
		t.Error("a refused publish did not reach saguin_publish_refused_total")
	}
	for _, r := range b.counted.refusals() {
		if r.reason == "" {
			t.Error("a refusal was counted under an empty reason")
		}
	}

	// **A queue delivery is not a publish.** Every hand-over to a worker
	// re-enters the publish path on its way out, so counting it there
	// reported one job with five attempts as six records accepted -
	// measured against a running broker before this line existed. The
	// inline client is the queue delivery and nothing else; a bridge and a
	// Will are different clients carrying records that really did arrive.
	b.inline = &mqtt.Client{ID: "saguin:inline"}
	was := b.counted.forChannel("jobs").published.Load()
	_, _ = b.OnPublish(b.inline, packets.Packet{
		TopicName: "jobs/work/1", Payload: []byte("x"),
		FixedHeader: packets.FixedHeader{Qos: 1},
	})
	if got := b.counted.forChannel("jobs").published.Load(); got != was {
		t.Errorf("a queue delivery was counted as a record accepted (%d, was %d): "+
			"a job with five attempts then reads as six records published", got, was)
	}
}

// fakeBridge is a bridge as the catalogue sees one.
type fakeBridge struct {
	name, peer       string
	up               bool
	halted           bool
	received         uint64
	reconnects       uint64
	forwarded        uint64
	loopsSkipped     uint64
	peerRefused      uint64
	broadcastRefused uint64
	unmappable       uint64
	liveDropped      uint64
	superseded       uint64
	queueFull        uint64
	noRule           uint64
	inUnmappable     uint64
	neverAccepted    uint64
}

func (f fakeBridge) Name() string                  { return f.name }
func (f fakeBridge) Peer() string                  { return f.peer }
func (f fakeBridge) Connected() bool               { return f.up }
func (f fakeBridge) Stopped() bool                 { return f.halted }
func (f fakeBridge) Received() uint64              { return f.received }
func (f fakeBridge) Reconnects() uint64            { return f.reconnects }
func (f fakeBridge) Forwarded() uint64             { return f.forwarded }
func (f fakeBridge) LoopsSkipped() uint64          { return f.loopsSkipped }
func (f fakeBridge) PeerRefused() uint64           { return f.peerRefused }
func (f fakeBridge) BroadcastRefused() uint64      { return f.broadcastRefused }
func (f fakeBridge) Unmappable() uint64            { return f.unmappable }
func (f fakeBridge) LiveDropped() uint64           { return f.liveDropped }
func (f fakeBridge) Superseded() uint64            { return f.superseded }
func (f fakeBridge) UnstoredQueueFull() uint64     { return f.queueFull }
func (f fakeBridge) UnstoredNoRule() uint64        { return f.noRule }
func (f fakeBridge) UnstoredUnmappable() uint64    { return f.inUnmappable }
func (f fakeBridge) UnstoredNeverAccepted() uint64 { return f.neverAccepted }

// RFC 0005 "The catalogue" - bridges
//
// **A bridge that is down is a metric and never a failed health probe.** An
// inbound bridge whose far end is unreachable is a real thing an operator
// wants to know and the worst possible thing to fail a liveness probe on:
// the broker would be restarted, repeatedly, because somebody else's broker
// is down - and the restart cannot fix it.
func TestABridgeIsReportedAndNeverFailsTheProbe(t *testing.T) {
	b := metricsBroker(t)
	b.SetBridges([]BridgeStats{
		fakeBridge{name: "head-office", peer: "tls://mqtt.example.com:8883",
			up: true, received: 4812, reconnects: 3, forwarded: 991,
			peerRefused: 2, broadcastRefused: 3, unmappable: 1, liveDropped: 5, superseded: 40,
			queueFull: 6, noRule: 7, inUnmappable: 8, neverAccepted: 9},
		// **Connected, sending nothing, and skipping everything.** On
		// `sent_total` alone this is identical to a bridge with nothing to
		// send, and the two want opposite actions - so the skip counter is
		// asserted here rather than only where it is zero.
		fakeBridge{name: "ring", peer: "tcp://10.0.0.2:1883", up: true, loopsSkipped: 77},
		fakeBridge{name: "vessel-07", peer: "tcp://10.0.0.9:1883"},
		// One that stopped itself rather than merely losing its link: the
		// peer holds a record larger than this broker's max_message_size,
		// which no reconnection can help.
		fakeBridge{name: "dr-link", peer: "tcp://10.0.0.5:1883", halted: true},
	})

	got := body(t, scrapeOnce(t, b, time.Minute, nil))
	for _, want := range []string{
		`saguin_bridge_info{bridge="head-office",peer="tls://mqtt.example.com:8883"} 1`,
		`saguin_bridge_connected{bridge="head-office"} 1`,
		`saguin_bridge_sent_total{bridge="head-office"} 991`,
		// A bridge sending nothing because the guard is skipping everything.
		// Zero sent and seventy-seven skipped is a topology somebody built -
		// two brokers pointed at each other - and it reads as an idle link
		// on the first counter alone.
		`saguin_bridge_sent_total{bridge="ring"} 0`,
		`saguin_bridge_loops_skipped_total{bridge="ring"} 77`,
		`saguin_bridge_loops_skipped_total{bridge="head-office"} 0`,
		`saguin_bridge_received_total{bridge="head-office"} 4812`,
		`saguin_bridge_reconnects_total{bridge="head-office"} 3`,
		// What the outbound half did not send, each cause its own series,
		// because each asks an operator for something different.
		`saguin_bridge_unsent_total{bridge="head-office",cause="peer_refused"} 2`,
		`saguin_bridge_unsent_total{bridge="head-office",cause="broadcast_refused"} 3`,
		`saguin_bridge_unsent_total{bridge="head-office",cause="unmappable"} 1`,
		`saguin_bridge_unsent_total{bridge="head-office",cause="live_queue_full"} 5`,
		`saguin_bridge_unsent_total{bridge="head-office",cause="superseded"} 40`,
		`saguin_bridge_unsent_total{bridge="vessel-07",cause="superseded"} 0`,
		// And what the inbound half did not store, each cause its own
		// series, served at zero from the start.
		`saguin_bridge_unstored_total{bridge="head-office",cause="no_rule"} 7`,
		`saguin_bridge_unstored_total{bridge="head-office",cause="unmappable"} 8`,
		`saguin_bridge_unstored_total{bridge="head-office",cause="never_accepted"} 9`,
		`saguin_bridge_unstored_total{bridge="head-office",cause="queue_full"} 6`,
		`saguin_bridge_unstored_total{bridge="vessel-07",cause="no_rule"} 0`,
		`saguin_bridge_unstored_total{bridge="vessel-07",cause="unmappable"} 0`,
		`saguin_bridge_unstored_total{bridge="vessel-07",cause="never_accepted"} 0`,
		`saguin_bridge_unstored_total{bridge="vessel-07",cause="queue_full"} 0`,
		// The one that is down says so, rather than being absent - a series
		// that disappears reads as a bridge nobody configured.
		`saguin_bridge_connected{bridge="vessel-07"} 0`,
		// **Stopped is not the same as down, and the pair says which.** A
		// link that is down may come back on its own; one that stopped will
		// not, and an operator watching only `connected` could not tell.
		`saguin_bridge_stopped{bridge="dr-link"} 1`,
		`saguin_bridge_stopped{bridge="head-office"} 0`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the scrape does not carry %q\n%s", want, got)
		}
	}

	// And the probe on the same listener is untroubled by it.
	h := b.operationsHandler(time.Second, time.Minute, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Errorf("/health answered %d with a bridge down; a restart cannot fix "+
			"somebody else's broker", w.Code)
	}
}

// failingLog is a store whose reads and writes fail in a chosen way.
type failingLog struct {
	LogStore
	err error
}

func (f failingLog) Append(r store.Record) (store.Record, error) {
	return store.Record{}, f.err
}

// RFC 0005 - saguin_storage_errors_total
//
// **Counted where every store call passes, not at the sites that log a
// failure.** The broker makes over a hundred store calls that can fail,
// and the next one would be missed by any counter wired site by site -
// silently, in the metric an operator watches to find out whether their
// disk is failing.
//
// The two errors that are not storage failures are the point of the test.
// A channel at its size bound and a read below the retention floor are both
// outcomes saguin promises: counting them would make a broker doing exactly
// what it said it would do look like one with a failing disk.
func TestOnlyRealStorageFailuresAreCounted(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		count bool
	}{
		{"a disk that failed", errors.New("input/output error"), true},
		{"a channel at its size bound", store.ErrFull, false},
		{"a read below the retention floor", store.ErrBelowFloor, false},
		{"a bound reached through a wrapped error", fmt.Errorf("channel %q: %w", "events", store.ErrFull), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := metricsBroker(t)
			b.SetStores(Stores{Logs: map[string]LogStore{
				"events": failingLog{LogStore: b.logs["events"], err: tc.err},
			}})

			if _, err := b.logs["events"].Append(store.Record{Topic: "events/x"}); err == nil {
				t.Fatal("the store did not fail")
			}
			got := b.counted.storageFailures()
			switch {
			case tc.count && len(got) == 0:
				t.Error("a storage failure was not counted: the operator's only " +
					"warning that a disk is going is a log line nobody is watching")
			case !tc.count && len(got) != 0:
				t.Errorf("%v was counted as a storage failure; it is an outcome saguin "+
					"promises, and counting it makes a working broker look like a failing disk",
					tc.err)
			case tc.count:
				if got[0].provider != "local" {
					t.Errorf("counted against provider %q, want the one the channel names", got[0].provider)
				}
			}
		})
	}
}

// **A counted store is still a store**, and this is the class rather than
// the one that broke.
//
// The wrappers that count storage failures stand in front of a store, so a
// type assertion asking what the store itself can do sees the wrapper and
// answers no. Two things ask. The dead-letter move asks whether the channel
// can be written inside the queue's own transaction, and its no left the
// record in the queue for ever - caught by the end-to-end suite. The
// snapshot asks whether a store can export itself, and its no is the
// ordinary answer for a sqlite channel, so a wrapped memory store would
// have been dropped from every snapshot in silence. SetStores wraps every
// channel store whatever its provider, so both questions reach a wrapper.
func TestWrappingAStoreDoesNotHideWhatItCanDo(t *testing.T) {
	b := metricsBroker(t)

	// Wrap all three, which is what SetStores does to a durable provider,
	// and then ask both questions of them.
	b.SetStores(Stores{
		Logs:   map[string]LogStore{"events": b.logs["events"]},
		Latest: map[string]LatestStore{"state": b.latest["state"]},
		Queues: map[string]QueueStore{"jobs": b.queues["jobs"]},
	})
	if _, ok := b.logs["events"].(countingLog); !ok {
		t.Fatal("the log was not wrapped, so this test is asserting nothing")
	}

	for _, name := range []string{"events", "state", "jobs"} {
		if _, ok := b.channelState(b.reg.Get(name)); !ok {
			t.Errorf("a wrapped %s channel cannot export itself: it would be dropped "+
				"from every snapshot, and nothing would say so (invariant 14)", name)
		}
	}

	if _, ok := storeBehind(b.logs["jobs__dlq"]).(LogStore); !ok {
		t.Error("the dead-letter channel does not come back out of its wrapper: " +
			"a record leaving the queue would have nowhere to arrive (invariant 5)")
	}
}

// RFC 0005 "The catalogue" - saguin_published_total, saguin_broadcast_unmatched_total
//
// **A publish on a topic no channel claims is still a publish.** The
// per-channel tally is a map keyed by channel name, and forChannel answers
// nil for a broadcast topic on purpose - otherwise a client could grow the
// map a key at a time by publishing to names it made up. So the one number
// the catalogue promises beside the channels is the one number that path
// cannot produce, and every broadcast record went uncounted while the HELP
// text said otherwise.
func TestBroadcastIsCountedAndSaysWhetherItReachedAnybody(t *testing.T) {
	b := metricsBroker(t)

	for _, topic := range []string{"telemetry/a", "telemetry/b", "events/order/1"} {
		if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
			TopicName: topic, Payload: []byte("x"),
			FixedHeader: packets.FixedHeader{Qos: 1},
		}); err != nil && !errors.Is(err, packets.CodeSuccessIgnore) {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}
	if got := b.counted.broadcast.Load(); got != 2 {
		t.Errorf("broadcast publishes counted %d, want 2: a record accepted on a "+
			"topic no channel claims is missing from saguin_published_total", got)
	}

	// Reached nobody, which is what a mistyped channel name looks like.
	// MQTT answers a publish that matched no subscription with success, so
	// this series is the only thing that can tell an operator.
	b.OnSelectSubscribers(&mqtt.Subscribers{}, packets.Packet{TopicName: "telemetry/a"})
	// Reached somebody: not unmatched, however few.
	b.OnSelectSubscribers(&mqtt.Subscribers{
		Subscriptions: map[string]packets.Subscription{"c": {Filter: "telemetry/#"}},
	}, packets.Packet{TopicName: "telemetry/b"})
	// A channel topic is never broadcast at all.
	b.OnSelectSubscribers(&mqtt.Subscribers{}, packets.Packet{TopicName: "events/order/1"})
	// **Nor is anything in the server's own space.** The substrate once
	// published its whole $SYS tree at startup, and counting those read 20
	// unmatched broadcasts on a broker nothing had connected to - measured,
	// not supposed. It no longer does, and the exclusion stays for anything
	// that publishes there.
	for _, sys := range []string{
		"$SYS/broker/version", "$SYS/broker/uptime", "$SYS/broker/clients/connected",
	} {
		b.OnSelectSubscribers(&mqtt.Subscribers{}, packets.Packet{TopicName: sys})
	}

	if got := b.counted.broadcastUnmatched.Load(); got != 1 {
		t.Errorf("unmatched broadcast counted %d, want 1", got)
	}

	body := body(t, scrapeOnce(t, b, 0, nil))
	for _, want := range []string{
		`saguin_published_total{channel="broadcast"} 2`,
		"saguin_broadcast_unmatched_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("a scrape does not carry %q, which the catalogue lists", want)
		}
	}
}

// RFC 0005 "The catalogue" - saguin_publish_refused_total
//
// **The reason label is the MQTT specification's name for the code, not the
// sentence saguin wrote beside it.** A refusal carries both: a byte the
// specification names, and prose meant for the client that read it. Labelling
// by the prose put "a dead-letter channel takes records only from the queue
// that derives it; publish to the queue instead" in a metric - a label that
// is renamed by any rewording of an error message, silently breaking whatever
// matched on it. That is the failure a closed catalogue exists to prevent.
func TestARefusalIsLabelledByTheSpecificationsNameForItsCode(t *testing.T) {
	for _, c := range []struct {
		what string
		err  error
		want string
	}{
		{"saguin's own sentence on a specified code",
			packets.Code{Code: 0x87, Reason: "a dead-letter channel takes records " +
				"only from the queue that derives it; publish to the queue instead"},
			"not authorized"},
		{"the specification's own wording", packets.ErrQuotaExceeded, "quota exceeded"},
		{"retain refused, which is its own answer and not a bare rejection",
			packets.ErrRetainNotSupported, "retain not supported"},
		{"a code no name is known for", packets.Code{Code: 0x92, Reason: "invented"},
			"0x92"},
		{"no code at all", errors.New("plain"), "rejected"},
		{"the hook contract's stop signal, which is never answered on the wire",
			packets.ErrRejectPacket, "rejected"},
	} {
		if got := refusalReason(c.err); got != c.want {
			t.Errorf("%s: labelled %q, want %q", c.what, got, c.want)
		}
	}

	// The whole set, so that a label added tomorrow is bounded too: a
	// reason is a specification name, a hex code, or "rejected", and never
	// a sentence.
	for code, name := range reasonNames {
		if strings.ContainsAny(name, ";,.") || len(name) > 40 {
			t.Errorf("0x%02X is named %q, which reads as prose rather than as a "+
				"label", code, name)
		}
	}
}

// RFC 0005 "The catalogue" - saguin_publish_refused_total
//
// A refusal saguin answers by hand still has a code, and the counter must
// carry it. refuseRetain sends its own DISCONNECT because the hook contract
// offers no way to, then returns ErrRejectPacket to stop the packet - so
// the counter saw only "packet rejected" where the client was told 0x9A.
func TestARefusalThatAnswersByHandIsCountedByWhatItAnswered(t *testing.T) {
	b := metricsBroker(t)
	b.srv = mqtt.New(&mqtt.Options{InlineClient: true})

	cl := &mqtt.Client{ID: "p", Properties: mqtt.ClientProperties{ProtocolVersion: 5}}
	cl.Net.Conn = nil
	_, err := b.OnPublish(cl, packets.Packet{
		TopicName: "sensors/hall", Payload: []byte("x"),
		FixedHeader: packets.FixedHeader{Qos: 1, Retain: true},
	})
	if err == nil {
		t.Fatal("a retained publish with nowhere to keep it was accepted")
	}
	// The substrate must still stop the packet and answer nothing further:
	// the client has had its DISCONNECT already.
	if !errors.Is(err, packets.ErrRejectPacket) {
		t.Errorf("the substrate would no longer treat this as a rejection: %v", err)
	}
	if got := refusalReason(err); got != "retain not supported" {
		t.Errorf("counted under %q, want %q: the client was answered 0x9A",
			got, "retain not supported")
	}
	found := false
	for _, r := range b.counted.refusals() {
		if r.reason == "retain not supported" {
			found = true
		}
	}
	if !found {
		t.Errorf("saguin_publish_refused_total has no series for the code the "+
			"client was answered with: %v", b.counted.refusals())
	}
}

// RFC 0005 "The catalogue" - saguin_publish_refused_total
//
// **The refusal an operator is most likely to have caused was the one the
// counter never saw.**
//
// RFC 0005 gives this metric one job: "what answers 'the fleet's data is
// not arriving' without reading a log". A rule that is wrong refuses every
// publish the fleet makes with 0x87 - and that answer is given at the
// authorization hook, before the publish path that counts and logs runs at
// all. Six publishes answered 0x87 and there was no such series
// in the scrape and no line in the log at any level, including debug.
//
// It asks the hook directly because that is where the gap was: a test that
// went through OnPublish would exercise the verb refusal instead, which was
// always counted, and pass while this stayed broken.
func TestAPublishTheACLRefusesIsCounted(t *testing.T) {
	b := metricsBroker(t)
	acl := filepath.Join(t.TempDir(), "acl.yaml")
	// A file that names somebody else, so this client is refused by the
	// default rather than by a rule written at it.
	if err := os.WriteFile(acl, []byte(`
roles:
  publisher:
    - channel: events
      allow: [write]
users:
  "someone-else": [publisher]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	f, err := authz.Load(acl, b.Registry(), 0)
	if err != nil {
		t.Fatalf("load acl: %v", err)
	}
	b.Authorize(authz.New(f, b.Registry()))

	cl := &mqtt.Client{ID: "device-7", Properties: mqtt.ClientProperties{ProtocolVersion: 5}}
	for i := 0; i < 3; i++ {
		if b.OnACLCheck(cl, "events/a", true) {
			t.Fatal("the acl refused nothing, so this test proves nothing about counting")
		}
	}

	var n uint64
	for _, r := range b.counted.refusals() {
		if r.reason == "not authorized" {
			n = r.n
		}
	}
	if n != 3 {
		t.Errorf("three publishes were refused 0x87 and the counter holds %d in "+
			"%v; an operator watching this number sees a fleet that has stopped "+
			"sending and a metric saying nothing was refused", n, b.counted.refusals())
	}
}

// RFC 0005 "saguin_storage_errors_total"
//
// **The number reaches the catalogue**, which is the half
// TestOnlyRealStorageFailuresAreCounted does not reach: that one asks the
// counter directly and calls the store by hand, so it proves the tally and
// says nothing about the series an operator actually scrapes. Until this,
// the metric had never been seen non-zero by anything.
//
// Driven through OnPublish rather than through the store, because the
// wrapper that counts is installed by SetStores and a test that reaches
// past it is exercising a path the broker does not take.
func TestAStorageFailureReachesTheCatalogue(t *testing.T) {
	series := `saguin_storage_errors_total{provider="local"}`
	b := metricsBroker(t)
	b.SetStores(Stores{Logs: map[string]LogStore{
		"events": failingLog{LogStore: b.logs["events"], err: errors.New("input/output error")},
	}})

	// No series at all until a provider has actually failed - RFC 0005's
	// rule, and the reason a reading of 1 below means anything.
	if got := body(t, scrapeOnce(t, b, 0, nil)); strings.Contains(got, series) {
		t.Fatal("a broker that has failed nothing already publishes the series")
	}

	if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "events/order/1", Payload: []byte("x"),
	}); err == nil {
		t.Fatal("the publish succeeded, so no storage call failed and this proves nothing")
	}

	got := body(t, scrapeOnce(t, b, 0, nil))
	if want := series + " 1"; !strings.Contains(got, want) {
		t.Errorf("want %q. What the catalogue says about storage:\n%s", want, storageLines(got))
	}
}

// storageLines is what the catalogue says about storage, so a failure here
// prints what the broker answered rather than only what was wanted.
func storageLines(catalogue string) string {
	var out []string
	for _, l := range strings.Split(catalogue, "\n") {
		if strings.Contains(l, "storage_errors") {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return "(no saguin_storage_errors_total lines at all)"
	}
	return strings.Join(out, "\n")
}

// RFC 0005 "saguin_queue_expired_total"
//
// Work that ages out with no worker attached. It had only ever been
// observed at zero, which is what a counter that is never incremented also
// looks like - so this configures an expiry and drives a job out through
// the sweep, which is the only path that reaches a queue whose workers have
// all gone away.
//
// The two numbers are asserted together because an operator reads them
// together: work left the queue for its dead-letter channel, so the queue
// is shorter and somebody has to be told why.
func TestWorkThatAgesOutIsCounted(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, Storage: "local",
			VisibilityTimeout: 30, MaxAttempts: 3, JobExpiresAfter: 60},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	topics, headers, conns := 1024, 32, int64(10000)
	var lim config.Limits
	lim.MaxTopicLength, lim.MaxHeaderCount, lim.MaxConnections = &topics, &headers, &conns
	lim.MaxMessageSize, lim.MaxHeaderBytes = "1MiB", "8KiB"
	b.limits = lim.Resolve()
	b.SetIdentity("edge-1", "0.1.0-test")
	b.SetProviderKinds(map[string]string{"local": "memory"})

	if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "jobs/build/1", Payload: []byte("x"),
	}); err != nil && !errors.Is(err, packets.CodeSuccessIgnore) {
		// CodeSuccessIgnore is how a channel publish succeeds: stored, and
		// not handed on to the substrate's own delivery.
		t.Fatalf("enqueue: %v", err)
	}

	// Nobody is subscribed, so no delivery is ever offered and the sweep is
	// the only thing that can reach this job. An hour on, it is past its
	// sixty-second expiry.
	b.sweepByAge(time.Now().Add(time.Hour))

	// It really did leave the queue, or the counters below would be right
	// to read zero.
	b.mu.Lock()
	dlq := b.logs["jobs__dlq"]
	b.mu.Unlock()
	if dlq == nil {
		t.Fatal("the queue has no dead-letter channel, so nothing could have moved")
	}
	moved, err := dlq.ReadFrom(1)
	if err != nil {
		t.Fatalf("read the dead-letter channel: %v", err)
	}
	if len(moved) != 1 {
		t.Fatalf("the dead-letter channel holds %d records, want 1: the job did not "+
			"expire, so this test proves nothing about the counters", len(moved))
	}

	got := body(t, scrapeOnce(t, b, 0, nil))
	for _, want := range []string{
		`saguin_queue_expired_total{channel="jobs"} 1`,
		`saguin_queue_dead_lettered_total{channel="jobs"} 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("want %q. A job aged out and was dead-lettered, and the number "+
				"an operator watches to find out says otherwise:\n%s", want, queueLines(got))
		}
	}
}

// queueLines is what the catalogue says about this queue, so a failure
// prints what the broker answered rather than only what was wanted.
func queueLines(catalogue string) string {
	var out []string
	for _, l := range strings.Split(catalogue, "\n") {
		if strings.HasPrefix(l, "saguin_queue_") && strings.Contains(l, `channel="jobs"`) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// RFC 0005 "saguin_publish_refused_total"
//
// Every label this broker can actually produce, produced - by a publish
// through OnPublish rather than by asserting the constants, which is what
// the catalogue had before: the codes were spec constants checked in unit
// tests and the *series* had been seen for three of them.
//
// **The map is closed by the MQTT specification, not by saguin's call
// sites**, deliberately, so a code saguin never answers is in it on purpose
// and "produce all ten" is the wrong target. What can be asked is whether
// every code the publish path can answer reaches the catalogue under the
// specification's name for it. Four cannot be reached from here and each is
// unreachable for a reason rather than for want of a test:
//
//   - 0x95 packet too large - the substrate applies the packet bound before
//     any hook runs, and the bridge's own check (BridgeClient.Bounded) is
//     not a publish from a client.
//   - 0x9B qos not supported - a QoS 2 publish above a client's ceiling is
//     refused, but in OnPacketRead before OnPublish is reached, so it is
//     counted there and asserted by
//     TestAPublishAboveTheAdvertisedMaximumQoSIsRefused. A subscription is
//     granted at a lower QoS and a Will carrying one is refused at CONNECT.
//   - 0x9F connection rate exceeded - a connection, not a publish.
//   - 0x80 unspecified error - saguin answers a named code everywhere it
//     refuses, which is the point of the closed set. Nothing produces this,
//     and if something starts to, the map already has the name for it.
func TestEveryRefusalTheBrokerCanAnswerReachesTheCatalogue(t *testing.T) {
	b := metricsBroker(t)

	// A server, because two of these refusals take the client's connection
	// down as well as answering a code - the retain flag where nothing can
	// keep it is a DISCONNECT by design (RFC 0003). The refusal is still
	// returned and still counted; without a server the disconnect panics on
	// a nil one, which is the harness lacking a part rather than anything
	// about the broker.
	b.SetServer(mqtt.New(nil))

	tooMany := make([]packets.UserProperty, 64) // max_header_count is 32
	for i := range tooMany {
		tooMany[i] = packets.UserProperty{Key: fmt.Sprintf("k%d", i), Val: "v"}
	}

	for _, tc := range []struct {
		name  string
		label string
		pk    packets.Packet
	}{
		{"a topic in the reserved space that saguin does not define", "topic name invalid",
			packets.Packet{TopicName: "$saguin/nothing/here", Payload: []byte("x")}},
		{"more headers than max_header_count", "quota exceeded",
			packets.Packet{TopicName: "events/a", Payload: []byte("x"),
				Properties: packets.Properties{User: tooMany}}},
		{"a payload that lied about being UTF-8", "payload format invalid",
			packets.Packet{TopicName: "events/a", Payload: []byte{0xff, 0xfe},
				Properties: packets.Properties{PayloadFormatFlag: true, PayloadFormat: 1}}},
		// Broadcast rather than a queue: a retained publish to a queue is
		// taken as ordinary work with the flag dropped, so the one thing
		// left that can be answered 0x9A is a broadcast topic on a broker
		// with no retained store - which this one is.
		{"the retain flag with no store to keep it", "retain not supported",
			packets.Packet{TopicName: "loose/thing", Payload: []byte("x"),
				FixedHeader: packets.FixedHeader{Retain: true}}},
		{"a publish into a dead-letter channel", "not authorized",
			packets.Packet{TopicName: "jobs/__dlq/x", Payload: []byte("x")}},
		{"a point read with nowhere to answer", "implementation specific error",
			packets.Packet{TopicName: "$saguin/kv/get", Payload: []byte("state/a")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pk := tc.pk
			pk.FixedHeader.Type = packets.Publish
			pk.FixedHeader.Qos = 1
			if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, pk); err == nil ||
				errors.Is(err, packets.CodeSuccessIgnore) {
				t.Fatalf("the publish was accepted (%v), so no refusal was counted", err)
			}
			got := body(t, scrapeOnce(t, b, 0, nil))
			want := fmt.Sprintf(`saguin_publish_refused_total{reason=%q} 1`, tc.label)
			if !strings.Contains(got, want) {
				t.Errorf("want %q. What the catalogue says about refusals:\n%s",
					want, refusalLines(got))
			}
		})
	}

	// And the one that is not a code at all. A QoS 0 publish has no reply to
	// carry a refusal, so it is dropped - ErrRejectPacket, which carries no
	// reason a client could read and gets its own series rather than being
	// folded into one that names a code.
	t.Run("a refusal with no code to give", func(t *testing.T) {
		if _, err := b.OnPublish(&mqtt.Client{ID: "p"}, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
			TopicName:   "$saguin/nothing/here", Payload: []byte("x"),
		}); err == nil {
			t.Fatal("the QoS 0 publish was accepted, so nothing was dropped")
		}
		got := body(t, scrapeOnce(t, b, 0, nil))
		if want := `saguin_publish_refused_total{reason="rejected"} 1`; !strings.Contains(got, want) {
			t.Errorf("want %q. What the catalogue says about refusals:\n%s",
				want, refusalLines(got))
		}
	})
}

func refusalLines(catalogue string) string {
	var out []string
	for _, l := range strings.Split(catalogue, "\n") {
		if strings.HasPrefix(l, "saguin_publish_refused_total{") {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return "(no saguin_publish_refused_total series at all)"
	}
	return strings.Join(out, "\n")
}

// RFC 0005 - saguin_storage_errors_total, read from the source rather than
// from a running broker.
//
// **Embedding does not make an added method a compile error.** The counting
// wrappers each embed their store interface and override only the methods
// that return an error, and for a while a comment claimed the compiler
// would catch a method added to one of those interfaces. It does not: the
// new method is promoted from the embedded value, the wrapper satisfies the
// interface, everything builds. That is how `Get` was added to LatestStore
// and every point-read storage failure went uncounted - silently, in the
// number an operator watches to find out whether their disk is failing, and
// a counter reading zero is not a thing anybody goes looking at.
//
// So the guard is a check on the source, and it is written to **ask the
// opposite question**: not "is this one of the methods I know about" but
// "is every storage failure the broker can be handed provably counted", so
// that what nobody has thought of yet fails here until somebody decides.
//
// It asks that of three things, because a failure goes uncounted if any one
// of them is missing and the first two are invisible at the call site:
//
//   - a store interface with no wrapper standing in front of it,
//   - a wrapper that does not declare one of its interface's failing
//     methods, which is the defect that happened,
//   - a wrapper nothing installs, which counts exactly as little as no
//     wrapper at all.
//
// **Nothing here is a list of the three stores.** The subject is the Stores
// struct, which is the broker's own statement of what a store is; the
// wrapper for each is whatever struct embeds that interface; and the
// installation site is whatever function takes a Stores. A fourth store
// type added next month is therefore checked on the day it is written
// rather than the day somebody remembers this file. Only non-test files are
// read: a test's own stub embeds the same interfaces to override one method
// on purpose, which is what a stub is for.
func TestEveryStoreMethodThatCanFailIsCounted(t *testing.T) {
	fset := token.NewFileSet()

	ifaces := map[string]*ast.InterfaceType{} // what this package declares
	structs := map[string]*ast.StructType{}   // the same for structs
	embeds := map[string][]string{}           // struct -> interfaces it embeds
	methods := map[string]map[string]bool{}   // receiver -> methods declared on it
	installs := map[string]bool{}             // identifiers named by SetStores
	bodies := map[string]map[string]bool{}    // function -> identifiers its body names

	var scanned int
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++

		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not ours to report; the compiler already has
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					switch t := ts.Type.(type) {
					case *ast.InterfaceType:
						ifaces[ts.Name.Name] = t
					case *ast.StructType:
						structs[ts.Name.Name] = t
						for _, field := range t.Fields.List {
							if len(field.Names) != 0 {
								continue // a named field, not an embedding
							}
							if id, ok := field.Type.(*ast.Ident); ok {
								embeds[ts.Name.Name] = append(embeds[ts.Name.Name], id.Name)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if recv := receiverName(d); recv != "" {
					if methods[recv] == nil {
						methods[recv] = map[string]bool{}
					}
					methods[recv][d.Name.Name] = true
				}
				if d.Body != nil {
					if bodies[d.Name.Name] == nil {
						bodies[d.Name.Name] = map[string]bool{}
					}
					ast.Inspect(d.Body, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok {
							bodies[d.Name.Name][id.Name] = true
						}
						return true
					})
				}
				// The function that takes a Stores is the one that attaches
				// them, and every wrapper has to be named in it.
				if takesStores(d) {
					ast.Inspect(d.Body, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok {
							installs[id.Name] = true
						}
						return true
					})
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the package: %v", err)
	}
	// A check that silently examined nothing is worse than none, and this is
	// asked before anything is concluded from what was read: every failure
	// below would otherwise be reported as a missing wrapper when the truth
	// is that the walk never reached the package.
	if scanned < 5 {
		t.Fatalf("only %d source files scanned; the walk is not reaching the package", scanned)
	}

	// Whatever struct embeds an interface is the wrapper standing in front
	// of it. Two wrapping one interface is not a shape this package has, and
	// the check would rather report the pair than pick one.
	wrapperOf := map[string][]string{}
	for _, wrapper := range sortedKeys(embeds) {
		for _, embedded := range embeds[wrapper] {
			if _, ok := ifaces[embedded]; ok {
				wrapperOf[embedded] = append(wrapperOf[embedded], wrapper)
			}
		}
	}

	// The Stores struct is what the broker calls a store, so it is what this
	// asks about rather than three names written here.
	stores, ok := structs["Stores"]
	if !ok {
		t.Fatal("the Stores struct is not in this package any more; this check is " +
			"asking about a shape that no longer exists")
	}

	var storeTypes, checked int
	for _, field := range stores.Fields.List {
		iface := elementName(field.Type)
		if _, ok := ifaces[iface]; !ok {
			continue // a field that does not carry a store this package declares
		}
		storeTypes++

		wrappers := wrapperOf[iface]
		if len(wrappers) == 0 {
			t.Errorf("Stores carries %s and nothing in this package embeds it, so no "+
				"wrapper stands in front of it and every storage failure it returns "+
				"goes uncounted in saguin_storage_errors_total.\n"+
				"\tWrap it as countingLog wraps LogStore, and install it in SetStores.",
				iface)
			continue
		}
		for _, wrapper := range wrappers {
			if !installs[wrapper] {
				t.Errorf("%s wraps %s and SetStores never names it, so the store the "+
					"broker is handed is the store it keeps and nothing counts its "+
					"failures.\n\tInstall it where the other wrappers are installed.",
					wrapper, iface)
			}
			for _, m := range failingMethods(ifaces[iface], ifaces, map[string]bool{}) {
				checked++
				if methods[wrapper][m] {
					continue
				}
				t.Errorf("%s.%s is not declared on %s, so it is promoted from the "+
					"embedded %s and every storage failure it returns goes uncounted "+
					"in saguin_storage_errors_total.\n"+
					"\tDeclare it on %s, calling c.on(err) with the error it returns.",
					iface, m, wrapper, iface, wrapper)
			}
		}
	}

	// **The stores attached outside SetStores**, each by the function that
	// attaches it: the session store, the
	// broadcast log with its sessions and holds, and the retained store.
	// Checked through the Stores struct alone, 56 of the broker's 137 failing
	// store calls went uncounted and this test said nothing, because it
	// asked only about what SetStores is handed. A function renamed away
	// fails here rather than ending the check.
	for _, a := range []struct{ iface, attach string }{
		{"SessionStore", "SetSessions"},
		{"broadcastLog", "useBroadcastLog"},
		{"broadcastSessions", "useBroadcastLog"},
		{"HoldStore", "useBroadcastLog"},
		{"LatestStore", "SetRetained"},
	} {
		named, ok := bodies[a.attach]
		if !ok {
			t.Errorf("%s, which attaches %s, is not in this package any more", a.attach, a.iface)
			continue
		}
		storeTypes++
		wrappers := wrapperOf[a.iface]
		if len(wrappers) == 0 {
			t.Errorf("nothing in this package embeds %s, so every storage failure it returns "+
				"goes uncounted", a.iface)
			continue
		}
		wrapped := false
		for _, wrapper := range wrappers {
			if !named[wrapper] {
				continue
			}
			wrapped = true
			for _, m := range failingMethods(ifaces[a.iface], ifaces, map[string]bool{}) {
				checked++
				if !methods[wrapper][m] {
					t.Errorf("%s.%s is not declared on %s, so every storage failure it returns "+
						"goes uncounted in saguin_storage_errors_total", a.iface, m, wrapper)
				}
			}
		}
		if !wrapped {
			t.Errorf("%s attaches %s without any of its wrappers %v, so the store it keeps is "+
				"the store it was handed and nothing counts its failures", a.attach, a.iface, wrappers)
		}
	}

	t.Logf("%d failing methods examined across %d store types in %d source files",
		checked, storeTypes, scanned)
	if storeTypes < 3 {
		t.Fatalf("only %d store types found in Stores; there are three, so this check "+
			"is no longer recognising the shape it is about", storeTypes)
	}
	if checked < 15 {
		t.Fatalf("only %d failing methods examined, which is too few to believe - "+
			"this check is no longer reading the interfaces it thinks it is", checked)
	}
}

// takesStores says whether a function is handed the Stores struct, which is
// what makes it the place they are attached. Asking the parameter rather
// than the name is so that a rename does not quietly end the check.
func takesStores(d *ast.FuncDecl) bool {
	if d.Body == nil || d.Type.Params == nil {
		return false
	}
	for _, p := range d.Type.Params.List {
		if elementName(p.Type) == "Stores" {
			return true
		}
	}
	return false
}

// elementName is the type a field carries, with a map, slice or pointer
// taken off: the LogStore in `map[string]LogStore`, and the Stores in
// `s Stores`.
func elementName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.MapType:
		return elementName(t.Value)
	case *ast.ArrayType:
		return elementName(t.Elt)
	case *ast.StarExpr:
		return elementName(t.X)
	}
	return ""
}

// failingMethods is every method of an interface whose last result is an
// error, which is every method a counting wrapper has to declare.
//
// An interface embedded in another is followed, because a store interface
// that grows by embedding grows the same way as one that grows by adding a
// line, and the wrapper is just as silent about it. The seen set is for the
// cycle a mutually embedding pair would otherwise make.
func failingMethods(it *ast.InterfaceType, all map[string]*ast.InterfaceType, seen map[string]bool) []string {
	var out []string
	for _, field := range it.Methods.List {
		if len(field.Names) == 0 {
			if id, ok := field.Type.(*ast.Ident); ok && !seen[id.Name] {
				seen[id.Name] = true
				if inner, ok := all[id.Name]; ok {
					out = append(out, failingMethods(inner, all, seen)...)
				}
			}
			continue
		}
		fn, ok := field.Type.(*ast.FuncType)
		if !ok || fn.Results == nil || len(fn.Results.List) == 0 {
			continue
		}
		last := fn.Results.List[len(fn.Results.List)-1]
		if id, ok := last.Type.(*ast.Ident); !ok || id.Name != "error" {
			continue
		}
		for _, name := range field.Names {
			out = append(out, name.Name)
		}
	}
	return out
}

// receiverName is the type a method is declared on, with any pointer taken
// off, and "" for a function that is not a method.
func receiverName(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return ""
	}
	return elementName(d.Recv.List[0].Type)
}

// RFC 0005 "The catalogue" - the tie between what it promises and what the
// broker serves.
//
// **The catalogue says of itself: "Closed. A name here is a promise; a name
// not here is not published."** That is a two-way set equality rather than
// prose, so it can be run - and until it was, it drifted: the Bridges table
// listed four metrics while the broker served five, with the missing one
// printed in the RFC's own sample twelve lines further up and nothing
// anywhere able to object. Adding the row closes that instance. This closes
// the class, because a document has no other way to notice the code moved.
//
// It asks the opposite question from a list of known mistakes: not "is this
// name one I know is missing" but "is every name on both sides", so a metric
// nobody has thought of yet fails here until somebody writes it down.
//
// **A row may say a name is deliberately not served**, and one does:
// saguin_channel_consumers, with the measured cost of counting them. The
// exemption is read out of the document rather than kept in this file - a
// claim about what that row says, which cannot rot, rather than a name
// hardcoded here, which stops being true the moment the row changes.
//
// The cost, stated because whoever edits that document next inherits it:
// those tables are load-bearing for this suite now. A reformatting that
// stops a row starting with a backticked name will fail this test.
func TestTheCatalogueInRFC0005IsWhatTheBrokerServes(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs", "0005-operations.md"))
	if err != nil {
		t.Fatalf("read RFC 0005: %v", err)
	}

	// A table row naming a metric: `| \`saguin_x{labels}\` | type | words |`.
	// The labels are not compared - they are checked by
	// TestEveryLabelInTheCatalogueIsBounded, and duplicating that here would
	// be two tests failing for one cause.
	//
	// **Digits are part of a name**, which this pattern did not allow until
	// `saguin_qos2_held` arrived: every name until then happened to be
	// letters, so the omission read as deliberate and was not. A name the
	// pattern cannot see is a row this test skips, and a skipped row is a
	// promise nobody checks.
	row := regexp.MustCompile("(?m)^\\| `(saguin_[a-z0-9_]+)[`{]")
	promised, unpublished := map[string]bool{}, map[string]bool{}
	for _, line := range strings.Split(string(md), "\n") {
		m := row.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if strings.Contains(line, "**Not published.**") {
			unpublished[m[1]] = true
			continue
		}
		promised[m[1]] = true
	}

	// The counter every check of this shape owes. A regexp that quietly
	// matched nothing would agree with any broker at all.
	if len(promised) < 20 {
		t.Fatalf("only %d metric rows were found in RFC 0005, which is too few to be the "+
			"catalogue: this test read the document and did not understand it, so every "+
			"comparison below would pass by vacuum", len(promised))
	}

	served := map[string]bool{}
	for _, line := range strings.Split(body(t, scrapeOnce(t, metricsBroker(t), time.Minute, nil)), "\n") {
		if name, ok := strings.CutPrefix(line, "# TYPE "); ok {
			served[strings.Fields(name)[0]] = true
		}
	}
	if len(served) == 0 {
		t.Fatal("the scrape declared no metrics, so this compared nothing")
	}

	for name := range served {
		if promised[name] {
			continue
		}
		if unpublished[name] {
			t.Errorf("the broker serves %s, which RFC 0005 says is **Not published.** - "+
				"one of the two is wrong, and an operator reading the catalogue is the "+
				"one who finds out", name)
			continue
		}
		t.Errorf("the broker serves %s and RFC 0005's catalogue does not list it. The "+
			"catalogue calls itself closed, so a name it omits is one it promises does "+
			"not exist", name)
	}
	for name := range promised {
		if !served[name] {
			t.Errorf("RFC 0005's catalogue promises %s and the broker does not serve it. A "+
				"name here is a promise: a dashboard built on this row draws an empty "+
				"panel", name)
		}
	}
	t.Logf("%d names promised, %d served, %d listed as deliberately not served",
		len(promised), len(served), len(unpublished))
}

// slowMeasure is a storage provider that takes its time answering, which is
// what a sqlite one does whenever a write is in flight: it asks its database
// over a pool of exactly one connection, and the write has it.
type slowMeasure struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *slowMeasure) Bytes() int64 {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return 1
}

func (s *slowMeasure) MaxBytes() int64 { return 2 }

// **A provider that is slow to measure must be slow on its own time.**
//
// The sibling test above keeps the formatting out from under the lock. This
// keeps the asking out, which is a sharper version of the same rule: a
// memory provider answers from a counter, but a sqlite one answers by
// querying its database, and that query waits behind any write in flight.
// Under b.mu that wait belongs to everybody - pumpAll takes b.mu for every
// record stored - so one HTTP request would put one provider's commit
// latency in front of every publisher on every provider, including the ones
// with no database at all.
//
// Measured on a real broker before this was fixed: a publisher on a memory
// channel saw its acknowledgement go from 0.001s to 1.897s while a single
// scrape waited on another provider's write. That is invariant 16 - a
// publisher waits on storage and on nothing else - broken by an observer,
// and the observer is the shipped monitoring stack arriving every 15s.
//
// The assertion is a third party taking the lock, not the scrape finishing:
// a test that only waited for the answer would pass against a collector
// that had held the lock the whole way.
func TestASlowProviderDoesNotHoldTheBrokersLock(t *testing.T) {
	b := metricsBroker(t)
	slow := &slowMeasure{entered: make(chan struct{}), release: make(chan struct{})}
	b.SetProviderKinds(map[string]string{"local": "memory", "slow": "sqlite"})
	b.SetProviderMeasures(map[string]ProviderMeasure{"slow": slow})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.catalogue()
	}()

	select {
	case <-slow.entered:
	case <-time.After(5 * time.Second):
		close(slow.release)
		t.Fatal("the scrape never asked the provider how much it holds, so this test " +
			"never reached the state it is about")
	}

	// The provider is now inside Bytes and will not return. Anybody else
	// must still be able to take the broker's lock - which is what a
	// publish, a subscribe and the health probe all need.
	took := make(chan struct{})
	go func() {
		b.mu.Lock()
		b.mu.Unlock()
		close(took)
	}()
	select {
	case <-took:
	case <-time.After(2 * time.Second):
		close(slow.release)
		<-done
		t.Fatal("nothing could take the broker's lock while a storage provider was being " +
			"asked how much it holds: a scrape is holding it across that question, so " +
			"one provider's write latency is every publisher's acknowledgement latency")
	}

	close(slow.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scrape did not finish once the provider answered")
	}
}

// A real sqlite provider is what proves saguin_storage_commits_total is
// wired to something. The fake used elsewhere in this file answers whatever
// it is told, which is exactly what a metric about a mechanism must not be
// tested against: it would report a batch size for a provider that never
// batched anything.
var _ CommitCounter = (*sqlite.DB)(nil)

// Group commit, as an operator sees it.
//
// **The numbers are asserted against each other, not against constants.**
// Records over commits is the average batch size, and it is the only thing
// on the whole endpoint that says whether collecting publishes is doing
// anything - a provider that ignored publish_commit_interval serves the same
// records, offsets and byte counts as one that honoured it, and would pass
// every other test here.
func TestGroupCommitIsVisibleInTheMetrics(t *testing.T) {
	const publishes = 64

	db, err := sqlite.Open(filepath.Join(t.TempDir(), "saguin.db"), "metrics-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.CommitGroup(50*time.Millisecond, 32)

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	var wg sync.WaitGroup
	for range publishes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")}); err != nil {
				t.Errorf("append: %v", err)
			}
		}()
	}
	wg.Wait()

	// And a provider that says nothing, which collects behind the commit
	// before: one publish on it is one transaction closed that way.
	plain, err := sqlite.Open(filepath.Join(t.TempDir(), "plain.db"), "metrics-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = plain.Close() }()
	plainLog, err := plain.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if _, err := plainLog.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")}); err != nil {
		t.Fatalf("append: %v", err)
	}

	b := metricsBroker(t)
	b.SetProviderKinds(map[string]string{"local": "memory", "durable": "sqlite", "plain": "sqlite"})
	b.SetProviderMeasures(map[string]ProviderMeasure{"durable": db, "plain": plain})
	text := body(t, scrapeOnce(t, b, time.Minute, nil))

	series := map[string]float64{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		series[fields[0]] = v
	}

	byRecords := series[`saguin_storage_commits_total{provider="durable",closed_by="records"}`]
	byInterval := series[`saguin_storage_commits_total{provider="durable",closed_by="interval"}`]
	unbatched := series[`saguin_storage_commits_total{provider="durable",closed_by="unbatched"}`]
	behind, served := series[`saguin_storage_commits_total{provider="durable",closed_by="commit"}`]
	records := series[`saguin_storage_committed_records_total{provider="durable"}`]
	ceiling := series[`saguin_provider_publish_commit_max_records{provider="durable"}`]

	commits := byRecords + byInterval + unbatched
	switch {
	case commits == 0:
		t.Fatalf("no commits were counted for a provider that stored %d records.\n%s", publishes, text)
	case records != publishes:
		t.Errorf("the provider says it committed %v records and %d were published", records, publishes)
	case ceiling != 32:
		t.Errorf("saguin_provider_publish_commit_max_records is %v, want the configured 32", ceiling)
	case unbatched != 0:
		t.Errorf("%v transactions are counted as unbatched on a provider that was asked to collect", unbatched)
	case !served:
		t.Errorf("no closed_by=\"commit\" series is served, so a provider collecting behind the commit before "+
			"would have its transactions counted nowhere.\n%s", text)
	case behind != 0:
		t.Errorf("%v transactions are counted as closed behind the commit before on a provider that waits "+
			"out an interval", behind)
	}

	for _, c := range []struct {
		series string
		want   float64
	}{
		{`saguin_storage_commits_total{provider="plain",closed_by="commit"}`, 1},
		{`saguin_storage_commits_total{provider="plain",closed_by="unbatched"}`, 0},
		{`saguin_storage_commits_total{provider="plain",closed_by="interval"}`, 0},
		{`saguin_storage_committed_records_total{provider="plain"}`, 1},
		{`saguin_provider_publish_commit_max_records{provider="plain"}`, sqlite.BehindMaxRecords},
	} {
		if got, ok := series[c.series]; !ok || got != c.want {
			t.Errorf("%s is %v (served %v), want %v on a provider that says nothing and took one publish",
				c.series, got, ok, c.want)
		}
	}

	// The point of the endpoint: a batch size above one, which is the only
	// reading that distinguishes a provider that collected from one that
	// did not.
	if size := records / commits; size <= 1 {
		t.Errorf("the average batch size is %.2f (%v records in %v commits): the metrics report a "+
			"provider that collected nothing, and an operator reading them would conclude "+
			"publish_commit_interval is doing nothing", size, records, commits)
	} else {
		t.Logf("%v records in %v commits - batch size %.1f against a ceiling of %v; %v closed by the "+
			"record count, %v by the interval", records, commits, size, ceiling, byRecords, byInterval)
	}

	// A memory provider has no transaction to collect into, so it gets no
	// series at all rather than a zero that reads like a measurement.
	for name := range series {
		if strings.Contains(name, `provider="local"`) &&
			(strings.Contains(name, "commits_total") ||
				strings.Contains(name, "committed_records_total") ||
				strings.Contains(name, "publish_commit_max_records")) {
			t.Errorf("%s is served for a memory provider, which has no transaction to collect into", name)
		}
	}
}

// The shipped dashboard names metrics, and a name it gets wrong renders as
// an empty panel that nobody reports - the same silence the catalogue tie
// above exists for, one artefact along. A panel is a claim about what the
// broker serves, so it is checked against what the broker serves.
//
// **The direction matters.** This does not ask that every metric be
// graphed: some are deliberately not, and a dashboard is an editorial
// choice. It asks the other way round - that nothing the dashboard graphs
// is absent from the endpoint, which is the half that can be wrong without
// anybody noticing.
func TestEveryMetricTheDashboardGraphsIsOneTheBrokerServes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "examples", "bento-connectors",
		"grafana", "dashboards", "saguin.json"))
	if err != nil {
		t.Fatalf("read the dashboard: %v", err)
	}

	// Every saguin_ name anywhere in the file's queries. Read out of the
	// JSON as text rather than by walking the panel structure, because a
	// name can appear in an expr, in a legend, or in a transformation, and
	// a walk that knew about only the first would quietly check less than
	// it claimed.
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the dashboard is not valid JSON, so Grafana would not load it: %v", err)
	}
	named := map[string]bool{}
	var walk func(any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			for k, sub := range v {
				if k == "expr" {
					if s, ok := sub.(string); ok {
						for _, m := range regexp.MustCompile(`saguin_[a-z0-9_]+`).FindAllString(s, -1) {
							named[m] = true
						}
					}
				}
				walk(sub)
			}
		case []any:
			for _, sub := range v {
				walk(sub)
			}
		}
	}
	walk(doc)

	// **The counter every check of this shape owes.** A walk that found the
	// wrong field, or a dashboard that stopped using `expr`, would report
	// nothing wrong about a file it never read.
	if len(named) < 15 {
		t.Fatalf("only %d metric names were found in the dashboard's queries, which is too few to "+
			"be it: this test read the file and did not understand it, so every comparison "+
			"below would pass by vacuum", len(named))
	}

	served := map[string]bool{}
	for _, line := range strings.Split(body(t, scrapeOnce(t, metricsBroker(t), time.Minute, nil)), "\n") {
		if name, ok := strings.CutPrefix(line, "# TYPE "); ok {
			served[strings.Fields(name)[0]] = true
		}
	}
	if len(served) == 0 {
		t.Fatal("the scrape declared no metrics, so this compared nothing")
	}

	missing := 0
	for name := range named {
		if !served[name] {
			missing++
			t.Errorf("the dashboard graphs %s and the broker does not serve it: that panel is empty "+
				"on every saguin there has ever been, and an empty panel is not a thing anybody "+
				"reports", name)
		}
	}

	// **The other direction, because the README promises it.** "Every metric
	// saguin emits is on it" is a claim about completeness, and completeness
	// is the half nobody notices going wrong: a metric added without a panel
	// is invisible on the dashboard that says it shows everything.
	for name := range served {
		if !named[name] {
			missing++
			t.Errorf("the broker serves %s and no dashboard query names it, while "+
				"examples/bento-connectors/README.md says every metric saguin emits is on the "+
				"dashboard. "+
				"Add a panel or stop saying every", name)
		}
	}

	// And the count in that sentence, which has rotted once already: it said
	// thirty-seven when the broker served forty. A number in prose that
	// nothing runs is a number that is wrong from the next commit onwards.
	readme, err := os.ReadFile(filepath.Join("..", "..", "examples", "bento-connectors", "README.md"))
	if err != nil {
		t.Fatalf("read the connectors README: %v", err)
	}
	words := map[string]int{
		"thirty-seven": 37, "thirty-eight": 38, "thirty-nine": 39, "forty": 40,
		"forty-one": 41, "forty-two": 42, "forty-three": 43, "forty-four": 44,
		"forty-five": 45, "forty-six": 46, "forty-seven": 47, "forty-eight": 48,
		"forty-nine": 49, "fifty": 50, "fifty-one": 51, "fifty-two": 52,
		"fifty-three": 53, "fifty-four": 54, "fifty-five": 55, "fifty-six": 56,
		"fifty-seven": 57, "fifty-eight": 58, "fifty-nine": 59, "sixty": 60,
		"sixty-one": 61, "sixty-two": 62, "sixty-three": 63, "sixty-four": 64,
		"sixty-five": 65, "sixty-six": 66, "sixty-seven": 67, "sixty-eight": 68,
		"sixty-nine": 69, "seventy": 70, "seventy-one": 71, "seventy-two": 72,
		"eighty": 80, "eighty-one": 81, "eighty-two": 82, "eighty-three": 83,
	}
	// The product name is spelled with its diaeresis in prose since the
	// branding change, and this read the ASCII spelling alone.
	claim := regexp.MustCompile(`Every metric (?:saguin|Sagüin) emits is on it - ([a-z-]+) of them`).
		FindSubmatch(readme)
	switch {
	case claim == nil:
		t.Error("examples/bento-connectors/README.md no longer says how many metrics are on the " +
			"dashboard in the shape this checks, so the count is unheld again - reword the " +
			"check or the README")
	case words[string(claim[1])] == 0:
		t.Errorf("the connectors README says %q metrics, which this check cannot read as a number; "+
			"add it to the table here", claim[1])
	case words[string(claim[1])] != len(served):
		t.Errorf("the connectors README says the dashboard carries %q metrics and the broker serves "+
			"%d", claim[1], len(served))
	}

	if missing == 0 {
		t.Logf("%d metric names, served and graphed, matching the README's count", len(named))
	}
}

// RFC 0005 "What it costs, measured"
//
// One scrape, at the four configuration sizes the RFC quotes. A tenth of
// the channels are queues and a tenth are latest, which is the shape of a
// real file rather than a thousand of one kind, and every channel holds one
// record so that every series the catalogue can carry is present. The bytes
// are reported beside the time, plain and gzipped, because the RFC's table
// has both and a scraper on a metered link cares about the second.
func BenchmarkScrape(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		chans := make([]*channel.Channel, 0, n)
		for i := range n {
			c := &channel.Channel{Name: fmt.Sprintf("c%d", i), Type: channel.Append, Storage: "local"}
			switch i % 10 {
			case 0:
				c.Type, c.VisibilityTimeout, c.MaxAttempts = channel.Queue, 30, 3
			case 1:
				c.Type = channel.Latest
			}
			chans = append(chans, c)
		}
		reg, err := channel.NewRegistry(chans)
		if err != nil {
			b.Fatalf("registry: %v", err)
		}
		br := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		topics, headers, conns := 1024, 32, int64(10000)
		var lim config.Limits
		lim.MaxTopicLength, lim.MaxHeaderCount, lim.MaxConnections = &topics, &headers, &conns
		lim.MaxMessageSize, lim.MaxHeaderBytes = "1MiB", "8KiB"
		br.limits = lim.Resolve()
		br.SetIdentity("edge-1", "bench")
		br.SetProviderKinds(map[string]string{"local": "memory"})

		rec := store.Record{MessageID: "m", Topic: "x/y", Payload: []byte("payload")}
		for name, lg := range br.logs {
			if _, err := lg.Append(rec); err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		for name, q := range br.queues {
			if _, err := q.Enqueue(rec); err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		for name, l := range br.latest {
			if _, err := l.Set(rec); err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}

		for _, encoding := range []string{"", "gzip"} {
			name := fmt.Sprintf("%d-channels", n)
			if encoding != "" {
				name += "/" + encoding
			}
			b.Run(name, func(b *testing.B) {
				// A nanosecond rather than zero, so that every scrape is
				// recomputed rather than answered from the previous one.
				h := br.metricsHandler(time.Nanosecond)
				var size int
				b.ResetTimer()
				for range b.N {
					r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
					if encoding != "" {
						r.Header.Set("Accept-Encoding", encoding)
					}
					w := httptest.NewRecorder()
					h.ServeHTTP(w, r)
					size = w.Body.Len()
				}
				b.ReportMetric(float64(size), "bytes/scrape")
			})
		}
	}
}

// RFC 0005 "Logging, and the process", and a client that goes away being
// logged with its whole packet.
//
// **The wrapper is four characters at one call site and nothing else would
// notice it going.** `mqtt.New` takes whatever logger it is handed, so an
// edit that passes `log` instead of the wrapped one compiles, runs, and
// puts payloads back in the operator's log - with every other test still
// green, because saguin's own lines were never the problem. This is the
// guard on the wiring, in the same shape as the one on the binary handing
// the broker its stores.
func TestTheSubstrateIsGivenTheBoundedLogger(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	srv, _, err := NewServer(reg, config.Resolved{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	if _, ok := srv.Log.Handler().(boundedHandler); !ok {
		t.Fatalf("the substrate's logger is a %T: it writes whole packets, payload and "+
			"credential included, so the one saguin hands it has to be the bounded one",
			srv.Log.Handler())
	}
}

// The ceiling, and the shapes that get a summary instead of a size.
//
// **The unknown-type case is the one that matters.** A list of shapes to
// bound is a list to keep in step with somebody else's code; what makes
// this hold is that anything large is replaced whether or not it is a
// shape anybody has seen.
func TestNoLoggedValueEscapesTheCeiling(t *testing.T) {
	type unknownToSaguin struct{ Blob string }

	big := strings.Repeat("x", maxLoggedValue*3)
	for _, tc := range []struct {
		name    string
		attr    slog.Attr
		wants   string
		unwants string
	}{
		{"a packet keeps its fields and loses its payload",
			slog.Any("pk", packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
				PacketID:    7, TopicName: "sensors/ward-3", Payload: []byte("SECRETVALUE")}),
			"Publish id=7 qos=1 payload=11 bytes topic=sensors/ward-3", "SECRET"},
		{"a byte slice is a length, whatever its length",
			slog.Any("password", []byte("hunter2")), "7 bytes", "hunter2"},
		{"a long string is cut and says so",
			slog.String("topic", big), "truncated from 3072 bytes", big},
		{"a large value of a type nobody listed is replaced by its size",
			slog.Any("thing", unknownToSaguin{Blob: big}), "not logged", big},
		{"a small value is left exactly as it was",
			slog.String("client", "device-7"), "device-7", "truncated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			slog.New(boundedHandler{slog.NewTextHandler(&buf, nil)}).Info("m", tc.attr)
			got := buf.String()
			if !strings.Contains(got, tc.wants) {
				t.Errorf("the line does not carry %q:\n%s", tc.wants, got)
			}
			if strings.Contains(got, tc.unwants) {
				t.Errorf("the line still carries %q, which is the thing being bounded:\n%s",
					tc.unwants, got)
			}
			if len(got) > maxLoggedValue*2 {
				t.Errorf("the line is %d bytes", len(got))
			}
		})
	}
}

// RFC 0005, invariant 13
//
// **The accumulator is bounded, and what it does when full is the whole
// question.** Client ids arrive from strangers, so an unbounded map is a
// fleet taking a fresh id every boot writing memory into a broker whose
// point is that it runs on a small box.
//
// This drives the ceiling directly rather than through sockets, because
// what is under test is the eviction rule and a thousand real connections
// would test the machine instead.
func TestTheRefusalRecordIsBoundedAndKeepsTheWorst(t *testing.T) {
	keep := func(s string) string { return s }
	f := &refusals{seen: map[string]*refusal{}, ceiling: 3}

	// One repeat offender and two one-offs fill it.
	f.record("repeat", "u", "quota exceeded", keep)
	f.record("repeat", "u", "quota exceeded", keep)
	f.record("once-a", "u", "topic name invalid", keep)
	f.record("once-b", "u", "topic name invalid", keep)

	// A newcomer displaces a one-off, never the repeat offender.
	f.record("newcomer", "u", "not authorized", keep)
	rows, tracked, beyond := f.rows(10)
	if tracked != 3 {
		t.Fatalf("the ceiling is 3 and the record holds %d", tracked)
	}
	if beyond == 0 {
		t.Error("a client was displaced and beyond is 0, so a reader is told it has seen " +
			"everything")
	}
	if rows[0].ClientID != "repeat" || rows[0].Count != 2 {
		t.Errorf("the worst offender is not first: %+v", rows)
	}
	var haveNewcomer bool
	for _, r := range rows {
		if r.ClientID == "newcomer" {
			haveNewcomer = true
		}
		if r.ClientID == "repeat" && r.Count != 2 {
			t.Errorf("the repeat offender's count was reset: %+v", r)
		}
	}
	if !haveNewcomer {
		t.Error("the newcomer was refused while a one-off was still held: a device that " +
			"starts flapping after the record is full would never be seen")
	}

	// **And when every entry is a repeat offender the newcomer is refused
	// rather than evicting one**, which is the honest answer: there are
	// already three genuine offenders to look at.
	f2 := &refusals{seen: map[string]*refusal{}, ceiling: 2}
	for _, id := range []string{"a", "b"} {
		f2.record(id, "u", "r", keep)
		f2.record(id, "u", "r", keep)
	}
	f2.record("c", "u", "r", keep)
	rows2, tracked2, beyond2 := f2.rows(10)
	if tracked2 != 2 || beyond2 == 0 {
		t.Errorf("tracked=%d beyond=%d, want the ceiling held and the refusal counted",
			tracked2, beyond2)
	}
	for _, r := range rows2 {
		if r.ClientID == "c" {
			t.Error("a newcomer displaced a repeat offender")
		}
	}

	// The cap on the response is separate from the ceiling on the record.
	capped, tracked3, _ := f.rows(1)
	if len(capped) != 1 || tracked3 != 3 {
		t.Errorf("asked for 1 row of 3 and got %d rows, tracked %d", len(capped), tracked3)
	}
}

// RFC 0002 "Every reason code, in one place" - the CONNECT table
//
// **Every byte, not a sample.** A 3.1.1 CONNACK carries one of five return
// codes, and a byte outside that set is one the device's own specification
// has no meaning for - measured before the mapping existed, Eclipse Paho's
// 3.1.1 client reported an unmapped `0x9A` as no error at all. The code
// space is 256 values, so this is exhaustive rather than fuzzed: a property
// worth stating about a space that small is worth proving over all of it.
//
// It is also what would notice the substrate sending a code from a path
// nobody has thought about yet - F5 was exactly that, `0x80` from a
// condition with no row, saved only by the clamp.
func TestEveryCONNACKCodeBecomesOneA311ClientDefines(t *testing.T) {
	legacy := &mqtt.Client{}
	legacy.Properties.ProtocolVersion = 4
	modern := &mqtt.Client{}
	modern.Properties.ProtocolVersion = 5

	for code := 0; code <= 0xFF; code++ {
		in := packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Connack},
			ReasonCode:  byte(code),
		}
		if got := legacyConnack(legacy, in).ReasonCode; got > 0x05 {
			t.Fatalf("a CONNACK carrying %#02x reaches a 3.1.1 client as %#02x, and 3.1.1 "+
				"defines five return codes: a byte outside them is a refusal the device "+
				"cannot read", code, got)
		}
		// **And an MQTT 5 client's answer is never touched**, which is what
		// says this narrows nothing for the protocol that can carry it.
		if got := legacyConnack(modern, in).ReasonCode; got != byte(code) {
			t.Fatalf("a CONNACK carrying %#02x reached an MQTT 5 client as %#02x",
				code, got)
		}
	}

	// Success stays success, which the clamp must not touch either.
	ok := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connack},
		ReasonCode:  packets.CodeSuccess.Code,
	}
	if got := legacyConnack(legacy, ok).ReasonCode; got != 0x00 {
		t.Fatalf("an accepted CONNECT reached a 3.1.1 client as %#02x", got)
	}
}

// RFC 0002 "Every reason code, in one place" - the CONNECT table's last
// column
//
// **The test above proves every byte lands on one of the five; this one
// proves each lands on the right one.** The oracle is the RFC's "To a 3.1.1
// client" column, row by row, because a code that is legal and wrong - a
// store failure answered `0x05` - sends whoever holds the device to its
// credentials when the broker is what failed.
func TestEachCONNACKRefusalReachesA311ClientAsTheRFCSays(t *testing.T) {
	legacy := &mqtt.Client{}
	legacy.Properties.ProtocolVersion = 4
	for _, tc := range []struct {
		row       string
		code, got byte
	}{
		{"0x81 Malformed packet", 0x81, 0x05},
		{"0x82 Protocol error", 0x82, 0x05},
		{"0x83 a session store that failed", 0x83, 0x03},
		{"0x85 Client identifier not valid", 0x85, 0x02},
		{"0x85 as the substrate spells it, 0x80", 0x80, 0x02},
		{"0x86 Bad user name or password", 0x86, 0x04},
		{"0x89 Server busy", 0x89, 0x03},
		{"0x90 Topic name invalid", 0x90, 0x05},
		{"0x97 Quota exceeded", 0x97, 0x05},
		{"0x9A Retain not supported", 0x9A, 0x05},
		{"0x9B QoS not supported", 0x9B, 0x05},
	} {
		in := packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Connack},
			ReasonCode:  tc.code,
		}
		if got := legacyConnack(legacy, in).ReasonCode; got != tc.got {
			t.Errorf("%s reached a 3.1.1 client as %#02x; RFC 0002 answers it %#02x",
				tc.row, got, tc.got)
		}
	}
}

// RFC 0002 "Shared subscriptions" - the two refusals that survived
//
// **Two rules meet on one filter, and only their combination is the
// behaviour.** RFC 0002 refuses "`$share` written in any case but the one
// MQTT defines", and it says a 3.1.1 client cannot have a shared
// subscription at all, because its specification gives `$share` no meaning
// and nothing publishes to those topics. Each half is already checked and
// neither check sees the other: the spelling is fuzzed in internal/channel,
// which cannot see a protocol version at all, and the protocol is driven at
// a socket by cases somebody thought of.
//
// **This is the one rule in the broker with both a protocol dimension and
// an input space too large to enumerate**, which is what makes it a fuzz
// target rather than a table. Its neighbours are not: the reason-code
// translations are 256 values and are proved over every one of them, which
// is stronger than sampling and is why they are tests rather than targets.
//
// The oracle is RFC 0002's two sentences rather than the function's own
// expressions, so that a rewrite of either cannot make this agree with
// itself.
func FuzzASharedSubscriptionIsRefusedByProtocolAndBySpelling(f *testing.F) {
	for _, s := range []string{
		"$saguin/queue/jobs", "$SHARE/saguin/jobs/#", "$Share/saguin/jobs/#",
		"$share", "$SHARE", "$share/", "jobs/#", "", "/", "$shareX/a",
		"$sharing/a", "$share//", "#", "$SYS/broker", "$share/saguin",
		"$share//a", "$share/g+/a", "$share/g#/a", "$share/g/", "$share/g/$share/h/a",
		"$share/g/$SHARE", "$share/g/$shared/a", "$share/g/$SYS/#",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, filter string) {
		b := &Broker{}
		for _, tc := range []struct {
			name    string
			version byte
		}{{"a 3.1.1 client", 4}, {"an MQTT 5 client", 5}} {
			cl := &mqtt.Client{}
			cl.Properties.ProtocolVersion = tc.version

			// The first level is what names a shared subscription. A filter
			// whose first level is not that word in some casing has nothing
			// to do with sharing and must be left alone on both protocols -
			// the safety half of the rule, and the one a broadened refusal
			// would break.
			first, rest, named := strings.Cut(filter, "/")
			isShare := strings.EqualFold(first, "$share")
			misspelled := isShare && first != "$share"
			canonical := isShare && first == "$share" && named

			// MQTT 5 section 4.8.2: after `$share/`, a ShareName of at least
			// one character holding no `+` or `#`, a `/`, and a filter - which
			// RFC 0002 says may not begin with `$share` again, in any case.
			group, inner, slashed := strings.Cut(rest, "/")
			innerFirst, _, _ := strings.Cut(inner, "/")
			malformed := canonical && (group == "" || strings.ContainsAny(group, "+#") ||
				!slashed || inner == "" || strings.EqualFold(innerFirst, "$share"))

			want := misspelled || (tc.version < 5 && canonical) || malformed

			if got := b.shareRefusal(cl, filter) != ""; got != want {
				t.Fatalf("%s asking for %q: refused=%v, want %v (misspelled=%v, "+
					"canonical=%v, malformed=%v). RFC 0002 refuses $share in any "+
					"spelling but that one, refuses the canonical form to 3.1.1 "+
					"because that protocol has no shared subscriptions, and refuses "+
					"anything but $share/<ShareName>/<filter> whose filter is not "+
					"itself $share",
					tc.name, filter, got, want, misspelled, canonical, malformed)
			}
		}
	})
}

// RFC 0002 "Which versions may connect", RFC 0005 "The health endpoint"
//
// **The startup line is the one place an operator learns what the door
// does**, and it was reaching them unchecked: nothing in the suite called
// this, and checking it by hand confirmed it.
//
// **Read back from the server's own gate rather than restated from the
// configuration**, which is the whole reason it exists - the two can
// disagree and only one of them is what the door does. So the cases below
// go through NewServer with a real configuration rather than setting the
// capability directly: a test that sets the field it then reads would pass
// against a NewServer that had stopped applying it.
func TestTheStartupLineNamesWhatTheDoorAdmits(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name string
		min  byte
		want string
	}{
		{"the default admits both", 0, "MQTT 5, MQTT 3.1.1"},
		{"3.1.1 admits both", 4, "MQTT 5, MQTT 3.1.1"},
		{"5 keeps the older fleet out", 5, "MQTT 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, err := channel.NewRegistry(nil)
			if err != nil {
				t.Fatalf("registry: %v", err)
			}
			lim := config.Resolved{MinProtocolVersion: tc.min}
			srv, _, err := NewServer(reg, lim, quiet)
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			if got := AdmittedProtocols(srv); got != tc.want {
				t.Errorf("a broker configured with min_protocol_version %d says it "+
					"admits %q, want %q. This line is where an operator upgrading "+
					"into a release that admits a version the last one turned away "+
					"finds out", tc.min, got, tc.want)
			}
		})
	}

	// **A server it cannot read is said to be unknown, not assumed open.**
	// The line is written in main before anything else has run, so a nil
	// here is a startup ordering mistake - and naming a protocol set the
	// broker might not have is worse than admitting ignorance.
	if got := AdmittedProtocols(nil); got != "unknown" {
		t.Errorf("a server that could not be read says %q, want \"unknown\"", got)
	}
}

// RFC 0005 "No path in Sagüin holds that lock across disk I/O"
//
// A scrape asks every store what it holds, and must ask with the broker's
// lock let go. A store's figures sit behind its own mutex, which a writer
// holds across a commit, and on sqlite some of them are queries: asked under
// b.mu, one scrape held the broker's lock across disk I/O once per channel.
//
// Each store here checks, on every question the scrape asks it, that it
// could take b.mu at that moment - nothing else in this test holds it, so
// failing to means the scrape does. The questions are counted, or a scrape
// that stopped asking would pass.
func TestAScrapeAsksNoStoreUnderTheBrokersLock(t *testing.T) {
	b := metricsBroker(t)
	var asked int
	var underLock []string
	check := func(what string) {
		asked++
		if b.mu.TryLock() {
			b.mu.Unlock()
			return
		}
		underLock = append(underLock, what)
	}
	b.logs["events"] = lockCheckedLog{b.logs["events"], check}
	b.latest["state"] = lockCheckedLatest{b.latest["state"], check}
	b.queues["jobs"] = lockCheckedQueue{b.queues["jobs"], check}

	if out := b.catalogue(); !strings.Contains(string(out), "saguin_channel_floor_offset") {
		t.Fatal("the scrape carried no channel series, so it asked the stores nothing worth checking")
	}
	if asked < 6 {
		t.Fatalf("the scrape asked the three stores %d questions, which cannot be all of them: "+
			"this check is not seeing the scrape", asked)
	}
	if len(underLock) > 0 {
		t.Errorf("the scrape asked %v with the broker's lock held; every publish waits for "+
			"that, and on sqlite each is disk I/O", underLock)
	}
}

type lockCheckedLog struct {
	LogStore
	check func(string)
}

func (l lockCheckedLog) Next() uint64  { l.check("log Next"); return l.LogStore.Next() }
func (l lockCheckedLog) Floor() uint64 { l.check("log Floor"); return l.LogStore.Floor() }
func (l lockCheckedLog) Bytes() int64  { l.check("log Bytes"); return l.LogStore.Bytes() }
func (l lockCheckedLog) LowestPosition() (uint64, int) {
	l.check("log LowestPosition")
	return l.LogStore.LowestPosition()
}

type lockCheckedLatest struct {
	LatestStore
	check func(string)
}

func (l lockCheckedLatest) Len() int {
	l.check("latest Len")
	return storeBehind(l.LatestStore).(topicCounter).Len()
}

type lockCheckedQueue struct {
	QueueStore
	check func(string)
}

func (q lockCheckedQueue) Bytes() int64 { q.check("queue Bytes"); return q.QueueStore.Bytes() }
func (q lockCheckedQueue) Depth() (int, int) {
	q.check("queue Depth")
	return q.QueueStore.Depth()
}
