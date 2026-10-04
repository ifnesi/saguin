package broker

import (
	"compress/gzip"
	"errors"
	"fmt"
	"net/http"
	rtmetrics "runtime/metrics"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// The metrics catalogue is closed and RFC 0005 holds it. A name here is a
// promise: once somebody's dashboard reads it, renaming it breaks their
// alerting silently - the panel goes blank and nothing anywhere says why.
//
// No metrics library. For a closed catalogue the exposition format is a
// handful of formatted lines, where prometheus/client_golang would be the
// largest dependency in a tree whose binary the README sizes at around
// 20 MB, and would bring a registry, a collector model and a goroutine for
// problems a fixed list of names does not have.

// scrape is one rendered catalogue and when it was taken.
type scrape struct {
	body []byte
	at   time.Time
}

// metricsHandler answers /metrics.
//
// **The numbers are collected under the broker's lock and formatted
// outside it**, which is the discipline the snapshot and the retention
// sweep already follow. A scrape that formatted under the lock would stall
// every publish for as long as it took, and would fail the health probe
// sitting on the same listener - and the flapping would be telling the
// truth.
func (b *Broker) metricsHandler(minScrape time.Duration) http.Handler {
	var (
		mu   sync.Mutex
		last scrape
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "", http.StatusMethodNotAllowed)
			return
		}

		now := time.Now()
		mu.Lock()
		// **A scrape arriving early is answered, never refused.**
		// Prometheus records a refused scrape as a failed one: a gap in the
		// graph and, usually, an alert. A defence that turns into the
		// operator's incident is not a defence, so the early scrape gets
		// the previous bytes and nobody is told off.
		if last.body == nil || now.Sub(last.at) >= minScrape {
			last = scrape{body: b.catalogue(), at: now}
		}
		body := last.body
		mu.Unlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Gzip costs more than generating the page and returns about a
		// fifteenth of the bytes (RFC 0005's table: 493 KiB to 34 KiB), which is the right trade on any link an
		// operator would scrape across and the wrong one on none. Prometheus
		// asks for it by default.
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			zw := gzip.NewWriter(w)
			_, _ = zw.Write(body)
			_ = zw.Close()
			return
		}
		_, _ = w.Write(body)
	})
}

// catalogue renders the whole catalogue.
//
// It takes the broker's lock to copy what it needs and releases it before
// formatting a byte. Nothing between those two points does any work beyond
// reading fields.
func (b *Broker) catalogue() []byte {
	numbers := b.collect()
	if f := afterCollect.Load(); f != nil {
		(*f)(b)
	}
	var m metricWriter
	numbers.render(&m)
	numbers.renderCounters(&m)
	renderRuntime(&m)
	return m.bytes()
}

// afterCollect is a test seam, nil in production: when set it runs in
// catalogue between the collect, which takes the broker's lock, and the
// formatting, which must not. A test holds the scrape there, takes the lock
// itself, and lets the formatting go: one that asked for the lock could not
// finish (TestAScrapeDoesNotFormatUnderTheBrokersLock). Waiting for another
// goroutine to win the lock while scrapes ran proved nothing at one P, where
// it never ran.
var afterCollect atomic.Pointer[func(b *Broker)]

// runtimeSeries is the Go runtime's own figures the catalogue serves, each
// with the runtime/metrics key it is read from (RFC 0005 "The Go runtime").
// A cumulative key is a counter and every other a gauge
// (TestEveryRuntimeSeriesIsItsRuntimeMetricsSource). signed is a key the
// runtime writes as an unsigned integer that means -1 at its maximum.
var runtimeSeries = []struct {
	name, kind, key, help string
	signed                bool
}{
	{"saguin_go_heap_live_bytes", "gauge", "/gc/heap/live:bytes",
		"Heap the last garbage collection found reachable, the 8 MB heap floor included.", false},
	{"saguin_go_heap_goal_bytes", "gauge", "/gc/heap/goal:bytes",
		"Heap size at which the next garbage collection starts.", false},
	{"saguin_go_gc_cycles_total", "counter", "/gc/cycles/total:gc-cycles",
		"Garbage collections completed since start; its rate is collections a second.", false},
	{"saguin_go_gc_cpu_seconds_total", "counter", "/cpu/classes/gc/total:cpu-seconds",
		"CPU seconds spent collecting garbage since start, as the Go runtime estimates it.", false},
	{"saguin_go_gc_assist_cpu_seconds_total", "counter", "/cpu/classes/gc/mark/assist:cpu-seconds",
		"CPU seconds the broker's own goroutines spent helping the collector since start.", false},
	{"saguin_go_stack_bytes", "gauge", "/memory/classes/heap/stacks:bytes",
		"Memory held for goroutine stacks.", false},
	{"saguin_go_allocated_bytes_total", "counter", "/gc/heap/allocs:bytes",
		"Heap bytes allocated since start.", false},
	{"saguin_go_allocated_objects_total", "counter", "/gc/heap/allocs:objects",
		"Heap objects allocated since start.", false},
	{"saguin_go_gogc_percent", "gauge", "/gc/gogc:percent",
		"The effective GOGC; -1 when garbage collection is off.", true},
	{"saguin_go_memory_limit_bytes", "gauge", "/gc/gomemlimit:bytes",
		"The effective GOMEMLIMIT. With none set it reads math.MaxInt64, written 9.223372036854776e+18, which a dashboard can take as no limit.", false},
}

// renderRuntime writes the Go runtime's figures, **read here and nowhere
// else**: at a scrape the catalogue is recomputed for, so they cost what one
// runtime/metrics read costs - microseconds, measured in
// BenchmarkRuntimeRead - and nothing while nobody asks
// (TestTheRuntimeIsReadOnlyAtScrape). runtime/metrics, not
// runtime.ReadMemStats, which stops the world.
func renderRuntime(m *metricWriter) {
	samples := make([]rtmetrics.Sample, len(runtimeSeries))
	for i, s := range runtimeSeries {
		samples[i].Name = s.key
	}
	rtmetrics.Read(samples)
	for i, s := range runtimeSeries {
		var v float64
		switch samples[i].Value.Kind() {
		case rtmetrics.KindUint64:
			u := samples[i].Value.Uint64()
			v = float64(u)
			if s.signed {
				v = float64(int64(u))
			}
		case rtmetrics.KindFloat64:
			v = samples[i].Value.Float64()
		default:
			continue // a key this Go does not have: no line rather than a false zero
		}
		m.declare(s.name, s.kind, s.help)
		m.value(s.name, v)
	}
}

// metricWriter writes the Prometheus text exposition format, with the
// HELP and TYPE lines a name needs and in the order they are declared.
type metricWriter struct {
	b strings.Builder
	// seen stops a name being declared twice, which a scraper rejects the
	// whole page for.
	seen map[string]bool
}

func (m *metricWriter) bytes() []byte { return []byte(m.b.String()) }

// declare writes the HELP and TYPE lines for a name, once.
func (m *metricWriter) declare(name, kind, help string) {
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if m.seen[name] {
		return
	}
	m.seen[name] = true
	fmt.Fprintf(&m.b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

// value writes one sample. labels are pairs, and every value is escaped:
// a channel name comes from the operator's file and cannot carry a quote
// today, but a formatter that assumes that is one edit away from producing
// a page no scraper will parse.
func (m *metricWriter) value(name string, v float64, labels ...string) {
	m.b.WriteString(name)
	if len(labels) > 0 {
		m.b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				m.b.WriteByte(',')
			}
			m.b.WriteString(labels[i])
			m.b.WriteString(`="`)
			m.b.WriteString(escapeLabel(labels[i+1]))
			m.b.WriteByte('"')
		}
		m.b.WriteByte('}')
	}
	m.b.WriteByte(' ')
	m.b.WriteString(formatValue(v))
	m.b.WriteByte('\n')
}

// labelEscaper is built once, and that is the whole of why it is a package
// variable. strings.NewReplacer compiles a lookup structure, so building one
// inside escapeLabel built one per label of every sample - twenty thousand
// of them in a single scrape at a thousand channels, and most of what a
// scrape cost. Measured before and after: 96ms against 8.6ms at that size.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// escapeLabel applies the three escapes the exposition format defines.
func escapeLabel(v string) string {
	return labelEscaper.Replace(v)
}

// formatValue writes a number the way the format wants it: an integer
// without a decimal point, so that a byte count does not arrive as
// scientific notation and lose its last digits.
func formatValue(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// sortedKeys puts a map's keys in a fixed order, so that two scrapes of one
// broker produce the same bytes and a diff between them is a change rather
// than a map iteration. The startup log uses it for the same reason: two
// runs of one configuration read the same way.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// numbers is one scrape's worth of state, copied out of the broker so that
// formatting touches nothing under a lock.
type numbers struct {
	version  string
	brokerID string
	uptime   float64

	connections   float64
	subscriptions float64
	maxSession    float64

	// What crossed the MQTT listeners, in both directions, since this
	// process started. **Bytes are everything on the wire** - a publish's
	// payload, the acknowledgement behind it, a keepalive, a connection
	// being set up - because that is what an operator sizing a link needs
	// and there is no cheaper honest number. Publishes are the packets
	// themselves: arrivals include the ones saguin went on to refuse, and
	// deliveries include a queue record handed out a second time.
	//
	// The bridge's traffic is in none of them. That link is saguin acting
	// as a client of another broker through a different library, which
	// these counters never see; saguin_bridge_received_total counts its
	// records.
	bytesIn, bytesOut       float64
	publishesIn, deliveries float64

	// **What a subscriber too far behind costs, and how far behind it is.**
	// deliveriesRefused counts a QoS 1 delivery the broker could not send
	// because that client had not acknowledged enough of what it already
	// had, or had run through its packet identifiers. It is a different
	// loss from saguin_deliveries_dropped_total, which is the client's
	// outbound buffer overflowing - the same complaint, one layer apart,
	// and only the other one had a counter.
	//
	// **There is no gauge beside it**, and the reason is measured rather
	// than argued: the substrate keeps an in-flight count, and saguin puts
	// packets into a client's in-flight set itself without telling it. The
	// substrate decrements on every acknowledgement all the same, so the
	// number falls below what it was never raised for - it read -3 on a
	// broker that had delivered three records. RFC 0005 "What is
	// deliberately not measured" says so, beside the substrate's retained
	// count, which is wrong here for the same shape of reason.
	deliveriesRefused float64

	// The causes saguin_session_deliveries_dropped_total splits out, and what
	// sessions hold.
	packetIDsExhausted, sessionQueueFull    float64
	tooLarge                                float64
	sessionQueueMessages, sessionQueueBytes float64

	// Sessions this broker holds that nobody is connected to. Devices come
	// and go and a durable session outlives the connection by design, so
	// this is a fleet's shape rather than a fault - but a number that only
	// climbs is sessions nothing will ever come back for, held until their
	// expiry.
	sessionsOffline float64
	// sessionsStorageFull is counters.sessionsStorageFull, read once.
	sessionsStorageFull float64

	// sessionsExpiredWhileStopped is counters.sessionsExpiredWhileStopped, read once.
	sessionsExpiredWhileStopped float64

	// sessionsRefusedAtStart is counters.sessionsRefusedAtStart, read once.
	sessionsRefusedAtStart float64

	// sessionsRestored is counters.sessionsRestored, read once.
	sessionsRestored float64

	// retained is how many retained messages this broker holds on
	// broadcast topics, and hasRetained says it has such a store at all.
	// Every broker the binary starts has one (cmd/saguin always keeps it);
	// only an in-process harness that attaches none carries no series, since
	// a zero would read as a store that is empty.
	retained    float64
	hasRetained bool

	// Exactly-once publishes: how many are held now, how many
	// have been taken in and never released, and the per-client allowance
	// the operator configured. hasQoS2 says the broker offers exactly-once
	// at all. Every broker the binary starts does (cmd/saguin always attaches
	// the store); only an in-process harness that attaches none carries no
	// series, for the reason saguin_retained_messages carries none without
	// its store: a zero would read as a feature that is on and idle.
	qos2Held       float64
	qos2Abandoned  float64
	qos2MaxPerClnt float64
	hasQoS2        bool

	connectionsTotal     float64
	sessionsShortened    float64
	broadcast            float64
	broadcastUnmatched   float64
	dropped              float64
	expired              float64
	sharedNoMember       float64
	sharesHeld           float64
	sharesDrained        float64
	sharesDroppedFull    float64
	sharesDroppedNoMem   float64
	sharesDroppedExpired float64
	sharesDroppedStorage float64
	sharesDroppedMember  float64
	sharesDroppedAuth    float64
	// deliveriesStorageFull is counters.deliveriesStorageFull, read once.
	deliveriesStorageFull float64
	refused               []struct {
		reason string
		n      uint64
	}
	subscriptionsRefused []struct {
		reason string
		n      uint64
	}
	connectionsRefused []struct {
		reason string
		n      uint64
	}
	// willsPublished is the Wills this broker published, by what made each
	// due, and willsWaiting how many are waiting out a delay right now.
	willsPublished []struct {
		cause string
		n     uint64
	}
	willsWaiting   float64
	willsCancelled float64
	// byProtocol is clients connected now, keyed by the MQTT version they
	// connected with. Two keys at most, both saguin's own.
	byProtocol  map[string]float64
	storageErrs []struct {
		provider string
		n        uint64
	}

	channels  []channelNumbers
	providers []providerNumbers
	bridges   []bridgeNumbers
}

type bridgeNumbers struct {
	name, peer       string
	connected        float64
	stopped          float64
	received         float64
	reconnects       float64
	forwarded        float64
	loopsSkipped     float64
	peerRefused      float64
	broadcastRefused float64
	unmappable       float64
	liveDropped      float64
	superseded       float64
	queueFull        float64
	noRule           float64
	inUnmappable     float64
	neverAccepted    float64
}

type channelNumbers struct {
	name, kind, provider string
	// filter is the topic filter this channel claims. It is a label rather
	// than a series because it is a property of the channel and never a
	// number, and it is here at all because placement stopped being
	// derivable from the name: a reader that knows a channel exists and not
	// which topics it holds cannot subscribe to it, cannot attribute a
	// record to it, and cannot tell an operator where to look. The demo's
	// viewer builds its subscriptions from exactly this.
	filter             string
	next, floor, bytes float64

	// topics is how many topics a latest channel holds a current value
	// for, and topicsKnown says its store answered. Two fields rather than
	// a zero, because zero is an answer here - a latest channel nobody has
	// published to holds nothing - and "the store does not count" is not.
	topics      float64
	topicsKnown bool

	lowestPosition float64
	// consumers is how many durable consumers have a stored position on
	// this channel - the denominator lowestPosition is a single reading
	// from. It rides the same pass, so it costs nothing beyond it.
	consumers float64

	// partitioned is how many subscribers declared a slice on this channel.
	partitioned      float64
	published        float64
	retentionRemoved float64
	positionLost     float64
	superseded       float64

	queue     bool
	depth     float64
	inflight  float64
	delivered float64
	acked     float64
	returned  float64
	redeliver float64
	deadLetr  float64
	expired   float64
	// retainIgnored is retained publishes a queue took as ordinary work.
	// The flag asks for a store and a queue is not one, so it is dropped
	// rather than refused - and this is the only place a producer's
	// mistaken assumption about it is visible at all.
	retainIgnored float64
}

// **What a latest channel holds, and both stores answer.** Such a channel
// keeps one value per topic, so its topics and its records are the same
// count. The memory store reads the size of the map it is already holding;
// the sqlite store reads a count seeded when the channel opened and
// advanced by every write since. Neither asks the database anything here,
// which is what RFC 0005 requires of a scrape.
//
// **`next − floor` cannot stand in**, which is why this is asked of the
// store at all. A replacement takes a new offset, so on a latest channel
// that subtraction counts writes rather than holdings: a hundred updates to
// one topic advance next by a hundred while the channel still holds one
// value.
//
// It stays an interface rather than a method on LatestStore, because what
// it asks for is a number the store already has. A store that would have
// to go and find one does not implement it, and its channels carry no
// series rather than a scan.
type topicCounter interface {
	Len() int
}

// Both stores, named here so that a change to either signature fails the
// build rather than removing a metric quietly.
var (
	_ topicCounter = (*store.Latest)(nil)
	_ topicCounter = (*sqlite.Latest)(nil)
)

type providerNumbers struct {
	name, kind      string
	bytes, maxBytes float64
	// commits is nil for a provider that does not collect publishes into
	// shared transactions, which is what keeps the series absent rather
	// than zero for one that never could.
	commits *store.CommitStats
}

// providerMeasurements is what each storage provider says it holds and what
// bounds it, asked without the broker's lock held.
//
// **Asked of the provider, not of the quota.** Reading quotas meant reading
// the one mechanism that bounds a memory provider and missing the one that
// bounds a sqlite provider entirely, so a database refusing publishes at its
// max_page_count reported that it held nothing and had no bound - a fault
// rendering as health.
//
// The names and the references come out under the lock; the questions are
// asked after it is released, because a sqlite provider answers by querying
// its database and that can wait on a write.
func (b *Broker) providerMeasurements() []providerNumbers {
	type asking struct {
		name, kind string
		of         ProviderMeasure
	}
	b.mu.Lock()
	ask := make([]asking, 0, len(b.providerKinds))
	for _, name := range sortedKeys(b.providerKinds) {
		ask = append(ask, asking{name: name, kind: b.providerKinds[name], of: b.measures[name]})
	}
	b.mu.Unlock()

	out := make([]providerNumbers, 0, len(ask))
	for _, a := range ask {
		pn := providerNumbers{name: a.name, kind: a.kind}
		if a.of != nil {
			pn.bytes, pn.maxBytes = float64(a.of.Bytes()), float64(a.of.MaxBytes())
			// Asked here, outside the broker's lock, for the reason every
			// question on this path is: a provider's own mutexes are not
			// the scraper's to take under b.mu.
			if c, ok := a.of.(CommitCounter); ok {
				st := c.CommitStats()
				pn.commits = &st
			}
		}
		out = append(out, pn)
	}
	return out
}

// collect copies what a scrape needs, under the broker's lock, and does
// nothing else there.
//
// **Every number here is a field a store already holds** (RFC 0005 "What a
// metric is allowed to cost"). Nothing counts rows and nothing walks a
// history: a channel's record count is `next − floor`, which is exact
// because retention only ever removes a prefix and advances the floor past
// it, so no record is ever missing from the middle.
func (b *Broker) collect() numbers {
	n := numbers{
		version:  b.version,
		brokerID: b.brokerID,
		uptime:   time.Since(b.startedAt).Seconds(),
	}
	// **Read from the substrate's own counters, which it maintains anyway.**
	// It keeps these as atomics on the paths that change them - the same
	// numbers its `$SYS` tree published before saguin silenced that tree
	// (RFC 0005). Silencing it stopped the publishing, not the counting, so
	// asking here costs an atomic load and duplicates nothing.
	//
	// Subscriptions in particular was saguin's own sum over its
	// subscription map, taken under the broker's lock. That was the wrong
	// number as well as the slower one: saguin tracks channel subscriptions
	// only, so a broadcast subscriber did not appear, and the lock it was
	// taken under is the one the health probe on this same listener waits
	// for.
	if b.srv != nil {
		// **By protocol version, counted from the client table rather than
		// kept by increments** (RFC 0005 `saguin_connections_by_protocol`).
		// A gauge maintained on connect and disconnect is a gauge that
		// drifts: `saguin_subscriptions` landed on -1 for exactly that
		// reason, one decrement firing where nothing had been removed.
		// Counting what is there cannot drift, and the cost is one walk of
		// the table per scrape - bounded by `max_connections`, on a route
		// with a minimum scrape interval.
		n.byProtocol = map[string]float64{}
		for _, cl := range b.srv.Clients.GetAll() {
			// **The substrate's own inline client is in this table**, and
			// it is nobody's session: it is how the server publishes on its
			// own behalf. Counting it made a broker with two clients
			// connected report one session already offline.
			if cl.Net.Inline {
				continue
			}
			// What every session holds of deliveries it has not acknowledged,
			// connected or not: the same walk, two locked reads a session.
			n.sessionQueueMessages += float64(cl.State.Inflight.Messages())
			n.sessionQueueBytes += float64(cl.State.Inflight.Bytes())
			if cl.Closed() {
				// **Counted here rather than subtracted**, which is the
				// same choice this loop already makes for the protocol
				// split. The substrate derives a session total and takes
				// the connected ones off it, inside the `$SYS` tree saguin
				// silences - and a difference of two numbers read a moment
				// apart is how a count of sessions goes negative. A client
				// left in the table after its connection ended is a session
				// held: the substrate deletes a clean one on disconnect and
				// keeps a durable one until it expires.
				n.sessionsOffline++
				continue
			}
			switch v := cl.Properties.ProtocolVersion; {
			case v >= 5:
				n.byProtocol["5"]++
			case v == 4:
				n.byProtocol["3.1.1"]++
			}
		}
		// And what each session is owed from the broadcast log, which its
		// in-flight table leaves out (packets.Packet.Bounded): each delivery
		// counted once, as the bound that holds it counts it. Outside b.mu,
		// as every owed list's lock is.
		if d := b.broadcastDrain(); d != nil {
			m, by := d.queued()
			n.sessionQueueMessages += float64(m)
			n.sessionQueueBytes += float64(by)
		}
		n.connections = float64(b.srv.Info.ClientsConnected.Load())
		n.subscriptions = float64(b.srv.Info.Subscriptions.Load())
		// **Four more from the same place, and the reason the rule says to
		// read rather than to keep.** The substrate adds to these as it
		// reads a socket and as it writes one; a tally of saguin's own
		// beside them would be a second number for one fact, and the
		// throughput a dashboard drew would be whichever of the two had
		// not yet gone wrong.
		n.bytesIn = float64(b.srv.Info.BytesReceived.Load())
		n.bytesOut = float64(b.srv.Info.BytesSent.Load())
		n.publishesIn = float64(b.srv.Info.MessagesReceived.Load())
		n.deliveries = float64(b.srv.Info.MessagesSent.Load())
		n.deliveriesRefused = float64(b.srv.Info.InflightDropped.Load())
		n.sessionsStorageFull = float64(b.counted.sessionsStorageFull.Load())
		n.sessionsExpiredWhileStopped = float64(b.counted.sessionsExpiredWhileStopped.Load())
		n.sessionsRefusedAtStart = float64(b.counted.sessionsRefusedAtStart.Load())
		n.sessionsRestored = float64(b.counted.sessionsRestored.Load())
		n.packetIDsExhausted = float64(b.srv.Info.PacketIDsExhausted.Load())
		n.sessionQueueFull = float64(b.srv.Info.SessionQueueDropped.Load())
		n.tooLarge = float64(b.srv.Info.DeliveriesTooLarge.Load())
		n.subscriptionsRefused = subscribeRefusals(&b.srv.Info.SubscribeRefused)

		n.maxSession = float64(b.srv.Options.Capabilities.MaximumSessionExpiryInterval)
	}
	n.connectionsRefused = b.counted.connectionsRefused()
	n.willsPublished = b.counted.willsByCause()
	n.willsCancelled = float64(b.counted.willsCancelled.Load())
	b.mu.Lock()
	n.willsWaiting = float64(len(b.pendingWills))
	b.mu.Unlock()
	n.connectionsTotal = float64(b.counted.connections.Load())
	n.sessionsShortened = float64(b.counted.sessionsShortened.Load())
	n.broadcast = float64(b.counted.broadcast.Load())
	n.broadcastUnmatched = float64(b.counted.broadcastUnmatched.Load())
	n.dropped = float64(b.counted.dropped.Load())
	n.expired = float64(b.counted.expired.Load())
	n.sharedNoMember = float64(b.counted.sharedNoMember.Load())
	n.sharesHeld = float64(b.counted.sharesHeld.Load())
	n.sharesDrained = float64(b.counted.sharesDrained.Load())
	n.sharesDroppedFull = float64(b.counted.sharesDroppedFull.Load())
	n.sharesDroppedNoMem = float64(b.counted.sharesDroppedNoMember.Load())
	n.sharesDroppedExpired = float64(b.counted.sharesDroppedExpired.Load())
	n.sharesDroppedStorage = float64(b.counted.sharesDroppedStorageFull.Load())
	n.sharesDroppedMember = float64(b.counted.sharesDroppedMemberEnded.Load())
	n.sharesDroppedAuth = float64(b.counted.sharesDroppedNotAuthorized.Load())
	n.deliveriesStorageFull = float64(b.counted.deliveriesStorageFull.Load())
	// **Here rather than beside the two QoS 2 gauges below**, which is
	// where it was missing from and where it does not belong: those are
	// read off the pending store itself and are absent with no store, and
	// this is a tally the broker keeps whether or not one is configured.
	// Copied with the other tallies, which is the line a reader looks
	// along and the one the next counter will be added to.
	n.qos2Abandoned = float64(b.counted.qos2Abandoned.Load())
	// Outside b.mu: these have mutexes of their own, and taking one under
	// the broker's would put a refusal or a storage failure behind every
	// publish.
	n.refused = b.counted.refusals()
	n.storageErrs = b.counted.storageFailures()

	// **The providers, before the lock, for the same reason and a sharper
	// one.** A provider measures itself, and a sqlite one does it by asking
	// its database - over a pool of exactly one connection, so a write in
	// flight makes that question wait for the write. Under b.mu that wait
	// is not the scraper's: pumpAll takes b.mu for every record stored, so
	// one HTTP request would put one provider's commit latency in front of
	// every publisher on every provider, including those with no database
	// at all. Measured before this moved: a memory-channel publisher's
	// acknowledgement went from 0.001s to 1.897s while a scrape waited on
	// another provider's write, which is invariant 16 - a publisher waits
	// on storage and on nothing else - broken by an observer.
	//
	// The map is read under the lock and the asking is done outside it, so
	// the reference is safe and the query is nobody's but the scraper's.
	n.providers = b.providerMeasurements()

	// **And the lowest stored position, for exactly the same reason.** It
	// was a field until the sqlite store stopped keeping one; it is now
	// `SELECT min("offset") FROM positions WHERE channel = ?` on that same
	// one-connection pool, so asking it under b.mu would put a channel's
	// storage in front of every publisher on every provider - the hazard
	// the paragraph above measured at 1.897s, reintroduced by a metric that
	// used to be free.
	//
	// Nothing observed a stall here: a scrape a second against an
	// always-open batch stayed under a millisecond. The call is outside the
	// lock anyway, because the reasoning that moved the provider question
	// does not depend on which question it is, and a rule that holds only
	// while nobody has provoked it is not one.
	lowest, consumers := b.lowestPositions()
	partitioned := b.PartitionedConsumers()

	// **Only the references are taken under the lock**, and every store is
	// asked after it is let go. A store's own figures are behind its own
	// mutex, which a writer holds across a commit, and on sqlite some are
	// queries - Log.Floor and Latest.Len among them. Asked under b.mu, a
	// scrape held the broker's lock across disk I/O once per channel,
	// which RFC 0005 says no path does, and every publish waited for it.
	type behind struct {
		c  *channel.Channel
		lg LogStore
		q  QueueStore
		lt LatestStore
	}
	b.mu.Lock()
	retained, qos2Offered, qos2Max := b.retained, b.maxQoS >= maxQoSWithStore, b.qos2MaxPerClient
	names := sortedKeys(b.reg.All())
	chans := make([]behind, 0, len(names))
	for _, name := range names {
		chans = append(chans, behind{c: b.reg.Get(name), lg: b.logs[name], q: b.queues[name], lt: b.latest[name]})
	}
	bridges := slices.Clone(b.bridgeStats)
	b.mu.Unlock()

	// **How many retained messages this broker holds**, which the substrate
	// also has a field for and is wrong about: its number counts its own
	// store, which saguin empties as fast as it fills. This one is saguin's
	// store, which is the one an operator's retained messages are actually
	// in - and it is a latest channel, so its count is the field read every
	// other latest channel's is.
	if retained != nil {
		if tc, ok := storeBehind(retained).(topicCounter); ok {
			n.retained, n.hasRetained = float64(tc.Len()), true
		}
	}

	// **Read from the broker's own map of held exchanges**, which a start
	// rebuilds and every hold and release keeps, so this is a read rather
	// than a count - RFC 0005 refuses a metric that costs a scan, and asking
	// every channel's store would be one. Bytes are not published here: a
	// held publish sits in its channel's provider, or the broadcast log's,
	// and saguin_provider_bytes already measures it.
	if qos2Offered {
		b.heldMu.Lock()
		n.qos2Held, n.hasQoS2 = float64(len(b.held)), true
		b.heldMu.Unlock()
		n.qos2MaxPerClnt = float64(qos2Max)
	}

	for _, ch := range chans {
		c, name := ch.c, ch.c.Name
		cn := channelNumbers{name: name, kind: string(c.Type), provider: c.Storage,
			filter: c.Filter}
		// Every channel type, because partitioning is allowed on all of
		// them - a queue only ever reads zero, since a declaration on its
		// form is refused at SUBSCRIBE.
		cn.partitioned = float64(partitioned[name])
		switch {
		case ch.lg != nil:
			lg := ch.lg
			cn.next, cn.floor, cn.bytes = float64(lg.Next()), float64(lg.Floor()), float64(lg.Bytes())
			cn.lowestPosition = lowest[name]
			cn.consumers = consumers[name]
		case ch.q != nil:
			// A queue has no floor and no record count derivable from one:
			// resolution removes from the middle rather than the front, so
			// `next - floor` says nothing here and the store keeps the depth
			// itself - a field advanced from committed transactions rather
			// than a count(*) per scrape.
			cn.bytes = float64(ch.q.Bytes())
			total, inflight := ch.q.Depth()
			cn.depth, cn.inflight = float64(total), float64(inflight)
		case ch.lt != nil:
			// Asked through storeBehind because the store the broker holds
			// is the wrapper that counts storage failures, and what can
			// answer this is the store inside it - the same unwrapping the
			// snapshot does to find a store with records to hand over.
			if tc, ok := storeBehind(ch.lt).(topicCounter); ok {
				cn.topics, cn.topicsKnown = float64(tc.Len()), true
			}
			// **Bytes stay absent on both providers**, and for a reason the
			// count does not share. A write here replaces a value, so the
			// change in size is a difference and measuring it means reading
			// the outgoing value first - half again the cost of every
			// write, measured. The memory store pays that already to keep
			// its provider's bound; the sqlite store bounds itself by the
			// database file's pages and has never needed a record's size.
			// The count escapes that because a replacement does not change
			// it: only a topic arriving or leaving does, and both of those
			// are already known where they happen.
		}
		if cc := b.counted.forChannel(name); cc != nil {
			cn.published = float64(cc.published.Load())
			cn.retentionRemoved = float64(cc.retentionRemoved.Load())
			cn.positionLost = float64(cc.positionLost.Load())
			cn.superseded = float64(cc.latestSuperseded.Load())
			cn.queue = c.Type == channel.Queue
			cn.delivered = float64(cc.delivered.Load())
			cn.acked = float64(cc.acknowledged.Load())
			cn.returned = float64(cc.returned.Load())
			cn.redeliver = float64(cc.redelivered.Load())
			cn.deadLetr = float64(cc.deadLettered.Load())
			cn.expired = float64(cc.expired.Load())
			cn.retainIgnored = float64(cc.retainIgnored.Load())
		}
		n.channels = append(n.channels, cn)
	}

	// **Over the providers the operator configured, not over the ones that
	// happen to have a quota.** A quota exists only where a provider states
	// a max_bytes, so iterating those reported nothing at all for an
	// unbounded provider - which is most of them, and exactly the one an
	// operator would go looking for. RFC 0005 says max_bytes is "zero for no
	// bound", which is a provider that is listed and says zero rather than a
	// provider that is absent.

	for _, br := range bridges {
		bn := bridgeNumbers{name: br.Name(), peer: br.Peer(),
			received: float64(br.Received()), reconnects: float64(br.Reconnects()),
			forwarded: float64(br.Forwarded()), loopsSkipped: float64(br.LoopsSkipped()),
			peerRefused: float64(br.PeerRefused()), broadcastRefused: float64(br.BroadcastRefused()),
			unmappable: float64(br.Unmappable()), liveDropped: float64(br.LiveDropped()),
			superseded: float64(br.Superseded()), queueFull: float64(br.UnstoredQueueFull()),
			noRule: float64(br.UnstoredNoRule()), inUnmappable: float64(br.UnstoredUnmappable()),
			neverAccepted: float64(br.UnstoredNeverAccepted())}
		if br.Connected() {
			bn.connected = 1
		}
		if br.Stopped() {
			bn.stopped = 1
		}
		n.bridges = append(n.bridges, bn)
	}
	return n
}

// lowestPositions asks every append channel for its lowest stored consumer
// position, outside b.mu.
//
// The map is read under the lock and the asking is done outside it, the same
// shape providerMeasurements uses and for the same reason: on a sqlite
// provider this is a query over a pool of one connection, and a write in
// flight makes it wait.
//
// A channel nobody holds a position on answers zero, which the caller turns
// into an absent series rather than a nought - RFC 0005 is explicit that a
// dashboard reading nothing is better off than one reading a wrong number.
func (b *Broker) lowestPositions() (map[string]float64, map[string]float64) {
	b.mu.Lock()
	ask := make(map[string]LogStore, len(b.logs))
	for name, lg := range b.logs {
		ask[name] = lg
	}
	b.mu.Unlock()

	lowest := make(map[string]float64, len(ask))
	consumers := make(map[string]float64, len(ask))
	for name, lg := range ask {
		low, n := lg.LowestPosition()
		lowest[name], consumers[name] = float64(low), float64(n)
	}
	return lowest, consumers
}

// render writes the catalogue. It runs outside the broker's lock and
// touches nothing but the copy above.
func (n numbers) render(m *metricWriter) {
	m.declare("saguin_build_info", "gauge",
		"Always 1. This is where saguin's own version is stated.")
	m.value("saguin_build_info", 1, "version", n.version, "broker_id", n.brokerID)

	m.declare("saguin_uptime_seconds", "gauge", "Seconds since this process started.")
	m.value("saguin_uptime_seconds", n.uptime)

	m.declare("saguin_connections", "gauge", "Clients connected now.")
	m.value("saguin_connections", n.connections)

	// **The fleet mix, which is what an operator wants when both protocols
	// are admitted**: how much of the fleet is still on 3.1.1, and whether
	// that number is going down. Two series at most and both are saguin's
	// own labels, so nothing a client chooses reaches this.
	m.declare("saguin_connections_by_protocol", "gauge",
		"Clients connected now, by the MQTT version they connected with.")
	for _, v := range sortedKeys(n.byProtocol) {
		m.value("saguin_connections_by_protocol", n.byProtocol[v], "protocol", v)
	}

	m.declare("saguin_subscriptions", "gauge",
		"Every subscription the broker holds, not only the ones on channels.")
	m.value("saguin_subscriptions", n.subscriptions)

	m.declare("saguin_max_session_expiry_seconds", "gauge",
		"The configured cap on how long a client may keep its session.")
	m.value("saguin_max_session_expiry_seconds", n.maxSession)

	m.declare("saguin_channel_info", "gauge",
		"Always 1. Carries the channel's topic filter, type and storage provider.")
	for _, c := range n.channels {
		m.value("saguin_channel_info", 1,
			"channel", c.name, "filter", c.filter, "type", c.kind, "provider", c.provider)
	}

	// **A latest channel carries no series here rather than a zero.** Its
	// store keeps no byte total - a value replaced is a difference, so
	// measuring would have to read the outgoing value first, at half again
	// the cost of every write - and a zero would read as an empty channel
	// rather than as one nobody counts. A dashboard reading a wrong number
	// is worse off than one reading nothing.
	m.declare("saguin_channel_bytes", "gauge",
		"Bytes held, on append and queue channels. A latest channel is not measured.")
	for _, c := range n.channels {
		if c.kind != string(channel.Latest) {
			m.value("saguin_channel_bytes", c.bytes, "channel", c.name)
		}
	}

	// next and floor are absent on a queue, which has neither, so the three
	// below carry only the channels that have them rather than reporting a
	// zero that reads as an empty channel.
	m.declare("saguin_channel_next_offset", "gauge",
		"The offset this channel will assign next.")
	for _, c := range n.channels {
		if c.next > 0 {
			m.value("saguin_channel_next_offset", c.next, "channel", c.name)
		}
	}

	m.declare("saguin_channel_floor_offset", "gauge",
		"The oldest offset still readable.")
	for _, c := range n.channels {
		if c.floor > 0 {
			m.value("saguin_channel_floor_offset", c.floor, "channel", c.name)
		}
	}

	// Two derivations of one number, and the channel's shape picks which.
	// An append channel's is next minus floor, exact because retention only
	// ever removes a prefix. A latest channel's is how many topics hold a
	// current value, asked of a store that already knows - and a queue has
	// neither, so it carries saguin_queue_depth instead.
	m.declare("saguin_channel_records", "gauge",
		"Records held. On an append channel this is next minus floor; on a latest "+
			"channel it is how many topics have a current value, which its store keeps "+
			"rather than counts. A queue has neither and carries saguin_queue_depth.")
	for _, c := range n.channels {
		switch {
		case c.next > 0 && c.floor > 0:
			m.value("saguin_channel_records", c.next-c.floor, "channel", c.name)
		case c.topicsKnown:
			m.value("saguin_channel_records", c.topics, "channel", c.name)
		}
	}

	// **The alert this catalogue is worth building for**, against
	// saguin_channel_floor_offset: a floor above the lowest stored position
	// is data a consumer had not reached and retention has already removed.
	// Its warning shot is next minus this, growing - a device offline long
	// enough to be worth looking at before retention gets there.
	//
	// A channel nobody holds a position on carries no series rather than a
	// zero, because zero is not an offset: they start at one, and a zero
	// here would read as a consumer stuck at the very beginning.
	m.declare("saguin_channel_consumer_position_min", "gauge",
		"The lowest offset any durable consumer of this channel has stored.")
	for _, c := range n.channels {
		if c.lowestPosition > 0 {
			m.value("saguin_channel_consumer_position_min", c.lowestPosition, "channel", c.name)
		}
	}

	// **What the lowest position is a reading *from*.** On its own that
	// gauge cannot tell one straggler in a fleet of three hundred from a
	// single consumer that has stopped: both read as one number a long way
	// behind. The denominator separates them, and it is the difference
	// between a device to replace in the morning and a page tonight.
	//
	// **It does not say how many are behind**, and no bounded metric can:
	// that needs a threshold nobody can pick for an operator, or a series
	// per consumer, which is the one thing RFC 0005's catalogue is closed
	// against. `/consumers` answers it on demand instead.
	m.declare("saguin_channel_consumers", "gauge",
		"Durable consumers holding a stored position on this channel.")
	//
	// Every append channel carries it, zero included: none is a fact - no
	// device has ever stored a position here - where an absent series would
	// read as a channel that has none.
	for _, c := range n.channels {
		if c.kind == string(channel.Append) {
			m.value("saguin_channel_consumers", c.consumers, "channel", c.name)
		}
	}

	// **The denominator for a partitioned group, and the whole of what a
	// metric can honestly say about one.** It counts subscribers that
	// declared a slice on this channel; it does not say which slices, and it
	// cannot say whether they cover the space.
	//
	// **Coverage is deliberately not reported.** A missing index is
	// indistinguishable from a member that has not connected yet - the
	// broker has no way to know a group's intent - so a gauge claiming to
	// report a gap would be wrong during every rolling restart, which is
	// worse than silence. What is detectable is two subscribers on one
	// filter declaring different counts, and that is a log line rather than
	// a series: it cannot be correct, and it is not a number an operator
	// graphs.
	//
	// The count and the index are not labels: RFC 0005 bounds every label by
	// something the operator typed, and both are chosen by the client.
	m.declare("saguin_channel_partitioned_consumers", "gauge",
		"Subscribers that declared a partition slice on this channel.")
	for _, c := range n.channels {
		m.value("saguin_channel_partitioned_consumers", c.partitioned, "channel", c.name)
	}

	m.declare("saguin_provider_info", "gauge", "Always 1; type is memory or sqlite.")
	for _, p := range n.providers {
		m.value("saguin_provider_info", 1, "provider", p.name, "type", p.kind)
	}

	m.declare("saguin_provider_bytes", "gauge", "Bytes the provider holds, across every channel on it - a latest channel included, which saguin_channel_bytes does not measure, so this is larger than those summed. On sqlite it is the database's own size and not its disk usage: a write-ahead log waiting to be checkpointed is not counted here.")
	for _, p := range n.providers {
		m.value("saguin_provider_bytes", p.bytes, "provider", p.name)
	}

	m.declare("saguin_provider_max_bytes", "gauge", "The provider's bound; zero for none.")
	for _, p := range n.providers {
		m.value("saguin_provider_max_bytes", p.maxBytes, "provider", p.name)
	}

	// **The one thing an operator cannot otherwise see about group commit.**
	// A provider collecting publishes and one committing them singly hold
	// identical records, offsets and counter rows: only the wait differs.
	// So a record count set above the traffic costs several times the write
	// rate silently, and these are what say so - the average batch size,
	// read against the ceiling beside it.
	m.declare("saguin_storage_commits_total", "counter",
		"Transactions that stored publishes, by what closed them: `records` where the transaction "+
			"filled, `interval` where it did not and waited, `commit` where it collected what arrived "+
			"while the transaction before it committed, `unbatched` where the provider commits "+
			"one publish at a time. A retention sweep and a queue resolution are transactions too and "+
			"are not counted here.")
	for _, p := range n.providers {
		if p.commits == nil {
			continue
		}
		m.value("saguin_storage_commits_total", float64(p.commits.ClosedByRecords), "provider", p.name, "closed_by", "records")
		m.value("saguin_storage_commits_total", float64(p.commits.ClosedByInterval), "provider", p.name, "closed_by", "interval")
		m.value("saguin_storage_commits_total", float64(p.commits.ClosedByCommit), "provider", p.name, "closed_by", "commit")
		m.value("saguin_storage_commits_total", float64(p.commits.Unbatched), "provider", p.name, "closed_by", "unbatched")
	}

	m.declare("saguin_storage_committed_records_total", "counter",
		"Records those transactions carried. Divided by saguin_storage_commits_total it is the average "+
			"batch size, and read against saguin_provider_publish_commit_max_records it is the whole question "+
			"where publish_commit_interval is set: a batch size near one under a ceiling of hundreds means every "+
			"transaction is waiting out the interval and collecting almost nothing, which is slower than not "+
			"collecting at all. Without the key, a batch size near one only means little arrives at once, and "+
			"costs nothing.")
	for _, p := range n.providers {
		if p.commits != nil {
			m.value("saguin_storage_committed_records_total", float64(p.commits.Records), "provider", p.name)
		}
	}

	m.declare("saguin_provider_publish_commit_max_records", "gauge",
		"The provider's publish_commit_max_records; 256 where the key is absent and publishes are collected "+
			"without waiting; zero with `none`, where they are not collected. Published beside "+
			"the batch size for the same reason saguin_provider_max_bytes is published beside the bytes "+
			"held: a dashboard should not have to be told the configuration separately.")
	for _, p := range n.providers {
		if p.commits != nil {
			m.value("saguin_provider_publish_commit_max_records", float64(p.commits.MaxRecords), "provider", p.name)
		}
	}

	// No series until a provider has actually failed, which is the same
	// rule the refusal codes follow: a rate over a series that does not
	// exist is nothing, and nothing has gone wrong.
	m.declare("saguin_storage_errors_total", "counter",
		"Storage calls that failed, by provider. A channel at its size bound is not one of these.")
	for _, e := range n.storageErrs {
		m.value("saguin_storage_errors_total", float64(e.n), "provider", e.provider)
	}

	// **A bridge that is down is a metric and never a failed health probe.**
	// The far end being unreachable is a real thing to know and the worst
	// possible thing to restart this broker for, since the restart cannot
	// fix somebody else's broker (RFC 0005 "The health endpoint").
	m.declare("saguin_bridge_info", "gauge",
		"Always 1; carries the far end this bridge dials, in either direction.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_info", 1, "bridge", br.name, "peer", br.peer)
	}
	m.declare("saguin_bridge_connected", "gauge", "1 while the link to the peer is up.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_connected", br.connected, "bridge", br.name)
	}
	// **Separate from connected, because the two need different actions.** A
	// link that is down may come back on its own and usually does; one that
	// stopped never will, and an operator watching only `connected` would
	// see a bridge that looks like every other bad connection. A bridge
	// stops when the upstream holds a record larger than this broker's
	// `max_message_size`: the upstream disconnects rather than deliver it,
	// and every reconnect ends the same way on the same record, so retrying
	// is the one thing that cannot help (RFC 0002).
	m.declare("saguin_bridge_stopped", "gauge",
		"1 when the bridge halted itself and will not reconnect: the upstream holds a "+
			"record larger than this broker's max_message_size.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_stopped", br.stopped, "bridge", br.name)
	}
	// **Two counters for the outbound half, and the second is not noise.**
	// A bridge that is connected, sending nothing, and skipping everything
	// looks identical on `sent_total` alone to one with nothing to send -
	// and the two want opposite actions. `loops_skipped_total` climbing is a
	// topology an operator built: two brokers pointed at each other, where
	// the guard is doing its job and the records are not going anywhere.
	m.declare("saguin_bridge_sent_total", "counter",
		"Records this broker forwarded to the peer.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_sent_total", br.forwarded, "bridge", br.name)
	}
	m.declare("saguin_bridge_loops_skipped_total", "counter",
		"Records not forwarded because they arrived over a bridge. A record that came in "+
			"over a link is never sent back out over one, which is what stops a loop.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_loops_skipped_total", br.loopsSkipped, "bridge", br.name)
	}
	// **What the outbound half did not send, and why** - one series per cause,
	// a closed set, split by what an operator does about each: the peer's
	// rules, the shed, the bridge's own configuration, the link's speed.
	// Four are records the peer will never have; superseded is not a loss,
	// the newer value having crossed. Before these were served, the losses
	// were counted and read by nothing, and an outbound rule on a latest
	// channel sent nothing after it started with no number moving.
	m.declare("saguin_bridge_unsent_total", "counter",
		"Records an outbound rule did not send the peer, by cause. peer_refused: the peer "+
			"refused it for good (its ACL or limits), logged with the reason code. "+
			"broadcast_refused: the peer refused a broadcast record, or the link was down "+
			"when it was published - it has no store to retry from. unmappable: the rule could build no topic for it, or only one in the "+
			"reserved $ space - the bridge's configuration, logged. live_queue_full: a "+
			"broadcast record dropped because the rule's queue was full rather than hold up "+
			"publishers. superseded: a latest value replaced by a newer one for its topic "+
			"while it waited to cross - not a loss, the newer one crosses.")
	for _, br := range n.bridges {
		for _, c := range []struct {
			cause string
			n     float64
		}{
			{"peer_refused", br.peerRefused}, {"broadcast_refused", br.broadcastRefused},
			{"unmappable", br.unmappable}, {"live_queue_full", br.liveDropped},
			{"superseded", br.superseded},
		} {
			m.value("saguin_bridge_unsent_total", c.n, "bridge", br.name, "cause", c.cause)
		}
	}
	m.declare("saguin_bridge_received_total", "counter",
		"Records that arrived from the peer.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_received_total", br.received, "bridge", br.name)
	}
	// **What the inbound half received and did not store, by cause** - the
	// unsent family read the other way, a closed set. The bridge stores off
	// the connection's read loop so a refusing channel cannot blind it to a
	// lost link, and the queue between is bounded: QoS 0, which nothing
	// bounds, is dropped past its share of it.
	m.declare("saguin_bridge_unstored_total", "counter",
		"Records from the peer the bridge did not store, by cause. no_rule: no inbound rule "+
			"covers the topic. unmappable: a rule covers it and built no topic, or only one in "+
			"the reserved $ space - the bridge's configuration, logged. never_accepted: saguin "+
			"would refuse the record whatever the channel, so it is not retried. queue_full: a "+
			"QoS 0 record dropped because the bridge's queue was full behind a channel that was "+
			"not taking what it was given - at-most-once, as its publisher asked.")
	for _, br := range n.bridges {
		for _, c := range []struct {
			cause string
			n     float64
		}{
			{"no_rule", br.noRule}, {"unmappable", br.inUnmappable},
			{"never_accepted", br.neverAccepted}, {"queue_full", br.queueFull},
		} {
			m.value("saguin_bridge_unstored_total", c.n, "bridge", br.name, "cause", c.cause)
		}
	}
	m.declare("saguin_bridge_reconnects_total", "counter",
		"Times the link came back after going away; one that never lost it reports zero.")
	for _, br := range n.bridges {
		m.value("saguin_bridge_reconnects_total", br.reconnects, "bridge", br.name)
	}
}

// BridgeStats is what the catalogue asks of a bridge.
//
// An interface rather than the type itself, so that internal/broker does
// not import internal/bridge for the numbers it reports. The direction matters: a
// bridge is a client of the broker and already depends on it in spirit,
// and an import the other way would be the beginning of a cycle nobody
// wants to unpick later.
type BridgeStats interface {
	Name() string
	Peer() string
	Connected() bool
	Stopped() bool
	Received() uint64
	Reconnects() uint64
	// Forwarded and LoopsSkipped are the outbound half: what went to the
	// peer, and what was not sent because it had arrived over a bridge.
	Forwarded() uint64
	LoopsSkipped() uint64
	// And what else the outbound half did not send, by cause
	// (saguin_bridge_unsent_total).
	PeerRefused() uint64
	BroadcastRefused() uint64
	Unmappable() uint64
	LiveDropped() uint64
	Superseded() uint64
	// And what the inbound half received and did not store, by cause
	// (saguin_bridge_unstored_total).
	UnstoredQueueFull() uint64
	UnstoredNoRule() uint64
	UnstoredUnmappable() uint64
	UnstoredNeverAccepted() uint64
}

// SetBridges hands the broker the bridges to report on.
//
// **Under the lock, because it is not called before the listener is.** The
// operations port binds with the MQTT listeners so that a busy port is a
// startup error, and the bridges are built after that - so a scrape can
// arrive between the two and would otherwise read this field while it was
// being written. What such a scrape sees is a broker with no bridges, for
// the moment before there are any, which is true.
func (b *Broker) SetBridges(bs []BridgeStats) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bridgeStats = bs
}

// counters are saguin's own tallies, and every one of them is an atomic
// add at the site where the thing it counts actually happens.
//
// **Per channel and per reason code, and both are bounded by something
// nobody outside chooses** (RFC 0005 "Labels, and where the catalogue
// stops"). The channel map is built once from the registry before any
// listener opens, so finding a counter needs no lock and a topic nobody
// configured cannot add an entry. Reason codes are a closed set in the MQTT
// specification - at most a couple of hundred, and only the handful saguin
// answers with ever appear - so that map is guarded by a mutex it reaches
// only on a refusal.
type counters struct {
	connections       atomic.Uint64
	sessionsShortened atomic.Uint64

	// sessionsStorageFull is sessions that asked to be kept and were told
	// they end with their connection, because the session store's provider
	// had no room: saguin_sessions_dropped_total{cause="storage_full"}.
	sessionsStorageFull atomic.Uint64

	// sessionsExpiredWhileStopped is sessions whose expiry passed while the
	// broker was stopped, ended at startup:
	// saguin_sessions_dropped_total{cause="expired_while_stopped"}.
	sessionsExpiredWhileStopped atomic.Uint64

	// sessionsRefusedAtStart is sessions ended at startup because they held
	// a subscription the rules this broker started with refuse:
	// saguin_sessions_dropped_total{cause="subscription_refused"}.
	sessionsRefusedAtStart atomic.Uint64

	// sessionsRestored is sessions this broker put back at its start, from
	// what its session store kept while it was stopped:
	// saguin_sessions_restored_total.
	sessionsRestored atomic.Uint64

	// broadcast is the records accepted on a topic no channel claims, and
	// broadcastUnmatched the subset of those that reached nobody.
	//
	// They are two fields rather than entries in the map below because
	// "broadcast" is a name saguin writes and not one a client chooses:
	// forChannel deliberately answers nil for a topic no channel claims, so
	// that a map cannot grow a key per unknown name. That is right for the
	// map and it is why a broadcast publish went uncounted - the tally has
	// to live somewhere the map is not.
	broadcast          atomic.Uint64
	broadcastUnmatched atomic.Uint64

	// qos2Abandoned is exactly-once publishes that were taken in and never
	// released: their session ended, or they sat longer than
	// `broker.qos2.expires_after` and were swept.
	//
	// **Nothing else can show them.** The publisher is told nothing when
	// its exchange is dropped - it has had its PUBREC and is simply never
	// answered - and one that completes is already counted where the record
	// lands. So a number that climbs here is publishers opening exchanges
	// and not finishing them, which is invisible in every other series.
	//
	// One counter rather than one per reason: both are the same event seen
	// by a publisher, an exchange that did not finish, and an operator
	// splitting them would be reading the broker's bookkeeping rather than
	// their fleet's behaviour.
	qos2Abandoned atomic.Uint64

	// dropped is QoS 0 deliveries the broker could not hand to a consumer
	// because that consumer's outbound queue was full - one at QoS 1 or 2
	// waits in its session instead - and expired is deliveries
	// discarded because the publisher's Message Expiry Interval ran out
	// before they went.
	//
	// **Two numbers rather than one**, because they are not the same event
	// and an operator does one thing about each. A drop is a consumer that
	// cannot keep up - something is wrong at the other end, and on a
	// durable channel it costs a redelivery rather than a record because a
	// position only advances on the PUBACK. An expiry is the publisher's
	// own instruction being carried out, and the right number of them is
	// whatever that publisher meant. Added together they would be a figure
	// nobody could act on.
	//
	// **Neither carries a label**, and RFC 0005 "Labels, and where the
	// catalogue stops" is why: the useful split would be per consumer, and
	// a client picks its own id, so a fleet reconnecting with a fresh one
	// per boot writes unbounded series into somebody's monitoring. The
	// consumer's name goes in the log line beside the drop instead, bounded
	// the way every other client string is.
	dropped atomic.Uint64
	expired atomic.Uint64

	// sharedNoMember is deliveries to a shared group that no member could
	// take: every member offline, or connected with no room for it.
	// saguin_session_deliveries_dropped_total carries it under its own cause.
	sharedNoMember atomic.Uint64

	// sharesHeld is deliveries put on a shared group's list: every QoS 1 or
	// 2 broadcast a group with a cursor is owed (RFC 0003 "Broadcast"), and
	// every one returned to it. Held less drained less dropped is what the
	// groups hold.
	sharesHeld atomic.Uint64

	// sharesDrained is deliveries a group handed to a member, which is the
	// only way one leaves its list except by being dropped.
	sharesDrained atomic.Uint64

	// sharesDroppedFull is deliveries a group gave up at its
	// limits.session_queue_bytes, the oldest for a newer one, or refused
	// where it had nothing left to give up.
	sharesDroppedFull atomic.Uint64

	// sharesDroppedNoMember is backlogged deliveries dropped because the
	// last member session that could have collected them ended.
	sharesDroppedNoMember atomic.Uint64

	// sharesDroppedExpired is backlogged deliveries dropped for having been
	// held longer than broker.share.expires_after, at the sweep or at a
	// start.
	//
	// **Its own cause, not backlog_full**, which it was counted as until the
	// expiry was run with the queue nowhere near its bound: the label
	// names the knob an operator is being sent to, and one that says a queue
	// filled up when the operator's own expiry emptied it sends them to grow
	// a bound that was never reached. Named as the qos2 and job expiries
	// are.
	sharesDroppedExpired atomic.Uint64

	// sharesDroppedStorageFull is deliveries a group was owed and lost to
	// broker.session.storage's room: given up to make room in the log, not
	// kept by a full log, or not returned to the group at a member's ending
	// where even the reserve had no room for the returned row.
	sharesDroppedStorageFull atomic.Uint64

	// sharesDroppedMemberEnded is QoS 2 deliveries a group handed a member
	// whose session ended before the member answered with a PUBREC: MQTT
	// forbids sending one to another member (MQTT-4.8.2-5).
	sharesDroppedMemberEnded atomic.Uint64

	// sharesDroppedNotAuthorized is deliveries a group let go because the
	// acl_file allowed them to none of the members that could take them.
	sharesDroppedNotAuthorized atomic.Uint64

	// deliveriesStorageFull is deliveries a session was owed and its store
	// had no room for: the oldest given up to make room, or the new one where
	// nothing older could go. Under cause storage_full.
	deliveriesStorageFull atomic.Uint64

	// channel is fixed at startup: one entry per configured channel, and
	// nothing adds to it afterwards.
	channel map[string]*channelCounters

	refusedMu sync.Mutex
	refused   map[string]uint64

	// connRefused counts connections this broker refused, at CONNECT or by
	// ending them, on any protocol. **Bounded by the same closed set as `refused`**,
	// which is the reason vocabulary saguin writes rather than anything a
	// client chooses (invariant 13): the keys can only be reason names
	// `refusalReason` produces.
	connRefusedMu sync.Mutex
	connRefused   map[string]uint64

	// willsMu guards willsPub, which counts the Wills this broker published
	// by what made each due: saguin_wills_published_total{cause}. A closed
	// set of four, written here and nowhere else - a client chooses none of
	// them, which is what keeps the label bounded (invariant 13).
	willsMu  sync.Mutex
	willsPub map[string]uint64

	// willsCancelled counts the Wills a returning client cancelled inside
	// their delay: saguin_wills_cancelled_total, which is the delay doing
	// its job rather than a fault.
	willsCancelled atomic.Uint64

	// storage failures by provider. A mutex rather than a prebuilt map
	// because the provider names come from the configuration and the broker
	// is handed stores rather than the list of them; the map is bounded by
	// what the operator wrote either way.
	storageMu   sync.Mutex
	storageErrs map[string]uint64
}

// channelCounters are the tallies kept per channel. A queue uses the queue
// fields and an append or latest channel leaves them at zero, which is one
// struct rather than two maps to keep in step.
type channelCounters struct {
	published        atomic.Uint64
	retentionRemoved atomic.Uint64
	positionLost     atomic.Uint64
	// latestSuperseded is a latest channel's values that a subscriber was not
	// sent because a newer one for the topic took their place while they
	// waited - RFC 0003's "the current value, not every intermediate one",
	// counted so that it is not silent.
	latestSuperseded atomic.Uint64

	delivered     atomic.Uint64
	acknowledged  atomic.Uint64
	returned      atomic.Uint64
	redelivered   atomic.Uint64
	deadLettered  atomic.Uint64
	expired       atomic.Uint64
	retainIgnored atomic.Uint64
}

func newCounters(names []string) *counters {
	c := &counters{
		channel:     make(map[string]*channelCounters, len(names)),
		refused:     map[string]uint64{},
		connRefused: map[string]uint64{},
		storageErrs: map[string]uint64{},
	}
	for _, n := range names {
		c.channel[n] = &channelCounters{}
	}
	return c
}

// PartitionedConsumers is how many subscribers have declared a slice on
// each channel, by channel name.
//
// **The count and the index are not labels and never will be.** RFC 0005
// bounds every label by something the operator typed - channel, provider
// and bridge names from the configuration file, MQTT reason codes from a
// closed set in the specification - and a partition count is chosen by the
// client. That rule is about *who chooses* rather than how many they might
// choose, so `{channel, count}` fails it as surely as a client id does: a
// client is free to declare 2147483647. Which client declared what is on
// the operations `consumers` route, which is not a metric and has no
// cardinality budget.
//
// A declaration on a filter reaching several channels counts on each of
// them, which is what the subscriber is: a member of a slice of every
// channel that filter touches.
func (b *Broker) PartitionedConsumers() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]int{}
	for id, byFilter := range b.partitions {
		for _, s := range b.subs[id] {
			if _, declared := byFilter[s.filter]; declared {
				out[s.channel.Name]++
			}
		}
	}
	return out
}

// forChannel returns the counters for a channel, or nil for a name no
// channel has - which is broadcast, and is counted by its own totals.
//
// It returns nil rather than creating an entry on demand, and that is the
// whole reason it exists as a method: a topic is chosen by whoever
// publishes, so a map that grew a key per unknown name would be a series
// count a client picks, which is exactly what the catalogue is closed
// against.
func (c *counters) forChannel(name string) *channelCounters {
	if c == nil {
		return nil
	}
	return c.channel[name]
}

// refuse counts a publish saguin declined, by the reason code it answered
// with.
func (c *counters) refuse(reason string) {
	if c == nil {
		return
	}
	c.refusedMu.Lock()
	c.refused[reason]++
	c.refusedMu.Unlock()
}

// connectionRefused counts one connection this broker refused, at CONNECT or
// by ending it, whatever the client's protocol (RFC 0005
// `saguin_connections_refused_total`).
//
// **A second tally rather than a label on the first**, because they answer
// different questions and an operator does different things about each.
// `saguin_publish_refused_total` says a publish was refused, which is usually
// the application's business. This says a device was turned away - its
// CONNECT refused, or its connection ended for a refusal - and is the number
// behind a fleet in a reconnect loop.
//
// The reason vocabulary is the refusal one, so the two series divide the
// same way and a rate of one against the other reads directly: of the
// publishes refused for this reason, how many cost a connection.
func (c *counters) connectionRefused(reason string) {
	if c == nil {
		return
	}
	c.connRefusedMu.Lock()
	c.connRefused[reason]++
	c.connRefusedMu.Unlock()
}

// connectionsRefused is what the catalogue renders, sorted so the output is
// stable.
func (c *counters) connectionsRefused() []struct {
	reason string
	n      uint64
} {
	c.connRefusedMu.Lock()
	defer c.connRefusedMu.Unlock()
	out := make([]struct {
		reason string
		n      uint64
	}, 0, len(c.connRefused))
	for _, r := range sortedKeys(c.connRefused) {
		out = append(out, struct {
			reason string
			n      uint64
		}{r, c.connRefused[r]})
	}
	return out
}

// willsPublished counts one Will against what made it due.
func (c *counters) willsPublished(cause string) {
	if c == nil {
		return
	}
	c.willsMu.Lock()
	if c.willsPub == nil {
		c.willsPub = map[string]uint64{}
	}
	c.willsPub[cause]++
	c.willsMu.Unlock()
}

// willsByCause is what the catalogue renders, sorted so the output is stable.
func (c *counters) willsByCause() []struct {
	cause string
	n     uint64
} {
	c.willsMu.Lock()
	defer c.willsMu.Unlock()
	out := make([]struct {
		cause string
		n     uint64
	}, 0, len(c.willsPub))
	for _, cause := range sortedKeys(c.willsPub) {
		out = append(out, struct {
			cause string
			n     uint64
		}{cause, c.willsPub[cause]})
	}
	return out
}

// storageError counts one storage failure against a provider.
func (c *counters) storageError(provider string) {
	if c == nil {
		return
	}
	c.storageMu.Lock()
	c.storageErrs[provider]++
	c.storageMu.Unlock()
}

// storageFailures copies the by-provider tallies, in a fixed order.
func (c *counters) storageFailures() []struct {
	provider string
	n        uint64
} {
	c.storageMu.Lock()
	defer c.storageMu.Unlock()
	out := make([]struct {
		provider string
		n        uint64
	}, 0, len(c.storageErrs))
	for _, k := range sortedKeys(c.storageErrs) {
		out = append(out, struct {
			provider string
			n        uint64
		}{k, c.storageErrs[k]})
	}
	return out
}

// refusals copies the by-reason tallies, in a fixed order.
func (c *counters) refusals() []struct {
	reason string
	n      uint64
} {
	c.refusedMu.Lock()
	defer c.refusedMu.Unlock()
	out := make([]struct {
		reason string
		n      uint64
	}, 0, len(c.refused))
	for _, r := range sortedKeys(c.refused) {
		out = append(out, struct {
			reason string
			n      uint64
		}{r, c.refused[r]})
	}
	return out
}

// subscribeRefusals names the substrate's per-code SUBACK refusal counts
// (system.Info.SubscribeRefused) for saguin_subscriptions_refused_total, in
// the order of their names, leaving out a code nothing has been refused with.
// A code the specification names is labelled by that name, and any other by
// its number, as refusalReason does.
func subscribeRefusals(byCode *[256]atomic.Int64) []struct {
	reason string
	n      uint64
} {
	counts := map[string]uint64{}
	for code := range byCode {
		n := byCode[code].Load()
		if n <= 0 {
			continue
		}
		name, ok := reasonNames[byte(code)]
		if !ok {
			name = fmt.Sprintf("0x%02X", code)
		}
		counts[name] += uint64(n)
	}
	out := make([]struct {
		reason string
		n      uint64
	}, 0, len(counts))
	for _, r := range sortedKeys(counts) {
		out = append(out, struct {
			reason string
			n      uint64
		}{r, counts[r]})
	}
	return out
}

// reasonNames are the MQTT specification's names for the codes saguin can
// answer a publish with, keyed by the code itself.
//
// **A code is a byte and the specification names it; a Reason string is
// prose saguin wrote.** This map exists because refusalReason read the
// latter: saguin builds its refusals with sentences in them, so the label
// on saguin_publish_refused_total became, verbatim, "a dead-letter channel
// takes records only from the queue that derives it; publish to the queue
// instead". A label that is an error message is renamed by any rewording of
// that message, and whatever matched on it breaks silently - which is the
// failure the closed catalogue exists to prevent, arrived at from inside.
//
// The set is closed by the specification rather than by saguin's call
// sites, which is what makes this a map and not a list that rots: a code
// saguin starts answering tomorrow is already in it, and one that is not
// falls back to its number rather than to prose.
var reasonNames = map[byte]string{
	0x80: "unspecified error",
	0x83: "implementation specific error",
	0x87: "not authorized",
	0x90: "topic name invalid",
	0x95: "packet too large",
	0x97: "quota exceeded",
	0x99: "payload format invalid",
	0x9A: "retain not supported",
	0x9B: "qos not supported",
	0x9F: "connection rate exceeded",

	// **The CONNECT-time refusals**, which reached this map only once a
	// hook told saguin about them. They are the reasons a device is turned
	// away at the door rather than hung up on afterwards, and the route's
	// reason column is the whole of what an operator reads - a row saying
	// `0x84` is a row they have to go and look up.
	0x84: "unsupported protocol version",
	0x85: "client identifier not valid",
	0x86: "bad user name or password",
	0x89: "server busy",
	0x8C: "bad authentication method",

	// **The refusals that end a connection once it is made**, which the
	// substrate tells saguin about: a packet it could not decode or that
	// broke the protocol as it was read, a publish past the Receive
	// Maximum, one naming an alias above the maximum.
	0x81: "malformed packet",
	0x82: "protocol error",
	0x93: "receive maximum exceeded",
	0x94: "topic alias invalid",

	// **The SUBACK refusals** (saguin_subscriptions_refused_total).
	0x8F: "topic filter invalid",
	0x91: "packet identifier in use",
	0x9E: "shared subscriptions not supported",
	0xA1: "subscription identifiers not supported",
	0xA2: "wildcard subscriptions not supported",
}

// refusalReason names the reason code a refused publish was answered with,
// for saguin_publish_refused_total.
//
// A refusal carrying no code at all is counted as "rejected": the packet was
// dropped without a reason a client could read, which is what
// ErrRejectPacket means and is worth its own series rather than being folded
// in with the codes.
//
// **ErrRejectPacket is compared against rather than looked up**, because it
// is a reason code by type and not by use: it is the hook contract's way of
// saying "stop this packet", the substrate never puts it on the wire, and
// its number is 0x83. Looked up like any other it would label the refusal
// "implementation specific error" - naming a code the client was never
// answered with. It stays in the map beside the rest, where it is right for
// a refusal that really is answered 0x83.
func refusalReason(err error) string {
	// **A metric is not bound by MQTT's reason-code set, and here that
	// matters.** `0x97 Quota exceeded` is the wire answer for five
	// different things - a channel at its size bound, a provider at its
	// bound, too many headers, headers too large, and a client over its
	// publish rate. On the wire an operator can do nothing about that: the
	// specification has no finer code and the Reason String is withheld
	// from any client that asked not to receive it, which Eclipse Paho does
	// by default.
	//
	// In a metric it is fixable, and it is the difference between two
	// questions with opposite answers: "am I throttling my own devices" and
	// "is a channel full". The first is a limit somebody chose; the second
	// is storage running out. A single `quota exceeded` series makes an
	// operator guess between them, and the same is true at QoS 0, where
	// every refusal whatever its cause is answered with no code at all and
	// would otherwise be counted as one undifferentiated `rejected`.
	//
	// Two shapes because the substrate forces two: see the comment on
	// codeOverPublishRate. At QoS 0 the refusal is a wrapped
	// ErrRejectPacket; at QoS 1 it is that exact Code value, compared
	// rather than matched on its wording.
	var asCode packets.Code
	if errors.Is(err, errOverPublishRate) ||
		(errors.As(err, &asCode) && asCode == codeOverPublishRate) {
		return "publish rate exceeded"
	}
	// The third meaning of `0x97` on this broker, and split out for the same
	// reason as the second: a publisher holding its allowance of unfinished
	// exactly-once exchanges is a client to look at, and a full channel is
	// storage to look at. Counted as one series they are a number nobody can
	// act on. Compared as a value rather than matched on its wording.
	if errors.As(err, &asCode) && asCode == codeTooManyInflight {
		return "inflight allowance exceeded"
	}
	// The fourth, and split out for the same reason again: a connection
	// refused because the session store had no room for its Will is storage
	// to look at, and it is the only refusal here that names a provider.
	if errors.As(err, &asCode) && asCode == codeWillNotKept {
		return "session store full"
	}
	var code packets.Code
	if errors.As(err, &code) && code != packets.ErrRejectPacket {
		if name, ok := reasonNames[code.Code]; ok {
			return name
		}
		// Not a code this map knows. The number is still closed and still
		// stable, where the Reason beside it is prose.
		return fmt.Sprintf("0x%02X", code.Code)
	}
	return "rejected"
}

// renderCounters writes the tallies. Split from render only so that each
// function stays short enough to read; the two are one page of output and
// there is no ordering between them a scraper cares about.
func (n numbers) renderCounters(m *metricWriter) {
	m.declare("saguin_connections_total", "counter", "Connections accepted since start.")
	m.value("saguin_connections_total", n.connectionsTotal)

	// **What the box moved**, which nothing else in this catalogue answers:
	// every other counter here is about records saguin decided to keep or
	// deliver, and none of them is a rate an operator can size a link
	// against. Four counters, one atomic load each, and a rate over any of
	// them is throughput in the unit its name gives.
	m.declare("saguin_bytes_received_total", "counter",
		"Bytes read from MQTT connections since start. Everything on the wire: a "+
			"payload, the acknowledgement behind it, a keepalive, a connection being "+
			"set up. A bridge's traffic to an upstream broker is not among them.")
	m.value("saguin_bytes_received_total", n.bytesIn)

	m.declare("saguin_bytes_sent_total", "counter",
		"Bytes written to MQTT connections since start, on the same terms as the "+
			"row above.")
	m.value("saguin_bytes_sent_total", n.bytesOut)

	m.declare("saguin_publishes_received_total", "counter",
		"Publishes that arrived - from a client, or carried in by an inbound bridge - "+
			"including the ones saguin went on to refuse; not a queue's offer to its worker "+
			"or a Will. Read against saguin_published_total, which counts what a channel "+
			"kept, the two say how much of what a fleet sends is landing.")
	m.value("saguin_publishes_received_total", n.publishesIn)

	m.declare("saguin_deliveries_sent_total", "counter",
		"Publishes saguin sent to subscribers, a queue record handed out a second "+
			"time included. This is the read side: nothing else counts what leaves a "+
			"channel that is not a queue.")
	m.value("saguin_deliveries_sent_total", n.deliveries)

	// **The second way a broadcast delivery is lost to a slow subscriber**,
	// and the one that had no counter. The first is its outbound queue
	// overflowing, which saguin_deliveries_dropped_total has always
	// counted. This is a layer earlier: the session already holds
	// limits.session_queue_bytes its client has not acknowledged, or has no
	// packet identifier left. Each is also counted as one of those two
	// causes of saguin_session_deliveries_dropped_total, so the two series
	// are not added.
	//
	// **Broadcast, and only broadcast, which is what makes it a loss.** A
	// channel record meeting a full window is not counted here and is not
	// gone: saguin sends those itself - an append record waits at the
	// consumer's position, a latest value waits for that subscriber until a
	// newer one replaces it - and a consumer that leaves first has its
	// position held below what it was not sent, so a resume carries it
	// (RFC 0003). Broadcast promises nothing beyond delivery to whoever is
	// connected, so there is nothing to come back for - which is exactly
	// why this is the half worth a counter.
	m.declare("saguin_deliveries_refused_total", "counter",
		"Broadcast deliveries saguin could not send because the subscriber already held "+
			"limits.session_queue_bytes unacknowledged, or had run through its packet "+
			"identifiers; each is also counted in saguin_session_deliveries_dropped_total, "+
			"under session_queue_full or packet_ids_exhausted, so the two are not added. "+
			"A different loss from saguin_deliveries_dropped_total, which "+
			"is that client's outbound queue overflowing. A channel record meeting a "+
			"full window is neither: it waits for room - a latest value until a newer one "+
			"replaces it, counted in saguin_latest_superseded_total - and a consumer that "+
			"leaves first resumes below what it was not sent.")
	m.value("saguin_deliveries_refused_total", n.deliveriesRefused)

	// **A session outliving its connection is the point of a session**, so
	// this is a fleet's shape rather than a fault. What it answers is the
	// question saguin_connections cannot: a fleet of three hundred devices
	// with two hundred connected is either a rota or a hundred devices that
	// have stopped calling, and the two look identical from the connection
	// count alone.
	m.declare("saguin_sessions_offline", "gauge",
		"Sessions this broker holds that nothing is connected to. They are kept until "+
			"their expiry, so a number that only climbs is sessions nothing is coming "+
			"back for.")
	m.value("saguin_sessions_offline", n.sessionsOffline)

	// **What a Will did, by what made it due.** A Will is the one message a
	// broker sends on a client's behalf, so an operator watching a fleet
	// wants to know both that they are being sent and which rule sent them:
	// a device that went quiet (`delayed`), one whose session ran out while
	// it was away (`session_ended`), one owed from before a restart
	// (`start`), and the ordinary case of a link dying with no delay set
	// (`immediate`). The set is closed and saguin writes every member of it;
	// nothing a client sends reaches this label.
	m.declare("saguin_wills_published_total", "counter",
		"Wills this broker published on a client's behalf, by what made each due. "+
			"immediate is a connection that ended without a DISCONNECT and no Will Delay "+
			"Interval; delayed is one whose delay ran out; session_ended is a session that "+
			"ended while its Will was still due - it expired, a clean start under its client "+
			"id ended it, or its client came back to find retention had passed one of its "+
			"positions; start is a Will this broker found "+
			"already owed when it started, because the delay passed or the session ended "+
			"while it was stopped.")
	for _, w := range n.willsPublished {
		m.value("saguin_wills_published_total", float64(w.n), "cause", w.cause)
	}

	// **The delay doing its job**, which is worth a series of its own: a
	// number climbing here beside a flat published total is a fleet on a bad
	// link that is correctly not being announced dead.
	m.declare("saguin_wills_cancelled_total", "counter",
		"Wills a client cancelled by connecting again inside its Will Delay Interval "+
			"[MQTT-3.1.3-9]. This is the delay working: the device came back, so nothing "+
			"was announced.")
	m.value("saguin_wills_cancelled_total", n.willsCancelled)

	m.declare("saguin_wills_waiting", "gauge",
		"Wills waiting out their Will Delay Interval now: clients that have gone and "+
			"whose deaths have not been announced yet. Each is held in "+
			"broker.session.storage with the moment it becomes due, so a restart inside "+
			"the wait publishes it when it lands rather than losing it.")
	m.value("saguin_wills_waiting", n.willsWaiting)

	// **Sessions put back at the start**, which moves once per start and then
	// stands still: it is how many sessions this broker took over from the one
	// before it. Read beside saguin_sessions_dropped_total, the two answer what
	// a restart did to a fleet - and read alone, a start that restored none on
	// a broker whose clients ask for persistent sessions is a session store
	// that lost them, which nothing else would say.
	m.declare("saguin_sessions_restored_total", "counter",
		"Sessions this broker put back when it started, from what broker.session.storage "+
			"kept while it was stopped. They are answered Session Present 1 when their "+
			"clients come back, and are sent what they were owed. It moves once, at the "+
			"start, so what it reports is what the last start found.")
	m.value("saguin_sessions_restored_total", n.sessionsRestored)

	// **Sessions the store could not keep**, by cause. The first cause is the
	// only one while the broker runs: a client asked for its session to
	// outlive the connection, the provider holding sessions was at its
	// max_bytes, and it was told in its CONNACK that the session ends with the
	// connection. Nothing else says a fleet is losing its sessions to a full
	// provider.
	m.declare("saguin_sessions_dropped_total", "counter",
		"Sessions the session store could not keep, by cause. storage_full is a session "+
			"that asked to outlive its connection when broker.session.storage was at its "+
			"max_bytes, and was told in its CONNACK that it ends with the connection. "+
			"expired_while_stopped is a session whose expiry passed while the broker was "+
			"stopped, ended when it started. subscription_refused is a session ended at a "+
			"start because it held a subscription this broker's rules now refuse - a channel "+
			"that became a queue, most often - so its client comes back to Session Present 0 "+
			"and is answered for what it asks for again.")
	m.value("saguin_sessions_dropped_total", n.sessionsStorageFull, "cause", "storage_full")
	m.value("saguin_sessions_dropped_total", n.sessionsExpiredWhileStopped, "cause", "expired_while_stopped")
	m.value("saguin_sessions_dropped_total", n.sessionsRefusedAtStart, "cause", "subscription_refused")

	// **Absent where there is no retained store**, rather than zero - which
	// is only an in-process harness that attaches none, since every broker
	// the binary starts keeps one. There a retained publish is refused
	// outright, so nothing could ever be held, and a zero would read as a
	// store that happens to be empty. The same rule that keeps a latest channel out of
	// saguin_channel_bytes.
	m.declare("saguin_retained_messages", "gauge",
		"Retained messages this broker holds on broadcast topics. They accumulate "+
			"until something publishes an empty payload to the topic or their "+
			"retention removes them, and nothing else says how many there are.")
	if n.hasRetained {
		m.value("saguin_retained_messages", n.retained)
	}

	// The three exactly-once series, present on every broker the binary
	// builds, since it always attaches the store. Absent together only on an
	// in-process harness that attaches none, which advertises Maximum QoS 1,
	// so a zero there would say the feature was on and quiet.
	m.declare("saguin_qos2_held", "gauge",
		"Exactly-once publishes received and not yet released. They are messages this "+
			"broker has taken ownership of and no client has finished sending, so a "+
			"figure that climbs and stays is publishers not completing exchanges.")
	if n.hasQoS2 {
		m.value("saguin_qos2_held", n.qos2Held)
	}

	m.declare("saguin_qos2_abandoned_total", "counter",
		"Exactly-once publishes taken in and never released, because their session "+
			"ended, they aged past broker.qos2.expires_after, or the start found them "+
			"holding for a session that did not come back or in a channel the configuration "+
			"does not keep. Nothing else shows "+
			"them: the publisher is told nothing, and a completed exchange is counted "+
			"where its record lands.")
	if n.hasQoS2 {
		m.value("saguin_qos2_abandoned_total", n.qos2Abandoned)
	}

	m.declare("saguin_qos2_max_inflight_per_client", "gauge",
		"The configured broker.qos2.max_inflight_per_client, so a dashboard can read "+
			"what is held against what was allowed.")
	if n.hasQoS2 {
		m.value("saguin_qos2_max_inflight_per_client", n.qos2MaxPerClnt)
	}

	m.declare("saguin_session_expiry_shortened_total", "counter",
		"CONNECTs that asked to keep their session for longer than max_session_expiry and were given the cap.")
	m.value("saguin_session_expiry_shortened_total", n.sessionsShortened)

	m.declare("saguin_published_total", "counter",
		"Records accepted, by channel, plus one series for broadcast. "+
			"A dead-letter channel is always zero: a record arrives there by the "+
			"queue's own move and a publish into it is refused, so its arrivals are "+
			"saguin_queue_dead_lettered_total.")
	for _, c := range n.channels {
		m.value("saguin_published_total", c.published, "channel", c.name)
	}
	// The series the catalogue promises beside the channels, and which no
	// per-channel loop can emit: a topic no channel claims has no entry in
	// the map those loops walk.
	m.value("saguin_published_total", n.broadcast, "channel", "broadcast")

	m.declare("saguin_broadcast_unmatched_total", "counter",
		"Broadcast publishes that matched no subscription and no channel. "+
			"This is what answers a mistyped channel name; MQTT has no reason code for it.")
	m.value("saguin_broadcast_unmatched_total", n.broadcastUnmatched)

	m.declare("saguin_deliveries_dropped_total", "counter",
		"Broadcast deliveries at QoS 0 discarded because a subscriber's outbound "+
			"queue was full; one at QoS 1 or 2 waits in its session instead. "+
			"Broadcast keeps no position, so a dropped record is gone - which "+
			"is what broadcast promises. Channel deliveries are not counted here: "+
			"they are written under limits.write_timeout, and a consumer that does "+
			"not take one in time is disconnected rather than dropped, which costs "+
			"it nothing because it resumes at the record it had reached. The broker "+
			"log names the client either way.")
	m.value("saguin_deliveries_dropped_total", n.dropped)

	m.declare("saguin_deliveries_expired_total", "counter",
		"Deliveries discarded because the publisher's Message Expiry Interval "+
			"ran out: before they were sent, or, for a session that ends with its "+
			"connection, while on the wire and unacknowledged. This is the publisher's instruction "+
			"being carried out rather than a fault, which is why it is not counted "+
			"with the drops above.")
	m.value("saguin_deliveries_expired_total", n.expired)

	// **One series per cause, and the causes are saguin's own words**, a closed
	// set: what an operator does about a group with nobody to take its
	// messages is not what they do about a session that has filled up.
	m.declare("saguin_session_deliveries_dropped_total", "counter",
		"Deliveries a session never received, by cause. session_queue_full: at "+
			"limits.session_queue_bytes, the oldest the session held given up, or the new one "+
			"where nothing it held could go. packet_ids_exhausted: refused with no "+
			"packet identifier left. no_shared_member: a shared group "+
			"had no member that could take it - every member offline, or connected with no "+
			"room for it. storage_full: broker.session.storage had no room, so the oldest the "+
			"session held was given up, or the new one where nothing older could go. "+
			"too_large: larger than the client's Maximum Packet Size, so discarded "+
			"rather than sent, as MQTT has the server do.")
	m.value("saguin_session_deliveries_dropped_total", n.sessionQueueFull, "cause", "session_queue_full")
	m.value("saguin_session_deliveries_dropped_total", n.packetIDsExhausted, "cause", "packet_ids_exhausted")
	m.value("saguin_session_deliveries_dropped_total", n.sharedNoMember, "cause", "no_shared_member")
	m.value("saguin_session_deliveries_dropped_total", n.deliveriesStorageFull, "cause", "storage_full")
	m.value("saguin_session_deliveries_dropped_total", n.tooLarge, "cause", "too_large")

	// **What a shared group is holding for members that are away**, which is
	// the other half of no_shared_member above: a delivery no member could
	// take is now held where the group has a member whose session outlives
	// its connection, and dropped and counted there where it has none. An
	// operator watching only the drop series would see a fleet's work
	// vanish from it and conclude nothing was being lost.
	m.declare("saguin_shares_held_total", "counter",
		"Deliveries put on a shared group's list: every QoS 1 or 2 delivery a group with a "+
			"member whose session outlives its connection is owed, one it has no room for "+
			"counted dropped as well, every one returned to it by a member's ending or dropped "+
			"there once handed out, and at a start every one put back on a group's list or "+
			"dropped with a group no session holds. A group over a channel counts exactly as "+
			"one over a broadcast topic.")
	m.value("saguin_shares_held_total", n.sharesHeld)

	m.declare("saguin_shares_drained_total", "counter",
		"Deliveries a shared group handed to a member. Held minus drained minus dropped is "+
			"what the groups are holding; a drained count that stays flat while held climbs is "+
			"a fleet that is not coming back.")
	m.value("saguin_shares_drained_total", n.sharesDrained)

	m.declare("saguin_shares_dropped_total", "counter",
		"Deliveries a shared group dropped, by cause. backlog_full: at limits.session_queue_bytes "+
			"for that group, the oldest given up for a newer one. no_member_left: the last member session "+
			"that could have collected them ended, so nobody was owed them any more. expired: "+
			"held longer than broker.share.expires_after, dropped by the sweep or by a start, or "+
			"past the publisher's own Message Expiry Interval when the group reached it. "+
			"storage_full: broker.session.storage had no room for them. member_ended: a QoS 2 "+
			"delivery whose member's session ended before its PUBREC, which MQTT forbids "+
			"sending to another member (MQTT-4.8.2-5). not_authorized: the acl_file allowed "+
			"it to none of the members that could take it.")
	m.value("saguin_shares_dropped_total", n.sharesDroppedFull, "cause", "backlog_full")
	m.value("saguin_shares_dropped_total", n.sharesDroppedNoMem, "cause", "no_member_left")
	m.value("saguin_shares_dropped_total", n.sharesDroppedExpired, "cause", "expired")
	m.value("saguin_shares_dropped_total", n.sharesDroppedStorage, "cause", "storage_full")
	m.value("saguin_shares_dropped_total", n.sharesDroppedMember, "cause", "member_ended")
	m.value("saguin_shares_dropped_total", n.sharesDroppedAuth, "cause", "not_authorized")

	// **What sessions hold that nobody has acknowledged**, summed rather than
	// by client: a client chooses its own id, and a series per session is a
	// series count a fleet chooses. The largest single session is in the log
	// line when one reaches its bound.
	m.declare("saguin_session_queue_messages", "gauge",
		"Deliveries every session holds and its client has not acknowledged, connected or not: "+
			"on the wire, waiting for room in the client's window, or queued while it is away. "+
			"A channel's records waiting to be sent are not in it: an append consumer waits "+
			"at its position, and a latest subscriber's waiting values - at most one per topic "+
			"its subscriptions reach - belong to its connection rather than its session.")
	m.value("saguin_session_queue_messages", n.sessionQueueMessages)
	m.declare("saguin_session_queue_bytes", "gauge",
		"The memory those deliveries take, as limits.session_queue_bytes counts it for one session.")
	m.value("saguin_session_queue_bytes", n.sessionQueueBytes)

	// **No series until a refusal happens**, which is what keeps the label
	// closed rather than merely bounded: the codes are a closed set in the
	// specification, and only the ones saguin has actually answered with
	// appear. A scraper reading a rate over a series that does not exist
	// yet gets nothing, which is correct - nothing has been refused.
	m.declare("saguin_publish_refused_total", "counter",
		"Publishes refused, by the MQTT reason code answered.")
	for _, r := range n.refused {
		m.value("saguin_publish_refused_total", float64(r.n), "reason", r.reason)
	}

	// **No series until a refusal happens**, as saguin_publish_refused_total:
	// the label is the specification's name for a SUBACK code, a closed set,
	// and only the codes saguin has answered with appear.
	m.declare("saguin_subscriptions_refused_total", "counter",
		"Topic filters a SUBACK refused, by the MQTT reason code decided for each - before a "+
			"3.1.1 client's is written as 0x80.")
	for _, r := range n.subscriptionsRefused {
		m.value("saguin_subscriptions_refused_total", float64(r.n), "reason", r.reason)
	}

	// **No series until it happens**, for the same reason as the one above:
	// a broker that has refused nobody writes none of these, and a rate over
	// a series that does not exist is correctly nothing.
	m.declare("saguin_connections_refused_total", "counter",
		"Connections this broker refused at CONNECT or ended for a refusal, on any protocol, "+
			"by reason.")
	for _, r := range n.connectionsRefused {
		m.value("saguin_connections_refused_total", float64(r.n), "reason", r.reason)
	}

	m.declare("saguin_channel_retention_removed_total", "counter",
		"Records retention has deleted, by age or by size.")
	for _, c := range n.channels {
		m.value("saguin_channel_retention_removed_total", c.retentionRemoved, "channel", c.name)
	}

	// **Every reader the floor passed, whatever kind of reader it was and
	// however well it was told.** The old sentence here said "reads refused
	// for being below the retention floor", which described one of the three
	// producers and misdescribed the others: a bridge poll is not an MQTT
	// read, and a position dropped at CONNECT is refused nothing. An
	// operator watching this number is asking whether records are being
	// lost, and that question does not change with which door the loss came
	// through - the bridge has said so at its own site since it was written.
	m.declare("saguin_channel_position_lost_total", "counter",
		"Readers whose stored position the retention floor passed, so records they "+
			"had a claim on were unreadable - invariant 1 firing. Counts occurrences "+
			"rather than readers: the floor passes a lagging reader repeatedly. Every "+
			"kind of reader and every way it is discovered - a consumer overtaken "+
			"while connected and reading, which cannot be told; one whose stored "+
			"position had been passed while it was away, which is told with Session "+
			"Present = 0; and an outbound bridge rule's position.")
	for _, c := range n.channels {
		m.value("saguin_channel_position_lost_total", c.positionLost, "channel", c.name)
	}

	m.declare("saguin_latest_superseded_total", "counter",
		"Values on a latest channel that a subscriber was not sent because a newer value "+
			"for the same topic took their place while they waited for it: the current "+
			"value, not every intermediate one (RFC 0003). Not a loss - each subscriber "+
			"is sent what is current - but the only thing that says a latest subscriber "+
			"reads more slowly than its topics change.")
	for _, c := range n.channels {
		if c.kind == string(channel.Latest) {
			m.value("saguin_latest_superseded_total", c.superseded, "channel", c.name)
		}
	}

	// Two gauges among the counters, because they are the queue's own
	// numbers and a reader asking "what is in this queue" should not have to
	// know which kind each one is.
	m.declare("saguin_queue_depth", "gauge", "Unresolved work held by the queue.")
	for _, c := range n.channels {
		if c.queue {
			m.value("saguin_queue_depth", c.depth, "channel", c.name)
		}
	}
	m.declare("saguin_queue_inflight", "gauge", "Records out with a worker now.")
	for _, c := range n.channels {
		if c.queue {
			m.value("saguin_queue_inflight", c.inflight, "channel", c.name)
		}
	}

	// The queue series carry only queue channels. An append channel with a
	// zero here would read as a queue nothing has happened on, which is a
	// different statement from "not a queue".
	for _, q := range []struct {
		name, help string
		of         func(channelNumbers) float64
	}{
		{"saguin_queue_delivered_total", "Records handed to a worker.",
			func(c channelNumbers) float64 { return c.delivered }},
		{"saguin_queue_acknowledged_total", "Records a worker acknowledged.",
			func(c channelNumbers) float64 { return c.acked }},
		{"saguin_queue_returned_total",
			"Records handed back by a worker and made available again. " +
				"A return that spends the last attempt is dead-lettered instead " +
				"and counted there, not here.",
			func(c channelNumbers) float64 { return c.returned }},
		{"saguin_queue_redelivered_total", "Records taken back on a visibility timeout.",
			func(c channelNumbers) float64 { return c.redeliver }},
		{"saguin_queue_dead_lettered_total",
			"Records moved to a dead-letter channel, however the last attempt ended.",
			func(c channelNumbers) float64 { return c.deadLetr }},
		{"saguin_queue_expired_total", "Work that aged out unresolved.",
			func(c channelNumbers) float64 { return c.expired }},
		{"saguin_queue_retain_ignored_total",
			"Retained publishes this queue took as ordinary work, with the flag dropped. " +
				"A queue holds work rather than state and grants no subscription a retained " +
				"message could be delivered to, so there is nothing for the flag to ask for. " +
				"It rises when a producer believes it is setting state on a topic a queue " +
				"claims, which is a configuration question rather than a broker fault - and " +
				"this series is the only place it is visible.",
			func(c channelNumbers) float64 { return c.retainIgnored }},
	} {
		m.declare(q.name, "counter", q.help)
		for _, c := range n.channels {
			if c.queue {
				m.value(q.name, q.of(c), "channel", c.name)
			}
		}
	}
}
