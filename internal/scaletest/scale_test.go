// Package scaletest is the scale-run harness: `make scale ROLE=…` on each
// machine, in-process Paho clients, never part of `make check`. One role
// runs per machine and the roles coordinate through the broker itself.
package scaletest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// cfg is everything `make scale` passes through the environment. One
// struct, read once, printed once, so the report never guesses what the
// run was asked for.
type cfg struct {
	role     string // consumers | publishers | replay | say | watch | kvget
	broker   string // host:port, MQTT over TLS
	ops      string // host:port, the operations listener (http)
	ca       string // path to ca.pem
	user     string
	pass     string
	admin    string // operations credential, for /metrics
	adminPw  string
	duration time.Duration // the full-beam window (SCALE)
	clients  int           // this role's client count
	peers    int           // the other role's count, for the topic mapping
	rate     float64       // publishes a second per publisher
	ramp     int           // clients brought up per stage
	settle   time.Duration // read-on after run/done
	report   string        // path of this role's report.md
	topic    string        // say/kvget
	msg      string        // say
	seed     int64

	// The scenario (SCENARIO): fleet, today's thirty thousand thin clients,
	// or gateway, a few fat publishers into a few wide consumers. The rest
	// of this block is gateway only, and fleet leaves every one of them
	// empty, so a fleet run's topics, ids, markers and reports are the ones
	// it always had.
	scenario string
	size     int      // payload bytes a publish is padded to (SIZE); 0 is unpadded
	cohort   string   // this role's cohort (COHORT), in every client id and topic it owns
	shape    string   // a consumer cohort's filters (SHAPE): wide or narrow
	ready    []string // consumer cohorts a publisher role waits for (READY)
	done     []string // publisher cohorts a consumer role waits for (DONE)
	target   int      // connections a publisher role proves before its clock starts (TARGET)
	// plaintext is set by the in-process smoke tests and by nothing a run
	// passes: a scale run measures TLS and never skips it.
	plaintext bool
	// patience, when set, replaces the roles' waits on each other - thirty
	// minutes for consumers-ready, twenty past the run for run/done - so a
	// smoke test whose roles never meet fails in seconds rather than at the
	// test binary's timeout. A run never sets it.
	patience time.Duration

	// retention only: the append channels under test as name:prefix, the
	// size of each cohort, and how hard a slow reader is held back.
	channels   []retentionChannel
	fast       int
	slow       int
	publishers int
	pause      time.Duration
	pad        int
	log        string // the broker's log file, when the role is given one
}

func readCfg(t *testing.T) cfg {
	get := func(k, d string) string {
		if v := os.Getenv("SAGUIN_SCALE_" + k); v != "" {
			return v
		}
		return d
	}
	num := func(k string, d int) int {
		n, err := strconv.Atoi(get(k, strconv.Itoa(d)))
		if err != nil || n < 1 {
			t.Fatalf("SAGUIN_SCALE_%s=%q is not a positive integer", k, get(k, ""))
		}
		return n
	}
	c := cfg{
		role:   os.Getenv("SAGUIN_SCALE_ROLE"),
		broker: get("BROKER", ""),
		ops:    get("OPS", ""),
		ca:     get("CA", ""),
		user:   get("USER", ""), pass: get("PASS", ""),
		admin: get("ADMIN_USER", "admin"), adminPw: get("ADMIN_PASS", ""),
		report: get("REPORT", "scale-report.md"),
		topic:  get("TOPIC", ""), msg: get("MSG", ""),
	}
	var err error
	if c.duration, err = time.ParseDuration(get("DURATION", "30m")); err != nil {
		t.Fatalf("SAGUIN_SCALE_DURATION: %v", err)
	}
	if c.settle, err = time.ParseDuration(get("SETTLE", "60s")); err != nil {
		t.Fatalf("SAGUIN_SCALE_SETTLE: %v", err)
	}
	defClients := 20000
	if c.role == "publishers" {
		defClients = 10000
	}
	c.clients = num("CLIENTS", defClients)
	defPeers := 10000
	if c.role == "publishers" {
		defPeers = 20000
	}
	c.peers = num("PEERS", defPeers)
	c.ramp = num("RAMP", 500)
	if c.rate, err = strconv.ParseFloat(get("RATE", "1"), 64); err != nil || c.rate <= 0 {
		t.Fatalf("SAGUIN_SCALE_RATE=%q is not a positive number", get("RATE", "1"))
	}
	c.seed = time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		if c.seed, err = strconv.ParseInt(v, 10, 64); err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q: %v", v, err)
		}
	}
	if c.broker == "" {
		t.Fatal("SAGUIN_SCALE_BROKER is required (host:port)")
	}
	c.scenario = get("SCENARIO", "fleet")
	switch c.scenario {
	case "fleet":
	case "gateway":
		c.size = num("SIZE", 16<<10)
		c.cohort, c.shape = get("COHORT", ""), get("SHAPE", "wide")
		// The replay owns no topics and names no clients, so it has no
		// cohort; it needs the scenario only to read the gateway's channels.
		if c.role != "replay" && !cohortPattern.MatchString(c.cohort) {
			t.Fatalf("SAGUIN_SCALE_COHORT=%q: a gateway role names its cohort in lower-case "+
				"letters, digits and dashes, such as local or air, because it goes into every "+
				"client id and topic the role owns", c.cohort)
		}
		if c.shape != "wide" && c.shape != "narrow" {
			t.Fatalf("SAGUIN_SCALE_SHAPE=%q: wide or narrow", c.shape)
		}
		list := func(k string) []string {
			var out []string
			for _, s := range strings.Split(get(k, ""), ",") {
				if s = strings.TrimSpace(s); s != "" {
					if !cohortPattern.MatchString(s) {
						t.Fatalf("SAGUIN_SCALE_%s names %q, which is not a cohort", k, s)
					}
					out = append(out, s)
				}
			}
			return out
		}
		c.ready, c.done = list("READY"), list("DONE")
		switch {
		case c.role == "publishers" && len(c.ready) == 0:
			t.Fatal("SAGUIN_SCALE_READY is required for gateway publishers: the consumer " +
				"cohorts whose consumers-ready this role waits for, comma-separated")
		case c.role == "consumers" && len(c.done) == 0:
			t.Fatal("SAGUIN_SCALE_DONE is required for gateway consumers: the publisher " +
				"cohorts whose run/done this role waits for, comma-separated")
		}
		if c.role == "publishers" {
			c.target = num("TARGET", c.clients+c.peers)
		}
	default:
		t.Fatalf("SAGUIN_SCALE_SCENARIO=%q: fleet or gateway", c.scenario)
	}
	if c.role == "retention" {
		c.fast, c.slow = num("FAST", 12), num("SLOW", 6)
		c.publishers, c.pad = num("PUBLISHERS", 4), num("PAD", 400)
		if c.pause, err = time.ParseDuration(get("PAUSE", "120ms")); err != nil {
			t.Fatalf("SAGUIN_SCALE_PAUSE: %v", err)
		}
		for _, pair := range strings.Split(get("CHANNELS", ""), ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			name, prefix, ok := strings.Cut(pair, ":")
			if !ok || name == "" || prefix == "" {
				t.Fatalf("SAGUIN_SCALE_CHANNELS=%q: each entry is name:topic-prefix, "+
					"such as trim-db:trim/db", get("CHANNELS", ""))
			}
			c.channels = append(c.channels, retentionChannel{name: name, prefix: prefix})
		}
		if len(c.channels) == 0 {
			t.Fatal("SAGUIN_SCALE_CHANNELS is required for the retention role: the append " +
				"channels under test, as name:topic-prefix pairs")
		}
	}
	return c
}

// runlog is the run's own record: every line UTC-stamped, written to
// stderr as it happens and kept for the report, so nothing the run did
// has to be reconstructed afterwards.
type runlog struct {
	mu    sync.Mutex
	lines []string
}

func (l *runlog) logf(format string, a ...any) {
	line := time.Now().UTC().Format("2006-01-02T15:04:05Z") + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	fmt.Fprintln(os.Stderr, line)
}

// losses records every client whose connection ended before the run did.
// Without it a consumer that dropped at minute five would simply stop
// counting, and "no miss" would hold for the rest of the run over nothing.
// Each client is counted once; Paho may report one loss several times.
type losses struct {
	log     *runlog
	seen    sync.Map
	n       atomic.Int64
	mu      sync.Mutex
	first   string
	closing atomic.Bool // set before the harness disconnects its own clients
}

func (l *losses) lost(id, why string) {
	if l == nil || l.closing.Load() {
		return
	}
	if _, dup := l.seen.LoadOrStore(id, true); dup {
		return
	}
	k := l.n.Add(1)
	l.mu.Lock()
	if l.first == "" {
		l.first = id + ": " + why
	}
	l.mu.Unlock()
	switch {
	case k <= 50:
		l.log.logf("LOST %s: %s", id, why)
	case k == 51:
		l.log.logf("LOST: more than 50 clients lost; further losses are counted, not logged")
	}
}

// was says whether this client was lost during the run. A topic whose
// consumers all went away stops advancing, and its tail comparison then
// measures the client's death rather than the broker: the 2026-09-22 run
// read "not yet delivered" as "missing" for 2,073 of them.
func (l *losses) was(id string) bool {
	if l == nil {
		return false
	}
	_, ok := l.seen.Load(id)
	return ok
}

func (l *losses) assertion() assertion {
	l.mu.Lock()
	defer l.mu.Unlock()
	return assertion{"no client lost its connection during the run", l.n.Load() == 0,
		fmt.Sprintf("%d lost; first: %s", l.n.Load(), l.first)}
}

// assertion is one row of the report's closing table.
type assertion struct {
	name     string
	held     bool
	evidence string
}

// TestScaleRole is the whole harness. Without SAGUIN_SCALE_ROLE it skips,
// which is what keeps this package out of `make check`; `make scale` sets
// the role and everything else.
func TestScaleRole(t *testing.T) {
	role := os.Getenv("SAGUIN_SCALE_ROLE")
	if role == "" {
		t.Skip("scale harness: set SAGUIN_SCALE_ROLE (via make scale) to run")
	}
	// The head-to-head reads its own configuration: it shares none of the
	// fleet's topics, credentials or TLS (compare_test.go).
	if role == "compare" {
		runCompare(t)
		return
	}
	c := readCfg(t)
	useScenario(t, c.scenario)
	log := &runlog{}
	log.logf("role=%s broker=%s clients=%d peers=%d ramp=%d rate=%g duration=%s settle=%s seed=%d",
		c.role, c.broker, c.clients, c.peers, c.ramp, c.rate, c.duration, c.settle, c.seed)
	if c.scenario == "gateway" {
		log.logf("scenario=gateway cohort=%s shape=%s size=%d ready=%s done=%s target=%d",
			c.cohort, c.shape, c.size, strings.Join(c.ready, ","), strings.Join(c.done, ","), c.target)
	}

	switch role {
	case "consumers":
		runConsumers(t, c, log)
	case "publishers":
		runPublishers(t, c, log)
	case "say":
		runSay(t, c, log)
	case "watch":
		runWatch(t, c, log)
	case "kvget":
		runKVGet(t, c, log)
	case "replay":
		runReplay(t, c, log)
	case "retention":
		runRetention(t, c, log)
	default:
		t.Fatalf("SAGUIN_SCALE_ROLE=%q: not consumers, publishers, replay, retention, "+
			"say, watch or kvget", role)
	}
}

// ---------------------------------------------------------------- transport

func tlsConfig(t *testing.T, caPath string) *tls.Config {
	if caPath == "" {
		t.Fatal("SAGUIN_SCALE_CA is required: the run measures TLS, never skips it")
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("reading the authority: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatalf("%s holds no certificate this run can trust", caPath)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

// roleTLS is tlsConfig for the fleet and gateway roles, and nil - plaintext -
// only where an in-process smoke test set plaintext, which no run can.
func roleTLS(t *testing.T, c cfg) *tls.Config {
	if c.plaintext {
		return nil
	}
	return tlsConfig(t, c.ca)
}

// dial makes one connected MQTT client. Handlers are installed before
// paho.NewClient: paho starts its receive goroutine inside Connect, and a
// resumed session is pumped its backlog before any SUBSCRIBE, so a handler
// attached later misses records and the miss reads as the broker's fault.
// recvMax of 0 is paho's own default of 65,535, which every fleet role
// wants; the retention role pins a slow reader to 1 so that a pause per
// record really holds it behind the floor rather than letting the broker
// run tens of thousands of records ahead of it.
func dial(c cfg, tc *tls.Config, id string, cleanStart bool, expiry uint32,
	onMsg func(paho.PublishReceived), lost *losses, recvMax uint16) (*paho.Client, error) {
	var conn net.Conn
	var err error
	// **A nil tls.Config is plaintext, and only one role may ask for it.**
	// The fleet roles measure TLS and tlsConfig refuses to be skipped; the
	// retention role runs against a broker on the same machine, where the
	// question is what retention does to a reader and the transport is not
	// part of it. The in-process smoke tests are the other caller (roleTLS),
	// through a field no run can set.
	if tc == nil {
		conn, err = net.DialTimeout("tcp", c.broker, 20*time.Second)
	} else {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 20 * time.Second}, "tcp", c.broker, tc)
	}
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	// A *tls.Conn is not safe for concurrent packet writes: Paho writes a
	// packet as several Writes, and holds a per-packet lock only when the
	// connection implements sync.Locker. Unwrapped, the keepalive PINGREQ
	// lands inside a SUBSCRIBE or PUBACK and the broker rightly closes the
	// connection as malformed.
	pcfg := paho.ClientConfig{Conn: packets.NewThreadSafeConn(conn), ClientID: id}
	if onMsg != nil {
		pcfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) { onMsg(pr); return true, nil },
		}
	}
	if lost != nil {
		pcfg.OnClientError = func(err error) { lost.lost(id, err.Error()) }
		pcfg.OnServerDisconnect = func(d *paho.Disconnect) {
			lost.lost(id, fmt.Sprintf("server sent DISCONNECT 0x%02X", d.ReasonCode))
		}
	}
	cl := paho.NewClient(pcfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	props := &paho.ConnectProperties{SessionExpiryInterval: &expiry}
	if recvMax > 0 {
		props.ReceiveMaximum = &recvMax
	}
	// **The flags follow the values.** A password flag set over an empty
	// password is a protocol violation and the broker rightly refuses it -
	// so a run against a broker with no password_file, which is what the
	// retention role's own broker is, connects anonymously rather than
	// claiming a credential it does not have.
	conn2 := &paho.Connect{
		ClientID: id, CleanStart: cleanStart, KeepAlive: 60,
		Properties: props,
	}
	if c.user != "" {
		conn2.Username, conn2.UsernameFlag = c.user, true
	}
	if c.pass != "" {
		conn2.Password, conn2.PasswordFlag = []byte(c.pass), true
	}
	ca, err := cl.Connect(ctx, conn2)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, fmt.Errorf("connect refused 0x%02X", ca.ReasonCode)
	}
	return cl, nil
}

// connLine matches the connections gauge, labelled or not.
var connLine = regexp.MustCompile(`(?m)^saguin_connections(?:\{[^}]*\})? ([0-9.eE+]+)$`)

// connections reads saguin_connections off /metrics as the operations
// credential, summing labelled series if the gauge carries any.
func connections(c cfg) (int, error) {
	if c.ops == "" {
		return 0, fmt.Errorf("SAGUIN_SCALE_OPS is required: the harness proves its target on /metrics")
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/metrics", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("/metrics answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return 0, err
	}
	total, found := 0.0, false
	for _, m := range connLine.FindAllStringSubmatch(string(body), -1) {
		v, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, err
		}
		total += v
		found = true
	}
	if !found {
		return 0, fmt.Errorf("saguin_connections is not on /metrics")
	}
	return int(total), nil
}

// ------------------------------------------------------------- assignments

// The fifteen channels and broadcast, with the share of each role's
// clients that lands on each. The counts are derived from these weights
// and the role's totals alone, identically on both machines, so the two
// sides agree on the topic map without a byte of coordination.
type target struct {
	name   string  // channel name, or "bcast"
	prefix string  // topic prefix: prefix + "/p" + ordinal
	kind   string  // append | latest | queue | bcast
	subW   float64 // share of consumers
	pubW   float64 // share of publishers
}

// targets is the scenario's table, which every role reads: the fleet's
// unless useScenario chose the gateway's.
var targets = fleetTargets

var fleetTargets = []target{
	{"evt-1", "scale/evt/1", "append", 0.20, 0.20},
	{"evt-2", "scale/evt/2", "append", 0.08, 0.08},
	{"evt-3", "scale/evt/3", "append", 0.04, 0.04},
	{"evt-4", "scale/evt/4", "append", 0.02, 0.02},
	{"evt-5", "scale/evt/5", "append", 0.01, 0.01},
	{"kv-1", "scale/kv/1", "latest", 0.15, 0.15},
	{"kv-2", "scale/kv/2", "latest", 0.06, 0.06},
	{"kv-3", "scale/kv/3", "latest", 0.03, 0.03},
	{"kv-4", "scale/kv/4", "latest", 0.02, 0.02},
	{"kv-5", "scale/kv/5", "latest", 0.01, 0.01},
	{"jobs-1", "scale/jobs/1", "queue", 0.005, 0.01},
	{"jobs-2", "scale/jobs/2", "queue", 0.005, 0.01},
	{"jobs-3", "scale/jobs/3", "queue", 0.005, 0.01},
	{"jobs-4", "scale/jobs/4", "queue", 0.005, 0.01},
	{"jobs-5", "scale/jobs/5", "queue", 0.005, 0.01},
	{"bcast", "scale/bcast", "bcast", 0, 0}, // both remainders land here
}

// gatewayTargets is the IIoT plant: few fat publishers spread over one
// append channel, one latest channel and broadcast, read by a few consumers
// that each take everything (wide) and a few over the air that each take
// one stream (narrow). A consumer's weights are unused: its filter is its
// shape, not a slot.
var gatewayTargets = []target{
	{"gw-evt", "scale/gw/evt", "append", 0, 0.60},
	{"gw-kv", "scale/gw/kv", "latest", 0, 0.20},
	{"gw-bcast", "scale/gw/bcast", "bcast", 0, 0}, // the publishers' remainder
}

// cohortPattern is what a cohort may be called: it is a level of every topic
// its publishers own and part of every client id, so nothing a filter or an
// acl_file pattern would read as more than one level.
var cohortPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

// useScenario points every role at the scenario's table, for this test.
func useScenario(t *testing.T, scenario string) {
	if scenario == "gateway" {
		targets = gatewayTargets
		t.Cleanup(func() { targets = fleetTargets })
	}
}

// pubTopicOf is the topic publisher ordinal ord of this role publishes to.
// A gateway cohort's topics carry its name, because two machines publish at
// once and their ordinals would otherwise be the same topics.
func pubTopicOf(c cfg, tg target, ord int) string {
	if c.cohort == "" {
		return pubTopic(tg, ord)
	}
	return tg.prefix + "/" + c.cohort + "/p" + strconv.Itoa(ord)
}

// clientID names this role's client i, with its cohort where it has one, so
// no two machines' clients take each other's sessions and a report's lost
// client says which cohort it was in.
func clientID(c cfg, kind string, i int) string {
	if c.cohort == "" {
		return fmt.Sprintf("scale-%s-%d", kind, i)
	}
	return fmt.Sprintf("scale-%s-%s-%d", kind, c.cohort, i)
}

// coordID is a role's coordination client id. Fixed in the fleet, where each
// role runs once; with its cohort in a gateway run, where two consumer roles
// and two publisher roles are on the air at once and one fixed id would have
// each take over the other's session.
func coordID(c cfg, id string) string {
	if c.cohort == "" {
		return id
	}
	return id + "-" + c.cohort
}

// marker is a run marker's topic. A gateway run has two consumer roles and
// two publisher roles on the air at once, so each marks its own cohort's,
// and a role waits for the cohorts it was told to (READY, DONE) rather than
// for whichever of them wrote the one shared topic last.
func marker(name, cohort string) string {
	if cohort == "" {
		return "scale/state/run/" + name
	}
	return "scale/state/run/" + name + "/" + cohort
}

// gatewayFilter is what gateway consumer i subscribes to, filters separated
// by a space. Wide takes the whole plant, as one filter per target in one
// SUBSCRIBE: `scale/gw/#` would reach broadcast space no role is granted,
// and the acl_file refuses the whole SUBSCRIBE for it. Narrow takes one
// publisher ordinal on one target from every publishing cohort - a cloud
// bridge or a remote dashboard's shape.
func gatewayFilter(c cfg, i int) string {
	if c.shape == "wide" {
		fs := make([]string, 0, len(targets))
		for _, tg := range targets {
			fs = append(fs, tg.prefix+"/#")
		}
		return strings.Join(fs, " ")
	}
	tg := targets[i%len(targets)]
	return tg.prefix + "/+/p" + strconv.Itoa(i/len(targets))
}

// padded is a record's payload: its seq and timestamp, then padding to size
// bytes where the scenario asks for records of a size.
func padded(seq uint64, size int) []byte {
	p := fmt.Sprintf("seq=%d ts=%d", seq, time.Now().UnixNano())
	if size > len(p)+1 {
		p += " " + strings.Repeat("x", size-len(p)-1)
	}
	return []byte(p)
}

// latHist is one publisher's PUBACK latencies, in buckets 25% apart from
// 50us to about a minute: a bounded record of a figure a thirty-minute run
// takes millions of, whose percentiles are read to within a bucket.
type latHist struct {
	n     [64]uint64
	count uint64
	max   time.Duration
}

func latBound(i int) time.Duration {
	return time.Duration(float64(50*time.Microsecond) * math.Pow(1.25, float64(i)))
}

func (h *latHist) add(d time.Duration) {
	i := 0
	for i < len(h.n)-1 && d > latBound(i) {
		i++
	}
	h.n[i]++
	h.count++
	h.max = max(h.max, d)
}

func (h *latHist) merge(o *latHist) {
	for i := range h.n {
		h.n[i] += o.n[i]
	}
	h.count += o.count
	h.max = max(h.max, o.max)
}

// quantile is the upper bound of the bucket holding quantile q, and never
// more than the largest latency seen: a bucket's bound can pass it, and a
// p99 printed above the max reads as a measurement error.
func (h *latHist) quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	want := uint64(math.Ceil(q * float64(h.count)))
	var seen uint64
	for i, n := range h.n {
		if seen += n; seen >= want {
			return min(latBound(i), h.max).Round(time.Microsecond)
		}
	}
	return h.max.Round(time.Microsecond)
}

// counts spreads n clients over the targets by the given weight, the
// remainder on bcast. Deterministic, so both roles compute the same map.
func counts(n int, pub bool) []int {
	out := make([]int, len(targets))
	used := 0
	for i, tg := range targets[:len(targets)-1] {
		w := tg.subW
		if pub {
			w = tg.pubW
		}
		out[i] = int(float64(n) * w)
		used += out[i]
	}
	out[len(targets)-1] = n - used
	return out
}

// slot is one client's place in the world: which target, which ordinal
// within it. A publisher's ordinal names its topic; a consumer's ordinal
// picks the publisher stream it follows.
type slot struct {
	tgt int
	ord int
}

func slots(n int, pub bool) []slot {
	cs := counts(n, pub)
	out := make([]slot, 0, n)
	for ti, c := range cs {
		for o := 0; o < c; o++ {
			out = append(out, slot{tgt: ti, ord: o})
		}
	}
	return out
}

// hotShare sets the hot stream's audience: as many consumers as a tenth of
// the channel's publisher count follow its publisher 0, so a few topics
// carry hundreds of subscribers while the rest carry a handful - the
// fan-out shapes the run has to answer for.
const hotShare = 0.10

// pubTopic is publisher stream ord's topic on a target.
func pubTopic(tg target, ord int) string {
	return tg.prefix + "/p" + strconv.Itoa(ord)
}

// subTopic is what consumer ordinal ord on a target subscribes to. Queue
// workers take the queue's whole form; everything else follows one
// publisher stream, the first hotShare of them all following stream 0.
func subTopic(tg target, ord, pubCount int) string {
	if tg.kind == "queue" {
		return "$saguin/queue/" + tg.name
	}
	if pubCount < 1 {
		pubCount = 1
	}
	if float64(ord) < hotShare*float64(pubCount) {
		return pubTopic(tg, 0)
	}
	return pubTopic(tg, ord%pubCount)
}

// ------------------------------------------------------------------- ramp

// stageDeadline and stagePause pace the ramp: the longest a stage may
// take before the ramp stops, and the rest between stages.
const (
	stageDeadline = 2 * time.Minute
	stagePause    = 2 * time.Second
)

// rampUp brings clients up in stages rather than all at once. A stage is
// gated on the broker's own answers - every CONNACK and SUBACK in it - not
// on /metrics, which the broker caches for min_scrape_interval. A stage
// that does not finish within stageDeadline, or in which any client fails,
// stops the ramp at the count that held: a stuck broker becomes a logged
// finding, never a hang. Every stage is logged with its time, which is the
// connect curve the report is read against.
func rampUp(c cfg, log *runlog, bring func(i int) error) (int, []string) {
	done := 0
	var failures []string
	for done < c.clients {
		stage := c.ramp
		if done+stage > c.clients {
			stage = c.clients - done
		}
		t0 := time.Now()
		var wg sync.WaitGroup
		var mu sync.Mutex
		gate := make(chan struct{}, 100)
		finished := make(chan struct{})
		go func(from, to int) {
			for i := from; i < to; i++ {
				wg.Add(1)
				gate <- struct{}{}
				go func(i int) {
					defer wg.Done()
					defer func() { <-gate }()
					if err := bring(i); err != nil {
						mu.Lock()
						failures = append(failures, fmt.Sprintf("client %d: %v", i, err))
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			close(finished)
		}(done, done+stage)
		select {
		case <-finished:
		case <-time.After(stageDeadline):
			msg := fmt.Sprintf("RAMP stuck: stage %d..%d did not finish within %s",
				done, done+stage, stageDeadline)
			log.logf("%s", msg)
			mu.Lock()
			failures = append(failures, msg)
			mu.Unlock()
			return done, failures
		}
		if len(failures) > 0 {
			log.logf("RAMP stopped at %d/%d: %d clients failed, first: %s",
				done, c.clients, len(failures), failures[0])
			return done, failures
		}
		done += stage
		log.logf("RAMP stage done: %d/%d connected, stage took %s",
			done, c.clients, time.Since(t0).Round(time.Millisecond))
		if done < c.clients {
			time.Sleep(stagePause)
		}
	}
	return done, nil
}

// proveTarget is the contract's gate before the clock starts: /metrics must
// read at least want connections. want is absolute - this role's clients
// plus any the other role already holds - so a cached answer from before
// the ramp can never satisfy it. It waits out the cache, and says so.
func proveTarget(c cfg, log *runlog, want int) error {
	deadline := time.Now().Add(3 * time.Minute)
	last := -1
	for time.Now().Before(deadline) {
		n, err := connections(c)
		if err != nil {
			log.logf("TARGET metrics read failed: %v", err)
		} else {
			last = n
			if n >= want {
				log.logf("TARGET proven: saguin_connections=%d >= %d", n, want)
				return nil
			}
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("saguin_connections read %d, never reached %d within 3m", last, want)
}

// -------------------------------------------------------------- consumers

// topicRow is one append or broadcast topic's raw tail evidence: what the
// consumers following it reached, and whether they were all still there to
// reach it. The role writes these rows out unjudged (`-topics.tsv` beside
// the report) and the report role joins them with the publishers' own rows,
// so that a comparison which a dead client can move is never presented as
// an equality the broker failed.
type topicRow struct {
	target        string
	maxSeq        uint64
	records       int64
	gaps          int
	dups          int
	consumers     int
	lostConsumers int
	survived      bool
}

// stream is what one consumer saw of the one stream it follows. Misses are
// judged on the publisher's seq, contiguous from 1 per topic: an append
// channel's saguin-offset counts every topic in the channel, so one topic's
// offsets rise with holes that are other topics' records (RFC 0003,
// "Which channel a record came from"). The offset judges order only.
type stream struct {
	mu       sync.Mutex
	lastSeq  uint64 // highest seq seen
	maxOff   uint64 // highest saguin-offset seen (append)
	count    int64
	gaps     int
	dups     int
	disorder int
	gapEv    string
	orderEv  string
}

// consumerStreams is what one consumer saw, one stream per topic. A fleet
// consumer follows one topic, so it holds one stream, made when it
// subscribes; a wide gateway consumer takes the whole plant and holds one
// per topic that reached it, because seq is contiguous per publisher topic
// and a single sequence over interleaved topics would read as gaps.
type consumerStreams struct {
	mu sync.Mutex
	by map[string]*stream
}

func (cs *consumerStreams) get(topic string) *stream {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	s := cs.by[topic]
	if s == nil {
		s = &stream{}
		cs.by[topic] = s
	}
	return s
}

// targetFor is the target a topic belongs to, by its prefix.
func targetFor(topic string) (int, bool) {
	for i, tg := range targets {
		if strings.HasPrefix(topic, tg.prefix+"/") {
			return i, true
		}
	}
	return 0, false
}

func runConsumers(t *testing.T, c cfg, log *runlog) {
	tc := roleTLS(t, c)
	gateway := c.scenario == "gateway"
	started := time.Now().UTC().Truncate(time.Second)
	// A record stamped before this role started is an earlier run's history
	// or a latest channel's current state, never one this run asked for.
	startNs := time.Now().UnixNano()
	lost := &losses{log: log}

	// The markers live on a latest channel, which hands its current value
	// to every new subscriber, so a marker left by an earlier run on the
	// same broker would read as this run's. This role clears the one it
	// owns before anything else, and trusts a run/done only if it was
	// stamped after this role started.
	sender, err := dial(c, tc, coordID(c, "scale-consumer-say"), true, 0, nil, nil, 0)
	if err != nil {
		t.Fatalf("coordination sender: %v", err)
	}
	defer sender.Disconnect(&paho.Disconnect{ReasonCode: 0})
	if _, err := sender.Publish(context.Background(), &paho.Publish{
		Topic: marker("consumers-ready", c.cohort), QoS: 1, Payload: nil,
	}); err != nil {
		t.Fatalf("clearing consumers-ready: %v", err)
	}
	// The fleet's topic map is derived from the two roles' counts; a gateway
	// consumer's filter is its shape (gatewayFilter) and takes no slot.
	var subSlots []slot
	var pubCounts []int
	if !gateway {
		subSlots = slots(c.clients, false)
		pubCounts = counts(c.peers, true)
	}

	streams := make([]consumerStreams, c.clients)
	for i := range streams {
		streams[i].by = map[string]*stream{}
	}
	subs := make([]string, c.clients) // what consumer i subscribed to
	clients := make([]*paho.Client, c.clients)
	var delivered, foreign atomic.Int64
	// The consumer's answers to the queue jobs it is offered, counted as the
	// publishers count theirs: a refused or failed ack leaves a job
	// unresolved, and until now it showed only as a redelivery a visibility
	// timeout later - never within the smoke's run.
	var acksSent, ackErrors atomic.Int64
	var ackMu sync.Mutex
	ackRefusals := map[byte]int64{}
	firstAckErr := ""
	// Gateway only: payload bytes this cohort received, and the first and
	// last arrival, which bound the window its MB/s is measured over.
	var gotBytes, firstAt, lastAt atomic.Int64

	// One job, one worker: every queue delivery claims its job and attempt
	// here. saguin-channel is sent only where a filter reaches two channels
	// (RFC 0003), so the job is the worker's queue name and saguin-offset.
	// Two workers holding the same attempt is the duplication the run exists
	// to catch; a higher attempt is the redelivery the RFC permits.
	var jobMu sync.Mutex
	attemptOwner := map[string]int{} // queue/offset#attempt -> worker
	jobTarget := map[string]int{}    // queue/offset -> target, this run's jobs
	var doubleJobs []string
	var redeliveries int64

	handler := func(i int) func(paho.PublishReceived) {
		return func(pr paho.PublishReceived) {
			delivered.Add(1)
			// A fleet consumer's one stream is its subscription's; a gateway
			// consumer's is the record's own topic.
			ti, key := 0, subs[i]
			if gateway {
				var ok bool
				if ti, ok = targetFor(pr.Packet.Topic); !ok {
					foreign.Add(1)
					return
				}
				key = pr.Packet.Topic
				now := time.Now().UnixNano()
				gotBytes.Add(int64(len(pr.Packet.Payload)))
				firstAt.CompareAndSwap(0, now)
				lastAt.Store(now)
			} else {
				ti = subSlots[i].tgt
			}
			tg := targets[ti]
			s := streams[i].get(key)
			var off uint64
			var haveOff bool
			offStr, attempt := "", ""
			if pr.Packet.Properties != nil {
				offStr = pr.Packet.Properties.User.Get("saguin-offset")
				attempt = pr.Packet.Properties.User.Get("saguin-attempt")
				if n, err := strconv.ParseUint(offStr, 10, 64); err == nil {
					off, haveOff = n, true
				}
			}
			payload := string(pr.Packet.Payload)
			seq := seqOf(payload)
			thisRun := seq > 0 && tsOf(payload) >= startNs
			switch tg.kind {
			case "append", "bcast":
				if !thisRun {
					foreign.Add(1)
					return
				}
				s.mu.Lock()
				switch {
				case seq > s.lastSeq+1:
					s.gaps++
					if s.gapEv == "" {
						s.gapEv = fmt.Sprintf("seq %d after %d on %s", seq, s.lastSeq, pr.Packet.Topic)
					}
				case seq <= s.lastSeq:
					s.dups++
				}
				if tg.kind == "append" && haveOff {
					if seq > s.lastSeq && off < s.maxOff {
						s.disorder++
						if s.orderEv == "" {
							s.orderEv = fmt.Sprintf("seq %d at offset %d after offset %d on %s",
								seq, off, s.maxOff, pr.Packet.Topic)
						}
					}
					if off > s.maxOff {
						s.maxOff = off
					}
				}
				if seq > s.lastSeq {
					s.lastSeq = seq
				}
				s.count++
				s.mu.Unlock()
			case "latest":
				if !thisRun {
					foreign.Add(1)
					return
				}
				s.mu.Lock()
				if seq < s.lastSeq {
					s.disorder++
					if s.orderEv == "" {
						s.orderEv = fmt.Sprintf("seq %d after %d on %s", seq, s.lastSeq, pr.Packet.Topic)
					}
				}
				if seq > s.lastSeq {
					s.lastSeq = seq
				}
				s.count++
				s.mu.Unlock()
			case "queue":
				if thisRun {
					key := tg.name + "/" + offStr
					jobMu.Lock()
					if owner, seen := attemptOwner[key+"#"+attempt]; seen && owner != i {
						doubleJobs = append(doubleJobs, fmt.Sprintf(
							"job %s attempt %s reached workers %d and %d", key, attempt, owner, i))
					}
					attemptOwner[key+"#"+attempt] = i
					jobTarget[key] = ti
					if attempt != "1" {
						redeliveries++
					}
					jobMu.Unlock()
					s.mu.Lock()
					s.count++
					s.mu.Unlock()
				} else {
					foreign.Add(1)
				}
				// Every job is acknowledged, an earlier run's too, so none
				// waits out its visibility timeout and returns.
				if pr.Packet.Properties != nil && pr.Packet.Properties.ResponseTopic != "" {
					resp, err := pr.Client.Publish(context.Background(), &paho.Publish{
						Topic: pr.Packet.Properties.ResponseTopic, QoS: 1,
						Payload:    []byte("ack"),
						Properties: &paho.PublishProperties{CorrelationData: pr.Packet.Properties.CorrelationData},
					})
					acksSent.Add(1)
					// **One path: paho v0.23 answers a PUBACK at 0x80 or above
					// with an error**, and hands the PUBACK back beside it, so
					// a refusal is an error whose response carries its code.
					if err != nil {
						ackErrors.Add(1)
						ackMu.Lock()
						if resp != nil {
							ackRefusals[resp.ReasonCode]++
						}
						if firstAckErr == "" {
							firstAckErr = fmt.Sprintf("%s: %v", pr.Packet.Topic, err)
						}
						ackMu.Unlock()
					}
				}
			}
		}
	}

	// Every subscription is fixed before any dial: a resumed session is
	// pumped its backlog before any SUBSCRIBE (dial), and the handler keys a
	// fleet stream by it. A fleet consumer's one stream exists from here,
	// whether or not the ramp reaches it, as it always did.
	for i := range subs {
		if gateway {
			subs[i] = gatewayFilter(c, i)
			continue
		}
		tg := targets[subSlots[i].tgt]
		subs[i] = subTopic(tg, subSlots[i].ord, pubCounts[subSlots[i].tgt])
		streams[i].get(subs[i])
	}

	bring := func(i int) error {
		topic := subs[i]
		cl, err := dial(c, tc, clientID(c, "c", i), true, 7200, handler(i), lost, 0)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// One filter in the fleet; a wide gateway consumer's several, in one
		// SUBSCRIBE (gatewayFilter).
		var opts []paho.SubscribeOptions
		for _, f := range strings.Fields(topic) {
			opts = append(opts, paho.SubscribeOptions{Topic: f, QoS: 1})
		}
		sa, err := cl.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", topic, err)
		}
		if len(sa.Reasons) != len(opts) {
			return fmt.Errorf("subscribe %s answered %d reasons for %d filters", topic, len(sa.Reasons), len(opts))
		}
		for _, r := range sa.Reasons {
			if r > 2 {
				return fmt.Errorf("subscribe %s refused 0x%02X", topic, r)
			}
		}
		clients[i] = cl
		return nil
	}

	connected, failures := rampUp(c, log, bring)
	if gateway {
		log.logf("SPREAD cohort=%s shape=%s consumers=%d", c.cohort, c.shape, connected)
	} else {
		logSpread(log, subSlots, connected)
	}

	// The runbook's word: consumers-ready goes out only when every
	// subscription is acknowledged. A partial ramp never says ready.
	ready := connected == c.clients
	if ready {
		if err := proveTarget(c, log, c.clients); err != nil {
			failures = append(failures, err.Error())
			ready = false
		}
	}
	if ready {
		_, err = sender.Publish(context.Background(), &paho.Publish{
			Topic: marker("consumers-ready", c.cohort), QoS: 1,
			Payload: []byte(fmt.Sprintf("ready subs=%d %s", connected, time.Now().UTC().Format(time.RFC3339))),
		})
		if err != nil {
			t.Fatalf("publishing consumers-ready: %v", err)
		}
		log.logf("consumers-ready published: %d subscriptions acknowledged", connected)
	}

	// A separate reader waits for the run markers - the sender never reads.
	markers := make(chan string, 8)
	watcher, err := dial(c, tc, coordID(c, "scale-consumer-watch"), true, 0, func(pr paho.PublishReceived) {
		at, err := time.Parse(time.RFC3339, string(pr.Packet.Payload))
		if err != nil || at.Before(started) {
			log.logf("ignoring %s %q: not this run's", pr.Packet.Topic, pr.Packet.Payload)
			return
		}
		select {
		case markers <- pr.Packet.Topic:
		default:
		}
	}, nil, 0)
	if err != nil {
		t.Fatalf("coordination watcher: %v", err)
	}
	defer watcher.Disconnect(&paho.Disconnect{ReasonCode: 0})
	// The run is done when every publishing cohort says so: the fleet's one,
	// or each a gateway role was told to wait for (DONE).
	doneTopics := map[string]bool{marker("done", ""): true}
	if gateway {
		doneTopics = map[string]bool{}
		for _, co := range c.done {
			doneTopics[marker("done", co)] = true
		}
	}
	for topic := range doneTopics {
		if _, err := watcher.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
		}); err != nil {
			t.Fatalf("subscribing to %s: %v", topic, err)
		}
	}

	// The minute log: deliveries so far, once a minute, so a stall has a
	// timestamp and the report has a curve.
	stopMinutes := make(chan struct{})
	go func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-stopMinutes:
				return
			case <-tick.C:
				log.logf("MINUTE deliveries=%d", delivered.Load())
			}
		}
	}()

	// Wait for run/done: the load window plus generous slack, and a ramp
	// that failed waits only briefly for the humans to read the log.
	wait := c.duration + 20*time.Minute
	if c.patience > 0 {
		wait = c.duration + c.patience
	}
	if !ready {
		wait = time.Minute
	}
	doneSeen := false
	seen := map[string]bool{}
	deadline := time.After(wait)
waiting:
	for len(seen) < len(doneTopics) {
		select {
		case topic := <-markers:
			if doneTopics[topic] {
				seen[topic] = true
			}
		case <-deadline:
			log.logf("run/done never arrived within %s (%d of %d cohorts)", wait, len(seen), len(doneTopics))
			break waiting
		}
	}
	if len(seen) == len(doneTopics) {
		doneSeen = true
		log.logf("run/done seen; settling for %s", c.settle)
		time.Sleep(c.settle)
	}
	close(stopMinutes)

	// The count of what this sweep examined, before any verdict on it.
	total := delivered.Load()
	var gaps, dups, silent, latestBack, appendBack int
	firstGap, firstSilent, firstLatest, firstAppend := "", "", "", ""
	queueTotals := map[string]int64{}
	// Tail loss, which contiguity cannot see: per stream, the highest seq
	// any of its consumers reached; summed per target, it is the count the
	// publishers' acknowledged records per target must equal.
	topicMax := map[string]uint64{}
	// Raw, per topic, for the report role to do the arithmetic on: the
	// highest seq any of its consumers reached, and whether every one of
	// them was still connected at the end. A topic with a dead consumer is
	// excluded from the equality and reconciled against a storage replay
	// instead, because its number moved for a reason the broker did not
	// cause.
	topicRows := map[string]*topicRow{}
	// sweepStream judges one stream of consumer i: the per-stream body this
	// sweep always had, with a gateway stream's target read off its topic.
	sweepStream := func(i int, topic string, s *stream) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var tg target
		if gateway {
			ti, _ := targetFor(topic)
			tg = targets[ti]
		} else {
			tg = targets[subSlots[i].tgt]
		}
		if tg.kind == "append" || tg.kind == "bcast" {
			r := topicRows[topic]
			if r == nil {
				r = &topicRow{target: tg.name, survived: true}
				topicRows[topic] = r
			}
			r.consumers++
			r.records += s.count
			r.gaps += s.gaps
			r.dups += s.dups
			if s.lastSeq > r.maxSeq {
				r.maxSeq = s.lastSeq
			}
			if lost.was(clientID(c, "c", i)) {
				r.survived = false
				r.lostConsumers++
			}
		}
		switch {
		case tg.kind == "queue":
			queueTotals[tg.name] += s.count
		case !gateway && s.count == 0 && i < connected:
			silent++
			if firstSilent == "" {
				firstSilent = fmt.Sprintf("client %d on %s", i, topic)
			}
		}
		if (tg.kind == "append" || tg.kind == "bcast") && i < connected {
			if _, ok := topicMax[topic]; !ok || s.lastSeq > topicMax[topic] {
				topicMax[topic] = s.lastSeq
			}
		}
		gaps += s.gaps
		dups += s.dups
		if s.gapEv != "" && firstGap == "" {
			firstGap = s.gapEv
		}
		if tg.kind == "latest" {
			latestBack += s.disorder
			if s.orderEv != "" && firstLatest == "" {
				firstLatest = s.orderEv
			}
		} else {
			appendBack += s.disorder
			if s.orderEv != "" && firstAppend == "" {
				firstAppend = s.orderEv
			}
		}
	}
	// Streams examined, counted before any verdict on them (rule 12).
	examined := 0
	for i := range streams {
		cs := &streams[i]
		cs.mu.Lock()
		topics := make([]string, 0, len(cs.by))
		for topic := range cs.by {
			topics = append(topics, topic)
		}
		cs.mu.Unlock()
		sort.Strings(topics)
		var seenByConsumer int64
		for _, topic := range topics {
			s := cs.get(topic)
			examined++
			sweepStream(i, topic, s)
			s.mu.Lock()
			seenByConsumer += s.count
			s.mu.Unlock()
		}
		// A gateway consumer is fed if anything reached it; its streams are
		// the topics that did, so none of them can be empty.
		if gateway && seenByConsumer == 0 && i < connected {
			silent++
			if firstSilent == "" {
				firstSilent = fmt.Sprintf("client %d on %s", i, subs[i])
			}
		}
	}
	perTarget := map[string]uint64{}
	unfollowed := 0
	if gateway {
		// Every topic that reached a consumer is a publisher stream that was
		// followed; the report role joins them to the publishers' rows.
		for topic, m := range topicMax {
			if ti, ok := targetFor(topic); ok {
				perTarget[targets[ti].name] += m
			}
		}
	}
	for ti, tg := range targets {
		if gateway {
			break
		}
		switch tg.kind {
		case "append", "bcast":
			for o := 0; o < pubCounts[ti]; o++ {
				if m, ok := topicMax[pubTopic(tg, o)]; ok {
					perTarget[tg.name] += m
				} else {
					unfollowed++
				}
			}
		}
	}
	jobMu.Lock()
	for _, ti := range jobTarget {
		perTarget[targets[ti].name]++
	}
	nJobs := len(jobTarget)
	jobMu.Unlock()

	asserts := []assertion{
		{"every client connected and subscribed, proven on /metrics", ready,
			fmt.Sprintf("%d of %d; %d failures; first: %s", connected, c.clients, len(failures), firstString(failures))},
		{"the run completed (run/done seen)", doneSeen, fmt.Sprintf("deliveries=%d", total)},
		{"no subscriber missed a record it asked for", gaps == 0,
			fmt.Sprintf("%d gaps; first: %s", gaps, firstGap)},
		{"latest deliveries never went backwards", latestBack == 0,
			fmt.Sprintf("%d out-of-order; first: %s", latestBack, firstLatest)},
		{"no newer append record arrived at a lower offset", appendBack == 0,
			fmt.Sprintf("%d out-of-order; first: %s", appendBack, firstAppend)},
		{"no queue job reached two workers", len(doubleJobs) == 0,
			fmt.Sprintf("%d double deliveries; first: %s", len(doubleJobs), firstString(doubleJobs))},
		{"every subscribed stream was fed", silent == 0,
			fmt.Sprintf("%d silent; first: %s", silent, firstSilent)},
		func() assertion {
			ackMu.Lock()
			defer ackMu.Unlock()
			return assertion{"every job's ack was accepted", ackErrors.Load() == 0,
				fmt.Sprintf("%d of %d acks failed, refusals by code %v; first: %s",
					ackErrors.Load(), acksSent.Load(), ackRefusals, firstAckErr)}
		}(),
		lost.assertion(),
	}
	numbers := map[string]string{
		"deliveries total":                          fmt.Sprintf("%d", total),
		"duplicates (at-least-once, informational)": fmt.Sprintf("%d", dups),
		"queue deliveries by queue":                 fmt.Sprintf("%v", queueTotals),
		"jobs examined for doubles":                 fmt.Sprintf("%d", nJobs),
		"queue redeliveries (attempt > 1)":          fmt.Sprintf("%d", redeliveries),
		"job acks sent":                             fmt.Sprintf("%d", acksSent.Load()),
		"records from before this run, ignored":     fmt.Sprintf("%d", foreign.Load()),
		"publisher streams nobody followed":         fmt.Sprintf("%d", unfollowed),
	}
	for name, n := range perTarget {
		numbers[perTargetKey(name)] = fmt.Sprintf("%d", n)
	}
	if gateway {
		// A gateway consumer cannot know which publisher streams exist on
		// other machines, so that number is the report role's join, not
		// this role's. Its own numbers are per cohort, never blended.
		delete(numbers, "publisher streams nobody followed")
		numbers["cohort"] = c.cohort + " (" + c.shape + ")"
		numbers["streams examined"] = fmt.Sprintf("%d", examined)
		numbers["payload bytes received"] = fmt.Sprintf("%d", gotBytes.Load())
		window := time.Duration(lastAt.Load() - firstAt.Load())
		mbps := 0.0
		if window > 0 {
			mbps = float64(gotBytes.Load()) / window.Seconds() / (1 << 20)
		}
		numbers["delivered MB/s (first to last arrival)"] = fmt.Sprintf("%.2f over %s", mbps,
			window.Round(time.Millisecond))
	}
	// The three populations the report role must account for, counted here
	// so that neither side can quietly drop a topic (rule 12).
	surv, excl := 0, 0
	survSum := map[string]uint64{}
	for _, r := range topicRows {
		if r.survived {
			surv++
			survSum[r.target] += r.maxSeq
		} else {
			excl++
		}
	}
	numbers["tail topics compared (all consumers survived)"] = fmt.Sprintf("%d", surv)
	numbers["tail topics excluded (a consumer was lost)"] = fmt.Sprintf("%d", excl)
	numbers["tail topics total"] = fmt.Sprintf("%d", len(topicRows))
	for name, n := range survSum {
		numbers["records per target, survivor topics only "+name] = fmt.Sprintf("%d", n)
	}
	writeTopicRows(t, c, log, topicRows)
	writeReport(t, c, log, asserts, numbers)
	for _, cl := range clients {
		if cl != nil {
			_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
		}
	}
	failIfNotHeld(t, asserts)
}

// -------------------------------------------------------------- publishers

func runPublishers(t *testing.T, c cfg, log *runlog) {
	tc := roleTLS(t, c)
	gateway := c.scenario == "gateway"
	lost := &losses{log: log}

	// The markers this role owns are cleared first, so a consumer that
	// subscribes later is never handed an earlier run's run/done.
	sender, err := dial(c, tc, coordID(c, "scale-producer-say"), true, 0, nil, nil, 0)
	if err != nil {
		t.Fatalf("coordination sender: %v", err)
	}
	defer sender.Disconnect(&paho.Disconnect{ReasonCode: 0})
	say := func(topic, msg string) {
		if _, err := sender.Publish(context.Background(), &paho.Publish{
			Topic: topic, QoS: 1, Payload: []byte(msg),
		}); err != nil {
			t.Fatalf("publishing %s: %v", topic, err)
		}
	}
	say(marker("start", c.cohort), "")
	say(marker("done", c.cohort), "")
	pubSlots := slots(c.clients, true)
	clients := make([]*paho.Client, c.clients)
	var published, pubErrors atomic.Int64
	perTgt := make([]atomic.Int64, len(targets))
	var refMu sync.Mutex
	refusals := map[byte]int64{}
	firstErr := ""

	bring := func(i int) error {
		cl, err := dial(c, tc, clientID(c, "p", i), true, 0, nil, lost, 0)
		if err != nil {
			return err
		}
		clients[i] = cl
		return nil
	}
	connected, failures := rampUp(c, log, bring)
	logSpread(log, pubSlots, connected)
	if connected != c.clients {
		asserts := []assertion{{"every publisher connected", false,
			fmt.Sprintf("%d of %d; first failure: %s", connected, c.clients, firstString(failures))}}
		writeReport(t, c, log, asserts, map[string]string{"published total": "0"})
		failIfNotHeld(t, asserts)
		return
	}

	// Wait for the consumers' word. The subscription's current-state pass
	// answers a late joiner, so the order the machines start in cannot
	// starve this wait - only a consumer role that never finished can.
	// The fleet waits for its one consumer role; a gateway role for every
	// consumer cohort it was told about (READY).
	readyTopics := map[string]bool{marker("consumers-ready", ""): true}
	if gateway {
		readyTopics = map[string]bool{}
		for _, co := range c.ready {
			readyTopics[marker("consumers-ready", co)] = true
		}
	}
	ready := make(chan string, 16)
	watcher, err := dial(c, tc, coordID(c, "scale-producer-watch"), true, 0, func(pr paho.PublishReceived) {
		if len(pr.Packet.Payload) == 0 {
			return // the consumer role clearing it at its start
		}
		select {
		case ready <- pr.Packet.Topic:
		default:
		}
	}, nil, 0)
	if err != nil {
		t.Fatalf("coordination watcher: %v", err)
	}
	defer watcher.Disconnect(&paho.Disconnect{ReasonCode: 0})
	for topic := range readyTopics {
		if _, err := watcher.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
		}); err != nil {
			t.Fatalf("subscribing to %s: %v", topic, err)
		}
	}
	log.logf("waiting for consumers-ready (%d cohorts)", len(readyTopics))
	readySeen := map[string]bool{}
	readyWait := 30 * time.Minute
	if c.patience > 0 {
		readyWait = c.patience
	}
	readyBy := time.After(readyWait)
	for len(readySeen) < len(readyTopics) {
		select {
		case topic := <-ready:
			if readyTopics[topic] && !readySeen[topic] {
				readySeen[topic] = true
				log.logf("consumers-ready seen on %s", topic)
			}
		case <-readyBy:
			t.Fatalf("consumers-ready arrived from %d of %d cohorts within %s: a consumer role "+
				"has not finished its ramp", len(readySeen), len(readyTopics), readyWait)
		}
	}

	want := c.clients + c.peers
	if gateway {
		want = c.target
	}
	if err := proveTarget(c, log, want); err != nil {
		t.Fatalf("the clock does not start: %v", err)
	}

	say(marker("start", c.cohort), time.Now().UTC().Format(time.RFC3339))
	log.logf("run/start published")

	// The load, ramped: publishers activate in waves, each with its own
	// seeded phase so ten thousand tickers never beat in step. The
	// full-beam clock starts when the last wave is active.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(c.seed))
	interval := time.Duration(float64(time.Second) / c.rate)
	// The per-publisher acknowledged count, written by its own goroutine and
	// read only after wg.Wait: the publishers' half of the per-topic join
	// the report role does.
	ackedSeq := make([]uint64, c.clients)
	// Gateway only: each publisher's PUBACK latency, written by its own
	// goroutine and read after wg.Wait.
	hists := make([]latHist, c.clients)
	activate := func(i int, jitter time.Duration) {
		defer wg.Done()
		tg := targets[pubSlots[i].tgt]
		topic := pubTopicOf(c, tg, pubSlots[i].ord)
		seq := uint64(0)
		defer func() { ackedSeq[i] = seq }()
		select {
		case <-stop:
			return
		case <-time.After(jitter):
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				// seq advances only on an acknowledged record, so the seqs
				// that were stored run contiguously from 1 and a refused or
				// failed one is offered again under the same number.
				sent := time.Now()
				resp, err := clients[i].Publish(context.Background(), &paho.Publish{
					Topic: topic, QoS: 1,
					Payload: padded(seq+1, c.size),
				})
				if gateway && err == nil {
					hists[i].add(time.Since(sent))
				}
				// **A refusal arrives as an error with its PUBACK beside it**:
				// paho v0.23 answers a code at 0x80 or above that way. So the
				// PUBACK is what tells the broker's refusal, a number in the
				// report, from a publish that failed on the wire.
				if err != nil && resp != nil {
					refMu.Lock()
					refusals[resp.ReasonCode]++
					refMu.Unlock()
					continue
				}
				if err != nil {
					pubErrors.Add(1)
					refMu.Lock()
					if firstErr == "" {
						firstErr = fmt.Sprintf("publisher %d on %s: %v", i, topic, err)
					}
					refMu.Unlock()
					continue
				}
				seq++
				published.Add(1)
				perTgt[pubSlots[i].tgt].Add(1)
			}
		}
	}
	wave := c.ramp
	for at := 0; at < c.clients; at += wave {
		end := at + wave
		if end > c.clients {
			end = c.clients
		}
		for i := at; i < end; i++ {
			wg.Add(1)
			// Drawn here, not in the goroutine: a *rand.Rand is not safe
			// for concurrent use.
			go activate(i, time.Duration(rng.Int63n(int64(interval))))
		}
		log.logf("WAVE active=%d/%d published-so-far=%d", end, c.clients, published.Load())
		time.Sleep(2 * time.Second)
	}
	log.logf("full beam: %d publishers active for %s", c.clients, c.duration)

	stopMinutes := make(chan struct{})
	go func() {
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-stopMinutes:
				return
			case <-tick.C:
				log.logf("MINUTE published=%d errors=%d", published.Load(), pubErrors.Load())
			}
		}
	}()
	time.Sleep(c.duration)
	close(stop)
	wg.Wait()
	close(stopMinutes)
	say(marker("done", c.cohort), time.Now().UTC().Format(time.RFC3339))
	log.logf("run/done published; total published=%d", published.Load())

	refMu.Lock()
	refStr := fmt.Sprintf("%v", refusals)
	nRefused := int64(0)
	for _, n := range refusals {
		nRefused += n
	}
	refMu.Unlock()
	asserts := []assertion{
		{"every publisher connected", true, fmt.Sprintf("%d of %d", connected, c.clients)},
		{"the load ran (records published)", published.Load() > 0,
			fmt.Sprintf("published=%d", published.Load())},
		{"no publish failed on the wire", pubErrors.Load() == 0,
			fmt.Sprintf("%d errors; first: %s", pubErrors.Load(), firstErr)},
		lost.assertion(),
	}
	numbers := map[string]string{
		"published total":    fmt.Sprintf("%d", published.Load()),
		"refusals by code":   refStr,
		"refused total":      fmt.Sprintf("%d", nRefused),
		"achieved rate (/s)": fmt.Sprintf("%.0f", float64(published.Load())/c.duration.Seconds()),
	}
	for ti, tg := range targets {
		numbers[perTargetKey(tg.name)] = fmt.Sprintf("%d", perTgt[ti].Load())
	}
	if gateway {
		// Per cohort, because a machine on the air and one on the wire are
		// two populations and one blended figure describes neither.
		var all latHist
		worst, worstAt := time.Duration(0), -1
		for i := range hists {
			all.merge(&hists[i])
			if p := hists[i].quantile(0.99); p > worst {
				worst, worstAt = p, i
			}
		}
		bytes := published.Load() * int64(len(padded(1, c.size)))
		numbers["cohort"] = c.cohort
		numbers["payload bytes per record"] = fmt.Sprintf("%d", len(padded(1, c.size)))
		numbers["published MB/s"] = fmt.Sprintf("%.2f", float64(bytes)/c.duration.Seconds()/(1<<20))
		numbers["offered rate (/s)"] = fmt.Sprintf("%.0f", float64(c.clients)*c.rate)
		numbers["PUBACK latency p50 / p99 / max, every publisher"] = fmt.Sprintf("%s / %s / %s (%d samples; "+
			"p50 and p99 are bucket upper bounds, 25%% apart)", all.quantile(0.5), all.quantile(0.99),
			all.max.Round(time.Microsecond), all.count)
		numbers["PUBACK latency p99, worst publisher"] = fmt.Sprintf("%s (%s)", worst,
			clientID(c, "p", worstAt))
	}
	// The publishers' half of the per-topic join. Several publishers may
	// share a topic, so the acknowledged counts are summed per topic.
	// Latest is included: its mid-run history is unobservable because values
	// overwrite, but its END state is exactly what it promises - the newest
	// value survives - so the replay reads each name's final value and the
	// report role asserts it is at least what was acknowledged.
	acked, ackedTgt := map[string]uint64{}, map[string]string{}
	for i := 0; i < c.clients; i++ {
		tg := targets[pubSlots[i].tgt]
		if tg.kind == "queue" {
			continue // a job's identity is the broker's offset, not a seq
		}
		topic := pubTopicOf(c, tg, pubSlots[i].ord)
		acked[topic] += ackedSeq[i]
		ackedTgt[topic] = tg.name
	}
	writePubTopicRows(t, c, log, acked, ackedTgt)
	writeReport(t, c, log, asserts, numbers)
	for _, cl := range clients {
		if cl != nil {
			_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
		}
	}
	failIfNotHeld(t, asserts)
}

// ---------------------------------------------------- coordination roles

// runSay publishes one line and leaves: the runbook's sender, which never
// reads. Requires TOPIC and MSG.
func runSay(t *testing.T, c cfg, log *runlog) {
	if c.topic == "" || c.msg == "" {
		t.Fatal("say needs SAGUIN_SCALE_TOPIC and SAGUIN_SCALE_MSG")
	}
	tc := tlsConfig(t, c.ca)
	cl, err := dial(c, tc, oneShotID(c.user, "say"), true, 0, nil, nil, 0)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	if _, err := cl.Publish(context.Background(), &paho.Publish{
		Topic: c.topic, QoS: 1, Payload: []byte(c.msg),
	}); err != nil {
		t.Fatalf("publish %s: %v", c.topic, err)
	}
	log.logf("said %s to %s", c.msg, c.topic)
}

// runWatch follows the two coordination channels on a durable session and
// prints one line per record, replayed from the start. It reads only.
func runWatch(t *testing.T, c cfg, log *runlog) {
	tc := tlsConfig(t, c.ca)
	cl, err := dial(c, tc, c.user+"-watch", false, 7200, func(pr paho.PublishReceived) {
		fmt.Printf("%s %s\n", pr.Packet.Topic, pr.Packet.Payload)
	}, nil, 0)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	sa, err := cl.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: "scale/state/#", QoS: 1},
			{Topic: "scale/log/#", QoS: 1},
		},
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for i, r := range sa.Reasons {
		if r > 2 {
			t.Fatalf("watch subscription %d refused 0x%02X", i, r)
		}
	}
	log.logf("watching scale/state/# and scale/log/# for %s", c.duration)
	time.Sleep(c.duration)
}

// runKVGet asks the latest channel for one value, the point read the
// runbook uses to wait on a status without prose-reading a log.
func runKVGet(t *testing.T, c cfg, log *runlog) {
	if c.topic == "" {
		t.Fatal("kvget needs SAGUIN_SCALE_TOPIC, the topic whose value to read")
	}
	tc := tlsConfig(t, c.ca)
	reply := make(chan string, 1)
	cl, err := dial(c, tc, oneShotID(c.user, "kvget"), true, 0, func(pr paho.PublishReceived) {
		if pr.Packet.Topic == "kv-reply" {
			select {
			case reply <- string(pr.Packet.Payload):
			default:
			}
		}
	}, nil, 0)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
	if _, err := cl.Publish(context.Background(), &paho.Publish{
		Topic: "$saguin/kv/get", QoS: 1, Payload: []byte(c.topic),
		Properties: &paho.PublishProperties{ResponseTopic: "kv-reply"},
	}); err != nil {
		t.Fatalf("kv/get: %v", err)
	}
	select {
	case v := <-reply:
		fmt.Printf("VALUE %s %s\n", c.topic, v)
	case <-time.After(5 * time.Second):
		t.Fatalf("no value for %s within 5s", c.topic)
	}
}

// ---------------------------------------------------------------- helpers

// oneShotID is unique per call: a heartbeat loop and an ad-hoc say on one
// machine run at once, and a shared id would take one session over.
func oneShotID(user, what string) string {
	return fmt.Sprintf("%s-%s-%s", user, what, strconv.FormatInt(time.Now().UnixNano(), 36))
}

var seqRe = regexp.MustCompile(`seq=(\d+)`)
var tsRe = regexp.MustCompile(`ts=(\d+)`)

// tsOf is the publisher's send time, unix nanoseconds; 0 when absent.
func tsOf(payload string) int64 {
	m := tsRe.FindStringSubmatch(payload)
	if m == nil {
		return 0
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}

// perTargetKey names one target's record count identically in both roles'
// reports: the publishers' acknowledged records, the consumers' highest seq
// per stream summed (append, bcast) or distinct jobs (queue), so the report
// role compares them one to one.
func perTargetKey(name string) string { return "records per target " + name }

func seqOf(payload string) uint64 {
	m := seqRe.FindStringSubmatch(payload)
	if m == nil {
		return 0
	}
	n, _ := strconv.ParseUint(m[1], 10, 64)
	return n
}

func firstString(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	return s[0]
}

// logSpread records the realised assignment, counted rather than assumed:
// a spread the report does not state is one nobody can question.
func logSpread(log *runlog, sl []slot, connected int) {
	per := map[string]int{}
	for i := 0; i < connected && i < len(sl); i++ {
		per[targets[sl[i].tgt].name]++
	}
	names := make([]string, 0, len(per))
	for n := range per {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", n, per[n]))
	}
	log.logf("SPREAD %s", strings.Join(parts, " "))
}

func failIfNotHeld(t *testing.T, as []assertion) {
	for _, a := range as {
		if !a.held {
			t.Errorf("assertion failed: %s (%s)", a.name, a.evidence)
		}
	}
}

// topicRowsPath names the raw tail evidence beside the role's report:
// report.md -> report-topics.tsv, so the two travel together when the human
// copies a client's report to the broker machine.
func topicRowsPath(report string) string {
	return strings.TrimSuffix(report, filepath.Ext(report)) + "-topics.tsv"
}

// writeTopicRows writes the consumers' raw per-topic tail evidence. It is
// deliberately unjudged: one row per topic, the survival flag beside the
// number it qualifies, and the report role decides what follows.
func writeTopicRows(t *testing.T, c cfg, log *runlog, rows map[string]*topicRow) {
	var b strings.Builder
	fmt.Fprintf(&b, "# consumers, scale run, raw per-topic tail evidence\n")
	fmt.Fprintf(&b, "# survived=0 means a consumer of this topic was lost, so its\n")
	fmt.Fprintf(&b, "# max_seq stopped for a reason the broker did not cause: exclude it\n")
	fmt.Fprintf(&b, "# from the equality and reconcile it against a storage replay.\n")
	// A gateway role's rows carry its cohort, so the report role joins the
	// machines' files without ever blending two cohorts into one number.
	head, tail := "", ""
	if c.cohort != "" {
		head, tail = "\tcohort", "\t"+c.cohort
	}
	fmt.Fprintf(&b, "topic\ttarget\tmax_seq\trecords\tgaps\tdups\tconsumers\tlost_consumers\tsurvived%s\n", head)
	names := make([]string, 0, len(rows))
	for k := range rows {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		r := rows[n]
		s := 1
		if !r.survived {
			s = 0
		}
		fmt.Fprintf(&b, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d%s\n",
			n, r.target, r.maxSeq, r.records, r.gaps, r.dups, r.consumers, r.lostConsumers, s, tail)
	}
	p := topicRowsPath(c.report)
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing the per-topic rows to %s: %v", p, err)
		return
	}
	log.logf("per-topic tail evidence written to %s (%d topics)", p, len(rows))
}

// writePubTopicRows writes the publishers' side of the same join: what each
// topic's publisher got acknowledged. Acknowledged, not attempted - a publish
// whose PUBACK was lost is re-sent under the same seq, so this is the number
// the consumer's max_seq is compared against, and storage may legitimately
// hold more than it.
func writePubTopicRows(t *testing.T, c cfg, log *runlog, acked map[string]uint64, tgt map[string]string) {
	var b strings.Builder
	fmt.Fprintf(&b, "# publishers, scale run, raw per-topic acknowledged records\n")
	fmt.Fprintf(&b, "# acked_seq is the highest seq the broker acknowledged for this topic.\n")
	fmt.Fprintf(&b, "# Storage may hold MORE: a lost PUBACK is re-sent under the same seq.\n")
	head, tail := "", ""
	if c.cohort != "" {
		head, tail = "\tcohort", "\t"+c.cohort
	}
	fmt.Fprintf(&b, "topic\ttarget\tacked_seq%s\n", head)
	names := make([]string, 0, len(acked))
	for k := range acked {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(&b, "%s\t%s\t%d%s\n", n, tgt[n], acked[n], tail)
	}
	p := topicRowsPath(c.report)
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing the per-topic rows to %s: %v", p, err)
		return
	}
	log.logf("per-topic acknowledged records written to %s (%d topics)", p, len(acked))
}

// writeReport writes the role's report.md in the runbook's format: the
// machine line, the numbers, the assertions table, and the whole log.
func writeReport(t *testing.T, c cfg, log *runlog, as []assertion, numbers map[string]string) {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s - scale run %s\n", c.role, time.Now().UTC().Format("2006-01-02"))
	fmt.Fprintf(&b, "machine: <model, RAM - fill in> %s/%s %d cpus\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	// **The shape line is the role's own.** CLIENTS, RAMP and RATE belong to
	// the fleet roles and are untouched defaults everywhere else, so
	// printing them on a retention report states a population and an offered
	// rate that run never had.
	if c.role == "retention" {
		names := make([]string, 0, len(c.channels))
		for _, ch := range c.channels {
			names = append(names, ch.name)
		}
		fmt.Fprintf(&b, "broker: %s   channels: %s   fast: %d   slow: %d   publishers: %d   "+
			"pause: %s   duration: %s   settle: %s   seed: %d\n\n",
			c.broker, strings.Join(names, ","), c.fast, c.slow, c.publishers, c.pause,
			c.duration, c.settle, c.seed)
	} else {
		fmt.Fprintf(&b, "broker: %s   clients: %d   ramp: %d   rate: %g/s   duration: %s   seed: %d\n\n",
			c.broker, c.clients, c.ramp, c.rate, c.duration, c.seed)
	}
	fmt.Fprintf(&b, "## numbers\n\n")
	keys := make([]string, 0, len(numbers))
	for k := range numbers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "- %s: %s\n", k, numbers[k])
	}
	fmt.Fprintf(&b, "\n## assertions\n\n| assertion | result | evidence |\n|---|---|---|\n")
	for _, a := range as {
		verdict := "held"
		if !a.held {
			verdict = "FAILED"
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", a.name, verdict, a.evidence)
	}
	fmt.Fprintf(&b, "\n## log\n\n```\n")
	log.mu.Lock()
	for _, l := range log.lines {
		fmt.Fprintln(&b, l)
	}
	log.mu.Unlock()
	fmt.Fprintf(&b, "```\n")
	if err := os.WriteFile(c.report, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing the report to %s: %v", c.report, err)
	}
	log.logf("report written to %s", c.report)
}

// ------------------------------------------------------------------ replay

// runReplay is the storage arbiter, and the report role owns it. The tail
// comparison between the two client reports answers two questions at once -
// did the broker keep every record it acknowledged, and did live subscribers
// receive what it kept - and a consumer that died mid-run moves the second
// without touching the first. On 2026-09-22 that read as 19,030 missing
// records which were in fact sitting contiguously in storage, undelivered to
// clients that were no longer there.
//
// So this role asks the first question on its own: a fresh consumer replays
// the whole history from the floor and rebuilds, per topic, the highest seq
// storage holds and whether the seqs below it run contiguously. It is
// dup-tolerant by construction - a publisher whose PUBACK was lost re-sends
// the same seq, so storage legitimately holds it twice.
//
// Two rules, both learned the hard way:
//
//   - THE INSTRUMENT LEAVES NO RESIDUE IN WHAT IT MEASURES. The client is a
//     throwaway with its own id and a session this role expires on the way
//     out, so running the report twice does not accumulate sessions and
//     stored positions in the store under test.
//   - THE COVERAGE CHECK IS AN ASSERTION, NOT A HABIT. The replayed record
//     count must equal the sum of saguin_channel_records over the channels
//     replayed, or the reconciliation is void and the report says so. That
//     check is the only reason the first such replay could be trusted.
//
// It must run while the broker still holds the run, before any restart, and
// it is only valid while the floor has not moved - a trimmed channel cannot
// replay what retention reclaimed, which the role states rather than hides.
func runReplay(t *testing.T, c cfg, log *runlog) {
	tc := tlsConfig(t, c.ca)
	id := fmt.Sprintf("scale-replay-%d", time.Now().UnixNano())

	var mu sync.Mutex
	rows := map[string]*replayRow{}
	var total, appendRecords int64
	lastAt := time.Now()

	onMsg := func(pr paho.PublishReceived) {
		seq := seqOf(string(pr.Packet.Payload))
		mu.Lock()
		defer mu.Unlock()
		total++
		lastAt = time.Now()
		r := rows[pr.Packet.Topic]
		if r == nil {
			tg, kind := targetOf(pr.Packet.Topic)
			r = &replayRow{target: tg, kind: kind}
			rows[pr.Packet.Topic] = r
		}
		if r.kind == "append" {
			appendRecords++
		}
		r.take(pr.Packet.Topic, seq)
	}

	cl, err := dial(c, tc, id, true, 3600, onMsg, nil, 0)
	if err != nil {
		t.Fatalf("the replay client could not connect: %v", err)
	}
	var subs []paho.SubscribeOptions
	var replayed []string
	var latestChans []string
	for _, tg := range targets {
		switch tg.kind {
		case "append":
			// The whole history, from the floor.
			subs = append(subs, paho.SubscribeOptions{Topic: tg.prefix + "/#", QoS: 1})
			replayed = append(replayed, tg.name)
		case "latest":
			// Not a history - a latest channel hands a new subscriber the
			// current value of every name, which is the end state its
			// contract promises and the only thing here worth checking.
			subs = append(subs, paho.SubscribeOptions{Topic: tg.prefix + "/#", QoS: 1})
			latestChans = append(latestChans, tg.name)
		}
		// bcast is claimed by no channel, so it has no stored history at all.
	}
	t0 := time.Now()
	if _, err := cl.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: subs}); err != nil {
		t.Fatalf("subscribing the replay client: %v", err)
	}
	log.logf("REPLAY subscribed to %d append channels, reading from the floor", len(subs))

	// Done when nothing has arrived for the settle window.
	deadline := time.Now().Add(c.duration)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		mu.Lock()
		n, idle := total, time.Since(lastAt)
		mu.Unlock()
		if idle > c.settle && n > 0 {
			break
		}
		if idle > 3*c.settle {
			log.logf("REPLAY nothing arrived within %s: storage did not replay", 3*c.settle)
			break
		}
	}
	took := time.Since(t0)

	mu.Lock()
	defer mu.Unlock()
	stored, floors, err := channelRecords(c, replayed)
	coverage := "storage did not answer"
	covered := false
	if err == nil {
		coverage = fmt.Sprintf("replayed %d append records, saguin_channel_records %d",
			appendRecords, stored)
		covered = uint64(appendRecords) == stored
	} else {
		coverage = fmt.Sprintf("replayed %d append records, but /metrics: %v", appendRecords, err)
	}

	cut := judgeCuts(rows, floors)

	gaps, firstGap := 0, ""
	perTarget := map[string]uint64{}
	latestNames := 0
	for _, r := range rows {
		if r.kind == "latest" {
			// One value per name: there is no sequence to be contiguous in,
			// only a final seq for the report role to compare.
			latestNames++
			perTarget[r.target] += r.maxSeq
			continue
		}
		gaps += r.gaps
		if r.firstGap != "" && firstGap == "" {
			firstGap = r.firstGap
		}
		perTarget[r.target] += r.maxSeq
	}
	asserts := []assertion{
		{"the replay covered every stored record", covered, coverage},
		{"storage holds no gap below any topic's highest seq, above the floor retention set", gaps == 0,
			fmt.Sprintf("%d gaps; first: %s", gaps, firstGap)},
	}
	numbers := map[string]string{
		"records replayed":         fmt.Sprintf("%d", total),
		"append records replayed":  fmt.Sprintf("%d", appendRecords),
		"topics seen":              fmt.Sprintf("%d", len(rows)),
		"append channels replayed": strings.Join(replayed, " "),
		"latest names read":        fmt.Sprintf("%d", latestNames),
		"latest channels read":     strings.Join(latestChans, " "),
		"replay took":              took.Round(time.Millisecond).String(),
		"replay rate (records/s)":  fmt.Sprintf("%.0f", float64(total)/took.Seconds()),
	}
	for name, n := range perTarget {
		numbers["stored max seq per target "+name] = fmt.Sprintf("%d", n)
	}
	numbers["topics starting at retention's cut"] = fmt.Sprintf("%d", cut)
	for _, name := range replayed {
		if f := floors[name]; f > 1 {
			numbers["records retention had removed, "+name] = fmt.Sprintf("%d (the floor is at %d)", f-1, f)
		}
	}
	writeReplayRows(t, c, log, rows)
	writeReport(t, c, log, asserts, numbers)

	// No residue: expire the session on the way out, so the store under test
	// is not left holding this instrument's cursor.
	zero := uint32(0)
	_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0,
		Properties: &paho.DisconnectProperties{SessionExpiryInterval: &zero}})
	failIfNotHeld(t, asserts)
}

// take counts one record storage returned for topic.
//
// Contiguity is an append channel's property. A latest channel hands over
// one value per name, so its single record is not a sequence and counting a
// "gap" from seq 1 to it would be noise, not evidence.
//
// **Judged from the first record read, not from seq 1.** A channel whose
// retention has moved its floor holds each topic's tail, so its first record
// is not seq 1 and that is no gap; whether a topic starting late is
// retention or loss is decided once the floor is known (judgeCuts).
func (r *replayRow) take(topic string, seq uint64) {
	r.records++
	if r.kind == "append" {
		switch {
		case r.first == 0:
			r.first = seq
		case seq > r.last+1:
			r.gaps++
			if r.firstGap == "" {
				r.firstGap = fmt.Sprintf("%s: seq %d after %d", topic, seq, r.last)
			}
		case seq <= r.last:
			r.dups++
		}
	}
	if seq > r.maxSeq {
		r.maxSeq = seq
	}
	// **Forward only**, as the consumer's own stream and retReader.got are:
	// a re-sent copy landing after a later seq moved the mark back, and the
	// next record read as a gap - 5, 6, 5, 7 reported "seq 7 after 5".
	if seq > r.last {
		r.last = seq
	}
}

// judgeCuts decides every append topic whose first stored seq is above 1,
// against its channel's floor, and answers how many were retention's cut.
//
// **A cut where the floor has moved, a gap where it has not.** A moved
// floor means the operator's retention removed the channel's oldest
// records, which is configured and reported, not lost. A floor that never
// moved means nothing was removed, so the records below the first stored
// seq were never stored.
func judgeCuts(rows map[string]*replayRow, floors map[string]uint64) int {
	cut := 0
	for topic, r := range rows {
		if r.kind != "append" || r.first <= 1 {
			continue
		}
		if floors[r.target] > 1 {
			cut++
			continue
		}
		r.gaps++
		if r.firstGap == "" {
			r.firstGap = fmt.Sprintf("%s: starts at seq %d, and %s's floor never moved", topic, r.first, r.target)
		}
	}
	return cut
}

// replayRow is one topic's evidence as storage returned it.
type replayRow struct {
	target   string
	kind     string
	maxSeq   uint64
	first    uint64
	last     uint64
	records  int64
	gaps     int
	dups     int
	firstGap string
}

func targetOf(topic string) (name, kind string) {
	for _, tg := range targets {
		if strings.HasPrefix(topic, tg.prefix+"/") {
			return tg.name, tg.kind
		}
	}
	return "unknown", "unknown"
}

func writeReplayRows(t *testing.T, c cfg, log *runlog, rows map[string]*replayRow) {
	var b strings.Builder
	fmt.Fprintf(&b, "# storage replay, raw per-topic evidence\n")
	fmt.Fprintf(&b, "# stored_max_seq is the highest seq storage holds for this topic.\n")
	fmt.Fprintf(&b, "# It may exceed the publishers' acked_seq: a lost PUBACK is re-sent\n")
	fmt.Fprintf(&b, "# under the same seq, so storage legitimately holds it twice.\n")
	fmt.Fprintf(&b, "# kind=latest rows are one name's CURRENT value, not a history:\n")
	fmt.Fprintf(&b, "# stored_max_seq must be >= the publishers' acked_seq for that name.\n")
	fmt.Fprintf(&b, "# first_seq is the first seq this replay read: above 1 where retention cut.\n")
	fmt.Fprintf(&b, "topic\ttarget\tkind\tfirst_seq\tstored_max_seq\trecords\tgaps\tdups\n")
	names := make([]string, 0, len(rows))
	for k := range rows {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		r := rows[n]
		fmt.Fprintf(&b, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			n, r.target, r.kind, r.first, r.maxSeq, r.records, r.gaps, r.dups)
	}
	p := topicRowsPath(c.report)
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing the replay rows to %s: %v", p, err)
		return
	}
	log.logf("replay evidence written to %s (%d topics)", p, len(rows))
}

// channelRecords sums saguin_channel_records over the named channels, which
// is what the replay's own count must equal, and returns each one's floor
// from the same scrape.
func channelRecords(c cfg, names []string) (uint64, map[string]uint64, error) {
	if c.ops == "" {
		return 0, nil, fmt.Errorf("SAGUIN_SCALE_OPS is required: the replay proves its coverage on /metrics")
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/metrics", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, nil, fmt.Errorf("/metrics answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return 0, nil, err
	}
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	var total uint64
	seen := 0
	for _, m := range recordsLine.FindAllStringSubmatch(string(body), -1) {
		if !want[m[1]] {
			continue
		}
		v, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			return 0, nil, err
		}
		total += v
		seen++
	}
	if seen != len(names) {
		return 0, nil, fmt.Errorf("saguin_channel_records covered %d of the %d channels replayed", seen, len(names))
	}
	// Each channel's floor, off the same scrape: where retention has moved
	// it, a topic's tail starting above seq 1 is the cut, not a gap.
	floors := map[string]uint64{}
	for _, m := range floorLine.FindAllStringSubmatch(string(body), -1) {
		if !want[m[1]] {
			continue
		}
		v, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			return 0, nil, err
		}
		floors[m[1]] = v
	}
	if len(floors) != len(names) {
		return 0, nil, fmt.Errorf("saguin_channel_floor_offset covered %d of the %d channels replayed", len(floors), len(names))
	}
	return total, floors, nil
}

var recordsLine = regexp.MustCompile(`saguin_channel_records\{channel="([^"]+)"\}\s+([0-9]+)`)

var floorLine = regexp.MustCompile(`saguin_channel_floor_offset\{channel="([^"]+)"\}\s+([0-9]+)`)

// headroom is how far the nearest fast reader finished above the floor, per
// channel. **It is reported and never asserted**, and it is here for the
// morning somebody finds this red on a slower machine: a positive headroom
// falling towards zero says the run is sized too tightly for that hardware,
// and a fast reader passed with headroom to spare says something about the
// broker. Without it the two look identical - which cost an afternoon when
// the smoke's first budget held about one record and every cohort was
// overtaken.
//
// The readers' own positions come from /v1/operations/consumers, which
// already answers "which of my devices is behind"; the floor comes off the
// same scrape as everything else.
func headroom(c cfg, names []string, fast map[string]bool) (map[string]int64, error) {
	body, err := scrape(c)
	if err != nil {
		return nil, err
	}
	floors := map[string]uint64{}
	for _, m := range floorLine.FindAllStringSubmatch(body, -1) {
		v, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			return nil, err
		}
		floors[m[1]] = v
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/v1/operations/consumers", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("/v1/operations/consumers answered %d", resp.StatusCode)
	}
	var doc struct {
		Channels []struct {
			Channel   string `json:"channel"`
			Positions []struct {
				Reader string `json:"reader"`
				Offset uint64 `json:"offset"`
			} `json:"positions"`
		} `json:"channels"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&doc); err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	out := map[string]int64{}
	seen := map[string]bool{}
	for _, ch := range doc.Channels {
		if !want[ch.Channel] {
			continue
		}
		for _, p := range ch.Positions {
			if !fast[readerID(p.Reader)] {
				continue
			}
			h := int64(p.Offset) - int64(floors[ch.Channel])
			if !seen[ch.Channel] || h < out[ch.Channel] {
				out[ch.Channel], seen[ch.Channel] = h, true
			}
		}
	}
	return out, nil
}

// --------------------------------------------------------------- retention

// The retention role: what the floor does to readers while many of them are
// reading. It is not coverage of invariant 1's unreportable case -
// TestTheFloorPassingAConnectedConsumerSaysHowMuchWent owns that, a single
// consumer held still while the floor passes it - but the interleaving that
// a single-consumer test cannot state: **one reader is passed correctly
// while its neighbour on the same channel loses nothing**, with the floor
// moving under live load, drains running under per-consumer locks, and
// pending lists carrying offsets the sweep is about to pass.
//
// Two cohorts on every channel. The fast one keeps up and must lose
// nothing. The slow one is pinned to Receive Maximum 1 with a pause per
// record, so the floor overtakes it while it is connected and reading.
//
// Three rules this role keeps, each of which was a mistake first:
//
//   - **Sequences are per reader PER TOPIC.** Each publisher numbers its own
//     stream from 1 and a reader follows several, so one sequence per reader
//     makes interleaved streams look contiguous and reports zero gaps
//     whatever the broker did.
//   - **`position_lost` counts occurrences, not readers.** The floor
//     overtakes the same slow reader repeatedly - 2,154 times on one channel
//     in six minutes - so this asserts the counter against the route's rows
//     rather than against a count of readers.
//   - **The positive control is mandatory.** The slow cohort must be shown
//     to produce gaps in the same run, or the fast cohort's zero is a fact
//     about a blind checker rather than about the broker.

// retentionChannel is one append channel under test and the prefix its
// publishers write to.
type retentionChannel struct{ name, prefix string }

// retReader is one reader's view of what arrived. Sequences are kept per
// topic, for the reason given above.
type retReader struct {
	id      string
	channel string
	slow    bool

	mu       sync.Mutex
	last     map[string]uint64
	first    map[string]uint64
	gaps     int
	firstGap string
	records  int64
}

func (r *retReader) got(topic string, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records++
	if r.last == nil {
		r.last, r.first = map[string]uint64{}, map[string]uint64{}
	}
	prev, seen := r.last[topic]
	switch {
	case !seen:
		r.first[topic] = seq
	case seq > prev+1:
		r.gaps++
		if r.firstGap == "" {
			r.firstGap = fmt.Sprintf("%s: seq %d after %d (%d missing)", topic, seq, prev, seq-prev-1)
		}
	}
	if seq > prev {
		r.last[topic] = seq
	}
}

func (r *retReader) snapshot() (topics int, gaps int, records int64, firstGap string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.last), r.gaps, r.records, r.firstGap
}

// cohort names which set a reader belongs to, and is what the route's rows
// are judged against.
func (r *retReader) cohort() string {
	if r.slow {
		return "slow"
	}
	return "fast"
}

func runRetention(t *testing.T, c cfg, log *runlog) {
	// Plaintext when no authority is named: this role drives a broker on
	// the same machine and the transport is not what it measures.
	var tc *tls.Config
	if c.ca != "" {
		tc = tlsConfig(t, c.ca)
	}
	names := make([]string, 0, len(c.channels))
	for _, ch := range c.channels {
		names = append(names, ch.name)
	}
	log.logf("retention: channels=%v fast=%d slow=%d publishers=%d pause=%s pad=%d",
		names, c.fast, c.slow, c.publishers, c.pause, c.pad)

	// **The counters are proved at zero before anything is published.** A
	// broker carrying an earlier run's numbers would let every assertion
	// below pass on somebody else's evidence.
	before, err := channelCounters(c, names)
	if err != nil {
		t.Fatalf("reading the counters before the run: %v", err)
	}
	for _, n := range names {
		if v := before[n]; v.removed != 0 || v.lost != 0 {
			t.Fatalf("%s starts at retention_removed=%v position_lost=%v, not 0 and 0: this "+
				"broker is carrying an earlier run and nothing measured here would be "+
				"this run's", n, v.removed, v.lost)
		}
	}
	if rows, tracked, _, err := positionLostRoute(c); err != nil {
		t.Fatalf("reading /v1/operations/position-lost before the run: %v", err)
	} else if tracked != 0 {
		t.Fatalf("the route already names %d reader(s) before the run: %v", tracked, rows)
	}
	log.logf("counters and route verified empty on %d channel(s)", len(names))

	// **The counter is cached and the route is not, so the settle has to
	// outlast the cache.** /metrics answers an early scrape with the
	// previous body rather than refusing it - deliberately, so that a
	// scraper never records a gap - for `min_scrape_interval`, which
	// defaults to 60s. Read live against a body up to a minute old, the
	// identity between them is a comparison of two different moments:
	// measured on a 90-second run, the route said 528 occurrences and the
	// cached counter 342, and nothing was wrong with either.
	//
	// Once publishing has stopped and the readers have drained, the floor
	// stops moving; waiting longer than the cache then makes any body the
	// handler serves the final one. So this is a precondition rather than a
	// retry, and it is checked before the run rather than after it - the
	// alternative is discovering it six minutes in.
	interval, err := scrapeInterval(c)
	if err != nil {
		t.Fatalf("reading min_scrape_interval from /v1/operations/config: %v", err)
	}
	if c.settle <= interval {
		t.Fatalf("SETTLE is %s and the broker caches /metrics for %s, so this run would "+
			"compare a live route against a counter up to %s old and fail on the "+
			"arithmetic rather than on the broker. Give SETTLE more than %s",
			c.settle, interval, interval, interval)
	}
	log.logf("metrics cached for %s; settling for %s after the run, which outlasts it",
		interval, c.settle)

	// Readers first, so nothing is published before there is somebody to
	// miss it.
	var readers []*retReader
	var clients []*paho.Client
	lost := &losses{log: log}
	for _, ch := range c.channels {
		for i := 0; i < c.fast+c.slow; i++ {
			slow := i >= c.fast
			r := &retReader{
				id:      fmt.Sprintf("ret-%s-%s-%d", ch.name, cohortName(slow), i),
				channel: ch.name, slow: slow,
			}
			readers = append(readers, r)
			var recvMax uint16
			if slow {
				recvMax = 1 // one in flight, so the pause below really holds it back
			}
			cl, err := dialRecvMax(c, tc, r.id, recvMax, func(pr paho.PublishReceived) {
				r.got(pr.Packet.Topic, seqOf(string(pr.Packet.Payload)))
				if r.slow {
					// Connected and reading, just far too slowly: this is
					// the reader the floor is allowed to overtake.
					time.Sleep(c.pause)
				}
			}, lost)
			if err != nil {
				t.Fatalf("%s: %v", r.id, err)
			}
			if _, err := cl.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: ch.prefix + "/#", QoS: 1}},
			}); err != nil {
				t.Fatalf("%s subscribe: %v", r.id, err)
			}
			clients = append(clients, cl)
		}
	}
	log.logf("%d readers subscribed (%d fast, %d slow per channel)", len(readers), c.fast, c.slow)

	rng := rand.New(rand.NewSource(c.seed))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var published atomic.Int64
	pad := strings.Repeat("x", c.pad)
	for _, ch := range c.channels {
		for p := 0; p < c.publishers; p++ {
			wg.Add(1)
			every := time.Duration(8+rng.Intn(4)) * time.Millisecond
			go func(ch retentionChannel, p int, every time.Duration) {
				defer wg.Done()
				cl, err := dialRecvMax(c, tc, fmt.Sprintf("ret-pub-%s-%d", ch.name, p), 0, nil, lost)
				if err != nil {
					log.logf("publisher %s/%d: %v", ch.name, p, err)
					return
				}
				defer cl.Disconnect(&paho.Disconnect{ReasonCode: 0})
				topic := fmt.Sprintf("%s/p%d", ch.prefix, p)
				var seq uint64
				tick := time.NewTicker(every)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
						if _, err := cl.Publish(context.Background(), &paho.Publish{
							Topic: topic, QoS: 1,
							Payload: []byte(fmt.Sprintf("seq=%d %s", seq+1, pad)),
						}); err == nil {
							seq++
							published.Add(1)
						}
					}
				}
			}(ch, p, every)
		}
	}
	log.logf("%d publishers running for %s", len(c.channels)*c.publishers, c.duration)

	deadline := time.Now().Add(c.duration)
	for time.Now().Before(deadline) {
		time.Sleep(min(30*time.Second, time.Until(deadline)))
		if now, err := channelCounters(c, names); err == nil {
			for _, n := range names {
				// **Said to be cached, because it is.** These two come off
				// /metrics, which serves a body up to min_scrape_interval
				// old, so an early line reads removed=0 while the broker's
				// log already carries hundreds of passings. `published` is
				// this role's own count and is live. Nothing is asserted
				// from these lines - the assertions wait the cache out -
				// but a progress line nobody can trust is worse than none.
				log.logf("minute %.0f %s: removed=%.0f lost=%.0f (both as of the last "+
					"scrape, up to %s old) published=%d (live)",
					time.Since(deadline.Add(-c.duration)).Minutes(), n,
					now[n].removed, now[n].lost, interval, published.Load())
			}
		}
	}
	// **Before the publishers are stopped, not after.** Each one disconnects
	// on its way out, and a disconnect this role asked for is not a client
	// the broker lost - counted as one, the run reports its own tidying up
	// as a failure and the assertion stops meaning anything.
	lost.closing.Store(true)
	close(stop)
	wg.Wait()
	log.logf("publishing stopped after %d records; draining readers for %s", published.Load(), c.settle)
	time.Sleep(c.settle)

	as := retentionAssertions(t, c, log, readers, lost, names, published.Load())
	writeRetentionRows(t, c, log, readers)
	writeReport(t, c, log, as, retentionNumbers(c, names, fastSet(readers), published.Load()))

	for _, cl := range clients {
		zero := uint32(0)
		_ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0,
			Properties: &paho.DisconnectProperties{SessionExpiryInterval: &zero}})
	}
	failIfNotHeld(t, as)
}

// fastSet is the readers that must lose nothing, by client id.
func fastSet(readers []*retReader) map[string]bool {
	out := map[string]bool{}
	for _, r := range readers {
		if !r.slow {
			out[r.id] = true
		}
	}
	return out
}

func cohortName(slow bool) string {
	if slow {
		return "slow"
	}
	return "fast"
}

// dialRecvMax is dial with a Receive Maximum, which is how a slow reader is
// held to one record in flight. paho's default is 65,535, and a pause per
// record cannot hold back a reader the broker is allowed to run that far
// ahead of.
func dialRecvMax(c cfg, tc *tls.Config, id string, recvMax uint16,
	onMsg func(paho.PublishReceived), lost *losses) (*paho.Client, error) {
	return dial(c, tc, id, true, 3600, onMsg, lost, recvMax)
}

// retentionAssertions is what the run has to hold, whatever the machine.
// Never a count, a rate or a duration: the numbers belong in the report.
func retentionAssertions(t *testing.T, c cfg, log *runlog, readers []*retReader,
	lost *losses, names []string, published int64) []assertion {
	var as []assertion

	after, err := channelCounters(c, names)
	if err != nil {
		t.Fatalf("reading the counters after the run: %v", err)
	}
	rows, tracked, beyond, err := positionLostRoute(c)
	if err != nil {
		t.Fatalf("reading /v1/operations/position-lost after the run: %v", err)
	}

	// 1. The floor moved. Without this the run proves nothing and has to be
	// re-sized - every assertion below is vacuous on a channel that never
	// trimmed.
	moved, still := 0, []string{}
	for _, n := range names {
		if after[n].removed > 0 {
			moved++
		} else {
			still = append(still, n)
		}
	}
	as = append(as, assertion{
		name: "the floor moved on every channel", held: len(still) == 0,
		evidence: fmt.Sprintf("%d of %d channels trimmed; %d records published%s",
			moved, len(names), published, joinIfAny(", never trimmed: ", still)),
	})

	// 2. The survivors lost nothing. The assertion the run exists for, and
	// the one no single-consumer test can state.
	fastGaps, fastReaders, worstFast := 0, 0, ""
	for _, r := range readers {
		if r.slow {
			continue
		}
		fastReaders++
		_, gaps, _, firstGap := r.snapshot()
		fastGaps += gaps
		if gaps > 0 && worstFast == "" {
			worstFast = r.id + " " + firstGap
		}
	}
	as = append(as, assertion{
		name: "no fast reader missed a record", held: fastGaps == 0,
		evidence: fmt.Sprintf("%d fast readers, %d gaps%s", fastReaders, fastGaps,
			joinIfAny("; first: ", nonEmpty(worstFast))),
	})

	// 3. The positive control. Without it the zero above is a fact about the
	// checker rather than about the broker.
	slowGaps, slowReaders := 0, 0
	for _, r := range readers {
		if !r.slow {
			continue
		}
		slowReaders++
		_, gaps, _, _ := r.snapshot()
		slowGaps += gaps
	}
	as = append(as, assertion{
		name: "the slow cohort did record gaps (positive control)", held: slowGaps > 0,
		evidence: fmt.Sprintf("%d slow readers, %d gaps - a zero here makes the fast "+
			"cohort's zero meaningless", slowReaders, slowGaps),
	})

	// 4. Who was passed, read off the route rather than inferred. The rows
	// are judged kind-agnostically on purpose: a slow reader that drops and
	// reconnects mid-run is recorded as `session` rather than `consumer`,
	// and it is still a slow reader.
	fast := fastSet(readers)
	var namedFast []string
	for k := range rows {
		if fast[readerID(k[0])] {
			namedFast = append(namedFast, k[0]+" on "+k[1])
		}
	}
	sort.Strings(namedFast)
	as = append(as, assertion{
		name: "no fast reader was passed by the floor", held: len(namedFast) == 0,
		evidence: fmt.Sprintf("%d readers named on /v1/operations/position-lost, %d of them "+
			"fast%s", tracked, len(namedFast), joinIfAny(": ", namedFast)),
	})

	// 5. The route and the counter are two instruments on the same events.
	// The identity holds exactly while nothing was evicted, which is why
	// both halves are asserted rather than the sum alone.
	var mismatched []string
	for _, n := range names {
		var summed uint64
		for k, row := range rows {
			if k[1] == n {
				summed += row.Count
			}
		}
		if float64(summed) != after[n].lost {
			mismatched = append(mismatched, fmt.Sprintf("%s: route %d, counter %.0f",
				n, summed, after[n].lost))
		}
	}
	as = append(as, assertion{
		name: "the route's occurrences equal the counter, on every channel",
		held: len(mismatched) == 0 && beyond == 0 && tracked == len(rows),
		evidence: fmt.Sprintf("beyond=%d tracked=%d returned=%d%s", beyond, tracked, len(rows),
			joinIfAny("; ", mismatched)),
	})

	// 6. Every row is self-consistent. A row passed once must have lost
	// exactly the distance between where it stood and the floor; above one
	// passing the two are not comparable and the row is only checked for
	// having lost something.
	var wrong []string
	for k, row := range rows {
		switch {
		case row.RecordsMissed == 0:
			wrong = append(wrong, fmt.Sprintf("%s on %s reports losing nothing", k[0], k[1]))
		case row.Count == 1 && row.RecordsMissed != row.LastFloor-row.LastPosition:
			wrong = append(wrong, fmt.Sprintf("%s on %s: missed %d, floor %d - position %d",
				k[0], k[1], row.RecordsMissed, row.LastFloor, row.LastPosition))
		}
	}
	as = append(as, assertion{
		name: "every row on the route is self-consistent", held: len(wrong) == 0,
		evidence: fmt.Sprintf("%d rows checked%s", len(rows), joinIfAny("; ", wrong)),
	})

	// 7. Nothing outside the accounting moved. The passing warning is the
	// only loss this run is entitled to produce.
	var moved2 []string
	for _, n := range names {
		v := after[n]
		if v.dropped != 0 || v.expired != 0 || v.storageErrors != 0 {
			moved2 = append(moved2, fmt.Sprintf("%s: dropped=%.0f expired=%.0f storage_errors=%.0f",
				n, v.dropped, v.expired, v.storageErrors))
		}
	}
	as = append(as, assertion{
		name: "nothing was dropped, expired or failed to store", held: len(moved2) == 0,
		evidence: fmt.Sprintf("%d channels checked%s", len(names), joinIfAny("; ", moved2)),
	})

	as = append(as, lost.assertion())
	for _, a := range as {
		log.logf("assertion %q: held=%v (%s)", a.name, a.held, a.evidence)
	}
	return as
}

// ------------------------------------------------- retention: instruments

// counters is one channel's numbers, read over /metrics as an operator
// reads them rather than from anything this process holds.
type counters struct{ removed, lost, dropped, expired, storageErrors float64 }

var (
	removedLine  = regexp.MustCompile(`saguin_channel_retention_removed_total\{channel="([^"]+)"\}\s+([0-9.eE+]+)`)
	lostLine     = regexp.MustCompile(`saguin_channel_position_lost_total\{channel="([^"]+)"\}\s+([0-9.eE+]+)`)
	droppedLine  = regexp.MustCompile(`saguin_deliveries_dropped_total\s+([0-9.eE+]+)`)
	expiredLine  = regexp.MustCompile(`saguin_deliveries_expired_total\s+([0-9.eE+]+)`)
	storageErrLn = regexp.MustCompile(`saguin_storage_errors_total(?:\{[^}]*\})?\s+([0-9.eE+]+)`)
)

// channelCounters reads every number this role judges, for the named
// channels, in one scrape.
//
// **It fails when a channel it was asked about is not on /metrics.** A
// missing series would otherwise read as a zero, and a zero is what several
// of the assertions above are looking for.
func channelCounters(c cfg, names []string) (map[string]counters, error) {
	body, err := scrape(c)
	if err != nil {
		return nil, err
	}
	out := map[string]counters{}
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	take := func(re *regexp.Regexp, set func(*counters, float64)) error {
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			if !want[m[1]] {
				continue
			}
			v, err := strconv.ParseFloat(m[2], 64)
			if err != nil {
				return err
			}
			e := out[m[1]]
			set(&e, v)
			out[m[1]] = e
		}
		return nil
	}
	if err := take(removedLine, func(e *counters, v float64) { e.removed = v }); err != nil {
		return nil, err
	}
	if err := take(lostLine, func(e *counters, v float64) { e.lost = v }); err != nil {
		return nil, err
	}
	for _, n := range names {
		if _, ok := out[n]; !ok {
			return nil, fmt.Errorf("neither retention_removed_total nor position_lost_total "+
				"carries a series for %q, so this role cannot tell a channel that trimmed "+
				"nothing from one that is not there", n)
		}
	}
	// Broker-wide rather than per channel, so they are recorded against
	// every channel this role is watching.
	one := func(re *regexp.Regexp) float64 {
		if m := re.FindStringSubmatch(body); m != nil {
			v, _ := strconv.ParseFloat(m[1], 64)
			return v
		}
		return 0
	}
	d, x, s := one(droppedLine), one(expiredLine), one(storageErrLn)
	for n, e := range out {
		e.dropped, e.expired, e.storageErrors = d, x, s
		out[n] = e
	}
	return out, nil
}

// scrapeInterval is how long the broker serves a cached /metrics body,
// read from its own resolved configuration rather than assumed. A role that
// guessed this would be wrong on every broker whose operator had tuned it.
func scrapeInterval(c cfg) (time.Duration, error) {
	if c.ops == "" {
		return 0, fmt.Errorf("SAGUIN_SCALE_OPS is required")
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/v1/operations/config", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("/v1/operations/config answered %d", resp.StatusCode)
	}
	var doc struct {
		Broker struct {
			Operations struct {
				MinScrapeInterval string `json:"min_scrape_interval"`
			} `json:"operations"`
		} `json:"broker"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&doc); err != nil {
		return 0, err
	}
	v := doc.Broker.Operations.MinScrapeInterval
	if v == "" {
		return 0, fmt.Errorf("the configuration route does not carry min_scrape_interval, " +
			"so this role cannot tell how stale a counter it is reading")
	}
	return time.ParseDuration(v)
}

func scrape(c cfg) (string, error) {
	if c.ops == "" {
		return "", fmt.Errorf("SAGUIN_SCALE_OPS is required: this role reads the broker's " +
			"own counters and will not infer them")
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/metrics", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("/metrics answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	return string(body), err
}

// routeRow is one row of /v1/operations/position-lost.
type routeRow struct {
	Reader        string `json:"reader"`
	Channel       string `json:"channel"`
	Kind          string `json:"kind"`
	Reported      bool   `json:"reported"`
	Count         uint64 `json:"count"`
	RecordsMissed uint64 `json:"records_missed"`
	LastPosition  uint64 `json:"last_position"`
	LastFloor     uint64 `json:"last_floor"`
	LastSeen      string `json:"last_seen"`
}

// positionLostRoute reads which readers the floor passed, keyed by reader
// and channel. **Which cohort was passed is only answerable here**: the
// counter says how many times and the readers' own streams say what they
// missed, but neither names who, and naming who is the assertion this run
// exists to make.
func positionLostRoute(c cfg) (map[[2]string]routeRow, int, uint64, error) {
	if c.ops == "" {
		return nil, 0, 0, fmt.Errorf("SAGUIN_SCALE_OPS is required: the cohort assertion " +
			"reads /v1/operations/position-lost")
	}
	req, _ := http.NewRequest("GET", "http://"+c.ops+"/v1/operations/position-lost", nil)
	req.SetBasicAuth(c.admin, c.adminPw)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, 0, 0, fmt.Errorf("/v1/operations/position-lost answered %d", resp.StatusCode)
	}
	var body struct {
		Readers  []routeRow `json:"readers"`
		Returned int        `json:"returned"`
		Tracked  int        `json:"tracked"`
		Beyond   uint64     `json:"beyond"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&body); err != nil {
		return nil, 0, 0, err
	}
	out := map[[2]string]routeRow{}
	for _, r := range body.Readers {
		out[[2]string{r.Reader, r.Channel}] = r
	}
	return out, body.Tracked, body.Beyond, nil
}

// readerID strips the reader scheme the broker stores a name under, so a
// row can be matched against the client id this role dialled with.
func readerID(reader string) string {
	if _, id, ok := strings.Cut(reader, ":"); ok {
		return id
	}
	return reader
}

func joinIfAny(prefix string, items []string) string {
	if len(items) == 0 {
		return ""
	}
	return prefix + strings.Join(items, "; ")
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// writeRetentionRows writes the raw per-reader evidence beside the report,
// unjudged, the way every other reporting role writes its rows: the cohort
// beside the number it qualifies, so a reader of the file can redo the
// arithmetic rather than take this role's word for it.
func writeRetentionRows(t *testing.T, c cfg, log *runlog, readers []*retReader) {
	var b strings.Builder
	fmt.Fprintf(&b, "# retention, raw per-reader evidence\n")
	fmt.Fprintf(&b, "# gaps are counted PER READER PER TOPIC: each publisher numbers its own\n")
	fmt.Fprintf(&b, "# stream from 1, and one sequence per reader makes interleaved streams\n")
	fmt.Fprintf(&b, "# look contiguous and reports zero whatever the broker did.\n")
	fmt.Fprintf(&b, "# A fast reader with gaps > 0 is the run failing; a slow cohort with\n")
	fmt.Fprintf(&b, "# gaps == 0 is the checker failing.\n")
	fmt.Fprintf(&b, "reader\tchannel\tcohort\ttopics\trecords\tgaps\tfirst_gap\n")
	sort.Slice(readers, func(i, j int) bool { return readers[i].id < readers[j].id })
	for _, r := range readers {
		topics, gaps, records, firstGap := r.snapshot()
		fmt.Fprintf(&b, "%s\t%s\t%s\t%d\t%d\t%d\t%s\n",
			r.id, r.channel, r.cohort(), topics, records, gaps, firstGap)
	}
	p := topicRowsPath(c.report)
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing the per-reader rows to %s: %v", p, err)
		return
	}
	log.logf("per-reader evidence written to %s (%d readers)", p, len(readers))
}

func retentionNumbers(c cfg, names []string, fast map[string]bool, published int64) map[string]string {
	out := map[string]string{"records published": strconv.FormatInt(published, 10)}
	if after, err := channelCounters(c, names); err == nil {
		for _, n := range names {
			out["floor moved on "+n] = fmt.Sprintf("%.0f records", after[n].removed)
			out["position_lost on "+n] = fmt.Sprintf("%.0f occurrences", after[n].lost)
		}
	}
	if rows, tracked, beyond, err := positionLostRoute(c); err == nil {
		out["readers named on the route"] = fmt.Sprintf("%d (returned %d, beyond %d)",
			tracked, len(rows), beyond)
	}
	if h, err := headroom(c, names, fast); err == nil {
		for _, n := range names {
			if v, ok := h[n]; ok {
				out["fast cohort headroom on "+n] = fmt.Sprintf("%d records above the floor "+
					"(the nearest one; near zero means this run is sized too tightly for "+
					"this machine, not that the broker misbehaved)", v)
			}
		}
	}
	return out
}
