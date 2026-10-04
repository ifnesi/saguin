package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// BenchmarkFirstDeployment is the first-deployment row: the binary, on the
// configuration README.md shows a first user, with nothing tuned.
//
// **A default a first user trips over fails a row here instead of a
// reputation.** Every other bench row runs a harness whose limits a test
// chose; this one runs what `saguin --config` does with the block a reader
// copies. Three things are changed, and only because the row cannot run
// without them: the database path, which README puts under /tmp; the
// MQTT port, which defaults to :1883; and an operations listener on
// loopback, which the block may not have and which is how the row reads what
// the broker lawfully gave up.
//
// **The row fails on what a client sees as an error**: a refused CONNECT or
// SUBSCRIBE, a refused or failed publish, a DISCONNECT from the broker, or a
// record that neither arrived nor was counted by the broker as given up or
// superseded - and on a duplicate, a gap, or a latest subscriber left on an
// older value. A broadcast delivery given up at `limits.session_queue_bytes`
// and a latest value superseded while its subscriber waited are reported,
// not failed: RFC 0003 permits both.
//
// The gateway shape runs by default - DeliveryPaths' 100 publishers of 16KB
// at 40 a second into 15 wide consumers. The fleet shape's 500 consumers run
// with SAGUIN_BENCH_MAX_CONSUMERS set, as DeliveryPaths' do.
func BenchmarkFirstDeployment(b *testing.B) {
	bin := filepath.Join(b.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		b.Fatalf("build the binary: %v\n%s", err, out)
	}
	block := readmeBlock(b)

	shapes := []firstShape{
		{"gateway", 100, 15, 16 << 10, 40},
		{"fleet", 8, 500, 128, 2000},
	}
	for _, sh := range shapes {
		for _, k := range firstKinds {
			b.Run(sh.name+"/"+k.name, func(b *testing.B) {
				if sh.consumers > 100 && os.Getenv("SAGUIN_BENCH_MAX_CONSUMERS") == "" {
					b.Skip("set SAGUIN_BENCH_MAX_CONSUMERS to run the fleet rows")
				}
				runFirstDeployment(b, bin, block, sh, k)
			})
		}
	}
}

type firstShape struct {
	name      string
	pubs      int
	consumers int
	bytes     int
	rate      float64 // records a second per publisher
}

// The four destinations, on README's own topics: each channel's filter as
// the block writes it, and a topic no channel claims for broadcast.
var firstKinds = []struct {
	name   string
	topic  func(i int) string
	filter string
}{
	{"append", func(i int) string { return fmt.Sprintf("iot/line%d/devices/telemetry/reading", i) },
		"iot/+/devices/telemetry/#"},
	{"latest", func(i int) string { return fmt.Sprintf("iot/line%d/devices/state", i) },
		"iot/+/devices/state"},
	{"queue", func(i int) string { return fmt.Sprintf("iot/line%d/work/resize", i) },
		"$saguin/queue/jobs"},
	{"broadcast", func(i int) string { return fmt.Sprintf("sensors/line%d", i) }, "sensors/#"},
}

// readmeBlock is the configuration README.md introduces, extracted as
// TestTheREADMEConfigurationLoads extracts it.
func readmeBlock(b *testing.B) string {
	md, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		b.Fatalf("read README.md: %v", err)
	}
	_, rest, ok := strings.Cut(string(md), "Channels are configuration, not client-side objects:")
	if ok {
		_, rest, ok = strings.Cut(rest, "```yaml\n")
	}
	var body string
	if ok {
		body, _, ok = strings.Cut(rest, "```")
	}
	if !ok || !strings.Contains(body, "channels:") {
		b.Fatal("README.md no longer shows the configuration this row runs; move or delete the row")
	}
	return body
}

// stock is the README block with the three changes the row needs, each
// required to have happened exactly once: a README that moved a line fails
// the row rather than running something else under its name.
func stock(b *testing.B, block, dir, mqttAddr, opsAddr string) string {
	// The block may already carry a loopback operations listener, as the
	// quick start does for saguin-viewer. Then its address is rewritten to
	// the port this row scrapes; otherwise a listener is added. A second
	// `operations:` key would be a startup error.
	const opsListen = "  operations:\n    listen:\n      tcp:\n        address: "
	ops := opsListen + opsAddr + "\n"
	if n := strings.Count(block, "  operations:\n"); n > 1 {
		b.Fatalf("README's block holds an operations key %d times, want at most once:\n%s", n, block)
	} else if n == 1 {
		pre, rest, ok := strings.Cut(block, opsListen)
		if !ok {
			b.Fatalf("README's operations block is not a plain tcp listener:\n%s", block)
		}
		_, after, _ := strings.Cut(rest, "\n")
		block = pre + ops + after
		ops = ""
	}
	for _, c := range []struct{ old, new string }{
		{"/tmp/saguin-quickstart.db", filepath.Join(dir, "saguin.db")},
		{"broker:\n  id: edge-1\n", "broker:\n  id: edge-1\n" +
			"  mqtt:\n    listen:\n      tcp:\n        address: " + mqttAddr + "\n" + ops},
	} {
		if n := strings.Count(block, c.old); n != 1 {
			b.Fatalf("README's block holds %q %d times, want once:\n%s", c.old, n, block)
		}
		block = strings.Replace(block, c.old, c.new, 1)
	}
	return block
}

// probePort finds a free port in 31000-31999, the range this repository's
// probe brokers use.
func probePort(b *testing.B, after int) (string, int) {
	for p := after + 1; p < 32000; p++ {
		addr := "127.0.0.1:" + strconv.Itoa(p)
		if ln, err := net.Listen("tcp", addr); err == nil {
			_ = ln.Close()
			return addr, p
		}
	}
	b.Fatal("no free port in 31000-31999")
	return "", 0
}

func runFirstDeployment(b *testing.B, bin, block string, sh firstShape, k struct {
	name   string
	topic  func(i int) string
	filter string
}) {
	dir := b.TempDir()
	mqttAddr, p := probePort(b, 31000)
	opsAddr, _ := probePort(b, p)
	cfg := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(stock(b, block, dir, mqttAddr, opsAddr)), 0o600); err != nil {
		b.Fatalf("write config: %v", err)
	}
	logs := &lockedLog{}
	cmd := exec.Command(bin, "--config", cfg)
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		b.Fatalf("start the binary: %v", err)
	}
	b.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if c, err := net.Dial("tcp", mqttAddr); err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("the binary never listened on %s:\n%s", mqttAddr, logs.String())
		}
	}

	var (
		got, gotBytes, broken atomic.Int64
		lastAt                atomic.Int64 // UnixNano of the latest arrival
		mu                    sync.Mutex
		firstBroken           string
		dups, gaps            int
		last                  = map[string]int{}    // consumer+topic: last seq seen
		jobs                  = map[string]int{}    // topic+seq: times a job was delivered
		final                 = map[string]string{} // consumer+topic: last latest value
	)
	lose := func(why string) {
		broken.Add(1)
		mu.Lock()
		if firstBroken == "" {
			firstBroken = why
		}
		mu.Unlock()
	}
	for i := range sh.consumers {
		id := fmt.Sprintf("first-con-%d", i)
		dialFirst(b, mqttAddr, id, lose, func(pr paho.PublishReceived) {
			got.Add(1)
			lastAt.Store(time.Now().UnixNano())
			gotBytes.Add(int64(len(pr.Packet.Payload)))
			seq, _, _ := strings.Cut(string(pr.Packet.Payload), "|")
			n, _ := strconv.Atoi(seq)
			key := id + " " + pr.Packet.Topic
			mu.Lock()
			switch k.name {
			case "queue":
				jobs[pr.Packet.Topic+" "+seq]++
			case "latest":
				final[key] = seq
			default:
				// A gap counts only on append: a broadcast subscriber's
				// stream lawfully skips what its session gave up.
				switch prev := last[key]; {
				case n <= prev:
					dups++
				case n > prev+1 && k.name == "append":
					gaps++
				}
				last[key] = max(last[key], n)
			}
			mu.Unlock()
			if k.name == "queue" && pr.Packet.Properties != nil && pr.Packet.Properties.ResponseTopic != "" {
				if _, err := pr.Client.Publish(context.Background(), &paho.Publish{
					Topic: pr.Packet.Properties.ResponseTopic, QoS: 1, Payload: []byte("ack"),
					Properties: &paho.PublishProperties{CorrelationData: pr.Packet.Properties.CorrelationData},
				}); err != nil {
					lose(fmt.Sprintf("%s: the ack for %s failed: %v", id, pr.Packet.Topic, err))
				}
			}
		}, k.filter)
	}
	pubs := make([]*paho.Client, sh.pubs)
	for i := range pubs {
		pubs[i] = dialFirst(b, mqttAddr, fmt.Sprintf("first-pub-%d", i), lose, nil, "")
	}

	per := max(b.N/sh.pubs, 1)
	total := per * sh.pubs
	pad := strings.Repeat("x", sh.bytes)
	every := time.Duration(float64(time.Second) / sh.rate)
	lat := make([][]time.Duration, sh.pubs)
	var refused atomic.Int64
	b.ResetTimer()
	began := time.Now()
	var wg sync.WaitGroup
	for i, c := range pubs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := time.Now()
			for s := 1; s <= per; s++ {
				time.Sleep(time.Until(next))
				next = next.Add(every)
				payload := strconv.Itoa(s) + "|" + pad[:max(sh.bytes-len(strconv.Itoa(s))-1, 0)]
				at := time.Now()
				// paho v0.23 answers a PUBACK at 0x80 or above with an error,
				// so the error is the one path a refusal takes.
				if _, err := c.Publish(context.Background(), &paho.Publish{
					Topic: k.topic(i), QoS: 1, Payload: []byte(payload)}); err != nil {
					refused.Add(1)
					lose(fmt.Sprintf("first-pub-%d seq %d: %v", i, s, err))
					continue
				}
				lat[i] = append(lat[i], time.Since(at))
			}
		}()
	}
	wg.Wait()
	published := time.Since(began)

	owed := int64(total) * int64(sh.consumers)
	if k.name == "queue" {
		owed = int64(total) // each job is owed to one worker
	}
	// Drained when everything owed arrived, or when nothing has arrived for
	// five seconds: what is left is then the broker's to account for.
	for idle, seen := time.Now(), got.Load(); got.Load() < owed; time.Sleep(50 * time.Millisecond) {
		if n := got.Load(); n != seen {
			idle, seen = time.Now(), n
		} else if time.Since(idle) > 5*time.Second {
			break
		}
	}
	b.StopTimer()
	// To the last arrival, not to the end of the wait: five idle seconds
	// charged to the rate would describe the wait rather than the broker.
	drained := time.Duration(lastAt.Load() - began.UnixNano())

	// One scrape per broker, at the end: the first scrape a broker answers
	// is computed rather than cached (min_scrape_interval), and every counter
	// began at zero when this broker started.
	m := scrapeMetrics(b, "http://"+opsAddr+"/metrics")
	series := func(name string) float64 {
		v, ok := m[name]
		if !ok {
			b.Fatalf("the scrape has no %s, so nothing it would have counted can be read", name)
		}
		return v
	}
	givenUp := int64(series(`saguin_session_deliveries_dropped_total{cause="session_queue_full"}`))
	superseded := int64(series(`saguin_latest_superseded_total{channel="state"}`))
	delivered := got.Load()

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	slices.Sort(all)
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[min(int(p*float64(len(all))), len(all)-1)]) / float64(time.Millisecond)
	}
	b.ReportMetric(float64(total)*float64(sh.bytes)/published.Seconds()/(1<<20), "publish_MB/s")
	b.ReportMetric(pct(0.50), "puback_p50_ms")
	b.ReportMetric(pct(0.99), "puback_p99_ms")
	b.ReportMetric(float64(delivered)/drained.Seconds(), "deliveries/s")
	b.ReportMetric(float64(delivered)/float64(owed)*100, "drained_%")
	b.ReportMetric(float64(givenUp), "given_up")
	b.ReportMetric(float64(superseded), "superseded")

	mu.Lock()
	defer mu.Unlock()
	if n := broken.Load(); n != 0 {
		b.Errorf("%d client errors, refusals or broker DISCONNECTs; first: %s", n, firstBroken)
	}
	if delivered+givenUp+superseded != owed {
		b.Errorf("%d received + %d given up + %d superseded = %d, owed %d: %d records nobody "+
			"accounts for", delivered, givenUp, superseded, delivered+givenUp+superseded, owed,
			owed-delivered-givenUp-superseded)
	}
	if dups != 0 || gaps != 0 {
		b.Errorf("%d duplicates and %d gaps across the subscribers' streams", dups, gaps)
	}
	if k.name == "queue" {
		once := 0
		for _, n := range jobs {
			if n == 1 {
				once++
			}
		}
		if len(jobs) != total || once != total {
			b.Errorf("%d of %d jobs delivered, %d of them exactly once", len(jobs), total, once)
		}
	}
	if k.name == "latest" {
		stale := 0
		for key, v := range final {
			if v != strconv.Itoa(per) {
				stale++
				if stale == 1 {
					b.Logf("first stale: %s holds seq %s, the last published was %d", key, v, per)
				}
			}
		}
		if want := sh.consumers * sh.pubs; len(final) != want || stale != 0 {
			b.Errorf("%d of %d subscriber-topic pairs hold a value, %d of them older than the last "+
				"published: a latest subscriber is owed the newest value", len(final), want, stale)
		}
	}
	if b.Failed() {
		b.Logf("the broker's output:\n%s", logs.String())
	}
}

// dialFirst connects one client as a first user's would: MQTT 5, clean
// start, anonymous. A consumer subscribes at QoS 1 to filter.
func dialFirst(b *testing.B, addr, id string, lose func(string),
	onMsg func(paho.PublishReceived), filter string) *paho.Client {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("%s: dial: %v", id, err)
	}
	cfg := paho.ClientConfig{Conn: packets.NewThreadSafeConn(conn), ClientID: id,
		OnClientError: func(err error) { lose(id + ": " + err.Error()) },
		OnServerDisconnect: func(d *paho.Disconnect) {
			lose(fmt.Sprintf("%s: the broker sent DISCONNECT 0x%02X", id, d.ReasonCode))
		},
	}
	if onMsg != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) { onMsg(pr); return true, nil },
		}
	}
	c := paho.NewClient(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, &paho.Connect{ClientID: id, CleanStart: true, KeepAlive: 60})
	if err != nil || ca.ReasonCode != 0 {
		b.Fatalf("%s: a stock broker refused the CONNECT (%v, %+v)", id, err, ca)
	}
	b.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
	if filter != "" {
		sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}})
		if err != nil || len(sa.Reasons) != 1 || sa.Reasons[0] > 1 {
			b.Fatalf("%s: a stock broker refused SUBSCRIBE %s (%v, %+v)", id, filter, err, sa)
		}
	}
	return c
}
