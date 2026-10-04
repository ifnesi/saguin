package scaletest

// A seconds-scale run of ROLE=compare on this machine: saguin built from
// this tree, and mosquitto where it is installed, started, sampled and
// driven by the same code that drives the Pi, with REMOTE empty.
//
// **Plumbing guards, not evidence**, as the other smokes are: that each
// case yields its rows, that the sampler read the broker it started, and
// that a broker which loses nothing is reported as losing nothing.

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// freeProbePort is a port in 31000-31999 that nothing listens on.
func freeProbePort(t *testing.T) int {
	t.Helper()
	for range 50 {
		p := 31000 + rand.Intn(1000)
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(p))
		if err == nil {
			ln.Close()
			return p
		}
	}
	t.Fatal("no free port in 31000-31999")
	return 0
}

// Ubuntu confines /usr/sbin/mosquitto to /etc/mosquitto with AppArmor.
// The compare role deliberately keeps every generated file in DIR, so its
// local smoke executes a byte-for-byte copy under the test directory. The
// broker bits and version are unchanged; only the path attachment is absent.
func localMosquitto(t *testing.T, installed, dir string) string {
	t.Helper()
	b, err := os.ReadFile(installed)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the basename "mosquitto", which is what /proc/comm reports and
	// the sampler verifies, but not at dir/mosquitto: that is also the plain
	// configuration's directory, which start removes before each case.
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	to := filepath.Join(binDir, "mosquitto")
	if err := os.WriteFile(to, b, 0o755); err != nil {
		t.Fatal(err)
	}
	return to
}

func TestTheCompareRoleMeasuresEachBrokerItStarts(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/saguin").CombinedOutput(); err != nil {
		t.Fatalf("building saguin: %v\n%s", err, out)
	}
	// Every case on one configuration of each broker; the second of each
	// only starts, holds and hands back a queue, which is where its
	// storage differs, so the smoke stays seconds long.
	full, partial := []string{"saguin-memory", "saguin-memory-tls"}, []string{"saguin-sqlite"}
	mosq, err := exec.LookPath("mosquitto")
	if err == nil {
		mosq = localMosquitto(t, mosq, dir)
		full = append(full, "mosquitto", "mosquitto-tls")
		partial = append(partial, "mosquitto-persist")
	} else {
		t.Log("mosquitto is not on PATH: only saguin's configurations are driven")
	}
	configs := append(append([]string{}, full...), partial...)
	c := cmpCfg{
		dir: dir, saguin: bin, mosquitto: mosq,
		host: "127.0.0.1", port: freeProbePort(t),
		configs: full, cases: cmpCases, runs: 1,
		ns: []int{20}, rates: []int{1000}, qos: []byte{0, 1},
		pairs: 5, size: 128,
		idle: 200 * time.Millisecond, warm: 200 * time.Millisecond,
		window: time.Second, step: 300 * time.Millisecond,
		ceilFrom: 1000, ceilMax: 1000,
		queueN: 20, queueD: 5, drain: 5 * time.Second,
		sample: 20 * time.Millisecond, quiet: 100 * time.Millisecond, listen: 30 * time.Second,
		report: filepath.Join(dir, "compare-report.md"), commit: "smoke",
		pinCPUs: os.Getenv("SAGUIN_SCALE_BROKER_CPUS"),
	}
	c = c.prepareSecure(t)
	for _, name := range []string{"saguin-memory", "saguin-sqlite", "saguin-memory-tls"} {
		brokerDir := filepath.Join(dir, name)
		if err := os.MkdirAll(brokerDir, 0o755); err != nil {
			t.Fatal(err)
		}
		conf := filepath.Join(dir, name+".yaml")
		if err := os.WriteFile(conf, []byte(cmpBroker(name).conf(c.port, brokerDir)), 0o644); err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command(bin, "-check-config", conf).CombinedOutput(); err != nil {
			t.Fatalf("%s's configuration is refused: %v\n%s", name, err, out)
		}
	}

	log := &runlog{}
	var rows []cmpRow
	c.matrix(t, log, &rows)
	p := c
	p.configs, p.cases = partial, []string{"idle", "queue"}
	p.matrix(t, log, &rows)
	c.configs = configs
	c.writeReport(t, c.boxFacts(), rows, log)

	count := map[string]int{}
	for _, r := range rows {
		count[r.config+" "+r.kase]++
		where := fmt.Sprintf("%s %s %s", r.config, r.kase, r.label)
		if r.proc.rssMedMB <= 0 {
			t.Errorf("%s: the sampler read no resident memory", where)
		}
		if b := cmpBroker(r.config); b != nil && b.secure {
			if !strings.Contains(r.extra, "acl_leaked=0 ") || strings.Contains(r.extra, "acl_denied=0 ") ||
				!strings.Contains(r.extra, "acl_denied=") {
				t.Errorf("%s: the ACL check's outcome is missing or not clean in %q", where, r.extra)
			}
		} else if strings.Contains(r.extra, "acl_") {
			t.Errorf("%s: an unsecured config reports an ACL check: %q", where, r.extra)
		}
		switch r.kase {
		case "rate":
			if r.offered == 0 || r.p50 <= 0 {
				t.Errorf("%s: offered %d with p50 %s, so the load never ran", where, r.offered, r.p50)
			}
			// Due comes before send, so end to end can never be the smaller.
			if r.e2eP50 <= 0 || r.e2eP99 < r.p99 {
				t.Errorf("%s: end-to-end p50 %s and p99 %s against from-send p99 %s", where, r.e2eP50, r.e2eP99, r.p99)
			}
			// Every step waited for quiet first, which proves steps settles.
			if i := strings.Index(r.extra, "settled_before="); i < 0 {
				t.Errorf("%s: no settled_before in %q", where, r.extra)
			} else if d, err := time.ParseDuration(strings.Fields(r.extra[i+len("settled_before="):])[0]); err != nil || d < c.quiet {
				t.Errorf("%s: settled for %s (%v) before the step, want at least %s", where, d, err, c.quiet)
			}
			if strings.HasPrefix(r.label, "qos1") && r.delivered != r.offered {
				t.Errorf("%s: delivered %d of %d offered at QoS 1 on a local link", where, r.delivered, r.offered)
			}
			if r.proc.cpuCores <= 0 {
				t.Errorf("%s: the broker used no CPU while it carried %d messages", where, r.delivered)
			}
			if r.genCores <= 0 {
				t.Errorf("%s: the load used no CPU of its own while it offered %d messages", where, r.offered)
			}
		case "queue":
			if want := int64(c.queueN * c.queueD); r.offered != want || r.delivered != want {
				t.Errorf("%s: %d acknowledged and %d delivered, want %d each (%s)",
					where, r.offered, r.delivered, want, r.extra)
			}
		case "conns":
			if !r.pass {
				t.Errorf("%s: %s", where, r.extra)
			}
		}
	}
	for _, name := range configs {
		want := map[string]int{"idle": 1, "queue": 1}
		if contains(full, name) {
			want = map[string]int{"idle": 1, "conns": 1, "rate": 2, "ceiling": 2, "queue": 1}
		}
		for _, kase := range cmpCases {
			if got := count[name+" "+kase]; got != want[kase] {
				t.Errorf("%s %s: %d rows, want %d", name, kase, got, want[kase])
			}
		}
	}

	tsv, err := os.ReadFile(filepath.Join(dir, "compare-report-rows.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(tsv), "\n") - 1; got != len(rows) {
		t.Errorf("the rows file holds %d rows, want %d", got, len(rows))
	}
	report, err := os.ReadFile(c.report)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range configs {
		if !strings.Contains(string(report), "| "+name) && !strings.Contains(string(report), " "+name+" |") {
			t.Errorf("the report's table has no column for %s", name)
		}
	}
	t.Logf("%d rows over %d configuration(s)\n%s", len(rows), len(configs), c.summary(rows))
}

// The secure configuration generators name real certificates, password
// files and ACLs. Sagüin's checker opens all of them; Mosquitto 2.0 has no
// config-test flag, so successfully reaching its listener is the equivalent.
func TestTheTLSCompareConfigurationsParse(t *testing.T) {
	mosq, err := exec.LookPath("mosquitto")
	if err != nil {
		t.Skip("mosquitto is not on PATH")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not on PATH")
	}
	dir := t.TempDir()
	mosq = localMosquitto(t, mosq, dir)
	bin := filepath.Join(dir, "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/saguin").CombinedOutput(); err != nil {
		t.Fatalf("building saguin: %v\n%s", err, out)
	}
	c := cmpCfg{
		dir: dir, saguin: bin, mosquitto: mosq, host: "127.0.0.1", port: freeProbePort(t),
		configs: []string{"saguin-memory-tls", "mosquitto-tls"}, ns: []int{1}, phaseNs: []int{2},
		pairs: 3, queueN: 1, sample: 20 * time.Millisecond, listen: 10 * time.Second,
	}
	c = c.prepareSecure(t)
	saguinPasswords, err := os.ReadFile(filepath.Join(dir, "h2h-secure", "saguin.passwd"))
	if err != nil {
		t.Fatal(err)
	}
	mosquittoPasswords, err := os.ReadFile(filepath.Join(dir, "h2h-secure", "mosquitto.passwd"))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"saguin": saguinPasswords, "mosquitto": mosquittoPasswords} {
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if !strings.Contains(line, ":$7$1000$") {
				t.Fatalf("%s password entry does not use PBKDF2-SHA512 at 1000 iterations: %q", name, line)
			}
		}
	}
	saguinDir := filepath.Join(dir, "saguin-memory-tls")
	if err := os.MkdirAll(saguinDir, 0o755); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(saguinDir, "conf")
	if err := os.WriteFile(conf, []byte(cmpBroker("saguin-memory-tls").conf(c.port, saguinDir)), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "-check-config", conf).CombinedOutput(); err != nil {
		t.Fatalf("saguin-memory-tls's configuration is refused: %v\n%s", err, out)
	}
	c.secure = true
	for _, name := range c.configs {
		t.Run(name+"-acl", func(t *testing.T) {
			b := cmpBroker(name)
			p := c.start(t, b)
			defer c.stop(t, b, p)
			assertTLSACLIsolation(t, c, b)
			res := c.aclProbe(b)
			t.Logf("%s: %s", b.name, res)
			if err := res.verdict(); err != nil {
				t.Errorf("%s: %v", b.name, err)
			}
		})
	}
}

// The check must be able to fail (rule 15): with Sagüin's ACL replaced by
// one that grants everything, it has to report the crossing and fail.
func TestTheACLCheckSeesAnAllowAllACL(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not on PATH")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/saguin").CombinedOutput(); err != nil {
		t.Fatalf("building saguin: %v\n%s", err, out)
	}
	c := cmpCfg{
		dir: dir, saguin: bin, host: "127.0.0.1", port: freeProbePort(t),
		configs: []string{"saguin-memory-tls"}, ns: []int{1}, phaseNs: []int{2},
		pairs: 3, queueN: 1, sample: 20 * time.Millisecond, listen: 10 * time.Second,
		saguinACL: "roles:\n  all:\n    - topic: \"#\"\n      allow: [read, write]\nusers:\n  \"h2h-*\":\n    roles: [all]\n    client_ids: \"%u\"\n",
	}
	c = c.prepareSecure(t)
	c.secure = true
	b := cmpBroker("saguin-memory-tls")
	p := c.start(t, b)
	defer c.stop(t, b, p)
	res := c.aclProbe(b)
	t.Logf("allow-all: %s leaks=%q", res, res.leaks)
	if res.err != nil || res.leaked == 0 || res.verdict() == nil {
		t.Fatalf("an allow-all ACL was not reported as a leak: %+v verdict=%v", res, res.verdict())
	}
}

func TestTheACLVerdictFailsWhatItCannotVouchFor(t *testing.T) {
	good := aclResult{attempts: 5, allowed: 2, wantAllowed: 2}
	for name, tc := range map[string]struct {
		r    aclResult
		fail bool
	}{
		"clean":               {good, false},
		"zero attempts":       {aclResult{allowed: 2, wantAllowed: 2}, true},
		"leak":                {aclResult{attempts: 5, allowed: 2, wantAllowed: 2, leaked: 1, leaks: []string{"x"}}, true},
		"allowed lost":        {aclResult{attempts: 5, allowed: 1, wantAllowed: 2}, true},
		"no allowed sent":     {aclResult{attempts: 5}, true},
		"check could not run": {aclResult{err: fmt.Errorf("dial")}, true},
	} {
		if got := tc.r.verdict() != nil; got != tc.fail {
			t.Errorf("%s: verdict failed=%v, want %v", name, got, tc.fail)
		}
	}
	if s := good.String(); s != "acl_denied=5 acl_leaked=0 acl_allowed=2/2" {
		t.Errorf("row text %q", s)
	}
}

// assertTLSACLIsolation proves that credentials do not grant another pair's
// topic. It first sends authorised messages at QoS 0 and 1, so "nothing
// arrived" in the negative checks cannot be explained by a broken path.
func assertTLSACLIsolation(t *testing.T, c cmpCfg, b *cmpBrokerDef) {
	t.Helper()
	const topic = "h2h/b/h2h-pub-2/h2h-sub-2"
	delivered := make(chan string, 2)
	foreign := make(chan string, 2)
	sub, err := dial(c.mqtt("h2h-sub-2"), c.clientTLS, "h2h-sub-2", true, 0,
		func(pr paho.PublishReceived) { delivered <- string(pr.Packet.Payload) }, nil, 20)
	if err != nil {
		t.Fatalf("%s refused subscriber B's generated credential: %v", b.name, err)
	}
	pubB, err := dial(c.mqtt("h2h-pub-2"), c.clientTLS, "h2h-pub-2", true, 0, nil, nil, 20)
	if err != nil {
		disconnectAll(nil, []*paho.Client{sub})
		t.Fatalf("%s refused publisher B's generated credential: %v", b.name, err)
	}
	pubA, err := dial(c.mqtt("h2h-pub-1"), c.clientTLS, "h2h-pub-1", true, 0,
		func(pr paho.PublishReceived) { foreign <- string(pr.Packet.Payload) }, nil, 20)
	if err != nil {
		disconnectAll(nil, []*paho.Client{sub, pubB})
		t.Fatalf("%s refused publisher A's generated credential: %v", b.name, err)
	}
	defer disconnectAll(nil, []*paho.Client{sub, pubB, pubA})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sa, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}})
	if err != nil || len(sa.Reasons) != 1 || sa.Reasons[0] != 1 {
		t.Fatalf("%s did not grant subscriber B its own topic: SUBACK=%v, err=%v", b.name, sa, err)
	}
	sa, err = pubA.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}})
	if sa == nil || len(sa.Reasons) != 1 {
		t.Fatalf("%s returned no reason for A's subscription to B's topic: SUBACK=%v, err=%v", b.name, sa, err)
	}
	switch b.comm {
	case "saguin":
		if sa.Reasons[0] != 0x87 {
			t.Fatalf("%s answered A's forbidden subscription with SUBACK 0x%02X, want 0x87: err=%v", b.name, sa.Reasons[0], err)
		}
	case "mosquitto":
		if err != nil || sa.Reasons[0] != 1 {
			t.Fatalf("%s did not accept A's foreign filter for delivery-time ACL enforcement: SUBACK=%v, err=%v", b.name, sa, err)
		}
	}

	for _, qos := range []byte{0, 1} {
		payload := fmt.Sprintf("authorised-qos%d", qos)
		ack, err := pubB.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: []byte(payload)})
		if err != nil || ack == nil || ack.ReasonCode >= 0x80 {
			t.Fatalf("%s refused publisher B's QoS %d message to its own topic: PUBACK=%v, err=%v", b.name, qos, ack, err)
		}
		select {
		case got := <-delivered:
			if got != payload {
				t.Fatalf("%s delivered %q, want %q positive control", b.name, got, payload)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not deliver B's authorised QoS %d positive control", b.name, qos)
		}
		select {
		case got := <-foreign:
			t.Fatalf("%s delivered B's QoS %d message to A's foreign subscription: %q", b.name, qos, got)
		case <-time.After(300 * time.Millisecond):
		}
	}

	// Mosquitto 2.0 silently discards this unauthorised publish. Sagüin gives
	// the MQTT 5 publisher an explicit not-authorized PUBACK. In both cases B
	// must receive nothing; the two positive controls above prove its path.
	ack, err := pubA.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("forged")})
	if b.comm == "saguin" && (ack == nil || ack.ReasonCode != 0x87) {
		t.Fatalf("%s answered A's forbidden publish with PUBACK=%v, want reason 0x87: err=%v", b.name, ack, err)
	}
	select {
	case got := <-delivered:
		t.Fatalf("%s delivered client A's unauthorised publish to B: %q (PUBACK=%v, err=%v)", b.name, got, ack, err)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestOnlyTLSCompareConfigurationsUseScopedTopics(t *testing.T) {
	plain, secure := cmpCfg{}, cmpCfg{secure: true}
	for _, tc := range []struct {
		name              string
		plain, secure     string
		plainFn, secureFn func(int) string
	}{
		{"connections", "h2h/i/7", "h2h/i/h2h-idle-7", plain.idleTopic, secure.idleTopic},
		{"rate and ceiling", "h2h/b/7", "h2h/b/h2h-pub-7/h2h-sub-7", plain.broadcastTopic, secure.broadcastTopic},
		{"queue", "h2h/q/7", "h2h/q/h2h-qpub-7/h2h-q-7", plain.queueTopic, secure.queueTopic},
		{"phases", "h2h/p/7", "h2h/p/h2h-ppub-7/h2h-psub-7", plain.phaseTopic, secure.phaseTopic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.plainFn(7); got != tc.plain {
				t.Errorf("plaintext topic = %q, want the unscoped topic %q", got, tc.plain)
			}
			if got := tc.secureFn(7); got != tc.secure {
				t.Errorf("TLS topic = %q, want scoped topic %q", got, tc.secure)
			}
		})
	}
}

// The ceiling is bracketed to within 5% of its lower bound, from a ramp
// 40% apart, and stops ramping at the first failure.
func TestTheCeilingIsBisectedBetweenItsLastPassAndFirstFail(t *testing.T) {
	const truth = 6100 // a broker that sustains up to here
	var ran []int
	step := func(rate int) cmpRow {
		ran = append(ran, rate)
		return cmpRow{kase: "ceiling", label: "qos1 x", rate: rate, pass: rate <= truth, config: "b", run: 1}
	}
	rows := ramp([]int{1000, 1400, 1960, 2744, 3841, 5378, 7529, 10541}, 3, step)
	lo, hi := 0, 1<<30
	for _, r := range rows {
		if r.pass {
			lo = max(lo, r.rate)
		} else {
			hi = min(hi, r.rate)
		}
	}
	if !(lo <= truth && truth < hi) || float64(hi-lo) > 0.05*float64(lo) {
		t.Errorf("bracketed [%d, %d) around %d, want it within 5%%; ran %v", lo, hi, truth, ran)
	}
	if slices.Contains(ran, 10541) {
		t.Errorf("ramped past the first failure: ran %v", ran)
	}
	sum := cmpCfg{configs: []string{"b"}}.summary(rows)
	for _, want := range []string{
		fmt.Sprintf("| ceiling qos1: highest passing, msg/s | %d |", lo),
		fmt.Sprintf("| ceiling qos1: lowest failing, msg/s | %d |", hi),
	} {
		if !strings.Contains(sum, want) {
			t.Errorf("the summary has no %q:\n%s", want, sum)
		}
	}
	// Without bisect it is the rate case: every rate, failed or not.
	ran = nil
	if rows := ramp([]int{1000, 7000, 2000}, 0, step); len(rows) != 3 {
		t.Errorf("the rate case ran %v, want every rate", ran)
	}
}

// A rate cell drawn from a failed step says so, and the headline latency is
// end to end: the case the review measured, 100% delivered with a p99 of
// 16.5 ms from send, and 1.2 s behind schedule.
func TestARateCellFromAFailedStepSaysSo(t *testing.T) {
	rows := []cmpRow{
		{config: "b", kase: "rate", label: "qos1 80000/s", run: 1, offered: 100, delivered: 100,
			e2eP99: 1201 * time.Millisecond, p99: 16500 * time.Microsecond, pass: false},
		{config: "b", kase: "rate", label: "qos1 80000/s", run: 2, offered: 100, delivered: 100,
			e2eP99: 20 * time.Millisecond, p99: 16 * time.Millisecond, pass: true},
	}
	if rows[0].passes(0) {
		t.Error("a step 1.2s behind its schedule passed the bar")
	}
	if !rows[1].passes(0) {
		t.Error("a step delivering everything within 20ms failed the bar")
	}
	var st stepStats
	due := time.Unix(100, 0)
	st.take(due, due.Add(time.Second), due.Add(time.Second+10*time.Millisecond))
	if e2e, send := st.e2e.quantile(0.99), st.lat.quantile(0.99); e2e < time.Second || send > 20*time.Millisecond {
		t.Errorf("a delivery 10ms after a send 1s late read %s end to end and %s from send", e2e, send)
	}
	sum := cmpCfg{configs: []string{"b"}}.summary(rows)
	for _, want := range []string{
		"| qos1 80000/s: p99 ms, end to end | 1201.00 (20.00-1201.00), **1 of 2 failed** |",
		"| qos1 80000/s: delivered % | 100.0 (100.0-100.0), **1 of 2 failed** |",
	} {
		if !strings.Contains(sum, want) {
			t.Errorf("the summary has no %q:\n%s", want, sum)
		}
	}
}

// A step starts only once the deliveries before it have stopped: a
// backlog still arriving holds it, and nothing arriving does not.
func TestAStepWaitsForTheBacklogBeforeIt(t *testing.T) {
	var n atomic.Int64
	stop := time.Now().Add(300 * time.Millisecond)
	go func() {
		for time.Now().Before(stop) {
			n.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if waited := settle(n.Load, 100*time.Millisecond, 5*time.Second); waited < 300*time.Millisecond {
		t.Errorf("settled after %s while deliveries arrived for 300ms", waited)
	}
	if waited := settle(n.Load, 100*time.Millisecond, 5*time.Second); waited > 200*time.Millisecond {
		t.Errorf("waited %s with nothing arriving, want about 100ms", waited)
	}
	var busy atomic.Int64
	go func() {
		for range 200 {
			busy.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if waited := settle(busy.Load, 100*time.Millisecond, 300*time.Millisecond); waited > 400*time.Millisecond {
		t.Errorf("waited %s past a 300ms limit", waited)
	}
}

// A case that fails is recorded as a failed row with its reason, its
// directory is kept, its broker is stopped, and the next case still runs:
// whether it fails in start, before its broker listens, or inside the case
// with the broker up, which is where a long run's reset
// connect once failed it. "mute" listens and speaks no MQTT, so every connect
// to it fails.
func TestAFailedCaseIsARowAndTheMatrixGoesOn(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Fatal("python3 is the mute broker here, and it is not on PATH")
	}
	dir := t.TempDir()
	saved := cmpBrokers
	t.Cleanup(func() { cmpBrokers = saved })
	cmpBrokers = append(append([]cmpBrokerDef{}, saved...),
		cmpBrokerDef{
			name: "never", comm: "sleep", durability: "none",
			conf: func(int, string) string { return "x\n" },
			cmd:  func(cmpCfg) string { return "sleep 300" },
		},
		cmpBrokerDef{
			name: "mute", comm: "python3", durability: "none",
			conf: func(int, string) string { return "x\n" },
			cmd: func(c cmpCfg) string {
				return fmt.Sprintf("python3 -m http.server --bind 127.0.0.1 %d", c.port)
			},
		})
	c := cmpCfg{
		dir: dir, host: "127.0.0.1", port: freeProbePort(t),
		configs: []string{"never", "mute"}, cases: []string{"idle", "queue"}, runs: 1,
		idle: 200 * time.Millisecond, warm: 100 * time.Millisecond,
		queueN: 5, queueD: 2, drain: 5 * time.Second,
		sample: 20 * time.Millisecond, quiet: 100 * time.Millisecond, listen: 2 * time.Second,
		report: filepath.Join(dir, "r.md"),
	}
	var rows []cmpRow
	c.matrix(t, &runlog{}, &rows)
	c.writeReport(t, "cpus: 1\n", rows, &runlog{})

	byCase := map[string]cmpRow{}
	for _, r := range rows {
		byCase[r.config+" "+r.kase] = r
	}
	if len(rows) != 4 {
		t.Fatalf("%d rows, want one for each of four cases: %+v", len(rows), rows)
	}
	for _, k := range []string{"never idle", "never queue"} {
		if r := byCase[k]; !r.caseFailed || !strings.Contains(r.extra, "did not listen") {
			t.Errorf("%s: %+v", k, r)
		}
	}
	if r := byCase["mute idle"]; r.caseFailed || !r.pass {
		t.Errorf("the case after the failures reads %+v", r)
	}
	if r := byCase["mute queue"]; !r.caseFailed || !strings.Contains(r.extra, "connecting 5 clients") {
		t.Errorf("a case failing inside, with its broker up, reads %+v", r)
	}
	for _, g := range []string{"never-failed-idle-run1-*", "never-failed-queue-run1-*", "mute-failed-queue-run1-*"} {
		if kept, _ := filepath.Glob(filepath.Join(dir, g)); len(kept) != 1 {
			t.Errorf("kept %v, want one directory matching %s", kept, g)
		}
	}
	for _, p := range []string{"sleep 300", fmt.Sprintf("http.server --bind 127.0.0.1 %d", c.port)} {
		if out, _ := exec.Command("sh", "-c", "ps -eo args | grep -F -- '"+p+"' | grep -v grep").Output(); len(out) > 0 {
			t.Errorf("a failed case's broker is still running: %s", out)
		}
	}
	report, _ := os.ReadFile(c.report)
	if !strings.Contains(string(report), "**3 case(s) failed and are in no figure**") {
		t.Errorf("the report does not count the failed cases:\n%s", report)
	}
}
