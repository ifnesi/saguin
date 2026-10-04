package scaletest

// Seconds-scale runs of the fleet and gateway roles against an in-process
// broker, every role at once as the machines run them.
//
// **Plumbing guards, not evidence**, as the retention smoke is: the numbers a
// scenario exists for need the machines, the link and thirty minutes. These
// assert that the roles still meet - markers, ramp, the target proof - that
// every assertion a role makes still holds on a broker that loses nothing,
// and that the rows the report role joins still add up: every publisher
// topic's acknowledged seq is the highest seq its consumers reached.
//
// **Not gated on SAGUIN_SCALE_ROLE**, for the retention smoke's reason: a
// guard behind the role variable skips in every `make check` and reports
// success for work nobody did.

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/channel"
)

// startScaleBroker runs an in-process broker on chans with an operations
// listener whose /metrics is never cached, and answers the broker's address
// and the listener's.
func startScaleBroker(t *testing.T, chans []*channel.Channel) (string, string) {
	t.Helper()
	h := brokertest.StartRandom(t, chans)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	ops := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(brokertest.TCPOnly(ops), time.Millisecond, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	t.Cleanup(stop)
	return h.Addr, ops
}

// runRoles runs every role at once, each in its own subtest so a role's
// Fatalf is legal and stops only that role, and waits for all of them.
//
// **Each subtest on a goroutine of its own, not t.Parallel**: the roles wait
// for each other's markers, and parallel subtests run at most -parallel at
// a time, which is GOMAXPROCS - so on a 2-core machine the third role waited
// for the other two to give up.
func runRoles(t *testing.T, roles map[string]cfg) {
	t.Run("roles", func(t *testing.T) {
		var wg sync.WaitGroup
		for name, c := range roles {
			wg.Add(1)
			go func() {
				defer wg.Done()
				t.Run(name, func(t *testing.T) {
					log := &runlog{}
					switch c.role {
					case "publishers":
						runPublishers(t, c, log)
					case "consumers":
						runConsumers(t, c, log)
					}
				})
			}()
		}
		wg.Wait()
	})
}

// tsvRows reads a role's -topics.tsv: topic -> column -> value.
func tsvRows(t *testing.T, report string) map[string]map[string]string {
	t.Helper()
	f, err := os.Open(topicRowsPath(report))
	if err != nil {
		t.Fatalf("the role left no rows: %v", err)
	}
	defer f.Close()
	rows := map[string]map[string]string{}
	var cols []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if cols == nil {
			cols = fields
			continue
		}
		row := map[string]string{}
		for i, v := range fields {
			if i < len(cols) {
				row[cols[i]] = v
			}
		}
		rows[fields[0]] = row
	}
	return rows
}

// joinTails is the report role's equality, done here: for every append and
// broadcast topic a publisher had acknowledged, a consumer file that holds
// the topic reached exactly that seq. It answers how many topics it
// compared, so an empty join fails rather than passes (rule 12).
func joinTails(t *testing.T, pub map[string]map[string]string, consumers ...map[string]map[string]string) int {
	t.Helper()
	compared := 0
	for topic, p := range pub {
		ti, ok := targetFor(topic)
		if !ok || (targets[ti].kind != "append" && targets[ti].kind != "bcast") {
			continue
		}
		for _, rows := range consumers {
			r, ok := rows[topic]
			if !ok {
				continue
			}
			compared++
			if r["max_seq"] != p["acked_seq"] || r["survived"] != "1" {
				t.Errorf("%s: consumers reached seq %s (survived %s), the publisher had %s acknowledged",
					topic, r["max_seq"], r["survived"], p["acked_seq"])
			}
		}
	}
	return compared
}

func reportHas(t *testing.T, report string, wants ...string) {
	t.Helper()
	body, err := os.ReadFile(report)
	if err != nil {
		t.Fatalf("reading %s: %v", report, err)
	}
	for _, w := range wants {
		if !strings.Contains(string(body), w) {
			t.Errorf("%s carries no %q", filepath.Base(report), w)
		}
	}
}

// The gateway, on one machine: a publishing cohort, a wide consumer cohort
// that takes the whole plant and a narrow one that takes one stream a
// target, each waiting for the others through its own cohort's markers.
func TestTheGatewayScenarioStillRuns(t *testing.T) {
	useScenario(t, "gateway")
	addr, ops := startScaleBroker(t, []*channel.Channel{
		{Name: "scale-state", Type: channel.Latest, Filter: "scale/state/#"},
		{Name: "gw-evt", Type: channel.Append, Filter: "scale/gw/evt/#"},
		{Name: "gw-kv", Type: channel.Latest, Filter: "scale/gw/kv/#"},
	})
	dir := t.TempDir()
	base := cfg{
		scenario: "gateway", broker: addr, ops: ops, plaintext: true,
		duration: 3 * time.Second, settle: 2 * time.Second, rate: 20, seed: 1,
		patience: 30 * time.Second,
	}
	pub, wide, narrow := base, base, base
	pub.role, pub.cohort, pub.clients, pub.ramp = "publishers", "local", 6, 6
	pub.size, pub.ready, pub.target = 2048, []string{"wide", "narrow"}, 6+2+3
	pub.report = filepath.Join(dir, "pub.md")
	wide.role, wide.cohort, wide.shape, wide.clients, wide.ramp = "consumers", "wide", "wide", 2, 2
	wide.done, wide.report = []string{"local"}, filepath.Join(dir, "wide.md")
	narrow.role, narrow.cohort, narrow.shape, narrow.clients, narrow.ramp = "consumers", "narrow", "narrow", 3, 3
	narrow.done, narrow.report = []string{"local"}, filepath.Join(dir, "narrow.md")

	runRoles(t, map[string]cfg{"publishers": pub, "wide": wide, "narrow": narrow})
	if t.Failed() {
		return
	}

	pubRows, wideRows, narrowRows := tsvRows(t, pub.report), tsvRows(t, wide.report), tsvRows(t, narrow.report)
	// Six publishers: three on gw-evt, one on gw-kv, two on broadcast. The
	// wide cohort holds all five append and broadcast topics; the narrow
	// one the p0 of each.
	if n := joinTails(t, pubRows, wideRows); n != 5 {
		t.Errorf("the wide cohort's rows joined %d publisher topics, want all 5 append and broadcast", n)
	}
	if n := joinTails(t, pubRows, narrowRows); n != 2 {
		t.Errorf("the narrow cohort's rows joined %d publisher topics, want the 2 append and broadcast p0", n)
	}
	for _, rows := range []map[string]map[string]string{pubRows, wideRows} {
		for topic, r := range rows {
			if r["cohort"] == "" {
				t.Errorf("row %s carries no cohort", topic)
			}
		}
	}
	for topic := range pubRows {
		if !strings.Contains(topic, "/local/p") {
			t.Errorf("publisher topic %s does not carry its cohort", topic)
		}
	}
	reportHas(t, pub.report, "published MB/s", "PUBACK latency p50 / p99 / max", "cohort: local")
	reportHas(t, wide.report, "delivered MB/s", "payload bytes received", "cohort: wide (wide)")
	// The size reached the wire: every record the wide cohort took was
	// padded to exactly 2KB, so its bytes are its deliveries times that.
	got := map[string]int{}
	body, _ := os.ReadFile(wide.report)
	for _, line := range strings.Split(string(body), "\n") {
		for _, k := range []string{"payload bytes received", "deliveries total"} {
			if v, ok := strings.CutPrefix(line, "- "+k+": "); ok {
				got[k], _ = strconv.Atoi(v)
			}
		}
	}
	if got["deliveries total"] == 0 || got["payload bytes received"] != got["deliveries total"]*2048 {
		t.Errorf("the wide cohort received %d payload bytes over %d deliveries, want 2048 each",
			got["payload bytes received"], got["deliveries total"])
	}
}

// The fleet, unchanged by the gateway: the smallest population in which
// every target has a publisher and a consumer (a queue takes 1% of
// publishers and half a percent of consumers).
func TestTheFleetScenarioStillRuns(t *testing.T) {
	chans := []*channel.Channel{{Name: "scale-state", Type: channel.Latest, Filter: "scale/state/#"}}
	for _, tg := range fleetTargets {
		ch := &channel.Channel{Name: tg.name, Filter: tg.prefix + "/#"}
		switch tg.kind {
		case "append":
			ch.Type = channel.Append
		case "latest":
			ch.Type = channel.Latest
		case "queue":
			ch.Type, ch.VisibilityTimeout = channel.Queue, 30
			ch.MaxAttempts = brokertest.MaxAttempts
		default:
			continue
		}
		chans = append(chans, ch)
	}
	addr, ops := startScaleBroker(t, chans)
	dir := t.TempDir()
	base := cfg{
		scenario: "fleet", broker: addr, ops: ops, plaintext: true,
		duration: 3 * time.Second, settle: 2 * time.Second, rate: 2, seed: 1,
		patience: 30 * time.Second,
	}
	pub, con := base, base
	pub.role, pub.clients, pub.peers, pub.ramp = "publishers", 100, 200, 100
	pub.report = filepath.Join(dir, "pub.md")
	con.role, con.clients, con.peers, con.ramp = "consumers", 200, 100, 200
	con.report = filepath.Join(dir, "con.md")

	runRoles(t, map[string]cfg{"publishers": pub, "consumers": con})
	if t.Failed() {
		return
	}
	if n := joinTails(t, tsvRows(t, pub.report), tsvRows(t, con.report)); n == 0 {
		t.Error("the fleet's rows joined no publisher topic")
	}
	reportHas(t, con.report, "publisher streams nobody followed: 0", "job acks sent: ")
	// The ack assertion reads nothing if no job was acked: prove some were.
	if body, _ := os.ReadFile(con.report); strings.Contains(string(body), "job acks sent: 0\n") {
		t.Error("the consumers acked no job, so \"every job's ack was accepted\" examined nothing")
	}
}

// A gateway run's replay takes the scenario and no cohort: it owns no topics
// and names no clients, and the runbook's replay line carries no COHORT. The
// gateway scale run of 2026-09-27 had to invent one to run its replay.
func TestAGatewayReplayNeedsNoCohort(t *testing.T) {
	t.Setenv("SAGUIN_SCALE_ROLE", "replay")
	t.Setenv("SAGUIN_SCALE_SCENARIO", "gateway")
	t.Setenv("SAGUIN_SCALE_BROKER", "127.0.0.1:1")
	if c := readCfg(t); c.scenario != "gateway" || c.cohort != "" {
		t.Errorf("a gateway replay read as scenario %q, cohort %q; want gateway and none", c.scenario, c.cohort)
	}
}
