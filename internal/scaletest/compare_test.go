package scaletest

// The head-to-head: saguin against mosquitto on one box, driven from
// another. ROLE=compare runs on the load machine and runs the whole matrix:
// it starts each broker on the broker box, samples the broker's process
// there, drives the load itself and writes one report.
//
// **Only what both brokers serve**: ordinary MQTT broadcast, no channels and
// nothing read off /metrics. The baseline configurations are plaintext and
// anonymous; the `-tls` pair adds the same TLS, per-client credentials and
// per-client topic confinement to both brokers.
//
// **The broker box is reached through one command, REMOTE**, normally
// `ssh -o BatchMode=yes user@host`, and empty runs the same shell scripts
// on this machine, which is what the smoke test does. Everything the box
// is asked is a POSIX shell script, so it needs no Go and no binary of ours
// beyond saguin itself.
//
// **The sampler's lines are timed where they arrive**, on this machine's
// clock, as the load's windows are. Nothing depends on two clocks agreeing,
// and publish-to-delivery is one clock too: every publisher and subscriber
// is in this process.

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// cmpCfg is everything ROLE=compare is given, read once and printed once.
type cmpCfg struct {
	remote    []string // argv that runs a script on the broker box; empty is local sh
	dir       string   // working directory on the broker box; the shell expands it
	saguin    string   // saguin binary on the broker box
	mosquitto string   // mosquitto binary on the broker box
	host      string   // the broker box as this machine dials it
	port      int
	configs   []string
	cases     []string
	runs      int
	ns        []int // idle connections held, one case each
	rates     []int // offered msg/s, total across the pairs
	qos       []byte
	pairs     int           // publisher/subscriber pairs, one topic each
	size      int           // payload bytes
	idle      time.Duration // the idle case's measured window
	warm      time.Duration // before every measured window
	window    time.Duration // the rate cases' measured window
	step      time.Duration // the ceiling's measured part of each step
	ceilFrom  int
	ceilMax   int
	phaseNs   []int         // the phases case's connection counts
	phaseRate int           // its busy phase's total msg/s at each of them
	perPair   int           // then again at this many msg/s a pair; 0 is not
	pinCPUs   string        // taskset's list for the broker, or empty
	gctrace   bool          // Sagüin runs with GODEBUG=gctrace=1 (the phases case)
	queueN    int           // offline persistent sessions
	queueD    int           // messages queued for each
	drain     time.Duration // the most the queue's return may take
	sample    time.Duration
	listen    time.Duration // how long a broker may take to listen
	quiet     time.Duration // how long deliveries must stop before a step
	report    string
	commit    string
	secure    bool        // this case's broker uses TLS, passwords and the ACL
	clientTLS *tls.Config // authority fetched from the broker box once per run
	saguinACL string      // replaces saguinH2HACL; only the ACL check's own mutant test sets it

	// lost is the current case's clients whose connection ended under
	// them. Set per case by matrix, so every case starts at none.
	lost *losses
}

var cmpCases = []string{"idle", "conns", "rate", "ceiling", "queue", "phases"}

// cmpDefaultCases is the head-to-head's matrix; phases, the same-host
// baseline, runs when CASES names it.
var cmpDefaultCases = []string{"idle", "conns", "rate", "ceiling", "queue"}

func readCmpCfg(t *testing.T) cmpCfg {
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
	nums := func(k, d string) []int {
		var out []int
		for _, s := range strings.Split(get(k, d), ",") {
			n, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil || n < 1 {
				t.Fatalf("SAGUIN_SCALE_%s=%q: a comma-separated list of positive integers", k, get(k, d))
			}
			out = append(out, n)
		}
		return out
	}
	dur := func(k, d string) time.Duration {
		v, err := time.ParseDuration(get(k, d))
		if err != nil || v <= 0 {
			t.Fatalf("SAGUIN_SCALE_%s=%q is not a positive duration", k, get(k, d))
		}
		return v
	}
	c := cmpCfg{
		remote:    strings.Fields(get("REMOTE", "")),
		dir:       get("DIR", "$HOME/h2h"),
		mosquitto: get("MOSQUITTO", "/usr/sbin/mosquitto"),
		host:      get("HOST", ""),
		port:      num("PORT", 1883),
		configs:   strings.Split(get("CONFIGS", strings.Join(cmpBrokerNames(), ",")), ","),
		cases:     strings.Split(get("CASES", strings.Join(cmpDefaultCases, ",")), ","),
		runs:      num("RUNS", 5),
		ns:        nums("NS", "1000,5000,10000"),
		rates:     nums("RATES", "1000,5000,10000"),
		pairs:     num("PAIRS", 100),
		size:      num("SIZE", 128),
		idle:      dur("IDLE", "20s"),
		warm:      dur("WARM", "5s"),
		window:    dur("WINDOW", "30s"),
		step:      dur("STEP", "10s"),
		ceilFrom:  num("CEIL_FROM", 1000),
		ceilMax:   num("CEIL_MAX", 200000),
		phaseNs:   nums("PHASE_NS", "200,1000,10000"),
		phaseRate: num("PHASE_RATE", 10000),
		pinCPUs:   get("BROKER_CPUS", ""),
		queueN:    num("QUEUE_CLIENTS", 1000),
		queueD:    num("QUEUE_DEPTH", 100),
		drain:     dur("DRAIN", "120s"),
		sample:    dur("SAMPLE", "500ms"),
		quiet:     time.Second,
		listen:    30 * time.Second,
		report:    get("REPORT", "compare-report.md"),
		commit:    get("COMMIT", "unstated"),
		qos:       []byte{0, 1},
	}
	c.saguin = get("SAGUIN", c.dir+"/saguin")
	if v := get("PER_PAIR", "5"); v != "0" {
		c.perPair = num("PER_PAIR", 5)
	}
	// A window the sampler reads fewer than four times has no median and
	// barely a CPU delta; better refused now than found at the end of a
	// night's run.
	for k, w := range map[string]time.Duration{"IDLE": c.idle, "WINDOW": c.window, "STEP": c.step} {
		if w < 4*c.sample {
			t.Fatalf("SAGUIN_SCALE_%s=%s is under four samples at SAGUIN_SCALE_SAMPLE=%s", k, w, c.sample)
		}
	}
	if c.host == "" {
		t.Fatal("SAGUIN_SCALE_HOST is required: the broker box as this machine dials it")
	}
	if c.size < 24 {
		t.Fatalf("SAGUIN_SCALE_SIZE=%d: a payload carries 24 bytes of sequence, due and send time", c.size)
	}
	for _, n := range c.configs {
		if cmpBroker(n) == nil {
			t.Fatalf("SAGUIN_SCALE_CONFIGS names %q, which is not one of %s", n, strings.Join(cmpBrokerNames(), ", "))
		}
	}
	for _, k := range c.cases {
		if !contains(cmpCases, k) {
			t.Fatalf("SAGUIN_SCALE_CASES names %q, which is not one of %s", k, strings.Join(cmpCases, ", "))
		}
	}
	return c
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// ------------------------------------------------------------------ brokers

// cmpBrokerDef is one configuration under test: the file it runs from, the
// command that runs it in its own directory, and what it keeps.
type cmpBrokerDef struct {
	name       string
	comm       string // /proc/<pid>/comm, which the sampler must find
	durability string
	conf       func(port int, dir string) string // dir is where it runs, absolute
	cmd        func(c cmpCfg) string
	secure     bool
}

// **The limits are made equal, and each is one a broker would otherwise
// apply to the other's disadvantage**: saguin's connect rate would slow a
// ramp mosquitto takes at once, and mosquitto's Nagle would add delay Go's
// sockets never have. Everything else is each broker's default.
var cmpBrokers = []cmpBrokerDef{
	{
		name: "saguin-memory", comm: "saguin",
		durability: "memory; a snapshot at a clean stop, nothing across a crash",
		conf: func(port int, dir string) string {
			return saguinConf(port, "type: memory\n          snapshot_dir: "+dir+"/snap")
		},
		cmd: func(c cmpCfg) string { return c.saguin + " -config conf" },
	},
	{
		name: "saguin-sqlite", comm: "saguin",
		durability: "sqlite, WAL, synchronous=NORMAL: a crash keeps every commit, a power cut can lose the last",
		conf: func(port int, dir string) string {
			return saguinConf(port, "type: sqlite\n          file_path: "+dir+"/saguin.db")
		},
		cmd: func(c cmpCfg) string { return c.saguin + " -config conf" },
	},
	{
		name: "saguin-memory-tls", comm: "saguin", secure: true,
		durability: "memory; TLS with per-client passwords ($7$ PBKDF2-HMAC-SHA512, 1000 iterations per CONNECT) and per-client topic ACLs; a snapshot at a clean stop, nothing across a crash",
		conf: func(port int, dir string) string {
			return saguinTLSConf(port, dir, "type: memory\n          snapshot_dir: "+dir+"/snap")
		},
		cmd: func(c cmpCfg) string { return c.saguin + " -config conf" },
	},
	{
		name: "mosquitto", comm: "mosquitto",
		durability: "memory only; nothing across any stop",
		conf:       func(port int, dir string) string { return mosquittoConf(port, dir, false) },
		cmd:        func(c cmpCfg) string { return c.mosquitto + " -c conf" },
	},
	{
		name: "mosquitto-persist", comm: "mosquitto",
		durability: "memory, written at autosave_interval (default 1800s) and at a clean stop",
		conf:       func(port int, dir string) string { return mosquittoConf(port, dir, true) },
		cmd:        func(c cmpCfg) string { return c.mosquitto + " -c conf" },
	},
	{
		name: "mosquitto-tls", comm: "mosquitto", secure: true,
		durability: "memory only; TLS with per-client passwords ($7$ PBKDF2-HMAC-SHA512, 1000 iterations per CONNECT, matching Sagüin rather than Mosquitto 2.0's 101-iteration default) and per-client topic ACLs; nothing across any stop",
		conf:       func(port int, dir string) string { return mosquittoTLSConf(port, dir) },
		cmd:        func(c cmpCfg) string { return c.mosquitto + " -c conf" },
	},
}

func cmpBrokerNames() []string {
	var out []string
	for _, b := range cmpBrokers {
		out = append(out, b.name)
	}
	return out
}

func cmpBroker(name string) *cmpBrokerDef {
	for i := range cmpBrokers {
		if cmpBrokers[i].name == name {
			return &cmpBrokers[i]
		}
	}
	return nil
}

// saguinConf is a plain MQTT 5 broker with no channels, its sessions and
// broadcast log on the one provider.
func saguinConf(port int, provider string) string {
	return fmt.Sprintf(`broker:
  id: h2h
  log_level: info
  mqtt:
    listen:
      tcp:
        address: 0.0.0.0:%d
  limits:
    max_connections: 20000
    max_connect_rate: none
  storage:
    default: s
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - s:
          %s
channels: {}
`, port, provider)
}

func saguinTLSConf(port int, dir, provider string) string {
	return fmt.Sprintf(`broker:
  id: h2h
  log_level: info
  mqtt:
    password_file: %s/../h2h-secure/saguin.passwd
    acl_file: %s/../h2h-secure/saguin-acl.yaml
    allow_anonymous: false
    listen:
      tcp:
        address: 0.0.0.0:%d
        tls:
          cert_file: %s/../h2h-secure/server.pem
          key_file: %s/../h2h-secure/server-key.pem
  limits:
    max_connections: 20000
    max_connect_rate: none
  storage:
    default: s
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - s:
          %s
channels: {}
`, dir, dir, port, dir, dir, provider)
}

func mosquittoConf(port int, dir string, persist bool) string {
	return fmt.Sprintf(`listener %d 0.0.0.0
allow_anonymous true
persistence %t
persistence_location %s/
set_tcp_nodelay true
max_connections -1
log_dest stdout
`, port, persist, dir)
}

func mosquittoTLSConf(port int, dir string) string {
	return fmt.Sprintf(`listener %d 0.0.0.0
cafile %s/../h2h-secure/ca.pem
certfile %s/../h2h-secure/server.pem
keyfile %s/../h2h-secure/server-key.pem
password_file %s/../h2h-secure/mosquitto.passwd
acl_file %s/../h2h-secure/mosquitto.acl
allow_anonymous false
persistence false
persistence_location %s/
set_tcp_nodelay true
max_connections -1
log_dest stdout
`, port, dir, dir, dir, dir, dir, dir)
}

// ------------------------------------------------------------- broker box

// sh runs script on the broker box.
func (c cmpCfg) sh(script string) *exec.Cmd {
	if len(c.remote) == 0 {
		return exec.Command("sh", "-c", script)
	}
	args := append(append([]string{}, c.remote[1:]...), script)
	return exec.Command(c.remote[0], args...)
}

func (c cmpCfg) run(script string) (string, error) {
	out, err := c.sh(script).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

const h2hPassword = "saguin-h2h"

const saguinH2HACL = `roles:
  idle:
    - topic: h2h/i/%u
      allow: [read]
  subscriber:
    - topic: h2h/b/+/%u
      allow: [read]
  publisher:
    - topic: h2h/b/%u/+
      allow: [write]
  queue-reader:
    - topic: h2h/q/+/%u
      allow: [read]
  queue-writer:
    - topic: h2h/q/%u/+
      allow: [write]
  phase-reader:
    - topic: h2h/p/+/%u
      allow: [read]
  phase-writer:
    - topic: h2h/p/%u/+
      allow: [write]
users:
  "h2h-idle-*":
    roles: [idle]
    client_ids: "%u"
  "h2h-sub-*":
    roles: [subscriber]
    client_ids: "%u"
  "h2h-pub-*":
    roles: [publisher]
    client_ids: "%u"
  "h2h-q-*":
    roles: [queue-reader]
    client_ids: "%u"
  "h2h-qpub-*":
    roles: [queue-writer]
    client_ids: "%u"
  "h2h-psub-*":
    roles: [phase-reader]
    client_ids: "%u"
  "h2h-ppub-*":
    roles: [phase-writer]
    client_ids: "%u"
`

const mosquittoH2HACL = `pattern read h2h/i/%c
pattern read h2h/b/+/%c
pattern write h2h/b/%c/+
pattern read h2h/q/+/%c
pattern write h2h/q/%c/+
pattern read h2h/p/+/%c
pattern write h2h/p/%c/+
`

func maxInt(v []int) int {
	n := 0
	for _, x := range v {
		n = max(n, x)
	}
	return n
}

// prepareSecure makes one authority and server certificate on the broker
// box, and the complete set of users needed by this run. It is outside every
// broker directory because start removes that directory before each case.
// Both password files use PBKDF2-SHA512 at the same 1000-iteration work
// factor. Mosquitto 2.0 requires its 12-byte salt shape; saguin writes its
// usual 64-byte salt, which changes entropy but not verification work.
func (c cmpCfg) prepareSecure(t testing.TB) cmpCfg {
	t.Helper()
	wantSaguin, wantMosquitto := false, false
	for _, name := range c.configs {
		b := cmpBroker(name)
		if b == nil || !b.secure {
			continue
		}
		wantSaguin = wantSaguin || b.comm == "saguin"
		wantMosquitto = wantMosquitto || b.comm == "mosquitto"
	}
	if !wantSaguin && !wantMosquitto {
		return c
	}
	san := "DNS:" + c.host
	if net.ParseIP(c.host) != nil {
		san = "IP:" + c.host
	}
	maxIdle, maxPhases := maxInt(c.ns), maxInt(c.phaseNs)/2
	script := fmt.Sprintf(`set -e
d="%s/h2h-secure"; rm -rf "$d"; mkdir -p "$d"; cd "$d"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 -subj /CN=saguin-h2h-ca -keyout ca-key.pem -out ca.pem >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 -subj /CN=saguin-h2h -keyout server-key.pem -out server.csr >/dev/null 2>&1
cat > server.ext <<'SAGUIN_H2H_EXT'
subjectAltName=%s
extendedKeyUsage=serverAuth
SAGUIN_H2H_EXT
openssl x509 -req -sha256 -days 2 -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -extfile server.ext -out server.pem >/dev/null 2>&1
cat > saguin-acl.yaml <<'SAGUIN_H2H_ACL'
%sSAGUIN_H2H_ACL
cat > mosquitto.acl <<'SAGUIN_H2H_MOSQ_ACL'
%sSAGUIN_H2H_MOSQ_ACL
: > users
i=0; while [ $i -lt %d ]; do echo "h2h-idle-$i" >> users; i=$((i+1)); done
i=0; while [ $i -lt %d ]; do echo "h2h-sub-$i" >> users; echo "h2h-pub-$i" >> users; i=$((i+1)); done
i=0; while [ $i -lt %d ]; do echo "h2h-q-$i" >> users; i=$((i+1)); done
i=0; while [ $i -lt 10 ]; do echo "h2h-qpub-$i" >> users; i=$((i+1)); done
i=0; while [ $i -lt %d ]; do echo "h2h-psub-$i" >> users; echo "h2h-ppub-$i" >> users; i=$((i+1)); done
sort -u users -o users
`, c.dir, san, cmp.Or(c.saguinACL, saguinH2HACL), mosquittoH2HACL, maxIdle, c.pairs, c.queueN, maxPhases)
	if wantSaguin {
		script += fmt.Sprintf(`while IFS= read -r u; do %s --passwd add "$d/saguin.passwd" "$u" %s >/dev/null; done < users
`, c.saguin, h2hPassword)
	}
	if wantMosquitto {
		script += fmt.Sprintf(`: > mosquitto.passwd
openssl rand -out salt.bin 12
salt64=$(openssl base64 -A -in salt.bin)
salthex=$(od -An -v -tx1 salt.bin | tr -d ' \n')
hash64=$(openssl kdf -keylen 64 -binary -kdfopt digest:SHA512 -kdfopt pass:%s -kdfopt hexsalt:"$salthex" -kdfopt iter:1000 PBKDF2 | openssl base64 -A)
while IFS= read -r u; do
  printf '%%s:$7$1000$%%s$%%s\n' "$u" "$salt64" "$hash64" >> mosquitto.passwd
done < users
rm -f salt.bin
`, h2hPassword)
	}
	out, err := c.run(script)
	if err != nil {
		t.Fatalf("preparing TLS, credentials and ACLs on the broker box: %v\n%s", err, out)
	}
	pem, err := c.run(fmt.Sprintf(`cat "%s/h2h-secure/ca.pem"`, c.dir))
	if err != nil {
		t.Fatalf("reading the generated authority from the broker box: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pem)) {
		t.Fatal("the generated authority held no PEM certificate")
	}
	c.clientTLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return c
}

// boxFacts is what the report says about the broker box, asked of it.
func (c cmpCfg) boxFacts() string {
	out, err := c.run(fmt.Sprintf(`d="%s"; mkdir -p "$d"
echo "model: $(cat /proc/device-tree/model 2>/dev/null | tr -d '\0')"
echo "kernel: $(uname -srm)"
echo "cpus: $(nproc)"
echo "memory: $(awk '/^MemTotal:/ {print $2 " kB"}' /proc/meminfo)"
echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
echo "disk under $d: $(findmnt -no SOURCE,FSTYPE -T "$d" 2>/dev/null || echo unknown)"
echo "mosquitto: $(%s -h 2>&1 | head -1)"
echo "clock ticks: $(getconf CLK_TCK)"`, c.dir, c.mosquitto))
	if err != nil {
		return "the broker box did not answer: " + err.Error()
	}
	return out
}

// proc is one broker process on the box, and its sampler.
type proc struct {
	pid     int
	mu      sync.Mutex
	samples []procSample
	tck     float64
	comm    string
	done    chan struct{}
	sampler *exec.Cmd
}

type procSample struct {
	at    time.Time
	ticks int64
	rssKB int64
	tempC float64 // NaN where the box has no sensor
	mhz   float64
}

// start writes the configuration, starts the broker in a fresh directory
// and begins sampling it. Nothing may already be listening on the port: a
// run against a broker left over from the last one measures that one.
func (c cmpCfg) start(t testing.TB, b *cmpBrokerDef) *proc {
	t.Helper()
	addr := net.JoinHostPort(c.host, strconv.Itoa(c.port))
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Fatalf("something already listens on %s before %s started", addr, b.name)
	}
	out, err := c.run(fmt.Sprintf(`set -e
d="%s/%s"; rm -rf "$d"; mkdir -p "$d/snap"; cd "$d"
cat > conf <<'SAGUIN_H2H_EOF'
%sSAGUIN_H2H_EOF
sed "s|@DIR@|$(pwd)|g" conf > conf.new && mv conf.new conf
nohup %s > broker.log 2>&1 < /dev/null &
echo "pid $!"`, c.dir, b.name, b.conf(c.port, "@DIR@"), c.brokerCmd(b)))
	if err != nil {
		t.Fatalf("starting %s: %v", b.name, err)
	}
	var pid int
	i := strings.LastIndex(out, "pid ")
	if i < 0 {
		t.Fatalf("starting %s answered %q, with no pid", b.name, out)
	}
	if _, err := fmt.Sscanf(strings.TrimSpace(out[i:]), "pid %d", &pid); err != nil {
		t.Fatalf("starting %s answered %q, with no pid", b.name, out)
	}
	// Every way out of here but the last is a t.Fatalf, and a broker started
	// with nohup outlives the test that started it.
	started := false
	defer func() {
		if !started {
			_, _ = c.run(killScript(pid))
		}
	}()
	p := &proc{pid: pid, done: make(chan struct{})}
	p.sampler = c.sh(fmt.Sprintf(`pid=%d
echo "tck $(getconf CLK_TCK)"
while kill -0 $pid 2>/dev/null; do
  # Every sample, not once: a command started through env or taskset
  # becomes the broker a moment after its pid is known. A process that
  # ends between kill -0 and the read ends the sampling, not with no name.
  comm=$(cat /proc/$pid/comm 2>/dev/null) || break
  echo "comm $comm"
  s=$(cat /proc/$pid/stat 2>/dev/null) || break
  s=${s##*) }
  set -- $s
  rss=$(awk '/^VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null)
  temp=$(cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo -)
  khz=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null || echo -)
  echo "s $((${12} + ${13})) ${rss:-0} $temp $khz"
  sleep %g
done`, pid, c.sample.Seconds()))
	stdout, err := p.sampler.StdoutPipe()
	if err != nil {
		t.Fatalf("sampler: %v", err)
	}
	if err := p.sampler.Start(); err != nil {
		t.Fatalf("sampler: %v", err)
	}
	go func() {
		defer close(p.done)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			switch {
			case len(f) == 2 && f[0] == "tck":
				p.tck, _ = strconv.ParseFloat(f[1], 64)
			case len(f) >= 1 && f[0] == "comm":
				p.comm = strings.TrimSpace(strings.TrimPrefix(sc.Text(), "comm"))
			case len(f) == 5 && f[0] == "s":
				s := procSample{at: time.Now(), tempC: math.NaN(), mhz: math.NaN()}
				s.ticks, _ = strconv.ParseInt(f[1], 10, 64)
				s.rssKB, _ = strconv.ParseInt(f[2], 10, 64)
				if v, err := strconv.ParseFloat(f[3], 64); err == nil {
					s.tempC = v / 1000
				}
				if v, err := strconv.ParseFloat(f[4], 64); err == nil {
					s.mhz = v / 1000
				}
				p.mu.Lock()
				p.samples = append(p.samples, s)
				p.mu.Unlock()
			}
		}
		_ = p.sampler.Wait()
	}()
	deadline := time.Now().Add(c.listen)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			log, _ := c.run(fmt.Sprintf(`tail -20 "%s/%s/broker.log"`, c.dir, b.name))
			t.Fatalf("%s did not listen on %s within %s: %v\n%s", b.name, addr, c.listen, err, log)
		}
		time.Sleep(100 * time.Millisecond)
	}
	started = true
	return p
}

// brokerCmd is b's command, on BROKER_CPUS when they are named, and for
// Sagüin with its GC trace on when the case reads it (phases_test.go).
func (c cmpCfg) brokerCmd(b *cmpBrokerDef) string {
	cmd := b.cmd(c)
	if c.pinCPUs != "" {
		cmd = "taskset -c " + c.pinCPUs + " " + cmd
	}
	if c.gctrace && b.comm == "saguin" {
		cmd = "env GODEBUG=gctrace=1 " + cmd
	}
	return cmd
}

// killScript ends pid as an operator does, then for certain.
func killScript(pid int) string {
	return fmt.Sprintf(`pid=%d; kill -TERM $pid 2>/dev/null || exit 0
i=0; while kill -0 $pid 2>/dev/null && [ $i -lt 300 ]; do sleep 0.1; i=$((i+1)); done
kill -KILL $pid 2>/dev/null; true`, pid)
}

// stop ends the broker, as an operator does, and waits for the sampler to
// see it gone.
func (c cmpCfg) stop(t testing.TB, b *cmpBrokerDef, p *proc) {
	t.Helper()
	if _, err := c.run(killScript(p.pid)); err != nil {
		t.Errorf("stopping %s: %v", b.name, err)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		_ = p.sampler.Process.Kill()
		<-p.done
	}
}

// procStats is the broker's process across one window.
type procStats struct {
	samples   int
	rssMedMB  float64
	rssPeakMB float64
	cpuCores  float64 // CPU seconds per second, across every core
	tempMaxC  float64
	mhzMin    float64
}

func (p *proc) window(from, to time.Time) procStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	var in []procSample
	for _, s := range p.samples {
		if !s.at.Before(from) && !s.at.After(to) {
			in = append(in, s)
		}
	}
	st := procStats{samples: len(in), tempMaxC: math.NaN(), mhzMin: math.NaN()}
	if len(in) == 0 {
		return st
	}
	rss := make([]float64, len(in))
	for i, s := range in {
		rss[i] = float64(s.rssKB) / 1024
		st.rssPeakMB = math.Max(st.rssPeakMB, rss[i])
		if !math.IsNaN(s.tempC) && !(s.tempC <= st.tempMaxC) {
			st.tempMaxC = s.tempC
		}
		if !math.IsNaN(s.mhz) && !(s.mhz >= st.mhzMin) {
			st.mhzMin = s.mhz
		}
	}
	sort.Float64s(rss)
	st.rssMedMB = rss[len(rss)/2]
	if el := in[len(in)-1].at.Sub(in[0].at).Seconds(); len(in) > 1 && el > 0 && p.tck > 0 {
		st.cpuCores = float64(in[len(in)-1].ticks-in[0].ticks) / p.tck / el
	}
	return st
}

// -------------------------------------------------------------------- load

// cmpRow is one measured window: what the load saw and what the broker's
// process did while it lasted.
type cmpRow struct {
	config, kase, label string
	run                 int
	from, to            time.Time
	offered, delivered  int64
	rate                int // offered msg/s, for a rate or ceiling step
	// e2e is from when a message was due to its delivery, the schedule's
	// lag included: the latency a device would see. p50/p99 are from when
	// it was sent, the broker's share of it.
	e2eP50, e2eP99   time.Duration
	p50, p99, lagP99 time.Duration
	genCores         float64 // this process's CPU across the step, every core
	caseFailed       bool    // the whole case failed; extra says why
	extra            string
	pass             bool
	proc             procStats

	// The phases case's: connections held; from the publish call to paho's
	// return with the PUBACK, a wait for one of the 16 slots in flight
	// included; and Sagüin's GC trace across the window.
	conns                int
	pubackP50, pubackP99 time.Duration
	gc                   *gcPhase
}

func (r cmpRow) deliveredRate() float64 {
	return float64(r.delivered) / r.to.Sub(r.from).Seconds()
}

// stamp is a payload: sequence, the time it was due, the time it was
// sent, then padding. Binary, because a parse per delivery at tens of
// thousands a second would be the load machine's cost, not the broker's.
func stamp(seq uint64, due time.Time, size int) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b, seq)
	binary.BigEndian.PutUint64(b[8:], uint64(due.UnixNano()))
	binary.BigEndian.PutUint64(b[16:], uint64(time.Now().UnixNano()))
	for i := 24; i < size; i++ {
		b[i] = 'x'
	}
	return b
}

func unstamp(b []byte) (seq uint64, due, sent time.Time, ok bool) {
	if len(b) < 24 {
		return 0, time.Time{}, time.Time{}, false
	}
	return binary.BigEndian.Uint64(b), time.Unix(0, int64(binary.BigEndian.Uint64(b[8:]))),
		time.Unix(0, int64(binary.BigEndian.Uint64(b[16:]))), true
}

func (c cmpCfg) mqtt(id string) cfg {
	x := cfg{broker: net.JoinHostPort(c.host, strconv.Itoa(c.port)), plaintext: !c.secure}
	if c.secure {
		x.user, x.pass = id, h2hPassword
	}
	return x
}

func (c cmpCfg) idleTopic(i int) string {
	if c.secure {
		return fmt.Sprintf("h2h/i/h2h-idle-%d", i)
	}
	return fmt.Sprintf("h2h/i/%d", i)
}

func (c cmpCfg) broadcastTopic(i int) string {
	if c.secure {
		return fmt.Sprintf("h2h/b/h2h-pub-%d/h2h-sub-%d", i, i)
	}
	return fmt.Sprintf("h2h/b/%d", i)
}

func (c cmpCfg) queueTopic(i int) string {
	if c.secure {
		return fmt.Sprintf("h2h/q/h2h-qpub-%d/h2h-q-%d", i%10, i)
	}
	return fmt.Sprintf("h2h/q/%d", i)
}

func (c cmpCfg) phaseTopic(i int) string {
	if c.secure {
		return fmt.Sprintf("h2h/p/h2h-ppub-%d/h2h-psub-%d", i, i)
	}
	return fmt.Sprintf("h2h/p/%d", i)
}

// connectAll dials n clients, 200 at a time, and fails on the first that
// the broker does not accept: a figure over fewer connections than asked
// for is not the figure asked for.
//
// lost is nil only for clients the case disconnects itself before its
// measurement, whose ending is the point.
func (c cmpCfg) connectAll(t testing.TB, n int, id func(int) string, clean bool, lost *losses,
	onMsg func(i int) func(paho.PublishReceived)) []*paho.Client {
	t.Helper()
	out := make([]*paho.Client, n)
	sem := make(chan struct{}, 200)
	var wg sync.WaitGroup
	var first atomic.Value
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var h func(paho.PublishReceived)
			if onMsg != nil {
				h = onMsg(i)
			}
			clientID := id(i)
			tc := c.clientTLS
			if !c.secure {
				tc = nil
			}
			cl, err := dial(c.mqtt(clientID), tc, clientID, clean,
				map[bool]uint32{true: 0, false: 3600}[clean], h, lost, 20)
			if err != nil {
				first.CompareAndSwap(nil, fmt.Sprintf("%s: %v", id(i), err))
				return
			}
			out[i] = cl
		}()
	}
	wg.Wait()
	if e := first.Load(); e != nil {
		// The harness's own endings, not n losses on top of the one that
		// failed the case.
		disconnectAll(lost, out)
		t.Fatalf("connecting %d clients: %s", n, e)
	}
	return out
}

// disconnectAll ends clients' connections from their side. lost, when
// given, stops counting first: these endings are the harness's own.
func disconnectAll(lost *losses, cls []*paho.Client) {
	if lost != nil {
		lost.closing.Store(true)
	}
	var wg sync.WaitGroup
	for _, cl := range cls {
		if cl != nil {
			wg.Add(1)
			go func() { defer wg.Done(); _ = cl.Disconnect(&paho.Disconnect{ReasonCode: 0}) }()
		}
	}
	wg.Wait()
}

// subscribeAll subscribes client i to topic(i) at qos and fails unless the
// broker granted exactly that: a broker that granted less would be
// measured at the wrong QoS.
func subscribeAll(t testing.TB, cls []*paho.Client, topic func(int) string, qos byte) {
	t.Helper()
	var first atomic.Value
	sem := make(chan struct{}, 200)
	var wg sync.WaitGroup
	for i, cl := range cls {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sa, err := cl.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic(i), QoS: qos}}})
			if err == nil && (len(sa.Reasons) != 1 || sa.Reasons[0] != qos) {
				err = fmt.Errorf("granted %v, want QoS %d", sa.Reasons, qos)
			}
			if err != nil {
				first.CompareAndSwap(nil, fmt.Sprintf("subscribe %s: %v", topic(i), err))
			}
		}()
	}
	wg.Wait()
	if e := first.Load(); e != nil {
		t.Fatal(e)
	}
}

// stepStats is one step's deliveries, bucketed by the time each message was
// due rather than when it arrived, so a slow delivery counts against the
// step that offered it.
type stepStats struct {
	mu        sync.Mutex
	from, to  time.Time
	delivered int64
	lat       latHist // from send
	e2e       latHist // from due
}

// take counts one delivery of a message due at due and sent at sent.
func (s *stepStats) take(due, sent, now time.Time) {
	s.mu.Lock()
	s.delivered++
	s.lat.add(now.Sub(sent))
	s.e2e.add(now.Sub(due))
	s.mu.Unlock()
}

// settle waits until count has not moved for still, or limit has passed,
// and answers how long it waited.
func settle(count func() int64, still, limit time.Duration) time.Duration {
	t0 := time.Now()
	last, since := count(), time.Now()
	for time.Since(t0) < limit {
		time.Sleep(still / 10)
		if n := count(); n != last {
			last, since = n, time.Now()
		} else if time.Since(since) >= still {
			break
		}
	}
	return time.Since(t0)
}

// passes is the bar every rate and ceiling step is held to.
func (r cmpRow) passes(lost int64) bool {
	return r.offered > 0 && float64(r.delivered) >= 0.95*float64(r.offered) &&
		r.e2eP99 < time.Second && lost == 0
}

// steps offers each rate in turn over the same pairs and measures the last
// `measure` of each. A step fails if fewer than 95% of the messages it
// offered were delivered, or its end-to-end p99 reached a second.
//
// When bisect is above 0 this is the ceiling: it stops at the first step
// that fails and then halves the gap between the last that passed and the
// first that failed, up to bisect times or until the gap is under 5%, so
// the two brokers' ceilings are told apart to better than the ramp's 40%.
//
// **Open loop, with at most 16 publishes in flight per publisher**: each
// message is due at its place in the schedule, and a message is counted
// to the step whose window it was due in, whenever it went. **End to end
// is from the due time**, so the wait for a publish slot - where a broker
// slow to acknowledge QoS 1 builds its delay - is in the headline figure
// and in the bar, rather than hiding as a lower offered rate. From-send
// is kept beside it as the broker's own share.
func (c cmpCfg) steps(t testing.TB, qos byte, rates []int, warm, measure time.Duration, bisect int) []cmpRow {
	t.Helper()
	var cur atomic.Pointer[[]*stepStats]
	cur.Store(&[]*stepStats{})
	var arrived atomic.Int64 // every delivery, in a window or not
	subs := c.connectAll(t, c.pairs, func(i int) string { return fmt.Sprintf("h2h-sub-%d", i) }, true, c.lost,
		func(i int) func(paho.PublishReceived) {
			return func(pr paho.PublishReceived) {
				_, due, sent, ok := unstamp(pr.Packet.Payload)
				if !ok {
					return
				}
				arrived.Add(1)
				now := time.Now()
				for _, s := range *cur.Load() {
					if !due.Before(s.from) && due.Before(s.to) {
						s.take(due, sent, now)
						return
					}
				}
			}
		})
	defer disconnectAll(c.lost, subs)
	subscribeAll(t, subs, c.broadcastTopic, qos)
	pubs := c.connectAll(t, c.pairs, func(i int) string { return fmt.Sprintf("h2h-pub-%d", i) }, true, c.lost, nil)
	defer disconnectAll(c.lost, pubs)

	step := func(rate int) cmpRow {
		// A step after one the broker could not keep up with would start on
		// its backlog and fail on it: each starts once deliveries have
		// stopped for c.quiet, a second in a run, or after thirty.
		drained := settle(arrived.Load, c.quiet, 30*time.Second)
		start := time.Now().Add(100 * time.Millisecond)
		end := start.Add(warm + measure)
		st := &stepStats{from: start.Add(warm), to: end}
		all := append(append([]*stepStats{}, *cur.Load()...), st)
		cur.Store(&all)

		genFrom, genAt := selfCPU(), time.Now()
		interval := time.Duration(float64(time.Second) * float64(c.pairs) / float64(rate))
		var offered, sendErrs atomic.Int64
		var lagMu sync.Mutex
		var lag latHist
		var wg sync.WaitGroup
		for i, cl := range pubs {
			// Spread across one interval, so the pairs do not fire together.
			phase := time.Duration(int64(interval) * int64(i) / int64(c.pairs))
			due := make(chan time.Time)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(due)
				for d := start.Add(phase); d.Before(end); d = d.Add(interval) {
					time.Sleep(time.Until(d))
					if !d.Before(st.from) {
						offered.Add(1)
					}
					due <- d
				}
			}()
			var seq atomic.Uint64
			for range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var mine latHist
					for d := range due {
						mine.add(time.Since(d))
						_, err := cl.Publish(context.Background(), &paho.Publish{
							Topic: c.broadcastTopic(i), QoS: qos,
							Payload: stamp(seq.Add(1), d, c.size),
						})
						if err != nil {
							sendErrs.Add(1)
						}
					}
					lagMu.Lock()
					lag.merge(&mine)
					lagMu.Unlock()
				}()
			}
		}
		wg.Wait()
		genCores := (selfCPU() - genFrom).Seconds() / time.Since(genAt).Seconds()
		// What is still on its way has up to a second to arrive: past that
		// its p99 has failed the step anyway.
		for wait := time.Now().Add(time.Second); time.Now().Before(wait); time.Sleep(10 * time.Millisecond) {
			st.mu.Lock()
			all := st.delivered >= offered.Load()
			st.mu.Unlock()
			if all {
				break
			}
		}
		st.mu.Lock()
		row := cmpRow{
			label: fmt.Sprintf("qos%d %d/s", qos, rate), from: st.from, to: st.to, rate: rate,
			offered: offered.Load(), delivered: st.delivered,
			e2eP50: st.e2e.quantile(0.50), e2eP99: st.e2e.quantile(0.99),
			p50: st.lat.quantile(0.50), p99: st.lat.quantile(0.99), lagP99: lag.quantile(0.99),
			genCores: genCores,
			extra: fmt.Sprintf("send_errors=%d lost=%d settled_before=%s",
				sendErrs.Load(), c.lost.n.Load(), drained.Round(time.Millisecond)),
		}
		st.mu.Unlock()
		row.pass = row.passes(c.lost.n.Load())
		return row
	}

	return ramp(rates, bisect, step)
}

// ramp runs step at each rate and, when bisect is above 0, stops at the
// first failure and bisects between it and the last pass (steps).
func ramp(rates []int, bisect int, step func(rate int) cmpRow) []cmpRow {
	var rows []cmpRow
	lo, hi := 0, 0 // the highest rate that passed, the lowest that failed
	for _, rate := range rates {
		row := step(rate)
		rows = append(rows, row)
		if row.pass {
			lo = rate
			continue
		}
		if bisect > 0 {
			hi = rate
			break
		}
	}
	for ; bisect > 0 && lo > 0 && hi > 0 && float64(hi-lo) > 0.05*float64(lo); bisect-- {
		mid := (lo + hi) / 2
		row := step(mid)
		rows = append(rows, row)
		if row.pass {
			lo = mid
		} else {
			hi = mid
		}
	}
	return rows
}

// selfCPU is the CPU this process has used, user and system. A step whose
// load used most of this machine's cores measured the load machine, not
// the broker, and the report says so.
func selfCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// idleConns holds n persistent sessions, each with one QoS 1 subscription,
// and measures the broker holding them.
func (c cmpCfg) idleConns(t testing.TB, n int) cmpRow {
	t.Helper()
	t0 := time.Now()
	cls := c.connectAll(t, n, func(i int) string { return fmt.Sprintf("h2h-idle-%d", i) }, false, c.lost, nil)
	defer disconnectAll(c.lost, cls)
	subscribeAll(t, cls, c.idleTopic, 1)
	up := time.Since(t0)
	time.Sleep(c.warm)
	from := time.Now()
	time.Sleep(c.window)
	return cmpRow{label: fmt.Sprintf("n=%d", n), from: from, to: time.Now(), pass: c.lost.n.Load() == 0,
		extra: fmt.Sprintf("connected=%d connect_and_subscribe=%s lost=%d", n, up.Round(time.Millisecond), c.lost.n.Load())}
}

// queue parks queueN persistent sessions, publishes queueD QoS 1 messages
// to each while they are away, and times their return until every message
// is delivered.
func (c cmpCfg) queue(t testing.TB) cmpRow {
	t.Helper()
	id := func(i int) string { return fmt.Sprintf("h2h-q-%d", i) }
	from := time.Now()
	cls := c.connectAll(t, c.queueN, id, false, nil, nil)
	subscribeAll(t, cls, c.queueTopic, 1)
	disconnectAll(nil, cls)

	pubs := c.connectAll(t, 10, func(i int) string { return fmt.Sprintf("h2h-qpub-%d", i) }, true, nil, nil)
	t0 := time.Now()
	var acked, refused atomic.Int64
	var wg sync.WaitGroup
	for p, cl := range pubs {
		work := make(chan int)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(work)
			for i := p; i < c.queueN; i += len(pubs) {
				for range c.queueD {
					work <- i
				}
			}
		}()
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					_, err := cl.Publish(context.Background(), &paho.Publish{
						Topic: c.queueTopic(i), QoS: 1, Payload: stamp(0, time.Now(), c.size)})
					if err != nil {
						refused.Add(1)
					} else {
						acked.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()
	disconnectAll(nil, pubs)
	stored := time.Since(t0)

	want := acked.Load()
	var got atomic.Int64
	all := make(chan struct{})
	var once sync.Once
	t1 := time.Now()
	back := c.connectAll(t, c.queueN, id, false, c.lost, func(int) func(paho.PublishReceived) {
		return func(paho.PublishReceived) {
			if got.Add(1) == want {
				once.Do(func() { close(all) })
			}
		}
	})
	defer disconnectAll(c.lost, back)
	select {
	case <-all:
	case <-time.After(c.drain):
	}
	drained := time.Since(t1)
	// The broker's memory once it has handed everything over is part of
	// the window, which also keeps a drain faster than the sampler in it.
	time.Sleep(c.warm)
	return cmpRow{label: fmt.Sprintf("%dx%d", c.queueN, c.queueD), from: from, to: time.Now(),
		offered: want, delivered: got.Load(),
		pass: got.Load() == want && refused.Load() == 0 && c.lost.n.Load() == 0,
		extra: fmt.Sprintf("stored_in=%s drained_in=%s refused=%d lost=%d",
			stored.Round(time.Millisecond), drained.Round(time.Millisecond), refused.Load(), c.lost.n.Load())}
}

// ------------------------------------------------------------------ matrix

// runCase starts b afresh, runs one case against it and stops it: no case
// inherits another's sessions, memory or heat of the moment.
//
// **A case that fails is a row, not the end of the night.** It runs in its
// own goroutine against a caseT, whose Fatalf ends the case alone and keeps
// the reason; the failed case's directory on the box is kept, broker log
// and all, rather than wiped by the next start; and the matrix goes on.
func (c cmpCfg) runCase(t *testing.T, log *runlog, b *cmpBrokerDef, run int, kase string,
	do func(t testing.TB, c cmpCfg) []cmpRow) []cmpRow {
	ct := &caseT{TB: t}
	var rows []cmpRow
	done := make(chan struct{})
	go func() {
		defer close(done)
		rows = c.oneCase(ct, log, b, run, kase, do)
	}()
	<-done
	// A case that ended with nothing to show and no reason given was ended
	// by something other than its caseT - a closure holding the role's t -
	// and is a failure all the same.
	if ct.reason == "" && len(rows) == 0 {
		ct.reason = "the case ended without rows or a reason"
	}
	if ct.reason == "" {
		return rows
	}
	why := strings.Join(strings.Fields(ct.reason), " ")
	if len(why) > 400 {
		why = why[:400] + "..."
	}
	log.logf("FAILED %s run %d %s: %s", b.name, run, kase, why)
	kept := fmt.Sprintf("%s-failed-%s-run%d-%s", b.name, kase, run, time.Now().UTC().Format("20060102T150405Z"))
	if _, err := c.run(fmt.Sprintf(`d="%s"; [ ! -d "$d/%s" ] || mv "$d/%s" "$d/%s"`, c.dir, b.name, b.name, kept)); err != nil {
		log.logf("keeping %s's directory as %s: %v", b.name, kept, err)
	}
	return []cmpRow{{config: b.name, kase: kase, run: run, label: "case failed", caseFailed: true,
		extra: "failed: " + why + " (kept as " + kept + ")"}}
}

// caseT fails one case: Fatalf records the first reason and ends the
// case's goroutine, whose defers stop its broker. The role is failed once,
// at its end, by runCompare, for every case that failed.
type caseT struct {
	testing.TB
	mu     sync.Mutex
	reason string
}

func (ct *caseT) Fatalf(format string, a ...any) { ct.fail(fmt.Sprintf(format, a...)) }
func (ct *caseT) Fatal(a ...any)                 { ct.fail(fmt.Sprint(a...)) }
func (ct *caseT) FailNow()                       { ct.fail("FailNow") }

func (ct *caseT) fail(msg string) {
	ct.mu.Lock()
	if ct.reason == "" {
		ct.reason = msg
	}
	ct.mu.Unlock()
	ct.TB.Logf("case failed: %s", msg)
	runtime.Goexit()
}

// oneCase starts b afresh, runs one case against it and stops it.
func (c cmpCfg) oneCase(t testing.TB, log *runlog, b *cmpBrokerDef, run int, kase string,
	do func(t testing.TB, c cmpCfg) []cmpRow) []cmpRow {
	t.Helper()
	c.secure = b.secure
	c.lost = &losses{log: log}
	p := c.start(t, b)
	rows := func() (rows []cmpRow) {
		defer c.stop(t, b, p)
		rows = do(t, c)
		if c.secure {
			// After the measured work, before the broker stops: the
			// measured windows are already closed, and every case on a
			// secure config proves its ACL refused what it does not grant.
			res := c.aclProbe(b)
			if err := res.verdict(); err != nil {
				t.Fatalf("%s's ACL check failed, so its figures are not evidence: %v", b.name, err)
			}
			for i := range rows {
				rows[i].extra = strings.TrimSpace(rows[i].extra + " " + res.String())
			}
		}
		return rows
	}()
	if p.comm != b.comm {
		t.Fatalf("the sampler read process %d as %q, want %q: it measured something else", p.pid, p.comm, b.comm)
	}
	for i := range rows {
		rows[i].config, rows[i].kase, rows[i].run = b.name, kase, run
		rows[i].proc = p.window(rows[i].from, rows[i].to)
		if rows[i].proc.samples < 2 {
			t.Fatalf("%s %s %s: %d samples in its window, so no figure for it is evidence",
				b.name, kase, rows[i].label, rows[i].proc.samples)
		}
	}
	return rows
}

// matrix appends each case's rows to *rows as it finishes, so a run that
// stops part-way still reports every case it completed.
func (c cmpCfg) matrix(t *testing.T, log *runlog, rows *[]cmpRow) {
	for run := 1; run <= c.runs; run++ {
		for _, name := range c.configs {
			b := cmpBroker(name)
			for _, kase := range c.cases {
				var got []cmpRow
				switch kase {
				case "idle":
					got = c.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow {
						time.Sleep(c.warm)
						from := time.Now()
						time.Sleep(c.idle)
						return []cmpRow{{label: "no clients", from: from, to: time.Now(), pass: true}}
					})
				case "conns":
					for _, n := range c.ns {
						got = append(got, c.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow {
							return []cmpRow{c.idleConns(t, n)}
						})...)
					}
				case "rate":
					for _, q := range c.qos {
						got = append(got, c.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow {
							return c.steps(t, q, c.rates, c.warm, c.window, 0)
						})...)
					}
				case "ceiling":
					var ramp []int
					for r := float64(c.ceilFrom); int(r) <= c.ceilMax; r *= 1.4 {
						ramp = append(ramp, int(r))
					}
					for _, q := range c.qos {
						got = append(got, c.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow {
							return c.steps(t, q, ramp, c.warm, c.step, 3)
						})...)
					}
				case "queue":
					got = c.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow { return []cmpRow{c.queue(t)} })
				case "phases":
					pc := c
					pc.gctrace = true
					for _, sh := range c.phaseShapes() {
						got = append(got, pc.runCase(t, log, b, run, kase, func(t testing.TB, c cmpCfg) []cmpRow {
							return c.phases(t, b, sh)
						})...)
					}
				}
				for _, r := range got {
					log.logf("ROW %s", r.tsv())
				}
				*rows = append(*rows, got...)
			}
		}
	}
}

// ------------------------------------------------------------------ report

var cmpTSVHeader = "config\trun\tcase\tlabel\twindow_s\toffered\tdelivered\tdelivered_per_s\t" +
	"offered_per_s\te2e_p50_ms\te2e_p99_ms\tp50_ms\tp99_ms\tgen_lag_p99_ms\tgen_cpu_cores\trss_median_mb\trss_peak_mb\tcpu_cores\t" +
	"cpu_pct_per_1k_msg_s\ttemp_max_c\tmhz_min\tsamples\tpass\textra\t" +
	"conns\tpuback_p50_ms\tpuback_p99_ms\tgc_cycles\tgc_cpu_ms\tgc_assist_ms\talloc_mb\talloc_s\tlive_mb\tstacks_mb"

func ms(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d)/float64(time.Millisecond)) }

func (r cmpRow) cpuPer1k() float64 {
	if r.delivered == 0 {
		return math.NaN()
	}
	return r.proc.cpuCores * 100 / (r.deliveredRate() / 1000)
}

func (r cmpRow) tsv() string {
	return strings.Join([]string{
		r.config, strconv.Itoa(r.run), r.kase, r.label,
		fmt.Sprintf("%.1f", r.to.Sub(r.from).Seconds()),
		strconv.FormatInt(r.offered, 10), strconv.FormatInt(r.delivered, 10),
		fmt.Sprintf("%.0f", r.deliveredRate()),
		strconv.Itoa(r.rate), ms(r.e2eP50), ms(r.e2eP99),
		ms(r.p50), ms(r.p99), ms(r.lagP99), fmt.Sprintf("%.2f", r.genCores),
		fmt.Sprintf("%.1f", r.proc.rssMedMB), fmt.Sprintf("%.1f", r.proc.rssPeakMB),
		fmt.Sprintf("%.3f", r.proc.cpuCores), fmt.Sprintf("%.2f", r.cpuPer1k()),
		fmt.Sprintf("%.1f", r.proc.tempMaxC), fmt.Sprintf("%.0f", r.proc.mhzMin),
		strconv.Itoa(r.proc.samples), strconv.FormatBool(r.pass), r.extra,
		strconv.Itoa(r.conns), ms(r.pubackP50), ms(r.pubackP99), r.gcTSV(),
	}, "\t")
}

// gcTSV is the row's GC trace columns, empty for a broker without one.
func (r cmpRow) gcTSV() string {
	if r.gc == nil {
		return strings.Repeat("\t", 6)
	}
	g := r.gc
	return fmt.Sprintf("%d\t%.1f\t%.1f\t%.0f\t%.2f\t%.0f\t%.0f", g.cycles, g.cpuMs, g.assistMs, g.allocMB, g.allocSecs, g.liveMB, g.stacksMB)
}

// spread is min / median / max over the runs, which is what a figure from
// five runs is.
func spread(v []float64, format string) string {
	var ok []float64
	none := 0
	for _, x := range v {
		switch {
		case x == -1:
			none++
		case !math.IsNaN(x):
			ok = append(ok, x)
		}
	}
	if none > 0 {
		// Only the ceiling's upper bound uses it: a run that never failed.
		if len(ok) == 0 {
			return "not reached"
		}
		return spread(ok, format) + fmt.Sprintf(", not reached in %d", none)
	}
	if len(ok) == 0 {
		return "-"
	}
	sort.Float64s(ok)
	f := func(x float64) string { return fmt.Sprintf(format, x) }
	if len(ok) == 1 {
		return f(ok[0])
	}
	return f(ok[len(ok)/2]) + " (" + f(ok[0]) + "-" + f(ok[len(ok)-1]) + ")"
}

// summary is the table the item asks for: one row per measure, one column
// per configuration, each cell median (min-max) over the runs. **A cell
// that includes a failed step says how many**, since a latency from a step
// that delivered too little or too late is not a latency the broker
// sustains.
func (c cmpCfg) summary(rows []cmpRow) string {
	type key struct{ measure, config string }
	vals := map[key][]float64{}
	fails := map[key]int{}
	var order []string
	add := func(measure, config string, v float64, failed bool) {
		k := key{measure, config}
		if _, ok := vals[k]; !ok && !contains(order, measure) {
			order = append(order, measure)
		}
		vals[k] = append(vals[k], v)
		if failed {
			fails[k]++
		}
	}
	// Ceiling: per run and QoS, the highest offered rate that passed and
	// the lowest that failed. The ceiling is between them.
	type ceilKey struct {
		config, qos string
		run         int
	}
	pass, fail := map[ceilKey]int{}, map[ceilKey]int{}
	var ceilOrder []ceilKey
	for _, r := range rows {
		if r.caseFailed {
			continue
		}
		switch r.kase {
		case "idle":
			add("idle RSS, MB", r.config, r.proc.rssMedMB, false)
		case "conns":
			add("RSS holding "+r.label+" sessions, MB", r.config, r.proc.rssMedMB, !r.pass)
		case "rate":
			add(r.label+": p50 ms, end to end", r.config, float64(r.e2eP50)/1e6, !r.pass)
			add(r.label+": p99 ms, end to end", r.config, float64(r.e2eP99)/1e6, !r.pass)
			add(r.label+": p99 ms, broker's share (from send)", r.config, float64(r.p99)/1e6, !r.pass)
			add(r.label+": delivered %", r.config, 100*float64(r.delivered)/math.Max(1, float64(r.offered)), !r.pass)
			add(r.label+": CPU % of a core per 1k msg/s", r.config, r.cpuPer1k(), !r.pass)
		case "ceiling":
			k := ceilKey{r.config, r.label[:4], r.run}
			if _, seen := pass[k]; !seen {
				pass[k] = 0
				ceilOrder = append(ceilOrder, k)
			}
			if r.pass {
				pass[k] = max(pass[k], r.rate)
			} else if f, ok := fail[k]; !ok || r.rate < f {
				fail[k] = r.rate
			}
		case "queue":
			add("queue "+r.label+": delivered %", r.config, 100*float64(r.delivered)/math.Max(1, float64(r.offered)), !r.pass)
			add("queue "+r.label+": peak RSS, MB", r.config, r.proc.rssPeakMB, !r.pass)
			var drained time.Duration
			if i := strings.Index(r.extra, "drained_in="); i >= 0 {
				drained, _ = time.ParseDuration(strings.Fields(r.extra[i+len("drained_in="):])[0])
			}
			add("queue "+r.label+": drain s", r.config, drained.Seconds(), !r.pass)
		}
	}
	sort.SliceStable(ceilOrder, func(i, j int) bool { return ceilOrder[i].qos < ceilOrder[j].qos })
	for _, k := range ceilOrder {
		add("ceiling "+k.qos+": highest passing, msg/s", k.config, float64(pass[k]), false)
		f, ok := fail[k]
		if !ok {
			f = -1 // never failed up to CEIL_MAX: no upper bound
		}
		add("ceiling "+k.qos+": lowest failing, msg/s", k.config, float64(f), false)
	}
	var b strings.Builder
	b.WriteString("| measure | " + strings.Join(c.configs, " | ") + " |\n|---|")
	b.WriteString(strings.Repeat("---|", len(c.configs)) + "\n")
	for _, m := range order {
		b.WriteString("| " + m + " |")
		for _, cf := range c.configs {
			format := "%.1f"
			switch {
			case strings.HasPrefix(m, "ceiling "):
				format = "%.0f"
			case strings.Contains(m, " ms"):
				format = "%.2f"
			}
			cell := spread(vals[key{m, cf}], format)
			if n := fails[key{m, cf}]; n > 0 {
				cell += fmt.Sprintf(", **%d of %d failed**", n, len(vals[key{m, cf}]))
			}
			b.WriteString(" " + cell + " |")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (c cmpCfg) writeReport(t *testing.T, facts string, rows []cmpRow, log *runlog) {
	var b strings.Builder
	fmt.Fprintf(&b, "# compare - head to head %s\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "commit %s, %d run(s), %d pairs, %d-byte payloads, subscribers' Receive Maximum 20, "+
		"16 publishes in flight per publisher, load from %d cores. **End to end** is from when a message was "+
		"due in the schedule to its delivery, so a publisher kept waiting for a slot counts; the broker's "+
		"share is from its send. Both on one clock, each read to within a histogram bucket (25%% wide). "+
		"A step fails below 95%% delivered or at an end-to-end p99 of a second. The ceiling is bracketed "+
		"by its highest passing and lowest failing offered rate, bisected to within 5%%.\n\n",
		c.commit, c.runs, c.pairs, c.size, runtime.NumCPU())
	b.WriteString("## broker box\n\n```\n" + facts + "```\n\n## configurations\n\n")
	for _, n := range c.configs {
		bd := cmpBroker(n)
		fmt.Fprintf(&b, "- **%s**: %s.\n\n```\n%s```\n\n", n, bd.durability,
			bd.conf(c.port, c.dir+"/"+n))
	}
	b.WriteString("## numbers\n\nmedian (min-max) over the runs.")
	var failedCases []string
	for _, r := range rows {
		if r.caseFailed {
			failedCases = append(failedCases, fmt.Sprintf("- %s run %d %s: %s", r.config, r.run, r.kase, r.extra))
		}
	}
	if len(failedCases) > 0 {
		fmt.Fprintf(&b, " **%d case(s) failed and are in no figure**; each is listed under anything odd.", len(failedCases))
	}
	b.WriteString("\n\n" + c.summary(rows))
	if contains(c.cases, "phases") {
		fmt.Fprintf(&b, "\n### phases: idle, busy, idle again\n\nEach broker holds the connections (half publishers, "+
			"half subscribers, clean sessions, one QoS 1 topic a pair) for %s idle, %s warm and %s measured at the "+
			"shape's rate, then %s idle again once deliveries stop. **RSS as held** is the median of the /proc samples "+
			"in the phase, what an operator sees. Sagüin's GC figures are from its own GC trace (GODEBUG=gctrace=1), "+
			"read between the phase's start and end; no collection is forced, so the live heap and the stack scanned "+
			"are the most recent collection's in or before the phase, in whole MB (the stack figure is the part in use, "+
			"not what the stacks hold); GC CPU is given under load only, and counts a stop-the-world pause as every "+
			"processor's time, so it is an upper bound. PUBACK is from "+
			"the publish call to its acknowledgement, a wait for a slot in flight included. Broker on CPUs %q.\n\n",
			c.idle, c.warm, c.window, c.idle, c.pinCPUs)
		b.WriteString(c.phasesSummary(rows))
	}
	failed := 0
	for _, r := range rows {
		if !r.pass && r.kase != "ceiling" && !r.caseFailed {
			failed++
		}
	}
	b.WriteString("\n## anything odd\n\n")
	for _, l := range failedCases {
		b.WriteString(l + "\n")
	}
	fmt.Fprintf(&b, "\n%d window(s) outside the ceiling ramp failed their own bar "+
		"(95%% delivered, end-to-end p99 under 1s, or a queue not fully delivered); the rows say which.\n", failed)
	// A step that failed while the broker had most of its box to spare was
	// likely held up elsewhere: the load machine (one hot goroutine or a
	// collection need not show as its total CPU) or the link.
	var boxCPUs float64
	for _, l := range strings.Split(facts, "\n") {
		if v, ok := strings.CutPrefix(l, "cpus: "); ok {
			boxCPUs, _ = strconv.ParseFloat(strings.TrimSpace(v), 64)
		}
	}
	for _, r := range rows {
		if (r.kase == "rate" || r.kase == "ceiling") && !r.pass && boxCPUs > 0 && r.proc.cpuCores < boxCPUs/2 {
			fmt.Fprintf(&b, "- %s run %d %s %s failed with the broker at %.2f of its box's %.0f cores: "+
				"the limit may be the load machine or the link, not the broker.\n",
				r.config, r.run, r.kase, r.label, r.proc.cpuCores, boxCPUs)
		}
	}
	// A load machine at its own limit sets the ceiling it reports.
	for _, r := range rows {
		if r.genCores > 0.8*float64(runtime.NumCPU()) {
			fmt.Fprintf(&b, "- %s run %d %s %s: the load used %.1f of this machine's %d cores, "+
				"so this step may measure the load machine rather than the broker.\n",
				r.config, r.run, r.kase, r.label, r.genCores, runtime.NumCPU())
		}
	}
	b.WriteString("\n## log\n\n```\n" + strings.Join(log.lines, "\n") + "\n```\n")
	if err := os.WriteFile(c.report, []byte(b.String()), 0o644); err != nil {
		t.Errorf("writing %s: %v", c.report, err)
	}
	var tsv strings.Builder
	tsv.WriteString(cmpTSVHeader + "\n")
	for _, r := range rows {
		tsv.WriteString(r.tsv() + "\n")
	}
	path := strings.TrimSuffix(c.report, ".md") + "-rows.tsv"
	if err := os.WriteFile(path, []byte(tsv.String()), 0o644); err != nil {
		t.Errorf("writing %s: %v", path, err)
	}
}

func runCompare(t *testing.T) {
	c := readCmpCfg(t)
	c = c.prepareSecure(t)
	log := &runlog{}
	log.logf("role=compare host=%s port=%d remote=%q dir=%s configs=%s cases=%s runs=%d ns=%v rates=%v "+
		"pairs=%d size=%d warm=%s window=%s step=%s ceiling=%d..%d queue=%dx%d drain=%s commit=%s "+
		"phase_ns=%v phase_rate=%d per_pair=%d broker_cpus=%q",
		c.host, c.port, strings.Join(c.remote, " "), c.dir, strings.Join(c.configs, ","),
		strings.Join(c.cases, ","), c.runs, c.ns, c.rates, c.pairs, c.size, c.warm, c.window,
		c.step, c.ceilFrom, c.ceilMax, c.queueN, c.queueD, c.drain, c.commit,
		c.phaseNs, c.phaseRate, c.perPair, c.pinCPUs)
	facts := c.boxFacts()
	var rows []cmpRow
	// Deferred, so a case that fails the role still leaves the report of
	// every case before it: t.Fatalf runs this on its way out.
	defer func() { c.writeReport(t, facts, rows, log) }()
	c.matrix(t, log, &rows)
	failed := 0
	for _, r := range rows {
		if r.caseFailed {
			failed++
		}
	}
	if failed > 0 {
		t.Errorf("%d case(s) failed; the report lists each with its reason", failed)
	}
}

// ------------------------------------------------------------------ ACL check

// aclResult is one broker's answer to the ACL check: what it was asked to
// refuse, what it delivered anyway, and whether the allowed traffic that
// makes "nothing arrived" mean something did arrive.
type aclResult struct {
	attempts    int      // forbidden subscribes and publishes the broker answered
	leaked      int      // deliveries that crossed the ACL
	leaks       []string // what crossed, for the failure message
	allowed     int      // authorised messages that reached their subscriber
	wantAllowed int      // ... and how many were sent
	err         error    // the check itself could not run
}

func (r aclResult) denied() int { return r.attempts - min(r.attempts, r.leaked) }

func (r aclResult) String() string {
	return fmt.Sprintf("acl_denied=%d acl_leaked=%d acl_allowed=%d/%d", r.denied(), r.leaked, r.allowed, r.wantAllowed)
}

// verdict fails a check that did not run, that ran nothing forbidden (a
// count of 0 attempts is not a pass), whose allowed traffic did not flow (so
// silence proves nothing), or that saw anything cross.
func (r aclResult) verdict() error {
	switch {
	case r.err != nil:
		return fmt.Errorf("the check could not run: %w", r.err)
	case r.attempts == 0:
		return fmt.Errorf("the check made 0 forbidden attempts")
	case r.wantAllowed == 0 || r.allowed != r.wantAllowed:
		return fmt.Errorf("only %d of %d authorised messages were delivered, so nothing can be concluded from silence", r.allowed, r.wantAllowed)
	case r.leaked > 0:
		return fmt.Errorf("%d delivery(ies) crossed the ACL: %q", r.leaked, r.leaks)
	}
	return nil
}

// aclProbe is the same check for every broker. Client h2h-pub-1 holds a
// write grant on its own topics only. It subscribes to h2h-sub-0's topic and
// to wildcards, and publishes to it at QoS 0 and 1, while h2h-pub-0
// publishes to that same topic at QoS 0 and 1 as its ACL allows. The
// oracle is delivery, never a SUBACK code (Mosquitto 2.0 filters delivery
// and grants the SUBACK): h2h-pub-1 must receive nothing, and h2h-sub-0
// must receive exactly the two authorised messages and not the forged ones.
func (c cmpCfg) aclProbe(b *cmpBrokerDef) (res aclResult) {
	fail := func(format string, a ...any) aclResult { res.err = fmt.Errorf(format, a...); return res }
	if c.pairs < 2 {
		return fail("needs 2 pairs, has %d", c.pairs)
	}
	topic := c.broadcastTopic(0)
	var mu sync.Mutex
	var atSub, atAttacker []string
	record := func(dst *[]string) func(paho.PublishReceived) {
		return func(pr paho.PublishReceived) {
			mu.Lock()
			*dst = append(*dst, string(pr.Packet.Payload))
			mu.Unlock()
		}
	}
	sub, err := dial(c.mqtt("h2h-sub-0"), c.clientTLS, "h2h-sub-0", true, 0, record(&atSub), nil, 20)
	if err != nil {
		return fail("h2h-sub-0: %w", err)
	}
	pub, err := dial(c.mqtt("h2h-pub-0"), c.clientTLS, "h2h-pub-0", true, 0, nil, nil, 20)
	if err != nil {
		disconnectAll(nil, []*paho.Client{sub})
		return fail("h2h-pub-0: %w", err)
	}
	att, err := dial(c.mqtt("h2h-pub-1"), c.clientTLS, "h2h-pub-1", true, 0, record(&atAttacker), nil, 20)
	if err != nil {
		disconnectAll(nil, []*paho.Client{sub, pub})
		return fail("h2h-pub-1: %w", err)
	}
	defer disconnectAll(nil, []*paho.Client{sub, pub, att})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sa, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}})
	if err != nil || len(sa.Reasons) != 1 || sa.Reasons[0] != 1 {
		return fail("h2h-sub-0 was not granted its own topic: SUBACK=%v err=%v", sa, err)
	}
	for _, filter := range []string{topic, "h2h/b/#", "h2h/#"} {
		sa, err := att.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}})
		if sa == nil || len(sa.Reasons) != 1 {
			return fail("no SUBACK for h2h-pub-1's forbidden filter %q: err=%v", filter, err)
		}
		res.attempts++ // the answer is not the oracle: delivery below is
	}
	for _, qos := range []byte{0, 1} {
		ack, err := att.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: []byte(fmt.Sprintf("forged-qos%d", qos))})
		if qos == 1 && ack == nil && err != nil {
			return fail("no PUBACK for h2h-pub-1's forbidden QoS 1 publish: %v", err)
		}
		if qos == 0 && err != nil {
			return fail("h2h-pub-1's forbidden QoS 0 publish did not go out: %v", err)
		}
		res.attempts++
	}
	// A forged publish that was going to cross has crossed by now.
	time.Sleep(500 * time.Millisecond)
	for _, qos := range []byte{0, 1} {
		res.wantAllowed++
		ack, err := pub.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: []byte(fmt.Sprintf("allowed-qos%d", qos))})
		if err != nil || (ack != nil && ack.ReasonCode >= 0x80) {
			return fail("h2h-pub-0's authorised QoS %d publish was refused: PUBACK=%v err=%v", qos, ack, err)
		}
	}
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		n := 0
		for _, p := range atSub {
			if strings.HasPrefix(p, "allowed-") {
				n++
			}
		}
		mu.Unlock()
		if n >= res.wantAllowed {
			break
		}
	}
	time.Sleep(300 * time.Millisecond) // and anything else that was going to arrive
	mu.Lock()
	defer mu.Unlock()
	for _, p := range atSub {
		if strings.HasPrefix(p, "allowed-") {
			res.allowed++
		} else {
			res.leaked++
			res.leaks = append(res.leaks, "to h2h-sub-0: "+p)
		}
	}
	for _, p := range atAttacker {
		res.leaked++
		res.leaks = append(res.leaks, "to h2h-pub-1: "+p)
	}
	return res
}
