package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/store"
)

// **A configuration is not valid because its schema is.** RFC 0002 promises
// `--check-config` exits non-zero on any finding so that it can be a
// systemd `ExecStartPre`, and every file the security schema names was
// unchecked: the pre-check passed and the unit then died at startup, which
// for a restart is the worst moment available.
//
// Each case here is a file the broker opens before a listener binds, so
// each is a startup failure that `--check-config` used to call ok.
func TestEveryFileTheConfigurationNamesIsOpened(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "nowhere.pem")

	// A real password file and a real certificate, so that a case fails for
	// the one reason it is about.
	goodPasswd := filepath.Join(dir, "good.passwd")
	pf := passwd.New(goodPasswd)
	if err := pf.Set("alice", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	emptyPasswd := filepath.Join(dir, "empty.passwd")
	if err := os.WriteFile(emptyPasswd, []byte("# nobody\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	argon := filepath.Join(dir, "argon.passwd")
	if err := os.WriteFile(argon,
		[]byte("erin:$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	notPEM := filepath.Join(dir, "notpem.pem")
	if err := os.WriteFile(notPEM, []byte("this is not a certificate\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, tc := range []struct {
		name string
		file *config.File
		want string
	}{
		{"a client password file that is not there",
			withMQTT(&config.MQTT{PasswordFile: missing}), "broker.mqtt.password_file"},
		{"a client password file saguin cannot read",
			withMQTT(&config.MQTT{PasswordFile: argon}), "argon2id"},
		{"a client password file naming nobody, with anonymous refused",
			withMQTT(&config.MQTT{PasswordFile: emptyPasswd}), "names no users"},
		{"a certificate that is not there",
			withMQTT(&config.MQTT{Listen: config.Listen{TCP: []config.TCPDoor{{Name: "tcp", Address: config.Address{
				Address: ":1883", TLS: &config.TLS{CertFile: missing, KeyFile: missing}}}}}}),
			"broker.mqtt.listen.tcp.tls"},
		{"a client authority that is not a certificate",
			withMQTT(&config.MQTT{Listen: config.Listen{TCP: []config.TCPDoor{{Name: "tcp", Address: config.Address{
				Address: ":1883", TLS: &config.TLS{
					CertFile: testCert(t, dir), KeyFile: testKey(t, dir), ClientCAFile: notPEM}}}}}}),
			"holds no PEM certificates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			findings := unopenable(tc.file, nil)
			if len(findings) == 0 {
				t.Fatal("no finding: --check-config would answer ok and the broker would " +
					"then refuse to start, which is the failure this exists to stop")
			}
			joined := strings.Join(findings, "\n")
			if !strings.Contains(joined, tc.want) {
				t.Errorf("the finding does not carry %q:\n%s", tc.want, joined)
			}
		})
	}

	// The other half: a configuration whose files are all readable has no
	// findings, or the check would refuse every valid broker.
	t.Run("a configuration whose files are all there", func(t *testing.T) {
		f := withMQTT(&config.MQTT{PasswordFile: goodPasswd})
		if findings := unopenable(f, nil); len(findings) != 0 {
			t.Errorf("a readable configuration was refused:\n%s", strings.Join(findings, "\n"))
		}
	})

	// An empty password file is fine when something else admits people.
	t.Run("no users, but anonymous connections are allowed", func(t *testing.T) {
		yes := true
		f := withMQTT(&config.MQTT{PasswordFile: emptyPasswd, AllowAnonymous: &yes})
		if findings := unopenable(f, nil); len(findings) != 0 {
			t.Errorf("refused a file naming nobody beside allow_anonymous: true:\n%s",
				strings.Join(findings, "\n"))
		}
	})
}

// A bridge's own files are opened here too, and for the reason every other
// one is: RFC 0002 says a certificate saguin cannot use stops the broker, so
// a `--check-config` that called it ok would gate a deploy on nothing.
//
// The certificate is the interesting case rather than a missing file: a
// certificate and a key that do not belong together both exist, both parse,
// and only loading them as a pair says so.
func TestABridgesOwnFilesAreOpened(t *testing.T) {
	dir := t.TempDir()
	pair := writePair(t, dir)
	other := writePair(t, t.TempDir())

	cfgWith := func(t *testing.T, cert, key string) *config.File {
		t.Helper()
		path := filepath.Join(dir, "saguin.yaml")
		if err := os.WriteFile(path, []byte(`
broker:
  id: t
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
channels:
  events:
    type: append
bridges:
  head-office:
    peer: tls://h:8883
    client_id: v
    cert_file: `+cert+`
    key_file: `+key+`
    topics:
      - filter: fleet/#
        topic: $#
        direction: in
`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		f, _, err := config.Load(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		return f
	}

	t.Run("a key that is not the certificate's", func(t *testing.T) {
		findings := unopenable(cfgWith(t, pair[0], other[1]), nil)
		if len(findings) == 0 {
			t.Fatal("a mismatched pair was reported ok: the broker will not start, and the " +
				"check exists so that a deploy finds that out first")
		}
		joined := strings.Join(findings, "\n")
		if !strings.Contains(joined, "bridges.head-office") {
			t.Errorf("the finding does not name the bridge:\n%s", joined)
		}
	})

	t.Run("a pair that belongs together", func(t *testing.T) {
		if findings := unopenable(cfgWith(t, pair[0], pair[1]), nil); len(findings) != 0 {
			t.Errorf("a usable pair was refused:\n%s", strings.Join(findings, "\n"))
		}
	})
}

func withMQTT(m *config.MQTT) *config.File {
	f := &config.File{}
	f.Broker.MQTT = *m
	return f
}

// testCert and testKey write a usable pair once per directory, so a case
// about a client authority does not fail on the certificate beside it.
func testCert(t *testing.T, dir string) string { return writePair(t, dir)[0] }
func testKey(t *testing.T, dir string) string  { return writePair(t, dir)[1] }

// writePair is a self-signed certificate and its key, generated once per
// directory. Generated rather than committed: a certificate in the
// repository expires, and the day it does every test using it fails for a
// reason that has nothing to do with the code.
func writePair(t *testing.T, dir string) [2]string {
	t.Helper()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if _, err := os.Stat(certFile); err == nil {
		return [2]string{certFile, keyFile}
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	if err := os.WriteFile(certFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(keyFile,
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return [2]string{certFile, keyFile}
}

// **A dropped `+` level is reported and does not stop anything.**
//
// It was an open question: an unused `$1` is
// legal and silent, and it collapses topics by accident with nothing said.
// Refusing it was the other option and it is wrong - filter `fleet/+/+/temp`
// with topic `temp/$2` drops the first level deliberately, and there is no
// other way to write it, so a refusal would refuse a real configuration the
// broker cannot tell from a typo.
//
// So it is a note in `--check-config`, where an operator reads a
// configuration back before deploying it, and the exit code does not move.
func TestCheckConfigNotesARuleThatDropsAWildcard(t *testing.T) {
	dir := t.TempDir()
	load := func(t *testing.T, rules string) (*config.File, *channel.Registry) {
		t.Helper()
		path := filepath.Join(dir, "saguin.yaml")
		body := `broker:
  id: t
` + memStorage + `channels:
  events:
    type: append
  state:
    type: latest
bridges:
  head-office:
    peer: tcp://127.0.0.1:1883
    client_id: vessel-07
    topics:
` + rules
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		f, reg, err := config.Load(path)
		if err != nil {
			t.Fatalf("the test's own configuration does not load: %v", err)
		}
		return f, reg
	}

	t.Run("a template using every wildcard is silent", func(t *testing.T) {
		f, reg := load(t, "      - filter: fleet/+/telemetry/#\n"+
			"        topic: telemetry/$1/$#\n        direction: in\n")
		if got := collapsingRules(f, reg); len(got) != 0 {
			t.Errorf("a rule that drops nothing was noted anyway: %v", got)
		}
	})

	t.Run("a dropped level on an append channel is noted", func(t *testing.T) {
		f, reg := load(t, "      - filter: fleet/+/+/temp\n"+
			"        topic: events/temp/$2\n        direction: in\n")
		got := collapsingRules(f, reg)
		if len(got) != 1 {
			t.Fatalf("want one note, got %d: %v", len(got), got)
		}
		if !strings.Contains(got[0], "$1") {
			t.Errorf("the note does not name which level was dropped: %q", got[0])
		}
		if strings.Contains(got[0], "latest") {
			t.Errorf("an append channel was described as a latest one: %q", got[0])
		}
	})

	// The distinction worth having: on `latest` the collapse is not a mess
	// but a loss, and the channel is doing its job while it happens.
	t.Run("a dropped level into a latest channel says it overwrites", func(t *testing.T) {
		f, reg := load(t, "      - filter: site/+/+/level\n"+
			"        topic: state/$2\n        direction: in\n")
		got := collapsingRules(f, reg)
		if len(got) != 1 {
			t.Fatalf("want one note, got %d: %v", len(got), got)
		}
		if !strings.Contains(got[0], "overwriting") {
			t.Errorf("the note does not say the value is overwritten, which is what "+
				"makes this one worth reading: %q", got[0])
		}
	})

	// **An outbound rule gets the plain sentence, whatever this broker's
	// channels are.** The sharper one is about what the destination does
	// with two topics that collapsed into one, and an `out` rule's
	// destination is the peer - so reading it off a local `latest` channel
	// is an answer to a question about somebody else's broker. Here the
	// filter reads `state`, a `latest` channel, and the note must still not
	// claim a value is overwritten: what the peer keeps under
	// `mirror/…` is not knowable from this file.
	t.Run("an outbound rule is not sharpened by this broker's channels", func(t *testing.T) {
		f, reg := load(t, "      - filter: state/+/+/level\n"+
			"        topic: mirror/$2\n        direction: out\n")
		got := collapsingRules(f, reg)
		if len(got) != 1 {
			t.Fatalf("want one note, got %d: %v", len(got), got)
		}
		if strings.Contains(got[0], "overwriting") {
			t.Errorf("an outbound rule was described by what a local latest channel "+
				"would do, which is a statement about the peer this file cannot make: %q",
				got[0])
		}
	})
}

// **And the note reaches standard output**, which the test above cannot
// say: it calls the function, and a function nothing calls passes it just
// as well. That gap - a rule computed and never wired - is the one this
// suite has now found five times, so the wiring gets its own run of the
// real binary.
func TestTheBinaryPrintsTheDroppedWildcardNote(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}

	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(`broker:
  id: t
`+memStorage+`channels:
  events:
    type: append
bridges:
  head-office:
    peer: tcp://127.0.0.1:1883
    client_id: vessel-07
    topics:
      - filter: fleet/+/+/temp
        topic: temp/$2
        direction: in
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	out, err := exec.Command(bin, "--check-config", cfg).CombinedOutput()
	said := string(out)
	// **Zero, and that is half the point.** A note is not a finding: this
	// configuration works, and a deploy gating on the exit code must not
	// start failing because somebody dropped a level on purpose.
	if err != nil {
		t.Errorf("--check-config exited non-zero over a note: %v\n%s", err, said)
	}
	if !strings.Contains(said, ": ok") {
		t.Errorf("the configuration was not reported ok:\n%s", said)
	}
	if !strings.Contains(said, "note:") || !strings.Contains(said, "$1") {
		t.Errorf("the dropped level was computed and never printed, so nobody is told:\n%s",
			said)
	}
}

// **The channel summary is a thing people diff, so it has to be the same
// twice.**
//
// It is what an `ExecStartPre` or a CI step captures before a deploy, and
// it was walked straight from a map - so two runs over a file nobody had
// touched came out in different orders, and a `diff` meant to notice a
// change reported one that had not happened. No document was wrong: RFC
// 0002 promises no order for it. It was noise where the whole point is
// signal.
//
// **Two assertions, because they are different claims.** Sorted is the
// mechanism and is checked in one run, deterministically. Identical across
// runs is what an operator actually relies on, and it would catch a second
// source of disorder that sorting by name never touched. Neither alone is
// enough: two runs can agree by luck, and "sorted" is only the answer while
// sorting is what stability rests on.
//
// The configuration below carries five channels - four written and the
// `__dlq` a queue derives - because a summary of one is in order whatever
// the code does.
func TestTheCheckConfigSummaryIsTheSameTwice(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	cfg := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(cfg, []byte(`broker:
  id: t
`+memStorage+`channels:
  state:
    type: latest
  events:
    type: append
  presence:
    type: latest
  jobs:
    type: queue
    visibility_timeout: 30s
    retry:
      max_attempts: 3
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Names in the order the summary printed them, from the indented lines
	// under the `ok`. A `note:` line is indented too and is not a channel.
	run := func() (string, []string) {
		t.Helper()
		out, err := exec.Command(bin, "--check-config", cfg).CombinedOutput()
		if err != nil {
			t.Fatalf("--check-config: %v\n%s", err, out)
		}
		var names []string
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "  note:") {
				continue
			}
			names = append(names, strings.Fields(line)[0])
		}
		return string(out), names
	}

	first, names := run()
	if len(names) != 5 {
		t.Fatalf("read %d channels from the summary, want 5 - four written and the "+
			"__dlq a queue derives. A summary this check cannot see is one it is not "+
			"checking:\n%s", len(names), first)
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("the summary is not in name order, so two runs over one file can "+
			"differ: %v", names)
	}
	// `jobs__dlq` directly after `jobs` falls out of sorting by name for any
	// ordinary channel name, and is the arrangement worth having: the
	// derived channel reads beside the queue that derives it.
	for i, n := range names {
		if n == "jobs" && (i+1 >= len(names) || names[i+1] != "jobs__dlq") {
			t.Errorf("a queue's derived __dlq is not printed beside it: %v", names)
		}
	}

	// Enough runs that agreeing by chance is not the reason. Five channels
	// walked from a map have 120 orders; ten runs agreeing by luck is not
	// something to explain away.
	for i := 0; i < 9; i++ {
		again, _ := run()
		if again != first {
			t.Fatalf("two runs over an untouched configuration differ, which is a diff "+
				"reporting a change nobody made:\n--- run 1\n%s--- run %d\n%s",
				first, i+2, again)
		}
	}
}

// **The constant and the changelog cannot disagree.**
//
// Version is written down, which CHANGELOG.md's own rule makes checkable:
// "A section here means a tag on GitHub." So a release number the changelog
// does not name is a claim about a tag nobody cut, and a changelog entry the
// constant has not caught up with is a binary that will report the wrong
// release for as long as nobody notices.
//
// This is the whole reason a constant is allowed here at all. The version
// used to be read from the build precisely so that it could not be
// forgotten; writing it down is only safe while forgetting fails.
//
// The rule holds in both states, which is what stops it needing to be
// rewritten the day the first release is cut:
//
//   - no release section - Version must carry a pre-release suffix. Nothing
//     is tagged, so nothing may claim to be tagged.
//   - a release section - Version's release number must equal the newest.
//
// Headings are matched as Keep a Changelog writes them, which is the format
// the document says it follows: `## [1.2.3] - 2026-08-24`, with the
// brackets and a leading v both optional. `## [Unreleased]` is not a
// release and does not match.
func TestTheVersionAgreesWithTheChangelog(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "CHANGELOG.md"))
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	// The counter this shape owes: a file that came back empty would agree
	// with anything at all.
	if len(md) < 200 {
		t.Fatalf("CHANGELOG.md is %d bytes, which is too short to be the changelog: "+
			"this test read something else and every check below would pass by vacuum",
			len(md))
	}

	heading := regexp.MustCompile(`(?m)^##\s+\[?v?(\d+\.\d+\.\d+)\]?`)
	var released []string
	for _, m := range heading.FindAllStringSubmatch(string(md), -1) {
		released = append(released, m[1])
	}

	// The release number this binary claims, with any pre-release suffix and
	// any build metadata taken off: 0.1.0-dev and 0.1.0+abc123 are both
	// claims about the release 0.1.0.
	claimed := Version
	if i := strings.IndexAny(claimed, "-+"); i >= 0 {
		claimed = claimed[:i]
	}
	prerelease := strings.Contains(Version, "-")

	// The reassuring line is only ever printed when there is something to be
	// reassured about. A failure that prints "as it should be" underneath
	// itself is a report arguing with its own first line.
	if len(released) == 0 {
		if !prerelease {
			t.Fatalf("Version is %q and CHANGELOG.md names no release at all. Its rule is "+
				"that a section means a tag on GitHub, so this binary claims a release "+
				"nobody cut - give it a suffix such as %q, or write the section and tag it",
				Version, claimed+"-dev")
		}
		t.Logf("Version %q, no release in CHANGELOG.md yet - a pre-release, as it should be",
			Version)
		return
	}

	newest := released[0]
	if claimed != newest {
		t.Fatalf("Version is %q, so it claims release %s, and the newest section in "+
			"CHANGELOG.md is %s. One of the two was updated and the other was not, and "+
			"the binary is what an operator believes", Version, claimed, newest)
	}
	if prerelease {
		t.Logf("Version %q is a pre-release of %s, which CHANGELOG.md names", Version, claimed)
	} else {
		t.Logf("Version %q is the release CHANGELOG.md names newest", Version)
	}
}

// **Every channel a sqlite provider holds is given a store, and no other
// channel is.** The wiring is one loop in main, and nothing called it from a
// test until now - while its twin in the end-to-end harness spent the whole
// life of the disaster-recovery feature naming three channels instead of
// walking the registry. A copy channel was never one of the three, so it was
// given no store, kept the memory one the broker builds for every channel,
// and passed every test while holding its records where a restart loses
// them.
//
// **The fallback is deliberate and stays.** A configuration with no storage
// block is a real one, and memory is exactly what it means. What makes a
// missed channel dangerous is that it is silent: nothing refuses it, and the
// per-channel metric reports the provider the *configuration* names, so a
// channel that asked for sqlite and got memory reports itself as being on
// disk. A wiring that quietly holds a durable channel in memory is the shape
// this project ranks worst - it reports success and loses the data anyway.
//
// The oracle is the configuration rather than the loop: what belongs in the
// database is every channel the registry says names that provider, which
// includes the dead-letter channel a queue derives and no file ever writes.
// The observation is the database itself, listed back.
func TestEveryChannelOnASQLiteProviderIsGivenAStore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "saguin.db")
	cfgPath := filepath.Join(dir, "saguin.yaml")
	// Both providers, because "every channel got a store" is half the rule:
	// a loop that opened a store for everything would pass that half and put
	// a memory channel in the database.
	//
	// A copy channel is here on purpose. It is the one the harness could not
	// reach, it is reachable from no publish, and its name is not one
	// anybody would think to write into a list.
	body := fmt.Sprintf(`broker:
  id: wiring
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
      mem:
        type: memory
        snapshot_dir: none
channels:
  events:
    type: append
    storage: disk
  state:
    type: latest
    storage: disk
  jobs:
    type: queue
    storage: disk
  scratch:
    type: append
    storage: mem
  ephemeral:
    type: latest
    storage: mem
bridges:
  dr-link:
    peer: tcp://127.0.0.1:1883
    client_id: dr-link
    topics:
      - filter: upstream/#
        topic: scratch/$#
        direction: in
`, dbPath)
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	cfg, reg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	limits := cfg.Broker.Limits.Resolve()
	_, b, err := broker.NewServer(reg, limits, log)
	if err != nil {
		t.Fatalf("build the server: %v", err)
	}
	defer b.Stop()

	// The production wiring, called as main calls it.
	dbs, err := openDatabases(cfg, reg, b, log)
	if err != nil {
		t.Fatalf("open the databases: %v", err)
	}
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	db := dbs["disk"]
	if db == nil {
		t.Fatal("the sqlite provider was never opened, so nothing below asserts anything")
	}

	// What the configuration says belongs there, kinds included, so a queue
	// wired as a log is a failure rather than a name that happens to match.
	want := map[string]store.Kind{}
	for name, c := range reg.All() {
		if c.Storage != "disk" {
			continue
		}
		switch c.Type {
		case channel.Append:
			want[name] = store.KindAppend
		case channel.Latest:
			want[name] = store.KindLatest
		case channel.Queue:
			want[name] = store.KindQueue
		}
	}
	// Counted rather than assumed: three channels are written on this
	// provider and a queue derives a fourth, and an oracle that had quietly
	// come back empty would agree with a wiring that stored nothing.
	if len(want) != 4 {
		t.Fatalf("the oracle holds %d channels, want 4: %v", len(want), want)
	}
	if _, ok := want["jobs"+channel.DLQSuffix]; !ok {
		t.Fatalf("the oracle does not hold the dead-letter channel a queue derives, so the "+
			"one channel no configuration writes is not being asked about: %v", want)
	}

	states, err := db.Export()
	if err != nil {
		t.Fatalf("list the database's channels: %v", err)
	}
	got := map[string]store.Kind{}
	for _, cs := range states {
		got[cs.Name] = cs.Kind
	}

	for name, kind := range want {
		switch held, ok := got[name]; {
		case !ok:
			t.Errorf("channel %q names the sqlite provider and has no store in it: it keeps "+
				"the memory store every channel starts with, so it holds its records where "+
				"a restart loses them and nothing anywhere says so", name)
		case held != kind:
			t.Errorf("channel %q is in the database as kind %v, want %v", name, held, kind)
		}
	}
	// The other half. A memory channel given a database is not a silent
	// failure - it is durable where the operator asked for volatile, and the
	// bytes are on a disk they did not size for it.
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("the database holds a store for %q, which names no sqlite provider", name)
		}
	}
}

// RFC 0002 "How a sqlite provider reads": **what read_connections says is
// what the provider opens**, through the production wiring. A provider set
// to 0 must open no read connection at all - a small box said it cannot
// spare one - and one set to 3 must open up to three. The observation is the
// provider's own count, the value the startup line reports; the oracle is
// the file.
func TestReadConnectionsReachTheProvider(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "saguin.yaml")
	body := fmt.Sprintf(`broker:
  id: wiring
  storage:
    default: small
    default_retention_period: none
    default_retention_bytes: none
    providers:
      small:
        type: sqlite
        file_path: %s
        read_connections: 0
      busy:
        type: sqlite
        file_path: %s
        read_connections: 3
      plain:
        type: sqlite
        file_path: %s
channels:
  a:
    type: append
    storage: small
  b:
    type: append
    storage: busy
  c:
    type: append
    storage: plain
`, filepath.Join(dir, "small.db"), filepath.Join(dir, "busy.db"), filepath.Join(dir, "plain.db"))
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	cfg, reg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, b, err := broker.NewServer(reg, cfg.Broker.Limits.Resolve(), log)
	if err != nil {
		t.Fatalf("build the server: %v", err)
	}
	defer b.Stop()
	dbs, err := openDatabases(cfg, reg, b, log)
	if err != nil {
		t.Fatalf("open the databases: %v", err)
	}
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	for provider, want := range map[string]int{"small": 0, "busy": 3, "plain": store.SQLiteReadConnections} {
		db := dbs[provider]
		if db == nil {
			t.Fatalf("provider %s was never opened, so nothing is asked of it", provider)
		}
		if got := db.ReadConnections(); got != want {
			t.Errorf("provider %s opened %d read connections, want %d: the file's read_connections "+
				"did not reach it", provider, got, want)
		}
	}
}
