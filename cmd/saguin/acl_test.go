package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
)

// The tool exists because roles cost indirection: with a flat list "why can
// device-7 not publish?" is a line in a file, and with roles it is a
// pattern match and two lookups done in somebody's head. These assert the
// three answers an operator actually needs.
func TestACLExplain(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  publisher:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
  worker:
    - channel: jobs
      allow: [consume]
    - topic: alerts/%u/#
      allow: [read]
users:
  "vessel-*": [publisher, worker]
  dashboard:  [publisher]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}

	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
  - jobs:
      type: queue
      visibility_timeout: 30s
      retry:
        max_attempts: 3
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	run := func(id string) string {
		// Two buffers, and the refusal one is asserted empty: an
		// explanation that succeeded must say nothing on the stream a
		// script reads for failures.
		var out, bad bytes.Buffer
		if code := explainACL([]string{cfgPath, id}, nil, &out, &bad, false, asColumns); code != 0 {
			t.Fatalf("explain %s: exit %d\n%s%s", id, code, out.String(), bad.String())
		}
		if bad.Len() != 0 {
			t.Errorf("explaining %s succeeded but wrote %q to the refusal stream",
				id, bad.String())
		}
		return out.String()
	}

	t.Run("the roles a pattern reaches, and what they grant", func(t *testing.T) {
		got := run("vessel-7")
		for _, want := range []string{
			"vessel-*",
			"publisher, worker",
			// %u is the client's own identity, which is the whole reason a
			// suffix is worth printing resolved rather than as written.
			"channel events matching events/telemetry/vessel-7/#",
			"channel jobs",
			"consume",
			// A topic rule takes the same substitution, and this is where
			// the defect was visible before it was fixed: the tool printed
			// the rule with the literal %u in it, which reads as a grant
			// that is present when it is one that can never match.
			"topic alerts/vessel-7/#",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("the explanation does not carry %q:\n%s", want, got)
			}
		}
		if strings.Contains(got, "%u") {
			t.Errorf("a rule was printed as written rather than resolved, which is "+
				"the one thing an operator cannot work out by reading the file:\n%s", got)
		}
	})

	// The commonest real question: a device that is refused everything is
	// usually looking at a pattern that did not match.
	t.Run("a user no pattern reaches", func(t *testing.T) {
		got := run("laptop")
		if !strings.Contains(got, "no user pattern matches") {
			t.Errorf("the explanation does not say the name matched nothing:\n%s", got)
		}
		for _, want := range []string{"dashboard", "vessel-*"} {
			if !strings.Contains(got, want) {
				t.Errorf("it does not list the patterns there are, so there is nothing "+
					"to compare the id against: %q missing from\n%s", want, got)
			}
		}
	})

	t.Run("a configuration with no acl_file", func(t *testing.T) {
		plain := filepath.Join(dir, "plain.yaml")
		body, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		cleaned := strings.ReplaceAll(string(body), "    acl_file: "+aclPath+"\n", "")
		if err := os.WriteFile(plain, []byte(cleaned), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		var out, bad bytes.Buffer
		if code := explainACL([]string{plain, "vessel-7"}, nil, &out, &bad, false, asColumns); code != 0 {
			t.Fatalf("exit %d: %s%s", code, out.String(), bad.String())
		}
		if bad.Len() != 0 {
			t.Errorf("a configuration with no acl_file is an answer, not a refusal, but "+
				"%q went to the refusal stream", bad.String())
		}
		if !strings.Contains(out.String(), "may do anything") {
			t.Errorf("a broker with no acl_file was not said to allow everything, which "+
				"is what it does:\n%s", out.String())
		}
	})
}

// **--check-config opens the acl_file.** It answered `ok` for one that did
// not exist, and for one whose every rule was impossible - while the real
// startup refused both, correctly. README says --check-config "validates
// the lot without opening a socket", and the same check already opens the
// password file and the TLS certificates; the acl_file was the one startup
// file the feature did not add to it.
//
// The failure that matters is a deploy pipeline gating on --check-config
// and shipping a broker that will not come up.
// **A shadowed entry is the one thing an acl_file can now do silently**, so
// `--acl` has to say it out loud.
//
// One entry applies - the pattern that spells the id out most exactly - and
// every other matching entry does nothing. Nothing can refuse that at
// startup, because it is the intended behaviour and it is what makes an
// exception expressible at all. From inside the file a pattern that matched
// and lost looks exactly like one that applied, so this command is the only
// place the difference can be seen.
//
// **The assertion that matters is the last one.** Labelling a line
// "shadowed" would pass a test that only read the labels while the roles
// were still being unioned underneath; what proves the shadowing is real is
// that the shadowed entry's second role is absent from the grants.
func TestACLNamesTheEntryThatAppliesAndTheOnesItShadows(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  device:
    - topic: alerts/%u/#
      allow: [read]
  worker:
    - channel: jobs
      allow: [consume]
users:
  "device-*":
    roles: [device, worker]
    limits:
      publish_rate: 20
      publish_bytes: 8KiB
  "device-7": [device]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - jobs:
      type: queue
      visibility_timeout: 30s
      retry:
        max_attempts: 3
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var out, bad bytes.Buffer
	if code := explainACL([]string{cfgPath, "device-7"}, nil, &out, &bad, false, asColumns); code != 0 {
		t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
	}
	got := out.String()

	for _, want := range []string{
		"device-7", "applies",
		"device-*", `shadowed by "device-7"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the explanation does not say %q, so which entry is in force cannot "+
				"be read off it:\n%s", want, got)
		}
	}

	// The entry in force comes first, so a reader who stops at the first
	// line has the answer rather than whichever pattern sorts earliest.
	if strings.Index(got, "device-7 ") > strings.Index(got, "device-* ") {
		t.Errorf("the shadowed entry is printed above the one that applies:\n%s", got)
	}

	// The bound that precedence took away. A narrow entry written to add a
	// role, carrying no limits, takes the fleet's off the client it names -
	// and a lost bound shows up as a device allowed to flood the broker,
	// where a lost role shows up the moment the device is refused something.
	if !strings.Contains(got, "carries limits and matches this client") {
		t.Errorf("the fleet's limits were shadowed away and nothing said so:\n%s", got)
	}

	// And the half that proves the labels are not decoration: `worker` is
	// the shadowed entry's second role, and device-7 must not hold it.
	if strings.Contains(got, "consume") || strings.Contains(got, "jobs") {
		t.Errorf("device-7 holds a role only the shadowed entry names, so the entries "+
			"are still being unioned and the labels above are decoration:\n%s", got)
	}
}

// **What `%c` gives is said where somebody is reasoning about their own
// file**, because a grant reading `iot/cohort-north/health/north-17` looks
// like per-device isolation and is not.
//
// **The second half is what keeps it from being noise.** A note printed on
// every file is one nobody reads by the third time, so a file with no `%c`
// in it must not carry it - and that is the assertion that fails if the
// check is ever loosened to "this file has cohorts" or "a client id was
// given".
func TestACLSaysWhatClientIDScopingIsWorth(t *testing.T) {
	const note = "by mistake rather than by force"

	run := func(t *testing.T, rules string, args ...string) string {
		t.Helper()
		dir := t.TempDir()
		aclPath := filepath.Join(dir, "acl.yaml")
		if err := os.WriteFile(aclPath, []byte(rules), 0o600); err != nil {
			t.Fatalf("write acl: %v", err)
		}
		cfgPath := filepath.Join(dir, "saguin.yaml")
		if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		var out, bad bytes.Buffer
		if code := explainACL(append([]string{cfgPath}, args...), nil, &out, &bad, false, asColumns); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		return out.String()
	}

	scoped := `
roles:
  sensor:
    - topic: iot/%u/health/%c
      allow: [write, read]
users:
  "cohort-*": [sensor]
`
	t.Run("a rule using %c, resolved", func(t *testing.T) {
		if got := run(t, scoped, "cohort-north", "north-17"); !strings.Contains(got, note) {
			t.Errorf("a resolved %%c grant carried no note about what it is worth:\n%s", got)
		}
	})
	t.Run("the same rule, unresolved", func(t *testing.T) {
		if got := run(t, scoped, "cohort-north"); !strings.Contains(got, note) {
			t.Errorf("the note is missing when no client id was given, which is where an "+
				"operator is most likely to be reasoning about it:\n%s", got)
		}
	})
	t.Run("a file with no %c in it", func(t *testing.T) {
		got := run(t, `
roles:
  sensor:
    - topic: iot/%u/health/+
      allow: [write, read]
users:
  "cohort-*": [sensor]
`, "cohort-north")
		if strings.Contains(got, note) {
			t.Errorf("a file scoping nothing by client id was told what client id scoping "+
				"is worth, which is how a note stops being read:\n%s", got)
		}
	})
}

// A rule withheld for a name - one naming %c or %u, where the name holds
// +, # or / - is named by --acl with why, in words and in JSON, rather than
// left out of the list for an operator to puzzle over.
func TestACLNamesARuleWithheldForAName(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  sensor:
    - topic: iot/health/%c
      allow: [write, read]
users:
  "*": [sensor]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels: {}
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	run := func(t *testing.T, form outputForm, args ...string) string {
		t.Helper()
		var out, bad bytes.Buffer
		if code := explainACL(append([]string{cfgPath}, args...), nil, &out, &bad, false, form); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		return out.String()
	}

	got := run(t, asColumns, "device", "#")
	for _, want := range []string{"withheld:", "iot/health/%c", "+, # or /", "(none: every rule it matched is withheld above)"} {
		if !strings.Contains(got, want) {
			t.Errorf("--acl for client id \"#\" does not say %q:\n%s", want, got)
		}
	}
	if got := run(t, asColumns, "device", "north-17"); strings.Contains(got, "withheld:") {
		t.Errorf("--acl for an ordinary client id names a rule withheld:\n%s", got)
	}

	var e struct {
		Grants         []map[string]any `json:"grants"`
		GrantsWithheld []map[string]any `json:"grants_withheld"`
	}
	if err := json.Unmarshal([]byte(run(t, asJSON, "device", "#")), &e); err != nil {
		t.Fatalf("decode the JSON form: %v", err)
	}
	if len(e.Grants) != 0 || len(e.GrantsWithheld) != 1 {
		t.Errorf("the JSON form for client id \"#\" holds %d grants and %d withheld, want 0 and 1", len(e.Grants), len(e.GrantsWithheld))
	}
}

// **What a client is held to, answerable for a fleet on one screen.**
//
// The paragraph form named the two configuration keys and left an operator
// to go and read them, which made the question this command exists to
// answer - what is this device actually bounded by - a thing to work out
// somewhere else. Both figures are printed now, with where each came from,
// and the list form carries them as two columns so forty devices can be
// compared with a `cut`.
//
// **`no bound` in words, never `0` and never `-`.** A zero in a column of
// limits reads as a bound of zero, which is the one figure the
// configuration refuses because it would mean refusing every publish; and
// `-` in this format is defined to mean granted nothing, which is the
// reverse of the truth for a client nothing bounds.
func TestACLPrintsWhatAClientIsHeldTo(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  sensor:
    - topic: "iot/#"
      allow: [write, read]
users:
  "device-*":
    roles: [sensor]
    limits:
      publish_rate: 20
      publish_bytes: 8KiB
  "device-7":
    roles: [sensor]
    limits:
      publish_rate: 200
  plain: [sensor]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
  limits:
    publish_rate: 1000
    publish_bytes: 1MiB
channels:
  - events:
      type: append
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Run("the columns", func(t *testing.T) {
		var out, bad bytes.Buffer
		in := strings.NewReader("device-1\ndevice-7\nplain\nnobody\n")
		if code := explainACL([]string{cfgPath}, in, &out, &bad, true, asColumns); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		for _, want := range []string{
			"\trate\tbytes",            // the header names them
			"device-1\t", "\t20\t8192", // the fleet's figures
			"device-7\t", "\t200\tno bound", // its own entry, and what it left out
			"plain\t", "\t1000\t1048576", // no entry limits: the broker-wide pair
			"nobody\t", "\t1000\t1048576", // and a user no pattern matches takes them too
		} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("the columns do not carry %q:\n%s", want, out.String())
			}
		}
	})

	t.Run("device-7 is told what its own entry left out", func(t *testing.T) {
		var out, bad bytes.Buffer
		if code := explainACL([]string{cfgPath, "device-7"}, nil, &out, &bad, false, asColumns); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		got := out.String()
		if !strings.Contains(got, "200") || !strings.Contains(got, "no bound") {
			t.Errorf("both figures are not printed:\n%s", got)
		}
		if !strings.Contains(got, "unbounded rather than taken from the") {
			t.Errorf("an entry naming one figure silently unbounds the other and nothing "+
				"said so - which is the half an operator does not expect:\n%s", got)
		}
		if !strings.Contains(got, `the acl_file entry "device-7"`) {
			t.Errorf("the figures do not say where they came from, so 200 from an entry "+
				"and 200 broker-wide read alike:\n%s", got)
		}
	})

	t.Run("json carries null for no bound", func(t *testing.T) {
		var out, bad bytes.Buffer
		in := strings.NewReader("device-7\nplain\n")
		if code := explainACL([]string{cfgPath}, in, &out, &bad, true, asJSON); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		got := out.String()
		if !strings.Contains(got, `"publish_rate":200,"publish_bytes":null`) {
			t.Errorf("no bound is not null in JSON, so a consumer cannot tell it from a "+
				"figure:\n%s", got)
		}
		if !strings.Contains(got, `"publish_rate":1000,"publish_bytes":1048576`) {
			t.Errorf("the broker-wide pair is not carried:\n%s", got)
		}
		if strings.Contains(got, `"publish_rate":0`) || strings.Contains(got, `"publish_bytes":0`) {
			t.Errorf("a limit is written as 0, which reads as a bound of zero - the one "+
				"figure the configuration refuses:\n%s", got)
		}
	})
}

func TestCheckConfigOpensTheACLFile(t *testing.T) {
	dir := t.TempDir()
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}

	cfgWith := func(acl string) *config.File {
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: `+filepath.Join(dir, "clients.passwd")+`
    acl_file: `+acl+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		f, _, err := config.Load(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		return f
	}
	// A password file that exists, so the only finding can be the acl one.
	if err := os.WriteFile(filepath.Join(dir, "clients.passwd"),
		[]byte("device-7:$6$aaaaaaaaaaaaaaaa$"+
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==\n"),
		0o600); err != nil {
		t.Fatalf("write passwd: %v", err)
	}

	t.Run("a file that is not there", func(t *testing.T) {
		got := unopenable(cfgWith(filepath.Join(dir, "absent.yaml")), reg)
		if len(got) == 0 {
			t.Fatal("an acl_file naming no file was reported ok: the broker will not start")
		}
		joined := strings.Join(got, "\n")
		if !strings.Contains(joined, "acl_file") {
			t.Errorf("the finding does not name the key: %v", got)
		}
		// **Named once.** authz.Load wrapped its own read error as
		// `acl_file: …` while both callers already name the key, so this
		// read `broker.mqtt.acl_file: acl_file: open …` where the
		// password-file sibling reads clean.
		if n := strings.Count(joined, "acl_file:"); n != 1 {
			t.Errorf("the key is named %d times in one finding, want once: %s", n, joined)
		}
	})

	t.Run("a file whose rules cannot mean what they say", func(t *testing.T) {
		bad := filepath.Join(dir, "bad.yaml")
		if err := os.WriteFile(bad, []byte(
			"roles:\n  r:\n    - channel: nowhere\n      allow: [write]\n"+
				"    - topic: events/#\n      allow: [read]\nusers:\n  a: [r, ghost]\n"),
			0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		got := strings.Join(unopenable(cfgWith(bad), reg), "\n")
		// Every finding, not the first: an operator fixing this file one
		// error per run is restarting a broker to find the next one.
		for _, want := range []string{"not configured", "Write it as a channel rule",
			"does not define"} {
			if !strings.Contains(got, want) {
				t.Errorf("the check stops before %q: %s", want, got)
			}
		}
	})

	// **Held to limits.max_topic_levels, the configuration's own**: the file
	// is read against the bound the broker will run with (RFC 0002 "How
	// deep a topic may be"), so a rule deeper than any topic fails here.
	t.Run("a rule deeper than max_topic_levels", func(t *testing.T) {
		deep := filepath.Join(dir, "deep.yaml")
		if err := os.WriteFile(deep, []byte(
			"roles:\n  r:\n    - topic: alerts"+strings.Repeat("/x", 200)+"\n      allow: [read]\nusers:\n  a: [r]\n"),
			0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := strings.Join(unopenable(cfgWith(deep), reg), "\n"); !strings.Contains(got, "limits.max_topic_levels") {
			t.Errorf("an acl_file rule of 201 levels was not refused for its depth: %q", got)
		}
	})

	t.Run("a file that is right", func(t *testing.T) {
		good := filepath.Join(dir, "good.yaml")
		if err := os.WriteFile(good, []byte(
			"roles:\n  r:\n    - channel: events\n      allow: [write]\nusers:\n  a: [r]\n"),
			0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := unopenable(cfgWith(good), reg); len(got) != 0 {
			t.Errorf("a good acl_file was reported unusable: %v", got)
		}
	})
}

// **Batch mode: a list of client ids in, a line each out.**
//
// The case this exists for is a fleet. An operator asking "which of these
// forty devices may publish" against a command that prints a paragraph
// apiece is an operator reading forty screens, and the answer they want is
// one screen they can sort, cut and diff.
//
// Every row asserts the whole line rather than a substring, because the
// columns are the contract: a script cutting field 5 for the verbs breaks
// silently if a column is inserted before it, and a test matching "write"
// somewhere in the output would not notice.
func TestACLBatchAnswersAListOfClients(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  publisher:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
  watcher:
    - topic: alerts/%u/#
      allow: [read]
users:
  "vessel-*": [publisher]
  dashboard:  [watcher]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// A comment line and a blank one, because a file an operator keeps is
	// not a bare list; and a client no pattern matches, because "granted
	// nothing" and "I never asked about that one" must not be the same
	// output to somebody piping a fleet in.
	in := strings.NewReader("# the fleet\nvessel-7\n\ndashboard\nnobody-at-all\n")
	var out, bad bytes.Buffer
	if code := explainACL([]string{cfgPath}, in, &out, &bad, true, asColumns); code != 0 {
		t.Fatalf("batch exited %d: %s", code, bad.String())
	}
	if bad.Len() != 0 {
		t.Errorf("a batch run that succeeded wrote to the refusal stream: %q", bad.String())
	}

	want := []string{
		// The header, first and always: the reader of this form is a person
		// with a screen of columns, and without it they are counting tabs to
		// find out which one is the role.
		"# user\trole\tkind\tsubject\tverbs\trate\tbytes",
		// `%u` already substituted: the substituted form is what the broker
		// compares against, and the unsubstituted one an operator can read
		// off the file for themselves.
		"vessel-7\tpublisher\tchannel\tevents (events/telemetry/vessel-7/#)\twrite\tno bound\tno bound",
		"dashboard\twatcher\ttopic\talerts/dashboard/#\tread\tno bound\tno bound",
		"nobody-at-all\t-\t-\t-\t-\tno bound\tno bound",
	}
	got := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(got), len(want), out.String())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d:\n  got  %q\n  want %q", i+1, got[i], want[i])
		}
	}
}

// A command whose argument was forgotten must say so rather than sit there
// reading a terminal. A run that appears to have frozen is the worst answer
// to a typo: the operator kills it and is left wondering what it did.
func TestACLWithNoClientAndNoPipeRefusesRatherThanWaiting(t *testing.T) {
	// A configuration that loads, and an acl_file, so the run reaches the
	// guard rather than stopping at a problem before it. A missing file
	// refuses for its own reason and would have proved nothing here.
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte("roles:\n  r:\n    - topic: \"a/#\"\n      allow: [read]\nusers:\n  x: [r]\n"), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var out, bad bytes.Buffer
	if code := explainACL([]string{cfgPath}, nil, &out, &bad, false, asColumns); code == 0 {
		t.Fatal("a batch run with nothing piped in was accepted, so it would have read a terminal")
	}
	if !strings.Contains(bad.String(), "nothing piped in") {
		t.Errorf("the refusal does not say why:\n%s", bad.String())
	}
	if out.Len() != 0 {
		t.Errorf("a refusal wrote to standard output: %q", out.String())
	}
}

// **A configuration naming no acl_file must answer in words, in both
// shapes.** Without one every authenticated client may do anything, and the
// row of `-` this command prints for a client no pattern matches means the
// exact opposite - granted nothing. Printing that row for a broker that
// grants everything is the harshest refusal the format has, given as the
// answer to the most permissive setting there is.
//
// It is not hypothetical. A doc sweep ran RFC 0002's own example against
// examples/saguin.yaml, read three rows of `-`, and filed a bug saying the
// no-acl_file case answered that way. It does not and has not; what it had
// actually run was a configuration that *does* name an acl_file, holding
// patterns none of those three ids match, where the rows are correct. The
// wording had no test either way, so the answer that was right was as
// unpinned as the one that would have been wrong.
//
// Both shapes, because the check that produces this answer sits above the
// branch between them: one edit moving it below would leave the single-id
// form right and the piped form printing dashes, which is the shape the
// sweep reported and the one nobody would have noticed.
func TestNoACLFileIsAnsweredInWordsRatherThanDashes(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels:
  - events:
      type: append
      filter: events/#
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	for _, tc := range []struct {
		why  string
		args []string
		in   string
	}{
		{why: "one client id", args: []string{cfgPath, "device-7"}},
		{why: "a list piped in", args: []string{cfgPath}, in: "device-7\ndashboard\nnobody\n"},
	} {
		t.Run(tc.why, func(t *testing.T) {
			var out, bad bytes.Buffer
			if code := explainACL(tc.args, strings.NewReader(tc.in), &out, &bad, true, asColumns); code != 0 {
				t.Fatalf("exited %d: %s", code, bad.String())
			}
			if bad.Len() != 0 {
				t.Errorf("an answer went to the refusal stream: %q", bad.String())
			}
			got := out.String()
			if !strings.Contains(got, "names no acl_file") {
				t.Errorf("the answer does not say the configuration names no acl_file:\n%s", got)
			}
			if !strings.Contains(got, "may do anything") {
				t.Errorf("the answer does not say what that means for a client:\n%s", got)
			}
			// The specific wrong answer, asserted as itself: a `-` in an
			// answer column is what RFC 0002 defines as granted nothing.
			if strings.Contains(got, "\t-\t") {
				t.Errorf("a broker granting everything answered with the row that means "+
					"granted nothing:\n%s", got)
			}
		})
	}
}

// **The header never becomes an answer**, which is the reason it starts
// with `#` and the reason its column names carry no `/`. Both commands
// already skip a comment line, so an operator who keeps a run's output and
// asks the same question again - `cut -f1` and pipe it back, which is the
// whole point of the input always being column one - gets the same answers
// and not one extra about a line naming the columns.
//
// The first version of this test asserted the stronger thing, that the
// whole output could be fed back unchanged. It cannot: an answer line's
// tabs make it one long subject, and `--route` duly answered
// `iot/depot/flow/dev-1\treadings\tappend\tiot/+/+/+` with `- broadcast
// live`. The comment in explain.go claimed the round trip until this test
// was run against it.
//
// What is a one-word edit away is renaming a column to something holding a
// `/` - `topic/filter` - which turns the header into a question about a
// topic, answered in the middle of the next run's output. That is invisible
// to every other test here.
func TestTheHeaderNeverBecomesAnAnswer(t *testing.T) {
	// The first column of every line, header included: what an operator
	// pipes back when they want to ask again.
	firstColumn := func(s string) string {
		var b strings.Builder
		for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
			b.WriteString(strings.SplitN(line, "\t", 2)[0])
			b.WriteString("\n")
		}
		return b.String()
	}

	t.Run("route", func(t *testing.T) {
		cfg := routeConfig(t)
		var first, bad bytes.Buffer
		if code := routeTopic([]string{cfg},
			strings.NewReader("iot/depot/flow/dev-1\n"), &first, &bad, true, asColumns); code != 0 {
			t.Fatalf("first run exited %d: %s", code, bad.String())
		}
		var second bytes.Buffer
		if code := routeTopic([]string{cfg},
			strings.NewReader(firstColumn(first.String())), &second, &bad, true, asColumns); code != 0 {
			t.Fatalf("second run exited %d: %s", code, bad.String())
		}
		if first.String() != second.String() {
			t.Errorf("asking the same question again did not give the same answers:\n"+
				"  first  %q\n  second %q", first.String(), second.String())
		}
	})

	t.Run("acl", func(t *testing.T) {
		dir := t.TempDir()
		aclPath := filepath.Join(dir, "acl.yaml")
		if err := os.WriteFile(aclPath, []byte(`
roles:
  r:
    - topic: "alerts/#"
      allow: [read]
users:
  dashboard: [r]
`), 0o600); err != nil {
			t.Fatalf("write acl: %v", err)
		}
		cfgPath := filepath.Join(dir, "saguin.yaml")
		if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
      filter: events/#
`), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}

		var first, bad bytes.Buffer
		if code := explainACL([]string{cfgPath},
			strings.NewReader("dashboard\n"), &first, &bad, true, asColumns); code != 0 {
			t.Fatalf("first run exited %d: %s", code, bad.String())
		}
		var second bytes.Buffer
		if code := explainACL([]string{cfgPath},
			strings.NewReader(firstColumn(first.String())), &second, &bad, true, asColumns); code != 0 {
			t.Fatalf("second run exited %d: %s", code, bad.String())
		}
		if first.String() != second.String() {
			t.Errorf("asking the same question again did not give the same answers:\n"+
				"  first  %q\n  second %q", first.String(), second.String())
		}
	})
}

// **A configuration with no acl_file must not answer JSON with an empty
// grant list**, which is the JSON spelling of the row of `-` this file's
// other test is about: both read as "granted nothing" for a broker that
// grants everything. It says so in a field instead.
func TestNoACLFileInJSONSaysEverythingIsAllowed(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels:
  - events:
      type: append
      filter: events/#
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	for _, tc := range []struct {
		why  string
		args []string
		in   string
	}{
		{why: "one client id", args: []string{cfgPath, "device-7"}},
		{why: "a list piped in", args: []string{cfgPath}, in: "device-7\nnobody\n"},
	} {
		t.Run(tc.why, func(t *testing.T) {
			var out, bad bytes.Buffer
			if code := explainACL(tc.args, strings.NewReader(tc.in), &out, &bad, true, asJSON); code != 0 {
				t.Fatalf("exited %d: %s", code, bad.String())
			}
			var got authz.Unrestricted
			if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &got); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, out.String())
			}
			if got.ACLFile != nil {
				t.Errorf("acl_file is %q, want null", *got.ACLFile)
			}
			if !got.EverythingAllowed {
				t.Errorf("everything_allowed is false for a broker with no acl_file:\n%s",
					out.String())
			}
		})
	}
}

// The list form's JSON, held to the columns beside it the way --route's is:
// same number of answers, `-` as null, and the verbs a list rather than the
// comma-joined string the column carries.
func TestACLJSONCarriesTheSameAnswersAsTheColumns(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`
roles:
  publisher:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
users:
  "vessel-*": [publisher]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ids := "vessel-7\nnobody-at-all\n"
	var cols, js, bad bytes.Buffer
	if code := explainACL([]string{cfgPath}, strings.NewReader(ids), &cols, &bad, true, asColumns); code != 0 {
		t.Fatalf("columns exited %d: %s", code, bad.String())
	}
	if code := explainACL([]string{cfgPath}, strings.NewReader(ids), &js, &bad, true, asJSON); code != 0 {
		t.Fatalf("json exited %d: %s", code, bad.String())
	}
	if strings.HasPrefix(js.String(), "#") {
		t.Errorf("the JSON form printed the column header:\n%s", js.String())
	}

	// Against the literal text first, for the reason --route's twin says:
	// holding the JSON to the columns passes when both are wrong the same
	// way, and `-` becoming a non-empty string is exactly that mutation.
	if !strings.Contains(js.String(), `"role":null`) {
		t.Errorf("no answer wrote a null role, so a client granted nothing is being "+
			"given one:\n%s", js.String())
	}

	colLines := strings.Split(strings.TrimRight(cols.String(), "\n"), "\n")[1:]
	jsLines := strings.Split(strings.TrimRight(js.String(), "\n"), "\n")
	if len(colLines) != len(jsLines) {
		t.Fatalf("%d column lines and %d JSON lines:\n%s\n%s",
			len(colLines), len(jsLines), cols.String(), js.String())
	}
	for i := range colLines {
		f := strings.Split(colLines[i], "\t")
		var got aclAnswer
		if err := json.Unmarshal([]byte(jsLines[i]), &got); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i+1, err, jsLines[i])
		}
		if got.User != f[0] {
			t.Errorf("line %d: client %q, column %q", i+1, got.User, f[0])
		}
		for _, c := range []struct {
			what   string
			column string
			field  *string
		}{
			{"role", f[1], got.Role},
			{"kind", f[2], got.Kind},
			{"subject", f[3], got.Subject},
		} {
			if c.column == "-" {
				if c.field != nil {
					t.Errorf("line %d: the %s column is %q and the field is %q, want null",
						i+1, c.what, c.column, *c.field)
				}
				continue
			}
			if c.field == nil || *c.field != c.column {
				t.Errorf("line %d, %s: column %q, field %v", i+1, c.what, c.column, c.field)
			}
		}
		// The verbs are a list in JSON and a comma-joined string in the
		// column, and a client granted nothing is `-` against an empty
		// list - never null, which a consumer iterating would trip over.
		if got.Verbs == nil {
			t.Errorf("line %d: verbs is null rather than an empty list", i+1)
		}
		want := strings.Join(got.Verbs, ",")
		if want == "" {
			want = "-"
		}
		if want != f[4] {
			t.Errorf("line %d, verbs: column %q, field %v", i+1, f[4], got.Verbs)
		}
	}
}

// **The text form says a refused pair cannot connect, before the grants.**
//
// `client_ids` is checked at CONNECT and answered `0x86` - the same code a
// wrong password gets - before a single rule is read. A paragraph of grants
// with nothing above it saying the pair cannot get in answers the wrong
// question, and the reader is somebody whose device is not working.
//
// **It is here because the JSON form's guard does not reach it.** Both
// doors take their body from `authz.Explain`, so the route's test holds
// `client_id_allowed` - and it held nothing at all about this paragraph,
// which lives in the command. Disabling the paragraph left
// `go test ./cmd/saguin ./internal/authz` green.
func TestACLSaysWhenTheClientIDCannotConnectAtAll(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(`roles:
  sensor:
    - channel: events
      allow: [write]
users:
  "north-*":
    roles: [sensor]
    client_ids: "north-*"
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
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
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
    password_file: /etc/saguin/clients.passwd
    acl_file: `+aclPath+`
channels:
  - events:
      type: append
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	run := func(t *testing.T, args ...string) string {
		t.Helper()
		var out, bad bytes.Buffer
		if code := explainACL(append([]string{cfgPath}, args...), nil, &out, &bad, false, asColumns); code != 0 {
			t.Fatalf("explain: exit %d\n%s%s", code, out.String(), bad.String())
		}
		return out.String()
	}

	refused := run(t, "north-1", "south-1")
	if !strings.Contains(refused, "may not be used by client id") {
		t.Errorf("a pair the entry refuses is explained with no word about it being unable "+
			"to connect, which sends the reader to look at the rules:\n%s", refused)
	}
	if !strings.Contains(refused, "0x86") {
		t.Errorf("the refusal does not name the code the client actually gets, which is what "+
			"an operator is looking at in a log:\n%s", refused)
	}
	// **Above the grants, because it outranks them.** A reader who stops at
	// the first paragraph has the answer.
	if at, grants := strings.Index(refused, "may not be used"),
		strings.Index(refused, "effective grants:"); grants >= 0 && at > grants {
		t.Errorf("the refusal is printed below the grants it makes irrelevant:\n%s", refused)
	}

	// And it is silent where the pair is fine, and where none was asked
	// about: a warning that appears either way is one nobody reads.
	for _, args := range [][]string{{"north-1", "north-9"}, {"north-1"}} {
		if got := run(t, args...); strings.Contains(got, "may not be used by client id") {
			t.Errorf("%v is refused nothing and was told it cannot connect:\n%s", args, got)
		}
	}
}
