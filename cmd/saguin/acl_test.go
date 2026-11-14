package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// aclLimit bounds one reading of a fuzzed acl_file: a few kilobytes of
// YAML, read in a millisecond; a reading still going after it hangs.
const aclLimit = 10 * time.Second

// aclSeeds are acl_files of each shape the file takes, read and refused:
// design.md's example (Access and reload), every key and verb, patterns of
// equal and unequal counts, substitutions, and the mistakes the design
// lists; then YAML that is no acl_file: empty, a scalar, a sequence, an
// alias bomb, a circular alias, a second document and bytes that are not
// UTF-8.
var aclSeeds = []string{
	`roles:
  sensor:
    - channel: events
      filter: events/%u/#
      allow: [write]
    - broker: features
      deny: [persistent, qos2]
  dashboard:
    - channel: events
      allow: [read, seek]
    - topic: alerts/#
      allow: [read]
users:
  "sensor-*":
    roles: [sensor]
    limits: {publish_rate: 100, publish_bytes: 64KiB}
    client_ids: "%u"
  ops: [dashboard]
`,
	`roles:
  all:
    - channel: kv
      allow: [write, read]
    - channel: jobs
      allow: [write, read, seek, consume]
    - channel: jobs__dlq
      allow: [read, seek]
    - topic: "home/%u/%c/#"
      allow: [write, read]
    - broker: sessions
      allow: [disconnect]
    - broker: features
      deny: [persistent, will, share, retained, qos2]
users:
  device-7: [all]
  "device-*": {roles: [all], client_ids: ["%u-*", spare]}
  "*-7": [all]
  "é*": [all]
  "*": [all]
`,
	"roles:\n  r:\n    - topic: a/#\n      allow: [read]\nusers:\n  \"a*\": [r]\n  \"*b\": [r]\n",
	"roles:\n  r:\n    - topic: events/a\n      allow: [read]\nusers:\n  u: [r]\n",
	"roles:\n  r:\n    - channel: events\n      topic: a\n      allow: [read]\nusers:\n  u: [r]\n",
	"roles:\n  r:\n    - broker: features\n      allow: [read]\n      deny: [will, will]\nusers:\n  u: [ghost]\n",
	"roles:\n  r: []\nusers:\n  u: {roles: [r], client_ids: \"%c\", limits: {publish_rate: 0, publish_bytes: lots}}\n",
	"users:\n  u: []\n",
	"",
	"acl\n",
	"- roles\n- users\n",
	"a: &a [x, x, x, x, x, x, x, x, x]\nb: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a]\nc: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b]\nroles: [*c, *c, *c, *c, *c, *c, *c, *c, *c]\n",
	"roles: &r {x: *r}\n",
	"roles: {}\n---\nusers: {}\n",
	"roles:\n  \xff: [{topic: a, allow: [read]}]\n",
}

// design.md, Testing: "Code that reads bytes or text from outside Sagüin
// is fuzzed, whether the input comes from clients, a bridge's peer or the
// operator's files"; Access and reload: "The broker refuses a file that
// holds any of the mistakes below. The refusal names the file, the entry and
// what is wrong"; Operations: saguin --acl
// "explains, from the files on disk, what a client may do", exiting 0, or
// 1 for a file refused (Restart: "1 after [...] a refused start", the
// status a refused configuration exits with).
//
// FuzzACLFile reads every input as the acl_file of one configuration,
// through the command the operator runs, in this process: saguin --acl
// <config> device-7 returns within aclLimit, never panicking, and either
// reads the file, exit 0 and nothing on standard error, or refuses it, exit
// 1 and a line of standard error naming the file. The configuration has a
// channel of each type and a client password file holding device-7.
func FuzzACLFile(f *testing.F) {
	dir := f.TempDir()
	hash, err := harness.SevenHash("device-7-pw", 1000, []byte("sixteen-byte-salt"))
	if err != nil {
		f.Fatal(err)
	}
	acl := filepath.Join(dir, "acl.yaml")
	config, err := harness.WriteConfig(&harness.Options{Dir: dir,
		Login:   harness.Login{PasswordFile: testkit.WriteFile(f, dir, "passwd", "device-7:"+hash+"\n")},
		ACLFile: acl,
		Channels: []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"},
			{Name: "kv", Type: "latest", Filter: "kv/#"}, {Name: "jobs", Type: "queue", Filter: "jobs/#"}}})
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range aclSeeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(acl, data, 0o600); err != nil {
			t.Fatal(err)
		}
		var code int
		var stdout, stderr string
		if !testkit.ReturnsWithin(aclLimit, func() {
			code, stdout, stderr = testkit.RunCommand(run, testkit.Separate, "--acl", config, "device-7")
		}) {
			t.Fatalf("--acl still read the file after %v:\n%q", aclLimit, data)
		}
		switch code {
		case 0:
			if stderr != "" {
				t.Fatalf("--acl read the file, exit 0, and printed %q on standard error; want nothing:\n%q", stderr, data)
			}
		case 1:
			if !strings.Contains(stderr, acl) {
				t.Fatalf("--acl refused the file, exit 1, printing %q; want a line naming %s:\n%q", stderr, acl, data)
			}
		default:
			t.Fatalf("--acl: exit %d, stdout %q, stderr %q; want 0, the file read, or 1, refused:\n%q", code, stdout, stderr, data)
		}
	})
}
