package config_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/config"
)

// The configuration is tested as the operator meets it, through saguin
// --check-config [--output] (cmd/saguin, config_test.go; design.md,
// Testing). What stays here is what neither the command nor its output
// shows: FuzzParse, which reads the operator's files ("Code that reads
// bytes or text from outside Sagüin [...] is fuzzed"), and, until the
// maintainer decides where they go, the cases whose value --output does
// not print: a bridge rule's {a,b} expansions, and the file each channel
// was defined in.

// required is the smallest valid file: the keys config.md marks required.
const required = "broker:\n  id: edge-1\n  storage:\n    file_path: /var/lib/saguin/saguin.db\n" +
	"    default_retention_period: 7d\n    default_retention_bytes: none\n"

// withChannels and withBridges are required with the section's text
// written after "channels:" or "bridges:".
func withChannels(text string) []byte { return []byte(required + "channels:" + text + "\n") }
func withBridges(text string) []byte  { return []byte(required + "bridges:" + text + "\n") }

// parsed is config.Parse(data), failing the test if data is refused.
func parsed(t *testing.T, data []byte) *config.Config {
	t.Helper()
	c, err := config.Parse(data)
	if err != nil {
		t.Fatalf("refused: %v\n%s", err, data)
	}
	return c
}

// TestFilterExpansions: config.md, bridges.<name>: a rule's filter takes
// "Braces such as {a,b} follow a channel filter's rules"; design.md, Words:
// "A {a,b} level is
// expanded first, with each alternative treated as a written level"; each
// expansion once, the alternatives in the order written and an earlier level
// varying more slowly than a later one: x/{a,b}/{c,d} is x/a/c, x/a/d, x/b/c,
// x/b/d. --output prints the filter as configured, and no command shows a
// bridge's expansions yet; the cases are those of the old piece tests that
// checked them, the rest of each case being a row of cmd/saguin's
// TestConfigAccepted. A channel's expansions are seen through saguin --route
// (internal/harness/conformance, TestRouteAnswersAsTheBrokerRoutes).
func TestFilterExpansions(t *testing.T) {
	for _, tc := range []struct {
		name, file string
		expanded   []string
	}{
		// Bridges (TestBridgeFilters, TestBridgeTopics, TestBridgeTopicLimits,
		// TestBridgeInboundOverlap, TestBridgeOutRules, TestBridgesMarshal).
		{"a rule's first level as {a,b}", "{a,b}/#", []string{"a/#", "b/#"}},
		{"a rule's {a,b} before +", "fleet/{north,south}/+", []string{"fleet/north/+", "fleet/south/+"}},
		{"a rule's two {a,b} levels", "x/{a,b}/{c,d}", []string{"x/a/c", "x/a/d", "x/b/c", "x/b/d"}},
		{"a rule's alternatives in the order written", "x/{b,a}/+", []string{"x/b/+", "x/a/+"}},
		{"a rule's {a,b} between levels", "x/{a,b}/+/y", []string{"x/a/+/y", "x/b/+/y"}},
		{"a rule's expansions of 5 bytes", "ab/{cd,ef,gh}", []string{"ab/cd", "ab/ef", "ab/gh"}},
		{"a rule's expansions of 3 levels", "a/{b,c}/d", []string{"a/b/d", "a/c/d"}},
		{"a rule's {a,b} last", "a/{d,e}", []string{"a/d", "a/e"}},
		{"a rule's two alternatives", "x/{a,b}", []string{"x/a", "x/b"}},
		{"a rule's {a,b} before + and #", "fleet/{north,south}/+/#", []string{"fleet/north/+/#", "fleet/south/+/#"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := parsed(t, withBridges(` {b: {peer: "tcp://peer.example:1883", client_id: edge-1, topics: [{filter: "`+tc.file+`", direction: both}]}}`))
			if got := c.Bridges[0].Topics[0].Filters; !slices.Equal(got, tc.expanded) {
				t.Errorf("%s expands to %q, want %q", tc.file, got, tc.expanded)
			}
		})
	}
}

// includeToken stands, in a fuzzed file, for the path of the included file
// FuzzParse writes: a path differs from one run to the next, so the corpus
// keeps the token, never a path.
const includeToken = "<included>"

// fuzzSeeds are files of each shape the configuration takes, accepted and
// refused, each with the group an !include of includeToken reads: every
// listener kind, channels in groups and included, a bridge, dropped and
// unknown keys, aliases, keys that are not plain strings (the first three
// root keys are inputs FuzzParse found) and a second document.
var fuzzSeeds = []struct{ data, group string }{
	{required, ""},
	{required + "  mqtt:\n    listen:\n      tcp: [{name: a, address: \":1883\"}, {name: b, address: \"[::]:8883\", " +
		"tls: {cert_file: /etc/saguin/server.crt, key_file: /etc/saguin/server.key, client_ca_file: /etc/saguin/ca.crt}}]\n" +
		"      ws: {address: \":8080\", allowed_origins: [\"https://example.com\"]}\n      unix: {path: saguin.sock, mode: \"0600\", proxy_protocol: true}\n" +
		"    password_file: /etc/saguin/passwd\n    acl_file: /etc/saguin/acl.yaml\n" +
		"  operations:\n    listen: {tcp: {address: \"127.0.0.1:9090\"}, unix: {path: /run/saguin/ops.sock}}\n", ""},
	{required + "  limits: {max_inflight: 7, write_timeout: 1500ms, max_incoming_memory: 1GiB, max_outgoing_memory: 1GiB}\n", ""},
	{required + "  storage: {flush_interval: 150ms}\n", ""},
	{required + "  colour: blue\n", ""},
	{required + "  mqtt:\n    listen:\n      tcp: &l [{name: a, address: \":1883\"}, *l]\n", ""},
	{required + "  <<: {log_level: warn}\n", ""},
	{required + "---\nbroker:\n  log_level: warn\n", ""},
	{"&0:\n", ""},
	{"? >\n", ""},
	{"{{0}}\n", ""},
	{"!!int 3: 1\n", ""},
	{string(withChannels(" [{zeta: {type: append, filter: \"x/{a,b}\"}}, {q: {type: queue, retry: {mode: linear}}}]")), ""},
	{string(withChannels("\n  - {x: {type: append}}\n  - !include " + includeToken)), "q:\n  type: queue\n  retry:\n    backoff: linear\n"},
	{string(withChannels("\n  - !include " + includeToken)), "l:\n  type: latest\n  tombstone_retention_period: 36h\n"},
	{string(withBridges(` {b: {peer: "wss://peer.example:443", client_id: edge-1, tls: {ca_file: /etc/saguin/ca.crt}, ` +
		`topics: [{filter: "a/{b,c}/+", topic: "x/$1", direction: in, qos: 2}, {filter: "o/#", topic: "y/$#", direction: out}]}}`)), ""},
	{string(withBridges(` {b: {upstream: peer}}`)), ""},
}

// FuzzParse: config.Parse reads any bytes without panicking, and either
// accepts them or refuses them, never both or neither. Every refusal is a
// *config.Error naming a key, in Key and in its text (malformed YAML names
// "configuration"). What it accepts, --output prints, and the output reads
// back as the same configuration, its channels then defined in the output
// itself (cmd/saguin's TestConfigRoundTrip). Where the file names
// includeToken, the token is replaced by the path of a file holding the
// second input, so !include reads what the fuzzer writes.
func FuzzParse(f *testing.F) {
	included := filepath.Join(f.TempDir(), "group.yaml")
	for _, seed := range fuzzSeeds {
		f.Add([]byte(seed.data), []byte(seed.group))
	}
	f.Fuzz(func(t *testing.T, data, group []byte) {
		if bytes.Contains(data, []byte(includeToken)) {
			if err := os.WriteFile(included, group, 0o600); err != nil {
				t.Fatal(err)
			}
			data = bytes.ReplaceAll(data, []byte(includeToken), []byte(included))
		}
		c, err := config.Parse(data)
		if err != nil {
			var e *config.Error
			switch {
			case c != nil:
				t.Fatalf("refused (%v) and accepted at once:\n%s", err, data)
			case !errors.As(err, &e):
				t.Fatalf("error %q is not a *config.Error:\n%s", err, data)
			case e.Key == "" || !strings.Contains(err.Error(), e.Key):
				t.Fatalf("error %q has Key %q; want a key, named in the text:\n%s", err, e.Key, data)
			}
			return
		}
		if c == nil {
			t.Fatalf("neither accepted nor refused:\n%s", data)
		}
		out, err := c.Marshal()
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		for i := range c.Channels {
			c.Channels[i].ConfiguredIn = ""
		}
		if back := parsed(t, out); !reflect.DeepEqual(back, c) {
			t.Fatalf("the output reads back as\n%+v\nwant\n%+v\n%s", back, c, out)
		}
	})
}
