package main

import (
	"fmt"
	"maps"
	"math/big"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ifnesi/saguin/internal/testkit"
)

// The configuration as the operator meets it. design.md, Testing: "Sagüin is
// tested from outside, as the people who use it see it. [...] An operator
// observes the command line, exit status". Operations: `saguin --check-config
// <file> [--output]` "checks a configuration and exits; --output prints it
// resolved, every !include expanded and every default filled in".
//
// The command these tests run is run, the whole command apart from os.Exit,
// args without the program's name:
//
//	func run(args []string, stdout, stderr io.Writer) int
//
// It exits 0 when done, 1 when a configuration is refused or cannot be read
// (Restart: "1 after [...] a refused start"), the reason on stderr, and 2 for
// a command line it does not take, with its usage on stderr. Each case
// writes a configuration file and checks only what the operator sees: the
// exit status, the message on standard error, and what --output prints.
// config.md's tables are the oracle: a refusal names the key by its full
// path, "key: why" ("an unknown or repeated key stops startup"; a Dropped
// key is "refused at startup, naming the replacement"); an included file's
// refusal names the file before the key.

// absent, as a value in a configuration's key/value pairs, removes the key.
const absent = "<absent>"

// dirToken stands, in a case's values, files, command line and expected
// text, for the case's own directory, which differs from one run to the
// next; fileToken stands, on a command line, for the configuration file.
const (
	dirToken  = "<dir>"
	fileToken = "<file>"
)

// emptyFile, as a configuration's raw text, writes a file holding nothing.
const emptyFile = "<empty>"

// required is the smallest valid file: the keys config.md marks required.
var required = []string{
	"broker.id", "edge-1",
	"broker.storage.file_path", "/var/lib/saguin/saguin.db",
	"broker.storage.default_retention_period", "7d",
	"broker.storage.default_retention_bytes", "none",
}

// configuration is a file a case writes: the required keys with each
// key/value pair of sets written over them, or raw as it stands, and the
// files beside it.
type configuration struct {
	sets  []string                       // a full key path and raw YAML; absent removes the key
	raw   string                         // the whole file, in place of the required keys and sets
	files map[string]string              // files written in the case's directory first, by name
	setup func(t *testing.T, dir string) // makes in the directory what files cannot
}

// yamlOf renders the required keys with each key/value pair of sets written
// over them as block YAML, two spaces a level. A value is written as it
// stands after "key:": a flow value starts with a space, a block one with a
// line feed.
func yamlOf(sets []string) string {
	root := map[string]any{}
	pairs := slices.Concat(required, sets)
	for i := 0; i+1 < len(pairs); i += 2 {
		put(root, strings.Split(pairs[i], "."), pairs[i+1])
	}
	var b strings.Builder
	write(&b, root, "")
	return b.String()
}

// put sets the value at path in a tree of mappings, making the mappings on
// the way and replacing a value that stands where a mapping must go.
func put(m map[string]any, path []string, value string) {
	if len(path) == 1 {
		if value == absent {
			delete(m, path[0])
		} else {
			m[path[0]] = value
		}
		return
	}
	child, ok := m[path[0]].(map[string]any)
	if !ok {
		child = map[string]any{}
		m[path[0]] = child
	}
	put(child, path[1:], value)
}

// write renders a tree of mappings as block YAML, two spaces a level.
func write(b *strings.Builder, m map[string]any, indent string) {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if child, ok := m[k].(map[string]any); ok {
			fmt.Fprintf(b, "%s%s:\n", indent, k)
			write(b, child, indent+"  ")
		} else {
			fmt.Fprintf(b, "%s%s: %s\n", indent, k, m[k])
		}
	}
}

// brokerFirst is the required keys' file with text written first in its
// broker mapping, for what key/value pairs cannot write: a key twice, a
// merge key, a key that is not a string.
func brokerFirst(text string) string {
	return strings.Replace(yamlOf(nil), "broker:\n", "broker:\n"+text, 1)
}

// caseDir is a directory of the case's own. It is made under the test's,
// whose name holds no character a path or YAML would read otherwise, never
// under the case's, whose name may.
func caseDir(t *testing.T, root string) string {
	t.Helper()
	dir, err := os.MkdirTemp(root, "case")
	if err != nil {
		t.Fatal(err)
	}
	files, err := fixtureFiles()
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		testkit.WriteFile(t, dir, name, content)
	}
	return dir
}

// fixtureFiles are the files a case's configuration names in its folder,
// each holding what loads: --check-config opens every file a configuration
// names and checks it as startup does (design.md, Operations), so a file a
// valid case names must exist and load. A password file holds one user, a
// certificate and key are a pair from one test authority, a CA file holds
// that authority, and the acl_file grants one $broadcast topic. Made once.
var fixtureFiles = sync.OnceValues(func() (map[string]string, error) {
	ca, err := testkit.NewAuthority("fixture authority")
	if err != nil {
		return nil, err
	}
	leaf, err := ca.Issue(&testkit.Leaf{CommonName: "fixture", DNSNames: []string{"localhost"}, Server: true})
	if err != nil {
		return nil, err
	}
	passwd := "fixture:$6$c2FsdA==$aGFzaA==\n" // config.md, password_file: lengths unchecked
	files := map[string]string{
		"server.crt": string(leaf.CertPEM), "server.key": string(leaf.KeyPEM),
		"other.crt": string(leaf.CertPEM),
		"peer.crt":  string(leaf.CertPEM), "peer.key": string(leaf.KeyPEM),
		"clients-ca.crt": string(ca.PEM), "peer-ca.crt": string(ca.PEM),
		"peer.pass": "fixture\n",
		"acl.yaml":  "roles:\n  fixture:\n    - topic: fixture/#\n      allow: [read]\nusers:\n  fixture: [fixture]\n",
	}
	for _, name := range []string{"passwd", "b.passwd", "clients.passwd", "local.passwd", "metrics.passwd",
		"operators.passwd", "ops.passwd", "plain.passwd", "tcp.passwd", "web.passwd", "ws.passwd"} {
		files[name] = passwd
	}
	return files, nil
})

// in replaces dirToken with dir in s.
func in(dir, s string) string { return strings.ReplaceAll(s, dirToken, dir) }

// write writes c's files into dir, and the configuration as name there, and
// returns the configuration's path. setup is the caller's to run.
func (c configuration) write(t *testing.T, dir, name string) string {
	t.Helper()
	if len(c.sets)%2 != 0 {
		t.Fatalf("sets takes key/value pairs, got %q", c.sets)
	}
	for file, content := range c.files {
		testkit.WriteFile(t, dir, file, in(dir, content))
	}
	text := c.raw
	switch text {
	case "":
		text = yamlOf(c.sets)
	case emptyFile:
		text = ""
	}
	return testkit.WriteFile(t, dir, name, in(dir, text))
}

// saguin runs the command and returns its exit code and what it printed.
func saguin(args ...string) (code int, stdout, stderr string) {
	return testkit.RunCommand(run, testkit.Separate, args...)
}

// output runs --check-config path --output, failing the test unless the
// file is accepted, and returns what it printed.
func output(t *testing.T, path string) string {
	t.Helper()
	code, stdout, stderr := saguin("--check-config", path, "--output")
	if code != 0 || stderr != "" {
		t.Fatalf("--output: exit %d, stderr %q; want 0 and nothing\n%s", code, stderr, readBack(t, path))
	}
	return stdout
}

// readBack is the file at path, for a failure's message.
func readBack(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// node reads text as one YAML document's root, failing the test if it is
// not YAML. Empty text is a null.
func node(t *testing.T, text string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("not YAML: %v\n%s", err, text)
	}
	if len(doc.Content) == 0 {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null"}
	}
	return doc.Content[0]
}

// at walks a key path through a mapping and returns the node there, the
// root for "". A part with an index, as tcp[1], needs a list holding that
// entry.
func at(root *yaml.Node, key string) (*yaml.Node, bool) {
	n := root
	if key == "" {
		return n, true
	}
	for part := range strings.SplitSeq(key, ".") {
		name, index, indexed := strings.Cut(part, "[")
		var ok bool
		if n, ok = entry(n, name); !ok {
			return nil, false
		}
		if indexed {
			var i int
			n = resolved(n)
			if _, err := fmt.Sscanf(index, "%d]", &i); err != nil || n.Kind != yaml.SequenceNode || i < 0 || i >= len(n.Content) {
				return nil, false
			}
			n = n.Content[i]
		}
	}
	return n, true
}

// entry is the value of key in mapping m.
func entry(m *yaml.Node, key string) (*yaml.Node, bool) {
	m = resolved(m)
	if m.Kind != yaml.MappingNode {
		return nil, false
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1], true
		}
	}
	return nil, false
}

// resolved follows an alias to its anchored value.
func resolved(n *yaml.Node) *yaml.Node {
	if n.Kind == yaml.AliasNode {
		return n.Alias
	}
	return n
}

// ordered names the mappings whose entries keep the order written
// (design.md, Operations, the --check-config row: "--output prints it
// [...], channels as one mapping keyed by name and channels and bridges in
// the order written"). The order of any other mapping's keys is not the
// operator's concern.
var ordered = map[string]bool{"channels": true, "bridges": true}

// differ reports how got differs from want at key, "" where it does not.
// Exact, got holds want's keys and no other; otherwise got holds want's
// keys, beside the defaults filled in. A scalar equals another spelled
// alike, or of the same duration or size (config.md, Values), as --output
// may write 60s as 1m.
func differ(want, got *yaml.Node, key string, exact bool) string {
	want, got = resolved(want), resolved(got)
	if want.Kind != got.Kind {
		return fmt.Sprintf("%s is %s, want %s", key, show(got), show(want))
	}
	switch want.Kind {
	case yaml.ScalarNode:
		if !sameScalar(want.Value, got.Value) {
			return fmt.Sprintf("%s is %q, want %q", key, got.Value, want.Value)
		}
	case yaml.SequenceNode:
		if len(want.Content) != len(got.Content) {
			return fmt.Sprintf("%s holds %d entries, want %d", key, len(got.Content), len(want.Content))
		}
		for i := range want.Content {
			if d := differ(want.Content[i], got.Content[i], fmt.Sprintf("%s[%d]", key, i), exact); d != "" {
				return d
			}
		}
	case yaml.MappingNode:
		return differMapping(want, got, key, exact)
	}
	return ""
}

// differMapping is differ for two mappings.
func differMapping(want, got *yaml.Node, key string, exact bool) string {
	if exact && len(want.Content) != len(got.Content) {
		return fmt.Sprintf("%s holds %s, want exactly %s", key, keys(got), keys(want))
	}
	last := -1
	for i := 0; i+1 < len(want.Content); i += 2 {
		name := want.Content[i].Value
		child := name
		if key != "" {
			child = key + "." + name
		}
		j := -1
		for k := 0; k+1 < len(got.Content); k += 2 {
			if got.Content[k].Value == name {
				j = k
				break
			}
		}
		if j < 0 {
			return fmt.Sprintf("%s has no %s; it holds %s", key, name, keys(got))
		}
		if ordered[key] && j < last {
			return fmt.Sprintf("%s holds %s; want %s, in that order", key, keys(got), keys(want))
		}
		last = j
		if d := differ(want.Content[i+1], got.Content[j+1], child, exact); d != "" {
			return d
		}
	}
	return ""
}

// keys lists a mapping's keys in its order.
func keys(m *yaml.Node) []string {
	var names []string
	for i := 0; i < len(m.Content); i += 2 {
		names = append(names, m.Content[i].Value)
	}
	return names
}

// show writes a node back as YAML, for a message.
func show(n *yaml.Node) string {
	out, err := yaml.Marshal(n)
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(string(out))
}

// The two spellings of a number with a unit (config.md, Values): "A duration
// is a whole number above 0 followed by one unit, s, m, h or d (24 hours)",
// some keys also taking ms; "A size is a whole number of bytes above 0,
// optionally followed by KiB, MiB or GiB".
var (
	durationText = regexp.MustCompile(`^(\d+)(ms|s|m|h|d)$`)
	sizeText     = regexp.MustCompile(`^(\d+)(KiB|MiB|GiB)?$`)
	durationUnit = map[string]int64{"ms": 1, "s": 1e3, "m": 60e3, "h": 3600e3, "d": 86400e3}
	sizeUnit     = map[string]int64{"": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}
)

// sameScalar reports whether a and b are one value: spelled alike, or the
// same duration, or the same size.
func sameScalar(a, b string) bool {
	if a == b {
		return true
	}
	for _, spelling := range []struct {
		text *regexp.Regexp
		unit map[string]int64
	}{{durationText, durationUnit}, {sizeText, sizeUnit}} {
		x, y := spelling.text.FindStringSubmatch(a), spelling.text.FindStringSubmatch(b)
		if x != nil && y != nil {
			return amount(x, spelling.unit).Cmp(amount(y, spelling.unit)) == 0
		}
	}
	return false
}

// amount is a matched number times its unit.
func amount(match []string, unit map[string]int64) *big.Int {
	n, _ := new(big.Int).SetString(match[1], 10)
	return n.Mul(n, big.NewInt(unit[match[2]]))
}

// printedAs fails the test unless out, what --output printed, holds at each
// key of pairs the value pairs gives, YAML in dir's terms. Exact, it holds
// that value and nothing more; otherwise that value with the defaults filled
// in beside it.
func printedAs(t *testing.T, out, dir string, pairs []string, exact bool) {
	t.Helper()
	root := node(t, out)
	for i := 0; i+1 < len(pairs); i += 2 {
		key, value := pairs[i], pairs[i+1]
		if value == absent {
			if got, ok := at(root, key); ok {
				t.Errorf("--output writes %s: %s, want it left out:\n%s", key, show(got), out)
			}
			continue
		}
		got, ok := at(root, key)
		if !ok {
			t.Errorf("--output has no %s:\n%s", key, out)
			continue
		}
		if d := differ(node(t, in(dir, value)), got, key, exact); d != "" {
			t.Errorf("--output: %s\n%s", d, out)
		}
	}
}

// printed is one file and the configuration --output prints for it: at each
// key of want exactly the value want gives, YAML; where want is nil, each
// key of sets as written, the defaults filled in beside it (design.md,
// Operations, the --check-config row: "--output prints it resolved [...]
// every default filled in, each value as written").
type printed struct {
	name string
	configuration
	want []string
}

// check runs the case: the file is accepted, with --output and without, and
// --output prints it as the case says. It returns the output and the case's
// directory.
func (tc *printed) check(t *testing.T, root string) (out, dir string) {
	t.Helper()
	dir = caseDir(t, root)
	if tc.setup != nil {
		tc.setup(t, dir)
	}
	path := tc.write(t, dir, "saguin.yaml")
	if code, _, stderr := saguin("--check-config", path); code != 0 || stderr != "" {
		t.Fatalf("exit %d, stderr %q; want 0 and nothing\n%s", code, stderr, readBack(t, path))
	}
	out = output(t, path)
	want, exact := tc.want, true
	if want == nil {
		want, exact = tc.sets, false
	}
	printedAs(t, out, dir, want, exact)
	return out, dir
}

// TestConfigDefaults: config.md's Default column, every key's, as --output
// prints it ("every default filled in", design.md, Operations) for a file
// that writes only the keys a case needs: the required keys alone, every
// listener kind, each channel type, a bridge.
func TestConfigDefaults(t *testing.T) {
	root := t.TempDir()
	for _, tc := range slices.Concat(brokerDefaults, listenerDefaultCases, channelDefaults, bridgeDefaults) {
		t.Run(tc.name, func(t *testing.T) { tc.check(t, root) })
	}
}

// TestConfigAccepted: each value config.md's Values paragraph and a key's
// row allow, at both edges of its range, is accepted and printed as read;
// so is every file keeping the rules that tie keys together, channel groups
// and !include, listeners, TLS, origins, identity and operations.
func TestConfigAccepted(t *testing.T) {
	root := t.TempDir()
	for _, tc := range slices.Concat(brokerAccepted, listenerAccepted, channelAccepted, bridgeAccepted) {
		t.Run(tc.name, func(t *testing.T) { tc.check(t, root) })
	}
}

// roundTrip is a file whose --output, written to a file of its own and read
// back through the command, prints the same again; never lists what the
// output must not hold.
type roundTrip struct {
	printed
	never []string
}

// TestConfigRoundTrip: what --output prints reads back, through the
// command, as the same configuration (design.md, Operations: "--output
// prints it resolved, every !include expanded"): printed again, it is
// printed alike, and it holds no !include. For the defaults, and for files
// setting every key: the broker's, every listener's, a channel of each type
// from an included group, bridges.
func TestConfigRoundTrip(t *testing.T) {
	root := t.TempDir()
	for _, tc := range slices.Concat(brokerRoundTrips, channelRoundTrips, bridgeRoundTrips) {
		t.Run(tc.name, func(t *testing.T) {
			first, dir := tc.check(t, root)
			for _, text := range append([]string{"!include"}, tc.never...) {
				if strings.Contains(first, in(dir, text)) {
					t.Errorf("--output holds %q:\n%s", text, first)
				}
			}
			again := output(t, testkit.WriteFile(t, caseDir(t, root), "resolved.yaml", first))
			if again != first {
				t.Errorf("read back, --output prints\n%s\nwant what it printed first\n%s", again, first)
			}
		})
	}
}

// refusal is one command line and how it is refused: the exit status, and a
// message on stderr naming the key, "key: why", with nothing on stdout.
type refusal struct {
	name string
	configuration
	args  []string // the command line, fileToken standing for the file; nil: --check-config and the file
	code  int      // the exit status; 0 stands for 1
	key   string   // the key refused; "" where a command line or a file that cannot be read is refused
	or    string   // the other key, where config.md leaves open which of two is named
	says  []string // further text the message holds, in dir's terms: keys, files, a dropped key's replacement
	never []string // text the message does not hold
	// alone runs the case in a run of the test binary of its own, which
	// must answer within aloneLimit and with its heap under aloneHeap.
	alone bool
	// nest, where set, makes the file: nest(levels, aliased) written at
	// nestAt, for 1 to nestLevels levels (refusedPromptly).
	nest   func(levels int, aliased bool) string
	nestAt string
}

// caseDirEnv names the environment variable that holds, in the run an alone
// case makes, the directory the case's setup made.
const caseDirEnv = "SAGUIN_CONFIG_CASE_DIR"

// aloneLimit is how long an alone case's run may take: reading a file is
// instant, so a run still going then is reading something that never ends,
// or waiting for a writer that never comes.
const aloneLimit = 10 * time.Second

// aloneHeap is the heap an alone case's run may grow to: an !include of
// /dev/zero, read to its end, would take every byte the machine has before
// aloneLimit.
const aloneHeap = 256 << 20

// TestConfigRefused: every value just past what config.md allows, every
// rule between keys broken, an unknown, duplicate or dropped key, a missing
// required key, a second document, a merge key, a circular or deeply nested
// alias, a bad !include, a written path repeated, a file that cannot be
// read and a command line saguin does not take are refused: exit 1 (2 for
// the command line, with its usage), nothing on stdout, and on stderr a
// message naming the key by its full path, and for a dropped key its
// replacement.
func TestConfigRefused(t *testing.T) {
	root := t.TempDir()
	for _, tc := range slices.Concat(brokerRefused, listenerRefused, channelRefused, bridgeRefused) {
		t.Run(tc.name, func(t *testing.T) {
			dir, inner := os.LookupEnv(caseDirEnv)
			if !inner {
				dir = caseDir(t, root)
				if tc.setup != nil {
					tc.setup(t, dir)
				}
			}
			switch {
			case tc.nest != nil:
				tc.refusedPromptly(t, dir)
				return
			case tc.alone && !inner:
				testkit.Rerun(t, "the run of "+tc.name, aloneLimit, []string{caseDirEnv + "=" + dir})
				return
			case tc.alone:
				defer guardHeap()()
			}
			path := tc.write(t, dir, "saguin.yaml")
			args := []string{"--check-config", fileToken}
			if tc.args != nil {
				args = slices.Clone(tc.args)
			}
			for i := range args {
				args[i] = in(dir, strings.ReplaceAll(args[i], fileToken, path))
			}
			code, stdout, stderr := saguin(args...)
			tc.refused(t, dir, code, stdout, stderr)
		})
	}
}

// refused fails the test unless the command's answer is the case's refusal.
func (tc *refusal) refused(t *testing.T, dir string, code int, stdout, stderr string) {
	t.Helper()
	want := tc.code
	if want == 0 {
		want = 1
	}
	if code != want || stdout != "" || stderr == "" {
		t.Fatalf("exit %d, stdout %q, stderr %q; want %d, nothing on stdout and the reason on stderr", code, stdout, stderr, want)
	}
	// The table's code marks (backticks) are the document's, not the
	// message's.
	message := strings.ReplaceAll(stderr, "`", "")
	if tc.key != "" && !names(message, tc.key) && (tc.or == "" || !names(message, tc.or)) {
		t.Errorf("stderr %q does not refuse %s, named as %q", stderr, tc.key, tc.key+": ")
	}
	for _, text := range tc.says {
		if !strings.Contains(message, in(dir, text)) {
			t.Errorf("stderr %q does not name %q", stderr, in(dir, text))
		}
	}
	for _, text := range tc.never {
		if strings.Contains(message, text) {
			t.Errorf("stderr %q holds %q", stderr, text)
		}
	}
}

// names reports whether message refuses key: "key: why", or, where an
// included file is named first, "file: key: why".
func names(message, key string) bool {
	return strings.HasPrefix(message, key+": ") || strings.Contains(message, ": "+key+": ")
}

// nestLevels is the deepest nesting refusedPromptly tries: each level ten
// aliases of the level before, twelve levels, under 1 KiB, expand to 10^12
// nodes.
const nestLevels = 12

// refusedPromptly: a key is found by reading the file, so a refusal costs
// the file's reading, never what its aliases would expand to. Its deadline
// is counted in allocations, not time: at each depth, refusing the file
// costs at most twice what refusing the same file costs with every alias
// written as a plain value. Depths are tried shallowest first, and the first
// over its bound ends the case, so a reader that expands aliases fails
// within a few levels instead of running for hours.
func (tc *refusal) refusedPromptly(t *testing.T, dir string) {
	t.Helper()
	for levels := 1; levels <= nestLevels; levels++ {
		files := map[bool]string{}
		for _, aliased := range []bool{true, false} {
			c := tc.configuration
			c.sets = slices.Concat(c.sets, []string{tc.nestAt, tc.nest(levels, aliased)})
			files[aliased] = c.write(t, dir, fmt.Sprintf("aliased-%t.yaml", aliased))
		}
		cost := func(path string) float64 {
			var code int
			var stdout, stderr string
			allocs := testing.AllocsPerRun(1, func() { code, stdout, stderr = saguin("--check-config", path) })
			tc.refused(t, dir, code, stdout, stderr)
			return allocs
		}
		if work, bound := cost(files[true]), 2*cost(files[false]); work > bound {
			t.Fatalf("refusing %d levels of aliases took %.0f allocations; at most %.0f, twice the same file's without aliases\n%s",
				levels, work, bound, readBack(t, files[true]))
		}
	}
}

// guardHeap ends the run at once, failing it, should its heap grow past
// aloneHeap; the function it returns stops the guard.
func guardHeap() (stop func()) {
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > aloneHeap {
					fmt.Fprintf(os.Stderr, "the heap grew past %d bytes\n", aloneHeap)
					os.Exit(2)
				}
			}
		}
	}()
	return func() { close(done) }
}

// config.md, broker.storage: "When one is written in another mode, startup
// and --check-config refuse it, naming the key and the mode." Exercise both
// commands, including an explicit value equal to the key's default.
func TestStorageKeysBelongToTheirMode(t *testing.T) {
	root := t.TempDir()
	for _, key := range []struct{ name, value, mode string }{
		{"sync_interval", "1s", "normal"},
		{"snapshot_interval", "30m", "memory"},
		{"memory_dir", "/dev/shm/saguin", "memory"},
	} {
		for _, mode := range []string{"full", "normal", "memory"} {
			if mode == key.mode {
				continue
			} // accepted values and round-trips cover this mode
			for _, command := range []string{"--config", "--check-config"} {
				t.Run(key.name+"/"+mode+"/"+command, func(t *testing.T) {
					dir := caseDir(t, root)
					tc := refusal{key: "broker.storage." + key.name, says: []string{mode},
						sets: []string{"broker.storage.mode", mode, "broker.storage." + key.name, key.value}}
					path := tc.write(t, dir, "mode.yaml")
					code, out, err := saguin(command, path)
					tc.refused(t, dir, code, out, err)
				})
			}
		}
	}
}
