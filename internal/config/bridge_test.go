package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
)

// A bridge over the two channels the rules below land in.
const bridgeChannels = "\nchannels:\n" +
	"  readings:\n    type: append\n" +
	"  raw:\n    type: append\n" +
	"  jobs:\n    type: queue\n"

func bridgeYAML(rules string) string {
	return "broker:\n  id: t\n" + memStorage + bridgeChannels +
		"bridges:\n" +
		"  head-office:\n" +
		"    peer: tls://mqtt.example.com:8883\n" +
		"    client_id: vessel-07\n" +
		"    topics:\n" + rules
}

func loadBridge(t *testing.T, rules string) []config.Bridge {
	t.Helper()
	f, _, err := config.Load(write(t, bridgeYAML(rules)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return f.BridgeSet()
}

// RFC 0002 "Bridges"
//
// An inbound filter is an ordinary MQTT topic filter, and the topic it
// produces is the whole topic the record takes - with `$1`, `$2` … for
// the filter's `+` levels in order and `$#` for its `#` tail.
//
// The reordering case is the one a prefix strip cannot do, and it is why
// there is a template at all: `fleet/<vessel>/telemetry/<metric>` becomes
// `telemetry/<vessel>/<metric>`, which is a different shape rather than a
// shorter one.
func TestInboundTopicTemplate(t *testing.T) {
	for _, tc := range []struct {
		name, filter, channel, topic string
		cases                        map[string]string // upstream topic -> local topic, "" for no match
	}{
		{
			name: "a wildcard level and a tail", filter: "fleet/+/telemetry/#",
			topic: "readings/telemetry/$1/$#",
			cases: map[string]string{
				"fleet/vessel-07/telemetry/temp":     "readings/telemetry/vessel-07/temp",
				"fleet/vessel-07/telemetry/hold/psi": "readings/telemetry/vessel-07/hold/psi",
				// `#` stands in for nothing as well as for everything, so the
				// filter reaches the topic above it and the separator that was
				// joining the tail goes with it.
				"fleet/vessel-07/telemetry": "readings/telemetry/vessel-07",
				// `+` is exactly one level, which is the whole reason it was
				// chosen over a regex: it cannot swallow a separator.
				"fleet/a/b/telemetry/c": "",
				"depot/a/telemetry/b":   "",
				"fleet/vessel-07":       "",
			},
		},
		{
			name: "the tail alone", filter: "fleet/#",
			topic: "raw/$#",
			cases: map[string]string{
				"fleet/a/b": "raw/a/b",
				"fleet/a":   "raw/a",
				"other/a":   "",
			},
		},
		{
			name: "levels reordered", filter: "fleet/+/+",
			topic: "readings/$2/$1",
			cases: map[string]string{
				"fleet/vessel-07/temp": "readings/temp/vessel-07",
				"fleet/a":              "",
				"fleet/a/b/c":          "",
			},
		},
		{
			name: "a level deliberately dropped", filter: "fleet/+/telemetry/#",
			topic: "readings/telemetry/$#",
			cases: map[string]string{
				// One bridge per vessel drops the vessel id on purpose. Only a
				// discarded `#` is refused, because that one collapses an
				// unbounded number of topics onto one.
				"fleet/vessel-07/telemetry/temp": "readings/telemetry/temp",
				"fleet/vessel-99/telemetry/temp": "readings/telemetry/temp",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs := loadBridge(t, "      - filter: "+tc.filter+"\n"+

				"        topic: "+tc.topic+"\n        direction: in\n")
			if len(bs) != 1 || len(bs[0].Topics) != 1 {
				t.Fatalf("got %d bridges", len(bs))
			}
			in := bs[0].Topics[0]
			for upstream, want := range tc.cases {
				got, ok := in.Match(upstream)
				if want == "" {
					if ok && got != "" {
						t.Errorf("%q matched as %q, want no match", upstream, got)
					}
					continue
				}
				if !ok {
					t.Errorf("%q did not match, want %q", upstream, want)
					continue
				}
				if got != want {
					t.Errorf("%q -> %q, want %q", upstream, got, want)
				}
			}
		})
	}
}

// A `#` that matched zero levels leaves nothing to publish, because a
// channel topic has a non-empty suffix (RFC 0002). That is told apart from
// "no rule covers this": one is traffic the bridge was never asked about,
// the other is traffic it was asked about and cannot place, and only the
// second is worth a warning.
func TestInboundTailCanMatchNothing(t *testing.T) {
	// **A template that is a `$#` and nothing else.** The tail stands in for
	// nothing, so there is no topic left to publish, and the rule covered the
	// record rather than ignoring it - which is the pair the bridge tells
	// apart with a warning.
	bare := loadBridge(t, "      - filter: fleet/#\n        topic: $#\n        direction: in\n")
	got, ok := bare[0].Topics[0].Match("fleet")
	if !ok {
		t.Fatal("`fleet/#` did not reach the bare `fleet`, which MQTT 5 section 4.7.1.2 says it does")
	}
	if got != "" {
		t.Fatalf("bare `fleet` produced %q, want nothing to publish", got)
	}

	// **And a template that spells a topic out around the tail still has one.**
	// This changed when a channel stopped being a prefix the rule prepended:
	// `raw/$#` builds a whole topic, so an empty tail leaves `raw`, which the
	// channel's own filter `raw/#` matches because a `#` stands in for nothing
	// at both ends. There is nothing left to refuse - the record has a topic
	// and it lands in the channel the rule names.
	whole := loadBridge(t, "      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n")
	in := whole[0].Topics[0]
	if got, ok := in.Match("fleet"); !ok || got != "raw" {
		t.Fatalf("bare `fleet` produced %q ok=%v, want %q", got, ok, "raw")
	}

	if _, ok := in.Match("other/thing"); ok {
		t.Fatal("a topic no rule covers reported a match")
	}
}

// The reserved space is saguin's own: a queue acknowledgement and a
// consumer's seek live there. A bridge able to publish into it would be
// forging them on behalf of whatever it carries, and only a rule naming no
// channel could reach it at all.
//
// It is closed in two different places, which is worth knowing because only
// one of them is a check. A `$` written in a template begins a substitution
// and never survives as literal text, so a template cannot express the space
// - `$saguin/…` is refused as a `$` that is neither `$#` nor a number. What
// is left is a capture built from whatever the upstream published, which
// nothing at startup can see, and that is what Match refuses.
func TestABridgeCannotReachTheReservedSpace(t *testing.T) {
	_, _, err := config.Load(write(t, bridgeYAML(
		"      - filter: fleet/#\n        topic: $saguin/queue/jobs/response\n        direction: in\n")))
	if err == nil {
		t.Fatal("a template in the reserved space was accepted")
	}
	if !strings.Contains(err.Error(), "not `$#` or a wildcard number") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// The one a capture builds. It has to match, or the assertion below
	// passes for the wrong reason.
	bs := loadBridge(t, "      - filter: fleet/+/#\n        topic: $1/$#\n        direction: in\n")
	got, ok := bs[0].Topics[0].Match("fleet/$saguin/queue/jobs/response")
	if !ok {
		t.Fatal("the rule did not match, so the check below proves nothing")
	}
	if got != "" {
		t.Fatalf("a capture reached the reserved space as %q", got)
	}

	// And the same rule carries an ordinary topic through, so the check above
	// is refusing the `$` rather than everything.
	if got, ok := bs[0].Topics[0].Match("fleet/vessel-07/alerts/fire"); !ok || got != "vessel-07/alerts/fire" {
		t.Fatalf("an ordinary topic did not survive: %q ok=%v", got, ok)
	}
}

// Every finding is reported and the message says which rule was broken, so
// that a configuration with two mistakes in it takes one restart rather
// than two.
func TestInboundRulesAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name, rules, want string
	}{
		{"`#` is only ever last", "      - filter: fleet/#/telemetry\n        topic: readings/$#\n        direction: in\n",
			"only ever the last level"},
		{"a wildcard takes a whole level", "      - filter: fleet/a+b/#\n        topic: readings/$#\n        direction: in\n",
			"mixes a wildcard"},
		{"a numbered capture the filter does not have",
			"      - filter: fleet/+/#\n        topic: $2/$#\n        direction: in\n",
			"names a wildcard the filter does not have"},
		{"`$#` with no `#` to fill it", "      - filter: fleet/+\n        topic: $1/$#\n        direction: in\n",
			"has no `#` to fill it"},
		{"a discarded tail collapses every topic onto one",
			"      - filter: fleet/+/#\n        topic: $1\n        direction: in\n",
			"would collapse onto one"},
		{"`$#` is only ever at the end", "      - filter: fleet/#\n        topic: readings/$#/x\n        direction: in\n",
			"only ever at the end"},
		{"a template does not lead with a separator",
			"      - filter: fleet/#\n        topic: /$#\n        direction: in\n",
			"does not lead with a separator"},
		{"a template does not end with a separator",
			"      - filter: fleet/+/#\n        topic: \"$1/$#/\"\n        direction: in\n",
			"empty last level"},
		{"a bare `$` is a typo", "      - filter: fleet/#\n        topic: $x/$#\n        direction: in\n",
			"not `$#` or a wildcard number"},
		// A published topic may hold neither character anywhere
		// (MQTT-3.3.2-2), and every one of these was accepted until the
		// check moved onto the compiled parts. The old one asked whether the
		// whole template contained `+` or `#` *and no* `$#` - which exempted
		// every template using a tail, because `$#` contains a `#`.
		//
		// What each produced was a topic the broker refuses with 0x82, which
		// a bridge then held at the head of its link for ever.
		//
		// There is deliberately no `a#b/$#` row beside the `a+b/$#` one. The
		// check is a single IndexAny over both characters, so there is no
		// path on which they can diverge and the row would cover nothing the
		// `+` row does not already reach. What a row here guards against is
		// somebody "simplifying" this into a level-aware check - `text == "+"
		// || text == "#"` - and `a+b/$#` fails that refactor on its own.
		{"a wildcard level beside a tail",
			"      - filter: fleet/+/#\n        topic: x/+/$#\n        direction: in\n",
			"literal '+'"},
		{"a hash level beside a tail",
			"      - filter: fleet/+/#\n        topic: x/#/$#\n        direction: in\n",
			"literal '#'"},
		{"a wildcard level leading a tail",
			"      - filter: fleet/+/#\n        topic: \"+/$#\"\n        direction: in\n",
			"literal '+'"},
		{"a wildcard character inside a level",
			"      - filter: fleet/+/#\n        topic: a+b/$#\n        direction: in\n",
			"literal '+'"},
		{"a published topic holds no wildcard",
			"      - filter: fleet/+\n        topic: $1/+\n        direction: in\n",
			"literal '+'"},
		{"no topic", "      - filter: fleet/#\n        direction: in\n", "no topic"},
		{"no filter", "      - topic: x\n        direction: in\n", "filter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, bridgeYAML(tc.rules)))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("message does not say what is wrong.\n got: %v\nwant it to contain: %q", err, tc.want)
			}
		})
	}
}

// A bridge itself, rather than one of its rules.
func TestBridgeIsRefused(t *testing.T) {
	const rule = "    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"
	for _, tc := range []struct {
		name, bridges, want string
	}{
		{"no upstream", "  a:\n    client_id: v\n" + rule, "no address"},
		{"a scheme nothing can dial", "  a:\n    peer: mqtt://h:1883\n    client_id: v\n" + rule,
			"is not one of"},
		{"no port", "  a:\n    peer: tls://h\n    client_id: v\n" + rule, "no port"},
		{"no client_id", "  a:\n    peer: tls://h:8883\n" + rule, "no client_id"},
		{"no rules", "  a:\n    peer: tls://h:8883\n    client_id: v\n", "carry nothing"},
		// Half a pair is a typo rather than a setting: a certificate with no
		// key cannot be presented and a key with no certificate names nobody.
		{"a certificate with no key",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n" +
				"    cert_file: /etc/saguin/tls/bridge.pem\n" + rule,
			"both the certificate it presents"},
		{"a key with no certificate",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n" +
				"    key_file: /etc/saguin/tls/bridge-key.pem\n" + rule,
			"both the certificate it presents"},
		{"a relative certificate",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n" +
				"    cert_file: bridge.pem\n    key_file: /etc/saguin/tls/bridge-key.pem\n" + rule,
			"is relative"},
		// There is no handshake on a tcp:// link, so a certificate named
		// beside one is never presented while reading as though it were.
		{"a certificate on an unencrypted upstream",
			"  a:\n    peer: tcp://h:1883\n    client_id: v\n" +
				"    cert_file: /etc/saguin/tls/bridge.pem\n" +
				"    key_file: /etc/saguin/tls/bridge-key.pem\n" + rule,
			"is not encrypted"},
		{
			// Two connections with one client id disconnect each other as fast
			// as they can reconnect, and nothing reports it but a connection
			// log that scrolls.
			"one client id at one upstream twice",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n" + rule +
				"  b:\n    peer: tls://h:8883\n    client_id: v\n" + rule,
			"disconnect each other",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t,
				"broker:\n  id: t\n"+memStorage+bridgeChannels+"bridges:\n"+tc.bridges))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("message does not say what is wrong.\n got: %v\nwant it to contain: %q", err, tc.want)
			}
		})
	}
}

// **The two renamed keys are refused by name, and the refusal says what to
// write instead.** Every bridge ever configured has both in it, so the
// decoder's own "field upstream not found in type config.BridgeConfig" -
// true, and no help at all - is not good enough on its own.
func TestTheRenamedKeysNameTheirReplacements(t *testing.T) {
	const rule = "      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"
	for _, tc := range []struct {
		name, bridge string
		want         []string
	}{
		{
			"upstream: instead of peer:",
			"  a:\n    upstream: tls://h:8883\n    client_id: v\n    topics:\n" + rule,
			[]string{"upstream:", "peer:"},
		},
		{
			"inbound: instead of topics:",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n    inbound:\n" + rule,
			[]string{"inbound:", "topics:"},
		},
		{
			// The shape every existing configuration actually has, which is
			// why both are refused in one pass rather than one at a time: an
			// operator fixing this file should not have to run the check
			// twice to find out there was a second rename.
			"both, as every existing file writes them",
			"  a:\n    upstream: tls://h:8883\n    client_id: v\n    inbound:\n" + rule,
			[]string{"upstream:", "peer:", "inbound:", "topics:"},
		},
		{
			// **`channel:` was not renamed, it went**, and the refusal has
			// to say so: an operator who reads "unknown key" looks for the
			// new spelling of a key that has none. What replaced it is the
			// topic, which was already deciding, and the place to check the
			// expectation the key used to carry.
			"channel: on a rule, which every bridge written before this has",
			"  a:\n    peer: tls://h:8883\n    client_id: v\n    topics:\n" +
				"      - filter: fleet/#\n        topic: raw/$#\n" +
				"        channel: raw\n        direction: in\n",
			[]string{"channel:", "--check-config", "Delete the key"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t,
				"broker:\n  id: t\n"+memStorage+bridgeChannels+"bridges:\n"+tc.bridge))
			if err == nil {
				t.Fatal("accepted a configuration written against the old key names")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q, so it does not say what to "+
						"write instead:\n%v", want, err)
				}
			}
		})
	}
}

// **No bridge rule may name the reserved `$saguin/` space, in any
// direction** (invariant 6).
//
// Pointed at another saguin, a rule naming `$saguin/queue/<channel>` is a
// bridge subscribing to that broker's queue: it takes a lease on every job
// it is offered and never sends the application acknowledgement, which is a
// separate publish to `$saguin/queue/<channel>/response` that no bridge
// makes. So each job's lease expires, it redelivers, its attempts are spent
// and it dead-letters - because a *bridge* read it, not because any worker
// failed.
//
// **This is the half `bridge_source` on a queue never covered.** That
// refusal guarded the channel a record lands in, which is the writing side;
// nothing guarded the filter, which is the reading side. Measured before it
// was a rule: a bridge whose only rule subscribed to a far end's queue
// passed `--check-config` without a word.
func TestNoRuleReachesIntoTheReservedSpace(t *testing.T) {
	// **Each rule is otherwise valid**, and that is the point of writing the
	// topic out per case rather than sharing one. A `topic: raw/$#` against
	// a filter with no `#` is refused for the template, not for the space -
	// so with the rule below removed three of these four still failed, and
	// the test would have been reporting on a complaint it was not about.
	for _, tc := range []struct{ name, filter, topic string }{
		{"a queue at the far end", "$saguin/queue/jobs", "raw/jobs"},
		{"a queue's acknowledgements", "$saguin/queue/jobs/response", "raw/acks"},
		{"the root itself", "$saguin", "raw/root"},
		{"a wildcard under it", "$saguin/#", "raw/$#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, bridgeYAML(
				"      - filter: "+tc.filter+"\n        topic: "+tc.topic+"\n")))
			if err == nil {
				t.Fatal("accepted a rule reaching into the reserved space: pointed at a " +
					"saguin this takes queue leases it can never acknowledge")
			}
			if !strings.Contains(err.Error(), "reserved") {
				t.Errorf("the refusal does not say what is wrong: %v", err)
			}
		})
	}

	// The other half, or the rule above would be a rule against every `$`.
	// `$SYS` is the other broker's space and reading it is an ordinary
	// thing to want; saguin refuses the space it defines, not the character.
	t.Run("$SYS at the far end is still allowed", func(t *testing.T) {
		if _, _, err := config.Load(write(t, bridgeYAML(
			"      - filter: $SYS/broker/#\n        topic: raw/$#\n        direction: in\n"))); err != nil {
			t.Errorf("a rule reading the far end's $SYS was refused, and that space is "+
				"not saguin's to claim: %v", err)
		}
	})
}

// The same client id at two different upstreams is fine: the sessions are
// at different brokers and never meet.
func TestOneClientIDAtTwoUpstreams(t *testing.T) {
	const rule = "    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+bridgeChannels+"bridges:\n"+
			"  a:\n    peer: tls://one:8883\n    client_id: v\n"+rule+
			"  b:\n    peer: tls://two:8883\n    client_id: v\n"+rule))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := len(f.BridgeSet()); got != 2 {
		t.Fatalf("got %d bridges, want 2", got)
	}
}

// RFC 0002 "On a bridge, dialling out": **a certificate without a `ca_file` is
// allowed**, which is not the rule a listener follows.
//
// The two keys answer different questions here - the authority checks the
// far end, the pair identifies this one - so an upstream whose certificate a
// public root vouches for, reached with a private client certificate, is an
// ordinary arrangement rather than a mistake. Refusing it would be a rule
// with no failure behind it.
func TestABridgeMayPresentACertificateWithoutNamingAnAuthority(t *testing.T) {
	const rule = "    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+bridgeChannels+"bridges:\n"+
			"  a:\n    peer: tls://h:8883\n    client_id: v\n"+
			"    cert_file: /etc/saguin/tls/bridge.pem\n"+
			"    key_file: /etc/saguin/tls/bridge-key.pem\n"+rule))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b := f.BridgeSet()[0]
	if b.CAFile != "" {
		t.Errorf("ca_file is %q, and the file names none", b.CAFile)
	}
	if b.CertFile != "/etc/saguin/tls/bridge.pem" || b.KeyFile != "/etc/saguin/tls/bridge-key.pem" {
		t.Errorf("the pair did not reach the bridge: cert %q key %q", b.CertFile, b.KeyFile)
	}
}

// Bridges are ordered by name, so two runs of one configuration start them
// in the same order and a log reads the same way twice.
func TestBridgesAreOrdered(t *testing.T) {
	const rule = "    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+bridgeChannels+"bridges:\n"+
			"  zulu:\n    peer: tls://h:8883\n    client_id: z\n"+rule+
			"  alpha:\n    peer: tls://h:8883\n    client_id: a\n"+rule))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	bs := f.BridgeSet()
	if len(bs) != 2 || bs[0].Name != "alpha" || bs[1].Name != "zulu" {
		t.Fatalf("got %v", []string{bs[0].Name, bs[1].Name})
	}
}

// `channels:` written as a list is decoded by a different path from
// `channels:` written as a mapping, and that path decodes with unknown keys
// refused. A top-level key it did not know about would therefore be a hard
// error naming a key the operator was entitled to write.
func TestBridgesSurviveChannelsWrittenAsAList(t *testing.T) {
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+
			"channels:\n"+
			"  - raw:\n      type: append\n"+
			"bridges:\n"+
			"  head-office:\n"+
			"    peer: tls://mqtt.example.com:8883\n"+
			"    client_id: vessel-07\n"+
			"    topics:\n"+
			"      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := len(f.BridgeSet()); got != 1 {
		t.Fatalf("got %d bridges, want 1", got)
	}
}

// A configuration with no bridges block has no bridges, and nothing about
// it changes.
func TestNoBridgesBlock(t *testing.T) {
	f, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := len(f.BridgeSet()); got != 0 {
		t.Fatalf("got %d bridges, want none", got)
	}
}

// An inbound bridge puts no restriction on channel type: it re-publishes
// through the broker's own publish path, so a queue takes work exactly as
// it would from a local client.
func TestInboundMayTargetAQueue(t *testing.T) {
	bs := loadBridge(t, "      - filter: fleet/+/jobs/#\n        topic: jobs/$1/$#\n        direction: in\n")
	got, ok := bs[0].Topics[0].Match("fleet/vessel-07/jobs/build/now")
	if !ok || got != "jobs/vessel-07/build/now" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
}

// Rules are tried in the order they are written and the first match wins,
// so a narrow rule above a broad one reaches the topics it names and the
// broad one takes the rest.
func TestFirstRuleWins(t *testing.T) {
	bs := loadBridge(t,
		"      - filter: fleet/+/telemetry/#\n"+
			"        topic: readings/telemetry/$1/$#\n        direction: in\n"+
			"      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n")
	in := bs[0].Topics
	if len(in) != 2 {
		t.Fatalf("got %d rules", len(in))
	}

	// Both rules cover this topic; the order of the slice is what decides.
	const both = "fleet/vessel-07/telemetry/temp"
	if got, ok := in[0].Match(both); !ok || got != "readings/telemetry/vessel-07/temp" {
		t.Fatalf("first rule: %q ok=%v", got, ok)
	}
	if got, ok := in[1].Match(both); !ok || got != "raw/vessel-07/telemetry/temp" {
		t.Fatalf("second rule: %q ok=%v", got, ok)
	}

	// And only the second reaches this one.
	if _, ok := in[0].Match("fleet/vessel-07/status/x"); ok {
		t.Fatal("the narrow rule matched a topic it does not name")
	}
	if got, ok := in[1].Match("fleet/vessel-07/status/x"); !ok || got != "raw/vessel-07/status/x" {
		t.Fatalf("second rule: %q ok=%v", got, ok)
	}
}

// The block RFC 0002 "Bridges" prints, loaded verbatim, and the three rows
// of the table under it checked against what the code actually produces. A
// documented example that does not load is the drift this is here to catch.
func TestTheBridgesBlockFromRFC0002Loads(t *testing.T) {
	f, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+bridgeChannels+
		"bridges:\n"+
		"  head-office:\n"+
		"    peer: tls://mqtt.example.com:8883\n"+
		"    client_id: vessel-07          # saguin's identity at the far end\n"+
		"    topics:\n"+
		"      - filter: fleet/+/telemetry/#\n"+

		"        topic: readings/telemetry/$1/$#\n        direction: in\n"+
		"      - filter: fleet/+/jobs/#\n"+

		"        topic: jobs/$1/$#\n        direction: in\n"))
	if err != nil {
		t.Fatalf("the block RFC 0002 prints does not load: %v", err)
	}
	bs := f.BridgeSet()
	if len(bs) != 1 || len(bs[0].Topics) != 2 {
		t.Fatalf("got %d bridges", len(bs))
	}

	// The table in that section, row by row.
	const upstream = "fleet/vessel-07/telemetry/hold/psi"
	for _, tc := range []struct{ filter, channel, topic, want string }{
		{"fleet/+/telemetry/#", "readings", "readings/telemetry/$1/$#", "readings/telemetry/vessel-07/hold/psi"},
		{"fleet/+/telemetry/#", "readings", "readings/telemetry/$#", "readings/telemetry/hold/psi"},
		{"fleet/#", "readings", "readings/$#", "readings/vessel-07/telemetry/hold/psi"},
	} {
		bs := loadBridge(t, "      - filter: "+tc.filter+"\n"+
			"        topic: "+tc.topic+"\n        direction: in\n")
		got, ok := bs[0].Topics[0].Match(upstream)
		if !ok || got != tc.want {
			t.Errorf("%s + %s: got %q ok=%v, want %q", tc.filter, tc.topic, got, ok, tc.want)
		}
	}
}

// What one rule costs, because "probably fine" is not a figure. A message
// pays this once per rule until one matches.
func BenchmarkInboundMatch(b *testing.B) {
	var t testing.T
	bs := loadBridge(&t,
		"      - filter: fleet/+/telemetry/#\n        topic: telemetry/$1/$#\n        direction: in\n")
	in := bs[0].Topics[0]
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := in.Match("fleet/vessel-07/telemetry/hold/psi"); !ok {
			b.Fatal("no match")
		}
	}
}

// And what a rule costs when it does not match, which is the figure that
// bounds the whole scan: a message matching nothing is the only one that
// runs every rule, so the worst case is traffic the operator did not ask
// for. It is the cheaper half - no topic is built - which is why the two are
// measured apart rather than averaged into one misleading number.
func BenchmarkInboundMatchMiss(b *testing.B) {
	var t testing.T
	bs := loadBridge(&t,
		"      - filter: fleet/+/telemetry/#\n        topic: telemetry/$1/$#\n        direction: in\n")
	in := bs[0].Topics[0]
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := in.Match("depot/vessel-07/status/hold/psi"); ok {
			b.Fatal("matched")
		}
	}
}

// RFC 0002 "Bridges", "The three keys that tune a link"
//
// The defaults are what the code ran with before these were configurable,
// so a configuration written against the previous release behaves exactly
// as it did. That is the whole of this test: a bridge that says nothing
// gets the three numbers RFC 0002 names.
func TestABridgeThatTunesNothingGetsTheDefaults(t *testing.T) {
	b := loadBridge(t, "      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n")[0]
	if b.SessionExpiry != config.DefaultSessionExpiry {
		t.Errorf("session_expiry defaulted to %v, want %v", b.SessionExpiry, config.DefaultSessionExpiry)
	}
	if b.ReceiveMaximum != config.DefaultReceiveMaximum {
		t.Errorf("receive_maximum defaulted to %d, want %d", b.ReceiveMaximum, config.DefaultReceiveMaximum)
	}
	if b.AckInterval != config.DefaultAckInterval {
		t.Errorf("ack_interval defaulted to %v, want %v", b.AckInterval, config.DefaultAckInterval)
	}
}

func TestTheThreeTuningKeysAreRead(t *testing.T) {
	f, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+bridgeChannels+
		"bridges:\n  head-office:\n    peer: tls://h:8883\n    client_id: v\n"+
		"    session_expiry: 2h\n    receive_maximum: 200\n    ack_interval: 20ms\n"+
		"    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b := f.BridgeSet()[0]
	if b.SessionExpiry != 2*time.Hour {
		t.Errorf("session_expiry is %v, want 2h", b.SessionExpiry)
	}
	if b.ReceiveMaximum != 200 {
		t.Errorf("receive_maximum is %d, want 200", b.ReceiveMaximum)
	}
	if b.AckInterval != 20*time.Millisecond {
		t.Errorf("ack_interval is %v, want 20ms", b.AckInterval)
	}
}

// Each refusal says what the value costs rather than only what the range
// is, because all three are the sort of number somebody raises to make a
// symptom go away.
func TestTheThreeTuningKeysAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name, keys, want string
	}{
		{
			// `none` is a value everywhere else in this file, and MQTT can
			// even express it - 0xFFFFFFFF never expires. saguin declines:
			// that session outlives the box it belonged to on somebody
			// else's broker, and only they can clear it.
			"a session that never expires",
			"    session_expiry: none\n", "a guest at its peer",
		},
		{"a session expiry that is not a duration", "    session_expiry: 3\n", "not a duration"},
		{"a session expiry below a second", "    session_expiry: 500ms\n", "held in whole seconds"},
		{"a session expiry MQTT cannot carry", "    session_expiry: 60000d\n", "136 years"},
		{
			// The one somebody writes meaning "no limit". It means the
			// opposite, and it has to be told apart from an absent key, which
			// is why the field is a pointer: a plain int made a written zero
			// indistinguishable from omission, so it was silently defaulted
			// to 20 while this document said it was refused.
			"a receive maximum of zero",
			"    receive_maximum: 0\n", `not "no limit"`,
		},
		{
			"a negative receive maximum",
			"    receive_maximum: -1\n", "no link at all",
		},
		{"a receive maximum above MQTT's own", "    receive_maximum: 70000\n", "ceiling of 65535"},
		{"an ack interval that is not a duration", "    ack_interval: soon\n", "not a duration"},
		{"an ack interval below a millisecond", "    ack_interval: 0ms\n", "greater than zero"},
		{
			// Past a second a bridge is indistinguishable from one that has
			// stopped, and the message says what the ceiling would become.
			"an ack interval long enough to look like a stopped bridge",
			"    ack_interval: 5s\n", "records a second",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+bridgeChannels+
				"bridges:\n  head-office:\n    peer: tls://h:8883\n    client_id: v\n"+tc.keys+
				"    topics:\n      - filter: fleet/#\n        topic: raw/$#\n        direction: in\n"))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("message does not say what is wrong.\n got: %v\nwant it to contain: %q", err, tc.want)
			}
		})
	}
}

// `ms` is accepted by the duration form and refused by every key that
// cannot honour it, and the refusal is the key's rather than the form's.
//
// The distinction is the whole point. A retention period given in
// milliseconds would be divided into whole seconds and come out as zero,
// and zero means "keep nothing" - so a channel would discard every record
// it was given because somebody wrote a unit this file does not hold. The
// message has to say that, not that `500ms` is not a duration.
func TestMillisecondsAreRefusedByTheKeysThatCannotHoldThem(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{
			"a retention period",
			"broker:\n  id: t\n  storage:\n    default: m\n    default_retention_period: 500ms\n" +
				"    default_retention_bytes: none\n    providers:\n      m:\n        type: memory\n" +
				"        snapshot_dir: none\n" + bridgeChannels,
			"held in whole seconds",
		},
		{
			"a visibility timeout",
			"broker:\n  id: t\n" + memStorage + "channels:\n  jobs:\n    type: queue\n    visibility_timeout: 100ms\n",
			"held in whole seconds",
		},
		{
			"a job expiry",
			"broker:\n  id: t\n" + memStorage + "channels:\n  jobs:\n    type: queue\n    job_expires_after: 250ms\n",
			"held in whole seconds",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.body))
			if err == nil {
				t.Fatal("accepted a duration the key cannot hold")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("message does not say what is wrong.\n got: %v\nwant it to contain: %q", err, tc.want)
			}
		})
	}
}

// **A braced level is expanded into one rule per spelling**, because the
// notation is saguin's own and the far end is an ordinary MQTT broker. A
// brace left in place goes on the wire as one literal level: driven
// against a saguin upstream, subscribing to `fleet/+/{telemetry,alarms}/#`
// is granted and then receives nothing, so the link was configured,
// reported no error, and carried no records at all.
//
// The template is unaffected, which is what makes the expansion safe:
// `$1`, `$2` … number the filter's `+` levels, and a braced level is not
// one of them.
func TestABracedInboundFilterBecomesOneRulePerSpelling(t *testing.T) {
	bs := loadBridge(t,
		"      - filter: \"fleet/+/{telemetry,alarms}/#\"\n"+
			"        topic: readings/$1/$#\n        direction: in\n")
	in := bs[0].Topics
	if len(in) != 2 {
		t.Fatalf("got %d rules from one braced filter, want 2", len(in))
	}
	for _, tc := range []struct{ topic, want string }{
		{"fleet/vessel-07/telemetry/temp", "readings/vessel-07/temp"},
		{"fleet/vessel-07/alarms/fire", "readings/vessel-07/fire"},
	} {
		matched := false
		for _, one := range in {
			if got, ok := one.Match(tc.topic); ok {
				matched = true
				if got != tc.want {
					t.Errorf("%q became %q, want %q", tc.topic, got, tc.want)
				}
				break
			}
		}
		if !matched {
			t.Errorf("%q matched no rule: a spelling of the brace does not reach the "+
				"upstream, which is a link that carries nothing and says nothing",
				tc.topic)
		}
	}
	// And nothing outside the two spellings.
	for _, one := range in {
		if _, ok := one.Match("fleet/vessel-07/status/x"); ok {
			t.Error("a spelling the brace does not name was matched")
		}
	}
}

// Every MQTT rule is asked of what the braces expanded to, not of the
// written form - a brace is opaque to a rule about levels, so asking the
// written form leaves one hole per rule. The finding names both spellings,
// because the expansion says what is wrong and the written form is the
// line to edit.
func TestABracedInboundFilterIsCheckedAfterItExpands(t *testing.T) {
	_, _, err := config.Load(write(t, bridgeYAML(
		"      - filter: \"fleet/#/{telemetry,alarms}\"\n"+
			"        topic: readings/$#\n")))
	if err == nil {
		t.Fatal("a `#` in the middle of a braced filter was accepted")
	}
	for _, want := range []string{"fleet/#/{telemetry,alarms}", "expands to", "last level"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// **One rule's complaint is printed once**, however many spellings its
// braces expanded to. The rule is one line in the file, so the same
// sentence twice with nothing to tell the two apart is a reader looking
// for a second mistake that is not there.
//
// What is wrong with the *filter* does name its spelling, so those stay
// distinct - and both are still reported, because an operator fixing one
// spelling needs to know the other is wrong too.
func TestOneRuleComplainsOnce(t *testing.T) {
	_, _, err := config.Load(write(t, bridgeYAML(
		"      - filter: \"fleet/+/{telemetry,alarms}/#\"\n"+
			"        topic: readings/$1\n        direction: in\n")))
	if err == nil {
		t.Fatal("a template dropping the `#` tail was accepted")
	}
	if n := strings.Count(err.Error(), "does not use `$#`"); n != 1 {
		t.Errorf("the template complaint appears %d times, want 1:\n%v", n, err)
	}

	_, _, err = config.Load(write(t, bridgeYAML(
		"      - filter: \"fleet/#/{telemetry,alarms}\"\n"+
			"        topic: readings/$#\n")))
	if err == nil {
		t.Fatal("a `#` in the middle of a braced filter was accepted")
	}
	for _, want := range []string{"fleet/#/telemetry", "fleet/#/alarms"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the filter complaint does not name %q, and an operator "+
				"fixing one spelling needs to know the other is wrong too: %v",
				want, err)
		}
	}
}

// **`direction:` is required, and the refusal names what the three words
// do.** There is no default, and that is against mosquitto's own shape -
// its `topic` line defaults to `out`. Either default here would be wrong in
// a way nobody could see in the file: `both` is refused on a templated
// rule, so defaulting to it would reject every templated rule for a key
// nobody wrote, and defaulting to `in` or `out` would let an omitted key
// decide whether this broker's records leave the building.
func TestADirectionIsRequiredAndNamed(t *testing.T) {
	for _, tc := range []struct{ name, rule, want string }{
		{
			"omitted",
			"      - filter: fleet/#\n        topic: raw/$#\n",
			"no direction",
		},
		{
			"a word that is not one of the three",
			"      - filter: fleet/#\n        topic: raw/$#\n        direction: inbound\n",
			`direction "inbound" is not one of in, out or both`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, bridgeYAML(tc.rule)))
			if err == nil {
				t.Fatal("accepted a rule with no usable direction")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not say what is wrong.\n got: %v\nwant: %q", err, tc.want)
			}
		})
	}

	// All three words are accepted, or the refusal above would be a rule
	// against every direction rather than against an absent one.
	for _, dir := range []string{"in", "out", "both"} {
		t.Run("direction "+dir, func(t *testing.T) {
			rule := "      - filter: raw/#\n        direction: " + dir + "\n"
			if dir != "both" {
				rule = "      - filter: raw/#\n        topic: raw/$#\n" +
					"        direction: " + dir + "\n"
			}
			bs := loadBridge(t, rule)
			got := bs[0].Topics[0]
			if wantIn := dir != "out"; got.In != wantIn {
				t.Errorf("direction %q has In=%v, want %v", dir, got.In, wantIn)
			}
			if wantOut := dir != "in"; got.Out != wantOut {
				t.Errorf("direction %q has Out=%v, want %v", dir, got.Out, wantOut)
			}
		})
	}
}

// **`both` carries no `topic:`, because it has to map back the same way.**
// A template reordering captures could be inverted and one dropping a level
// could not, so rather than a rule about which templates invert, `both` is
// the identity mapping and anything else is two rules - which is the shape
// mosquitto's prefix pair has, and for the same reason.
func TestBothIsTheIdentityMappingOrItIsTwoRules(t *testing.T) {
	_, _, err := config.Load(write(t, bridgeYAML(
		"      - filter: raw/#\n        topic: raw/$#\n        direction: both\n")))
	if err == nil {
		t.Fatal("accepted `both` with a template, which has no reverse")
	}
	for _, want := range []string{"`both` with a topic:", "two rules"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}

	// And with no template it is the identity: the filter still decides what
	// the rule covers, and a topic it covers maps to itself.
	bs := loadBridge(t,
		"      - filter: raw/+/telemetry\n        direction: both\n")
	in := bs[0].Topics[0]
	if got, ok := in.Match("raw/vessel-07/telemetry"); !ok || got != "raw/vessel-07/telemetry" {
		t.Errorf("an identity rule mapped %q to %q ok=%v, want it unchanged",
			"raw/vessel-07/telemetry", got, ok)
	}
	// The filter still bounds it, or `both` would carry everything.
	if _, ok := in.Match("other/vessel-07/telemetry"); ok {
		t.Error("an identity rule matched a topic its filter does not cover")
	}

	// A one-directional rule still needs its template, or the identity
	// above would be a way to write a rule that says nothing.
	_, _, err = config.Load(write(t, bridgeYAML(
		"      - filter: raw/#\n        direction: in\n")))
	if err == nil {
		t.Fatal("accepted `in` with no topic:")
	}
	if !strings.Contains(err.Error(), "no topic") {
		t.Errorf("the refusal does not name the missing key:\n%v", err)
	}
}

// **Reach is the local end of the rule, and which end that is depends on the
// direction.** An `in` rule's local end is the topic it builds, so the
// template stands for it; an `out` rule's local end is the filter it reads
// this broker with, and the topic it builds is the peer's business.
//
// Written because the implementation used the template for both, which is a
// question about the peer's topic space answered with this broker's channels.
// It is not a corner: an `out` rule must carry a `topic:` - only `both` may
// leave it out - so every outbound rule that rewrites was reported against
// the wrong side. `--check-config` printed "broadcast, because no channel
// claims those topics" for a rule that drains a `latest` channel.
func TestAReachIsTheLocalEndOfTheRule(t *testing.T) {
	yaml := "broker:\n  id: t\n" + memStorage +
		"\nchannels:\n" +
		"  readings:\n    type: append\n    filter: readings/#\n" +
		"  state:\n    type: latest\n    filter: state/+/+/value\n" +
		"  jobs:\n    type: queue\n    filter: work/#\n" +
		"bridges:\n  head-office:\n    peer: tcp://127.0.0.1:1883\n" +
		"    client_id: vessel-07\n    topics:\n" +
		// in: the template lands in readings; the filter matches nothing here.
		"      - filter: fleet/+/telemetry/#\n        topic: readings/$1/$#\n" +
		"        direction: in\n" +
		// out: the filter drains state; the template is the peer's topic.
		"      - filter: state/+/+/value\n        topic: mirror/$1/$2\n" +
		"        direction: out\n" +
		// both: identity, so the two ends are the same string.
		"      - filter: readings/#\n        direction: both\n" +
		// out, across a queue: drained for everything but the queue.
		"      - filter: work/+/urgent\n        topic: peer/$1\n" +
		"        direction: out\n"

	f, reg, err := config.Load(write(t, yaml))
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	rules := f.BridgeSet()[0].Topics

	names := func(cs []*channel.Channel) []string {
		out := make([]string, 0, len(cs))
		for _, c := range cs {
			out = append(out, c.Name)
		}
		sort.Strings(out)
		return out
	}
	for _, tc := range []struct {
		at      int
		what    string
		reaches []string
		crosses []string
	}{
		{0, `in "fleet/+/telemetry/#" -> "readings/$1/$#"`, []string{"readings"}, nil},
		{1, `out "state/+/+/value" -> "mirror/$1/$2"`, []string{"state"}, nil},
		{2, `both "readings/#"`, []string{"readings"}, nil},
		// The queue's dead-letter companion is an ordinary append channel
		// and is reached; the queue itself is crossed and never served.
		{3, `out "work/+/urgent" -> "peer/$1"`, []string{"jobs__dlq"}, []string{"jobs"}},
	} {
		t.Run(tc.what, func(t *testing.T) {
			if got := names(rules[tc.at].Reaches(reg)); !slices.Equal(got, tc.reaches) {
				t.Errorf("reaches %v, want %v - an `in` rule is read off the topic it "+
					"builds here and an `out` rule off the filter it reads here; the "+
					"other end belongs to the peer", got, tc.reaches)
			}
			if got := names(rules[tc.at].Crosses(reg)); !slices.Equal(got, tc.crosses) {
				t.Errorf("crosses %v, want %v", got, tc.crosses)
			}
		})
	}
}

// **A fault about the bridge does not silence the faults about its rules.**
//
// These reported one level at a time: a bridge missing its `client_id` was
// answered with that alone, and the rule still writing `channel:` was not
// mentioned until the operator fixed the first and ran the check again. Two
// runs for one file, against this package's own rule that a pass is whole -
// the same rule that has `upstream:` and `inbound:` reported together.
func TestABridgeFaultDoesNotHideItsRules(t *testing.T) {
	_, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+bridgeChannels+
		"bridges:\n  a:\n    peer: tls://h:8883\n    topics:\n"+
		"      - filter: fleet/#\n        topic: raw/$#\n        channel: raw\n"+
		"        direction: in\n"))
	if err == nil {
		t.Fatal("accepted a bridge with no client_id and a rule writing channel:")
	}
	for _, want := range []string{"no client_id", "channel: is gone"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q, so the operator runs the check "+
				"again to be told the rest:\n%v", want, err)
		}
	}
}

// The replica arrangement is gone, and its three keys are refused by name
// rather than by the decoder naming a Go type - the same treatment
// `upstream:`, `inbound:` and `channel:` were given, for the same reason:
// every deployment built on it has these keys.
func TestTheReplicaKeysAreRefusedByName(t *testing.T) {
	_, _, err := config.Load(write(t, "broker:\n  id: t\n  replica: true\n"+memStorage+
		"channels:\n  events:\n    type: append\n    bridge_source: head-office\n"+
		"    dlq_bridge_source: head-office\n"))
	if err == nil {
		t.Fatal("accepted a configuration written for the replica arrangement")
	}
	// All three in one pass: an operator holding such a file should not
	// have to run the check three times to be told it three times.
	for _, want := range []string{
		"broker.replica is gone", "bridge_source is gone", "dlq_bridge_source is gone",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "not found in type") {
		t.Errorf("the decoder answered with a Go type name, which is what keeping these "+
			"fields exists to prevent:\n%v", err)
	}

	// `replica: false` was the honest way to write "not a copy", and it is
	// told the same thing: the word no longer means anything either way.
	_, _, err = config.Load(write(t, "broker:\n  id: t\n  replica: false\n"+memStorage+
		"channels:\n  events:\n    type: append\n"))
	if err == nil || !strings.Contains(err.Error(), "broker.replica is gone") {
		t.Errorf("replica: false was not refused by name: %v", err)
	}
}

// **An `acl_file` asks for something that names a client, and a client
// certificate is such a thing.**
//
// A certificate client is named by its Common Name and is deliberately not
// looked up in the password file, so requiring one anyway made a
// pure-certificate estate write a file authenticating nobody in order to
// have any authorization - a decoy, which is the exact shape the refusal
// exists to prevent.
func TestAnACLFileIsSatisfiedByAClientAuthority(t *testing.T) {
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(acl, []byte(
		"roles:\n  device:\n    - channel: events\n      allow: [write, read]\n"+
			"users:\n  \"sensor-*\": [device]\n"), 0o600); err != nil {
		t.Fatalf("write the acl file: %v", err)
	}
	crt, key, ca := filepath.Join(dir, "s.crt"), filepath.Join(dir, "s.key"), filepath.Join(dir, "ca.crt")
	for _, f := range []string{crt, key, ca} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}
	base := "broker:\n  id: t\n  mqtt:\n    acl_file: " + acl + "\n    listen:\n" +
		"      tcp:\n        address: 127.0.0.1:1883\n        tls:\n" +
		"          cert_file: " + crt + "\n          key_file: " + key + "\n"
	tail := memStorage + "channels:\n  events:\n    type: append\n"

	// With a client authority: accepted, no password file anywhere.
	if _, _, err := config.Load(write(t, base+"          client_ca_file: "+ca+"\n"+tail)); err != nil {
		t.Errorf("an acl_file with mutual TLS and no password file was refused, so a "+
			"certificate-only estate must write a password file that authenticates "+
			"nobody to have any authorization at all: %v", err)
	}

	// Without one, and with no password file either: still refused, and the
	// refusal names both ways out - or this test would pass on a check that
	// had simply been deleted.
	_, _, err := config.Load(write(t, base+tail))
	if err == nil {
		t.Fatal("an acl_file was accepted with nothing to authenticate a client")
	}
	for _, want := range []string{"password_file", "client_ca_file"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q as a way out:\n%v", want, err)
		}
	}
}

// **A door that admits a client nothing identifies, beside an acl_file, is
// named** (RFC 0002 "Authorization"). Such a client's identity is the empty
// name, which only a `*` pattern matches, and none of the three shapes below
// needs anybody to write allow_anonymous: a pure-certificate estate's Unix
// socket, a plain ws door, a tcp door that verifies a certificate without
// requiring one. Written allow_anonymous is refused at load, before this
// question. Each arm names exactly the doors it should, so a predicate that
// named every door, or none, fails here.
func TestEveryDoorAdmittingAClientNothingIdentifiesBesideAnACLFileIsNamed(t *testing.T) {
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(acl, []byte(
		"roles:\n  device:\n    - channel: events\n      allow: [write, read]\n"+
			"users:\n  \"sensor-*\": [device]\n"), 0o600); err != nil {
		t.Fatalf("write the acl file: %v", err)
	}
	crt, key, ca := filepath.Join(dir, "s.crt"), filepath.Join(dir, "s.key"), filepath.Join(dir, "ca.crt")
	for _, f := range []string{crt, key, ca} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}
	mutualTCP := "      tcp:\n        address: 127.0.0.1:1883\n        tls:\n" +
		"          cert_file: " + crt + "\n          key_file: " + key + "\n" +
		"          client_ca_file: " + ca + "\n"
	tail := memStorage + "channels:\n  events:\n    type: append\n"

	for _, tc := range []struct {
		why, mqtt string
		want      []string
	}{
		{"mutual TLS only: every client is named by its certificate",
			"    acl_file: " + acl + "\n    listen:\n" + mutualTCP, nil},
		{"a pure-certificate estate's Unix socket",
			"    acl_file: " + acl + "\n    listen:\n" + mutualTCP +
				"      unix:\n        path: /run/saguin/saguin.sock\n", []string{"unix"}},
		{"a plain ws door beside it",
			"    acl_file: " + acl + "\n    listen:\n" + mutualTCP +
				"      ws:\n        address: 127.0.0.1:8083\n", []string{"ws"}},
		{"a certificate verified but not required, and no password file",
			"    acl_file: " + acl + "\n    listen:\n" +
				strings.Replace(mutualTCP, "          client_ca_file:", "          require_certificate: false\n          client_ca_file:", 1),
			[]string{"tcp"}},
		{"a password file: nobody is admitted without a name",
			"    acl_file: " + acl + "\n    password_file: " + ca + "\n    listen:\n" +
				"      tcp:\n        address: 127.0.0.1:1883\n" +
				"      ws:\n        address: 127.0.0.1:8083\n", nil},
		{"no acl_file: there is nothing for the empty name to be granted by",
			"    listen:\n      tcp:\n        address: 127.0.0.1:1883\n", nil},
	} {
		t.Run(tc.why, func(t *testing.T) {
			f, _, err := config.Load(write(t, "broker:\n  id: t\n  mqtt:\n"+tc.mqtt+tail))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := f.Broker.MQTT.AnonymousBesideACL(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("named %q, want %q", got, tc.want)
			}
		})
	}
}

// Each outbound rule keeps its own position, named by its filter and its
// topic (RFC 0002 "Bridges"), so two outbound rules with one filter and one
// topic would share a position - two drains saving one number, the loss the
// per-rule position exists to prevent - and would send every record to the
// same topic at the peer twice. They are refused, a braced spelling meeting
// a written one included; one filter with two topics is two rules and loads.
func TestTwoOutboundRulesWithOneFilterAndOneTopicAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name, rules string
	}{
		{"written twice", "      - filter: events/#\n        topic: up/$#\n        direction: out\n" +
			"      - filter: events/#\n        topic: up/$#\n        direction: out\n"},
		{"a braced spelling meets a written one", "      - filter: events/{a,b}/#\n        direction: both\n" +
			"      - filter: events/a/#\n        direction: both\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, bridgeYAML(tc.rules)))
			if err == nil {
				t.Fatal("two outbound rules with one filter and one topic were accepted")
			}
			if !strings.Contains(err.Error(), "repeats topics[0], so every record would be sent to the peer twice") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}

	got := loadBridge(t, "      - filter: events/#\n        topic: up/a/$#\n        direction: out\n"+
		"      - filter: events/#\n        topic: up/b/$#\n        direction: out\n")
	if len(got) != 1 || len(got[0].Topics) != 2 {
		t.Fatalf("one filter with two topics did not load as two rules: %+v", got)
	}
}

// A `both` rule on a `$` filter can never deliver: it maps a topic to itself,
// so what it brings in would be published here as a `$` topic, which no
// publisher may write - every record refused, dropped and logged twice, for
// the life of the bridge, from a line that loaded cleanly. It is refused at load, saying so; `$SYS/#`
// carried `in` under a template still loads, which is what RFC 0002 allows
// the filter for.
func TestABothRuleOnADollarFilterIsRefused(t *testing.T) {
	_, _, err := config.Load(write(t, bridgeYAML("      - filter: $SYS/#\n        direction: both\n")))
	if err == nil {
		t.Fatal("a `both` rule on $SYS/# was accepted, and it can never deliver a record")
	}
	if !strings.Contains(err.Error(), "nothing may publish a topic beginning with `$` here") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	if got := loadBridge(t, "      - filter: $SYS/#\n        topic: peer/sys/$#\n        direction: in\n"); len(got) != 1 {
		t.Fatalf("`in` on $SYS/# with a template did not load: %+v", got)
	}
}

// **A bridge rule never panics on a topic a peer sends**. A `both` rule is an identity rule, and its branch read the
// topic's first byte without asking whether there was one; `#` and `+`
// match the empty topic, and a peer's PUBLISH can carry one, so the
// bridge's worker panicked and the broker exited. The templated branch
// already guarded its twin. Every kind of rule is held to it here, identity
// and templated, over arbitrary topics: Match and Reserved never panic, an
// identity rule answers with the topic itself or drops it, and nothing a
// rule builds is in the reserved space.
func FuzzABridgeRuleNeverPanics(f *testing.F) {
	path := filepath.Join(f.TempDir(), "saguin.yaml")
	body := bridgeYAML("      - filter: \"#\"\n        direction: both\n" +
		"      - filter: \"+\"\n        direction: both\n" +
		"      - filter: fleet/+/#\n        direction: both\n" +
		"      - filter: \"#\"\n        topic: raw/$#\n        direction: in\n" +
		"      - filter: fleet/#\n        topic: $#\n        direction: in\n" +
		"      - filter: +/+\n        topic: x/$2/$1\n        direction: in\n")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		f.Fatal(err)
	}
	cfg, _, err := config.Load(path)
	if err != nil {
		f.Fatalf("load: %v", err)
	}
	rules := cfg.BridgeSet()[0].Topics
	if len(rules) != 6 {
		f.Fatalf("%d rules compiled, want 6, so the fuzzing below is not about the rules it names", len(rules))
	}
	for _, s := range []string{"", "/", "$", "$x", "fleet", "fleet/", "fleet/a", "a/b", "//", "+", "#"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, topic string) {
		for i, r := range rules {
			// A rule that covers a topic and cannot place it answers "" and
			// true: covered, and dropped.
			got, ok := r.Match(topic)
			_ = r.Reserved(topic)
			if !ok || got == "" {
				continue
			}
			if got[0] == '$' {
				t.Fatalf("rule %d on %q built %q, in the reserved space", i, topic, got)
			}
			if i < 3 && got != topic {
				t.Fatalf("identity rule %d mapped %q to %q", i, topic, got)
			}
		}
	})
}

// RFC 0002 "How deep a topic may be": a filter in the configuration deeper
// than limits.max_topic_levels could match nothing a client may publish, so
// the file is refused and the finding names where. At the bound it loads;
// one level past it does not - a channel's filter, a bridge rule's filter
// and its topic: template, and the key itself below 1.
func TestTheConfigurationIsHeldToMaxTopicLevels(t *testing.T) {
	deep := func(first string, n int) string { return first + strings.Repeat("/x", n-1) }
	channelDoc := func(filter, limits string) string {
		return "broker:\n  id: t\n" + limits + memStorage + "channels:\n  deep:\n    type: append\n    filter: " + filter + "\n"
	}
	rule := func(filter, topic string) string {
		return bridgeYAML("      - filter: " + filter + "\n        topic: " + topic + "\n        direction: in\n")
	}
	for _, tc := range []struct {
		name, doc, want string // want "" loads
	}{
		{"a channel filter of 200 levels", channelDoc(deep("deep", 199)+"/#", ""), ""},
		{"a channel filter of 201 levels", channelDoc(deep("deep", 200)+"/#", ""), `channel "deep": filter`},
		{"a channel filter of 3 levels under a bound of 3", channelDoc("deep/x/#", "  limits:\n    max_topic_levels: 3\n"), ""},
		{"a channel filter of 4 levels under a bound of 3", channelDoc("deep/x/y/#", "  limits:\n    max_topic_levels: 3\n"), "more than limits.max_topic_levels allows (3)"},
		{"a bridge filter of 200 levels", rule(deep("fleet", 199)+"/#", "raw/$#"), ""},
		{"a bridge filter of 201 levels", rule(deep("fleet", 200)+"/#", "raw/$#"), "filter"},
		{"a bridge template of 200 levels", rule("fleet/#", deep("raw", 199)+"/$#"), ""},
		{"a bridge template of 201 levels", rule("fleet/#", deep("raw", 200)+"/$#"), "topic"},
		{"max_topic_levels 0", channelDoc("deep/#", "  limits:\n    max_topic_levels: 0\n"), "limits.max_topic_levels 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.doc))
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.want != "" && err == nil:
				t.Fatalf("loaded, want a finding holding %q", tc.want)
			case tc.want != "" && (!strings.Contains(err.Error(), tc.want) ||
				(tc.name != "max_topic_levels 0" && !strings.Contains(err.Error(), "limits.max_topic_levels"))):
				t.Fatalf("refused with %v, want a finding holding %q that names limits.max_topic_levels", err, tc.want)
			}
		})
	}
}
