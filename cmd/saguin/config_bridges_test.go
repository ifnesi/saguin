package main

import (
	"fmt"
	"slices"
	"strings"
)

// The bridges (config.md: the bridges.<name> table, its Values paragraph
// and the Dropped rows for a bridge's keys; design.md, Bridges and
// Operations). Refusals name the key by its full path:
// "bridges.<name>.<key>", "bridges.<name>.tls.<key>",
// "bridges.<name>.topics[<i>].<key>" (the rule's index from 0), a rule
// itself "bridges.<name>.topics[<i>]", a name "bridges.<name>". A rule
// between two bridges or two rules refuses the later, naming the earlier.
// --output writes bridges as one mapping keyed by name, in the order
// written, each with its keys.

// The peer and files the bridge cases name.
const (
	peerURL  = "tcp://peer.example:1883"
	peerCA   = dirToken + "/peer-ca.crt"
	peerCert = dirToken + "/peer.crt"
	peerKey  = dirToken + "/peer.key"
	peerPass = dirToken + "/peer.pass"
	peerPair = "cert_file: " + peerCert + ", key_file: " + peerKey
)

// baseBridge is the smallest valid bridge, as key/value pairs of flow YAML:
// the keys config.md's bridges.<name> table marks required, with one in
// rule.
var baseBridge = []string{
	"peer", fmt.Sprintf("%q", peerURL),
	"client_id", "edge-1",
	"topics", `[{filter: "a/#", topic: "x/$#", direction: in}]`,
}

// bridge is the YAML text of a bridges mapping holding one bridge, b:
// baseBridge with each key/value pair of pairs written over it, absent
// removing the key. A key baseBridge does not hold is written before
// baseBridge's, so a reader cannot depend on meeting peer first.
func bridge(pairs ...string) string {
	return " {b: " + bridgeKeys(pairs...) + "}"
}

// bridgeKeys is the flow mapping bridge writes for one bridge.
func bridgeKeys(pairs ...string) string {
	values := make(map[string]string, len(baseBridge)/2+len(pairs)/2)
	var order, added []string
	for i := 0; i < len(baseBridge); i += 2 {
		order = append(order, baseBridge[i])
		values[baseBridge[i]] = baseBridge[i+1]
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		if _, ok := values[pairs[i]]; !ok {
			added = append(added, pairs[i])
		}
		values[pairs[i]] = pairs[i+1]
	}
	var keys []string
	for _, key := range slices.Concat(added, order) {
		if values[key] != absent {
			keys = append(keys, key+": "+values[key])
		}
	}
	return "{" + strings.Join(keys, ", ") + "}"
}

// rule is one rule's flow mapping: direction, filter and, unless absent,
// topic, then the key/value pairs of more.
func rule(direction, filter, topic string, more ...string) string {
	keys := []string{fmt.Sprintf("filter: %q", filter)}
	if topic != absent {
		keys = append(keys, fmt.Sprintf("topic: %q", topic))
	}
	keys = append(keys, "direction: "+direction)
	for i := 0; i+1 < len(more); i += 2 {
		keys = append(keys, more[i]+": "+more[i+1])
	}
	return "{" + strings.Join(keys, ", ") + "}"
}

// rules is a topics list of the rules given.
func rules(each ...string) string { return "[" + strings.Join(each, ", ") + "]" }

// topics is bridge with its topics list holding the rules given.
func topics(each ...string) string { return bridge("topics", rules(each...)) }

// printedRule is how --output prints a rule: its filter as configured,
// {a,b} kept, its topic on in and out rules, its direction and its qos,
// "default 1" (config.md, bridges.<name>).
func printedRule(direction, filter, topic string, qos ...string) string {
	q := "1"
	if len(qos) > 0 {
		q = qos[0]
	}
	if topic == absent {
		return fmt.Sprintf("{filter: %q, direction: %s, qos: %s}", filter, direction, q)
	}
	return fmt.Sprintf("{filter: %q, topic: %q, direction: %s, qos: %s}", filter, topic, direction, q)
}

// printedBridge is how --output prints baseBridge under the table's
// defaults: username, password_file and tls "none" (tls written on a tls://
// or wss:// peer only), session_expiry "1d", receive_maximum 20,
// ack_interval "5ms"; each key/value pair of pairs replacing its key's
// value.
func printedBridge(pairs ...string) string {
	keys := []string{
		"peer", fmt.Sprintf("%q", peerURL),
		"client_id", "edge-1",
		"username", "none",
		"password_file", "none",
		"session_expiry", "1d",
		"receive_maximum", "20",
		"ack_interval", "5ms",
		"topics", "[" + printedRule("in", "a/#", "x/$#") + "]",
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		j := slices.Index(keys, pairs[i])
		if j < 0 || j%2 != 0 {
			keys = slices.Insert(keys, len(keys)-2, pairs[i], pairs[i+1])
			continue
		}
		keys[j+1] = pairs[i+1]
	}
	if peer := keys[1]; (strings.Contains(peer, "tls://") || strings.Contains(peer, "wss://")) && !slices.Contains(pairs, "tls") {
		keys = slices.Insert(keys, len(keys)-2, "tls", "none")
	}
	return flow(keys)
}

// bridgesAre is the want of a case whose bridges --output prints as each
// name/bridge pair, exactly, in their order.
func bridgesAre(pairs ...string) []string { return []string{"bridges", flow(pairs)} }

// bridgesAs is a case of the bridges text, over the required keys and sets,
// whose bridges --output prints as each name/bridge pair.
func bridgesAs(name, text string, sets []string, pairs ...string) printed {
	return printed{name: name, configuration: file(slices.Concat(sets, []string{"bridges", text})...), want: bridgesAre(pairs...)}
}

// oneBridge is a case of bridge b with pairs, printed as printedBridge
// prints the pairs given there.
func oneBridge(name string, pairs []string, printed ...string) printed {
	return bridgesAs(name, bridge(pairs...), nil, "b", printedBridge(printed...))
}

// ruled is a case of one bridge b holding the rules written, printed as
// the rules given.
func ruled(name, text string, sets []string, printedRules ...string) printed {
	return bridgesAs(name, text, sets, "b", printedBridge("topics", "["+strings.Join(printedRules, ", ")+"]"))
}

// bridgeDefaults: each key's Default column in config.md's bridges.<name>
// table, for a bridge that writes only its required keys (peer, client_id,
// topics), and a rule's qos, "default 1" (config.md, bridges.<name>); bridges
// kept
// in the order written. With no bridges written, there are none
// (requiredDefaults).
var bridgeDefaults = []printed{
	bridgesAs("the required keys alone", bridge(), nil, "b", printedBridge()),
	// Not sorted: the order written. Two client ids, as one peer takes each
	// once.
	bridgesAs("two bridges", " {zeta: "+bridgeKeys("client_id", "z")+", alpha: "+bridgeKeys("client_id", "a")+"}", nil,
		"zeta", printedBridge("client_id", "z"), "alpha", printedBridge("client_id", "a")),
}

// bridgeValues: each value a bridge's key takes, and how --output prints
// the bridge: config.md's bridges.<name> rows and its Values paragraph.
var bridgeValues = [][]string{
	// session_expiry: a duration, "at most 4,294,967,295 seconds"; "every
	// other duration is at least 1s".
	{"session_expiry", "1s"},
	{"session_expiry", "90m"},
	{"session_expiry", "4294967295s"},
	{"session_expiry", "49710d"},
	// receive_maximum: "1 to 65,535".
	{"receive_maximum", "1"},
	{"receive_maximum", "65535"},
	// ack_interval: "at most 1s"; Values: "a bridge's ack_interval also
	// take[s] ms".
	{"ack_interval", "1ms"},
	{"ack_interval", "250ms"},
	{"ack_interval", "1000ms"},
	{"ack_interval", "1s"},
	// username: "none" by default, so none is accepted (Values).
	{"username", "saguin"},
	{"username", "none"},
	// password_file: "The file holds the password alone. One trailing line
	// ending (\n or \r\n) is not counted. This key requires username."; none,
	// its default.
	{"username", "saguin", "password_file", peerPass},
	{"username", "saguin", "password_file", "none"},
}

// ruleCase is one rule, alone in bridge b, and its outcome: refused naming
// bridges.b.topics[0].<refused>; else printed as written.
type ruleCase struct {
	direction, filter, topic string
	refused                  string
}

// ruleCases splits cases into those accepted and those refused.
func ruleCases(cases []ruleCase) (accepted []printed, refused []refusal) {
	for _, tc := range cases {
		name := fmt.Sprintf("%s %q %q", tc.direction, tc.filter, tc.topic)
		text := topics(rule(tc.direction, tc.filter, tc.topic))
		if tc.refused != "" {
			refused = append(refused, refuse(name, "bridges.b.topics[0]."+tc.refused, file("bridges", text)))
			continue
		}
		accepted = append(accepted, ruled(name, text, nil, printedRule(tc.direction, tc.filter, tc.topic)))
	}
	return accepted, refused
}

// bridgeFilters: config.md, bridges.<name>: a rule's "A filter is never none,
// $share or under $saguin/. On an out rule it is also never under $SYS/.
// Braces such as {a,b} follow a channel filter's rules (Channels)"; "The
// both direction is refused on a $ filter"; bridges.<name>.topics:
// "direction (in, out or both)".
// A filter is an MQTT topic filter: + and # each a whole level, # only last,
// no U+0000 (MQTT 5 4.7.1, 1.5.4), an empty level allowed. config.md, a
// channel's filter: "a level holding { or } is exactly one {a,b} [...]
// offering alternatives: at least two, each a non-empty written level with no
// {, } or ,, none repeated" (a comma in a level holding neither is the
// level's own). A channel's other filter rules (its first level, __dlq) are
// not a bridge's.
var bridgeFilters = []ruleCase{
	{direction: "in", filter: "a/+/b", topic: "x/$1"},
	{direction: "in", filter: "#", topic: "x/$#"},
	{direction: "in", filter: "+", topic: "x/$1"},
	{direction: "in", filter: "a//b", topic: "x/y"},
	{direction: "both", filter: "a/b", topic: absent},
	{direction: "both", filter: "#", topic: absent},
	// $SYS/ only on in; a $ filter otherwise, in or out.
	{direction: "in", filter: "$SYS/#", topic: "peer/sys/$#"},
	{direction: "in", filter: "$SYS/broker/+", topic: "sys/$1"},
	{direction: "out", filter: "$app/#", topic: "x/$#"},
	{direction: "in", filter: "$app/#", topic: "x/$#"},
	// {a,b}, the first level's included; a comma in an ordinary level.
	{direction: "in", filter: "{a,b}/#", topic: "x/$#"},
	{direction: "out", filter: "fleet/{north,south}/+", topic: "remote/$1"},
	{direction: "both", filter: "x/{a,b}/{c,d}", topic: absent},
	{direction: "out", filter: "x/{b,a}/+", topic: "y/$1"},
	{direction: "in", filter: "x/a,b", topic: "y"},
	{direction: "out", filter: "a,b/c", topic: "y"},

	// An MQTT topic filter.
	{direction: "in", filter: "a/#/b", topic: "y", refused: "filter"},
	{direction: "in", filter: "a/b#", topic: "y", refused: "filter"},
	{direction: "in", filter: "a/#b", topic: "y", refused: "filter"},
	{direction: "out", filter: "a+", topic: "y", refused: "filter"},
	{direction: "in", filter: "a/+b", topic: "y", refused: "filter"},
	{direction: "in", filter: "a/b\x00c", topic: "y", refused: "filter"},
	// Never $share or under $saguin/.
	{direction: "in", filter: "$share/g/a/#", topic: "x/$#", refused: "filter"},
	{direction: "out", filter: "$share/g/a/#", topic: "x/$#", refused: "filter"},
	{direction: "in", filter: "$saguin/#", topic: "x/$#", refused: "filter"},
	{direction: "out", filter: "$saguin/queue/jobs", topic: "x", refused: "filter"},
	// Nor under $SYS/ on an out rule.
	{direction: "out", filter: "$SYS/#", topic: "x/$#", refused: "filter"},
	{direction: "out", filter: "$SYS/broker/+", topic: "x/$1", refused: "filter"},
	// both refused on a $ filter.
	{direction: "both", filter: "$SYS/#", topic: absent, refused: "direction"},
	{direction: "both", filter: "$app/x", topic: absent, refused: "direction"},
	{direction: "both", filter: "$x", topic: absent, refused: "direction"},
	// {a,b}: each alternative a written level, non-empty, with no {, } or
	// ',', at least two, none repeated.
	{direction: "in", filter: "x/{a,+}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a,#}", topic: "y", refused: "filter"},
	{direction: "out", filter: "{+,a}/x", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a,a}", topic: "y", refused: "filter"},
	{direction: "out", filter: "x/{a,b,a}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a,}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{,a}", topic: "y", refused: "filter"},
	{direction: "both", filter: "x/{a,,b}", topic: absent, refused: "filter"},
	{direction: "in", filter: "x/{a,{b,c}}", topic: "y", refused: "filter"},
	// A level holding { or } is exactly one {a,b}.
	{direction: "in", filter: "x/a{b,c}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{b,c}d", topic: "y", refused: "filter"},
	{direction: "out", filter: "x/{a,b}}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a,b", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/a}", topic: "y", refused: "filter"},
	{direction: "in", filter: "x/{a}{b}", topic: "y", refused: "filter"},
	{direction: "both", filter: "x/{a,b}{c,d}", topic: absent, refused: "filter"},
	{direction: "in", filter: "x/}a,b{", topic: "y", refused: "filter"},
}

// bridgeTopics: design.md, Bridges: "In a topic template, $1, $2 and so on
// stand for the filter's + levels. Each must name a level the filter has and
// may occur anywhere in a level. The $# stands for the filter's # and must be
// the topic's whole last level"; config.md, bridges.<name>: "The template
// contains no + or #, or leading or trailing /. It is never under $share/ or
// $saguin/. On an in rule it is also never under $SYS/". An empty level between two others is neither
// leading nor trailing; a {a,b} level is a written level (Words), not a +
// level; a topic under $SYS/ on an out rule is not refused. $n is written as
// "$1, $2…" are, so one with a leading zero ($01) is refused (agreed with the
// maintainer, audit 261015-01). Values: a key written empty is refused.
var bridgeTopics = []ruleCase{
	{direction: "in", filter: "a/+/b/+", topic: "x/$2/$1"},
	{direction: "in", filter: "a/+/b/+", topic: "x/$1"},
	{direction: "in", filter: "+", topic: "$1"},
	{direction: "in", filter: "a/#", topic: "$#"},
	{direction: "in", filter: "a/+/#", topic: "b/$1/$#"},
	{direction: "in", filter: "x/{a,b}/+/y", topic: "z/$1"},
	{direction: "in", filter: "a/b", topic: "x/y"},
	{direction: "out", filter: "a/+", topic: "x/$1"},
	{direction: "out", filter: "a/b", topic: "$SYS/x"},
	// $n anywhere in a level.
	{direction: "in", filter: "a/+", topic: "sensor-$1"},
	{direction: "out", filter: "+/+", topic: "x/$1-$2/y"},
	{direction: "in", filter: "a/+/#", topic: "pre-$1-post/$#"},
	// An empty middle level.
	{direction: "in", filter: "a/b", topic: "x//y"},
	// $n names the filter's nth + level, n written as $1, $2…, so with ten
	// + levels $10 is the tenth, never $1 followed by a 0.
	{direction: "in", filter: "+/+/+/+/+/+/+/+/+/+", topic: "x/$10"},
	{direction: "out", filter: "+/+/+/+/+/+/+/+/+/+", topic: "x/$10/$1"},

	// Each $n naming one of the filter's + levels.
	{direction: "in", filter: "a/+/b/+", topic: "x/$3", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "x/$1", refused: "topic"},
	{direction: "in", filter: "a/+", topic: "x/$0", refused: "topic"},
	{direction: "out", filter: "x/{a,b}/+", topic: "y/$2", refused: "topic"},
	{direction: "in", filter: "a/+/+", topic: "sensor-$3", refused: "topic"},
	{direction: "out", filter: "a/+", topic: "x/a-$2-b", refused: "topic"},
	// $1, $2… are written without a leading zero: $01 is not $1.
	{direction: "in", filter: "a/+", topic: "$01", refused: "topic"},
	{direction: "out", filter: "a/+", topic: "$001", refused: "topic"},
	{direction: "in", filter: "a/+", topic: "x/$01", refused: "topic"},
	{direction: "out", filter: "a/+/+", topic: "sensor-$02", refused: "topic"},
	{direction: "in", filter: "+/+/+/+/+/+/+/+/+/+", topic: "x/$010", refused: "topic"},
	// $# for the filter's #, as the topic's whole last level.
	{direction: "in", filter: "a/b", topic: "x/$#", refused: "topic"},
	{direction: "in", filter: "a/+", topic: "x/$#", refused: "topic"},
	{direction: "in", filter: "a/#", topic: "$#/x", refused: "topic"},
	{direction: "out", filter: "a/#", topic: "x/$#/y", refused: "topic"},
	{direction: "in", filter: "a/#", topic: "x$#", refused: "topic"},
	{direction: "out", filter: "a/#", topic: "x/a$#", refused: "topic"},
	// No +, # or leading or trailing /.
	{direction: "in", filter: "a/b", topic: "x/+", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "x/#", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "+", refused: "topic"},
	{direction: "out", filter: "a/b", topic: "#", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "x/a+b", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "/x", refused: "topic"},
	{direction: "out", filter: "a/b", topic: "x/", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "/", refused: "topic"},
	// Never under $share/ or $saguin/.
	{direction: "in", filter: "a/b", topic: "$share/g/x", refused: "topic"},
	{direction: "out", filter: "a/b", topic: "$share/g/x", refused: "topic"},
	{direction: "in", filter: "a/b", topic: "$saguin/x", refused: "topic"},
	{direction: "out", filter: "a/b", topic: "$saguin/queue/x", refused: "topic"},
	// Nor under $SYS/ on an in rule.
	{direction: "in", filter: "a/b", topic: "$SYS/x", refused: "topic"},
	{direction: "in", filter: "a/#", topic: "$SYS/$#", refused: "topic"},
	// Values: written empty.
	{direction: "in", filter: "a/b", topic: "", refused: "topic"},
}

var (
	filtersAccepted, filtersRefused = ruleCases(bridgeFilters)
	topicsAccepted, topicsRefused   = ruleCases(bridgeTopics)
)

// tlsPeer and wssPeer are the peers a tls block belongs on.
const tlsPeer, wssPeer = `"tls://peer.example:8883"`, `"wss://peer.example:443/mqtt"`

// twoBridges is a bridges mapping of first and second, each with its own
// pairs over baseBridge.
func twoBridges(first, second []string) string {
	return fmt.Sprintf(" {first: %s, second: %s}", bridgeKeys(first...), bridgeKeys(second...))
}

// peerAndID is a bridge's peer and client_id pairs.
func peerAndID(peer, id string) []string {
	return []string{"peer", fmt.Sprintf("%q", peer), "client_id", id}
}

// inRule is a case of one in rule, under limits, printed as written.
func inRule(name, filter, topic string, limits []string) printed {
	return ruled(name, topics(rule("in", filter, topic)), limits, printedRule("in", filter, topic))
}

var bridgeAccepted = slices.Concat(
	each(bridgeValues, func(pairs []string) printed { return oneBridge(bridge(pairs...), pairs, pairs...) }),
	// Names: "A bridge name is 1 to 128 bytes with no whitespace": bytes, not
	// characters.
	each([]string{"a", "up-stream.1", "日本", "a/b", "$x", "a#b", strings.Repeat("n", 128), strings.Repeat("é", 64)}, func(name string) printed {
		return bridgesAs("accepted "+name, fmt.Sprintf(" {%q: %s}", name, bridgeKeys()), nil, fmt.Sprintf("%q", name), printedBridge())
	}),
	// peer: "the peer's URL: tcp://, tls://, ws:// or wss://, with a host
	// as in a listener's address, never empty, and a port from 1 to 65,535;
	// ws:// and wss:// may add a path (default /)". The path's default is
	// filled in, as every default is (design.md, Operations, --output).
	each([][2]string{
		{"tcp://peer.example:1883", "tcp://peer.example:1883"},
		{"tcp://Peer.Example:1883", "tcp://Peer.Example:1883"},
		{"tls://10.0.0.1:8883", "tls://10.0.0.1:8883"},
		{"tcp://[::1]:1883", "tcp://[::1]:1883"},
		{"tls://[2001:db8::1]:65535", "tls://[2001:db8::1]:65535"},
		{"ws://peer.example:8080", "ws://peer.example:8080/"},
		{"ws://peer.example:8080/", "ws://peer.example:8080/"},
		{"ws://peer.example:8080/mqtt", "ws://peer.example:8080/mqtt"},
		{"wss://peer.example:443", "wss://peer.example:443/"},
		{"wss://peer.example:443/mqtt/v5", "wss://peer.example:443/mqtt/v5"},
		{"tcp://peer_1.example:1", "tcp://peer_1.example:1"},
		{"tcp://localhost:1883", "tcp://localhost:1883"},
		{"tcp://edge-2.peer.example:1883", "tcp://edge-2.peer.example:1883"},
		{"tcp://123.example:1883", "tcp://123.example:1883"},
		{"tcp://" + strings.Repeat("a", 63) + ".example:1883", "tcp://" + strings.Repeat("a", 63) + ".example:1883"},
	}, func(p [2]string) printed {
		return oneBridge("accepted "+p[0], []string{"peer", fmt.Sprintf("%q", p[0])}, "peer", fmt.Sprintf("%q", p[1]))
	}),
	[]printed{
		// tls (tls, wss): "ca_file, the authority for the peer's certificate
		// (default: the system's roots); cert_file and key_file, Sagüin's
		// certificate for the peer, written together; min_version, 1.2
		// (default) or 1.3".
		oneBridge("ca_file", []string{"peer", tlsPeer, "tls", "{ca_file: " + peerCA + "}"},
			"peer", tlsPeer, "tls", `{ca_file: `+peerCA+`, min_version: "1.2"}`),
		oneBridge("a certificate", []string{"peer", tlsPeer, "tls", "{" + peerPair + "}"},
			"peer", tlsPeer, "tls", `{`+peerPair+`, min_version: "1.2"}`),
		oneBridge("every key on wss", []string{"peer", wssPeer, "tls", "{ca_file: " + peerCA + ", " + peerPair + ", min_version: 1.3}"},
			"peer", wssPeer, "tls", `{ca_file: `+peerCA+`, `+peerPair+`, min_version: "1.3"}`),
		oneBridge("min_version 1.2 quoted", []string{"peer", tlsPeer, "tls", `{ca_file: ` + peerCA + `, min_version: "1.2"}`},
			"peer", tlsPeer, "tls", `{ca_file: `+peerCA+`, min_version: "1.2"}`),
		oneBridge("tls none", []string{"peer", tlsPeer, "tls", "none"}, "peer", tlsPeer, "tls", "none"),

		// client_id: "unique per peer: the same host, compared without case,
		// and port": another port, another host, another client id, the
		// client id compared as written.
		bridgesAs("another port", twoBridges(peerAndID(peerURL, "edge-1"), peerAndID("tcp://peer.example:1884", "edge-1")), nil,
			"first", printedBridge(), "second", printedBridge("peer", `"tcp://peer.example:1884"`)),
		bridgesAs("another host", twoBridges(peerAndID(peerURL, "edge-1"), peerAndID("tcp://peer2.example:1883", "edge-1")), nil,
			"first", printedBridge(), "second", printedBridge("peer", `"tcp://peer2.example:1883"`)),
		bridgesAs("another client id", twoBridges(peerAndID(peerURL, "edge-1"), peerAndID(peerURL, "edge-2")), nil,
			"first", printedBridge(), "second", printedBridge("client_id", "edge-2")),
		bridgesAs("the client id in another case", twoBridges(peerAndID(peerURL, "edge-1"), peerAndID(peerURL, "Edge-1")), nil,
			"first", printedBridge(), "second", printedBridge("client_id", "Edge-1")),

		// config.md, bridges.<name>.topics: "at least one rule: filter,
		// topic, direction (in, out or both), qos (0, 1 or 2, default 1)";
		// "A topic template is required for in and out, and refused for
		// both"; topics holds rules in order.
		ruled("out", topics(rule("out", "a/#", "x/$#")), nil, printedRule("out", "a/#", "x/$#")),
		ruled("both, no topic", topics(rule("both", "a/#", absent)), nil, printedRule("both", "a/#", absent)),
		ruled("qos 0, 1 and 2, in order",
			topics(rule("in", "a/#", "x/$#", "qos", "0"), rule("out", "b/#", "y/$#", "qos", "1"), rule("both", "c/+", absent, "qos", "2")), nil,
			printedRule("in", "a/#", "x/$#", "0"), printedRule("out", "b/#", "y/$#"), printedRule("both", "c/+", absent, "2")),
	},
	filtersAccepted,
	topicsAccepted,
	[]printed{
		// "Filters, with {a,b} expanded, and topics keep
		// limits.max_topic_length and limits.max_topic_levels": each
		// expansion is measured, never the filter as written;
		// max_topic_length is "in bytes" (config.md); # and + are levels, as
		// an empty level is. A topic template is measured as written: $# and
		// $n count as the bytes and level written.
		inRule("a filter of 8 bytes", "ab/cdefg", "x", maxLength8),
		inRule("13 bytes written, each expansion 5", "ab/{cd,ef,gh}", "x", maxLength8),
		inRule("a topic of 8 bytes", "a/b", "ab/cdefg", maxLength8),
		inRule("a topic of 8 bytes in 4 characters", "a/b", "éééé", maxLength8),
		inRule("a topic of 8 bytes ending in $#", "a/#", "abcde/$#", maxLength8),
		inRule("a topic of 8 bytes holding $1", "a/+", "abc/de$1", maxLength8),
		inRule("a filter of 3 levels", "a/b/c", "x", maxLevels3),
		inRule("3 levels ending in #", "a/b/#", "x/$#", maxLevels3),
		inRule("3 levels with {a,b}", "a/{b,c}/d", "x", maxLevels3),
		inRule("a topic of 3 levels", "a/b", "x/y/z", maxLevels3),
		inRule("a topic of 3 levels ending in $#", "a/#", "x/y/$#", maxLevels3),

		// config.md, bridges.<name>: "A bridge's inbound filters may not
		// overlap": an out rule's filter is not inbound; those of two bridges are never compared; a filter
		// starting with a wildcard matches no topic starting with $
		// (MQTT-4.7.2-1).
		ruled("apart", topics(rule("in", "a/b", "x"), rule("in", "a/c", "y"), rule("in", "a/+/c", "z/$1"), rule("both", "a/{d,e}", absent)), nil,
			printedRule("in", "a/b", "x"), printedRule("in", "a/c", "y"), printedRule("in", "a/+/c", "z/$1"), printedRule("both", "a/{d,e}", absent)),
		ruled("# and a $ filter", topics(rule("in", "#", "x/$#"), rule("in", "$SYS/#", "sys/$#")), nil,
			printedRule("in", "#", "x/$#"), printedRule("in", "$SYS/#", "sys/$#")),
		// The in filter +/b overlaps the out filter before it (a/b) and the
		// one after it (c/b); the two out filters are apart.
		ruled("an out rule over an in rule", topics(rule("out", "a/#", "x/$#"), rule("in", "+/b", "y/$1"), rule("out", "c/+", "z/$1")), nil,
			printedRule("out", "a/#", "x/$#"), printedRule("in", "+/b", "y/$1"), printedRule("out", "c/+", "z/$1")),
		bridgesAs("two bridges' inbound filters",
			fmt.Sprintf(" {first: %s, second: %s}", bridgeKeys(), bridgeKeys("client_id", "edge-2",
				"topics", rules(rule("in", "a/b", "y"), rule("both", "c/+", absent)))), nil,
			"first", printedBridge(),
			"second", printedBridge("client_id", "edge-2", "topics", "["+printedRule("in", "a/b", "y")+", "+printedRule("both", "c/+", absent)+"]")),

		// config.md, bridges.<name>: "A bridge's outbound filters, those of
		// its out rules and of its both rules, may not overlap, with {a,b}
		// expanded": x/{a,b} and x/c do not, nor does an in rule beside an
		// out rule. "Their topics may not overlap either", so each out rule
		// here has a topic of its own.
		ruled("expansions apart", topics(rule("out", "x/{a,b}", "y"), rule("out", "x/c", "z")), nil,
			printedRule("out", "x/{a,b}", "y"), printedRule("out", "x/c", "z")),
		ruled("another filter, an in rule",
			topics(rule("out", "a/#", "x/$#"), rule("out", "b/#", "y/$#"), rule("in", "a/#", "x/$#")), nil,
			printedRule("out", "a/#", "x/$#"), printedRule("out", "b/#", "y/$#"), printedRule("in", "a/#", "x/$#")),

		// config.md, bridges.<name>: "A bridge's outbound filters, those of
		// its out rules and of its both rules, may not overlap, with {a,b}
		// expanded. A configuration where they do is refused at load, naming
		// both rules": filters no topic matches together,
		// {a,b} expanded, are apart, as are # and a $ filter
		// (MQTT-4.7.2-1); an in rule's filter is not outbound ("an out rule
		// over an in rule" above); those of two bridges are never compared.
		ruled("outbound filters apart",
			topics(rule("out", "s/a", "x"), rule("out", "s/b/+", "y/$1"), rule("both", "t/#", absent), rule("out", "u/{a,b}", "z"), rule("out", "u/c/#", "w/$#")), nil,
			printedRule("out", "s/a", "x"), printedRule("out", "s/b/+", "y/$1"), printedRule("both", "t/#", absent),
			printedRule("out", "u/{a,b}", "z"), printedRule("out", "u/c/#", "w/$#")),
		ruled("# and a $ filter, out", topics(rule("out", "#", "x/$#"), rule("out", "$app/#", "y/$#")), nil,
			printedRule("out", "#", "x/$#"), printedRule("out", "$app/#", "y/$#")),
		bridgesAs("two bridges' outbound filters",
			twoBridges([]string{"topics", rules(rule("out", "s/#", "x/$#"), rule("both", "t/+", absent))},
				[]string{"client_id", "edge-2", "topics", rules(rule("out", "s/#", "x/$#"), rule("both", "t/+", absent))}), nil,
			"first", printedBridge("topics", "["+printedRule("out", "s/#", "x/$#")+", "+printedRule("both", "t/+", absent)+"]"),
			"second", printedBridge("client_id", "edge-2", "topics", "["+printedRule("out", "s/#", "x/$#")+", "+printedRule("both", "t/+", absent)+"]")),

		// config.md, bridges.<name>: "Their topics may not overlap either.
		// For this check each topic template is read as a filter: a level
		// holding any $1, $2 and so on is read as +, $# as #, and a both
		// rule's topic is its filter. A topic starting with $ is compared
		// like any other. Such a configuration is refused at load, naming
		// both rules." Example: "s/+ to x/$1 beside t/+ to y/$1 are
		// accepted": x/+ and y/+, x/+/c and x/+/d, and a both rule's w/# are
		// apart. Only a bridge's own outbound topics are compared: the same
		// topic on two bridges, and an in rule's topic over an out or a
		// both rule's (x/# over x/+, t/+ under t/#), are accepted.
		ruled("templates apart",
			topics(rule("out", "s/+", "x/$1"), rule("out", "t/+", "y/$1"), rule("out", "u/+", "x/$1/c"), rule("out", "v/+", "x/$1/d"), rule("both", "w/#", absent)), nil,
			printedRule("out", "s/+", "x/$1"), printedRule("out", "t/+", "y/$1"), printedRule("out", "u/+", "x/$1/c"),
			printedRule("out", "v/+", "x/$1/d"), printedRule("both", "w/#", absent)),
		bridgesAs("one topic on two bridges",
			twoBridges([]string{"topics", rules(rule("out", "s/a", "one"))}, []string{"client_id", "edge-2", "topics", rules(rule("out", "s/b", "one"))}), nil,
			"first", printedBridge("topics", "["+printedRule("out", "s/a", "one")+"]"),
			"second", printedBridge("client_id", "edge-2", "topics", "["+printedRule("out", "s/b", "one")+"]")),
		ruled("an in rule's topic over outbound topics",
			topics(rule("out", "a/+", "x/$1"), rule("in", "b/#", "x/$#"), rule("both", "t/#", absent), rule("in", "c/+", "t/$1")), nil,
			printedRule("out", "a/+", "x/$1"), printedRule("in", "b/#", "x/$#"), printedRule("both", "t/#", absent), printedRule("in", "c/+", "t/$1")),

		// Values: "A path Sagüin writes must differ from every other path in
		// the configuration": paths only read may be shared.
		bridgesAs("password_file is the broker's password_file", bridge("username", "saguin", "password_file", dirToken+"/passwd"),
			[]string{"broker.mqtt.password_file", dirToken + "/passwd"},
			"b", printedBridge("username", "saguin", "password_file", dirToken+"/passwd")),
		bridgesAs("two bridges share their files",
			twoBridges([]string{"peer", tlsPeer, "username", "saguin", "password_file", peerPass, "tls", "{ca_file: " + peerCA + "}"},
				[]string{"peer", `"wss://peer.example:443"`, "username", "saguin", "password_file", peerPass, "tls", "{ca_file: " + peerCA + "}"}), nil,
			"first", printedBridge("peer", tlsPeer, "username", "saguin", "password_file", peerPass, "tls", `{ca_file: `+peerCA+`, min_version: "1.2"}`),
			"second", printedBridge("peer", `"wss://peer.example:443/"`, "username", "saguin", "password_file", peerPass,
				"tls", `{ca_file: `+peerCA+`, min_version: "1.2"}`)),
	},
)

// thePassword is what the round trip's password file holds.
const thePassword = "s3cret-peer-password"

// bridgeRoundTrips: --output prints each bridge's keys, none written where
// the row's default is none, tls on a tls:// or wss:// peer only; each
// rule's filter as configured, direction and qos, its topic on in and out
// rules; password_file as its path, never the password the file holds (a
// guard: nothing in config.md has the configuration hold a password). Read
// back, the same bridges.
var bridgeRoundTrips = []roundTrip{{
	name: "bridges of every peer kind, every key set",
	sets: []string{"bridges", fmt.Sprintf(" {up: %s, secure: %s, plain: %s}",
		bridgeKeys(
			"peer", `"tls://Peer.Example:8883"`,
			"client_id", "edge-up",
			"username", "saguin",
			"password_file", dirToken+"/peer.pass",
			"session_expiry", "36h",
			"receive_maximum", "7",
			"ack_interval", "250ms",
			"tls", "{ca_file: "+peerCA+", "+peerPair+", min_version: 1.3}",
			"topics", rules(
				rule("in", "fleet/{north,south}/+/#", "remote/$1/$#", "qos", "2"),
				rule("out", "local/#", "edge/$#", "qos", "0"),
				rule("both", "shared/x", absent),
			),
		),
		bridgeKeys("peer", `"wss://peer.example:443"`, "client_id", "edge-secure"),
		bridgeKeys("client_id", "edge-plain", "topics", rules(rule("out", "a/+", "b/$1"))))},
	files: map[string]string{"peer.pass": thePassword + "\n"},
	want: bridgesAre(
		"up", printedBridge("peer", `"tls://Peer.Example:8883"`, "client_id", "edge-up", "username", "saguin",
			"password_file", dirToken+"/peer.pass", "session_expiry", "36h", "receive_maximum", "7", "ack_interval", "250ms",
			"tls", `{ca_file: `+peerCA+`, `+peerPair+`, min_version: "1.3"}`,
			"topics", "["+printedRule("in", "fleet/{north,south}/+/#", "remote/$1/$#", "2")+", "+
				printedRule("out", "local/#", "edge/$#", "0")+", "+printedRule("both", "shared/x", absent)+"]"),
		"secure", printedBridge("peer", `"wss://peer.example:443/"`, "client_id", "edge-secure"),
		"plain", printedBridge("client_id", "edge-plain", "topics", "["+printedRule("out", "a/+", "b/$1")+"]"),
	),
	never: []string{thePassword},
}}

// bridgeRefusal is a case of bridge b with pairs, refused naming
// bridges.b.<key>.
func bridgeRefusal(key string, pairs ...string) refusal {
	return refuse(bridge(pairs...), "bridges.b."+key, file("bridges", bridge(pairs...)))
}

// bridgesRefusal is a case of the bridges text refused naming key.
func bridgesRefusal(name, text, key string, says ...string) refusal {
	return refuse(name, key, file("bridges", text), says...)
}

var bridgeRefused = slices.Concat(
	[]refusal{
		// peer, client_id and topics: "required"; Values: "The loader refuses
		// a key written empty or ~".
		bridgeRefusal("peer", "peer", absent),
		bridgeRefusal("peer", "peer", `""`),
		bridgeRefusal("peer", "peer", "~"),
		bridgeRefusal("peer", "peer", "none"),
		bridgeRefusal("client_id", "client_id", absent),
		bridgeRefusal("client_id", "client_id", `""`),
		bridgeRefusal("client_id", "client_id", "~"),
		bridgeRefusal("topics", "topics", absent),
		bridgeRefusal("topics", "topics", "~"),
		bridgeRefusal("topics", "topics", "none"),
		// topics: "at least one rule".
		bridgeRefusal("topics", "topics", "[]"),
		bridgeRefusal("topics", "topics", `"a/#"`),
		bridgeRefusal("username", "username", `""`),
		bridgeRefusal("username", "username", "~"),
		// password_file: Values, "Every file path is absolute".
		bridgeRefusal("password_file", "username", "saguin", "password_file", "peer.pass"),
		bridgeRefusal("password_file", "username", "saguin", "password_file", `""`),
		bridgeRefusal("password_file", "username", "saguin", "password_file", "~"),
		// session_expiry: "never none"; a duration at least 1s, without ms;
		// "at most 4,294,967,295 seconds".
		bridgeRefusal("session_expiry", "session_expiry", "none"),
		bridgeRefusal("session_expiry", "session_expiry", "0s"),
		bridgeRefusal("session_expiry", "session_expiry", "500ms"),
		bridgeRefusal("session_expiry", "session_expiry", "1000ms"),
		bridgeRefusal("session_expiry", "session_expiry", "4294967296s"),
		bridgeRefusal("session_expiry", "session_expiry", "49711d"),
		bridgeRefusal("session_expiry", "session_expiry", "1.5h"),
		bridgeRefusal("session_expiry", "session_expiry", "1h30m"),
		bridgeRefusal("session_expiry", "session_expiry", "60"),
		// receive_maximum: "1 to 65,535"; none is not its default.
		bridgeRefusal("receive_maximum", "receive_maximum", "0"),
		bridgeRefusal("receive_maximum", "receive_maximum", "65536"),
		bridgeRefusal("receive_maximum", "receive_maximum", "-1"),
		bridgeRefusal("receive_maximum", "receive_maximum", "1.5"),
		bridgeRefusal("receive_maximum", "receive_maximum", "20s"),
		bridgeRefusal("receive_maximum", "receive_maximum", "none"),
		// ack_interval: a duration above 0, "at most 1s"; none is not its
		// default.
		bridgeRefusal("ack_interval", "ack_interval", "0ms"),
		bridgeRefusal("ack_interval", "ack_interval", "1001ms"),
		bridgeRefusal("ack_interval", "ack_interval", "2s"),
		bridgeRefusal("ack_interval", "ack_interval", "1m"),
		bridgeRefusal("ack_interval", "ack_interval", "1.5ms"),
		bridgeRefusal("ack_interval", "ack_interval", "5us"),
		bridgeRefusal("ack_interval", "ack_interval", "5"),
		bridgeRefusal("ack_interval", "ack_interval", "none"),
		// Read strictly: a key the table does not hold, a password among
		// them (the password lives in password_file).
		bridgeRefusal("colour", "colour", "blue"),
		bridgeRefusal("password", "password", "secret"),

		// password_file: "This key requires username"; a username of none is
		// none.
		bridgesRefusal("password_file without username", bridge("password_file", peerPass), "bridges.b.password_file", "username"),
		bridgesRefusal("password_file with username none", bridge("username", "none", "password_file", peerPass), "bridges.b.password_file", "username"),

		// A bridge is a mapping of its keys; bridges, a mapping of bridges by
		// name (bridges.<name>), never ~ (Values); config.md,
		// bridges.<name>: "Neither bridges: nor a tls block is empty. The
		// topics value is a list".
		bridgesRefusal("a bridge written as a scalar", " {b: "+peerURL+"}", "bridges.b"),
		bridgesRefusal("a bridge written as a list", " {b: ["+bridgeKeys()+"]}", "bridges.b"),
		bridgesRefusal("a bridge written empty", " {b: ~}", "bridges.b"),
		bridgesRefusal("bridges written empty", "", "bridges"),
		bridgesRefusal("bridges written ~", " ~", "bridges"),
		bridgesRefusal("bridges a scalar", " b", "bridges"),
		bridgesRefusal("bridges a list of bridges", " [{b: "+bridgeKeys()+"}]", "bridges"),
		bridgesRefusal("bridges an empty mapping", " {}", "bridges"),
		bridgesRefusal("topics a mapping", bridge("topics", `{filter: "a/#", topic: "x/$#", direction: in}`), "bridges.b.topics"),

		// Names are unique: a key written twice (read strictly).
		bridgesRefusal("a name twice", " {b: "+bridgeKeys()+", b: "+bridgeKeys("client_id", "other")+"}", "bridges.b"),
	},
	// Names: "1 to 128 bytes with no whitespace": bytes, not characters;
	// any whitespace, Unicode's included. A refused name is named by its
	// key, bridges.<name>, and the empty name by the mapping it stands in,
	// bridges (the manager's decision, audit 261015-006a).
	each([]string{
		"", strings.Repeat("n", 129), strings.Repeat("é", 64) + "n",
		"a b", " a", "a ", "a\tb", "a\nb", "a b", "a b",
	}, func(name string) refusal {
		key := "bridges"
		if name != "" {
			key += "." + name
		}
		return bridgesRefusal(fmt.Sprintf("refused %q", name), fmt.Sprintf(" {%q: %s}", name, bridgeKeys()), key)
	}),
	// peer: "tcp://, tls://, ws:// or wss://, with a host as in a listener's
	// address, never empty, and a port from 1 to 65,535; ws:// and wss://
	// may add a path (default /); never a user, query or fragment".
	each([]string{
		// The four schemes alone.
		"mqtt://peer.example:1883", "mqtts://peer.example:8883", "ssl://peer.example:8883",
		"http://peer.example:80", "https://peer.example:443", "udp://peer.example:1883",
		"peer.example:1883", "//peer.example:1883", "tcp:peer.example:1883",
		// With host and port.
		"tcp://peer.example", "tcp://peer.example:", "ws://peer.example/mqtt", "tcp://:1883",
		"tcp://peer.example:mqtt", "tcp://peer.example:65536", "tcp://peer.example:-1",
		"tcp://peer example:1883", "tcp://peer.example:0",
		// A host as in a listener's address.
		"tcp://-peer.example:1883", "tcp://peer-.example:1883", "tcp://peer.123:1883", "tcp://123:1883",
		"tcp://" + strings.Repeat("a", 64) + ".example:1883", "tcp://pe!er.example:1883", "tcp://peer..example:1883",
		"tcp://224.0.0.1:1883", "tcp://[ff02::1]:1883", "tcp://[10.0.0.1]:1883",
		// Never a user.
		"tcp://user@peer.example:1883", "tcp://user:secret@peer.example:1883", "wss://user@peer.example:443/mqtt",
		// A path only on ws:// and wss://.
		"tcp://peer.example:1883/", "tcp://peer.example:1883/mqtt", "tls://peer.example:8883/mqtt",
		// Never a query.
		"ws://peer.example:8080/mqtt?v=5", "ws://peer.example:8080/?", "tcp://peer.example:1883?x=1", "wss://peer.example:443?x",
		// Never a fragment.
		"ws://peer.example:8080/mqtt#top", "ws://peer.example:8080/#", "tcp://peer.example:1883#f",
	}, func(peer string) refusal { return bridgeRefusal("peer", "peer", fmt.Sprintf("%q", peer)) }),
	[]refusal{
		// tls: "A key on a peer it does not belong to is refused", whatever
		// its value and wherever it is written (bridge writes it before
		// peer).
		bridgesRefusal("tls on tcp", bridge("tls", "{ca_file: "+peerCA+"}"), "bridges.b.tls"),
		bridgesRefusal("tls on ws", bridge("peer", `"ws://peer.example:8080"`, "tls", "{ca_file: "+peerCA+"}"), "bridges.b.tls"),
		bridgesRefusal("tls none on tcp", bridge("tls", "none"), "bridges.b.tls"),
		// peer: "required". With no peer, no kind is known to judge tls by:
		// the missing peer is refused, never tls as a key that does not
		// belong (agreed with the maintainer, audit 261015-01).
		bridgesRefusal("tls with no peer", bridge("peer", absent, "tls", "{ca_file: "+peerCA+"}"), "bridges.b.peer", "required"),
		bridgesRefusal("every tls key with no peer", bridge("peer", absent, "tls", "{ca_file: "+peerCA+", "+peerPair+", min_version: 1.3}"),
			"bridges.b.peer", "required"),
		bridgesRefusal("tls none with no peer", bridge("peer", absent, "tls", "none"), "bridges.b.peer", "required"),
		// "written together": the missing one is refused.
		bridgesRefusal("cert_file alone", bridge("peer", tlsPeer, "tls", "{cert_file: "+peerCert+"}"),
			"bridges.b.tls.key_file", "bridges.b.tls.cert_file"),
		bridgesRefusal("key_file alone", bridge("peer", wssPeer, "tls", "{ca_file: "+peerCA+", key_file: "+peerKey+"}"),
			"bridges.b.tls.cert_file", "bridges.b.tls.key_file"),
		// Read strictly: a listener's keys are not a bridge's; tls is a
		// mapping or none.
		bridgesRefusal("client_ca_file", bridge("peer", tlsPeer, "tls", "{client_ca_file: "+peerCA+"}"), "bridges.b.tls.client_ca_file"),
		bridgesRefusal("require_certificate", bridge("peer", tlsPeer, "tls", "{ca_file: "+peerCA+", require_certificate: true}"),
			"bridges.b.tls.require_certificate"),
		bridgesRefusal("tls a scalar", bridge("peer", tlsPeer, "tls", peerCA), "bridges.b.tls"),
		bridgesRefusal("tls ~", bridge("peer", tlsPeer, "tls", "~"), "bridges.b.tls"),
		// "Neither bridges: nor a tls block is empty".
		bridgesRefusal("an empty tls", bridge("peer", tlsPeer, "tls", "{}"), "bridges.b.tls"),
	},
	// The files: absolute paths, never none or empty, each named by its key
	// (Values).
	each([][2]string{
		{"ca_file: peer-ca.crt", "ca_file"},
		{"ca_file: none", "ca_file"},
		{`ca_file: ""`, "ca_file"},
		{"cert_file: peer.crt, key_file: " + peerKey, "cert_file"},
		{"cert_file: none, key_file: " + peerKey, "cert_file"},
		{"cert_file: " + peerCert + ", key_file: peer.key", "key_file"},
		{`cert_file: ` + peerCert + `, key_file: ""`, "key_file"},
	}, func(f [2]string) refusal {
		return bridgesRefusal(f[0], bridge("peer", tlsPeer, "tls", "{"+f[0]+"}"), "bridges.b.tls."+f[1])
	}),
	// min_version: "1.2 (default) or 1.3".
	each([]string{"1.0", "1.1", "1.4", `"TLS1.3"`, "13", "none"}, func(v string) refusal {
		return bridgesRefusal("min_version "+v, bridge("peer", tlsPeer, "tls", "{ca_file: "+peerCA+", min_version: "+v+"}"), "bridges.b.tls.min_version")
	}),
	// client_id: "unique per peer: the same host, compared without case, and
	// port": the scheme and a ws path do not make another peer. The later
	// bridge's client_id is refused, naming the earlier bridge.
	each([][4]string{
		{peerURL, "edge-1", peerURL, "edge-1"},
		{"tcp://Peer.Example:1883", "edge-1", peerURL, "edge-1"},
		{peerURL, "edge-1", "tls://peer.example:1883", "edge-1"},
		{"ws://peer.example:8080/a", "edge-1", "wss://PEER.example:8080/b", "edge-1"},
		{"tcp://[::1]:1883", "edge-1", "tls://[::1]:1883", "edge-1"},
	}, func(p [4]string) refusal {
		return bridgesRefusal("one peer, one client id: "+p[0]+" and "+p[2], twoBridges(peerAndID(p[0], p[1]), peerAndID(p[2], p[3])),
			"bridges.second.client_id", "bridges.first")
	}),
	[]refusal{
		bridgesRefusal("the third repeats the first",
			fmt.Sprintf(" {first: %s, second: %s, third: %s}", bridgeKeys(), bridgeKeys("client_id", "edge-2"), bridgeKeys()),
			"bridges.third.client_id", "bridges.first"),

		// A filter and a direction on every rule; the topic required for in
		// and out, refused for both; read strictly; a rule is a mapping (a
		// key that is not a non-empty plain string refused naming the rule,
		// the manager's decision, audit 261015-006a).
		bridgesRefusal("no filter", bridge("topics", `[{topic: "x", direction: in}]`), "bridges.b.topics[0].filter"),
		bridgesRefusal("an empty filter", topics(rule("in", "", "x")), "bridges.b.topics[0].filter"),
		bridgesRefusal("no direction", bridge("topics", `[{filter: "a/b", topic: "x"}]`), "bridges.b.topics[0].direction"),
		bridgesRefusal("the second rule's direction", topics(rule("in", "a/#", "x/$#"), rule("sideways", "b/#", "y/$#")), "bridges.b.topics[1].direction"),
		bridgesRefusal("in without a topic", topics(rule("in", "a/b", absent)), "bridges.b.topics[0].topic"),
		bridgesRefusal("out without a topic", topics(rule("out", "a/b", absent)), "bridges.b.topics[0].topic"),
		bridgesRefusal("both with a topic", topics(rule("both", "a/b", "a/b")), "bridges.b.topics[0].topic"),
		bridgesRefusal("an unknown key", topics(rule("in", "a/#", "x/$#", "colour", "blue")), "bridges.b.topics[0].colour"),
		bridgesRefusal("an empty key", topics(rule("in", "a/#", "x/$#", `""`, "1")), "bridges.b.topics[0]"),
		bridgesRefusal("a rule written as a scalar", bridge("topics", `["a/#"]`), "bridges.b.topics[0]"),
		bridgesRefusal("a rule written as a list", bridge("topics", `[[a, b]]`), "bridges.b.topics[0]"),
		bridgesRefusal("a rule written empty", bridge("topics", `[~]`), "bridges.b.topics[0]"),
	},
	// direction: "in, out or both"; qos: "0, 1 or 2", none not its default.
	each([][2]string{
		{"direction", "In"}, {"direction", "inbound"}, {"direction", "none"}, {"direction", `""`}, {"direction", "1"},
		{"qos", "3"}, {"qos", "-1"}, {"qos", "1.0"}, {"qos", "none"}, {"qos", `""`}, {"qos", "at_least_once"},
	}, func(kv [2]string) refusal {
		r := fmt.Sprintf(`{filter: "a/#", topic: "x/$#", %s: %s}`, kv[0], kv[1])
		if kv[0] == "qos" {
			r = rule("in", "a/#", "x/$#", "qos", kv[1])
		}
		return bridgesRefusal(kv[0]+" "+kv[1], topics(r), "bridges.b.topics[0]."+kv[0])
	}),
	filtersRefused,
	topicsRefused,
	[]refusal{
		// The topic limits, past them.
		bridgeLimit("a filter of 9 bytes", "ab/cdefgh", "x", maxLength8, "filter"),
		bridgeLimit("one expansion of 9 bytes", "ab/{x,cdefgh}", "x", maxLength8, "filter"),
		bridgeLimit("a topic of 9 bytes", "a/b", "ab/cdefgh", maxLength8, "topic"),
		bridgeLimit("a topic of 9 bytes in 5 characters", "a/b", "ééééa", maxLength8, "topic"),
		bridgeLimit("a topic of 9 bytes ending in $#", "a/#", "abcdef/$#", maxLength8, "topic"),
		bridgeLimit("a topic of 9 bytes holding $1", "a/+", "abc/def$1", maxLength8, "topic"),
		bridgeLimit("a filter of 4 levels", "a/b/c/d", "x", maxLevels3, "filter"),
		bridgeLimit("4 levels ending in #", "a/b/c/#", "x/$#", maxLevels3, "filter"),
		bridgeLimit("4 levels, two empty", "a//b/", "x", maxLevels3, "filter"),
		bridgeLimit("a topic of 4 levels", "a/b", "x/y/z/w", maxLevels3, "topic"),
		bridgeLimit("a topic of 4 levels ending in $#", "a/#", "x/y/z/$#", maxLevels3, "topic"),

		// config.md, bridges.<name>: "A bridge's inbound filters may not
		// overlap": the filters of one bridge's in and both rules, {a,b} expanded, two overlapping where
		// some topic matches both (MQTT 5 4.7.1: "sport/#" also matches
		// "sport"). The later rule's filter is refused, naming the earlier.
		overlap("one filter twice", rule("in", "a/b", "x"), rule("in", "a/b", "y")),
		overlap("# over a written level", rule("in", "a/#", "x/$#"), rule("in", "a/b", "y")),
		overlap("+ over a written level", rule("in", "a/+", "x/$1"), rule("in", "a/b", "y")),
		overlap("+ against +", rule("in", "a/+/c", "x/$1"), rule("in", "a/b/+", "y/$1")),
		overlap("+/+ over a/b", rule("in", "a/b", "y"), rule("in", "+/+", "x/$1/$2")),
		overlap("a/# matches a", rule("in", "a", "x"), rule("in", "a/#", "y/$#")),
		overlap("in against both", rule("in", "a/b", "x"), rule("both", "a/#", absent)),
		overlap("both against both", rule("both", "a/+", absent), rule("both", "+/b", absent)),
		overlap("an expansion", rule("in", "a/{b,c}", "x"), rule("in", "a/c", "y")),
		bridgesRefusal("the third against the first", topics(rule("in", "a/#", "x/$#"), rule("in", "b/#", "y/$#"), rule("in", "a/b/c", "z")),
			"bridges.b.topics[2].filter", "bridges.b.topics[0]"),

		// config.md, bridges.<name>: "A bridge's outbound filters, those of
		// its out rules and of its both rules, may not overlap, with {a,b}
		// expanded": the later rule, naming the earlier, whatever their qos;
		// one filter twice overlaps.
		outTwice("one filter and topic twice", topics(rule("out", "a/#", "x/$#"), rule("out", "a/#", "x/$#")), "bridges.b.topics[1]"),
		outTwice("another qos", topics(rule("out", "a/#", "x/$#", "qos", "0"), rule("out", "a/#", "x/$#", "qos", "2")), "bridges.b.topics[1]"),
		outTwice("an in rule between", topics(rule("out", "a/+", "x/$1"), rule("in", "b/#", "y/$#"), rule("out", "a/+", "x/$1")), "bridges.b.topics[2]"),
		outTwice("an expansion written alone", topics(rule("out", "x/{a,b}", "y"), rule("out", "x/b", "y")), "bridges.b.topics[1]"),
		outTwice("alternatives in another order", topics(rule("out", "{a,b}/#", "x/$#"), rule("out", "{b,a}/#", "x/$#")), "bridges.b.topics[1]"),
		outTwice("two expansions alike", topics(rule("out", "x/{a,b}", "y"), rule("out", "x/{b,c}", "y")), "bridges.b.topics[1]"),

		// config.md, bridges.<name>: "A bridge's outbound filters, those of
		// its out rules and of its both rules, may not overlap, with {a,b}
		// expanded. A configuration where they do is refused at load, naming
		// both rules": two overlapping where some topic
		// matches both (MQTT 5 4.7.1: "sport/#" also matches "sport"),
		// {a,b} expanded, whatever their topics and qos. As for inbound
		// filters, the later rule's filter is refused, naming the earlier.
		overlap("out against out", rule("out", "s/#", "x/$#"), rule("out", "s/+", "y/$1")),
		overlap("one filter, another topic", rule("out", "a/#", "x/$#"), rule("out", "a/#", "y/$#")),
		overlap("+ against +, another qos", rule("out", "a/+/c", "x/$1"), rule("out", "a/b/+", "y/$1", "qos", "0")),
		overlap("a/# matches a, out", rule("out", "a", "x"), rule("out", "a/#", "y/$#")),
		overlap("out against both", rule("out", "a/b", "x"), rule("both", "a/#", absent)),
		overlap("both against out", rule("both", "a/+", absent), rule("out", "+/b", "y/$1")),
		overlap("through {a,b}", rule("out", "x/{a,b}", "y"), rule("out", "x/b", "z")),
		overlap("through two {a,b}", rule("out", "{a,b}/c", "y"), rule("out", "{b,d}/+", "z/$1")),
		bridgesRefusal("the third out against the first, an in rule between",
			topics(rule("out", "a/#", "x/$#"), rule("in", "b/#", "y/$#"), rule("out", "a/b/c", "z")),
			"bridges.b.topics[2].filter", "bridges.b.topics[0]"),

		// config.md, bridges.<name>: "Their topics may not overlap either.
		// For this check each topic template is read as a filter: a level
		// holding any $1, $2 and so on is read as +, $# as #, and a both
		// rule's topic is its filter. A topic starting with $ is compared
		// like any other. Such a configuration is refused at load, naming
		// both rules." Example: "out rules s/a to one and s/b to one are
		// refused". Each pair's filters are apart; the later rule's topic,
		// or a later both rule's filter, is refused, naming the earlier. An
		// in rule's topic is not compared: one/# over one, between them.
		topicsOverlap("one constant topic", "topic", rule("out", "s/a", "one"), rule("out", "s/b", "one")),
		topicsOverlap("through $1 and $#", "topic", rule("out", "a/+", "x/$1/c"), rule("out", "b/#", "x/b/$#")),
		topicsOverlap("a template over a both rule's filter", "topic", rule("both", "t/#", absent), rule("out", "s/+", "t/$1")),
		topicsOverlap("a both rule's filter under a template", "filter", rule("out", "s/+", "t/$1"), rule("both", "t/b", absent)),
		bridgesRefusal("the third topic against the first, an in rule between",
			topics(rule("out", "s/a", "one"), rule("in", "b/#", "one/$#"), rule("out", "s/b", "one")),
			"bridges.b.topics[2].topic", "bridges.b.topics[0]"),

		// config.md's Dropped row "[...] a bridge's upstream and inbound, a
		// rule's channel [...] | none: refused, with no replacement", whatever else the
		// bridge holds.
		refusedWithNoReplacement("upstream", "bridges.b.upstream", "bridges", bridge("upstream", "peer")),
		refusedWithNoReplacement("inbound", "bridges.b.inbound", "bridges", bridge("inbound", "true")),
		refusedWithNoReplacement("upstream alone", "bridges.b.upstream", "bridges", " {b: {upstream: peer}}"),
		refusedWithNoReplacement("a rule's channel", "bridges.b.topics[0].channel", "bridges", topics(rule("in", "a/#", "x/$#", "channel", "events"))),
		refusedWithNoReplacement("the second rule's channel", "bridges.b.topics[1].channel", "bridges",
			topics(rule("in", "a/#", "x/$#"), rule("out", "b/#", "y/$#", "channel", "events"))),

		// config.md, Values: "A path Sagüin writes must differ from every
		// other path in the configuration. These paths are compared as files
		// at start", a bridge's password_file and tls files among them. The
		// bridge's key, later in config.md's order than broker, is refused,
		// naming the earlier.
		refuse("password_file is pid_file", "bridges.b.password_file",
			file("broker.pid_file", "/run/saguin/saguin.pid", "bridges", bridge("username", "saguin", "password_file", "/run/saguin/saguin.pid")),
			"broker.pid_file"),
		bridgesRefusal("ca_file is file_path's -wal file, spelled otherwise", bridge("peer", tlsPeer, "tls", "{ca_file: /var/lib/saguin/./saguin.db-wal}"),
			"bridges.b.tls.ca_file", "broker.storage.file_path"),
		bridgesRefusal("cert_file is memory_dir", bridge("peer", tlsPeer, "tls", "{cert_file: /dev/shm/saguin, key_file: "+peerKey+"}"),
			"bridges.b.tls.cert_file", "broker.storage.memory_dir"),
		refuse("key_file is a unix socket's lock file", "bridges.b.tls.key_file",
			file("broker.mqtt.listen.unix", "{path: /run/saguin/mqtt.sock}",
				"bridges", bridge("peer", tlsPeer, "tls", "{cert_file: "+peerCert+", key_file: /run/saguin/mqtt.sock.lock}")),
			"broker.mqtt.listen.unix.path"),
	},
)

// bridgeLimit is one in rule past a topic limit, refused naming its key.
func bridgeLimit(name, filter, topic string, limit []string, key string) refusal {
	return refuse(name, "bridges.b.topics[0]."+key, file(slices.Concat(limit, []string{"bridges", topics(rule("in", filter, topic))})...))
}

// overlap is two rules whose inbound or whose outbound filters overlap: the
// second's filter refused, naming the first.
func overlap(name, first, second string) refusal {
	return bridgesRefusal(name, topics(first, second), "bridges.b.topics[1].filter", "bridges.b.topics[0]")
}

// topicsOverlap is two rules of one bridge whose outbound topics overlap
// and whose filters do not: the second's key, its topic or a both rule's
// filter, or the second rule itself, refused, naming the first (config.md,
// bridges.<name>: "Such a configuration is refused at load, naming both
// rules").
func topicsOverlap(name, key, first, second string) refusal {
	r := bridgesRefusal(name, topics(first, second), "bridges.b.topics[1]."+key, "bridges.b.topics[0]")
	r.or = "bridges.b.topics[1]"
	return r
}

// outTwice is the bridges text, two out rules with one filter, refused naming
// the later rule, at later, or its filter, and naming the earlier,
// topics[0] (config.md, bridges.<name>: "A bridge's outbound filters [...]
// may not overlap [...] A configuration where they do is refused at load,
// naming both rules").
func outTwice(name, text, later string) refusal {
	r := bridgesRefusal(name, text, later, "bridges.b.topics[0]")
	r.or = later + ".filter"
	return r
}
