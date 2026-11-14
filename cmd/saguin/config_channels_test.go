package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
)

// The channels (config.md: the paragraph on channels: and !include, the
// channels.<name> table, the Dropped rows for a channel's keys; design.md,
// Words (Channel), Channels, Operations). Refusals name the key by its full
// path: "channels.<name>.<key>", "channels.<name>.retry.<key>", a name
// itself "channels.<name>"; where channels is a list, "channels[<i>]" for
// the entry, its !include included, and "channels[<i>].<name>.<key>" for
// what the group holds, an included file's group too, the message then
// naming the included file. A rule between two channels (a name defined
// twice, one filter twice) refuses the later, naming the earlier. --output
// writes channels as one mapping keyed by name, in the file's order: a
// mapping's channels as written; a list's groups in turn, an included
// file's where its !include stands.

// entries is the YAML text of a block list, one entry per line as written.
func entries(lines ...string) string { return "\n  - " + strings.Join(lines, "\n  - ") }

// one is the YAML text of a channels mapping holding one channel, c, of
// type kind (absent: no type) with the key/value pairs of pairs. A key
// retry.<k> goes into the channel's retry mapping. type is written last, so
// a reader cannot depend on meeting it first.
func one(kind string, pairs ...string) string {
	var keys, retry []string
	for i := 0; i+1 < len(pairs); i += 2 {
		if sub, ok := strings.CutPrefix(pairs[i], "retry."); ok {
			retry = append(retry, sub+": "+pairs[i+1])
		} else {
			keys = append(keys, pairs[i]+": "+pairs[i+1])
		}
	}
	if len(retry) > 0 {
		keys = append(keys, "retry: {"+strings.Join(retry, ", ")+"}")
	}
	if kind != absent {
		keys = append(keys, "type: "+kind)
	}
	return " {c: {" + strings.Join(keys, ", ") + "}}"
}

// channelKeys is each type's keys, in config.md's order, as its table's
// defaults leave them under the required keys' storage defaults (7d, none):
// filter "<name>/#" (filled in by printedChannel), start "floor",
// tombstone_retention_period "1d", visibility_timeout "30s", every retention
// "the storage default", max_bytes none. A queue's retry is retryKeys.
var (
	channelKeys = map[string][]string{
		"append": {"type", "append", "filter", "", "start", "floor", "retention_period", "7d", "retention_bytes", "none", "max_bytes", "none"},
		"latest": {"type", "latest", "filter", "", "retention_period", "7d", "tombstone_retention_period", "1d"},
		"queue": {"type", "queue", "filter", "", "start", "floor", "retention_period", "7d", "retention_bytes", "none",
			"visibility_timeout", "30s", "retry", "", "dlq_retention_period", "7d", "dlq_retention_bytes", "none"},
	}
	// retryKeys: retry.max_attempts 5, retry.mode "fixed", retry.delay "5s".
	retryKeys = []string{"max_attempts", "5", "mode", "fixed", "delay", "5s"}
)

// printedChannel is how --output prints the channel name of type kind:
// channelKeys, each key/value pair of pairs replacing its key's value, a
// key retry.<k> its retry's.
func printedChannel(kind, name string, pairs ...string) string {
	keys, retry := slices.Clone(channelKeys[kind]), slices.Clone(retryKeys)
	set := func(keys []string, key, value string) {
		i := slices.Index(keys, key)
		if i < 0 || i%2 != 0 {
			panic("no key " + key + " on a " + kind + " channel")
		}
		keys[i+1] = value
	}
	set(keys, "filter", strconv.Quote(name+"/#"))
	for i := 0; i+1 < len(pairs); i += 2 {
		if sub, ok := strings.CutPrefix(pairs[i], "retry."); ok {
			set(retry, sub, pairs[i+1])
		} else {
			set(keys, pairs[i], pairs[i+1])
		}
	}
	if kind == "queue" {
		set(keys, "retry", flow(retry))
	}
	return flow(keys)
}

// flow is the flow mapping of key/value pairs, in their order.
func flow(pairs []string) string {
	entries := make([]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		entries = append(entries, pairs[i]+": "+pairs[i+1])
	}
	return "{" + strings.Join(entries, ", ") + "}"
}

// channel is one entry of the channels --output prints: name, and the
// channel as printedChannel prints it.
func channel(kind, name string, pairs ...string) string {
	return strconv.Quote(name) + ": " + printedChannel(kind, name, pairs...)
}

// channelsAre is the want of a case whose channels --output prints as each
// entry, exactly, in their order.
func channelsAre(each ...string) []string {
	return []string{"channels", "{" + strings.Join(each, ", ") + "}"}
}

// channelsAs is a case of the channels text, over the required keys and
// sets, whose channels --output prints as each entry, exactly.
func channelsAs(name, text string, sets []string, each ...string) printed {
	return printed{name: name, configuration: file(slices.Concat(sets, []string{"channels", text})...), want: channelsAre(each...)}
}

// storageSet sets storage defaults other than the required keys', and
// $broadcast's apart from them, so that a channel's retention shows which
// it takes.
var storageSet = []string{
	"broker.storage.default_retention_period", "3d",
	"broker.storage.default_retention_bytes", "5GiB",
	"broker.broadcast.retention_period", "1h",
	"broker.broadcast.retention_bytes", "1GiB",
}

// channelDefaults: each key's Default column in config.md's channels.<name>
// table, for a channel that writes only its type; channels kept in the
// order written; and the retention keys' default, "the storage default"
// (broker.storage's default_retention_period and default_retention_bytes,
// "the retention of every log that omits its own"), never $broadcast's, an
// explicit none overriding it ("none allowed" on each retention key).
var channelDefaults = []printed{
	// Not sorted: the order written.
	channelsAs("one of each type", " {zeta: {type: append}, alpha: {type: latest}, mid: {type: queue}}", nil,
		channel("append", "zeta"), channel("latest", "alpha"), channel("queue", "mid")),
	channelsAs("the storage defaults inherited", " {a: {type: append}, l: {type: latest}, q: {type: queue}}", storageSet,
		channel("append", "a", "retention_period", "3d", "retention_bytes", "5GiB"),
		channel("latest", "l", "retention_period", "3d"),
		channel("queue", "q", "retention_period", "3d", "retention_bytes", "5GiB", "dlq_retention_period", "3d", "dlq_retention_bytes", "5GiB")),
	channelsAs("none over the storage defaults",
		` {a: {type: append, retention_period: none, retention_bytes: none},`+
			` l: {type: latest, retention_period: none},`+
			` q: {type: queue, retention_period: none, retention_bytes: none, dlq_retention_period: none, dlq_retention_bytes: none}}`,
		storageSet,
		channel("append", "a", "retention_period", "none"),
		channel("latest", "l", "retention_period", "none"),
		channel("queue", "q", "retention_period", "none", "dlq_retention_period", "none")),
}

// channelValue is one key's value on a channel of a type.
type channelValue struct {
	kind  string
	pairs []string
}

// channelValues: each value a channel's key takes (config.md's
// channels.<name> rows and its Values paragraph).
var channelValues = []channelValue{
	// start (append, queue): "floor or tail".
	{"append", []string{"start", "floor"}},
	{"append", []string{"start", "tail"}},
	{"queue", []string{"start", "tail"}},
	// retention_period: a duration, at least 1s, "none allowed".
	{"append", []string{"retention_period", "1s"}},
	{"latest", []string{"retention_period", "90m"}},
	{"queue", []string{"retention_period", "2d"}},
	{"latest", []string{"retention_period", "none"}},
	// retention_bytes (append, queue): a size, "none allowed".
	{"append", []string{"retention_bytes", "1"}},
	{"queue", []string{"retention_bytes", "2GiB"}},
	// max_bytes (append): a size; none is its default.
	{"append", []string{"max_bytes", "64MiB"}},
	{"append", []string{"max_bytes", "none"}},
	// tombstone_retention_period (latest): a duration.
	{"latest", []string{"tombstone_retention_period", "1s"}},
	{"latest", []string{"tombstone_retention_period", "12h"}},
	// visibility_timeout (queue): a duration, under the 7d retention.
	{"queue", []string{"visibility_timeout", "1s"}},
	{"queue", []string{"visibility_timeout", "5m"}},
	// retry.max_attempts (queue): a count, at least 1.
	{"queue", []string{"retry.max_attempts", "1"}},
	{"queue", []string{"retry.max_attempts", "100"}},
	// retry.mode (queue): "fixed, linear or exponential".
	{"queue", []string{"retry.mode", "fixed"}},
	{"queue", []string{"retry.mode", "linear"}},
	{"queue", []string{"retry.mode", "exponential"}},
	// retry.delay (queue): Values, "Only write_timeout,
	// publish_commit_interval, a queue's retry.delay and a bridge's
	// ack_interval also take ms".
	{"queue", []string{"retry.delay", "1ms"}},
	{"queue", []string{"retry.delay", "250ms"}},
	{"queue", []string{"retry.delay", "2s"}},
	{"queue", []string{"retry.delay", "1h"}},
	{"queue", []string{"retry.max_attempts", "3", "retry.mode", "exponential", "retry.delay", "100ms"}},
	// dlq_retention_period, dlq_retention_bytes (queue): "none allowed".
	{"queue", []string{"dlq_retention_period", "1d"}},
	{"queue", []string{"dlq_retention_period", "none"}},
	{"queue", []string{"dlq_retention_bytes", "1GiB"}},
	{"queue", []string{"dlq_retention_bytes", "none"}},
}

// oneChannel is a case of the one channel c of type kind with pairs,
// printed as written.
func oneChannel(name, kind string, pairs ...string) printed {
	return channelsAs(name, one(kind, pairs...), nil, channel(kind, "c", pairs...))
}

// channelsIn is a case whose channels stand in a list, some included from
// files, printed as each entry.
func channelsIn(name string, files map[string]string, text string, sets []string, each ...string) printed {
	c := channelsAs(name, text, sets, each...)
	c.files = files
	return c
}

// The included groups the cases of !include write.
var (
	groupFile  = map[string]string{"group.yaml": "zeta:\n  type: queue\nalpha:\n  type: latest\n  filter: \"kv/#\"\n"}
	otherFile  = map[string]string{"other.yaml": "other:\n  type: append\n"}
	groupFiles = map[string]string{"group.yaml": groupFile["group.yaml"], "other.yaml": otherFile["other.yaml"]}
)

var channelAccepted = slices.Concat(
	each(channelValues, func(v channelValue) printed { return oneChannel(v.kind+one(absent, v.pairs...), v.kind, v.pairs...) }),
	// Names: "A channel name contains 1 to 128 of A–Z, a–z, 0–9, ., _ and -.
	// It is not . or .. and does not end in __dlq".
	each([]string{"a", "9", "Orders.EU-1_x", strings.Repeat("n", 128), "...", ".hidden", "a..b", "x__dlqx", "__dlq_", "-", "_"},
		func(name string) printed {
			return channelsAs("accepted "+name, fmt.Sprintf(" {%q: {type: append}}", name), nil, channel("append", name))
		}),
	// filter: the topics it holds, a valid MQTT topic filter (design.md,
	// Words): + and # each a whole level, # only last; MQTT allows an empty
	// level, inside a filter or at its end; no level is __dlq, a level
	// holding it among other characters is not; {a,b}, the first level
	// "alone or as {a,b}"; a comma in a level holding neither { nor } is
	// the level's own. The filter is printed as configured, {a,b} kept.
	each([][2]string{
		{"append", "sensors/+/temp"}, {"latest", "sensors"}, {"queue", "jobs/#"}, {"append", "a/+/+/#"},
		{"append", "a//b"}, {"append", "a/b/"}, {"append", "a/x__dlq/#"}, {"queue", "a/__dlqx"},
		{"append", "fleet/{north,south}/#"}, {"latest", "kv/{a,b,c}"}, {"append", "x/{a,b}/{c,d}"}, {"append", "x/{b,a}/+"},
		{"append", "{orders,invoices}/#"}, {"latest", "{a,b}"}, {"append", "x/a,b"}, {"latest", "a,b/c"},
	}, func(f [2]string) printed {
		return oneChannel(f[0]+" "+f[1], f[0], "filter", strconv.Quote(f[1]))
	}),
	[]printed{
		// filter: "{a,b} expanded, each filter within
		// limits.max_topic_length and limits.max_topic_levels": each
		// expansion is measured, never the filter as written;
		// max_topic_length is "in bytes"; # and + are levels. The default
		// <name>/# is measured as a written filter is.
		channelsAs("8 bytes", one("append", "filter", `"ab/cdefg"`), maxLength8, channel("append", "c", "filter", `"ab/cdefg"`)),
		channelsAs("13 bytes written, each expansion 5", one("append", "filter", `"ab/{cd,ef,gh}"`), maxLength8,
			channel("append", "c", "filter", `"ab/{cd,ef,gh}"`)),
		channelsAs("8 bytes in 4 characters", one("latest", "filter", `"éééé"`), maxLength8, channel("latest", "c", "filter", `"éééé"`)),
		channelsAs("3 levels", one("append", "filter", `"a/b/c"`), maxLevels3, channel("append", "c", "filter", `"a/b/c"`)),
		channelsAs("3 levels ending in #", one("append", "filter", `"a/b/#"`), maxLevels3, channel("append", "c", "filter", `"a/b/#"`)),
		channelsAs("3 levels with {a,b}", one("latest", "filter", `"a/{b,c}/d"`), maxLevels3, channel("latest", "c", "filter", `"a/{b,c}/d"`)),
		channelsAs("a default of 8 bytes", " {abcdef: {type: append}}", maxLength8, channel("append", "abcdef")),

		// "no two channels with one filter", after {a,b} expansion;
		// overlapping filters are allowed (design.md, Words: "Where filters
		// overlap, the topic goes to the filter that spells it out most
		// exactly").
		channelsAs("overlapping filters",
			` {a: {type: append, filter: "x/#"}, b: {type: latest, filter: "x/+"},`+
				` c: {type: queue, filter: "x/a"}, d: {type: append, filter: "x/{a,b}/#"}, e: {type: append, filter: "x"}}`, nil,
			channel("append", "a", "filter", `"x/#"`), channel("latest", "b", "filter", `"x/+"`), channel("queue", "c", "filter", `"x/a"`),
			channel("append", "d", "filter", `"x/{a,b}/#"`), channel("append", "e", "filter", `"x"`)),

		// visibility_timeout: "shorter than retention_period where one is
		// set", the channel's retention_period as resolved; none sets none.
		oneChannel("59s under 1m", "queue", "visibility_timeout", "59s", "retention_period", "1m"),
		oneChannel("1d with no retention", "queue", "visibility_timeout", "1d", "retention_period", "none"),

		// "The channels: section is one mapping of channels, or a list whose
		// entries are each a group of channels. A group is written in place
		// or as !include <file>"; the order written; "Anchors and aliases are
		// allowed": a channel's keys reused.
		channelsAs("groups in place, in order", " [{zeta: {type: append}, alpha: {type: latest}}, {mid: {type: queue}}]", nil,
			channel("append", "zeta"), channel("latest", "alpha"), channel("queue", "mid")),
		channelsAs("a block list of groups", "\n  - zeta:\n      type: append\n  - alpha:\n      type: latest\n      filter: \"kv/{a,b}\"\n", nil,
			channel("append", "zeta"), channel("latest", "alpha", "filter", `"kv/{a,b}"`)),
		channelsAs("an aliased channel", " {a: &q {type: queue, visibility_timeout: 1m}, b: *q}", nil,
			channel("queue", "a", "visibility_timeout", "1m"), channel("queue", "b", "visibility_timeout", "1m")),

		// An included file's channels stand where its !include stands.
		channelsIn("included groups among groups in place", groupFile,
			entries("{first: {type: append}}", "!include "+dirToken+"/group.yaml", "{last: {type: latest}}"), nil,
			channel("append", "first"), channel("queue", "zeta"), channel("latest", "alpha", "filter", `"kv/#"`), channel("latest", "last")),
		channelsIn("included groups first and last", groupFiles,
			entries("!include "+dirToken+"/other.yaml", "{first: {type: append}}", "!include "+dirToken+"/group.yaml"), nil,
			channel("append", "other"), channel("append", "first"), channel("queue", "zeta"), channel("latest", "alpha", "filter", `"kv/#"`)),
		// Values: a path Sagüin writes "must differ from every other path in
		// the configuration": files apart are no such path.
		channelsIn("the same files apart", map[string]string{"saguin.pid": "p:\n  type: append\n"},
			entries("!include "+dirToken+"/saguin.pid"), []string{"broker.storage.file_path", dirToken + "/saguin.db"},
			channel("append", "p")),
		// "its path names one regular file": a regular file, and a symbolic
		// link to one, is read.
		channelsIn("an included regular file", map[string]string{"regular.yaml": "r:\n  type: append\n"},
			entries("!include "+dirToken+"/regular.yaml"), nil, channel("append", "r")),
		{
			name:  "an included symbolic link to a regular file",
			sets:  []string{"channels", entries("!include " + dirToken + "/link.yaml")},
			files: map[string]string{"regular.yaml": "r:\n  type: append\n"},
			setup: func(t *testing.T, dir string) {
				if err := os.Symlink(filepath.Join(dir, "regular.yaml"), filepath.Join(dir, "link.yaml")); err != nil {
					t.Fatal(err)
				}
			},
			want: channelsAre(channel("append", "r")),
		},
	},
)

// The limits the filter-limit cases set.
var (
	maxLength8 = []string{"broker.limits.max_topic_length", "8"}
	maxLevels3 = []string{"broker.limits.max_topic_levels", "3"}
)

// channelsMarshalGroup is a queue setting every key, in an included group.
const channelsMarshalGroup = "jobs:\n  type: queue\n  filter: \"work/+/#\"\n  start: tail\n  retention_period: 90m\n  retention_bytes: 1536KiB\n" +
	"  visibility_timeout: 45s\n  retry: {max_attempts: 9, mode: linear, delay: 1500ms}\n" +
	"  dlq_retention_period: none\n  dlq_retention_bytes: 2GiB\n"

// channelRoundTrips: --output prints each channel's keys for its type, no
// key of another type (config.md: such a key "is refused"), no !include;
// read back, the same channels.
var channelRoundTrips = []roundTrip{{printed: channelsIn("a channel of each type, one included, every key set",
	map[string]string{"group.yaml": channelsMarshalGroup},
	"\n  - events: {type: append, filter: \"fleet/{north,south}/#\", max_bytes: 10MiB, retention_bytes: none}"+
		"\n    state: {type: latest, tombstone_retention_period: 36h, retention_period: none}"+
		"\n  - !include "+dirToken+"/group.yaml\n", nil,
	channel("append", "events", "filter", `"fleet/{north,south}/#"`, "max_bytes", "10MiB"),
	channel("latest", "state", "tombstone_retention_period", "36h", "retention_period", "none"),
	channel("queue", "jobs", "filter", `"work/+/#"`, "start", "tail", "retention_period", "90m", "retention_bytes", "1536KiB",
		"visibility_timeout", "45s", "retry.max_attempts", "9", "retry.mode", "linear", "retry.delay", "1500ms",
		"dlq_retention_period", "none", "dlq_retention_bytes", "2GiB"),
)}}

// channelRefusal is a case of the channels text refused naming key.
func channelRefusal(name, text, key string, says ...string) refusal {
	return refuse(name, key, file("channels", text), says...)
}

// includeRefused is a case of the channels text, with files written beside
// it, refused naming key and each of says.
func includeRefused(name string, files map[string]string, text, key string, says ...string) refusal {
	r := channelRefusal(name, text, key, says...)
	r.files = files
	return r
}

// The replacements the Dropped table gives a queue's keys.
const (
	retryInstead     = "retry.mode, retry.delay"
	retentionInstead = "its retention_bytes, retention_period"
)

// includedKind is an !include of a path that names no regular file, refused
// at its entry, the message naming the path, and promptly (aloneLimit): the
// file is never read to an end that does not come, nor waited on.
func includedKind(name, path string, setup func(t *testing.T, dir string)) refusal {
	r := channelRefusal(name, entries("!include "+path), "channels[0]", path)
	r.setup, r.alone = setup, true
	return r
}

// device skips a case where the system has no device at path.
func device(path string) func(t *testing.T, dir string) {
	return func(t *testing.T, _ string) {
		if _, err := os.Stat(path); err != nil {
			t.Skipf("no %s here to include: %v", path, err)
		}
	}
}

var channelValuesRefused = slices.Concat(
	each([][3]string{
		// type: "required", "append, latest or queue".
		{absent, "", "type"},
		{absent, "filter=\"c/#\"", "type"},
		{"Append", "", "type"},
		{"log", "", "type"},
		{"compacted", "", "type"},
		{"none", "", "type"},
		{`""`, "", "type"},
		{"~", "", "type"},
		{"[append]", "", "type"},
	}, func(v [3]string) refusal {
		var pairs []string
		if key, value, ok := strings.Cut(v[1], "="); ok {
			pairs = []string{key, value}
		}
		return channelRefusal(one(v[0], pairs...), one(v[0], pairs...), "channels.c."+v[2])
	}),
	each([]channelRefusedValue{
		// start: "floor or tail"; none is not its default.
		{"append", []string{"start", "Tail"}, "start"},
		{"append", []string{"start", "head"}, "start"},
		{"queue", []string{"start", "0"}, "start"},
		{"append", []string{"start", "none"}, "start"},
		// Values: a duration is a whole number above 0 and one unit; "every
		// other duration is at least 1s".
		{"append", []string{"retention_period", "0s"}, "retention_period"},
		{"append", []string{"retention_period", "-1s"}, "retention_period"},
		{"append", []string{"retention_period", "1.5h"}, "retention_period"},
		{"append", []string{"retention_period", "1h30m"}, "retention_period"},
		{"append", []string{"retention_period", "60"}, "retention_period"},
		{"latest", []string{"retention_period", "500ms"}, "retention_period"},
		{"queue", []string{"retention_period", "1000ms"}, "retention_period"},
		{"append", []string{"retention_period", "106752d"}, "retention_period"},
		{"latest", []string{"tombstone_retention_period", "500ms"}, "tombstone_retention_period"},
		{"latest", []string{"tombstone_retention_period", "0s"}, "tombstone_retention_period"},
		{"queue", []string{"visibility_timeout", "500ms"}, "visibility_timeout"},
		{"queue", []string{"visibility_timeout", "1500ms"}, "visibility_timeout"},
		{"queue", []string{"visibility_timeout", "0s"}, "visibility_timeout"},
		{"queue", []string{"dlq_retention_period", "999ms"}, "dlq_retention_period"},
		{"queue", []string{"retry.delay", "0ms"}, "retry.delay"},
		{"queue", []string{"retry.delay", "1.5s"}, "retry.delay"},
		{"queue", []string{"retry.delay", "5us"}, "retry.delay"},
		{"queue", []string{"retry.delay", "5"}, "retry.delay"},
		// Values: a size is a whole number of bytes above 0, optionally
		// KiB, MiB or GiB; none past 9,223,372,036,854,775,807.
		{"append", []string{"retention_bytes", "0"}, "retention_bytes"},
		{"append", []string{"retention_bytes", "1KB"}, "retention_bytes"},
		{"queue", []string{"retention_bytes", "1.5GiB"}, "retention_bytes"},
		{"append", []string{"max_bytes", "0"}, "max_bytes"},
		{"append", []string{"max_bytes", "1TiB"}, "max_bytes"},
		{"append", []string{"max_bytes", "9223372036854775808"}, "max_bytes"},
		{"append", []string{"max_bytes", "10s"}, "max_bytes"},
		{"queue", []string{"dlq_retention_bytes", "0KiB"}, "dlq_retention_bytes"},
		// Values: "A count is at least 1 unless its row says otherwise".
		{"queue", []string{"retry.max_attempts", "0"}, "retry.max_attempts"},
		{"queue", []string{"retry.max_attempts", "-1"}, "retry.max_attempts"},
		{"queue", []string{"retry.max_attempts", "1.5"}, "retry.max_attempts"},
		{"queue", []string{"retry.max_attempts", "9223372036854775808"}, "retry.max_attempts"},
		// retry.mode: "fixed, linear or exponential".
		{"queue", []string{"retry.mode", "Fixed"}, "retry.mode"},
		{"queue", []string{"retry.mode", "jitter"}, "retry.mode"},
		{"queue", []string{"retry.mode", "none"}, "retry.mode"},
		// Values: "none is accepted where a row says so, or where it is the
		// default": refused where the default is a value and the row says
		// nothing of none.
		{"latest", []string{"tombstone_retention_period", "none"}, "tombstone_retention_period"},
		{"queue", []string{"visibility_timeout", "none"}, "visibility_timeout"},
		{"queue", []string{"retry.max_attempts", "none"}, "retry.max_attempts"},
		{"queue", []string{"retry.delay", "none"}, "retry.delay"},
		// filter: "never none". Values: "The loader refuses a key written
		// empty or ~. Leave it out for its default", a key whose default is
		// none included.
		{"append", []string{"filter", "none"}, "filter"},
		{"append", []string{"retention_period", "~"}, "retention_period"},
		{"append", []string{"max_bytes", ""}, "max_bytes"},
		{"append", []string{"filter", "~"}, "filter"},
		{"queue", []string{"retry.mode", ""}, "retry.mode"},
		{"queue", []string{"retry", "~"}, "retry"},
		// A value of the wrong shape: a list or mapping for a scalar, a
		// scalar for retry's mapping.
		{"append", []string{"retention_period", "[1d]"}, "retention_period"},
		{"append", []string{"filter", `["c/#"]`}, "filter"},
		{"queue", []string{"retry", "3"}, "retry"},
		// read strictly: an unknown key, in a channel or its retry; a key
		// twice.
		{"append", []string{"colour", "blue"}, "colour"},
		{"queue", []string{"retry.colour", "blue"}, "retry.colour"},
		{"queue", []string{"retry.max_attempts", "3", "retry.max_attempts", "4"}, "retry.max_attempts"},

		// "A key on a type it does not belong to is refused": each key the
		// table marks with types, on every other type, written before type.
		// A queue's max_bytes is a Dropped row.
		{"latest", []string{"start", "floor"}, "start"},
		{"latest", []string{"retention_bytes", "1GiB"}, "retention_bytes"},
		{"latest", []string{"max_bytes", "1GiB"}, "max_bytes"},
		{"append", []string{"tombstone_retention_period", "1d"}, "tombstone_retention_period"},
		{"queue", []string{"tombstone_retention_period", "1d"}, "tombstone_retention_period"},
		{"append", []string{"visibility_timeout", "30s"}, "visibility_timeout"},
		{"latest", []string{"visibility_timeout", "30s"}, "visibility_timeout"},
		{"append", []string{"retry.max_attempts", "5"}, "retry"},
		{"latest", []string{"retry.mode", "fixed"}, "retry"},
		{"append", []string{"retry.delay", "5s"}, "retry"},
		{"append", []string{"dlq_retention_period", "1d"}, "dlq_retention_period"},
		{"latest", []string{"dlq_retention_period", "1d"}, "dlq_retention_period"},
		{"append", []string{"dlq_retention_bytes", "1GiB"}, "dlq_retention_bytes"},
		{"latest", []string{"dlq_retention_bytes", "1GiB"}, "dlq_retention_bytes"},
	}, func(v channelRefusedValue) refusal {
		return channelRefusal(v.kind+one(absent, v.pairs...), one(v.kind, v.pairs...), "channels.c."+v.key)
	}),
	[]refusal{
		// A channel is a mapping of its keys.
		channelRefusal("a channel written as a scalar", " {c: append}", "channels.c"),
		channelRefusal("a channel written as a list", " {c: [append]}", "channels.c"),
		channelRefusal("a channel written empty", " {c: ~}", "channels.c"),
		// "unique": in one mapping, a key written twice (read strictly).
		channelRefusal("a name twice in one mapping", " {a: {type: append}, a: {type: latest}}", "channels.a"),
	},
	// Names: ASCII alone, a letter outside it refused. A refused name is
	// named by its key, channels.<name>, and the empty name by the mapping
	// it stands in, channels (the manager's decision, audit 261015-006a).
	each([]string{
		"", strings.Repeat("n", 129), ".", "..", "x__dlq", "__dlq", "orders.__dlq",
		"a b", "a/b", "a+b", "a#b", "$a", "a:b", "a*b", "a,b", "a{b}", "a\tb",
		// Letters, but not A–Z or a–z.
		"é", "café", "naïve", "Ω", "日本",
	}, func(name string) refusal {
		key := "channels"
		if name != "" {
			key += "." + name
		}
		return channelRefusal(fmt.Sprintf("refused %q", name), fmt.Sprintf(" {%q: {type: append}}", name), key)
	}),
)

// channelRefusedValue is one key's value on a channel of a type, refused
// naming the key under channels.c.
type channelRefusedValue struct {
	kind  string
	pairs []string
	key   string
}

// badChannelFilters: config.md, filter: "never none: the first level
// written out (alone or as {a,b}), not empty and not starting with $, no
// __dlq level; a level holding { or } is exactly one {a,b} (not on a queue)
// offering alternatives: at least two, each a non-empty written level with
// no {, } or ,, none repeated"; and a valid MQTT topic filter (MQTT 5 4.7.1,
// 1.5.4): + and # each a whole level, # only last, no U+0000.
var badChannelFilters = [][2]string{
	// A valid MQTT topic filter: not empty, wildcards whole levels, # last,
	// no U+0000.
	{"append", ""}, {"append", "a/#/b"}, {"append", "a/b#"}, {"append", "a/#b"}, {"append", "a/+b"}, {"append", "a/b+"},
	{"latest", "a/b\x00c"},
	// The first level written out ...
	{"append", "+"}, {"append", "#"}, {"append", "+/a"}, {"latest", "#/a"},
	// ... and not $.
	{"append", "$/a"}, {"append", "$SYS/#"}, {"append", "$saguin/x"}, {"latest", "$share/g/a"},
	// ... and not empty, nor any alternative of a first {a,b} level starting
	// with $ or written otherwise.
	{"append", "/a"}, {"append", "/"}, {"append", "{$SYS,a}/#"}, {"append", "{a,$b}/x"}, {"append", "{a,+}/x"},
	// No __dlq level.
	{"append", "a/__dlq"}, {"queue", "a/__dlq/#"}, {"append", "__dlq/a"}, {"append", "a/{__dlq,b}"},
	// {a,b}: the whole level, at least two, each a written level with no {,
	// } or ',', none repeated.
	{"append", "x/{a}"}, {"append", "x/{}"}, {"append", "x/{a,a}"}, {"append", "x/{a,b,a}"}, {"append", "x/{a,+}"},
	{"append", "x/{a,#}"}, {"latest", "x/{+,#}"}, {"append", "x/a{b,c}"}, {"append", "x/{b,c}d"}, {"append", "x/{a,{b,c}}"},
	{"append", "x/{a,b}}"},
	// Each alternative non-empty.
	{"append", "x/{a,}"}, {"append", "x/{,a}"}, {"append", "x/{a,,b}"},
	// A level holding { or } is exactly one {a,b}.
	{"append", "x/{a,b"}, {"append", "x/a}"}, {"append", "x/{a,b}c"}, {"append", "x/{a}{b}"}, {"append", "x/{a,b}{c,d}"},
	{"latest", "x/}a,b{"},
	// Not on a queue.
	{"queue", "x/{a,b}"}, {"queue", "x/{a,b}/#"},
}

var channelRefused = slices.Concat(
	channelValuesRefused,
	each(badChannelFilters, func(f [2]string) refusal {
		return channelRefusal(fmt.Sprintf("%s %q", f[0], f[1]), one(f[0], "filter", fmt.Sprintf("%q", f[1])), "channels.c.filter")
	}),
	[]refusal{
		// Each expansion measured: max_topic_length in bytes, # and + and an
		// empty level all levels; the default <name>/# refused naming
		// channels.<name>.filter.
		refuse("9 bytes", "channels.c.filter", file(slices.Concat(maxLength8, []string{"channels", one("append", "filter", `"ab/cdefgh"`)})...)),
		refuse("one expansion of 9 bytes", "channels.c.filter",
			file(slices.Concat(maxLength8, []string{"channels", one("append", "filter", `"ab/{x,cdefgh}"`)})...)),
		refuse("9 bytes in 5 characters", "channels.c.filter", file(slices.Concat(maxLength8, []string{"channels", one("latest", "filter", `"ééééa"`)})...)),
		refuse("4 levels", "channels.c.filter", file(slices.Concat(maxLevels3, []string{"channels", one("append", "filter", `"a/b/c/d"`)})...)),
		refuse("4 levels ending in #", "channels.c.filter", file(slices.Concat(maxLevels3, []string{"channels", one("queue", "filter", `"a/b/c/#"`)})...)),
		refuse("4 levels, two empty", "channels.c.filter", file(slices.Concat(maxLevels3, []string{"channels", one("append", "filter", `"a//b/"`)})...)),
		refuse("4 levels with {a,b}", "channels.c.filter",
			file(slices.Concat(maxLevels3, []string{"channels", one("append", "filter", `"a/{b,c}/d/+"`)})...)),
		refuse("a default of 9 bytes", "channels.abcdefg.filter", file(slices.Concat(maxLength8, []string{"channels", " {abcdefg: {type: append}}"})...)),
		refuse("a default of 2 levels, 1 allowed", "channels.a.filter",
			file("broker.limits.max_topic_levels", "1", "channels", " {a: {type: latest}}")),

		// "no two channels with one filter", after {a,b} expansion: the
		// later channel's filter is refused, naming the earlier channel.
		channelRefusal("one filter written twice", ` {first: {type: append, filter: "x/#"}, second: {type: latest, filter: "x/#"}}`,
			"channels.second.filter", "first"),
		channelRefusal("a written filter equal to a default one", ` {orders: {type: queue}, second: {type: append, filter: "orders/#"}}`,
			"channels.second.filter", "orders"),
		channelRefusal("a filter equal to an expansion", ` {first: {type: append, filter: "x/{p,q}"}, second: {type: append, filter: "x/q"}}`,
			"channels.second.filter", "first"),
		channelRefusal("two expansions alike", ` {first: {type: latest, filter: "x/{p,q}/+"}, second: {type: append, filter: "x/{q,r}/+"}}`,
			"channels.second.filter", "first"),

		// visibility_timeout: "shorter than retention_period where one is
		// set", its storage default included: refused naming
		// retention_period.
		channelRefusal("equal", one("queue", "visibility_timeout", "1m", "retention_period", "60s"), "channels.c.visibility_timeout", "retention_period"),
		channelRefusal("longer", one("queue", "visibility_timeout", "2h", "retention_period", "1h"), "channels.c.visibility_timeout", "retention_period"),
		channelRefusal("the default 30s against 30s", one("queue", "retention_period", "30s"), "channels.c.visibility_timeout", "retention_period"),
		refuse("the default 30s against the storage default", "channels.c.visibility_timeout",
			file("broker.storage.default_retention_period", "20s", "channels", one("queue")), "retention_period"),

		// The Dropped rows for a channel's keys: storage under a channel; a
		// queue's retry.backoff and retry.backoff_base; a queue's max_bytes
		// and job_expires_after; deletion_retention_period, whose
		// replacement is tombstone_retention_period. Where channels is a
		// list, the key's path holds the entry's index.
		channelRefusal("storage on an append channel", one("append", "storage", "local"), "channels.c.storage", theStore),
		channelRefusal("storage on a queue", one("queue", "storage", "{type: sqlite}"), "channels.c.storage", theStore),
		channelRefusal("retry.backoff", one("queue", "retry.backoff", "exponential"), "channels.c.retry.backoff", retryInstead),
		channelRefusal("retry.backoff_base", one("queue", "retry.backoff_base", "1s"), "channels.c.retry.backoff_base", retryInstead),
		channelRefusal("retry.backoff none", one("queue", "retry.backoff", "none"), "channels.c.retry.backoff", retryInstead),
		channelRefusal("a queue's max_bytes", one("queue", "max_bytes", "1GiB"), "channels.c.max_bytes", retentionInstead),
		channelRefusal("a queue's job_expires_after", one("queue", "job_expires_after", "1h"), "channels.c.job_expires_after", retentionInstead),
		channelRefusal("deletion_retention_period", one("latest", "deletion_retention_period", "1d"),
			"channels.c.deletion_retention_period", "tombstone_retention_period"),
		channelRefusal("a dropped key in a list's group", " [{a: {type: append}}, {q: {type: queue, retry: {backoff: linear}}}]",
			"channels[1].q.retry.backoff", retryInstead),
		// The row "replica, bridge_source, dlq_bridge_source, [...] | none:
		// refused, with no replacement", for the channel keys among them.
		refusedWithNoReplacement("bridge_source on an append channel", "channels.c.bridge_source", "channels", one("append", "bridge_source", "upstream")),
		refusedWithNoReplacement("bridge_source on a queue", "channels.c.bridge_source", "channels", one("queue", "bridge_source", "upstream")),
		refusedWithNoReplacement("dlq_bridge_source on a queue", "channels.c.dlq_bridge_source", "channels", one("queue", "dlq_bridge_source", "upstream")),
		refusedWithNoReplacement("bridge_source in a list's group", "channels[0].a.bridge_source", "channels", " [{a: {type: latest, bridge_source: upstream}}]"),
		// replica and flush_interval were never channel keys: under a channel
		// they are unknown keys (read strictly), neither that row's refusal nor
		// config.md's Dropped replacement "Syncing follows the storage mode
		// (Store)."
		{name: "replica under a channel", configuration: file("channels", one("append", "replica", "1")), key: "channels.c.replica",
			never: []string{"instead", noReplacement}},
		{name: "flush_interval under a channel", configuration: file("channels", one("append", "flush_interval", "1")), key: "channels.c.flush_interval",
			never: []string{"instead", noReplacement}},

		// channels: one mapping or a list of groups, written in place or as
		// !include; names unique across groups, and filters with them.
		channelRefusal("a group's key path", " [{a: {type: append}}, {b: {type: nope}}]", "channels[1].b.type"),
		channelRefusal("a group's name", " [{a: {type: append}}, {b__dlq: {type: append}}]", "channels[1].b__dlq"),
		channelRefusal("a name in two groups", " [{a: {type: append}}, {b: {type: append}, a: {type: latest}}]", "channels[1].a", "channels[0]"),
		channelRefusal("a filter in two groups", ` [{first: {type: append, filter: "x/#"}}, {second: {type: latest, filter: "x/#"}}]`,
			"channels[1].second.filter", "first"),
		channelRefusal("a filter in two groups, expanded", ` [{first: {type: append, filter: "x/{y,z}"}}, {second: {type: latest, filter: "x/z"}}]`,
			"channels[1].second.filter", "first"),
		// A key that is not a non-empty plain string, refused naming the
		// channel it stands in (the manager's decision, audit 261015-006a).
		channelRefusal("an empty key in a channel", ` {c: {type: append, "": 1}}`, "channels.c"),
		channelRefusal("a null key in a group's channel", " [{c: {type: append, ~: 1}}]", "channels[0].c"),
		channelRefusal("an entry that is a name", " [events]", "channels[0]"),
		channelRefusal("an entry that is a list", " [{a: {type: append}}, [{b: {type: append}}]]", "channels[1]"),
		channelRefusal("an entry written empty", " [{a: {type: append}}, ~]", "channels[1]"),
		channelRefusal("a tag other than !include", " [!includes /etc/saguin/channels.yaml]", "channels[0]"),
		channelRefusal("channels written empty", "", "channels"),
		channelRefusal("channels written ~", " ~", "channels"),
		channelRefusal("channels a scalar", " events", "channels"),
		channelRefusal("channels one !include", " !include /etc/saguin/channels.yaml", "channels"),
		// "Neither channels:, a group nor an included file is empty: with no
		// channels, channels: is left out."
		channelRefusal("channels an empty mapping", " {}", "channels"),
		channelRefusal("channels an empty list", " []", "channels"),
		channelRefusal("an empty group", " [{a: {type: append}}, {}]", "channels[1]"),
		channelRefusal("only an empty group", " [{}]", "channels[0]"),
		channelRefusal("a channel written as !include", " {a: !include /etc/saguin/a.yaml}", "channels.a"),
		channelRefusal("a group's channel written as !include", " [{a: !include /etc/saguin/a.yaml}]", "channels[0].a"),
	},
	channelIncludesRefused,
)

// channelIncludesRefused: a group "written in place or as !include <file>. An
// included file holds one group and no !include. Its path names one regular
// file, never a device, pipe or socket. The path contains no *, ? or [", and,
// Values, "Every file path is absolute". A path naming no readable file names
// no file. Inside an included file the configuration is read as strictly as
// the file itself (unknown, repeated and dropped keys, merge keys, one
// document), its key paths under the entry, naming the file. Names and
// filters are unique across files.
var channelIncludesRefused = []refusal{
	includeRefused("an included file that is one !include",
		map[string]string{"nested.yaml": "!include " + dirToken + "/other.yaml\n", "other.yaml": otherFile["other.yaml"]},
		entries("!include "+dirToken+"/nested.yaml"), "channels[0]", dirToken+"/nested.yaml"),
	includeRefused("an !include inside an included file",
		map[string]string{"nested-channel.yaml": "a:\n  type: append\nb: !include " + dirToken + "/other.yaml\n", "other.yaml": otherFile["other.yaml"]},
		entries("!include "+dirToken+"/nested-channel.yaml"), "channels[0].b", dirToken+"/nested-channel.yaml"),
	includeRefused("an included list", map[string]string{"list.yaml": "- a:\n    type: append\n"},
		entries("!include "+dirToken+"/list.yaml"), "channels[0]", dirToken+"/list.yaml"),
	includeRefused("an included scalar", map[string]string{"scalar.yaml": "events\n"},
		entries("!include "+dirToken+"/scalar.yaml"), "channels[0]", dirToken+"/scalar.yaml"),
	includeRefused("an empty included file", map[string]string{"empty.yaml": ""},
		entries("{x: {type: append}}", "!include "+dirToken+"/empty.yaml"), "channels[1]", dirToken+"/empty.yaml"),
	includeRefused("an included file of comments only", map[string]string{"comments.yaml": "# no channels yet\n"},
		entries("!include "+dirToken+"/comments.yaml"), "channels[0]", dirToken+"/comments.yaml"),
	includeRefused("an included empty mapping", map[string]string{"empty-mapping.yaml": "{}\n"},
		entries("!include "+dirToken+"/empty-mapping.yaml"), "channels[0]", dirToken+"/empty-mapping.yaml"),
	includeRefused("an included file of two documents", map[string]string{"two.yaml": "a:\n  type: append\n---\nb:\n  type: append\n"},
		entries("!include "+dirToken+"/two.yaml"), "channels[0]", dirToken+"/two.yaml"),

	// Its path: one file, absolute, with no *, ? or [.
	channelRefusal("a missing file", entries("!include "+dirToken+"/missing.yaml"), "channels[0]", dirToken+"/missing.yaml"),
	{
		name: "a directory",
		sets: []string{"channels", entries("!include " + dirToken + "/a-directory")}, setup: directory("a-directory"),
		key:  "channels[0]",
		says: []string{dirToken + "/a-directory"},
	},
	includeRefused("a relative path", otherFile, entries("!include other.yaml"), "channels[0]"),
	channelRefusal("no path", entries("!include"), "channels[0]"),
	channelRefusal("an empty path", entries(`!include ""`), "channels[0]"),
	// The wildcard characters, each in the name of a file that exists, so
	// only the character can refuse it.
	includeRefused("a path with *", map[string]string{"ch*.yaml": "s:\n  type: append\n"}, entries("!include "+dirToken+"/ch*.yaml"), "channels[0]"),
	includeRefused("a path with ?", map[string]string{"ch?.yaml": "q:\n  type: append\n"}, entries("!include "+dirToken+"/ch?.yaml"), "channels[0]"),
	includeRefused("a path with [", map[string]string{"ch[1].yaml": "b:\n  type: append\n"}, entries("!include "+dirToken+"/ch[1].yaml"), "channels[0]"),
	includeRefused("a mapping for a path", otherFile, entries("!include {path: "+dirToken+"/other.yaml}"), "channels[0]"),
	includeRefused("a list for a path", otherFile, entries("!include ["+dirToken+"/other.yaml]"), "channels[0]"),
	// A file that exists but cannot be read names no file it can read.
	{
		name: "an unreadable file",
		sets: []string{"channels", entries("!include " + dirToken + "/unreadable.yaml")}, setup: unreadable("unreadable.yaml"),
		key:  "channels[0]",
		says: []string{dirToken + "/unreadable.yaml"},
	},
	// "never a device, pipe or socket".
	includedKind("dev_zero", "/dev/zero", device("/dev/zero")),
	includedKind("dev_null", "/dev/null", device("/dev/null")),
	includedKind("fifo", dirToken+"/fifo.yaml", func(t *testing.T, dir string) {
		if err := syscall.Mkfifo(filepath.Join(dir, "fifo.yaml"), 0o600); err != nil {
			t.Skipf("no FIFO can be made here to include: %v", err)
		}
	}),
	includedKind("socket", dirToken+"/socket.yaml", func(t *testing.T, dir string) {
		// A socket is bound by a path of at most 107 bytes (unix(7)), and
		// the case's folder lies as deep as the checkout: the socket is
		// bound through the folder's descriptor, /proc/self/fd/<fd>, a
		// path as short wherever the checkout lies.
		folder, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = folder.Close() })
		l, err := net.Listen("unix", fmt.Sprintf("/proc/self/fd/%d/socket.yaml", folder.Fd()))
		if err != nil {
			t.Fatalf("making a Unix socket to include: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() }) // before the folder's descriptor closes
	}),

	// Read strictly, its key paths under the entry, naming the file.
	includeRefused("a refused value in an included file", map[string]string{"bad-type.yaml": "a:\n  type: nope\n"},
		entries("{x: {type: append}}", "!include "+dirToken+"/bad-type.yaml"), "channels[1].a.type", dirToken+"/bad-type.yaml"),
	includeRefused("an unknown key in an included file", map[string]string{"unknown.yaml": "a:\n  type: append\n  colour: blue\n"},
		entries("!include "+dirToken+"/unknown.yaml"), "channels[0].a.colour", dirToken+"/unknown.yaml"),
	// A key that is not a non-empty plain string, at the included group's
	// root: refused naming the include entry, the message naming the file
	// (the manager's decision, audit 261015-006a).
	includeRefused("a null key in an included file", map[string]string{"null-key.yaml": "~:\n  type: append\n"},
		entries("!include "+dirToken+"/null-key.yaml"), "channels[0]", dirToken+"/null-key.yaml"),
	includeRefused("a repeated key in an included file", map[string]string{"repeated.yaml": "a:\n  type: append\na:\n  type: latest\n"},
		entries("!include "+dirToken+"/repeated.yaml"), "channels[0].a", dirToken+"/repeated.yaml"),
	includeRefused("a merge key in an included file", map[string]string{"merge.yaml": "a:\n  <<: {type: append}\n"},
		entries("!include "+dirToken+"/merge.yaml"), "channels[0].a.<<", dirToken+"/merge.yaml"),
	// A dropped key in an included file names its replacement.
	includeRefused("a dropped key in an included file", map[string]string{"backoff.yaml": "q:\n  type: queue\n  retry:\n    backoff_base: 1s\n"},
		entries("!include "+dirToken+"/backoff.yaml"), "channels[0].q.retry.backoff_base", retryInstead, dirToken+"/backoff.yaml"),

	// Unique across files: names and filters.
	includeRefused("one file included twice", otherFile,
		entries("!include "+dirToken+"/other.yaml", "!include "+dirToken+"/other.yaml"), "channels[1].other", "channels[0]"),
	includeRefused("a name in a group and an included file", otherFile,
		entries("{other: {type: latest}}", "!include "+dirToken+"/other.yaml"), "channels[1].other", "channels[0]"),
	includeRefused("a filter in a group and an included file", map[string]string{"same-filter.yaml": "b:\n  type: latest\n  filter: \"first/#\"\n"},
		entries("{first: {type: append}}", "!include "+dirToken+"/same-filter.yaml"), "channels[1].b.filter", "first"),

	// config.md, Values: "A path Sagüin writes must differ from every other
	// path in the configuration. These paths are compared as files at start.
	// They include pid_file, file_path with its -wal, -shm and -journal files
	// and its lock file", an !include's path among them. The include, later
	// in config.md's order than broker, is refused, naming the key it meets.
	// Each included file holds a valid group, so only its path can refuse it.
	{
		name:  "an include naming pid_file",
		sets:  []string{"broker.pid_file", dirToken + "/saguin.pid", "channels", entries("!include " + dirToken + "/saguin.pid")},
		files: map[string]string{"saguin.pid": "p:\n  type: append\n"},
		key:   "channels[0]", says: []string{"broker.pid_file"},
	},
	{
		name: "an include naming pid_file, spelled otherwise",
		sets: []string{"broker.pid_file", dirToken + "/saguin.pid",
			"channels", entries("{x: {type: latest}}", "!include "+dirToken+"/./saguin.pid")},
		files: map[string]string{"saguin.pid": "p:\n  type: append\n"},
		key:   "channels[1]", says: []string{"broker.pid_file"},
	},
	{
		name:  "an include naming file_path's -wal",
		sets:  []string{"broker.storage.file_path", dirToken + "/saguin.db", "channels", entries("!include " + dirToken + "/saguin.db-wal")},
		files: map[string]string{"saguin.db-wal": "w:\n  type: append\n"},
		key:   "channels[0]", says: []string{"broker.storage.file_path"},
	},
}

// directory is a setup that makes the directory name in the case's.
func directory(name string) func(t *testing.T, dir string) {
	return func(t *testing.T, dir string) {
		if err := os.Mkdir(filepath.Join(dir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

// unreadable is a setup that writes the file name, a valid group, and takes
// every permission from it; root reads a file whatever its permissions, so
// the case is skipped there.
func unreadable(name string) func(t *testing.T, dir string) {
	return func(t *testing.T, dir string) {
		if os.Geteuid() == 0 {
			t.Skip("root reads a file whatever its permissions")
		}
		if err := os.Chmod(testkit.WriteFile(t, dir, name, "u:\n  type: append\n"), 0); err != nil {
			t.Fatal(err)
		}
	}
}
