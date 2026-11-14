package main

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/compress/s2"
)

// The broker's keys (config.md: Values, broker, broker.mqtt's own keys,
// broker.storage, broker.broadcast, broker.retained, broker.qos2,
// broker.limits and Dropped; design.md, Store), the file as a whole (one
// document, keys and aliases) and the command line (design.md, Operations).

// leastIncoming is the least max_incoming_memory a max_packet_size of
// packet allows where no bridge has an in rule: config.md,
// max_incoming_memory, "It must hold the largest body one reader can ask
// for: max_packet_size, plus the largest S2 copy of it where
// max_packet_size is over 4 KiB"; the largest copy S2 makes of n bytes is
// s2.MaxEncodedLen(n), the library storage.md names (A stored payload).
func leastIncoming(packet int) int {
	if packet <= 4<<10 {
		return packet
	}
	return packet + s2.MaxEncodedLen(packet)
}

// defaultIncoming is max_incoming_memory's default, 32MiB (config.md).
const defaultIncoming = 32 << 20

// largestUnderDefault is the largest max_packet_size max_incoming_memory's
// default allows.
var largestUnderDefault = sort.Search(defaultIncoming, func(n int) bool { return leastIncoming(n+1) > defaultIncoming })

// requiredDefaults is what --output prints for a file holding only the
// required keys: every key config.md gives a default holds it. There is no
// broker.operations ("absent, Sagüin starts no operations listener"), no
// channels and no bridges; a listener's password_file, which takes the
// broker's none and which no file may write as none, is left out.
const requiredDefaults = `
broker:
  id: edge-1
  log_level: info
  pid_file: none
  delivery_admission_delay: 100ms
  delivery_peer_timeout: 1s
  mqtt:
    listen:                      # "one tcp listener on :1883"
      tcp:
        name: tcp                # "its kind"
        address: ":1883"
        allow_anonymous: true    # "the broker's"
        tls: none
    password_file: none
    allow_anonymous: true        # "true without password_file"
    min_protocol_version: 3.1.1
    acl_file: none               # "none: everything allowed"
  storage:
    file_path: /var/lib/saguin/saguin.db
    mode: full
    default_retention_period: 7d
    default_retention_bytes: none
    publish_commit_interval: none   # "none": groups dispatch without a collection delay
    publish_commit_max_records: 256 # "the smaller of 256 and limits.max_connections"
    read_connections: 2
  broadcast:                     # "the storage defaults"
    retention_period: 7d
    retention_bytes: none
  retained:
    retention_period: none
    max_bytes: none
  qos2:
    max_inflight_per_client: 20
  limits:
    max_packet_size: 1MiB
    max_topic_length: 1024
    max_client_id_length: 256
    max_header_count: 32
    max_connections: 10000
    max_subscriptions: 10000
    max_topic_levels: 200
    max_session_expiry: 30d
    max_keepalive: none
    publish_rate: none
    publish_bytes: none
    write_timeout: 5s
    connect_timeout: 10s
    max_connect_size: 100KiB     # "the smaller of 100KiB and max_packet_size"
    max_connect_rate: 500
    max_inflight: 20
    max_incoming_memory: 32MiB
    max_outgoing_memory: 224MiB
`

var brokerDefaults = []printed{
	// Every key's Default column in config.md's broker, broker.mqtt,
	// broker.storage, broker.broadcast, broker.retained, broker.qos2 and
	// broker.limits tables, for a file that writes only the required keys.
	{name: "the required keys alone", want: []string{"", requiredDefaults}},
}

// checked is the file TestCheckConfig tried: the required keys, a size for
// the storage default and a max_inflight of 7.
var checked = []string{"broker.storage.default_retention_bytes", "1GiB", "broker.limits.max_inflight", "7"}

// value is a case of one key/value pair written over the required keys.
type value struct{ key, value string }

// acceptedValues: each value config.md's Values paragraph and the key's row
// allow, at both edges of its range.
var acceptedValues = []value{
	// Values: "a duration is a whole number above 0 and one unit: s, m, h
	// or d (24 hours)".
	{"broker.limits.connect_timeout", "1s"},
	{"broker.limits.connect_timeout", "90s"},
	{"broker.limits.connect_timeout", "2m"},
	{"broker.limits.connect_timeout", "3h"},
	{"broker.limits.connect_timeout", "1d"},
	// "Only write_timeout, publish_commit_interval [...] also take ms"
	// (publish_commit_interval and sync_interval below).
	{"broker.limits.write_timeout", "1ms"},
	{"broker.limits.write_timeout", "250ms"},
	{"broker.limits.write_timeout", "1d"},

	// Values: "a size is a whole number of bytes above 0, optionally
	// followed by KiB, MiB or GiB". max_incoming_memory "must hold the
	// largest body one reader can ask for: max_packet_size, plus the largest
	// S2 copy of it where max_packet_size is over 4 KiB", max_packet_size
	// 1MiB by default; max_outgoing_memory and publish_bytes have no floor.
	{"broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(1 << 20))},
	{"broker.limits.publish_bytes", "1KiB"},
	{"broker.limits.max_incoming_memory", "3MiB"},
	{"broker.limits.max_incoming_memory", "2GiB"},
	{"broker.limits.max_outgoing_memory", "1"},
	{"broker.limits.max_outgoing_memory", "2GiB"},

	// Values: "a count is at least 1 unless its row says otherwise".
	{"broker.limits.max_connections", "1"},
	{"broker.limits.max_connections", "250000"},

	// broker: id, "1 to 64 of A–Z, a–z, 0–9, - and _".
	{"broker.id", "A"},
	{"broker.id", "Edge_09-z"},
	{"broker.id", "-_"},
	{"broker.id", strings.Repeat("e", 64)},
	// log_level: "debug, info, warn or error".
	{"broker.log_level", "debug"},
	{"broker.log_level", "info"},
	{"broker.log_level", "warn"},
	{"broker.log_level", "error"},
	// pid_file, default none; "every file path is absolute".
	{"broker.pid_file", "/run/saguin.pid"},
	{"broker.pid_file", "none"},
	// delivery_admission_delay, "none disables the pause", and
	// delivery_peer_timeout: Values, "Only write_timeout,
	// publish_commit_interval, delivery_admission_delay,
	// delivery_peer_timeout [...] also take ms".
	{"broker.delivery_admission_delay", "150ms"},
	{"broker.delivery_admission_delay", "none"},
	{"broker.delivery_peer_timeout", "150ms"},

	// broker.mqtt. password_file, default none. allow_anonymous, false
	// only with an identity source (the listeners' cases).
	// min_protocol_version: "3.1.1 or 5". acl_file, "none: everything
	// allowed"; a file needs a listener with an identity source.
	{"broker.mqtt.password_file", dirToken + "/passwd"},
	{"broker.mqtt.password_file", "none"},
	{"broker.mqtt.allow_anonymous", "true"},
	{"broker.mqtt.min_protocol_version", "3.1.1"},
	{"broker.mqtt.min_protocol_version", "5"},
	{"broker.mqtt.min_protocol_version", `"5"`},
	{"broker.mqtt.acl_file", "none"},

	// broker.storage.
	{"broker.storage.file_path", "/data/saguin.db"},
	// mode: "full, normal or memory" (memory mode below).
	{"broker.storage.mode", "full"},
	{"broker.storage.mode", "normal"},
	// default_retention_period and _bytes: "none allowed".
	{"broker.storage.default_retention_period", "1s"},
	{"broker.storage.default_retention_period", "none"},
	{"broker.storage.default_retention_bytes", "1"},
	{"broker.storage.default_retention_bytes", "none"},
	// read_connections: "2 to 8; request handling follows Store" (memory mode
	// below).
	{"broker.storage.read_connections", "2"},
	{"broker.storage.read_connections", "8"},
	{"broker.storage.memory_dir", "/mnt/ram"},
	// sync_interval: "at least 100ms"; Values: "Only [...] sync_interval
	// [...] also take ms".
	{"broker.storage.sync_interval", "100ms"},
	{"broker.storage.sync_interval", "1500ms"},
	{"broker.storage.sync_interval", "1s"},
	{"broker.storage.sync_interval", "1d"},
	// publish_commit_interval, default none: "none is accepted [...] where
	// it is the default" (Values), read as leaving it out.
	{"broker.storage.publish_commit_interval", "none"},
	// publish_commit_max_records: "the record limit per publication batch,
	// 1 to limits.max_connections, with or without an interval" (config.md,
	// broker.storage), max_connections being 10000 by default.
	{"broker.storage.publish_commit_max_records", "1"},
	{"broker.storage.publish_commit_max_records", "10000"},
	// snapshot_interval: "at least 1m".
	{"broker.storage.snapshot_interval", "1m"},
	{"broker.storage.snapshot_interval", "60s"},
	{"broker.storage.snapshot_interval", "1d"},

	// broker.broadcast: "none allowed".
	{"broker.broadcast.retention_period", "1s"},
	{"broker.broadcast.retention_period", "none"},
	{"broker.broadcast.retention_bytes", "1"},
	{"broker.broadcast.retention_bytes", "none"},
	// broker.retained, both default none.
	{"broker.retained.retention_period", "1s"},
	{"broker.retained.retention_period", "none"},
	{"broker.retained.max_bytes", "1"},
	{"broker.retained.max_bytes", "none"},
	// broker.qos2: max_inflight_per_client "1 to 65,535".
	{"broker.qos2.max_inflight_per_client", "1"},
	{"broker.qos2.max_inflight_per_client", "65535"},

	// broker.limits. max_packet_size: "at least 20 bytes, at most
	// 268,435,460", 32MiB being max_incoming_memory's default, which "must
	// hold the largest body one reader can ask for" (the largest is a case
	// of brokerAccepted, with max_incoming_memory).
	{"broker.limits.max_packet_size", "20"},
	{"broker.limits.max_packet_size", strconv.Itoa(largestUnderDefault)},
	// max_topic_length: "1 to 65,535".
	{"broker.limits.max_topic_length", "1"},
	{"broker.limits.max_topic_length", "65535"},
	// max_client_id_length: "23 to 65,535".
	{"broker.limits.max_client_id_length", "23"},
	{"broker.limits.max_client_id_length", "65535"},
	{"broker.limits.max_header_count", "1"},
	{"broker.limits.max_subscriptions", "1"},
	{"broker.limits.max_topic_levels", "1"},
	// max_session_expiry: "at most 4,294,967,295 seconds".
	{"broker.limits.max_session_expiry", "1s"},
	{"broker.limits.max_session_expiry", "4294967295s"},
	{"broker.limits.max_session_expiry", "49710d"},
	// max_keepalive, default none: "at most 65,535 seconds".
	{"broker.limits.max_keepalive", "1s"},
	{"broker.limits.max_keepalive", "65535s"},
	{"broker.limits.max_keepalive", "18h"},
	{"broker.limits.max_keepalive", "none"},
	// publish_rate (a count) and publish_bytes (a size), default none.
	{"broker.limits.publish_rate", "1"},
	{"broker.limits.publish_rate", "none"},
	{"broker.limits.publish_bytes", "1"},
	{"broker.limits.publish_bytes", "none"},
	{"broker.limits.max_connect_size", "1"},
	{"broker.limits.max_connect_size", "1MiB"},
	// max_connect_rate: "none allowed".
	{"broker.limits.max_connect_rate", "1"},
	{"broker.limits.max_connect_rate", "none"},
	{"broker.limits.max_inflight", "1"},
	{"broker.limits.max_inflight", "65535"},

	// Values: "No number exceeds 9,223,372,036,854,775,807 bytes, items or
	// nanoseconds (about 106,751 days)": the largest of each is read.
	{"broker.limits.max_incoming_memory", "9223372036854775807"},
	{"broker.limits.max_outgoing_memory", "9223372036854775807"},
	{"broker.limits.max_connections", "9223372036854775807"},
	{"broker.retained.retention_period", "106751d"},
	{"broker.retained.retention_period", "9223372036s"},
}

// valuesPrinted makes a case of each value: accepted, and printed as read.
func valuesPrinted(values []value) []printed {
	cases := make([]printed, 0, len(values))
	for _, v := range values {
		sets := []string{v.key, v.value}
		switch v.key {
		case "broker.storage.sync_interval":
			sets = append(sets, "broker.storage.mode", "normal")
		case "broker.storage.memory_dir", "broker.storage.snapshot_interval":
			sets = append(sets, "broker.storage.mode", "memory")
		}
		cases = append(cases, printed{name: v.key + "=" + v.value, sets: sets})
	}
	return cases
}

var brokerAccepted = append(valuesPrinted(acceptedValues), []printed{
	// A file read from its path ("Load reads the file at a path"), and the
	// file TestCheckConfig tried.
	{name: "max_inflight 7", sets: []string{"broker.limits.max_inflight", "7"}},
	{name: "the checked file", sets: checked},

	// max_packet_size: "at most 268,435,460, the largest MQTT packet";
	// max_incoming_memory: "It must hold the largest body one reader can ask
	// for: max_packet_size, plus the largest S2 copy of it where
	// max_packet_size is over 4 KiB".
	{name: "max_packet_size at its largest, max_incoming_memory at its least",
		sets: []string{"broker.limits.max_packet_size", "268435460", "broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(268435460))}},
	{name: "max_incoming_memory at its least for max_packet_size 64KiB",
		sets: []string{"broker.limits.max_packet_size", "64KiB", "broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(64 << 10))}},
	{name: "max_incoming_memory at its least for max_packet_size 4KiB",
		sets: []string{"broker.limits.max_packet_size", "4KiB", "broker.limits.max_incoming_memory", "4KiB"}},

	// "One YAML file holding one document": one document marked with ---
	// is read.
	{
		name: "one document marked with ---",
		raw:  "---\n" + yamlOf([]string{"broker.log_level", "warn"}),
		want: []string{"broker.log_level", "warn"},
	},

	// read_connections: "2 to 8; request handling follows Store", in memory
	// mode as in full mode. design.md, Store: "The store has no size limit of
	// its own", so memory mode needs none (config.md's Dropped table:
	// storage.max_bytes).
	{
		name: "memory mode",
		sets: []string{"broker.storage.mode", "memory"},
		want: []string{"broker.storage.mode", "memory", "broker.storage.read_connections", "2"},
	},
	{
		name: "memory mode, 8 read connections",
		sets: []string{"broker.storage.mode", "memory", "broker.storage.read_connections", "8"},
	},

	// publish_commit_interval: "the core's collection period, at most 1s";
	// publish_commit_max_records, by default "the smaller of 256 and
	// limits.max_connections": "the record limit per publication batch, 1
	// to limits.max_connections, with or without an interval" (config.md,
	// broker.storage). Each is written without the other.
	{
		name: "an interval alone",
		sets: []string{"broker.storage.publish_commit_interval", "1ms"},
		want: []string{"broker.storage.publish_commit_interval", "1ms", "broker.storage.publish_commit_max_records", "256"},
	},
	{name: "an interval of 1s", sets: []string{"broker.storage.publish_commit_interval", "1s"}},
	{
		name: "the most of both",
		sets: []string{"broker.storage.publish_commit_interval", "1000ms", "broker.storage.publish_commit_max_records", "10000"},
	},
	{name: "one record, without an interval", sets: []string{"broker.storage.publish_commit_max_records", "1"}},
	{
		name: "records at a smaller max_connections",
		sets: []string{"broker.limits.max_connections", "50", "broker.storage.publish_commit_max_records", "50"},
	},
	{
		name: "the record default under a smaller max_connections",
		sets: []string{"broker.limits.max_connections", "50"},
		want: []string{"broker.limits.max_connections", "50", "broker.storage.publish_commit_max_records", "50"},
	},
	{
		name: "the record default under a max_connections above 256",
		sets: []string{"broker.limits.max_connections", "257"},
		want: []string{"broker.limits.max_connections", "257", "broker.storage.publish_commit_max_records", "256"},
	},

	// max_connect_size: "the smaller of 100KiB and max_packet_size"; "at
	// most max_packet_size". A max_packet_size above largestUnderDefault
	// writes max_incoming_memory at its least for it (max_incoming_memory:
	// "It must hold the largest body one reader can ask for").
	{
		name: "max_connect_size follows a smaller max_packet_size",
		sets: []string{"broker.limits.max_packet_size", "64KiB"},
		want: []string{"broker.limits.max_connect_size", "64KiB"},
	},
	{
		name: "max_connect_size at max_packet_size 100KiB",
		sets: []string{"broker.limits.max_packet_size", "100KiB"},
		want: []string{"broker.limits.max_connect_size", "100KiB"},
	},
	{
		name: "max_connect_size under a larger max_packet_size",
		sets: []string{"broker.limits.max_packet_size", "256MiB", "broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(256 << 20))},
		want: []string{"broker.limits.max_connect_size", "100KiB"},
	},
	{
		name: "max_connect_size written",
		sets: []string{"broker.limits.max_packet_size", "64KiB", "broker.limits.max_connect_size", "1KiB"},
	},
	{
		name: "max_connect_size written at max_packet_size",
		sets: []string{"broker.limits.max_packet_size", "64KiB", "broker.limits.max_connect_size", "64KiB"},
	},

	// broadcast.retention_period and _bytes: "the storage defaults"; "none
	// allowed".
	{
		name: "broadcast takes the storage defaults",
		sets: []string{"broker.storage.default_retention_period", "3d", "broker.storage.default_retention_bytes", "5GiB"},
		want: []string{"broker.broadcast.retention_period", "3d", "broker.broadcast.retention_bytes", "5GiB"},
	},
	{
		name: "broadcast none over the storage defaults",
		sets: []string{
			"broker.storage.default_retention_period", "3d",
			"broker.storage.default_retention_bytes", "5GiB",
			"broker.broadcast.retention_period", "none",
			"broker.broadcast.retention_bytes", "none",
		},
		want: []string{"broker.broadcast.retention_period", "none", "broker.broadcast.retention_bytes", "none"},
	},
	{
		name: "broadcast sets one, takes the other",
		sets: []string{
			"broker.storage.default_retention_period", "3d",
			"broker.storage.default_retention_bytes", "5GiB",
			"broker.broadcast.retention_period", "1h",
		},
		want: []string{"broker.broadcast.retention_period", "1h", "broker.broadcast.retention_bytes", "5GiB"},
	},
	{
		name: "broadcast values over storage none",
		sets: []string{
			"broker.storage.default_retention_period", "none",
			"broker.broadcast.retention_period", "12h",
			"broker.broadcast.retention_bytes", "1MiB",
		},
		want: []string{"broker.broadcast.retention_period", "12h", "broker.broadcast.retention_bytes", "1MiB"},
	},

	// Values: "A path Sagüin writes must differ from every other path in the
	// configuration": a -wal name beside another file_path is no such path.
	{
		name: "a -wal name beside another file_path",
		sets: []string{"broker.pid_file", "/var/lib/saguin/saguin.db-wal", "broker.storage.file_path", "/data/saguin.db"},
	},

	// design.md, Store: "The store has no size limit of its own. Each
	// channel, $broadcast and the retained values have their own limits.
	// [...] The operator sizes the disk", so their limits add up to nothing
	// the configuration refuses.
	{
		name: "size limits add up to no store limit",
		sets: []string{"broker.broadcast.retention_bytes", "1024GiB", "broker.retained.max_bytes", "1024GiB"},
	},

	// "Anchors and aliases are allowed": a tls mapping reused.
	{
		name: "an alias",
		sets: []string{
			"broker.mqtt.listen.tcp", `[{name: a, address: ":8883", tls: &t {` + pair + `}}, {name: b, address: ":8884", tls: *t}]`,
		},
		want: []string{"broker.mqtt.listen.tcp[0].tls", servedTLS, "broker.mqtt.listen.tcp[1].tls", servedTLS},
	},
}...)

// everyKeySet sets every key of the broker outside its listeners and
// operations, each to a value other than its default, values no unit
// divides exactly, none where a default is not, and memory mode.
var everyKeySet = []string{
	"broker.id", "core_2",
	"broker.log_level", "debug",
	"broker.pid_file", "/run/saguin.pid",
	"broker.delivery_admission_delay", "150ms",
	"broker.delivery_peer_timeout", "1500ms",
	"broker.storage.file_path", "/data/saguin.db",
	"broker.storage.mode", "memory",
	"broker.storage.default_retention_period", "90m",
	"broker.storage.default_retention_bytes", "1536KiB",
	"broker.storage.publish_commit_interval", "250ms",
	"broker.storage.publish_commit_max_records", "64",
	"broker.storage.read_connections", "4",
	"broker.storage.memory_dir", "/mnt/ram/saguin",
	"broker.storage.snapshot_interval", "90s",
	"broker.broadcast.retention_period", "none",
	"broker.broadcast.retention_bytes", "1073741825",
	"broker.retained.retention_period", "36h",
	"broker.retained.max_bytes", "10MiB",
	"broker.qos2.max_inflight_per_client", "7",
	"broker.limits.max_packet_size", "64KiB",
	"broker.limits.max_topic_length", "512",
	"broker.limits.max_client_id_length", "64",
	"broker.limits.max_header_count", "8",
	"broker.limits.max_connections", "500",
	"broker.limits.max_subscriptions", "100",
	"broker.limits.max_topic_levels", "16",
	"broker.limits.max_session_expiry", "7d",
	"broker.limits.max_keepalive", "90s",
	"broker.limits.publish_rate", "100",
	"broker.limits.publish_bytes", "64KiB",
	"broker.limits.write_timeout", "1500ms",
	"broker.limits.connect_timeout", "2m",
	"broker.limits.max_connect_size", "16KiB",
	"broker.limits.max_connect_rate", "none",
	"broker.limits.max_inflight", "10",
	"broker.limits.max_incoming_memory", "64MiB",
	"broker.limits.max_outgoing_memory", "512MiB",
}

var brokerRoundTrips = []roundTrip{
	{name: "normal defaults", sets: []string{"broker.storage.mode", "normal"},
		want: []string{"broker.storage.sync_interval", "1s", "broker.storage.memory_dir", absent, "broker.storage.snapshot_interval", absent}},
	{name: "memory defaults", sets: []string{"broker.storage.mode", "memory"},
		want: []string{"broker.storage.sync_interval", absent, "broker.storage.memory_dir", "/dev/shm/saguin", "broker.storage.snapshot_interval", "30m"}},
	{name: "defaults", want: []string{"", requiredDefaults}},
	{name: "the checked file", sets: checked},
	{ // Every listener kind and broker.operations, at their defaults.
		name: "listener defaults",
		sets: listenerDefaults,
		want: listenerDefaultsPrinted},
	{ // broadcast takes the storage defaults it does not set.
		name: "storage defaults inherited",
		sets: []string{"broker.storage.default_retention_period", "3d", "broker.storage.default_retention_bytes", "5GiB"},
		want: []string{
			"broker.storage.default_retention_period", "3d",
			"broker.storage.default_retention_bytes", "5GiB",
			"broker.broadcast", "{retention_period: 3d, retention_bytes: 5GiB}",
		}},
	{name: "every key set",
		sets: append(append([]string(nil), everyKeySet...), everyListenerKey...),
		want: append(append([]string(nil), everyKeySet...), everyListenerKeyPrinted...)},
}

// refusedValues: each value just past what config.md's Values paragraph and
// the key's row allow.
var refusedValues = []value{
	// Values: durations. Above 0, whole, one unit of s, m, h or d.
	{"broker.limits.connect_timeout", "0s"},
	{"broker.limits.connect_timeout", "-5s"},
	{"broker.limits.connect_timeout", "1.5h"},
	{"broker.limits.connect_timeout", "1h30m"},
	{"broker.limits.connect_timeout", "10"},
	{"broker.limits.connect_timeout", "5S"},
	{"broker.limits.connect_timeout", "1w"},
	{"broker.limits.connect_timeout", "5us"},
	{"broker.limits.connect_timeout", `""`},
	// "every other duration is at least 1s": no ms but on the keys named.
	{"broker.limits.connect_timeout", "500ms"},
	{"broker.limits.connect_timeout", "1000ms"},
	{"broker.limits.max_keepalive", "500ms"},
	{"broker.storage.default_retention_period", "999ms"},
	{"broker.retained.retention_period", "1000ms"},
	{"broker.limits.write_timeout", "0ms"},
	// A duration too long to hold is refused, never wrapped.
	{"broker.retained.retention_period", "200000d"},
	{"broker.retained.retention_period", "9223372037s"},

	// Values: sizes. Above 0, whole, bytes or KiB, MiB, GiB.
	{"broker.limits.max_outgoing_memory", "0"},
	{"broker.limits.max_outgoing_memory", "0KiB"},
	{"broker.limits.max_outgoing_memory", "-1"},
	{"broker.limits.max_outgoing_memory", "1.5MiB"},
	{"broker.limits.max_outgoing_memory", "1KB"},
	{"broker.limits.max_outgoing_memory", "1kib"},
	{"broker.limits.max_outgoing_memory", "1B"},
	{"broker.limits.max_outgoing_memory", "1TiB"},
	{"broker.limits.max_outgoing_memory", "9999999999GiB"}, // past the largest number
	{"broker.limits.max_outgoing_memory", "10s"},

	// Values: counts. Whole, at least 1.
	{"broker.limits.max_connections", "0"},
	{"broker.limits.max_connections", "-1"},
	{"broker.limits.max_connections", "1.5"},
	{"broker.limits.max_connections", "1KiB"},
	{"broker.limits.max_connections", "ten"},

	// Values: "every file path is absolute".
	{"broker.storage.file_path", "saguin.db"},
	{"broker.storage.file_path", "./saguin.db"},
	{"broker.storage.file_path", `""`},
	{"broker.pid_file", "run/saguin.pid"},
	{"broker.storage.memory_dir", "shm/saguin"},
	{"broker.mqtt.password_file", "passwd"},
	{"broker.mqtt.password_file", `""`},

	// Values: "none is accepted where a row says so, or where it is the
	// default": refused on every key whose default is a value and whose row
	// does not allow it. write_timeout: "never none".
	{"broker.log_level", "none"},
	{"broker.storage.file_path", "none"},
	{"broker.storage.mode", "none"},
	{"broker.storage.sync_interval", "none"},
	{"broker.storage.read_connections", "none"},
	{"broker.storage.memory_dir", "none"},
	{"broker.storage.publish_commit_max_records", "none"},
	{"broker.storage.snapshot_interval", "none"},
	{"broker.qos2.max_inflight_per_client", "none"},
	{"broker.limits.max_packet_size", "none"},
	{"broker.limits.max_topic_length", "none"},
	{"broker.limits.max_client_id_length", "none"},
	{"broker.limits.max_header_count", "none"},
	{"broker.limits.max_connections", "none"},
	{"broker.limits.max_subscriptions", "none"},
	{"broker.limits.max_topic_levels", "none"},
	{"broker.limits.max_session_expiry", "none"},
	{"broker.limits.write_timeout", "none"},
	{"broker.limits.connect_timeout", "none"},
	{"broker.limits.max_connect_size", "none"},
	{"broker.limits.max_inflight", "none"},
	{"broker.limits.max_outgoing_memory", "none"},

	// broker.id: "1 to 64 of A–Z, a–z, 0–9, - and _".
	{"broker.id", `""`},
	{"broker.id", strings.Repeat("e", 65)},
	{"broker.id", `"edge\n1"`},
	{"broker.id", `"edge-1\n"`},
	{"broker.id", "edge+1"},
	{"broker.id", "edge 1"},
	{"broker.id", "edge.1"},
	{"broker.id", "edge/1"},
	{"broker.id", "édge"},
	{"broker.log_level", "INFO"},
	{"broker.log_level", "trace"},
	// mode: "full, normal or memory", as written; disk is the name the
	// first build gave full mode.
	{"broker.storage.mode", "Memory"},
	{"broker.storage.mode", "ram"},
	{"broker.storage.mode", "disk"},
	{"broker.storage.mode", "FULL"},
	{"broker.storage.mode", "Normal"},
	// sync_interval: "at least 100ms".
	{"broker.storage.sync_interval", "99ms"},
	{"broker.storage.sync_interval", "0ms"},
	{"broker.storage.sync_interval", "-1s"},
	// read_connections: "2 to 8; request handling follows Store".
	{"broker.storage.read_connections", "-1"},
	{"broker.storage.read_connections", "0"},
	{"broker.storage.read_connections", "1"},
	{"broker.storage.read_connections", "9"},
	{"broker.storage.snapshot_interval", "59s"},
	{"broker.qos2.max_inflight_per_client", "0"},
	// "the Receive Maximum sent in CONNACK", a Two Byte Integer of 1 to
	// 65,535 (MQTT 5 3.2.2.3.3).
	{"broker.qos2.max_inflight_per_client", "65536"},
	{"broker.limits.max_packet_size", "0"},
	{"broker.limits.max_packet_size", "19"},
	{"broker.limits.max_packet_size", "268435461"},
	{"broker.limits.max_topic_length", "0"},
	{"broker.limits.max_topic_length", "65536"},
	{"broker.limits.max_client_id_length", "22"},
	{"broker.limits.max_client_id_length", "65536"},
	{"broker.limits.max_header_count", "0"},
	{"broker.limits.max_subscriptions", "0"},
	{"broker.limits.max_topic_levels", "0"},
	{"broker.limits.max_session_expiry", "4294967296s"},
	{"broker.limits.max_session_expiry", "49711d"},
	{"broker.limits.max_keepalive", "65536s"},
	{"broker.limits.max_keepalive", "1093m"},
	{"broker.limits.publish_rate", "0"},
	{"broker.limits.max_connect_rate", "0"},
	{"broker.limits.max_inflight", "0"},
	// Values: "A boolean is true or false"; yes and 1 are not YAML 1.2
	// booleans, and True is not as config.md writes it.
	{"broker.mqtt.allow_anonymous", "yes"},
	{"broker.mqtt.allow_anonymous", "1"},
	{"broker.mqtt.allow_anonymous", "True"},
	{"broker.mqtt.allow_anonymous", "TRUE"},
	{"broker.mqtt.allow_anonymous", "none"},
	// min_protocol_version: "3.1.1 or 5", as written; 4 is 3.1.1's
	// protocol level, not its name.
	{"broker.mqtt.min_protocol_version", "3.1"},
	{"broker.mqtt.min_protocol_version", "3"},
	{"broker.mqtt.min_protocol_version", "4"},
	{"broker.mqtt.min_protocol_version", "311"},
	{"broker.mqtt.min_protocol_version", "5.0"},
	{"broker.mqtt.min_protocol_version", "v5"},
	{"broker.mqtt.min_protocol_version", "none"},

	// Values: "The loader refuses a key written empty or ~. Leave it out for
	// its default", a key whose default is none included.
	{"broker.log_level", ""},
	{"broker.log_level", "~"},
	{"broker.pid_file", ""},
	{"broker.pid_file", "~"},
	{"broker.mqtt.allow_anonymous", "~"},
	{"broker.limits.max_keepalive", "~"},
	{"broker.limits.max_inflight", ""},

	// Values: "No number exceeds 9,223,372,036,854,775,807 bytes, items or
	// nanoseconds": one past, refused, never wrapped.
	{"broker.limits.max_outgoing_memory", "9223372036854775808"},
	{"broker.limits.max_connections", "9223372036854775808"},
	{"broker.retained.retention_period", "106752d"},

	// A value of the wrong shape: a list or a mapping for a scalar.
	{"broker.log_level", "[info]"},
	{"broker.storage.file_path", "{path: /data/saguin.db}"},
}

// valuesRefused makes a case of each value: refused, naming its key.
func valuesRefused(values []value) []refusal {
	cases := make([]refusal, 0, len(values))
	for _, v := range values {
		cases = append(cases, refusal{name: v.key + "=" + v.value, sets: []string{v.key, v.value}, key: v.key})
	}
	return cases
}

// unknownKeys: a key no section takes, at every level, or a section written
// as a value or a value as a section ("one YAML file holding one document.
// The loader reads it strictly. An unknown [...] key stops startup"), the key
// refused by its full path.
var unknownKeys = []refusal{
	{key: "colour", sets: []string{"colour", "blue"}},
	{key: "broker.colour", sets: []string{"broker.colour", "blue"}},
	{key: "broker.storage.colour", sets: []string{"broker.storage.colour", "blue"}},
	{key: "broker.broadcast.colour", sets: []string{"broker.broadcast.colour", "blue"}},
	{key: "broker.retained.colour", sets: []string{"broker.retained.colour", "blue"}},
	{key: "broker.qos2.colour", sets: []string{"broker.qos2.colour", "blue"}},
	{key: "broker.limits.colour", sets: []string{"broker.limits.colour", "blue"}},
	{key: "broker.limits.max_topic_lenght", sets: []string{"broker.limits.max_topic_lenght", "1024"}},
	{key: "broker.limits", sets: []string{"broker.limits", "5"}},
	{key: "broker.id", sets: []string{"broker.id.name", "edge-1"}},

	// broker.mqtt, its listen, a listener (one, or an entry of a list) and
	// its tls; broker.operations and its listen. listen's kinds are tcp, ws
	// and unix; broker.operations takes "tcp and unix listeners", and an
	// operations listener's keys are those it lists. A listener is a
	// mapping of its keys.
	{key: "broker.mqtt.colour", sets: []string{"broker.mqtt.colour", "blue"}},
	{key: "broker.mqtt", sets: []string{"broker.mqtt", "5"}},
	{key: "broker.mqtt.listen.tls", sets: []string{"broker.mqtt.listen.tls", `{address: ":8883"}`}},
	{key: "broker.mqtt.listen.tcp.colour", sets: []string{"broker.mqtt.listen.tcp", `{address: ":1883", colour: blue}`}},
	{key: "broker.mqtt.listen.tcp[1].colour", sets: []string{"broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: b, address: ":1884", colour: blue}]`}},
	{key: "broker.mqtt.listen.tcp", sets: []string{"broker.mqtt.listen.tcp", `":1883"`}},
	{key: "broker.mqtt.listen.ws.tls.colour", sets: []string{"broker.mqtt.listen.ws", `{address: ":8080", tls: {` + pair + `, colour: blue}}`}},
	{key: "broker.operations.colour", sets: []string{"broker.operations", `{listen: {unix: {path: ops.sock}}, colour: blue}`}},
	{key: "broker.operations.allow_anonymous", sets: []string{"broker.operations", `{listen: {unix: {path: ops.sock}}, allow_anonymous: false}`}},
	{key: "broker.operations.listen.ws", sets: []string{"broker.operations.listen.ws", `{address: "127.0.0.1:9090"}`}},
	{key: "broker.operations.listen.tcp.same_origin", sets: []string{"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", same_origin: true}`}},
	{key: "broker.operations.listen.tcp.allowed_origins", sets: []string{"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", allowed_origins: ["https://example.com"]}`}},
	{key: "broker.operations.listen.unix.address", sets: []string{"broker.operations.listen.unix", `{path: ops.sock, address: "127.0.0.1:9090"}`}},
	{key: "broker.operations.listen.unix.tls", sets: []string{"broker.operations.listen.unix", `{path: ops.sock, tls: {` + pair + `}}`}},
	{key: "broker.operations.listen.tcp.tls.colour", sets: []string{"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", tls: {` + pair + `, colour: blue}}`}},
	{key: "broker.operations.listen.tcp[1].colour", sets: []string{"broker.operations.listen.tcp", `[{name: a, address: "127.0.0.1:9090"}, {name: b, address: "127.0.0.1:9091", colour: blue}]`}},

	// Shapes: a list entry that is not a listener; tls, a mapping or none;
	// allowed_origins, a list of origins or none.
	{key: "broker.mqtt.listen.tcp[1]", sets: []string{"broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, ":1884"]`}},
	{key: "broker.mqtt.listen.tcp.tls", sets: []string{"broker.mqtt.listen.tcp", `{address: ":8883", tls: ` + serverCert + `}`}},
	{key: "broker.mqtt.listen.tcp[1].tls", sets: []string{"broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: b, address: ":8883", tls: [` + serverCert + `]}]`}},
	{key: "broker.mqtt.listen.ws.allowed_origins", sets: []string{"broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: "https://example.com"}`}},
	{key: "broker.mqtt.listen.ws.allowed_origins", sets: []string{"broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: {origin: "https://example.com"}}`}},
	{key: "broker.mqtt.listen.ws.allowed_origins[1]", sets: []string{"broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: ["https://example.com", {host: example.com}]}`}},

	// A key that is empty, null or not a scalar (the manager's decision,
	// audit 261015-006a) is refused naming the mapping it stands in, here
	// broker.limits: empty, null, an alias to an empty string, a list. A
	// non-empty scalar key is named by its own text, whatever its tag.
	{key: "broker.limits", sets: []string{"broker.limits", `{"": 5}`}},
	{key: "broker.limits", sets: []string{"broker.limits", "{~: 5}"}},
	{key: "broker.limits", sets: []string{"broker.limits", `{max_inflight: &e "", *e : 5}`}},
	{key: "broker.limits", sets: []string{"broker.limits", "{[max_inflight]: 5}"}},
	{key: "broker.limits.3", sets: []string{"broker.limits", "{!!int 3: 5}"}},

	// channels: a key no channel takes. bridges: a key no bridge takes;
	// the bridges' cases have the rest.
	{key: "channels.events.colour", sets: []string{"channels.events", "{type: append, colour: blue}"}},
	{key: "bridges.upstream.colour", sets: []string{"bridges.upstream",
		`{peer: "tcp://peer:1883", client_id: edge-1, topics: [{filter: "a/#", topic: "x/$#", direction: in}], colour: blue}`}},
}

// named gives each case without a name one: its key/value pairs.
func named(cases []refusal) []refusal {
	for i := range cases {
		if cases[i].name == "" {
			cases[i].name = strings.Join(cases[i].sets, "=")
		}
	}
	return cases
}

// Dropped: config.md's table. "A file using any of these is refused at
// startup, naming the replacement." The replacements shared by several
// rows, and the reason of the row "none: refused, with no replacement",
// which names no replacement.
const (
	oneStore      = "broker.storage: one SQLite store, mode"
	theStore      = "the one store"
	noReplacement = "refused, with no replacement"
)

// droppedKey is a Dropped row's key written with a value, and its
// replacement.
type droppedKey struct{ key, value, instead string }

// droppedKeys: the rows for the broker's keys.
var droppedKeys = []droppedKey{
	{"broker.storage.default", "local", oneStore},
	{"broker.storage.providers", "{local: {type: sqlite}}", oneStore},
	{"broker.storage.type", "sqlite", oneStore},
	{"broker.storage.snapshot_dir", "/var/lib/saguin/snapshots", oneStore},
	{"broker.retained.storage", "local", theStore},
	{"broker.session.storage", "local", theStore},
	{"broker.share.storage", "local", theStore},
	{"broker.qos2.storage", "local", theStore},
	// config.md, Dropped, flush_interval: "Syncing follows the storage mode
	// (Store)."
	{"broker.storage.flush_interval", "150ms", "Syncing follows the storage mode (Store)."},
	{"broker.storage.max_bytes", "1GiB", "none: each channel's own limits, and the disk (Store)"},
	{"broker.storage.max_bytes", "none", "none: each channel's own limits, and the disk (Store)"},
	{"broker.limits.max_message_size", "1MiB", "limits.max_packet_size"},
	{"broker.limits.max_header_bytes", "8KiB", "none: limits.max_packet_size bounds a publish's User Properties"},
	{"broker.limits.session_queue_bytes", "1MiB", "broker.broadcast retention"},
	{"broker.limits.session_queue_full", "drop", "broker.broadcast retention"},
	{"broker.qos2.expires_after", "1h", "a held QoS 2 message stays until PUBREL or its session ends"},
	{"broker.share.expires_after", "1h", "the channel's retention"},
	{"broker.session.ack_commit_interval", "200ms", "the next commit (Delivery)"},
}

// dropped makes a case of each row: refused, naming the replacement.
func dropped(rows []droppedKey) []refusal {
	cases := make([]refusal, 0, len(rows))
	for _, r := range rows {
		cases = append(cases, refusal{
			name: r.key + "=" + r.value,
			sets: []string{r.key, r.value},
			key:  r.key,
			says: []string{r.instead},
		})
	}
	return cases
}

// refusedWithNoReplacement is a Dropped row "none: refused, with no
// replacement": the refusal names its key and that reason, and offers
// nothing instead.
func refusedWithNoReplacement(name, key string, sets ...string) refusal {
	return refusal{name: name, sets: sets, key: key, says: []string{noReplacement}, never: []string{"instead"}}
}

// nestedLists writes levels+1 anchored lists in flow YAML, the first of ten
// plain values and each later one of ten aliases of the list before it, or
// of ten plain values where aliased is false.
func nestedLists(levels int, aliased bool) string {
	lists := make([]string, 0, levels+1)
	for level := range levels + 1 {
		lists = append(lists, fmt.Sprintf("&a%d [%s]", level, strings.Join(tenOf(level, aliased, ""), ", ")))
	}
	return "[" + strings.Join(lists, ", ") + "]"
}

// nestedMappings is nestedLists with mappings: each level's ten values under
// the keys k0 to k9, each level under the key l0, l1 and so on.
func nestedMappings(levels int, aliased bool) string {
	mappings := make([]string, 0, levels+1)
	for level := range levels + 1 {
		mappings = append(mappings, fmt.Sprintf("l%d: &a%d {%s}", level, level, strings.Join(tenOf(level, aliased, "k"), ", ")))
	}
	return "{" + strings.Join(mappings, ", ") + "}"
}

// tenOf is one level's ten values: aliases of the level before where aliased
// and the level is not the first, else plain values; with a key prefix,
// each value under its own key.
func tenOf(level int, aliased bool, key string) []string {
	v := "x"
	if aliased && level > 0 {
		v = fmt.Sprintf("*a%d", level-1)
	}
	values := make([]string, 10)
	for i := range values {
		values[i] = v
		if key != "" {
			values[i] = fmt.Sprintf("%s%d: %s", key, i, v)
		}
	}
	return values
}

// pathRule is a file whose paths break config.md's Values: "A path Sagüin
// writes must differ from every other path in the configuration. These paths
// are compared as files at start. They include pid_file, file_path with its
// -wal, -shm and -journal files and its lock file, memory_dir, memory mode's
// file_path.new snapshot file, and a Unix socket and its lock file", those it
// reads included. The later key in
// config.md's order is refused (broker, broker.mqtt and its listeners,
// broker.operations, broker.storage), naming the earlier.
func pathRule(name, key, earlier string, sets ...string) refusal {
	return refusal{name: name, sets: sets, key: key, says: []string{earlier}}
}

// The paths the path rules name: file_path as required writes it, its
// -wal, -shm and -journal files, and its lock file (design.md, Store:
// "<file_path>.lock beside the database").
const (
	db        = "/var/lib/saguin/saguin.db"
	dbWAL     = db + "-wal"
	dbSHM     = db + "-shm"
	dbJournal = db + "-journal"
	dbLock    = db + ".lock"
	dbOther   = "/var/lib//saguin/../saguin/./saguin.db" // file_path spelled otherwise
)

var brokerRefused = slices.Concat(
	valuesRefused(refusedValues),
	named(unknownKeys),
	dropped(droppedKeys),
	[]refusal{
		// The keys whose Default is "required" (broker.id, file_path,
		// default_retention_period and default_retention_bytes) are refused
		// absent; with nothing at all, broker.id and the storage keys are
		// missing.
		{name: "broker.id absent", sets: []string{"broker.id", absent}, key: "broker.id"},
		{name: "file_path absent", sets: []string{"broker.storage.file_path", absent}, key: "broker.storage.file_path"},
		{
			name: "default_retention_period absent",
			sets: []string{"broker.storage.default_retention_period", absent},
			key:  "broker.storage.default_retention_period",
		},
		{
			name: "default_retention_bytes absent",
			sets: []string{"broker.storage.default_retention_bytes", absent},
			key:  "broker.storage.default_retention_bytes",
		},
		{name: "an empty file", raw: emptyFile},

		// A file that cannot be read: refused, saying why, naming the file.
		{
			name: "a missing file",
			args: []string{"--check-config", dirToken + "/missing.yaml"},
			says: []string{dirToken + "/missing.yaml", "no such file or directory"},
		},

		// The file TestCheckConfig tried, each change refused naming its key
		// (config.md's Dropped rows also name the replacement); not YAML,
		// refused as a whole file, "configuration"; and with --output, which
		// prints nothing for a refused file.
		{name: "the checked file, an id with a space", sets: slices.Concat(checked, []string{"broker.id", "edge 1"}), key: "broker.id"},
		{
			name: "the checked file, a value out of range",
			sets: slices.Concat(checked, []string{"broker.limits.max_inflight", "0"}),
			key:  "broker.limits.max_inflight",
		},
		{
			// config.md, Dropped, flush_interval: "Syncing follows the storage mode
			// (Store)."
			name: "the checked file, a dropped key",
			sets: slices.Concat(checked, []string{"broker.storage.flush_interval", "150ms"}),
			key:  "broker.storage.flush_interval",
			says: []string{"Syncing follows the storage mode (Store)."},
		},
		{
			name: "the checked file, an unknown key",
			sets: slices.Concat(checked, []string{"channels.events", "{type: append, colour: blue}"}),
			key:  "channels.events.colour",
		},
		{name: "not YAML", raw: "broker: [\n", key: "configuration"},
		{
			name: "the checked file, an id with a space, with --output",
			sets: slices.Concat(checked, []string{"broker.id", "edge 1"}),
			args: []string{"--check-config", fileToken, "--output"},
			key:  "broker.id",
		},

		// A command line saguin does not take exits 2 and says how it is
		// used (design.md, Operations).
		{name: "no arguments", args: []string{}, code: 2},
		{name: "--check-config alone", args: []string{"--check-config"}, code: 2},
		{name: "--output alone", args: []string{"--output"}, code: 2},
		{name: "an extra argument", args: []string{"--check-config", fileToken, "extra"}, code: 2},
		{name: "an unknown flag", args: []string{"--check-config", fileToken, "--bogus"}, code: 2},

		// A key written twice in one mapping is refused, not read as either
		// value (read strictly; a YAML mapping's keys are unique), named by
		// its full path: in a block mapping, in a list's listener, in the
		// tls of an operations list's listener.
		{name: "log_level twice", raw: brokerFirst("  log_level: info\n  log_level: warn\n"), key: "broker.log_level"},
		{
			name: "a list entry's address twice",
			sets: []string{
				"broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: b, address: ":1884", address: ":1885"}]`,
			},
			key: "broker.mqtt.listen.tcp[1].address",
		},
		{
			name: "an operations list entry's cert_file twice",
			sets: []string{
				"broker.operations.listen.tcp", `[{name: a, address: "127.0.0.1:9090"}, ` +
					`{name: b, address: "127.0.0.1:9091", tls: {` + pairCA + `, cert_file: <dir>/other.crt}}]`,
			},
			key: "broker.operations.listen.tcp[1].tls.cert_file",
		},

		// "One YAML file holding one document": a second document is
		// refused, whatever it holds.
		{name: "an empty second document", raw: yamlOf(nil) + "---\n"},
		{name: "a second document", raw: yamlOf(nil) + "---\nbroker:\n  log_level: warn\n"},

		// "Every key is a plain string. The loader refuses YAML merge keys
		// (<<). Anchors and aliases are allowed, but the loader refuses a
		// circular one." A refused key is named by its full path.
		{name: "a merge key", raw: brokerFirst("  <<: {log_level: warn}\n"), key: "broker.<<"},
		{
			name: "a merge key of an anchored mapping",
			sets: []string{
				"broker.mqtt.listen.tcp", `[{name: a, address: ":1883", tls: &t {` + pair + `}}, {name: b, address: ":8883", tls: {<<: *t}}]`,
			},
			key: "broker.mqtt.listen.tcp[1].tls.<<",
		},
		{name: "an integer key", raw: brokerFirst("  1: x\n"), key: "broker.1"},
		{name: "a boolean key", raw: brokerFirst("  true: x\n"), key: "broker.true"},
		// Files whose root mapping holds a key that is not a plain string
		// (the manager's decision, audit 261015-006a): a key that is empty,
		// null or not a scalar is refused naming the mapping it stands in,
		// "configuration" at the file's root, as a whole-file error is
		// named; a non-empty scalar key is named by its own text, whatever
		// its tag. The first three are the inputs FuzzParse found.
		{name: "an anchored null key at the root", raw: "&0:\n", key: "configuration"},
		{name: "an empty folded key at the root", raw: "? >\n", key: "configuration"},
		{name: "a mapping as a key at the root", raw: "{{0}}\n", key: "configuration"},
		{name: "an empty key at the root", raw: "\"\": 1\n", key: "configuration"},
		{name: "a null key at the root", raw: "~: 1\n", key: "configuration"},
		{name: "an !!int key at the root", raw: "!!int 3: 1\n", key: "3"},
		// A list and a mapping each holding itself through an alias.
		{name: "a list holding itself", sets: []string{"broker.mqtt.listen.tcp", `&l [{name: a, address: ":1883"}, *l]`}},
		{name: "a mapping holding itself", sets: []string{"broker.mqtt.listen", `&m {tcp: {address: ":1883"}, ws: *m}`}},

		// "one YAML file holding one document. The loader reads it strictly.
		// An unknown [...] key stops startup", however its aliases nest;
		// promptly (refusedPromptly).
		{name: "lists under an unknown key", nest: nestedLists, nestAt: "colour", key: "colour"},
		{name: "mappings under an unknown nested key", nest: nestedMappings, nestAt: "broker.colour", key: "broker.colour"},
		{name: "lists where listeners go", nest: nestedLists, nestAt: "broker.mqtt.listen.tcp", key: "broker.mqtt.listen.tcp[0]"},
		// channels: a list of groups, and a queue's retry, under which a
		// Dropped key (retry.backoff) can stand.
		{name: "lists where channel groups go", nest: nestedLists, nestAt: "channels", key: "channels[0]"},
		{
			name: "mappings in a queue's retry",
			sets: []string{"channels.q.type", "queue"},
			nest: nestedMappings, nestAt: "channels.q.retry", key: "channels.q.retry.l0",
		},

		// The Dropped row "none: refused, with no replacement", for the broker's keys:
		// "an operations listener's allow_anonymous", true or false, on
		// either kind, as one listener or an entry of a list
		// (broker.operations: "never allow_anonymous").
		refusedWithNoReplacement("commit_interval", "broker.storage.commit_interval", "broker.storage.commit_interval", "5ms"),
		refusedWithNoReplacement("commit_max_records", "broker.storage.commit_max_records", "broker.storage.commit_max_records", "100"),
		refusedWithNoReplacement("retained.retention_bytes", "broker.retained.retention_bytes", "broker.retained.retention_bytes", "1MiB"),
		refusedWithNoReplacement("session.max_bytes", "broker.session.max_bytes", "broker.session.max_bytes", "1MiB"),
		refusedWithNoReplacement("qos2.max_bytes", "broker.qos2.max_bytes", "broker.qos2.max_bytes", "1MiB"),
		refusedWithNoReplacement("an operations tcp listener's allow_anonymous", "broker.operations.listen.tcp.allow_anonymous",
			"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", allow_anonymous: true}`),
		refusedWithNoReplacement("an operations unix listener's allow_anonymous", "broker.operations.listen.unix.allow_anonymous",
			"broker.operations.listen.unix", `{path: /run/saguin/ops.sock, allow_anonymous: false}`),
		refusedWithNoReplacement("an operations list entry's allow_anonymous", "broker.operations.listen.tcp[1].allow_anonymous",
			"broker.operations.listen.tcp", `[{name: a, address: "127.0.0.1:9090"}, {name: b, address: "127.0.0.1:9091", allow_anonymous: true}]`),

		// read_connections, in memory mode: "2 to 8".
		{name: "memory mode, 1 read connection", sets: []string{"broker.storage.mode", "memory", "broker.storage.read_connections", "1"},
			key: "broker.storage.read_connections"},
		{name: "memory mode, no read connection", sets: []string{"broker.storage.mode", "memory", "broker.storage.read_connections", "0"},
			key: "broker.storage.read_connections"},
		{name: "memory mode, 9 read connections", sets: []string{"broker.storage.mode", "memory", "broker.storage.read_connections", "9"},
			key: "broker.storage.read_connections"},

		// publish_commit_interval: "at most 1s"; publish_commit_max_records:
		// "1 to limits.max_connections" (config.md, broker.storage).
		{name: "an interval past 1s", sets: []string{"broker.storage.publish_commit_interval", "1001ms"},
			key: "broker.storage.publish_commit_interval"},
		{name: "an interval of 2s", sets: []string{"broker.storage.publish_commit_interval", "2s"},
			key: "broker.storage.publish_commit_interval"},
		{name: "no records", sets: []string{"broker.storage.publish_commit_max_records", "0"},
			key: "broker.storage.publish_commit_max_records"},
		{name: "records past max_connections", sets: []string{"broker.storage.publish_commit_max_records", "10001"},
			key: "broker.storage.publish_commit_max_records"},
		{name: "records past a smaller max_connections", sets: []string{
			"broker.limits.max_connections", "50", "broker.storage.publish_commit_max_records", "51",
		}, key: "broker.storage.publish_commit_max_records"},

		// max_connect_size: "at most max_packet_size".
		{name: "max_connect_size written above max_packet_size", sets: []string{
			"broker.limits.max_packet_size", "64KiB", "broker.limits.max_connect_size", "65537",
		}, key: "broker.limits.max_connect_size", says: []string{"broker.limits.max_packet_size"}},

		// config.md, broker.limits, max_incoming_memory: "the incoming
		// budget. It must hold the largest body one reader can ask for:
		// max_packet_size, plus the largest S2 copy of it where
		// max_packet_size is over 4 KiB, plus max_topic_length where a
		// bridge has an in rule. A smaller value is refused, and the refusal
		// names the value to set"; design.md, Restart, Start: "startup checks
		// the configuration, including [...] size limits"; Message memory,
		// Asking: "A body over 4 KiB also asks for room for the largest copy
		// S2 can make of it". The value to set is taken as the least the key
		// allows, in bytes.
		{name: "max_incoming_memory written below max_packet_size", sets: []string{
			"broker.limits.max_packet_size", "64KiB", "broker.limits.max_incoming_memory", "65535",
		}, key: "broker.limits.max_incoming_memory", says: []string{strconv.Itoa(leastIncoming(64 << 10))}},
		{name: "max_incoming_memory a byte below max_packet_size plus its largest S2 copy", sets: []string{
			"broker.limits.max_packet_size", "64KiB", "broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(64<<10) - 1),
		}, key: "broker.limits.max_incoming_memory", says: []string{strconv.Itoa(leastIncoming(64 << 10))}},
		{name: "max_incoming_memory a byte below a max_packet_size of 4KiB", sets: []string{
			"broker.limits.max_packet_size", "4KiB", "broker.limits.max_incoming_memory", "4095",
		}, key: "broker.limits.max_incoming_memory", says: []string{"4096"}},
		{name: "max_packet_size above max_incoming_memory's default", sets: []string{"broker.limits.max_packet_size", "268435460"},
			key: "broker.limits.max_incoming_memory", or: "broker.limits.max_packet_size",
			says: []string{"broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(268435460))}},
		{name: "max_packet_size a byte past what max_incoming_memory's default allows",
			sets: []string{"broker.limits.max_packet_size", strconv.Itoa(largestUnderDefault + 1)},
			key:  "broker.limits.max_incoming_memory", or: "broker.limits.max_packet_size",
			says: []string{"broker.limits.max_incoming_memory", strconv.Itoa(leastIncoming(largestUnderDefault + 1))}},

		// The paths Sagüin writes, against every other path.
		pathRule("pid_file is file_path", "broker.storage.file_path", "broker.pid_file", "broker.pid_file", db),
		pathRule("pid_file is file_path, spelled otherwise", "broker.storage.file_path", "broker.pid_file",
			"broker.pid_file", db, "broker.storage.file_path", dbOther),
		pathRule("pid_file is the broker's password_file", "broker.mqtt.password_file", "broker.pid_file",
			"broker.pid_file", dirToken+"/passwd", "broker.mqtt.password_file", dirToken+"/passwd"),
		pathRule("pid_file is the acl_file", "broker.mqtt.acl_file", "broker.pid_file",
			"broker.pid_file", dirToken+"/acl.yaml", "broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", dirToken+"/acl.yaml"),
		pathRule("pid_file is a listener's tls key_file", "broker.mqtt.listen.tcp.tls.key_file", "broker.pid_file",
			"broker.pid_file", serverKey, "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`}}`),
		pathRule("pid_file is a unix socket", "broker.mqtt.listen.unix.path", "broker.pid_file",
			"broker.pid_file", "/run/saguin/mqtt.sock", "broker.mqtt.listen.unix.path", "/run/saguin/mqtt.sock"),
		pathRule("pid_file is a unix socket's lock file", "broker.mqtt.listen.unix.path", "broker.pid_file",
			"broker.pid_file", "/run/saguin/mqtt.sock.lock", "broker.mqtt.listen.unix.path", "/run/saguin/mqtt.sock"),
		pathRule("an operations listener's password_file is pid_file", "broker.operations.listen.tcp.password_file", "broker.pid_file",
			"broker.pid_file", "/run/saguin.pid", "broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: /run/saguin.pid}`),
		pathRule("a unix socket is a tcp listener's tls cert_file", "broker.mqtt.listen.unix.path", "broker.mqtt.listen.tcp.tls.cert_file",
			"broker.mqtt.listen.tcp", `{address: ":8883", tls: {cert_file: /run/saguin/mqtt.sock, key_file: `+serverKey+`}}`,
			"broker.mqtt.listen.unix.path", "/run/saguin/mqtt.sock"),
		// A socket path is at most 107 bytes, so this one is the test's
		// own, short wherever the checkout lies, not the case's folder.
		pathRule("an operations unix socket is the broker's password_file", "broker.operations.listen.unix.path", "broker.mqtt.password_file",
			"broker.mqtt.password_file", "/run/saguin/passwd", "broker.operations.listen.unix.path", "/run/saguin/passwd"),
		pathRule("file_path is the acl_file", "broker.storage.file_path", "broker.mqtt.acl_file",
			"broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", dirToken+"/acl.yaml", "broker.storage.file_path", dirToken+"/acl.yaml"),
		pathRule("file_path is a listener's password_file", "broker.storage.file_path", "broker.mqtt.listen.tcp.password_file",
			"broker.mqtt.listen.tcp", `{address: ":1883", password_file: `+db+`}`),
		pathRule("file_path is a listener's tls client_ca_file", "broker.storage.file_path", "broker.mqtt.listen.tcp.tls.client_ca_file",
			"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, client_ca_file: `+db+`}}`),
		pathRule("file_path is a unix socket's lock file", "broker.storage.file_path", "broker.mqtt.listen.unix.path",
			"broker.mqtt.listen.unix.path", "/run/saguin/mqtt.sock", "broker.storage.file_path", "/run/saguin/mqtt.sock.lock"),
		pathRule("memory_dir is file_path", "broker.storage.memory_dir", "broker.storage.file_path",
			"broker.storage.mode", "memory", "broker.storage.memory_dir", db),
		pathRule("memory_dir is the operators' password_file", "broker.storage.memory_dir", "broker.operations.password_file",
			"broker.operations.password_file", dirToken+"/operators.passwd", "broker.operations.listen.unix.path", "/run/saguin/ops.sock",
			"broker.storage.mode", "memory", "broker.storage.memory_dir", dirToken+"/operators.passwd"),
	},
	// file_path's -wal, -shm and -journal files and its lock file, written
	// beside it, are paths Sagüin writes too: file_path, later in config.md's
	// order than every key but memory_dir, is refused naming the other key.
	sidecars(dbWAL, "pid_file", "pid_file, file_path spelled otherwise", "password_file", "acl_file", "cert_file", "client_ca_file",
		"unix socket", "operators' password_file", "memory_dir"),
	sidecars(dbSHM, "pid_file", "password_file", "acl_file", "listener password_file", "key_file", "unix socket",
		"operations listener password_file", "operations unix socket", "memory_dir"),
	sidecars(dbJournal, "pid_file", "pid_file, file_path spelled otherwise", "password_file", "acl_file", "listener password_file",
		"cert_file", "key_file", "client_ca_file", "unix socket", "operators' password_file", "operations listener password_file",
		"operations unix socket", "memory_dir"),
	sidecars(dbLock, "pid_file"),
)

// sidecar names each path rule a sidecar file of file_path breaks: the
// other path that is that file.
var sidecar = map[string]func(f string) refusal{
	"pid_file": func(f string) refusal {
		return pathRule("pid_file is file_path's "+suffix(f)+" file", "broker.storage.file_path", "broker.pid_file", "broker.pid_file", f)
	},
	"pid_file, file_path spelled otherwise": func(f string) refusal {
		return pathRule("pid_file is file_path's "+suffix(f)+" file, file_path spelled otherwise", "broker.storage.file_path", "broker.pid_file",
			"broker.pid_file", f, "broker.storage.file_path", dbOther)
	},
	"password_file": func(f string) refusal {
		return pathRule("the broker's password_file is file_path's "+suffix(f)+" file", "broker.storage.file_path", "broker.mqtt.password_file",
			"broker.mqtt.password_file", f)
	},
	"acl_file": func(f string) refusal {
		return pathRule("the acl_file is file_path's "+suffix(f)+" file", "broker.storage.file_path", "broker.mqtt.acl_file",
			"broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", f)
	},
	"listener password_file": func(f string) refusal {
		return pathRule("a listener's password_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.mqtt.listen.tcp.password_file", "broker.mqtt.listen.tcp", `{address: ":1883", password_file: `+f+`}`)
	},
	"cert_file": func(f string) refusal {
		return pathRule("a listener's tls cert_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.mqtt.listen.tcp.tls.cert_file", "broker.mqtt.listen.tcp", `{address: ":8883", tls: {cert_file: `+f+`, key_file: `+serverKey+`}}`)
	},
	"key_file": func(f string) refusal {
		return pathRule("a listener's tls key_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.mqtt.listen.tcp.tls.key_file", "broker.mqtt.listen.tcp", `{address: ":8883", tls: {cert_file: `+serverCert+`, key_file: `+f+`}}`)
	},
	"client_ca_file": func(f string) refusal {
		return pathRule("a listener's tls client_ca_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.mqtt.listen.tcp.tls.client_ca_file", "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, client_ca_file: `+f+`}}`)
	},
	"unix socket": func(f string) refusal {
		return pathRule("a unix socket is file_path's "+suffix(f)+" file", "broker.storage.file_path", "broker.mqtt.listen.unix.path",
			"broker.mqtt.listen.unix.path", f)
	},
	"operators' password_file": func(f string) refusal {
		return pathRule("the operators' password_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.operations.password_file", "broker.operations.password_file", f, "broker.operations.listen.unix.path", "/run/saguin/ops.sock")
	},
	"operations listener password_file": func(f string) refusal {
		return pathRule("an operations listener's password_file is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.operations.listen.tcp.password_file",
			"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: `+f+`}`)
	},
	"operations unix socket": func(f string) refusal {
		return pathRule("an operations unix socket is file_path's "+suffix(f)+" file", "broker.storage.file_path",
			"broker.operations.listen.unix.path", "broker.operations.listen.unix.path", f)
	},
	"memory_dir": func(f string) refusal {
		return pathRule("memory_dir is file_path's "+suffix(f)+" file", "broker.storage.memory_dir", "broker.storage.file_path",
			"broker.storage.mode", "memory", "broker.storage.memory_dir", f)
	},
}

// suffix is a sidecar file's suffix to file_path: -wal, -shm, -journal or
// .lock.
func suffix(f string) string { return strings.TrimPrefix(f, db) }

// sidecars is the path rules sidecar names, for the sidecar file f.
func sidecars(f string, rules ...string) []refusal {
	cases := make([]refusal, 0, len(rules))
	for _, r := range rules {
		cases = append(cases, sidecar[r](f))
	}
	return cases
}
