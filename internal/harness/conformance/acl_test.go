package conformance

import (
	"bytes"
	"cmp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the acl_file at its reading and at CONNECT (item 0018c):
// design.md, Access and reload, "The acl_file is YAML and only narrows what
// the rest of this document allows. With none, everything is allowed";
// Flows, CONNECT, its refusal table and check order; config.md,
// broker.mqtt, acl_file, "needs a listener with an identity source", so
// every broker here reads a client password file, allow_anonymous then
// false (config.md: "true, on the broker or any listener, is refused beside
// an acl_file"). The checks per message are item 0018e's; every rule a test
// writes still grants what its clients do, so they stand once those checks
// are built.

// aclPasswordSuffix makes a user's password in the files these tests write:
// the user name and the suffix.
const aclPasswordSuffix = "-pw"

// aclOptions is the configuration of a broker of the test's own reading acl
// as its acl_file, beside a client password file holding users (their
// passwords made with aclPasswordSuffix), both written in the test's
// folder; its other keys are the caller's to add.
func aclOptions(t *testing.T, acl string, users ...string) *harness.Options {
	t.Helper()
	dir := t.TempDir()
	return &harness.Options{Dir: dir, WriteTimeout: support.WriteTimeout,
		Login:   harness.Login{PasswordFile: usersFile(t, dir, "passwd", users...)},
		ACLFile: testkit.WriteFile(t, dir, "acl.yaml", acl)}
}

// usersFile writes the client password file name in dir, holding users,
// each one's password its name and aclPasswordSuffix, and returns its path:
// written again, it is the file a reload reads.
func usersFile(t *testing.T, dir, name string, users ...string) string {
	t.Helper()
	lines := make([]string, len(users))
	for i, user := range users {
		lines[i] = user + ":" + sixHash(user+aclPasswordSuffix, salt12)
	}
	return passwordFile(t, dir, name, lines...)
}

// aclLogin is an MQTT 5 CONNECT of a new session for client, carrying user
// and the password aclOptions gave it.
func aclLogin(client, user string) *harness.Connect {
	return credentials(client, user, user+aclPasswordSuffix)
}

// aclStartRefused starts the broker on o's configuration and fails the test
// unless the start is refused, exit 1, a line of what it printed naming each
// of names (design.md, Access and reload: "The broker refuses a file that
// holds any of the mistakes below. The refusal names the file, the entry and
// what is wrong. At start, a refused file stops the broker"; Restart: "The exit status is [...] 1 after [...] a refused
// start").
func aclStartRefused(t *testing.T, o *harness.Options, names ...string) {
	t.Helper()
	config, err := harness.WriteConfig(o)
	if err != nil {
		t.Fatalf("writing the configuration: %v", err)
	}
	code, stdout, stderr := execute(t, support.BrokerBinary, "--config", config)
	if code != 1 {
		t.Errorf("exit %d; want the start refused, exit 1", code)
	}
	if !namesAll(stdout+stderr, names...) {
		t.Errorf("no line of what the refused start printed names each of %q:\n%s%s", names, stdout, stderr)
	}
}

// namesAll reports whether one line of printed holds each of names.
func namesAll(printed string, names ...string) bool {
	for line := range strings.Lines(printed) {
		if !slices.ContainsFunc(names, func(name string) bool { return !strings.Contains(line, name) }) {
			return true
		}
	}
	return false
}

// aclChannels are the channels of the brokers whose acl_file names one:
// one of each type (design.md, Channels).
var aclChannels = []harness.Channel{
	{Name: "events", Type: "append", Filter: "events/#"},
	{Name: "kv", Type: "latest", Filter: "kv/#"},
	{Name: "jobs", Type: "queue", Filter: "jobs/#"},
}

// faultyACL is an acl_file of one good role and user beside the role
// faulty, whose rules are role where it is not "", and the user entry
// faulty-user, written as entry, or naming faulty where entry is "". The
// good role names the queue jobs' dead-letter channel as design.md,
// Channels, has a rule name it: "An acl_file rule names it <channel>__dlq,
// as an append channel allowing read and seek, never write".
func faultyACL(role, entry string) string {
	acl := "roles:\n  base:\n    - topic: alerts/#\n      allow: [read]\n    - channel: jobs__dlq\n      allow: [read, seek]\n"
	if role != "" {
		acl += "  faulty: " + role + "\n"
		entry = cmp.Or(entry, "[faulty]")
	}
	acl += "users:\n  ops: [base]\n"
	if entry != "" {
		acl += "  faulty-user: " + entry + "\n"
	}
	return acl
}

// design.md, Access and reload: "The broker refuses a file that holds any of
// the mistakes below. The refusal names the file, the entry and what is
// wrong. At start, a refused file stops the broker", then the list: an
// unknown key; a rule that names none of channel:, topic: and broker:, or
// more than one; %u or %c in a channel name, or a channel that is not
// configured; a filter: on a topic: or broker: rule, or a filter: that
// matches none of its channel's topics; a topic: rule that lies inside a
// configured channel's filter; a verb its subject does not take, or a
// feature that does not exist; allow: on a features rule, or deny: on any
// other rule; a rule that allows or denies nothing, or names a verb or
// denial twice; a role with no rules; a user entry with no roles, or one
// naming a role that is not defined; %c in client_ids, or an empty
// client_ids or an empty entry in it; a publish_rate that is not a whole
// number of at least 1, or a publish_bytes that is not a size; none is
// refused for either. Restart: exit status 1 "after [...] a refused start".
//
// Channels: a dead-letter channel's rule is "an append channel allowing read
// and seek, never write".
//
// The broker has an append, a latest and a queue channel (aclChannels).
// The file without a mistake, naming the queue's dead-letter channel,
// starts. Each case writes it with one mistake,
// in the role faulty or the user entry faulty-user, and starts the broker:
// exit 1, and a line of what it printed names the file, the entry and what
// is wrong (the key, the name, the verb, the feature or the value at
// fault).
func TestACLFileMistakesRefuseTheStart(t *testing.T) {
	t.Run("the file without a mistake starts", func(t *testing.T) {
		o := aclOptions(t, faultyACL("", ""), "ops")
		o.Channels = aclChannels
		support.Launch(t, o)
	})
	for _, c := range []struct {
		name, role, entry string
		names             []string // beside the file: the entry and what is wrong
	}{
		{"an unknown key", "[{channel: events, allow: [read], colour: blue}]", "", []string{"faulty", "colour"}},
		{"a rule naming none of channel, topic and broker", "[{allow: [read]}]", "", []string{"faulty"}},
		{"a rule naming more than one of channel, topic and broker", "[{channel: events, topic: alerts/x, allow: [read]}]", "", []string{"faulty"}},
		{"%u in a channel name", `[{channel: "events-%u", allow: [read]}]`, "", []string{"faulty", "%u"}},
		{"%c in a channel name", `[{channel: "events-%c", allow: [read]}]`, "", []string{"faulty", "%c"}},
		{"a channel not configured", "[{channel: nowhere, allow: [read]}]", "", []string{"faulty", "nowhere"}},
		{"a filter on a topic rule", "[{topic: alerts/#, filter: alerts/x, allow: [read]}]", "", []string{"faulty", "filter"}},
		{"a filter on a broker rule", "[{broker: sessions, filter: x, allow: [disconnect]}]", "", []string{"faulty", "filter"}},
		{"a filter matching none of its channel's topics", `[{channel: events, filter: "other/#", allow: [read]}]`, "", []string{"faulty", "other/#"}},
		{"a topic rule inside a configured channel's filter", `[{topic: "events/a/#", allow: [read]}]`, "", []string{"faulty", "events/a/#"}},
		{"a verb the subject does not take", "[{channel: events, allow: [consume]}]", "", []string{"faulty", "consume"}},
		{"write on a dead-letter channel", "[{channel: jobs__dlq, allow: [write]}]", "", []string{"faulty", "write"}},
		{"an unknown feature", "[{broker: features, deny: [teleport]}]", "", []string{"faulty", "teleport"}},
		{"allow on a features rule", "[{broker: features, allow: [read]}]", "", []string{"faulty", "allow"}},
		{"deny on a rule other than features", "[{channel: events, deny: [read]}]", "", []string{"faulty", "deny"}},
		{"a rule allowing nothing", "[{channel: events, allow: []}]", "", []string{"faulty"}},
		{"a rule denying nothing", "[{broker: features, deny: []}]", "", []string{"faulty"}},
		{"a verb written twice", "[{channel: kv, allow: [read, read]}]", "", []string{"faulty", "read"}},
		{"a denial written twice", "[{broker: features, deny: [will, will]}]", "", []string{"faulty", "will"}},
		{"a role with no rules", "[]", "", []string{"faulty"}},
		{"a user entry with no roles", "", "[]", []string{"faulty-user"}},
		{"a user entry naming a role not defined", "", "[ghost]", []string{"faulty-user", "ghost"}},
		{"%c in client_ids", "", `{roles: [base], client_ids: "%c"}`, []string{"faulty-user", "%c"}},
		{"an empty client_ids", "", "{roles: [base], client_ids: []}", []string{"faulty-user", "client_ids"}},
		{"an empty entry in client_ids", "", `{roles: [base], client_ids: ["%u", ""]}`, []string{"faulty-user", "client_ids"}},
		{"a publish_rate of zero", "", "{roles: [base], limits: {publish_rate: 0}}", []string{"faulty-user", "publish_rate"}},
		{"a publish_rate below zero", "", "{roles: [base], limits: {publish_rate: -5}}", []string{"faulty-user", "publish_rate"}},
		{"a publish_bytes that is not a size", "", "{roles: [base], limits: {publish_bytes: lots}}", []string{"faulty-user", "publish_bytes"}},
		{"a publish_rate of none", "", "{roles: [base], limits: {publish_rate: none}}", []string{"faulty-user", "publish_rate"}},
		{"a publish_bytes of none", "", "{roles: [base], limits: {publish_bytes: none}}", []string{"faulty-user", "publish_bytes"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			o := aclOptions(t, faultyACL(c.role, c.entry), "ops", "faulty-user")
			o.Channels = aclChannels
			aclStartRefused(t, o, append([]string{o.ACLFile}, c.names...)...)
		})
	}
}

// design.md, Access and reload: "Two patterns with the same count that can
// both match one name are refused at load, naming both. Characters are
// Unicode code points, not bytes, so é counts as one. The broker matches a
// name against a pattern with Go's regular expressions, escaping every part
// of the pattern except *"; "At start, a refused file stops the broker".
//
// Each case is an acl_file whose users are two patterns of one role. Two
// patterns of equal count that some name matches both of refuse the start,
// exit 1, a line naming the file and both: a* and *b (ab), and a* and *é,
// equal in code points though not in bytes (aé). Two of equal count that no
// name matches both of start: a*b and b*a, x* and y*, and a.* and ab*, the
// dot a character like any other.
func TestACLEqualPatternsMatchingOneNameRefuseTheStart(t *testing.T) {
	acl := func(first, second string) string {
		return "roles:\n  base:\n    - topic: alerts/#\n      allow: [read]\n" +
			"users:\n  " + strconv.Quote(first) + ": [base]\n  " + strconv.Quote(second) + ": [base]\n"
	}
	for _, c := range []struct{ name, first, second string }{
		{"a* and *b, both matching ab", "a*", "*b"},
		{"a* and *é, one character each, both matching aé", "a*", "*é"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			o := aclOptions(t, acl(c.first, c.second), "ab")
			aclStartRefused(t, o, o.ACLFile, c.first, c.second)
		})
	}
	for _, c := range []struct{ name, first, second string }{
		{"a*b and b*a", "a*b", "b*a"},
		{"x* and y*", "x*", "y*"},
		{"a.* and ab*, the dot no wildcard", "a.*", "ab*"},
	} {
		t.Run(c.name+" start", func(t *testing.T) {
			support.Launch(t, aclOptions(t, acl(c.first, c.second), "ab"))
		})
	}
}

// connackSays returns the value of the CONNACK property id and whether the
// CONNACK carries it.
func connackSays(ack *harness.Connack, id byte) (uint32, bool) {
	p, ok := ack.Props.Get(id)
	return p.Int, ok
}

// withholds reports whether ack says the feature its property id stands
// for is withheld: Retain Available or Shared Subscription Available 0
// (MQTT 5 3.2.2.3.5, 3.2.2.3.13: "If not present, then [it is]
// supported"), or a Maximum QoS (3.2.2.3.4: "If the Maximum QoS is absent,
// the Client uses a Maximum QoS of 2").
func withholds(ack *harness.Connack, id byte) bool {
	v, ok := connackSays(ack, id)
	if id == harness.MaximumQoS {
		return ok && v < 2
	}
	return ok && v == 0
}

// aclFree is a role granting every verb on aclChannels' events and every
// $broadcast topic: what the clients of these tests do.
const aclFree = `  free:
    - topic: "#"
      allow: [write, read]
    - channel: events
      allow: [write, read, seek]
`

// design.md, Access and reload: "A name written without * beats every
// pattern. Among patterns matching a user name, the one with the most
// characters other than * wins. It supplies the whole entry, including
// roles, limits and client-id restrictions. The other entries do nothing";
// "Characters are Unicode code points, not bytes, so é counts as one";
// "Example: for device-7, device-7 beats device-*, and device-* beats
// *-7"; "A feature is denied if any of a user's roles denies it"; Flows,
// CONNECT, 8: the CONNACK carries "the Maximum QoS, Retain Available and
// Shared Subscription Available the acl_file allows"; 0x86 for "a client id
// the acl_file refuses".
//
// Each entry's roles deny a different feature, which the MQTT 5 CONNACK
// shows: device-7 denies none, device-* retained (Retain Available 0) and
// admits the client ids only-*, *-7 share (Shared Subscription Available
// 0), ééé* retained and *abcd share.
//   - device-7: device-7 decides: accepted, neither feature withheld;
//   - device-x-7, client id only-1: device-* (7 characters) beats *-7 (2):
//     Retain Available 0, share not withheld;
//   - device-x-7, client id any-id: device-*'s client ids refuse it, 0x86,
//     though *-7 restricts none;
//   - ab-7: *-7 alone matches: Shared Subscription Available 0;
//   - éééabcd: *abcd (4 characters) beats ééé* (3 characters, 6 bytes):
//     Shared Subscription Available 0, retained not withheld.
func TestACLTheMostExactEntryDecides(t *testing.T) {
	acl := "roles:\n" + aclFree + `  noretain:
    - broker: features
      deny: [retained]
  noshare:
    - broker: features
      deny: [share]
users:
  device-7: [free]
  "device-*":
    roles: [free, noretain]
    client_ids: "only-*"
  "*-7": [free, noshare]
  "ééé*": [free, noretain]
  "*abcd": [free, noshare]
`
	o := aclOptions(t, acl, "device-7", "device-x-7", "ab-7", "éééabcd")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	for _, c := range []struct {
		name, user, client string
		retain, share      bool // withheld
	}{
		{"a name without * beats every pattern", "device-7", "", false, false},
		{"the pattern with more characters wins", "device-x-7", "only-1", true, false},
		{"one pattern alone", "ab-7", "", false, true},
		{"characters are code points", "éééabcd", "", false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, ack := harness.Connected(t, b.Addr, harness.V5, aclLogin(cmp.Or(c.client, clientID(t, "")), c.user))
			if got := withholds(ack, harness.RetainAvailable); got != c.retain {
				t.Errorf("%s: CONNACK withholds retained: %v; want %v (its properties: %+v)", c.user, got, c.retain, ack.Props)
			}
			if got := withholds(ack, harness.SharedSubscriptionAvailable); got != c.share {
				t.Errorf("%s: CONNACK withholds shared subscriptions: %v; want %v (its properties: %+v)", c.user, got, c.share, ack.Props)
			}
		})
	}
	t.Run("the winning entry's client ids", func(t *testing.T) {
		connackRefuses(t, b.Addr, harness.V5, aclLogin("any-id", "device-x-7"), 0x86)
	})
}

// design.md, Access and reload: "The client ids are one pattern or a list,
// with %u substituted. When the user name holds a *, a pattern with %u
// matches no client id. Any other client id is refused at CONNECT with 0x86"; "In the
// pattern, * matches any run of characters"; Flows, CONNECT, the refusal
// table: "0x86 | a wrong or missing credential, or a client id the acl_file
// refuses | 4".
//
// sensor-* admits client id %u; cam-* the list %u-* and spare; st* the list
// %u, %u-* and plain. sensor-1 is accepted as sensor-1 and refused as
// sensor-2, 0x86 (MQTT 3.1.1: 4). cam-1 is accepted as cam-1-front and
// spare, refused as cam-2-front. st*r, holding *, is refused as st*r,
// st*r-x and stXr, which %u's patterns would match were the * a wildcard,
// and accepted as plain.
func TestACLClientIDs(t *testing.T) {
	acl := "roles:\n" + aclFree + `users:
  "sensor-*": {roles: [free], client_ids: "%u"}
  "cam-*": {roles: [free], client_ids: ["%u-*", spare]}
  "st*": {roles: [free], client_ids: ["%u", "%u-*", plain]}
`
	o := aclOptions(t, acl, "sensor-1", "cam-1", "st*r")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	for _, c := range []struct {
		user, client string
		version      byte
		refused      byte // 0 for accepted
	}{
		{"sensor-1", "sensor-1", harness.V5, 0},
		{"sensor-1", "sensor-2", harness.V5, 0x86},
		{"sensor-1", "sensor-2", harness.V311, 4},
		{"cam-1", "cam-1-front", harness.V5, 0},
		{"cam-1", "spare", harness.V5, 0},
		{"cam-1", "cam-2-front", harness.V5, 0x86},
		{"st*r", "st*r", harness.V5, 0x86},
		{"st*r", "st*r-x", harness.V5, 0x86},
		{"st*r", "stXr", harness.V5, 0x86},
		{"st*r", "plain", harness.V5, 0},
	} {
		t.Run(c.user+" as "+c.client+", "+harness.VersionName(c.version), func(t *testing.T) {
			if c.refused == 0 {
				harness.Connected(t, b.Addr, c.version, aclLogin(c.client, c.user))
				return
			}
			connackRefuses(t, b.Addr, c.version, aclLogin(c.client, c.user), c.refused)
		})
	}
}

// design.md, Flows, CONNECT: "A user matched by no users: pattern is
// accepted with no rights". (Its publishes and subscriptions refused with
// 0x87 are item 0018e's.)
//
// With an acl_file whose one entry is sensor-*, stranger, whom the password
// file holds, connects in MQTT 5 and MQTT 3.1.1: CONNACK 0x00.
func TestACLUserMatchedByNoEntryIsAccepted(t *testing.T) {
	o := aclOptions(t, "roles:\n"+aclFree+"users:\n  \"sensor-*\": [free]\n", "stranger")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			harness.Connected(t, b.Addr, version, aclLogin(clientID(t, ""), "stranger"))
		})
	}
}

// aclDenials are roles each denying features (design.md, Access and
// reload: "A features rule takes deny: instead, naming any of persistent,
// will, share, retained and qos2. A feature is denied if any of a user's
// roles denies it"), and narrow, granting write on allowed/# alone.
const aclDenials = `  nowill:
    - broker: features
      deny: [will]
  noretained:
    - broker: features
      deny: [retained]
  noqos2:
    - broker: features
      deny: [qos2]
  noshare:
    - broker: features
      deny: [share]
  nopersist:
    - broker: features
      deny: [persistent]
  narrow:
    - topic: allowed/#
      allow: [write]
`

// willCase is a CONNECT with a Will and the answer it gets.
type willCase struct {
	name, user, client string
	will               harness.Will
	refused            byte               // MQTT 5's reason; 0 for accepted
	props              harness.Properties // the CONNECT's own; a row holding any is MQTT 5's alone
}

// returnCodeOf is the MQTT 3.1.1 return code of a CONNECT refusal
// (design.md, Flows, CONNECT, the refusal table).
func returnCodeOf(reason byte) byte {
	switch reason {
	case 0x85:
		return 2
	case 0x86:
		return 4
	}
	return 5 // 0x87, 0x90, 0x9A, 0x97 (the Will), 0x9B
}

// design.md, Flows, CONNECT, the refusal table: "0x87 | the Will: will
// denied, or its topic not allowed | 5"; "0x9A | a retained Will, on any
// topic, with retained denied (MQTT-3.2.2-13) | 5"; "0x97 | a retained Will on
// $broadcast that alone exceeds retained.max_bytes | 5"; "0x9B | a QoS 2
// Will with qos2 denied | 5"; "It checks admission reasons in the order
// 0x85, 0x86, 0x87, 0x90, 0x9A, 0x97 (the Will), 0x9B, 0x83, 0x8C"; "0x83 |
// an unknown saguin- property | 3"; "0x8C | an Authentication Method | not
// in MQTT 3.1.1"; Flows:
// "Policy checks select the first applicable reason in the stated order";
// Access and reload: "Readers check the acl_file at CONNECT for client ids,
// features and the Will's topic"; CONNECT: "A user matched by no users: pattern
// is accepted with no rights". MQTT 5 [MQTT-3.2.2-12], [MQTT-3.2.2-13];
// MQTT 3.1.1 3.2.2.3, return code 5, "Connection Refused, not authorized".
//
// The broker has retained.max_bytes 64 and the channel events. Each CONNECT
// carries a Will, in MQTT 5 and MQTT 3.1.1, and gets the reason of its row,
// or 0x00 (the users' roles: aclFree and aclDenials). A row with CONNECT
// properties runs in MQTT 5 alone: MQTT 3.1.1's CONNECT has none.
func TestACLWillAtConnect(t *testing.T) {
	acl := "roles:\n" + aclFree + aclDenials + `users:
  writer: [free]
  scoped: [narrow]
  mute: [free, nowill]
  keeper: [free, noretained]
  single: [free, noqos2]
  bare: [free, noretained, noqos2]
  hush: [free, nowill, noretained, noqos2]
  silent: [narrow, noretained]
  fenced: {roles: [free, nowill], client_ids: "fenced-*"}
`
	o := aclOptions(t, acl, "writer", "scoped", "mute", "keeper", "single", "bare", "hush", "silent", "fenced", "stranger")
	o.Channels = aclChannels
	o.RetainedMaxBytes = 64
	b := support.Launch(t, o)
	payload := []byte("gone")
	big := bytes.Repeat([]byte("x"), 200) // alone over retained.max_bytes
	for _, c := range []willCase{
		{"will denied", "mute", "", harness.Will{Topic: "w/x", Payload: payload}, 0x87, nil},
		{"a topic the user may not write", "scoped", "", harness.Will{Topic: "elsewhere/x", Payload: payload}, 0x87, nil},
		{"a topic the user may write", "scoped", "", harness.Will{Topic: "allowed/x", Payload: payload}, 0, nil},
		{"a user matched by no entry, with no rights", "stranger", "", harness.Will{Topic: "w/x", Payload: payload}, 0x87, nil},
		{"retained on $broadcast with retained denied", "keeper", "", harness.Will{Retain: true, Topic: "w/x", Payload: payload}, 0x9A, nil},
		{"not retained, retained denied", "keeper", "", harness.Will{Topic: "w/x", Payload: payload}, 0, nil},
		{"retained on a channel topic with retained denied", "keeper", "", harness.Will{Retain: true, Topic: "events/x", Payload: payload}, 0x9A, nil},
		{"QoS 2 with qos2 denied", "single", "", harness.Will{QoS: 2, Topic: "w/x", Payload: payload}, 0x9B, nil},
		{"0x85 before 0x87", "mute", strings.Repeat("i", 257), harness.Will{Topic: "w/x", Payload: payload}, 0x85, nil},
		{"0x86 before 0x87: a client id the entry refuses", "fenced", "other", harness.Will{Topic: "w/x", Payload: payload}, 0x86, nil},
		{"0x87 before 0x90", "mute", "", harness.Will{Topic: "w/#", Payload: payload}, 0x87, nil},
		{"0x87 before 0x9A: will denied", "hush", "", harness.Will{Retain: true, Topic: "w/x", Payload: payload}, 0x87, nil},
		{"0x87 before 0x9A: a topic not allowed", "silent", "", harness.Will{Retain: true, Topic: "elsewhere/x", Payload: payload}, 0x87, nil},
		{"0x87 before 0x9B", "hush", "", harness.Will{QoS: 2, Topic: "w/x", Payload: payload}, 0x87, nil},
		{"0x90 before 0x9A", "keeper", "", harness.Will{Retain: true, Topic: "w/+", Payload: payload}, 0x90, nil},
		{"0x9A before 0x97", "keeper", "", harness.Will{Retain: true, Topic: "w/x", Payload: big}, 0x9A, nil},
		{"0x9A before 0x9B", "bare", "", harness.Will{Retain: true, QoS: 2, Topic: "w/x", Payload: payload}, 0x9A, nil},
		{"0x97 before 0x9B", "single", "", harness.Will{Retain: true, QoS: 2, Topic: "w/x", Payload: big}, 0x97, nil},
		{"0x9B before 0x83", "single", "", harness.Will{QoS: 2, Topic: "w/x", Payload: payload}, 0x9B, harness.Properties{harness.User("saguin-x", "v")}},
		{"0x9B before 0x8C", "single", "", harness.Will{QoS: 2, Topic: "w/x", Payload: payload}, 0x9B, harness.Properties{{ID: harness.AuthenticationMethod, Str: "m"}}},
	} {
		for _, version := range []byte{harness.V5, harness.V311} {
			if c.props != nil && version == harness.V311 {
				continue // MQTT 3.1.1's CONNECT carries no properties
			}
			t.Run(c.name+", "+harness.VersionName(version), func(t *testing.T) {
				w := c.will
				p := aclLogin(cmp.Or(c.client, clientID(t, "")), c.user)
				p.Will = &w
				p.Props = c.props
				if c.refused == 0 {
					harness.Connected(t, b.Addr, version, p)
					return
				}
				want := c.refused
				if version == harness.V311 {
					want = returnCodeOf(c.refused)
				}
				connackRefuses(t, b.Addr, version, p, want)
			})
		}
	}
}

// design.md, Flows, CONNECT, 2: "Check. The broker checks password, access,
// version, client id and limits. A failure gets a CONNACK with its reason,
// then the connection closes", before "4. Load", "5. Take over" and "6.
// Decide"; item 0018c (261019 054601, from audits 261018-101a and -102a):
// "the CONNECT's check comes before any session is touched [...] the
// acl_file's admission of that Will comes before that save". MQTT 5
// [MQTT-3.1.4-2]: a failed authorization check closes the connection, and
// only "If validation is successful" is the existing Client's connection
// taken over and Clean Start processed.
//
// owner's durable session subscribes to owned/t. A CONNECT for its client
// id by fenced, whose entry admits other client ids alone, is refused 0x86,
// and owner's connection is kept: its PINGREQ is answered. owner goes away;
// a message is published to its filter. CONNECTs for its client id are then
// refused, none answered with anything but the CONNACK: fenced, Clean Start
// 0, 0x86; mute (will denied), Clean Start 0 with a Will, 0x87; keeper
// (retained denied), Clean Start 1 with a retained Will, 0x9A; single (qos2
// denied), Clean Start 0 with a QoS 2 Will, 0x9B; mute in MQTT 3.1.1, Clean
// Session 1 with a Will, 5. The store's rows of the session are as they
// were, and owner resumes it, Session Present 1, and is sent the message.
func TestACLRefusedConnectLeavesTheSessionUntouched(t *testing.T) {
	acl := "roles:\n" + aclFree + aclDenials + `users:
  owner: [free]
  fenced: {roles: [free], client_ids: "fenced-*"}
  mute: [free, nowill]
  keeper: [free, noretained]
  single: [free, noqos2]
`
	o := aclOptions(t, acl, "owner", "fenced", "mute", "keeper", "single")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	id := clientID(t, "")
	owner := aclLogin(id, "owner")
	owner.Props = support.Durable
	c := support.Subscribed(t, b.Addr, harness.V5, owner, []harness.Subscription{{Filter: "owned/t", QoS: 1}})

	connackRefuses(t, b.Addr, harness.V5, aclLogin(id, "fenced"), 0x86)
	if err := c.Ping(); err != nil {
		t.Fatalf("the session's own client after a CONNECT for its id the acl_file refused: %v; want its connection kept", err)
	}
	support.Away(t, c)
	publisher, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(id+"-publisher", "owner"))
	support.Acknowledged(t, publisher, &harness.Publish{QoS: 1, Topic: "owned/t", Payload: []byte("while away")})
	db := support.OpenDB(t, b.DB)
	before := sessionState(t, db, id)

	withWill := func(user string, clean bool, w harness.Will) *harness.Connect {
		p := aclLogin(id, user)
		p.CleanStart, p.Props, p.Will = clean, support.Durable, &w
		return p
	}
	resume := aclLogin(id, "fenced")
	resume.CleanStart, resume.Props = false, support.Durable
	connackRefuses(t, b.Addr, harness.V5, resume, 0x86)
	connackRefuses(t, b.Addr, harness.V5, withWill("mute", false, harness.Will{Topic: "w/x", Payload: support.WillPayload}), 0x87)
	connackRefuses(t, b.Addr, harness.V5, withWill("keeper", true, harness.Will{Retain: true, Topic: "w/x", Payload: support.WillPayload}), 0x9A)
	connackRefuses(t, b.Addr, harness.V5, withWill("single", false, harness.Will{QoS: 2, Topic: "w/x", Payload: support.WillPayload}), 0x9B)
	old := withWill("mute", true, harness.Will{Topic: "w/x", Payload: support.WillPayload})
	old.Props = nil
	connackRefuses(t, b.Addr, harness.V311, old, 5)
	if after := sessionState(t, db, id); after != before {
		t.Errorf("CONNECTs the acl_file refused changed the stored session:\nbefore %s\nafter  %s", before, after)
	}
	r := resumed(t, b.Addr, harness.V5, owner)
	if p := harness.Expect[*harness.Publish](t, r); p.Topic != "owned/t" || string(p.Payload) != "while away" {
		t.Errorf("the resumed session is sent %q %q; want the message published while it was away", p.Topic, p.Payload)
	}
}

// design.md, Flows, CONNECT: "A client whose persistent is denied is
// accepted with Session Expiry 0"; 8: the CONNACK carries the Session
// Expiry Interval the broker "capped or chose"; Words: Session, "durable
// when its expiry is above zero"; CONNECT: "Session Present is 1 when the
// CONNECT keeps its session and a session exists for the client id [...]
// Keeping the session means Clean Start 0, or MQTT 3.1.1 Clean Session 0".
// MQTT 5 3.2.2.3.2: "If the Server uses a Session Expiry Interval different
// from the one the Client sent, it MUST include it in the CONNACK".
//
// transient's roles deny persistent; keeper's do not.
//   - MQTT 5: transient connects asking a Session Expiry Interval of an
//     hour: CONNACK 0x00 carrying Session Expiry Interval 0. It subscribes
//     and disconnects; its CONNECT with Clean Start 0 gets Session Present
//     0.
//   - MQTT 3.1.1, Clean Session 0: transient's reconnect gets Session
//     Present 0, and keeper's, the same steps, Session Present 1.
func TestACLPersistentDenied(t *testing.T) {
	acl := "roles:\n" + aclFree + aclDenials + `users:
  transient: [free, nopersist]
  keeper: [free]
`
	o := aclOptions(t, acl, "transient", "keeper")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	subs := []harness.Subscription{{Filter: "kept/t", QoS: 1}}
	t.Run(harness.VersionName(harness.V5), func(t *testing.T) {
		p := aclLogin(clientID(t, ""), "transient")
		p.Props = support.Durable
		c, ack := harness.Connected(t, b.Addr, harness.V5, p)
		if v, ok := connackSays(ack, harness.SessionExpiryInterval); !ok || v != 0 {
			t.Errorf("CONNACK's Session Expiry Interval %d (present %v); want 0, persistent being denied", v, ok)
		}
		support.Suback(t, c, nil, subs, 1)
		support.Away(t, c)
		p.CleanStart = false
		if _, ack := harness.Connected(t, b.Addr, harness.V5, p); ack.SessionPresent {
			t.Error("the reconnect with Clean Start 0 has Session Present 1; want 0, the session not kept")
		}
	})
	for _, c := range []struct {
		user string
		kept bool
	}{{"transient", false}, {"keeper", true}} {
		t.Run(harness.VersionName(harness.V311)+", "+c.user, func(t *testing.T) {
			p := aclLogin(clientID(t, ""), c.user)
			p.CleanStart = false
			cl, _ := harness.Connected(t, b.Addr, harness.V311, p)
			support.Suback(t, cl, nil, subs, 1)
			support.Away(t, cl)
			if _, ack := harness.Connected(t, b.Addr, harness.V311, p); ack.SessionPresent != c.kept {
				t.Errorf("the reconnect with Clean Session 0 has Session Present %v; want %v", ack.SessionPresent, c.kept)
			}
		})
	}
}

// design.md, Flows, CONNECT, 8: the CONNACK carries "the Maximum QoS,
// Retain Available and Shared Subscription Available the acl_file allows";
// Access and reload: "With none, everything is allowed". MQTT 5 3.2.2.3.4
// (Maximum QoS: "If the Maximum QoS is absent, the Client uses a Maximum
// QoS of 2"), 3.2.2.3.5 (Retain Available: "If not present, then retained
// messages are supported"), 3.2.2.3.13 (Shared Subscription Available: "If
// not present, then Shared Subscriptions are supported").
//
// limited's roles deny qos2, retained and share: its CONNACK carries
// Maximum QoS 1, Retain Available 0 and Shared Subscription Available 0.
// open's deny nothing, and the package's broker has no acl_file: neither
// CONNACK withholds retained messages, shared subscriptions or QoS 2, its
// Maximum QoS left out.
func TestACLConnackCapabilities(t *testing.T) {
	acl := "roles:\n" + aclFree + aclDenials + `users:
  limited: [free, noqos2, noretained, noshare]
  open: [free]
`
	o := aclOptions(t, acl, "limited", "open")
	o.Channels = aclChannels
	b := support.Launch(t, o)
	t.Run("denied", func(t *testing.T) {
		_, ack := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, ""), "limited"))
		for _, w := range []struct {
			id   byte
			name string
			want uint32
		}{
			{harness.MaximumQoS, "Maximum QoS", 1},
			{harness.RetainAvailable, "Retain Available", 0},
			{harness.SharedSubscriptionAvailable, "Shared Subscription Available", 0},
		} {
			if v, ok := connackSays(ack, w.id); !ok || v != w.want {
				t.Errorf("CONNACK's %s %d (present %v); want %d", w.name, v, ok, w.want)
			}
		}
	})
	for _, c := range []struct {
		name, addr string
		p          func(client string) *harness.Connect
	}{
		{"allowed under an acl_file", b.Addr, func(client string) *harness.Connect { return aclLogin(client, "open") }},
		{"without an acl_file", brokerAddr, func(client string) *harness.Connect {
			return &harness.Connect{ClientID: client, CleanStart: true}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, ack := harness.Connected(t, c.addr, harness.V5, c.p(clientID(t, "")))
			for _, id := range []byte{harness.RetainAvailable, harness.SharedSubscriptionAvailable, harness.MaximumQoS} {
				if withholds(ack, id) {
					t.Errorf("CONNACK withholds property 0x%02X's feature: %+v; want it allowed", id, ack.Props)
				}
			}
		})
	}
}

// ackReasons sends n MQTT 5 PUBLISHes at QoS 1 on topic in one write, as
// soon as c's CONNACK has come (pubackReasons), and returns their PUBACKs'
// reasons, in order.
func ackReasons(t *testing.T, c *harness.Client, n int, topic string) []byte {
	t.Helper()
	ps := make([]*harness.Publish, n)
	for i := range ps {
		ps[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("x")}
	}
	return pubackReasons(t, c, ps)
}

// config.md, broker.limits: publish_rate, publish_bytes, "messages and
// bytes per second per connection; throttling, per-user overrides and
// accounting follow PUBLISH, Rate limits";
// design.md, Flows, PUBLISH, Rate limits: "Each limit comes from the user's
// acl_file entry where it sets one, and otherwise from broker.limits
// (Access and reload). An entry setting only one limit keeps the broker's
// value of the other"; "When a charge leaves the message bucket below one
// message, or the byte bucket at or below zero, the reader reads nothing
// more from its connection until the message bucket holds one message again
// and the byte bucket is above zero"; Access and reload: "The limits are
// publish_rate, a whole number of messages per second, and publish_bytes, a
// size per second. An entry may write either or both, and they apply to
// each of the user's connections separately".
//
// The broker's publish_rate is 5 and its publish_bytes 1000. Each case is
// an MQTT 5 client of its own writing PUBLISHes at QoS 1 at once, a PINGREQ
// after them (paced): every PUBACK is 0x00, and the PINGRESP comes no
// sooner than least's part of:
//   - rate-alone, its entry's publish_rate 1 alone, two of a byte: two
//     seconds, each read a second after the one before, the entry's rate
//     standing where the broker's 5 would take them at once;
//   - rate-alone, one of 4000 bytes: three seconds, the broker's
//     publish_bytes kept, in debt by at least 3000 bytes, where the
//     entry's rate alone would read the PINGREQ a second on;
//   - bytes-alone, its entry's publish_bytes 500 alone, one of 1500 bytes:
//     two seconds, the entry's bytes standing, in debt by at least 1000,
//     where the broker's 1000 a second would pay half as much;
//   - bytes-alone, seven of a byte: three fifths of a second, the broker's
//     publish_rate kept, its bucket of 5 spent and the last two and the
//     PINGREQ each read a fifth of a second after the one before, where the
//     entry's bytes alone would take them at once.
func TestACLEntryLimitsReplaceTheBrokers(t *testing.T) {
	acl := "roles:\n" + aclFree + `users:
  rate-alone: {roles: [free], limits: {publish_rate: 1}}
  bytes-alone: {roles: [free], limits: {publish_bytes: 500}}
`
	o := aclOptions(t, acl, "rate-alone", "bytes-alone")
	o.Channels = aclChannels
	o.PublishRate, o.PublishBytes = 5, 1000
	b := support.Launch(t, o)
	sized := func(n, size int) [][]byte { return slices.Repeat([][]byte{make([]byte, size)}, n) }
	for _, c := range []struct {
		name, user string
		payloads   [][]byte
		wait       time.Duration
	}{
		{"the entry's publish_rate", "rate-alone", sized(2, 1), 2 * time.Second},
		{"the broker's publish_bytes beside the entry's publish_rate", "rate-alone", sized(1, 4000), 3 * time.Second},
		{"the entry's publish_bytes", "bytes-alone", sized(1, 1500), 2 * time.Second},
		{"the broker's publish_rate beside the entry's publish_bytes", "bytes-alone", sized(7, 1), 3 * time.Second / 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cl, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, ""), c.user))
			ps := make([]*harness.Publish, len(c.payloads))
			for i, payload := range c.payloads {
				ps[i] = &harness.Publish{QoS: 1, Topic: "rated/" + c.user, Payload: payload}
			}
			took, got := paced(t, cl, ps)
			if slices.ContainsFunc(got, func(r byte) bool { return r != 0 }) {
				t.Errorf("%d PUBLISHes written at once get PUBACKs % X; want 0x00 to each, nothing refused", len(ps), got)
			}
			if took < least(c.wait) {
				t.Errorf("the PINGRESP after them came %v after the write; want no sooner than %v", took, least(c.wait))
			}
		})
	}
}
