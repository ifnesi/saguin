package conformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the operations state views (item 0020f): design.md,
// Operations, HTTP, the routes config, users, sessions, consumers,
// queues/<channel> and bridges, their order, their 100-row lists, their
// times and the 5-second rule; metrics.md, Process,
// saguin_operations_requests_total. Each launches a broker of its own with
// an operations listener on a loopback address and calls it with no
// credential, as an operator on the same machine does.

// stateView GETs path from b's operations listener and returns its answer,
// failing the test unless it is 200 and a JSON object (design.md,
// Operations, HTTP: "Answers are JSON"; "Each answer under /v1/operations/
// is an object").
func stateView(t *testing.T, b *harness.Broker, path string) map[string]json.RawMessage {
	t.Helper()
	a := opsRequest(t, b, http.MethodGet, path, "")
	if a.Status != http.StatusOK || a.MediaType != "application/json" {
		t.Fatalf("GET %s: %d %s %q; want 200 and a JSON object", path, a.Status, a.MediaType, a.Body)
	}
	var view map[string]json.RawMessage
	if err := json.Unmarshal([]byte(a.Body), &view); err != nil || view == nil {
		t.Fatalf("GET %s: %q is no JSON object (%v)", path, a.Body, err)
	}
	return view
}

// field is harness.StateField, failing the test where it fails.
func field(t *testing.T, view map[string]json.RawMessage, key string, into any) {
	t.Helper()
	if err := harness.StateField(view, key, into); err != nil {
		t.Fatal(err)
	}
}

// listed is harness.StateRows, failing the test where it fails.
func listed(t *testing.T, view map[string]json.RawMessage, key string, rows any) (total, returned int) {
	t.Helper()
	total, returned, err := harness.StateRows(view, key, rows)
	if err != nil {
		t.Fatal(err)
	}
	return total, returned
}

// decoded is a's body decoded as JSON, failing the test where it is not.
func decoded(t *testing.T, a harness.OpsAnswer) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(a.Body), &v); err != nil {
		t.Fatalf("%q is no JSON: %v", a.Body, err)
	}
	return v
}

// expectUTCSecond fails the test unless s, the time what names, is RFC 3339
// in UTC to the second (harness.UTCSecond), within from (to the second) and
// to; a null s fails. It checks every time a state view or a history
// answers (design.md, Operations, HTTP: "Times are RFC 3339 in UTC, to the
// second").
func expectUTCSecond(t *testing.T, what string, s *string, from, to time.Time) {
	t.Helper()
	if s == nil {
		t.Errorf("%s is null; want a time from %v to %v", what, from, to)
		return
	}
	at, err := harness.UTCSecond(*s)
	switch {
	case err != nil:
		t.Errorf("%s %v", what, err)
	case at.Before(from.Truncate(time.Second)) || at.After(to):
		t.Errorf("%s %s; want from %s to %s", what, *s, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))
	}
}

// hashSchemes are how every hash a password file holds begins (config.md,
// broker.mqtt: password_file, "the hash is $7$<iterations>$<salt>$<hash>
// [...] or $6$<salt>$<hash>").
var hashSchemes = []string{"$7$", "$6$"}

// The channels the state views' tests configure: an append channel, a
// queue and a latest channel, each holding its own topics.
var (
	eventsLog = harness.Channel{Name: "events", Type: "append", Filter: "events/#"}
	jobsQueue = harness.Channel{Name: "jobs", Type: "queue", Filter: "jobs/#"}
	kvLatest  = harness.Channel{Name: "kv", Type: "latest", Filter: "kv/#"}
)

// design.md, Operations, HTTP: "config returns the resolved configuration,
// with channels keyed by name. Each channel has configured_in, which names
// its file."; "Dead-letter channels are left out. The repeatable ?section=
// parameter takes bridges, channels, limits, listeners, operations or
// storage. Any other value gets 400."; "Errors are plain text". config.md:
// "A group is written in place or as !include <file>"; broker.mqtt,
// password_file; 0020f.md, 261015 171717: "password_file printed as its
// path, never a password". This replaces internal/config's
// TestConfiguredIn.
//
// The configuration names events and jobs, a queue, in its channels' first
// group, and kv in a group it includes; and a password file. config keys
// its channels events, jobs and kv, not jobs__dlq; events and jobs are
// configured in the configuration file, kv in the included one. The
// password file shows as its path and no hash shows. ?section=channels
// shows the channels and neither the storage nor the limits;
// ?section=storage&section=limits shows those and no channel. A section
// broker, or nothing beside channels, gets 400 in plain text.
func TestOperationsConfigShowsTheResolvedConfiguration(t *testing.T) {
	dir := t.TempDir()
	passwords := usersFile(t, dir, "passwords", "alice")
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, Login: harness.Login{PasswordFile: passwords},
		Channels: []harness.Channel{eventsLog, jobsQueue}, IncludedChannels: []harness.Channel{kvLatest}})
	included := filepath.Join(dir, harness.IncludedChannelsName)

	view := stateView(t, b, "/v1/operations/config")
	var channels map[string]struct {
		ConfiguredIn string `json:"configured_in"`
	}
	field(t, view, "channels", &channels)
	if got, want := slices.Sorted(maps.Keys(channels)), []string{"events", "jobs", "kv"}; !slices.Equal(got, want) {
		t.Errorf("config's channels are %q; want %q, keyed by name, the dead-letter channel left out", got, want)
	}
	for name, file := range map[string]string{"events": b.Config, "jobs": b.Config, "kv": included} {
		if got := channels[name].ConfiguredIn; got != file {
			t.Errorf("channel %s: configured_in %q; want %q, the file that defines it", name, got, file)
		}
	}
	a := opsRequest(t, b, http.MethodGet, "/v1/operations/config", "")
	if got := harness.ValuesOf(decoded(t, a), "password_file"); !slices.Contains(got, any(passwords)) {
		t.Errorf("config's password_file values are %q; want the path %q among them", got, passwords)
	}
	for _, scheme := range hashSchemes {
		if strings.Contains(a.Body, scheme) {
			t.Errorf("config shows a password hash, %s: %q", scheme, a.Body)
		}
	}

	for _, s := range []struct {
		query     string
		shows     []string // keys the sections hold
		leavesOut []string // keys of the sections not asked
	}{
		{"section=channels", []string{"configured_in"}, []string{"file_path", "write_timeout"}},
		{"section=storage&section=limits", []string{"file_path", "write_timeout"}, []string{"configured_in"}},
	} {
		path := "/v1/operations/config?" + s.query
		got := decoded(t, opsRequest(t, b, http.MethodGet, path, ""))
		for _, key := range s.shows {
			if len(harness.ValuesOf(got, key)) == 0 {
				t.Errorf("GET %s holds no %s; want the sections asked", path, key)
			}
		}
		for _, key := range s.leavesOut {
			if len(harness.ValuesOf(got, key)) != 0 {
				t.Errorf("GET %s holds %s; want only the sections asked", path, key)
			}
		}
	}
	for _, query := range []string{"section=broker", "section=channels&section=nothing"} {
		path := "/v1/operations/config?" + query
		if a := opsRequest(t, b, http.MethodGet, path, ""); a.Status != http.StatusBadRequest || a.MediaType != "text/plain" {
			t.Errorf("GET %s: %d %s %q; want 400 in plain text", path, a.Status, a.MediaType, a.Body)
		}
	}
}

// design.md, Operations, HTTP: config "shows the configuration in effect.
// An applied reload changes only the log level and WebSocket origins it
// shows. Any other edit to the file shows only after a restart."; Access
// and reload: "It reloads the log level and WebSocket origins"; "Nothing
// else reloads".
//
// A broker has log_level error, a ws listener allowing firstOrigin, and
// events with a retention_period of an hour: config shows log_level error
// and firstOrigin. The configuration then has log_level warn, allows
// secondOrigin alone and keeps events two hours, and the broker gets
// SIGUSR1, every step applied. config shows log_level warn and
// secondOrigin, not firstOrigin, and events as it was before.
func TestOperationsConfigShowsOnlyWhatAReloadApplied(t *testing.T) {
	kept := eventsLog
	kept.RetentionPeriod = time.Hour
	b := support.OpsLaunch(t, &harness.Options{Dir: t.TempDir(), LogLevel: "error", Channels: []harness.Channel{kept},
		WS: []harness.WS{{SameOrigin: "false", AllowedOrigins: []string{firstOrigin}}}})
	shown := func(when, level, origin, gone string) json.RawMessage {
		t.Helper()
		var channels map[string]json.RawMessage
		field(t, stateView(t, b, "/v1/operations/config"), "channels", &channels)
		a := opsRequest(t, b, http.MethodGet, "/v1/operations/config", "")
		if got := harness.ValuesOf(decoded(t, a), "log_level"); !slices.Equal(got, []any{level}) {
			t.Errorf("%s, config's log_level is %q; want %q", when, got, level)
		}
		if !strings.Contains(a.Body, origin) || strings.Contains(a.Body, gone) {
			t.Errorf("%s, config %q; want the allowed origin %s, not %s", when, a.Body, origin, gone)
		}
		return channels["events"]
	}
	before := shown("before the reload", "error", firstOrigin, secondOrigin)

	edited(t, b.Config, "log_level: error", "log_level: warn")
	edited(t, b.Config, strconv.Quote(firstOrigin), strconv.Quote(secondOrigin))
	edited(t, b.Config, "retention_period: 3600s", "retention_period: 7200s")
	reload(t, b)
	if after := shown("after the reload", "warn", secondOrigin, firstOrigin); !bytes.Equal(after, before) {
		t.Errorf("after the reload, config shows events as %s; want %s, as before: an edited channel shows only after a restart", after, before)
	}
}

// design.md, Operations, HTTP: "users returns users, anonymous_allowed and
// listeners. The users are the sorted names in the broker's password file.
// The listeners are keyed by listener name, each with users,
// anonymous_allowed, certificate (required, accepted or none) and
// proxy_identity. The answer never includes a hash or the operators'
// file."; "Equal sort ranks use the complete row key in UTF-8 byte order".
// config.md, A listener: password_file, "the listener's own file";
// allow_anonymous, "false with the listener's own password_file"; broker.mqtt:
// allow_anonymous, "false with one".
//
// The broker's password file holds zed, alice and Bob, in that order;
// listener tcp1 takes it and allows anonymous clients; tcp2 has its own
// file, holding dave and carol. The operators' file holds operator-root.
// users is Bob, alice, zed, anonymous not allowed; tcp1 has the broker's
// users, anonymous allowed, certificate none; tcp2 its own, carol and dave,
// anonymous not allowed, certificate none; each has a proxy_identity. No
// hash, operator-root or operators' file shows.
func TestOperationsUsersShowsNamesAndListeners(t *testing.T) {
	dir := t.TempDir()
	brokers := usersFile(t, dir, "passwords", "zed", "alice", "Bob")
	own := usersFile(t, dir, "tcp2-passwords", "dave", "carol")
	operators := usersFile(t, dir, "operators", "operator-root")
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, TCPListeners: 2, OpsPasswordFile: operators,
		Login: harness.Login{PasswordFile: brokers}, ListenerLogin: []harness.Login{{AllowAnonymous: "true"}, {PasswordFile: own}}})

	a := opsRequest(t, b, http.MethodGet, "/v1/operations/users", "")
	view := stateView(t, b, "/v1/operations/users")
	var users []string
	var anonymous bool
	var listeners map[string]map[string]json.RawMessage
	field(t, view, "users", &users)
	field(t, view, "anonymous_allowed", &anonymous)
	field(t, view, "listeners", &listeners)
	if want := []string{"Bob", "alice", "zed"}; !slices.Equal(users, want) || anonymous {
		t.Errorf("users %q, anonymous_allowed %v; want %q, sorted, and false", users, anonymous, want)
	}
	if got := slices.Sorted(maps.Keys(listeners)); !slices.Equal(got, []string{"tcp1", "tcp2"}) {
		t.Fatalf("listeners keyed %q; want tcp1 and tcp2", got)
	}
	for name, want := range map[string]struct {
		users     []string
		anonymous bool
	}{"tcp1": {[]string{"Bob", "alice", "zed"}, true}, "tcp2": {[]string{"carol", "dave"}, false}} {
		l := listeners[name]
		var users []string
		var anonymous bool
		var certificate string
		field(t, l, "users", &users)
		field(t, l, "anonymous_allowed", &anonymous)
		field(t, l, "certificate", &certificate)
		if _, ok := l["proxy_identity"]; !ok {
			t.Errorf("listener %s has no proxy_identity", name)
		}
		if !slices.Equal(users, want.users) || anonymous != want.anonymous || certificate != "none" {
			t.Errorf("listener %s: users %q, anonymous_allowed %v, certificate %q; want %q, %v, none",
				name, users, anonymous, certificate, want.users, want.anonymous)
		}
	}
	for _, secret := range slices.Concat(hashSchemes, []string{"operator-root", operators}) {
		if strings.Contains(a.Body, secret) {
			t.Errorf("users shows %q: %q; want never a hash or the operators' file", secret, a.Body)
		}
	}
}

// sessionsAnswer is sessions' answer: its rows, and its total, returned,
// connected and away (design.md, Operations, HTTP: "The answer includes
// total, connected and away").
type sessionsAnswer struct {
	rows                             []harness.StateSession
	total, returned, connected, away int
}

// sessionsOf reads b's sessions.
func sessionsOf(t *testing.T, b *harness.Broker) sessionsAnswer {
	t.Helper()
	view := stateView(t, b, "/v1/operations/sessions")
	var s sessionsAnswer
	s.total, s.returned = listed(t, view, harness.SessionsRows, &s.rows)
	field(t, view, "connected", &s.connected)
	field(t, view, "away", &s.away)
	return s
}

// awaitSessions waits until b's sessions count connected connected and
// away away sessions, and returns them, failing the test where they do not
// within harness.DefaultTimeout, the test's watchdog: a session goes away
// once the core takes its connection's end.
func awaitSessions(t *testing.T, b *harness.Broker, connected, away int) sessionsAnswer {
	t.Helper()
	var s sessionsAnswer
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		s = sessionsOf(t, b)
		return s.connected == connected && s.away == away
	}) {
		t.Fatalf("watchdog: after %v, sessions counts %d connected and %d away; want %d and %d", harness.DefaultTimeout, s.connected, s.away, connected, away)
	}
	return s
}

// leaves sends DISCONNECT 0x00 from c and waits for its connection's end.
func leaves(t *testing.T, c *harness.Client) {
	t.Helper()
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
}

// design.md, Operations, HTTP: sessions "The answer includes total,
// connected and away. Away sessions come first, then sessions by client
// id."; "A state list holds at most 100 rows."; "Identity and sorting use
// uncut values. Display truncation of a client id or user name keeps the
// longest complete UTF-8 prefix of at most 1,024 bytes."; "The
// expires_after is the session's granted expiry interval. The
// subscriptions is a count." Words: Session, "An away session lives in the
// store alone." config.md, broker.limits: max_session_expiry, "30d [...] is
// MQTT 3.1.1 Clean Session 0's"; A listener: name, "its kind".
//
// long, 1,023 bytes of a then é and z, connects over MQTT 5 with a Session
// Expiry of an hour and leaves; b-away connects over MQTT 3.1.1 with Clean
// Session 0, subscribes to one filter and leaves; a-00 to a-98, whose ids
// sort first, connect over MQTT 5 with Clean Start 1 and a keep-alive of
// 30, in reverse order, and stay. sessions counts 101 in all, 99 connected
// and 2 away, and returns 100 rows: long, cut to its 1,023 bytes of a,
// away, expires_after 3600; b-away, away, 3.1.1, expires_after 2,592,000,
// clean_start false, one subscription; then a-00 to a-97, connected,
// protocol 5, listener tcp, keepalive 30, expires_after 0, clean_start
// true.
func TestOperationsSessionsListsAwayFirstThenByClientID(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxClientIDLength: 2048})
	long := strings.Repeat("a", 1023) + "éz"
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: long, CleanStart: true, Props: support.Durable})
	leaves(t, c)
	c = support.Subscribed(t, b.Addr, harness.V311, &harness.Connect{ClientID: "b-away"}, support.At(1, "x/#"))
	leaves(t, c)
	connected := make([]string, 99)
	for i := range connected {
		connected[i] = fmt.Sprintf("a-%02d", i)
	}
	for _, id := range slices.Backward(connected) {
		harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, KeepAlive: 30})
	}

	s := awaitSessions(t, b, 99, 2)
	if s.total != 101 || s.returned != 100 || len(s.rows) != 100 {
		t.Fatalf("sessions: total %d, returned %d, %d rows; want 101, 100 and 100 rows", s.total, s.returned, len(s.rows))
	}
	ids := make([]string, len(s.rows))
	for i, r := range s.rows {
		ids[i] = r.ClientID
	}
	if want := slices.Concat([]string{long[:1023], "b-away"}, connected[:98]); !slices.Equal(ids, want) {
		t.Errorf("sessions' client ids, in order: %.80q; want %.80q: away first, then by client id, long cut to its 1,023 bytes of a", ids, want)
	}
	if r := s.rows[0]; r.Connected || r.ExpiresAfter != 3600 {
		t.Errorf("long's row: connected %v, expires_after %d; want away, 3600", r.Connected, r.ExpiresAfter)
	}
	if r := s.rows[1]; r.Connected || r.Protocol != "3.1.1" || r.ExpiresAfter != 2_592_000 || r.CleanStart || r.Subscriptions != 1 {
		t.Errorf("b-away's row %+v; want away, protocol 3.1.1, expires_after 2592000, clean_start false, subscriptions 1", r)
	}
	for _, r := range s.rows[2:] {
		if !r.Connected || r.Protocol != "5" || r.Listener == nil || *r.Listener != "tcp" || r.Keepalive == nil || *r.Keepalive != 30 ||
			r.ExpiresAfter != 0 || !r.CleanStart || r.Subscriptions != 0 {
			t.Errorf("%s's row %+v; want connected, protocol 5, listener tcp, keepalive 30, expires_after 0, clean_start true, subscriptions 0", r.ClientID, r)
		}
	}
}

// consumersOf reads b's consumers, each entry by its channel, failing the
// test unless the entries come by channel name, counted.
func consumersOf(t *testing.T, b *harness.Broker) map[string]harness.StateConsumers {
	t.Helper()
	var entries []harness.StateConsumers
	total, returned := listed(t, stateView(t, b, "/v1/operations/consumers"), harness.ConsumersRows, &entries)
	names := make([]string, len(entries))
	byName := make(map[string]harness.StateConsumers, len(entries))
	for i, e := range entries {
		names[i] = e.Channel
		byName[e.Channel] = e
	}
	if !slices.IsSorted(names) || total != len(entries) || returned != len(entries) {
		t.Fatalf("consumers' entries %q, total %d, returned %d; want them by name, each counted", names, total, returned)
	}
	return byName
}

// ruleNames reports whether rule, a position's rule, names filter and topic.
// The design names what a bridge's rule holds, not its form.
func ruleNames(rule json.RawMessage, filter, topic string) bool {
	return bytes.Contains(rule, []byte(filter)) && bytes.Contains(rule, []byte(topic))
}

// design.md, Operations, HTTP: "consumers returns channels, one entry per
// append or queue channel, by name. Each entry has channel, next_offset, consumers and
// positions. The consumers counts the positions on the channel: connected
// and away sessions and bridge rules. Each position has reader (mqtt:<client
// id> or bridge:<name>), rule, offset, behind and last_seen. The behind is
// next_offset minus offset. A bridge reader's rule is its filter and topic,
// and otherwise is null."; "Positions appear furthest behind first, at most
// 100 per channel."; "A consumer's key is reader, rule filter and rule
// topic." Words: Retention, "The tail is the next offset"; Bridges, Out:
// "each rule is a durable consumer of what its filter matches locally, with
// its own cursor"; "One record per topic at the peer is in flight at a
// time"; Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records". Channels:
// "$broadcast is the reserved append channel for every topic no configured
// channel claims"; a queue's retention "moves the job to the append channel
// <channel>__dlq".
//
// The broker has events, jobs (a queue) and kv (latest), and bridge edge
// with an out rule events/# to up/$#. r-00 to r-98 subscribe to events/#
// with durable sessions, in reverse order, and leave; a-reader subscribes
// and stays. A publisher stores events/x three times, at offsets 1 to 3.
// a-reader acknowledges all three; edge's peer acknowledges the first and
// holds the second. consumers has entries for $broadcast, events, jobs and
// jobs__dlq, in that order, none for kv; jobs counts no position. events: next_offset 4, consumers 101, 100
// positions: r-00 to r-98, offset 1, behind 3, rule null; then edge, offset
// 2, behind 2, its rule naming events/# and up/$#. a-reader, caught up, is
// past the 100.
func TestOperationsConsumersShowsFurthestBehindFirst(t *testing.T) {
	rule := support.Out("events/#", "up/$#", 1)
	b, _, l := support.Outbound(t, &harness.Options{Operations: true, Channels: []harness.Channel{eventsLog, jobsQueue, kvLatest}},
		"edge", support.Quiet, rule)
	support.AwaitReady(t, b)
	for i := 98; i >= 0; i-- {
		c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: fmt.Sprintf("r-%02d", i), CleanStart: true, Props: support.Durable},
			support.At(1, "events/#"))
		leaves(t, c)
	}
	reader := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "a-reader", CleanStart: true}, support.At(1, "events/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	ps := numbered("events/x", 3)
	support.PublishAll(t, pub, ps)
	for _, p := range ps {
		d := support.DeliveredAt(t, reader, 1, p.Topic, p.Payload)
		acknowledge(t, reader, d.ID, 0)
	}
	first, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: first.ID})
	harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout) // the second, held

	var entries map[string]harness.StateConsumers
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		entries = consumersOf(t, b)
		ps := entries["events"].Positions
		return len(ps) == 100 && ps[99].Offset == 2
	}) {
		t.Fatalf("watchdog: after %v, consumers reads %+v; want events' 100th position edge's, at offset 2", harness.DefaultTimeout, entries)
	}
	if got, want := slices.Sorted(maps.Keys(entries)), []string{"$broadcast", "events", "jobs", "jobs__dlq"}; !slices.Equal(got, want) {
		t.Errorf("consumers' entries %q; want %q: one per append or queue channel, $broadcast and the dead-letter channel included, none for kv", got, want)
	}
	if len(entries["jobs"].Positions) != 0 || entries["jobs"].Consumers != 0 {
		t.Errorf("consumers: jobs %+v; want jobs with no position", entries["jobs"])
	}
	events := entries["events"]
	if events.NextOffset != 4 || events.Consumers != 101 {
		t.Errorf("events: next_offset %d, consumers %d; want 4 and 101: 99 away sessions, a-reader and edge's rule", events.NextOffset, events.Consumers)
	}
	for i, p := range events.Positions[:99] {
		if want := fmt.Sprintf("mqtt:r-%02d", i); p.Reader != want || p.Offset != 1 || p.Behind != 3 || string(p.Rule) != "null" {
			t.Errorf("events' position %d: %+v; want %s at offset 1, behind 3, rule null", i, p, want)
		}
	}
	if p := events.Positions[99]; p.Reader != "bridge:edge" || p.Behind != 2 || !ruleNames(p.Rule, rule.Filter, rule.Topic) {
		t.Errorf("events' position 99: reader %s, offset %d, behind %d, rule %s; want bridge:edge at offset 2, behind 2, its rule naming %s and %s",
			p.Reader, p.Offset, p.Behind, p.Rule, rule.Filter, rule.Topic)
	}
}

// design.md, Operations, HTTP: "consumers returns channels, one entry per
// append or queue channel, by name. Each entry has channel, next_offset,
// consumers and positions. The consumers counts the positions on the
// channel: connected and away sessions and bridge rules. A position a
// durable session keeps with no subscription reaching the channel counts
// too. Each position has reader (mqtt:<client id> or bridge:<name>), rule,
// offset, behind and last_seen. The behind is next_offset minus offset. A
// bridge reader's rule is its filter and topic, and otherwise is null";
// "Positions appear furthest behind first"; "Equal sort ranks use the
// complete row key in UTF-8 byte order"; "The core supplies every session
// it holds, connected, and every position it holds [...] The store supplies
// only what the core does not hold: away sessions and their positions".
// Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last subscription reaching an
// append channel or a queue is removed, a durable session keeps its
// position there"; "A volatile session's positions [...] still end with
// the last subscription reaching the channel".
//
// The broker has events, an append channel, where a publisher stores
// events/x, at offset 1. kept-away and kept-here, holding durable sessions,
// and passing, whose session ends with its connection, each subscribe at
// QoS 1 to events/#, receive the record and acknowledge it, and
// unsubscribe; kept-away then leaves. The publisher stores events/x again.
// consumers' events entry has next_offset 3, consumers 2, and two
// positions, kept-away's, away, then kept-here's, connected, each at offset
// 2, behind 1, rule null: both kept with no subscription reaching events,
// and passing's ended.
func TestOperationsConsumersCountKeptPositions(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{eventsLog}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	ps := numbered("events/x", 2)
	support.Acknowledged(t, pub, ps[0])
	for _, id := range []string{"kept-away", "kept-here", "passing"} {
		connect := &harness.Connect{ClientID: id, CleanStart: true}
		if id != "passing" {
			connect.Props = support.Durable
		}
		c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "events/#"))
		acknowledge(t, c, support.DeliveredAt(t, c, 1, ps[0].Topic, ps[0].Payload).ID, 0)
		unsuback(t, c, nil, []string{"events/#"}, 0x00)
		if id == "kept-away" {
			leaves(t, c)
			support.AwaitAway(t, db, id)
		}
	}
	support.Acknowledged(t, pub, ps[1])

	var events harness.StateConsumers
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		events = consumersOf(t, b)["events"]
		return events.NextOffset == 3
	}) {
		t.Fatalf("watchdog: after %v, consumers reads events %+v; want next_offset 3, both records stored", harness.DefaultTimeout, events)
	}
	if events.Consumers != 2 || len(events.Positions) != 2 {
		t.Fatalf("events: consumers %d, positions %+v; want 2 and two positions, kept-away's and kept-here's, each kept with no subscription", events.Consumers, events.Positions)
	}
	for i, reader := range []string{"mqtt:kept-away", "mqtt:kept-here"} {
		if p := events.Positions[i]; p.Reader != reader || p.Offset != 2 || p.Behind != 1 || string(p.Rule) != "null" {
			t.Errorf("events' position %d: %+v; want %s at offset 2, behind 1, rule null", i, p, reader)
		}
	}
}

// design.md, Operations, HTTP: "queues/<channel> returns 404 for anything
// but a queue. It returns the queue's unfinished jobs as records, oldest
// first. Each has offset, topic, attempts and state (ready, backoff, offered or held).
// Where applicable, it also has holder (client id), held_until,
// first_attempt and last_attempt. It never includes a payload."; "Errors
// are plain text". Delivery, Queue jobs: "Jobs are offered oldest first";
// "The hold starts at the worker's PUBACK and lasts visibility_timeout";
// "An attempt is spent at the worker's PUBACK or its response naming the
// attempt, whichever is first. A refusing PUBACK (reason 0x80 or above)
// spends the attempt and ends it as a failure"; "A failed attempt is
// offered again after a wait set by retry.mode".
//
// queues/events, queues/kv, queues/jobs__dlq and queues/nothing get 404 in
// plain text. jobs holds for an hour and retries after an hour. A publisher
// stores jobs/1 to jobs/4. Worker w1 receives jobs/1 and acknowledges it;
// w2 receives jobs/2, refuses it with PUBACK 0x80, and receives jobs/3,
// which it leaves unanswered. queues/jobs lists offsets 1 to 4, oldest
// first: jobs/1 held by w1, one attempt, held until an hour after its
// PUBACK; jobs/2 in backoff, one attempt; jobs/3 offered; jobs/4 ready, no
// attempt. No payload shows.
func TestOperationsQueueShowsUnfinishedJobs(t *testing.T) {
	queue := jobsQueue
	queue.VisibilityTimeout, queue.RetryDelay = time.Hour, time.Hour
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{eventsLog, queue, kvLatest}})
	for _, name := range []string{"events", "kv", "jobs__dlq", "nothing"} {
		path := "/v1/operations/queues/" + name
		if a := opsRequest(t, b, http.MethodGet, path, ""); a.Status != http.StatusNotFound || a.MediaType != "text/plain" {
			t.Errorf("GET %s: %d %s %q; want 404 in plain text, %s being no queue", path, a.Status, a.MediaType, a.Body, name)
		}
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	ps := make([]*harness.Publish, 4)
	for i := range ps {
		ps[i] = &harness.Publish{QoS: 1, Topic: fmt.Sprintf("jobs/%d", i+1), Payload: fmt.Appendf(nil, "payload-of-job-%d", i+1)}
	}
	support.PublishAll(t, pub, ps)
	worker := "$saguin/queue/" + queue.Name
	w1 := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "w1", CleanStart: true}, support.At(1, worker))
	offer := support.DeliveredAt(t, w1, 1, ps[0].Topic, ps[0].Payload)
	acked := time.Now()
	acknowledge(t, w1, offer.ID, 0)
	w2 := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "w2", CleanStart: true}, support.At(1, worker))
	offer = support.DeliveredAt(t, w2, 1, ps[1].Topic, ps[1].Payload)
	acknowledge(t, w2, offer.ID, 0x80)
	support.DeliveredAt(t, w2, 1, ps[2].Topic, ps[2].Payload)

	want := []string{"held", "backoff", "offered", "ready"}
	var jobs []harness.StateJob
	var body string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		body = opsRequest(t, b, http.MethodGet, "/v1/operations/queues/jobs", "").Body
		listed(t, stateView(t, b, "/v1/operations/queues/jobs"), harness.QueueRows, &jobs)
		states := make([]string, len(jobs))
		for i, j := range jobs {
			states[i] = j.State
		}
		return slices.Equal(states, want)
	}) {
		t.Fatalf("watchdog: after %v, queues/jobs reads %+v; want the states %q", harness.DefaultTimeout, jobs, want)
	}
	// jobs/3's attempts are left unchecked: its offer is an attempt not yet
	// spent, which the design does not say attempts counts.
	attempts := map[int]int{0: 1, 1: 1, 3: 0}
	for i, j := range jobs {
		if n, ok := attempts[i]; j.Offset != int64(i+1) || j.Topic != ps[i].Topic || (ok && j.Attempts != n) {
			t.Errorf("job %d: offset %d, topic %q, attempts %d; want offset %d, %q (attempts %d where checked)", i, j.Offset, j.Topic, j.Attempts, i+1, ps[i].Topic, n)
		}
	}
	if h := jobs[0].Holder; h == nil || *h != "w1" {
		t.Errorf("jobs/1's holder %v; want w1", h)
	}
	expectUTCSecond(t, "jobs/1's held_until", jobs[0].HeldUntil, acked.Add(queue.VisibilityTimeout), time.Now().Add(queue.VisibilityTimeout))
	for _, p := range ps {
		if strings.Contains(body, string(p.Payload)) {
			t.Errorf("queues/jobs shows the payload %q: %q; want never a payload", p.Payload, body)
		}
	}
}

// bridgesOf reads b's bridges, failing the test unless each row is counted.
func bridgesOf(t *testing.T, b *harness.Broker) []harness.StateBridge {
	t.Helper()
	var rows []harness.StateBridge
	if total, returned := listed(t, stateView(t, b, "/v1/operations/bridges"), harness.BridgesRows, &rows); total != len(rows) || returned != len(rows) {
		t.Fatalf("bridges: %d rows, total %d, returned %d; want each counted", len(rows), total, returned)
	}
	return rows
}

// design.md, Operations, HTTP: "bridges returns bridges, each with name,
// peer, connected, since, subscriptions_granted, subscriptions_pending,
// unanswered_subscribes, last_suback, sent and received. The last_suback is
// null until the first. The since is when the link last went up or down,
// or when the broker started for a link never up. The
// unanswered_subscribes counts the SUBSCRIBEs left unanswered since start
// ([Bridges, In](#bridges)). Disconnected bridges come first, then those with
// most pending, then bridges by name."; "Times are RFC 3339 in UTC, to the
// second." Bridges, In: "the bridge subscribes at the peer in one
// SUBSCRIBE carrying every inbound filter".
//
// Four bridges, each with an in rule: z-down, whose peer is down; y-pending,
// whose peer accepts its link and leaves its SUBSCRIBE unanswered; b-up,
// with an out rule out/# to peer/$# besides, and d-up, whose peers grant
// their SUBSCRIBEs. Once b-up has one record crossed out and one taken in,
// bridges lists z-down, y-pending, b-up, d-up: z-down not connected;
// y-pending connected, nothing granted, one pending, last_suback null;
// b-up and d-up connected, one granted, none pending, last_suback and since
// between their link's coming up and the read; none has a SUBSCRIBE left
// unanswered for 30 seconds; each names its peer; b-up's sent and received
// have each risen.
func TestOperationsBridgesShowsLinksInOrder(t *testing.T) {
	peers := map[string]*harness.Peer{"y-pending": harness.ListenedPeer(t), "b-up": harness.ListenedPeer(t), "d-up": harness.ListenedPeer(t)}
	down := "tcp://" + harness.DownPeer(t)
	bridges := []harness.Bridge{
		{Name: "z-down", Peer: down, ClientID: "bridge-z-down", Topics: []harness.BridgeRule{support.In("z/#", 1)}},
		support.BridgeTo("y-pending", peers["y-pending"], support.In("y/#", 1)),
		support.BridgeTo("b-up", peers["b-up"], support.In("b/#", 1), support.Out("out/#", "peer/$#", 1)),
		support.BridgeTo("d-up", peers["d-up"], support.In("d/#", 1)),
	}
	started := time.Now()
	b := support.OpsLaunch(t, &harness.Options{Bridges: bridges})
	pending := peers["y-pending"].Link(t)
	pending.Accept(t, support.Quiet)
	harness.Arrive[*harness.Subscribe](t, pending, harness.DefaultTimeout)
	up, _, _ := support.LinkUp(t, peers["b-up"], support.Quiet)
	support.LinkUp(t, peers["d-up"], support.Quiet)

	ordered := func(rows []harness.StateBridge) bool {
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = r.Name
		}
		return slices.Equal(names, []string{"z-down", "y-pending", "b-up", "d-up"}) &&
			rows[1].SubscriptionsPending == 1 && rows[2].SubscriptionsGranted == 1 && rows[3].SubscriptionsGranted == 1
	}
	var rows []harness.StateBridge
	if !testkit.Within(harness.DefaultTimeout, func() bool { rows = bridgesOf(t, b); return ordered(rows) }) {
		t.Fatalf("watchdog: after %v, bridges reads %+v; want z-down, y-pending with one pending, then b-up and d-up with one granted each", harness.DefaultTimeout, rows)
	}
	before := rows[2]
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "out/1", Payload: []byte("out")})
	support.PeerTakes(t, up, 1)
	support.Bridged(t, up, &harness.Publish{QoS: 1, ID: 1, Topic: "b/1", Payload: []byte("in")})
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		rows = bridgesOf(t, b)
		return ordered(rows) && rows[2].Sent > before.Sent && rows[2].Received > before.Received
	}) {
		t.Errorf("watchdog: after %v, b-up reads %+v; want sent and received above %+v's, a record having crossed each way", harness.DefaultTimeout, rows[2], before)
	}
	read := time.Now()
	for i, r := range rows {
		if r.Peer != bridges[i].Peer || r.UnansweredSubscribes != 0 {
			t.Errorf("bridge %s: peer %q, unanswered_subscribes %d; want %q and 0", r.Name, r.Peer, r.UnansweredSubscribes, bridges[i].Peer)
		}
	}
	if r := rows[0]; r.Connected {
		t.Errorf("z-down connected; want not, its peer down")
	}
	if r := rows[1]; !r.Connected || r.SubscriptionsGranted != 0 || r.LastSuback != nil {
		t.Errorf("y-pending: %+v; want connected, nothing granted, last_suback null", r)
	}
	for _, r := range rows[2:] {
		if !r.Connected || r.SubscriptionsPending != 0 {
			t.Errorf("%s: %+v; want connected, none pending", r.Name, r)
		}
		expectUTCSecond(t, r.Name+"'s since", r.Since, started, read)
		expectUTCSecond(t, r.Name+"'s last_suback", r.LastSuback, started, read)
	}
}

// design.md, Operations, HTTP: "A route that asks the core waits at most 5
// seconds for its answer, a store read included. Otherwise it returns 503
// {"status":"unavailable"}."; sessions, from Words: Session, "An away
// session lives in the store alone"; DISCONNECT, A clean DISCONNECT: "The
// client may change its session expiry [...] The change is stored before
// the connection closes." metrics.md, Process:
// saguin_operations_requests_total, code "503".
//
// The broker's store log is read from through a rig that makes each read
// of it wait 8 seconds once stalled (launchSlowed). away connects with a
// durable session; sessions answers 200. The rig is stalled, and away
// leaves with a DISCONNECT changing its Session Expiry, so the log holds
// what sessions reads next. GET sessions then answers 503
// {"status":"unavailable"} as JSON 5 seconds after it was sent, to within
// the scheduling margin; the rig notes a read it slowed once that read's
// wait is over; route sessions, code 503, rose by one.
func TestOperationsRouteAnswers503AfterFiveSeconds(t *testing.T) {
	const wait, slowRead = 5 * time.Second, 8 * time.Second
	s := launchSlowed(t, &harness.Options{}, "read", slowRead)
	c, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: "away", CleanStart: true, Props: support.Durable})
	sessionsOf(t, s.Broker)
	s.stall(t, true)
	if err := c.Send(&harness.Disconnect{Props: support.SessionExpiry(7200)}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)

	series := testkit.SeriesKey("saguin_operations_requests_total", "route", "sessions", "code", "503")
	before := requestCounts(t, s.Broker)[series]
	sent := time.Now()
	a, err := support.OpsTry(s.Broker, http.MethodGet, "/v1/operations/sessions", "")
	took := time.Since(sent)
	if err != nil {
		t.Fatalf("GET sessions, its store read stalled: %v after %v", err, took)
	}
	var got map[string]any
	if a.Status != http.StatusServiceUnavailable || a.MediaType != "application/json" ||
		json.Unmarshal([]byte(a.Body), &got) != nil || !maps.Equal(got, map[string]any{"status": "unavailable"}) {
		t.Errorf("GET sessions, its store read stalled: %d %s %q; want 503 {\"status\":\"unavailable\"}", a.Status, a.MediaType, a.Body)
	}
	support.Between(t, "GET sessions' answer, its store read stalled", took, wait, wait+support.Late)
	// The rig notes a slowed read once it is made, its delay over: within
	// one delay of the 503, the scheduling margin added.
	if !testkit.Within(slowRead+support.Margin, func() bool { return testkit.Injected(t, s.trace) }) {
		t.Errorf("the rig noted no slowed read of the log within %v of the 503; the test stalled nothing", slowRead+support.Margin)
	}
	if rose := requestCounts(t, s.Broker)[series] - before; rose != 1 {
		t.Errorf("%s rose by %v; want 1", series, rose)
	}
}

// design.md, Operations, HTTP: "The broker decides the route first [...]
// For a known route, the broker checks the credential, then the method. Any
// other method gets 405."; "A rejected credential gets 401"; "Otherwise it
// gets 403"; config "Any other value gets 400"; "queues/<channel> returns
// 404 for anything but a queue." metrics.md, Process:
// saguin_operations_requests_total{route,code}, "route is /health,
// /metrics, acl, config, bridges, consumers, sessions, users, queues,
// refused, position-lost or other; code is 200, 400, 401, 403, 404, 405 or
// 503".
//
// The broker has an operators' file holding root, and jobs, a queue. Each
// call below adds one to its route and code and to no other series, the
// test's own reads of /metrics aside: a GET
// of config, bridges, consumers, sessions, users and queues/jobs, 200 each;
// config?section=broker, config 400; queues/events, queues 404; POST
// sessions, sessions 405; users with root's wrong password, users 401;
// consumers with Host example.com and no credential, consumers 403.
func TestOperationsCountsStateRequestsByRouteAndCode(t *testing.T) {
	dir := t.TempDir()
	operators := usersFile(t, dir, "operators", "root")
	b := support.OpsLaunch(t, &harness.Options{Dir: dir, OpsPasswordFile: operators, Channels: []harness.Channel{eventsLog, jobsQueue}})
	type call struct {
		harness.OpsCall
		route string
		code  int
	}
	get := func(path, route string, code int) call {
		return call{harness.OpsCall{Method: http.MethodGet, Path: path}, route, code}
	}
	calls := []call{
		get("/v1/operations/config", "config", 200), get("/v1/operations/bridges", "bridges", 200),
		get("/v1/operations/consumers", "consumers", 200), get("/v1/operations/sessions", "sessions", 200),
		get("/v1/operations/users", "users", 200), get("/v1/operations/queues/jobs", "queues", 200),
		get("/v1/operations/config?section=broker", "config", 400), get("/v1/operations/queues/events", "queues", 404),
		{harness.OpsCall{Method: http.MethodPost, Path: "/v1/operations/sessions"}, "sessions", 405},
		{harness.OpsCall{Method: http.MethodGet, Path: "/v1/operations/users", User: "root", Password: "wrong"}, "users", 401},
		{harness.OpsCall{Method: http.MethodGet, Path: "/v1/operations/consumers", Host: "example.com"}, "consumers", 403},
	}
	// counted is saguin_operations_requests_total but for /metrics' 200s:
	// each read of the series is a GET of /metrics of its own.
	scrapes := testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "200")
	counted := func() map[string]float64 {
		got := requestCounts(t, b)
		delete(got, scrapes)
		return got
	}
	for _, c := range calls {
		before := counted()
		a := opsCall(t, support.PlainOps(b), &c.OpsCall)
		if a.Status != c.code {
			t.Errorf("%s %s: %d %q; want %d", c.Method, c.Path, a.Status, a.Body, c.code)
		}
		want := maps.Clone(before)
		key := testkit.SeriesKey("saguin_operations_requests_total", "route", c.route, "code", strconv.Itoa(c.code))
		want[key]++
		if got := counted(); !maps.Equal(got, want) {
			t.Errorf("%s %s: saguin_operations_requests_total reads %v; want %v, %s risen by one", c.Method, c.Path, got, want, key)
		}
	}
}

// design.md, Operations, HTTP: "Equal sort ranks use the complete row key
// in UTF-8 byte order. A consumer's key is reader, rule filter and rule
// topic."; "Other named rows use their complete name or client id."; "Times
// are RFC 3339 in UTC, to the second. Identity and sorting use uncut
// values."; consumers "Positions appear furthest behind first"; sessions
// "Away sessions come first, then sessions by client id"; bridges "The
// since is when the link last went up or down, or when the broker started
// for a link never up." config.md,
// bridges.<name>: outbound filters "may not overlap".
//
// Bridge edge has two out rules on events, events/b/# to x/b/$# then
// events/a/# to x/a/$#; nothing is published, so both are behind by 0.
// Two durable sessions share their first 1,024 bytes, q repeated, and end
// in b then a: the b one, with a Session Expiry of an hour, connects and
// leaves first, then the a one, with two hours. consumers lists events'
// positions edge's events/a/# rule, then its events/b/# rule. sessions
// lists the a one (expires_after 7200) before the b one (3600), both shown
// cut to the 1,024 bytes they share. bridges gives edge's since in RFC 3339
// in UTC to the second, between the launch and the read.
func TestOperationsBreaksTiesByTheCompleteKey(t *testing.T) {
	second, first := support.Out("events/b/#", "x/b/$#", 1), support.Out("events/a/#", "x/a/$#", 1)
	started := time.Now()
	b, _, _ := support.Outbound(t, &harness.Options{Operations: true, MaxClientIDLength: 2048, Channels: []harness.Channel{eventsLog}},
		"edge", support.Quiet, second, first)
	support.AwaitReady(t, b)
	shared := strings.Repeat("q", 1024)
	for _, s := range []struct {
		id     string
		expiry uint32
	}{{shared + "b", 3600}, {shared + "a", 7200}} {
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: s.id, CleanStart: true, Props: support.SessionExpiry(s.expiry)})
		leaves(t, c)
	}

	var positions []harness.StatePosition
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		positions = consumersOf(t, b)["events"].Positions
		return len(positions) == 2
	}) {
		t.Fatalf("watchdog: after %v, events' positions read %+v; want edge's two rules", harness.DefaultTimeout, positions)
	}
	for i, r := range []harness.BridgeRule{first, second} {
		if p := positions[i]; p.Reader != "bridge:edge" || p.Behind != 0 || !ruleNames(p.Rule, r.Filter, r.Topic) {
			t.Errorf("events' position %d: reader %s, behind %d, rule %s; want bridge:edge, behind 0, its rule %s to %s: rules by filter",
				i, p.Reader, p.Behind, p.Rule, r.Filter, r.Topic)
		}
	}
	s := awaitSessions(t, b, 0, 2)
	if len(s.rows) != 2 || s.rows[0].ClientID != shared || s.rows[1].ClientID != shared ||
		s.rows[0].ExpiresAfter != 7200 || s.rows[1].ExpiresAfter != 3600 {
		t.Errorf("sessions' rows %.60v; want the session ending in a (expires_after 7200), then the one ending in b (3600), each cut to its 1,024 bytes of q", s.rows)
	}
	rows := bridgesOf(t, b)
	read := time.Now()
	if len(rows) != 1 {
		t.Fatalf("bridges' rows %+v; want edge's alone", rows)
	}
	expectUTCSecond(t, "edge's since", rows[0].Since, started, read)
}
