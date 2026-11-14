package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
)

// The run's operations state views (design.md, Operations, HTTP; item
// 0020f). The run's random operations calls (operations.go) ask for them
// too, each answer checked as the others are. At each checkpoint, after
// those calls, the run reads every view with no credential over the plain
// listener and checks it (checkViews): against the model where it knows
// the answer exactly (the configuration, the run's users and listeners,
// the bridge, the offsets the record checks have read, each connected
// slot's protocol) and, for every view, against the design's rules for
// lists: their order, their counts and the 100-row bound.

// stateRoutes are the state views' routes; queuesRoute takes a channel
// after it ("queues/<channel>").
var stateRoutes = []string{"/v1/operations/config", "/v1/operations/users", "/v1/operations/sessions",
	"/v1/operations/consumers", "/v1/operations/bridges"}

const queuesRoute = "/v1/operations/queues/"

// statePaths are the state views' paths the run's operations calls ask
// for: each route, config's sections taken and refused, and queues/ of the
// run's queue and of an append channel.
var statePaths = []string{"/v1/operations/config", "/v1/operations/config?section=channels&section=limits",
	"/v1/operations/config?section=broker", "/v1/operations/users", "/v1/operations/sessions",
	"/v1/operations/consumers", "/v1/operations/bridges", queuesRoute + queueName, queuesRoute + "ca"}

// configSections are what config's ?section= takes: "The repeatable
// ?section= parameter takes bridges, channels, limits, listeners,
// operations or storage. Any other value gets 400."
var configSections = []string{"bridges", "channels", "limits", "listeners", "operations", "storage"}

// stateRoute reports whether route, a path without its query, is a state
// view's.
func stateRoute(route string) bool {
	return slices.Contains(stateRoutes, route) || strings.HasPrefix(route, queuesRoute)
}

// stateAnswer is the status a GET of path, a state view's, gets once its
// credential holds: 400 for a section config does not take; 404 for
// queues/ of anything but a queue ("queues/<channel> returns 404 for
// anything but a queue"); otherwise 200.
func stateAnswer(path string) int {
	route, query, _ := strings.Cut(path, "?")
	if channel, ok := strings.CutPrefix(route, queuesRoute); ok {
		if i := slices.IndexFunc(channels, func(c channelConfig) bool { return c.name == channel }); i < 0 || channels[i].queue == nil {
			return http.StatusNotFound
		}
		return http.StatusOK
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return http.StatusBadRequest
	}
	for _, s := range values["section"] {
		if !slices.Contains(configSections, s) {
			return http.StatusBadRequest
		}
	}
	return http.StatusOK
}

// stateGet reads the view at path over o's plain listener, with no
// credential, and returns its answer's fields and its body, or an error
// unless it is 200 and a JSON object ("Answers are JSON"; "Each answer
// under /v1/operations/ is an object").
func (o *operations) stateGet(path string) (view map[string]json.RawMessage, body string, err error) {
	a, err := o.targets[opsPlain].Do(&harness.OpsCall{Method: http.MethodGet, Path: path})
	if err != nil {
		return nil, "", fmt.Errorf("view %s: %w", path, err)
	}
	if a.Status != http.StatusOK || a.MediaType != "application/json" || json.Unmarshal([]byte(a.Body), &view) != nil || view == nil {
		return nil, "", fmt.Errorf("view %s: %d %s %q; want 200 and a JSON object", path, a.Status, a.MediaType, a.Body)
	}
	return view, a.Body, nil
}

// stateRows is stateGet and harness.StateRows: path's rows, read over o,
// checked to be counted ("Every list is sorted and comes with total and
// returned"; "A state list holds at most 100 rows"), and the answer's
// fields.
func stateRows[T any](o *operations, path, key string) (rows []T, view map[string]json.RawMessage, err error) {
	view, _, err = o.stateGet(path)
	if err != nil {
		return nil, nil, err
	}
	total, returned, err := harness.StateRows(view, key, &rows)
	if err != nil {
		return nil, nil, fmt.Errorf("view %s: %w", path, err)
	}
	if returned != len(rows) || returned != min(total, 100) {
		return nil, nil, fmt.Errorf("view %s: %d rows, total %d, returned %d; want them all counted, at most 100 returned", path, len(rows), total, returned)
	}
	return rows, view, nil
}

// checkViews reads and checks each state view (the comment above).
func (f *fuzzer) checkViews() error {
	return errors.Join(f.checkConfigView(), f.checkUsersView(), f.checkSessionsView(), f.checkConsumersView(),
		f.checkQueueView(), f.checkBridgesView())
}

// checkConfigView checks config: its channels keyed by name, the run's
// configured channels, the dead-letter channel left out, each configured
// in the run's configuration; the run's password file shown as its path,
// and no hash ("config returns the resolved configuration, with channels
// keyed by name. Each channel has configured_in, which names its file";
// "Dead-letter channels are left out").
func (f *fuzzer) checkConfigView() error {
	view, body, err := f.ops.stateGet("/v1/operations/config")
	if err != nil {
		return err
	}
	var shown map[string]struct {
		ConfiguredIn string `json:"configured_in"`
	}
	if err := harness.StateField(view, "channels", &shown); err != nil {
		return fmt.Errorf("view config: %w", err)
	}
	want := make([]string, 0, len(channels))
	for _, c := range channels {
		if c.deadLetter == "" {
			want = append(want, c.name)
		}
	}
	slices.Sort(want)
	if got := slices.Sorted(maps.Keys(shown)); !slices.Equal(got, want) {
		return fmt.Errorf("view config: channels %q; want %q", got, want)
	}
	for name, c := range shown {
		if c.ConfiguredIn != f.ops.config {
			return fmt.Errorf("view config: channel %s configured_in %q; want %q", name, c.ConfiguredIn, f.ops.config)
		}
	}
	var all any
	if err := json.Unmarshal([]byte(body), &all); err != nil {
		return fmt.Errorf("view config: %w", err)
	}
	passwords := filepath.Join(filepath.Dir(f.ops.config), passwordFileName)
	if got := harness.ValuesOf(all, "password_file"); !slices.Contains(got, any(passwords)) {
		return fmt.Errorf("view config: password_file values %q; want the path %q among them", got, passwords)
	}
	return noHash("config", body)
}

// noHash returns an error where body, view's, holds a password hash: a
// $7$ or $6$ one (config.md, broker.mqtt, password_file).
func noHash(view, body string) error {
	for _, scheme := range []string{"$7$", "$6$"} {
		if strings.Contains(body, scheme) {
			return fmt.Errorf("view %s shows a password hash, %s: %q", view, scheme, body)
		}
	}
	return nil
}

// runListeners are the run's MQTT listeners by name (options): tcp1 and
// tcp2, the unix listener, named by its kind, and ws1 and ws2.
var runListeners = []string{"tcp1", "tcp2", "unix", "ws1", "ws2"}

// checkUsersView checks users: the run's users, sorted, anonymous clients
// refused, and each of the run's listeners with the same users, no client
// certificate asked; never a hash, an operator or the operators' file
// ("The users are the sorted names in the broker's password file. The
// listeners are keyed by listener name, each with users,
// anonymous_allowed, certificate (required, accepted or none) and
// proxy_identity. The answer never includes a hash or the operators'
// file").
func (f *fuzzer) checkUsersView() error {
	view, body, err := f.ops.stateGet("/v1/operations/users")
	if err != nil {
		return err
	}
	want := make([]string, len(logins))
	for i, l := range logins {
		want[i] = l.user
	}
	slices.Sort(want)
	var users []string
	var anonymous bool
	var listeners map[string]struct {
		Users     []string `json:"users"`
		Anonymous bool     `json:"anonymous_allowed"`
		Cert      string   `json:"certificate"`
	}
	if err := errors.Join(harness.StateField(view, "users", &users), harness.StateField(view, "anonymous_allowed", &anonymous),
		harness.StateField(view, "listeners", &listeners)); err != nil {
		return fmt.Errorf("view users: %w", err)
	}
	if !slices.Equal(users, want) || anonymous {
		return fmt.Errorf("view users: users %q, anonymous_allowed %v; want %q and false", users, anonymous, want)
	}
	if got := slices.Sorted(maps.Keys(listeners)); !slices.Equal(got, runListeners) {
		return fmt.Errorf("view users: listeners %q; want %q", got, runListeners)
	}
	for name, l := range listeners {
		if !slices.Equal(l.Users, want) || l.Anonymous || l.Cert != "none" {
			return fmt.Errorf("view users: listener %s %+v; want users %q, anonymous refused, certificate none", name, l, want)
		}
	}
	secrets := []string{filepath.Join(filepath.Dir(f.ops.config), operatorsFileName)}
	for _, o := range operators {
		secrets = append(secrets, o.user)
	}
	for _, secret := range secrets {
		if strings.Contains(body, secret) {
			return fmt.Errorf("view users shows %q: %q; want never the operators' file", secret, body)
		}
	}
	return noHash("users", body)
}

// checkSessionsView checks sessions: counted, away sessions first, then by
// client id, and each connected slot's row, where shown, in its
// connection's protocol ("The answer includes total, connected and away.
// Away sessions come first, then sessions by client id"; "protocol (3.1.1
// or 5)").
func (f *fuzzer) checkSessionsView() error {
	rows, view, err := stateRows[harness.StateSession](f.ops, "/v1/operations/sessions", harness.SessionsRows)
	if err != nil {
		return err
	}
	var total, connected, away int
	if err := errors.Join(harness.StateField(view, "total", &total), harness.StateField(view, "connected", &connected),
		harness.StateField(view, "away", &away)); err != nil {
		return fmt.Errorf("view sessions: %w", err)
	}
	if total != connected+away {
		return fmt.Errorf("view sessions: total %d, connected %d, away %d; want the total both", total, connected, away)
	}
	if !slices.IsSortedFunc(rows, func(a, b harness.StateSession) int {
		switch {
		case a.Connected == b.Connected:
		case a.Connected:
			return 1 // away sessions come first
		default:
			return -1
		}
		return strings.Compare(a.ClientID, b.ClientID)
	}) {
		return fmt.Errorf("view sessions: rows %+v; want away first, then by client id", rows)
	}
	for _, s := range f.slots {
		if s.conn == nil || s.clientID == "" {
			continue
		}
		i := slices.IndexFunc(rows, func(r harness.StateSession) bool { return r.Connected && r.ClientID == s.clientID })
		if want := protocolNames[s.conn.Version]; i >= 0 && rows[i].Protocol != want {
			return fmt.Errorf("view sessions: %s's protocol %q; want %q, its connection's", s.clientID, rows[i].Protocol, want)
		}
	}
	return nil
}

// protocolNames are sessions' protocols by MQTT version: "protocol (3.1.1
// or 5)".
var protocolNames = map[byte]string{harness.V311: "3.1.1", harness.V5: "5"}

// checkConsumersView checks consumers: an entry by name for $broadcast and
// each of the run's append channels, its queue and its dead-letter channel,
// none for its latest channel; each
// entry's positions in order (harness.PositionsInOrder) and counted; and
// each channel's next offset beyond every offset the record checks have
// read ("consumers returns channels, one entry per append or queue
// channel, by name";
// "The consumers counts the positions on the channel"; Channels:
// "$broadcast is the reserved append channel"; a queue's retention "moves
// the job to the append channel <channel>__dlq"; Words: Retention, "The
// tail is the next offset").
func (f *fuzzer) checkConsumersView() error {
	entries, _, err := stateRows[harness.StateConsumers](f.ops, "/v1/operations/consumers", harness.ConsumersRows)
	if err != nil {
		return err
	}
	byName := make(map[string]harness.StateConsumers, len(entries))
	names := make([]string, len(entries))
	for i, e := range entries {
		byName[e.Channel], names[i] = e, e.Channel
	}
	if !slices.IsSorted(names) {
		return fmt.Errorf("view consumers: entries %q; want them by name", names)
	}
	if _, ok := byName["$broadcast"]; !ok {
		return fmt.Errorf("view consumers: entries %q; want one for $broadcast, the reserved append channel", names)
	}
	for _, c := range channels {
		e, ok := byName[c.name]
		switch {
		case c.latest && ok:
			return fmt.Errorf("view consumers: an entry for %s, a latest channel; want one per append or queue channel", c.name)
		case c.latest:
			continue
		case !ok:
			return fmt.Errorf("view consumers: no entry for %s; want one per append or queue channel", c.name)
		}
		if err := harness.PositionsInOrder(e.Positions, e.NextOffset); err != nil {
			return fmt.Errorf("view consumers: %s: %w", c.name, err)
		}
		if e.Consumers < len(e.Positions) {
			return fmt.Errorf("view consumers: %s counts %d consumers for %d positions; want each position counted", c.name, e.Consumers, len(e.Positions))
		}
	}
	if f.db == nil {
		return nil // no record read yet
	}
	ids, err := f.channelNames()
	if err != nil {
		return err
	}
	for id, name := range ids {
		if e, ok := byName[name]; ok && e.NextOffset <= f.read[id] {
			return fmt.Errorf("view consumers: %s's next_offset %d; want it beyond offset %d, read from the store", name, e.NextOffset, f.read[id])
		}
	}
	return nil
}

// checkQueueView checks queues/ of the run's queue: its unfinished jobs
// oldest first, each in one of the design's states ("It returns the
// queue's unfinished jobs as records, oldest first. Each has offset, topic,
// attempts and state (ready, backoff, offered or held)").
func (f *fuzzer) checkQueueView() error {
	jobs, _, err := stateRows[harness.StateJob](f.ops, queuesRoute+queueName, harness.QueueRows)
	if err != nil {
		return err
	}
	for i, j := range jobs {
		if !slices.Contains(harness.JobStates, j.State) || (i > 0 && jobs[i-1].Offset >= j.Offset) {
			return fmt.Errorf("view queues/%s: jobs %+v; want them oldest first, each in a state of %q", queueName, jobs, harness.JobStates)
		}
	}
	return nil
}

// checkBridgesView checks bridges: the run's one bridge, by its name and
// peer ("bridges returns bridges, each with name, peer, connected, since,
// [...]").
func (f *fuzzer) checkBridgesView() error {
	rows, _, err := stateRows[harness.StateBridge](f.ops, "/v1/operations/bridges", harness.BridgesRows)
	if err != nil {
		return err
	}
	if f.bridge == nil {
		return nil
	}
	if len(rows) != 1 || rows[0].Name != bridgeName || rows[0].Peer != f.bridge.peer.URL() {
		return fmt.Errorf("view bridges: %+v; want the run's bridge %s to %s alone", rows, bridgeName, f.bridge.peer.URL())
	}
	return nil
}
