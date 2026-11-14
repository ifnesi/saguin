package conformance

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The operator's explanations of the acl_file (item 0018c): design.md,
// Operations, `saguin --acl <config> <user> [client-id]`, its --json, and
// HTTP /v1/operations/acl. Both read one configuration, explainedACL's.

// explainedACL is the acl_file both explanations read. Roles are defined
// reader, sensor, camera in that order, and device-* lists sensor before
// reader, so file order differs from the entry's. reader's home/%u/#,
// sensor's channel filter events/%u/# and camera's cmd/%c are voided by a
// name holding +, # or / (design.md, Access and reload: "A value holding
// +, # or / makes that rule grant nothing"). device-* sets its own publish
// limits; the others have the broker's.
const explainedACL = `roles:
  reader:
    - topic: alerts/#
      allow: [read]
    - topic: home/%u/#
      allow: [write, read]
  sensor:
    - channel: events
      filter: events/%u/#
      allow: [write]
    - broker: features
      deny: [retained, qos2]
  camera:
    - topic: cmd/%c
      allow: [read]
users:
  "device-*":
    roles: [sensor, reader]
    limits: {publish_rate: 100, publish_bytes: 64KiB}
    client_ids: ["%u", "spare-*"]
  "*-7": [reader]
  admin: [reader]
  "cam-*": [camera]
`

// explainedRate is the broker's own publish_rate beside explainedACL.
const explainedRate = 50

// explainedOptions is explainedACL's broker: its channel events, and the
// broker's publish_rate explainedRate.
func explainedOptions(t *testing.T) *harness.Options {
	t.Helper()
	o := aclOptions(t, explainedACL, "device-7")
	o.Channels = []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}
	o.PublishRate = explainedRate
	return o
}

// aclGrant is one grant of an explanation: design.md, Operations, HTTP,
// "Each grant contains role, kind (channel, topic or broker), subject,
// verbs and denies, the last two lists, [] when empty".
type aclGrant struct {
	Role    string   `json:"role"`
	Kind    string   `json:"kind"`
	Subject string   `json:"subject"`
	Verbs   []string `json:"verbs"`
	Denies  []string `json:"denies"`
}

// aclLimit is one publish limit of an explanation: design.md, Operations,
// HTTP, "Each publish limit is {"value":…,"source":…}: the limit per
// second, null where there is none, and where it comes from, broker or
// users.<pattern>".
type aclLimit struct {
	Value  *int64 `json:"value"`
	Source string `json:"source"`
}

// String is l as an explanation writes it.
func (l aclLimit) String() string {
	value := "null"
	if l.Value != nil {
		value = strconv.FormatInt(*l.Value, 10)
	}
	return fmt.Sprintf(`{"value":%s,"source":%q}`, value, l.Source)
}

// aclExplanation is what /v1/operations/acl answers with an acl_file, and
// --acl --json prints: design.md, Operations, HTTP, "it contains user,
// acl_file, pattern_applied, patterns_matched, patterns_in_file, grants,
// grants_withheld, client_id_allowed, publish_rate and publish_bytes".
type aclExplanation struct {
	User            string     `json:"user"`
	ACLFile         string     `json:"acl_file"`
	PatternApplied  *string    `json:"pattern_applied"`
	PatternsMatched []string   `json:"patterns_matched"`
	PatternsInFile  []string   `json:"patterns_in_file"`
	Grants          []aclGrant `json:"grants"`
	GrantsWithheld  []aclGrant `json:"grants_withheld"`
	ClientIDAllowed *bool      `json:"client_id_allowed"`
	PublishRate     aclLimit   `json:"publish_rate"`
	PublishBytes    aclLimit   `json:"publish_bytes"`
}

// explanationCase is one question to the explanations and its answer,
// acl_file being explainedACL's path.
type explanationCase struct {
	name, user, client string // client "" for no client id asked
	want               aclExplanation
}

// The grants explainedACL gives, by rule, as their user's explanation
// lists them: a channel grant's subject names the channel; withheld rules
// are compared by role and kind.
var (
	alertsGrant   = aclGrant{Role: "reader", Kind: "topic", Subject: "alerts/#", Verbs: []string{"read"}}
	eventsGrant   = aclGrant{Role: "sensor", Kind: "channel", Subject: "events", Verbs: []string{"write"}}
	featuresGrant = aclGrant{Role: "sensor", Kind: "broker", Subject: "features", Denies: []string{"qos2", "retained"}}
	homeWithheld  = aclGrant{Role: "reader", Kind: "topic"}
	eventsHeld    = aclGrant{Role: "sensor", Kind: "channel"}
)

// The publish limits explainedACL's users are given: device-*'s own, and
// otherwise the broker's, publish_rate explainedRate and no publish_bytes.
var (
	deviceRate  = aclLimit{Value: new(int64(100)), Source: "users.device-*"}
	deviceBytes = aclLimit{Value: new(int64(64 << 10)), Source: "users.device-*"}
	brokerRate  = aclLimit{Value: new(int64(explainedRate)), Source: "broker"}
	brokerBytes = aclLimit{Source: "broker"}
)

// home is reader's home/%u/# grant for user.
func home(user string) aclGrant {
	return aclGrant{Role: "reader", Kind: "topic", Subject: "home/" + user + "/#", Verbs: []string{"read", "write"}}
}

// cmd is camera's cmd/%c grant for the client id client.
func cmd(client string) aclGrant {
	return aclGrant{Role: "camera", Kind: "topic", Subject: "cmd/" + client, Verbs: []string{"read"}}
}

// explanationCases are the questions both explanations are asked. design.md,
// Access and reload: "A name written without * beats every pattern. Among
// patterns matching a user name, the one with the most characters other
// than * wins"; "%u (the proved user name) and %c (the client id) are
// substituted in filters and topics. A value holding +, # or / makes that
// rule grant nothing"; client ids "one pattern or a list, with %u
// substituted"; Operations, HTTP: "All grants appear in file order. The
// grants_withheld field lists rules voided by a +, # or / in the name. The
// client_id_allowed field is null unless a client id was asked"; "Each
// publish limit is {"value":…,"source":…}: the limit per second, null where
// there is none, and where it comes from, broker or users.<pattern>";
// "Every list is sorted", in "UTF-8 byte order"; Operations, --acl: "With no
// client id, a rule holding %c is shown as written".
func explanationCases(file string) []explanationCase {
	inFile := []string{"*-7", "admin", "cam-*", "device-*"}
	// device7 is device-7's explanation, client_id_allowed allowed.
	device7 := func(allowed *bool) aclExplanation {
		return aclExplanation{User: "device-7", ACLFile: file, PatternApplied: new("device-*"),
			PatternsMatched: []string{"*-7", "device-*"}, PatternsInFile: inFile,
			Grants: []aclGrant{alertsGrant, home("device-7"), eventsGrant, featuresGrant}, ClientIDAllowed: allowed,
			PublishRate: deviceRate, PublishBytes: deviceBytes}
	}
	withheld := make([]explanationCase, 0, 3)
	for _, held := range []string{"/", "+", "#"} {
		user := "device-a" + held + "b"
		withheld = append(withheld, explanationCase{"a user name holding " + held, user, "", aclExplanation{User: user, ACLFile: file,
			PatternApplied: new("device-*"), PatternsMatched: []string{"device-*"}, PatternsInFile: inFile,
			Grants: []aclGrant{alertsGrant, featuresGrant}, GrantsWithheld: []aclGrant{homeWithheld, eventsHeld},
			PublishRate: deviceRate, PublishBytes: deviceBytes}})
	}
	return append([]explanationCase{
		{"device-* beats *-7, grants in file order", "device-7", "", device7(nil)},
		{"the client id %u", "device-7", "device-7", device7(new(true))},
		{"the client id spare-*", "device-7", "spare-3", device7(new(true))},
		{"a client id the entry refuses", "device-7", "other", device7(new(false))},
		{"*-7 alone", "ab-7", "", aclExplanation{User: "ab-7", ACLFile: file, PatternApplied: new("*-7"),
			PatternsMatched: []string{"*-7"}, PatternsInFile: inFile, Grants: []aclGrant{alertsGrant, home("ab-7")},
			PublishRate: brokerRate, PublishBytes: brokerBytes}},
		{"a name without *", "admin", "", aclExplanation{User: "admin", ACLFile: file, PatternApplied: new("admin"),
			PatternsMatched: []string{"admin"}, PatternsInFile: inFile, Grants: []aclGrant{alertsGrant, home("admin")},
			PublishRate: brokerRate, PublishBytes: brokerBytes}},
		{"%c substituted", "cam-1", "cam-1", aclExplanation{User: "cam-1", ACLFile: file, PatternApplied: new("cam-*"),
			PatternsMatched: []string{"cam-*"}, PatternsInFile: inFile,
			Grants: []aclGrant{cmd("cam-1")}, ClientIDAllowed: new(true), PublishRate: brokerRate, PublishBytes: brokerBytes}},
		{"%c as written, no client id asked", "cam-1", "", aclExplanation{User: "cam-1", ACLFile: file, PatternApplied: new("cam-*"),
			PatternsMatched: []string{"cam-*"}, PatternsInFile: inFile,
			Grants: []aclGrant{cmd("%c")}, PublishRate: brokerRate, PublishBytes: brokerBytes}},
		{"a client id holding /", "cam-1", "a/b", aclExplanation{User: "cam-1", ACLFile: file, PatternApplied: new("cam-*"),
			PatternsMatched: []string{"cam-*"}, PatternsInFile: inFile,
			GrantsWithheld: []aclGrant{{Role: "camera", Kind: "topic"}}, ClientIDAllowed: new(true),
			PublishRate: brokerRate, PublishBytes: brokerBytes}},
		{"a user matched by no entry", "nobody", "", aclExplanation{User: "nobody", ACLFile: file, PatternsInFile: inFile,
			PublishRate: brokerRate, PublishBytes: brokerBytes}},
	}, withheld...)
}

// checkExplanation fails the test unless body, an explanation, is a JSON
// object containing every field of harness.ACLFields and saying what want says: a
// channel grant's subject naming want's channel, every other subject as
// want's; verbs and denies as sets, each a list, [] when empty and never
// null; each publish limit's value and source; withheld rules by role and
// kind, in order.
func checkExplanation(t *testing.T, body []byte, want *aclExplanation) {
	t.Helper()
	if err := harness.CheckACLAnswer(body); err != nil {
		t.Fatalf("the explanation %s: %v", body, err)
	}
	var got aclExplanation
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("the explanation %s: %v", body, err)
	}
	if got.User != want.User || got.ACLFile != want.ACLFile {
		t.Errorf("user %q, acl_file %q; want %q, %q", got.User, got.ACLFile, want.User, want.ACLFile)
	}
	if show(got.PatternApplied) != show(want.PatternApplied) {
		t.Errorf("pattern_applied %s; want %s", show(got.PatternApplied), show(want.PatternApplied))
	}
	if !slices.Equal(got.PatternsMatched, want.PatternsMatched) {
		t.Errorf("patterns_matched %q; want %q, sorted", got.PatternsMatched, want.PatternsMatched)
	}
	if !slices.Equal(got.PatternsInFile, want.PatternsInFile) {
		t.Errorf("patterns_in_file %q; want %q, sorted", got.PatternsInFile, want.PatternsInFile)
	}
	if !samePointee(got.ClientIDAllowed, want.ClientIDAllowed) {
		t.Errorf("client_id_allowed %s; want %s", showBool(got.ClientIDAllowed), showBool(want.ClientIDAllowed))
	}
	for _, l := range []struct {
		field     string
		got, want aclLimit
	}{{"publish_rate", got.PublishRate, want.PublishRate}, {"publish_bytes", got.PublishBytes, want.PublishBytes}} {
		if l.got.Source != l.want.Source || !samePointee(l.got.Value, l.want.Value) {
			t.Errorf("%s %s; want %s", l.field, l.got, l.want)
		}
	}
	if len(got.Grants) != len(want.Grants) {
		t.Fatalf("grants %+v; want %+v, in file order", got.Grants, want.Grants)
	}

	for i, g := range got.Grants {
		w := want.Grants[i]
		subject := g.Subject == w.Subject || (w.Kind == "channel" && strings.Contains(g.Subject, w.Subject))
		if g.Role != w.Role || g.Kind != w.Kind || !subject || !sameSet(g.Verbs, w.Verbs) || !sameSet(g.Denies, w.Denies) {
			t.Errorf("grant %d is %+v; want %+v (grants in file order)", i, g, w)
		}
	}
	if len(got.GrantsWithheld) != len(want.GrantsWithheld) {
		t.Fatalf("grants_withheld %+v; want rules of %+v", got.GrantsWithheld, want.GrantsWithheld)
	}
	for i, g := range got.GrantsWithheld {
		if w := want.GrantsWithheld[i]; g.Role != w.Role || g.Kind != w.Kind {
			t.Errorf("withheld rule %d is %+v; want role %q, kind %q", i, g, w.Role, w.Kind)
		}
	}
}

// showBool is *p, or null.
func showBool(p *bool) string {
	if p == nil {
		return "null"
	}
	if *p {
		return "true"
	}
	return "false"
}

// samePointee reports whether a and b are both nil, or point to equal
// values.
func samePointee[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// sameSet reports whether a and b hold the same strings, whatever their
// order.
func sameSet(a, b []string) bool {
	a, b = slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b))
	return slices.Equal(a, b)
}

// everythingAllowed is the explanation without an acl_file: design.md,
// Operations, HTTP, "Without an acl_file, the answer is
// {"acl_file":null,"everything_allowed":true}".
var everythingAllowed = map[string]any{"acl_file": nil, "everything_allowed": true}

// checkEverythingAllowed fails the test unless body is the JSON object
// everythingAllowed.
func checkEverythingAllowed(t *testing.T, body []byte) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil || !maps.Equal(got, everythingAllowed) {
		t.Errorf("without an acl_file the explanation is %s (%v); want {\"acl_file\":null,\"everything_allowed\":true}", body, err)
	}
}

// aclCommand runs saguin --acl on config for user, with client where it is
// not "", and then the flags, to its end.
func aclCommand(t *testing.T, config, user, client string, flags ...string) (code int, stdout, stderr string) {
	t.Helper()
	args := []string{"--acl", config, user}
	if client != "" {
		args = append(args, client)
	}
	return execute(t, support.BrokerBinary, append(args, flags...)...)
}

// design.md, Operations: "saguin --acl <config> <user> [client-id] |
// explains, from the files on disk, what a client may do and which entry
// decided it: the user and the acl_file; the entry in force and every
// pattern matched; whether the client id is allowed, where one is given;
// the publish limits and where they come from; the withheld rules; the
// effective grants in file order, each with role, kind, subject, verbs and
// denies. With no client id, a rule holding %c is shown as written. With no
// acl_file, one line says everything is allowed"; "--json |
// with --acl or --route: one compact JSON object per line", whose fields
// are /v1/operations/acl's ("acl?user=<name>[&client_id=<id>] behaves as
// the command does").
//
// With no broker running, on explainedACL's configuration (the broker's
// publish_rate explainedRate):
//   - --json, each of explanationCases: exit 0, one compact JSON object on
//     one line, saying what the case wants;
//   - text, device-7: exit 0, nothing on standard error, naming the user,
//     the file, the entry device-* and the pattern *-7, the entry's limits
//     (publish_rate 100, publish_bytes 64KiB) and the grants' subjects in
//     file order (alerts/#, home/device-7/#, events, features); ab-7's
//     names the broker's publish_rate; device-7's with the client id other
//     names it; device-a/b's names its withheld rule home/…; cam-1's, no
//     client id given, names camera's rule as written, cmd/%c;
//   - with no acl_file: text, exit 0 and one line; --json the object
//     {"acl_file":null,"everything_allowed":true}.
//
// The text's wording is not the design's to say; what it names is.
func TestACLCommandExplains(t *testing.T) {
	o := explainedOptions(t)
	config, err := harness.WriteConfig(o)
	if err != nil {
		t.Fatalf("writing the configuration: %v", err)
	}
	for _, c := range explanationCases(o.ACLFile) {
		t.Run("--json, "+c.name, func(t *testing.T) {
			code, stdout, stderr := aclCommand(t, config, c.user, c.client, "--json")
			if code != 0 {
				t.Fatalf("exit %d, stderr %q; want 0", code, stderr)
			}
			checkExplanation(t, []byte(compactLine(t, "--acl --json", stdout)), &c.want)
		})
	}
	text := func(t *testing.T, user, client string) string {
		t.Helper()
		code, stdout, stderr := aclCommand(t, config, user, client)
		if code != 0 || stderr != "" {
			t.Fatalf("exit %d, stderr %q; want 0 and nothing", code, stderr)
		}
		return stdout
	}
	t.Run("text", func(t *testing.T) {
		out := text(t, "device-7", "")
		for _, name := range []string{"device-7", o.ACLFile, "device-*", "*-7", "100"} {
			if !strings.Contains(out, name) {
				t.Errorf("--acl device-7 does not name %q:\n%s", name, out)
			}
		}
		if !strings.Contains(out, "64KiB") && !strings.Contains(out, "65536") {
			t.Errorf("--acl device-7 does not name its entry's publish_bytes, 64KiB:\n%s", out)
		}
		at := 0
		for _, subject := range []string{"alerts/#", "home/device-7/#", "events", "features"} {
			i := strings.Index(out[at:], subject)
			if i < 0 {
				t.Errorf("--acl device-7 does not name %q after the grants before it in file order:\n%s", subject, out)
				break
			}
			at += i + len(subject)
		}
		if out := text(t, "ab-7", ""); !strings.Contains(out, "50") {
			t.Errorf("--acl ab-7, its entry without limits, does not name the broker's publish_rate, %d:\n%s", explainedRate, out)
		}
		if out := text(t, "device-7", "other"); !strings.Contains(out, "other") {
			t.Errorf("--acl device-7 other does not name the client id:\n%s", out)
		}
		if out := text(t, "device-a/b", ""); !strings.Contains(out, "home/") {
			t.Errorf("--acl device-a/b does not name its withheld rule home/%%u/#:\n%s", out)
		}
		if out := text(t, "cam-1", ""); !strings.Contains(out, "cmd/%c") {
			t.Errorf("--acl cam-1, no client id given, does not name its rule as written, cmd/%%c:\n%s", out)
		}
	})
	t.Run("without an acl_file", func(t *testing.T) {
		plain, err := harness.WriteConfig(&harness.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("writing the configuration: %v", err)
		}
		code, stdout, stderr := aclCommand(t, plain, "anyone", "")
		if code != 0 || strings.Count(stdout, "\n") != 1 || !strings.HasSuffix(stdout, "\n") {
			t.Errorf("--acl without an acl_file: exit %d, stdout %q, stderr %q; want 0 and one line", code, stdout, stderr)
		}
		code, stdout, stderr = aclCommand(t, plain, "anyone", "", "--json")
		if code != 0 {
			t.Fatalf("--acl --json without an acl_file: exit %d, stderr %q; want 0", code, stderr)
		}
		checkEverythingAllowed(t, []byte(compactLine(t, "--acl --json", stdout)))
	})
}

// aclPath is /v1/operations/acl asking about user, and client where it is
// not "".
func aclPath(user, client string) string {
	q := url.Values{"user": {user}}
	if client != "" {
		q.Set("client_id", client)
	}
	return "/v1/operations/acl?" + q.Encode()
}

// design.md, Operations, HTTP: "acl?user=<name>[&client_id=<id>] behaves as
// the command does (--acl). The user is required and must be at most 1,024
// bytes, or the request gets 400. Without an acl_file, the answer is
// {"acl_file":null,"everything_allowed":true}. Otherwise it contains user,
// acl_file, pattern_applied, patterns_matched, patterns_in_file, grants,
// grants_withheld, client_id_allowed, publish_rate and publish_bytes. Each
// grant contains role, kind (channel, topic or broker), subject, verbs and
// denies, the last two lists, [] when empty. Each publish limit is
// {"value":…,"source":…}: the limit per second, null where there is none,
// and where it comes from, broker or users.<pattern>. All grants appear
// in file order. The grants_withheld field lists rules voided by a +, # or
// / in the name. The client_id_allowed field is null unless a client id
// was asked"; "Answers are JSON [...] Errors are plain text".
//
// A broker on explainedACL, with an operations listener, answers each of
// explanationCases with 200, a JSON object saying what the case wants. A
// request without user gets 400, and one whose user is 1,025 bytes; one of
// 1,024 bytes gets 200. A broker without an acl_file answers 200
// {"acl_file":null,"everything_allowed":true}.
func TestACLOperationsRoute(t *testing.T) {
	o := explainedOptions(t)
	b := support.OpsLaunch(t, o)
	for _, c := range explanationCases(o.ACLFile) {
		t.Run(c.name, func(t *testing.T) {
			a := opsRequest(t, b, http.MethodGet, aclPath(c.user, c.client), "")
			if a.Status != http.StatusOK || a.MediaType != "application/json" {
				t.Fatalf("GET %s: %d %s %q; want 200 and JSON", aclPath(c.user, c.client), a.Status, a.MediaType, a.Body)
			}
			checkExplanation(t, []byte(a.Body), &c.want)
		})
	}
	t.Run("the user", func(t *testing.T) {
		for _, c := range []struct {
			name, path string
			status     int
		}{
			{"none", "/v1/operations/acl", http.StatusBadRequest},
			{"none, a client id asked", "/v1/operations/acl?client_id=device-7", http.StatusBadRequest},
			{"of 1,025 bytes", aclPath(strings.Repeat("u", 1025), ""), http.StatusBadRequest},
			{"of 1,024 bytes", aclPath(strings.Repeat("u", 1024), ""), http.StatusOK},
		} {
			if a := opsRequest(t, b, http.MethodGet, c.path, ""); a.Status != c.status {
				t.Errorf("a user %s: %d %q; want %d", c.name, a.Status, a.Body, c.status)
			}
		}
	})
	t.Run("without an acl_file", func(t *testing.T) {
		plain := support.OpsLaunch(t, &harness.Options{})
		a := opsRequest(t, plain, http.MethodGet, aclPath("anyone", ""), "")
		if a.Status != http.StatusOK || a.MediaType != "application/json" {
			t.Fatalf("GET %s: %d %s %q; want 200 and JSON", aclPath("anyone", ""), a.Status, a.MediaType, a.Body)
		}
		checkEverythingAllowed(t, []byte(a.Body))
	})
}
