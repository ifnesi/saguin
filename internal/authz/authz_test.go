package authz_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/channel"
)

// The channels every case here is written against: one of each type, which
// is what makes the verb table testable at all.
func registry(t *testing.T) *channel.Registry {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append},
		{Name: "state", Type: channel.Latest},
		{Name: "jobs", Type: channel.Queue, VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

func load(t *testing.T, body string) (*authz.File, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return authz.Load(path, registry(t), 0)
}

// The file RFC 0002 prints, which has to load.
func TestTheFileTheSpecificationPrintsLoads(t *testing.T) {
	f, err := load(t, `
roles:
  telemetry-publisher:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
  job-worker:
    - channel: jobs
      allow: [consume]
  reader:
    - channel: events
      allow: [read, seek]
    - topic: alerts/#
      allow: [read]

users:
  "vessel-*": [telemetry-publisher, job-worker]
  dashboard:  [reader]
`)
	if err != nil {
		t.Fatalf("the file RFC 0002 prints was refused: %v", err)
	}
	if len(f.Roles) != 3 || len(f.Users) != 2 {
		t.Errorf("read %d roles and %d clients, want 3 and 2", len(f.Roles), len(f.Users))
	}
}

// **The refusal that keeps the two rule kinds apart.** Invariant 12's shape
// one level up: refuse the ambiguity while the operator is looking, rather
// than resolving it consistently until something changes underneath.
func TestATopicRuleInsideAChannelIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		topic string
	}{
		{"the channel itself", "events/#"},
		{"below it", "events/telemetry/+"},
		{"an exact topic in it", "jobs/build"},
		// The literal prefix is what decides, and a substitution sits below
		// it: `events/%u/#` is inside `events` whoever the client is.
		{"a substitution below a channel name", "events/%u/#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, "roles:\n  r:\n    - topic: "+tc.topic+
				"\n      allow: [read]\nusers:\n  c: [r]\n")
			if err == nil {
				t.Fatalf("a topic rule for %q was accepted: it governs a channel's topics "+
					"through the rule kind that governs broadcast, which is the overlap "+
					"invariant 12 refuses one level down", tc.topic)
			}
			if !strings.Contains(err.Error(), "Write it as a channel rule") {
				t.Errorf("the error does not say what to do instead: %v", err)
			}
		})
	}
}

// The other half of the partition: a filter whose wildcard sits at or above
// channel depth reaches broadcast only (invariant 11), so it is exactly
// what a topic rule is for and must not be refused.
func TestATopicRuleThatReachesBroadcastIsAccepted(t *testing.T) {
	// Quoted, because YAML reads an unquoted # as a comment - which is the
	// trap the "names neither" finding names, and the reason `#` is here.
	for _, topic := range []string{`"alerts/#"`, `"+/telemetry"`, `"#"`, "house/lamp",
		`"alerts/%u/#"`} {
		if _, err := load(t, "roles:\n  r:\n    - topic: "+topic+
			"\n      allow: [read]\nusers:\n  c: [r]\n"); err != nil {
			t.Errorf("topic rule %q was refused: it reaches no channel, which is what a "+
				"topic rule governs: %v", topic, err)
		}
	}
}

// A verb belongs to a channel type. "consume" on an append channel is not a
// narrower grant, it is a sentence about a thing that channel does not do.
//
// **The retired vocabulary is in the table on purpose.** `publish`, `set`,
// `get` and `subscribe` were the verbs until the day `write` and `read`
// replaced them, and `publish`/`subscribe` are what anyone arriving from
// another broker will type first. Each has to be refused by name rather
// than half-understood, because a verb saguin does not recognise is a grant
// that is not there and nothing else would ever say so.
func TestAVerbBelongsToItsChannelType(t *testing.T) {
	for _, tc := range []struct{ name, rule, want string }{
		{"consume on an append channel", "channel: events\n      allow: [consume]", "write, read, seek"},
		{"seek on a latest channel", "channel: state\n      allow: [seek]", "write, read, delete"},
		{"delete on an append channel", "channel: events\n      allow: [delete]", "write, read, seek"},
		{"a channel verb on a topic", "topic: alerts/#\n      allow: [seek]", "write, read"},
		{"the retired publish, on a channel", "channel: events\n      allow: [publish]", "write, read, seek"},
		{"the retired subscribe, on a topic", "topic: alerts/#\n      allow: [subscribe]", "write, read"},
		{"the retired set and get, on a latest channel", "channel: state\n      allow: [set, get]", "write, read, delete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, "roles:\n  r:\n    - "+tc.rule+"\nusers:\n  c: [r]\n")
			if err == nil {
				t.Fatal("a verb the rule kind does not take was accepted, which is a grant " +
					"that is not there and nothing else would ever say so")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not name the verbs it does take (%s): %v", tc.want, err)
			}
		})
	}
}

// `read` on a queue is the one wrong verb somebody writes on purpose, so
// the refusal has to teach rather than list.
//
// Every other store in the world is read, so an operator wanting a client
// that *watches* the queue reaches for it. Told only that it is not one of
// `write, consume`, they take the nearer of the two - and `consume` grants
// the taking of work to a thing that was never meant to do any: every job
// it takes stalls, redelivers, exhausts its attempts and is dead-lettered,
// with the broker reporting success throughout.
//
// So the assertion is as much about what the finding must *not* be. The
// generic "which is not one of" wording would pass a test that only checked
// for a refusal, and it is precisely the wording that causes the damage.
func TestReadOnAQueueSaysWhyRatherThanListingTheVerbs(t *testing.T) {
	_, err := load(t, "roles:\n  r:\n    - channel: jobs\n      allow: [read]\nusers:\n  c: [r]\n")
	if err == nil {
		t.Fatal("read on a queue was accepted: a client granted it takes work rather " +
			"than watching it, which is invariant 11's failure reached through the ACL")
	}
	got := err.Error()
	if strings.Contains(got, "which is not one of") {
		t.Errorf("the refusal only lists the verbs a queue takes, which sends an operator "+
			"who meant to watch straight to consume - the one grant that drains the "+
			"queue: %v", err)
	}
	for _, want := range []string{
		"consumed rather than read", // what the verb means here
		"no other worker sees it",   // what granting it costs everybody else
		"no verb for watching",      // that the thing they wanted does not exist
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the refusal does not say %q, so it does not explain why: %v", want, err)
		}
	}
}

// The verb table in RFC 0002 is a claim about this package, and a claim
// about behaviour is a thing to run rather than a thing to write once. The
// commonest defect this project finds is a sentence in prose that the code
// has quietly stopped agreeing with, and the sentence is believed by the
// next person to read it, which is what a specification is for.
//
// So the table is read out of the document and both directions are asked of
// the broker: every verb a row names must be accepted, and the list the
// broker names back in a refusal must be that row exactly.
//
// **The refusal is what closes the second direction.** Asking only whether
// the document's verbs are accepted would miss a verb the code takes and
// the table never mentions - the table would be short and every assertion
// would still pass. The refusal prints the code's own list verbatim, so
// comparing against it catches drift either way.
func TestTheVerbTableInRFC0002IsWhatTheCodeEnforces(t *testing.T) {
	rows := verbTableFromRFC(t)
	if len(rows) != 4 {
		t.Fatalf("read %d rows from RFC 0002's verb table, want one per channel type "+
			"and one for broadcast - a check reporting on part of the table is "+
			"reporting on work it did not do", len(rows))
	}
	for _, row := range rows {
		t.Run(row.kind, func(t *testing.T) {
			for _, v := range row.verbs {
				if _, err := load(t, roleAllowing(row.rule, v)); err != nil {
					t.Errorf("RFC 0002 says a %s rule takes %q, and the broker refuses "+
						"it: %v", row.kind, v, err)
				}
			}
			_, err := load(t, roleAllowing(row.rule, "not-a-verb"))
			if err == nil {
				t.Fatal("a verb that is not a verb was accepted, so this row proves nothing")
			}
			want := strings.Join(row.verbs, ", ")
			if got := verbsNamedBy(err); got != want {
				t.Errorf("RFC 0002 says a %s rule takes %q; the broker answers %q",
					row.kind, want, got)
			}
		})
	}
}

// verbRow is one line of that table: what the document calls the rule kind,
// a rule of that kind written against the channels registry gives, and the
// verbs the document says it takes.
type verbRow struct {
	kind, rule string
	verbs      []string
}

// verbTableFromRFC reads the table under "#### The verbs".
//
// **It fails rather than returning nothing** when the heading has moved,
// because a parser that finds no rows and reports success is the shape this
// whole check exists to catch: a green tick over a rule it never applied.
func verbTableFromRFC(t *testing.T) []verbRow {
	t.Helper()
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs",
		"0002-channels-and-configuration.md"))
	if err != nil {
		t.Fatalf("RFC 0002: %v", err)
	}
	body := string(md)
	at := strings.Index(body, "#### The verbs")
	if at < 0 {
		t.Fatal(`RFC 0002 no longer has a "#### The verbs" heading, so there is no ` +
			`table to hold the code to and this check would pass by finding nothing`)
	}
	// Where a rule of each kind is written. The registry these tests use has
	// one channel of each type, which is what makes the table testable.
	rules := map[string]string{
		"append": "channel: events",
		"latest": "channel: state",
		"queue":  "channel: jobs",
		"topic":  "topic: alerts/#",
	}
	var rows []verbRow
	for _, line := range strings.Split(body[at:], "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			if len(rows) > 0 {
				break // past the table
			}
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 2 {
			continue
		}
		kind := strings.TrimSpace(strings.ReplaceAll(cells[0], "`", ""))
		kind = strings.TrimSuffix(strings.Fields(kind + " x")[0], ":")
		rule, ok := rules[kind]
		if !ok {
			continue // the header row, the separator, or a table that grew a column
		}
		// A note such as "(submit work)" is prose inside the cell.
		verbs := regexp.MustCompile(`\([^)]*\)`).ReplaceAllString(cells[1], "")
		var got []string
		for _, v := range strings.Split(verbs, ",") {
			if v = strings.TrimSpace(strings.ReplaceAll(v, "`", "")); v != "" {
				got = append(got, v)
			}
		}
		rows = append(rows, verbRow{kind: kind, rule: rule, verbs: got})
	}
	return rows
}

// roleAllowing is a file whose single rule allows one verb.
func roleAllowing(rule, verb string) string {
	return "roles:\n  r:\n    - " + rule + "\n      allow: [" + verb +
		"]\nusers:\n  c: [r]\n"
}

// verbsNamedBy is the list a refusal names back, which is the code's own.
func verbsNamedBy(err error) string {
	const marker = "which is not one of "
	at := strings.Index(err.Error(), marker)
	if at < 0 {
		return ""
	}
	return strings.TrimSpace(err.Error()[at+len(marker):])
}

func TestWhatCannotMeanWhatItSays(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{
			"a rule naming both kinds",
			"roles:\n  r:\n    - channel: events\n      topic: alerts/#\n      allow: [write]\nusers:\n  c: [r]\n",
			"one or the other",
		},
		{
			"a rule naming neither",
			"roles:\n  r:\n    - allow: [write]\nusers:\n  c: [r]\n",
			"nothing for it to govern",
		},
		{
			"a channel that is not configured",
			"roles:\n  r:\n    - channel: nowhere\n      allow: [write]\nusers:\n  c: [r]\n",
			"not configured",
		},
		{
			"a role nobody defined",
			"roles:\n  r:\n    - channel: events\n      allow: [write]\nusers:\n  c: [ghost]\n",
			"does not define",
		},
		{
			"a role with no rules",
			"roles:\n  r: []\nusers:\n  c: [r]\n",
			"carries no rules",
		},
		{
			"a client with no roles",
			"roles:\n  r:\n    - channel: events\n      allow: [write]\nusers:\n  c: []\n",
			"given no roles",
		},
		{
			"%u in a channel name",
			"roles:\n  r:\n    - channel: dev-%u\n      allow: [write]\nusers:\n  c: [r]\n",
			"never in a channel name",
		},
		{
			"a filter on a topic rule",
			"roles:\n  r:\n    - topic: alerts/#\n      filter: x/#\n      allow: [read]\nusers:\n  c: [r]\n",
			"A topic rule's filter is its topic: line",
		},
		{
			"an empty file",
			"roles: {}\nusers: {}\n",
			"grants nothing to nobody",
		},
		{
			"a key saguin does not know",
			"roles:\n  r:\n    - channel: events\n      allowed: [publish]\nusers:\n  c: [r]\n",
			"field allowed not found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.body)
			if err == nil {
				t.Fatal("accepted a file that cannot mean what it says")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not say what is wrong (want %q): %v", tc.want, err)
			}
		})
	}
}

// Every finding at once. An operator fixing this file one error per run
// edits it blind, and the file is read at startup, so every trip round is a
// restart.
func TestEveryProblemIsReportedAtOnce(t *testing.T) {
	_, err := load(t, "roles:\n  r:\n    - channel: nowhere\n      allow: [write]\n"+
		"    - channel: events\n      allow: [consume]\n"+
		"    - topic: events/#\n      allow: [read]\nusers:\n  c: [r, ghost]\n")
	if err == nil {
		t.Fatal("a file with four problems was accepted")
	}
	for _, want := range []string{"not configured", "write, read, seek",
		"Write it as a channel rule", "does not define"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error stops before %q, so an operator fixes one per restart: %v",
				want, err)
		}
	}
}

// **One entry applies, and within it the roles union.** Those are two rules
// working at two levels and the difference is the whole of this change: how
// exactly a pattern spells the id out decides which *entry* supplies the
// roles, and nothing decides between the rules inside it, because a rule
// only grants and two of them cannot disagree.
//
// The case that shows it is `vessel-7`, which matches the fleet pattern and
// its own line. Under the union this replaced it held `worker` from the
// fleet; now its own entry is the whole of what it gets, and the fleet's
// second role is not part of it. That is the subtraction precedence buys,
// and it is also the way it can surprise - which is why `--acl` names the
// entry a client lost to.
func TestTheMostExactEntrySuppliesTheRoles(t *testing.T) {
	f := mustLoad(t, `
roles:
  publisher:
    - channel: events
      allow: [write]
  worker:
    - channel: jobs
      allow: [consume]
users:
  "vessel-*": [publisher, worker]
  vessel-7:   [publisher]
`)

	// vessel-7's own entry wins whole: publisher, and not the fleet's worker.
	g := f.For("vessel-7", "")
	if !authz.AllowsChannel(g, "events", "events/a", "write") {
		t.Error("vessel-7 may not publish to events, which its own entry grants")
	}
	if authz.AllowsChannel(g, "jobs", "jobs/build", "consume") {
		t.Error("vessel-7 may consume from jobs, which only the fleet entry grants: " +
			"the entry that spells the id out most exactly supplies the roles, and a " +
			"broader one that also matches does not add to them")
	}
	if authz.AllowsChannel(g, "events", "events/a", "read") {
		t.Error("vessel-7 may read events, which nothing granted")
	}

	// And a fleet member with no line of its own takes the fleet's, both
	// roles of it - which is the union, one level down.
	fleet := f.For("vessel-9", "")
	if !authz.AllowsChannel(fleet, "events", "events/a", "write") ||
		!authz.AllowsChannel(fleet, "jobs", "jobs/build", "consume") {
		t.Error("vessel-9 does not hold both roles of the one entry that matches it: " +
			"roles union within an entry, which is what grant-only means")
	}

	// A client matching nothing holds nothing: default refuse.
	if got := f.For("laptop", ""); len(got) != 0 {
		t.Errorf("a client matching no pattern holds %d grants, want none", len(got))
	}
	if authz.AllowsChannel(f.For("laptop", ""), "events", "events/a", "write") {
		t.Error("a client no rule names may publish: the default is refuse")
	}
}

// A role named twice in one entry is one set of grants. It grants the same
// rules either way, so the repetition is a typo rather than a statement -
// and emitting it twice puts every rule of that role on the screen twice
// for the person reading `--acl` to find out what a client may do.
func TestARoleNamedTwiceIsOneSetOfGrants(t *testing.T) {
	f := mustLoad(t, `
roles:
  publisher:
    - channel: events
      allow: [write]
users:
  dup: [publisher, publisher]
`)
	if got := f.For("dup", ""); len(got) != 1 {
		t.Errorf("a client naming one role twice holds %d grants, want 1 - the rule is "+
			"printed once per rule of each role held, not once per mention", len(got))
	}
}

// %u is the client's own identity, which is what lets one rule serve a
// fleet without naming any of them.
func TestAFilterScopesAChannelRuleToTheClient(t *testing.T) {
	f, err := load(t, `
roles:
  telemetry:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
users:
  "vessel-*": [telemetry]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	g := f.For("vessel-7", "")
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{"events/telemetry/vessel-7/temp", true},
		{"events/telemetry/vessel-7", true},
		{"events/telemetry/vessel-8/temp", false},
		{"events/other/vessel-7/temp", false},
		{"events", false},
	} {
		if got := authz.AllowsChannel(g, "events", tc.topic, "write"); got != tc.want {
			t.Errorf("publish to %q by vessel-7: %v, want %v - a channel rule's filter is "+
				"compared against the whole topic", tc.topic, got, tc.want)
		}
	}
}

// nested is a registry whose channel is named in the middle of its own
// filter, which is how the shipped example lays its channels out and the
// layout the mechanism this replaced could not serve at all.
func nested(t *testing.T) *channel.Registry {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "readings", Type: channel.Append, Filter: "iot/+/readings/+"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

func loadAgainst(t *testing.T, reg *channel.Registry, body string) (*authz.File, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return authz.Load(path, reg, 0)
}

// **The layout the mechanism this replaced could not serve, and the reason
// it was replaced.**
//
// `suffix:` compared against what followed the *channel name*. A channel
// named in the middle of its own filter left nothing there to compare, so
// every rule scoping such a channel matched no topic whatever and refused
// the device it was written for. Silently: the file loaded, `--acl` printed
// a grant, and every publish came back 0x87.
//
// Compared against the whole topic, the same rule is ordinary.
func TestAFilterScopesAChannelWhoseNameIsNotTheFirstLevel(t *testing.T) {
	f, err := loadAgainst(t, nested(t), `
roles:
  device:
    - channel: readings
      filter: iot/+/readings/%u
      allow: [write, read]
users:
  "vessel-*": [device]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	g := f.For("vessel-7", "")
	if !authz.AllowsChannel(g, "readings", "iot/hq/readings/vessel-7", "write") {
		t.Error("vessel-7 may not write its own topic in a channel whose name is not " +
			"the first level of its filter: that is the whole defect this replaced the " +
			"suffix for, and it failed by refusing the device the rule was written for")
	}
	if authz.AllowsChannel(g, "readings", "iot/hq/readings/vessel-8", "write") {
		t.Error("vessel-7 may write another device's topic, so the filter is not " +
			"scoping and the assertion above passes for the wrong reason")
	}
}

// **A rule that can never match is refused while the operator is looking.**
// It grants nothing and reads as though it granted part of the channel,
// which is the class this file exists to refuse - and it is how the
// mechanism this replaced failed, quietly, for a whole channel layout.
func TestAFilterThatCannotReachItsChannelIsRefused(t *testing.T) {
	_, err := loadAgainst(t, nested(t), `
roles:
  device:
    - channel: readings
      filter: readings/%u/#
      allow: [write]
users:
  "vessel-*": [device]
`)
	if err == nil {
		t.Fatal("a filter matching none of its channel's topics was accepted: it grants " +
			"nothing and reads as though it granted part of the channel")
	}
	for _, want := range []string{`readings/%u/#`, "iot/+/readings/+", "grants nothing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q, so an operator cannot see why: %v",
				want, err)
		}
	}
}

// The same substitution in the other field a rule can name a filter in.
// A `topic:` rule kept the literal `%u`, so it matched nothing: the file
// loaded, `--check-config` said ok, and every publish the operator meant to
// grant came back 0x87 - the shape this project fixes before anything else,
// because a rule that is present and dead reads as a rule that works.
func TestATopicRuleScopesToTheClient(t *testing.T) {
	f, err := load(t, `
roles:
  alerting:
    - topic: alerts/%u/#
      allow: [write, read]
users:
  "vessel-*": [alerting]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	g := f.For("vessel-7", "")
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{"alerts/vessel-7/fire", true},
		{"alerts/vessel-7", true},
		{"alerts/vessel-8/fire", false},
		{"alerts/%u/fire", false}, // the literal, which is what it granted before
		{"alerts", false},
	} {
		if got := authz.AllowsTopic(g, tc.topic, "write"); got != tc.want {
			t.Errorf("publish to %q by vessel-7: %v, want %v - %%u in a topic is the "+
				"client's own identity, exactly as it is in a suffix", tc.topic, got, tc.want)
		}
	}

	// The grant is what `--acl explain` prints, so an unsubstituted %u there
	// is a grant the broker will never match, shown to the one person asking
	// why their device is refused.
	if len(g) != 1 || strings.Contains(g[0].Topic, "%u") {
		t.Errorf("the grant carries %q, so the tool that explains it prints a rule that "+
			"cannot match", g[0].Topic)
	}
}

// The one shape a substituted topic could widen through, if the lookup
// asked its questions in the other order: an identity that spells a channel
// name. `topic: "%u/#"` loads, because no channel is called `%u`, and for a
// client called `events` the grant becomes `events/#`.
//
// It still reaches broadcast only. Allows resolves the real topic to a
// channel first and asks AllowsTopic only when none claims it, so the two
// rule kinds keep partitioning the topic space whatever an identity
// happens to spell.
func TestASubstitutedTopicCannotReachAChannel(t *testing.T) {
	f, err := load(t, `
roles:
  r:
    - topic: "%u/#"
      allow: [write, read]
users:
  "*": [r]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	r := authz.New(f, registry(t))
	if r.Allows("events", "", "events/a", true) {
		t.Error("a topic rule reached a channel's records because the client's identity " +
			"spelled the channel's name: a topic rule governs broadcast, and the " +
			"substitution may not carry it across the partition")
	}
	// The same rule for an identity that spells no channel still reaches
	// broadcast, so what is above is the partition holding rather than the
	// grant having gone missing.
	//
	// It takes a second identity to say that. For this one the grant is
	// `events/#`, and every topic that matches it is inside the channel -
	// so the rule correctly grants that client nothing at all, and an
	// assertion looking for something it still reaches would be looking for
	// something that cannot exist.
	if !r.Allows("loose", "", "loose/a", true) {
		t.Error("the rule no longer reaches broadcast at all")
	}
}

func TestAClientPatternMatchesWhatItLooksLike(t *testing.T) {
	f, err := load(t, `
roles:
  r:
    - topic: alerts/#
      allow: [read]
users:
  "vessel-*":     [r]
  "*-probe":      [r]
  "a*b*c":        [r]
  exactly-this:   [r]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, tc := range []struct {
		id   string
		want bool
	}{
		{"vessel-7", true},
		{"vessel-", true},
		// A client id is not a path, so a * crosses a / - which is why this
		// is not path.Match.
		{"vessel-7/sensor", true},
		{"vessel", false},
		{"north-probe", true},
		{"probe", false},
		{"abc", true},
		{"axxbyyc", true},
		{"acb", false},
		{"exactly-this", true},
		{"exactly-that", false},
	} {
		got := authz.AllowsTopic(f.For(tc.id, ""), "alerts/fire", "read")
		if got != tc.want {
			t.Errorf("client %q matched a pattern: %v, want %v", tc.id, got, tc.want)
		}
	}
}

// **A shared subscription is asked about the channel it reaches.**
// `$saguin/queue/jobs` resolves to no channel by prefix, so asking about
// the string as sent fell through to broadcast - which refused every worker
// whose role carried `consume`, and granted the queue's own subscription
// form to any role holding a `topic:` rule.
//
// The second half is the one that cost data. A `topic:` rule reaching the
// queue form is a phantom worker: it holds a share of the group, the
// delivery-side check then refuses every record, and the job it was offered
// is never re-offered, returned or dead-lettered. Accepted, acknowledged
// and wedged.
func TestASharedSubscriptionIsAskedAboutItsChannel(t *testing.T) {
	f, err := load(t, `
roles:
  worker:
    - channel: jobs
      allow: [consume]
  broadcaster:
    - topic: "#"
      allow: [read, write]
users:
  worker-only: [worker]
  bcast:       [broadcaster]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	rules := authz.New(f, registry(t))

	// The canonical queue form, which is the only one a queue admits.
	const canonical = "$saguin/queue/jobs"

	if !rules.Allows("worker-only", "", canonical, false) {
		t.Error("a worker whose role carries consume may not subscribe to the queue's " +
			"own form: every legitimate worker is locked out and the queue has no " +
			"consumers at all")
	}
	if rules.Allows("bcast", "", canonical, false) {
		t.Error("a role carrying only a topic: rule was granted the queue's subscription " +
			"form: that is a topic rule reaching across the partition into a channel, " +
			"and the phantom worker it creates strands the job it is offered")
	}
	// The same client, on what a topic: rule really governs.
	if !rules.Allows("bcast", "", "loose/#", false) {
		t.Error("a broadcast rule no longer reaches broadcast")
	}
	// And a share prefix does not launder a channel a role has no rule for.
	if rules.Allows("bcast", "", "$share/g/events/#", false) {
		t.Error("a share prefix in front of an events filter was granted to a role with " +
			"no rule about events")
	}
}

// **Which limits a client gets when several patterns match it.**
//
// Roles union across every matching pattern, which works because a rule
// only ever grants and two of them cannot disagree. Limits disagree by
// their nature - `device-*` at 100 a second and `device-7` at 10 have no
// union - so the most specific pattern supplies them, which is RFC 0002's
// rule for two channel filters claiming one topic rather than a second one
// to learn.
//
// **It wins in both directions.** The exception may be looser than the
// fleet it sits in: a gateway that legitimately publishes faster than a
// sensor is most of why per-client limits exist at all.
//
// **And it wins even by carrying none**, which is `device-9` below and the
// case worth having a name for: its own entry is more exact than the
// fleet's and writes no `limits:`, so the fleet's figures do not reach it
// and the broker-wide ones apply. That is the same subtraction the roles
// do, on the one thing whose removal loosens rather than tightens.
func TestTheMostExactEntrySuppliesTheLimits(t *testing.T) {
	f := mustLoad(t, `
roles:
  device:
    - topic: "iot/#"
      allow: [write]
users:
  "device-*":
    roles: [device]
    limits:
      publish_rate: 100
      publish_bytes: 64KiB
  "device-7":
    roles: [device]
    limits:
      publish_rate: 10
      publish_bytes: 6KiB
  "gateway-*":
    roles: [device]
    limits:
      publish_rate: 5000
      publish_bytes: 8MiB
  "device-9": [device]
  plain: [device]
`)
	for _, tc := range []struct {
		id    string
		rate  int
		bytes int64
		named bool
		why   string
	}{
		{"device-1", 100, 64 << 10, true, "matches only the fleet pattern"},
		{"device-7", 10, 6 << 10, true, "an exact id beats the wildcard that also matches it"},
		{"gateway-1", 5000, 8 << 20, true, "an exception may be looser, which is the point"},
		{"device-9", 0, 0, false, "its own entry carries no limits, so the fleet's do not " +
			"reach it - the entry that wins supplies the limits as well as the roles, " +
			"and this is the one shape of that rule which removes a bound"},
		{"plain", 0, 0, false, "a client with roles and no limits takes the broker-wide figures"},
		{"nobody", 0, 0, false, "a client no pattern matches takes them too"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			l, named := f.LimitsFor(tc.id)
			if named != tc.named {
				t.Fatalf("named = %v, want %v (%s)", named, tc.named, tc.why)
			}
			if l.PublishRate != tc.rate || l.PublishBytes != tc.bytes {
				t.Errorf("%s: %d/s and %d bytes/s, want %d and %d - %s",
					tc.id, l.PublishRate, l.PublishBytes, tc.rate, tc.bytes, tc.why)
			}
		})
	}
}

// **Two patterns equally exact and both matching one id are refused at
// startup, naming both**, because which entry applies would then be decided
// by nothing an operator can read. It is the refusal RFC 0002 already makes
// for two channels carrying one filter.
//
// **Limits have nothing to do with it any more**, which is the half that
// changed. While roles unioned they could not disagree, so a tie mattered
// only where limits were written; now one entry supplies the roles too, and
// a tie decides which - quietly, and differently as soon as anything about
// either pattern changes. So the plainest form of the file is the case:
// two patterns, roles only, no limits anywhere.
func TestEquallyExactPatternsAreRefusedEvenWithNoLimits(t *testing.T) {
	_, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
users:
  "ab*": [r]
  "a*b": [r]
`)
	if err == nil {
		t.Fatal("two equally exact patterns were accepted with no limits in the file: " +
			"which entry supplies the roles is then decided by nothing an operator " +
			"can read, and it decides them alone now that they no longer union")
	}
	for _, want := range []string{"ab*", "a*b"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q, so an operator cannot find it: %v",
				want, err)
		}
	}
}

// **Six refusals of a rule or a limit, each saying what is wrong** - each a
// sentence an operator is meant to act on, which no test read until they were counted: a rule still written with the retired
// suffix:, a publish_rate or publish_bytes that refuses every publish or
// cannot be read, a rule allowing nothing, and a braced topic that does not
// expand. A publish_bytes of zero is refused by config.ParseBytes itself,
// which is why authz has no branch of its own for it. Each is asserted by its own words, so a branch dropped or a
// sentence that stops saying why fails here.
func TestEachRefusalOfARuleOrALimitSaysWhatIsWrong(t *testing.T) {
	const topicRule = "roles:\n  r:\n    - topic: \"iot/#\"\n      allow: [write]\n"
	limits := func(key string) string {
		return topicRule + "users:\n  u:\n    roles: [r]\n    limits:\n      " + key + "\n"
	}
	for _, tc := range []struct{ why, body, want string }{
		{"a rule written with suffix:",
			"roles:\n  r:\n    - channel: events\n      suffix: x/+\n      allow: [read]\nusers:\n  u: [r]\n",
			"names a suffix:. It is filter: now"},
		{"a publish_rate of zero", limits("publish_rate: 0"),
			"limits.publish_rate 0: it is how many messages a second"},
		{"a publish_bytes that is not a size", limits("publish_bytes: lots"),
			`limits.publish_bytes "lots"`},
		{"a publish_bytes of zero", limits("publish_bytes: 0"),
			`limits.publish_bytes "0": must be greater than zero`},
		{"a rule with no allow:", "roles:\n  r:\n    - topic: \"iot/#\"\nusers:\n  u: [r]\n",
			"allows nothing, which is what a client has without it"},
		{"a braced topic that does not expand",
			"roles:\n  r:\n    - topic: \"alerts/{a,b\"\n      allow: [read]\nusers:\n  u: [r]\n",
			`names topic "alerts/{a,b"`},
	} {
		t.Run(tc.why, func(t *testing.T) {
			_, err := load(t, tc.body)
			if err == nil {
				t.Fatal("loaded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not say %q:\n%v", tc.want, err)
			}
		})
	}
}

// **`ab*c` against `a*bc` is such a pair** - three bytes spelled out each,
// both matching `abbc` - and the four witnesses the check used to try
// reached none of it, so the file loaded and sort order chose the entry.
// `ab*` against `ba*` ties too and shares no id, which is the half that
// keeps the refusal from being "any two patterns that tie".
func TestEquallyExactPatternsNoWitnessReachedAreRefused(t *testing.T) {
	const rules = "roles:\n  r:\n    - topic: \"iot/#\"\n      allow: [write]\n"
	_, err := load(t, rules+"users:\n  \"ab*c\": [r]\n  \"a*bc\": [r]\n")
	if err == nil {
		t.Fatal("ab*c and a*bc both match abbc and spell out as much, and the file " +
			"loaded: which entry applies is decided by sort order")
	}
	for _, want := range []string{`"ab*c"`, `"a*bc"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %s: %v", want, err)
		}
	}
	if _, err := load(t, rules+"users:\n  \"ab*\": [r]\n  \"ba*\": [r]\n"); err != nil {
		t.Errorf("ab* and ba* share no client id and were refused: %v", err)
	}
}

// **`device-*` against `*-7` is not such a pair**, which is worth its own
// case because it looks like one: both match `device-7`, but the first
// spells out seven characters and the second two, so the rule separates
// them and `device-*` wins. A tie needs the same count - `ab*` and `a*b`
// both match `ab` and both spell out two.
//
// Refusing this pair would make an ordinary acl_file stop loading, so the
// check has to be the rule rather than "these two overlap".
func TestPatternsOfDifferentExactnessAreFine(t *testing.T) {
	if _, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
users:
  "device-*":
    roles: [r]
    limits:
      publish_rate: 100
  "*-7": [r]
`); err != nil {
		t.Fatalf("two overlapping patterns of different exactness were refused, so the "+
			"check is matching on overlap rather than on the tie: %v", err)
	}
}

// **A user name and a client id are two different strings, and only one of
// them is proved.** `client_ids:` is where an operator ties them together,
// and it is the whole of what makes a cohort - a thousand devices on five
// credentials - anything other than one principal wearing a thousand hats.
//
// The rows below are the three shapes an operator writes and the two that
// have to keep working untouched.
func TestClientIDsSayWhichDevicesMayCarryAName(t *testing.T) {
	f := mustLoad(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write, read]
users:
  "cohort-north":
    roles: [r]
    client_ids: "north-*"
  "cohort-south":
    roles: [r]
    client_ids: ["south-*", "spare-99"]
  "device-*":
    roles: [r]
    client_ids: "%u"
  unconstrained: [r]
`)
	for _, tc := range []struct {
		identity, clientID string
		want               bool
		why                string
	}{
		{"cohort-north", "north-17", true, "one pattern, a device inside it"},
		{"cohort-north", "north-204", true, "the same, further along the fleet"},
		{"cohort-north", "south-3", false, "the northern credential may not carry a southern id"},
		{"cohort-north", "cohort-north", false, "not even the name itself, unless the pattern says so"},
		{"cohort-south", "south-3", true, "a list, first entry"},
		{"cohort-south", "spare-99", true, "a list, the spare that replaced a failed unit"},
		{"cohort-south", "north-17", false, "a list refuses what none of its entries match"},
		{"device-7", "device-7", true, "%u: connect under the name you logged in as"},
		{"device-7", "device-8", false, "%u: and not under anybody else's"},
		{"unconstrained", "anything-at-all", true, "no client_ids, so any client id - which is every file written before the key existed"},
		{"nobody", "anything-at-all", true, "no entry applies, so there is nothing to constrain"},
	} {
		t.Run(tc.identity+"/"+tc.clientID, func(t *testing.T) {
			if got := f.AllowsClientID(tc.identity, tc.clientID); got != tc.want {
				t.Errorf("AllowsClientID(%q, %q) = %v, want %v - %s",
					tc.identity, tc.clientID, got, tc.want, tc.why)
			}
		})
	}
}

// **A restriction that admits nobody reads as protection and is a locked
// door.** Every device holding the credential is refused at connect, which
// looks like a broken fleet rather than a configuration mistake, so it is
// refused while the operator is looking.
func TestClientIDsThatAdmitNobodyAreRefused(t *testing.T) {
	_, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
users:
  "cohort-north":
    roles: [r]
    client_ids: ["north-*", ""]
`)
	if err == nil {
		t.Fatal("an empty client_ids entry was accepted: it matches no client id, so " +
			"the fleet holding this credential is refused at the door")
	}
	if !strings.Contains(err.Error(), "matches no client id") {
		t.Errorf("the refusal does not say what is wrong: %v", err)
	}
}

// **Every spelling of "a restriction I began and did not finish".**
//
// `client_ids: []` was refused from the start; the other two were not. They matter because they fail in opposite
// directions, so neither looks like a mistake from the log:
//
//   - `client_ids:` with nothing under it restricts *nothing* - any device
//     may use the credential. yaml calls no unmarshaler for a null value,
//     so the field arrives as the same nil an absent key gives, and the
//     check that catches `[]` guarded on exactly that difference.
//   - `client_ids: "%c"` restricts to *one* device, the one whose id is
//     literally `%c`. `%c` means "the id this client connected under"
//     everywhere else in the file, and this key is what decides whether it
//     connects at all, so there is no id to substitute yet.
//
// The second is the worse of the two: a whole fleet refused 0x86 reads in a
// log exactly like devices that are misconfigured, and the file that did it
// loads clean.
func TestEveryHalfWrittenClientIDsRestrictionIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, ids, want string }{
		{"the key with nothing under it", "    client_ids:\n", "with nothing in it"},
		{"the key written null", "    client_ids: null\n", "with nothing in it"},
		{"an empty list", "    client_ids: []\n", "with nothing in it"},
		{"%c alone", `    client_ids: "%c"` + "\n", "there is none yet"},
		{"%c in a list", `    client_ids: ["north-*", "%c"]` + "\n", "there is none yet"},
		{"%c inside a pattern", `    client_ids: "north-%c"` + "\n", "there is none yet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
users:
  "cohort-north":
    roles: [r]
`+tc.ids)
			if err == nil {
				t.Fatalf("%s was accepted, so it reads as a restriction and is not one",
					tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not say what is wrong; want %q in: %v",
					tc.want, err)
			}
		})
	}

	// And the spellings that mean something still load, so the check above
	// is about the half-written ones rather than about the key.
	for _, ids := range []string{
		`    client_ids: "north-*"` + "\n",
		`    client_ids: ["north-*", "spare-1"]` + "\n",
		`    client_ids: "%u"` + "\n",
		"", // absent: any client id, which is what most files mean
	} {
		if _, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
users:
  "cohort-north":
    roles: [r]
`+ids); err != nil {
			t.Errorf("a client_ids that says something was refused: %q: %v", ids, err)
		}
	}
}

// **`%c` is the other name a client has, and the one nothing proves.**
//
// It exists for the deployment `%u` cannot serve: a cohort sharing one
// credential, where the proved name is the same for every device and the
// only thing telling them apart is the id each chose. Scoping by it gives
// each device its own topics; it does not stop one taking another's, and
// the file says so by using it.
//
// The pair below is the point. `%u` alone cannot separate two devices
// sharing a name, and `%c` alone would let any device reach any cohort's
// topics - written together, the credential fixes the cohort and the client
// id picks the device within it.
func TestAClientIDScopesARule(t *testing.T) {
	f := mustLoad(t, `
roles:
  sensor:
    - topic: iot/%u/health/%c
      allow: [write, read]
users:
  "cohort-*":
    roles: [sensor]
    client_ids: "north-*"
`)
	for _, tc := range []struct {
		identity, clientID, topic string
		want                      bool
		why                       string
	}{
		{"cohort-north", "north-17", "iot/cohort-north/health/north-17", true, "its own topic"},
		{"cohort-north", "north-17", "iot/cohort-north/health/north-18", false, "another device in its own cohort"},
		{"cohort-north", "north-18", "iot/cohort-north/health/north-18", true, "and that device reaches it"},
		{"cohort-south", "north-17", "iot/cohort-north/health/north-17", false, "%u fixes the cohort, so a southern credential cannot reach a northern topic"},
	} {
		t.Run(tc.clientID+"->"+tc.topic, func(t *testing.T) {
			g := f.For(tc.identity, tc.clientID)
			if got := authz.AllowsTopic(g, tc.topic, "write"); got != tc.want {
				t.Errorf("%s as %s writing %q: %v, want %v - %s",
					tc.clientID, tc.identity, tc.topic, got, tc.want, tc.why)
			}
		})
	}
}

// With no client id in hand, `%c` stands. The grant then matches nothing,
// which is correct - there is no device to resolve it for - and it is why
// the tool that prints grants says so rather than showing the line bare.
func TestAnUnresolvedClientIDLeavesTheRuleStanding(t *testing.T) {
	f := mustLoad(t, `
roles:
  sensor:
    - topic: iot/health/%c
      allow: [write]
users:
  "cohort-*": [sensor]
`)
	g := f.For("cohort-north", "")
	if len(g) != 1 {
		t.Fatalf("holds %d grants, want 1", len(g))
	}
	if g[0].Topic != "iot/health/%c" {
		t.Errorf("the grant is %q, want the literal left standing: substituting an empty "+
			"client id would widen it to a topic every device matches", g[0].Topic)
	}
	if authz.AllowsTopic(g, "iot/health/north-17", "write") {
		t.Errorf("an unresolved %%c matched a real topic, so a rule nobody could resolve " +
			"is granting")
	}
}

// RFC 0002's acl_file: a name put into a rule by %u or %c names one level,
// and a name holding what a topic filter reads as its own - `+`, `#` or `/` -
// is never put in: the rule grants that client nothing (Withheld), and every
// other rule still applies. The client id is whatever the device typed, so
// without this a device called `#` under `iot/health/%c` could read and
// write every device's topic.
func TestANameHoldingFilterSyntaxWidensNoRule(t *testing.T) {
	f := mustLoad(t, `
roles:
  sensor:
    - topic: iot/health/%c
      allow: [read, write]
  fleet:
    - topic: iot/%u/#
      allow: [read]
  common:
    - topic: announce
      allow: [read]
users:
  "*": [sensor, fleet, common]
`)
	for _, tc := range []struct {
		identity, clientID string
		// withheld is how many rules grant this pair nothing.
		withheld int
		topic    string
		verb     string
		want     bool
	}{
		{"device", "north-17", 0, "iot/health/north-17", "write", true},
		{"device", "north-17", 0, "iot/health/south-3", "write", false},
		{"device", "#", 1, "iot/health/north-17", "read", false},
		{"device", "#", 1, "iot/health/north-17", "write", false},
		{"device", "#", 1, "iot/health/other/deep", "read", false},
		{"device", "+", 1, "iot/health/north-17", "write", false},
		{"device", "a/b", 1, "iot/health/a/b", "write", false},
		{"device", "#", 1, "announce", "read", true}, // a rule naming neither still applies
		{"device", "#", 1, "iot/device/x", "read", true},
		{"+", "north-17", 1, "iot/other-device/status", "read", false},
		{"#", "north-17", 1, "iot/other-device/status", "read", false},
		{"a/b", "north-17", 1, "iot/a/b/status", "read", false},
		{"device", "north-17", 0, "iot/device/status", "read", true},
	} {
		grants := f.For(tc.identity, tc.clientID)
		if got := authz.AllowsTopic(grants, tc.topic, tc.verb); got != tc.want {
			t.Errorf("identity %q, client id %q: %s %s = %t, want %t (grants %+v)",
				tc.identity, tc.clientID, tc.verb, tc.topic, got, tc.want, grants)
		}
		if got := len(f.Withheld(tc.identity, tc.clientID)); got != tc.withheld {
			t.Errorf("identity %q, client id %q: %d rules withheld, want %d", tc.identity, tc.clientID, got, tc.withheld)
		}
	}
}

// A substitution is one pass: a name holding `%c` is put into a %u rule as
// written, never substituted again with the client id.
func TestASubstitutedNameIsNotSubstitutedAgain(t *testing.T) {
	f := mustLoad(t, `
roles:
  fleet:
    - topic: iot/%u/status
      allow: [read]
users:
  "*": [fleet]
`)
	grants := f.For("%c", "north-17")
	if authz.AllowsTopic(grants, "iot/north-17/status", "read") {
		t.Errorf("an identity %%c under iot/%%u/status reached the client id's topic: the name "+
			"was substituted a second time (grants %+v)", grants)
	}
	if !authz.AllowsTopic(grants, "iot/%c/status", "read") {
		t.Errorf("the identity was not put in as written (grants %+v)", grants)
	}
}

// client_ids' `*` is the pattern's wildcard, so an identity holding one is
// never put into a %u pattern: identity `*` under `client_ids: ["%u"]` would
// admit every client id.
func TestAClientIDsPatternTakesNoWildcardFromTheName(t *testing.T) {
	f := mustLoad(t, `
roles:
  sensor:
    - topic: iot/%u
      allow: [write]
users:
  "*":
    roles: [sensor]
    client_ids: ["%u"]
`)
	for _, tc := range []struct {
		identity, clientID string
		want               bool
	}{
		{"device", "device", true},
		{"device", "anything", false},
		{"*", "anything", false},
		{"dev*", "device-7", false},
		{"*", "*", false},
	} {
		if got := f.AllowsClientID(tc.identity, tc.clientID); got != tc.want {
			t.Errorf("identity %q, client id %q: allowed %t, want %t", tc.identity, tc.clientID, got, tc.want)
		}
	}
}

// **The block was called `clients:` until today**, so every acl_file that
// exists names it that way - and a file that was correct yesterday needs
// one word changed. Left to the decoder the operator gets `field clients
// not found in type authz.File`, which names a Go type and says nothing
// about what to do.
func TestTheRetiredClientsKeyIsRefusedWithASentence(t *testing.T) {
	_, err := load(t, `
roles:
  r:
    - topic: "iot/#"
      allow: [write]
clients:
  c: [r]
`)
	if err == nil {
		t.Fatal("a file using the retired clients: key was accepted")
	}
	got := err.Error()
	if strings.Contains(got, "authz.File") {
		t.Errorf("the refusal is the decoder's, naming a Go type rather than the "+
			"one-word change the operator has to make: %v", err)
	}
	for _, want := range []string{"users:", "Rename the key"} {
		if !strings.Contains(got, want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// **The short form still loads**, which is every acl_file written before
// limits existed. Making those files say `roles:` for no reason would be
// churn charged to every operator for a feature most will not use.
func TestTheShortClientFormStillLoads(t *testing.T) {
	f := mustLoad(t, `
roles:
  tour:
    - topic: "iot/#"
      allow: [write]
users:
  demo: [tour]
`)
	if got := f.Users["demo"].Roles; len(got) != 1 || got[0] != "tour" {
		t.Fatalf("the short form gave roles %v, want [tour]", got)
	}
	if l, named := f.LimitsFor("demo"); named {
		t.Errorf("a client written in the short form reports limits %+v", l)
	}
}

// mustLoad is load for a case where the file is expected to be valid.
func mustLoad(t *testing.T, body string) *authz.File {
	t.Helper()
	f, err := load(t, body)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return f
}

// **The rule kind that governs no topic**, and the property it exists for:
// nothing written as a filter can confer it.
//
// A `broker:` rule was made a kind of its own rather than a reserved topic
// because the ACL's filter matcher has no `$` exclusion - `#` matches
// `$saguin/anything`, so a control topic spelled into this file would have
// been granted by `topic: "#"`, which is how an operator says "any
// broadcast topic" and is in the specification's own example role. The
// first case below is that property, and it is the one that must never
// stop holding.
func TestOnlyABrokerRuleGrantsABrokerVerb(t *testing.T) {
	f, err := load(t, `
roles:
  everything:
    - channel: events
      allow: [write, read, seek]
    - channel: state
      allow: [write, read, delete]
    - channel: jobs
      allow: [write, consume]
    - topic: "#"
      allow: [write, read]
  on-call:
    - broker: sessions
      allow: [disconnect]
users:
  greedy: [everything]
  oncall: [on-call]
`)
	if err != nil {
		t.Fatalf("the file does not load: %v", err)
	}

	// Every verb on every channel, and the widest topic rule there is.
	if authz.AllowsBroker(f.For("greedy", "greedy"), "sessions", "disconnect") {
		t.Error("a role holding every channel verb and topic: \"#\" may hang clients up: " +
			"the broker rule kind exists precisely so that no filter can reach it, and a " +
			"filter has reached it")
	}
	// And the rule written for it does.
	if !authz.AllowsBroker(f.For("oncall", "oncall"), "sessions", "disconnect") {
		t.Error("the role that names the facility and the verb grants nothing, so the " +
			"check above passes by granting nobody anything")
	}
	// The other direction: a broker grant is not a broadcast grant. It
	// carries no channel, so it reaches AllowsTopic's loop, and what stops
	// it there is an explicit skip rather than an empty filter failing to
	// match.
	for _, verb := range []string{"write", "read", "disconnect"} {
		if authz.AllowsTopic(f.For("oncall", "oncall"), "anything/at/all", verb) {
			t.Errorf("the broker grant answers %q on a broadcast topic", verb)
		}
	}
	if authz.AllowsChannel(f.For("oncall", "oncall"), "events", "events/x", "read") {
		t.Error("the broker grant answers a channel verb")
	}
}

// A facility nobody named, and a verb the facility does not have. Both are
// startup failures rather than a rule that grants nothing and reads as
// though it does - which is the class this whole file refuses.
func TestABrokerRuleIsCheckedLikeEveryOtherKind(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		want string
	}{
		"a facility that does not exist": {
			"roles:\n  r:\n    - broker: session\n      allow: [disconnect]\nusers:\n  c: [r]\n",
			`broker facility "session", which is not one of sessions`},
		"a verb the facility does not have": {
			"roles:\n  r:\n    - broker: sessions\n      allow: [expire]\nusers:\n  c: [r]\n",
			`allows "expire", which is not one of disconnect`},
		"a channel verb on the broker": {
			"roles:\n  r:\n    - broker: sessions\n      allow: [read]\nusers:\n  c: [r]\n",
			`allows "read", which is not one of disconnect`},
		"the same verb twice": {
			"roles:\n  r:\n    - broker: sessions\n      allow: [disconnect, disconnect]\nusers:\n  c: [r]\n",
			`allows "disconnect" twice`},
		"nothing allowed": {
			"roles:\n  r:\n    - broker: sessions\n      allow: []\nusers:\n  c: [r]\n",
			"allows nothing"},
		"a filter, which a facility has no topics to narrow": {
			"roles:\n  r:\n    - broker: sessions\n      filter: \"#\"\n      allow: [disconnect]\nusers:\n  c: [r]\n",
			"carries a filter"},
		"a facility and a channel in one rule": {
			"roles:\n  r:\n    - broker: sessions\n      channel: events\n      allow: [disconnect]\nusers:\n  c: [r]\n",
			`beside channel "events"`},
		"a facility and a topic in one rule": {
			"roles:\n  r:\n    - broker: sessions\n      topic: \"alerts/#\"\n      allow: [disconnect]\nusers:\n  c: [r]\n",
			`beside topic "alerts/#"`},
		"a rule naming none of the three": {
			"roles:\n  r:\n    - allow: [read]\nusers:\n  c: [r]\n",
			"neither a channel, a topic nor a broker facility"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(t, tc.body)
			if err == nil {
				t.Fatalf("accepted, so a broker with this file starts and the rule means "+
					"nothing:\n%s", tc.body)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not carry %q: %v", tc.want, err)
			}
		})
	}
}

// **Every presentation of a grant has to agree what kind it is**, and there
// are three of them: the flag's table, its columns, and the operations
// route. They share one function, which answered "topic" for anything
// holding no channel - so a broker grant would have been printed as a topic
// rule governing an empty topic, in the column an operator reads to check
// what a role does.
func TestABrokerGrantIsPresentedAsOne(t *testing.T) {
	f, err := load(t, "roles:\n  on-call:\n    - broker: sessions\n      allow: [disconnect]\n"+
		"users:\n  oncall: [on-call]\n")
	if err != nil {
		t.Fatalf("the file does not load: %v", err)
	}
	grants := f.For("oncall", "oncall")
	if len(grants) != 1 {
		t.Fatalf("the role granted %d rules, want 1", len(grants))
	}
	if got := authz.Kind(grants[0]); got != "broker" {
		t.Errorf("the grant is presented as kind %q, want broker", got)
	}
	if got := authz.Named(grants[0]); got != "sessions" {
		t.Errorf("the grant's subject is %q, want sessions - an operator reading the "+
			"column cannot tell which facility a rule is about", got)
	}
}

// braced is a registry whose channel filter carries the `{a,b}` notation
// RFC 0002 gives an operator for one level with two spellings. Every case
// below is about the acl_file meeting that notation, which for a while it
// did not do at all: the registry expands a brace before it routes or
// replays anything, and this file was the one layer that met the written
// form and read it as an ordinary level.
func braced(t *testing.T) *channel.Registry {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "readings", Type: channel.Append, Filter: "iot/+/{device,sensor}/#"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

// **The half that failed open, and the reason this is a startup refusal
// rather than a note in the documentation.** A rule filter was never put
// through the check a channel's filter goes through, so `alerts/#/page`
// loaded - and a `#` the matcher meets in the middle of a filter stands in
// for everything after it, so the rule granted every `alerts` topic to an
// operator who had written three. The file read as a narrowing, `--acl`
// printed it as one, and it was not one.
//
// Both rule kinds, because the filter arrives in `filter:` for a channel
// and in `topic:` for broadcast, and a fix to one of those leaves the
// other granting the lot.
func TestASpellingRefusedOnAChannelIsRefusedInARule(t *testing.T) {
	for _, tc := range []struct {
		what, body string
	}{
		{"a channel rule's filter, `#` in the middle", `
roles:
  r:
    - channel: readings
      filter: "iot/#/device"
      allow: [read]
users:
  d: [r]
`},
		{"a channel rule's filter, two wildcards in one level", `
roles:
  r:
    - channel: readings
      filter: "iot/+/device/++"
      allow: [read]
users:
  d: [r]
`},
		{"a broadcast rule's topic, `#` in the middle", `
roles:
  r:
    - topic: "alerts/#/page"
      allow: [read]
users:
  d: [r]
`},
		{"a broadcast rule's topic, two wildcards in one level", `
roles:
  r:
    - topic: "alerts/++/page"
      allow: [read]
users:
  d: [r]
`},
		{"a brace holding a `/`, which is a level and never a subtree", `
roles:
  r:
    - channel: readings
      filter: "iot/+/{device,sen/sor}/#"
      allow: [read]
users:
  d: [r]
`},
	} {
		if _, err := loadAgainst(t, braced(t), tc.body); err == nil {
			t.Errorf("%s was accepted. A spelling the broker refuses on a channel "+
				"grants more than it says here, and reads as though it granted less",
				tc.what)
		}
	}
}

// The refusal above must not reach the one an operator writes on purpose.
// `topic: "#"` is how RFC 0002 says "any broadcast topic", and the first
// level's rule - spelled out, no `$` - is a channel's rather than MQTT's:
// a channel claiming everything would swallow `$SYS` and `$saguin`, and a
// broadcast grant claims no route at all.
func TestTheWidestBroadcastRuleStillLoads(t *testing.T) {
	f, err := loadAgainst(t, braced(t), `
roles:
  everything:
    - topic: "#"
      allow: [read, write]
users:
  d: [everything]
`)
	if err != nil {
		t.Fatalf(`topic: "#" was refused, and it is how an operator says "any `+
			`broadcast topic": %v`, err)
	}
	if !authz.AllowsTopic(f.For("d", ""), "anything/at/all", "write") {
		t.Error(`topic: "#" loaded and granted nothing`)
	}
}

// **The half that failed closed**, and the one an operator hits first: the
// natural way to scope a rule to one of a channel's two spellings. It was
// refused at startup with a sentence saying it matched none of the
// channel's topics, about a filter matching exactly half of them.
func TestARuleMayNarrowABracedChannelToOneSpelling(t *testing.T) {
	f, err := loadAgainst(t, braced(t), `
roles:
  devices:
    - channel: readings
      filter: "iot/+/device/#"
      allow: [read, write]
users:
  d: [devices]
`)
	if err != nil {
		t.Fatalf("narrowing to one spelling of a braced channel was refused, and it "+
			"matches half that channel's topics: %v", err)
	}
	g := f.For("d", "")
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{"iot/s1/device/t", true},
		{"iot/s1/sensor/t", false},
	} {
		if got := authz.AllowsChannel(g, "readings", tc.topic, "write"); got != tc.want {
			t.Errorf("publish to %q: %v, want %v - the narrowing grants the spelling it "+
				"names and only that one", tc.topic, got, tc.want)
		}
	}
}

// The other spelling of the same intention, and the one that failed
// silently: an operator who cannot narrow copies the channel's own filter
// into the rule. It loaded, `--acl` printed a grant, and the device was
// answered 0x87 on both verbs.
func TestARuleCarryingTheChannelsOwnBracedFilterGrantsIt(t *testing.T) {
	f, err := loadAgainst(t, braced(t), `
roles:
  both:
    - channel: readings
      filter: "iot/+/{device,sensor}/#"
      allow: [read, write]
users:
  d: [both]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	g := f.For("d", "")
	for _, topic := range []string{"iot/s1/device/t", "iot/s1/sensor/t"} {
		if !authz.AllowsChannel(g, "readings", topic, "write") {
			t.Errorf("publish to %q was refused by a rule carrying the channel's own "+
				"filter. The rule is present, reads as a grant, and grants nothing",
				topic)
		}
	}
}

// A braced broadcast rule, which reached the matcher the same way and
// matched the literal brace - so it granted nothing at all.
func TestABracedBroadcastRuleGrantsEverySpelling(t *testing.T) {
	f, err := loadAgainst(t, braced(t), `
roles:
  paging:
    - topic: "alerts/{fire,flood}/page"
      allow: [write]
users:
  d: [paging]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	g := f.For("d", "")
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{"alerts/fire/page", true},
		{"alerts/flood/page", true},
		{"alerts/quake/page", false},
	} {
		if got := authz.AllowsTopic(g, tc.topic, "write"); got != tc.want {
			t.Errorf("publish to %q: %v, want %v", tc.topic, got, tc.want)
		}
	}
}

// **Why a brace is expanded before `%u` and `%c` are put in, and not
// after.** The other order reads as the tidier one - substitute, then
// expand whatever came out - and it hands a client the ability to widen
// its own grant: a device that names itself `{s1,s9}`, substituted into
// `iot/%c/state/+`, would be two filters, and a rule an operator wrote for
// one device would reach two.
//
// Expanding what the file says settles the grant before any client is
// named, and then nothing a device calls itself can change it. The braced
// name is left as the one ordinary level it is, which is the level the
// device asked for and no other.
func TestABracedClientNameCannotWidenItsOwnGrant(t *testing.T) {
	f, err := loadAgainst(t, nested(t), `
roles:
  own:
    - channel: readings
      filter: "iot/%c/readings/+"
      allow: [write]
users:
  d: [own]
`)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	g := f.For("d", "{s1,s9}")
	for _, tc := range []struct {
		topic string
		want  bool
	}{
		{"iot/s1/readings/t", false},
		{"iot/s9/readings/t", false},
		{"iot/{s1,s9}/readings/t", true},
	} {
		if got := authz.AllowsChannel(g, "readings", tc.topic, "write"); got != tc.want {
			t.Errorf("a device calling itself {s1,s9} publishing to %q: %v, want %v - "+
				"a name is one level and cannot become two", tc.topic, got, tc.want)
		}
	}
}

// RFC 0002 "Taking a feature away: `broker: features`" - a features rule may
// deny the five features, each named for its broker block, and only those,
// and only there.
func TestAFeaturesRuleMayDenyEachFeature(t *testing.T) {
	f, err := load(t, "roles:\n  fleet:\n    - broker: features\n      deny: [persistent, will, share, retained, qos2]\n"+
		"  oncall:\n    - broker: sessions\n      allow: [disconnect]\n"+
		"    - broker: features\n      deny: [will]\n"+
		"users:\n  device-7: [fleet]\n  ops: [oncall]\n")
	if err != nil {
		t.Fatalf("a features rule denying all five did not load: %v", err)
	}
	r := authz.New(f, registry(t))
	for _, tc := range []struct {
		user, feature string
		denied        bool
	}{
		{"device-7", "persistent", true},
		{"device-7", "will", true},
		{"device-7", "share", true},
		{"device-7", "retained", true},
		{"device-7", "qos2", true},
		{"ops", "will", true},
		{"ops", "persistent", false},
		{"ops", "share", false},
		{"ops", "retained", false},
		{"ops", "qos2", false},
		{"stranger", "will", false},
	} {
		if got := r.DeniesFeature(tc.user, "c", tc.feature); got != tc.denied {
			t.Errorf("%s %s: denied %v, want %v", tc.user, tc.feature, got, tc.denied)
		}
	}
	// A denial is not a grant: the role that only denies still grants nothing,
	// and the one granting disconnect keeps it beside its denial.
	if r.AllowsBrokerVerb("device-7", "c", "sessions", "disconnect") {
		t.Error("a rule that only denies granted disconnect")
	}
	if !r.AllowsBrokerVerb("ops", "c", "sessions", "disconnect") {
		t.Error("a features rule took away the disconnect a sessions rule grants")
	}
}

// A feature is denied when any of the client's roles denies it, whatever the
// others allow.
func TestADenialInAnyRoleWins(t *testing.T) {
	f, err := load(t, "roles:\n  wide:\n    - channel: events\n      allow: [write, read, seek]\n"+
		"    - topic: \"#\"\n      allow: [write, read]\n    - broker: sessions\n      allow: [disconnect]\n"+
		"  short-lived:\n    - broker: features\n      deny: [persistent]\n"+
		"users:\n  device-7: [wide, short-lived]\n")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	r := authz.New(f, registry(t))
	if !r.DeniesFeature("device-7", "c", "persistent") {
		t.Error("a role granting everything undid another role's denial of a persistent session")
	}
	if r.DeniesFeature("device-7", "c", "will") {
		t.Error("a feature no role denies was denied")
	}
	if !r.AllowsBrokerVerb("device-7", "c", "sessions", "disconnect") {
		t.Error("the denial took a verb the other role grants")
	}
}

// Every way to write a denial that cannot mean what it says is refused by
// name.
func TestADenialThatCannotMeanWhatItSaysIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		// **`retain` is the flag, `retained` is the feature**, named for
		// broker.retained. The near miss is the one worth refusing by name.
		{"an unknown feature",
			"roles:\n  r:\n    - broker: features\n      deny: [retain]\nusers:\n  c: [r]\n", `"retain"`},
		{"a repeat",
			"roles:\n  r:\n    - broker: features\n      deny: [will, will]\nusers:\n  c: [r]\n", "twice"},
		{"on a channel rule",
			"roles:\n  r:\n    - channel: events\n      allow: [read]\n      deny: [will]\nusers:\n  c: [r]\n",
			"deny: is written only on a `broker: features` rule"},
		{"on a topic rule",
			"roles:\n  r:\n    - topic: \"alerts/#\"\n      allow: [read]\n      deny: [persistent]\nusers:\n  c: [r]\n",
			"deny: is written only on a `broker: features` rule"},
		{"on a sessions rule",
			"roles:\n  r:\n    - broker: sessions\n      deny: [will]\nusers:\n  c: [r]\n",
			"Move it there"},
		{"allowing on a features rule",
			"roles:\n  r:\n    - broker: features\n      allow: [disconnect]\nusers:\n  c: [r]\n",
			"takes deny: and nothing else"},
		{"a features rule denying nothing",
			"roles:\n  r:\n    - broker: features\n      deny: []\nusers:\n  c: [r]\n", "denies nothing"},
		{"a sessions rule neither allowing nor denying",
			"roles:\n  r:\n    - broker: sessions\n      deny: []\nusers:\n  c: [r]\n", "denies nothing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("not refused naming %q: %v", tc.want, err)
			}
		})
	}
}

// A denial is shown as one, in every presentation of a grant.
func TestADenialIsPresentedAsOne(t *testing.T) {
	f, err := load(t, "roles:\n  fleet:\n    - broker: features\n      deny: [will]\nusers:\n  device-7: [fleet]\n")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := authz.Describe(f, "device-7", "")
	if len(got) != 1 || fmt.Sprint(got[0].Denies) != "[will]" || len(got[0].Verbs) != 0 {
		t.Fatalf("the explanation of a denying rule is %+v", got)
	}
}

// **A filter is granted only where every topic it matches is granted**:
// nothing granted on channel rules alone matches a broadcast topic, and
// nothing granted beside one channel's rule matches another channel's
// topic. Over every filter of up to three levels from `a`, `b` and `+`,
// with or without a trailing `#`, against six registries - one of them the
// shape only several channels together cover (`a/+/#` over `a/+` and
// `a/+/+/#`), and every one with a route whose `+` or `#` would take a
// filter's own wildcard as a level if the filter were read as a topic. The oracle is RFC 0002 "What a filter reaches": a broadcast
// topic is one no channel's filter matches, and only a `topic:` rule
// grants it; and a channel's topics only its channel rule grants. Each grant
// is checked against every topic of up to four levels from `a`, `b` and
// `c` - one level deeper than any filter here, and `c` a level no filter
// spells.
//
// Granted and refused are both counted: a rule refusing everything would
// pass the property alone, and one granting everything fails it.
func TestAFilterIsGrantedOnlyWhereEveryTopicItMatchesIsGranted(t *testing.T) {
	var filters []string
	var grow func(levels []string)
	grow = func(levels []string) {
		if len(levels) > 0 {
			filters = append(filters, strings.Join(levels, "/"), strings.Join(levels, "/")+"/#")
		}
		if len(levels) == 3 {
			return
		}
		for _, l := range []string{"a", "b", "+"} {
			grow(append(append([]string{}, levels...), l))
		}
	}
	grow(nil)
	filters = append(filters, "#")

	var topics []string
	var spell func(levels []string)
	spell = func(levels []string) {
		if len(levels) > 0 {
			topics = append(topics, strings.Join(levels, "/"))
		}
		if len(levels) == 4 {
			return
		}
		for _, l := range []string{"a", "b", "c"} {
			spell(append(append([]string{}, levels...), l))
		}
	}
	spell(nil)

	// Pinned rows first, each a shape that went wrong: `a/#` read as a topic
	// resolved to `a/+` and was granted on it alone, though `a` and `a/b/c`
	// are broadcast; `+/x` was granted on its channels alone; and the
	// dead-letter shape, `a/d/#` inside both `a/d/#` and `a/#`, which is
	// the more exact channel's alone and needs nothing from the other.
	for _, tc := range []struct {
		routes []string
		grant  string // the channel read on, beside no topic rule
		filter string
		want   bool
	}{
		{[]string{"a/+", "b/#"}, "c0", "a/#", false},
		{[]string{"a/+", "b/+"}, "", "+/x", false},
		{[]string{"a/#", "a/d/#"}, "c1", "a/d/#", true},
		{[]string{"a/#", "a/d/#"}, "c0", "a/d/#", false},
	} {
		var chans []*channel.Channel
		for i, filter := range tc.routes {
			chans = append(chans, &channel.Channel{Name: fmt.Sprintf("c%d", i), Type: channel.Append, Filter: filter})
		}
		reg, err := channel.NewRegistry(chans)
		if err != nil {
			t.Fatalf("registry %v: %v", tc.routes, err)
		}
		acl := "roles:\n  r:\n"
		for _, c := range chans {
			if tc.grant == "" || c.Name == tc.grant {
				acl += "    - channel: " + c.Name + "\n      allow: [read]\n"
			}
		}
		path := filepath.Join(t.TempDir(), "acl.yaml")
		if err := os.WriteFile(path, []byte(acl+"users:\n  u: [r]\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		f, err := authz.Load(path, reg, 0)
		if err != nil {
			t.Fatalf("load for %v: %v", tc.routes, err)
		}
		if got := authz.New(f, reg).Allows("u", "u", tc.filter, false); got != tc.want {
			t.Errorf("channels %v, read on %q: %q granted %v, want %v", tc.routes,
				tc.grant, tc.filter, got, tc.want)
		}
	}

	var granted, refused int
	for _, routes := range [][]string{
		{"a/#"},
		{"a/+", "b/#"},
		{"a/+/a", "b/+/#"},
		{"a/+", "a/+/+/#"},
		{"a/b", "b/+/b"},
		{"a/#", "a/b"},
	} {
		var chans []*channel.Channel
		for i, filter := range routes {
			chans = append(chans, &channel.Channel{Name: fmt.Sprintf("c%d", i), Type: channel.Append, Filter: filter})
		}
		reg, err := channel.NewRegistry(chans)
		if err != nil {
			t.Fatalf("registry %v: %v", routes, err)
		}
		// Two arms, each holding one kind of grant and not the other: read
		// on every channel and no topic rule, so nothing granted may match a
		// broadcast topic; and read on c0 with `topic: "#"`, so nothing
		// granted may match a topic another channel claims.
		everyChannel := "roles:\n  r:\n"
		for _, c := range chans {
			everyChannel += "    - channel: " + c.Name + "\n      allow: [read]\n"
		}
		oneChannel := "roles:\n  r:\n    - channel: c0\n      allow: [read]\n" +
			"    - topic: \"#\"\n      allow: [read]\n"
		for _, arm := range []struct {
			acl    string
			leaked func(topic string) bool
			what   string
		}{
			{everyChannel, func(topic string) bool { return reg.Resolve(topic) == nil },
				"no channel claims"},
			{oneChannel, func(topic string) bool {
				c := reg.Resolve(topic)
				return c != nil && c.Name != "c0"
			}, "a channel it holds no grant on claims"},
		} {
			path := filepath.Join(t.TempDir(), "acl.yaml")
			if err := os.WriteFile(path, []byte(arm.acl+"users:\n  u: [r]\n"), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			f, err := authz.Load(path, reg, 0)
			if err != nil {
				t.Fatalf("load for %v: %v", routes, err)
			}
			rules := authz.New(f, reg)
			for _, filter := range filters {
				if !rules.Allows("u", "u", filter, false) {
					refused++
					continue
				}
				granted++
				for _, topic := range topics {
					if channel.Matches(filter, topic) && arm.leaked(topic) {
						t.Errorf("channels %v: %q was granted and matches %q, which %s",
							routes, filter, topic, arm.what)
						break
					}
				}
			}
		}
	}
	t.Logf("%d filters against %d topics over 6 registries and 2 grants: %d granted, %d refused",
		len(filters), len(topics), granted, refused)
	if granted == 0 || refused == 0 {
		t.Fatalf("%d granted and %d refused: the sweep did not exercise both answers",
			granted, refused)
	}
}

// **A decision costs the same whatever the number of user patterns**, in
// allocations, which is what a sort or a split per decision shows up as.
// entryFor sorted and split every pattern on every publish and delivery:
// 81 allocations a decision with 2 patterns, 1081 with 1001, and 108us.
// Sorted and split once at load, the two files allocate the same. The
// identity matches the last pattern written, so every pattern is walked.
func TestADecisionAllocatesTheSameWhateverTheNumberOfPatterns(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append, Filter: "iot/+/events/+"}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	allocs := func(patterns int) float64 {
		var b strings.Builder
		b.WriteString("roles:\n  r:\n    - channel: events\n      filter: iot/%u/events/+\n" +
			"      allow: [write, read]\n    - topic: alerts/%u/#\n      allow: [write]\n" +
			"users:\n")
		for i := 0; i < patterns-1; i++ {
			fmt.Fprintf(&b, "  \"fleet-%04d-*\": [r]\n", i)
		}
		b.WriteString("  device-7: [r]\n")
		path := filepath.Join(t.TempDir(), "acl.yaml")
		if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		f, err := authz.Load(path, reg, 0)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		r := authz.New(f, reg)
		if !r.Allows("device-7", "device-7", "iot/device-7/events/t", true) {
			t.Fatal("device-7 was refused its own topic, so nothing below measures a grant")
		}
		return testing.AllocsPerRun(100, func() {
			r.Allows("device-7", "device-7", "iot/device-7/events/t", true)
		})
	}
	few, many := allocs(2), allocs(1001)
	t.Logf("a decision allocates %.0f times with 2 patterns and %.0f with 1001", few, many)
	if many != few {
		t.Errorf("a decision allocates %.0f times with 2 patterns and %.0f with 1001: "+
			"something per decision grows with the file", few, many)
	}
}

// RFC 0002 "How deep a topic may be": a filter or topic in the acl_file
// deeper than limits.max_topic_levels could match nothing a client may
// publish, so the rule would grant nothing while reading as though it
// granted something. At the bound it loads; one level past it the file is
// refused, naming the key.
func TestAnACLFilterOrTopicIsHeldToMaxTopicLevels(t *testing.T) {
	deep := func(first string, n int) string { return first + strings.Repeat("/x", n-1) }
	path := filepath.Join(t.TempDir(), "acl.yaml")
	for _, tc := range []struct {
		name, rule string
		refused    bool
	}{
		{"a channel rule's filter of 200 levels", "channel: events\n      filter: " + deep("events", 199) + "/#", false},
		{"a channel rule's filter of 201 levels", "channel: events\n      filter: " + deep("events", 200) + "/#", true},
		{"a topic rule of 200 levels", "topic: " + deep("alerts", 199) + "/#", false},
		{"a topic rule of 201 levels", "topic: " + deep("alerts", 200) + "/#", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "roles:\n  r:\n    - " + tc.rule + "\n      allow: [read]\nusers:\n  c: [r]\n"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := authz.Load(path, registry(t), 200)
			switch {
			case !tc.refused && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.refused && err == nil:
				t.Fatal("loaded")
			case tc.refused && !strings.Contains(err.Error(), "limits.max_topic_levels"):
				t.Fatalf("refused with %v, which does not name limits.max_topic_levels", err)
			}
		})
	}
}
