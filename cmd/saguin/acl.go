package main

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/config"
)

// explainACL answers the question an operator asks at the worst moment:
// **why can device-7 not publish?**
//
// Roles are what save the work - one rule for a fleet rather than ten
// thousand copies of it - and indirection is what they cost. With a flat
// list the answer is a line in a file; with roles it is a pattern match and
// two lookups, done in somebody's head, at the point where a device that
// should be working is not.
//
// So the resolved form is printable, which is the same instinct as
// `--check-config --output`: where behaviour is decided from several places
// at once, print what those places came to. It also gives the tests
// something to assert against that is not the broker's live behaviour.
// **Two writers, because an answer and a refusal are not the same thing.**
// The explanation is what somebody pipes; a refusal is what a script watching
// standard error is looking for, and one writer put "usage:" and "no such
// file" down the pipe as though they were the answer. Which stream a message
// goes to is decided by whether it is one, so a new refusal below cannot
// take the wrong one by inheriting a variable.
func explainACL(args []string, in io.Reader, out, bad io.Writer, piped bool, form outputForm) int {
	if len(args) == 0 || len(args) > 3 {
		fmt.Fprintln(bad, "usage: saguin --acl <config-file> [user-name] [client-id]")
		fmt.Fprintln(bad, "  the configuration names the acl_file and the channels its rules are about")
		fmt.Fprintln(bad, "  the user name is what a client authenticates as - a password-file entry")
		fmt.Fprintln(bad, "  or a certificate's Common Name - and is what every rule is written about")
		fmt.Fprintln(bad, "  the client id is what the device calls its session, and is needed only")
		fmt.Fprintf(bad, "  to resolve a rule that uses %%c\n")
		fmt.Fprintln(bad, "  with no user name, one is read per line from standard input")
		return 1
	}
	cfgPath := args[0]

	cfg, reg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(bad, "%v\n", err)
		return 1
	}
	if cfg.Broker.MQTT.ACLFile == "" {
		// The JSON half says the same thing in a field a script can test.
		// A row of `-` per client would be the reverse of the truth here,
		// and so would an empty grant list: both read as "granted nothing"
		// about a broker that grants everything.
		if form == asJSON {
			if err := jsonLines(out).Encode(authz.NoACLFile()); err != nil {
				fmt.Fprintf(bad, "%v\n", err)
				return 1
			}
			return 0
		}
		fmt.Fprintf(out, "%s names no acl_file, so every authenticated client may do "+
			"anything.\n", cfgPath)
		return 0
	}

	f, err := authz.Load(cfg.Broker.MQTT.ACLFile, reg, cfg.Broker.Limits.Resolve().MaxTopicLevels)
	if err != nil {
		fmt.Fprintf(bad, "%v\n", err)
		return 1
	}

	// **No client id means a list of them on standard input**, one per line,
	// answered a line at a time. A fleet's worth of ids is the case this
	// command is least good at by hand: an operator asking "which of these
	// forty devices can publish" wants one screen, not forty runs of a
	// command that prints a paragraph each.
	if len(args) == 1 {
		if !piped {
			fmt.Fprintln(bad, "no user name given and nothing piped in.")
			fmt.Fprintln(bad, "  saguin --acl <config-file> <user-name> [client-id]   one, explained")
			fmt.Fprintln(bad, "  saguin --acl <config-file> < names.txt               one per line, a line each")
			return 1
		}
		return aclBatch(f, cfg.Broker.Limits.Resolve(), in, out, form)
	}
	identity := args[1]
	// **Empty when it was not given, which leaves `%c` standing.** A rule
	// using it cannot be resolved without one, and printing a grant with the
	// literal `%c` still in it would show a rule that matches nothing as
	// though it were what the client holds. Said out loud below instead.
	clientID := ""
	if len(args) == 3 {
		clientID = args[2]
	}

	grants := f.For(identity, clientID)

	// **The JSON form of one client is the list form's answer plus the two
	// things the paragraph adds**: which patterns matched, and which
	// patterns the file holds. Both are always present rather than only
	// when nothing matched, because a shape that changes with the answer is
	// one a script has to branch on before it can read it.
	if form == asJSON {
		// **`pattern_applied` is always present**, null where nothing
		// matched, for the reason the other two fields are: a shape that
		// changes with the answer is one a script has to branch on before it
		// can read it. Which patterns matched no longer says what a client
		// gets, so a script reading only that field would now be wrong.
		if err := jsonLines(out).Encode(
			authz.Explain(f, cfg.Broker.MQTT.ACLFile, identity, clientID)); err != nil {
			fmt.Fprintf(bad, "%v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(out, "user %q, against %s\n\n", identity, cfg.Broker.MQTT.ACLFile)

	// **Before the grants, because it outranks them.** `client_ids` refuses
	// at CONNECT with 0x86 - the same code a wrong password gets - before a
	// single rule is read. Somebody running this because a device is not
	// working needs that first; everything below it is what the pair would
	// be allowed if it could get in.
	if clientID != "" && !f.AllowsClientID(identity, clientID) {
		fmt.Fprintf(out, "This user name may not be used by client id %q: the entry's "+
			"`client_ids`\nrefuses it and the connection is answered 0x86 at CONNECT, "+
			"before any rule\nbelow is consulted.\n\n", clientID)
	}

	// Which patterns matched, before what they granted: an operator whose
	// device is refused everything is usually looking at a pattern that did
	// not match, and that is the first thing to be able to see.
	//
	// **And which of them is in force**, which is the question precedence
	// created. One entry applies - the pattern spelling the id out most
	// exactly - and the others match and do nothing. That is the only way an
	// acl_file can now take something away without saying so, because a
	// shadowed entry cannot be refused at startup: it is the intended
	// behaviour. So it is named here, where somebody is already looking
	// because a device is not doing what its file appears to say.
	applied, _ := f.PatternFor(identity)
	matched := authz.MatchingPatterns(f, identity)
	if len(matched) == 0 {
		fmt.Fprintln(out, "no user pattern matches this name, so it is granted nothing.")
		fmt.Fprintln(out, "patterns in the file:")
		for _, pattern := range sortedKeys(f.Users) {
			fmt.Fprintf(out, "  %s\n", pattern)
		}
		return 0
	}
	fmt.Fprintln(out, "patterns matched:")
	// The one in force first, then the rest: the answer before the
	// alternatives, so a reader who stops at the first line has the truth.
	for _, pattern := range orderApplied(matched, applied) {
		note := fmt.Sprintf("shadowed by %q", applied)
		if pattern == applied {
			note = "applies"
		}
		fmt.Fprintf(out, "  %-24s -> %-20s %s\n",
			pattern, strings.Join(f.Users[pattern].Roles, ", "), note)
	}
	if len(matched) > 1 {
		fmt.Fprintf(out, "\nOnly %q is in force: its roles and its limits are the whole "+
			"of what this\nclient gets, and a shadowed entry adds nothing to them.\n",
			applied)
	}

	// **The limits beside the grants, because they answer one question.**
	// An entry naming limits replaces the broker-wide figures, up or down -
	// so an exception written to give a gateway more can also, by accident,
	// undo the bound protecting the box.
	//
	// **Both figures, always, with where each came from.** Naming the two
	// configuration keys and leaving the operator to go and read them is
	// what this said before, and it made the one question this command
	// exists to answer - what is this client actually held to - a thing to
	// go and work out somewhere else. The source matters beside the number
	// because 200 from an entry and 200 broker-wide are the same figure in
	// different situations, and only one of them changes when the entry is
	// deleted.
	rate, bytes, from := authz.EffectiveLimits(f, cfg.Broker.Limits.Resolve(), identity)
	fmt.Fprintln(out, "\npublish limits, what this client is held to:")
	fmt.Fprintf(out, "  %-24s %-10s from %s\n", "messages a second", boundOrNone(int64(rate)), from)
	fmt.Fprintf(out, "  %-24s %-10s from %s\n", "bytes a second", boundOrNone(bytes), from)
	if _, named := f.LimitsFor(identity); named {
		// **The half an operator does not expect.** The entry replaces the
		// pair rather than merging with it, so a figure it leaves out is
		// unbounded rather than inherited - and the one they left out is
		// exactly the one they were not thinking about.
		if rate <= 0 || bytes <= 0 {
			fmt.Fprintf(out, "  note: an entry naming limits supplies both figures, so the "+
				"one it leaves out\n        is unbounded rather than taken from the "+
				"broker-wide pair.\n")
		}
	}
	for _, pattern := range matched {
		if pattern == applied || f.Users[pattern].Limits == nil {
			continue
		}
		fmt.Fprintf(out, "  note: %q carries limits and matches this client, but %q "+
			"applies.\n", pattern, applied)
	}

	// **An unresolved `%c` is named rather than printed as a grant.** A rule
	// using it needs the client id, and the literal left standing matches no
	// topic at all - so shown without a word it is the exact defect RFC 0002
	// records for `%u`: a rule written on purpose, a configuration reporting
	// ok, and a fleet refused 0x87.
	if clientID == "" {
		for _, g := range grants {
			if !strings.Contains(g.Topic, "%c") && !strings.Contains(g.Filter, "%c") {
				continue
			}
			fmt.Fprintf(out, "\nsome rules are scoped by client id and none was given, so "+
				"%%c is left\nstanding below and those lines match nothing as printed. "+
				"Give one:\n  saguin --acl <config-file> %s <client-id>\n", identity)
			break
		}
	}

	// **A rule withheld for this name is named, and why**, rather than
	// dropped from the list: an operator reading the grants would otherwise
	// see a rule they wrote grant nothing and nothing saying so.
	withheld := f.Withheld(identity, clientID)
	if len(withheld) > 0 {
		fmt.Fprintf(out, "\nwithheld: these rules name %%u or %%c, and the name they would put in holds\n")
		fmt.Fprintln(out, "+, # or /, which a topic filter reads as a wildcard or a level, so they grant")
		fmt.Fprintln(out, "this client nothing (mosquitto and EMQX refuse the same names):")
		for _, g := range withheld {
			where := authz.Kind(g) + " " + authz.Named(g)
			if g.Filter != "" {
				where += " matching " + g.Filter
			}
			fmt.Fprintf(out, "  %s  %s  (role %s)\n", where, strings.Join(withDenials(g.Verbs, g.Denies), ", "), g.Role)
		}
	}

	fmt.Fprintln(out, "\neffective grants:")
	if len(grants) == 0 {
		if len(withheld) > 0 {
			fmt.Fprintln(out, "  (none: every rule it matched is withheld above)")
		} else {
			fmt.Fprintln(out, "  (none: the roles it matched carry no rules)")
		}
		return 0
	}
	// Measured rather than guessed at: a suffix carrying a long %u makes a
	// fixed column wrap into the next one, and a table that only lines up
	// for short names is one somebody stops reading.
	type row struct{ where, verbs, role string }
	rows := make([]row, 0, len(grants))
	width := 0
	for _, g := range grants {
		// Which kind of rule this is comes from authz, which is where the
		// JSON form and the columns ask it too. Only the spelling below is
		// this table's own.
		r := row{
			where: authz.Kind(g) + " " + authz.Named(g),
			verbs: strings.Join(withDenials(g.Verbs, g.Denies), ", "), role: g.Role,
		}
		// **"matching", not "under".** The filter is compared against the
		// whole topic now, so a word implying a path below the channel name
		// would describe the mechanism this replaced.
		if g.Filter != "" {
			r.where += " matching " + g.Filter
		}
		if len(r.where) > width {
			width = len(r.where)
		}
		rows = append(rows, r)
	}
	for _, r := range rows {
		fmt.Fprintf(out, "  %-*s  %s  (role %s)\n", width, r.where, r.verbs, r.role)
	}

	// **What `%c` gives, said where somebody is reasoning about their own
	// file.** A grant reading `iot/cohort-north/health/north-17` looks like
	// per-device isolation and is not: nothing proves a client id, so any
	// device holding this credential can take another's. It separates
	// devices from each other's topics by mistake rather than by force,
	// which is worth having and is not the same thing.
	//
	// Read off the rules rather than the grants, because a resolved grant
	// no longer holds the `%c` that produced it - printing this only when a
	// client id was omitted would say it exactly where it matters least.
	if scopedByClientID(f, identity) {
		fmt.Fprintf(out, "\nnote: rules scoped by client id (%%c) keep devices sharing this "+
			"credential out of\n      each other's topics by mistake rather than by force. "+
			"Nothing proves a client\n      id, so a device holding this credential can "+
			"take another's - one credential\n      per device is what makes this a rule "+
			"instead of a convention.\n")
	}
	return 0
}

// scopedByClientID reports whether any rule the applying entry reaches is
// written with `%c`.
func scopedByClientID(f *authz.File, identity string) bool {
	pattern, ok := f.PatternFor(identity)
	if !ok {
		return false
	}
	for _, role := range f.Users[pattern].Roles {
		for _, r := range f.Roles[role] {
			if strings.Contains(r.Topic, "%c") || strings.Contains(r.Filter, "%c") {
				return true
			}
		}
	}
	return false
}

// aclBatch answers a list of client ids, one per line in, with one
// tab-separated line per grant out.
//
// **Seven columns, and the first is always the input**, so a line stands on
// its own however the output is cut about: the name asked for, the role
// that granted it, whether the rule is about a channel or a broadcast
// topic, which one, the verbs, and the publishes and bytes a second the
// role allows. `%u` is already substituted, because the
// substituted form is what the broker will compare against and the
// unsubstituted one is what an operator can read off the file themselves.
//
// A client no pattern matches gets one line with `-` in every answer
// column rather than no line at all. Silence would make "granted nothing"
// and "I did not ask about that one" the same output, and they are the two
// answers somebody piping a fleet's worth of ids most needs to tell apart.
//
// **A header line names the columns**, because the reader of this form is a
// person: the whole reason it exists is that somebody asking which of forty
// devices may publish wants one screen rather than forty paragraphs, and a
// screen of bare columns leaves them counting tabs to find out which one is
// the role. It starts with `#` so that it is skipped by the same rule that
// skips a comment in the input - an operator asking the same question again
// cuts the first column and pipes it back, and the header must not become
// one more question - and so that anyone wanting bare columns can drop it
// with the filter they already have.
func aclBatch(f *authz.File, wide config.Resolved, in io.Reader, out io.Writer, form outputForm) int {
	enc := jsonLines(out)
	if form == asColumns {
		fmt.Fprintf(out, "# user\trole\tkind\tsubject\tverbs\trate\tbytes\n")
	}
	s := bufio.NewScanner(in)
	// A client id is bounded by limits.max_client_id_length, which defaults
	// to 256 and may be raised; this is well past any of it, and a line
	// longer than the buffer would otherwise be reported as end of input.
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// **A line may name both, and most name one.** A rule using `%c`
		// cannot be resolved without a client id, so a fleet's worth of
		// them has to be answerable the same way one is - and the input
		// keeps the shape it had, because a line with no second field is
		// exactly what every file written for this command already holds.
		identity, clientID, _ := strings.Cut(line, "\t")
		if clientID == "" {
			identity, clientID, _ = strings.Cut(line, " ")
		}
		identity, clientID = strings.TrimSpace(identity), strings.TrimSpace(clientID)
		for _, a := range aclAnswers(f, wide, identity, clientID) {
			if form == asJSON {
				if err := enc.Encode(a); err != nil {
					return 1
				}
				continue
			}
			// The verbs column takes a `-` for the same reason the other
			// three do: an empty fifth column would make a client granted
			// nothing and one granted a rule with no verbs look alike, and
			// a trailing tab with nothing after it is the one difference a
			// reader cannot see.
			verbs := strings.Join(withDenials(a.Verbs, a.Denies), ",")
			if verbs == "" {
				verbs = "-"
			}
			if _, err := fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", a.User,
				dash(a.Role), dash(a.Kind), dash(a.Subject), verbs,
				boundOrNone(deref(a.Rate)), boundOrNone(deref(a.Bytes))); err != nil {
				return 1
			}
		}
	}
	if err := s.Err(); err != nil {
		return 1
	}
	return 0
}

// authz.EffectiveLimits is what a user is actually held to, and where the
// figures came from, and aclBatch prints both.
//
// **Both figures come from one place.** An entry naming `limits:` replaces
// the broker-wide pair wholesale rather than merging with it, so an entry
// writing only `publish_rate` leaves that client's bytes unbounded - the
// broker-wide byte figure does not reach it. That is what the broker does
// and it is the surprising half, which is the reason to print both numbers
// beside each other rather than only the one somebody wrote.
//
// Zero is no bound, which is the permissive end. It is not a figure an
// operator can write - a `0` in the file is refused at startup, because
// there it would mean "refuse every publish" - so it can only ever have got
// here by nobody setting one.

// boundOrNone prints a limit in words where there is none, because a column
// of integers with a `0` in it reads as a bound of zero - which is the
// opposite, and is the one figure the configuration refuses.
func boundOrNone(n int64) string {
	if n <= 0 {
		return "no bound"
	}
	return fmt.Sprintf("%d", n)
}

// aclAnswer is one grant, worked out and not yet written: the seven columns
// (the column form folds Denies into its verbs), where a nil is the `-` the
// column form prints and the `null` the JSON form does. A client no pattern
// matches is one answer with nothing in it rather than no answer at all -
// silence would make "granted nothing" and "I never asked about that one"
// the same output.
type aclAnswer struct {
	User    string   `json:"user"`
	Role    *string  `json:"role"`
	Kind    *string  `json:"kind"`
	Subject *string  `json:"subject"`
	Verbs   []string `json:"verbs"`
	// Denies is what a `broker: features` rule takes away, `[]` elsewhere.
	Denies []string `json:"denies"`

	// Rate and Bytes are what this user is held to, per second, and are
	// **null where there is no bound at all**.
	//
	// Null is the permissive end here, which is the reverse of what it
	// means in the four fields above, and it is said in RFC 0002 for that
	// reason. The alternative was `0`, and a zero in a column of limits
	// reads as a bound of zero - the one figure the configuration refuses,
	// because it would mean refusing every publish.
	Rate  *int64 `json:"publish_rate"`
	Bytes *int64 `json:"publish_bytes"`
}

// aclAnswers is what one client came to. Both forms render this, so the
// columns and the JSON cannot become two accounts of what a client may do.
func aclAnswers(f *authz.File, wide config.Resolved, identity, clientID string) []aclAnswer {
	// **Repeated on every row of a user, as the name is.** The figures
	// belong to the user rather than to the grant, and the format's own
	// rule is that a line stands on its own however the output is cut
	// about - which is why the first column is always the input too.
	rate, bytes, _ := authz.EffectiveLimits(f, wide, identity)

	// **The grants come from authz.Describe rather than from a derivation
	// here**, because `/v1/operations/acl` answers the same question and two
	// derivations of "channel or topic, and what is it called" would agree
	// until somebody changed one. What this form adds is the user and the
	// limits on every row; what a rule *means* is settled in one place.
	described := authz.Describe(f, identity, clientID)
	if len(described) == 0 {
		return []aclAnswer{{User: identity, Verbs: []string{}, Denies: []string{},
			Rate: orNoBound(int64(rate)), Bytes: orNoBound(bytes)}}
	}
	rows := make([]aclAnswer, 0, len(described))
	for _, g := range described {
		rows = append(rows, aclAnswer{
			User: identity, Role: g.Role, Kind: g.Kind, Subject: g.Subject,
			Verbs: g.Verbs, Denies: g.Denies, Rate: orNoBound(int64(rate)), Bytes: orNoBound(bytes),
		})
	}
	return rows
}

// withDenials is a grant's verbs and then what it denies, each denial spelled
// `deny <capability>`, so the one column an operator reads to check a rule
// shows a denial rather than a rule that seems to allow nothing.
func withDenials(verbs, denies []string) []string {
	out := append([]string(nil), verbs...)
	for _, d := range denies {
		out = append(out, "deny "+d)
	}
	return out
}

// orNoBound is a limit as JSON: the figure, or null where nothing bounds it.
func orNoBound(n int64) *int64 {
	if n <= 0 {
		return nil
	}
	return &n
}

// deref is a limit back as a number, with absent meaning no bound.
func deref(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}

// authz.MatchingPatterns is every client entry whose pattern matches this
// id, in a stable order. Only one of them is in force; the rest are what an
// operator has to be able to see, because a pattern that matched and lost
// looks exactly like one that applied from inside the file.

// orderApplied puts the entry in force first and leaves the rest as they
// were, so a reader who stops at the first line has the answer rather than
// whichever pattern sorts earliest.
func orderApplied(matched []string, applied string) []string {
	out := make([]string, 0, len(matched))
	out = append(out, applied)
	for _, p := range matched {
		if p != applied {
			out = append(out, p)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
