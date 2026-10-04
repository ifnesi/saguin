// Package authz reads the file that says what each client may do.
//
// RFC 0002 "What a client may do" is the specification. The shape is roles
// carrying rules, and clients matched to roles by pattern - because a fleet
// of ten thousand devices that are all the same kind of thing needs one
// rule, not ten thousand copies of it, and every copy is a chance to get
// one wrong.
//
// **Nothing here decides anything about a live client.** This package reads
// the file and refuses one that cannot mean what it says; the broker asks
// the rules, and only after its own structural refusals have had their say
// (invariants 4 and 11).
package authz

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
)

// channelMatches is the one MQTT filter matcher in the tree: the same code
// the delivery path uses, so a rule grants exactly what a subscription
// would reach. A second implementation would be two ways to be wrong.
var channelMatches = channel.Matches

// channelIntersects answers whether two filters can name any topic in
// common - the same code the registry uses, so a rule is refused exactly
// where it could never have matched.
var channelIntersects = channel.Intersects

// channelExpand resolves a `{a,b}` level into the plain filters it stands
// for - again the registry's own, so that a rule written in the notation a
// channel is written in means there what it means here. RFC 0002 says
// nothing past the loader ever sees a brace; this file is part of the
// loader, and for a while it was the one part that did.
var channelExpand = channel.Expand

// anyIntersects reports whether any spelling of one filter can name a topic
// any spelling of the other can. Two braced filters are two sets of plain
// ones, and they overlap when any pair does.
func anyIntersects(a, b []string) bool {
	for _, one := range a {
		for _, other := range b {
			if channelIntersects(one, other) {
				return true
			}
		}
	}
	return false
}

// Rule is one grant. It names exactly one of three things: a channel, a
// broadcast topic, or a facility of the broker.
//
// The first two never appear together, because every topic resolves to
// exactly one channel or to broadcast (invariant 12) - so those two kinds
// partition the topic space between them and cannot disagree about
// anything. The third is outside that partition: it governs no topic, so
// no filter can reach what it grants and it can conflict with neither.
type Rule struct {
	Channel string `yaml:"channel"`
	Topic   string `yaml:"topic"`

	// Broker names a facility of the broker itself, and is the one rule
	// kind that is not about the topic tree at all. There is one facility,
	// `sessions`, and one verb on it: `disconnect`.
	//
	// **It is a kind of its own rather than a reserved topic** because a
	// topic is something a wide filter can reach. `topic: "#"` is how an
	// operator says "any broadcast topic" and the matcher has no `$`
	// exclusion, so a control topic spelled into this file would be
	// conferred by a rule nobody thought was about hanging clients up. A
	// rule kind cannot be reached by any filter.
	Broker string `yaml:"broker"`

	Filter string   `yaml:"filter"`
	Allow  []string `yaml:"allow"`

	// Deny takes away a feature a client would otherwise have, and is written
	// only on a `broker: features` rule: `persistent` is a session that
	// outlives its connection, `will` is a Last Will, `share` is a shared
	// subscription, and `retained` is a retained value on a broadcast topic.
	// Each is named for the broker block that configures it.
	//
	// **A feature, not a topic, and so not a grant.** Every other word in
	// this file adds something a client may do to somebody's records; these
	// are things every client can do unless told otherwise, because MQTT
	// gives them to any client. A client is denied one when any role it
	// holds denies it, whatever its other roles allow.
	Deny []string `yaml:"deny"`

	// RetiredSuffix exists to be refused with a sentence rather than by the
	// decoder, for the same reason RetiredClients does - and it was left out
	// when `clients:` was given one, on the same morning, which is how a
	// pair of retired keys ended up with one explanation between them.
	//
	// `suffix:` matched the part of a topic after the channel's own filter;
	// `filter:` matches the whole topic. Left to KnownFields the operator
	// gets `field suffix not found in type authz.Rule`, which names a Go
	// type and does not say what to write.
	RetiredSuffix string `yaml:"suffix"`
}

// File is an acl_file as it is written.
type File struct {
	Roles map[string][]Rule `yaml:"roles"`
	Users map[string]User   `yaml:"users"`

	// RetiredClients exists to be refused with a sentence rather than by
	// the decoder, and is read nowhere else.
	//
	// This block was called `clients:` until its keys were understood to be
	// user names, so every acl_file written before that names it. Left to
	// KnownFields the operator gets `field clients not found in type
	// authz.File`, which names a Go type and does not say what to do - for a
	// file that was correct yesterday and needs one word changed.
	RetiredClients map[string]User `yaml:"clients"`

	// patterns is Users' keys in the order entryFor walks them, each split
	// on its `*`s, made once when the file loads - see compilePatterns.
	patterns []userPattern
}

// userPattern is a users key ready to be matched: the key, its literal runs
// between `*`s, and how much of an id it spells out.
type userPattern struct {
	text  string
	parts []string
	spec  int
}

// compilePatterns sorts and splits every users key once. **Once per load,
// never per decision**: entryFor runs on every publish and every delivery,
// and sorting and splitting a thousand patterns there cost 108us a publish
// and 1081 allocations - on a file that changes only when it is re-read.
func compilePatterns(users map[string]User) []userPattern {
	out := make([]userPattern, 0, len(users))
	for _, key := range sorted(users) {
		out = append(out, userPattern{text: key, parts: strings.Split(key, "*"), spec: specificity(key)})
	}
	return out
}

// User is what one pattern grants and what it bounds.
//
// **Two forms, and the short one is not a shorthand to be tidied away.** A
// file that only assigns roles reads as it always did:
//
//	users:
//	  demo: [tour]
//	  "device-*":
//	    roles: [device]
//	    limits:
//	      publish_rate:  100
//	      publish_bytes: 64KiB
//
// The alternative was a second top-level block keyed by the same patterns,
// which would have every operator writing each device pattern twice - two
// places to keep in step, and a typo in the second one silently meaning no
// limit at all. One pattern, one entry, roles and limits together.
type User struct {
	Roles  []string    `yaml:"roles"`
	Limits *UserLimits `yaml:"limits"`

	// ClientIDs is which client ids may log in under this user name.
	//
	// **A user name and a client id are two different strings in one
	// CONNECT packet, and only one of them is proved.** The user name is
	// checked against a password or carried by a verified certificate; the
	// client id is chosen by the device and nothing verifies it. Everything
	// else in this file is about the proved one, which is why a rule can be
	// trusted at all.
	//
	// This is where an operator ties them together. Absent, any client id
	// may use the credential, which is what every file written before this
	// key existed means and what most deployments want.
	//
	// **It is what makes a cohort possible.** A thousand sensors on five
	// credentials is an operator's choice - five passwords to manage rather
	// than a thousand - and the cost is that the broker cannot tell one
	// northern sensor from another. `client_ids: "north-*"` restores the
	// boundary that matters: a device holding the northern credential
	// cannot connect as a southern one. Inside the cohort it cannot, and a
	// broker cannot fix that, because those devices share a secret and are
	// therefore one principal.
	//
	// `%u` is substituted, so `client_ids: "%u"` says "connect under the
	// name you logged in as" - one line for a fleet of any size, and the
	// thing that makes a client id worth trusting in a log line.
	ClientIDs clientIDs `yaml:"client_ids"`
}

// UserLimits is what one client may send per second. Absent means the
// broker-wide figure applies; see config.Limits.
type UserLimits struct {
	// PublishRate is messages a second, and PublishBytes is bytes a second.
	//
	// **Both, because they bound different things and either alone leaves a
	// hole.** A thousand one-byte publishes cost a kilobyte and a thousand
	// topic lookups, permission checks and store writes - which is what
	// actually saturates saguin, and why its own benchmark is in messages a
	// second. Ten one-megabyte publishes cost ten operations and ten
	// megabytes. A device can exhaust the broker either way.
	//
	// Either may be written alone. Whichever is reached first refuses.
	PublishRate  *int    `yaml:"publish_rate"`
	PublishBytes *string `yaml:"publish_bytes"`
}

// UnmarshalYAML accepts a bare list of roles as well as the mapping.
//
// The short form is what every acl_file written before limits existed
// holds, and it stays correct: a user entry is most often nothing but the
// roles it is given, and making those files say `roles:` for no reason
// would be churn charged to every operator for one feature most of them
// will not use.
func (c *User) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		return value.Decode(&c.Roles)
	}
	type plain User // no recursion back into this method
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*c = User(p)
	// **`client_ids:` with nothing under it is the key written, not the key
	// left out**, and nothing below this line can tell the two apart: yaml
	// does not call an unmarshaler for a null value, so the field is the
	// same nil either way. They mean the same thing to whoever typed them -
	// a restriction begun and not filled in - so they are made the same
	// thing here, and `client_ids: []` is already refused at startup with a
	// sentence saying why.
	if c.ClientIDs == nil && writesNullClientIDs(value) {
		c.ClientIDs = clientIDs{}
	}
	return nil
}

// writesNullClientIDs reports whether this entry has a `client_ids` key
// whose value is empty, which the decoded struct cannot show.
func writesNullClientIDs(entry *yaml.Node) bool {
	if entry.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(entry.Content); i += 2 {
		if entry.Content[i].Value == "client_ids" && entry.Content[i+1].Tag == "!!null" {
			return true
		}
	}
	return false
}

// clientIDs is one pattern or a list of them, because a cohort is usually
// one and occasionally two - a fleet plus the spare that replaced a failed
// unit. Splitting an entry in half to say so would mean writing its roles
// and its limits twice, and the second copy is where they drift apart.
type clientIDs []string

func (c *clientIDs) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		var one string
		if err := value.Decode(&one); err != nil {
			return err
		}
		*c = clientIDs{one}
		return nil
	}
	var many []string
	if err := value.Decode(&many); err != nil {
		return err
	}
	*c = many
	return nil
}

// **One pair of verbs everywhere, and three words that are not a
// direction.** `write` puts something in and `read` takes it out, on every
// channel type and on broadcast alike, so an operator writing a rule never
// has to know which vocabulary a channel speaks before they can say which
// way the data goes. What this replaced spelled that one act four ways -
// `read`, `get`, `consume`, `subscribe` - and reading a rule meant looking
// up the channel it named to find out what its words meant.
//
// `seek` and `delete` are not directions. Moving a consumer's position and
// removing a key are acts of their own, and a client may hold either
// without the other.
//
// **`consume` is the irregular one, and it is here to stop a mistake.**
// Reading an append channel costs nobody anything: ten clients read the
// same records and each keeps its own place. Taking a job is the opposite -
// the record is held for one worker and no other sees it. Spelled `read`,
// which everywhere else in computing means look-do-not-touch, a dashboard
// granted it to watch the queue would drain production instead: every job
// it took stalling, redelivering, exhausting its attempts and being
// dead-lettered, with the broker reporting success throughout. That is
// invariant 11's failure reached through the ACL rather than through a
// wildcard.
//
// One irregular cell is the cheap side of that trade. `read` on a queue is
// refused at startup naming what a queue takes; a uniform `read` that
// silently meant "take" is not discovered until the work is gone.
var (
	appendVerbs    = []string{"write", "read", "seek"}
	latestVerbs    = []string{"write", "read", "delete"}
	queueVerbs     = []string{"write", "consume"}
	broadcastVerbs = []string{"write", "read"}

	// **The broker's own verbs, which name no channel.** Hanging a client
	// up is not an act on anybody's records, so there is nothing for it to
	// ride on and it gets a vocabulary of its own.
	brokerVerbs = []string{"disconnect"}

	// **What a `broker: features` rule may deny**, each named for the broker
	// block that configures it. A denied client is answered as a broker
	// without that feature would answer it (RFC 0002 "Taking a feature
	// away"): `persistent` is accepted with a Session Expiry Interval of 0,
	// `will` refuses a CONNECT carrying one `0x87`, `share` is told Shared
	// Subscription Available 0, `retained` is refused a retained value on a
	// broadcast topic `0x9A`, and `qos2` is told Maximum QoS 1.
	featureDenials = []string{"persistent", "will", "share", "retained", "qos2"}

	// The facilities a `broker:` rule may name. A list rather than
	// constants so that the finding for a misspelling can say what was
	// available.
	brokerFacilities = []string{channel.SessionsFacility, channel.FeaturesFacility}
)

// verbsFor is what a rule of this kind may allow. **The empty type is a
// broadcast rule** - which is what a rule naming a topic rather than a
// channel is - so every rule kind this file can hold is answered here, and
// the nil is left for a channel type that does not exist rather than for a
// rule kind nobody wired up.
func verbsFor(t channel.Type) []string {
	switch t {
	case channel.Append:
		return appendVerbs
	case channel.Latest:
		return latestVerbs
	case channel.Queue:
		return queueVerbs
	case "":
		return broadcastVerbs
	}
	return nil
}

// Load reads an acl_file and refuses one that cannot mean what it says,
// holding every filter and topic in it to maxLevels (limits.max_topic_levels;
// below 1 bounds nothing).
//
// **Every finding is collected rather than the first being returned.** An
// operator fixing an authorization file one error per run edits it blind,
// and a broker that refuses the file goes on running what it had - so every
// trip round is another signal, and was another restart before there was
// one.
func Load(path string, reg *channel.Registry, maxLevels int) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// **The caller names the key, not this.** Both callers already do -
		// `broker.mqtt.acl_file: %v` in the named-files check, and "cannot
		// read the authorization file" in main - so naming it here as well
		// produced `broker.mqtt.acl_file: acl_file: open …`, where the
		// password file beside it reads clean.
		return nil, err
	}

	var f File
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	// A key saguin half-understands is worse than one it refuses: a typo in
	// `allow` is a grant that silently is not there.
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if findings := f.validate(reg, maxLevels); len(findings) > 0 {
		return nil, fmt.Errorf("%s:\n  - %s", path, strings.Join(findings, "\n  - "))
	}
	f.patterns = compilePatterns(f.Users)
	return &f, nil
}

// validate reports everything wrong with a file, in a stable order so that
// two runs over one file say the same thing in the same sequence.
func (f *File) validate(reg *channel.Registry, maxLevels int) []string {
	var findings []string

	for _, role := range sorted(f.Roles) {
		for i, r := range f.Roles[role] {
			if r.RetiredSuffix != "" {
				return []string{fmt.Sprintf(
					"roles %q rule %d names a suffix:. It is filter: now, and the "+
						"difference is what it matches: suffix: was the part of a "+
						"topic after the channel's own filter, where filter: is "+
						"matched against the whole topic. Write the filter the "+
						"topics have, not the tail - a channel filtered "+
						"iot/+/events/+ takes filter: iot/north/events/+",
					role, i+1)}
			}
		}
	}

	if len(f.RetiredClients) > 0 {
		return []string{"names a clients: block. It is users: now, because its keys are " +
			"user names - the name a client authenticates under, from the password file " +
			"or a certificate's Common Name - and never client ids, which no rule here " +
			"reads. Rename the key; nothing inside the block changes"}
	}

	if len(f.Roles) == 0 && len(f.Users) == 0 {
		return []string{"names no roles and no clients, so it grants nothing to nobody. " +
			"Remove the acl_file, or write what it is for"}
	}

	for _, name := range sorted(f.Roles) {
		rules := f.Roles[name]
		if len(rules) == 0 {
			findings = append(findings, fmt.Sprintf(
				"role %q carries no rules, so every client given it is granted nothing. "+
					"A role that grants nothing reads as one that grants something", name))
		}
		for i, r := range rules {
			findings = append(findings, r.validate(reg, fmt.Sprintf("role %q rule %d", name, i+1), maxLevels)...)
		}
	}

	// **Two patterns equally exact and both matching one id have no honest
	// answer**, so the broker does not start rather than pick one. `a-*` and
	// `*-b` both match `a--b` and neither spells more of it out; resolving
	// that quietly is how a rule an operator wrote stops applying without
	// anything saying so. It is the same refusal RFC 0002 makes for two
	// channels carrying one filter, and it names both.
	//
	// **Every entry, not only the ones carrying limits.** While roles
	// unioned across every matching pattern they could not disagree, so a
	// tie mattered only where limits were written. Now one entry supplies
	// the roles as well, and a tie decides which - silently, and differently
	// as soon as anything about either pattern changes.
	patterns := sorted(f.Users)
	for i, a := range patterns {
		for _, b := range patterns[i+1:] {
			if specificity(a) != specificity(b) || !patternsCanCollide(a, b) {
				continue
			}
			findings = append(findings, fmt.Sprintf(
				"users %q and %q both match the same client ids and neither spells one "+
					"out more exactly, so which entry applies - its roles and its "+
					"limits - would be decided by nothing an operator can read: make one "+
					"of them more specific, or merge them into one entry", a, b))
		}
	}

	for _, pattern := range sorted(f.Users) {
		roles := f.Users[pattern].Roles
		if len(roles) == 0 {
			findings = append(findings, fmt.Sprintf(
				"users %q is given no roles, which grants it nothing: remove the line, "+
					"or name the roles it should have", pattern))
		}
		for i, id := range f.Users[pattern].ClientIDs {
			// **`%c` has nothing to stand for here.** Everywhere else in
			// this file it means "the id this client connected under", and
			// this key is what decides whether that connection happens -
			// so there is no id yet to substitute. Left alone it is not
			// inert: it matches the one device whose id is literally `%c`
			// and refuses the whole fleet 0x86, which reads in a log
			// exactly like a cohort whose devices are misconfigured.
			if strings.Contains(id, "%c") {
				findings = append(findings, fmt.Sprintf(
					"users %q client_ids entry %d writes %%c, which is the client "+
						"id a client connected under - and this key is what decides "+
						"whether it may connect, so there is none yet. Only %%u is "+
						"substituted here; write the pattern the ids match, or %%u "+
						"for the user name itself", pattern, i+1))
			}
			if strings.TrimSpace(id) == "" {
				findings = append(findings, fmt.Sprintf(
					"users %q client_ids entry %d is empty, which matches no client id "+
						"at all: every device holding this credential is refused at "+
						"connect. Remove the entry, or write the pattern it should be",
					pattern, i+1))
			}
		}
		if ids := f.Users[pattern].ClientIDs; ids != nil && len(ids) == 0 {
			findings = append(findings, fmt.Sprintf(
				"users %q writes client_ids with nothing in it, which reads as a "+
					"restriction and is one that admits nobody: every device holding "+
					"this credential is refused at connect. Remove the key to allow any "+
					"client id", pattern))
		}
		if lim := f.Users[pattern].Limits; lim != nil {
			if lim.PublishRate != nil && *lim.PublishRate < 1 {
				findings = append(findings, fmt.Sprintf(
					"users %q limits.publish_rate %d: it is how many messages a second "+
						"this client may publish, so at or below zero every publish is "+
						"refused; omit the key to take the broker-wide figure",
					pattern, *lim.PublishRate))
			}
			if lim.PublishBytes != nil {
				// ParseBytes refuses zero, a negative and an overflow itself.
				if _, err := config.ParseBytes(*lim.PublishBytes); err != nil {
					findings = append(findings, fmt.Sprintf(
						"users %q limits.publish_bytes %q: %v; write a size such as 64KiB",
						pattern, *lim.PublishBytes, err))
				}
			}
		}
		for _, want := range roles {
			if _, ok := f.Roles[want]; !ok {
				findings = append(findings, fmt.Sprintf(
					"users %q is given role %q, which the roles: block does not define. "+
						"A role that does not exist grants nothing and reads as though it "+
						"does",
					pattern, want))
			}
		}
	}

	return findings
}

// validate reports what is wrong with one rule, named by where it sits.
func (r Rule) validate(reg *channel.Registry, where string, maxLevels int) []string {
	var findings []string

	ch, topic := strings.TrimSpace(r.Channel), strings.TrimSpace(r.Topic)
	brk := strings.TrimSpace(r.Broker)
	// **Named rather than counted**, so the finding can say which two were
	// written. A count says "name one of three" about a file that names two,
	// and the operator has to work out which two they are.
	switch named := len(subjectsNamed(ch, topic, brk)); {
	case named == 0:
		// A filter beginning with `#` is the likely way to arrive here and
		// the hardest to see: YAML reads an unquoted # as the start of a
		// comment, so `topic: #` is an empty value rather than the broadest
		// broadcast filter somebody wrote.
		return []string{fmt.Sprintf(
			"%s names neither a channel, a topic nor a broker facility, so there is "+
				"nothing for it to govern. A filter carrying # must be quoted, or YAML "+
				"reads it as a comment and the value as empty", where)}
	case named > 1 && ch != "" && topic != "" && brk == "":
		return []string{fmt.Sprintf(
			"%s names both channel %q and topic %q. Every topic resolves to one channel "+
				"or to broadcast, so a rule is about one or the other", where, ch, topic)}
	case named > 1:
		// **The broker kind mixed with either of the others.** It governs no
		// topic at all, so a rule holding it and a channel is two rules
		// written as one - and the verbs of the two kinds do not overlap, so
		// whichever list the allow: line was checked against, half of it
		// would be refused.
		return []string{fmt.Sprintf(
			"%s names broker facility %q beside %s. A `broker:` rule governs no topic, "+
				"so it cannot also be about one: write them as two rules",
			where, brk, describeOther(ch, topic))}
	}

	if brk != "" {
		if r.Filter != "" {
			findings = append(findings, fmt.Sprintf(
				"%s is a broker rule and carries a filter, which narrows a rule within a "+
					"channel. A broker facility has no topics to narrow", where))
		}
		if !contains(brokerFacilities, brk) {
			return append(findings, fmt.Sprintf(
				"%s names broker facility %q, which is not one of %s", where, brk,
				strings.Join(brokerFacilities, ", ")))
		}
		// **Features only deny, and sessions only allow.** A feature is
		// something every client has until a role takes it away, so there is
		// nothing for `allow:` to add; hanging a client up is an act a role
		// grants, so there is nothing for `deny:` to take.
		if brk == channel.FeaturesFacility {
			if len(r.Allow) > 0 {
				findings = append(findings, fmt.Sprintf(
					"%s allows %s on broker facility %q, which takes deny: and nothing else: a "+
						"feature is something every client has until a role takes it away",
					where, strings.Join(r.Allow, ", "), brk))
			}
			if len(r.Deny) == 0 {
				findings = append(findings, fmt.Sprintf(
					"%s denies nothing, which is what a client has without it", where))
			}
			return append(findings, checkDenials(r.Deny,
				fmt.Sprintf("%s on broker facility %q", where, brk))...)
		}
		if len(r.Deny) > 0 {
			findings = append(findings, fmt.Sprintf(
				"%s denies %s on broker facility %q, and deny: is written only on a "+
					"`broker: features` rule. Move it there: `broker: sessions` grants disconnect "+
					"and nothing else", where, strings.Join(r.Deny, ", "), brk))
		}
		if len(r.Allow) == 0 && len(r.Deny) == 0 {
			findings = append(findings, fmt.Sprintf(
				"%s allows nothing and denies nothing, which is what a client has without it", where))
		}
		return append(findings, checkVerbList(r.Allow, brokerVerbs,
			fmt.Sprintf("%s on broker facility %q", where, brk), nil)...)
	}

	// **A denial is a feature's word and nowhere else's.** A rule about a
	// channel or a topic grants acts on records, and a feature MQTT gives a
	// client is not one of those - so `deny:` there would read as taking a
	// verb away, which no rule here can do.
	if len(r.Deny) > 0 {
		findings = append(findings, fmt.Sprintf(
			"%s denies %s, and deny: is written only on a `broker: features` rule: it takes a "+
				"feature a client has, never an act on a channel or a topic. "+
				"Rules about records only grant", where, strings.Join(r.Deny, ", ")))
	}

	if len(r.Allow) == 0 {
		findings = append(findings, fmt.Sprintf(
			"%s allows nothing, which is what a client has without it", where))
	}

	if ch != "" {
		c := reg.Get(ch)
		if c == nil {
			// **`%u` is a substitution into a filter, and a channel is not a
			// filter.** A channel is declared once in `channels:`, so it
			// cannot vary by client, and a name holding `%` is refused by
			// the channel-name allow-list anyway - meaning one here is never
			// a channel somebody could have configured.
			//
			// Said separately because the finding below is true and points
			// at the wrong repair: told only that the channel does not
			// exist, an operator goes and declares one per device. Which is
			// the fleet-sized copying roles exist to avoid.
			if strings.Contains(ch, "%") {
				findings = append(findings, fmt.Sprintf(
					"%s names channel %q: %%u and %%c are substituted only where a rule "+
						"names a filter - topic: and filter: - never in a channel name. A "+
						"channel is declared once in channels: and cannot vary by client - "+
						"name it, and scope the rule within it with filter:", where, ch))
				return findings
			}
			findings = append(findings, fmt.Sprintf(
				"%s names channel %q, which is not configured. A rule about a channel that "+
					"does not exist grants nothing and reads as though it does", where, ch))
			return findings
		}
		findings = append(findings, checkVerbs(r.Allow, c.Type,
			fmt.Sprintf("%s on %s channel %q", where, c.Type, ch))...)
		// **A filter that cannot reach the channel it narrows is refused
		// here rather than discovered in production.** It grants nothing
		// and reads as though it granted something, which is the whole
		// class this file exists to refuse - and it is exactly how the
		// mechanism this replaced failed: a rule that matched no topic at
		// all, accepted at startup, printed as a grant by `--acl`, and
		// answering 0x87 to the device it was written for.
		//
		// `%u` is compared as an ordinary spelled-out level, because the
		// rule has to be decidable before any client connects - and a level
		// no client's name can equal still matches a `+` or a `#` on the
		// channel's side, so no real grant is refused by treating it that
		// way.
		//
		// **The filter goes through the same door a channel's own filter
		// does, and in the same order: braces expand, then every MQTT rule
		// is asked of what they expanded to.** This file used to ask
		// neither, and both halves of that were defects an operator could
		// not see. `alerts/#/page` - a spelling a channel is refused for -
		// was accepted here and then granted the whole channel, because a
		// `#` the matcher meets mid-filter stands in for everything after
		// it. And `iot/+/{device,sensor}/#`, the channel's own filter
		// copied into a rule, was accepted and granted nothing, because
		// the matcher met a brace it has never been taught to read.
		if f := strings.TrimSpace(r.Filter); f != "" {
			spellings, err := channelExpand(f)
			if err != nil {
				return append(findings, fmt.Sprintf(
					"%s narrows channel %q with filter %q: %v", where, ch, f, err))
			}
			for _, one := range spellings {
				if err := channel.ValidFilter(one); err != nil {
					return append(findings, fmt.Sprintf(
						"%s narrows channel %q with filter %s: %v",
						where, ch, channel.Spelled(f, one), err))
				}
				// A filter deeper than any topic may be grants nothing.
				if err := channel.TooDeep(one, maxLevels); err != nil {
					return append(findings, fmt.Sprintf(
						"%s narrows channel %q with filter %s, which %v: it grants nothing",
						where, ch, channel.Spelled(f, one), err))
				}
			}
			if !anyIntersects(spellings, c.Filters) {
				findings = append(findings, fmt.Sprintf(
					"%s narrows channel %q with filter %q, which matches none of that "+
						"channel's topics: its filter is %q. The rule grants nothing and "+
						"reads as though it granted part of the channel",
					where, ch, f, c.Filter))
			}
		}
		return findings
	}

	if r.Filter != "" {
		findings = append(findings, fmt.Sprintf(
			"%s is a topic rule and carries a filter, which narrows a rule within a "+
				"channel. A topic rule's filter is its topic: line", where))
	}

	// **The refusal that keeps the two rule kinds from overlapping**, and it
	// is invariant 12's own shape one level up: a channel and a topic rule
	// that both govern one topic would need an arbitration, and the answer
	// is to refuse the ambiguity while the operator is looking rather than
	// to resolve it consistently until something changes underneath.
	//
	// **Wholly inside a channel, not merely crossing one**, and the
	// difference decides whether this rule can be written at all. A filter
	// that touches a channel and some unclaimed topics beside it is an
	// ordinary broadcast rule - `#` and `+/telemetry` reach every channel on
	// the broker and are still the way an operator says "any broadcast
	// topic". What is refused is the rule that can only ever be about a
	// channel's topics, `topic: iot/water/+/location` where that is a
	// channel's own filter, because it grants nothing and reads as though it
	// granted the channel.
	//
	// Nothing is left ambiguous by accepting the crossing form: Allows
	// resolves a topic before it asks anything, so a channel topic never
	// reaches a topic rule whatever that rule's filter covers. `%u` is
	// compared as an ordinary spelled-out level, because the rule has to be
	// decidable before any client connects.
	// **A topic rule's filter goes through the same door, less one rule.**
	// The wildcard rules are MQTT's and apply to any filter - a `#` in the
	// middle of one granted every `alerts` topic to a rule that named
	// three. What is not asked is the rule about the first level, because
	// that one is a channel's: `topic: "#"` is how an operator says "any
	// broadcast topic" and is meant to work.
	spellings, err := channelExpand(topic)
	if err != nil {
		return append(findings, fmt.Sprintf(
			"%s names topic %q: %v", where, topic, err))
	}
	for _, one := range spellings {
		if err := channel.ValidWildcards(one); err != nil {
			return append(findings, fmt.Sprintf(
				"%s names topic %s: %v", where, channel.Spelled(topic, one), err))
		}
		if err := channel.TooDeep(one, maxLevels); err != nil {
			return append(findings, fmt.Sprintf(
				"%s names topic %s, which %v: it grants nothing",
				where, channel.Spelled(topic, one), err))
		}
	}
	// Asked of each spelling rather than of the written form: a brace is
	// opaque to a comparison against a channel's filter, so a braced topic
	// rule lying inside a channel would have read as though it lay outside
	// every one.
	for _, one := range spellings {
		if c := reg.ChannelContaining(one); c != nil {
			findings = append(findings, fmt.Sprintf(
				"%s: topic %s lies inside channel %q, whose filter is %q, and a topic rule "+
					"governs broadcast only. Write it as a channel rule",
				where, channel.Spelled(topic, one), c.Name, c.Filter))
			return findings
		}
	}

	findings = append(findings, checkVerbs(r.Allow, "",
		fmt.Sprintf("%s on broadcast topic %q", where, topic))...)
	return findings
}

// checkVerbs refuses a verb that is not one this rule kind takes, naming
// the ones it does. A verb saguin does not know is a grant that is not
// there, and nothing else would ever say so.
//
// **`read` on a queue gets a sentence of its own**, because it is the one
// wrong verb somebody writes on purpose. Every other store in the world is
// read, so an operator wanting a client that watches the queue reaches for
// it - and told only that it is not one of `write, consume`, they take the
// nearer of the two and grant the taking of work to something that was
// never meant to do any. The finding has to say that a queue cannot be
// watched at all, or it sends them into the failure the verb list exists
// to prevent.
func checkVerbs(allow []string, kind channel.Type, where string) []string {
	return checkVerbList(allow, verbsFor(kind), where, func(v string) string {
		if kind != channel.Queue || v != "read" {
			return ""
		}
		return fmt.Sprintf(
			"%s allows %q: a queue is consumed rather than read, and consuming takes "+
				"the work - a job handed to this client is held for it and no other "+
				"worker sees it. Write \"consume\" for a client meant to do that work. "+
				"There is no verb for watching a queue: its depth is on the metrics "+
				"listener", where, v)
	})
}

// checkVerbList is what every rule kind's allow: line goes through. The
// valid list is the caller's, and so is the sentence for a particular wrong
// word - a queue's `read` has one, and nothing else does yet.
//
// **One implementation rather than one per kind**, because the repeat check
// below is the sort of thing a second copy quietly loses.
func checkVerbList(allow, valid []string, where string, explain func(v string) string) []string {
	var findings []string
	// **A verb written twice grants nothing extra and means somebody edited
	// this line and lost their place.** It survived as far as `--acl`,
	// which printed `read, read` in the column an operator reads to check a
	// rule - so the one tool for asking what a rule does showed a rule
	// nobody wrote.
	seen := map[string]bool{}
	for _, v := range allow {
		v = strings.TrimSpace(v)
		if seen[v] {
			findings = append(findings, fmt.Sprintf(
				"%s allows %q twice. It grants no more than once and reads as an "+
					"edit that lost its place; remove the repeat", where, v))
			continue
		}
		seen[v] = true
		if contains(valid, v) {
			continue
		}
		if explain != nil {
			if said := explain(v); said != "" {
				findings = append(findings, said)
				continue
			}
		}
		findings = append(findings, fmt.Sprintf(
			"%s allows %q, which is not one of %s", where, v, strings.Join(valid, ", ")))
	}
	return findings
}

// checkDenials is what a `broker: features` rule's deny: line goes through:
// a feature saguin does not know is a denial that is not there, and one
// written twice is an edit that lost its place.
func checkDenials(deny []string, where string) []string {
	var findings []string
	seen := map[string]bool{}
	for _, d := range deny {
		d = strings.TrimSpace(d)
		if seen[d] {
			findings = append(findings, fmt.Sprintf(
				"%s denies %q twice. It takes no more than once and reads as an edit that "+
					"lost its place; remove the repeat", where, d))
			continue
		}
		seen[d] = true
		if !contains(featureDenials, d) {
			findings = append(findings, fmt.Sprintf(
				"%s denies %q, which is not one of %s", where, d, strings.Join(featureDenials, ", ")))
		}
	}
	return findings
}

// subjectsNamed is the ones of these a rule actually wrote, for a finding
// that has to say how many things it named. (`nonEmpty` above is a
// different question about a different type and predates this.)
func subjectsNamed(vs ...string) []string {
	var out []string
	for _, v := range vs {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// describeOther names the topic-tree half of a rule that also names a
// broker facility, so the finding says what to split rather than that
// something is wrong.
func describeOther(ch, topic string) string {
	switch {
	case ch != "" && topic != "":
		return fmt.Sprintf("channel %q and topic %q", ch, topic)
	case ch != "":
		return fmt.Sprintf("channel %q", ch)
	default:
		return fmt.Sprintf("topic %q", topic)
	}
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// sorted gives a map's keys in a stable order, so that a file with several
// problems reports them the same way twice.
func sorted[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Grant is one rule resolved for one client: the same rule as written, with
// %u replaced by that client's identity.
type Grant struct {
	Role    string
	Channel string // "" when this grant is about broadcast or the broker
	Topic   string // the filter, when the rule is about broadcast
	Broker  string // the facility, when the rule is about the broker itself
	Filter  string // the filter within a channel, when the rule narrows one
	Verbs   []string
	Denies  []string // the features a `broker: features` rule takes away
}

// entryFor is the one client entry that applies to an identity: **the
// pattern that spells the id out most exactly wins the whole entry**, its
// roles and its limits together.
//
// That is the rule RFC 0002 already uses twice - for two channel filters
// claiming one topic, and it used to be written a second time here for
// limits alone. Roles unioned across every matching pattern while the
// limits beside them did not, so one four-line block was governed by two
// rules: an operator reading `device-7: roles: [device], limits: 200/s`
// reasonably concluded it was the whole of what `device-7` got, and half
// of it was.
//
// **The cost is that a narrow entry can now take something away**, which
// union could not do. It is also what makes an exception expressible at
// all - a fleet's test rig that should have fewer roles than the fleet has
// no spelling under union. Nothing can refuse it at startup, because it is
// the intended behaviour, so `--acl` names the entries an id matched and
// lost to instead.
//
// Two patterns equally exact and both matching one id are refused when the
// file is read - see validate.
func (f *File) entryFor(identity string) (string, User, bool) {
	patterns := f.patterns
	if patterns == nil {
		patterns = compilePatterns(f.Users)
	}
	best, bestAt := "", -1
	for _, p := range patterns {
		if p.spec > bestAt && matchParts(p.parts, identity) {
			best, bestAt = p.text, p.spec
		}
	}
	if bestAt < 0 {
		return "", User{}, false
	}
	return best, f.Users[best], true
}

// PatternFor is the entry that applies, for the tool that explains a
// decision. An operator whose device lost a role is looking at a narrower
// pattern they did not expect to match, and the answer has to come from the
// same function the broker uses or the explanation is a second
// implementation with its own bugs.
func (f *File) PatternFor(identity string) (string, bool) {
	pattern, _, ok := f.entryFor(identity)
	return pattern, ok
}

// For returns everything a client is granted: the rules of every role the
// applying entry names.
//
// **Within that entry the roles still union, and need no rule about which
// wins.** Rules about records only grant, so two roles cannot disagree about
// them. The one denial the file can write - a feature, on a
// `broker: features` rule - is settled by DeniesBroker instead: any role
// denying it denies it. What is decided by exactness is which entry supplies
// the roles, never which rule beats which.
//
// **A role named twice is one set of grants.** It grants the same rules
// either way, so repeating it in a list is a typo rather than a statement,
// and emitting its rules twice would put every one of them on the screen
// twice for the person reading `--acl` to find out what a client may do.
func (f *File) For(identity, clientID string) []Grant {
	kept, _ := f.grants(identity, clientID)
	return kept
}

// Withheld is the grants For leaves out for this pair, as the file writes
// them: every rule naming %u or %c whose value holds a character the rule
// would read as its own syntax (substitute). The broker says so once at
// connect, and --acl says which and why, so a rule that grants nothing to a
// client is never silent.
func (f *File) Withheld(identity, clientID string) []Grant {
	_, withheld := f.grants(identity, clientID)
	return withheld
}

// grants is For's answer and Withheld's together.
func (f *File) grants(identity, clientID string) (out, withheld []Grant) {
	_, entry, ok := f.entryFor(identity)
	if !ok {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, role := range entry.Roles {
		if seen[role] {
			continue
		}
		seen[role] = true
		for _, r := range f.Roles[role] {
			// **One grant per spelling of a braced filter**, which is what
			// RFC 0002 already says the bridge makes of the same notation:
			// a rule is expanded here and nothing past this point ever
			// meets a brace, so the matcher stays the ordinary MQTT one.
			//
			// **Expanded before the substitution below, never after.**
			// Expanding what a substitution produced would let a client
			// widen its own grant - a client whose name is `{a,b}`,
			// substituted into `iot/%u/#`, would be two filters, and a rule
			// written for one device would reach two. Expanding what the
			// file says settles the grant before any client is named, and
			// then there is nothing a name can do to it.
			//
			// Only one of the two can hold a brace: a rule naming both a
			// channel filter and a topic is refused when the file is read.
			for _, filter := range spellings(r.Filter) {
				for _, topic := range spellings(r.Topic) {
					substTopic, topicOK := substitute(topic, identity, clientID)
					substFilter, filterOK := substitute(filter, identity, clientID)
					if !topicOK || !filterOK {
						withheld = append(withheld, Grant{Role: role, Channel: strings.TrimSpace(r.Channel),
							Topic: strings.TrimSpace(topic), Broker: strings.TrimSpace(r.Broker),
							Filter: strings.TrimSpace(filter), Verbs: r.Verbs(), Denies: trimmed(r.Deny)})
						continue
					}
					out = append(out, Grant{
						Role:    role,
						Channel: strings.TrimSpace(r.Channel),
						// **A topic takes the substitution as a suffix does**,
						// and for the same reason: `alerts/%u/#` is the first
						// thing an operator reaches for after a per-device
						// channel suffix. Substituting one field and not the
						// other kept the literal `%u` in the grant, where it
						// matched nothing - a rule written on purpose, a
						// configuration reporting ok, and a fleet refused 0x87.
						//
						// **It cannot widen by taking it.** A substituted topic
						// may spell a channel - `topic: "%u/#"` for a client
						// called `events` - but Allows resolves the real topic
						// to a channel first and reaches AllowsTopic only when
						// none claims it, so a topic rule governs broadcast
						// whatever an identity happens to spell.
						Topic: substTopic,
						// **No substitution here.** `%u` and `%c` are substituted
						// into filters, and a facility is not a filter - it is one
						// of a closed list, checked when the file is read, so a
						// name holding `%` could never have been one.
						Broker: strings.TrimSpace(r.Broker),
						Filter: substFilter,
						Verbs:  r.Verbs(),
						Denies: trimmed(r.Deny),
					})
				}
			}
		}
	}
	return out, withheld
}

// spellings is the plain filters a written one stands for, and is the
// written one itself where it holds no brace.
//
// A filter whose braces cannot be expanded stands for itself too, which
// matches nothing - the safe answer, and unreachable besides: the file is
// refused at startup for exactly that.
func spellings(filter string) []string {
	if !channel.HasBraces(filter) {
		return []string{filter}
	}
	out, err := channelExpand(filter)
	if err != nil || len(out) == 0 {
		return []string{filter}
	}
	return out
}

// AllowsClientID answers whether this client id may log in under this user
// name, and is asked once, at connect, after the name has been proved.
//
// **Absent means any**, which is every file written before the key existed
// and most deployments after it. A credential per device already ties the
// two together, and there is nothing for this to add.
//
// **It can only refuse.** The user name decides which entry applies and
// what that entry grants; this decides whether the connection happens at
// all. So it cannot widen anything, and a client id - which nothing proves
// - never reaches a grant.
//
// `%u` is the proved name, so `client_ids: "%u"` is a fleet-sized way of
// saying "connect under the name you logged in as".
//
// **A name holding `*` is never put into a pattern**, since `*` is the
// pattern's own wildcard: an identity `*` under `client_ids: ["%u"]` would
// admit every client id. Such a pattern matches nothing for that name; a
// pattern without `%u` is unaffected.
func (f *File) AllowsClientID(identity, clientID string) bool {
	_, entry, ok := f.entryFor(identity)
	if !ok || len(entry.ClientIDs) == 0 {
		return true
	}
	for _, pattern := range entry.ClientIDs {
		if strings.Contains(pattern, "%u") && strings.Contains(identity, "*") {
			continue
		}
		if matchPattern(strings.ReplaceAll(pattern, "%u", identity), clientID) {
			return true
		}
	}
	return false
}

// substitute puts a client's two names into a filter.
//
// **`%u` is the proved name and `%c` is the one the device chose**, and the
// difference is the whole of what each is for. `%u` scopes a rule to an
// identity a password or a certificate stands behind, so a rule resting on
// it rests on something. `%c` scopes a rule to a device that shares its
// credential with others - a cohort - where the broker has no finer name to
// use and the operator has accepted that.
//
// **`%c` is a fence and `%u` is a wall**, and an acl_file using `%c` says so
// by using it: a device inside the cohort can take another's client id and
// reach its topics, and the only thing stopping it is that MQTT hands the
// session over, so the device it displaced reconnects and somebody notices.
// `client_ids:` is what keeps that inside one cohort rather than across
// them.
// **A name holding what the filter's own syntax reads - `+`, `#` or `/` -
// is never substituted, and the rule grants nothing for that client**:
// client id `#` under `iot/health/%c` would be `iot/health/#`, every
// device's topic for reading and writing, and `a/b` under `iot/%c/#` would
// reach into device `a`'s subtree. The client id is whatever the device
// typed; mosquitto denies such a client its ACL checks and EMQX refuses the
// substitution, for the same reason. ok is false for such a value, and For
// leaves the grant out (Withheld). **One pass**, so a value holding `%c` or
// `%u` is put in as written rather than substituted again.
//
// **An absent client id leaves `%c` standing rather than substituting
// nothing.** Only the tool that explains a decision can be asked without
// one - a live client always has an id, MQTT requires it - and putting the
// empty string in its place would quietly rewrite `iot/health/%c` into
// `iot/health/`, a different filter that matches a different topic. The
// literal left standing matches nothing, which is the truthful answer for a
// rule nobody has named a device for, and it is what lets `--acl` say so
// instead of printing a line that looks resolved.
//
// **It runs on every permission check** - every publish and every delivery,
// once per rule - so it costs nothing where there is nothing to put in: a
// filter naming neither placeholder is returned as it is, with no
// allocation. It used to build a strings.Replacer for every rule on every
// check, and that was a third of the broker's CPU under the fleet scale
// run's acl_file, twice the cost per delivery of the build before it.
func substitute(filter, identity, clientID string) (string, bool) {
	filter = strings.TrimSpace(filter)
	namesU, namesC := strings.Contains(filter, "%u"), clientID != "" && strings.Contains(filter, "%c")
	if !namesU && !namesC {
		return filter, true
	}
	if namesU && strings.ContainsAny(identity, filterSyntax) || namesC && strings.ContainsAny(clientID, filterSyntax) {
		return "", false
	}
	// One pass, left to right: what is put in is written and never read
	// again, so a name holding `%c` or `%u` stays as the name.
	var b strings.Builder
	b.Grow(len(filter) + len(identity) + len(clientID))
	for i := 0; i < len(filter); i++ {
		if filter[i] == '%' && i+1 < len(filter) {
			switch {
			case filter[i+1] == 'u':
				b.WriteString(identity)
				i++
				continue
			case filter[i+1] == 'c' && clientID != "":
				b.WriteString(clientID)
				i++
				continue
			}
		}
		b.WriteByte(filter[i])
	}
	return b.String(), true
}

// filterSyntax is what an MQTT topic filter reads as its own: the two
// wildcards and the level separator. A substituted name holding one of them
// would change the filter's shape rather than name a level in it.
const filterSyntax = "+#/"

// Verbs is what a rule allows, trimmed.
func (r Rule) Verbs() []string {
	out := make([]string, 0, len(r.Allow))
	for _, v := range r.Allow {
		out = append(out, strings.TrimSpace(v))
	}
	return out
}

// trimmed is a list of words as a rule compares them, or nil for none.
func trimmed(words []string) []string {
	if len(words) == 0 {
		return nil
	}
	out := make([]string, 0, len(words))
	for _, w := range words {
		out = append(out, strings.TrimSpace(w))
	}
	return out
}

// DeniesBroker reports whether any of these grants denies a capability on a
// facility of the broker.
//
// **Any one of them, whatever the others allow.** Grants union because a
// grant cannot disagree with another; a denial can, and the answer that
// cannot surprise an operator is that it wins - a role written to keep a
// fleet's sessions short is not undone by a second role that says nothing
// about sessions at all.
func DeniesBroker(grants []Grant, facility, capability string) bool {
	if facility == "" {
		return false
	}
	for _, g := range grants {
		if g.Broker == facility && contains(g.Denies, capability) {
			return true
		}
	}
	return false
}

// AllowsChannel reports whether these grants permit verb on a topic that
// resolves to the named channel.
//
// A grant with no filter covers the whole channel; one with a filter covers
// the topics inside it that the filter matches, which is where an operator
// writes `iot/+/events/%u`.
//
// **The filter is compared against the whole topic**, exactly as a
// broadcast rule's `topic:` is and as a channel's own filter is. What this
// replaced compared against the part of the topic below the *channel name*,
// which only exists when the name happens to be the topic's first level. A
// channel named in the middle of its own filter - `events` holding
// `iot/+/events/+`, which is how the shipped example is laid out - left
// nothing to compare, so every such rule matched nothing and refused its
// own device. It did that silently: the file loaded, `--acl` printed a
// plausible grant, and every publish came back 0x87.
//
// One comparison for both rule kinds is also one thing to learn. A rule now
// says what it grants on its own line, rather than meaning something that
// can only be worked out by looking up the channel it names.
func AllowsChannel(grants []Grant, channel, topic, verb string) bool {
	for _, g := range grants {
		if g.Channel != channel || !contains(g.Verbs, verb) {
			continue
		}
		if g.Filter == "" || channelMatches(g.Filter, topic) {
			return true
		}
	}
	return false
}

// VerbsOnChannel is every verb these grants name on this channel, as a set,
// **whatever topics inside it a grant is narrowed to**.
//
// It answers a different question from AllowsChannel and is deliberately
// broader: not "may this client do that to that topic", which is what the
// publish and subscribe paths ask, but "has this client any business with
// this channel at all". The catalogue asks it, because a grant narrowed to
// part of a channel is still a client that must be told what the channel
// is - and the narrowing is enforced where it decides something, on the
// publish or the subscribe.
//
// A broker grant names no channel and so reaches nothing here.
func VerbsOnChannel(grants []Grant, channel string) map[string]bool {
	held := map[string]bool{}
	for _, g := range grants {
		if g.Broker != "" || g.Channel != channel {
			continue
		}
		for _, v := range g.Verbs {
			held[v] = true
		}
	}
	return held
}

// AllowsTopic reports whether these grants permit verb on a broadcast
// topic - one no channel claims.
func AllowsTopic(grants []Grant, topic, verb string) bool {
	for _, g := range grants {
		// **A broker grant is skipped by name rather than by relying on its
		// empty topic never matching.** It has no channel, so it reaches
		// this loop, and what stops it is that `Matches("", topic)` happens
		// to be false - a property of the matcher rather than of this rule.
		// Said out loud, because "provably cannot" is the standard here and
		// "does not, today" is how a widening arrives.
		if g.Broker != "" || g.Channel != "" || !contains(g.Verbs, verb) {
			continue
		}
		if channelMatches(g.Topic, topic) {
			return true
		}
	}
	return false
}

// AllowsBroker answers a verb on a facility of the broker itself - the one
// rule kind that governs no topic.
//
// **A grant of the other two kinds can never satisfy it**, whatever filter
// it carries and however wide: they hold no facility, and an empty facility
// matches nothing here because the caller always names one. That is the
// property the separate rule kind exists for. A role holding every verb on
// every channel, with `topic: "#"` beside them, hangs nobody up.
func AllowsBroker(grants []Grant, facility, verb string) bool {
	if facility == "" {
		return false
	}
	for _, g := range grants {
		if g.Broker == facility && contains(g.Verbs, verb) {
			return true
		}
	}
	return false
}

// Limits is what one client may send per second, already parsed.
// Zero in either field means the broker-wide figure applies.
type Limits struct {
	PublishRate  int
	PublishBytes int64
}

// HasLimits reports whether any client entry carries limits at all.
//
// **The default path has to be free for a broker that has an acl_file and
// no limits in it**, which is most of them: without this, every publish on
// every such broker would take a map lookup and a mutex for a feature
// nobody asked for. Answered once from the loaded file rather than by
// walking the patterns per publish.
func (f *File) HasLimits() bool {
	for _, c := range f.Users {
		if c.Limits != nil {
			return true
		}
	}
	return false
}

// LimitsFor is what the applying entry bounds this client to, and whether
// it bounds it at all.
//
// **From the same entry the roles come from**, which is what makes an entry
// readable as a whole. It used to be its own search - the most exact pattern
// *carrying limits* - so `device-*` with limits and `device-7` without gave
// `device-7` the fleet's figures while its roles came from somewhere else.
// One block, two rules.
//
// **The consequence is that an entry with no `limits:` has none**, even
// where a broader pattern that matches the same client carries some: the
// client takes the broker-wide figures instead. That is the same
// subtraction the roles do and it is the point of the rule, but it is the
// one shape of it that removes a *bound* rather than a permission, so
// `--acl` prints what the client comes to and names the entry it lost to.
//
// **It replaces the broker-wide figures in both directions**, which is why
// there is no tightest-wins: an exception may be looser than the fleet it
// sits in, and a gateway that legitimately publishes faster than a sensor
// is most of why per-client limits exist at all.
func (f *File) LimitsFor(identity string) (Limits, bool) {
	_, entry, ok := f.entryFor(identity)
	if !ok || entry.Limits == nil {
		return Limits{}, false
	}
	return entry.Limits.resolved(), true
}

// specificity ranks a pattern by how much of an identity it spells out: the
// count of literal characters, with a pattern holding no `*` at all ranked
// above every pattern that does.
//
// The literal count rather than the whole length, because `device-*` and
// `dev*` both match `device-7` and the first says more about it.
func specificity(pattern string) int {
	if !strings.Contains(pattern, "*") {
		return 1 << 20 // exact: more specific than any wildcard, whatever its length
	}
	return len(pattern) - strings.Count(pattern, "*")
}

// resolved parses the written limits. Validation has already refused
// anything unparseable, so a failure here reads as "unset" rather than
// stopping a broker that started.
func (l *UserLimits) resolved() Limits {
	out := Limits{}
	if l.PublishRate != nil {
		out.PublishRate = *l.PublishRate
	}
	if l.PublishBytes != nil {
		if n, err := config.ParseBytes(*l.PublishBytes); err == nil {
			out.PublishBytes = n
		}
	}
	return out
}

// patternsCanCollide reports whether any client id could match both.
//
// **Decided, not sampled.** It tried four witnesses built from the two
// patterns, and missed pairs no such witness reaches: `ab*c` and `a*bc`
// both match `abbc`, the file loaded, and which entry applied was decided
// by sort order. can[i][j] is whether some string matches both a[i:] and
// b[j:]: a `*` either stops here or takes one more byte of what the other
// pattern must produce, whether that is a literal or its own `*`, and two
// literals must be the same byte. Bytes, as matchPattern compares them.
func patternsCanCollide(a, b string) bool {
	can := make([][]bool, len(a)+1)
	for i := range can {
		can[i] = make([]bool, len(b)+1)
	}
	can[len(a)][len(b)] = true
	for i := len(a); i >= 0; i-- {
		for j := len(b); j >= 0; j-- {
			if i == len(a) && j == len(b) {
				continue
			}
			starA := i < len(a) && a[i] == '*'
			starB := j < len(b) && b[j] == '*'
			switch {
			case starA:
				can[i][j] = can[i+1][j] || (j < len(b) && can[i][j+1])
			case starB:
				can[i][j] = can[i][j+1] || (i < len(a) && can[i+1][j])
			case i < len(a) && j < len(b):
				can[i][j] = a[i] == b[j] && can[i+1][j+1]
			}
		}
	}
	return can[0][0]
}

// matchPattern matches a client id against a pattern carrying `*`, which
// stands for any run of characters including none.
//
// **Not path.Match**, which treats `/` as a separator a `*` may not cross -
// a client id is not a path, and `vessel-*` would then fail to match
// `vessel-7/a`. Not a regular expression either: a pattern is written by an
// operator, and one that quietly compiled into something else would be a
// surprising way to grant more than was meant.
func matchPattern(pattern, id string) bool {
	return matchParts(strings.Split(pattern, "*"), id)
}

// matchParts is matchPattern for a pattern already split on its `*`s: one
// part is an exact name.
func matchParts(parts []string, id string) bool {
	if len(parts) == 1 {
		return parts[0] == id
	}
	if !strings.HasPrefix(id, parts[0]) {
		return false
	}
	id = id[len(parts[0]):]
	for i := 1; i < len(parts)-1; i++ {
		j := strings.Index(id, parts[i])
		if j < 0 {
			return false
		}
		id = id[j+len(parts[i]):]
	}
	return strings.HasSuffix(id, parts[len(parts)-1])
}

// Rules is a loaded file with the channels it was validated against, and is
// what the broker asks.
type Rules struct {
	f   *File
	reg *channel.Registry
}

// New pairs a file with the channels its rules are about.
func New(f *File, reg *channel.Registry) *Rules { return &Rules{f: f, reg: reg} }

// For is the grants one identity holds, which is what `--acl explain`
// prints and what every question below is answered from.
func (r *Rules) For(identity, clientID string) []Grant { return r.f.For(identity, clientID) }

// Withheld is File.Withheld for the loaded file.
func (r *Rules) Withheld(identity, clientID string) []Grant { return r.f.Withheld(identity, clientID) }

// VerbFor names the operation a plain MQTT publish or delivery is, given
// the channel the topic resolves to. It is the mapping between what MQTT
// calls things and what an operator writes.
//
// **A direction, and one exception.** Every write is `write` and every read
// is `read`, on a channel of any type and on broadcast alike; a queue's read
// is `consume`, for the reason set out where the verb tables are. A nil
// channel is broadcast, and takes the ordinary pair.
//
// **The types are listed rather than defaulted, in both directions**, which
// is why this is not the four lines it looks like it should be. A type this
// function does not know answers "", which is a verb no rule can grant, so
// such a channel refuses reads and writes alike - where a fall-through to
// the ordinary pair would grant both. The input is unreachable today,
// because a channel's type is checked when the configuration is read; it is
// written closed because the cost of being wrong is one-sided, and because
// the next person to add a channel type should have to come here.
//
// **The control verbs are not here**, because this is answered from a topic
// and a direction alone: a point read names its key in the payload, and a
// `latest` delete is a publish with an empty one. Those are asked on the
// publish path, where both are in hand.
func VerbFor(c *channel.Channel, write bool) string {
	if c == nil { // broadcast: the topics no channel claims
		if write {
			return "write"
		}
		return "read"
	}
	switch c.Type {
	case channel.Append, channel.Latest:
		if write {
			return "write"
		}
		return "read"
	case channel.Queue:
		if write {
			return "write"
		}
		return "consume"
	}
	return ""
}

// HasPublishLimits reports whether the acl_file names any limits at all.
func (r *Rules) HasPublishLimits() bool { return r.f.HasLimits() }

// AllowsClientID is File.AllowsClientID, asked through the loaded rules.
func (r *Rules) AllowsClientID(identity, clientID string) bool {
	return r.f.AllowsClientID(identity, clientID)
}

// PublishLimits is what this identity may send in a second, and whether the
// file names it at all. See File.LimitsFor for how a client matching
// several patterns is settled.
func (r *Rules) PublishLimits(identity string) (int, int64, bool) {
	l, named := r.f.LimitsFor(identity)
	return l.PublishRate, l.PublishBytes, named
}

// Allows answers an ordinary publish or delivery: the topic decides the
// channel, the channel and the direction decide the verb.
//
// A filter is answered by the channel it reaches, which is the same
// question invariant 11 asks - so a wildcard at or above channel depth is a
// broadcast subscription here too, and a rule about a channel does not
// grant it.
func (r *Rules) Allows(identity, clientID, topic string, write bool) bool {
	// **A queue's own subscription form names its channel and is not a
	// topic filter**, so it is answered here rather than through the
	// routing table below. `$saguin/queue/jobs` reaches no channel that way
	// - a queue filtered `iot/+/work/+` holds nothing under that name - and
	// falling through to the broadcast rule is the failure this branch
	// exists to prevent: it refuses every worker whose role carries
	// `consume`, and grants the queue's own form to any role holding a
	// `topic:` rule. That is the widening the two rule kinds exist to make
	// impossible, and the phantom subscriber it produced took a job the
	// broker then never re-offered, returned or dead-lettered.
	//
	// **It is asked before the reserved space below, and that order is the
	// whole of the rule.** The form used to be `$share/saguin/<name>/#`,
	// which does not begin `$saguin/`, so the two branches could not meet.
	// It is `$saguin/queue/<name>` now: answered in the order below, every
	// worker's subscription would take the reserved space's blanket `true`
	// and no `consume` grant would ever be asked for. Any client that could
	// connect could consume any queue.
	//
	// The grants are looked up inside the branch rather than above it, so
	// that the reserved space below - which answers without asking for any
	// - still costs nothing.
	if c := r.reg.CanonicalQueue(topic); c != nil {
		return AllowsChannel(r.f.For(identity, clientID), c.Name, topic, VerbFor(c, write))
	}

	// **The rest of the reserved space is not answered here.** Every other
	// `$saguin/` topic is a control verb the publish path resolves - a seek
	// names its channel in the topic, a point read names its key in the
	// payload - and none of them resolves to a channel by prefix. Answered
	// here they would all look like a broadcast publish and be refused
	// before their real verb was ever asked, which is what the first
	// version did: a seek a role explicitly granted came back 0x87.
	//
	// Nothing escapes by this door. processPacket handles each of those
	// topics and refuses any other in that space outright, so a client
	// cannot reach anything through a `$saguin/` topic that is not asked
	// for by name there - and the broker refuses a subscription to one of
	// them before this is ever asked.
	if strings.HasPrefix(topic, channel.ReservedRoot+"/") {
		return true
	}
	// **The filter inside a shared subscription, not the string as sent.**
	// `$share/grp/events/#` resolves to no channel by prefix, so asking
	// about it directly falls through to broadcast - which would refuse
	// every consumer whose role names the channel, and grant the channel's
	// topics to any role holding a `topic:` rule. That is the widening the
	// two rule kinds exist to make impossible.
	//
	// The broker strips the same prefix before resolving a granted filter,
	// and this is that function rather than a second copy of it: a rule and
	// a subscription have to reach the same channel or they answer
	// different questions about one filter.
	inner := channel.InnerFilter(topic)
	grants := r.f.For(identity, clientID)

	// A publish names one topic and resolves to one channel, and a filter
	// whose every topic resolves to one channel is that channel's alone.
	// Ask that first, because the answer below would be the same one
	// arrived at more slowly.
	//
	// **Owner, not Resolve.** Resolve reads its argument as a topic, and a
	// route's `+` or `#` takes a filter's own `+` or `#` as a level: `a/#`
	// resolved to a channel filtered `a/+`, and `iot/water/w-7/#` to the one
	// filtered `iot/water/+/+/#`, so each was granted on that channel alone -
	// never asked about the other channels it touches, nor the broadcast
	// topics it matches. Owner keeps what that shortcut got right:
	// `jobs/__dlq/#` is the dead-letter channel's alone, though the queue's
	// `jobs/#` touches it too.
	if c := r.reg.Owner(inner); c != nil {
		return AllowsChannel(grants, c.Name, inner, VerbFor(c, write))
	}

	// A filter, which may now reach several channels at once -
	// `iot/water/w-7/#` is a device's readings and its location, in two
	// channels with two types and two verbs.
	//
	// **It is granted or refused whole**, as MQTT ACLs work everywhere: a
	// client allowed half of what it asked for and told `0x00` has been
	// given a silent partial grant, which is the failure this project
	// refuses. A role meaning "everything about your own device" therefore
	// needs a grant per channel the tree crosses, and a topic rule as well
	// when the tree holds topics no channel claims (below).
	reached := r.reg.ResolveFilters(inner)
	if len(reached) == 0 {
		return AllowsTopic(grants, inner, VerbFor(nil, write))
	}
	for _, c := range reached {
		if !AllowsChannel(grants, c.Name, inner, VerbFor(c, write)) {
			return false
		}
	}
	// **A filter inside no one channel can match a broadcast topic**, and
	// only a topic rule grants those. Granted on its channels alone, `+/x`
	// was answered 0x01 and every broadcast record under it was refused at
	// delivery, where nothing is logged: the silent half above. Judged by
	// the filter's shape, so a filter that only several channels together
	// cover needs the topic rule too - a refusal, answered 0x87 where the
	// client can see it, never a record withheld.
	if r.reg.ChannelContaining(inner) == nil {
		return AllowsTopic(grants, inner, VerbFor(nil, write))
	}
	return true
}

// AllowsVerb answers a named operation on a named channel, which is what
// the publish path asks once it has resolved a control topic: a seek names
// its channel in the topic, a point read names its key in the payload.
func (r *Rules) AllowsVerb(identity, clientID, channelName, topic, verb string) bool {
	return AllowsChannel(r.f.For(identity, clientID), channelName, topic, verb)
}

// VerbsOnChannel is every verb one client holds on one channel, whatever
// topics inside it its grants are narrowed to. See the package-level
// function for why that is the right question for a catalogue answer and
// the wrong one for a publish.
func (r *Rules) VerbsOnChannel(identity, clientID, channelName string) map[string]bool {
	return VerbsOnChannel(r.f.For(identity, clientID), channelName)
}

// AllowsBrokerVerb answers a verb on a broker facility for one client, and
// is asked where the publish path resolves a control topic that names no
// channel.
func (r *Rules) AllowsBrokerVerb(identity, clientID, facility, verb string) bool {
	return AllowsBroker(r.f.For(identity, clientID), facility, verb)
}

// DeniesFeature answers whether one client is denied a feature -
// `persistent`, `will`, `share` or `retained` - which the broker asks once
// the client has authenticated.
func (r *Rules) DeniesFeature(identity, clientID, feature string) bool {
	return DeniesBroker(r.f.For(identity, clientID), channel.FeaturesFacility, feature)
}

// MatchesPattern is matchPattern for the tool that explains a decision: an
// operator whose device is refused everything is usually looking at a
// pattern that did not match, and the answer has to come from the same
// function the broker uses or the explanation is a second implementation
// with its own bugs.
func MatchesPattern(pattern, id string) bool { return matchPattern(pattern, id) }

// ExplainedGrant is one thing a user may do, in the words an answer uses
// rather than the words the file uses: the rule's `channel:` or `topic:`
// collapsed into what kind of subject it is and what that subject is
// called.
//
// **Every field is a pointer or a slice so that absent is null and not the
// zero value.** A grant with no role and a grant whose role is the empty
// string are different things, and a consumer that cannot tell them apart
// reads one as the other.
type ExplainedGrant struct {
	Role    *string  `json:"role"`
	Kind    *string  `json:"kind"`
	Subject *string  `json:"subject"`
	Verbs   []string `json:"verbs"`
	// Denies is what a `broker: features` rule takes away, and `[]` for every
	// other grant: a list either way, so a consumer ranges over it unchecked.
	Denies []string `json:"denies"`
}

// Explanation is what a user may do and how the file arrived at it: the
// grants, the pattern in force, the patterns that matched and did nothing,
// and every pattern the file holds.
//
// **It lives here rather than in whichever command prints it**, because
// two things answer this question now - `saguin --acl` and
// `/v1/operations/acl` - and an operator comparing one against the other is
// exactly the person who must not be given two answers. The shape is a
// contract in RFC 0005, so the json tags are the specification.
type Explanation struct {
	User            string           `json:"user"`
	ACLFile         *string          `json:"acl_file"`
	PatternApplied  *string          `json:"pattern_applied"`
	PatternsMatched []string         `json:"patterns_matched"`
	PatternsInFile  []string         `json:"patterns_in_file"`
	Grants          []ExplainedGrant `json:"grants"`

	// GrantsWithheld is the rules that name %u or %c and grant this pair
	// nothing, because the name they would put in holds +, # or / (File.
	// Withheld), as the file writes them. `[]` where none is, so a consumer
	// ranges over it unchecked.
	GrantsWithheld []ExplainedGrant `json:"grants_withheld"`

	// ClientIDAllowed is whether this user name may connect under the
	// client id asked about: null where none was given, false where the
	// entry's `client_ids` refuses it.
	//
	// **Grants for a connection that never happens are the wrong answer to
	// the right question.** The reader here is somebody whose device is not
	// working, and `client_ids` refuses at CONNECT with `0x86` - the same
	// code a wrong password gets - before a single rule is consulted. A
	// body listing what that pair may publish, with nothing saying it
	// cannot get in, sends them to look at the rules.
	ClientIDAllowed *bool `json:"client_id_allowed"`
}

// Unrestricted is the answer for a configuration naming no acl_file.
// `acl_file` is null because there is none, and the second field says what
// that means rather than leaving a reader to infer it from an absence -
// which is the whole defect this shape exists to avoid. An empty grant list
// would be the JSON spelling of "granted nothing" about a broker that
// grants everything.
type Unrestricted struct {
	ACLFile           *string `json:"acl_file"`
	EverythingAllowed bool    `json:"everything_allowed"`
}

// Kind says whether a grant is about a channel, a broadcast topic or a
// facility of the broker, which is the one decision every presentation of a
// grant has to make.
//
// **A broker grant is asked about first**, because it holds no channel and
// the test below for "no channel means broadcast" would otherwise call it a
// topic rule with no topic - a grant printed as something it is not, in the
// one column an operator reads to check what a role does.
//
// **It is a function rather than three lines each caller writes**, because
// there are three presentations of a grant now - the flag's table, the
// flag's columns and `/v1/operations/acl` - and a rule that read as a
// channel in one and a topic in another would be the same file described
// two ways to the same operator.
func Kind(g Grant) string {
	switch {
	case g.Broker != "":
		return "broker"
	case g.Channel == "":
		return "topic"
	}
	return "channel"
}

// Named is what a grant's subject is called: the channel or the topic it is
// about. The filter is deliberately not folded in - every presentation
// spells that part differently, and only the choice of subject is shared.
func Named(g Grant) string {
	switch {
	case g.Broker != "":
		return g.Broker
	case g.Channel == "":
		return g.Topic
	}
	return g.Channel
}

// NoACLFile is the answer for a broker whose configuration names none, and
// it is a function so that the two doors answering it cannot answer it
// differently - a field added here reaches both.
func NoACLFile() Unrestricted { return Unrestricted{EverythingAllowed: true} }

// Describe is the derivation from a rule to the answer about it, and it is
// one function because the list form and the explanation both need it and
// two derivations would drift.
//
// A client no pattern matches has an empty list rather than one entry of
// nulls: a caller asking about one user cannot confuse "granted nothing"
// with "not asked about", which is the ambiguity the row of nulls exists to
// resolve in the flag's list form and nowhere else.
func Describe(f *File, identity, clientID string) []ExplainedGrant {
	return describe(f.For(identity, clientID))
}

func describe(grants []Grant) []ExplainedGrant {
	out := []ExplainedGrant{}
	for _, g := range grants {
		kind, subject := Kind(g), Named(g)
		// The channel and the filter both, because either alone
		// under-reports: the filter says which topics, and the channel name
		// says which of several rules on one screen this line belongs to.
		if g.Filter != "" {
			subject += " (" + g.Filter + ")"
		}
		out = append(out, ExplainedGrant{
			Role: nonEmpty(g.Role), Kind: nonEmpty(kind), Subject: nonEmpty(subject),
			Verbs: someOf(g.Verbs), Denies: someOf(g.Denies),
		})
	}
	return out
}

// MatchingPatterns is every user pattern in the file this name matches, in
// the file's sorted order. More than one can match; only the most exact
// applies, which is what PatternFor answers.
func MatchingPatterns(f *File, identity string) []string {
	out := []string{}
	for _, pattern := range sortedUsers(f) {
		if MatchesPattern(pattern, identity) {
			out = append(out, pattern)
		}
	}
	return out
}

// EffectiveLimits is what a client is actually held to, and where the
// figures came from.
//
// An entry naming limits **replaces** the broker-wide pair rather than
// merging with it, so a figure it leaves out is unbounded rather than
// inherited. The third return is where the numbers came from, because 200
// from an entry and 200 broker-wide are the same figure in different
// situations and only one of them changes when the entry is deleted.
func EffectiveLimits(f *File, wide config.Resolved, identity string) (int, int64, string) {
	if l, named := f.LimitsFor(identity); named {
		where := "the acl_file"
		if pattern, ok := f.PatternFor(identity); ok {
			where = fmt.Sprintf("the acl_file entry %q", pattern)
		}
		return l.PublishRate, l.PublishBytes, where
	}
	return wide.PublishRate, wide.PublishBytes, "the broker-wide limits"
}

// Explain is the whole answer about one user, as `saguin --acl --output
// json` prints it and as `/v1/operations/acl` returns it.
//
// **The three pattern fields are always present**, null or empty where
// there is nothing to say, because a shape that changes with the answer is
// one a script has to branch on before it can read it. `pattern_applied` is
// the one that decides: the others matched, and only that one is in force.
func Explain(f *File, aclFile string, identity, clientID string) Explanation {
	applied, _ := f.PatternFor(identity)
	e := Explanation{
		User:            identity,
		ACLFile:         nonEmpty(aclFile),
		PatternApplied:  nonEmpty(applied),
		PatternsMatched: MatchingPatterns(f, identity),
		PatternsInFile:  sortedUsers(f),
		Grants:          Describe(f, identity, clientID),
		GrantsWithheld:  describe(f.Withheld(identity, clientID)),
	}
	// Only where a client id was given: absent means the question was not
	// asked, and answering `true` for a pair nobody named would be a claim
	// about a connection this call knows nothing about.
	if clientID != "" {
		allowed := f.AllowsClientID(identity, clientID)
		e.ClientIDAllowed = &allowed
	}
	return e
}

// sortedUsers is every pattern the file holds, in a stable order: two calls
// on one file answer with the same list, which a caller diffing two brokers
// depends on.
func sortedUsers(f *File) []string {
	out := make([]string, 0, len(f.Users))
	for pattern := range f.Users {
		out = append(out, pattern)
	}
	sort.Strings(out)
	return out
}

// nonEmpty is a string as JSON, with the empty one as null.
func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// someOf is a list as JSON, with nil rendered as `[]` rather than null: an
// absent list and an empty one are the same fact here, and `[]` is the one
// a caller can range over without checking.
func someOf(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}
