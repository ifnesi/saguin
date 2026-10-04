package config

import (
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
)

// A bridge is saguin as a client of somebody else's broker. It subscribes
// upstream and re-publishes what arrives through the broker's own publish
// path, so channel resolution, size bounds, header limits and reason codes
// all apply unchanged - an inbound bridge adds no second way into a
// channel, which is why it puts no restriction on channel type.
//
// It is a translator rather than a copy: the far end has never heard of a
// channel or an offset, so everything that made a record saguin's is lost
// on the way across and what arrives is a new record at an offset this
// broker assigns.

// BridgeConfig is one bridge as the file writes it.
type BridgeConfig struct {
	// Peer is the far end this bridge dials.
	Peer     string `yaml:"peer"`
	ClientID string `yaml:"client_id"`

	// Upstream and Inbound are the two keys `peer:` and `topics:` replaced,
	// and they are **still fields so that the refusal can name what to write
	// instead**. Left off the struct, a file using either would be answered
	// by the decoder's own "field upstream not found in type
	// config.BridgeConfig" - true, and no help at all to somebody holding a
	// configuration that worked last week. Every bridge ever written has
	// both in it, so the one-line answer is worth a field that exists only
	// to be refused.
	Upstream string      `yaml:"upstream"`
	Inbound  []TopicRule `yaml:"inbound"`

	// The three keys that tune a link (RFC 0002 "Bridges"). They are per
	// bridge because what they describe is the far end, and no two upstreams
	// are alike: a broker on the same LAN and one over a metered satellite
	// link want different answers to all three. Each is optional and each
	// has a default.
	SessionExpiry string `yaml:"session_expiry"`
	// A pointer so that a written `0` is not the same as an absent key.
	// Zero is the value somebody writes meaning "no limit", and it means the
	// opposite - a subscriber that may receive nothing, which MQTT forbids
	// sending - so it has to be refused rather than quietly defaulted.
	ReceiveMaximum *int   `yaml:"receive_maximum"`
	AckInterval    string `yaml:"ack_interval"`

	// CAFile is the authority that signs the upstream's certificate, for a
	// `tls://` or `wss://` upstream whose certificate a public root does not
	// vouch for. Absent, the system roots are used, which is right for a
	// broker with a certificate from a public authority and useless for the
	// private one an estate usually runs.
	//
	// **There is no way to turn verification off**, and that is deliberate.
	// A bridge carries records out of this broker; a link that accepts any
	// certificate accepts a machine that answered first, and the records go
	// to it. An operator who cannot name the authority has a problem this
	// key cannot fix and a flag would only hide.
	CAFile string `yaml:"ca_file"`

	// CertFile and KeyFile are the other half: the certificate saguin
	// presents to an upstream that asks for one, which is what saguin itself
	// does on a listener with `client_ca_file` set. Without them two saguins
	// cannot bridge to each other under the strongest setting either offers.
	//
	// Either both or neither, like a listener's pair. **A pair without a
	// `ca_file` is allowed**, which is not the listener's rule, and the
	// difference is that here the two halves answer different questions: the
	// authority checks the far end, the pair identifies this one. An upstream
	// with a certificate from a public authority and a private client
	// certificate at this end is an ordinary arrangement, and refusing it
	// would be a rule with no failure behind it.
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	// Topics is the routing, one rule per line of it. It was `inbound:`
	// while reading was the only direction there was.
	Topics []TopicRule `yaml:"topics"`
}

// What a bridge gets when it does not say. Each is also the value the code
// ran with before these were configurable, so an existing configuration
// behaves exactly as it did. RFC 0002 "Bridges" has what each costs when it
// is moved; what is here is why the default is the default.
const (
	// DefaultSessionExpiry is a day: the outage a bridge exists to survive
	// is a bad connection rather than a decommissioning. It spends somebody
	// else's storage, in a queue bounded by their rules - measured against
	// mosquitto 2.1.2, whose max_queued_messages defaults to 1000, a session
	// offline while 1200 were published came back with exactly 1000.
	DefaultSessionExpiry = 24 * time.Hour

	// DefaultReceiveMaximum is 20 - enough that the link is not a round trip
	// per record, small enough that what a bridge holds unacknowledged is
	// bounded by a number somebody wrote down (invariant 13).
	//
	// It is also the backpressure: measured against mosquitto 2.1.2, a
	// subscriber asking for 5 and never acknowledging received exactly 5 of
	// 50 published, which is the behaviour the whole design assumes.
	DefaultReceiveMaximum = 20

	// DefaultAckInterval is 5ms, against the 50ms a client library is likely
	// to default to. An acknowledgement is not sent the instant a record is
	// stored - it is marked, and a ticker sends what has been marked - so
	// with the Receive Maximum it sets a ceiling of one over the other.
	//
	// A ceiling rather than a rate, and the two only coincide at the long
	// end. Measured end to end through mosquitto 2.1.2, 200 records into a
	// memory-backed append channel, on an AMD A10-7800 of 2014, four cores:
	//
	//	50ms   450ms   445/s   against 400/s predicted
	//	 5ms   106ms  1881/s   against 4,000/s
	//	 1ms    73ms  2736/s   against 20,000/s
	//
	// So 5ms buys a factor of four over 50ms and 1ms buys little more, by
	// which point something else is the slower half - and it would cost a
	// timer wake-up every millisecond for the life of every bridge, on
	// hardware where that is not free.
	//
	// It is also the window in which a stored record is not yet acknowledged
	// upstream, so a link cut inside it makes the upstream redeliver, and a
	// foreign record carries no saguin identity: it becomes a second record
	// at its own offset. That duplicate is a stated cost, and this is ten
	// times narrower than the likely default.
	DefaultAckInterval = 5 * time.Millisecond
)

// The bounds each of the three is held to, and every one of them is a rule
// with a consequence rather than a range somebody liked the look of.
const (
	// maxSessionExpiry is MQTT's own ceiling for the Session Expiry Interval,
	// which travels as four bytes of seconds.
	maxSessionExpiry = time.Duration(math.MaxUint32) * time.Second
	// MQTT's range for Receive Maximum. Zero is not "unlimited" - it is a
	// subscriber that may receive nothing, and MQTT forbids sending it.
	minReceiveMaximum = 1
	maxReceiveMaximum = 65535
	// The only reason to delay an acknowledgement is to batch the ones
	// behind it, which gains nothing after a few milliseconds; past a second
	// a bridge is indistinguishable from one that has stopped.
	//
	// There is deliberately no floor to match it. RFC 0002 says `1ms` to
	// `1s`, and the `1ms` is the duration form's rather than this key's: one
	// whole number and one unit, with `ms` the smallest unit, so 1ms is the
	// smallest value that can be written at all. A check for it here would
	// be unreachable code claiming to enforce a rule, which is the thing
	// this file refuses to do elsewhere for the same reason.
	maxAckInterval = time.Second
)

// TopicRule is one routing rule as the file writes it: what saguin asks the
// peer for, which channel the result lands in, and the topic it takes inside
// that channel.
type TopicRule struct {
	// Filter is an ordinary MQTT topic filter, and it is what goes on the
	// wire as saguin's SUBSCRIBE. It is not a pattern saguin evaluates
	// locally over a broader subscription: asking the peer for more than
	// is wanted and discarding the rest pulls the difference across the link,
	// which on the metered connection this feature exists for is exactly
	// backwards.
	//
	// **It may not name the reserved `$saguin/` space**, in any direction.
	// That space is where saguin defines its own control topics - a queue
	// acknowledgement, a seek - and a rule reaching into it at the far end
	// is a bridge subscribing to somebody else's queue: it would take leases
	// it can never acknowledge, so the work redelivers, burns its attempts
	// and dead-letters because a *bridge* read it. That is invariant 6's
	// failure reached from the reading side, where `bridge_source` on a
	// queue used to guard only the writing side. Refused at startup.
	Filter string `yaml:"filter"`

	// Topic is the whole topic a record takes, with `$1`, `$2` … standing
	// for the filter's `+` levels in order and `$#` for its `#` tail.
	Topic string `yaml:"topic"`

	// Channel is the key a rule used to carry, and it is **still a field so
	// that the refusal can name what to do instead** - the same reason
	// `upstream:` and `inbound:` are fields on BridgeConfig. Left off, a
	// file holding it is answered by the decoder's own "field channel not
	// found in type config.TopicRule", which is a Go type name in an
	// operator's face and says nothing about what replaced it.
	//
	// Nothing replaced it: the topic decides where a record lands, exactly
	// as it does for every other publisher, and `--check-config` prints
	// where each rule reaches. That sentence is what the refusal says, and
	// every bridge written before this has the key in it.
	Channel string `yaml:"channel"`

	// Direction is `in`, `out` or `both`, and it is **required**.
	//
	// **No default, and that is deliberate against mosquitto's own shape.**
	// mosquitto's `topic` line defaults to `out`; saguin defaults to
	// nothing. Either default here would be wrong in a way an operator
	// could not see. `both` is refused on a rule carrying a `topic:`
	// template, so defaulting to it would reject every templated rule for a
	// key nobody wrote; defaulting to `in` or `out` would let an omitted
	// key decide whether this broker's records leave the building. An
	// omission that starts egress is not something an operator should be
	// able to make by leaving a line out.
	//
	// `in` subscribes to Filter at the peer and lands what arrives under
	// Topic here. `out` reads the local channels, matching Filter against
	// local topics, and publishes under Topic at the peer. Each key means
	// the same thing in both directions, mirrored: Filter selects at the
	// source end, Topic builds at the destination end.
	Direction string `yaml:"direction"`
}

// Bridge is one validated bridge.
type Bridge struct {
	Name     string
	Peer     string
	ClientID string

	// CAFile is the authority the upstream's certificate is checked
	// against, or empty for the system roots.
	CAFile string

	// CertFile and KeyFile are the certificate saguin presents when the
	// upstream asks for one, or both empty for a link that identifies itself
	// by client id and password alone. Validation guarantees they are set
	// together.
	CertFile string
	KeyFile  string

	// The three tuning keys, defaulted and range-checked. Durations rather
	// than strings, so nothing downstream parses a configuration twice.
	SessionExpiry  time.Duration
	ReceiveMaximum int
	AckInterval    time.Duration

	Topics []Rule
}

// Rule is one validated routing rule, with its topic template compiled.
type Rule struct {
	Filter string

	Topic string

	// In and Out are the validated Direction, as two questions rather than
	// a string to compare. A rule written `both` answers yes to both, which
	// is why they are not one enumeration: the two sides run independently -
	// one subscribes at the peer, the other drains a local channel - and
	// nothing downstream ever wants to know which of the three words was
	// written.
	In, Out bool

	// levels is Filter split on `/`, walked a level at a time by Match.
	levels []string
	// hash records that the filter ends in `#`, which matches zero levels as
	// well as many (MQTT 5 section 4.7.1.2) - so `fleet/#` reaches `fleet`
	// itself.
	hash bool
	// pluses is how many `+` levels the filter has, and so how many numbered
	// substitutions the template may use.
	pluses int
	parts  []part
	// identity says the rule carries no template and a topic maps to
	// itself, which is what `both` is. It is a flag rather than a template
	// standing for it because no template spells "whatever arrived" for
	// every filter - `$#` needs a `#`, and the numbered captures need the
	// filter's own shape.
	identity bool
}

// tailCapture marks the `$#` substitution. Numbered captures are 1-based, so
// zero is free to mean "literal text" and a negative number cannot collide.
const tailCapture = -1

// part is one piece of a compiled template: literal text, or a substitution.
type part struct {
	text  string
	index int // 0 literal, 1..N for $1..$N, tailCapture for $#
}

// Match reports the local topic an upstream topic becomes under this rule.
//
// It returns false when the rule does not cover the topic at all, and **true
// with an empty topic** when the rule covers it but there is nothing
// publishable. The two are worth telling apart: one is traffic this bridge
// was never asked about, and the other is traffic it was asked about and
// cannot place, which is the one worth a warning.
//
// Nothing publishable has two causes, and both come from a `#` that matched
// zero levels - as when `fleet/#` reaches the bare `fleet`:
//
//   - The result is empty, so there is no topic to publish to.
//   - The result begins with `$`, which only a `channel: none` rule can
//     reach. MQTT reserves that space and saguin defines topics inside it -
//     a queue acknowledgement, a seek - so a bridge that could publish there
//     would be forging them on behalf of whatever it is carrying. This is
//     the only place that can be caught: a `$` written in a template begins
//     a substitution and never survives to be literal text, so the one to
//     stop is the one a capture built out of what the upstream published.
//
// The topic is split on every call, which is the one allocation a rule that
// does not match makes. Measured on an AMD A10-7800 of 2014, four cores, at
// `-benchtime 2s -count 2`: **253-281ns for a miss** at 1 allocation, and
// **641-754ns for a hit** at 4, the difference being the topic that only a
// hit builds.
//
// The miss is the figure that bounds anything, because a message matching no
// rule is the only one that runs every rule - so the worst case is traffic
// the operator did not ask for, at about 270ns per rule. Fifty rules is 13us
// on a message nothing wanted, which at the load RFC 0001 describes is not
// close to mattering.
//
// Hoisting the split is the obvious thing to do if that ever changes: the
// caller would split once per message and every rule would walk the same
// slice. It is written the simple way first because the figure says so, and
// this project prefers a figure to a prediction.
func (in Rule) Match(topic string) (string, bool) {
	t, _, ok := in.match(topic)
	return t, ok
}

// Direction spells this rule's direction back as the operator wrote it,
// for a message that has to name it. The two booleans are what everything
// else asks; this is the one place the word is wanted again.
func (in Rule) Direction() string {
	switch {
	case in.In && in.Out:
		return DirectionBoth
	case in.Out:
		return DirectionOut
	default:
		return DirectionIn
	}
}

// Reaches is every channel a record this rule produces could land in, and
// it is **visibility rather than a rule**: a bridge does not decide where a
// record goes and cannot hold a wrong opinion about it, because the topic
// decides, exactly as it does for any other publisher (invariant 11).
//
// It replaced an assertion. A rule used to carry `channel:`, checked at
// startup where it could be decided and at runtime where it could not, with
// a mismatched record dropped and counted. That made the bridge know where
// the channels are - the one thing invariant 11 says a client never has to
// do - and it bought nothing a record could not already be traced by. What
// an operator actually wants is the answer, not a guess they have to keep
// in step, so `--check-config` prints this.
//
// **On the reading side the answer is where records will land; on the
// writing side it is which channels the rule drains.** One question, asked
// of whichever end this rule reads from, which is what makes the two
// directions one piece of code.
//
// The template stands for every topic it can build - `$1` is a `+` and `$#`
// is a `#` - so this is the same over-inclusive geometry a subscriber's
// filter gets, and it is safe for the same reason: a channel only holds
// records whose topics resolved to it in the first place.
func (in Rule) Reaches(reg *channel.Registry) []*channel.Channel {
	return in.reaching(reg, false)
}

// Crosses is every queue a rule's topics touch and which it is therefore
// **never served** (invariant 11). It is the quiet half of the answer:
// nothing refuses such a rule and nothing logs it at runtime, because from
// the broker's side there is nothing wrong - a filter crossing a queue is
// served everything else it matches, exactly as any subscriber's is.
//
// Quiet is what makes it worth printing. An operator who writes `orders/#`
// over an estate that includes a queue has written a rule that works and
// carries less than they may think, and `--check-config` is the one place
// they would find that out before the records did not arrive.
func (in Rule) Crosses(reg *channel.Registry) []*channel.Channel {
	return in.reaching(reg, true)
}

func (in Rule) reaching(reg *channel.Registry, queues bool) []*channel.Channel {
	if reg == nil {
		return nil
	}
	// **Whichever end of this rule is local.** An `in` rule's local side is
	// the topic it builds, so the template stands for it; an `out` rule's
	// local side is the filter it reads this broker with, and the topic it
	// builds belongs to the peer. Resolving the built topic either way
	// answered a question about the peer's topic space using this broker's
	// channels - measured on `out "iot/+/state/#"` producing
	// `mirror/$1/state/$#`, which reported "broadcast, because no channel
	// claims those topics" for a rule that reads a `latest` channel.
	//
	// An identity rule builds whatever it matched, so the two are the same
	// string and `both` - which may only be identity - is unaffected either
	// way.
	filter := in.Filter
	if in.In && !in.identity {
		filter = templateFilter(in.parts)
	}
	var out []*channel.Channel
	for _, c := range reg.ResolveFilters(filter) {
		if (c.Type == channel.Queue) == queues {
			out = append(out, c)
		}
	}
	return out
}

// DroppedWildcards is the filter's `+` levels that the topic template does
// not use, written as `$1`, `$2` … in order, or nothing when it uses them
// all.
//
// **A dropped `+` is legal and is sometimes meant**: one bridge per vessel
// drops the vessel id on purpose, and there is no other way to spell that,
// so this is not a validation finding and the configuration is accepted.
// What it costs is that every value of that level lands on the same topic -
// finitely many, which is why compileTemplate refuses the `#` version and
// allows this one. `--check-config` says so, because the difference between
// meaning it and mistyping it is not visible from the file.
func (in Rule) DroppedWildcards() []string {
	// **An identity rule drops nothing**, and saying otherwise is not a
	// harmless overstatement: it carries no template, so the walk below
	// finds no capture used and reports every one of them dropped. A `both`
	// rule would then be warned that it collapses topics it reproduces
	// exactly - noise on the one rule that cannot have this fault.
	if in.identity {
		return nil
	}
	used := make(map[int]bool, len(in.parts))
	for _, p := range in.parts {
		if p.index > 0 {
			used[p.index] = true
		}
	}
	var dropped []string
	for n := 1; n <= in.pluses; n++ {
		if !used[n] {
			dropped = append(dropped, fmt.Sprintf("$%d", n))
		}
	}
	return dropped
}

// Reserved reports whether this rule covers the topic and dropped it for
// aiming at the reserved `$` space, rather than for producing nothing at
// all. Both make Match answer "covered, nothing publishable", and RFC 0002
// lists them as two different drops with two different things to look at -
// so the caller that has to write the line can ask which it was.
//
// It walks the topic a second time. That is affordable because it is only
// ever asked on a record already being dropped, and it is worth more than
// keeping one answer that has to be guessed at.
func (in Rule) Reserved(topic string) bool {
	_, why, _ := in.match(topic)
	return why == droppedReserved
}

// Why a covered topic produced nothing publishable.
type dropReason int

const (
	droppedNothing  dropReason = iota // it was published, or no rule covered it
	droppedEmpty                      // the suffix came out empty
	droppedReserved                   // the suffix began with `$`
)

func (in Rule) match(topic string) (string, dropReason, bool) {
	tl := strings.Split(topic, "/")

	var caps []string
	if in.pluses > 0 {
		caps = make([]string, 0, in.pluses)
	}
	tail := ""

	for i, fl := range in.levels {
		if fl == "#" {
			// Stands in for nothing as well as for everything, so a filter
			// ending in `#` reaches the topic above it (MQTT 5 section
			// 4.7.1.2). The slice is empty in that case and the tail is the
			// empty string.
			tail = strings.Join(tl[i:], "/")
			break
		}
		if i >= len(tl) {
			return "", droppedNothing, false // the filter has levels left, the topic does not
		}
		if fl == "+" {
			caps = append(caps, tl[i])
			continue
		}
		if fl != tl[i] {
			return "", droppedNothing, false
		}
	}
	// Without a trailing `#` the two must have run out together. A filter of
	// `fleet/+` does not reach `fleet/a/b`.
	if !in.hash && len(tl) != len(in.levels) {
		return "", droppedNothing, false
	}

	// **The identity rule answers with what it was given**, which is what
	// `both` is: the filter decided whether the record is covered, and a
	// topic that maps to itself maps back the same way. It is answered
	// after the match above rather than before it, so a rule still covers
	// exactly the topics its filter covers - an identity that answered
	// first would carry everything.
	if in.identity {
		// **An empty topic maps to nothing publishable**, as an empty built
		// topic does below. `#` and `+` both match it, and a peer's
		// PUBLISH can carry one - paho hands a zero-length topic on
		// unchecked - so reading its first byte panicked in the bridge's
		// worker and the broker exited
		// (FuzzABridgeRuleNeverPanics).
		if topic == "" {
			return "", droppedEmpty, true
		}
		// Behind the load-time refusal of `both` on a `$` filter: a topic
		// mapped to itself is still never one in the reserved space.
		if topic[0] == '$' {
			return "", droppedReserved, true
		}
		return topic, droppedNothing, true
	}

	var b strings.Builder
	b.Grow(len(in.Topic) + len(tail))
	for _, p := range in.parts {
		switch p.index {
		case 0:
			b.WriteString(p.text)
		case tailCapture:
			b.WriteString(tail)
		default:
			b.WriteString(caps[p.index-1])
		}
	}
	built := b.String()

	// An empty tail leaves the separator that was joining it: `telemetry/$1/$#`
	// with nothing after `telemetry` would be `telemetry/vessel-07/`, whose
	// last level is empty. The separator goes with the tail it was there for.
	// Nothing else can produce a trailing separator, because a template that
	// literally ends in one is refused at startup and `$#` is refused anywhere
	// but at the end.
	built = strings.TrimSuffix(built, "/")
	if built == "" {
		return "", droppedEmpty, true
	}

	// **The template builds the whole topic, whether or not the rule names a
	// channel**, and this is where the rule stopped prefixing one. A channel
	// used to be a literal prefix, so `channel: readings` plus a suffix was
	// a placement; now a channel is a filter, and there is no suffix that
	// generally lands inside one - `iot/+/+/events/#` has no prefix to hang
	// anything off.
	//
	// So the topic decides, exactly as it does for every other publish, and
	// `channel:` on the rule is the assertion that it landed where the
	// operator meant. One router rather than two, which is the point: two
	// disagree eventually, and the disagreement is a record in the wrong
	// channel with nothing reporting it.
	if built[0] == '$' {
		return "", droppedReserved, true
	}
	return built, droppedNothing, true
}

// validateBridges checks every bridge and compiles its rules, returning what
// the broker runs against and every finding rather than only the first.
//
// reg may be nil, when the channels themselves did not validate. The channel
// check is then skipped rather than reported wrongly: a rule naming a channel
// that failed for its own reasons would otherwise be reported as naming a
// channel that does not exist, which sends the operator to the wrong line.
func validateBridges(raw map[string]BridgeConfig, reg *channel.Registry, maxLevels int) ([]Bridge, []string) {
	if len(raw) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names) // findings must not depend on map iteration order

	var findings []string
	var out []Bridge

	// Two bridges that connect to one broker under one client id fight for
	// ever: MQTT closes the older session when a new one arrives with the same
	// identifier, so each disconnects the other as fast as it can reconnect,
	// and neither ever delivers anything. Nothing reports it but a connection
	// log that scrolls.
	seen := map[string]string{}

	for _, name := range names {
		bc := raw[name]
		bad := func(format string, a ...any) {
			findings = append(findings, fmt.Sprintf("bridge %q: "+format, append([]any{name}, a...)...))
		}

		// The name reaches log lines and will reach a metric label, so it is
		// held to something an operator can read back. It is not a channel
		// name and never becomes a file or a topic level, so the rule is only
		// that it is short and has no spaces in it.
		switch {
		case strings.TrimSpace(name) == "":
			findings = append(findings, "a bridge has no name")
			continue
		case len(name) > channel.MaxNameLength:
			bad("name is longer than %d bytes", channel.MaxNameLength)
			continue
		case strings.ContainsAny(name, " \t\n"):
			bad("name contains whitespace")
			continue
		}

		// **The two renamed keys are refused by name, and the refusal says
		// what to write instead.** Both are asked before anything else about
		// this bridge, so a file written against the old shape is answered
		// once about the rename rather than about whatever the missing key
		// makes the rest of the block look like - `peer` empty reads as "no
		// peer", which is a different mistake and a confusing one to be told
		// about while `upstream:` is sitting there in the file.
		renamed := false
		if strings.TrimSpace(bc.Upstream) != "" {
			bad("upstream: is now peer:, because a bridge writes as well as reads and " +
				"the far end is not upstream of anything. Rename the key; its value is " +
				"unchanged")
			renamed = true
		}
		if len(bc.Inbound) > 0 {
			bad("inbound: is now topics:, because it was named for the only direction " +
				"there used to be. Rename the key; the rules under it are unchanged")
			renamed = true
		}
		if renamed {
			continue
		}

		// **A fault about the bridge does not silence the faults about its
		// rules.** These used to `continue`, so a bridge missing its
		// `client_id` reported only that and the operator ran the check
		// again to be told their rule still wrote `channel:` - two runs for
		// one file, against this package's own rule that a pass is whole.
		// The rules are independent of the peer, the client id and the
		// tuning, so they are read whichever of those is wrong; `broken`
		// only stops the bridge being *built* at the end.
		broken := false
		if finding := validPeer(bc.Peer); finding != "" {
			bad("peer %q: %s", bc.Peer, finding)
			broken = true
		}
		ca := strings.TrimSpace(bc.CAFile)
		certPath, keyPath := strings.TrimSpace(bc.CertFile), strings.TrimSpace(bc.KeyFile)
		// Either both or neither. A certificate with no key cannot be
		// presented and a key with no certificate names nobody, so half a
		// pair is a typo rather than a setting - and left to the handshake it
		// is a link that never comes up against a broker whose log says it is
		// running.
		if (certPath == "") != (keyPath == "") {
			bad("cert_file and key_file: a bridge states both the certificate it presents " +
				"and its key, or neither")
		}
		for _, p := range []struct{ key, path, what string }{
			{"ca_file", ca, "which authority a bridge trusts"},
			{"cert_file", certPath, "which certificate a bridge presents"},
			{"key_file", keyPath, "which key a bridge holds"},
		} {
			if p.path != "" && !filepath.IsAbs(p.path) {
				bad("%s %q is relative: %s must not depend on where the broker was "+
					"started from", p.key, p.path, p.what)
			}
		}
		// An authority or a certificate on a link that is not encrypted is a
		// rule with nothing to apply it to, and it reads as though the link
		// were verified - or as though saguin were identifying itself - when
		// there is no handshake to do either in.
		if ca != "" || certPath != "" || keyPath != "" {
			if u, err := url.Parse(bc.Peer); err == nil && u.Scheme != "tls" && u.Scheme != "wss" {
				bad("peer %q is not encrypted, so there is no handshake for ca_file, "+
					"cert_file or key_file to take part in", bc.Peer)
			}
		}

		// No default. A client id is saguin's identity at the far end, where
		// it decides which session the upstream resumes and, once there is
		// authentication, which principal it is. Inventing one means two edge
		// boxes silently share a session and take half each other's messages.
		if strings.TrimSpace(bc.ClientID) == "" {
			bad("no client_id, which is saguin's identity at the peer")
			broken = true
		} else {
			// Asked only where there is an id to collide: keyed on an empty
			// one, two bridges both missing it would be reported as sharing
			// a client id, which is a second complaint about the same
			// missing line.
			key := bc.Peer + "\x00" + bc.ClientID
			if other, ok := seen[key]; ok {
				bad("client_id %q at %s is already used by bridge %q; two connections with one "+
					"client id disconnect each other for ever", bc.ClientID, bc.Peer, other)
				broken = true
			}
			seen[key] = name
		}

		// A bridge with no rules connects, subscribes to nothing, and holds a
		// session open at somebody else's broker for ever.
		if len(bc.Topics) == 0 {
			bad("no topics rules, so it would connect and carry nothing")
			continue // there is nothing below to say anything about
		}

		tuning, bads := tuning(bc)
		for _, s := range bads {
			bad("%s", s)
		}
		if len(bads) > 0 {
			broken = true
		}

		b := Bridge{
			Name: name, Peer: bc.Peer, ClientID: bc.ClientID,
			CAFile:         bc.CAFile,
			CertFile:       bc.CertFile,
			KeyFile:        bc.KeyFile,
			SessionExpiry:  tuning.SessionExpiry,
			ReceiveMaximum: tuning.ReceiveMaximum,
			AckInterval:    tuning.AckInterval,
		}
		ok := true
		// outAt is where each outbound rule was first written, by its filter
		// and topic, which is also the name it stores its position under.
		outAt := map[[2]string]int{}
		for i, r := range bc.Topics {
			// **A `{a,b}` level expands here, into one rule per spelling.**
			// The notation is saguin's own and the upstream is an ordinary
			// MQTT broker, so a brace left in place goes on the wire as one
			// literal level and matches nothing there - a link that is
			// configured, reports no error, and silently carries no
			// records. RFC 0002 already says the outbound side generated
			// from a braced channel is one rule per spelling; this is the
			// same rule read the other way.
			//
			// The template's `$1`, `$2` … are unaffected: they number the
			// filter's `+` levels, and a braced level is not one.
			// **Refused before anything is compiled**, because a rule
			// carrying it was written against a broker that placed records
			// by a key rather than by their topic, and every other finding
			// about that rule would be an answer to the wrong question.
			if strings.TrimSpace(r.Channel) != "" {
				bad("topics[%d]: channel: is gone. The topic decides where a record "+
					"lands, as it does for every publisher - `--check-config` prints "+
					"where each rule reaches. Delete the key", i)
				ok = false
				continue
			}
			spellings, err := channel.Expand(r.Filter)
			if err != nil {
				bad("topics[%d]: filter %q: %v", i, r.Filter, err)
				ok = false
				continue
			}
			// **One rule's complaint is printed once**, however many
			// spellings it expanded to. What is wrong with a template or a
			// channel name is wrong the same way in every spelling, so
			// repeating it puts the same sentence on the screen twice with
			// nothing to tell the two apart, about one line in the file.
			// A complaint about the filter itself does name its spelling,
			// so those stay distinct and are still all reported.
			said := map[string]bool{}
			for _, one := range spellings {
				spelt := r
				spelt.Filter = one
				in, bads := compileRule(spelt, r.Filter, reg, maxLevels)
				for _, s := range bads {
					if said[s] {
						continue
					}
					said[s] = true
					bad("topics[%d]: %s", i, s)
					ok = false
				}
				if len(bads) == 0 && in.Out {
					// **Two outbound rules with one filter and one topic are
					// refused**, a braced spelling meeting a written one
					// included. They would send every record to the same
					// topic at the peer twice, and they would share the one
					// position their filter and topic name - two drains
					// saving one position, the loss RFC 0002 "Bridges" keeps
					// a position per rule to prevent.
					key := [2]string{in.Filter, in.Topic}
					if j, dup := outAt[key]; dup {
						bad("topics[%d]: filter %q with topic %q repeats topics[%d], so every "+
							"record would be sent to the peer twice; write it once", i, in.Filter, in.Topic, j)
						ok = false
						continue
					}
					outAt[key] = i
				}
				if len(bads) == 0 {
					b.Topics = append(b.Topics, in)
				}
			}
		}
		// Built only where nothing about the bridge or its rules was wrong -
		// `broken` carries the first, `ok` the second, and both had to be
		// reported before either could stop this.
		if ok && !broken {
			out = append(out, b)
		}
	}
	return out, findings
}

// tuning reads the three keys that tune a link, filling in the defaults and
// holding each to its range.
//
// Every refusal says what the value costs rather than only what the range
// is, because all three are the sort of number somebody raises to make a
// symptom go away - and two of the three spend something that is not
// theirs: the upstream's storage, and the upstream's willingness to keep
// sending.
func tuning(bc BridgeConfig) (Bridge, []string) {
	var findings []string
	out := Bridge{
		SessionExpiry:  DefaultSessionExpiry,
		ReceiveMaximum: DefaultReceiveMaximum,
		AckInterval:    DefaultAckInterval,
	}

	if s := strings.TrimSpace(bc.SessionExpiry); s != "" {
		switch d, err := parseWholeSeconds(s); {
		case s == RetentionNone:
			// `none` is a value everywhere else in this file, and here it is
			// the one thing a bridge may not ask for. MQTT can express it -
			// a Session Expiry Interval of 0xFFFFFFFF never expires - and
			// that session then outlives the box it belonged to, on somebody
			// else's broker, holding records for a subscriber that is never
			// coming back. Only they can clear it, by hand, having first
			// worked out what it is.
			findings = append(findings, fmt.Sprintf(
				"session_expiry %q: a bridge is a guest at its peer, so its session "+
					"must be able to expire; write a duration such as 1d", s))
		case err != nil:
			findings = append(findings, fmt.Sprintf("session_expiry %q: %v", s, err))
		case d > maxSessionExpiry:
			findings = append(findings, fmt.Sprintf(
				"session_expiry %q is longer than MQTT can carry, which is about 136 years", s))
		default:
			out.SessionExpiry = d
		}
	}

	if bc.ReceiveMaximum != nil {
		n := *bc.ReceiveMaximum
		switch {
		case n < minReceiveMaximum:
			findings = append(findings, fmt.Sprintf(
				"receive_maximum %d: it is how many records the peer may have in flight, so "+
					"below %d there is no link at all. Zero is not \"no limit\" - MQTT forbids "+
					"sending it, and omitting the key is what asks for the default of %d",
				n, minReceiveMaximum, DefaultReceiveMaximum))
		case n > maxReceiveMaximum:
			findings = append(findings, fmt.Sprintf(
				"receive_maximum %d is above MQTT's own ceiling of %d", n, maxReceiveMaximum))
		default:
			out.ReceiveMaximum = n
		}
	}

	if s := strings.TrimSpace(bc.AckInterval); s != "" {
		switch d, err := parseDuration(s); {
		case err != nil:
			findings = append(findings, fmt.Sprintf("ack_interval %q: %v", s, err))
		case d > maxAckInterval:
			findings = append(findings, fmt.Sprintf(
				"ack_interval %q is longer than %s: with receive_maximum %d it would hold the "+
					"bridge to about %d records a second, and the only reason to delay an "+
					"acknowledgement is to batch the ones behind it",
				s, maxAckInterval, out.ReceiveMaximum,
				int(float64(out.ReceiveMaximum)/d.Seconds())))
		default:
			out.AckInterval = d
		}
	}

	return out, findings
}

// upstreamSchemes are the ways a bridge may reach an upstream broker. Each
// is a transport MQTT is carried over, and naming them explicitly is what
// stops `mqtt://` - which is not a scheme any Go dialler knows - from being
// accepted and then failing at the first connection instead of at startup.
var upstreamSchemes = map[string]bool{
	"tcp": true, "tls": true, "ws": true, "wss": true,
}

func validPeer(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "no address, such as tls://mqtt.example.com:8883"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return err.Error()
	}
	schemes := make([]string, 0, len(upstreamSchemes))
	for s := range upstreamSchemes {
		schemes = append(schemes, s+"://")
	}
	sort.Strings(schemes)
	if !upstreamSchemes[u.Scheme] {
		return fmt.Sprintf("scheme %q is not one of %s", u.Scheme, strings.Join(schemes, ", "))
	}
	if u.Host == "" {
		return "no host"
	}
	if u.Port() == "" {
		return "no port"
	}
	return ""
}

// compileRule validates one rule and compiles its template.
//
// `written` is the filter as the operator wrote it, which is the same as
// the rule's own unless a `{a,b}` expanded - and then the complaint has to
// name both, or it names a filter that is nowhere in the file.
func compileRule(r TopicRule, written string, reg *channel.Registry, maxLevels int) (Rule, []string) {
	var findings []string

	// **The direction first, because three later rules are about it.** A
	// rule whose direction did not parse is not a rule with one mistake in
	// it - it is a rule nobody can say what it does, so the checks that ask
	// "may this be an out source" would be answering about a direction the
	// operator never wrote.
	in, out, dirErr := readDirection(r.Direction)
	if dirErr != "" {
		findings = append(findings, dirErr)
	}

	levels, pluses, hash, err := splitFilter(r.Filter)
	if err != nil {
		findings = append(findings, fmt.Sprintf(
			"filter %s: %v", channel.Spelled(written, r.Filter), err))
	}

	// **No rule reaches into the reserved space, in any direction.** See
	// TopicRule.Filter for what it costs: pointed at a saguin, a rule
	// naming `$saguin/queue/<channel>` is a bridge taking leases it can
	// never acknowledge, so the far end's work redelivers, burns its
	// attempts and dead-letters because a bridge read it.
	//
	// The exact prefix and not every `$`, because the rest of that space is
	// somebody else's: `$SYS/#` on a foreign broker is an ordinary thing to
	// read, and refusing it would be saguin claiming a namespace it does
	// not define. A trailing wildcard cannot reach `$saguin` either, since
	// MQTT-4.7.2-1 says a wildcard does not match a topic beginning with
	// `$` - so what is left to refuse is the filter that names it outright,
	// which is exactly this.
	// **Held to limits.max_topic_levels at both ends**: a filter deeper than
	// any topic may be matches nothing, and a template is at least as deep as
	// it is written - a `$#` tail only adds levels, and a topic it makes past
	// the bound is refused as it lands, as any publish is.
	if err := channel.TooDeep(r.Filter, maxLevels); err != nil {
		findings = append(findings, fmt.Sprintf("filter %s %v", channel.Spelled(written, r.Filter), err))
	}
	if err := channel.TooDeep(r.Topic, maxLevels); err != nil && strings.TrimSpace(r.Topic) != "" {
		findings = append(findings, fmt.Sprintf("topic %q %v", r.Topic, err))
	}

	if f := strings.TrimSpace(r.Filter); f == channel.ReservedRoot || strings.HasPrefix(f, channel.ReservedRoot+"/") {
		findings = append(findings, fmt.Sprintf(
			"filter %s names the reserved %s space, where saguin defines its own control "+
				"topics. A rule there would carry a queue's deliveries across the link, "+
				"taking leases nothing acknowledges - so the work redelivers, spends its "+
				"attempts and dead-letters because a bridge read it (invariant 6)",
			channel.Spelled(written, r.Filter), channel.ReservedRoot))
	}

	// **`both` carries no `topic:`, and the reason is that it has to be
	// reversible.** A rule that runs in both directions maps one way and
	// has to map back: a template reordering captures could be inverted and
	// one dropping a level could not, so rather than a rule about which
	// templates invert, `both` is the identity mapping and anything else is
	// two rules. Same shape as mosquitto's prefix pair, which is a pair
	// precisely so the reverse exists.
	hasTopic := strings.TrimSpace(r.Topic) != ""
	switch {
	case !in && !out:
		// The direction itself is already a finding; every check below
		// would be about a direction nobody wrote.
	case in && out && hasTopic:
		findings = append(findings, fmt.Sprintf(
			"direction `both` with a topic: template. `both` maps a topic to itself so that "+
				"it maps back the same way; a template that drops or reorders levels has no "+
				"reverse. Write two rules, `in` and `out`, or drop topic: %q", r.Topic))
	case !hasTopic && !(in && out):
		findings = append(findings, "no topic, which is the topic a record takes. Only "+
			"`direction: both` may leave it out, where a topic maps to itself")
	case !hasTopic && strings.HasPrefix(strings.TrimSpace(r.Filter), "$"):
		// **`both` on a `$` topic is refused, because it can never
		// deliver.** It maps a topic to itself, so what it brings in would
		// be published here as `$SYS/…`, and no publisher may write a topic
		// beginning with `$`: every record would be refused, dropped and
		// logged twice, for the life of the bridge, from a line that loaded
		// cleanly. mosquitto's bridge
		// carries `$SYS` under a prefix for the same reason; `in` with a
		// template does that here.
		findings = append(findings, fmt.Sprintf(
			"direction `both` on %s: `both` maps a topic to itself, and nothing may publish a "+
				"topic beginning with `$` here, so every record it brought in would be refused. "+
				"Write it `in` with a topic: template outside `$`, such as peer/$#",
			channel.Spelled(written, r.Filter)))
	}

	if len(findings) > 0 {
		return Rule{}, findings
	}
	// `both` with no template: the topic maps to itself, and Match says so
	// rather than a template being synthesised for it - there is no
	// template language that spells "whatever arrived" for every filter.
	if !hasTopic {
		return Rule{
			Filter: r.Filter, In: in, Out: out, identity: true,
			levels: levels, hash: hash, pluses: pluses,
		}, nil
	}
	if err != nil {
		return Rule{}, findings
	}
	parts, terr := compileTemplate(r.Topic, pluses, hash)
	if terr != nil {
		return Rule{}, []string{fmt.Sprintf("topic %q: %v", r.Topic, terr)}
	}
	return Rule{
		Filter: r.Filter, Topic: r.Topic, In: in, Out: out,
		levels: levels, hash: hash, pluses: pluses, parts: parts,
	}, nil
}

// templateFilter reads a compiled template as a topic filter: every `$1`,
// `$2` … becomes `+`, because a capture is exactly one topic level, and `$#`
// becomes `#`, because a tail is the rest.
//
// It stands for every topic the template can build, which is what makes the
// check below decidable at startup - the thing a regular expression could
// never be, and the reason this file has a template language rather than one.
func templateFilter(parts []part) string {
	var b strings.Builder
	for _, p := range parts {
		switch p.index {
		case 0:
			b.WriteString(p.text)
		case tailCapture:
			b.WriteString("#")
		default:
			b.WriteString("+")
		}
	}
	return strings.TrimSuffix(b.String(), "/")
}

// The three words a rule's `direction:` may be.
const (
	DirectionIn   = "in"
	DirectionOut  = "out"
	DirectionBoth = "both"
)

// readDirection turns `direction:` into the two questions a rule is asked,
// or names what is wrong with it.
//
// **Omission is its own finding rather than a default**, and the message
// says what the three words do - an operator who left the key out is being
// told about a key they have never written, so naming the values is worth
// more than naming the key twice.
func readDirection(raw string) (in, out bool, finding string) {
	switch strings.TrimSpace(raw) {
	case DirectionIn:
		return true, false, ""
	case DirectionOut:
		return false, true, ""
	case DirectionBoth:
		return true, true, ""
	case "":
		return false, false, "no direction; write `in` to bring records here, `out` to send " +
			"them to the peer, or `both`. There is no default: an omitted key deciding " +
			"whether this broker's records leave it is not something you could see in the file"
	default:
		return false, false, fmt.Sprintf(
			"direction %q is not one of in, out or both", raw)
	}
}

// splitFilter reads an MQTT topic filter into its levels, and reports how
// many `+` it holds and whether it ends in `#`.
func splitFilter(filter string) (levels []string, pluses int, hash bool, err error) {
	if strings.TrimSpace(filter) == "" {
		return nil, 0, false, fmt.Errorf("empty")
	}
	if strings.ContainsRune(filter, 0) {
		return nil, 0, false, fmt.Errorf("contains a NUL byte")
	}
	levels = strings.Split(filter, "/")
	for i, l := range levels {
		switch {
		case l == "#":
			// MQTT-4.7.1-1: `#` is the last character of the filter. A `#`
			// with anything after it is not a wildcard that matches less - it
			// is a filter the upstream broker will refuse, at the first
			// connection rather than here.
			if i != len(levels)-1 {
				return nil, 0, false, fmt.Errorf("`#` is only ever the last level")
			}
			hash = true
		case l == "+":
			pluses++
		case strings.ContainsAny(l, "+#"):
			// MQTT-4.7.1-1 and -2: both wildcards occupy a whole level. `a+b`
			// is a literal level containing a plus sign, which is legal and is
			// almost never what somebody meant.
			return nil, 0, false, fmt.Errorf(
				"level %q mixes a wildcard with other characters; `+` and `#` each take a whole level", l)
		}
	}
	return levels, pluses, hash, nil
}

// compileTemplate reads a topic template into literal text and substitutions.
func compileTemplate(tmpl string, pluses int, hash bool) ([]part, error) {
	switch {
	case strings.HasPrefix(tmpl, "/"):
		return nil, fmt.Errorf("starts with `/`; a topic does not lead with a separator")
	case strings.HasSuffix(tmpl, "/"):
		return nil, fmt.Errorf("ends with `/`, which would give the topic an empty last level")
	case strings.ContainsRune(tmpl, 0):
		return nil, fmt.Errorf("contains a NUL byte")
	}
	// The wildcard check is below, on the compiled parts, and not here on the
	// whole string. It used to be here as "contains + or # and does not
	// contain `$#`", which exempted every template using a tail - because
	// `$#` contains a `#` and the check had to let it through somehow. So
	// `x/+/$#` and `x/#/$#` were both accepted, and a wildcard in a produced
	// topic is refused by the substrate every time (MQTT-3.3.2-2), which is
	// a record that can never be published sitting at the head of a link.

	var parts []part
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			parts = append(parts, part{text: lit.String()})
			lit.Reset()
		}
	}

	used := map[int]bool{}
	for i := 0; i < len(tmpl); {
		if tmpl[i] != '$' {
			lit.WriteByte(tmpl[i])
			i++
			continue
		}
		flush()
		i++

		if i < len(tmpl) && tmpl[i] == '#' {
			if !hash {
				return nil, fmt.Errorf("uses `$#`, but the filter has no `#` to fill it")
			}
			// The same rule the filter follows, for the same reason: a tail is
			// any number of levels, so a substitution with anything after it
			// puts those levels in the middle of a topic and the shape of the
			// result depends on how deep the upstream published.
			if i+1 != len(tmpl) {
				return nil, fmt.Errorf("`$#` is only ever at the end, as `#` is in a filter")
			}
			parts = append(parts, part{index: tailCapture})
			i++
			continue
		}

		j := i
		for j < len(tmpl) && tmpl[j] >= '0' && tmpl[j] <= '9' {
			j++
		}
		if j == i {
			return nil, fmt.Errorf("has a `$` that is not `$#` or a wildcard number such as `$1`")
		}
		n, convErr := strconv.Atoi(tmpl[i:j])
		if convErr != nil || n < 1 {
			return nil, fmt.Errorf("`$%s` is not a wildcard number; they start at `$1`", tmpl[i:j])
		}
		if n > pluses {
			return nil, fmt.Errorf("`$%d` names a wildcard the filter does not have; it has %s",
				n, plural(pluses, "`+` level"))
		}
		used[n] = true
		parts = append(parts, part{index: n})
		i = j
	}
	flush()

	// A `#` whose tail is thrown away collapses an unbounded number of
	// upstream topics onto one local topic. On an append channel that is a
	// mess; on a latest channel it is destructive, because two topics become
	// one value overwriting itself while the channel does exactly what it is
	// designed to do. A `+` that is thrown away collapses finitely many and is
	// a choice an operator can reasonably make - one bridge per vessel drops
	// the vessel id on purpose - so only the unbounded one is refused.
	if hash && !containsTail(parts) {
		return nil, fmt.Errorf(
			"the filter ends in `#` but the topic does not use `$#`, so every topic under it " +
				"would collapse onto one")
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty")
	}

	// No literal `+` or `#` anywhere, checked on the compiled parts so that
	// `$#` and `$1` - which are substitutions rather than text - cannot
	// excuse a wildcard sitting beside them.
	//
	// Anywhere, and not only as a whole level. MQTT forbids either character
	// in a published topic name at all (MQTT-3.3.2-2), so `a+b` is refused
	// exactly as `+` is: measured, a publish to `events/a+b/hold` answers
	// 0x82 the same as one to `events/x/+/hold`. A template producing one is
	// a record the broker can never accept, and it would sit at the head of
	// the bridge's link for ever.
	for _, p := range parts {
		if p.index != 0 {
			continue
		}
		if i := strings.IndexAny(p.text, "+#"); i >= 0 {
			return nil, fmt.Errorf(
				"contains a literal %q, which a published topic may not hold anywhere; "+
					"the filter's `+` levels are written `$1`, `$2` … and its `#` tail is `$#`",
				p.text[i])
		}
	}
	return parts, nil
}

func containsTail(parts []part) bool {
	for _, p := range parts {
		if p.index == tailCapture {
			return true
		}
	}
	return false
}

func plural(n int, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}
