// Package channel owns the mapping between the MQTT topic space and
// saguin's channels: which topics a channel claims, which subscriptions
// reach it, and which forms the broker refuses.
//
// Every rule here is enforced against what the client actually sent
// (invariant 10). Nothing in this package trusts a client to have used an SDK.
package channel

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ifnesi/saguin/internal/store"
)

// MaxNameLength bounds a configured channel name. It is a constant
// rather than a configuration key: a channel name is written by hand by
// the operator in a file the broker already trusts, so a limit on it
// protects nothing and tunes nothing (RFC 0002 "Channel names").
const MaxNameLength = 128

// ReservedRoot is the topic space saguin defines its own control verbs in -
// a queue delivery and acknowledgement, a seek, a point read. Nothing a
// client publishes may land inside it and no bridge rule may name it.
//
// It is a constant because five places ask the same question of a topic and
// each of them spelled the answer out. That is the shape a prefix drifts in:
// one site gains a rule, or loses a slash, and the space a broker defends is
// not quite the space it documents.
const ReservedRoot = "$saguin"

// QueuePrefix is where a queue is consumed, and the one place `$saguin/`
// carries a subscription rather than a publish. Pinning the whole form -
// this prefix and the channel's name, with nothing after it - is what
// makes a queue's competing-consumer population exactly one group
// (invariant 4).
const QueuePrefix = ReservedRoot + "/queue/"

// DLQSuffix names the append channel derived from every queue.
const DLQSuffix = "__dlq"

// Source is one channel an in-process reader drains, as much of it as such
// a reader needs: which channel, whether it holds a history or a value per
// topic, and where a reader with no stored position begins.
//
// **It lives here so that neither end has to import the other.** A bridge's
// outbound rule reads channels and the broker owns them, and the broker
// deliberately does not import the bridge - `BridgeStats` is an interface
// for the same reason. A shared shape in the package that owns the concept
// costs nothing and keeps that arrow pointing one way.
type Source struct {
	Name string
	// Latest says the channel holds a value per topic rather than a
	// history, which is the difference between resuming at a position and
	// re-reading current state.
	Latest bool
	// StartAtTail is the channel's own answer for a reader with no stored
	// position (`start: tail`). An outbound rule is a reader like any
	// other, so it takes that answer rather than inventing a third rule.
	StartAtTail bool
	// RetentionPeriod and DeletionRetentionPeriod are a latest channel's two
	// clocks, in seconds, 0 for none: past them its sweep removes a value,
	// or a deletion. A reader holding values to send later holds them to the
	// same clocks, so that it never sends one the channel no longer holds.
	RetentionPeriod, DeletionRetentionPeriod int64
}

// Type is a channel's semantic. Broadcast is deliberately absent: it is
// not a channel type, it is what saguin does with a topic no channel
// claims (RFC 0001).
type Type string

const (
	Append Type = "append"
	Latest Type = "latest"
	Queue  Type = "queue"
)

// Channel is one configured claim on a region of the topic space.
type Channel struct {
	Name string
	Type Type

	// Filter is the topic filter this channel claims, as the operator
	// wrote it - `iot/water/+/location/#`. A channel that writes none gets
	// `<name>/#`, which is what a channel name has always claimed, so
	// every configuration written before filters existed keeps its meaning.
	//
	// **The name is identity and the filter is placement**, and they are
	// deliberately separate: the name is a snapshot file, a storage key, a
	// metric label, three `$saguin/` control topics and a queue's
	// subscription form, none of which gains anything from the hierarchy the
	// filter carries (RFC 0002 "Channel names").
	Filter string

	// Filters is Filter with its `{a,b}` levels expanded, which is what the
	// registry routes on. A filter with no braces expands to itself, and a
	// queue's may hold none at all - two filters are two pinned strings and
	// two pinned strings are two consumer groups (invariant 4).
	Filters []string

	// Storage is the provider holding this channel's records, or "" when
	// the configuration defines no providers and nothing is durable.
	Storage string

	// MaxBytes is the most this channel may hold before a publish is
	// refused with 0x97, or zero for no bound. It removes nothing: a
	// channel at its bound refuses, which on a queue is backpressure -
	// resolution frees the space and it accepts work again (RFC 0002).
	//
	// A latest channel never has one. It holds a value per topic rather
	// than a history, so what grows there is the number of topics, and the
	// retention period is the tool for a topic nothing publishes to.
	MaxBytes int64

	// Retention is what REMOVES records, where MaxBytes refuses the publish
	// that would exceed. Zero means none - keep everything - which an
	// operator states as the word `none` rather than reaching by omission,
	// because a channel that does not say gets the broker-wide default.
	//
	// A queue has neither: removing unacknowledged work by age or size is
	// eviction of unresolved work (invariant 2). Its dead-letter channel is
	// an ordinary append channel and has both, from the pair below.
	//
	// On a latest channel the period means something else, and RFC 0003
	// says so where expiry is described: it deletes the current value of a
	// topic that has gone quiet, so a device reporting less often than the
	// period loses its state between reports. A latest channel has no
	// RetentionBytes - what grows there is the topic count, not a history.
	// StartAtTail says a subscriber with no stored position begins at the
	// channel's next offset rather than at its retention floor - `start:
	// tail` in the file. It applies to `append` channels and to every
	// reader of one alike, whatever protocol they speak (RFC 0003 "Where a
	// subscription starts").
	StartAtTail bool

	RetentionPeriod int64 // seconds
	RetentionBytes  int64

	// DeletionRetentionPeriod is how long a `latest` channel keeps a
	// deletion - a stored value with no payload - before removing it for
	// good. Zero on every other type, where nothing is stored for a
	// deletion at all.
	//
	// It is a period of its own rather than the one above because a value
	// and a deletion are not the same thing to keep. A value lives as long
	// as it is the truth; a deletion only has to live long enough for
	// everything that reads this channel to have seen it. Held under the
	// value period, a channel keeping values for ever would keep a row for
	// every device ever decommissioned.
	DeletionRetentionPeriod int64 // seconds

	// Queue policy. Zero on every other type.
	VisibilityTimeout int64 // seconds
	JobExpiresAfter   int64 // seconds; 0 means work never expires
	MaxAttempts       int

	// Backoff is the gap before a returned record is offered again, and
	// BackoffBase is what that gap is built from. Zero is store.BackoffNone
	// - offered at once, which is what every queue did before these existed
	// and what one that says nothing still does.
	//
	// It is asked only of a record that is available, so it never competes
	// with VisibilityTimeout above: that governs a record a worker holds,
	// this governs one waiting to go out. store.Backoff has the whole of it.
	Backoff     store.BackoffKind
	BackoffBase int64 // seconds

	// DLQRetention is what the derived dead-letter channel gets, spelled in
	// the queue's own block because a derived name is part of the topic
	// contract and there is nowhere else to write it. Zero on every other
	// type, and copied onto DLQ below.
	DLQRetentionPeriod int64 // seconds
	DLQRetentionBytes  int64

	// DLQ is the derived companion for a queue, nil otherwise.
	DLQ *Channel
}

// QueueFilter is the only subscription form a queue admits: QueuePrefix
// and this channel's **name**, with nothing after it.
//
// **The name, because everything else a client says about a channel is the
// name.** A seek is `$saguin/consumer/<name>/seek`, a worker answers on
// `$saguin/queue/<name>/response`, failed work lands in `<name>__dlq`, the
// operations route is `/v1/operations/queues/<name>`, and the ACL
// authorizes by channel name.
//
// **A topic of saguin's own, rather than the shared subscription
// `$share/saguin/<name>/#` this replaced.** Reserving a ShareName made a
// standard MQTT 5 feature behave differently depending on which channel a
// filter happened to land in, and it cost a page of refusal rules that
// existed only to explain the choice. A client writing `$share` is asking
// for MQTT semantics - live load balancing, no offsets, no replay - and
// gets exactly that, on every channel type, refused nowhere. Consuming a
// queue does not have to share a namespace with a protocol feature.
//
// One consumer group is still one exact string, and every other spelling
// is refused rather than made to work: two spellings are two populations
// that each take a copy of every job (invariant 4). What enforces that is
// no longer MQTT's ShareName-and-filter pairing but saguin's own index of
// who is consuming which queue - the broker picks the worker itself
// (OnSelectSubscribers), so a second spelling is a subscription that names
// no queue rather than a second group.
func (c *Channel) QueueFilter() string {
	return QueuePrefix + c.Name
}

// ResponseTopic is where a worker publishes ack or return for this
// queue's deliveries (RFC 0003 "Acknowledgement and return").
//
// **It sits one level below the topic the work arrives on**, which is what
// makes `$saguin/queue/<name>/#` a filter straddling both - a worker
// subscribing to it would be sent its own acknowledgements. QueueName
// refuses every form but the exact one for that reason.
func (c *Channel) ResponseTopic() string {
	return QueuePrefix + c.Name + responseSuffix
}

const responseSuffix = "/response"

// SeekTopic is where a consumer publishes to move its own position in this
// channel (RFC 0003 "Moving a consumer's position").
func (c *Channel) SeekTopic() string {
	return "$saguin/consumer/" + c.Name + "/seek"
}

// SeekReplyTopic is where a consumer reads the answer to its own seek when
// it set no Response Topic (RFC 0003 "Moving a consumer's position").
//
// **Derived from the seek topic rather than spelled out**, so the reserved
// prefix stays written in one place and a reply cannot drift away from the
// request it answers.
//
// It is one of the two topics under `$saguin/` a client may subscribe to,
// the other being a queue's own form (QueueFilter). Nothing may be
// published to it - the ones saguin defines are unchanged - and what makes
// that safe is that the answer is written to the seeking client's socket
// rather than published: a subscription here routes a client's own replies
// and can never carry anybody else's, whoever else is subscribed.
//
// **The two are safe for different reasons and neither reason covers the
// other.** This one carries nothing a subscriber did not ask for because of
// how the reply is written; a queue's form carries another client's work by
// design, and what makes that safe is the `consume` grant the acl_file
// answers for it (authz.Allows).
func (c *Channel) SeekReplyTopic() string {
	return c.SeekTopic() + seekReplySuffix
}

const seekReplySuffix = "/reply"

// SeekReplyChannel is the channel name in a seek reply topic, and whether
// the topic was one.
//
// **It asks SeekChannel rather than parsing again**, so the rule that a
// channel name is one level - and the reasoning behind it - lives in one
// function. A second copy would be a second answer the day that rule moves.
func SeekReplyChannel(topic string) (string, bool) {
	rest, ok := strings.CutSuffix(topic, seekReplySuffix)
	if !ok {
		return "", false
	}
	return SeekChannel(rest)
}

// SeekChannel is the channel name in a seek topic, and whether the topic
// was one. It is a parse rather than a comparison against every channel
// because the answer for a name no channel has must be "that is a seek for
// a channel I do not have" rather than "that is not a seek", which are
// different refusals to a client.
//
// The middle is one topic level because a channel name is one: no name may
// hold a `/`, so a topic with more levels than this names no channel that
// could exist, and refusing it here costs a client nothing it could have
// had. That was not always true. While a name could be hierarchical, this
// check made the seek for `site/events` unrecognisable - it fell through to
// the queue response handler, which answered PUBACK Success, sent no reply
// to a client that was waiting for one, and logged a complaint about a
// payload that was neither ack nor return.
func SeekChannel(topic string) (string, bool) {
	const prefix, suffix = "$saguin/consumer/", "/seek"
	name, ok := controlName(topic, prefix, suffix)
	if !ok || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

// ResponseChannel is the queue name in a response topic, and whether the
// topic was one. It is the reverse of ResponseTopic above, and it exists so
// that the broker can tell a topic it defines from one it does not: a
// publish into the reserved `$saguin/` space that is neither a seek nor a
// response is refused with 0x90 (RFC 0002 "Publishing"), and without this
// there was nothing to compare against.
//
// The middle is one topic level for the same reason as a seek's: a channel
// name may hold no `/`, so a topic with more levels names no queue that
// could exist.
func ResponseChannel(topic string) (string, bool) {
	name, ok := controlName(topic, QueuePrefix, responseSuffix)
	if !ok || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

// controlName is what a control topic holds between its prefix and its
// suffix, and whether it has both.
//
// **The suffix is cut from what follows the prefix, never from the whole
// topic.** Asking HasPrefix and HasSuffix of the whole topic and slicing
// between them let the two overlap: `$saguin/consumer/seek` has the prefix
// `$saguin/consumer/` and the suffix `/seek`, sharing its `/`, and slicing
// from 17 to 16 panicked. It was one PUBLISH from any client a door
// admitted, before any permission was asked, and the broker exited
// (FuzzControlTopicParsersNeverPanic).
func controlName(topic, prefix, suffix string) (string, bool) {
	rest, ok := strings.CutPrefix(topic, prefix)
	if !ok {
		return "", false
	}
	return strings.CutSuffix(rest, suffix)
}

// QueueName is the channel name in a queue's subscription form, and
// whether the filter was one.
//
// **A parse rather than a comparison against every queue**, for the reason
// SeekChannel is one: the answer for a name no queue has must be "that
// names a queue I do not have" rather than "that is not a queue
// subscription", because they are different refusals to a client.
//
// The name is one topic level because a channel name is one, so every
// wider or narrower spelling falls out here rather than needing a rule of
// its own: `$saguin/queue/jobs/#` straddles the work topic and the
// response topic, `$saguin/queue/#` reaches every queue at once, and
// `$saguin/queue/jobs/urgent` names no channel that could exist.
func QueueName(filter string) (string, bool) {
	name, ok := strings.CutPrefix(filter, QueuePrefix)
	if !ok || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

// KVGetTopic is where a client asks for the current value of one topic on a
// `latest` channel. The key is the payload.
//
// **It names no channel, unlike the other two topics saguin defines.** A key
// is a topic and a topic already resolves to a channel, so naming the
// channel here as well would be two sources for one answer, and the broker
// would have to decide what to do when they disagree.
const KVGetTopic = "$saguin/kv/get"

// IsKVGet reports whether a topic is the point-read request, and exists for
// the same reason ResponseChannel does: a publish into the reserved
// `$saguin/` space that is none of the topics saguin defines is refused with
// 0x90, and without this there would be nothing to compare against.
func IsKVGet(topic string) bool { return topic == KVGetTopic }

// DisconnectTopic is where an operator asks the broker to hang up a
// connected client. The client id to hang up is the payload.
//
// **In the payload rather than in the topic**, for the reason a point read
// puts its key there: a client id is whatever the device typed - `/`
// included, since MQTT bounds its length and not its alphabet - and a topic
// carrying one could not be parsed back into it.
//
// It names no channel because hanging a client up is not an act on
// anybody's records. That is also why its permission is not a channel verb:
// RFC 0002's `broker: sessions` rule kind exists for it.
const DisconnectTopic = "$saguin/sessions/disconnect"

// SessionsFacility is the name of that rule kind's one facility, as an
// acl_file writes it. It is here beside the topic so the two cannot drift:
// a verb whose grant names a different facility is granted to nobody.
const SessionsFacility = "sessions"

// FeaturesFacility is the acl_file's `broker: features` rule kind, which takes
// away a feature a client would otherwise have: `persistent`, `will`, `share`
// and `retained`, each named for the broker block that configures it. It
// names no topic, and it is spelled here beside SessionsFacility so the
// acl_file's word for it and the broker's cannot drift.
const FeaturesFacility = "features"

// IsDisconnect reports whether a topic is that request, and exists for the
// same reason IsKVGet does: anything in the reserved space that is none of
// the topics saguin defines is refused with 0x90, and without this there
// would be nothing to compare against.
func IsDisconnect(topic string) bool { return topic == DisconnectTopic }

// cataloguePrefix is where a client asks what a channel is.
const cataloguePrefix = "$saguin/catalogue/"

// CatalogueTopic is where a client asks for one channel's type and the
// filter it claims (RFC 0002 "Asking what a channel is").
//
// **The name is in the topic, as a seek's is**, rather than in the payload
// like a point read's key. A key is a topic and a topic resolves to a
// channel on its own; a channel name resolves to nothing, so it has to be
// said - and saying it in the topic is what lets the substrate's own
// per-topic machinery see the request for what it is.
func CatalogueTopic(name string) string { return cataloguePrefix + name }

// CatalogueChannel is the channel name in such a topic, and whether the
// topic was one.
//
// It is a parse rather than a comparison against every channel, for the
// reason SeekChannel gives: the answer for a name no channel has must be
// "that is a question about a channel I do not have" rather than "that is
// not a question", which reach different answers below. The middle is one
// topic level because a channel name is one.
func CatalogueChannel(topic string) (string, bool) {
	name, ok := strings.CutPrefix(topic, cataloguePrefix)
	if !ok || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

// Registry resolves topics and filters to channels.
type Registry struct {
	byName map[string]*Channel
	// routes, most exact first, so that Resolve is a walk that returns the
	// first match (invariant 12). MoreExact does not depend on any topic,
	// which is what lets the order be settled once here rather than
	// recomputed per publish - and what makes it independent of the order
	// channels appear in the configuration file, or of which file they
	// were split into.
	routes []route
}

// route is one expanded filter and the channel that claims it.
type route struct {
	filter string
	ch     *Channel
}

// NewRegistry validates a set of channels and derives the dead-letter
// companion for every queue.
func NewRegistry(chans []*Channel) (*Registry, error) {
	r := &Registry{byName: map[string]*Channel{}}

	for _, c := range chans {
		if err := validName(c.Name); err != nil {
			return nil, fmt.Errorf("channel %q: %w", c.Name, err)
		}
		if strings.HasSuffix(c.Name, DLQSuffix) {
			return nil, fmt.Errorf("channel %q: the %s suffix is reserved", c.Name, DLQSuffix)
		}
		if _, dup := r.byName[c.Name]; dup {
			return nil, fmt.Errorf("channel %q: declared twice", c.Name)
		}
		r.byName[c.Name] = c
	}

	// Placement. A channel writing no filter claims `<name>/#`, which is
	// what its name claimed before filters existed - so every configuration
	// written until now keeps its meaning without being edited.
	for _, c := range chans {
		if c.Filter == "" {
			c.Filter = c.Name + "/#"
		}
		if c.Type == Queue && HasBraces(c.Filter) {
			return nil, fmt.Errorf(
				"channel %q: filter %q: a queue's filter holds no `{a,b}` - its workers pin "+
					"one exact string, and two strings are two consumer groups that each take "+
					"a copy of every job", c.Name, c.Filter)
		}
		// **Braces expand first, and every MQTT rule is asked of what they
		// expanded to.** The order is the check rather than a tidiness: a
		// brace is opaque to a rule about levels, so `{$SYS,a}/#` reads as a
		// first level that is spelled out and does not begin with `$` - and
		// then expands to a route claiming the broker's own `$SYS` tree. The
		// same holds for `{a,+}`, a level that looks literal and is not.
		// Asking the rules of the written form leaves one hole per rule;
		// asking them of the expansions leaves none.
		expanded, err := Expand(c.Filter)
		if err != nil {
			return nil, fmt.Errorf("channel %q: filter %q: %w", c.Name, c.Filter, err)
		}
		for _, f := range expanded {
			if err := ValidFilter(f); err != nil {
				return nil, fmt.Errorf("channel %q: filter %s: %w", c.Name, Spelled(c.Filter, f), err)
			}
			if err := NoDLQLevel(f); err != nil {
				return nil, fmt.Errorf("channel %q: filter %s: %w", c.Name, Spelled(c.Filter, f), err)
			}
		}
		c.Filters = expanded
	}

	// Every queue derives its dead-letter companion. It is an append
	// channel and is never configured directly.
	//
	// It takes the queue's storage provider, which is what lets the move
	// out of the queue and into it be one operation rather than two that
	// can fail apart (invariant 5). The operator never writes this: the
	// __dlq suffix is refused as a configured name above, so there is
	// nowhere to write it even if they wanted to.
	for _, c := range chans {
		if c.Type != Queue {
			continue
		}
		dlq := &Channel{
			Name: c.Name + DLQSuffix, Type: Append, Storage: c.Storage,
			// The queue's two dlq_retention_* keys, which is the only way to
			// reach a derived channel's retention: it cannot be configured
			// under its own name because that name is not the operator's to
			// write.
			RetentionPeriod: c.DLQRetentionPeriod,
			RetentionBytes:  c.DLQRetentionBytes,
		}
		// Its filter is derived too, and so is the topic each dead-lettered
		// record takes: a `__dlq` level where the queue's filter carries its
		// `#`, or appended where it carries none. That rewrite is what makes
		// a dead letter readable at all - a subscriber finds a channel
		// through the filters and nothing else, so a record still carrying
		// the queue's own topic would belong to the queue and be refused to
		// everyone who is not a worker.
		dlq.Filter = DLQFilter(c.Filters[0])
		dlq.Filters = []string{dlq.Filter}
		c.DLQ = dlq
		r.byName[dlq.Name] = dlq
	}

	// Invariant 12: which channel a topic belongs to is decided by the
	// configuration and never by the broker. Two channels carrying the same
	// filter are the one tie the rule can be handed, and there is nothing in
	// the file to break it with - so it is refused here, naming both.
	//
	// Nothing narrower needs refusing. If two filters both match a topic and
	// score the same at every level of it, then every spelled-out level of
	// each equals that topic's own, so the two are character for character
	// identical and this map catches them.
	claimed := map[string]*Channel{}
	for _, c := range r.byName {
		for _, f := range c.Filters {
			if other, dup := claimed[f]; dup {
				a, b := min(c.Name, other.Name), max(c.Name, other.Name)
				return nil, fmt.Errorf(
					"channels %q and %q both claim the filter %q: which one holds a topic "+
						"is decided by the filters, and two identical filters decide nothing",
					a, b, f)
			}
			claimed[f] = c
			r.routes = append(r.routes, route{filter: f, ch: c})
		}
	}

	// Most exact first, so that Resolve returns the first match.
	//
	// **The filter string breaks a tie in the scores, and that second line
	// is load-bearing.** Two filters score the same at every level without
	// being the same filter - `iot/water/+/inspect` and
	// `iot/weather/+/data` are both spelled out, spelled out, `+`, spelled
	// out. Resolution does not care, because two filters that score alike
	// and both match one topic are identical and were refused above, so any
	// tie here is between filters that never meet. But the *order* is
	// visible: `--route` prints this table, and left to an unstable sort
	// over a map walk it printed a different one on every run. A table an
	// operator diffs between two runs must not be noise, and "the broker's
	// map order shows through" is the shape invariant 12 exists to refuse
	// even where it decides nothing.
	slices.SortFunc(r.routes, func(a, b route) int {
		if n := MoreExact(b.filter, a.filter); n != 0 {
			return n
		}
		return strings.Compare(a.filter, b.filter)
	})

	return r, nil
}

// Spelled names a filter in an error: the written one, plus what it
// expanded to when a brace made the two differ. An operator looking for the
// line to edit needs the form they wrote; an operator working out *why*
// needs the form the rule was applied to.
//
// Exported because the acl_file and the bridge rules need the same
// sentence: both are written with braces too, and a complaint about one
// has to name the line to edit as well as the spelling that is wrong.
//
// **It quotes, so callers take it with `%s`.** Quoting the whole thing at
// the call site put one quotation mark before the written filter and the
// other after the expansion, with the backticks between them pairing with
// nothing - `filter "a/{b,c}`, which expands to `a/b":` - which is a
// message about two filters wearing the punctuation of one.
func Spelled(written, expanded string) string {
	if written == expanded {
		return fmt.Sprintf("%q", written)
	}
	return fmt.Sprintf("%q, which expands to %q", written, expanded)
}

// All returns every channel, including derived dead-letter companions.
func (r *Registry) All() map[string]*Channel { return r.byName }

// Get returns a channel by exact name.
func (r *Registry) Get(name string) *Channel { return r.byName[name] }

// Resolve returns the channel claiming a published topic, or nil when no
// channel does - in which case the topic is ordinary broadcast.
//
// The routes are sorted most exact first, so the first filter that matches
// is the one that spells the topic out most exactly, which is the whole of
// invariant 12's rule. Same shape as when a channel claimed its name: a
// linear walk over a list settled at startup.
func (r *Registry) Resolve(topic string) *Channel {
	for _, rt := range r.routes {
		if Matches(rt.filter, topic) {
			return rt.ch
		}
	}
	return nil
}

// Routes returns every filter in resolution order with the channel it
// claims, which is what `--route` and `--check-config --output` print.
// It is the same slice resolution walks, so the two cannot disagree.
func (r *Registry) Routes() (filters []string, channels []*Channel) {
	for _, rt := range r.routes {
		filters = append(filters, rt.filter)
		channels = append(channels, rt.ch)
	}
	return filters, channels
}

// ResolveFilters returns every channel a subscription filter touches, most
// exact first - a channel it touches being one where some topic matches
// both its filter and this one.
//
// A subscriber is served from all of them, each record under the semantics
// its own topic has, so that a client subscribes the way MQTT already works
// and never has to know where the channels are (RFC 0002 "What a filter
// reaches"). Queues are in the answer too and the caller leaves them out:
// this is geometry, and what a queue admits is QueueSubscriptionError's.
//
// **The test is deliberately over-inclusive and that is safe.** A filter
// can touch a channel whose records it will never match, because a more
// exact filter took every topic they had in common - `iot/water/w-7/#`
// touches an append channel filtered `iot/water/+/+/#` even where a queue
// holds `iot/water/+/work`. It costs a delivery goroutine that finds
// nothing. It cannot leak a record, because a channel only ever holds
// records whose topics resolved to it in the first place.
func (r *Registry) ResolveFilters(filter string) []*Channel {
	var out []*Channel
	seen := map[string]bool{}
	for _, rt := range r.routes {
		if !Intersects(rt.filter, filter) || seen[rt.ch.Name] {
			continue
		}
		seen[rt.ch.Name] = true
		out = append(out, rt.ch)
	}
	return out
}

// containingRoute returns the most exact route inside which every topic a
// filter can match lies, and the channel that route claims.
//
// This is the queue's question rather than the subscriber's. A filter
// wholly inside a queue's filter is asking for part of that queue and is
// refused unless it is the pin; a filter that merely crosses one - `#`,
// `iot/#` - is contained by nothing and is granted.
//
// **Most exact matters here.** A queue whose filter ends in `#` contains
// its own derived dead-letter filter, so both routes contain a reader's
// `…/work/__dlq`. The routes are sorted, so the first hit is the dead-letter
// channel and the reader is a consumer rather than a worker spelling its
// queue wrongly.
func (r *Registry) containingRoute(filter string) (string, *Channel) {
	for _, rt := range r.routes {
		if Contains(rt.filter, filter) {
			return rt.filter, rt.ch
		}
	}
	return "", nil
}

// Owner returns the channel every topic a filter can match resolves to, or
// nil when they can resolve to more than one channel or to none. It is the
// first route in resolution order the filter touches, when that route also
// contains the filter: an earlier route touching it would take some of its
// topics, and a later one never takes any.
//
// For a topic it is Resolve. For a filter it is what Resolve answered
// wrongly, reading the filter as a topic: a route's `+` or `#` took the
// filter's own wildcard as a level, so `a/#` "resolved" to a channel
// filtered `a/+` though `a` and `a/b/c` are not that channel's.
func (r *Registry) Owner(filter string) *Channel {
	for _, rt := range r.routes {
		if !Intersects(rt.filter, filter) {
			continue
		}
		if Contains(rt.filter, filter) {
			return rt.ch
		}
		return nil
	}
	return nil
}

// ChannelContaining returns the channel a filter belongs to - every topic
// it can match lying inside that channel's filter - or nil when it belongs
// to none. A filter that merely crosses channels belongs to none.
func (r *Registry) ChannelContaining(filter string) *Channel {
	_, c := r.containingRoute(filter)
	return c
}

// CanonicalQueue is the queue a subscription asks for outright, or nil.
//
// **Compared as a name, not matched as a filter**: the form carries the
// channel's name, and a queue filtered `iot/+/work/+` matches nothing under
// its own name. A filter that merely happens to look like the form is not
// the same request.
//
// A name that is a channel of some other type answers nil, so it falls
// through to the refusal rather than being granted and never fed. That is
// the case the documents invite: they say failed work lands in
// `<queue>__dlq`, and a reader putting that name in this form would be
// asking to consume an append channel through a queue's door.
func (r *Registry) CanonicalQueue(filter string) *Channel {
	name, ok := QueueName(filter)
	if !ok {
		return nil
	}
	if c := r.Get(name); c != nil && c.Type == Queue {
		return c
	}
	return nil
}

// QueueSubscriptionError reports why a filter may not be subscribed to, or
// "" when it may. The channel it returns is the queue the filter was judged
// against, and is nil where the refusal names no queue - a caller reporting
// the canonical form must check it.
//
// Two refusals, and neither of them is about `$share` any more.
//
// **A filter in the queue space that is not a queue's form.** Everything
// under QueuePrefix is saguin's, so a spelling that names no queue asks
// for something the broker can never feed: granted, connected, and empty
// for ever. Three ways to arrive there, each one keystroke from a correct
// line - a misspelt channel name, a name of some other channel type
// (`<queue>__dlq` is the one the documents invite), and a wildcard or an
// extra level, which QueueName refuses because a channel name is one
// topic level.
//
// **A plain filter lying entirely inside a queue's filter.** A queue
// admits its form and nothing else, so a subscriber asking for part of a
// queue's topics through an ordinary filter is refused rather than served
// records that belong to a worker (invariant 11). One that merely
// *crosses* a queue - `#`, `iot/#` - is contained by nothing, is an
// ordinary subscriber, and is granted: refusing it would break
// `mosquitto_sub -t '#'`, the first thing anyone types at a new broker,
// and it is served everything except the queue's records.
//
// **A shared subscription is judged as the ordinary filter it is.** It is
// never contained by a queue's filter - its first level is `$share`, and
// no channel's filter may begin with `$` - so it reaches here and is
// granted, which is the point of the change: `$share` is MQTT's and asks
// for MQTT semantics. A shared group over a queue's topics receives
// nothing, because queue records leave through the queue's own path and
// never through the substrate's fan-out. Refusing it instead was
// considered and dropped: it reintroduces the channel analysis this change
// deletes, and under this scheme nobody consuming a queue is using
// `$share` at all.
func (r *Registry) QueueSubscriptionError(filter string, qos byte) (*Channel, string) {
	if strings.HasPrefix(filter, QueuePrefix) {
		c := r.CanonicalQueue(filter)
		if c == nil {
			return nil, QueuePrefix + " is where a queue is consumed and this names none - " +
				"a queue is consumed through " + QueuePrefix + "<channel>, the queue's " +
				"name and nothing after it"
		}
		if qos != 1 {
			// The form admits QoS 1 and no other. At QoS 0 there is no
			// transport acknowledgement to start the visibility timeout from
			// (invariant 7), and no MQTT-level redelivery on disconnect. A
			// QoS 2 *request* never reaches here: OnSubscribe caps it to 1
			// before the substrate stores it - MQTT grants the lower of what
			// was asked and what is offered - because the offer is sent at
			// QoS 1 whatever the grant says, and "granted 2" would be a
			// grant the delivery never honours. So a 2 arriving here is a
			// granted subscription the cap never touched, and the caller
			// disconnects it as it does any grant saguin refused.
			return c, "a queue subscription requires QoS 1"
		}
		return c, ""
	}

	_, c := r.containingRoute(filter)
	if c == nil || c.Type != Queue {
		return nil, ""
	}
	return c, "a queue is consumed only through " + c.QueueFilter()
}

// validName admits a-z, A-Z, 0-9, and the three punctuation characters a
// Kafka topic admits: `-`, `_` and `.` (RFC 0002 "Channel names").
//
// An allow-list rather than a list of the characters that break something,
// because the two are not the same size. Refusing `+`, `#`, NUL and a
// leading `$` covers what MQTT reserves and nothing else, and every rule
// that follows from a name then has to be re-derived for whatever is left:
// `/` made a name hierarchical, which made it a prefix another name could
// overlap, which made it a file name that needed encoding, which made a
// name inside the length limit still too long to store. An allow-list
// answers all of those once.
//
// `.` and `..` are refused although both are inside the set. They are a
// directory rather than a channel in every listing an operator will read,
// and a name that means "here" is a name somebody will misread.
func validName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("empty")
	case len(name) > MaxNameLength:
		return fmt.Errorf("longer than %d bytes", MaxNameLength)
	case name == "." || name == "..":
		return fmt.Errorf("is %q, which names a directory rather than a channel", name)
	}
	for i := range len(name) {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' {
			continue
		}
		return fmt.Errorf(
			"contains %q: a channel name holds only letters, digits, and - _ .", string(c))
	}
	return nil
}
