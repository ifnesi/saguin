package broker

import (
	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/channel"
)

// rules is the Authorizer an acl_file becomes: it answers from the file,
// and it is asked only after saguin's structural refusals have passed.
type rules struct{ r *authz.Rules }

// Identity is the name a rule can be about: the password-file user name, a
// client certificate's name - its Common Name, or else its first DNS name -
// or a proxy's, which the connect hook puts in the same field.
//
// **A client with none reaches here, and its identity is the empty
// string** - whatever user name it typed, since a name nothing checked is
// never one (connectAuth.authenticate). An acl_file requires a password file
// or a client authority, but a listener may still admit anonymous clients: a
// pure-certificate estate's Unix socket, a plain ws door. It is not
// rule-less: `*` matches an empty name like any other, so such a client is
// granted whatever the wildcard grants. Written allow_anonymous beside an
// acl_file is refused; reached without it, the start and --check-config
// name the door (config AnonymousBesideACL).
func identity(cl *mqtt.Client) string { return string(cl.Properties.Username) }

func (a rules) Allows(identity, clientID, topic string, write bool) bool {
	return a.r.Allows(identity, clientID, topic, write)
}

// PublishLimits is what the acl_file says a client of this identity may
// send in a second. The identity is the user name, which is what every
// other rule in that file is written about - see withinPublishRate for why
// the budget it selects is nonetheless held per client id.
func (a rules) PublishLimits(identity string) (int, int64, bool) {
	return a.r.PublishLimits(identity)
}

func (a rules) HasPublishLimits() bool { return a.r.HasPublishLimits() }

// AllowsClientID is the acl_file's `client_ids:` answered at connect.
func (a rules) AllowsClientID(identity, clientID string) bool {
	return a.r.AllowsClientID(identity, clientID)
}

// WithheldGrants is how many acl_file rules name %u or %c and grant this
// pair nothing, because the name they would put in holds a character a
// topic filter reads as its own (authz substitute).
func (a rules) WithheldGrants(identity, clientID string) int {
	return len(a.r.Withheld(identity, clientID))
}

// DeniesFeature is the acl_file's `broker: features` denial.
func (a rules) DeniesFeature(identity, clientID, feature string) bool {
	return a.r.DeniesFeature(identity, clientID, feature)
}

// feature is one of the things MQTT gives a client that a `broker: features`
// rule may take away, each named for the broker block that configures it, as
// a closed set of constants for the reason verb and facility are.
type feature string

const (
	featurePersistent feature = "persistent"
	featureWill       feature = "will"
	featureShare      feature = "share"
	featureRetained   feature = "retained"
	featureQoS2       feature = "qos2"
)

// deniesFeature answers whether this client is denied a feature. False where
// no acl_file is configured, which is every deployment that has not asked
// for one.
func (b *Broker) deniesFeature(cl *mqtt.Client, f feature) bool {
	a := b.authorizer()
	return a != nil && a.DeniesFeature(identity(cl), cl.ID, string(f))
}

// Authorize installs the rules an acl_file carries, and is what main calls
// once it has read one.
func (b *Broker) Authorize(r *authz.Rules) { b.SetAuthorizer(rules{r: r}) }

// mayDeliver answers whether this client may still be *sent* a record on this
// topic, and it is asked per record on the routes saguin writes itself.
//
// **The substrate asks per delivery and saguin did not.** A queue offer and a
// broadcast delivery go out through the substrate, which calls OnACLCheck
// every time; an `append` or `latest` delivery - and a dead-letter record,
// which is the same pump - is written by this package, and that path asked
// the acl_file once, at SUBSCRIBE. That was sound while the file could not
// change. The moment SIGUSR1 could re-read it (RFC 0002, "Withdrawing a
// device's access") it became an authorization failure inside the feature
// built to end one: an operator narrowed a role, signalled, watched the
// broadcast stop, hung the client up - and the device resumed its durable
// session, sent no SUBSCRIBE for anything to refuse, and went on receiving
// channel data indefinitely.
//
// **Per record rather than per channel**, because a channel grant may carry a
// `filter:` that narrows which topics inside it are covered, so the answer is
// not a property of the channel alone. The cost is one atomic load on a
// broker with no acl_file, which is every deployment that has not asked for
// one, and the same grant walk the publish path already pays on one that has.
func (b *Broker) mayDeliver(cl *mqtt.Client, topic string) bool {
	return b.permits(cl, topic, false)
}

// noteRefusedDelivery logs a stalled consumer once per acl_file rather than
// once per record.
//
// A consumer whose grant has been taken away is pumped again on every append,
// so a line per refusal is a line per publish for as long as the operator
// leaves it that way - a log that floods at exactly the moment somebody is
// reading it. Keyed by the generation, so the next re-read says so again:
// what an operator wants to see is the state changing, and the file changing
// is the only way it can.
//
// **The caller holds no lock**, because this takes one.
func (b *Broker) noteRefusedDelivery(cl *mqtt.Client, chName, topic string) {
	gen := b.authzGen.Load()
	key := cl.ID + "\x00" + chName
	b.mu.Lock()
	// **Not for a connection that no longer holds its id** (invariant 17).
	// The pump that calls this runs beside the teardown, and an entry
	// written after the teardown swept this client's stayed until the id
	// next disconnected as owner.
	if b.owner[cl.ID] != cl {
		b.mu.Unlock()
		return
	}
	said, seen := b.refusedRead[key]
	if b.refusedRead == nil {
		b.refusedRead = map[string]uint64{}
	}
	b.refusedRead[key] = gen
	b.mu.Unlock()
	if seen && said == gen {
		return
	}
	// **The position is not advanced past it**, which is the half worth
	// saying: the consumer is stalled rather than skipped, so nothing is lost
	// and it resumes where it stopped if the grant comes back.
	b.log.Warn("the acl_file no longer allows this subscriber to read here, so it is "+
		"not being fed; its position is unchanged",
		"client", b.limits.Loggable(cl.ID), "channel", chName,
		"topic", b.limits.Loggable(topic))
}

// permitsVerb answers a named operation on a named channel - the control
// verbs, which the topic and direction alone cannot name: a seek is a
// publish to `$saguin/consumer/<channel>/seek`, and a point read names its
// key in its payload.
//
// True when no acl_file is configured, which is every deployment that has
// not asked for one.
func (b *Broker) permitsVerb(cl *mqtt.Client, c *channel.Channel, topic string, v verb) bool {
	a := b.authorizer()
	r, ok := a.(rules)
	if !ok {
		return true
	}
	return r.r.AllowsVerb(identity(cl), cl.ID, c.Name, topic, string(v))
}

// verbsOf is every verb a channel of this type defines, in the order RFC
// 0002's table writes them.
//
// **A closed list per type rather than a question asked of the ACL**, and
// the difference matters where this is used: the catalogue answers a client
// only about a channel it holds *some* verb on, so it has to ask about each
// verb the type has. A type whose verbs were guessed would answer about a
// channel the client cannot touch, or hide one it can.
func verbsOf(t channel.Type) []verb {
	switch t {
	case channel.Append:
		return []verb{verbWrite, verbRead, verbSeek}
	case channel.Latest:
		return []verb{verbWrite, verbRead, verbDelete}
	case channel.Queue:
		return []verb{verbWrite, verbConsume}
	}
	return nil
}

// permittedVerbs is the subset of a channel's verbs this client holds, in
// the order RFC 0002's table writes them.
//
// **It reads the same grants the publish and subscribe paths read**, asking
// the broader of the two questions those grants can answer: has this client
// any business with this channel, rather than may it do this to that topic.
// The narrower question cannot be asked here, because a catalogue answer is
// about a channel and not about a topic - and asking it with the channel's
// own filter in a topic's place would compare a `+` against a `+` and
// return whatever fell out.
//
// Everything it reports is still enforced where it decides something: a
// grant narrowed to part of the channel refuses the publish that leaves it.
//
// Every verb, when no acl_file is configured - which is every deployment
// that has not asked for one, and where the client may indeed do all of it.
func (b *Broker) permittedVerbs(cl *mqtt.Client, c *channel.Channel) []string {
	all := verbsOf(c.Type)
	r, ok := b.authorizer().(rules)
	if !ok {
		held := make([]string, 0, len(all))
		for _, v := range all {
			held = append(held, string(v))
		}
		return held
	}
	granted := r.r.VerbsOnChannel(identity(cl), cl.ID, c.Name)
	var held []string
	for _, v := range all {
		if granted[string(v)] {
			held = append(held, string(v))
		}
	}
	return held
}

// refuseUnauthorized answers a client that may not do this, with the code
// RFC 0002 gives it: 0x87, on the acknowledgement, which is the one answer
// that always arrives.
//
// **Nothing the client sent is echoed.** A refusal names the verb and the
// channel - both from the configuration - and not the topic, which is a
// client string bounded only by max_message_size. The log line is where an
// operator looks, and it carries the client id through Loggable for the
// same reason.
func (b *Broker) refuseUnauthorized(cl *mqtt.Client, v verb, c *channel.Channel) error {
	b.log.Warn("refused: the client's roles do not allow this",
		"client", b.limits.Loggable(cl.ID), "verb", string(v), "channel", c.Name)
	return packets.Code{
		Code:   packets.ErrNotAuthorized.Code,
		Reason: "the client's roles do not allow " + string(v) + " on " + c.Name,
	}
}

// verb is one of the operations RFC 0002 gives a channel type, and is a
// defined type with a closed set of constants rather than a string.
//
// **That is what makes it safe to log.** A channel name parsed out of a
// client's own topic is a client string until the registry has been asked,
// so this refusal takes the channel rather than its name - and the verb
// cannot be a client string at all, because the only values that exist are
// the constants below. Neither guarantee depends on where these are called from,
// which is the kind that does not rot.
type verb string

const (
	verbSeek    verb = "seek"
	verbConsume verb = "consume"
	verbRead    verb = "read"
	verbDelete  verb = "delete"
	verbWrite   verb = "write"

	// The one verb that names no channel: it is granted by a `broker:`
	// rule rather than by a channel's own verbs (RFC 0002 "Hanging up a
	// client").
	verbDisconnect verb = "disconnect"
)

// facility is one of the broker's own facilities an ACL rule may name, and
// it is a defined type with a closed set of constants for exactly the reason
// verb is one: what makes it safe to log is that no other value exists.
//
// A `string` parameter would be safe only as long as every caller passed a
// constant, which is a fact about today's call sites rather than about the
// value - and an exemption argued that way rots the moment somebody adds a
// caller.
type facility string

// The only one there is, spelled once in internal/channel so that the ACL's
// word for it and the broker's cannot drift.
const facilitySessions facility = channel.SessionsFacility

// permitsBrokerVerb answers a verb on a facility of the broker itself,
// where permitsVerb answers one on a channel.
//
// **A broker with no acl_file allows it**, which is the same answer
// permitsVerb gives and for the same reason: without an authorization file
// every authenticated client may already write to every channel and drain
// every queue, so there is no privilege boundary here for this verb to
// stand alone inside.
func (b *Broker) permitsBrokerVerb(cl *mqtt.Client, fac facility, v verb) bool {
	a := b.authorizer()
	r, ok := a.(rules)
	if !ok {
		return true
	}
	return r.r.AllowsBrokerVerb(identity(cl), cl.ID, string(fac), string(v))
}

// refuseBrokerVerb is refuseUnauthorized for a verb with no channel to
// name. Neither the verb nor the facility can be a client string, because
// neither type has a value that is not one of saguin's own constants.
func (b *Broker) refuseBrokerVerb(cl *mqtt.Client, v verb, fac facility) error {
	b.log.Warn("refused: the client's roles do not allow this",
		"client", b.limits.Loggable(cl.ID), "verb", string(v), "broker", string(fac))
	return packets.Code{
		Code:   packets.ErrNotAuthorized.Code,
		Reason: "the client's roles do not allow " + string(v) + " on " + string(fac),
	}
}
