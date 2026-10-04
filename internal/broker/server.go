package broker

import (
	"context"
	"fmt"
	"log/slog"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
)

// maxQoSWithoutStore is the highest QoS a broker offers before SetQoS2 has
// offered exactly-once. The binary always offers it, so only an in-process
// harness runs at this ceiling. It is what NewServer starts from; SetQoS2
// raises the broker's own ceiling and the CONNACK together.
//
// **The channels' stores are what make the higher number honest.** MQTT
// hands the server ownership of an exactly-once message at the PUBREC, a
// full round trip before the client is told the exchange is done, so a
// broker advertising QoS 2 is one holding messages nobody has finished
// sending. Each is held in the store of the channel it is for, or the
// broadcast log's for a broadcast, inside its provider's bound
// (holdForRelease).
const maxQoSWithoutStore byte = 1

// maxQoSWithStore is what a broker offering exactly-once advertises. Both numbers
// are here rather than at their use sites so that the advertisement and the
// refusal cannot come to different answers.
const maxQoSWithStore byte = 2

// NewServer builds the MQTT server with saguin's hooks installed, in the
// one order that is correct.
//
// The server ORs its ACL hooks - any hook returning true wins - so a
// permissive hook installed alongside saguin's would override every
// subscription refusal saguin makes. connectAuth therefore answers the
// authentication question and provides no ACL hook at all.
func NewServer(reg *channel.Registry, lim config.Resolved, log *slog.Logger) (*mqtt.Server, *Broker, error) {
	// Start from the server's own defaults and override only what saguin
	// requires. Building Capabilities from scratch silently zeroes every
	// field left out - MaximumClients: 0 refuses every connection with
	// "server busy" and nothing says why.
	caps := mqtt.NewDefaultServerCapabilities()

	// **The version gate, and it is an operator's decision.** `3.1.1` - the
	// default - admits both protocols; `5` keeps the earlier promise that
	// only MQTT 5 gets in the door (RFC 0002 "Which versions may connect").
	// Zero is nothing said, which is the default.
	//
	// **What had to be built before this number could move**, written here
	// because this is the line somebody will reach for: saguin's publish
	// path answers a refusal with a reason code, and the substrate turns
	// that into a PUBACK-with-code inside a branch reading
	// `ProtocolVersion == 5`. Below that the code matched nothing and the
	// packet carried on down the same function - retained, delivered to
	// every broadcast subscriber, and answered PUBACK success. Refused and
	// published at the same time. `OnPublish` closes the connection instead,
	// `OnPacketRead` refuses QoS 2 whoever sent it (both RFC 0002 "Every
	// reason code, in one place"), and the subscribe path refuses a shared
	// subscription to a protocol that has none, and a queue subscription to
	// one that cannot acknowledge the work (RFC 0002 "What each channel type
	// admits"). Moving this number without those is a data-loss defect
	// rather than a partial feature.
	//
	// **And the CONNACK**, which is quieter. The substrate maps a few MQTT 5
	// CONNACK codes into 3.1.1's five-code space and leaves the rest alone,
	// so the three Will refusals - 0x90, 0x9A and 0x9B - reached a 3.1.1
	// client as raw MQTT 5 bytes, which 3.1.1 does not define as return
	// codes at all, and a bad credential read "not authorized" rather than
	// "bad user name or password". legacyConnack now makes RFC 0002's
	// mapping.
	caps.MinimumProtocolVersion = lim.MinProtocolVersion
	if caps.MinimumProtocolVersion == 0 {
		caps.MinimumProtocolVersion = 4
	}
	// The ceiling a broker starts from, raised by SetQoS2 where the
	// operator configured somewhere to hold a half-finished exchange. MQTT
	// lets a server grant lower and never higher, so below it a QoS 2
	// subscription request is served at QoS 1.
	//
	// **A publish above it is a different question and has a different
	// answer**: the specification says refuse and disconnect rather than
	// serve lower, and OnPacketRead is where saguin does it. Both read
	// b.maxQoS, so raising it cannot leave the refusal enforcing a number
	// the broker no longer advertises.
	caps.MaximumQos = maxQoSWithoutStore

	// Acknowledgements carry nothing the client sent, which used to be
	// set here as a capability. It is the engine's unconditional
	// behaviour now, in buildAck, with [MQTT-3.1.2-29] and the reasoning
	// beside it; the note is there rather than repeated here because
	// that is where somebody would change it.
	//
	// It supersedes half of an earlier fix, which stripped saguin's own
	// `saguin-` prefix from acknowledgements at OnPacketEncode. That
	// strip still stands for the packets this does not reach.

	// The two bounds the server enforces on saguin's behalf, and it does
	// so better than a hook could: MaximumPacketSize is checked against the
	// declared remaining length before the body is read, which is what RFC
	// 0002 asks for.
	//
	// It is not what puts the number in the CONNACK. SendConnack writes
	// Receive Maximum, Maximum QoS, the assigned client id, a server
	// keepalive and a shortened session expiry, and nothing else, whatever
	// this field holds - so the advertisement is saguin's own, in
	// OnPacketEncode, and this field is only the enforcement.
	caps.MaximumPacketSize = lim.MaxMessageSize
	caps.MaximumClients = lim.MaxConnections

	// Topic Alias, capped (RFC 0001). It is the MQTT 5 feature that pays
	// for itself on a constrained link - a device publishing to a 900-byte
	// topic sends it once and then two bytes - which is the deployment
	// saguin is for.
	//
	// What it costs is a table per connection, and the substrate's default
	// of 65,535 makes that table something no configuration bounds: 65,535
	// aliases over 900-byte topics measured 83MB on one connection against
	// 5MB for the same traffic without, and `max_connections` defaults to
	// 10,000. Invariant 13 asks for a bound, and the cap is one by
	// arithmetic rather than by hope - at sixteen the worst case is
	// sixteen times `max_topic_length` per connection, which an operator
	// can work out from two numbers they already set.
	//
	// Sixteen rather than a configuration key because the settings either
	// side of it are not decisions anybody can make: what it gives up is a
	// client with more than sixteen long topics, which is not the case the
	// feature exists for. The substrate refuses a larger alias with 0x94
	// on its own, so the cap is enforced where the packet is parsed.
	caps.TopicAliasMaximum = topicAliasMaximum

	// What this field actually gates is one thing: a CONNECT whose *Will*
	// carries the retain flag, which the server refuses with 0x9A before any
	// hook runs. It does not gate a retained publish - measured, at a
	// socket: with this at 0 a retained publish to a latest channel was
	// answered 0x00 and one to a broadcast topic was disconnected by
	// saguin's own rule, which is where that decision belongs (RFC 0003
	// "Retained messages").
	//
	// At 1 the decision is saguin's instead, and it is made in OnConnect
	// against the topic the Will names: refused where a retained publish to
	// that same topic would be refused, which is a broadcast topic on a
	// broker with no retained store, and admitted everywhere else. One rule
	// for the flag rather than two, so a device cannot be turned away at
	// CONNECT for promising to send a message it would be allowed to
	// publish. A retained Will into a `latest` channel is the standard
	// availability pattern - a retained `online`, and a retained Will
	// publishing `offline` - and refusing it here would have been refusing
	// the connection of every device that uses it.
	//
	// What the client is *told* is a different number and is not this one:
	// OnPacketEncode writes Retain Available into the CONNACK, 1 where the
	// flag can be honoured somewhere and 0 where it can be honoured nowhere.
	caps.RetainAvailable = 1

	// No message expiry that nobody asked for. The substrate's default is
	// 24 hours, and it applies it two ways: it stamps Message Expiry
	// Interval on a delivery the publisher set none on, and it force-removes
	// anything still in flight after that long.
	//
	// The stamp was the visible half. A broadcast subscriber and a queue
	// worker were handed `message-expiry-interval: 86400` on records nobody
	// gave an expiry to, while an append or latest consumer got nothing -
	// because saguin writes those two deliveries itself. Two consumers of
	// one broker reading different metadata off the same kind of record is
	// worse than either answer on its own. At 0 a client's own expiry is
	// honoured exactly as sent instead of being capped at a day, which is
	// what RFC 0001 says the property is for.
	//
	// The removal is the half worth being careful about, and it is not a
	// safeguard saguin gives up. saguin's OnQosDropped only counts an
	// expiry, so a sweep taking an in-flight record away returns nothing to
	// its queue: the record stays in Delivering with no deadline and no
	// worker, which is the
	// stranded job OnSelectSubscribers goes to some length to avoid. Every
	// deadline saguin does rely on - the visibility timeout, job expiry -
	// is its own and is far shorter.
	caps.MaximumMessageExpiryInterval = 0

	// How long a client may ask its session - and so its durable consumer's
	// stored position - to outlive its connection. The substrate's default
	// is 0xFFFFFFFF, about 136 years, which is not a bound: a fleet taking a
	// fresh client id every boot leaves one immortal row per boot per
	// channel and nothing an operator sets reaches it (invariant 13).
	//
	// MQTT supplies the whole mechanism. The server shortens what the client
	// asked for, and SendConnack writes the shortened value into the CONNACK
	// - so a conforming client is told what it got rather than discovering
	// it later. That is why this is a capability rather than a refusal at
	// CONNECT: a client asking for longer is served, at the cap.
	caps.MaximumSessionExpiryInterval = lim.MaxSessionExpiry

	srv := mqtt.New(&mqtt.Options{
		InlineClient: true,
		Capabilities: caps,
		// Wrapped, because the substrate logs whole packets and one of those
		// lines is an ordinary disconnect. boundedHandler says why.
		Logger: slog.New(boundedHandler{log.Handler()}),

		// **The one bound that has to live in the substrate**, because the
		// substrate is what writes a queue's offers and a broadcast. Its
		// WritePacket takes the client's lock and writes to the connection
		// inside it, so a client that stops reading holds that lock for as
		// long as it stays connected, and every write to that client waits
		// behind it. Unbounded, a deaf worker froze the whole
		// queue-and-retention loop while a deaf broadcast subscriber hung
		// its publishers.
		//
		// A publisher is no longer held there: an identifier is taken
		// without that lock and the delivery is handed to the client's write
		// loop, so the queue that sheds a slow subscriber is reached at every
		// QoS. What the deadline still ends is the client itself.
		//
		// Zero is the substrate's default and leaves writes unbounded, so
		// `limits.write_timeout: none` restores exactly the old behaviour.
		ClientNetWriteTimeout: lim.WriteTimeout,
		ClientConnectTimeout:  lim.ConnectTimeout,
		ClientMaxConnectSize:  lim.MaxConnectSize,
		// What one session may hold of unacknowledged deliveries (RFC 0002
		// `limits.session_queue_bytes`). The substrate enforces it because the
		// substrate is what queues a broadcast or shared delivery for a session.
		ClientSessionQueueBytes: lim.SessionQueueBytes,
	})

	b := New(reg, log)
	b.limits = lim
	b.SetServer(srv)
	if err := srv.AddHook(b, nil); err != nil {
		return nil, nil, err
	}
	if err := srv.AddHook(&connectAuth{b: b}, nil); err != nil {
		return nil, nil, err
	}
	return srv, b, nil
}

// boundedHandler is the ceiling on what reaches an operator's log through
// the logger saguin hands the substrate.
//
// **The substrate logs whole packets, and one of those lines is a client
// going away.** `server.go`'s "error processing packet" takes any error
// that is not a `packets.Code`, and a broken pipe from writing a PUBACK to
// a publisher whose socket has closed is one - the most ordinary event a
// broker has. What it writes is the packet struct: 2,409 bytes for a
// 36-byte payload, with `Payload:[80 65 84 …]` in it byte for byte and the
// whole `Connect` struct beside it, `Password` field included. That is the
// application's data, and on a CONNECT the client's credential, in a file
// that is rotated, shipped to an aggregator and backed up.
//
// **Eight sites and two keys**, which is why this is a handler and not a
// patch: `pk` at one, `packet` at seven, across the engine. Fixing the line
// the defect was found at would have left the rest, and the next release
// can add another.
//
// **The ceiling is the floor, and the type-aware part sits on top.** Asking
// "is this one of the shapes I know are bad" is how the sixth site gets
// missed; asking "is this provably small" cannot be. So every attribute
// over maxLoggedValue is replaced whatever it is, and `packets.Packet` and
// `[]byte` are then given summaries so that the useful fields survive
// rather than the whole value becoming its own size.
//
// **What a change upstream costs**, since that is the question this design
// is answering: renaming a key or adding a site changes nothing here,
// because nothing matches on keys. Moving or renaming `packets.Packet`
// stops the build, which is the loud failure and the right one. A future
// line carrying something large in a shape nobody has seen is caught by
// the ceiling. What would still pass is something *small* and secret under
// a new key - and what bounds that is that the fork is pinned and bumped
// deliberately.
type boundedHandler struct{ slog.Handler }

// maxLoggedValue is how much of one attribute reaches the log. Generous
// enough that no ordinary field is touched - a topic is bounded at
// max_topic_length, 1024 by default - and far below the 2.4KB a single
// packet dump costs.
const maxLoggedValue = 1024

func (h boundedHandler) Handle(ctx context.Context, r slog.Record) error {
	// A record is rebuilt rather than edited: slog.Record's attributes are
	// only reachable through Attrs, and a copy shares the backing array
	// with the original, so writing through one would reach the other.
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(bound(a))
		return true
	})
	return h.Handler.Handle(ctx, out)
}

func (h boundedHandler) WithAttrs(as []slog.Attr) slog.Handler {
	for i := range as {
		as[i] = bound(as[i])
	}
	return boundedHandler{h.Handler.WithAttrs(as)}
}

func (h boundedHandler) WithGroup(name string) slog.Handler {
	return boundedHandler{h.Handler.WithGroup(name)}
}

// bound is one attribute, small enough to log.
func bound(a slog.Attr) slog.Attr {
	switch v := a.Value.Resolve().Any().(type) {
	case packets.Packet:
		return slog.String(a.Key, describePacket(v))
	case *packets.Packet:
		if v == nil {
			return a
		}
		return slog.String(a.Key, describePacket(*v))
	case []byte:
		// Whatever its length: a password is short, and there is no size at
		// which somebody else's bytes belong in a log line.
		return slog.String(a.Key, fmt.Sprintf("%d bytes", len(v)))
	}

	// Everything else, by size. A string is measured as it stands; anything
	// else is rendered once and measured, which costs a formatting call on
	// a line that was going to be formatted anyway.
	s, ok := a.Value.Any().(string)
	if !ok {
		if a.Value.Kind() != slog.KindString {
			rendered := fmt.Sprint(a.Value.Any())
			if len(rendered) <= maxLoggedValue {
				return a
			}
			return slog.String(a.Key, fmt.Sprintf("%T, %d bytes, not logged",
				a.Value.Any(), len(rendered)))
		}
		s = a.Value.String()
	}
	if len(s) <= maxLoggedValue {
		return a
	}
	return slog.String(a.Key, s[:maxLoggedValue]+
		fmt.Sprintf("…(truncated from %d bytes)", len(s)))
}

// describePacket is what a packet is worth in a log line: which packet, on
// which topic, how big. Not a truncation of the struct dump - the head of
// that is zero-valued Connect fields and none of it is the answer to why a
// write failed.
func describePacket(pk packets.Packet) string {
	name := packets.PacketNames[pk.FixedHeader.Type]
	if name == "" {
		name = fmt.Sprintf("type %d", pk.FixedHeader.Type)
	}
	s := fmt.Sprintf("%s id=%d qos=%d payload=%d bytes",
		name, pk.PacketID, pk.FixedHeader.Qos, len(pk.Payload))
	if pk.TopicName != "" {
		s += " topic=" + pk.TopicName
	}
	return s
}

// AdmittedProtocols names the MQTT versions this broker will admit, read
// from the server's own gate rather than from the configuration. It
// is exported because the line that says a broker is up is written in
// main, and the type it reads belongs to the substrate.
//
// **Read back rather than restated**, because the two can disagree and only
// one of them is what the door does. An operator upgrading into a release
// that admits a version the last one turned away has this line as the
// place they find out, and a line derived from the file would say what was
// asked for rather than what happened.
func AdmittedProtocols(srv *mqtt.Server) string {
	if srv == nil || srv.Options == nil || srv.Options.Capabilities == nil {
		return "unknown"
	}
	switch v := srv.Options.Capabilities.MinimumProtocolVersion; {
	case v >= 5:
		return "MQTT 5"
	case v == 4:
		return "MQTT 5, MQTT 3.1.1"
	default:
		return fmt.Sprintf("MQTT 5 and every version down to protocol level %d", v)
	}
}
