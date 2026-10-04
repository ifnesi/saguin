package main

// The second rule about the whole tree, beside the doc-comment one and for
// the same reason: it is about every package rather than any one of them.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// Values that may appear unwrapped in a log line, each with the reason.
//
// **An entry is justified by what the value is, never by which call sites
// use it.** That distinction is the whole of what this list gets right and
// its first version got wrong. It carried `pk.TopicName` with the reason
// "every site using it runs after checkBounds" - a claim about the call
// sites, which stopped being true the moment a site was added, and restoring
// the original defect at the one site that runs *before*
// checkBounds with this check still reporting ok. A claim about the value
// cannot rot that way: a client id over the bound does not exist, because
// the connection carrying it was refused.
//
// Everything else is either a number, which cannot be unbounded, or goes
// through Loggable.
var loggableWithoutABound = map[string]string{
	// Refused at CONNECT, so no longer string exists to log (OnConnect).
	"cl.ID":    "a client id over max_client_id_length is refused at CONNECT",
	"clientID": "the same value, one frame down",
	// A key of Broker.partitions, which is a client id: the map is written
	// only by declare(), from a connected client's own id. Justified by what
	// the value is rather than by who reads it.
	"otherClientID": "the same value, read back from the partition index",
	// Built by partitioning() from saguin's own sentences and the property
	// names, with any client-supplied value passed through clipped() first -
	// 32 bytes, which a number could not exceed. Justified by how it is
	// made rather than by its callers.
	"badPartition": "a refusal from saguin's vocabulary, with any client value clipped",
	// A fixed sentence chosen from two, naming a prefix and a property.
	// Nothing a client sends reaches it.
	"partitionRefusal(f.Filter)": "one of two fixed refusals, from saguin's vocabulary",
	// One of two fixed sentences this broker writes about a Will it found
	// owed at a start - its delay passed, or its session ended, while the
	// broker was stopped. Chosen from a closed set in session.go; nothing a
	// client sends reaches it.
	"d.why":      "one of two fixed reasons, from saguin's vocabulary",
	"chosen.id":  "the same value, from the client table",
	"d.holder":   "the same value, stored as a delivery's holder",
	"pos.Reader": "a reader name, which is `mqtt:` and a client id",
	// A package constant in broker.go: one fixed sentence, written by
	// saguin, saying why No Local cannot be honoured on a queue. Justified
	// by what the value is - a compile-time string - rather than by the one
	// SUBACK that carries it.
	"noLocalOnAQueue": "a fixed sentence, from saguin's vocabulary",
	// A listener id from config's AnonymousBesideACL, which returns keys of
	// ListenerAuth: `tcp`, `ws` or `unix`, written by saguin. Nothing a
	// client or a file supplies reaches it.
	"door": "a listener id, one of three saguin writes",

	// The reader name a passing was recorded under, with the position-lost
	// record's own bound already applied - `bound` is that record's only
	// way of holding a name at all, applied before the key is built, so the
	// logged value and the stored one are the same bounded string. Named as
	// the call rather than as a variable so that this exempts one
	// expression and not every argument somebody later calls `name`.
	"bound(reader)": "a reader name, bounded by the record that logs it",
	// One of three constants of a named type declared in health.go -
	// passingKind, whose values saguin writes and a client cannot reach.
	// The conversion is what makes the expression unique: `kind` alone
	// would exempt every argument of that name in the tree.
	"string(kind)": "one of three passingKind constants, from saguin's vocabulary",

	// One of three constants, chosen by a number the broker set on itself.
	// AdmittedProtocols returns "MQTT 5", "MQTT 5, MQTT 3.1.1", or a
	// sentence naming a protocol level - a single byte, so three digits at
	// most. Nothing a client sends reaches it: the value read is the
	// server's own minimum-version capability.
	"broker.AdmittedProtocols(srv)": "one of three constants, from the server's own gate",

	// A name from the refusal vocabulary, or the code in hex. refusalReason
	// returns a value from `reasonNames` - saguin's own constants - one of
	// two fixed strings, or `fmt.Sprintf("0x%02X", …)`, which is four
	// characters. Nothing a client sends can reach any of those: the input
	// is a reason code, and what comes back is chosen from a closed set
	// rather than derived from it.
	"refusalReason(code)": "a name from the closed refusal vocabulary, or four hex characters",

	// A single byte from the CONNECT packet's protocol level field, so
	// three characters at most whatever a client puts there. Bounded by its
	// type rather than by anything checking it.
	"pk.ProtocolVersion": "a byte, and every byte is at most three characters",

	// MQTT's Will Delay Interval as the CONNECT carried it, logged beside the
	// wait the broker actually keeps so an operator can see where the two
	// differ. A uint32, so ten characters at most whatever a client puts
	// there - bounded by its type, like the protocol level above, rather than
	// by anything checking it.
	"will.WillDelayInterval": "a uint32, and every uint32 is at most ten characters",

	// The operator's own file, not a client's socket.
	"filter":    "a bridge's own configured filter, from the operator's file",
	"in.Filter": "the same",
	"in.Topic":  "a bridge rule's topic template, from the operator's file",
	// The same two values, on the outbound half, where the rule is reached
	// through the outRule that holds it.
	"o.rule.Filter": "the same filter, from the operator's file",
	"o.rule.Topic":  "the same template, from the operator's file",
	"cfg.Name":      "a bridge's name, from the operator's file",
	"cfg.Peer":      "a bridge's peer address, from the operator's file",
	// The same two values, reached through the Bridge that holds the
	// config. The key is the expression rather than the value, so one
	// value spelled two ways needs both spellings - which is the price of
	// asking about every expression rather than keeping a list of the bad
	// ones, and the right way round.
	"b.cfg.Name": "the same, from the bridge that holds it",
	"b.cfg.Peer": "the same",
	"c.Name":     "a channel name, from the operator's file",
	// A listener's configuration key - one of three constants main passes to
	// serverTLS - held beside the certificate SIGUSR1 re-reads.
	"l.at":   "a listener's configuration key, one of three constants in main",
	"n.l.at": "the same, from the certificate being put in force",
	// A certificate's expiry and whether a client CA is named: a time and a
	// bool, bounded by their types.
	"notAfter": "a time.Time, bounded by its type",
	// A bridge's wait before it says a record is unacknowledged: the
	// constant unackedAfter, a minute, which only tests shorten.
	"o.b.unackedAfter": "a time.Duration from a constant, bounded by its type",
	"clientCA":         "a bool",
	// A channel's configured topic filter, written in the same file beside
	// the name above, and refused at startup when it is longer than
	// limits.max_topic_length leaves room for. A client cannot reach it:
	// nothing writes this field but the loader, and a channel that writes
	// none is given `<name>/#`, built from the bounded name.
	"c.Filter": "a channel's filter, from the operator's file",
	// The socket a proxied connection arrived on: a proxyproto.Conn's
	// listener field is written only by newConn, from the socket path the
	// operator configured or the address of the listener bound to it. Nothing
	// a peer sends reaches it.
	"c.listener": "a socket path or listener address, from the operator's file",
	// What a PROXY header said about a client whose certificate the proxy
	// could not verify. SourceAddr is a net.Addr proxyproto builds from four
	// or sixteen address bytes and a uint16 port, so an IPv6 address and a
	// port at most, whatever the proxy sent; VerifyResult is a uint32, ten
	// digits at most. Bounded by how they are made, not by who logs them.
	"h.SourceAddr":   "an IP address and port parsed from fixed-width header bytes",
	"h.VerifyResult": "a uint32",
	// The ws listener's configured address, spelled through the WSListener
	// that embeds its Address block.
	"l.Address.Address": "a listener address, from the operator's file",
	// The same, walking a door of a kind that may now hold more than one -
	// RFC 0002 "TLS on a listener", "one listener of each kind".
	"d.Address.Address": "a listener address, from the operator's file",
	// A door's name: "tcp", "ws" or "unix" for the single-door form a
	// configuration writes by default, or whatever an operator named a
	// second door of a kind - from the operator's file like the address
	// beside it.
	"d.Name": "a door's name, from the operator's file",
	// A bool, so four or five characters whatever the file says. Bounded by
	// its type, which this check cannot see.
	"w.SameOriginOn()": "a bool",
	"d.SameOriginOn()": "a bool",
	// A defined type whose only values are the four constants declared
	// beside it, so no string a client sent can be one. The guarantee is
	// the type's, not the call sites' - a conversion from a client string
	// would be visible at the point somebody wrote it.
	"string(v)": "one of internal/broker's verb constants, which is a closed set",
	// The same guarantee, one type along: `facility` is a defined type whose
	// only value is a constant in that package. Nothing a client sends can
	// be one, whoever calls it.
	"string(fac)": "one of internal/broker's facility constants, which is a closed set",
	"name":        "the same",
	"chName":      "the same",
	"c.DLQ.Name":  "the same, derived from it",
	"d.channel":   "the same",
	"p.channel":   "the same",
	"u.channel":   "the same, a key of latestPending",
	// heldExchange.channel, which is written only with the name of a
	// channel the registry resolved or a start listed from it.
	"at.channel":       "the same, where an exactly-once publish is held",
	"h.channel":        "the same",
	"where[e].channel": "the same, copied from it",
	"b.name":           "the broker's own id, from the operator's file",
	"address":          "a listener address, from the operator's file",
	"af":               "the acl_file path, from the operator's file",
	// A listener id, which saguin chooses: "tcp", "ws", "unix". No client
	// can name one, because the id is set where the listener is created
	// and a connection is only ever told which one it arrived on.
	"l.id":            "a listener id, chosen by saguin when the listener is created",
	"cl.Net.Listener": "the same value, carried on the connection",
	// A key of MQTT.ListenerAuth, which builds its map from the same three
	// literals. The guarantee is the constructor's: there is nowhere for a
	// client string to enter it.
	"listenerID": "the same value, ranging over the configured listeners",
	// A key of Broker.listenerAuth, which only SetListenerCredentials and
	// SetListenerCertificates write and which main fills from
	// MQTT.ListenerAuth and MQTT.ListenerCertificates - the same three
	// literals. Justified by what the value is: there is nowhere for a
	// client string to enter that map.
	"untold": "a listener id, from the map main fills from the configuration",
	// A password file path out of the operator's own configuration, the
	// same kind of value as the acl_file above it.
	// A finding built by loadAuthorization from a path in the operator's
	// configuration and the error the file itself produced. Justified by
	// what it is made of: neither half can carry anything a client sent,
	// because no client names a file or reads one.
	"finding": "a path from the operator's file and that file's own error",
	// A credential file's path and one of its entries' names, the name
	// quoted with %q when the entry is built. Justified by what it is made
	// of: both are the operator's own file, which no client writes.
	"entry":           "a credential file's path and an entry's name from it, the name quoted",
	"d.Where":         "an operations password_file's configuration key, a saguin constant",
	"passwordFile":    "a listener's password_file, from the operator's file",
	"path":            "the same",
	"c.QueueFilter()": "`$saguin/queue/` and a channel's own name, both from the operator's file",
	// One of two compile-time strings, chosen on the connection's protocol
	// version. Justified by what the value is: there is no path by which a
	// client's own bytes reach either literal.
	"b.queueRefusal(cl)": "one of two fixed refusals from saguin's own vocabulary",

	// A uint16, so its printed form is at most five digits whatever a
	// client sends. The bound is the type's, which is the kind of
	// justification that cannot rot: nothing a caller does can widen it,
	// and a change of type would be visible where somebody made it.
	"pk.PacketID":        "a uint16 packet identifier, bounded at five digits by its type",
	"pr.Packet.PacketID": "the same value, on an inbound bridge publish",
	"e.PacketID":         "the same value, in the exchange it names",

	// An int64 byte count, returned by a method that can return nothing
	// else: it reads a provider's bound and there is no string on the path.
	// The guarantee is the return type's, which cannot rot the way a claim
	// about callers would.
	"b.providerBound(provider)": "a provider's bound, a byte count",

	// Named in the source rather than anywhere else. A constant cannot be
	// influenced by a client, an operator, or a later caller, which is the
	// only kind of exemption that cannot rot.
	"RetainedStoreName":        "a compile-time constant, the name of the retained store",
	"broker.RetainedStoreName": "the same constant, named from another package",
	// A `byte`, so it is one to three digits whatever it holds, and nothing
	// a client sends reaches it: the broker sets its own ceiling from the
	// configuration and reads it back here. The justification is the value's
	// own type rather than who happens to call it.
	"ceiling": "a byte, and every byte is at most three digits",

	// Provider names from the configuration file, and the two broker.qos2
	// limits: a bounded integer and a duration parsed out of it. A client
	// cannot reach any of them.
	"sessionProvider": "a provider name from the operator's configuration, the one broker.session names",
	"backlogProvider": "a provider name from the operator's configuration, the one broker.share names",
	"qos2MaxInflight": "an int from the operator's configuration",
	"qos2Expires":     "a duration from the operator's configuration",
	"after":           "the same duration, read back from the broker",

	// One of saguin's own refusal sentences. packets.Code values reaching
	// this line are built here, from constants; the substrate's own codes
	// carry its constants. Nothing a client sends is copied into one.
	"code.Reason": "a sentence from saguin's own refusal vocabulary",

	// An `int` out of the operator's own configuration, and refused at
	// startup below 1. Two things bound it and both are the value's own:
	// nothing a client sends reaches Limits, and an int's printed form is
	// at most twenty characters whatever it holds.
	"b.limits.PublishRate":  "the configured publish rate, an int from the operator's file",
	"b.limits.PublishBytes": "the configured publish byte rate, an int64 from the same file",
	// The same two numbers after resolution, which is either the pair above
	// or a pair out of the acl_file - the operator's other file. The bound
	// is the types': an int and an int64 print at most twenty characters
	// whatever they hold, and there is no string on the path for a client's
	// to reach. Neither half depends on who calls refuseRate.
	"rate":     "the publish rate this client was held to, an int",
	"byteRate": "the publish byte rate this client was held to, an int64",
	// How many filters of one SUBSCRIBE were over limits.max_subscriptions,
	// an int counted in capSubscriptions: at most the packet's filter count,
	// and an int prints at most twenty characters whatever it holds.
	"overBound": "how many filters of one SUBSCRIBE were refused at max_subscriptions, an int",

	// The operator's own file again, at startup. Nothing a client sends can
	// reach cmd/saguin: it runs before a listener accepts and after they
	// close, and it holds no packet.
	"cfg.Broker.ID": "the broker id, from the operator's file",
	"provider":      "a storage provider's name, from the operator's file",
	// What a full broadcast log gave up room for, joined: each is one of
	// saguin's own names for a store - the broadcast log, the session store,
	// the retained store - or "channel " and a
	// channel's name from the operator's file (bdrain.sayGaveUp).
	"roomFor":                          "names of stores, saguin's own or a channel's from the operator's file",
	"retainedProvider":                 "the same, the one the retained store names",
	"names":                            "channel names, from the operator's file",
	"bridge.Names(bridges)":            "bridge names, from the operator's file",
	"paths[name]":                      "a database path, from the operator's file",
	"db.Path()":                        "the same",
	"l.Address":                        "a listener address, from the operator's file",
	"where":                            "the operations listener's addresses, from the operator's file",
	"o.PasswordFile":                   "a path to the operators' password file, from the operator's file",
	"pf":                               "a path to the clients' password file, from the operator's file",
	"at":                               "a configuration key path such as broker.mqtt.listen.tcp - a literal from saguin's own source, never from a file",
	"t.ClientCAFile":                   "a path to a listener's client CA, from the operator's file",
	"authenticationState(o)":           "words this file writes, one per configured door, for what /metrics asks of a caller",
	"clientAuthState(cfg.Broker.MQTT)": "phrases this file writes, one per door where the doors differ, for how clients authenticate",
	"ln.Addr().String()":               "the address a listener saguin opened is bound to",
	"o.ScrapeInterval()":               "min_scrape_interval, a duration parsed from the operator's file",
	"h.Address":                        "the same",
	"bounds[provider]":                 "a provider's max_bytes, a byte count",
	// publish_commit_interval reaches the log as a duration's own String, or as the
	// two words written a few lines above it when a provider has none - so
	// the value is a duration the configuration parsed, which validation
	// refuses above a second, or a literal in this file. Neither can carry
	// anything a client sent: cmd/saguin runs before a listener accepts.
	"err.Error()":                        "the flusher's own fsync failure, wrapped with the provider's path: no client wrote any of it",
	"flushInterval.String()":             "flush_interval as a duration, validated to 10ms-1s by config",
	"commitEvery":                        "publish_commit_interval as a duration, or this file's own words for a provider that has none",
	"commitRecords":                      "publish_commit_max_records, a count bounded by config.MaxCommitRecords",
	"db.ReadConnections()":               "a provider's read_connections, an int validation holds to 0-8",
	"boundedProviders":                   "how many providers declared a bound, a count",
	"publish":                            "where publishes stop, a byte count",
	"full":                               "a provider's bound, a byte count",
	"mode":                               "a socket's permissions, an os.FileMode",
	"it.LeaseUntil.Format(time.RFC3339)": "a timestamp in a fixed layout",

	// saguin's own words, written in this repository.
	"why":           "a fixed reason from saguin's own vocabulary",
	"reason":        "the same",
	"reason.Reason": "the substrate's own reason text for a packet code",
	"action":        "one of saguin's two reserved verbs",
	"err":           "an error saguin or the substrate built",
	"w":             "a finding from saguin's own snapshot check",
	// The header of a sessions file: a fixed 24-byte field the format
	// truncates into, and a time. Bounded by the file format and by the
	// type, whatever wrote the file.
	"snap.Writer":       "a snapshot header's writer, a fixed 24-byte field",
	"snap.WrittenAt":    "a time.Time, bounded by its type",
	"deliveryID[:8]":    "eight bytes of a delivery id saguin issued",
	"c.ResponseTopic()": "built from a channel name",
	"codes[i]":          "an MQTT reason code",
	"code.Code":         "the same, the byte inside one",
	// And the one a peer answered an outbound publish with. A uint8 by its
	// type, so what is logged is at most three digits whatever the peer
	// sent - the bound is the type rather than anything about the peer.
	"ack.ReasonCode": "an MQTT reason code a peer answered with, a uint8",
	"channelName(c)": "a channel name from the operator's file, or the word broadcast",
	// A channel an outbound rule reads. It comes from the registry rather
	// than from anything on the wire, so it is a configured channel name -
	// bounded by channel.MaxNameLength, which validation enforces before a
	// listener opens.
	"src.Name": "a channel name from the operator's file, bounded at 128 bytes",
	// A channel an inbound rule names, which validation has already checked
	// exists - so it is one of the names the operator's own file writes.
	"in.Channel": "a channel name from an inbound rule, from the operator's file",
	"backoffName(c.Backoff)": "one of three literals this package writes, for a kind validation " +
		"has already reduced to three values",
	"c.BackoffBase": "a duration in whole seconds, parsed from the operator's file",
	"code":          "the same",
	// `Error` as a method name also catches http.Error, which writes to a
	// client rather than to a log. Judging the method and not the receiver
	// is what closes the escape where a logger is stored in a field nobody
	// named "log", and the price is the occasional entry like this one. A
	// false positive costs a line here; a false negative costs the class.
	// A log level is one of the four values slog defines, and a process id
	// is an integer the kernel assigned. Neither can carry a string a
	// client sent, whatever the call site.
	"level.Level()":          "a slog.Level, one of four values",
	"level.Level().String()": "the same, spelled out",
	"was.String()":           "the same, held before it changed",
	"now.String()":           "the same, about to be set",
	"os.Getpid()":            "an int from the kernel",
	// A listener's own address is the path or host:port the operator wrote
	// in the configuration file. Nothing a caller sends can reach it, on
	// any listener.
	"l.Listener.Addr().String()": "a listen address from the config file",

	"http.StatusBadRequest":          "an HTTP status constant",
	"http.StatusMethodNotAllowed":    "an HTTP status constant",
	"http.StatusUnauthorized":        "the same",
	"http.StatusForbidden":           "the same",
	"http.StatusNotFound":            "the same",
	"http.StatusInternalServerError": "the same",
	// The section vocabulary `/v1/operations/config` serves, joined from a
	// table declared in that file. Justified by what the value is rather
	// than by who passes it: every element is a string literal in this
	// repository, so no caller's string can become one.
	"configSectionNames": "the `?section=` names, from a table in health.go",
	// net/http sets RemoteAddr from the accepted socket, so it is an
	// address the kernel produced rather than anything the caller wrote -
	// unlike every header on the same request.
	"hostOf(r.RemoteAddr)": "the address a connection came from, as the socket reports it",
	// The substrate fills Net.Remote from the accepted connection, the same
	// way net/http fills RemoteAddr: an address the kernel produced, not
	// anything the client wrote in a packet.
	"hostOf(cl.Net.Remote)": "the address a connection came from, as the socket reports it",
	"version()":             "saguin's own build version",

	"listening(cfg.Broker.MQTT.Listen)": "the listeners, from the operator's file",

	// The MQTT engine, which this rule reached for the first time when the
	// engine moved from a module into internal/mqtt. Every entry below is
	// bounded by what the value is, the same as the rest of this list.
	//
	// **A packet is bounded by its type, and that is a stronger guarantee
	// than any of the others here.** `bound` in internal/broker switches on
	// packets.Packet and *packets.Packet and replaces either with
	// describePacket - the name, the id, the QoS, the payload's size and the
	// topic - and NewServer hands the engine a logger wrapped in that
	// handler. So the substitution happens for every packet logged anywhere,
	// by the engine or by saguin, whatever the key and whoever wrote the
	// call. There is no call site that can opt out of it, which is what
	// separates this from a claim about who calls what.
	//
	// TestAVanishingPublisherIsNotLoggedWithItsPayload is the behavioural
	// half, and it is the one that matters: it drives 200 publishes into a
	// connection it then resets, and asserts the line that comes out names a
	// payload size and carries none of the payload's bytes. This entry
	// stopping being true would fail that test, not only this one.
	//
	// Four spellings because the check asks about expressions, and these are
	// the four the engine writes.
	"pk":  "a packets.Packet, which the bounded handler renders as describePacket",
	"pkx": "the same, the packet a hook was handed",
	"pkv": "the same, a retained message being delivered",

	// A hook's own identifier, which is the string its ID() method returns.
	// Every hook in this binary is saguin's or the engine's, and each
	// returns a literal: nothing a client sends reaches one. The guarantee
	// is the method's, not any call site's.
	"hook.ID()": "a hook's id, a literal in this repository",

	// The listener's identity, chosen where the listener is created from the
	// operator's configuration. `l.Address()` and `l.Protocol()` are the
	// same: an address out of that file and one of four literals. A client
	// is only ever told which listener it arrived on.
	"l.ID()":                            "a listener id, chosen when the listener is created",
	"l.Protocol()":                      "one of four literals: tcp, ws, wss, unix",
	"l.Address()":                       "a listen address, from the operator's file",
	"slog.String(\"listener\", l.ID())": "the same id, in slog's attribute form",
	"listener":                          "the same id, passed to the function that logs it",

	// The engine's own version constant, written in its source.
	"Version": "a compile-time constant, the engine's version",

	// The address the kernel reports for an accepted connection, the same
	// value and the same guarantee as hostOf(cl.Net.Remote) above: it is
	// what the socket says, not what the client wrote in a packet.
	"cl.Net.Remote":       "the address a connection came from, as the socket reports it",
	"existing.Net.Remote": "the same, for the session being taken over",

	// A byte out of the fixed header, and a uint16 out of CONNECT. Both are
	// bounded by their types whatever a client sends, and minimumKeepalive
	// is a constant beside them.
	"pk.FixedHeader.Type":  "a byte, and every byte is at most three digits",
	"pk.Connect.Keepalive": "a uint16, bounded at five digits by its type",
	"minimumKeepalive":     "a compile-time constant",
}

// A refusal must not copy an unbounded client-chosen string into the log.
//
// Everything a client names is length-prefixed with two bytes on the wire,
// so the ceiling is 65,535 characters - and the places that log one are
// refusals, which is exactly where no bound has been applied to it. A
// publish topic is logged *because* it broke `max_topic_length`. A Will
// topic arrives in CONNECT and is never checked. A subscribe filter is not
// length-bounded at all. A bridge's topic belongs to a broker saguin does
// not administer. A seek or response payload is bounded only by
// `max_message_size`, a megabyte by default. At QoS 0 a client needs no
// acknowledgement, so it produces these as fast as the socket takes them.
//
// Measured before any of this was bounded: five refused publishes wrote
// 326kB, five refused subscribes 301kB, five dropped bridge records 607kB,
// and five connects with a long client id 302kB - none of them stored
// anywhere.
//
// **This is a check on the source rather than a test of behaviour, and it
// is written the other way up from its first version.** That one listed the
// four key names it knew about and looked for an unfamiliar value beside
// them, which is a list of things to catch: `"client", cl.ID` was invisible
// to it because `client` was not one of the four, and a call gofmt had
// wrapped across lines was invisible because it read one line at a time.
// This one asks the opposite question of every value in every log call -
// *is this provably bounded?* - so a key nobody has thought of yet fails
// until somebody decides about it, and line breaks do not exist, because
// what it walks is the syntax tree rather than the text.
func TestNoLogLineCarriesAnUnboundedClientString(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}

	var scanned, checked int
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++

		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not ours to report; the compiler already has
		}
		rel, _ := filepath.Rel(root, path)

		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			from, ok := logArgsFrom(call)
			if !ok {
				return true
			}
			// Every argument, rather than every second one. Counting in
			// pairs assumes the call is a message and then keys and values,
			// which `slog.String("topic", x)` is not and an argument list
			// somebody miscounts is not either - and in both cases the
			// parity slides and the check reads the keys while skipping the
			// values. Keys are string literals, so judging all of them
			// costs nothing and assumes nothing.
			for i := from; i < len(call.Args); i++ {
				checked++
				if bounded(call.Args[i]) {
					continue
				}
				t.Errorf("%s:%d: %s is logged with no bound on it.\n"+
					"\tWrap it in limits.Loggable, or add it to loggableWithoutABound "+
					"with the reason no unbounded value can reach it.",
					rel, fset.Position(call.Args[i].Pos()).Line,
					types.ExprString(call.Args[i]))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	// A check that silently examined nothing is worse than none. The first
	// version of this walked from "..", which is cmd/, so it read two files,
	// found nothing and reported success - a green tick over a rule it never
	// applied. Both numbers are asserted: the files reached, and the log
	// arguments actually examined inside them.
	if scanned < 20 {
		t.Fatalf("only %d source files scanned; the walk is not reaching the tree", scanned)
	}
	t.Logf("%d log arguments examined across %d source files", checked, scanned)
	if checked < 300 {
		t.Fatalf("only %d log arguments examined, which is too few to believe - "+
			"this check is no longer finding the log calls it thinks it is", checked)
	}
}

// **Nothing per record logs at INFO.** A line per record is a line per job,
// per acknowledgement, per deleted value: the fleet run's broker.log reached
// 40 MB in 30 minutes, 361,924 INFO lines of which all but a handful were a
// queue's "offering" and "acked" - about 1.9 GB a day on an edge box, for
// numbers saguin_queue_delivered_total and _acknowledged_total already
// count. mosquitto logs a PUBLISH only at its debug log type and EMQX only
// in a trace, so per-record lines are DEBUG here too; what an operator acts
// on - a dead-letter, a superseded acknowledgement, a refusal - is WARN.
//
// **How a line is recognised as per-record: by what it names.** An INFO
// call whose keys include `offset` or `topic` names one record - its place
// in a channel, or the topic it was published to - and fails unless it also
// names a `client`. A line naming the client is about that client's event
// (a Will, a seek), which happens once per session or per request rather
// than once per record, and stays at INFO. Keys are string literals, so they
// are read from the syntax tree wherever gofmt put them. Both halves are
// counted: the INFO calls examined, and the client-naming lines let through,
// so a walk that reads no keys fails rather than passing empty.
func TestNothingPerRecordLogsAtInfo(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	var scanned, infos, byClient int
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // not ours to report; the compiler already has
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Info" && sel.Sel.Name != "InfoContext") {
				return true
			}
			from, ok := logArgsFrom(call)
			if !ok {
				return true
			}
			infos++
			keys := map[string]bool{}
			for _, a := range call.Args[from:] {
				if lit, ok := a.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if k, err := strconv.Unquote(lit.Value); err == nil {
						keys[k] = true
					}
				}
			}
			if !keys["offset"] && !keys["topic"] {
				return true
			}
			if keys["client"] {
				byClient++
				return true
			}
			t.Errorf("%s:%d: %s names one record at INFO. A line per record is a line per "+
				"job or message; log it at DEBUG, with a counter carrying the number, or at "+
				"WARN if an operator acts on it", rel, fset.Position(call.Pos()).Line,
				types.ExprString(call.Args[from-1]))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
	t.Logf("%d INFO calls examined across %d source files; %d name a record and a client, and stay",
		infos, scanned, byClient)
	if scanned < 20 || infos < 50 {
		t.Fatalf("only %d INFO calls in %d files: the walk is not reaching the log calls it thinks "+
			"it is", infos, scanned)
	}
	if byClient == 0 {
		t.Fatal("no INFO call named a record and a client, though the Will lines do: the keys " +
			"are not being read, so nothing above was checked")
	}
}

// logArgsFrom says whether a call writes a log line, and from which
// argument its values start. The message and any context come first and are
// not values; everything after them is.
//
// **It judges the method and never the receiver.** The version before this
// also required the receiver's name to contain "log", with a comment
// claiming that was loose - it was the opposite, and a logger stored in a
// field called `out`, `l` or `journal` escaped the whole check. A rename is
// not a thing a bounds rule should turn on, and the method names are what
// make a call a log line.
//
// `With` and `WithGroup` are here because they carry key and value pairs
// like the rest and their values outlive the call: a logger built with
// `log.With("client", cl.ID)` writes that string into every line it is used
// for afterwards, and the calls that use it carry no sign of where it came
// from. One is live in internal/bridge.
//
// This is a list of method names, which is a list - but it is the one place
// the check cannot avoid being one, because "is this a log call" has no
// answer in the syntax otherwise. It is kept honest by naming every form
// slog offers rather than the four somebody remembered.
func logArgsFrom(call *ast.CallExpr) (int, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return 0, false
	}
	switch sel.Sel.Name {
	case "With", "WithGroup":
		return 0, true // pairs all the way down
	case "Debug", "Info", "Warn", "Error":
		return 1, true // after the message
	case "DebugContext", "InfoContext", "WarnContext", "ErrorContext":
		return 2, true // after the context and the message
	case "Log", "LogAttrs":
		return 3, true // after the context, the level and the message
	}
	return 0, false
}

// bounded says whether a value can be written into a log line as it stands.
//
// The four forms below cannot carry an unbounded string, whatever their
// type: a literal is written in this repository, `len` yields an int, a
// Loggable call is the bound itself, and a Sprintf is bounded exactly when
// everything it formats is. Anything else has to be named in
// loggableWithoutABound with the reason.
func bounded(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return true
	case *ast.ParenExpr:
		return bounded(v.X)
	case *ast.BinaryExpr:
		// Arithmetic on bounded values, and the concatenation of two
		// literals that gofmt has broken over two lines.
		return bounded(v.X) && bounded(v.Y)
	case *ast.CallExpr:
		switch fn := v.Fun.(type) {
		case *ast.Ident:
			if fn.Name == "len" {
				return true
			}
		case *ast.SelectorExpr:
			if fn.Sel.Name == "Loggable" {
				return true
			}
			// A Sprintf carries whatever it is given, so ask about each.
			if types.ExprString(fn) == "fmt.Sprintf" {
				for _, a := range v.Args[1:] {
					if !bounded(a) {
						return false
					}
				}
				return true
			}
			// slog's attribute form - slog.String("topic", x) - puts the
			// value inside a call rather than beside the key, so the value
			// is one node further in. Same question, one level down.
			if strings.HasPrefix(types.ExprString(fn), "slog.") {
				for _, a := range v.Args {
					if !bounded(a) {
						return false
					}
				}
				return true
			}
		}
	}
	if _, ok := loggableWithoutABound[types.ExprString(e)]; ok {
		return true
	}
	// A number cannot be an unbounded string. There is no type information
	// here, so this is the one judgement made by shape: an expression whose
	// name reads as a count, a size, an offset or a time is taken at its
	// word, and a string that arrives under such a name is a naming problem
	// before it is a bounds one.
	return numeric(types.ExprString(e))
}

// numeric recognises the counts, sizes, offsets, durations and flags that
// make up most of what a log line carries beside the string that named the
// thing. It judges the last word of the expression, so `out.Item.Offset` is
// judged by `Offset`.
//
// The two lists are separate because a suffix match is the dangerous kind.
// The first version of this had "n" among the suffixes, which makes every
// expression ending in that letter a number - `reason` and `token` included
// - and so would have waved through the very class this file is about. Only
// words long and specific enough to mean one thing are matched as suffixes;
// everything else has to be the whole word.
func numeric(expr string) bool {
	parts := strings.FieldsFunc(expr, func(r rune) bool {
		return r == '.' || r == '(' || r == ')' || r == '[' || r == ']'
	})
	if len(parts) == 0 {
		return false
	}
	last := strings.ToLower(parts[len(parts)-1])
	for _, w := range []string{
		"offset", "off", "attempt", "attempts", "count", "size", "bytes",
		"max", "min", "next", "floor", "first", "age", "removed", "freed",
		"qos", "timeout", "want", "position", "second", "seconds", "delay",
		"n",
		"interval", "deadline", "records", "subs", "clients", "retain",
	} {
		if last == w {
			return true
		}
	}
	for _, w := range []string{"size", "length", "count", "bytes", "offset",
		"period", "expiry", "present", "attempts", "timeout"} {
		if strings.HasSuffix(last, w) {
			return true
		}
	}
	return false
}
