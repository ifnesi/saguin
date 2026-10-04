package broker

import (
	"strconv"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// Connection admission: what a CONNECT has to be before saguin will have
// the client, and how a connection saguin will not have is refused.
//
// # Validate, authenticate, then commit state
//
// That is the order, and it is written here because it was not written
// anywhere: the moments live in four hooks the substrate calls at different
// times, and side effects were once
// committed before the credentials were checked - a refused CONNECT could
// cancel another client's delayed Will and overwrite a live client's Will
// properties, because the hook that did it ran first and nothing said it
// should not.
//
//   - **Validate** - `malformedConnect` and `OnConnect`, here. Is this a
//     CONNECT at all, and will this broker have it: the packet the
//     specification calls malformed, an Authentication Method saguin does
//     not implement, a client id longer than `max_client_id_length`, a Will
//     that could never be delivered. **Nothing here commits anything**: a
//     refusal at this moment must leave the broker exactly as it found it,
//     including the state of some other connection that happens to share a
//     client id.
//   - **Authenticate** - `OnConnectAuthenticate` and the rest of the
//     `connectAuth` hook, in `auth.go`. The password file, the ACL, and the
//     features the acl_file grants this identity. Everything before this
//     moment asked about the packet; this one asks who is sending it.
//   - **Commit state** - `OnSessionEstablish` and `OnSessionEstablished`,
//     the moment the broker takes this connection's word for anything: the
//     session record, an id taken over from an earlier connection, the
//     Will's properties, the subscriptions a resumed session brings back.
//     After authentication because a connection that fails it must be able
//     to leave nothing behind.
//
// The names are the hooks rather than their files on purpose: the hooks are
// what the substrate calls and what the order is about, and a file this
// sentence pointed at could move underneath it.
//
// # Refusing
//
// A refusal is a CONNACK and a closed connection, and the CONNACK is the
// last thing the client hears - so it carries the reason, and it is
// counted. `recordRefusal` is where a refusal becomes a number
// (`saguin_connections_refused_total`, by reason), whatever refused it:
// this file, the authentication hook, or the substrate itself.
//
// **A 3.1.1 client is told in codes it defines**, which is what
// `legacyConnack` is for. It is applied in `OnPacketEncode`, where every
// CONNACK is finally shaped, because a wrong credential is refused by the
// substrate before any saguin hook is asked: a translation at saguin's own
// send sites would have left the refusal a fleet meets most often speaking
// MQTT 5 reason codes at a client that has never heard of them.

// malformedConnect reports why a CONNECT is not a well-formed one, and the
// code to answer with, or "" when it is.
//
// Each rule is one MQTT states and a third-party suite found saguin not
// enforcing. Four more the suite found were here as well and are the
// engine's own now - Will Retain with no Will [MQTT-3.1.2-13], a user name
// that is not well-formed UTF-8 [MQTT-1.5.4-1], a Will QoS with no Will
// [MQTT-3.1.2-11] and, on 3.1.1, a password with no user name
// [MQTT-3.1.2-22]: ConnectValidate and the decoder's decodeString refuse
// them first, with the same codes, so the copies here could not be reached.
func malformedConnect(pk packets.Packet) (string, packets.Code) {
	c := pk.Connect

	// A zero-length Client Identifier is for a client asking the server to
	// name it, which only makes sense with Clean Start 1: with 0 the client
	// is asking to resume a session under a name it never had. 3.1.1 makes
	// it a refusal ([MQTT-3.1.3-8], identifier rejected), and MQTT 5 lets a
	// server refuse a Client Identifier 0x85 ([MQTT-3.1.3-8]); MQTT-3.1.3-3,
	// cited here before, is the rule that the field is present at all.
	if c.ClientIdentifier == "" && !c.Clean {
		return "a zero-length client id needs Clean Start 1: with 0 there is no " +
			"session to resume (MQTT-3.1.3-8)", packets.ErrClientIdentifierNotValid
	}
	return "", packets.Code{}
}

// OnConnect refuses a client id longer than `max_client_id_length`, which
// is the one bound that has to be applied before anything else runs.
//
// A client id is not merely logged. A durable consumer's stored position is
// keyed by it - `store.MQTTReader(cl.ID)` - so it is written into SQLite and
// into the snapshot files, and it stays there for the session expiry
// interval and across restarts. Unbounded, that is durable state an
// anonymous client chooses the size of, which is invariant 13 exactly.
// Measured before this existed: twenty connections with 60,000-byte ids,
// eight seconds, 1,260,124 bytes of reader names and a 2.6MB database, with
// every CONNECT answered `0x00`.
//
// Bounding it here rather than truncating it at each log line is what makes
// the rest safe by construction: the same string is logged wherever a line
// names a client, and a bound at the door holds all of them, the stored
// rows, and the session tables at once. A truncation would have held only
// the log.
//
// The CONNACK has to be sent by hand. Returning an error from this hook
// stops the connection but writes nothing - `establishClient` returns straight
// out - so a client would get a closed socket and no reason. `SendConnack`
// is exported and b.srv is right here, which is the same technique
// refuseRetain uses for a DISCONNECT the hook contract does not offer.
//
// It reads the packet's own field rather than cl.ID, because a client that
// sends no id at all is assigned one by the substrate: an xid, 20 bytes,
// and refusing saguin's own generated name would be absurd.
func (b *Broker) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	id := pk.Connect.ClientIdentifier

	// **The packet before the policy.** Everything below this asks whether
	// saguin will have this client; these ask whether the bytes are a
	// CONNECT at all, and a packet the specification calls malformed is one
	// no client can have meant. Taking it means the connection runs on a
	// reading of those bytes the client does not share - a Will QoS that
	// governs a Will that is not there, a password nobody will look up.
	//
	// The substrate validates a good deal here already; each of these is
	// one it does not, found by driving `sammiq/mqtt_test` and
	// `vibesrc/mqttconformance` against saguin with mosquitto 2.0.22 as the
	// control and reading the differences.
	if why, code := malformedConnect(pk); why != "" {
		b.log.Warn("refusing a connection: the CONNECT is malformed",
			"client", b.limits.Loggable(id), "reason", why)
		// Best effort, as above: a 3.1.1 client has no reason code for most
		// of these and gets the connection closed, which the returned error
		// does either way.
		_ = b.srv.SendConnack(cl, code, false, nil)
		b.recordRefusal(cl, id, refusalReason(code))
		return code
	}

	// [MQTT-4.12.0-1] a server that does not support the Authentication
	// Method a client named "MAY send a CONNACK with a Reason Code of 0x8C
	// (Bad authentication method) or 0x87 (Not Authorized) ... and MUST
	// close the Network Connection".
	//
	// **saguin implements no enhanced authentication at all**, so every
	// method named is one it does not support and this is the whole of the
	// rule for this broker. Enhanced authentication is the challenge and
	// response exchange MQTT 5 added - the client names a method in its
	// CONNECT and the two sides trade AUTH packets before the CONNACK -
	// and saguin authenticates from a password file and an ACL instead.
	//
	// Measured before this existed: a CONNECT naming `SCRAM-SHA-1` was
	// answered CONNACK `0x00`, and the client, which by [MQTT-3.1.2-30]
	// may send nothing but AUTH or DISCONNECT until it has its CONNACK,
	// then waited on an AUTH that was never coming. **The connection was
	// up and useless.** Refusing it is what lets the client fall back to
	// an ordinary CONNECT, which is what a client library does with a
	// 0x8C and cannot do with a success.
	//
	// 3.1.1 carries no properties, so this reads empty there and the rule
	// does not arise.
	if m := pk.Properties.AuthenticationMethod; m != "" {
		b.log.Warn("refusing a connection: it asks for an authentication method "+
			"saguin does not support",
			"client", b.limits.Loggable(id), "method", b.limits.Loggable(m))
		_ = b.srv.SendConnack(cl, packets.ErrBadAuthenticationMethod, false, nil)
		b.recordRefusal(cl, id, refusalReason(packets.ErrBadAuthenticationMethod))
		return packets.ErrBadAuthenticationMethod
	}

	if len(id) > b.limits.MaxClientIDLength {
		b.log.Warn("refusing a connection: the client id is longer than the configured bound",
			"client", b.limits.Loggable(id), "bytes", len(id), "max", b.limits.MaxClientIDLength)
		// Best effort: if the CONNACK cannot be written the connection is
		// going anyway, and the error below is what stops it either way.
		_ = b.srv.SendConnack(cl, packets.ErrClientIdentifierTooLong, false, nil)
		b.recordRefusal(cl, id, refusalReason(packets.ErrClientIdentifierNotValid))
		return packets.ErrClientIdentifierTooLong
	}

	// **A client id is a name too** - it is substituted into rules by %c,
	// logged on every line about its client, and kept in the session store
	// - so it takes the rule every other name does (ValidName): no control
	// character. mosquitto refuses one 0x85 and EMQX by default since 6.3;
	// the engine has already refused U+0000 and invalid UTF-8, which MQTT
	// makes a malformed packet [MQTT-1.5.4-1] [MQTT-1.5.4-2].
	if !ValidName(id) {
		b.log.Warn("refusing a connection: the client id holds a control character",
			"client", b.limits.Loggable(strconv.QuoteToASCII(id)))
		_ = b.srv.SendConnack(cl, packets.ErrClientIdentifierNotValid, false, nil)
		b.recordRefusal(cl, id, refusalReason(packets.ErrClientIdentifierNotValid))
		return packets.ErrClientIdentifierNotValid
	}

	// **A `saguin-` property saguin does not read is an instruction it
	// cannot carry out**, so the connection is refused rather than taken
	// and quietly not honoured (RFC 0003 "The reserved prefix on a client's
	// own packets"). saguin defines none on CONNECT today, which is exactly
	// why the rule goes in now: nothing can break, and a release that
	// shipped the silence would make closing it a breaking change.
	if key := unreadReserved(pk, connectProps); key != "" {
		b.log.Warn("refusing a connection: it carries a reserved property saguin "+
			"does not read", "client", b.limits.Loggable(id),
			"property", b.limits.Loggable(key))
		_ = b.srv.SendConnack(cl, packets.ErrImplementationSpecificError, false, nil)
		b.recordRefusal(cl, id, refusalReason(packets.ErrImplementationSpecificError))
		return packets.ErrImplementationSpecificError
	}

	// A Will that could never be delivered is refused here, where the
	// device is still there to be told (RFC 0003 "Last Will").
	if pk.Connect.WillFlag {
		// Judged here, where the device is still there to be told. The
		// Will's properties are kept in OnSessionEstablish, once the
		// connection has been authenticated: a CONNECT that is refused
		// changes nothing.
		if code, why := b.willRefusal(pk); why != "" {
			// Named locals rather than the fields themselves: the source
			// check that keeps client strings out of the log judges the
			// last word of an expression, and `pk.Connect.WillRetain` ends
			// in a word it does not know. `retain` says what it is.
			retain := pk.Connect.WillRetain
			b.log.Warn("refusing a connection: its Will could never be delivered",
				"client", b.limits.Loggable(id),
				"topic", b.limits.Loggable(pk.Connect.WillTopic),
				"retain", retain, "reason", why, "code", code.Code)
			_ = b.srv.SendConnack(cl, code, false, nil)
			b.recordRefusal(cl, id, refusalReason(code))
			return code
		}
	}

	// **The keepalive ceiling, applied here because this is where MQTT puts
	// it.** The server may answer with a keepalive of its own and the client
	// must then use that one (MQTT-3.1.2-21); the substrate sends whatever
	// this leaves on the session, and its own comment says to set it from
	// this hook.
	//
	// Zero is the case worth having a ceiling for rather than an edge of
	// it: MQTT defines a keepalive of zero as "never disconnect me for
	// being idle", so a device that asks for it holds a session the broker
	// has no way to tell is dead until the session expiry runs out - a
	// clock that is longer by design, and the one that decides when a
	// durable consumer's stored position is released. A ceiling makes the
	// broker's own answer the answer.
	//
	// Nothing is refused. A client asking for more than the ceiling is
	// given the ceiling and told, which is what MQTT provides for; refusing
	// the connection would cost a device its link over a number the
	// protocol has a way to correct.
	// **And it is enforced only where it can be communicated** (RFC 0002
	// `limits.max_keepalive`). Server Keep Alive is an MQTT 5 CONNACK
	// property with no 3.1.1 equivalent, so a 3.1.1 client cannot be told
	// the ceiling - and shortening its clock anyway is a disconnection every
	// time its own timer is the longer one, which is a flap loop written
	// into the configuration file rather than a limit. Measured before this
	// test existed: a 3.1.1 client asking for sixty seconds against a
	// one-second ceiling was hung up on inside two. It keeps what it asked
	// for; RFC 0002 says so beside the key.
	if max := b.limits.MaxKeepalive; max > 0 && !legacyClient(cl) {
		if asked := pk.Connect.Keepalive; asked == 0 || asked > max {
			cl.State.Keepalive = max
			cl.State.ServerKeepalive = true
		}
	}

	// **Nothing past this point acts on the CONNECT.** The substrate asks
	// the password file after this hook returns, so everything that changes
	// state on the broker's behalf - a Will's properties, a delayed Will
	// cancelled, the counters of connections taken - is in
	// OnSessionEstablish, which only an authenticated connection reaches. A
	// refused CONNECT used to cancel another client's delayed Will and
	// replace a live client's Will properties, by naming its client id with
	// no valid credential.
	return nil
}

// connectRefusalReason names a refused CONNECT for the operator's
// instruments, where one code means something at the door that it does not
// mean anywhere else.
//
// **`0x80` answers one publish on this broker**: an MQTT 5 exactly-once
// PUBLISH from a connection that no longer holds its client id, answered
// in its PUBREC (codeNotTheSession; RFC 0002 "Publishing"). A failed write
// to the broadcast log answered it too, and answers `0x83` now, the code
// RFC 0002 gives every storage failure on a publish. **In a CONNACK it has one producer**: the
// substrate's `validateConnect`, for a zero-byte client id with
// `cleanSession = 0`. legacyConnack has the whole argument and the test that
// notices if it stops being true; this is the same fact, told to the
// operator instead of to the device.
//
// Reported unchanged, the one row whose device cannot be named - the client
// id is empty, that being the refusal - also carried the one reason that
// names nothing, so `/v1/operations/refused` answered "which devices" with a
// blank and "why" with a shrug. The name is fetched from the same table
// rather than written out, so this row and the too-long-id row above cannot
// drift into two spellings of one condition.
func connectRefusalReason(code packets.Code) string {
	if code.Code == packets.ErrUnspecifiedError.Code {
		return refusalReason(packets.ErrClientIdentifierNotValid)
	}
	return refusalReason(code)
}

// OnConnectRefused records a CONNECT the substrate turned away before any
// saguin hook could see it.
//
// **The refusals this covers are the ones saguin cannot make itself**: a
// protocol version below `broker.mqtt.min_protocol_version`, the connection
// limit, and the checks the substrate makes on a Will before saguin's own
// run. Until the fork carried a hook for it, the only trace of any of them
// was a line in saguin's listener wrapper with no client id - so an
// operator who had just closed the door on an older fleet could not learn
// which devices were still knocking, which is the question
// `/v1/operations/refused` exists to answer.
//
// **The instruments cover every connection this broker refused, whatever
// the protocol and however well the device was told.** A 3.1.1 client
// refused for its version is told, correctly, in its own vocabulary, and an
// MQTT 5 client refused as busy is told `0x89`; both are recorded, because
// the operator's question is which devices, and it does not change with how
// well the device was answered.
func (b *Broker) OnConnectRefused(cl *mqtt.Client, pk packets.Packet, code packets.Code) {
	// The id comes from the packet rather than the client: the substrate
	// has parsed it by now, but taking it from the CONNECT is what keeps
	// this right if a refusal is ever added before it adopts one.
	//
	// **The packet need not be a CONNECT**, and then it carries no CONNECT
	// fields at all (packets.ConnectParams): a connection refused for
	// opening with some other packet [MQTT-3.1.0-1], or cut off before its
	// CONNECT was decoded, is refused with no id and no user name.
	var id, user string
	if c := pk.Connect; c != nil {
		id, user = c.ClientIdentifier, string(c.Username)
	}
	why := connectRefusalReason(code)
	b.log.Warn("refused a connection before it was established",
		"client", b.limits.Loggable(id), "reason", why,
		"protocol", pk.ProtocolVersion, "listener", cl.Net.Listener)
	b.counted.connectionRefused(why)
	b.refused.record(id, user, why, b.limits.Loggable)
}

// OnConnectionRefused records a connection the substrate ended for a refusal
// of its own: a packet it refused while decoding it, one over the Maximum
// Packet Size, or one processPacket answered with a reason code - an alias
// never registered, a Receive Maximum exceeded. The substrate has told the
// client already, and nothing else would count it: saguin's own refusals
// end the connection through disconnect, which the substrate then does not
// end or tell again.
func (b *Broker) OnConnectionRefused(cl *mqtt.Client, code packets.Code) {
	b.recordRefusal(cl, cl.ID, refusalReason(code))
}

// OnSocketRefused counts a socket a listener closed before it was
// a connection: a ws socket that got no max_connections slot. It
// is counted by the same reason a tcp CONNECT refused for it is, and it
// makes no row on the refused route, because there is no client id to name
// (RFC 0002: "closed with nothing written"). Logged at debug, because a
// fleet at the limit knocks faster than a log should be written.
func (b *Broker) OnSocketRefused(listener string, code packets.Code) {
	why := connectRefusalReason(code)
	b.counted.connectionRefused(why)
	b.log.Debug("closed a socket at accept", "listener", listener, "reason", why)
}

// recordRefusal counts a connection this broker refused, at CONNECT or by
// ending it, whatever protocol the client speaks.
//
// **A CONNECT refusal has to reach the same record as a publish refusal**
// (RFC 0005 `/v1/operations/refused`), and the case that decides it is the
// one most likely to happen: a fleet of devices that connect with a
// retained Will, against a broker with no retained store, is refused before
// it publishes anything at all. A record fed only from the publish path
// would show nothing for the flap an operator is most likely to meet.
//
// **Every protocol, because the question is which devices.** This used to
// return early for MQTT 5, on the reasoning that a client told why needs no
// record - while the engine's own refusals, which reach OnConnectRefused,
// were recorded for MQTT 5 regardless. One broker then answered "which
// devices were turned away" differently by protocol: measured, an MQTT 5
// client refused as busy was listed and one refused for its password was
// not.
//
// The client id is taken from the CONNECT packet rather than from the
// client, because the substrate has not adopted it yet at the point the
// long-id refusal fires. The record bounds it on the way in, and the user
// name with it: both are strings a stranger chose, and the record keeps
// them after the connection is gone.
func (b *Broker) recordRefusal(cl *mqtt.Client, id, reason string) {
	b.counted.connectionRefused(reason)
	b.refused.record(id, identity(cl), reason, b.limits.Loggable)
}

// legacyConnack rewrites a refusal on its way to a 3.1.1 client into a
// return code that protocol defines (RFC 0002 "Every reason code, in one
// place").
//
// **3.1.1 has five return codes and saguin has more reasons than five**, so
// the mapping is saguin's to make. The substrate has one of its own and it
// is both incomplete and, in one row, not what this broker wants: it covers
// a handful of codes and leaves the rest alone, so `0x90`, `0x9A` and
// `0x9B` - every Will refusal - reached a 3.1.1 client as raw MQTT 5 bytes,
// which 3.1.1 does not define as return codes at all. Measured at a socket
// before this existed: a Tasmota-shaped CONNECT with a retained Will was
// refused `0x9a`, and Eclipse Paho's 3.1.1 client reported *no error* - the
// device cannot read its own refusal, so it reads nothing.
//
// **The translation is expressed as a substitution rather than a value**,
// which is worth explaining because it looks wrong. `SendConnack` maps a
// code through its own table for a pre-5 client, and only for codes at or
// above `0x80` - handing it a 3.1.1 code directly would fall below that
// line and be sent as *success*. So each row names the MQTT 5 code whose
// existing mapping lands on the 3.1.1 one this broker wants. The comment on
// each is what it means, since the constant no longer says it.
//
// `0x85` has a row although the substrate maps its own spelling of it to
// `0x02`: that table is keyed by the whole Code, reason text and all, so
// saguin's too-long client id (ErrClientIdentifierTooLong) is not in it and
// would reach the clamp. `0x89` has one too, which the substrate answers
// `0x03` to a pre-5 client before this is reached; it is kept rather than
// trusted.
func legacyConnack(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	// **A version nobody knows is answered before asking which version the
	// client speaks**, because that question has no answer for it: a client
	// declaring protocol level 99 is refused precisely because nothing here
	// knows what it is, so legacyClient - which means "speaks 3.1.1" - is
	// false and the mapping below would never run.
	//
	// **Scoped to a level this broker does not admit**, which is anything
	// but 4 and 5. A client declaring 5 and refused for its version is an
	// MQTT 5 client and keeps the MQTT 5 code; one declaring 4 is a 3.1.1
	// client and goes through the table below. Written wider than this, it
	// rewrote an MQTT 5 client's `0x82` to `0x01` - which
	// TestEveryCONNACKCodeBecomesOneA311ClientDefines caught.
	//
	// **Levels 1 to 3 as well as 0 and above 5.** "MQTT" at level 3 is
	// refused by the substrate as a name that does not match its level, and
	// fell to the table's clamp as `0x05` Not authorized, where MQTT 3.1.1
	// [MQTT-3.1.2-2] asks `0x01` for a level the server does not support. Any `0x82` below 4 is a client on a
	// protocol saguin does not admit, whatever else was wrong with it, so
	// `0x01` is the true answer for all of them.
	//
	// `0x01` is what MQTT 3.1.1 section 3.1.2.2 names for an unsupported
	// protocol level, and the CONNACK is already being written in that
	// protocol's shape. What went before was `0x82`, an MQTT 5 code 3.1.1
	// does not define, so a client refused for its version read a byte with
	// no meaning - the same failure the Will refusals had before this
	// function existed. Mosquitto 2.0.22 answers `0x01`, measured.
	if v := cl.Properties.ProtocolVersion; (v < 4 || v > 5) &&
		pk.ReasonCode == packets.ErrProtocolViolationProtocolVersion.Code {
		pk.ReasonCode = 0x01 // unacceptable protocol level
		return pk
	}
	if !legacyClient(cl) {
		return pk
	}
	// The five 3.1.1 CONNACK return codes, and which of saguin's refusals
	// each one answers. RFC 0002's CONNECT table is this map in prose.
	const (
		identifierRejected = 0x02
		badCredential      = 0x04
		notAuthorized      = 0x05
	)
	switch pk.ReasonCode {
	case packets.ErrClientIdentifierNotValid.Code:
		// `0x85` for both spellings the substrate has of it.
		pk.ReasonCode = identifierRejected
	case packets.ErrUnspecifiedError.Code:
		// **A zero-byte client id with `cleanSession = 0`**, which 3.1.1
		// names itself: MQTT-3.1.3-8 says `0x02`, identifier rejected. The
		// substrate answers it `0x80`, a code with no row of its own, and
		// the clamp below would make it `0x05` - sending whoever holds the
		// device to check credentials when the fix is an id or the clean
		// flag, the wrong-person failure `0x04` and `0x05` are kept apart
		// to avoid.
		//
		// **`0x80` in a CONNACK has one producer**, which is what makes this
		// precise rather than a guess: `validateConnect` returns it for that
		// condition and nothing else, and every other CONNACK code saguin or
		// the substrate sends is named. The test below is what notices if
		// that stops being true.
		pk.ReasonCode = identifierRejected
	case packets.ErrBadUsernameOrPassword.Code:
		// **Kept apart from the three below on purpose.** A credential that
		// does not work and a Will a connection may not register are fixed
		// by different people in different files, and one code for both
		// sends whoever is holding the device to the wrong one. The
		// substrate's own table collapses exactly this distinction, which
		// is one of the two reasons the translation is saguin's.
		pk.ReasonCode = badCredential
	case packets.ErrTopicNameInvalid.Code,
		packets.ErrRetainNotSupported.Code,
		packets.ErrQosNotSupported.Code:
		// The three Will refusals, each of them "this connection may not do
		// what it is asking to do".
		pk.ReasonCode = notAuthorized
	case packets.ErrServerBusy.Code:
		pk.ReasonCode = 0x03 // server unavailable
	case packets.ErrImplementationSpecificError.Code:
		// **A session store that could not read or write what the connection
		// needs** (codeSessionUnreadable, codeWillNotWritten): the other
		// `0x83` at a CONNECT is a `saguin-` User Property, which 3.1.1 has
		// nothing to carry. Server unavailable is the true answer, and the
		// one a device retries on - `0x05` would send it to its credentials.
		pk.ReasonCode = 0x03
	case packets.ErrUnsupportedProtocolVersion.Code:
		pk.ReasonCode = 0x01
	default:
		// **Anything unmapped becomes `0x05` rather than going out raw**,
		// because a byte 3.1.1 does not define is a refusal the device
		// cannot read - measured, Eclipse Paho's 3.1.1 client reported an
		// unmapped `0x9A` as no error at all. The nearest of the five says
		// less than the truth and is still true.
		if pk.ReasonCode > notAuthorized {
			pk.ReasonCode = notAuthorized
		}
	}
	return pk
}
