package broker

import (
	"crypto/tls"
	"net"
	"strconv"
	"unicode/utf8"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/proxyproto"
)

// connectAuth answers who may connect, and deliberately provides no
// OnACLCheck.
//
// The server ORs the ACL hooks together - any hook returning true wins - so
// installing the stock permissive hook alongside saguin's would override
// every subscription refusal saguin makes and silently grant the
// non-canonical queue subscriptions that produce two workers on one job
// (invariant 4). Authorization is saguin's, because a correctness rule
// rides on it; this hook exists because mochi needs some hook to answer the
// authentication question, and the stock one answers both.
type connectAuth struct {
	mqtt.HookBase
	b *Broker
}

func (h *connectAuth) ID() string { return "connect-auth" }

func (h *connectAuth) Provides(b byte) bool {
	return b == mqtt.OnConnectAuthenticate
}

// OnConnectAuthenticate admits a client whose credentials are in the
// password file, and a client offering none when anonymous connections are
// allowed.
//
// A client that offers no user name is anonymous whatever else it sends,
// and `allow_anonymous` is the whole of the answer for it - including the
// mixed mode where a password file names some clients and the rest are let
// in anyway, which is what Mosquitto does with both configured. A user name
// with no file behind it is the same case: there is nothing to check it
// against, so the question is again whether anonymous connections are
// allowed.
//
// **Two questions, in this order.** Who is this, and may this device carry
// that name - the second asked only once the first has an answer, because
// `client_ids:` is written against a proved user name and would otherwise
// be matched against whatever the caller typed.
//
// **The refusal is the same `0x86` a bad password gets**, and deliberately.
// Answering "your credential is good, your client id is wrong" tells an
// unauthenticated caller that the credential is good, which is the thing
// the single code exists not to say - the same reason a wrong password and
// a missing one are one answer here. The log line below carries the
// distinction, which is where an operator looks.
func (h *connectAuth) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if h.authenticate(cl, pk) && h.mayCarryThisName(cl) {
		return h.sessionCapabilities(cl, pk)
	}

	// **A 3.1.1 client is answered here rather than by the substrate**, and
	// this is the one refusal that has to be. RFC 0002's CONNECT
	// table gives a credential `0x04` Bad user name or password and keeps it
	// apart from `0x05` Not authorized, where the Will refusals go: the two
	// are fixed by different people in different files, and one code for
	// both sends whoever is holding the device to the wrong one.
	//
	// **The substrate collapses them and does it before saguin can see the
	// packet.** Its own table maps `0x86` to `0x05`, and it applies that
	// inside `SendConnack` - so by the time the encode hook rewrites a
	// CONNACK on its way to a 3.1.1 client, a wrong credential and a refused
	// Will are the same byte and cannot be told apart. Every other refusal
	// is translated there, in one place; this one cannot be.
	//
	// So the packet is written here and the connection stopped. The
	// substrate then tries its own `SendConnack` on a closed socket, which
	// fails and ends the connection it was going to end anyway - the cost
	// is one extra line in the log, and the alternative is a device told
	// "not authorized" when its password is wrong.
	// **Recorded here, for every protocol.** This is the one place a
	// credential is refused: for MQTT 5 the substrate writes the CONNACK
	// itself after this returns, and tells no hook that it did.
	h.b.recordRefusal(cl, cl.ID, "bad user name or password")
	if legacyClient(cl) {
		const badCredential = 0x04
		_ = cl.WritePacket(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Connack},
			ReasonCode:  badCredential,
		})
		cl.Stop(packets.ErrBadUsernameOrPassword)
	}
	return false
}

// sessionCapabilities applies the acl_file's `broker: features` denials that a
// CONNECT can meet to a client that has authenticated, and reports whether it
// may connect. A denied client is answered as a broker without the feature
// would answer it.
//
// **Asked here, after the name is proved**, because a denial is written
// against a user name - asked of the name a CONNECT merely claims, the
// refusal would tell an unauthenticated caller which names exist and what
// their roles take away.
//
// **A denied Will refuses the connection, `0x87`**: the device asked the
// broker to announce its death, and accepting the connection while dropping
// that request is the silent half-grant this project refuses. Written here and
// the connection stopped, so the substrate's own `0x86` for a false return
// reaches a closed socket, the arrangement the 3.1.1 credential refusal above
// already uses.
//
// **A denied persistent session is accepted**, with a Session Expiry Interval
// of 0: MQTT gives a server that answer in the CONNACK (OnPacketEncode writes
// it), and a device loses nothing it was promised by being told. A 3.1.1
// client has no such property, so its session is made clean: it ends with
// the connection either way.
//
// **A retained Will aimed at a broadcast topic, from a client denied
// `retained`, is refused `0x9A`** - what MQTT has a broker without retained
// messages answer, and `0x05` to a 3.1.1 client by the same translation. One
// aimed at a channel is left alone: the channel is the store, so the flag
// keeps nothing extra there, and the denial is about broadcast only.
//
// **A QoS 2 Will from a client denied `qos2` is refused `0x9B`**, the code
// MQTT-3.2.2-12 gives a Will above the Maximum QoS the CONNACK would have
// said, and `0x05` to a 3.1.1 client. The substrate makes the same check,
// but against the broker-wide ceiling, which is 2.
//
// **A Will on a topic the client may not write is refused `0x87`**, and
// `0x05` to a 3.1.1 client: a Will is a publish the client asks the broker
// to make for it, so the rules its own PUBLISH meets are the Will's too
// (RFC 0002 "One pair everywhere"). Without this the Will went out as
// saguin, which the ACL does not ask, and a device whose role only reads a
// `latest` channel overwrote that channel's state by dropping its link.
// mosquitto refuses such a CONNECT the same way. publishWill asks again
// when the Will fires, because the acl_file can be reloaded in between.
func (h *connectAuth) sessionCapabilities(cl *mqtt.Client, pk packets.Packet) bool {
	if pk.Connect.WillFlag && h.b.deniesFeature(cl, featureWill) {
		h.b.log.Warn("refused a connection: this client's roles deny it a Will",
			"client", h.b.limits.Loggable(cl.ID),
			"user", h.b.limits.Loggable(identity(cl)), "listener", cl.Net.Listener)
		h.b.recordRefusal(cl, cl.ID, refusalReason(packets.ErrNotAuthorized))
		_ = h.b.srv.SendConnack(cl, packets.ErrNotAuthorized, false, nil)
		cl.Stop(packets.ErrNotAuthorized)
		return false
	}
	if pk.Connect.WillFlag && pk.Connect.WillRetain &&
		h.b.reg.Resolve(pk.Connect.WillTopic) == nil && h.b.deniesFeature(cl, featureRetained) {
		h.b.log.Warn("refused a connection: this client's roles deny it a retained Will "+
			"on a broadcast topic",
			"client", h.b.limits.Loggable(cl.ID),
			"user", h.b.limits.Loggable(identity(cl)), "listener", cl.Net.Listener)
		h.b.recordRefusal(cl, cl.ID, refusalReason(packets.ErrRetainNotSupported))
		_ = h.b.srv.SendConnack(cl, packets.ErrRetainNotSupported, false, nil)
		cl.Stop(packets.ErrRetainNotSupported)
		return false
	}
	if pk.Connect.WillFlag && pk.Connect.WillQos > 1 && h.b.qosCeilingFor(cl) < pk.Connect.WillQos {
		h.b.log.Warn("refused a connection: the Will is QoS 2 and this client's roles deny it QoS 2",
			"client", h.b.limits.Loggable(cl.ID),
			"user", h.b.limits.Loggable(identity(cl)), "listener", cl.Net.Listener)
		h.b.recordRefusal(cl, cl.ID, refusalReason(packets.ErrQosNotSupported))
		_ = h.b.srv.SendConnack(cl, packets.ErrQosNotSupported, false, nil)
		cl.Stop(packets.ErrQosNotSupported)
		return false
	}
	if pk.Connect.WillFlag && !h.b.permits(cl, pk.Connect.WillTopic, true) {
		h.b.log.Warn("refused a connection: this client's roles do not allow it to publish its Will's topic",
			"client", h.b.limits.Loggable(cl.ID), "user", h.b.limits.Loggable(identity(cl)),
			"topic", h.b.limits.Loggable(pk.Connect.WillTopic), "listener", cl.Net.Listener)
		h.b.recordRefusal(cl, cl.ID, refusalReason(packets.ErrNotAuthorized))
		_ = h.b.srv.SendConnack(cl, packets.ErrNotAuthorized, false, nil)
		cl.Stop(packets.ErrNotAuthorized)
		return false
	}
	if h.b.deniesFeature(cl, featurePersistent) {
		cl.Properties.Props.SessionExpiryInterval = 0
		cl.Properties.Props.SessionExpiryIntervalFlag = true
		cl.Properties.Clean = true
	}
	return true
}

// mayCarryThisName answers the acl_file's `client_ids:` for a client whose
// user name is now established, whichever of the three ways set it.
//
// True when no acl_file is configured, and true when the entry that applies
// writes no `client_ids:` - which is every deployment that has not asked
// for this.
func (h *connectAuth) mayCarryThisName(cl *mqtt.Client) bool {
	a := h.b.authorizer()
	if a == nil {
		return true
	}
	if a.AllowsClientID(identity(cl), cl.ID) {
		// **Said once, here, and not on every refusal it causes**: a rule
		// withheld for a name is withheld on every publish and subscribe the
		// client makes, and one line at connect names the cause where a
		// refusal per packet would only repeat it.
		if n := a.WithheldGrants(identity(cl), cl.ID); n > 0 {
			h.b.log.Warn("rules naming %c or %u grant this client nothing: its client id or user name "+
				"holds +, # or /, which the rule would read as a wildcard or a level",
				"client", h.b.limits.Loggable(cl.ID), "user", h.b.limits.Loggable(identity(cl)),
				"rules", n, "remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
		}
		return true
	}
	// **Both strings are bounded before they are logged.** The user name is
	// proved, but a certificate's name is an authority's string and
	// not this broker's, and the client id is the caller's own.
	h.b.log.Warn("refused a connection: this client id may not use this user name",
		"client", h.b.limits.Loggable(cl.ID),
		"user", h.b.limits.Loggable(identity(cl)),
		"remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
	return false
}

func (h *connectAuth) authenticate(cl *mqtt.Client, pk packets.Packet) bool {
	// **A verified client certificate is the whole answer, and the password
	// file is not consulted for that client.** The authority has already
	// made the statement the password file exists to make, and asking for
	// both would mean a device could not be issued a certificate without
	// also being given a password to keep somewhere.
	//
	// The certificate's name - its Common Name, or else its first DNS name -
	// becomes the client's user name, which
	// is Mosquitto's use_identity_as_username and is not optional here: a
	// certificate is a name, and letting the CONNECT carry a different one
	// beside it would give one client two identities and saguin two answers
	// to "who published this".
	//
	// Nothing here decides whether a certificate was *required* - the
	// handshake did, before this runs. RequireAndVerifyClientCert refuses a
	// client with none at the TLS layer, so a connection reaching this
	// function either presented a good certificate or was never asked for
	// one.
	// A proxy that terminated TLS carries the name it verified in the PROXY
	// header, and it is the same kind of claim as a certificate this broker
	// verified itself: an authority checked it, and saguin is trusting the
	// hop rather than the client. What makes that safe is the socket - the
	// header is readable only on a Unix listener, whose file permissions
	// decide who may assert anything at all.
	//
	// **Announced at Info, with everything the proxy said.** A deployment
	// where identity arrives over a hop is one where "who does the broker
	// think this is" has two possible answers and no way to see which - so
	// the name, the client's real address and the listener are on one line,
	// and the name goes through Loggable because it is a string a proxy
	// chose.
	if name := proxyproto.CommonNameOf(cl.Net.Conn); name != "" {
		if !ValidName(name) {
			h.b.log.Warn("refused a connection: the name a proxy gave it holds a NUL or a control "+
				"character, so it is not a name", "client", h.b.limits.Loggable(cl.ID),
				"principal", h.b.limits.Loggable(strconv.QuoteToASCII(name)),
				"remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
			return false
		}
		cl.Properties.Username = []byte(name)
		h.b.log.Info("a proxy named this client", "client", cl.ID,
			"principal", h.b.limits.Loggable(name),
			"remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
		return true
	}

	switch name, ok := verifiedName(cl.Net.Conn); {
	case ok && !ValidName(name):
		h.b.log.Warn("refused a connection: its certificate's name holds a NUL or a "+
			"control character, so it is not a name", "client", h.b.limits.Loggable(cl.ID),
			"principal", h.b.limits.Loggable(strconv.QuoteToASCII(name)), "remote", hostOf(cl.Net.Remote))
		return false
	case ok:
		cl.Properties.Username = []byte(name)
		return true
	case hasVerifiedCertificate(cl.Net.Conn):
		// Verified, and it names nobody. Falling through to the password
		// file is the safe half of the answer; saying so is the other,
		// because an operator who issued that certificate is entitled to
		// know why it did not authenticate anything.
		h.b.log.Warn("a verified client certificate carries no Common Name and no DNS name, "+
			"so it names nobody and authenticates nothing", "client", cl.ID, "remote", hostOf(cl.Net.Remote))
	}

	// **A user name is checked wherever one is sent**, before the listener
	// decides whether it needs one: on a door that admits anonymous
	// clients the name is not the client's identity (below), but it is
	// still what the CONNECT carried, which the refused list records. Not
	// logged, for the reason the password refusal below gives.
	if pk.Connect.UsernameFlag && !ValidName(string(pk.Connect.Username)) {
		h.b.log.Warn("refused a connection: its user name holds a control character, so it is "+
			"not a name", "client", cl.ID, "remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
		return false
	}
	// **The listener decides, and the broker's pair is what a listener that
	// said nothing gets.** A deployment with one answer writes it once and
	// nothing here changes; one with two - a Unix socket whose file
	// permissions already decide who may reach it, beside a TCP port facing
	// a fleet - gets the answer belonging to the door this connection came
	// through.
	users, anonymous := h.b.credentialsFor(cl.Net.Listener)
	if !pk.Connect.UsernameFlag || users == nil {
		if anonymous {
			// **A name nothing checked is not an identity.** The CONNECT's
			// user name is whatever the client typed, and with no password
			// file to hold it against it would be the name every acl_file
			// rule is asked about: a client typing `admin` was granted
			// admin's rules. Admitted anonymously, the client's name is the
			// empty one, which only a `*` pattern matches (RFC 0002
			// "Authorization"). mosquitto and EMQX keep the typed name;
			// Sagüin's own rule is that rules are about the name a client
			// authenticates under.
			cl.Properties.Username = nil
			return true
		}
		// The listener is named because with per-listener credentials
		// "anonymous connections are not allowed" is no longer a fact about
		// the broker, and an operator reading this needs to know which of
		// their doors refused.
		h.b.log.Warn("refused a connection: no user name, and anonymous connections are not "+
			"allowed on this listener",
			"client", cl.ID, "remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
		return false
	}
	if users.Verify(string(pk.Connect.Username), string(pk.Connect.Password)) {
		return true
	}
	// The username is not logged: it arrives in the CONNECT packet, so it is
	// whatever the caller chose to send, and a refusal is exactly where no
	// bound has been applied to it. The client id is bounded by
	// max_client_id_length, which is checked before this runs.
	h.b.log.Warn("refused a connection: bad user name or password",
		"client", cl.ID, "remote", hostOf(cl.Net.Remote), "listener", cl.Net.Listener)
	return false
}

// ValidName reports whether s can be somebody's name: valid UTF-8, with no
// U+0000 and none of the control characters U+0001-U+001F and
// U+007F-U+009F. Every source of an identity asks it - a certificate's
// Common Name or DNS name, a proxy's, a CONNECT user name, an operations
// principal - so a name that could rewrite a log line or pass for another
// in one is never an identity. mosquitto (mosquitto_validate_utf8) and EMQX
// (is_mqtt_safe_utf8) refuse the same set, and MQTT forbids U+0000 in a
// UTF-8 string [MQTT-1.5.4-2] and asks that the others not be sent
// (section 1.5.4, a SHOULD NOT that carries no identifier of its own:
// MQTT-1.5.4-3 is the U+FEFF rule).
func ValidName(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r >= 0x7f && r <= 0x9f {
			return false
		}
	}
	return true
}

// hasVerifiedCertificate is whether the TLS layer verified a client
// certificate at all, whatever it names.
func hasVerifiedCertificate(conn net.Conn) bool {
	state, ok := connectionState(conn)
	return ok && len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0
}

// connectionState is a connection's TLS state, from whichever door it came
// through: a *tls.Conn on the tcp door, and the WebSocket door's wrapper,
// which answers for the *tls.Conn it carries.
//
// **Asked of the connection rather than asserted on its type.** Both readers
// here asserted *tls.Conn, which the WebSocket door never hands over, so a
// certificate its handshake verified was never read: with a password file
// the client was refused 0x86, and without one it was named whatever it
// typed - while the configuration counted that door's client_ca_file as
// what authenticates the acl_file's clients.
func connectionState(conn net.Conn) (tls.ConnectionState, bool) {
	c, ok := conn.(interface{ ConnectionState() tls.ConnectionState })
	if !ok {
		return tls.ConnectionState{}, false
	}
	return c.ConnectionState(), true
}

// verifiedName is the name of a client certificate the TLS layer has
// already verified, and whether there was one.
//
// **Named as the operations door names it** (certificateName): the Common
// Name, else the first DNS name. This door took the Common Name alone, so a
// certificate carrying only a subject alternative name - which many
// authorities now issue - was an operator at one door and nobody at the
// other: one certificate, two identities.
//
// It reads VerifiedChains rather than PeerCertificates, and the reason is
// narrower than the one written here first. That version said a certificate
// which failed verification would be absent from VerifiedChains while
// present in PeerCertificates, on a listener that verifies only when a
// certificate is offered. It is not: `VerifyClientCertIfGiven` means the
// certificate is optional and, if sent, must be valid, so a bad one is
// refused during the handshake - measured, an authority this listener does
// not trust gets "x509: certificate signed by unknown authority" and an
// expired one "tls: expired certificate", in both modes. No such connection
// reaches this function.
//
// So this is defence against a change rather than against a client:
// PeerCertificates is what was *sent* and VerifiedChains is what was
// *checked*, and the day somebody adds a mode where the two differ - a
// hook that inspects certificates before deciding, RequestClientCert for
// logging - reading the checked one is what keeps a certificate from any
// authority from becoming an identity.
func verifiedName(conn net.Conn) (string, bool) {
	state, ok := connectionState(conn)
	if !ok {
		return "", false
	}
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return "", false
	}
	name := certificateName(state.VerifiedChains[0][0])
	if name == "" {
		return "", false
	}
	return name, true
}
