package broker_test

// What saguin refuses at the door, driven with packets no client library
// will send.
//
// **Every case here is a rule MQTT states and a third-party suite found us
// not enforcing.** Two of them, `sammiq/mqtt_test` and
// `vibesrc/mqttconformance`, were run against saguin and against mosquitto
// 2.0.22 as a control, and these are the differences. The oracle for each is
// the specification statement named above it, never what the code does.
//
// The packets are built byte by byte because that is the only way to send
// them: a CONNECT whose Will flag is 0 while its Will QoS is 1 is one no
// library will construct, which is exactly why a broker can go years without
// finding out what it does with one.
//
// **Four rules from those suites are not here, because no hook can reach
// them.** Each is decided while the packet is being decoded, before saguin
// is asked anything, and the decoder is the MQTT engine's:
//
//   - [MQTT-1.5.5-1] a non-minimal Variable Byte Integer, in a CONNECT's or
//     a PUBLISH's property length or in a Subscription Identifier.
//   - [MQTT-3.1.2-1] a protocol name that is not "MQTT" - the engine
//     answers a CONNACK where the specification says close the connection.
//   - [MQTT-3.1.2-2] an unsupported protocol level, which must be return
//     code 0x01 to a 3.1.1 client and reaches it as an MQTT 5 byte.
//   - [MQTT-3.8.3-5] reserved bits in the Subscription Options, which the
//     decoder drops before building the Subscription a hook is handed, so
//     there is nothing left here to refuse.

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// rawConn is a socket and nothing else. It writes the bytes it is given and
// reads whatever comes back.
type rawConn struct {
	t *testing.T
	c net.Conn
}

func rawDialTo(t *testing.T, addr string) *rawConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &rawConn{t: t, c: c}
}

func (r *rawConn) write(b []byte) {
	r.t.Helper()
	if _, err := r.c.Write(b); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

// reply is what one read produced, and it has three states rather than two
// on purpose.
//
// **Silence is not a refusal**, and a helper that cannot tell it from one
// hands back a pass for a broker that did nothing at all. This returned two
// states until an AUTH test written against it passed on the unfixed tree:
// the broker swallowed the packet, the read hit its deadline, and "no
// packet arrived" was read as "the connection was closed". The test took
// three seconds and reported success. Which of the two happened is the
// whole question for any rule whose answer may legitimately be a close.
type reply struct {
	kind   byte
	body   []byte
	got    bool // a packet arrived
	closed bool // the broker closed the connection; false means it never spoke
}

// answer reads one packet, or reports how the read ended instead.
func (r *rawConn) answer(d time.Duration) reply {
	r.t.Helper()
	_ = r.c.SetReadDeadline(time.Now().Add(d))
	head := make([]byte, 2)
	if _, err := io.ReadFull(r.c, head); err != nil {
		return reply{closed: !timedOut(err)}
	}
	rest := make([]byte, int(head[1]))
	if _, err := io.ReadFull(r.c, rest); err != nil {
		return reply{kind: head[0] >> 4, closed: !timedOut(err)}
	}
	return reply{kind: head[0] >> 4, body: rest, got: true}
}

// timedOut reports whether a read ended at its deadline rather than because
// the other end went away.
func timedOut(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// str writes an MQTT UTF-8 string: two length bytes then the content. It
// takes bytes rather than a string so a test can send a sequence that is not
// valid UTF-8, which is the whole point of one of the cases below.
func str(b []byte) []byte {
	out := make([]byte, 2, 2+len(b))
	binary.BigEndian.PutUint16(out, uint16(len(b)))
	return append(out, b...)
}

// connectPacket assembles a CONNECT from parts a library would not let a
// caller get wrong.
func connectPacket(version byte, flags byte, clientID []byte, extra ...[]byte) []byte {
	var v []byte
	v = append(v, str([]byte("MQTT"))...)
	v = append(v, version)
	v = append(v, flags)
	v = append(v, 0x00, 0x3c) // keepalive
	if version == 5 {
		v = append(v, 0x00) // no properties
	}
	v = append(v, str(clientID)...)
	for _, e := range extra {
		v = append(v, e...)
	}
	return append([]byte{0x10, byte(len(v))}, v...)
}

// refused reports whether the broker refused: either a CONNACK carrying a
// non-zero reason, or the connection closed with nothing said. A 3.1.1
// client gets the second where MQTT 5 gets the first, and a rule about the
// packet holds for both.
//
// A broker that says nothing and stays connected has not refused: the
// client is left holding a connection it cannot use, which is the outcome
// worth failing over rather than the one to count as a pass.
func refused(t *testing.T, r *rawConn) (bool, string) {
	t.Helper()
	a := r.answer(3 * time.Second)
	switch {
	case !a.got && a.closed:
		return true, "the connection was closed"
	case !a.got:
		return false, "the broker neither answered nor closed"
	case a.kind != 2:
		return true, "answered with packet type " + string(rune('0'+a.kind))
	case len(a.body) >= 2 && a.body[1] != 0x00:
		return true, "CONNACK reason 0x" + hex(a.body[1])
	}
	return false, "CONNACK success"
}

func hex(b byte) string {
	const d = "0123456789ABCDEF"
	return string([]byte{d[b>>4], d[b&0x0F]})
}

// The CONNECT flag rules. Each is a byte a client cannot set by accident and
// a broker cannot notice without looking.
func TestAMalformedConnectIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name, clause string
		version      byte
		flags        byte
		clientID     []byte
		extra        [][]byte
	}{
		// [MQTT-3.1.2-11] "If the Will Flag is set to 0, then the Will QoS
		// MUST be set to 0." Bit 2 is the Will flag; bits 3 and 4 are the
		// Will QoS. Here the QoS says 1 and the flag says there is no Will.
		{"a Will QoS with no Will", "MQTT-3.1.2-11", 5, 0x02 | 0x08, []byte("v-willqos"), nil},

		// The same rule for a 3.1.1 client, which is refused by having its
		// connection closed rather than by a reason code.
		{"a Will QoS with no Will, on 3.1.1", "MQTT-3.1.2-11", 4, 0x02 | 0x08, []byte("v-willqos311"), nil},

		// [MQTT-3.1.2-22] on 3.1.1: "If the User Name Flag is set to 0, the
		// Password Flag MUST be set to 0." MQTT 5 permits a password with
		// no user name, so this case is the older protocol's alone.
		{"a password with no user name, on 3.1.1", "MQTT-3.1.2-22", 4,
			0x02 | 0x40, []byte("v-pwdonly"), [][]byte{str([]byte("secret"))}},

		// [MQTT-1.5.4-1] every UTF-8 encoded string "MUST be a well-formed
		// UTF-8 string". 0xC3 begins a two-byte sequence and 0x28 cannot
		// continue one, so this user name is ill-formed. The engine's
		// decoder refuses it, before any hook is asked.
		{"a user name that is not UTF-8", "MQTT-1.5.4-1", 5,
			0x02 | 0x80, []byte("v-badutf8"), [][]byte{str([]byte{0xC3, 0x28})}},

		// [MQTT-3.1.3-8] a zero-length Client Identifier is allowed only
		// with Clean Start 1: the server assigns one, and there would be
		// nothing to resume a session under otherwise. Bit 1 is Clean
		// Start, and it is 0 here.
		{"no client id and no clean start", "MQTT-3.1.3-8", 5, 0x00, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t)
			r := rawDialTo(t, h.Addr)
			r.write(connectPacket(tc.version, tc.flags, tc.clientID, tc.extra...))
			no, how := refused(t, r)
			if !no {
				t.Errorf("%s: the broker accepted it (%s). A packet the specification "+
					"calls malformed is one a client cannot have meant, and taking it "+
					"means the connection runs on a reading of the bytes the client "+
					"does not share", tc.clause, how)
			}
		})
	}
}

// connectNamingAuthMethod assembles an MQTT 5 CONNECT whose only property is
// an Authentication Method. It is a separate builder rather than a parameter
// on connectPacket because it is the one case here where the packet is
// well-formed: what saguin refuses is what it asks for, not how it is
// written.
func connectNamingAuthMethod(clientID, method string) []byte {
	props := append([]byte{0x15}, str([]byte(method))...) // 0x15 = Authentication Method

	var v []byte
	v = append(v, str([]byte("MQTT"))...)
	v = append(v, 5, 0x02) // version, Clean Start
	v = append(v, 0x00, 0x3c)
	v = append(v, byte(len(props)))
	v = append(v, props...)
	v = append(v, str([]byte(clientID))...)
	return append([]byte{0x10, byte(len(v))}, v...)
}

// [MQTT-4.12.0-1] a server that does not support the Authentication Method
// a client supplied "MAY send a CONNACK with a Reason Code of 0x8C (Bad
// authentication method) or 0x87 (Not Authorized) ... and MUST close the
// Network Connection".
//
// saguin implements no enhanced authentication, so every method named is
// one it does not support. What made this worth finding is the state the
// client is left in rather than the clause: [MQTT-3.1.2-30] forbids it to
// send anything but AUTH or DISCONNECT until its CONNACK arrives, so a
// success answered to a request saguin will never continue leaves a
// connection that is up, authenticated in the client's view, and unable to
// carry a single publish.
//
// mosquitto 2.0.22, run as the control, closes.
func TestAConnectNamingAnAuthenticationMethodIsRefused(t *testing.T) {
	h := start(t)
	r := rawDialTo(t, h.Addr)
	r.write(connectNamingAuthMethod("v-authmethod", "SCRAM-SHA-1"))

	a := r.answer(3 * time.Second)
	switch {
	case !a.got && a.closed:
		return // closed, which the clause asks for on its own
	case !a.got:
		t.Fatalf("the broker neither answered nor closed. The client may send " +
			"nothing but AUTH until it is authenticated, so it is now waiting on " +
			"an exchange saguin will never begin")
	case a.kind != 2:
		t.Fatalf("answered with packet type %d, want a CONNACK or a close", a.kind)
	case len(a.body) >= 2 && a.body[1] == 0x00:
		t.Fatalf("the broker accepted a CONNECT naming an authentication method it "+
			"cannot run (CONNACK 0x00). The client may send nothing but AUTH until "+
			"it is authenticated, so it now waits for an AUTH that is never coming "+
			"on a connection it believes is up (body % 02x)", a.body)
	case len(a.body) >= 2 && a.body[1] != 0x8C && a.body[1] != 0x87:
		t.Errorf("refused with 0x%s; MQTT-4.12.0-1 names 0x8C and 0x87, and a client "+
			"library falls back to an ordinary CONNECT on those", hex(a.body[1]))
	}
}

// The other door to the same defect. Re-authentication (§4.12.1) is open
// only to a client that supplied an Authentication Method in its CONNECT,
// and saguin refuses every CONNECT that does - so no connection here has
// negotiated one, and an AUTH arriving on one is a protocol error, which
// §4.13.1 answers with a DISCONNECT carrying 0x82.
//
// The substrate reads an AUTH, offers it to a hook saguin does not
// implement, and does nothing else with it. Left alone, the packet was
// swallowed and the client waited on an answer that was never sent, which
// is the same silence as the CONNECT above one round trip later.
func TestAnAuthPacketOnAConnectionThatNegotiatedNoneIsRefused(t *testing.T) {
	h := start(t)
	r := rawDialTo(t, h.Addr)
	r.write(connectPacket(5, 0x02, []byte("v-reauth")))
	if no, how := refused(t, r); no {
		t.Fatalf("the connection itself was refused (%s), so this says nothing "+
			"about the AUTH", how)
	}

	// AUTH, reason 0x19 (Re-authenticate), no properties.
	r.write([]byte{0xF0, 0x02, 0x19, 0x00})

	a := r.answer(3 * time.Second)
	switch {
	case !a.got && a.closed:
		return // closed
	case !a.got:
		t.Fatalf("the AUTH was swallowed: the broker neither answered nor closed, " +
			"and the client is left waiting on a re-authentication that will never " +
			"be answered")
	case a.kind == 14:
		if len(a.body) >= 1 && a.body[0] != 0x82 {
			t.Errorf("disconnected with 0x%s, want 0x82: a client asking to "+
				"re-authenticate when it never authenticated has made a protocol "+
				"error, and §4.13.1 names that code", hex(a.body[0]))
		}
	default:
		t.Errorf("answered packet type %d (body % 02x). saguin runs no enhanced "+
			"authentication, so there is no exchange to continue and no answer "+
			"but a refusal", a.kind, a.body)
	}
}

// subscribePacket assembles a SUBSCRIBE with one filter and the options byte
// as given, so a test can set bits the specification reserves.
func subscribePacket(version byte, id uint16, filter string, opts byte) []byte {
	var v []byte
	v = append(v, byte(id>>8), byte(id))
	if version == 5 {
		v = append(v, 0x00) // no properties
	}
	v = append(v, str([]byte(filter))...)
	v = append(v, opts)
	return append([]byte{0x82, byte(len(v))}, v...)
}

// The filter rules. A filter saguin cannot serve must be refused rather than
// granted, or a client is told yes and served nothing - which is the shape
// invariant 11 refuses everywhere else.
func TestAMalformedSubscribeIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name, clause string
		filter       string
		opts         byte
	}{
		// [MQTT-4.7.3-1] "All Topic Names and Topic Filters MUST be at
		// least one character long."
		{"an empty filter", "MQTT-4.7.3-1", "", 0x00},

		// [MQTT-4.7.1-2] "The single-level wildcard MUST occupy an entire
		// level of the filter." `sport+` is one level carrying a `+` beside
		// other characters.
		{"a + that is not a whole level", "MQTT-4.7.1-2", "sport+/x", 0x00},

		// The same rule at the end of a filter, which is the spelling a
		// person writes when they mean a prefix match.
		{"a + appended to a level", "MQTT-4.7.1-2", "sport/tennis+", 0x00},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t)
			r := rawDialTo(t, h.Addr)
			r.write(connectPacket(5, 0x02, []byte("sub-"+tc.clause), nil))
			if no, how := refused(t, r); no {
				t.Fatalf("the connection itself was refused (%s), so this says nothing "+
					"about the subscription", how)
			}
			r.write(subscribePacket(5, 1, tc.filter, tc.opts))

			a := r.answer(3 * time.Second)
			switch {
			case !a.got && a.closed:
				return // closed, which is a refusal
			case !a.got:
				t.Fatalf("%s: the SUBSCRIBE was neither answered nor refused, so the "+
					"client waits on a SUBACK that is never coming", tc.clause)
			case a.kind == 14:
				return // DISCONNECT, likewise
			case a.kind == 9 && len(a.body) > 0 && a.body[len(a.body)-1] >= 0x80:
				return // SUBACK carrying a failure code
			}
			t.Errorf("%s: the filter %q was granted (packet type %d, body % 02x). A "+
				"subscription saguin cannot serve has to be refused, or the client is "+
				"told yes and served nothing", tc.clause, tc.filter, a.kind, a.body)
		})
	}
}

// Which of the two refusals a CONNECT earns is a byte on the wire, and MQTT
// decides it rather than the broker. A Malformed Packet is "a control packet
// that cannot be parsed according to this specification"; a Protocol Error
// is "detected after the packet has been parsed and found to contain data
// that is not allowed by the protocol" (section 1.2), and section 4.13.1
// answers them 0x81 and 0x82. Where the specification labels a condition,
// the label is the oracle.
//
// The test asks for the byte, not for a refusal: the tests above pass on
// either code. Only the CONNACK carries one here. A packet refused while it
// is decoded after the CONNECT is answered by closing the connection with no
// DISCONNECT, so its code is pinned where the engine decides it
// (TestEachRefusalCarriesTheCodeItsConditionIsGiven).
func TestAMalformedConnectIsAnsweredMalformed(t *testing.T) {
	for _, tc := range []struct {
		name, oracle string
		connect      []byte
	}{
		// Section 3.1.2.3: "If the reserved flag is not 0 it is a
		// Malformed Packet."
		{"its reserved flag set", "3.1.2.3", connectPacket(5, 0x02|0x01, []byte("v-reserved"))},

		// Section 3.1.2.6: "A value of 3 (0x03) is a Malformed Packet", of
		// the Will QoS. The Will is otherwise whole, so nothing else about
		// it is refused first.
		{"a Will QoS of 3", "3.1.2.6", connectPacket(5, 0x02|0x04|0x18, []byte("v-willqos3"),
			[]byte{0x00}, str([]byte("w/t")), str([]byte("bye")))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t)
			r := rawDialTo(t, h.Addr)
			r.write(tc.connect)
			a := r.answer(3 * time.Second)
			if !a.got || a.kind != 2 || len(a.body) < 2 {
				t.Fatalf("%s: no CONNACK carrying a reason (got=%v closed=%v type %d body % 02x)",
					tc.oracle, a.got, a.closed, a.kind, a.body)
			}
			if a.body[1] != 0x81 {
				t.Errorf("%s: a Malformed Packet was answered 0x%s, want 0x81 (body % 02x)",
					tc.oracle, hex(a.body[1]), a.body)
			}
		})
	}
}
