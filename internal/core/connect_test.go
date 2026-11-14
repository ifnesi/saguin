package core_test

import (
	"fmt"
	"regexp"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// settings is what an MQTT 5 client takes from its CONNACK, each value MQTT
// 5's rule for an absent property filling in what the CONNACK leaves out.
type settings struct {
	keepAlive  uint32 // Server Keep Alive, else the CONNECT's Keep Alive [MQTT-3.2.2-21], [MQTT-3.2.2-22]
	expiry     uint32 // 3.2.2.3.2: "If the Session Expiry Interval is absent the value in the CONNECT Packet used"
	receiveMax uint32 // 3.2.2.3.3: "If the Receive Maximum value is absent, then its value defaults to 65,535"
	maxPacket  uint32 // 3.2.2.3.6: absent, "there is no limit": 0 here
	aliasMax   uint32 // 3.2.2.3.8: "If the Topic Alias Maximum property is absent, the default value is 0"
	maxQoS     uint32 // 3.2.2.3.4: "If the Maximum QoS is absent, the Client uses a Maximum QoS of 2"
	retain     uint32 // 3.2.2.3.5: "If not present, then retained messages are supported"
	shared     uint32 // 3.2.2.3.13: "If not present, then Shared Subscriptions are supported"
	clientID   string // the Assigned Client Identifier, else the CONNECT's [MQTT-3.1.3-7]
}

// settingsOf reads ack, the CONNACK answering p, as the client does.
func settingsOf(ack *packet.Connack, p *packet.Connect) settings {
	s := settings{keepAlive: uint32(p.KeepAlive), receiveMax: 65_535, maxQoS: 2, retain: 1, shared: 1, clientID: p.ClientID}
	for prop := range p.Properties.All() {
		if prop.ID == packet.SessionExpiryInterval {
			s.expiry = prop.Int
		}
	}
	for prop := range ack.Properties.All() {
		switch prop.ID {
		case packet.ServerKeepAlive:
			s.keepAlive = prop.Int
		case packet.SessionExpiryInterval:
			s.expiry = prop.Int
		case packet.ReceiveMaximum:
			s.receiveMax = prop.Int
		case packet.MaximumPacketSize:
			s.maxPacket = prop.Int
		case packet.TopicAliasMaximum:
			s.aliasMax = prop.Int
		case packet.MaximumQoS:
			s.maxQoS = prop.Int
		case packet.RetainAvailable:
			s.retain = prop.Int
		case packet.SharedSubscriptionAvailable:
			s.shared = prop.Int
		case packet.AssignedClientIdentifier:
			s.clientID = prop.String
		}
	}
	return s
}

// checked is the CONNECT a reader hands over with the reason its checks
// give: for clientID at version, decoded whole; or, refused for its size
// (0x95) or its version (0x84), before the body is decoded, its protocol
// level alone, none for a level that is neither MQTT 3.1.1's nor 5's
// (design.md, CONNECT: the order is "It checks max_connect_size. 3. It checks
// Protocol Name. 4. It checks Protocol Level and min_protocol_version. 5. It
// checks complete packet validity"; "A size refusal precedes checks requiring
// the unread body").
func checked(version packet.Version, clientID string, reason byte) *packet.Connect {
	switch {
	case reason != 0x84 && reason != 0x95:
		return connectOf(version, clientID)
	case version == packet.V311 || version == packet.V5:
		return &packet.Connect{Version: version}
	default:
		return &packet.Connect{}
	}
}

// design.md, CONNECT, 2: "The broker checks password, access, version, client
// id and limits. A failure gets a CONNACK with its reason, then the
// connection closes", the reader's checks handed over as the CONNECT's Reason
// (connection_test.go: 0x84, an unknown level and MQTT 3.1 among them, 0x85,
// 0x8C, 0x83, 0x95); only then, "5. Take over". MQTT 5 [MQTT-3.1.4-2]: "If
// any of these checks fail, it MUST close the Network Connection";
// [MQTT-3.2.2-6]: Session Present 0. The refused CONNECT names the id of a
// client already connected: the core answers it with its refusal alone, and
// the connected client keeps its session.
func TestCheckFailuresAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version packet.Version
		reason  byte
	}{
		{"MQTT 3.1.1 below min_protocol_version", packet.V311, 0x84},
		{"MQTT 3.1", 3, 0x84},
		{"an unknown protocol level", 6, 0x84},
		{"a client id the reader refuses, MQTT 5", packet.V5, 0x85},
		{"a client id the reader refuses, MQTT 3.1.1", packet.V311, 0x85},
		{"an Authentication Method", packet.V5, 0x8C},
		{"a saguin- User Property", packet.V5, 0x83},
		{"over max_connect_size, MQTT 5", packet.V5, 0x95},
		{"over max_connect_size, MQTT 3.1.1", packet.V311, 0x95},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, newConfig(t))
			holder := s.connected(connectOf(packet.V5, "x"))
			refused := s.accept()
			ack, acts := s.connect(refused, checked(tc.version, "x", tc.reason), tc.reason)
			if ack.ReasonCode != tc.reason || ack.SessionPresent || len(acts) != 1 {
				t.Errorf("CONNACK 0x%02X, Session Present %v, in a decision of %d actions; want 0x%02X, 0, and nothing else",
					ack.ReasonCode, ack.SessionPresent, len(acts), tc.reason)
			}
			s.gone(refused)
			s.pinged(holder)
			s.expectConnections(0, 1)
		})
	}
}

// MQTT 5: "If the Server rejects the CONNECT, it MUST NOT process any data
// sent by the Client after the CONNECT packet except AUTH packets
// [MQTT-3.1.4-6]"; 0007a.md, 261012 112240: "after a CONNECT the core
// refuses (a credential, max_connections, a full store), the reader, which
// passed it on as acceptable, reads on until the writer closes; the core
// ignores whatever that connection hands it after the refused CONNECT".
// Here the core refuses for a full store (Store: "Changes made by CONNECT
// [...] are refused with that packet's code", 0x97). A PINGREQ gets no
// PINGRESP (step); its Ended, whatever it carries, gets the connection's one
// final entry and nothing more.
func TestWhatFollowsARefusedConnectIsIgnored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		d      *packet.Disconnect
		reason byte
	}{
		{"the client's DISCONNECT", &packet.Disconnect{}, 0},
		{"the client's DISCONNECT with Will Message", &packet.Disconnect{ReasonCode: 0x04}, 0},
		{"a protocol error", nil, 0x82},
		{"lost", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, newConfig(t))
			a := s.connected(connectOf(packet.V5, "a"))
			refused := s.accept()
			s.receive(refused, keeping(packet.V5, "b", 60), 0)
			s.answer(only[*store.LoadSession](s))
			if ack := connackIn(s.reply(only[store.Write](s), store.ErrFull), refused); ack == nil || ack.ReasonCode != 0x97 {
				t.Fatalf("CONNACK %+v, want 0x97", ack)
			}
			if got := repliesTo(s.ping(refused), refused); len(got) != 0 {
				t.Errorf("a PINGREQ after the refused CONNECT answered with %v, want nothing", showAll(got))
			}
			s.end(refused, tc.d, tc.reason)
			s.close(refused)
			s.pinged(a)
			s.expectConnections(0, 1)
		})
	}
}

// design.md, Closing a connection, 2: "The core ignores packets from a
// connection it has already closed or taken over"; CONNECT, 5: "An old
// connection for the id gets DISCONNECT 0x8E on MQTT 5 and is closed. If
// its writer has not taken that DISCONNECT within 500 ms, its socket is cut
// (Closing a connection). Anything its reader still delivers is ignored"
// [MQTT-3.1.4-3]. A client
// connected as "x" is taken over by a second CONNECT for "x"; a PINGREQ its
// reader then hands the core gets no PINGRESP, while the taker's PINGREQ is
// answered. TestWhatFollowsARefusedConnectIsIgnored is the closed case.
func TestWhatATakenOverConnectionHandsIsIgnored(t *testing.T) {
	for _, version := range []packet.Version{packet.V5, packet.V311} {
		t.Run(protocols[version], func(t *testing.T) {
			s := newScript(t, newConfig(t))
			old := s.connected(connectOf(version, "x"))
			taker := s.connected(connectOf(version, "x"))
			if got := repliesTo(s.ping(old), old); len(got) != 0 {
				t.Errorf("a PINGREQ from the connection taken over answered with %v, want nothing", showAll(got))
			}
			s.pinged(taker)
			s.gone(old)
		})
	}
}

// design.md, CONNECT, 3: "Order. While an earlier CONNECT for the same id
// waits for the store, this one waits too"; 4: "Load. Unless the id is
// connected, the core reads its session from the store"; 8: "Once the changes
// are stored, the broker sends CONNACK"; design.md, Flows, Replies: "CONNACK
// (when it changes a session) [...] sent only once the store confirms". A
// CONNECT for an id not connected asks the store for its session, carrying
// its connection, and answers nothing yet; a second CONNECT for the id waits,
// asking nothing, while one for another id does not wait. The first is
// answered once its session is stored, before the second takes it over
// (DISCONNECT 0x8E after its CONNACK) and resumes it, Session Present 1
// [MQTT-3.2.2-3].
func TestConnectWaitsForTheStore(t *testing.T) {
	for _, version := range []packet.Version{packet.V5, packet.V311} {
		t.Run(protocols[version], func(t *testing.T) {
			s := newScript(t, newConfig(t))
			first, second, other := s.accept(), s.accept(), s.accept()
			acts := s.receive(first, keeping(version, "x", 60), 0)
			load := only[*store.LoadSession](s)
			if load.Client != "x" || len(acts) != 1 || acts[0].Conn != first {
				t.Fatalf("the CONNECT's decision: %d actions, a LoadSession for %q; want the LoadSession for \"x\" alone, carrying its connection", len(acts), load.Client)
			}
			if acts := s.receive(second, keeping(version, "x", 60), 0); len(acts) != 0 {
				t.Fatalf("a second CONNECT for \"x\" while the first waits for the store: %d actions, want none", len(acts))
			}
			if acts := s.receive(other, connectOf(version, "y"), 0); len(acts) != 1 || acts[0].Read == nil {
				t.Fatalf("a CONNECT for \"y\" while one for \"x\" waits: %d actions, want its LoadSession", len(acts))
			}
			acts = s.answer(load)
			if replyCount(acts) != 0 || len(writesIn(acts)) != 1 {
				t.Fatalf("the first CONNECT's session loaded: %d replies, %d writes; want its session stored first, alone", replyCount(acts), len(writesIn(acts)))
			}
			acts = s.settle()
			got := repliesTo(acts, first)
			if len(got) != 2 || connackIn(acts, first) != got[0] || got[0].(*packet.Connack).ReasonCode != 0 || !isDisconnect(got[1], 0x8E) {
				t.Fatalf("the first connection got %v, want CONNACK 0x00, then DISCONNECT 0x8E", showAll(got))
			}
			if ack := connackIn(acts, second); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
				t.Fatalf("the second CONNECT got %v, want CONNACK 0x00, Session Present 1", showAll(repliesTo(acts, second)))
			}
			s.expectRows()
		})
	}
}

// R6: "Everything is bounded"; design.md, Execution, Resource bounds: "The
// configured limits bound what the core keeps. It holds connected sessions up
// to limits.max_connections"; design.md, CONNECT, 3: "While an earlier
// CONNECT for the same id waits for the store, this one waits too". Audits
// 261018-103a: a CONNECT waiting behind an unanswered store request, whose
// connection then ends and reports its close, stayed in the core until that
// answer, so a client id whose store request is slow held one connection's
// state per CONNECT, however many closed. Under a max_connections of 4, never
// exceeded, the first CONNECT for "x" waits for its session's read; a live
// CONNECT for "x" waits behind it; then ten times max_connections connections
// send a CONNECT for "x", end and report their close, one at a time; then a
// last live CONNECT for "x" waits. design.md, Store reads: "Before applying a
// result, the core checks whether its requesting owner and connection still
// exist and still own that request. An ended or superseded request has no
// further effects." A CONNECT for "y" is read, the store holding back the
// read's view; its connection ends and reports its close; the view's
// confirmation and the read's result then ask nothing. With x's read still
// unanswered, the core holds nothing of the closed connections, y's included.
// Once it is answered, the live CONNECTs are served in their order: the first
// is answered and taken over by the second, which is answered and taken over
// by the last, which holds the session.
func TestClosedWaitingConnectsAreNotHeld(t *testing.T) {
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxConnections = 4
	s := newScript(t, cfg)
	s.holdViews = true
	first, second := s.accept(), s.accept()
	s.receive(first, connectOf(packet.V5, "x"), 0)
	load := only[*store.LoadSession](s)
	if acts := s.receive(second, connectOf(packet.V5, "x"), 0); len(acts) != 0 {
		t.Fatalf("a second CONNECT for \"x\" while the first waits for the store: %d actions, want none", len(acts))
	}
	closed := closeWhileWaiting(s, "x", 10*cfg.Broker.Limits.MaxConnections)
	last := s.accept()
	if acts := s.receive(last, connectOf(packet.V5, "x"), 0); len(acts) != 0 {
		t.Fatalf("the last CONNECT for \"x\" while the first waits for the store: %d actions, want none", len(acts))
	}
	closed = append(closed, closeWhileRead(s, "y"))

	runtime.GC() // every closed connection nothing holds is collected
	held := 0
	for _, conn := range closed {
		if conn.Value() != nil {
			held++
		}
	}
	if held != 0 {
		t.Errorf("%d of %d connections that sent a CONNECT for \"x\" and closed while it waited are still held, "+
			"under a max_connections of %d (R6)", held, len(closed), cfg.Broker.Limits.MaxConnections)
	}

	acts := append(s.answer(load), s.settle()...)
	for _, c := range []struct {
		name string
		conn *connection.Conn
		want string
	}{
		{"first", first, "[CONNACK 0x00 DISCONNECT 0x8E]"},
		{"second", second, "[CONNACK 0x00 DISCONNECT 0x8E]"},
		{"last", last, "[CONNACK 0x00]"},
	} {
		if got := fmt.Sprint(showAll(repliesTo(acts, c.conn))); got != c.want {
			t.Errorf("the %s live CONNECT for \"x\" got %s, want %s: the live CONNECTs served in their order", c.name, got, c.want)
		}
	}
}

// closeWhileWaiting has n connections, one at a time, send a CONNECT for
// client, which waits for an earlier one, end, and report their close, and
// returns a weak pointer to each. The script forgets each once closed, so
// only the core can still hold it.
func closeWhileWaiting(s *script, client string, n int) []weak.Pointer[connection.Conn] {
	s.t.Helper()
	closed := make([]weak.Pointer[connection.Conn], n)
	for i := range closed {
		conn := s.accept()
		if acts := s.receive(conn, connectOf(packet.V5, client), 0); len(acts) != 0 {
			s.t.Fatalf("CONNECT %d for %q while an earlier one waits for the store: %d actions, want none", i+1, client, len(acts))
		}
		if got := repliesTo(s.end(conn, nil, 0), conn); len(got) != 1 || got[0] != (connection.Close{}) {
			s.t.Fatalf("connection %d ended while its CONNECT waited, and got %v, want its final entry, Close", i+1, showAll(got))
		}
		s.close(conn)
		delete(s.conns, conn)
		closed[i] = weak.Make(conn)
	}
	return closed
}

// closeWhileRead has a connection send a CONNECT for client, an id with no
// other request, end while the store holds back its read's view, and report
// its close; then the store confirms the read's view and answers it, which
// must ask nothing. It returns a weak pointer to the connection, which the
// script forgets once closed, so only the core can still hold it.
func closeWhileRead(s *script, client string) weak.Pointer[connection.Conn] {
	s.t.Helper()
	conn := s.accept()
	acts := s.receive(conn, connectOf(packet.V5, client), 0)
	if len(acts) != 1 || acts[0].Read == nil {
		s.t.Fatalf("the CONNECT for %q: %d actions, want its read", client, len(acts))
	}
	load := acts[0].Read
	if got := repliesTo(s.end(conn, nil, 0), conn); len(got) != 1 || got[0] != (connection.Close{}) {
		s.t.Fatalf("the connection ended while its CONNECT's read was outstanding, and got %v, want its final entry, Close", showAll(got))
	}
	s.close(conn)
	delete(s.conns, conn)
	if acts := append(s.view(load), s.answer(load)...); len(acts) != 0 {
		s.t.Fatalf("the read of a closed connection's CONNECT, its view confirmed and its result answered, asked %d things, want none", len(acts))
	}
	return weak.Make(conn)
}

// What a session stands as before a CONNECT for its client id.
const (
	noSession        = "no session"
	awaySession      = "an away session"
	connectedSession = "a session ending with its connection, connected"
	durableSession   = "a durable session, connected"
)

// design.md, CONNECT: "Session Present is 1 when the CONNECT keeps its
// session and a session exists for the client id, either away or connected
// with a session expiry above 0. Keeping the session means Clean Start 0, or
// MQTT 3.1.1 Clean Session 0. A takeover of a session whose expiry is 0 ends
// it and starts a new one. Otherwise Session Present is 0" [MQTT-3.2.2-2],
// [MQTT-3.2.2-3] (MQTT 3.1.1: [MQTT-3.2.2-1], [MQTT-3.2.2-2]); MQTT 5
// 3.1.2.11.2: with an expiry of 0 "the Session ends when the Network
// Connection is closed"; MQTT 3.1.1 3.1.2.4: such a session's state "MUST NOT
// be reused in any subsequent Session" [MQTT-3.1.2-6]; metrics.md:
// saguin_sessions_ended_total's cause connection_end "(expiry 0)"; "4. Load.
// Unless the id is connected, the core reads its session from the store"; "5.
// Take over. An old connection for the id gets DISCONNECT 0x8E"; "Clean start
// ends an existing session [...] Keeping the session resumes it [...]
// Otherwise a new session starts" [MQTT-3.1.2-4], [MQTT-3.1.2-5] (MQTT 3.1.1:
// [MQTT-3.1.2-4], [MQTT-3.1.2-6]); "The core stores step 6's changes [...]
// and the session record in one transaction, unless nothing changed. Nothing
// changed when there was no earlier session, there is no Will, and the new
// session ends with its connection"; "Once the changes are stored, the broker
// sends CONNACK". config.md: max_session_expiry "is MQTT 3.1.1 Clean Session
// 0's". storage.md, Ids: an id "is never reused"; metrics.md:
// saguin_sessions_ended_total{cause="clean_start"}.
//
// For each session a client id can have, and each CONNECT: the CONNECT
// loads the session unless it is connected, whose connection gets
// DISCONNECT 0x8E at once; it is answered once its changes are stored, and
// at once where nothing changed in the store; Session Present is as MQTT
// says; the store then holds what a restart restores (expectRows), a
// resumed session under its own id and a new one under a new id, never the
// id of the session it took over; a clean start that ends a session counts
// it, and a takeover that ends a connected session of expiry 0 counts it
// as its connection's end.
func TestConnectDecidesAndStores(t *testing.T) {
	const highest = 5
	for _, version := range []packet.Version{packet.V5, packet.V311} {
		for _, before := range []string{noSession, awaySession, connectedSession, durableSession} {
			for _, clean := range []bool{true, false} {
				for _, expiry := range []uint32{0, 300} {
					if version == packet.V311 && expiry != 0 {
						continue // MQTT 3.1.1's expiry is Clean Session's
					}
					name := fmt.Sprintf("%s, %s, clean start %v, expiry %d", protocols[version], before, clean, expiry)
					t.Run(name, func(t *testing.T) {
						p := keeping(version, "x", expiry)
						p.CleanStart = clean
						decideAndStore(t, highest, before, p)
					})
				}
			}
		}
	}
}

// decideAndStore is one case of TestConnectDecidesAndStores: a CONNECT, p,
// for client id "x", whose session stands as before, on a store whose
// highest session id is highest.
func decideAndStore(t *testing.T, highest int64, before string, p *packet.Connect) {
	away := store.Session{ID: highest, Client: "x", Protocol: byte(p.Version), Expiry: 3600, Left: epoch.UnixMilli(), ReceiveMaximum: 20, KeepAlive: 60}
	var s *script
	var old *connection.Conn
	switch before {
	case awaySession:
		s = newScriptOn(t, newConfig(t), &store.State{HighestSession: highest}, away)
	case connectedSession:
		s = newScriptOn(t, newConfig(t), &store.State{HighestSession: highest})
		old = s.connected(connectOf(p.Version, "x"))
	case durableSession:
		s = newScriptOn(t, newConfig(t), &store.State{HighestSession: highest})
		old = s.connected(keeping(p.Version, "x", 600))
	default:
		s = newScriptOn(t, newConfig(t), &store.State{HighestSession: highest})
	}
	earlier, stored := s.rows["x"]
	_, granted := grantedTo(s.cfg, p, &packet.Connack{})
	changes := stored || granted != 0

	conn := s.accept()
	acts := s.receive(conn, p, 0)
	loads := pending[*store.LoadSession](s)
	if want := old == nil; (len(loads) == 1) != want {
		t.Fatalf("%d LoadSessions; want one: %v (design.md, CONNECT, 4)", len(loads), want)
	}
	if old != nil {
		if got := repliesTo(acts, old); len(got) != 1 || !isDisconnect(got[0], 0x8E) {
			t.Fatalf("the old connection got %v, want DISCONNECT 0x8E at once", showAll(got))
		}
	} else {
		acts = s.answer(loads[0])
	}
	writes := pending[store.Write](s)
	ack := connackIn(acts, conn)
	switch {
	case changes && (len(writes) != 1 || ack != nil):
		t.Fatalf("%d writes outstanding, CONNACK %v; want the CONNECT's one write, and no CONNACK before it is stored", len(writes), ack != nil)
	case changes:
		ack = connackIn(s.answer(writes[0]), conn)
	case len(writes) != 0:
		t.Fatalf("%d writes for a CONNECT that changes nothing in the store, want none", len(writes))
	}
	// A session a CONNECT can keep: away, or connected with an expiry
	// above 0; a takeover ends a connected one of expiry 0.
	keepable := before == awaySession || before == durableSession
	resumed := !p.CleanStart && keepable
	if ack == nil || ack.ReasonCode != 0 || ack.SessionPresent != resumed {
		t.Fatalf("CONNACK %+v; want 0x00, Session Present %v", ack, resumed)
	}
	s.expectRows()
	// The id of the session before: its row's, or, for a connected one
	// of expiry 0, which has none, the first after the highest loaded
	// (storage.md, Ids: "assignment resumes at the highest of that kind
	// loaded at start plus one").
	oldID := max(highest, earlier.ID)
	if before == connectedSession {
		oldID = highest + 1
	}
	row, durable := s.rows["x"]
	switch {
	case durable && resumed && stored && row.ID != earlier.ID:
		t.Errorf("the resumed session is stored as session %d, want its own id, %d", row.ID, earlier.ID)
	case durable && !resumed && row.ID <= oldID:
		t.Errorf("the new session is stored as session %d, want a new id, above %d (storage.md, Ids)", row.ID, oldID)
	}
	var cleanStarts, connectionEnds float64
	if p.CleanStart && keepable {
		cleanStarts = 1
	}
	if before == connectedSession {
		connectionEnds = 1
	}
	s.expectEnded(cleanStarts, 0, connectionEnds)
}

// design.md, Words: a session is "Away when it outlives its connection,
// until its expiry"; CONNECT: Session Present is 1 when "a session exists for
// the client id, either away"; 4: "the core reads its session from the
// store"; DISCONNECT: "When the expiry runs out, [...] the session deleted";
// Testing: a decision is "a function of its state, one event and that event's
// time". MQTT 5 [MQTT-4.1.0-2]: "The Server MUST discard the Session State
// when the Network Connection is closed and the Session Expiry Interval has
// passed". A CONNECT keeping its session whose row runs out at the CONNECT's
// own time finds no session, Session Present 0, and that session's ending is
// counted as expired; one whose row runs out a millisecond later resumes it,
// Session Present 1, even though the store answers after it has run out: what
// counts is the CONNECT's time. Either way the store then holds the client's
// session as it is.
func TestExpiredSessionIsNoSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after int64 // how long after the CONNECT the row runs out, in milliseconds
		want  bool  // Session Present
	}{
		{"running out at the CONNECT's time", 0, false},
		{"running out a millisecond later", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const expiry = 60
			at := epoch.Add(2 * time.Second) // the CONNECT's: accept, then receive
			row := store.Session{ID: 5, Client: "x", Protocol: 5, Expiry: expiry, Left: at.UnixMilli() + tc.after - expiry*1000, ReceiveMaximum: 20}
			s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 5}, row)
			conn := s.accept()
			s.receive(conn, keeping(packet.V5, "x", expiry), 0)
			if !s.now.Equal(at) {
				t.Fatalf("the CONNECT at %v, want %v", s.now, at)
			}
			s.tick = time.Minute // the store answers after the row has run out
			ack := connackIn(s.settle(), conn)
			if ack == nil || ack.ReasonCode != 0 || ack.SessionPresent != tc.want {
				t.Fatalf("CONNACK %+v; want 0x00, Session Present %v", ack, tc.want)
			}
			s.expectRows()
			var expired float64
			if !tc.want {
				expired = 1
				if got := s.rows["x"].ID; got == row.ID {
					t.Errorf("the new session is stored as session %d, the expired one's (storage.md, Ids: never reused)", got)
				}
			}
			s.expectEnded(0, expired, 0)
		})
	}
}

// storage.md, Ids: "The core assigns ids [...] ids start from 1 and
// assignment resumes at the highest of that kind loaded at start plus one. An
// id is therefore never reused while anything names it [...] Every session
// has an id, durable or not"; 0007c.md, 261016 093650: "State keeps
// HighestSession". With 41 loaded, the first session is 42; a session ending
// with its connection takes an id of its own, so the next durable one is
// above 43; a resumed session keeps its id, and a clean start makes a new
// session with a new id.
func TestSessionIDsContinue(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 41})
	a := s.connected(keeping(packet.V5, "a", 60))
	s.connected(connectOf(packet.V5, "b"))
	s.connected(keeping(packet.V5, "c", 60))
	if got := s.rows["a"].ID; got != 42 {
		t.Errorf("the first session's id is %d, want 42", got)
	}
	if got := s.rows["c"].ID; got <= 43 {
		t.Errorf("the third session's id is %d, want above 43", got)
	}
	s.gone(a)
	if ack, _ := s.connect(s.accept(), keeping(packet.V5, "a", 60), 0); !ack.SessionPresent || s.rows["a"].ID != 42 {
		t.Errorf("a resumed: Session Present %v, stored as %d; want 1, and its id, 42", ack.SessionPresent, s.rows["a"].ID)
	}
	fresh := keeping(packet.V5, "a", 60)
	fresh.CleanStart = true
	s.connect(s.accept(), fresh, 0)
	if got := s.rows["a"].ID; got <= s.rows["c"].ID {
		t.Errorf("a's clean start stored session %d, want a new id above %d", got, s.rows["c"].ID)
	}
}

// assignedPattern is design.md, CONNECT, 8's Assigned Client Identifier:
// "the CONNECT's time in UTC as YYMMDDHHMMSS followed by six random letters
// or digits".
var assignedPattern = regexp.MustCompile(`^\d{12}[0-9A-Za-z]{6}$`)

// checkAssigned fails the test unless id is an Assigned Client Identifier
// made at at.
func checkAssigned(t *testing.T, id string, at time.Time) {
	t.Helper()
	if !assignedPattern.MatchString(id) || id[:12] != at.UTC().Format("060102150405") {
		t.Fatalf("assigned %q, want %s followed by six letters or digits", id, at.UTC().Format("060102150405"))
	}
}

// design.md, CONNECT, 8: "An Assigned Client Identifier is the CONNECT's time
// in UTC as YYMMDDHHMMSS followed by six random letters or digits. If a
// connected or stored session already holds that identifier, it is replaced
// by another before the CONNACK"; 0007d.md, 261016 084432: "The time is the
// event's, never a clock read; Load's store read for the id is the check
// against stored sessions" [MQTT-3.2.2-16]; MQTT 5 [MQTT-3.1.3-7]: the server
// "MUST return the Assigned Client Identifier in the CONNACK"; MQTT 3.1.1
// [MQTT-3.1.3-6] assigns one too, returned nowhere. A CONNECT with an empty
// client id, at a time five hours behind UTC, reads the session of an id of
// that time in UTC; where the store holds a session for it, the core reads
// another, still of the CONNECT's time though the store answered later, and
// answers with the one no session holds. No client can force an assigned id
// onto one held: the conformance test TestAssignedClientIdentifier checks the
// rest from the wire.
func TestAssignedClientIdentifierIsTheConnectsTime(t *testing.T) {
	for _, version := range []packet.Version{packet.V5, packet.V311} {
		t.Run(protocols[version], func(t *testing.T) {
			s := newScript(t, newConfig(t))
			s.now = time.Date(2026, 3, 4, 23, 30, 58, 0, time.FixedZone("UTC-5", -5*60*60))
			conn := s.accept()
			s.receive(conn, connectOf(version, ""), 0)
			second := s.collide(conn, version, s.now)
			ack := connackIn(s.answer(second), conn)
			if ack == nil || ack.ReasonCode != 0 || ack.SessionPresent {
				t.Fatalf("CONNACK %+v, want 0x00, Session Present 0", ack)
			}
			if got := settingsOf(ack, connectOf(version, "")).clientID; version == packet.V5 && got != second.Client {
				t.Errorf("the CONNACK assigns %q, want %q", got, second.Client)
			}

		})
	}
}

// collide answers the LoadSession of conn's CONNECT, at version with an
// empty client id, taken at at, as a store holding a session for the id
// assigned answers it, and returns the core's LoadSession of the id it
// assigns instead, unanswered: design.md, CONNECT, 8, "the CONNECT's time in
// UTC as YYMMDDHHMMSS followed by six random letters or digits. If a
// connected or stored session already holds that identifier, it is replaced
// by another before the CONNACK".
func (s *script) collide(conn *connection.Conn, version packet.Version, at time.Time) *store.LoadSession {
	s.t.Helper()
	first := only[*store.LoadSession](s)
	checkAssigned(s.t, first.Client, at)
	first.Session = store.Session{ID: 3, Client: first.Client, Protocol: byte(version), Expiry: 3600, Left: at.UnixMilli(), ReceiveMaximum: 20}
	first.Found = true
	if acts := s.reply(first, nil); connackIn(acts, conn) != nil {
		s.t.Fatalf("CONNACK with %q, which a stored session holds", first.Client)
	}
	second := only[*store.LoadSession](s)
	checkAssigned(s.t, second.Client, at)
	if second.Client == first.Client {
		s.t.Fatalf("assigned %q again, which a stored session holds", second.Client)
	}
	return second
}

// design.md, Words: a record holds "The publisher's client id is used for No
// Local and is never sent"; storage.md, records: publisher, "the publisher's
// client id"; CONNECT, 8: an Assigned Client Identifier "If a connected or
// stored session already holds that identifier, it is replaced by another
// before the CONNACK"; Testing: a test inside the code checks "orderings no
// client can force". Audit 261019-009a: no test read a record's publisher.
//
// The ordering: a QoS 1 PUBLISH pipelined behind an MQTT 5 CONNECT with an
// empty client id reaches the core before the store answers the CONNECT's
// load that a stored session holds the identifier assigned. The core
// assigns another, which the CONNACK names; the PUBLISH's record, the one
// record the core sends the store, names that identifier as its
// publisher, never the one replaced.
func TestPipelinedPublishNamesTheIdentifierFinallyAssigned(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &broadcast)
	conn := s.accept()
	empty := connectOf(packet.V5, "")
	s.receive(conn, empty, 0)
	at := s.now
	acts := s.publish(conn, 1, 20)
	second := s.collide(conn, packet.V5, at)
	acts = append(acts, s.answer(second)...)
	ack := connackIn(acts, conn)
	if ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("CONNACK %+v, want 0x00", ack)
	}
	granted := settingsOf(ack, empty).clientID
	if granted != second.Client {
		t.Fatalf("the CONNACK assigns %q, want %q, the identifier no session holds", granted, second.Client)
	}
	writes := writesIn(acts)
	records := make([]store.Record, 0, len(writes))
	for _, w := range writes {
		records = append(records, core.RecordsOf(w)...)
	}
	s.settle()
	if len(records) != 1 || records[0].Publisher != granted {
		t.Fatalf("the records stored are %+v; want one, its publisher %q, the client id the CONNACK assigned", records, granted)
	}
}

// design.md, Store, A full disk: "Group refusal follows Writes. [...]
// Changes made by CONNECT, SUBSCRIBE or UNSUBSCRIBE are
// refused with that packet's code"; CONNECT: "0x97 | a full disk | 3"
// (0007a.md, 261012 223309: "CONNACK 0x97, MQTT 3.1.1 3", which the writer
// maps); "After a takeover whose CONNECT is then refused, the old session
// stays away"; MQTT 5 [MQTT-3.1.4-6]: after a refused CONNECT the server
// "MUST NOT process any data sent by the Client". A CONNECT whose write the
// store answers full gets CONNACK 0x97, Session Present 0; its PINGREQ is not
// answered; no session is counted as ended; the store keeps what it held, an
// away session staying as it was and a session taken over going away.
func TestFullDiskRefusesTheConnect(t *testing.T) {
	away := store.Session{ID: 5, Client: "x", Protocol: 5, Expiry: 3600, Left: epoch.UnixMilli(), ReceiveMaximum: 20, KeepAlive: 60}
	for _, version := range []packet.Version{packet.V5, packet.V311} {
		for _, tc := range []struct {
			name   string
			before string
			clean  bool
		}{
			{"a new session", noSession, false},
			{"resuming an away session", awaySession, false},
			{"a clean start over an away session", awaySession, true},
			{"taking a durable session over", durableSession, false},
		} {
			t.Run(protocols[version]+", "+tc.name, func(t *testing.T) {
				var rows []store.Session
				if tc.before == awaySession {
					away.Protocol = byte(version)
					rows = append(rows, away)
				}
				s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 5}, rows...)
				var old *connection.Conn
				if tc.before == durableSession {
					old = s.connected(keeping(version, "x", 600))
				}
				kept := s.rows["x"]
				p := keeping(version, "x", 300)
				p.CleanStart = tc.clean
				conn := s.accept()
				s.receive(conn, p, 0)
				for _, load := range pending[*store.LoadSession](s) {
					s.answer(load)
				}
				ack := connackIn(s.reply(only[store.Write](s), store.ErrFull), conn)
				if ack == nil || ack.ReasonCode != 0x97 || ack.SessionPresent {
					t.Fatalf("CONNACK %+v, want 0x97, Session Present 0", ack)
				}
				if got := repliesTo(s.ping(conn), conn); len(got) != 0 {
					t.Errorf("a PINGREQ after the refusal answered with %v, want nothing", showAll(got))
				}
				s.settle()
				switch row, ok := s.rows["x"]; {
				case old == nil && row != kept:
					t.Errorf("the store holds %+v (%v), want what it held, %+v", row, ok, kept)
				case old != nil && (row.Expiry != kept.Expiry || row.Left == 0):
					t.Errorf("the session taken over is stored as %+v, want it away with its expiry, %d", row, kept.Expiry)
				}
				if old != nil {
					s.end(old, nil, 0)
				}
				s.end(conn, nil, 0)
				s.settle()
				s.expectRows()
				s.expectEnded(0, 0, 0)
			})
		}
	}
}
