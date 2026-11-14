package core_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// disconnectWith is a client's DISCONNECT of code, carrying a Session
// Expiry Interval of expiry seconds where expiry is not nil.
func disconnectWith(code byte, expiry *uint32) *packet.Disconnect {
	d := &packet.Disconnect{ReasonCode: code}
	if expiry != nil {
		properties, err := packet.NewProperties(packet.DISCONNECT, []packet.Property{{ID: packet.SessionExpiryInterval, Int: *expiry}})
		if err != nil {
			panic(err)
		}
		d.Properties = properties
	}
	return d
}

// design.md, DISCONNECT: "The client may change its session expiry, except
// from 0, which is a protocol error. The change is stored before the
// connection closes"; "A lost connection [...] The session goes away until
// its expiry"; "For all away sessions together, the core keeps only the next
// session expiry and the next Will due time still to come. [...] They are
// brought forward when a session goes away"; "MQTT 3.1.1: Clean Session 1
// ends the session" (and Clean Session 0 keeps it, max_session_expiry its
// expiry, config.md); MQTT 5 3.14.2.2.2; [MQTT-3.1.2-23]: the session state
// is stored "after the Network Connection is closed if the Session Expiry
// Interval is greater than 0". storage.md, sessions: left is "when its
// connection ended"; config.md: max_session_expiry "caps session expiry".
// metrics.md: saguin_sessions_ended_total{cause="connection_end"}, "(expiry
// 0)". A durable session's connection ends: the core stores it away, with the
// expiry the DISCONNECT gives it, capped, else its own, and the Ended's time;
// one changed to 0 ends with its connection, its row going. Where the
// DISCONNECT changes the expiry, the final entry waits for the store. The
// core's timer then comes at the session's expiry.
func TestDisconnectStoresTheSessionAway(t *testing.T) {
	seconds := func(n uint32) *uint32 { return &n }
	for _, tc := range []struct {
		name    string
		version packet.Version
		d       *packet.Disconnect
		reason  byte
		want    uint32 // the expiry stored; 0: the session ends
		waits   bool   // the DISCONNECT changes the expiry
	}{
		{"a longer expiry", packet.V5, disconnectWith(0, seconds(120)), 0, 120, true},
		{"a shorter expiry", packet.V5, disconnectWith(0, seconds(30)), 0, 30, true},
		{"an expiry over max_session_expiry", packet.V5, disconnectWith(0, seconds(7200)), 0, 3600, true},
		{"with Will Message and a new expiry", packet.V5, disconnectWith(0x04, seconds(120)), 0, 120, true},
		{"an expiry of 0", packet.V5, disconnectWith(0, seconds(0)), 0, 0, true},
		{"no new expiry", packet.V5, disconnectWith(0, nil), 0, 60, false},
		{"lost", packet.V5, nil, 0, 60, false},
		{"keep-alive timeout", packet.V5, nil, 0x8D, 60, false},
		{"MQTT 3.1.1 Clean Session 0", packet.V311, &packet.Disconnect{}, 0, 3600, false},
		{"MQTT 3.1.1 Clean Session 0, lost", packet.V311, nil, 0, 3600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newConfig(t)
			cfg.Broker.Limits.MaxSessionExpiry = time.Hour
			s := newScript(t, cfg)
			conn := s.connected(keeping(tc.version, "x", 60))
			id := s.rows["x"].ID
			acts := s.end(conn, tc.d, tc.reason)
			left := s.now.UnixMilli()
			if tc.waits && len(repliesTo(acts, conn)) != 0 {
				t.Errorf("the final entry went before the store confirmed the new expiry")
			}
			var want store.Write = &store.LeaveSession{ID: id, Expiry: tc.want, Left: left}
			if tc.want == 0 {
				want = &store.EndSession{ID: id}
			}
			if got := writesIn(acts); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
				t.Fatalf("the connection's end wrote %v, want %v", got, want)
			}
			s.settle()
			if !s.conns[conn].final {
				t.Fatalf("no final entry once the store confirmed")
			}
			s.close(conn)
			s.expectRows()
			if tc.want == 0 {
				s.expectEnded(0, 0, 1)
				return
			}
			s.expectEnded(0, 0, 0)
			if row := s.rows["x"]; row.Expiry != tc.want || row.Left != left {
				t.Errorf("the session is stored as %+v, want Expiry %d, Left %d", row, tc.want, left)
			}
			if got, want := s.core.Timer(), time.UnixMilli(left+int64(tc.want)*1000); !got.Equal(want) {
				t.Errorf("Timer() = %v, want the session's expiry, %v", got, want)
			}
		})
	}
}

// metrics.md: saguin_sessions_ended_total{cause}, "connection_end (expiry
// 0)"; MQTT 5 3.1.2.11.2: "If it is set to 0, or is absent, the Session
// ends when the Network Connection is closed"; design.md, DISCONNECT:
// "MQTT 3.1.1: Clean Session 1 ends the session"; storage.md, What is
// stored: "non-durable sessions and everything in flight to them stay in
// memory", and Ids: "Only a durable session has a row in sessions". MQTT 5
// 3.14.2.2.2: a DISCONNECT changing the expiry from 0 "does not [count] as a
// valid DISCONNECT packet. The Server uses DISCONNECT with Reason Code 0x82",
// the reader's Ended owing 0x82 (connection_test.go). Each such end sends its
// final entry at once, writes nothing, and counts one session ended.
func TestSessionEndingWithItsConnectionIsCounted(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version packet.Version
		d       *packet.Disconnect
		reason  byte
	}{
		{"a clean DISCONNECT", packet.V5, &packet.Disconnect{}, 0},
		{"lost", packet.V5, nil, 0},
		{"the expiry changed from 0", packet.V5, nil, 0x82},
		{"MQTT 3.1.1 Clean Session 1", packet.V311, &packet.Disconnect{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, newConfig(t))
			conn := s.connected(connectOf(tc.version, "x"))
			acts := s.end(conn, tc.d, tc.reason)
			if len(repliesTo(acts, conn)) != 1 || len(writesIn(acts)) != 0 {
				t.Errorf("the end: replies %v, writes %v; want the final entry at once, and nothing stored", showAll(repliesTo(acts, conn)), writesIn(acts))
			}
			s.expectEnded(0, 0, 1)
		})
	}
}
