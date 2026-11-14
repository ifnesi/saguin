package store_test

import (
	"context"
	"maps"
	"strconv"
	"testing"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// These tests drive the store's delivery bookkeeping (storage.md, positions
// and session_inflight) through groups as the core seals them, each a real
// transaction on a real database, and check the rows a commit leaves:
// a transaction's composition is an ordering no client can see (design.md,
// Testing). design.md, Store, Writes: "The writer executes exactly the
// sealed group as one transaction and returns one transaction result.
// Consecutive members changing the delivery bookkeeping of different
// sessions may be combined into shared statements; any other member, or a
// later member for the same session, keeps its place in member order."

// deliveryChannel is the channel every test exchange and position is on.
const deliveryChannel = 2

// inflightRow is session's exchange packetID, on deliveryChannel at offset,
// with moves: a session_inflight row of storage.md.
func inflightRow(t *testing.T, session, packetID, offset, moves int64) tableRow {
	t.Helper()
	return tableRow{"session_inflight", testkit.With(testkit.TableNamed(t, "session_inflight").Valid[0], testkit.Row{
		"session": session, "packet_id": packetID, "seq": packetID, "channel": deliveryChannel, "offset": offset, "moves": moves,
	})}
}

// positionRow is session's position on deliveryChannel at offset: a
// positions row of storage.md.
func positionRow(t *testing.T, session, offset int64) tableRow {
	t.Helper()
	return tableRow{"positions", testkit.With(testkit.TableNamed(t, "positions").Valid[0], testkit.Row{
		"session": session, "channel": deliveryChannel, "offset": offset,
	})}
}

// bookkeeping is a test read of one session's delivery bookkeeping on
// deliveryChannel: its position's offset, 0 where it has none, and each of
// its exchanges' moves, by packet identifier.
type bookkeeping struct {
	session  int64
	position int64
	moves    map[int64]int64
}

func (b *bookkeeping) ReadConn(conn *store.Conn) error {
	err := conn.QueryRowContext(context.Background(), `SELECT coalesce(max("offset"), 0) FROM positions WHERE session = ? AND channel = ?`,
		b.session, deliveryChannel).Scan(&b.position)
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(context.Background(), `SELECT packet_id, moves FROM session_inflight WHERE session = ? AND channel = ?`,
		b.session, deliveryChannel)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	b.moves = map[int64]int64{}
	for rows.Next() {
		var id, moves int64
		if err := rows.Scan(&id, &moves); err != nil {
			return err
		}
		b.moves[id] = moves
	}
	return rows.Err()
}

// bookkeepingOf reads session's bookkeeping through the readers.
func bookkeepingOf(t *testing.T, r *running, session int64) *bookkeeping {
	t.Helper()
	b := &bookkeeping{session: session}
	if err := read(t, r, b); err != nil {
		t.Fatalf("reading session %d's delivery bookkeeping: %v", session, err)
	}
	return b
}

// design.md, Store, Writes: "Consecutive members changing the delivery
// bookkeeping of different sessions may be combined into shared statements;
// any other member, or a later member for the same session, keeps its place
// in member order." Delivery, Positions: "Saving progress past an unsettled
// exchange marks it `moves = 0` in the same transaction"; storage.md,
// session_inflight.moves: "1 while the row protects or may move the current
// position; 0 after a seek or saved progress supersedes it".
//
// Session 1 stands at offset 3 with exchanges at offsets 5 and 7, both
// moving. One group holds two of its delivery changes: the first saves
// progress to 10, which supersedes both exchanges' movement; the second, its
// departure, rewinds to 5 and restores both as live. In member order the
// restore comes after the supersede, so after commit the position is 5 and
// both exchanges move again.
func TestLaterMemberForSameSessionKeepsItsPlace(t *testing.T) {
	r := start(t, newConfig(t, 2))
	seed(t, r, positionRow(t, 1, 3), inflightRow(t, 1, 1, 5, 1), inflightRow(t, 1, 2, 7, 1))
	g := &core.Group{Writes: []store.Write{
		&store.DeliveryChanges{Session: 1, Channel: deliveryChannel, Position: 10, Seen: 1760000000001},
		&store.DeliveryChanges{Session: 1, Channel: deliveryChannel, Position: 5, Seen: 1760000000002, Rewind: true, Restore: []uint16{1, 2}},
	}}
	if err := write(t, r, g); err != nil {
		t.Fatalf("the group: %v", err)
	}
	b := bookkeepingOf(t, r, 1)
	if b.position != 5 {
		t.Errorf("after the rewind, the later member, session 1's position is %d, want 5", b.position)
	}
	if want := map[int64]int64{1: 1, 2: 1}; !maps.Equal(b.moves, want) {
		t.Errorf("session 1's exchanges' moves, by packet identifier, after the progress then the rewind restoring them: %v, want %v", b.moves, want)
	}
	stop(t, r)
}

// design.md, Store, Writes: "Consecutive members changing the delivery
// bookkeeping of different sessions may be combined into shared statements;
// any other member, or a later member for the same session, keeps its place
// in member order."
//
// One group holds session 1's delivery change, then session 2's SUBSCRIBE
// creating its position at offset 1, then session 2's delivery change moving
// that position to 4. The SUBSCRIBE sits between two delivery changes of
// different sessions, and keeps its place: after commit, session 2's
// position is the moved one, 4, as is session 1's.
func TestOtherMemberKeepsItsPlace(t *testing.T) {
	r := start(t, newConfig(t, 2))
	seed(t, r, positionRow(t, 1, 1))
	starts := []store.SubscriptionStart{{Channel: deliveryChannel, Start: 1}}
	g := &core.Group{Writes: []store.Write{
		&store.DeliveryChanges{Session: 1, Channel: deliveryChannel, Position: 4, Seen: 1760000000001},
		&store.SaveSubscriptions{Session: 2, Positions: starts, Updates: []store.SubscriptionUpdate{
			{New: true, Filter: "orders/#", QoS: 1, Starts: starts},
		}},
		&store.DeliveryChanges{Session: 2, Channel: deliveryChannel, Position: 4, Seen: 1760000000001},
	}}
	if err := write(t, r, g); err != nil {
		t.Fatalf("the group: %v", err)
	}
	for _, session := range []int64{1, 2} {
		if got := bookkeepingOf(t, r, session).position; got != 4 {
			t.Errorf("session %d's position after the group is %d, want 4, where its delivery change moved it", session, got)
		}
	}
	stop(t, r)
}

// limitPages is a test write of no record that holds the writer's
// connection to the pages its database has now (SQLite's max_page_count):
// a write needing one more page is answered "full", as on a full disk, with
// SQLITE_FULL.
type limitPages struct{}

func (*limitPages) Records() int { return 0 }

func (*limitPages) WriteTx(tx *store.Tx) error {
	var pages int
	if err := tx.QueryRowContext(context.Background(), "PRAGMA page_count").Scan(&pages); err != nil {
		return err
	}
	return tx.QueryRowContext(context.Background(), "PRAGMA max_page_count = "+strconv.Itoa(pages)).Scan(&pages)
}

// design.md, Store, A full disk: "a write is answered "full" only when
// SQLite reports lack of space and rollback confirms that the transaction
// did not commit"; Writes: "Consecutive members changing the delivery
// bookkeeping of different sessions may be combined into shared statements;
// any other member, or a later member for the same session, keeps its place
// in member order." Combining never refuses a group that fits in member
// order.
//
// Session 1 holds 64 exchanges of 1 KiB of delivery properties each, and
// the store is then held to the pages it has. Session 2's 48 new exchanges
// alone are answered "full": the store has no room for them. One group
// holding session 1's settlement of all 64, then session 2's 48, fits in
// member order, the settlement freeing the pages the exchanges need, and
// commits: session 1 holds no exchange and session 2 all 48.
func TestSettlementsMakeRoomForLaterInsertions(t *testing.T) {
	const settled, added = 64, 48
	r := start(t, newConfig(t, 2))
	rows := make([]tableRow, settled)
	settle := make([]uint16, settled)
	for i := range rows {
		rows[i] = inflightRow(t, 1, int64(i+1), int64(i+1), 1)
		rows[i].row["out_properties"] = kib
		settle[i] = uint16(i + 1)
	}
	seed(t, r, rows...)
	if err := write(t, r, &limitPages{}); err != nil {
		t.Fatalf("holding the store to its pages: %v", err)
	}
	// additions returns session 2's new exchanges, a new write each time: a
	// sent op is never reused.
	additions := func() *store.DeliveryChanges {
		d := &store.DeliveryChanges{Session: 2, Channel: deliveryChannel, Save: make([]store.SessionDelivery, added)}
		for i := range d.Save {
			d.Save[i] = store.SessionDelivery{PacketID: uint16(i + 1), Seq: int64(i + 1), Offset: int64(i + 1), QoS: 1, Properties: kib}
		}
		return d
	}
	if !isFull(t, write(t, r, additions())) {
		t.Fatal("session 2's exchanges alone are answered as stored: the store is not full, and the test proves nothing")
	}
	g := &core.Group{Writes: []store.Write{
		&store.DeliveryChanges{Session: 1, Channel: deliveryChannel, Delete: settle},
		additions(),
	}}
	if err := write(t, r, g); err != nil {
		t.Fatalf("the group settling session 1's exchanges, then adding session 2's, which fits in member order: %v", err)
	}
	for session, want := range map[int64]int{1: 0, 2: added} {
		if got := len(bookkeepingOf(t, r, session).moves); got != want {
			t.Errorf("after the group, session %d holds %d exchanges, want %d", session, got, want)
		}
	}
	stop(t, r)
}
