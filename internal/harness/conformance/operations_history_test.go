package conformance

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the operations histories, refused and position-lost
// (design.md, Operations, HTTP; Words: Missed). Each launches a broker of
// its own with one tcp operations listener on a loopback address and reads
// the histories with no credential, as an operator on the same machine
// does.

// lossKey is r's position-loss key: design.md, Operations, HTTP, "A
// position-loss key is reader and channel".
func lossKey(r *harness.LossRow) string { return r.Reader + " " + r.Channel }

// readHistory GETs b's history route and decodes it (harness.DecodeHistory),
// failing the test unless the answer is a 200 JSON object holding, under
// list, every row kept, each carrying every one of fields, beside tracked,
// beyond and returned.
func readHistory[R any](t *testing.T, b *harness.Broker, route, list string, fields []string) harness.History[R] {
	t.Helper()
	path := "/v1/operations/" + route
	a := opsRequest(t, b, http.MethodGet, path, "")
	if a.Status != http.StatusOK || a.MediaType != "application/json" {
		t.Fatalf("GET %s: %d %s %.200q; want 200 and a JSON object", path, a.Status, a.MediaType, a.Body)
	}
	h, err := harness.DecodeHistory[R]([]byte(a.Body), list, fields)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return h
}

// refusedHistory is b's refused history.
func refusedHistory(t *testing.T, b *harness.Broker) harness.History[harness.RefusedRow] {
	t.Helper()
	return readHistory[harness.RefusedRow](t, b, "refused", "clients", harness.RefusedFields)
}

// lossHistory is b's position-lost history.
func lossHistory(t *testing.T, b *harness.Broker) harness.History[harness.LossRow] {
	t.Helper()
	return readHistory[harness.LossRow](t, b, "position-lost", "readers", harness.LossFields)
}

// awaitRefused waits until b's refused history satisfies ok, and returns
// it; want says what is awaited.
func awaitRefused(t *testing.T, b *harness.Broker, want string, ok func(harness.History[harness.RefusedRow]) bool) harness.History[harness.RefusedRow] {
	t.Helper()
	return awaitState(t, want, func() harness.History[harness.RefusedRow] { return refusedHistory(t, b) }, ok)
}

// awaitLosses waits until b's position-lost history satisfies ok, and
// returns it; want says what is awaited.
func awaitLosses(t *testing.T, b *harness.Broker, want string, ok func(harness.History[harness.LossRow]) bool) harness.History[harness.LossRow] {
	t.Helper()
	return awaitState(t, want, func() harness.History[harness.LossRow] { return lossHistory(t, b) }, ok)
}

// lossOf is the row of reader and channel in rows, if any.
func lossOf(rows []harness.LossRow, reader, channel string) (harness.LossRow, bool) {
	i := slices.IndexFunc(rows, func(r harness.LossRow) bool { return r.Reader == reader && r.Channel == channel })
	if i < 0 {
		return harness.LossRow{}, false
	}
	return rows[i], true
}

// refusedOf is the row of client in rows, if any.
func refusedOf(rows []harness.RefusedRow, client string) (harness.RefusedRow, bool) {
	i := slices.IndexFunc(rows, func(r harness.RefusedRow) bool { return r.ClientID == client })
	if i < 0 {
		return harness.RefusedRow{}, false
	}
	return rows[i], true
}

// mqttReader is the reader of client id's consumer (design.md, Operations,
// HTTP, consumers: "reader (mqtt:<client id> or bridge:<name>)").
func mqttReader(id string) string { return "mqtt:" + id }

// connectRefused connects an MQTT 5 client, id, to addr with an
// Authentication Method, naming user as its user name where user is not
// empty, and returns an error unless the broker answers CONNACK reason then
// closes the connection (design.md, CONNECT's refusal table: "0x8C | an
// Authentication Method"; "0x85 | a client id over max_client_id_length",
// checked first: "It checks admission reasons in the order 0x85, [...]
// 0x8C"; "A failure gets a CONNACK with its reason, then the connection
// closes"). It returns errors rather than failing, so that many can run
// side by side (refuseAll).
func connectRefused(addr, id, user string, reason byte) error {
	c, err := harness.Dial(addr, harness.V5)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }() // refused or failed, it carries nothing more
	ack, err := c.Connect(&harness.Connect{ClientID: id, CleanStart: true, HasUsername: user != "", Username: user,
		Props: harness.Properties{{ID: harness.AuthenticationMethod, Str: "m"}}})
	if err != nil {
		return fmt.Errorf("connecting %q: %w", id, err)
	}
	if ack.Reason != reason {
		return fmt.Errorf("connecting %q: CONNACK 0x%02X; want 0x%02X", id, ack.Reason, reason)
	}
	if err := c.AwaitClose(); err != nil {
		return fmt.Errorf("connecting %q: %w", id, err)
	}
	return nil
}

// refuseAll has each of ids refused at CONNECT with reason
// (connectRefused), naming user, eight connections at a time, failing the
// test where any is not.
func refuseAll(t *testing.T, addr, user string, ids []string, reason byte) {
	t.Helper()
	errs := make([]error, len(ids))
	next := make(chan int)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range next {
				errs[i] = connectRefused(addr, ids[i], user, reason)
			}
		})
	}
	for i := range ids {
		next <- i
	}
	close(next)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
}

// laterSecond waits until the clock has passed the second of at. The
// histories compare last_seen, which is to the second (design.md,
// Operations, HTTP: "Times are RFC 3339 in UTC, to the second"): an event
// after this wait is seen in a later second than one before at.
func laterSecond(t *testing.T, at time.Time) {
	t.Helper()
	if !testkit.Within(2*time.Second, func() bool { return time.Now().Unix() > at.Unix() }) {
		t.Fatal("the clock has not passed the next second within 2s")
	}
}

// publishWindowed sends ps from c at QoS 1, leaving at most window of them
// unanswered, and waits for the PUBACK 0x00 of each: design.md, PUBLISH,
// "A reader counts its client's unanswered QoS 1 and 2 PUBLISHes against
// qos2.max_inflight_per_client, the value its CONNACK carries", beyond
// which "the connection ends with DISCONNECT 0x93".
func publishWindowed(t *testing.T, c *harness.Client, ps []*harness.Publish, window int) {
	t.Helper()
	for i, p := range ps {
		if i >= window {
			support.Puback(t, c, ps[i-window])
		}
		support.Sent(t, c, p)
	}
	for _, p := range ps[max(0, len(ps)-window):] {
		support.Puback(t, c, p)
	}
}

// publisher connects an MQTT 5 client of its own, id, to b, and returns it
// with its CONNACK's Receive Maximum, the most QoS 1 PUBLISHes it may leave
// unanswered (MQTT 5 3.2.2.3.3: absent, it is 65,535).
func publisher(t *testing.T, b *harness.Broker, id string) (c *harness.Client, window int) {
	t.Helper()
	c, ack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true})
	window = 65_535
	if p, ok := ack.Props.Get(harness.ReceiveMaximum); ok {
		window = int(p.Int)
	}
	return c, window
}

// storedPosition reads from db the stored position of client's durable
// session on channel, one row of storage.md's positions: its offset, "the
// next offset for new delivery", and its last_seen, "nullable: when it last
// moved; NULL for never", 0 for never.
func storedPosition(t *testing.T, db support.RowsQuerier, client, channel string) (offset, lastSeen int64) {
	t.Helper()
	var seen sql.NullInt64
	found := false
	support.Query(t, db, `SELECT p."offset", p.last_seen FROM positions p JOIN sessions s ON s.id = p.session
		JOIN channels c ON c.id = p.channel WHERE s.client = ? AND c.name = ?`, func(rows *sql.Rows) error {
		found = true
		return rows.Scan(&offset, &seen)
	}, client, channel)
	if !found {
		t.Fatalf("the database holds no position of %q on %s", client, channel)
	}
	return offset, seen.Int64
}

// lagging connects an MQTT 5 client, id, to b with Receive Maximum 1, and
// subscribes it at QoS 1 to filter: one delivery it leaves unacknowledged
// fills its window (design.md, Delivery: "The window is the lower of the
// client's Receive Maximum and limits.max_inflight"), so what follows stays
// owed to it.
func lagging(t *testing.T, b *harness.Broker, id, filter string) *harness.Client {
	t.Helper()
	return support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: receiveMaximum5(1)}, support.At(1, filter))
}

// design.md, Operations, HTTP: "refused lists, under clients, connections
// refused at CONNECT or ended by the broker with a reason, with one row per
// client id. Each row has client_id (empty before a CONNECT), user, reason,
// count and last_seen. The user is the user name the CONNECT named, proved
// or not, or empty without one. [...] The reason is the latest, named as in
// metrics.md. The most refused come first."; "Equal sort ranks use the
// complete row key in UTF-8 byte order"; "Times are RFC 3339 in UTC, to the
// second".
// metrics.md, Rules: "the reason label is its MQTT 5 reason name in lower
// case, with each space or hyphen replaced by an underscore".
//
// With a password file holding alice, client repeat names alice with a
// wrong password twice, refused 0x86, then connects with the right one and
// sends a malformed PINGREQ, ended with DISCONNECT 0x81. Client once names
// alice with a wrong password once. A connection sends a PINGREQ before any
// CONNECT, closed with no CONNACK. The history has three rows: repeat,
// counted 3, its reason malformed_packet, its user alice; then, each
// counted once, the row of no client id, protocol_error, its user empty,
// and once, bad_user_name_or_password, its user alice, named though never
// proved.
func TestRefusedHistoryKeepsTheLatestReasonPerClient(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := passwordFile(t, dir, "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.OpsLaunch(t, &harness.Options{Login: harness.Login{PasswordFile: file}})
	from := time.Now()
	for range 2 {
		connackRefuses(t, b.Addr, harness.V5, credentials("repeat", "alice", "wrong"), 0x86)
	}
	c, _ := harness.Connected(t, b.Addr, harness.V5, credentials("repeat", "alice", "alice-pw"))
	harness.SendBytes(t, c, []byte{0xC0, 0x01, 0x00})
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x81 {
		t.Fatalf("DISCONNECT 0x%02X to a PINGREQ with a body; want 0x81 (Malformed Packet)", d.Reason)
	}
	harness.ExpectClosed(t, c)
	connackRefuses(t, b.Addr, harness.V5, credentials("once", "alice", "wrong"), 0x86)
	early, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = early.Close() }) // the test is over
	harness.SendBytes(t, early, pingreq)
	harness.ExpectClosed(t, early)

	h := awaitRefused(t, b, "three rows, five events", func(h harness.History[harness.RefusedRow]) bool {
		n := int64(0)
		for _, r := range h.Rows {
			n += r.Count
		}
		return len(h.Rows) == 3 && n == 5
	})
	to := time.Now()
	want := []struct {
		row  harness.RefusedRow
		user string
	}{
		{harness.RefusedRow{ClientID: "repeat", Reason: "malformed_packet", Count: 3}, "alice"},
		{harness.RefusedRow{ClientID: "", Reason: "protocol_error", Count: 1}, ""},
		{harness.RefusedRow{ClientID: "once", Reason: "bad_user_name_or_password", Count: 1}, "alice"},
	}
	for i, w := range want {
		r := h.Rows[i]
		if r.ClientID != w.row.ClientID || r.Reason != w.row.Reason || r.Count != w.row.Count {
			t.Errorf("row %d: %q, %s, counted %d; want %q, %s, counted %d", i, r.ClientID, r.Reason, r.Count, w.row.ClientID, w.row.Reason, w.row.Count)
		}
		switch {
		case r.User == nil:
			t.Errorf("row %q: user null; want %q, the user name its CONNECT named, proved or not, or empty without one", r.ClientID, w.user)
		case *r.User != w.user:
			t.Errorf("row %q: user %q; want %q, the user name its CONNECT named, proved or not, or empty without one", r.ClientID, *r.User, w.user)
		}
		expectUTCSecond(t, fmt.Sprintf("row %q's last_seen", r.ClientID), &r.LastSeen, from, to)
	}
	if h.Tracked != 3 || h.Beyond != 0 {
		t.Errorf("tracked %d, beyond %d; want 3 rows kept, nothing beyond", h.Tracked, h.Beyond)
	}
}

// seriesSum is the sum of e's samples of the metric name, those whose
// reason label is one of except left out.
func seriesSum(e testkit.Exposition, name string, except ...string) float64 {
	sum := 0.0
	for _, s := range e.Samples {
		if s.Name == name && !slices.Contains(except, s.Labels["reason"]) {
			sum += s.Value
		}
	}
	return sum
}

// design.md, Operations, HTTP: "refused lists, under clients, connections
// refused at CONNECT or ended by the broker with a reason"; "The beyond counts events
// no longer represented in retained rows". metrics.md,
// saguin_connections_refused_total: "The refused history records that same
// closure event"; saguin_disconnects_total, "the reason Sagüin selected for
// a closure [...] or client (the client's DISCONNECT), connection_lost".
// 0020i, from Fable's sweep of metrics.md: "test when quiet:
// saguin_connections_refused_total plus saguin_disconnects_total without
// client and connection_lost equals the refused list's counts plus beyond".
//
// Connections are refused with 0x8C and 0x85, one sends a PINGREQ before
// any CONNECT, one is ended with DISCONNECT 0x81 and one taken over; two
// clients leave with DISCONNECT and two drop their connections. Once
// /metrics counts two more client and connection_lost closures, the
// refused series and the disconnects but those two reasons sum to the
// history's counts plus beyond.
func TestRefusedHistoryAgreesWithTheSeries(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{})
	excluded := []string{disconnectSeries("client"), disconnectSeries("connection_lost")}
	base := support.Sampled(t, b, excluded...)
	if err := connectRefused(b.Addr, "agree-method", "", 0x8C); err != nil {
		t.Fatal(err)
	}
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{}, 0x85)
	early, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = early.Close() }) // the test is over
	harness.SendBytes(t, early, pingreq)
	harness.ExpectClosed(t, early)
	malformed, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "agree-malformed", CleanStart: true})
	harness.SendBytes(t, malformed, []byte{0xC0, 0x01, 0x00})
	if d := harness.Expect[*harness.Disconnect](t, malformed); d.Reason != 0x81 {
		t.Fatalf("DISCONNECT 0x%02X to a PINGREQ with a body; want 0x81", d.Reason)
	}
	taken, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "agree-taken", CleanStart: true})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "agree-taken", CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, taken); d.Reason != 0x8E {
		t.Fatalf("DISCONNECT 0x%02X to the connection taken over; want 0x8E", d.Reason)
	}
	for i := range 2 {
		leaver, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: fmt.Sprint("agree-leaver-", i), CleanStart: true})
		support.Disconnected(t, leaver, &harness.Disconnect{})
		lost, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: fmt.Sprint("agree-lost-", i), CleanStart: true})
		support.DropSocket(t, lost)
	}
	awaitSeries(t, b, map[string]float64{excluded[0]: base[excluded[0]] + 2, excluded[1]: base[excluded[1]] + 2})

	var series, rows float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		h := refusedHistory(t, b)
		e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
		series = seriesSum(e, "saguin_connections_refused_total") + seriesSum(e, "saguin_disconnects_total", "client", "connection_lost")
		rows = float64(h.Beyond)
		for _, r := range h.Rows {
			rows += float64(r.Count)
		}
		return series > 0 && series == rows
	}) {
		t.Fatalf("the refused and disconnect series, but client and connection_lost, sum to %v; the refused history's counts plus beyond to %v; want them equal", series, rows)
	}
}

// design.md, Operations, HTTP: "Both histories keep up to 1,000 rows in
// memory and are emptied by a restart. Each returns every row it keeps.
// They include tracked, the number of rows kept, and beyond. A full refused history replaces a row counted once, choosing the oldest
// last_seen, then the smallest key. If none qualifies, the new event is not
// retained."; "The beyond counts events no longer represented in retained
// rows, including an evicted row's accumulated event count".
//
// ~twice is refused twice and ~once once; in a later second, 998 more
// client ids are refused once each: 1,000 rows. new-1 is refused: it
// replaces ~once, counted once and the oldest, not ~twice, older still but
// counted twice; beyond is 1. Every row counted once is refused again; then
// new-2 is refused: no row qualifies, so the rows stay as they were and
// beyond is 2.
func TestFullRefusedHistoryReplacesARowCountedOnce(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{MaxConnectRate: harness.NoConnectRate})
	refuseAll(t, b.Addr, "", []string{"~twice", "~twice", "~once"}, 0x8C)
	awaitRefused(t, b, "~twice and ~once", func(h harness.History[harness.RefusedRow]) bool { return h.Tracked == 2 })
	laterSecond(t, time.Now())
	fill := make([]string, 998)
	for i := range fill {
		fill[i] = fmt.Sprintf("r%04d", i)
	}
	refuseAll(t, b.Addr, "", fill, 0x8C)
	awaitRefused(t, b, "1,000 rows", func(h harness.History[harness.RefusedRow]) bool { return h.Tracked == 1000 && len(h.Rows) == 1000 })

	refuseAll(t, b.Addr, "", []string{"new-1"}, 0x8C)
	h := awaitRefused(t, b, "a row for new-1", func(h harness.History[harness.RefusedRow]) bool {
		_, ok := refusedOf(h.Rows, "new-1")
		return ok
	})
	if _, ok := refusedOf(h.Rows, "~once"); ok || h.Tracked != 1000 || h.Beyond != 1 {
		t.Errorf("after new-1: ~once kept %v, tracked %d, beyond %d; want ~once replaced, 1,000 rows, beyond 1", ok, h.Tracked, h.Beyond)
	}
	if r, ok := refusedOf(h.Rows, "~twice"); !ok || r.Count != 2 {
		t.Errorf("after new-1: ~twice %+v (kept %v); want it kept, counted 2: only a row counted once is replaced", r, ok)
	}

	var once []string
	for _, r := range h.Rows {
		if r.Count == 1 {
			once = append(once, r.ClientID)
		}
	}
	refuseAll(t, b.Addr, "", once, 0x8C)
	before := awaitRefused(t, b, "every row counted twice", func(h harness.History[harness.RefusedRow]) bool {
		return !slices.ContainsFunc(h.Rows, func(r harness.RefusedRow) bool { return r.Count < 2 })
	})
	refuseAll(t, b.Addr, "", []string{"new-2"}, 0x8C)
	after := awaitRefused(t, b, "beyond 2", func(h harness.History[harness.RefusedRow]) bool { return h.Beyond == 2 })
	byClient := func(a, b harness.RefusedRow) int { return strings.Compare(a.ClientID, b.ClientID) }
	slices.SortFunc(before.Rows, byClient)
	slices.SortFunc(after.Rows, byClient)
	if after.Tracked != 1000 || !slices.EqualFunc(before.Rows, after.Rows, func(a, b harness.RefusedRow) bool {
		return a.ClientID == b.ClientID && a.Count == b.Count
	}) {
		t.Errorf("after new-2 with no row counted once: tracked %d, the rows changed; want new-2 not retained and the 1,000 rows as they were", after.Tracked)
	}
}

// design.md, Operations, HTTP: refused, "A client id refused for exceeding
// max_client_id_length is kept, and keys its row, as its display prefix";
// "Display truncation of a client id or user name keeps the longest
// complete UTF-8 prefix of at most 1,024 bytes"; CONNECT's refusal table:
// "0x85 | a client id over max_client_id_length". config.md,
// max_client_id_length: 256 by default. metrics.md,
// saguin_connections_refused_total: client_identifier_not_valid.
//
// Two client ids of 1,026 bytes, over max_client_id_length, share their
// first 1,024 bytes and differ in their last two; each is refused with
// CONNACK 0x85. The history has one row, keyed by those 1,024 bytes,
// client_identifier_not_valid, counted 2.
func TestRefusedHistoryKeysAnOverlongClientIDByItsPrefix(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{})
	prefix := strings.Repeat("p", 1024)
	for _, id := range []string{prefix + "-a", prefix + "-b"} {
		connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}, 0x85)
	}
	h := awaitRefused(t, b, "the two refusals counted", func(h harness.History[harness.RefusedRow]) bool {
		n := int64(0)
		for _, r := range h.Rows {
			n += r.Count
		}
		return n == 2
	})
	if r, ok := refusedOf(h.Rows, prefix); !ok || len(h.Rows) != 1 || r.Reason != "client_identifier_not_valid" || r.Count != 2 {
		got := make([]string, len(h.Rows))
		for i, r := range h.Rows {
			got[i] = fmt.Sprintf("a %d-byte client id, %s, counted %d", len(r.ClientID), r.Reason, r.Count)
		}
		t.Errorf("the refused history holds %v; want one row, the 1,024 bytes the two ids share, client_identifier_not_valid, counted 2", got)
	}
}

// design.md, Operations, HTTP: refused, "The user is the user name the
// CONNECT named, proved or not, or empty without one. The user name is kept
// as its display prefix. A client id refused for exceeding
// max_client_id_length is kept, and keys its row, as its display prefix";
// "with one row per client id. Each row has `client_id` (empty before a
// CONNECT), `user`, `reason`, `count` and `last_seen`"; "Display truncation
// of a client id or user name keeps the longest complete UTF-8 prefix of at
// most 1,024 bytes"; "Both histories keep up to 1,000 rows in memory and
// are emptied by a restart". CONNECT's refusal table: "0x85 | a client id
// over max_client_id_length". metrics.md, Go runtime:
// saguin_go_heap_live_bytes, "heap the last collection found reachable";
// saguin_go_gc_cycles_total, "collections completed".
//
// 1,000 MQTT 5 CONNECTs, each with its own client id of 39,005 bytes (five
// digits, then 13,000 three-byte characters), all naming one user name of
// 40,001 bytes (one byte, then 20,000 two-byte characters), are refused
// with 0x85, the package's broker having config.md's max_client_id_length,
// 256. Each prefix cut falls inside a character, so the rows hold 1,000
// client ids each shown, and keyed, as its first 1,022 bytes, and each
// user as the user name's first 1,023, each row counted once. The same
// 1,000 client ids are then refused again, each CONNECT naming another
// user name of 40,001 bytes: each row, the one its client id keys, is
// counted 2, its user the first 1,023 bytes of a user name its CONNECTs
// named (the design names the latest reason, not the latest user). After
// each round, the broker's live heap, read after
// collections a publisher brings on (collected), has grown from before the
// refusals by no more than 1,000 rows of two 1,024-byte values, the most a
// row's user and client id can hold, with as much again for the rows'
// other fields, their index and the heap's noise between the two readings:
// 4,096,000 bytes. Whole client ids and user names would be 79 MB.
func TestRefusedHistoryKeepsDisplayPrefixes(t *testing.T) {
	t.Parallel()
	const rows, most = 1000, 1024
	b := support.OpsLaunch(t, &harness.Options{MaxConnectRate: harness.NoConnectRate, StorageMode: "normal"})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "heap-publisher", CleanStart: true})
	before := collected(t, b, pub)
	ids := make([]string, rows)
	for i := range ids {
		ids[i] = fmt.Sprintf("%05d", i) + strings.Repeat("€", 13_000)
	}
	named := map[string]bool{} // the display prefixes of the user names sent so far
	for round, user := range []string{"u" + strings.Repeat("é", 20_000), "v" + strings.Repeat("ñ", 20_000)} {
		count := int64(round + 1)
		named[user[:1023]] = true
		refuseAll(t, b.Addr, user, ids, 0x85)
		h := awaitRefused(t, b, fmt.Sprintf("1,000 rows, each counted %d", count), func(h harness.History[harness.RefusedRow]) bool {
			return h.Tracked == rows && len(h.Rows) == rows && !slices.ContainsFunc(h.Rows, func(r harness.RefusedRow) bool { return r.Count != count })
		})
		shown := make(map[string]bool, rows) // each id's display prefix
		for _, id := range ids {
			shown[id[:1022]] = true
		}
		for _, r := range h.Rows {
			if !shown[r.ClientID] || r.User == nil || !named[*r.User] {
				t.Errorf("refusal %d of each id: a row of a %d-byte client id, its user a sent user name's first 1,023 bytes %v: want a client id's first 1,022 bytes and the first 1,023 of a user name its CONNECTs named, each its display prefix",
					count, len(r.ClientID), r.User != nil && named[*r.User])
				break
			}
			delete(shown, r.ClientID)
		}
		after := collected(t, b, pub)
		if grown, bound := after-before, float64(2*2*rows*most); grown > bound {
			t.Errorf("refusal %d of 1,000 39,005-byte client ids, each with a 40,001-byte user name: the live heap grew by %.0f bytes, from %.0f to %.0f; want at most %.0f, each kept as its display prefix of at most 1,024 bytes", count, grown, before, after, bound)
		}
	}
}

// heapProbe is the payload collected publishes: a few hundred publications
// bring on a collection, and the one in transit when it marks is small
// beside a history's bound.
const heapProbe = 64 << 10

// collected has c, a publisher connected to b, publish heapProbe bytes at
// QoS 1, one at a time, until b has completed two collections more than
// when it began, and returns saguin_go_heap_live_bytes then: the second
// began after the first read, so the heap it found reachable is b's state
// since, beside one publication in transit. harness.DefaultTimeout is the
// watchdog.
func collected(t *testing.T, b *harness.Broker, c *harness.Client) float64 {
	t.Helper()
	const cycles, live = "saguin_go_gc_cycles_total{}", "saguin_go_heap_live_bytes{}"
	probe := bytes.Repeat([]byte{'h'}, heapProbe)
	start := support.Sampled(t, b, cycles)[cycles]
	for deadline := time.Now().Add(harness.DefaultTimeout); ; {
		got := support.Sampled(t, b, cycles, live)
		if got[cycles] >= start+2 {
			return got[live]
		}
		if time.Now().After(deadline) {
			t.Fatalf("watchdog: %v collections after %v of publishing; want 2", got[cycles]-start, harness.DefaultTimeout)
		}
		for range 16 {
			support.PublishAll(t, c, []*harness.Publish{{QoS: 1, Topic: "heap/probe", Payload: probe}})
		}
	}
}

// design.md, Words: Missed, "In the retention write, the core names its
// connected consumers and bridge rules [...] Connected consumers use their
// progress held by the core"; "Deliveries already settled or handed to a
// writer are excluded"; "The successful deletion answer carries [...]
// exact counts for the connected consumers and bridges represented at that
// boundary. The core applies these counts once." Operations, HTTP:
// "position-lost lists, under readers, readers retention passed, with one
// row per reader and channel. [...] The kind is consumer when passed while its client was
// connected"; "The count counts recorded observations. The records_missed
// is the sum of exact counts". metrics.md, saguin_missed_total:
// "retention (the channel's total from each deletion's answer".
//
// An append channel, events, keeps 470 bytes. A client with Receive
// Maximum 1 subscribes at QoS 1 to events/# and holds e1 unacknowledged; a
// publisher stores e2, e3 and e4, 100 bytes each, then a record of 400:
// one deletion passes e1, in flight, and e2 to e4, owed. The client's row
// is a consumer row counted once, three records missed, its last floor the
// last record's offset, and saguin_missed_total reads 3. The client then
// acknowledges e1 and receives the last record: both stay as they were.
func TestPositionLostCountsAPassedConsumer(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 470},
	}})
	db := support.OpenDB(t, b.DB)
	c := lagging(t, b, "passed", "events/#")
	pub, _ := publisher(t, b, "passed-pub")
	e1 := support.Sized("events/e1", testkit.RecordBytes)
	support.Acknowledged(t, pub, e1)
	inFlight := support.DeliveredAt(t, c, 1, e1.Topic, e1.Payload)
	last := support.Sized("events/last", 4*testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized("events/e2", testkit.RecordBytes), support.Sized("events/e3", testkit.RecordBytes),
		support.Sized("events/e4", testkit.RecordBytes), last,
	})
	floor := awaitHeld(t, db, "events", []string{last.Topic}, "once the last record is stored")[0].offset
	committed(t, pub, "elsewhere/barrier")

	check := func(when string) {
		t.Helper()
		h := awaitLosses(t, b, "a row for "+mqttReader("passed"), func(h harness.History[harness.LossRow]) bool { return h.Tracked == 1 })
		r, _ := lossOf(h.Rows, mqttReader("passed"), "events")
		if r.Kind != "consumer" || r.Count != 1 || r.Missed() != "3" || r.LastFloor != floor {
			t.Errorf("%s: %v; want a consumer row counted once, 3 missed, last floor %d", when, r, floor)
		}
		expectSample(t, b, testkit.MissedSeries("events"), 3, when)
	}
	check("once retention has passed the consumer")
	acknowledge(t, c, inFlight.ID, 0)
	support.DeliveredAt(t, c, 1, last.Topic, last.Payload)
	committed(t, pub, "elsewhere/barrier")
	check("once the consumer has moved past the records it missed")
}

// design.md, Words: Missed, "Away sessions use their stored state"; "On
// return, an away session whose position is below the floor adds a history
// observation with last_position and last_floor. It adds no reconstructed
// loss count. That observation does not increment the missed counter
// again". Operations, HTTP, position-lost: "session when passed while
// away"; "The records_missed is the sum of exact counts, or null once any
// observation represented by that row has an unknown count. Null means
// unknown, not zero." storage.md, positions: offset, "the next offset for
// new delivery".
//
// An append channel, events, keeps 470 bytes. A client holding a durable
// session subscribes at QoS 1 to events/#, receives and acknowledges e1,
// and leaves. A publisher stores e2 and e3, 100 bytes each, then a record
// of 400: retention deletes e1 to e3, and saguin_missed_total reads 2, the
// session's stored state owing e2 and e3. The client returns: its row is a
// session row counted once, records_missed null, its last position the
// stored one, its last floor the last record's offset; the counter still
// reads 2.
func TestPositionLostNotesAnAwaySessionsReturn(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 470},
	}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "returner", CleanStart: true, Props: support.Durable}
	c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "events/#"))
	pub, _ := publisher(t, b, "returner-pub")
	e1 := support.Sized("events/e1", testkit.RecordBytes)
	support.Acknowledged(t, pub, e1)
	acknowledge(t, c, support.DeliveredAt(t, c, 1, e1.Topic, e1.Payload).ID, 0)
	support.Away(t, c)
	support.AwaitAway(t, db, connect.ClientID)
	last := support.Sized("events/last", 4*testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized("events/e2", testkit.RecordBytes), support.Sized("events/e3", testkit.RecordBytes), last,
	})
	floor := awaitHeld(t, db, "events", []string{last.Topic}, "once the last record is stored")[0].offset
	committed(t, pub, "elsewhere/barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 2, "once retention has passed the away session")
	position, _ := storedPosition(t, db, connect.ClientID, "events")

	c = resumed(t, b.Addr, harness.V5, connect)
	support.DeliveredAt(t, c, 1, last.Topic, last.Payload)
	h := awaitLosses(t, b, "a row for "+mqttReader("returner"), func(h harness.History[harness.LossRow]) bool { return h.Tracked == 1 })
	r, _ := lossOf(h.Rows, mqttReader("returner"), "events")
	if r.Kind != "session" || r.Count != 1 || r.RecordsMissed != nil || r.LastPosition != position || r.LastFloor != floor {
		t.Errorf("after the session's return: %v; want a session row counted once, missed null, last position %d, last floor %d", r, position, floor)
	}
	expectSample(t, b, testkit.MissedSeries("events"), 2, "once the session has returned below the floor")
}

// design.md, Words: Missed, "When a session returns, a takeover included,
// or a subscription reaches a position it kept, and that position is below
// the floor, the core adds a history observation with last_position and
// last_floor, moves the position to the floor and updates its last_seen. It
// adds no reconstructed loss count. That observation does not increment
// the missed counter again"; "A loss counts records owed under the
// consumer's filtering"; "An away session's losses are counted from its
// stored subscriptions alone"; Flows, SUBSCRIBE, UNSUBSCRIBE: "When the
// last subscription reaching an append channel or a queue is removed, a
// durable session keeps its position there"; "Without a subscription, the
// position moves only by a seek, or to the floor"; Delivery, Positions: "A
// position is saved with the next commit". Operations, HTTP, position-lost:
// "The kind is [...] session when passed while away, or found below the
// floor when a subscription reaches a kept position"; "The count counts
// recorded observations. The records_missed is the sum of exact counts, or
// null once any observation represented by that row has an unknown count".
// storage.md, positions: last_seen, "when it last moved".
//
// An append channel, events, keeps 470 bytes. A client holding a durable
// session subscribes at QoS 1 to events/#, receives and acknowledges e1,
// and unsubscribes, the last subscription reaching events: its kept
// position is saved. It stays connected, or in the second case leaves. A
// publisher stores e2 and e3, 100 bytes each, then a record of 400:
// retention deletes e1 to e3, passing the kept position, and
// saguin_missed_total reads 0, nothing being owed through no subscription.
// Then the client subscribes to events/# again and receives the last
// record; or it resumes; or, in the third case, a second connection
// resumes the session, taking the first over. The history holds one
// session row for it, counted once, records_missed null, its last position
// the kept one, its last floor the last record's offset; once the client's
// own publication is acknowledged, the stored position is at the floor and
// its last_seen later than before. The client leaves and resumes again:
// the row is still counted once, and saguin_missed_total still reads 0.
func TestPositionLostNotesAKeptPositionBelowTheFloor(t *testing.T) {
	for _, how := range []string{"SUBSCRIBE", "reconnect", "takeover"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
				{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 470},
			}})
			db := support.OpenDB(t, b.DB)
			connect := &harness.Connect{ClientID: "keeper", CleanStart: true, Props: support.Durable}
			c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "events/#"))
			pub, _ := publisher(t, b, "keeper-pub")
			e1 := support.Sized("events/e1", testkit.RecordBytes)
			support.Acknowledged(t, pub, e1)
			acknowledge(t, c, support.DeliveredAt(t, c, 1, e1.Topic, e1.Payload).ID, 0)
			unsuback(t, c, nil, []string{"events/#"}, 0x00)
			position, seen := storedPosition(t, db, connect.ClientID, "events")
			if how == "reconnect" {
				support.Away(t, c)
				support.AwaitAway(t, db, connect.ClientID)
			}
			last := support.Sized("events/last", 4*testkit.RecordBytes)
			support.PublishAll(t, pub, []*harness.Publish{
				support.Sized("events/e2", testkit.RecordBytes), support.Sized("events/e3", testkit.RecordBytes), last,
			})
			floor := awaitHeld(t, db, "events", []string{last.Topic}, "once the last record is stored")[0].offset
			committed(t, pub, "elsewhere/barrier")
			expectSample(t, b, testkit.MissedSeries("events"), 0, "once retention has passed the kept position, which no subscription reaches")

			if how == "SUBSCRIBE" {
				support.Suback(t, c, nil, support.At(1, "events/#"), 0x01)
				support.DeliveredAt(t, c, 1, last.Topic, last.Payload)
			} else {
				c = resumed(t, b.Addr, harness.V5, connect)
			}
			h := awaitLosses(t, b, "a row for "+mqttReader("keeper"), func(h harness.History[harness.LossRow]) bool { return h.Tracked == 1 })
			r, _ := lossOf(h.Rows, mqttReader("keeper"), "events")
			if r.Kind != "session" || r.Count != 1 || r.RecordsMissed != nil || r.LastPosition != position || r.LastFloor != floor {
				t.Errorf("after the %s: %v; want a session row counted once, missed null, last position %d, last floor %d", how, r, position, floor)
			}
			committed(t, c, "elsewhere/barrier")
			var moved int64
			awaitState(t, fmt.Sprintf("the stored position of keeper on events at the floor, %d", floor),
				func() (p int64) { p, moved = storedPosition(t, db, connect.ClientID, "events"); return p }, func(p int64) bool { return p == floor })
			if moved <= seen {
				t.Errorf("after the %s, the position's last_seen is %d; want it later than %d, when the UNSUBSCRIBE kept it: the move to the floor updates it", how, moved, seen)
			}

			support.Away(t, c)
			support.AwaitAway(t, db, connect.ClientID)
			c = resumed(t, b.Addr, harness.V5, connect)
			committed(t, c, "elsewhere/barrier")
			if r, _ := lossOf(lossHistory(t, b).Rows, mqttReader("keeper"), "events"); r.Count != 1 {
				t.Errorf("after a further resume at the floor: %v; want the row still counted once", r)
			}
			expectSample(t, b, testkit.MissedSeries("events"), 0, "once the kept position has been found below the floor")
		})
	}
}

// design.md, Words: Missed, "In the retention write, the core names its
// connected consumers and bridge rules"; "exact counts for the connected
// consumers and bridges represented at that boundary". Operations, HTTP,
// position-lost: "one row per reader and channel"; "bridge for an out
// rule"; consumers: "reader (mqtt:<client id> or bridge:<name>)". 0020i,
// planned (maintainer, 261105): "Several rules of one bridge passed on one
// channel in one deletion make one observation, their records added".
// Bridges, Out: "Across the whole connection, at most the lower of the
// peer's Receive Maximum and receive_maximum are in flight".
//
// An append channel, events, keeps 470 bytes; a bridge, lagging, has two
// out rules on it, events/a/# and events/b/#, and its peer's CONNACK allows
// Receive Maximum 1. A client stores a1, which reaches the peer and waits
// for its PUBACK, then b1, a2 and b2, 100 bytes each, then a record of 400
// on events/c: one deletion passes a1, in flight, a2, owed to the first
// rule, and b1 and b2, owed to the second. The history holds one bridge row
// for bridge:lagging on events, counted once, three records missed, and
// saguin_missed_total reads 3.
func TestPositionLostMakesOneObservationPerBridgeChannel(t *testing.T) {
	t.Parallel()
	b, _, l := support.Outbound(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 470},
		elsewhere,
	}}, "lagging", support.QuietWith(false, harness.Property{ID: harness.ReceiveMaximum, Int: 1}),
		support.Out("events/a/#", "x/a/$#", 1), support.Out("events/b/#", "x/b/$#", 1))
	support.AwaitReady(t, b)
	db := support.OpenDB(t, b.DB)
	pub, _ := publisher(t, b, "lagging-pub")
	a1 := support.Sized("events/a/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, a1)
	first, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	support.Crossed(t, first, "x/a/1", 1, a1.Payload)
	last := support.Sized("events/c/last", 4*testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized("events/b/1", testkit.RecordBytes), support.Sized("events/a/2", testkit.RecordBytes),
		support.Sized("events/b/2", testkit.RecordBytes), last,
	})
	floor := awaitHeld(t, db, "events", []string{last.Topic}, "once the last record is stored")[0].offset
	committed(t, pub, "elsewhere/barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 3, "once retention has passed both rules")
	h := awaitLosses(t, b, "a row for bridge:lagging", func(h harness.History[harness.LossRow]) bool { return h.Tracked > 0 })
	r, _ := lossOf(h.Rows, "bridge:lagging", "events")
	if h.Tracked != 1 || r.Kind != "bridge" || r.Count != 1 || r.Missed() != "3" || r.LastFloor != floor {
		t.Errorf("tracked %d, the bridge's row %v; want one bridge row, counted once, 3 missed, last floor %d", h.Tracked, r, floor)
	}
}

// design.md, Operations, HTTP, position-lost: "The kind is the latest
// observation's."; "The records_missed is the sum of exact counts, or null
// once any observation represented by that row has an unknown count";
// "Counted rows sort by decreasing missed count. Unknown rows follow, by
// decreasing count."; Words: Missed, "On return, an away session whose
// position is below the floor adds a history observation".
//
// An append channel, events, keeps 470 bytes. Clients with Receive Maximum
// 1 subscribe at QoS 1: big, and mixed, durable, to events/#, small to
// events/a/#; dozer, durable, subscribes to events/# and leaves. Each holds
// a1; a2, b1 and b2, then a record of 400, x1, make one deletion: big and
// mixed miss 3, small 1. big leaves; mixed and small acknowledge a1, mixed
// receives and acknowledges x1 and leaves. small holds a3, of 30 bytes;
// a4, of 30, then y1, of 450, make another deletion: small misses 1 more.
// mixed and dozer return below the floor. The rows come: big (3 missed,
// counted once), small (2 missed, counted twice), then mixed (unknown,
// counted twice, its kind now session) and dozer (unknown, counted once).
func TestPositionLostSortsCountedRowsFirst(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 470},
	}})
	db := support.OpenDB(t, b.DB)
	dozer := &harness.Connect{ClientID: "dozer", CleanStart: true, Props: support.Durable}
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, dozer, support.At(1, "events/#")))
	big := lagging(t, b, "big", "events/#")
	small := lagging(t, b, "small", "events/a/#")
	mixedConnect := &harness.Connect{ClientID: "mixed", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	mixed := support.Subscribed(t, b.Addr, harness.V5, mixedConnect, support.At(1, "events/#"))
	pub, _ := publisher(t, b, "sorted-pub")

	a1 := support.Sized("events/a/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, a1)
	inFlight := map[*harness.Client]uint16{}
	for _, c := range []*harness.Client{big, small, mixed} {
		inFlight[c] = support.DeliveredAt(t, c, 1, a1.Topic, a1.Payload).ID
	}
	x1 := support.Sized("events/c/x1", 4*testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized("events/a/2", testkit.RecordBytes), support.Sized("events/b/1", testkit.RecordBytes),
		support.Sized("events/b/2", testkit.RecordBytes), x1,
	})
	awaitHeld(t, db, "events", []string{x1.Topic}, "once x1 is stored")
	committed(t, pub, "elsewhere/barrier")
	awaitLosses(t, b, "rows for big, small and mixed", func(h harness.History[harness.LossRow]) bool { return h.Tracked == 3 })
	support.Disconnected(t, big, &harness.Disconnect{})
	acknowledge(t, small, inFlight[small], 0)
	acknowledge(t, mixed, inFlight[mixed], 0)
	acknowledge(t, mixed, support.DeliveredAt(t, mixed, 1, x1.Topic, x1.Payload).ID, 0)
	support.Away(t, mixed)

	a3 := support.Sized("events/a/3", 30)
	support.Acknowledged(t, pub, a3)
	support.DeliveredAt(t, small, 1, a3.Topic, a3.Payload)
	y1 := support.Sized("events/c/y1", 450)
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("events/a/4", 30), y1})
	awaitHeld(t, db, "events", []string{y1.Topic}, "once y1 is stored")
	committed(t, pub, "elsewhere/barrier")
	support.DeliveredAt(t, resumed(t, b.Addr, harness.V5, mixedConnect), 1, y1.Topic, y1.Payload)
	support.DeliveredAt(t, resumed(t, b.Addr, harness.V5, dozer), 1, y1.Topic, y1.Payload)

	want := []string{"{mqtt:big events consumer count 1 missed 3}", "{mqtt:small events consumer count 2 missed 2}",
		"{mqtt:mixed events session count 2 missed null}", "{mqtt:dozer events session count 1 missed null}"}
	var got []string
	awaitLosses(t, b, fmt.Sprint(want), func(h harness.History[harness.LossRow]) bool {
		got = got[:0]
		for _, r := range h.Rows {
			got = append(got, fmt.Sprintf("{%s %s %s count %d missed %s}", r.Reader, r.Channel, r.Kind, r.Count, r.Missed()))
		}
		return slices.Equal(got, want)
	})
}

// design.md, Operations, HTTP: "Both histories keep up to 1,000 rows in
// memory and are emptied by a restart. Each returns every row it keeps.";
// "A full position-loss history replaces its least-missed row only when
// the new row missed more. Ties between victims use oldest last_seen, then
// key. At capacity, a counted new row replaces an unknown row before
// competing with counted rows under that replacement rule. An unknown new
// row replaces no existing row. The beyond counts events no longer
// represented in retained rows, including an evicted row's accumulated
// event count." Words: Channel, "A subscription's filter reaches the
// channels its topics land in".
//
// Channels c000 to c998, n1, n2 and n4 lie under c/, each keeping 250
// bytes, hold under c/hold, and u under u/. Two durable sessions, dozer and
// dozer2, subscribe to u/# and leave; retention passes both; dozer returns
// below the floor twice: one unknown row counted twice. A client, reader,
// with Receive Maximum 1, subscribes to c/# and holds a record of hold.
// Each fill channel then gets a record of 100 bytes and one of 200, which
// deletes the first, owed to reader: c998 first, then, in a later second,
// the rest: 1,000 rows. On n1, reader misses 1: the new row replaces
// dozer's, the one unknown, and beyond is 2. On n2, reader misses 1, no
// more than the least: not retained, beyond 3. dozer2 returns below the
// floor: unknown, not retained, beyond 4. On n4, reader misses 2 in one
// deletion: the row replaces c998's, the least missed and oldest; beyond
// is 5.
func TestFullPositionLostHistoryReplacesTheLeastMissed(t *testing.T) {
	t.Parallel()
	channel := func(name, filter string) harness.Channel {
		return harness.Channel{Name: name, Type: "append", Filter: filter, RetentionBytes: 250}
	}
	channels := make([]harness.Channel, 0, 1004)
	channels = append(channels, channel("hold", "c/hold/#"), channel("u", "u/#"),
		channel("n1", "c/n1/#"), channel("n2", "c/n2/#"), channel("n4", "c/n4/#"))
	for i := range 999 {
		channels = append(channels, channel(fmt.Sprintf("c%03d", i), fmt.Sprintf("c/%03d/#", i)))
	}
	b := support.OpsLaunch(t, &harness.Options{Channels: channels})
	db := support.OpenDB(t, b.DB)
	pub, window := publisher(t, b, "full-pub")
	pair := func(prefix string) []*harness.Publish {
		return []*harness.Publish{support.Sized(prefix+"/1", testkit.RecordBytes), support.Sized(prefix+"/2", 2*testkit.RecordBytes)}
	}

	dozer := &harness.Connect{ClientID: "dozer", CleanStart: true, Props: support.Durable}
	dozer2 := &harness.Connect{ClientID: "dozer2", CleanStart: true, Props: support.Durable}
	for _, p := range []*harness.Connect{dozer, dozer2} {
		support.Away(t, support.Subscribed(t, b.Addr, harness.V5, p, support.At(1, "u/#")))
	}
	support.PublishAll(t, pub, pair("u/a"))
	awaitHeld(t, db, "u", []string{"u/a/2"}, "once u/a/2 is stored")
	c := resumed(t, b.Addr, harness.V5, dozer)
	acknowledge(t, c, support.DeliveredAt(t, c, 1, "u/a/2", pair("u/a")[1].Payload).ID, 0)
	support.Away(t, c)
	support.PublishAll(t, pub, pair("u/b"))
	awaitHeld(t, db, "u", []string{"u/b/2"}, "once u/b/2 is stored")
	support.DeliveredAt(t, resumed(t, b.Addr, harness.V5, dozer), 1, "u/b/2", pair("u/b")[1].Payload)
	awaitLosses(t, b, "dozer's row counted twice", func(h harness.History[harness.LossRow]) bool {
		r, ok := lossOf(h.Rows, mqttReader("dozer"), "u")
		return ok && r.Count == 2
	})

	reader := lagging(t, b, "reader", "c/#")
	hold := support.Sized("c/hold/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, hold)
	support.DeliveredAt(t, reader, 1, hold.Topic, hold.Payload)
	publishWindowed(t, pub, pair("c/998"), window)
	awaitLosses(t, b, "reader's row on c998", func(h harness.History[harness.LossRow]) bool { return h.Tracked == 2 })
	laterSecond(t, time.Now())
	fill := make([]*harness.Publish, 0, 2*998)
	for i := range 998 {
		fill = append(fill, pair(fmt.Sprintf("c/%03d", i))...)
	}
	publishWindowed(t, pub, fill, window)
	h := awaitLosses(t, b, "1,000 rows", func(h harness.History[harness.LossRow]) bool { return h.Tracked == 1000 && len(h.Rows) == 1000 })
	if h.Beyond != 0 {
		t.Fatalf("with 1,000 rows, beyond %d; want 0", h.Beyond)
	}

	keys := func(rows []harness.LossRow) []string {
		ks := make([]string, len(rows))
		for i := range rows {
			ks[i] = lossKey(&rows[i])
		}
		slices.Sort(ks)
		return ks
	}
	present := func(channel string) func(harness.History[harness.LossRow]) bool {
		return func(h harness.History[harness.LossRow]) bool {
			_, ok := lossOf(h.Rows, mqttReader("reader"), channel)
			return ok
		}
	}
	publishWindowed(t, pub, pair("c/n1"), window)
	h = awaitLosses(t, b, "reader's row on n1", present("n1"))
	if _, ok := lossOf(h.Rows, mqttReader("dozer"), "u"); ok || h.Tracked != 1000 || h.Beyond != 2 {
		t.Errorf("after n1: dozer's row kept %v, tracked %d, beyond %d; want it replaced, 1,000 rows, beyond 2, its two events", ok, h.Tracked, h.Beyond)
	}
	rows := keys(h.Rows)

	publishWindowed(t, pub, pair("c/n2"), window)
	h = awaitLosses(t, b, "beyond 3", func(h harness.History[harness.LossRow]) bool { return h.Beyond == 3 })
	if !slices.Equal(keys(h.Rows), rows) {
		t.Errorf("after n2, missing 1, no more than the least: the rows changed; want it not retained")
	}
	c = resumed(t, b.Addr, harness.V5, dozer2)
	support.DeliveredAt(t, c, 1, "u/b/2", pair("u/b")[1].Payload)
	h = awaitLosses(t, b, "beyond 4", func(h harness.History[harness.LossRow]) bool { return h.Beyond == 4 })
	if !slices.Equal(keys(h.Rows), rows) {
		t.Errorf("after dozer2's return, unknown: the rows changed; want it not retained")
	}

	publishWindowed(t, pub, []*harness.Publish{support.Sized("c/n4/1", testkit.RecordBytes),
		support.Sized("c/n4/2", testkit.RecordBytes), support.Sized("c/n4/3", 2*testkit.RecordBytes)}, window)
	h = awaitLosses(t, b, "reader's row on n4", present("n4"))
	r, _ := lossOf(h.Rows, mqttReader("reader"), "n4")
	if _, ok := lossOf(h.Rows, mqttReader("reader"), "c998"); ok || r.Missed() != "2" || h.Tracked != 1000 || h.Beyond != 5 {
		t.Errorf("after n4: its row %v, c998's kept %v, tracked %d, beyond %d; want n4's row missing 2 in c998's place, 1,000 rows, beyond 5", r, ok, h.Tracked, h.Beyond)
	}
}

// design.md, Operations, HTTP: "Both histories keep up to 1,000 rows in
// memory and are emptied by a restart"; Words: Missed, "Counters and
// histories start empty".
//
// A connection is refused with 0x8C, and retention passes a connected
// consumer: each history has a row. After a graceful stop, the next start
// of the same broker shows both histories empty: no row, nothing tracked,
// nothing beyond.
func TestRestartEmptiesTheHistories(t *testing.T) {
	t.Parallel()
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 250},
	}}
	b := support.OpsLaunch(t, o)
	if err := connectRefused(b.Addr, "refused-once", "", 0x8C); err != nil {
		t.Fatal(err)
	}
	c := lagging(t, b, "passed", "events/#")
	pub, _ := publisher(t, b, "restart-pub")
	e1 := support.Sized("events/e1", testkit.RecordBytes)
	support.Acknowledged(t, pub, e1)
	support.DeliveredAt(t, c, 1, e1.Topic, e1.Payload)
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("events/e2", testkit.RecordBytes), support.Sized("events/e3", 2*testkit.RecordBytes)})
	awaitRefused(t, b, "a refused row", func(h harness.History[harness.RefusedRow]) bool { return h.Tracked > 0 })
	awaitLosses(t, b, "a position-lost row", func(h harness.History[harness.LossRow]) bool { return h.Tracked > 0 })

	stopped(t, b)
	b = support.OpsLaunch(t, o)
	if h := refusedHistory(t, b); h.Tracked != 0 || h.Beyond != 0 || len(h.Rows) != 0 {
		t.Errorf("after a restart, refused: %d rows, tracked %d, beyond %d; want it empty", len(h.Rows), h.Tracked, h.Beyond)
	}
	if h := lossHistory(t, b); h.Tracked != 0 || h.Beyond != 0 || len(h.Rows) != 0 {
		t.Errorf("after a restart, position-lost: %d rows, tracked %d, beyond %d; want it empty", len(h.Rows), h.Tracked, h.Beyond)
	}
}

// 0020i, planned (maintainer, 261019): "an ordinary delivery lost for its
// size does not enter the retention history". design.md, Operations, HTTP:
// "position-lost lists, under readers, readers retention passed";
// Delivery, "A message
// over the client's Maximum Packet Size is discarded for that subscriber
// as if sent, counted as missed and logged (MQTT-3.1.2-25)". metrics.md,
// saguin_missed_total{channel,cause}: "retention [...] or too_large".
//
// A client with Maximum Packet Size 512 subscribes at QoS 1 to events/# on
// an append channel. A publisher stores a record of 1,000 bytes, then one of
// 100: the client receives the second alone, saguin_missed_total of events,
// too_large, reads 1, and the position-lost history holds no row.
func TestTooLargeLossStaysOutOfTheHistory(t *testing.T) {
	t.Parallel()
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{{Name: "events", Type: "append", Filter: "events/#"}}})
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "narrow-max", CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: 512}}}, support.At(1, "events/#"))
	pub, _ := publisher(t, b, "narrow-pub")
	small := support.Sized("events/small", testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("events/large", 10*testkit.RecordBytes), small})
	support.DeliveredAt(t, c, 1, small.Topic, small.Payload)
	tooLarge := `saguin_missed_total{channel="events",cause="too_large"}`
	testkit.AwaitSample(t, b.Metrics(), tooLarge, 1, harness.DefaultTimeout)
	if h := lossHistory(t, b); h.Tracked != 0 || h.Beyond != 0 || len(h.Rows) != 0 {
		t.Errorf("after a loss for size: %v, tracked %d, beyond %d; want no row", h.Rows, h.Tracked, h.Beyond)
	}
}
