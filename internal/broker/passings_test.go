package broker

// RFC 0005 "The /v1 routes", invariant 13
//
// The bounded record behind `/v1/operations/position-lost`, driven directly
// because what it has to get right only shows at the ceiling: a thousand
// readers is not a fleet an end-to-end test can connect.

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/store"
)

func keep(s string) string { return s }

// **The cap is safe only because the rows that fall off are the ones
// nobody was looking for.** Worst-first here means most records lost, not
// most occurrences: a reader passed five hundred times for one record
// apiece has a smaller hole in its history than one passed twice for ten
// thousand, and the second is the device somebody has to go and look at.
func TestThePositionLostRecordKeepsTheWorstLoss(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.ceiling = 3

	// Three readers, losing 10, 20 and 30.
	p.add("small", "events", kindConsumer, false, 0, 10, keep)
	p.add("middle", "events", kindConsumer, false, 0, 20, keep)
	p.add("large", "events", kindConsumer, false, 0, 30, keep)

	// A fourth losing more than the least displaces the least, and only it.
	p.add("largest", "events", kindConsumer, false, 0, 40, keep)
	rows, tracked, beyond := p.worst(10)
	if tracked != 3 {
		t.Fatalf("the record holds %d rows at a ceiling of 3", tracked)
	}
	if beyond != 1 {
		t.Errorf("beyond is %d after one row was displaced, want 1: a reader shown "+
			"everything the broker kept and not told it kept less than it saw believes "+
			"it has seen the fleet", beyond)
	}
	if rows[0].Reader != "largest" || rows[0].RecordsMissed != 40 {
		t.Errorf("the first row is %s at %d records, want largest at 40 - the rows are "+
			"not worst-first", rows[0].Reader, rows[0].RecordsMissed)
	}
	for _, r := range rows {
		if r.Reader == "small" {
			t.Errorf("the reader that lost least survived and one that lost more did not: %+v", r)
		}
	}

	// A newcomer that lost less than everything held does not displace
	// anybody, and is itself what did not fit.
	p.add("tiny", "events", kindConsumer, false, 0, 5, keep)
	rows, tracked, beyond = p.worst(10)
	if tracked != 3 || beyond != 2 {
		t.Errorf("tracked %d beyond %d after a newcomer smaller than every row held, "+
			"want 3 and 2", tracked, beyond)
	}
	for _, r := range rows {
		if r.Reader == "tiny" {
			t.Error("a reader that lost less than every one held displaced one of them")
		}
	}
}

// **Occurrences accumulate and the positions do not.** The floor overtakes
// the same lagging reader repeatedly - 2,154 times on one channel in a
// six-minute run - so a row that reported only the last sweep's loss would
// describe a reader with a vast hole as having missed a handful of records.
func TestThePositionLostRecordAccumulatesWhatAReaderLost(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add("laggard", "events", kindConsumer, false, 10, 110, keep)  // 100
	p.add("laggard", "events", kindConsumer, false, 110, 310, keep) // 200
	rows, _, _ := p.worst(10)
	if len(rows) != 1 {
		t.Fatalf("two passings of one reader on one channel made %d rows, want 1", len(rows))
	}
	r := rows[0]
	if r.Count != 2 {
		t.Errorf("count is %d after two passings, want 2", r.Count)
	}
	if r.RecordsMissed != 300 {
		t.Errorf("records_missed is %d after losing 100 then 200, want 300", r.RecordsMissed)
	}
	if r.LastPosition != 110 || r.LastFloor != 310 {
		t.Errorf("last_position/last_floor are %d/%d, want the most recent passing's "+
			"110/310", r.LastPosition, r.LastFloor)
	}
}

// **One reader on three channels is three rows.** Keyed by client alone,
// the third channel's passing would overwrite the first two and the route
// would report a device as having lost only what it lost most recently,
// on only one of the channels it reads.
func TestThePositionLostRecordKeepsAReaderPerChannel(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add("device", "events", kindConsumer, false, 0, 10, keep)
	p.add("device", "audit", kindConsumer, false, 0, 20, keep)
	p.add("device", "telemetry", kindConsumer, false, 0, 30, keep)
	rows, tracked, _ := p.worst(10)
	if tracked != 3 {
		t.Fatalf("one reader passed on three channels made %d rows, want 3: keyed by "+
			"client alone a device's channels overwrite each other", tracked)
	}
	seen := map[string]uint64{}
	for _, r := range rows {
		seen[r.Channel] = r.RecordsMissed
	}
	for ch, want := range map[string]uint64{"events": 10, "audit": 20, "telemetry": 30} {
		if seen[ch] != want {
			t.Errorf("channel %s lost %d, want %d", ch, seen[ch], want)
		}
	}
}

// **The record keeps a reader's name bounded**, as the refusal record
// beside it does and for the same reason: the id arrives from whoever
// connected, the record outlives the connection, and invariant 13 is about
// everything that accumulates. The bound is the caller's, so this proves
// the record asks for one rather than that any particular limit is right.
func TestThePositionLostRecordBoundsTheClientIdItKeeps(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	long := ""
	for range 5000 {
		long += "u"
	}
	bounded := func(s string) string {
		if len(s) > 64 {
			return s[:64]
		}
		return s
	}
	p.add(long, "events", kindConsumer, false, 0, 10, bounded)
	rows, _, _ := p.worst(10)
	if len(rows) != 1 {
		t.Fatalf("the passing was not recorded at all")
	}
	if len(rows[0].Reader) > 64 {
		t.Errorf("the record keeps a %d-byte client id, want the bound its caller "+
			"passed: a stranger's string kept whole is invariant 13 failing",
			len(rows[0].Reader))
	}
}

// A passing that lost nothing is not a passing. The floor landing exactly
// where a reader stood costs it no record, and a row for it would put a
// device on an operator's worst-first list for nothing.
func TestThePositionLostRecordIgnoresAFloorThatTookNothing(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add("level", "events", kindConsumer, false, 40, 40, keep)
	p.add("behind", "events", kindConsumer, false, 41, 40, keep)
	_, tracked, beyond := p.worst(10)
	if tracked != 0 || beyond != 0 {
		t.Errorf("tracked %d beyond %d after two passings that lost nothing, want 0 and 0",
			tracked, beyond)
	}
}

// **`reported` is sticky-false, and that is the triage field's whole
// meaning.** A slow reader can be passed while connected - untold, because
// MQTT offers no way - then drop, be passed again while away, and come back
// to Session Present = 0. Both passings aggregate into one row, and with
// last-writer-wins the row would end up saying the reader was told. RFC
// 0005 sends an operator to `reported: false` first, so that row would
// under-alarm on exactly the reader carrying a hole nobody ever mentioned.
//
// So the field answers "was every passing this row aggregates reported",
// which is the question an operator is actually asking.
func TestThePositionLostRowKeepsAnUntoldPassingUntold(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add("laggard", "events", kindConsumer, false, 10, 110, keep) // untold
	p.add("laggard", "events", kindSession, true, 110, 310, keep)  // told
	rows, _, _ := p.worst(10)
	if len(rows) != 1 {
		t.Fatalf("two passings of one reader on one channel made %d rows, want 1", len(rows))
	}
	if rows[0].Reported {
		t.Error("the row says the reader was told. One of its two passings happened while " +
			"it was connected and reading, which cannot be reported at all, so the hole " +
			"from that passing was never mentioned to anybody. An operator sent to " +
			"reported = false first would skip this reader")
	}
	// The other direction is not sticky: a row that has only ever been
	// reported stays reported, or the field alarms on everything.
	q := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	q.add("told", "events", kindSession, true, 1, 5, keep)
	q.add("told", "events", kindSession, true, 5, 9, keep)
	if r, _, _ := q.worst(10); !r[0].Reported {
		t.Error("a row whose every passing was reported says it was not, so the field " +
			"alarms on readers that were told and stops meaning anything")
	}
}

// store.BridgeReader names this attack and exists to stop it: a client is
// free to call itself `bridge:head-office`, MQTT putting almost no rule on
// a client id, and the store keeps the two apart by writing every MQTT
// position under `mqtt:` and every outbound rule's under `bridge:`.
//
// This record holds both kinds of reader, so it has to keep them apart for
// the same reason. Merged, the counts sum and the kind takes whichever
// arrived last: an operator reads one row that hands the link's missing
// records to a device, or the device's to the link.
func TestThePositionLostRecordKeepsABridgeApartFromAClientOfThatName(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add(store.BridgeReader("head-office"), "events", kindBridge, false, 0, 100, keep)
	p.add(store.MQTTReader("bridge:head-office"), "events", kindConsumer, false, 0, 7, keep)
	rows, tracked, _ := p.worst(10)
	if tracked != 2 {
		t.Fatalf("an outbound rule named head-office and a device calling itself "+
			"bridge:head-office made %d row(s), want 2: %+v", tracked, rows)
	}
	byKind := map[string]uint64{}
	for _, r := range rows {
		byKind[r.Kind] = r.RecordsMissed
	}
	if byKind["bridge"] != 100 || byKind["consumer"] != 7 {
		t.Errorf("the link lost %d and the device %d, want 100 and 7: the rows have "+
			"traded losses", byKind["bridge"], byKind["consumer"])
	}
}

// **The tripwire for the next reader kind.** The two schemes that exist are
// closed by construction today - store.go defines `mqtt:` and `bridge:` and
// every stored position is written through one of the two constructors - so
// this check fires for nothing that can happen now. It is here for the kind
// added later and wired up unprefixed, which would otherwise share a key
// with whatever client calls itself by that name and merge two readers into
// one row in silence.
//
// It records anyway: a row under a suspect name is worth more to an
// operator than a loss that went unnamed, and the line is what says the
// record cannot be trusted to tell those two apart.
func TestThePositionLostRecordSaysWhenAReaderHasNoScheme(t *testing.T) {
	var buf bytes.Buffer
	p := newPassings(slog.New(slog.NewTextHandler(&buf, nil)))

	p.add("head-office", "events", kindBridge, false, 0, 100, keep) // no scheme
	if !strings.Contains(buf.String(), "does not carry the scheme its kind implies") {
		t.Errorf("a bridge row under an unprefixed name was recorded with nothing said "+
			"about it, so the first reader kind wired up without a scheme collides "+
			"silently. The log said: %q", buf.String())
	}
	if _, tracked, _ := p.worst(10); tracked != 1 {
		t.Errorf("the passing was dropped rather than recorded: %d rows. A loss that "+
			"goes unnamed is worse than one named under a suspect reader", tracked)
	}

	// And it stays quiet for the names that are right, or it is noise and an
	// operator learns to scroll past it.
	buf.Reset()
	p.add(store.BridgeReader("head-office"), "events", kindBridge, false, 0, 100, keep)
	p.add(store.MQTTReader("truck-114"), "events", kindConsumer, false, 0, 50, keep)
	p.add(store.MQTTReader("truck-114"), "audit", kindSession, true, 0, 50, keep)
	if buf.Len() != 0 {
		t.Errorf("correctly prefixed readers of all three kinds logged: %q", buf.String())
	}
}

// **A kind with no scheme declared for it is the loudest case, not the
// skipped one.** The tripwire above exists for the reader kind added later,
// and it is worth nothing if the way that kind arrives is the way it goes
// quiet: schemeFor answers "" for a constant its switch has not been taught,
// so a `want != ""` guard in front of the check would hand the new kind an
// empty scheme, skip the prefix test, and let it collide in silence -
// closing the hole through the escape hatch the type opened.
//
// Declaring a fourth passingKind and not teaching schemeFor is exactly that
// mistake, so it is what this drives.
func TestThePositionLostRecordSaysWhenAKindHasNoSchemeAtAll(t *testing.T) {
	var buf bytes.Buffer
	p := newPassings(slog.New(slog.NewTextHandler(&buf, nil)))

	// A kind schemeFor has never been taught, which is what a fourth
	// constant declared without touching its switch would be.
	p.add("head-office", "events", passingKind("share"), false, 0, 100, keep)

	if !strings.Contains(buf.String(), "no declared scheme") {
		t.Errorf("a reader kind with no scheme declared for it was recorded in silence, "+
			"so the tripwire is disabled by the one thing it exists to catch. The log "+
			"said: %q", buf.String())
	}
	if _, tracked, _ := p.worst(10); tracked != 1 {
		t.Errorf("the passing was dropped rather than recorded: %d rows", tracked)
	}
}

// **Equal losses are ordered by reader, then channel, and n bounds the rows
// while tracked still counts them all** (RFC 0005 `/v1/operations/
// position-lost`). Two reads of an
// unchanged record must answer in the same order, or an operator cannot diff
// them; and a body cut to n must say how many it held.
func TestThePositionLostRowsTieByNameAndStopAtN(t *testing.T) {
	p := newPassings(slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.add("b", "events", kindConsumer, false, 0, 10, keep)
	p.add("a", "state", kindConsumer, false, 0, 10, keep)
	p.add("a", "events", kindConsumer, false, 0, 10, keep)
	p.add("c", "events", kindConsumer, false, 0, 30, keep)
	rows, tracked, _ := p.worst(3)
	if tracked != 4 || len(rows) != 3 {
		t.Fatalf("worst(3) gave %d rows of %d tracked, want 3 of 4", len(rows), tracked)
	}
	want := [][2]string{{"c", "events"}, {"a", "events"}, {"a", "state"}}
	for i, w := range want {
		if rows[i].Reader != w[0] || rows[i].Channel != w[1] {
			t.Errorf("row %d is %s/%s, want %s/%s: worst first, then by reader, then by channel",
				i, rows[i].Reader, rows[i].Channel, w[0], w[1])
		}
	}
}
