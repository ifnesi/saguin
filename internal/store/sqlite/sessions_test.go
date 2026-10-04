package sqlite

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Both implementations of the store that keeps persistent sessions and their
// in-flight tables on the broadcast log, driven through one set of oracles,
// for the reason the pending store's are: a broker whose sessions behave
// differently when an operator moves `broker.session.storage` to another
// provider is the failure nobody sees. The rules are
// MQTT's (section 4.1, MQTT-4.4.0-1), never either implementation's.

// sessionStore is what both must answer.
type sessionStore interface {
	Save(store.Session) error
	Get(string) (store.Session, bool, error)
	All() ([]store.Session, error)
	Drop(string, []string) (store.Dropped, error)
	DropExpired(time.Time, func(store.Session) bool) ([]string, store.Dropped, error)
	SetInFlight(client string, window uint16, f store.InFlight) error
	SetInFlightAll(client string, window uint16, fs []store.InFlight) error
	InFlight(client string) (uint16, []store.InFlight, error)
	Len() int
	Bytes() int64
}

// eachSessions runs one case against the memory store and a sqlite
// provider's.
func eachSessions(t *testing.T, run func(t *testing.T, s sessionStore)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { run(t, store.NewSessions()) })
	t.Run("sqlite", func(t *testing.T) { run(t, sessionsOn(t, open(t, tempPath(t)))) })
}

func sessionsOn(t *testing.T, db *DB) *Sessions {
	t.Helper()
	s, err := db.Sessions()
	if err != nil {
		t.Fatalf("sessions store: %v", err)
	}
	return s
}

// logOf is the broadcast log a session store owns, which its sessions'
// in-flight entries point into.
func logOf(t *testing.T, s sessionStore) cursorLog {
	t.Helper()
	switch s := s.(type) {
	case *store.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatalf("the memory sessions' log: %v", err)
		}
		return lg
	case *Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatalf("the sqlite sessions' log: %v", err)
		}
		return lg
	}
	t.Fatalf("a %T owns no broadcast log", s)
	return nil
}

// flyingWindow is the Receive Maximum the tables below are written under.
const flyingWindow = 16

// flying appends one message to s's broadcast log for each state given and
// puts it on client's in-flight table, under packet identifiers from 1, at
// QoS 2 where the state is MessageReleased and 1 otherwise, and answers the
// entries in the order written. What a session holds of what it is owed is
// its in-flight table on the log (RFC 0004), so this is what gives a session
// state beyond its record.
func flying(t *testing.T, s sessionStore, client string, states ...store.MessageState) []store.InFlight {
	t.Helper()
	lg := logOf(t, s)
	fs := make([]store.InFlight, 0, len(states))
	for i, st := range states {
		r, err := lg.Append(store.Record{MessageID: fmt.Sprintf("m-%s-%d", client, i), Topic: "loose/" + client,
			Payload: []byte(client), Timestamp: time.Unix(1_700_000_000, 0)})
		if err != nil {
			t.Fatalf("append to the log: %v", err)
		}
		qos := byte(1)
		if st == store.MessageReleased {
			qos = 2
		}
		fs = append(fs, store.InFlight{Offset: r.Offset, PacketID: uint16(i + 1), QoS: qos, State: st})
	}
	if err := s.SetInFlightAll(client, flyingWindow, fs); err != nil {
		t.Fatalf("put %d entries on %s's in-flight table: %v", len(fs), client, err)
	}
	return fs
}

// sameTableOf reads client's in-flight table and compares it with want, in
// packet identifier order.
func sameTableOf(t *testing.T, s sessionStore, when, client string, want []store.InFlight) {
	t.Helper()
	_, got, err := s.InFlight(client)
	if err != nil {
		t.Fatalf("%s: %s's in-flight table: %v", when, client, err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].PacketID < got[j].PacketID })
	want = slices.Clone(want)
	sort.Slice(want, func(i, j int) bool { return want[i].PacketID < want[j].PacketID })
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: %s's in-flight table is\n%+v\nwant\n%+v", when, client, got, want)
	}
}

// aSession has one of every subscription option set, so a round trip that
// drops any of them fails.
func aSession(client string) store.Session {
	return store.Session{
		Client:         client,
		ExpiryInterval: 3600,
		Subscriptions: []store.SessionSubscription{
			{Filter: "loose/#", QoS: 1, NoLocal: true, Identifier: 42, Since: 7},
			{Filter: "$share/g/fleet/+", QoS: 2, RetainAsPublished: true, RetainHandling: 2, Since: 1},
			{Filter: "iot/+/events/+", QoS: 1, PartitionCount: 4, PartitionIndices: []int{1, 3}, Since: 1 << 40},
		},
	}
}

// sameSession compares what matters, treating an empty subscription list and
// none as the same thing, and times at the precision a file keeps.
func sameSession(t *testing.T, got, want store.Session) {
	t.Helper()
	if len(got.Subscriptions) == 0 && len(want.Subscriptions) == 0 {
		got.Subscriptions, want.Subscriptions = nil, nil
	}
	if !got.DisconnectedAt.Equal(want.DisconnectedAt) {
		t.Errorf("DisconnectedAt %v, want %v", got.DisconnectedAt, want.DisconnectedAt)
	}
	got.DisconnectedAt, want.DisconnectedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("session came back as\n%+v\nwant\n%+v", got, want)
	}
}

// A saved session comes back whole, every subscription option included, and
// saving it again replaces it rather than adding a second.
func TestASavedSessionComesBackWhole(t *testing.T) {
	eachSessions(t, func(t *testing.T, s sessionStore) {
		want := aSession("sensor-1")
		if err := s.Save(want); err != nil {
			t.Fatalf("save: %v", err)
		}
		got, ok, err := s.Get("sensor-1")
		if err != nil || !ok {
			t.Fatalf("get: %v, %v", ok, err)
		}
		sameSession(t, got, want)

		want.Subscriptions = want.Subscriptions[:1]
		want.DisconnectedAt = time.Unix(1_700_000_100, 0)
		if err := s.Save(want); err != nil {
			t.Fatalf("save again: %v", err)
		}
		all, err := s.All()
		if err != nil || len(all) != 1 {
			t.Fatalf("all: %d sessions, %v - saving twice kept two", len(all), err)
		}
		sameSession(t, all[0], want)
		if s.Len() != 1 {
			t.Errorf("Len %d, want 1", s.Len())
		}
		if _, ok, _ := s.Get("nobody"); ok {
			t.Error("a client never saved has a session")
		}
	})
}

// Dropping a session takes its in-flight table with it and nobody else's, and
// gives back every byte - a store believing it is fuller than it is refuses
// for ever, and nothing recomputes it.
func TestDroppingASessionTakesOnlyItsOwnAndGivesTheRoomBack(t *testing.T) {
	eachSessions(t, func(t *testing.T, s sessionStore) {
		tables := map[string][]store.InFlight{}
		for _, c := range []string{"a", "b"} {
			if err := s.Save(aSession(c)); err != nil {
				t.Fatal(err)
			}
			tables[c] = flying(t, s, c, store.MessageSent, store.MessageSent, store.MessageReleased)
		}
		before := s.Bytes()
		if before == 0 || s.Len() != 2 {
			t.Fatalf("the store holds %d bytes and %d sessions, so a drop proves nothing", before, s.Len())
		}
		if _, err := s.Drop("a", nil); err != nil {
			t.Fatalf("drop: %v", err)
		}
		if _, ok, _ := s.Get("a"); ok {
			t.Error("the dropped session is still kept")
		}
		// Read back rather than counted: the counts are the store's own tally,
		// and a drop that forgot the table left it there with the tally
		// saying otherwise.
		sameTableOf(t, s, "after dropping a", "a", nil)
		sameTableOf(t, s, "after dropping a", "b", tables["b"])
		if s.Bytes() != before/2 {
			t.Errorf("Bytes %d after dropping one of two equal sessions, want %d", s.Bytes(), before/2)
		}
		if d, err := s.Drop("nobody", nil); !reflect.DeepEqual(d, store.Dropped{}) || err != nil {
			t.Errorf("dropping a client with no session: %+v, %v", d, err)
		}
		if _, err := s.Drop("b", nil); err != nil {
			t.Fatal(err)
		}
		if s.Bytes() != 0 || s.Len() != 0 {
			t.Errorf("an empty store holds %d bytes and %d sessions", s.Bytes(), s.Len())
		}
	})
}

// A session expires when its client has been away for its whole interval,
// and not a moment before. A connected session never does, and an interval of
// zero ends with the disconnection.
func TestExpiredSessionsGoAndTheRestStay(t *testing.T) {
	eachSessions(t, func(t *testing.T, s sessionStore) {
		now := time.Unix(1_700_010_000, 0)
		tables := map[string][]store.InFlight{}
		for _, c := range []struct {
			client string
			expiry uint32
			away   time.Duration // zero means connected
		}{
			{"connected", 0, 0},
			{"zero-interval", 0, time.Second},
			{"just-expired", 60, 60 * time.Second},
			{"one-second-left", 60, 59 * time.Second},
			{"long", 3600, 10 * time.Minute},
			// Expired, and spared by keep: a start keeps such a record until
			// the Will its ending owes is published, and ends it then.
			{"kept-for-its-will", 60, 2 * time.Minute},
		} {
			sess := store.Session{Client: c.client, ExpiryInterval: c.expiry}
			if c.away > 0 {
				sess.DisconnectedAt = now.Add(-c.away)
			}
			if err := s.Save(sess); err != nil {
				t.Fatal(err)
			}
			tables[c.client] = flying(t, s, c.client, store.MessageSent)
		}
		spared := 0
		gone, _, err := s.DropExpired(now, func(sess store.Session) bool {
			if sess.Client != "kept-for-its-will" {
				return false
			}
			spared++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if spared != 1 {
			t.Errorf("keep was asked about the expired session it spares %d times, want 1", spared)
		}
		if want := []string{"just-expired", "zero-interval"}; !reflect.DeepEqual(gone, want) {
			t.Errorf("expired %v, want %v", gone, want)
		}
		all, _ := s.All()
		var kept []string
		for _, sess := range all {
			kept = append(kept, sess.Client)
		}
		if want := []string{"connected", "kept-for-its-will", "long", "one-second-left"}; !reflect.DeepEqual(kept, want) {
			t.Errorf("kept %v, want %v", kept, want)
		}
		// An expired session's in-flight table goes with it, and a kept one's
		// stays whole.
		for _, c := range []string{"just-expired", "zero-interval"} {
			sameTableOf(t, s, "after the sweep", c, nil)
		}
		for _, c := range kept {
			sameTableOf(t, s, "after the sweep", c, tables[c])
		}
	})
}

// A full provider refuses a session with ErrFull and keeps nothing of it: the
// session it would have replaced, the in-flight table already written and the
// counts are exactly as they were, and a new session it refused is not there.
func TestAFullProviderKeepsNothingHalfWritten(t *testing.T) {
	withFilters := func(client string, n, width int) store.Session {
		s := aSession(client)
		for i := range n {
			s.Subscriptions = append(s.Subscriptions, store.SessionSubscription{
				Filter: fmt.Sprintf("a/long/filter/to/fill/the/provider/%04d/%0*d", i, width, i), QoS: 1})
		}
		return s
	}
	check := func(t *testing.T, s sessionStore) {
		t.Helper()
		if err := s.Save(aSession("c")); err != nil {
			t.Fatal(err)
		}
		table := flying(t, s, "c", store.MessageSent, store.MessageReleased)
		var kept int
		var err error
		for i := range 10000 {
			if err = s.Save(withFilters(fmt.Sprintf("filler-%05d", i), 20, 100)); err != nil {
				break
			}
			kept++
		}
		if !errors.Is(err, store.ErrFull) {
			t.Fatalf("after %d sessions the store answered %v, want ErrFull", kept, err)
		}
		if kept == 0 {
			t.Fatal("the first session was refused, so this proves nothing about what was kept")
		}
		refused := fmt.Sprintf("filler-%05d", kept)
		if _, ok, _ := s.Get(refused); ok {
			t.Errorf("the session refused with ErrFull, %s, is kept", refused)
		}
		sess, _, _ := s.Get("c")
		count, bytes := s.Len(), s.Bytes()
		if count != kept+1 {
			t.Errorf("%d sessions counted, want the %d accepted and c", count, kept)
		}
		if err := s.Save(withFilters("c", 400, 100)); !errors.Is(err, store.ErrFull) {
			t.Fatalf("a session larger than the room left was answered %v, want ErrFull", err)
		}
		after, _, _ := s.Get("c")
		sameSession(t, after, sess)
		sameTableOf(t, s, "after the refused saves", "c", table)
		if s.Bytes() != bytes || s.Len() != count {
			t.Errorf("a refused save moved Bytes %d to %d and sessions %d to %d", bytes, s.Bytes(), count, s.Len())
		}
	}
	t.Run("memory", func(t *testing.T) {
		s := store.NewSessions()
		s.SetQuota(store.NewQuota(256<<10, 0))
		check(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		db, err := OpenBounded(tempPath(t), "test", 256<<10, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		check(t, sessionsOn(t, db))
	})
}

// What is kept survives what its provider promises to survive: a sqlite
// provider's file reopened, and a memory provider's sessions file written and
// read back. A session's in-flight table survives too, with the window it was
// written under, and an entry written after the restart joins it.
func TestSessionsSurviveTheirProvidersRestart(t *testing.T) {
	fill := func(t *testing.T, s sessionStore) ([]store.Session, []store.InFlight) {
		t.Helper()
		sess := aSession("dev-1")
		sess.DisconnectedAt = time.Unix(1_700_000_500, 123)
		// **A Will with every field it keeps**, its User Properties
		// repeating a key, in an order no sort would produce: RFC 0003 has
		// them delivered "in the order the publisher wrote them", and the
		// name its client authenticated as is what it is judged as when it
		// fires.
		sess.Will = &store.SessionWill{
			Topic: "wills/dev-1", Payload: []byte("i-died"), QoS: 1, Delay: 30,
			Props: store.Props{ContentType: "application/json", ResponseTopic: "where/to/answer",
				CorrelationData: []byte("ticket-7"), MessageExpiry: 60, Retain: true},
			User:     []store.Header{{Key: "tenant", Value: "acme"}, {Key: "site", Value: "7"}, {Key: "tenant", Value: "beta"}},
			Identity: "sensor-a",
		}
		if err := s.Save(sess); err != nil {
			t.Fatal(err)
		}
		if err := s.Save(store.Session{Client: "dev-2", ExpiryInterval: 1}); err != nil {
			t.Fatal(err)
		}
		table := flying(t, s, "dev-1", store.MessageSent, store.MessageReleased, store.MessageSent)
		all, _ := s.All()
		return all, table
	}
	after := func(t *testing.T, s sessionStore, sessions []store.Session, table []store.InFlight, bytes int64) {
		t.Helper()
		all, err := s.All()
		if err != nil || len(all) != len(sessions) {
			t.Fatalf("%d sessions after the restart, want %d: %v", len(all), len(sessions), err)
		}
		for i := range all {
			sameSession(t, all[i], sessions[i])
		}
		if window, _, err := s.InFlight("dev-1"); err != nil || window != flyingWindow {
			t.Errorf("the table's window came back as %d (%v), want %d", window, err, flyingWindow)
		}
		sameTableOf(t, s, "after the restart", "dev-1", table)
		if s.Bytes() != bytes || s.Len() != len(sessions) {
			t.Errorf("counts after restart: %d bytes %d sessions, want %d %d",
				s.Bytes(), s.Len(), bytes, len(sessions))
		}
		r, err := logOf(t, s).Append(store.Record{MessageID: "m-late", Topic: "loose/dev-1", Payload: []byte("late"),
			Timestamp: time.Unix(1_700_000_000, 0)})
		if err != nil {
			t.Fatalf("append after the restart: %v", err)
		}
		late := store.InFlight{Offset: r.Offset, PacketID: 9, QoS: 1, State: store.MessageSent}
		if err := s.SetInFlight("dev-1", flyingWindow, late); err != nil {
			t.Fatal(err)
		}
		sameTableOf(t, s, "after an entry written since", "dev-1", append(slices.Clone(table), late))
	}

	t.Run("sqlite reopened", func(t *testing.T) {
		path := tempPath(t)
		db := open(t, path)
		s := sessionsOn(t, db)
		sessions, table := fill(t, s)
		bytes := s.Bytes()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		after(t, sessionsOn(t, open(t, path)), sessions, table, bytes)
	})
	t.Run("memory through its sessions file", func(t *testing.T) {
		s := store.NewSessions()
		sessions, table := fill(t, s)
		bytes := s.Bytes()
		dir := store.NewDir(t.TempDir(), "0.1.0-test")
		if err := dir.SaveSessions(s.Snapshot()); err != nil {
			t.Fatal(err)
		}
		loaded, err := dir.LoadSessions()
		if err != nil || loaded == nil {
			t.Fatalf("load: %v, %v", loaded, err)
		}
		after(t, store.RestoreSessions(loaded), sessions, table, bytes)
	})
}

// A sessions file that is not there is no sessions. One that is there and
// damaged is an error rather than an empty start, which would be
// indistinguishable from a fresh install (invariant 14) - every byte flipped
// and every truncation is refused, never read as something.
func TestADamagedSessionsFileIsRefused(t *testing.T) {
	path := t.TempDir()
	dir := store.NewDir(path, "0.1.0-test")
	if got, err := dir.LoadSessions(); got != nil || err != nil {
		t.Fatalf("an empty directory loaded %v, %v", got, err)
	}
	s := store.NewSessions()
	if err := s.Save(aSession("dev-1")); err != nil {
		t.Fatal(err)
	}
	flying(t, s, "dev-1", store.MessageSent, store.MessageReleased)
	if err := dir.SaveSessions(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(path, "saguin.sessions")
	whole, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		seed, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("seed %d (SAGUIN_RANDOM_SEED)", seed)
	rng := rand.New(rand.NewSource(seed))
	checked := 0
	for i := range 200 {
		b := append([]byte(nil), whole...)
		if i%2 == 0 {
			b[rng.Intn(len(b))] ^= byte(1 + rng.Intn(255))
		} else {
			b = b[:rng.Intn(len(b))]
		}
		if err := os.WriteFile(file, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := dir.LoadSessions(); err == nil {
			t.Fatalf("case %d: a damaged file loaded as %+v", i, got)
		}
		checked++
	}
	if checked != 200 {
		t.Fatalf("checked %d damaged files, want 200", checked)
	}
}

// RFC 0004 "The schema": a database at the previous version is refused by
// name rather than opened and half-read; there is no in-place upgrade.
func TestADatabaseFromBeforeSessionsIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if _, err := db.db.Exec(`PRAGMA user_version = 7`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path, "test")
	if err == nil {
		t.Fatal("a version 7 file opened")
	}
	if want := "schema version 7"; !strings.Contains(err.Error(), want) {
		t.Errorf("refused with %q, want it to name %q", err, want)
	}
}

// store.SQLiteEmptyBytes is what configuration validation assumes an empty
// database takes, because `--check-config` cannot open the file. A schema
// change that moves it fails here rather than letting validation admit a
// provider that opens over its own bound.
func TestAnEmptyDatabaseTakesWhatValidationAssumes(t *testing.T) {
	db := open(t, tempPath(t))
	var pages, size int64
	if err := db.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(`PRAGMA page_size`).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if pages*size != store.SQLiteEmptyBytes {
		t.Errorf("an empty database takes %d pages of %d bytes, %d bytes; store.SQLiteEmptyBytes says %d",
			pages, size, pages*size, store.SQLiteEmptyBytes)
	}
}

// RFC 0002 `broker.session`: a Will is session state, so it costs its
// provider what it carries - and the provider's `max_bytes` is what bounds it.
//
// **The arithmetic is exact rather than "it went up".** A Will is five
// variable-length fields: its topic, its payload, and the three publish
// properties that carry bytes of their own. A test that only watched the
// number rise would go on passing when somebody adds a sixth field to
// SessionWill and forgets to charge for it - which is a session store holding
// bytes its provider does not know about, and a bound that stops bounding.
func TestAWillCostsItsProviderWhatItCarries(t *testing.T) {
	eachSessions(t, func(t *testing.T, s sessionStore) {
		bare := aSession("dev-1")
		if err := s.Save(bare); err != nil {
			t.Fatal(err)
		}
		without := s.Bytes()
		if without == 0 {
			t.Fatal("a session with subscriptions costs nothing, so this store is not counting")
		}

		armed := aSession("dev-1")
		armed.Will = &store.SessionWill{
			Topic:   "house/device/gone",
			Payload: []byte("i-died"),
			QoS:     1,
			Delay:   30,
			DueAt:   time.Unix(1_700_000_500, 0),
			Props: store.Props{
				ContentType:     "application/json",
				ResponseTopic:   "where/to/answer",
				CorrelationData: []byte("ticket-7"),
				MessageExpiry:   60,
				Retain:          true,
			},
			User:     []store.Header{{Key: "tenant", Value: "acme"}, {Key: "site", Value: "7"}},
			Identity: "sensor-a",
		}
		if err := s.Save(armed); err != nil {
			t.Fatal(err)
		}
		w := armed.Will
		const user = len("tenant") + len("acme") + len("site") + len("7")
		want := int64(len(w.Topic) + len(w.Payload) + len(w.Props.ContentType) +
			len(w.Props.ResponseTopic) + len(w.Props.CorrelationData) + user + len(w.Identity))
		if got := s.Bytes() - without; got != want {
			t.Errorf("arming a Will cost the provider %d bytes, want %d - its topic (%d), "+
				"payload (%d), content type (%d), response topic (%d), correlation data "+
				"(%d), User Properties (%d) and identity (%d). A field that costs nothing "+
				"here is one the bound cannot see",
				got, want, len(w.Topic), len(w.Payload), len(w.Props.ContentType),
				len(w.Props.ResponseTopic), len(w.Props.CorrelationData), user, len(w.Identity))
		}

		// And taking the Will gives every byte of it back: a Will fires once,
		// and a provider that kept charging for it would drift upwards by one
		// Will per device for the life of the broker.
		taken := aSession("dev-1")
		if err := s.Save(taken); err != nil {
			t.Fatal(err)
		}
		if got := s.Bytes(); got != without {
			t.Errorf("the provider holds %d bytes once the Will is taken, want %d: %d bytes "+
				"of a Will nobody holds any more", got, without, got-without)
		}
	})
}

// TestASessionSurvivesUntilItsIntervalHasPassed is the boundary itself, on
// both providers, to the millisecond. A session ending early is data loss
// rather than untidiness: its subscriptions and the messages it was owed go
// with it while its client is still inside the interval it asked for and can
// still come back for them. The engine's sweep held this arithmetic in whole
// seconds and ended a two-second session after 1.892; these rows are what
// says the store never did and never will.
func TestASessionSurvivesUntilItsIntervalHasPassed(t *testing.T) {
	eachSessions(t, func(t *testing.T, s sessionStore) {
		// Late in its second, which is where truncation does its damage.
		away := time.Unix(1_700_010_005, 974_000_000)

		for _, c := range []struct {
			client string
			after  time.Duration
		}{
			{"at-1892ms", 1892 * time.Millisecond},
			{"at-1999ms", 1999 * time.Millisecond},
			{"at-2000ms", 2 * time.Second},
			{"at-2001ms", 2001 * time.Millisecond},
		} {
			if err := s.Save(store.Session{
				Client: c.client, ExpiryInterval: 2, DisconnectedAt: away,
			}); err != nil {
				t.Fatal(err)
			}
		}

		// Each moment asked separately, because DropExpired removes what it
		// finds and a later moment must not be answered on a thinner store.
		for _, c := range []struct {
			at    time.Duration
			gone  []string
			stays []string
		}{
			{1892 * time.Millisecond, nil, []string{"at-1892ms", "at-1999ms", "at-2000ms", "at-2001ms"}},
			{1999 * time.Millisecond, nil, []string{"at-1892ms", "at-1999ms", "at-2000ms", "at-2001ms"}},
			{2 * time.Second, []string{"at-1892ms", "at-1999ms", "at-2000ms", "at-2001ms"}, nil},
		} {
			held, _, err := s.DropExpired(away.Add(c.at), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(held) != len(c.gone) {
				t.Errorf("%v after the disconnect dropped %v, want %v: a two-second session "+
					"ends at 2.000 and not before", c.at, held, c.gone)
			}
			all, err := s.All()
			if err != nil {
				t.Fatal(err)
			}
			var kept []string
			for _, sess := range all {
				kept = append(kept, sess.Client)
			}
			sort.Strings(kept)
			if !reflect.DeepEqual(kept, c.stays) {
				t.Errorf("%v after the disconnect the store still holds %v, want %v",
					c.at, kept, c.stays)
			}
		}
	})
}
