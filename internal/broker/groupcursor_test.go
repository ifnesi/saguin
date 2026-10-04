package broker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// refusesRecordWrites refuses one client's record writes while armed: the
// plain Save, and the one that keeps a group's cursor with the record.
type refusesRecordWrites struct {
	broker.SessionStore
	client  string
	err     error
	plain   bool // Save too, not only the write with cursors
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *refusesRecordWrites) Unwrap() broker.SessionStore { return s.SessionStore }

// Snapshot is the memory store's, which a stop writes to its sessions file:
// without it a memory harness restarts empty, and proves nothing.
func (s *refusesRecordWrites) Snapshot() *store.SessionsSnapshot {
	return s.SessionStore.(interface {
		Snapshot() *store.SessionsSnapshot
	}).Snapshot()
}

func (s *refusesRecordWrites) Save(sess store.Session) error {
	if s.plain && s.armed.Load() && sess.Client == s.client {
		s.refused.Add(1)
		return s.err
	}
	return s.SessionStore.Save(sess)
}

func (s *refusesRecordWrites) SaveWithShareCursors(sess store.Session, cursor uint64, groups []string) error {
	if s.armed.Load() && sess.Client == s.client {
		s.refused.Add(1)
		return s.err
	}
	return s.SessionStore.(interface {
		SaveWithShareCursors(store.Session, uint64, []string) error
	}).SaveWithShareCursors(sess, cursor, groups)
}

// refusesCursorWrites refuses the drain's own writes of a group's cursor
// while armed.
type refusesCursorWrites struct {
	drainSessions
	err     error
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *refusesCursorWrites) CreateShareCursor(group string, cursor uint64) error {
	if s.armed.Load() {
		s.refused.Add(1)
		return s.err
	}
	return s.drainSessions.CreateShareCursor(group, cursor)
}

// providerOf is the provider's own session store under the harness's,
// whatever wraps it.
func providerOf(s broker.SessionStore) broker.SessionStore {
	for {
		w, ok := s.(interface{ Unwrap() broker.SessionStore })
		if !ok {
			return s
		}
		s = w.Unwrap()
	}
}

// storedCursors is every group's cursor the provider keeps.
func storedCursors(t *testing.T, s broker.SessionStore) map[string]uint64 {
	t.Helper()
	c, err := providerOf(s).(interface {
		ShareCursors() (map[string]uint64, error)
	}).ShareCursors()
	if err != nil {
		t.Fatalf("read the groups' cursors: %v", err)
	}
	return c
}

// unkeepGroup leaves group as a broker that swallowed its cursor's refusal
// left it: its member's record holds the filter, and neither the store nor the
// drain has a cursor or a list for it.
func unkeepGroup(t *testing.T, h *brokertest.Harness, group string) {
	t.Helper()
	dropped, err := providerOf(brokertest.HarnessSessions).(interface {
		DropShareCursor(string) (bool, error)
	}).DropShareCursor(group)
	if err != nil || !dropped {
		t.Fatalf("drop %s's cursor: %v, %v", group, dropped, err)
	}
	h.B.EndGroupForTest(group)
	if _, ok := storedCursors(t, brokertest.HarnessSessions)[group]; ok {
		t.Fatal("the cursor is still stored")
	}
}

// RFC 0002 SUBACK reason codes, RFC 0003 "Broadcast", invariant 18: a
// durable member's SUBSCRIBE to a group with no cursor keeps the record and
// the cursor in one write, and a cursor the store refuses refuses the packet -
// every filter it adds, `0x97` where the provider has no room and `0x83`
// otherwise - with nothing of it kept: no filter in the record, no cursor, no
// list, and after a restart a session that does not hold the filter. A SUBACK
// of success here was a group keeping nothing while its member was told it
// was subscribed.
//
// On memory the provider's own quota refuses it for room, the record alone
// fitting; elsewhere the write is refused at the store's surface, the
// session store's write and the drain's alike.
func TestAGroupCursorTheStoreRefusesRefusesTheSubscribe(t *testing.T) {
	const group, plain = "$share/g/news/#", "news/plain/#"
	full := fmt.Errorf("refused for the test: %w", store.ErrProviderFull)
	failed := errors.New("disk I/O error (refused for the test)")
	for _, c := range []struct {
		name  string
		quota bool
		err   error
		want  byte
	}{
		{"memory/no room in the quota", true, nil, 0x97},
		{"memory/failed", false, failed, 0x83},
		{"sqlite/no room", false, full, 0x97},
		{"sqlite/failed", false, failed, 0x83},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := restartables(t)[0]
			if strings.HasPrefix(c.name, "sqlite") {
				p = restartables(t)[1]
			}
			rs := &refusesRecordWrites{client: "member", err: c.err}
			var inner broker.SessionStore
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				inner, rs.SessionStore = s, s
				return rs
			}
			h := p.start(t)
			brokertest.WrapSessions = nil
			dr := &refusesCursorWrites{err: c.err}
			attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				dr.drainSessions = s
				return dr
			})
			m := dial(t, h, "member", false, false, 0, 3600, 0)
			if c.quota {
				mem := inner.(*store.Sessions)
				rec, _, err := mem.Get("member")
				if err != nil {
					t.Fatal(err)
				}
				grown := rec
				grown.Subscriptions = append(slices.Clone(rec.Subscriptions),
					store.SessionSubscription{Filter: group, QoS: 1}, store.SessionSubscription{Filter: plain, QoS: 1})
				probe := store.NewQuota(0, 0)
				mem.SetQuota(probe)
				room := probe.Bytes() + store.SessionSize(grown) - store.SessionSize(rec) + store.ShareCursorSize(group) - 1
				mem.SetQuota(store.NewQuota(room, 0))
			} else {
				rs.armed.Store(true)
				dr.armed.Store(true)
			}

			codes := m.SubMany(t, []string{group, plain}, 1)
			kept := storedFilters(t, inner, "member")
			cursors := storedCursors(t, inner)
			_, listed := h.B.GroupList(group)
			t.Logf("SUBACK %#v; the record holds %v; cursors %v; a list: %v; refused at the store's surface %d, the drain's %d",
				codes, kept, cursors, listed, rs.refused.Load(), dr.refused.Load())
			if !c.quota && rs.refused.Load()+dr.refused.Load() == 0 {
				t.Fatal("no cursor write reached the refusal, so this proves nothing")
			}
			for i, code := range codes {
				if code != c.want {
					t.Errorf("filter %d was answered 0x%02x, want 0x%02x", i, code, c.want)
				}
			}
			if len(kept) != 0 {
				t.Errorf("the record holds %v after a SUBACK refusing them", kept)
			}
			if _, ok := cursors[group]; ok {
				t.Errorf("the group has a cursor at %d after its SUBACK was refused", cursors[group])
			}
			if listed {
				t.Error("the group has a list after its SUBACK was refused")
			}

			before := h.Disconnects.Snapshot("member")
			m.Close()
			sessionGoneAfter(t, h, "member", before)
			h.Stop()
			h = p.start(t)
			if _, ok, err := brokertest.HarnessSessions.Get("member"); err != nil || !ok {
				t.Fatalf("the restart kept no record for the member (%v), so this proves nothing", err)
			}
			after := storedFilters(t, brokertest.HarnessSessions, "member")
			cursors = storedCursors(t, brokertest.HarnessSessions)
			t.Logf("after the restart the record holds %v; cursors %v", after, cursors)
			if len(after) != 0 {
				t.Errorf("after the restart the session holds %v, which its SUBACK refused", after)
			}
			if _, ok := cursors[group]; ok {
				t.Error("after the restart the group has a cursor")
			}
		})
	}
}

// RFC 0004 `saguin.sessions`, RFC 0003 "Broadcast": a session's subscription
// is kept with the broadcast log's offset when it was made, so that a
// subscription made later does not reach back to a message already in the
// log; and it is made when the SUBACK grants it. A SUBSCRIBE the store
// refused made nothing: subscribing again later is new then, and what was
// published between the two is not owed - neither to the session's own
// filter, nor to the group whose cursor came with it. A refused write that
// left where the filter was made behind stored the later subscription as made
// before what was published in between.
func TestARefusedSubscribeLeavesNoBoundaryBehind(t *testing.T) {
	const group, own = "$share/g/news/#", "news/#"
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			rs := &refusesRecordWrites{client: "member", err: errors.New("disk I/O error (refused for the test)"), plain: true}
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				rs.SessionStore = s
				return rs
			}
			h := p.start(t)
			brokertest.WrapSessions = nil
			awayKeeper(t, h)
			m := dial(t, h, "member", false, false, 0, 3600, 0)
			pub := dial(t, h, "publisher", true, false, 0, 0, 0)

			rs.armed.Store(true)
			refused := m.SubMany(t, []string{own, group}, 1)
			rs.armed.Store(false)
			sinceRefused := h.B.BroadcastSince("member")
			pub.Pub(t, "news/x", "before")
			through := h.B.CountedThrough()
			granted := m.SubMany(t, []string{own, group}, 1)
			since := h.B.BroadcastSince("member")
			cursor := storedCursors(t, brokertest.HarnessSessions)[group]
			t.Logf("refused %#v (Since then %v); %q counted through %d; granted %#v, Since %v, the group's cursor %d",
				refused, sinceRefused, "before", through, granted, since, cursor)
			if !slices.Equal(refused, []byte{0x83, 0x83}) || !slices.Equal(granted, []byte{1, 1}) {
				t.Fatalf("SUBACKs %#v then %#v, want 0x83 twice then granted", refused, granted)
			}
			rec, _, err := brokertest.HarnessSessions.Get("member")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("the stored record: %+v", rec.Subscriptions)
			for _, sub := range rec.Subscriptions {
				if sub.Since <= through || since[sub.Filter] <= through {
					t.Errorf("%s is stored as made at %d (the drain says %d), at or before %q at %d, published before its SUBACK",
						sub.Filter, sub.Since, since[sub.Filter], "before", through)
				}
			}
			if len(rec.Subscriptions) != 2 {
				t.Errorf("the stored record holds %d subscriptions, want 2", len(rec.Subscriptions))
			}
			if cursor <= through {
				t.Errorf("the group's cursor is at %d, at or before %q at %d", cursor, "before", through)
			}
			pub.Pub(t, "news/x", "after")
			eventually(t, "the member is sent what came after, by both", 5*time.Second, func() bool {
				return payloadsOf(m.Payloads(), "after") == 2
			})

			before := h.Disconnects.Snapshot("member")
			m.Close()
			sessionGoneAfter(t, h, "member", before)
			h.Stop()
			h = p.start(t)
			back := dial(t, h, "member", false, false, 0, 3600, 0)
			pub = dial(t, h, "publisher", true, false, 0, 0, 0)
			pub.Pub(t, "news/x", "end")
			ended := eventuallyTrue(5*time.Second, func() bool { return payloadsOf(back.Payloads(), "end") == 2 })
			t.Logf("after the restart the member was sent %v", back.Payloads())
			if !ended {
				t.Fatal("the member was not sent the end by both its subscriptions within 5s")
			}
			if n := payloadsOf(back.Payloads(), "before"); n != 0 {
				t.Errorf("after the restart the member was sent %q %d times, published before its subscription", "before", n)
			}
		})
	}
}

// awayKeeper is a durable session holding news/# whose client has gone, so
// that what is published there stays in the log, owed to it.
func awayKeeper(t *testing.T, h *brokertest.Harness) {
	t.Helper()
	keeper := dial(t, h, "keeper", false, false, 0, 3600, 0)
	keeper.Sub(t, "news/#", 1)
	before := h.Disconnects.Snapshot("keeper")
	keeper.Close()
	sessionGoneAfter(t, h, "keeper", before)
}

func payloadsOf(ps []string, p string) int {
	n := 0
	for _, x := range ps {
		if x == p {
			n++
		}
	}
	return n
}

// blockedIn reports whether a goroutine is blocked in state with fn on its
// stack.
func blockedIn(fn, state string) bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	for n == len(buf) {
		buf = make([]byte, 2*len(buf))
		n = runtime.Stack(buf, true)
	}
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		header, _, _ := strings.Cut(g, "\n")
		if strings.Contains(header, "["+state) && strings.Contains(g, fn) {
			return true
		}
	}
	return false
}

// RFC 0003 "Broadcast", MQTT-4.3.2: a QoS 1 publish answered while a durable
// member holds its group's filter reaches the member across a crash. **A
// publish in the moment the group's cursor is being made is on one side of
// it**: here it is published once the store has kept the cursor and before
// the group has its list, while a returning member's CONNECT makes it, and it
// waits for the list and is kept for the group. Decided in that moment
// without the list, it was written nowhere and went to the member live: the
// member had it unacknowledged, and after the crash it was gone.
//
// On sqlite, the provider that promises what a crash leaves.
func TestAPublishRacingAGroupsCursorIsKeptForIt(t *testing.T) {
	const group = "$share/g/news/#"
	path := t.TempDir() + "/sessions.db"
	h := startDurableSQLite(t, path)
	m := dial(t, h, "member", false, false, 0, 3600, 0)
	m.Sub(t, group, 1)
	awaitGroup(t, h, group)
	before := h.Disconnects.Snapshot("member")
	m.Close()
	sessionGoneAfter(t, h, "member", before)
	unkeepGroup(t, h, group)
	pub := dial(t, h, "publisher", true, false, 0, 0, 0)

	acked := make(chan error, 1)
	decided := make(chan string, 1)
	var once sync.Once
	defer broker.SetGroupCreating(func(b *broker.Broker, groups []string) {
		if b != h.B || !slices.Contains(groups, group) {
			return
		}
		once.Do(func() {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := pub.C.Publish(ctx, &paho.Publish{Topic: "news/x", QoS: 1, Payload: []byte("racing")})
				acked <- err
			}()
			// Until the publish has been decided without the list, or is
			// waiting for it.
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
				if blockedIn("bdrain).wanted", "sync.RWMutex.RLock") {
					decided <- "waited for the list"
					return
				}
				select {
				case err := <-acked:
					acked <- err
					decided <- "was answered without the list"
					return
				default:
				}
			}
			decided <- "neither waited nor was answered within 5s"
		})
	})()

	back := dial(t, h, "member", false, true, 0, 3600, 0)
	var how string
	select {
	case how = <-decided:
	default:
		t.Fatal("the returning member's CONNECT made no cursor, so nothing raced it")
	}
	if err := <-acked; err != nil {
		t.Fatalf("the racing publish: %v", err)
	}
	t.Logf("the publish in the moment the cursor was made %s", how)
	got, sent := back.Await(t, 5*time.Second)
	t.Logf("before the crash the member was sent %q (%v); the group's cursor %v", got.Payload, sent,
		storedCursors(t, brokertest.HarnessSessions))
	h.Crash()

	h = startDurableSQLite(t, path)
	again := dial(t, h, "member", false, false, 0, 3600, 0)
	r, ok := again.Await(t, 5*time.Second)
	t.Logf("after the crash the member was sent %q (%v)", r.Payload, ok)
	if !ok || r.Payload != "racing" {
		t.Fatalf("a publish answered while the member held the group was lost across the crash "+
			"(it %s; sent before the crash: %v)", how, sent)
	}
}

// RFC 0003 "Broadcast": a start gives each group a durable member holds and
// the store has no cursor for one, at the earliest place any member's
// subscription was made, so the group is owed what was published to it while
// it had none; and a start that cannot give it one fails, rather than serve a
// group that keeps nothing.
func TestAStartGivesAGroupWithNoCursorOne(t *testing.T) {
	const group = "$share/g/news/#"
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			awayKeeper(t, h)
			m := dial(t, h, "member", false, false, 0, 3600, 0)
			m.Sub(t, group, 1)
			awaitGroup(t, h, group)
			before := h.Disconnects.Snapshot("member")
			m.Close()
			sessionGoneAfter(t, h, "member", before)
			unkeepGroup(t, h, group)
			pub := dial(t, h, "publisher", true, false, 0, 0, 0)
			pub.Pub(t, "news/x", "while unkept")
			since := h.B.BroadcastSince("member")[group]

			// A start the store refuses the cursor fails.
			lg, st := providerLogAndSessions(t)
			refusing := &refusesCursorWrites{drainSessions: st, err: errors.New("disk I/O error (refused for the test)")}
			refusing.armed.Store(true)
			err := h.B.UseBroadcastLog(lg, refusing)
			t.Logf("a start refused the cursor: %v (refused %d)", err, refusing.refused.Load())
			if err == nil || !strings.Contains(err.Error(), "refused for the test") {
				t.Errorf("a start whose cursor the store refused answered %v, want the refusal", err)
			}

			h.Stop()
			h = p.start(t)
			cursors := storedCursors(t, brokertest.HarnessSessions)
			_, listed := h.B.GroupList(group)
			t.Logf("the member's subscription was made at %d; after the start the cursors are %v, a list: %v",
				since, cursors, listed)
			if c, ok := cursors[group]; !ok || c != since {
				t.Errorf("after the start the group's cursor is %d (%v), want %d, where its member's subscription was made",
					c, ok, since)
			}
			back := dial(t, h, "member", false, false, 0, 3600, 0)
			r, ok := back.Await(t, 5*time.Second)
			t.Logf("the member came back to %q (%v)", r.Payload, ok)
			if !ok || r.Payload != "while unkept" {
				t.Errorf("the member was not sent what was published to its group while it had no cursor")
			}
		})
	}
}

// providerLogAndSessions is the harness's provider's broadcast log and session
// store, as a start attaches them.
func providerLogAndSessions(t *testing.T) (drainReads, drainSessions) {
	t.Helper()
	switch s := providerOf(brokertest.HarnessSessions).(type) {
	case *store.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg, s
	case *sqlite.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg, s
	}
	t.Fatalf("the harness's session store is a %T", brokertest.HarnessSessions)
	return nil, nil
}

// RFC 0003 "Broadcast", RFC 0002 CONNACK reason codes: a resumed session
// holding a group with no cursor is given one before its CONNACK, and where
// the store refuses it the CONNECT is refused - `0x97` for room, `0x83`
// otherwise - and nothing changes: the record still holds the filter, and the
// group has no cursor and no list until a CONNECT the store keeps one for.
func TestAResumeWhoseGroupCursorTheStoreRefusesIsRefused(t *testing.T) {
	const group = "$share/g/news/#"
	for _, c := range []struct {
		name string
		err  error
		want byte
	}{
		{"no room", fmt.Errorf("refused for the test: %w", store.ErrProviderFull), 0x97},
		{"failed", errors.New("disk I/O error (refused for the test)"), 0x83},
	} {
		t.Run(c.name, func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
				dr := &refusesCursorWrites{err: c.err}
				attachDrainThrough(t, h, func(s drainSessions) drainSessions {
					dr.drainSessions = s
					return dr
				})
				m := dial(t, h, "member", false, false, 0, 3600, 0)
				m.Sub(t, group, 1)
				awaitGroup(t, h, group)
				before := h.Disconnects.Snapshot("member")
				m.Close()
				sessionGoneAfter(t, h, "member", before)
				unkeepGroup(t, h, group)

				dr.armed.Store(true)
				_, connack := connectRawWith(t, h, "member", 10, false, 3600)
				kept := storedFilters(t, brokertest.HarnessSessions, "member")
				cursors := storedCursors(t, brokertest.HarnessSessions)
				_, listed := h.B.GroupList(group)
				t.Logf("CONNACK 0x%02x; the record holds %v; cursors %v; a list: %v; refused %d",
					connack.ReasonCode, kept, cursors, listed, dr.refused.Load())
				if connack.ReasonCode != c.want {
					t.Errorf("the CONNECT was answered 0x%02x, want 0x%02x", connack.ReasonCode, c.want)
				}
				if !slices.Contains(kept, group) {
					t.Errorf("the refused CONNECT changed the record to %v", kept)
				}
				if _, ok := cursors[group]; ok || listed {
					t.Errorf("the group has a cursor (%v) or a list (%v) its store refused", ok, listed)
				}

				dr.armed.Store(false)
				dial(t, h, "member", false, false, 0, 3600, 0)
				cursors = storedCursors(t, brokertest.HarnessSessions)
				_, listed = h.B.GroupList(group)
				t.Logf("accepted: cursors %v; a list: %v", cursors, listed)
				if _, ok := cursors[group]; !ok || !listed {
					t.Errorf("the accepted CONNECT left the group without a cursor (%v) or a list (%v)", ok, listed)
				}
			})
		})
	}
}

// holdsUnheldEnding holds the drain's ending of group's cursor once the store
// has ended it, before the drain lets its list go.
type holdsUnheldEnding struct {
	drainSessions
	group            string
	reached, release chan struct{}
	once             sync.Once
}

func (s *holdsUnheldEnding) EndShareCursorIfUnheld(group string) (bool, error) {
	ended, err := s.drainSessions.EndShareCursorIfUnheld(group)
	if group == s.group && ended && err == nil {
		s.once.Do(func() {
			close(s.reached)
			<-s.release
		})
	}
	return ended, err
}

// holdsSessionEnding holds a session's ending - a Drop, or the Begin of a
// session in its place - once the store has ended group's cursor with it,
// before the drain lets the group's list go.
type holdsSessionEnding struct {
	broker.SessionStore
	group            string
	reached, release chan struct{}
	once             sync.Once
}

func (s *holdsSessionEnding) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *holdsSessionEnding) hold(d store.Dropped, err error) {
	if err == nil && slices.Contains(d.Groups, s.group) {
		s.once.Do(func() {
			close(s.reached)
			<-s.release
		})
	}
}

func (s *holdsSessionEnding) Drop(id string, held []string) (store.Dropped, error) {
	d, err := s.SessionStore.Drop(id, held)
	s.hold(d, err)
	return d, err
}

func (s *holdsSessionEnding) Begin(id string, held []string, next *store.Session) (store.Dropped, error) {
	d, err := s.SessionStore.Begin(id, held, next)
	s.hold(d, err)
	return d, err
}

// joinAsTheLastLeaves subscribes a new durable member to group while the
// group's last member's leaving is held between the store's ending of the
// cursor and the drain's of its list (reached, release), and then shows that
// a QoS 1 publish answered while the new member is away reaches it across a
// crash. **Its SUBSCRIBE is on one side of the ending**: before it, and the
// store does not end a cursor a kept session holds; or after it, and the
// cursor is made again with its record. Decided between the two, it saw the
// list still there, kept its record alone, and the list went: the group kept
// nothing, and the publish was answered and lost.
func joinAsTheLastLeaves(t *testing.T, h *brokertest.Harness, path, group string, leave func(),
	reached, release chan struct{}) {
	t.Helper()
	left := make(chan struct{})
	go func() {
		defer close(left)
		leave()
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the last member's leaving never ended the group's cursor, so nothing raced it")
	}
	b := dial(t, h, "new", false, false, 0, 3600, 0)
	subacked := make(chan []byte, 1)
	go func() { subacked <- b.SubMany(t, []string{group}, 1) }()
	var how string
	for deadline := time.Now().Add(5 * time.Second); how == ""; time.Sleep(time.Millisecond) {
		switch {
		case blockedIn("bdrain).createGroups", "sync.RWMutex.Lock"):
			how = "waited for the ending"
		case len(subacked) > 0:
			how = "was answered inside the ending"
		case time.Now().After(deadline):
			t.Fatal("the SUBSCRIBE neither waited for the ending nor was answered within 5s")
		}
	}
	close(release)
	<-left
	codes := <-subacked
	t.Logf("the new member's SUBSCRIBE %s: SUBACK %#v; the record holds %v; cursors %v",
		how, codes, storedFilters(t, brokertest.HarnessSessions, "new"), storedCursors(t, brokertest.HarnessSessions))
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("the new member's SUBACK is %#v, want granted", codes)
	}

	before := h.Disconnects.Snapshot("new")
	b.Close()
	sessionGoneAfter(t, h, "new", before)
	pub := dial(t, h, "publisher", true, false, 0, 0, 0)
	pub.Pub(t, "news/x", "answered") // QoS 1, and answered
	h.Crash()
	h = startDurableSQLite(t, path)
	back := dial(t, h, "new", false, false, 0, 3600, 0)
	r, ok := back.Await(t, 5*time.Second)
	t.Logf("after the crash the new member was sent %q (%v)", r.Payload, ok)
	if !ok || r.Payload != "answered" {
		t.Fatalf("a publish answered while the new member held the group was lost across the crash "+
			"(its SUBSCRIBE %s)", how)
	}
}

// RFC 0003 "Broadcast", MQTT-4.3.2: the group's last member UNSUBSCRIBEs, and
// a new durable member SUBSCRIBEs while that ending is between the store's
// cursor and the drain's list (endUnheld). On sqlite, the provider that
// promises what a crash leaves.
func TestAMemberJoiningAsTheLastUnsubscribesKeepsTheGroup(t *testing.T) {
	const group = "$share/g/news/#"
	path := t.TempDir() + "/sessions.db"
	h := startDurableSQLite(t, path)
	hold := &holdsUnheldEnding{group: group, reached: make(chan struct{}), release: make(chan struct{})}
	attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		hold.drainSessions = s
		return hold
	})
	old := dial(t, h, "old", false, false, 0, 3600, 0)
	old.Sub(t, group, 1)
	awaitGroup(t, h, group)
	joinAsTheLastLeaves(t, h, path, group, func() {
		if _, err := old.C.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{group}}); err != nil {
			t.Errorf("the old member's UNSUBSCRIBE: %v", err)
		}
	}, hold.reached, hold.release)
}

// The same, where the last member's session ends instead, its group's cursor
// with it in one store write: a DISCONNECT that ends the session (Drop), and
// a clean start that begins another in its place (Begin).
func TestAMemberJoiningAsTheLastsSessionEndsKeepsTheGroup(t *testing.T) {
	const group = "$share/g/news/#"
	for _, c := range []struct {
		name  string
		leave func(t *testing.T, h *brokertest.Harness, old *brokertest.Client)
	}{
		{"a DISCONNECT ending the session", func(t *testing.T, h *brokertest.Harness, old *brokertest.Client) {
			zero := uint32(0)
			if err := old.C.Disconnect(&paho.Disconnect{Properties: &paho.DisconnectProperties{SessionExpiryInterval: &zero}}); err != nil {
				t.Errorf("the old member's DISCONNECT: %v", err)
			}
		}},
		{"a clean start in its place", func(t *testing.T, h *brokertest.Harness, old *brokertest.Client) {
			dial(t, h, "old", true, false, 0, 3600, 0)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := t.TempDir() + "/sessions.db"
			hold := &holdsSessionEnding{group: group, reached: make(chan struct{}), release: make(chan struct{})}
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				hold.SessionStore = s
				return hold
			}
			h := startDurableSQLite(t, path)
			brokertest.WrapSessions = nil
			old := dial(t, h, "old", false, false, 0, 3600, 0)
			old.Sub(t, group, 1)
			awaitGroup(t, h, group)
			joinAsTheLastLeaves(t, h, path, group, func() { c.leave(t, h, old) }, hold.reached, hold.release)
		})
	}
}

// Invariant 17, RFC 0003 "Sessions": a CONNECT that begins a new session -
// here because the session it takes over ends with its connection - is not
// resuming that session's groups, and a CONNECT the store then refuses (its
// Will cannot be kept, `0x83`) changes nothing of them: no cursor, no list.
func TestARefusedConnectBeginningANewSessionLeavesTheOldGroupsAlone(t *testing.T) {
	const group = "$share/g/news/#"
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			rs := &refusesRecordWrites{client: "same", err: errors.New("disk I/O error (refused for the test)"), plain: true}
			brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
				rs.SessionStore = s
				return rs
			}
			h := p.start(t)
			brokertest.WrapSessions = nil
			old := dial(t, h, "same", false, false, 0, 0, 0) // ends with its connection
			old.Sub(t, group, 1)
			if _, ok := storedCursors(t, brokertest.HarnessSessions)[group]; ok {
				t.Fatal("a member whose session ends with its connection made a cursor, so this proves nothing")
			}

			rs.armed.Store(true)
			conn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			raw := &rawClient{t: t, conn: conn}
			raw.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
				Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "same", Keepalive: 60,
					Clean: false, WillFlag: true, WillTopic: "will/same", WillPayload: []byte("gone")},
				Properties: packets.Properties{SessionExpiryInterval: 3600, SessionExpiryIntervalFlag: true}})
			ack := raw.read()
			cursors := storedCursors(t, brokertest.HarnessSessions)
			_, listed := h.B.GroupList(group)
			t.Logf("CONNACK 0x%02x; record writes refused %d; cursors %v; a list: %v",
				ack.ReasonCode, rs.refused.Load(), cursors, listed)
			if ack.ReasonCode != 0x83 || rs.refused.Load() == 0 {
				t.Fatalf("the CONNECT was answered 0x%02x with %d writes refused, want 0x83 for its refused record, "+
					"so this proves nothing", ack.ReasonCode, rs.refused.Load())
			}
			if _, ok := cursors[group]; ok || listed {
				t.Errorf("the refused CONNECT left the session it would have replaced a cursor (%v) or a list (%v)", ok, listed)
			}
		})
	}
}
