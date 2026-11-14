package core_test

import (
	"math"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// dueRow is an away session of client id client, whose expiry, a minute
// long, runs out at expires.
func dueRow(id int64, client string, expires time.Time) store.Session {
	return store.Session{ID: id, Client: client, Protocol: 5, Expiry: 60, Left: expires.Add(-time.Minute).UnixMilli(), ReceiveMaximum: 20}
}

// checkTimer fails the test unless the core's timer is set for at, Unix
// milliseconds, or for never where at is 0.
func checkTimer(t *testing.T, s *script, at int64) {
	t.Helper()
	got := s.core.Timer()
	if (at == 0 && !got.IsZero()) || (at != 0 && !got.Equal(time.UnixMilli(at))) {
		t.Errorf("Timer() = %v, want %v (0: never)", got, at)
	}
}

// design.md, Restart, Start: "The core loads [...] It also loads the next
// session expiry and Will due time still to come, but no session. It ends
// expired sessions. Listeners and bridges start last"; DISCONNECT: the core
// keeps "only the next session expiry [...] read from the store at start and
// at the end of each pass"; "Each time its timer comes, the core goes once
// through the sessions due, in expiry order. It then sets the timer for the
// next expiry still to come". The start's Fire reads due sessions at its own
// time, asking nothing of a connection; the core sets no timer until the
// answer, then sets it for Next, or for never where Next is 0 (store: "0 for
// none"). The timer's fire, which reads again, is spent: no timer until that
// read's answer.
func TestStartReadsDueSessions(t *testing.T) {
	for _, next := range []int64{0, epoch.Add(time.Hour).UnixMilli()} {
		s := newScript(t, newConfig(t))
		acts := s.fire()
		if len(acts) != 1 || acts[0].Conn != nil {
			t.Fatalf("the start's Fire: %d actions, want one read carrying no connection", len(acts))
		}
		due, ok := acts[0].Read.(*store.DueSessions)
		if !ok || due.At != s.now.UnixMilli() || len(due.Sessions) != 0 {
			t.Fatalf("the start's Fire read %+v, want DueSessions at %d", acts[0].Read, s.now.UnixMilli())
		}
		checkTimer(t, s, 0)
		due.Next = next
		s.reply(due, nil)
		checkTimer(t, s, next)
		if next == 0 {
			continue
		}
		s.now = time.UnixMilli(next)
		s.fire()
		only[*store.DueSessions](s)
		checkTimer(t, s, 0)
	}
}

// design.md, DISCONNECT: "When the expiry runs out, the delayed Will is
// published and the session deleted, in one transaction"; 0007d.md, 261016
// 093545: "each ending is one EndSession"; Store, Writes: "Publications,
// session saves and endings [...] may share a group"; metrics.md:
// saguin_sessions_ended_total{cause="expired"}. The timer's read finds two
// sessions due and a third not yet: each due one is ended by an EndSession
// of its own, the two members of the one group the read's answer sends,
// and counted once the store confirms it; the timer is then set for the
// third's expiry, the read's Next.
func TestDueSessionsEnd(t *testing.T) {
	later := epoch.Add(time.Hour)
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 3},
		dueRow(1, "a", epoch), dueRow(2, "b", epoch), dueRow(3, "c", later))
	s.fire()
	acts := s.answer(only[*store.DueSessions](s))
	if groups := groupsIn(acts); len(acts) != 1 || len(groups) != 1 || !slices.Equal(s.members(groups[0]), []string{"end a", "end b"}) {
		t.Fatalf("the due read's answer asked %+v, want one group of an EndSession per session due", acts)
	}
	s.expectEnded(0, 0, 0)
	s.settle()
	if _, ok := s.rows["c"]; len(s.rows) != 1 || !ok {
		t.Errorf("the store holds %v, want c's session alone", s.rows)
	}
	s.expectEnded(0, 2, 0)
	checkTimer(t, s, later.UnixMilli())
}

// design.md, Store, Requests: "Each session [...] keeps at most one request
// of its own in flight"; Store reads: a read's view holds the writes
// accepted before it, later writes "may then proceed while the read
// finishes", and "An ended or superseded request has no further effects";
// CONNECT: Session Present is 1 when "a session exists for the client id,
// either away". x, y and z are away, each due. The timer's read is sent and
// its view confirmed, listing all three; then x reconnects, its CONNECT
// finding its session run out (Session Present 0) and its new session stored,
// and y's CONNECT waits for its read, all before the timer's read is
// answered: only z is ended by the sweep. y's CONNECT then finds its session
// run out too, Session Present 0. Each ending is counted as z's is: three
// expired, each once.
func TestDueSessionsOfClientsInUseAreSkipped(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 3},
		dueRow(1, "x", epoch), dueRow(2, "y", epoch), dueRow(3, "z", epoch))
	s.fire()
	due := only[*store.DueSessions](s)
	if len(due.Sessions) != 3 {
		t.Fatalf("the timer's read lists %v, want x, y and z, each due", due.Sessions)
	}
	x := s.accept()
	s.receive(x, keeping(packet.V5, "x", 60), 0)
	s.answer(only[*store.LoadSession](s))
	if ack := connackIn(s.answer(only[*core.Group](s)), x); ack == nil || ack.SessionPresent {
		t.Fatalf("x's CONNACK %+v, want Session Present 0: its session ran out", ack)
	}
	y := s.accept()
	s.receive(y, keeping(packet.V5, "y", 60), 0)
	acts := s.answer(due)
	if got := writesIn(acts); len(got) != 1 || *got[0].(*store.EndSession) != (store.EndSession{ID: 3}) {
		t.Fatalf("the sweep wrote %v, want z's ending alone", got)
	}
	if ack := connackIn(s.settle(), y); ack == nil || ack.SessionPresent {
		t.Errorf("y's CONNACK %+v, want Session Present 0", ack)
	}
	s.expectRows()
	s.expectEnded(0, 3, 0)
}

// design.md, DISCONNECT, A lost connection: "When the expiry runs out, the
// delayed Will is published and the session deleted, in one transaction";
// "Each time its timer comes, the core goes once through the sessions
// due"; Store reads: "An ended or superseded request has no further
// effects"; Store, A full disk: "What a lost write would have changed comes
// back only from what the store still holds [...] The next sweep or retention
// pass may rediscover a due Will or session"; metrics.md:
// saguin_sessions_ended_total{cause="expired"}. x is away and due when the
// timer comes; x reconnects, and its read is outstanding when the sweep's
// read is answered, so the sweep leaves x to its CONNECT. The CONNECT then
// does not take the session: in the first case x's connection ends before its
// read is answered; in the second x reconnects with Clean Start 1, its read
// is answered, its connection ends, and its write, ending the old session, is
// refused for a full store, so the CONNECT is refused. The expiry still runs
// out: x's session is ended, by this pass or by a later one the timer starts,
// and counted as expired, once.
func TestDueSessionOfAnAbandonedReconnectEnds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		connect *packet.Connect
		abandon func(s *script, x *connection.Conn, load *store.LoadSession)
	}{
		{"its connection ends before its read is answered", keeping(packet.V5, "x", 60),
			func(s *script, x *connection.Conn, load *store.LoadSession) {
				s.end(x, nil, 0)
				s.answer(load)
			}},
		{"its write is refused for a full store", connectOf(packet.V5, "x"),
			func(s *script, x *connection.Conn, load *store.LoadSession) {
				s.answer(load)
				s.end(x, nil, 0)
				s.reply(only[store.Write](s), store.ErrFull)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 1}, dueRow(1, "x", epoch))
			s.fire()
			due := only[*store.DueSessions](s)
			x := s.accept()
			s.receive(x, tc.connect, 0)
			load := only[*store.LoadSession](s)
			s.answer(due)
			tc.abandon(s, x, load)
			s.settle()
			s.close(x)
			// The later passes: each at the time the core sets, at once where
			// already past; a full-store retry may come first.
			for range 3 {
				if _, held := s.rows["x"]; !held || s.core.Timer().IsZero() {
					break
				}
				at := s.core.Timer()
				if at.Before(s.now) {
					at = s.now
				}
				s.fireAt(at)
				s.settle()
			}
			if row, held := s.rows["x"]; held {
				t.Errorf("the store still holds x's session %+v, its expiry run out; Timer() = %v (zero: never)", row, s.core.Timer())
			}
			s.expectEnded(0, 1, 0)
		})
	}
}

// design.md, DISCONNECT: the core keeps "only the next session expiry [...]
// These are read from the store at start and at the end of each pass. They
// are brought forward when a session goes away", and "then sets the timer for
// the next expiry still to come", the read's Next, past by its answer or not;
// Store reads: "Later writes may then proceed while the read finishes", so
// the read's view does not hold a going away after it. Each case moves
// sessions away and back around one read, answers it with Next, and checks
// the time the timer is set for.
func TestTimerKeepsTheEarliestExpiry(t *testing.T) {
	const expiry = 100 // seconds
	for _, tc := range []struct {
		name string
		run  func(s *script) (next, want int64)
	}{
		{"a session gone away after the read is sent", func(s *script) (int64, int64) {
			conn := s.connected(keeping(packet.V5, "x", expiry))
			s.fire()
			s.end(conn, nil, 0)
			gone := s.now.Add(expiry * time.Second).UnixMilli()
			checkTimer(s.t, s, gone)
			s.answer(only[*store.LeaveSession](s))
			return gone + 1000, gone
		}},
		{"a session gone away and back before the read is sent", func(s *script) (int64, int64) {
			s.gone(s.connected(keeping(packet.V5, "x", expiry)))
			s.connect(s.accept(), keeping(packet.V5, "x", expiry), 0)
			s.fire()
			next := s.now.Add(time.Hour).UnixMilli()
			return next, next
		}},
		{"Next already past", func(s *script) (int64, int64) {
			s.fire()
			past := s.now.Add(-time.Second).UnixMilli()
			return past, past
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, newConfig(t))
			next, want := tc.run(s)
			due := only[*store.DueSessions](s)
			due.Next = next
			s.reply(due, nil)
			checkTimer(t, s, want)
		})
	}
}

// design.md, DISCONNECT: "For all away sessions together, the core keeps only
// the next session expiry and the next Will due time still to come. These are
// read from the store at start and at the end of each pass. They are brought
// forward when a session goes away"; Store reads: "Earlier accepted writes
// are resolved before its view is fixed. Later writes wait until the reader
// confirms [...] that view"; design.md, Execution, Decisions: a departure is
// an ordered input, and "a read's view" follows its stated rules. A durable
// session x goes away after the sweep's read has entered the store-request
// order but before its view is fixed, its going-away write entering the order
// behind the read: the view does not hold x as away, so neither the read's
// sessions nor its Next carry x's expiry, and once everything is answered the
// timer is still set for it. In the first case the read waits behind another
// client's outstanding publish group when x goes away; in the second, x's own
// CONNECT's session save is outstanding when x goes away, its going-away
// write waiting in its lane, and the timer fires then.
func TestTimerKeepsAnExpiryTheReadsViewMisses(t *testing.T) {
	const expiry = 100 // seconds
	for _, tc := range []struct {
		name string
		run  func(s *script) (gone int64)
	}{
		{"gone away while the read waits behind a group", func(s *script) int64 {
			p := s.connected(connectOf(packet.V5, "p"))
			x := s.connected(keeping(packet.V5, "x", expiry))
			s.publish(p, 1, 20)
			publication := only[*core.Group](s)
			s.fire()
			if reads := pending[*store.DueSessions](s); len(reads) != 0 {
				s.t.Fatalf("the sweep's read was sent with a group outstanding: %+v", reads)
			}
			s.end(x, nil, 0)
			gone := s.now.Add(expiry * time.Second).UnixMilli()
			checkTimer(s.t, s, gone)
			s.answer(publication)
			return gone
		}},
		{"gone away with its going-away write in its lane", func(s *script) int64 {
			x := s.accept()
			s.receive(x, keeping(packet.V5, "x", expiry), 0)
			s.answer(only[*store.LoadSession](s))
			save := only[*core.Group](s)
			s.end(x, nil, 0)
			gone := s.now.Add(expiry * time.Second).UnixMilli()
			s.fire()
			s.answer(save)
			return gone
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScriptOn(t, newConfig(t), &broadcast)
			gone := tc.run(s)
			due := only[*store.DueSessions](s)
			if len(due.Sessions) != 0 || due.Next != 0 {
				t.Fatalf("the read's view holds %+v, Next %d; want none: x's going away is behind the read", due.Sessions, due.Next)
			}
			leave := only[*store.LeaveSession](s)
			s.answer(leave)
			s.answer(due)
			s.settle()
			checkTimer(t, s, gone)
		})
	}
}

// 0007d.md, 261016 093545: "between reads the kept expiry only comes
// earlier"; design.md, DISCONNECT: "brought forward when a session goes
// away". After a read sets the timer, a session going away with a later
// expiry leaves it as it is, and one with an earlier expiry brings it
// forward.
func TestTimerOnlyComesEarlierBetweenReads(t *testing.T) {
	s := newScript(t, newConfig(t))
	late, early := s.connected(keeping(packet.V5, "late", 7200)), s.connected(keeping(packet.V5, "early", 60))
	s.fire()
	due := only[*store.DueSessions](s)
	next := s.now.Add(time.Hour).UnixMilli()
	due.Next = next
	s.reply(due, nil)
	s.gone(late)
	checkTimer(t, s, next)
	s.end(early, nil, 0)
	gone := s.now.Add(time.Minute).UnixMilli()
	s.settle()
	checkTimer(t, s, gone)
}

// design.md, Flows, DISCONNECT, A lost connection: "Expiry-timer fires
// that come while a pass is unfinished, however many, start one more pass
// when it ends, at the time of the event that ends it" (decided from item
// 0007d); Store, Writes: a batch's endings share one group. A batch ended
// with no fire during it asks for no read; two fires during a second batch
// ask for nothing, and the answer to its endings' group, which ends the
// batch, sends one read, at its time.
func TestFireDuringABatchIsOneFlag(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 4},
		dueRow(1, "a", epoch), dueRow(2, "b", epoch))
	s.fire()
	s.answer(only[*store.DueSessions](s))
	for _, a := range s.settle() {
		if a.Read != nil {
			t.Fatalf("a batch ended with no fire during it read %+v, want no read", a.Read)
		}
	}
	for id, client := range map[int64]string{3: "c", 4: "d"} {
		s.rows[client] = dueRow(id, client, s.now)
		s.clients[id] = client
	}
	s.fire()
	s.answer(only[*store.DueSessions](s))
	for range 2 {
		if acts := s.fire(); len(acts) != 0 {
			t.Fatalf("a fire during a batch asked %d things, want none", len(acts))
		}
	}
	acts := s.answer(only[*core.Group](s))
	if reads := pending[*store.DueSessions](s); len(reads) != 1 || len(acts) != 1 || reads[0].At != s.now.UnixMilli() {
		t.Fatalf("the batch's endings answered: %d actions, reads %+v; want one read, DueSessions at %d", len(acts), reads, s.now.UnixMilli())
	}
}

// refuseEndings answers the one outstanding group, the endings of sessions
// due, full, as the store does when it has no room for them (design.md,
// Store, Writes: a full refusal "answers every member 'full'"), and returns
// the time the core took that answer. The answer asks the store nothing:
// "Failed write bodies are not retained" (Store), so no ending is sent again
// before the retry. Its one action is a log line where first says the store
// first refuses for lack of room, none otherwise (Flows, Logging, What the
// broker logs of its own life: "A full disk: one WARN line when the store
// first refuses for lack of room, and one INFO line when it has room
// again").
func refuseEndings(t *testing.T, s *script, first bool) time.Time {
	t.Helper()
	g := only[*core.Group](s)
	for _, w := range g.Writes {
		if _, ok := w.(*store.EndSession); !ok {
			t.Fatalf("the group refused holds a %T, want session endings alone", w)
		}
	}
	acts := s.reply(g, store.ErrFull)
	lines := linesIn(acts)
	warned := len(acts) == 1 && len(lines) == 1 && acts[0] == core.Action{Line: lines[0]} && lines[0].Level == logger.Warn
	switch {
	case first && !warned:
		t.Fatalf("the store's first full answer asked %d things, its log lines %+v; want one WARN line alone, nothing else until the retry", len(acts), lines)
	case !first && len(acts) != 0:
		t.Fatalf("a full answer after the first asked %d things, its log lines %+v; want nothing until the retry", len(acts), lines)
	}
	return s.now
}

// expectRediscovery fires the core's timer at at, a retry deadline, and
// returns the one request the fire sends: a read of the sessions due at at,
// from the earliest (design.md, Store: "At the deadline, the core
// rediscovers due Wills, session endings and retention work from the
// store"; "Failed write bodies are not retained": no ending is sent again
// from memory before that read).
func expectRediscovery(t *testing.T, s *script, at time.Time) *store.DueSessions {
	t.Helper()
	s.fireAt(at)
	if ends := pending[*store.EndSession](s); len(ends) != 0 {
		t.Fatalf("the retry sent %d endings, want none before the store is read afresh", len(ends))
	}
	due := only[*store.DueSessions](s)
	if len(s.outstanding) != 1 || due.At != at.UnixMilli() || due.After != (store.DueSession{}) {
		t.Fatalf("the retry asked %d things, a DueSessions at %d after %+v; want one read, of the sessions due at %d, from the earliest",
			len(s.outstanding), due.At, due.After, at.UnixMilli())
	}
	return due
}

// design.md, Store: "A housekeeping write refused for room sets a retry
// deadline one second after that answer"; "At the deadline, the core
// rediscovers due Wills, session endings and retention work from the
// store"; "Failed write bodies are not retained"; DISCONNECT: "When the
// expiry runs out, [...] the session deleted"; metrics.md:
// saguin_sessions_ended_total{cause="expired"}. A due session's ending is
// answered full at t: nothing is counted, and the core's timer is set for
// t + 1 s. Fired then, the core sends no ending from memory: it reads the
// sessions due afresh, finds the session still stored, and sends a new
// ending, which, saved, ends it, counted as expired.
func TestEndingRefusedForRoomIsRetried(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 1}, dueRow(1, "a", epoch))
	s.fire()
	s.answer(only[*store.DueSessions](s))
	retry := refuseEndings(t, s, true).Add(time.Second)
	s.expectEnded(0, 0, 0)
	checkTimer(t, s, retry.UnixMilli())
	acts := s.answer(expectRediscovery(t, s, retry))
	if got := writesIn(acts); len(got) != 1 || !reflect.DeepEqual(got[0], &store.EndSession{ID: 1}) {
		t.Fatalf("the retry's read, finding a's session due, wrote %v; want its ending alone", got)
	}
	s.settle()
	s.expectRows()
	if _, ok := s.rows["a"]; ok {
		t.Errorf("the store still holds a's session after the retry ended it")
	}
	s.expectEnded(0, 1, 0)
}

// design.md, Store: "A housekeeping write refused for room sets a retry
// deadline one second after that answer. An existing earlier retry is not
// postponed"; DISCONNECT: "Each time its timer comes, the core goes once
// through the sessions due". a is due, and b's expiry comes 0.6 s later.
// a's ending is answered full at t, and the timer is set for b's expiry,
// the earlier deadline; when it comes, the pass finds a and b due, and
// their endings are answered full at t + 0.5 s: the timer stays set for
// t + 1 s, the first refusal's retry.
func TestEarlierRetryIsNotPostponed(t *testing.T) {
	const tick, half = 100 * time.Millisecond, 500 * time.Millisecond
	expires := epoch.Add(tick + half) // b's: the second refusal comes two events after it
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 2}, dueRow(1, "a", epoch), dueRow(2, "b", expires))
	s.tick = tick
	s.fire()
	s.answer(only[*store.DueSessions](s))
	refused := refuseEndings(t, s, true)
	checkTimer(t, s, expires.UnixMilli())
	s.fireAt(expires)
	due := only[*store.DueSessions](s)
	acts := s.answer(due)
	if got := writesIn(acts); len(got) != 2 {
		t.Fatalf("b's expiry's pass, reading %v, wrote %v; want a's ending and b's", due.Sessions, got)
	}
	if again := refuseEndings(t, s, false); !again.Equal(refused.Add(half)) {
		t.Fatalf("the second refusal came at %v, want %v, half a second after the first", again, refused.Add(half))
	}
	checkTimer(t, s, refused.Add(time.Second).UnixMilli())
}

// design.md, Store: "Another full refusal schedules the next retry"; "At
// the deadline, the core rediscovers due Wills, session endings and
// retention work from the store". A due session's ending is answered full;
// at the retry the core reads afresh and sends a new ending, answered full
// again at u: the timer is set for u + 1 s, and that retry too reads the
// sessions due afresh, its ending then saved, the session counted as
// expired once.
func TestRefusalAtTheRetrySchedulesTheNext(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 1}, dueRow(1, "a", epoch))
	s.fire()
	s.answer(only[*store.DueSessions](s))
	retry := refuseEndings(t, s, true).Add(time.Second)
	s.answer(expectRediscovery(t, s, retry))
	next := refuseEndings(t, s, false).Add(time.Second)
	checkTimer(t, s, next.UnixMilli())
	s.answer(expectRediscovery(t, s, next))
	s.settle()
	s.expectEnded(0, 1, 0)
}

// design.md, Store: "Stop cancels the retry"; Restart, Graceful stop:
// "once the stop begins, the core's timer starts no housekeeping (expired
// sessions and due Wills are handled at the next start); the graceful-stop
// cutoff still runs", and "after limits.write_timeout from when the core
// accepts the stop, the remaining sockets are cut". A due session's ending
// is answered full, setting a retry a second later; the stop then begins,
// before it: the core's timer is set for the stop's cutoff alone,
// write_timeout after the stop began, and fired at the retry's time the
// core asks the store nothing.
func TestStopCancelsTheRetry(t *testing.T) {
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 1}, dueRow(1, "a", epoch))
	s.tick = 100 * time.Millisecond // the stop begins before the retry
	s.fire()
	s.answer(only[*store.DueSessions](s))
	retry := refuseEndings(t, s, true).Add(time.Second)
	checkTimer(t, s, retry.UnixMilli())
	s.stop()
	cutoff := s.now.Add(s.cfg.Broker.Limits.WriteTimeout)
	if got := s.core.Timer(); !got.Equal(cutoff) {
		t.Errorf("Timer() after the stop began = %v, want the stop's cutoff, %v, the retry cancelled", got, cutoff)
	}
	if acts := s.fireAt(retry); len(acts) != 0 || len(s.outstanding) != 0 {
		t.Errorf("fired at the cancelled retry's time, the core asked %d things, %d requests outstanding; want nothing", len(acts), len(s.outstanding))
	}
}

// design.md, DISCONNECT: "Each time its timer comes, the core goes once
// through the sessions due, in expiry order. It then sets the timer for the
// next expiry still to come"; Store: "A housekeeping write refused for room
// sets a retry deadline one second after that answer. An existing earlier
// retry is not postponed. At the deadline, the core rediscovers [...] session
// endings [...] from the store" (audit finding 261016-003a; 003a.md, 261016
// 134250: every read started again from the earliest); Store, Writes: a full
// refusal "answers every member 'full'". DueBatch+2 sessions are due, one is
// not, and every group of endings is answered full. One ring of the timer
// sends each due session's ending exactly once, each read starting after the
// last session the pass handled; at the pass's end, nothing is outstanding
// and the timer is set for the first refusal's retry, already past. When it
// rings, the pass starts again from the earliest, finds the sessions the full
// disk kept, and ends them; the timer is then set for the session not due.
func TestOnePassThroughTheSessionsDue(t *testing.T) {
	n := store.DueBatch + 2
	rows := make([]store.Session, 0, n+1)
	for id := range int64(n) {
		rows = append(rows, dueRow(id+1, "c"+strconv.FormatInt(id+1, 10), epoch.Add(-time.Duration(id+1)*time.Millisecond)))
	}
	later := epoch.Add(time.Hour)
	rows = append(rows, dueRow(int64(n+1), "later", later))
	s := newScriptOn(t, newConfig(t), &store.State{HighestSession: int64(n + 1)}, rows...)
	s.fire()
	ended := map[int64]int{}
	var after store.DueSession
	var retry time.Time // the first refusal's
	reads := 0
	for len(s.outstanding) != 0 {
		if due := pending[*store.DueSessions](s); len(due) != 0 {
			if reads == 2 {
				t.Fatalf("a third read in one pass, after %+v", due[0].After)
			}
			if due[0].After != after {
				t.Fatalf("read %d starts after %+v, want after %+v, the last session the pass handled", reads+1, due[0].After, after)
			}
			reads++
			s.fill(due[0]) // the answer is the core's once handed over
			if len(due[0].Sessions) != 0 {
				after = due[0].Sessions[len(due[0].Sessions)-1]
			}
			s.reply(due[0], nil)
			continue
		}
		for _, g := range pending[*core.Group](s) {
			for _, w := range g.Writes {
				end, ok := w.(*store.EndSession)
				if !ok {
					t.Fatalf("the pass wrote a %T, want endings alone", w)
				}
				ended[end.ID]++
				if ended[end.ID] > 1 {
					t.Fatalf("session %d's ending sent again in one pass", end.ID)
				}
			}
			s.reply(g, store.ErrFull)
			if retry.IsZero() {
				retry = s.now.Add(time.Second)
			}
		}
	}
	if reads != 2 || len(ended) != n {
		t.Errorf("the pass made %d reads and ended %d sessions, want 2, and each of the %d due once", reads, len(ended), n)
	}
	checkTimer(t, s, retry.UnixMilli())

	s.fire() // the retry is past: the core's timer rings at once
	again := only[*store.DueSessions](s)
	if again.After != (store.DueSession{}) {
		t.Fatalf("the retry's read starts after %+v, want from the earliest", again.After)
	}
	s.fill(again)
	if len(again.Sessions) == 0 || again.Sessions[0].ID != int64(n) {
		t.Fatalf("the retry's pass found %v first, want session %d, the earliest, still stored", again.Sessions, n)
	}
	s.reply(again, nil)
	s.settle()
	s.expectEnded(0, float64(n), 0)
	checkTimer(t, s, later.UnixMilli())
	s.expectRows()
}

// design.md, Store, Requests: "Each session [...] keeps at most one request
// of its own in flight"; CONNECT, 3: "While an earlier CONNECT for the same
// id waits for the store, this one waits too"; Session Present is 1 when
// "a session exists for the client id, either away"; DISCONNECT: "When the
// expiry runs out, [...] the session deleted"; Store reads: "An ended or
// superseded request has no further effects". A reconnect for a client whose
// session the sweep has found due is ordered with its ending: in each order,
// the client's CONNECT, keeping its session, gets Session Present 0 (its
// session ran out before it), the session's ending is counted once, and the
// store then holds the client's new session.
func TestReconnectRacesItsExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		race func(s *script, reconnect func())
	}{
		{"the ending first", func(s *script, reconnect func()) {
			s.answer(only[*store.DueSessions](s))
			reconnect()
			if len(pending[*store.LoadSession](s)) != 0 {
				s.t.Fatalf("the CONNECT read its session while its ending is outstanding")
			}
		}},
		{"the CONNECT first", func(s *script, reconnect func()) {
			reconnect()
			s.answer(only[*store.DueSessions](s))
			if ends := pending[*store.EndSession](s); len(ends) != 0 {
				s.t.Fatalf("the sweep ended the session of a client whose CONNECT waits for the store")
			}
		}},
		{"the CONNECT's read answered first", func(s *script, reconnect func()) {
			reconnect()
			s.answer(only[*store.LoadSession](s))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScriptOn(t, newConfig(t), &store.State{HighestSession: 1}, dueRow(1, "x", epoch))
			s.fire()
			conn := s.accept()
			tc.race(s, func() { s.receive(conn, keeping(packet.V5, "x", 60), 0) })
			ack := connackIn(s.settle(), conn)
			if ack == nil || ack.ReasonCode != 0 || ack.SessionPresent {
				t.Fatalf("CONNACK %+v, want 0x00, Session Present 0", ack)
			}
			s.expectRows()
			s.expectEnded(0, 1, 0)
		})
	}
}

// design.md, DISCONNECT: "A granted Session Expiry of 4,294,967,295 means
// never expires. It has no expiry deadline."; MQTT 5 3.1.2.11.2: "If the
// Session Expiry Interval is 0xFFFFFFFF (UINT_MAX), the Session does not
// expire."; CONNECT: Session Present is 1 when the CONNECT keeps its
// session and "a session exists for the client id, either away". No client
// can wait as long as a finite reading of that expiry, so the core is driven
// at fake times. With max_session_expiry at 4,294,967,295 seconds, a client
// granted that expiry goes away after the start's read: the core's timer
// stays at never. A CONNECT keeping its session then loads its row past the
// instant its left time plus that many seconds would give, and resumes it.
func TestNeverExpiringSessionHasNoDeadline(t *testing.T) {
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxSessionExpiry = math.MaxUint32 * time.Second
	s := newScript(t, cfg)
	s.fire()
	s.answer(only[*store.DueSessions](s))
	s.gone(s.connected(keeping(packet.V5, "forever", math.MaxUint32)))
	checkTimer(t, s, 0)
	row := s.rows["forever"]
	s.now = time.UnixMilli(testkit.ExpiryAt(row.Left, row.Expiry)) // the next event comes a tick past it
	ack, _ := s.connect(s.accept(), keeping(packet.V5, "forever", math.MaxUint32), 0)
	if ack.ReasonCode != 0 || !ack.SessionPresent {
		t.Errorf("CONNACK 0x%02X, Session Present %v at %v, want 0x00 and 1: the session never expires", ack.ReasonCode, ack.SessionPresent, s.now)
	}
}
