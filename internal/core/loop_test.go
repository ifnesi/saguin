package core_test

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// These tests run the core's loop (Run) on its own order, in a synctest
// bubble with a scripted store and a stepping clock (stepClock): what a
// client sees of it is the conformance tests'.

// stepClock is the loop's clock in a synctest bubble: Now waits for the
// test to give each event's time, so the test sees every decision on its
// own; At and Timer hand the test each timer the loop makes, and each
// time it moves one, to fire when it chooses. Run reads the clock on its
// goroutine while the test reads and fires the timers on its own, hence
// the mutex: this is test code, not the broker's (R3).
type stepClock struct {
	times  chan time.Time
	mu     sync.Mutex
	timers []*stepTimer
}

// stepTimer is a timer the loop made: the times it was set for, the first
// at its making and one more for each move, the zero time where it was
// stopped, and its channel.
type stepTimer struct {
	sets []time.Time
	c    chan time.Time
}

// at is the time the timer is set for now.
func (t *stepTimer) at() time.Time { return t.sets[len(t.sets)-1] }

// Now returns the next time the test gives.
func (c *stepClock) Now() time.Time { return <-c.times }

// At makes a timer for t and returns its channel.
func (c *stepClock) At(t time.Time) <-chan time.Time {
	ch, _ := c.Timer(t)
	return ch
}

// Timer makes a timer for t and returns its channel and the function that
// moves it to another time, or stops it at the zero time, dropping a fire
// the loop has not taken.
func (c *stepClock) Timer(t time.Time) (ch <-chan time.Time, reset func(time.Time)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &stepTimer{sets: []time.Time{t}, c: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	return timer.c, func(t time.Time) {
		c.mu.Lock()
		defer c.mu.Unlock()
		timer.sets = append(timer.sets, t)
		select {
		case <-timer.c:
		default:
		}
	}
}

// timer returns the timer the loop made last, failing the test where it
// has made none.
func (c *stepClock) timer(t *testing.T) stepTimer {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers) == 0 {
		t.Fatalf("the loop set no timer")
	}
	last := c.timers[len(c.timers)-1]
	return stepTimer{sets: slices.Clone(last.sets), c: last.c}
}

// made returns the times each timer the loop made was set for, in the
// order it made them.
func (c *stepClock) made() [][]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	sets := make([][]time.Time, len(c.timers))
	for i, timer := range c.timers {
		sets[i] = slices.Clone(timer.sets)
	}
	return sets
}

// loop is a core's Run in a synctest bubble, with a stepping clock, the
// store's channels, sized as the store's (store.Credit), and the logger's,
// sized as its (logger.Capacity), which the test plays, and the log
// admission main hands the core (logger.Credits).
type loop struct {
	t       *testing.T
	inbound chan connection.Event
	answers chan store.Answer
	writes  chan store.Write
	reads   chan store.Read
	lines   chan logger.Line
	results chan logger.Result
	credits *logger.Credits
	stop    chan struct{}
	done    chan struct{}
	clock   *stepClock
}

// runLoop starts a new core's Run, with inbound already holding events.
// The test's cleanup stops it.
func runLoop(t *testing.T, events ...connection.Event) *loop {
	l := newLoop(t)
	for _, ev := range events {
		l.inbound <- ev
	}
	l.start()
	return l
}

// newLoop returns a loop whose Run has not started.
func newLoop(t *testing.T) *loop {
	return &loop{
		t: t, inbound: make(chan connection.Event, 64), answers: make(chan store.Answer, store.Credit),
		writes: make(chan store.Write, store.Credit), reads: make(chan store.Read, store.Credit),
		lines: make(chan logger.Line, logger.Capacity), results: make(chan logger.Result, logger.Capacity),
		credits: logger.NewCredits(logger.Info, metrics.New()),
		stop:    make(chan struct{}), done: make(chan struct{}), clock: &stepClock{times: make(chan time.Time)},
	}
}

// start starts a new core's Run on the loop's channels, clock and log
// admission. The test's cleanup stops it.
func (l *loop) start() {
	l.run(core.New(newConfig(l.t), metrics.New(), &store.State{}, l.writes, l.reads, l.lines, l.credits))
}

// run starts c's Run on the loop's channels and clock. The test's cleanup
// stops it.
func (l *loop) run(c *core.Core) { l.runOn(c, l.clock) }

// runOn starts c's Run on the loop's channels and on clock. The test's
// cleanup stops it.
func (l *loop) runOn(c *core.Core, clock connection.Clock) {
	go func() {
		c.Run(l.inbound, l.answers, l.results, l.stop, clock)
		close(l.done)
	}()
	l.t.Cleanup(l.end)
}

// decide gives the loop's next decision its time, at, then waits until
// the loop waits again.
func (l *loop) decide(at time.Time) {
	l.t.Helper()
	select {
	case l.clock.times <- at:
	case <-l.done:
		l.t.Fatalf("the loop has returned, want it deciding")
	}
	synctest.Wait()
}

// deciding reports whether the loop waits for a decision's time, having
// taken an event, rather than for an event.
func (l *loop) deciding() bool {
	synctest.Wait()
	select {
	case l.clock.times <- epoch:
		synctest.Wait()
		return true
	default:
		return false
	}
}

// end stops the loop, giving it a time while it waits for one, and waits
// for Run to return.
func (l *loop) end() {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	for {
		select {
		case <-l.done:
			return
		case l.clock.times <- epoch:
		}
	}
}

// answerRead hands the loop the store's two answers to read, as a reader
// gives them (design.md, Store reads): the confirmation that its view is
// established, then its result; and gives each decision its time, at.
func (l *loop) answerRead(read store.Read, at time.Time) {
	l.t.Helper()
	l.answers <- store.Answer{Op: read, View: true}
	l.decide(at)
	l.answers <- store.Answer{Op: read}
	l.decide(at)
}

// sent returns the store requests the loop sent since the last call, in
// order: reads, then writes.
func (l *loop) sent() (reads []store.Read, writes []store.Write) {
	for {
		select {
		case r := <-l.reads:
			reads = append(reads, r)
		case w := <-l.writes:
			writes = append(writes, w)
		default:
			return reads, writes
		}
	}
}

// startRead returns the loop's first act, which must be its start's read,
// alone: design.md, Restart, Start: "The core loads [...] It also loads the
// next session expiry and Will due time still to come, but no session. It
// ends expired sessions. Listeners and bridges start last".
func startRead(t *testing.T, l *loop) *store.DueSessions {
	t.Helper()
	reads, writes := l.sent()
	if len(reads) != 1 || len(writes) != 0 {
		t.Fatalf("Run's first act sent reads %v and writes %v, want the start's DueSessions alone", reads, writes)
	}
	due, ok := reads[0].(*store.DueSessions)
	if !ok {
		t.Fatalf("Run's first act read %+v, want DueSessions", reads[0])
	}
	return due
}

// accepted is a new connection's Accepted, which asks nothing of the
// store or of the connection.
func accepted() connection.Event {
	return connection.Event{Kind: connection.Accepted, Conn: new(connection.Conn),
		Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 50000}}}
}

// connectOn is conn's CONNECT for clientID, Clean Start 1, which the core
// answers only after reading its session.
func connectOn(conn *connection.Conn, clientID string) connection.Event {
	return connection.Event{Kind: connection.Received, Conn: conn, Packet: connectOf(packet.V5, clientID)}
}

// design.md, Restart, Start: the core loads "the next session expiry [...]
// still to come" and "ends expired sessions; listeners and bridges start
// last"; DISCONNECT: "Each time its timer comes, the core goes once through
// the sessions due, in expiry order. It then sets the timer for the next
// expiry still to come"; Testing: the loop "owns the one real timer". With a
// CONNECT already waiting, Run's first act is the start's read, at its first
// time; the CONNECT's read follows. The read's answer sets the timer for its
// Next; when the timer comes, Run reads again, at that decision's time.
func TestRunReadsDueSessionsAtStartAndOnItsTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ev := accepted()
		l := runLoop(t, ev, connectOn(ev.Conn, "x"))
		t0 := epoch.Add(time.Second)
		l.decide(t0)
		start := startRead(t, l)
		if start.At != t0.UnixMilli() {
			t.Fatalf("Run's first act read DueSessions at %d, want %d", start.At, t0.UnixMilli())
		}
		l.decide(t0)
		l.decide(t0)
		if reads, _ := l.sent(); len(reads) != 1 {
			t.Fatalf("the CONNECT read %v, want its LoadSession", reads)
		} else if load, ok := reads[0].(*store.LoadSession); !ok || load.Client != "x" {
			t.Fatalf("the CONNECT read %+v, want its LoadSession", reads[0])
		}

		next := t0.Add(time.Minute)
		start.Next = next.UnixMilli()
		l.answerRead(start, t0)
		timer := l.clock.timer(t)
		if !timer.at().Equal(next) {
			t.Fatalf("the timer set for %v, want the read's Next, %v", timer.at(), next)
		}
		timer.c <- next
		t1 := next.Add(time.Millisecond)
		l.decide(t1)
		if reads, _ := l.sent(); len(reads) != 1 {
			t.Fatalf("the timer read %v, want DueSessions", reads)
		} else if due, ok := reads[0].(*store.DueSessions); !ok || due.At != t1.UnixMilli() {
			t.Fatalf("the timer read %+v, want DueSessions at %d", reads[0], t1.UnixMilli())
		}
	})
}

// design.md, Testing: "The loop around the core carries that out and owns the
// one real timer"; DISCONNECT: the core keeps "the next session expiry [...]
// read from the store at start"; Store: "A housekeeping write refused for
// room sets a retry deadline one second after that answer". The start's read,
// answered with one session due and Next an hour away, sets the timer for
// that hour; the session's ending, answered full, brings the deadline forward
// to the retry, a second after that answer. The loop has made one timer and
// moved it, and when it fires at the retry the loop reads the sessions due at
// that decision's time.
func TestRunMovesItsOneTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := runLoop(t)
		l.decide(epoch)
		start := startRead(t, l)
		start.Sessions = []store.DueSession{{ID: 1, Client: "x"}}
		hour := epoch.Add(time.Hour)
		start.Next = hour.UnixMilli()
		l.answerRead(start, epoch)
		_, writes := l.sent()
		if len(writes) != 1 {
			t.Fatalf("the start's batch wrote %v, want x's ending", writes)
		}
		l.answers <- store.Answer{Op: writes[0], Err: store.ErrFull}
		l.decide(epoch)
		retry := epoch.Add(time.Second)
		if made := l.clock.made(); len(made) != 1 || !slices.EqualFunc(made[0], []time.Time{hour, retry}, time.Time.Equal) {
			t.Fatalf("the loop made timers set for %v, want one timer, set for %v and moved to %v", made, hour, retry)
		}
		l.clock.timer(t).c <- retry
		l.decide(retry)
		if reads, _ := l.sent(); len(reads) != 1 {
			t.Fatalf("the moved timer's fire read %v, want DueSessions", reads)
		} else if due, ok := reads[0].(*store.DueSessions); !ok || due.At != retry.UnixMilli() {
			t.Fatalf("the moved timer's fire read %+v, want DueSessions at %d", reads[0], retry.UnixMilli())
		}
	})
}

// design.md, Testing: "The loop around the core carries that out and owns the
// one real timer"; "The busy path is anything run per packet, per message or
// per connection; it does no needless work"; DISCONNECT: "Each time its timer
// comes, the core goes once through the sessions due [...] It then sets the
// timer for the next expiry still to come". The start's read sets the timer
// for its Next, an hour away; when it comes, the loop reads the sessions due
// and the core has no deadline until that read's answer: the loop stops its
// one timer (moves it to the zero time), never into the past. The read's
// answer, Next two hours away, moves the same timer again, and when it fires
// the loop reads the sessions due at that decision's time.
func TestRunStopsItsTimerWithNoDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := runLoop(t)
		l.decide(epoch)
		start := startRead(t, l)
		hour, twoHours := epoch.Add(time.Hour), epoch.Add(2*time.Hour)
		start.Next = hour.UnixMilli()
		l.answerRead(start, epoch)
		l.clock.timer(t).c <- hour
		l.decide(hour)
		reads, _ := l.sent()
		if len(reads) != 1 {
			t.Fatalf("the timer's fire read %v, want DueSessions", reads)
		}
		due, ok := reads[0].(*store.DueSessions)
		if !ok {
			t.Fatalf("the timer's fire read %+v, want DueSessions", reads[0])
		}
		if made := l.clock.made(); len(made) != 1 || !slices.EqualFunc(made[0], []time.Time{hour, {}}, time.Time.Equal) {
			t.Fatalf("the loop made timers set for %v, want one timer, set for %v and stopped (the zero time)", made, hour)
		}
		due.Next = twoHours.UnixMilli()
		l.answerRead(due, hour)
		if made := l.clock.made(); len(made) != 1 || !slices.EqualFunc(made[0], []time.Time{hour, {}, twoHours}, time.Time.Equal) {
			t.Fatalf("the loop made timers set for %v, want one timer, set for %v, stopped, then moved to %v", made, hour, twoHours)
		}
		l.clock.timer(t).c <- twoHours
		l.decide(twoHours)
		if reads, _ := l.sent(); len(reads) != 1 {
			t.Fatalf("the moved timer's fire read %v, want DueSessions", reads)
		} else if again, ok := reads[0].(*store.DueSessions); !ok || again.At != twoHours.UnixMilli() {
			t.Fatalf("the moved timer's fire read %+v, want DueSessions at %d", reads[0], twoHours.UnixMilli())
		}
	})
}

// The five things the loop takes from, in R2's order.
const (
	fromStop = iota
	fromAnswers
	fromResults
	fromTimer
	fromReaders
)

// sourceNames names the five for subtests.
var sourceNames = [...]string{"stop", "store answers", "logger results", "the timer", "the readers' channel"}

// design.md, Execution, Event order: "When several events are ready, the core
// takes them in this order: stop, store answers, logger results, the core's
// timer, then the readers' channel"; Execution, Decisions: "Go's choice
// between ready cases decide[s] none of them". For each pair: the loop has a
// store answer it can take (an ending of the start's batch), a logger result
// it can take (for a line main admitted before the core started, its credit
// passed with admission: design.md, Flows, Logging), a timer set (the batch's
// Next) and an event on the readers' channel to come; while it decides an
// event, the test readies the pair, then lets it go on. When it next waits,
// it has taken the first of the pair, and on stop returned; it has left the
// second, waiting for the time of its decision on the first. A logger
// result's processing needs no time (design.md, Flows, Logging: "An admitted
// line holds its credit until the core processes its output-write result";
// design.md, Execution, Decisions: time observations are inputs only where
// "specified by their flows"), so after one the loop may go on to the second:
// the order shows in the result being gone when the loop first waits, as the
// second, taken first, would have held the loop at its decision with the
// result left.
func TestRunTakesInOrder(t *testing.T) {
	for first := fromStop; first <= fromReaders; first++ {
		for second := first + 1; second <= fromReaders; second++ {
			t.Run(sourceNames[first]+" before "+sourceNames[second], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					takeInOrder(t, first, second)
				})
			})
		}
	}
}

// takeInOrder is one pair of TestRunTakesInOrder.
func takeInOrder(t *testing.T, first, second int) {
	l := newLoop(t)
	if !l.credits.Admit(logger.Error) {
		t.Fatal("main's line before the core starts was not admitted")
	}
	l.start()
	l.decide(epoch)
	start := startRead(t, l)
	start.Sessions = []store.DueSession{{ID: 1, Client: "x"}}
	start.Next = epoch.Add(time.Hour).UnixMilli()
	l.answerRead(start, epoch)
	_, writes := l.sent()
	if len(writes) != 1 {
		t.Fatalf("the start's batch wrote %v, want x's ending", writes)
	}
	timer := l.clock.timer(t)

	l.inbound <- accepted()
	synctest.Wait() // the loop has taken it, and waits for its time
	pending := map[int]func() int{
		fromAnswers: func() int { return len(l.answers) },
		fromResults: func() int { return len(l.results) },
		fromTimer:   func() int { return len(timer.c) },
		fromReaders: func() int { return len(l.inbound) },
	}
	for _, source := range []int{first, second} {
		switch source {
		case fromStop:
			close(l.stop)
		case fromAnswers:
			l.answers <- store.Answer{Op: writes[0]}
		case fromResults:
			l.results <- logger.Result{}
		case fromTimer:
			timer.c <- timer.at()
		case fromReaders:
			l.inbound <- accepted()
		}
	}
	l.decide(epoch)
	if first == fromStop {
		select {
		case <-l.done:
		default:
			t.Fatalf("stop ready with %s: the loop did not return", sourceNames[second])
		}
	} else if pending[first]() != 0 {
		t.Fatalf("the loop left %s for %s", sourceNames[first], sourceNames[second])
	}
	if first != fromResults && pending[second]() != 1 {
		t.Fatalf("the loop took %s before %s", sourceNames[second], sourceNames[first])
	}
}

// Store, Requests: "Each dispatched read and each dispatched write group
// consumes one store credit until its answer is processed"; Execution,
// Communication, the readers' channel: "The core does not take from the
// channel while it has no store credit"; "the core stops taking packets and
// the clients slow, while timers and answers carry on"; 0007a.md, 261009
// 211151: "while intake is paused for lack of credit, store answers are still
// taken and intake resumes". The start's read and a CONNECT's read for each
// of store.Credit-1 clients spend every credit: an event waiting on the
// readers' channel is not taken, while the store's answers to the start's
// read are: the confirmation of its view, after which the event still waits,
// and its result, which gives the credit back that lets the loop take the
// event.
func TestRunPausesIntakeWithoutCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := runLoop(t)
		l.decide(epoch)
		reads := make([]store.Read, 0, store.Credit)
		reads = append(reads, startRead(t, l))
		for i := range store.Credit - 1 {
			ev := accepted()
			l.inbound <- ev
			l.inbound <- connectOn(ev.Conn, "c"+strconv.Itoa(i))
			l.decide(epoch)
			l.decide(epoch)
			more, _ := l.sent()
			reads = append(reads, more...)
		}
		if len(reads) != store.Credit {
			t.Fatalf("%d reads sent, want %d", len(reads), store.Credit)
		}
		l.inbound <- accepted()
		if l.deciding() || len(l.inbound) != 1 {
			t.Fatalf("the loop took a readers' event with no store credit")
		}
		l.answers <- store.Answer{Op: reads[0], View: true}
		if !l.deciding() || l.deciding() || len(l.inbound) != 1 {
			t.Fatalf("the confirmation of a read's view was not taken, or a readers' event was taken after it with no store credit")
		}
		l.answers <- store.Answer{Op: reads[0]}
		answered := l.deciding()
		if took := l.deciding(); !answered || !took || len(l.inbound) != 0 {
			t.Fatalf("the store's answer was not taken, or the readers' event not taken after it")
		}
	})
}

// wireWait bounds each wait of TestRunStopsIntakeAtTheBatchBound for what a
// real connection, socket and clock do; quiet is how long it watches for
// what must not happen.
const wireWait, quiet = 5 * time.Second, 300 * time.Millisecond

// received returns the next value on ch, failing the test unless one comes
// within wireWait; what names it.
func received[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(wireWait):
		var zero T
		t.Fatalf("no %s within %v", what, wireWait)
		return zero
	}
}

// design.md, Execution, Communication: "The core does not take from the
// channel while it has no store credit. While unresolved publication batches
// are at their bound, the core grants no publication body but takes every
// other event"; Capacities: "Accepted, unresolved publication batches are
// limited to 4 per channel [...] Commit, refusal or abandonment releases a
// batch"; R2: "When work backs up, the clients slow, never the core".
//
// The loop runs on a real clock with publish_commit_max_records 1 and no
// interval, a real MQTT listener handing it real connections, and the test
// playing the store. MQTT 5 clients p and q connect; p publishes five
// times at QoS 1, every publication a batch of its own: the first goes to
// the store, unanswered, the other three wait behind it, and the fifth's
// ask for its body is taken from the readers' channel, its grant withheld,
// so no other group is sent. q's PINGREQ, meanwhile, has its PINGRESP. The
// first group's commit gives p its PUBACK and grants the fifth body; each
// later group, committed in turn, gives the next PUBACK, the fifth's last.
func TestRunStopsIntakeAtTheBatchBound(t *testing.T) {
	cfg := newConfig(t)
	cfg.Broker.Storage.PublishCommitMaxRecords = 1
	l := newLoop(t)
	listener, err := connection.Listen(cfg, &cfg.Broker.MQTT.Listen.TCP[0], l.inbound,
		connection.NewPermits(cfg.Broker.Limits.MaxConnections), connection.NewMetrics(metrics.New()))
	if err != nil {
		t.Fatalf("the listener: %v", err)
	}
	l.runOn(core.New(cfg, metrics.New(), &broadcast, l.writes, l.reads, l.lines, l.credits), connection.RealClock{})
	go func() { _ = listener.Run() }()
	t.Cleanup(func() { _ = listener.Close() })
	start := received(t, l.reads, "start's read")
	l.answers <- store.Answer{Op: start, View: true}
	l.answers <- store.Answer{Op: start}

	connect := func(id string) *harness.Client {
		t.Helper()
		c, err := harness.Dial(listener.Addr().String(), harness.V5)
		if err != nil {
			t.Fatalf("dialling the listener: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		c.Timeout = wireWait
		connected := make(chan error, 1)
		go func() {
			ack, err := c.Connect(&harness.Connect{ClientID: id, CleanStart: true})
			if err == nil && ack.Reason != 0 {
				err = fmt.Errorf("CONNACK 0x%02X", ack.Reason)
			}
			connected <- err
		}()
		load := received(t, l.reads, "read of "+id+"'s session")
		l.answers <- store.Answer{Op: load, View: true}
		l.answers <- store.Answer{Op: load}
		if err := received(t, connected, "CONNACK"); err != nil {
			t.Fatalf("%s's CONNECT: %v; want CONNACK 0x00", id, err)
		}
		return c
	}
	c, q := connect("p"), connect("q")

	for id := uint16(1); id <= batchBound+1; id++ {
		if err := c.Send(&harness.Publish{QoS: 1, ID: id, Topic: "bound", Payload: []byte("x")}); err != nil {
			t.Fatalf("PUBLISH %d: %v", id, err)
		}
	}
	first := received(t, l.writes, "group for the first publication")
	time.Sleep(quiet)
	if n := len(l.inbound); n != 0 {
		t.Fatalf("%v after a fifth PUBLISH, with %d publication batches unresolved, %d events wait on the readers' channel; "+
			"want its reader's ask taken, only its grant withheld", quiet, batchBound, n)
	}
	if err := q.Ping(); err != nil {
		t.Fatalf("q's PINGREQ at the batch bound: %v; want its PINGRESP, every other event taken", err)
	}
	select {
	case w := <-l.writes:
		t.Fatalf("at the bound, with the first group unanswered, the loop sent another write, %T", w)
	default:
	}
	group := first
	for id := uint16(1); id <= batchBound+1; id++ {
		if id > 1 {
			group = received(t, l.writes, "next group")
		}
		l.answers <- store.Answer{Op: group}
		p, err := c.Recv()
		if ack, ok := p.(*harness.Ack); err != nil || !ok || ack.Kind != harness.PUBACK || ack.ID != id || ack.Reason != 0 {
			t.Fatalf("after group %d's commit the client read %v, %v; want PUBACK %d, 0x00", id, p, err, id)
		}
	}
}
