package logger_test

import (
	"bytes"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The API these tests fix (item 0026d, logger admission by the core):
//
//	type Level            // Debug < Info < Warn < Error, config.md's log_level
//	const Capacity        // the log credits, 1,024 (design.md, Execution, Capacities)
//
//	// Line is one log line: the time its event happened, which the sender
//	// gives (the core never reads the clock: design.md, Testing), its
//	// level and its message.
//	type Line struct {
//		At    time.Time
//		Level Level
//		Msg   string
//	}
//
//	// Result says one line's output write is done, and nothing more: the
//	// logger counts a failed or short write as refused itself, so a result
//	// only returns its line's credit (audit 261023-03, observation 3).
//	type Result struct{}
//
//	// Start starts the logger's one goroutine with a go statement of its
//	// own (the R1 allowlist names logger.Start), writing on out, each line
//	// with the broker id id (config.md, log_level). It registers
//	// saguin_log_lines_dropped_total{cause="refused"} on reg, at 0, and
//	// counts there every write that fails or is short, the final drain's
//	// included (metrics.md: "refused (the log output refused it)").
//	func Start(out io.Writer, id string, reg *metrics.Registry) *Logger
//	// Lines is where the side that owns admission sends each line it has
//	// admitted; Capacity lines fit. The logger writes every line it is
//	// sent, whatever its level: admission is the sender's.
//	func (l *Logger) Lines() chan<- Line
//	// Results holds one result per line, in line order (design.md, Flows,
//	// Logging: "Results return in line order on a channel sized to the
//	// credits").
//	func (l *Logger) Results() <-chan Result
//	// StopWithin tells the logger, on a channel of its own (the line
//	// channel is never closed: R3), to write the lines queued, and waits
//	// until it has and its goroutine has ended, but at most timeout
//	// (design.md, Restart, Graceful stop: "the logger writes the lines
//	// queued until limits.write_timeout after main requests its drain,
//	// main waiting no longer, and ignores any later one"). It is called
//	// once.
//	func (l *Logger) StopWithin(timeout time.Duration)
//
//	// Credits is log admission: the log level and the log credits, Capacity
//	// of them, held by one owner at a time, main before the core starts and
//	// after it ends, the core between (design.md, Flows, Logging; R1's
//	// logger row). It is no goroutine's and takes no lock: its owner hands
//	// it on. NewCredits registers saguin_log_lines_dropped_total{cause="full"}
//	// on reg, at 0 (metrics.md, Rules).
//	type Credits struct{ ... }
//	func NewCredits(level Level, reg *metrics.Registry) *Credits
//	// Admit decides one line of level: below the log level it is not
//	// eligible, and Admit returns false and counts nothing; an eligible
//	// line without credit is dropped, counted as full, and Admit returns
//	// false; else the line takes one credit and Admit returns true, and the
//	// owner sends it on Lines.
//	func (c *Credits) Admit(level Level) bool
//	// Return processes one result: its line's credit comes back, and
//	// nothing is counted. "An admitted line holds its credit until the core
//	// processes its output-write result."
//	func (c *Credits) Return(r Result)

// backstop bounds every wait in these tests; a correct logger is far faster.
const backstop = 30 * time.Second

// drainTimeout is the generous timeout the tests give StopWithin when they
// check the drain, not its deadline: a correct logger drains far sooner.
const drainTimeout = backstop

// brokerID is the broker id the tests give the logger (config.md, broker.id).
const brokerID = "edge-1"

// at is the time the tests' lines happened, unless a test says otherwise.
var at = time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)

// errRefused is what an output that refuses a line returns from Write.
var errRefused = errors.New("the output refuses the line")

// outcome is what one Write of the output does.
type outcome uint8

const (
	keeps   outcome = iota // keeps the line and reports it all written
	refuses                // keeps nothing and returns errRefused
	short                  // keeps nothing and reports one byte fewer, without an error
)

// output stands in for standard error. It keeps what the logger writes, can
// hold the logger inside Write until released or let it make one Write at a
// time, can take longer over each Write than over the one before, does to
// each Write what its plan says (keeps past the plan), and wakes a waiting
// test after each Write.
type output struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	calls   int           // Write calls so far
	plan    []outcome     // what the first Write calls do
	pace    time.Duration // when not zero, Write call n takes n times pace
	held    chan struct{} // when not nil, Write waits for a step or its close
	release func()        // closes held, once
	entered chan struct{} // closed when Write is first entered
	enter   func()        // closes entered, once
}

// newOutput returns an output; with hold, every Write waits for a step or
// for release.
func newOutput(t *testing.T, hold bool) *output {
	o := &output{entered: make(chan struct{}), release: func() {}}
	o.enter = sync.OnceFunc(func() { close(o.entered) })
	if hold {
		o.held = make(chan struct{})
		o.release = sync.OnceFunc(func() { close(o.held) })
		// A failed test still lets a held logger go.
		t.Cleanup(o.release)
	}
	return o
}

func (o *output) Write(p []byte) (int, error) {
	o.enter()
	if o.held != nil {
		<-o.held
	}
	o.mu.Lock()
	o.calls++
	call := o.calls
	o.mu.Unlock()
	// A slow output keeps the line only once its Write's time has passed.
	time.Sleep(time.Duration(call) * o.pace)
	o.mu.Lock()
	defer o.mu.Unlock()
	what := keeps
	if call <= len(o.plan) {
		what = o.plan[call-1]
	}
	switch what {
	case refuses:
		return 0, errRefused
	case short:
		return len(p) - 1, nil
	}
	o.buf.Write(p)
	return len(p), nil
}

// step lets a held logger make one Write, waiting until it is inside it. It
// is not called once the output is released.
func (o *output) step() { o.held <- struct{}{} }

// text returns everything written so far.
func (o *output) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

// lines returns the complete lines written so far, without their line breaks.
func (o *output) lines() []string {
	complete, _, found := strings.CutLast(o.text(), "\n")
	if !found {
		return nil
	}
	return strings.Split(complete, "\n")
}

// waitEntered waits until the logger is inside Write.
func (o *output) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-o.entered:
	case <-time.After(backstop):
		t.Fatalf("the logger wrote nothing within %v", backstop)
	}
}

// send puts line on l's channel as its sender does, holding a credit for
// it, so the channel has room: a full channel fails the test.
func send(t *testing.T, l *logger.Logger, line logger.Line) {
	t.Helper()
	select {
	case l.Lines() <- line:
	default:
		t.Fatalf("the logger's channel is full with a credit held for the line %q", line.Msg)
	}
}

// results takes n results from l, in the order they come.
func results(t *testing.T, l *logger.Logger, n int) []logger.Result {
	t.Helper()
	got := make([]logger.Result, 0, n)
	for range n {
		got = append(got, testkit.Recv(t, l.Results(), backstop, "a line's result"))
	}
	return got
}

// write sends each line, taking its result before the next.
func write(t *testing.T, l *logger.Logger, lines ...logger.Line) {
	t.Helper()
	for _, line := range lines {
		send(t, l, line)
		results(t, l, 1)
	}
}

// stop calls StopWithin(drainTimeout), failing the test if it does not
// return within its timeout and the backstop.
func stop(t *testing.T, l *logger.Logger) {
	t.Helper()
	if !testkit.ReturnsWithin(drainTimeout+backstop, func() { l.StopWithin(drainTimeout) }) {
		t.Fatalf("StopWithin(%v) did not return within %v", drainTimeout, drainTimeout+backstop)
	}
}

// dropped reads saguin_log_lines_dropped_total{cause="<cause>"} from a scrape
// of reg, and checks that it is rendered as a counter.
func dropped(t *testing.T, reg *metrics.Registry, cause string) uint64 {
	t.Helper()
	return uint64(testkit.Sample(t, reg, `saguin_log_lines_dropped_total{cause="`+cause+`"}`, testkit.Typed("counter")))
}

// startedBy counts the live goroutines that logger.Start started when called
// on goroutine id. A function holding a go statement is never inlined, so each
// such goroutine's stack ends "created by …/internal/logger.Start in goroutine
// <id>". Counting only these, a logger another test started and that is
// still exiting is never counted.
func startedBy(id string) int {
	n := 0
	for _, g := range testkit.Goroutines() {
		if g.Creator == "github.com/ifnesi/saguin/internal/logger.Start" && g.Parent == id {
			n++
		}
	}
	return n
}

// waitStartedBy waits until startedBy(id) returns want.
func waitStartedBy(t *testing.T, id string, want int) {
	t.Helper()
	if !testkit.Within(backstop, func() bool { return startedBy(id) == want }) {
		t.Fatalf("goroutines logger.Start started here: got %d, want %d", startedBy(id), want)
	}
}

// design.md, Flows, Logging: "Each log line is written on standard error. It
// contains the time the event happened, in UTC with milliseconds
// (2026-10-10T09:00:00.123Z), the level (DEBUG, INFO, WARN or ERROR), the
// broker id and the message. These fields are separated by single spaces. A
// line feed or carriage return in a message is written as \n or \r".
// design.md, Testing: "The core never reads the clock", so the time written
// is the one the sender gives, in whatever zone it comes.
func TestLineFormat(t *testing.T) {
	out := newOutput(t, false)
	l := logger.Start(out, brokerID, metrics.New())
	east := time.FixedZone("UTC+5", 5*60*60)
	write(t, l,
		logger.Line{At: time.Date(2026, 10, 10, 14, 0, 0, 123_000_000, east), Level: logger.Info, Msg: "listener started"},
		logger.Line{At: time.Date(2026, 10, 10, 9, 0, 1, 0, time.UTC), Level: logger.Debug, Msg: "whole second"},
		logger.Line{At: time.Date(2026, 10, 10, 9, 0, 2, 120_000_000, time.UTC), Level: logger.Warn, Msg: "two\nlines"},
		logger.Line{At: time.Date(2001, 2, 3, 4, 5, 6, 7_000_000, time.UTC), Level: logger.Error, Msg: "ends in a break\n"},
	)
	stop(t, l)

	want := "2026-10-10T09:00:00.123Z INFO edge-1 listener started\n" +
		"2026-10-10T09:00:01.000Z DEBUG edge-1 whole second\n" +
		"2026-10-10T09:00:02.120Z WARN edge-1 two\\nlines\n" +
		"2001-02-03T04:05:06.007Z ERROR edge-1 ends in a break\\n\n"
	if got := out.text(); got != want {
		t.Errorf("the log output:\n got %q\nwant %q", got, want)
	}
}

// design.md, Flows, Logging: "a line feed or carriage return in a message
// is written as `\n` or `\r`": a carriage return, alone or beside a line
// feed, anywhere in a message, is written as the two characters \r.
func TestCarriageReturnsAndLineFeedsAreEscaped(t *testing.T) {
	out := newOutput(t, false)
	l := logger.Start(out, brokerID, metrics.New())
	for _, msg := range []string{
		"crlf\r\nthen lf\nthen cr\rthen lfcr\n\rend",
		"\rat the start",
		"at the end\r",
	} {
		write(t, l, logger.Line{At: at, Level: logger.Info, Msg: msg})
	}
	stop(t, l)

	want := `2026-10-10T09:00:00.000Z INFO edge-1 crlf\r\nthen lf\nthen cr\rthen lfcr\n\rend` + "\n" +
		`2026-10-10T09:00:00.000Z INFO edge-1 \rat the start` + "\n" +
		`2026-10-10T09:00:00.000Z INFO edge-1 at the end\r` + "\n"
	if got := out.text(); got != want {
		t.Errorf("the log output:\n got %q\nwant %q", got, want)
	}
}

// config.md, broker, log_level: "`debug`, `info`, `warn` or `error`": the
// level set is the lowest written. design.md, Flows, Logging: "An eligible
// line without credit is dropped and counted as `full`": a line below the
// level is not eligible, so it is not admitted, takes no credit and is not
// counted as full. Each level is admitted once at every level;
// then the eligible lines that fill the credits are admitted, and the next
// one is dropped as full.
func TestLinesBelowLogLevelAreNotAdmitted(t *testing.T) {
	levels := []struct {
		level logger.Level
		name  string
	}{
		{logger.Debug, "DEBUG"},
		{logger.Info, "INFO"},
		{logger.Warn, "WARN"},
		{logger.Error, "ERROR"},
	}
	for _, set := range levels {
		t.Run(set.name, func(t *testing.T) {
			reg := metrics.New()
			c := logger.NewCredits(set.level, reg)
			held := 0
			for _, line := range levels {
				if got, want := c.Admit(line.level), line.level >= set.level; got != want {
					t.Errorf("log_level %s, a %s line: admitted %v, want %v", set.name, line.name, got, want)
				}
				if line.level >= set.level {
					held++
				}
			}
			if got := dropped(t, reg, "full"); got != 0 {
				t.Errorf("saguin_log_lines_dropped_total{cause=\"full\"} after a line of each level: got %d, want 0", got)
			}
			for i := held; i < logger.Capacity; i++ {
				if !c.Admit(logger.Error) {
					t.Fatalf("log_level %s: line %d refused with %d of %d credits held; a line below the level took one", set.name, i+1, i, logger.Capacity)
				}
			}
			if c.Admit(logger.Error) {
				t.Errorf("log_level %s: a line admitted with all %d credits held", set.name, logger.Capacity)
			}
			if got := dropped(t, reg, "full"); got != 1 {
				t.Errorf("saguin_log_lines_dropped_total{cause=\"full\"}: got %d, want 1", got)
			}
		})
	}
}

// design.md, Flows, Logging: "a failed output write is counted as
// `refused`. Results return in line order on a channel sized to the
// credits." metrics.md, Process: `refused` "(the log output refused it)";
// design.md, Access and reload: "a log output that closes loses its lines,
// counted as refused, and never stops the broker". Five lines admitted at
// once meet an output that keeps the first, refuses the second, keeps the
// third, writes the fourth short and keeps the fifth, letting the logger
// make one Write at a time: each line's result comes once its write is
// done and before the next line's write, so the results come one per
// line, in line order; the logger writes on after a refusal and counts
// each lost line as refused at its write, before any result is processed;
// processing the results only returns their credits, counting nothing
// more.
func TestResultsReturnInLineOrder(t *testing.T) {
	// In a bubble, synctest.Wait returns once the logger is blocked, so the
	// results and the count are read between one Write and the next.
	synctest.Test(t, func(t *testing.T) {
		out := newOutput(t, true)
		defer out.release()
		out.plan = []outcome{keeps, refuses, keeps, short, keeps}
		reg := metrics.New()
		l := logger.Start(out, brokerID, reg)
		if got := cap(l.Lines()); got != logger.Capacity {
			t.Errorf("the lines channel holds %d, want the %d credits", got, logger.Capacity)
		}
		if got := cap(l.Results()); got != logger.Capacity {
			t.Errorf("the results channel holds %d, want the %d credits", got, logger.Capacity)
		}
		c := logger.NewCredits(logger.Info, reg)
		for i := range len(out.plan) {
			if !c.Admit(logger.Info) {
				t.Fatalf("line %d not admitted", i)
			}
			send(t, l, logger.Line{At: at, Level: logger.Info, Msg: "line " + strconv.Itoa(i)})
		}
		refused := uint64(0)
		for i, what := range out.plan {
			synctest.Wait()
			if got := len(l.Results()); got != i {
				t.Fatalf("results before line %d's write: got %d, want %d, one per line written", i, got, i)
			}
			out.step()
			synctest.Wait()
			if got := len(l.Results()); got != i+1 {
				t.Fatalf("results once line %d's write is done: got %d, want %d", i, got, i+1)
			}
			if what != keeps {
				refused++
			}
			if got := dropped(t, reg, "refused"); got != refused {
				t.Errorf("saguin_log_lines_dropped_total{cause=\"refused\"} once line %d's write is done: got %d, want %d", i, got, refused)
			}
		}
		out.release()
		got := results(t, l, len(out.plan))
		stop(t, l)
		for _, r := range got {
			c.Return(r)
		}
		if got := dropped(t, reg, "refused"); got != 2 {
			t.Errorf("saguin_log_lines_dropped_total{cause=\"refused\"} after the results were processed: got %d, want 2 (a result only returns its credit)", got)
		}
		if got := dropped(t, reg, "full"); got != 0 {
			t.Errorf("saguin_log_lines_dropped_total{cause=\"full\"}: got %d, want 0", got)
		}
		wantText := "2026-10-10T09:00:00.000Z INFO edge-1 line 0\n" +
			"2026-10-10T09:00:00.000Z INFO edge-1 line 2\n" +
			"2026-10-10T09:00:00.000Z INFO edge-1 line 4\n"
		if got := out.text(); got != wantText {
			t.Errorf("the log output: got %q, want %q", got, wantText)
		}
	})
}

// design.md, Flows, Restart, Graceful stop: "The logger writes the lines
// queued until limits.write_timeout after main requests its drain. Main waits
// no longer". Lines queued before the stop, as many as the credits allow, are
// all written, in order, before StopWithin returns, each with its result, the
// output keeping well within its timeout.
func TestStopDrainsQueuedLines(t *testing.T) {
	// In a bubble, synctest.Wait returns once every goroutine is blocked, so
	// the stop is known to be seen before the output lets the logger go.
	// Released earlier, the logger could write the queue on its own, and a
	// StopWithin that drains nothing would pass.
	synctest.Test(t, func(t *testing.T) {
		out := newOutput(t, true)
		defer out.release()
		l := logger.Start(out, brokerID, metrics.New())

		// Hold the logger on its first line, then queue the rest of the
		// credits behind it.
		send(t, l, logger.Line{At: at, Level: logger.Warn, Msg: "queued 0"})
		synctest.Wait()
		for i := 1; i < logger.Capacity; i++ {
			send(t, l, logger.Line{At: at, Level: logger.Warn, Msg: "queued " + strconv.Itoa(i)})
		}

		// StopWithin is called and waits; the logger, still held, has written
		// nothing. What was written is read the moment StopWithin returns.
		stopped := make(chan struct{})
		var atStop []string
		go func() {
			l.StopWithin(drainTimeout)
			atStop = out.lines()
			close(stopped)
		}()
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("StopWithin returned while the logger was still held on its first line")
		default:
		}
		out.release()
		<-stopped

		if len(atStop) != logger.Capacity {
			t.Fatalf("lines written by the time StopWithin returned: got %d, want %d", len(atStop), logger.Capacity)
		}
		for i, line := range atStop {
			if want := " WARN edge-1 queued " + strconv.Itoa(i); !strings.HasSuffix(line, want) {
				t.Errorf("line %d: got %q, want it to end %q", i, line, want)
			}
		}
		if got := len(l.Results()); got != logger.Capacity {
			t.Errorf("results by the time StopWithin returned: got %d, want %d", got, logger.Capacity)
		}
	})
}

// design.md, Flows, Restart, Graceful stop: "The logger writes the lines
// queued until limits.write_timeout after main requests its drain. Main waits
// no longer. The logger ignores any later line"; Flows, Logging: "Final
// draining follows the named shutdown deadline". Lines are queued when
// StopWithin(timeout) is called, and each case reads the output once the
// logger has written what it will:
//
//   - stuck: the output holds the logger inside Write on its first line.
//     Once timeout has passed, StopWithin has returned though the output
//     still holds the logger. Released only then, the output receives the
//     held line, which gets its result, and none queued behind it: the
//     deadline has passed, so none of them is written or gets a result.
//   - slow: the output is slow but moving, each Write finishing a little
//     later than the last, the drain still writing at the deadline. The
//     lines whose writes begin before the deadline are written, in order,
//     each with its result; no line is written after it, nor gets a
//     result; StopWithin returns at the deadline.
func TestStopWithinEndsAtItsDeadline(t *testing.T) {
	const timeout = 5 * time.Second // limits.write_timeout's default
	const queued = 8
	// In a bubble, time moves only when every goroutine is blocked, so the
	// logger cannot write past the held line before the deadline, and each
	// slow Write takes exactly its time on the clock.
	t.Run("stuck", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			out := newOutput(t, true)
			defer out.release()
			l := logger.Start(out, brokerID, metrics.New())
			send(t, l, logger.Line{At: at, Level: logger.Warn, Msg: "held"})
			synctest.Wait() // the logger is inside Write
			for i := range queued {
				send(t, l, logger.Line{At: at, Level: logger.Warn, Msg: "queued " + strconv.Itoa(i)})
			}

			stopped := make(chan struct{})
			go func() {
				l.StopWithin(timeout)
				close(stopped)
			}()
			time.Sleep(timeout)
			synctest.Wait()
			select {
			case <-stopped:
			default:
				t.Errorf("StopWithin(%v) had not returned %v after it was called, the output still holding the logger", timeout, timeout)
			}

			out.release()
			synctest.Wait() // the logger has written what it will
			<-stopped
			written := out.lines()
			if len(written) != 1 || !strings.HasSuffix(written[0], " WARN edge-1 held") {
				t.Errorf("lines written after the deadline: got %q, want only the held line", written)
			}
			if got := len(l.Results()); got != 1 {
				t.Errorf("results: got %d, want 1, the held line's; the %d lines queued behind it get none", got, queued)
			}
		})
	})
	t.Run("slow", func(t *testing.T) {
		// Write n takes n paces, so it begins (n-1)n/2 paces after the stop:
		// writes 1 to 5 begin before the deadline, at 0, 0.4, 1.2, 2.4 and
		// 4 seconds, the fifth ending past it; the sixth would begin at 6.
		const pace = 400 * time.Millisecond
		var want []string
		for n := 1; n <= queued && time.Duration((n-1)*n/2)*pace < timeout; n++ {
			want = append(want, "queued "+strconv.Itoa(n-1))
		}
		if len(want) == queued {
			t.Fatalf("at pace %v every queued line's write begins before the deadline: the case checks nothing", pace)
		}
		synctest.Test(t, func(t *testing.T) {
			out := newOutput(t, false)
			out.pace = pace
			l := logger.Start(out, brokerID, metrics.New())
			for i := range queued {
				send(t, l, logger.Line{At: at, Level: logger.Warn, Msg: "queued " + strconv.Itoa(i)})
			}

			// The clock has not moved since the lines were sent: it moves
			// only once this goroutine waits in StopWithin.
			called := time.Now()
			l.StopWithin(timeout)
			if waited := time.Since(called); waited != timeout {
				t.Errorf("StopWithin(%v) returned after %v, want %v, the output still writing", timeout, waited, timeout)
			}

			// Long enough for the output to take every queued line.
			time.Sleep(time.Duration(queued*(queued+1)/2) * pace)
			synctest.Wait() // the logger has written what it will
			var got []string
			for _, line := range out.lines() {
				_, msg, _ := strings.Cut(line, " WARN edge-1 ")
				got = append(got, msg)
			}
			if !slices.Equal(got, want) {
				t.Errorf("lines written: got %q, want %q, those whose writes begin before the deadline", got, want)
			}
			if got := len(l.Results()); got != len(want) {
				t.Errorf("results: got %d, want %d, one per line written", got, len(want))
			}
		})
	})
}

// design.md, Flows, Restart, Graceful stop: "The logger writes the lines
// queued until limits.write_timeout after main requests its drain [...] The
// logger ignores any later line". design.md, Execution, Communication: "a
// channel with several senders is never closed: its receiver is told to stop
// on a channel of its own". A line sent once StopWithin has returned, the
// logger's goroutine ended, does not panic, is not written and has no result.
func TestLineAfterStopIsIgnored(t *testing.T) {
	out := newOutput(t, false)
	id := testkit.Goid()
	l := logger.Start(out, brokerID, metrics.New())
	write(t, l, logger.Line{At: at, Level: logger.Info, Msg: "before the stop"})
	stop(t, l)
	waitStartedBy(t, id, 0)

	before := out.text()
	send(t, l, logger.Line{At: at, Level: logger.Error, Msg: "after the stop"})
	if got := out.text(); got != before {
		t.Errorf("the output changed after StopWithin:\n got %q\nwant %q", got, before)
	}
	if got := len(l.Results()); got != 0 {
		t.Errorf("%d results after a line sent after StopWithin, want none", got)
	}
}

// design.md, Testing: "The busy path is anything run per packet, per message
// or per connection. It does no needless work. [...] avoids every copy or
// allocation it can." The core admits and sends its lines as it decides;
// admitting a line and sending it allocate nothing, nor does dropping one
// without credit, nor processing a result. The logger is held writing its
// first line, so the one goroutine that could allocate meanwhile is still.
func TestAdmissionDoesNotAllocate(t *testing.T) {
	const runs = 100 // testkit.NoAllocs calls f runs+1 times
	if logger.Capacity <= 2*(runs+1) {
		t.Fatalf("logger.Capacity is %d: the credits cannot hold the %d lines measured", logger.Capacity, 2*(runs+1))
	}
	out := newOutput(t, true)
	l := logger.Start(out, brokerID, metrics.New())
	c := logger.NewCredits(logger.Debug, metrics.New())
	line := logger.Line{At: at, Level: logger.Info, Msg: "admitted"}
	c.Admit(logger.Info)
	send(t, l, line)
	out.waitEntered(t)

	testkit.NoAllocs(t, "admitting and sending a line", func() {
		if c.Admit(logger.Info) {
			l.Lines() <- line
		}
	})
	testkit.NoAllocs(t, "processing a result", func() { c.Return(logger.Result{}) })
	for range logger.Capacity { // spends every credit left
		c.Admit(logger.Info)
	}
	testkit.NoAllocs(t, "dropping a line without credit", func() { c.Admit(logger.Info) })
	out.release()
	stop(t, l)
}
