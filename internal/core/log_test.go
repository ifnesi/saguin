package core_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/testkit"
)

// errCannotBuild is the error a writer's close report carries for a reply
// it could not build.
var errCannotBuild = errors.New("the reply exceeds the peer's Maximum Packet Size")

// cannotBuild plays one connection to its end, a writer that could not
// build a reply: accepted, lost before any CONNECT, its close report
// carrying errCannotBuild. It returns the decision on that report.
func (s *script) cannotBuild() []core.Action {
	s.t.Helper()
	conn := s.accept()
	s.end(conn, nil, 0)
	return s.closeReport(conn, errCannotBuild)
}

// linesIn returns, in order, the log lines acts send.
func linesIn(acts []core.Action) []logger.Line {
	var lines []logger.Line
	for _, a := range acts {
		if a.Line != (logger.Line{}) {
			lines = append(lines, a.Line)
		}
	}
	return lines
}

// design.md, Flows, Logging: "The core owns runtime log admission and the
// log credits specified in Execution, Capacities. An admitted line holds
// its credit until the core processes its output-write result. Dequeuing a
// line does not release credit. An eligible line without credit is dropped
// and counted as `full`"; "Before the core starts and after it finishes,
// main alone owns
// admission; ownership does not overlap"; Execution, Capacities:
// "Outstanding log entries have 1,024 log credits"; metrics.md:
// saguin_log_lines_dropped_total's `full`, "the core held no log credit".
// Main admits main lines before the core's first event, their results not yet
// back, and hands the credits over. Each of logger.Capacity-main writers that
// could not build a reply has its close report's line sent, and no result
// returns: the next such line finds every credit held, and is dropped and
// counted as full. Each result, main's or the core's, returns one credit, and
// one line more is sent; with none returned, the next is dropped again.
func TestLogCreditsBoundTheLinesOutstanding(t *testing.T) {
	for _, main := range []int{0, 3} {
		t.Run(strconv.Itoa(main)+" lines main admitted", func(t *testing.T) {
			s := newScript(t, newConfig(t))
			for range main {
				if !s.credits.Admit(logger.Error) {
					t.Fatal("a line main admits before the core starts, refused")
				}
			}
			full := func() float64 {
				return testkit.Sample(t, s.reg, `saguin_log_lines_dropped_total{cause="full"}`)
			}
			for range logger.Capacity - main {
				s.cannotBuild()
			}
			if len(s.lines) != logger.Capacity-main || full() != 0 {
				t.Fatalf("%d lines sent and %v dropped as full with credit for each; want %d sent, none dropped",
					len(s.lines), full(), logger.Capacity-main)
			}
			if got := linesIn(s.cannotBuild()); len(got) != 0 {
				t.Fatalf("a line sent with every one of the %d credits held: %+v", logger.Capacity, got)
			}
			if got := full(); got != 1 {
				t.Fatalf(`saguin_log_lines_dropped_total{cause="full"} = %v with every credit held, want 1`, got)
			}
			for i := range max(main, 1) {
				s.core.Logged(logger.Result{})
				if got := linesIn(s.cannotBuild()); len(got) != 1 {
					t.Fatalf("after result %d, the next line: %d sent, want 1 (the result's credit back)", i+1, len(got))
				}
			}
			if got := linesIn(s.cannotBuild()); len(got) != 0 {
				t.Fatalf("a line sent with every returned credit spent: %+v", got)
			}
			if got := full(); got != 2 {
				t.Errorf(`saguin_log_lines_dropped_total{cause="full"} = %v, want 2: one line dropped before the results, one after`, got)
			}
		})
	}
}

// design.md, Flows, Logging: "Runtime log-worthy events reach the core
// through their existing event or failure reports"; DISCONNECT: "0x80 for a
// packet it cannot build, which is never sent". The writer's error travels on
// its close report (a Closed event's Packet, connection.CannotBuild), and the
// core logs exactly one line for it, at Error, at the report's time, naming
// the error: Logging, "The core logs its first fatal store error as one ERROR
// line naming the error. It also logs each reply a writer could not build as
// one ERROR line naming the error, from that writer's close report. These
// lines follow the admission rules above" (decided from item 0008a).
func TestCoreLogsAReplyItsWriterCouldNotBuild(t *testing.T) {
	s := newScript(t, newConfig(t))
	got := linesIn(s.cannotBuild())
	if len(got) != 1 {
		t.Fatalf("the close report of a reply its writer could not build logged %d lines, want 1: %+v", len(got), got)
	}
	if line := got[0]; line.Level != logger.Error || !line.At.Equal(s.now) || !strings.Contains(line.Msg, errCannotBuild.Error()) {
		t.Errorf("the line %+v; want it at Error, at the report's time %v, naming %q", line, s.now, errCannotBuild)
	}
}
