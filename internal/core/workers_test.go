package core_test

import (
	"bytes"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of queue workers driven inside, with the real store behind the
// core (liveScript): a hold's end and its worker's answer ready at once
// reach the core in the order R2 gives them, an ordering no client can
// force (design.md, Testing: "A test inside the code checks only what
// neither can see. These checks cover [...] orderings no client can
// force").

// holdYAML is a queue, tasks, holding a job for a second from its worker's
// PUBACK and offering a failed attempt again 5 seconds after its end.
const holdYAML = `channels:
  tasks:
    type: queue
    filter: "tasks/#"
    visibility_timeout: 1s
    retry:
      mode: fixed
      delay: 5s
`

// attemptIn returns the attempt id d, a worker offer, carries as its
// Correlation Data among the delivery properties the core gives it
// (design.md, Words: Attempt, "It carries a broker-made id as its
// Correlation Data"), nil where it carries none.
func attemptIn(t testing.TB, d *connection.Delivery) []byte {
	t.Helper()
	var attempt []byte
	if _, err := packet.ReadStoredProperties(d.Additions, func(p packet.PropertyView) error {
		if p.ID == packet.CorrelationData {
			attempt = bytes.Clone(p.Value)
		}
		return nil
	}); err != nil {
		t.Fatalf("the delivery of %q carries damaged delivery properties: %v", d.Topic, err)
	}
	return attempt
}

// design.md, Execution, Event order: "When several events are ready, the
// core takes them in this order: stop, store answers, logger results, the
// core's timer, then the readers' channel"; Testing: "The loop around the
// core carries that out and owns the one real timer"; Delivery, Queue jobs:
// "The hold starts at the worker's PUBACK and lasts visibility_timeout";
// "The wait starts at the attempt's end, at its return, a refusing PUBACK,
// its timeout or the worker's going"; $saguin topics, the response row: "A
// response naming an attempt that is not current or not this session's
// [...] changes nothing and is logged". metrics.md, Queues:
// saguin_queue_attempts_failed_total, "timeout (a hold past
// visibility_timeout)"; saguin_queue_stale_responses_total, "responses that
// changed nothing: naming an attempt not current"; saguin_queue_acknowledged_total,
// "ack responses". storage.md, queue_jobs: spent, failed, next_try.
//
// A worker subscribes to $saguin/queue/tasks, a publisher stores tasks/1,
// and the worker acknowledges its offer at P. Its reader asks for the body
// of an ack naming the offer's attempt, and is given it. At P plus
// visibility_timeout the core's timer comes due and the ack is handed over
// at that same moment, so the loop takes the timer, then the ack, each at
// that moment. Once the store has carried out what they lead to, the job
// is unfinished, one attempt spent and one failed, its next try 5 seconds
// after P plus visibility_timeout; one attempt is counted failed by
// timeout, one response stale and no ack; and a log line names the worker.
func TestHoldEndingAsItsAnswerArrives(t *testing.T) {
	const visibility, delay = time.Second, 5 * time.Second
	const client = "hold-worker"
	s := newLiveScript(t, holdYAML)
	var lines []logger.Line
	// The offer is read as it is handed to the writer: once reported, the
	// core may reuse the delivery.
	var offerID uint16
	var attempt []byte
	s.nudge = func(acts []core.Action) {
		lines = append(lines, linesIn(acts)...)
		for _, a := range acts {
			if d, ok := a.Reply.(*connection.Delivery); ok && string(d.Topic) == "tasks/1" {
				offerID, attempt = d.PacketID, attemptIn(t, d)
			}
		}
	}
	worker := s.connect(client, 0, 10)
	s.subscribe(worker, "$saguin/queue/tasks")
	pub := s.connect("hold-pub", 0, 10)
	s.publish(pub, 1, "tasks/1")
	s.serve()
	if offerID == 0 || len(attempt) == 0 {
		t.Fatalf("the worker was handed %v, the offer of tasks/1 under packet identifier %d with Correlation Data %q; want its offer, at QoS 1, carrying its attempt's id (Words: Attempt)", showAll(s.replies[worker]), offerID, attempt)
	}
	s.event(connection.Event{Kind: connection.Received, Conn: worker, Packet: &packet.Ack{PacketID: offerID}})
	held := s.now
	s.serve()

	p := &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("$saguin/queue/tasks/response"), Payload: []byte("ack")}
	ack := readerBody(t, s.cfg, p, packet.Property{ID: packet.CorrelationData, Binary: attempt})
	n := bodyOf(p)
	s.event(connection.Event{Kind: connection.BytesRequested, Conn: worker, Bytes: n})
	s.serve()
	if s.granted[worker] < n {
		t.Fatalf("the worker's reader asked for the ack's body of %d bytes and, the store idle, was given %d", n, s.granted[worker])
	}
	s.granted[worker] -= n

	end := held.Add(visibility)
	s.fireBefore(end, func(time.Time) {})
	s.now = end
	if at := s.core.Timer(); !at.IsZero() && !at.After(end) {
		s.take(s.core.Fire(nil, end)) // the timer, due, goes first
	}
	s.take(s.core.Handle(nil, connection.Event{Kind: connection.Received, Conn: worker, Packet: ack, Bytes: n}, end))
	s.serveStill()

	var spent, failed int64
	var next sql.NullInt64
	if err := s.db.QueryRow(`SELECT spent, failed, next_try FROM queue_jobs`).Scan(&spent, &failed, &next); err != nil {
		t.Fatalf("reading tasks/1's job once the hold's end and the ack were taken at once: %v; want it unfinished, its hold ended before the ack", err)
	}
	if want := end.Add(delay).UnixMilli(); spent != 1 || failed != 1 || !next.Valid || next.Int64 != want {
		t.Errorf("tasks/1's job has %d attempts spent, %d failed, next try %+v; want 1, 1 and %d, its hold's end plus retry.delay", spent, failed, next, want)
	}
	scrape := testkit.ParseScrape(t, testkit.Scrape(t, s.reg))
	for _, c := range []struct {
		name   string
		labels []string
		want   float64
	}{
		{"saguin_queue_attempts_failed_total", []string{"channel", "tasks", "cause", "timeout"}, 1},
		{"saguin_queue_stale_responses_total", []string{"channel", "tasks"}, 1},
		{"saguin_queue_acknowledged_total", []string{"channel", "tasks"}, 0},
	} {
		if got := scrape.Value(t, c.name, c.labels...); got != c.want {
			t.Errorf("%s = %v; want %v", testkit.SeriesKey(c.name, c.labels...), got, c.want)
		}
	}
	if !slices.ContainsFunc(lines, func(l logger.Line) bool { return strings.Contains(l.Msg, client) }) {
		t.Errorf("no log line names %q, whose ack named an attempt its hold had ended; the lines: %v", client, lines)
	}
}
