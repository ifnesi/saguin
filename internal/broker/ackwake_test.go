package broker_test

import (
	"fmt"
	"testing"
	"time"
)

// A freed slot is served whatever is waiting for it, with the
// acknowledgement that freed it as the only wake-up there is.
//
// **One row for every kind of work a slot can be owed to**, because an
// acknowledgement's wake-up is now decided rather than unconditional: it
// wakes an append channel only when the consumer may have records to send,
// and a decision that is wrong for one kind of work strands that consumer in
// silence - records stored, window open, nothing sent. Each row fills a
// Receive Maximum of 1, leaves something owed behind it, publishes nothing
// more, and then acknowledges: the next item must arrive.
//
// The rows are counted, so that a kind of work cannot be dropped from this
// table without the table saying so.
func TestAFreedSlotServesWhateverWaitsForIt(t *testing.T) {
	rows := []struct {
		name string
		run  func(t *testing.T, h *harness)
	}{
		{"append records behind the cursor", func(t *testing.T, h *harness) {
			c := connectRx(t, h, "owed-append", true, true, 1)
			c.Sub(t, "events/#", 1)
			p := connect(t, h, "owed-append-pub", true, false)
			p.Pub(t, "events/x", "e1")
			p.Pub(t, "events/x", "e2")
			firstThenNext(t, c, "e1", "e2")
		}},
		{"append records on the pending list", func(t *testing.T, h *harness) {
			c := connectRx(t, h, "owed-pending", true, true, 1)
			c.Sub(t, "events/a/#", 1)
			p := connect(t, h, "owed-pending-pub", true, false)
			p.Pub(t, "events/a/1", "a1")
			p.Pub(t, "events/b/1", "declined")
			p.Pub(t, "events/a/2", "a2")
			firstThenNext(t, c, "a1", "a2")
		}},
		{"the rest of a latest snapshot", func(t *testing.T, h *harness) {
			p := connect(t, h, "owed-latest-pub", true, false)
			p.Pub(t, "state/1", "s1")
			p.Pub(t, "state/2", "s2")
			c := connectRx(t, h, "owed-latest", true, true, 1)
			c.Sub(t, "state/#", 1)
			r, ok := c.Await(t, 3*time.Second)
			if !ok {
				t.Fatal("the snapshot's first value never arrived")
			}
			if more, ok := c.Await(t, 300*time.Millisecond); ok {
				t.Fatalf("%s arrived with the window already full, so nothing below "+
					"depends on the acknowledgement", more.Payload)
			}
			r.Ack()
			next, ok := c.Await(t, 3*time.Second)
			if !ok {
				t.Fatalf("after %s was acknowledged the rest of the snapshot never came: "+
					"the freed slot woke nothing", r.Payload)
			}
			if next.Payload == r.Payload || (next.Payload != "s1" && next.Payload != "s2") {
				t.Fatalf("after %s the snapshot sent %s, want the other value", r.Payload, next.Payload)
			}
		}},
		{"a queue worker's next job", func(t *testing.T, h *harness) {
			const jobs = 10
			p := connect(t, h, "owed-queue-pub", true, false)
			for i := 1; i <= jobs; i++ {
				p.Pub(t, fmt.Sprintf("jobs/task/%d", i), fmt.Sprintf("job-%d", i))
			}
			// Answer first, PUBACK second - Paho's order - so the answer's
			// own offer finds the window still full and only the PUBACK can
			// open it before the queue's 200ms tick.
			w := connectRx(t, h, "owed-worker", true, true, 1)
			began := time.Now()
			w.Sub(t, "$saguin/queue/jobs", 1)
			for n := 0; n < jobs; n++ {
				r, ok := w.Await(t, 3*time.Second)
				if !ok {
					t.Fatalf("the worker stopped at %d of %d jobs", n, jobs)
				}
				w.Respond(t, r.RespTopic, "ack", r.CorrData)
				r.Ack()
			}
			if took := time.Since(began); took > time.Second {
				t.Errorf("%d jobs at Receive Maximum 1 took %v: the next job waited for the "+
					"tick, not for the PUBACK that opened the window", jobs, took)
			}
		}},
		// The two below are for a client holding an append channel beside
		// the other kind: its append PUBACK is the only wake-up, and that
		// PUBACK takes b.mu for the other kind only when the client's flags
		// say it may be owed it. A flag left down is work stranded.
		{"a latest remainder queued after the flag was cleared", func(t *testing.T, h *harness) {
			p := connect(t, h, "owed-mixed-pub", true, false)
			p.Pub(t, "state/1", "s1")
			p.Pub(t, "state/2", "s2")
			c := connectRx(t, h, "owed-mixed", true, true, 1)
			c.Sub(t, "state/#", 1)
			// The first snapshot drains, which raises the flag and then,
			// on the acknowledgement that finds nothing left, lowers it.
			for n := 0; n < 2; n++ {
				r, ok := c.Await(t, 3*time.Second)
				if !ok {
					t.Fatalf("the first snapshot stopped at %d of 2", n)
				}
				r.Ack()
			}
			if r, ok := c.Await(t, 300*time.Millisecond); ok {
				r.Ack()
			}
			// Now an append record fills the window, and a second snapshot
			// is queued behind it.
			c.Sub(t, "events/#", 1)
			p.Pub(t, "events/x", "e1")
			e1, ok := c.Await(t, 3*time.Second)
			if !ok || e1.Payload != "e1" {
				t.Fatalf("the append record never arrived: %v %v", e1.Payload, ok)
			}
			p.Pub(t, "other/1", "o1")
			p.Pub(t, "other/2", "o2")
			c.Sub(t, "state/#", 1) // re-subscribe: the whole snapshot again, queued
			if more, ok := c.Await(t, 300*time.Millisecond); ok {
				t.Fatalf("%s arrived with the window already full", more.Payload)
			}
			e1.Ack() // the append PUBACK is the only wake-up there is
			r, ok := c.Await(t, 3*time.Second)
			if !ok {
				t.Fatal("after the append record was acknowledged the queued snapshot never came: " +
					"the client's flag said it was owed nothing")
			}
			if r.Payload != "s1" && r.Payload != "s2" {
				t.Fatalf("after the append record the client was sent %s, want a snapshot value", r.Payload)
			}
		}},
		{"a queue job for a client that also reads an append channel", func(t *testing.T, h *harness) {
			const rounds = 8
			p := connect(t, h, "owed-both-pub", true, false)
			w := connectRx(t, h, "owed-both", true, true, 1)
			w.Sub(t, "events/#", 1)
			w.Sub(t, "$saguin/queue/jobs", 1)
			began := time.Now()
			// Each round: an append record fills the window, a job waits
			// behind it, and the append PUBACK is what opens the window.
			for n := 0; n < rounds; n++ {
				p.Pub(t, "events/x", fmt.Sprintf("e%d", n))
				r, ok := w.Await(t, 3*time.Second)
				if !ok || r.RespTopic != "" {
					t.Fatalf("round %d: the append record did not come first: %v %v", n, r.Payload, ok)
				}
				p.Pub(t, fmt.Sprintf("jobs/task/%d", n), fmt.Sprintf("job-%d", n))
				r.Ack()
				job, ok := w.Await(t, 3*time.Second)
				if !ok || job.RespTopic == "" {
					t.Fatalf("round %d: the job never came after the append PUBACK: %v %v", n, job.Payload, ok)
				}
				w.Respond(t, job.RespTopic, "ack", job.CorrData)
				job.Ack()
			}
			if took := time.Since(began); took > time.Second {
				t.Errorf("%d rounds took %v: each job waited for the queue's tick rather than for "+
					"the append PUBACK that opened the window", rounds, took)
			}
		}},
	}

	ran := 0
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			row.run(t, start(t))
			ran++
		})
	}
	if ran != len(rows) || ran != 6 {
		t.Fatalf("%d of the 6 kinds of owed work were checked", ran)
	}
}

// firstThenNext takes the first delivery, proves the window is holding the
// next one back, acknowledges, and requires the next.
func firstThenNext(t *testing.T, c *client, first, next string) {
	t.Helper()
	r, ok := c.Await(t, 3*time.Second)
	if !ok || r.Payload != first {
		t.Fatalf("first delivery %v %v, want %s", r.Payload, ok, first)
	}
	if more, ok := c.Await(t, 300*time.Millisecond); ok {
		t.Fatalf("%s arrived with the window already full, so nothing below depends on "+
			"the acknowledgement", more.Payload)
	}
	r.Ack()
	got, ok := c.Await(t, 3*time.Second)
	if !ok {
		t.Fatalf("after %s was acknowledged %s never came: the freed slot woke nothing", first, next)
	}
	if got.Payload != next {
		t.Fatalf("after %s was acknowledged %s came, want %s", first, got.Payload, next)
	}
}
