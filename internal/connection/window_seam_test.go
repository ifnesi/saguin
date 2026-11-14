package connection_test

import (
	"net"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/packet"
)

// windowPublish is the client's n-th QoS 1 PUBLISH, its packet identifier
// n: a body of 7 bytes, under 128, behind a fixed header of windowHeader.
func windowPublish(t *testing.T, n uint16) []byte {
	t.Helper()
	return harness.Encoded(t, &harness.Publish{QoS: 1, Topic: "t", ID: n, Payload: []byte("m")}, harness.V5)
}

// windowHeader is the length of windowPublish's fixed header.
const windowHeader = 2

// startWindowSeam runs the seam of a broker whose
// qos2.max_inflight_per_client is 1, so one QoS 1 PUBLISH unanswered fills
// the client's window. The reader reads an MQTT 5 CONNECT and the writer writes a CONNACK
// 0x00 carrying no Receive Maximum, which a reader taking the limit from
// the CONNACK would read as 65,535 (MQTT 5 3.2.2.3.3: "If the Receive
// Maximum value is absent, then its value defaults to 65,535"). Once every
// goroutine waits, the CONNACK's reply slot is back with the reader's
// writer; the reader then waits in a read.
func startWindowSeam(t *testing.T) *seam {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.QoS2.MaxInflightPerClient = 1
	s := launchSeam(t, cfg)
	s.accepted()
	s.connect()
	synctest.Wait()
	return s
}

// decision plays the client once the reader has read a PUBLISH's fixed
// header: it gives each read the reader makes what is left of the packet,
// keeps the writer's calls for later, and returns the next event the reader
// sends the core, its decision on that PUBLISH.
func (s *seam) decision(rest []byte) connection.Event {
	s.t.Helper()
	deadline := time.After(backstop)
	for {
		select {
		case ev := <-s.inbound:
			return ev
		case c := <-s.sock.calls:
			switch {
			case c.who == writer:
				s.pending[writer] = c
			case c.op == opRead:
				rest = rest[c.give(rest):]
			default:
				c.ok()
			}
		case <-deadline:
			s.t.Fatalf("the reader made no decision on the PUBLISH within %v", backstop)
		}
	}
}

// undecided fails the test if the reader has decided on the PUBLISH whose
// fixed header it has read, or read on, while every goroutine waits.
func (s *seam) undecided(when string) {
	s.t.Helper()
	synctest.Wait()
	select {
	case ev := <-s.inbound:
		s.t.Fatalf("%s, the core got %s: the reader decided while the PUBACK's write was under way, "+
			"on a count the write was about to change", when, describe(&ev))
	case c := <-s.sock.calls:
		if c.who == reader {
			s.t.Fatalf("%s, the reader called %v: it read on while the PUBACK's write was under way", when, c)
		}
		s.pending[c.who] = c
	default:
	}
}

// design.md, PUBLISH: "A reader counts its client's unanswered QoS 1 and 2
// PUBLISHes against qos2.max_inflight_per_client, the value its CONNACK
// carries. That value is fixed from the resolved configuration before the
// reader starts. A written PUBACK, PUBCOMP or refusing PUBREC frees one. A
// held QoS 2 identifier takes exactly one until its PUBCOMP, including one
// held from an earlier connection, which counts from the CONNACK. A PUBLISH
// repeating a held identifier takes none. A returned reply slot carries
// capacity only, never this limit. A QoS 1
// PUBLISH arriving at Receive Maximum while a PUBACK is being sent waits for
// it. The PUBLISH is taken once that PUBACK is sent. If none is being sent,
// the connection ends with DISCONNECT 0x93. If sending fails, the connection
// ends without a DISCONNECT". MQTT 5 3.3.4: "The Client MUST NOT send more
// than Receive Maximum QoS 1 and QoS 2 PUBLISH packets for which it has not
// received PUBACK, PUBCOMP, or PUBREC with a Reason Code of 128 or greater
// from the Server [MQTT-3.3.4-7]. If it receives more than Receive Maximum
// QoS 1 and QoS 2 PUBLISH packets where it has not sent a PUBACK or PUBCOMP
// in response, the Server uses a DISCONNECT packet with Reason Code 0x93"
// (the same paragraph, server to client, is [MQTT-3.3.4-9]'s).
//
// qos2.max_inflight_per_client is 1; the CONNACK the core's stand-in sends
// carries no Receive Maximum, and its reply slot has gone back before the
// client's PUBLISHes, so a reader that took the limit from it would admit
// the second where the design refuses it. The client's first QoS 1 PUBLISH
// reaches the core, filling the window; the client's second arrives, its
// fixed header read while the first's PUBACK:
//
//   - is being written, the Write made and returning once every goroutine
//     waits, the client having its bytes: the second is admitted;
//   - was never queued: 0x93, decided without waiting;
//   - is being written and the Write blocks, the clock moving on: the reader
//     decides nothing until the Write ends, then admits the second;
//   - is being written and the Write fails: the connection ends and the
//     second never reaches the core.
func TestReceiveMaximumAtTheWindowsEdge(t *testing.T) {
	for _, tc := range []struct {
		name   string
		puback bool          // the core queues the first PUBLISH's PUBACK
		blocks time.Duration // how long the Write is seen blocked before it ends; 0: not watched
		fails  bool          // the Write fails
	}{
		{name: "the next PUBLISH arrives as the PUBACK is written", puback: true},
		{name: "no PUBACK written: one past Receive Maximum"},
		{name: "the PUBACK's write blocks", puback: true, blocks: seamTimeout / 2},
		{name: "the PUBACK's write fails", puback: true, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := startWindowSeam(t)
				first := windowPublish(t, 1)
				s.arrive(first[:windowHeader])
				if ev := s.readPublish(first[windowHeader:]); !isPublish(&ev, 1) {
					t.Fatalf("the core got %s, want the first PUBLISH", describe(&ev))
				}
				s.hold() // the reader waits for the client's next packet

				var write *sockCall
				if tc.puback {
					s.queue(&connection.Ack{Type: packet.PUBACK, PacketID: 1})
					s.expect(opWriteDeadline).ok()
					write = s.expect(opWrite) // made: the client has the PUBACK
				}
				second := windowPublish(t, 2)
				header := s.reading.give(second)
				s.reading = nil
				fixed, body := second[header:windowHeader], second[windowHeader:]
				if write == nil {
					ev := s.decision(fixed)
					if ev.Kind != connection.Ended || ev.Reason != 0x93 {
						t.Fatalf("the core got %s, want the reader's end with 0x93: two QoS 1 PUBLISHes unanswered, "+
							"qos2.max_inflight_per_client 1, no PUBACK written", describe(&ev))
					}
					return
				}
				synctest.Wait() // every goroutine waits: the reader has decided, or waits for the write
				if tc.blocks != 0 {
					s.undecided("with the PUBACK's write made")
					s.clock.set(epoch.Add(tc.blocks))
					s.undecided("with the PUBACK's write blocked " + tc.blocks.String())
				}
				if tc.fails {
					write.fail(&net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET})
					s.finish()
					if len(s.publishes) != 0 {
						t.Fatalf("the core got PUBLISH %d: the PUBACK's write failed, so the window never had room for it",
							s.publishes[0].PacketID)
					}
					return
				}
				write.ok()
				if ev := s.decision(fixed); ev.Kind != connection.BytesRequested || ev.Bytes != int64(len(body)) {
					t.Fatalf("the core got %s, want the reader's ask for the second PUBLISH's body: the first's PUBACK was written, "+
						"so the client had one QoS 1 PUBLISH unanswered, at qos2.max_inflight_per_client 1, not past it [MQTT-3.3.4-7]", describe(&ev))
				}
				s.grant(len(body))
				if ev := s.readBody(body); !isPublish(&ev, 2) {
					t.Fatalf("the core got %s, want the second PUBLISH", describe(&ev))
				}
			})
		})
	}
}

// isPublish reports whether ev hands the core the PUBLISH whose packet
// identifier is id.
func isPublish(ev *connection.Event, id uint16) bool {
	p, ok := ev.Packet.(*packet.Publish)
	return ev.Kind == connection.Received && ok && p.PacketID == id
}
