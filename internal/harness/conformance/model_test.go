package conformance

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The reference model of durable QoS 0 and 1 delivery (item 0010c, from the
// design review of 261009): a small model of what the design promises a
// durable session across disconnects, crashes and restarts, driven together
// with a broker by random steps, every delivery checked against it.

// modelSeedEnv names the environment variable that, set to a decimal
// seed, replays a run of TestDeliveryModel: design.md, Testing, "Each run's
// seed is recorded so a failure can be rerun with the same steps".
const modelSeedEnv = "SAGUIN_MODEL_SEED"

// modelStepsEnv names the environment variable that sets how many steps
// TestDeliveryModel takes, for a longer run than modelSteps.
const modelStepsEnv = "SAGUIN_MODEL_STEPS"

// modelSteps is how many steps TestDeliveryModel takes by default: a few
// seconds, within make ci's budget, with some crashes and restarts.
const modelSteps = 150

// modelInflight is the broker.limits.max_inflight of the model's broker:
// small, so in-flight windows fill and resumes meet a full quota.
const modelInflight = 4

// recentSteps is how many of the last steps a failure prints.
const recentSteps = 40

// fate is what the model knows of one record for one session, every
// record being owed to every session (each subscribed to the whole topic
// space before the first publication).
type fate uint8

const (
	// unseen: owed and not yet received. A QoS 1 record the broker sent but
	// the client never read is unseen too: its resend may come, DUP 1.
	unseen fate = iota
	// inFlight: received at QoS 1, not yet acknowledged; it is in the
	// session's unacked list, and comes again only as a resend.
	inFlight
	// settled: acknowledged, or received at QoS 0. It never comes again
	// while connected (design.md, Delivery: "While connected, the broker
	// starts no resend. Its replies to repeated packets follow their QoS
	// flow."), nor across a reconnect or a graceful restart: Positions,
	// "a durable session may also pass records represented by stored QoS 1
	// or QoS 2 exchanges", so settled records after an unacknowledged one are
	// passed. Whether a commit holds it before a kill the model does not
	// know: a QoS 0 delivery settles when its writer reports the write ("QoS
	// 0 settles on successful writing"), which no packet of the client's is
	// ordered with.
	settled
	// repeatable: settled when the broker was killed: it may come again,
	// DUP 0 from the position or DUP 1 from its session_inflight row
	// (Positions: "recovery can replay unsaved progress").
	repeatable
	// ackLost: acknowledged at QoS 1 before a graceful stop, which may have
	// taken the PUBACK without storing it (Restart, Graceful stop: "any
	// other packet it takes is neither stored nor answered"): it may come
	// again, but only as a resend from its row, DUP 1.
	ackLost
	// droppable: owed at QoS 0 to a session whose connection the broker's
	// death ended, not received: the broker may have written it, which
	// settles it ("QoS 0 settles on successful writing"), and the client
	// lost it; it may still come.
	droppable
)

// modelRecord is a record the model knows stored: a publication its
// publisher had acknowledged, or one sent before an acknowledged one on
// the same connection.
type modelRecord struct {
	topic, payload string
	qos            byte
}

// inflightEntry is a QoS 1 delivery the client has read and not
// acknowledged: its record, its packet identifier, and whether the current
// connection has received it (a resend, or its first sending there).
type inflightEntry struct {
	record int
	id     uint16
	here   bool
}

// modelSession is one durable session and its client.
type modelSession struct {
	connect *harness.Connect
	version byte
	grant   byte            // the QoS its one subscription was granted
	c       *harness.Client // nil while away
	fates   []fate          // by record
	unacked []inflightEntry // in sending order
	lastNew int             // the record of the last DUP 0 delivery on this connection, -1 for none
	sawNew  bool            // a DUP 0 delivery came on this connection
}

// deliveryModel drives the broker and the model together.
type deliveryModel struct {
	t         *testing.T
	rng       *rand.Rand
	dir       string
	b         *harness.Broker
	pub       *harness.Client
	records   []modelRecord
	byPayload map[string]int
	sessions  []*modelSession
	step      int
	recent    []string
}

// note records what the current step does, for a failure's report.
func (m *deliveryModel) note(format string, args ...any) {
	line := fmt.Sprintf("step %d: ", m.step) + fmt.Sprintf(format, args...)
	m.recent = append(m.recent, line)
	if len(m.recent) > recentSteps {
		m.recent = m.recent[1:]
	}
}

// start launches the broker on the model's files and connects the
// publisher, which keeps no session.
func (m *deliveryModel) start() {
	m.b = support.Launch(m.t, &harness.Options{Dir: m.dir, MaxInflight: modelInflight})
	m.pub, _ = harness.Connected(m.t, m.b.Addr, harness.V5, &harness.Connect{ClientID: "model-pub", CleanStart: true})
}

// publish has the publisher send n messages, their QoS and topics drawn
// from bits, the last at QoS 1, and waits for the PUBACKs: the last one's
// commit holds every earlier message of the connection (Store, Writes), so
// all n are stored.
func (m *deliveryModel) publish(n int, bits uint32) {
	ps := make([]*harness.Publish, n)
	for j := range ps {
		qos := byte(bits>>j) & 1
		if j == n-1 {
			qos = 1
		}
		ps[j] = &harness.Publish{QoS: qos, Topic: fmt.Sprintf("model/%d", bits>>(8+2*j)&3),
			Payload: fmt.Appendf(nil, "%d-%d", m.step, j)}
	}
	support.PublishAll(m.t, m.pub, ps)
	for _, p := range ps {
		m.byPayload[string(p.Payload)] = len(m.records)
		m.records = append(m.records, modelRecord{p.Topic, string(p.Payload), p.QoS})
		for _, s := range m.sessions {
			s.fates = append(s.fates, unseen)
		}
		m.note("published %q at QoS %d on %q", p.Payload, p.QoS, p.Topic)
	}
}

// receive checks one delivery to s against the model and records it.
func (m *deliveryModel) receive(s *modelSession, p *harness.Publish) error {
	i, ok := m.byPayload[string(p.Payload)]
	if !ok || m.records[i].topic != p.Topic {
		return fmt.Errorf("%q received PUBLISH %q of %q, no record it is owed", s.connect.ClientID, p.Topic, p.Payload)
	}
	r := m.records[i]
	// SUBSCRIBE, One copy: "The delivery uses the lower of the
	// publication's QoS and the highest contributing grant".
	if want := min(r.qos, s.grant); p.QoS != want {
		return fmt.Errorf("%q received %q at QoS %d; want %d", s.connect.ClientID, r.payload, p.QoS, want)
	}
	m.note("%q received %q, QoS %d, DUP %v, identifier %d", s.connect.ClientID, r.payload, p.QoS, p.Dup, p.ID)
	if p.Dup {
		return m.resent(s, i, p)
	}
	// CONNECT, 9. Resume: "in-flight messages [...] resends them [...] Resent
	// messages have DUP set [...] New application deliveries wait behind
	// recovered PUBLISHes".
	if k := slices.IndexFunc(s.unacked, func(e inflightEntry) bool { return !e.here }); k >= 0 {
		return fmt.Errorf("%q received %q, DUP 0, before the resend of %q, in flight when its connection ended",
			s.connect.ClientID, r.payload, m.records[s.unacked[k].record].payload)
	}
	switch s.fates[i] {
	case inFlight:
		return fmt.Errorf("%q received %q, DUP 0, while it was in flight: a resend is DUP 1 [MQTT-3.3.1-1]", s.connect.ClientID, r.payload)
	case settled, ackLost:
		return fmt.Errorf("%q received %q again, DUP 0, though it was settled and the broker was not killed since", s.connect.ClientID, r.payload)
	}
	// Delivery, Order: "a channel's offset order on $broadcast".
	if i <= s.lastNew {
		return fmt.Errorf("%q received %q after %q: out of offset order", s.connect.ClientID, r.payload, m.records[s.lastNew].payload)
	}
	if p.QoS == 1 && slices.ContainsFunc(s.unacked, func(e inflightEntry) bool { return e.id == p.ID }) {
		return fmt.Errorf("%q received %q under packet identifier %d, still in use", s.connect.ClientID, r.payload, p.ID)
	}
	s.lastNew, s.sawNew = i, true
	m.take(s, i, p)
	return nil
}

// resent checks a DUP 1 delivery of record i to s: CONNECT, 9. Resume,
// "in-flight messages [...] resends them with their packet identifiers and in
// their order. Resent messages have DUP set (MQTT-4.4.0-1)"; Delivery:
// "While connected, the broker starts no resend. Its replies to repeated
// packets follow their QoS flow."
func (m *deliveryModel) resent(s *modelSession, i int, p *harness.Publish) error {
	if s.sawNew {
		return fmt.Errorf("%q received the resend of %q after a new delivery on the same connection", s.connect.ClientID, p.Payload)
	}
	if k := slices.IndexFunc(s.unacked, func(e inflightEntry) bool { return e.record == i }); k >= 0 {
		e := &s.unacked[k]
		first := slices.IndexFunc(s.unacked, func(e inflightEntry) bool { return !e.here })
		switch {
		case e.here:
			return fmt.Errorf("%q received %q again on the connection that already had it", s.connect.ClientID, p.Payload)
		case k != first:
			return fmt.Errorf("%q received the resend of %q before that of %q, sent before it", s.connect.ClientID, p.Payload, m.records[s.unacked[first].record].payload)
		case p.ID != e.id:
			return fmt.Errorf("%q received the resend of %q under packet identifier %d; want its original %d", s.connect.ClientID, p.Payload, p.ID, e.id)
		}
		e.here = true
		return nil
	}
	switch s.fates[i] {
	case unseen, repeatable, droppable, ackLost:
		// Sent on an earlier connection and never read, settled before the
		// broker was killed, or its PUBACK taken by a graceful stop: its row
		// may be there. Resends come in their original order, so it takes its
		// place among them: after those already resent on this connection,
		// before those still to come.
		if p.QoS != 1 {
			m.take(s, i, p)
			return nil
		}
		s.fates[i] = inFlight
		first := slices.IndexFunc(s.unacked, func(e inflightEntry) bool { return !e.here })
		if first < 0 {
			first = len(s.unacked)
		}
		s.unacked = slices.Insert(s.unacked, first, inflightEntry{record: i, id: p.ID, here: true})
		return nil
	}
	return fmt.Errorf("%q received the resend of %q, which it had settled, the broker neither killed nor stopped since", s.connect.ClientID, p.Payload)
}

// take records record i as received by s.
func (m *deliveryModel) take(s *modelSession, i int, p *harness.Publish) {
	if p.QoS == 1 {
		s.fates[i] = inFlight
		s.unacked = append(s.unacked, inflightEntry{record: i, id: p.ID, here: true})
		return
	}
	s.fates[i] = settled
}

// absorb checks what s's client read past while waiting for something
// else.
func (m *deliveryModel) absorb(s *modelSession) error {
	for len(s.c.Pending()) > 0 {
		p, _ := s.c.Recv() // from the backlog: no read, no error
		pub, ok := p.(*harness.Publish)
		if !ok {
			return fmt.Errorf("%q read %s, not a PUBLISH, while waiting", s.connect.ClientID, harness.Name(p.Type()))
		}
		if err := m.receive(s, pub); err != nil {
			return err
		}
	}
	return nil
}

// poll has s's client send PINGREQ and take every delivery before its
// PINGRESP, acknowledging none.
func (m *deliveryModel) poll(s *modelSession) error {
	if err := s.c.Send(&harness.Pingreq{}); err != nil {
		return err
	}
	if _, err := s.c.Await(func(p harness.Packet) bool { _, ok := p.(*harness.Pingresp); return ok }); err != nil {
		return fmt.Errorf("%q waiting for PINGRESP: %w", s.connect.ClientID, err)
	}
	return m.absorb(s)
}

// ack acknowledges up to n of s's deliveries in flight on this connection,
// oldest first: a client sends PUBACKs "in the order in which the
// corresponding PUBLISH packets were received" [MQTT-4.6.0-2].
func (m *deliveryModel) ack(s *modelSession, n int) error {
	for ; n > 0 && len(s.unacked) > 0 && s.unacked[0].here; n-- {
		e := s.unacked[0]
		if err := s.c.Send(&harness.Ack{Kind: harness.PUBACK, ID: e.id}); err != nil {
			return err
		}
		m.note("%q acknowledged %q, identifier %d", s.connect.ClientID, m.records[e.record].payload, e.id)
		s.fates[e.record] = settled
		s.unacked = s.unacked[1:]
	}
	return nil
}

// barrier has s's client publish at QoS 1 on a topic no subscription
// matches and wait for its PUBACK, taking the deliveries that come before
// it: the core has then taken every packet the client sent before.
func (m *deliveryModel) barrier(s *modelSession) error {
	p := &harness.Publish{QoS: 1, Topic: "model-barrier/" + s.connect.ClientID, Payload: []byte("barrier"), ID: s.c.NextID()}
	if err := s.c.Send(p); err != nil {
		return err
	}
	ack, err := s.c.AwaitAck(harness.PUBACK, p.ID)
	if err != nil {
		return fmt.Errorf("%q publishing its barrier: %w", s.connect.ClientID, err)
	}
	if ack.Reason != 0 {
		return fmt.Errorf("%q publishing its barrier: PUBACK 0x%02X, want 0x00", s.connect.ClientID, ack.Reason)
	}
	m.note("%q: its publication acknowledged", s.connect.ClientID)
	return m.absorb(s)
}

// readToEnd takes every delivery to s until its connection ends, and marks
// the session away. Unless the broker died, the end is orderly, and only a
// DISCONNECT 0x8B, at a graceful stop, may come besides deliveries; after
// its death, any end will do.
func (m *deliveryModel) readToEnd(s *modelSession, died bool) error {
	for {
		p, err := s.c.Recv()
		if err != nil {
			if !died && !errors.Is(err, io.EOF) {
				return fmt.Errorf("%q: %w, want an orderly end of stream", s.connect.ClientID, err)
			}
			break
		}
		switch p := p.(type) {
		case *harness.Publish:
			if err := m.receive(s, p); err != nil {
				return err
			}
		case *harness.Disconnect:
			if p.Reason != 0x8B {
				return fmt.Errorf("%q received DISCONNECT 0x%02X, want 0x8B (Server shutting down)", s.connect.ClientID, p.Reason)
			}
		default:
			return fmt.Errorf("%q read %s before the end of its connection", s.connect.ClientID, harness.Name(p.Type()))
		}
	}
	_ = s.c.Close() // ended: nothing is lost
	s.c = nil
	for k := range s.unacked {
		s.unacked[k].here = false
	}
	return nil
}

// disconnect has s's client send DISCONNECT, end its sending
// [MQTT-3.14.4-2], and take what comes until the end: the session goes
// away.
func (m *deliveryModel) disconnect(s *modelSession) error {
	if err := s.c.Send(&harness.Disconnect{}); err != nil {
		return err
	}
	if err := s.c.CloseWrite(); err != nil {
		return err
	}
	m.note("%q sent DISCONNECT", s.connect.ClientID)
	return m.readToEnd(s, false)
}

// resume connects s's client again, keeping its session, an MQTT 5 client
// with Receive Maximum rm: CONNECT, 9. Resume.
func (m *deliveryModel) resume(s *modelSession, rm uint32) {
	if s.version == harness.V5 {
		s.connect.Props = slices.Concat(support.Durable, receiveMaximum5(rm))
	}
	s.c = resumed(m.t, m.b.Addr, s.version, s.connect)
	s.lastNew, s.sawNew = -1, false
	m.note("%q resumed its session, Receive Maximum %d", s.connect.ClientID, rm)
}

// owedDone reports whether s has received every record it is owed that
// the model says must come, and every resend.
func (s *modelSession) owedDone() bool {
	return !slices.Contains(s.fates, unseen) && !slices.ContainsFunc(s.unacked, func(e inflightEntry) bool { return !e.here })
}

// drain acknowledges and reads s's deliveries until every record it is
// owed has come: R5 and Choices MQTT leaves open, "QoS 0 messages for away
// sessions | delivered on return". Each read waits at most the client's
// timeout.
func (m *deliveryModel) drain(s *modelSession) error {
	for {
		if err := m.ack(s, len(s.unacked)); err != nil {
			return err
		}
		if s.owedDone() {
			return nil
		}
		p, err := s.c.Recv()
		if err != nil {
			k := slices.Index(s.fates, unseen)
			return fmt.Errorf("%q waiting for what it is owed (%q among it): %w", s.connect.ClientID, m.records[max(k, 0)].payload, err)
		}
		pub, ok := p.(*harness.Publish)
		if !ok {
			return fmt.Errorf("%q read %s, want a delivery", s.connect.ClientID, harness.Name(p.Type()))
		}
		if err := m.receive(s, pub); err != nil {
			return err
		}
	}
}

// died updates the model after the broker's end. After a kill, every
// settlement may be undone (Positions: "recovery can replay unsaved
// progress"), the deliveries its clients read from their dead connections
// included, and an unseen QoS 0 delivery to a session in connected, those
// whose connection the kill ended, may be lost. After a graceful stop, a QoS 1 acknowledgement may have been taken
// and not stored (Restart, Graceful stop: "any other packet it takes is
// neither stored nor answered"); every other settlement holds, the stop
// draining what the core accepted before it.
func (m *deliveryModel) died(killed bool, connected []*modelSession) {
	for _, s := range m.sessions {
		for i, f := range s.fates {
			qos := min(m.records[i].qos, s.grant)
			switch {
			case killed && f == settled:
				s.fates[i] = repeatable
			case killed && f == unseen && qos == 0 && slices.Contains(connected, s):
				s.fates[i] = droppable
			case !killed && f == settled && qos == 1:
				s.fates[i] = ackLost
			}
		}
	}
}

// kill kills the broker, takes what each connected client can still
// read, and starts the broker again on the same files: Store, full mode,
// "Everything acknowledged survives a crash or a power cut". The model
// learns of the kill once the clients have read what the broker wrote
// before it: those deliveries were settled by a broker that may not have
// saved them.
func (m *deliveryModel) kill() error {
	support.Crash(m.t, m.b)
	m.note("killed the broker")
	var connected []*modelSession
	for _, s := range m.sessions {
		if s.c != nil {
			connected = append(connected, s)
			if err := m.readToEnd(s, true); err != nil {
				return err
			}
		}
	}
	m.died(true, connected)
	m.start()
	return nil
}

// restart stops the broker gracefully, each connected client ending its
// sending and reading to the end, and starts it again on the same files.
func (m *deliveryModel) restart() error {
	if err := m.b.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	m.note("stopped the broker")
	m.died(false, nil)
	for _, s := range m.sessions {
		if s.c == nil {
			continue
		}
		if err := s.c.CloseWrite(); err != nil {
			return err
		}
		if err := m.readToEnd(s, false); err != nil {
			return err
		}
	}
	if state := support.Exited(m.t, m.b); !state.Success() {
		return fmt.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
	m.start()
	return nil
}

// act takes one step. Every step draws the same four values, so one seed
// gives one sequence of steps whatever the broker's timing; a step that
// does not apply to the session drawn does nothing.
func (m *deliveryModel) act() error {
	action, s := m.rng.IntN(100), m.sessions[m.rng.IntN(len(m.sessions))]
	n, bits := 1+m.rng.IntN(4), m.rng.Uint32()
	rm := 1 + bits%6
	// Of every 100 draws: 30 publish; 15 poll, 15 ack, 8 barrier and 10
	// disconnect, which an away session skips; 10 resume, which a
	// connected one skips; 5 drain; 4 kill; 3 restart.
	switch {
	case action < 30:
		m.publish(n, bits)
	case action < 88 && s.c == nil:
		if action >= 78 {
			m.resume(s, rm)
		}
	case action < 45:
		return m.poll(s)
	case action < 60:
		return m.ack(s, n)
	case action < 68:
		return m.barrier(s)
	case action < 78:
		return m.disconnect(s)
	case action < 88:
		// A resume drawn for a connected session.
	case action < 93:
		if s.c == nil {
			m.resume(s, rm)
		}
		return m.drain(s)
	case action < 97:
		return m.kill()
	default:
		return m.restart()
	}
	return nil
}

// design.md, Testing: "The broker as a whole is fuzzed end to end from the
// wire [...] Every answer is checked against MQTT and this document. Each
// run's seed is recorded so a failure can be rerun with the same steps".
// Store, full mode: "Everything acknowledged survives a crash or a power
// cut"; "A restart recovers committed state"; R5: "A
// consumer reads only stored records, from its own position"; Choices MQTT
// leaves open: "QoS 0 messages for away sessions | delivered on return";
// Delivery, Positions: "a crash re-sends at most what that commit carried";
// the rest cited where the model applies it.
//
// In full mode, three durable sessions subscribe to one filter, two over
// MQTT 5 granted QoS 1 and 0 and one over MQTT 3.1.1 granted QoS 1; the
// broker's max_inflight is modelInflight. Then random steps: a publisher's
// messages at QoS 0 and 1, the last of each batch at QoS 1; a client
// taking the deliveries before a PINGRESP, acknowledging some in order,
// publishing at QoS 1 on a topic of its own, sending
// DISCONNECT, resuming its session with a random Receive Maximum, or
// taking everything it is owed; the broker killed and started again, or
// stopped and started again. Every delivery is checked against the model:
// a record owed, at the right QoS; resends first, DUP 1, under their
// identifiers, in their order; new deliveries DUP 0, in offset order;
// nothing settled sent again unless the broker was killed since, or,
// after a graceful stop, as the DUP 1 resend of an acknowledgement the
// stop may have dropped.
// At the end every session takes everything it is owed, and the stopped
// broker's database holds every message its publisher had acknowledged.
func TestDeliveryModel(t *testing.T) {
	t.Parallel()
	seed, steps := rand.Uint64(), modelSteps
	if v := os.Getenv(modelSeedEnv); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatalf("%s=%q: %v", modelSeedEnv, v, err)
		}
	}
	if v := os.Getenv(modelStepsEnv); v != "" {
		var err error
		if steps, err = strconv.Atoi(v); err != nil || steps < 1 {
			t.Fatalf("%s=%q: want a positive number of steps", modelStepsEnv, v)
		}
	}
	t.Logf("seed %d, %d steps; replay with %s=%d %s=%d", seed, steps, modelSeedEnv, seed, modelStepsEnv, steps)
	m := &deliveryModel{t: t, rng: rand.New(rand.NewPCG(seed, 0)), dir: t.TempDir(), byPayload: make(map[string]int)}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d; the last steps:", seed)
			for _, line := range m.recent {
				t.Log(line)
			}
		}
	})
	m.start()
	for i, cfg := range []struct {
		version, grant byte
	}{{harness.V5, 1}, {harness.V311, 1}, {harness.V5, 0}} {
		s := &modelSession{version: cfg.version, grant: cfg.grant, lastNew: -1,
			connect: &harness.Connect{ClientID: fmt.Sprintf("model-%d", i), CleanStart: cfg.version == harness.V5}}
		if cfg.version == harness.V5 {
			s.connect.Props = support.Durable
		}
		s.c = support.Subscribed(t, m.b.Addr, cfg.version, s.connect, support.At(cfg.grant, "model/#"))
		m.sessions = append(m.sessions, s)
	}

	for m.step = 1; m.step <= steps; m.step++ {
		if err := m.act(); err != nil {
			t.Fatalf("seed %d, step %d: %v", seed, m.step, err)
		}
	}
	for _, s := range m.sessions {
		if s.c == nil {
			m.resume(s, modelInflight)
		}
		if err := m.drain(s); err != nil {
			t.Fatalf("seed %d, at the end: %v", seed, err)
		}
	}
	stopped(t, m.b)
	rs := records(t, m.b.DB)
	stored := make(map[string]*record, len(rs))
	for i := range rs {
		stored[string(rs[i].payload)] = &rs[i]
	}
	for _, want := range m.records {
		if r, ok := stored[want.payload]; !ok {
			t.Errorf("seed %d: the database holds no record of %q, acknowledged to its publisher", seed, want.payload)
		} else if r.topic != want.topic || r.qos != int(want.qos) {
			t.Errorf("seed %d: the database holds %q on %q at QoS %d; want %q at QoS %d", seed, want.payload, r.topic, r.qos, want.topic, want.qos)
		}
	}
}
