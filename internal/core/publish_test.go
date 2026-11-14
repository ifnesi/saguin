package core_test

import (
	"net"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// design.md, Flows, PUBLISH: "That time is carried with the packet. Message
// Expiry and publish-rate accounting use receipt time. A core wait does not
// restart either. The record's timestamp is the core's intake time."
// storage.md, records: time, "its timestamp, the core's intake time";
// expires, "when its Message Expiry runs out, counted from its receipt".
// Testing: a test inside the code checks "orderings no client can force":
// the wait between a body's arrival and the core's intake.
//
// A connected MQTT 5 client's reader asks for a body and is given it; its
// PUBLISH at QoS 1, Message Expiry Interval 10 seconds, received at t0,
// reaches the core at t1, a tick later than t0. The one record the core
// sends the store expires at t0 + 10 s, and its time is t1.
func TestRecordExpiresFromReceipt(t *testing.T) {
	const body = 64
	s := newScriptOn(t, newConfig(t), &broadcast)
	conn := s.connected(connectOf(packet.V5, "x"))
	if got := s.ask(conn, body); got != body {
		t.Fatalf("a reader asked for a body of %d bytes and was given %d", body, got)
	}
	received := s.now.Add(s.tick / 4)
	acts := s.handOverWith(conn, 1, 1, body, connection.PublishMetadata{Expiry: 10, Expires: true, Received: received.UnixMilli()})
	intake := s.now
	writes := writesIn(acts)
	records := make([]store.Record, 0, len(writes))
	for _, w := range writes {
		records = append(records, core.RecordsOf(w)...)
	}
	s.settle()
	if len(records) != 1 {
		t.Fatalf("the store was sent %d records, want 1: %+v", len(records), records)
	}
	r := records[0]
	if want := received.Add(10 * time.Second).UnixMilli(); !r.Expires.Valid || r.Expires.Int64 != want {
		t.Errorf("the record expires at %+v, want %d: Message Expiry runs from receipt time, %v, not the intake, %v",
			r.Expires, want, received, intake)
	}
	if r.Time != intake.UnixMilli() {
		t.Errorf("the record's time is %d, want %d, the core's intake time", r.Time, intake.UnixMilli())
	}
}

// design.md, Testing: a test inside the code checks "the guards of [...]
// the busy path"; storage.md: "A message's properties are a BLOB holding its
// MQTT 5 properties in their wire form. This is a Property Length followed by
// the properties, as internal/packet writes them (MQTT 5 2.2.2)". The reader
// hands the core a PUBLISH's properties in that form
// (packet.Publish.StoredProperties), in the body it hands over. A connected
// MQTT 5 client publishes into $broadcast at QoS 1, one message at a time,
// with properties and without: the record the core sends the store holds the
// very bytes handed over, not a copy, and a publication with properties
// allocates no more than one without.
func TestPublishPropertiesAreStoredUncopied(t *testing.T) {
	stored, err := packet.AppendProperties(nil, packet.PUBLISH, []packet.Property{
		{ID: packet.ContentType, String: "text/plain"},
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "k", Value: "v"}},
	})
	if err != nil || int(stored[0]) != len(stored)-1 {
		t.Fatalf("the properties % X, %v: want a one-byte Property Length and the properties", stored, err)
	}
	with, without := newPublisher(t, stored), newPublisher(t, nil)
	w := with.publish()
	members := w.(*core.Group).Writes
	records := make([]store.Record, 0, len(members))
	for _, member := range members {
		records = append(records, core.RecordsOf(member)...)
	}
	with.saved(w)
	if len(records) != 1 {
		t.Fatalf("the store was sent %+v, want one record", records)
	}
	if got := records[0].Properties; len(got) != len(stored) || &got[0] != &stored[0] {
		t.Errorf("the record's properties are % X; want the bytes handed over, % X, not a copy", got, stored)
	}
	allocs := func(p *publisher) float64 { return testing.AllocsPerRun(100, func() { p.saved(p.publish()) }) }
	if a, b := allocs(with), allocs(without); a != b {
		t.Errorf("a publication with properties allocates %v times, one without %v: want as many", a, b)
	}
}

// publisher is a core with one MQTT 5 client connected, which publishes
// into $broadcast at QoS 1 with properties, in their stored form, nil for
// none.
type publisher struct {
	t     *testing.T
	core  *core.Core
	conn  *connection.Conn
	now   time.Time
	ev    connection.Event
	acts  []core.Action
	reply []core.Action
}

// newPublisher returns a publisher whose PUBLISH carries properties.
func newPublisher(t *testing.T, properties []byte) *publisher {
	reg := metrics.New()
	cfg := newConfig(t)
	c := core.New(cfg, reg, &broadcast, nil, nil, nil, logger.NewCredits(cfg.Broker.LogLevel, reg))
	answered(t, c, c.Fire(nil, epoch), epoch)
	conn := new(connection.Conn)
	accept := connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1883}}
	answered(t, c, c.Handle(nil, connection.Event{Kind: connection.Accepted, Conn: conn, Packet: accept}, epoch), epoch)
	replies := answered(t, c, c.Handle(nil, connection.Event{Kind: connection.Received, Conn: conn, Packet: connectOf(packet.V5, "publisher")}, epoch), epoch)
	if ack := connackIn(replies, conn); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("the CONNECT answered with %v, want CONNACK 0x00", replies)
	}
	const topic = "properties"
	p := &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte(topic), Payload: []byte("payload")}
	section := 1 // an empty property section: its Property Length alone
	if properties != nil {
		section = len(properties)
		p.Properties = packet.Properties(properties[1:]) // the section, as the reader decoded it
		p.StoredProperties = properties
	}
	body := int64(2 + len(topic) + 2 + section + len(p.Payload))
	return &publisher{t: t, core: c, conn: conn, now: epoch,
		ev:   connection.Event{Kind: connection.Received, Conn: conn, Packet: p, Bytes: body},
		acts: make([]core.Action, 0, 8), reply: make([]core.Action, 0, 8)}
}

// publish hands the core the reader's ask for the body's bytes, then,
// given them, its PUBLISH, and returns the write the core sends the store.
func (p *publisher) publish() store.Write {
	p.now = p.now.Add(time.Millisecond)
	p.acts = p.core.Handle(p.acts[:0], connection.Event{Kind: connection.BytesRequested, Conn: p.conn, Bytes: p.ev.Bytes}, p.now)
	if len(p.acts) != 1 || p.acts[0].Bytes != p.ev.Bytes {
		p.t.Fatalf("an ask for a body of %d bytes, nothing reserved, was answered %+v; want the bytes given", p.ev.Bytes, p.acts)
	}
	p.acts = p.core.Handle(p.acts[:0], p.ev, p.now)
	for _, a := range p.acts {
		if a.Write != nil {
			return a.Write
		}
	}
	p.t.Fatalf("a QoS 1 PUBLISH into $broadcast asked the store for no write: %+v", p.acts)
	return nil
}

// saved answers w, saved, and fails the test unless the answer sends the
// publisher its PUBACK.
func (p *publisher) saved(w store.Write) {
	p.reply = p.core.Answer(p.reply[:0], store.Answer{Op: w}, p.now)
	if len(p.reply) != 1 || p.reply[0].Conn != p.conn || p.reply[0].Reply == nil {
		p.t.Fatalf("the store's answer to a QoS 1 PUBLISH's write sent %+v, want the publisher's PUBACK", p.reply)
	}
}
