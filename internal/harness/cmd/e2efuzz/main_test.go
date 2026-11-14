package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// fault names what the fake broker gets wrong; "" is nothing.
type fault string

const (
	lostSession    fault = "lost session"    // answers every CONNECT with Session Present 0
	silentTakeover fault = "silent takeover" // closes an MQTT 5 connection taken over without DISCONNECT 0x8E
	// oversizeRefusal refuses an MQTT 5 CONNECT advertising a Maximum
	// Packet Size with a CONNACK one Reason String makes larger than it.
	oversizeRefusal fault = "oversize refusal"
	// lostRecord acknowledges every fourth accepted QoS 1 PUBLISH with
	// PUBACK 0x00 without storing it, on $broadcast or an append channel:
	// a latest channel's or a queue's checks name a lost record in their
	// own ways, the first of which timing decides.
	lostRecord fault = "lost record"
	// countsAcked counts every QoS 1 PUBLISH of a connection against
	// Receive Maximum, acknowledged or not, so a client at the window's
	// edge gets DISCONNECT 0x93.
	countsAcked fault = "counts acknowledged"
	// wrongRefusal refuses a topic under $SYS/ with 0x90, not 0x87.
	wrongRefusal fault = "wrong refusal"
	// lateReply writes a PINGRESP after the DISCONNECT 0x8E on an MQTT 5
	// connection taken over, as a reply written outside the takeover's
	// step would land.
	lateReply fault = "late reply"
	// cutTakeover cuts a live connection taken over mid-packet, the first
	// byte of its DISCONNECT 0x8E written then the end of stream, and
	// counts no writer_full closure.
	cutTakeover fault = "takeover cut mid-packet"
	// cleanWill publishes a connection's Will when it ends with a normal
	// DISCONNECT too.
	cleanWill fault = "Will on a normal DISCONNECT"
	// anonymousWill publishes each Will without its client id as
	// publisher, so No Local cannot hold it back.
	anonymousWill fault = "Will without its publisher"
	// firstMatch routes a topic to the first configured channel whose
	// filter matches it, not the most exact.
	firstMatch fault = "first match"
	// wrongBoundary has retention keep only the newest record of a channel
	// with a retention_bytes, whatever its bytes.
	wrongBoundary fault = "wrong retention boundary"
	// ignoredACL answers as a broker with no acl_file: no feature withheld,
	// every session kept and every Will of a valid topic taken.
	ignoredACL fault = "acl_file ignored"
	// unreadable delivers records to a subscription whose user may not
	// read their topics.
	unreadable fault = "unreadable delivered"
	// twiceStored stores a QoS 2 PUBLISH repeating a held identifier as a
	// record at once, the held copy kept.
	twiceStored fault = "QoS 2 stored twice"
)

// fakeBroker listens on loopback and answers the runner's steps as the
// design has the broker answer them, with options' limits, so the runner's
// driving and checks can be tested apart from the broker. It is written
// from design.md's CONNECT, PUBLISH, SUBSCRIBE, Delivery and DISCONNECT,
// not from the runner's model, and decodes with the harness, which it
// shares. It returns the fake, holding its address and the database it
// stores records in, storage.md's records as far as the runner reads them.
func fakeBroker(t *testing.T, f fault) *fake {
	t.Helper()
	db := filepath.Join(t.TempDir(), "fake.db")
	store, err := sql.Open("sqlite3", "file:"+db+"?_journal_mode=WAL&_synchronous=OFF")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() }) // the test is over; nothing to report
	// The fake has no bridge: every record's bridge is 0, none (storage.md,
	// records), and bridges holds no row.
	if _, err := store.Exec(`CREATE TABLE records (channel INTEGER, "offset" INTEGER, id TEXT, time INTEGER, topic TEXT, qos INTEGER,
		publisher TEXT, bridge INTEGER NOT NULL DEFAULT 0, expires INTEGER, properties BLOB, payload BLOB, payload_size INTEGER,
		published_retained INTEGER, retained INTEGER, PRIMARY KEY (channel, "offset"));
		CREATE TABLE bridges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	fakeChannels(t, store)
	if _, err := store.Exec(fakeQueueSchema); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() }) // the test is over; nothing to report
	b := &fake{fault: f, sessions: map[string]*fakeSession{}, db: store, addr: ln.Addr().String(), dbPath: db,
		retained: map[string]*fakeRecord{}, latest: map[string]*fakeRecord{}, bytes: map[int64]int64{}, reserved: map[int64]int64{},
		reached: map[string][]int64{}, reads: map[[2]string]bool{}}
	b.seen.deadLettered = map[string]int{}
	t.Cleanup(func() {
		// The queues' sweeps end with the test (queue_test.go).
		b.mu.Lock()
		defer b.mu.Unlock()
		b.stopped = true
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(conn.(*net.TCPConn))
		}
	}()
	return b
}

// fake is the fake broker's state, shared by its connections' goroutines
// under a lock, as test code may. design.md, Testing: "A fake broker
// modelling takeover serializes session ownership, packet effects and
// replies. After ownership changes, the former connection performs no
// further session work. Its terminal DISCONNECT is its last outgoing MQTT
// packet." Each packet is handled in one locked step (owns), and so is a
// takeover (accept).
type fake struct {
	fault    fault
	addr     string // where it listens
	dbPath   string // its database
	mu       sync.Mutex
	sessions map[string]*fakeSession
	assigned int // client ids assigned so far
	db       *sql.DB
	offset   int64 // the last record's offset
	acked    int   // accepted QoS 1 PUBLISHes so far
	// retained holds each topic's current retained value (design.md,
	// Channels, $broadcast: "Retained is a flag, not a copy"), and latest
	// each latest channel topic's current record (latest_test.go).
	retained map[string]*fakeRecord
	latest   map[string]*fakeRecord
	seen     seen            // the delivery situations the run drove it through
	bytes    map[int64]int64 // each channel's bytes, by id (channels_test.go)
	// reserved is each channel's charges of held QoS 2 publications, by id
	// (design.md, PUBLISH: "a held copy stored and charged to the channel
	// before PUBREC"), which its stored bytes leave out (storage.md,
	// channels: "the bytes of its records").
	reserved map[int64]int64
	records  []*fakeRecord // every record stored, in offset order
	lost     int           // deliveries of QoS 1 messages so far, for lostDelivery
	// lastShared is each $share group's last selected member, by filter.
	lastShared map[string]string
	// reached and reads keep fakeReach's and fakeReads' answers, which the
	// configuration and the fault fix (channels_test.go).
	reached map[string][]int64
	reads   map[[2]string]bool
	// queues are the queues by channel id, dispatching guards their
	// dispatch against itself, and stopped ends their sweeps
	// (queue_test.go).
	queueTime   time.Time // nonzero: a scene controls queue intake and retention time, under mu
	queues      map[int64]*fakeQueue
	dispatching bool
	stopped     bool
	// writerFull counts the connections a scene had the fake cut as a
	// full writer (scene.cutWriter), its
	// saguin_disconnects_total{reason="writer_full"}.
	writerFull int
	// unreceipted, a scene's control, has the fake hold each QoS 2
	// PUBLISH it takes as new without sending its PUBREC, as a broker
	// whose store confirms the held copy only after the connection has
	// ended (design.md, Flows, Replies: "If the store confirms after that
	// connection has ended or been taken over, what was stored stays and
	// the reply is dropped").
	unreceipted bool
	// unreleased, a scene's control, has the fake drop each PUBREL it
	// reads, as a broker whose reader drops the PUBREL with its connection's
	// close (design.md, Flows, Closing a connection, 2: "Any other
	// unsubmitted packet is dropped"; CONNECT, 5: "Anything its reader still
	// delivers is ignored").
	unreleased bool
}

// fakeSession is one client id's session: its subscriptions, the
// deliveries waiting for it and those awaiting their PUBACK, and the
// connection holding it, nil while away.
type fakeSession struct {
	id       string // the client id
	user     string // the user its connection logged in as
	conn     *fakeConn
	durable  bool
	subs     map[string]*fakeSub // by filter
	queue    []*delivery         // waiting, in the order they go
	inflight []*delivery         // sent at QoS 1, awaiting PUBACK, in sending order
	held     *delivery           // with reordered, the delivery kept back
	// positions are the append channels it holds a position on, by id,
	// and kept, by id, those a durable session keeps with no subscription
	// reaching the channel, at the offset its next subscription reads from
	// (channels_test.go, unposition).
	positions map[int64]bool
	kept      map[int64]int64
	// waiting is the delayed Will of its last connection, while away,
	// published by its timer unless a reconnect settles it first.
	waiting *harness.Will
	// exchanges are its incoming QoS 2 publications held awaiting their
	// PUBREL, by packet identifier.
	exchanges map[uint16]*heldPublish
	// unsubscribed are the worker subscriptions it has left, by queue
	// (queue_test.go).
	unsubscribed map[string]*fakeSub
}

// heldPublish is a QoS 2 publication held before PUBREC (design.md, Flows,
// PUBLISH, the table): its topic, the PUBLISH, the channel it charges and
// what, and the connection it came on.
type heldPublish struct {
	topic   string
	p       *harness.Publish
	channel int64
	charge  int64
	conn    *fakeConn
}

// fakeConn is one connection. Everything it sends goes through out, in
// order, written by its own writer (write), so a client that is not
// reading holds up its own connection alone, never the lock.
type fakeConn struct {
	tcp      *net.TCPConn
	out      chan outgoing
	done     chan struct{} // closed when the writer has ended the sending
	finished bool          // out is closed: nothing more is sent
	version  byte
	expiry   uint32
	aliases  map[uint16]string // the client's Topic Alias mappings (MQTT 5 3.3.2.3.4)
	qos1     int               // QoS 1 PUBLISHes read, for countsAcked
	session  string            // the id of the session it holds, once accepted
	user     string            // the user its CONNECT logged in as
	// What the CONNECT asked of deliveries (design.md, Delivery): the
	// in-flight window, the client's Maximum Packet Size (0: MQTT's
	// largest), its Topic Alias Maximum, and Request Problem Information 0.
	window, maxPacket int
	aliasMax          uint16
	quiet             bool
	sentAliases       map[string]uint16 // the fake's own Topic Alias mappings, by topic
	unwritten         int               // QoS 0 deliveries handed to the writer and not yet written
	// will is the Will its CONNECT stored, nil for none or once discarded
	// or published (design.md, DISCONNECT).
	will *harness.Will
	// requests are the identifiers of its QoS 2 $saguin requests awaiting
	// their PUBREL, kept on the connection alone (seeks_test.go).
	requests map[uint16]bool
}

// outgoing is one packet for the writer; qos0 marks a QoS 0 delivery,
// which is in flight until written (design.md, Delivery: "QoS 0 settles on
// successful writing").
type outgoing struct {
	wire []byte
	qos0 bool
}

// outbox is how many packets a connection's writer may have waiting: more
// than a window of deliveries and the replies a client waits for.
const outbox = 1024

// send queues p, in the connection's version, for its writer.
func (c *fakeConn) send(p harness.Packet) {
	b, err := harness.Encode(p, c.version)
	if err == nil {
		c.queue(b, false) // a lost write shows in the runner as a missing answer
	}
}

// queue hands wire to the writer, unless the connection is finished. Once
// the CONNECT is accepted it runs under the lock.
func (c *fakeConn) queue(wire []byte, qos0 bool) {
	if !c.finished {
		c.out <- outgoing{wire, qos0}
	}
}

// finish ends what c sends: its writer writes what is queued, then ends
// the sending. Once the CONNECT is accepted it runs under the lock.
func (c *fakeConn) finish() {
	if !c.finished {
		c.finished = true
		close(c.out)
	}
}

// write is c's writer: it writes each packet queued, until a write fails,
// tells the fake of each QoS 0 delivery written, and ends the sending.
func (b *fake) write(c *fakeConn) {
	defer close(c.done)
	var failed error
	for o := range c.out {
		if failed == nil {
			_ = c.tcp.SetWriteDeadline(time.Now().Add(harness.DefaultTimeout)) // a failed write shows in the runner
			_, failed = c.tcp.Write(o.wire)
		}
		if o.qos0 {
			b.written(c)
		}
	}
	_ = c.tcp.CloseWrite() // the runner sees an end of stream, or the close
}

// readRaw reads one packet's bytes: the fixed header, a Remaining Length in
// its shortest form (design.md, Choices MQTT leaves open: refused in both
// versions) of at most four bytes, and the body. A packet over limit is
// read whole and reported too large.
func readRaw(r io.Reader, limit int) ([]byte, error) {
	raw := make([]byte, 1, 5)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, err
	}
	length := 0
	for i := 0; ; i++ {
		if i == 4 {
			return nil, harness.ErrMalformed
		}
		var d [1]byte
		if _, err := io.ReadFull(r, d[:]); err != nil {
			return nil, err
		}
		if i > 0 && d[0] == 0 {
			return nil, harness.ErrMalformed
		}
		raw = append(raw, d[0])
		length |= int(d[0]&0x7F) << (7 * i)
		if d[0]&0x80 == 0 {
			break
		}
	}
	header := len(raw)
	raw = append(raw, make([]byte, length)...)
	if _, err := io.ReadFull(r, raw[header:]); err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return raw, harness.ErrTooLarge
	}
	return raw, nil
}

// serve is one connection's life.
func (b *fake) serve(tcp *net.TCPConn) {
	c := &fakeConn{tcp: tcp, version: harness.V311, out: make(chan outgoing, outbox), done: make(chan struct{})}
	go b.write(c)
	defer func() {
		// R2's orderly close: end the sending, discard what the client
		// still sends until its end, then close.
		b.mu.Lock()
		c.finish()
		b.mu.Unlock()
		<-c.done
		_ = tcp.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.Copy(io.Discard, tcp)
		_ = tcp.Close()
	}()
	id, ok := b.connect(c)
	if !ok {
		return
	}
	// The Will goes once the connection has let its session go: never to
	// the connection itself, and to its session as to any other.
	defer b.ended(id, c)
	defer b.leave(id, c)
	for {
		raw, err := readRaw(tcp, maxPacketSize)
		var p harness.Packet
		if err == nil {
			p, err = harness.Decode(raw, c.version)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
			return
		}
		if !b.owns(id, c, func() bool { return b.handle(c, id, p, err) }) {
			return
		}
	}
}

// owns runs step, one packet's effect and reply, under the lock while c
// holds id's session, and reports whether the connection goes on: never
// once it has been taken over, which ends it with nothing more done.
func (b *fake) owns(id string, c *fakeConn, step func() bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.sessions[id]; s == nil || s.conn != c {
		return false
	}
	return step()
}

// handle answers p, or the error reading it, as the design has the broker
// answer it after the CONNACK, and reports whether the connection goes on.
// It runs under the lock (owns).
func (b *fake) handle(c *fakeConn, id string, p harness.Packet, err error) bool {
	if err != nil {
		b.refuse(c, err)
		return false
	}
	switch p := p.(type) {
	case *harness.Pingreq:
		c.send(&harness.Pingresp{})
		return true
	case *harness.Disconnect:
		sei, change := p.Props.Get(harness.SessionExpiryInterval)
		if !slices.Contains(append([]byte{0x00, 0x04}, clientReasons...), p.Reason) || (change && sei.Int > 0 && c.expiry == 0) {
			b.refuse(c, harness.ErrProtocol)
			return false
		}
		if change {
			b.sessions[id].durable = sei.Int > 0
		}
		// design.md, DISCONNECT: "0x00 discards the Will"; "An MQTT 3.1.1
		// DISCONNECT always discards the Will". Any other reason code leaves
		// it to go as on a lost connection.
		if (c.version != harness.V5 || p.Reason == 0) && b.fault != cleanWill {
			c.will = nil
		}
		b.release(id, c) // stored before the close (design.md, DISCONNECT)
		return false
	case *harness.Publish:
		return b.publish(c, id, p)
	case *harness.Ack:
		switch p.Kind {
		case harness.PUBACK, harness.PUBCOMP:
			b.settle(b.sessions[id], p)
			return true
		case harness.PUBREC:
			b.receipt(c, b.sessions[id], p)
			return true
		case harness.PUBREL:
			if !b.unreleased {
				b.releaseHeld(c, b.sessions[id], p.ID)
			}
			return true
		}
	case *harness.Subscribe:
		return b.subscribe(c, b.sessions[id], p)
	case *harness.Unsubscribe:
		return b.unsubscribe(c, b.sessions[id], p)
	}
	b.refuse(c, harness.ErrProtocol)
	return false
}

// publish answers p as design.md's PUBLISH flow has the broker answer it,
// storing what it takes before the PUBACK (R4), and reports whether the
// connection goes on. It runs under the lock (owns).
func (b *fake) publish(c *fakeConn, id string, p *harness.Publish) bool {
	topic := p.Topic
	if a, ok := p.Props.Get(harness.TopicAlias); ok {
		// MQTT 5 3.3.4: 0 or over the CONNACK's Topic Alias Maximum (16,
		// design.md, CONNECT, 8) gets 0x94; a zero length Topic Name uses
		// the alias's mapping, 0x82 where it has none; any other sets it.
		alias := uint16(a.Int)
		switch {
		case alias == 0 || alias > topicAliasMaximum:
			b.refuse(c, harness.ErrTopicAlias)
			return false
		case topic != "":
			if c.aliases == nil {
				c.aliases = map[uint16]string{}
			}
			c.aliases[alias] = topic
		case c.aliases[alias] == "":
			b.refuse(c, harness.ErrProtocol)
			return false
		default:
			topic = c.aliases[alias]
		}
	}
	// design.md, DISCONNECT, Reasons Sagüin sends: "0x9B [...] for [...]
	// QoS 2 [...] its CONNACK withheld". "An MQTT 3.1.1 connection just
	// closes". Its held identifiers too: the protocol error comes first
	// (Flows: "Checks proceed through framing, protocol recognition and
	// packet validity, admission policy, then storage").
	if p.QoS == 2 && b.fault != ignoredACL && denied(c.user, "qos2") {
		b.withheld(c, 0x9B)
		return false
	}
	// design.md, DISCONNECT, Reasons Sagüin sends: "0x9A [...] for
	// retained messages its CONNACK withheld", whatever the topic
	// (MQTT-3.2.2-14). "An MQTT 3.1.1 connection just closes".
	if p.Retain && b.fault != ignoredACL && denied(c.user, "retained") {
		b.withheld(c, 0x9A)
		return false
	}
	if p.QoS == 1 && b.fault == countsAcked && c.version == harness.V5 {
		c.qos1++
		if c.qos1 > receiveMaximum {
			c.send(&harness.Disconnect{Reason: 0x93})
			return false
		}
	}
	if q := b.fakeResponseQueue(topic); q != nil {
		return b.respond(c, b.sessions[id], q, p)
	}
	if ch, ok := fakeSeekChannel(topic); ok {
		return b.request(c, id, topic, ch, p)
	}
	if handled, goesOn := b.serviceRequest(c, id, topic, p); handled {
		return goesOn
	}
	s := b.sessions[id]
	if h := s.exchanges[p.ID]; p.QoS == 2 && h != nil {
		// design.md, PUBLISH: "While a QoS 2 PUBLISH's identifier is held
		// awaiting PUBREL, any later PUBLISH with that identifier gets PUBREC
		// again. It is not stored twice, or charged twice against its
		// channel's size, whatever its DUP flag".
		b.seen.repeated++
		if b.fault == twiceStored {
			b.distribute(b.store(id, h.topic, h.p, false))
		}
		c.send(&harness.Ack{Kind: harness.PUBREC, ID: p.ID})
		return true
	}
	reason := b.publishRefusal(c.user, topic, p.Props)
	if reason == 0 || reason == 0x83 {
		// design.md, PUBLISH: 0x97 for "a channel's max_bytes", taken in
		// the table's order, before 0x83.
		if full, _ := b.room(topic, p.Payload, p.Props); full != 0 {
			reason = full
			b.seen.full++
		}
	}
	var r *fakeRecord
	switch {
	case reason != 0:
	case p.QoS == 2:
		b.hold(c, s, topic, p)
	default:
		r = b.store(id, topic, p, false)
	}
	switch {
	case p.QoS == 0:
		// A refusal is "dropped and counted".
	case reason != 0 && c.version != harness.V5:
		return false // "an MQTT 3.1.1 client has no refusal code and is
		// disconnected"
	case p.QoS == 2 && reason == 0 && b.unreceipted:
		// Held, its PUBREC dropped with the connection (unreceipted).
	case p.QoS == 2:
		// "A QoS 2 refusal rides the PUBREC."
		c.send(&harness.Ack{Kind: harness.PUBREC, ID: p.ID, Reason: reason})
	default:
		c.send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID, Reason: reason})
	}
	if r != nil {
		b.distribute(r)
	}
	return true
}

// hold keeps p, a QoS 2 PUBLISH on topic, in s until its PUBREL, charged to
// its channel: design.md, PUBLISH, the table, QoS 2 "enters on PUBREL, from
// a held copy stored and charged to the channel before PUBREC". It runs
// under the lock (owns).
func (b *fake) hold(c *fakeConn, s *fakeSession, topic string, p *harness.Publish) {
	channel, _ := b.fakeRoute(topic)
	_, charge := b.room(topic, p.Payload, p.Props)
	b.reserved[channel] += charge
	if s.exchanges == nil {
		s.exchanges = map[uint16]*heldPublish{}
	}
	s.exchanges[p.ID] = &heldPublish{topic: topic, p: p, channel: channel, charge: charge, conn: c}
}

// releaseHeld answers a PUBREL for id: the held publication, its charge
// moved from reserved to stored, enters its channel, then PUBCOMP 0x00
// ("PUBREC, then PUBCOMP once in the channel"); an identifier s holds none
// for gets PUBCOMP 0x92 ("A PUBREL for an unknown identifier gets PUBCOMP
// 0x92"; MQTT 3.1.1's has no code). It runs under the lock (owns).
func (b *fake) releaseHeld(c *fakeConn, s *fakeSession, id uint16) {
	if c.requests[id] {
		// design.md, $saguin topics: a QoS 2 request's "identifier is held
		// [...] Nothing about the exchange is stored".
		delete(c.requests, id)
		c.send(&harness.Ack{Kind: harness.PUBCOMP, ID: id})
		return
	}
	h := s.exchanges[id]
	if h == nil {
		b.seen.unknownRelease++
		c.send(&harness.Ack{Kind: harness.PUBCOMP, ID: id, Reason: 0x92})
		return
	}
	delete(s.exchanges, id)
	b.seen.released++
	if h.conn != c {
		b.seen.releasedLater++
	}
	b.reserved[h.channel] -= h.charge
	r := b.store(s.id, h.topic, h.p, false)
	c.send(&harness.Ack{Kind: harness.PUBCOMP, ID: id})
	b.distribute(r)
}

// abandon drops what s holds as it ends: its held QoS 2 publications, their
// charges released (storage.md, held_publishes: "A non-durable session's
// rows are deleted when it ends"). It runs under the lock.
func (b *fake) abandon(s *fakeSession) {
	if s == nil {
		return
	}
	for _, h := range s.exchanges {
		b.reserved[h.channel] -= h.charge
	}
	s.exchanges = nil
}

// publishRefusal is the code design.md's PUBLISH table refuses a publish
// by user to topic with, before a channel's max_bytes, or 0: 0x87 where
// "the acl_file refuses the topic", first ("Policy reasons are taken in the
// order of the table"), but for a topic under $saguin/, which reaches it
// naming no request: "A publish under $saguin/ naming no request is refused
// with 0x90 without consulting the acl_file" ($saguin topics).
func (b *fake) publishRefusal(user, topic string, props harness.Properties) byte {
	levels := strings.Split(topic, "/")
	switch {
	case levels[0] == "$SYS" && b.fault == wrongRefusal:
		return 0x90
	case levels[0] == "$SYS":
		return 0x87
	case levels[0] == "$saguin":
		return 0x90 // "naming no request [...] without consulting the acl_file"
	case !b.fakeMay(user, topic, true):
		b.seen.unauthorized++
		return 0x87
	case len(topic) > maxTopicLength || len(levels) > maxTopicLevels || levels[0] == "$share":
		return 0x90
	}
	for _, q := range props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") && q.Str != "saguin-id" {
			return 0x83
		}
	}
	return 0
}

// store writes p, published to topic by publisher, as a record: its id the
// first saguin-id, else a UUIDv7 (design.md, Sagüin properties), and
// returns it for delivery, nil where lostRecord skips it. It runs under the
// lock (owns).
func (b *fake) store(publisher, topic string, p *harness.Publish, will bool) *fakeRecord {
	if p.QoS == 1 && !will {
		b.acked++
		if ch, _ := b.fakeRoute(topic); b.fault == lostRecord && b.acked%4 == 0 && !fakeLatest(ch) && b.fakeQueueOf(ch) == nil {
			return nil
		}
	}
	channel, _ := b.fakeRoute(topic)
	q := b.fakeQueueOf(channel)
	ms := time.Now().UnixMilli()
	if q != nil {
		ms = b.queueNow().UnixMilli()
	}
	id := fmt.Sprintf("%08x-%04x-7%03x-%x%03x-%012x", ms>>16, ms&0xFFFF, rand.IntN(1<<12), 8+rand.IntN(4), rand.IntN(1<<12), rand.Int64N(1<<48))
	if ids := p.Props.User("saguin-id"); len(ids) > 0 {
		id = ids[0]
	}
	b.offset++
	_, charge := b.room(topic, p.Payload, p.Props)
	b.bytes[channel] += charge
	if channel != 1 {
		b.seen.channelled++
	}
	_, _ = b.db.Exec(`UPDATE channels SET bytes = ? WHERE id = ?`, b.bytes[channel], channel)
	retain := p.Retain && channel == 1 // "A retain flag on a configured channel topic is ignored"
	retained := 0
	if retain {
		retained = 1
	}
	// storage.md, records: retained "1 while it is its topic's current
	// value: [...] on a latest channel, every record"; the fake keeps no
	// other flag in it, which the runner reads on a latest channel alone.
	current := 0
	if fakeLatest(channel) {
		current = 1
		b.replace(channel, topic)
	}
	props := slices.DeleteFunc(slices.Clone(p.Props), func(q harness.Property) bool { return q.ID == harness.TopicAlias })
	// storage.md: "a Property Length followed by the properties [...] NULL
	// for none"; expires "counted from its receipt".
	var stored []byte
	if len(props) > 0 {
		stored, _ = harness.AppendProperties(nil, props) // drawn valid by the runner
	}
	var expires any
	if mei, ok := props.Get(harness.MessageExpiryInterval); ok {
		expires = ms + int64(mei.Int)*1000
	}
	// storage.md, records: every column "is NOT NULL unless its row says
	// nullable", payload among them, so an empty payload is an empty BLOB,
	// never the NULL a nil slice binds as.
	payload := p.Payload
	if payload == nil {
		payload = []byte{}
	}
	// A lost write shows in the runner as a missing record.
	// The fake stores every payload as received (storage.md, A stored
	// payload: "one of equal length is not [compressed]"). A queue's
	// record and its job's row go in one transaction (storage.md,
	// queue_jobs; queue_test.go).
	if tx, err := b.db.Begin(); err == nil {
		_, _ = tx.Exec(`INSERT INTO records (channel, "offset", id, time, topic, qos, publisher, expires, properties, payload, payload_size, published_retained, retained)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, channel, b.offset, id, ms, topic, p.QoS, publisher, expires, stored, payload, len(payload), retained, current)
		if q != nil {
			_, _ = tx.Exec(`INSERT INTO queue_jobs (channel, "offset") VALUES (?, ?)`, channel, b.offset)
		}
		_ = tx.Commit()
	}
	r := &fakeRecord{channel: channel, id: id, ms: ms, offset: b.offset, topic: topic, qos: p.QoS, retain: retain, publisher: publisher, payload: p.Payload,
		props: props, received: time.Now()}
	if q != nil {
		b.newJob(q, r)
	}
	if current == 1 {
		b.latest[topic] = r
		if len(payload) == 0 {
			b.seen.latestDeleted++
		}
	}
	return r
}

// refuse answers a malformed packet or a protocol error after the CONNACK:
// in MQTT 5, DISCONNECT with its reason; the close follows. It finishes the
// sending at once, under the lock, so nothing queued later, such as a
// delivery a written QoS 0 frees room for, follows that last packet
// (design.md, Testing: "Its terminal DISCONNECT is its last outgoing MQTT
// packet"). It runs under the lock (owns).
func (b *fake) refuse(c *fakeConn, err error) {
	defer c.finish()
	if c.version != harness.V5 {
		return
	}
	reason := byte(0x82)
	switch {
	case errors.Is(err, harness.ErrTopicAlias):
		reason = 0x94
	case errors.Is(err, harness.ErrMalformed):
		reason = 0x81
	case errors.Is(err, harness.ErrTooLarge):
		reason = 0x95
	}
	c.send(&harness.Disconnect{Reason: reason})
}

// withheld ends c for a feature its CONNACK withheld: in MQTT 5,
// DISCONNECT with reason first (design.md, DISCONNECT, Reasons Sagüin sends:
// "0x9A, 0x9B or 0x9E for retained messages, QoS 2 or shared subscriptions
// its CONNACK withheld. These reasons are MQTT 5 only. An MQTT 3.1.1
// connection just closes"). As refuse does, it finishes the sending at
// once, under the lock, so no delivery another connection's packet queues
// follows that last packet (design.md, Testing: "Its terminal DISCONNECT is
// its last outgoing MQTT packet"). It runs under the lock (owns).
func (b *fake) withheld(c *fakeConn, reason byte) {
	if c.version == harness.V5 {
		c.send(&harness.Disconnect{Reason: reason})
	}
	c.finish()
}

// connect reads c's first packet and answers it; it returns the id of the
// session c holds once accepted, and false when the connection is to close.
func (b *fake) connect(c *fakeConn) (id string, ok bool) {
	raw, err := readRaw(c.tcp, maxConnectSize)
	if raw == nil {
		return "", false
	}
	// The Protocol Name and level, read by hand: the harness decodes no
	// other protocol than MQTT 3.1.1 and 5.
	header := 2
	for raw[header-1]&0x80 != 0 {
		header++
	}
	var name string
	var level byte
	if body := raw[header:]; len(body) >= 3 && len(body) >= 3+int(body[1]) {
		name, level = string(body[2:2+int(body[1])]), body[2+int(body[1])]
	}
	switch {
	case errors.Is(err, harness.ErrTooLarge):
		if raw[0] == 0x10 && name == "MQTT" && level == harness.V5 {
			c.version = harness.V5
			c.send(&harness.Connack{Reason: 0x95})
		}
		return "", false
	case raw[0] == 0x10 && ((name == "MQTT" && level != harness.V311 && level != harness.V5) || (name == "MQIsdp" && level == 3)):
		c.send(&harness.Connack{Reason: 1})
		return "", false
	}
	p, err := harness.Decode(raw, harness.V5)
	connect, ok := p.(*harness.Connect)
	if err != nil || !ok {
		return "", false
	}
	c.version = connect.Version
	if reason := b.refusal(connect); reason != 0 {
		b.mu.Lock()
		switch {
		case reason == 0x86 || (c.version == harness.V311 && reason == 4):
			b.seen.wrongLogins++
		case reason == 0x87 || reason == 0x9A:
			b.seen.deniedWills++
		}
		b.mu.Unlock()
		ack := &harness.Connack{Reason: reason}
		if mps, ok := connect.Props.Get(harness.MaximumPacketSize); ok && c.version == harness.V5 && b.fault == oversizeRefusal {
			ack = overLimit(ack, int(mps.Int))
		}
		c.send(ack)
		return "", false
	}
	return b.accept(c, connect)
}

// refusal is the CONNECT refusal the design gives p, in p's version, or 0.
// The runner's acl_file (writeACL) allows every topic "#" matches, which no
// topic beginning with $ is [MQTT-4.7.2-1], and denies the features of
// denials; with ignoredACL, the fake reads none.
func (b *fake) refusal(p *harness.Connect) byte {
	v5 := p.Version == harness.V5
	code := func(v5code, v311code byte) byte {
		if v5 {
			return v5code
		}
		return v311code
	}
	id := p.ClientID
	if len(id) > maxClientIDLength || strings.IndexFunc(id, unicode.IsControl) >= 0 || (id == "" && !p.CleanStart) {
		return code(0x85, 2)
	}
	// 0x86, "a wrong or missing credential": the fake is configured as the
	// runner configures its broker, with the runner's password file, so
	// allow_anonymous is false (config.md, broker.mqtt).
	if !p.HasUsername || !p.HasPassword || !slices.Contains(logins, login{p.Username, string(p.Password)}) {
		return code(0x86, 4)
	}
	acl := b.fault != ignoredACL
	if w := p.Will; w != nil {
		// design.md, CONNECT: "0x87 | the Will: will denied, or its topic
		// not allowed", before 0x90 and 0x9A.
		if acl && (denied(p.Username, "will") || !b.fakeMay(p.Username, w.Topic, true)) {
			return code(0x87, 5)
		}
		levels := strings.Split(w.Topic, "/")
		if w.Topic == "" || strings.ContainsAny(w.Topic, "+#") || len(w.Topic) > maxTopicLength ||
			len(levels) > maxTopicLevels || levels[0] == "$share" || levels[0] == "$saguin" {
			return code(0x90, 5)
		}
		// "0x9A | a retained Will, on any topic, with retained denied
		// (MQTT-3.2.2-13)".
		if acl && w.Retain && denied(p.Username, "retained") {
			return code(0x9A, 5)
		}
		// "0x9B | a QoS 2 Will with qos2 denied".
		if acl && w.QoS == 2 && denied(p.Username, "qos2") {
			return code(0x9B, 5)
		}
	}
	for _, q := range p.Props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") {
			return 0x83
		}
		if q.ID == harness.AuthenticationMethod {
			return 0x8C
		}
	}
	return 0
}

// overLimit returns ack with a Reason String that makes it larger than
// limit bytes, otherwise valid, or ack itself where no Reason String can.
func overLimit(ack *harness.Connack, limit int) *harness.Connack {
	wire, err := harness.Encode(ack, harness.V5)
	// The Reason String's identifier and length take 3 bytes; a longer
	// Remaining Length only makes the packet larger still.
	n := limit + 1 - len(wire) - 3
	if err != nil || n < 0 || n > math.MaxUint16 {
		return ack
	}
	padded := *ack
	padded.Props = append(slices.Clone(ack.Props), harness.Property{ID: harness.ReasonString, Str: strings.Repeat("r", n)})
	return &padded
}

// accept takes the session for p's client id over, sends the CONNACK, and
// returns the id, or false when the CONNACK is too large for the client.
// The takeover, the old connection's DISCONNECT 0x8E and the end of its
// sending, is one locked step with the ownership change, so nothing the old
// connection does can follow its 0x8E; the CONNACK is sent in that step
// too, so no later takeover's 0x8E can precede it, and so are the
// deliveries a kept session resumes (design.md, CONNECT, 9).
func (b *fake) accept(c *fakeConn, p *harness.Connect) (id string, accepted bool) {
	ack := &harness.Connack{}
	// design.md, CONNECT: "A client whose persistent is denied is accepted
	// with Session Expiry 0"; 8: the CONNACK carries "the Maximum QoS, Retain
	// Available and Shared Subscription Available the acl_file allows".
	acl := b.fault != ignoredACL
	persistent := !acl || !denied(p.Username, "persistent")
	durable := !p.CleanStart && persistent
	id = p.ClientID
	// design.md, Delivery: "The window is the lower of the client's Receive
	// Maximum and limits.max_inflight, or the cap alone for MQTT 3.1.1".
	c.window = fakeInflight
	if c.version == harness.V5 {
		if rm, ok := p.Props.Get(harness.ReceiveMaximum); ok {
			c.window = min(c.window, int(rm.Int))
		}
		mps, _ := p.Props.Get(harness.MaximumPacketSize)
		tam, _ := p.Props.Get(harness.TopicAliasMaximum)
		rpi, ok := p.Props.Get(harness.RequestProblemInformation)
		c.maxPacket, c.aliasMax, c.quiet = int(mps.Int), uint16(tam.Int), ok && rpi.Int == 0
		sei, _ := p.Props.Get(harness.SessionExpiryInterval)
		c.expiry = min(sei.Int, maxSessionExpiry)
		if !persistent {
			c.expiry = 0
		}
		durable = c.expiry > 0
		ack.Props = harness.Properties{
			{ID: harness.ReceiveMaximum, Int: receiveMaximum},
			{ID: harness.MaximumPacketSize, Int: maxPacketSize},
			{ID: harness.TopicAliasMaximum, Int: topicAliasMaximum},
		}
		if acl && denied(p.Username, "qos2") {
			ack.Props = append(ack.Props, harness.Property{ID: harness.MaximumQoS, Int: 1})
		}
		if c.expiry != sei.Int {
			ack.Props = append(ack.Props, harness.Property{ID: harness.SessionExpiryInterval, Int: c.expiry})
		}
		if acl && denied(p.Username, "retained") {
			ack.Props = append(ack.Props, harness.Property{ID: harness.RetainAvailable, Int: 0})
		}
		if acl && denied(p.Username, "share") {
			ack.Props = append(ack.Props, harness.Property{ID: harness.SharedSubscriptionAvailable, Int: 0})
		}
		if p.KeepAlive == 0 || p.KeepAlive > maxKeepalive {
			ack.Props = append(ack.Props, harness.Property{ID: harness.ServerKeepAlive, Int: maxKeepalive})
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if id == "" {
		// A new id for each (MQTT-3.2.2-16; 3.1.1 MQTT-3.1.3-6), in the
		// design's form; MQTT 3.1.1's client never sees it.
		b.assigned++
		id = time.Now().UTC().Format("060102150405") + fmt.Sprintf("%06d", b.assigned)
		if c.version == harness.V5 {
			ack.Props = append(ack.Props, harness.Property{ID: harness.AssignedClientIdentifier, Str: id})
		}
	}
	s := b.sessions[id]
	// design.md, CONNECT: a session is kept when "away or connected with a
	// session expiry above 0. Keeping the session means Clean Start 0, or
	// MQTT 3.1.1 Clean Session 0. A takeover of a session whose expiry is 0
	// ends it".
	kept := s != nil && (s.conn == nil || s.durable) && !p.CleanStart
	// design.md, CONNECT, 5 and 6: the old connection's Will "is published
	// if it has no delay"; a delayed one is cancelled where the session is
	// kept, published where it ends; an away session's waiting Will is
	// cancelled by a reconnect keeping the session, published by a clean
	// start.
	// Each is stored before the CONNACK, in the change the CONNACK
	// follows (CONNECT, 7 and 8), and goes to its subscribers once the
	// CONNACK is queued, never to the connection taken over.
	var wills []*harness.Will
	if s != nil && s.conn != nil && s.conn.will != nil {
		if !kept || willDelay(s.conn.will) == 0 || !s.durable {
			wills = append(wills, s.conn.will)
		} else {
			b.seen.cancelled++
		}
		s.conn.will = nil
	}
	if s != nil && s.waiting != nil {
		if !kept {
			wills = append(wills, s.waiting)
		} else {
			b.seen.cancelled++
		}
		s.waiting = nil
	}
	var stored []*fakeRecord
	for _, w := range wills {
		if r := b.storeWill(id, w); r != nil {
			stored = append(stored, r)
		}
	}
	defer func() {
		for _, r := range stored {
			b.distribute(r)
		}
	}()
	if s != nil && s.conn != nil {
		b.workerGone(s.conn) // its worker goes (queue_test.go)
		if b.fault == cutTakeover {
			if wire, err := harness.Encode(&harness.Disconnect{Reason: 0x8E}, s.conn.version); err == nil {
				s.conn.queue(wire[:1], false) // the rest never comes
			}
		} else if s.conn.version == harness.V5 && b.fault != silentTakeover {
			s.conn.send(&harness.Disconnect{Reason: 0x8E})
			if b.fault == lateReply {
				s.conn.send(&harness.Pingresp{})
			}
		}
		s.conn.finish() // its own goroutine ends the rest
		if kept {
			s.away()
		}
	}
	ack.SessionPresent = kept && b.fault != lostSession
	if !kept {
		b.abandon(s)
		s = &fakeSession{id: id, subs: map[string]*fakeSub{}, positions: map[int64]bool{}, kept: map[int64]int64{}}
		b.sessions[id] = s
	} else if len(s.subs) > 0 {
		b.seen.resumed++
	}
	s.conn, s.durable, c.session, c.will = c, durable, id, p.Will
	s.user, c.user = p.Username, p.Username
	wire, err := harness.Encode(ack, c.version)
	if err != nil || (c.maxPacket > 0 && c.maxPacket < len(wire)) {
		b.release(id, c)
		b.lose(id, c)
		return "", false
	}
	c.queue(wire, false) // a lost write shows in the runner as a missing answer
	b.pump(s)
	return id, true
}

// ended publishes c's Will, if it still holds one, when c ends: design.md,
// DISCONNECT, with no Will Delay "The Will goes [...] at once" (MQTT 3.1.1
// always), and CONNECT, 5, a takeover's: "Its Will is published if it has
// no delay".
func (b *fake) ended(id string, c *fakeConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lose(id, c)
}

// lose settles c's Will at its end, under the lock: design.md, DISCONNECT,
// with a Will Delay, the Will goes "after its Will Delay [...] A reconnect in
// time cancels it. If the session ends first, the Will goes early": one
// ending with its connection.
func (b *fake) lose(id string, c *fakeConn) {
	if s := b.sessions[id]; c.will != nil && willDelay(c.will) > 0 && s != nil && s.conn == nil && s.durable {
		w := c.will
		c.will, s.waiting = nil, w
		time.AfterFunc(willDelay(w), func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if s.waiting == w {
				s.waiting = nil
				b.publishWillOf(id, w)
			}
		})
		return
	}
	b.publishWill(id, c)
}

// willDelay is w's Will Delay Interval, 0 for none.
func willDelay(w *harness.Will) time.Duration {
	d, _ := w.Props.Get(harness.WillDelayInterval)
	return time.Duration(d.Int) * time.Second
}

// publishWill publishes c's Will as id's record, its Will Delay left out,
// and delivers it, under the lock.
func (b *fake) publishWill(id string, c *fakeConn) {
	w := c.will
	if w == nil {
		return
	}
	c.will = nil
	b.publishWillOf(id, w)
}

// publishWillOf publishes w as id's record, under the lock.
func (b *fake) publishWillOf(id string, w *harness.Will) {
	if r := b.storeWill(id, w); r != nil {
		b.distribute(r)
	}
}

// storeWill stores w as id's record, under the lock, and returns it; nil
// where its channel is full, the Will dropped. Its deliveries are the
// caller's (distribute).
func (b *fake) storeWill(id string, w *harness.Will) *fakeRecord {
	b.seen.wills++
	if b.fault == anonymousWill {
		id = ""
	}
	p := &harness.Publish{QoS: w.QoS, Retain: w.Retain, Topic: w.Topic, Payload: w.Payload,
		Props: slices.DeleteFunc(slices.Clone(w.Props), func(q harness.Property) bool { return q.ID == harness.WillDelayInterval })}
	if full, _ := b.room(w.Topic, p.Payload, p.Props); full != 0 {
		return nil // design.md, DISCONNECT: dropped "if [...] its channel [...] is full"
	}
	return b.store(id, w.Topic, p, true)
}

// leave ends c's hold on id's session, under the lock.
func (b *fake) leave(id string, c *fakeConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.release(id, c)
}

// release ends c's hold on id's session: away if durable, else ended. A
// connection taken over holds it no longer. It runs under the lock.
func (b *fake) release(id string, c *fakeConn) {
	b.workerGone(c) // its worker goes (queue_test.go)
	if s, ok := b.sessions[id]; ok && s.conn == c {
		if s.durable {
			s.away()
		} else {
			b.abandon(s)
			delete(b.sessions, id)
		}
	}
}

// fuzz runs the runner and returns its exit code and everything it
// printed, in order.
func fuzz(t *testing.T, args ...string) (exit int, output string) {
	t.Helper()
	exit, output, _ = testkit.RunCommand(run, testkit.Combined, args...)
	return exit, output
}

// watchdog is the -duration of a run counted by -steps whose steps, not
// its speed, are what the test checks. design.md, Testing: "A watchdog
// bounds the test and is reported separately"; with -steps the duration is
// that watchdog, so a slow machine taking the steps in more time is no
// failure. It is the time the test binary has left, less a margin for the
// run's last checks and its report, at most the longest run -duration
// allows; with no test deadline, that longest run.
func watchdog(t *testing.T) string {
	t.Helper()
	d := longestRun
	if deadline, ok := t.Deadline(); ok {
		d = min(d, max(time.Until(deadline)-time.Minute, time.Second))
	}
	return d.Round(time.Second).String()
}

// watchdogExpired reports whether out, a run's output, ends on the run's
// watchdog, its -duration passed before its -steps were taken, rather than
// on a check.
func watchdogExpired(out string) bool {
	return strings.Contains(out, "e2efuzz: FAIL: watchdog: ")
}

// TestNoBrokerFails: design.md, Testing, "A test tool that finds no broker
// to drive fails, naming the binary; a run that tested nothing never
// passes" (decided from item 0008a): a broker binary that does not exist
// fails the run, naming it.
func TestNoBrokerFails(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "saguin")
	code, out := fuzz(t, "-binary", missing, "-duration", "1s")
	if code != 1 || !strings.Contains(out, missing) {
		t.Errorf("exit %d, want 1, naming %s; output:\n%s", code, missing, out)
	}
}

// TestRunWithoutAFullMemoryFails: 0020a.md, 261019 184809: "the fuzzer
// reads saguin_memory_full_seconds_total at the end of the default run and
// fails it if the value never rose above 0 (metrics.md's line, Message
// memory's Used up), shown failing with flood's weight at 0", now the
// incoming budget's, saguin_memory_full_seconds_total{direction="incoming"}
// (metrics.md, Store and memory). design.md, Testing: "A run that tested
// nothing never passes"; Message memory, Two queues: "When the head of a
// FIFO does not fit in its budget, it waits". The same default run, on the
// broker make builds, passes with the flood action at its weight, its
// bodies filling max_incoming_memory, and fails, naming the series at 0,
// with the flood action never drawn: the check is met by the floods, not by
// any run. Each run takes its 1000 steps whatever the machine's speed, its
// -duration the watchdog (watchdog), and a run its watchdog ends is
// reported as the watchdog, not as the check it never reached (design.md,
// Testing: "A watchdog bounds the test and is reported separately").
func TestRunWithoutAFullMemoryFails(t *testing.T) {
	flood := reflect.ValueOf((*fuzzer).flood).Pointer()
	i := slices.IndexFunc(actions, func(a action) bool { return reflect.ValueOf(a.do).Pointer() == flood })
	if i < 0 {
		t.Fatal("no flood action to leave out")
	}
	weight := actions[i].weight
	t.Cleanup(func() { actions[i].weight = weight })
	run := func(what string) (int, string) {
		t.Helper()
		code, out := fuzz(t, "-binary", "../../../../bin/saguin", "-seed", "1", "-clients", "8", "-steps", "1000", "-duration", watchdog(t))
		if watchdogExpired(out) {
			t.Fatalf("watchdog: %s, the run did not take its 1000 steps in the time the test has; output:\n%s", what, out)
		}
		return code, out
	}
	if code, out := run("with floods"); code != 0 {
		t.Errorf("with floods: exit %d, want 0; output:\n%s", code, out)
	}
	actions[i].weight = 0
	code, out := run("without floods")
	if code != 1 || !strings.Contains(out, memoryFull+" is 0 at the end of the run") {
		t.Errorf("without floods: exit %d, want 1 naming %s at 0; output:\n%s", code, memoryFull, out)
	}
}

// TestMemoryDirIsKeptOnlyAfterAFailure: a memory-mode run's broker keeps
// its store's RAM copy in memory_dir, a folder the run makes in ramFolder,
// /dev/shm, RAM on the maintainer's machine. A run that passes leaves
// nothing there to look at; one that fails keeps the folder for whoever
// takes the failure up, and names it (audit finding 261109-001a, whose
// failed run's 280 MB database was found kept there). The default run of
// TestRunWithoutAFullMemoryFails, in memory mode, its watchdog as that
// test's, passes and leaves no new folder in ramFolder; the same run given far more steps than its
// -duration allows fails on the watchdog and names the memory_dir it
// keeps, which is there.
func TestMemoryDirIsKeptOnlyAfterAFailure(t *testing.T) {
	folders := func() []string {
		found, err := filepath.Glob(filepath.Join(ramFolder, "saguin-e2efuzz-*"))
		if err != nil {
			t.Fatal(err)
		}
		return found
	}
	before := folders()
	args := []string{"-binary", "../../../../bin/saguin", "-mode", "memory", "-seed", "1", "-clients", "8"}
	code, out := fuzz(t, append(slices.Clone(args), "-steps", "1000", "-duration", watchdog(t))...)
	if watchdogExpired(out) {
		t.Fatalf("watchdog: a memory-mode run did not take its 1000 steps in the time the test has; output:\n%s", out)
	}
	if code != 0 {
		t.Fatalf("a memory-mode run: exit %d, want 0; output:\n%s", code, out)
	}
	if left := slices.DeleteFunc(folders(), func(f string) bool { return slices.Contains(before, f) }); len(left) != 0 {
		t.Errorf("a memory-mode run passed and left %v in %s; want nothing", left, ramFolder)
	}

	code, out = fuzz(t, append(slices.Clone(args), "-steps", "1000000", "-duration", "1s")...)
	const kept = "e2efuzz: its memory_dir is kept in "
	_, after, named := strings.Cut(out, kept)
	if code != 1 || !named {
		t.Fatalf("a memory-mode run failing on its watchdog: exit %d, want 1 naming the memory_dir it keeps; output:\n%s", code, out)
	}
	dir, _, _ := strings.Cut(after, "\n")
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(dir)) }) // RAM: the test has looked
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Errorf("a memory-mode run failed and named %s as kept: %v; want the folder there", dir, err)
	}
}

// TestRunPasses: against a broker that answers as the design says, a run
// passes, its seed printed first and its steps counted. 0029a.md, the
// maintainer: it "proves no false alarm once the new rules run, asserting
// that subscriptions and deliveries happened (withheld acknowledgements
// filling a window, overlapping subscriptions, a delivery over a Maximum
// Packet Size, an UNSUBSCRIBE with a delivery in flight, a durable
// reconnect holding subscriptions), so it cannot pass on a run that never
// subscribed". design.md, Delivery: "The window is the lower of the client's
// Receive Maximum and limits.max_inflight"; Flows, SUBSCRIBE, One copy: "a
// record matching several of a session's non-shared subscriptions is sent
// once"; Delivery: "A message over the client's Maximum Packet Size is
// discarded for that subscriber as if sent, counted as missed and logged
// (MQTT-3.1.2-25)"; UNSUBSCRIBE: "In-flight messages are finished"; CONNECT,
// 9: "in-flight messages [...] resends them with their packet identifiers and
// in their order. Resent messages have DUP set"; DISCONNECT, with no Will
// Delay, "The Will goes [...] at once" when its connection ends but by
// "0x00"; Delivery, Positions: "Once a successful PUBREC is accepted for an
// outgoing QoS 2 exchange, every further PUBREC for it is answered with a
// successful PUBREL"; Flows, SUBSCRIBE, A shared group: "Each message goes
// to one member with room"; Channels, latest: "It holds one value per
// topic, replaced in place at a new offset [...] An empty payload deletes
// the value and leaves a tombstone [...] Returning consumers receive
// surviving tombstones as empty payloads"; SUBSCRIBE, the subscription
// table: "latest | the current values, as Retain Handling allows, then
// each change"; Delivery, Order: "On latest, they follow offset order of
// changes and skip replaced values"; Channels, queue: "A subscription to
// $saguin/queue/<channel> at QoS 1 makes a worker [...] Retention never
// deletes an unfinished job. It moves the job to the append channel
// <channel>__dlq"; Delivery, Queue jobs: "The hold starts at the worker's
// PUBACK and lasts visibility_timeout [...] A failed attempt is offered
// again after a wait set by retry.mode [...] at its return, a refusing
// PUBACK, its timeout or the worker's going. [...] When an attempt fails
// with retry.max_attempts spent, the job moves to the dead-letter channel
// as attempts_exhausted"; $saguin topics: "A response naming an attempt
// that is not current or not this session's, or with another payload,
// changes nothing".
func TestRunPasses(t *testing.T) {
	b := fakeBroker(t, "")
	// The seed must reach every situation below within its 3000 steps of
	// six clients. With the queue's draws (0017a, 0017c, 0017d) seed 29 no
	// longer reaches every one. Of seeds 1 to 30, seed 9 reaches them all
	// and takes the fewest seconds; it did in each of eight runs, its
	// rarest a delayed Will
	// cancelled and a QoS 2 exchange completed on a later connection (1
	// each). The queue's situations listed come in tens every run; those
	// its timers bring about only now and then (a hold's timeout, a
	// refusing PUBACK, a job expired or out of attempts, an offer resent)
	// are shown by scenes run without a fault (queue_test.go, scene). So
	// is a job dead-lettered as evicted, which comes only when a burst
	// lands on the queue before its jobs are taken, which load on the
	// machine can prevent: TestQueueEvictionIsReached. A
	// recovered exchange of a replaced latest value comes in none of the
	// seeds that reach everything, as how often it comes depends on
	// timing: it is shown reached by TestLatestReplacedResendIsCaught.
	code, out := fuzz(t, "-addr", b.addr, "-db", b.dbPath, "-seed", "9", "-clients", "6", "-steps", "3000", "-duration", watchdog(t), "-timeout", "2s")
	if code != 0 || !strings.HasPrefix(out, "e2efuzz: seed 9\n") || !strings.Contains(out, "ok, seed 9: 3001 steps") {
		t.Fatalf("exit %d, output:\n%s", code, out)
	}
	b.mu.Lock()
	got := b.seen
	b.mu.Unlock()
	for _, s := range []struct {
		what  string
		times int
	}{
		{"deliveries waiting for acknowledgements a subscriber withheld, its window full", got.waited},
		{"a delivery through overlapping subscriptions", got.overlapping},
		{"a delivery over its subscriber's Maximum Packet Size", got.tooLarge},
		{"an UNSUBSCRIBE of a filter with a delivery through it in flight", got.unsubscribed},
		{"a durable session reconnected with its subscriptions", got.resumed},
		{"a Will published", got.wills},
		{"a record on a configured channel", got.channelled},
		{"a PUBLISH refused for its channel's max_bytes", got.full},
		{"a delayed Will cancelled", got.cancelled},
		{"a position made on an append channel", got.positions},
		{"a position a durable session kept with no subscription read again by its next one", got.kept},
		{"a record retention deleted", got.retention},
		{"a CONNECT refused for its credential", got.wrongLogins},
		{"a Will the acl_file refused", got.deniedWills},
		{"a PUBLISH or a filter refused for the acl_file's rules", got.unauthorized},
		{"a record skipped for a subscription whose user may not read it", got.unreadable},
		{"a QoS 2 publication entering its channel on PUBREL", got.released},
		{"a QoS 2 exchange completed on a later connection than its PUBLISH's", got.releasedLater},
		{"a QoS 2 PUBLISH repeating a held identifier", got.repeated},
		{"a PUBREL for an identifier no exchange holds", got.unknownRelease},
		{"a QoS 2 delivery's PUBREC 0x00 answered with its PUBREL", got.receipted},
		{"a repeated PUBREC answered with PUBREL 0x00", got.repeatedReceipt},
		{"a PUBREC naming no delivery", got.strayReceipt},
		{"a seek answered", got.answered},
		{"a delivery through a $share filter", got.shared},
		{"a record for a $share group of two or more connected members", got.sharedAmong},
		{"a latest value replaced in place", got.latestReplaced},
		{"a latest tombstone stored", got.latestDeleted},
		{"a latest channel's current value sent to an initial pass", got.latestPass},
		{"a replaced latest value skipped", got.latestSkipped},
		{"a latest change made while its session was away delivered on its return", got.latestReturned},
		{"a queue job stored", got.jobs},
		{"a worker offer", got.offers},
		{"a worker's hold begun by its PUBACK", got.held},
		{"a job finished by its worker's ack", got.completed},
		{"an attempt ended by return", got.returned},
		{"an attempt ended by its worker's going", got.workerGone},
		{"a response that changed nothing", got.staleResponses},
	} {
		if s.times == 0 {
			t.Errorf("the run passed without %s; its last lines:\n%s", s.what, out[max(0, strings.LastIndex(out, "e2efuzz: Wills")):])
		}
	}
}

// TestStepsOutlastingDurationFail: design.md, Testing: "A fuzz run
// asserting an exact step count disables successful duration-based
// termination. [...] A watchdog bounds the test and is reported
// separately". A run given -steps whose -duration passes first has not
// taken its steps: it fails, naming the watchdog and the steps taken of
// those asked, and never reports ok.
func TestStepsOutlastingDurationFail(t *testing.T) {
	b := fakeBroker(t, "")
	code, out := fuzz(t, "-addr", b.addr, "-db", b.dbPath, "-seed", "1", "-clients", "4", "-steps", "1000000", "-duration", "1s", "-timeout", "2s")
	named := false
	for line := range strings.SplitSeq(out, "\n") {
		named = named || (strings.HasPrefix(line, "e2efuzz: FAIL") && strings.Contains(line, "watchdog") && strings.Contains(line, " of 1000000 steps"))
	}
	if code != 1 || !named || strings.Contains(out, "e2efuzz: ok") {
		t.Errorf("exit %d, want 1 with a line \"e2efuzz: FAIL\" naming the watchdog and the steps taken \"of 1000000 steps\", and no ok; output:\n%s", code, out)
	}
}

// TestFaultsAreCaught: each fault planted in the broker fails the run,
// named by the check of the rule it breaks, in a scene of its own
// (faultScenes, scene_test.go) that reaches it whatever the machine's load;
// the same scene without the fault passes.
func TestFaultsAreCaught(t *testing.T) {
	for _, f := range slices.Sorted(maps.Keys(faultScenes)) {
		sc := faultScenes[f]
		caughtInScene(t, f, sceneQueue, sc.want, sc.script)
	}
}

// caught runs the runner against a fake broker with f planted and checks
// the run fails, its failure naming each of want, with the last steps and
// the command that replays it. That command, run as make runs it against
// a fault-free fake broker of its own (in place of the -addr and -db
// it names, the first fake's), takes the same steps through the failure.
// Client arrival order is input too (design.md, Testing), so replaying
// the draws need not reproduce a fault that depends on broker timing;
// leaving it planted could also stop the replay at an earlier step.
func caught(t *testing.T, f fault, want ...string) {
	t.Helper()
	caughtAt(t, f, "1", want...)
}

// caughtAt is caught with the run's seed, for a fault seed 1's steps do
// not reach within the run.
func caughtAt(t *testing.T, f fault, seed string, want ...string) {
	t.Helper()
	b := fakeBroker(t, f)
	code, out := fuzz(t, "-addr", b.addr, "-db", b.dbPath, "-seed", seed, "-clients", "4", "-duration", "20s", "-timeout", "2s", "-v")
	_, failure, _ := strings.Cut(out, "e2efuzz: FAIL at step ")
	failure, _, _ = strings.Cut(failure, "\ne2efuzz: the last ")
	named := failure != ""
	for _, w := range want {
		named = named && strings.Contains(failure, w)
	}
	step, _, _ := strings.Cut(failure, ":")
	wantCommand := fmt.Sprintf("make e2efuzz E2EFUZZ_DURATION=20s E2EFUZZ_FLAGS=\"-seed %s -clients 4 -steps %s -addr %s -db %s -timeout 2s\"",
		seed, step, b.addr, b.dbPath)
	if code != 1 || !named || !strings.Contains(out, "the last ") {
		t.Errorf("%s: exit %d, want 1, the failure naming %q; output:\n%s", f, code, want, out)
		return
	}
	_, command, _ := strings.Cut(out, "e2efuzz: replay with ")
	command, _, _ = strings.Cut(command, "\n")
	if command != wantCommand {
		t.Errorf("%s: replay command %q; want %q", f, command, wantCommand)
	}
	// make e2efuzz runs the runner with -duration $(E2EFUZZ_DURATION)
	// $(E2EFUZZ_FLAGS).
	duration, flags, _ := strings.Cut(strings.TrimPrefix(command, "make e2efuzz E2EFUZZ_DURATION="), " E2EFUZZ_FLAGS=")
	flags, err := strconv.Unquote(flags)
	if err != nil {
		t.Fatal(err)
	}
	fresh := fakeBroker(t, "")
	args := append([]string{"-duration", duration}, strings.Fields(flags)...)
	for i, a := range args {
		switch a {
		case b.addr:
			args[i] = fresh.addr
		case b.dbPath:
			args[i] = fresh.dbPath
		}
	}
	// -v observes the draws without changing them. A step can log several
	// lines; compare every line of the failed run, including its last step.
	// A replay may log more of that step or reach the final checks.
	_, again := fuzz(t, append(args, "-v")...)
	steps := func(output string) []string {
		return slices.DeleteFunc(strings.Split(output, "\n"), func(line string) bool {
			return !strings.HasPrefix(line, "step ")
		})
	}
	taken, replayed := steps(out), steps(again)
	if len(taken) == 0 || len(replayed) < len(taken) || !slices.Equal(taken, replayed[:len(taken)]) {
		t.Errorf("%s: replay did not take the same draws through failing step %s; original steps:\n%s\nreplay output:\n%s", f, step, strings.Join(taken, "\n"), again)
	}
}

// TestOneSeedOneSequence: design.md, Testing: "Each run's seed is recorded so
// a failure can be rerun with the same steps". The same seed takes the same
// steps, in the same order; another seed, others.
func TestOneSeedOneSequence(t *testing.T) {
	steps := func(seed string) []string {
		// A broker of its own for each run: -db holds the run's records alone.
		b := fakeBroker(t, "")
		code, out := fuzz(t, "-addr", b.addr, "-db", b.dbPath, "-seed", seed, "-clients", "5", "-steps", "400", "-duration", watchdog(t), "-timeout", "2s", "-v")
		if code != 0 {
			t.Fatalf("seed %s: exit %d, output:\n%s", seed, code, out)
		}
		lines := slices.DeleteFunc(strings.Split(out, "\n"), func(l string) bool { return !strings.HasPrefix(l, "step ") })
		// A step sending several PUBLISHes logs a line for each.
		taken := map[string]bool{}
		for _, l := range lines {
			step, _, _ := strings.Cut(l, ":")
			taken[step] = true
		}
		if len(taken) != 401 {
			t.Fatalf("seed %s: %d steps logged, want 401 (400 and the last health check)", seed, len(taken))
		}
		return lines
	}
	first, again, other := steps("42"), steps("42"), steps("43")
	if !slices.Equal(first, again) {
		t.Errorf("seed 42 twice took different steps:\n%s\n---\n%s", strings.Join(first, "\n"), strings.Join(again, "\n"))
	}
	if slices.Equal(first, other) {
		t.Error("seeds 42 and 43 took the same steps")
	}
}

// TestPredict checks the model's oracle against design.md's CONNECT, row
// by row and at each limit options sets.
func TestPredict(t *testing.T) {
	// v5 and v311 log in as the password file's first user.
	v5 := func(id string, clean bool, props ...harness.Property) *harness.Connect {
		return signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: id, CleanStart: clean, Props: props}, logins[0].user, logins[0].password)
	}
	v311 := func(id string, clean bool) *harness.Connect {
		return signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V311, ClientID: id, CleanStart: clean}, logins[0].user, logins[0].password)
	}
	// password gives p's user another password; anonymous takes p's user
	// name and password away.
	password := func(p *harness.Connect, password string) *harness.Connect {
		return signIn(p, p.Username, password)
	}
	anonymous := func(p *harness.Connect) *harness.Connect {
		p.HasUsername, p.Username, p.HasPassword, p.Password = false, "", false, nil
		return p
	}
	will := func(p *harness.Connect, topic string) *harness.Connect {
		p.Will = &harness.Will{Topic: topic}
		return p
	}
	// as logs p in as the password file's user, with its password;
	// retained makes p's Will retained.
	as := func(p *harness.Connect, user string) *harness.Connect {
		i := slices.IndexFunc(logins, func(l login) bool { return l.user == user })
		return signIn(p, user, logins[i].password)
	}
	retained := func(p *harness.Connect) *harness.Connect {
		p.Will.Retain = true
		return p
	}
	// willQoS gives p's Will, a valid one if p has none, QoS qos.
	willQoS := func(p *harness.Connect, qos byte) *harness.Connect {
		if p.Will == nil {
			p.Will = &harness.Will{Topic: "w"}
		}
		p.Will.QoS = qos
		return p
	}
	long := strings.Repeat("i", maxClientIDLength)
	cases := []struct {
		name     string
		p        *harness.Connect
		size     int
		known    session
		refusals []byte
		tooBig   bool
		present  bool
	}{
		{"a client id at max_client_id_length", v5(long, true), 40, noSession, nil, false, false},
		{"a client id over it", v5(long+"i", true), 40, noSession, []byte{0x85}, false, false},
		{"over it, MQTT 3.1.1", v311(long+"i", true), 40, noSession, []byte{2}, false, false},
		{"a control character", v5("a\x7fb", true), 20, noSession, []byte{0x85}, false, false},
		{"empty with Clean Start 0", v5("", false), 20, noSession, []byte{0x85}, false, false},
		{"empty with Clean Start 1", v5("", true), 20, noSession, nil, false, false},
		{"a Will topic at both limits", will(v5("c", true), "abcdef/abcdef/abcdef/abc"), 60, noSession, nil, false, false},
		{"a Will topic over max_topic_length", will(v5("c", true), strings.Repeat("a", maxTopicLength+1)), 60, noSession, []byte{0x90}, false, false},
		{"a Will topic over max_topic_levels", will(v5("c", true), "a/b/c/d/e"), 60, noSession, []byte{0x90}, false, false},
		{"a Will topic under $share/, which no rule allows", will(v311("c", true), "$share/g/t"), 60, noSession, []byte{5, 5}, false, false},
		{"a Will topic beginning with $, which no rule allows", will(v5("c", true), "$x/t"), 60, noSession, []byte{0x87}, false, false},
		{"will denied", as(will(v5("c", true), "w"), "cy"), 60, noSession, []byte{0x87}, false, false},
		{"will denied, MQTT 3.1.1", as(will(v311("c", true), "w"), "cy"), 60, noSession, []byte{5}, false, false},
		{"will denied and an invalid Will topic", as(will(v5("c", true), "w/#"), "cy"), 60, noSession, []byte{0x87, 0x90}, false, false},
		{"a retained Will on $broadcast, retained denied", as(retained(will(v5("c", true), "w")), "di"), 60, noSession, []byte{0x9A}, false, false},
		{"a retained Will on a channel's topic, retained denied", as(retained(will(v5("c", true), "ca/w")), "di"), 60, noSession, []byte{0x9A}, false, false},
		{"a retained Will, retained allowed", retained(will(v5("c", true), "w")), 60, noSession, nil, false, false},
		{"a Will topic holding #", will(v5("c", true), "a/#"), 60, noSession, []byte{0x90}, false, false},
		{"a QoS 2 Will", willQoS(v5("c", true), 2), 60, noSession, nil, false, false},
		{"a QoS 2 Will, qos2 denied", as(willQoS(v5("c", true), 2), "di"), 60, noSession, []byte{0x9B}, false, false},
		{"a QoS 2 Will, qos2 denied, MQTT 3.1.1", as(willQoS(v311("c", true), 2), "di"), 60, noSession, []byte{5}, false, false},
		{"a QoS 1 Will", willQoS(v5("c", true), 1), 60, noSession, nil, false, false},
		{"a QoS 2 Will on an invalid topic, qos2 denied", as(willQoS(will(v5("c", true), "+"), 2), "di"), 60, noSession, []byte{0x90, 0x9B}, false, false},
		{"an unknown saguin- property", v5("c", true, harness.User("saguin-x", "")), 30, noSession, []byte{0x83}, false, false},
		{"an Authentication Method", v5("c", true, harness.Property{ID: harness.AuthenticationMethod, Str: "m"}), 30, noSession, []byte{0x8C}, false, false},
		{"another user's password", password(v5("c", true), logins[1].password), 30, noSession, []byte{0x86}, false, false},
		{"a wrong password, MQTT 3.1.1", password(v311("c", false), logins[0].password+"x"), 30, present, []byte{4}, false, false},
		{"no user name or password", anonymous(v5("c", true)), 20, noSession, []byte{0x86}, false, false},
		{"a wrong password and a client id over its limit", password(v5(long+"i", true), ""), 40, noSession, []byte{0x85, 0x86}, false, false},
		{"two refusals", will(v5(long+"i", true), "+"), 60, noSession, []byte{0x85, 0x90}, false, false},
		{"MQTT 3.1", &harness.Connect{ProtocolName: "MQIsdp", Version: 3, ClientID: "c"}, 20, noSession, []byte{1}, false, false},
		{"an unknown level", &harness.Connect{ProtocolName: "MQTT", Version: 6, ClientID: "c"}, 20, noSession, []byte{1}, false, false},
		{"at max_connect_size", will(v5(long+"i", true), "+"), maxConnectSize, noSession, []byte{0x85, 0x90}, false, false},
		{"over max_connect_size, refused for it alone", will(v5(long+"i", true), "+"), maxConnectSize + 1, noSession, nil, true, false},
		{"keeping an existing session", v311("c", false), 20, present, nil, false, true},
		{"starting clean on one", v5("c", true), 20, present, nil, false, false},
		{"keeping none", v5("c", false), 20, noSession, nil, false, false},
	}
	for _, c := range cases {
		got := predict(c.p, c.size, c.known)
		if !bytes.Equal(got.refusals, c.refusals) || got.tooBig != c.tooBig || got.present != c.present {
			t.Errorf("%s: refusals % X, too big %v, Session Present %v; want % X, %v, %v",
				c.name, got.refusals, got.tooBig, got.present, c.refusals, c.tooBig, c.present)
		}
	}
	// design.md, CONNECT: "A client whose persistent is denied is accepted
	// with Session Expiry 0"; 8: the CONNACK carries "the Maximum QoS,
	// Retain Available and Shared Subscription Available the acl_file
	// allows".
	for _, c := range []struct {
		name                       string
		p                          *harness.Connect
		expiry                     uint32
		durable, noRetain, noShare bool
	}{
		{"persistent denied", as(v5("c", false, harness.Property{ID: harness.SessionExpiryInterval, Int: expiryFloor}), "cy"), 0, false, false, false},
		{"persistent denied, MQTT 3.1.1 Clean Session 0", as(v311("c", false), "cy"), 0, false, false, false},
		{"persistent allowed, MQTT 3.1.1 Clean Session 0", v311("c", false), 0, true, false, false},
		{"retained and share denied", as(v5("c", false, harness.Property{ID: harness.SessionExpiryInterval, Int: expiryFloor}), "di"), expiryFloor, true, true, true},
	} {
		got := predict(c.p, 40, noSession)
		if got.expiry != c.expiry || got.durable != c.durable || got.noRetain != c.noRetain || got.noShare != c.noShare {
			t.Errorf("%s: Session Expiry %d, durable %v, Retain Available 0 %v, Shared Subscription Available 0 %v; want %d, %v, %v, %v",
				c.name, got.expiry, got.durable, got.noRetain, got.noShare, c.expiry, c.durable, c.noRetain, c.noShare)
		}
	}
}
