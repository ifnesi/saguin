package broker_test

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// MQTT 5 3.1.2.5 and 4.13: a server that ends a connection sends a DISCONNECT
// naming its reason before it closes it, and publishes the client's Will
// unless the client itself sent a DISCONNECT with 0x00 [MQTT-3.1.2-8]. RFC
// 0003 "Sessions" has a replaced connection's Will published where its delay
// is 0, and a stopping broker's connections keep theirs.
//
// **The case is an end decided off the client's read loop while that loop
// cannot see it**: busy with a packet, it comes back to find the end decided -
// by the DISCONNECT's writer, before it writes ("decided"), or already closed
// ("closed"); or waiting for the next packet, its read fails while the
// DISCONNECT is being written ("read error"), or it reads a packet it refuses
// ("refused read"). And a second goroutine decides
// to end it while the first writes ("two deciders"). The order is forced
// rather than waited for: the read loop is held inside a PUBLISH it sent
// until the broker has decided, and the DISCONNECT's write is held until
// every other goroutine ending the connection has either closed the socket
// or is parked waiting for the owner to, which its goroutine's stack shows.
// What the client actually received is read off its own socket.
func TestAnEndDecidedWhileTheReadLoopIsBusySendsItsDisconnectAndTheWill(t *testing.T) {
	type path struct {
		name    string
		maxPkt  uint32
		filter  string
		code    packets.Code
		will    bool // whether the Will is owed: a stopping broker keeps it instead
		setup   func(t *testing.T)
		decide  func(t *testing.T, h *brokertest.Harness, id string)
		release []string // the read loop's modes this path is driven in
	}
	paths := []path{
		{name: "a refusal: a record over the consumer's Maximum Packet Size", maxPkt: 512, filter: "events/#",
			code: packets.ErrPacketTooLarge, will: true,
			decide: func(t *testing.T, h *brokertest.Harness, _ string) {
				connect(t, h, "producer", true, false).Pub(t, "events/a/1", strings.Repeat("x", 2048))
			},
			release: []string{"decided", "closed", "read error", "refused read", "two deciders"}},
		{name: "a shared group hanging up a member that refused a delivery", maxPkt: 256,
			filter: "$share/g/news/#", code: packets.ErrPacketTooLarge, will: true,
			setup: func(t *testing.T) { t.Cleanup(broker.MeasureSharedFitWith(-1 << 20)) },
			decide: func(t *testing.T, h *brokertest.Harness, _ string) {
				connect(t, h, "producer", true, false).Pub(t, "news/x", strings.Repeat("x", 1024))
			},
			release: []string{"decided", "closed", "read error"}},
		// The takeover's DISCONNECT is written holding the client id's
		// session lock, which the taken-over connection's teardown waits for,
		// so that teardown cannot close the socket under it: its write is
		// not held, only the read loop.
		{name: "a takeover", code: packets.ErrSessionTakenOver, will: true,
			decide: func(t *testing.T, h *brokertest.Harness, id string) {
				resume(t, h, id)
			},
			release: []string{"unheld", "closed"}},
		{name: "a shutdown", code: packets.ErrServerShuttingDown, will: false,
			decide:  func(t *testing.T, h *brokertest.Harness, _ string) { go h.Stop() },
			release: []string{"decided", "read error"}},
	}
	for _, p := range paths {
		for _, mode := range p.release {
			t.Run(p.name+"/"+mode, func(t *testing.T) {
				if p.setup != nil {
					p.setup(t)
				}
				h := start(t)
				const id = "victim"
				var watcher *client
				if p.will {
					watcher = connect(t, h, "watcher", true, false)
					watcher.Sub(t, "will/#", 1)
				}

				hook := newEndHook(mode != "closed" && mode != "unheld")
				t.Cleanup(hook.free)
				if err := h.Srv.AddHook(hook, nil); err != nil {
					t.Fatal(err)
				}
				v := dialVictim(t, h, id, p.maxPkt)
				if p.filter != "" {
					v.subscribe(t, p.filter)
				}
				v.ping(t)
				// Rule 8: the broker holds the Will the victim's CONNECT carried.
				// The flag atomically, as sendLWT clears it.
				victim, ok := h.Srv.Clients.Get(id)
				if !ok || atomic.LoadUint32(&victim.Properties.Will.Flag) != 1 ||
					victim.Properties.Will.TopicName != "will/"+id {
					t.Fatalf("the victim's connection is not registered with its Will armed: found=%v", ok)
				}
				hook.victim.Store(victim)
				if mode != "read error" && mode != "refused read" {
					v.publish(t, holdTopic, "hold")
					select {
					case <-hook.readerHeld:
					case <-time.After(5 * time.Second):
						t.Fatal("the victim's read loop never read the PUBLISH that holds it")
					}
				}

				p.decide(t, h, id)
				var code byte
				select {
				case code = <-hook.decided:
				case <-time.After(5 * time.Second):
					t.Fatal("the broker never decided to end the victim's connection")
				}
				if code != p.code.Code {
					t.Fatalf("the broker decided to end it with %#x, want %#x", code, p.code.Code)
				}

				var parked atomic.Bool
				// A second goroutine decides to end the connection while
				// the first is writing its DISCONNECT: a shutdown, which must
				// wait for that DISCONNECT rather than write its own or close
				// the socket under it.
				waiter := "mqtt.(*Server).attachClient("
				if mode == "two deciders" {
					go h.Stop()
					waiter = "mqtt.(*Server).DisconnectClient("
				}
				switch mode {
				case "read error":
					// The read loop is waiting for the next packet, and the
					// client ends its side: the loop's read fails while the
					// DISCONNECT is being written.
					if err := v.conn.(*net.TCPConn).CloseWrite(); err != nil {
						t.Fatal(err)
					}
				case "refused read":
					// Or the client sends a PUBLISH at QoS 3, which the loop
					// refuses as it reads it, and would answer 0x81 were the
					// end not already another goroutine's.
					if _, err := v.conn.Write([]byte{0x36, 0}); err != nil {
						t.Fatal(err)
					}
				}
				switch mode {
				case "decided", "read error", "refused read", "two deciders":
					// The DISCONNECT's write waits until the read loop has
					// acted on the end: closed the socket, or parked to wait
					// for the close.
					go func() {
						defer hook.releaseWrite()
						buf := make([]byte, 64<<20)
						for {
							if parkedAtEnd(victim, waiter, buf) {
								parked.Store(true)
								return
							}
							select {
							case <-v.eof:
								return
							case <-hook.freed:
								return
							case <-time.After(time.Millisecond):
							}
						}
					}()
				case "closed":
					select {
					case <-v.eof:
					case <-time.After(10 * time.Second):
						t.Fatal("the deciding goroutine never closed the connection")
					}
				}
				if mode != "read error" && mode != "refused read" {
					close(hook.releaseReader)
				}

				select {
				case <-v.eof:
				case <-time.After(10 * time.Second):
					t.Fatal("the victim's connection was never closed")
				}
				got := v.received()
				t.Logf("the victim received %s, then %v", got, v.endErr)
				last := v.last()
				if last == nil || last.kind != 0xE0 || len(last.body) == 0 || last.body[0] != p.code.Code {
					t.Errorf("the connection closed without the DISCONNECT %#x before it: received %s", p.code.Code, got)
				}

				// Rule 4: the read loop found the end another goroutine had
				// claimed, and reports that end rather than its own.
				select {
				case err := <-hook.gone:
					if err == nil || errors.Is(err, mqtt.ErrClientDisconnected) ||
						!strings.Contains(err.Error(), p.code.Reason) {
						t.Errorf("the read loop ended on %v, want an end naming %q: it did not find "+
							"the end another goroutine owned", err, p.code.Reason)
					}
				case <-time.After(5 * time.Second):
					t.Error("the victim's connection was never torn down")
				}
				if mode != "closed" && mode != "unheld" && !parked.Load() {
					t.Errorf("%s never parked to wait for the close: the DISCONNECT's write was let go "+
						"by the socket closing under it", waiter)
				}

				// A stopping broker keeps the Will (RFC 0003 "Last Will").
				if !p.will || mode == "two deciders" {
					return
				}
				r, ok := watcher.Await(t, 5*time.Second)
				if !ok {
					t.Fatalf("no Will was published for a connection the broker ended with %#x [MQTT-3.1.2-8]", p.code.Code)
				}
				if r.Topic != "will/"+id || r.Payload != "i-died" {
					t.Fatalf("the watcher received %q on %q, want the victim's Will", r.Payload, r.Topic)
				}
			})
		}
	}
}

// The other side of the same rule: a client's own DISCONNECT 0x00 still ends
// its read loop with nil, which withdraws its Will [MQTT-3.14.4-3], now that
// a loop finding its connection closed by anything else does not. The
// DISCONNECT arrives while the loop is held in an earlier PUBLISH, so it is
// read after the loop has come back for the next packet.
func TestAClientsOwnDisconnectStillWithdrawsItsWill(t *testing.T) {
	h := start(t)
	const id = "victim"
	watcher := connect(t, h, "watcher", true, false)
	watcher.Sub(t, "will/#", 1)
	hook := newEndHook(false)
	t.Cleanup(hook.free)
	if err := h.Srv.AddHook(hook, nil); err != nil {
		t.Fatal(err)
	}
	v := dialVictim(t, h, id, 0)
	v.ping(t)
	victim, ok := h.Srv.Clients.Get(id)
	if !ok {
		t.Fatal("the victim's connection is not registered")
	}
	hook.victim.Store(victim)
	v.publish(t, holdTopic, "hold")
	select {
	case <-hook.readerHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("the victim's read loop never read the PUBLISH that holds it")
	}
	if _, err := v.conn.Write([]byte{0xE0, 0}); err != nil { // DISCONNECT 0x00
		t.Fatal(err)
	}
	close(hook.releaseReader)
	select {
	case err := <-hook.gone:
		if err != nil {
			t.Errorf("the read loop ended on %v after the client's own DISCONNECT, want nil: "+
				"the Will it withdrew is published", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the victim's connection was never torn down")
	}
	if got := v.received(); got != "[20 d0]" {
		t.Errorf("the broker answered the client's DISCONNECT with %s, want nothing after the PINGRESP", got)
	}
	// A Will is published before the teardown that reported above; a
	// publish made after it reaches the watcher behind any Will.
	connect(t, h, "marker", true, false).Pub(t, "will/marker", "after")
	r, ok := watcher.Await(t, 5*time.Second)
	if !ok {
		t.Fatal("the marker never arrived")
	}
	if r.Topic != "will/marker" {
		t.Fatalf("the watcher received %q on %q before the marker: a withdrawn Will was published", r.Payload, r.Topic)
	}
}

// holdTopic is the PUBLISH that holds the victim's read loop (endHook).
const holdTopic = "hold/reader"

// endHook holds the victim's read loop inside a PUBLISH it sends to
// holdTopic until released, and reports the DISCONNECT the broker decides for
// that connection as its encoding begins - after the end is decided
// (Client.beginEnd) and before a byte is written - holding the write there
// when hold is set.
type endHook struct {
	mqtt.HookBase
	hold          bool
	victim        atomic.Pointer[mqtt.Client]
	readerHeld    chan struct{}
	releaseReader chan struct{}
	decided       chan byte
	write         chan struct{}
	gone          chan error
	freed         chan struct{}
	heldOnce      sync.Once
	decideOnce    sync.Once
	writeOnce     sync.Once
	goneOnce      sync.Once
	freeOnce      sync.Once
}

func newEndHook(hold bool) *endHook {
	return &endHook{hold: hold, readerHeld: make(chan struct{}), releaseReader: make(chan struct{}),
		decided: make(chan byte, 1), write: make(chan struct{}), gone: make(chan error, 1),
		freed: make(chan struct{})}
}

func (h *endHook) ID() string { return "test-end-elsewhere" }

func (h *endHook) Provides(b byte) bool {
	return b == mqtt.OnPacketRead || b == mqtt.OnPacketEncode || b == mqtt.OnDisconnect
}

// OnDisconnect reports the error the victim's read loop ended on.
func (h *endHook) OnDisconnect(cl *mqtt.Client, err error, _ bool) {
	if cl == h.victim.Load() {
		h.goneOnce.Do(func() { h.gone <- err })
	}
}

func (h *endHook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Publish && pk.TopicName == holdTopic && cl == h.victim.Load() {
		h.heldOnce.Do(func() {
			close(h.readerHeld)
			select {
			case <-h.releaseReader:
			case <-h.freed:
			}
		})
	}
	return pk, nil
}

func (h *endHook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Disconnect && cl == h.victim.Load() {
		h.decideOnce.Do(func() {
			h.decided <- pk.ReasonCode
			if h.hold {
				select {
				case <-h.write:
				case <-h.freed:
				}
			}
		})
	}
	return pk
}

func (h *endHook) releaseWrite() { h.writeOnce.Do(func() { close(h.write) }) }

// parkedAtEnd is whether a goroutine running frame for cl - the read loop
// under attachClient, or another that decided to end it under
// DisconnectClient - is parked waiting for an end another goroutine owns to be
// closed, from every goroutine's stack, read into buf.
func parkedAtEnd(cl *mqtt.Client, frame string, buf []byte) bool {
	ptr := fmt.Sprintf("%p", cl)
	for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "[chan receive") && strings.Contains(g, frame) && strings.Contains(g, ptr) &&
			strings.Contains(g, "mqtt.(*Client).awaitEnd(") {
			return true
		}
	}
	return false
}

// free lets go of whatever the hook still holds, so a failed test does not
// hold the broker's shutdown.
func (h *endHook) free() { h.freeOnce.Do(func() { close(h.freed) }) }

// victimConn is a raw MQTT 5 connection carrying a Will: written by the test,
// and read by one goroutine of its own that records every packet the broker
// sent until the socket ends.
type victimConn struct {
	conn   net.Conn
	pkts   chan rawPacket
	eof    chan struct{}
	endErr error
	mu     sync.Mutex
	got    []rawPacket
	nextID uint16
}

type rawPacket struct {
	kind byte
	body []byte
}

// dialVictim connects id with a Clean Start, a Session Expiry Interval of an
// hour, a QoS 1 Will on will/<id> with no delay, and maxPkt as its Maximum
// Packet Size where it is not 0.
func dialVictim(t *testing.T, h *brokertest.Harness, id string, maxPkt uint32) *victimConn {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	vh := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x02 | 0x04 | 0x08, 0, 60}
	props := []byte{0x11, 0, 0, 0x0E, 0x10}
	if maxPkt > 0 {
		props = append(props, 0x27, byte(maxPkt>>24), byte(maxPkt>>16), byte(maxPkt>>8), byte(maxPkt))
	}
	vh = append(vh, byte(len(props)))
	vh = append(vh, props...)
	vh = appendString(vh, id)
	vh = append(vh, 0) // no Will properties: a delay of 0
	vh = appendString(vh, "will/"+id)
	vh = appendString(vh, "i-died")
	if _, err := conn.Write(brokertest.MqttPacket(0x10, vh)); err != nil {
		t.Fatal(err)
	}
	v := &victimConn{conn: conn, pkts: make(chan rawPacket, 64), eof: make(chan struct{}), nextID: 1}
	go v.readLoop()
	p := v.await(t, 0x20)
	if len(p.body) < 2 || p.body[1] != 0 {
		t.Fatalf("CONNACK refused: % x", p.body)
	}
	return v
}

func (v *victimConn) readLoop() {
	defer close(v.eof)
	for {
		kind, body, err := readRawPacket(v.conn)
		if err != nil {
			v.endErr = err
			return
		}
		p := rawPacket{kind: kind, body: body}
		v.mu.Lock()
		v.got = append(v.got, p)
		v.mu.Unlock()
		select {
		case v.pkts <- p:
		default:
		}
	}
}

// await returns the next packet of the given fixed-header type.
func (v *victimConn) await(t *testing.T, kind byte) rawPacket {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case p := <-v.pkts:
			if p.kind&0xF0 == kind {
				return p
			}
		case <-v.eof:
			t.Fatalf("the connection ended waiting for packet %#x: %v; received %s", kind, v.endErr, v.received())
		case <-deadline:
			t.Fatalf("no packet %#x; received %s", kind, v.received())
		}
	}
}

func (v *victimConn) subscribe(t *testing.T, filter string) {
	t.Helper()
	id := v.nextID
	v.nextID++
	body := []byte{byte(id >> 8), byte(id), 0}
	body = appendString(body, filter)
	body = append(body, 1) // QoS 1
	if _, err := v.conn.Write(brokertest.MqttPacket(0x82, body)); err != nil {
		t.Fatal(err)
	}
	p := v.await(t, 0x90)
	if len(p.body) < 4 || p.body[3] > 2 {
		t.Fatalf("SUBSCRIBE %s refused: % x", filter, p.body)
	}
}

// ping sends a PINGREQ and waits for its PINGRESP: the read loop has handled
// everything sent before it.
func (v *victimConn) ping(t *testing.T) {
	t.Helper()
	if _, err := v.conn.Write([]byte{0xC0, 0}); err != nil {
		t.Fatal(err)
	}
	v.await(t, 0xD0)
}

// publish sends a QoS 0 PUBLISH: one the broker answers with nothing, so the
// read loop goes straight back for the next packet.
func (v *victimConn) publish(t *testing.T, topic, payload string) {
	t.Helper()
	body := appendString(nil, topic)
	body = append(body, 0)
	body = append(body, payload...)
	if _, err := v.conn.Write(brokertest.MqttPacket(0x30, body)); err != nil {
		t.Fatal(err)
	}
}

func (v *victimConn) last() *rawPacket {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.got) == 0 {
		return nil
	}
	p := v.got[len(v.got)-1]
	return &p
}

func (v *victimConn) received() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	var b strings.Builder
	b.WriteString("[")
	for i, p := range v.got {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%02x", p.kind)
		if p.kind&0xF0 == 0xE0 && len(p.body) > 0 {
			fmt.Fprintf(&b, "(%02x)", p.body[0])
		}
	}
	b.WriteString("]")
	return b.String()
}

func appendString(b []byte, s string) []byte {
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

// resume sends a CONNECT for id that resumes its session, and reads nothing:
// the test reads the connection it takes over.
func resume(t *testing.T, h *brokertest.Harness, id string) {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	vh := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0, 0, 60, 5, 0x11, 0, 0, 0x0E, 0x10}
	if _, err := conn.Write(brokertest.MqttPacket(0x10, appendString(vh, id))); err != nil {
		t.Fatal(err)
	}
}

// holdDisconnecting holds the victim's OnDisconnecting - the store of what
// its clean DISCONNECT changes - until released, after saguin's own has run
// or while it runs, as far as any other goroutine can tell.
type holdDisconnecting struct {
	mqtt.HookBase
	victim  atomic.Pointer[mqtt.Client]
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *holdDisconnecting) ID() string           { return "test-hold-disconnecting" }
func (h *holdDisconnecting) Provides(b byte) bool { return b == mqtt.OnDisconnecting }
func (h *holdDisconnecting) OnDisconnecting(cl *mqtt.Client, _ packets.Packet) {
	if cl == h.victim.Load() {
		h.once.Do(func() {
			close(h.entered)
			<-h.release
		})
	}
}

// Invariant 18: the close that answers a clean DISCONNECT comes after what
// the DISCONNECT changes is stored - its Will withdrawn, its expiry - so a
// crash once the client has seen the close publishes no Will it withdrew.
// **A shutdown arriving while that store runs waits for it** rather than
// closing the connection under it: the read loop owns the end from before the
// store until the close (mqtt.Client.claimEnd). Forced: the store is held
// until the shutdown is parked waiting for the end, or has closed the socket.
func TestAShutdownDuringACleanDisconnectsStoreWaitsForIt(t *testing.T) {
	h := start(t)
	const id = "victim"
	hold := &holdDisconnecting{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-hold.release:
		default:
			close(hold.release)
		}
	})
	if err := h.Srv.AddHook(hold, nil); err != nil {
		t.Fatal(err)
	}
	v := dialVictim(t, h, id, 0)
	v.ping(t)
	victim, ok := h.Srv.Clients.Get(id)
	if !ok {
		t.Fatal("the victim's connection is not registered")
	}
	hold.victim.Store(victim)
	if rec, ok, err := brokertest.HarnessSessions.Get(id); err != nil || !ok || rec.Will == nil {
		t.Fatalf("the victim's session record holds no Will to withdraw: ok=%v err=%v", ok, err)
	}
	if _, err := v.conn.Write([]byte{0xE0, 0}); err != nil { // DISCONNECT 0x00
		t.Fatal(err)
	}
	select {
	case <-hold.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the DISCONNECT's store never began")
	}

	go h.Stop()
	buf := make([]byte, 64<<20)
	parked := false
	for !parked {
		if parkedAtEnd(victim, "mqtt.(*Server).DisconnectClient(", buf) {
			parked = true
			break
		}
		select {
		case <-v.eof:
		case <-time.After(time.Millisecond):
			continue
		}
		break
	}
	closedEarly := false
	select {
	case <-v.eof:
		closedEarly = true
	default:
	}
	close(hold.release)
	select {
	case <-v.eof:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was never closed")
	}
	t.Logf("the victim received %s, then %v", v.received(), v.endErr)
	if closedEarly || !parked {
		t.Fatalf("the shutdown closed the connection while its clean DISCONNECT was being stored "+
			"(parked=%v): received %s", parked, v.received())
	}
	if got := v.received(); got != "[20 d0]" {
		t.Errorf("the client was sent %s after its DISCONNECT, want nothing: its own DISCONNECT ended it", got)
	}
	if rec, ok, err := brokertest.HarnessSessions.Get(id); err != nil || !ok || rec.Will != nil {
		t.Errorf("the session record still holds the Will the clean DISCONNECT withdrew: ok=%v err=%v", ok, err)
	}
}

// RFC 0003 "Last Will": **a broker stopping is not a device dying**. A
// connected client's Will is neither published at a shutdown nor taken off
// its durable session, which keeps it for whichever comes first, the client
// returning or the session expiring at the broker that comes back.
func TestAShutdownNeitherPublishesNorDropsAConnectedClientsWill(t *testing.T) {
	h := start(t)
	const id = "victim"
	v := dialVictim(t, h, id, 0)
	v.ping(t)
	if rec, ok, err := brokertest.HarnessSessions.Get(id); err != nil || !ok || rec.Will == nil {
		t.Fatalf("the victim's session record holds no Will: ok=%v err=%v", ok, err)
	}
	h.Stop()
	select {
	case <-v.eof:
	case <-time.After(10 * time.Second):
		t.Fatal("the shutdown never closed the connection")
	}
	t.Logf("the victim received %s, then %v", v.received(), v.endErr)
	if n := h.B.WillsPublished("immediate"); n != 0 {
		t.Errorf("the shutdown published %d Wills", n)
	}
	rec, ok, err := brokertest.HarnessSessions.Get(id)
	if err != nil || !ok || rec.Will == nil || rec.Will.Topic != "will/"+id {
		t.Errorf("the shutdown took the Will off the durable session: ok=%v err=%v rec=%+v", ok, err, rec)
	}
}
