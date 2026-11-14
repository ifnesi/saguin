package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The tests of the reconnect experiment, 0008e's plan [261107 065639]: "a
// reconnect experiment in which durable subscribers reconnect with the
// same client id and Clean Start 0, alternating takeover and return, while
// the standard publishing runs; its report gives the CONNACK delays of
// takeovers and returns apart, from each reconnect". It measures "CONNACK
// delay of takeovers (no session read) against returns to an away
// session" (design.md, CONNECT, step 4: "Unless the id is connected, the
// core reads its session from the store").
//
// The interface the tests drive, beside -experiment reconnect:
//
//	func (s *subscriber) reconnects(addr string, r *run, closing *atomic.Bool, stop <-chan struct{}, every time.Duration) error
//
// reconnects is one subscriber's part of the experiment, s connected and
// subscribed by subscribe. It receives on each of s's connections as
// receive does. Once each reconnect's CONNACK is in, it waits every, then
// begins the next, unless stop has closed: it begins none once stop has
// closed, as publish begins no publication. Its reconnects alternate,
// first a takeover:
//
//   - a takeover dials its new connection while the old one is still open,
//     sending nothing more on the old one, which stays open until the
//     broker ends it (DISCONNECT 0x8E, session taken over: design.md,
//     CONNECT, step 5);
//   - a return sends DISCONNECT on the old connection and waits for the
//     broker to close it, as leave does, before it dials the new one, so
//     the session is away when the new CONNECT comes.
//
// Each reconnect's CONNECT has s's client id, Clean Start 0 and the
// recipe's Session Expiry, and subscribes nothing new: its session keeps
// its subscription, as comeBack does. Each one's CONNACK delay, from its
// CONNECT to its CONNACK, is recorded by kind. A connection the broker ends
// in a takeover is no failure; reconnects returns once stop has closed and
// its last connection has ended with closing set, as receive does, with an
// error only where a reconnect failed.
//
//	func reconnectSection(subs []*subscriber) section
//
// reconnectSection is the experiment's table in the results file, over
// subs: a row per kind, its first cell naming the kind (takeover or
// return), its second how many reconnects of that kind subs made, and the
// rest that kind's CONNACK delays in milliseconds, as latencyMS gives them.

// returnHold is how long the fake broker holds each return's CONNACK, so
// every return's CONNACK delay is known to be at least this.
const returnHold = 250 * time.Millisecond

// A servedLink is one connection the fake broker serves once its CONNECT
// is answered: it closes the connection on the client's DISCONNECT, as the
// broker does, and records what else arrives, then the connection's end.
type servedLink struct {
	link  *harness.PeerLink
	ended chan struct{} // closed once end is set
	mu    sync.Mutex
	// disconnected is set, before the link is closed, once the client's
	// DISCONNECT has arrived.
	disconnected bool
	// stray is what arrived other than a DISCONNECT and the end; end is
	// the end.
	stray []harness.Arrival
	end   harness.Arrival
}

// serve serves l until it ends.
func serve(l *harness.PeerLink) *servedLink {
	sl := &servedLink{link: l, ended: make(chan struct{})}
	go func() {
		defer close(sl.ended)
		for {
			a, err := l.Next(time.Minute) // a backstop: each test ends its links well before
			if err != nil {
				a = harness.Arrival{At: time.Now(), Err: err}
			}
			sl.mu.Lock()
			_, disconnect := a.Packet.(*harness.Disconnect)
			switch {
			case a.Packet == nil:
				sl.end = a
				sl.mu.Unlock()
				return
			case disconnect:
				sl.disconnected = true
			default:
				sl.stray = append(sl.stray, a)
			}
			sl.mu.Unlock()
			if disconnect {
				_ = l.Close() // the broker's close after a DISCONNECT
			}
		}
	}()
	return sl
}

// wait waits for sl's end.
func (sl *servedLink) wait(t *testing.T) {
	t.Helper()
	select {
	case <-sl.ended:
	case <-time.After(harness.DefaultTimeout):
		t.Fatalf("the connection had not ended %v after the broker closed it", harness.DefaultTimeout)
	}
}

// A reconnectSeen is what the fake broker saw of one reconnect.
type reconnectSeen struct {
	connect *harness.Connect
	// takeover is set where the connection before it was still open when
	// this CONNECT came, no DISCONNECT having arrived on it; endedEarly,
	// for a takeover, where that connection's end came before the
	// broker's DISCONNECT 0x8E was sent on it.
	takeover, endedEarly bool
	// served is this reconnect's connection, ended once reconnected
	// returns.
	served *servedLink
}

// reconnected runs subscriber 0 through n reconnects of the experiment,
// with every 0, against the test playing the broker, the run's epoch ago
// before it starts (warmUp puts every reconnect in the measured window),
// and returns the subscriber, once reconnects has returned, with what the
// broker saw of each reconnect. The broker answers each CONNECT with
// Session Present 1, first ending the old connection where it is still
// open, with DISCONNECT 0x8E, then closing it; it holds a return's CONNACK
// for returnHold. At the n-th CONNECT it sets closing and closes stop
// before answering, and once that CONNACK is sent it closes the last
// connection: the run's end.
func reconnected(t *testing.T, n int, ago time.Duration) (*subscriber, []reconnectSeen) {
	t.Helper()
	p := harness.ListenedPeer(t)
	s, first, _ := subscribed(t, p)
	r := newRun(sizes[0], 0)
	r.epoch = time.Now().Add(-ago)
	var closing atomic.Bool
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.reconnects(p.Addr(), r, &closing, stop, 0) }()
	prev := serve(first)
	seen := make([]reconnectSeen, 0, n)
	for k := 1; k <= n; k++ {
		l := nextLink(t, p, done, k)
		c, _ := harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
		saw := reconnectSeen{connect: c}
		prev.mu.Lock()
		saw.takeover = !prev.disconnected
		prev.mu.Unlock()
		if saw.takeover {
			// A send failing where the client has already closed shows as
			// that connection's end coming first.
			took, _ := prev.link.Send(&harness.Disconnect{Reason: 0x8E})
			_ = prev.link.Close()
			prev.wait(t)
			saw.endedEarly = prev.end.At.Before(took)
		} else {
			prev.wait(t) // closed at its DISCONNECT
		}
		if k == n {
			closing.Store(true)
			close(stop)
		}
		if !saw.takeover {
			time.Sleep(returnHold) // the fake broker's scripted slowness, not a wait for a condition
		}
		if _, err := l.Send(&harness.Connack{SessionPresent: true}); err != nil {
			t.Fatal(err)
		}
		prev = serve(l)
		saw.served = prev
		seen = append(seen, saw)
	}
	_ = prev.link.Close() // the run's end
	prev.wait(t)
	ended(t, done)
	return s, seen
}

// nextLink is the connection of reconnect k, failing t where none comes or
// reconnects returns first.
func nextLink(t *testing.T, p *harness.Peer, done <-chan error, k int) *harness.PeerLink {
	t.Helper()
	links, failed := make(chan *harness.PeerLink, 1), make(chan error, 1)
	go func() {
		l, err := p.Next(harness.DefaultTimeout)
		if err != nil {
			failed <- err
			return
		}
		links <- l
	}()
	select {
	case l := <-links:
		return l
	case err := <-failed:
		t.Fatalf("reconnect %d: %v", k, err)
	case err := <-done:
		t.Fatalf("reconnects returned %v before its reconnect %d, stop still open", err, k)
	}
	return nil
}

// TestReconnectKinds checks the reconnects of -experiment reconnect, on
// the wire: each has the subscriber's same client id, Clean Start 0 and
// the recipe's Session Expiry; they alternate, first a takeover, a
// takeover's old connection still open until the broker ends it, a
// return's ended by its DISCONNECT and the broker's close before the new
// CONNECT; nothing else arrives on any connection.
func TestReconnectKinds(t *testing.T) {
	if !slices.Contains(experiments, "reconnect") {
		t.Fatalf("experiments %q, want reconnect among them", experiments)
	}
	_, seen := reconnected(t, 5, warmUp)
	for i, saw := range seen {
		c := saw.connect
		expiry, ok := c.Props.Get(harness.SessionExpiryInterval)
		if c.ClientID != "load-sub-0" || c.CleanStart || !ok || expiry.Int != sessionExpiry {
			t.Errorf("reconnect %d: CONNECT for %q, Clean Start %v, Session Expiry %+v (present %v); want load-sub-0, Clean Start 0, Session Expiry %d",
				i+1, c.ClientID, c.CleanStart, expiry, ok, sessionExpiry)
		}
		kind := map[bool]string{true: "a takeover (the old connection still open)", false: "a return (the old connection ended by its DISCONNECT)"}
		switch {
		case saw.takeover != (i%2 == 0):
			t.Errorf("reconnect %d: %s; want %s, first a takeover, then alternating", i+1, kind[saw.takeover], kind[i%2 == 0])
		case saw.takeover && saw.endedEarly:
			t.Errorf("reconnect %d, a takeover: the old connection ended before the broker's DISCONNECT 0x8E; want it open until the broker ends it", i+1)
		}
		if len(saw.served.stray) > 0 {
			t.Errorf("reconnect %d: its connection carried %v; want nothing but a return's DISCONNECT", i+1, saw.served.stray)
		}
	}
}

// TestReconnectReport checks the reconnect experiment's table in the
// results file: the CONNACK delays of takeovers and of returns apart, each
// kind's count the reconnects of that kind the subscriber made in the
// measured window, as the broker saw them. Every return's CONNACK was held for returnHold, so each
// of the return row's delays is at least that: a table that mixes the two
// kinds, or swaps them, shows less.
func TestReconnectReport(t *testing.T) {
	s, seen := reconnected(t, 5, warmUp)
	var takeovers, returns int
	for _, saw := range seen {
		if saw.takeover {
			takeovers++
		} else {
			returns++
		}
	}
	sec := reconnectSection([]*subscriber{s})
	if !strings.Contains(sec.heading+strings.Join(sec.columns, " ")+strings.Join(sec.notes, " "), "CONNACK") {
		t.Errorf("the reconnect table %q, columns %q, does not say its delays are CONNACK delays", sec.heading, sec.columns)
	}
	row := func(kind string) []string {
		i := slices.IndexFunc(sec.rows, func(row []string) bool { return len(row) > 2 && strings.Contains(strings.ToLower(row[0]), kind) })
		if i < 0 {
			t.Fatalf("the reconnect table's rows %q: none names %s, with its count and delays", sec.rows, kind)
		}
		return sec.rows[i]
	}
	if got := row("takeover"); got[1] != strconv.Itoa(takeovers) {
		t.Errorf("takeovers row %q: count %s, want %d, the takeovers made", got, got[1], takeovers)
	}
	got := row("return")
	if got[1] != strconv.Itoa(returns) {
		t.Errorf("returns row %q: count %s, want %d, the returns made", got, got[1], returns)
	}
	for _, cell := range got[2:] {
		if v, err := strconv.ParseFloat(cell, 64); err != nil || v < ms(returnHold) {
			t.Errorf("returns row %q: delay %q; want each at least %v ms, every return's CONNACK held that long", got, cell, ms(returnHold))
		}
	}
}

// TestReconnectWindow checks that -receive-maximum holds across the
// reconnect experiment, 0008c's plan [261107 065639]: "subscribers with a
// given window (that Receive Maximum on CONNECT)". Every reconnect's
// CONNECT, takeover or return, carries the window the first CONNECT does,
// as the subscriber's whole window depends on it (design.md, Delivery:
// "The window is the lower of the client's Receive Maximum and
// limits.max_inflight"; CONNECT, Resume: "The new connection's Receive
// Maximum governs resumed PUBLISHes as well as new ones").
func TestReconnectWindow(t *testing.T) {
	asked(t, "-receive-maximum", "7")
	_, seen := reconnected(t, 4, warmUp)
	for i, saw := range seen {
		if rm, ok := saw.connect.Props.Get(harness.ReceiveMaximum); !ok || rm.Int != 7 {
			t.Errorf("with -receive-maximum 7, reconnect %d's CONNECT carried %+v; want Receive Maximum 7 on every reconnect", i+1, saw.connect.Props)
		}
	}
}

// TestReconnectStaggered checks that the reconnect experiment's subscribers
// do not reconnect all at once: "-experiment reconnect keeps standard
// publishing running while durable subscribers alternate takeovers and
// returns, paced by -reconnect-interval (1s when unset), initially
// staggered evenly across that interval" (the load tool's documentation).
// With an interval of every and n subscribers, subscriber i's first
// reconnect is due every + i·every/n after the run's epoch. Its CONNECT
// reaches the broker no sooner, a timer never firing early, and before
// the next subscriber's is due: each first reconnect in its own slot of
// every/n.
func TestReconnectStaggered(t *testing.T) {
	const n, every = 3, 900 * time.Millisecond
	p := harness.ListenedPeer(t)
	subs := make([]*subscriber, n)
	old := make([]*harness.PeerLink, n)
	for i := range n {
		subs[i], old[i], _ = subscribedAs(t, p, i)
	}
	r := newRun(size{name: "staggered", publishers: sizes[0].publishers, subscribers: n}, 0)
	var closing atomic.Bool
	stop := make(chan struct{})
	done := make(chan error, n)
	r.epoch = time.Now()
	for _, s := range subs {
		go func() { done <- s.reconnects(p.Addr(), r, &closing, stop, every) }()
	}
	first := make([]time.Duration, n) // each first reconnect's CONNECT, since the epoch
	links := make([]*harness.PeerLink, n)
	for range n {
		l := p.Link(t)
		c, a := harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
		i, err := strconv.Atoi(strings.TrimPrefix(c.ClientID, "load-sub-"))
		if err != nil || i < 0 || i >= n || links[i] != nil {
			t.Fatalf("a reconnect's CONNECT for %q; want one from each of load-sub-0 to load-sub-%d", c.ClientID, n-1)
		}
		links[i], first[i] = l, a.At.Sub(r.epoch)
	}
	// Each first reconnect is a takeover: the broker ends the old
	// connection, then accepts the new one, once stop has closed, so none
	// begins another.
	closing.Store(true)
	close(stop)
	for i := range n {
		if _, err := old[i].Send(&harness.Disconnect{Reason: 0x8E}); err != nil {
			t.Fatal(err)
		}
		_ = old[i].Close()
		if _, err := links[i].Send(&harness.Connack{SessionPresent: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range links {
		_ = l.Close() // the run's end
	}
	for range n {
		ended(t, done)
	}
	for i, at := range first {
		due := every + time.Duration(i)*every/n
		if at < due || at >= due+every/n {
			t.Errorf("subscriber %d of %d, reconnecting every %v: its first reconnect's CONNECT came %v after the epoch; want from %v, before %v: the first reconnects spread evenly over the interval",
				i, n, every, at, due, due+every/n)
		}
	}
}

// TestReconnectWarmUp checks the reconnect table's counts, on audit
// finding 261108-004a: like the run's other figures, it covers the
// measured window, its reconnects those whose CONNECT falls in it, and
// pools no reconnect of the warm-up with them. reconnected's four
// reconnects, two takeovers and two returns, all come within the run's
// first second, inside its warm-up: the table counts none of either kind.
func TestReconnectWarmUp(t *testing.T) {
	s, seen := reconnected(t, 4, 0)
	sec := reconnectSection([]*subscriber{s})
	for _, row := range sec.rows {
		if len(row) < 2 || row[1] != "0" {
			t.Errorf("%d reconnects, every CONNECT in the warm-up: the reconnect table's rows %q; want each kind counting 0, the measured window's alone", len(seen), sec.rows)
			break
		}
	}
}

// TestTakeoverKeepsReading checks the takeover, on audit finding
// 261108-004a, 3: while its new CONNECT awaits the CONNACK, the subscriber
// keeps reading its old connection, so that socket never backs up for want
// of a reader (the broker's writer to it would stall, and the cut a stall
// brings delay the CONNACK measured: design.md, CONNECT, step 5), and each
// delivery on it is stamped as it arrives. In the measured window, once
// the takeover's CONNECT has come, the broker sends four deliveries on the
// old connection, each stamped as sent then, holds the CONNACK for
// takeoverHold, then ends the old connection with DISCONNECT 0x8E and
// sends the CONNACK. Each delivery's latency, from its send to its
// receipt, is then below takeoverHold: one read only once the CONNACK is
// in is at least that late.
func TestTakeoverKeepsReading(t *testing.T) {
	const takeoverHold = 250 * time.Millisecond
	asked(t) // every option as the recipe has it
	p := harness.ListenedPeer(t)
	s, old, _ := subscribed(t, p)
	r := newRun(sizes[0], 0)
	r.epoch = time.Now().Add(-warmUp) // the window opens now: the deliveries are its cohort
	k := publisherAt(r, 1)
	var closing atomic.Bool
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.reconnects(p.Addr(), r, &closing, stop, warmUp) }() // the takeover due now
	l := nextLink(t, p, done, 1)
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	close(stop) // no reconnect after this one
	const n = 4
	for seq := range uint64(n) {
		payload := make([]byte, payloadSize)
		stamp{sent: time.Since(r.epoch), publisher: k, seq: seq}.put(payload)
		if _, err := old.Send(&harness.Publish{QoS: 1, ID: uint16(seq) + 1, Topic: r.topics[k], Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(takeoverHold) // the fake broker's scripted slowness, not a wait for a condition
	if _, err := old.Send(&harness.Disconnect{Reason: 0x8E}); err != nil {
		t.Fatal(err)
	}
	_ = old.Close()
	if _, err := l.Send(&harness.Connack{SessionPresent: true}); err != nil {
		t.Fatal(err)
	}
	closing.Store(true)
	_ = l.Close() // the run's end
	ended(t, done)
	if longest, _ := quantileMS(&s.sendLatency, 1); s.cohort != n || longest >= ms(takeoverHold) {
		t.Errorf("%d deliveries on the old connection while the takeover's CONNACK was held %v: %d received, the longest latency %.2f ms; want all %d, each below %v, read as they arrived",
			n, takeoverHold, s.cohort, longest, n, takeoverHold)
	}
}

// ended waits for reconnects' result on done, failing t where it is an
// error or does not come.
func ended(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reconnects: %v", err)
		}
	case <-time.After(harness.DefaultTimeout):
		t.Fatalf("reconnects had not returned %v after stop closed and its last connection ended", harness.DefaultTimeout)
	}
}

// TestReconnectDuplicates checks how the reconnect experiment counts a
// delivery received again, on audit finding 261108-004a: a QoS 1 PUBLISH
// with DUP set is the broker's resend, counted apart, only where it follows
// a reconnect of its subscriber and its publication was sent before that
// reconnect, so the broker could have held it unacknowledged (design.md,
// CONNECT, Resume: "The broker reads in-flight messages and PUBRELs from
// the store and resends them with their packet identifiers and in their
// order. Resent messages have DUP set (MQTT-4.4.0-1)"). It is one at most
// per publication and connection: once received there, the broker does
// not send it again on that connection (design.md, Delivery: "While
// connected, the broker starts no resend"); each new connection may resume
// it once again. Any other repeat, with DUP or without, is a duplicate,
// which still fails the run (load tool: "QoS 1 resends with DUP counted
// apart from unexpected duplicates"); and only a resend is excused from
// the order check. Through resentAfterTakeover's script: 2 resends, A's
// first after the takeover and A's first after the return; 4 duplicates,
// A's DUP before the takeover, A's second after it, once its resend was
// acknowledged, B's, and A's second after the return; none reordered.
func TestReconnectDuplicates(t *testing.T) {
	s := resentAfterTakeover(t)
	if s.resends != 2 || s.duplicates != 4 || s.reordered != 0 {
		t.Errorf("A, A with DUP, a takeover, A with DUP twice, B, B with DUP, a return, A with DUP twice: %d resends, %d duplicates, %d reordered; want 2 resends, A's first after the takeover and A's first after the return, 4 duplicates, A's DUP before the takeover, A's second after it, B's and A's second after the return, none reordered",
			s.resends, s.duplicates, s.reordered)
	}
}

// TestResendNoted checks the results file's note under its wire
// accounting, on audit finding 261108-004a ([261109 170708]): it says what
// TestReconnectDuplicates checks, not less. A QoS 1 delivery with DUP is
// counted apart as a resend only where it is its publication's first
// receipt on a connection after a reconnect, the publication sent before
// that reconnect; any other repeat, with DUP or without, is a duplicate
// and fails the run. A note saying only that "a repeat without DUP still
// fails" leaves out the repeat with DUP that fails too.
func TestResendNoted(t *testing.T) {
	sections := resultSections([]runResult{{sizes[0], maximum, &outcome{target: "saguin"}}})
	i := slices.IndexFunc(sections, func(s section) bool { return s.heading == "Wire accounting" })
	if i < 0 {
		t.Fatalf("the results' sections hold no wire accounting: %+v", sections)
	}
	notes := strings.Join(sections[i].notes, " ")
	for _, want := range []string{"first receipt", "after a reconnect", "sent before", "with DUP or without"} {
		if !strings.Contains(notes, want) {
			t.Errorf("the wire accounting's note %q does not say %q: a resend is a publication's first receipt on a connection after a reconnect, sent before it, and any other repeat, with DUP or without, fails", notes, want)
		}
	}
}

// TestRatesCountDistinct checks deliveries per second and delivered MB per
// second, on audit finding 261108-004a, 2: in every mode they count each
// distinct publication received in the window once, never a resend or a
// duplicate, as the per-second series does ("Rates, series and backlog all
// count the same distinct receipts"). The reconnect experiment in the
// maximum mode, without a per-second series, through resentAfterTakeover's
// script: eight deliveries in the window, of two publications; the
// subscriber counts 2 received in it, and 2 payloads' bytes.
func TestRatesCountDistinct(t *testing.T) {
	s := resentAfterTakeover(t)
	if s.arrived != 2 || s.arrivedBytes != 2*payloadSize {
		t.Errorf("eight deliveries of two publications, six of them repeats: %d deliveries, %d payload bytes counted received in the window; want 2 and %d, each publication once",
			s.arrived, s.arrivedBytes, 2*payloadSize)
	}
}

// resentAfterTakeover runs subscriber 0 of a maximum-mode run, its window
// open, through the reconnect experiment against the test playing the
// broker, and returns it once reconnects has returned. Its reconnects are
// due every second: the first, a takeover, at once, the run's epoch being
// warmUp ago; the second, a return, a second after the takeover's CONNACK,
// time enough for the four exchanges between them. On the first
// connection the broker sends a publication A, at QoS 1, then A again with
// DUP, no reconnect before it, all counted before the takeover's
// connection replaces it (drainConnection). On the takeover's connection
// the broker sends A with DUP, its resend, then A with DUP again once that
// resend is acknowledged, then a publication B, stamped as sent once those
// PUBACKs show the takeover's CONNACK in, then B again with DUP. On the
// return's connection, A was sent before its CONNACK too: the broker sends
// A with DUP, its resend there, then A with DUP again once that resend is
// acknowledged. Each PUBACK on the new connections says its delivery has
// been counted.
func resentAfterTakeover(t *testing.T) *subscriber {
	t.Helper()
	asked(t) // every option as the recipe has it: no per-second series
	p := harness.ListenedPeer(t)
	s, old, _ := subscribed(t, p)
	r := newRun(sizes[0], 0)
	r.epoch = time.Now().Add(-warmUp) // the window opens now, every delivery in it
	k := publisherAt(r, 1)
	var closing atomic.Bool
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.reconnects(p.Addr(), r, &closing, stop, time.Second) }()
	publication := func(seq uint64) []byte {
		payload := make([]byte, payloadSize)
		stamp{sent: time.Since(r.epoch), publisher: k, seq: seq}.put(payload)
		return payload
	}
	a := publication(0)
	for _, pub := range []*harness.Publish{
		{QoS: 1, ID: 1, Topic: r.topics[k], Payload: a},
		{QoS: 1, ID: 1, Dup: true, Topic: r.topics[k], Payload: a}, // no reconnect before it
	} {
		if _, err := old.Send(pub); err != nil {
			t.Fatal(err)
		}
	}
	acked := func(l *harness.PeerLink, pub *harness.Publish) {
		t.Helper()
		if _, err := l.Send(pub); err != nil {
			t.Fatal(err)
		}
		if ack, a := harness.Arrive[*harness.Ack](t, l, harness.DefaultTimeout); ack.Kind != harness.PUBACK || ack.ID != pub.ID {
			t.Fatalf("the broker received %s; want PUBACK %d", a, pub.ID)
		}
	}
	answered := func(l *harness.PeerLink) {
		t.Helper()
		if _, err := l.Send(&harness.Connack{SessionPresent: true}); err != nil {
			t.Fatal(err)
		}
	}

	l := nextLink(t, p, done, 1) // the takeover
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	if _, err := old.Send(&harness.Disconnect{Reason: 0x8E}); err != nil {
		t.Fatal(err)
	}
	_ = old.Close()
	answered(l)
	acked(l, &harness.Publish{QoS: 1, ID: 1, Dup: true, Topic: r.topics[k], Payload: a}) // the takeover's resend
	acked(l, &harness.Publish{QoS: 1, ID: 1, Dup: true, Topic: r.topics[k], Payload: a}) // no reconnect before it
	b := publication(1)
	acked(l, &harness.Publish{QoS: 1, ID: 2, Topic: r.topics[k], Payload: b})
	acked(l, &harness.Publish{QoS: 1, ID: 2, Dup: true, Topic: r.topics[k], Payload: b}) // B sent after the reconnect

	// The return leaves, and the broker closes the connection at its
	// DISCONNECT.
	harness.Arrive[*harness.Disconnect](t, l, harness.DefaultTimeout)
	_ = l.Close()
	l = nextLink(t, p, done, 2)
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	close(stop) // no reconnect after this one
	answered(l)
	acked(l, &harness.Publish{QoS: 1, ID: 1, Dup: true, Topic: r.topics[k], Payload: a}) // the return's resend
	acked(l, &harness.Publish{QoS: 1, ID: 1, Dup: true, Topic: r.topics[k], Payload: a}) // no reconnect before it
	closing.Store(true)
	_ = l.Close() // the run's end
	ended(t, done)
	return s
}

// brokerBinary is the broker make build writes, from this package's folder.
const brokerBinary = "../../../../bin/saguin"

// The relay's CONNACK holds, by reconnect kind: takeover, then return. The
// two differ, so each kind's reported delays must be its own.
var relayHolds = [2]time.Duration{40 * time.Millisecond, 160 * time.Millisecond}

// relaySlack is how much later the tool may see a CONNACK than the relay
// passes it on: its own writes, reads and scheduling on loopback.
const relaySlack = 30 * time.Millisecond

// A relay stands between the load tool and the broker, passing every packet
// on byte for byte, each end of stream as an end of stream, and a reset as
// a reset. It holds each reconnect's CONNACK for its kind's relayHolds,
// and notes what crossed: each reconnect's CONNECT, when it was read, with
// its CONNACK delay, from then to its CONNACK passed on, and the QoS 1
// resends with DUP passed to a subscriber.
type relay struct {
	ln     net.Listener
	broker string
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  []*relayed
	last   map[string]*relayed // each client id's latest connection
	// connack holds the reconnects as they crossed, by kind; resends
	// counts the resends.
	connack [2][]crossing
	resends int64
}

// A crossing is one reconnect through the relay: when its CONNECT was read,
// and its CONNACK delay as the relay measured it.
type crossing struct {
	at    time.Time
	delay time.Duration
}

// relayed is one connection through the relay.
type relayed struct {
	client, broker net.Conn
	open           atomic.Int32 // directions still passing
	// id is its CONNECT's client id; kind is 0 for a takeover, 1 for a
	// return, -1 for an id's first connection; disconnected is set once
	// the client's DISCONNECT has crossed; connectAt is when its CONNECT
	// was read.
	id           string
	kind         int
	disconnected bool
	connectAt    time.Time
}

// relayTo starts a relay to the broker at addr, closed when t ends.
func relayTo(t *testing.T, addr string) *relay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rl := &relay{ln: ln, broker: addr, last: map[string]*relayed{}}
	rl.wg.Go(rl.accept)
	t.Cleanup(rl.close)
	return rl
}

// accept relays each connection until the relay closes.
func (rl *relay) accept() {
	for {
		client, err := rl.ln.Accept()
		if err != nil {
			return
		}
		broker, err := net.Dial("tcp", rl.broker)
		if err != nil {
			_ = client.Close() // the broker is gone: so is the connection
			continue
		}
		c := &relayed{client: client, broker: broker, kind: -1}
		c.open.Store(2)
		rl.mu.Lock()
		rl.conns = append(rl.conns, c)
		rl.mu.Unlock()
		rl.wg.Go(func() { rl.direction(c, true) })
		rl.wg.Go(func() { rl.direction(c, false) })
	}
}

// direction passes c's packets the one way, up from the client or down
// from the broker, then the end: an orderly end of stream as one, anything
// else as a close of both sides. The last direction to end closes both.
func (rl *relay) direction(c *relayed, up bool) {
	from, to := c.broker, c.client
	if up {
		from, to = c.client, c.broker
	}
	err := rl.pass(c, from, to, up)
	if errors.Is(err, io.EOF) {
		_ = to.(*net.TCPConn).CloseWrite() // the end, passed on; a failure shows as the other side's end
	} else {
		_ = c.client.Close()
		_ = c.broker.Close()
	}
	if c.open.Add(-1) == 0 {
		_ = c.client.Close()
		_ = c.broker.Close()
	}
}

// pass reads each packet from from and writes its own bytes to to, until
// a read or write fails.
func (rl *relay) pass(c *relayed, from, to net.Conn, up bool) error {
	in := bufio.NewReader(from)
	for {
		var raw bytes.Buffer
		p, err := harness.ReadPacket(io.TeeReader(in, &raw), harness.V5, 0)
		if err != nil {
			return err
		}
		time.Sleep(rl.read(c, p, up)) // the CONNACK's hold, scripted, not a wait for a condition
		if _, err := to.Write(raw.Bytes()); err != nil {
			return err
		}
		rl.passed(c, p, up)
	}
}

// read notes p, just read, and returns how long to hold it.
func (rl *relay) read(c *relayed, p harness.Packet, up bool) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	switch p := p.(type) {
	case *harness.Connect:
		if prev := rl.last[p.ClientID]; prev != nil {
			c.kind = 0
			if prev.disconnected {
				c.kind = 1
			}
		}
		c.id, c.connectAt = p.ClientID, time.Now()
		rl.last[p.ClientID] = c
	case *harness.Disconnect:
		if up {
			c.disconnected = true
		}
	case *harness.Connack:
		if c.kind >= 0 {
			return relayHolds[c.kind]
		}
	}
	return 0
}

// passed notes p, just written on to its receiver.
func (rl *relay) passed(c *relayed, p harness.Packet, up bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	switch p := p.(type) {
	case *harness.Connack:
		if c.kind >= 0 {
			rl.connack[c.kind] = append(rl.connack[c.kind], crossing{c.connectAt, time.Since(c.connectAt)})
		}
	case *harness.Publish:
		if !up && p.QoS == 1 && p.Dup && strings.HasPrefix(c.id, "load-sub-") {
			rl.resends++
		}
	}
}

// close stops the relay: no more connections, each one closed, and every
// direction ended.
func (rl *relay) close() {
	_ = rl.ln.Close()
	rl.mu.Lock()
	for _, c := range rl.conns {
		_ = c.client.Close()
		_ = c.broker.Close()
	}
	rl.mu.Unlock()
	rl.wg.Wait()
}

// reportedMS is d as the report gives a latency: the upper bound, in
// milliseconds as latencyMS writes them, of the histogram bucket d falls in.
func reportedMS(d time.Duration) float64 {
	var h histogram
	h.add(d)
	v, _ := strconv.ParseFloat(latencyMS(&h, 1), 64)
	return v
}

// savedTable is the table under the saved results file's first heading
// beginning heading: its columns and rows, nil where there is none.
func savedTable(file, heading string) (columns []string, rows [][]string) {
	cells := func(line string) []string {
		return strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "| "), " |"), " | ")
	}
	in := false
	for line := range strings.Lines(file) {
		line = strings.TrimSuffix(line, "\n")
		switch {
		case strings.HasPrefix(line, "## "):
			if in {
				return columns, rows
			}
			in = strings.HasPrefix(line, "## "+heading)
		case !in || !strings.HasPrefix(line, "|"):
		case columns == nil:
			columns = cells(line)
		case !strings.HasPrefix(line, "|---"):
			rows = append(rows, cells(line))
		}
	}
	return columns, rows
}

// A firstWrite closes first at the first write through it.
type firstWrite struct {
	w     io.Writer
	once  sync.Once
	first chan struct{}
}

func (f *firstWrite) Write(p []byte) (int, error) {
	f.once.Do(func() { close(f.first) })
	return f.w.Write(p)
}

// TestReconnectRun checks short runs of -experiment reconnect against
// Sagüin itself (0008e's plan [261107 065639]: "its report gives the
// CONNACK delays of takeovers and returns apart, from each reconnect"),
// the smallest size in the fixed mode at a low rate, with -ack-delay, so
// deliveries are still unacknowledged at each reconnect and the broker
// resends them (design.md, CONNECT, Resume). Two runs go side by side, as
// each takes the recipe's whole window:
//
//   - the command itself, load, which must pass and save a results file
//     holding the reconnect table, with takeovers and returns counted, and
//     resends with DUP in its wire accounting;
//   - the same run through a relay that holds each reconnect's CONNACK by a
//     known amount, different for takeovers and returns, the command's own
//     launch leaving no way between its clients and its broker. It must
//     pass its own accounting, and its report, the sections the command
//     saves, agree with what crossed the wire: as many takeovers and
//     returns whose CONNECT came in the measured window, at least one of
//     each; as many resends with DUP passed to subscribers; and each
//     kind's CONNACK delays (p50, p90, p99 and max) in the histogram's own
//     terms, the bucket of the delay that crossed, the tool seeing each
//     CONNECT before the relay and each CONNACK after it, up to the bucket
//     of that delay plus relaySlack. The tool sends each CONNECT up to
//     relaySlack before the relay reads it, so one the relay read within
//     relaySlack after either end of the window may fall either side of
//     it: the report counts at least the reconnects surely in the window
//     and at most those and the uncertain ones, and each quantile lies
//     between the nearest-rank values its count allows among them.
func TestReconnectRun(t *testing.T) {
	asked(t) // every option set back once the test ends
	results := t.TempDir()
	args := []string{"-experiment", "reconnect", "-binary", brokerBinary, "-sizes", "small", "-modes", "fixed", "-rate", "100", "-runs", "1",
		"-ack-delay", "100ms", "-results", results}
	var stdout, stderr bytes.Buffer
	out := &firstWrite{w: &stdout, first: make(chan struct{})}
	var code int
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		code = load(args, out, &stderr)
	}()
	t.Cleanup(func() { <-ended }) // the command's run ends before the test's options are set back
	select {
	case <-out.first: // its header: the command has read its options, which the relay's run shares
	case <-ended:
		t.Fatalf("load %q: exit %d before its header; stderr:\n%s", args, code, stderr.String())
	}

	opts := brokerOptions(t.TempDir())
	opts.Under = nil // as the harness's tests launch it, under the test's own bound
	b, err := harness.Launch(brokerBinary, opts)
	if err != nil {
		t.Fatalf("starting %s: %v", brokerBinary, err)
	}
	t.Cleanup(func() { _ = b.Stop(); b.Release() }) // stopped already where the run got that far
	rl := relayTo(t, b.Addr)
	tg := &target{addr: rl.ln.Addr().String(), binary: brokerBinary, broker: b}
	run, err := tg.measure(sizes[0], rateOverride, true)
	if err = tg.finish(run, err); err != nil {
		t.Fatalf("the relayed run: %v\nthe broker's log:\n%s", err, b.Log())
	}
	rl.close() // every connection has ended: everything has crossed
	rows := reconnectSection(run.subs).rows
	opens := run.run.epoch.Add(warmUp)
	closes := opens.Add(measuredWindow)
	for kind, name := range []string{"takeover", "return"} {
		var sure, all []time.Duration // the delays of the reconnects surely in the window, and of those maybe in it too
		for _, c := range rl.connack[kind] {
			if c.at.Before(opens) || !c.at.Before(closes.Add(relaySlack)) {
				continue
			}
			all = append(all, c.delay)
			if !c.at.Before(opens.Add(relaySlack)) && c.at.Before(closes) {
				sure = append(sure, c.delay)
			}
		}
		slices.Sort(all)
		if len(sure) == 0 {
			t.Errorf("relayed run: no %s crossed the wire in the measured window; want the run to reconnect, alternating takeovers and returns", name)
			continue
		}
		i := slices.IndexFunc(rows, func(row []string) bool { return len(row) == 6 && strings.Contains(strings.ToLower(row[0]), name) })
		if i < 0 {
			t.Fatalf("relayed run: the reconnect table's rows %q: none names %s, with its count and delays", rows, name)
		}
		row := rows[i]
		count, err := strconv.Atoi(row[1])
		if err != nil || count < len(sure) || count > len(all) {
			t.Errorf("relayed run: the report's %s row %q: count %s; want from %d to %d, the %ss that crossed the wire in the measured window", name, row, row[1], len(sure), len(all), name)
			continue
		}
		// The report's count of the uncertain left out: its k-th delay
		// lies between all's k-th and all's (k+out)-th.
		out := len(all) - count
		for j, q := range []float64{0.5, 0.9, 0.99, 1} {
			k := max(int(math.Ceil(q*float64(count))), 1) - 1 // nearest rank, as the histogram reads it
			got, err := strconv.ParseFloat(row[2+j], 64)
			if lo, hi := reportedMS(all[k]), reportedMS(all[k+out]+relaySlack); err != nil || got < lo || got > hi {
				t.Errorf("relayed run: the report's %s row %q: quantile %v %q ms; want from %.2f, the bucket of the %v that crossed the wire (each held %v), to %.2f, that of %v and %v more",
					name, row, q, row[2+j], lo, all[k], relayHolds[kind], hi, all[k+out], relaySlack)
			}
		}
	}
	if rl.resends == 0 {
		t.Error("relayed run: no resend with DUP crossed the wire; want resends, deliveries unacknowledged at each reconnect")
	}
	sections := resultSections([]runResult{{sizes[0], fixed, run}})
	i := slices.IndexFunc(sections, func(s section) bool { return s.heading == "Wire accounting" })
	if i < 0 {
		t.Fatalf("relayed run: the report's sections hold no wire accounting: %+v", sections)
	}
	acct := sections[i]
	col := slices.Index(acct.columns, "MQTT resends (DUP)")
	if col < 0 || len(acct.rows) != 1 || acct.rows[0][col] != strconv.FormatInt(rl.resends, 10) {
		t.Errorf("relayed run: the report's wire accounting, columns %q, rows %q: want MQTT resends (DUP) %d, the resends that crossed the wire", acct.columns, acct.rows, rl.resends)
	}
	t.Logf("relayed run: reported %q; crossed: takeovers %d, returns %d, resends %d", rows, len(rl.connack[0]), len(rl.connack[1]), rl.resends)

	<-ended
	if code != 0 {
		t.Fatalf("load %q: exit %d; stdout:\n%s\nstderr:\n%s", args, code, stdout.String(), stderr.String())
	}
	saved, err := filepath.Glob(filepath.Join(results, "*.md"))
	if err != nil || len(saved) != 1 {
		t.Fatalf("load %q saved %q (%v); want one results file", args, saved, err)
	}
	data, err := os.ReadFile(saved[0])
	if err != nil {
		t.Fatal(err)
	}
	file := string(data)
	columns, table := savedTable(file, "Reconnect CONNACK delays")
	if len(columns) < 2 || columns[1] != "Reconnects" {
		t.Fatalf("the command's results file holds no reconnect table with its counts; columns %q:\n%s", columns, file)
	}
	for _, name := range []string{"takeover", "return"} {
		i := slices.IndexFunc(table, func(row []string) bool { return len(row) == len(columns) && row[0] == name })
		if n, err := strconv.Atoi(cellAt(table, i, 1)); err != nil || n == 0 {
			t.Errorf("the command's saved reconnect table %q: no %ss counted; want the run to reconnect, alternating takeovers and returns", table, name)
		}
	}
	t.Logf("command: saved reconnect table %q", table)
	columns, table = savedTable(file, "Wire accounting")
	if n, err := strconv.Atoi(cellAt(table, 0, slices.Index(columns, "MQTT resends (DUP)"))); err != nil || n == 0 {
		t.Errorf("the command's saved wire accounting, columns %q, rows %q: no MQTT resends (DUP); want resends, deliveries unacknowledged at each reconnect", columns, table)
	}
}

// cellAt is rows[i][j], or "" where there is none.
func cellAt(rows [][]string, i, j int) string {
	if i < 0 || i >= len(rows) || j < 0 || j >= len(rows[i]) {
		return ""
	}
	return rows[i][j]
}
