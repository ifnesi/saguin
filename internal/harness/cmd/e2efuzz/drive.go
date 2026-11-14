package main

import (
	"bufio"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"math"
	"math/rand/v2"
	"net/netip"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// healthEvery is how many steps pass between two health checks.
const healthEvery = 200

// stopWriteTimeout is explicit in options so the stop check knows its
// cutoff without observing the core's clock or reading broker state.
const stopWriteTimeout = 5 * time.Second

// fuzzer is the one driver: it takes the clients' actions one at a time,
// in the order its seeded generator draws them, and checks each answer
// before the next.
type fuzzer struct {
	trace    *log.Logger  // optional raw TCP evidence; never an input to the model
	wireStep atomic.Int64 // current step for socket readers' wire evidence
	dials    int
	// wsAddr and wssAddr are the broker's ws listener and its ws listener
	// with tls, "" for none: with them, a connection in turn goes to each
	// (dial), the wss one with secure, webSockets counting them, overWSS
	// those over TLS and split those whose packets cross frames (framing).
	wsAddr, wssAddr            string
	webSockets, overWSS, split int
	// secureAddr is the broker's TLS listener and secure the TLS
	// configuration of a client of it, nil for none: then every
	// connection goes to addr. With one, a connection the run opens in
	// turn goes to it (dial), secured counting them.
	secureAddr string
	secure     *tls.Config
	secured    int
	// unixPath is the broker's unix listener with proxy_protocol, "" for
	// none: with one, a connection in turn goes to it (dial), its PROXY
	// header first, proxied counting them.
	unixPath       string
	proxied        int
	deliveryCutoff int                 // publications through this checkpoint were checked or exempted
	serial         uint64              // orders publications and subscription changes
	publications   map[string]*message // compact publication metadata, by payload tag
	retained       int                 // RETAIN publications the broker may store: the most retained values there are
	addr           string
	rng            *rand.Rand
	timeout        time.Duration // the longest wait for one answer
	slots          []*slot
	// held are connections that never complete a CONNECT, until their
	// client closes them.
	held []*harness.Client
	// rows indexes the harness.BadPackets rows the broker reads, and
	// rowNeedsZero marks those that are wrong only on a connection whose
	// session expiry is 0.
	rows         []int
	rowNeedsZero []bool
	step         int
	recent       []string  // the last steps, oldest first
	log          io.Writer // where each step is written as taken; nil for none
	healths      int
	// messages are the messages sent that the database may hold and the
	// record checks have not yet found, by payload, which is the run's
	// own for each (fill).
	messages map[string]*message
	dbPath   string          // the broker's database, whose records are checked; "" for none
	db       *sql.DB         // dbPath, opened at the first check
	read     map[int64]int64 // the last offset checked, by channel id
	// stored counts the records over 4 KiB read, compressed or not.
	stored largeStored
	// limits are the channels with a max_bytes, by name (channels.go), and
	// retention those with a retention_bytes (retention.go).
	limits    map[string]*limited
	retention map[string]*retentionLog
	// onChannels counts the records found on configured channels, and
	// forRoom the PUBLISHes refused with 0x97 for a channel's max_bytes,
	// for the run's last line.
	onChannels int
	// channelRecords counts the messages sent to configured channels
	// (mostDeliveries).
	channelRecords int
	// appendStats counts what the run did with append channels' delivery,
	// for its last lines (appends.go).
	appendStats appendStats
	// seekStats counts the run's seeks and $saguin/ filters (seeks.go),
	// requestStats its other requests (requests.go).
	seekStats    seekStats
	requestStats requestStats
	forRoom      atomic.Int64
	// hidden counts the MQTT 3.1.1 connections accepted with an empty
	// client id, and hiddenIDs holds the identifier the broker assigned
	// each, by its count, as its first record found named it (records).
	hidden    int
	hiddenIDs map[int]string
	// ended: every step is taken, and what fails now is one of the run's
	// last checks, step naming the step after the last.
	ended bool
	// wills are the Wills the run's CONNECTs carried, by tag (wills.go).
	wills     map[string]*will
	willStats willStats
	// checkAt is when the current checkpoint's delivery checks began.
	checkAt time.Time
	// began is when the current step began, and lowerAt the bytes the
	// database held committed then for each channel with a max_bytes,
	// read only while a slot holds a Will there (beginStep).
	began   time.Time
	lowerAt map[string]int64
	// stopped: the run stopped the broker, which exited 0, so its Wills
	// can be checked against the database (checkWillsAfterStop).
	stopped bool
	// willAt is the latest observation boundary for Will decisions. It is
	// frozen when the broker's stop is observed, so later file inspections
	// cannot extend the time in which its checks could have happened.
	willAt time.Time
	// shareRng draws the run's shared subscriptions, apart from rng so
	// the other draws stay as they were, and shares is what the run has
	// seen of the groups (shares.go).
	shareRng *rand.Rand
	shares   shareLog
	// latest is what the run has sent to the latest channels (latest.go).
	latest latestLog
	// queues is what the run has seen of the queue (queues.go), and scrape
	// reads the /metrics of a broker the run started, nil for none.
	queues queueLog
	scrape func() (testkit.Exposition, error)
	// writerFull is how many of the broker's writer_full cuts the run has
	// found, each the end of a connection a takeover came to (takenOver).
	writerFull int
	// ops calls the operations listeners of a broker the run started, nil
	// for a broker given with -addr (operations.go).
	ops *operations
	// reloads reloads a broker the run started, nil for a broker given
	// with -addr (reload.go).
	reloads *reloads
	// bridge is the peer of the bridge of a broker the run started, nil
	// for a broker given with -addr (bridges.go).
	bridge *bridging
	// paces: the broker reads the run's acl_file, so pacedUser's
	// connections are held to its publish limits (pacing.go); false for a
	// broker given with -addr. heldBack counts the PUBLISHes the model
	// knows the reader could not read when they were written, for the
	// run's last lines.
	paces    bool
	heldBack atomic.Int64
}

// slot is one client: its client id, what the model knows of the id's
// session, and the client's open connection.
type slot struct {
	readPacket func() (harness.Packet, error) // optional packet source; nil uses conn directly
	sent       []*message                     // QoS 0 publications since its last confirmed input barrier
	f          *fuzzer
	subs       *subscriptions // the connected session
	saved      *subscriptions // the named session, also while an anonymous client uses this slot
	incoming   incoming       // connection-local delivery observations
	id         string
	// window is conn's in-flight window (appends.go, inFlightUnread).
	window int
	// waiting is the delayed Will of id's session, its connection gone,
	// until a CONNECT for id resolves it (wills.go).
	waiting *will
	session session
	conn    *harness.Client // nil when it has none
	will    *will           // the Will conn's CONNECT stored, nil for none
	anon    bool            // conn connected with an empty client id: the session it holds is not id's
	// user is the user conn logged in as, whose rights the acl_file gives
	// (grantsOf); bound is the slot's: restricted for every CONNECT, so
	// that no session is held under two users' rights.
	user  string
	bound bool
	// clientID is the client id conn's session holds, which its records
	// name as their publisher (storage.md, records): id, or for an empty
	// client id in MQTT 5 the CONNACK's Assigned Client Identifier; ""
	// in MQTT 3.1.1, whose client never sees the one assigned, hidden then
	// counting the connection (fuzzer.hidden).
	clientID string
	hidden   int
	durable  bool   // conn's session outlives conn
	expiry   uint32 // conn's MQTT 5 Session Expiry Interval, which a DISCONNECT may change
	quiet    bool   // conn's CONNECT set Request Problem Information to 0
	// pending are conn's QoS 1 PUBLISHes whose PUBACK is still to come, and
	// its QoS 2 ones whose PUBREC is, by packet identifier.
	pending map[uint16]*out
	// held are the incoming QoS 2 exchanges of id's session, by packet
	// identifier, kept while the session lasts (carry); unknown are the
	// identifiers of PUBRELs conn sent that no exchange holds, their PUBCOMP
	// still to come.
	held    map[uint16]*exchange
	unknown map[uint16]bool
	// aliases are conn's Topic Alias mappings (MQTT 5 3.3.2.3.4), which
	// last only as long as conn [MQTT-3.3.2-7].
	aliases map[uint16]string
	// asked is the seek conn awaits answers to, nil for none (seeks.go).
	asked *asked
	// observed: conn's client has seen the broker close it (closed).
	observed bool
	// pace is conn's publish allowance, where the model holds it to
	// pacedUser's limits (pacing.go).
	pace pacing
}

// recentSteps is how many steps a failure reports.
const recentSteps = 32

func newFuzzer(addr, dbPath string, seed uint64, clients int, timeout time.Duration, steps io.Writer) *fuzzer {
	f := &fuzzer{addr: addr, dbPath: dbPath, rng: rand.New(rand.NewPCG(seed, seed)), shareRng: rand.New(rand.NewPCG(seed, ^seed)), timeout: timeout, log: steps,
		publications: map[string]*message{}, messages: map[string]*message{}, hiddenIDs: map[int]string{},
		read: map[int64]int64{}, limits: map[string]*limited{}, retention: map[string]*retentionLog{}}
	for _, c := range channels {
		if c.maxBytes != 0 {
			f.limits[c.name] = &limited{max: int64(c.maxBytes)}
		}
		if c.retentionBytes != 0 && c.queue == nil {
			// A queue's retention moves its unfinished jobs (queues.go).
			f.retention[c.name] = &retentionLog{max: int64(c.retentionBytes)}
		}
	}
	for i := range clients {
		// An earlier run against the same broker may have left a session
		// for the id: unsettled until its first accepted CONNECT.
		f.slots = append(f.slots, &slot{f: f, id: fmt.Sprintf("fuzz-%d", i), session: unsettled, bound: i%4 == 3})
	}
	for i := range harness.BadPackets {
		row := &harness.BadPackets[i]
		if row.Stage != harness.First && row.Stage != harness.AfterConnack {
			continue
		}
		f.rows = append(f.rows, i)
		f.rowNeedsZero = append(f.rowNeedsZero, raisesExpiry(row))
	}
	return f
}

// raisesExpiry reports whether row is a DISCONNECT carrying a non-zero
// Session Expiry Interval, which is a protocol error only where the
// CONNECT's was 0 (design.md, DISCONNECT: "The client may change its session
// expiry, except from 0, which is a protocol error"; MQTT 5 3.14.2.2.2). On
// any other connection it is a valid DISCONNECT, so such a row is sent only
// where the expiry is 0.
func raisesExpiry(row *harness.BadPacket) bool {
	p, err := harness.Decode(row.Packet, row.Version)
	if err != nil {
		return false
	}
	d, ok := p.(*harness.Disconnect)
	if !ok {
		return false
	}
	sei, ok := d.Props.Get(harness.SessionExpiryInterval)
	return ok && sei.Int > 0
}

// note records the step about to be taken.
func (f *fuzzer) note(format string, args ...any) {
	line := fmt.Sprintf("step %d: ", f.step) + fmt.Sprintf(format, args...)
	if len(f.recent) == recentSteps {
		f.recent = f.recent[1:]
	}
	f.recent = append(f.recent, line)
	if f.log != nil {
		harness.Say(f.log, "%s\n", line)
	}
}

// errWatchdog means the run's deadline passed before its requested steps
// were done; it says nothing about a broker timeout.
var errWatchdog = errors.New("watchdog expired")

// drive takes steps until steps are done (0: no count), with a health
// check and a check of the records stored since the last every healthEvery
// steps, and both at the end, then leaves the bridge's exchanges unfinished
// for the stop (unfinishedCrossings). The deadline ends an uncounted run
// successfully; with a count it is a watchdog whose expiry fails the run.
func (f *fuzzer) drive(deadline time.Time, steps int) error {
	for f.step = 1; steps == 0 || f.step <= steps; f.step++ {
		if !time.Now().Before(deadline) {
			if steps > 0 {
				return errWatchdog
			}
			break
		}
		if err := f.beginStep(); err != nil {
			return err
		}
		var err error
		if f.step%healthEvery == 0 {
			err = f.checked()
		} else {
			err = f.act()
		}
		if err != nil {
			return err
		}
	}
	f.ended = true
	if err := f.outlastHolds(); err != nil {
		return err
	}
	if err := f.beginStep(); err != nil {
		return err
	}
	if err := f.checked(); err != nil {
		return err
	}
	if err := f.readDeadLetters(); err != nil {
		return err
	}
	return f.unfinishedCrossings()
}

// checked checks health, the operations listeners and histories, committed
// records and delivery obligations.
func (f *fuzzer) checked() error {
	if err := f.health(); err != nil {
		return err
	}
	if f.bridge != nil {
		if err := f.bridge.check(time.Now()); err != nil {
			return err
		}
		if err := f.bridge.settle(f.timeout); err != nil {
			return err
		}
	}
	if err := f.operate(); err != nil {
		return err
	}
	if err := f.checkHistories(); err != nil {
		return err
	}
	if err := f.checkRecords(); err != nil {
		return err
	}
	return f.checkDeliveries()
}

// action is one thing a client may do, with its weight in the draw. It
// returns false when the state allows it nothing to act on, and another is
// drawn.
type action struct {
	weight int
	do     func(*fuzzer) (bool, error)
}

// actions are what a client may do.
var actions = []action{
	{24, (*fuzzer).connect},
	{16, (*fuzzer).ping},
	{10, (*fuzzer).disconnect},
	{6, (*fuzzer).drop},
	{4, (*fuzzer).witness},
	{8, (*fuzzer).takeover},
	{8, (*fuzzer).badPacket},
	{3, (*fuzzer).silent},
	{3, (*fuzzer).halfConnect},
	{5, (*fuzzer).release},
	{16, (*fuzzer).publish},
	{24, (*fuzzer).subscribe},
	{8, (*fuzzer).unsubscribe},
	{12, (*fuzzer).receive},
	{6, (*fuzzer).acknowledge},
	{8, (*fuzzer).window},
	{4, (*fuzzer).flood},
	{2, (*fuzzer).overfill},
	{8, (*fuzzer).exactlyOnce},
	{3, (*fuzzer).crowd},
	{1, (*fuzzer).lateReconnect},
	{3, (*fuzzer).wrongPassword},
	{6, (*fuzzer).seek},
	{4, (*fuzzer).kvGet},
	{2, (*fuzzer).catalogue},
	{2, (*fuzzer).hangUpOther},
	{20, (*fuzzer).work},
	{4, (*fuzzer).respond},
	{6, (*fuzzer).enqueue},
}

// operatorActions are what the operator of a broker the run started may
// do, drawn beside actions in such a run alone, so a run against -addr
// draws as it always has.
var operatorActions = []action{
	{1, (*fuzzer).reload},
	{1, (*fuzzer).dropBridge},
	{1, (*fuzzer).refuseBridge},
	{4, (*fuzzer).forwardBridge},
	{4, (*fuzzer).crossBridge},
}

// act draws actions until one can be taken, and takes it.
func (f *fuzzer) act() error {
	drawn := actions
	if f.reloads != nil {
		drawn = slices.Concat(actions, operatorActions)
	}
	total := 0
	for _, a := range drawn {
		total += a.weight
	}
	for {
		n := f.rng.IntN(total)
		for _, a := range drawn {
			if n -= a.weight; n < 0 {
				if done, err := a.do(f); done || err != nil {
					return err
				}
				break
			}
		}
	}
}

// pick draws one of the slots ok accepts, or nil when there is none.
func (f *fuzzer) pick(ok func(*slot) bool) *slot {
	var from []*slot
	for _, s := range f.slots {
		if ok(s) {
			from = append(from, s)
		}
	}
	if len(from) == 0 {
		return nil
	}
	return from[f.rng.IntN(len(from))]
}

// The ways dial reaches the broker, each connection taking the next in
// turn among those the broker has.
const (
	viaTCP = iota
	viaTLS
	viaUnix
	viaWS
	viaWSS
)

// ways are the ways the broker can be reached, in the order dial takes
// them.
func (f *fuzzer) ways() []int {
	ways := []int{viaTCP}
	if f.secure != nil {
		ways = append(ways, viaTLS)
	}
	if f.unixPath != "" {
		ways = append(ways, viaUnix)
	}
	if f.wsAddr != "" {
		ways = append(ways, viaWS, viaWSS)
	}
	return ways
}

// dial opens a connection whose answers are read in version, each in turn
// to addr; over TLS to secureAddr where the broker has a TLS listener, the
// handshake done before anything else is sent; to unixPath where it has a
// unix listener with proxy_protocol, a PROXY header sent first
// (proxyHeader); and over a WebSocket to wsAddr, and over one with TLS to
// wssAddr, where it has them, each framed as framing says. The choice
// draws nothing from the generator, so one seed gives one sequence either
// way.
func (f *fuzzer) dial(version byte) (*harness.Client, error) {
	ways := f.ways()
	way := ways[f.dials%len(ways)]
	addr := f.addr
	var c *harness.Client
	var err error
	switch way {
	case viaTLS:
		addr = f.secureAddr
		c, err = harness.DialTLS(nil, addr, version, f.secure)
	case viaUnix:
		addr = f.unixPath
		c, err = harness.DialUnix(addr, version)
	case viaWS, viaWSS:
		d := f.framing()
		addr = f.wsAddr
		if way == viaWSS {
			addr, d.TLS = f.wssAddr, f.secure
		}
		c, err = harness.DialWebSocket(addr, version, d)
	default:
		c, err = harness.Dial(addr, version)
	}
	if err != nil {
		return nil, fmt.Errorf("dialling %s: %w", addr, err)
	}
	switch way {
	case viaTLS:
		f.secured++
	case viaWSS:
		f.overWSS++
	}
	c.Timeout = f.timeout
	f.dials++
	observed := &observedConn{stream: c.Conn.(stream), f: f, number: f.dials}
	observed.reader = bufio.NewReader(socketInput{conn: observed})
	c.Conn = observed
	if f.trace != nil {
		f.trace.Printf("step %d conn %d open %s %s version=%d", f.step, f.dials, observed.LocalAddr(), observed.RemoteAddr(), version)
	}
	if way == viaUnix {
		if err := f.proxyHeader(c); err != nil {
			_ = c.Close() // the connection never carried MQTT
			return nil, fmt.Errorf("sending %s a PROXY header: %w", addr, err)
		}
	}
	return c, nil
}

// framing is how the next WebSocket connection frames its writes, drawing
// nothing from the generator: in turn, each write one frame; each write
// one message in frames of 1 to 7 bytes; each write cut into messages of
// 1 to 11 bytes. A write holding several packets, as a CONNECT and its
// PINGREQ are sent (attempt), packs them into one frame or message; the
// others split packets across frames or messages (MQTT-6.0.0-2: "A single
// WebSocket data frame can contain multiple or partial MQTT Control
// Packets").
func (f *fuzzer) framing() *harness.WebSocketDial {
	n := f.webSockets
	f.webSockets++
	d := &harness.WebSocketDial{Subprotocols: []string{"mqtt"}}
	switch n % 3 {
	case 1:
		d.FrameSize = 1 + n%7
	case 2:
		d.MessageSize = 1 + n%11
	}
	if n%3 != 0 {
		f.split++
	}
	return d
}

// proxyHeader sends c's PROXY v2 header, as a proxy relaying a client's
// connection does before anything else (design.md, Flows, Transport
// setup): the PROXY command, an IPv4 or an IPv6 client in turn, and no
// record, so the client's password decides. Every other pair of headers is
// split across two writes, at a byte that moves with the dial count, so the
// broker reads a header in pieces as well as whole. The header draws
// nothing from the generator.
func (f *fuzzer) proxyHeader(c *harness.Client) error {
	f.proxied++
	n := f.proxied
	client := netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(n)}), uint16(40000+n%20000))
	proxy, family := netip.MustParseAddrPort("192.0.2.1:1883"), byte(harness.ProxyTCP4)
	if n%2 == 0 {
		client = netip.AddrPortFrom(netip.AddrFrom16([16]byte{0x20, 0x01, 0x0d, 0xb8, 15: byte(n)}), uint16(40000+n%20000))
		proxy, family = netip.MustParseAddrPort("[2001:db8::1]:1883"), harness.ProxyTCP6
	}
	header := harness.ProxyHeader(harness.ProxyProxy, family, client, proxy)
	if n%4 < 2 {
		return c.SendRaw(header)
	}
	cut := 1 + f.dials%(len(header)-1)
	if err := c.SendRaw(header[:cut]); err != nil {
		return err
	}
	return c.SendRaw(header[cut:])
}

// end records the end of s's connection, by a DISCONNECT the client sent
// (clean) or otherwise. design.md, DISCONNECT: the session of a durable
// connection "goes away until its expiry", so it still exists; a clean
// DISCONNECT's change "is stored before the connection closes", so a
// session ending with its connection is gone once the client sees the
// close; otherwise its end is unsettled (session).
func (s *slot) end(clean bool) {
	// Every end but hangUp's says what ended the connection to its Will
	// first; hangUp decides it by its DISCONNECT.
	s.fire("its connection ended")
	if s.subs != nil {
		// The session is away from an end its client saw the broker take
		// (closed); from any other, the model does not know when.
		s.subs.departedStep, s.subs.departedAt = 0, 0
		if s.observed {
			s.subs.departedStep, s.subs.departedAt = s.f.step, s.f.serial
		}
	}
	s.observed = false
	s.leaveAttempts()
	anonymous := s.anon
	if !s.anon {
		// design.md, Restart: a durable session keeps its "held incoming
		// QoS 2 exchanges"; storage.md, held_publishes: "A non-durable
		// session's rows are deleted when it ends".
		s.carry()
		if !s.durable {
			s.held = nil
		}
		switch {
		case s.durable:
			s.session = present
		case clean:
			s.session = noSession
		default:
			s.session = unsettled
		}
	}
	s.conn, s.anon, s.clientID, s.hidden, s.pending, s.aliases = nil, false, "", 0, nil, nil
	s.incoming = incoming{}
	s.sent = nil
	switch {
	case !s.durable:
		s.f.shares.ended(s.subs)
		s.subs = nil
		if !anonymous {
			s.saved = nil
		}
	case anonymous:
		// The slot's next CONNECT replaces s.subs, while the session, away,
		// stays a member of its groups.
		s.f.shares.wentAway(s.subs)
	}
}

// word is a run of lower-case letters, lo to hi long.
func (f *fuzzer) word(lo, hi int) string {
	b := make([]byte, lo+f.rng.IntN(hi-lo+1))
	for i := range b {
		b[i] = byte('a' + f.rng.IntN(26))
	}
	return string(b)
}

// topic is a topic of n levels, each one to four letters.
func (f *fuzzer) topic(n int) string {
	levels := make([]string, n)
	for i := range levels {
		levels[i] = f.word(1, 4)
	}
	return strings.Join(levels, "/")
}

// willTopic draws a Will topic: mostly valid, two at the limits, and each
// way design.md, CONNECT's 0x90 names of being invalid. A valid one begins
// with tag (wills.go).
func (f *fuzzer) willTopic(tag string) string {
	switch f.rng.IntN(18) {
	case 0:
		return ""
	case 1:
		return f.topic(1+f.rng.IntN(2)) + "/+"
	case 2:
		return f.word(1, 3) + "+" + f.word(1, 3)
	case 3:
		return f.topic(1+f.rng.IntN(2)) + "/#"
	case 4:
		return f.word(maxTopicLength+1, maxTopicLength+8)
	case 5:
		return strings.Repeat("a/", maxTopicLevels+f.rng.IntN(3)) + "a"
	case 6:
		return "$share/" + f.topic(2)
	case 7:
		return "$saguin/" + f.topic(1)
	case 8:
		n := maxTopicLength - len(tag) - 1
		return tag + "/" + f.word(n, n)
	case 9:
		return tag + strings.Repeat("/a", maxTopicLevels-1)
	case 10, 11, 12:
		// On the channels' levels (channels.go): ca/w1/b lands in cab,
		// ca/s/w1 in small, ca/z/w1 and cb/z/w1 in cz, the rest of ca in
		// ca, and cb's and cc's others in $broadcast.
		first := []string{"ca", "cb", "cc"}[f.rng.IntN(3)]
		if f.rng.IntN(3) == 0 {
			return first + "/" + tag + "/b"
		}
		return first + "/" + []string{"z", "s"}[f.rng.IntN(2)] + "/" + tag
	}
	if n := f.rng.IntN(maxTopicLevels); n > 0 {
		return tag + "/" + f.topic(n)
	}
	return tag
}

// attempt is one CONNECT, its bytes and the answer the model predicts.
type attempt struct {
	connect *harness.Connect
	wire    []byte // the CONNECT then a PINGREQ, in one write
	*prediction
}

// pingreq is a PINGREQ's bytes. It goes in the CONNECT's write: accepted,
// it is answered after the CONNACK (MQTT-3.2.0-1); refused, never ("If the
// Server rejects the CONNECT, it MUST NOT process any data sent by the
// Client after the CONNECT packet", MQTT-3.1.4-6, 3.1.1 MQTT-3.1.4-5).
var pingreq = []byte{0xC0, 0x00}

// seal encodes p and predicts its answer, known being what the model knows
// of the session of p's client id.
func seal(p *harness.Connect, known session) (*attempt, error) {
	wire, err := harness.Encode(p, p.Version)
	if err != nil {
		return nil, fmt.Errorf("encoding %s: %w", describe(p), err)
	}
	return &attempt{connect: p, wire: append(wire, pingreq...), prediction: predict(p, len(wire), known)}, nil
}

// newAttempt draws a CONNECT for s; a takeover keeps s's client id. It
// logs in as one of the password file's users, with a password not that
// user's where wrong, and is then always an MQTT 3.1.1 or 5 CONNECT.
func (f *fuzzer) newAttempt(s *slot, takeover, wrong bool) (*attempt, error) {
	r := f.rng
	if !wrong && r.IntN(25) == 0 {
		// design.md, CONNECT's table: "an unknown protocol level with the
		// Protocol Name MQTT, or MQTT 3.1 (MQIsdp, level 3)".
		p := &harness.Connect{ProtocolName: "MQIsdp", Version: 3, ClientID: s.id, CleanStart: true}
		if r.IntN(2) == 0 {
			level := byte(r.IntN(254)) // any level but 4 and 5
			if level >= harness.V311 {
				level += 2
			}
			p.ProtocolName, p.Version = "MQTT", level
		}
		return seal(p, s.session)
	}
	p := &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: s.id,
		CleanStart: r.IntN(2) == 0 || s.session == unsettled}
	// A session that held a $share filter resumes over MQTT 5 alone, whose
	// Subscription Identifiers tell its shared deliveries (shares.go), and
	// so does one that held a worker subscription: whether such a session,
	// resumed over MQTT 3.1.1, which "cannot be a worker", is still offered
	// jobs the design leaves open (queues.go).
	if r.IntN(2) == 0 && (p.CleanStart || (!s.saved.shared() && !s.saved.worker())) {
		p.Version = harness.V311
	}
	l := s.login()
	password := l.password
	if wrong {
		password = f.badPassword(l)
	}
	signIn(p, l.user, password)
	if !takeover {
		switch r.IntN(20) {
		case 0:
			p.ClientID = ""
		case 1:
			p.ClientID = s.id + strings.Repeat("x", maxClientIDLength+1-len(s.id)+r.IntN(8))
		case 2:
			p.ClientID = s.id[:1] + string("\x01\x1f\x7f"[r.IntN(3)]) + s.id[1:]
		}
	}
	if r.IntN(3) > 0 {
		p.KeepAlive = uint16(keepAliveFloor + r.IntN(math.MaxUint16-keepAliveFloor+1))
	}
	v5 := p.Version == harness.V5
	if v5 {
		p.Props = f.connectProps()
	}
	if r.IntN(3) == 0 {
		w := &harness.Will{QoS: byte(r.IntN(3)), Retain: r.IntN(2) == 0, Topic: f.willTopic(willTag(f.step)), Payload: []byte(f.word(0, 8))}
		if v5 {
			w.Props = f.willProps()
		}
		p.Will = w
	}
	if v5 {
		// A client's Maximum Packet Size, last: one too small for any
		// accepting CONNACK only where the CONNECT is otherwise accepted,
		// since a refusing CONNACK's size is not the design's to fix, and
		// not with a Will for an empty client id, whose publisher, the
		// identifier the CONNACK would assign, the client never learns.
		a, err := seal(p, s.session)
		if err != nil {
			return nil, err
		}
		switch n := r.IntN(10); {
		case n == 0 && len(a.refusals) == 0 && (p.Will == nil || p.ClientID != ""):
			p.Props = append(p.Props, harness.Property{ID: harness.MaximumPacketSize, Int: uint32(1 + r.IntN(smallestConnack-1))})
		case n < 4:
			p.Props = append(p.Props, harness.Property{ID: harness.MaximumPacketSize, Int: uint32(largestConnack + r.IntN(128-largestConnack))})
		}
	}
	return seal(p, s.session)
}

// login draws the user a CONNECT for s logs in as: restricted where s is
// bound to it, else any other. A session is then only ever held under one
// user's rights, so none of its subscriptions is held for the rules no
// longer allowing it (design.md, Access and reload), which the run does not
// model: the conformance tests check it.
func (s *slot) login() login {
	i := slices.IndexFunc(logins, func(l login) bool { return l.user == restricted })
	if s.bound {
		return logins[i]
	}
	j := s.f.rng.IntN(len(logins) - 1)
	if j >= i {
		j++
	}
	return logins[j]
}

// connectProps draws an MQTT 5 CONNECT's properties: a Session Expiry
// Interval absent, 0, within max_session_expiry or over it; the client's
// own limits; User Properties, some under the reserved saguin- prefix; and
// now and then an Authentication Method.
func (f *fuzzer) connectProps() harness.Properties {
	r := f.rng
	var ps harness.Properties
	switch r.IntN(5) {
	case 1:
		ps = append(ps, harness.Property{ID: harness.SessionExpiryInterval})
	case 2, 3:
		ps = append(ps, harness.Property{ID: harness.SessionExpiryInterval, Int: uint32(expiryFloor + r.IntN(maxSessionExpiry-expiryFloor+1))})
	case 4:
		ps = append(ps, harness.Property{ID: harness.SessionExpiryInterval, Int: maxSessionExpiry + 1 + r.Uint32N(maxUint32-maxSessionExpiry)})
	}
	if r.IntN(3) == 0 {
		ps = append(ps, harness.Property{ID: harness.ReceiveMaximum, Int: 1 + r.Uint32N(math.MaxUint16)})
	}
	if r.IntN(3) == 0 {
		ps = append(ps, harness.Property{ID: harness.TopicAliasMaximum, Int: r.Uint32N(math.MaxUint16 + 1)})
	}
	if r.IntN(3) == 0 {
		ps = append(ps, harness.Property{ID: harness.RequestProblemInformation, Int: r.Uint32N(2)})
	}
	for range r.IntN(3) {
		name := f.word(1, 4)
		if r.IntN(12) == 0 {
			name = "saguin-" + name
		}
		ps = append(ps, harness.User(name, f.word(0, 4)))
	}
	if r.IntN(30) == 0 {
		ps = append(ps, harness.Property{ID: harness.AuthenticationMethod, Str: f.word(1, 4)})
	}
	return ps
}

// badPassword draws a password that is not l's: l's with letters added,
// l's cut short, or another user's (design.md, CONNECT's table: 0x86, "a
// wrong or missing credential").
func (f *fuzzer) badPassword(l login) string {
	if former, ok := f.reloads.former(l.user); ok && f.rng.IntN(4) == 0 {
		// The password a reload replaced: "An MQTT client's password is
		// checked against the new password file from its next CONNECT"
		// (design.md, Access and reload).
		return former
	}
	switch f.rng.IntN(3) {
	case 0:
		return l.password + f.word(1, 2)
	case 1:
		return l.password[:len(l.password)-1]
	}
	i := slices.Index(logins, l)
	return logins[(i+1+f.rng.IntN(len(logins)-1))%len(logins)].password
}

// wrongPassword: a CONNECT for a client's id, connected, away or with no
// session, carries a password that is not its user's: refused with 0x86,
// MQTT 3.1.1 return code 4 (design.md, CONNECT's table: "a wrong or missing
// credential"). The check of step 2 comes before the takeover of step 5 and
// the decision of step 6, and a refused CONNECT changes nothing (step 7), so
// the id's session is as it was: a connection holding it stays and answers,
// its Will held (try); an away session keeps its Session Present, its
// subscriptions and its waiting Will for the id's next CONNECT, which the
// model predicts from what it knew before.
func (f *fuzzer) wrongPassword() (bool, error) {
	// A connection of an empty client id holds a session that is not the
	// slot's id's: a CONNECT for the id would take nothing over.
	s := f.pick(func(s *slot) bool { return s.conn == nil || !s.anon })
	if s == nil {
		return false, nil
	}
	a, err := f.newAttempt(s, s.conn != nil, true)
	if err != nil {
		return true, err
	}
	f.note("%s: wrong password, %s: want %s, the id's session as it was", s.id, describe(a.connect), a.prediction)
	return true, f.try(s, a, s.conn)
}

// connect: a client without a connection connects.
func (f *fuzzer) connect() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn == nil })
	if s == nil {
		return false, nil
	}
	a, err := f.newAttempt(s, false, false)
	if err != nil {
		return true, err
	}
	f.note("%s: %s: want %s", s.id, describe(a.connect), a.prediction)
	return true, f.try(s, a, nil)
}

// takeover: a second connection for a connected client's id (design.md,
// CONNECT, step 5).
func (f *fuzzer) takeover() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && !s.anon })
	if s == nil {
		return false, nil
	}
	return true, f.takeOver(s)
}

// takeOver: a second connection for s's client id.
func (f *fuzzer) takeOver(s *slot) error {
	a, err := f.newAttempt(s, true, false)
	if err != nil {
		return err
	}
	f.note("%s: takeover, %s: want %s", s.id, describe(a.connect), a.prediction)
	return f.try(s, a, s.conn)
}

// try sends a's CONNECT for s on a new connection, checks the answer, and
// records what it changed; old is the connection it would take over, nil
// for none.
func (f *fuzzer) try(s *slot, a *attempt, old *harness.Client) error {
	w := f.newWill(s, a)
	c, err := f.dial(a.version)
	if err != nil {
		return err
	}
	if !a.tooBig {
		// The broker refuses one over max_connect_size before it reads
		// the Maximum Packet Size (design.md, CONNECT's 0x95 row).
		c.Advertise(a.connect)
	}
	if err := c.Conn.(*observedConn).serve(); err != nil {
		_ = c.Close() // the connection never carried MQTT
		return err
	}
	sent := time.Now()
	if err := c.SendRaw(a.wire); err != nil {
		_ = c.Close() // the step has failed; the close adds nothing
		return fmt.Errorf("sending the CONNECT: %w", err)
	}
	if a.tooBig || len(a.refusals) > 0 {
		w.refuse(a.String())
		err := refused(c, a.prediction)
		_ = c.Close() // the broker has closed its end, as checked
		if err == nil && old != nil {
			// The checks of step 2 come before the takeover of step 5, so
			// a refused CONNECT leaves the old connection as it was.
			if err := pingPast(s); err != nil {
				return fmt.Errorf("the connection a refused CONNECT left: %w", err)
			}
		}
		return err
	}
	window := maxInflight
	if rm, ok := a.connect.Props.Get(harness.ReceiveMaximum); ok && a.version == harness.V5 {
		window = min(window, int(rm.Int))
	}
	fresh := &slot{f: f, id: s.id, conn: c, clientID: s.id, incoming: incoming{since: f.step}, window: window, user: a.connect.Username}
	if a.present {
		fresh.subs = s.saved
	}
	if fresh.subs == nil {
		fresh.subs = newSubscriptions()
	}
	// The resends follow the CONNACK (CONNECT, 9), with the deliveries of a
	// position made again after them: their second copies are let pass
	// before any is read.
	fresh.resumed()
	// Recorded before the resends after its CONNACK are read (CONNECT, 9).
	// A connection whose CONNACK is too large for it holds the session
	// too, and the broker may hand it a first transmission it never writes.
	fresh.subs.holders = append(fresh.subs.holders, holder{at: f.serial, version: a.version})
	var ack *harness.Connack
	if a.unsendable {
		err = c.AwaitClose()
	} else {
		ack, err = acceptingConnack(c, a.prediction)
		fresh.incoming.established = time.Now()
	}
	if err != nil {
		_ = c.Close() // the step has failed; the close adds nothing
		return err
	}
	// Both answers confirm CONNECT's transaction. A mere socket close on
	// any other path supplies no such evidence (design.md, CONNECT, 7–8).
	settled := ack != nil || a.unsendable
	answered := time.Now()
	if old != nil {
		// The old connection is cut before its Will is published (design.md,
		// CONNECT, 5), so the Will, still held, is no delivery of its own.
		err = f.takenOver(s)
		_ = old.Close() // checked closed by the broker, or the step failed
		s.leaveAttempts()
		if err != nil {
			return fmt.Errorf("the connection taken over: %w", err)
		}
		// The takeover comes before the CONNACK (CONNECT, 5 and 8): the
		// Will it publishes is known before the deliveries that follow the
		// CONNACK are judged, the new connection's own among them.
		f.takeOverWill(s, !a.connect.CleanStart, settled)
	}
	// The core took the CONNECT before its CONNACK, or before the close of
	// one too large to send (CONNECT, 5, 6 and 8): the waiting Will it
	// settles is known, from the time its answer came, before the
	// deliveries that follow are judged, the new connection's own among
	// them (wills.go).
	if !a.anon {
		f.reconnectWill(s, a.connect.CleanStart, settled, sent, answered)
	}
	if !a.anon && !a.present {
		// The CONNECT ended the session it replaced, if any (design.md,
		// CONNECT, 6: "Clean start ends an existing session").
		f.shares.ended(s.saved)
	}
	if !a.unsendable {
		err = afterConnack(fresh)
		if err != nil {
			_ = c.Close() // the step has failed; the close adds nothing
			return err
		}
	}
	if !a.anon {
		// A takeover ends a connected session of expiry 0 (design.md,
		// CONNECT), so only a durable one can be kept while connected.
		s.session = noSession
		if a.durable {
			s.session = present
		}
	}
	rpi, ok := a.connect.Props.Get(harness.RequestProblemInformation)
	s.conn, s.anon, s.durable, s.expiry, s.quiet = c, a.anon, a.durable, a.expiry, ok && rpi.Int == 0
	s.user, s.pace = a.connect.Username, pacing{}
	s.window = fresh.window
	// Stored with the CONNECT, before its CONNACK (CONNECT, 7 and 8).
	s.will = w
	if w != nil {
		w.state = willHeld
	}
	if old != nil {
		// A takeover's old connection was the id's own (takeover).
		s.carry()
	}
	if !a.anon && !a.present {
		// A new session for the id: the one it replaces held its
		// exchanges, which end with it.
		s.held = nil
	}
	s.pending, s.aliases, s.sent, s.unknown, s.observed = nil, nil, nil, nil, false
	if old != nil {
		// A takeover leaves the session no time away.
		fresh.subs.departedStep, fresh.subs.departedAt = 0, 0
	}
	s.subs, s.incoming = fresh.subs, fresh.incoming
	if !a.anon {
		s.saved = s.subs
	}
	switch {
	case a.unsendable:
		// "Where a Packet is too large to send, the Server MUST discard it
		// without sending it" (MQTT-3.1.2-25), and the connection closes,
		// as checked: the CONNECT took effect, steps 5 to 7, its Will
		// stored, which the close publishes (newAttempt draws no Will for
		// an empty client id here).
		_ = c.Close()
		s.clientID = s.id
		s.fire("its CONNACK over the client's Maximum Packet Size, the broker closed the connection")
		s.end(false)
	case !a.anon:
		s.clientID = s.id
		if a.present {
			return f.resumeHeld(s)
		}
	case a.version == harness.V5:
		// "the Server MUST return the Assigned ClientID in the CONNACK"
		// (MQTT-3.1.3-7), which checkConnack has checked it does.
		s.clientID = assignedID(ack)
	default:
		// MQTT 3.1.1: "the Server MUST assign a unique ClientId to that
		// Client" (MQTT-3.1.3-6), and sends it nowhere.
		f.hidden++
		s.hidden = f.hidden
	}
	return nil
}

// refused checks a CONNECT's refusal: a CONNACK with one of its reasons and
// Session Present 0 ("If a Server sends a CONNACK packet containing a
// non-zero Reason Code it MUST set Session Present to 0", MQTT-3.2.2-6,
// 3.1.1 MQTT-3.2.2-4), or none where the design gives none, then the close
// and nothing else (design.md, CONNECT, step 2: "A failure gets a CONNACK
// with its reason, then the connection closes").
func refused(c *harness.Client, a *prediction) error {
	reasons := a.refusals
	if a.tooBig {
		reasons = nil
		if a.version == harness.V5 {
			reasons = []byte{0x95}
		}
	}
	if len(reasons) > 0 {
		ack, err := next[*harness.Connack](&slot{conn: c})
		if err != nil {
			return err
		}
		if !slices.Contains(reasons, ack.Reason) || ack.SessionPresent {
			return fmt.Errorf("CONNACK 0x%02X, Session Present %v; want one of % X, Session Present false", ack.Reason, ack.SessionPresent, reasons)
		}
	}
	return c.AwaitClose()
}

// accepted checks an accepting CONNACK: reason 0x00, Session Present as
// predicted, and in MQTT 5 its properties; then the PINGREQ's PINGRESP.
func accepted(s *slot, a *prediction) error {
	if _, err := acceptingConnack(s.conn, a); err != nil {
		return err
	}
	return afterConnack(s)
}

// acceptingConnack reads c's CONNACK and checks it accepts: reason 0x00,
// Session Present as predicted, and in MQTT 5 its properties.
func acceptingConnack(c *harness.Client, a *prediction) (*harness.Connack, error) {
	// CONNACK is first; only subsequent answers may have deliveries between.
	ack, err := next[*harness.Connack](&slot{conn: c})
	if err != nil {
		return nil, err
	}
	if ack.Reason != 0 || ack.SessionPresent != a.present {
		return nil, fmt.Errorf("CONNACK 0x%02X, Session Present %v; want 0x00, Session Present %v", ack.Reason, ack.SessionPresent, a.present)
	}
	if a.version == harness.V5 {
		if err := checkConnack(ack, a); err != nil {
			return nil, err
		}
	}
	return ack, nil
}

// afterConnack reads, past the deliveries between, the PINGRESP to the
// PINGREQ sent after the CONNECT, then whatever else s's connection holds.
func afterConnack(s *slot) error {
	if _, err := next[*harness.Pingresp](s); err != nil {
		return err
	}
	return s.drain()
}

// assignedID is ack's Assigned Client Identifier, "" where it has none.
func assignedID(ack *harness.Connack) string {
	p, _ := ack.Props.Get(harness.AssignedClientIdentifier)
	return p.Str
}

// next reads s's next control packet past deliveries and pending PUBACKs,
// and it must be a T: a type whose Type needs no value, as harness.Expect
// asks.
func next[T harness.Packet](s *slot) (T, error) {
	var want T
	p, err := answer(s)
	if err != nil {
		return want, fmt.Errorf("waiting for %s: %w", harness.Name(want.Type()), err)
	}
	got, ok := p.(T)
	if !ok {
		return want, fmt.Errorf("%s %+v, want %s", harness.Name(p.Type()), p, harness.Name(want.Type()))
	}
	switch p.(type) {
	case *harness.Suback, *harness.Unsuback, *harness.Pingresp:
		s.acknowledgeIntake(0, time.Now().UnixMilli())
	}
	return got, nil
}

// closedWith checks a close the broker makes: in MQTT 5, a DISCONNECT with
// reason first, then the end of stream; in MQTT 3.1.1 the end of stream
// alone (design.md, DISCONNECT, "Reasons Sagüin sends": "These reasons are
// MQTT 5 only. An MQTT 3.1.1 connection just closes"). The PUBACKs of pending
// may come first: a PUBLISH the broker took before it began the close is
// answered once stored, unless the connection has ended (R4). The stop checks
// below also require the replies established by its preceding ping, where the
// observed close rules out the stop cutoff.
func closedWith(s *slot, reason byte) error {
	c := s.conn
	if c.Version == harness.V5 {
		d, err := next[*harness.Disconnect](s)
		if err != nil {
			return fmt.Errorf("want DISCONNECT 0x%02X then the close: %w", reason, unannounced{err})
		}
		if d.Reason != reason {
			return fmt.Errorf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, reason)
		}
		if err := c.AwaitClose(); err != nil {
			return err
		}
		s.closed()
		return nil
	}
	return awaitClose(s)
}

// unannounced is how closedWith wraps what ended an MQTT 5 connection
// before its DISCONNECT: no reason came first.
type unannounced struct{ error }

func (u unannounced) Unwrap() error { return u.error }

// disconnectsTotal counts each closure by the reason Sagüin selected
// (metrics.md, Connections and sessions).
const disconnectsTotal = "saguin_disconnects_total"

// takenOver checks the end of s's connection, which a takeover of its id
// closes (design.md, CONNECT, 5: "An old connection for the id gets
// DISCONNECT 0x8E on MQTT 5 and is closed"), as closedWith does. A
// connection the broker had already cut as writer_full owes neither:
// "A write that has not finished limits.write_timeout after its first
// socket-write attempt closes the connection as writer_full", and such a
// connection "just closes" with no time left, whatever its bytes in
// flight (design.md, DISCONNECT, Reasons Sagüin sends; Closing a
// connection). That is so only where what ended the stream is the stream's
// end, an orderly one, cut mid-packet or reset, with no DISCONNECT before
// it, and the broker counts one more writer_full closure
// (saguin_disconnects_total{reason="writer_full"}, metrics.md) than the run
// has found; each counted cut accounts for one such end. A connection the
// takeover found live gets its DISCONNECT 0x8E then the orderly end.
func (f *fuzzer) takenOver(s *slot) error {
	err := closedWith(s, 0x8E)
	if err == nil || f.scrape == nil || !streamEnd(err) || (s.conn.Version == harness.V5 && !errors.As(err, new(unannounced))) {
		return err
	}
	key := testkit.SeriesKey(disconnectsTotal, "reason", "writer_full")
	// The writer reports its close to the core, which counts it, after
	// the socket is closed (Closing a connection, 4): the count may follow
	// the end the client read.
	for deadline := time.Now().Add(f.timeout); ; {
		e, serr := f.scrape()
		if serr != nil {
			return errors.Join(err, serr)
		}
		if n, ok := e.Values[key]; !ok {
			return errors.Join(err, fmt.Errorf("/metrics has no %s (metrics.md, Rules: \"Every series exists from start\")", key))
		} else if n > float64(f.writerFull) {
			f.writerFull++
			f.note("%s: its connection ended without DISCONNECT 0x8E, cut as writer_full before the takeover: %s %v", s.id, key, n)
			s.closed()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w; %s counts no writer_full cut the run has not found (%d found)", err, key, f.writerFull)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// streamEnd reports whether err is the end of a connection's stream: an
// orderly one, one cut mid-packet, or a reset.
func streamEnd(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

// closed records that s's client has seen the broker close its connection,
// which the broker does after taking every packet it read before
// (design.md, Flows, Closing a connection: the reader reports its end to
// the core, which "ignores packets from a connection it has already closed
// or taken over", before the writer reports the close, and the writer
// closes only after): each message whose intake the close bounds
// (closeBounded) entered its channel before now or never, its intake
// confirmed (message.confirmed: "an upper boundary on broker intake"), and
// the session, kept, is away from now on (end).
func (s *slot) closed() {
	for m := range s.closeBounded() {
		m.confirmed.CompareAndSwap(0, s.f.serial)
	}
	s.observed = true
}

// closeBounded yields the messages s's connection sent whose intake its
// close bounds: those that enter their channel on a PUBLISH the run never
// sends again, at QoS 0 and 1 (design.md, Flows, PUBLISH, the table: "on
// PUBLISH"; the next CONNECT clears a QoS 1 PUBLISH's wait, unresent).
// A QoS 2 publication "enters on PUBREL", and in every state its exchange
// can be in at the close, it may enter after it: its PUBLISH or PUBREL
// unanswered, a resumed session sends it again (resumeHeld), and the core
// may have dropped the PUBREL with the close (Closing a connection, 2: "Any
// other unsubmitted packet is dropped") or lost its move on a full disk
// ("The client's PUBREL, resent when it reconnects, completes the move");
// its PUBREL unsent, it is yet to come. Only its PUBCOMP bounds its intake
// (complete): its delivery is allowed until then, never owed.
func (s *slot) closeBounded() iter.Seq[*message] {
	return func(yield func(*message) bool) {
		for _, m := range s.sent {
			if !yield(m) {
				return
			}
		}
		for _, o := range s.pending {
			if o.m != nil && o.p.QoS < 2 && !yield(o.m) {
				return
			}
		}
	}
}

// latestUnconfirmed reports whether s's connection sent a change to a
// latest channel whose intake its close would bound (closeBounded) and the
// model has not seen confirmed.
func (s *slot) latestUnconfirmed() bool {
	for m := range s.closeBounded() {
		if m.logged && m.confirmed.Load() == 0 {
			return true
		}
	}
	return false
}

// pingPast checks s's PINGREQ is answered, then drains deliveries and PUBACKs.
func pingPast(s *slot) error {
	if err := pingReply(s); err != nil {
		return err
	}
	return s.drain()
}

// pingReply checks through the PINGRESP, an input barrier. It leaves any
// subsequent packets to the caller's reader.
func pingReply(s *slot) error {
	c := s.conn
	before := time.Now()
	if err := c.Send(&harness.Pingreq{}); err != nil {
		return fmt.Errorf("PINGREQ: %w", err)
	}
	_, err := next[*harness.Pingresp](s)
	if err == nil {
		s.confirmShares()
		s.confirmPubacks(before, time.Now())
		for _, m := range s.sent {
			m.confirmed.CompareAndSwap(0, s.f.serial)
		}
		s.sent = nil
	}
	return err
}

// ping: a connected client pings.
func (f *fuzzer) ping() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	f.note("%s: PINGREQ", s.id)
	return true, pingPast(s)
}

// disconnect: a connected client sends DISCONNECT, MQTT 5 with 0x00, 0x04
// or another reason code a client may send (MQTT 5 3.14.2.1, Table 3-10),
// and now and then a new session expiry, never one raised from 0 (a
// protocol error, which BadPackets' row sends) nor one that would expire
// within the run. design.md, Flows, DISCONNECT: "0x00 discards the Will. The
// reason 0x04 keeps it for publication at the earlier of Will Delay ending
// and the session ending. So does any other reason code, as a lost connection
// does. The client may change its session expiry, except from 0, which is a
// protocol error. The change is stored before the connection closes".
func (f *fuzzer) disconnect() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	d := &harness.Disconnect{}
	if s.conn.Version == harness.V5 {
		r := f.rng
		switch n := r.IntN(6); {
		case n >= 3 && n < 5:
			d.Reason = 0x04
		case n == 5:
			d.Reason = clientReasons[r.IntN(len(clientReasons))]
		}
		switch n := r.IntN(3); {
		case n == 0:
			d.Props = harness.Properties{{ID: harness.SessionExpiryInterval}}
		case n == 1 && s.expiry > 0:
			d.Props = harness.Properties{{ID: harness.SessionExpiryInterval, Int: expiryFloor + r.Uint32N(maxUint32-expiryFloor+1)}}
		}
		if sei, ok := d.Props.Get(harness.SessionExpiryInterval); ok {
			s.durable = sei.Int > 0
		}
	}
	f.note("%s: DISCONNECT 0x%02X %v: want the close", s.id, d.Reason, d.Props)
	return true, f.hangUp(s, d)
}

// clientReasons are the DISCONNECT reason codes but 0x00 and 0x04 that
// MQTT 5's Table 3-10 lets a client send.
var clientReasons = []byte{0x80, 0x81, 0x82, 0x83, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99}

// hangUp sends d on s's connection and checks the close that follows, past
// the PUBACKs of its pending PUBLISHes. The DISCONNECT decides the Will:
// MQTT 5 0x00 and every MQTT 3.1.1 DISCONNECT discard it, any other reason
// code publishes it (design.md, DISCONNECT).
func (f *fuzzer) hangUp(s *slot, d *harness.Disconnect) error {
	err := s.conn.Send(d)
	if err == nil {
		err = awaitClose(s)
	}
	_ = s.conn.Close() // checked closed by the broker, or the step failed
	switch {
	case s.conn.Version != harness.V5:
		s.discardWill("an MQTT 3.1.1 DISCONNECT")
	case d.Reason == 0:
		s.discardWill("DISCONNECT 0x00")
	case err == nil:
		s.fireTaken(fmt.Sprintf("DISCONNECT 0x%02X", d.Reason))
	default:
		s.fire(fmt.Sprintf("DISCONNECT 0x%02X", d.Reason))
	}
	s.end(true)
	return err
}

// lose ends s's connection without a DISCONNECT, as a lost client's ends.
// Where it holds a Will, the client ends its sending and reads to the
// broker's close, which follows the core's taking its end (design.md,
// Flows, Closing a connection) and with it the Will's publication, so the
// slot's next CONNECT reaches the core after it: one reaching it first
// would take the connection over, which the run does not do to a
// connection holding a Will (wills.go). So it does where a change it sent
// to a latest channel is not yet known taken, which the close bounds
// (closed).
func (s *slot) lose() error {
	if s.will == nil && !s.latestUnconfirmed() {
		return s.conn.Close()
	}
	err := s.conn.Conn.(*observedConn).CloseWrite()
	if err == nil {
		err = awaitClose(s)
	}
	_ = s.conn.Close() // checked closed by the broker, or the step failed
	return err
}

// drop: a connected client's connection ends without a DISCONNECT, as a
// lost client's does.
func (f *fuzzer) drop() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	f.note("%s: drops its connection", s.id)
	err := s.lose()
	if err == nil {
		s.fireTaken("its connection dropped")
	}
	s.end(false)
	return true, err
}

// badPacket: a malformed packet from harness.BadPackets, at the stage its
// row names. Before the CONNECT: design.md, CONNECT, "Any other Protocol
// Name, a malformed CONNECT [...], or one that breaks the protocol closes
// the connection without a CONNACK (MQTT-3.1.2-1)". After the CONNACK:
// "When a Server detects a Malformed Packet or Protocol Error, and a Reason
// Code is given in the specification, it MUST close the Network Connection"
// (MQTT-4.13.1-1), after a DISCONNECT with the row's reason in MQTT 5.
func (f *fuzzer) badPacket() (bool, error) {
	i := f.rng.IntN(len(f.rows))
	row := &harness.BadPackets[f.rows[i]]
	if row.Stage == harness.First {
		f.note("before a CONNECT: %s: want the close, nothing sent", row.Name)
		c, err := f.dial(row.Version)
		if err != nil {
			return true, err
		}
		if err = c.SendRaw(row.Packet); err == nil {
			err = c.AwaitClose()
		}
		_ = c.Close() // checked closed by the broker, or the step failed
		return true, err
	}
	s := f.pick(func(s *slot) bool {
		return s.conn != nil && s.conn.Version == row.Version && (!f.rowNeedsZero[i] || s.expiry == 0)
	})
	if s == nil {
		return false, nil
	}
	f.note("%s: %s: want the close (MQTT 5: DISCONNECT 0x%02X first)", s.id, row.Name, row.Reason())
	err := s.conn.SendRaw(row.Packet)
	if err == nil {
		err = closedWith(s, row.Reason())
	}
	_ = s.conn.Close() // checked closed by the broker, or the step failed
	s.fire("the broker closed the connection for " + row.Name)
	s.end(false)
	return true, err
}

// silent: a connection that sends nothing, held until its client closes
// it (release).
func (f *fuzzer) silent() (bool, error) {
	if len(f.held) == maxHeld {
		return false, nil
	}
	f.note("held connection %d: sends no CONNECT", len(f.held))
	c, err := f.dial(harness.V5)
	if err == nil {
		f.held = append(f.held, c)
	}
	return true, err
}

// halfConnect: a connection that sends part of a CONNECT the broker would
// accept, held until its client closes it.
func (f *fuzzer) halfConnect() (bool, error) {
	if len(f.held) == maxHeld {
		return false, nil
	}
	version := harness.V5
	if f.rng.IntN(2) == 0 {
		version = harness.V311
	}
	l := logins[f.rng.IntN(len(logins))]
	p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: version, ClientID: "half", CleanStart: true}, l.user, l.password)
	wire, err := harness.Encode(p, version)
	if err != nil {
		return true, err
	}
	cut := 1 + f.rng.IntN(len(wire)-1)
	f.note("held connection %d: sends %d of a CONNECT's %d bytes", len(f.held), cut, len(wire))
	c, err := f.dial(version)
	if err != nil {
		return true, err
	}
	f.held = append(f.held, c)
	return true, c.SendRaw(wire[:cut])
}

// release: a held connection's client ends it. Without a whole CONNECT
// the broker has nothing to answer: it closes in turn, sending nothing
// (MQTT-3.2.0-1: CONNACK is the server's first packet).
func (f *fuzzer) release() (bool, error) {
	if len(f.held) == 0 {
		return false, nil
	}
	i := f.rng.IntN(len(f.held))
	c := f.held[i]
	f.held = slices.Delete(f.held, i, i+1)
	f.note("held connection %d: its client closes it: want the close, nothing sent", i)
	err := c.Conn.(*observedConn).CloseWrite()
	if err == nil {
		err = c.AwaitClose()
	}
	_ = c.Close() // checked closed by the broker, or the step failed
	return true, err
}

// health: a fresh client, with an id never used before, connects, pings
// and disconnects, so a broker that has stopped serving new clients fails
// the run even when no action of the step's draw would notice.
func (f *fuzzer) health() error {
	f.healths++
	l := logins[f.healths%len(logins)]
	p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: fmt.Sprintf("health-%d", f.healths), CleanStart: true}, l.user, l.password)
	a, err := seal(p, noSession)
	if err != nil {
		return err
	}
	f.note("health check: %s: want %s, then the close after DISCONNECT", describe(p), a.prediction)
	c, err := f.dial(harness.V5)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }() // checked closed by the broker, or the step failed
	if err := c.SendRaw(a.wire); err != nil {
		return err
	}
	if err := accepted(&slot{conn: c}, a.prediction); err != nil {
		return err
	}
	if err := c.Send(&harness.Disconnect{}); err != nil {
		return err
	}
	return c.AwaitClose()
}

// stopPublications leaves accepted QoS 1 work on each open connection, up
// to its receive window. PINGRESP proves the core took every preceding
// PUBLISH before SIGTERM; it need not wait for their store answers (R4).
// PUBACKs arriving first are checked and settled by pingPast as usual.
// They are made at the step after the last, as the queue's last worker's
// job is (outlastHolds), whose index 0 theirs follow, or with the run's
// bridge at the step after the bridge's last (unfinishedCrossings), so each
// keeps a tag of its own (payloadTag).
func (f *fuzzer) stopPublications() error {
	var errs []error
	for i, s := range f.slots {
		if s.conn == nil {
			continue
		}
		batch := make([]*out, min(1+f.rng.IntN(4), receiveMaximum-len(s.pending)-len(s.held)))
		for n := range batch {
			o, err := f.newPublish(s, 1+i*receiveMaximum+n, drawQoS1)
			if err != nil {
				return err
			}
			batch[n] = o
		}
		if err := f.placeAll(batch); err != nil {
			return err
		}
		for _, o := range batch {
			f.note("%s: before SIGTERM: %s", s.id, o)
		}
		f.expect(batch...)
		err := send(s, batch...)
		if err == nil {
			err = pingPast(s)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: establishing publications before SIGTERM: %w", s.id, err))
		}
	}
	return errors.Join(errs...)
}

// stop ends the broker as an operator does and checks the graceful stop
// (design.md, Restart, Graceful stop): "every client gets 0x8B" (MQTT 5)
// and the close; a held connection, which has no CONNACK, the close alone
// ("A Server MUST NOT send a DISCONNECT until after it has sent a CONNACK
// with Reason Code of less than 0x80", MQTT-3.14.0-1); and "The exit status
// is 0 after a stop requested by SIGINT or SIGTERM" (design.md, Restart, A
// stop for a failure), within the stop's bound. A close observed before
// SIGTERM + write_timeout cannot have reached the cutoff, which starts only
// when the core accepts the stop. In that case, the preceding ping and
// successful exit require every pending PUBACK. Later observation proves
// nothing about whether the cutoff intervened; those closes retain all
// existing checks without requiring missing replies.
func (f *fuzzer) stop(b *harness.Broker) error {
	var errs []error
	prepared := f.stopPublications()
	if prepared != nil {
		errs = append(errs, prepared)
	}
	f.note("SIGTERM: want 0x8B to each MQTT 5 client, every connection closed, exit status 0 within %v, and no Will of an open connection published", b.StopBound())
	for _, s := range f.slots {
		s.release("open at SIGTERM", willDeferred, false, false)
	}
	earliestCut := time.Now().Add(stopWriteTimeout)
	if f.bridge != nil {
		f.bridge.stop()
	}
	if err := b.Signal(syscall.SIGTERM); err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, s := range f.slots {
		if s.conn != nil {
			if err := closedWith(s, 0x8B); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", s.id, err))
			} else if prepared == nil && time.Now().Before(earliestCut) && len(s.pending) != 0 {
				errs = append(errs, fmt.Errorf("%s: closed before the earliest stop cutoff with %d PUBACKs missing for PUBLISHes taken before SIGTERM", s.id, len(s.pending)))
			}
		}
	}
	for i, c := range f.held {
		if err := c.AwaitClose(); err != nil {
			errs = append(errs, fmt.Errorf("held connection %d: %w", i, err))
		}
	}
	f.closeAll()
	state, err := b.Wait(b.StopBound())
	switch {
	case err != nil:
		errs = append(errs, err)
	case !state.Success():
		errs = append(errs, fmt.Errorf("the broker ended %v after SIGTERM, want exit status 0", state))
	default:
		f.willAt = time.Now()
		f.stopped = true
	}
	if f.bridge != nil {
		errs = append(errs, f.bridge.stopped())
	}
	return errors.Join(errs...)
}

// closeAll closes every connection the run holds, publishing their Wills.
func (f *fuzzer) closeAll() {
	// An unreadable database predicts no Will dropped for certain, and the
	// run's last checks read it again.
	_ = f.beginStep()
	for _, s := range f.slots {
		if s.conn != nil {
			_ = s.conn.Close() // the run is over; nothing more is read from it
			s.fire("closed at the run's end")
			s.conn = nil
		}
	}
	for _, c := range f.held {
		_ = c.Close() // the run is over; nothing more is read from it
	}
	f.held = nil
}
