package core_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of ready work (design.md, Execution, Ready work), driven
// inside with the real store behind the core (liveScript): whether the
// core leaves work it could do waiting is something no client can see
// apart from an unrelated event that happens to rescue it (design.md,
// Testing: "A test inside the code checks only what neither can see").

// readySeed replays one seed of TestReadyWorkIsNeverLeftWaiting, printed by
// a failure; 0 runs the usual seeds.
var readySeed = flag.Uint64("ready.seed", 0, "replay one seed of TestReadyWorkIsNeverLeftWaiting")

// readyCount is how many seeds TestReadyWorkIsNeverLeftWaiting runs, from
// 1: a longer search than make check's.
var readyCount = flag.Uint64("ready.seeds", readySeeds, "how many seeds TestReadyWorkIsNeverLeftWaiting runs")

// readyFresh is how many seeds TestReadyWorkIsNeverLeftWaiting draws at
// random after its fixed ones, so that each long run also explores
// scenarios no earlier run played; make check draws none.
var readyFresh = flag.Uint64("ready.fresh", 0, "how many random seeds TestReadyWorkIsNeverLeftWaiting adds to its fixed ones")

// readyFreshDraw names the environment variable holding the draw that
// makes a run's fresh seeds.
const readyFreshDraw = "SAGUIN_READY_FRESH_DRAW"

// readySeeds, readySteps and readyQuiet size TestReadyWorkIsNeverLeftWaiting's
// runs: each seed plays readySteps random steps, and after every readyQuiet
// of them checks that the broker, once left nothing to do, is idle.
const (
	readySeeds = 24
	readySteps = 1200
	readyQuiet = 100 // steps between the checks of an idle broker (quiet)
)

// readyClients are the client ids the runs connect, each possibly several
// times at once, which takes the session over.
var readyClients = []string{"c0", "c1", "c2", "c3"}

// readyFilters are the filters the runs subscribe to: the append channel
// events, latest values, $broadcast, their shared groups and seek replies.
var readyFilters = []string{"events/#", "events/a", "b/#", "b/x", "$share/g/events/#", "$share/g/b/#", "values/#", "$share/g/values/#", eventsSeek + "/reply"}

// readyTopics are the topics the runs publish on.
var readyTopics = []string{"events/a", "events/b", "b/x", "b/y", "values/a", "values/b"}

// readySeeks are the seek payloads the runs send, malformed among them.
var readySeeks = []string{"0", "-1", "1", "3", "1h", "x"}

// readyYAML gives the runs memory pressure (an incoming budget of one body
// of max_packet_size, the least it may be, and an outgoing budget of a few
// deliveries), retention that deletes records by size and age on events
// and by size on $broadcast, a queue whose retention moves its jobs to its
// dead-letter channel by size and age and whose failed attempts are offered
// again after short, growing waits, three spent at most, and the default
// admission gate. Its write
// timeout outlasts a run, so the ghost's writer may still be in its write
// when the core takes each nudge (readyRun.nudged).
const readyYAML = `  delivery_admission_delay: 100ms
  broadcast:
    retention_bytes: 700
  qos2:
    max_inflight_per_client: 4
  limits:
    max_inflight: 3
    max_subscriptions: 8
    max_packet_size: 1KiB
    max_incoming_memory: 1KiB
    max_outgoing_memory: 2KiB
    write_timeout: 1h
channels:
  events:
    type: append
    filter: "events/#"
    retention_bytes: 700
    retention_period: 3s
  values:
    type: latest
    filter: "values/#"
    retention_period: 4s
    tombstone_retention_period: 2s
  recovery:
    type: append
    filter: "recovery/#"
  tasks:
    type: queue
    filter: "tasks/#"
    retention_bytes: 700
    retention_period: 3s
    visibility_timeout: 1s
    retry:
      mode: linear
      delay: 300ms
      max_attempts: 3
`

// design.md, Execution, Ready work: "The core never leaves work it could
// do waiting for an unrelated event"; rule 2, "After applying the event's
// results as Writes requires, the core works through scheduled work until
// none can progress, including work scheduled meanwhile [...] The event's
// actions are returned before the next outside event is taken"; Store,
// Requests: "Applying a predecessor's result schedules its dependent
// request"; Testing: "The core's decisions are a function of its state, one
// event and that event's time. They return what to do".
//
// So an event that carries nothing new, at the same time as the last one,
// can make nothing possible that was not already possible: if it leads to
// any action, that work was left waiting. Such an event is a packet from a
// connection the core has taken over: CONNECT, 5. Take over, "Anything its
// reader still delivers is ignored"; Closing a connection: "The core
// ignores packets from a connection it has already closed or taken over".
// Its reader keeps reading until its writer takes the final entry and
// stops it (Closing a connection, 1), and a writer held in a write by a
// client that does not read takes it only when that write ends, at the
// latest limits.write_timeout after it began: readyYAML sets an hour, more
// than a run lasts. The core's timer is not such an event: a fire is the
// session-expiry timer and starts a pass (DISCONNECT, "Each time its timer
// comes, the core goes once through the sessions due").
//
// Each seed plays clients connecting (Clean Start 0 and 1, durable and
// not), taking sessions over, subscribing (shared groups included) and
// unsubscribing, publishing at QoS 0, 1 and 2 (retained on $broadcast),
// seeking, acknowledging, disconnecting and losing connections, then
// reconnecting to stored exchanges, while the store answers requests in a
// random order, writers report in a random order across connections, the
// message budgets hold bodies and deliveries back, and time moves on so
// that retention deletes records. Each connection's packets come in an
// order its reader can hand them over in: one packet at a time, and once
// it has asked for a PUBLISH body, nothing before that PUBLISH but its end
// (Message memory, "A reader has at most one request or granted body";
// Closing a connection, 2: an unsubmitted packet other than a complete
// DISCONNECT is dropped at the end). A client acknowledges only what its
// writer has written, possibly before its report reaches the core.
// Reply slots and incoming quota return on writing, a request's
// acknowledgement's as any reply's (design.md, $saguin topics: "The
// request's reply slot follows the ordinary rule in Replies"). After each
// eligible decision, the ignored
// PUBACK of a taken-over connection is handed to it at that decision's
// time (liveScript.nudge): it must lead to no action at all.
// Every readyQuiet steps, the broker is left nothing to do, and must then
// be idle (quiet): work registered nowhere is found there. The store
// stopping on an error, a panic, or the core asking without end (a piece
// of work looked at again although nothing it waits for changed) fails the
// run too.
//
// Each seed also drives a durable session recovering saved exchanges
// among unmatched records and an intervening owed record, under a reduced
// delivery window. Delivery, Positions permits unsaved progress to replay;
// CONNECT, Resume requires the saved exchanges to be resent in order under
// the new Receive Maximum. Ready work, 3 requires that work leave everything
// it waits for able to finish, including through a chain of waits. Once
// every external obligation is answered, recovery must finish even when an
// acknowledged exchange cannot be deleted until the scan passes a gap.
// This is checked before advancing time or adding unrelated client work:
// recovery does not yet qualify for the admission gate (Delivery, Admission
// qualification), so quiet's open-gate check alone cannot find this wait.
// The other recovery modes retain this oracle for QoS 2 PUBLISH/PUBREL and
// for Session Expiry 0 with inherited settled markers (Delivery, Positions).
// CONNECT, Resume puts new application deliveries after recovered PUBLISHes;
// the owed record must then arrive normally, never be skipped with a marker.
// Dedicated shared, Will, full-store, restart, latest-channel, queue-move,
// queue-worker, message-memory, message-memory order and kept-position flows guarantee that each seed reaches
// those transitions, using the same random endpoint driver. No core call
// may fail to return: a watchdog fails a flow that outlasts its bound
// (readyWatchdog), naming its seed and flow.
//
// Every even seed's random flow runs the store in normal mode at
// sync_interval's least (readyStorage): Store, normal mode, "Every
// sync_interval, counted from the end of the previous sync, the core's timer
// asks the writer for a sync if anything was committed since. The request
// takes its place in the store-request order like a write". Its timer fires
// among the core's others (Event order), and the store carries out each sync
// in the write lane, in the order the core sends them, as it does a group.
//
// Every odd seed's random flow runs the store in memory mode at
// snapshot_interval's least, its memory_dir a tmpfs of the run's own
// (testkit.OnSmallDisk): Store, memory mode, "Every snapshot_interval [...]
// a snapshot copies the RAM copy's database file"; "A snapshot request takes
// its place in the store-request order like a write [...] the writer folds
// the whole log into the database file [...] then a store reader opens a
// read". Its timer fires among the core's others, the store carrying out its
// fold in the write lane and its copy in the read lane, as it does any write
// and read; once the steps are played, time passes a snapshot interval, so
// that a snapshot has run (Testing: "A run that tested nothing never
// passes").
func TestReadyWorkIsNeverLeftWaiting(t *testing.T) {
	if *readySeed == 0 && *readyCount+*readyFresh == 0 {
		t.Fatal("ready.seeds or ready.fresh must be positive")
	}
	seeds := []uint64{*readySeed}
	if *readySeed == 0 {
		seeds = seeds[:0]
		for i := range *readyCount {
			seeds = append(seeds, i+1)
		}
		if *readyFresh != 0 {
			// A seed's run on a small disk is a rerun of this test, given
			// its flags and environment (testkit.RunAlone): it inherits the
			// draw, so it makes the same fresh seeds.
			draw, err := strconv.ParseUint(os.Getenv(readyFreshDraw), 10, 64)
			if err != nil {
				draw = rand.Uint64()
				t.Setenv(readyFreshDraw, strconv.FormatUint(draw, 10))
			}
			fresh := rand.New(rand.NewPCG(draw, draw^0x5eed))
			for range *readyFresh {
				seed := fresh.Uint64()
				for seed <= *readyCount { // never 0, never a fixed seed
					seed = fresh.Uint64()
				}
				seeds = append(seeds, seed)
			}
			t.Logf("fresh seeds: %v", seeds[*readyCount:])
		}
	}
	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			for _, mode := range []string{"events", "recovery", "recovery-qos2", "recovery-zero", "shared", "wills", "will-shared", "full", "restart", "latest", "queue", "workers", "memory", "memory-order", "kept"} {
				t.Run(mode, func(t *testing.T) {
					if readyInMemory(seed, mode) && os.Getenv(readyRAM) == "" {
						testkit.OnSmallDisk(t, readyRAM, readyRAMSize, readyRAMLimit)
						return
					}
					defer readyWatchdog(seed, mode).Stop()
					if mode == "restart" {
						synctest.Test(t, func(t *testing.T) {
							// Store startup and core events use the same fake clock.
							time.Sleep(time.Until(epoch))
							newReadyRun(t, seed, mode).lifecycle()
						})
						return
					}
					r := newReadyRun(t, seed, mode)
					defer func() {
						if p := recover(); p != nil {
							r.fail("the core panicked: %v\n%s", p, debug.Stack())
						}
					}()
					switch mode {
					case "recovery", "recovery-qos2", "recovery-zero":
						r.resume()
						return
					case "shared":
						r.shared()
						return
					case "wills":
						r.wills()
						return
					case "will-shared":
						r.sharedWill()
						return
					case "full":
						r.fullStore()
						return
					case "latest":
						r.latest()
						return
					case "queue":
						r.queueMoves()
						return
					case "workers":
						r.workers()
						return
					case "memory":
						r.memory()
						return
					case "memory-order":
						r.memoryOrder()
						return
					case "kept":
						r.kept()
						return
					}
					for i := range readySteps {
						r.step()
						if (i+1)%readyQuiet == 0 {
							r.quiet()
						}
					}
					if readyInMemory(seed, mode) {
						r.until(r.now.Add(r.cfg.Broker.Storage.SnapshotInterval))
						r.quiet()
						if r.lone == 0 {
							r.fail("memory mode: no snapshot's fold was carried out, a snapshot interval after the steps")
						}
					}
				})
			}
		})
	}
}

// readyStorage is broker.storage as the loader reads it from baseYAML with
// keys, each "key: value", added: normal mode at sync_interval's least,
// 100ms, or memory mode at snapshot_interval's least, 1m (config.md), so
// that the store-sync or snapshot timer runs among the core's others
// (design.md, Execution, Event order: "commit cutoff, store sync, metric
// refresh, snapshot").
func readyStorage(t *testing.T, keys ...string) config.Storage {
	t.Helper()
	var added strings.Builder
	for _, key := range keys {
		added.WriteString("    " + key + "\n")
	}
	cfg, err := config.Parse([]byte(strings.Replace(baseYAML, "    file_path:", added.String()+"    file_path:", 1)))
	if err != nil {
		t.Fatalf("broker.storage with %q: %v", keys, err)
	}
	return cfg.Broker.Storage
}

// readyRAM names the environment variable that tells the test binary, run
// again by testkit.OnSmallDisk for a memory-mode flow (readyInMemory), the
// directory of the tmpfs it has as memory_dir, never the machine's /dev/shm.
const readyRAM = "SAGUIN_READY_RAM"

// readyRAMSize is that tmpfs's size: room for the RAM copy and a
// write-ahead log of SQLite's 1,000 pages before it folds them in.
const readyRAMSize = 16 << 20

// readyRAMLimit bounds the run of one memory-mode flow: a backstop over a
// flow that takes seconds.
const readyRAMLimit = 2 * time.Minute

// readyFlowLimit bounds one flow run in this process: a backstop over a
// flow that takes well under a second, shorter than readyRAMLimit, so that a
// memory-mode flow's own watchdog, in its run on a small disk, fires before
// that run is killed.
const readyFlowLimit = time.Minute

// readyWatchdog fails seed's flow mode, naming both, once it has run for
// readyFlowLimit: a core call that does not return never returns control to
// the test, so only another goroutine can end the run, and it does so the
// one way it can, by a panic, which prints every goroutine's stack, the
// core's among them.
func readyWatchdog(seed uint64, mode string) *time.Timer {
	return time.AfterFunc(readyFlowLimit, func() {
		debug.SetTraceback("all")
		panic(fmt.Sprintf("seed %d, flow %s: still running after %v, a core call that does not return (design.md, Execution, Ready work: \"The event's actions are returned before the next outside event is taken\"); replay: -run TestReadyWorkIsNeverLeftWaiting -ready.seed=%d",
			seed, mode, readyFlowLimit, seed))
	})
}

// readyInMemory reports whether seed's flow mode runs the store in memory
// mode: every odd seed's random flow.
func readyInMemory(seed uint64, mode string) bool {
	return mode == "events" && seed%2 == 1
}

// readyRun is one seed's run: a liveScript and what its clients know.
type readyRun struct {
	*liveScript
	rng          *rand.Rand
	seed         uint64
	mode         string           // flow, also printed by the subtest name
	wantRecovery []readySent      // ordered recovered PUBLISHes, then the normal owed delivery
	log          []string         // every step, in order, for a failure's report
	ghost        *connection.Conn // taken over: its packets are ignored
	conns        []*readyConn     // open connections, in accept order
	accepts      int
	quota        uint32                // the last incoming QoS 2 generation a reader handed over
	resends      int                   // exchanges in the recovery fixture
	tail         int64                 // recovery channel's tail, after its matching records and gaps
	answers      []store.Answer        // workers paused before reporting; each read's view precedes its result
	pauseRead    func(store.Read) bool // optionally hold a read's result, still sending its view first
	pausedRead   store.Read            // selected before execution, which may clear the read's page
	full         bool                  // writes roll back while set; no write body is retained by the driver
	stalled      bool                  // the write lane waits while set, as behind another program's write lock; reads go on
	logs         int                   // logger writes waiting to return their credit, in line order
	ghosts       []*connection.Conn    // both connections used to establish the ignored-packet probe
	lone         int                   // writes carried out alone: normal mode's syncs, memory mode's snapshot folds
	reads        func(store.Read)      // if set, told of every read the store carries out, before it does
}

// readyConn is what a client knows of one of its connections.
type readyConn struct {
	conn      *connection.Conn
	name      string                     // its client id and accept number, for the log
	connect   bool                       // its CONNECT has been handed over
	connack   bool                       // a successful CONNACK came
	mustEnd   bool                       // a refusing CONNACK or a final entry came: its reader ends next
	broken    bool                       // the socket failed or was cut; no further packet can be written
	final     bool                       // its final entry came
	ended     bool                       // its Ended has been handed over
	seen      int                        // its replies examined so far
	asked     int64                      // the body its reader asked for and has not handed over
	kept      int64                      // what its reader keeps of that body once it has compressed it
	body      any                        // the packet that body carries
	quota     uint32                     // that packet's incoming QoS 2 generation
	ids       map[uint16]byte            // its own publications' identifiers awaiting their last reply, with QoS
	pubrecs   []uint16                   // incoming QoS 2 identifiers whose PUBREC came, owed a PUBREL
	acks      []readyAck                 // broker deliveries and PUBRELs handed to its writer, owed an acknowledgement
	sent      []readySent                // immutable observations made while the writer owns each delivery
	queued    []connection.Reply         // writer entries, split into the two queues by nextEntry
	reporting connection.Reply           // written entry whose report has not reached the core
	order     uint64                     // last delivery whose report reached the core
	slots     int                        // reader's available reply slots (design.md, Capacities)
	expiry    uint32                     // CONNECT's expiry, for legal DISCONNECT changes
	meta      connection.PublishMetadata // receipt metadata of the pending body
	packets   uint16                     // SUBSCRIBE and UNSUBSCRIBE identifiers, from 60000
}

// readyAck is an acknowledgement a client owes the broker.
type readyAck struct {
	typ packet.Type // PUBACK, PUBREC or PUBCOMP
	id  uint16
}

// readySent records peer-visible fields before a writer returns ownership.
// The core may reuse the delivery object after its report and settlement.
type readySent struct {
	topic   string
	id      uint16
	qos     byte
	dup     bool
	body    string
	attempt string // a worker offer's attempt id, its Correlation Data (attemptIn); "" for any other delivery
}

// ackNames names the acknowledgements a client sends, for the log.
var ackNames = map[packet.Type]string{packet.PUBACK: "PUBACK", packet.PUBREC: "PUBREC", packet.PUBCOMP: "PUBCOMP"}

// newReadyRun starts a liveScript on readyYAML with seed's random draws
// and, for a recovery flow, saved exchanges to resume. It takes a ghost client's
// session over, so that its first connection's packets are ignored, and
// then nudges the core after every decision.
func newReadyRun(t *testing.T, seed uint64, mode string) *readyRun {
	t.Helper()
	r := &readyRun{liveScript: newLiveScript(t, readyYAML, func(cfg *config.Config) {
		if strings.HasPrefix(mode, "recovery") {
			cfg.Broker.Limits.MaxInflight = 6
			return
		}
		if mode == "events" {
			path := cfg.Broker.Storage.FilePath
			if readyInMemory(seed, mode) {
				cfg.Broker.Storage = readyStorage(t, "mode: memory", "memory_dir: "+strconv.Quote(os.Getenv(readyRAM)), "snapshot_interval: 1m")
			} else {
				cfg.Broker.Storage = readyStorage(t, "mode: normal", "sync_interval: 100ms")
			}
			cfg.Broker.Storage.FilePath = path
		}
		cfg.Broker.Limits.MaxInflight = 1 + int(seed%4)
		cfg.Broker.Limits.MaxIncomingMemory = config.Size(1024 * (1 + seed%4))
		cfg.Broker.Limits.MaxOutgoingMemory = config.Size(1024 * (1 + seed/4%4))
		if strings.HasPrefix(mode, "memory") {
			cfg.Broker.Limits.MaxPacketSize, cfg.Broker.Limits.MaxInflight = memoryPacket, memoryInflight
			cfg.Broker.Limits.MaxIncomingMemory, cfg.Broker.Limits.MaxOutgoingMemory = memoryBudget, memoryBudget
		}
		cfg.Broker.Storage.PublishCommitMaxRecords = 2
		if mode == "will-shared" {
			cfg.Broker.Retained.MaxBytes = 1 << 20
		}
		if mode == "events" {
			cfg.Broker.Storage.PublishCommitInterval = []time.Duration{0, 2 * time.Millisecond, 10 * time.Millisecond}[seed%3]
		}
	}), rng: rand.New(rand.NewPCG(seed, seed^0x5eed)), seed: seed, mode: mode}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d failed; replay: -run TestReadyWorkIsNeverLeftWaiting -ready.seed=%d\nsteps:\n%s", seed, seed, strings.Join(r.log, "\n"))
		}
	})
	if strings.HasPrefix(mode, "recovery") {
		r.prepareRecovery()
	}
	r.probe()
	r.answer = func(a store.Answer) []core.Action {
		r.answers = append(r.answers, a)
		return nil
	}
	r.refuse = func(*core.Group) bool { return r.full }
	r.nudge = r.nudged
	return r
}

// probe leaves one writer paused in its CONNACK write through takeover.
// Its reader can still hand over acknowledgements until that writer stops it.
func (r *readyRun) probe() {
	nudge := r.nudge
	r.nudge = nil
	defer func() { r.nudge = nudge }()
	r.ghost = r.connect("ghost", 0, 1)
	r.ghosts = []*connection.Conn{r.ghost, r.connect("ghost", 0, 1)}
	if !slices.ContainsFunc(r.replies[r.ghost], func(p connection.Reply) bool { return isDisconnect(p, 0x8E) }) {
		r.t.Fatal("the probe connection was not taken over")
	}
}

// prepareRecovery makes a restart fixture allowed by design.md, Delivery,
// Positions and Preparation: the exchanges were saved, but progress past
// their records was not. A crash may leave even never-sent preparations
// to be resent. The stored window belongs to the previous connection;
// the restarted broker and reconnecting client may have smaller capacities.
// The real-store helpers make the session, subscription and records. The
// fixture marks the session connected at the crash and adds its saved
// exchanges while the store is stopped, using storage.md's representation.
// This places its preparations after the publications. No retention can
// remove this channel's records and rescue an otherwise circular wait.
// The owed record is QoS 0: a crash can leave its progress unsaved between
// saved QoS 1/2 exchanges without giving it an exchange of its own. It is
// still owed on return (Words, Session; Delivery, Positions). The payload
// distinguishes this record from every resend despite their common topic.
// These small windows do not shrink MQTT's identifier space: a test of
// identifier exhaustion needs all 65,535 identifiers, not a reduced limit
// absent from the design (Sending; MQTT 5, 2.2.1; storage.md, session_inflight).
func (r *readyRun) prepareRecovery() {
	r.resends = 3 + r.rng.IntN(4)
	owedAfter := 1 + r.rng.IntN(r.resends-1)
	sub := r.connect("recovery", 300, uint16(r.resends))
	r.liveScript.subscribe(sub, "recovery/a")
	r.leave(sub)
	pub := r.connect("recovery-pub", 0, 10)
	offsets := make([]int64, 0, r.resends)
	r.tail = 1
	for i := range r.resends {
		if i == owedAfter {
			r.send(pub, &packet.Publish{Topic: []byte("recovery/a"), Payload: []byte("owed")})
			r.serve()
			r.note("owed matching record at offset %d, no recovered exchange", r.tail)
			r.tail++
		}
		for range r.rng.IntN(3) {
			r.send(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("recovery/gap"), Payload: []byte("g")})
			r.serve()
			r.tail++
		}
		r.send(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("recovery/a"), Payload: []byte("a")})
		r.serve()
		offsets = append(offsets, r.tail)
		r.tail++
	}
	r.leave(pub)
	session, channel := r.sessions["recovery"], r.channelID("recovery")
	r.stop()
	db, err := sql.Open("sqlite3", "file:"+r.cfg.Broker.Storage.FilePath+"?mode=rw")
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), `UPDATE sessions SET "left" = 0 WHERE id = ?`, session); err != nil {
		r.t.Fatal(err)
	}
	for i, offset := range offsets {
		qos, phase := 1, 1
		if r.mode == "recovery-qos2" {
			qos = 2
			if i%3 == 1 {
				phase = 2
			}
			// A legal crash fixture: these publications entered at QoS 2;
			// some outgoing PUBRECs committed before the crash (phase 2).
			if _, err := db.ExecContext(context.Background(), `UPDATE records SET qos = 2 WHERE channel = ? AND "offset" = ?`, channel, offset); err != nil {
				r.t.Fatal(err)
			}
		}
		if phase == 1 {
			r.wantRecovery = append(r.wantRecovery, readySent{"recovery/a", uint16(i + 1), byte(qos), true, "a", ""})
		}
		row := testkit.With(testkit.TableNamed(r.t, "session_inflight").Valid[0], testkit.Row{
			"session": session, "packet_id": i + 1, "seq": i + 1, "channel": channel,
			"offset": offset, "qos": qos, "phase": phase, "moves": 1, "share": 0,
		})
		if err := testkit.InsertRow(db, "session_inflight", row); err != nil {
			r.t.Fatal(err)
		}
	}
	r.wantRecovery = append(r.wantRecovery, readySent{"recovery/a", 0, 0, false, "owed", ""})
	if err := db.Close(); err != nil {
		r.t.Fatal(err)
	}
	r.cfg.Broker.Limits.MaxInflight = 1 + r.rng.IntN(4)
	r.note("restart: saved exchanges at offsets %v, position 1, tail %d, max_inflight %d", offsets, r.tail, r.cfg.Broker.Limits.MaxInflight)
	r.restart(r.now)
}

// resume drains a reconnect's obligations using the same random-order
// helpers as the main driver. No client abandons the session, changes its
// subscriptions, seeks or publishes until this recovery has finished.
func (r *readyRun) resume() {
	expiry := uint32(300)
	if r.mode == "recovery-zero" {
		expiry = 0
	}
	c := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "recovery",
		SessionExpiryInterval: expiry, ReceiveMaximum: uint16(1 + r.rng.IntN(4))})
	r.learn()
	r.settle()
	if ack := connackIn(r.decisions(c.conn), c.conn); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
		r.fail("recovery CONNECT got %+v; want a successful CONNACK with Session Present", ack)
	}
	delivered := len(c.sent)
	for i, d := range c.sent {
		if i >= len(r.wantRecovery) || d != r.wantRecovery[i] {
			r.fail("recovery: delivery %d is %+v; want ordered resends then the normal owed record %v", i, d, r.wantRecovery)
		}
	}
	remaining := r.count(`SELECT count(*) FROM session_inflight WHERE session = (SELECT id FROM sessions WHERE client = 'recovery')`)
	position := r.tail
	if expiry != 0 {
		position = int64(r.count(`SELECT "offset" FROM positions WHERE session = (SELECT id FROM sessions WHERE client = 'recovery') AND channel = (SELECT id FROM channels WHERE name = 'recovery')`))
	}
	if delivered != len(r.wantRecovery) || remaining != 0 || position != r.tail {
		r.fail("recovery still waiting with every external obligation answered: delivered %d/%d, stored exchanges %d, position %d/tail %d (design.md, Execution, Ready work, 3; Delivery, Positions)",
			delivered, len(r.wantRecovery), remaining, position, r.tail)
	}
}

// nudged hands the core the ghost's ignored PUBACK at the time of the
// decision just taken, and fails the run if it leads to any action.
func (r *readyRun) nudged(acts []core.Action) {
	for _, a := range acts {
		if a.Line != (logger.Line{}) {
			r.logs++
		}
		if a.Cut {
			for _, c := range r.conns {
				if c.conn == a.Conn {
					c.broken, c.mustEnd = true, true
				}
			}
		}
	}
	if r.ghost == nil {
		return
	}
	// A due timer or exhausted store credit prevents the loop from taking
	// another inbound event. Do not manufacture one at that boundary.
	at := r.core.Timer()
	if r.core.Credit() == 0 || (!at.IsZero() && !at.After(r.now)) {
		return
	}
	acts = r.core.Handle(nil, connection.Event{Kind: connection.Received, Conn: r.ghost, Packet: &packet.Ack{PacketID: 1}}, r.now)
	if len(acts) == 0 {
		return
	}
	r.fail("after step %d, an ignored packet at the same time led to %s: work the core could do was left waiting for an unrelated event (design.md, Execution, Ready work)",
		len(r.log), describe(acts))
}

// fail fails the run with its seed; the run's cleanup reports its steps.
func (r *readyRun) fail(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf("seed %d: %s", r.seed, fmt.Sprintf(format, args...))
}

// describe names each action's kind.
func describe(acts []core.Action) string {
	names := make([]string, 0, len(acts))
	for _, a := range acts {
		switch {
		case a.Read != nil:
			names = append(names, fmt.Sprintf("read %T", a.Read))
		case a.Write != nil:
			var members []string
			if g, ok := a.Write.(*core.Group); ok {
				for _, w := range g.Writes {
					members = append(members, fmt.Sprintf("%T", w))
				}
			}
			names = append(names, fmt.Sprintf("write %v", members))
		case a.Bytes != 0:
			names = append(names, fmt.Sprintf("%d body bytes given", a.Bytes))
		case a.Cut:
			names = append(names, "a cut")
		case a.Line != (logger.Line{}):
			names = append(names, "a log line")
		default:
			names = append(names, "reply "+show(a.Reply))
		}
	}
	return strings.Join(names, ", ")
}

// note logs a step.
func (r *readyRun) note(format string, args ...any) {
	r.log = append(r.log, fmt.Sprintf("%3d %v ", len(r.log)+1, r.now.Sub(epoch))+fmt.Sprintf(format, args...))
}

// step plays one random step that can be played, then updates what the
// clients know from the replies it led to.
func (r *readyRun) step() {
	if r.fire() {
		r.learn()
		return
	}
	for played := false; !played; {
		switch n := r.rng.IntN(100); {
		case n < 2:
			played = r.logged()
		case n < 12:
			played = r.carryOut()
		case n < 23:
			played = r.answerOne()
		case n < 29:
			played = r.reportOne()
		case n < 38:
			played = r.write()
		case n < 42:
			played = r.acknowledge()
		case n < 46:
			played = r.release()
		case n < 52:
			played = r.open()
		case n < 58:
			played = r.subscribe()
		case n < 61:
			played = r.unsubscribe()
		case n < 71:
			played = r.ask()
		case n < 81:
			played = r.handOver()
		case n < 85:
			played = r.end()
		case n < 89:
			played = r.closed()
		case n < 95:
			played = r.fire()
		default:
			played = r.wait()
		}
	}
	r.learn()
}

// quiet leaves the broker nothing to do: the store carries out every
// request, writers report every entry, clients acknowledge everything they
// owe and hand over every body given, while time passes every deadline of
// the next two seconds. Then no work is left that contributes admission
// delay, so the delivery admission gate is open, and a body a client's
// reader asks for is given in that event (design.md, Execution,
// Capacities, Delivery admission: "No contributing work means zero
// delay"; "Once closed, it opens when every contributing delay is below
// half that value, or no qualifying work remains"; Message memory: no
// message bytes remain in transit). Work registered nowhere never runs,
// and its delay holds the gate closed: the nudge cannot find it, but this
// does, unless an unrelated event rescues it first.
func (r *readyRun) quiet() {
	r.note("quiet")
	r.learn()
	r.settle()
	for range 4 {
		r.now = r.now.Add(500 * time.Millisecond)
		r.note("waits 500ms")
		for r.fire() {
			r.settle()
		}
	}
	for _, c := range r.conns {
		if c.live() && c.connack && c.asked != 0 {
			r.fail("admission: %s's reader still waits for %d bytes after all external obligations", c.name, c.asked)
		}
	}
	c := r.pick(func(c *readyConn) bool { return c.connack && c.reading() })
	if c == nil {
		return
	}
	p := &packet.Publish{Topic: []byte("events/a"), Payload: []byte("idle")}
	c.body, c.asked, c.kept = p, bodyOf(p), bodyOf(p)
	c.slots--
	before := r.granted[c.conn]
	r.note("%s: asks %d bytes, nothing left to do", c.name, c.asked)
	r.event(connection.Event{Kind: connection.BytesRequested, Conn: c.conn, Bytes: c.asked})
	if got := r.granted[c.conn] - before; got != c.asked {
		r.fail("%s's reader asked for %d bytes with nothing left to do and was given %d: the admission gate stays closed, work was left waiting", c.name, c.asked, got)
	}
}

// settle plays, in a random order, every step that answers what the broker
// asked: the store's answers, writers' reports, clients' acknowledgements
// and PUBRELs, the hand-over of bodies given, the end of each reader whose
// writer took a final entry or a refusing CONNACK (design.md, Closing a
// connection, 1: "The writer writes the last packet, ends its sending and
// stops the reader") and the close of each connection whose reader ended;
// until none is left.
func (r *readyRun) settle() {
	r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.acknowledge, r.release, r.handOver, r.stopped, r.closed, r.logged)
}

// drain answers a chosen set of external obligations in random order.
// Omitting acknowledgement permits a peer to keep its exchanges unfinished.
func (r *readyRun) drain(actions ...func() bool) {
	r.learn()
	for steps := 0; ; steps++ {
		if steps > liveLimit {
			r.fail("watchdog: the core still asks after %d answers", liveLimit)
		}
		played := false
		for _, n := range r.rng.Perm(len(actions)) {
			if played = actions[n](); played {
				break
			}
		}
		if !played {
			return
		}
		r.learn()
	}
}

// pick returns a random connection that ok accepts, nil for none.
func (r *readyRun) pick(ok func(c *readyConn) bool) *readyConn {
	var from []*readyConn
	for _, c := range r.conns {
		if ok(c) {
			from = append(from, c)
		}
	}
	if len(from) == 0 {
		return nil
	}
	return from[r.rng.IntN(len(from))]
}

// live reports whether c's client may still send packets the core takes.
func (c *readyConn) live() bool { return c.connect && !c.mustEnd && !c.ended }

// reading reports whether c's reader is between packets, so may hand over
// any packet next: live, and not waiting to hand over a PUBLISH whose body
// it asked for (design.md, Message memory, "A reader has at most one
// request or granted body").
func (c *readyConn) reading() bool { return c.live() && c.asked == 0 && c.slots > 0 }

// carryOut starts the next request in a randomly chosen store lane. The
// writer and reader request channels are FIFO. At most read_connections
// readers can be paused, and the core may dispatch only one write group.
// The existing real-store helpers fix the view and execute the operation;
// answers models a pause before the worker sends each notification.
func (r *readyRun) carryOut() bool {
	reads := 0
	for _, a := range r.answers {
		if _, ok := a.Op.(store.Read); ok && !a.View {
			reads++
		}
	}
	var choices []int
	readSeen, writeSeen := false, false
	for i, op := range r.queue {
		switch op.(type) {
		case store.Read:
			if !readSeen && reads < r.cfg.Broker.Storage.ReadConnections {
				choices = append(choices, i)
			}
			readSeen = true
		case store.Write: // a group, normal mode's sync or memory mode's snapshot fold, in the one write lane
			if !writeSeen && !r.stalled {
				choices = append(choices, i)
			}
			writeSeen = true
		}
	}
	if len(choices) == 0 {
		return false
	}
	i := choices[r.rng.IntN(len(choices))]
	op := r.queue[i]
	r.queue = slices.Delete(r.queue, i, i+1)
	switch op := op.(type) {
	case store.Read:
		r.note("store: read %T", op)
		if r.reads != nil {
			r.reads(op)
		}
		if r.pauseRead != nil && r.pauseRead(op) {
			if r.pausedRead != nil {
				r.fail("driver: more than one read selected for the pause")
			}
			r.pausedRead = op
		}
		r.read(op)
	case *core.Group:
		r.note("store: write %s, full %v", describe([]core.Action{{Write: op}}), r.full)
		r.commit(op)
	case store.Write:
		r.note("store: write %T", op)
		r.lone++
		r.carry(op)
	}
	return true
}

// answerOne lets one worker send its next notification. A result cannot
// overtake that read's view confirmation. Other workers may finish first.
func (r *readyRun) answerOne() bool {
	var choices []int
	for i, a := range r.answers {
		if !a.View && a.Op == r.pausedRead {
			continue
		}
		if !slices.ContainsFunc(r.answers[:i], func(b store.Answer) bool { return a.Op == b.Op }) {
			choices = append(choices, i)
		}
	}
	if len(choices) == 0 {
		return false
	}
	i := choices[r.rng.IntN(len(choices))]
	a := r.answers[i]
	r.answers = slices.Delete(r.answers, i, i+1)
	r.note("store: answer %T, view %v, error %v", a.Op, a.View, a.Err)
	r.now = r.now.Add(eventGap)
	r.take(r.core.Answer(nil, a, r.now))
	return true
}

// nextEntry chooses between the writer's two FIFO heads. Replies go first,
// except a recovered PUBREL waits for its preceding PUBLISH reports.
func (c *readyConn) nextEntry() int {
	if c.reporting != nil {
		return -1 // the writer's previous send to inbound has not completed
	}
	delivery := -1
	for i, p := range c.queued {
		if _, ok := p.(*connection.Delivery); ok {
			if delivery == -1 {
				delivery = i
			}
			continue
		}
		if rel, ok := p.(*connection.Release); ok && !c.final && !c.broken && rel.After > c.order {
			break
		}
		return i
	}
	return delivery
}

// write completes one writer entry. The peer may acknowledge it before its
// report reaches the core, but cannot acknowledge an entry merely queued.
func (r *readyRun) write() bool {
	c := r.pick(func(c *readyConn) bool { return c.nextEntry() >= 0 })
	if c == nil {
		return false
	}
	i := c.nextEntry()
	p := c.queued[i]
	c.queued = slices.Delete(c.queued, i, i+1)
	result := connection.DeliveryWritten
	if r.mode == "events" && !c.final && r.rng.IntN(40) == 0 {
		c.broken, c.mustEnd = true, true
		r.note("%s writer: socket failed", c.name)
	}
	if c.final || c.broken {
		result = connection.DeliveryDiscarded
	}
	if d, ok := p.(*connection.Delivery); ok && !c.final && !c.broken && !d.Dup && d.Channel == 1 && d.Expires != 0 && d.Expires <= r.now.UnixMilli() {
		result = connection.DeliveryExpired
	}
	r.note("%s writer: writes %s (%d)", c.name, show(p), result)
	switch p := p.(type) {
	case *packet.Connack:
		c.slots++
		c.connack = p.ReasonCode == 0 && !c.broken
		c.mustEnd = c.mustEnd || p.ReasonCode != 0
		c.broken = c.broken || p.ReasonCode != 0
	case *packet.Suback, *packet.Unsuback, connection.Pingresp:
		c.slots++
	case *connection.Ack:
		c.slots++
		if !c.final && !c.broken {
			c.answered(p)
		}
	case *connection.Delivery:
		p.Result = result
		c.reporting = p
		if result == connection.DeliveryWritten {
			c.sent = append(c.sent, readySent{string(p.Topic), p.PacketID, p.QoS, p.Dup, string(p.Payload), string(attemptIn(r.t, p))})
			if p.QoS != 0 {
				typ := packet.PUBACK
				if p.QoS == 2 {
					typ = packet.PUBREC
				}
				c.acks = append(c.acks, readyAck{typ, p.PacketID})
			}
		}
	case *connection.Release:
		p.Result = result
		c.reporting = p
		if !p.Resumed {
			c.slots++
		}
		if result == connection.DeliveryWritten {
			c.acks = append(c.acks, readyAck{packet.PUBCOMP, p.PacketID})
		}
	case *packet.Disconnect, connection.Close:
		c.final, c.mustEnd = true, true
	}
	return true
}

// reportOne returns the completed entry with the existing report helper.
// A writer cannot report another entry or Closed before this handover ends.
func (r *readyRun) reportOne() bool {
	c := r.pick(func(c *readyConn) bool { return c.reporting != nil })
	if c == nil {
		return false
	}
	p := c.reporting
	result := connection.DeliveryWritten
	switch p := p.(type) {
	case *connection.Delivery:
		result, c.order = p.Result, p.Order
	case *connection.Release:
		result = p.Result
	}
	i := slices.IndexFunc(r.writing, func(h handed) bool { return h.Conn == c.conn && h.recorded == p })
	if i < 0 {
		r.fail("driver: missing writer report for %s", c.name)
	}
	c.reporting = nil
	r.note("%s writer: reports %s (%d)", c.name, show(p), result)
	for r.fire() {
		r.learn()
	}
	r.report(i, result)
	return true
}

// acknowledge has a random client acknowledge the oldest delivery or
// PUBREL it is owed, once handed to its writer. The workers flow refuses
// none: what a refusing PUBACK does to a worker offer is not this flow's
// to choose.
func (r *readyRun) acknowledge() bool {
	c := r.pick(func(c *readyConn) bool { return c.reading() && len(c.acks) > 0 })
	if c == nil {
		return false
	}
	r.acknowledgeOf(c)
	return true
}

// acknowledgeOf has c's client, between packets, acknowledge the oldest
// delivery or PUBREL it is owed.
func (r *readyRun) acknowledgeOf(c *readyConn) {
	a := c.acks[0]
	c.acks = c.acks[1:]
	if a.typ == packet.PUBREC {
		c.slots--
	}
	var reason byte
	if !strings.HasPrefix(r.mode, "recovery") && r.mode != "workers" && a.typ != packet.PUBCOMP && r.rng.IntN(8) == 0 {
		reason = 0x80
	}
	var p any = &connection.Ack{Type: a.typ, PacketID: a.id, ReasonCode: reason}
	if a.typ == packet.PUBACK {
		p = &packet.Ack{PacketID: a.id, ReasonCode: reason}
	}
	r.note("%s: %s %d reason 0x%02X", c.name, ackNames[a.typ], a.id, reason)
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: p})
}

// release has a random client send PUBREL for an incoming QoS 2 exchange
// whose PUBREC came.
func (r *readyRun) release() bool {
	c := r.pick(func(c *readyConn) bool { return c.reading() && len(c.pubrecs) > 0 })
	if c == nil {
		return false
	}
	if !c.connack {
		return false
	}
	c.slots--
	id := c.pubrecs[0]
	c.pubrecs = c.pubrecs[1:]
	r.note("%s: PUBREL %d", c.name, id)
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: &connection.Ack{Type: packet.PUBREL, PacketID: id}})
	return true
}

// open accepts a new connection for a random client id and hands over
// its MQTT 5 CONNECT: Clean Start 0 or 1, durable or not, a small Receive
// Maximum.
func (r *readyRun) open() bool {
	if len(r.conns) >= 8 {
		return false
	}
	client := readyClients[r.rng.IntN(len(readyClients))]
	p := &packet.Connect{Version: packet.V5, CleanStart: r.rng.IntN(3) == 0, ClientID: client,
		SessionExpiryInterval: []uint32{0, 2, 300}[r.rng.IntN(3)], ReceiveMaximum: uint16(1 + r.rng.IntN(4))}
	if r.rng.IntN(3) == 0 {
		p.WillFlag, p.Will = true, r.will("b/will", uint32(r.rng.IntN(4)))
	}
	r.openClient(p)
	return true
}

// openClient accepts a connection and hands over p, leaving its store
// answers and writer reports to the driver.
func (r *readyRun) openClient(p *packet.Connect) *readyConn {
	properties := []packet.Property{{ID: packet.SessionExpiryInterval, Int: p.SessionExpiryInterval},
		{ID: packet.ReceiveMaximum, Int: uint32(p.ReceiveMaximum)}}
	if p.MaximumPacketSize != 0 {
		properties = append(properties, packet.Property{ID: packet.MaximumPacketSize, Int: p.MaximumPacketSize})
	}
	p.Properties = connectProperties(properties)
	p = decodedConnect(p)
	r.accepts++
	c := &readyConn{conn: new(connection.Conn), name: fmt.Sprintf("%s#%d", p.ClientID, r.accepts), ids: map[uint16]byte{}, packets: 60000, slots: connection.ReplySlots - 1, expiry: p.SessionExpiryInterval}
	r.conns = append(r.conns, c)
	r.note("%s: accepted; CONNECT clean %v expiry %d receive maximum %d", c.name, p.CleanStart, p.SessionExpiryInterval, p.ReceiveMaximum)
	r.event(connection.Event{Kind: connection.Accepted, Conn: c.conn,
		Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 2, byte(r.accepts)), Port: 40000 + r.accepts}}})
	r.learn()
	c.connect = true
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: p})
	return c
}

// subscribe has a random client subscribe to one or two filters.
func (r *readyRun) subscribe() bool {
	c := r.pick(func(c *readyConn) bool { return c.reading() && c.connack })
	if c == nil {
		return false
	}
	subs := make([]connection.Subscription, 1+r.rng.IntN(2))
	for i := range subs {
		subs[i] = connection.Subscription{Filter: readyFilters[r.rng.IntN(len(readyFilters))], QoS: byte(r.rng.IntN(3))}
	}
	c.packets++
	c.slots--
	r.note("%s: SUBSCRIBE %d %v", c.name, c.packets, subs)
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: &connection.Subscribe{PacketID: c.packets, Filters: subs}})
	return true
}

// unsubscribe has a random client unsubscribe from a filter.
func (r *readyRun) unsubscribe() bool {
	c := r.pick(func(c *readyConn) bool { return c.reading() && c.connack })
	if c == nil {
		return false
	}
	filter := readyFilters[r.rng.IntN(len(readyFilters))]
	c.packets++
	c.slots--
	r.note("%s: UNSUBSCRIBE %d %s", c.name, c.packets, filter)
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: &connection.Unsubscribe{PacketID: c.packets, Filters: []string{filter}}})
	return true
}

// ask has a random client's reader ask for the body of its next packet: a
// PUBLISH at QoS 0, 1 or 2, retained on $broadcast now and then, or a
// seek. A reader asks for one body at a time (design.md, Message memory),
// and a client keeps a few publications of its own unanswered.
func (r *readyRun) ask() bool {
	c := r.pick(func(c *readyConn) bool { return c.reading() && len(c.ids) < r.cfg.Broker.QoS2.MaxInflightPerClient })
	if c == nil {
		return false
	}
	qos := byte(r.rng.IntN(3))
	if !c.connack {
		qos = 0 // a reader takes no QoS 1 or 2 PUBLISH before its CONNACK is written
	}
	var id uint16
	if qos != 0 {
		id = c.freeID()
	}
	p := &packet.Publish{QoS: qos, PacketID: id}
	var properties []packet.Property
	switch n := r.rng.IntN(8); {
	case n < 2:
		p.Topic, p.Payload = []byte(eventsSeek), []byte(readySeeks[r.rng.IntN(len(readySeeks))])
		if r.rng.IntN(2) == 0 {
			properties = append(properties, packet.Property{ID: packet.ResponseTopic, String: "answers/seek"},
				packet.Property{ID: packet.CorrelationData, Binary: []byte{byte(id)}})
		}
	case n == 2:
		// These requests currently reach the core only as reader refusals.
		// Never invent RequestPublish routes absent from the reader contract.
		requests := []string{"$saguin/kv/get", "$saguin/catalogue/events", "$saguin/sessions/disconnect", "$saguin/queue/jobs/response", "$saguin/unknown"}
		p.Topic, p.Payload = []byte(requests[r.rng.IntN(len(requests))]), []byte("c0")
		properties = append(properties, packet.Property{ID: packet.ResponseTopic, String: "answers/service"})
	default:
		p.Topic = []byte(readyTopics[r.rng.IntN(len(readyTopics))])
		p.Payload = make([]byte, r.rng.IntN(310)) // empty clears a retained value or leaves a latest tombstone
		p.Retain = p.Topic[0] == 'b' && r.rng.IntN(3) == 0
		if r.rng.IntN(3) == 0 {
			properties = append(properties, packet.Property{ID: packet.MessageExpiryInterval, Int: uint32(r.rng.IntN(3))})
		}
	}
	r.askPacket(c, p, properties...)
	return true
}

// freeID returns the lowest packet identifier c's client can give its next
// QoS 1 or 2 publication: none of its own unanswered, nor held awaiting its
// PUBREL.
func (c *readyConn) freeID() uint16 {
	id := uint16(1)
	for c.ids[id] != 0 || slices.Contains(c.pubrecs, id) {
		id++
	}
	return id
}

// askPacket prepares the same metadata and property bytes as the reader.
// It reserves a reply slot before the body and leaves handover to the driver.
func (r *readyRun) askPacket(c *readyConn, p *packet.Publish, properties ...packet.Property) {
	if !c.reading() {
		r.fail("driver: %s is not between packets", c.name)
	}
	if p.QoS != 0 && (!c.connack || len(c.ids) >= r.cfg.Broker.QoS2.MaxInflightPerClient || c.ids[p.PacketID] != 0) {
		r.fail("driver: %s cannot admit packet identifier %d", c.name, p.PacketID)
	}
	c.body = readerBody(r.t, r.cfg, p, properties...)
	for _, prop := range properties {
		if prop.ID == packet.MessageExpiryInterval {
			c.meta.Expires, c.meta.Expiry = true, prop.Int
		}
	}
	if p.QoS == 2 {
		r.quota++
		c.quota = 1<<16 + r.quota
	}
	c.slots--
	c.asked, c.kept = payload.BodyMemory(int(bodyOf(p))), bodyOf(p)
	if q, ordinary := c.body.(*packet.Publish); ordinary {
		c.kept = compressed(q)
	}
	r.note("%s: asks %d bytes for %s QoS %d id %d", c.name, c.asked, p.Topic, p.QoS, p.PacketID)
	r.event(connection.Event{Kind: connection.BytesRequested, Conn: c.conn, Bytes: c.asked})
}

// readerBody gives p the property section of properties, decoded and
// stored as a reader keeps them, and returns what that reader hands the
// core for it: a *connection.RequestPublish for a seek or a queue response,
// routed to its channel with its Response Topic and Correlation Data, as
// the reader's routing recognizes them in that order, else p itself.
func readerBody(t testing.TB, cfg *config.Config, p *packet.Publish, properties ...packet.Property) any {
	t.Helper()
	wire, err := packet.AppendProperties(nil, packet.PUBLISH, properties)
	if err != nil {
		t.Fatal(err)
	}
	p.Properties, _, err = packet.DecodeProperties(wire, packet.PUBLISH)
	if err != nil {
		t.Fatal(err)
	}
	p.StoredProperties = wire
	routing := config.NewRouting(cfg.Channels)
	kind := connection.SeekRequest
	index, ok := routing.SeekChannel(string(p.Topic), false)
	if !ok {
		kind = connection.QueueResponse
		index, ok = routing.QueueResponseChannel(string(p.Topic))
	}
	if !ok {
		return p
	}
	q := &connection.RequestPublish{Kind: kind, Publish: *p, Channel: index}
	for _, prop := range properties {
		switch prop.ID {
		case packet.ResponseTopic:
			q.ResponseTopic = prop.String
		case packet.CorrelationData:
			q.Correlation = prop.Binary
		}
	}
	return q
}

// handOver has a random reader whose body was given hand its packet over.
func (r *readyRun) handOver() bool {
	c := r.pick(func(c *readyConn) bool { return c.live() && c.asked != 0 && r.granted[c.conn] >= c.asked })
	if c == nil {
		return false
	}
	r.handOverOf(c)
	return true
}

// handOverOf has c's reader, its body given, hand its packet over.
func (r *readyRun) handOverOf(c *readyConn) {
	r.granted[c.conn] -= c.asked
	var qos byte
	var id uint16
	switch p := c.body.(type) {
	case *packet.Publish:
		qos, id = p.QoS, p.PacketID
	case *connection.RequestPublish:
		qos, id = p.QoS, p.PacketID
	}
	if qos != 0 {
		c.ids[id] = qos
	}
	r.note("%s: hands over %T QoS %d id %d", c.name, c.body, qos, id)
	ev := connection.Event{Kind: connection.Received, Conn: c.conn, Packet: c.body, Bytes: c.kept, Quota: c.quota, Publish: c.meta}
	ev.Publish.Received = r.now.UnixMilli() // complete body, before core handover
	if p, ordinary := c.body.(*packet.Publish); ordinary && strings.HasPrefix(string(p.Topic), "$saguin/") {
		ev.Kind, ev.Reason = connection.Refused, 0x90
		ev.Packet = connection.PublishRefusal{QoS: qos, PacketID: id}
	}
	if _, request := c.body.(*connection.RequestPublish); qos == 0 && !request {
		c.slots++
	}
	c.meta = connection.PublishMetadata{}
	c.asked, c.kept, c.body, c.quota = 0, 0, nil, 0
	r.event(ev)
}

// end has a random connection's reader end: a normal DISCONNECT, or a lost
// connection; a connection refused or given its final entry, or whose
// reader waits to hand over a PUBLISH, ends lost.
func (r *readyRun) end() bool {
	c := r.pick(func(c *readyConn) bool { return c.connect && !c.ended })
	if c == nil {
		return false
	}
	var p any
	if c.connack && c.reading() && r.rng.IntN(2) == 0 {
		var expiry *uint32
		if c.expiry != 0 && r.rng.IntN(2) == 0 {
			n := uint32(r.rng.IntN(4))
			expiry = &n
		}
		p = disconnectWith([]byte{0, 4}[r.rng.IntN(2)], expiry)
	}
	r.depart(c, p)
	return true
}

// depart keeps Ended after every reader packet and before Closed.
func (r *readyRun) depart(c *readyConn, p any) {
	c.ended, c.asked, c.body = true, 0, nil
	r.note("%s: Ended %v", c.name, p != nil)
	r.event(connection.Event{Kind: connection.Ended, Conn: c.conn, Packet: p})
}

// stopped ends the reader of a random connection whose writer took a
// final entry or a refusing CONNACK.
func (r *readyRun) stopped() bool {
	c := r.pick(func(c *readyConn) bool { return c.mustEnd && !c.ended })
	if c == nil {
		return false
	}
	c.ended, c.asked, c.body = true, 0, nil
	r.note("%s: Ended, stopped by its writer", c.name)
	r.event(connection.Event{Kind: connection.Ended, Conn: c.conn})
	return true
}

// closed has a random connection's writer report its close: once its
// reader has ended, its final entry has come and it has reported every
// entry it was handed (design.md, Closing a connection, 4).
func (r *readyRun) closed() bool {
	c := r.pick(func(c *readyConn) bool {
		return c.ended && c.final && len(c.queued) == 0 && c.reporting == nil && !slices.ContainsFunc(r.writing, func(h handed) bool { return h.Conn == c.conn })
	})
	if c == nil {
		return false
	}
	r.conns = slices.DeleteFunc(r.conns, func(o *readyConn) bool { return o == c })
	r.note("%s: Closed", c.name)
	r.event(connection.Event{Kind: connection.Closed, Conn: c.conn})
	return true
}

// fire hands the core its timer, when due.
func (r *readyRun) fire() bool {
	at := r.core.Timer()
	if at.IsZero() || at.After(r.now) {
		return false
	}
	r.note("timer due at %v", at.Sub(epoch))
	r.take(r.core.Fire(nil, r.now))
	return true
}

// wait lets up to 1.5 seconds pass with no event.
func (r *readyRun) wait() bool {
	d := time.Duration(1+r.rng.IntN(1500)) * eventGap
	r.now = r.now.Add(d)
	r.note("waits %v", d)
	return true
}

// learn transfers actions to the connection endpoints. ReceiveQuota is a
// reader-local restoration, not a wire reply; CONNACK still gates PUBREL.
// All other replies wait for the writer, including terminal entries.
func (r *readyRun) learn() {
	for _, c := range r.conns {
		replies := r.replies[c.conn]
		for _, p := range replies[c.seen:] {
			if quota, ok := p.(connection.ReceiveQuota); ok {
				for _, id := range quota.Identifiers {
					c.ids[id] = 2
					c.pubrecs = append(c.pubrecs, id)
				}
			} else {
				c.queued = append(c.queued, p)
			}
		}
		c.seen = len(replies)
		if c.slots < 0 || c.slots > connection.ReplySlots {
			r.fail("reply slots: %s has %d, outside 0..%d", c.name, c.slots, connection.ReplySlots)
		}
	}
}

// answered applies the broker's answer a to one of c's own publications.
func (c *readyConn) answered(a *connection.Ack) {
	switch {
	case a.Type == packet.PUBREC && a.ReasonCode < 0x80:
		if !slices.Contains(c.pubrecs, a.PacketID) {
			c.pubrecs = append(c.pubrecs, a.PacketID)
		}
	case a.Type == packet.PUBACK || a.Type == packet.PUBREC || a.Type == packet.PUBCOMP:
		delete(c.ids, a.PacketID)
	}
}

// event respects timer priority over the inbound lane. Store answers keep
// their higher priority and may arrive before a due timer.
func (r *readyRun) event(ev connection.Event) {
	for r.fire() {
		r.learn()
	}
	if r.core.Credit() == 0 {
		r.fail("driver: inbound event without store credit")
	}
	r.liveScript.event(ev)
}

// will constructs a wire-decoded Will, including its stored properties.
func (r *readyRun) will(topic string, delay uint32) packet.Will {
	wire, err := packet.AppendWillProperties(nil, []packet.Property{{ID: packet.WillDelayInterval, Int: delay}})
	if err != nil {
		r.t.Fatal(err)
	}
	properties, _, err := packet.DecodeWillProperties(wire)
	if err != nil {
		r.t.Fatal(err)
	}
	return packet.Will{QoS: 1, Topic: topic, Payload: []byte("will"), Properties: properties}
}

// until advances only to named timer deadlines, draining their external
// obligations in random orders. There is no periodic readiness sweep.
// Time never goes back: draining a deadline's obligations moves the clock
// an eventGap per event, and may carry it past end, where it stays. Set
// back, it would put the next event before what the core has already
// handled, and a deadline worked out from it, such as when a worker's
// hold runs out, would fall short of the core's own.
func (r *readyRun) until(end time.Time) {
	for steps := 0; ; steps++ {
		if steps > liveLimit {
			r.fail("watchdog: timers did not reach %v", end)
		}
		at := r.core.Timer()
		if at.IsZero() || at.After(end) {
			if r.now.Before(end) {
				r.now = end
			}
			return
		}
		if at.After(r.now) {
			r.now = at
		}
		r.fire()
		r.settle()
	}
}

// shared keeps assignments outstanding while a member ends and another
// joins. The first durable member must capture a volatile group's obligations;
// ending its member returns QoS 1 and drops QoS 2 (SUBSCRIBE, A shared group).
// The last unsubscribe removes group state; MQTT exchanges still finish.
func (r *readyRun) shared() {
	const filter = "$share/ready/recovery/#"
	first := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "first", ReceiveMaximum: 1})
	second := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "second", SessionExpiryInterval: 300, ReceiveMaximum: 2})
	r.settle()
	qos := byte(1 + r.rng.IntN(2))
	r.subscription(first, filter, qos, false)
	r.settle()
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "publisher", ReceiveMaximum: 1})
	r.settle()
	for id := uint16(1); id <= 3; id++ {
		r.askPacket(pub, &packet.Publish{QoS: qos, PacketID: id, Topic: []byte("recovery/a"), Payload: []byte("shared")})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.release, r.handOver)
	}
	if len(first.acks) == 0 {
		r.fail("shared: no assignment before membership change")
	}
	r.subscription(second, filter, qos, false)
	// End the volatile member with its exchange unfinished, before or after
	// the first durable member's save, as the random store schedule decides.
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.depart(first, nil)
	r.settle()
	r.subscription(second, filter, qos, true)
	r.settle()
	if n := r.count(`SELECT count(*) FROM share_groups`); n != 0 {
		r.fail("shared: last unsubscribe left %d groups with no external obligation", n)
	}
	r.quiet()
}

// subscription is the common client handover used by the flow fixtures.
func (r *readyRun) subscription(c *readyConn, filter string, qos byte, leave bool) {
	if !c.reading() {
		r.fail("driver: subscription while %s cannot read", c.name)
	}
	c.packets++
	c.slots--
	var p any = &connection.Subscribe{PacketID: c.packets, Filters: []connection.Subscription{{Filter: filter, QoS: qos}}}
	if leave {
		p = &connection.Unsubscribe{PacketID: c.packets, Filters: []string{filter}}
	}
	r.note("%s: %T %s", c.name, p, filter)
	r.event(connection.Event{Kind: connection.Received, Conn: c.conn, Packet: p})
}

// drainOne lets one external worker proceed without draining everything.
func (r *readyRun) drainOne() {
	r.drainOneOf(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release)
}

// drainOneOf lets one of a chosen set of external obligations proceed, the
// first in a random order that can.
func (r *readyRun) drainOneOf(actions ...func() bool) {
	for _, i := range r.rng.Perm(len(actions)) {
		if actions[i]() {
			r.learn()
			return
		}
	}
}

// wills observes the two required outcomes against the store: a delayed
// Will publishes once when due; reconnection before its due time cancels it.
// A clean DISCONNECT also cancels, and expiry brings a longer delay forward.
func (r *readyRun) wills() {
	for _, outcome := range []string{"delayed", "reconnect", "clean", "expiry"} {
		delay, expiry := uint32(2), uint32(8)
		if outcome == "expiry" {
			delay, expiry = 8, 2
		}
		topic := "recovery/" + outcome
		c := r.openClient(&packet.Connect{Version: packet.V5, ClientID: outcome, SessionExpiryInterval: expiry,
			ReceiveMaximum: 1, WillFlag: true, Will: r.will(topic, delay)})
		r.settle()
		var disc any
		if outcome == "clean" {
			disc = &packet.Disconnect{}
		}
		r.depart(c, disc)
		for range r.rng.IntN(5) {
			r.drainOne()
		}
		if outcome == "reconnect" {
			r.openClient(&packet.Connect{Version: packet.V5, ClientID: outcome, SessionExpiryInterval: 8, ReceiveMaximum: 1})
		}
		r.settle()
		r.until(r.now.Add(3 * time.Second))
		want := 1
		if outcome == "clean" || outcome == "reconnect" {
			want = 0
		}
		if got := r.count(`SELECT count(*) FROM records WHERE topic = '` + topic + `'`); got != want {
			r.fail("will %s: stored %d publications, want %d after the deadline and all answers", outcome, got, want)
		}
	}
	r.quiet()
}

// sharedWill overlaps a session-ending retained Will with another member's
// acknowledgement. CONNECT, Decide and Store require the Will and clean
// start in one transaction; Delivery, Positions requires terminal evidence
// for the volatile member. Ready work, 3 requires both to remain able to
// finish while the retained-charge read is outstanding. Only that read's
// result is held: its view confirmation and unrelated work may proceed
// (Store reads). The oracle is the completed CONNECT, one Will publication
// and its shared delivery, before a timer or unrelated packet can rescue it.
func (r *readyRun) sharedWill() {
	const filter, topic = "$share/will/b/#", "b/retained-will"
	member := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "a-member", ReceiveMaximum: 1})
	will := r.will(topic, 8)
	will.Retain = true
	owner := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "will-owner", SessionExpiryInterval: 300,
		ReceiveMaximum: 1, WillFlag: true, Will: will})
	keeper := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "z-keeper", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	r.settle()
	for _, c := range []*readyConn{member, owner, keeper} {
		r.subscription(c, filter, 1, false)
		r.settle()
	}
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "will-publisher", ReceiveMaximum: 1})
	r.settle()
	r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("b/assigned"), Payload: []byte("assignment")})
	r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver)
	if len(member.acks) != 1 || len(member.sent) != 1 || member.sent[0].topic != "b/assigned" {
		r.fail("will-shared: volatile member has no written assignment: sent %v, acknowledgements %v", member.sent, member.acks)
	}
	r.pauseRead = isAdmission
	replacement := r.openClient(&packet.Connect{Version: packet.V5, CleanStart: true, ClientID: "will-owner",
		SessionExpiryInterval: 300, ReceiveMaximum: 1})
	r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.stopped, r.closed, r.logged)
	if !slices.ContainsFunc(r.answers, func(a store.Answer) bool {
		return !a.View && a.Op == r.pausedRead
	}) {
		r.fail("will-shared: no retained-charge result outstanding before the member's acknowledgement")
	}
	r.note("retained Will charge read outstanding; acknowledge the volatile member's assignment")
	if !r.acknowledge() || len(member.acks) != 0 {
		r.fail("will-shared: volatile member did not acknowledge its assignment")
	}
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.pauseRead = nil
	r.pausedRead = nil
	r.settle()
	ack := connackIn(r.decisions(replacement.conn), replacement.conn)
	stored := r.count(`SELECT count(*) FROM records WHERE topic = 'b/retained-will'`)
	delivered := 0
	for _, c := range []*readyConn{member, keeper} {
		for _, d := range c.sent {
			if d.topic == topic {
				delivered++
			}
		}
	}
	if ack == nil || ack.ReasonCode != 0 || ack.SessionPresent || stored != 1 || delivered != 1 {
		r.fail("will-shared still waiting with every external obligation answered: CONNACK %+v, stored Wills %d/1, shared deliveries %d/1 (design.md, Ready work, 3; CONNECT, Store; Delivery, Positions)", ack, stored, delivered)
	}
	if n := r.count(`SELECT count(*) FROM share_returned`); n != 0 {
		r.fail("will-shared: %d shared evidence rows remain after all deliveries settled", n)
	}
}

// fullStore refuses a delivery save as one rolled-back group. The retry
// must be rediscovered one second after that answer, with no client work
// rescuing it (A full disk). It then mixes full responses into ordinary work.
func (r *readyRun) fullStore() {
	c := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "full-sub", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "full-pub", ReceiveMaximum: 1})
	r.settle()
	r.subscription(c, "recovery/#", 1, false)
	r.settle()
	refused := false
	r.refuse = func(g *core.Group) bool {
		if !refused && slices.ContainsFunc(g.Writes, func(w store.Write) bool { _, ok := w.(*store.DeliveryChanges); return ok }) {
			refused = true
			return true
		}
		return r.full
	}
	r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("recovery/a"), Payload: []byte("retry")})
	r.settle()
	if !refused {
		r.fail("full: no delivery save was refused")
	}
	r.until(r.now.Add(2 * time.Second))
	if n := r.count(`SELECT count(*) FROM session_inflight`); n != 0 || len(c.sent) != 1 || c.sent[0].topic != "recovery/a" {
		r.fail("full: retry left %d exchanges or sent %v; want one completed recovery/a delivery", n, c.sent)
	}
	for range 160 {
		r.full = r.rng.IntN(4) == 0
		r.step()
	}
	r.full = false
	r.quiet()
}

// latest drives the latest channel values through its transitions, the
// store answering and writers reporting in random orders. A durable
// subscriber with Receive Maximum 1 subscribes to values/# after a value
// and a tombstone are stored: its initial pass sends current values only
// (Channels, latest: "An empty payload deletes the value"). While its
// first delivery holds the window, values are replaced, waiting to be sent
// (Delivery, Order: "On latest, they follow offset order of changes and
// skip replaced values"). It leaves with that delivery unacknowledged,
// and every topic then changes, one deleted: its recovered exchange's
// value has been replaced, so it is "settled as if sent, and not counted"
// (Delivery), and "Returning consumers receive surviving tombstones as
// empty payloads" (Channels). Once every external obligation is answered,
// the returning connection must have received each topic's current value
// once, in offset order, as first deliveries, and the session must store
// no exchange (Ready work, 3: "Work must leave everything it waits for able
// to finish"); once the channel's retention_period and
// tombstone_retention_period have passed, retention must have deleted every
// record (Words: Retention: "retention deletes each record once it is due,
// wherever it sits"), before the broker is left nothing to do (quiet).
func (r *readyRun) latest() {
	sub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "latest-sub", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "latest-pub", ReceiveMaximum: 1})
	r.settle()
	// put has pub publish each topic and payload pair at QoS 1, the
	// store and writers proceeding in a random order, no subscriber
	// acknowledging.
	put := func(pairs ...string) {
		for i := 0; i < len(pairs); i += 2 {
			r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte(pairs[i]), Payload: []byte(pairs[i+1])})
			r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
		}
	}
	put("values/a", "a1", "values/b", "b1", "values/b", "")
	r.subscription(sub, "values/#", 1, false)
	r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
	put("values/a", "a2", "values/c", "c1", "values/a", "a3")
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.depart(sub, nil)
	r.settle()
	put("values/a", "a4", "values/b", "b2", "values/c", "")
	r.settle()

	back := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "latest-sub", SessionExpiryInterval: 300, ReceiveMaximum: uint16(1 + r.rng.IntN(3))})
	r.settle()
	want := []readySent{{topic: "values/a", body: "a4"}, {topic: "values/b", body: "b2"}, {topic: "values/c", body: ""}}
	got := make([]readySent, len(back.sent))
	for i, d := range back.sent {
		if d.dup || d.qos != 1 {
			r.fail("latest: the returning connection's delivery %d is %+v; want a first delivery at QoS 1, the replaced value's exchange settled as if sent (design.md, Delivery)", i, d)
		}
		got[i] = readySent{topic: d.topic, body: d.body}
	}
	remaining := r.count(`SELECT count(*) FROM session_inflight WHERE session = (SELECT id FROM sessions WHERE client = 'latest-sub')`)
	if !slices.Equal(got, want) || remaining != 0 {
		r.fail("latest still waiting with every external obligation answered: the returning connection received %v, want %v; stored exchanges %d, want 0 (design.md, Execution, Ready work, 3; Channels, latest; Delivery)",
			got, want, remaining)
	}
	r.until(r.now.Add(5 * time.Second))
	if n := r.recordsOn(r.channelID("values")); n != 0 {
		r.fail("latest: %d records left on values past retention_period and tombstone_retention_period, with every external obligation answered (design.md, Words: Retention)", n)
	}
	r.quiet()
}

// memoryPacket, memoryBudget and memoryInflight are the memory flows'
// limits.max_packet_size, each of max_incoming_memory and
// max_outgoing_memory, and max_inflight: the end-to-end fuzzer's
// (internal/harness/cmd/e2efuzz, model.go), a budget of under three
// packets, in which one body over 4 KiB, asking for room for its S2 copy
// too (design.md, Message memory, Asking), holds most of the incoming one.
const (
	memoryPacket   = 6 << 10
	memoryBudget   = 16 << 10
	memoryInflight = 4
)

// memory drives the incoming budget into the state of audit findings
// 261109-001a and 261111-003a, with the end-to-end fuzzer's limits
// (memoryPacket) and mixed message sizes, the store answering and writers
// reporting in random orders: a body waits for incoming room while a
// delivery is ready. design.md, Message memory, Two queues: "A head that
// waits holds back only its own FIFO: a delivery never waits for room in
// the incoming budget, and a body never waits for room in the outgoing
// one"; Delivery, Admission progress: "For the oldest delivery waiting for
// message memory, the core counts the outgoing reservations that cannot be
// released until peer input arrives or a writer finishes"; Capacities,
// Delivery admission: "The gate closes when the admission delay reaches
// delivery_admission_delay. Once closed, it opens when every contributing
// delay is below half that value, or no qualifying work remains."
//
// F subscribes to values/# at QoS 1 with a Receive Maximum of 1, and a
// publisher stores two values: F is sent the first, and the second, D, of
// a size drawn so that its delivery would not fit beside A's ask below in
// one budget, waits for F's window. A asks for a body over 4 KiB, its
// reader handing nothing over: its grant waits for A's input. X asks for a
// body that fits beside A's and hands it over, its write waiting at the
// store. Q asks for B's body, which fits beside A's but not beside A's and
// X's, so it waits. F acknowledges the first value; the store's reads and
// the writers go on, its write lane still waiting (stalled): F is sent D,
// its delivery reserving outgoing room while B's body waits for incoming
// room. Once delivery_admission_delay has passed, the core's timer fires,
// if due, and the write lane goes on. Every core call returns
// (readyWatchdog), the ignored packet after each leads to nothing (nudged),
// and once every external obligation is answered, Q's body has been given,
// before the broker is left nothing to do (quiet).
func (r *readyRun) memory() {
	f := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "memory-f", ReceiveMaximum: 1})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "memory-pub", ReceiveMaximum: 1})
	a := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "memory-a", ReceiveMaximum: 1})
	x := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "memory-x", ReceiveMaximum: 1})
	q := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "memory-q", ReceiveMaximum: 1})
	r.settle()
	r.subscription(f, "values/#", 1, false)
	r.settle()
	// A's body, near the packet limit, leaves free beside its ask; X's and
	// B's are within 4 KiB, asking for their own size alone.
	aBody := memoryPacket - 64 - r.rng.IntN(256)
	free := memoryBudget - int(payload.BodyMemory(aBody))
	xBody := free/4 + r.rng.IntN(free/4)
	bBody := free - xBody + 1 + r.rng.IntN(min(free, 4<<10)-(free-xBody))
	d := make([]byte, free+1+r.rng.IntN(memoryPacket-64-free))
	r.note("memory: max_incoming_memory %d, A's ask leaves %d, X's body %d, B's %d, D's payload %d", memoryBudget, free, xBody, bBody, len(d))
	for i, value := range [][]byte{[]byte("1"), d} {
		r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: fmt.Appendf(nil, "values/%d", i), Payload: value})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
	}
	if len(f.sent) != 1 || len(f.acks) != 1 {
		r.fail("driver: F was sent %d values and owes %d acknowledgements; want the first sent and D waiting for its window", len(f.sent), len(f.acks))
	}
	r.askPacket(a, sized(1, "events/a", aBody))
	r.askPacket(x, sized(1, "b/x", xBody))
	if r.granted[a.conn] != a.asked || r.granted[x.conn] != x.asked {
		r.fail("driver: A's ask of %d and X's of %d were given %d and %d; want both, A's leaving %d", a.asked, x.asked, r.granted[a.conn], r.granted[x.conn], free)
	}
	r.stalled = true
	r.note("store: the write lane waits")
	r.handOverOf(x)
	r.askPacket(q, sized(1, "b/y", bBody))
	if r.granted[q.conn] != 0 {
		r.fail("driver: B's ask of %d was given beside A's grant and X's body", q.asked)
	}
	r.acknowledgeOf(f)
	r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.logged)
	if len(f.sent) != 2 || r.granted[q.conn] != 0 {
		r.fail("memory: with the write lane waiting, F was sent %d values of 2 and B's ask given %d; want D sent, its delivery never waiting for incoming room, and B's body waiting (design.md, Message memory, Two queues)",
			len(f.sent), r.granted[q.conn])
	}
	r.now = r.now.Add(r.cfg.Broker.DeliveryAdmissionDelay)
	r.note("waits %v", r.cfg.Broker.DeliveryAdmissionDelay)
	for r.fire() {
		r.learn()
	}
	r.stalled = false
	r.note("store: the write lane goes on")
	r.settle()
	if q.asked != 0 || len(f.sent) != 2 {
		r.fail("memory still waiting with every external obligation answered: Q's reader waits for %d bytes, F was sent %d values of 2 (design.md, Execution, Ready work, 3; Delivery, Admission progress)",
			q.asked, len(f.sent))
	}
	r.quiet()
}

// sized is a PUBLISH at qos on topic whose body is n bytes, under packet
// identifier 1 at QoS 1 or 2.
func sized(qos byte, topic string, n int) *packet.Publish {
	p := &packet.Publish{QoS: qos, Topic: []byte(topic)}
	if qos != 0 {
		p.PacketID = 1
	}
	p.Payload = make([]byte, n-int(bodyOf(p)))
	return p
}

// memoryOrder changes message memory's incoming FIFO in every way it changes
// while a delivery is ready, the store answering and writers reporting in
// random orders, with the memory flows' limits (memoryPacket). design.md,
// Message memory, Two queues: "When the head of a FIFO does not fit in its
// budget, it waits with every younger request of that FIFO behind it, and
// no younger request of that FIFO passes it. A head that waits holds back
// only its own FIFO: a delivery never waits for room in the incoming
// budget"; "An ineligible incoming request does not stand in the way: the
// oldest incoming request whose body grant is not withheld is the incoming
// head"; "A CONNECT that must publish a Will gets that Will's reservation,
// from the incoming budget, and its batch slot before the core grants any
// body pipelined behind that CONNECT".
//
// W connects with a Will of no delay, which is stored. F subscribes to
// values/# at QoS 1 with a Receive Maximum of 1, and a publisher stores
// three values: F is sent the first, and the second, D, of a size drawn,
// and the third wait for F's window. A asks for a body over 4 KiB, its
// reader handing nothing over: its grant waits for A's input. E0 asks for a
// body that does not fit beside A's, so it waits at the head of the
// incoming FIFO; F acknowledges the first value, and is sent D, its
// delivery never waiting behind E0's body. Then, in a random order, the
// store and the writers going on between them: E1, E2 and E3 ask for
// bodies, behind E0's; C, a new client, connects and asks for a body while
// its session is read, so that the body waits for its CONNECT and is
// restored at its own place once read; W', for W's client id, takes W's
// session over and asks for a body the same way, so that W's Will, firing,
// takes that body's place before it; and, at a random point, the first
// body asked for of E1's, E2's, E3's and C's still waiting is cancelled,
// its connection lost. Once the store and the writers have caught up, none
// of them has been given its body, younger than E0's. A hands its body
// over and asks for a second as large; its first's storage gives the room
// back, E0's body is given, and the younger requests are given their bodies
// in turn, but for A's second, which fits neither beside theirs nor in the
// channel's batch bound beside the four slots they reserve (Two queues: "A
// granted body, unread or held behind its CONNECT, reserves one potential
// batch slot"; Communication: "While unresolved publication batches are at
// their bound, the core grants no publication body"). F acknowledges D, and
// is sent the third value, A's second still waiting. Once every external
// obligation is answered, every reader still connected has been given its
// body, before the broker is left nothing to do (quiet).
func (r *readyRun) memoryOrder() {
	const wID = "order-w"
	w := r.openClient(&packet.Connect{Version: packet.V5, ClientID: wID, SessionExpiryInterval: 300, ReceiveMaximum: 1,
		WillFlag: true, Will: r.will("b/will", 0)})
	f := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "order-f", ReceiveMaximum: 1})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "order-pub", ReceiveMaximum: 1})
	a := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "order-a", ReceiveMaximum: 1})
	es := make([]*readyConn, 4)
	for i := range es {
		es[i] = r.openClient(&packet.Connect{Version: packet.V5, ClientID: fmt.Sprintf("order-e%d", i), ReceiveMaximum: 1})
	}
	r.settle()
	if !w.connack {
		r.fail("driver: W's CONNECT, with its Will, was not answered")
	}
	r.subscription(f, "values/#", 1, false)
	r.settle()
	for i, value := range [][]byte{[]byte("1"), make([]byte, 1+r.rng.IntN(memoryPacket-128)), []byte("3")} {
		r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: fmt.Appendf(nil, "values/%d", i), Payload: value})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
	}
	if len(f.sent) != 1 || len(f.acks) != 1 {
		r.fail("driver: F was sent %d values and owes %d acknowledgements; want the first sent and D waiting for its window", len(f.sent), len(f.acks))
	}
	r.askPacket(a, sized(1, "events/a", memoryPacket-64-r.rng.IntN(256)))
	free := memoryBudget - int(a.asked)
	r.askPacket(es[0], sized(1, "b/x", free+1+r.rng.IntN(memoryPacket-64-free)))
	r.note("memory-order: max_incoming_memory %d, A's ask leaves %d, E0 asks %d", memoryBudget, free, es[0].asked)
	if r.granted[a.conn] != a.asked || r.granted[es[0].conn] != 0 {
		r.fail("driver: A's ask of %d and E0's of %d were given %d and %d; want A's alone", a.asked, es[0].asked, r.granted[a.conn], r.granted[es[0].conn])
	}
	// The store and the writers go on; no reader hands a body over, so A's
	// grant keeps waiting for its input, and no client acknowledges.
	catchUp := []func() bool{r.carryOut, r.answerOne, r.write, r.reportOne, r.stopped, r.closed, r.logged}
	r.acknowledgeOf(f)
	r.drain(catchUp...)
	if len(f.sent) != 2 || r.granted[es[0].conn] != 0 {
		r.fail("memory-order: F acknowledged the first value, and was sent %d values of 3, E0 given %d; want D sent, its delivery never waiting behind E0's body for incoming room (design.md, Message memory, Two queues)",
			len(f.sent), r.granted[es[0].conn])
	}
	// asking has c's reader ask for a QoS 0 body of 2 KiB to 4 KiB, which
	// its client may send before its CONNACK; cancellable are E1, E2, E3 and
	// C, in the order they asked.
	var cancellable []*readyConn
	asking := func(c *readyConn, cancel bool) {
		r.askPacket(c, sized(0, "b/y", 2<<10+r.rng.IntN(2<<10)))
		if cancel {
			cancellable = append(cancellable, c)
		}
	}
	moves := []func(){
		func() { asking(es[1], true) },
		func() { asking(es[2], true) },
		func() { asking(es[3], true) },
		func() {
			asking(r.openClient(&packet.Connect{Version: packet.V5, ClientID: "order-c", ReceiveMaximum: 1}), true)
		},
		func() {
			asking(r.openClient(&packet.Connect{Version: packet.V5, ClientID: wID, SessionExpiryInterval: 300, ReceiveMaximum: 1}), false)
		},
	}
	cancelled := false
	for n, i := range r.rng.Perm(len(moves)) {
		moves[i]()
		waiting := slices.DeleteFunc(slices.Clone(cancellable), func(e *readyConn) bool {
			return !e.live() || e.asked == 0 || r.granted[e.conn] != 0
		})
		if !cancelled && len(waiting) != 0 && (n == len(moves)-1 || r.rng.IntN(2) == 0) {
			r.depart(waiting[0], nil)
			cancelled = true
		}
		for range r.rng.IntN(4) {
			r.drainOneOf(catchUp...)
		}
	}
	r.drain(catchUp...)
	if !cancelled || r.granted[es[0].conn] != 0 {
		r.fail("driver: before A's input, cancelled %v, E0 given %d; want a body cancelled and E0's ask waiting", cancelled, r.granted[es[0].conn])
	}
	for _, e := range cancellable {
		if e.live() && e.asked != 0 && r.granted[e.conn] != 0 {
			r.fail("memory-order: before A's input, %s was given %d bytes behind E0's waiting ask; no younger request of the incoming FIFO passes its head (design.md, Message memory, Two queues)",
				e.name, r.granted[e.conn])
		}
	}
	r.handOverOf(a)
	r.askPacket(a, sized(0, "events/a", memoryPacket-64-r.rng.IntN(256)))
	r.drain(catchUp...)
	if r.granted[es[0].conn] != es[0].asked {
		r.fail("driver: with A's first body stored, E0's ask of %d was given %d; want all of it", es[0].asked, r.granted[es[0].conn])
	}
	r.handOverOf(es[0])
	r.drain(catchUp...)
	r.acknowledgeOf(f)
	r.drain(catchUp...)
	if len(f.sent) != 3 || a.asked == 0 || r.granted[a.conn] != 0 {
		r.fail("memory-order: F acknowledged D and was sent %d values of 3, A's second ask of %d given %d; want the third value sent, A's second still waiting for a batch slot (design.md, Message memory, Two queues)",
			len(f.sent), a.asked, r.granted[a.conn])
	}
	r.settle()
	for _, e := range r.conns {
		if e.live() && e.asked != 0 {
			r.fail("memory-order still waiting with every external obligation answered: %s's reader waits for %d bytes (design.md, Execution, Ready work, 3; Message memory)", e.name, e.asked)
		}
	}
	r.quiet()
}

// queueMoves drives a queue's jobs to its dead-letter channel, the store
// answering and writers reporting in random orders. A durable reader of the
// queue's own topics, with Receive Maximum 1, and a reader of its
// dead-letter channel subscribe, and five jobs of 200 bytes are published
// on tasks, neither reader acknowledging meanwhile: each commit past the
// queue's 700 bytes moves its oldest job (Channels, queue: "evicted when
// retention_bytes causes the move"; Words: Retention, "Every committed
// change that affects size schedules a size pass for its channel"). Once
// the queue's retention_period has passed, the jobs left move too
// ("expired when the job is past retention_period"). Once every external
// obligation is answered, the queue must hold no record and no unfinished
// job, its dead-letter channel every job, in offset order, on its topic
// with the prefix __dlq/ ("It moves the job to the append channel
// <channel>__dlq in one transaction. The topic gains the prefix __dlq/"),
// and the dead-letter reader must have received each once, as a first
// delivery (Ready work, 3: "Work must leave everything it waits for able
// to finish"), before the broker is left nothing to do (quiet).
func (r *readyRun) queueMoves() {
	reader := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "queue-reader", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	dead := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "queue-dead", SessionExpiryInterval: 300, ReceiveMaximum: uint16(1 + r.rng.IntN(3))})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "queue-pub", ReceiveMaximum: 1})
	r.settle()
	r.subscription(reader, "tasks/#", 1, false)
	r.subscription(dead, "__dlq/tasks/#", 1, false)
	r.settle()
	const jobs = 5
	want := make([]string, 0, jobs)
	for i := range jobs {
		topic := fmt.Sprintf("tasks/%d", i)
		r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte(topic), Payload: make([]byte, 200)})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
		want = append(want, "__dlq/"+topic)
	}
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.settle()
	r.until(r.now.Add(4 * time.Second))
	got := make([]string, len(dead.sent))
	for i, d := range dead.sent {
		if d.dup || d.qos != 1 {
			r.fail("queue: the dead-letter reader's delivery %d is %+v; want a first delivery at QoS 1", i, d)
		}
		got[i] = d.topic
	}
	queued, unfinished := r.recordsOn(r.channelID("tasks")), r.count(`SELECT count(*) FROM queue_jobs`)
	moved := r.topics(r.channelID("tasks__dlq"))
	if queued != 0 || unfinished != 0 || !slices.Equal(moved, want) || !slices.Equal(got, want) {
		r.fail("queue still waiting with every external obligation answered: tasks holds %d records and %d jobs, want none; tasks__dlq holds %q and its reader received %q, want %q (design.md, Execution, Ready work, 3; Channels, queue)",
			queued, unfinished, moved, got, want)
	}
	r.quiet()
}

// workers drives the queue's workers, the store answering and writers
// reporting in random orders. worker-a, holding a durable session, and
// worker-b, each with a small Receive Maximum, subscribe to
// $saguin/queue/tasks at QoS 1, and four jobs are published on tasks, each
// worker acknowledging every offer once it is written (Channels, queue: "A
// subscription to $saguin/queue/<channel> at QoS 1 makes a worker. Each job
// goes to one worker at a time"; Delivery, Queue jobs: "each worker receives
// one job at a time per queue. Workers are selected round-robin among those
// connected, with room"; "The hold starts at the worker's PUBACK"; "Jobs
// are offered oldest first"). Once every external obligation is answered, each worker must
// have received one offer, a first delivery at QoS 1, the two oldest jobs
// between them, and no second offer while it holds its job (Ready work, 3:
// "Work must leave everything it waits for able to finish"). worker-b then
// unsubscribes, keeping the job it holds (Flows, SUBSCRIBE, UNSUBSCRIBE: "A
// worker that unsubscribes keeps
// the job it holds until it answers, the hold ends or it goes. Its offers
// not yet sent are cancelled"): once every obligation is answered again,
// still no other offer has been written.
//
// worker-b, unsubscribed, then answers ack for the job it holds, and
// worker-a return for its own; afterwards, round after round, worker-a
// answers each offer it has received with ack or return, or lets its hold
// run out, and time passes to random moments, the queue's deadlines firing
// on the way ($saguin topics: "$saguin/queue/<channel>/response | ack or
// return a job, with its attempt's Correlation Data"; Delivery, Queue jobs:
// "The hold starts at the worker's PUBACK and lasts visibility_timeout";
// "A failed attempt is offered again after a wait set by retry.mode"; "When
// an attempt fails with retry.max_attempts spent, the job moves to the
// dead-letter channel as attempts_exhausted"; Channels, queue: a job past
// retention_period moves as expired). In each round where worker-a answered
// every offer, once every external obligation is answered, either a new
// offer has been written to it or no job is ready (metrics.md, Queues:
// saguin_queue_jobs, "ready (can be offered)"): a job worker-a, free, could
// take is never left waiting (Ready work, 3). Within readyRounds rounds no
// job may be left unfinished, each one either acknowledged once or on the
// dead-letter channel (metrics.md, Queues: saguin_queue_acknowledged_total,
// "ack responses"; saguin_queue_stale_responses_total counts the others).
// The broker is then left nothing to do (quiet).
func (r *readyRun) workers() {
	a := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "worker-a", SessionExpiryInterval: 300, ReceiveMaximum: uint16(1 + r.rng.IntN(3))})
	b := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "worker-b", ReceiveMaximum: uint16(1 + r.rng.IntN(3))})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "worker-pub", ReceiveMaximum: 1})
	r.settle()
	const filter = "$saguin/queue/tasks"
	r.subscription(a, filter, 1, false)
	r.subscription(b, filter, 1, false)
	r.settle()
	const jobs = 4
	for i := range jobs {
		r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: fmt.Appendf(nil, "tasks/%d", i), Payload: []byte("job")})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.handOver, r.release, r.logged)
	}
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.settle()
	// offered fails the run unless each worker has received one offer, a
	// first delivery at QoS 1, the two of them tasks/0 and tasks/1.
	offered := func(when string) {
		got := make([]string, 0, 2)
		for _, w := range []*readyConn{a, b} {
			if len(w.sent) != 1 || w.sent[0].dup || w.sent[0].qos != 1 {
				r.fail("workers %s, with every external obligation answered: %s received %+v; want one offer, a first delivery at QoS 1 (design.md, Execution, Ready work, 3; Delivery, Queue jobs)", when, w.name, w.sent)
			}
			got = append(got, w.sent[0].topic)
		}
		slices.Sort(got)
		if !slices.Equal(got, []string{"tasks/0", "tasks/1"}) {
			r.fail("workers %s: the workers received %q; want tasks/0 and tasks/1, the oldest jobs, one each (Delivery, Queue jobs)", when, got)
		}
	}
	offered("once every job is published")
	r.subscription(b, filter, 1, true)
	for range r.rng.IntN(8) {
		r.drainOne()
	}
	r.settle()
	offered("once worker-b has unsubscribed, keeping its job")

	r.respond(b, b.sent[0], "ack")
	r.respond(a, a.sent[0], "return")
	visibility := r.cfg.Channels[slices.IndexFunc(r.cfg.Channels, func(ch config.Channel) bool { return ch.Name == "tasks" })].VisibilityTimeout
	// busy is when the last hold worker-a was left to run out has surely
	// ended: every offer it was given was acknowledged before its round.
	var busy time.Time
	for answered, round := 1, 0; r.count(`SELECT count(*) FROM queue_jobs`) != 0; round++ {
		if round == readyRounds {
			r.fail("workers: %d jobs still unfinished after %d rounds of answers and deadlines, with every external obligation answered (design.md, Execution, Ready work, 3; Delivery, Queue jobs)", r.count(`SELECT count(*) FROM queue_jobs`), round)
		}
		given := len(a.sent)
		for _, offer := range a.sent[answered:given] {
			switch r.rng.IntN(3) {
			case 0:
				r.respond(a, offer, "ack")
			case 1:
				r.respond(a, offer, "return")
			default:
				busy = r.now.Add(visibility) // its hold runs out
			}
		}
		r.settle()
		if len(a.sent) == given && !r.now.Before(busy) {
			if n := testkit.ParseScrape(r.t, testkit.Scrape(r.t, r.reg)).Value(r.t, "saguin_queue_jobs", "channel", "tasks", "state", "ready"); n != 0 {
				r.fail("workers: %v jobs ready while worker-a, holding none, is given none, with every external obligation answered (design.md, Execution, Ready work, 3; Delivery, Queue jobs)", n)
			}
		}
		answered = given
		r.until(r.now.Add(time.Duration(1+r.rng.IntN(1200)) * time.Millisecond))
	}
	acked := testkit.Sample(r.t, r.reg, `saguin_queue_acknowledged_total{channel="tasks"}`)
	if dead := r.recordsOn(r.channelID("tasks__dlq")); int(acked)+dead != jobs {
		r.fail("workers: with no job unfinished, %v jobs were acknowledged and %d dead lettered; want %d in all, each job ending once (Delivery, Queue jobs; Channels, queue)", acked, dead, jobs)
	}
	r.quiet()
}

// readyRounds bounds the rounds of the workers flow's answers: the queue's
// retention_period ends every job long before.
const readyRounds = 64

// respond has c's reader hand over a QoS 1 response on tasks of answer
// naming offer's attempt as its Correlation Data, then plays every step that
// answers what it leads to ($saguin topics: "$saguin/queue/<channel>/response
// | ack or return a job, with its attempt's Correlation Data").
func (r *readyRun) respond(c *readyConn, offer readySent, answer string) {
	if offer.attempt == "" {
		r.fail("workers: %s's offer %+v carries no Correlation Data; want its attempt's id (design.md, Words: Attempt)", c.name, offer)
	}
	r.note("%s: %s for %s", c.name, answer, offer.topic)
	r.askPacket(c, &packet.Publish{QoS: 1, PacketID: c.freeID(), Topic: []byte("$saguin/queue/tasks/response"), Payload: []byte(answer)},
		packet.Property{ID: packet.CorrelationData, Binary: []byte(offer.attempt)})
	r.settle()
}

// lifecycle stops with reads, writes, replies or granted bodies still in
// flight. Only actual endpoint reports may finish the stop. Restart reuses
// liveScript's store lifecycle and randomizes its startup answers before Serve.
func (r *readyRun) lifecycle() {
	c := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "restart-due", SessionExpiryInterval: 2,
		ReceiveMaximum: 1, WillFlag: true, Will: r.will("recovery/restart", 8)})
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "restart-pub", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	r.settle()
	r.depart(c, nil)
	r.settle()
	r.subscription(pub, "events/#", 2, false)
	r.askPacket(pub, &packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("events/a"), Payload: []byte("stop")})
	for range r.rng.IntN(18) {
		r.drainOne()
	}
	r.note("graceful stop")
	r.take(r.core.BeginStop(nil, r.now))
	// The probe's readers now end and its writers consume their final entries.
	// No further ignored packet is legal after those readers have ended.
	r.ghost = nil
	for _, conn := range r.ghosts {
		r.event(connection.Event{Kind: connection.Ended, Conn: conn})
		r.event(connection.Event{Kind: connection.Closed, Conn: conn})
	}
	if r.seed%3 == 0 {
		// The writers have not completed their pending entries when the
		// stop's one cutoff arrives; a cut cannot produce successful writes.
		r.now = r.now.Add(r.cfg.Broker.Limits.WriteTimeout)
		r.fire()
	}
	r.settle()
	// Run owns the final empty-inbound transition, not Handle. Exercise it
	// with the existing loop helper, as the other stop tests do.
	l := newLoop(r.t)
	l.run(r.core)
	l.decide(r.now)
	select {
	case <-r.core.Drained():
	default:
		r.fail("shutdown: every external obligation answered, but Drained is still open; connections %d, requests %d, answers %d", len(r.conns), len(r.queue), len(r.answers))
	}
	l.end()
	if len(r.queue) != 0 || len(r.answers) != 0 || len(r.conns) != 0 {
		r.fail("shutdown: Drained before all requests and connections ended")
	}
	r.startup = func() { r.drain(r.carryOut, r.answerOne) }
	r.note("startup after five seconds down")
	r.stop()
	time.Sleep(time.Until(r.now.Add(5 * time.Second)))
	r.restart(time.Now())
	if n := r.count(`SELECT count(*) FROM sessions WHERE client = 'restart-due'`); n != 0 {
		r.fail("startup: expired session still stored after startup completed")
	}
	if n := r.count(`SELECT count(*) FROM records WHERE topic = 'recovery/restart'`); n != 1 {
		r.fail("startup: due Will published %d times, want once", n)
	}
	r.answer = nil
	r.probe()
	r.answer = func(a store.Answer) []core.Action { r.answers = append(r.answers, a); return nil }
	r.nudge = r.nudged
	r.openClient(&packet.Connect{Version: packet.V5, ClientID: "restart-pub", SessionExpiryInterval: 300, ReceiveMaximum: 1})
	r.settle()
	r.quiet()
}

// logged completes the next log write. It returns only its admission credit,
// exactly as the logger's ordered result channel does (design.md, Logging).
func (r *readyRun) logged() bool {
	if r.logs == 0 {
		return false
	}
	r.logs--
	r.core.Logged(logger.Result{})
	return true
}

// seekIndex is the routing index of the channel topic seeks, as the reader
// hands it to the core with the request.
func seekIndex(t *testing.T, cfg *config.Config, topic string) int {
	t.Helper()
	index, ok := config.NewRouting(cfg.Channels).SeekChannel(topic, false)
	if !ok {
		t.Fatalf("%s names no seek", topic)
	}
	return index
}

// eventsSeek is the seek of eventsChannel ($saguin topics).
const eventsSeek = "$saguin/consumer/events/seek"

// design.md, Store, Requests: "Applying a predecessor's result schedules
// its dependent request. Other requirements may still make it wait
// ([Ready work](#ready-work))"; "A request waiting for its predecessor holds back
// only its own owner's later requests"; Execution, Ready work, 1: "Each
// change that allows work to continue schedules it immediately: applying a
// store answer, settling a delivery, returning capacity or ending a
// request"; 2: "The event's actions are returned before the next outside
// event is taken".
//
// A durable client subscribed to events at QoS 1 is sent a record, whose
// exchange the core saves before the hand-off (Delivery, Positions); that
// write is held at the store, so the client's consumer is busy with it.
// The client then seeks twice on events: first malformed, which needs no
// store request and is answered at once when it runs, then to offset 0,
// which reads its position from the store. Neither runs while the
// consumer's write is outstanding. When the store answers that write, the
// first seek runs and ends in that event, ending its request, which makes
// the second its session's next: the second seek's read goes out in the
// same event's actions, with no further input. From the wire this cannot
// be told apart: the same answer hands the record to the writer, whose
// report is the next event (Testing: "orderings no client can force").
func TestADependentRequestIsTakenUpInTheSameEvent(t *testing.T) {
	s := newLiveScript(t, eventsChannel)
	sub := s.connect("sub", 300, 10)
	s.subscribe(sub, "events/#")
	pub := s.connect("pub", 0, 10)
	s.hold = func(g *core.Group) bool {
		return slices.ContainsFunc(g.Writes, func(w store.Write) bool { _, ok := w.(*store.DeliveryChanges); return ok })
	}
	s.publish(pub, 1, "events/1")
	s.serve()
	if len(s.held) != 1 {
		t.Fatalf("a record published to a durable QoS 1 subscriber led to %d held groups saving delivery changes; want 1, its exchange saved before the hand-off", len(s.held))
	}
	index := seekIndex(t, s.cfg, eventsSeek)
	for _, q := range []struct {
		id      uint16
		payload string
	}{{1, "x"}, {2, "0"}} {
		p := packet.Publish{QoS: 1, PacketID: q.id, Topic: []byte(eventsSeek), Payload: []byte(q.payload)}
		s.hand(sub, bodyOf(&p), &connection.RequestPublish{Publish: p, Channel: index})
		s.serve()
	}
	for _, r := range s.replies[sub] {
		if a, ok := r.(*connection.Ack); ok && a.Type == packet.PUBACK {
			t.Fatalf("seek %d was acknowledged (%s) while the consumer's write is outstanding; want both seeks waiting", a.PacketID, show(r))
		}
	}
	g := s.held[0]
	s.held = s.held[1:]
	acts := s.commit(g)
	if !slices.ContainsFunc(acts, func(a core.Action) bool { _, ok := a.Read.(*store.ReadSeekPosition); return ok }) {
		t.Errorf("the answer to the consumer's write, which let the malformed seek run and end, led to %s; want the second seek's read of its position among them, taken up in the same event", describe(acts))
	}
	s.hold = nil
	s.serve()
}

// kept plays a durable session leaving the recovery channel, which no
// retention touches, with deliveries in flight and prepared, and coming
// back to it: design.md, Flows, SUBSCRIBE, UNSUBSCRIBE, "When the last
// subscription reaching an append channel or a queue is removed, a durable
// session keeps its position there. If a delivery was prepared but never
// handed to the writer, the core first sets the position back to the
// earliest such delivery's offset, so no record is skipped"; "Exchanges
// already sent finish as before [...] Their completion never moves the kept
// position, even after another subscription reaches the channel"; "The
// next subscription reaching the channel reads from the kept position, so
// a record at or beyond it may be delivered again"; Execution, Resource
// bounds: "A position no subscription reaches has no scan or page of its
// own, and never holds publishers back".
//
// keeper subscribes to recovery/k at QoS 1 or 2, and a publisher stores
// three to six records there at that QoS, which keeper does not
// acknowledge, its small window holding some in flight and some prepared.
// It acknowledges a random few, then unsubscribes; its other
// acknowledgements come in a random order with the store's work on the
// UNSUBSCRIBE's transaction, or, in half the runs, keeper leaves a few
// steps after its UNSUBACK, keeping its session, and resumes, its
// exchanges resent. Once every obligation is answered, the session has no
// stored exchange and a kept position on recovery, below which every
// record was written to it.
// The publisher stores one more record, and while no subscription reaches
// the channel, the store carries out no read of recovery's records. keeper
// subscribes again: its first deliveries, DUP 0, are every record from the
// kept position on, in offset order, and its position then reaches the
// tail. The broker is then left nothing to do (quiet).
func (r *readyRun) kept() {
	const topic = "recovery/k"
	connect := func() *readyConn {
		return r.openClient(&packet.Connect{Version: packet.V5, ClientID: "kept-reader", SessionExpiryInterval: 300, ReceiveMaximum: uint16(1 + r.rng.IntN(3))})
	}
	keeper := connect()
	pub := r.openClient(&packet.Connect{Version: packet.V5, ClientID: "kept-pub", ReceiveMaximum: 1})
	r.settle()
	qos := byte(1 + r.rng.IntN(2))
	r.subscription(keeper, topic, qos, false)
	r.settle()
	publish := func(body string) {
		r.askPacket(pub, &packet.Publish{QoS: qos, PacketID: pub.freeID(), Topic: []byte(topic), Payload: []byte(body)})
		r.drain(r.carryOut, r.answerOne, r.write, r.reportOne, r.release, r.handOver)
	}
	for i := range 3 + r.rng.IntN(4) {
		publish(fmt.Sprintf("k%d", i+1))
	}
	for range r.rng.IntN(len(keeper.acks) + 1) {
		if len(keeper.acks) == 0 || !keeper.reading() {
			break
		}
		r.acknowledgeOf(keeper)
		r.drainOne()
	}
	written := map[string]bool{} // what keeper's connections were written, by body
	r.subscription(keeper, topic, qos, true)
	if r.rng.IntN(2) == 0 {
		// keeper leaves once the UNSUBSCRIBE is stored, its UNSUBACK handed
		// to the writer, some acknowledgements still owed.
		for steps := 0; !slices.ContainsFunc(r.replies[keeper.conn], func(p connection.Reply) bool { _, ok := p.(*packet.Unsuback); return ok }); steps++ {
			if steps > liveLimit {
				r.fail("kept: no UNSUBACK for keeper after %d steps of the store's and writers' work", liveLimit)
			}
			r.drainOne()
		}
		for range r.rng.IntN(8) {
			r.drainOne()
		}
		var p any
		if keeper.reading() {
			p = disconnectWith(0, nil)
		}
		r.depart(keeper, p)
		r.settle()
		for _, d := range keeper.sent {
			written[d.body] = true
		}
		keeper = connect()
		r.settle()
		if ack := connackIn(r.decisions(keeper.conn), keeper.conn); ack == nil || ack.ReasonCode != 0 || !ack.SessionPresent {
			r.fail("kept: keeper's resuming CONNECT got %+v; want a successful CONNACK with Session Present", ack)
		}
	}
	r.settle()
	const session = `(SELECT id FROM sessions WHERE client = 'kept-reader')`
	if n := r.count(`SELECT count(*) FROM session_inflight WHERE session = ` + session); n != 0 {
		r.fail("kept: %d exchanges still stored with every acknowledgement answered (design.md, UNSUBSCRIBE: \"Exchanges already sent finish as before\")", n)
	}
	at, ok := r.positionOf("kept-reader", "recovery")
	if !ok {
		r.fail("kept: no position on recovery once the last subscription reaching it was removed (design.md, UNSUBSCRIBE: \"a durable session keeps its position there\")")
	}
	for _, d := range keeper.sent {
		written[d.body] = true
	}

	channel := r.channelID("recovery")
	var scans []string
	r.reads = func(op store.Read) {
		if m, ok := op.(*store.ReadRecordMetadata); ok && m.Channel == channel {
			scans = append(scans, fmt.Sprintf("%+v", *m))
		}
	}
	publish("k-unsubscribed")
	r.settle()
	r.reads = nil
	if len(scans) != 0 {
		r.fail("kept: with no subscription reaching recovery, the store read its records %v (design.md, Resource bounds: \"A position no subscription reaches has no scan or page of its own\")", scans)
	}

	before := len(keeper.sent)
	r.subscription(keeper, topic, qos, false)
	r.settle()
	got := make([]string, 0, len(keeper.sent)-before)
	var want []string
	for _, d := range keeper.sent[before:] {
		got = append(got, fmt.Sprintf("%s DUP %v", d.body, d.dup))
	}
	rows, err := r.db.QueryContext(context.Background(), `SELECT "offset", payload FROM records WHERE channel = ? ORDER BY "offset"`, channel)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }() // read whole, or the run has failed
	tail := int64(0)
	for rows.Next() {
		var offset int64
		var body []byte
		if err := rows.Scan(&offset, &body); err != nil {
			r.t.Fatal(err)
		}
		if strings.HasPrefix(string(body), "k") && offset >= at {
			want = append(want, string(body)+" DUP false")
		} else if strings.HasPrefix(string(body), "k") && !written[string(body)] {
			r.fail("kept: %s, at offset %d below the kept position %d, was never written to keeper: skipped (design.md, UNSUBSCRIBE: \"so no record is skipped\")", body, offset, at)
		}
		tail = offset + 1
	}
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		r.fail("kept: after the SUBSCRIBE, keeper received %q; want %q, every record from the kept position %d, in offset order (design.md, UNSUBSCRIBE: \"The next subscription reaching the channel reads from the kept position\")", got, want, at)
	}
	if position, _ := r.positionOf("kept-reader", "recovery"); position != tail {
		r.fail("kept: with every delivery acknowledged, keeper's position on recovery is %d; want %d, the tail", position, tail)
	}
	r.quiet()
}
