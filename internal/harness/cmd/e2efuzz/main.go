// Command e2efuzz fuzzes the broker end to end from the wire (design.md,
// Testing: "The broker as a whole is fuzzed end to end from the wire, with
// many clients acting at random. Every answer is checked against MQTT and
// this document. Each run's seed is recorded so a failure can be rerun with
// the same steps").
//
// One driver takes the clients' actions one at a time, in the order a
// generator seeded with -seed draws them, and waits for each answer the
// model predicts before the next; concurrency under load is the soak's, but
// for the window action's clients, which publish together.
// Each client connects with random MQTT 5 or 3.1.1 settings, pings,
// publishes, subscribes, unsubscribes, receives, releases or withholds
// PUBACKs, disconnects, drops its connection, or is taken over. Sessions
// keep their subscriptions when a reconnect keeps the session. A QoS 2
// PUBLISH is held until its PUBREL (design.md, PUBLISH): its PUBREC is
// checked, a record of it before its PUBREL is an error, a PUBLISH repeating
// its identifier gets PUBREC again and is never stored twice, its PUBCOMP
// makes it owed to the database, and a PUBREL for an identifier no exchange
// holds gets PUBCOMP 0x92. A durable session keeps its exchanges across
// reconnects, which resend the PUBLISHes and PUBRELs left unanswered
// (MQTT-4.4.0-1); a session ending with its connection drops them. Held
// exchanges count against Receive Maximum. A subscriber answers each QoS 2
// delivery with PUBREC, in MQTT 5 every other one refusing, which settles
// it (design.md, Delivery, Positions), and each PUBREL with PUBCOMP, half of
// those it took keeping their PUBCOMP until it next releases its
// acknowledgements, so that a connection may end at either phase. Each
// PUBREL is checked: 0x00 after its PUBREC 0x00 and never after a refusing
// one, sent again on each later connection until its PUBCOMP and never
// replaced there by its PUBLISH (MQTT-4.3.3-6), each one owed coming by the
// next checkpoint; a PUBREC repeated after one 0x00 gets a second PUBREL
// 0x00, and a PUBREC naming no delivery gets PUBREL 0x92 (MQTT 3.1.1:
// PUBREL) before its PINGRESP. Malformed
// packets, silent connections and half CONNECTs exercise refusals and closes.
// The broker the run starts has two listeners, one speaking TLS with a
// certificate the run makes (design.md, Transport setup: "A listener with
// tls speaks only TLS"): every other connection the run opens goes to it,
// its handshake done first, and every check holds there as on the other;
// a broker given with -addr is driven over plain TCP alone.
// At each checkpoint, a broker the run started is also called on its
// operations listeners, plain TCP, TLS and a unix socket behind a PROXY
// header, with certificates, PROXY names, HTTP Basic credentials and Hosts
// drawn against its operators' file, each status checked against the
// design's order: route, credential and scope, method (operations.go).
// Every CONNECT logs in as a user of the password file the run writes
// (model.go); one with a wrong password is refused with 0x86 (MQTT 3.1.1:
// 4), leaving its client id's session as it was.
// The acl_file the run writes beside it (model.go, writeACL) lets three
// users publish and subscribe everywhere, and gives bo, which a quarter of
// the clients log in as and no other, rights per topic (rights): some
// topics it may write, some read, some neither. It denies two users
// features: cy persistent and will, di retained, share and qos2. Their
// CONNACKs carry what is withheld (Retain Available and Shared Subscription
// Available 0), a Session Expiry Interval of 0 where one above it was
// asked, and keep no session; their Wills are refused with 0x87 or 0x9A
// where the design says (design.md, Access and reload; Flows, CONNECT). A
// PUBLISH on a topic its user may not write is refused with 0x87 (dropped
// at QoS 0; an MQTT 3.1.1 connection closed at QoS 1), a retained one from
// di ends the connection with 0x9A, a filter under which its user may read
// nothing is refused with 0x87 (MQTT 3.1.1: 0x80), a QoS 2 one of di's is
// granted QoS 1, and a delivery of a topic its subscriber's user may not
// read is an error (design.md, PUBLISH; SUBSCRIBE; DISCONNECT); di's
// CONNACK carries Maximum QoS 1, its QoS 2 PUBLISH ends the connection with
// 0x9B, a held QoS 2 PUBLISH its resumed session resends included, and its
// QoS 2 Will is refused with 0x9B.
// The acl_file also gives ann publish limits (pacing.go): nothing it
// publishes is refused or reordered for them, and on a broker the run
// starts each answer to its PUBLISHes comes no sooner than the limits let
// its reader read the PUBLISH, and some reader must have waited for its
// allowance by the run's end.
// CONNECT and PUBLISH answers are predicted from the design, including
// concurrent publication windows at Receive Maximum and floods whose bodies
// together exceed max_incoming_memory, which they fill. Random SUBSCRIBEs use QoS, No Local, Retain
// As Published and Subscription Identifiers, with Retain Handling 2 in
// MQTT 5 but on a filter reaching the latest channel and not $broadcast,
// and filters and properties provoking the SUBSCRIBE refusal table.
// Every SUBACK is checked against that table in filter order.
// Every delivery is checked for a subscription held at publication, its
// QoS (a first transmission's the lower of the publication's and the
// highest contributing grant), one QoS 1 or 2 copy per connection unless
// DUP, publisher order, No Local, Retain
// As Published and the contributing Subscription Identifiers. Subscription
// revisions permit in-flight messages to finish after unsubscribe or option
// replacement. MQTT 3.1.1 RETAIN 1 deliveries only have their filter match
// checked, including when those retained exchanges resume.
// Every 200 steps, and at the end, a fresh client connects and pings, and
// subscribers release withheld PUBACKs and, under a watchdog, must receive
// every acknowledged QoS 1 or 2 publication whose PUBACK or PUBCOMP followed their SUBACK
// and whose connection and subscription continued to the checkpoint.
// Packets exceeding the subscriber's Maximum Packet Size are exempt.
// A third of CONNECTs carry a Will, with no Will Delay: published at once
// when its accepted connection ends any way but a normal DISCONNECT (a
// drop, a protocol error, the broker's close, MQTT 5 DISCONNECT 0x04 or
// any other reason code but 0x00), it is a record and a delivery like any
// other, checked as they are, its record also for its payload, its
// properties without the Will Delay and its Message Expiry counted from
// publication; discarded, refused or still connected, it has none. After
// SIGTERM every Will published is in the database, and none of a
// connection open at the stop. A takeover of a connection holding a Will
// is not drawn (wills.go).
// The broker holds configured append channels overlapping one another and
// $broadcast, one through {a,b} and one with a small max_bytes
// (channels.go); a third of topics, and some Wills, are drawn on them. The
// model routes each publication and Will by the design's most-exact rule
// and checks each record's channel, RETAIN ignored there; a publication is
// refused with 0x97 where the bytes already committed leave it no room,
// accepted where every publication sent to the channel fits, either
// otherwise, and the channel's records never pass its max_bytes.
// Subscribers read the channels from one position each per channel, at
// the floor or the tail, every filter reading from it (appends.go): each
// delivery comes through a subscription held while its position lasted, in
// the channel's offset order, RETAIN 0, with the Sagüin properties in MQTT
// 5 (those of $broadcast carry none) and its saguin-filter partition
// selecting it by topic_hash; a position made at the floor on the
// subscriber's connection owes its channel's history by the next
// checkpoint, and one copy holds as on $broadcast.
// One channel has a small retention_bytes (retention.go): what its size
// passes delete is held to the design's rules, oldest first and only
// beyond the limit, and a record they deleted is no longer owed.
// The records stored since the last check are read from the database:
// every message acknowledged 0x00 is there, unless retention deleted it,
// with its topic, payload and publisher, and no refused one is.
// Clients seek on the append channels (seeks.go), at every QoS, with and
// without a Response Topic and Correlation Data: each PUBACK or PUBREC is
// checked, and each answer, which comes only to the asker, on its Response
// Topic or else on the seek's reply topic, with its Correlation Data: the
// floor's offset for every form reaching it, malformed, beyond-end with
// saguin-next, no-session, none for a refusal at QoS 0 or one by the
// reader (0x87 without seek, 0x90 for a seek topic over max_topic_length),
// and none over the client's Maximum Packet Size. A seek's replay may
// repeat what its session received or had in flight, out of order; what it
// had prepared is cancelled. A PUBLISH under $saguin/ naming no request gets
// 0x90 whatever the acl_file. SUBSCRIBEs of $saguin/
// filters are refused with 0x87 (MQTT 3.1.1: 0x80). Clients subscribe to
// and unsubscribe from $share filters (shares.go), a second connected MQTT 5
// client often joining a group at once: each SUBACK is checked,
// or the DISCONNECT 0x9E or 0x82 that ends the connection instead, and
// each shared delivery against its member's subscription, each message
// going to one member, a second only where the first's session ended with
// it unacknowledged at QoS 1. The broker holds a latest channel, kv
// (latest.go): clients replace its values, MQTT 5 clients delete them with
// an empty payload, subscriptions to it take its current values as Retain
// Handling allows (any, in MQTT 5, on a filter not reaching $broadcast),
// and durable sessions come back to it. Its records are read whole at each
// check: one per topic, flagged retained, never a value surely replaced,
// and one for every topic a change of which was acknowledged. Each
// delivery from it is checked as a channel's, a pass's value one that may
// have been current at its SUBSCRIBE and never a tombstone, a tombstone
// known by its saguin-id or, over MQTT 3.1.1, by its topic; a returning
// session gets no change replaced while it was away, nor the resend of a
// replaced value. At each checkpoint a subscriber is owed each topic's
// current change alone: through a pass of its connection, or a
// subscription held since before it, a change made while its durable
// session was away included. The broker holds a queue, q, and its
// dead-letter channel (queues.go): clients publish jobs at every QoS, read
// the queue and its dead letters, seek on the queue, become workers and
// answer each offer with a PUBACK, refusing or not, ack, return or another
// payload, at any QoS or never, unsubscribe and go while holding jobs, and
// send responses naming attempts not theirs. Each offer is checked for its
// attempt id, properties and Subscription Identifier, a resend for being
// the same message, each job for going to one worker at a time, each
// worker for holding one job at a time, a new attempt for waiting out
// retry.mode, and each record check's read for every job the run stored
// being unfinished, dead-lettered once or finished by an ack of a worker it
// went to, never two of these; each dead letter for its topic, saguin-id,
// reason by its age at the move and the attempts its workers could have
// spent, and at the run's end a reader of the dead letters gets them all,
// bracketing the dead-letter counters of a broker the run started. With
// no -addr the run starts the
// broker (make's bin/saguin), reloads it now and then by SIGUSR1, its
// password file and acl_file changed: a user's password replaced, bo's
// write on topics of three levels taken away or given back, and both old
// files kept where the acl_file is broken (reload.go). It gives that
// broker a bridge to a peer the run serves, which checks the bridge's
// CONNECT and SUBSCRIBEs, drops the link now and then, refuses filters,
// and wants a redial at once, the refused filters asked for again alone
// after the redial wait, and DISCONNECT 0x00 at SIGTERM (bridges.go); and
// takes the records its clients publish for the bridge's out rules, each
// checked as it crosses, refused now and then, every one owed reaching it
// by each checkpoint, in each rule's order (crossings.go). It
// reads its database, and ends it with
// SIGTERM, which must give 0x8B to each MQTT 5 client and exit 0 within the
// graceful stop's bound. That broker keeps its store in the mode -mode
// names, or in one the seed draws (config.md, broker.storage, mode: "full,
// normal or memory"): full; normal, at sync_interval's least; or memory,
// its memory_dir a folder it creates in /dev/shm, the folder of
// config.md's default memory_dir. Every action and check is the same in
// each. The checks read the database the broker works on, in memory mode
// its RAM copy (design.md, Store: "runs on the RAM copy"). A memory-mode
// run's broker, once stopped, is started again on its files and stopped
// again (restart), and the last checks read file_path.
//
// Left out: full delivery prediction, reconnect-resume correctness but a
// latest channel's, retained values and Retain Handling but a latest
// channel's, a seek on a latest channel, latest retention (its periods a
// day), which worker the round-robin offers a job to, a dead letter's age
// for its channel's retention, the queue's job-state gauges, a queue's
// records owed to its readers, which member of a group receives a message
// and whether one does, the $saguin reply subscription, a seek's replay
// owed, seeks past records owed, a Will Delay above 0, retained.max_bytes,
// an MQTT 3.1.1 QoS 1 PUBLISH to a channel that may be full; and anything
// that waits on a clock (connect_timeout, keep-alive expiry, write_timeout) and filling max_connections, which exact wire
// tests cover; the configuration (model.go) keeps the run from reaching
// them, max_connections being the connections the run opens at once.
// Every PUBLISH body asks for its bytes (design.md, Message memory).
//
// Each run prints its seed first; a failure prints the seed, the replay
// flags and the last steps. The same seed and -clients give the same steps,
// whatever -duration.
//
//	go run ./internal/harness/cmd/e2efuzz [-addr host:port [-db file]] [-seed n] [-mode full|normal|memory] [-clients 16] [-duration 30s] [-steps n] [-v]
package main

import (
	"cmp"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// defaultClients is how many clients act when -clients is not given.
const defaultClients = 16

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the whole command, apart from os.Exit, so tests can call it.
func run(args []string, stdout, stderr io.Writer) (code int) {
	flags := flag.NewFlagSet("e2efuzz", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", "", "the MQTT address of a broker configured as this runner configures its own; empty starts -binary")
	binary := flags.String("binary", "bin/saguin", "the broker binary started when -addr is empty")
	dbPath := flags.String("db", "", "with -addr, the broker's database, holding no records but the run's, whose records are checked; empty checks none")
	seed := flags.Uint64("seed", 0, "the generator's seed; a random one when not given")
	mode := flags.String("mode", "", "the store's mode in the broker the run starts: full, normal or memory; empty for one the seed draws")
	clients := flags.Int("clients", defaultClients, "how many clients act")
	duration := flags.Duration("duration", 30*time.Second, "how long the steps go on")
	steps := flags.Int("steps", 0, "how many steps to take; -duration is a watchdog that fails the run if fewer finish; 0 for no count")
	timeout := flags.Duration("timeout", harness.DefaultTimeout, "the longest wait for one answer")
	wirePath := flags.String("wire", "", "write timestamped raw TCP I/O to this file")
	verbose := flags.Bool("v", false, "print every step as it is taken")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	switch {
	case *clients < 1 || *clients > maxClients:
		harness.Say(stderr, "e2efuzz: -clients must be 1 to %d\n", maxClients)
		return 2
	case *duration <= 0 || *duration > longestRun:
		harness.Say(stderr, "e2efuzz: -duration must be above 0 and at most %v, which the configured clocks outlast\n", longestRun)
		return 2
	case *steps < 0:
		harness.Say(stderr, "e2efuzz: -steps must be at least 0\n")
		return 2
	case *mode != "" && !slices.Contains(storageModes, *mode):
		harness.Say(stderr, "e2efuzz: -mode must be full, normal or memory\n")
		return 2
	}
	seeded := false
	flags.Visit(func(f *flag.Flag) { seeded = seeded || f.Name == "seed" })
	if !seeded {
		*seed = rand.Uint64()
	}
	harness.Say(stdout, "e2efuzz: seed %d\n", *seed)
	if *mode == "" {
		*mode = storageMode(*seed)
	}

	var b, launched *harness.Broker
	// The configuration of a broker the run starts, which a memory-mode
	// run's restart takes again (restart).
	var o *harness.Options
	// The TLS listener of a broker the run starts, and the configuration
	// of its clients; a broker given with -addr has none for the run.
	var secureAddr string
	var secure *tls.Config
	// unixPath is the unix listener with proxy_protocol of a broker the
	// run starts; a broker given with -addr has none for the run.
	var unixPath string
	// webSockets are the ws listener and the ws listener with tls of a
	// broker the run starts; a broker given with -addr has none for the run.
	var webSockets []string
	// authority issued the TLS listeners' certificate, and issues the
	// operators' (operations.go); a broker given with -addr has none for
	// the run.
	var authority *testkit.Authority
	// bridge is the peer of the bridge of a broker the run starts
	// (bridges.go); a broker given with -addr has none for the run.
	var bridge *bridging
	if *addr == "" {
		dir, err := os.MkdirTemp("", "saguin-e2efuzz-")
		if err != nil {
			harness.Say(stderr, "e2efuzz: %v\n", err)
			return 1
		}
		if err = writePasswords(dir); err != nil {
			harness.Say(stderr, "e2efuzz: writing the password file: %v\n", err)
			_ = os.RemoveAll(dir) // nothing ran there
			return 1
		}
		if err = writeACL(dir); err != nil {
			harness.Say(stderr, "e2efuzz: writing the acl_file: %v\n", err)
			_ = os.RemoveAll(dir) // nothing ran there
			return 1
		}
		if secure, authority, err = writeCertificates(dir); err != nil {
			harness.Say(stderr, "e2efuzz: writing the TLS listener's certificate: %v\n", err)
			_ = os.RemoveAll(dir) // nothing ran there
			return 1
		}
		if err = writeOperators(dir); err != nil {
			harness.Say(stderr, "e2efuzz: writing the operators' file: %v\n", err)
			_ = os.RemoveAll(dir) // nothing ran there
			return 1
		}
		if bridge, err = newBridging(*seed, dir); err != nil {
			harness.Say(stderr, "e2efuzz: %v\n", err)
			_ = os.RemoveAll(dir) // nothing ran there
			return 1
		}
		defer bridge.close()
		o = options(dir, *clients)
		o.Bridges = []harness.Bridge{bridge.bridge()}
		o.StorageMode = *mode
		// ram is the folder in /dev/shm holding a memory-mode broker's
		// memory_dir, which the broker creates; "" in the other modes.
		var ram string
		switch *mode {
		case "normal":
			o.SyncInterval = syncInterval
		case "memory":
			if ram, err = os.MkdirTemp(ramFolder, "saguin-e2efuzz-"); err != nil {
				harness.Say(stderr, "e2efuzz: making the folder of memory_dir: %v\n", err)
				_ = os.RemoveAll(dir) // nothing ran there
				return 1
			}
			o.MemoryDir = filepath.Join(ram, "memory")
		}
		harness.Say(stdout, "e2efuzz: storage mode %s\n", *mode)
		harness.Say(stdout, "e2efuzz: bridge peer %s\n", bridge.peer.URL())
		if b, err = harness.Launch(*binary, o); err != nil {
			harness.Say(stderr, "e2efuzz: starting %s: %v\n", *binary, err)
			_ = os.RemoveAll(dir) // nothing ran there
			if ram != "" {
				_ = os.RemoveAll(ram) // nor there
			}
			return 1
		}
		*addr, *dbPath, secureAddr, unixPath, webSockets = b.Addr, cmp.Or(b.RAM, b.DB), b.Addrs[1], b.Unix[0].Path, b.WS
		launched = b
		defer func() {
			if code == 0 {
				_ = os.RemoveAll(dir) // a passing run leaves nothing to look at
				if ram != "" {
					_ = os.RemoveAll(ram)
				}
				return
			}
			harness.Say(stdout, "e2efuzz: the broker's files and log are kept in %s\n", dir)
			if ram != "" {
				harness.Say(stdout, "e2efuzz: its memory_dir is kept in %s\n", o.MemoryDir)
			}
		}()
	}

	var log io.Writer
	if *verbose {
		log = stdout
	}
	f := newFuzzer(*addr, *dbPath, *seed, *clients, *timeout, log)
	f.secureAddr, f.secure, f.unixPath = secureAddr, secure, unixPath
	if webSockets != nil {
		f.wsAddr, f.wssAddr = webSockets[0], webSockets[1]
	}
	if launched != nil {
		f.paces = true
		f.scrape = func() (testkit.Exposition, error) { return scrape(launched) }
		var err error
		if f.ops, err = newOperations(*seed, launched, authority, secure); err != nil {
			harness.Say(stderr, "e2efuzz: issuing the operators' certificates: %v\n", err)
			_ = launched.Stop()
			return 1
		}
		f.reloads = newReloads(o.Dir, launched)
		defer f.reloads.restore()
		f.bridge = bridge
		bridge.start()
	}
	if *wirePath != "" {
		file, err := os.Create(*wirePath)
		if err != nil {
			harness.Say(stderr, "e2efuzz: wire log: %v\n", err)
			if b != nil {
				_ = b.Stop()
			}
			return 1
		}
		defer func() { _ = file.Close() }()
		f.trace = newWireLogger(file)
	}
	defer func() {
		if f.db != nil {
			_ = f.db.Close() // opened read-only: nothing is lost
		}
	}()
	harness.Say(stdout, "e2efuzz: %d clients against %s for %v\n", *clients, *addr, *duration)
	start := time.Now()
	err := f.drive(start.Add(*duration), *steps)
	took := time.Since(start)
	switch {
	case err != nil:
		f.closeAll()
		if b != nil {
			if err := b.Stop(); err != nil {
				harness.Say(stdout, "e2efuzz: stopping the broker after the failure: %v\n", err)
			}
		}
	case b != nil:
		// The check reads the broker before the stop ends it.
		err = errors.Join(memoryFilled(b), throttled(b), f.stop(b))
		if err == nil && (b.RAM != "" || f.bridge != nil) {
			err = restart(*binary, o, f.bridge, f.timeout)
		}
		if b.RAM != "" {
			f.readFrom(b.DB)
		}
	default:
		f.closeAll()
	}
	if err == nil {
		// What the clients sent last, now settled.
		err = f.checkRecords()
	}
	if err == nil && b != nil {
		// The broker's floods sent payloads over 4 KiB of both kinds.
		err = f.stored.tested()
	}
	if err != nil {
		if errors.Is(err, errWatchdog) {
			// step names the next step, which the watchdog prevented.
			harness.Say(stdout, "e2efuzz: FAIL: watchdog: -duration %v passed after %d of %d steps\n", *duration, f.step-1, *steps)
		} else {
			harness.Say(stdout, "e2efuzz: FAIL at step %d: %v\n", f.step, err)
		}
		harness.Say(stdout, "e2efuzz: the last %d steps:\n", len(f.recent))
		for _, line := range f.recent {
			harness.Say(stdout, "  %s\n", line)
		}
		harness.Say(stdout, "e2efuzz: replay with %s\n", replay(flags, f, err))
		return 1
	}
	published, found, notDue, possibleDrops := f.willCounts()
	ws := f.willStats
	harness.Say(stdout, "e2efuzz: Wills: %d delayed, %d of them found published after their delay; %d cancelled in time, %d reconnects too late, %d near their due time, %d published early by a clean start, %d connections holding one taken over\n",
		ws.delayed, f.wentLate(), ws.cancelled, ws.late, ws.uncertain, ws.early, ws.takeovers)
	as := &f.appendStats
	harness.Say(stdout, "e2efuzz: append channels: %d deliveries, %d positions made, %d of them at the floor, %d kept with no subscription read again, %d subscriptions with a partition; %d deliveries handed over before an UNSUBSCRIBE that left them no subscription; %d records retention deleted\n",
		as.deliveries.Load(), as.positions, as.floors, as.resumed, as.partitioned, as.orphans.Load(), f.retentionDeleted())
	ss := f.seekStats
	harness.Say(stdout, "e2efuzz: seeks: %d answered with an offset, %d answered with a refusal, %d refused or dropped without an answer, %d of them answers over Maximum Packet Size; %d $saguin/ filters refused\n",
		ss.moved, ss.answeredRefusal, ss.refused, ss.tooLarge, ss.filters)
	rs := &f.requestStats
	harness.Say(stdout, "e2efuzz: requests: kv/get %d answered and %d refused or dropped; catalogue %d described, %d answered empty and %d dropped or discarded; disconnect %d clients hung up, %d answered no-such-client and %d refused or dropped\n",
		rs.kvValues, rs.kvRefused, rs.catalogued, rs.catalogueEmpty, rs.catDrops, rs.hungUp, rs.noSuchClient, rs.disconnectFails)
	sl := &f.shares
	harness.Say(stdout, "e2efuzz: shared subscriptions: %d $share filters granted, %d shared deliveries, %d of them to a second member, the first's session ended\n",
		sl.granted, sl.delivered, sl.returned)
	qs := &f.queues.stats
	harness.Say(stdout, "e2efuzz: queue: %d jobs, %d worker subscriptions granted and %d refused, %d offers read and %d resent; %d PUBACKs 0x00 and %d refusing, %d acks, %d returns and %d other payloads sent, %d responses naming no attempt of the sender's that it surely holds; %d jobs found finished and %d dead-lettered, %d dead letters read by the last reader (%d expired, %d evicted, %d attempts_exhausted); %d worker subscriptions unsubscribed\n",
		qs.jobs, qs.workers, qs.workerRefusals, qs.offers, qs.resent, qs.puback0, qs.refusing, qs.acks, qs.returns, qs.others, qs.notOwn, qs.completed, qs.deadLetters,
		qs.reasons["expired"]+qs.reasons["evicted"]+qs.reasons["attempts_exhausted"], qs.reasons["expired"], qs.reasons["evicted"], qs.reasons["attempts_exhausted"], qs.unsubscribedAt)
	ll := &f.latest
	harness.Say(stdout, "e2efuzz: latest channels: %d values and %d deletions sent on %d topics, %d initial passes, %d values a pass sent again, %d tombstones delivered, %d changes made while their session was away delivered on its return; %d current changes owed at checkpoints, %d of them awaited there\n",
		ll.values, ll.deletions, len(ll.topics), ll.passes, ll.copies.Load(), ll.tombstones.Load(), ll.returned.Load(), ll.due, ll.awaited)
	if r := f.reloads; r != nil {
		harness.Say(stdout, "e2efuzz: reloads: %d applied and %d refused, their files written back; %d users' passwords replaced; %d changes of %s's write on %q\n",
			r.applied, r.refused, len(r.formers), r.toggles, restricted, rights[toggled].topic)
	}
	if b := f.bridge; b != nil {
		bs := b.stats
		harness.Say(stdout, "e2efuzz: bridge: %d links, %d of them after the peer dropped one and %d after a restart; %d SUBSCRIBEs, %d of them asking again for %d filters refused; %d PINGREQs; %d records forwarded by the peer, %d acknowledged stored and %d refused; %d records sent for its out rules, %d of them to be skipped, %d taken by the peer and %d refused there, %d answers held back, %d records sent again under their packet identifiers; %d PUBRELs, %d of them sent again\n",
			bs.links, bs.drops, bs.restarts, bs.subscribes, bs.retries, bs.refused, bs.pings, bs.records, bs.stored, bs.refusedRecords, bs.crossings, bs.skipped, bs.crossed, bs.refusedOut, bs.withheldOut, bs.resent, bs.releases, bs.resentReleases)
	}
	if f.paces {
		harness.Say(stdout, "e2efuzz: publish limits: %d PUBLISHes of %s's written before its limits, %s, let its reader read them, every one answered no sooner than they allow\n",
			f.heldBack.Load(), pacedUser, pacedLimits)
	}
	if o := f.ops; o != nil {
		harness.Say(stdout, "e2efuzz: operations: %d calls, %d of them over TLS and %d through a PROXY header over a unix socket; statuses %v\n",
			o.calls[opsPlain]+o.calls[opsSecure]+o.calls[opsProxied], o.calls[opsSecure], o.calls[opsProxied], o.statuses)
	}
	harness.Say(stdout, "e2efuzz: ok, seed %d: %d steps in %v, %.0f steps/s; %d of %d connections over TLS, %d over a WebSocket (%d of them over TLS, %d with their packets split across frames or messages), %d through a PROXY header over a unix socket; %d Wills published, %d found stored, %d not yet due, %d possibly dropped; %d records on configured channels, %d PUBLISHes refused for a channel's max_bytes\n",
		*seed, f.step, took.Round(time.Millisecond), float64(f.step)/took.Seconds(), f.secured, f.dials, f.webSockets, f.overWSS, f.split, f.proxied, published, found, notDue, possibleDrops, f.onChannels, f.forRoom.Load())
	return 0
}

// restart starts binary again on o, the configuration of the run's broker,
// once that broker's graceful stop has ended, its final snapshot taken in
// memory mode, and stops it gracefully, requiring the start to serve and the
// stop to exit 0: the run's checks then read file_path as the restarted
// broker leaves it (design.md, Store, memory mode: "A restart in memory mode
// recovers the whole store, offsets included, from file_path"; "A start
// replaces any RAM copy left there"; Restart, Graceful stop: "Memory mode
// takes its final snapshot, except after a store error"). With the run's bridge, whose stop left its
// exchanges unfinished (unfinishedCrossings), the peer serves the restarted
// broker's bridge as after a dropped link, its CONNACK's Session Present
// drawn, and every record an out rule owes the peer reaches it, within
// limit, before the stop (bridging.settle): the exchanges taken up again
// under their packet identifiers, or sent again as new messages (design.md,
// Restart, Restored: the broker "restores [...] bridges' in-flight windows";
// Bridges, Out).
func restart(binary string, o *harness.Options, bridge *bridging, limit time.Duration) error {
	if bridge != nil {
		bridge.restarting()
	}
	b, err := harness.Launch(binary, o)
	if err != nil {
		return fmt.Errorf("restarting on the files the graceful stop left: %w", err)
	}
	if bridge != nil {
		if _, err = bridge.up(nil, limit); err == nil {
			err = bridge.settle(limit)
		}
		bridge.stop()
	}
	if stopErr := b.Stop(); stopErr != nil {
		err = errors.Join(err, fmt.Errorf("stopping the restarted broker: %w", stopErr))
	}
	if bridge != nil {
		err = errors.Join(err, bridge.stopped())
	}
	return err
}

// memoryFull is the series a default run's floods must move: metrics.md,
// Store and memory, saguin_memory_full_seconds_total{direction}, "each
// budget's full time (design.md, Message memory)", the incoming budget's:
// Message memory, "A budget is full from the event at which its eligible
// FIFO head does not fit to the first event at which that head fits or no
// head remains".
var memoryFull = testkit.SeriesKey("saguin_memory_full_seconds_total", "direction", "incoming")

// scrape reads b's /metrics.
func scrape(b *harness.Broker) (testkit.Exposition, error) {
	var page strings.Builder
	if _, err := b.Metrics().WriteTo(&page); err != nil {
		return testkit.Exposition{}, fmt.Errorf("scraping /metrics: %w", err)
	}
	e, err := testkit.ParseExposition(page.String())
	if err != nil {
		return testkit.Exposition{}, fmt.Errorf("scraping /metrics: %w", err)
	}
	return e, nil
}

// memoryFilled reads b's /metrics at the end of a default run, whose
// floods' bodies together outgrow max_incoming_memory, and returns an
// error unless memoryFull rose above 0: a run in which no reader ever
// waited for its bytes did not test what the floods are for (design.md,
// Message memory; Testing: "A run that tested nothing never passes";
// 0020a.md, 261019 184809).
func memoryFilled(b *harness.Broker) error {
	return risen(b, memoryFull, "the incoming budget was never full, so no reader waited for its bytes (design.md, Message memory)")
}

// risen reads b's /metrics at the end of a run and returns an error unless
// the counter series, as testkit.SeriesKey names it, rose above 0, untested
// saying what the run then left untested.
func risen(b *harness.Broker, series, untested string) error {
	e, err := scrape(b)
	if err != nil {
		return fmt.Errorf("%s: %w", series, err)
	}
	v, ok := e.Values[series]
	switch {
	case !ok:
		return fmt.Errorf("/metrics has no %s", series)
	case v <= 0:
		return fmt.Errorf("%s is %v at the end of the run: %s", series, v, untested)
	}
	return nil
}

// replay is the command that reruns a failed run to its failure, as
// printed: make's e2efuzz target with the run's -duration, its seed and
// clients, -steps through the step that failed, and every other flag
// given that changes the run. Without -steps a replay of a short run
// would end "ok" before the step that failed. A watchdog failure is a run
// that could not take its -steps within -duration: only the same two
// repeat it. A failure in the run's last checks replays the steps taken;
// uncounted, those took the whole -duration, so the replay, counted, gets
// the longest a run may last, the watchdog bounding it and deciding no
// step (design.md, Testing).
func replay(flags *flag.FlagSet, f *fuzzer, err error) string {
	duration := flags.Lookup("duration").Value.String()
	steps := strconv.Itoa(f.step)
	switch {
	case errors.Is(err, errWatchdog):
		steps = flags.Lookup("steps").Value.String()
	case f.ended:
		steps = strconv.Itoa(f.step - 1)
		if flags.Lookup("steps").Value.String() == "0" {
			duration = longestRun.String()
		}
	}
	args := []string{"-seed", flags.Lookup("seed").Value.String(), "-clients", flags.Lookup("clients").Value.String(), "-steps", steps}
	flags.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "seed", "clients", "steps", "duration", "v", "wire":
			// Given above, or changing what is printed, not the run.
		default:
			args = append(args, "-"+fl.Name, fl.Value.String())
		}
	})
	return fmt.Sprintf("make e2efuzz E2EFUZZ_DURATION=%s E2EFUZZ_FLAGS=%q", duration, strings.Join(args, " "))
}
