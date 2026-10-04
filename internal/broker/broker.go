// Package broker is the mochi-mqtt hook that gives saguin's channels
// their semantics. Everything here is enforcement: the rules hold against
// whatever a client actually sent, never because an SDK was polite
// (invariant 10).
package broker

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/store"
)

// Broker is the saguin hook.
type Broker struct {
	mqtt.HookBase

	reg *channel.Registry

	// allowances is one publish budget per connected client, for
	// limits.publish_rate. A sync.Map rather than a field under b.mu
	// because checkBounds runs on the publish path without that lock, and
	// putting every publisher behind it is the cost this feature must not
	// have. Empty and never read when the limit is unset, which is the
	// default.
	allowances sync.Map

	// authz answers what a client may do, once saguin's own structural
	// rules have let the operation through. nil is no acl_file, which is
	// every authenticated client may do anything - what saguin did before
	// authorization existed, and what it still does unless one is set.
	//
	// **Atomic because it is replaced on a running broker.** SIGUSR1
	// re-reads the acl_file (RFC 0002), so what was once written at startup
	// and read for ever is now written under load - and two of its readers
	// are on the publish path, which cannot take a lock per message. A
	// pointer to the interface rather than an atomic.Value, because the
	// value stored is sometimes a nil Authorizer and atomic.Value refuses
	// an inconsistent concrete type.
	authz atomic.Pointer[Authorizer]

	// refusedRead remembers which consumers have been told their grant is
	// gone, and under which acl_file, so a stalled consumer is one log line
	// rather than one per publish. Keyed by client id and channel; dropped
	// with the rest of a client's state when it goes.
	refusedRead map[string]uint64

	// authzGen counts how many times that has happened, and is how anything
	// caching an answer derived from the acl_file knows to ask again.
	// Without it a re-read that changed a client's publish_rate would apply
	// to the next client and never to the one connected - the cache is
	// keyed on the client id, which does not change when the file does.
	authzGen atomic.Uint64
	srv      *mqtt.Server
	log      *slog.Logger

	// limits are the bounds a client is held to. Set once by NewServer,
	// read without the lock because nothing writes them after startup.
	limits config.Resolved

	// refused is the bounded record behind /v1/operations/refused: which
	// clients this broker refused, at CONNECT or after it, on any protocol.
	refused *refusals

	// passed is the bounded record behind /v1/operations/position-lost:
	// which readers the retention floor overtook, and how much each lost.
	// The counter beside it says how many times; this says who.
	passed *passings

	// credentials are who may connect, or nil when no password file is
	// configured. anonymous is whether a client offering no user name is
	// admitted. Both are read on every CONNECT and written once, before any
	// listener opens, which is why neither is under the broker's lock.
	// configDoc is the resolved configuration `/v1/operations/config`
	// answers with, as the binary resolved it at startup. An atomic pointer
	// like the credentials below rather than a plain field: it is written
	// once before any listener exists and read on every request, and a
	// field would be a data race that happens to be safe today and would
	// stop being so the moment anything reloaded.
	//
	// **Nil is a wiring failure and not a configuration**, so the handler
	// says so rather than answering with an empty document. Nothing in the
	// schema turns this route off; the operations listener being absent is
	// what does that, and it is checked one level up.
	configDoc atomic.Pointer[map[string]any]

	// acl is the authorization file `/v1/operations/acl` explains, and the
	// path it was read from, which the answer names. Nil where the
	// configuration writes no `acl_file` - which is not a wiring failure
	// but a real state with a real answer: every authenticated client may
	// do anything, said out loud rather than as an empty grant list.
	acl atomic.Pointer[aclSource]

	credentials atomic.Pointer[passwd.File]
	anonymous   atomic.Bool
	// listenerAuth overrides the two above for one listener, keyed by the
	// id it was created under. Written before any listener opens and only
	// read afterwards; under b.mu because a map is not atomic and the
	// alternative is a second pointer nobody would remember to swap.
	listenerAuth map[string]listenerCredentials

	// retainAvail is the CONNACK's Retain Available: 1 where the retain
	// flag can be honoured somewhere, 0 where it can be honoured nowhere.
	// Worked out once in New because it is a property of the configuration,
	// which does not change while the broker runs, and because the loop
	// that answers it walks every channel - cheap once, and paid on every
	// CONNECT if it were asked there.
	retainAvail byte

	// retaken counts the messages OnRetainMessage has taken back out of the
	// substrate's retained store. Atomic rather than under the lock: it is
	// touched on the publish path and read only by a test.
	//
	// atomic.Uint64 rather than uint64 because the atomic types carry
	// align64. A raw uint64 among pointer-width fields can land on a
	// 4-byte boundary on a 32-bit platform, and a 64-bit atomic there
	// panics - for this field, the first time a retained message is
	// published.
	retaken atomic.Uint64

	// startedAt is when this broker was built, for saguin_uptime_seconds.
	// Set once and never written again, so it needs no lock.
	startedAt time.Time

	// version and brokerID are what saguin_build_info reports, and
	// providerKinds is what saguin_provider_info does. The command sets all
	// three before any listener opens, so none needs a lock.
	version       string
	brokerID      string
	providerKinds map[string]string

	// counted are saguin's own tallies, incremented at the sites where the
	// things they count happen. Built with the broker from the registry, so
	// a channel always has somewhere to count and nothing a client sends
	// can add an entry.
	counted *counters

	// bridgeStats are the bridges the catalogue reports on, set once at
	// startup by the command that built them.
	bridgeStats []BridgeStats

	mu sync.Mutex

	logs map[string]LogStore // append channels, by name
	// providers is the store behind those channels, by provider name, for
	// what a provider can do and a channel cannot. See Stores.Providers.
	providers map[string]ReaderDropper
	// waiters is each of those providers that serves a waiting client's
	// writes first (wireClientWaiting), set with them.
	waiters []clientWaiter
	queues  map[string]QueueStore  // queue channels, by name
	latest  map[string]LatestStore // latest channels, by name

	// subs tracks channel subscriptions only. A filter whose channel
	// prefix is not literal never appears here, which is how invariant 11 holds:
	// such a filter reaches broadcast topics and nothing else.
	subs map[string][]subscription // by client id

	// partitions is what each client declared about which slice of a
	// channel it wants, by client id and then by the filter it was declared
	// on (RFC 0003 "Client-declared partitioning").
	//
	// **A second index rather than a field on `subscription`, because a
	// broadcast subscriber has no `subscription` at all**: track returns nil
	// for a filter reaching no channel, and partitioning is allowed on
	// broadcast - it needs no channel, no storage and no position. Making
	// room for a channel-less entry in `subs` instead would ripple through
	// every one of its readers, each of which assumes a channel.
	//
	// **Two maps are a thing to keep in step, so they are removed from in
	// one place each**: forgetClient for a whole client, forgetFilters for
	// individual filters. Nothing else may remove from either, and a test
	// walks the source to say so.
	partitions map[string]map[string]partition // by client id, then filter

	// byTopic and members are subs turned round, for the paths that ask
	// which clients a record or a queue concerns rather than what one client
	// holds: pumpAll and publishLatest per record, workersOf and roomFor per
	// queue offer.
	//
	// **Each of the four used to walk every client's subscriptions**, under
	// b.mu, to find the few a record was for. The scale run wedged on that
	// at 20,000 consumers: throughput fell 25x from 50 consumers to 20,000
	// with the delivery work held the same, and a mutex profile charged
	// pumpAll's walk with the most time spent waiting for the lock
	// (BenchmarkPublishAtScale).
	//
	// **byTopic is keyed by topic, not only by channel**, because the usual
	// fleet is one channel and a filter per device: an index that answered
	// "who is on this channel" would hand every one of them back to be
	// matched. It is the substrate's own trie, one per channel, and it
	// answers identity only - which clients hold a filter matching the
	// topic. Everything about the subscription itself is still read from
	// subs, so there is one source of it. members is who is on a channel at
	// all, which is the question for a queue: a worker's pin matches none of
	// its queue's topics.
	//
	// **Only reindex writes either**, from subs, and every writer of subs
	// calls it - TestEveryWriterOfTheSubscriptionIndexReindexes walks the
	// source for that. So they are rebuilt on a resume, a restart and a
	// takeover by the same path subs is, and cannot disagree with it.
	byTopic map[string]*mqtt.TopicsIndex   // channel name -> its filters, by client
	members map[string]map[string]struct{} // channel name -> client ids
	indexed map[string]map[subKey]struct{} // client id -> what reindex last wrote for it

	// positions orders every write of a stored position against every drop
	// of one, and it is deliberately not b.mu.
	//
	// The two cannot be ordered on the broker-wide lock, because the writes
	// do not happen under it: flushPositions collects its batch under b.mu,
	// releases it, and writes outside - on purpose, since that write is what
	// used to sit on the publish path. So a drop that took b.mu excluded
	// nothing, and a position collected before its session was swept landed
	// after the row was deleted and put it back for ever. Measured: a row
	// whose last_seen predated by half a second the delete that reported
	// removing it, still present afterwards, and a replacement device on
	// that client id then told Session Present = 0 and served none of the
	// records below the ghost offset - invariant 1.
	//
	// It is held across store I/O, which b.mu may never be (RFC 0005): that
	// is the whole reason it is a second lock rather than a wider use of the
	// first. **b.mu may be taken while this is held; this may never be taken
	// while b.mu is held**, which is the one rule that keeps the pair
	// acyclic.
	positions sync.Mutex

	// owner is the connection that currently holds the state keyed by each
	// client id - saguin's own answer to "is this teardown still the one
	// that owns this", rather than a third signal read from the substrate.
	//
	// Everything a takeover can damage is keyed by client id and shared by
	// both connections, so the question a teardown has to answer is whose
	// state it is about to remove. The two substrate signals cannot answer
	// it: both are read before the lock that protects those maps, and both
	// are set at moments the substrate chooses, so a teardown that reads
	// them in the gap between the old connection being disconnected and
	// being marked taken over passes both and then deletes what the new
	// session has since installed. Measured: a delay injected between the
	// reads and the deletes failed 9 rounds in 10 against 0 for the same
	// build without it.
	//
	// This is written and read under b.mu, so the check and the act are one
	// critical section and the substrate's ordering stops mattering.
	owner map[string]*mqtt.Client // by client id

	// claims is what a connection took over when it claimed a client id, for
	// as long as its CONNACK has not been written: claim has why. Written and
	// read under b.mu; claimMu orders a claim being given back against the
	// next connection reading what the id holds.
	claims map[*mqtt.Client]*claim
	// supersededWills is what became of the session of each connection a
	// confirmed claim took the id from while it was still open: true where a
	// clean start ended it, false where it was resumed. Its Will, if it has a
	// delayed one, is decided by that when the connection closes
	// (willAtTakeover). Under b.mu; spent at its OnWill or its OnDisconnect.
	supersededWills map[*mqtt.Client]bool
	// cleanStartEnds is the session each confirmed clean start ends, handed
	// from confirmClaim, which is the last moment the replaced connection
	// still holds its subscriptions, to OnSessionRegistered, which ends it
	// once the new connection is registered. Under b.mu; spent there or at
	// that connection's OnDisconnect.
	cleanStartEnds map[*mqtt.Client]endedSession
	// willFired is each connection whose OnWill published its Will, so the
	// disconnect that follows takes it off the record whatever the broker
	// has begun since (recordDisconnect). Under b.mu; spent at that
	// connection's OnDisconnect.
	willFired map[*mqtt.Client]bool
	claimMu   sync.Mutex

	// quotas are the memory providers' bounds, by provider name. A channel
	// on a provider with one shares it with every other channel there.
	quotas map[string]*store.Quota

	// measures answer saguin_provider_bytes and saguin_provider_max_bytes,
	// by provider name.
	//
	// **A second map rather than reading quotas, because a quota is only
	// one of the two things that can bound a provider.** A sqlite provider
	// bounds its own file with max_page_count and has no quota at all, so a
	// collector reading quotas reported that a provider actively refusing
	// publishes held nothing and had no bound - a failure that renders as
	// health: 198 publishes accepted, three
	// refused 0x97, and both gauges zero throughout.
	//
	// Set before any listener opens, like providerKinds, so nothing needs a
	// lock beyond the one every collector already takes.
	measures map[string]ProviderMeasure

	// dirs are the snapshot directories, by storage provider. Empty when
	// every provider is memory with snapshot_dir: none, which is a broker
	// where nothing survives a restart.
	dirs map[string]*store.Dir

	// durable says a store that keeps its own records was attached, which
	// a snapshot directory does not cover.
	durable bool

	// retained is the one store holding the current value of broadcast
	// topics a client asked to have kept, or nil where the operator
	// configured none - which is the broker that refuses such a publish
	// with 0x9A (RFC 0002 "Retained messages on a broadcast topic").
	//
	// It is not a channel and is not in the registry: it claims no prefix
	// and holds only topics no channel claims, so it cannot overlap one
	// (invariant 12) and a filter reaching it reaches broadcast topics and
	// nothing else (invariant 11).
	retained LatestStore
	// retainedProvider is which storage provider holds it, so that a
	// snapshot goes to the right directory and the provider's bound
	// reaches it like any channel's.
	retainedProvider string
	// retainedPeriod is how long a broadcast topic keeps its value after
	// going quiet, in seconds, or zero for `none` - which is the default,
	// because any figure would silently lose the state of a device that
	// reports less often than it.
	retainedPeriod int64
	// retainedPeriodNow is retainedPeriod, readable without b.mu: a
	// subscriber's drain judges its waiting retained values by it under the
	// consumer's lock, which is taken under b.mu and never around it.
	retainedPeriodNow atomic.Int64

	// pendingExpires is how long an unreleased publish waits before the
	// sweep drops it (`broker.qos2.expires_after`), or zero for never.
	pendingExpires time.Duration
	// qos2MaxPerClient is `broker.qos2.max_inflight_per_client`: how many
	// exactly-once publishes one client may hold unreleased, across every
	// channel (heldBy).
	qos2MaxPerClient int

	// held is every exactly-once publish held for its release, by exchange:
	// the channel its PUBLISH resolved to, which is the store the release
	// finds it in - never the topic resolved again - and when it was held.
	// heldBy is how many each client holds, across every channel, for
	// broker.qos2.max_inflight_per_client. Rebuilt at a start from every
	// channel store's holds (unreleasedBySession).
	//
	// **heldMu guards the two maps and nothing else, and no store is called
	// holding it.** The order is the client id's session lock, then the
	// store, then heldMu: a hold, a release, an expiry and a session's end
	// each run under the id's session lock, so the maps and the store agree
	// for that id whenever the lock is let go.
	heldMu sync.Mutex
	held   map[store.Exchange]heldExchange
	heldBy map[string]int
	// holdWraps is a channel's holds as a test has wrapped them (WrapHolds),
	// consulted by holdsOf before the channel's own store. Empty outside
	// tests. Guarded by mu.
	holdWraps map[string]HoldStore

	// sessions keeps the state of every MQTT session on the provider
	// `broker.session.storage` names (RFC 0002), and sessionsProvider is that
	// provider, for its bound to reach the store. nil is an in-process harness
	// that attached none, which keeps sessions the way the substrate always
	// has.
	sessions         SessionStore
	sessionsProvider string

	// sessionsNow is sessions, read without b.mu. The store is attached once,
	// before any listener opens, and asked for on every delivery a session
	// keeps; taking the broker's lock for that put each of a resumed
	// session's re-sends in the queue behind whatever else held the lock.
	sessionsNow atomic.Pointer[sessionsRef]

	// ackCommit is broker.session.ack_commit_interval, in nanoseconds, and
	// zero until one is set (AckCommitInterval).
	ackCommit atomic.Int64

	// sessionNotKept is the connections whose session the store had no room
	// for, told in their CONNACK that it ends with the connection. Keyed by
	// the connection, as willProps is, and removed when it ends, so it is
	// bounded by the connections open.
	sessionNotKept map[*mqtt.Client]bool

	// subscriptionsCapped is the connections whose last SUBSCRIBE had a filter
	// refused at limits.max_subscriptions, so the refusal is said once an
	// episode rather than once a SUBSCRIBE (capSubscriptions). Keyed by the
	// connection and removed when it ends, as sessionNotKept is.
	subscriptionsCapped map[*mqtt.Client]bool

	// maxQoS is the highest QoS this broker offers, advertised in every
	// CONNACK and enforced against every publish. It is a field rather than
	// a constant because the answer depends on the configuration: without a
	// place to hold an unfinished exchange, exactly-once cannot be offered.
	//
	// The advertisement and the refusal read this one number, so raising it
	// can never leave OnPacketRead enforcing a ceiling the CONNACK no longer
	// states.
	maxQoS byte

	// willClient is what every Will is published as. It cannot be the
	// server's own inline client: routePublish short-circuits for that one,
	// because a queue delivery re-entering the path is already stored - and
	// a Will injected as it would skip channel resolution and reach
	// subscribers having been stored nowhere, which is the whole defect
	// this feature exists to fix. A separate in-process client is routed
	// like anybody else, which is the same reasoning a bridge's client
	// carries - except for the one thing that is keyed by the client
	// rather than by the publish: the publish rate (routePublish). Who a
	// Will is published as is the client that armed it (publisherOf).
	willClient *mqtt.Client

	// willMu serialises Will publishes, so that willOutcome is the answer
	// to the one in flight: publishWill injects under it, and OnPublish,
	// running inside that injection, records what the publish path decided.
	willMu      sync.Mutex
	willOutcome error
	// willArmer is the client id that armed the Will in flight, under
	// willMu: who the Will is published as (publisherOf).
	willArmer string

	// willProps holds the publish properties of a connected client's Will -
	// its Content Type, Payload Format Indicator, Message Expiry Interval,
	// Response Topic and Correlation Data.
	//
	// **It exists because the substrate's own `mqtt.Will` does not carry
	// them.** That struct keeps a topic, a payload, a QoS, the retain flag,
	// the delay and the User Properties; the five above are read off the
	// CONNECT, validated, and dropped before any hook sees a Will. So they
	// are taken from the CONNECT packet here, where saguin already reads it
	// to judge the Will, and put back on the publish when it fires.
	//
	// **Bounded the way every other per-connection map is** (invariant 13):
	// one entry per authenticated connection that armed a Will, written at
	// OnSessionEstablish, taken out by OnWill, and deleted at disconnect for
	// a connection whose Will never fired. A CONNECT that is refused never
	// writes one, so it cannot leave one behind either.
	//
	// **Keyed by the connection rather than the client id**, so a connection
	// taking over an id and the one it supersedes each have only their own;
	// OnSessionEstablish has why.
	willProps map[*mqtt.Client]store.Props

	// pendingWills holds a Will whose client asked for it to be delayed,
	// until the delay expires or that client id connects again. It is one
	// timer per client that has gone and left a Will waiting, each on a
	// session the store keeps, so the session store's max_bytes bounds it,
	// not max_connections. A restart rebuilds it from the records
	// (restoreWill), for what is left of each wait.
	pendingWills map[string]*pendingWill

	// unwritten holds a disconnect the session store refused to record,
	// by client id, until it is written - every second (retryOwedWrites), by
	// a CONNECT for the id (settleOwed), or at the stop - or the id's session
	// is replaced or ends (recordDisconnect). One entry per session the store holds, which
	// the provider's max_bytes bounds.
	unwritten map[string]unwrittenDisconnect

	// disconnecting is what OnDisconnecting stored for a connection whose
	// DISCONNECT it answered, so that OnDisconnect does not write the same
	// again (recordDisconnect). Taken as that connection is torn down.
	disconnecting map[*mqtt.Client]unwrittenDisconnect

	// owedRecords holds, by client id, a session record a connection that
	// never completed wrote over and the store refused to put back
	// (settleClaim), and owedEndings what of an ended session the store
	// refused to drop - its positions, its unfinished exactly-once publishes
	// (endSession, RestoreSessions). Each is written again every second
	// (retryOwedWrites), by a CONNECT for the id before it claims anything
	// (settleOwed), and before any other write of the id's record
	// (settleRecord). One entry per client id the broker held a session for.
	owedRecords map[string]*owedRecord
	owedEndings map[string]*owedEnding

	// owedLogged is when retryOwedWrites last said the store still refuses
	// what it owes. Only its own goroutine (Run) reads or writes it.
	owedLogged time.Time

	// stopping says this broker is shutting down, so that the connections it
	// closes are not read as devices dying. See Shutdown.
	stopping atomic.Bool

	// dueWills are the Wills this start found already owed: their delay
	// passed, or their session ended, while the broker was stopped. They are
	// collected as the sessions are read and published once the channels
	// have been loaded (PublishDueWills), because a Will goes through the
	// publish path and a record written before the snapshots load is one the
	// load replaces.
	dueWills []dueWill

	// latestHeads is each latest channel's newest offset handed to
	// subscribers per topic - channel name to *sync.Map of topic to
	// *latestHead. See latestHead.
	latestHeads sync.Map
	// headsSwept is when latestHeads was last swept (sweepLatestHeads).
	headsSwept atomic.Int64

	// latestSeen is how far a consumer has got through a `latest` channel,
	// by client id and channel.
	//
	// It is memory only, and that is not a shortcut. A `latest` position is
	// worth having across a *reconnect*, which is where the delta pays; a
	// broker restart takes the session with it, so the client is told
	// Session Present = 0, subscribes again, and is served the whole of
	// current state - which is complete rather than a skip. Storing it
	// would be durable state with nothing to be durable for.
	//
	// **Behind a lock of its own, not b.mu.** See latestPositions.
	latestSeen *latestPositions

	// existed records, per client, which of the filters in a SUBSCRIBE the
	// client already held. MQTT's Retain Handling 1 means "send retained
	// messages only if the subscription did not already exist", and the
	// answer is only knowable before the substrate applies the packet -
	// OnSubscribe runs there, OnSubscribed does not.
	existed map[string]map[string]bool

	// afterSuback holds the deliveries a SUBSCRIBE earned, from the moment
	// they are decided until its SUBACK is on the wire (OnPacketSent).
	//
	// **One entry per connection and not one per packet**, because there can
	// only be one: the substrate reads, processes and writes for a
	// connection in a single goroutine, so the SUBACK is written before
	// that connection's next packet is read. An entry outlives the pair
	// only when the SUBACK write failed, and OnDisconnect clears it.
	//
	// **Keyed by the connection, not the client id.** Keyed by id, a
	// SUBSCRIBE handled on a connection another had since taken the id from
	// replaced what the new connection's own SUBSCRIBE was waiting to be
	// sent: the new connection's SUBACK then ran the old one's catch-up, to
	// a closed socket, and the new session was granted its subscription and
	// sent none of the backlog.
	afterSuback map[*mqtt.Client]func()

	// stopped is closed when Run returns, and running says Run was ever
	// started. Shutdown waits on the first only when the second is true:
	// a broker whose sweeps were never started - a migration, a test that
	// only publishes - must not wait for a loop that will never close
	// anything. Run is called at most once per broker, which is what makes
	// closing the channel safe.
	stopped chan struct{}
	running atomic.Bool

	// deliveries maps a Delivery ID to the queue item it designates.
	deliveries map[string]*delivery
	// byPacket correlates a worker's PUBACK back to the delivery, since
	// the PUBACK carries only a packet identifier.
	byPacket map[string]string

	// consumers is each client's delivery state - its cursors and the
	// packets it has in flight - by client id, a *consumer each. Written
	// under b.mu; read without it, since a consumer's own lock guards what
	// the entry holds. See consumer.
	consumers sync.Map
	// notified is each append channel's watermark, written by pumpAll and
	// started by attachLog. See watermark.
	notified map[string]*watermark

	// inline is the pseudo-client saguin injects queue deliveries as.
	inline *mqtt.Client

	// bridges are the in-process clients inbound bridges publish as, each
	// holding the outcome of the publish it has in flight. See BridgeClient.
	//
	// Its own lock rather than mu, and read only when a publish is refused:
	// mu is taken once per publish as it is, to announce the record to the
	// consumers it is for, and an ordinary publish must not pay for a
	// feature it does not use.
	bridgeMu sync.RWMutex
	bridges  map[*mqtt.Client]*BridgeClient

	// watchers are what an outbound bridge rule waits on: one set per
	// channel, signalled when a record lands there, and one list for
	// broadcast, called with the record itself.
	//
	// **The two differ because what they carry differs.** A channel stores
	// its records, so a watcher is told only that there is something to
	// look at and reads from its own stored position - the position is the
	// truth and the signal is a hint, which is what makes a missed signal
	// harmless. Broadcast stores nothing, so there is no position to read
	// from and the record itself has to be handed over as it passes. A
	// latest channel's value is handed over too: its store keeps only what
	// is current, with no history to read forward through, so a rule told
	// only "something changed" would have to reread every topic to find what
	// (bridge's drainLatest).
	//
	// Under their own lock for the reason bridges is: the publish path is
	// where throughput stops, and a broker with no bridge must not pay for
	// one. A signal is non-blocking into a buffer of one - a second wakeup
	// before the first was taken is the same wakeup.
	watchMu    sync.RWMutex
	watchers   map[string][]chan struct{}
	broadcasts []func(store.Record)
	// latestWatchers is the outbound bridge rules fed each value a latest
	// channel takes, by channel (WatchLatest). Pointers, so one can be
	// removed by identity.
	latestWatchers map[string][]*func(store.Record)

	// rr is the round-robin cursor per queue, so work is spread across
	// the live workers rather than piling onto whichever the map yields
	// first.
	rr map[string]uint64

	// holding counts the jobs each worker holds from each queue, from the
	// moment the server puts one in flight to that worker until the delivery
	// is forgotten - answered, timed out, or returned when the worker went.
	// A worker holding one is offered nothing more from that queue (RFC 0003
	// "Delivery"). Entries go at zero, so it is bounded by the deliveries
	// outstanding (invariant 13).
	holding map[heldBy]int

	// offerDue names the queues an answer or a reopened window has freed a
	// worker on, and offerNow wakes Run to offer them at once. A job handed
	// back by a worker that left waits for the tick, as new work does. Without it a worker
	// held to one job is offered its next one on the next tick, which holds a
	// busy worker to five jobs a second per queue. offerNow holds one signal,
	// so a burst of answers is one pass; offerDue is bounded by the queues.
	offerDue map[string]bool
	offerNow chan struct{}

	// shareCursors is each shared group's round-robin position, by the share
	// filter the substrate groups its members under - `$share/<group>/<filter>`
	// - the same rotation rr gives a queue's workers.
	//
	// **Its own lock rather than mu**, because it is taken for every publish a
	// shared group matches, broadcast included, and mu is the broker-wide lock
	// the publish path already queues on.
	//
	// **Bounded by the groups that exist**: a share filter is a subscription
	// somebody holds, and sweepShareCursors drops a cursor nothing has selected
	// with for shareCursorIdle, so one whose members have all gone does not
	// stay (invariant 13).
	shareMu      sync.Mutex
	shareCursors map[string]*shareCursor

	// shareExpiry is broker.share.expires_after: how long a group's
	// delivery waits before it is dropped whatever its members' sessions
	// say. Zero is no expiry of its own, and a backlog then lives exactly as
	// long as a member session that could collect it.
	shareExpiry time.Duration

	// drains counts the delivery goroutines pumpOffPath has started, so
	// that Shutdown can wait for them. They write to consumers and read
	// from stores, and the snapshot is written from the same stores - so a
	// drain outliving the wait would be read against a file already
	// written, or a position advanced after it was saved.
	//
	// **Added to only where Shutdown's Wait cannot have begun**: from a
	// goroutine it already counts, or from one Shutdown has stopped before
	// it waits - a client's (the listeners' close waits for every one), the
	// run loop (<-b.stopped), a bridge (stopped before Shutdown), or a
	// delayed Will's timer, which counts itself here (firePendingWill). An
	// Add from anywhere else can land after Wait has returned and run a
	// drain beside the snapshot.
	drains sync.WaitGroup

	// bcast is the session provider's broadcast log with every durable
	// session's owed list on it, or nil where none is attached. See bdrain.
	bcast atomic.Pointer[bdrain]
}

// partition is what a subscriber declared about which slice of a channel it
// wants, or the zero value for a subscriber that declared nothing and is
// therefore served everything - which is every subscriber until one asks
// otherwise (RFC 0003 "Client-declared partitioning").
//
// **It is keyed by filter rather than held on the subscription record**,
// because a broadcast subscriber has no subscription record at all: track
// returns nil for a filter that reaches no channel, and partitioning is
// allowed on broadcast. See Broker.partitions.
type partition struct {
	// count is the size of the space the client declared, and zero means it
	// declared none. A count with no index, or an index outside the count,
	// is refused at SUBSCRIBE rather than stored.
	count int

	// indices is which slices this member takes, sorted and deduplicated. A
	// member wanting several repeats the property, which is MQTT's own way
	// of carrying a list.
	indices []int
}

// declared reports whether this subscriber asked for a slice at all.
func (p partition) declared() bool { return p.count > 0 }

// wants reports whether a topic hashing to h belongs to this subscriber.
//
// **The hash is passed in rather than computed here**, because one publish
// is offered to many subscribers and the hash is a property of the topic
// alone. Computing it inside the per-subscriber loop pays for it once per
// member for nothing.
func (p partition) wants(h uint64) bool {
	if !p.declared() {
		return true
	}
	idx := int(h % uint64(p.count))
	// **A search rather than a scan, and the reason is a bound rather than
	// a micro-optimisation.** indices is deduplicated and every entry is
	// below count, so a client may legally declare as many of them as the
	// count allows - and this runs on every delivery to that subscriber. A
	// linear scan measured 1.7ns at one index and 1,329ns at thirty
	// thousand, which is a cost a client chooses for the broker.
	//
	// Bounding the count instead was considered and dropped: it would mean
	// inventing a number nobody can justify, and refusing a deployment with
	// more workers than somebody guessed. Making the lookup logarithmic
	// removes the reason to have one - 64 comparisons at a count no client
	// can exceed - so the count stays unbounded and the delivery path does
	// not care how large it is. indices is sorted by partitioning() for
	// this.
	// One slice is the overwhelmingly common shape - one worker taking one
	// partition - and a search costs it 11% for a case it never reaches.
	// One line on the per-delivery path buys it back.
	if len(p.indices) == 1 {
		return p.indices[0] == idx
	}
	i := sort.SearchInts(p.indices, idx)
	return i < len(p.indices) && p.indices[i] == idx
}

type subscription struct {
	filter  string
	channel *channel.Channel
	qos     byte

	// spans is whether this filter reaches more than one channel, which is
	// the only case where a consumer cannot tell one channel's offsets from
	// another's - it receives two independent sequences, interleaved, with
	// nothing saying which is which, and cannot recover the channel from
	// the topic without knowing the filters that RFC 0002 says it never has
	// to know.
	//
	// **Settled here, once, when the subscription is made.** Deciding it
	// per record would put a registry walk on the delivery path for an
	// answer that cannot change while the subscription lives.
	spans bool

	// deletions is a subscriber asking to be sent a latest channel's
	// deletions along with its current state, which a copy of that channel
	// on another broker needs and nobody else does (RFC 0003).
	//
	// It rides on the subscription rather than on the client, because MQTT
	// carries it on the SUBSCRIBE: a client may hold one subscription that
	// asked and another that did not, and each is served what it asked for.
	deletions bool

	// noLocal is the subscriber asking not to be sent its own publishes
	// [MQTT-3.8.3-3]. It rides on the subscription for the reason
	// `deletions` does: MQTT carries it per filter on the SUBSCRIBE.
	//
	// **A channel answers it here rather than in the substrate.** The
	// substrate compares a packet's origin against the client it is about
	// to write to, which is the whole answer for a broadcast topic; a
	// channel delivery is a read from the store, so what it compares is the
	// publisher the record carries (store.Record.Publisher).
	noLocal bool

	// retainAsPublished is the subscriber asking to be sent the retain flag
	// as the publisher set it (MQTT-3.3.1-13) rather than the 0 a live
	// delivery otherwise carries. It rides on the subscription for the same
	// reason `deletions` does: MQTT puts it on the SUBSCRIBE, per filter.
	retainAsPublished bool

	// identifier is the Subscription Identifier this filter was subscribed
	// with, and zero where the client sent none - which is not a value the
	// property may take, MQTT 5 bounding it to 1..268,435,455, so zero is
	// unambiguous.
	//
	// **MQTT 5 section 3.3.4 makes echoing it a MUST**, and a channel's
	// records are written by saguin rather than fanned out by the server,
	// so nothing attached it. The effect was one client on one connection
	// getting both behaviours - an identifier on broadcast, none on a
	// channel's records - and which of the two a message got depended on
	// whether an operator had placed a channel over that topic, which the
	// client is deliberately not told (*What a filter reaches*). A library
	// routing callbacks by identifier, which is what the field is for,
	// sends those records to its fallback handler or drops them.
	identifier int
}

// HoldStore is how a channel's store keeps an exactly-once publish between its
// PUBREC and its PUBREL, in the store the record will be kept in
// (store.HeldPublish): Hold at the PUBLISH, one ReleaseHold at the PUBREL
// that writes the record and forgets the hold together, DropHold where the
// exchange ends unreleased, and Holds for a start. Every channel store has
// them, on either provider.
type HoldStore interface {
	Hold(store.Exchange, store.Record, time.Time) error
	ReleaseHold(store.Exchange) (store.Record, bool, error)
	DropHold(store.Exchange) (bool, error)
	Holds() ([]store.HeldPublish, error)
}

// What the broker needs from whatever holds a channel's records. The
// interfaces are declared here, where they are used, rather than beside
// the implementation: nothing in the broker may depend on how the records
// are kept, and a store is free to offer more than this.
//
// Anything that stores a record or reads records back can fail once the
// records are on a disk, so it says so. A publish whose record was not
// stored is refused rather than acknowledged: acknowledged and lost is
// worse than refused.
//
// Next and Floor do not, because a store is the only writer of its own
// channels and can hold both in memory whatever it keeps the records on.
// Floor is read on the delivery path, where a failure has nothing useful
// to do with itself, and that is what the exception buys.
//
// How many records a channel holds is deliberately absent. Counting them
// costs a scan in any store that keeps them on a disk, and nothing here
// asks - a count is for an operator, through the administration API when
// there is one, not for the delivery path.
type LogStore interface {
	HoldStore
	Append(store.Record) (store.Record, error)

	// SetMaxBytes bounds what the channel holds. LatestStore deliberately
	// has no such method: RFC 0002 gives a latest channel no size bound, so
	// this is the table the compiler keeps rather than a rule somebody has
	// to remember.
	SetMaxBytes(int64)

	// Bytes is what the channel holds, for saguin_channel_bytes. Both
	// implementations keep it as a field and neither counts anything to
	// answer it (RFC 0005 "What a metric is allowed to cost").
	Bytes() int64
	ReadFrom(offset uint64) ([]store.Record, error)
	ReadFromN(offset uint64, max int) ([]store.Record, error)
	Next() uint64
	Floor() uint64

	// FirstAtOrAfter is what a seek by time resolves against: the earliest
	// offset carrying a record the broker received at or after a moment.
	// Both stores answer it by looking at every candidate rather than by
	// bisecting, because the receipt clock is the wall clock and a step
	// backwards from NTP leaves records whose times run the other way from
	// their offsets.
	FirstAtOrAfter(t time.Time) (uint64, bool, error)

	// Trim is retention: it removes the oldest records to bring the channel
	// under a size and to drop everything published before a moment,
	// advancing the floor over what went in the same operation. Either rule
	// is off at its zero value.
	Trim(before time.Time, maxBytes int64) (removed int, freed int64, err error)

	// A durable consumer's position belongs to the channel it points into,
	// not to the broker: it is stored where the records are, so that the
	// two cannot end up in different places with different lifetimes.
	Position(reader string) (store.Position, bool, error)
	SavePosition(store.Position) error
	DropPosition(reader string) (bool, error)

	// LowestPosition is the worst-placed consumer on this channel, and
	// against Floor it is the one alert RFC 0005 exists for: a floor above
	// it is data a consumer had not reached and retention has removed.
	// LowestPosition is the lowest offset any durable consumer has stored,
	// and how many consumers have stored one. Two answers from one pass:
	// the count is what saguin_channel_consumers reports, and asking for it
	// separately would walk the same map, or run a second query against the
	// same rows, per channel per scrape.
	LowestPosition() (lowest uint64, consumers int)

	// ListPositions is every stored position, furthest-behind first, capped
	// at limit, and how many there are in total. It is what
	// /v1/operations/consumers answers from, and is asked when a person
	// asks rather than on a scrape - which is why it may cost what the
	// catalogue may not.
	ListPositions(limit int) ([]store.Position, int, error)
}

type LatestStore interface {
	HoldStore
	Set(store.Record) (store.Record, error)
	Delete(topic string) (bool, error)
	Match(match func(topic string) bool) ([]store.Record, error)

	// MatchWithDeletions is Match, including the deletions Match leaves
	// out. It serves one reader: a copy of this channel on another broker,
	// which has to be told a topic is gone (RFC 0003).
	MatchWithDeletions(match func(topic string) bool) ([]store.Record, error)

	// Get is one topic's current value, and whether it has one. It is here
	// rather than left to Match with an exact predicate because the two
	// differ by more than style: Match reads the whole channel and decodes
	// all of it to return one record, and measured on ten thousand topics
	// that is 1.34ms in memory and 85ms on SQLite against a flat 86ns and
	// 43us. A point read on the publish path cannot be a scan.
	Get(topic string) (store.Record, bool, error)

	// Trim expires the current value of every topic that has gone quiet for
	// longer than the channel's period. There is no size rule and no floor:
	// a latest channel holds no history, so nothing holds a position into it
	// and there is no coordinate for a removal to invalidate.
	// Trim expires values and deletions on two clocks. A value lives as
	// long as it is the truth; a deletion only has to live long enough for
	// everything reading this channel to have seen it (RFC 0003).
	Trim(before, deletionsBefore time.Time) (removed int, freed int64, err error)

	// TrimExpired removes every value whose publisher's Message Expiry
	// Interval has run out (MQTT-3.3.2-5). It serves the retained store
	// only - on a `latest` channel the client's expiry never deletes
	// anything, because retention on a channel is the operator's
	// (invariant 2) - and it is on the interface rather than asserted for
	// at the call site so that a store that cannot answer it fails the
	// build instead of quietly never being swept.
	TrimExpired(now time.Time) (removed int, freed int64, err error)
}

// qosCeiling is the highest QoS this broker offers. One reader for the
// CONNACK and one for the refusal, so the two cannot disagree.
func (b *Broker) qosCeiling() byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxQoS
}

// qosCeilingFor is the highest QoS this client may use: the broker's, or 1
// where the acl_file denies it `qos2` (RFC 0002 "Taking a feature away").
// One reader for the CONNACK, the publish refusal and the subscription
// grant, so what a client is told and what it is held to cannot disagree.
//
// Never asked of an in-process client: saguin's own Will client carries no
// user name, so a `"*"` entry denying `qos2` would otherwise hold a Will to
// a ceiling its own client was allowed at CONNECT.
func (b *Broker) qosCeilingFor(cl *mqtt.Client) byte {
	c := b.qosCeiling()
	if c > 1 && !cl.Net.Inline && b.deniesFeature(cl, featureQoS2) {
		return 1
	}
	return c
}

// holdsFor is the store the exactly-once publishes held for where are in:
// a channel's own, or the broadcast log's for store.BroadcastLog - in each
// case the store the release writes the record to. nil where this broker has
// no such store.
func (b *Broker) holdsFor(where string) HoldStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.holdsForLocked(where)
}

// holdsForLocked is holdsFor for a caller holding mu.
func (b *Broker) holdsForLocked(where string) HoldStore {
	if h := b.holdWraps[where]; h != nil {
		return h
	}
	if where == store.BroadcastLog {
		// The drain's own log, whichever provider opened it: both keep
		// holds beside their records (store.HeldPublish).
		if d := b.bcast.Load(); d != nil {
			if d.holds != nil {
				return d.holds
			}
		}
		return nil
	}
	c := b.reg.Get(where)
	if c == nil {
		return nil
	}
	switch c.Type {
	case channel.Append:
		if lg := b.logs[where]; lg != nil {
			return lg
		}
	case channel.Latest:
		if lt := b.latest[where]; lt != nil {
			return lt
		}
	case channel.Queue:
		if q := b.queues[where]; q != nil {
			return q
		}
	}
	return nil
}

// WrapHolds has one store's exactly-once holds - a channel's, or the
// broadcast log's under store.BroadcastLog - go through wrap from now on:
// every hold, release, drop and listing the broker makes, and nothing else
// of the store. It exists for the tests of the hold, the release and a
// session's end, whose races cannot be seen from the wire until they are
// lost (brokertest.WrapHolds); nothing in the broker calls it. Called before
// any listener opens.
func (b *Broker) WrapHolds(where string, wrap func(HoldStore) HoldStore) {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.holdsForLocked(where)
	if h == nil {
		return
	}
	if b.holdWraps == nil {
		b.holdWraps = map[string]HoldStore{}
	}
	b.holdWraps[where] = wrap(h)
}

// WrapLog wraps an append channel's log beneath the wrapper that counts its
// failures, so a failure the wrapper injects is counted as the store's own
// would be. For a test, and only before the broker serves, as WrapHolds is
// used: the pumps read the log map without a lock, because nothing replaces
// a log once they run.
func (b *Broker) WrapLog(name string, wrap func(LogStore) LogStore) {
	if b.running.Load() {
		panic("WrapLog after Run: b.logs is read without b.mu and must be fixed by then")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	lg, ok := b.logs[name]
	if !ok {
		return
	}
	if c, ok := lg.(countingLog); ok {
		c.LogStore = wrap(c.LogStore)
		b.logs[name] = c
		return
	}
	b.logs[name] = wrap(lg)
}

// forgetHeld takes an exchange out of the held maps. The caller holds the
// client id's session lock, and not heldMu.
func (b *Broker) forgetHeld(e store.Exchange) {
	b.heldMu.Lock()
	defer b.heldMu.Unlock()
	if _, ok := b.held[e]; !ok {
		return
	}
	delete(b.held, e)
	if b.heldBy[e.Client]--; b.heldBy[e.Client] <= 0 {
		delete(b.heldBy, e.Client)
	}
}

// holdForRelease takes ownership of an exactly-once publish until its
// PUBREL arrives, and answers with the code the PUBREC should carry when it
// cannot.
//
// **Refusing here is refusing before ownership**, which is what the
// specification asks of a receiver: every check that could produce a
// forwarding failure is made before the PUBREC, and the PUBREC carries the
// outcome (section 4.3.3, note 1). `0x97` is the code for a bound, and the
// connection is left alone - a publisher at its allowance succeeds again as
// soon as one of its own exchanges completes.
func (b *Broker) holdForRelease(cl *mqtt.Client, t holdTarget, pk packets.Packet,
	rec store.Record) error {
	b.mu.Lock()
	offered, limit := b.maxQoS >= maxQoSWithStore, b.qos2MaxPerClient
	b.mu.Unlock()
	h := b.holdsFor(t.where)
	if !offered || h == nil {
		// Unreachable: a broker with no store advertises Maximum QoS 1 and
		// OnPacketRead disconnects a publish above it, so nothing carrying
		// QoS 2 gets this far. Kept because the alternative to a refusal
		// here is storing the record at the PUBLISH, which is the defect
		// this whole path exists to remove - and a check whose necessity
		// rests on another rule staying exactly as it is is not one to drop.
		return b.refuseNoQoS2Store(cl, pk)
	}

	// **Held under the client id's session lock, and by the id's owner
	// only.** A PUBLISH is read on its own connection and held by client id,
	// so one read before a clean start took the id and held after that
	// start's ending had dropped the old session's publishes landed in the
	// new session's identifier space. The new session's own publish under the
	// same identifier then read as its retransmit: it was answered PUBREC and
	// PUBCOMP and never stored, and its PUBREL published the ended session's
	// message instead. Under the lock the hold lands before the claim, and
	// the clean start's ending drops it, or it finds another connection
	// holding the id and holds nothing. That refusal is a reason code on an
	// MQTT 5 PUBREC, and OnPublish closes a 3.1.1 connection instead, whose
	// PUBREC cannot carry one. In-process publishers are left out: nothing
	// takes over their ids.
	if !cl.Net.Inline {
		unlock := b.lockSession(cl.ID)
		defer unlock()
		b.mu.Lock()
		holds := b.owner[cl.ID] == cl
		b.mu.Unlock()
		if !holds {
			b.log.Info("refused an exactly-once publish: another connection holds its client id",
				"client", b.limits.Loggable(cl.ID), "packet_id", pk.PacketID)
			return codeNotTheSession
		}
	}

	// **In the store of the channel it is for** (RFC 0003 "Exactly once"),
	// or the broadcast log's for a broadcast, so its release is one
	// operation in that one store. The maps say where, and count it against
	// the client's allowance across every store.
	e := store.Exchange{Client: cl.ID, PacketID: pk.PacketID}
	b.heldMu.Lock()
	_, repeat := b.held[e]
	count := b.heldBy[cl.ID]
	b.heldMu.Unlock()
	if repeat {
		// A PUBLISH re-sent before its PUBREC arrived is the same message
		// ([MQTT-4.3.3-10]): held once, answered as the first was.
		return nil
	}
	if limit > 0 && count >= limit {
		return b.refuseInflight(cl, pk)
	}
	now := time.Now()
	if err := t.hold(h, e, rec, now); err != nil {
		return t.refuse(pk, err)
	}
	b.heldMu.Lock()
	if b.held == nil {
		b.held, b.heldBy = map[store.Exchange]heldExchange{}, map[string]int{}
	}
	if _, ok := b.held[e]; !ok {
		b.held[e] = heldExchange{channel: t.where, at: now,
			retain: t.where == store.BroadcastLog && rec.Retain}
		b.heldBy[cl.ID]++
	}
	b.heldMu.Unlock()
	return nil
}

// holdTarget is where holdForRelease holds an exactly-once publish: the
// store's name in the held maps, how a hold there makes room, and how a
// refusal there is answered.
type holdTarget struct {
	where  string
	hold   func(h HoldStore, e store.Exchange, rec store.Record, now time.Time) error
	refuse func(pk packets.Packet, err error) error
}

// channelHold is a channel's store as a hold's target. The broadcast log
// gives way to it as to any write on its provider (withRoom).
func (b *Broker) channelHold(c *channel.Channel) holdTarget {
	return holdTarget{
		where: c.Name,
		hold: func(h HoldStore, e store.Exchange, rec store.Record, now time.Time) error {
			return b.withRoom(c.Storage, store.RecordSize(rec), "channel "+c.Name, func() error {
				return h.Hold(e, rec, now)
			})
		},
		refuse: func(pk packets.Packet, err error) error {
			return b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
		},
	}
}

// broadcastHold is the broadcast log's store as a hold's target. A hold there
// finds room the way the log's own append does (appendRoom): at a full
// provider its oldest messages go, each counted against the sessions owed it,
// until the hold fits or nothing is left to give. Then the PUBREC is 0x97,
// before ownership is taken - unlike a QoS 1 broadcast, which is
// acknowledged and lost to its sessions, because this publisher is still
// owed the answer.
func (b *Broker) broadcastHold(d *bdrain) holdTarget {
	return holdTarget{
		where: store.BroadcastLog,
		hold: func(h HoldStore, e store.Exchange, rec store.Record, now time.Time) error {
			for {
				err := h.Hold(e, rec, now)
				if !errors.Is(err, store.ErrFull) {
					return err
				}
				if d.giveUp(store.RecordSize(rec), theLog) == 0 {
					return err
				}
			}
		},
		refuse: func(pk packets.Packet, err error) error {
			return b.refuseStore("the broadcast log", d.provider, 0, pk, err)
		},
	}
}

// codeNotTheSession refuses an exactly-once publish from a connection that no
// longer holds its client id (holdForRelease).
var codeNotTheSession = packets.Code{
	Code:   packets.ErrUnspecifiedError.Code,
	Reason: "another connection holds this client id",
}

// codeTooManyInflight is the third thing `0x97` means on this broker, and
// the metric splits it out for the reason the other two are split: an
// operator seeing `quota exceeded` cannot otherwise tell a full channel
// from a publisher holding more unfinished exchanges than
// `broker.qos2.max_inflight_per_client` allows it, and the two have
// opposite answers.
//
// A plain packets.Code rather than a wrapped error, for the reason
// codeOverPublishRate is one: the substrate extracts it with a bare type
// assertion and panics on a wrapped one.
var codeTooManyInflight = packets.Code{
	Code: packets.ErrQuotaExceeded.Code,
	Reason: "holding as many unfinished exactly-once publishes as " +
		"broker.qos2.max_inflight_per_client allows this client",
}

// refuseInflight answers a client that has as many exactly-once exchanges
// open as its allowance permits.
//
// **The connection is left alone**, which is the same choice refuseRate
// makes: the client is inside a bound rather than misbehaving, and its next
// exchange succeeds the moment one of its own completes. Disconnecting
// would turn a publisher that pipelines a little too eagerly into a
// reconnect loop.
//
// There is no QoS 0 branch here and there cannot be: this is only reachable
// for a QoS 2 publish, which always has a PUBREC to carry the code.
//
// Debug rather than warn, for the reason refuseRate is: one line per
// refused publish floods a log under exactly the load the bound exists for,
// and the metric is where an operator sees it.
func (b *Broker) refuseInflight(cl *mqtt.Client, pk packets.Packet) error {
	b.log.Debug("refusing publish: the client holds its allowance of unfinished exactly-once publishes",
		"client", b.limits.Loggable(cl.ID),
		"topic", b.limits.Loggable(pk.TopicName),
		"packet_id", pk.PacketID)
	return codeTooManyInflight
}

// refuseNoQoS2Store answers an exactly-once publish on a broker that has
// nowhere to hold it. Unreachable while the ceiling and the store are set
// together by SetQoS2; see holdForRelease for why it is kept.
func (b *Broker) refuseNoQoS2Store(cl *mqtt.Client, pk packets.Packet) error {
	b.log.Warn("refusing an exactly-once publish: this broker has no store attached for it",
		"client", b.limits.Loggable(cl.ID),
		"topic", b.limits.Loggable(pk.TopicName))
	return packets.Code{
		Code:   packets.ErrImplementationSpecificError.Code,
		Reason: "this broker keeps no store for unfinished exactly-once publishes",
	}
}

// releaseHeld writes the record an exactly-once publish was holding, on the
// PUBREL that finishes it, and hands the packet on so the substrate answers
// PUBCOMP.
//
// **The order is the point.** processPubrel writes that PUBCOMP before it
// calls any hook, so a record stored from a later hook would be one the
// client had already been told was complete - invariant 16's rule that a
// record is durable before anybody is told about it, failing in the one
// direction that reports success over data nobody has.
//
// A PUBREL naming an exchange this broker is not holding is passed through
// untouched, and the substrate answers `0x92 Packet Identifier not found`.
// That is the honest answer: after a restart, or past
// `broker.qos2.expires_after`, there is no message to write.
func (b *Broker) releaseHeld(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		// An in-process publisher's QoS 2 is written at once and never held.
		return pk, nil
	}
	e := store.Exchange{Client: cl.ID, PacketID: pk.PacketID}

	// **Under the client id's session lock, and for the id's owner only.** A
	// PUBREL read on a connection that has been taken over is rejected and
	// never passed through: passed through, the old connection's engine
	// would answer PUBCOMP from its own table, and the client could take that
	// as done while the message waited for a release that never comes.
	// Under the lock the swap cannot interleave with an expiry or the
	// session's end, which take the same lock to forget an exchange.
	unlock := b.lockSession(cl.ID)
	b.mu.Lock()
	owns := b.owner[cl.ID] == cl
	b.mu.Unlock()
	if !owns {
		unlock()
		b.log.Info("refused a PUBREL: another connection holds its client id",
			"client", b.limits.Loggable(cl.ID), "packet_id", pk.PacketID)
		return pk, fmt.Errorf("%w: %s", packets.ErrRejectPacket, codeNotTheSession.Reason)
	}
	b.heldMu.Lock()
	at, held := b.held[e]
	b.heldMu.Unlock()
	if !held {
		// Released before - its PUBCOMP lost on the way - or never held:
		// the substrate answers from its own table, PUBCOMP for an exchange
		// it holds and 0x92 for one it does not. Every path that forgets a
		// hold without its release forgets the substrate's entry with it, so
		// a PUBCOMP here always follows a record that was stored.
		unlock()
		return pk, nil
	}
	broadcast := at.channel == store.BroadcastLog
	var c *channel.Channel
	if !broadcast {
		c = b.reg.Get(at.channel)
	}
	h := b.holdsFor(at.channel)
	var (
		rec   store.Record
		found bool
		err   error
	)
	if h != nil {
		// **A held broadcast that asked to be retained is retained first**,
		// and a retained store that refuses refuses the release: the hold
		// stays and no PUBCOMP says a value was kept that was not. Retained
		// after the swap instead, its refusal would come after the record
		// was written and nothing could take the PUBCOMP back. The swap is
		// the commit point, so everything that can fail goes before it.
		//
		// **The retry converges, because retainHeld is idempotent**: a swap
		// that fails after the value was retained leaves the hold, and the
		// next PUBREL writes the same value again and then swaps. A crash
		// between the two heals the same way, the hold outliving it. What
		// does not heal is a release refused and never sent again: the hold
		// expires or its session ends, and the value stays retained with no
		// delivery behind it (retainedStands).
		if at.retain {
			err = b.retainHeld(cl, h, e)
		}
		if err == nil {
			rec, found, err = h.ReleaseHold(e)
		}
	}
	if err != nil {
		unlock()
		// **Refused, and the message stays held**: the channel is at its own
		// max_bytes, or the store failed. No PUBCOMP, and the connection is
		// closed with the reason, so the client sends its PUBREL again on
		// the next one - by when the channel may have room (RFC 0003).
		code := packets.ErrImplementationSpecificError
		var answered packets.Code
		switch {
		case errors.Is(err, store.ErrFull):
			code = packets.ErrQuotaExceeded
		case errors.As(err, &answered) && answered == packets.ErrQuotaExceeded:
			code = packets.ErrQuotaExceeded
		}
		b.log.Warn("an exactly-once publish could not be released yet: it stays held",
			"client", b.limits.Loggable(cl.ID), "channel", at.channel, "packet_id", pk.PacketID,
			"reason", code.Reason, "error", err)
		b.disconnect(cl, code)
		return pk, fmt.Errorf("%w: %w", code, packets.ErrRejectPacket)
	}
	if !found {
		// The store holds nothing the maps said it did: its channel went,
		// or the two disagree. Nothing was stored, so the substrate's entry
		// goes too, and the PUBREL is answered 0x92 rather than completed.
		b.forgetHeld(e)
		b.forgetExchange(cl.ID, pk.PacketID)
		unlock()
		b.counted.qos2Abandoned.Add(1)
		b.log.Warn("an exactly-once publish was not held where it was recorded; its release stores nothing",
			"client", b.limits.Loggable(cl.ID), "channel", at.channel, "packet_id", pk.PacketID)
		return pk, nil
	}
	b.forgetHeld(e)
	unlock()

	// Handed to its readers after the lock, since nothing publishes holding
	// one (TestNothingPublishesUnderASessionLock).
	if broadcast {
		b.releaseBroadcast(cl, rec)
		return pk, nil
	}
	b.announceStored(c, rec)
	b.fanOutReleased(c, rec, b.publisherOf(cl))
	return pk, nil
}

// holdsBroadcast reports whether an exactly-once publish is a broadcast held
// for its release (holdBroadcast) rather than delivered at its PUBLISH: a
// socket client's QoS 2, on a topic no channel claims and outside the
// server's `$` space, with a broadcast log whose store keeps holds.
func (b *Broker) holdsBroadcast(cl *mqtt.Client, pk packets.Packet) bool {
	return heldForRelease(cl, pk) && !strings.HasPrefix(pk.TopicName, "$") &&
		b.reg.Resolve(pk.TopicName) == nil && b.holdsFor(store.BroadcastLog) != nil
}

// holdBroadcast holds an exactly-once broadcast in the broadcast log's store
// until its PUBREL (RFC 0003 "Exactly once"), as a channel's store holds a
// channel's publish, and answers what the PUBREC should carry.
//
// **Held whether or not a session that outlives its connection wants it**,
// unlike a QoS 1 broadcast, which the log keeps only for one
// (keepBroadcast): what has to outlive a restart here is the exchange, so
// that a publisher that re-sends its PUBLISH after one is answered as a
// repeat rather than delivered twice. At the release the message is counted
// for the sessions it reaches, and one reaching none leaves the log at once.
//
// Nothing is delivered or retained at the PUBLISH: CodeSuccessIgnore is what
// tells the substrate so, as for a channel's held publish.
func (b *Broker) holdBroadcast(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	d := b.broadcastDrain()
	if d == nil {
		return pk, nil
	}
	if code := b.holdForRelease(cl, b.broadcastHold(d), pk, b.record(pk, nil, bridgeOf(b, cl), b.publisherOf(cl))); code != nil {
		return pk, code
	}
	return pk, packets.CodeSuccessIgnore
}

// retainHeld writes a held broadcast's value to the retained store, as its
// PUBLISH would have: the record it was held as, with the retain flag it
// carried. The caller holds the client id's session lock.
func (b *Broker) retainHeld(cl *mqtt.Client, h HoldStore, e store.Exchange) error {
	holds, err := h.Holds()
	if err != nil {
		return err
	}
	for _, p := range holds {
		if p.Exchange == e {
			pk := publishOf(p.Record, cl.ID, time.Now())
			pk.FixedHeader.Retain = true
			return b.keepRetained(cl, pk)
		}
	}
	return nil // gone meanwhile: the release finds nothing held
}

// releaseBroadcast hands a released broadcast to whoever it reaches, as a
// QoS 2 broadcast's PUBLISH was handed on before it was held: the record is
// pending on the drain at the offset the swap gave it, so the selection
// counts it for the sessions that outlive their connections
// (OnSelectSubscribers, chosen), and the substrate delivers it to the rest.
// Its origin is the publisher, for No Local.
//
// **A crash between the swap and the count loses nothing**: the record is in
// the log, and a start counts every message at or after a session's cursor
// that one of its subscriptions matched (rebuild).
func (b *Broker) releaseBroadcast(cl *mqtt.Client, rec store.Record) {
	d := b.broadcastDrain()
	if d == nil || b.srv == nil {
		return
	}
	d.pending.Store(rec.Offset, rec)
	pk := publishOf(rec, cl.ID, time.Now())
	pk.FixedHeader.Retain = rec.Retain
	pk.LogOffset = rec.Offset
	b.srv.PublishToSubscribers(pk)
}

// publishOf is the packet a stored record goes out as: its topic, payload,
// headers and properties, at QoS 2, from origin.
func publishOf(rec store.Record, origin string, now time.Time) packets.Packet {
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: maxQoSWithStore},
		TopicName:   rec.Topic,
		Payload:     rec.Payload,
		Origin:      origin,
		Created:     now.Unix(),
	}
	for _, h := range rec.Headers {
		pk.Properties.User = append(pk.Properties.User, packets.UserProperty{Key: h.Key, Val: h.Value})
	}
	applyProps(&pk, rec, now, false)
	return pk
}

// fanOutReleased serves a released record to the shared subscribers of its
// channel, which is what fanOut does for every other publish to an `append`
// or `latest` channel and could not do for this one: it was held at its
// PUBLISH, when there was no record to serve.
//
// The packet is built from the record rather than kept from the PUBLISH,
// because the record is what was stored - its properties and its headers,
// the reserved ones already stripped - and nothing about the release packet
// says any of it. It carries no offset, as a shared delivery never does.
func (b *Broker) fanOutReleased(c *channel.Channel, rec store.Record, origin string) {
	if b.srv == nil || c.Type == channel.Queue {
		return
	}
	b.srv.PublishToSubscribers(publishOf(rec, origin, time.Now()))
}

// isSuccessIgnore reports whether a publish path answered "accepted, and
// nothing further should fan it out" rather than refusing.
//
// Compared as a value rather than by its code, because
// packets.CodeSuccessIgnore carries 0x00: told apart by the number alone it
// is indistinguishable from success itself, and by errors.Is it would not
// match at all, since the substrate returns it unwrapped.
func isSuccessIgnore(err error) bool {
	var code packets.Code
	return errors.As(err, &code) && code == packets.CodeSuccessIgnore
}

// SessionStore keeps the state of every MQTT session: a record for each
// session, its subscriptions, and its in-flight table. Declared here, where
// it is used, for the reason the other store interfaces are. store.Sessions and
// the sqlite provider's Sessions both answer it, and both are driven through one
// set of oracles in internal/store/sqlite.
type SessionStore interface {
	// Save keeps a session, replacing what was kept under its client id.
	// ErrFull leaves the previous one as it was.
	Save(store.Session) error
	// SaveWithShareCursors keeps a session as Save does and gives each of
	// groups, shared groups whose filters it holds, a cursor on the
	// broadcast log at cursor where it has none, in one write: both or
	// neither.
	SaveWithShareCursors(sess store.Session, cursor uint64, groups []string) error
	Get(string) (store.Session, bool, error)
	// Disconnected records when a session's client went away, the expiry
	// interval in force as it went and, with dropWill, that its Will is
	// withdrawn: one write, with nothing read first by the caller.
	// store.ErrNoSession where there is no session.
	Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error
	All() ([]store.Session, error)
	// Begin ends whatever is kept under a client id, as Drop does, and keeps
	// the new session's record in its place, in one write; nil keeps none.
	// A session that begins new begins with nothing an ended one left.
	Begin(client string, held []string, next *store.Session) (store.Dropped, error)
	// Drop ends a session with every message it is owed, and with the
	// cursors of the shared groups it was the last member of, and says what
	// became of what its groups had handed it (store.Dropped). The groups
	// given are the ones the ending session held, which its record may no
	// longer say (store.Sessions.Drop).
	Drop(string, []string) (store.Dropped, error)
	// DropExpired ends the sessions whose expiry has passed, as Drop ends
	// one, and names them, with what their endings did together. One keep
	// answers true for is left for its caller to end, which is how a start
	// keeps a record until the Will its ending owes is published.
	DropExpired(time.Time, func(store.Session) bool) ([]string, store.Dropped, error)
	Len() int
	Bytes() int64
}

type QueueStore interface {
	HoldStore
	Enqueue(store.Record) (store.Record, error)
	SetMaxBytes(int64)
	Bytes() int64

	// Depth is the unresolved work and how much of it is out with a worker.
	// A queue's depth is the one number in RFC 0005's catalogue that cannot
	// be derived from the free ones: resolution removes a record from the
	// middle rather than the front, so `next - floor` says nothing here.
	Depth() (total, inflight int)
	SetBackoff(store.Backoff)
	Offer(max int, now time.Time) ([]store.Offered, error)
	Lease(h store.Held, now time.Time, visibility time.Duration) (store.Item, bool, error)
	Resolve(h store.Held) (store.Item, bool, error)
	Release(h store.Held, now time.Time, answered bool, maxAttempts int, dl store.DeadLetter) (store.Outcome, bool, error)
	ExpiredLeases(now time.Time) ([]store.Held, error)

	// Unresolved is the work this queue holds, oldest first, capped at
	// limit, and how many there are in total. It leases nothing: the only
	// other way to see a queue's contents is to consume them, and a viewer
	// that consumed would take work from the workers it was sent to
	// diagnose. It carries no payloads.
	Unresolved(limit int) ([]store.Item, int, error)

	// ExpireOlderThan dead-letters the waiting jobs published before a
	// moment, which is the half of `job_expires_after` no worker can
	// enforce: a queue whose workers have all gone away is never offered
	// anything, and expiry is for work nobody is processing. Records out
	// with a worker are left alone - removing one is removing a record in
	// flight to a consumer.
	ExpireOlderThan(before time.Time, dl store.DeadLetter) ([]store.Outcome, error)
}

type delivery struct {
	channel string
	offset  uint64
	epoch   uint64
	holder  string

	// conn is the connection this delivery was handed to, learned in
	// OnQosPublish beside the holder.
	//
	// **The holder is a client id, and a client id is not a connection.**
	// A teardown asking "does this delivery hold my id" answers yes for the
	// connection that took the id over - so a superseded teardown returned
	// a job the live worker was still doing, the queue offered it again,
	// and the two answers then raced with invariant 3 discarding whichever
	// arrived second as superseded: two live deliveries of one record
	// (invariant 4), the work done twice, one result thrown away, and every
	// instrument reporting success. Measured: a live worker holding `job-1`
	// was offered it a second time after a stale teardown returned it.
	//
	// It is the same distinction the ownership map draws one layer up, and
	// it is needed here rather than solved there: a superseded connection's
	// own deliveries must still go back, because no acknowledgement for
	// them can ever arrive - they are invariant 7's stranded case, returned
	// with the attempt unspent - so the teardown cannot simply stop when it
	// loses the id. It has to return its own and nothing else.
	//
	// Nil until the substrate has chosen a worker, which is a delivery
	// offered and not yet handed over. A resumed session re-sends what it
	// held and fires OnQosPublish again, so this follows the record onto
	// the connection that now carries it.
	conn *mqtt.Client

	// packetID is the identifier the server gave this delivery on the
	// holder's connection, learned in OnQosPublish. It is what lets saguin
	// ask the one question a Delivering record cannot otherwise answer:
	// whether the packet was ever put on the wire (returnUnsent).
	packetID uint16
}

// held is how the store names this delivery. The epoch travels with it on
// every operation, so one that has been superseded changes nothing
// (invariant 3).
func (d *delivery) held() store.Held {
	return store.Held{Offset: d.offset, Epoch: d.epoch, Holder: d.holder}
}

// pending is one append record written to a consumer at QoS 1 and not yet
// acknowledged. A PUBACK carries only a packet identifier, so which offset
// it confirms has to be remembered here.
type pending struct {
	// conn is the connection this packet was registered on.
	//
	// **The key here is a client id and a packet identifier, and neither
	// names a connection.** Two connections can hold the same id - one
	// superseding the other - and each allocates packet identifiers from
	// its own sequence starting at 1, so a successor's first packets
	// collide with its predecessor's by construction rather than by
	// coincidence. A stale connection's write path then overwrites the
	// live session's record on registration, and its abandon deletes it
	// outright; the successor's PUBACK afterwards finds nothing, takes the
	// path for a packet saguin never sent, and that path does not resume a
	// queued current-state snapshot.
	//
	// So every read of this map that acts on the record asks whose it is.
	// It is the same distinction delivery.conn draws for queue work, and
	// the same one the ownership map draws for everything keyed by client
	// id.
	conn    *mqtt.Client
	channel string
	offset  uint64
	latest  bool   // a latest value rather than an append record
	gen     uint64 // the cursor era this delivery belongs to; see cursor.gen
}

// cursor is a consumer's place in one append channel while records are in
// flight to it.
type cursor struct {
	next        uint64          // one past the highest offset written
	outstanding map[uint64]bool // written, awaiting PUBACK

	// seeking is how many seeks have moved this cursor and not yet written
	// their reply. While it is above zero no batch is served from it
	// (pumpPlanLocked), so no record from the new position reaches the
	// consumer before the reply naming it (RFC 0003 "Moving a consumer's
	// position"). A count, not a flag: a seek read on a connection taken
	// over and handled after the takeover can be in handleSeek beside the
	// new connection's own, for the same consumer. Guarded by cmu.
	seeking int

	// positioned says this consumer has somewhere to lose: it resumed a
	// stored position, or it has read at least one record. Only then can
	// the retention floor passing it be the loss invariant 1 describes.
	//
	// **A subscription that has never read anything cannot have lost
	// records**, and counting it as though it had is how the one alert RFC
	// 0005 builds the catalogue for came to fire on every arrival at any
	// channel older than its retention.
	positioned bool

	// pumping says a pump is already running for this consumer and channel,
	// and repump that something changed while it ran. Guarded by the
	// consumer's cmu, as every field here is (claimPump, drainPump).
	pumping bool
	repump  bool

	// wireWait is the size of the record at wireWaitAt, which this cursor
	// stopped on because its session's share of the wire had no room for it
	// (wireHasRoom). pumpPlanLocked asks the wire for that size before the
	// store is read again, so a wake that cannot write reads nothing.
	wireWait   int64
	wireWaitAt uint64

	// expiresIn is how long this consumer's session - and so its stored
	// position - outlives its connection. Kept here because the flusher
	// writes the position long after the packet that set it.
	expiresIn time.Duration

	// gen says which era of this consumer's position the records in flight
	// belong to. A seek ends an era: it discards what is outstanding, moves
	// next to where the consumer asked to be, and raises this - after which
	// every delivery registered before it stops counting against the
	// position (RFC 0003 "Moving a consumer's position").
	//
	// It is what lets a seek apply to a stream already running. The records
	// already on the wire are acknowledged afterwards, and each PUBACK
	// removes an offset from outstanding - including an offset the seek has
	// since re-sent under a fresh packet identifier, whose own PUBACK has
	// not arrived. The position then advances over a record this consumer
	// is still holding, silently and in the direction that skips, which is
	// the one failure a position must never produce.
	gen uint64

	// saved is the offset last written to the store, so the flusher can
	// tell a position that has moved from one that has not. Zero means
	// nothing has been written for this consumer yet, and offsets start at
	// one, so the first real position is never mistaken for it.
	saved uint64

	// pending, pendingSince and prunedBelow are what lets a consumer on a
	// narrow filter step over records it declined without reading them -
	// see skipDeclinedLocked. pending is the offsets above pendingSince
	// that pumpAll found one of this consumer's filters matching, in
	// order; offsets below prunedBelow have been dropped from it.
	pending      []uint64
	pendingSince uint64
	prunedBelow  uint64

	// con is the consumer this cursor belongs to, whose cmu guards every
	// field above.
	con *consumer
}

// consumer is one client's delivery state, under its own lock, cmu.
//
// cursors is its in-flight bookkeeping for each append channel, by channel
// name, bounded by the client's Receive Maximum, which is what makes it safe
// to keep per consumer (invariant 13). inflight correlates its PUBACKs back
// to the record each confirms - append or latest - by packet identifier,
// since that is all a PUBACK carries.
//
// **A lock of its own, so one consumer's deliveries stop queueing behind
// every other consumer's.** Claiming a drain, planning and committing a
// batch, and an acknowledgement's bookkeeping take cmu and not b.mu. With
// b.mu for all of it, a record every one of 5,000 subscribers takes cost
// four turns of the broker-wide lock per subscriber, and the longest wait
// for it was 45-63ms (BenchmarkPublishAtScale/wide); a consumer's own lock
// is contended only by that consumer's own drain and acknowledgements.
//
// **b.positions, then b.mu, then cmu; never b.mu while cmu is held.** Code
// holding b.mu may take a consumer's cmu, and code holding cmu may not reach
// b.mu by any path - TestNoClientLockSectionReachesTheBrokerLock walks the
// source for that. Every field below is read and written under cmu; b.mu
// alone is not enough, since the pump reads them without it.
type consumer struct {
	cmu      sync.Mutex
	cursors  map[string]*cursor
	inflight map[uint16]pending
	// plans is what this client's subscriptions ask of each append channel,
	// copied out of subs and partitions by replanLocked - see channelPlan.
	plans map[string]*channelPlan
	// hasQueues says whether an acknowledgement from this client may be
	// owed a queue offer, which is decided under b.mu - so that an
	// acknowledgement owed none never takes it. replanLocked's.
	hasQueues bool
	// latest is what this client is owed on each latest channel and has not
	// yet been written: the remainder of a current-state snapshot, and live
	// values published since. One entry per topic - a newer value replaces
	// an older one (enqueueLatest) - so it is bounded by the number of topics
	// the subscription reaches, the bound the snapshot always had, and it
	// goes with the connection (forgetConsumer; invariant 13). The retained
	// store's list is one entry per topic and subscription identifier, for
	// the same reason (deliverRetained). Held in offset order, which is the
	// order it is written in.
	//
	// **Here, under cmu, and not under b.mu**, which is where it was first
	// put and where it cost a broker-wide acquisition per value per
	// subscriber: gateway latest delivered 64% of what HEAD delivered at 10k
	// msg/s with half the broker idle, and b.mu carried 567s of lock wait in
	// 15s. It is append's split, applied to the path that did not have it.
	latest map[string][]pendingValue
	// latestPruneAt is how long each channel's list grows before runLatest
	// looks through it for values the channel's retention has removed:
	// twice what was left last time, and never below latestPruneFloor.
	latestPruneAt map[string]int
	// draining is the claim on each connection's drain of one latest
	// channel, present while it runs: one drain per connection and channel,
	// so values are written in the order they are held (invariant 16).
	draining map[latestDrainKey]struct{}
	// gone is set when this entry is dropped, so that anything still
	// holding it - a drain, an acknowledgement - stops rather than writing
	// into state a later consumer on the same client id no longer shares.
	gone bool
}

// lookupConsumer is a client's delivery state, or nil, without b.mu. The
// map is written only under b.mu, and what it holds is read under the
// entry's cmu, which re-checks gone.
func (b *Broker) lookupConsumer(id string) *consumer {
	if v, ok := b.consumers.Load(id); ok {
		return v.(*consumer)
	}
	return nil
}

// consumerLocked is a client's delivery state, made if it has none.
//
// The caller holds b.mu.
func (b *Broker) consumerLocked(id string) *consumer {
	if con := b.lookupConsumer(id); con != nil {
		return con
	}
	con := &consumer{cursors: map[string]*cursor{}, inflight: map[uint16]pending{}}
	b.consumers.Store(id, con)
	return con
}

// forgetConsumerLocked drops a client's delivery state outright, and marks
// it so that whatever still holds it stops.
//
// The caller holds b.mu, and not the consumer's cmu.
func (b *Broker) forgetConsumerLocked(id string) {
	con := b.lookupConsumer(id)
	if con == nil {
		return
	}
	con.cmu.Lock()
	con.gone = true
	con.cmu.Unlock()
	b.consumers.Delete(id)
}

// dropCursorsLocked forgets a client's cursors and keeps what it has in
// flight, whose PUBACKs may still arrive.
//
// The caller holds b.mu, and not the consumer's cmu.
func (b *Broker) dropCursorsLocked(id string) {
	con := b.lookupConsumer(id)
	if con == nil {
		return
	}
	con.cmu.Lock()
	con.cursors = map[string]*cursor{}
	con.cmu.Unlock()
	b.tidyConsumerLocked(id, con)
}

// inflightLocked is what a client's packet in flight confirms. The caller
// holds b.mu, and not the consumer's cmu.
func (b *Broker) inflightLocked(id string, packetID uint16) (pending, bool) {
	con := b.lookupConsumer(id)
	if con == nil {
		return pending{}, false
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	p, ok := con.inflight[packetID]
	return p, ok
}

// setInflightLocked records what a client's packet in flight confirms. The
// caller holds b.mu, and not the consumer's cmu.
func (b *Broker) setInflightLocked(id string, packetID uint16, p pending) {
	con := b.consumerLocked(id)
	con.cmu.Lock()
	con.inflight[packetID] = p
	con.cmu.Unlock()
}

// deleteInflightLocked forgets one of a client's packets in flight. The
// caller holds b.mu, and not the consumer's cmu.
func (b *Broker) deleteInflightLocked(id string, packetID uint16) {
	con := b.lookupConsumer(id)
	if con == nil {
		return
	}
	con.cmu.Lock()
	delete(con.inflight, packetID)
	con.cmu.Unlock()
	b.tidyConsumerLocked(id, con)
}

// tidyConsumerLocked drops a client's entry once it holds nothing, so that
// the map is bounded by the clients with something to remember
// (invariant 13).
//
// The caller holds b.mu, and not the consumer's cmu.
func (b *Broker) tidyConsumerLocked(id string, con *consumer) {
	con.cmu.Lock()
	// **What pins a latest subscriber's record is read, not recorded**: a
	// value waiting, or a drain writing one. A flag set beside the list and
	// cleared somewhere else could disagree with it, and a record tidied
	// with values still waiting takes them with it silently.
	empty := len(con.cursors) == 0 && len(con.inflight) == 0 && len(con.plans) == 0 &&
		!con.hasQueues && len(con.latest) == 0 && len(con.draining) == 0
	if empty {
		con.gone = true
	}
	con.cmu.Unlock()
	if empty {
		b.consumers.Delete(id)
	}
}

// channelPlan is one client's subscriptions to one append channel, in the
// order subs holds them: everything a batch for it needs to know about them.
//
// **A copy, and a third one.** byTopic and members are subs turned round;
// this is subs cut down to what a delivery reads, so that reading it does
// not need subs' lock. Like the other two it is written by one function,
// replanLocked, from subs and partitions, wherever either changes, and a
// property test holds it to them.
type channelPlan struct {
	filters []planFilter
	qos     byte // the highest a matching subscription was granted
}

type planFilter struct {
	pumpFilter
	retainAsPublished bool
	identifier        int
	spans             bool
}

// pumpFilters is the plan's filters as a batch matches them.
func (p *channelPlan) pumpFilters() []pumpFilter {
	out := make([]pumpFilter, len(p.filters))
	for i, f := range p.filters {
		out[i] = f.pumpFilter
	}
	return out
}

// retainAsPublished answers whether any filter reaching the record asked for
// Retain As Published, which decides whether the delivery carries the flag
// the publisher set (MQTT-3.3.1-13). identifiers and spans answer, for the
// same topic, what identifiersForLocked and spansChannelsLocked answer from
// subs.
//
// **Any rather than all**, and the alternative is worse in the direction
// that matters. A client holding two filters that both reach a record - the
// ordinary shape, since a replay serves everything either matches - would
// otherwise have the flag decided by whichever list order won, which is a
// delivery that changes with a map. Answering "it asked, on one of them"
// gives the publisher's own flag to a client that said it wanted it, and
// the flag is information rather than an instruction: the record is the
// same record either way.
func (p *channelPlan) retainAsPublished(topic string) bool {
	for _, f := range p.filters {
		if f.retainAsPublished && channel.Matches(f.filter, topic) {
			return true
		}
	}
	return false
}

func (p *channelPlan) identifiers(topic string) []int {
	var ids []int
	for _, f := range p.filters {
		if f.identifier != 0 && channel.Matches(f.filter, topic) {
			ids = append(ids, f.identifier)
		}
	}
	sort.Ints(ids)
	return ids
}

func (p *channelPlan) spans(topic string) bool {
	for _, f := range p.filters {
		if f.spans && channel.Matches(f.filter, topic) {
			return true
		}
	}
	return false
}

// replanLocked rebuilds a client's channel plans from subs and partitions,
// and is the only thing that writes them. Every writer of either calls it.
//
// The caller holds b.mu.
func (b *Broker) replanLocked(id string) {
	var plans map[string]*channelPlan
	hasQueues := false
	for _, s := range b.subs[id] {
		if s.channel.Type == channel.Queue {
			hasQueues = true
		}
		if s.channel.Type != channel.Append {
			continue
		}
		if plans == nil {
			plans = map[string]*channelPlan{}
		}
		p := plans[s.channel.Name]
		if p == nil {
			p = &channelPlan{}
			plans[s.channel.Name] = p
		}
		p.filters = append(p.filters, planFilter{
			pumpFilter:        pumpFilter{s.filter, b.partitions[id][s.filter], s.noLocal},
			retainAsPublished: s.retainAsPublished,
			identifier:        s.identifier,
			spans:             s.spans,
		})
		if s.qos > p.qos {
			p.qos = s.qos
		}
	}
	con := b.lookupConsumer(id)
	if con == nil && plans == nil && !hasQueues {
		return
	}
	if con == nil {
		con = b.consumerLocked(id)
	}
	con.cmu.Lock()
	con.plans, con.hasQueues = plans, hasQueues
	con.cmu.Unlock()
	if plans == nil {
		b.tidyConsumerLocked(id, con)
	}
}

// pendingCap bounds a consumer's pending list (invariant 13). One that
// fills is started again from what has been notified so far, which only
// means the records between are read rather than stepped over.
const pendingCap = 256

// watermark is how far announcing has run for an append channel, or
// counting for the broadcast drain: every offset up to through has been
// through it, and maxSeen is the highest that has.
//
// **Contiguity is the point.** A record is stored under the log's lock and
// announced by pumpAll under b.mu, separately, so two publishers' records
// can be announced in the other order: the later one's pumpAll runs while
// the earlier's has not. Only offsets up to through are known to have been
// matched against every consumer, so only they can be stepped over; the
// store's head says nothing about that.
//
// **early holds what was announced above a gap as runs, not offsets.** Each
// gap between two runs is an offset still being announced, by the goroutine
// handling its publish, and a client's read loop handles one packet at a
// time: so there are never more runs than publishes in progress at once,
// however long a gap stays open (invariant 13). Held as offsets, a gap open
// while 4,096 later records were announced - one publisher descheduled
// between its append and its announcement while the others went on - was
// bounded instead by stopping for good: a channel never stepped over
// anything again, and the broadcast drain's cursors froze, so a restart sent
// every session everything since, and a subscription made after that moment
// was owed what was published before it (RFC 0003 "Broadcast").
type watermark struct {
	// through and maxSeen are written under b.mu (d.mu for the drain), by
	// seen and done, and read under a consumer's cmu alone, so they are
	// atomics. early is only ever touched under the writer's lock.
	through, maxSeen atomic.Uint64
	early            []run
}

// run is offsets from through to, all announced, with a gap below from.
type run struct{ from, to uint64 }

// seen records that an offset is being announced: maxSeen moves up to it.
//
// **Before any pending list is written, not after.** A consumer's list is
// started above maxSeen, and that can happen under its cmu alone - a new
// cursor - while an announcement is under way. With maxSeen already past
// the record, a list started then does not claim to cover it; and one
// started before it is in the announcement's loop and gets the record on
// its list. Either way the record reaches the consumer.
//
// The caller holds b.mu.
func (w *watermark) seen(off uint64) {
	if off > w.maxSeen.Load() {
		w.maxSeen.Store(off)
	}
}

// done records that an offset's announcement is complete, every pending
// list it belongs on having it: through moves up while it is contiguous.
//
// The caller holds b.mu.
func (w *watermark) done(off uint64) {
	through := w.through.Load()
	if off <= through {
		return
	}
	if off != through+1 {
		w.early = addToRuns(w.early, off)
		return
	}
	// Runs are kept apart by a gap each, so only the first can join.
	if len(w.early) > 0 && w.early[0].from == off+1 {
		off = w.early[0].to
		w.early = slices.Delete(w.early, 0, 1)
	}
	w.through.Store(off)
}

// addToRuns puts off into runs, sorted and kept apart by a gap each, joining
// the run on either side it closes the gap to.
func addToRuns(runs []run, off uint64) []run {
	i := sort.Search(len(runs), func(i int) bool { return runs[i].to >= off })
	if i < len(runs) && runs[i].from <= off {
		return runs // already announced
	}
	below := i > 0 && runs[i-1].to+1 == off
	above := i < len(runs) && runs[i].from == off+1
	switch {
	case below && above:
		runs[i-1].to = runs[i].to
		return slices.Delete(runs, i, i+1)
	case below:
		runs[i-1].to = off
	case above:
		runs[i].from = off
	default:
		runs = slices.Insert(runs, i, run{off, off})
	}
	return runs
}

// pendingValue is one value waiting for a subscriber's in-flight window,
// carrying the Subscription Identifier it has to go out under.
//
// **The identifier belongs to the value and not to the drain**, which is
// the whole reason this is a struct rather than a store.Record. A drain
// sends what fits and comes back on the next acknowledgement, and the
// caller that comes back is OnQosComplete, which knows the channel and the
// packet and has never heard of the SUBSCRIBE that started it. Told the
// identifier once per drain, it had none to give: the first retained value
// carried the identifier and every one after it arrived bare, so a client
// routing by identifier got its first value on the right handler and the
// rest on the fallback. Measured at Receive Maximum 1: [9], then nothing,
// three times over.
//
// It is also what one pending list per client can hold two subscriptions'
// values at once - the retained store keeps one per topic and subscription
// identifier, so one topic can wait once for each - so even a drain that was
// told an identifier would give the wrong one to the leftovers of the
// subscription before it.
//
// **And it carries how it is to be sent**, since a live value and a snapshot
// value wait in the same list: a live value goes at the QoS of the
// subscription that matched and with RETAIN only for Retain As Published,
// where a snapshot value always carries RETAIN (RFC 0003), and a live value
// carries what publishLatest resolved when it chose the subscriber.
type pendingValue struct {
	rec   store.Record
	ident int
	qos   byte
	// retain is the flag it goes out with.
	retain bool
	// live says it is a change published while the subscriber was
	// connected, rather than part of the state it was handed on subscribe or
	// resume. A live value refused by the acl_file is skipped and recorded as
	// missed; a snapshot value refused waits (runLatest).
	live bool
	// carry is what the write would otherwise take b.mu again to learn. Nil
	// for a snapshot value. See latestCarry.
	carry *latestCarry
	// head is the topic's newest offset handed out, for a live value: one
	// found below it when it is queued has been overtaken (latestHead).
	head *latestHead
}

// latestDrainKey names one connection's drain of one latest channel. By
// connection rather than client id: a session taken over by a new connection
// must not wait for the old one's drain, which ends at its first failed write.
type latestDrainKey struct {
	cl      *mqtt.Client
	channel string
}

// latestPos is where a consumer has got to on a `latest` channel.
//
// **Two numbers, because one of them was the wrong one.** The mark used to
// be `seen` alone - the highest offset acknowledged - and a `latest`
// channel dropped a live update whose subscriber's in-flight window was
// full, by design, because buffering it was the unbounded per-subscriber
// queue this design refuses everywhere. (It now waits in the pending list,
// one value per topic, which is bounded; see publishLatest.) The dropped
// value kept its own, lower offset. An update to a *different* topic is then acknowledged, the mark
// moves past it, and the resume filter removes it for ever: a consumer
// holding a view of current state with a topic missing from it, told
// nothing, which is invariant 1 reached on the channel type whose whole job
// is to be current.
//
// The rule this restores is not a new one. An `append` consumer's position
// is the lowest offset *not yet delivered* and never the highest delivered,
// and the comment on lowestUnacknowledged below says why in what reads as a
// description of this defect - reached there by out-of-order PUBACKs rather
// than by a drop. Both channel types issue offsets from one counter per
// channel, so the rule transfers as it stands.
//
// `latest` needs one number rather than a set to obey it. Anything sent
// and unacknowledged is resent by the session itself on resume, and a
// session that expires drops this whole entry - so what is left is the
// records that were undelivered and are not coming back, and the lowest of
// them is all that has to be remembered. The position is then
// min(seen, missed-1), which is "the lowest not yet delivered" arrived at
// from the other side.
//
// **There are two populations of those, and the first version of this
// counted one.** A value the broker skipped - once for a full window, now
// for an acl_file refusal (missedLatest) - is the obvious one. The second is
// a value still sitting in the pending list when the consumer goes away,
// snapshot or live, which forgetConsumer throws out -
// undelivered, not coming back, and for a while not recorded, so a resume
// stepped over it whenever its offset was below what had been
// acknowledged. Both now set `missed` through the same field; see
// forgetConsumer for how the second one is caught.
type latestPos struct {
	// seen is the highest offset this consumer has acknowledged.
	//
	// It is not what a resume is served from - sent is, below - and the two
	// are kept apart rather than collapsed because they answer different
	// questions. This one is "what does the consumer definitely hold".
	seen uint64
	// sent is the highest offset written to this session at QoS 1, which is
	// what a resume is served from.
	//
	// **A value sent and not acknowledged is re-sent by the session, so
	// saguin must not send it again.** Resuming from `seen` did: the
	// session's own retransmission arrived marked DUP, and the snapshot
	// served from `seen` arrived beside it carrying the same value with no
	// DUP flag, on every reconnect. The `append` half of the same defect is
	// described on forgetConsumer.
	//
	// **QoS 0 is not recorded here, and that is the whole reason this is
	// about writes at QoS 1 rather than writes.** MQTT keeps no QoS 0
	// packet in the session and re-sends none of them, so a value sent at
	// QoS 0 and lost with the link is not coming back from anywhere;
	// stepping over it would leave the subscriber holding a view of current
	// state with a topic missing from it and nothing saying so. A QoS 0
	// subscriber resumes exactly where it did before.
	sent uint64
	// missed is the lowest offset dropped for it because its window was
	// full, or zero for none. A resume never starts above it, so a value
	// dropped is a value re-sent rather than a value skipped.
	//
	// It is cleared by a new SUBSCRIBE, which serves the whole of current
	// state and so answers everything it was standing in for.
	missed uint64
}

// resumeFrom is the offset a resumed session is served from: everything
// above it, and nothing at or below.
//
// A consumer that has dropped nothing resumes from what it acknowledged,
// which is the delta a reconnect is meant to cost. One that has dropped
// something resumes from below the drop and is re-sent values it already
// had - which for a `latest` channel is a client being told the current
// value of a topic twice, and is the trade this design makes every time:
// over-deliver rather than claim caught up.
//
// The worst case is a consumer too slow to keep up at all, which then
// resumes from near the start every time - exactly what the broker did for
// everybody before the delta existed. So the cost falls on the consumers
// that earned it and nowhere else.
func (p latestPos) resumeFrom() uint64 {
	if p.missed > 0 && p.missed-1 < p.sent {
		return p.missed - 1
	}
	return p.sent
}

// latestPositions is each client's resume position on each `latest` channel,
// with a lock of its own.
//
// **Why it is not under b.mu any more.** writeLatest advanced a subscriber's
// `sent` after every write at QoS 1, and that was a take of the broker-wide
// lock once per subscriber per value. After the identifiers, the spans and
// the consumer record stopped being re-asked at the write, this one line was
// what was left: measured on 2026-09-23 - 100 publishers of 16KB into 15 wide
// latest consumers, the broker pinned to four physical cores, fed by a client
// that could not be the limit - its Unlock was 47.5% of all lock delay on the
// latest path, and the broker capped with CPUs idle.
//
// **The map cannot be reached except through these methods**, and each method
// is one hold of posMu for one whole read-modify-write. That is the property
// the move depends on: a position is a struct of three fields that different
// paths advance - `sent` at a write, `seen` at an acknowledgement, `missed`
// when a value is dropped - and two of them updating the struct under two
// different locks would lose one update. Unexported is package-private, not
// sealed - `b.latestSeen.by[id]` compiles anywhere in this package - so
// TestLatestPositionsAreReachedOnlyThroughTheirMethods walks the source for
// any use of the map, the lock or `at` outside these methods.
//
// **It changes no ordering the broker had.** b.mu serialised these updates;
// it never ordered them. A write's `sent` could always land before or after a
// session end's delete, because the write happens first and the two then
// raced for b.mu - and every outcome that interleaving allows under posMu, it
// already allowed under b.mu. Callers that hold b.mu for something else keep
// it and take posMu inside it; callers whose b.mu hold existed only for this
// map no longer take b.mu at all.
//
// **posMu is a leaf.** Nothing is acquired while it is held, so it can be
// taken under b.mu, under a consumer's cmu, or under neither, and no order
// can be inverted through it. TestLatestPositionsLockIsALeaf holds that: these
// methods call nothing but the lock, each other, latestPos and builtins, and
// none but `at` returns anything that refers into the map.
//
// Batching `sent` per value instead - one hold for all of a value's
// subscribers, after the writes - was rejected: a `sent` that lags the write
// is the defect described on latestPos.sent, a resumed session served a value
// its own session also re-sends.
type latestPositions struct {
	posMu sync.Mutex
	by    map[string]map[string]latestPos
}

func newLatestPositions() *latestPositions {
	return &latestPositions{by: map[string]map[string]latestPos{}}
}

// at is a client's entry, made when absent - which is what every writer did
// before this type, so that a client counts as held from its first update
// whatever that update decides. The caller holds posMu.
func (l *latestPositions) at(client string) map[string]latestPos {
	m := l.by[client]
	if m == nil {
		m = map[string]latestPos{}
		l.by[client] = m
	}
	return m
}

// advanceSent records a value written to the session at QoS 1.
func (l *latestPositions) advanceSent(client, ch string, off uint64) {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	m := l.at(client)
	if pos := m[ch]; off > pos.sent {
		pos.sent = off
		m[ch] = pos
	}
}

// advanceSeen records a value the consumer acknowledged.
func (l *latestPositions) advanceSeen(client, ch string, off uint64) {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	m := l.at(client)
	if pos := m[ch]; off > pos.seen {
		pos.seen = off
		m[ch] = pos
	}
}

// lowerMissed records a value that will not reach the consumer, keeping the
// lowest such offset: a resume never starts above it.
func (l *latestPositions) lowerMissed(client, ch string, off uint64) {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	m := l.at(client)
	if pos := m[ch]; pos.missed == 0 || off < pos.missed {
		pos.missed = off
		m[ch] = pos
	}
}

// clearMissed forgets what was dropped for a consumer, where it holds a
// position at all - a subscribe is about to serve the whole of current state.
func (l *latestPositions) clearMissed(client, ch string) {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	if pos, ok := l.by[client][ch]; ok {
		pos.missed = 0
		l.by[client][ch] = pos
	}
}

// resumeFrom is where a resumed session is served from on a channel.
func (l *latestPositions) resumeFrom(client, ch string) uint64 {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	return l.by[client][ch].resumeFrom()
}

// drop forgets every position a client holds, and says whether it held any.
func (l *latestPositions) drop(client string) bool {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	_, had := l.by[client]
	delete(l.by, client)
	return had
}

// clients is how many clients hold a position.
func (l *latestPositions) clients() int {
	l.posMu.Lock()
	defer l.posMu.Unlock()
	return len(l.by)
}

// lowestUnacknowledged is the offset this consumer resumes from.
//
// It is never the highest one sent. PUBACKs may complete out of order, and
// storing the highest would step over a record that is still outstanding -
// which the consumer would then never receive, because on reconnect it
// resumes after the gap and reports success.
func (c *cursor) lowestUnacknowledged() uint64 {
	p := c.next
	for off := range c.outstanding {
		if off < p {
			p = off
		}
	}
	return p
}

func New(reg *channel.Registry, log *slog.Logger) *Broker {
	b := &Broker{
		reg:                 reg,
		log:                 log,
		startedAt:           time.Now(),
		counted:             newCounters(sortedKeys(reg.All())),
		refused:             newRefusals(),
		passed:              newPassings(log),
		logs:                map[string]LogStore{},
		queues:              map[string]QueueStore{},
		latest:              map[string]LatestStore{},
		subs:                map[string][]subscription{},
		partitions:          map[string]map[string]partition{},
		byTopic:             map[string]*mqtt.TopicsIndex{},
		members:             map[string]map[string]struct{}{},
		indexed:             map[string]map[subKey]struct{}{},
		owner:               map[string]*mqtt.Client{},
		claims:              map[*mqtt.Client]*claim{},
		supersededWills:     map[*mqtt.Client]bool{},
		cleanStartEnds:      map[*mqtt.Client]endedSession{},
		willFired:           map[*mqtt.Client]bool{},
		subscriptionsCapped: map[*mqtt.Client]bool{},
		dirs:                map[string]*store.Dir{},
		deliveries:          map[string]*delivery{},
		byPacket:            map[string]string{},
		rr:                  map[string]uint64{},
		holding:             map[heldBy]int{},
		offerDue:            map[string]bool{},
		offerNow:            make(chan struct{}, 1),

		shareCursors: map[string]*shareCursor{},

		notified:     map[string]*watermark{},
		existed:      map[string]map[string]bool{},
		afterSuback:  map[*mqtt.Client]func(){},
		latestSeen:   newLatestPositions(),
		willProps:    map[*mqtt.Client]store.Props{},
		pendingWills: map[string]*pendingWill{},
		unwritten:    map[string]unwrittenDisconnect{},

		disconnecting: map[*mqtt.Client]unwrittenDisconnect{},
		owedRecords:   map[string]*owedRecord{},
		owedEndings:   map[string]*owedEnding{},

		// The ceiling a broker starts from. SetQoS2 raises it where the
		// operator configured somewhere to hold a half-finished exchange,
		// and nothing else writes it.
		maxQoS: maxQoSWithoutStore,

		stopped: make(chan struct{}),
	}
	// A broker nobody has told about credentials admits everybody, which is
	// what a configuration with no password file asks for and what every
	// test that builds one directly expects. SetCredentials is what changes
	// it, and cmd/saguin calls it before a listener opens.
	b.anonymous.Store(true)
	for name, c := range reg.All() {
		switch c.Type {
		case channel.Append:
			b.attachLog(name, store.NewLog())
		case channel.Queue:
			b.queues[name] = store.NewQueue()
		case channel.Latest:
			b.latest[name] = store.NewLatest()
			// A latest channel holds the current value of every topic
			// beneath it, which is what the retain flag asks for, so a
			// broker with one can honour it somewhere (RFC 0003 "Retained
			// messages"). A broker with none refuses it everywhere, and
			// says so at connect rather than by disconnecting later.
			b.retainAvail = 1
		}
	}
	b.applyBounds()
	return b
}

// applyBounds tells each store what its channel may hold.
//
// It runs wherever a store is attached rather than where one is built, so
// that a store arriving later - a provider that keeps its own records,
// replacing the memory one New made - cannot arrive without its bound. A
// store that had one and then quietly did not is a channel that stopped
// refusing, which nothing would report. Callers hold b.mu, or hold nothing
// because no listener is open yet.
func (b *Broker) applyBounds() {
	for name, c := range b.reg.All() {
		if c.MaxBytes > 0 {
			if lg, ok := b.logs[name]; ok {
				lg.SetMaxBytes(c.MaxBytes)
			}
			if q, ok := b.queues[name]; ok {
				q.SetMaxBytes(c.MaxBytes)
			}
		}

		// The retry policy travels the same path and for the same reason: a
		// store attached later must not arrive without it. Unconditional
		// where the bound above is not, because "no backoff" is a policy an
		// operator can have chosen and a store keeping the last one it was
		// given would be a queue whose behaviour depended on the order two
		// things happened at startup.
		if q, ok := b.queues[name]; ok {
			q.SetBackoff(store.Backoff{
				Kind: c.Backoff,
				Base: time.Duration(c.BackoffBase) * time.Second,
			})
		}

		quota := b.quotas[c.Storage]
		if quota == nil {
			continue
		}
		// A store that keeps its own file is bounded by its provider's own
		// mechanism and implements none of this, so the assertion is the
		// question "does this store live in the broker's memory" rather
		// than a capability check.
		type quotaed interface{ SetQuota(*store.Quota) }
		for _, s := range []any{b.logs[name], b.queues[name], b.latest[name]} {
			if q, ok := s.(quotaed); ok {
				q.SetQuota(quota)
			}
		}
	}

	// The retained store is not in the registry, so the loop above cannot
	// reach it, and it shares its provider's pool with that provider's
	// channels - what a bound protects is one machine's memory rather than
	// one store's growth.
	if b.retained != nil {
		if quota := b.quotas[b.retainedProvider]; quota != nil {
			type quotaed interface{ SetQuota(*store.Quota) }
			if q, ok := storeBehind(b.retained).(quotaed); ok {
				q.SetQuota(quota)
			}
		}
	}

	if b.sessions != nil {
		if quota := b.quotas[b.sessionsProvider]; quota != nil {
			type quotaed interface{ SetQuota(*store.Quota) }
			if q, ok := b.sessions.(quotaed); ok {
				q.SetQuota(quota)
			}
		}
	}
}

// aclSource is the authorization file and where it was read from, taken
// together because an answer that names the file is the one an operator can
// act on and the two must not be able to disagree.
type aclSource struct {
	file *authz.File
	path string
}

// SetACL hands the broker the authorization file `/v1/operations/acl`
// explains, and the path it came from. A nil file is the configuration
// writing no `acl_file`, which the route answers rather than refuses.
//
// **Separate from SetAuthorizer, and deliberately.** That one takes an
// interface because the broker asks it yes-or-no questions on the publish
// path and must not care what is behind it. This is the file itself,
// because explaining an answer means reading the patterns that produced it,
// and an interface wide enough for that would be one the publish path had
// to carry.
func (b *Broker) SetACL(f *authz.File, path string) {
	b.acl.Store(&aclSource{file: f, path: path})
}

// SetConfigDocument hands the broker the configuration the binary resolved,
// which is what `/v1/operations/config` answers with (RFC 0005).
//
// **The document is taken once and never re-read**, because the
// configuration file is read once at startup and editing it under a running
// broker changes nothing there. So what this route returns is what the
// process is running, which is the only question worth asking it - a route
// that re-read the file would answer with a configuration this broker is not
// using, at the moment somebody is trying to find out why it is behaving
// oddly.
//
// **The two things SIGUSR1 does change are not in this document**: the log
// level, which is a key here and may have moved since, and the *contents* of
// the password files and the acl_file, which this document names by path
// rather than quotes. `/v1/operations/acl` answers from the file the broker
// is holding, so it follows a re-read.
//
// Called by cmd/saguin with config.File.Report in hand. A broker nobody
// calls this on serves the route with a 500 naming the omission rather than
// an empty document, because an empty configuration and an unwired one are
// the same bytes to a caller and only one of them is a bug.
func (b *Broker) SetConfigDocument(doc map[string]any) {
	b.configDoc.Store(&doc)
}

// SetCredentials names who may connect, and whether a client offering no
// user name is admitted.
//
// Called before any listener opens, and read on every CONNECT. They are
// atomic, but credentialsFor takes the broker's lock on every CONNECT
// anyway, to look up the door's own entry, which every door in the binary
// has.
func (b *Broker) SetCredentials(users *passwd.File, anonymous bool) {
	b.credentials.Store(users)
	b.anonymous.Store(anonymous)
}

// SetListenerCredentials names who may connect **on one listener**,
// overriding what SetCredentials said for the broker as a whole.
//
// Called before any listener opens, once per listener that resolved to
// something of its own. A listener with no entry here uses the broker's
// pair, so the ordinary deployment writes one answer and nothing looks
// anything up.
//
// The key is the id the listener is created under - `tcp`, `ws`, `unix` -
// which is what the substrate puts on a connection as `cl.Net.Listener`,
// and the only thing a CONNECT carries that says which door it came
// through.
func (b *Broker) SetListenerCredentials(listener string, users *passwd.File, anonymous bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listenerAuth == nil {
		b.listenerAuth = map[string]listenerCredentials{}
	}
	// **Merged rather than assigned**, because two setters write this entry
	// and a whole-struct assignment makes the answer depend on which of
	// them main calls last. It did: the certificate state was written
	// first and silently wiped here, and the route reported `none` on a
	// door requiring one - which is the exact misreading the field was
	// added to prevent, reintroduced by the order of two lines.
	c := b.listenerAuth[listener]
	c.users, c.anonymous = users, anonymous
	b.listenerAuth[listener] = c
}

// credentialsFor is the pair in force for the listener a client arrived on.
//
// **The broker-wide pair is the answer for a listener nobody configured**,
// including a listener saguin does not know about - a test harness opening
// its own, for instance. Falling back rather than refusing is right: the
// alternative is a connection denied because of how a listener was named.
func (b *Broker) credentialsFor(listener string) (*passwd.File, bool) {
	b.mu.Lock()
	c, ok := b.listenerAuth[listener]
	b.mu.Unlock()
	if !ok {
		return b.credentials.Load(), b.anonymous.Load()
	}
	return c.users, c.anonymous
}

// listenerCredentials is one listener's answer to who may connect.
type listenerCredentials struct {
	users     *passwd.File
	anonymous bool

	// certificate is what this door does about client certificates:
	// "required", "accepted" or "none". It changes who may connect as
	// completely as the two above do and in a direction they cannot
	// express - a door requiring one admits whoever the authority signed,
	// with no password, and refuses every name in the file. A body that
	// carried only the names would read "closed to all but these three"
	// about a door standing open to a CA, which is the failure `anonymous`
	// was added to prevent, arriving a second way.
	certificate string
}

// SetListenerCertificates says what each door does about client
// certificates, keyed as SetListenerCredentials is. Called once at startup
// for every configured listener, before any of them opens.
func (b *Broker) SetListenerCertificates(listener, state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listenerAuth == nil {
		b.listenerAuth = map[string]listenerCredentials{}
	}
	c := b.listenerAuth[listener]
	c.certificate = state
	b.listenerAuth[listener] = c
}

func (b *Broker) SetServer(s *mqtt.Server) {
	b.srv = s
	// The server's own inline client, not one of ours: it is registered
	// in the client table and carries the protocol version InjectPacket
	// stamps onto the packet. A hand-made one is neither.
	if cl, ok := s.Clients.Get(mqtt.InlineClientId); ok {
		b.inline = cl
		b.inline.Properties.ProtocolVersion = 5
	}
	// Not registered in the client table, for the reason a bridge's is not:
	// it holds no session, so there is nothing there to resume, expire or
	// take over, and a real client connecting with that identifier cannot
	// collide with it.
	b.willClient = s.NewClient(nil, mqtt.LocalListener, "saguin:will", true)
	b.willClient.Properties.ProtocolVersion = 5
	b.wireClientWaiting()
}

// clientWaiter is a provider that serves first the writes a packet a client
// is waiting on needs, from the moment the packet is read until it is
// answered (mqtt.Server.ClientWaiting).
type clientWaiter interface {
	Waiting(client string) (answered func())
}

// wireClientWaiting tells every provider that can use it which client ids
// have a packet waiting on the store: the session store's, and each channel's,
// since a SUBSCRIBE or a seek waits on a position in the channel's provider.
// Called from SetServer and SetStores, before any listener opens, whichever
// runs second doing it.
func (b *Broker) wireClientWaiting() {
	if b.srv == nil || len(b.waiters) == 0 {
		return
	}
	if len(b.waiters) == 1 {
		b.srv.ClientWaiting = b.waiters[0].Waiting
		return
	}
	ws := slices.Clone(b.waiters)
	b.srv.ClientWaiting = func(id string) func() {
		answered := make([]func(), len(ws))
		for i, w := range ws {
			answered[i] = w.Waiting(id)
		}
		return func() {
			for _, f := range answered {
				f()
			}
		}
	}
}

// topicAliasMaximum is how many topic aliases one connection may hold, and
// is what the CONNACK advertises so a client never has to guess.
//
// A client that keeps within it sends a long topic once and two bytes
// afterwards; one that exceeds it is refused that alias with 0x94 by the
// substrate, at the point the packet is parsed. Sixteen bounds the table
// by arithmetic: at most sixteen topics per connection, each at most
// max_topic_length, times max_connections.
const topicAliasMaximum = 16

// RetainedStoreName is what the retained store is called wherever the
// delivery machinery wants a channel name - the pending snapshot per
// client, the in-flight bookkeeping, the log lines.
//
// It holds a `/`, which is what makes it safe: a channel name is one topic
// level and may not contain one, so this can never collide with a channel
// however a configuration is written. The reserved prefix is the same one
// RFC 0002 already reserves for topics saguin defines.
const RetainedStoreName = "$saguin/retained"

// Authorizer answers whether a client may do a thing, and is the shape
// per-client authorization plugs into (RFC 0002, "What a client may do").
//
// **It is asked last, and may only narrow.** OnACLCheck carries saguin's
// structural refusals - the queue subscription form (invariant 4) and the
// wildcard boundary (invariant 11) - and the substrate ORs its ACL hooks,
// so an Authorizer able to answer before them, or instead of them, would
// grant a non-canonical queue subscription and put two workers on one job.
// That is invariant 4's exact failure reached through a feature meant to
// restrict access, which is why the order is a rule rather than a detail.
type Authorizer interface {
	// Allows says whether the client id, authenticated as identity, may
	// write to, or read from, this topic. It is never asked about an
	// operation saguin has already refused, so answering true can only leave
	// a refusal standing.
	//
	// **A name and an id rather than a connection**, because one question
	// is asked after the connection has gone: a Will is published for a
	// client that is no longer there, as the client that armed it
	// (publishWill).
	Allows(identity, clientID, topic string, write bool) bool

	// PublishLimits is what the client authenticated as identity may send
	// in a second, and whether the file names it at all. Not named means the
	// broker-wide figures apply; named replaces them, up or down.
	PublishLimits(identity string) (rate int, bytes int64, named bool)

	// HasPublishLimits reports whether the file names any at all, so that a
	// broker with an acl_file and no limits in it - which is most of them -
	// pays nothing per publish for a feature it never asked for.
	HasPublishLimits() bool

	// AllowsClientID says whether this client id may log in under this
	// proved user name. It is asked once, at connect.
	//
	// **It is asked after the name is proved and can only refuse**, so an
	// unverified client id never widens anything. Absent from the file it
	// answers true, which is every deployment that has not asked for it.
	AllowsClientID(identity, clientID string) bool

	// WithheldGrants is how many rules grant this pair nothing because the
	// name %u or %c would put into them holds a wildcard or a level
	// separator. Asked once at connect, to say so: the rules are withheld
	// whether or not anybody asks.
	WithheldGrants(identity, clientID string) int

	// DeniesFeature says whether this client is denied a feature -
	// `persistent`, `will`, `share`, `retained` or `qos2` - by a `broker: features`
	// rule. Asked once the name is proved, like AllowsClientID, and it too
	// can only take away: absent from the file it answers false.
	DeniesFeature(identity, clientID, feature string) bool
}

// Registry is the channels this broker serves, which an authorization file
// is validated against: a rule naming a channel that is not configured
// grants nothing and reads as though it does.
func (b *Broker) Registry() *channel.Registry { return b.reg }

// SetAuthorizer installs the rules an acl_file carries. Without one, every
// authenticated client may do anything.
func (b *Broker) SetAuthorizer(a Authorizer) {
	b.authz.Store(&a)
	// **Bumped after the store, so nothing reads a new generation beside an
	// old file.** A cache that re-resolved on the generation and then read
	// the previous authorizer would hold a stale answer and believe it was
	// fresh, which is worse than the stale answer it replaced.
	b.authzGen.Add(1)
	b.wakeStalled()
}

// wakeFreed offers a connected client's freed room to what it is owed, as an
// acknowledgement does (OnQosComplete): its append cursors, its latest
// values waiting, and its queues' next jobs. For room freed by no
// acknowledgement - a delivery that expired in flight.
//
// On a goroutine of its own, as wakeStalled's is: the substrate's
// housekeeping loop calls this, and a latest drain writes to the socket.
func (b *Broker) wakeFreed(cl *mqtt.Client) {
	b.drains.Add(1)
	go func() {
		defer b.drains.Done()
		var owed freed
		if con := b.lookupConsumer(cl.ID); con != nil {
			con.cmu.Lock()
			if !con.gone {
				owed.drains = b.appendFreedC(cl, con)
				owed.latest = latestOwedC(con)
			}
			con.cmu.Unlock()
		}
		b.mu.Lock()
		owed.queues = b.queuesFreedLocked(cl)
		b.mu.Unlock()
		b.serveFreed(cl, owed)
	}()
}

// wakeStalled feeds every consumer once, because a grant that has just come
// back is not an event any of them is waiting on.
//
// **A stall is keyed to traffic and a restored grant is not traffic.** A
// consumer refused mid-channel keeps its position and is pumped again on the
// next append - so on a busy channel the backlog moves the moment the file is
// re-read, and on an idle one it waits for a publish that may not come today.
// A schema registry or a configuration channel is exactly that: the operator
// signals, the log says the file was re-read, and the consumer sits connected
// and silent holding records it is now allowed to have. RFC 0002 promises it
// "resumes from there and receives what it missed", and before this the
// broker only made that true on the next event.
//
// **The third of this shape the project has paid for** - a retained-store
// lookup and a snapshot drain keyed to the wrong event came before it - which
// is why the wake is here, on the one thing every restored grant passes
// through, rather than in the signal handler that happens to be today's
// caller.
//
// On its own goroutine: this runs under the signal handler and a pump takes
// the broker's lock and writes to sockets. Harmless before there is a server
// or a client - startup calls it with neither.
func (b *Broker) wakeStalled() {
	if b.srv == nil {
		return
	}
	b.drains.Add(1)
	go func() {
		defer b.drains.Done()
		for _, cl := range b.srv.Clients.GetAll() {
			if cl.Closed() || cl.Net.Inline {
				continue
			}
			// Both halves of what a stall can hold: an append consumer's
			// cursor, and a `latest` value queued for a subscriber that was
			// refused it.
			b.drainPending(cl)
			b.pumpConsumer(cl)
		}
	}()
}

// authorizer is the acl_file's rules as they stand, or nil where there is no
// acl_file. Every reader goes through here: the file is replaced under a
// running broker, and a field read would be a race the moment it is.
func (b *Broker) authorizer() Authorizer {
	if p := b.authz.Load(); p != nil {
		return *p
	}
	return nil
}

// permits asks the authorization rules and nothing else. Every caller has
// already run the structural checks, which is what makes "a rule may take
// permission away and may never give it" true by construction rather than
// by everyone remembering it.
func (b *Broker) permits(cl *mqtt.Client, topic string, write bool) bool {
	return b.permitsAs(identity(cl), cl.ID, topic, write)
}

// permitsAs is permits for a client id and the name it authenticated as,
// which is all a Will has once its connection has gone.
func (b *Broker) permitsAs(ident, clientID, topic string, write bool) bool {
	a := b.authorizer()
	if a == nil {
		return true
	}
	return a.Allows(ident, clientID, topic, write)
}

// SetQoS2 offers exactly-once, raising this broker's QoS ceiling to 2, with
// the allowance and expiry of `broker.qos2` (RFC 0002). A publish is held in
// the store of the channel it is for, or the broadcast log's for a broadcast
// (holdForRelease), so there is no store to attach: every store holds its
// own.
//
// The CONNACK's number is written here too. It is read live by the
// substrate at every connect, and SetServer has already run by the time any
// caller reaches this - so the advertisement and the refusal in
// OnPacketRead are one number with one writer, rather than two that have to
// be kept in step.
//
// Called before any listener opens, so nothing reads these while they are
// being written.
func (b *Broker) SetQoS2(maxPerClient int, expiresAfter time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pendingExpires = expiresAfter
	b.qos2MaxPerClient = maxPerClient
	b.maxQoS = maxQoSWithStore
	if b.srv != nil {
		b.srv.Options.Capabilities.MaximumQos = maxQoSWithStore
	}
}

// SetSessions attaches the store that keeps every session's state, and which
// provider holds it. Called before any listener opens, like every other store.
func (b *Broker) SetSessions(provider string, s SessionStore) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// The store itself stays in b.sessions, for the questions asked of its
	// type (a quota); every call goes through sessionStore(), counted.
	b.sessions = s
	b.sessionsProvider = provider
	b.sessionsNow.Store(&sessionsRef{countingSessions{SessionStore: s, on: b.countStorageOn(provider)}})
	b.applyBounds()
}

// SetAckCommitInterval sets broker.session.ack_commit_interval: how long a
// connected session's acknowledgements may wait before they are stored.
func (b *Broker) SetAckCommitInterval(d time.Duration) { b.ackCommit.Store(int64(d)) }

// AckCommitInterval is broker.session.ack_commit_interval, and its default
// for a broker that was never given one.
func (b *Broker) AckCommitInterval() time.Duration {
	if d := time.Duration(b.ackCommit.Load()); d > 0 {
		return d
	}
	return config.DefaultAckCommitInterval
}

// sessionStore is the session store, or nil where none is attached.
func (b *Broker) sessionStore() SessionStore {
	if r := b.sessionsNow.Load(); r != nil {
		return r.s
	}
	return nil
}

// sessionsRef holds the session store for atomic.Pointer.
type sessionsRef struct{ s SessionStore }

// SetRetained attaches the store that holds retained messages on broadcast
// topics: which provider keeps it, the store itself, and how long a topic
// keeps its value after going quiet.
//
// Called before any listener opens, like every other store, so nothing
// reads these while they are being written.
func (b *Broker) SetRetained(provider string, lt LatestStore, period int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retained = countingLatest{LatestStore: lt, on: b.countStorageOn(provider)}
	b.retainedProvider = provider
	b.retainedPeriod = period
	b.retainedPeriodNow.Store(period)
	// The flag can now be honoured somewhere, whatever the channels are.
	b.retainAvail = 1
	// A store that keeps its own records makes this broker durable, the
	// same way an attached channel store does. The question is asked of
	// the store rather than of the caller: a memory store hands its state
	// over for a snapshot and a sqlite one has nothing to hand over
	// because it wrote as it went, which is exactly the distinction.
	if _, memory := lt.(exportableLatest); !memory {
		b.durable = true
	}
	// Its provider's bound reaches it the same way a channel's does. A
	// store with no bound where the provider has one is a pool protected
	// everywhere but here.
	b.applyBounds()
}

// Shutdown stops the broker in the one order that is safe, and exists so
// that the order is a thing that can be called and asserted rather than
// three statements in main that read top to bottom.
//
// The caller cancels Run's context first - that is the caller's, not the
// broker's - and stops any bridge, so that nothing is still bringing
// records in from somewhere else. Then, in this order and for a reason
// each:
//
//  1. **Close the server.** It returns once every client's read loop has
//     finished, so no publish is in flight afterwards. Measured, by
//     freezing a publish inside the hook that stores it: Close blocked for
//     as long as the publish was held and returned 2.001s after it was
//     released. The acknowledgement follows that same hook, so a record a
//     producer was told about is already stored by the time this returns.
//  2. **Wait for Run.** Cancelling its context makes it return soon, not
//     now: a retention sweep may be inside a store, deleting records, when
//     the cancel lands. Nothing has ever shown that costing anything - a
//     533ms snapshot with a sweep firing every second never caught one,
//     under the race detector - so this is an ordering that can be
//     asserted rather than a defect being repaired.
//  3. **Wait for the deliveries in flight.** A record is stored on the
//     publishing goroutine and written to its consumers on another, so
//     step 1 does not end them. Every client is closed by then, so each
//     stops at its first write.
//  4. **Write the snapshot.** Memory durability is exactly this file
//     (invariant 14), so its error is the caller's to treat as fatal.
//
// It is not idempotent and does not need to be: main calls it once, and
// the test harness wraps it in a sync.Once for the same reason it always
// did - writing the snapshot twice would hide a save that only works the
// first time.
func (b *Broker) Shutdown() error {
	// **Nothing dies because the broker is stopping.** Closing the listeners
	// closes every client, and a client whose connection ends without a
	// DISCONNECT is one whose Will is due - so a stop announced every
	// connected device as dead, immediately for a Will with no delay and at
	// the next start for one with a delay, whose moment this broker now
	// writes down. Measured before this: a client connected with a Will on a
	// channel topic, the broker stopped and started, and the record was in
	// the channel for the next consumer to read.
	//
	// The flag is set before the listeners close, which is the only ordering
	// that matters: after it, OnWill is asked about connections the broker
	// itself is ending.
	b.stopping.Store(true)
	// **A Will waiting out its delay does not fire into a stopping broker.**
	// Its due moment is written on its session's record (holdWill), so the
	// next start publishes it; published here, its record could land after
	// the channel's snapshot and its Will be taken from a session store
	// saved without it - a Will lost. Stopped before anything else, and
	// under b.mu, so a timer already firing either counted itself in
	// drains first or finds the broker stopping (firePendingWill).
	b.mu.Lock()
	for id, p := range b.pendingWills {
		p.timer.Stop()
		delete(b.pendingWills, id)
	}
	b.mu.Unlock()
	if b.srv != nil {
		// The error here is the listener's, and there is nothing to do
		// about it at this point: the process is going either way.
		_ = b.srv.Close()
	}
	if b.running.Load() {
		<-b.stopped
	}
	// Every client is closed by now, so a drain still running is writing to
	// a connection that has gone and will stop at its first write. This
	// waits for that rather than assuming it: the snapshot below reads the
	// same stores a drain reads, and advances the positions it advances.
	// A release gathering what nobody owes stops waiting and removes it now,
	// before the snapshot reads the log (bdrain.release).
	if d := b.bcast.Load(); d != nil {
		d.kickRelease()
	}
	b.drains.Wait()
	var unwritten error
	if d := b.bcast.Load(); d != nil {
		unwritten = d.flushAll()
		// A flush that freed a member's room wakes its groups' drains,
		// which find no client; waited for before the snapshot reads.
		b.drains.Wait()
	}
	if afterDrainsWait != nil {
		afterDrainsWait()
	}
	// The disconnects the store refused while it ran, so the next start
	// reads each session as its client left it (recordDisconnect).
	b.retryUnwrittenDisconnects()

	if !b.Durable() {
		return unwritten
	}
	return errors.Join(unwritten, b.SaveSnapshots())
}

// afterDrainsWait is nil but in a test, which fires a delayed Will where a
// stopping broker used to still have its timer armed.
var afterDrainsWait func()

func (b *Broker) ID() string { return "saguin" }

// provided is the set of hook events this broker answers, indexed by the
// event rather than searched for.
//
// **It is on the publish path about eighteen times per message**, because
// the engine asks every hook whether it provides an event before dispatching
// it, and a publish dispatches around nine. The version before this built a
// twenty-three byte slice on each call and scanned it: 6.73ns where the
// event was in the set and 6.95ns where it was not, against 0.21ns for the
// index below, measured at `-benchtime 2s -count 3`. That is about 117ns a
// message, on a memory-store publish whose floor is 2,960ns.
//
// The events are named rather than numbered, so adding one to the hook set
// is an entry here and not an arithmetic problem. They are mqtt's iota
// constants and their values move when that list changes, which is the
// reason this is built at initialisation from the constants instead of
// being written out as a literal table.
var provided = func() (s [256]bool) {
	for _, e := range []byte{
		mqtt.OnConnect,
		mqtt.OnConnectRefused,
		mqtt.OnConnectionRefused,
		mqtt.OnSocketRefused,
		mqtt.OnACLCheck,
		mqtt.OnSubscribe,
		mqtt.OnSessionEstablish,
		mqtt.OnSessionEstablished,
		mqtt.OnSessionRegistered,
		mqtt.OnSessionSuperseded,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribe,
		mqtt.OnUnsubscribed,
		mqtt.OnDisconnect,
		mqtt.OnPublish,
		mqtt.OnSelectSubscribers,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnWill,
		mqtt.OnPacketEncode,
		mqtt.OnClientExpired,
		mqtt.OnRetainMessage,
		mqtt.OnPublishDropped,
		mqtt.OnQosDropped,
		mqtt.OnPacketRead,
		mqtt.OnPacketSent,
		mqtt.OnDeliveryReleased,
		mqtt.OnDeliveryDone,
		mqtt.OnDisconnecting,
		mqtt.OnDeliveryUnsent,
	} {
		s[e] = true
	}
	return
}()

func (b *Broker) Provides(y byte) bool { return provided[y] }

// storeBeforeDisconnectCloses stores what the close answering a clean
// DISCONNECT tells the client is done, before the engine closes the
// connection (invariant 18; RFC 0003 "A stored position may lag, and never
// leads"). It runs from OnDisconnecting, once the read loop owns the end
// (mqtt.Client.claimEnd): from OnPacketRead, before that claim, a shutdown or
// hang-up could close the connection while it stored.
//
// **The engine closes the socket as it handles the DISCONNECT**, and
// OnDisconnect's flush runs only once the read loop has returned, so a
// broker that died between the two had told the consumer its connection was
// closed and kept none of what it acknowledged just before: the consumer was
// sent those records again after the restart. Only for a session that
// outlives its connection, whose positions are kept, and only by the id's
// owner.
func (b *Broker) storeBeforeDisconnectCloses(cl *mqtt.Client) {
	if cl.Net.Inline || !persistentSession(cl) {
		return
	}
	b.mu.Lock()
	owns := b.owner[cl.ID] == cl
	b.mu.Unlock()
	if !owns {
		return
	}
	b.flushPositionsOf(cl.ID)
	// And its broadcast acknowledgements, which the drain writes on its own
	// goroutine: a clean DISCONNECT's PUBACK comes just before it.
	if d := b.broadcastDrain(); d != nil {
		if o := d.session(cl.ID); o != nil {
			d.flush(o)
		}
	}
}

// OnPacketRead refuses a PUBLISH carrying a QoS above the one this broker
// advertised, which MQTT 5 requires and which nothing else here can do.
//
// **The CONNACK says Maximum QoS 1 and a conforming client will not exceed
// it** - [MQTT-3.2.2-9] forbids that outright. A client that does anyway is
// covered by section 3.2.2.3.4, and the answer it names is not a
// negotiation: *"If a Server receives a PUBLISH packet with a QoS greater
// than the Maximum QoS then it MUST respond with a DISCONNECT packet
// containing Reason Code 0x9B."*
//
// The substrate does something else. `processPublish` rewrites the packet:
//
//	if pk.FixedHeader.Qos > s.Options.Capabilities.MaximumQos {
//	    pk.FixedHeader.Qos = s.Options.Capabilities.MaximumQos
//	}
//
// and then acknowledges it. Measured on the wire before this existed: a
// QoS 2 PUBLISH was answered `PUBACK` reason `0x00`, Success. **So the
// publisher asked for exactly-once, was told it succeeded, and got
// at-least-once** - a guarantee quietly downgraded and reported as met,
// which is the failure `docs/invariants.md` puts ahead of a crash, because
// nobody goes looking for it. The clause the substrate cites is real, but
// it governs what a server *sends*; there is no clause anywhere permitting
// an inbound downgrade.
//
// **It has to be this hook and not OnPublish.** The rewrite above happens
// before `OnPublish` is called, so by the time saguin sees the packet its
// QoS reads 1 and a downgraded QoS 2 is indistinguishable from an ordinary
// QoS 1 publish. `OnPacketRead` runs as the packet is read, before
// validation and before the rewrite, and is the only place the client's own
// number is still there to be read.
//
// Disconnecting rather than refusing the publish is the specification's
// choice rather than saguin's, and it is the right one: a client that sends
// QoS 2 is running a state machine expecting `PUBREC`, so any answer that
// leaves the connection open leaves it waiting for a packet that is never
// coming.
func (b *Broker) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// Only a publish. **Every protocol version, which it did not used to
	// be**: this returned early for anything below MQTT 5, on the grounds
	// that such a client was never told a Maximum QoS and cannot be sent a
	// DISCONNECT explaining one. Both halves of that are true and neither
	// is a reason to let the packet through - the substrate's downgrade
	// does not check the version either, so a 3.1.1 QoS 2 publish was
	// rewritten to QoS 1, stored, and answered `PUBACK` Success. A
	// publisher asking for exactly-once, told it succeeded, given
	// at-least-once, exactly as an MQTT 5 one was before this hook existed.
	// Now it closes the connection (RFC 0002 "Every reason code, in one
	// place").
	// **A PUBREL is the other packet this hook has to see**, and it is not
	// a refusal. saguin holds an exactly-once publish rather than storing
	// it, so the record is written when the release arrives - and it has to
	// be written *before* the substrate answers PUBCOMP, or the client is
	// told the exchange finished for a record nobody has. processPubrel
	// writes that PUBCOMP before it calls any hook, so this is the only
	// moment early enough (invariant 16).
	if pk.FixedHeader.Type == packets.Pubrel {
		return b.releaseHeld(cl, pk)
	}
	// **An AUTH packet is the other half of the refusal OnConnect makes**,
	// and without it that refusal is only half a fix. saguin turns away
	// every CONNECT naming an Authentication Method, so no connection here
	// ever negotiated one - and MQTT's re-authentication (§4.12.1) is
	// available only to a client that did. An AUTH on any other connection
	// is a protocol error, which §4.13.1 answers with a DISCONNECT
	// carrying `0x82`.
	//
	// The substrate used to hand an AUTH to an OnAuthPacket hook and
	// otherwise do nothing with it, so with no such hook implemented the
	// packet was read, dropped, and never answered. Measured before this existed:
	// a client sending AUTH `0x19` to re-authenticate waited until its own
	// timeout, twice - once for the exchange and once for the failure it
	// expected to be told about. Same defect as the CONNECT and the same
	// answer, at the door the client reaches second.
	//
	// A *first* packet of AUTH never arrives here: the substrate refuses
	// anything but a CONNECT before the packet is read at all.
	if pk.FixedHeader.Type == packets.Auth {
		b.log.Warn("disconnecting: an AUTH packet on a connection that negotiated "+
			"no authentication method", "client", b.limits.Loggable(cl.ID))
		// Both codes and in this order, for the reason spelled out at the
		// refusal below: the hook contract returns only on ErrRejectPacket
		// and a bare code is swallowed.
		refused := fmt.Errorf("%w: %w", packets.ErrProtocolViolation, packets.ErrRejectPacket)
		b.disconnect(cl, packets.ErrProtocolViolation)
		return pk, refused
	}
	if pk.FixedHeader.Type != packets.Publish {
		return pk, nil
	}

	// QoS 0 and 1 are under every ceiling, so the acl_file is asked only
	// about the publishes it could refuse.
	if pk.FixedHeader.Qos <= 1 {
		return pk, nil
	}
	ceiling := b.qosCeilingFor(cl)
	if pk.FixedHeader.Qos <= ceiling {
		return pk, nil
	}

	b.log.Warn("disconnecting: a publish above the advertised Maximum QoS",
		"client", b.limits.Loggable(cl.ID),
		"topic", b.limits.Loggable(pk.TopicName),
		"qos", pk.FixedHeader.Qos, "max", ceiling)

	// **What the client is told differs and the outcome does not.** MQTT 5
	// gets a DISCONNECT naming `0x9B`; 3.1.1 has no server-to-client
	// DISCONNECT, so it gets the socket closed and nothing said - the same
	// choice, for the same reason, as a refused publish it cannot be told
	// about.
	//
	// Best effort either way: if the DISCONNECT cannot be written the
	// connection is going anyway, and the error returned here is what stops
	// the packet being processed.
	// **Both codes, in this order, because the hook contract only
	// understands `ErrRejectPacket`** - the same wrap `refuseRetain` makes
	// and for the same reason, which this predated and did not copy.
	//
	// A bare code is *swallowed*: the substrate's OnPacketRead aggregator
	// returns only on `ErrRejectPacket` and continues on anything else, so
	// the packet carried on into the publish path and saguin's own
	// `OnPublish` stored it. Measured at HEAD: a 3.1.1 client published
	// `ev/x` at QoS 2, was disconnected with nothing acknowledged, and a
	// later subscriber replaying the channel was served the record. Refused
	// and published at the same time - and worse than the QoS 0 case that
	// phrase was coined for, because a conforming QoS 2 publisher never got
	// its PUBREC and re-sends on every reconnect, storing another copy each
	// time.
	//
	// The client has already had its DISCONNECT, or its close, above; this
	// only stops the packet.
	//
	// **Counted as a refused publish as well as a refused connection**, the
	// pair a refused retained publish already makes: RFC 0005 gives the two
	// series one vocabulary so that one divided by the other reads "of the
	// publishes refused for this reason, how many cost a connection". Counted
	// in disconnect alone, a client denied `qos2` showed as a connection
	// refused `qos not supported` for a publish refused nowhere, measured on
	// bin/saguin. This path never reaches OnPublish, where the rest are
	// counted.
	refused := fmt.Errorf("%w: %w", packets.ErrQosNotSupported, packets.ErrRejectPacket)
	b.counted.refuse(reasonNames[packets.ErrQosNotSupported.Code])
	b.disconnect(cl, packets.ErrQosNotSupported)
	return pk, refused
}

// OnPublishDropped counts a delivery the broker could not hand to a
// consumer, and says whose it was.
//
// **The bound itself is not the gap.** A client's outbound queue is
// MaximumClientWritesPending packets deep and half its session's
// limits.session_queue_bytes in bytes, and past that the substrate discards
// a QoS 0 delivery rather than growing - a QoS 1 or 2 one waits in its
// session instead - a defined outcome at a bound, which is
// what invariant 13 asks for, and it is the reason one slow consumer
// cannot become a dead broker. What was missing is that nothing outside
// could see it happen: the hook was not in the list above and no metric
// read it, so a fleet could shed for a week in silence.
//
// **A drop is not lost channel data.** A durable consumer's position
// advances on the PUBACK, so an undelivered record is offered again; the
// cost is a redelivery. On broadcast it is a real loss, and broadcast
// promises nothing, so the two agree.
//
// The consumer's name is here rather than in a label because a client
// chooses its own id and a metric keyed by one is a series count chosen by
// whoever connects - RFC 0005 refuses that outright. It goes through
// Loggable for the same reason every other client-chosen string does.
//
// **At debug**, because this repeats for as long as the consumer is
// behind: a client 8192 packets in arrears produces one of these per
// packet, and a warning that arrives thousands of times a second is how a
// log stops being read. The counter is the thing that is always on; this
// is what an operator turns up once the counter has told them to look.
func (b *Broker) OnPublishDropped(cl *mqtt.Client, pk packets.Packet) {
	b.counted.dropped.Add(1)
	b.log.Debug("dropped a delivery: the consumer's outbound queue is full",
		"client", b.limits.Loggable(cl.ID), "topic", b.limits.Loggable(pk.TopicName),
		"listener", cl.Net.Listener)
}

// writeTo sends a packet to a client under the configured write deadline,
// and disconnects a client that does not take it in time.
//
// **The deadline is the whole of the fix for a duplicate on a durable
// channel.** A client can stop reading its socket while the connection
// stays open, and the write then waits for as long as that client cares to
// stay - on whichever goroutine is delivering. An append or latest record
// went out on the goroutine that *published* it, so an unrelated PUBACK
// was withheld for a record already stored; its library gave up,
// reconnected, and re-sent what it had already delivered. Measured before
// this existed: a publisher stalled at its 644th record, and the channel
// read back 645 records with 644 distinct values.
// Both are written by a drain of the consumer's own now - append from the start, latest since its values began to wait (runLatest) - so the
// deadline bounds what a consumer holds of its own delivery, not a
// publisher.
//
// The same write reaches the queue and retention loop through a
// dead-letter channel, and stops it for every channel at once. Measured:
// 1,499 records dead-lettered to a consumer that had stopped reading, and
// twenty later jobs sat unexpired until that consumer's socket closed.
//
// **A client that trips it is stopped, not merely reported.** The deadline
// fires mid-packet, so what is on the wire is half a packet and there is no
// way to continue that stream. It costs the consumer nothing: a durable
// consumer's position advances only on its own PUBACK, so it reconnects and
// resumes at the record it had reached. That is a property of the cursor
// rather than of this function, and it is what makes hanging up affordable.
//
// **The deadline itself is the substrate's**, set once from
// `limits.write_timeout` when the server is built. It has to be: the
// substrate writes a queue's offers and every broadcast, holding the
// client's lock across the write, so a bound applied only here left those
// routes able to freeze the queue-and-retention loop and to hang a
// publisher. Arming it there also arms it inside
// that lock, which is the only place a connection deadline can be armed
// without one goroutine clearing another's.
//
// What this adds on top is stopping the client. The substrate's WriteLoop
// does that for the writes it makes, and this write never went through it -
// a timed-out write has put half a packet on the wire and that stream
// cannot be continued. The operator's line is written once, in
// OnDisconnect, where both routes end.
func (b *Broker) writeTo(cl *mqtt.Client, pk packets.Packet) error {
	// A write that fails on the connection - a timeout or otherwise - stops
	// the client inside WritePacket, so a write saguin makes itself ends a
	// broken stream the same way a queued one does. OnDisconnect writes the
	// line for both routes.
	return cl.WritePacket(pk)
}

// isTimeout reports whether an error is a network timeout - a write that
// ran out of time under `limits.write_timeout`.
//
// **net.Error's Timeout rather than errors.Is against os.ErrDeadlineExceeded**,
// which is what this checked first and which is right for a plain TCP
// write and wrong for a WebSocket one. gorilla writes through a buffered
// writev, and the error that comes back is "writev tcp …: i/o timeout" -
// a timeout by every definition that matters, and not that sentinel. So a
// deaf WebSocket consumer was never hung up on, by a broker whose
// configuration said it would be.
func isTimeout(err error) bool {
	var ne net.Error
	return err != nil && errors.As(err, &ne) && ne.Timeout()
}

// OnQosDropped counts a delivery discarded because the publisher's Message
// Expiry Interval ran out before it could be sent.
//
// **Counted apart from the drops above**, because it is the publisher's
// own instruction being carried out rather than anything going wrong, and
// the right number of them is whatever that publisher meant. Added
// together the two would be a figure nobody could act on.
//
// **The hook fires for more than expiry, and more than once**, which is
// what the check below is for. The substrate calls it from five places,
// three of them reachable here:
//
//   - Client.ClearExpiredInflights, per packet whose expiry has passed -
//     the one this counts;
//   - Server.clearExpiredInflights, again for every id that function just
//     returned, so a single expiry arrives twice;
//   - Client.ClearInflights, for every packet still in flight when a
//     session is discarded - at a disconnect with expiry 0, at a
//     clean-start takeover and at shutdown. None of those expired.
//
// The remaining two are the QoS 2 PUBREC and PUBREL paths, reachable since
// every saguin offers QoS 2: an exchange they drop is counted here only if
// its message's own expiry has passed, by the check below.
//
// **So the packet is asked whether it actually expired** rather than
// trusted to have. The second call carries only a packet identifier, so
// its Expiry is zero and it fails the test; a session discard fails it for
// every packet whose deadline has not passed. Measured before this check
// existed: one publish carrying a two-second expiry counted 2, and five
// with no expiry at all counted 5 more at the disconnect - 7 for one
// expiry.
func (b *Broker) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	// **A delivery dropped from a connected client's in-flight table is room
	// in its window**, and only an acknowledgement used to bring the
	// client's channels back to use it: a latest value or an append record
	// waiting behind an expired delivery stayed unsent until the next
	// publish or acknowledgement, which a quiet channel need never have. A
	// PUBLISH only: the sweep says each identifier a second time with a bare
	// packet, and processPubrel says it of the client's own QoS 2 exchange,
	// which holds no room in this window.
	if pk.FixedHeader.Type == packets.Publish && !cl.Closed() {
		b.wakeFreed(cl)
	}
	if pk.Expiry <= 0 || pk.Expiry > time.Now().Unix() {
		return
	}
	b.counted.expired.Add(1)
	b.log.Debug("dropped a delivery: it expired before it could be sent",
		"client", b.limits.Loggable(cl.ID), "packet", pk.PacketID)
}

// disconnect ends a connection with the code, in the way the client's own
// protocol allows it to be told.
//
// **MQTT 5 is sent a DISCONNECT naming the code; anything older gets the
// socket closed and nothing said**, because 3.1.1 has no server-to-client
// DISCONNECT and the substrate encodes one regardless - a bare `0xE0` the
// client's specification does not define. Measured at the wire: a 3.1.1
// client publishing a retained message to a broker with no retained store
// read `0xe0` before the close.
//
// **It is one function because it was three sites and then five.** The
// refused publish and the QoS 2 publish converted their own and left the
// rest, which is the shape of defect this codebase keeps paying for: a rule
// applied where it was noticed rather than everywhere it reaches. Asked
// here, a site added tomorrow inherits it.
func (b *Broker) disconnect(cl *mqtt.Client, code packets.Code) {
	// **Once per connection, not once per site that decides to end it.**
	// A refused retained publish is answered by `refuseRetain`, which ends
	// the connection here - and then returns a wrapped code that reaches
	// the publish path's own legacy branch, which ends it again. The stop
	// is idempotent and the counting was not: one QoS 1 retained publish
	// produced a route row reading 2, and the counter over-read by the same
	// factor for exactly the refusal an operator is most likely to alert on.
	//
	// Asked of the connection rather than of the caller, so a second path
	// added later cannot double-count either.
	if cl.Closed() {
		return
	}
	// **Counted and named here, where the disconnection happens, rather
	// than at each site that decides on one.** The count used to live in
	// the publish path's own branch, which is gated on QoS above zero - so
	// a 3.1.1 client hung up on for a *QoS 0* retained publish appeared in
	// neither the counter nor the route, and that is the most Tasmota-shaped
	// flap there is: a retained birth message against a broker with no
	// retained store. RFC 0005 defines both instruments as every connection
	// this broker refused, on any protocol, which is exactly that client.
	// Asked here, no site can be added that forgets to say so.
	b.recordRefusal(cl, cl.ID, refusalReason(code))
	b.endConnection(cl, code)
}

// endConnection ends a connection in the way the client's protocol allows,
// and says nothing about it.
//
// **Two rules were one function and they are not the same rule.** How a
// connection ends is a protocol question - MQTT 5 is sent a DISCONNECT
// naming the code, and anything older gets the socket closed, because
// 3.1.1 has no server-to-client DISCONNECT and the substrate encodes one
// regardless. Whether the ending is *counted* is a different question, and
// the answer is no for an ending that is not a refusal: a session taken
// over is the client's own doing, and folding it into the instruments that
// report refusals would inflate the number an operator alerts on with
// something nobody needs to act on.
//
// **The enumeration that found the remaining sites is worth recording.**
// The first sweep grepped for `DisconnectClient(cl` and reported every site
// converted - and missed two, because they name the client `existing` and
// `chosen.cl`. A class enumerated by a pattern that includes a variable
// name is a class enumerated by luck.
func (b *Broker) endConnection(cl *mqtt.Client, code packets.Code) {
	// DisconnectClient writes a 3.1.1 client nothing, and bounds a takeover's
	// wait for the end (mqtt.Server.DisconnectClient).
	_ = b.srv.DisconnectClient(cl, code)
}

// hangUp ends cl's connection with code, as endConnection does, on a
// goroutine of its own and once; it reports whether this call began it.
//
// **For a decision made where other clients are served**: a publisher's
// goroutine choosing a shared member, a queue's offer choosing a worker, an
// operator's connection asking for another to be hung up. The DISCONNECT is
// a write to cl's socket, and it waits for cl's lock, which cl's own write
// loop holds for as long as a write cl is not reading lasts. Written there, a
// client that had stopped reading held the queue's next offer to a worker
// that was reading, for all of limits.write_timeout - measured at 29.7s of
// 30 - and would have held the publisher or the operator the same way
// (invariant 16).
//
// **Once per connection, and chosen for nothing while it closes**
// (mqtt.Client.HangingUp), because until then it is still connected: a queue
// re-offering the job it was released with chose the same worker again at
// once, and every publish too large for a shared member began another.
//
// A takeover's DISCONNECT to the connection it replaces stays where it is:
// it holds only the connection taking the client id over, and its order
// with the unsubscribe and EndTakenOver after it is invariant 17's.
func (b *Broker) hangUp(cl *mqtt.Client, code packets.Code) bool {
	if cl.Closed() || !cl.BeginHangUp() {
		return false
	}
	b.drains.Add(1)
	go func() {
		defer b.drains.Done()
		b.endConnection(cl, code)
	}()
	return true
}

// isAcknowledgement reports whether a packet is one saguin answers a
// client's own request with - the packets the substrate builds by copying
// that request's properties.
//
// Written as the closed set rather than as "not a PUBLISH", because the
// next packet type somebody adds should have to be considered rather than
// silently included.
func isAcknowledgement(t byte) bool {
	switch t {
	case packets.Connack, packets.Puback, packets.Pubrec,
		packets.Pubcomp, packets.Suback, packets.Unsuback:
		return true
	}
	return false
}

// ackProperties is the User Properties an acknowledgement may carry: the
// client's own, and the reserved names saguin itself writes there.
//
// `saguin-unread` is the only one, and it is named rather than matched by a
// pattern so that a client cannot construct a name that survives.
func ackProperties(user []packets.UserProperty) []packets.UserProperty {
	kept := user[:0]
	for _, u := range user {
		// **Everything the client sent, not only the reserved names.** The
		// prefix was only half of it; MQTT draws the line
		// wider. On an acknowledgement a User Property is *the sender's*
		// diagnostic information - section 3.4.2.2.3 - and [MQTT-3.1.2-29]
		// lets a client switch these off with Request Problem Information
		// while exempting PUBLISH, because a publish's properties are
		// application data being forwarded rather than problem information
		// about a failure. So a publisher's own labels coming back on the
		// packet that answers it put application data in the field the
		// protocol reserves for what the broker has to say.
		//
		// It helped nobody either: a client already knows what it sent. It
		// cost two of Eclipse Paho's tests, whose client stops reading an
		// acknowledgement carrying anything extra - so its own messages
		// were never completed - and would cost any client written that
		// way.
		//
		// **The substrate covers PUBACK, PUBREC, PUBREL and PUBCOMP** in
		// buildAck (server.go), always. It does not build a
		// SUBACK or an UNSUBACK through the same function, which is why
		// this is here as well rather than instead: one rule, and the two
		// halves reach different packets.
		if u.Key != unreadProp {
			continue
		}
		kept = append(kept, u)
	}
	return kept
}

// OnPacketEncode adds to the CONNACK the two things it is meant to say
// about this broker and does not, on the packet's way out.
//
// The substrate's SendConnack writes Receive Maximum, Maximum QoS, the
// assigned client id, a server keepalive and a shortened session expiry,
// and nothing else - whatever Capabilities holds. Measured at a socket
// before this existed, the whole CONNACK body was `0000052104002401`: five
// bytes of properties, Receive Maximum and Maximum QoS, and no other
// identifier at all. So a client was told neither bound it is entitled to
// know, and learned each one by breaking it.
//
//   - **Maximum Packet Size.** The server refuses an oversized packet
//     against the declared length before reading the body, which is right
//     and is earlier than any hook - but the refusal is a closed socket
//     with nothing on it, because ErrPacketTooLarge comes out of
//     ReadFixedHeader into cl.Stop and only an error out of processPacket
//     ever reaches the code that writes a DISCONNECT. Measured: a PUBLISH
//     declaring 2MiB against a 1MiB bound answered EOF and no packet.
//     Advertising the number is therefore not a nicety, it is the only
//     warning a conforming client can get, and RFC 0002 has always said it
//     was sent.
//
//   - **Retain Available.** MQTT reads the absence of this one as
//     *supported*, which is right for a broker holding a latest channel and
//     wrong for one holding none - and the two are indistinguishable to a
//     client, which discovers the difference by being disconnected with
//     0x9A mid-session. Saying it costs two bytes.
//
//   - **Topic Alias Maximum.** Absence reads as zero, so a client that is
//     told nothing sends no alias and pays for its long topics on every
//     publish. Saying the cap out loud is what lets a constrained link use
//     the one MQTT 5 feature that was designed for it.
//
// Only a CONNACK that accepted the connection. A refusal carries a reason
// code and closes the connection, and the bounds of a broker a client is
// not connected to are not information, they are noise on the way out.
func (b *Broker) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	// **The reserved prefix comes off every acknowledgement, here, because
	// this is the one place all of them pass.** The substrate answers a
	// request by copying its User Properties onto the acknowledgement, so a
	// client's own `saguin-filter` came back on the SUBACK, and the
	// `saguin-slice` a subscription had just been *refused for* came back
	// on that refusal - saguin's prefix on a packet saguin wrote, naming
	// something saguin does not read.
	//
	// **Here rather than at each hook, because one of them has no hook.** A
	// publish an ACL denies is answered before OnPublish is ever called -
	// the substrate builds the PUBACK from the raw packet and returns - so
	// a fix at the hooks would have covered the SUBACK and left the
	// refusal, which is the case a client is most likely to look at. This
	// is the last thing every outgoing packet passes through, which is the
	// same reason the CONNACK translation below lives here.
	//
	// **Acknowledgements only.** A PUBLISH carries saguin's own stamps -
	// `saguin-id`, `saguin-offset`, `saguin-timestamp` - and they are the
	// whole point of the prefix; stripping them here would empty every
	// delivery the broker makes.
	if isAcknowledgement(pk.FixedHeader.Type) {
		// **The sentence a refused SUBSCRIBE was owed, lifted before the
		// strip below removes its carrier.** RFC 0003 promises that a
		// refused declaration is answered "with the reason naming the
		// property and stating the form", and until this existed those
		// sentences went only to the broker's log - which the author of the
		// refused client cannot read. A refused PUBLISH already carried its
		// sentence on the wire; a refused SUBSCRIBE did not, and the
		// difference was an accident of which packet the substrate builds.
		//
		// **Only when there is none.** The substrate sets its own Reason
		// String for a packet identifier already in use, and that answer is
		// about the packet rather than about saguin's rules.
		//
		// **Problem information is not decided here.** The substrate sets
		// Mods.DisallowProblemInfo before this hook and the encoder honours
		// it afterwards, so a client that asked for no problem information
		// gets none of this without a second copy of MQTT-3.1.2-29 living
		// in saguin.
		if pk.FixedHeader.Type == packets.Suback && pk.Properties.ReasonString == "" {
			for _, u := range pk.Properties.User {
				if u.Key == reasonProp {
					pk.Properties.ReasonString = u.Val
					break
				}
			}
		}
		pk.Properties.User = ackProperties(pk.Properties.User)
	}

	if pk.FixedHeader.Type != packets.Connack {
		return pk
	}
	if pk.ReasonCode != packets.CodeSuccess.Code {
		// **A refusal on its way to a 3.1.1 client, wherever it came from**
		// (RFC 0002 "Every reason code, in one place"). This is the last
		// place every CONNACK passes, which is what makes it the right one:
		// the Will refusals are saguin's own and could be translated where
		// they are sent, but a wrong credential is refused by the substrate
		// before any saguin hook is asked, so a fix at saguin's send sites
		// would leave the one refusal a fleet meets most often answering a
		// code 3.1.1 does not define.
		return legacyConnack(cl, pk)
	}
	pk.Properties.MaximumPacketSize = b.limits.MaxMessageSize
	// **A session the acl_file denies is told it ends with the connection**,
	// in the one property MQTT gives a server for saying so. The flag is what
	// puts a zero on the wire; connectAuth has already set the session's own
	// interval to match.
	if !legacyClient(cl) && b.deniesFeature(cl, featurePersistent) {
		pk.Properties.SessionExpiryInterval = 0
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	// **A session the store had no room for is told the same**, in the same
	// property: it asked to be kept, nothing could keep it, and saying so is
	// what MQTT gives a server rather than refusing the connection.
	if !legacyClient(cl) && b.sessionWasNotKept(cl) {
		pk.Properties.SessionExpiryInterval = 0
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	// **A client denied shared subscriptions is told so here**, in the
	// property MQTT gives a server for it, so a conforming client never sends
	// one. One that does is disconnected `0x9E` in OnSubscribe, which is what
	// the specification says follows (MQTT 5 section 3.2.2.3.13).
	if !legacyClient(cl) && b.deniesFeature(cl, featureShare) {
		pk.Properties.SharedSubAvailable = 0
		pk.Properties.SharedSubAvailableFlag = true
	}
	// **A client denied QoS 2 is told Maximum QoS 1**, so a conforming client
	// never publishes above it, and one that does is disconnected `0x9B` in
	// OnPacketRead, which is what MQTT-3.2.2-11 says follows. The encoder
	// writes the property only below 2, so a client offered QoS 2 is told
	// nothing, which MQTT reads as 2. 3.1.1 has no such property: its QoS 2
	// publish closes the connection.
	if !legacyClient(cl) {
		if c := b.qosCeilingFor(cl); c < 2 {
			pk.Properties.MaximumQos = c
			pk.Properties.MaximumQosFlag = true
		}
	}
	// The encoder writes this one only when it is above zero, which is
	// exactly right: zero means "no aliases", and MQTT reads an absent
	// property as zero, so a broker offering none says it by saying
	// nothing.
	pk.Properties.TopicAliasMaximum = topicAliasMaximum
	// The flag rather than the value is what puts a zero on the wire: the
	// encoder writes this property only when the flag is set, so a broker
	// that can keep nothing would otherwise say nothing, which reads as the
	// opposite.
	pk.Properties.RetainAvailable = b.retainAvail
	pk.Properties.RetainAvailableFlag = true
	return pk
}

// channelName is the channel a Will reached, for a log line, or the word
// for none. A channel name comes from the operator's file.
func channelName(c *channel.Channel) string {
	if c == nil {
		return "broadcast"
	}
	return c.Name
}

// LatestPositionsHeld is how many consumers saguin is holding a `latest`
// position for. It exists for the test that says an expired session takes
// its position with it: the leak it guards against is invisible from the
// wire, because a client that never comes back never asks for anything.
func (b *Broker) LatestPositionsHeld() int {
	return b.latestSeen.clients()
}

// declaredBy is what a client declared, by filter, as the operations
// consumers route answers it: the count first and then the indices it took,
// so one entry reads as "3 slices, and I hold 0 and 2".
//
// Nil for a client that declared nothing, which is almost every client -
// the field is omitted from the JSON entirely rather than answered as an
// empty object.
func (b *Broker) declaredBy(clientID string) []DeclaredSlice {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.partitions[clientID]) == 0 {
		return nil
	}
	out := make([]DeclaredSlice, 0, len(b.partitions[clientID]))
	for filter, p := range b.partitions[clientID] {
		out = append(out, DeclaredSlice{
			Filter: filter, Count: p.count, Indices: append([]int(nil), p.indices...)})
	}
	// Sorted, because a map's order is not one and an operator comparing two
	// scrapes of this route should not see rows move.
	sort.Slice(out, func(i, j int) bool { return out[i].Filter < out[j].Filter })
	return out
}

// DeclaredSlices is the partition declarations saguin holds for a client,
// by the filter each was made on, or nil.
//
// It exists for the test that says a declaration does not outlive the
// session that made it. That leak is invisible from the wire - nothing is
// delivered without a subscription, so an entry against a filter the client
// no longer holds shows up as nothing at all until the day something reads
// it - which is the same shape as the position leak LatestPositionsHeld
// guards, and needs the same kind of window into the broker.
func (b *Broker) DeclaredSlices(clientID string) map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.partitions[clientID]) == 0 {
		return nil
	}
	out := make(map[string]int, len(b.partitions[clientID]))
	for filter, p := range b.partitions[clientID] {
		out[filter] = p.count
	}
	return out
}

// legacyClient reports whether this connection declared a protocol version
// below MQTT 5.
//
// **A version of zero is not a 3.1.1 client and must not be read as one.**
// The obvious test, `ProtocolVersion < 5`, calls an unset field legacy -
// and an in-process client that nobody stamped has one. Measured: with
// `sessionExpiry` written that way, three position tests failed, because a
// hand-built client carrying a Session Expiry Interval and no version was
// given the 3.1.1 rule instead, took the broker's ceiling of zero, and
// silently had no durable session at all. saguin's own three in-process
// publishers are each stamped 5 where they are made, so nothing in the
// binary was wrong - but a rule whose correctness rests on every future
// caller remembering to stamp a field is not a rule, and the failure it
// produces is a session quietly not being stored.
//
// So the question is asked of a version that was actually declared. Zero
// takes the MQTT 5 path, which is the one that stores, replays and refuses
// nothing extra.
func legacyClient(cl *mqtt.Client) bool {
	v := cl.Properties.ProtocolVersion
	return v > 0 && v < 5
}

// shareRefusal reports why a subscription asking for a shared subscription
// cannot be given one, or "".
//
// **Two ways to ask for something this connection cannot have, and one
// answer.** A filter beginning `$share/` is served only to a protocol that
// defines what those characters mean and in the spelling that protocol
// defines - otherwise the client is granted a filter nothing publishes to
// and waits for ever having been told it succeeded.
//
// **Neither of these is a channel rule and that is why both survived the
// change that deleted the rest.** saguin no longer reserves a ShareName,
// no longer asks which channels a shared filter touches, and refuses no
// `$share` filter for anything it reaches. These two ask nothing about the
// broker's configuration - one is a spelling, one is a protocol version -
// they cost a string comparison each, and each of them catches a filter
// that would otherwise match nothing for ever.
//
// The 3.1.1 half is RFC 0002 "What each channel type admits". Its
// specification has no shared subscriptions at all, so
// `$share/grp/events/#` from such a client is an ordinary topic filter
// over a topic space nothing publishes to - saguin refuses a publish
// beginning `$share/` outright. The substrate reads it as a shared
// subscription anyway and starts delivering `events/#` to it: a client
// served messages its own protocol says it did not ask for.
//
// **The third is the shape**, which is MQTT 5's own rule and costs no more
// than the other two: `$share/<ShareName>/<filter>`, with a filter that is
// not a shared subscription again (channel.ShareMalformed). What the filter
// reaches is not asked here - a filter inside a reserved space is refused
// in OnACLCheck, with the code it gets alone.
func (b *Broker) shareRefusal(cl *mqtt.Client, filter string) string {
	if channel.MisspelledShare(filter) {
		return "$share is spelled in lower case and no other"
	}
	if legacyClient(cl) && strings.HasPrefix(filter, "$share/") {
		return "MQTT 3.1.1 has no shared subscriptions, so this is an ordinary " +
			"filter that nothing publishes to"
	}
	return channel.ShareMalformed(filter)
}

// shareDenied reports whether this filter is a shared subscription the
// client's roles deny it (`broker: features`, `deny: [share]`).
//
// **MQTT 5 only.** A 3.1.1 client is refused every `$share/` filter already,
// by shareRefusal, because its protocol has no shared subscriptions; a denial
// changes nothing for it.
func (b *Broker) shareDenied(cl *mqtt.Client, filter string) bool {
	return !legacyClient(cl) && strings.HasPrefix(filter, "$share/") &&
		b.deniesFeature(cl, featureShare)
}

// queueRefusal reports why this connection may not consume a queue through
// the form it sent, or "". The filter is assumed to be a queue's canonical
// form; the caller has already asked CanonicalQueue.
//
// **MQTT 5 and no lower, and this is the rule that moved rather than the
// rule that went.** While a queue was consumed through
// `$share/saguin/<name>/#`, shareRefusal covered it: the pin began
// `$share/` and a 3.1.1 client was turned away by the clause above. The
// pin is `$saguin/queue/<name>` now and carries no such prefix, so without
// this a 3.1.1 worker is granted it and fed jobs.
//
// **That is worse than receiving nothing, and it is data-affecting.**
// Acknowledgement is a publish to the Response Topic echoing the
// Correlation Data the delivery arrived with. Both are MQTT 5 properties a
// 3.1.1 client cannot read and cannot set, so such a worker takes work it
// can never resolve: every job it is handed times out, redelivers, and
// dead-letters on attempt exhaustion. Measured before the pin moved, with
// the refusal reverted: a 3.1.1 worker was granted the pin and received 3
// of 3 jobs.
func (b *Broker) queueRefusal(cl *mqtt.Client) string {
	if legacyClient(cl) {
		return "a queue is consumed over MQTT 5: a delivery carries its Response Topic " +
			"and Correlation Data as MQTT 5 properties, and 3.1.1 has neither, so a " +
			"3.1.1 worker could never acknowledge the work it was given"
	}
	return ""
}

// filterMalformed reports why a filter is not a topic filter at all, or ""
// when it is one.
//
// **It asks saguin's own validator**, the one that refuses an operator's
// filter at startup, so `+` and `#` mean the same thing in a configuration
// file and on the wire. [MQTT-4.7.1-1] and [MQTT-4.7.1-2] give each of them
// a whole level and [MQTT-4.7.3-1] requires at least one character, and a
// broker enforcing those in one place and not the other has two rules.
//
// **A shared subscription is judged by the filter inside it.** `$share/g/`
// is a wrapper MQTT defines, so what has to be a legal filter is what
// follows the group - and the wrapper's own shape is somebody else's
// refusal, in shareRefusal.
//
// The substrate does its own check and it is looser than this: measured
// against `sammiq/mqtt_test`, `sport+/x` and `sport/tennis+` were both
// granted, which tells a client its filter is live when nothing will ever
// match it.
func filterMalformed(filter string) string {
	inner := channel.InnerFilter(filter)
	if inner == "" {
		return "a topic filter is at least one character (MQTT-4.7.3-1)"
	}
	if err := channel.ValidWildcards(inner); err != nil {
		return fmt.Sprintf("%q is not a topic filter: %v (MQTT-4.7.1-1, -2)", inner, err)
	}
	return ""
}

// filterAbsent reports whether a SUBSCRIBE carries a filter that is not a
// string at all - zero length, which [MQTT-4.7.3-1] forbids.
//
// **It is answered by closing the connection rather than by a code in the
// SUBACK**, and the difference from every other filter refusal is what the
// packet is. `0x8F Topic Filter invalid` exists for a well-formed string
// that is not a legal filter - `sport+/x` is one, and a client can read that
// answer and fix its filter. A zero-length filter is not a filter the server
// dislikes; it is a field the client did not send, which section 4.13.1 calls
// a Protocol Error and answers by closing the connection.
//
// Mosquitto 2.0.22 does the same, measured: it disconnects where saguin
// answered a SUBACK, which is the difference `vibesrc/mqttconformance` found.
func filterAbsent(pk packets.Packet) bool {
	for _, f := range pk.Filters {
		if channel.InnerFilter(f.Filter) == "" {
			return true
		}
	}
	return false
}

// OnSubscribe refuses a subscription with the code that describes it,
// per filter, and is the only hook that can.
//
// The authorization check answers `0x87` (Not authorized) and nothing else,
// so before this existed every refusal here pointed a developer at their
// credentials rather than at the shape of their filter - which RFC 0002
// documented as a wart so nobody lost an afternoon to it. It also had no
// way to see the requested QoS, so QoS 0 on a queue could only be caught
// afterwards, by disconnecting.
//
// Two codes, each the one MQTT provides for what is actually wrong:
//
//   - `0x8F` (Topic Filter invalid) for a queue subscribed through anything
//     but its canonical form - a narrower or wider filter, a spelling under
//     `$saguin/queue/` naming no queue, or a filter lying inside a queue's
//     own filter. One consumer group is one exact string, and a second
//     spelling that worked would be a second population taking a copy of
//     every job (invariant 4).
//   - `0x83` (Implementation specific error) for QoS 0 on a queue. There is
//     no transport acknowledgement to start a visibility timeout from
//     (invariant 7), and MQTT permits a server to grant a lower QoS than
//     asked for but never a higher one, so it cannot be quietly upgraded
//     into something that works. Also, on every filter of the packet, for a
//     `saguin-` property saguin does not read and for a partition
//     declaration that is malformed or self-inconsistent (RFC 0003
//     "Client-declared partitioning") - the checks at the top of this
//     function, where the property names no filter so there is nothing
//     narrower to refuse. A valid declaration on `$share/…` or a queue's
//     form refuses that filter alone, and the rest of the packet is
//     granted with the declaration applied.
//
// **`0x9E` is gone**, and with it every refusal that had to work out which
// channels a filter touched. A shared subscription is granted on every
// channel type and served live messages only - which is what `$share`
// means everywhere else - so there is no longer such a thing as a shared
// subscription this broker cannot honour.
func (b *Broker) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	codes := make([]byte, len(pk.Filters))
	refused := false
	// What the engine has already refused, decided once before this hook
	// (mqtt.Server.processSubscribe) and answered as handed back below.
	engine := pk.ReasonCodes

	// **A client denied QoS 2 asks for it and is granted 1**, as a broker
	// offering Maximum QoS 1 grants it: MQTT grants the lower of what was
	// asked and what the server supports (MQTT-3.8.4-7), and the substrate
	// caps only at the broker-wide ceiling, so the request is lowered here
	// before it decides. OnSubscribed holds every delivery to the same
	// ceiling for a substrate that ignored this.
	if ceiling := b.qosCeilingFor(cl); ceiling < 2 {
		for i := range pk.Filters {
			if pk.Filters[i].Qos > ceiling {
				pk.Filters[i].Qos = ceiling
			}
		}
	}

	// **A malformed partition declaration is on the packet, so it answers
	// every filter in it.** The property is packet-level (RFC 0003), so
	// there is no per-filter answer to give: the whole SUBSCRIBE is
	// unanswerable, and granting some of its filters would leave the client
	// holding half of what it asked for with no way to tell which half.
	// **The same rule as CONNECT, and packet-level for the same reason the
	// declaration is**: the property names no filter, so there is no
	// per-filter answer to give.
	if key := unreadReserved(pk, subscribeProps); key != "" {
		b.log.Warn("refused a subscription carrying a reserved property saguin "+
			"does not read", "client", cl.ID, "property", b.limits.Loggable(key))
		for i := range codes {
			codes[i] = packets.ErrImplementationSpecificError.Code
		}
		refusedBecause(&pk, fmt.Sprintf("the User Property %q is reserved and "+
			"saguin does not read it on a SUBSCRIBE", key))
		pk.ReasonCodes = codes
		return pk
	}

	// **A filter that is not a string is a Protocol Error**, so the
	// connection goes rather than the subscription being answered. See
	// filterAbsent for why this one is not a SUBACK code like the rest.
	if filterAbsent(pk) {
		b.log.Warn("disconnecting: a SUBSCRIBE carries a zero-length topic filter",
			"client", cl.ID)
		b.disconnect(cl, packets.ErrProtocolViolationNoFilters)
		for i := range codes {
			codes[i] = packets.ErrTopicFilterInvalid.Code
		}
		pk.ReasonCodes = codes
		return pk
	}

	// **A shared subscription the client's roles deny is a Protocol Error**,
	// because its CONNACK said Shared Subscription Available 0 - so the
	// connection goes, `0x9E`, as MQTT 5 section 3.2.2.3.13 says it does,
	// rather than the one filter being answered.
	for _, f := range pk.Filters {
		if b.shareDenied(cl, f.Filter) {
			b.log.Warn("disconnecting: a SUBSCRIBE asks for a shared subscription this "+
				"client's roles deny", "client", cl.ID, "filter", b.limits.Loggable(f.Filter))
			b.disconnect(cl, packets.ErrSharedSubscriptionsNotSupported)
			for i := range codes {
				codes[i] = packets.ErrSharedSubscriptionsNotSupported.Code
			}
			pk.ReasonCodes = codes
			return pk
		}
	}

	// **No Local on a shared subscription is a Protocol Error**
	// [MQTT-3.8.3-4], so the connection goes with `0x82` as section 4.13
	// says a Protocol Error does - the shape the two refusals above it take.
	//
	// It is answered here rather than left to the substrate, which put
	// `0x82` in the SUBACK instead: that is not one of the reason codes a
	// SUBACK may carry (section 3.9.3), so a client was handed a code it
	// cannot interpret on a connection the specification says should have
	// been closed. Measured before this - Paho read it as a bare failure
	// with no reason, and the connection went on publishing afterwards.
	//
	// The SUBACK codes are set to `0x8F` for the case where one still
	// reaches the wire ahead of the DISCONNECT: it is a code a SUBACK may
	// carry, and it is true of the filter as sent.
	for _, f := range pk.Filters {
		if !f.NoLocal || !mqtt.IsSharedFilter(f.Filter) {
			continue
		}
		b.log.Warn("disconnecting: a SUBSCRIBE sets No Local on a shared subscription",
			"client", cl.ID, "filter", b.limits.Loggable(f.Filter))
		b.disconnect(cl, packets.ErrProtocolViolationInvalidSharedNoLocal)
		for i := range codes {
			codes[i] = packets.ErrTopicFilterInvalid.Code
		}
		pk.ReasonCodes = codes
		return pk
	}

	declared, badPartition := partitioning(pk)
	if badPartition != "" {
		b.log.Warn("refused a subscription whose partition declaration cannot be read",
			"client", cl.ID, "reason", badPartition)
		for i := range codes {
			codes[i] = packets.ErrImplementationSpecificError.Code
		}
		refusedBecause(&pk, badPartition)
		pk.ReasonCodes = codes
		return pk
	}

	for i, f := range pk.Filters {
		switch {
		case filterMalformed(f.Filter) != "":
			// **The filter is not a filter**, which is a different answer
			// from every refusal below it: those are about what this
			// connection may have, and this is about what the client sent.
			// `0x8F Topic Filter invalid` is the code MQTT names for it,
			// and on 3.1.1 it reaches the wire as `0x80`, that protocol's
			// only failure.
			//
			// **saguin's own rule, not a second copy of it.**
			// channel.ValidWildcards is what refuses an operator's filter at
			// startup, and applying it here is what stops the two answering
			// differently - a broker that refuses `sport+/x` in its
			// configuration and grants it to a client has two rules and one
			// of them is wrong.
			codes[i] = packets.ErrTopicFilterInvalid.Code
			b.log.Warn("refused a malformed subscription filter",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", b.limits.Loggable(filterMalformed(f.Filter)))
			refusedBecause(&pk, filterMalformed(f.Filter))
		case channel.TooDeep(channel.InnerFilter(f.Filter), b.limits.MaxTopicLevels) != nil:
			// **Deeper than limits.max_topic_levels**: no topic may be that
			// deep, so it could match nothing, and every level is a node the
			// topic index holds for as long as the subscription lasts. The
			// same 0x8F, measured after `$share/<name>/`. Asked of a new
			// SUBSCRIBE only: a session restored holding a filter over a
			// lowered bound keeps it, as one over a lowered max_subscriptions
			// keeps its filters, since ending it would drop what it is owed.
			why := "the topic filter " + channel.TooDeep(channel.InnerFilter(f.Filter), b.limits.MaxTopicLevels).Error()
			codes[i] = packets.ErrTopicFilterInvalid.Code
			b.log.Warn("refused a subscription filter deeper than max_topic_levels",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter), "reason", b.limits.Loggable(why))
			refusedBecause(&pk, why)
		case declared.declared() && partitionRefusal(f.Filter) != "":
			// **`0x83` rather than `0x8F`, and the difference is what the
			// client has to change.** `0x8F` means "a queue subscribed
			// through anything but its canonical form", where the filter is
			// the thing to fix. Here the filter is the one part that is not
			// negotiable - the client wants that queue, or that shared
			// group - and the declaration is what must go. Pointing at the
			// filter would send them at the wrong half.
			codes[i] = packets.ErrImplementationSpecificError.Code
			b.log.Warn("refused a partition declaration this filter cannot carry",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", partitionRefusal(f.Filter))
			refusedBecause(&pk, partitionRefusal(f.Filter))
		case b.shareRefusal(cl, f.Filter) != "":
			why := b.shareRefusal(cl, f.Filter)
			// On 3.1.1 the code goes out as `0x80`, which is the only
			// failure a 3.1.1 SUBACK carries - and the subscribe side is the
			// one place in that protocol where saguin can refuse in words at
			// all.
			codes[i] = packets.ErrTopicFilterInvalid.Code
			b.log.Warn("refused a shared subscription this connection cannot have",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", why)
			refusedBecause(&pk, why)
		case f.NoLocal && b.reg.CanonicalQueue(f.Filter) != nil:
			// **No Local and a queue cannot both be honoured, so the
			// subscription is refused rather than half-served.** Withholding
			// a job from the one worker whose client id published it leaves
			// that job unofferable while a worker holds the queue open: it
			// is never delivered, so it is never acknowledged, never times
			// out and never dead-letters, and nothing anywhere reports a job
			// that no longer moves. A single worker publishing its own work
			// is all it takes.
			//
			// This is [MQTT-3.8.3-4]'s own reasoning - echo-suppression is
			// incoherent under work distribution, which is why MQTT makes
			// the flag a Protocol Error on a shared subscription. A queue's
			// canonical form is saguin's rather than MQTT's, so saguin says
			// it here, in the SUBACK: the client keeps its connection and
			// gets a code naming the filter it must change (RFC 0003).
			codes[i] = packets.ErrTopicFilterInvalid.Code
			b.log.Warn("refused a queue subscription asking for No Local",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", noLocalOnAQueue)
			refusedBecause(&pk, noLocalOnAQueue)
		case b.reg.CanonicalQueue(f.Filter) != nil && b.queueRefusal(cl) != "":
			codes[i] = packets.ErrTopicFilterInvalid.Code
			b.log.Warn("refused a queue subscription this connection cannot have",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", b.queueRefusal(cl))
			refusedBecause(&pk, b.queueRefusal(cl))
		default:
			// **A QoS 2 request on a queue's form is granted at 1, never at
			// 2.** The offer is sent at QoS 1 whatever the grant says
			// (RFC 0003 "Delivery"), so on a `broker.qos2` broker - where the
			// substrate's ceiling is 2 - an uncapped request was answered
			// "granted 2" and then served at 1: a grant the delivery never
			// honours. MQTT grants the lower of what was asked and what is
			// offered ([MQTT-3.8.4-8]), so the request is capped here, before
			// the substrate stores it, and the stored subscription, the
			// SUBACK and every delivery agree. QoS 0 stays a refusal below:
			// it cannot work degraded, having no acknowledgement to start
			// the visibility timeout from.
			if f.Qos > 1 && b.reg.CanonicalQueue(f.Filter) != nil {
				pk.Filters[i].Qos = 1
				f.Qos = 1
			}
			c, why := b.reg.QueueSubscriptionError(f.Filter, f.Qos)
			if why == "" {
				continue
			}
			// **A nil channel is the refusal that names no queue** - a
			// spelling in the queue space that no queue answers to. It gets
			// a line of its own rather than a blank in the queue's: there is
			// no canonical form to print, and an operator reading this is
			// looking for a name rather than for a channel.
			if c == nil {
				codes[i] = packets.ErrTopicFilterInvalid.Code
				b.log.Warn("refused a subscription in the queue space naming no queue",
					"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
					"reason", why)
				refusedBecause(&pk, why)
				break
			}
			// The QoS is the only thing wrong with it when the filter is
			// already the canonical form.
			if f.Filter == c.QueueFilter() {
				codes[i] = packets.ErrImplementationSpecificError.Code
			} else {
				codes[i] = packets.ErrTopicFilterInvalid.Code
			}
			b.log.Warn("refused queue subscription", "client", cl.ID,
				"filter", b.limits.Loggable(f.Filter),
				"reason", why, "canonical", c.QueueFilter(), "code", codes[i])
			refusedBecause(&pk, why)
		}
		refused = true
	}

	// **The substrate's own refusals, as it decided them**, so that what the
	// session store is asked below to keep is exactly what the SUBACK grants:
	// the engine answers with these codes and asks nothing again. A refusal
	// of this hook's own keeps its code, as it always did.
	if len(engine) == len(pk.Filters) {
		for i, c := range engine {
			if c >= packets.ErrUnspecifiedError.Code && codes[i] < packets.ErrUnspecifiedError.Code {
				codes[i] = c
				refused = true
			}
		}
	}

	// **At most limits.max_subscriptions filters a client**, counted once
	// every refusal above is known and before the store is asked to keep any
	// (capSubscriptions).
	if b.capSubscriptions(cl, &pk, codes) {
		refused = true
	}

	// Which of these filters the client already held, recorded here because
	// this is the last moment it can be: the substrate applies the packet
	// after this hook returns, so by OnSubscribed every filter exists.
	// MQTT's Retain Handling 1 asks exactly this question - send retained
	// messages only if the subscription is new - and answering it wrong
	// means sending a client state it asked not to be sent again.
	had := make(map[string]bool, len(pk.Filters))
	for _, f := range pk.Filters {
		if _, ok := cl.State.Subscriptions.Get(f.Filter); ok {
			had[f.Filter] = true
		}
	}
	b.mu.Lock()
	b.existed[cl.ID] = had
	b.mu.Unlock()

	// **The session store is asked last**, once every refusal above is
	// known, so what it is asked to keep is only what may be granted. A
	// store with no room refuses the new filters `0x97` and keeps the
	// session as it was, so the substrate leaves any existing subscription
	// untouched. A durable member's shared group with no cursor is given
	// one in the same write, before the substrate indexes the filter, so no
	// publish can match the member while its group has no cursor: joined
	// after the SUBACK, a publish in between went to the member live
	// (selectShared) and was recorded nowhere, lost across a restart.
	if b.keepSubscriptions(cl, &pk, codes) {
		refused = true
	}

	if !refused {
		return pk
	}
	pk.ReasonCodes = codes
	return pk
}

// capSubscriptions refuses the filters of a SUBSCRIBE that would take its
// client past limits.max_subscriptions, `0x97` each - `0x80` on 3.1.1, that
// protocol's one failure - and reports whether it refused any.
//
// **A count, because a filter is memory the store's charge never saw**:
// about 2.3KB a filter across the three indexes that hold it, charged 20
// bytes, and one connection granted 100,000 of them took 201MiB. EMQX's max_subscriptions answers the same way, filter by
// filter, and keeps the client connected.
//
// What counts is what the client would hold after this SUBSCRIBE: a filter it
// already has is replaced and adds nothing, a filter named twice in one
// packet is one, and a shared subscription is one filter like any other. A
// filter already refused above is not counted.
//
// **Said once an episode**, the first refusal since the client last added a
// filter, so a client subscribing in a loop cannot choose how much the log
// holds.
func (b *Broker) capSubscriptions(cl *mqtt.Client, pk *packets.Packet, codes []byte) bool {
	max := b.limits.MaxSubscriptions
	if max <= 0 {
		return false
	}
	held := cl.State.Subscriptions.Len()
	named := make(map[string]bool, len(pk.Filters))
	overBound, accepted := 0, false
	for i, f := range pk.Filters {
		if codes[i] >= packets.ErrUnspecifiedError.Code {
			continue
		}
		// Held already, or named earlier in this packet: replaced, adding
		// nothing - and so not the end of an episode either, or a client
		// at its bound alternating a filter it holds with one it may not
		// add would be said once a refusal.
		if _, ok := cl.State.Subscriptions.Get(f.Filter); ok || named[f.Filter] {
			continue
		}
		if held < max {
			held++
			named[f.Filter] = true
			accepted = true
			continue
		}
		codes[i] = packets.ErrQuotaExceeded.Code
		overBound++
	}
	b.mu.Lock()
	first := overBound > 0 && !b.subscriptionsCapped[cl]
	switch {
	case overBound > 0:
		b.subscriptionsCapped[cl] = true
	case accepted:
		delete(b.subscriptionsCapped, cl)
	}
	b.mu.Unlock()
	if overBound == 0 {
		return false
	}
	refusedBecause(pk, fmt.Sprintf("this client holds limits.max_subscriptions (%d) topic filters", max))
	if first {
		b.log.Warn("refused subscriptions: the client holds limits.max_subscriptions topic filters",
			"client", b.limits.Loggable(cl.ID), "max_subscriptions", max, "refused", overBound)
	}
	return true
}

// OnACLCheck is a second copy of the subscription rules, and it is here
// because the substrate decides what a refusal does.
//
// OnSubscribe answers the reason code and OnSubscribed disconnects anything
// granted that should not have been; this catches what it can at the
// authorization check, which is the one refusal path every mochi honours.
// Three checks for one rule is more than it looks like it needs, and the
// measurement is why: on a server that ignores per-filter reason codes, the
// first is advisory and only the last two are enforcement.
func (b *Broker) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if write {
		// Nothing structural refuses a write here - the reserved space, the
		// bounds and the channel rules are all applied on the publish path,
		// where the payload and the reason code are - so this is the whole
		// of the question for one.
		if b.permits(cl, topic, true) {
			return true
		}
		// **Counted here because here is where it ends.** A write the
		// acl_file refuses never reaches the publish path, so the counting
		// and the logging that path does never happen either - and
		// saguin_publish_refused_total, whose stated job is answering "the
		// fleet's data is not arriving" without reading a log, held no
		// series at all for the one refusal an operator is most likely to
		// have caused. Six publishes answered 0x87 and the number read
		// zero.
		//
		// **Warn, because its sibling is Warn.** The other half of this
		// same 0x87 - a client whose roles do not allow the verb - is
		// refused on the publish path by refuseUnauthorized, which logs at
		// Warn and is counted. Both are one publish refused by the
		// acl_file, both carry the same code, and both flood at the same
		// rate if a fleet's rule is wrong; splitting them across two levels
		// would mean an operator watching one saw half the story and could
		// not tell which half.
		//
		// The topic is a client string here - this runs before any registry
		// has bounded it - so it goes through Loggable, as the client id
		// does.
		b.counted.refuse(reasonNames[0x87])
		b.log.Warn("refused: the acl_file does not allow this client to publish here",
			"client", b.limits.Loggable(cl.ID),
			"topic", b.limits.Loggable(topic))
		return false
	}
	// Nothing under `$SYS` is saguin's to serve (RFC 0005), and this is the
	// one place that holds for every route it could arrive by: a live
	// publish and a retained value handed over on subscribe both reach a
	// client through publishToClient, which asks this hook first.
	//
	// It sits above the early return below because that return answers true
	// for anything without a wildcard, which `$SYS/broker/version` is - so
	// the check has to run before it to cover both the filter a client
	// subscribes with and the topic name a delivery carries. A subscription
	// to `$SYS/#` is therefore refused rather than granted and left silent,
	// which is the same rule saguin follows everywhere else: it does not
	// acknowledge a promise it cannot keep.
	//
	// Nothing is logged. This runs on every delivery to every subscriber, and
	// a client that subscribes to `$SYS/#` in a reconnect loop would
	// otherwise choose how much an operator's disk holds.
	if strings.HasPrefix(topic, mqtt.SysPrefix+"/") {
		return false
	}
	// **Nor into saguin's own reserved space**, for the same reason and with
	// the same answer. `$saguin/` holds the topics saguin defines, and most
	// of them are topics a client *publishes to* - a seek, a point read, a
	// queue's response. A subscription to one of those is a promise that can
	// never be kept, and this broker does not acknowledge one of those.
	//
	// It was once granted: SUBACK 0x01 to
	// anybody, `$SYS/#` refused beside it, and then silence for ever. The
	// silence is the cost - somebody watching `$saguin/queue/jobs/response`
	// to see their worker's acknowledgements reads it as a broker that is
	// not publishing them, and goes looking in the wrong place.
	//
	// The consequence to know: **nothing reaches this space by wildcard.**
	// MQTT already stops `#` matching a topic beginning with `$`, so the two
	// exceptions below are reachable only by a client spelling them out, and
	// nothing else here is reachable at all.
	//
	// Publishing is untouched: it is how every one of those verbs is asked
	// for, and it is answered on the publish path.
	if strings.HasPrefix(topic, channel.ReservedRoot+"/") {
		// **A seek reply.** It is written to the seeking client's socket
		// rather than published, so a subscription here exists to let a
		// client's own library route its own answer - it can never carry
		// another client's, whoever subscribes. Without it a 3.1.1 client
		// would receive the packet and have nothing to match it against.
		//
		// Publishing to it is still refused with everything else under the
		// prefix: this is not one of the topics saguin defines to be published
		// to. Not "one of the four" - a count written in prose goes stale in
		// silence the day a verb is added, and one of them already had.
		if name, ok := channel.SeekReplyChannel(topic); ok && b.reg.Get(name) != nil {
			return true
		}
		// **A queue's own form, which is the second and is a real
		// delivery.** Queue work is delivered here, so "nothing is delivered
		// under `$saguin/`" is no longer the rule - the rule is that a
		// subscription in this space is granted only where saguin defines
		// one, and there are two.
		//
		// This is permission rather than shape: whether the form itself is
		// well spelled is QueueSubscriptionError's answer, below, and it is
		// asked of a queue's form as of any other filter because the pin
		// holds no wildcard and would otherwise skip the whole block.
		if b.reg.CanonicalQueue(topic) != nil {
			if why := b.queueRefusal(cl); why != "" {
				b.log.Warn("refused a queue subscription this connection cannot have",
					"client", cl.ID, "filter", b.limits.Loggable(topic), "reason", why)
				return false
			}
			return b.permits(cl, topic, false)
		}
		if _, why := b.reg.QueueSubscriptionError(topic, 1); why != "" {
			b.log.Warn("refused a subscription in the queue space naming no queue",
				"client", cl.ID, "filter", b.limits.Loggable(topic), "reason", why)
		}
		return false
	}
	// The server calls this hook with a filter when a client subscribes
	// and with a concrete topic name when it delivers. The two are
	// indistinguishable from the arguments, so the subscription rules are
	// applied only to strings that cannot be a topic name: a wildcard, or
	// the shared-subscription prefix. A delivery of "jobs/build" is
	// neither, and must not be refused as though it were a subscription
	// to it.
	//
	// So this hook does not see a wildcard-free subscription to a queue
	// topic - "SUBSCRIBE jobs/build" reaches the same records without the
	// shared subscription that makes them exclusive. Nor can it see the
	// requested QoS: the argument list has no room for one, so the check
	// below passes 1 and a QoS 0 request looks identical to a valid one.
	//
	// Both are caught where the QoS is known: OnSubscribe answers the
	// reason code, and OnSubscribed disconnects a client left holding one
	// anyway. Invariant 4 holds across the three and not in any alone -
	// and the early return below is not dead weight, because a delivery of
	// "jobs/build" must not be refused as though it were a subscription
	// to it.
	if strings.ContainsAny(topic, "+#") || strings.HasPrefix(topic, "$share/") ||
		channel.MisspelledShare(topic) {
		if why := b.shareRefusal(cl, topic); why != "" {
			b.log.Warn("refused a shared subscription this connection cannot have",
				"client", cl.ID, "filter", b.limits.Loggable(topic), "reason", why)
			return false
		}
		// A shared subscription the client's roles deny. OnSubscribe has
		// already disconnected it and said so; this holds on a substrate
		// that ignores that.
		if b.shareDenied(cl, topic) {
			return false
		}
		// **A shared group gets nothing its filter would be refused alone**,
		// and in the reserved spaces that is everything. The two checks above
		// read the whole string, which begins `$share/`, so neither saw the
		// filter inside: `$share/g/$SYS/#` and `$share/g/$saguin/queue/jobs`
		// were granted and then silent.
		//
		// The exceptions `$saguin/` makes alone are not made here. A seek
		// reply is written to one client's socket and a queue's work to the
		// one worker saguin chose, so neither is ever delivered to a group.
		//
		// Not logged, as `$SYS/` and `$saguin/` alone are not.
		if inner := channel.InnerFilter(topic); inner != topic &&
			(strings.HasPrefix(inner, mqtt.SysPrefix+"/") || strings.HasPrefix(inner, channel.ReservedRoot+"/")) {
			return false
		}
		if c, why := b.reg.QueueSubscriptionError(topic, 1); why != "" {
			if c == nil {
				b.log.Warn("refused a subscription in the queue space naming no queue",
					"client", cl.ID, "filter", b.limits.Loggable(topic), "reason", why)
				return false
			}
			b.log.Warn("refused queue subscription",
				"client", cl.ID, "filter", b.limits.Loggable(topic),
				"reason", why, "canonical", c.QueueFilter())
			return false
		}
	}

	// **Last, and only after every refusal above has had its say.** The
	// structural rules are correctness and this is permission; a rule may
	// take permission away and may never give it, so this cannot run first
	// and cannot stand in for any of them.
	return b.permits(cl, topic, false)
}

// feeding is one granted filter and the channels it reached, held between
// OnSubscribed's two passes.
type feeding struct {
	f     packets.Subscription
	chans []*channel.Channel
}

// OnSubscribed feeds a granted subscription, and refuses one that should
// never have been granted.
//
// **The refusal here is the enforcement point; OnSubscribe is only the
// reason code.** A hook says what it wants and the server decides what to
// do about it, so a server that ignores `ReasonCodes` grants exactly the
// subscriptions saguin refused - and it says so in the log while doing it,
// which is the shape of failure this whole file is written against.
// Invariant 10 is the rule: the broker is the only enforcement point, and
// a substrate obliging is not the broker enforcing.
//
// Measured on mochi v2.7.9, which ignores them: `jobs/build` and the
// canonical queue filter at QoS 0 are both granted. The second is the
// serious one - a QoS 0 shared subscriber receives jobs, has no `PUBACK`
// to start a visibility deadline from (invariant 7), and takes them with
// it when it goes. No deadline ever runs, so nothing is redelivered and no
// worker is ever offered them again: work accepted and quietly dropped,
// which is invariant 2's failure.
//
// So every *granted* filter is checked again, and a client holding one it
// should not have is disconnected. On a substrate that honours the codes
// this never fires, because those filters were refused before they
// reached here. It is a duplicated check on purpose: the cost is one
// comparison per granted subscription, and what it buys is that the
// guarantee does not depend on which mochi somebody built against.
func (b *Broker) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	// Read again rather than carried from OnSubscribe: a hook says what it
	// wants and the server decides, so this runs on whatever was granted -
	// including on a substrate that ignored the reason codes above.
	declared, badPartition := partitioning(pk)

	var granted []feeding
	for i, f := range pk.Filters {
		if i >= len(reasonCodes) || reasonCodes[i] > 2 {
			continue // not granted
		}

		// **The granted QoS, never the requested one.** A reason code of 0,
		// 1 or 2 on a granted subscription *is* the QoS the server granted,
		// and MQTT grants the lower of what the client asked for and what
		// the broker supports - so the two differ exactly when a client asks
		// for more than the broker offers.
		//
		// saguin advertises Maximum QoS 1 and stored the requested value
		// here, so a client subscribing at QoS 2 was granted 1 in its SUBACK
		// and then sent every delivery at 2: replayed append records, latest
		// values, and the retained store alike. That is MQTT-3.2.2-9 - a
		// server must not send a PUBLISH above the Maximum QoS it declared -
		// and MQTT-3.8.4-8, which makes a delivery's QoS the lower of the
		// two. It also ran saguin's own in-flight accounting, written for
		// PUBACK, through the substrate's QoS 2 handshake, on a broker that
		// did not offer QoS 2 at all.
		//
		// **It is what makes exactly-once work now that saguin does offer
		// it**, rather than a fix that outlived its defect: a QoS 2
		// subscription is granted 2 wherever the broker offers QoS 2, and
		// the granted value is again what every delivery reads. What
		// completes the accounting is the PUBCOMP rather than the PUBACK,
		// which the substrate reports through the same hook.
		//
		// Normalising it here rather than at each delivery is what makes it
		// hold everywhere: `track` stores it, and every later reader -
		// pumpBatch, deliverLatest, publishLatest, deliverRetained - takes
		// its QoS from what `track` stored.
		f.Qos = reasonCodes[i]
		// A grant above this client's ceiling - a substrate that ignored the
		// lowered request in OnSubscribe - is served at the ceiling: a
		// delivery below the granted QoS is allowed, one above what the
		// CONNACK said is not (MQTT-3.2.2-9).
		if ceiling := b.qosCeilingFor(cl); f.Qos > ceiling {
			f.Qos = ceiling
		}
		if _, why := b.reg.QueueSubscriptionError(f.Filter, f.Qos); why != "" {
			b.log.Warn("disconnecting: a subscription saguin refused was granted anyway - "+
				"the substrate ignored the reason code",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"qos", f.Qos, "reason", why)
			b.disconnect(cl, packets.ErrQosNotSupported)
			return
		}
		if b.reg.CanonicalQueue(f.Filter) != nil {
			if why := b.queueRefusal(cl); why != "" {
				b.log.Warn("disconnecting: a queue subscription saguin refused was granted "+
					"anyway - the substrate ignored the reason code",
					"client", cl.ID, "filter", b.limits.Loggable(f.Filter), "reason", why)
				b.disconnect(cl, packets.ErrTopicFilterInvalid)
				return
			}
		}
		// **The malformed-declaration net, and it belongs inside this loop
		// rather than above it.** Every other safety net here runs only on
		// a *granted* filter, because that is what a safety net is for: a
		// substrate that honoured the reason codes granted nothing, and
		// there is nothing to catch.
		//
		// Run before the loop it disconnected unconditionally, and the
		// substrate this fork pins calls OnSubscribed *before* it writes the
		// SUBACK - so the connection died before the refusal could leave,
		// and a client that auto-resubscribes met a reconnect loop instead
		// of the `0x83` RFC 0003 promises. The log line blamed the
		// substrate for ignoring a code it had honoured, which sent an
		// operator at the wrong component.
		if key := unreadReserved(pk, subscribeReserved); key != "" {
			b.log.Warn("disconnecting: a subscription carrying a reserved property "+
				"saguin does not read was granted anyway - the substrate ignored "+
				"the reason code",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"property", b.limits.Loggable(key))
			b.disconnect(cl, packets.ErrImplementationSpecificError)
			return
		}
		if badPartition != "" {
			b.log.Warn("disconnecting: a subscription with an unreadable partition "+
				"declaration was granted anyway - the substrate ignored the reason code",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter),
				"reason", badPartition)
			b.disconnect(cl, packets.ErrImplementationSpecificError)
			return
		}
		if declared.declared() {
			if why := partitionRefusal(f.Filter); why != "" {
				b.log.Warn("disconnecting: a partition declaration saguin refused was "+
					"granted anyway - the substrate ignored the reason code",
					"client", cl.ID, "filter", b.limits.Loggable(f.Filter), "reason", why)
				b.disconnect(cl, packets.ErrImplementationSpecificError)
				return
			}
		}
		if why := b.shareRefusal(cl, f.Filter); why != "" {
			b.log.Warn("disconnecting: a shared subscription saguin refused was granted "+
				"anyway - the substrate ignored the reason code",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter), "reason", why)
			b.disconnect(cl, packets.ErrTopicFilterInvalid)
			return
		}
		if b.shareDenied(cl, f.Filter) {
			b.log.Warn("disconnecting: a shared subscription this client's roles deny was "+
				"granted anyway - the substrate ignored the reason code",
				"client", cl.ID, "filter", b.limits.Loggable(f.Filter))
			b.disconnect(cl, packets.ErrSharedSubscriptionsNotSupported)
			return
		}

		// **Recorded now and fed below**, which is two passes over one
		// packet rather than one, and the reason is a gap this used to
		// serve in silence.
		//
		// A replay matches each record against every filter the consumer
		// has *on that channel*, and steps over the records that match
		// none - necessarily, or a record for somebody else is re-read for
		// ever. Feeding inside this loop meant the first filter's replay
		// walked the whole channel while it was the only filter recorded,
		// so a record that only the second filter matched was stepped over
		// and the cursor moved past it. The consumer then received
		// everything after it and reported success over a record it never
		// saw, which is the one thing a position must never do.
		//
		// One SUBSCRIBE naming `iot/+/device/#` and `iot/+/sensor/#` is
		// exactly that, and it is the ordinary shape rather than an odd
		// one: a channel whose filter has a `{a,b}` level is read by one
		// subscription per alternative, because a `+` there would also
		// reach topics the channel does not claim.
		//
		// **Two separate SUBSCRIBEs are a different question and are left
		// alone.** There the consumer had already read to where it had
		// read when the second filter arrived, and a subscription made
		// later does not reach back before the consumer's own position -
		// which is MQTT's own rule for a subscription, and is what RFC
		// 0003 says of a position. What was wrong here was that one
		// SUBSCRIBE's outcome depended on the order its filters happened
		// to be processed in.
		// **Recorded before the feeding below**, so that a replay starting
		// in this same pass is already filtered by the slice the client
		// asked for rather than sending it everything once and slicing
		// afterwards.
		// **A re-SUBSCRIBE replaces this filter's subscription rather than
		// adding a second one** [MQTT-3.8.4-3]. `track` only ever appended,
		// and `forgetFilters` - the only thing that removes by filter - ran
		// solely from OnUnsubscribed, so every re-SUBSCRIBE left the
		// previous entry beside the new one.
		//
		// The wire showed it first: `identifiersForLocked` appends one
		// identifier per matching entry, so a delivery carried a replaced
		// subscription's identifier alongside the one in force - a client
		// told that a record answers a subscription it has already
		// replaced.
		//
		// The other half is the index itself. Nothing removed those entries
		// before the connection ended, so a client re-subscribing N times
		// held N sets of them, and every delivery path walks this slice per
		// record. That is an unbounded per-client structure grown by a
		// packet the client chooses, which is invariant 13's shape, and it
		// needs no misbehaviour to reach: MQTT offers a re-SUBSCRIBE as the
		// way to change a subscription's options.
		//
		// **Through forgetFilters, and before the declaration below.** The
		// subscription index and the partition declarations are two maps
		// that must stay in step, which is why every removal goes through
		// one place - and the order matters the other way too: clearing
		// after `declare` would throw away the declaration this SUBSCRIBE
		// just made. Cleared here, a re-SUBSCRIBE that declares nothing is
		// a subscriber asking for the whole channel again, which is what
		// replacing its options means.
		//
		// A shared filter has no entry to remove: `track` stores none, and
		// the filter is compared as it arrived, so this cannot reach the
		// ordinary subscription of the same name.
		b.mu.Lock()
		b.forgetFilters(cl.ID, []packets.Subscription{f})
		b.mu.Unlock()

		b.declare(cl.ID, f.Filter, declared)

		granted = append(granted, feeding{
			f: f,
			chans: b.track(cl.ID, f.Filter, f.Qos, wantsDeletions(pk), f.RetainAsPublished,
				f.NoLocal, f.Identifier),
		})
	}

	// **Everything above decided what this subscription gets; everything
	// below sends it, and the sending waits for the SUBACK.**
	//
	// MQTT permits either order - §3.8.4 says a server "is permitted to
	// start sending PUBLISH packets matching the Subscription before the
	// Server sends the SUBACK" - so what was here before was legal. It was
	// also alone: the substrate writes the SUBACK and *then* sends its own
	// retained messages, mosquitto does the same, and saguin sent its
	// retained values, its replays and its current state first because this
	// hook happens to run one line earlier.
	//
	// The cost of being alone is paid by the client. A library that reads
	// one packet expecting its SUBACK gets a PUBLISH instead, and what
	// follows depends on how forgiving it is; `sammiq/mqtt_test` mistook
	// nine separate rules for failures on that alone, each reading a
	// message where an acknowledgement was due and then reading the
	// acknowledgement as the next test's message. A broker cannot know
	// which libraries are forgiving, and the ordering buys saguin nothing:
	// the SUBACK is one small packet already built.
	//
	// **Only the sending moves.** The subscription is recorded in saguin's
	// index above, before the SUBACK, so a message published in the gap is
	// delivered by the ordinary path rather than missed - which is also
	// what the substrate does with its own topic index.
	send := func() {
		// **Asked of every filter, not only one reaching no channel.** The
		// retained store holds broadcast topics and nothing else - a
		// channel's record never enters it - so a filter that reaches two
		// channels and some unclaimed topics beside them is served all
		// three, and this is the third. deliverRetained answers nothing for
		// a filter with no broadcast topics under it, and refuses a shared
		// one outright, so asking it always costs a miss rather than a
		// mistake.
		for _, g := range granted {
			b.deliverRetained(cl, g.f, b.alreadyHeld(cl.ID, g.f.Filter), declared)
		}
		b.feed(cl, granted)

		// **And a shared group's backlog, after the SUBACK like every other
		// delivery here**: the member has just said it is back, and what the
		// group was holding is owed to it in the order the log holds it. A
		// member whose session outlives its connection gave its group a
		// cursor with its record (keepSubscriptions).
		b.wakeGroups(cl)

		// Last, because alreadyHeld above reads it: OnSubscribe records
		// which of these filters the client already held, and this pass is
		// the last reader of that snapshot.
		b.mu.Lock()
		delete(b.existed, cl.ID)
		b.mu.Unlock()
	}

	// **No second write here.** What the session store keeps is what was
	// granted, decided before it was asked (OnSubscribe, from the engine's
	// refusals and its own): a write taking a refused filter back out, after
	// the SUBACK had been decided, was one a refusing store left undone and a
	// crash restored.

	// An inline subscription has no SUBACK to wait for and no wire to be
	// ordered on, so it is served here.
	if cl.Net.Inline {
		send()
		return
	}
	b.mu.Lock()
	b.afterSuback[cl] = send
	b.mu.Unlock()
}

// feed serves what each granted subscription earned from the channels it
// reaches: a replay, or a latest channel's current state.
func (b *Broker) feed(cl *mqtt.Client, granted []feeding) {
	for _, g := range granted {
		f := g.f
		for _, c := range g.chans {
			switch {
			case c.Type == channel.Append:
				// **Retain Handling 2 asks for nothing older than now**, and
				// an append channel answers it by moving the consumer to the
				// head before the pump reads anything. Nothing else in MQTT
				// lets a subscriber say "live only": the alternative was to
				// seek to -1, which needs a durable session and a round trip
				// of its own, so a client that merely wanted to watch had to
				// become a consumer with a stored position first.
				//
				// 0 and 1 both replay. 1 means "only if the subscription is
				// new", which is a rule about not re-sending one topic's
				// current value; a replay has no equivalent, so reading 1 as
				// 0 is the answer that invents nothing. A 3.1.1 SUBSCRIBE
				// carries no such option at all and arrives as 0, which is
				// why `start` goes on deciding for those.
				if f.RetainHandling == 2 {
					b.skipToHead(cl, c)
				}
				b.pump(cl, c, nil)
			case c.Type == channel.Latest:
				// Retain Handling is MQTT's way for a client to say "live
				// updates only", and a `latest` channel's state is delivered
				// with the retain flag set, so the option governs it here too.
				// Ignoring it would send a client state it asked twice not to
				// be sent (RFC 0003 "`latest`").
				if f.RetainHandling == 2 || (f.RetainHandling == 1 && b.alreadyHeld(cl.ID, f.Filter)) {
					continue
				}
				// A SUBSCRIBE is answered with the whole of current state, so a
				// client that clears its own cache and rebuilds from what
				// arrives is correct - which is what most of them do and what
				// MQTT's retained delivery promises.
				//
				// It also answers whatever was dropped for this consumer, so the
				// mark standing in for those goes: everything they were holding
				// the resume back to is in the pass about to be sent. Cleared
				// before rather than after, since a live update dropped while
				// the pass is draining has to set it again.
				b.latestSeen.clearMissed(cl.ID, c.Name)

				// **Scoped to the filter just granted.** A client that
				// already holds a wide subscription on this channel and
				// then subscribes a narrow one was handed the whole
				// current-state pass again - every topic the wide filter
				// reaches, tagged with the wide subscription's identifier -
				// because the pass is matched against the client's filters
				// as a set. State is idempotent so nothing was lost, but a
				// second subscribe is not a request to re-send the first
				// one's state.
				b.deliverLatest(cl, c, 0, f.Filter)
			}
		}
	}
}

// OnPacketSent runs the deliveries a SUBSCRIBE earned, now that its SUBACK
// has been written.
//
// **It is on this hook and not on a timer or a goroutine** because this is
// the only moment that is both after the SUBACK bytes and before anything
// else is read from that client: the substrate reads, processes and writes
// for one connection in one goroutine, so OnSubscribed, the SUBACK write
// and this run back to back with nothing of the client's in between. A
// goroutine would have been a race dressed as an ordering.
//
// Every other packet returns on the first line. This is the write path for
// every PUBLISH the broker sends, so the type check is the whole cost of
// the hook for all of them.
func (b *Broker) OnPacketSent(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	// **A CONNACK written is what makes a claim on a client id stand**, and
	// this is the first moment it is known: the write has returned, and the
	// substrate has not yet taken the session over (claim has why).
	if pk.FixedHeader.Type == packets.Connack {
		if pk.ReasonCode == packets.CodeSuccess.Code {
			b.confirmClaim(cl)
		}
		return
	}
	if pk.FixedHeader.Type != packets.Suback {
		return
	}
	if h := subackBeforeServed.Load(); h != nil {
		(*h)(cl.ID)
	}
	b.mu.Lock()
	send := b.afterSuback[cl]
	delete(b.afterSuback, cl)
	b.mu.Unlock()
	if send != nil {
		send()
	}
}

// subackBeforeServed is a test seam, nil in production: when set it runs in
// OnPacketSent once a SUBACK is written and before what the SUBSCRIBE earned
// is served - the window in which the substrate already matches the new
// subscription to a publish. A test publishes from it.
var subackBeforeServed atomic.Pointer[func(clientID string)]

// wantsDeletions reports whether a SUBSCRIBE asked to be sent a latest
// channel's deletions along with its current state.
//
// **It is on the packet rather than on each filter**, because that is where
// MQTT puts User Properties: one SUBSCRIBE carries one set of them, however
// many filters it names, so asking applies to every filter in that packet.
// A client wanting one subscription with deletions and one without sends two
// SUBSCRIBEs, which is what a bridge does anyway.
//
// Presence is the switch, which is the shape saguin uses everywhere a key
// turns something on. An empty value is not presence: it is a client that
// built the property and had nothing to put in it.
func wantsDeletions(pk packets.Packet) bool { return carries(pk, deletionsProp) }

// deletionsProp is the property a `latest` subscriber asks for deletions
// with, and the bridge sets it on its own inbound SUBSCRIBE.
const deletionsProp = "saguin-deletions"

// subscribeProps is every `saguin-` User Property a SUBSCRIBE may carry,
// and connectProps is the same for a CONNECT - empty, because saguin
// defines none there yet.
//
// **These are allow-lists and the point is that they are short.** The
// `saguin-` prefix is the broker's own (reservedPrefix): a publisher's is
// stripped on the way into storage and again on the way out, so a client
// cannot forge broker metadata. The same prefix on a SUBSCRIBE is not
// metadata riding along - it is an instruction, and an instruction saguin
// does not understand must not be answered with a grant.
//
// **A name that is read and missing from this list is refused**, which
// looks like a trap and is the opposite: the feature's own test fails the
// moment it is added, because its property stops working. That is the
// enforcement, and it is why there is no source-level sweep here - a list
// that breaks the feature that outgrew it needs no separate guard.
var (
	subscribeProps = []string{filterProp, deletionsProp}

	// subscribeReserved is subscribeProps plus saguin's own carrier, for
	// the safety net in OnSubscribed rather than for the refusal in
	// OnSubscribe.
	//
	// **The net asks what the client sent, and by then saguin has added
	// one of its own.** refusedBecause puts the refusal's sentence on the
	// packet so the SUBACK can carry it, and the net read that back as a
	// client sending a property saguin does not read - so a SUBSCRIBE
	// naming one bad filter and two good ones had its bad filter refused,
	// correctly, and then the whole connection dropped. Every reserved
	// name a client actually sent is still on the packet and still caught:
	// refusedBecause removes only its own.
	subscribeReserved = []string{filterProp, deletionsProp, reasonProp}
	connectProps      []string
)

// unreadReserved is the first `saguin-` property on a packet that saguin
// does not read there, or "".
//
// **Refusing is what lets saguin grow, rather than what corners it.** A
// property silently ignored means a client that asked for a slice, or for
// deletions, is served something else and told it succeeded - and every
// release that ships that silence makes closing it later a breaking
// change. Refused, a client learns in one round trip, and RFC 0002 already
// promises a refusal is total with the connection surviving, so it can ask
// again without the property.
//
// **A hint an older broker should tolerate goes outside the prefix.** An
// unprefixed User Property stays opaque exactly as MQTT says it should,
// which is the escape hatch and costs nothing to keep open.
func unreadReserved(pk packets.Packet, allowed []string) string {
	for _, u := range pk.Properties.User {
		if !strings.HasPrefix(u.Key, reservedPrefix) {
			continue // not addressed to saguin; MQTT says leave it alone
		}
		known := false
		for _, name := range allowed {
			if u.Key == name {
				known = true
				break
			}
		}
		if !known {
			return u.Key
		}
	}
	return ""
}

// filterProp is the property a subscriber declares its slice with, and
// topicHashCall is the one function that property may hold (RFC 0003
// "Client-declared partitioning").
//
// **One property carrying a call, rather than two carrying numbers.** The
// pair this replaced - a count and an index - could arrive half-declared in
// two directions, and two of the refusals below existed only to say that
// one number had come without the other. A call carries both arguments or
// is not a call, so neither case is reachable any more.
//
// **A function rather than the expression `$topic_hash % 8 == 1`**, which
// was the first design and reads better in a packet capture - that is the
// whole of what it wins. What it costs is a grammar: `%` and `==` have one
// legal spelling each here, so every other operator and every way of
// spacing them becomes surface to refuse with a sentence of its own. A name
// and two arguments admit exactly what is implemented, and a second
// predicate would be a second name rather than a larger language.
//
// **Argument order is positional and cannot be got wrong silently**, which
// is the usual hazard of positional arguments and is closed here by a rule
// that was already needed: an index must be below the count. So if
// `topic_hash(8, 1)` is valid then `topic_hash(1, 8)` is refused, and a
// client that swaps them is told rather than served a slice it did not ask
// for. That holds for every valid pair, not just this one.
const (
	filterProp    = "saguin-filter"
	topicHashCall = "topic_hash"
)

// maxPartitionCount is the largest partition space a subscriber may
// declare, and the largest index it may name is one below it.
//
// **A type boundary rather than a judgement, and that is the whole of why
// this number exists.** Bounding it for performance would need an invented
// figure nobody can defend to an operator with more workers than somebody
// guessed - and the binary search in partition.wants removed that reason
// anyway. What cannot be removed is that `int` is 64 bits on this machine
// and 32 on the ARM gateways saguin is built to run on: parsed into an
// `int`, the same SUBSCRIBE would be accepted by one saguin and refused by
// another, and RFC 0003 could not write the rule down without saying "it
// depends which build you have".
//
// 2^31-1 is the largest value every build agrees on, so it is the
// threshold, it goes in the RFC as a number, and a client can know before
// it connects whether its declaration is legal.
const maxPartitionCount = 1<<31 - 1

// partitioning is the slice a SUBSCRIBE declared, and why it may not have
// one - "" when it may.
//
// **Declaring nothing is the ordinary case and is not an error**: a packet
// carrying no saguin-filter yields the zero partition, which wants every
// topic. That is today's behaviour and every client that never heard of
// this.
//
// **Several saguin-filter properties are an OR.** MQTT allows a User
// Property to appear more than once, which is how a member asks for more
// than one slice - a member covering a failed peer's share, most often.
// Asking twice for the same slice means what it says and is not an error.
//
// **They must agree on the partition count.** Two calls with different
// counts on one packet - `topic_hash(4, 0)` beside `topic_hash(6, 1)` - are
// expressible in this syntax and refused, because saguin holds one count
// per subscription: the sessions route answers with it, and the log line
// warning that two members disagree about the size of a space compares it.
// Nothing here needs mixed counts, so the narrower rule is the one that
// ships; widening it later breaks no client, and starting wide could not be
// taken back.
//
// **The property is on the packet rather than on a filter**, because that
// is where MQTT 5 puts User Properties: one SUBSCRIBE carries one set of
// them however many filters it names, so a declaration applies to all of
// them. A client wanting different slices for different filters sends
// different SUBSCRIBEs.
//
// **`count: 0` is the case worth naming, because a modulus by it panics the
// broker** - from one SUBSCRIBE, from any client the listener admits,
// before any authorization rule has an opinion on it.
//
// **Three lines stop it and only one of them is obvious, which fuzzing had
// to say rather than reading.** The `count < 1` check in topicHashArgs is
// the direct one. Remove it and nothing gets through anyway: an argument is
// decimal digits, so it is never negative, and an index is refused at or
// above the count - a count of 0 would need an index below 0, which cannot
// be written. So the lexis and the index bound each close it independently,
// and the direct check is a third line rather than the only one.
// partition.wants guards the zero value as a fourth.
//
// That is written down because the opposite belief is how a check gets
// "simplified": a reader who thinks one line holds this will not look for
// the other two before removing it.
//
// The rest of the set, decided rather than discovered:
//
//   - A value that is not `topic_hash(<count>, <index>)` is refused, and
//     the sentence says the form rather than guessing which part was meant.
//     Spelling is exact: `TOPIC_HASH` is not the function, for the same
//     reason `$SHARE` is not a shared subscription.
//   - Whitespace around the name, the parentheses and the comma is ignored.
//     Anything else inside the value is refused.
//   - An argument is one or more ASCII decimal digits and nothing else -
//     no sign, no `0x`, no decimal point, no exponent, and no digit from
//     outside ASCII. Leading zeros are allowed and mean what they say.
//   - An argument that is not a number and one that is too large get
//     different sentences, because they send a reader looking in different
//     places: "not written in decimal digits" about 10^23 would send
//     somebody hunting a stray character that is not there.
//   - An index at or above the count is refused: it names a slice that
//     cannot exist, so nothing would ever match it.
//   - `count: 1` is valid and degenerate - one slice, holding everything.
//     Refusing it would mean a client cannot write the general form with a
//     count of one, which is what a configuration knob set to 1 produces.
//   - A very large count is allowed. It costs the broker a modulus either
//     way, and refusing it would mean inventing a bound nobody can justify.
func partitioning(pk packets.Packet) (partition, string) {
	var (
		p       partition
		haveCnt bool
		raw     []int
	)
	for _, u := range pk.Properties.User {
		if u.Key != filterProp {
			continue
		}
		count, index, why := topicHashArgs(u.Val)
		if why != "" {
			return partition{}, filterProp + " is " + clipped(u.Val) + ": " + why
		}
		if haveCnt && count != p.count {
			return partition{}, filterProp + " declares a partition count of " +
				strconv.Itoa(p.count) + " and one of " + strconv.Itoa(count) +
				" on the same subscription, and a subscription has one partition space"
		}
		p.count, haveCnt = count, true
		raw = append(raw, index)
	}
	if !haveCnt {
		return partition{}, "" // declared nothing, and is served everything
	}

	seen := map[int]bool{}
	for _, n := range raw {
		if seen[n] {
			continue // asking twice for one slice means what it says
		}
		seen[n] = true
		p.indices = append(p.indices, n)
	}
	sort.Ints(p.indices)
	return p, ""
}

// topicHashArgs reads one saguin-filter value, or says what is wrong with
// it. The sentence it returns is the tail of a refusal: partitioning puts
// the property name and the value in front of it.
//
// **The value is not quoted into any of these sentences**, because the
// caller has already quoted it once, clipped to a length a client cannot
// choose. Naming which argument was wrong is what an operator needs; the
// bytes are in front of it either way.
func topicHashArgs(val string) (count, index int, why string) {
	form := "the only filter saguin reads is " +
		topicHashCall + "(<partitions>, <index>)"

	rest, ok := strings.CutPrefix(trimASCII(val), topicHashCall)
	if !ok {
		return 0, 0, form
	}
	rest = trimASCII(rest)
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") || len(rest) < 2 {
		return 0, 0, form
	}

	first, second, ok := strings.Cut(rest[1:len(rest)-1], ",")
	if !ok || strings.Contains(second, ",") {
		return 0, 0, topicHashCall +
			" takes two arguments, the partition count and the slice index"
	}

	n, why := partitionValue("the partition count", first)
	if why != "" {
		return 0, 0, why
	}
	if n < 1 {
		return 0, 0, "the partition count is " + strconv.FormatInt(n, 10) +
			", and a partition space holds at least one slice"
	}
	count = int(n)

	n, why = partitionValue("the slice index", second)
	if why != "" {
		return 0, 0, why
	}
	if int(n) >= count {
		return 0, 0, "the slice index is " + strconv.FormatInt(n, 10) +
			" and the partition count is " + strconv.Itoa(count) +
			", so slices are numbered 0 to " + strconv.Itoa(count-1)
	}
	return count, int(n), ""
}

// partitionValue reads one declared argument, or says why it is not a
// number saguin admits.
//
// **Parsed as a 64-bit integer on every build and then bounded by
// maxPartitionCount**, never with strconv.Atoi, whose range is the
// platform's `int`. Atoi would make the refusal threshold 2^63 here and
// 2^31 on a 32-bit gateway, so the same SUBSCRIBE would be answered
// differently by two saguins running the same version.
//
// The two failures get different sentences because they are different
// mistakes: "not a whole number" about 10^23 sends a developer looking for
// a stray character that is not there.
func partitionValue(what, raw string) (int64, string) {
	digits := trimASCII(raw)
	if !decimal(digits) {
		return 0, what + " is not written in decimal digits"
	}
	// ParseInt cannot fail on syntax here, so an error is the range one -
	// which is the same answer as a number that parsed and came out too
	// large, and must give the same sentence rather than fall back to the
	// one about spelling.
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n > maxPartitionCount {
		return 0, what + " is above the largest partition space saguin admits, " +
			"which is " + strconv.Itoa(maxPartitionCount)
	}
	return n, ""
}

// decimal reports whether a string is one or more ASCII decimal digits and
// nothing else, which is what RFC 0003 says an argument is.
//
// **Written out rather than left to strconv.ParseInt**, which accepts a
// leading `+` or `-` and would make `topic_hash(+3, 0)` legal - a spelling
// RFC 0003 does not define and a second implementation of that document
// would refuse. The bound is fixed at 2^31-1 for exactly this reason: a
// value one saguin accepts and another rejects is a rule the specification
// cannot state.
//
// A sign cannot be lexed, so no argument reaches the range checks negative,
// and `topic_hash(-1, 0)` is refused for its spelling rather than its
// value.
func decimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// trimASCII removes the whitespace RFC 0003 says is ignored - the ASCII
// space, tab, carriage return and line feed, and nothing else.
//
// **Not strings.TrimSpace**, which trims every rune Unicode calls
// whitespace: a non-breaking space, U+00A0, is one, so `topic_hash(3,\u00a00)`
// was granted here and would be refused by an implementation that read RFC
// 0003 and trimmed the four characters it names. The same argument as the
// bound: what two implementations disagree about cannot be in the
// specification.
func trimASCII(s string) string { return strings.Trim(s, " \t\r\n") }

// clipped is a client-supplied value on its way into a refusal an operator
// reads, cut to a length a number could not exceed.
//
// **The reason string reaches the log, so the value in it is a length the
// client chooses.** A property value is bounded only by Maximum Packet
// Size, so quoting it whole would let one SUBSCRIBE write a megabyte into
// somebody's log, and a client in a reconnect loop write as much of it as
// it likes - which is the failure TestNoLogLineCarriesAnUnboundedClientString
// exists to catch, and did catch here.
//
// Kept rather than dropped, because an operator reading this is debugging
// somebody else's device and the value is the whole of what they need. A
// call that does not fit in this much was never going to parse.
func clipped(raw string) string {
	const most = 48
	if len(raw) > most {
		return strconv.Quote(raw[:most]) + "..."
	}
	return strconv.Quote(raw)
}

// partitionRefusal reports why this filter may not carry a partition
// declaration, or "".
//
// **Two prefixes, checked as strings, and never a registry lookup.** Asking
// which channels a filter reaches at subscribe time is the analysis the
// `$share` change deleted, and the scope this feature was given is
// deliberately not conditional on channel type: broadcast, `append` and
// `latest` all take partitioning, and it is refused on exactly two spellings.
//
//   - `$share/…` already divides a stream between members, and dividing it
//     twice by two mechanisms has no sensible reading.
//   - `$saguin/queue/<name>` is a queue, which divides work by design. A
//     worker that also declared a slice would be offered jobs and then
//     silently drop the ones outside it - back into the queue at their
//     visibility timeout, so the work would circulate rather than be done.
func partitionRefusal(filter string) string {
	switch {
	case strings.HasPrefix(filter, "$share/"):
		return "a shared subscription already divides a stream between its members, " +
			"so it does not take " + filterProp
	case strings.HasPrefix(filter, channel.QueuePrefix):
		return "a queue already divides work between its workers, so it does not take " +
			filterProp
	}
	return ""
}

// identifiersFor is every Subscription Identifier this client's
// subscriptions into one channel attach to one topic, sorted.
//
// **All of them, not the one that caused the delivery.** MQTT 5 section
// 3.3.4 says a server sending a single copy of a message that matches
// several of a client's subscriptions MUST carry every one of their
// identifiers on it - and saguin does send a single copy, which is the
// whole reason this returns a slice. A client holding `iot/+/events/+`
// as 7 and `iot/hq/events/+` as 21 gets one record stamped with both.
//
// Getting that wrong is invisible in the obvious test: with one matching
// subscription, "the identifier that caused this" and "every identifier
// that matches" are the same answer, and they part company only where a
// client has deliberately subscribed twice.
//
// Zero is not a value the property may take - MQTT bounds it to
// 1..268,435,455 - so a subscription that carries none is skipped rather
// than contributing a zero, which would be an identifier the client never
// asked for.
//
// The channel is matched by name because the two callers have a name
// rather than a channel: a latest delivery and the retained store share a
// writer, and the store is not a channel at all.
//
// **Two entry points, because the two delivery paths differ in whether they
// already hold the lock**, and the difference is not visible from the call
// site. pumpBatch holds b.mu across its whole delivery loop - every Unlock
// in it is an early return - so calling the locking form there deadlocks
// the broker outright: the first CONNECT after it never gets a CONNACK,
// because every goroutine ends up waiting on a mutex the pump will not
// release. Measured, from a SIGQUIT dump, before this was split.
func (b *Broker) identifiersFor(clientID, chName, topic string) []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.identifiersForLocked(clientID, chName, topic)
}

// subscriptionIDs answers for the one writer that serves two different
// things: a latest channel, whose subscriptions are in saguin's index, and
// the retained store, whose are not.
//
// **The store is not a channel**, so nothing recorded it in `subs` - a
// filter reaching only broadcast topics is deliberately not indexed there -
// and its identifier can only come from the SUBSCRIBE being answered, which
// deliverRetained has and stamps on each value it queues. That is the right
// identifier rather than a convenient one: a retained value is delivered in
// answer to one subscription, at the moment it is made.
//
// **It rides on the value and not on the drain**, which is what makes it
// survive a window: a snapshot is sent a window at a time, and the caller
// that comes back for the rest is an acknowledgement rather than the
// SUBSCRIBE. See pendingValue.
//
// Measured before this existed: a live broadcast message carried its
// identifier, delivered by the substrate, and the retained value on the very
// same topic carried none, delivered by saguin. One client, one filter, two
// answers.
func (b *Broker) subscriptionIDs(clientID, chName, topic string, ident int) []int {
	if chName == RetainedStoreName {
		if ident == 0 {
			return nil
		}
		return []int{ident}
	}
	return b.identifiersFor(clientID, chName, topic)
}

// identifiersForLocked is the same, for a caller already holding b.mu.
func (b *Broker) identifiersForLocked(clientID, chName, topic string) []int {
	var ids []int
	for _, sub := range b.subs[clientID] {
		if sub.identifier == 0 || sub.channel == nil || sub.channel.Name != chName {
			continue
		}
		if !channel.Matches(sub.filter, topic) {
			continue
		}
		ids = append(ids, sub.identifier)
	}
	sort.Ints(ids)
	return ids
}

// spansChannelsLocked reports whether any subscription this client holds for
// this channel reaches another channel too, and is what decides whether a
// delivery carries `saguin-channel`.
//
// **It walks the same list identifiersForLocked walks, at the same moment,
// under the same lock**, so a delivery pays a field read rather than a
// second pass.
//
// Asked per delivery rather than per subscription because a client may hold
// both a narrow filter and a wide one over the same channel: the narrow one
// is unambiguous and the wide one is not, and the record goes out once. The
// wide one decides, because a consumer holding it is the one that cannot
// tell where the record came from.
func (b *Broker) spansChannelsLocked(clientID, chName, topic string) bool {
	for _, sub := range b.subs[clientID] {
		if !sub.spans || sub.channel == nil || sub.channel.Name != chName {
			continue
		}
		if channel.Matches(sub.filter, topic) {
			return true
		}
	}
	return false
}

// spansChannels is the same for a caller that does not already hold b.mu,
// which is the `latest` delivery path.
func (b *Broker) spansChannels(clientID, chName, topic string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spansChannelsLocked(clientID, chName, topic)
}

// alreadyHeld reports whether this client held this filter before the
// SUBSCRIBE now being answered, which is what MQTT's Retain Handling 1
// turns on.
func (b *Broker) alreadyHeld(clientID, filter string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.existed[clientID][filter]
}

// track records a granted subscription in saguin's index and returns every
// channel it reaches, or nil when it reaches only broadcast topics.
//
// **A filter now reaches several channels at once**, which is the point of
// placing a channel with a filter rather than a name: `iot/water/w-7/#` is
// one subscription served the device's readings from an append channel and
// its location from a latest one, each under the semantics its own topic
// has. One index entry per (filter, channel), because a durable position is
// per channel and the delivery goroutines are claimed per channel too
// (invariant 16).
//
// **A worker's pin is the exception, and it is one entry.** The pin names
// its queue and matches none of that queue's topics, so it cannot be
// resolved through the routing table at all; it is recognised as the form
// it is and recorded against the one channel it asks for.
//
// **A shared subscription is tracked as nothing, and that is the whole of
// what makes `$share` live-only.** Everything a tracked subscription buys -
// a stored position, a replay from it, a delivery goroutine, a `latest`
// channel's current-state pass - is what `$share` asks not to have.
// Recording none of it leaves the subscription to the substrate's own
// shared-subscription machinery, which delivers each live message to one
// member of each group and keeps no position at all: MQTT's semantics,
// served by MQTT's code.
//
// It is one return rather than a flag threaded through the delivery paths
// because the failure a flag invites is silent. A client holding an
// ordinary durable subscription *and* a shared one over the same channel is
// the case: two entries, two deliveries of one record, and a durable
// subscription fed records its own filter never matched
// (TestASharedSubscriptionDoesNotWidenADurableOne). With nothing recorded
// for the shared filter there is no second entry to confuse, and saguin's
// in-flight table keys on the packet identifiers it issued itself, so the
// substrate's acknowledgement matches nothing of saguin's.
//
// **What the two lines below are worth, measured rather than assumed.** The
// resolution further down would decline a shared filter on its own: it is
// asked of the string as sent, and `$share/g/events/#` intersects no
// channel, because ValidFilter refuses a channel filter whose first level
// begins `$`. Removing the early return alone is green. What goes red is
// removing it *together with* the stripping this replaced - which is the
// change somebody makes, since InnerFilter is still here and reinstating it
// looks like tidying. So the return states the rule and the stripping's
// absence enforces it, and neither is left as the only thing standing.
func (b *Broker) track(clientID, filter string, qos byte, deletions, retainAsPublished,
	noLocal bool, identifier int) []*channel.Channel {
	if strings.HasPrefix(filter, "$share/") {
		return nil
	}
	// **The string the client sent, never the filter inside a shared
	// subscription.** This was InnerFilter, which stripped `$share/<group>/`
	// so that a worker's pin resolved; nothing needs stripping now, and
	// putting it back is what lets a shared filter reach a channel.
	inner := filter

	reached := []*channel.Channel{}
	if q := b.reg.CanonicalQueue(filter); q != nil {
		// **A worker's pin names its queue and matches none of its topics.**
		// It cannot be resolved through the routing table the way every
		// other filter is: a queue filtered `iot/+/work/+` is named `jobs`
		// and holds nothing under `jobs/`. Recognised here as the form it
		// is, or the worker is recorded as reaching no channel at all -
		// broadcast - and the delivery path below finds no consumer for a
		// queue somebody is subscribed to.
		reached = append(reached, q)
	} else {
		// A plain filter lying inside a queue's own filter reaches here too,
		// though nothing sends one: the SUBACK refuses it, and so does a
		// restore (refusedNow). It is served what every filter meeting a
		// queue is, which is never the queue's records.
		for _, c := range b.reg.ResolveFilters(inner) {
			// **A filter that crosses a queue is not a worker, and this
			// line is the whole of what says so.** workersOf is saguin's own
			// index and asks nothing but "is this client recorded against
			// this channel" - so an entry here would make an ordinary
			// subscriber a live consumer of that queue and it would be
			// offered jobs: a worker nobody configured, taking work from the
			// real ones and acknowledging it (invariant 4).
			//
			// A worker is the branch above and nothing else: the pin names
			// its queue. Everything reaching here merely intersects one -
			// `#`, `iot/#`, a shared group's inner filter - and is served
			// everything except the queue's records.
			//
			// **It used to be the second of two lines and is now the
			// first.** The other was the SUBACK: while a queue was consumed
			// through a shared subscription, every shared filter meeting a
			// queue anywhere was refused `0x8F`, so little reached here to
			// be excluded. Nothing is refused for meeting a queue any more,
			// so `#` reaches this loop on every broker that has one.
			if c.Type == channel.Queue {
				continue
			}
			reached = append(reached, c)
		}
	}
	if len(reached) == 0 {
		return nil // broadcast only
	}

	// More than one channel behind one filter is what a consumer cannot see
	// and cannot work out. A worker's pin is the branch above and reaches
	// exactly one queue, so a queue offer is never ambiguous and carries
	// nothing.
	spans := len(reached) > 1

	b.mu.Lock()
	for _, c := range reached {
		b.subs[clientID] = append(b.subs[clientID],
			subscription{filter: inner, channel: c, qos: qos, deletions: deletions,
				retainAsPublished: retainAsPublished, noLocal: noLocal,
				identifier: identifier, spans: spans})
	}
	b.reindex(clientID)
	b.mu.Unlock()
	return reached
}

// below is one channel where a reader's stored position is unreadable.
type below struct{ position, floor uint64 }

// positionsBelowTheFloor returns the channels where this reader's stored
// position points at records retention has removed.
//
// Every append channel is asked, because at this point the client has not
// subscribed - mochi restores subscriptions after this hook, and a client
// that subscribes does so later still. A lookup per channel at connect time
// is the cost, and connect is not a hot path.
func (b *Broker) positionsBelowTheFloor(clientID string) map[string]below {
	b.mu.Lock()
	logs := make(map[string]LogStore, len(b.logs))
	for k, v := range b.logs {
		logs[k] = v
	}
	b.mu.Unlock()

	var out map[string]below
	reader := store.MQTTReader(clientID)
	for name, lg := range logs {
		p, ok, err := lg.Position(reader)
		if err != nil {
			// Reading it failed, so whether it is below the floor is unknown.
			// Discarding a session on a guess would cost a consumer its
			// position for nothing, so this leaves it alone and says so.
			b.log.Error("cannot read a stored position", "client", clientID,
				"channel", name, "error", err)
			continue
		}
		if !ok || p.Offset >= lg.Floor() {
			continue
		}
		if out == nil {
			out = map[string]below{}
		}
		out[name] = below{position: p.Offset, floor: lg.Floor()}
	}
	return out
}

// providerReaderDroppers splits the channels into the providers that can
// forget a reader in one operation and the channels that must be asked one
// at a time.
//
// **An optional capability rather than a method every store must have**,
// which is the shape SetQuota already uses here. A provider keeping its own
// records answers this with one statement over a table keyed by channel and
// reader; a memory provider has no such table and no provider-level object
// to ask, and its per-channel drop is a map delete that costs nothing worth
// removing. Requiring it everywhere would mean inventing a provider object
// for memory so that it could answer a question its channels already answer
// for free.
//
// Callers hold b.mu.
func (b *Broker) providerReaderDroppers(logs map[string]LogStore) (map[string]ReaderDropper, map[string]LogStore) {
	droppers := map[string]ReaderDropper{}
	walk := map[string]LogStore{}
	for name, lg := range logs {
		c := b.reg.Get(name)
		if c == nil {
			walk[name] = lg
			continue
		}
		if d, ok := b.providers[c.Storage]; ok {
			droppers[c.Storage] = d
			continue
		}
		walk[name] = lg
	}
	return droppers, walk
}

// OnUnsubscribed forgets what this client's index holds for the filters it
// gave up.
//
// **The filter as sent, never the one inside a shared subscription.** This
// used to strip the `$share/` prefix before comparing, because a queue was
// consumed through one and the index recorded the inner filter. A shared
// subscription is tracked as nothing now (see track), so stripping cannot
// find its own entry - it can only find somebody else's.
//
// The failure that was: a client holding `events/#` *and*
// `$share/g/events/#` unsubscribing from the shared one. The strip turns it
// into `events/#`, which matches the ordinary subscription's entry, and the
// durable consumer is removed from saguin's index while its subscription is
// still live - it keeps its stored position, receives nothing more, and
// nothing is logged. Comparing what was sent makes the shared unsubscribe
// match no entry, which is right: it had none.
func (b *Broker) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	b.mu.Lock()
	b.forgetFilters(cl.ID, pk.Filters)
	b.mu.Unlock()
	// Outside b.mu: a store write. **Not for the client's own UNSUBSCRIBE**,
	// whose record OnUnsubscribe stored before the substrate applied it
	// (keepUnsubscribe), and which carries a packet identifier; the engine's
	// own unsubscribe of a session it is discarding carries none.
	if pk.PacketID == 0 {
		b.syncSessionSubscriptions(cl)
	}
	// **A group whose last kept member unsubscribed has nobody left to take
	// its backlog**, which goes as it would at that member's ending, counted
	// as no_member_left: asked of the store, which holds the members and
	// ends the cursor in one step.
	//
	// **Only for the client's own UNSUBSCRIBE**, which carries a packet
	// identifier. The engine unsubscribes a session it is discarding with
	// none (Server.UnsubscribeClient) - a clean start's predecessor, an
	// expired session - and that is part of the session's ending, which
	// ends its groups in the same store write as its record
	// (endStoredSession) or, where a claim holds the record, in endSession:
	// a session ends in one place (RFC 0003 "Sessions"), and a group ended
	// here first would be a second write a crash could come between.
	if d := b.broadcastDrain(); d != nil && pk.PacketID != 0 {
		var groups []string
		for _, f := range pk.Filters {
			if isShareFilter(f.Filter) {
				groups = append(groups, f.Filter)
			}
		}
		d.endUnheld(groups)
	}
}

// OnUnsubscribe says so when an UNSUBSCRIBE carries a `saguin-` property
// saguin does not read, and lets the packet through.
//
// **The one packet where refusing is worse than the silence.** A refused
// UNSUBSCRIBE leaves the client subscribed and still receiving what it
// asked to stop receiving, which turns a property nobody read into records
// nobody wanted - so the answer here is the log line rather than the
// reason code. Nothing saguin defines could qualify "stop these filters"
// anyway: the packet's meaning is complete without properties. If that
// ever stops being true, this becomes a refusal like the other two.
func (b *Broker) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	pk = b.saidUnread(cl, pk)
	// **Stored before the substrate applies it, and refused where it cannot
	// be** (keepUnsubscribe): the UNSUBACK says the subscription is gone.
	b.keepUnsubscribe(cl, &pk)
	return pk
}

// saidUnread is OnUnsubscribe's answer to a `saguin-` property it does not
// read: logged, stripped, and named on the UNSUBACK.
func (b *Broker) saidUnread(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	key := unreadReserved(pk, nil)
	if key == "" {
		return pk
	}
	b.log.Warn("an unsubscribe carries a reserved property saguin does not "+
		"read, and it is being unsubscribed anyway: refusing would leave the "+
		"client subscribed to what it asked to stop receiving",
		"client", cl.ID, "property", b.limits.Loggable(key))

	// **The client is told in the same round trip, rather than only the
	// operator.** The substrate copies these onto the UNSUBACK, which is
	// the one packet saguin already answers here - so the silence this
	// exception used to cost is a log line an operator may never read
	// *and* a property the client can act on, instead of only the first.
	//
	// A Reason String would be the more idiomatic carrier and is not
	// reachable: the substrate sets one only for a failing code, and this
	// packet succeeds by design.
	//
	// **The echo is stripped first.** Those properties are copied back
	// verbatim, so a client's `saguin-` name would return on a packet
	// saguin wrote, over the prefix saguin just finished claiming as its
	// own - and the name would be the client's rather than the broker's,
	// which is the whole thing this round is about.
	pk = stripReserved(pk)
	pk.Properties.User = append(pk.Properties.User,
		packets.UserProperty{Key: unreadProp, Val: key})
	return pk
}

// unreadProp names, on an UNSUBACK, the reserved property saguin did not
// read on the UNSUBSCRIBE it is answering.
const unreadProp = "saguin-unread"

// reasonProp carries a refusal's sentence from OnSubscribe to
// OnPacketEncode, where it becomes the SUBACK's Reason String.
//
// **The packet is the channel, because a map would need a key that does
// not exist.** A SUBACK carries no filters, so a sentence stashed in
// OnSubscribe has to be found again by client and packet identifier - and
// packet identifiers restart at 1 on every connection, so that key needs a
// connection generation too, plus eviction for a SUBSCRIBE whose client
// went away before the answer was written. The substrate already copies a
// SUBSCRIBE's User Properties onto its SUBACK - which is the behaviour an
// earlier prefix-stripping encode hook had to undo - so the sentence can ride the same packet as the
// reason codes it explains and no two things can drift apart.
//
// **It never reaches a client.** It carries the reserved prefix, so
// ackProperties removes it from every acknowledgement, and the lift in
// OnPacketEncode happens immediately before that strip.
const reasonProp = "saguin-reason"

// refusedBecause records why a SUBSCRIBE was refused, for the SUBACK.
//
// **Replacing rather than appending, which is the whole of its safety.** A
// client may not send this property - unreadReserved refuses a SUBSCRIBE
// carrying any reserved name saguin does not read - but that refusal is
// itself a refusal, and appending there would echo the client's own text
// back as the broker's explanation of why it was refused. So the client's
// value is dropped and saguin's is the only one that can survive.
//
// **One sentence per packet, and it is the first refusal's.** MQTT allows
// one Reason String on a SUBACK however many filters it answers, so a
// SUBSCRIBE refused for two different reasons explains the first; the
// per-filter reason codes still distinguish them, and the log has both.
// **A new slice rather than a filter in place.** The packet this hook was
// handed shares its backing array with the substrate's own, so rewriting
// it in place edits a packet saguin does not own - which took the
// connection down on the first SUBSCRIBE that named three filters.
func refusedBecause(pk *packets.Packet, why string) {
	kept := make([]packets.UserProperty, 0, len(pk.Properties.User)+1)
	for _, u := range pk.Properties.User {
		if u.Key != reasonProp {
			kept = append(kept, u)
		}
	}
	pk.Properties.User = append(kept,
		packets.UserProperty{Key: reasonProp, Val: why})
}

// declare records what a client asked for on one filter, and says so when
// another client disagrees about the size of the space.
//
// **Nothing is stored for a client that declared nothing**, which is almost
// every client: the index holds declarations rather than subscriptions, so
// the walk in disagreeingCount is over the handful of partitioned
// subscribers rather than over the fleet.
//
// **And a filter re-subscribed without properties clears what it had.** A
// client may drop its declaration by subscribing again, and leaving the old
// one would serve it a slice it no longer asks for - the same silent
// inheritance forgetFilters exists to prevent, by the other route.
func (b *Broker) declare(clientID, filter string, p partition) {
	b.mu.Lock()
	if !p.declared() {
		if byFilter, ok := b.partitions[clientID]; ok {
			delete(byFilter, filter)
			if len(byFilter) == 0 {
				delete(b.partitions, clientID)
			}
		}
		b.replanLocked(clientID)
		b.mu.Unlock()
		return
	}
	if b.partitions[clientID] == nil {
		b.partitions[clientID] = map[string]partition{}
	}
	b.partitions[clientID][filter] = p
	b.replanLocked(clientID)
	otherClientID, otherCount := b.disagreeingCount(clientID, filter, p.count)
	b.mu.Unlock()

	if otherClientID == "" {
		return
	}
	b.log.Warn("two subscribers disagree about the size of a partition space",
		"filter", b.limits.Loggable(filter),
		"client", clientID, "declared", p.count,
		"other_client", otherClientID, "other_declared", otherCount,
		"consequence", "records in the slices neither of them claims reach nobody")
}

// slice removes the broadcast subscribers whose declared partition does not
// take this topic.
//
// **Broadcast is the one channel type saguin does not deliver itself**, so
// the predicate cannot be applied where the other three apply it. It is
// applied to the substrate's matched set instead, which is what
// OnSelectSubscribers is for - and mochi #523, saguin's own patch, is what
// makes that possible at all: before it the hook fired only where a shared
// subscriber existed, so an ordinary broadcast never reached a hook.
//
// **Nothing here asks which channel the topic belongs to.** The scope this
// feature was given is deliberately not conditional on channel type, and
// establishing that a filter reaches no channel would be the analysis the
// `$share` change deleted, at delivery time rather than at subscribe time.
//
// The hash is computed once for the topic and then asked of each
// subscriber, which is the shape this whole design is arranged around.
func (b *Broker) slice(subs *mqtt.Subscribers, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// **Nothing declared anywhere is the ordinary case**, and it costs one
	// map length rather than a hash and a walk of the subscriber set.
	if len(b.partitions) == 0 {
		return
	}
	h := channel.PartitionHash(topic)
	for id := range subs.Subscriptions {
		// **A client that declared nothing is served everything**, and that
		// is most of them - answered without touching its subscriptions.
		if len(b.partitions[id]) == 0 {
			continue
		}
		if !b.clientWantsLocked(id, topic, h) {
			delete(subs.Subscriptions, id)
		}
	}
}

// clientWantsLocked reports whether any subscription this client holds over
// a broadcast topic wants it.
//
// **Every one of them, because the set this runs against holds one merged
// entry per client.** The substrate collapses a client's matching filters
// into a single Subscription before the hook sees them, so the filter on
// that entry is one of the client's rather than all of them - and judging
// the client by it deleted the client outright, vetoing records an
// overlapping subscription was owed. A client holding an undeclared
// `loose/ovl/+` beside a declared exact filter was served only the declared
// slice, with both SUBACKs granted and every publish acknowledged: silent
// under-delivery.
//
// The three sites that walk saguin's own per-filter index already keep
// looking after a filter that matches but does not want, which is what
// wantsLocked's contract asks of every caller. This one runs after the
// merge, so it goes back to the client's own subscriptions to do the same.
//
// **Read from the session rather than from saguin's index**, because a
// broadcast subscription has no entry there: track records only filters
// that reach a channel.
//
// The caller holds b.mu.
func (b *Broker) clientWantsLocked(clientID, topic string, h uint64) bool {
	cl, ok := b.srv.Clients.Get(clientID)
	if !ok {
		// Gone between the match and here. Nothing is delivered to it
		// either way, and answering true leaves the substrate to find that
		// out on the write rather than inventing an answer here.
		return true
	}
	// Walked in place, once per publish per subscriber, under b.mu: the
	// callback reads b.partitions, which b.mu guards, and takes no lock.
	held, wanted := false, false
	cl.State.Subscriptions.Each(func(filter string, _ packets.Subscription) {
		if wanted {
			return
		}
		// **A shared subscription has no say here, in either direction.**
		// Share deliveries do not travel through the set this walk judges -
		// the substrate merges the selected member into it *after* this hook
		// returns - so a `$share` filter can neither veto the ordinary
		// fan-out nor entitle it.
		//
		// It could do the second, and did. Stripped by InnerFilter a share
		// filter matches the topic, and it can carry no declaration because
		// one is refused on it at SUBSCRIBE - so it answered "this client
		// wants everything" and voided the declaration on every ordinary
		// subscription the client held beside it. A partition group whose
		// members also held a share filter was served every slice: the
		// duplicate class, one arrangement over from the veto this walk was
		// written to remove.
		if strings.HasPrefix(filter, "$share/") {
			return
		}
		inner := channel.InnerFilter(filter)
		if !channel.Matches(inner, topic) {
			return
		}
		held = true
		if b.wantsLocked(clientID, filter, h) {
			wanted = true
		}
	})
	if wanted {
		return true
	}
	// **No matching subscription at all is not a refusal.** The substrate
	// matched this client for a reason - a shared group, or a filter form
	// this walk does not read the same way - and deleting it here on an
	// answer this function could not work out would be the veto it exists
	// to remove.
	return !held
}

// wantsLocked reports whether the slice this client declared on this filter
// takes a topic that has already been matched against it.
//
// **The hash is the caller's, computed once**, because the shape this runs
// in is one publish offered to many subscribers: hashing inside the
// per-subscriber loop pays for the same topic once per member.
//
// A filter with no declaration wants everything, which is almost every
// filter and is the answer the zero partition gives.
//
// **Every caller asks it beside a Matches**, and the pairing is deliberate:
// a filter that matches but does not want must not stop the search, because
// the same client may hold a second filter on that channel that does want
// it. All four call sites keep looking rather than breaking on a match.
//
// The caller holds b.mu.
func (b *Broker) wantsLocked(clientID, filter string, h uint64) bool {
	p, ok := b.partitions[clientID][filter]
	if !ok {
		return true
	}
	return p.wants(h)
}

// disagreeingCount is another client that declared a different partition
// count for this same filter, and the count it declared - or "" when none
// does.
//
// **Scoped by the filter string, and by nothing else.** It needs no channel
// lookup, so broadcast needs no special case and the rule reads the same on
// every channel type. It catches the mistake that actually happens - N
// workers pointed at one filter with one of them misconfigured - and two
// subscribers slicing unrelated topic trees never compare, because their
// filters differ.
//
// **It misses overlapping-but-different filters** - `iot/+/events` against
// `iot/site-a/events` - and that miss is deliberate. Where the filters
// genuinely differ the counts may well differ on purpose, and warning there
// would be noise on a broker doing nothing wrong.
//
// This is a warning and never a refusal: the broker cannot know a group's
// intent, and a rolling restart legitimately has members arriving one at a
// time. What it can say is that two clients disagree about the size of the
// space, which is not a timing artefact and cannot be correct.
//
// The caller holds b.mu. The walk is over clients that declared
// partitioning rather than over every client, because a declaration is the
// only thing stored here.
func (b *Broker) disagreeingCount(clientID, filter string, count int) (string, int) {
	for id, byFilter := range b.partitions {
		if id == clientID {
			continue
		}
		if p, ok := byFilter[filter]; ok && p.count != count {
			return id, p.count
		}
	}
	return "", 0
}

// forgetSubscriptions drops a departing connection's subscription index
// while leaving what its session declared.
//
// **The asymmetry is MQTT's rather than saguin's.** A resumed session's
// subscriptions come back from the substrate, so b.subs is rebuilt from
// them in OnSessionEstablished; the partition properties came on a
// SUBSCRIBE packet that is not sent again, so nothing would rebuild
// b.partitions. Keeping it is what makes a declaration last as long as the
// subscription it was made on - and it is dropped with the session by
// forgetClient in endSession, which each of the four paths where the session
// itself ends calls: a disconnect that discards it, a clean start, a position
// retention has passed, and the expiry sweep through OnClientExpired. The
// last one is the one that used not to, and
// TestEveryPathThatEndsASessionDropsTheSameState is what holds all four now.
//
// The caller holds b.mu.
func (b *Broker) forgetSubscriptions(id string) {
	delete(b.subs, id)
	b.reindex(id)
}

// subKey is one of a client's subscriptions as the two turned-round
// indexes see it.
type subKey struct{ channel, filter string }

// reindex brings byTopic and members into line with subs for one client,
// and is the only thing that writes either.
//
// **It recomputes from subs rather than being told what changed**, so it
// has nothing to get wrong about the change: whatever subs now says for
// this client is what the indexes say, and a filter held twice, or on two
// channels, or given up while another reaching the same channel stays, all
// come out right without a case of their own. It costs the client's own
// subscriptions, never anyone else's.
//
// A filter that goes is unsubscribed from the trie rather than left, which
// is what lets the trie trim its empty nodes (invariant 13).
//
// The caller holds b.mu.
func (b *Broker) reindex(id string) {
	now := map[subKey]struct{}{}
	for _, s := range b.subs[id] {
		now[subKey{s.channel.Name, s.filter}] = struct{}{}
	}
	was := b.indexed[id]

	for k := range was {
		if _, kept := now[k]; !kept {
			b.byTopic[k.channel].Unsubscribe(k.filter, id)
		}
	}
	for k := range now {
		if _, had := was[k]; had {
			continue
		}
		x := b.byTopic[k.channel]
		if x == nil {
			x = mqtt.NewTopicsIndex()
			b.byTopic[k.channel] = x
		}
		x.Subscribe(id, packets.Subscription{Filter: k.filter})
	}

	for k := range was {
		delete(b.members[k.channel], id)
		if len(b.members[k.channel]) == 0 {
			delete(b.members, k.channel)
		}
	}
	for k := range now {
		if b.members[k.channel] == nil {
			b.members[k.channel] = map[string]struct{}{}
		}
		b.members[k.channel][id] = struct{}{}
	}

	if len(now) == 0 {
		delete(b.indexed, id)
	} else {
		b.indexed[id] = now
	}

	// What this client's pending lists say was matched against the filters
	// it held; a filter added now could match records they call declined.
	if con := b.lookupConsumer(id); con != nil {
		con.cmu.Lock()
		for chName, cur := range con.cursors {
			b.resetPendingLocked(cur, chName)
		}
		con.cmu.Unlock()
	}
	b.replanLocked(id)
}

// matchingLocked is the clients holding a filter on this channel that
// matches the topic, in no particular order.
//
// **Identity only.** What each subscription asked for - its QoS, No Local,
// Retain As Published, identifier, partition - is read from subs by the
// caller, never from what the trie keeps, so there is one source of it. The
// trie can only say who to look at.
//
// The caller holds b.mu.
func (b *Broker) matchingLocked(c *channel.Channel, topic string) []string {
	x := b.byTopic[c.Name]
	if x == nil {
		return nil
	}
	// **Visited rather than collected.** Subscribers builds four maps and
	// merges a subscription per matching client, all of which this would
	// throw away: announcing one record to 5,000 of them held b.mu for
	// 17.5ms on average, about 3.5us a recipient, and the copying was most
	// of it. A client holding two matching filters is visited twice, so the
	// ids are deduplicated here.
	var ids []string
	var seen map[string]struct{}
	x.EachSubscriber(topic, func(id string) {
		if seen == nil {
			seen = map[string]struct{}{}
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	})
	return ids
}

// forgetFilters drops what a client held for the filters it just gave up,
// and is the only place individual filters leave those indexes.
//
// **This is the half a whole-client helper cannot cover, and it is the
// likelier bug of the two.** A partition declaration left behind by an
// UNSUBSCRIBE is inherited by the next SUBSCRIBE on the same filter: the
// client sent no saguin-filter, saguin still holds the one it was sent
// last time, and the subscriber is served a slice of the channel while
// believing it asked for all of it. Nothing reports that - it looks exactly
// like a channel with less traffic than expected.
//
// The caller holds b.mu.
func (b *Broker) forgetFilters(id string, filters []packets.Subscription) {
	kept := b.subs[id][:0]
	for _, s := range b.subs[id] {
		drop := false
		for _, f := range filters {
			if f.Filter == s.filter {
				drop = true
			}
		}
		if !drop {
			kept = append(kept, s)
		}
	}
	b.subs[id] = kept
	b.reindex(id)

	for _, f := range filters {
		delete(b.partitions[id], f.Filter)
	}
	if len(b.partitions[id]) == 0 {
		delete(b.partitions, id)
	}
}

// forgetExchange takes the substrate's side of an exchange down with
// saguin's, so a PUBREL arriving afterwards is answered `0x92 Packet
// Identifier not found` rather than PUBCOMP success.
//
// **Without it, dropping a held message is a false acknowledgement.**
// processPubrel answers from its own inflight table, which still holds the
// PUBREC this exchange started with - so a client whose message saguin had
// just swept was told the exchange completed for a record nobody has.
// Measured: `the release of an expired publish was answered 0x00`. That is
// the failure this project puts ahead of every other, and the fix is to
// stop claiming an exchange we are no longer holding.
//
// Best effort by nature. A client that has gone has no table to clear, and
// there is then nothing left to answer wrongly.
func (b *Broker) forgetExchange(clientID string, packetID uint16) {
	if b.srv == nil {
		return
	}
	cl, ok := b.srv.Clients.Get(clientID)
	if !ok {
		return
	}
	// While cl has its session (WhileOwned), as every change saguin makes to
	// a connection's table. Both callers hold the client id's session lock,
	// which a takeover holds too, so the table is never mid-copy here.
	cl.WhileOwned(func() {
		if cl.State.Inflight.Delete(packetID) {
			// The slot this exchange was holding goes back, or a publisher
			// that had messages swept would lose one of its send quota per
			// sweep and silently stop being able to publish.
			cl.State.Inflight.IncreaseReceiveQuota()
		}
	})
}

// dropHeldPublishes forgets the exactly-once publishes a session was
// holding, on the two paths that end one: an expiry passing, and a
// disconnect from a client that asked for no session at all.
//
// **They belong to the session and to nothing else.** The PUBREL that would
// complete one can only come from the session that sent the PUBLISH, so
// once it has gone the message is one nobody can ever finish - and holding
// it spends a provider's bytes until `expires_after` catches up with it.
//
// Outside b.mu, for the reason dropStoredPositions is: on a sqlite provider
// this is a transaction, and the broker-wide lock is never held across disk
// I/O (RFC 0005).
func (b *Broker) dropHeldPublishes(cl *mqtt.Client, why string) {
	// **Asked here for the reason dropStoredPositions asks**, and the
	// asymmetry between the two was the defect: on both teardown paths this
	// is called one line after that sweep, which stops when the id is taken
	// over and then leaves this one to run on the successor anyway. The
	// window is that sweep - one store transaction per channel, 42ms median
	// at three hundred channels and over a second at ten thousand - so a
	// CONNECT landing in it claims the id, is answered Session Present = 0,
	// opens an exactly-once exchange, and has the held message deleted
	// between its PUBREC and its PUBREL. The publisher was told the broker
	// had it, and nothing is left for the PUBREL to complete.
	//
	// Absent means nobody holds the id, which is the ordinary case here: the
	// owner entry goes with the connection, and the two paths that discard a
	// session on its own connect claim it before they reach this.
	b.mu.Lock()
	if owner, held := b.owner[cl.ID]; held && owner != cl {
		b.mu.Unlock()
		b.log.Info("stopped dropping unfinished exactly-once publishes: the client "+
			"id was taken over while its session was ending",
			"client", b.limits.Loggable(cl.ID), "reason", why)
		return
	}
	b.mu.Unlock()
	n, refused := b.dropHeldOf(cl.ID)
	if refused {
		b.oweEnding(cl.ID, cl, why, false, nil, true)
	}
	if n > 0 {
		b.counted.qos2Abandoned.Add(uint64(n))
		b.log.Info("dropped unfinished exactly-once publishes with a session",
			"client", b.limits.Loggable(cl.ID), "publishes", n, "reason", why)
	}
}

// dropHeldOf drops every exactly-once publish a client holds, from each
// channel store it is in, and answers how many went and whether the store
// refused any - which stays held, and is owed (oweEnding). The caller holds
// the client id's session lock.
func (b *Broker) dropHeldOf(client string) (n int, refused bool) {
	b.heldMu.Lock()
	var mine []store.Exchange
	where := map[store.Exchange]heldExchange{}
	for e, h := range b.held {
		if e.Client == client {
			mine = append(mine, e)
			where[e] = h
		}
	}
	b.heldMu.Unlock()
	for _, e := range mine {
		if h := b.holdsFor(where[e].channel); h != nil {
			if _, err := h.DropHold(e); err != nil {
				b.log.Error("cannot drop an unfinished exactly-once publish; it is dropped again "+
					"before the client id is used again",
					"client", b.limits.Loggable(client), "channel", where[e].channel, "error", err)
				refused = true
				continue
			}
		}
		b.forgetHeld(e)
		b.retainedStands(e, where[e], "its session ended")
		n++
	}
	return n, refused
}

// retainedStands says so when an exactly-once broadcast that asked to be
// retained is abandoned. Its release writes the retained value before the
// swap (releaseHeld), so a release refused after that write, and never sent
// again, leaves the value retained with no delivery behind it. There is no
// undo: taking the value back would race a newer one. It is counted where
// every abandoned exchange is, saguin_qos2_abandoned_total, and this is the
// line that says which ones may have left a value.
func (b *Broker) retainedStands(e store.Exchange, h heldExchange, why string) {
	if !h.retain {
		return
	}
	b.log.Warn("an exactly-once broadcast that asked to be retained was abandoned: "+
		"if a refused release had already written it, its retained value stays",
		"client", b.limits.Loggable(e.Client), "packet_id", e.PacketID, "why", why)
}

// dropStoredPositions removes a client id's stored positions, from every
// channel rather than only the ones it was subscribed to when it left: it
// may have held a position in a channel it unsubscribed from, and that one
// has to go too. Callers hold b.mu.
//
// **A position outlives its session for nobody**, and that is a correctness
// rule before it is a bound. A client id whose session is gone is told
// Session Present = 0 on its next connect - the standard "I have no state
// for you" - and a replacement device taking that id over would then be
// resumed at the departed consumer's offset, told it had missed nothing and
// served none of the records below it. That is invariant 1's failure with
// the signal and the behaviour disagreeing.
//
// It is also what keeps the stored positions bounded (invariant 13), which
// is what `limits.max_session_expiry` caps the interval for. The cap alone
// is not enough: it only decides *when* a row is due, and something has to
// come and take it.
// **Callers must not hold b.mu.** This takes b.positions and then b.mu in
// that order, and it does its store writes and its logging with neither the
// broker-wide lock nor the health endpoint's probe behind it (RFC 0005: no
// path holds b.mu across disk I/O - a hold long enough to fail that probe
// is a defect, and this loop is one transaction per channel).
//
// only, when it is not nil, names the channels whose positions go, and every
// other position stays: the retention-floor discard, which drops what
// retention passed and keeps what it did not (endedSession.retentionPassed).
// It is walked channel by channel, since a provider-wide drop cannot spare
// any.
//
// **Reports false where the store refused a drop**, so the caller owes it
// (oweEnding) rather than leaving the position for the next session under
// the id; a sweep stopped by a takeover reports true, the positions being
// the new owner's.
func (b *Broker) dropStoredPositions(cl *mqtt.Client, why string, only map[string]bool) (dropped bool) {
	clientID := cl.ID
	refused := false
	b.positions.Lock()
	defer b.positions.Unlock()

	// The cursors go first, and under the lock, so that a batch
	// flushPositions has already collected finds them missing and declines
	// to write the row back. Ordering alone is not enough: whichever of the
	// two takes b.positions second still has to know the other happened, and
	// this is what tells it.
	b.mu.Lock()
	// **Asked here and not only by the caller.** A caller that checked and
	// then called has a gap: this takes b.positions first, which a flush
	// batch can hold, and then walks a channel per store - measured at 42ms
	// median and 54ms worst at three hundred channels, and arithmetically
	// over a second at the ten thousand channels saguin plans for. The
	// substrate has
	// not removed the old client from its table until this returns, so a
	// CONNECT with that id inside that window inherits the session, is
	// answered Session Present = 1, and would then have every position for
	// that reader deleted underneath it: it subscribes, finds nothing, and
	// replays from the floor - a duplicate delivery reported as a resumed
	// session. This is the rule that fix wrote for itself, applied to the
	// function that rule was added to.
	//
	// Absent means nobody holds the id, which is the ordinary case: the
	// owner entry goes when the connection does, long before its session
	// comes due.
	if owner, held := b.owner[clientID]; held && owner != cl {
		b.mu.Unlock()
		return true
	}
	b.dropCursorsLocked(clientID)
	logs := make(map[string]LogStore, len(b.logs))
	for name, lg := range b.logs {
		if only == nil || only[name] {
			logs[name] = lg
		}
	}
	// The same split the connect path takes, and the same reason: a walk
	// here is a transaction per channel, which the comment above costs at
	// over a second at ten thousand. What it buys beyond speed is that a
	// provider answering in one statement cannot be interrupted partway -
	// the hazard the loop below re-checks for, removed rather than watched.
	droppers, walk := map[string]ReaderDropper(nil), logs
	if only == nil {
		droppers, walk = b.providerReaderDroppers(logs)
	}
	b.mu.Unlock()

	reader := store.MQTTReader(clientID)
	for provider, d := range droppers {
		// Between providers rather than between channels, because that is
		// where the gap now is: one provider's rows go in one statement, and
		// a takeover landing between two providers is the case the
		// per-channel check below was written for.
		b.mu.Lock()
		owner, held := b.owner[clientID]
		b.mu.Unlock()
		if held && owner != cl {
			b.log.Info("stopped dropping positions: the client id was taken over "+
				"while its expired session was being swept",
				"client", clientID, "provider", provider)
			return true
		}
		n, err := d.DropReader(reader)
		if err != nil {
			b.log.Error("cannot drop the positions of a session that ended",
				"client", clientID, "provider", provider, "reason", why, "error", err)
			refused = true
			continue
		}
		if n > 0 {
			b.log.Info("a session's stored positions went with it",
				"client", clientID, "provider", provider, "discarded", n, "reason", why)
		}
	}
	for name, lg := range walk {
		// **Asked again, once per channel, and the check above is not
		// enough on its own.** This walk is one store transaction per
		// channel - 42 to 54ms measured at three hundred, and over a second
		// at the ten thousand channels saguin plans for - and the substrate
		// does not
		// remove the expired client from its table until this returns. A
		// CONNECT claiming the id partway through inherits the session, is
		// answered Session Present = 1, and would then have the rows not yet
		// reached deleted underneath it: some channels resume and the rest
		// replay from the floor, which is a client told it missed nothing
		// and handed half its state.
		//
		// It costs a map read under b.mu per channel, against a transaction
		// that costs three orders of magnitude more in the same iteration.
		// This is not the hold RFC 0005 forbids - that is the lock kept
		// across the I/O, which is what this loop was moved out of b.mu to
		// avoid.
		b.mu.Lock()
		owner, held := b.owner[clientID]
		b.mu.Unlock()
		if held && owner != cl {
			b.log.Info("stopped dropping positions: the client id was taken over "+
				"while its expired session was being swept",
				"client", clientID, "channel", name)
			return true
		}

		had, err := lg.DropPosition(reader)
		if err != nil {
			b.log.Error("cannot drop the position of a session that ended",
				"client", clientID, "channel", name, "reason", why, "error", err)
			refused = true
			continue
		}
		if had {
			b.log.Info("dropped a position with its session",
				"client", clientID, "channel", name, "reason", why)
		}
	}
	return !refused
}

// publishAllowance is one client's publish budget: a token bucket holding
// one second's worth, refilled continuously.
//
// **One of these per client and never shared**, which is the whole design.
// checkBounds runs on the publish path without the broker-wide lock held,
// so a shared map consulted there would put every publisher behind one
// mutex - and the benchmark record already says everything queues on that
// mutex past eight publishers. A rate limiter that pays for itself on every
// publish, to protect against something rare, is what this must not become.
type publishAllowance struct {
	mu sync.Mutex
	// Two buckets, because the two units bound different things and either
	// alone leaves the other hole open: a thousand one-byte publishes cost
	// a kilobyte and a thousand store writes, ten one-megabyte publishes
	// cost ten operations and ten megabytes.
	msgs, bytes  bucket
	rate         int
	byteRate     int64
	resolvedFor  string
	haveResolved bool
	// Which acl_file the two numbers above were resolved from, so that a
	// file re-read under a connected client is not a change that applies to
	// everybody else and not to them.
	resolvedGen uint64

	// leftAt is when the connection holding this budget went, or the zero
	// time while one holds it.
	//
	// **A budget that died with the connection was a budget a client could
	// refill by reconnecting.** Dropped at OnDisconnect, a client held to
	// five publishes a second sent five, was refused, reconnected under the
	// same id and sent five more - ten inside the same second, against a
	// limit that says what one client may send in one. It does not take a
	// hostile client: a library that treats the 0x97 as fatal and reconnects
	// does it by itself, and turns a rate limit into a reconnect storm,
	// which costs the broker more than the publishes did.
	leftAt time.Time
	// dead marks an entry the sweep has taken out of the map, so a publish
	// that loaded it a moment earlier fetches a fresh one rather than
	// spending against a budget nothing will ever collect.
	dead bool
}

// bucket is one token bucket, holding a second's worth.
type bucket struct {
	tokens float64
	last   time.Time
}

// full reports whether this bucket has refilled to its ceiling, which is
// when a kept budget and a new one are the same thing and the kept one can
// go.
//
// **It is asked rather than timed.** A second is enough to refill a bucket
// spent to nothing and is not enough for one spent into deficit - which
// take does deliberately, for a message larger than the whole bucket - so a
// sweep on a one-second clock would hand exactly those clients a fresh
// budget and forgive the deficit.
func (b *bucket) full(rate float64, now time.Time) bool {
	if rate <= 0 {
		return true // this unit is unbounded, so there is nothing to refill
	}
	if b.last.IsZero() {
		return true // never spent
	}
	return b.tokens+now.Sub(b.last).Seconds()*rate >= rate
}

// take spends one unit of cost, or reports that it does not fit.
//
// A bucket rather than a count per second, because a window lets a client
// send its whole allowance in a millisecond and then wait - which is the
// burst the broker is being protected from, arriving on schedule. A quiet
// client may burst one second's worth and no more.
func (b *bucket) take(rate, cost float64, now time.Time) bool {
	if rate <= 0 {
		return true // this unit is unbounded
	}
	if b.last.IsZero() {
		b.last, b.tokens = now, rate
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.last = now
		if b.tokens += elapsed * rate; b.tokens > rate {
			b.tokens = rate
		}
	}
	// **A cost larger than the whole bucket is admitted once**, rather than
	// refused for ever. A 1MiB publish against a 64KiB/s ceiling can never
	// fit a full bucket, and refusing it every time would make the limit a
	// silent ban on large messages rather than a rate. It spends the bucket
	// into deficit instead, and the client waits that much longer.
	if b.tokens < 1 && cost >= rate {
		return false
	}
	if b.tokens < cost && cost < rate {
		return false
	}
	b.tokens -= cost
	return true
}

// allow reports whether one publish of this many bytes fits, and spends it
// if it does. Whichever unit is reached first refuses.
func (a *publishAllowance) allow(size int64, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Both are asked before either is spent, so a publish refused on bytes
	// does not silently cost a message token as well.
	if !a.msgs.would(float64(a.rate), 1, now) {
		return false
	}
	if !a.bytes.would(float64(a.byteRate), float64(size), now) {
		return false
	}
	a.msgs.take(float64(a.rate), 1, now)
	a.bytes.take(float64(a.byteRate), float64(size), now)
	return true
}

// would is take without spending.
func (b *bucket) would(rate, cost float64, now time.Time) bool {
	probe := *b
	return probe.take(rate, cost, now)
}

// withinPublishRate reports whether this client may send another publish,
// and returns the limits it was held to so that a refusal can name them.
//
// **Which limits apply is decided by the identity the acl_file's patterns
// are about - the user name - and the budget itself is one per client
// id.** The two are deliberately different keys and it took a wrong version
// to see why.
//
// Matching on the client id would have made one `clients:` entry mean two
// things: its roles about the user name and its limits about the id, so a
// device whose two differ would take its permissions from one pattern and
// its rate from another. Budgeting by the user name would be worse in the
// other direction: a fleet sharing one certificate's Common Name shares one
// name, and would then share one allowance - the first device to wake would
// spend the fleet's.
//
// So the pattern answers "which numbers", and the connection answers "whose
// budget".
func (b *Broker) withinPublishRate(identity, clientID string, size int64) (bool, int, int64) {
	// **The unset case is an atomic load and a branch**, taken before
	// anything else, because that is the path every deployment that has not
	// asked for this takes on every publish. An acl_file may still name a
	// client, so the acl is asked only when there is one.
	az := b.authorizer()
	if b.limits.PublishRate <= 0 && b.limits.PublishBytes <= 0 &&
		(az == nil || !az.HasPublishLimits()) {
		return true, 0, 0
	}
	// **Which acl_file the numbers below came from.** SIGUSR1 replaces it
	// on a running broker, and the resolution under it is cached per client
	// id - so without this a re-read that narrowed somebody's publish_rate
	// would apply to every client but the one it was written about.
	gen := b.authzGen.Load()

	for {
		v, ok := b.allowances.Load(clientID)
		if !ok {
			v, _ = b.allowances.LoadOrStore(clientID, &publishAllowance{})
		}
		a := v.(*publishAllowance)

		a.mu.Lock()
		if a.dead {
			// Swept between the load and the lock. Finish taking it out of
			// the map rather than spending against it - CompareAndDelete so
			// that a fresh entry another publish has already installed is
			// left alone - and go round for the one that replaces it.
			a.mu.Unlock()
			b.allowances.CompareAndDelete(clientID, v)
			continue
		}
		// A connection is using it again, so it is no longer a candidate for
		// the sweep. This is what makes a reconnect resume the budget rather
		// than replace it.
		a.leftAt = time.Time{}

		// **Resolved once per identity and per acl_file, not per publish.**
		// Walking the acl_file's patterns on every publish would put a
		// string match per pattern on the hot path, for an answer that
		// changes twice: when the identity behind this client id changes,
		// and when SIGUSR1 replaces the file.
		//
		// **The identity, not the client id**, which is the entry's key and
		// so never differed: a new identity taking over a live or recently
		// left client id kept the figures resolved for the one before it.
		// device-1 (3/s) taking over an id device-7 (200/s) held sent 20 with
		// none refused, which is any client exceeding its own limit by
		// taking a faster client's id. The
		// budget stays the client id's; only the figures follow the name.
		if !a.haveResolved || a.resolvedFor != identity || a.resolvedGen != gen {
			a.rate, a.byteRate = b.limits.PublishRate, b.limits.PublishBytes
			if az != nil {
				if r, by, named := az.PublishLimits(identity); named {
					// **Replaces rather than tightens.** An exception may be
					// looser than the fleet it sits in - a gateway that
					// legitimately publishes faster than a sensor is most of
					// why per-client limits exist. Tightest-wins would make
					// the broker-wide figure a ceiling nobody could exceed.
					a.rate, a.byteRate = r, by
				}
			}
			a.resolvedFor, a.haveResolved, a.resolvedGen = identity, true, gen
		}
		rate, byteRate := a.rate, a.byteRate
		a.mu.Unlock()

		if rate <= 0 && byteRate <= 0 {
			return true, rate, byteRate
		}
		return a.allow(size, time.Now()), rate, byteRate
	}
}

// forgetAllowance marks a client's budget as no longer held by a
// connection. It does not drop it: sweepAllowances does that, once the
// budget has refilled and a kept one is indistinguishable from a new one.
//
// **Dropping it here was the hole.** A budget is what one client may spend
// in a second, and a client that got a fresh one on every connect could
// spend a second's worth per reconnect - bounded only by how fast it could
// reconnect, which nothing bounds. Keeping it until it has refilled costs a
// client that has genuinely been quiet nothing, because a refilled budget
// is a full one.
//
// The bound invariant 13 asks for is still there and is now the sweep's:
// what is held between a disconnect and the next sweep is one entry per
// client id that has disconnected within it, rather than one per client id
// ever seen.
func (b *Broker) forgetAllowance(clientID string) {
	if v, ok := b.allowances.Load(clientID); ok {
		a := v.(*publishAllowance)
		a.mu.Lock()
		a.leftAt = time.Now()
		a.mu.Unlock()
	}
}

// sweepAllowances drops the budgets of departed clients that have refilled.
//
// A budget is only worth keeping while it says something a fresh one would
// not, which is exactly while it is short of full.
func (b *Broker) sweepAllowances(now time.Time) {
	b.allowances.Range(func(k, v any) bool {
		a := v.(*publishAllowance)
		a.mu.Lock()
		gone := !a.leftAt.IsZero() &&
			a.msgs.full(float64(a.rate), now) &&
			a.bytes.full(float64(a.byteRate), now)
		if gone {
			// Marked under the same lock a publish takes, so a client
			// reconnecting at this moment cannot start spending against an
			// entry that is on its way out of the map.
			a.dead = true
		}
		a.mu.Unlock()
		if gone {
			b.allowances.CompareAndDelete(k, v)
		}
		return true
	})
}

// checkBounds holds a publish to the configured limits, returning nil when
// it passes. max_message_size is absent here on purpose: the server checks
// it against the declared remaining length before the body is read, which
// is earlier than any hook can run (RFC 0002 "Publishing").
//
// Both header bounds answer 0x97 (Quota exceeded). PUBACK admits a short
// list of reason codes and there is no "too many properties" among them;
// 0x97 is the one that says the client asked for more than it may have.
func (b *Broker) checkBounds(pk packets.Packet) error {
	var reason packets.Code

	switch {
	case len(pk.TopicName) > b.limits.MaxTopicLength:
		reason = packets.ErrTopicNameInvalid
	case channel.TooDeep(pk.TopicName, b.limits.MaxTopicLevels) != nil:
		// limits.max_topic_levels, answered as max_topic_length is: both
		// bound what a topic name may be, and here too for a bridged record,
		// which is asked this function (BridgeClient.Bounded).
		reason = packets.ErrTopicNameInvalid
	case pk.Properties.PayloadFormatFlag && pk.Properties.PayloadFormat == 1 &&
		!utf8.Valid(pk.Payload):
		// The publisher said this payload is UTF-8 and it is not
		// (MQTT-3.3.2-4). Saying so is worth more than it looks: the
		// indicator exists so that a consumer can treat the bytes as text
		// without checking, and a record stored under a false one is a
		// decoding failure somewhere downstream, at a consumer that has no
		// way to know the producer lied.
		//
		// saguin does not otherwise interpret a payload - it is opaque bytes
		// (RFC 0001) - and this is not interpretation. It is the one claim a
		// publisher makes *about* the bytes, checked against them.
		reason = packets.ErrPayloadFormatInvalid
	case len(pk.Properties.User) > b.limits.MaxHeaderCount:
		reason = packets.ErrQuotaExceeded
	default:
		n := 0
		for _, u := range pk.Properties.User {
			n += len(u.Key) + len(u.Val)
		}
		if n > b.limits.MaxHeaderBytes {
			reason = packets.ErrQuotaExceeded
		}
	}

	if reason.Code == 0 {
		return nil
	}

	// Said out loud, because at QoS 0 this is the only place it is said at
	// all. refuseStore logs whatever the QoS and this did not, so a producer
	// sending oversized topics or too many headers at QoS 0 was discarded in
	// silence on both sides: no reply by construction, and nothing in the
	// log to find afterwards. It costs nothing on the accepted path, which
	// returns above.
	b.log.Warn("refusing publish: outside the configured bounds",
		"topic", b.limits.Loggable(pk.TopicName), "qos", pk.FixedHeader.Qos,
		"reason", reason.Reason, "headers", len(pk.Properties.User))

	// A QoS 0 publish has no reply to carry the refusal, so it is dropped.
	// Returning the reason code instead would leave the server with no
	// branch to take and the packet would be delivered anyway - refused and
	// published at the same time, which is the worst of both.
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return reason
}

// refuseStore turns a store that could not keep a record into a refusal.
//
// It takes a name and a bound rather than a channel because the retained
// store is neither: it is one store, holding broadcast topics, with no
// channel behind it and its provider's `max_bytes` as its ceiling. The
// rule about which reason code answers which failure is the same for both
// and is stated once, here, rather than copied to a second site that would
// drift from this one.
//
// The record was not stored, so the publish must not be acknowledged:
// acknowledged and lost is worse than refused, and a producer that is told
// its publish failed can retry, buffer, or raise an alarm. A producer told
// it succeeded can do none of those.
//
// The two refusals are different answers to a producer and must not be
// collapsed into one:
//
//   - **0x97 (Quota exceeded)** when the store is at its size bound. The
//     storage is working. The channel is full, and on a queue it empties
//     again as workers resolve records, so this says "try later" and
//     trying later works.
//   - **0x83 (Implementation specific error)** for anything else. This one
//     says the storage did not work, and an operator reading it goes and
//     looks at the disk.
//
// A full channel reported as 0x83 sends that operator after a fault that
// is not there, which is why ErrFull is separable from every other failure
// in the first place.
//
// 0x83 rather than 0x80: the publish itself is valid - nothing the client
// sent is wrong - and the broker is simply unable to accept it, which is
// what MQTT 5 gives 0x83 for. 0x80 is the code for declining to say why,
// and every refusal here carries a Reason String saying exactly why.
func (b *Broker) refuseStore(name, provider string, maxBytes int64, pk packets.Packet, err error) error {
	full := errors.Is(err, store.ErrFull)
	if full {
		// **Both bounds, named for what they are.** A channel's own
		// max_bytes and its provider's are different ceilings and either can
		// be the one that refused, so a line saying `max_bytes` said neither
		// - it printed the channel's, which is zero on a channel that
		// declares none, so a provider refusing at 3MiB reported a bound of
		// nothing. An operator reading this has to be
		// able to tell which ceiling to raise.
		b.log.Warn("refusing publish: the store is at its size bound",
			"store", name, "topic", b.limits.Loggable(pk.TopicName),
			"channel_max_bytes", maxBytes,
			"provider", provider, "provider_max_bytes", b.providerBound(provider))
	} else {
		b.log.Error("refusing publish: the storage could not keep it",
			"store", name, "topic", b.limits.Loggable(pk.TopicName), "error", err)
	}

	// A QoS 0 publish has no reply to carry the refusal, so it is dropped -
	// the same trade as an out-of-bounds publish, and one more reason a
	// producer that cares uses QoS 1.
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	if full {
		return packets.ErrQuotaExceeded
	}
	return packets.ErrImplementationSpecificError
}

// providerBound is the ceiling a provider is held to, or zero when it has
// none or is not one saguin measures. It is the same number
// saguin_provider_max_bytes carries, asked the same way, so a log line and a
// scrape cannot disagree about what refused a publish.
func (b *Broker) providerBound(provider string) int64 {
	if provider == "" {
		return 0
	}
	if m := b.measures[provider]; m != nil {
		return m.MaxBytes()
	}
	return 0
}

// refuseDerived turns a publish into a derived dead-letter channel into
// 0x87 (RFC 0002 "Publishing").
//
// 0x87 rather than 0x90 because the topic is not the problem: it names a
// real channel, and every read of that channel is legitimate. What is
// refused is this client writing to it, which is what that row of the table
// describes - a channel that takes its records from one place and from
// nothing else.
func (b *Broker) refuseDerived(c *channel.Channel, pk packets.Packet) error {
	b.log.Warn("refusing publish: a dead-letter channel takes records only from its queue",
		"channel", c.Name, "topic", b.limits.Loggable(pk.TopicName))

	// A QoS 0 publish has no reply to carry the refusal, so it is dropped -
	// the same trade as every other refusal here.
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return packets.Code{
		Code: packets.ErrNotAuthorized.Code,
		Reason: "a dead-letter channel takes records only from the queue that derives it; " +
			"publish to the queue instead",
	}
}

// deleteLatest is a zero-length publish to a latest channel: the topic's
// value goes, following MQTT's own retained-message convention.
//
// **The deletion is stored rather than merely applied**, as a value with no
// payload and an offset of its own, and it changes nothing that anybody can
// see. A subscriber is not sent it - Match leaves deletions out - so a later
// subscriber still sees nothing about a deleted topic rather than an empty
// record, which is what this channel promised before deletions were stored
// at all. A point read still answers with an empty payload, because a topic
// never set and one whose value was deleted are the same answer, which
// RFC 0003 already said.
//
// What it is for is a reader mirroring this channel on another broker - a
// bridge that asked for deletions (RFC 0003 "`latest`", `saguin-deletions`).
// A state made only of current values cannot say that a topic is gone - so
// a topic deleted while the link was down would live at the far end for
// ever. Stored, the deletion has a place in the drain and the far end
// learns it like anything else.
//
// It also makes a deletion an ordinary thing on this channel: it has a
// position now, where before it was the one event here with none, and two
// deletions of one topic order against each other like any two values.
//
// **A deletion of a topic that has no value stores nothing.** There is
// nothing to say is gone, and storing one would let anybody fill a channel
// with rows for topics that never existed.
func (b *Broker) deleteLatest(c *channel.Channel, lt LatestStore,
	pk packets.Packet, rec store.Record) error {

	// **A live value, not merely a row.** Get answers for a stored deletion
	// too, so asking it alone would make deleting an already-deleted topic
	// store a second deletion at a new offset, and a client repeating itself
	// would fill the channel with them.
	held, found, err := lt.Get(rec.Topic)
	if err != nil {
		return b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
	}
	out := rec
	if found && !store.IsDeletion(held) {
		var stored store.Record
		err := b.withRoom(c.Storage, store.RecordSize(rec), "channel "+c.Name, func() (err error) {
			stored, err = lt.Set(rec)
			return err
		})
		if err != nil {
			return b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
		}
		out = stored
		b.log.Debug("deleted", "channel", c.Name, "topic", b.limits.Loggable(rec.Topic))
	}
	// Delivered whether or not there was a value here: that is how a
	// subscriber learns the topic is gone, and one may hold a value this
	// broker has already expired. What goes out is the stored deletion where
	// there is one, so it carries the offset it was given - a deletion used
	// to be the one thing on this channel a consumer could not place against
	// the value it replaced.
	b.publishLatest(c, out)
	return packets.CodeSuccessIgnore
}

// carries reports whether a publish names a User Property at all, which is
// a different question from what its value is: an empty one is present and
// says nothing.
func carries(pk packets.Packet, key string) bool {
	for _, u := range pk.Properties.User {
		if u.Key == key && u.Val != "" {
			return true
		}
	}
	return false
}

// bridgeName is the configured name of the bridge a publish came from, and
// whether it came from one at all.
//
// It asks the table a bridge is registered in rather than anything about
// the client's name or how it arrived: a client id is the client's to
// choose, and `cl.Net.Inline` is true of a Will and of a queue delivery as
// well, neither of which is a bridge.
func (b *Broker) bridgeName(cl *mqtt.Client) (string, bool) {
	b.bridgeMu.RLock()
	defer b.bridgeMu.RUnlock()
	bc, ok := b.bridges[cl]
	if !ok {
		return "", false
	}
	return bc.name, true
}

// keepRetained puts a broadcast topic's current value in the retained
// store, and returns a refusal when it could not be kept.
//
// The publish goes on being an ordinary broadcast afterwards: it is
// delivered live to whoever is subscribed at the time, and the store is
// what a later subscriber is served from. Both halves happen, which is
// what MQTT asks of a retained publish.
//
// A zero-length payload deletes the entry, following MQTT's own
// convention, and the delete is still broadcast live - that is how a
// subscriber learns the topic is gone.
//
// Nothing saguin adds is stored with it. The value goes back out as it was
// published, so a broadcast message never gains a record identity or a
// position it did not have (RFC 0003 "Retained messages").
func (b *Broker) keepRetained(cl *mqtt.Client, pk packets.Packet) error {
	// **The QoS travels with the value.** A retained message is a
	// publisher's message handed on later, so [MQTT-3.8.4-8] makes its
	// delivery QoS the lower of the subscription's and the one it was
	// published at - and a store that did not keep it had nothing to take
	// the minimum of. Measured against Eclipse Paho's suite before this:
	// three values retained at QoS 0, 1 and 2 all came back at 2, which
	// also left the subscriber holding an exactly-once exchange for a
	// message published at QoS 0.
	r := store.Record{Topic: pk.TopicName, Payload: pk.Payload, Timestamp: time.Now()}

	// **The retained store is the one thing a broadcast topic keeps, so it
	// carries the mark like any other stored record.** Broadcast otherwise
	// stores nothing, which is why the mark rides the packet there - but a
	// retained value outlives the connection that set it, and an outbound
	// rule handed the peer's own retained set back would echo it once per
	// restart. Taken from the publishing client for the reason record()
	// takes it there: who sent it is a fact this broker holds, and a user
	// property saying the same thing is a claim anybody can make.
	r.Bridge, _ = b.bridgeName(cl)

	// **And the publisher, for the same reason and one more**: a retained
	// value is served on every subscribe, so a No Local subscriber that set
	// it would otherwise be handed its own value back on each reconnect -
	// once per reconnect, for as long as the value stands.
	r.Publisher = b.publisherOf(cl)

	// **What the publisher sent travels with it**, which is the other half
	// of "as it was published" and was missing: the value was kept and its
	// User Properties and Content Type were not, so an MQTT 5 device
	// retaining a Content Type on its status topic had it seen by whoever
	// was subscribed at that moment and by nobody who subscribed later. The
	// same message read two ways depending on when you arrived.
	//
	// **The reserved prefix is dropped, and the stored copy matches the
	// live one because both drop it.** This kept `saguin-` names verbatim
	// on the stated grounds that the live fan-out kept them too, so that a
	// stored value and a live one would not disagree. The premise was the
	// defect: fanOut now strips a broadcast packet as well, so keeping them
	// here is what would make the two disagree - and a `saguin-offset` a
	// publisher invented, handed to a late subscriber as retained state,
	// is the forgery outliving the connection that sent it.
	//
	// Nothing saguin adds is stored either: no Message ID and no offset. A
	// broadcast message has no record identity and no position, and giving
	// it one on the way out would make broadcast look like a channel
	// (RFC 0003 "Retained messages"). So a stored broadcast value carries
	// exactly the publisher's own properties, which is what it was always
	// meant to be.
	for _, u := range pk.Properties.User {
		if strings.HasPrefix(u.Key, reservedPrefix) {
			continue
		}
		r.Headers = append(r.Headers, store.Header{Key: u.Key, Value: u.Val})
	}
	r.SetProps(store.Props{
		ContentType:       pk.Properties.ContentType,
		ResponseTopic:     pk.Properties.ResponseTopic,
		CorrelationData:   pk.Properties.CorrelationData,
		PayloadFormat:     pk.Properties.PayloadFormat,
		PayloadFormatFlag: pk.Properties.PayloadFormatFlag,
		MessageExpiry:     pk.Properties.MessageExpiryInterval,
		// Present and empty, kept as such.
		ContentTypeEmpty:     pk.Properties.ContentTypeFlag,
		ResponseTopicEmpty:   pk.Properties.ResponseTopicFlag,
		CorrelationDataEmpty: pk.Properties.CorrelationDataFlag,
		// **Set here rather than on the literal above**, because SetProps
		// writes the whole set: a field assigned before it and left out of
		// this literal is zeroed again, silently. That is how the first
		// version of this lost it - the QoS was stored on the record and
		// then overwritten one statement later, and every retained value
		// went out at 0.
		QoS: pk.FixedHeader.Qos,
	})

	if len(r.Payload) == 0 {
		had, err := b.retained.Delete(r.Topic)
		if err != nil {
			return b.refuseStore(RetainedStoreName, b.retainedProvider, 0, pk, err)
		}
		if had {
			b.log.Debug("deleted", "store", RetainedStoreName, "topic", b.limits.Loggable(r.Topic))
		}
		return nil
	}

	if err := b.withRoom(b.retainedProvider, store.RecordSize(r), "the retained store", func() error {
		_, err := b.retained.Set(r)
		return err
	}); err != nil {
		return b.refuseStore(RetainedStoreName, b.retainedProvider, 0, pk, err)
	}
	return nil
}

// OnRetainMessage takes back the copy the substrate has just put in its own
// retained store, so that the retain flag can survive to delivery without a
// second store existing to hold it.
//
// saguin used to clear the flag on the way past instead. That kept the
// substrate's store empty - it skips it for a packet with the flag down -
// and cost one nonconformance: MQTT-3.3.1-13 says a subscriber that asked
// for Retain As Published sees the flag as the publisher set it, and every
// such subscriber saw 0.
//
// The order is what makes this work. The substrate stores its copy, calls
// this, and only then fans the message out, so the flag is still set when
// the fan-out reads it - which is where MQTT-3.3.1-12 and -13 are applied,
// per subscriber - while the store it was briefly in is empty again.
//
// Invariant 13 holds because that store never accumulates: one entry
// between those two calls, and nothing between publishes. What is left is a
// window rather than a leak - a SUBSCRIBE arriving inside it is served the
// substrate's copy as well as saguin's, which is a duplicate of an
// identical value and never a loss.
//
// Measured rather than assumed, by hammering SUBSCRIBE against a stream of
// retained publishes on one topic: 187 of 37,864 subscribes (0.49%) at
// 1,676 retained publishes a second, and 345 of 2,945 (11.7%) with four
// publishers saturating the broker. It scales with the rate, at about three
// microseconds of exposure per retained publish, so a fleet publishing a
// hundred retained values a second is around 0.03%.
//
// Deleting is the same call with an empty payload, which is MQTT's own
// convention for removing a retained message rather than anything special.
//
// Every path reaches it: the substrate retains from the publish path, from
// a Will and from a delayed Will, and all three go through here, so a
// retained Will needs no case of its own.
func (b *Broker) OnRetainMessage(_ *mqtt.Client, pk packets.Packet, _ int64) {
	b.retaken.Add(1)
	b.srv.Topics.RetainMessage(packets.Packet{TopicName: pk.TopicName})
}

// RetakenFromSubstrate is how many times a message has entered the
// substrate's retained store for OnRetainMessage to take back out.
//
// It exists because RetainedElsewhere below cannot tell "never stored" from
// "stored and removed", and for a channel's record the difference is the
// whole point: while it sits in that store a wildcard at channel depth can
// reach it, which is invariant 11 rather than a question of size. A channel
// publish is supposed to reach that store never - the substrate skips it
// for a packet a hook ignored - and this is what says so. Proved by
// mutation: the end-state count passes when a channel branch stops being
// ignored, and this one does not.
func (b *Broker) RetakenFromSubstrate() uint64 {
	return b.retaken.Load()
}

// RetainedElsewhere is what the substrate's own retained store holds, by
// topic and in name order. It exists for the test that says saguin's is the
// only store there is: the other one is invisible from the wire except as a
// duplicate delivery, and reading it is the difference between proving it is
// empty and inferring it.
//
// **It reports every topic, and it used to return a count that excused a
// prefix.** The substrate wrote its twenty `$SYS` counters straight into
// this store, past every hook and on a timer, so the first version read 20
// against a store nothing had published to and the fix was to skip them. An
// exemption is a thing that stops being true quietly: it covered a whole
// namespace on the grounds of what was in it that day. The engine has
// written no `$SYS` tree since its publisher was removed, so this is the whole store again.
//
// It names them rather than counting them because a count cannot be acted
// on. One run of the suite reported four messages here, once, and has not
// done so since across five full runs - and a number is all that run left
// behind, so there is nothing to say what they were. The next occurrence
// says.
func (b *Broker) RetainedElsewhere() []string {
	all := b.srv.Topics.Retained.GetAll()
	topics := make([]string, 0, len(all))
	for topic := range all {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// deliverRetained sends a new subscriber the stored value of every
// broadcast topic its filter reaches, as MQTT delivers retained messages -
// and only as MQTT delivers them, because the whole point of this store is
// that an unmodified client works.
//
// Three rules, all MQTT's:
//
//   - Nothing goes to a shared subscription.
//   - Retain Handling 2 sends nothing, and 1 sends nothing when the client
//     already held this filter.
//   - Delivery happens on SUBSCRIBE. A session resumed without one is sent
//     nothing here, which is what a standard broker does. A `latest`
//     channel deliberately differs, and says so.
func (b *Broker) deliverRetained(cl *mqtt.Client, f packets.Subscription, existed bool,
	declared partition) {
	b.mu.Lock()
	lt := b.retained
	b.mu.Unlock()
	if lt == nil {
		return
	}
	if mqtt.IsSharedFilter(f.Filter) {
		return
	}
	if f.RetainHandling == 2 || (f.RetainHandling == 1 && existed) {
		return
	}

	// The filter a shared subscription carries reaches the same topics as
	// the one inside it, so the match is made against that. Without the
	// stripping the guard above would be dead code - a `$share/…` filter
	// matches no topic name, so nothing would be delivered whether or not
	// anybody remembered the rule - and a guard that cannot fail is a guard
	// nobody can test.
	inner := channel.InnerFilter(f.Filter)
	state, err := lt.Match(func(topic string) bool {
		// **The declared slice, because this is a current-state pass.** It
		// is the fourth delivery site and the one the partitioning commit
		// did not enumerate: a member sent the whole retained store at
		// subscribe and then only its share of the live traffic holds a
		// copy that starts complete and drifts - the same failure RFC 0003
		// already names for a `latest` channel's snapshot, on the store
		// RFC 0002 gives broadcast topics. Across a group every member
		// received every retained topic, which is state processed once per
		// member rather than once per group.
		//
		// Asked before the match rather than after, because the hash is the
		// cheaper question only when a slice was declared - and when none
		// was, this costs a bool.
		if declared.declared() && !declared.wants(channel.PartitionHash(topic)) {
			return false
		}
		// A topic a channel claims is never in this store, so nothing here
		// can reach across a channel boundary. The filter is matched with
		// saguin's own matcher rather than the server's, so that one rule
		// answers "does this filter reach this topic" everywhere.
		return channel.Matches(inner, topic)
	})
	if err != nil {
		// The subscription stands and the subscriber simply has not been
		// sent what was stored. Silence would leave somebody looking at a
		// dashboard that never fills in with no reason why.
		b.log.Error("cannot read retained messages for a new subscriber",
			"client", cl.ID, "filter", b.limits.Loggable(f.Filter), "error", err)
		return
	}
	// The store's subscriptions are not in saguin's index - a filter
	// reaching only broadcast topics is deliberately not recorded there - so
	// the identifier can only come from the SUBSCRIBE being answered, and it
	// is stamped on each value here while that is still in hand.
	//
	// **A value whose publisher's Message Expiry has run out is not
	// state any more** (MQTT-3.3.2-5): the sweep deletes it on its own
	// clock, and this is the window between the expiry and the tick -
	// which on a broker whose shortest retention is long is most of the
	// time. Skipped here rather than deleted here, because a read path
	// that writes is how a subscribe comes to race the sweep. Note the
	// asymmetry this checks the *retained store's* clock only: a `latest`
	// channel serves an expired record with a countdown of 0, because
	// there the operator's retention is the only clock that removes
	// anything (invariant 2).
	now := time.Now()
	queued := make([]pendingValue, 0, len(state))
	for _, r := range state {
		if r.Expired(now) {
			continue
		}
		// **No Local, on the one broadcast delivery the substrate does not
		// make** [MQTT-3.8.3-3]. The live fan-out compares the packet's
		// origin there; a retained value is served out of a store on every
		// subscribe, so the publisher that set it would be handed its own
		// value back once per reconnect for as long as it stands. The flag
		// is taken off the SUBSCRIBE being answered because a filter
		// reaching only broadcast topics is deliberately not in saguin's
		// index, so there is no subscription record to read it from.
		//
		// **This is the one place implementations diverge, and the reading
		// here is the specification's literal one**: 3.8.3.1 says an
		// Application Message is not forwarded to a connection whose client
		// id equals the publishing connection's, and a retained value served
		// on subscribe is an Application Message being forwarded. Eclipse
		// Paho's interoperability suite does not settle it - it never
		// combines the flag with a retained publish, and the broker it runs
		// against has no channels - so this is written down rather than left
		// to a suite that was checked and found silent.
		if f.NoLocal && r.Publisher == cl.ID {
			continue
		}
		queued = append(queued, pendingValue{rec: r, ident: f.Identifier, qos: f.Qos, retain: true})
	}
	// **What goes out at QoS 1 or 2 to a session that outlives its
	// connection is kept in the broadcast log, owed to it alone**
	// (bdrain.keepRetained), so a restart sends it again under its identifier
	// until it is acknowledged (MQTT-4.4.0-1). The rest are sent from here.
	if d := b.broadcastDrain(); d != nil && d.session(cl.ID) != nil {
		queued = d.keepRetained(cl.ID, queued)
	}
	if len(queued) == 0 {
		return
	}

	// b.mu to find or make the record, and its cmu for the list, in that
	// order - a subscribe, not a publish, so the broker-wide lock is paid
	// once per SUBSCRIBE rather than per value.
	b.mu.Lock()
	// **Only for the connection holding the id, asked in this hold**
	// (invariant 17). A SUBSCRIBE whose connection was taken over while the
	// values were read made a record for the id after the takeover had
	// dropped it, and nothing removed it until that id next disconnected as
	// owner.
	if b.owner[cl.ID] != cl {
		b.mu.Unlock()
		return
	}
	con := b.consumerLocked(cl.ID)
	con.cmu.Lock()
	if con.latest == nil {
		con.latest = map[string][]pendingValue{}
	}
	// **One value per topic and subscription, replaced rather than
	// appended**, as every other writer of a pending list keeps it
	// (enqueueLatest). Retain Handling 0 has each SUBSCRIBE sent the retained
	// values, and sending each one's copy is right; keeping every copy while
	// the client's window is full is not. 200 SUBSCRIBEs of one filter left
	// 20,099 values waiting for a store of 100 topics, bounded by nothing but
	// how often the client subscribed. The newer
	// copy carries the newer subscription's options, and a value already on
	// the wire is not in this list to be replaced.
	pending := con.latest[RetainedStoreName]
	at := make(map[retainedKey]int, len(pending))
	for i, p := range pending {
		at[retainedKey{p.rec.Topic, p.ident}] = i
	}
	for _, q := range queued {
		k := retainedKey{q.rec.Topic, q.ident}
		if i, ok := at[k]; ok {
			pending[i] = q
			continue
		}
		at[k] = len(pending)
		pending = append(pending, q)
	}
	con.latest[RetainedStoreName] = pending
	con.cmu.Unlock()
	b.mu.Unlock()

	b.drainLatest(cl, RetainedStoreName)
}

// retainedKey is what makes two retained values waiting for one client the
// same value: its topic, and the subscription it is sent for.
type retainedKey struct {
	topic string
	ident int
}

// refuseRetain disconnects a client whose retained publish saguin cannot
// honour, and says which of the two reasons it is.
//
// **The rule, in one sentence: the retain flag is honoured wherever it can
// be and refused wherever it cannot, and it is never accepted and
// ignored.** Two places cannot honour it. A broadcast topic has a store
// only where `broker.retained` names a provider, and with no such block
// there is nowhere to keep the value. A queue holds work rather than
// state, hands each record to one worker, and grants no subscription that
// a retained message may be delivered to - so there is nothing to keep and
// nobody to keep it for.
//
// Everywhere else it is honoured: an `append` channel stores the record
// and replays it, and a `latest` channel is exactly a store of current
// values, which is what the flag is asking for.
//
// Refusing costs a connection and buys the one thing worth buying: a
// producer is never told its state was kept when nothing kept it. Accepting
// and ignoring is what a broker does when the protocol lets it, and it is
// the only case in saguin where an acknowledgement would not mean what it
// says.
//
// What is refused here is never MQTT's own retained store, which nothing
// is ever left in: that one is broker-wide, unbounded, and has no retention
// at all, and left to fill it took this broker from 14MB to 495MB in 1.45
// seconds from one client, which is what invariant 13 refuses. A message
// passes through it and OnRetainMessage takes it straight back out.
// saguin's is a named provider with a bound and a retention period, which
// is the whole of the difference (RFC 0002 "Retained messages on a
// broadcast topic").
//
// It is a disconnect because MQTT says so: a PUBLISH with RETAIN to a
// server that does not support it is a protocol error, answered with 0x9A.
//
// A client can learn which broker this is before trying: the CONNACK
// carries Retain Available, 1 where the flag can be honoured somewhere and
// 0 where it can be honoured nowhere, which OnPacketEncode writes.
func (b *Broker) refuseRetain(cl *mqtt.Client, pk packets.Packet, why string) error {
	b.log.Warn("disconnecting: this retained publish cannot be honoured",
		"client", cl.ID, "topic", b.limits.Loggable(pk.TopicName), "reason", why,
		"fix", "publish to a latest channel, which is the same thing with a bound on it")
	b.disconnect(cl, packets.ErrRetainNotSupported)
	// Both codes, in this order, because the answer is 0x9A and the hook
	// contract only understands ErrRejectPacket.
	//
	// The substrate tests ErrRejectPacket with errors.Is, so it stops the
	// packet exactly as a bare one did - the client has already had its
	// DISCONNECT. saguin's refusal counter reads the code with errors.As,
	// which walks the wrapped errors in the order they are written here and
	// takes the first: returning ErrRejectPacket alone labelled this refusal
	// "packet rejected", indistinguishable from every other refusal that
	// answers for itself, where the client was told "retain not supported".
	//
	// **ErrRejectPacket is itself a reason code**, so the order is the whole
	// of the fix and reversing it silently restores the defect.
	return fmt.Errorf("%w: %w", packets.ErrRetainNotSupported, packets.ErrRejectPacket)
}

// refuseReserved turns a publish into the reserved `$saguin/` space that is
// none of the topics saguin defines there into 0x90 (RFC 0002
// "Publishing").
//
// saguin defines exactly two: `$saguin/consumer/<channel>/seek` and
// `$saguin/queue/<queue>/response`. Everything else under the prefix did
// nothing and was answered Success, which is the worst of the three
// available answers - a client told its publish succeeded cannot retry it,
// correct it, or raise an alarm, and the only trace was a Warn line in the
// broker's log that the client never sees.
//
// **The topic names the target and the payload carries the request**, and
// this refusal is the first half only. A seek naming a channel that does
// not exist, or one that cannot hold a position, is a topic this broker
// does not have, and so is a response naming something that is not a queue
// - both are refused here, before either handler runs. What is *inside* the
// message is the handler's business and is answered its way: a seek by a
// reply on the client's own Response Topic, a response by being ignored and
// logged, which is RFC 0003 saying a worker is never told whether its
// answer was applied.
//
// Splitting it anywhere else produces an inconsistency rather than a rule.
// Refusing only a response whose queue is missing would put a verdict in the
// PUBACK for a mistyped queue name and none in it for a stale Delivery ID,
// the wrong session, or a payload that is neither ack nor return - three
// cases the same handler ignores. Refusing neither leaves a worker with a
// typo acknowledging into silence for ever, because a response has no reply
// path to tell it otherwise.
//
// Answering in the PUBACK is also the only answer that always arrives. A
// seek's reply needs the client to have set a Response Topic and to still be
// connected, which is why `mosquitto_pub` - publish and exit - cannot show
// you a refused seek at all.
//
// The Reason String carries which of them it was, because the code alone
// leaves an operator looking at a topic that is perfectly well formed.
func (b *Broker) refuseReserved(pk packets.Packet, why string) error {
	b.log.Warn("refusing publish into the reserved space",
		"topic", b.limits.Loggable(pk.TopicName), "reason", why)

	// A QoS 0 publish has no reply to carry the refusal, so it is dropped -
	// the same trade as an out-of-bounds publish. Returning the reason code
	// instead would leave the server with no branch to take and the packet
	// would be delivered to whoever is subscribed under the prefix.
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return packets.Code{Code: packets.ErrTopicNameInvalid.Code, Reason: why}
}

// refuseTopicName turns a publish to a topic name MQTT does not allow one
// to into `0x90`, whoever sent it.
//
// **`$share/` is one of them and the substrate does not say so**, which is
// why saguin asks separately. Its validator accepts the shape for a publish
// because the string is a well-formed shared-subscription *filter*, and a
// filter is not what a PUBLISH carries. Measured, with whatever ShareName
// was to hand: a publish to `$share/saguin/jobs/x` was answered `0x00` and
// reached nobody - the name in the middle is a group's and no longer
// resembles anything saguin defines. It cannot
// reach anybody - every subscription beginning `$share/` is read as a
// shared subscription rather than as a topic, so no client can ask for
// these topics, and no channel can claim them either, since a channel
// filter beginning `$` is refused when the file loads. A publish here is
// therefore always accepted, always discarded, and always reported as
// having succeeded, which is the failure the reserved-space refusal above
// exists to prevent, one prefix along.
//
// **Exactly `$share/`, case sensitively**, because that is the only
// spelling MQTT reads as a subscription form (MQTT-4.7.3, MQTT-4.8.2-1).
// `$SHARE/x` is an ordinary topic that an ordinary filter can subscribe
// to, so publishing there is legitimate and is not refused.
//
// A wildcard is the one that costs, and it costs durably. A record whose
// topic holds `+` or `#` is stored like any other and then delivered like
// any other - at which point a conforming subscriber closes the connection,
// because MQTT-3.3.2-2 forbids a wildcard in a delivered topic name as
// firmly as in a published one. It is at the head of the channel for ever,
// every consumer stops there, and on a `latest` channel no client can even
// remove it: deleting a value means publishing an empty payload to its
// topic, which is the same refused publish.
//
// The reason names the rule rather than the packet, because the packet is
// usually fine and the person reading the log is looking at a topic that
// appears perfectly ordinary until they notice the `+`.
//
// notATopicName is the question, and it is asked in two places: here, of a
// publish, and at CONNECT of a Will (willRefusal). **One function, because
// two answers disagreed**: the Will asked the substrate's IsValidFilter
// alone, which accepts `$share/g/x`, so a Will there was answered 0x00 at
// CONNECT and refused when it fired, reaching nobody.
func (b *Broker) refuseTopicName(pk packets.Packet) error {
	why := notATopicName(pk.TopicName)

	b.log.Warn("refusing publish: not a topic anything may publish to",
		"topic", b.limits.Loggable(pk.TopicName))

	// The same trade the reserved-space refusal makes: at QoS 0 there is no
	// reply to carry a reason code, and returning one leaves the server with
	// no branch to take and the packet delivered anyway.
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return packets.Code{Code: packets.ErrTopicNameInvalid.Code, Reason: why}
}

// notATopicName says why nothing may publish to topic, or "" where
// something may: a wildcard (MQTT-3.3.2-2) or the reserved `$SYS` tree,
// both the substrate's own answer, and `$share/`, which is a subscription
// form (refuseTopicName).
func notATopicName(topic string) string {
	switch {
	case strings.HasPrefix(topic, "$share/"):
		return "$share/ is a subscription form and not a topic name: every subscription to " +
			"one is read as a shared subscription, so nothing can ever receive a publish here"
	case !mqtt.IsValidFilter(topic, true):
		return "a topic name may not contain + or #, and may not be in the reserved $SYS space"
	}
	return ""
}

// The two answers a publish over its rate gets, kept as values so the
// metric can tell this refusal from everything else that answers the same
// way. See refusalReason.
//
// **The QoS 0 one is wrapped and the QoS 1 one is not**, because the
// substrate answers the two differently. At QoS 0 there is no reply to carry
// a code, so the refusal is `packets.ErrRejectPacket`, which processPublish
// finds with `errors.Is` and drops the packet on, wrapped around
// errOverPublishRate so the metric can still tell it apart. At QoS 1 the
// code is the reply: processPublish takes it with `errors.As` and writes it
// in the PUBACK, so the answer is the code itself, told apart by being this
// exact value - Code is comparable, so no string matching is needed and
// nothing rots if the wording changes.
var (
	errOverPublishRate = errors.New("over the client's publish rate")

	codeOverPublishRate = packets.Code{
		Code:   packets.ErrQuotaExceeded.Code,
		Reason: "publishing faster than the configured publish limits allow",
	}
)

// refuseRate answers a client that is publishing faster than
// limits.publish_rate allows.
//
// **At QoS 0 there is no reply and the log line is the whole of it**, which
// is the trade refuseTopicName makes above and for the same reason: the
// server has no branch to take for a reason code at QoS 0 and delivers the
// packet anyway. A client hammering a broadcast topic at QoS 0 is exactly
// the case this feature is for, so the refusal has to work there - it does,
// by dropping the packet; what it cannot do is say so on the wire.
//
// The log line is at debug rather than warn, and that is deliberate: a
// client being held to its rate writes one of these per refused publish,
// and a limiter that floods the log under load has moved the problem rather
// than solved it. The metric is where an operator sees it.
func (b *Broker) refuseRate(cl *mqtt.Client, pk packets.Packet, rate int, byteRate int64) error {
	// **The numbers this client was actually held to, not the broker-wide
	// ones.** An acl_file gives a device its own figures, and logging
	// b.limits printed the floor whatever they were: a device limited to 2
	// a second and one limited to 4 both produced `rate=3`, sending an
	// operator reading the line to explain a refusal to the wrong knob.
	b.log.Debug("refusing publish: over the client's publish rate",
		"client", b.limits.Loggable(cl.ID),
		"topic", b.limits.Loggable(pk.TopicName),
		"rate", rate, "bytes", byteRate)

	if pk.FixedHeader.Qos == 0 {
		return fmt.Errorf("%w: %w", errOverPublishRate, packets.ErrRejectPacket)
	}
	return codeOverPublishRate
}

// cannotAnswer reports why the channel a reserved topic names cannot answer
// it, or "" when it can. The two reasons are different to the person
// reading them: a name nothing matches is a typo or a channel that was
// never configured, and a name that matches the wrong kind of channel is a
// misunderstanding about what that channel is for.
func (b *Broker) cannotAnswer(name string, want channel.Type, does string) string {
	c := b.reg.Get(name)
	switch {
	case c == nil:
		return fmt.Sprintf("there is no channel named %q", name)
	case c.Type != want:
		return fmt.Sprintf("%q is %s, not %s: only %s %s",
			name, aChannel(c.Type), aChannel(want), aChannel(want), does)
	}
	return ""
}

// aChannel names a channel type the way the RFCs name it, so that a reason
// string reads as a sentence rather than as a struct field. A queue is a
// queue rather than a "queue channel".
func aChannel(t channel.Type) string {
	switch t {
	case channel.Append:
		return "an append channel"
	case channel.Latest:
		return "a latest channel"
	case channel.Queue:
		return "a queue"
	}
	return string(t)
}

// BridgeClient is how an inbound bridge publishes into saguin: as an
// in-process client, through the broker's own publish path, so that channel
// resolution, the size bounds, the header limits and every reason code
// apply to a bridged record exactly as they would to one from a client on
// the wire. There is no second way into a channel (RFC 0002 "Bridges").
//
// It carries the refusal back itself, because the server will not. mochi's
// InjectPacket returns nil whether the record was stored or refused: at
// QoS 1 the server turns the hook's reason code into a PUBACK and writes it
// to the publishing client, and an in-process client has no connection, so
// the write is discarded and the injection reports success. Measured on
// 2026-08-13 against the fork, the same nil for 0x97 Quota exceeded, 0x83
// Implementation specific error and ErrRejectPacket alike - the refusal is
// real, nothing is stored and nothing is delivered, and the caller cannot
// see it.
//
// That is worse here than it would be anywhere else. An inbound bridge must
// not acknowledge the upstream until saguin's own store has taken the
// record, so a bridge that believed that nil would send a PUBACK for a
// record it had dropped - acknowledged and lost, which is the failure the
// rest of saguin refuses everywhere.
//
// So the answer comes back beside the call rather than through it: Publish
// parks a slot, OnPublish fills it in when it refuses, and Publish reads it
// once the injection is over. Both happen on the caller's goroutine, since
// the server calls the hook from inside InjectPacket.
type BridgeClient struct {
	b  *Broker
	cl *mqtt.Client

	// name is the bridge as the configuration names it. The client id is not
	// it: an in-process bridge is registered as "bridge:<name>", and asking
	// an operator to write that prefix would put an internal spelling in
	// their file - and make a rename here a silent change to what their
	// channels accept.
	name string

	// mu makes one bridge's publishes serial, so the outcome below belongs
	// to the publish reading it. A bridge is serial anyway - Paho hands
	// inbound publishes to their handler one at a time, on one goroutine -
	// so this costs nothing, and it removes the way a later caller could
	// silently attribute one record's refusal to another record. Silently,
	// and in the direction that acknowledges something that was refused.
	mu sync.Mutex
	// outcome is what OnPublish decided about the publish in flight, or nil
	// if it did not refuse it. Touched only between the two halves of one
	// Publish, which mu is what guarantees.
	outcome error
}

// NewBridgeClient makes the in-process client one bridge publishes as.
//
// It is deliberately not registered in the server's client table, which is
// what NewClient does and Clients.Add would undo: a bridge holds no MQTT
// session, so there is nothing there to resume, to expire, or to take over.
// A real client connecting with the same identifier therefore cannot
// collide with it, and nothing can disconnect it but saguin.
func (b *Broker) NewBridgeClient(name string) *BridgeClient {
	// Inline, because that is what excuses an in-process client from the
	// checks that only make sense against something that arrived on a
	// socket - the topic-as-filter check, and the authorization hook, whose
	// refusal would otherwise be answered to a connection that is not there.
	cl := b.srv.NewClient(nil, mqtt.LocalListener, "bridge:"+name, true)
	// InjectPacket stamps the client's protocol version onto the packet,
	// and every answer saguin gives is an MQTT 5 answer.
	cl.Properties.ProtocolVersion = 5

	bc := &BridgeClient{b: b, cl: cl, name: name}
	b.bridgeMu.Lock()
	defer b.bridgeMu.Unlock()
	if b.bridges == nil {
		b.bridges = map[*mqtt.Client]*BridgeClient{}
	}
	b.bridges[cl] = bc
	return bc
}

// Publish puts one bridged record through the publish path and reports what
// that path decided: nil when the record was stored, or the reason code a
// client on the wire would have been given in its PUBACK.
//
// The caller acknowledges its upstream only on nil. A 0x97 means the local
// channel is at its max_bytes, and not acknowledging is what turns that into
// backpressure on the upstream subscription rather than into a discarded
// record.
func (bc *BridgeClient) Publish(topic string, payload []byte, headers []packets.UserProperty,
	props store.Props) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.outcome = nil

	pk := packets.Packet{
		// QoS 1, always. At QoS 0 a reason code is not merely invisible: the
		// server has no branch to take for one, so it falls through and
		// delivers the packet anyway. Measured - 0x97 at QoS 0 reached the
		// subscribers, at QoS 1 it did not - which is the same trap the QoS 0
		// arms of checkBounds and refuseStore already exist to avoid.
		// **The publisher's retain flag crosses the link** (RFC 0002), so a
		// value retained at the far end becomes state here and a subscriber
		// arriving later is served it. Everything a retained publish reaches
		// afterwards is the ordinary path: the retained store's bounds, the
		// acl_file's `retain` permission, and a zero-length payload as a
		// deletion. A bridge gets no route of its own into that store.
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: props.Retain},
		TopicName:   topic,
		Payload:     payload,
		// Any non-zero value: a QoS 1 packet must carry a packet identifier
		// to be valid, and nothing sends this one anywhere, so nothing reads
		// it back.
		PacketID: 1,
		Properties: packets.Properties{
			User: headers,
			// What the source held, so a copy holds it too. The record's own
			// receipt time is stamped here, so Message Expiry crosses as the
			// remainder the upstream sent rather than as the publisher's
			// original ceiling - which is what a conforming server sends and
			// what the copy should store.
			ContentType:           props.ContentType,
			ResponseTopic:         props.ResponseTopic,
			CorrelationData:       props.CorrelationData,
			PayloadFormat:         props.PayloadFormat,
			PayloadFormatFlag:     props.PayloadFormatFlag,
			MessageExpiryInterval: props.MessageExpiry,
			ContentTypeFlag:       props.ContentTypeEmpty,
			ResponseTopicFlag:     props.ResponseTopicEmpty,
			CorrelationDataFlag:   props.CorrelationDataEmpty,
		},
	}
	// **Arrived, over the bridge's own upstream connection**, so counted as
	// received as a client's PUBLISH is, and before the outcome, which
	// counts the refused ones as well (InjectPacket counts nothing).
	bc.b.srv.Info.MessagesReceived.Add(1)
	if err := bc.b.srv.InjectPacket(bc.cl, pk); err != nil {
		return err
	}
	return bc.outcome
}

// Sources are the channels an `out` rule's filter reads from, and it is the
// same question a subscriber's filter asks: **every `append` and `latest`
// channel the filter matches, and never a queue** (invariant 11).
//
// A queue is left out here rather than refused at startup, and that is the
// invariant read literally: a filter that merely crosses a queue "is served
// everything else it matches and none of the queue's records". Refusing the
// rule instead would make a bridge the one client that has to know where
// the queues are, and an operator adding a queue under a filter a bridge
// already reads would find the bridge refusing to start - punishing them
// for a rule that was correct when written and is still correct now.
//
// The one spelling that *could* drain a queue is its own subscription form
// under `$saguin/`, which no rule's filter may name in any direction.
func (bc *BridgeClient) Sources(filter string) []channel.Source {
	var out []channel.Source
	for _, c := range bc.b.reg.ResolveFilters(filter) {
		switch c.Type {
		case channel.Append, channel.Latest:
			out = append(out, channel.Source{
				Name: c.Name, Latest: c.Type == channel.Latest, StartAtTail: c.StartAtTail,
				RetentionPeriod: c.RetentionPeriod, DeletionRetentionPeriod: c.DeletionRetentionPeriod,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Watch signals on the returned channel whenever a record lands in the
// named channel, and the function stops watching.
//
// **A hint rather than a delivery.** The signal carries nothing: a watcher
// reads from its own stored position, so a wakeup it missed costs it
// nothing but the wait until the next one, and a wakeup for a record it has
// already carried costs one empty read. That is what lets the signal be
// dropped when the buffer is full - two wakeups nobody has taken yet are
// one wakeup - and it is why this cannot lose a record: the position is the
// truth and the channel is only an alarm clock.
func (bc *BridgeClient) Watch(name string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b := bc.b
	b.watchMu.Lock()
	if b.watchers == nil {
		b.watchers = map[string][]chan struct{}{}
	}
	b.watchers[name] = append(b.watchers[name], ch)
	b.watchMu.Unlock()

	return ch, func() {
		b.watchMu.Lock()
		defer b.watchMu.Unlock()
		kept := b.watchers[name][:0]
		for _, one := range b.watchers[name] {
			if one != ch {
				kept = append(kept, one)
			}
		}
		b.watchers[name] = kept
	}
}

// WatchBroadcast calls fn with every record published to a topic no channel
// claims, and the function stops watching.
//
// **The record rather than a signal, because nothing stored it.** It is the
// live fan-out and only the live fan-out: an outbound rule is never handed
// a retained pass, so what it forwards is what was published while it was
// running and a reconnect re-ships nothing (RFC 0003). The record carries
// the bridge it arrived on, so a rule can skip what another bridge brought.
func (bc *BridgeClient) WatchBroadcast(fn func(store.Record)) func() {
	b := bc.b
	b.watchMu.Lock()
	b.broadcasts = append(b.broadcasts, fn)
	at := len(b.broadcasts) - 1
	b.watchMu.Unlock()

	return func() {
		b.watchMu.Lock()
		defer b.watchMu.Unlock()
		if at < len(b.broadcasts) {
			b.broadcasts[at] = nil
		}
	}
}

// publisherOf is the client id a publish is recorded as published by, for
// No Local (RFC 0003): the connection's own, except for a Will, which is
// the client that armed it rather than the in-process client that carries
// it. mosquitto attributes a Will the same way. Without this every Will
// carried `saguin:will`, and a device's No Local subscription was sent its
// own Will. Read under willMu, which publishWill holds for the injection
// every one of its callers runs inside.
func (b *Broker) publisherOf(cl *mqtt.Client) string {
	if cl == b.willClient {
		return b.willArmer
	}
	return cl.ID
}

// bridgeOf names the bridge a client publishes as, or "" for anything else.
// It is a free function so that the broadcast path can ask it without a
// BridgeClient in hand - there is not one, because the question is about
// the publisher rather than about the reader.
func bridgeOf(b *Broker, cl *mqtt.Client) string {
	name, _ := b.bridgeName(cl)
	return name
}

// woke tells every watcher of a channel that there is something to look at.
func (b *Broker) woke(name string) {
	b.watchMu.RLock()
	defer b.watchMu.RUnlock()
	for _, ch := range b.watchers[name] {
		select {
		case ch <- struct{}{}:
		default: // one pending wakeup is as good as two
		}
	}
}

// broadcastWatched hands a broadcast record to whoever is watching, and is
// called on the publish path - so it returns at once when nobody is, which
// is every broker with no outbound bridge on a broadcast topic.
//
// **The record is built here, at the first watcher, and not by the
// caller.** Building it takes a UUID and copies the payload and the
// properties: eight allocations and 1.4 µs of every broadcast publish at
// 10,000 msg/s, for a broker that has nobody to hand it to. It is decided
// under watchMu, which WatchBroadcast takes to add a watcher, so a watcher
// is handed exactly the publishes it was before. A removed watcher leaves
// a nil in broadcasts, which is why this is not a length check.
func (b *Broker) broadcastWatched(pk *packets.Packet, from, by string) {
	b.watchMu.RLock()
	defer b.watchMu.RUnlock()
	var rec store.Record
	built := false
	for _, fn := range b.broadcasts {
		if fn == nil {
			continue
		}
		if !built {
			rec, built = b.record(*pk, nil, from, by), true
		}
		fn(rec)
	}
}

// ReadFrom is up to max records at or after offset, with the channel's
// floor, for an outbound rule draining an append channel.
//
// **The floor comes back with the records rather than being asked for
// separately**, because the two have to be read together to mean anything:
// retention moves, and a caller that read the records and then asked for
// the floor could be told a floor that had already passed what it holds.
func (bc *BridgeClient) ReadFrom(name string, offset uint64, max int) ([]store.Record, uint64, error) {
	bc.b.mu.Lock()
	lg := bc.b.logs[name]
	bc.b.mu.Unlock()
	if lg == nil {
		return nil, 0, fmt.Errorf("channel %q holds no append log", name)
	}
	recs, err := lg.ReadFromN(offset, max)
	return recs, lg.Floor(), err
}

// Head is the offset the next record written to a channel will take, which
// is where a rule reading `start: tail` begins.
func (bc *BridgeClient) Head(name string) uint64 {
	bc.b.mu.Lock()
	defer bc.b.mu.Unlock()
	if lg := bc.b.logs[name]; lg != nil {
		return lg.Next()
	}
	return 1
}

// WatchLatest hands fn each value a latest channel takes, as publishLatest
// hands it out: only the newest of its topic (latestHead), a publish with no
// local subscriber included. fn runs on the publishing goroutine and must not
// block it (invariant 16).
func (bc *BridgeClient) WatchLatest(name string, fn func(store.Record)) func() {
	b := bc.b
	b.watchMu.Lock()
	if b.latestWatchers == nil {
		b.latestWatchers = map[string][]*func(store.Record){}
	}
	at := &fn
	b.latestWatchers[name] = append(b.latestWatchers[name], at)
	b.watchMu.Unlock()
	return func() {
		b.watchMu.Lock()
		defer b.watchMu.Unlock()
		kept := b.latestWatchers[name][:0]
		for _, w := range b.latestWatchers[name] {
			if w != at {
				kept = append(kept, w)
			}
		}
		b.latestWatchers[name] = kept
	}
}

// latestWatched hands a value to every outbound rule watching its channel.
func (b *Broker) latestWatched(chName string, r store.Record) {
	b.watchMu.RLock()
	defer b.watchMu.RUnlock()
	for _, fn := range b.latestWatchers[chName] {
		(*fn)(r)
	}
}

// Position is where this bridge had reached in a channel, and whether it
// had reached anywhere at all.
func (bc *BridgeClient) Position(name, reader string) (uint64, bool, error) {
	bc.b.mu.Lock()
	lg := bc.b.logs[name]
	bc.b.mu.Unlock()
	if lg == nil {
		return 0, false, nil
	}
	p, ok, err := lg.Position(reader)
	return p.Offset, ok, err
}

// SavePosition stores how far this bridge has carried a channel.
//
// **No expiry, unlike a session's.** A stored position is removed when the
// session that owns it expires, and a bridge has no session: it is
// configuration, and what removes it is an operator deleting the rule. An
// expiry here would quietly restart a link at the floor after a long outage
// - which is the one moment its position is worth most.
func (bc *BridgeClient) SavePosition(name, reader string, offset uint64) error {
	bc.b.mu.Lock()
	lg := bc.b.logs[name]
	bc.b.mu.Unlock()
	if lg == nil {
		return nil
	}
	bc.b.positions.Lock()
	defer bc.b.positions.Unlock()
	return lg.SavePosition(store.Position{Reader: reader, Offset: offset, LastSeen: time.Now()})
}

// PositionLost counts a reader whose stored position retention had already
// passed, against the channel it happened on - the same counter an MQTT
// consumer's does, because it is the same loss and an operator watching for
// it should not have to know which kind of reader it was.
func (bc *BridgeClient) PositionLost(name, reader string, position, floor uint64) {
	if cc := bc.b.counted.forChannel(name); cc != nil {
		cc.positionLost.Add(1)
	}
	// And named, for the same reason an MQTT consumer is: the counter says a
	// channel lost records and never which reader. `reported` is false - the
	// rule resumes at the floor and the peer on the other side of the link
	// is told nothing, so nothing downstream knows there is a hole.
	bc.b.passed.add(reader, name, kindBridge, false,
		position, floor, bc.b.limits.Loggable)
}

// Bounded reports what is wrong with the record itself, whatever the state
// of the channel it is bound for, and nil when nothing is.
//
// It runs the same check the publish path runs, on the same limits, through
// the same function. A bridge holding its own copy of these bounds would be
// a second enforcement point and the two would drift (invariant 10); this
// asks the one that already exists.
//
// A bridge has to ask *before* publishing because it has nowhere to put the
// answer afterwards. What it does with a refusal is retry, without a limit
// and on purpose: a full channel is backpressure on the upstream and clears
// when a consumer drains it. A record that can never be accepted - one
// carrying more headers than saguin allows - answers 0x97 as well, and
// retrying that one holds the link for ever behind a record that will never
// move. So the two are separated here, and only the ones that can change
// their mind reach Publish.
//
// The payload check is a guard rather than the enforcement point. The bound
// is on a whole packet, and it is the upstream that applies it: the bridge
// declares Maximum Packet Size in its CONNECT, so a conforming broker never
// sends a larger one. Applying a packet bound to a payload lets a record
// within a header's worth of the limit through, which is the right way round
// for something that is only there in case the upstream does not conform.
func (bc *BridgeClient) Bounded(topic string, payload []byte, headers []packets.UserProperty,
	props store.Props) error {
	if uint32(len(payload)) > bc.b.limits.MaxMessageSize {
		return packets.ErrPacketTooLarge
	}
	return bc.b.checkBounds(packets.Packet{
		// Carried so the bounds a retained value meets are the ones it will
		// actually meet: asking about it as an ordinary publish would let a
		// record past here that the publish itself then refuses, which is
		// the split this function exists to avoid.
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: props.Retain},
		TopicName:   topic,
		// The payload goes in so that the format check sees it. It is not
		// copied: checkBounds reads and keeps nothing.
		Payload: payload,
		Properties: packets.Properties{
			User:              headers,
			PayloadFormat:     props.PayloadFormat,
			PayloadFormatFlag: props.PayloadFormatFlag,
		},
	})
}

// OnPublish routes a publish, and keeps the answer where a bridge can read
// it - see BridgeClient for why a bridge cannot read it any other way.
func (b *Broker) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// **A Will is published as the client that armed it**, which is what
	// the engine's No Local compares the packet's origin against, live and
	// in the broadcast drain. The records below take the same id through
	// publisherOf.
	if cl != nil && cl == b.willClient {
		pk.Origin = b.willArmer
	}
	pkx, err := b.routePublish(cl, pk)
	pkx, err = b.fanOut(cl, pkx, err)
	if err == nil && cl != b.inline {
		if b.holdsBroadcast(cl, pkx) {
			pkx, err = b.holdBroadcast(cl, pkx)
		} else {
			pkx, err = b.keepBroadcast(cl, pkx)
		}
	}

	// **The one place every publish passes, accepted or refused**, which is
	// why both counters are here rather than at the several sites that
	// answer a reason code. A counter at each of those is a counter the
	// next refusal forgets to add.
	//
	// The channel is resolved from the topic rather than carried back,
	// because Resolve is the same answer routePublish just acted on and
	// asking again costs a map lookup on a path that has already done a
	// store write.
	//
	// **A queue delivery is not a publish**, and counting it as one was
	// measured rather than reasoned: one job published to a queue with five
	// attempts reported six records accepted, because every hand-over to a
	// worker re-enters this path on its way out. The test is the same one
	// routePublish makes - saguin's own inline client, which is the queue
	// delivery and nothing else. A bridge and a Will are different clients
	// and are real records arriving, so both still count.
	switch {
	case cl == b.inline:
	case err == nil || errors.Is(err, packets.CodeSuccessIgnore):
		// A topic no channel claims is broadcast, and it has a tally of its
		// own: forChannel answers nil for it on purpose, so counting only
		// through the map dropped every broadcast publish from the total the
		// catalogue promises one for.
		if c := b.counted.forChannel(channelName(b.reg.Resolve(pk.TopicName))); c != nil {
			c.published.Add(1)
		} else {
			b.counted.broadcast.Add(1)
			// **An outbound bridge rule sees a broadcast record here or
			// nowhere**, because nothing stores one: there is no position
			// to read it back from later, so it is handed over as it
			// passes. Under the same branch that counts it, which is the
			// one place a publish is known to be broadcast and to have been
			// accepted - a refused publish returns above and is offered to
			// nobody.
			//
			// The mark rides on the record rather than in the packet's
			// properties, for the reason the stored path keeps a field: a
			// property is a claim anything on the wire can make, and who
			// published this is a fact the broker holds.
			b.broadcastWatched(&pk, bridgeOf(b, cl), b.publisherOf(cl))
		}
	default:
		b.counted.refuse(refusalReason(err))
	}

	// **A refusal a 3.1.1 client cannot be told about closes the connection**
	// (RFC 0001 "Both protocols", RFC 0002 "Publishing"). It is here rather
	// than at the sites that answer a reason code for the same reason the
	// counters above are: one place every publish passes, so the next
	// refusal added cannot forget it.
	//
	// **Without it, the refusal is not a refusal.** The substrate turns a
	// reason code into a PUBACK-with-code inside a branch reading
	// `ProtocolVersion == 5`; below that the code matches nothing and the
	// packet carries on down the same function - retained if it asked to
	// be, delivered to every broadcast subscriber, and answered PUBACK
	// success. Refused and published at the same time, which is the worst
	// of both. `ErrRejectPacket` is what stops that: the substrate discards
	// the packet and writes nothing back.
	//
	// **`cl.Stop` rather than `DisconnectClient`**, because 3.1.1 has no
	// server-to-client DISCONNECT. The substrate would encode one anyway -
	// a bare fixed header, since the reason code is MQTT 5's - and send a
	// 3.1.1 client a packet its own specification does not define. A TCP
	// close says less and is the truthful version of it.
	//
	// Only a socket client can reach this. saguin's three in-process
	// publishers - the queue delivery, the Will, and a bridge - are each
	// stamped version 5 where they are made, so none can be stopped here.
	if err != nil && !errors.Is(err, packets.CodeSuccessIgnore) &&
		legacyClient(cl) && pk.FixedHeader.Qos > 0 {
		var code packets.Code
		if errors.As(err, &code) {
			// The reason is bounded rather than exempted. No reason string
			// interpolates anything today - they are all constants at their
			// sites - but that is a fact about every current caller rather
			// than about the value, and an exemption argued from call sites
			// rots the moment somebody adds one.
			//
			// The protocol version is not logged at all: the gate admits
			// 3.1.1 and MQTT 5 and nothing else, so on this line it is
			// always 4 and the message already says so.
			b.log.Warn("disconnecting a legacy publisher: its PUBACK cannot carry a refusal",
				"client", b.limits.Loggable(cl.ID),
				"topic", b.limits.Loggable(pk.TopicName),
				"code", code.Code, "reason", b.limits.Loggable(code.Reason))
			b.disconnect(cl, code)
			return pkx, packets.ErrRejectPacket
		}
	}

	// CodeSuccessIgnore is how a record that *was* stored reports itself, so
	// it is success and is not an outcome to carry back. Everything else
	// that is not nil is a refusal, and the server is about to discard it.
	if err != nil && !errors.Is(err, packets.CodeSuccessIgnore) {
		b.bridgeMu.RLock()
		bc := b.bridges[cl]
		b.bridgeMu.RUnlock()
		if bc != nil {
			bc.outcome = err
		}
	}
	// A Will's outcome, under willMu, which publishWill holds for the
	// injection this runs inside. Nil only where the publish path took it.
	if cl != nil && cl == b.willClient {
		if err == nil || errors.Is(err, packets.CodeSuccessIgnore) {
			b.willOutcome = nil
		} else {
			b.willOutcome = err
		}
	}
	return pkx, err
}

// keepBroadcast writes an accepted broadcast publish at QoS 1 or 2 to the
// broadcast log once, where a session that outlives its connection wants it,
// and marks the packet with where (LogOffset) for the selection to count it
// for the sessions it chooses (OnSelectSubscribers). The publisher is
// answered after this one write and after nothing any session does (RFC 0003
// "Broadcast", invariant 16).
//
// **A write that fails refuses the publish** - 0x83, RFC 0002's code for
// storage that could not keep a record, or the close a 3.1.1
// client gets in OnPublish - because a publisher told its message was
// accepted is told it is kept (invariant 18). A full log with nothing it can
// give up is not that case: the publish is accepted, and the sessions it was
// for lose it, counted (RFC 0002's full-store table).
func (b *Broker) keepBroadcast(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	d := b.broadcastDrain()
	if d == nil || strings.HasPrefix(pk.TopicName, "$") || b.reg.Resolve(pk.TopicName) != nil || !d.wanted(pk) {
		return pk, nil
	}
	off, err := d.published(b.record(pk, nil, bridgeOf(b, cl), b.publisherOf(cl)))
	if err != nil {
		b.log.Error("cannot keep a broadcast in the log; the publish is refused",
			"client", b.limits.Loggable(cl.ID), "topic", b.limits.Loggable(pk.TopicName), "error", err)
		return pk, packets.ErrImplementationSpecificError
	}
	pk.LogOffset = off
	return pk, nil
}

// fanOut decides whether the substrate delivers a publish saguin has
// already stored, and is the one place that decision is made.
//
// **`CodeSuccessIgnore` means "stored, do not fan out"**, and every store
// path returns it - the plain ones, the two a bridge's copy takes, and a
// `latest` deletion. It sets `pk.Ignore`, which stops the substrate
// matching subscribers at all.
//
// That was right while no subscription on a channel could be served by the
// substrate. It stopped being right when `$share` became an ordinary
// subscription on every channel type: a shared group over an `append` or
// `latest` channel is delivered by the substrate's own shared-subscription
// machinery, from `publishToSubscribers`, and a packet marked Ignore never
// reaches it. So the mark is lifted for those two types and kept for a
// queue, whose records only ever leave through the queue's own path.
//
// **One place rather than eight.** The alternative was editing each store
// path's success return, and the failure that invites is the next one added
// - a store path that forgets is a channel type whose shared subscribers
// silently receive nothing, which reads from the client as a group that was
// granted and never fed.
//
// What it costs on the ordinary path is the subscriber lookup the substrate
// makes for every broadcast publish, plus this hook clearing the set. What
// it buys back is that the retained store behaves the same way it does for
// broadcast: a `latest` publish carrying RETAIN is put into the substrate's
// store and taken straight out again by OnRetainMessage, which is what
// already happened to every retained broadcast message.
func (b *Broker) fanOut(cl *mqtt.Client, pk packets.Packet, err error) (packets.Packet, error) {
	// **The strip is unconditional, and it is above both returns because it
	// used to be below them.** A broadcast publish is not ignored - the
	// substrate fans it out - so it left by the first return with whatever
	// the publisher wrote still on it, and a subscriber received
	// `saguin-offset 999` from a client that made it up. A wide filter
	// spans channel and broadcast space, and nothing in a delivery says
	// which half it came from, so a forged stamp is indistinguishable from
	// one saguin wrote.
	//
	// **Whose packet it is, rather than which topic it is on.** Every
	// publish comes through here and not all of them are a client's: a
	// queue's offer and a Will landing in a channel are built by saguin,
	// carry the stamps saguin has just written, and pass this way too - so
	// a strip that asked only about the topic took the broker's own
	// `saguin-attempt` and `saguin-id` off the delivery it was making, and
	// ten tests said so within a minute. The inline client is saguin
	// publishing to itself, and it is the one exemption.
	//
	// **A bridge is not saguin publishing to itself, though it publishes
	// through an inline client too.** What a bridge carries was written by
	// another broker - its offsets, its receipt time, its Message ID - and on
	// a broadcast topic here none of them means anything: a broadcast has no
	// position and no record identity, and giving it one on the way out would
	// make broadcast look like a channel. The exemption is for who wrote the
	// properties, not for how the packet arrived, so a bridge's publish is
	// stripped as any publisher's is. A local subscriber was handed a peer's
	// `saguin-offset` before this (TestABroadcastArrivingOverABridgeCarriesNoPeersStamps).
	//
	// **On every path out, because the packet leaves by more than one.**
	// The properties that survive here reach the subscriber *and* the
	// acknowledgement the publisher gets back, which the substrate builds
	// from this same packet. A refused publish returns below without
	// reaching a subscriber at all and still answered a PUBACK carrying
	// the client's `saguin-offset` (the same class as the reserved prefix on a PUBACK).
	if !cl.Net.Inline {
		pk = stripReserved(pk)
	} else if _, bridged := b.bridgeName(cl); bridged {
		pk = stripReserved(pk)
	}

	c := b.reg.Resolve(pk.TopicName)
	if !errors.Is(err, packets.CodeSuccessIgnore) {
		return pk, err
	}
	if c == nil || c.Type == channel.Queue {
		return pk, err
	}
	// **A publish held for its release is not a record yet**, so its shared
	// subscribers are served when releaseHeld writes it and not now. Fanned
	// out here, a member was handed a message that an abandoned exchange
	// never stored.
	if heldForRelease(cl, pk) {
		return pk, err
	}

	// **The RETAIN flag comes off, and that is not cosmetic.** The substrate
	// retains before it fans out, and a channel's record must never enter
	// the substrate's own retained store: it would sit there until
	// OnRetainMessage took it back out, and a `#` subscriber arriving inside
	// that window is served a channel's record by a store that knows nothing
	// about channels - unstamped, and beside the copy saguin is already
	// sending it (invariant 11). `pk.Ignore` used to make that impossible
	// and no longer does, so the flag does.
	//
	// What it costs a shared subscriber is the flag on a `latest` change,
	// which would otherwise reach one that asked for Retain As Published.
	// That is the honest answer: nothing retained it, and a shared
	// subscriber is served changes rather than state.
	pk.FixedHeader.Retain = false
	return stripReserved(pk), nil
}

// reservedPrefix is the prefix on every user property saguin writes onto a
// delivery, and the one a publisher may not forge.
const reservedPrefix = "saguin-"

// stripReserved removes the reserved `saguin-` user properties from a
// packet the substrate is about to fan out from a channel.
//
// **A shared subscriber is told no offsets, so it must not be handed a
// publisher's claim about one.** record() strips the prefix on its way into
// storage precisely so that a publisher cannot forge broker metadata a
// consumer would read as the broker's; a fan-out of the packet as received
// would put every one of those on the wire, and a `saguin-offset` a
// producer invented is worse than none - a consumer has no way to tell it
// from one saguin stamped.
//
// The scan is separate from the copy so that the ordinary publish, which
// carries no such property, costs one pass over a short slice and no
// allocation at all.
func stripReserved(pk packets.Packet) packets.Packet {
	found := false
	for _, u := range pk.Properties.User {
		if strings.HasPrefix(u.Key, reservedPrefix) {
			found = true
			break
		}
	}
	if !found {
		return pk
	}
	kept := make([]packets.UserProperty, 0, len(pk.Properties.User))
	for _, u := range pk.Properties.User {
		if !strings.HasPrefix(u.Key, reservedPrefix) {
			kept = append(kept, u)
		}
	}
	pk.Properties.User = kept
	return pk
}

// routePublish is the routing itself. A topic no channel claims is
// untouched and flows on as ordinary broadcast.
func (b *Broker) routePublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// A saguin-injected queue delivery re-enters here on its way to a
	// worker. It is already stored; let it through to the subscribers.
	//
	// This asks for that one client rather than for any in-process client,
	// because a bridge is one of those too and its records are the opposite
	// case: nothing has stored them yet, so they are routed like anybody
	// else's. Asking `cl.Net.Inline` here would send every bridged record
	// straight past channel resolution to the subscribers, storing nothing.
	if cl == b.inline {
		return pk, nil
	}

	// **Not for a Will**, which RFC 0003 "What is refused, and when" asks
	// one question of when it fires - may its client still publish there -
	// and nothing else. Every Will is published as the one Will client, so
	// a budget keyed by client id made every client's Will share one: at
	// publish_rate 2, six clients dying together had two Wills delivered
	// and four refused and lost, each answered 0x00 at CONNECT. Charging each Will to its own client instead
	// would still refuse a Will, which is what the RFC rules out; mosquitto
	// has no publish rate, and EMQX's limiter is on a connection's own
	// packets, which a Will is not.
	//
	// **The rate a client is allowed, before the work of routing it.**
	// Checked here rather than in checkBounds so that a refused publish
	// costs a token-bucket update and nothing else - no channel resolution,
	// no ACL lookup, no store write. A limiter that did the work and then
	// refused would be a slower broker under exactly the load it exists to
	// survive.
	//
	// A bridge's own publishes reach this too and are bounded with
	// everybody else's, which is right: an upstream flooding a bridge is
	// the same pressure on the same box.
	//
	// `0x97 Quota exceeded` is the word saguin already uses for a channel
	// at its bound, and it means the same thing here - come back later. The
	// connection is left alone deliberately: disconnecting turns a burst
	// into a reconnect storm, and a client that ignores the code is already
	// bounded, since nothing is stored and nothing accumulates. What
	// invariant 13 protects is storage and memory, and both hold.
	// **The packet as the client sent it**, which is what the wire length
	// already says: topic, packet identifier, properties and payload. saguin
	// adds its own `saguin-*` properties on delivery and never to what
	// arrives, so there is nothing to subtract here.
	if cl != b.willClient {
		if within, rate, byteRate := b.withinPublishRate(
			identity(cl), cl.ID, int64(pk.FixedHeader.Remaining)); !within {
			return pk, b.refuseRate(cl, pk, rate, byteRate)
		}
	}

	// Is this a topic anything may publish to at all? The substrate asks
	// the same question of a client on a socket, and saguin has to ask it
	// again because that check exempts an in-process publisher - and saguin
	// has three of them: a queue delivery, a Will, and an inbound bridge.
	// Invariant 10 says the broker is the only enforcement point, and an
	// exemption keyed on how the publish arrived is not one.
	//
	// **The Will was reachable by anyone who could open a socket**, and is
	// what this was written for: one naming `$SYS/broker/version` forged
	// that topic into a subscriber's stream, and one naming `events/+` was
	// stored in a channel where it stopped every conforming consumer for
	// ever.
	//
	// The bridge is a different case and the difference is worth writing
	// down, because the first version of this comment had it wrong. Calling
	// `BridgeClient.Publish` with such a topic does reach here, but **no
	// configuration can make it happen**: an inbound rule that would produce
	// a topic beginning with `$` is dropped by the bridge's own rule engine
	// (`droppedReserved`), a rule whose template names one literally is
	// refused when the file loads, and so is a `both` rule on a `$` filter,
	// which maps a topic to itself (it once loaded, and dropped every
	// record). So this check is defence behind a check
	// that already holds rather than the thing standing between an upstream
	// broker and a forged tree - kept for the reason RFC 0002 keeps the
	// channel-overlap check no configuration can now reach: a check whose
	// necessity rests on a second rule staying exactly as it is is not a
	// check to remove.
	//
	// It asks the substrate's own function rather than listing the rules,
	// so a rule added there is inherited instead of missed. Today that is a
	// wildcard in a topic name (MQTT-3.3.2-2) and the reserved `$SYS` tree;
	// what it is tomorrow is not saguin's to keep in step.
	if notATopicName(pk.TopicName) != "" {
		return pk, b.refuseTopicName(pk)
	}

	if code := b.checkBounds(pk); code != nil {
		return pk, code
	}

	// **The control verbs are asked here and nowhere else.** OnACLCheck is
	// handed a topic and a direction, which cannot name any of these: a
	// point read carries its key in the payload, and a seek is a publish to
	// a topic that resolves to no channel by prefix. Here the channel and
	// the payload are both in hand, and each verb is the one RFC 0002 gives
	// its channel type.
	//
	// **Structural first, permission after**, the same order as OnACLCheck:
	// cannotAnswer refuses a control topic naming the wrong kind of channel
	// whatever any rule says.
	if strings.HasPrefix(pk.TopicName, channel.ReservedRoot+"/") {
		if name, ok := channel.SeekChannel(pk.TopicName); ok {
			if why := b.cannotAnswer(name, channel.Append, "holds a position to move"); why != "" {
				return pk, b.refuseReserved(pk, why)
			}
			if c := b.reg.Get(name); c != nil && !b.permitsVerb(cl, c, pk.TopicName, verbSeek) {
				return pk, b.refuseUnauthorized(cl, verbSeek, c)
			}
			return pk, b.handleSeek(cl, pk, name)
		}
		if name, ok := channel.ResponseChannel(pk.TopicName); ok {
			if why := b.cannotAnswer(name, channel.Queue, "has deliveries to answer for"); why != "" {
				return pk, b.refuseReserved(pk, why)
			}
			// **Answering is part of consuming, not a verb of its own.**
			// Separate them and a worker can be granted `consume` and no way
			// to acknowledge: every job it takes times out, is retried,
			// exhausts its attempts and is dead-lettered. Work destroyed by
			// a configuration that reads as reasonable.
			if c := b.reg.Get(name); c != nil && !b.permitsVerb(cl, c, pk.TopicName, verbConsume) {
				return pk, b.refuseUnauthorized(cl, verbConsume, c)
			}
			b.handleResponse(cl, pk)
			return pk, packets.CodeSuccessIgnore
		}
		if channel.IsKVGet(pk.TopicName) {
			// The key is the payload, which is the whole reason this cannot
			// be answered from OnACLCheck. handleKVGet refuses a key that
			// names no latest channel, so an unresolved one falls through
			// to it rather than being answered here.
			//
			// **The key as sent, as handleKVGet reads it.** A topic with a
			// trailing space is another topic, and trimmed here and not
			// there, the permission was asked of one channel and the value
			// read from another.
			if key := string(pk.Payload); key != "" {
				if c := b.reg.Resolve(key); c != nil && c.Type == channel.Latest &&
					!b.permitsVerb(cl, c, key, verbRead) {
					return pk, b.refuseUnauthorized(cl, verbRead, c)
				}
			}
			return pk, b.handleKVGet(cl, pk)
		}
		if channel.IsDisconnect(pk.TopicName) {
			// **Permission first, and it is not a channel's.** Hanging a
			// client up is an act on no channel, so there is no channel
			// verb to ride on: RFC 0002's `broker: sessions` rule kind is
			// what grants it, and nothing written as a filter can.
			if !b.permitsBrokerVerb(cl, facilitySessions, verbDisconnect) {
				return pk, b.refuseBrokerVerb(cl, verbDisconnect, facilitySessions)
			}
			return pk, b.handleDisconnectRequest(cl, pk)
		}
		if name, ok := channel.CatalogueChannel(pk.TopicName); ok {
			// **No permission check out here, unlike the three above**, and
			// that is the point of this verb rather than an omission: a
			// channel this client may not use and a channel that does not
			// exist must reach the same answer, or asking becomes a way to
			// discover what is there. Both are the empty reply inside.
			return pk, b.handleCatalogue(cl, pk, name)
		}
		return pk, b.refuseReserved(pk, "this is not a topic saguin defines")
	}

	c := b.reg.Resolve(pk.TopicName)
	if c == nil {
		// The retain flag asks for a store, and on a broadcast topic there
		// is one only where the operator configured one (RFC 0003
		// "Retained messages"). With none, refusing is better than dropping
		// the flag and acknowledging, which tells a producer its state was
		// kept when nothing kept it. A channel *is* that store - better,
		// since it is configured, bounded and durable - so the flag is
		// redundant there rather than impossible, and a `latest` channel
		// receiving one is a client saying "this is state", which is what
		// that channel type is for.
		//
		// Refusing it above channel resolution was wrong and broke the one
		// migration RFC 0003 recommends: a homeassistant/ or zigbee2mqtt/
		// tree pointed at a `latest` channel publishes with the flag set,
		// and was disconnected on its first record.
		//
		// Only a client on the wire is refused. An in-process publisher -
		// the inline client injecting a queue delivery, or a bridge
		// carrying somebody else's retained message - is saguin's own and
		// is not something to disconnect.
		if pk.FixedHeader.Retain {
			// **A client the acl_file denies `retained` is answered as a broker
			// with no store would answer it**, here and nowhere else: on a
			// channel's topic the channel is the store and the flag keeps
			// nothing extra. Never an in-process publisher: a Will is published
			// on its client's behalf by saguin's own Will client, which carries
			// no user name, so a `"*"` entry would otherwise refuse a retained
			// Will its own client was allowed at CONNECT.
			denied := !cl.Net.Inline && b.deniesFeature(cl, featureRetained)
			if b.retained == nil || denied {
				if cl.Net.Inline {
					return pk, nil
				}
				why := "this broker has no retained store attached, so a broadcast topic has nowhere to keep one"
				if denied {
					why = "this client's roles deny it retained values on broadcast topics"
				}
				return pk, b.refuseRetain(cl, pk, why)
			}
			// A broadcast held for its release is retained at the release,
			// with its message (releaseHeld): retained now, a subscriber
			// arriving before the PUBREL would be served a value no
			// delivery has carried, from an exchange that may never finish.
			if !b.holdsBroadcast(cl, pk) {
				if code := b.keepRetained(cl, pk); code != nil {
					return pk, code
				}
			}
			// The flag stays set, and two things then happen to it. The
			// substrate puts a copy in its own retained store, which
			// OnRetainMessage deletes on the spot; and the fan-out hands the
			// message to each subscriber with the flag as MQTT-3.3.1-12 and
			// -13 require - down for an ordinary one, up for one that asked
			// for Retain As Published.
			//
			// Clearing it here was simpler and got the second of those
			// wrong: every Retain As Published subscriber saw 0.
		}
		return pk, nil // broadcast
	}

	// A dead-letter channel takes records from its queue and from nothing
	// else. RFC 0002 already writes that rule and its reason, against a
	// bridge rule naming one - and a rule enforced against a configuration
	// file and not against a client is the failure invariant 10 exists to
	// describe: `mosquitto_pub` reaches the same broker.
	//
	// What it costs is provenance rather than capacity. An injected record
	// carries no `saguin-dlq-*` at all, because the reserved prefix is
	// stripped from it like any other publish - so it is indistinguishable
	// on the wire from a genuine dead-letter whose metadata was lost, and
	// the link between a failure and the work that failed names nothing. A
	// redrive, which reads `saguin-dlq-channel` and `saguin-dlq-offset` to
	// know where to put the work back, would have neither.
	//
	// Reading one stays open: consumers subscribe to `<name>__dlq/#`, replay
	// it and hold positions, and a seek on one is accepted, which is what
	// RFC 0002 means by "an ordinary append channel in every other respect"
	// - every item in that sentence's own list is a read.
	if strings.HasSuffix(c.Name, channel.DLQSuffix) {
		return pk, b.refuseDerived(c, pk)
	}

	// **A queue cannot keep a current value, and nothing on it could ever
	// read one.** The flag asks for the message to be held as the topic's
	// state and handed to whoever subscribes next; a queue holds work,
	// gives it to one worker, and is done with it. Worse, the only
	// subscription a queue channel grants is the shared one - an ordinary
	// filter on it is refused `0x8F` - and MQTT never delivers a retained
	// message to a shared subscription. So there is no client that could
	// be served such a value even if one were kept.
	//
	// **This was a `0x9A` disconnect and is now a dropped flag**, which is
	// the argument reversing rather than the rule bending. The case for
	// refusing was that accepting and ignoring tells a producer its state
	// was kept when nothing kept it. But a `PUBACK` on a queue never said
	// anything about state to begin with: it says the job was stored, and
	// it is true. The flag is the only part that cannot be honoured, and
	// dropping a flag a broker cannot honour is what MQTT does everywhere
	// else - saguin does it for Retain As Published, for a subscription's
	// QoS, for a session expiry above the cap. What refusing actually cost
	// was the connection of any producer whose library sets the flag by
	// default, on every reconnect, for work the broker was perfectly able
	// to take.
	//
	// The producer is not left with nothing to read, either: it is the
	// operator whose configuration made this topic a queue, and
	// saguin_queue_retain_ignored_total is where they see it.
	//
	// An in-process publisher takes the same path, which is a change from
	// when this refused: there is one answer to the flag on a queue now,
	// so there is nothing for saguin's own publishers to be exempt from.
	if pk.FixedHeader.Retain && c.Type == channel.Queue {
		// **The flag is dropped and the job is taken.** One flag, one
		// answer: everywhere else in MQTT a retain flag a broker cannot
		// honour is a flag it ignores, and a queue is the last place saguin
		// answered a different way. The producer's message is work either
		// way - the flag says something about a *store* and asks nothing of
		// the payload - so refusing it cost the connection of a device
		// whose only mistake was a library default, and cost it on every
		// reconnect, while the work it was sending was perfectly good.
		//
		// Dropped rather than kept: nothing on a queue could ever be served
		// a retained message, since a queue grants one subscription form
		// and MQTT sends no retained message to a shared one. Leaving the
		// flag up would put a copy in the substrate's own retained store on
		// the way past - reachable through a wildcard at channel depth,
		// which is invariant 11.
		//
		// Counted, because this is the only place it is visible: a producer
		// that believes it is setting state on a topic a queue claims has
		// made a configuration mistake nobody is answering for it, and
		// saguin_queue_retain_ignored_total is where an operator sees it.
		pk.FixedHeader.Retain = false
		if cc := b.counted.forChannel(c.Name); cc != nil {
			cc.retainIgnored.Add(1)
		}
	}

	// **The mark is taken from who published, never from the packet.** A
	// bridge is the one publisher whose records must not go back out over a
	// bridge, and this is the only place that knows which client sent this
	// one. Reading it off a user property instead would make it a claim
	// anybody on the wire could make; record() strips the reserved prefix
	// precisely so that no such claim survives.
	from, _ := b.bridgeName(cl)
	rec := b.record(pk, c, from, b.publisherOf(cl))

	// **Exactly-once stops here and is written when its release arrives.**
	// Everything above has run - the rate, the topic, the bounds, the
	// permission, the channel - because the specification requires a
	// receiver to make every check that could produce a forwarding failure
	// *before* it accepts ownership, and ownership transfers at the PUBREC
	// that this return produces (section 4.3.3, note 1). What is deferred
	// is the write and nothing else.
	//
	// Storing here instead is what saguin used to do, and it is the whole
	// defect: a restart between the store and the client's PUBCOMP leaves
	// the record kept and the publisher believing the publish failed, so
	// the application sends it again and the channel holds it twice, with
	// nothing on the wire saying so.
	//
	// **A broadcast topic never reaches this**, because the code above has
	// already returned for a publish that resolves to no channel. Its hold
	// is OnPublish's (holdBroadcast), in the broadcast log's store.
	if heldForRelease(cl, pk) {
		if code := b.holdForRelease(cl, b.channelHold(c), pk, rec); code != nil {
			return pk, code
		}
		return pk, packets.CodeSuccessIgnore
	}

	return b.storeAndDeliver(c, pk, rec)
}

// heldForRelease reports whether a publish waits for its PUBREL before its
// record is written.
//
// **A publisher on a socket, at QoS 2, and nothing else.** saguin's own
// in-process publishers - a Will, a bridge - send no PUBREL, so a QoS 2
// publish of theirs held here was never released. Measured: two exactly-once
// Wills fired into an append channel and neither was stored, and both were
// held under the one identifier the Will client uses. Such a publish is
// injected once, so storing it at once is exactly once.
//
// fanOut asks the same question, so a publish that is held is not fanned out
// at its PUBLISH either.
func heldForRelease(cl *mqtt.Client, pk packets.Packet) bool {
	return pk.FixedHeader.Qos == maxQoSWithStore && !cl.Net.Inline
}

// announceStored hands a record just written to a channel to whoever is
// reading it: the publish path's after its write, and a release's after its
// swap. A latest channel's record without an offset is a deletion that
// found no value, delivered as the publish path delivers one.
func (b *Broker) announceStored(c *channel.Channel, stored store.Record) {
	switch c.Type {
	case channel.Append:
		if hook := storedBeforeAnnounced.Load(); hook != nil {
			(*hook)(c.Name, stored.Offset)
		}
		// saguin delivers append records itself rather than letting the
		// server fan out, so that a wildcard at channel depth cannot
		// reach them (invariant 11). The in-flight accounting QoS 1 needs
		// is therefore saguin's job too, which is what pump does.
		b.pumpAll(c, stored)
	case channel.Latest:
		if stored.Offset != 0 {
			if hook := latestAfterSet.Load(); hook != nil {
				(*hook)(stored.Topic, stored.Offset)
			}
		}
		b.publishLatest(c, stored)
	}
}

// storeAndDeliver writes a resolved record to its channel and hands it to
// whoever is reading. It is reached twice: directly for QoS 0 and QoS 1,
// and from the PUBREL for an exactly-once publish that was held.
//
// **It does not take the publisher**, and that is the shape rather than an
// omission: where a record goes is decided by its topic and nothing else,
// so who sent it cannot change the answer. Authorization has already been
// asked, higher up and once.
func (b *Broker) storeAndDeliver(c *channel.Channel, pk packets.Packet,
	rec store.Record) (packets.Packet, error) {
	switch c.Type {
	case channel.Append:
		b.mu.Lock()
		lg := b.logs[c.Name]
		b.mu.Unlock()

		var stored store.Record
		err := b.withRoom(c.Storage, store.RecordSize(rec), "channel "+c.Name, func() (err error) {
			stored, err = lg.Append(rec)
			return err
		})
		if err != nil {
			return pk, b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
		}
		b.announceStored(c, stored)
		return pk, packets.CodeSuccessIgnore

	case channel.Latest:
		b.mu.Lock()
		lt := b.latest[c.Name]
		b.mu.Unlock()

		if len(rec.Payload) == 0 {
			return pk, b.deleteLatest(c, lt, pk, rec)
		}

		var stored store.Record
		err := b.withRoom(c.Storage, store.RecordSize(rec), "channel "+c.Name, func() (err error) {
			stored, err = lt.Set(rec)
			return err
		})
		if err != nil {
			return pk, b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
		}
		b.announceStored(c, stored)
		return pk, packets.CodeSuccessIgnore

	case channel.Queue:
		b.mu.Lock()
		q := b.queues[c.Name]
		b.mu.Unlock()
		if err := b.withRoom(c.Storage, store.RecordSize(rec), "channel "+c.Name, func() error {
			_, err := q.Enqueue(rec)
			return err
		}); err != nil {
			return pk, b.refuseStore(c.Name, c.Storage, c.MaxBytes, pk, err)
		}
		return pk, packets.CodeSuccessIgnore

	default:
		return pk, nil
	}
}

// OnSelectSubscribers chooses which worker receives an injected queue
// delivery.
//
// Clearing the ordinary subscriptions first keeps a channel's records
// unreachable through a wildcard at channel depth (invariant 11).
//
// The worker is then chosen from saguin's own registry of live queue
// consumers rather than left to the server's shared-subscription
// selection. The two disagree: saguin disconnects a client whose
// subscription it refuses, and a session can end at any moment, but the
// server's topic index may still list it. Selecting from that index hands
// the record to a session that is gone - and because a record in
// Delivering has no visibility deadline by design (invariant 7), it is stranded
// there permanently, with no worker holding it and no timer to notice.
// Observed live: one job disappeared for good after an unrelated client
// was disconnected for subscribing at QoS 0.
func (b *Broker) OnSelectSubscribers(subs *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	c := b.reg.Resolve(pk.TopicName)
	if c == nil {
		// **Broadcast that reached nobody**, which nothing else in the
		// catalogue can answer: a publish to `event/x` where the channel is
		// `events` is ordinary broadcast to no subscriber, acknowledged
		// `0x00`, and MQTT has no reason code for it. A mistyped channel name
		// looks exactly like this and nothing else reports it.
		//
		// It is countable here and nowhere else, and only since
		// mochi-mqtt/server#523: the substrate collects the subscribers for
		// every publish, and before that patch it called this hook only when
		// the set held a shared subscription, so an ordinary broadcast never
		// reached a hook at all. The set is already computed, so this is one
		// atomic add on a publish that delivered nothing.
		//
		// **A topic beginning with `$` is never broadcast.** That space is
		// the server's, not a client's: MQTT 4.7.2 keeps it out of wildcard
		// matches, saguin refuses a client publish into it, and everything
		// that arrives here from it was published by the broker itself.
		// Counting them read 20 unmatched broadcasts on a broker nothing had
		// connected to, because the substrate publishes its whole $SYS tree
		// once at startup before saguin clears it - a number an operator
		// would have taken for twenty mistyped channel names.
		if strings.HasPrefix(pk.TopicName, "$") {
			return subs
		}
		// **A worker's pin used to need stripping here and no longer
		// does.** While it was `$share/saguin/<name>/#` it matched every
		// broadcast topic under the channel's name, and the substrate would
		// hand one to a worker as a stray message - no Delivery ID, no
		// attempt, indistinguishable at the worker from a job. The pin is
		// `$saguin/queue/<name>` now, which matches itself and nothing
		// else, and the early return above already covers that: a topic
		// beginning `$` is never broadcast.
		// **The partition predicate, and this branch is the whole of where
		// it belongs in this hook.** It is applied to broadcast subscribers
		// here, above the queue's own branch below, so the two jobs this
		// hook now does cannot cross. If a declaration ever reached the
		// queue path it would withhold a record from the very worker
		// selection chose to hold it - and that record has already been
		// marked as being delivered, so it would sit with no holder and no
		// lease clock (invariant 7). Partitioning is refused on
		// `$saguin/queue/<name>` at SUBSCRIBE, so no worker can carry one;
		// this is the second line, and it is the branch boundary rather
		// than a check somebody has to remember.
		b.slice(subs, pk.TopicName)
		if len(subs.Subscriptions) == 0 && len(subs.Shared) == 0 &&
			len(subs.InlineSubscriptions) == 0 {
			b.counted.broadcastUnmatched.Add(1)
		}
		// **The sessions that outlive their connections are served from the
		// log**, the message counted for exactly the ones this selection
		// chose, and the fan-out writes nothing to them. Before the shared
		// groups are chosen, which stay the fan-out's.
		if d := b.broadcastDrain(); d != nil && pk.LogOffset != 0 {
			ids, groups := d.takeRecipients(subs, pk)
			d.chosen(pk.LogOffset, ids, groups)
		}
		b.selectShared(subs, pk)
		return subs
	}
	// **The ordinary subscriptions go and the shared groups stay**, and
	// that pair is the whole of what `$share` means on a channel.
	//
	// Clearing the ordinary ones is invariant 11: a channel's records are
	// delivered by saguin, from a stored position, so a wildcard at channel
	// depth must not also be served them by the substrate's fan-out.
	//
	// Leaving the shared ones is what gives `$share` back to MQTT. The
	// substrate picks one member of each group and writes this one live
	// message to it - no offset, no replay, no position - which is what a
	// shared subscription does on every other broker. saguin tracks such a
	// subscription as nothing (see track), so there is no second delivery
	// and no position for the acknowledgement to move.
	subs.Subscriptions = map[string]packets.Subscription{}
	if c.Type != channel.Queue {
		// **A shared group whose members' sessions outlive their connections
		// is served a channel's record from the broadcast log**, as it is a
		// broadcast (bdrain.keepForGroups): the groups left are the ones
		// served live.
		if d := b.broadcastDrain(); d != nil {
			d.keepForGroups(subs, pk)
		}
		b.selectShared(subs, pk)
		return subs
	}

	// **Every candidate comes from saguin's own index, not from the set the
	// server matched.** The pin names the channel, so it matches none of the
	// queue's topics and the server's set for a queue delivery is empty -
	// there is nothing here to filter down. saguin records a worker against
	// its queue when the subscription is granted, and that record is what
	// says who may be offered this job.
	//
	// It is also the index that was already being trusted over the server's:
	// saguin disconnects a client whose subscription it refuses, and a
	// session can end at any moment, while the server's topic index may
	// still list it. Selecting from that index hands the record to a session
	// that is gone, and a record in Delivering has no visibility deadline by
	// design (invariant 7), so it is stranded there permanently.
	type cand struct {
		id  string
		cl  *mqtt.Client
		sub packets.Subscription
	}
	var live []cand
	for _, id := range b.workersOf(c) {
		cl, ok := b.srv.Clients.Get(id)
		if !ok || cl.Closed() || cl.HangingUp() {
			continue
		}
		// **The subscription the worker actually sent**, read back from the
		// session rather than built here. It carries NoLocal, Retain As
		// Published and the Subscription Identifiers the delivery is
		// supposed to echo, and a constructed stand-in would quietly drop
		// whichever of them this code forgot.
		sub, ok := cl.State.Subscriptions.Get(c.QueueFilter())
		if !ok {
			continue
		}
		{
			// And it must have room for it. A worker whose in-flight window
			// is full has the delivery registered against it and then never
			// receives it: the server withholds the packet for want of send
			// quota and resends only when a session resumes, so the record is
			// held by a worker that has not got it, with no deadline to
			// rescue it (invariant 7). deliver offers no more than the room
			// these windows add up to, but the window is shared with anything
			// else being delivered to the same session, so it is checked
			// again here against the one worker that would receive it.
			b.mu.Lock()
			room := b.window(cl)
			busy := b.holds(id, c.Name)
			b.mu.Unlock()
			if room <= 0 {
				continue
			}
			// **And it must hold no job from this queue already.** A worker
			// takes one job from a queue at a time, from the offer until it
			// answers, its visibility timeout runs out, or it goes - whatever
			// its Receive Maximum (RFC 0003 "Delivery"). The job being offered
			// now is not counted: its worker is learned in OnQosPublish, after
			// this returns.
			if busy {
				continue
			}
			// **And the acl_file must still allow it**, asked here rather
			// than left to the substrate's own check on the way out.
			//
			// Both ask the same question; the difference is what happens to
			// the record when the answer is no. Refused there, the packet is
			// dropped after this hook has handed the worker over: the record
			// stays Delivering with no holder and no lease clock - invariant
			// 7's deadline starts at the PUBACK, which cannot come for a
			// packet that was never written - and the offer loop only feeds
			// what is waiting. So the job is stranded for ever, acknowledged
			// `0x00` to its publisher, never processed, never returned,
			// never dead-lettered, and in the queue view indistinguishable
			// from work in hand. Two workers make it a running loss: the
			// selection keeps handing the withdrawn one its share while the
			// healthy one sits idle.
			//
			// Refused here, it is one fewer candidate, and the release below
			// puts the record back with its attempt unspent when that leaves
			// nobody - which is what this function already does for a worker
			// that has gone or has no room.
			//
			// **Nothing re-offers it when the grant comes back, and nothing
			// needs to.** wakeStalled feeds append cursors and `latest`
			// values, which are the two things a stall holds; a queue record
			// is not held by anybody, it is `waiting`, and the queue's own
			// loop offers what is waiting every queuePeriod. A job
			// can arrive 0.2s after the signal: it is that tick, and the whole of
			// what makes it work is the record being in the right state.
			if !b.mayDeliver(cl, pk.TopicName) {
				continue
			}
			live = append(live, cand{id, cl, sub})
		}
	}

	subs.Shared = map[string]map[string]packets.Subscription{}
	subs.SharedSelected = map[string]packets.Subscription{}

	if len(live) == 0 {
		// Nobody can take it - gone, with no room left, or no longer allowed
		// to consume this queue. Put it back rather than leaving it in
		// Delivering, where nothing would ever look at it again.
		b.releaseUndelivered(pk, "no live worker with room and a grant")
		return subs
	}

	sort.Slice(live, func(i, j int) bool { return live[i].id < live[j].id })

	b.mu.Lock()
	n := b.rr[c.Name]
	b.rr[c.Name] = n + 1
	b.mu.Unlock()

	chosen := live[int(n%uint64(len(live)))]

	// A worker that declared a Maximum Packet Size cannot be sent anything
	// larger, and this is the one delivery path on which saguin cannot find
	// out that it was not: the delivery is injected, the server queues it on
	// the worker's outbound channel, and the write that fails happens later
	// in a goroutine that drops the error. Nothing comes back here.
	//
	// Left to happen, the record sits in Delivering holding a packet
	// identifier and a slot of that worker's send quota, with no PUBACK to
	// start a deadline from (invariant 7) and so nothing that will ever look
	// at it again: not redelivered, not dead-lettered, and no other worker
	// offered it. It burns no attempt either, so a worker reconnecting with
	// the same bound strands it again - a job with no exit.
	//
	// So it is caught before the delivery is handed over, and the answer is
	// the one the other two delivery paths give: disconnect. A worker that
	// cannot hold this queue's records cannot do the work it subscribed for,
	// and quietly routing around it would leave an operator with a worker
	// taking only the small jobs and nothing anywhere saying so. The record
	// goes back to available with its attempt count untouched, and the next
	// offer finds one fewer worker to choose from - the worker is hung up on
	// (hangUp), and chosen for nothing while it closes.
	if max := chosen.cl.Properties.Props.MaximumPacketSize; max > 0 {
		if size := deliverySize(pk); size+queuePacketMargin > int(max) {
			b.log.Warn("disconnecting: a job is larger than this worker's Maximum Packet Size",
				"client", chosen.id, "channel", c.Name, "topic", b.limits.Loggable(pk.TopicName),
				"size", size, "maximum_packet_size", max)
			b.hangUp(chosen.cl, packets.ErrPacketTooLarge)
			b.releaseUndelivered(pk, "job too large for the chosen worker")
			return subs
		}
	}

	// **Handed over as an ordinary subscriber, not as a shared selection.**
	// The server decides whether to merge SharedSelected from the set it
	// matched *before* this hook runs, and a queue delivery matches no
	// shared subscription any more - the pin names the channel. So filling
	// SharedSelected here would be writing into a map nothing goes on to
	// read, and the job would be acknowledged to its publisher and
	// delivered to nobody. Subscriptions is iterated unconditionally, and
	// it was emptied above, so this one entry is the whole delivery.
	subs.Subscriptions[chosen.id] = chosen.sub
	return subs
}

// shareCursor is one shared group's round-robin position, and when it was
// last used, so an idle one can be dropped.
type shareCursor struct {
	next uint64
	used time.Time
}

// shareCursorIdle is how long a group's cursor is kept without being used.
// Losing one costs the group nothing but where the rotation starts.
const shareCursorIdle = time.Minute

// selectShared chooses the member of each shared group a publish goes to
// (RFC 0002 "Shared subscriptions").
//
// **A member is a session that can take the delivery now**: connected, with
// room in its outbound queue, and - for a delivery that needs a packet
// identifier - with room in the in-flight window its Receive Maximum gives
// it. The substrate's own choice was the first member a Go map yielded, with
// no liveness check: an offline member was picked as often as a live one, and
// its share of the group queued in a session nobody was reading. Measured on
// 40 publishes to two live members, an offline one and a connected one with a
// window of 1 that acknowledged nothing, three runs: the live two received 13
// to 21 between them, the offline session was handed 7 to 13, and the deaf
// member held 6 to 15.
//
// **Round-robin over the members sorted by client id**, with a cursor per
// group, as a queue already rotates its workers - so a group's split does not
// depend on map order or on which member subscribed first.
//
// **No member that can take it means the delivery is dropped and counted**,
// never handed to one that cannot. The substrate falls back to its own choice
// when this leaves nothing selected, so the groups are taken out of the set it
// would choose from.
//
// **A member too small for the record is passed over** (fitsShared).
func (b *Broker) selectShared(subs *mqtt.Subscribers, pk packets.Packet) {
	if len(subs.Shared) == 0 || b.srv == nil {
		return
	}
	selected := map[string]packets.Subscription{}
	for filter, members := range subs.Shared {
		ids := make([]string, 0, len(members))
		for id, sub := range members {
			if b.canTakeShared(id, sub, pk) && b.fitsShared(id, pk) {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			// **Dropped for a group nobody is owed anything by**: a group
			// with a member whose session outlives its connection has a
			// cursor on the broadcast log, and its QoS 1 and 2 deliveries
			// were taken out of this selection to wait there
			// (bdrain.takeRecipients). What reaches here is QoS 0, a group
			// of clean-start members, or a channel's live record.
			b.counted.sharedNoMember.Add(1)
			b.log.Debug("dropped a delivery: no member of the shared group can take it",
				"group", b.limits.Loggable(filter), "topic", b.limits.Loggable(pk.TopicName),
				"members", len(members))
			continue
		}
		sort.Strings(ids)
		id := ids[b.nextShare(filter)%uint64(len(ids))]
		chosen, ok := selected[id]
		if !ok {
			chosen = members[id]
		}
		selected[id] = chosen.Merge(members[id])
	}
	subs.SharedSelected = selected
	subs.Shared = map[string]map[string]packets.Subscription{}
}

// canTakeShared reports whether a shared group's member can be handed this
// delivery now.
func (b *Broker) canTakeShared(id string, sub packets.Subscription, pk packets.Packet) bool {
	cl, ok := b.srv.Clients.Get(id)
	if !ok || cl.Closed() || cl.HangingUp() {
		return false
	}
	// A member whose socket's queue is full is passed over: a QoS 0 delivery
	// would be shed there, and one at QoS 1 or 2 would wait behind it.
	if !cl.OutboundHasRoom(pk) {
		return false
	}
	qos := pk.FixedHeader.Qos
	if sub.Qos < qos {
		qos = sub.Qos
	}
	if qos == 0 {
		return true
	}
	// The window as b.window measures it, read without mu: a Receive Maximum
	// and a length, both the session's own.
	max := int(cl.Properties.Props.ReceiveMaximum)
	if max <= 0 {
		max = 65535 // section 3.1.2.11.3: a Receive Maximum not given is 65,535
	}
	return cl.State.Inflight.Len() < max
}

// fitsShared reports whether a shared group's member can be handed this
// delivery for its size: under its Maximum Packet Size, measured as a queue
// offer measures it (deliverySize, queuePacketMargin).
//
// **A member the record does not fit is passed over** (RFC 0003 "When a
// record is too large for a subscriber"), for every group alike: it is not
// disconnected, and nothing is counted lost, since the group needs only one
// member to take it. Where no member it fits can take it now, the group does
// what it does when none is connected: holds it, or, with nobody coming back,
// drops it as no_shared_member.
func (b *Broker) fitsShared(id string, pk packets.Packet) bool {
	cl, ok := b.srv.Clients.Get(id)
	if !ok {
		return false
	}
	max := cl.Properties.Props.MaximumPacketSize
	return max == 0 || deliverySize(pk)+sharedFitMargin <= int(max)
}

// sharedFitMargin is the margin fitsShared measures with: a queue offer's.
// A variable only so that a test can make a member look as if it fits, and
// reach what follows a connection refusing a delivery that was measured as
// fitting (bdrain.refusedAfterFitting), which the margin makes unreachable.
var sharedFitMargin = queuePacketMargin

// nextShare is a group's next round-robin position.
func (b *Broker) nextShare(filter string) uint64 {
	b.shareMu.Lock()
	defer b.shareMu.Unlock()
	cur := b.shareCursors[filter]
	if cur == nil {
		cur = &shareCursor{}
		b.shareCursors[filter] = cur
	}
	n := cur.next
	cur.next++
	cur.used = time.Now()
	return n
}

// sweepShareCursors drops the cursors of groups nothing has selected with for
// shareCursorIdle.
func (b *Broker) sweepShareCursors(now time.Time) {
	b.shareMu.Lock()
	defer b.shareMu.Unlock()
	for filter, cur := range b.shareCursors {
		if now.Sub(cur.used) > shareCursorIdle {
			delete(b.shareCursors, filter)
		}
	}
}

// queuePacketMargin covers what the server adds to a delivery after saguin
// has measured it and before it reaches the wire: a Topic Alias for a worker
// that asked for one, a Subscription Identifier, and the two variable-length
// lengths that can each grow a byte when those appear. It is a bound rather
// than a tuned number - the encoder below measures the packet, and this is
// only the part saguin cannot see from here.
const queuePacketMargin = 32

// deliverySize is how many bytes this delivery is on the wire, as the
// server's own encoder measures it rather than as saguin estimates it.
//
// The two fields the encoder is sensitive to are pinned rather than trusted:
// properties are encoded only for protocol version 5, and a QoS 1 publish
// with no packet identifier does not encode at all. Every queue delivery is
// QoS 1 and every queue worker is MQTT 5 - queueRefusal turns a 3.1.1
// client away from a queue's subscription form (RFC 0002) - so both are
// known here. **The reason has changed twice and the line has not**, which
// is why it is written out: this used to say "saguin is MQTT 5 only", which
// stopped being true the day a 3.1.1 client could connect; then it rested
// on a 3.1.1 client being unable to form a shared subscription, which
// stopped being the pin's shape. What holds it now is an explicit refusal.
// Taking these from the injected packet instead would silently measure a
// delivery with none of its properties on it, which is most of its size.
//
// A packet that will not encode is reported as too large for anybody: it
// will not encode at the wire either, and the record is better left in the
// queue than handed to a worker that cannot receive it.
func deliverySize(pk packets.Packet) int {
	pk.ProtocolVersion = 5
	if pk.PacketID == 0 {
		pk.PacketID = 1
	}
	var buf bytes.Buffer
	if err := pk.PublishEncode(&buf); err != nil {
		return int(^uint(0) >> 1)
	}
	return buf.Len()
}

// workersOf is every client saguin has recorded as a consumer of this
// queue.
//
// **saguin's index and not the server's**, for the reason
// OnSelectSubscribers gives: a worker's pin matches none of its queue's
// topics, so the server's matched set is empty for a queue delivery - and
// even where it is not, it can list a session that has already ended.
//
// The ids are returned rather than the clients, so the broker-wide lock is
// held for the walk and released before anything is asked of a session.
func (b *Broker) workersOf(c *channel.Channel) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, 0, len(b.members[c.Name]))
	for id := range b.members[c.Name] {
		ids = append(ids, id)
	}
	return ids
}

// releaseUndelivered returns a record whose delivery reached nobody.
func (b *Broker) releaseUndelivered(pk packets.Packet, why string) {
	id := hex.EncodeToString(pk.Properties.CorrelationData)
	if id == "" {
		return
	}
	b.mu.Lock()
	d, ok := b.deliveries[id]
	b.mu.Unlock()
	if !ok {
		return
	}
	b.releaseDelivery(d, why, false)
}

// OnQosPublish fires as the delivery is set in flight for one worker.
// That is where saguin learns which worker the server chose.
func (b *Broker) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, resends int) {
	id := hex.EncodeToString(pk.Properties.CorrelationData)
	if id == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.deliveries[id]
	if !ok {
		return
	}
	// Counted once per worker, however many times the packet is sent: a
	// resumed session re-sends what it held and fires this again.
	//
	// **This is also where a queue delivery is counted as delivered**, for
	// the same reason: it is the first moment a worker is known to hold it.
	// The offer that injected it cannot tell, because the selection runs
	// inside the injection and gives back a record nobody can take without
	// returning an error.
	if d.holder != cl.ID {
		b.unhold(d)
		d.holder = cl.ID
		b.holding[heldBy{cl.ID, d.channel}]++
		if cc := b.counted.forChannel(d.channel); cc != nil {
			cc.delivered.Add(1)
		}
	}
	// **Outside the branch above, which is about the id.** A resumed session
	// re-sends what it held on a new connection, so the id is unchanged and
	// the connection is not - and it is the connection a teardown asks
	// about. Set inside, the marker would still name the socket that went
	// away and the successor's teardown would decline to return a record
	// nobody else can.
	d.conn = cl
	d.packetID = pk.PacketID
	b.byPacket[packetKey(cl.ID, pk.PacketID)] = id
}

// OnDeliveryUnsent gives back a job whose delivery expired before it was
// ever sent: in the worker's outbound queue, withheld for its window, or
// carried unsent across a takeover (mqtt.Hooks.OnDeliveryUnsent).
//
// **returnUnsent cannot see it.** It finds a never-sent job by deleting the
// withheld entry, and the expiry has already removed the entry - so the job
// stayed Delivering, held by a worker that never had it, with no PUBACK to
// come and no visibility clock to rescue it (invariant 7). It goes back as
// returnUnsent's do, with no attempt spent: the worker never had it.
// Whether the job itself is still worth doing is the queue's own question,
// asked when it is offered again; nothing is advanced or deleted here.
//
// Only the delivery registered under this packet on this connection, or the
// one that took the id over (acksFor): the Correlation Data of a broadcast
// or channel delivery names nothing here, and is passed over.
func (b *Broker) OnDeliveryUnsent(cl *mqtt.Client, pk packets.Packet) {
	id := hex.EncodeToString(pk.Properties.CorrelationData)
	if id == "" {
		return
	}
	b.mu.Lock()
	d, ok := b.deliveries[id]
	ok = ok && d.holder == cl.ID && b.byPacket[packetKey(cl.ID, pk.PacketID)] == id
	recorded := (*mqtt.Client)(nil)
	if ok {
		recorded = d.conn
	}
	b.mu.Unlock()
	if !ok || !b.acksFor(recorded, cl) {
		return
	}
	b.releaseDelivery(d, "never sent", false)
}

// acksFor is whether an acknowledgement on connection cl settles a delivery
// recorded on connection recorded: when it is that connection, or cl is the
// connection the engine registers under the client id now - a successor's
// PUBACK for a delivery its takeover carried. **Nothing else**, and not a
// delivery recorded on no connection: that falls to the second question like
// any other.
//
// A PUBACK is a client id and a packet identifier, and neither names a
// connection. The engine retires the identifier on a taken-over connection
// and lets go of its handover lock before the hook runs; the takeover then
// clones a table that no longer holds it, and the successor can register a
// delivery of its own under the same identifier before the old connection's
// hook lands. Asked nothing, that late hook settled the successor's
// delivery: a channel's stored position passed a record the successor never
// acknowledged, and a job's visibility clock started - and an attempt was
// spent - on a worker that had not acknowledged it. The broadcast drain asked from the start (bdrain.takes);
// every exit of OnQosComplete asks now, the same question.
func (b *Broker) acksFor(recorded, cl *mqtt.Client) bool {
	if recorded == cl {
		return true
	}
	if b.srv == nil {
		return false
	}
	now, ok := b.srv.Clients.Get(cl.ID)
	return ok && now == cl
}

// OnQosComplete fires on PUBACK. This is where the visibility timeout
// starts and where the attempt is counted - never at send (invariant 7). A
// delivery that never reaches PUBACK burns nothing: the worker never
// confirmed it had the job.
func (b *Broker) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	key := packetKey(cl.ID, pk.PacketID)

	// **A freed in-flight slot belongs to the client, whatever freed it**,
	// and what it may be owed is served on every path out, because three
	// paths freed one and pumped nothing.
	//
	// The one that matters is a resumed session. A consumer whose socket
	// dies mid-replay leaves packets unacknowledged; the substrate restores
	// them with the session and re-sends them, but forgetConsumer cleared
	// saguin's record of them at the disconnect - so their acknowledgements
	// arrive here as packets saguin has never heard of, fall past both
	// branches below, and free window room that nothing then uses. The
	// resume-time pump has already run and found the window full, so the
	// consumer sits connected, `Session Present` = 1, position intact, and
	// receives none of the backlog it is owed until an unrelated publish to
	// one of its channels wakes it. Nothing is logged, because from the
	// broker's side nothing went wrong.
	//
	// The other two are the same shape one step in: a `latest` delivery's
	// PUBACK drained only that store, and a queue delivery's pumped nothing,
	// so a client holding an append channel beside either was starved by it.
	//
	// What is owed is decided in the one hold below, with the bookkeeping,
	// and served by this defer once the lock is let go - see appendFreedC and
	// serveFreed.
	var owed freed
	defer func() { b.serveFreed(cl, owed) }()
	// **And what a shared group's backlog is waiting for.** A member that is
	// connected but full is passed over, so what the group holds moves when
	// its window opens - not at its next SUBSCRIBE, which a member with a
	// standing subscription never sends again. This is the same "frees a
	// slot and does not wake the thing waiting on it" the three defects
	// above were: bdrain.acked and bdrain.freed wake its groups.

	// **A delivery from the session's broadcast log**, settled in memory on
	// this read loop and written by the session's own drain (bdrain.acked).
	// Anything else's acknowledgement frees room in the window the drain
	// shares, so it is woken for that too - the starvation the paths above
	// were.
	drained := false
	if d := b.broadcastDrain(); d != nil {
		if drained = d.acked(cl, pk.PacketID); !drained {
			defer d.freed(cl)
		}
	}

	// **The acknowledgement's own bookkeeping, and the append channels it
	// frees a slot for, under this client's lock and not b.mu** - see
	// consumer. An append consumer's PUBACK takes nothing else.
	var p pending
	isSaguin := false
	// stale is saguin's packet recorded on a connection this one does not
	// settle for (acksFor): a late PUBACK from a taken-over connection, whose
	// identifier the successor has since given a delivery of its own.
	stale := false
	mayOweOther := true // no delivery state known: ask b.mu everything
	if con := b.lookupConsumer(cl.ID); con != nil {
		con.cmu.Lock()
		if !con.gone {
			p, isSaguin = con.inflight[pk.PacketID]
			stale = isSaguin && !b.acksFor(p.conn, cl)
			if isSaguin && !stale {
				delete(con.inflight, pk.PacketID)
				// Only while this delivery still belongs to the consumer's
				// current era. A seek discarded what was outstanding and may
				// have re-sent this very offset under a fresh packet
				// identifier; removing it here on the strength of the old
				// packet's PUBACK would let the position pass a record the
				// consumer is still holding - see cursor.gen. The slot and
				// the packet identifier are freed either way, which is what
				// the delete above and mochi's own accounting do.
				if !p.latest {
					if cur := con.cursors[p.channel]; cur != nil && cur.gen == p.gen {
						delete(cur.outstanding, p.offset)
					}
				}
			}
			owed.drains = b.appendFreedC(cl, con)
			owed.latest = latestOwedC(con)
			mayOweOther = con.hasQueues
		}
		con.cmu.Unlock()
	}
	if isSaguin && !stale && p.latest && p.channel != RetainedStoreName {
		// **A `latest` value acknowledged, so the consumer has it**, and the
		// position may move up to it. The retained store keeps no position:
		// it is never served to a resumed session - RFC 0003 promises a
		// standard broker's behaviour there, and `track` cannot return it -
		// so a mark for it would be written and read by nobody. Outside
		// b.mu: the positions have a lock of their own (latestPositions).
		b.latestSeen.advanceSeen(cl.ID, p.channel, p.offset)
	}
	// **An acknowledgement owed no queue offer takes no b.mu**, append's,
	// latest's or the broadcast log's: what each is owed was decided under
	// its own lock above.
	if (isSaguin || drained) && !mayOweOther {
		return
	}

	b.mu.Lock()
	// A queue delivery, answered by its PUBACK.
	var d *delivery
	var q QueueStore
	if !isSaguin && !drained {
		if id, ok := b.byPacket[key]; ok {
			// A worker's job is settled by the connection it is on, or by the
			// one that now has the session (acksFor). A stale PUBACK leased the
			// successor's job, and the worker's answer then completed it: a job
			// removed that nobody processed.
			if d = b.deliveries[id]; d != nil && !b.acksFor(d.conn, cl) {
				d = nil
			} else {
				delete(b.byPacket, key)
			}
		}
		if d != nil {
			q = b.queues[d.channel]
		}
	}
	owed.queues = b.queuesFreedLocked(cl)
	if !isSaguin && owed.latest == nil {
		// Not one of saguin's deliveries, and no consumer record read above:
		// a record made since may hold values waiting.
		if con := b.lookupConsumer(cl.ID); con != nil {
			con.cmu.Lock()
			owed.latest = latestOwedC(con)
			con.cmu.Unlock()
		}
	}
	b.mu.Unlock()

	if isSaguin || d == nil || q == nil {
		return
	}

	var vis int64
	if c := b.reg.Get(d.channel); c != nil {
		vis = c.VisibilityTimeout
	}

	// The holder comes from the acknowledging session rather than from the
	// delivery, which learned it in OnQosPublish: the PUBACK is the packet
	// that proves which worker has the record.
	h := d.held()
	h.Holder = cl.ID

	it, ok, err := q.Lease(h, time.Now(), time.Duration(vis)*time.Second)
	if err != nil {
		// The lease was not recorded, so the attempt was not counted and no
		// deadline is running. The record stays where it was and the worker
		// keeps it until its session ends, which returns it.
		b.log.Error("cannot record a lease", "channel", d.channel, "offset", d.offset,
			"worker", cl.ID, "error", err)
		return
	}
	if !ok {
		return
	}
	b.log.Debug("leased", "channel", d.channel, "offset", d.offset,
		"worker", cl.ID, "attempt", it.Attempts, "until", it.LeaseUntil.Format(time.RFC3339))
}

// freed is what a slot an acknowledgement freed may be owed to, decided
// under b.mu and served after it - see appendFreedC and serveFreed.
type freed struct {
	latest []string // `latest` channels with a snapshot remainder waiting
	queues []string // queues to offer this worker its next job from
	drains []claimedPump
}

type claimedPump struct {
	c   *channel.Channel
	cur *cursor
}

// appendFreedC, latestOwedC and queuesFreedLocked decide, for a client whose
// acknowledgement has just freed a slot, everything that slot may be owed to
// - in the same holds as the acknowledgement's bookkeeping, so nothing can
// change between the decision and what it rests on.
//
// **Three questions, each decided on its own.** A `latest` snapshot queued
// while the window was full has nothing in flight, so no acknowledgement of
// its own can resume it (drainPending); a queue worker may be owed its next
// job (queuesFreedLocked); and an append consumer may have records behind
// its cursor. The third is the only one elided, and only for append
// channels: applied to all three it would leave a `latest` consumer stranded
// part-way through its snapshot. The queue's is b.mu's, asked only when the
// consumer says it has queues (hasQueues); the snapshot's is read off the
// consumer's own `latest` list, under its cmu (latestOwedC).
//
// **An append channel is claimed here or not woken at all.** One already
// being drained is told to read again (repump), as claimPump does; one with
// nothing to send is left - its cursor has passed every record announced on
// the channel and it has nothing pending. That is safe because a record
// stored after this is announced by pumpAll, which wakes the consumer
// itself, so a wake skipped here is never the last one. The rest are
// claimed, for serveFreed to drain.
//
// **It used to be four holds of b.mu, and two wakes.** OnQosComplete,
// offerFreed, drainPending and pumpConsumer each took the lock to ask about
// the same client, and pumpConsumer was called twice for every append
// PUBACK; each call claimed and planned a batch whether or not there was
// anything to send. Counted with a mutex that records its callers, that was
// about 7.5 acquisitions of b.mu per delivery draining a backlog and 17 on a
// channel every subscriber takes every record from.
//
// appendFreedC: the caller holds con.cmu.
func (b *Broker) appendFreedC(cl *mqtt.Client, con *consumer) []claimedPump {
	var drains []claimedPump
	for chName := range con.plans {
		c := b.reg.Get(chName)
		if c == nil {
			continue
		}
		cur := b.cursorC(con, cl.ID, chName, c.StartAtTail)
		cur.expiresIn = b.sessionExpiry(cl)
		if cur.pumping {
			cur.repump = true
			continue
		}
		if !b.mayHaveRecordsLocked(cur, chName) {
			continue
		}
		cur.pumping = true
		drains = append(drains, claimedPump{c, cur})
	}
	return drains
}

// latestOwedC is the latest channels this consumer has values waiting on, for
// an acknowledgement that has just freed a slot. The caller holds cmu.
func latestOwedC(con *consumer) []string {
	if len(con.latest) == 0 {
		return nil
	}
	latest := make([]string, 0, len(con.latest))
	for name := range con.latest {
		latest = append(latest, name)
	}
	sort.Strings(latest)
	return latest
}

// mayHaveRecordsLocked reports whether a consumer may have records to be
// sent on an append channel: false only when its cursor is past every
// record the channel has announced and nothing is on its pending list.
//
// It errs towards true - a watermark that is missing says nothing, so it
// answers true - because a false that should have been true is a consumer
// with records it is owed and nothing to send them.
//
// The caller holds b.mu.
func (b *Broker) mayHaveRecordsLocked(cur *cursor, chName string) bool {
	w := b.notified[chName]
	if w == nil {
		return true
	}
	return cur.next <= w.maxSeen.Load() || len(cur.pending) > 0
}

// serveFreed serves what appendFreedC, latestOwedC and queuesFreedLocked
// decided, outside the lock.
//
// **Current state before more history.** A freed slot is offered to the
// snapshots first because they are finite and an append channel is not:
// pumping first on a channel with a backlog means the window is full again
// before any snapshot is looked at, which is the starvation this whole path
// exists to avoid.
func (b *Broker) serveFreed(cl *mqtt.Client, f freed) {
	for _, name := range f.latest {
		b.drainLatest(cl, name)
	}
	// **Each claimed drain runs on a goroutine of its own**, which is
	// invariant 16's rule and pumpOffPath's shape: the claim was taken in
	// appendFreedC, so this only starts what it holds. Run here instead, on
	// the acknowledging connection's read loop, the drain was paid before
	// that connection's next packet was read, and a PINGREQ behind the
	// PUBACK waited for a drain competing for b.mu with every other
	// consumer's.
	for _, d := range f.drains {
		b.drains.Add(1)
		go func() {
			defer b.drains.Done()
			b.drainPump(cl, d.c, d.cur, nil)
		}()
	}
	for _, name := range f.queues {
		b.offerSoon(name)
	}
}

// handleSeek moves a consumer's own position, which is what lets it replay
// after a bug or skip a backlog it no longer wants without an operator
// touching storage (RFC 0003 "Moving a consumer's position").
//
// It takes effect where the consumer is, without a reconnect. Records are
// in flight to this client at QoS 1 while the seek arrives, and a PUBACK
// for one of them landing afterwards would advance the position past the
// place the consumer just asked for - silently, and in the direction that
// skips records, which is the one failure a position must never produce.
// So the seek discards what is outstanding on this channel and ends the
// cursor's era, after which an acknowledgement for anything sent before it
// is recognised and ignored (cursor.gen), and the consumer is fed again
// from the new position.
//
// **It reaches this consumer and this channel and nothing else.** A
// position is held per client id and per channel, and a shared
// subscription is refused on every channel that is not a queue, so no
// other subscriber of the same channel has a stream to disturb.
//
// What was already written to the socket stays written: the broker stops
// counting those records, it cannot unsend them. That is at-least-once
// behaving as promised, and it is what the consumer asked for.
// The channel is known to exist and to be an append channel: routePublish
// refuses the seek in the PUBACK otherwise, because which channel a topic
// names is a property of the topic. Everything left here is a property of
// the request, and is answered in the reply.
func (b *Broker) handleSeek(cl *mqtt.Client, pk packets.Packet, name string) error {
	// A position is stored per session, so a client whose session ends with
	// its connection has nothing to move: the seek would be stored and then
	// discarded, which is worse than being told.
	//
	// **What makes a session outlive its connection is the Session Expiry
	// Interval and nothing else.** Clean Start says what happens to a
	// *previous* session at CONNECT - discard it and begin a new one - and
	// says nothing about how long the new one lasts. This check used to
	// refuse a clean start as well, and the broker then stored positions
	// durably for the very sessions whose seeks it turned away: a consumer
	// connecting Clean Start = 1 with an expiry read, disconnected, and came
	// back to Session Present = 1 with its position honoured, having been
	// told it had no session to move.
	//
	// That is the ordinary first connection, too. A durable consumer's
	// standard opening is Clean Start = 1 with an expiry - start from a
	// known state - and Clean Start = 0 afterwards, so the refusal fell on
	// the whole of its first session. It cost little while a seek waited for
	// the next connect; now that a seek applies where the consumer is, the
	// first connection is where seeks happen.
	if b.sessionExpiry(cl) <= 0 {
		return b.refuseSeek(cl, pk, name, "no-session")
	}

	asked := strings.TrimSpace(string(pk.Payload))

	b.mu.Lock()
	lg := b.logs[name]
	b.mu.Unlock()
	if lg == nil {
		// An append channel with no log behind it is the broker's own
		// inconsistency rather than anything the client did, so it answers
		// the way every other storage failure does.
		b.log.Error("an append channel has no log", "channel", name)
		return b.refuseSeek(cl, pk, name, "storage")
	}

	// `0` and `-1` are read here rather than by the client so that it never
	// has to ask for the floor or the end first: the answer to that question
	// can change between the asking and the seek, and offsets start at 1 and
	// are never reused, so neither number can collide with a real one.
	floor, next := lg.Floor(), lg.Next()

	var offset uint64
	want, err := strconv.ParseInt(asked, 10, 64)
	switch {
	case err == nil && want < -1:
		return b.refuseSeek(cl, pk, name, "malformed")

	case err == nil:
		// **A bare integer goes on meaning an offset**, and that is the
		// whole reason a time is told apart by shape rather than by a flag.
		// `1763000000` is a plausible offset and a plausible Unix time, and
		// a consumer that seeks to the wrong one reads from there in order
		// and reports success - a silent skip, which is the one failure a
		// position must never produce.
		switch want {
		case 0:
			offset = floor
		case -1:
			offset = next
		default:
			offset = uint64(want)
		}

	default:
		at, ok := seekTime(asked, time.Now())
		if !ok {
			return b.refuseSeek(cl, pk, name, "malformed")
		}
		resolved, code, props := b.seekToTime(lg, at, floor, next)
		if code != "" {
			return b.refuseSeek(cl, pk, name, code, props...)
		}
		offset = resolved
	}

	// Both bounds are refused rather than clamped, for one reason: clamping
	// manufactures exactly the state a position must never be in. A consumer
	// that asked for 500, was given 900, and was not told, processes the
	// remainder in order and concludes it saw everything.
	switch {
	case offset < floor:
		return b.refuseSeek(cl, pk, name, "below-floor", packets.UserProperty{
			Key: "saguin-floor", Val: strconv.FormatUint(floor, 10)})
	case offset > next:
		return b.refuseSeek(cl, pk, name, "beyond-end", packets.UserProperty{
			Key: "saguin-next", Val: strconv.FormatUint(next, 10)})
	}

	// **The store and the cursor move as one, under b.positions**, and that
	// is the whole of the ordering. flushPositions decides what to write
	// while holding the same lock, so there is no schedule in which it reads
	// the offset this consumer's stream had reached and writes it after the
	// position stored here - which is the broker confirming a seek and then
	// silently undoing it: a consumer seeking back got no replay, one
	// seeking past a poison stretch was handed it again.
	//
	// Holding it is also what orders a seek against the drops, so a seek
	// cannot land after the session it belongs to has been swept and leave a
	// row nothing will remove.
	//
	// **Stored first, so a seek refused at storage changes nothing at all.**
	// The stream keeps running from where it was and the client is told. The
	// flag this replaced had to be raised before the store and put back
	// afterwards, and getting that rollback exactly right - right about its
	// own seek and about an earlier one on the same cursor - was two of its
	// four rules; a counter that only ever moves on success has neither.
	b.positions.Lock()
	// **Only the connection holding the id moves its position.** A seek is
	// read on its own connection and stored by client id, and a device that
	// reconnects with a clean start while its old connection's seek is still
	// being handled - the engine reads that packet before the takeover and
	// handles it after - had the new session's position moved by a seek it
	// never sent: stored for it, or applied to the cursor it had just started
	// reading from the floor, which stepped it past every record in between
	// (invariant 1). Asked here, under b.positions, it is ordered against the
	// clean start's discard of the old session's positions, which takes this
	// lock too: the seek either lands first and is discarded with the old
	// session, or finds another connection holding the id and changes
	// nothing. That connection's session is not this one's to move, which is
	// what `no-session` says; a successor whose CONNACK then fails gives the
	// id back, and a seek refused in that moment can be sent again.
	b.mu.Lock()
	holds := b.owner[cl.ID] == cl
	b.mu.Unlock()
	if !holds {
		b.positions.Unlock()
		return b.refuseSeek(cl, pk, name, "no-session")
	}
	err = lg.SavePosition(store.Position{
		Reader:    store.MQTTReader(cl.ID),
		Offset:    offset,
		LastSeen:  time.Now(),
		ExpiresIn: b.sessionExpiry(cl),
	})
	var applied *cursor // the cursor this seek moved and holds until its reply
	if err == nil {
		// Under the consumer's lock, which is what orders this against a
		// drain's commit and flushPositions' read of the cursor.
		con := b.lookupConsumer(cl.ID)
		if con != nil {
			con.cmu.Lock()
		}
		// Only a consumer that has started reading has a cursor. One that
		// seeks before it subscribes has no stream to move and none to
		// restart: its first read builds a cursor from the position just
		// stored, which is the place it asked for.
		var cur *cursor
		if con != nil {
			cur = con.cursors[name]
		}
		if cur != nil {
			// The era ends here, and all of it in one critical section: a
			// gap between the clear and the raise lets a record registered
			// in the old era survive the clear and then be acknowledged as
			// though it belonged to the new one.
			cur.next = offset
			clear(cur.outstanding)
			cur.gen++
			// Already written, so the flusher has nothing to do until this
			// consumer acknowledges something from the new position.
			cur.saved = offset
			// **And nothing is served from the new position until the
			// reply is written**, in the same section as the move, so a
			// drain already running for this consumer - one another
			// client's publish started - finds the cursor held before it
			// can read from it. The reply is written with no lock held: a
			// write can wait up to limits.write_timeout on a slow client,
			// and held under cmu or b.positions it would stall this
			// consumer's acknowledgements or every consumer's positions.
			cur.seeking++
			applied = cur
		}
		if con != nil {
			con.cmu.Unlock()
		}
	}
	b.positions.Unlock()
	if applied != nil {
		// Let go on every way out - a refusal below, a panic recovered
		// above - because a cursor left held is a consumer never sent
		// another record. Then fed again from where it asked to be, which
		// is also what resumes a drain that stopped at the hold. Without
		// the pump the consumer waits for the next publish on the channel
		// to notice it has moved, which on a quiet channel is for ever.
		defer b.releaseSeek(cl, name, applied)
	}
	if err != nil {
		b.log.Error("cannot store a seek", "client", cl.ID, "channel", name, "error", err)
		return b.refuseSeek(cl, pk, name, "storage")
	}

	b.log.Info("consumer moved its position", "client", cl.ID, "channel", name,
		"asked", b.limits.Loggable(asked), "offset", offset, "floor", floor, "next", next)
	if hook := seekBeforeReply.Load(); hook != nil {
		(*hook)(cl.ID)
	}
	b.replySeek(cl, pk, strconv.FormatUint(offset, 10))

	// Handled, and not delivered to anybody: the seek topic is the broker's
	// own, not a channel a subscriber reads.
	return packets.CodeSuccessIgnore
}

// releaseSeek lets a cursor a seek held until its reply be served again, and
// feeds it from where the seek put it (handleSeek).
func (b *Broker) releaseSeek(cl *mqtt.Client, name string, cur *cursor) {
	cur.con.cmu.Lock()
	cur.seeking--
	cur.con.cmu.Unlock()
	if c := b.reg.Get(name); c != nil {
		b.pump(cl, c, nil)
	}
}

// handleKVGet answers the current value of one topic on a `latest` channel,
// without making the caller a subscriber (RFC 0003 "Reading one value").
//
// A `latest` channel is already a key-value store: SET is an ordinary
// publish, DELETE is a publish with a zero-length payload, and a new
// subscription is a multi-get. What was missing is a read that does not
// enrol the caller in every later update to a topic it wanted once, and
// that can tell *no value* from *not yet* - subscribing to ask answers both
// with silence.
//
// **An empty reply means there is no value**, which is what a zero-length
// payload already means on this channel type, so absence needs no new
// vocabulary and deleted and never-set stay indistinguishable exactly as
// the channel type already makes them. That is the whole reason every
// refusal below goes in the PUBACK instead: if a bad request could answer
// on the Response Topic, an empty reply would mean two things.
//
// **Which also means the request must be QoS 1**, because a QoS 0 publish
// has no PUBACK to carry a refusal. It is dropped, the same answer
// refuseReserved gives QoS 0 in this space and for the same reason.
//
// The reply carries Correlation Data whether or not the caller set any: the
// caller's if it did, and otherwise the key it asked about. A client with
// several reads outstanding can then tell the answers apart without keeping
// a table, and one that wants its own bookkeeping still has it.
func (b *Broker) handleKVGet(cl *mqtt.Client, pk packets.Packet) error {
	if pk.FixedHeader.Qos == 0 {
		b.log.Warn("refusing a point read published at QoS 0, which has no PUBACK to "+
			"carry a refusal", "client", cl.ID)
		return packets.ErrRejectPacket
	}

	// Refused rather than treated as a no-op. Unlike a seek, which does its
	// work and merely says nothing when there is nowhere to reply, a read
	// with nowhere to send the answer has no effect at all - and accepting
	// it would tell the client its read succeeded.
	if pk.Properties.ResponseTopic == "" {
		return b.refuseKVGet(cl, "a point read needs a Response Topic to answer on")
	}

	key := string(pk.Payload)
	switch {
	case key == "":
		return b.refuseKVGet(cl, "the payload of a point read is the topic to read, and it is empty")
	case len(key) > b.limits.MaxTopicLength:
		return b.refuseKVGetKey(cl, key, "longer than max_topic_length")
	case channel.TooDeep(key, b.limits.MaxTopicLevels) != nil:
		return b.refuseKVGetKey(cl, key, "deeper than max_topic_levels")
	case strings.HasPrefix(key, "$"):
		// Not "claimed by no channel, so it is broadcast", which is what
		// this used to say and is false twice over: a publish into
		// `$saguin/` is refused by the reserved-space rule and one into
		// `$SYS/` is refused 0x87, so neither is broadcast either. No
		// channel name may begin with `$` - the name rule admits letters,
		// digits, `-`, `_` and `.` - so this is provable about the key
		// rather than a list of prefixes somebody has to keep up to date.
		return b.refuseKVGetKey(cl, key, "in a reserved topic space, which holds no channel's values")
	case !mqtt.IsValidFilter(key, true):
		// Wildcards are the case worth naming: "everything under X" is what
		// SUBSCRIBE already does, and allowing it here would make a point
		// read return an unbounded set - a scan wearing a get's clothes.
		return b.refuseKVGetKey(cl, key, "not a topic name: a point read takes one exact topic, and no wildcard")
	}

	c := b.reg.Resolve(key)
	switch {
	case c == nil:
		return b.refuseKVGetKey(cl, key, "claimed by no channel, so it is broadcast and has no stored value")
	case c.Type != channel.Latest:
		return b.refuseKVGetKey(cl, key, fmt.Sprintf("in %q, which is %s: only %s holds a current value per topic",
			c.Name, aChannel(c.Type), aChannel(channel.Latest)))
	}

	b.mu.Lock()
	lt := b.latest[c.Name]
	b.mu.Unlock()
	if lt == nil {
		// A latest channel with no store behind it is the broker's own
		// inconsistency rather than anything the client did.
		b.log.Error("a latest channel has no store", "channel", c.Name)
		return packets.Code{Code: packets.ErrImplementationSpecificError.Code,
			Reason: "this channel has no store behind it"}
	}

	r, found, err := lt.Get(key)
	if err != nil {
		b.log.Error("cannot read a value for a point read",
			"channel", c.Name, "client", cl.ID, "error", err)
		return packets.Code{Code: packets.ErrImplementationSpecificError.Code,
			Reason: "the value could not be read; the broker logs why"}
	}

	// Absent is an answer rather than a refusal, and it is the empty one.
	var payload []byte
	if found {
		payload = r.Payload
	}
	corr := pk.Properties.CorrelationData
	if len(corr) == 0 {
		corr = []byte(key)
	}
	b.reply(cl, pk.Properties.ResponseTopic, payload, corr, "a point read")
	return packets.CodeSuccessIgnore
}

// handleCatalogue answers what one channel is: its type and the filter it
// claims, as the operator wrote it (RFC 0002 "Asking what a channel is").
//
// **It exists so that a client can address a channel by name.** Everything
// else a client says about a channel already names it - a seek, a worker's
// answer, a dead-letter channel, the queue subscription form - but
// publishing and subscribing name topics, and only the operator's
// configuration says which topics a channel claims. Without this, a library
// offering `publish(channel, key)` would have to read that configuration or
// hold an operator's credential, and RFC 0005 says why a device must have
// neither.
//
// **The answer is scoped to what the asking client may use**, and a channel
// it holds no verb on is answered exactly as a channel that does not exist
// is: an empty reply. So a client cannot walk a list of names and learn
// which of them are real. It is a disclosure rule rather than a permission
// one - nothing here grants anything, and every act on the channel is
// refused or allowed later by the same rules as before.
//
// **The filter goes back as written, braces and all.** `{a,b}` is a level
// with a fixed set of spellings, which is exactly what a caller composing a
// topic needs; the plain filters it expands to are the broker's business.
//
// Refusals ride the PUBACK, for the reason a point read's do: the reply
// carries an answer and nothing else, so an empty one can mean one thing.
func (b *Broker) handleCatalogue(cl *mqtt.Client, pk packets.Packet, name string) error {
	if pk.FixedHeader.Qos == 0 {
		b.log.Warn("refusing a catalogue request published at QoS 0, which has no "+
			"PUBACK to carry a refusal", "client", cl.ID)
		return packets.ErrRejectPacket
	}
	if pk.Properties.ResponseTopic == "" {
		b.log.Warn("refusing a catalogue request with no Response Topic",
			"client", b.limits.Loggable(cl.ID))
		return packets.Code{Code: packets.ErrImplementationSpecificError.Code,
			Reason: "a catalogue request needs a Response Topic to answer on"}
	}

	// The caller's, or the name it asked about - the same rule a point read
	// follows, so that several questions can be outstanding at once without
	// the caller keeping a table.
	corr := pk.Properties.CorrelationData
	if len(corr) == 0 {
		corr = []byte(name)
	}

	c := b.reg.Get(name)
	if c == nil {
		b.reply(cl, pk.Properties.ResponseTopic, nil, corr, "a catalogue request")
		return packets.CodeSuccessIgnore
	}
	held := b.permittedVerbs(cl, c)
	if len(held) == 0 {
		b.log.Debug("a catalogue request about a channel this client holds nothing on",
			"client", b.limits.Loggable(cl.ID), "channel", c.Name)
		b.reply(cl, pk.Properties.ResponseTopic, nil, corr, "a catalogue request")
		return packets.CodeSuccessIgnore
	}

	answer := struct {
		Name   string   `json:"name"`
		Type   string   `json:"type"`
		Filter string   `json:"filter"`
		Verbs  []string `json:"verbs"`
		Pin    string   `json:"pin,omitempty"`
	}{Name: c.Name, Type: string(c.Type), Filter: c.Filter, Verbs: held}
	if c.Type == channel.Queue {
		// The one addressing answer a client cannot derive from the filter:
		// a queue is consumed through this exact string and no other.
		answer.Pin = c.QueueFilter()
	}
	payload, err := json.Marshal(answer)
	if err != nil {
		b.log.Error("cannot build a catalogue answer", "channel", c.Name, "error", err)
		return packets.Code{Code: packets.ErrImplementationSpecificError.Code,
			Reason: "the answer could not be built; the broker logs why"}
	}
	b.reply(cl, pk.Properties.ResponseTopic, payload, corr, "a catalogue request")
	return packets.CodeSuccessIgnore
}

// handleDisconnectRequest hangs up the client named in the payload (RFC
// 0002 "Hanging up a client").
//
// **It ends the connection and leaves the session alone.** A session
// outliving its connection is what a session is for, so the device
// reconnects and resumes at its stored position: the whole cost of this
// verb is one reconnection, which is what makes it safe to put in front of
// an operator. Ending a session would take a durable consumer's position
// with it, and saguin does not offer that.
//
// **It withdraws nothing on its own**, and saying otherwise was a defect in
// the document rather than in the code: a hung-up device reconnects with
// whatever the password file and the acl_file say at that moment. What it is
// the second half of is RFC 0002's "Withdrawing a device's access" - edit
// the two files, signal the broker to re-read them, then hang the client up
// so that it connects again and is refused. The acl_file is asked on every
// publish and so bites the open connection; the password file is read at
// CONNECT and so needs this.
//
// The other reason to reach for it has nothing to do with credentials: a
// queue lease goes back when the connection ends, so hanging up a wedged
// worker returns the jobs it was holding rather than waiting out their
// visibility timeout.
func (b *Broker) handleDisconnectRequest(cl *mqtt.Client, pk packets.Packet) error {
	// **A Response Topic is required, exactly as a point read's is.** The
	// outcome that matters - a client hung up, or nothing found under that
	// id - is a payload rather than a reason code, because a `PUBACK` may
	// leave its reason-code byte out and the substrate leaves it out for
	// every code below `0x80`. So the answer travels on the reply, and a
	// request with nowhere to reply is one whose answer would be thrown
	// away: refused here rather than acted on silently.
	//
	// The alternative was a code at `0x80` or above, which does reach the
	// wire. It says the request failed, and this one did not - and saguin
	// closes a 3.1.1 client's connection on any such code, so an operator
	// asking about a device that had already gone offline would be hung up
	// themselves.
	if pk.Properties.ResponseTopic == "" {
		return b.refuseDisconnect(cl, pk, "no response topic",
			"a disconnect needs a Response Topic to answer on: whether a client "+
				"was hung up, or nothing was connected under that id, is the reply")
	}

	// The id is a client string bounded only by max_message_size, so it is
	// logged through Loggable everywhere below and never echoed on the wire.
	//
	// **As sent, and not trimmed**: a client id is whatever the device typed
	// and admission bounds only its length, so `dev ` and `dev` can both be
	// connected - and a trimmed request for the first hung up the second
	// and answered hung-up.
	id := string(pk.Payload)
	if id == "" {
		return b.refuseDisconnect(cl, pk, "no client id in the payload",
			"the payload of a disconnect is the client id to hang up, and it is empty")
	}

	// **A client may not hang itself up, and the reason is that this verb
	// cannot answer for it.** The acknowledgement is written by the
	// substrate after this returns, so ending the connection here means the
	// PUBACK is written to a stopped client and never arrives: the caller
	// sees its library report a publish that was never acknowledged, which
	// is what a broken broker looks like. Measured rather than reasoned
	// about - the first version allowed it, and the operator's client got
	// `PUBLISH transmitted but not fully acknowledged` and no answer.
	//
	// Nothing is lost by refusing. Ending its own connection is something
	// every client can already do, with the DISCONNECT its protocol
	// defines, and that one is acknowledged by being the last thing sent.
	if id == cl.ID {
		return b.refuseDisconnect(cl, pk, "a client named itself",
			"a client cannot hang itself up: send a DISCONNECT instead, "+
				"which is acknowledged by being the last packet on the connection")
	}

	target, found := b.srv.Clients.Get(id)
	switch {
	case !found, target.Closed():
		// **A session held with nothing connected to it is nothing to hang
		// up**, and it is the common case: the table keeps a durable
		// session until it expires, so an id an operator reads off a
		// dashboard may well be there with no socket behind it.
		return b.nothingToDisconnect(cl, pk, id, "no connection under that id")
	case target.Net.Inline:
		// The substrate's own in-process publisher - a Will, a queue
		// delivery, a bridge. It is how the broker publishes on its own
		// behalf rather than anybody's session, and hanging it up would
		// stop the broker doing its own work.
		return b.nothingToDisconnect(cl, pk, id, "not a session")
	}

	b.log.Info("hanging up a client on an operator's request",
		"client", b.limits.Loggable(id), "by", b.limits.Loggable(cl.ID))
	b.replyDisconnect(cl, pk, "hung-up")
	// **endConnection rather than disconnect.** The difference is the
	// legacy-flap instruments, which RFC 0005 defines as clients "hung up on
	// because their protocol could not be told why" - a refusal an operator
	// alerts on. This is not a refusal: somebody asked for it. Counting it
	// there would inflate that number with the one ending nobody needs to
	// act on, which is the reason a session taken over is not counted either.
	b.hangUp(target, packets.ErrAdministrativeAction)
	return packets.CodeSuccessIgnore
}

// nothingToDisconnect answers a request that named no connected client:
// authorized, understood, and nothing to do.
//
// **The answer is a payload rather than a reason code, and that is not a
// preference.** MQTT 5 has a code that fits - `0x10`, No matching
// subscribers - and it cannot be relied on to arrive. A `PUBACK` may leave
// its reason-code byte out, and the substrate leaves it out for every code
// below `0x80`: `0x10` reaches the client as `0x00`, which is the answer for
// the opposite outcome. Measured rather than reasoned about - the first
// version of this returned `0x10` and four end-to-end cases read `0x00`.
//
// So the two outcomes are told apart the way a seek's are, on the Response
// Topic, where the answer is a payload nothing may omit.
func (b *Broker) nothingToDisconnect(cl *mqtt.Client, pk packets.Packet, id, why string) error {
	b.log.Info("a disconnect named no connected client",
		"client", b.limits.Loggable(id), "by", b.limits.Loggable(cl.ID), "reason", why)
	b.replyDisconnect(cl, pk, "no-such-client")
	return packets.CodeSuccessIgnore
}

// refuseDisconnect turns a request the broker will not act on into `0x83`,
// and drops the packet where there is no acknowledgement to carry it.
//
// **A QoS 0 publish has nowhere to put a refusal**, so it is dropped rather
// than answered - the same trade `refuseReserved` makes, and for the second
// reason too: returning a code for one leaves the substrate with no branch
// to take, and the packet carries on down the publish path as though it had
// been accepted.
func (b *Broker) refuseDisconnect(cl *mqtt.Client, pk packets.Packet, why, reason string) error {
	b.log.Warn("refusing a disconnect", "client", b.limits.Loggable(cl.ID), "reason", why)
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return packets.Code{Code: packets.ErrImplementationSpecificError.Code, Reason: reason}
}

// replyDisconnect answers on the Response Topic the request named, which is
// where a seek's answer goes and for the same reason.
//
// **No reserved fallback topic here**, unlike a seek. A seek has one because
// a 3.1.1 consumer cannot name a Response Topic and its position moving
// silently is a data question. Here the request is refused without one
// instead, which a seek cannot do - a seek is sent by the consumer whose own
// position moves, and refusing it would turn "your client library is old"
// into "your consumer cannot start where it meant to".
//
// QoS 0, like a seek's reply: it answers a request somebody is waiting on,
// so a reply that does not arrive is a call that times out at the caller
// rather than a record that goes missing.
func (b *Broker) replyDisconnect(cl *mqtt.Client, pk packets.Packet, outcome string) {
	topic := pk.Properties.ResponseTopic
	if topic == "" {
		return
	}
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:   topic,
		Payload:     []byte(outcome),
		Properties: packets.Properties{
			CorrelationData: pk.Properties.CorrelationData,
		},
	}
	if err := b.writeTo(cl, out); err != nil {
		b.log.Warn("cannot answer a disconnect", "client", cl.ID, "error", err)
	}
}

// refuseKVGet answers a point read whose request is unusable with 0x83.
//
// The split from 0x90 below is by what is wrong rather than by where the
// check sits, because the code is what `saguin_publish_refused_total`
// carries: 0x83 says the request itself cannot be acted on, 0x90 says the
// key is not a topic this broker has a value for. An operator watching one
// number can then tell a fleet mistyping keys from a client that forgot its
// reply path, which are different incidents with different fixes.
func (b *Broker) refuseKVGet(cl *mqtt.Client, why string) error {
	b.log.Warn("refusing a point read", "client", cl.ID, "reason", why)
	return packets.Code{Code: packets.ErrImplementationSpecificError.Code, Reason: why}
}

// refuseKVGetKey answers a point read whose key is wrong with 0x90, naming
// the key. The key is a client string bounded only by max_message_size until
// the length check above has run, so it is logged through Loggable.
func (b *Broker) refuseKVGetKey(cl *mqtt.Client, key, why string) error {
	b.log.Warn("refusing a point read", "client", cl.ID,
		"key", b.limits.Loggable(key), "reason", why)
	return packets.Code{Code: packets.ErrTopicNameInvalid.Code,
		Reason: "the topic asked for is " + why}
}

// reply answers a request on the Response Topic it named. `what` is the
// request being answered, for the one log line this can produce.
//
// **Written to the asking connection rather than published**, which is what
// lets a caller receive it without subscribing, and what stops a third
// client subscribed to the same topic receiving anybody else's answer.
//
// QoS 0, like a seek's reply: the answer is to a request the client is
// waiting on, so a reply it does not receive is a question that times out at
// the caller rather than a record that goes missing.
func (b *Broker) reply(cl *mqtt.Client, topic string, payload, corr []byte, what string) {
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:   topic,
		Payload:     payload,
		Properties:  packets.Properties{CorrelationData: corr},
	}
	if err := b.writeTo(cl, out); err != nil {
		b.log.Warn("cannot answer "+what, "client", cl.ID, "error", err)
	}
}

// seekTime reads the two shapes a seek may name a moment in, and reports
// whether it was one of them.
//
// **A duration always means ago**, sign or no sign, because a moment in
// the future has no records and would mean the same as `-1`. So `-12h` and
// `12h` are one request: the last twelve hours. Days are admitted because
// `-7d` is what an operator actually types; Go's own parser stops at hours.
//
// **An absolute time is RFC 3339**, which is the one format that carries
// its own offset from UTC and cannot be read two ways - `2026-08-14` alone
// is a date in some places and nothing in others, and a broker guessing
// between them would move a consumer somewhere it did not ask for.
func seekTime(s string, now time.Time) (time.Time, bool) {
	if d, ok := seekDuration(s); ok {
		return now.Add(-d), true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// maxSeekDays is the most days a time.Duration holds, whole.
const maxSeekDays = float64(math.MaxInt64 / int64(24*time.Hour))

// dayCount reports whether s is digits, with a fraction of digits after one
// point if it has one.
func dayCount(s string) bool {
	whole, frac, point := strings.Cut(s, ".")
	return decimal(whole) && (!point || decimal(frac))
}

// seekDuration reads a duration, as an absolute length of time however it
// was signed.
func seekDuration(s string) (time.Duration, bool) {
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return 0, false
	}
	// Days first: Go's parser knows nothing above an hour, and a week is
	// the unit an operator reaches for on a channel retaining for one.
	//
	// **Digits, and a fraction if one is given - and nothing else strconv
	// would take.** ParseFloat also reads NaN, infinities, exponents,
	// hexadecimal and digit separators, none of which is a number of days
	// anybody typed; and a count past what a Duration holds converted to
	// whatever the platform makes of the overflow, which on amd64 was a
	// moment in 1734 - a full replay, or below-floor. RFC 0003 refuses as
	// malformed any payload that is not one of its shapes.
	if rest, found := strings.CutSuffix(s, "d"); found {
		if !dayCount(rest) {
			return 0, false
		}
		n, err := strconv.ParseFloat(rest, 64)
		if err != nil || n > maxSeekDays {
			return 0, false
		}
		return time.Duration(n * float64(24*time.Hour)), true
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, false
	}
	return d, true
}

// seekToTime turns a moment into the offset a consumer should resume at,
// or the refusal it earns.
//
// **A time older than the oldest surviving record has two causes, and only
// one of them is safe to answer.** If the floor is still 1 then nothing was
// ever removed: the channel does not go back that far, serving from the
// start is a *complete* answer, and refusing it would be absurd. If the
// floor has moved, retention took records the consumer asked for, and that
// is the same `below-floor` an offset seek gives - for the same reason,
// because the alternative is a consumer that reads from the oldest
// survivor and reports success over the gap (invariant 1).
//
// A time after every record answers the end. There is nothing to replay
// yet, and the consumer receives what arrives from now on, which is what
// asking for a moment in the future means.
func (b *Broker) seekToTime(lg LogStore, at time.Time, floor, next uint64) (uint64, string, []packets.UserProperty) {
	// Whether retention removed what was asked for is a question about the
	// oldest record still here, so it is asked before the search: if the
	// floor has moved and the survivor is younger than the moment wanted,
	// the records in between are gone.
	if floor > 1 {
		oldest, err := lg.ReadFromN(floor, 1)
		if err != nil {
			b.log.Error("cannot read a channel's oldest record for a seek", "error", err)
			return 0, "storage", nil
		}
		below := []packets.UserProperty{{
			Key: "saguin-floor", Val: strconv.FormatUint(floor, 10),
		}}
		switch {
		case len(oldest) == 0:
			// **Retention emptied the channel, and this is the case that
			// gets missed.** There is no surviving record to compare a
			// moment against, so a check written only against the oldest
			// survivor falls straight through and answers with an offset -
			// which is invariant 1's failure exactly: a consumer that asked
			// for the last hour, was placed at the end, and was told
			// nothing about the hour that was removed.
			//
			// Anything in the past is refused here. A moment in the future
			// is not: nothing was removed after it, so the end is a
			// complete answer and means what asking for it meant.
			if at.Before(time.Now()) {
				return 0, "below-floor", below
			}
		case at.Before(oldest[0].Timestamp):
			return 0, "below-floor", below
		}
	}

	off, found, err := lg.FirstAtOrAfter(at)
	if err != nil {
		b.log.Error("cannot search a channel by time for a seek", "error", err)
		return 0, "storage", nil
	}
	if !found {
		return next, "", nil
	}
	return off, "", nil
}

// refuseSeek logs and answers, so that a refusal is visible to the
// operator, to the client that caused it, and on the acknowledgement.
//
// **The reason code is the half that reaches every client.** The reply goes
// to the Response Topic, and a client that set none - which RFC 0003 says is
// allowed and `mosquitto_pub` always is - then had no way to tell a seek
// that applied from one thrown away as malformed: both acknowledged 0x00.
// A consumer that believes it moved and did not reads on from somewhere
// other than where it thinks it is, and nothing anywhere says so.
//
// 0x83 with the reason, which is what a refused point read already answers
// four lines from here. A QoS 0 seek has no acknowledgement to carry it and
// is dropped instead, the same trade refuseReserved makes.
func (b *Broker) refuseSeek(cl *mqtt.Client, pk packets.Packet, name, why string, props ...packets.UserProperty) error {
	// A seek payload is a client string bounded only by max_message_size,
	// and `malformed` is exactly the case where it is not the small number
	// the protocol asks for.
	b.log.Warn("refused a seek", "client", cl.ID, "channel", name,
		"reason", why, "payload", b.limits.Loggable(string(pk.Payload)))
	b.replySeek(cl, pk, why, props...)
	if pk.FixedHeader.Qos == 0 {
		return packets.ErrRejectPacket
	}
	return packets.Code{Code: packets.ErrImplementationSpecificError.Code, Reason: why}
}

// replySeek answers on the Response Topic the client set, echoing the
// Correlation Data it sent, exactly as a queue worker's acknowledgement is
// shaped. A client that set no Response Topic gets no reply and the seek
// still applies - it is a confirmation, not a handshake.
//
// The reply goes at QoS 0. It carries no state the broker must not lose,
// and QoS 1 would take a packet identifier and a slot in the client's send
// window from the records the client is here for.
func (b *Broker) replySeek(cl *mqtt.Client, pk packets.Packet, payload string, props ...packets.UserProperty) {
	topic := pk.Properties.ResponseTopic
	if topic == "" {
		// **The reserved reply topic, for a client that could not ask for
		// one.** A Response Topic is an MQTT 5 property, so a 3.1.1 client
		// has no way to set one and used to get no answer at all - its
		// seek applied and it learned nothing, or its seek was refused and
		// its connection closed with nothing said. An MQTT 5 client that
		// simply did not set one gains the same answer.
		//
		// The topic mirrors the seek it answers and is built by the channel
		// rather than here, so the reserved prefix stays in one place.
		//
		// **And only to a client that subscribed to it, which is the whole
		// of the opt-in.** The reply is written to the socket rather than
		// published, so nothing else would stop it reaching a client that
		// never asked - and a client that never asked is exactly what a
		// Response Topic is: naming one *is* the request. Without this
		// check every MQTT 5 consumer that seeks without naming a reply
		// address is sent an unsolicited publish on a topic it never
		// subscribed to, arriving in the middle of the records it is
		// reading. Measured: two existing seek tests received "2" where
		// they expected "order-2".
		if name, ok := channel.SeekChannel(pk.TopicName); ok {
			if c := b.reg.Get(name); c != nil {
				reply := c.SeekReplyTopic()
				if _, subscribed := cl.State.Subscriptions.Get(reply); subscribed {
					topic = reply
				}
			}
		}
	}
	if topic == "" {
		return
	}
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0},
		TopicName:   topic,
		Payload:     []byte(payload),
		Properties: packets.Properties{
			CorrelationData: pk.Properties.CorrelationData,
			User:            props,
		},
	}
	if err := b.writeTo(cl, out); err != nil {
		b.log.Warn("cannot answer a seek", "client", cl.ID, "error", err)
	}
}

// handleResponse applies a worker's ack or return.
func (b *Broker) handleResponse(cl *mqtt.Client, pk packets.Packet) {
	action := strings.TrimSpace(string(pk.Payload))
	if action != "ack" && action != "return" {
		// The payload is bounded only by max_message_size, a megabyte by
		// default, and a worker looping on a malformed answer writes one of
		// these per attempt. What identifies the mistake is the first few
		// bytes of it, not all of them.
		b.log.Warn("ignoring response: payload is neither ack nor return",
			"client", cl.ID, "topic", b.limits.Loggable(pk.TopicName),
			"payload", b.limits.Loggable(action))
		return
	}

	id := hex.EncodeToString(pk.Properties.CorrelationData)
	if id == "" {
		b.log.Warn("ignoring response: no correlation data", "client", cl.ID)
		return
	}

	b.mu.Lock()
	d, ok := b.deliveries[id]
	b.mu.Unlock()
	if !ok {
		// Superseded: another worker already holds this record. Nothing
		// useful for this worker to do about it (invariant 3).
		b.log.Warn("ignoring response: unknown or superseded delivery", "client", cl.ID)
		return
	}
	// One worker must not resolve another's work.
	if d.holder != cl.ID {
		b.log.Warn("ignoring response: delivery belongs to another session",
			"client", cl.ID, "holder", d.holder)
		return
	}

	// The answer frees this worker for its next job from the queue, offered
	// now rather than at the next tick.
	defer b.offerSoon(d.channel)
	if action == "ack" {
		b.resolve(d)
		return
	}
	b.releaseDelivery(d, "returned by worker", true)
}

func (b *Broker) resolve(d *delivery) {
	b.mu.Lock()
	q := b.queues[d.channel]
	b.mu.Unlock()
	if q == nil {
		return
	}
	it, ok, err := q.Resolve(d.held())
	switch {
	case err != nil:
		// The record was not removed, so it is still work. It will be
		// redelivered, which is at-least-once behaving as promised rather
		// than a record lost.
		b.log.Error("cannot resolve an acknowledged record", "channel", d.channel,
			"offset", d.offset, "worker", d.holder, "error", err)
	case ok:
		// Only when the resolution applied. A superseded acknowledgement
		// below resolves nothing, and counting it would report work done
		// that another worker is still holding.
		if cc := b.counted.forChannel(d.channel); cc != nil {
			cc.acknowledged.Add(1)
		}
		b.log.Debug("acked", "channel", d.channel, "offset", d.offset,
			"worker", d.holder, "attempts", it.Attempts)
	default:
		// Invariant 3: a resolution carrying a Delivery ID that is no longer
		// current changes nothing, **and says so**. This is the branch that
		// says so, and it is the one worth having: the two refusals
		// handleResponse already logs are client mistakes - a payload that
		// is neither verb, correlation data that names nothing - where this
		// is the case the invariant is about. Worker A stalled past its
		// deadline, B took the job, A answered late. The record is safe and
		// the work was done twice, and an operator watching a queue has
		// nothing else that would tell them so. It is also how somebody
		// diagnoses a worker whose own processing time exceeds
		// visibility_timeout, which is the ordinary misconfiguration.
		b.log.Warn("ignoring an acknowledgement for a superseded delivery",
			"channel", d.channel, "offset", d.offset, "worker", d.holder)
	}
	b.forget(d)
}

// expired reports whether a job has outlived its queue's
// `job_expires_after`, which is the one question both places that enforce
// it have to answer the same way.
//
// **The operator's clock is the only one that ends a job.** A publisher's
// own Message Expiry Interval does not: taking work out of a queue is the
// operator's decision, and `job_expires_after` is where they make it. What
// the publisher's TTL does instead is ride the delivery as `saguin-expires`
// and let the worker decide - ignore the job and acknowledge, or do it
// anyway. The same rule holds on every channel type, so the publisher's
// clock removes nothing saguin was asked to store; it deletes only a
// retained value on a broadcast topic, which is MQTT's own store and has no
// consumer position to damage.
//
// A record with no timestamp is never expired. An unset time taken at face
// value is a date in 1754, so a queue of them would be dead-lettered whole
// on the first check - the same rule the retention sweep follows, for the
// same reason.
func expired(c *channel.Channel, r store.Record, now time.Time) bool {
	if c.JobExpiresAfter <= 0 || r.Timestamp.IsZero() {
		return false
	}
	return now.Sub(r.Timestamp) > time.Duration(c.JobExpiresAfter)*time.Second
}

// releaseDelivery returns a held record to the queue, or dead-letters it
// when its attempts are spent.
//
// The decision and both halves of the move belong to the store, which is
// what keeps a record from being removed from the queue without arriving
// in the dead-letter channel (invariant 5). All the broker supplies is the
// policy: how many attempts this queue allows, and what a dead-lettered
// record looks like.
func (b *Broker) releaseDelivery(d *delivery, why string, answered bool) {
	b.releaseWith(d, why, answered, "attempts_exhausted")
}

// releaseWith is releaseDelivery with the dead-letter reason chosen by the
// caller, and with the attempt limit that goes with it.
//
// An expiry dead-letters whatever the attempt count says, because the
// record is out of time rather than out of attempts - a limit of zero is
// how the store is told that, since it dead-letters once the attempts are
// not below the limit. A record that expired on its first delivery is
// dead-lettered on its first delivery.
func (b *Broker) releaseWith(d *delivery, why string, answered bool, reason string) {
	b.mu.Lock()
	q := b.queues[d.channel]
	c := b.reg.Get(d.channel)
	var dlq LogStore
	if c != nil && c.DLQ != nil {
		dlq, _ = storeBehind(b.logs[c.DLQ.Name]).(LogStore)
	}
	b.mu.Unlock()
	if q == nil || c == nil || dlq == nil {
		return
	}

	limit := c.MaxAttempts
	if reason == "expired" {
		limit = 0
	}
	out, ok, err := q.Release(d.held(), time.Now(), answered, limit, store.DeadLetter{
		Log:    dlq,
		Record: b.deadLettered(c, reason),
	})
	b.forget(d)
	if err != nil {
		// Nothing was applied: the record is exactly as it was, delivery
		// state and attempt count included, so the attempt is not spent on
		// a failure that was the broker's (RFC 0003). It stays held until
		// its deadline passes, and this is tried again.
		b.log.Error("cannot release a held record", "channel", d.channel,
			"offset", d.offset, "reason", why, "error", err)
		return
	}
	if !ok {
		// The return half of the same rule. **Only a worker's own answer is
		// worth a Warn**, which is what `answered` says: that is invariant
		// 3's case - a worker overran its deadline, another took the job,
		// and the first answered late. Everything else reaching here is
		// routine cleanup, most often OnDisconnect handing back a delivery
		// whose lease had already expired and been reassigned, and on a busy
		// queue that is ordinary rather than an incident. Reporting it at
		// Warn is the shape this project has an upstream patch about - an
		// ordinary outcome told to an operator as a problem (#516).
		if answered {
			b.log.Warn("ignoring a return for a superseded delivery",
				"channel", d.channel, "offset", d.offset, "worker", d.holder,
				"reason", why)
		} else {
			b.log.Debug("a superseded delivery was already returned",
				"channel", d.channel, "offset", d.offset, "worker", d.holder,
				"reason", why)
		}
		return
	}

	// **Counted here because this is where a release actually applied.**
	// Above it are the two paths that changed nothing - a store that
	// refused, and a superseded delivery - and counting before them would
	// report work returned that is still exactly where it was.
	//
	// `why` is saguin's own word for what happened rather than anything a
	// client chose, which is what makes these four separable at all: a
	// worker's own answer, a deadline that passed, and a job too old to do
	// are three different things an operator acts on differently.
	if cc := b.counted.forChannel(d.channel); cc != nil {
		switch {
		case out.DeadLettered:
			cc.deadLettered.Add(1)
		case why == "visibility timeout":
			cc.redelivered.Add(1)
		case answered:
			cc.returned.Add(1)
		}
		if reason == "expired" {
			cc.expired.Add(1)
		}
	}

	if !out.DeadLettered {
		b.log.Debug("returned to available", "channel", d.channel, "offset", d.offset,
			"reason", why, "attempts", out.Item.Attempts, "of", c.MaxAttempts)
		return
	}
	b.log.Warn("dead-lettered", "from", c.Name, "offset", out.Item.Offset,
		"to", c.DLQ.Name, "dlq_offset", out.Stored.Offset,
		"reason", reason, "attempts", out.Item.Attempts)
	b.pumpAll(c.DLQ, out.Stored)
}

// deadLettered builds the record a spent queue record becomes in its
// companion append channel. It keeps its Message ID (invariant 8) and
// gains metadata as ordinary User Properties, so a stock mosquitto_sub
// reads it with no tooling.
//
// reason is one of the two RFC 0003 names: attempts_exhausted, or expired
// once job_expires_after is enforced on the delivery path.
func (b *Broker) deadLettered(c *channel.Channel, reason string) func(store.Item) store.Record {
	return func(it store.Item) store.Record {
		// The record's own properties first and unchanged, then the
		// dead-letter metadata. Any `saguin-dlq-` the record already carried
		// is dropped rather than appended beside the broker's: this used a
		// map, where writing the key was what removed the old one, and an
		// ordered list would otherwise hand a consumer the same name twice
		// with two different answers.
		var h []store.Header
		for _, one := range it.Headers {
			if strings.HasPrefix(one.Key, "saguin-dlq-") {
				continue
			}
			h = append(h, one)
		}
		add := func(k, v string) { h = append(h, store.Header{Key: k, Value: v}) }
		add("saguin-dlq-channel", c.Name)
		add("saguin-dlq-offset", strconv.FormatUint(it.Offset, 10))
		add("saguin-dlq-attempts", strconv.Itoa(it.Attempts))
		add("saguin-dlq-reason", reason)
		add("saguin-dlq-at", time.Now().UTC().Format(time.RFC3339))
		if !it.FirstSeen.IsZero() {
			add("saguin-dlq-first", it.FirstSeen.UTC().Format(time.RFC3339))
		}
		if !it.LastSeen.IsZero() {
			add("saguin-dlq-last", it.LastSeen.UTC().Format(time.RFC3339))
		}

		// **The topic gains a `__dlq` level and keeps everything else**, at
		// the position the queue's filter carries its `#` or on the end
		// where it carries none. The rewrite is what makes a dead letter
		// readable at all rather than decoration: a subscriber finds a
		// channel through the filters, so a record still carrying the
		// queue's own topic would belong to the queue and be refused to
		// everyone who is not a worker.
		//
		// The channel keeps its derived *name*, which is a snapshot file, a
		// storage key and a seek topic. Only the topic moves.
		out := store.Record{
			MessageID: it.MessageID,
			Topic:     channel.DLQTopic(c.Filter, it.Topic),
			Payload:   it.Payload,
			Headers:   h,
			Timestamp: time.Now(),

			// **The bridge mark is deliberately not carried.** Work that
			// arrived over a bridge and then failed produces a dead letter
			// that is this broker stating a new fact about it, rather than
			// this broker relaying somebody's record - so it is locally
			// originated and an `out` rule may ship it.
			//
			// Carried, the mark would stop a `__dlq` shipping anywhere for
			// exactly the records an operator most needs out, and would say
			// nothing about having done it. The record's own identity still
			// crosses: a dead letter keeps the Message ID of the queue
			// record behind it (invariant 8), which is what ties the two
			// together wherever it ends up.

			// **The publisher is carried, unlike the mark above.** The two
			// fields answer different questions and a dead letter answers
			// them differently: it is this broker's statement about the
			// work, so no bridge brought it - but the message is still the
			// producer's by identity, which is what invariant 8 keeps the
			// Message ID for. A producer subscribed No Local to the `__dlq`
			// is therefore not sent its own failed work back, which is what
			// the flag asked for and what it would expect.
			Publisher: it.Publisher,
		}
		// **The publish properties cross with the record.** A queue
		// delivery is the one place the publisher's Response Topic and
		// Correlation Data are held back, because saguin's own pair carries
		// the reply address and the Delivery ID there - and the dead-letter
		// channel is where they come back, since it is an ordinary append
		// channel with nothing to collide with. A record that reached it
		// stripped would lose the failed job's reply address at exactly the
		// point somebody is reading the channel to find out what failed.
		//
		// Built field by field above, so this is the one line that has to be
		// remembered: a record assembled from parts keeps only the parts
		// somebody listed.
		out.SetProps(it.Props())
		// **What is left of the expiry, not what the publisher wrote.** The
		// record above is stamped with the moment of the move, so copying
		// the interval across unchanged starts its clock again - a job that
		// spent an hour in the queue would arrive here claiming a full hour
		// still to run, and RFC 0003 promises the opposite: the interval a
		// consumer is given is decremented by the time the record has
		// waited.
		//
		// A record whose interval has already run out carries none, which is
		// the same answer a delivery gives. It is still readable: retention
		// on the dead-letter channel is the operator's, and nothing a
		// publisher set removes a record from it.
		out.MessageExpiry = 0
		if left, ok := it.Record.RemainingExpiry(time.Now()); ok {
			out.MessageExpiry = left
		}
		return out
	}
}

// forget drops a delivery once it has been resolved, returned, or
// dead-lettered.
//
// The packet mapping goes with it, and that is not tidiness. OnQosComplete
// is the only other place that removes one, so a delivery whose PUBACK
// never arrives - the whole of the case returnUnsent exists for, and every
// record a worker holds when its session ends - used to leave its entry
// behind for the life of the process. That is one more map keyed by client
// id growing without a bound, which is the shape invariant 13 is about;
// a consumer's `inflight` table goes with the consumer in forgetConsumer
// for the same reason.
// **The mapping is removed only while it still names this delivery.** The
// key is a client id and a packet identifier and both are reused: the
// substrate's packet identifiers wrap back to zero, and a new connection
// under the same client id starts its own counter there. So a delivery
// forgotten late - an expired lease, a dead-lettering, a teardown racing a
// reconnect - would otherwise delete the entry of a *different* delivery
// that had since been given the same number. That record's PUBACK would
// then find nothing in OnQosComplete, so no attempt would be counted and no
// visibility deadline would start; and returnUnsent skips it too, because
// its own guard is the presence of this entry. It is the exact state
// returnUnsent was written to end, reached through the thing that ends it.
//
// **Matched by the attempt it names, not only by pointer.** expireLeases
// releases what the store reports as expired, and the store hands back an
// offset, an epoch and a holder rather than the delivery saguin tracked - so
// a pointer comparison matched nothing, and the tracked delivery stayed in
// the map for the life of the process. Once a worker held one job from a
// queue at a time that was a worker frozen: its hold was never counted down,
// and after its first visibility timeout it was offered nothing again. A
// channel, an offset and an epoch name one attempt (invariant 3), so no
// other delivery can share them.
func (b *Broker) forget(d *delivery) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, x := range b.deliveries {
		if x != d && (x.channel != d.channel || x.offset != d.offset || x.epoch != d.epoch) {
			continue
		}
		delete(b.deliveries, id)
		// The tracked delivery's own worker and packet, which a value the
		// store handed back may not carry. Only what this call removed is
		// counted down, so forgetting one twice cannot free its worker of a
		// job it has since been given.
		b.unhold(x)
		if x.holder == "" {
			continue
		}
		key := packetKey(x.holder, x.packetID)
		if b.byPacket[key] == id {
			delete(b.byPacket, key)
		}
	}
}

// heldBy names one worker's hold on one queue.
type heldBy struct {
	client, channel string
}

// holds reports whether a worker holds a job from this queue. Callers hold
// b.mu.
func (b *Broker) holds(clientID, queue string) bool {
	return b.holding[heldBy{clientID, queue}] > 0
}

// unhold takes a delivery off its worker's count. Callers hold b.mu.
func (b *Broker) unhold(d *delivery) {
	if d.holder == "" {
		return
	}
	k := heldBy{d.holder, d.channel}
	if b.holding[k] <= 1 {
		delete(b.holding, k)
		return
	}
	b.holding[k]--
}

// offerSoon asks Run to offer this queue's work now rather than at the next
// tick. It never blocks: a wake already pending covers this one.
func (b *Broker) offerSoon(queue string) {
	b.mu.Lock()
	b.offerDue[queue] = true
	b.mu.Unlock()
	select {
	case b.offerNow <- struct{}{}:
	default:
	}
}

// queuesFreedLocked is every queue a worker should be offered its next job
// from: those it holds none from,
// when the acknowledgement that has just arrived is what reopened its
// in-flight window.
//
// **An answer can free a worker before its window does.** Paho answers a job
// from its handler and sends the PUBACK after the handler returns, so at
// Receive Maximum 1 the answer's offer (offerSoon) finds the job's own packet
// still filling the window and offers nothing. Nothing else woke the queue
// when the PUBACK came, and the next job waited for the tick: 300 jobs in
// 59.9s, 5.0 a second, measured with examples/support/worker at -window 1.
//
// **Only a window this PUBACK reopened**, which is a window of exactly one
// now: the substrate removes the packet before calling OnQosComplete. A
// worker with room to spare was already offered its job by the answer, and
// an append consumer's every acknowledgement is not a pass over the queues.
//
// **The queues are asked first and the window last**, and not only because
// most acknowledgements come from clients that work no queue. window reads
// the server's options, and a broker with no server - one a unit test drives
// through this hook directly - panicked there with b.mu held, so the
// deferred pumpConsumer waited on the lock for ever and the test hung until
// the package timed out.
//
// The caller holds b.mu, and offers what this returns once it has let go.
func (b *Broker) queuesFreedLocked(cl *mqtt.Client) []string {
	var due []string
	for _, s := range b.subs[cl.ID] {
		if s.channel.Type == channel.Queue && !b.holds(cl.ID, s.channel.Name) {
			due = append(due, s.channel.Name)
		}
	}
	if len(due) > 0 && (b.srv == nil || b.window(cl) != 1) {
		due = nil
	}
	return due
}

// publishLatest hands a changed value to every current subscriber whose filter
// reaches it, and returns: it writes to nobody.
//
// **Invariant 16: a publisher waits on storage and on nothing another client
// does.** This ran inside the publisher's OnPublish, before its PUBACK, and
// wrote the value to every subscriber's socket from there - so a subscriber
// that stopped reading held the publisher for as long as the write deadline
// let it (TestADeafLatestSubscriberDoesNotDelayAPublisher, 2.0s of a 2s
// deadline before this). The value is stored before this is called, so there
// is nothing left for the publisher to wait for: each subscriber's share is
// put in its pending list and that subscriber's own drain writes it
// (runLatest), as append's records always have been.
//
// **Who receives it is decided here, at publish time**, with everything a
// subscription says about it - No Local, Retain As Published, its QoS, its
// identifiers. Only the writing moves.
//
// **A subscriber whose window is full is sent the current value when it has
// room, not every value in between.** Its pending list holds one value per
// topic and a newer one replaces an older one, which is RFC 0003's promise -
// the current value, not every intermediate one - and keeps the list bounded
// by the topics the subscription reaches rather than by how fast a publisher
// writes. Before this a live value that found the window full was dropped and
// recorded as missed.
//
// A subscriber whose connection has gone is not queued for: its session is
// served on resume from its position (OnSessionEstablished), which covers
// this value, and a list kept for it would be memory held for as long as it
// stays away.
func (b *Broker) publishLatest(c *channel.Channel, r store.Record) {
	type target struct {
		id string
		v  pendingValue
	}
	var targets []target
	// Once for this value rather than once per subscriber to it.
	h := channel.PartitionHash(r.Topic)
	b.mu.Lock()
	candidates := b.matchingLocked(c, r.Topic)
	// **Decided pumpBatchCap subscribers at a time, with the lock let go
	// between**, so that a value thousands follow does not hold it for all
	// of them at once. Each subscriber is judged against subs as it stands
	// when its turn comes, which is what a publish arriving a moment later
	// would see anyway.
	for i, id := range candidates {
		if i > 0 && i%pumpBatchCap == 0 {
			b.mu.Unlock()
			b.mu.Lock()
		}
		for _, s := range b.subs[id] {
			if s.noLocal && r.Publisher == id {
				// This subscription asked not to be sent its own publishes,
				// and another of this client's may not have - so the next
				// one is tried rather than the client being skipped.
				continue
			}
			if s.channel.Name == c.Name && channel.Matches(s.filter, r.Topic) &&
				b.wantsLocked(id, s.filter, h) {
				// **Retain As Published is read off the subscription that
				// matched**, and it is why this carries a flag at all: a
				// live change goes out with RETAIN down for everybody else
				// (MQTT-3.3.1-12), and a subscriber that asked to be told
				// what the publisher set is told (MQTT-3.3.1-13). The
				// current-state pass is not this path and always carries
				// the flag - there it is saguin saying "you are catching
				// up" rather than repeating a publisher.
				targets = append(targets, target{id, pendingValue{
					rec: r, qos: s.qos, retain: s.retainAsPublished && r.Retain, live: true,
					carry: b.latestCarryLocked(id, c.Name, r.Topic),
				}})
				break
			}
		}
	}
	lt := b.latest[c.Name]
	b.mu.Unlock()

	// **The newest value of this topic wins, whatever order two publishers'
	// hand-offs run in** - see latestHead. Set before any of this value's
	// enqueues, so every enqueue of an older one after it sees it.
	if r.Offset > 0 {
		head, current := b.advanceHead(c.Name, lt, r)
		if !current {
			// Already overtaken: no enqueue of it could succeed, so none is
			// tried. The enqueue's own check is what makes this correct -
			// this only saves the work.
			b.superseded(c.Name, len(targets))
			return
		}
		for i := range targets {
			targets[i].v.head = head
		}
		// And to every outbound bridge rule on the channel - beside the
		// subscribers rather than among them, so a value nobody here
		// subscribes to still crosses, and after the head so an overtaken
		// value never does.
		b.latestWatched(c.Name, r)
		if hook := latestBeforeQueue.Load(); hook != nil {
			(*hook)(r.Topic, r.Offset)
		}
	}

	// The connections, outside b.mu: the substrate's client table has a lock
	// of its own and nothing here needs both at once.
	conns := make([]*mqtt.Client, len(targets))
	for i, t := range targets {
		if cl, ok := b.srv.Clients.Get(t.id); ok && !cl.Closed() {
			conns[i] = cl
		}
	}

	// **Queued under each subscriber's own lock, not b.mu** - see
	// consumer.latest for what the broker-wide lock cost here.
	type drain struct {
		cl  *mqtt.Client
		con *consumer
	}
	var claimed []drain
	for i, t := range targets {
		cl := conns[i]
		if cl == nil {
			continue
		}
		if con := b.queueLatest(t.id, cl, c.Name, t.v); con != nil {
			claimed = append(claimed, drain{cl, con})
		}
	}

	// Each claimed drain on a goroutine of its own, which is pumpOffPath's
	// shape: claimed before it starts, so a hot topic's second publish finds
	// the claim taken and only asks the running drain to look again rather
	// than starting another.
	for _, d := range claimed {
		b.drains.Add(1)
		go func(d drain) {
			defer b.drains.Done()
			b.runLatest(d.cl, d.con, c.Name)
		}(d)
	}
}

// latestHead is the newest offset of one topic handed to that topic's
// subscribers, and when it last moved.
//
// **Why it exists.** Two publishers setting one topic at nearly the same moment
// store their values in one order and hand them out in whichever order their
// goroutines reach the subscribers: the older value, stored first, can reach
// a subscriber after the newer one has been written to it, and be applied over
// it as current - for good, if that topic is not written again. Measured on
// 2026-09-23 with eight publishers on two topics: 794 to 881 values out of
// order per run with direct writes, with subscribers left holding a replaced value;
// 7 to 23 once values began to wait in a list, which coalesces most
// of it away but not the case where the newer value has already been sent.
//
// **The rule:** a value's publish raises its topic's head before it queues the
// value anywhere, and every enqueue of a value below the head is refused
// (enqueueLatest, under the subscriber's cmu). The bad ending needs the newer
// value written before the older is queued; the newer raised the head before
// it was queued, it was queued before it was written, and the older one's
// enqueue comes after that write - so it sees the head and is refused.
//
// **One atomic per topic, read without a lock**: it is asked on every enqueue,
// per subscriber per value, and a shared lock there is the broker-wide
// acquisition per delivery this path was taken off the same day. About 130
// bytes a topic, measured, bounded by topics recently active: an entry idle
// for latestHeadIdle is swept, and a topic with no entry reads the store's
// current offset once to seed it (advanceHead) - so a publish that outlived
// its topic's sweep is still seen to be overtaken. The idle bound is memory
// policy, not a correctness margin, and not configuration. A deletion is a
// stored value with an offset and raises the head like any other; one that
// stores nothing (offset 0) has no place in the order and skips all of this.
type latestHead struct {
	offset  atomic.Uint64
	touched atomic.Int64 // unix nanoseconds
}

// latestHeadIdle is how long a topic's head is kept with nothing published to
// it. Long enough that a fleet reporting every few minutes keeps its entries,
// so the seeding read is paid by topics that have gone quiet, not by traffic.
const latestHeadIdle = time.Hour

// advanceHead raises a topic's head to a value being handed out and reports
// whether that value is still the newest. No lock held: the store read that
// seeds a new entry is I/O on a sqlite provider.
func (b *Broker) advanceHead(chName string, lt LatestStore, r store.Record) (*latestHead, bool) {
	m, _ := b.latestHeads.LoadOrStore(chName, &sync.Map{})
	heads := m.(*sync.Map)
	v, ok := heads.Load(r.Topic)
	if !ok {
		seed := &latestHead{}
		seed.offset.Store(r.Offset)
		if lt != nil {
			if held, found, err := lt.Get(r.Topic); err == nil && found && held.Offset > r.Offset {
				seed.offset.Store(held.Offset)
			}
		}
		v, _ = heads.LoadOrStore(r.Topic, seed)
	}
	head := v.(*latestHead)
	head.touched.Store(time.Now().UnixNano())
	for {
		cur := head.offset.Load()
		if cur >= r.Offset {
			return head, cur == r.Offset
		}
		if head.offset.CompareAndSwap(cur, r.Offset) {
			return head, true
		}
	}
}

// sweepLatestHeads drops topics' heads that have not moved for latestHeadIdle,
// at most once a minute: a pass over every entry on every housekeeping tick
// would cost more than the entries do.
func (b *Broker) sweepLatestHeads(now time.Time) {
	last := b.headsSwept.Load()
	if now.UnixNano()-last < int64(time.Minute) || !b.headsSwept.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	cutoff := now.Add(-latestHeadIdle).UnixNano()
	b.latestHeads.Range(func(_, m any) bool {
		m.(*sync.Map).Range(func(topic, v any) bool {
			if v.(*latestHead).touched.Load() < cutoff {
				m.(*sync.Map).Delete(topic)
			}
			return true
		})
		return true
	})
}

// queueLatest puts a live value in a subscriber's pending list and, when the
// caller took the claim on that connection's drain of the channel and so must
// start it, returns the record the drain is to run on; nil otherwise.
//
// **Under the subscriber's cmu, on the record publishLatest chose it
// against, when that record is still the registry's** - which it almost
// always is: what waits in the list, or a drain writing it, is what keeps a
// record from being tidied (tidyConsumerLocked). A record that is gone
// was tidied because it held nothing, or dropped because the session ended.
// Either way the value is put on the record the registry holds now, found or
// made under b.mu as every record is - so a value is never queued where no
// drain and no resume will look.
func (b *Broker) queueLatest(id string, cl *mqtt.Client, chName string, v pendingValue) *consumer {
	if con := v.carry.record(); con != nil {
		con.cmu.Lock()
		if !con.gone {
			queued, gone := con.enqueueLatest(chName, v)
			claimed := queued && con.claimLatest(cl, chName)
			con.cmu.Unlock()
			if gone {
				b.superseded(chName, 1)
			}
			if claimed {
				return con
			}
			return nil
		}
		con.cmu.Unlock()
	}
	b.mu.Lock()
	con := b.consumerLocked(id)
	con.cmu.Lock()
	queued, gone := con.enqueueLatest(chName, v)
	claimed := queued && con.claimLatest(cl, chName)
	con.cmu.Unlock()
	b.mu.Unlock()
	if gone {
		b.superseded(chName, 1)
	}
	if claimed {
		return con
	}
	return nil
}

// enqueueLatest puts a value in the consumer's pending list for a channel, and
// reports whether it did, and whether a value this subscriber will now never
// be sent was superseded - the one it replaced, or itself where a newer one
// already waits. Equal offsets are the same value, and supersede nothing.
//
// **One value per topic, and never an older one over a newer one.** A value
// for a topic already waiting replaces it only when its offset is at least as
// high; one lower than what waits is not queued at all, since the subscriber
// is owed the current value and that is not it. The list stays in offset
// order - a replacing value moves to its own offset's place - because that is
// the order it is written in, and the position a resumed session is served
// from assumes nothing below the highest offset written is still unsent
// except what the list holds when the connection goes (forgetConsumer).
//
// The caller holds cmu.
func (con *consumer) enqueueLatest(chName string, v pendingValue) (queued, superseded bool) {
	// **Overtaken on its way here.** A newer value of this topic was handed
	// out after this one was stored and before it reached this list - and may
	// already be written - so this one is not current and never will be.
	// Asked here, under the cmu the newer value's own enqueue took, which is
	// what orders the two (latestHead).
	if v.head != nil && v.head.offset.Load() > v.rec.Offset {
		return false, true
	}
	list := con.latest[chName]
	for i, e := range list {
		if e.rec.Topic != v.rec.Topic {
			continue
		}
		if e.rec.Offset > v.rec.Offset {
			return false, true
		}
		superseded = e.rec.Offset < v.rec.Offset
		list = append(list[:i], list[i+1:]...)
		break
	}
	at := len(list)
	for at > 0 && list[at-1].rec.Offset > v.rec.Offset {
		at--
	}
	list = append(list, pendingValue{})
	copy(list[at+1:], list[at:])
	list[at] = v
	if con.latest == nil {
		con.latest = map[string][]pendingValue{}
	}
	con.latest[chName] = list
	return true, superseded
}

// latestPruneFloor is the length below which a subscriber's pending list is
// not looked through for expired values: a pass over that many costs less
// than the memory it could give back is worth.
const latestPruneFloor = 1024

// latestExpired reports whether the retention of the latest channel named -
// or of the retained store - has removed rec by now: store.PastRetention on
// the clocks its sweep uses (sweepByAge).
func (b *Broker) latestExpired(chName string, rec store.Record, now time.Time) bool {
	if chName == RetainedStoreName {
		return store.PastRetention(rec, deadline(now, b.retainedPeriodNow.Load()), time.Time{})
	}
	c := b.reg.Get(chName)
	if c == nil {
		return false
	}
	return store.PastRetention(rec, deadline(now, c.RetentionPeriod), deadline(now, c.DeletionRetentionPeriod))
}

// dropExpired returns queued without the values latestExpired says are gone.
func (b *Broker) dropExpired(chName string, queued []pendingValue, now time.Time) []pendingValue {
	kept := queued[:0]
	for _, v := range queued {
		if !b.latestExpired(chName, v.rec, now) {
			kept = append(kept, v)
		}
	}
	clear(queued[len(kept):])
	return kept
}

// superseded counts a latest value a subscriber will not be sent because a
// newer one for its topic took its place while it waited
// (saguin_latest_superseded_total).
func (b *Broker) superseded(chName string, n int) {
	if n == 0 {
		return
	}
	if cc := b.counted.forChannel(chName); cc != nil {
		cc.latestSuperseded.Add(uint64(n))
	}
}

// missedLatest records a live update that will not reach a subscriber, so
// that a resumed session is served from below it rather than over it.
//
// **A full window no longer reaches here**: a live value waits in the
// subscriber's pending list, where a newer one replaces it (publishLatest).
// What does is a value the acl_file refuses this subscriber, which is skipped
// rather than left blocking every value behind it. The silence is what this
// exists to end: the value keeps a lower offset than the next
// acknowledgement, the mark would move past it, and the resume filter would
// drop it for good.
//
// It is logged because nothing else says it happened. The subscriber is not
// told - MQTT has no signal for "you missed an update" outside CONNECT, and
// inventing one is an extension this project does not make - so an operator
// whose consumers cannot keep up has this line and nothing else.
//
// **The reason comes from writeLatest rather than from here.** A full
// window is only one of the four ways a value fails to reach the wire, and
// naming it unconditionally made this line assert a cause nothing had
// checked - a value too large for the subscriber was reported as a slow
// consumer, beside the line that had just said otherwise.
func (b *Broker) missedLatest(clientID, chName string, r store.Record, why string) {
	b.latestSeen.lowerMissed(clientID, chName, r.Offset)

	b.log.Warn("a latest update was not delivered. It is re-sent when the session "+
		"resumes or the client subscribes again",
		"client", clientID, "channel", chName,
		"topic", b.limits.Loggable(r.Topic), "offset", r.Offset, "reason", why)
}

// deliverLatest sends a subscriber the current value of every topic its
// filters reach, as retained messages, which is how a client tells state it
// is catching up on from an update that has just happened.
//
// What does not fit the in-flight window waits, and is sent as
// acknowledgements free it.
func (b *Broker) deliverLatest(cl *mqtt.Client, c *channel.Channel, since uint64,
	only string) {
	b.mu.Lock()
	lt := b.latest[c.Name]
	// **The slice each filter declared travels with it**, because this is
	// the half of `latest` that is easy to leave unpartitioned. A member
	// sent the whole of current state on subscribe and then only its share
	// of the updates holds a copy of the key space that starts complete and
	// drifts - which reads as correct on the day it is set up and as data
	// loss a week later. Both halves take the predicate or neither does.
	type reachable struct {
		filter  string
		part    partition
		noLocal bool
	}
	var filters []reachable
	var qos byte
	var deletions bool
	for _, s := range b.subs[cl.ID] {
		if s.channel.Name == c.Name {
			// `only` names the subscription that asked, on a SUBSCRIBE.
			// Empty on a resume, where every restored filter asked.
			if only != "" && s.filter != only {
				continue
			}
			filters = append(filters, reachable{s.filter, b.partitions[cl.ID][s.filter], s.noLocal})
			if s.qos > qos {
				qos = s.qos
			}
			// Any subscription on this channel that asked is enough. The
			// filters are matched as a set below, so a value is sent when
			// any of them reaches it, and a deletion follows the same rule
			// rather than a second one nobody could work out from outside.
			deletions = deletions || s.deletions
		}
	}
	b.mu.Unlock()
	if lt == nil || len(filters) == 0 {
		return
	}

	// reachBy is reach with the publisher in hand, which is what No Local
	// compares against [MQTT-3.8.3-3]. Current state is read from the store
	// long after the publishing connection has gone, so the record carries
	// who wrote it and nothing else can say.
	reachBy := func(topic, publisher string) bool {
		// **One client, many topics here**, which is the mirror of the
		// delivery paths: there the hash is computed once for a topic and
		// asked of many subscribers, and here it is computed once per topic
		// for one subscriber. Only when a slice was actually declared, so
		// the ordinary pass costs nothing.
		var h uint64
		var hashed bool
		for _, f := range filters {
			if !channel.Matches(f.filter, topic) {
				continue
			}
			if f.noLocal && publisher == cl.ID {
				continue
			}
			if !f.part.declared() {
				return true
			}
			if !hashed {
				h, hashed = channel.PartitionHash(topic), true
			}
			if f.part.wants(h) {
				return true
			}
		}
		return false
	}
	// **Which topics exist is asked without a publisher**, because the
	// answer must not depend on one: a topic whose current value this
	// subscriber published is still a topic its filter reaches, and the
	// value is withheld below, per record. The empty string is no client
	// id, so nothing matches it.
	reach := func(topic string) bool { return reachBy(topic, "") }
	match := lt.Match
	if deletions {
		match = lt.MatchWithDeletions
	}
	if hook := latestBeforeState.Load(); hook != nil {
		(*hook)(cl.ID)
	}
	state, err := match(reach)
	if since > 0 {
		// Only what changed. A value's offset moves every time its topic is
		// written, so an offset above the line is a change the consumer has
		// not been given.
		kept := state[:0]
		for _, r := range state {
			if r.Offset > since {
				kept = append(kept, r)
			}
		}
		state = kept
	}
	if err != nil {
		// The subscription stands; the subscriber simply has not been sent
		// the current state. It receives every change from here on, and the
		// next subscribe tries again. Saying nothing would leave somebody
		// looking at a dashboard that never fills in with no reason why.
		b.log.Error("cannot read current state for a new subscriber",
			"client", cl.ID, "channel", c.Name, "error", err)
		return
	}
	// **Withheld here rather than by `reach`**, which is asked about topics:
	// a value this subscriber published is one its No Local subscription
	// asked not to be sent, and only the record says who published it.
	kept := state[:0]
	for _, r := range state {
		if reachBy(r.Topic, r.Publisher) {
			kept = append(kept, r)
		}
	}
	state = kept

	if len(state) == 0 {
		return
	}

	// A channel's subscriptions are in saguin's index, so nothing is
	// stamped here: subscriptionIDs looks up every one of this client's
	// subscriptions whose filter matches the topic, which is the answer MQTT
	// asks for and is more than one identifier when more than one matches.
	//
	// **Merged into what is already waiting, never put in its place.** A live
	// value published since this state was read may already be in the list,
	// and replacing the list would lose it from both - the state was read
	// before it and the list no longer holds it. The merge keeps one value
	// per topic, the newer (enqueueLatest).
	//
	// **And never a value older than one already handed out.** The state was
	// read a moment ago, and a newer value of one of its topics may have been
	// published and written to this subscriber since - its subscription was
	// already in the index - so each is checked against its topic's head as
	// it is merged, as a live value is (latestHead). A head above it means
	// this subscriber was among the newer value's targets.
	if hook := latestAfterState.Load(); hook != nil {
		(*hook)(cl.ID)
	}
	heads := make([]*latestHead, len(state))
	if m, ok := b.latestHeads.Load(c.Name); ok {
		for i, r := range state {
			if v, ok := m.(*sync.Map).Load(r.Topic); ok {
				heads[i] = v.(*latestHead)
			}
		}
	}
	b.mu.Lock()
	// Only for the connection holding the id, as deliverRetained asks.
	if b.owner[cl.ID] != cl {
		b.mu.Unlock()
		return
	}
	con := b.consumerLocked(cl.ID)
	con.cmu.Lock()
	gone := 0
	for i, r := range state {
		v := pendingValue{rec: r, qos: qos, retain: true, head: heads[i]}
		if _, sup := con.enqueueLatest(c.Name, v); sup {
			gone++
		}
	}
	con.cmu.Unlock()
	b.mu.Unlock()
	b.superseded(c.Name, gone)

	b.drainLatest(cl, c.Name)
}

// drainPending resumes every current-state snapshot this client is waiting
// on, whichever channel just freed a slot.
//
// **A snapshot queued while the window was full has nothing in flight**, so
// nothing about that channel will ever be acknowledged - and resuming only
// the acknowledged record's own channel therefore left it waiting for ever.
// A `#` subscription makes that the normal case rather than a corner: the
// append channels are pumped first, they fill the window, and every `latest`
// channel reached after them is queued whole and never sent. The subscriber
// is connected, its subscription is granted, and the current state it asked
// for silently never arrives - measured on a broker with 70,000 records of
// history, where the six values of a schema registry never appeared at QoS 1
// and appeared immediately at QoS 0, which has no window to fill.
//
// It is the same shape as the retained-store bug fixed above it, one lookup
// along: there, the resume stopped because the registry was asked about a
// store that is not a channel; here, because the resume is keyed to a
// channel that has nothing outstanding.
//
// Each value carries the QoS it goes out at, so the resume needs none of its
// own.
func (b *Broker) drainPending(cl *mqtt.Client) {
	con := b.lookupConsumer(cl.ID)
	if con == nil {
		return
	}
	con.cmu.Lock()
	names := latestOwedC(con) // sorted, so two brokers behave alike
	con.cmu.Unlock()
	for _, name := range names {
		b.drainLatest(cl, name)
	}
}

// drainLatest sends as much of a subscriber's pending values on a channel as
// its window allows, on the calling goroutine - or returns at once when that
// connection's drain of the channel is already running, which will see
// whatever the caller came about (runLatest).
func (b *Broker) drainLatest(cl *mqtt.Client, chName string) {
	con := b.lookupConsumer(cl.ID)
	if con == nil {
		return
	}
	con.cmu.Lock()
	claimed := !con.gone && con.claimLatest(cl, chName)
	con.cmu.Unlock()
	if claimed {
		b.runLatest(cl, con, chName)
	}
}

// claimLatest takes the claim on a connection's drain of a channel and reports
// whether the caller now holds it. The caller holds cmu.
func (con *consumer) claimLatest(cl *mqtt.Client, chName string) bool {
	k := latestDrainKey{cl, chName}
	if _, running := con.draining[k]; running {
		return false
	}
	if con.draining == nil {
		con.draining = map[latestDrainKey]struct{}{}
	}
	con.draining[k] = struct{}{}
	return true
}

// runLatest writes a connection's pending values on one channel, in the order
// they are held, while its window has room, and lets the claim go when it has
// nothing more it can send. The caller holds the claim (claimLatest).
//
// **Taken a window's worth at a time**, under one hold of the consumer's cmu,
// and written without it: the lock is paid per batch rather than per value.
//
// **Nothing to send is decided, and the claim let go, in one cmu hold.**
// Whatever would give the drain more to do - a value queued, or a slot an
// acknowledgement freed, whose send quota the substrate raises before the
// hook that comes here takes cmu - is either visible to that decision or
// comes after the claim is gone, and then its caller takes the claim itself.
// So nothing is ever left waiting with nobody to send it, and a caller that
// finds the claim held has nothing to hand over. The one decision made
// outside the lock - writeLatest finding no room - is taken back to the top
// and made again inside it.
func (b *Broker) runLatest(cl *mqtt.Client, con *consumer, chName string) {
	for {
		con.cmu.Lock()
		if con.gone {
			// The session ended and took its list (forgetConsumer); a later
			// record on this client id is not this drain's.
			con.cmu.Unlock()
			return
		}
		queued := con.latest[chName]
		// **Nothing the channel's retention has removed is held or sent**
		// (RFC 0003): a value waiting while the subscriber is short of
		// window outlives its period as easily as one waiting for a bridge's
		// link, and a stale reading is worse than none. Looked through when
		// the list has doubled since last time, so each value queued pays a
		// constant share and the list stays within twice what the channel
		// holds; each value is asked again at its turn (writeLatestBatch).
		if len(queued) >= max(con.latestPruneAt[chName], latestPruneFloor) {
			queued = b.dropExpired(chName, queued, time.Now())
			if len(queued) == 0 {
				delete(con.latest, chName)
			} else {
				con.latest[chName] = queued
			}
			if con.latestPruneAt == nil {
				con.latestPruneAt = map[string]int{}
			}
			con.latestPruneAt[chName] = max(2*len(queued), latestPruneFloor)
		}
		room := b.window(cl)
		n := 0
		for n < len(queued) {
			if sendQoS(queued[n], chName) > 0 {
				if room <= 0 {
					break
				}
				room--
			}
			n++
		}
		if n == 0 {
			con.releaseLatest(cl, chName)
			con.cmu.Unlock()
			return // resumes when a PUBACK frees a slot or a value is queued
		}
		batch := append([]pendingValue(nil), queued[:n]...)
		if n == len(queued) {
			delete(con.latest, chName)
		} else {
			con.latest[chName] = queued[n:]
		}
		con.cmu.Unlock()

		if !b.writeLatestBatch(cl, con, chName, batch) {
			return
		}
	}
}

// writeLatestBatch writes what runLatest took, in order, and reports whether
// the drain should go on. On a value that waits for room it puts that value
// and the rest back and sends the drain round again; on a connection that
// has gone it lets the claim go and stops.
func (b *Broker) writeLatestBatch(cl *mqtt.Client, con *consumer, chName string, batch []pendingValue) bool {
	for i, next := range batch {
		if b.latestExpired(chName, next.rec, time.Now()) {
			continue // the channel no longer holds it: see runLatest
		}
		if next.live {
			if hook := latestBeforeWrite.Load(); hook != nil {
				(*hook)(cl.ID)
			}
		}
		ok, why := b.writeLatest(cl, chName, next.rec, sendQoS(next, chName), next.retain,
			next.ident, next.carry)
		if ok {
			continue
		}
		if cl.Closed() {
			// **What was in hand when the connection went is undelivered, and
			// the resume must not step over it.** The list itself is
			// forgetConsumer's to account for; these left it before the
			// connection did, so they are recorded here. A live one that
			// failed is said, with its reason, as any live miss is.
			if next.live && !waitsForRoom(why) {
				b.missedLatest(cl.ID, chName, next.rec, why)
			}
			if chName != RetainedStoreName {
				for _, v := range batch[i:] {
					b.latestSeen.lowerMissed(cl.ID, chName, v.rec.Offset)
				}
			}
			con.cmu.Lock()
			con.releaseLatest(cl, chName)
			con.cmu.Unlock()
			return false
		}
		if next.live && !waitsForRoom(why) {
			// Skipped rather than put back, and said, with the reason: a live
			// value refused by the acl_file, or too large for this
			// subscriber, is not going to reach it, and put back it would
			// stop every value behind it. Recorded as missed, which holds the
			// resume back below it.
			b.missedLatest(cl.ID, chName, next.rec, why)
			continue
		}
		// Put back, with everything behind it, in front of what was queued
		// since - the list's order is the order it is written in. A value
		// newer than one put back may have been queued meanwhile for the
		// same topic; the put-back one then yields to it.
		if why == whyWindowFull || why == whyWireFull {
			if hook := latestAfterNoRoom.Load(); hook != nil {
				(*hook)(cl.ID)
			}
		}
		con.cmu.Lock()
		gone := 0
		if !con.gone {
			rest := con.latest[chName]
			delete(con.latest, chName)
			for _, v := range append(batch[i:len(batch):len(batch)], rest...) {
				if _, sup := con.enqueueLatest(chName, v); sup {
					gone++
				}
			}
		}
		b.superseded(chName, gone)
		if why == whyWindowFull {
			// **Back to the top, not away.** The window is checked above and
			// again in writeLatest, and another of this client's deliveries -
			// an append record, another channel's value - can take the last
			// slot in between. That "no room" was decided outside cmu, so an
			// acknowledgement arriving since may have found the claim still
			// held and left it to this drain; the top makes the decision again
			// inside the lock (TestALatestValueFindingNoRoomAtItsWriteIsSentWhenRoomComes).
			con.cmu.Unlock()
			return true
		}
		// **The wire is not asked at the top**, which counts the window
		// alone, so going back there on this answer took the same value
		// and was refused it again, for ever. On the connection's own read
		// loop - a SUBSCRIBE's snapshot drains there - it spun without
		// reading the acknowledgements that would have made room: a replay
		// of 1.28 million records stopped at 479, a core busy. So the
		// claim goes, and the acknowledgement that makes room brings the
		// drain back (serveFreed): the wire holds something of this
		// client's, or this write would have been allowed. Asked again
		// here, inside cmu, because the "no" was decided outside it, and an
		// acknowledgement since may have found the claim held and left.
		// Empty, the wire has room for anything, so going back sends.
		if why == whyWireFull && cl.State.Inflight.WireEmpty() {
			con.cmu.Unlock()
			return true
		}
		// A snapshot value refused, no identifier, a share of the wire that
		// is full, or an in-flight set that would not take it: the
		// acknowledgement or acl_file reload that ends it brings the drain
		// back.
		con.releaseLatest(cl, chName)
		con.cmu.Unlock()
		return false
	}
	return true
}

// releaseLatest lets a drain's claim go. The caller holds cmu.
func (con *consumer) releaseLatest(cl *mqtt.Client, chName string) {
	delete(con.draining, latestDrainKey{cl, chName})
}

// writeLatest's answers that mean "not yet": the subscriber has no room for the
// value now and will have when an acknowledgement frees some. runLatest keeps
// a value that failed for one of these waiting; any other answer is a value
// that is not going to reach this subscriber, and a live one is skipped and
// recorded as missed with the reason (missedLatest).
const (
	whyWindowFull = "this subscriber's in-flight window is full"
	whyWireFull   = "this subscriber's session's share of the wire is full"
	whyNoPacketID = "this subscriber has no free packet identifier"
	whyNotTaken   = "this subscriber's in-flight set would not take it"
	whyRefused    = "the acl_file no longer allows this subscriber to read it"
	whyTakenOver  = "this subscriber's session has moved to another connection"
)

// waitsForRoom reports whether a value writeLatest did not send is waiting for
// room rather than refused.
func waitsForRoom(why string) bool {
	return why == whyWindowFull || why == whyWireFull || why == whyNoPacketID || why == whyNotTaken
}

// sendQoS is the QoS a pending value goes out at.
//
// **A retained message goes out at the lower of the two**, which is
// [MQTT-3.8.4-8]: it is a publisher's message handed on later, so the QoS it
// was published at is half of the answer. Measured against Eclipse Paho's
// suite before this: three values retained at QoS 0, 1 and 2 all arrived at
// 2, and the subscriber was left holding an exactly-once exchange for a
// message published at QoS 0.
//
// **A `latest` channel is the exception and takes the subscription's**, which
// is the same distinction that forces the retain flag up on one and not the
// other: that delivery is saguin saying "this is current state" rather than
// repeating a publisher (RFC 0003). It is also what makes a durable consumer
// work - capped to a QoS 0 publish there would be a record delivered with no
// acknowledgement, so the consumer's position could never advance past it.
func sendQoS(v pendingValue, chName string) byte {
	if chName == RetainedStoreName && v.rec.QoS < v.qos {
		return v.rec.QoS
	}
	return v.qos
}

// writeLatest sends one value to one client, registering it in the client's
// in-flight set first when the QoS calls for an acknowledgement - the
// PUBACK can arrive before WritePacket returns. It reports whether the
// value reached the wire, and when it did not, **why**.
//
// The reason is returned rather than assumed by the caller because there
// are four of them and only this function can tell them apart. The caller
// logged one of them as though it were all four: a value refused for being
// larger than the subscriber's Maximum Packet Size was reported as an
// in-flight window that was full, sending an operator to look at consumer
// throughput for a bound they had set themselves.
func (b *Broker) writeLatest(cl *mqtt.Client, chName string, r store.Record,
	qos byte, retained bool, ident int, carry *latestCarry) (bool, string) {
	// **The same question the pump asks, at the other funnel.** Every value
	// this package sends a subscriber passes through here, so one check
	// covers a current-state snapshot, a live update and a catch-up alike -
	// see mayDeliver for why asking at SUBSCRIBE was not enough.
	if !b.mayDeliver(cl, r.Topic) {
		b.noteRefusedDelivery(cl, chName, r.Topic)
		return false, whyRefused
	}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos, Retain: retained},
		TopicName:   r.Topic,
		Payload:     r.Payload,
		Properties: packets.Properties{
			// MQTT 5 section 3.3.4, as the append pump does.
			SubscriptionIdentifier: carry.identifiers(b, cl.ID, chName, r.Topic, ident),
		},
	}
	applyProps(&pk, r, time.Now(), false)
	if chName != RetainedStoreName {
		pk.Properties.User = userProps(r, b.channelNameFor(carry.spans(b, cl.ID, chName, r.Topic), chName))
	} else {
		// **A value from the retained store goes back out as it was
		// published**, which is two rules rather than one and they pull
		// opposite ways.
		//
		// Nothing saguin adds is stamped on it: a broadcast message has no
		// record identity and no position, and giving it one on the way out
		// would make broadcast look like a channel to whichever client
		// happened to subscribe late (RFC 0003 "Retained messages"). That is
		// why userProps is not called here - it builds saguin-id,
		// saguin-offset and saguin-timestamp.
		//
		// But what the *publisher* sent is not saguin's to drop, and
		// skipping the line above dropped it along with the stamps: a device
		// retaining a Content Type or a User Property on its status topic
		// had it seen by whoever was subscribed at that moment and by nobody
		// who subscribed later. The same message read two ways depending on
		// when you arrived, and a `latest` channel - which the RFC calls the
		// same thing under a different name - kept them throughout.
		for _, h := range r.Headers {
			pk.Properties.User = append(pk.Properties.User,
				packets.UserProperty{Key: h.Key, Val: h.Value})
		}
	}

	if qos > 0 {
		if b.window(cl) <= 0 {
			return false, whyWindowFull
		}
		if !b.wireHasRoom(cl, pk) {
			return false, whyWireFull
		}
		id, err := cl.NextPacketID()
		if err != nil {
			return false, whyNoPacketID
		}
		pk.PacketID = uint16(id)
		// Registered only while cl still has its session (WhileOwned), as
		// pumpBatch's are: a takeover copies the table it goes into.
		set := false
		if !cl.WhileOwned(func() {
			if set = cl.State.Inflight.Set(pk); set {
				cl.State.Inflight.DecreaseSendQuota()
				// **Sent, as far as a resume is concerned, from its
				// registration, and in the same hold.** A takeover copies the
				// table under this lock, so a value it carries is one the
				// resumed session sends again, and a resume served from a
				// `sent` that lagged the copy sent it a second time beside
				// that (latestPos.sent). One that then never leaves is still
				// owed to a resume: its write fails only where the connection
				// has gone or is ended for it, and what the drain held then is
				// recorded missed (writeLatestBatch). The retained store keeps no
				// position: RFC 0003 promises a standard broker's behaviour
				// there, so a mark for it would be written and read by nobody.
				if chName != RetainedStoreName {
					b.latestSeen.advanceSent(cl.ID, chName, r.Offset)
				}
			}
		}) {
			cl.State.Inflight.Unclaim(pk.PacketID) // never registered, so released
			return false, whyTakenOver
		}
		if !set {
			cl.State.Inflight.Unclaim(pk.PacketID) // never registered, so released
			return false, whyNotTaken
		}

		carry.register(b, cl.ID, pk.PacketID, pending{
			conn: cl, channel: chName, offset: r.Offset, latest: true})
	}

	// A value a takeover carried - one abandonLatest cannot undo - is the
	// session's from here, as a written one is: the connection that has the
	// session now sends it again.
	if err := b.writeTo(cl, pk); err != nil && (qos == 0 || b.abandonLatest(cl, pk.PacketID)) {
		// A value larger than this subscriber's declared Maximum Packet
		// Size is refused by MQTT itself (MQTT-3.1.2-24, -25) while the
		// connection stays healthy, and it will be refused again on every
		// subscribe - so the subscriber holds a view of current state with
		// a topic missing from it for as long as that value is current, and
		// nothing says so. A live update replaced while it waits for room
		// is a different thing and is permitted (RFC 0003): that value is
		// superseded by the time the subscriber catches up, and this one
		// never is.
		//
		// Same answer as the append path, for the same reason: disconnect,
		// and let it come back able to hold the value. A subscribe re-sends
		// the whole of current state, so the value is offered again as soon
		// as it reconnects able to hold it - and the position now holds the
		// resume back below it too, so a resumed session is offered it as
		// well rather than stepping over it.
		if errors.Is(err, packets.ErrPacketTooLarge) {
			b.log.Warn("disconnecting: a value is larger than this subscriber's Maximum Packet Size",
				"client", cl.ID, "channel", chName, "topic", b.limits.Loggable(r.Topic),
				"maximum_packet_size", cl.Properties.Props.MaximumPacketSize)
			b.disconnect(cl, packets.ErrPacketTooLarge)
			return false, "it is larger than this subscriber's Maximum Packet Size"
		}
		return false, "the write to this subscriber failed"
	}
	return true, ""
}

// latestCarry is what publishLatest already knew about one subscriber when it
// chose it, carried to the write so the write does not take b.mu to learn it
// again.
//
// **Why it exists.** writeLatest runs once per subscriber per value, and it
// used to take the broker-wide lock three times each run: for the
// subscription identifiers, for whether the subscription spans channels, and
// to find the consumer record its in-flight entry goes on. Measured on
// 2026-09-23 - 100 publishers of 16KB into 15 wide latest consumers, the
// broker pinned to four physical cores and the load from a client that could
// not be the limit - latest capped at 9,753 msg/s with two of its eight CPUs
// idle, and writeLatest carried 67% of all lock delay and 67% of all blocking
// in the mutex and block profiles independently. Append, fed the same load,
// kept up at 15,000: it computes the same two facts once per batch into its
// plan and never asks b.mu per delivery.
//
// **Carried, the values are the same values.** identifiersFor and
// spansChannels are the Locked variants below wrapped in b.mu, and
// publishLatest computes them under the b.mu hold it already has while it
// judges the subscriber. What moves is when a subscription is read - at the
// choice rather than at the write - which is what the append path does and
// what publishLatest's own comment already says: a subscriber is judged
// against subs as it stands when its turn comes.
//
// **The consumer record is carried under one rule, and the rule is the
// point.** A carried record is valid only while something pins it - a cursor,
// an in-flight entry, a value waiting in its `latest` list. A latest
// subscriber holds no cursor, so once its values are acknowledged its record
// can be empty; and an empty record is tidied, marked gone and dropped from
// the registry, by the next subscription change, whose replan finds no append
// plan to build. That could land between the choice and the write. **A
// waiting value pins the record** - it is in the `latest` list from the
// moment queueLatest puts it there, under the record's cmu, until the drain
// has written it (releaseLatest) - so the tidy cannot happen while a carry
// waits, and TestAWaitingLatestValuePinsItsSubscribersRecord holds that.
// `gone` stays the defence: on gone the write RE-RESOLVES under b.mu and never
// drops the value. Dropping it would leave a subscriber connected and
// subscribed throughout holding its previous value as current state while the
// broker holds the next one.
//
// Any future carrier of a consumer record inherits this rule. A nil carry,
// or a nil record in one, is the path as it was before: every question asked
// under b.mu at write time.
type latestCarry struct {
	ids     []int
	spanned bool      // whether the subscription spans channels
	con     *consumer // nil when the subscriber had no record at the choice
}

// latestCarryLocked resolves a subscriber's carry. The caller holds b.mu.
func (b *Broker) latestCarryLocked(id, chName, topic string) *latestCarry {
	if chName == RetainedStoreName {
		// The retained store computes neither: its identifiers come from the
		// caller and its user properties from the publisher's headers. Not
		// carried, so that branch is untouched.
		return nil
	}
	return &latestCarry{
		ids:     b.identifiersForLocked(id, chName, topic),
		spanned: b.spansChannelsLocked(id, chName, topic),
		con:     b.lookupConsumer(id),
	}
}

// identifiers is the carried subscription identifiers, or the question asked
// as before when nothing was carried.
func (c *latestCarry) identifiers(b *Broker, id, chName, topic string, ident int) []int {
	if c == nil {
		return b.subscriptionIDs(id, chName, topic, ident)
	}
	return c.ids
}

// spans is the carried answer to whether the subscription spans channels, or
// the question asked as before when nothing was carried.
func (c *latestCarry) spans(b *Broker, id, chName, topic string) bool {
	if c == nil {
		return b.spansChannels(id, chName, topic)
	}
	return c.spanned
}

// record is the carried consumer record, or nil.
func (c *latestCarry) record() *consumer {
	if c == nil {
		return nil
	}
	return c.con
}

// register records a latest value's in-flight entry.
//
// On the carried record under its own cmu when that record is still the
// registry's; otherwise - no carry, no record at the choice, or a record
// tidied since - exactly as before, under b.mu, where setInflightLocked finds
// or makes the record. cmu is let go before b.mu is taken, so the order b.mu
// then cmu is never inverted.
func (c *latestCarry) register(b *Broker, id string, packetID uint16, p pending) {
	if c != nil && c.con != nil {
		c.con.cmu.Lock()
		if !c.con.gone {
			c.con.inflight[packetID] = p
			c.con.cmu.Unlock()
			return
		}
		c.con.cmu.Unlock()
	}
	b.mu.Lock()
	b.setInflightLocked(id, packetID, p)
	b.mu.Unlock()
}

// abandonLatest undoes the bookkeeping for a value that never reached the
// wire.
//
// Nothing is rewound here, and that is a statement about this function
// rather than about the channel. A `latest` consumer does have a position
// now - but it moves on an acknowledgement, so a value that was never
// acknowledged never moved it, and the caller records the miss separately
// so the resume is served from below it. The value itself stays current
// either way, and a new subscribe is answered with the whole of it.
//
// It reports whether it undid it: not where a takeover carried the value to
// the connection that has the session now, whose own acknowledgement settles
// it (WhileOwned, as abandon).
func (b *Broker) abandonLatest(cl *mqtt.Client, packetID uint16) bool {
	retired := false
	if !cl.WhileOwned(func() {
		retired = cl.State.Inflight.Retire(packetID)
		cl.State.Inflight.IncreaseSendQuota()
	}) {
		return false
	}
	// Kept reserved until its record here is forgotten (Inflight.Retire).
	if retired {
		defer cl.State.Inflight.Unclaim(packetID)
	}
	if con := b.lookupConsumer(cl.ID); con != nil {
		con.cmu.Lock()
		forgetInflightOf(con, cl, packetID)
		con.cmu.Unlock()
	}
	return true
}

// forgetInflightOf removes a packet's record only when this connection is
// the one it was registered on. Callers hold the consumer's cmu.
//
// **A superseded connection must not delete the successor's.** The key is a
// client id and a packet identifier, and a successor allocates identifiers
// from 1 just as its predecessor did, so the two collide on the low numbers
// every reconnect uses first. Deleted here, the live session's next PUBACK
// finds nothing and takes the path for a packet saguin never sent - which
// frees the window slot and wakes the append pump, the queue offers and the
// shared drains, but not a queued current-state snapshot. The subscriber
// then holds a snapshot it will never be sent, with room to send it.
//
// An entry with no connection recorded is one from before this rule and is
// removed as it always was, rather than leaked.
func forgetInflightOf(con *consumer, cl *mqtt.Client, packetID uint16) {
	if p, ok := con.inflight[packetID]; ok && p.conn != nil && p.conn != cl {
		return
	}
	delete(con.inflight, packetID)
}

// pumpAll feeds the subscribers of a channel a newly appended record could
// reach. Called when a record is appended, with the topic it was published
// to.
//
// A subscriber whose filters cannot match that topic is left alone. Pumping
// it would read its backlog from the store, find nothing it wants, advance
// its cursor past the record and store the new position - a read and a
// write, per subscriber, per publish, to deliver nothing. That is what made
// a channel with a hundred consumers on their own filters cost a hundred
// times what one consumer costs, however selective the filters were.
//
// What it defers rather than removes is the cursor advance. A subscriber
// skipped here reads past those records the next time one does match it,
// in bounded batches, which is the same work amortised instead of paid per
// record - and pumpBatch already has to do exactly that, because a run of
// non-matching records longer than the window is the ordinary case.
//
// The cost of deferring is paid at a restart. A stored position now tracks
// the last record that matched rather than the head of the channel, so a
// consumer whose filter matches rarely resumes further back and reads
// forward over what it has already declined. It is bounded by the channel
// and it delivers nothing twice - the records it re-reads are ones it never
// wanted - but on a long channel with a very selective filter it is not
// free, and it is the reason to prefer a channel per concern over one
// channel that everybody filters.
//
// A subscriber that is behind and never matches again is never pumped, and
// that is correct: it has nothing to receive. Every other way in - a
// subscribe, a resumed session, an acknowledgement freeing the window -
// calls pump directly and is unaffected.
func (b *Broker) pumpAll(c *channel.Channel, rec store.Record) {
	tail := &rec
	ids := b.announce(c, rec)

	for _, id := range ids {
		if cl, ok := b.srv.Clients.Get(id); ok {
			b.pumpOffPath(cl, c, tail)
		}
	}

	// **And whoever is not an MQTT session.** An outbound bridge rule reads
	// a channel from a stored position and holds no subscription, so the
	// walk above cannot find it - it is woken instead and reads for itself.
	// Signalled after the sessions rather than before, so that a wakeup
	// never arrives for a record the store has not finished with.
	b.woke(c.Name)
}

// announce is the part of pumpAll that decides who a stored record is for:
// the consumers it matches, with the record put on each one's pending list,
// and the record counted as announced on its channel's watermark.
//
// **Three steps, and b.mu is not held across the second.** The candidates
// are found under b.mu (announceBegin); each one's pending list is written
// under its own cmu with b.mu let go (announcing.note); and the watermark is
// moved on under b.mu again (announceDone). Written in one hold of b.mu, the
// second step waited for each candidate's cmu while the broker-wide lock was
// held - against that consumer's own batch commit - and on a record 5,000
// subscribers take, one announcement held b.mu for 137ms, 409ms of it in
// total spent waiting for their locks.
//
// **What keeps it safe, all of it:**
//   - seen() comes before any list is written, so a list started or reset
//     in between - a new cursor, a changed subscription - observes maxSeen
//     at or past this record, and never claims it as declined;
//   - done() comes after every list is written, so through cannot pass the
//     record until each candidate has it on its list;
//   - a subscriber added between the steps is not a candidate, and its list,
//     reset when it subscribed, starts above the record: it reads it rather
//     than stepping over it;
//   - a candidate that unsubscribed between the steps gets the record on
//     its list anyway, which costs it one wasted read - what is sent is
//     judged by takes() at the batch, against the filters it holds then.
//     That is why the second step re-checks nothing but gone, and must not:
//     a re-check of subs there would need b.mu inside a cmu section, which
//     is the lock order this whole arrangement forbids.
//   - two announcements can reach one list in either order;
//     notePendingLocked inserts in order.
//
// The publisher's goroutine still walks every recipient, so a publish to a
// wide channel costs in proportion to its subscribers, as it did - paid now
// in a cmu each, briefly, instead of under b.mu.
func (b *Broker) announce(c *channel.Channel, rec store.Record) []string {
	ids, a := b.announceBegin(c, rec)
	a.note()
	b.announceDone(a)
	return ids
}

// announcing is one announcement between its steps.
type announcing struct {
	b     *Broker
	ch    string
	off   uint64
	w     *watermark
	cands []*consumer
}

// announceBegin is announce's first step, under b.mu.
func (b *Broker) announceBegin(c *channel.Channel, rec store.Record) ([]string, *announcing) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := &announcing{b: b, ch: c.Name, off: rec.Offset, w: b.notified[c.Name]}
	if a.w != nil {
		a.w.seen(rec.Offset)
	}
	var ids []string
	for _, id := range b.matchingLocked(c, rec.Topic) {
		for _, s := range b.subs[id] {
			if s.channel.Name == c.Name && channel.Matches(s.filter, rec.Topic) {
				ids = append(ids, id)
				if con := b.lookupConsumer(id); con != nil {
					a.cands = append(a.cands, con)
				}
				break
			}
		}
	}
	// **A record nobody is waiting for is finished in this hold.** There is
	// no list to write between the steps, so the second hold would buy
	// nothing, and a publish to a channel with no consumers would pay two
	// takes of b.mu where it used to pay one - 12% of a memory publish.
	if len(a.cands) == 0 && a.w != nil {
		a.w.done(rec.Offset)
		a.w = nil
	}
	return ids, a
}

// note is announce's second step: each candidate's list, under its cmu.
func (a *announcing) note() {
	for _, con := range a.cands {
		con.cmu.Lock()
		if cur := con.cursors[a.ch]; cur != nil && !con.gone {
			a.b.notePendingLocked(cur, a.ch, a.off)
		}
		con.cmu.Unlock()
	}
}

// announceDone is announce's third step, under b.mu.
func (b *Broker) announceDone(a *announcing) {
	if a.w == nil {
		return
	}
	b.mu.Lock()
	a.w.done(a.off)
	b.mu.Unlock()
}

// **The partition predicate is deliberately not asked here**, and that is
// worth writing down because it was asked here first. This decides who to
// *wake*; pumpBatch decides what they are sent, from the consumer's own
// cursor, and applying the predicate in both places would be two copies of
// one rule to keep in step. Mutation testing found the copy: neutering the
// predicate left the append tests green, because the enforcement point they
// were actually exercising is pumpBatch.
//
// Waking a consumer that turns out to want nothing costs a pump that finds
// no matching record and returns, which is what pump already does for a
// filter that matches nothing in the batch. Skipping the wake would be the
// riskier half: a consumer with a backlog it does want, not woken because
// *this* record is outside its slice.

// pumpConsumer drains every append channel this consumer subscribes to.
//
// **A freed in-flight slot belongs to the client, not to the channel whose
// acknowledgement freed it.** One consumer can be served by more than one
// append channel, and they share one Receive Maximum - so a channel that
// fills the window leaves the others with no room, and re-pumping only the
// acknowledged packet's channel meant every slot it freed went straight
// back to itself. When that channel ran dry the acknowledgements stopped,
// and the others were never woken again: a consumer frozen part-way
// through, position intact, nothing in any log, until the next record
// published to one of the starved channels happened to wake it.
//
// It was reachable before a channel carried a filter - a client subscribing
// to two append channels' filters had the same shared window - but it took
// two deliberate subscriptions. Now one filter reaches every channel it
// matches, so an ordinary `iot/site/#` does it, and the soak found it on
// the first run that gave one consumer two overlapping channels.
//
// Pumping the others is enough rather than merely likely to help: pump is
// claimed per consumer and channel, so a channel already draining records
// the wake and re-reads before it finishes, and one with nothing to send
// returns at once.
func (b *Broker) pumpConsumer(cl *mqtt.Client) {
	b.mu.Lock()
	var chans []*channel.Channel
	seen := map[string]bool{}
	for _, s := range b.subs[cl.ID] {
		if s.channel.Type != channel.Append || seen[s.channel.Name] {
			continue
		}
		seen[s.channel.Name] = true
		chans = append(chans, s.channel)
	}
	b.mu.Unlock()

	// Outside the lock: pump takes it.
	for _, c := range chans {
		b.pump(cl, c, nil)
	}
}

// pumpOffPath drains a consumer on a goroutine of its own rather than on
// the goroutine that published the record.
//
// **A publisher must not wait on a consumer's socket at all.** Bounding
// that write stopped it waiting for ever, which is what turned one deaf
// consumer into a duplicate on a durable channel and a frozen broker. It
// left a smaller version of the same thing: a publisher less patient than
// `limits.write_timeout` gives up inside that window, concludes the broker
// is gone, reconnects and re-sends a record already stored. The record is
// durable before this is called, so there is nothing for the publisher to
// wait for.
//
// One drain per consumer and channel, claimed before the goroutine starts
// rather than inside it - the claim is what keeps two drains from writing
// one consumer's records out of order, and starting a goroutine to discover
// it cannot have the claim would spawn one per publish under exactly the
// load where that costs most.
//
// A record appended while a drain is running is not lost by finding the
// claim taken: the drain re-reads from the cursor when it is asked to look
// again, which is the same mechanism a full in-flight window already used.
func (b *Broker) pumpOffPath(cl *mqtt.Client, c *channel.Channel, tail *store.Record) {
	cur := b.claimPump(cl, c)
	if cur == nil {
		return
	}
	b.drains.Add(1)
	go func() {
		defer b.drains.Done()
		b.drainPump(cl, c, cur, tail)
	}()
}

// qos0Batch bounds one pass of the QoS 0 drain: how many records are read
// and held as packets at a time. It is not configuration - the only reason
// to change it is to make it worse - and it is a bound rather than a
// tuning knob, so the number matters much less than its existence.
const qos0Batch = 256

// pumpBatchCap is the most records one batch reads, and so the most one
// commit under b.mu registers, whatever the consumer's window says.
//
// **A Receive Maximum is the client's to choose, and the default is
// 65,535.** Sized to the window, a consumer that had fallen behind was
// matched and registered a whole window at a time under the broker-wide
// lock: BenchmarkPumpHoldOnBacklog measured 230-285ms per hold, and the
// further behind the broker fell the longer every hold became. Capped, a
// batch that fills it asks to be called back and the rest follows in the
// next one, with the lock free between them.
const pumpBatchCap = 256

// pumpBetweenReadAndCommit is a test seam, nil in production: when set it
// runs in pumpBatch once the records are read and judged, before the commit
// takes b.mu again.
//
// **It opens exactly one window**, the one the commit's re-checks exist
// for: a SUBSCRIBE adding a filter, a seek moving the cursor, or other
// traffic shrinking the in-flight window, landing after the verdicts were
// made and before they are used. Without the re-checks each of those serves
// the wrong records - the first skips what the new filter matches, which is
// invariant 1's failure. The window is microseconds wide, so no test could
// reach it by timing; no lock is held at this point, so a test reaches it by
// doing the interfering thing inline, from the hook.
var pumpBetweenReadAndCommit atomic.Pointer[func(clientID string)]

// pumpBeforeTakeoverStop is a test seam, nil in production: when set it runs
// in drainPump once it has found its connection taken over with no successor
// registered, before it takes cmu to stop. The successor registering and its
// resume being refused the claim in that window is what the stop's repump
// check is for; the window is microseconds wide and no lock is held in it,
// so a test lets the takeover finish from the hook.
var pumpBeforeTakeoverStop atomic.Pointer[func(clientID string)]

// seekBeforeReply is a test seam, nil in production: when set it runs in
// handleSeek once the seek has moved the cursor and before its reply is
// written - the window a drain started by another client's publish used to
// serve the new position through, ahead of the reply. Nothing is locked at
// this point, so a test drives that drain from the hook.
var seekBeforeReply atomic.Pointer[func(clientID string)]

// latestBeforeWrite, when set, is called by a subscriber's latest drain
// before a live value is written to it, with no lock held: after the value
// was chosen for that subscriber and queued, before it reaches the wire. A
// test holds the write here to land a subscription change inside that wait
// every time rather than by luck.
var latestBeforeWrite atomic.Pointer[func(clientID string)]

// latestAfterSet, when set, is called on the publishing goroutine after a
// latest value is stored and before it is handed to its subscribers, with no
// lock held. A test holds one publish here to let a newer one of the same
// topic overtake it - the gap two publishers of one topic race through.
var latestAfterSet atomic.Pointer[func(topic string, offset uint64)]

// latestBeforeQueue, when set, is called by publishLatest after a value has
// raised its topic's head and before it is queued for any subscriber, with no
// lock held. A test lets a newer value overtake it here - past the head check
// and short of the enqueue, which is what the enqueue's own check is for.
var latestBeforeQueue atomic.Pointer[func(topic string, offset uint64)]

// latestAfterState, when set, is called by deliverLatest after it has read the
// current state it is about to serve and before it merges it into the
// subscriber's list, with no lock held. A test publishes and delivers a newer
// value here, so the state it merges is older than what was sent.
var latestAfterState atomic.Pointer[func(clientID string)]

// latestAfterNoRoom, when set, is called by a subscriber's latest drain after
// a write found no room in the window or on the wire and before the drain takes b.mu to put
// the value back and decide what to do next, with no lock held and the claim
// still held. A test acknowledges here to land a freed slot inside that gap.
var latestAfterNoRoom atomic.Pointer[func(clientID string)]

// latestBeforeState, when set, is called by deliverLatest before it reads the
// current state it is about to serve a subscriber, with no lock held: after
// the subscription is in the index, so live values already reach it. A test
// publishes here to make a live value and the state it is also part of meet.
var latestBeforeState atomic.Pointer[func(clientID string)]

// storedBeforeAnnounced is a test seam, nil in production: when set it runs
// in storeAndDeliver after a record is stored and before pumpAll announces
// it.
//
// **That gap is what the watermark exists for.** Another publisher's record
// can be stored and announced inside it, and a consumer woken by the later
// one must not step over the earlier one, which is stored and matches it but
// is not on its pending list yet. No lock is held here, so a test publishes
// the second record inline.
var storedBeforeAnnounced atomic.Pointer[func(channel string, offset uint64)]

// pump writes as much of one consumer's backlog as its in-flight window
// allows. It is called whenever that window or the backlog can have
// changed: on subscribe, when a record is appended, and when a PUBACK
// frees a slot.
//
// A consumer's position is a cursor into the channel's own log, so an
// offline consumer costs one integer rather than a copy of every record it
// missed - and a backlog larger than the window is not a problem to size
// for, because it is drained a window at a time rather than buffered.
//
// QoS 0 has no window and no acknowledgement, so nothing calls pump again
// when a slot frees, and the backlog has to be drained here. It is drained
// a bounded batch at a time with the lock released between batches, never
// in one read: reading it whole would hold the broker lock across the
// entire channel - a full table scan on a SQLite provider - and build a
// packet per record before writing one of them. That is the unbounded
// output buffer invariant 13 exists to forbid, and on an edge box the
// bound would be the OOM killer.
//
// One pump runs at a time per consumer and channel. Batches are written
// with the lock released, so two pumps - the drain loop, and a publish
// landing between two of its batches - would each read a batch and write
// them concurrently, handing the subscriber offsets out of order. The
// second caller instead asks the running one to look again, which also
// covers a record appended after the drain's last read.
// tail is the record that has just been appended, when pump was called
// because of it, and nil otherwise. A consumer sitting on the live tail is
// then fed from it directly rather than from a read that would return the
// same record - see pumpBatch.
func (b *Broker) pump(cl *mqtt.Client, c *channel.Channel, tail *store.Record) {
	if cur := b.claimPump(cl, c); cur != nil {
		b.drainPump(cl, c, cur, tail)
	}
}

// claimPump takes the right to drain one consumer's backlog on one channel,
// and returns the cursor to drain or nil if another drain already holds it.
// A caller refused the claim has been recorded: the running drain reads
// again before it finishes.
//
// The cursor is returned rather than looked up again by the drain, because
// pumpBatch stops when the cursor in the map is no longer the one it
// started on - that is how a consumer disconnecting mid-drain is noticed.
func (b *Broker) claimPump(cl *mqtt.Client, c *channel.Channel) *cursor {
	// A client with no delivery state is subscribed to nothing that could
	// be pumped: replanLocked makes the entry for any append subscription.
	con := b.lookupConsumer(cl.ID)
	if con == nil {
		return nil
	}
	con.cmu.Lock()
	defer con.cmu.Unlock()
	if con.gone {
		return nil
	}
	cur := b.cursorC(con, cl.ID, c.Name, c.StartAtTail)
	cur.expiresIn = b.sessionExpiry(cl)
	if cur.pumping {
		cur.repump = true
		return nil
	}
	cur.pumping = true
	return cur
}

// drainPump writes the backlog until it is empty or the window is full, and
// releases the claim. It runs on the caller's goroutine for every way in
// that is already the consumer's own - a subscribe, a resumed session, a
// PUBACK freeing a slot - and on a goroutine of its own for a record
// somebody else published.
//
// **It drains for the connection that has the session**, which is not always
// the one it began on: the claim is the client id's, so a takeover's resume,
// finding this drain running, leaves it the work (claimPump), and a seek
// answered on a connection since taken over asks it of that one
// (releaseSeek). Kept on the old connection, it registered nothing there
// (pumpBatch) and the new one waited for the next record to reach it.
func (b *Broker) drainPump(cl *mqtt.Client, c *channel.Channel, cur *cursor, tail *store.Record) {
	for {
		if cl.IsTakenOver() {
			now, ok := b.srv.Clients.Get(cl.ID)
			if !ok || now == cl {
				if hook := pumpBeforeTakeoverStop.Load(); hook != nil {
					(*hook)(cl.ID)
				}
				cur.con.cmu.Lock()
				// **A wake refused since the read is read again, not
				// dropped.** The successor can register and have its
				// resume's claim refused between the read above and this
				// lock, and that resume is the wake nothing repeats: stopped
				// here, its backlog waited for the next record.
				if cur.repump {
					cur.repump = false
					cur.con.cmu.Unlock()
					continue
				}
				cur.pumping = false
				cur.con.cmu.Unlock()
				return
			}
			cl = now
		}
		for b.pumpBatch(cl, c, cur, tail) {
		}
		cur.con.cmu.Lock()
		if !cur.repump {
			cur.pumping = false
			cur.con.cmu.Unlock()
			return
		}
		cur.repump = false
		cur.con.cmu.Unlock()
	}
}

// pumpFilter is one of a consumer's filters on a channel, with the slice it
// declared.
type pumpFilter struct {
	filter  string
	part    partition
	noLocal bool
}

// pumpPlanLocked is what a batch for this consumer may read now: the channel's
// log, the consumer's filters on it, the QoS it is served at and how many
// records it has room for. ok is false where there is nothing to read. The
// filters are gathered only where asked for: deciding what to read needs the
// QoS and the room, and building the list twice a batch is an allocation on
// every acknowledgement. Called with the cursor's consumer's cmu held.
func (b *Broker) pumpPlanLocked(cl *mqtt.Client, c *channel.Channel, cur *cursor) (lg LogStore, plan *channelPlan, room int, ok bool) {
	// The cursor this drain started on is the cursor it finishes on. A
	// disconnect landing in the middle of one deletes the client's entry
	// (forgetConsumer), and re-reading the map here would hand back a fresh
	// cursor with pumping false - so the next pump would start a second
	// drain beside this one, which is the out-of-order delivery the guard in
	// pump exists to prevent, reached from underneath it.
	//
	// A drain whose cursor is no longer the one in the map belongs to a
	// consumer that has gone, and a later consumer on the same client id has
	// its own. There is nothing useful left to write - the connection this
	// is feeding is closed - so it stops here rather than reading another
	// batch, and touches neither the new cursor nor the stored position.
	con := cur.con
	if con.gone || con.cursors[c.Name] != cur {
		return nil, nil, 0, false
	}
	// A seek has moved this cursor and not yet written its reply. The seek
	// pumps once it has, so nothing is lost by stopping here, and a record
	// served now would reach the consumer before the reply naming its
	// position.
	if cur.seeking > 0 {
		return nil, nil, 0, false
	}

	lg = b.logs[c.Name]
	// The slice each filter declared travels with it, as it does for
	// `latest`: a replay is matched against the same predicate as a live
	// record, or a consumer's first read would hold what its later reads
	// never bring it. It is read from this client's plan for the channel,
	// replanLocked's copy of subs and partitions.
	plan = con.plans[c.Name]
	if lg == nil || plan == nil {
		return nil, nil, 0, false
	}

	// QoS 0 has no in-flight window and no acknowledgement, so the position
	// advances as records are written and a batch is as much as is read at
	// once. Records lost to a dropping link are what QoS 0 means.
	room = qos0Batch
	if plan.qos > 0 { // MQTT grants the highest matching subscription's QoS
		room = b.window(cl)
		if room <= 0 {
			return nil, nil, 0, false
		}
		// **Asked before the store is read, as the window is.** A cursor
		// that stopped for want of wire is woken by every record appended
		// to its channel, and each wake read a batch only to throw it away
		// at the first record: a store read per append per stopped
		// consumer. The acknowledgement that makes room pumps again, and
		// one that lands while this drain runs is a repump, which comes
		// back here.
		if cur.wireWait > 0 && cur.next == cur.wireWaitAt {
			if !cl.State.Inflight.WireHasRoom(cur.wireWait, b.limits.SessionQueueBytes/2) {
				return nil, nil, 0, false
			}
			cur.wireWait = 0 // read again; the batch says anew where it stops
		}
	}
	return lg, plan, room, true
}

// pumpBatch writes at most one batch and reports whether to come back for
// another. For a consumer with a window it is normally false - a PUBACK is
// what calls pump again - except when a full batch put nothing in flight,
// which sends no packet to be acknowledged.
func (b *Broker) pumpBatch(cl *mqtt.Client, c *channel.Channel, cur *cursor, tail *store.Record) bool {
	// **Under the consumer's own lock, not b.mu** - see consumer. Nothing
	// here may reach b.mu while it is held: noteRefusedDelivery and abandon,
	// which take it, run after it is let go.
	con := cur.con
	// **The store is read outside the broker's lock.** A read from a sqlite
	// provider is disk I/O, and made under the lock it held back everything
	// else that takes the lock - acknowledgements, subscriptions, a resumed
	// session's re-sends - for as long as the read took. Measured in the
	// link-churn soak: a resuming client's SUBSCRIBE went unanswered for five
	// seconds, and a goroutine dump at the timeout had its connection waiting
	// for the lock behind pumps reading channels for other consumers. So what
	// to read is decided under the lock, the records are read without it, and
	// the batch goes ahead only if nothing moved this consumer's cursor in
	// between. If something did, the read is thrown away and taken again.
	con.cmu.Lock()
	lg, plannedPlan, room, ok := b.pumpPlanLocked(cl, c, cur)
	if !ok {
		con.cmu.Unlock()
		return false
	}
	planned := plannedPlan.pumpFilters()
	from, gen := cur.next, cur.gen
	readFrom := b.skipDeclinedLocked(cur, c.Name, from)
	con.cmu.Unlock()
	limit := min(room, pumpBatchCap)

	// A consumer whose next record is the one that has just been appended is
	// fed from it rather than from the store. The record is already in hand,
	// and asking the store for it is a query per consumer per publish that
	// returns exactly what the caller was holding.
	//
	// The condition is the whole of the safety: from is the offset this
	// consumer needs next, so it can only equal the appended record's offset
	// when everything before it has already been sent. A consumer that is
	// behind, or one for a record appended while another was in flight, does
	// not match and reads as before. Offsets are assigned under the log's
	// lock and the record is stored before this runs, so there is no case
	// where the record in hand is ahead of something the store would have
	// returned first.
	var recs []store.Record
	var err error
	if tail != nil && readFrom == tail.Offset {
		recs = []store.Record{*tail}
	} else {
		recs, err = lg.ReadFromN(readFrom, limit)
	}

	// **Which records this consumer takes is decided here too, outside the
	// lock.** It is a pure function of the records and the filters planned
	// above, and it was most of what a batch held the lock for: a consumer
	// on a narrow filter reads over every record since its last match and
	// matches each one. mayDeliver takes no lock and is asked here, per
	// record, against the live acl_file, as it was inside - the gap between
	// asking and writing was already there, since the write is outside too.
	type verdict struct{ matched, allowed bool }
	verdicts := make([]verdict, len(recs))
	for i, r := range recs {
		if takes(planned, r, cl.ID) {
			verdicts[i] = verdict{matched: true, allowed: b.mayDeliver(cl, r.Topic)}
		}
	}
	if hook := pumpBetweenReadAndCommit.Load(); hook != nil {
		(*hook)(cl.ID)
	}

	// **Registered only while cl still has its session** (Client.Own), as the
	// broadcast drain's are, and for the same reason: a takeover copies cl's
	// in-flight table to the new connection, so a record registered after
	// the copy is on no connection's table, and one retired after it is sent
	// again by both. The read lock that says so is taken before cmu and let go
	// with it: the engine holds it while its hooks take b.mu, which comes
	// before cmu, so waiting for it under cmu would deadlock with a takeover
	// waiting to write. Taken over, the pump goes on for the connection that
	// has the session now (drainPump).
	own, owned := cl.Own()
	if !owned {
		return false
	}
	// Let go once, on every way out: at each exit below, straight after cmu,
	// and here otherwise - a way out that forgot it would hold the read lock
	// until the batch returned rather than for good, when the next takeover
	// of this id would have waited on it forever.
	released := false
	release := func() {
		if !released {
			released = true
			own()
		}
	}
	defer release()
	con.cmu.Lock()
	lg, plan, room, ok := b.pumpPlanLocked(cl, c, cur)
	if !ok {
		con.cmu.Unlock()
		release()
		return false
	}
	filters, qos := plan.pumpFilters(), plan.qos
	// Everything decided outside rests on the cursor and the filters staying
	// as they were planned. The cursor moving is a seek, an abandon or
	// another batch; the filters changing is a SUBSCRIBE or an UNSUBSCRIBE
	// meeting this batch. Either way the verdicts are of the wrong records
	// or against the wrong filters, and the batch is decided again.
	if cur.next != from || cur.gen != gen || !samePumpFilters(filters, planned) {
		con.cmu.Unlock()
		release()
		return true
	}
	// The window is read again because acknowledgements and the substrate's
	// own traffic move it between the plan and here.
	limit = min(room, pumpBatchCap)
	if len(recs) > limit {
		recs = recs[:limit]
	}
	switch {
	case errors.Is(err, store.ErrBelowFloor):
		// A consumer whose stored position retention had passed was told at
		// CONNECT, with Session Present = 0, and restarted at the floor -
		// see OnSessionEstablish. Reaching here means the floor moved under
		// a consumer that was already connected and reading, which no
		// CONNACK can report because there is no CONNECT to carry it.
		//
		// Restarting at the floor is then the only thing left, and it is
		// the failure invariant 1 describes rather than a defect in this
		// line: the consumer receives the rest in order and reports success
		// over what went. The warning is the only notice anyone gets, so it
		// says how much.
		// The alert RFC 0005 builds the catalogue for, counted where it
		// actually fires: invariant 1 holding, which is correct and is also
		// somebody's missing afternoon of telemetry.
		//
		// **Only for a consumer that had a position to lose.** A
		// subscription seeded at the floor above can still arrive here if
		// retention moved in the moment between the two, and it has missed
		// nothing: it had never read a record and had no claim on one. The
		// records it is served are the same either way; what this decides
		// is whether an arrival is reported as a loss.
		//
		// **The naming is done after the unlock below, from these two
		// numbers.** /v1/operations/position-lost is a record with a lock of
		// its own, and taking it here would run it inside a client's cmu on
		// the delivery path - a lock-order edge and a hold this batch does
		// not need. TestNoClientLockSectionReachesTheBrokerLock is what says
		// so, and it said so about this line.
		var passedAt, passedFloor uint64
		if cur.positioned {
			if cc := b.counted.forChannel(c.Name); cc != nil {
				cc.positionLost.Add(1)
			}
			passedAt, passedFloor = cur.next, lg.Floor()
			b.log.Warn("the retention floor passed a connected consumer; it restarts at the floor "+
				"and cannot be told, because there is no CONNECT to carry Session Present = 0",
				"client", cl.ID, "channel", c.Name, "position", cur.next, "floor", lg.Floor(),
				"records_missed", lg.Floor()-cur.next)
		}
		cur.next = lg.Floor()
		clear(cur.outstanding)
		con.cmu.Unlock()
		release()
		// `reported` is false, and that is the whole reason this site is the
		// worst of the three: the consumer cannot be told, so an operator
		// reading the route is the only party who will ever know this device
		// has a hole in its history.
		if passedFloor > passedAt {
			b.passed.add(store.MQTTReader(cl.ID), c.Name, kindConsumer, false,
				passedAt, passedFloor, b.limits.Loggable)
		}
		return true // read again from the floor, outside the lock
	case err != nil:
		// The records could not be read at all. Sending nothing leaves the
		// consumer where it is, so the next PUBACK or publish tries again;
		// treating this as a below-floor read would move it forward over
		// records it never received, which is the one thing that must not
		// happen here.
		b.log.Error("cannot read the channel; this consumer is not being fed",
			"client", cl.ID, "channel", c.Name, "position", cur.next, "error", err)
		con.cmu.Unlock()
		release()
		return false
	}

	// Everything is registered before anything is written, because a
	// PUBACK can arrive before WritePacket returns.
	type outbound struct {
		pk  packets.Packet
		off uint64
		gen uint64 // the era this was registered in; see cursor.gen
	}
	var batch []outbound
	blocked := false // a matching record could not be registered
	refused := ""    // ...because the acl_file no longer allows it here
	// What skipDeclinedLocked stepped over, stepped over here: the cursor
	// is still at from, checked above, and every record below readFrom is
	// one this consumer's filters declined.
	if readFrom > cur.next {
		cur.next = readFrom
	}
	for i, r := range recs {
		cur.next = r.Offset + 1
		// It has read something, so from here the floor passing it is a
		// loss rather than an arrival.
		cur.positioned = true

		if !verdicts[i].matched {
			// Skipped records still advance the cursor, or the same
			// non-matching record is read forever.
			continue
		}

		// **Asked per record, and not once at SUBSCRIBE** - above, outside
		// the lock. The acl_file can be re-read under a running consumer,
		// and a grant taken away must stop the data - see mayDeliver.
		//
		// **Stalled rather than skipped**, which is the same choice this
		// loop makes for an exhausted packet identifier one branch below:
		// the cursor goes back to this record, so nothing steps over a
		// record the consumer did not receive. A skip would have the
		// consumer processing everything after it and reporting success
		// over one it never saw, which is invariant 1's failure - and this
		// consumer may simply have its grant restored a minute later.
		if !verdicts[i].allowed {
			cur.next = r.Offset
			blocked = true
			refused = r.Topic
			break
		}

		pk := packets.Packet{
			// **The flag only where the subscriber asked for it.** An
			// append record is never marked retained otherwise - on this
			// channel a consumer's *position* is what says where it is, so
			// a replayed record carries no flag however old it is - and a
			// subscriber that asked for Retain As Published is told what
			// the publisher set instead (MQTT-3.3.1-13).
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos,
				Retain: r.Retain && plan.retainAsPublished(r.Topic)},
			TopicName: r.Topic,
			Payload:   r.Payload,
			Properties: packets.Properties{
				User: userProps(r, b.channelNameFor(plan.spans(r.Topic), c.Name)),
				// MQTT 5 section 3.3.4: a subscription that carried an
				// identifier has it on every delivery made for it.
				SubscriptionIdentifier: plan.identifiers(r.Topic),
			},
		}
		applyProps(&pk, r, time.Now(), false)

		if qos > 0 {
			if !b.wireHasRoom(cl, pk) {
				cur.next = r.Offset // waits for an acknowledgement to make room
				cur.wireWait, cur.wireWaitAt = mqtt.InflightSize(pk), r.Offset
				blocked = true
				break
			}
			id, err := cl.NextPacketID()
			if err != nil {
				// Said once an episode (Client.Exhausted), not once a record.
				if cl.Exhausted() {
					b.log.Warn("packet identifiers exhausted; consumer will resume from its position",
						"client", cl.ID, "channel", c.Name, "offset", r.Offset)
				}
				cur.next = r.Offset // not sent: do not step over it
				blocked = true
				break
			}
			pk.PacketID = uint16(id)
			if !cl.State.Inflight.Set(pk) {
				cl.State.Inflight.Unclaim(pk.PacketID) // never registered, so released
				cur.next = r.Offset
				blocked = true
				break
			}
			cl.State.Inflight.DecreaseSendQuota()
			cur.outstanding[r.Offset] = true
			con.inflight[pk.PacketID] = pending{
				conn: cl, channel: c.Name, offset: r.Offset, gen: cur.gen}
		}

		batch = append(batch, outbound{pk, r.Offset, cur.gen})
	}
	b.prunePendingLocked(cur)
	con.cmu.Unlock()
	release()

	// Outside the lock, because noteRefusedDelivery takes it - and after the
	// batch is written, because the records before the refused one were
	// registered under a grant this consumer still had.
	if refused != "" {
		defer b.noteRefusedDelivery(cl, c.Name, refused)
	}

	for i, o := range batch {
		if err := b.writeTo(cl, o.pk); err != nil {
			// The write failed, so this record was not delivered - and
			// neither is anything after it, because the loop stops here.
			// Undo the bookkeeping for all of them, not only the one that
			// failed: the rest are registered and unwritten, and left that
			// way they hold packet identifiers and send quota that nothing
			// but a reconnect returns, while the stored position cannot
			// advance past the lowest of them. Undone, the position is at
			// or below every one of these offsets, so they are sent again
			// rather than lost.
			for _, u := range batch[i:] {
				b.abandon(cl, c.Name, cur, u.pk.PacketID, u.off, u.gen)
			}
			// A record larger than this consumer's declared Maximum Packet
			// Size is not a broken link. MQTT forbids sending the packet
			// (MQTT-3.1.2-24, -25) while the connection stays perfectly
			// healthy, so retrying from this offset fails the same way on
			// every publish and every PUBACK and the consumer is never fed
			// again, in silence.
			//
			// Skipping the record instead would leave the consumer
			// processing everything after it in order and reporting success
			// over one it never received - invariant 1's failure with the
			// report going to the operator instead of the consumer.
			// Disconnecting keeps the position honest: abandon has just put
			// it back at this offset, so the consumer gets the record as
			// soon as it reconnects able to hold it, and one that reconnects
			// with the same bound wedges loudly rather than stalling.
			if errors.Is(err, packets.ErrPacketTooLarge) {
				b.log.Warn("disconnecting: a record is larger than this consumer's Maximum Packet Size",
					"client", cl.ID, "channel", c.Name, "offset", o.off,
					"maximum_packet_size", cl.Properties.Props.MaximumPacketSize)
				b.disconnect(cl, packets.ErrPacketTooLarge)
			}
			// And stop draining. abandon rewinds the cursor to this offset,
			// so carrying on would read the same records again and fail on
			// them again, for as long as the link stays broken.
			return false
		}
	}

	// A short read is the end of the backlog; a full one may not be. QoS 0
	// always has to come back for the rest itself. A QoS 1 consumer is
	// normally called back by its own PUBACKs - but a full batch that put
	// nothing in flight sends nothing to be acknowledged, so unless pump
	// comes back here, a run of non-matching records longer than the window
	// stops the consumer being fed, silently and for as long as the channel
	// is quiet. Never after blocked: the record that could not be registered
	// then still cannot be, and coming back would spin on it.
	//
	// A batch cut short by pumpBatchCap rather than by the window is full in
	// the same sense, and its rest is fetched the same way: by the PUBACKs of
	// what it put in flight, or by coming straight back when it put nothing.
	return len(recs) == limit && !blocked && (qos == 0 || len(batch) == 0)
}

// takes reports whether a consumer holding these filters is owed this
// record: one of them matches it, that one did not ask for No Local on the
// consumer's own publish, and the record is in the slice that one declared.
func takes(filters []pumpFilter, r store.Record, clientID string) bool {
	// One record, this one consumer's filters: hashed once, and only where
	// a slice was declared.
	var h uint64
	var hashed bool
	for _, f := range filters {
		if !channel.Matches(f.filter, r.Topic) {
			continue
		}
		// **No Local is asked of the filter that matched, not of the
		// consumer** [MQTT-3.8.3-3]. A client may hold one subscription
		// that asked for it and another that did not, and a record reaching
		// both is owed to the one that did not - so this skips the filter
		// and lets the rest of them answer, rather than skipping the record.
		if f.noLocal && r.Publisher == clientID {
			continue
		}
		if f.part.declared() {
			if !hashed {
				h, hashed = channel.PartitionHash(r.Topic), true
			}
			// **Stepped over, and the cursor still moves past it.** A
			// partitioned consumer holds one position into the channel, so
			// a record outside its slice is skipped exactly as a record
			// matching none of its filters is - see RFC 0003 for what that
			// costs.
			if !f.part.wants(h) {
				continue
			}
		}
		return true
	}
	return false
}

// samePumpFilters reports whether two plans of one consumer's filters are
// the same, so that what was decided against the first holds for the
// second. Order counts, because both come from one walk of subs.
//
// **Every field that decides whether a record is owed is compared**, not
// only the filter: a partition redeclared mid-batch mis-slices by the same
// mechanism as an added filter mis-matches, and No Local changing moves who
// a record is owed to.
func samePumpFilters(a, b []pumpFilter) bool {
	return slices.EqualFunc(a, b, func(x, y pumpFilter) bool {
		return x.filter == y.filter && x.noLocal == y.noLocal &&
			x.part.count == y.part.count && slices.Equal(x.part.indices, y.part.indices)
	})
}

// cursorC returns a consumer's in-flight bookkeeping for one channel, seeded
// from its stored position. The caller holds the consumer's cmu - with or
// without b.mu, since nothing here needs it: b.logs is fixed once the broker
// runs, and the store answers under its own lock.
func (b *Broker) cursorC(con *consumer, clientID, chName string, fromTail bool) *cursor {
	cur, ok := con.cursors[chName]
	if !ok {
		// **A consumer with no stored position starts at the retention
		// floor**, which is what RFC 0003 says it gets: "the stored session
		// is discarded, the client is told its session was not found, and
		// it starts fresh at the channel's current retention floor".
		//
		// It used to start at 1 and let the read be refused, on the
		// reasoning that the refusal is where invariant 1 gets reported.
		// That reported it for consumers that had nothing to lose: every
		// arrival at a channel whose floor had moved - a clean-start
		// subscriber with no session at all among them - was counted as a
		// consumer retention had passed, and logged with the number of
		// records it had "missed" from before it existed. The records
		// served are identical either way, because a read from 1 is
		// restarted at the floor; what differs is whether a first
		// subscription is an incident.
		from := uint64(1)
		positioned := false
		if lg := b.logs[chName]; lg != nil {
			p, has, err := lg.Position(store.MQTTReader(clientID))
			switch {
			case err != nil:
				// Resuming from the beginning re-sends records the consumer
				// may already have, which is at-least-once behaving as
				// promised. Resuming from a guess further on would skip.
				b.log.Error("cannot read a stored position; this consumer resumes from the beginning",
					"client", clientID, "channel", chName, "error", err)
			case has:
				from, positioned = p.Offset, true
			case fromTail:
				// **`start: tail` on the channel**, which is where this
				// question belongs: a channel of commands a constrained
				// fleet acts on wants the tail for every reader, and a
				// channel of events wants the floor for every reader. The
				// operator knows which it is; the broker cannot tell from a
				// client's protocol version, and answering it from there
				// was two rules for one state.
				//
				// Not a capability taken away: a seek to `0` is everything
				// the channel still holds, and a durable session of either
				// protocol may send one.
				from = lg.Next()
			default:
				if f := lg.Floor(); f > from {
					from = f
				}
			}
		}
		cur = &cursor{next: from, positioned: positioned, outstanding: map[uint64]bool{}, con: con}
		b.resetPendingLocked(cur, chName)
		con.cursors[chName] = cur
	}
	return cur
}

// attachLog makes lg the store behind an append channel and starts its
// watermark at the records it already holds, and is the only thing that
// sets either.
//
// **Every record already in the store is taken as announced**, which is
// true only before any publish can arrive: all three callers run at
// startup, before a listener opens. A record stored and not yet announced
// counted this way would be stepped over by consumers it matched.
//
// The caller holds b.mu, or runs before anything else can.
func (b *Broker) attachLog(name string, lg LogStore) {
	// **Enforced, not only written down**: b.logs and b.notified are read
	// without b.mu by a consumer's drain, which is sound only because
	// nothing writes them once the broker runs.
	if b.running.Load() {
		panic("attachLog after Run: b.logs is read without b.mu and must be fixed by then")
	}
	b.logs[name] = lg
	head := lg.Next() - 1
	w := &watermark{}
	w.through.Store(head)
	w.maxSeen.Store(head)
	b.notified[name] = w
}

// resetPendingLocked empties a consumer's pending list and starts it again
// above everything pumpAll has announced so far.
//
// **Above maxSeen, not through.** A record announced above a gap was
// matched against the consumers subscribed then, and this one may not have
// been one of them - on a new subscription, that is the whole point of the
// reset. Starting at through would count that record as declined.
//
// The caller holds b.mu.
func (b *Broker) resetPendingLocked(cur *cursor, chName string) {
	cur.pending = cur.pending[:0]
	cur.pendingSince = ^uint64(0) // nothing can be stepped over without a watermark
	if w := b.notified[chName]; w != nil {
		cur.pendingSince = w.maxSeen.Load()
	}
}

// notePendingLocked adds an offset pumpAll found this consumer's filters
// matching.
//
// The caller holds b.mu.
func (b *Broker) notePendingLocked(cur *cursor, chName string, off uint64) {
	if off <= cur.pendingSince {
		return
	}
	if len(cur.pending) >= pendingCap {
		b.resetPendingLocked(cur, chName)
		if off <= cur.pendingSince {
			return
		}
	}
	// Announcements arrive nearly in order, so this is almost always an
	// append.
	i := len(cur.pending)
	for i > 0 && cur.pending[i-1] > off {
		i--
	}
	if i < len(cur.pending) && cur.pending[i] == off {
		return
	}
	cur.pending = slices.Insert(cur.pending, i, off)
}

// prunePendingLocked drops the offsets below the cursor, which it has
// passed.
//
// **The cursor can be wound back below them** - an abandoned write, a
// seek - and that is why prunedBelow is kept: skipDeclinedLocked steps
// nowhere from below it, so a consumer wound back reads normally until it
// is past it again. Measured against lowestUnacknowledged instead, this
// never lost that case, but it walked every outstanding record under b.mu on
// every commit, which on a backlog is the whole window.
//
// The caller holds b.mu.
func (b *Broker) prunePendingLocked(cur *cursor) {
	low := cur.next
	if low <= cur.prunedBelow {
		return
	}
	i := 0
	for i < len(cur.pending) && cur.pending[i] < low {
		i++
	}
	cur.pending = cur.pending[i:]
	cur.prunedBelow = low
}

// skipDeclinedLocked is where a batch for this consumer may start reading,
// given that it would otherwise start at from: from itself, or further on
// over records it is proven not to want.
//
// **The proof, and nothing short of it.** An offset in (pendingSince,
// through] has been matched against this consumer's filters by pumpAll,
// under b.mu, and put on its pending list if any of them matched. So a run
// of such offsets that are not on the list matched none of them, and can be
// stepped over exactly as reading them would have. It stops at the first
// offset on the list, and at through, since above it an announcement may
// still be on its way. It steps nowhere if from is at or below pendingSince
// (a new cursor, a changed subscription, a restart - the list does not
// cover there) or below prunedBelow (a seek back past what was dropped).
//
// Filters changing between this and the commit are caught there, by
// samePumpFilters, since any change to them resets the list.
//
// A consumer on a narrow filter was reading, and matching, every record
// since its last match on each delivery: about 49 per delivery at 500
// consumers and 273 at 5,000 (BenchmarkPublishAtScale/fanout10).
//
// The caller holds b.mu.
func (b *Broker) skipDeclinedLocked(cur *cursor, chName string, from uint64) uint64 {
	w := b.notified[chName]
	if w == nil || from <= cur.pendingSince || from < cur.prunedBelow {
		return from
	}
	to := w.through.Load() + 1
	i, _ := slices.BinarySearch(cur.pending, from)
	if i < len(cur.pending) && cur.pending[i] < to {
		to = cur.pending[i]
	}
	if to <= from {
		return from
	}
	return to
}

// skipToHead moves a consumer to the end of an append channel, so that the
// subscription about to be served carries nothing that was published before
// it. It is the Retain Handling 2 half of a subscribe, and it is exactly
// what a seek to `-1` does - the same two writes in the same order, under
// the same lock.
//
// **The position is committed, not stepped over.** A consumer asking for
// live only is choosing to skip a backlog, and a skip that lasted one
// connection would hand the whole of it back on the next connect - which is
// the opposite of what it asked for, arriving later, when nothing it did
// explains it. Committing is also what makes the choice visible: the stored
// position moved, so an operator reading /v1/operations/consumers sees a
// consumer at the head rather than one that appears to be permanently
// behind.
//
// **A session that does not outlive its connection stores nothing**, for
// the reason handleSeek refuses a seek from one: the row would be written
// and then swept, and a position nobody can come back to is not a position.
// The cursor still moves, so the subscription itself is served live only,
// which is the whole of what such a client can be promised.
//
// A failure to store is logged and the cursor moves anyway. The alternative
// is serving the backlog to a subscriber that asked not to have it, which
// is the one outcome this cannot produce. **The head is not marked stored
// until it is**, so the flush writes it again on its next tick, and before
// a clean DISCONNECT closes (invariant 18). Marked first, a refused write was
// never tried again: nothing the consumer acknowledged moved its cursor off
// the head, the DISCONNECT's flush found nothing to write, and a crash after
// it replayed the backlog the subscription had asked not to be sent.
func (b *Broker) skipToHead(cl *mqtt.Client, c *channel.Channel) {
	b.mu.Lock()
	lg := b.logs[c.Name]
	b.mu.Unlock()
	if lg == nil {
		b.log.Error("an append channel has no log", "channel", c.Name)
		return
	}
	head := lg.Next()

	// The same critical section handleSeek takes, and for its reasons: the
	// flusher decides what to write while holding b.positions, so a stored
	// head cannot be overwritten by a position read before it, and the era
	// bump has to happen in one piece or a record from the old one is
	// acknowledged as though it belonged to the new.
	b.positions.Lock()
	defer b.positions.Unlock()
	// **Only the connection holding the id moves its position**, which is
	// handleSeek's rule for the same race. A SUBSCRIBE is read on its own
	// connection and this runs after its SUBACK, so one handled on a
	// connection another had since taken the id from - with a clean start,
	// or taken and left - planted a head position for the successor's
	// session, which then resumed from it and was served none of its backlog
	// while being told it had missed nothing (invariant 1). Asked under
	// b.positions it is ordered against the clean start's discard of the old
	// session's positions, as the seek is.
	//
	// **And in the same hold of b.mu that moves the cursor.** Asked in a
	// hold of its own, with the cursor made in a later one, a takeover
	// between the two had the successor's session handed this connection's
	// cursor at the head.
	b.mu.Lock()
	if b.owner[cl.ID] != cl {
		b.mu.Unlock()
		b.log.Info("a subscription asking for no replay moved nothing: another connection holds "+
			"its client id", "client", b.limits.Loggable(cl.ID), "channel", c.Name)
		return
	}
	// cursor rather than a map read, because a first subscription has none
	// yet: built here it starts at the head instead of at the channel's
	// `start`, which is the case with no stored position.
	con := b.consumerLocked(cl.ID)
	con.cmu.Lock()
	cur := b.cursorC(con, cl.ID, c.Name, c.StartAtTail)
	cur.next = head
	clear(cur.outstanding)
	cur.gen++
	stores := b.sessionExpiry(cl) > 0
	if !stores {
		cur.saved = head // nothing to store, so nothing for the flush to write
	}
	cur.positioned = true
	con.cmu.Unlock()
	b.mu.Unlock()

	if stores {
		if err := lg.SavePosition(store.Position{
			Reader:    store.MQTTReader(cl.ID),
			Offset:    head,
			LastSeen:  time.Now(),
			ExpiresIn: b.sessionExpiry(cl),
		}); err != nil {
			b.log.Error("cannot store the position of a subscriber asking for no replay; "+
				"it is written again with the next flush",
				"client", cl.ID, "channel", c.Name, "error", err)
			return
		}
		con.cmu.Lock()
		if con.cursors[c.Name] == cur && cur.saved < head {
			cur.saved = head
		}
		con.cmu.Unlock()
	}
}

// wireHasRoom says whether a channel delivery saguin writes itself may go on
// the wire now, by the rule the engine's own deliveries and the broadcast
// drain keep: no more than half the session's bound written and
// unacknowledged, and one always goes when nothing is (RFC 0002 "How much a
// session may hold").
//
// **A channel's record that does not go now waits where it is**: an append
// consumer's cursor holds and a `latest` value stays pending, and the next
// acknowledgement pumps again. Nothing is given up, because a channel's own
// record never is. Without this the window alone sized a batch, and a
// consumer that read and never acknowledged held Receive Maximum x a record's
// size: 16 MiB at a 1 MiB bound, measured.
//
// WireHasRoom rather than MayWrite: MayWrite also waits behind the engine's
// withheld deliveries, which is their order to keep, not a channel's.
//
// **Asked, then taken, in separate critical sections**: the caller asks
// this, takes a packet identifier, and only then sets the delivery in
// flight (pumpBatch, writeLatest). One pump runs per consumer and channel,
// so a client subscribed to N channels whose records arrive together can
// pass half its bound by one record per channel, once - up to N x
// `max_message_size` - after which every pump is stopped. The engine's
// sessionHasRoom and the broadcast drain's memberHasRoom have the same
// shape and the same overshoot. It is bounded, and is the price of not
// holding the connection's lock across a store read.
func (b *Broker) wireHasRoom(cl *mqtt.Client, pk packets.Packet) bool {
	return cl.State.Inflight.WireHasRoom(mqtt.InflightSize(pk), b.limits.SessionQueueBytes/2)
}

// window is how many more packets may be in flight to this client. It is
// measured against everything the server has outstanding to it, not only
// saguin's own records, because they share one packet identifier space and
// one Receive Maximum. Callers hold b.mu.
func (b *Broker) window(cl *mqtt.Client) int {
	max := int(cl.Properties.Props.ReceiveMaximum)
	if max <= 0 {
		max = 65535 // section 3.1.2.11.3: a Receive Maximum not given is 65,535
	}
	return max - cl.State.Inflight.Len()
}

// flushPositions writes every consumer position that has moved since it was
// last written, and is what actually stores them.
//
// The write used to happen on the publish path, under the broker-wide lock,
// once per consumer per batch - and on a sqlite provider a position is a
// transaction, so a channel with four consumers spent four commits per
// publish on bookkeeping. Removing it from that path is worth about half
// the throughput of a SQLite channel with consumers on it.
//
// Deferring is safe in the one direction that matters. A stored position
// that lags makes a consumer resume earlier than it had reached and receive
// records again, which at-least-once permits and QoS 0 permits absolutely.
// A position that ran *ahead* would skip records and report success, which
// is invariant 1's failure - and it cannot happen here, because what is
// written is the lowest offset the consumer has not acknowledged, computed
// from the cursor at the moment of writing.
//
// How far it may lag is one tick of the broker's clock, plus a flush at
// each of the two moments a lag would otherwise be lost: the consumer going
// away, and the broker shutting down.
func (b *Broker) flushPositions() {
	b.flushCursors("")
}

// flushPositionsOf is flushPositions for one client's cursors, which is
// all a consumer going away has to write.
//
// **It used to flush every cursor in the broker**, a walk of the whole
// population under b.mu on each disconnect. One leaving was cheap; many
// leaving at once was quadratic. The scale run lost 19,266 consumers to
// write_timeout inside a minute, and the 20,001 disconnects then queued on
// b.mu each held a full scan to do: BenchmarkDisconnectStorm measured 47.6s
// for 20,000 to settle, with a 26s wait for anyone else wanting the lock.
// The other cursors lose nothing by waiting for the tick; their consumers
// are still connected, and a lagging stored position is a replay, never a
// skip.
func (b *Broker) flushPositionsOf(clientID string) {
	b.flushCursors(clientID)
}

// flushCursors writes what flushPositions says, for one client's cursors or,
// with clientID empty, for everybody's.
func (b *Broker) flushCursors(clientID string) {
	type pending struct {
		client  string
		channel string
		cur     *cursor
	}

	// Which cursors are worth a write, as a first pass only. What each one
	// says is read again below, under the lock that orders the writes.
	//
	// **Under each consumer's own lock, and not b.mu** - which a tick walking
	// every cursor held once for the whole population.
	var due []pending
	now := time.Now()
	collect := func(id string, con *consumer) {
		con.cmu.Lock()
		defer con.cmu.Unlock()
		for chName, cur := range con.cursors {
			if cur.lowestUnacknowledged() == cur.saved {
				continue
			}
			due = append(due, pending{id, chName, cur})
		}
	}
	if clientID != "" {
		if con := b.lookupConsumer(clientID); con != nil {
			collect(clientID, con)
		}
	} else {
		b.consumers.Range(func(k, v any) bool {
			collect(k.(string), v.(*consumer))
			return true
		})
	}

	// Held across the writes, so that a drop cannot land between the batch
	// being collected and its rows being written. The check below is the
	// other half: the lock orders the two, and the check decides which of
	// them won.
	b.positions.Lock()
	defer b.positions.Unlock()

	for _, p := range due {
		con := p.cur.con
		con.cmu.Lock()
		lg := b.logs[p.channel]
		// **What this cursor says is read here, not at collection**, and
		// b.positions is held across the read and the write both.
		//
		// A seek is why. It stores where the consumer asked to be and moves
		// the same cursor object to match, so identity alone still reads as
		// live while the offset collected a moment earlier is the one value
		// that must not be written - and the gap between collecting and
		// writing is wide, because this loop waits on b.positions, which a
		// sweep can hold for tens of milliseconds. Reading under the locks
		// handleSeek stores and moves the cursor under - b.positions, and
		// the consumer's cmu, which is where every cursor field lives now -
		// leaves no schedule for it: either this read happens first and the
		// seek's write lands after it, or the seek is complete and this
		// reads where it put the consumer.
		//
		// The cursor may also be gone: a session that expired, a clean
		// start, or a consumer that went away takes it out of this map, and
		// writing then puts back the row a drop had just removed - which
		// nothing would remove again, since the client has left the
		// substrate's table and its expiry cannot fire twice.
		live := !con.gone && con.cursors[p.channel] == p.cur
		at := p.cur.lowestUnacknowledged()
		pos := store.Position{
			Reader:    store.MQTTReader(p.client),
			Offset:    at,
			LastSeen:  now,
			ExpiresIn: p.cur.expiresIn,
		}
		unchanged := at == p.cur.saved
		con.cmu.Unlock()
		if lg == nil || !live || unchanged {
			continue
		}
		// Outside b.mu on purpose: this is the write the publish path used
		// to do while holding it. Under b.positions, which is what orders it
		// against a drop.
		if err := lg.SavePosition(pos); err != nil {
			// The consumer keeps reading - its live position is the cursor,
			// and that is in memory either way. What is lost is where it
			// resumes after a restart, which is a replay rather than a skip.
			// Left unmarked, so the next tick tries again.
			b.log.Error("cannot store a consumer position", "client", pos.Reader,
				"channel", p.channel, "error", err)
			continue
		}
		con.cmu.Lock()
		p.cur.saved = at
		con.cmu.Unlock()
	}
}

// sessionExpiry is how long a client's session outlives its connection,
// and so how long its stored position does.
func (b *Broker) sessionExpiry(cl *mqtt.Client) time.Duration {
	if !legacyClient(cl) {
		return time.Duration(cl.Properties.Props.SessionExpiryInterval) * time.Second
	}

	// **3.1.1 has no Session Expiry Interval, so its clean flag is the whole
	// of the question** (RFC 0003 "Durable consumers"). `cleanSession = 1`
	// is a session that ends with the connection and stores nothing;
	// `cleanSession = 0` is a durable one and is given
	// `limits.max_session_expiry`, which is the same ceiling that caps what
	// an MQTT 5 client may ask for and is the only value there is to give -
	// such a client has no way to name a shorter one.
	//
	// **Three questions are answered by this one function**, which is why
	// the version test is here rather than at each of them: whether a seek
	// is allowed at all, how long a stored position outlives the connection,
	// and what the live cursor carries. Returning zero for every 3.1.1
	// client, as this did before, made all three say the same wrong thing -
	// no durable position, and a seek refused for want of a session.
	if cl.Properties.Clean {
		return 0
	}
	return time.Duration(b.limits.MaxSessionExpiry) * time.Second
}

// abandon undoes the bookkeeping for a record that was registered but
// never reached the wire.
func (b *Broker) abandon(cl *mqtt.Client, chName string, cur *cursor, packetID uint16, offset, gen uint64) {
	// Only a QoS 1 record took a packet identifier and a slot of send
	// quota; a QoS 0 one took neither. Returning a slot that was never
	// taken raises the quota above the client's Receive Maximum - mochi
	// clamps it at the maximum so it cannot run away, but with a real
	// record in flight at that moment one packet gets past the bound.
	if packetID != 0 {
		// **Retired only while cl still has its session** (WhileOwned). One a
		// takeover carried is the new connection's: it is sent again there
		// under this identifier, and its acknowledgement there settles this
		// record and the cursor (OnQosComplete), so nothing here is undone -
		// rewound as well, it went out a second time under another.
		retired := false
		if !cl.WhileOwned(func() {
			retired = cl.State.Inflight.Retire(packetID)
			cl.State.Inflight.IncreaseSendQuota()
		}) {
			return
		}
		// Kept reserved until its record is forgotten below (Inflight.Retire);
		// deferred first, so it runs after cmu is let go.
		if retired {
			defer cl.State.Inflight.Unclaim(packetID)
		}
	}

	con := cur.con
	con.cmu.Lock()
	defer con.cmu.Unlock()
	forgetInflightOf(con, cl, packetID)

	// The drain's own cursor, not whichever one the map holds now. A
	// disconnect between reading this batch and failing to write it replaces
	// the map's cursor, and rewinding *that* one would drag a later consumer
	// backwards over records it has already been sent - or, since this
	// cursor is usually the further on of the two, save a position ahead of
	// where that consumer has reached and skip the records in between.
	//
	// Rewinding the drain's own cursor is harmless once it is orphaned:
	// nothing reads it again, and flushPositions walks the cursors in the
	// map, so an orphan is never written to the store either. That guard
	// used to be a condition here and is now structural.
	//
	// A seek since this record was registered is the other way this cursor
	// stops being the right one to rewind, and it is not orphaned - it is
	// the same cursor in a later era. What was outstanding is already
	// discarded and next is where the consumer asked to be, so rewinding to
	// this offset would drag it back over the seek; and this offset may have
	// been re-sent since, so removing it from outstanding would free a
	// delivery that is still live. See cursor.gen.
	if cur.gen != gen {
		return
	}
	delete(cur.outstanding, offset)
	if offset < cur.next {
		cur.next = offset
	}
}

// forgetSessionDeliveriesLocked drops the delivery state that belongs to a
// session rather than to a connection: where the consumer had reached in
// each channel, and the append records still awaiting acknowledgement.
// Callers hold b.mu.
//
// **Three things end a session and every one of them has to come here**,
// which is why this is a function rather than two lines written out where
// they are needed: a clean start, an expiry at disconnect, and the sweep
// for a session whose client never came back. A list of the ones somebody
// remembered is how one defect becomes three.
//
// **That third one is what bounds this** (invariant 13). Between a
// disconnect and the sweep, what is held is one cursor per channel and one
// entry per unacknowledged packet, for each session that has not expired -
// the same population, with the same cap in `limits.max_session_expiry`,
// that already bounds the stored positions and a `latest` consumer's mark.
// The in-flight entries are bounded a second time by the client's Receive
// Maximum, which is what limits them while the connection is live too.
func (b *Broker) forgetSessionDeliveriesLocked(clientID string) {
	b.forgetConsumerLocked(clientID)
}

// forgetConsumer drops what a departed consumer's *connection* held, and
// keeps what its *session* holds.
//
// **The session's half is endSession's, and that is the whole of the
// contract.** A disconnect keeps the cursor and the in-flight append records:
// the substrate re-sends the unacknowledged packets on resume, and their
// acknowledgements have to find the offsets they belong to. A session that
// ends - at an expiry, at a disconnect that discards it, at a clean start or
// where retention passed it - takes both with it in endSession, because a
// fresh session must not inherit the position of one it has been told it did
// not have.
//
// **Deleting the cursor on every disconnect served every in-flight record
// twice.** Rebuilt from the stored position - the lowest *unacknowledged*
// offset - the pump re-sent exactly what the session was already re-sending,
// with no DUP flag on the second copy. It is bounded by the consumer's
// Receive Maximum and paid on every reconnect, and it compounds: the extra
// copies cost the consumer time, so it falls behind, so more records are in
// flight at the next drop. Measured at a window of 256 with the link
// dropping once a second, a consumer that did not deduplicate never caught
// up - 55,000 deliveries for 713 records of progress, where the same
// consumer drained the backlog in 18s once the cursor was kept.
//
// The stored position is still the lowest unacknowledged offset and is
// still left alone here. That is what a *restart* resumes from, where
// nothing re-sends anything and a position above an unacknowledged record
// would step over it (invariant 1).
//
// **A `latest` subscriber's undrained snapshot is the exception**, and it
// is why this function touches a position at all. A subscribe queues the
// whole of current state and drains it a window at a time; whatever is
// still queued when the consumer goes is undelivered, and it is thrown
// away here. Nothing else held the mark back to it, so a resumed session
// stepped straight over it whenever those offsets were below what the
// consumer had already acknowledged - which a client reaches by widening
// its subscription to a prefix whose topics were written earlier, an
// entirely ordinary thing to do. Five rounds out of five, silently: the
// values were in the store, a fresh subscriber could read them, and only
// the resumed session was denied them (invariant 1).
//
// The comment on latestPos used to say the drops were the only records
// undelivered and not coming back. This is the second population, and it
// is recorded through the same mark rather than a second one.
func (b *Broker) forgetConsumer(cl *mqtt.Client) {
	clientID := cl.ID
	// Collected under the lock and logged after it. The health endpoint
	// takes this mutex with a timeout, and logging is disk I/O - RFC 0005
	// promises no path holds it across any.
	type undrained struct {
		channel string
		offset  uint64
		count   int
	}
	var held []undrained

	b.mu.Lock()
	// The same question as OnDisconnect's fast path, asked again inside the
	// lock that protects what is about to be deleted. A superseded teardown
	// that got this far has already lost ownership and removes nothing.
	if b.owner[clientID] != cl {
		b.mu.Unlock()
		return
	}

	// What the connection was still owed on latest channels goes with it,
	// and holds its resume below the lowest of it. Values a drain had
	// already taken are the drain's to record (writeLatestBatch).
	if con := b.lookupConsumer(clientID); con != nil {
		con.cmu.Lock()
		for chName, queued := range con.latest {
			// The retained store keeps no position - a resumed session is
			// sent nothing from it, which is what a standard broker does - so
			// there is no mark to hold back. A partly drained retained
			// snapshot is answered by subscribing again.
			if chName == RetainedStoreName || len(queued) == 0 {
				continue
			}
			lowest := queued[0].rec.Offset
			for _, q := range queued[1:] {
				if q.rec.Offset < lowest {
					lowest = q.rec.Offset
				}
			}
			b.latestSeen.lowerMissed(clientID, chName, lowest)
			held = append(held, undrained{chName, lowest, len(queued)})
		}
		con.latest = nil
		con.cmu.Unlock()
	}

	// OnSubscribed clears this on its way out, so an entry here belongs to
	// a client whose connection ended between the two hooks. Left behind it
	// is one map per such client, for the life of the process.
	//
	// This one goes on every disconnect rather than with the session: it
	// records which filters the client held before the SUBSCRIBE now being
	// answered, which is Retain Handling 1's question and is asked and
	// answered within one connection.
	delete(b.existed, clientID)
	b.mu.Unlock()

	// One line per channel rather than one per value: a snapshot of ten
	// thousand topics would otherwise be ten thousand lines about a single
	// disconnect.
	for _, u := range held {
		b.log.Info("a consumer left before its current state finished sending; "+
			"its resume is held back so the rest is re-sent",
			"client", clientID, "channel", u.channel,
			"undelivered", u.count, "resuming_below", u.offset)
	}
}

// record builds the stored record from a publish. c is the channel it is
// bound for, or nil for broadcast; from names the bridge the publish
// arrived on, or is empty for a record this broker originated.
func (b *Broker) record(pk packets.Packet, c *channel.Channel, from, by string) store.Record {
	var h []store.Header
	id := ""
	// This broker's own receipt time. Nothing a publisher sends can move it:
	// the time a record was received here is this broker's to state.
	at := time.Now()
	for _, u := range pk.Properties.User {
		if u.Key == "saguin-id" {
			id = u.Val
			continue
		}
		// **Every reserved property a publisher sent is stripped**, so that
		// nothing on the wire can forge a Message ID, an offset, a receipt
		// time or dead-letter metadata that a consumer would read as this
		// broker's. What a delivery carries in that space is stamped on the
		// way out, from the record's own fields.
		if strings.HasPrefix(u.Key, reservedPrefix) {
			continue
		}
		// **Every one of them, in order, repeats included.** This kept a
		// map and dropped all but the last of a repeated name, which is a
		// silent loss of data a publisher sent: MQTT 5 allows the same name
		// more than once and that is the standard way to carry a list.
		// Measured before the change - three properties named `tag` arrived
		// as one - and the same publish to a broadcast topic, which never
		// becomes a record, delivered all three in order.
		h = append(h, store.Header{Key: u.Key, Value: u.Val})
	}
	if id == "" {
		id = uuidv7()
	}
	return store.Record{
		MessageID: id,
		Topic:     pk.TopicName,
		Payload:   append([]byte(nil), pk.Payload...),
		Headers:   h,
		Timestamp: at,

		// Which bridge brought it, empty for a record this broker originated.
		// An `out` rule skips every record carrying one, which is what stops a
		// bridge loop (RFC 0002 "Bridges").
		Bridge: from,

		// Who published it, for the one comparison No Local asks for. Taken
		// from the connection for the reason the mark above is: the packet
		// is a claim, and every reserved property a publisher sent is
		// stripped a few lines up precisely so that none of them survives.
		Publisher: by,

		// The publish properties that are not User Properties. A record kept
		// none of them, so a channel silently answered a stock MQTT 5 client
		// differently from a broadcast topic on the same broker (RFC 0003
		// "From publish to record").
		ContentType:       pk.Properties.ContentType,
		ResponseTopic:     pk.Properties.ResponseTopic,
		CorrelationData:   append([]byte(nil), pk.Properties.CorrelationData...),
		PayloadFormat:     pk.Properties.PayloadFormat,
		PayloadFormatFlag: pk.Properties.PayloadFormatFlag,
		MessageExpiry:     pk.Properties.MessageExpiryInterval,

		// Present and empty, kept as such.
		ContentTypeEmpty:     pk.Properties.ContentTypeFlag,
		ResponseTopicEmpty:   pk.Properties.ResponseTopicFlag,
		CorrelationDataEmpty: pk.Properties.CorrelationDataFlag,

		// **What the publisher set, kept rather than acted on.** A channel
		// is the store the flag was asking for, so there is nothing further
		// to do with it - but a subscriber that asked for Retain As
		// Published is owed the publisher's own answer (MQTT-3.3.1-13), and
		// before this was stored a channel had no way to give it: fanOut
		// clears the flag on every channel publish, so every such
		// subscriber saw 0 whatever any publisher did.
		Retain: pk.FixedHeader.Retain,
		// **The QoS the publisher used**, stored for the reason the flag
		// above is: a delivery has to be able to say what the original
		// publish was. [MQTT-3.8.4-8] makes a retained message's delivery
		// QoS the lower of the subscription's and the publish's, and
		// without this the broker had nothing to take the minimum of.
		QoS: pk.FixedHeader.Qos,
	}
}

// applyProps puts a record's stored publish properties onto a delivery.
//
// **queueOwned is where saguin's own mechanism wins, and it is the only
// place it does.** A queue delivery carries the Response Topic a worker
// answers on and the Delivery ID as its Correlation Data - that pair *is*
// the acknowledgement protocol, and MQTT gives one field of each per packet,
// so there is nowhere to put the publisher's beside it. The publisher's pair
// is still stored and still delivered anywhere else the record goes,
// including the dead-letter channel, which is an ordinary append channel
// where nothing collides.
//
// Message Expiry is sent decremented by the time the record has been
// waiting, as MQTT requires, and as 0 once it has run out. saguin does not
// delete an expired record: retention on a channel is the operator's, and a
// publisher removing records from another consumer's replay is the silent
// gap invariant 1 exists to prevent.
//
// **0 rather than nothing, because absent is already taken.** A delivery
// with no Message Expiry Interval says the publisher set none, so a
// consumer sent nothing for a record whose time is up cannot tell it from
// one that was never timed - and the two call for different decisions. The
// field is unsigned, so a spent record is 0 rather than a negative
// remainder.
func applyProps(pk *packets.Packet, r store.Record, now time.Time, queueOwned bool) {
	pk.Properties.ContentType = r.ContentType
	pk.Properties.ContentTypeFlag = r.ContentTypeEmpty
	if r.PayloadFormatFlag {
		pk.Properties.PayloadFormat = r.PayloadFormat
		pk.Properties.PayloadFormatFlag = true
	}
	if r.MessageExpiry != 0 && !r.Timestamp.IsZero() {
		left, _ := r.RemainingExpiry(now)
		pk.Properties.MessageExpiryInterval = left
	}
	if queueOwned {
		return
	}
	pk.Properties.ResponseTopic = r.ResponseTopic
	pk.Properties.ResponseTopicFlag = r.ResponseTopicEmpty
	if len(r.CorrelationData) > 0 {
		pk.Properties.CorrelationData = r.CorrelationData
	}
	pk.Properties.CorrelationDataFlag = r.CorrelationDataEmpty
}

// userProps builds the properties every channel delivery carries: what the
// record is, where it sits, when the broker received it, and the headers
// the publisher sent.
//
// It takes the record rather than three of its fields because the list was
// growing a parameter at a time, and because every caller has the record
// anyway.
//
// **saguin-timestamp is on every channel delivery, not only a latest
// one.** The receipt time is the same fact whichever channel holds the
// record, and two consumers of one broker reading different metadata off
// the same kind of record is worse than either answer alone - which is the
// argument that already settled Message Expiry Interval here. On a `latest`
// channel it does the most work: read with that channel's retention
// period, it turns "this is the current value" into "this is the current
// value, it is this old, and nothing older than the period is served at
// all", which is a promise no retained message can make.
// **`saguin-channel` is on a delivery only where the consumer cannot work
// it out**, which is where its filter reaches more than one channel. Such a
// consumer receives two independent offset sequences interleaved, and one
// tracking "I have seen up to offset 3" is silently wrong about one of
// them. It cannot recover the channel from the topic without knowing the
// filters, which is the thing a filter-placed channel exists to spare it.
//
// **Not on every delivery, and the measurement is why.** It is 26 bytes for
// a channel called `events`, which is 17% of a 150-byte packet carrying a
// 20-byte reading - and a fixed-size reading on a metered radio is the
// target, not an edge case. A consumer whose filter reaches one channel
// already knows which channel and pays nothing.
//
// **An earlier draft said "always, because a property that is sometimes
// present is a trap"**, on the strength of two retained-store defects where
// one message read two ways depending on when a subscriber arrived. That
// reasoning does not carry: this varies by which filter the consumer chose,
// not by when it connected, and MQTT already varies properties per
// subscription - a Subscription Identifier is exactly that.
//
// It costs nothing to build. The list starts at two entries and the
// timestamp grows it to room for four, so the name lands in space already
// allocated: measured at 103 ns against 105 ns without it, with identical
// allocations.
func userProps(r store.Record, chName string) []packets.UserProperty {
	out := []packets.UserProperty{
		{Key: "saguin-id", Val: r.MessageID},
		{Key: "saguin-offset", Val: strconv.FormatUint(r.Offset, 10)},
	}
	// Unix milliseconds, because what a consumer does with this is compare
	// it, and every language compares integers without a date library. A
	// record with no timestamp - one restored from a file written before
	// this existed - carries no claim about its age rather than a wrong
	// one: the zero time formats as a large negative number that reads as
	// 1754.
	if !r.Timestamp.IsZero() {
		out = append(out, packets.UserProperty{
			Key: "saguin-timestamp", Val: strconv.FormatInt(r.Timestamp.UnixMilli(), 10),
		})

		// **When the publisher's Message Expiry Interval runs out, as a
		// moment, and only where the publisher set one.**
		//
		// MQTT's own field cannot carry this case. On a channel an expired
		// record is still served - a publisher does not remove records from
		// another consumer's replay (invariant 1) - so its countdown reaches
		// zero and stays there, and the specification has no way to send
		// that: it deletes an expired message rather than delivering one, so
		// a zero never arises and the substrate writes the field only above
		// zero. A delivery past the deadline therefore carries no expiry
		// field at all, which is exactly what a message that never had one
		// looks like. This says the difference in saguin's own namespace,
		// where saguin is entitled to define what it means and no client can
		// mistake it for something MQTT said.
		//
		// **A moment rather than a flag**, and rather than the seconds left.
		// It says *when*, which is what somebody reprocessing a channel
		// wants; it is stable, so two consumers reading one record an hour
		// apart agree, where the countdown gives each a different number;
		// and it is the same units as the timestamp above, so the difference
		// between the two is the interval the publisher asked for.
		//
		// Sent whether or not it has passed, because the alternative makes
		// its absence mean two things - no deadline, or a deadline with time
		// left - which is the ambiguity it exists to remove.
		//
		// Under the timestamp guard because it is derived from it: a record
		// with no receipt time has no moment to offer, and a deadline
		// counted from the zero time is a claim about 1754.
		if r.MessageExpiry > 0 {
			at := r.Timestamp.Add(time.Duration(r.MessageExpiry) * time.Second)
			out = append(out, packets.UserProperty{
				Key: "saguin-expires", Val: strconv.FormatInt(at.UnixMilli(), 10),
			})
		}
	}
	if chName != "" {
		out = append(out, packets.UserProperty{Key: "saguin-channel", Val: chName})
	}
	for _, one := range r.Headers {
		out = append(out, packets.UserProperty{Key: one.Key, Val: one.Value})
	}
	return out
}

// channelNameFor is the name to stamp, or the empty string for a consumer
// that can already tell. One place, so the two delivery paths cannot come
// to different answers about when the property is there.
func (b *Broker) channelNameFor(spans bool, chName string) string {
	if spans {
		return chName
	}
	return ""
}

func packetKey(clientID string, packetID uint16) string {
	return clientID + "\x00" + strconv.Itoa(int(packetID))
}

// uuidv7 returns a time-ordered identifier. Message identity is stable
// across redelivery, dead-lettering, and replay (invariant 8), and is distinct
// from the MQTT packet identifier.
func uuidv7() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UnixMilli())
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms<<16)
	copy(b[0:6], ts[0:6])
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// SetIdentity tells the broker what to report as its own version and id,
// which are the two labels in the catalogue it cannot work out for itself:
// the build stamp is read from the binary, and a package reading it in a
// test would report the test binary's.
//
// saguin_build_info is also the reason `$SYS/broker/version` is not to be
// believed - that one names the substrate (RFC 0005).
//
// Called once before any listener opens, so it needs no lock.
func (b *Broker) SetIdentity(brokerID, version string) {
	b.brokerID, b.version = brokerID, version
}

// SetProviderKinds says which storage provider is memory and which is
// sqlite, for saguin_provider_info.
//
// It comes from the command rather than being worked out here, because
// what a provider *is* lives in the configuration file and the broker is
// handed stores rather than the shape of them.
//
// It is the label that answers the question invariant 14 makes an operator
// ask: which of my channels are only as durable as the next clean
// shutdown. RFC 0005 refuses a "last written to disk" gauge and points at
// this instead.
func (b *Broker) SetProviderKinds(kinds map[string]string) {
	b.providerKinds = kinds
}

// noLocalOnAQueue is why a queue subscription asking for No Local is
// refused, in the words the client is given. It is a constant because the
// SUBACK's User Property and the log line must say the same thing.
const noLocalOnAQueue = "No Local cannot be honoured on a queue: a job " +
	"withheld from the worker that published it would never be delivered, " +
	"acknowledged, timed out or dead-lettered. Subscribe without it."

// heldExchange is where an exactly-once publish is held, and since when.
type heldExchange struct {
	channel string
	at      time.Time
	// retain says a held broadcast asked to be retained, which its release
	// writes to the retained store before the swap (releaseHeld).
	retain bool
}
