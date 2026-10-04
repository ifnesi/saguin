// Package brokertest is the harness the end-to-end suite drives saguin
// through: a broker on a real socket, stock Paho clients, and the waiters
// that ask the broker a question rather than sleeping on one.
//
// **It is a package rather than a file so that the suite can be split.**
// internal/broker's tests are one binary that runs its tests one at a time,
// and that binary is 727 of the 729 seconds `make race` spends. Test
// packages run in parallel, so splitting the end-to-end files into several
// of them is what buys the time back - and each package being its own
// binary is also why this road was taken over `t.Parallel`, which would
// have had to rewrite the shared state this harness keeps.
package brokertest

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	mqttv3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	mqtt "github.com/ifnesi/saguin/internal/mqtt"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/mqtt/hooks/auth"
	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

const (
	Visibility  = 2 * time.Second
	MaxAttempts = 3
)

// JobExpiry is the queue's job_expires_after, in seconds, or zero for a
// harness whose jobs never expire - which is what every test but the
// expiry ones wants, since a job that vanished mid-test would look like a
// defect in whatever that test was about.
var JobExpiry int64

// StartExpiring runs a broker whose jobs are dead-lettered once they are
// older than the given number of seconds.
func StartExpiring(t testing.TB, seconds int64) *Harness {
	t.Helper()
	JobExpiry = seconds
	t.Cleanup(func() { JobExpiry = 0 })
	return StartWith(t, "tcp", "", "")
}

// StartExpiringOnSQLite is the same broker with its channels on a database,
// which is the combination the sweep failed on: a SQLite queue asks its
// dead-letter log whether it can be written inside the queue's own
// transaction, and only a memory store is handed over unwrapped by default.
func StartExpiringOnSQLite(t testing.TB, seconds int64, path string) *Harness {
	t.Helper()
	JobExpiry = seconds
	t.Cleanup(func() { JobExpiry = 0 })
	return StartWith(t, "tcp", "", path)
}

type Harness struct {
	Addr    string
	Network string // "tcp" unless a test asked for a Unix socket
	// B is the broker itself, for the one thing that has no client on the
	// wire: an inbound bridge publishes in process rather than over MQTT.
	B *broker.Broker
	// Srv is the substrate, for the one question a client on the wire
	// cannot answer: whether the broker has finished with a session. A
	// DISCONNECT is one-way, so nothing comes back to say so - see
	// sessionGone.
	Srv *mqtt.Server
	// Via is the proxy a test dials through, when it does not dial the
	// broker directly: the server then sees the proxy's own connection, not
	// the client's address, and registered asks the proxy which one.
	Via *Proxy
	// Disconnects counts sessions the broker has finished with, from a hook
	// registered after saguin's - see sessionGone.
	Disconnects *disconnectHook
	Stop        func()
	// DB is the provider the channels are on, or nil for a broker holding
	// them in memory. It is here for the one question no client on the wire
	// can answer: how many transactions the records were stored in.
	DB *sqlite.DB
	// Crash stops the broker without the orderly shutdown: no snapshot,
	// and no database close. It is what a power cut looks like from the
	// outside, and the only way to tell a store that writes as it goes
	// from one that writes at the end.
	Crash func()

	named *namedUsers
}

// namedUsers is the harness's own password file: every user name a test has
// dialled under, each with harnessPassword.
type namedUsers struct {
	mu    sync.Mutex
	names []string
}

// harnessPassword is every named dial's password. What is under test is
// the name a client authenticates as, never the password.
const harnessPassword = "harness"

// Authenticate gives user a password on this harness's broker, so a named
// dial authenticates rather than typing a name nothing checks: a name the
// broker admits anonymously is not the client's identity (RFC 0002
// "Authorization"). The file is rebuilt and replaced whole, so no file the
// broker is reading is written, and anonymous dials are still admitted
// beside it - Mosquitto's mixed mode.
//
// A test that installs credentials of its own must not also dial named
// through the harness, which would replace them.
func (h *Harness) Authenticate(t testing.TB, user string) string {
	t.Helper()
	h.named.mu.Lock()
	defer h.named.mu.Unlock()
	if !slices.Contains(h.named.names, user) {
		h.named.names = append(h.named.names, user)
	}
	f := passwd.New("")
	for _, n := range h.named.names {
		if err := f.Set(n, harnessPassword); err != nil {
			t.Fatalf("give %q a password: %v", n, err)
		}
	}
	h.B.SetCredentials(f, true)
	return harnessPassword
}

// heldAs fails the test unless the broker holds the connection conn as
// user: a named dial that runs as anybody else tests nothing about the name
// it was given. The connection is found by its address rather than by id
// alone, because a test that dials one id twice would otherwise read the
// previous connection, still registered while it is taken over.
func (h *Harness) heldAs(t testing.TB, conn net.Conn, id, user string) {
	t.Helper()
	cl := h.registered(t, conn, nil, id)
	if got := string(cl.Properties.Username); got != user {
		t.Fatalf("%s dialled as %q and the broker holds it as %q", id, user, got)
	}
}

// registered waits until the server has registered the connection conn, and
// returns it - or nil if done, the client's own end, closed first. A nil
// done waits for the registration alone.
//
// **A CONNACK is sent before the connection is registered**, on purpose
// (attachClient, [MQTT-3.2.0-1]), so a client's Connect returning says
// nothing about the server's client table: a test reading it, or asking the
// broker about the client by id, straight after Connect found nobody, or the
// session the connection had just inherited from. Measured without the race
// detector on two cores: TestAQueueSubscriptionGrantedDespiteItsRefusal
// ClosesTheConnection lost that race in 5 of 1,500 subtests.
//
// Found by its address, as heldAs always did, because a test that dials one
// id twice would otherwise read the previous connection.
func (h *Harness) registered(t testing.TB, conn net.Conn, done <-chan struct{}, id string) *mqtt.Client {
	t.Helper()
	if h.Srv == nil {
		t.Fatalf("%s: this harness has no server to ask whether %s is registered; "+
			"give it the broker's Srv (and Via, when it dials through a proxy)", id, conn.LocalAddr())
	}
	local := conn.LocalAddr().String()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		want := local
		if h.Via != nil {
			// Through a proxy the server's Remote is the proxy's own end of
			// the connection, which the proxy knows; it has accepted the
			// connection before any CONNACK came back through it.
			if up, ok := h.Via.upstream(local); ok {
				want = up
			}
		}
		if cl, ok := h.Srv.Clients.Get(id); ok && cl.Net.Remote == want {
			return cl
		}
		select {
		case <-done:
			return nil
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s from %s was not registered 5s after its CONNACK", id, local)
		}
	}
}

// Limits are the defaults every test runs against, so that a test which
// cares about a bound sets that bound rather than inheriting a number.
var Limits = func() config.Resolved {
	// The counted limits are pointers so that a written zero can be told
	// from an absent key, which is what stops one being silently replaced by
	// its default.
	topics, headers, conns := 1024, 32, int64(10000)
	var l config.Limits
	l.MaxTopicLength, l.MaxHeaderCount, l.MaxConnections = &topics, &headers, &conns
	l.MaxMessageSize, l.MaxHeaderBytes = "1MiB", "8KiB"
	return l.Resolve()
}()

// ShortSocketDir is `t.TempDir()` for a Unix socket path specifically,
// which `t.TempDir()` itself is not safe for: it embeds the test's own
// name, and a name long enough - combined with macOS's default `TMPDIR`,
// already several directories deep under `/var/folders` - overruns the
// ~104-byte `sun_path` a Unix socket address is limited to. `bind` then
// fails with "invalid argument" on a path that is otherwise completely
// ordinary, which is indistinguishable from a real bind failure unless you
// already know to suspect the path's length. `TestUnixSocketListener`
// (23 characters) has never hit this; `TestTheOperationsListenerAnswersOverAUnixSocket`
// (48 characters) always does - the same helper, the same platform, and
// the only variable is how long somebody named the test. `/tmp` itself is
// short on every platform this runs on and is not subject to `TMPDIR`.
func ShortSocketDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sgn")
	if err != nil {
		t.Fatalf("a short-enough temp dir for a socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func Start(t testing.TB) *Harness {
	t.Helper()
	return StartOn(t, "tcp")
}

// StartTail configures the append channel this test starts with
// `start: tail`, so both protocols are driven against both settings.
var StartTail bool

// OnlyFive configures the broker this test starts to admit MQTT 5 alone,
// which is what `broker.mqtt.min_protocol_version: "5"` does.
var OnlyFive bool

// Admitting311 used to lower the gate for one test. The default admits
// MQTT 3.1.1 now, so it does nothing and is kept only because the tests
// that call it are about 3.1.1 and read better saying so.
func Admitting311(t testing.TB) { t.Helper() }

// StartAdmitting311 is a broker that admits MQTT 3.1.1, which is every
// broker unless min_protocol_version says otherwise.
func StartAdmitting311(t testing.TB) *Harness {
	t.Helper()
	return StartOn(t, "tcp")
}

// StartOn runs the broker on a TCP port or a Unix socket. The two are the
// same broker: nothing above the listener knows which it is.
func StartOn(t testing.TB, network string) *Harness {
	t.Helper()
	return StartWith(t, network, "", "")
}

// StartDurable runs a broker whose channels are kept in dir, so that
// stopping it and starting another on the same directory is a restart -
// the same thing an operator does, seen from the wire rather than from
// inside the broker.
func StartDurable(t testing.TB, dir string) *Harness {
	t.Helper()
	return StartWith(t, "tcp", dir, "")
}

// StartDurableSQLite runs a broker whose channels are kept in a SQLite
// database at path, so that stopping it and starting another on the same
// file is a restart - including the queue and, with it, the dead-letter
// channel it derives, which shares the file so the move between them is one
// commit (invariant 5).
func StartDurableSQLite(t testing.TB, path string) *Harness {
	t.Helper()
	return StartWith(t, "tcp", "", path)
}

// Bound is the channel size bounds a harness applies, by channel name, or
// nil for a broker where nothing is bounded.
var Bound map[string]int64

// StartBounded runs a broker where the named channels may hold only so
// much, which is what a configuration's max_bytes does.
func StartBounded(t testing.TB, dbPath string, bounds map[string]int64) *Harness {
	t.Helper()
	Bound = bounds
	t.Cleanup(func() { Bound = nil })
	return StartWith(t, "tcp", "", dbPath)
}

// SessionsNamedLikeChannels names a sqlite harness's session provider as its
// channels' ("disk"), since both are in its one database: the arrangement a
// configuration gets when broker.session.storage is left to default to the
// channels' provider. Otherwise the sessions are named "local", which is what
// every other test reads them as.
var SessionsNamedLikeChannels bool

// SessionsInMemory puts the session store on a memory provider while the
// channels stay on the harness's database, which is what
// `broker.session.storage` naming a memory provider gives an operator
// (RFC 0002). It is the pairing in which a stored position outlives the
// session that owns it: a restart keeps the position and loses the session.
var SessionsInMemory bool

// Retain is the channel retention_bytes a harness applies, by channel name.
// Retention REMOVES to stay under, where bound above refuses instead.
var Retain map[string]int64

// StartTrimming runs a broker whose named channels remove their oldest
// records to stay under a size. It is the only way a test can make the
// retention floor move, which is what a consumer has to be told about.
func StartTrimming(t testing.TB, sizes map[string]int64) *Harness {
	t.Helper()
	Retain = sizes
	t.Cleanup(func() { Retain = nil })
	return StartWith(t, "tcp", "", "")
}

// WithoutLatest drops the latest channel from the harness, which is the
// one configuration in which saguin can honour the retain flag nowhere.
var WithoutLatest bool

// StartWithoutLatest runs a broker of append and queue channels only. It
// exists for the CONNACK: Retain Available is a property of what the
// operator configured, so the answer has two branches and a test that runs
// only the harness's own configuration checks one of them.
func StartWithoutLatest(t testing.TB) *Harness {
	t.Helper()
	WithoutLatest = true
	t.Cleanup(func() { WithoutLatest = false })
	return StartWith(t, "tcp", "", "")
}

// retaining, when non-nil, gives the harness a retained store for
// broadcast topics, with this retention period in seconds - zero being
// `none`, which is the default an operator gets.
// KeepaliveCeiling, when non-zero, gives the harness's broker a
// `limits.max_keepalive` of that many seconds.
var KeepaliveCeiling uint16

// DialUsername, when set, is the user name the next client dialled carries.
// The acl_file's patterns are about the user name, so a test exercising one
// has to set it; every other test leaves it empty, which is what an
// anonymous client is.
var DialUsername string

// DialWithUser dials a client carrying a user name, and clears it again so
// that it cannot leak into the next client this test dials.
func DialWithUser(t testing.TB, h *Harness, id, user string) *Client {
	t.Helper()
	DialUsername = user
	defer func() { DialUsername = "" }()
	return Connect(t, h, id, true, false)
}

// DialSession is dialWithUser with a session that survives the connection,
// which is what a durable consumer has and what makes the hang-up affordable
// - and what the withdrawal test needs, because a resumed session sends no
// SUBSCRIBE.
func DialSession(t testing.TB, h *Harness, id, user string) *Client {
	t.Helper()
	DialUsername = user
	defer func() { DialUsername = "" }()
	return Connect(t, h, id, false, false)
}

// PublishCeiling and PublishByteCeiling, when non-zero, give the harness's
// broker a `limits.publish_rate` and `limits.publish_bytes` - messages and
// bytes a second, per client. Zero is the default every other test wants:
// no bound at all.
var (
	PublishCeiling     int
	PublishByteCeiling int64
)

// SessionQueueBytes, when non-zero, gives the harness's broker a
// `limits.session_queue_bytes` of this many bytes, so a test can fill a
// session without publishing a megabyte into it.
var SessionQueueBytes int64

// MaxSubscriptions, when non-zero, gives the harness's broker a
// `limits.max_subscriptions` of this many topic filters a client, so a test
// can reach the bound without ten thousand SUBSCRIBEs.
var MaxSubscriptions int

// StartACL, when set, is an acl_file put in force before the start publishes
// the Wills it found owed, as main puts its acl_file in force before that: a
// Will is judged against the rules as they stand when it fires, and a start
// is one of the moments it fires.
var StartACL string

// MaxSessionExpiry lowers limits.max_session_expiry for one test, which is
// what decides how long a session outlives its connection - and so how long a
// Will may be held for it.
var MaxSessionExpiry uint32

// WriteDeadline, when non-zero, gives the harness's broker a
// `limits.write_timeout` of this long. The default is seconds, which is
// right for a broker and slow for a test that has to wait one out.
var WriteDeadline time.Duration

var Retaining *int64

// LatestRetention, when non-zero, gives the harness's latest channel,
// "state", a retention_period of this many seconds.
var LatestRetention int64

// nested, when set, gives the harness channels placed by filter inside one
// `iot/<domain>/<device>/…` hierarchy rather than each claiming a first
// topic level of its own.
var nested bool

// randomChans, when non-nil, is the channel set a randomised scenario
// generated for this run - see random_test.go. It wins over every switch
// above, because the whole point of that test is a topology nobody wrote.
var randomChans []*channel.Channel

// queueOnly, when set, gives the harness a queue and nothing else. It
// isolates the queue's rule from everything else that could answer first,
// so a refusal here can only have come from the queue.
//
// It is not what makes the crossing case reachable. Nothing is refused any
// more for meeting a queue - `$share` is refused for nothing it reaches -
// so what this isolates is the delivery: a shared subscriber on a broker
// with one queue that receives a record can only have been fed by the
// queue.
var queueOnly bool

// QoS2Inflight is `broker.qos2.max_inflight_per_client` for the next
// harness, or zero for the default. A test that wants to reach the bound
// sets it small rather than opening twenty exchanges to get there.
var QoS2Inflight int

// QoS2ExpiresAfter is `broker.qos2.expires_after` for the next harness, or
// zero for a figure long enough that no test trips over it. The default
// here is not the shipped one: five minutes is right for a device on a poor
// link and wrong for a suite that must not depend on wall-clock timing, so
// the harness picks a long one and the test about expiry sets its own.
var QoS2ExpiresAfter time.Duration

// ShareExpiresAfter is what a test sets to give a harness a
// broker.share.expires_after, and shareExpiry reads it. Zero, which is the
// default, means a backlog lives as long as a member session that could
// collect it - the configuration's own default.
var ShareExpiresAfter time.Duration

func shareExpiry() time.Duration { return ShareExpiresAfter }

func qos2Expires() time.Duration {
	if QoS2ExpiresAfter > 0 {
		return QoS2ExpiresAfter
	}
	return time.Hour
}

// StartQueueOnly runs a broker whose only channel is a queue.
func StartQueueOnly(t testing.TB) *Harness {
	t.Helper()
	queueOnly = true
	t.Cleanup(func() { queueOnly = false })
	return StartWith(t, "tcp", "", "")
}

// StartRandom runs a broker on a channel set a scenario generated, rather
// than on one written here. The switch is a package variable like the rest,
// and is cleared on cleanup so a generated topology cannot leak into the
// next test.
func StartRandom(t testing.TB, chans []*channel.Channel) *Harness {
	t.Helper()
	randomChans = chans
	t.Cleanup(func() { randomChans = nil })
	return StartWith(t, "tcp", "", "")
}

// StartRandomSQLite is startRandom with the generated channels on a SQLite
// database. It exists so one generated scenario can drive both providers
// and the answers be compared - the comparison is the oracle for
// everything the RFC model has no opinion about, a stored property above
// all.
func StartRandomSQLite(t testing.TB, chans []*channel.Channel, path string) *Harness {
	t.Helper()
	randomChans = chans
	t.Cleanup(func() { randomChans = nil })
	return StartWith(t, "tcp", "", path)
}

// StartNested runs a broker whose channels are placed by filter.
func StartNested(t testing.TB) *Harness {
	t.Helper()
	nested = true
	t.Cleanup(func() { nested = false })
	return StartWith(t, "tcp", "", "")
}

// collecting, when non-nil, gives the harness's sqlite provider the group
// commit an operator asks for with publish_commit_interval and
// publish_commit_max_records, and a zero Interval is `none`: a transaction
// per publish. Nil is what every other test gets, and what an operator who
// says nothing gets: collected behind the commit before.
var collecting *config.CommitGroup

// CollectPublishes turns group commit on for one test, and off again after
// it. It is a package-level variable rather than an argument because
// startWith is called from several hundred places.
func CollectPublishes(t testing.TB, interval time.Duration, records int) {
	t.Helper()
	collecting = &config.CommitGroup{Interval: interval, MaxRecords: records}
	t.Cleanup(func() { collecting = nil })
}

// StartRetaining runs a broker an operator configured broker.retained on.
// Without it there is nowhere to keep a retained message on a broadcast
// topic and such a publish is refused, which is the other half of the rule
// and has a harness of its own in start().
func StartRetaining(t testing.TB, periodSeconds int64) *Harness {
	t.Helper()
	Retaining = &periodSeconds
	t.Cleanup(func() { Retaining = nil })
	return StartWith(t, "tcp", "", "")
}

// soaking, when set, gives the harness the soak's channels: the same four
// types placed by filter under one `iot/` hierarchy, plus one channel whose
// filter deliberately overlaps another's.
//
// **The overlap is the whole reason this exists.** With `<name>/#` filters
// every publish resolves by matching exactly one route, so the churn never
// crosses the rule that decides between two filters that both match. `hq`
// is `iot/hq/#` and `events` is `iot/+/events/+`; `iot/hq/events/7` matches
// both and belongs to `hq`, because a spelled-out level beats `+` at level
// 1. That is the resolution the soak could not reach before.
var soaking bool

// StartSoaking runs the durable, retaining broker with those channels.
func StartSoaking(t testing.TB, dbPath string) *Harness {
	t.Helper()
	soaking = true
	t.Cleanup(func() { soaking = false })
	return StartRetainingDurably(t, "", dbPath)
}

// StartRetainingDurably runs the same broker with its retained store kept
// where the channels are kept - a snapshot directory, or a database - so
// that stopping it and starting another on the same path is a restart.
func StartRetainingDurably(t testing.TB, snapshotDir, dbPath string) *Harness {
	t.Helper()
	var period int64
	Retaining = &period
	t.Cleanup(func() { Retaining = nil })
	return StartWith(t, "tcp", snapshotDir, dbPath)
}

// backingOff, when set, gives the harness's queue a retry backoff. Absent
// is `none`, which is what every other harness here runs.
var backingOff *store.Backoff

// StartBackingOff runs a broker whose queue makes a returned job wait before
// it is offered again (RFC 0002 `retry.backoff`).
func StartBackingOff(t testing.TB, b store.Backoff) *Harness {
	t.Helper()
	backingOff = &b
	t.Cleanup(func() { backingOff = nil })
	return StartWith(t, "tcp", "", "")
}

// StartLogging runs a broker whose log lines are handed to onLine, so a
// test can assert on something the broker only ever says to its operator.
func StartLogging(t testing.TB, onLine func(string)) *Harness {
	t.Helper()
	LogTo = onLine
	t.Cleanup(func() { LogTo = nil })
	return StartWith(t, "tcp", "", "")
}

// StartLoggingSQLite is startLogging against a provider that keeps its own
// records, which is the only kind that has a provider-level way to forget a
// reader - and so the only kind whose clean start can be asserted to have
// taken it.
func StartLoggingSQLite(t testing.TB, onLine func(string), path string) *Harness {
	t.Helper()
	LogTo = onLine
	t.Cleanup(func() { LogTo = nil })
	return StartWith(t, "tcp", "", path)
}

// logLevel is the level the line reader above captures at. Info is what
// every existing reader wants; startLoggingDebug lowers it for the lines
// saguin writes at debug.
var logLevel slog.Level

// StartLoggingDebug is startLogging for a test about a line written at
// debug level.
func StartLoggingDebug(t testing.TB, onLine func(string)) *Harness {
	t.Helper()
	logLevel = slog.LevelDebug
	t.Cleanup(func() { logLevel = slog.LevelInfo })
	return StartLogging(t, onLine)
}

// StartLoggingDebugSQLite is StartLoggingDebug with the channels on a SQLite
// provider at path.
func StartLoggingDebugSQLite(t testing.TB, onLine func(string), path string) *Harness {
	t.Helper()
	logLevel = slog.LevelDebug
	t.Cleanup(func() { logLevel = slog.LevelInfo })
	return StartLoggingSQLite(t, onLine, path)
}

// LogTo, when set, receives every line the broker logs.
var LogTo func(string)

type lineWriter struct{ fn func(string) }

func (w lineWriter) Write(p []byte) (int, error) {
	w.fn(string(p))
	return len(p), nil
}

func StartWith(t testing.TB, network, snapshotDir, dbPath string) *Harness {
	t.Helper()
	// **SAGUIN_TEST_STORAGE=sqlite puts a memory broker's channels on a
	// fresh sqlite file**, so a suite written against memory can be run on
	// the provider a deployment uses. Only where nothing persists: a broker
	// with no snapshot directory and no database keeps nothing across a
	// restart, so a new file per start is the same promise.
	switch s := os.Getenv("SAGUIN_TEST_STORAGE"); s {
	case "", "memory":
	case "sqlite":
		if snapshotDir == "" && dbPath == "" {
			dbPath = filepath.Join(t.TempDir(), "harness.db")
			t.Logf("SAGUIN_TEST_STORAGE: channels on sqlite at %s", dbPath)
		}
	default:
		t.Fatalf("SAGUIN_TEST_STORAGE=%q: memory or sqlite", s)
	}

	chans := []*channel.Channel{
		{Name: "events", Type: channel.Append, StartAtTail: StartTail},
		{Name: "state", Type: channel.Latest, RetentionPeriod: LatestRetention},
		{Name: "jobs", Type: channel.Queue,
			VisibilityTimeout: int64(Visibility / time.Second), MaxAttempts: MaxAttempts,
			JobExpiresAfter: JobExpiry},
	}
	// By name rather than by position: the slice is rebuilt below when a
	// harness drops the latest channel, and an index into it is a thing that
	// silently starts pointing at a different channel.
	for _, c := range chans {
		if c.Type == channel.Queue && backingOff != nil {
			c.Backoff, c.BackoffBase = backingOff.Kind, int64(backingOff.Base/time.Second)
		}
	}
	if WithoutLatest {
		chans = append(chans[:1], chans[2:]...)
	}
	if queueOnly {
		chans = []*channel.Channel{
			{Name: "jobs", Type: channel.Queue,
				VisibilityTimeout: int64(Visibility / time.Second), MaxAttempts: MaxAttempts,
				JobExpiresAfter: JobExpiry},
		}
	}
	if soaking {
		chans = []*channel.Channel{
			{Name: "events", Type: channel.Append, Filter: "iot/+/events/+"},
			{Name: "hq", Type: channel.Append, Filter: "iot/hq/#"},
			{Name: "state", Type: channel.Latest, Filter: "iot/+/state/+"},
			{Name: "jobs", Type: channel.Queue, Filter: "iot/+/work/+",
				VisibilityTimeout: int64(Visibility / time.Second), MaxAttempts: MaxAttempts,
				JobExpiresAfter: JobExpiry},
		}
	}
	if nested {
		// One `iot/` hierarchy divided four ways, which is the thing a
		// channel claiming its own name could not do at all. The queue's
		// filter is more exact than the append channel's at level 3, so
		// `iot/water/w-7/inspect` is work and `iot/water/w-7/flow` is a
		// reading, out of two lines that overlap on purpose.
		chans = []*channel.Channel{
			{Name: "water-location", Type: channel.Latest,
				Filter: "iot/water/+/location/#"},
			{Name: "water-measurement", Type: channel.Append,
				Filter: "iot/water/+/+/#"},
			{Name: "weather-measurement", Type: channel.Append,
				Filter: "iot/weather/+/{data,events}"},
			{Name: "work", Type: channel.Queue, Filter: "iot/water/+/inspect",
				VisibilityTimeout: int64(Visibility / time.Second), MaxAttempts: MaxAttempts,
				JobExpiresAfter: JobExpiry},
		}
	}
	if randomChans != nil {
		chans = randomChans
	}
	if snapshotDir != "" {
		for _, c := range chans {
			c.Storage = "local"
		}
	}
	if dbPath != "" {
		for _, c := range chans {
			c.Storage = "disk"
		}
	}
	for _, c := range chans {
		c.MaxBytes = Bound[c.Name]
		c.RetentionBytes = Retain[c.Name]
	}
	reg, err := channel.NewRegistry(chans)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if snapshotDir != "" {
		reg.Get("jobs").DLQ.Storage = "local"
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(NewTestWriter(t), &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	// Last, so that a test reading the broker's own lines gets them whether
	// or not the suite is running verbose.
	if LogTo != nil {
		// Info unless a test asks for lower. A line saguin writes at debug -
		// a per-refusal line, deliberately not at warn so that a throttled
		// client cannot flood the log - is invisible to a reader at Info,
		// and a test asserting its text passes by reading nothing.
		log = slog.New(slog.NewTextHandler(lineWriter{LogTo},
			&slog.HandlerOptions{Level: logLevel}))
	}

	// A copy, so a test that sets a ceiling does not leave it on the
	// package-level limits every later test reads.
	lim := Limits
	if SessionQueueBytes > 0 {
		lim.SessionQueueBytes = SessionQueueBytes
	}
	if MaxSubscriptions > 0 {
		lim.MaxSubscriptions = MaxSubscriptions
	}
	if MaxSessionExpiry > 0 {
		lim.MaxSessionExpiry = MaxSessionExpiry
	}
	lim.MaxKeepalive = KeepaliveCeiling
	lim.PublishRate = PublishCeiling
	lim.PublishBytes = PublishByteCeiling
	if WriteDeadline > 0 {
		lim.WriteTimeout = WriteDeadline
	}
	srv, b, err := broker.NewServer(reg, lim, log)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	if OnlyFive {
		srv.Options.Capabilities.MinimumProtocolVersion = 5
	}

	var addr string
	if network == "unix" {
		addr = filepath.Join(ShortSocketDir(t), "saguin.sock")
		if err := srv.AddListener(listeners.NewUnixSock(listeners.Config{ID: "u", Address: addr})); err != nil {
			t.Fatalf("add unix listener: %v", err)
		}
	} else if network == "ws" {
		// The WebSocket listener binds in Serve rather than Init, and its
		// Address reports the string it was configured with, so port 0
		// cannot be read back the way it can for TCP. The port is picked
		// here instead, and the client below retries until the server is
		// actually serving on it.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("pick a port: %v", err)
		}
		addr = ln.Addr().String()
		_ = ln.Close()
		if err := srv.AddListener(listeners.NewWebsocket(listeners.Config{ID: "w", Address: addr})); err != nil {
			t.Fatalf("add websocket listener: %v", err)
		}
	} else {
		// Let the server bind port 0 and report back which port it got.
		//
		// Picking the port here instead - listen, read the address, close,
		// hand the address over - releases it between the two binds, and
		// anything else on the machine may take it in that window. The
		// server then fails to bind, or worse binds after something else
		// has claimed and released it, and the test dials a port nobody is
		// serving. AddListener calls Init, so the address is real once it
		// returns.
		tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
		if err := srv.AddListener(tcp); err != nil {
			t.Fatalf("add listener: %v", err)
		}
		addr = tcp.Address()
	}

	// Loaded before Serve, as main.go does it, so that no client can
	// publish into a channel that is about to be replaced by its snapshot.
	var sessionsDir *store.Dir
	if snapshotDir != "" {
		sessionsDir = store.NewDir(snapshotDir, "e2e")
		b.SetSnapshotDirs(map[string]*store.Dir{"local": sessionsDir})
		// A memory-backed retained store is attached before the load, not
		// after: its file is one of the snapshots, and a store that is not
		// there yet is one the load has nowhere to put. Same order as
		// main.go, which is why the order is worth copying rather than
		// improvising here.
		if Retaining != nil && dbPath == "" {
			b.SetRetained("local", store.NewLatest(), *Retaining)
		}
		warnings, err := b.LoadSnapshots()
		if err != nil {
			t.Fatalf("load snapshots: %v", err)
		}
		for _, w := range warnings {
			t.Logf("snapshot: %s", w)
		}
	} else if Retaining != nil && dbPath == "" {
		b.SetRetained("local", store.NewLatest(), *Retaining)
	}

	var db *sqlite.DB
	if dbPath != "" {
		var err error
		// Bounded as the binary bounds it, room held back for the writes
		// that relieve a full provider included (config.Limits.StorageReserve):
		// a harness without it fills in a shape the binary never runs.
		if db, err = sqlite.OpenBounded(dbPath, "e2e", dbMaxBytes, lim.StorageReserve()); err != nil {
			t.Fatalf("open database: %v", err)
		}
		if collecting != nil {
			db.CommitGroup(collecting.Interval, collecting.MaxRecords)
		}
		// **The stores are built by walking the registry, the way cmd/saguin
		// builds them**, rather than from the three names this harness usually
		// runs. A wiring that names channels cannot reach one it does not
		// name, and that is how the whole disaster-recovery feature came to be
		// exercised in memory alone: a copy channel is never one of the three,
		// so a sqlite harness silently gave it nothing and a test that asked
		// for a copy on sqlite got one in memory.
		//
		// The registry rather than the channel slice, because the slice does
		// not hold what the registry derives - a queue's dead-letter channel
		// is in the registry and in no configuration, and a harness that
		// derives the name itself is keeping a second copy of a rule that
		// belongs to one place.
		st := broker.Stores{
			Logs:   map[string]broker.LogStore{},
			Latest: map[string]broker.LatestStore{},
			Queues: map[string]broker.QueueStore{},
			// The provider itself, as cmd/saguin attaches it: without this a
			// harness gets the per-channel walk on a clean start, which is
			// what the broker does for a provider that cannot answer for
			// itself - so a test meaning to exercise the one-operation path
			// would quietly exercise the other one.
			Providers: map[string]broker.ReaderDropper{"disk": db},
		}
		if WrapProviders != nil {
			st.Providers["disk"] = WrapProviders("disk", db)
		}
		for name, c := range reg.All() {
			// Gated on the provider the channel names, as main.go gates it, so
			// a channel kept somewhere else is left alone rather than given a
			// database it never asked for.
			if c.Storage != "disk" {
				continue
			}
			var err error
			switch c.Type {
			case channel.Append:
				st.Logs[name], err = db.Log(name)
			case channel.Latest:
				st.Latest[name], err = db.Latest(name)
			case channel.Queue:
				st.Queues[name], err = db.Queue(name)
			}
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		b.SetStores(st)
		if Retaining != nil {
			// In the database, in a table of its own, the way a latest
			// channel is - so it survives a crash rather than a graceful
			// stop.
			rt, err := db.Latest(broker.RetainedStoreName)
			if err != nil {
				t.Fatalf("open the retained store: %v", err)
			}
			b.SetRetained("disk", rt, *Retaining)
		}
	}

	// Exactly-once, on for the suite as it is on every broker, with
	// `broker.qos2`'s defaults. A held publish waits in the store of the
	// channel it is for, or the broadcast log's for a broadcast, so there is
	// no store to attach.
	//
	// QoS2Inflight lets one test drive the per-client allowance without the
	// rest of the suite pipelining into a bound.
	max := QoS2Inflight
	if max == 0 {
		max = 20
	}
	b.SetQoS2(max, qos2Expires())

	// Sessions, on the same provider, as main attaches them: a database where
	// the harness has one and memory where it does not. HarnessSessions is
	// what a test reads the store back through.
	var sessions broker.SessionStore
	if db != nil && !SessionsInMemory {
		s, err := db.Sessions()
		if err != nil {
			t.Fatalf("session store: %v", err)
		}
		sessions = s
	} else {
		sessions = store.NewSessions()
		// Read back from the provider's sessions file, in main.go's order:
		// before the store is attached, because a file comes back as a
		// store. Without this a memory harness restarted on the same
		// directory would start with no sessions and read as a broker that
		// keeps none.
		if sessionsDir != nil {
			snap, err := sessionsDir.LoadSessions()
			if err != nil {
				t.Fatalf("load sessions: %v", err)
			}
			if snap != nil {
				sessions = store.RestoreSessions(snap)
			}
		}
	}
	opened := sessions
	if WrapSessions != nil {
		sessions = WrapSessions(sessions)
	}
	sessionsProvider := "local"
	if db != nil && !SessionsInMemory && SessionsNamedLikeChannels {
		sessionsProvider = "disk"
	}
	b.SetSessions(sessionsProvider, sessions)
	HarnessSessions = sessions
	// As main does: the provider's own store, not a wrapper around it.
	if err := b.StartBroadcast(opened); err != nil {
		t.Fatalf("count what sessions are owed from the broadcast log: %v", err)
	}

	// What a shared group is owed is the broadcast log behind its cursor,
	// started above; its expiry is main's too.
	b.SetShareExpiry(shareExpiry())
	HarnessShares = b
	if WrapLogs != nil {
		for name := range reg.All() {
			b.WrapLog(name, func(l broker.LogStore) broker.LogStore { return WrapLogs(name, l) })
		}
	}
	if WrapHolds != nil {
		for name := range reg.All() {
			b.WrapHolds(name, func(h broker.HoldStore) broker.HoldStore { return WrapHolds(name, h) })
		}
		b.WrapHolds(store.BroadcastLog, func(h broker.HoldStore) broker.HoldStore {
			return WrapHolds(store.BroadcastLog, h)
		})
	}
	if StartACL != "" {
		authorizeFrom(t, b, StartACL)
	}
	// In main.go's order: what no client can have any more is ended, and
	// what a client can still have is put back, both before any listener
	// opens.
	if _, _, err := b.DropSessionsLeftByAStop(time.Now()); err != nil {
		t.Fatalf("end the sessions left by a stop: %v", err)
	}
	if _, err := b.RestoreSessions(); err != nil {
		t.Fatalf("restore the sessions left by a stop: %v", err)
	}
	// In main's order: what a group held past its expiry goes before any
	// listener opens.
	b.DropShareBacklogsLeftByAStop(time.Now())
	// The Wills this start found owed, published where main.go publishes
	// them: after the snapshots have loaded, which in this harness happened
	// above, and before any listener opens, which is below.
	b.PublishDueWills()

	// Serve does work after the listeners bind and before it returns: it
	// starts every listener serving, then fires OnStarted. A harness that
	// returns while that is still running lets a test assert against a
	// half-built broker.
	//
	// Waiting on a hook rather than on a dial, because the probe that used to
	// be here was itself the flake (see below), and rather than on a sleep,
	// because the thing being waited for announces itself exactly.
	started := make(chan struct{})
	// After saguin's hook, deliberately: sessionGone waits on it, and what
	// it has to outlast is saguin's own OnDisconnect.
	disconnects := &disconnectHook{}
	if err := srv.AddHook(disconnects, nil); err != nil {
		t.Fatalf("disconnect hook: %v", err)
	}
	if err := srv.AddHook(&startedHook{Ch: started}, nil); err != nil {
		t.Fatalf("started hook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)
	go func() { _ = srv.Serve() }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the broker did not finish starting")
	}

	// No readiness probe. AddListener calls Init, which binds the socket,
	// so by the time we are here the kernel queues connections whether or
	// not Serve has reached its Accept yet - a dial cannot arrive too
	// early.
	//
	// The probe that used to be here dialled and closed immediately, and
	// that connection was the flake. mochi's attachClient does
	// Listeners.ClientsWg.Add(1) while Close does ClientsWg.Wait(), which
	// sync.WaitGroup forbids; under -race the detector fires and fails
	// whichever test is running. The fastest tests lost most often,
	// because their Close came soonest after the probe.

	// Once, because a test that stops the broker itself is followed by the
	// cleanup stopping it again, and writing the snapshot twice would hide
	// a save that only works the first time.
	var once sync.Once
	h := &Harness{Addr: addr, Network: network, B: b, Srv: srv, Disconnects: disconnects, DB: db,
		named: &namedUsers{}}
	h.Stop = func() {
		once.Do(func() {
			cancel()
			// The same call main.go makes, rather than a copy of its steps
			// here: a harness with its own shutdown order tests that order
			// and not the one the broker ships with.
			if err := b.Shutdown(); err != nil {
				t.Errorf("shutdown: %v", err)
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Errorf("close database: %v", err)
				}
			}
		})
	}

	// crash stops the broker the way a power cut does: no snapshot, and
	// the database not closed, so nothing that only happens on the way out
	// gets a chance to happen. It shares the once with stop, so the
	// cleanup below does not then run the orderly path over it.
	h.Crash = func() {
		once.Do(func() {
			cancel()
			_ = srv.Close()
			if db != nil {
				// A dying process has its file locks taken back by the
				// kernel. Nothing else on the way out happens: no
				// checkpoint, no snapshot, no close.
				if err := db.Abandon(); err != nil {
					t.Errorf("abandon database: %v", err)
				}
			}
		})
	}

	t.Cleanup(h.Stop)
	return h
}

// testWriter hands the broker's log lines to the test that started it, and
// stops the moment that test is over.
//
// **Go panics on a log written after the test it names has completed**, and
// the broker has goroutines that outlive the test function by a moment: a
// listener's read loop reports the connection the harness just closed. That
// makes the whole package die - naming neither the test nor the reason -
// and only under `-v`, because the log goes to io.Discard otherwise. So it
// costs nothing until somebody debugs a failing test the obvious way, which
// is the worst moment for the suite to stop being able to tell them
// anything.
//
// There is nothing to attach a line to once the test has ended, so a late
// line is dropped rather than written somewhere else. The lock is held
// across the Logf, which is what makes the two outcomes the only two: a
// write already under way finishes before ended can be set, and every write
// after it is dropped. Checking a flag and then logging outside the lock
// would leave the window this exists to close.
type testWriter struct {
	T     logTarget
	Mu    sync.Mutex
	ended bool
}

// logTarget is the whole of what the writer needs from a test, and it is
// narrower than testing.TB for a reason: testing.TB cannot be implemented
// outside its own package, so a rule stated against it can only be tested
// by a real test - and the thing to assert here happens after a real test
// has ended, which is the one moment it can no longer be asked anything.
// *testing.T satisfies this, and so does a stand-in that counts calls.
type logTarget interface {
	Logf(format string, args ...any)
	Cleanup(func())
}

// NewTestWriter is where the cleanup is registered rather than at the three
// call sites, so that a fourth one cannot be added without it.
//
// Cleanups run last-registered-first, and this one is registered as the
// broker is built - before the harness registers its own stop - so it runs
// after everything the test does on its way out. A line logged during
// another cleanup still reaches the test, which is legal and is where the
// interesting ones are.
func NewTestWriter(t logTarget) io.Writer {
	w := &testWriter{T: t}
	t.Cleanup(func() {
		w.Mu.Lock()
		defer w.Mu.Unlock()
		w.ended = true
	})
	return w
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	if w.ended {
		return len(p), nil
	}
	w.T.Logf("%s", p)
	return len(p), nil
}

// CountingTarget stands in for a test: it counts what was logged to it and
// runs its cleanups when told, which is the moment a real test ends.
type CountingTarget struct {
	Mu       sync.Mutex
	Lines    int
	cleanups []func()
}

func (c *CountingTarget) Logf(string, ...any) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.Lines++
}

func (c *CountingTarget) Cleanup(fn func()) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.cleanups = append(c.cleanups, fn)
}

// End runs them last-registered-first, as testing does.
func (c *CountingTarget) End() {
	c.Mu.Lock()
	fns := c.cleanups
	c.Mu.Unlock()
	for i := len(fns) - 1; i >= 0; i-- {
		fns[i]()
	}
}

type Received struct {
	Topic     string
	Payload   string
	RespTopic string
	CorrData  []byte
	Retain    bool
	// ContentType and PayloadFormat are the two remaining publish
	// properties a store keeps (Message Expiry is the third, Expiry below,
	// and arrives decremented by waiting time). PayloadFormat is a string -
	// "", "0" or "1" -
	// because the property is a byte that can be absent, and absent is a
	// different answer from zero.
	ContentType   string
	PayloadFormat string
	User          map[string]string
	// Expiry is the Message Expiry Interval as it arrived, nil when the
	// delivery carried none. Its value is the publisher's less the time the
	// message waited, so a test bounds it rather than matching it.
	Expiry *uint32
	// SubIDs is the Subscription Identifier property as it arrived, which
	// MQTT 5 carries as a list because one delivery answers every one of a
	// client's subscriptions that matched it.
	SubIDs []int
	// QoS is what the delivery actually carried, which is not always what
	// the subscription asked for: MQTT sends the lower of the subscription's
	// QoS and the publish's, so a worker asking for 2 on a queue is served
	// at 1 because saguin offers its records at 1 (invariant 6).
	QoS byte
	Ack func()
}

type Client struct {
	T testing.TB
	C *paho.Client
	// H and id are what `gone` needs to ask the broker whether it has
	// finished with this session. A client dialled without a harness -
	// dialAt, for a listener a test opened itself - leaves H nil, and
	// `gone` refuses it (needsHarness).
	H  *Harness
	ID string
	// SessionPresent is what the CONNACK reported. Zero means the broker
	// discarded the stored session, which is the one moment MQTT provides
	// for telling a consumer its position is gone (RFC 0003).
	SessionPresent bool
	// Conn is the socket under the client, so a test can break the link
	// the way a bad connection does rather than closing it politely.
	Conn net.Conn
	Mu   sync.Mutex
	In   []Received
	Ch   chan Received

	// SeekCode is the reason code the last seek was acknowledged with, kept
	// because a refusal rides the acknowledgement and a client that set no
	// Response Topic has nothing else to read.
	SeekCode byte

	// Gone carries a DISCONNECT the broker sent, which is how a refusal
	// mid-session reaches the client at all.
	Gone chan *paho.Disconnect

	// A handler that blocks on Hold is a consumer that has received a
	// record and not acknowledged it: Paho sends the PUBACK when the
	// handler returns. HoldPayload selects which record to stop on.
	Hold        chan struct{}
	HoldPayload string

	// OnRecord and onJob are called from the receive handler, for tests
	// that have to act on a delivery as it lands rather than afterwards.
	// The flaky-link tests need that: a client whose socket is about to be
	// cut may never get to read c.in at all.
	//
	// **onJob is handed a reply rather than being expected to find one.**
	// These are installed before paho.NewClient so they can fire during
	// Connect - which is the whole reason they are passed to the dial - and
	// during Connect the *client the dial will return does not exist yet. A
	// callback closing over that variable dereferences nil the first time a
	// resumed session is handed an un-acknowledged job at connect, and takes
	// the test binary with it. The reply is built from the handler's own
	// client, so there is nothing to close over.
	OnRecord func(topic string, offset uint64)
	OnJob    func(payload string, reply func(action string))
}

// Close ends the connection politely. The flaky-link tests dial the same
// client id repeatedly and need the previous one gone first; everything
// else relies on t.Cleanup.
func (c *Client) Close() {
	_ = c.C.Disconnect(&paho.Disconnect{ReasonCode: 0})
}

// Connect dials an ordinary client, and **does not ask for problem
// information** - which matters for one kind of test and for no other.
//
// **MQTT-3.1.2-29: a server may not put a Reason String or a User Property
// on any packet but PUBLISH, CONNACK and DISCONNECT unless the client set
// Request Problem Information.** So a test that reads the User Properties
// of a SUBACK, PUBACK or UNSUBACK through a client dialled here sees an
// empty list whatever the broker did, and passes while proving nothing -
// the vacuous pass the test rules exist to prevent. connectAsking dials one
// that asks, and returns a bare Paho client rather than this harness's -
// which is the whole of what it is for. Everything else wants this one,
// because not asking is what most clients do and is the condition the rest
// of the suite should run under.
//
// **A test that asks must also assert something the broker is meant to
// keep**, not only that something is absent: with problem information off
// every property disappears, so an absence-only assertion passes for the
// wrong reason either way. The acknowledgement-echo tests assert that the
// client's own `x-app` survives beside the reserved names being gone, which
// is what makes them fail rather than pass if this is ever dropped.
func Connect(t testing.TB, h *Harness, id string, cleanStart bool, manualAck bool) *Client {
	t.Helper()
	return Dial(t, h, id, cleanStart, manualAck, 0, 3600, 0)
}

// ConnectRx connects with an explicit Receive Maximum, which is the client
// half of the in-flight window saguin delivers append records within.
func ConnectRx(t testing.TB, h *Harness, id string, cleanStart, manualAck bool, rxMax uint16) *Client {
	t.Helper()
	return Dial(t, h, id, cleanStart, manualAck, rxMax, 3600, 0)
}

// ConnectExpiring connects with Session Expiry Interval 0, so the session -
// and with it the consumer's position - ends when the connection does.
func ConnectExpiring(t testing.TB, h *Harness, id string) *Client {
	t.Helper()
	return Dial(t, h, id, false, false, 0, 0, 0)
}

// ConnectMaxPacket connects declaring a Maximum Packet Size, which MQTT
// forbids the server exceeding (MQTT-3.1.2-24, -25). It is a durable
// session, because what the bound costs is only visible across a reconnect.
func ConnectMaxPacket(t testing.TB, h *Harness, id string, maxPacket uint32) *Client {
	t.Helper()
	return Dial(t, h, id, false, false, 0, 3600, maxPacket)
}

// ConnectAsking connects a bare Paho client that sets Request Problem
// Information, and hands back the client rather than the harness's wrapper
// because what these tests read is the acknowledgement rather than a
// delivery.
//
// The flag is the whole point of it. MQTT-3.1.2-29 lets a server withhold
// the Reason String from a PUBACK when a client did not ask for problem
// information, and Paho does not ask by default - so a test that took the
// default would find every refusal bare and report a broker that says
// nothing when it said everything. The rest of the suite keeps Paho's
// default deliberately: that default is what the upstream User Property
// defect needed to show itself.
func ConnectAsking(t testing.TB, h *Harness, id string) *paho.Client {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	expiry := uint32(3600)
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: id, CleanStart: true, KeepAlive: 0,
		Properties: &paho.ConnectProperties{
			SessionExpiryInterval: &expiry,
			RequestProblemInfo:    true,
		},
	})
	if err != nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("connect %s refused: %d", id, ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return c
}

// DialThrough connects to an address rather than to the harness, declaring
// what an ESP32-class client declares: a small prefetch, a small buffer and
// a session that outlives the connection.
//
// The address is the point - the flaky-link tests put a proxy in front of
// the broker so the link can be cut for real, and a client that dialled the
// harness directly would be immune to the thing under test.
// **The callbacks are given here rather than assigned afterwards**, and
// that is not style. paho starts its receive goroutine inside Connect, and
// a *resumed* session is pumped its backlog by OnSessionEstablished at
// connect - before any SUBSCRIBE. A callback installed after Connect
// therefore misses whatever arrived in between, and the flaky-link soak
// duly reported four records "never received" out of seven thousand, which
// would have read as invariant 1 failing in the broker. It was the
// instrument. Set before Connect, there is no window and no race.
func DialThrough(t testing.TB, addr, id string, d ESPDevice,
	onRecord func(topic string, offset uint64), onJob func(payload string, reply func(action string))) *Client {
	t.Helper()
	return DialAt(t, addr, "tcp", id, false, false, d.Window, d.Expiry, d.MaxPacket, onRecord, onJob)
}

func Dial(t testing.TB, h *Harness, id string, cleanStart, manualAck bool, rxMax uint16, expirySecs, maxPacket uint32) *Client {
	if DialUsername != "" {
		dialPassword = h.Authenticate(t, DialUsername)
		defer func() { dialPassword = "" }()
	}
	c := DialAt(t, h.Addr, h.Network, id, cleanStart, manualAck, rxMax, expirySecs, maxPacket, nil, nil)
	c.H, c.ID = h, id
	if DialUsername != "" {
		h.heldAs(t, c.Conn, id, DialUsername)
	} else {
		h.registered(t, c.Conn, c.C.Done(), id)
	}
	return c
}

// dialPassword is the password the next client dialled carries, set by Dial
// beside DialUsername.
var dialPassword string

func DialAt(t testing.TB, addr, network, id string, cleanStart, manualAck bool, rxMax uint16, expirySecs, maxPacket uint32,
	onRecord func(topic string, offset uint64), onJob func(payload string, reply func(action string))) *Client {
	t.Helper()
	var conn net.Conn
	var err error
	if network == "ws" {
		conn, err = DialWS(addr)
	} else {
		conn, err = net.Dial(network, addr)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cl := &Client{T: t, Conn: conn, Ch: make(chan Received, 256), Gone: make(chan *paho.Disconnect, 4)}
	// Before paho.NewClient, so the receive goroutine cannot start without
	// them. See dialThrough for what setting them afterwards cost.
	cl.OnRecord, cl.OnJob = onRecord, onJob

	cfg := paho.ClientConfig{
		Conn:                       conn,
		EnableManualAcknowledgment: manualAck,
		OnServerDisconnect: func(d *paho.Disconnect) {
			select {
			case cl.Gone <- d:
			default:
			}
		},
	}
	cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
		func(pr paho.PublishReceived) (bool, error) {
			r := Received{
				Topic:   pr.Packet.Topic,
				Payload: string(pr.Packet.Payload),
				Retain:  pr.Packet.Retain,
				QoS:     pr.Packet.QoS,
				User:    map[string]string{},
			}
			if pr.Packet.Properties != nil {
				r.RespTopic = pr.Packet.Properties.ResponseTopic
				r.CorrData = pr.Packet.Properties.CorrelationData
				r.ContentType = pr.Packet.Properties.ContentType
				if pf := pr.Packet.Properties.PayloadFormat; pf != nil {
					r.PayloadFormat = strconv.Itoa(int(*pf))
				}
				if e := pr.Packet.Properties.MessageExpiry; e != nil {
					v := *e
					r.Expiry = &v
				}
				// paho.golang surfaces one identifier, not the list MQTT
				// allows - so a delivery answering two of this client's
				// subscriptions reads as one here. The two-identifier case
				// is asserted on the wire instead, in
				// TestOneCopyCarriesEveryMatchingSubscriptionIdentifier.
				if id := pr.Packet.Properties.SubscriptionIdentifier; id != nil {
					r.SubIDs = []int{*id}
				}
				for _, u := range pr.Packet.Properties.User {
					r.User[u.Key] = u.Value
				}
			}
			pkt := pr.Packet
			r.Ack = func() { _ = pr.Client.Ack(pkt) }
			cl.Mu.Lock()
			cl.In = append(cl.In, r)
			onRecord, onJob := cl.OnRecord, cl.OnJob
			var hold chan struct{}
			if cl.Hold != nil && cl.HoldPayload == r.Payload {
				hold = cl.Hold
			}
			cl.Mu.Unlock()
			select {
			case cl.Ch <- r:
			default:
			}
			if onRecord != nil {
				if off, err := strconv.ParseUint(r.User["saguin-offset"], 10, 64); err == nil {
					onRecord(r.Topic, off)
				}
			}
			if onJob != nil && r.RespTopic != "" {
				answer := pr.Client
				topic, corr := r.RespTopic, r.CorrData
				onJob(r.Payload, func(action string) {
					_, _ = answer.Publish(context.Background(), &paho.Publish{
						Topic:      topic,
						QoS:        1,
						Payload:    []byte(action),
						Properties: &paho.PublishProperties{CorrelationData: corr},
					})
				})
			}
			if hold != nil {
				<-hold // not acknowledged until released
			}
			return true, nil
		},
	}
	cl.C = paho.NewClient(cfg)

	props := &paho.ConnectProperties{SessionExpiryInterval: &expirySecs}
	if rxMax > 0 {
		props.ReceiveMaximum = &rxMax
	}
	if maxPacket > 0 {
		props.MaximumPacketSize = &maxPacket
	}
	ca, err := cl.C.Connect(context.Background(), &paho.Connect{
		ClientID: id,
		// The user name, when a test needs one: it is the identity every
		// acl_file rule is written about, and a client without one reaches
		// no pattern. Paho only sends it when told to, and a CONNECT
		// without the flag is what an anonymous client is.
		Username:     DialUsername,
		UsernameFlag: DialUsername != "",
		Password:     []byte(dialPassword),
		PasswordFlag: dialPassword != "",
		CleanStart:   cleanStart,
		// No keepalive. Every test here runs in under a second, so a ping
		// buys nothing - and Paho sends it from its own goroutine, which is
		// a second writer on a connection the test is already writing to.
		KeepAlive:  0,
		Properties: props,
	})
	if err != nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("connect %s refused: %d", id, ca.ReasonCode)
	}
	// Kept because it is the one moment MQTT provides for telling a
	// consumer its stored position is gone (RFC 0003).
	cl.SessionPresent = ca.SessionPresent
	t.Cleanup(func() { _ = cl.C.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return cl
}

// Sub returns the SUBACK even when the client reports the refusal as an
// error, because a refusal is exactly what several of these tests assert.
func (c *Client) Sub(t testing.TB, filter string, qos byte) *paho.Suback {
	t.Helper()
	return c.SubWithID(t, filter, qos, 0)
}

// SubWithID subscribes carrying a Subscription Identifier, which MQTT 5
// makes the server echo on every delivery for that subscription. Zero sends
// none: the property is bounded to 1..268,435,455, so there is no value that
// means "absent" other than leaving it off.
func (c *Client) SubWithID(t testing.TB, filter string, qos byte, id int) *paho.Suback {
	t.Helper()
	sub := &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}},
	}
	if id != 0 {
		sub.Properties = &paho.SubscribeProperties{SubscriptionIdentifier: &id}
	}
	sa, err := c.C.Subscribe(context.Background(), sub)
	if sa != nil {
		return sa
	}
	if err != nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	return sa
}

// SubMany sends ONE SUBSCRIBE naming several filters, and returns the reason
// code for each in the order they were asked.
//
// **One packet, not several.** MQTT answers a SUBSCRIBE with one code per
// filter (MQTT-3.9.3-1), and saguin decides each of them inside a loop over
// the packet - so a refusal that returned from the loop rather than from the
// decision would answer the filters behind it with whatever the array held.
// Nothing that sends one filter at a time can see that.
func (c *Client) SubMany(t testing.TB, filters []string, qos byte) []byte {
	t.Helper()
	sub := &paho.Subscribe{}
	for _, f := range filters {
		sub.Subscriptions = append(sub.Subscriptions, paho.SubscribeOptions{Topic: f, QoS: qos})
	}
	sa, err := c.C.Subscribe(context.Background(), sub)
	if sa == nil {
		t.Fatalf("subscribe %v: %v", filters, err)
	}
	return sa.Reasons
}

// Settle waits until the broker has processed everything this client has
// sent, including the PUBACKs for records it has just received.
//
// **Without it a test that reconnects cannot tell saguin's replay from
// MQTT's own redelivery**, which is the trap position_test.go's header
// names: a persistent session re-sends its unacknowledged packets on
// reconnect, so a record reaches the consumer over the wire whether or not
// the stored position is right. A client library sends the PUBACK when its
// receive handler returns, and the broker processes it a moment later - so
// a disconnect issued the instant the last record arrives can leave one
// unacknowledged, and the first thing the next connection receives is that
// redelivery rather than the replay under test.
//
// Measured: TestASeekIsNotUndoneByTheReadingThatFollowsIt failed about once
// in 250
// runs on a loaded machine, reporting order-4 where the seek asked for
// order-2 - with the broker's own trace showing it had stored 2 and started
// the new consumer at 2, exactly as it promised.
//
// The barrier is a QoS 1 publish to a topic no channel claims, whose PUBACK
// proves the point: a client's packets are read and processed in order, so
// an answer to a packet sent after the PUBACKs is an answer sent after they
// were processed. It is a round trip rather than a sleep, so it is as fast
// as the broker and does not get slower on a busy machine.
func Settle(t testing.TB, c *Client) {
	t.Helper()
	c.Pub(t, "settle/barrier", "x")
}

// Acknowledged waits until the broker holds nothing in flight to c: every
// PUBACK c owed has been read and processed.
//
// **Settle cannot say this.** Paho sends a PUBACK from its receive
// goroutine once the handler returns, and Settle publishes from the test's,
// so the barrier can reach the broker before the acknowledgement it was
// meant to follow. The broker's own in-flight table is what the
// acknowledgement changes, so it is what is read - and the processing that
// follows it on the read loop (the consumer's position) finishes before that
// loop reads anything else, a DISCONNECT or the socket closing included.
func Acknowledged(t testing.TB, c *Client) {
	t.Helper()
	AwaitInFlight(t, c, 0)
}

// AwaitInFlight waits until the broker holds exactly n deliveries in flight
// to c's connection: what it sent less what c has acknowledged.
func AwaitInFlight(t testing.TB, c *Client, n int) {
	t.Helper()
	c.needsHarness(t, "AwaitInFlight")
	local := c.Conn.LocalAddr().String()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		cl, ok := c.H.Srv.Clients.Get(c.ID)
		if ok && cl.Net.Remote == local && cl.State.Inflight.Len() == n {
			return
		}
		if time.Now().After(deadline) {
			got := -1
			if ok {
				got = cl.State.Inflight.Len()
			}
			t.Fatalf("%s had %d deliveries in flight at the broker after 5s, want %d", c.ID, got, n)
		}
	}
}

// FannedOut publishes and does not return until the substrate has finished
// handing that publish to subscribers.
//
// **A PUBACK does not mean the fan-out has happened.** The substrate writes
// the acknowledgement and *then* delivers to subscribers - `processPublish`
// calls `WritePacket(ack)` before `publishToSubscribers` - so a publish call
// returns with the delivery still pending. A test that then connects a new
// client and subscribes can land inside that window, and the new client is
// handed the message **live**, with the retain flag clear. Every assertion
// of the form "this subscriber must be sent nothing" reads that as the rule
// being broken.
//
// It is not hypothetical and it is not rare under load: it took a full
// `make check` red, and the packet that arrived said `retain=false` - a live
// delivery, not the retained one the test was about.
//
// The proof is an observer subscribed *before* the publish. Once it has the
// message, the fan-out for that publish has run over a subscriber set that
// could not have included anybody who subscribes afterwards. That is an
// ordering fact rather than a duration, which is why this is not a sleep -
// a sleep is a guess that gets shorter every time the machine gets busier,
// which is exactly when it is needed.
func FannedOut(t testing.TB, h *Harness, id, topic, payload string, retain bool) {
	t.Helper()
	obs := DialProps(t, h, "fanout-observer-"+id)
	if _, err := obs.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
	}); err != nil {
		t.Fatalf("the fan-out observer could not subscribe to %q: %v", topic, err)
	}

	pub := DialProps(t, h, "fanout-publisher-"+id)
	if _, err := pub.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: 1, Retain: retain, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}

	if got := AwaitProp(t, obs); got == nil {
		t.Fatalf("the fan-out observer never saw %q, so nothing here can say whether the "+
			"delivery has happened yet", topic)
	}
	// **Unsubscribed before it goes, not merely disconnected.** dialProps
	// connects with a Session Expiry of an hour, so a DISCONNECT leaves the
	// session - and its subscription - alive at the broker, quietly queueing
	// every later publish for a client that is never coming back. Removing
	// the filter first leaves nothing behind for the test to trip over.
	if _, err := obs.C.Unsubscribe(context.Background(), &paho.Unsubscribe{
		Topics: []string{topic},
	}); err != nil {
		t.Fatalf("the fan-out observer would not unsubscribe: %v", err)
	}
	if err := obs.C.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
		t.Fatalf("the fan-out observer would not disconnect: %v", err)
	}
}

// AwaitCount waits until a client has received at least n records, and
// returns quietly if it never does - the caller asserts, so that the failure
// says what the count actually was.
//
// It replaces a sleep chosen to be "long enough", which is a figure picked on
// an idle machine and needed on a busy one. Waiting for the arrival is
// one-sided: being slow costs time here and changes no answer.
func AwaitCount(t testing.TB, c *Client, n int, within time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		if c.Count() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// SessionGone waits until the broker has finished with a client id, so a
// test reconnecting under it gets a fresh session rather than taking over
// the live one.
//
// **This is the signal `gone` says does not exist on the wire, read from
// the broker instead.** A DISCONNECT is one-way and nothing comes back, so a
// client can only wait a while and hope; the substrate's own client table
// knows. A reconnect that beats the teardown is a takeover - it inherits the
// session, its subscriptions and whatever was queued for it - and the test
// that follows then reads the old session's deliveries as the new one's.
//
// Asked of the table rather than slept through, because how long teardown
// takes is a fact about the machine. On a loaded CI runner 200ms was not
// enough: `TestAResumedSessionIsSentNothingFromTheRetainedStore` reconnected
// into its own live session and was handed the retained value it was
// asserting it would not get.
// **Every connection the id has had, not a count of disconnects.** A count
// read after the link is taken away races the broker, and a count of zero
// is satisfied by any earlier close or takeover under the same id - which
// returned at once while the connection this was called for was still
// being torn down. So it waits until the broker
// has finished with every connection begun under the id (Finished), which
// is what "the session is gone" means at every call site, and needs no
// count taken beforehand. SessionGoneAfter is the wait for one teardown
// while another connection under the id stays open, for a caller that took
// a Snapshot.
func SessionGone(t testing.TB, h *Harness, clientID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.Disconnects.Finished(clientID) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the broker had not finished with every connection under %q after 10s, so a "+
		"reconnect here would take one over rather than resume the session", clientID)
}

// SessionGoneAfter waits for the teardown a snapshot was taken for
// (disconnectHook.FinishedSince), so a client id used more than once in a
// test - closed before, or taken over - waits for *its* teardown rather
// than returning on another connection's.
func SessionGoneAfter(t testing.TB, h *Harness, clientID string, before Snapshot) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// **The hook, not `cl.Closed()`.** The substrate calls `cl.Stop`
		// before it calls the disconnect hooks, so Closed goes true while
		// saguin's OnDisconnect - which returns a held job and records its
		// attempt - has not run. Waiting on Closed ended early in twelve of
		// 1,200 measured rounds; that is the window the sleep it replaced
		// was standing in for, narrowed rather than closed.
		if h.Disconnects.FinishedSince(before) {
			return
		}
		// A session that expires at once is deleted from the table after
		// the hooks, so its absence is the same signal arriving by another
		// route - and covers a client the hook never saw, such as one whose
		// CONNECT was refused. Not while a connection under the id is still
		// open: absent then is not yet registered (disconnectHook).
		if _, ok := h.Srv.Clients.Get(clientID); !ok && !h.Disconnects.Opening(clientID) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the broker had not finished with the session for %q after 10s, so a "+
		"reconnect here would take it over rather than resume it", clientID)
}

// LinkCut breaks a client's socket the way a bad connection does, and waits
// for the broker to have finished with the session.
//
// **The same wait as `gone`, for the other way a session ends.** A sweep for
// this shape that looks for `Disconnect` misses it: cutting the socket is
// the same event spelled differently, and the sleep that follows one is
// standing in for the same signal. It cost a test that asserts a queue
// attempt count survives a migration - the third job is returned by its
// session ending, and if the broker has not got there before the store is
// closed, the attempt was never spent and the count restarts.
func LinkCut(t testing.TB, c *Client) {
	t.Helper()
	c.needsHarness(t, "LinkCut")
	before := c.H.Disconnects.Snapshot(c.ID)
	_ = c.Conn.Close()
	SessionGoneAfter(t, c.H, c.ID, before)
}

// needsHarness fails a test that asks whether the broker has finished with
// a client dialled without a harness to ask.
//
// **There is no clock to fall back on.** LinkCut and Gone used to sleep
// 300ms for such a client, which is a guess at how long a teardown takes:
// a reconnect that beat it was a takeover, and the test read the old
// session's state as the new one's. A test that dials with DialAt or
// DialThrough sets H and ID itself when the broker it reaches is a harness.
func (c *Client) needsHarness(t testing.TB, what string) {
	t.Helper()
	if c.H == nil || c.ID == "" {
		t.Fatalf("%s was asked to wait for the broker to finish with a client dialled "+
			"without a harness; set the client's H and ID", what)
	}
}

// Gone disconnects a consumer and waits for the broker to have finished with
// it, so that a test asserting what the *next* connection receives is
// reading a fresh session rather than the old one carrying on.
//
// **The wait is the disconnect hook's signal.** A DISCONNECT is one-way:
// the client sends it and closes, and nothing comes back to say the broker
// has torn the session down. Reconnect before it has and the new connection
// is a *takeover* - it inherits the live session, cursor included, which is
// deliberate and is what stops a consumer being told Session Present = 1
// and then served nothing. Measured on a machine with six busy cores,
// reconnecting with no wait: 69 of 500 runs got the inherited cursor rather
// than a fresh one.
//
// A client dialled without a harness has no hook to read, so it is a test
// failure rather than a guess (needsHarness).
func Gone(t testing.TB, c *Client) {
	t.Helper()
	c.needsHarness(t, "Gone")
	Settle(t, c)
	// **Snapshotted before the DISCONNECT goes**, so what is waited for is
	// this teardown rather than one that has already happened.
	before := c.H.Disconnects.Snapshot(c.ID)
	_ = c.C.Disconnect(&paho.Disconnect{ReasonCode: 0})
	SessionGoneAfter(c.T, c.H, c.ID, before)
}

func (c *Client) Pub(t testing.TB, topic, payload string) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// PubRetainedAt publishes with the retain flag at a chosen QoS, which is
// what a test about the QoS a retained value comes back at needs: the two
// halves of [MQTT-3.8.4-8] are the subscription's QoS and this one, and
// every other helper here fixes it at 1.
func (c *Client) PubRetainedAt(t testing.TB, topic, payload string, qos byte) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: qos, Retain: true, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// PubPlainAt publishes without the RETAIN flag, for the tests that need a
// record to cross a bridge as ordinary traffic rather than as state. Its
// pair above sets the flag; keeping both spelled out is what lets a test
// say which of the two it is about.
func (c *Client) PubPlainAt(t testing.TB, topic, payload string, qos byte) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: qos, Retain: false, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// PubProps publishes carrying MQTT 5 publish properties. The randomised
// scenarios need it: a publish without properties gives a store nothing to
// lose, so two providers serving it back agree whether or not one of them
// keeps a replaced value's columns.
func (c *Client) PubProps(t testing.TB, topic, payload string, props *paho.PublishProperties) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte(payload), Properties: props,
	}); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

// PubRetained publishes with the retain flag, which on a broadcast topic
// means the broker keeps the value for whoever subscribes next - and needs
// a retained store configured, or the publish is refused.
func (c *Client) PubRetained(t testing.TB, topic, payload string) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: topic, QoS: 1, Retain: true, Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("publish retained %s: %v", topic, err)
	}
}

func (c *Client) Respond(t testing.TB, topic, action string, corr []byte) {
	t.Helper()
	if _, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic:      topic,
		QoS:        1,
		Payload:    []byte(action),
		Properties: &paho.PublishProperties{CorrelationData: corr},
	}); err != nil {
		t.Fatalf("respond %s: %v", action, err)
	}
}

func (c *Client) Await(t testing.TB, d time.Duration) (Received, bool) {
	t.Helper()
	select {
	case r := <-c.Ch:
		return r, true
	case <-time.After(d):
		return Received{}, false
	}
}

func (c *Client) Count() int {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return len(c.In)
}

// All is everything received, in arrival order, for the same reason
// payloads reads c.in rather than the channel.
func (c *Client) All() []Received {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	out := make([]Received, len(c.In))
	copy(out, c.In)
	return out
}

// Payloads is everything received, in arrival order. It reads c.in rather
// than the channel because the channel is bounded and drops when it is
// full, which is exactly what a test about a long backlog must not do.
func (c *Client) Payloads() []string {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	out := make([]string, 0, len(c.In))
	for _, r := range c.In {
		out = append(out, r.Payload)
	}
	return out
}

func Drain(c *Client) {
	for {
		select {
		case <-c.Ch:
		default:
			return
		}
	}
}

func HoldOneJobPerQueue(t *testing.T, workers, queues int) {
	// More than four workers can hold, so work is always still waiting.
	const backlog = 30

	names := []string{"jobs", "tasks"}[:queues]
	var chans []*channel.Channel
	for _, n := range names {
		// **No lease runs out inside this test.** Nothing here is about
		// expiry, and a job whose lease ran out is offered again - to the
		// worker that joins as the end marker, or to its first holder - which
		// reads as a second offer and an answer counted twice. With the
		// harness's two seconds, a run slowed past them failed so: under a
		// full conntrack table each dial waited a second for its SYN to be
		// sent again, and the worker that joined last was offered jobs-1.
		chans = append(chans, &channel.Channel{Name: n, Type: channel.Queue,
			VisibilityTimeout: int64(time.Hour / time.Second), MaxAttempts: MaxAttempts})
	}
	h := StartRandom(t, chans)

	ws := make([]*Client, workers)
	for i := range ws {
		ws[i] = Connect(t, h, fmt.Sprintf("worker-%d", i), true, false)
		for _, n := range names {
			ws[i].Sub(t, "$saguin/queue/"+n, 1)
		}
	}

	p := Connect(t, h, "producer", true, false)
	for i := 1; i <= backlog; i++ {
		for _, n := range names {
			p.Pub(t, fmt.Sprintf("%s/task/%d", n, i), fmt.Sprintf("%s-%d", n, i))
		}
	}
	queueOf := func(r Received) string { q, _, _ := strings.Cut(r.Payload, "-"); return q }

	// **Phase one: nobody answers.**
	held := make([]map[string]Received, workers)
	for i := range held {
		held[i] = map[string]Received{}
	}
	offered := 0
	poll := func() {
		for i, w := range ws {
			select {
			case r := <-w.Ch:
				if prev, dup := held[i][queueOf(r)]; dup {
					t.Fatalf("worker-%d was offered %q while it held %q from the same "+
						"queue, unanswered", i, r.Payload, prev.Payload)
				}
				held[i][queueOf(r)] = r
				offered++
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	for deadline := time.Now().Add(3 * time.Second); offered < workers*queues &&
		time.Now().Before(deadline); {
		poll()
	}
	if offered != workers*queues {
		t.Fatalf("%d workers on %d queues were offered %d jobs, want %d: one from each "+
			"queue for every worker", workers, queues, offered, workers*queues)
	}
	// **The end marker is a worker that joins now.** Records are offered in
	// offset order (RFC 0003 "Ordering"), so with every worker holding one
	// from each queue the next job of each - number workers+1 - is the
	// newcomer's. A second offer to a worker already holding one would have
	// taken it, and would have reached that worker: polled once more below,
	// and again in phase two before each answer.
	late := Connect(t, h, "worker-late", true, false)
	for _, n := range names {
		late.Sub(t, "$saguin/queue/"+n, 1)
	}
	lateHeld := map[string]Received{}
	for range names {
		r, ok := late.Await(t, 3*time.Second)
		if !ok {
			t.Fatalf("a worker joining with work waiting was offered nothing")
		}
		lateHeld[queueOf(r)] = r
	}
	for _, n := range names {
		if r, want := lateHeld[n], fmt.Sprintf("%s-%d", n, workers+1); r.Payload != want {
			t.Fatalf("the worker that joined last was offered %q from %s, want %q - the "+
				"next in offset order: the job before it went to a worker already "+
				"holding one", r.Payload, n, want)
		}
	}
	poll()
	ws, held = append(ws, late), append(held, lateHeld)

	// **Phase two: everybody answers.**
	var mu sync.Mutex
	acks := map[string]int{}
	returned := 0
	finished := make(chan struct{})
	var took time.Duration
	start := time.Now()

	var wg sync.WaitGroup
	for i, w := range ws {
		wg.Add(1)
		go func(i int, w *Client, mine map[string]Received) {
			defer wg.Done()
			// Everything that has arrived, each checked against what this
			// worker still holds.
			take := func(r Received) bool {
				if prev, dup := mine[queueOf(r)]; dup {
					t.Errorf("worker-%d was offered %q while it held %q from the same "+
						"queue, unanswered", i, r.Payload, prev.Payload)
					return false
				}
				mine[queueOf(r)] = r
				return true
			}
			drain := func() bool {
				for {
					select {
					case r := <-w.Ch:
						if !take(r) {
							return false
						}
					default:
						return true
					}
				}
			}
			for {
				if !drain() {
					return
				}
				if len(mine) == 0 {
					select {
					case r := <-w.Ch:
						take(r)
						continue
					case <-finished:
						return
					case <-time.After(3 * time.Second):
						return // idle: the count below says how far it got
					}
				}
				for _, n := range names {
					r, ok := mine[n]
					if !ok {
						continue
					}
					time.Sleep(time.Millisecond)
					// Once more before letting go, so an offer that came early
					// is seen as the second it is.
					if !drain() {
						return
					}
					delete(mine, n)

					action := "ack"
					mu.Lock()
					if r.User["saguin-attempt"] == "1" &&
						(strings.HasSuffix(r.Payload, "0") || strings.HasSuffix(r.Payload, "5")) {
						action = "return"
						returned++
					} else {
						acks[r.Payload]++
						if len(acks) == queues*backlog && took == 0 {
							took = time.Since(start)
							close(finished)
						}
					}
					mu.Unlock()

					if _, err := w.C.Publish(context.Background(), &paho.Publish{
						Topic: r.RespTopic, QoS: 1, Payload: []byte(action),
						Properties: &paho.PublishProperties{CorrelationData: r.CorrData},
					}); err != nil {
						t.Errorf("worker-%d answering %q: %v", i, r.Payload, err)
						return
					}
					break
				}
			}
		}(i, w, held[i])
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i <= backlog; i++ {
		for _, n := range names {
			if got := acks[fmt.Sprintf("%s-%d", n, i)]; got != 1 {
				t.Errorf("%s-%d was acknowledged %d times, want once", n, i, got)
			}
		}
	}
	// The return path has to have run for the count above to cover it.
	if want := queues * backlog / 5; returned != want {
		t.Errorf("%d jobs were returned on their first attempt, want %d", returned, want)
	}
	if took == 0 || took > 3*time.Second {
		t.Errorf("answering %d jobs as they arrived took %v: an answer that waits for the "+
			"next tick to offer the next job holds each worker to five jobs a second per "+
			"queue", queues*backlog, took)
	}
}

// Whether this broker admits an MQTT 3.1.1 client, asked at the packet
// level because a v5 client library cannot express the question.
//
// A helper rather than a line inside one test because two tests ask it:
// the one below, which is about the gate, and the one after, which is
// about two documents that describe the gate.
func ThreeOneOneAdmitted(t *testing.T) bool {
	t.Helper()
	h := Start(t)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// CONNECT, protocol name "MQTT", protocol level 4 (3.1.1), clean
	// session, keepalive 60, client id "old".
	pkt := []byte{
		0x10, 0x0f, // CONNECT, remaining length 15
		0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
		0x04,       // protocol level 4 = MQTT 3.1.1
		0x02,       // connect flags: clean session
		0x00, 0x3c, // keep alive 60
		0x00, 0x03, 'o', 'l', 'd', // client id
	}
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("write: %v", err)
	}

	// A refusal is either a CONNACK carrying a non-zero return code or a
	// closed connection. Both are refusals; only a zero return code is not.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Logf("MQTT 3.1.1 CONNECT refused by closing the connection: %v", err)
		return false
	}
	if buf[0] != 0x20 {
		t.Fatalf("expected CONNACK, got %#x", buf[0])
	}
	if buf[3] != 0x00 {
		// 0x01 = unacceptable protocol version, in 3.1.1's own code space.
		t.Logf("MQTT 3.1.1 CONNECT refused with return code %#x", buf[3])
		return false
	}
	return true
}

// DialWS opens an MQTT-over-WebSocket connection and presents it as a
// net.Conn, which is what a stock MQTT client wants.
//
// It retries, because the WebSocket listener binds inside Serve rather
// than Init: until that happens the port refuses connections. Retrying the
// real connection is deliberate - a throwaway probe would be one more
// connection racing the server's shutdown, which is the defect that used
// to make this suite flaky.
func DialWS(addr string) (net.Conn, error) {
	d := &websocket.Dialer{Subprotocols: []string{"mqtt"}}
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, _, err := d.Dial("ws://"+addr+"/", nil)
		if err == nil {
			return &WSConn{Conn: c}, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WSConn presents a WebSocket as a byte stream. MQTT over WebSocket puts
// MQTT bytes in binary frames, and frame boundaries mean nothing to the
// protocol above, so reads run on across them.
type WSConn struct {
	*websocket.Conn
	r io.Reader

	// gorilla permits one writer at a time, and an MQTT client writes from
	// more than one goroutine - a keepalive ping while a publish is in
	// flight is enough. A net.Conn is expected to tolerate that.
	w sync.Mutex
}

func (c *WSConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			_, r, err := c.Conn.NextReader()
			if err != nil {
				return 0, err
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil // frame finished; the next one continues the stream
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *WSConn) Write(p []byte) (int, error) {
	c.w.Lock()
	defer c.w.Unlock()
	if err := c.Conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *WSConn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.Conn.SetWriteDeadline(t)
}

// MigrateToSQLite is what `saguin --snapshots-to-sqlite` does, less the
// argument checking the command's own tests cover: read every snapshot in
// the directory, and write each file's channels into the database in one
// transaction.
func MigrateToSQLite(t testing.TB, dir, path string) {
	t.Helper()

	snaps, err := store.NewDir(dir, "e2e").LoadAll()
	if err != nil {
		t.Fatalf("read the snapshots: %v", err)
	}
	if len(snaps) == 0 {
		t.Fatal("the snapshot directory holds nothing to migrate")
	}
	db, err := sqlite.Open(path, "e2e")
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	for _, s := range snaps {
		if err := db.Import(s.Channels); err != nil {
			t.Fatalf("import: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close the database: %v", err)
	}
}

// MigrateToSnapshots is `saguin --sqlite-to-snapshots`, the same way.
func MigrateToSnapshots(t testing.TB, path, dir string) {
	t.Helper()

	db, err := sqlite.OpenForExport(path)
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	channels, err := db.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close the database: %v", err)
	}

	// A queue and its dead-letter channel share a file, so the move between
	// them cannot be recorded by half (invariant 5).
	byName := map[string]*store.Snapshot{}
	var snaps []*store.Snapshot
	for _, c := range channels {
		if c.Kind == store.KindQueue {
			s := &store.Snapshot{Channels: []store.ChannelState{c}}
			byName[c.Name] = s
			snaps = append(snaps, s)
		}
	}
	for _, c := range channels {
		if c.Kind == store.KindQueue {
			continue
		}
		if queue, ok := strings.CutSuffix(c.Name, channel.DLQSuffix); ok && byName[queue] != nil {
			byName[queue].Channels = append(byName[queue].Channels, c)
			continue
		}
		snaps = append(snaps, &store.Snapshot{Channels: []store.ChannelState{c}})
	}
	if err := store.NewDir(dir, "e2e").Save(snaps); err != nil {
		t.Fatalf("write the snapshots: %v", err)
	}
}

// BridgeReaderName is the reader name an outbound bridge rule holds its
// position under, so a test naming one builds it from the store's own
// constructor rather than from a second copy of the scheme.
func BridgeReaderName(bridge string) string { return store.BridgeReader(bridge) }

// MetricValue scrapes the operations listener and reads one series out of
// the catalogue by name and labels.
//
// It fails rather than returning zero when the series is absent, and says
// how much it read: a metric that is missing and a metric that is genuinely
// 0 are the same number to a caller, and this whole test turns on telling
// them apart.
func MetricValue(t testing.TB, addr, series string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics answered %d, want 200", resp.StatusCode)
	}
	for _, line := range strings.Split(string(body), "\n") {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("%s carried %q, which is not a number", series, rest)
		}
		return v
	}
	t.Fatalf("%s is not in the catalogue at all, in %d bytes scraped", series, len(body))
	return 0
}

// LogValue reads a numeric key out of a slog text line. Written for
// records_missed, which is the only notice anyone gets that a connected
// consumer was restarted at the floor.
func LogValue(line, key string) (uint64, bool) {
	i := strings.Index(line, key+"=")
	if i < 0 {
		return 0, false
	}
	rest := line[i+len(key)+1:]
	end := strings.IndexAny(rest, " \n")
	if end >= 0 {
		rest = rest[:end]
	}
	n, err := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
	return n, err == nil
}

// ConnectDurable connects with a session that outlives the connection -
// Clean Start 0 and a session expiry - which is what gives a consumer a
// stored position at all, and so the only kind of client a seek applies to.
func ConnectDurable(t testing.TB, h *Harness, id string) *Client {
	t.Helper()
	return Connect(t, h, id, false, false)
}

// Seek publishes a Seek and returns the broker's reply. The reply is
// written straight to this client rather than routed to subscribers, so
// nothing subscribes to the response topic - receiving it at all is part
// of what these tests check.
func (c *Client) Seek(t testing.TB, ch, payload string) Received {
	t.Helper()
	// **A refused seek acknowledges 0x83**, which paho reports as an error,
	// so an error here is not on its own a failure - for half these tests it
	// is the subject. The code is kept for whoever wants to assert on it and
	// only the absence of any acknowledgement is fatal.
	ack, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic:   "$saguin/consumer/" + ch + "/seek",
		QoS:     1,
		Payload: []byte(payload),
		Properties: &paho.PublishProperties{
			ResponseTopic:   "seek-reply",
			CorrelationData: []byte("corr-" + ch + "-" + payload),
		},
	})
	if ack == nil {
		t.Fatalf("seek %s %s: no acknowledgement: %v", ch, payload, err)
	}
	c.Mu.Lock()
	c.SeekCode = ack.ReasonCode
	c.Mu.Unlock()
	r, ok := c.Await(t, 3*time.Second)
	if !ok {
		t.Fatalf("no reply to a seek of %s to %s", ch, payload)
	}
	if r.Topic != "seek-reply" {
		t.Fatalf("the reply arrived on %q, want the response topic", r.Topic)
	}
	if got, want := string(r.CorrData), "corr-"+ch+"-"+payload; got != want {
		t.Errorf("correlation data %q, want %q", got, want)
	}
	return r
}

// seekAck is the reason code on the last seek's acknowledgement: 0 when it
// applied, 0x83 when the broker refused it.
func (c *Client) seekAck() byte {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.SeekCode
}

// SeekQuietly publishes a seek with no response topic, for the tests where
// records are still arriving and a reply would race them into the same
// channel.
//
// **It returns the acknowledgement's reason code**, which is the only thing
// such a client can read: with no Response Topic there is no reply, and the
// comment here used to say the PUBACK meant the broker "had handled the
// seek". It did not - every refusal acknowledged 0x00 as well, so this said
// a seek had worked when the broker had thrown it away as malformed. The
// link-churn soak issued 266 of those and counted every one as a success.
func (c *Client) SeekQuietly(t testing.TB, ch, payload string) byte {
	t.Helper()
	ack, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: "$saguin/consumer/" + ch + "/seek", QoS: 1, Payload: []byte(payload),
	})
	if ack == nil {
		t.Fatalf("seek %s %s: no acknowledgement: %v", ch, payload, err)
	}
	return ack.ReasonCode
}

// Distinct is the set of payloads that arrived.
func Distinct(rs []Received) map[string]bool {
	out := map[string]bool{}
	for _, r := range rs {
		out[r.Payload] = true
	}
	return out
}

func Payloads(rs []Received) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Payload)
	}
	return out
}

// CollectUntil gathers records until enough is true or the deadline passes.
func CollectUntil(t testing.TB, c *Client, within time.Duration, enough func([]Received) bool) []Received {
	t.Helper()
	var out []Received
	deadline := time.Now().Add(within)
	for !enough(out) {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		r, ok := c.Await(t, left)
		if !ok {
			break
		}
		out = append(out, r)
	}
	return out
}

// PubackClock records when each of one client's PUBACKs reached the
// upstream. It is the only place `ack_interval` is observable: the value
// never goes on the wire, so what it decides is the arrival time of packets
// that would be sent either way.
type PubackClock struct {
	mqtt.HookBase
	Mu       sync.Mutex
	ClientID string
	seen     []time.Time
}

func (c *PubackClock) ID() string { return "puback-clock" }

func (c *PubackClock) Provides(b byte) bool { return b == mqtt.OnPacketRead }

// Watch names the client whose acknowledgements are counted. The producer
// on this upstream is acknowledged *by* the broker rather than acknowledging
// to it, so it never appears here - but naming the bridge keeps that true of
// anything added later.
func (c *PubackClock) Watch(clientID string) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.ClientID = clientID
}

func (c *PubackClock) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Puback {
		c.Mu.Lock()
		if cl.ID == c.ClientID {
			c.seen = append(c.seen, time.Now())
		}
		c.Mu.Unlock()
	}
	return pk, nil
}

// Await blocks until n acknowledgements have arrived, and returns what did
// arrive rather than failing, so the caller can say how many were missing.
func (c *PubackClock) Await(t testing.TB, n int, within time.Duration) []time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		c.Mu.Lock()
		got := append([]time.Time(nil), c.seen...)
		c.Mu.Unlock()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// StartUpstream runs a second, entirely ordinary MQTT broker: no saguin
// hooks, no channels, nothing that has heard of an offset. It is what a
// bridge is for.
//
// It is mochi, so that `make test` needs nothing installed - and there is
// one thing to know before anybody scales a test up against it. **mochi
// stops delivering once a subscriber's Receive Maximum is used up and does
// not resume**, which is the same defect saguin already works around for
// its own queue deliveries: the server withholds a packet for want of send
// quota and sends it only when something else nudges the session. Measured
// on 2026-08-13, 200 records at a Receive Maximum of 20: 66 arrived in 30
// seconds against this broker, and 200 in 28ms once the window was raised
// above the whole batch. The same 200 through mosquitto 2.1.2 took 106ms.
//
// So a test here must stay under the window, which is why this one moves
// ten records and not a thousand. It is testing the *link* - a cut, a
// resumed session, a redelivery - and those need a handful of records and a
// broker that can be reached into from the test. Throughput belongs to
// mosquitto, where it was measured, and the figures are in internal/bridge.
func StartUpstream(t testing.TB) (*mqtt.Server, string) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// InlineClient so a test can publish at the upstream without opening a
	// client of its own; it changes nothing a bridge sees.
	srv := mqtt.New(&mqtt.Options{Logger: log, InlineClient: true})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("upstream hook: %v", err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "up", Address: "127.0.0.1:0"})
	if err := srv.AddListener(tcp); err != nil {
		t.Fatalf("upstream listener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv, tcp.Address()
}

// BridgeConfig writes a bridge configuration and loads it, so the test runs
// against rules the configuration actually accepts rather than against a
// hand-built approximation of them.
func BridgeConfig(t testing.TB, upstreamAddr string, tuning ...string) config.Bridge {
	t.Helper()
	yaml := fmt.Sprintf(`broker:
  id: t
`+MemStorage+`channels:
  events:
    type: append
bridges:
  head-office:
    peer: tcp://%s
    client_id: vessel-07
%s    topics:
      - filter: fleet/+/telemetry/#
        topic: events/telemetry/$1/$#
        direction: in
`, upstreamAddr, strings.Join(tuning, ""))
	path := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write the bridge configuration: %v", err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own bridge configuration does not load: %v", err)
	}
	return f.BridgeSet()[0]
}

// DialUpstream connects an ordinary MQTT 5 producer to the upstream broker.
func DialUpstream(t testing.TB, addr, id string) *paho.Client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial peer: %v", err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn, ClientID: id})
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: id, KeepAlive: 30, CleanStart: true,
	})
	if err != nil {
		t.Fatalf("connect peer: %v", err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("upstream connack 0x%02X", ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return c
}

// AwaitRegistered waits until the substrate has registered a client id's
// connection, and returns it.
//
// **A CONNACK does not say it has happened.** The broker writes the CONNACK
// and only then registers the connection, so that nothing can be written to a
// client before its CONNACK [MQTT-3.2.0-1]. A test that reads the registry the
// moment its CONNACK arrives is reading a promise the broker never made: the
// same read in internal/proxyproto found no client in 8 runs of 100.
func AwaitRegistered(t testing.TB, srv *mqtt.Server, clientID string, within time.Duration) *mqtt.Client {
	t.Helper()
	for deadline := time.Now().Add(within); ; time.Sleep(time.Millisecond) {
		if cl, ok := srv.Clients.Get(clientID); ok {
			return cl
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q was not registered %s after its CONNACK, so nothing below is about "+
				"the client that connected", clientID, within)
			return nil
		}
	}
}

// WaitForBridge blocks until the bridge is connected upstream and has a
// subscription there.
func WaitForBridge(t testing.TB, upstream *mqtt.Server, clientID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := upstream.Clients.Get(clientID); ok {
			if subs := upstream.Topics.Subscribers("fleet/vessel-07/telemetry/hold/psi"); len(subs.Subscriptions) > 0 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the bridge never subscribed upstream")
}

// WaitForBridgeGone blocks until the upstream has finished with the bridge's
// connection.
func WaitForBridgeGone(t testing.TB, upstream *mqtt.Server, clientID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cl, ok := upstream.Clients.Get(clientID)
		if !ok || cl.Closed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the bridge never disconnected upstream")
}

// Collect gathers up to n records, returning what arrived before the
// deadline rather than failing, so the caller can say what was missing.
func Collect(t testing.TB, c *Client, n int, within time.Duration) []Received {
	t.Helper()
	out := make([]Received, 0, n)
	deadline := time.Now().Add(within)
	for len(out) < n {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		r, ok := c.Await(t, left)
		if !ok {
			break
		}
		out = append(out, r)
	}
	return out
}

// Proxy is a TCP link a test can cut and restore. It is how an outage is
// made to last: a broker-side disconnect is over in a millisecond, so
// records "published while the link was down" would race the reconnect and
// the test would assert whichever won.
//
// Cutting closes every connection through it and refuses new ones, which is
// what a dropped link looks like from both ends - the far broker keeps the
// session, and the near end sees its socket go.
type Proxy struct {
	Addr string

	Mu    sync.Mutex
	down  bool
	conns []net.Conn
	up    map[string]string // client's address -> the proxy's own address to the broker
}

// upstream is the address the broker sees for the client at from.
func (p *Proxy) upstream(from string) (string, bool) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	a, ok := p.up[from]
	return a, ok
}

func NewProxy(t testing.TB, to string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{Addr: ln.Addr().String()}
	t.Cleanup(func() { _ = ln.Close(); p.Cut() })

	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			p.Mu.Lock()
			down := p.down
			p.Mu.Unlock()
			if down {
				_ = in.Close()
				continue
			}
			out, err := net.Dial("tcp", to)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.Mu.Lock()
			p.conns = append(p.conns, in, out)
			if p.up == nil {
				p.up = map[string]string{}
			}
			p.up[in.RemoteAddr().String()] = out.LocalAddr().String()
			p.Mu.Unlock()
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return p
}

func (p *Proxy) Cut() {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	p.down = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *Proxy) Restore() {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	p.down = false
}

// PropClient is a Paho client that keeps every property of every delivery,
// which the harness's own client discards.
type PropClient struct {
	C  *paho.Client
	Ch chan *paho.Publish
}

func DialProps(t testing.TB, h *Harness, id string) *PropClient {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	pc := &PropClient{Ch: make(chan *paho.Publish, 64)}
	cfg := paho.ClientConfig{Conn: conn}
	cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
		func(pr paho.PublishReceived) (bool, error) {
			select {
			case pc.Ch <- pr.Packet:
			default:
			}
			return true, nil
		},
	}
	pc.C = paho.NewClient(cfg)
	expiry := uint32(3600)
	ca, err := pc.C.Connect(context.Background(), &paho.Connect{
		ClientID: id, CleanStart: true, KeepAlive: 0,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	if err != nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("connect %s refused: %d", id, ca.ReasonCode)
	}
	t.Cleanup(func() { _ = pc.C.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return pc
}

func (pc *PropClient) Sub(t testing.TB, filter string, qos byte) {
	t.Helper()
	sa, err := pc.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}},
	})
	if sa == nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	if sa.Reasons[0] > 2 {
		t.Fatalf("subscribe %s refused with 0x%02X", filter, sa.Reasons[0])
	}
}

func AwaitProp(t testing.TB, pc *PropClient) *paho.Publish {
	t.Helper()
	select {
	case p := <-pc.Ch:
		return p
	case <-time.After(3 * time.Second):
		return nil
	}
}

// LockedWriter serialises writes from the bridge's goroutines so a test can
// read the log back without racing the logger.
type LockedWriter struct {
	Mu *sync.Mutex
	W  *strings.Builder
}

func (l *LockedWriter) Write(p []byte) (int, error) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	return l.W.Write(p)
}

// ConnackProperties connects, reads the CONNACK, and returns its property
// block indexed by identifier, along with the whole packet so that a
// failing assertion can print what the broker actually said rather than
// what the test expected to find.
//
// It walks the block by identifier and steps over each value by its own
// width, rather than searching the bytes for one. A property's value can
// hold any byte, so a search finds 0x27 inside somebody else's number and
// reports a property that is not there - and this test's whole job is to
// tell present from absent.
func ConnackProperties(t testing.TB, addr, id string) (map[byte][]byte, string) {
	t.Helper()
	return ConnackPropertiesAsking(t, addr, id, nil)
}

// ConnackPropertiesAsking is connackProperties with a CONNECT that carries
// properties of its own - the encoded property block, without its length
// prefix. It is how a test asks for something and reads what it was given.
func ConnackPropertiesAsking(t testing.TB, addr, id string, connectProps []byte) (map[byte][]byte, string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var body []byte
	body = append(body, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05)
	body = append(body, 0x02)       // clean start
	body = append(body, 0x00, 0x3C) // keepalive
	body = append(body, byte(len(connectProps)))
	body = append(body, connectProps...)
	body = append(body, byte(len(id)>>8), byte(len(id)))
	body = append(body, id...)
	if _, err := conn.Write(append([]byte{0x10, byte(len(body))}, body...)); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	fh := make([]byte, 2)
	if _, err := io.ReadFull(conn, fh); err != nil {
		t.Fatalf("no CONNACK: %v", err)
	}
	pk := make([]byte, int(fh[1]))
	if _, err := io.ReadFull(conn, pk); err != nil {
		t.Fatalf("short CONNACK: %v", err)
	}
	shown := fmt.Sprintf("% 02x", append(fh, pk...))
	if len(pk) < 3 {
		t.Fatalf("the CONNACK carries no properties at all: %s", shown)
	}
	if pk[1] != 0x00 {
		t.Fatalf("the connection was refused with 0x%02X: %s", pk[1], shown)
	}

	// Widths by identifier, for the properties a CONNACK may carry.
	one := map[byte]bool{0x24: true, 0x25: true, 0x28: true, 0x29: true, 0x2A: true}
	two := map[byte]bool{0x13: true, 0x21: true, 0x22: true}
	four := map[byte]bool{0x11: true, 0x27: true}
	str := map[byte]bool{0x12: true, 0x1A: true, 0x1C: true, 0x1F: true}

	out := map[byte][]byte{}
	block := pk[3 : 3+int(pk[2])]
	for i := 0; i < len(block); {
		id := block[i]
		i++
		var width int
		switch {
		case one[id]:
			width = 1
		case two[id]:
			width = 2
		case four[id]:
			width = 4
		case str[id]:
			// A UTF-8 string property is two bytes of length and then that
			// many bytes.
			if i+2 > len(block) {
				t.Fatalf("truncated property 0x%02X in %s", id, shown)
			}
			width = int(block[i])<<8 | int(block[i+1])
			i += 2
		default:
			// Stopping here would silently report every property after
			// this one as absent, which is the answer these tests are
			// about, so it is a failure rather than a break.
			t.Fatalf("cannot step over property 0x%02X, so what follows it was not read: %s", id, shown)
		}
		if i+width > len(block) {
			t.Fatalf("truncated property 0x%02X in %s", id, shown)
		}
		out[id] = block[i : i+width]
		i += width
	}
	return out, shown
}

// AllowEverything is an Authorizer that permits every operation, which is
// what makes it the instrument for the rule below: if a grant could widen a
// structural refusal, this is the one that would do it.
type AllowEverything struct{}

func (AllowEverything) Allows(string, string, string, bool) bool { return true }

// Names no client, so every one takes the broker-wide figures - which is
// what an acl_file with no `limits:` anywhere does.
func (AllowEverything) PublishLimits(string) (int, int64, bool) { return 0, 0, false }

func (AllowEverything) HasPublishLimits() bool { return false }

// Names no client_ids either, so any client id may carry any user name -
// which is what an acl_file that has not asked for the key does.
func (AllowEverything) AllowsClientID(string, string) bool { return true }
func (AllowEverything) WithheldGrants(string, string) int  { return 0 }

// And denies no session capability, which is what a file with no
// `broker: sessions` denial does.
func (AllowEverything) DeniesFeature(string, string, string) bool { return false }

// DenyEverything refuses every operation, and proves the seam is reached at
// all - a rule that cannot narrow anything is as broken as one that widens,
// and the two failures look identical from the passing side.
type DenyEverything struct{}

func (DenyEverything) Allows(string, string, string, bool) bool { return false }

func (DenyEverything) PublishLimits(string) (int, int64, bool) { return 0, 0, false }

func (DenyEverything) HasPublishLimits() bool { return false }

// **It refuses operations, not connections.** Refusing the client id here
// would stop every client at the door, and the cases below would then pass
// without ever reaching the seam they are about.
func (DenyEverything) AllowsClientID(string, string) bool { return true }
func (DenyEverything) WithheldGrants(string, string) int  { return 0 }

// Nor session capabilities, for the same reason: the cases below are about
// operations, and a refused Will would stop some of them at the door.
func (DenyEverything) DeniesFeature(string, string, string) bool { return false }

// SendRawConnect opens an MQTT 5 session with no properties.
func SendRawConnect(t testing.TB, conn net.Conn, id string) {
	t.Helper()
	var body []byte
	body = append(body, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00)
	body = append(body, byte(len(id)>>8), byte(len(id)))
	body = append(body, id...)
	if _, err := conn.Write(MqttPacket(0x10, body)); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if kind, _ := AwaitPacket(t, conn); kind>>4 != 2 {
		t.Fatalf("no CONNACK: packet type %d", kind>>4)
	}
}

// PublishWithAlias sends a QoS 1 publish carrying Topic Alias n. An empty
// topic is how a client uses an alias it has already established.
func PublishWithAlias(t testing.TB, conn net.Conn, topic string, n uint16, payload string) {
	t.Helper()
	var body []byte
	body = append(body, byte(len(topic)>>8), byte(len(topic)))
	body = append(body, topic...)
	body = append(body, 0x00, 0x01) // packet identifier
	body = append(body, 0x03)       // property length
	body = append(body, 0x23, byte(n>>8), byte(n))
	body = append(body, payload...)
	if _, err := conn.Write(MqttPacket(0x32, body)); err != nil {
		t.Fatalf("write publish: %v", err)
	}
}

// AwaitPacket reads one whole packet: its first byte and its body.
func AwaitPacket(t testing.TB, conn net.Conn) (byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	first := make([]byte, 1)
	if _, err := io.ReadFull(conn, first); err != nil {
		t.Fatalf("reading a packet: %v", err)
	}
	var remaining, mult int
	mult = 1
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			t.Fatalf("reading a remaining length: %v", err)
		}
		remaining += int(b[0]&127) * mult
		mult *= 128
		if b[0]&128 == 0 {
			break
		}
	}
	rest := make([]byte, remaining)
	if _, err := io.ReadFull(conn, rest); err != nil {
		t.Fatalf("short packet: %v", err)
	}
	return first[0], rest
}

// AwaitPuback reads the next packet and reports its reason code, failing if
// it is not a PUBACK - so a DISCONNECT arriving instead is named rather
// than read as a code.
func AwaitPuback(t testing.TB, conn net.Conn) byte {
	t.Helper()
	kind, body := AwaitPacket(t, conn)
	if kind>>4 != 4 {
		t.Fatalf("expected a PUBACK, got packet type %d (% 02x)", kind>>4, body)
	}
	if len(body) > 2 {
		return body[2]
	}
	return 0x00 // an omitted reason byte reads as success
}

// MqttPacket frames a packet body with its remaining length encoded the way
// MQTT asks - seven bits at a time, high bit as the continuation.
//
// A single byte is right up to 127 and silently wrong above it, which is
// how a test asking about an over-long Will topic came to report "no
// CONNACK" from a broker that had simply been sent a malformed packet.
// That is the instrument reporting on itself, and it is worth one helper
// to never do it again.
func MqttPacket(kind byte, body []byte) []byte {
	out := []byte{kind}
	n := len(body)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 128
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// ConnectWithWillProps arms a Will carrying the five publish properties a
// Will may have, so a test can ask whether they survive it firing.
//
// Hand-built like connectWithWill, and for the same reason: Paho offers no
// way to set Will properties, and a helper that could not send them could
// not fail. The property bytes are written out here rather than taken as a
// parameter, so the packet a test drives is one thing a reader can check
// against the specification's property identifiers.
func ConnectWithWillProps(t testing.TB, h *Harness, id, topic string, delay uint32) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	if _, err := conn.Write(WillConnectBytes(id, topic, delay)); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	fh := make([]byte, 2)
	if _, err := io.ReadFull(conn, fh); err != nil {
		t.Fatalf("read connack: %v", err)
	}
	body := make([]byte, fh[1])
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read connack body: %v", err)
	}
	if len(body) < 2 || body[1] != 0x00 {
		t.Fatalf("the connection was refused with 0x%02X", body[1])
	}
	return conn
}

// WillConnectBytes builds a CONNECT arming a Will that carries all six of
// the publish properties MQTT 5 lets one carry.
//
// **The bytes are laid out here rather than by a client library**, and the
// reason is measured rather than assumed: paho's own encoder writes a
// seven-byte Will Properties block carrying only the User Property and
// drops the other five silently. A test built on it would assert against
// properties that never left the client.
//
// One layout, two transports. connectWithWillProps dials TCP with it and
// TestAWillOverAWebSocketFiresWithItsProperties sends it in a WebSocket
// frame, so the two cannot drift into testing different packets.
func WillConnectBytes(id, topic string, delay uint32) []byte {
	return willConnectBytesWithExpiry(id, topic, delay, 0)
}

// willConnectBytesWithExpiry is the same CONNECT carrying a Session Expiry
// Interval, which is what decides whether the session outlives the connection
// - and so whether a Will Delay Interval is the whole of the wait (MQTT 5
// section 3.1.3.2.2). Zero is a session that ends with its connection, which
// is what the CONNECT above sends.
//
// **The expiry is written into the properties block rather than spliced into
// the bytes afterwards**, and that is not a style choice: this packet is
// longer than 127 bytes, so its remaining length is two bytes wide, and a
// splice that assumed one byte landed inside the variable header. The broker
// then read the Will Delay Interval as the expiry - a test that passed while
// sending something else entirely.
func willConnectBytesWithExpiry(id, topic string, delay, expiry uint32) []byte {
	return willConnectBytes(id, topic, delay, expiry, true)
}

// willConnectBytes is willConnectBytesWithExpiry with Clean Start chosen.
func willConnectBytes(id, topic string, delay, expiry uint32, clean bool) []byte {
	// No topic, no Will: the flag with no Will behind it is a CONNECT the
	// broker refuses, and a caller that wants a plain connection on this
	// layout should get one rather than a refusal.
	var flags byte
	if clean {
		flags = 0x02 // clean start
	}
	if topic != "" {
		flags |= 0x04 // will flag, QoS 0
	}
	var vh []byte
	vh = append(vh, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, flags, 0x00, 0x3c)
	if expiry > 0 {
		vh = append(vh, 0x05, 0x11, byte(expiry>>24), byte(expiry>>16), byte(expiry>>8), byte(expiry))
	} else {
		vh = append(vh, 0x00) // no CONNECT properties
	}
	vh = append(vh, byte(len(id)>>8), byte(len(id)))
	vh = append(vh, id...)
	if topic == "" {
		return MqttPacket(0x10, vh)
	}

	str := func(v string) []byte {
		return append([]byte{byte(len(v) >> 8), byte(len(v))}, v...)
	}
	var wp []byte
	if delay > 0 {
		wp = append(wp, 0x18, byte(delay>>24), byte(delay>>16), byte(delay>>8), byte(delay))
	}
	wp = append(wp, 0x01, 0x01)                   // Payload Format Indicator: UTF-8
	wp = append(wp, 0x02, 0x00, 0x00, 0x00, 0x3c) // Message Expiry Interval: 60
	wp = append(wp, 0x03)                         // Content Type
	wp = append(wp, str("application/json")...)
	wp = append(wp, 0x08) // Response Topic
	wp = append(wp, str("where/to/answer")...)
	wp = append(wp, 0x09) // Correlation Data
	wp = append(wp, str("ticket-7")...)
	wp = append(wp, 0x26) // User Property, which already survived
	wp = append(wp, str("tenant")...)
	wp = append(wp, str("acme")...)
	vh = append(vh, byte(len(wp)))
	vh = append(vh, wp...)

	vh = append(vh, byte(len(topic)>>8), byte(len(topic)))
	vh = append(vh, topic...)
	payload := []byte("i-died")
	vh = append(vh, byte(len(payload)>>8), byte(len(payload)))
	vh = append(vh, payload...)

	return MqttPacket(0x10, vh)
}

// WillConnectWithExpiry opens a connection carrying a Will and a Session
// Expiry Interval of its own, which is what says whether the session outlives
// the connection - and so whether a Will Delay Interval is the whole of the
// wait (MQTT 5 section 3.1.3.2.2).
func WillConnectWithExpiry(t testing.TB, h *Harness, id, topic string, delay, expiry uint32) net.Conn {
	t.Helper()
	return willDial(t, h, willConnectBytesWithExpiry(id, topic, delay, expiry))
}

// WillResumeWithExpiry is WillConnectWithExpiry with Clean Start 0: a client
// coming back to the session it left, which is what a device on a flapping
// link sends. A clean start ends that session instead, and its waiting Will
// is published (RFC 0003 "Sessions").
func WillResumeWithExpiry(t testing.TB, h *Harness, id, topic string, delay, expiry uint32) net.Conn {
	t.Helper()
	return willDial(t, h, willConnectBytes(id, topic, delay, expiry, false))
}

// willDial sends a CONNECT and returns the connection once it is accepted.
func willDial(t testing.TB, h *Harness, connect []byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := conn.Write(connect); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	cp, err := pahopackets.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	ca, ok := cp.Content.(*pahopackets.Connack)
	if !ok || ca.ReasonCode != 0 {
		t.Fatalf("the connection was refused: %+v", cp.Content)
	}
	return conn
}

// ConnectWithWill opens a raw connection carrying a Will and returns the
// socket, so a test can kill it without a DISCONNECT - which is the only
// way a Will fires at all.
//
// Raw bytes rather than Paho, because a Will is set in the CONNECT and
// fires on an unclean disconnect: Paho will do the first and not reliably
// the second, and the whole question is what arrives when a socket dies
// mid-session. It returns the CONNACK's reason code, because for some of
// these the refusal *is* the answer.
func ConnectWithWill(t testing.TB, h *Harness, id, topic string, retain bool,
	delay uint32, willQoS byte) (net.Conn, byte) {
	t.Helper()
	return ConnectWithWillExpiring(t, h, id, topic, retain, delay, willQoS, 0)
}

// ConnectWithWillExpiring is the same, with a Session Expiry Interval of its
// own - which is what gives a Will Delay Interval a session to wait in.
//
// **A delay needs one.** A Will is discarded when its session ends, and a
// Clean Start session with no expiry ends at the disconnect, so a delay on
// one is a delay with nothing to wait inside: RFC 0003's delay section says
// so, and a device that wants flap protection asks for an expiry at least as
// long as its delay. Every test about what a delay *does* therefore arms it
// on a session that outlives its connection; the ones that pass 0 are about
// something else, and their Wills fire at the disconnect.
func ConnectWithWillExpiring(t testing.TB, h *Harness, id, topic string, retain bool,
	delay uint32, willQoS byte, expiry uint32) (net.Conn, byte) {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// **Will QoS is two bits of the CONNECT flags**, not a Will property,
	// which is the whole reason this parameter exists rather than being
	// passed alongside the payload. It was missing, so every Will this file
	// has ever armed went at QoS 0 - and a Will at QoS 1 was never delivered
	// at all for eleven days with the table over it green.
	flags := byte(0x02 | 0x04) // clean start + will flag
	if retain {
		flags |= 0x20
	}
	flags |= willQoS << 3
	var vh []byte
	vh = append(vh, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, flags, 0x00, 0x3c)
	if expiry > 0 {
		vh = append(vh, 0x05, 0x11, byte(expiry>>24), byte(expiry>>16), byte(expiry>>8), byte(expiry))
	} else {
		vh = append(vh, 0x00) // no CONNECT properties
	}
	vh = append(vh, byte(len(id)>>8), byte(len(id)))
	vh = append(vh, id...)

	var wp []byte
	if delay > 0 {
		wp = append(wp, 0x18, byte(delay>>24), byte(delay>>16), byte(delay>>8), byte(delay))
	}
	vh = append(vh, byte(len(wp)))
	vh = append(vh, wp...)
	vh = append(vh, byte(len(topic)>>8), byte(len(topic)))
	vh = append(vh, topic...)
	payload := []byte("i-died")
	vh = append(vh, byte(len(payload)>>8), byte(len(payload)))
	vh = append(vh, payload...)

	if _, err := conn.Write(MqttPacket(0x10, vh)); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	fh := make([]byte, 2)
	if _, err := io.ReadFull(conn, fh); err != nil {
		t.Fatalf("no CONNACK: %v", err)
	}
	rest := make([]byte, int(fh[1]))
	if _, err := io.ReadFull(conn, rest); err != nil {
		t.Fatalf("short CONNACK: %v", err)
	}
	if len(rest) < 2 {
		t.Fatalf("CONNACK too short to carry a reason: % 02x", rest)
	}
	return conn, rest[1]
}

// Kill ends a connection the way a dead device does: no DISCONNECT, and no
// lingering close, so the broker sees the socket go rather than a polite
// goodbye. A Will fires only on this.
func Kill(t testing.TB, conn net.Conn) {
	t.Helper()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

func WillDelivered(t *testing.T, topic string, stored bool, qos byte) {
	h := Start(t)

	watcher := Connect(t, h, "watcher", true, false)
	watcher.Sub(t, "loose/#", 1)
	// A marker on the same filter, from a client of its own: the watcher
	// receiving it is the proof that it is subscribed, and later that
	// nothing published before it is still on its way.
	marker := Connect(t, h, "marker", true, false)
	expect := func(want, why string) {
		t.Helper()
		got, ok := watcher.Await(t, 3*time.Second)
		if !ok || got.Payload != want {
			t.Fatalf("the watcher was sent %q on %q (%v), want %q: %s",
				got.Payload, got.Topic, ok, want, why)
		}
	}
	marker.Pub(t, "loose/marker", "subscribed")
	expect("subscribed", "a watcher that is not subscribed proves nothing")

	conn, code := ConnectWithWill(t, h, "dying", topic, false, 0, qos)
	if code != 0x00 {
		t.Fatalf("the connection was refused with 0x%02X", code)
	}
	Kill(t, conn)

	if !stored {
		// Broadcast: whoever is subscribed at the time receives it,
		// and nothing keeps it.
		got, ok := watcher.Await(t, 3*time.Second)
		if !ok {
			t.Fatal("nothing arrived: a Will on a broadcast topic is delivered by the server's fan-out")
		}
		if got.Topic != topic || got.Payload != "i-died" {
			t.Fatalf("received %q %q, want %q i-died", got.Topic, got.Payload, topic)
		}
		// Exactly once. saguin publishes the Will itself and returns
		// an empty one to the substrate; a second copy would mean the
		// substrate had fanned it out as well, which is the road that
		// carried a wildcard topic past every rule. Both roads run when
		// the connection ends, before the marker is published, so a
		// second copy arrives ahead of it - at the Will's own QoS, since a
		// durable watcher is sent QoS 0 through its connection's queue and
		// QoS 1 and 2 from the broadcast log, and nothing orders the two.
		marker.PubPlainAt(t, "loose/marker", "after", qos)
		expect("after", "anything before the marker is the Will taking two roads")
		return
	}

	// A channel: the record must be *there* afterwards, which is
	// the half a Will published past the publish path never had. A
	// consumer that did not exist when the device died is the way
	// to see it.
	late := Connect(t, h, "late", true, false)
	switch {
	case strings.HasPrefix(topic, "jobs/"):
		late.Sub(t, "$saguin/queue/jobs", 1)
	default:
		late.Sub(t, strings.SplitN(topic, "/", 2)[0]+"/#", 1)
	}

	got, ok := late.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("a consumer arriving after the device died received nothing, " +
			"so the Will was not stored in the channel")
	}
	if got.Payload != "i-died" {
		t.Fatalf("received %q, want i-died", got.Payload)
	}
	// Everything a publish gets: an identity and a position.
	if got.User["saguin-id"] == "" {
		t.Error("the Will's record carries no saguin-id, so it did not go through the publish path")
	}
	if strings.HasPrefix(topic, "jobs/") && got.User["saguin-attempt"] == "" {
		t.Error("a Will into a queue arrived without an attempt count, so it is not an ordinary job")
	}
}

// Connect2 is connect with a name that does not shadow the raw CONNECT
// bytes above.
func Connect2(t testing.TB, h *Harness, id string) *Client {
	t.Helper()
	return Connect(t, h, id, true, false)
}

// RequirePasswords turns on the password file for a harness already
// running, so clients connected before it stay connected and every CONNECT
// after it must authenticate as "real" / "hunter2".
func RequirePasswords(t *testing.T, h *Harness) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clients.passwd")
	pf := passwd.New(path)
	if err := pf.Set("real", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	users, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	h.B.SetCredentials(users, false)
}

// AckAll acknowledges everything a manual-acknowledgement client is
// holding, which is what frees its in-flight window.
func AckAll(c *Client) {
	for _, m := range c.All() {
		if m.Ack != nil {
			m.Ack()
		}
	}
}

// disconnectHook records that the broker has finished with a session, and
// exists because nothing the substrate exposes says so on its own.
//
// **Registered after saguin's hook, which is what makes it the signal.**
// Hooks run in the order they were added, so this one is reached only once
// saguin's OnDisconnect has returned - and saguin's is where a held job is
// returned and its attempt recorded. `cl.Closed()` is *not* that moment: the
// substrate calls `cl.Stop` before `OnDisconnect` (server.go, detachClient),
// so a wait on Closed ends while the work a test is about to assert on has
// not happened. Measured at twelve early returns in 1,200 rounds.
//
// `Clients.Delete` would do for a session that expires at once, since it
// follows the hooks - but a session with an expiry stays in the table, and
// then there is nothing to watch but this.
//
// **It also sees each connection begin**, at OnConnect, so that a client id
// absent from the table can be told apart from one about to be in it. The
// substrate writes the CONNACK before it registers the connection, so a test
// that closed its socket on reading the CONNACK found its id absent, and
// sessionGone returned before the disconnect had run. Every connection that
// reaches OnConnect is stopped when it ends, refused or not (attachClient's
// deferred Stop), so one that is still open is one not yet finished with.
type disconnectHook struct {
	mqtt.HookBase
	Mu      sync.Mutex
	seen    map[string]int
	opening map[string][]*mqtt.Client
	// begun is every connection begun under an id, registered the ones the
	// server added to its table - whose teardown will run OnDisconnect - and
	// done the ones OnDisconnect has finished with (Finished).
	begun      map[string][]*mqtt.Client
	registered map[*mqtt.Client]bool
	done       map[*mqtt.Client]bool
	// hold is a test seam, nil unless a test sets it (HoldDisconnects): it
	// runs in OnDisconnect before the connection is recorded as finished, so
	// a test can hold one connection's teardown back from every wait that
	// reads this hook - a takeover's, whose order is otherwise the
	// scheduler's and at one P never the one the test is about.
	hold func(*mqtt.Client)
}

// HoldDisconnects sets the function each OnDisconnect runs before recording
// the connection finished, or clears it with nil. See disconnectHook.hold.
func (h *disconnectHook) HoldDisconnects(f func(*mqtt.Client)) {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	h.hold = f
}

func (h *disconnectHook) ID() string { return "test-disconnect" }

func (h *disconnectHook) Provides(b byte) bool {
	return b == mqtt.OnDisconnect || b == mqtt.OnConnect || b == mqtt.OnSessionRegistered
}

func (h *disconnectHook) OnConnect(cl *mqtt.Client, _ packets.Packet) error {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	if h.opening == nil {
		h.opening = map[string][]*mqtt.Client{}
	}
	h.opening[cl.ID] = append(h.open(cl.ID), cl)
	if h.begun == nil {
		h.begun = map[string][]*mqtt.Client{}
	}
	h.begun[cl.ID] = append(h.begun[cl.ID], cl)
	return nil
}

// OnSessionRegistered marks a connection the server has added to its table,
// which is the point from which its end runs OnDisconnect.
func (h *disconnectHook) OnSessionRegistered(cl *mqtt.Client, _ packets.Packet) {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	if h.registered == nil {
		h.registered = map[*mqtt.Client]bool{}
	}
	h.registered[cl] = true
}

// Finished reports whether the broker has finished with every connection
// begun under id: each one's OnDisconnect has run, or it closed without
// ever being registered - a CONNECT refused after OnConnect, which no
// disconnect follows.
//
// **Per connection, never a count.** A count cannot say whose teardown it
// was: on an id that had already been closed once, or taken over, the
// earlier connection's disconnect satisfied a wait for the current one's,
// and SessionGone returned while the connection it was called for was
// still being torn down - 47 times at 8 sites in an instrumented
// run.
func (h *disconnectHook) Finished(id string) bool {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	for _, cl := range h.begun[id] {
		if !h.finished(cl) {
			return false
		}
	}
	return true
}

// open is the connections begun under id that are still open, forgetting
// those that ended. Callers hold Mu.
func (h *disconnectHook) open(id string) []*mqtt.Client {
	cls := slices.DeleteFunc(h.opening[id], (*mqtt.Client).Closed)
	if len(cls) == 0 {
		delete(h.opening, id)
		return nil
	}
	h.opening[id] = cls
	return cls
}

// Opening reports whether a connection begun under id is still open.
func (h *disconnectHook) Opening(id string) bool {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	return len(h.open(id)) > 0
}

func (h *disconnectHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.Mu.Lock()
	hold := h.hold
	h.Mu.Unlock()
	if hold != nil {
		hold(cl) // outside Mu, so the waits that read this hook are not held too
	}
	h.Mu.Lock()
	defer h.Mu.Unlock()
	if h.seen == nil {
		h.seen = map[string]int{}
	}
	h.seen[cl.ID]++
	if h.done == nil {
		h.done = map[*mqtt.Client]bool{}
	}
	h.done[cl] = true
}

// Snapshot is what a wait for one teardown is measured against: the
// connections under an id the broker had not finished with when it was
// taken, and how many it had finished with by then.
type Snapshot struct {
	id    string
	live  []*mqtt.Client
	count int
}

// finished reports whether the broker is done with cl. Callers hold Mu.
func (h *disconnectHook) finished(cl *mqtt.Client) bool {
	return h.done[cl] || (!h.registered[cl] && cl.Closed())
}

// Snapshot captures the connections under id not yet finished with, for
// SessionGoneAfter. Take it before the close the wait is for.
func (h *disconnectHook) Snapshot(id string) Snapshot {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	snap := Snapshot{id: id, count: h.seen[id]}
	for _, cl := range h.begun[id] {
		if !h.finished(cl) {
			snap.live = append(snap.live, cl)
		}
	}
	return snap
}

// FinishedSince reports whether the teardown a snapshot was taken for is
// over: every connection live at the snapshot is finished, and at least one
// connection under the id has finished since.
//
// **Every captured connection, not the next disconnect.** Straight after a
// takeover the id has two connections not yet finished with - the one taken
// over, whose teardown can land late, and the one the test is about to end.
// A count satisfied by the first returned while the second was still being
// torn down. The second
// condition is for a snapshot taken before its connection began: what
// finishes then is a connection begun after it, which is the one meant.
func (h *disconnectHook) FinishedSince(snap Snapshot) bool {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	for _, cl := range snap.live {
		if !h.finished(cl) {
			return false
		}
	}
	return h.seen[snap.id] > snap.count
}

// Count is how many times the broker has finished with this client id.
func (h *disconnectHook) Count(id string) int {
	h.Mu.Lock()
	defer h.Mu.Unlock()
	return h.seen[id]
}

// startedHook closes a channel when the server has finished starting. It
// exists so the harness can hand back a broker that is wholly built rather
// than one still running its own startup, which is a race every test that
// asserts on broker state immediately would otherwise take.
type startedHook struct {
	mqtt.HookBase
	Ch   chan struct{}
	once sync.Once
}

func (h *startedHook) ID() string { return "test-started" }

func (h *startedHook) Provides(b byte) bool { return b == mqtt.OnStarted }

// Closed once. Serve is called once per harness, but a hook that panicked
// on a second call would turn a change in the substrate into a confusing
// failure a long way from its cause.
func (h *startedHook) OnStarted() { h.once.Do(func() { close(h.Ch) }) }

// KvGet asks for one topic's current value and returns the reply.
//
// The request is QoS 1 because that is the only QoS a point read has: a
// refusal rides the PUBACK, and QoS 0 has none. Awaiting the reply is safe
// here for the reason the helpers that broke this suite were not - the
// receive handler collects every packet into the client's own channel, so
// nothing read here is a packet swallowed from somewhere else.
func (c *Client) KvGet(t testing.TB, key string, corr []byte) Received {
	t.Helper()
	props := &paho.PublishProperties{ResponseTopic: "kv-reply"}
	if corr != nil {
		props.CorrelationData = corr
	}
	ack, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: ChannelKVGet, QoS: 1, Payload: []byte(key), Properties: props,
	})
	if ack == nil {
		t.Fatalf("no PUBACK for a point read of %q: %v", key, err)
	}
	if ack.ReasonCode != 0 {
		// The sentence is empty unless the client asked for problem
		// information, and this one deliberately does not - so the code is
		// what to read here. connectAsking is the client that sees both.
		t.Fatalf("reading %q was refused 0x%02X (reason string %q; this client does "+
			"not set Request Problem Information)", key, ack.ReasonCode, AckReason(ack))
	}
	r, ok := c.Await(t, 3*time.Second)
	if !ok {
		t.Fatalf("no reply to a point read of %q", key)
	}
	if r.Topic != "kv-reply" {
		t.Fatalf("the reply to %q arrived on %q, want kv-reply", key, r.Topic)
	}
	return r
}

func AckReason(ack *paho.PublishResponse) string {
	if ack == nil || ack.Properties == nil {
		return ""
	}
	return ack.Properties.ReasonString
}

const ChannelKVGet = "$saguin/kv/get"

// ConnectAs dials with a user name and password and returns what the broker
// answered, rather than failing on a refusal - a refusal is what most of
// these assert, and a helper that fatals on one can only test the happy
// half.
//
// Paho is used here for the reason the suite uses it everywhere: it sends
// what it is told to send. A client that validated credentials locally
// would make "the broker refused" and "the library refused" look the same.
func ConnectAs(t testing.TB, h *Harness, id, user, password string) byte {
	t.Helper()
	conn, err := net.Dial(h.Network, h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	var expiry uint32
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID:   id,
		CleanStart: true,
		KeepAlive:  0,
		Username:   user,
		Password:   []byte(password),
		// Paho only sends the user name when told to, and a CONNECT without
		// the flag is what an anonymous client is.
		UsernameFlag: user != "",
		PasswordFlag: password != "",
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	if ca == nil {
		t.Fatalf("connect %s: no CONNACK at all: %v", id, err)
	}
	if ca.ReasonCode == 0 {
		_ = c.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}
	return ca.ReasonCode
}

// ClientCert issues a certificate for a name, signed by a CA this test also
// makes, and returns the CA pool a listener trusts it with.
func ClientCert(t *testing.T, commonName string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	return ClientCertFrom(t, "Test Client CA", commonName)
}

// ClientCertFrom is the same, with the authority's own name given.
//
// **Two authorities in one test must not share a name.** crypto/tls decides
// whether to present a certificate by comparing its issuer's name against the
// names the server advertises - the key is never consulted - so two test CAs
// both called "Test Client CA" make a certificate from the wrong one look
// acceptable to the client and get it sent. That is fine for a case about a
// certificate being refused, and it silently disarms one about a certificate
// being withheld, which is how this argument was nearly lost.
func ClientCertFrom(t *testing.T, caName, commonName string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: caName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("client certificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// WriteClientPair writes a client certificate and its key where a
// configuration can name them, which is the only form a bridge takes: a
// bridge reads paths from the file, never a certificate a test handed it.
func WriteClientPair(t *testing.T, c tls.Certificate) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = filepath.Join(dir, "client-cert.pem")
	keyFile = filepath.Join(dir, "client-key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: c.Certificate[0]}), 0o600); err != nil {
		t.Fatalf("write the client certificate: %v", err)
	}
	der, err := x509.MarshalECPrivateKey(c.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatalf("marshal the client key: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{
		Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write the client key: %v", err)
	}
	return certFile, keyFile
}

// DialAsking connects a client and subscribes with the property that asks
// for deletions, which is what a bridge feeding a copy sends.
//
// The state arriving in the same breath as the SUBACK is not swallowed:
// Paho's own read loop dispatches publishes to the client's handler while
// Subscribe waits for the acknowledgement, so this helper never reads the
// socket itself. A helper that did would eat the very records the test is
// about and then report an empty channel about a broker that delivered.
func DialAsking(t testing.TB, h *Harness, id, filter string) *Client {
	t.Helper()
	c := Connect(t, h, id, true, false)
	if _, err := c.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
		Properties: &paho.SubscribeProperties{
			User: paho.UserProperties{{Key: "saguin-deletions", Value: "1"}},
		},
	}); err != nil {
		t.Fatalf("subscribe asking for deletions: %v", err)
	}
	return c
}

// WriteYAML puts a configuration where config.Load can read it.
func WriteYAML(t testing.TB, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	return path
}

// ConnectWatcher reports the CleanStart flag of every CONNECT the upstream
// receives, which is the only place that decision is observable.
type ConnectWatcher struct {
	mqtt.HookBase
	Seen chan bool
}

func (w *ConnectWatcher) ID() string { return "connect-watcher" }

func (w *ConnectWatcher) Provides(b byte) bool { return b == mqtt.OnConnect }

func (w *ConnectWatcher) OnConnect(_ *mqtt.Client, pk packets.Packet) error {
	select {
	case w.Seen <- pk.Connect.Clean:
	default:
	}
	return nil
}

// ConnectNamed dials with a user name and keeps the client, which is what
// separates it from connectAs: the identity is the thing under test rather
// than the CONNACK, so the connection has to survive to publish on.
//
// The name authenticates against the harness's own password file (see
// Authenticate), and the broker is checked to hold it: that is the field an
// `acl_file` rule is about.
func ConnectNamed(t testing.TB, h *Harness, id, user string) *paho.Client {
	t.Helper()
	password := h.Authenticate(t, user)
	conn, err := net.Dial(h.Network, h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	expiry := uint32(3600)
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: id, CleanStart: true, KeepAlive: 0,
		Username: user,
		// Paho only sends the name when told to, and a rule about %u
		// against a client that never sent one is a test of the empty
		// identity.
		UsernameFlag: true,
		Password:     []byte(password),
		PasswordFlag: true,
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	if ca == nil {
		t.Fatalf("connect %s: no CONNACK at all: %v", id, err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("connect %s refused: 0x%02X", id, ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	h.heldAs(t, conn, id, user)
	return c
}

// AwaitScrape opens an operations listener and scrapes it until the named
// metric stops reading zero, or the deadline passes - returning the last
// line it saw either way.
//
// **Polled rather than read once**, and that is not flake-proofing. There
// is nothing joining "the publisher called Publish 15,000 times" to "the
// broker has fanned all of them out": a QoS 0 publish is answered by
// nothing, so the client cannot tell, and the first version of this scraped
// immediately and reported a broker that had dropped nothing when it had
// not yet been given the chance to.
//
// The minimum scrape interval is zero here for the same reason: RFC 0005
// lets an operator hold a scraper off, and a test polling faster than that
// would be measuring the rate limiter.
func AwaitScrape(t testing.TB, h *Harness, metric string, within time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	stop, err := h.B.ServeOperations(TCPOnly(addr), 0, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	line := ""
	deadline := time.Now().Add(within)
	for {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			t.Fatalf("scrape: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read the scrape: %v", err)
		}
		line = ""
		for _, l := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(l, metric+" ") {
				line = l
			}
		}
		if line == "" {
			t.Fatalf("the scrape carries no %s line at all, so the catalogue and the "+
				"code disagree about whether it exists:\n%s", metric, body)
		}
		if !strings.HasSuffix(line, " 0") || time.Now().After(deadline) {
			return line
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitUntil polls until a condition holds or the deadline passes, and says
// which. A sleep long enough to be safe is a slow test and a sleep short
// enough to be quick is a flaky one.
func WaitUntil(d time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ok()
}

// AwaitStoredPosition polls until a channel's lowest stored consumer
// position reaches want.
//
// It reads the position the *store* holds rather than the one the cursor
// does, which is the whole point: a test about what survives a restart has
// to wait for the number that survives it, and that one is written every
// broker.session.ack_commit_interval rather than when the acknowledgement
// arrives.
func AwaitStoredPosition(t testing.TB, h *Harness, channel string, want uint64, within time.Duration) {
	t.Helper()
	series := fmt.Sprintf("saguin_channel_consumer_position_min{channel=%q}", channel)
	if last, ok := awaitSeries(t, h, series, within, func(v uint64) bool { return v == want }); !ok {
		t.Fatalf("the stored position for %q never reached %d within %s; last read %q",
			channel, want, within, last)
	}
}

// AwaitFloorPast polls until a channel's floor - the oldest offset still
// readable (invariant 1) - is above offset, which is what retention removing
// a stored position's record looks like from outside.
//
// It stands where a sleep through "the size sweep runs on its own second"
// stood: how long a sweep takes to come round is a fact about the machine,
// and what the test needs is the floor having moved, not the second.
func AwaitFloorPast(t testing.TB, h *Harness, channel string, offset uint64, within time.Duration) {
	t.Helper()
	series := fmt.Sprintf("saguin_channel_floor_offset{channel=%q}", channel)
	if last, ok := awaitSeries(t, h, series, within, func(v uint64) bool { return v > offset }); !ok {
		t.Fatalf("the floor of %q never passed %d within %s; last read %q",
			channel, offset, within, last)
	}
}

// awaitSeries scrapes the broker's metrics until one series' value
// satisfies ok, and answers the last line read and whether it did.
func awaitSeries(t testing.TB, h *Harness, series string, within time.Duration, ok func(uint64) bool) (string, bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	stop, err := h.B.ServeOperations(TCPOnly(addr), 0, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	last := "(no series)"
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			t.Fatalf("scrape: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read the scrape: %v", err)
		}
		for _, l := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(l, series+" ") {
				last = l
				v, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(l, series+" ")), 10, 64)
				if err == nil && ok(v) {
					return last, true
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last, false
}

// ReadRawPacket reads one MQTT packet, returning its first byte and body.
func ReadRawPacket(conn net.Conn) (byte, []byte, error) {
	var first [1]byte
	if _, err := io.ReadFull(conn, first[:]); err != nil {
		return 0, nil, err
	}
	mult, length := 1, 0
	for {
		var b [1]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return 0, nil, err
		}
		length += int(b[0]&127) * mult
		mult *= 128
		if b[0]&0x80 == 0 {
			break
		}
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil, err
	}
	return first[0], body, nil
}

// SubscriptionIdentifiersIn walks a PUBLISH's properties and returns every
// Subscription Identifier on it.
//
// It walks rather than searches: the property is a variable byte integer
// and 0x0B is a byte that can occur inside somebody's Content Type or User
// Property just as easily, so scanning for it would find identifiers nobody
// sent.
func SubscriptionIdentifiersIn(t testing.TB, body []byte, hasPacketID bool) []int {
	t.Helper()
	str := func(p []byte) ([]byte, int) {
		if len(p) < 2 {
			t.Fatal("truncated string in properties")
		}
		n := int(p[0])<<8 | int(p[1])
		if len(p) < 2+n {
			t.Fatal("truncated string in properties")
		}
		return p[2+n:], n
	}
	vbi := func(p []byte) ([]byte, int) {
		mult, v := 1, 0
		for i := 0; i < len(p); i++ {
			v += int(p[i]&127) * mult
			mult *= 128
			if p[i]&0x80 == 0 {
				return p[i+1:], v
			}
		}
		t.Fatal("truncated variable byte integer in properties")
		return nil, 0
	}

	p, _ := str(body) // topic name
	if hasPacketID {
		if len(p) < 2 {
			t.Fatal("truncated packet identifier")
		}
		p = p[2:]
	}
	p, plen := vbi(p)
	if len(p) < plen {
		t.Fatalf("property length %d but only %d bytes left", plen, len(p))
	}
	props := p[:plen]

	var ids []int
	for len(props) > 0 {
		id := props[0]
		props = props[1:]
		switch id {
		case 0x01: // Payload Format Indicator
			props = props[1:]
		case 0x02: // Message Expiry Interval
			props = props[4:]
		case 0x23: // Topic Alias
			props = props[2:]
		case 0x03, 0x08: // Content Type, Response Topic
			props, _ = str(props)
		case 0x09: // Correlation Data
			props, _ = str(props)
		case 0x26: // User Property, a pair
			props, _ = str(props)
			props, _ = str(props)
		case 0x0B: // Subscription Identifier
			var v int
			props, v = vbi(props)
			ids = append(ids, v)
		default:
			t.Fatalf("unhandled property 0x%02X in a delivery", id)
		}
	}
	return ids
}

// Legacy connects an Eclipse Paho 3.1.1 client and reports when the broker
// closes it.
//
// A helper because three tests need one, and because the options matter:
// auto-reconnect off, so a close stays closed and the wait below cannot be
// satisfied by a reconnection.
func Legacy(t testing.TB, h *Harness, id string, clean bool) (mqttv3.Client, <-chan struct{}) {
	t.Helper()
	return LegacyWithDefault(t, h, id, clean, nil)
}

// LegacyWithDefault is the same with a handler attached *before* connecting,
// for a test where a message can arrive before any SUBSCRIBE.
//
// **A resumed session is redelivered its backlog immediately after CONNACK**,
// which can beat a SUBSCRIBE sent after it - and Paho v3 acknowledges a QoS 1
// publish it has no route for and drops it, so the message is consumed and
// the test sees nothing. Attaching the route at subscribe time is a handler
// attached last without proof it is the one being used; attaching it here is
// the same rule kept.
func LegacyWithDefault(t testing.TB, h *Harness, id string, clean bool,
	def mqttv3.MessageHandler) (mqttv3.Client, <-chan struct{}) {
	t.Helper()
	lost := make(chan struct{}, 1)
	opts := mqttv3.NewClientOptions().
		AddBroker("tcp://" + h.Addr).
		SetClientID(id).
		SetProtocolVersion(4). // 3.1.1, and nothing else
		SetCleanSession(clean).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(mqttv3.Client, error) {
			select {
			case lost <- struct{}{}:
			default:
			}
		})
	if def != nil {
		opts.SetDefaultPublishHandler(def)
	}
	c := mqttv3.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("a 3.1.1 client could not connect: %v", tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c, lost
}

// SubscribeCode is the SUBACK code the broker actually sent for one filter,
// rather than what the client library made of it.
//
// **Paho v3 does not report a refusal as an error.** `Token.Error()` is nil
// for a SUBACK carrying `0x80`, so a test asserting on it calls every
// refusal a grant - which is exactly what the first version of the two
// tests below did, against a broker whose own log said "refused" three
// times in the same run. The granted code is in `Result()` and is what the
// broker said.
func SubscribeCode(t testing.TB, c mqttv3.Client, filter string,
	h mqttv3.MessageHandler) byte {
	t.Helper()
	tok := c.Subscribe(filter, 1, h)
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("%s: the SUBSCRIBE never completed", filter)
	}
	st, ok := tok.(*mqttv3.SubscribeToken)
	if !ok {
		t.Fatalf("%s: token is a %T, not a SubscribeToken", filter, tok)
	}
	// **Keyed by the inner filter for a shared subscription.** Paho v3
	// strips `$share/<group>/` before recording the result, so asking for
	// the filter as sent finds nothing and reports a broker that answered
	// no code - while the map beside it held `jobs/#: 128`. One filter went
	// out, so one entry comes back and it is the answer whatever it is
	// keyed by.
	res := st.Result()
	if len(res) != 1 {
		t.Fatalf("%s: one filter was sent and the SUBACK carries %d codes: %v",
			filter, len(res), res)
	}
	for _, code := range res {
		return code
	}
	panic("unreachable")
}

func Uint32Ptr(v uint32) *uint32 { return &v }

// ForceCloseLegacy drops a Paho v3 client's socket without a DISCONNECT, so
// the broker treats it as a lost connection and the Will fires. Paho v3
// offers no such call, so the connection is broken underneath it.
func ForceCloseLegacy(c mqttv3.Client) error {
	// **A clean DISCONNECT discards the Will**, by MQTT's own rule, so it
	// would test nothing. Paho v3 offers no way to drop a socket, so the
	// broker is made to drop it instead: a QoS 2 publish from a client
	// whose roles deny `qos2`, which saguin refuses by closing the
	// connection (RFC 0002, reason code 0x9B) and which a 3.1.1 client is
	// given no DISCONNECT to soften. From the Will's point of view that is
	// a lost connection, which is exactly the case under test.
	//
	// **This only works for a client denied `qos2`.** For a client allowed
	// it, the publish is accepted, the client stays up, and the error below is
	// what a caller gets - which is why it says what it says rather than
	// timing out somewhere further on.
	c.Publish("bcast/anything", 2, false, []byte("x")).WaitTimeout(3 * time.Second)
	if c.IsConnected() {
		return fmt.Errorf("the client is still connected, so the Will has no reason to fire")
	}
	return nil
}

// RefusedRouteOn serves the operations listener on a free port and answers
// with its address.
func RefusedRouteOn(t *testing.T, h *Harness) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(TCPOnly(addr), 0, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	t.Cleanup(func() { stop() })
	return addr
}

// RefusedRows reads /v1/operations/refused and answers with the clients on it.
func RefusedRows(t *testing.T, opsAddr string) []struct {
	ClientID string `json:"client_id"`
	Reason   string `json:"reason"`
} {
	t.Helper()
	resp, err := http.Get("http://" + opsAddr + "/v1/operations/refused")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Clients []struct {
			ClientID string `json:"client_id"`
			Reason   string `json:"reason"`
		} `json:"clients"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out.Clients
}

// refusalVBI appends n as a Variable Byte Integer.
func refusalVBI(b []byte, n int) []byte {
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		b = append(b, d)
		if n == 0 {
			return b
		}
	}
}

// RefusalConnect is a CONNECT at protocol level v (4 or 5) for id, with a
// user name and password when user is not empty, and props as the MQTT 5
// property block's contents. Written byte by byte so that what reaches the
// broker is exactly this, whatever a client library would refuse to send.
func RefusalConnect(v byte, id, user, password string, props []byte) []byte {
	str := func(s string) []byte { return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...) }
	flags := byte(0x02)
	if user != "" {
		flags |= 0xC0
	}
	body := append(str("MQTT"), v, flags, 0x00, 0x3c)
	if v == 5 {
		body = append(refusalVBI(body, len(props)), props...)
	}
	body = append(body, str(id)...)
	if user != "" {
		body = append(append(body, str(user)...), str(password)...)
	}
	return append(refusalVBI([]byte{0x10}, len(body)), body...)
}

// RefusalDial sends a CONNECT and returns the connection and the reason code
// in the CONNACK, having read the whole CONNACK and nothing after it.
func RefusalDial(t *testing.T, h *Harness, connect []byte) (net.Conn, byte) {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(connect); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	r := bufio.NewReader(conn)
	if b, err := r.ReadByte(); err != nil || b != 0x20 {
		t.Fatalf("no CONNACK: %#x (%v)", b, err)
	}
	remaining, mult := 0, 1
	for {
		b, err := r.ReadByte()
		if err != nil {
			t.Fatalf("reading the CONNACK length: %v", err)
		}
		remaining += int(b&0x7f) * mult
		if b&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil || len(body) < 2 {
		t.Fatalf("reading the CONNACK: %v", err)
	}
	if r.Buffered() > 0 {
		t.Fatalf("%d bytes arrived behind the CONNACK", r.Buffered())
	}
	return conn, body[1]
}

// RefusalRecord reads /v1/operations/refused, keyed by client id.
func RefusalRecord(t *testing.T, opsAddr string) map[string]struct {
	User   string
	Reason string
	Count  uint64
} {
	t.Helper()
	resp, err := http.Get("http://" + opsAddr + "/v1/operations/refused")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/v1/operations/refused answered %d", resp.StatusCode)
	}
	var out struct {
		Clients []struct {
			ClientID string `json:"client_id"`
			User     string `json:"user"`
			Reason   string `json:"reason"`
			Count    uint64 `json:"count"`
		} `json:"clients"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	rows := map[string]struct {
		User   string
		Reason string
		Count  uint64
	}{}
	for _, c := range out.Clients {
		rows[c.ClientID] = struct {
			User   string
			Reason string
			Count  uint64
		}{c.User, c.Reason, c.Count}
	}
	return rows
}

// WriteACL puts an acl_file in front of a running harness, which is what
// every test about a rule needs and what three of them had written out.
func WriteACL(t testing.TB, h *Harness, body string) {
	t.Helper()
	authorizeFrom(t, h.B, body)
}

func authorizeFrom(t testing.TB, b *broker.Broker, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write the acl file: %v", err)
	}
	f, err := authz.Load(path, b.Registry(), Limits.MaxTopicLevels)
	if err != nil {
		t.Fatalf("the test's own acl file does not load: %v", err)
	}
	b.Authorize(authz.New(f, b.Registry()))
}

// AskDisconnect publishes the verb and returns the PUBACK code and the reply
// the broker wrote to the Response Topic, or "" when it wrote none.
//
// **No subscription, and that is a property rather than a shortcut.** The
// reply is written to the asking client's own socket, as a seek's is, so it
// arrives without a grant on the reply topic - an operator whose role holds
// `broker: sessions` and nothing else can use the verb and read its answer,
// which is the smallest grant that works.
func AskDisconnect(t testing.TB, c *Client, id string) (byte, string) {
	t.Helper()
	reply := "ops/" + id + "/reply"
	ack, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: "$saguin/sessions/disconnect", QoS: 1, Payload: []byte(id),
		Properties: &paho.PublishProperties{ResponseTopic: reply},
	})
	if ack == nil {
		t.Fatalf("no acknowledgement for a disconnect naming %q: %v", id, err)
	}
	if ack.ReasonCode != 0x00 {
		return ack.ReasonCode, ""
	}
	r, ok := c.Await(t, 3*time.Second)
	if !ok {
		return ack.ReasonCode, ""
	}
	return ack.ReasonCode, r.Payload
}

// Catalogue asks what one channel is and returns the reply.
//
// QoS 1 because that is the only QoS this question has: a refusal rides the
// PUBACK, and QoS 0 has none. Awaiting the reply is safe for the reason
// KvGet's is - the receive handler collects every packet into the client's
// own channel, so nothing read here is a packet swallowed from elsewhere.
func (c *Client) Catalogue(t testing.TB, name string, corr []byte) Received {
	t.Helper()
	props := &paho.PublishProperties{ResponseTopic: CatalogueReply}
	if corr != nil {
		props.CorrelationData = corr
	}
	ack, err := c.C.Publish(context.Background(), &paho.Publish{
		Topic: channel.CatalogueTopic(name), QoS: 1, Properties: props,
	})
	if ack == nil {
		t.Fatalf("no PUBACK for a catalogue request about %q: %v", name, err)
	}
	if ack.ReasonCode != 0 {
		t.Fatalf("asking about %q was refused 0x%02X (reason string %q; this client "+
			"does not set Request Problem Information)", name, ack.ReasonCode, AckReason(ack))
	}
	r, ok := c.Await(t, 3*time.Second)
	if !ok {
		t.Fatalf("no reply to a catalogue request about %q", name)
	}
	if r.Topic != CatalogueReply {
		t.Fatalf("the reply about %q arrived on %q, want %s", name, r.Topic, CatalogueReply)
	}
	return r
}

const CatalogueReply = "catalogue-reply"

// catalogueAnswer is the reply's shape, read here rather than compared as a
// string so that a field order change is not a test failure.
type catalogueAnswer struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Filter string   `json:"filter"`
	Verbs  []string `json:"verbs"`
	Pin    string   `json:"pin"`
}

func ReadCatalogue(t testing.TB, r Received) catalogueAnswer {
	t.Helper()
	var a catalogueAnswer
	if err := json.Unmarshal([]byte(r.Payload), &a); err != nil {
		t.Fatalf("the answer %q is not JSON: %v", r.Payload, err)
	}
	return a
}

// AssertNoForgery holds both halves: nothing under the reserved prefix
// survives, and the publisher's own property does. A strip that took
// everything would pass the first and is not the fix.
func AssertNoForgery(t *testing.T, where string, user map[string]string) {
	t.Helper()
	for k, v := range user {
		if strings.HasPrefix(k, "saguin-") {
			t.Errorf("%s delivered %s=%q, which the publisher wrote: a consumer "+
				"cannot tell it from one saguin stamped", where, k, v)
		}
	}
	if user["app-key"] != "kept" {
		t.Errorf("%s dropped the publisher's own app-key (%v): the strip is "+
			"scoped to the reserved prefix, not to properties", where, user)
	}
}

// MemStorage is the one provider every configuration must define, for the
// tests whose subject is something else. It goes straight after `id:`.
const MemStorage = "  storage:\n    default: mem\n    default_retention_period: none\n" +
	"    default_retention_bytes: none\n    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"

// HarnessSessions is the session store the last harness started attached, for
// the tests that read what the broker kept.
var HarnessSessions broker.SessionStore

// WrapSessions, when set, is handed the session store a harness is about to
// attach and returns the one it attaches instead - so a test can hold a
// write open at a chosen moment and watch what else waits on it.
var WrapSessions func(broker.SessionStore) broker.SessionStore

// WrapLogs, when set, wraps each append channel's log as the next harness
// attaches its stores, before it serves, beneath the counting wrapper
// (broker.WrapLog). A test holds or fails a log call through a gate it
// installs here and arms later: a log replaced on a running broker races
// the pumps that read it without a lock.
var WrapLogs func(channel string, l broker.LogStore) broker.LogStore

// WrapProviders, when set, wraps the provider a sqlite harness attaches as
// the one that drops a reader's positions across all of its channels at once
// (broker.Stores.Providers), so a test can refuse that drop.
var WrapProviders func(provider string, d broker.ReaderDropper) broker.ReaderDropper

// WrapHolds, when set, wraps each channel's exactly-once holds, and the
// broadcast log's under store.BroadcastLog, as the next harness attaches its
// stores, so a test can hold or fail a hold, a release or a drop at a chosen
// moment (broker.WrapHolds).
var WrapHolds func(channel string, h broker.HoldStore) broker.HoldStore

// HarnessShares is the broker the last harness started, asked what a shared
// group holds (Broker.Backlog), for a test that reads a backlog back rather
// than inferring it from what a client was served.
var HarnessShares interface {
	Backlog(group string) ([]store.ShareMessage, error)
}

// dbMaxBytes is the max_bytes the next sqlite harness opens its database
// with, or zero for none. BoundSQLite is the only thing that sets it.
var dbMaxBytes int64

// BoundSQLite bounds the next sqlite harness's database at maxBytes, with
// broker.limits.max_message_size at maxMessage: a test that needs a full
// sqlite provider calls it.
//
// **The two together, and checked by the configuration's own validation.**
// A bounded provider holds back one largest message and its headers for the
// writes that relieve it (config.Limits.StorageReserve), so the bound and
// max_message_size are one decision. The pair is written as an operator's
// file would write it and loaded, so a harness runs no bound the binary
// refuses to start with.
func BoundSQLite(t testing.TB, maxBytes int64, maxMessage string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "bound.yaml")
	body := fmt.Sprintf(`broker:
  id: harness-bound
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:1
  limits:
    max_message_size: %s
    max_header_bytes: %d
  storage:
    default: disk
    default_retention_period: none
    default_retention_bytes: none
    providers:
      disk:
        type: sqlite
        file_path: %s
        max_bytes: %d
channels: {}
`, maxMessage, Limits.MaxHeaderBytes, filepath.Join(dir, "bound.db"), maxBytes)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write the bound's configuration: %v", err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the binary refuses the bound this test asks for: %v", err)
	}
	old := Limits
	Limits.MaxMessageSize = f.Broker.Limits.Resolve().MaxMessageSize
	dbMaxBytes = maxBytes
	t.Cleanup(func() { Limits, dbMaxBytes = old, 0 })
}

// WaitForBacklog reads the store until a group holds what is expected, so a
// test asserts on the store rather than on a sleep.
func WaitForBacklog(t *testing.T, group string, want int) []store.ShareMessage {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		backlog, err := HarnessShares.Backlog(group)
		if err != nil {
			t.Fatalf("reading the backlog of %q: %v", group, err)
		}
		if len(backlog) == want {
			return backlog
		}
		select {
		case <-deadline:
			return backlog
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func BacklogPayloads(ms []store.ShareMessage) string {
	out := ""
	for _, m := range ms {
		out += string(m.Record.Payload) + " "
	}
	return out
}

// TCPOnly is the operations listener a test that only wants a port asks
// for. The socket half has tests of its own.
func TCPOnly(address string) config.OperationsListen {
	return config.OperationsListen{
		TCP: []config.TCPDoor{{Name: "tcp", Address: config.Address{Address: address}}},
	}
}

// ESPDevice is what an ESP32-class client declares, and every field is a
// number the broker acts on rather than a detail of the device.
//
// The buffer bound is the one that surprises: ESP-IDF's MQTT client
// defaults to 1KiB, and a record larger than a subscriber's Maximum Packet
// Size is a disconnect with 0x95 rather than a skip (RFC 0003). The window
// is the prefetch such a device sets so a burst cannot swamp it, and it is
// the path that starved before mochi-mqtt/server#519. The expiry is what
// makes it a durable consumer at all, and what a device that sleeps
// depends on.
type ESPDevice struct {
	Window    uint16 // Receive Maximum: a small prefetch
	MaxPacket uint32 // Maximum Packet Size: a small buffer
	Expiry    uint32 // Session Expiry Interval: it sleeps and comes back
}

var ESP32 = ESPDevice{Window: 4, MaxPacket: 1024, Expiry: 300}

// SubSliced subscribes declaring a slice of the filter, as MQTT 5 User
// Properties on the SUBSCRIBE packet.
//
// **Built here rather than by calling into the broker's own reader**, so
// that these tests drive the wire format a client actually sends. A helper
// that shared code with partitioning() would agree with it by construction
// and prove nothing about the packet.
func (c *Client) SubSliced(t testing.TB, filter string, count int, indices ...int) *paho.Suback {
	t.Helper()
	props := &paho.SubscribeProperties{}
	for _, i := range indices {
		props.User = append(props.User, paho.UserProperty{
			Key:   "saguin-filter",
			Value: fmt.Sprintf("topic_hash(%d, %d)", count, i)})
	}
	sa, err := c.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
		Properties:    props,
	})
	if sa != nil {
		return sa
	}
	if err != nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	return sa
}

// SubDeclaring subscribes with raw user properties, and reports what came
// back rather than failing, because the refusals under test are the answer.
func (c *Client) SubDeclaring(t testing.TB, filters []string, ups [][2]string) (*paho.Suback, error) {
	t.Helper()
	props := &paho.SubscribeProperties{}
	for _, kv := range ups {
		props.User = append(props.User, paho.UserProperty{Key: kv[0], Value: kv[1]})
	}
	subs := make([]paho.SubscribeOptions, len(filters))
	for i, f := range filters {
		subs[i] = paho.SubscribeOptions{Topic: f, QoS: 1}
	}
	return c.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subs,
		Properties:    props,
	})
}

// QoS2Wire is a Client that sends exactly what it is told to and nothing
// else. It completes no handshake on its own.
type QoS2Wire struct {
	T *testing.T
	C net.Conn
}

func QoS2Dial(t *testing.T, addr, id string, clean bool, expiry uint32) (*QoS2Wire, *pahopackets.Connack) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	w := &QoS2Wire{T: t, C: c}
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	conn := cp.Content.(*pahopackets.Connect)
	conn.ClientID, conn.CleanStart, conn.KeepAlive = id, clean, 0
	conn.Properties.SessionExpiryInterval = &expiry
	if _, err := cp.WriteTo(c); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	ack := w.Next(5 * time.Second)
	ca, ok := ack.Content.(*pahopackets.Connack)
	if !ok {
		t.Fatalf("expected CONNACK, got %s", ack.PacketType())
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("CONNACK refused 0x%02X", ca.ReasonCode)
	}
	return w, ca
}

// InChannel reads the events channel whole with a fresh clean session and
// counts how many records carry this payload.
//
// **A fresh reader rather than the publisher's own tally**, because the
// question is what the channel kept: a subscriber counts what reached it,
// which can be short by whatever was still in flight, and this has to be
// able to see a second copy that nobody was delivered.
//
// **It reads to an end marker, not to a quiet second and a half.** A
// record published after the reader subscribes is stored after everything
// the channel held, and a subscriber is served in offset order (RFC 0003
// "Ordering"), so the marker arriving says every record before it has been
// read. A quiet period said only that nothing had arrived lately, and cost
// a second and a half a call.
func InChannel(t *testing.T, h *Harness, who, payload string) int {
	t.Helper()
	c := Connect(t, h, who, true, false)
	c.Sub(t, "events/#", 1)
	end := "end-of-" + who
	Connect(t, h, who+"-marker", true, false).Pub(t, "events/end-marker", end)
	n := 0
	for {
		r, ok := c.Await(t, 5*time.Second)
		if !ok {
			t.Fatalf("%s read %d copies of %q and never reached its end marker, so the "+
				"count is not of the whole channel", who, n, payload)
		}
		if r.Payload == end {
			return n
		}
		if r.Payload == payload {
			n++
		}
	}
}

// EachProvider runs one case against a broker whose channels, and the
// exactly-once publishes held in them, are in memory and against one whose
// channels are on a SQLite provider.
//
// **Both, for every rule, because the two are different code.** The memory
// store keeps a map and the SQLite one keeps a table, and an operator moves
// between them by editing a channel's `storage` - so a rule that held on one
// and not the other would be a broker that changes behaviour when somebody
// changes where it keeps things. The store package tests the two stores
// against one set of oracles; this drives them through the whole broker.
func EachProvider(t *testing.T, run func(t *testing.T, h *Harness)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { run(t, Start(t)) })
	t.Run("sqlite", func(t *testing.T) {
		run(t, StartDurableSQLite(t, filepath.Join(t.TempDir(), "qos2.db")))
	})
}

// Next reads one packet, failing the test if none arrives.
func (w *QoS2Wire) Next(d time.Duration) *pahopackets.ControlPacket {
	w.T.Helper()
	cp, err := w.Read(d)
	if err != nil {
		w.T.Fatalf("nothing came back within %s: %v", d, err)
	}
	return cp
}

func (w *QoS2Wire) Read(d time.Duration) (*pahopackets.ControlPacket, error) {
	if err := w.C.SetReadDeadline(time.Now().Add(d)); err != nil {
		return nil, err
	}
	return pahopackets.ReadPacket(w.C)
}

// Publish sends an exactly-once PUBLISH. dup marks it as a re-send, which
// is what a Client does when it never saw the receipt.
func (w *QoS2Wire) Publish(id uint16, topic, payload string, dup bool) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBLISH)
	p := cp.Content.(*pahopackets.Publish)
	p.Topic, p.Payload, p.PacketID, p.QoS, p.Duplicate = topic, []byte(payload), id, 2, dup
	cp.FixedHeader.Flags |= 2 << 1
	if dup {
		cp.FixedHeader.Flags |= 0x08
	}
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write PUBLISH: %v", err)
	}
}

func (w *QoS2Wire) Pubrel(id uint16) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBREL)
	cp.Content.(*pahopackets.Pubrel).PacketID = id
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write PUBREL: %v", err)
	}
}

func (w *QoS2Wire) Close() { _ = w.C.Close() }

// Settle is Settle for a raw wire: a PINGREQ round trip. The broker reads
// and handles a connection's packets in order, so its PINGRESP proves every
// PUBACK written before it was processed - which closing the socket and
// stopping the broker does not: the stop can close the connection with the
// PUBACK unread, and the restart rightly sends the record again. Anything
// else that arrives first is discarded.
func (w *QoS2Wire) Settle() {
	w.T.Helper()
	if _, err := pahopackets.NewControlPacket(pahopackets.PINGREQ).WriteTo(w.C); err != nil {
		w.T.Fatalf("write PINGREQ: %v", err)
	}
	for {
		if _, ok := w.Next(3 * time.Second).Content.(*pahopackets.Pingresp); ok {
			return
		}
	}
}

// Pubrec reads the next packet and insists it is a PUBREC, returning its
// reason code. A test that wanted a receipt and got something else says so
// rather than reporting a wrong code.
func (w *QoS2Wire) Pubrec(id uint16) byte {
	w.T.Helper()
	cp := w.Next(3 * time.Second)
	rec, ok := cp.Content.(*pahopackets.Pubrec)
	if !ok {
		w.T.Fatalf("expected a PUBREC for id %d, got %s", id, cp.PacketType())
	}
	if rec.PacketID != id {
		w.T.Fatalf("PUBREC names id %d, want %d", rec.PacketID, id)
	}
	return rec.ReasonCode
}

func (w *QoS2Wire) Pubcomp(id uint16) byte {
	w.T.Helper()
	cp := w.Next(3 * time.Second)
	comp, ok := cp.Content.(*pahopackets.Pubcomp)
	if !ok {
		w.T.Fatalf("expected a PUBCOMP for id %d, got %s", id, cp.PacketType())
	}
	if comp.PacketID != id {
		w.T.Fatalf("PUBCOMP names id %d, want %d", comp.PacketID, id)
	}
	return comp.ReasonCode
}

// Exchange runs a whole exactly-once publish and insists every step
// succeeded, for the tests whose subject is what happens afterwards.
func (w *QoS2Wire) Exchange(id uint16, topic, payload string) {
	w.T.Helper()
	w.Publish(id, topic, payload, false)
	if rc := w.Pubrec(id); rc != 0 {
		w.T.Fatalf("PUBREC for %q carried 0x%02X, want success", payload, rc)
	}
	w.Pubrel(id)
	if rc := w.Pubcomp(id); rc != 0 {
		w.T.Fatalf("PUBCOMP for %q carried 0x%02X, want success", payload, rc)
	}
}

// SendPubrec answers a QoS 2 delivery, which is the subscriber's half of the
// exchange: the tests above drive the publisher's.
func (w *QoS2Wire) SendPubrec(id uint16) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBREC)
	cp.Content.(*pahopackets.Pubrec).PacketID = id
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write PUBREC: %v", err)
	}
}

// SendPubcomp finishes it.
func (w *QoS2Wire) SendPubcomp(id uint16) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBCOMP)
	cp.Content.(*pahopackets.Pubcomp).PacketID = id
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write PUBCOMP: %v", err)
	}
}

// ScrapeGauges reads the whole catalogue once and returns it by series, so
// that one phase is one scrape.
//
// **One scrape per phase rather than one per gauge**, because every scrape
// inside min_scrape_interval is answered from the previous catalogue - nine
// scrapes in a row would read one moment nine times and call it nine
// measurements.
func ScrapeGauges(t testing.TB, addr string) map[string]float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics answered %d, want 200", resp.StatusCode)
	}
	out := map[string]float64{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
		if err != nil {
			continue
		}
		out[line[:i]] = v
	}
	if len(out) < 10 {
		t.Fatalf("the catalogue carried %d series, which cannot be all of them - "+
			"this read something it did not understand and would pass by vacuum",
			len(out))
	}
	return out
}

// OperationsAt stands up the operations listener this test scrapes, with no
// minimum scrape interval. The listener answers a scrape inside that interval
// with the catalogue before (RFC 0005), which is right for Prometheus and
// wrong for a test: a millisecond here let a publish answered on loopback in
// a quarter of one read the count from before it.
func OperationsAt(t *testing.T, h *Harness) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(TCPOnly(addr), 0, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	t.Cleanup(stop)
	return addr
}

// AwaitLeased waits until the operations route reports job's record leased at
// the attempt job carries: the broker has read the worker's PUBACK and counted
// the attempt (RFC 0003 "Attempts, timeout, and redelivery").
//
// **Await returns before that.** Paho writes its automatic PUBACK only after
// the receive handlers return, and Await returns from inside the handler, so
// a test that cuts the link or stops the broker straight after Await races
// the PUBACK - and a sleep standing in for it is a guess that gets shorter as
// the machine gets busier.
func AwaitLeased(t *testing.T, opsAddr, channel string, job Received) {
	t.Helper()
	offset, attempt := job.User["saguin-offset"], job.User["saguin-attempt"]
	if offset == "" || attempt == "" {
		t.Fatalf("the job carries offset %q and attempt %q, so there is nothing to wait for",
			offset, attempt)
	}
	url := "http://" + opsAddr + "/v1/operations/queues/" + channel
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s answered %d: %v %s", url, resp.StatusCode, err, body)
		}
		var out struct {
			Channel string `json:"channel"`
			Records []struct {
				Offset   uint64 `json:"offset"`
				Attempts int    `json:"attempts"`
				State    string `json:"state"`
			} `json:"records"`
		}
		if err := json.Unmarshal(body, &out); err != nil || out.Channel != channel {
			t.Fatalf("GET %s did not answer for %q: %v %s", url, channel, err, body)
		}
		for _, r := range out.Records {
			if fmt.Sprint(r.Offset) == offset && fmt.Sprint(r.Attempts) == attempt && r.State == "leased" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker did not report %s offset %s leased at attempt %s within 5s; "+
				"the route last said %s", channel, offset, attempt, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WindowDial connects a wire Client declaring a Receive Maximum and a session
// that outlives the connection. It completes nothing on its own, so what the
// broker sends is what arrives.
func WindowDial(t *testing.T, addr, id string, clean bool, rxMax uint16) (*QoS2Wire, *pahopackets.Connack) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	w := &QoS2Wire{T: t, C: c}
	cp := pahopackets.NewControlPacket(pahopackets.CONNECT)
	conn := cp.Content.(*pahopackets.Connect)
	conn.ClientID, conn.CleanStart, conn.KeepAlive = id, clean, 0
	expiry := uint32(300)
	conn.Properties.SessionExpiryInterval = &expiry
	conn.Properties.ReceiveMaximum = &rxMax
	if _, err := cp.WriteTo(c); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	ca, ok := w.Next(5 * time.Second).Content.(*pahopackets.Connack)
	if !ok || ca.ReasonCode != 0 {
		t.Fatalf("the CONNECT was not accepted: %+v", ca)
	}
	return w, ca
}

// DenyFeatures installs an acl_file whose every Client - `*` matches the
// Harness's clients, which carry no user name - holds the one role written.
// It grants broadcast and two channels, so what a test sees refused is the
// denial and never a missing grant.
func DenyFeatures(t *testing.T, h *Harness, rules string) {
	t.Helper()
	acl := filepath.Join(t.TempDir(), "acl.yaml")
	body := "roles:\n  everyone:\n    - topic: \"#\"\n      allow: [write, read]\n" +
		"    - channel: events\n      allow: [write, read]\n" +
		"    - channel: state\n      allow: [write, read]\n" + rules +
		"users:\n  \"*\": [everyone]\n"
	if err := os.WriteFile(acl, []byte(body), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	f, err := authz.Load(acl, h.B.Registry(), Limits.MaxTopicLevels)
	if err != nil {
		t.Fatalf("load acl: %v", err)
	}
	h.B.Authorize(authz.New(f, h.B.Registry()))
}

// Named insists a packet is a PUBLISH of this payload carrying its topic
// name and no topic alias, and returns its packet identifier.
func Named(t *testing.T, cp *pahopackets.ControlPacket, topic, payload string) uint16 {
	t.Helper()
	p, ok := cp.Content.(*pahopackets.Publish)
	if !ok {
		t.Fatalf("expected a PUBLISH of %q, got %s", payload, cp.PacketType())
	}
	if string(p.Payload) != payload {
		t.Fatalf("got PUBLISH %q, want %q", p.Payload, payload)
	}
	if p.Topic != topic || (p.Properties != nil && p.Properties.TopicAlias != nil) {
		alias := "none"
		if p.Properties != nil && p.Properties.TopicAlias != nil {
			alias = fmt.Sprint(*p.Properties.TopicAlias)
		}
		t.Errorf("PUBLISH %q was sent topic=%q alias=%s, want topic %q and no alias",
			payload, p.Topic, alias, topic)
	}
	return p.PacketID
}

// EachSessionProvider runs one case on a memory provider and on a sqlite one,
// because the session store is whichever the operator names and the broker's
// behaviour must not depend on which (RFC 0002 `broker.session`).
func EachSessionProvider(t *testing.T, run func(t *testing.T, h *Harness)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { run(t, StartDurable(t, t.TempDir())) })
	t.Run("sqlite", func(t *testing.T) { run(t, StartDurableSQLite(t, filepath.Join(t.TempDir(), "s.db"))) })
}

// Delivered is one PUBLISH as a wire Client read it: its packet identifier,
// and the number its payload starts with, or zero when it has none.
type Delivered struct {
	ID uint16
	N  int
}

// DeafReader reads everything the broker writes to w and acknowledges
// nothing, handing each delivery to got. It sends nothing itself.
func DeafReader(w *QoS2Wire, got chan<- Delivered) {
	go func() {
		defer close(got)
		for {
			cp, err := w.Read(10 * time.Second)
			if err != nil {
				return
			}
			if p, ok := cp.Content.(*pahopackets.Publish); ok {
				number, _, _ := strings.Cut(string(p.Payload), " ")
				n, _ := strconv.Atoi(number)
				got <- Delivered{ID: p.PacketID, N: n}
			}
		}
	}()
}

// Numbered is a payload of size bytes that starts with n.
func Numbered(n, size int) []byte {
	return []byte(strconv.Itoa(n) + " " + strings.Repeat("x", size))
}

func (w *QoS2Wire) Subscribe(filter string, qos byte) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.SUBSCRIBE)
	s := cp.Content.(*pahopackets.Subscribe)
	s.PacketID = 1
	s.Subscriptions = []pahopackets.SubOptions{{Topic: filter, QoS: qos}}
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write SUBSCRIBE: %v", err)
	}
	sa, ok := w.Next(3 * time.Second).Content.(*pahopackets.Suback)
	if !ok || len(sa.Reasons) != 1 || sa.Reasons[0] > 2 {
		w.T.Fatalf("the subscription was not granted: %+v", sa)
	}
}

func (w *QoS2Wire) Puback(id uint16) {
	w.T.Helper()
	cp := pahopackets.NewControlPacket(pahopackets.PUBACK)
	cp.Content.(*pahopackets.Puback).PacketID = id
	if _, err := cp.WriteTo(w.C); err != nil {
		w.T.Fatalf("write PUBACK: %v", err)
	}
}
