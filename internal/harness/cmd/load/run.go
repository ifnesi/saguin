package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// connecting is how many clients connect at once while a run sets up:
// enough that durable subscribers, each CONNECT and SUBSCRIBE waiting for
// its commit, do not set up one at a time.
const connecting = 64

// A run is one measurement: what its clients share, read-only once they
// start.
type run struct {
	// topics holds each publisher's topic, and qos its QoS, by publisher
	// index.
	topics []string
	qos    []byte
	// epoch is when publishing starts; every time in the run is measured
	// from it, on the monotonic clock.
	epoch time.Time
	// period is the fixed mode's time between one publisher's
	// publications; zero in the maximum mode.
	period time.Duration
	// observation is the fixed latest receipt time, since epoch. Cleanup
	// cannot extend it. Diagnostics set their own observation length.
	observation atomic.Int64 // nanoseconds since epoch
	// reconnecting selects the reconnect diagnostic; subscribers supplies
	// its initial, evenly staggered reconnect phases.
	reconnecting bool
	subscribers  int
	// Each client owns its timestamped counts; the window reads atomics
	// while they run and finalizes them after the client loops join. In
	// the fixed mode, skipped holds each publisher's slots never sent,
	// missed or unsent, by their due times.
	writes, receipts, skipped []secondCounts
	windowLength              time.Duration
}

// newRun lays out a run of sz: the topics, and which publishers publish at
// QoS 1 and which at QoS 0, as the recipe's seed decides. The recipe's
// half at QoS 1 are those at the odd places of the seed's permutation;
// another qos1Share takes its share of the places, the odd first, then
// the even, so every share draws from the same permutation and a larger
// one keeps every QoS 1 publisher of a smaller. rate is the fixed mode's
// publish rate, per second over all publishers; zero for the maximum mode.
func newRun(sz size, rate float64) *run {
	r := &run{topics: make([]string, sz.publishers), qos: make([]byte, sz.publishers), subscribers: sz.subscribers, windowLength: measuredWindow}
	if rate > 0 || windowRequested || measuredWindow != measured || monitorDisk {
		r.writes = newSecondCounts(sz.publishers, measuredWindow)
		r.receipts = newSecondCounts(sz.subscribers, measuredWindow)
	}
	if rate > 0 {
		r.skipped = newSecondCounts(sz.publishers, measuredWindow)
	}
	r.observation.Store(int64(warmUp + measuredWindow + observeLimit))
	for i := range sz.publishers {
		r.topics[i] = "load/" + strconv.Itoa(i/groupSize) + "/" + strconv.Itoa(i)
	}
	n := int(math.Round(qos1Share * float64(sz.publishers)))
	odd := sz.publishers / 2
	for k, i := range rand.New(rand.NewPCG(seed, 0)).Perm(sz.publishers) {
		if (k%2 == 1 && k/2 < n) || (k%2 == 0 && k/2 < n-odd) {
			r.qos[i] = 1
		}
	}
	if rate > 0 {
		r.period = time.Duration(float64(sz.publishers) / rate * float64(time.Second))
	}
	return r
}

// inWindow reports whether t, a time since the epoch, is in the measured
// window.
func (r *run) inWindow(t time.Duration) bool {
	return t >= warmUp && t < warmUp+r.windowLength
}

// afterCutoff compares receipt time with the monotonic observation boundary,
// independently of socket cleanup or the state of any publisher's write.
func (r *run) afterCutoff(at time.Duration) bool {
	return at >= time.Duration(r.observation.Load())
}

// endObservation fixes an earlier completion or failure boundary without
// ever extending the scheduled cutoff.
func (r *run) endObservation() {
	r.observation.Store(min(r.observation.Load(), int64(time.Since(r.epoch))))
}

// A target is what the clients drive: the broker, started fresh, or the
// in-process test peer.
type target struct {
	addr   string
	binary string
	broker *harness.Broker
	peer   *peer
	other  *outsideBroker
	dir    string
	// process is Sagüin itself, identified by the pid_file it writes before
	// listening, even when the launched process is strace.
	process  *os.Process
	stopOnce sync.Once
	stopErr  error
}

// Diagnostic overrides are outside the recipe. Zero and empty values
// leave the recipe's subscribers and broker configuration unchanged.
var (
	measuredWindow    = measured
	windowRequested   bool
	commitInterval    time.Duration
	commitMax         int
	ackDelay          time.Duration
	syncDelay         time.Duration
	receiveMaximum    int
	logLevel          string
	readConnections   int
	storageMode       string
	memoryDir         string
	reconnectInterval time.Duration
)

// brokerOptions is the configuration every Sagüin load run launches.
func brokerOptions(dir string) *harness.Options {
	mode := storageMode
	if mode == "full" {
		mode = "" // leave the harness's default in place
	}
	return &harness.Options{Dir: dir, Under: brokerScope(), PIDFile: filepath.Join(dir, "saguin.pid"),
		PublishCommitInterval: commitInterval, PublishCommitMaxRecords: commitMax,
		LogLevel: logLevel, ReadConnections: readConnections, StorageMode: mode, MemoryDir: memoryDir}
}

// brokerScope is the command the broker runs under: its own systemd scope,
// with its own memory bound, swap off, apart from the load tool's.
func brokerScope() []string {
	return []string{"systemd-run", "--user", "--scope", "--quiet",
		"-p", "MemoryMax=" + brokerMemory, "-p", "MemorySwapMax=0", "--"}
}

// startTarget starts the broker binary in the chosen storage mode (full by
// default) on a new database under make's TMPDIR, on the harness's
// configuration, in its own scope; or the peer.
func startTarget(binary, kind string) (*target, error) {
	switch kind {
	case "mosquitto":
		b, err := startMosquitto()
		if err != nil {
			return nil, err
		}
		return &target{addr: b.addr, other: b, dir: b.dir}, nil
	case "peer":
		p, err := startPeer()
		if err != nil {
			return nil, err
		}
		return &target{addr: p.addr(), peer: p}, nil
	}
	dir, err := os.MkdirTemp("", "saguin-load-")
	if err != nil {
		return nil, err
	}
	opts := brokerOptions(dir)
	if syncDelay != 0 {
		under, err := testkit.SlowIOCommand("sync", syncDelay, filepath.Join(dir, "saguin.db-wal"), filepath.Join(dir, "strace.out"))
		if err != nil {
			return nil, fmt.Errorf("delaying database syncs: %w", err)
		}
		opts.Under = append(opts.Under, under...)
	}
	b, err := harness.Launch(binary, opts)
	if err != nil {
		return nil, fmt.Errorf("starting %s: %w (its files are kept in %s)", binary, err, dir)
	}
	process, err := brokerProcess(opts.PIDFile)
	if err != nil {
		_ = b.Signal(os.Kill)
		_, _ = b.Wait(time.Second)
		b.Release()
		return nil, fmt.Errorf("finding the broker process: %w (its files are kept in %s)", err, dir)
	}
	return &target{addr: b.Addr, binary: binary, broker: b, dir: dir, process: process}, nil
}

// brokerProcess reads the broker's own process id from a fresh run's file.
// Launch has already observed its listener, so startup has written the file.
func brokerProcess(path string) (*os.Process, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return nil, fmt.Errorf("%s: invalid process id %q", path, data)
	}
	return os.FindProcess(pid)
}

// pid is the broker's process id, or zero for the peer.
func (t *target) pid() int {
	switch {
	case t.process != nil:
		return t.process.Pid
	case t.broker != nil:
		return t.broker.PID()
	case t.other != nil:
		return t.other.pid()
	}
	return 0
}

// stop stops the broker, which must exit 0 within its bound, or the peer.
func (t *target) stop() error {
	t.stopOnce.Do(func() { t.stopErr = t.shutdown() })
	return t.stopErr
}

// shutdown signals Sagüin itself: strace with an output file does not
// forward SIGTERM. Waiting still belongs to the harness's launched command.
func (t *target) shutdown() error {
	switch {
	case t.peer != nil:
		return t.peer.stop()
	case t.other != nil:
		return t.other.stop()
	case t.process == nil:
		return t.broker.Stop()
	}
	defer func() { _ = t.process.Release() }()
	if err := t.process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return t.broker.AwaitStop()
}

// resolved is the broker's configuration as it resolves it, every default
// filled in: saguin --check-config <file> --output.
func (t *target) resolved() (string, error) {
	out, err := exec.Command(t.binary, "--check-config", filepath.Join(t.dir, "saguin.yaml"), "--output").Output()
	if err != nil {
		return "", fmt.Errorf("%s --check-config: %w", t.binary, err)
	}
	return string(out), nil
}

// measure runs one case once at rate (zero for the maximum mode): the
// target started, the clients connected and subscribed, the warm-up, the
// measured window, the observation to its cutoff, the stop, then the
// broker's database read and every delivery accounted for. It returns the
// outcome of every run that got as far as its clients, with an error
// where the run failed or broke the design's promise; a failed run keeps
// the broker's files, its database and log, and says where.
func measure(binary string, sz size, rate float64, kind string, reconnect bool) (o *outcome, err error) {
	t, err := startTarget(binary, kind)
	if err != nil {
		return nil, err
	}
	var cfg string
	if t.other != nil {
		cfg = fmt.Sprintf("%s (%s), sha256 %s; its configuration:\n%s", mosquittoBinary, t.other.version, sumOr(mosquittoBinary), t.other.config)
		defer func() {
			if err == nil {
				_ = os.RemoveAll(t.dir) // its persistence is the run's alone
				return
			}
			err = fmt.Errorf("%w\n%s's files and log are kept in %s; its log:\n%s", err, t.other.name, t.dir, t.other.log())
		}()
	}
	if t.broker != nil {
		if cfg, err = t.resolved(); err != nil {
			_ = t.stop() // the configuration's error is the one reported
			return nil, fmt.Errorf("%w (the broker's files are kept in %s)", err, t.dir)
		}
		defer func() {
			if err == nil {
				_ = os.RemoveAll(t.dir) // the database is the run's alone
				return
			}
			err = fmt.Errorf("%w\nthe broker's database, configuration and log are kept in %s; its log:\n%s", err, t.dir, t.broker.Log())
		}()
	}
	o, err = t.measure(sz, rate, reconnect)
	if o != nil {
		o.config, o.target = cfg, kind
	}
	return o, t.finish(o, err)
}

// finish stops the target and accounts for o, as far as it got.
func (t *target) finish(o *outcome, runErr error) error {
	err := runErr
	if stopErr := t.stop(); stopErr != nil {
		err = errors.Join(err, fmt.Errorf("stopping the target: %w", stopErr))
	}
	if o == nil {
		return err
	}
	var st *stored
	if t.broker != nil {
		var readErr error
		if st, readErr = readStore(t.broker.DB, o); readErr != nil {
			return errors.Join(err, readErr)
		}
	}
	return errors.Join(err, o.account(st))
}

// measure is measure's run against a started target.
func (t *target) measure(sz size, rate float64, reconnect bool) (*outcome, error) {
	r := newRun(sz, rate)
	r.reconnecting = reconnect
	subs, pubs, err := setUp(t.addr, r, sz)
	if err != nil {
		return nil, err
	}
	o := &outcome{run: r, subs: subs, pubs: pubs, rate: rate, size: sz, addr: t.addr}
	err = o.drive(t.pid())
	return o, err
}

// setUp connects the subscribers, each subscribed, then the publishers,
// connecting at most connecting clients at once.
func setUp(addr string, r *run, sz size) ([]*subscriber, []*publisher, error) {
	groups := sz.publishers / groupSize
	subs := make([]*subscriber, sz.subscribers)
	pubs := make([]*publisher, sz.publishers)
	errs := make([]error, sz.subscribers+sz.publishers)
	slots := make(chan struct{}, connecting)
	var wg sync.WaitGroup
	for i := range subs {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			subs[i], errs[i] = subscribe(addr, i, i%groups)
		})
	}
	wg.Wait()
	for i := range pubs {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			pubs[i], errs[sz.subscribers+i] = connectPublisher(addr, i, r.qos[i])
			if pubs[i] != nil {
				pubs[i].idle = sz.qos1Only && r.qos[i] == 0
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		closeAll(subs, pubs)
		return nil, nil, err
	}
	return subs, pubs, nil
}

// subscribe connects subscriber i with a durable session and subscribes
// it at QoS 1 to its group's topics, which must be granted QoS 1.
func subscribe(addr string, i, group int) (*subscriber, error) {
	c, _, _, err := connectSubscriber(addr, i)
	if err != nil {
		return nil, err
	}
	s := &subscriber{index: i, group: group, c: c}
	filter := "load/" + strconv.Itoa(group) + "/+"
	req := &harness.Subscribe{ID: c.NextID(), Subs: []harness.Subscription{{Filter: filter, QoS: 1}}}
	if err := c.Send(req); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("subscriber %d: SUBSCRIBE: %w", i, err)
	}
	got, err := c.Recv()
	if err == nil {
		if ack, ok := got.(*harness.Suback); !ok || ack.ID != req.ID || !slices.Equal(ack.Reasons, []byte{1}) {
			err = fmt.Errorf("%s %+v, want SUBACK %d granting QoS 1", harness.Name(got.Type()), got, req.ID)
		}
	}
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("subscriber %d: subscribing to %s: %w", i, filter, err)
	}
	return s, nil
}

// connectPublisher connects publisher i, at qos, with a clean session; its
// window is the CONNACK's Receive Maximum, 65,535 where absent (MQTT 5
// §3.2.2.3.3).
func connectPublisher(addr string, i int, qos byte) (*publisher, error) {
	c, ack, err := connect(addr, "load-pub-"+strconv.Itoa(i), 0)
	if err != nil {
		return nil, err
	}
	window := 65535
	if rm, ok := ack.Props.Get(harness.ReceiveMaximum); ok {
		window = int(rm.Int)
	}
	return &publisher{index: i, qos: qos, c: c, window: make(chan struct{}, window), outstanding: map[uint16]sentAt{}}, nil
}

// closeAll ends every client there is, during a failed setup: a
// DISCONNECT, then the close. What either finds no longer matters.
func closeAll(subs []*subscriber, pubs []*publisher) {
	for _, c := range clients(subs, pubs) {
		_ = c.Send(&harness.Disconnect{})
		_ = c.Close()
	}
}

// clients lists every client there is.
func clients(subs []*subscriber, pubs []*publisher) []*harness.Client {
	var all []*harness.Client
	for _, s := range subs {
		if s != nil {
			all = append(all, s.c)
		}
	}
	for _, p := range pubs {
		if p != nil {
			all = append(all, p.c)
		}
	}
	return all
}

// drive runs the clients through the warm-up and the measured window,
// sampling the resources and the backlog at the window's two ends, then
// observes until every publication is acknowledged and received or until
// the cutoff, observeLimit after the window, whichever comes first; then
// sends each client's DISCONNECT, its readers reading on to the
// connection's end.
func (o *outcome) drive(pid int) error {
	var (
		once    sync.Once
		first   error
		failed  = make(chan struct{})
		stop    = make(chan struct{})
		closing atomic.Bool
		writers sync.WaitGroup
		readers sync.WaitGroup
		r       = o.run
	)
	fail := func(err error) {
		once.Do(func() { first = err; close(failed) })
	}
	r.epoch = time.Now()
	o.wallEpoch = r.epoch
	cutoff := r.epoch.Add(time.Duration(r.observation.Load()))
	for _, p := range o.pubs {
		writers.Go(func() {
			if err := p.publish(r, stop, cutoff); err != nil {
				fail(err)
			}
		})
		if p.qos == 1 {
			readers.Go(func() {
				if err := p.acks(r, &closing); err != nil {
					fail(err)
				}
			})
		}
	}
	for _, s := range o.subs {
		if r.reconnecting {
			s.stable = make(chan struct{})
		}
		readers.Go(func() {
			var err error
			if r.reconnecting {
				err = s.reconnects(o.addr, r, &closing, stop, effectiveReconnectInterval())
			} else {
				err = s.receive(r, &closing)
			}
			if err != nil {
				fail(err)
			}
		})
	}
	err := o.window(pid, failed)
	close(stop)
	// A reconnect already begun may still be awaiting CONNACK. Its owner
	// publishes the final connection before shutdown sends DISCONNECT.
	for _, s := range o.subs {
		if s.stable != nil {
			<-s.stable
		}
	}
	if err == nil {
		err = o.observe(cutoff, failed)
	}
	r.endObservation()
	closing.Store(true)
	if err == nil {
		err = o.peaks(pid)
	}
	writers.Wait()
	for _, c := range clients(o.subs, o.pubs) {
		_ = c.Send(&harness.Disconnect{}) // a client already gone shows in its reader
	}
	for _, p := range o.pubs {
		if p.qos == 0 {
			_ = p.c.Close() // no reader: nothing more comes to a QoS 0 publisher
		}
	}
	readers.Wait()
	o.refreshSeconds() // all timestamped counts are now final, including late writer updates
	select {
	case <-failed:
		return errors.Join(first, err)
	default:
	}
	return err
}

// window samples diskstats at absolute second boundaries, so read time
// does not accumulate into drift. Message figures use event timestamps,
// independent of when this goroutine wakes up or a client updates a count.
func (o *outcome) window(pid int, failed <-chan struct{}) error {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	wait := func(d time.Duration) error {
		timer.Reset(time.Until(o.run.epoch.Add(warmUp + d)))
		select {
		case <-failed:
			return errors.New("a client failed")
		case <-timer.C:
			return nil
		}
	}
	if err := wait(0); err != nil {
		return err
	}
	before, err := take(pid)
	if err != nil {
		return err
	}
	if monitorDisk {
		if o.disk, err = newDiskMonitor(monitoredDrive, slowWrite); err != nil {
			return err
		}
	}
	if o.run.writes == nil {
		// The ordinary maximum-mode recipe retains its two endpoint
		// samples and does no per-message or per-second series work.
		o.backlog[0] = o.owedSoFar() - o.receivedSoFar()
		if err := wait(measuredWindow); err != nil {
			return err
		}
		o.backlog[1] = o.owedSoFar() - o.receivedSoFar()
	} else {
		for elapsed := time.Duration(0); elapsed < measuredWindow; {
			next := min(elapsed+time.Second, measuredWindow)
			if err := wait(next); err != nil {
				return err
			}
			if o.disk != nil {
				if err := o.disk.sample(); err != nil {
					return err
				}
			}
			o.seconds = append(o.seconds, second{length: next - elapsed})
			o.refreshSeconds()
			elapsed = next
		}
	}
	after, err := take(pid)
	if err != nil {
		return err
	}
	o.resources = after.since(&before)
	return nil
}

// owedSoFar is the deliveries owed for what the publishers have written.
func (o *outcome) owedSoFar() int64 {
	var n int64
	for _, s := range o.subs {
		for k := range groupSize {
			n += int64(o.pubs[s.group*groupSize+k].sent.Load())
		}
	}
	return n
}

// receivedSoFar is the distinct deliveries the subscribers have received.
func (o *outcome) receivedSoFar() int64 {
	var n int64
	for _, s := range o.subs {
		n += s.unique.Load()
	}
	return n
}

// observe waits, once the publishers have stopped, until every QoS 1
// publication is acknowledged and every subscriber has received every
// publication of its group, or until the cutoff, and records how long it
// observed after the measured window.
func (o *outcome) observe(cutoff time.Time, failed <-chan struct{}) error {
	end := o.run.epoch.Add(warmUp + measuredWindow)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	limit := time.NewTimer(time.Until(cutoff))
	defer limit.Stop()
	defer func() { o.observed = min(time.Since(end), cutoff.Sub(end)) }()
	for {
		settled := true
		for _, p := range o.pubs {
			settled = settled && p.done.Load() && p.unacknowledged() == 0
		}
		if settled && o.receivedSoFar() >= o.owedSoFar() {
			return nil
		}
		select {
		case <-tick.C:
		case <-limit.C:
			return nil
		case <-failed:
			return errors.New("a client failed")
		}
	}
}
