// Command load measures Sagüin's end-to-end throughput and latency against
// the real binary over the network, in full mode by default, by the fixed recipe of
// recipe.go: three sizes of publishers, half at QoS 0 and half at QoS 1,
// and durable subscribers, each size in maximum throughput mode and,
// when -rate is given, at that fixed load, five runs of each on a fresh broker.
//
// Each run reports, by QoS, the publications begun, written, acknowledged
// and the surviving records with intake in the measured window, per
// second; the deliveries received per second and the backlog; the latency
// of the publications sent in the measured window (the cohort), from send
// and from due time, with the share of the cohort received by the cutoff,
// overall and for the worst subscriber; the fixed mode's slots due, sent,
// missed and unsent, and its delays from due time; the broker's CPU time
// in the window, its peak memory through the observation and its cgroup's
// memory; and the load tool's own CPU, memory, garbage collection and
// scheduling latency, with the host's CPU and stall pressure. Every
// subscriber's every publication is classified on the wire as received,
// received late, a gap before a later receipt, or an unresolved tail.
// Sagüin's database evidence is reported beside those counts after it
// stops: record offsets, positions, exchanges in flight and the floor.
// Unknown tail findings never count as loss. Duplicates and reordering
// fail both brokers; unexplained gaps fail QoS 1 for both and QoS 0 for
// Sagüin. Its database checks also fail a stored publication passed above
// the floor without delivery, or an acknowledged publication never stored.
//
// -peer drives an in-process MQTT test peer instead of the broker (peer.go),
// with the same clients and checks, to show the tool's own headroom.
// -sizes, -modes and -runs run part of the recipe; the results then say
// so. -rate supplies the fixed mode's rate, outside the recipe; without it,
// fixed mode is skipped, and a fixed-only run fails. -payload, -qos1-share and -fanout run a load
// outside the recipe, its payload sizes, its share of QoS 1 publishers or
// its subscribers per group, so Sagüin is not tuned to the recipe's one
// load; the results say so too.
// -window changes the measured duration; -disk-monitor with -disk-slow
// samples the store drive each second and labels its phase.
// -ack-delay and -receive-maximum change subscriber acknowledgement timing
// and windows. -log-level, -read-connections, -storage-mode, -memory-dir,
// -commit-interval and -commit-max override Sagüin's settings, so they
// require -broker saguin; memory mode requires an explicit memory
// directory. Every option's targets, experiments and modes are in one
// table (reaches): an option given where it has no effect is refused, and
// the results name only those that act.
// -sync-delay delays each sync of Sagüin's database log through strace.
// -experiment reconnect keeps standard publishing running while durable
// subscribers alternate takeovers and returns, paced by -reconnect-interval
// (1s when unset), initially staggered evenly across that interval. The
// CONNACK delays of the reconnects begun in the measured window are
// reported by kind, and QoS 1 resends with DUP of publications sent before
// a reconnect counted apart from unexpected duplicates. Each override is
// outside the recipe.
//
// The broker runs in its own systemd scope, under its own memory bound, on
// the harness's configuration (harness.WriteConfig: a full-mode store by default,
// retention 1 day and 1 GiB, config.md's defaults otherwise), which each
// results file records as the broker resolves it. The large size's 2,000 connections are
// within max_connections' 10,000; the start's open-file check needs about
// 10,050 descriptors, and the broker refuses to start, naming both
// numbers, where its limit is lower.
//
// The results are printed and written to a file named by date and commit
// under -results; a failed run writes what it has, its own outcome as far
// as it got and the broker's log included, to one marked FAILED, and keeps
// the broker's database, configuration and log where it says.
//
// It is a program rather than a test because it runs for half an hour.
// Run it under a memory bound with the machine to itself: make load.
//
//	go run ./internal/harness/cmd/load [-binary bin/saguin] [-sizes small,medium,large] [-modes maximum,fixed] [-runs 5] [-rate publications/s] [-payload 256|1KiB|mix] [-qos1-share 0.5] [-fanout 10] [-peer] [-results bin/load]
package main

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

func main() {
	os.Exit(load(os.Args[1:], os.Stdout, os.Stderr))
}

// A runResult is one run's outcome, named by its case.
type runResult struct {
	size size
	mode mode
	*outcome
}

// options are the command line's choices.
type options struct {
	binary, results string
	sizes           []size
	modes           []mode
	runs            int
	experiment      string // -experiment: a diagnostic outside the recipe
	// target is what the clients drive: saguin (-binary), mosquitto, or
	// the in-process test peer (-peer).
	target  string
	partial []string // what of the recipe is left out, for the header
}

// load is the whole command, apart from os.Exit.
func load(args []string, stdout, stderr io.Writer) int {
	o, err := parse(args, stderr)
	if err != nil {
		harness.Say(stderr, "load: %v\n", err)
		return 2
	}
	env, setup, err := environment(o.target, o.binary)
	if err != nil {
		harness.Say(stderr, "load: %v\n", err)
		return 1
	}
	head, what := header(o, env)
	harness.Say(stdout, "%s\n", head)
	f := &fileReport{title: "Sagüin " + what + ", " + time.Now().UTC().Format("2006-01-02 15:04 UTC"), setup: setup}
	f.setup = append(f.setup, [2]string{"Runtime", fmt.Sprintf("%v warm-up, %v measured, then up to %v observed; MQTT 5, %d-byte payloads; "+
		"publishers in groups of %d, half only QoS 0 and half only QoS 1 (seed %d); durable subscribers at QoS 1 on their group",
		warmUp, measuredWindow, observeLimit, payloadSize, groupSize, seed)})
	if len(o.partial) > 0 {
		f.setup = append(f.setup, [2]string{"Partial", strings.Join(o.partial, ", ")})
	}
	kind := ""
	switch {
	case o.experiment != "":
		kind = "-" + o.experiment + "-" + o.target
		err = runExperiment(stdout, o, o.experiment, f)
	default:
		if o.target != "saguin" {
			kind = "-" + o.target
		}
		err = measureAll(stdout, o, f)
	}
	if err != nil {
		harness.Say(stdout, "\nload: FAIL: %v\n", err)
		kind += "-FAILED"
		first, _, _ := strings.Cut(err.Error(), "\n")
		f.sections = append(f.sections, section{heading: "Failed", notes: []string{first + " (the full error and the broker's log are on the terminal)"}})
	}
	path, saveErr := save(o.results, kind, f.markdown())
	if saveErr != nil {
		harness.Say(stderr, "load: writing the results: %v\n", saveErr)
		return 1
	}
	harness.Say(stdout, "load: results in %s\n", path)
	if err != nil {
		return 1
	}
	return 0
}

// parse reads the command line.
func parse(args []string, stderr io.Writer) (*options, error) {
	o := &options{}
	flags := newCommandLine(o, stderr)
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	given := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { given[f.Name] = true })
	windowRequested = given["window"]
	switch {
	case *flags.peer:
		o.target = "peer"
	case o.target != "saguin" && o.target != "mosquitto":
		return nil, fmt.Errorf("-broker %q: saguin or mosquitto", o.target)
	}
	var err error
	if payloads, err = parsePayload(*flags.payload); err != nil {
		return nil, err
	}
	if o.sizes, err = pick(*flags.sizes, sizes, sizeName); err != nil {
		return nil, fmt.Errorf("-sizes: %w", err)
	}
	for i := range o.sizes {
		o.sizes[i].subscribers = o.sizes[i].publishers / groupSize * fanout
	}
	if o.modes, err = pick(*flags.modes, []mode{maximum, fixed}, mode.String); err != nil {
		return nil, fmt.Errorf("-modes: %w", err)
	}
	if err := validate(o); err != nil {
		return nil, err
	}
	if err := o.reached(given); err != nil {
		return nil, err
	}
	if (o.experiment == "" || o.experiment == "reconnect") && rateOverride == 0 && slices.Contains(o.modes, fixed) {
		if !slices.Contains(o.modes, maximum) {
			return nil, errors.New("-modes fixed requires -rate greater than 0")
		}
		o.modes = slices.DeleteFunc(o.modes, func(m mode) bool { return m == fixed })
		o.partial = append(o.partial, "fixed mode did not run: -rate is required")
	}
	return o, nil
}

// A commandLine is load's flags, each bound to its option, with the text
// of those parse reads further: -sizes, -modes, -payload and -peer.
type commandLine struct {
	*flag.FlagSet
	sizes, modes, payload *string
	peer                  *bool
}

// newCommandLine defines load's flags on o and the options outside it,
// each set to its default.
func newCommandLine(o *options, stderr io.Writer) *commandLine {
	flags := flag.NewFlagSet("load", flag.ContinueOnError)
	flags.SetOutput(stderr)
	c := &commandLine{FlagSet: flags}
	flags.StringVar(&o.binary, "binary", "bin/saguin", "the broker binary")
	c.sizes = flags.String("sizes", "small,medium,large", "the sizes to run, of the recipe's")
	c.modes = flags.String("modes", "maximum,fixed", "the modes to run, of the recipe's")
	flags.IntVar(&o.runs, "runs", runsPerCase, "runs per case")
	flags.StringVar(&o.experiment, "experiment", "", "a diagnostic outside the recipe: "+strings.Join(experiments, " or "))
	flags.DurationVar(&commitInterval, "commit-interval", 0, "outside the recipe: Sagüin's broker.storage.publish_commit_interval (0 for none, its default)")
	flags.IntVar(&commitMax, "commit-max", 0, "outside the recipe: broker.storage.publish_commit_max_records (0 for its default)")
	flags.DurationVar(&measuredWindow, "window", measured, "outside the recipe: duration of the measured window")
	flags.BoolVar(&monitorDisk, "disk-monitor", false, "outside the recipe: sample the store drive once per second (requires -disk-slow)")
	flags.DurationVar(&slowWrite, "disk-slow", 0, "outside the recipe: mean write latency above which a second is slow (required with, and requires, -disk-monitor)")
	flags.DurationVar(&ackDelay, "ack-delay", 0, "outside the recipe: delay each subscriber PUBACK from its delivery's arrival")
	flags.DurationVar(&syncDelay, "sync-delay", 0, "outside the recipe: delay each sync of Sagüin's database log through strace")
	flags.IntVar(&receiveMaximum, "receive-maximum", 0, "outside the recipe: subscribers' Receive Maximum (0 leaves it absent)")
	flags.StringVar(&logLevel, "log-level", "", "outside the recipe: broker.log_level (empty leaves its default)")
	flags.IntVar(&readConnections, "read-connections", 0, "outside the recipe: broker.storage.read_connections (0 leaves its default)")
	flags.StringVar(&storageMode, "storage-mode", "", "outside the recipe: broker.storage.mode (normal or memory; empty or full leaves its default)")
	flags.StringVar(&memoryDir, "memory-dir", "", "outside the recipe: broker.storage.memory_dir (required with -storage-mode memory)")
	flags.DurationVar(&reconnectInterval, "reconnect-interval", 0, "outside the recipe: each subscriber's interval between reconnects in -experiment reconnect, which it requires (0 uses 1s)")
	flags.StringVar(&brokerMemory, "broker-memory", brokerMemory, "outside the recipe: the broker scope's MemoryMax")
	c.payload = flags.String("payload", strconv.Itoa(payloadSize), "outside the recipe: each payload's size (B, KiB or MiB), or mix, drawn per publication")
	flags.Float64Var(&qos1Share, "qos1-share", qos1Share, "outside the recipe: the share of the publishers at QoS 1, from 0 to 1")
	flags.Float64Var(&rateOverride, "rate", 0, "outside the recipe: the fixed mode's total publications per second, for every size (required for fixed mode; 0 skips it)")
	flags.IntVar(&fanout, "fanout", fanout, "outside the recipe: subscribers per group, each receiving every message of its group")
	c.peer = flags.Bool("peer", false, "drive the in-process test peer instead of a broker")
	flags.StringVar(&mosquittoBinary, "mosquitto", mosquittoBinary, "the Mosquitto binary -broker mosquitto runs")
	flags.StringVar(&o.target, "broker", "saguin", "the broker to drive: saguin (-binary) or mosquitto ("+mosquittoBinary+"), accounted on the wire")
	flags.StringVar(&o.results, "results", "bin/load", "the folder the results file goes in")
	return c
}

// validate checks each option's value, and the values that only make
// sense together. Where each option acts is reaches'.
func validate(o *options) error {
	switch {
	case o.runs < 1:
		return errors.New("-runs must be at least 1")
	case qos1Share < 0 || qos1Share > 1:
		return fmt.Errorf("-qos1-share %g: from 0 to 1", qos1Share)
	case fanout < 1:
		return fmt.Errorf("-fanout %d: at least 1", fanout)
	case rateOverride < 0:
		return fmt.Errorf("-rate %g: at least 0", rateOverride)
	case measuredWindow <= 0 || measuredWindow > time.Duration(1<<63-1)-warmUp-observeLimit:
		return errors.New("-window must be positive and leave room for warm-up and observation")
	case slowWrite < 0:
		return errors.New("-disk-slow must be at least 0")
	case slowWrite != 0 && !monitorDisk:
		return errors.New("-disk-slow requires -disk-monitor")
	case monitorDisk && slowWrite == 0:
		return errors.New("-disk-monitor requires an explicit positive -disk-slow threshold for this machine")
	case ackDelay < 0:
		return errors.New("-ack-delay must be at least 0")
	case syncDelay < 0:
		return errors.New("-sync-delay must be at least 0")
	case receiveMaximum < 0 || receiveMaximum > 65535:
		return errors.New("-receive-maximum must be 0 (absent) or 1 to 65535")
	case !slices.Contains([]string{"", "debug", "info", "warn", "error"}, logLevel):
		return errors.New("-log-level must be empty, debug, info, warn or error")
	case readConnections != 0 && (readConnections < 2 || readConnections > 8):
		return errors.New("-read-connections must be 0 (default) or 2 to 8")
	case !slices.Contains([]string{"", "full", "normal", "memory"}, storageMode):
		return errors.New("-storage-mode must be empty, full, normal or memory")
	case memoryDir != "" && storageMode != "memory":
		return errors.New("-memory-dir requires -storage-mode memory")
	case storageMode == "memory" && memoryDir == "":
		return errors.New("-storage-mode memory requires -memory-dir")
	case reconnectInterval < 0:
		return errors.New("-reconnect-interval must be at least 0")
	case o.experiment == "reconnect" && o.target == "peer":
		return errors.New("-experiment reconnect requires a broker with durable sessions")
	}
	if monitorDisk {
		var err error
		if monitoredDrive, err = driveOf(os.TempDir()); err != nil {
			return fmt.Errorf("-disk-monitor: %w", err)
		}
	}
	return nil
}

// A reach is where an option of the command line acts: the targets
// (saguin, mosquitto, peer), the experiments ("" for the recipe) and the
// modes whose runs it changes, each nil for every one. Given where it
// cannot act, the option is refused; acting there, applied names it in the
// results' Partial row, or returns "" where its value is the recipe's or
// the results show it elsewhere.
type reach struct {
	name        string
	targets     []string
	experiments []string
	modes       []mode
	applied     func(o *options) string
}

// Where options act, by kind of run.
var (
	// brokers are the targets started in their own scope, each keeping its
	// files under the tool's temporary folder, on the drive the disk
	// monitor samples, and honouring a subscriber's Receive Maximum; the
	// peer runs in the tool's process, keeps no files and grants its own.
	brokers    = []string{"saguin", "mosquitto"}
	saguinOnly = []string{"saguin"} // started from -binary: Sagüin's configuration and its database log
	// recipeCases are the experiments that run the recipe's cases, by
	// size, mode and run; the others run the small size's clients once,
	// in the maximum mode.
	recipeCases = []string{"", "reconnect"}
	// withSubscribers are the experiments with subscribers;
	// publish-only and publish-mixed have none.
	withSubscribers = []string{"", "reconnect", "drain-only"}
	// monitored are the experiments that keep a per-second series for the
	// disk monitor; drain-only keeps none.
	monitored = []string{"", "publish-only", "publish-mixed", "reconnect"}
)

// reaches is every option's reach, in the order the results name them.
var reaches = []reach{
	{name: "binary", targets: saguinOnly},
	{name: "broker", targets: brokers}, // -peer drives the peer whatever it names
	{name: "peer"},
	{name: "mosquitto", targets: []string{"mosquitto"}},
	{name: "results"},
	{name: "experiment", applied: func(o *options) string {
		if o.experiment != "reconnect" {
			return "" // the results' title names the experiment
		}
		return "experiment reconnect (outside the recipe), each subscriber reconnects every " + effectiveReconnectInterval().String() + ", staggered evenly; takeover first, then alternating with returns"
	}},
	{name: "sizes", experiments: recipeCases, applied: func(o *options) string {
		return unless(len(o.sizes) == len(sizes), "sizes "+strings.Join(names(o.sizes, sizeName), ","))
	}},
	{name: "modes", experiments: recipeCases, applied: func(o *options) string {
		return unless(len(o.modes) == 2, "modes "+strings.Join(names(o.modes, mode.String), ","))
	}},
	{name: "runs", experiments: recipeCases, applied: func(o *options) string {
		return unless(o.runs == runsPerCase, fmt.Sprintf("%d run(s) per case", o.runs))
	}},
	{name: "rate", experiments: recipeCases, modes: []mode{fixed}, applied: func(*options) string {
		return unless(rateOverride == 0, fmt.Sprintf("fixed rate %g publications/s (outside the recipe)", rateOverride))
	}},
	{name: "payload", applied: func(*options) string {
		return unless(payloads.fixed == payloadSize, "payloads "+payloads.String()+" (outside the recipe)")
	}},
	{name: "qos1-share", applied: func(*options) string {
		return unless(qos1Share == 0.5, fmt.Sprintf("QoS 1 share %g of the publishers (outside the recipe)", qos1Share))
	}},
	{name: "fanout", experiments: recipeCases, applied: func(*options) string {
		return unless(fanout == groupSize, fmt.Sprintf("fanout %d subscribers per group (outside the recipe)", fanout))
	}},
	{name: "broker-memory", targets: brokers, applied: func(*options) string {
		return unless(brokerMemory == "4G", "broker MemoryMax "+brokerMemory+" (outside the recipe)")
	}},
	{name: "window", applied: func(*options) string {
		return unless(measuredWindow == measured, outside("window", measuredWindow.String()))
	}},
	{name: "ack-delay", experiments: withSubscribers, applied: func(*options) string {
		return unless(ackDelay == 0, outside("ack-delay", ackDelay.String()))
	}},
	{name: "receive-maximum", targets: brokers, experiments: withSubscribers, applied: func(*options) string {
		return unless(receiveMaximum == 0, outside("receive-maximum", strconv.Itoa(receiveMaximum)))
	}},
	{name: "sync-delay", targets: saguinOnly, applied: func(*options) string {
		return unless(syncDelay == 0, outside("sync-delay", syncDelay.String()))
	}},
	{name: "log-level", targets: saguinOnly, applied: func(*options) string {
		return unless(logLevel == "", outside("log-level", logLevel))
	}},
	{name: "read-connections", targets: saguinOnly, applied: func(*options) string {
		return unless(readConnections == 0, outside("read-connections", strconv.Itoa(readConnections)))
	}},
	{name: "storage-mode", targets: saguinOnly, applied: func(*options) string {
		return unless(storageMode == "", outside("storage-mode", storageMode))
	}},
	{name: "memory-dir", targets: saguinOnly, applied: func(*options) string {
		return unless(memoryDir == "", outside("memory-dir", memoryDir))
	}},
	{name: "commit-interval", targets: saguinOnly, applied: func(*options) string {
		return unless(commitInterval == 0, outside("publish_commit_interval", commitInterval.String()))
	}},
	{name: "commit-max", targets: saguinOnly, applied: func(*options) string {
		return unless(commitMax == 0, outside("publish_commit_max_records", strconv.Itoa(commitMax)))
	}},
	{name: "reconnect-interval", experiments: []string{"reconnect"}, applied: func(*options) string {
		return unless(reconnectInterval == 0, outside("reconnect-interval", reconnectInterval.String()))
	}},
	{name: "disk-monitor", targets: brokers, experiments: monitored, applied: func(*options) string {
		if monitorDisk {
			return fmt.Sprintf("disk monitor on %s, slow above %v (outside the recipe)", monitoredDrive, slowWrite)
		}
		return "" // the recipe's runs are unwatched
	}},
	{name: "disk-slow", targets: brokers, experiments: monitored}, // named with the monitor
}

// reached refuses each option given where it cannot act, and names in the
// results each that acts outside the recipe.
func (o *options) reached(given map[string]bool) error {
	var refused []error
	for i := range reaches {
		r := &reaches[i]
		err := r.refusal(o)
		switch {
		case err != nil && given[r.name]:
			refused = append(refused, err)
		case err == nil && r.applied != nil:
			if s := r.applied(o); s != "" {
				o.partial = append(o.partial, s)
			}
		}
	}
	return errors.Join(refused...)
}

// refusal is why r's option cannot act on o's run; nil where it can.
func (r *reach) refusal(o *options) error {
	switch {
	case r.targets != nil && !slices.Contains(r.targets, o.target):
		return fmt.Errorf("-%s has no effect against %s: it acts only against %s", r.name, o.target, either(r.targets))
	case r.experiments != nil && !slices.Contains(r.experiments, o.experiment):
		return fmt.Errorf("-%s has no effect in %s: it acts only in %s", r.name, experimentName(o.experiment), either(names(r.experiments, experimentName)))
	case r.modes != nil && !slices.ContainsFunc(o.modes, func(m mode) bool { return slices.Contains(r.modes, m) }):
		return fmt.Errorf("-%s has no effect with -modes %s: it acts only in the %s mode", r.name, strings.Join(names(o.modes, mode.String), ","), either(names(r.modes, mode.String)))
	}
	return nil
}

// experimentName names an experiment, "" being the recipe.
func experimentName(e string) string {
	if e == "" {
		return "the recipe"
	}
	return "experiment " + e
}

// either lists choices as alternatives: a, b or c.
func either(choices []string) string {
	n := len(choices)
	if n == 1 {
		return choices[0]
	}
	return strings.Join(choices[:n-1], ", ") + " or " + choices[n-1]
}

// sizeName is a size's name.
func sizeName(s size) string { return s.name }

// outside names an option's value outside the recipe.
func outside(name, value string) string { return name + " " + value + " (outside the recipe)" }

// unless is "" where recipe holds, else text.
func unless(recipe bool, text string) string {
	if recipe {
		return ""
	}
	return text
}

// names is each of xs's name, in order.
func names[T any](xs []T, name func(T) string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, name(x))
	}
	return out
}

// pick returns the items of all named in list, in all's order, each
// named once.
func pick[T any](list string, all []T, name func(T) string) ([]T, error) {
	wanted := strings.Split(list, ",")
	var chosen []T
	for _, x := range all {
		if slices.Contains(wanted, name(x)) {
			chosen = append(chosen, x)
		}
	}
	if len(chosen) != len(wanted) {
		return nil, fmt.Errorf("%q: name each of %s at most once", list, strings.Join(names(all, name), ", "))
	}
	return chosen, nil
}

// header is the results' opening: what was measured, where, and by which
// recipe, saying when the run is only part of it.
func header(o *options, env string) (text, what string) {
	var b strings.Builder
	what = "end-to-end baseline"
	switch {
	case o.experiment != "":
		what = "experiment " + o.experiment + " (outside the recipe) against " + o.target
	case o.target == "peer":
		what = "load tool headroom, against the in-process test peer (no broker)"
	case o.target == "mosquitto":
		what = "comparison: " + mosquittoBinary + " by Sagüin's recipe, accounted on the wire"
	}
	fmt.Fprintf(&b, "Sagüin %s, %s\n%s", what, time.Now().UTC().Format("2006-01-02 15:04:05 UTC"), env)
	fmt.Fprintf(&b, "recipe:   internal/harness/cmd/load/recipe.go: MQTT 5, %d-byte payloads, publishers in groups of %d,\n", payloadSize, groupSize)
	fmt.Fprintf(&b, "          half publishing only at QoS 0 and half only at QoS 1 (seed %d), each durable subscriber\n", seed)
	fmt.Fprintf(&b, "          at QoS 1 on one group; %v warm-up, %v measured, then observed for at most %v\n", warmUp, measured, observeLimit)
	for _, sz := range sizes {
		fmt.Fprintf(&b, "          %s: %d publishers, %d subscribers\n", sz.name, sz.publishers, sz.subscribers)
	}
	if len(o.partial) > 0 {
		fmt.Fprintf(&b, "PARTIAL:  %s; the recipe runs every size and mode %d times\n", strings.Join(o.partial, ", "), runsPerCase)
	}
	return b.String(), what
}

// measureAll runs the chosen cases, each run reported as it ends, then the
// table; a failed run ends it, with what was measured before it.
func measureAll(out io.Writer, o *options, f *fileReport) error {
	var all []runResult
	var config string
	defer func() {
		if config != "" {
			f.setup = append(f.setup, brokerSetup(o.target, config))
		}
		if len(all) > 0 {
			f.sections = append(f.sections, resultSections(all)...)
		}
		if o.experiment == "reconnect" {
			for _, c := range resultCases(all) {
				var subs []*subscriber
				for _, run := range c.runs {
					subs = append(subs, run.subs...)
				}
				s := reconnectSection(subs)
				s.heading += ": " + c.name
				f.sections = append(f.sections, s)
			}
		}
		if len(all) > 0 {
			harness.Say(out, "\n")
			summarize(out, all)
		}
		if config != "" {
			harness.Say(out, "\nbroker configuration (the listener's port differs per run):\n%s", config)
		}
	}()
	for _, sz := range o.sizes {
		for i := range o.runs {
			for _, m := range o.modes {
				rate := 0.0
				if m == fixed {
					rate = rateOverride
				}
				res, err := measure(o.binary, sz, rate, o.target, o.experiment == "reconnect")
				if res != nil {
					all = append(all, runResult{sz, m, res})
					config = cmp.Or(config, res.config)
					harness.Say(out, "\n%s %s run %d:\n%s", sz.name, m, i+1, res)
				}
				if err != nil {
					return fmt.Errorf("%s %s run %d: %w", sz.name, m, i+1, err)
				}
			}
		}
	}
	return nil
}

// save writes report to a new file in dir named by the date and time, in
// UTC, the commit measured and kind.
func save(dir, kind string, report []byte) (string, error) {
	head, err := commit()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s%s.md", time.Now().UTC().Format("060102-150405"), head[:12], kind)
	path := filepath.Join(dir, name)
	return path, os.WriteFile(path, report, 0o644)
}
