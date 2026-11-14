// Command saguin runs the Sagüin MQTT broker.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	opshttp "github.com/ifnesi/saguin/internal/operations"
	"github.com/ifnesi/saguin/internal/store"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is main's goroutine, including startup and graceful shutdown. It starts
// only the core and listeners; their other owners start themselves (R1).
func run(args []string, stdout, stderr io.Writer) (code int) {
	started := time.Now()
	var signals, reloadSignals, hangupSignals chan os.Signal
	stop := stopRequest{cause: "requested"}
	if len(args) != 0 && args[0] == "--config" {
		// Claim the broker's signals before even reading its configuration.
		// Other commands keep the process's default signal behaviour.
		signals = make(chan os.Signal, 2)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		signal.Ignore(syscall.SIGPIPE)
		stop.signals = signals
		defer signal.Stop(signals)
		reloadSignals = make(chan os.Signal, 1)
		signal.Notify(reloadSignals, syscall.SIGUSR1)
		defer signal.Stop(reloadSignals)
		hangupSignals = make(chan os.Signal, 1)
		signal.Notify(hangupSignals, syscall.SIGHUP)
		defer signal.Stop(hangupSignals)
	}
	if len(args) == 1 && args[0] == "--version" {
		if _, err := fmt.Fprintln(stdout, "saguin "+buildVersion()); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if len(args) == 1 && args[0] == "--licenses" {
		return licensesCommand(stdout, stderr)
	}
	if len(args) != 0 && args[0] == "--route" {
		return routeCommand(args[1:], stdout, stderr)
	}
	if len(args) != 0 && args[0] == "--acl" {
		return aclCommand(args[1:], stdout, stderr)
	}
	if len(args) != 0 && args[0] == "--drop-channel" {
		return dropChannelCommand(args[1:], stderr)
	}
	if len(args) != 0 && args[0] == "--passwd" {
		return passwdCommand(args[1:], stdout, stderr)
	}
	if !configCommand(args) {
		_, _ = fmt.Fprintln(stderr, "usage: saguin --config <file>\n       saguin --check-config <file> [--output]\n       saguin --route <file> <topic or filter> [--json]\n       saguin --acl <config> <user> [client-id] [--json]\n       saguin --drop-channel <file> <channel>\n       saguin --passwd list|add|delete|scope <file> ...\n       saguin --licenses\n       saguin --version")
		return 2
	}
	cfg, err := config.LoadAtStart(args[1])
	if err == nil && args[0] == "--check-config" {
		if err = errors.Join(cfg.LoadPasswords(), cfg.LoadBridgePasswords(), cfg.LoadACL(), cfg.CheckTLS()); err == nil {
			return checkOutput(cfg, len(args) == 3, stdout, stderr)
		}
	}
	if err == nil {
		err = errors.Join(cfg.LoadPasswords(), cfg.LoadBridgePasswords(), cfg.LoadACL())
	}
	if err == nil {
		err = cfg.InitReloads()
	}
	if err == nil {
		err = checkOpenFiles(cfg)
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if stop.asked() {
		return 0
	}
	reg := metrics.New()
	reg.Process(buildVersion(), cfg.Broker.ID, started)
	reg.Goroutine("main").Add(1)
	defer reg.Goroutine("main").Add(-1)
	log := logger.Start(stderr, cfg.Broker.ID, reg)
	credits := logger.NewCredits(cfg.Broker.LogLevel, reg)
	defer log.StopWithin(cfg.Broker.Limits.WriteTimeout)
	failed := func(err error) {
		code = 1
		mainLog(log, credits, logger.Line{At: time.Now(), Level: logger.Error, Msg: err.Error()})
	}
	if stop.asked() {
		return code
	}
	lock, err := store.Lock(cfg)
	if err != nil {
		failed(err)
		return code
	}
	defer func() {
		if lock != nil {
			if err := lock.Release(); err != nil {
				failed(err)
			}
		}
	}()
	if stop.asked() {
		return code
	}
	if cfg.Broker.PIDFile != "" {
		if err := os.WriteFile(cfg.Broker.PIDFile, fmt.Appendf(nil, "%d\n", os.Getpid()), 0o644); err != nil {
			failed(err)
			return code
		}
	}
	if stop.asked() {
		return code
	}
	st, state, err := store.Start(cfg, reg, lock)
	lock = nil // Start owns it on both success and failure.
	if err != nil {
		failed(err)
		return code
	}
	var stopBegan time.Time
	var closed int
	var snapshotTaken bool
	defer func() {
		if err := st.Stop(); err != nil {
			failed(err)
		}
		if !stopBegan.IsZero() {
			now := time.Now()
			message := fmt.Sprintf("stopped: took=%s connections_closed=%d", now.Sub(stopBegan), closed)
			if cfg.Broker.Storage.Mode == config.Memory {
				message += fmt.Sprintf(" final_snapshot_taken=%t", snapshotTaken)
			}
			mainLog(log, credits, logger.Line{At: now, Level: logger.Info, Msg: message})
		}
	}()
	if stop.asked() {
		return code
	}
	if err := core.CheckChannels(cfg, state); err != nil {
		failed(err)
		return code
	}
	inbound := make(chan connection.Event, core.InboundCapacity)
	permits := connection.NewPermits(cfg.Broker.Limits.MaxConnections)
	m := connection.NewMetrics(reg)
	c := core.New(cfg, reg, state, st.Writes(), st.Reads(), log.Lines(), credits)
	c.ConfigureBridges(cfg, m, reg)
	coreDone, coreStop := make(chan struct{}), make(chan struct{})
	go func() {
		c.Run(inbound, st.Answers(), log.Results(), coreStop, connection.RealClock{})
		close(coreDone)
	}()
	var listeners []*connection.Listener
	mqtt := &cfg.Broker.MQTT.Listen
	mqttListeners := slices.Concat(mqtt.TCP, mqtt.Unix, mqtt.WS)
	var ops []*opshttp.Listener
	var opsListeners []config.Listener
	if cfg.Broker.Operations != nil {
		o := &cfg.Broker.Operations.Listen
		opsListeners = slices.Concat(o.TCP, o.Unix)
	}
	opsCount := len(opsListeners)
	opsDone := make(chan error, opsCount)
	opsActive := 0
	listenDone := make(chan error, len(mqttListeners))
	active := 0
	// Main records its own errors while the core owns admission. It sends
	// their lines only after coreDone returns the same credits to main.
	var pending []logger.Line
	remember := func(err error) {
		code = 1
		stop.cause = err.Error()
		pending = append(pending, logger.Line{At: time.Now(), Level: logger.Error, Msg: err.Error()})
	}
	// Failure wins over a simultaneously ready signal or startup completion.
	if ready, err := waitReady(c, &stop); err != nil {
		code = 1 // the core logged its store error
	} else if ready {
		om := opshttp.NewMetrics(reg)
		startupStopped := false
		for i := range mqttListeners {
			if stop.asked() {
				startupStopped = true
				break
			}
			listen := connection.Listen
			if i >= len(mqtt.TCP)+len(mqtt.Unix) {
				listen = connection.ListenWebSocket
			}
			l, err := listen(cfg, &mqttListeners[i], inbound, permits, m)
			if err != nil {
				remember(err)
				break
			}
			listeners = append(listeners, l)
			active++
			go func() { listenDone <- l.Run() }()
		}
		startupStopped = startupStopped || stop.asked()
		if code == 0 && !startupStopped && len(listeners) == len(mqttListeners) {
			for i := range opsCount {
				if stop.asked() {
					startupStopped = true
					break
				}
				l, err := opshttp.Listen(cfg, &opsListeners[i], reg, om, c.Health(), inbound)
				if err != nil {
					remember(err)
					break
				}
				ops = append(ops, l)
				opsActive++
				go func() { opsDone <- l.Run() }()
			}
		}
		if code == 0 && !startupStopped && len(listeners) == len(mqttListeners) && len(ops) == opsCount {
			st.Started()
			c.Listening(startupInfo(cfg, args[1], started, listeners, ops))
			part, reloading, submitted := 0, false, false
		serving:
			for {
				// Stop and a core failure win before beginning another reload step.
				select {
				case <-c.Failures():
					code = 1
					break serving
				default:
				}
				if stop.asked() {
					break
				}
				if reloading && !submitted {
					event := reloadStep(cfg, args[1], part)
					c.Reloads() <- event
					submitted = true
				}
				var trigger <-chan os.Signal
				if !reloading {
					trigger = reloadSignals
				}
				select {
				case sig := <-signals:
					stop.cause = sig.String()
					break serving
				case <-hangupSignals:
					c.Reloads() <- core.Reload{Notice: &logger.Line{At: time.Now(), Level: logger.Info,
						Msg: "SIGHUP changes nothing; SIGUSR1 reloads"}}
				case <-trigger:
					part, reloading = 0, true
				case <-c.Reloaded():
					part++
					submitted = false
					reloading = part != 3
				case <-c.Failures():
					code = 1
					break serving
				case err := <-listenDone:
					active--
					if err != nil {
						remember(err)
					}
					break serving
				case err := <-opsDone:
					opsActive--
					if err != nil {
						remember(err)
					}
					break serving
				}
			}

		}
	}
	// An error already waiting wins even when a signal woke main first.
	select {
	case <-c.Failures():
		code = 1
	default:
	}
	stopBegan = time.Now()
	for _, l := range slices.Backward(listeners) {
		if err := l.Close(); err != nil {
			remember(err)
		}
	}
	for range active {
		if err := <-listenDone; err != nil {
			remember(err)
		}
	}
	c.StopFor(stop.cause)
	for {
		select {
		case <-c.Drained():
			select {
			case <-c.Failures():
				code = 1
			default:
			}
			for _, l := range listeners {
				l.Wait()
			}
			c.WaitBridges()
			for _, l := range ops {
				if err := l.Close(); err != nil {
					remember(err)
				}
			}
			for range opsActive {
				if err := <-opsDone; err != nil {
					remember(err)
				}
			}
			for _, l := range ops {
				l.Wait()
			}
			close(coreStop)
			<-coreDone
			closed, snapshotTaken = c.StopResult()
			if c.SnapshotSafe() {
				st.Snapshotted()
			} else if cfg.Broker.Storage.Mode == config.Memory && code == 0 {
				failed(fmt.Errorf("RAM copy %s retained: final snapshot is not safe", cfg.MemoryPath()))
			}
			for _, line := range pending {
				mainLog(log, credits, line)
			}
			return code
		case <-c.Failures():
			code = 1
		}
	}
}

// mainLog is called only while main owns admission, before the core starts
// or after it ends. Process ready write results before admitting another line.
func mainLog(log *logger.Logger, credits *logger.Credits, line logger.Line) {
	for {
		select {
		case result := <-log.Results():
			credits.Return(result)
		default:
			if credits.Admit(line.Level) {
				log.Lines() <- line
			}
			return
		}
	}
}

func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return "(devel)"
}

func configCommand(args []string) bool {
	return (len(args) == 2 || (len(args) == 3 && args[0] == "--check-config" && args[2] == "--output")) &&
		(args[0] == "--config" || args[0] == "--check-config") && args[1] != "" && !strings.HasPrefix(args[1], "--")
}

func checkOutput(cfg *config.Config, output bool, stdout, stderr io.Writer) int {
	if !output {
		return 0
	}
	out, err := cfg.Marshal()
	if err == nil {
		var n int
		n, err = stdout.Write(out)
		if err == nil && n != len(out) {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// stopRequest retains the signal that asked for shutdown. Each step checks its
// error first: a refused start wins over a simultaneously ready signal.
type stopRequest struct {
	signals <-chan os.Signal
	cause   string
}

func (r *stopRequest) asked() bool {
	select {
	case sig := <-r.signals:
		r.cause = sig.String()
		return true
	default:
		return false
	}
}

// waitReady orders a fatal error, a stop signal, then readiness. Its wakeup
// only retains what arrived; the checks after it decide simultaneous events.
func waitReady(c *core.Core, stop *stopRequest) (ready bool, err error) {
	select {
	case err = <-c.Failures():
		return false, err
	case <-c.Ready():
		ready = true
	case sig := <-stop.signals:
		stop.cause = sig.String()
	}
	select {
	case err = <-c.Failures():
		return false, err
	default:
	}
	if ready && stop.asked() {
		ready = false
	}
	return ready, nil
}

// dropChannelCommand runs without broker goroutines, under the same live lock.
func dropChannelCommand(args []string, stderr io.Writer) int {
	if len(args) != 2 {
		_, _ = fmt.Fprintln(stderr, "usage: saguin --drop-channel <file> <channel>")
		return 2
	}
	cfg, err := config.Load(args[0])
	if err == nil {
		err = store.DropChannel(cfg, args[1])
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
