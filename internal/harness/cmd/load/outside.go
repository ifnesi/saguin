package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// mosquittoBinary is the outside broker the tool can drive, for comparison
// (-broker mosquitto), set by -mosquitto.
var mosquittoBinary = "/usr/sbin/mosquitto"

// mosquittoConfig is Mosquitto's configuration for a run, every setting
// chosen to match what the recipe gives Sagüin, so neither is favoured:
//
//   - listener: one loopback TCP listener on a free port, as Sagüin's.
//   - allow_anonymous true: no authentication, as Sagüin's.
//   - persistence true, in a fresh folder per run: durable sessions and
//     their queues kept, as the recipe's durable subscribers ask. Mosquitto
//     writes its persistence file only at autosave (autosave_interval,
//     default 1800 s, longer than a run) and at shutdown; Sagüin commits
//     every message before acknowledging it. That difference is
//     Mosquitto's design, left as it is, and the results say so.
//   - max_inflight_messages 20: the in-flight window toward each
//     subscriber, Sagüin's limits.max_inflight.
//   - max_queued_messages 0 and max_queued_bytes 0: no configured queue limit.
//   - queue_qos0_messages true: keep QoS 0 for away durable sessions too,
//     as Sagüin does, so the drain experiment builds the same backlog.
//   - set_tcp_nodelay true: Nagle's algorithm off, as Go's network stack,
//     and so Sagüin, sets it by default; Mosquitto's default leaves it on.
//   - log_dest stderr: its log goes to the run's log file.
const mosquittoConfig = `listener %d 127.0.0.1
allow_anonymous true
persistence true
persistence_location %s/
max_inflight_messages 20
max_queued_messages 0
max_queued_bytes 0
queue_qos0_messages true
set_tcp_nodelay true
log_dest stderr
`

// An outsideBroker is a broker other than Sagüin, started as a command in
// its own systemd scope under the same memory bound.
type outsideBroker struct {
	name, version, config string
	addr, dir, logPath    string
	cmd                   *exec.Cmd
	done                  chan struct{}
	waitErr               error
}

// startMosquitto starts Mosquitto on a free loopback port with a fresh
// persistence folder under make's TMPDIR, and waits until it listens.
func startMosquitto() (*outsideBroker, error) {
	dir, err := os.MkdirTemp("", "mosquitto-load-")
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		return nil, err
	}
	b := &outsideBroker{name: "mosquitto", addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir,
		logPath: filepath.Join(dir, "mosquitto.log"), done: make(chan struct{}),
		config: fmt.Sprintf(mosquittoConfig, port, dir)}
	b.version = mosquittoVersion()
	path := filepath.Join(dir, "mosquitto.conf")
	if err := os.WriteFile(path, []byte(b.config), 0o600); err != nil {
		return nil, err
	}
	log, err := os.Create(b.logPath)
	if err != nil {
		return nil, err
	}
	args := append(brokerScope(), mosquittoBinary, "-c", path)
	b.cmd = exec.Command(args[0], args[1:]...)
	b.cmd.Stdout, b.cmd.Stderr = log, log
	err = b.cmd.Start()
	_ = log.Close() // only the child keeps writing it
	if err != nil {
		return nil, err
	}
	go func() {
		b.waitErr = b.cmd.Wait()
		close(b.done)
	}()
	if err := b.listening(); err != nil {
		return nil, fmt.Errorf("%w (its files are kept in %s)", err, dir)
	}
	return b, nil
}

// listening waits up to 10 seconds for the broker to accept a connection.
func (b *outsideBroker) listening() error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-b.done:
			return fmt.Errorf("%s exited before listening: %w\n%s", b.name, b.waitErr, b.log())
		default:
		}
		if conn, err := net.DialTimeout("tcp", b.addr, 100*time.Millisecond); err == nil {
			return conn.Close()
		}
		if time.Now().After(deadline) {
			return errors.Join(fmt.Errorf("%s did not listen on %s within 10s", b.name, b.addr), b.kill())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pid is the broker's process id: systemd-run's scope runs it in its own
// process.
func (b *outsideBroker) pid() int { return b.cmd.Process.Pid }

// stop asks the broker to stop (SIGTERM), which must end it, exit 0,
// within a minute: Mosquitto writes its persistence file as it stops.
func (b *outsideBroker) stop() error {
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-b.done:
	case <-time.After(time.Minute):
		return errors.Join(fmt.Errorf("%s did not stop within a minute of SIGTERM", b.name), b.kill())
	}
	if b.waitErr != nil {
		return fmt.Errorf("%s stop: %w", b.name, b.waitErr)
	}
	return nil
}

// kill ends the broker at once and waits for it.
func (b *outsideBroker) kill() error {
	err := b.cmd.Process.Kill()
	<-b.done
	return err
}

// log is what the broker printed.
func (b *outsideBroker) log() string {
	data, err := os.ReadFile(b.logPath)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// sumOr is the SHA-256 of the file at path, or why it cannot be had.
func sumOr(path string) string {
	sum, err := fileSum(path)
	if err != nil {
		return err.Error()
	}
	return sum
}

// mosquittoVersion is the version line Mosquitto prints for -h.
func mosquittoVersion() string {
	out, _ := exec.Command(mosquittoBinary, "-h").CombinedOutput() // -h exits non-zero, printing the version first
	line, _, _ := strings.Cut(string(out), "\n")
	return line
}
