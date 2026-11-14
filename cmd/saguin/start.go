package main

import (
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	opshttp "github.com/ifnesi/saguin/internal/operations"
)

// startupInfo describes only listeners main has successfully bound. Addresses
// come from the sockets, so an automatically chosen port is reported as served.
func startupInfo(cfg *config.Config, path string, began time.Time, mqtt []*connection.Listener, operations []*opshttp.Listener) core.StartInfo {
	var message strings.Builder
	fmt.Fprintf(&message, "started version=%s configuration=%q storage=%s file=%q listeners=[",
		buildVersion(), path, cfg.Broker.Storage.Mode, cfg.Broker.Storage.FilePath)
	appendListeners := func(listen *config.Listen, prefix string, address func(int) string) {
		index := 0
		for _, group := range []struct {
			kind string
			list []config.Listener
		}{{"tcp", listen.TCP}, {"unix", listen.Unix}, {"ws", listen.WS}} {
			for _, l := range group.list {
				fmt.Fprintf(&message, " %s%s:%s=%q", prefix, group.kind, l.Name, address(index))
				index++
			}
		}
	}
	appendListeners(&cfg.Broker.MQTT.Listen, "mqtt/", func(i int) string { return mqtt[i].Addr().String() })
	if cfg.Broker.Operations != nil {
		appendListeners(&cfg.Broker.Operations.Listen, "operations/", func(i int) string { return operations[i].Addr().String() })
	}
	message.WriteString(" ] bridges=[")
	for i := range cfg.Bridges {
		b := &cfg.Bridges[i]
		fmt.Fprintf(&message, " %s=%q", b.Name, b.Peer)
	}
	message.WriteString(" ]")
	return core.StartInfo{Message: message.String(), Began: began}
}

// checkOpenFiles counts served client sockets, one unserved socket per MQTT
// listener and the other simultaneous file uses: SQLite connections and
// sidecars, the storage lock, listener sockets and Unix locks, pid output
// and 64 per operations listener. A reserve of 32 covers runtime and
// temporary startup or reload uses, including standard streams and pollers.
func checkOpenFiles(cfg *config.Config) error {
	b := &cfg.Broker
	mqtt := &b.MQTT.Listen
	listeners := len(mqtt.TCP) + len(mqtt.WS) + len(mqtt.Unix)
	const (
		reserve     = 32
		storageLock = 1
	)
	// A bridge waits for its previous socket's close report before redial.
	// Establishment may race IPv4 and IPv6: two descriptors per bridge.
	required := uint64(b.Limits.MaxConnections) + reserve + storageLock + 3*uint64(b.Storage.ReadConnections+1) + uint64(2*listeners+len(mqtt.Unix)) + 2*uint64(len(cfg.Bridges))
	if b.Storage.Mode == config.Memory {
		// The RAM lock, and the source and destination of a snapshot copy.
		required += 3
	}
	if b.PIDFile != "" {
		required++
	}
	if b.Operations != nil {
		o := &b.Operations.Listen
		required += 65*uint64(len(o.TCP)+len(o.Unix)) + uint64(len(o.Unix))
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("read open-file limit: %w", err)
	}
	if limit.Cur < required {
		return fmt.Errorf("open-file limit %d must cover %d open files", limit.Cur, required)
	}
	return nil
}
