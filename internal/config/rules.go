package config

import (
	"errors"
	"fmt"

	"github.com/ifnesi/saguin/internal/payload"
)

// resolve fills only absent keys: an explicit none must override inheritance.
func (c *Config) resolve(seen map[string]bool) {
	b := &c.Broker
	if !seen["broker.broadcast.retention_period"] {
		b.Broadcast.RetentionPeriod = b.Storage.DefaultRetentionPeriod
	}
	if !seen["broker.broadcast.retention_bytes"] {
		b.Broadcast.RetentionBytes = b.Storage.DefaultRetentionBytes
	}
	if !seen["broker.limits.max_connect_size"] {
		b.Limits.MaxConnectSize = min(defaultMaxConnectSize, b.Limits.MaxPacketSize)
	}
	if !seen["broker.storage.publish_commit_max_records"] {
		b.Storage.PublishCommitMaxRecords = min(defaultCommitRecords, b.Limits.MaxConnections)
	}
	c.resolveListeners(seen)
}

func (c *Config) validate(seen map[string]bool) error {
	s, l := &c.Broker.Storage, &c.Broker.Limits
	if s.PublishCommitMaxRecords > l.MaxConnections {
		return refuse("broker.storage.publish_commit_max_records", "must not exceed broker.limits.max_connections")
	}
	if l.MaxConnectSize > l.MaxPacketSize {
		return refuse("broker.limits.max_connect_size", "must not exceed broker.limits.max_packet_size")
	}
	minimum := payload.BodyMemory(int(l.MaxPacketSize))
	requirement := ""
	if minimum > int64(l.MaxPacketSize) {
		requirement = " plus its largest S2 copy"
	}
	topicRequirement := ""
bridges:
	for i := range c.Bridges {
		bridge := &c.Bridges[i]
		for j := range bridge.Topics {
			if bridge.Topics[j].Direction == In {
				minimum += int64(l.MaxTopicLength)
				topicRequirement = " plus max_topic_length for an inbound bridge topic"
				break bridges
			}
		}
	}
	if int64(l.MaxIncomingMemory) < minimum {
		value, _ := formatQuantity(int64(l.MaxIncomingMemory), sizeUnits)
		packet, _ := formatQuantity(int64(l.MaxPacketSize), sizeUnits)
		source := ""
		if !seen["broker.limits.max_incoming_memory"] {
			source = " (its default)"
		}
		// Suggest a whole MiB the size parser accepts, always rounding up.
		const mib = 1 << 20
		setting, _ := formatQuantity((minimum+mib-1)/mib*mib, sizeUnits)
		return refuse("broker.limits.max_incoming_memory", fmt.Sprintf(
			"is %s%s, but max_packet_size %s%s needs at least %d bytes (%.1fMiB); set max_incoming_memory: %s",
			value, source, packet, requirement+topicRequirement, minimum, float64(minimum)/mib, setting))
	}
	return nil
}

// validateStorage distinguishes supplied mode-specific keys from defaults.
// Report every inapplicable key through the loader's ordinary refusals.
func (c *Config) validateStorage(seen map[string]bool) error {
	mode := c.Broker.Storage.Mode
	var findings []error
	for _, rule := range []struct {
		key  string
		mode StorageMode
	}{
		{"broker.storage.sync_interval", Normal},
		{"broker.storage.memory_dir", Memory},
		{"broker.storage.snapshot_interval", Memory},
	} {
		if seen[rule.key] && mode != rule.mode {
			findings = append(findings, refuse(rule.key, fmt.Sprintf("requires broker.storage.mode %s; mode is %s", rule.mode, mode)))
		}
	}
	return errors.Join(findings...)
}
