package config

import (
	"math"
	"time"
)

// field binds a YAML key to its resolved value, or to a section's children.
// Numeric bounds use bytes, nanoseconds or counts according to the value's
// type. The order of children is config.md's row order, also used by Marshal.
type field struct {
	name         string
	value        any
	children     []field
	minimum      int64
	maximum      int64
	none         bool
	required     bool
	milliseconds bool
	omit         bool // omit only on output; the key remains readable
}

func count(name string, value *int, minimum, maximum int, none bool) field {
	return field{name: name, value: value, minimum: int64(minimum), maximum: int64(maximum), none: none}
}

func size(name string, value *Size, minimum, maximum int64, none bool) field {
	return field{name: name, value: value, minimum: minimum, maximum: maximum, none: none}
}

func duration(name string, value *time.Duration, minimum, maximum time.Duration, none, milliseconds bool) field {
	return field{name: name, value: value, minimum: int64(minimum), maximum: int64(maximum), none: none, milliseconds: milliseconds}
}

// schema binds a configuration without relying on reflection or YAML's
// coercion of values. Unknown sections are deliberately absent.
func schema(c *Config) []field {
	b := &c.Broker
	s, l := &b.Storage, &b.Limits
	syncInterval := duration("sync_interval", &s.SyncInterval, 100*time.Millisecond, math.MaxInt64, false, true)
	syncInterval.omit = s.Mode != Normal
	snapshotInterval := duration("snapshot_interval", &s.SnapshotInterval, time.Minute, math.MaxInt64, false, false)
	snapshotInterval.omit = s.Mode != Memory
	return []field{{name: "broker", children: []field{
		{name: "id", value: &b.ID, required: true},
		{name: "log_level", value: &b.LogLevel},
		{name: "pid_file", value: &b.PIDFile, none: true},
		duration("delivery_admission_delay", &b.DeliveryAdmissionDelay, time.Millisecond, math.MaxInt64, true, true),
		duration("delivery_peer_timeout", &b.DeliveryPeerTimeout, time.Millisecond, math.MaxInt64, false, true),
		{name: "mqtt", children: []field{
			{name: "listen", value: &b.MQTT.Listen},
			{name: "password_file", value: &b.MQTT.PasswordFile, none: true},
			{name: "allow_anonymous", value: &b.MQTT.AllowAnonymous},
			{name: "min_protocol_version", value: &b.MQTT.MinProtocolVersion},
			{name: "acl_file", value: &b.MQTT.ACLFile, none: true},
		}},
		{name: "operations", value: &b.Operations, omit: b.Operations == nil},
		{name: "storage", children: []field{
			{name: "file_path", value: &s.FilePath, required: true},
			{name: "mode", value: &s.Mode},
			{name: "default_retention_period", value: &s.DefaultRetentionPeriod, minimum: int64(time.Second), maximum: math.MaxInt64, none: true, required: true},
			{name: "default_retention_bytes", value: &s.DefaultRetentionBytes, minimum: 1, maximum: math.MaxInt64, none: true, required: true},
			duration("publish_commit_interval", &s.PublishCommitInterval, time.Millisecond, time.Second, true, true),
			count("publish_commit_max_records", &s.PublishCommitMaxRecords, 1, math.MaxInt, false),
			count("read_connections", &s.ReadConnections, 2, maxReadConnections, false),
			syncInterval,
			{name: "memory_dir", value: &s.MemoryDir, omit: s.Mode != Memory},
			snapshotInterval,
		}},
		{name: "broadcast", children: []field{
			duration("retention_period", &b.Broadcast.RetentionPeriod, time.Second, math.MaxInt64, true, false),
			size("retention_bytes", &b.Broadcast.RetentionBytes, 1, math.MaxInt64, true),
		}},
		{name: "retained", children: []field{
			duration("retention_period", &b.Retained.RetentionPeriod, time.Second, math.MaxInt64, true, false),
			size("max_bytes", &b.Retained.MaxBytes, 1, math.MaxInt64, true),
		}},
		{name: "qos2", children: []field{
			count("max_inflight_per_client", &b.QoS2.MaxInflightPerClient, 1, maxMQTTTwoByteInteger, false),
		}},
		{name: "limits", children: []field{
			size("max_packet_size", &l.MaxPacketSize, 20, maxMQTTPacketSize, false),
			count("max_topic_length", &l.MaxTopicLength, 1, maxMQTTTwoByteInteger, false),
			count("max_client_id_length", &l.MaxClientIDLength, minClientIDLength, maxMQTTTwoByteInteger, false),
			count("max_header_count", &l.MaxHeaderCount, 1, math.MaxInt, false),
			count("max_connections", &l.MaxConnections, 1, math.MaxInt, false),
			count("max_subscriptions", &l.MaxSubscriptions, 1, math.MaxInt, false),
			count("max_topic_levels", &l.MaxTopicLevels, 1, math.MaxInt, false),
			duration("max_session_expiry", &l.MaxSessionExpiry, time.Second, maxMQTTExpiry, false, false),
			duration("max_keepalive", &l.MaxKeepalive, time.Second, maxMQTTTwoByteInteger*time.Second, true, false),
			count("publish_rate", &l.PublishRate, 1, math.MaxInt, true),
			size("publish_bytes", &l.PublishBytes, 1, math.MaxInt64, true),
			duration("write_timeout", &l.WriteTimeout, time.Millisecond, math.MaxInt64, false, true),
			duration("connect_timeout", &l.ConnectTimeout, time.Second, math.MaxInt64, false, false),
			size("max_connect_size", &l.MaxConnectSize, 1, math.MaxInt64, false),
			count("max_connect_rate", &l.MaxConnectRate, 1, math.MaxInt, true),
			count("max_inflight", &l.MaxInflight, 1, math.MaxInt, false),
			size("max_outgoing_memory", &l.MaxOutgoingMemory, 1, math.MaxInt64, false),
			size("max_incoming_memory", &l.MaxIncomingMemory, 1, math.MaxInt64, false),
		}},
	}},
		{name: "channels", value: &c.Channels, omit: len(c.Channels) == 0},
		{name: "bridges", value: &c.Bridges, omit: len(c.Bridges) == 0},
	}
}
