package config

// Error identifies a refused key and, for a dropped key, its replacement
// in Instead. Refusals without a replacement carry their reason separately.
// Key is the full path in the file; malformed YAML uses "configuration"
// because no reliable key path can be recovered from its syntax.
type Error struct {
	Key     string
	Instead string
	reason  string
}

func (e *Error) Error() string {
	if e.Instead != "" {
		return e.Key + ": dropped; instead use " + e.Instead
	}
	return e.Key + ": " + e.reason
}

func refuse(key, reason string) *Error {
	return &Error{Key: key, reason: reason}
}

const (
	oneStore            = "broker.storage: one SQLite store, mode"
	theStore            = "the one store"
	noReplacementReason = "refused, with no replacement"
)

// dropped lists keys with replacements.
// Dynamic channel keys are recognized by inspectChannelKey; dynamic bridge
// refusals are recognized in their inspection contexts by inspectIn.
var dropped = map[string]string{
	"broker.limits.max_message_size":     "limits.max_packet_size",
	"broker.limits.max_header_bytes":     "none: limits.max_packet_size bounds a publish's User Properties",
	"broker.storage.default":             oneStore,
	"broker.storage.providers":           oneStore,
	"broker.storage.type":                oneStore,
	"broker.storage.snapshot_dir":        oneStore,
	"broker.retained.storage":            theStore,
	"broker.session.storage":             theStore,
	"broker.share.storage":               theStore,
	"broker.qos2.storage":                theStore,
	"broker.storage.flush_interval":      "Syncing follows the storage mode (Store).",
	"broker.storage.max_bytes":           "none: each channel's own limits, and the disk (Store)",
	"broker.limits.session_queue_bytes":  "broker.broadcast retention",
	"broker.limits.session_queue_full":   "broker.broadcast retention",
	"broker.qos2.expires_after":          "a held QoS 2 message stays until PUBREL or its session ends",
	"broker.share.expires_after":         "the channel's retention",
	"broker.session.ack_commit_interval": "the next commit (Delivery)",
}

// refusedWithoutReplacement lists keys refused with no replacement.
// An operations listener's allow_anonymous is recognized by inspect,
// including indexed paths.
var refusedWithoutReplacement = map[string]bool{
	"broker.storage.commit_interval":    true,
	"broker.storage.commit_max_records": true,
	"broker.retained.retention_bytes":   true,
	"broker.session.max_bytes":          true,
	"broker.qos2.max_bytes":             true,
}
