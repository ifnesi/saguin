package config

import "go.yaml.in/yaml/v3"

// Dynamic names and list indexes share these inspection contexts. Descendants
// of unknown keys return to the ordinary context, keeping aliased graphs
// linear in their nodes rather than in the number of possible paths.
const (
	channelsContext     = "channels"
	channelGroupContext = "channel group"
	channelContext      = "channel"
	queueRetryContext   = "queue retry"
	bridgesContext      = "bridges"
	bridgeContext       = "bridge"
	bridgeTopicsContext = "bridge topics"
	bridgeRuleContext   = "bridge rule"
)

func queueMapping(node *yaml.Node) bool {
	if kind := channelTypeNode(node); kind != nil {
		kind = valueNode(kind)
		return kind.Kind == yaml.ScalarNode && kind.Tag == "!!str" && kind.Value == "queue"
	}
	return false
}

// inspectChannelKey preserves the Dropped rows before a type-specific schema
// can reject the enclosing key or a key belonging to a former schema.
func inspectChannelKey(context, name, key string, mapping *yaml.Node) error {
	if context == queueRetryContext && (name == "backoff" || name == "backoff_base") {
		return &Error{Key: key, Instead: "retry.mode, retry.delay"}
	}
	if context != channelContext {
		return nil
	}
	switch name {
	case "deletion_retention_period":
		return &Error{Key: key, Instead: "tombstone_retention_period"}
	case "storage":
		return &Error{Key: key, Instead: theStore}
	case "bridge_source", "dlq_bridge_source":
		return refuse(key, noReplacementReason)
	case "max_bytes", "job_expires_after":
		if queueMapping(mapping) {
			return &Error{Key: key, Instead: "its retention_bytes, retention_period"}
		}
	}
	return nil
}
