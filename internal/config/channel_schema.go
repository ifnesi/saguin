package config

import (
	"math"
	"time"

	"go.yaml.in/yaml/v3"
)

// channelSchema exposes only keys belonging to the channel's type, in the
// configuration table's order. Reading and resolved output share this schema.
func channelSchema(c *Channel) []field {
	fields := []field{
		{name: "type", value: &c.Type, required: true},
		{name: "filter", value: &c.Filter},
	}
	if c.Type != Latest {
		fields = append(fields, field{name: "start", value: &c.Start})
	}
	fields = append(fields, duration("retention_period", &c.RetentionPeriod, time.Second, math.MaxInt64, true, false))
	if c.Type != Latest {
		fields = append(fields, size("retention_bytes", &c.RetentionBytes, 1, math.MaxInt64, true))
	}
	switch c.Type {
	case Append:
		fields = append(fields, size("max_bytes", &c.MaxBytes, 1, math.MaxInt64, true))
	case Latest:
		fields = append(fields, duration("tombstone_retention_period", &c.TombstoneRetentionPeriod, time.Second, math.MaxInt64, false, false))
	case Queue:
		fields = append(fields,
			duration("visibility_timeout", &c.VisibilityTimeout, time.Second, math.MaxInt64, false, false),
			field{name: "retry", children: []field{
				count("max_attempts", &c.Retry.MaxAttempts, 1, math.MaxInt, false),
				{name: "mode", value: &c.Retry.Mode},
				duration("delay", &c.Retry.Delay, time.Millisecond, math.MaxInt64, false, true),
			}},
			duration("dlq_retention_period", &c.DLQRetentionPeriod, time.Second, math.MaxInt64, true, false),
			size("dlq_retention_bytes", &c.DLQRetentionBytes, 1, math.MaxInt64, true),
		)
	}
	return fields
}

// channelTypeNode finds type before reading the type-specific schema, so
// its position among the keys cannot change which keys are accepted.
func channelTypeNode(node *yaml.Node) *yaml.Node {
	return mappingValue(node, "type")
}

// mappingValue locates a discriminator before reading its dependent schema.
func mappingValue(node *yaml.Node, name string) *yaml.Node {
	node = valueNode(node)
	if node.Kind == yaml.MappingNode && node.Tag == "!!map" {
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == name {
				return node.Content[i+1]
			}
		}
	}
	return nil
}
