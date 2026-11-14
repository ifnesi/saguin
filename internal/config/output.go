package config

import (
	"slices"

	"go.yaml.in/yaml/v3"
)

// OperationsOutput uses the resolved configuration's one output schema.
// Only the reloadable log and origins fields are overlaid; all other values
// and provenance remain those accepted at startup. The returned tree belongs
// to the HTTP request and contains paths, never loaded credentials or keys.
func (c *Config) OperationsOutput(sections []string) (map[string]any, error) {
	fresh := *c
	if c.Live != nil {
		current := c.Live.origins.Load()
		fresh.Broker.LogLevel = current.Level
		fresh.Broker.MQTT.Listen.WS = slices.Clone(c.Broker.MQTT.Listen.WS)
		for i := range fresh.Broker.MQTT.Listen.WS {
			l := &fresh.Broker.MQTT.Listen.WS[i]
			policy := current.Origins[l.Name]
			l.SameOrigin = policy.SameOrigin
			l.AllowedOrigins = make([]string, len(policy.Allowed))
			for j, origin := range policy.Allowed {
				l.AllowedOrigins[j] = origin.String()
			}
		}
	}
	node, err := outputMapping(schema(&fresh), "")
	if err != nil {
		return nil, err
	}
	var output map[string]any
	if err := node.Decode(&output); err != nil {
		return nil, err
	}
	channels, _ := output["channels"].(map[string]any)
	for i := range c.Channels {
		ch := &c.Channels[i]
		channels[ch.Name].(map[string]any)["configured_in"] = ch.ConfiguredIn
	}
	if len(sections) == 0 {
		return output, nil
	}
	selected := make(map[string]any)
	broker := output["broker"].(map[string]any)
	for _, section := range sections {
		switch section {
		case "channels", "bridges":
			if value, exists := output[section]; exists {
				selected[section] = value
			}
		case "listeners":
			selected[section] = broker["mqtt"].(map[string]any)["listen"]
		default:
			selected[section] = broker[section]
		}
	}
	return selected, nil
}

// outputNamed writes ordered dynamic sections through their entry schemas.
// Channels and bridges share the same name and key-path handling.
func outputNamed[T any](values []T, key string, name func(*T) string, schema func(*T) []field) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for i := range values {
		value := &values[i]
		n := name(value)
		entry, err := outputMapping(schema(value), keyPath(key, n))
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, scalarNode(n, "!!str"), entry)
	}
	return node, nil
}

// outputSequence preserves list order and indexed diagnostics. Listener
// output may unwrap a single entry; bridge topics always remain a list.
func outputSequence[T any](values []T, key string, schema func(*T) []field) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	for i := range values {
		entry, err := outputMapping(schema(&values[i]), indexPath(key, i))
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, entry)
	}
	return node, nil
}
