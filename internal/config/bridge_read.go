package config

import (
	"crypto/tls"
	"strings"
	"unicode"

	"go.yaml.in/yaml/v3"
)

// readBridges keeps dynamic names and their entries in their written order.
// Peer is checked first so TLS's availability is independent of key order.
func readBridges(node *yaml.Node, dst *[]Bridge, key string, seen map[string]bool) error {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content) == 0 {
		return refuse(key, "expected a nonempty mapping of bridges")
	}
	bridges := make([]Bridge, 0, len(node.Content)/2)
	clients := make(map[bridgeClient]string, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		name := node.Content[i].Value
		parent := keyPath(key, name)
		if name == "" || len(name) > 128 || strings.ContainsFunc(name, unicode.IsSpace) {
			return refuse(parent, "expected 1 to 128 bytes with no whitespace")
		}
		b := defaultBridge(name)
		entry := valueNode(node.Content[i+1])
		if entry.Kind != yaml.MappingNode || entry.Tag != "!!map" {
			return refuse(parent, "expected a mapping")
		}
		peer := mappingValue(entry, "peer")
		if peer == nil {
			return refuse(parent+".peer", "required")
		}
		f := field{name: "peer", value: &b.Peer}
		if err := f.read(peer, parent+".peer", seen); err != nil {
			return err
		}
		resolved, host, port, err := b.parsePeer()
		if err != nil {
			return refuse(parent+".peer", err.Error())
		}
		fields := bridgeSchema(&b)
		if err := readMapping(entry, fields, parent, seen); err != nil {
			return err
		}
		if err := required(fields, parent, seen); err != nil {
			return err
		}
		b.Peer = resolved
		if b.PasswordFile != "" && b.Username == "" {
			return refuse(parent+".password_file", "requires "+parent+".username")
		}
		client := bridgeClient{host: host, port: port, id: b.ClientID}
		if earlier, ok := clients[client]; ok {
			return refuse(parent+".client_id", "same peer and client_id as "+earlier)
		}
		clients[client] = parent
		bridges = append(bridges, b)
	}
	*dst = bridges
	return nil
}

func readBridgeRules(node *yaml.Node, dst *[]BridgeRule, key string, seen map[string]bool) error {
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" || len(node.Content) == 0 {
		return refuse(key, "expected a list of at least one rule")
	}
	rules := make([]BridgeRule, len(node.Content))
	for i, entry := range node.Content {
		parent := indexPath(key, i)
		r := &rules[i]
		r.QoS = 1
		fields := bridgeRuleSchema(r)
		if err := readMapping(entry, fields, parent, seen); err != nil {
			return err
		}
		// Direction may follow topic; apply its presence rule after reading.
		if r.Direction == Both && seen[parent+".topic"] {
			return refuse(parent+".topic", "refused on a both rule")
		}
		if err := required(bridgeRuleSchema(r), parent, seen); err != nil {
			return err
		}
	}
	*dst = rules
	return nil
}

func readBridgeTLS(node *yaml.Node, dst **BridgeTLS, key string, seen map[string]bool) error {
	if isNone(node) {
		*dst = nil
		return nil
	}
	t := &BridgeTLS{MinVersion: tls.VersionTLS12}
	if err := readTLS(node, bridgeTLSSchema(t), key, seen, false); err != nil {
		return err
	}
	*dst = t
	return nil
}
