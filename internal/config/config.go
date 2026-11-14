package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Load reads the configuration file once and resolves it as Parse does.
// File errors retain their cause so callers can inspect them with errors.Is.
func Load(path string) (*Config, error) {
	return load(path, false)
}

// LoadAtStart also compares the loader's file uses by filesystem identity,
// before startup writes a pid, database or socket.
func LoadAtStart(path string) (*Config, error) { return load(path, true) }

func load(path string, atStart bool) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read configuration %s: %w", path, err)
	}
	c, err := parse(data, path, atStart)
	if err != nil {
		return nil, fmt.Errorf("configuration %s: %w", path, err)
	}
	return c, nil
}

// Parse reads exactly one YAML mapping, refuses unknown and duplicate keys,
// fills defaults and validates the resolved values before returning them.
func Parse(data []byte) (*Config, error) {
	return parse(data, "", false)
}

// readDocument checks the YAML stream once without expanding its aliases.
// A missing document is left to the caller's required-section diagnostic.
func readDocument(data []byte, key string) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := decoder.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, refuse(key, err.Error())
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, refuse(key, err.Error())
		}
		return nil, refuse(key, "expected one YAML document")
	}
	return doc.Content[0], nil
}

func parse(data []byte, source string, atStart bool) (*Config, error) {
	root, err := readDocument(data, "configuration")
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, refuse("broker.id", "required")
	}
	if err := inspect(root, "", make(map[*yaml.Node]bool), make(map[inspection]bool)); err != nil {
		return nil, err
	}
	c := defaults()
	fields := schema(c)
	channels := channelReader{config: c, source: source}
	// Input needs temporary provenance; output binds the resolved slice.
	findField(fields, "channels").value = &channels
	seen := make(map[string]bool)
	if err := readMapping(root, fields, "", seen); err != nil {
		return nil, err
	}
	if err := required(fields, "", seen); err != nil {
		return nil, err
	}
	c.resolve(seen)
	if err := c.validateStorage(seen); err != nil {
		return nil, err
	}
	if err := channels.resolve(seen); err != nil {
		return nil, err
	}
	if err := c.validateBridges(); err != nil {
		return nil, err
	}
	if err := c.validate(seen); err != nil {
		return nil, err
	}
	if err := c.validateListeners(seen, channels.includes, atStart); err != nil {
		return nil, err
	}
	return c, nil
}

// keyPath names an entry, or its enclosing mapping when it has no key text.
// The root mapping is named configuration in whole-file diagnostics.
func keyPath(parent, name string) string {
	if name == "" {
		if parent == "" {
			return "configuration"
		}
		return parent
	}
	if parent == "" {
		return name
	}
	return parent + "." + name
}

// valueNode follows an ordinary YAML alias to its anchored value. The YAML
// reader binds aliases to existing anchors, not to other alias nodes.
func valueNode(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.AliasNode {
		return node.Alias
	}
	return node
}

// inspection identifies a node in a context where keys can be refused with
// or without replacements.
// All other paths share one context: duplicate keys and cycles depend only
// on the node, not on where an alias uses it.
type inspection struct {
	node    *yaml.Node
	context string
}

func inspectionContext(parent string) string {
	for key := range dropped {
		if strings.HasPrefix(key, parent+".") {
			return parent
		}
	}
	for key := range refusedWithoutReplacement {
		if strings.HasPrefix(key, parent+".") {
			return parent
		}
	}
	for _, kind := range []string{"tcp", "unix"} {
		listener := "broker.operations.listen." + kind
		if strings.HasPrefix(listener, parent+".") {
			return parent
		}
	}
	if operationsAnonymous(parent + ".allow_anonymous") {
		// A listener's index changes its diagnostic path, not its rules.
		context, _, _ := strings.Cut(parent, "[")
		return context
	}
	return ""
}

// inspect preserves duplicate-key diagnostics, dropped-key replacements and
// refusals without replacements even inside the obsolete session and share
// sections. It walks the YAML graph once per inspection context, rather than
// expanding aliases into a tree. Active nodes detect cycles; completed visits
// allow independent reuse.
func inspect(node *yaml.Node, parent string, active map[*yaml.Node]bool, inspected map[inspection]bool) error {
	return inspectIn(node, parent, inspectionContext(parent), active, inspected)
}

func inspectIn(node *yaml.Node, parent, context string, active map[*yaml.Node]bool, inspected map[inspection]bool) error {
	node = valueNode(node)
	if node.Kind != yaml.MappingNode && node.Kind != yaml.SequenceNode {
		return nil
	}
	if active[node] {
		return refuse(parent, "cyclic YAML alias")
	}
	visit := inspection{node: node, context: context}
	if inspected[visit] {
		return nil
	}
	active[node] = true
	defer delete(active, node)
	if node.Kind == yaml.SequenceNode {
		for i, child := range node.Content {
			key := indexPath(parent, i)
			childContext := inspectionContext(key)
			if context == channelsContext {
				childContext = channelGroupContext
			}
			if context == bridgeTopicsContext {
				childContext = bridgeRuleContext
			}
			if err := inspectIn(child, key, childContext, active, inspected); err != nil {
				return err
			}
		}
		inspected[visit] = true
		return nil
	}
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		name, value := node.Content[i], node.Content[i+1]
		// Only a non-null scalar has key text; other nodes name the
		// mapping they stand in, never an alias's anchor or a null token.
		var text string
		if name.Kind == yaml.ScalarNode && name.Tag != "!!null" {
			text = name.Value
		}
		key := keyPath(parent, text)
		if name.Kind != yaml.ScalarNode || name.Tag != "!!str" || name.Value == "" {
			return refuse(key, "expected a string key")
		}
		if seen[name.Value] {
			return refuse(key, "duplicate key")
		}
		seen[name.Value] = true
		if err := inspectChannelKey(context, name.Value, key, node); err != nil {
			return err
		}
		if (context == bridgeContext && (name.Value == "upstream" || name.Value == "inbound")) ||
			(context == bridgeRuleContext && name.Value == "channel") {
			return refuse(key, noReplacementReason)
		}
		if instead, ok := dropped[key]; ok {
			return &Error{Key: key, Instead: instead}
		}
		if refusedWithoutReplacement[key] || operationsAnonymous(key) {
			return refuse(key, noReplacementReason)
		}
		childContext := inspectionContext(key)
		switch {
		case parent == "" && name.Value == "channels":
			childContext = channelsContext
		case parent == "" && name.Value == "bridges":
			childContext = bridgesContext
		case context == bridgesContext:
			childContext = bridgeContext
		case context == bridgeContext && name.Value == "topics":
			childContext = bridgeTopicsContext
		case context == channelsContext || context == channelGroupContext:
			childContext = channelContext
		case context == channelContext && name.Value == "retry":
			if queueMapping(node) {
				childContext = queueRetryContext
			}
		}
		if err := inspectIn(value, key, childContext, active, inspected); err != nil {
			return err
		}
	}
	inspected[visit] = true
	return nil
}

func readMapping(node *yaml.Node, fields []field, parent string, seen map[string]bool) error {
	node = valueNode(node)
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		if parent == "" {
			parent = "configuration"
		}
		return refuse(parent, "expected a mapping")
	}
	for i := 0; i < len(node.Content); i += 2 {
		name, value := node.Content[i].Value, node.Content[i+1]
		key := keyPath(parent, name)
		f := findField(fields, name)
		if f == nil {
			return refuse(key, "unknown key")
		}
		seen[key] = true
		if f.children != nil {
			if err := readMapping(value, f.children, key, seen); err != nil {
				return err
			}
		} else if err := f.read(value, key, seen); err != nil {
			return err
		}
	}
	return nil
}

func findField(fields []field, name string) *field {
	for i := range fields {
		if fields[i].name == name {
			return &fields[i]
		}
	}
	return nil
}

func required(fields []field, parent string, seen map[string]bool) error {
	for i := range fields {
		f := &fields[i]
		key := keyPath(parent, f.name)
		if f.required && !seen[key] {
			return refuse(key, "required")
		}
		if err := required(f.children, key, seen); err != nil {
			return err
		}
	}
	return nil
}

// Marshal writes every resolved key in config.md's row order, with none
// explicit and durations and sizes expressed in the largest whole unit.
func (c *Config) Marshal() ([]byte, error) {
	if c == nil {
		return nil, refuse("configuration", "cannot marshal a nil configuration")
	}
	node, err := outputMapping(schema(c), "")
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(node); err != nil {
		return nil, refuse("configuration", err.Error())
	}
	if err := encoder.Close(); err != nil {
		return nil, refuse("configuration", err.Error())
	}
	return out.Bytes(), nil
}

func outputMapping(fields []field, parent string) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for i := range fields {
		f := &fields[i]
		if f.omit {
			continue
		}
		key := keyPath(parent, f.name)
		var value *yaml.Node
		if f.children != nil {
			child, err := outputMapping(f.children, key)
			if err != nil {
				return nil, err
			}
			value = child
		} else {
			child, err := f.output(key)
			if err != nil {
				return nil, err
			}
			value = child
		}
		node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: f.name}, value)
	}
	return node, nil
}
