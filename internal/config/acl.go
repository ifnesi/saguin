package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/topic"
)

// LoadACL reads and validates the whole access file without touching broker
// state. Startup and --check-config call this same loader. Independent entry
// failures are accumulated so a bad rule does not conceal a bad user.
func (c *Config) LoadACL() error {
	path := c.Broker.MQTT.ACLFile
	if path == "" {
		return nil
	}
	file, err := c.readACL(path)
	if err != nil {
		return fmt.Errorf("acl_file %s: %w", path, err)
	}
	c.ACL = file
	return nil
}

func (c *Config) readACL(path string) (*acl.File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return c.ParseACL(data, path)
}

// ParseACL validates access-file bytes with the same rules as startup.
func (c *Config) ParseACL(data []byte, path string) (*acl.File, error) {
	root, err := readDocument(data, "acl_file")
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, refuse("acl_file", "expected roles or users")
	}
	if err := inspect(root, "acl_file", make(map[*yaml.Node]bool), make(map[inspection]bool)); err != nil {
		return nil, err
	}
	var rolesNode, usersNode *yaml.Node
	err = aclMapping(root, "acl_file", func(key string, value *yaml.Node) error {
		switch key {
		case "roles":
			rolesNode = value
		case "users":
			usersNode = value
		default:
			return refuse(key, "unknown key")
		}
		return nil
	})
	findings := []error{err}
	if rolesNode == nil && usersNode == nil {
		findings = append(findings, refuse("acl_file", "expected roles or users"))
	}
	routing := NewRouting(c.Channels)
	roles := make(map[string]bool)
	var rules []acl.Rule
	if rolesNode != nil {
		findings = append(findings, aclMapping(rolesNode, "roles", func(role string, value *yaml.Node) error {
			roles[role] = true
			value = valueNode(value)
			key := "roles." + role
			if value.Kind != yaml.SequenceNode || len(value.Content) == 0 {
				return refuse(key, "expected at least one rule")
			}
			var failures []error
			for i, node := range value.Content {
				rule, err := c.aclRule(node, role, indexPath(key, i), routing)
				if err != nil {
					failures = append(failures, err)
				} else {
					rules = append(rules, rule)
				}
			}
			return errors.Join(failures...)
		}))
	}
	var entries []acl.Entry
	if usersNode != nil {
		findings = append(findings, aclMapping(usersNode, "users", func(pattern string, value *yaml.Node) error {
			entry, err := readACLEntry(value, pattern, roles)
			if err == nil {
				entries = append(entries, entry)
			}
			return err
		}))
	}
	file, err := acl.New(path, rules, entries)
	if err == nil {
		channels := make([]string, len(routing.Channels))
		for i := range routing.Channels {
			channels[i] = routing.Channels[i].Name
		}
		file.SetRouting(routing.AccessRoutes(), channels)
	}
	findings = append(findings, err)
	return file, errors.Join(findings...)
}

// aclMapping visits already inspected mapping entries in file order and
// reports independent errors together. Alias cycles and duplicate keys have
// been checked by the same graph inspection as the main configuration.
func aclMapping(node *yaml.Node, key string, visit func(string, *yaml.Node) error) error {
	node = valueNode(node)
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return refuse(key, "expected a mapping")
	}
	var findings []error
	for i := 0; i < len(node.Content); i += 2 {
		if err := visit(node.Content[i].Value, valueNode(node.Content[i+1])); err != nil {
			findings = append(findings, err)
		}
	}
	return errors.Join(findings...)
}

func aclString(node *yaml.Node, key string) (string, error) {
	node = valueNode(node)
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" || node.Value == "" {
		return "", refuse(key, "expected a nonempty string")
	}
	return node.Value, nil
}

func aclStrings(node *yaml.Node, key string, scalar bool) ([]string, error) {
	node = valueNode(node)
	if scalar && node.Kind == yaml.ScalarNode {
		value, err := aclString(node, key)
		return []string{value}, err
	}
	if node.Kind != yaml.SequenceNode || len(node.Content) == 0 {
		return nil, refuse(key, "expected a nonempty list")
	}
	values := make([]string, 0, len(node.Content))
	var findings []error
	for i, child := range node.Content {
		value, err := aclString(child, indexPath(key, i))
		if err != nil {
			findings = append(findings, err)
		} else {
			values = append(values, value)
		}
	}
	return values, errors.Join(findings...)
}

func (c *Config) aclRule(node *yaml.Node, role, key string, routing *Routing) (acl.Rule, error) {
	r := acl.Rule{Role: role}
	subjects := 0
	var allow, deny, filter bool
	err := aclMapping(node, key, func(name string, value *yaml.Node) (err error) {
		path := key + "." + name
		switch name {
		case "channel", "topic", "broker":
			subjects++
			r.Kind = name
			r.Subject, err = aclString(value, path)
		case "filter":
			filter = true
			r.Filter, err = aclString(value, path)
		case "allow":
			allow = true
			r.Verbs, err = aclStrings(value, path, false)
		case "deny":
			deny = true
			r.Denies, err = aclStrings(value, path, false)
		default:
			err = refuse(path, "unknown key")
		}
		return err
	})
	findings := []error{err}
	if subjects != 1 {
		findings = append(findings, refuse(key, "expected exactly one of channel, topic and broker"))
		return r, errors.Join(findings...)
	}
	if filter && r.Kind != "channel" {
		findings = append(findings, refuse(key+".filter", "filter is allowed only on a channel rule"))
	}
	features := r.Kind == "broker" && r.Subject == "features"
	if features {
		if allow {
			findings = append(findings, refuse(key+".allow", "allow is refused on features"))
		}
		findings = append(findings, aclChoices(r.Denies, []string{"persistent", "will", "share", "retained", "qos2"}, key+".deny"))
	} else {
		if deny {
			findings = append(findings, refuse(key+".deny", "deny is allowed only on features"))
		}
		choices, err := c.aclSubject(&r, key, routing)
		findings = append(findings, err, aclChoices(r.Verbs, choices, key+".allow"))
	}
	return r, errors.Join(findings...)
}

func aclChoices(values, choices []string, key string) error {
	if len(values) == 0 {
		return refuse(key, "expected at least one verb or feature")
	}
	seen := make(map[string]bool, len(values))
	var findings []error
	for _, value := range values {
		if !slices.Contains(choices, value) {
			findings = append(findings, refuse(key, "unsupported verb or feature "+value))
		} else if seen[value] {
			findings = append(findings, refuse(key, "duplicate verb or feature "+value))
		}
		seen[value] = true
	}
	return errors.Join(findings...)
}

func (c *Config) aclSubject(r *acl.Rule, key string, routing *Routing) ([]string, error) {
	switch r.Kind {
	case "broker":
		if r.Subject != "sessions" {
			return nil, refuse(key+".broker", "unknown broker subject "+r.Subject)
		}
		return []string{"disconnect"}, nil
	case "topic":
		// A topic rule is an ordinary MQTT filter. Alternatives belong to
		// a channel rule's optional filter; braces here are literal text.
		limits := topic.Limits{MaxLength: c.Broker.Limits.MaxTopicLength, MaxLevels: c.Broker.Limits.MaxTopicLevels}
		err := topic.CheckFilter(r.Subject, limits)
		if strings.ContainsRune(r.Subject, '\x00') {
			err = topic.ErrMalformed
		}
		if err != nil {
			return nil, refuse(key+".topic", r.Subject+": "+err.Error())
		}
		if !slices.Contains(routing.Reaches(r.Subject), -1) {
			return nil, refuse(key+".topic", r.Subject+": lies inside a configured channel")
		}
		r.Filters = []string{r.Subject}
		return []string{"write", "read"}, nil
	}
	if strings.Contains(r.Subject, "%u") || strings.Contains(r.Subject, "%c") {
		return nil, refuse(key+".channel", "substitution is refused in channel "+r.Subject)
	}
	index := slices.IndexFunc(routing.Channels, func(ch Channel) bool { return ch.Name == r.Subject })
	if index < 0 {
		return nil, refuse(key+".channel", "channel is not configured: "+r.Subject)
	}
	ch := &routing.Channels[index]
	r.Filters = ch.Filters
	if r.Filter != "" {
		filters, err := expandFilter(r.Filter, &c.Broker.Limits, nil)
		if err != nil {
			return nil, refuse(key+".filter", r.Filter+": "+err.Error())
		}
		overlaps := false
		for _, a := range filters {
			for _, b := range ch.Filters {
				overlaps = overlaps || topic.FiltersOverlap(a, b)
			}
		}
		if !overlaps {
			return nil, refuse(key+".filter", r.Filter+": matches none of channel "+ch.Name)
		}
		r.Filters = filters
	}
	if strings.HasSuffix(ch.Name, "__dlq") {
		return []string{"read", "seek"}, nil
	}
	switch ch.Type {
	case Latest:
		return []string{"write", "read"}, nil
	case Queue:
		return []string{"write", "read", "seek", "consume"}, nil
	default:
		return []string{"write", "read", "seek"}, nil
	}
}

func readACLEntry(node *yaml.Node, pattern string, roles map[string]bool) (acl.Entry, error) {
	e := acl.Entry{Pattern: pattern}
	key := "users." + pattern
	var err error
	if node.Kind == yaml.SequenceNode {
		e.Roles, err = aclStrings(node, key, false)
	} else {
		err = aclMapping(node, key, func(name string, value *yaml.Node) (err error) {
			path := key + "." + name
			switch name {
			case "roles":
				e.Roles, err = aclStrings(value, path, false)
			case "client_ids":
				e.ClientIDs, err = aclStrings(value, path, true)
			case "limits":
				err = aclMapping(value, path, func(limit string, number *yaml.Node) error {
					bytes := Size(e.PublishBytes)
					fields := []field{count("publish_rate", &e.PublishRate, 1, math.MaxInt, false), size("publish_bytes", &bytes, 1, math.MaxInt64, false)}
					f := findField(fields, limit)
					if f == nil {
						return refuse(path+"."+limit, "unknown key")
					}
					err := f.read(number, path+"."+limit, nil)
					e.PublishBytes = int64(bytes)
					return err
				})
			default:
				err = refuse(path, "unknown key")
			}
			return err
		})
	}
	findings := []error{err}
	if len(e.Roles) == 0 {
		findings = append(findings, refuse(key+".roles", "expected at least one role"))
	}
	for _, role := range e.Roles {
		if !roles[role] {
			findings = append(findings, refuse(key+".roles", "undefined role "+role))
		}
	}
	for _, id := range e.ClientIDs {
		if strings.Contains(id, "%c") {
			findings = append(findings, refuse(key+".client_ids", "%c is refused"))
		}
	}
	return e, errors.Join(findings...)
}
