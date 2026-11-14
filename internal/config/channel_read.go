package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

var channelName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// channelReader keeps diagnostic paths and include uses during loading only.
// The resolved Config contains the channels themselves, without parser state.
type channelReader struct {
	config   *Config
	source   string
	keys     []string
	includes []pathRef
	names    map[string]string
}

func (r *channelReader) read(node *yaml.Node, key string, seen map[string]bool) error {
	r.names = make(map[string]string)
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return r.readGroup(node, key, r.source, seen)
	}
	if len(node.Content) == 0 {
		return refuse(key, "expected at least one group")
	}
	for i, entry := range node.Content {
		entry = valueNode(entry)
		parent := indexPath(key, i)
		if entry.Tag == "!include" {
			if err := r.readInclude(entry, parent, seen); err != nil {
				return err
			}
		} else if err := r.readGroup(entry, parent, r.source, seen); err != nil {
			return err
		}
	}
	return nil
}

func (r *channelReader) readInclude(node *yaml.Node, key string, seen map[string]bool) error {
	path := node.Value
	if node.Kind != yaml.ScalarNode || !filepath.IsAbs(path) || strings.ContainsAny(path, "*?[\x00") {
		return refuse(key, "expected !include with one absolute file path without wildcards")
	}
	r.includes = append(r.includes, pathRef{key: key, value: path})
	// Stat follows symlinks, accepting links to regular files while refusing
	// devices, pipes and sockets before opening anything that could block.
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: %w", refuse(key, "cannot inspect included file"), err)
	}
	if !info.Mode().IsRegular() {
		return inFile(path, refuse(key, "expected a regular included file"))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%w: %w", refuse(key, "cannot read included file"), err)
	}
	root, err := readDocument(data, key)
	if err == nil && root == nil {
		err = refuse(key, "expected a nonempty group")
	}
	if err == nil {
		err = inspectIn(root, key, channelGroupContext, make(map[*yaml.Node]bool), make(map[inspection]bool))
	}
	if err == nil {
		err = r.readGroup(root, key, path, seen)
	}
	return inFile(path, err)
}

func (r *channelReader) readGroup(node *yaml.Node, key, source string, seen map[string]bool) error {
	node = valueNode(node)
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content) == 0 {
		return refuse(key, "expected a nonempty group of channels")
	}
	for i := 0; i < len(node.Content); i += 2 {
		name := node.Content[i].Value
		parent := keyPath(key, name)
		if !channelName.MatchString(name) || name == "." || name == ".." || strings.HasSuffix(name, "__dlq") {
			return refuse(parent, "expected 1 to 128 ASCII letters, digits, '.', '_' or '-', except '.', '..' or a name ending in __dlq")
		}
		if earlier, ok := r.names[name]; ok {
			return refuse(parent, "already defined at "+earlier)
		}
		entry := valueNode(node.Content[i+1])
		if entry.Kind != yaml.MappingNode || entry.Tag != "!!map" {
			return refuse(parent, "expected a channel mapping")
		}
		typeNode := channelTypeNode(entry)
		if typeNode == nil {
			return refuse(parent+".type", "required")
		}
		var kind ChannelType
		f := field{name: "type", value: &kind}
		if err := f.read(typeNode, parent+".type", seen); err != nil {
			return err
		}
		c := defaultChannel(name, kind)
		c.ConfiguredIn = source
		if err := readMapping(entry, channelSchema(&c), parent, seen); err != nil {
			return err
		}
		r.names[name] = parent
		r.keys = append(r.keys, parent)
		r.config.Channels = append(r.config.Channels, c)
	}
	return nil
}

// inFile adds provenance while preserving the refused key's Error for
// errors.As and its Dropped replacement. Main-file Parse errors need no file.
func inFile(path string, err error) error {
	if path == "" || err == nil {
		return err
	}
	return fmt.Errorf("%s: %w", path, err)
}
