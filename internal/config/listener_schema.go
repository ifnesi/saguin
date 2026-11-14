package config

import (
	"math"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

func operationsSchema(o *Operations) []field {
	return []field{
		{name: "listen", value: &o.Listen, required: true},
		{name: "password_file", value: &o.PasswordFile, none: true},
		duration("min_scrape_interval", &o.MinScrapeInterval, minScrapeInterval, math.MaxInt64, false, false),
	}
}

func listenSchema(l *Listen, operations bool) []field {
	fields := []field{{name: "tcp", value: &l.TCP, omit: len(l.TCP) == 0}}
	if !operations {
		fields = append(fields, field{name: "ws", value: &l.WS, omit: len(l.WS) == 0})
	}
	return append(fields, field{name: "unix", value: &l.Unix, omit: len(l.Unix) == 0})
}

// listenerSchema exposes only keys belonging to the transport and section.
// Values that cannot be written back (an inherited absent password file)
// are omitted from output so Parse can supply them again.
func listenerSchema(l *Listener, kind string, operations bool) []field {
	fields := []field{{name: "name", value: &l.Name}}
	if kind == "unix" {
		fields = append(fields,
			field{name: "path", value: &l.Path, required: true},
			field{name: "mode", value: &l.Mode},
			field{name: "proxy_protocol", value: &l.ProxyProtocol},
		)
	} else {
		fields = append(fields, field{name: "address", value: &l.Address, required: true})
	}
	fields = append(fields, field{name: "password_file", value: &l.PasswordFile, omit: l.PasswordFile == ""})
	if !operations {
		fields = append(fields, field{name: "allow_anonymous", value: &l.AllowAnonymous})
	}
	if kind != "unix" {
		fields = append(fields, field{name: "tls", value: &l.TLS})
	}
	if kind == "ws" {
		fields = append(fields,
			field{name: "same_origin", value: &l.SameOrigin},
			field{name: "allowed_origins", value: &l.AllowedOrigins},
		)
	}
	return fields
}

// tlsSchema is the certificate and protocol subset shared by listeners
// and peers. Absent optional files are omitted so output remains readable.
func tlsSchema(certFile, keyFile *string, minVersion *uint16) []field {
	return []field{
		{name: "cert_file", value: certFile, omit: *certFile == ""},
		{name: "key_file", value: keyFile, omit: *keyFile == ""},
		{name: "min_version", value: minVersion},
	}
}

func listenerTLSSchema(t *TLS) []field {
	return append(tlsSchema(&t.CertFile, &t.KeyFile, &t.MinVersion),
		field{name: "client_ca_file", value: &t.ClientCAFile, omit: t.ClientCAFile == ""},
		field{name: "require_certificate", value: &t.RequireCertificate, omit: t.ClientCAFile == ""},
	)
}

func indexPath(parent string, index int) string {
	return parent + "[" + strconv.Itoa(index) + "]"
}

// operationsAnonymous recognizes allow_anonymous, refused with no replacement,
// both in a single listener and in a list entry, retaining the original
// indexed path in the error.
func operationsAnonymous(key string) bool {
	for _, kind := range []string{"tcp", "unix"} {
		rest, ok := strings.CutPrefix(key, "broker.operations.listen."+kind)
		if !ok {
			continue
		}
		if strings.HasPrefix(rest, "[") {
			_, rest, _ = strings.Cut(rest, "]")
		}
		if rest == ".allow_anonymous" {
			return true
		}
	}
	return false
}

func isNone(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!str" && node.Value == "none"
}

// readListeners handles either one mapping or an ordered list. The seen
// entry for each list element preserves the file's indexed diagnostic path.
func readListeners(node *yaml.Node, dst *[]Listener, kind, key string, seen map[string]bool) error {
	nodes := []*yaml.Node{node}
	indexed := node.Kind == yaml.SequenceNode
	if indexed {
		nodes = node.Content
	}
	if len(nodes) == 0 {
		return refuse(key, "expected at least one listener")
	}
	listeners := make([]Listener, len(nodes))
	for i, entry := range nodes {
		parent := key
		if indexed {
			parent = indexPath(key, i)
			seen[parent] = true
		}
		l := &listeners[i]
		*l = defaultListener(kind, "")
		fields := listenerSchema(l, kind, strings.HasPrefix(key, "broker.operations."))
		if err := readMapping(entry, fields, parent, seen); err != nil {
			return err
		}
		if err := required(fields, parent, seen); err != nil {
			return err
		}
		if len(nodes) > 1 && !seen[parent+".name"] {
			return refuse(parent+".name", "required when a kind has two or more listeners")
		}
	}
	*dst = listeners
	return nil
}

func readListenerTLS(node *yaml.Node, dst **TLS, key string, seen map[string]bool) error {
	if isNone(node) {
		*dst = nil
		return nil
	}
	t := defaultTLS()
	if err := readTLS(node, listenerTLSSchema(t), key, seen, true); err != nil {
		return err
	}
	if t.ClientCAFile == "" && seen[key+".require_certificate"] {
		return refuse(key+".require_certificate", "requires "+key+".client_ca_file")
	}
	if t.ClientCAFile != "" && !seen[key+".require_certificate"] {
		t.RequireCertificate = true
	}
	*dst = t
	return nil
}

// readTLS reads either TLS block through its schema, then applies the
// common certificate-pair rule. A listener always needs that pair; a peer
// may use system roots without presenting a client certificate.
func readTLS(node *yaml.Node, fields []field, key string, seen map[string]bool, certificateRequired bool) error {
	if err := readMapping(node, fields, key, seen); err != nil {
		return err
	}
	if len(node.Content) == 0 && !certificateRequired {
		return refuse(key, "expected a nonempty TLS block")
	}
	cert := seen[key+".cert_file"]
	privateKey := seen[key+".key_file"]
	if !cert && (certificateRequired || privateKey) {
		return refuse(key+".cert_file", "required with "+key+".key_file")
	}
	if !privateKey && (certificateRequired || cert) {
		return refuse(key+".key_file", "required with "+key+".cert_file")
	}
	return nil
}

func (f *field) output(key string) (*yaml.Node, error) {
	switch value := f.value.(type) {
	case *[]Bridge:
		return outputNamed(*value, key, func(b *Bridge) string { return b.Name }, bridgeSchema)
	case *[]BridgeRule:
		return outputSequence(*value, key, bridgeRuleSchema)
	case **BridgeTLS:
		if *value != nil {
			return outputMapping(bridgeTLSSchema(*value), key)
		}
		return scalarNode("none", "!!str"), nil
	case *[]Channel:
		return outputNamed(*value, key, func(c *Channel) string { return c.Name }, channelSchema)
	case *Listen:
		return outputMapping(listenSchema(value, key == "broker.operations.listen"), key)
	case **Operations:
		return outputMapping(operationsSchema(*value), key)
	case *[]Listener:
		return outputListeners(*value, f.name, key)
	case **TLS:
		if *value != nil {
			return outputMapping(listenerTLSSchema(*value), key)
		}
		return scalarNode("none", "!!str"), nil
	case *[]string:
		if len(*value) == 0 {
			return scalarNode("none", "!!str"), nil
		}
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, origin := range *value {
			node.Content = append(node.Content, scalarNode(origin, "!!str"))
		}
		return node, nil
	}
	text, err := f.text()
	if err != nil {
		return nil, refuse(key, err.Error())
	}
	tag := "!!str"
	switch f.value.(type) {
	case *int, *byte:
		if text != "none" {
			tag = "!!int"
		}
	case *bool:
		tag = "!!bool"
	}
	return scalarNode(text, tag), nil
}

func scalarNode(value, tag string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value}
}

func outputListeners(listeners []Listener, kind, key string) (*yaml.Node, error) {
	node, err := outputSequence(listeners, key, func(l *Listener) []field {
		return listenerSchema(l, kind, strings.HasPrefix(key, "broker.operations."))
	})
	if err != nil {
		return nil, err
	}
	if len(node.Content) == 1 {
		return node.Content[0], nil
	}
	return node, nil
}
