package config

import "path/filepath"

// listenerRef keeps a resolved listener together with its original file
// path. Metadata stays outside Config so only effective values are returned.
type listenerRef struct {
	listener *Listener
	key      string
	section  string
}

func listenerRefs(listen *Listen, section string, seen map[string]bool) []listenerRef {
	refs := make([]listenerRef, 0, len(listen.TCP)+len(listen.WS)+len(listen.Unix))
	for _, group := range []struct {
		kind string
		list []Listener
	}{{"tcp", listen.TCP}, {"ws", listen.WS}, {"unix", listen.Unix}} {
		parent := section + ".listen." + group.kind
		indexed := seen[indexPath(parent, 0)]
		for i := range group.list {
			key := parent
			if indexed {
				key = indexPath(parent, i)
			}
			refs = append(refs, listenerRef{listener: &group.list[i], key: key, section: section})
		}
	}
	return refs
}

// source names the supplied key for an inherited setting. Every listener
// password file that is explicitly written is nonempty, so the fallback is
// always the section's file; no listener can override it with none.
func (r listenerRef) source(name string, seen map[string]bool) string {
	key := r.key + "." + name
	if seen[key] {
		return key
	}
	return r.section + "." + name
}

func (c *Config) resolveListeners(seen map[string]bool) {
	m := &c.Broker.MQTT
	if !seen["broker.mqtt.allow_anonymous"] {
		m.AllowAnonymous = m.PasswordFile == ""
	}
	for _, ref := range listenerRefs(&m.Listen, "broker.mqtt", seen) {
		l := ref.listener
		ownPassword := seen[ref.key+".password_file"]
		if !ownPassword {
			l.PasswordFile = m.PasswordFile
		}
		if !seen[ref.key+".allow_anonymous"] {
			l.AllowAnonymous = m.AllowAnonymous && !ownPassword
		}
	}
	if o := c.Broker.Operations; o != nil {
		for _, ref := range listenerRefs(&o.Listen, "broker.operations", seen) {
			if !seen[ref.key+".password_file"] {
				ref.listener.PasswordFile = o.PasswordFile
			}
		}
	}
}

func (c *Config) validateListeners(seen map[string]bool, includes []pathRef, atStart bool) error {
	mqtt := listenerRefs(&c.Broker.MQTT.Listen, "broker.mqtt", seen)
	if err := validateNames(mqtt); err != nil {
		return err
	}
	if err := c.validateAnonymous(mqtt, seen); err != nil {
		return err
	}
	if err := c.validateIdentity(mqtt, seen); err != nil {
		return err
	}
	var operations []listenerRef
	if o := c.Broker.Operations; o != nil {
		operations = listenerRefs(&o.Listen, "broker.operations", seen)
		if err := validateNames(operations); err != nil {
			return err
		}
		if err := c.validateOperators(mqtt, operations, seen); err != nil {
			return err
		}
	}
	if atStart {
		if err := c.ResolveStoragePaths(); err != nil {
			return err
		}
	}
	paths := append(c.paths(mqtt, operations, seen), includes...)
	if err := validatePaths(paths); err != nil {
		return err
	}
	if atStart {
		if err := compareFiles(paths); err != nil {
			return err
		}
	}
	return validateBindings(append(mqtt, operations...))
}

func validateNames(refs []listenerRef) error {
	names := make(map[string]string, len(refs))
	for _, ref := range refs {
		key := ref.key + ".name"
		if earlier, ok := names[ref.listener.Name]; ok {
			return refuse(key, "must differ from "+earlier)
		}
		names[ref.listener.Name] = key
	}
	return nil
}

// HasIdentity reports whether the listener has a configured identity source.
func (l *Listener) HasIdentity() bool {
	return l.PasswordFile != "" || l.ProxyProtocol || (l.TLS != nil && l.TLS.ClientCAFile != "")
}

// validateAnonymous applies the ACL's anonymous-client rule to the broker
// and every listener, naming the supplied key when a value is inherited.
func (c *Config) validateAnonymous(refs []listenerRef, seen map[string]bool) error {
	m := &c.Broker.MQTT
	if m.ACLFile == "" {
		return nil
	}
	if m.AllowAnonymous {
		return refuse("broker.mqtt.allow_anonymous", "true is refused beside broker.mqtt.acl_file")
	}
	for _, ref := range refs {
		if ref.listener.AllowAnonymous {
			return refuse(ref.source("allow_anonymous", seen), ref.key+": true is refused beside broker.mqtt.acl_file")
		}
	}
	return nil
}

func (c *Config) validateIdentity(refs []listenerRef, seen map[string]bool) error {
	m := &c.Broker.MQTT
	identity := false
	for _, ref := range refs {
		l := ref.listener
		if l.HasIdentity() {
			identity = true
		} else if !l.AllowAnonymous {
			return refuse(ref.source("allow_anonymous", seen), ref.key+" needs an identity source when allow_anonymous is false")
		}
	}
	if m.ACLFile != "" && !identity {
		return refuse("broker.mqtt.acl_file", "needs an MQTT listener with an identity source")
	}
	return nil
}

func (c *Config) validateOperators(mqtt, operations []listenerRef, seen map[string]bool) error {
	clients := make(map[string]string, len(mqtt)+1)
	if file := c.Broker.MQTT.PasswordFile; file != "" {
		clients[filepath.Clean(file)] = "broker.mqtt.password_file"
	}
	for _, ref := range mqtt {
		if file := ref.listener.PasswordFile; file != "" {
			path := filepath.Clean(file)
			if _, ok := clients[path]; !ok {
				clients[path] = ref.source("password_file", seen)
			}
		}
	}
	if file := c.Broker.Operations.PasswordFile; file != "" {
		if earlier, ok := clients[filepath.Clean(file)]; ok {
			return refuse("broker.operations.password_file", "must differ from "+earlier)
		}
	}
	for _, ref := range operations {
		l := ref.listener
		if l.PasswordFile != "" {
			if earlier, ok := clients[filepath.Clean(l.PasswordFile)]; ok {
				return refuse(ref.source("password_file", seen), ref.key+" must use a different file from "+earlier)
			}
		}
		if l.Address != "" {
			host, _, err := splitAddress(l.Address)
			if err != nil {
				return refuse(ref.key+".address", err.Error())
			}
			_, _, loopback := canonicalHost(host)
			requiredCertificate := l.TLS != nil && l.TLS.RequireCertificate
			if !loopback && l.PasswordFile == "" && !requiredCertificate {
				return refuse(ref.key+".address", "a non-loopback address needs a password_file or a required client certificate")
			}
		}
	}
	return nil
}

type portBindings struct {
	first    string
	wildcard string
	hosts    map[string]string
}

// validateBindings visits the canonical section and kind order. Port zero
// chooses a port at runtime and is excluded from configured port clashes.
func validateBindings(refs []listenerRef) error {
	ports := make(map[uint16]*portBindings)
	for _, ref := range refs {
		l := ref.listener
		if l.Path != "" {
			continue
		}
		host, port, err := splitAddress(l.Address)
		if err != nil {
			return refuse(ref.key+".address", err.Error())
		}
		if port == 0 {
			continue
		}
		host, wildcard, _ := canonicalHost(host)
		key := ref.key + ".address"
		bindings := ports[port]
		if bindings == nil {
			bindings = &portBindings{first: key, hosts: make(map[string]string)}
			ports[port] = bindings
		} else if wildcard {
			return refuse(key, "port clashes with "+bindings.first)
		} else if bindings.wildcard != "" {
			return refuse(key, "port clashes with "+bindings.wildcard)
		} else if earlier, ok := bindings.hosts[host]; ok {
			return refuse(key, "port clashes with "+earlier)
		}
		if wildcard {
			bindings.wildcard = key
		}
		bindings.hosts[host] = key
	}
	return nil
}
