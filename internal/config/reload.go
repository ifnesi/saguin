package config

import (
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/passwd"
)

// AccessCopy is one immutable generation of credentials and authorization.
// Owners may cache its compiled grants, but never change the published snapshot.
type AccessCopy struct {
	Passwords map[string]*passwd.File
	ACL       *acl.File
}

// Origins is the already parsed WebSocket policy for one listener.
type Origins struct {
	SameOrigin bool
	Allowed    []*url.URL
}

// LogAndOrigins is published as a whole, independently of access and TLS.
type LogAndOrigins struct {
	Level   logger.Level
	Origins map[string]Origins
}

// TLSCopy holds one immutable generation of listener and bridge TLS material.
type TLSCopy struct {
	mqtt, operations, bridges map[string]*tls.Config
	certificates              []CertificateExpiry
}

// CertificateExpiry describes the identity actually loaded for a listener or
// bridge. Reload diagnostics use this material, never a second file read.
type CertificateExpiry struct {
	Owner, File string
	Expires     time.Time
}

func (s *TLSCopy) certificate(owner, file string, secure *tls.Config) {
	if secure != nil && len(secure.Certificates) != 0 {
		s.certificates = append(s.certificates, CertificateExpiry{
			Owner: owner, File: file, Expires: secure.Certificates[0].Leaf.NotAfter,
		})
	}
}

// Certificates returns immutable expiry observations in configuration order.
func (s *TLSCopy) Certificates() []CertificateExpiry { return s.certificates }

// Reloads holds only immutable copies. After initialization, the core alone
// publishes main's completed copies as it applies reload events.
type Reloads struct {
	access  atomic.Pointer[AccessCopy]
	origins atomic.Pointer[LogAndOrigins]
	tls     atomic.Pointer[TLSCopy]
}

// TLSCertificates returns the identity observations of the current generation.
func (r *Reloads) TLSCertificates() []CertificateExpiry { return r.tls.Load().Certificates() }

// InitReloads prepares the initial generations before any goroutine reads them.
func (c *Config) InitReloads() error {
	origins, err := c.logAndOrigins()
	if err != nil {
		return err
	}
	secure, err := c.loadTLS()
	if err != nil {
		return err
	}
	live := new(Reloads)
	live.access.Store(&AccessCopy{Passwords: c.Passwords, ACL: c.ACL})
	live.origins.Store(origins)
	live.tls.Store(secure)
	c.Live = live
	return nil
}

// Access returns the current whole access generation without allocating.
func (r *Reloads) Access() *AccessCopy { return r.access.Load() }

// Origins returns a listener's current origin policy with one atomic load.
func (r *Reloads) Origins(name string) Origins { return r.origins.Load().Origins[name] }

// SwapAccess publishes a checked immutable access copy from the core's event.
func (r *Reloads) SwapAccess(snapshot *AccessCopy) { r.access.Store(snapshot) }

// SwapLogAndOrigins publishes a checked immutable log and origins copy.
func (r *Reloads) SwapLogAndOrigins(snapshot *LogAndOrigins) { r.origins.Store(snapshot) }

// SwapTLS publishes a checked immutable TLS copy from the core's event.
func (r *Reloads) SwapTLS(snapshot *TLSCopy) { r.tls.Store(snapshot) }

// BridgeTLS returns the current immutable material for a new bridge attempt.
func (r *Reloads) BridgeTLS(name string) *tls.Config { return r.tls.Load().bridges[name] }

// OriginPolicy parses the listener's checked origins once, before serving.
func (l *Listener) OriginPolicy() (Origins, error) {
	policy := Origins{SameOrigin: l.SameOrigin, Allowed: make([]*url.URL, len(l.AllowedOrigins))}
	for i, value := range l.AllowedOrigins {
		origin, err := ParseOrigin(value)
		if err != nil {
			return Origins{}, err
		}
		policy.Allowed[i] = origin
	}
	return policy, nil
}

func (c *Config) logAndOrigins() (*LogAndOrigins, error) {
	snapshot := &LogAndOrigins{Level: c.Broker.LogLevel, Origins: make(map[string]Origins)}
	for _, l := range c.Broker.MQTT.Listen.WS {
		policy, err := l.OriginPolicy()
		if err != nil {
			return nil, err
		}
		snapshot.Origins[l.Name] = policy
	}
	return snapshot, nil
}

// ReloadLogAndOrigins uses the configuration loader's startup validation.
// All other settings in the parsed file are discarded.
func (c *Config) ReloadLogAndOrigins(path string) (*LogAndOrigins, error) {
	parsed, err := Load(path)
	if err != nil {
		return nil, err
	}
	configured, err := parsed.logAndOrigins()
	if err != nil {
		return nil, err
	}
	// Only startup membership can own a policy. Never retain an edited name
	// that did not start a listener, even across later renames.
	running := c.Broker.MQTT.Listen.WS
	snapshot := &LogAndOrigins{Level: configured.Level, Origins: make(map[string]Origins, len(running))}
	old := c.Live.origins.Load()
	for i := range running {
		name := running[i].Name
		policy, present := configured.Origins[name]
		if !present {
			policy = old.Origins[name]
		}
		snapshot.Origins[name] = policy
	}
	return snapshot, nil
}

// ReloadAccess reads the startup paths against the unchanged channel routing.
// Neither a failed password file nor a failed ACL publishes a partial snapshot.
func (c *Config) ReloadAccess() (*AccessCopy, error) {
	fresh := *c
	if err := errors.Join(fresh.LoadPasswords(), fresh.LoadACL()); err != nil {
		return nil, err
	}
	snapshot := &AccessCopy{Passwords: fresh.Passwords, ACL: fresh.ACL}
	return snapshot, nil
}

// TLSConfig supplies current material at ClientHello, rather than at accept.
// The returned wrapper is private to the listener; published configs never change.
func (c *Config) TLSConfig(l *Listener, operations bool) (*tls.Config, error) {
	if c.Live == nil {
		return l.LoadTLS()
	}
	if l.TLS == nil {
		return nil, nil
	}
	return &tls.Config{MinVersion: l.TLS.MinVersion, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		snapshot := c.Live.tls.Load()
		if operations {
			return snapshot.operations[l.Name], nil
		}
		return snapshot.mqtt[l.Name], nil
	}}, nil
}

func (c *Config) loadTLS() (*TLSCopy, error) {
	snapshot := &TLSCopy{mqtt: make(map[string]*tls.Config), operations: make(map[string]*tls.Config), bridges: make(map[string]*tls.Config)}
	var findings []error
	load := func(listen *Listen, into map[string]*tls.Config, operations bool) {
		for _, ref := range listenerRefs(listen, "", nil) {
			l := ref.listener
			secure, err := l.LoadTLS()
			if err == nil && secure != nil {
				if operations || strings.HasSuffix(ref.key, ".ws") {
					secure.NextProtos = []string{"http/1.1"}
				}
				err = newTicketKeys(secure)
			}
			if err != nil {
				findings = append(findings, err)
			}
			into[l.Name] = secure
			if err == nil && l.TLS != nil {
				snapshot.certificate("listener "+l.Name, l.TLS.CertFile, secure)
			}
		}
	}
	load(&c.Broker.MQTT.Listen, snapshot.mqtt, false)
	if o := c.Broker.Operations; o != nil {
		load(&o.Listen, snapshot.operations, true)
	}
	for i := range c.Bridges {
		b := &c.Bridges[i]
		if !b.UsesTLS() {
			continue
		}
		secure, err := b.LoadTLS()
		if err != nil {
			findings = append(findings, err)
		}
		snapshot.bridges[b.Name] = secure
		if err == nil && b.TLS != nil {
			snapshot.certificate("bridge "+b.Name, b.TLS.CertFile, secure)
		}
	}
	return snapshot, errors.Join(findings...)
}

func newTicketKeys(secure *tls.Config) error {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return fmt.Errorf("TLS session-ticket key: %w", err)
	}
	secure.SetSessionTicketKeys([][32]byte{key})
	return nil
}

// ReloadTLS builds every listener and bridge together without publishing.
// A refused change rotates tickets using clones of the old certificate/CA bundle.
func (c *Config) ReloadTLS() (*TLSCopy, error) {
	snapshot, err := c.loadTLS()
	if err != nil {
		old := c.Live.tls.Load()
		snapshot = &TLSCopy{mqtt: make(map[string]*tls.Config), operations: make(map[string]*tls.Config), bridges: old.bridges,
			certificates: old.certificates}
		for _, pair := range [2][2]map[string]*tls.Config{{old.mqtt, snapshot.mqtt}, {old.operations, snapshot.operations}} {
			for name, secure := range pair[0] {
				if secure == nil {
					continue
				}
				fresh := secure.Clone()
				if keyErr := newTicketKeys(fresh); keyErr != nil {
					return nil, errors.Join(err, keyErr)
				}
				pair[1][name] = fresh
			}
		}
	}
	return snapshot, err
}
