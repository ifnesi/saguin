package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
)

// unopenable is every file a configuration names that saguin cannot read or
// cannot use, in a fixed order.
//
// **A configuration is not valid because its schema is.** RFC 0002 promises
// `--check-config` exits non-zero on any finding, and gives the reason: it
// is meant as a systemd `ExecStartPre`, so a bad configuration fails the
// unit rather than the broker. That was true when written, when the schema
// named no file the broker opens. Then a password file, a certificate, a
// key, a client authority and a bridge's authority arrived, each of them
// opened at startup and none of them looked at by the check - so the
// pre-check passed and the unit died anyway, at the worst moment there is,
// which is a restart.
//
// So the check opens them, and so does the startup path, before a listener
// binds. **Every finding at once rather than the first**: an operator
// fixing a certificate path should not discover the password file's typo on
// the next attempt.
//
// It reads and never writes, which is what keeps RFC 0002's other promise -
// that this runs while a broker is up, on another machine, against a
// configuration from a backup. On a machine where the files are absent it
// now says so, which is true rather than convenient: the document
// `--output` prints is still printed.
func unopenable(cfg *config.File, reg *channel.Registry) []string {
	var findings []string
	add := func(format string, args ...any) {
		findings = append(findings, fmt.Sprintf(format, args...))
	}

	findings = append(findings, authUnopenable(cfg, reg)...)

	if o := cfg.Broker.Operations; o != nil {
		for _, d := range o.Listen.TCP {
			checkTLS(add, config.OpsDoorPath("tcp", d.Name), d.TLS)
		}
	}
	for _, d := range cfg.Broker.MQTT.Listen.TCP {
		checkTLS(add, config.DoorPath("tcp", d.Name), d.TLS)
	}
	for _, d := range cfg.Broker.MQTT.Listen.WS {
		checkTLS(add, config.DoorPath("ws", d.Name), d.TLS)
	}

	for _, b := range cfg.BridgeSet() {
		if b.CAFile != "" {
			if _, err := certPool(b.CAFile); err != nil {
				add("bridges.%s.ca_file: %v", b.Name, err)
			}
		}
		// The certificate a bridge presents upstream, read the way the bridge
		// will read it. Validation has already guaranteed the pair is both or
		// neither, so the certificate alone is the test for whether there is
		// one.
		if b.CertFile != "" {
			if _, err := tls.LoadX509KeyPair(b.CertFile, b.KeyFile); err != nil {
				add("bridges.%s.cert_file and key_file: %v", b.Name, err)
			}
		}
	}

	sort.Strings(findings)
	return findings
}

// authUnopenable is the half of unopenable about **who may connect and what
// they may do**: the two kinds of password file and the acl_file, checked
// the way the broker reads them.
//
// It is its own function because these three are the credential files
// SIGUSR1 re-reads, and a re-read must refuse a file it cannot use without
// ever putting it in force. Sharing this with --check-config is what keeps the signal from
// accepting a file the check refuses, or refusing one it passes - the same
// reason startup and the check share it.
//
// **It says nothing about certificates or bridges.** The certificates have
// a re-read path of their own, bridges are startup-only, and a finding
// about either must not stop a credential being withdrawn.
func authUnopenable(cfg *config.File, reg *channel.Registry) []string {
	var findings []string
	add := func(format string, args ...any) {
		findings = append(findings, fmt.Sprintf(format, args...))
	}
	if p := cfg.Broker.MQTT.PasswordFile; p != "" {
		checkPasswd(add, "broker.mqtt.password_file", p, cfg.Broker.MQTT.Anonymous(), false)
	}
	// **And each listener's own**, resolved through the same function
	// startup uses. A listener naming a file the broker cannot read is a
	// broker that will not come up, and a check that missed it would pass a
	// configuration the unit then dies on - which is the failure this whole
	// function exists for, one listener further in.
	//
	// In a fixed order, because a map is not one and an operator fixing
	// three findings should not get them shuffled between runs.
	auth := cfg.Broker.MQTT.ListenerAuth()
	ids := make([]string, 0, len(auth))
	for id := range auth {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		file, anonymous := auth[id].Resolve(cfg.Broker.MQTT)
		// Only what the listener named itself: the broker-wide file is
		// checked above, and reporting it again once per listener turns one
		// typo into four findings.
		if strings.TrimSpace(auth[id].PasswordFile) == "" {
			continue
		}
		checkPasswd(add, "broker.mqtt.listen."+id+".password_file", file, anonymous, false)
	}
	// The acl_file is read at startup like the password file and every
	// certificate, so a configuration naming one saguin cannot read - or
	// one whose rules cannot mean what they say - is a broker that will not
	// come up. A deploy pipeline gating on --check-config would ship it.
	//
	// **Every finding from the file, not the fact that there were some.**
	// authz.Load collects them all in one message for the same reason this
	// function does: an operator fixing an authorization file one error per
	// run is restarting a broker to find the next one.
	if p := cfg.Broker.MQTT.ACLFile; p != "" && reg != nil {
		if _, err := authz.Load(p, reg, cfg.Broker.Limits.Resolve().MaxTopicLevels); err != nil {
			add("broker.mqtt.acl_file: %v", err)
		}
	}
	if o := cfg.Broker.Operations; o != nil {
		// **Each door's file, through the same resolution the startup
		// uses**, so that --check-config cannot pass a configuration the
		// broker then refuses. A path written on a listener and a path
		// written on the block are the same kind of thing and get the same
		// check; a path both doors resolve to is checked once.
		seen := map[string]bool{}
		for _, d := range o.OperatorFiles() {
			if d.Path == "" || seen[d.Path] {
				continue
			}
			seen[d.Path] = true
			// An operations file naming nobody is refused at startup, so it
			// is a finding here for the same reason: a door nobody can read
			// is not what anybody configured.
			checkPasswd(add, d.Where, d.Path, false, true)
		}
	}
	sort.Strings(findings)
	return findings
}

// checkPasswd reads a password file the way the broker will, so that a hash
// saguin cannot read - an argon2id entry from Mosquitto 2.1 - is a finding
// here rather than a fleet that cannot connect.
// **`routes` says whether this file is allowed to name any.** Only the
// operations one is: an MQTT client reaches topics rather than paths, so a
// third field there governs nothing while reading as though it governed
// something - and it would stop that file being Mosquitto's format hash for
// hash, which RFC 0002 promises anybody migrating.
func checkPasswd(add func(string, ...any), at, path string, anonymous, routes bool) {
	f, err := passwd.Load(path)
	if err != nil {
		add("%s: %v", at, err)
		return
	}
	if len(f.Users()) == 0 && !anonymous {
		add("%s: names no users, and nothing else admits anybody", at)
	}
	if !routes {
		if f.AnyScoped() {
			add("%s: names routes for a user, which only the operations password file "+
				"does - an MQTT client's permissions are the acl_file's", at)
		}
		return
	}
	// A scope naming no route denies that user everything, from a file that
	// reads as though it granted something. This is the moment an operator
	// is looking.
	for _, finding := range f.CheckScopes() {
		add("%s: %s", at, finding)
	}
}

// checkTLS reads a listener's certificate, its key and its client authority.
func checkTLS(add func(string, ...any), at string, t *config.TLS) {
	if t == nil {
		return
	}
	if _, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile); err != nil {
		add("%s.tls: %v", at, err)
	}
	if t.MutualTLS() {
		if _, err := certPool(t.ClientCAFile); err != nil {
			add("%s.tls.client_ca_file: %v", at, err)
		}
	}
}

// certPool is a file of PEM authorities, or the reason it is not one.
//
// AppendCertsFromPEM reports only whether it found anything, so a file of
// the wrong kind - a key, a DER certificate, an empty file - arrives here
// as "no certificates" rather than as a parse error.
func certPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s holds no PEM certificates", path)
	}
	return pool, nil
}

// collapsingRules is every inbound bridge rule whose topic template throws
// a `+` level away, and what each one costs.
//
// **These are notes and not findings.** `unopenable` above returns things
// that stop a broker; this returns things an operator may well have meant.
// Dropping a level is the only way to write "one bridge per vessel, and the
// vessel id is in the configuration rather than in the topic", so refusing
// it would refuse a real configuration - and the broker cannot tell that
// apart from a template somebody mistyped. So `--check-config` says what
// was dropped and leaves the decision where it belongs, and the exit code
// does not change.
//
// The unbounded version of this is already refused where the template is
// compiled: a filter ending in `#` whose topic has no `$#` collapses an
// unlimited number of upstream topics onto one, and that is a startup
// error. What is left here is the finite case.
//
// A `latest` channel is called out separately because there it is not a
// mess but a loss: the channel keeps one value per topic, so two upstream
// topics that collapse into one become one value overwriting itself, and
// the channel is doing exactly what it is designed to do while it happens.
func collapsingRules(cfg *config.File, reg *channel.Registry) []string {
	var notes []string
	for _, b := range cfg.BridgeSet() {
		for _, in := range b.Topics {
			dropped := in.DroppedWildcards()
			if len(dropped) == 0 {
				continue
			}
			// **Asked of where the rule can reach rather than of a channel
			// it named.** A rule names no channel now - the topic decides,
			// as it does for every publisher (invariant 11) - so the cost
			// is read off the channels the template can land in. Any one of
			// them being a `latest` channel is enough for the sharper
			// sentence: that is where a dropped level stops being a mess
			// and becomes a loss.
			cost := "records differing only there land on the same topic"
			// **Sharpened for an inbound rule only.** The `latest` sentence
			// is about what the *destination* does with two topics that
			// collapsed into one, and an `out` rule's destination is the
			// peer: which of its topics is a last-value store is not
			// knowable here, and the channels this broker happens to have
			// say nothing about it. So an outbound rule gets the plain
			// statement, which is true of any broker.
			if in.In {
				for _, c := range in.Reaches(reg) {
					if c.Type == channel.Latest {
						cost = "records differing only there become one value in a `latest` " +
							"channel, each overwriting the last"
						break
					}
				}
			}
			notes = append(notes, fmt.Sprintf(
				"bridge %q, filter %q: the topic does not use %s, so %s",
				b.Name, in.Filter, strings.Join(dropped, " or "), cost))
		}
	}
	return notes
}

// authorization is everything the two kinds of password file and the
// acl_file resolve to: loaded, checked, and not yet in force.
//
// **It exists so that a re-read is whole or is nothing.** Both the startup
// and SIGUSR1 come through here, and neither applies anything until every
// file has been read and passed its own rules - a narrowed acl_file put in
// force beside credentials that failed to load is a broker nobody
// configured, and the moment it happens is the moment an operator is
// editing under load.
type authorization struct {
	// unnamable is every entry of a credential file whose name holds a NUL
	// or a control character (broker.ValidName): it can never authenticate,
	// so it is said once when the file is read, not refused - the rest of
	// the file is still good.
	unnamable []string

	// Who may connect on the broker as a whole, and on any listener that
	// resolved to something of its own.
	clients   *passwd.File
	anonymous bool
	doors     map[string]doorCredentials

	// What they may do once in. `rules` is the decision the publish path
	// asks; `acl` and `aclPath` are the file itself, which
	// `/v1/operations/acl` needs to explain an answer.
	acl     *authz.File
	aclPath string
	rules   *authz.Rules

	// Who may read the operations routes, keyed by the operations door's
	// own name (RFC 0002 "Several listeners of a kind") - its own id
	// space, separate from the MQTT doors' above.
	opsByDoor map[string]*passwd.File
}

// doorCredentials is one listener's resolved pair.
type doorCredentials struct {
	users     *passwd.File
	anonymous bool
}

// loadAuthorization reads every credential file a configuration names and
// returns them together, or the findings that stopped it.
//
// **Findings rather than an error**, because there is rarely one: an
// operator fixing an authorization file one message per signal is editing
// under a broker that keeps refusing the whole file.
func loadAuthorization(cfg *config.File, reg *channel.Registry) (*authorization, []string) {
	if findings := authUnopenable(cfg, reg); len(findings) > 0 {
		return nil, findings
	}
	a := &authorization{
		anonymous: cfg.Broker.MQTT.Anonymous(),
		doors:     map[string]doorCredentials{},
	}
	var findings []string
	// One read per path: three listeners naming one file is ordinary, and
	// reading it three times is three chances to disagree about what it
	// holds.
	loaded := map[string]*passwd.File{}
	read := func(at, path string) *passwd.File {
		if f, ok := loaded[path]; ok {
			return f
		}
		f, err := passwd.Load(path)
		if err != nil {
			// Reachable only when the file changed between the check above
			// and this read. It is still a finding rather than a panic: a
			// half-saved file is exactly what a signal arrives in the middle
			// of.
			findings = append(findings, fmt.Sprintf("%s: %v", at, err))
			return nil
		}
		loaded[path] = f
		for _, user := range f.Users() {
			if !broker.ValidName(user) {
				a.unnamable = append(a.unnamable, fmt.Sprintf("%s: user %q", at, user))
			}
		}
		return f
	}

	if p := cfg.Broker.MQTT.PasswordFile; p != "" {
		a.clients = read("broker.mqtt.password_file", p)
	}
	// **Every listener, not only the ones that overrode something.** A
	// listener taking the broker's pair resolves to exactly that pair, so
	// setting it changes nothing - and doing it unconditionally means there
	// is no branch here that could disagree with Auth.Resolve about which
	// listeners are special.
	for id, auth := range cfg.Broker.MQTT.ListenerAuth() {
		path, anonymous := auth.Resolve(cfg.Broker.MQTT)
		if path == "" {
			a.doors[id] = doorCredentials{users: nil, anonymous: anonymous}
			continue
		}
		a.doors[id] = doorCredentials{
			users:     read("broker.mqtt.listen."+id+".password_file", path),
			anonymous: anonymous,
		}
	}
	if p := cfg.Broker.MQTT.ACLFile; p != "" {
		acl, err := authz.Load(p, reg, cfg.Broker.Limits.Resolve().MaxTopicLevels)
		if err != nil {
			findings = append(findings, fmt.Sprintf("broker.mqtt.acl_file: %v", err))
		} else {
			a.acl, a.aclPath, a.rules = acl, p, authz.New(acl, reg)
			for pattern := range acl.Users {
				if !broker.ValidName(pattern) {
					a.unnamable = append(a.unnamable, fmt.Sprintf("broker.mqtt.acl_file: users %q", pattern))
				}
			}
		}
	}
	if o := cfg.Broker.Operations; o != nil {
		for _, d := range o.OperatorFiles() {
			if d.Path == "" {
				continue
			}
			if a.opsByDoor == nil {
				a.opsByDoor = map[string]*passwd.File{}
			}
			a.opsByDoor[d.Door] = read(d.Where, d.Path)
		}
	}
	if len(findings) > 0 {
		sort.Strings(findings)
		return nil, findings
	}
	return a, nil
}

// warnUnnamable says once, at the start and at each re-read, which entries
// of the credential files can never authenticate because their name holds
// a NUL or a control character (broker.ValidName).
func warnUnnamable(log *slog.Logger, a *authorization) {
	for _, entry := range a.unnamable {
		log.Warn("a credential entry can never authenticate: its name holds a control character",
			"entry", entry)
	}
}

// applyTo puts a loaded authorization in force.
//
// **Nothing here can fail**, which is the point of loading first: every
// setter takes a value already read, so there is no moment where half of a
// credential change is live. The broker's setters are an atomic store or a
// lock apiece for the same reason - they are called on a running broker.
func (a *authorization) applyTo(b *broker.Broker, ops *broker.Operators) {
	b.SetCredentials(a.clients, a.anonymous)
	for id, d := range a.doors {
		b.SetListenerCredentials(id, d.users, d.anonymous)
	}
	// **A configuration with no acl_file clears the authorizer rather than
	// installing an empty one.** Wrapping nil rules would make every verb
	// ask a file that is not there; the absent authorizer is what "no
	// acl_file" already means everywhere else in the broker.
	if a.rules != nil {
		b.Authorize(a.rules)
	} else {
		b.SetAuthorizer(nil)
	}
	// The file itself goes over separately, because explaining an answer
	// means reading the patterns that produced it. A configuration writing
	// no acl_file still says so, with nil: the route's answer there is that
	// everything is allowed, and it can only give it if it was told.
	b.SetACL(a.acl, a.aclPath)
	if ops != nil {
		ops.Set(a.opsByDoor)
	}
}

// bridgeReach is one line per bridge rule saying which channels the topics
// it can produce land in, and it is **the answer rather than an assertion
// the operator has to keep in step**.
//
// A rule used to carry `channel:`, checked at startup where the answer was
// decidable and at runtime where it was not, with a mismatched record
// dropped and counted. That made a bridge know where the channels are,
// which invariant 11 says a client never has to do - and a bridge is a
// client. What it was protecting against is a typo in a topic template, and
// a typo is better answered by printing where the records actually go than
// by asking an operator to write the answer twice and refusing them when
// the two drift.
//
// **Every channel the template can reach, not the one it usually hits.** A
// template with a wildcard in it spans whatever its filter spans, so a rule
// landing in two channels says two, and a rule landing in none says
// broadcast - which is not a channel type but what saguin does with a topic
// nothing claims.
func bridgeReach(cfg *config.File, reg *channel.Registry) []string {
	var out []string
	for _, b := range cfg.BridgeSet() {
		for _, r := range b.Topics {
			where := "broadcast, because no channel claims those topics"
			if cs := r.Reaches(reg); len(cs) > 0 {
				names := make([]string, 0, len(cs))
				for _, c := range cs {
					names = append(names, fmt.Sprintf("%s (%s)", c.Name, c.Type))
				}
				sort.Strings(names)
				where = strings.Join(names, ", ")
			}
			verb := "lands in"
			if !r.In {
				verb = "reads from"
			}
			line := fmt.Sprintf("bridge %q, %s %q: %s %s",
				b.Name, r.Direction(), r.Filter, verb, where)

			// **And the queues it touches and is never served.** Nothing
			// refuses such a rule and nothing logs it at runtime, because
			// from the broker's side nothing is wrong: a filter crossing a
			// queue is served everything else it matches, exactly as any
			// subscriber's is (invariant 11). That silence is the reason it
			// belongs here - an operator who wrote a broad filter over an
			// estate with a queue in it has a rule that works and carries
			// less than they may think, and this is where they find out
			// before the records do not arrive.
			if qs := r.Crosses(reg); len(qs) > 0 {
				names := make([]string, 0, len(qs))
				for _, c := range qs {
					names = append(names, c.Name)
				}
				sort.Strings(names)
				line += fmt.Sprintf("; crosses %s (queue), never served",
					strings.Join(names, ", "))
			}
			out = append(out, line)
		}
	}
	return out
}
