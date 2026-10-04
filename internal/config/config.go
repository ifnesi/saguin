// Package config loads and validates saguin's single configuration file.
//
// An invalid configuration is a startup failure, and every finding is
// reported rather than only the first. There is no partial start in which
// some channels work.
package config

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/store"
)

type File struct {
	Broker   Broker                   `yaml:"broker"`
	Channels map[string]ChannelConfig `yaml:"channels"`

	// Bridges is saguin as a client of other brokers, as the file writes it.
	Bridges map[string]BridgeConfig `yaml:"bridges"`

	// bridges is the validated, compiled form Load produces from it. Kept
	// apart from the map above so that neither has to be read as the other:
	// one is what the operator wrote and the other is what the broker runs.
	bridges []Bridge

	// channelsFrom is the file each channel was written in, however many
	// files the configuration is: Report's configured_in reads it for
	// every channel, and Flatten comments it only where two or more files
	// are involved (commentChannels).
	channelsFrom map[string]string
}

// BridgeSet returns the validated bridges, ordered by name so that two runs
// of one configuration start them in the same order.
func (f *File) BridgeSet() []Bridge { return f.bridges }

// LogLevels is what `broker.log_level` accepts, and the only list of them.
// main reads this rather than a switch of its own, so a level that
// validates is a level the broker can actually run at.
var LogLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// Level is the level this configuration asks for, or Info when it asks for
// nothing. Validation has already refused anything else.
func (b Broker) Level() slog.Level {
	if l, ok := LogLevels[strings.ToLower(strings.TrimSpace(b.LogLevel))]; ok {
		return l
	}
	return slog.LevelInfo
}

type Broker struct {
	ID   string `yaml:"id"`
	MQTT MQTT   `yaml:"mqtt"`

	// Replica is the key that made this broker a disaster-recovery copy of
	// another, and it is **still a field so that the refusal can say what
	// replaced it** - the same reason `upstream:` and `inbound:` are still
	// fields on a bridge. It is a bool pointer so that `replica: false`,
	// which was the honest way to write "not a copy", is told the same
	// thing rather than passing silently into a broker where the word no
	// longer means anything.
	//
	// Every deployment built on the arrangement RFC 0002 documented until
	// recently has this key, and the decoder's own "field replica not
	// found in type config.Broker" is a Go type name in an operator's face.
	Replica *bool `yaml:"replica"`

	// LogLevel is how much the broker says: `debug`, `info`, `warn` or
	// `error`, and `info` when it is not written.
	//
	// **It was a constant, and the lines below Info were unreachable.** The
	// bridge logs a failed connection attempt at Debug on purpose - a bad
	// link would otherwise write one line per retry for ever, and the two
	// lines that matter come from connecting and disconnecting - which made
	// that line invisible in every deployment rather than merely quiet. A
	// level nobody can set is not a level.
	//
	// It is also the honest answer to Mosquitto's `connection_messages`,
	// which exists so that a fleet of thousands on a flaky link does not
	// drown its own log in connects. That is a question about volume, and
	// volume is what a level is for; a second flag naming one kind of line
	// would go stale the first time somebody added another kind.
	LogLevel string `yaml:"log_level"`

	// PIDFile is where the running process writes its own id, or empty for
	// nowhere - which is the default and what most deployments want, since
	// systemd, Docker and runit all track the process themselves.
	//
	// **It is here and a log file is not, and the difference is invariant
	// 13.** Everything that accumulates is bounded, with defined behaviour
	// at the bound. A log file accumulates and would be the only such thing
	// in this file with no bound beside it; a pid file holds one number and
	// is replaced. So the one that cannot grow is offered and the one that
	// can is left to the init system, which already rotates it.
	//
	// **Absolute, like every other path saguin is given.** Which file a
	// process claims must not depend on the directory somebody started it
	// from - a relative path here means two copies started from two places
	// each believe they hold the lock.
	PIDFile string `yaml:"pid_file"`

	// Operations is the HTTP listener that carries everything saguin says
	// to an operator rather than to a client: `/health`, `/metrics` and the
	// `/v1/operations` routes (RFC 0005 "The operations listener"). Absent
	// means no listener is opened, the same way a `listen` with no `unix`
	// block opens no socket - a port nobody configured is a port nobody
	// asked to have open.
	//
	// It was `health` while that was the only path. Renamed rather than
	// added beside, because a listener answering more than `/health` under
	// a block named `health` misleads everyone who reads the file. Unknown
	// keys are refused, so a configuration written against the old name is a
	// startup error naming `broker.health` rather than a broker that quietly
	// opens nothing.
	Operations *Operations `yaml:"operations"`

	Storage Storage `yaml:"storage"`
	Limits  Limits  `yaml:"limits"`

	// Retained changes where a retained message on a *broadcast* topic is
	// kept and for how long (RFC 0002 "Retained messages on a broadcast
	// topic"). The store always exists: absent, it is on
	// broker.storage.default and keeps a value until it is replaced or
	// deleted. It is bounded by its provider's max_bytes like everything
	// else there, so no store is one a client can grow past what the
	// operator wrote. Taking retained values away from a client is the
	// acl_file's `broker: features` denial, never this block.
	//
	// A topic a channel claims is not this: the channel is already that
	// store, and the flag adds nothing to what it does.
	//
	// A node rather than a *Retained so that the strict decode in
	// RetainedBlock sees exactly what was written.
	Retained yaml.Node `yaml:"retained"`

	// QoS2 changes where a half-finished exactly-once publish waits between
	// the PUBLISH and the PUBREL, how many one client may hold, and for how
	// long (RFC 0003 "Exactly once"). QoS 2 is always offered: absent, the
	// store is on broker.storage.default with the defaults below.
	//
	// **The store is the feature.** MQTT transfers ownership of the message
	// at the PUBREC, a full round trip before the client is told the
	// exchange is done, so a broker offering QoS 2 is holding messages
	// nobody has finished sending. Somewhere to put them, bounded by a
	// provider and by max_inflight_per_client, is what makes offering it
	// honest. Taking QoS 2 away from a client is the acl_file's job.
	//
	// A node rather than a *QoS2 for the reason Retained is one.
	QoS2 yaml.Node `yaml:"qos2"`

	// Session changes where the state of every MQTT session is kept: its
	// subscriptions and the messages it is owed or has in flight, persistent
	// or not. Absent, it is on broker.storage.default. A persistent session
	// survives what its provider survives; a non-persistent one is dropped
	// at its disconnect and at startup.
	//
	// **One key for every session rather than one for persistent ones**, so
	// that an operator learns one rule: each kind of data has one storage
	// key, named after the data, whatever the client asked for. Taking a
	// persistent session away from a client is the acl_file's `persistent`
	// denial.
	//
	// A node rather than a *Session for the reason Retained is one.
	Session yaml.Node `yaml:"session"`

	// Share bounds how long a shared group's backlog waits: the deliveries
	// a group is owed while none of its members is connected to take them.
	//
	// A node rather than a *Share for the reason Retained is one.
	Share yaml.Node `yaml:"share"`
}

// Retained configures the one store that holds the current value of
// broadcast topics a client asked to have kept.
//
// It is not a channel: it claims no prefix, has no name, and holds only
// topics no channel claims. One store rather than one per topic prefix,
// because a channel created by a client is a channel the configuration
// file does not describe, and nothing would bound how many of them a
// device publishing to a fresh topic every second could make.
type Retained struct {
	// Storage is the provider holding it, defaulting to
	// broker.storage.default - the same word a channel uses, so there is
	// nothing new to learn.
	Storage string `yaml:"storage"`

	// RetentionPeriod deletes the current value of any topic that has gone
	// quiet for longer than it. It defaults to `none`, and that is the
	// deliberate half: on a store keyed by topic any figure that could be
	// defaulted to would lose the state of a device that reports less often
	// than it, and a dashboard would show nothing with no error anywhere.
	// Expiry that belongs to one value belongs to its publisher, and MQTT
	// already carries that as a Message Expiry Interval.
	RetentionPeriod string `yaml:"retention_period"`

	// Declared so that a configuration writing either is refused with a
	// saguin message saying why, rather than with a YAML parser complaining
	// about a key it has never heard of. Neither applies here, for the
	// reason a `latest` channel takes neither: what grows is the number of
	// topics rather than a history, and the provider's own max_bytes is
	// what bounds it.
	RetentionBytes string `yaml:"retention_bytes"`
	MaxBytes       string `yaml:"max_bytes"`
}

// RetainedBlock reads the block. Not written and written with nothing
// under it both mean the defaults.
//
// The strict decode is the same technique a group of channels goes through
// and is here for the same reason: a Node decodes without KnownFields, so
// `retenion_period` inside this block would otherwise be dropped in
// silence, and a bound an operator believes they set is worse than one
// they were refused.
func (b Broker) RetainedBlock() (*Retained, error) {
	if b.Retained.IsZero() || b.Retained.Tag == "!!null" {
		return &Retained{}, nil
	}
	if b.Retained.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("broker.retained: write the keys of the retained store under it, " +
			"or remove it for the defaults")
	}
	raw, err := yaml.Marshal(&b.Retained)
	if err != nil {
		return nil, fmt.Errorf("broker.retained: %w", err)
	}
	var r Retained
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&r); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("broker.retained: %w", err)
	}
	return &r, nil
}

// RetainedStore resolves the block into what the broker needs: which
// provider holds it and how long a quiet topic keeps its value, in
// seconds, zero meaning `none`.
//
// Validation has already refused anything unparseable, so a failure here
// reads as the defaults rather than being returned: that is the same
// contract Limits.Resolve works to, and Load will not have handed back a
// File that reaches this state.
func (f *File) RetainedStore() (provider string, period int64) {
	provider = f.Broker.Storage.Default
	r, err := f.Broker.RetainedBlock()
	if err != nil {
		return provider, 0
	}
	if r.Storage != "" {
		provider = r.Storage
	}
	if r.RetentionPeriod != "" {
		period, _ = parseRetentionPeriod(r.RetentionPeriod)
	}
	return provider, period
}

// validate holds the retained block to RFC 0002 "Validation".
func (r *Retained) validate(s Storage) []string {
	if r == nil {
		return nil
	}
	var findings []string

	// No providers at all is Storage.validate's finding, and repeating it
	// here as an undefined name would say the same thing twice.
	if len(s.Providers) == 0 {
		return nil
	}

	name := r.Storage
	if name == "" {
		name = s.Default
	}
	if _, ok := s.Providers[name]; !ok {
		findings = append(findings, fmt.Sprintf(
			"broker.retained.storage names %q, which is not a defined provider", name))
	}

	if r.RetentionPeriod != "" {
		if _, err := parseRetentionPeriod(r.RetentionPeriod); err != nil {
			findings = append(findings, fmt.Sprintf("broker.retained.retention_period %q: %v",
				r.RetentionPeriod, err))
		}
	}

	// Both are refused rather than ignored. A key that is accepted and does
	// nothing is a bound an operator believes they set.
	if r.RetentionBytes != "" {
		findings = append(findings, "broker.retained.retention_bytes does not apply: the retained "+
			"store holds one value per topic rather than a history, so what grows is the number "+
			"of topics - its provider's max_bytes is what bounds it, and past that a retained "+
			"publish is refused rather than an older value deleted")
	}
	if r.MaxBytes != "" {
		findings = append(findings, "broker.retained.max_bytes does not apply: bound the provider "+
			"under broker.storage.providers instead, which is what holds this store")
	}

	return findings
}

// QoS2 configures the one store that holds exactly-once publishes between
// the PUBLISH that starts them and the PUBREL that finishes them.
//
// It is not a channel and holds nothing an operator reads. Every row in it
// is a message one publisher has sent and not yet released, and the row is
// gone the moment it is released, the session ends, or it ages out. What
// accumulates here is unfinished work rather than data, which is why the
// two numbers below are about bounding a publisher rather than about
// keeping anything.
//
// **Nothing in it is written for durability, and that is deliberate.** A
// restart answers every reconnecting client Session Present = 0, and
// MQTT-3.2.2-5 then requires the client to discard the exchange while
// MQTT-4.4.0-1 forbids it from re-sending. So a row that survived a restart
// could never be completed by anybody; it is swept at startup. The store is
// a provider because a provider is bounded, counted and visible to an
// operator, not because these rows need to reach a disk.
type QoS2 struct {
	// Storage is declared so that a configuration writing it is refused with
	// a saguin message saying why, rather than with a YAML parser complaining
	// about a key it has never heard of: an exactly-once publish is held in
	// the store of the channel it is for, or the broadcast log's for a
	// broadcast, so there is no provider to name.
	Storage string `yaml:"storage"`

	// MaxInflightPerClient is how many exactly-once publishes one client
	// may have unreleased at once. Past it the next one is refused on its
	// PUBREC with `0x97`, before ownership of it is taken, and the
	// connection is left alone: the publisher's next exchange succeeds as
	// soon as one of its own completes.
	//
	// **Per client, because a shared pool is a client's misbehaviour
	// charged to everybody else.** One publisher that opens exchanges and
	// never releases them fills its own allowance and stalls itself.
	//
	// **A count rather than a size**, because `limits.max_message_size`
	// already caps what one row can be, so the worst case a client can
	// reach is this number times that - one figure to write instead of two
	// that have to agree.
	//
	// Refusing rather than making room is invariant 13: enforcement happens
	// before allocation, and the outcome at a bound is a reason code. The
	// alternative, dropping a held message to admit a new one, discards a
	// message saguin has already claimed ownership of at its PUBREC, and
	// the one it would discard is the one closest to finishing.
	MaxInflightPerClient *int `yaml:"max_inflight_per_client"`

	// ExpiresAfter drops a publish that has waited this long for its
	// PUBREL. Without it such a row would sit until its session ended,
	// which `limits.max_session_expiry` puts at thirty days by default,
	// spending a provider's bytes on an exchange nobody is going to finish.
	//
	// **The only reason to wait at all is a reconnect.** A connected
	// publisher sends its PUBREL within a round trip, so in ordinary
	// running nothing is held for measurable time. What the figure has to
	// cover is a client whose link dropped after the PUBREC and which comes
	// back to the same session to finish - which on a poor radio link is
	// tens of seconds rather than seconds, and is why the default is
	// minutes and not `write_timeout`.
	ExpiresAfter string `yaml:"expires_after"`

	// Declared so that a configuration writing it is refused with a saguin
	// message saying why, rather than with a YAML parser complaining about
	// a key it has never heard of. It is the same answer broker.retained
	// gives: the provider is what bounds the store.
	MaxBytes string `yaml:"max_bytes"`
}

// Share configures how long a shared group's backlog waits: the deliveries
// a group is owed while none of its members is connected to take them. They
// are the broadcast log behind the group's cursor, on the provider
// broker.session.storage names (RFC 0002 "What a shared group is owed").
type Share struct {
	// Storage is read only to be refused by name (validate): a group's
	// cursor is worth nothing without the log it points into, so it is kept
	// in the log's provider. Told "unknown key", an operator goes looking
	// for a spelling that no longer exists.
	Storage string `yaml:"storage"`

	// ExpiresAfter drops a backlogged delivery that has waited this long,
	// whatever its members' sessions say.
	//
	// **Empty is no expiry of its own**, and then a backlog lives exactly as
	// long as a member session that could collect it: the doors that end the
	// last one are what remove it. That is the conservative default, because
	// the alternative is a broker quietly discarding work an operator never
	// asked it to bound. An operator who would rather bound the wait writes
	// a duration here.
	ExpiresAfter string `yaml:"expires_after"`
}

// ShareBlock reads the block, the same technique and the same reason as
// RetainedBlock. Not written and written with nothing under it both mean
// the defaults.
func (b Broker) ShareBlock() (*Share, error) {
	if b.Share.IsZero() || b.Share.Tag == "!!null" {
		return &Share{}, nil
	}
	if b.Share.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("broker.share: write its keys under it, or remove it for the defaults")
	}
	raw, err := yaml.Marshal(&b.Share)
	if err != nil {
		return nil, fmt.Errorf("broker.share: %w", err)
	}
	var sh Share
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&sh); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("broker.share: %w", err)
	}
	return &sh, nil
}

// ShareExpiry resolves the block into what the broker needs: how long a
// group's delivery waits before it is dropped, zero for no expiry of its own.
//
// Validation has already refused anything unparseable, so a failure here
// reads as the default rather than being returned - the same contract
// RetainedStore works to.
func (f *File) ShareExpiry() time.Duration {
	sh, err := f.Broker.ShareBlock()
	if err != nil || sh.ExpiresAfter == "" {
		return 0
	}
	expiresAfter, _ := parseWholeSeconds(sh.ExpiresAfter)
	return expiresAfter
}

// validate holds the share block to RFC 0002 "Validation".
func (sh *Share) validate() []string {
	if sh == nil {
		return nil
	}
	var findings []string
	if strings.TrimSpace(sh.Storage) != "" {
		findings = append(findings,
			"broker.share.storage is gone. A shared group's backlog is the broadcast log behind "+
				"the group's cursor, kept on the provider broker.session.storage names "+
				"(RFC 0002 \"What a shared group is owed\"). Delete the key")
	}
	if sh.ExpiresAfter != "" {
		if _, err := parseWholeSeconds(sh.ExpiresAfter); err != nil {
			findings = append(findings, fmt.Sprintf("broker.share.expires_after %q: %v; omit the "+
				"key to let a backlog live as long as a member session that could collect it",
				sh.ExpiresAfter, err))
		}
	}
	return findings
}

// QoS2Block reads the block, the same technique and the same reason as
// RetainedBlock. Not written and written with nothing under it both mean
// the defaults.
func (b Broker) QoS2Block() (*QoS2, error) {
	if b.QoS2.IsZero() || b.QoS2.Tag == "!!null" {
		return &QoS2{}, nil
	}
	if b.QoS2.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("broker.qos2: write its keys under it, " +
			"or remove it for the defaults")
	}
	raw, err := yaml.Marshal(&b.QoS2)
	if err != nil {
		return nil, fmt.Errorf("broker.qos2: %w", err)
	}
	var q QoS2
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&q); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("broker.qos2: %w", err)
	}
	return &q, nil
}

// QoS2Limits resolves the block into what the broker needs: how many
// unfinished exactly-once publishes one client may have, and how long one
// waits before it is dropped.
//
// Validation has already refused anything unparseable, so a failure here
// reads as the defaults rather than being returned - the same contract
// RetainedStore works to.
func (f *File) QoS2Limits() (maxInflight int, expiresAfter time.Duration) {
	maxInflight = defaultQoS2MaxInflight
	expiresAfter = defaultQoS2ExpiresAfter
	q, err := f.Broker.QoS2Block()
	if err != nil {
		return maxInflight, expiresAfter
	}
	if q.MaxInflightPerClient != nil {
		maxInflight = *q.MaxInflightPerClient
	}
	if q.ExpiresAfter != "" {
		expiresAfter, _ = parseWholeSeconds(q.ExpiresAfter)
	}
	return maxInflight, expiresAfter
}

// defaultQoS2MaxInflight is how many unreleased exactly-once publishes one
// client may hold where the operator writes no figure.
//
// Twenty, which is what mosquitto advertises for the same thing - measured
// on 2.0.22 rather than read, its CONNACK carries Receive Maximum 20. It is
// a number a publisher with a backlog can pipeline into and a number a
// stalled one cannot spend a provider with: at the default
// `limits.max_message_size` of 1MiB it is 20MiB per client at the very
// worst, and a publisher reaches it only by leaving twenty exchanges
// unreleased at once.
const defaultQoS2MaxInflight = 20

// defaultQoS2ExpiresAfter is how long an unreleased publish waits where the
// operator writes no figure. See QoS2.ExpiresAfter for why this is minutes.
const defaultQoS2ExpiresAfter = 5 * time.Minute

// validate holds the qos2 block to RFC 0002 "Validation".
func (q *QoS2) validate(s Storage) []string {
	if q == nil {
		return nil
	}
	var findings []string

	// Refused by name: a configuration written when these publishes had a
	// store of their own carries it, and accepting it would be a key that
	// does nothing.
	if q.Storage != "" {
		findings = append(findings, "broker.qos2.storage does not apply: an exactly-once publish is "+
			"held in the store of the channel it is for, or the broadcast log's for a broadcast, so "+
			"there is no provider to name. Remove the key")
	}

	if q.MaxInflightPerClient != nil && *q.MaxInflightPerClient < 1 {
		findings = append(findings, fmt.Sprintf(
			"broker.qos2.max_inflight_per_client is %d: write 1 or more, or omit the key for the "+
				"default of %d. Zero would refuse every exactly-once publish while the CONNACK "+
				"advertised that they were accepted",
			*q.MaxInflightPerClient, defaultQoS2MaxInflight))
	}

	if q.ExpiresAfter != "" {
		if _, err := parseWholeSeconds(q.ExpiresAfter); err != nil {
			findings = append(findings, fmt.Sprintf("broker.qos2.expires_after %q: %v; omit the "+
				"key for the default of %s", q.ExpiresAfter, err, defaultQoS2ExpiresAfter))
		}
	}

	// Refused rather than ignored. A key that is accepted and does nothing
	// is a bound an operator believes they set.
	if q.MaxBytes != "" {
		findings = append(findings, "broker.qos2.max_bytes does not apply: a held publish counts "+
			"against the provider of the channel it is for, or of the broadcast log for a broadcast, "+
			"bounded under broker.storage.providers. "+
			"What bounds one publisher here is max_inflight_per_client")
	}

	return findings
}

// Session configures the store that keeps every session's state.
type Session struct {
	// Storage is the provider holding it, defaulting to
	// broker.storage.default - the same word a channel and every other store
	// uses.
	Storage string `yaml:"storage"`

	// Declared so that writing it is refused with a saguin message saying
	// why, rather than a YAML parser complaining about a key it has never
	// heard of. The provider is what bounds the store, and what bounds one
	// session is limits.session_queue_bytes.
	MaxBytes string `yaml:"max_bytes"`

	// AckCommitInterval is how long a connected session's acknowledgements
	// may wait to be stored: a broadcast session's, and a channel
	// consumer's position. Absent is DefaultAckCommitInterval. Read it with
	// File.AckCommitInterval.
	AckCommitInterval string `yaml:"ack_commit_interval"`
}

// DefaultAckCommitInterval is broker.session.ack_commit_interval when it is
// not written.
const DefaultAckCommitInterval = 200 * time.Millisecond

// MinAckCommitInterval and MaxAckCommitInterval bound
// broker.session.ack_commit_interval.
//
// **The interval is what an unclean stop sends again**: what a session
// acknowledged in the last interval is not stored yet, so a crash re-sends
// it (invariant 18). It is also how long those acknowledgements wait in
// memory and in the store's in-flight table. Past a second, "the last
// moment" is seconds of duplicate traffic per busy session, so a second is
// the most accepted - the same ceiling commit waits have (MaxCommitInterval).
// Below ten milliseconds the one ticker that stores channel positions spins
// rather than waits, and storing a broadcast session's acknowledgements
// after every batch measured no faster than on the default.
const (
	MinAckCommitInterval = 10 * time.Millisecond
	MaxAckCommitInterval = time.Second
)

// SessionBlock reads the block, the same technique and the same reason as
// RetainedBlock. Not written and written with nothing under it both mean the
// defaults.
func (b Broker) SessionBlock() (*Session, error) {
	if b.Session.IsZero() || b.Session.Tag == "!!null" {
		return &Session{}, nil
	}
	if b.Session.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("broker.session: write the keys of the session store under it, " +
			"or remove it for the defaults")
	}
	raw, err := yaml.Marshal(&b.Session)
	if err != nil {
		return nil, fmt.Errorf("broker.session: %w", err)
	}
	var sess Session
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&sess); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("broker.session: %w", err)
	}
	return &sess, nil
}

// SessionStore resolves the block to the provider that keeps every
// session's state. Validation has already refused anything unparseable.
func (f *File) SessionStore() (provider string) {
	provider = f.Broker.Storage.Default
	if s, err := f.Broker.SessionBlock(); err == nil && s.Storage != "" {
		provider = s.Storage
	}
	return provider
}

// validate holds the session block to RFC 0002 "Validation".
func (s *Session) validate(st Storage) []string {
	if s == nil || len(st.Providers) == 0 {
		return nil // no providers at all is Storage.validate's finding
	}
	var findings []string
	name := s.Storage
	if name == "" {
		name = st.Default
	}
	if _, ok := st.Providers[name]; !ok {
		findings = append(findings, fmt.Sprintf(
			"broker.session.storage names %q, which is not a defined provider", name))
	}
	if s.MaxBytes != "" {
		findings = append(findings, "broker.session.max_bytes does not apply: bound the provider "+
			"under broker.storage.providers instead, which is what holds this store. What bounds "+
			"one session is limits.session_queue_bytes")
	}
	if written := strings.TrimSpace(s.AckCommitInterval); written != "" {
		d, err := parseDuration(written)
		switch {
		case strings.EqualFold(written, "none"):
			findings = append(findings, fmt.Sprintf(
				"broker.session.ack_commit_interval cannot be none: acknowledgements are always stored, "+
					"and this is only how long they may wait while a session is connected. Write a "+
					"duration from %s to %s, or leave it out for %s",
				MinAckCommitInterval, MaxAckCommitInterval, DefaultAckCommitInterval))
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"broker.session.ack_commit_interval %q: %v. Write a duration from %s to %s, or leave it "+
					"out for %s", s.AckCommitInterval, err, MinAckCommitInterval, MaxAckCommitInterval,
				DefaultAckCommitInterval))
		case d < MinAckCommitInterval:
			findings = append(findings, fmt.Sprintf(
				"broker.session.ack_commit_interval %q is shorter than %s, the least saguin accepts. "+
					"Acknowledgements are always stored - a clean DISCONNECT and a stop store them "+
					"whatever this says - and this is only how long they may wait while a session is "+
					"connected, so there is no setting that turns storing them off",
				s.AckCommitInterval, MinAckCommitInterval))
		case d > MaxAckCommitInterval:
			findings = append(findings, fmt.Sprintf(
				"broker.session.ack_commit_interval %q is longer than %s, the most saguin accepts. It is "+
					"what an unclean stop sends again to every busy session that had acknowledged it, "+
					"and how long those acknowledgements wait in memory",
				s.AckCommitInterval, MaxAckCommitInterval))
		}
	}
	return findings
}

// AckCommitInterval is broker.session.ack_commit_interval, or its default.
// Validation has already refused anything else.
func (f *File) AckCommitInterval() time.Duration {
	s, err := f.Broker.SessionBlock()
	if err != nil || strings.TrimSpace(s.AckCommitInterval) == "" {
		return DefaultAckCommitInterval
	}
	d, err := parseDuration(s.AckCommitInterval)
	if err != nil || d < MinAckCommitInterval || d > MaxAckCommitInterval {
		return DefaultAckCommitInterval
	}
	return d
}

// Storage is where channels keep their records. At least one provider and a
// default naming one of them are mandatory: a configuration without them is
// refused at startup.
type Storage struct {
	Default string `yaml:"default"`

	// Providers is what the rest of saguin reads. It is filled from
	// ProvidersNode once the file it was written in is known, because an
	// `!include` names a path relative to that file and a decoder is not
	// told which file it is reading.
	Providers map[string]Provider `yaml:"-"`

	// ProvidersNode is `providers:` as written: one mapping, or a list
	// whose entries are each a group - `!include <file>`, or providers
	// written in place. The same two forms as `channels:`, for the same
	// reason: a domain's storage moves into a file its own owners review.
	//
	// It is a field rather than a custom unmarshaler on the map because the
	// path is not available inside one, and it is exported because yaml
	// does not fill what it cannot see. Read Providers.
	ProvidersNode yaml.Node `yaml:"providers"`

	// from is the file each provider was written in, for the two errors
	// that have to name it: a name defined twice, and two names on one
	// store.
	from map[string]string

	// What a channel gets when it does not state its own. Both are required,
	// exactly as Default is, and both accept the word `none`.
	//
	// There is no built-in figure because both candidates are wrong. A
	// built-in period makes a durable, replayable log quietly mean "the last
	// few days of one", and a reader who did not exist yesterday finds
	// yesterday's events missing with nothing to tell them - the retention
	// floor only reports a gap to a consumer that already had a position. A
	// built-in `none` makes every channel grow until the disk does not,
	// which is invariant 13's failure with no defined behaviour at the
	// bound. So the operator writes it once (RFC 0002).
	DefaultRetentionPeriod string `yaml:"default_retention_period"`
	DefaultRetentionBytes  string `yaml:"default_retention_bytes"`
}

// RetentionNone is what an operator writes to say that a channel keeps
// everything, and to mean it. Omission is not the same thing: a channel
// that says nothing takes the broker-wide default.
const RetentionNone = "none"

// parseRetentionPeriod reads a retention period, or `none`, which is zero.
//
// Zero written as a number is refused by parseDuration, and that matters
// here more than anywhere else: an operator who meant `none` and typed `0`
// would otherwise configure a channel that discards every record it is
// given (RFC 0002 "Validation").
func parseRetentionPeriod(s string) (int64, error) {
	if strings.TrimSpace(s) == RetentionNone {
		return 0, nil
	}
	d, err := parseWholeSeconds(s)
	if err != nil {
		return 0, err
	}
	return int64(d / time.Second), nil
}

// parseRetentionBytes reads a retention size, or `none`, which is zero.
func parseRetentionBytes(s string) (int64, error) {
	if strings.TrimSpace(s) == RetentionNone {
		return 0, nil
	}
	return parseBytes(s)
}

// Provider is one place records are kept: `memory`, whose durability is
// the last snapshot it wrote, or `sqlite`, which survives a crash.
//
// `max_bytes` is declared here so that a configuration written against
// RFC 0002 is refused with a saguin message saying what does not exist
// yet, rather than with a YAML parser complaining about a key it has never
// heard of.
type Provider struct {
	Type string `yaml:"type"`

	// SnapshotDir is the directory a memory provider's channels are
	// written to at shutdown, or the literal "none". There is no default,
	// because both defaults are wrong: a default path invents a filename
	// in the operator's filesystem, and defaulting to none makes "durable
	// channel, memory provider" quietly mean "discarded on shutdown"
	// (RFC 0002 "Validation").
	SnapshotDir string `yaml:"snapshot_dir"`

	// FilePath is where a sqlite provider keeps its database. It names a
	// file where snapshot_dir names a directory: they are not the same
	// kind of thing, and giving them one name would imply one guarantee.
	FilePath string `yaml:"file_path"`
	MaxBytes string `yaml:"max_bytes"`

	// How a sqlite provider commits what is published to it. A batch ends
	// at **whichever of the two comes first**.
	//
	// PublishCommitInterval is how long one transaction may collect publishes
	// before committing them together, written as a duration like every
	// other in this file. Absent, a transaction collects what arrived while
	// the one before it was committing and never waits for more
	// (sqlite.DB.storeBehind); `none` gives every publish a transaction of
	// its own. **Its useful
	// range is single-figure milliseconds and its ceiling is
	// MaxCommitInterval**, because the wait is paid per connection rather
	// than per batch: a client that publishes and waits gets one record per
	// interval, however busy the broker is. So `2ms` is a setting and `2s`
	// is refused. It is *not* a wait for readers, which is the natural
	// guess and wrong - see MaxCommitInterval.
	//
	// PublishCommitMaxRecords is how many records one transaction may hold, and it
	// is required alongside an interval with no default, because the figure
	// that decides whether any of this helps is how many connections
	// publish at the same time and saguin does not know it. It runs from
	// MinCommitMaxRecords to limits.max_connections; commitFindings has why
	// each end is where it is.
	//
	// Only sqlite has them. A memory provider commits nothing, so there is
	// nothing to collect.
	PublishCommitInterval   string `yaml:"publish_commit_interval"`
	PublishCommitMaxRecords int    `yaml:"publish_commit_max_records,omitempty"`

	// ReadConnections is how many read-only connections a sqlite provider
	// opens beside its one writer, so that consumers reading channels do not
	// queue behind publishes (sqlite.DB.ReadConnections). Absent is
	// store.SQLiteReadConnections; 0 is none, every read on the write
	// connection, for a box that cannot spare a page cache per connection;
	// at most store.SQLiteMaxReadConnections.
	ReadConnections *int `yaml:"read_connections"`

	// FlushInterval is how often a sqlite provider forces its write-ahead
	// log to disk while it runs with synchronous=NORMAL, which bounds what a
	// power cut can lose to the last interval plus one fsync (RFC 0004 "WAL,
	// and synchronous=NORMAL"). Absent is DefaultFlushInterval; there is no
	// value that turns it off. Read it with Storage.FlushIntervals.
	FlushInterval string `yaml:"flush_interval"`

	// RetiredCommitInterval and RetiredCommitMaxRecords are the two keys
	// above under the names they had before broker.session.ack_commit_interval
	// was a second commit wait beside them. Fields still, as Broker.Replica
	// is, so a configuration using them is told the new names rather than
	// shown an unknown key.
	RetiredCommitInterval   yaml.Node `yaml:"commit_interval"`
	RetiredCommitMaxRecords yaml.Node `yaml:"commit_max_records"`
}

// DefaultFlushInterval is a sqlite provider's flush_interval when it is
// absent. MinFlushInterval and MaxFlushInterval bound it: below the least,
// the fsyncs are the load; above the most, the window a power cut can take
// is longer than anything the setting was meant to allow.
const (
	DefaultFlushInterval = 150 * time.Millisecond
	MinFlushInterval     = 10 * time.Millisecond
	MaxFlushInterval     = time.Second
)

// flushFindings checks a sqlite provider's flush_interval.
func (p Provider) flushFindings(at string) []string {
	written := strings.TrimSpace(p.FlushInterval)
	if written == "" {
		return nil
	}
	d, err := parseDuration(written)
	switch {
	case strings.EqualFold(written, "none"):
		return []string{fmt.Sprintf(
			"%s.flush_interval cannot be none: the log is always forced to disk, and this is only how "+
				"often. Write a duration from %s to %s, or leave it out for %s",
			at, MinFlushInterval, MaxFlushInterval, DefaultFlushInterval)}
	case err != nil:
		return []string{fmt.Sprintf(
			"%s.flush_interval %q: %v. Write a duration from %s to %s, or leave it out for %s",
			at, p.FlushInterval, err, MinFlushInterval, MaxFlushInterval, DefaultFlushInterval)}
	case d < MinFlushInterval:
		return []string{fmt.Sprintf(
			"%s.flush_interval %q is shorter than %s, the least saguin accepts: below that the "+
				"fsyncs are the load, and each writes at least one block of the disk",
			at, p.FlushInterval, MinFlushInterval)}
	case d > MaxFlushInterval:
		return []string{fmt.Sprintf(
			"%s.flush_interval %q is longer than %s, the most saguin accepts: a power cut can lose "+
				"acknowledged data from about one interval plus the time a sync takes",
			at, p.FlushInterval, MaxFlushInterval)}
	}
	return nil
}

// FlushIntervals returns each sqlite provider's flush_interval, its default
// where absent. Validation has already refused anything else.
func (s Storage) FlushIntervals() map[string]time.Duration {
	out := map[string]time.Duration{}
	for name, p := range s.Providers {
		if p.Type != "sqlite" {
			continue
		}
		out[name] = DefaultFlushInterval
		if d, err := parseDuration(p.FlushInterval); err == nil && d >= MinFlushInterval && d <= MaxFlushInterval {
			out[name] = d
		}
	}
	return out
}

// There is no default for publish_commit_max_records, and this is why.
//
// **The figure that matters is how many client connections publish at the
// same time, and saguin does not know it.** Connections rather than records
// in flight: one connection's packets are read and answered in turn, so a
// client with twenty publishes outstanding still fills a batch by one. A
// transaction closes on the record count or on the interval, and only the
// first of those is fast: a batch that fills on the count commits the moment
// the last record arrives, while a batch that has to wait out the interval
// commits once per interval however little is in it. So a count above the
// number of publishers turns every commit into a fixed wait, and that is
// slower than a transaction per publish (`none`). RFC 0002's table, through
// the wire, publishers each waiting for their own acknowledgement, the
// interval at 2ms:
//
//	publishers      `none`          max_records 256          max_records at the count
//	         8  10,100-11,000/s   3,400/s (high)          32,900-33,400/s
//	        32  10,200-10,900/s  12,200/s (high)          56,700-76,100/s
//	       256  10,300-10,800/s  77,200-79,900/s (met)    73,600-92,200/s
//
// Where the count is above the traffic it is never reached, so every
// transaction waits out the interval and commits a handful - three times
// *worse* than `none` at eight publishers and no better at thirty-two, and
// it is the column an operator lands in if saguin picks the number for them.
// Where the traffic meets the count it is three to nine times better.
// Nothing about the setting distinguishes those two but a number only the
// deployment knows, so the operator writes it down. Leaving both keys out
// collects without waiting, which grows with the traffic and needs no
// number (RFC 0002 has it measured beside these).

// MaxCommitInterval is the longest wait a batch may be given.
//
// Three costs, and the first is the one that surprises people. **The
// interval is paid per connection, not per batch.** saguin stores one
// record per connection at a time - a connection's next PUBLISH is not read
// off the socket until its last is stored - so a client that publishes and
// waits for its acknowledgement gets exactly one record per interval,
// whatever else is going on. At a second, that is one message a second.
//
// Second - and this one is a correction of the obvious guess rather than a
// cost - **it is not a wait for readers.** Reads run beside the writer on
// connections of their own, and even with read_connections 0, where they
// share the one write connection and an open transaction does hold it, a
// batch collecting for 2ms reads as though it must hold every reader on
// that provider for 2ms. It does not:
// the leader waits out the interval holding nothing at all, and opens the
// transaction when the wait is over. A consumer's read, a point read and a
// metrics scrape wait for the commit alone, bounded by publish_commit_max_records.
// TestTheIntervalIsNotAWaitForAReader is what keeps that true, because the
// wrong version of this sentence was in seven places before a test drove
// it.
//
// Third, nothing beyond a few milliseconds buys throughput: the record
// count is what collects a batch, and the interval only decides how long a
// batch that will not fill waits before giving up.
//
// So a figure above this is a slip rather than a choice - almost always
// seconds written where milliseconds were meant - and refusing it at
// startup is cheaper than finding it in a graph.
const MaxCommitInterval = time.Second

// MinCommitMaxRecords is the smallest batch worth collecting.
//
// One is refused rather than taken to mean "off", because a transaction
// that may hold a single record still waits out publish_commit_interval before
// committing it: strictly slower than a transaction per publish, while
// reading in a file as though collecting were on. A transaction per publish
// is `publish_commit_interval: none`; leaving both keys out collects without
// waiting.
//
// **There is no constant for the other end**, because the largest useful
// batch is not a number saguin can name: it is limits.max_connections. See
// commitFindings.
const MinCommitMaxRecords = 2

// DefaultDeletionRetentionPeriod is a day, which is how long a `latest`
// channel keeps a deletion when the file does not say.
//
// A deletion is held so that a copy of the channel on another broker can be
// told a topic is gone, and it has to survive the link being down. A day is
// long enough for an outage somebody sleeps through and short enough that a
// channel keeping its values for ever does not keep a row for every device
// ever decommissioned. An estate whose links go down for longer says so.
const DefaultDeletionRetentionPeriod = int64(24 * 60 * 60)

// SnapshotNone is what an operator writes to say that a memory provider
// keeps nothing across a restart, and to mean it.
const SnapshotNone = "none"

// clashes refuses two providers that name one store.
//
// **Two names on one `file_path` or `snapshot_dir` are two writers on one
// store**, each with its own bound, its own retention and its own idea of
// what is in there. Today that dies at runtime on the database's lock file,
// which is late, and its message is about a lock rather than about a
// configuration - and for a snapshot directory there is no lock at all:
// two providers write one another's files at shutdown and the loser's
// channels come back short.
//
// It is refused here because this is the one moment an operator is looking,
// and it names **both providers and both files**. That second half is the
// point of the rule once providers can be included: the two halves of the
// clash are written by different people in different files, and a finding
// that names one of them sends the reader to the file that is not the
// problem.
//
// Paths are compared after cleaning, so `/var/lib/saguin/db` and
// `/var/lib/saguin/./db` are the clash they are. Nothing is resolved
// against the filesystem: a symlink or a bind mount can make two different
// paths one store, and saguin cannot see that from a configuration file -
// what it can do is refuse the case that is visible in the text.
func (s Storage) clashes(names []string) []string {
	var findings []string
	seen := map[string]string{} // cleaned path -> the provider that claimed it

	for _, name := range names {
		p := s.Providers[name]
		for what, path := range map[string]string{
			"file_path":    p.FilePath,
			"snapshot_dir": p.SnapshotDir,
		} {
			if path == "" || path == SnapshotNone {
				continue
			}
			key := what + "\x00" + filepath.Clean(path)
			prev, ok := seen[key]
			if !ok {
				seen[key] = name
				continue
			}
			findings = append(findings, fmt.Sprintf(
				"broker.storage.providers.%s.%s and .%s.%s are both %q: two providers on one "+
					"store are two writers on it, each with its own bound and its own retention. "+
					"%s", prev, what, name, what, path, s.writtenIn(prev, name)))
		}
	}
	sort.Strings(findings) // a map was iterated to get here
	return findings
}

// writtenIn says which file each of two providers was written in, for a
// finding about the pair of them. Both in one file is the ordinary case and
// says so shortly; two files is the case this sentence exists for.
func (s Storage) writtenIn(a, b string) string {
	fa, fb := s.from[a], s.from[b]
	switch {
	case fa == "" || fb == "":
		return "Give one of them its own path."
	case fa == fb:
		return fmt.Sprintf("Both are in %s; give one of them its own path.", fa)
	default:
		return fmt.Sprintf("%s is in %s and %s is in %s; give one of them its own path.",
			a, fa, b, fb)
	}
}

// Kinds is each provider's type, by name, for saguin_provider_info. It is
// what the operator wrote, so the label is bounded by their file (RFC 0005
// "Labels, and where the catalogue stops").
func (s Storage) Kinds() map[string]string {
	out := make(map[string]string, len(s.Providers))
	for name, p := range s.Providers {
		out[name] = p.Type
	}
	return out
}

// SQLitePaths returns the database file each sqlite provider keeps its
// records in.
func (s Storage) SQLitePaths() map[string]string {
	out := map[string]string{}
	for name, p := range s.Providers {
		if p.Type == "sqlite" && p.FilePath != "" {
			out[name] = p.FilePath
		}
	}
	return out
}

// CommitGroup is how one provider commits what is published to it: how long
// a transaction may collect publishes, and how many records it may hold.
// **Whichever is reached first closes it.** A zero Interval means each
// publish gets a transaction of its own, which is what `none` asks for. A
// provider that says nothing has no CommitGroup, and collects what arrives
// while the transaction before it commits (sqlite.DB.CommitGroup).
type CommitGroup struct {
	Interval   time.Duration
	MaxRecords int
}

// ReadConnections returns how many read connections each sqlite provider
// opens: what it wrote, or store.SQLiteReadConnections where it wrote
// nothing.
func (s Storage) ReadConnections() map[string]int {
	out := map[string]int{}
	for name, p := range s.Providers {
		if p.Type != "sqlite" {
			continue
		}
		out[name] = store.SQLiteReadConnections
		if p.ReadConnections != nil {
			out[name] = *p.ReadConnections
		}
	}
	return out
}

// CommitGroups returns how each provider commits, leaving out those that
// said nothing. Validation has already refused anything unparseable, so a
// key that does not read here is off rather than an error nobody saw.
func (s Storage) CommitGroups() map[string]CommitGroup {
	out := map[string]CommitGroup{}
	for name, p := range s.Providers {
		v := strings.TrimSpace(p.PublishCommitInterval)
		if v == "" {
			continue
		}
		if strings.EqualFold(v, CommitNone) {
			out[name] = CommitGroup{}
			continue
		}
		d, err := parseDuration(v)
		if err != nil || d <= 0 {
			continue
		}
		out[name] = CommitGroup{Interval: d, MaxRecords: p.PublishCommitMaxRecords}
	}
	return out
}

// CommitNone is what an operator writes to say that every publish gets its
// own transaction, so that no publisher's acknowledgement waits for another
// publisher's record to be written.
const CommitNone = "none"

// commitFindings checks a sqlite provider's two commit keys, and takes the
// connection limit because that is what bounds one of them.
//
// **Every combination this accepts does something, and every one that would
// do nothing is refused here** rather than at somebody's next performance
// review. publish_commit_interval is what makes a transaction wait, so a
// record count without an interval - the key absent, or `none` - is
// refused: nothing would wait for the count, and the file would read as
// though it set one.
//
// The bounds, each with what puts it there:
//
//   - publish_commit_interval: absent to collect without waiting, `none`
//     for a transaction per publish, otherwise a duration up to
//     MaxCommitInterval. Its own comment has the
//     three costs, of which the per-connection one is the surprise.
//   - publish_commit_max_records: at least MinCommitMaxRecords, because one is not
//     a batch.
//   - publish_commit_max_records: **at most limits.max_connections**, and this is
//     the bound that matters. saguin stores one record per connection at a
//     time, so a batch can never hold more records than there are
//     connections to have sent them - a count above the connection limit is
//     unreachable by construction. Every transaction would wait out the
//     interval and commit a handful, which is slower than a transaction per
//     publish - three times slower at eight publishers in RFC 0002's table.
//     It is the commonest way to get this wrong, and
//     the only one saguin can prove at startup rather than leave to a
//     graph.
//
// The last of those is a ceiling and not a target. A count at the
// connection limit is legal and usually still too high: what fills a batch
// is the connections publishing *at the same time*, which is smaller than
// the number that may connect.
func (p Provider) commitFindings(at string, maxConnections int64) []string {
	var findings []string

	// **off is "the operator did not ask for collecting", not "saguin could
	// not read what they asked for".** Treating an unparseable interval as
	// off made the count report that publish_commit_interval was not set, beside
	// the finding saying what was wrong with it - two messages, the second
	// contradicting the file in front of the reader.
	written := strings.TrimSpace(p.PublishCommitInterval)
	off := written == "" || strings.EqualFold(written, CommitNone)

	if !off {
		d, err := parseDuration(written)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"%s.publish_commit_interval %q: %v; write `%s` for a transaction per publish",
				at, p.PublishCommitInterval, err, CommitNone))
		case d > MaxCommitInterval:
			findings = append(findings, fmt.Sprintf(
				"%s.publish_commit_interval %q is longer than %s, which is the most saguin accepts. The useful "+
					"range is single-figure milliseconds, because the wait is paid per connection "+
					"rather than per batch: saguin stores one record per connection at a time, so a "+
					"client that publishes and waits gets one record every %s however busy the broker "+
					"is. Consumers reading this provider are not held by it - only publishers are",
				at, p.PublishCommitInterval, MaxCommitInterval, written))
		}
	}

	switch {
	case off && p.PublishCommitMaxRecords != 0:
		findings = append(findings, fmt.Sprintf(
			"%s.publish_commit_max_records is set and publish_commit_interval gives no wait (absent or `%s`), "+
				"so nothing waits for this number and it is never read. publish_commit_interval is what makes a "+
				"transaction wait", at, CommitNone))
	case off:
	case p.PublishCommitMaxRecords == 0:
		findings = append(findings, fmt.Sprintf(
			"%s.publish_commit_interval is set and publish_commit_max_records is not. There is no default: a "+
				"transaction closes at whichever comes first, and only the record count is quick - set "+
				"above the number of connections publishing at the same time, every transaction waits "+
				"out the interval instead, which is slower than a transaction per publish. Give it a "+
				"figure at or below how many publishing connections you expect, between %d and the %d "+
				"of limits.max_connections", at, MinCommitMaxRecords, maxConnections))
	case p.PublishCommitMaxRecords < MinCommitMaxRecords:
		findings = append(findings, fmt.Sprintf(
			"%s.publish_commit_max_records is %d and the least saguin accepts is %d. A transaction that may "+
				"hold one record still waits out publish_commit_interval before committing it, which is slower "+
				"than a transaction per publish - write publish_commit_interval: %s for that, or leave both "+
				"keys out to collect without waiting",
			at, p.PublishCommitMaxRecords, MinCommitMaxRecords, CommitNone))
	case int64(p.PublishCommitMaxRecords) > maxConnections:
		findings = append(findings, fmt.Sprintf(
			"%s.publish_commit_max_records is %d and limits.max_connections is %d, so that many records can "+
				"never be waiting at once: saguin stores one record per connection at a time, so a "+
				"batch holds at most one record per connection. Every transaction would wait out "+
				"publish_commit_interval and commit a handful, which is slower than a transaction per publish. "+
				"Set it at or below the number of connections that publish together",
			at, p.PublishCommitMaxRecords, maxConnections))
	}

	return findings
}

// MaxBytes returns the ceiling each provider was given, leaving out those
// with none. A provider's bound is across every channel it holds, because
// what it protects is one pool - one disk, or one machine's memory.
func (s Storage) MaxBytes() map[string]int64 {
	out := map[string]int64{}
	for name, p := range s.Providers {
		if p.MaxBytes == "" {
			continue
		}
		if n, err := parseBytes(p.MaxBytes); err == nil {
			out[name] = n
		}
	}
	return out
}

// SnapshotDirs returns the directory each provider writes to, leaving out
// those that write nothing.
func (s Storage) SnapshotDirs() map[string]string {
	out := map[string]string{}
	for name, p := range s.Providers {
		if p.SnapshotDir != "" && p.SnapshotDir != SnapshotNone {
			out[name] = p.SnapshotDir
		}
	}
	return out
}

// Limits bound what a client may send. Every one of them is checked
// against an untrusted client on a path that runs before saguin allocates
// anything for the packet, and every one has a real reason to be lowered
// on constrained hardware (RFC 0002 "Configuration").
//
// Sizes accept a suffix - 1MiB, 8KiB, 512 - because an operator writing a
// byte count in full gets it wrong by a factor of a thousand eventually.
type Limits struct {
	MaxMessageSize string `yaml:"max_message_size"`
	MaxHeaderBytes string `yaml:"max_header_bytes"`

	// MaxSessionExpiry caps how long a client may ask its session - and so
	// its stored position - to outlive its connection. A string, because it
	// is a duration and every other duration in this file is written the
	// same way.
	MaxSessionExpiry string `yaml:"max_session_expiry"`

	// MaxKeepalive caps how long a client may go quiet before the broker
	// may close it, and `none` - the default - is no ceiling at all, which
	// is what saguin did before this existed.
	//
	// **A ceiling on a clock the broker otherwise does not own.** A client
	// chooses its own keepalive and may choose zero, which MQTT defines as
	// "never disconnect me for being idle". A device that asks for eighteen
	// hours, or for zero, holds a session the broker cannot tell is dead
	// until the session expiry runs out - a different and much longer
	// clock, and one that decides when a durable consumer's position is
	// finally released.
	//
	// MQTT 5 is built for this: the server answers with its own value in
	// the CONNACK and the client must use it (MQTT-3.1.2-21), so a ceiling
	// costs a conforming client nothing and disconnects a non-conforming
	// one sooner. Mosquitto spells it `max_keepalive`.
	MaxKeepalive string `yaml:"max_keepalive"`

	// WriteTimeout is the longest the broker will wait to hand a packet to
	// a client before disconnecting it, as a duration, or `none` for no
	// ceiling. Absent is the default in defaultWriteTimeout.
	//
	// It governs what saguin writes itself - an append or latest delivery, a
	// dead-letter record, every control reply. A queue's offers and a
	// broadcast go through the substrate's own bounded queue, which discards
	// rather than waiting, so they are neither held nor disconnected.
	//
	// **It is not the slow consumer this protects.** A client that stops
	// reading its socket while its connection stays open holds the
	// goroutine writing to it, and channel records are written on the
	// goroutine that published it - so an unrelated publisher's PUBACK
	// is withheld for a record already stored, its library reconnects and
	// re-sends, and the channel holds it twice. The same write reaches the
	// queue and retention loop through a dead-letter channel, where it
	// stops that loop for every channel at once.
	WriteTimeout string `yaml:"write_timeout"`

	// ConnectTimeout is how long a new connection may take to send its
	// CONNECT, in whole seconds. Absent is defaultConnectTimeout, and `none`
	// is refused: before CONNECT a socket is nobody's client, so nothing
	// else bounds it.
	ConnectTimeout string `yaml:"connect_timeout"`

	// MaxConnectSize bounds a CONNECT packet, fixed header included, before
	// its client has authenticated - `100KiB`. Absent is the smaller of
	// defaultMaxConnectSize and max_message_size; never larger than
	// max_message_size. A raw scalar so that `0` reaches validation and is
	// refused by name rather than by the YAML decoder.
	MaxConnectSize ScalarText `yaml:"max_connect_size"`

	// MaxConnectRate is how many connections each MQTT listener accepts a
	// second, or `none`. Absent is defaultMaxConnectRate. A raw scalar,
	// because it is a number or a word.
	MaxConnectRate ScalarText `yaml:"max_connect_rate"`

	// Pointers, so that a written `0` is not the same as an absent key.
	// Every one of these means something specific at zero and none of those
	// things is what an operator writing it wants: no topic is publishable,
	// no header may be carried, no client may connect. Left as plain
	// integers, all three were silently replaced by the default and the
	// operator was told nothing (RFC 0002 "Validation").
	MaxTopicLength    *int   `yaml:"max_topic_length"`
	MaxClientIDLength *int   `yaml:"max_client_id_length"`
	MaxHeaderCount    *int   `yaml:"max_header_count"`
	MaxConnections    *int64 `yaml:"max_connections"`

	// MaxSubscriptions is how many topic filters one client may hold. A
	// SUBSCRIBE past it has each filter over the count refused 0x97, as EMQX
	// refuses one past its max_subscriptions; a re-subscribe replaces a filter
	// and does not count, and a shared subscription is one filter. Absent is
	// defaultMaxSubscriptions.
	MaxSubscriptions *int `yaml:"max_subscriptions"`

	// MaxTopicLevels is how many levels a topic name or a topic filter may
	// have, wherever one enters: a publish, a Will, a SUBSCRIBE or an
	// UNSUBSCRIBE, and every filter in this file and the acl_file. Absent is
	// defaultMaxTopicLevels.
	MaxTopicLevels *int `yaml:"max_topic_levels"`

	// PublishRate and PublishBytes are what ONE client may send in a second,
	// or unset for no bound - which is the default and is deliberate.
	//
	// **Both, because they bound different things.** A thousand one-byte
	// publishes cost a kilobyte and a thousand topic lookups, permission
	// checks and store writes; ten one-megabyte publishes cost ten
	// operations and ten megabytes. Either alone leaves the other hole open.
	//
	// **There is no figure to pick.** The rate a deployment can sustain is
	// its storage's: the benchmark record has SQLite on disk flattening at
	// 3,000–4,500/s across every publisher count, and memory around 30,000.
	// A default drawn from either would throttle a legitimate fleet on the
	// other, and the rule against a knob that refuses a knob whose
	// only settings are the default and wrong. So an operator writes it
	// from the row that matches their storage, and unwritten means the
	// bound saguin has always had, which is none.
	//
	// A plain integer here rather than a pointer: zero is a value an
	// operator might reasonably write for "no publishing at all", but that
	// is a thing an acl_file says, and reading zero as "unset" costs
	// nothing anybody wants.
	PublishRate  *int    `yaml:"publish_rate"`
	PublishBytes *string `yaml:"publish_bytes"`

	// SessionQueueBytes bounds what one session holds of deliveries it has not
	// acknowledged - a size such as `1MiB`. Absent is the larger of
	// defaultSessionQueueBytes and max_message_size, and it is never smaller
	// than max_message_size. A full session gives up its oldest; the
	// publisher is never refused for it.
	SessionQueueBytes string `yaml:"session_queue_bytes"`

	// SessionQueueFull is read only to be refused by name: what a full session
	// gives up is not a choice.
	SessionQueueFull string `yaml:"session_queue_full"`
}

// Resolved is Limits with the sizes parsed, which is what the broker
// enforces against. Validation has already rejected anything unparseable.
type Resolved struct {
	MaxMessageSize    uint32
	MaxTopicLength    int
	MaxClientIDLength int
	MaxHeaderCount    int
	MaxHeaderBytes    int
	MaxConnections    int64
	// MaxSubscriptions is how many topic filters one client may hold.
	MaxSubscriptions int
	// MaxTopicLevels is how many levels a topic name or filter may have.
	MaxTopicLevels int
	// MaxSessionExpiry in seconds, which is what MQTT carries it as.
	MaxSessionExpiry uint32
	// MaxKeepalive in seconds, or zero for no ceiling. MQTT carries a
	// keepalive as a 16-bit count of seconds, so the ceiling is 65,535 and
	// a longer duration is refused rather than truncated.
	MaxKeepalive uint16
	// PublishRate and PublishBytes are what one client may send in a second
	// - messages and bytes - or zero for no bound. They are the floor every
	// client takes; an acl_file entry naming a client replaces them.
	PublishRate  int
	PublishBytes int64
	// MinProtocolVersion is the protocol level below which a CONNECT is
	// refused: 4 for MQTT 3.1.1, 5 for MQTT 5. It comes from `broker.mqtt`
	// rather than from `broker.limits`, and rides here because this is what
	// the broker is handed. Zero is nothing said, which is the default and
	// admits 3.1.1.
	MinProtocolVersion byte
	// WriteTimeout is the deadline put on a write to a client, or zero for
	// none. Zero is what `none` resolves to and is the behaviour that made
	// a deaf consumer able to duplicate a record on a durable channel, so
	// it is a thing an operator writes rather than a thing they get.
	WriteTimeout time.Duration
	// ConnectTimeout is the deadline on a new connection's CONNECT.
	ConnectTimeout time.Duration
	// MaxConnectSize is the largest CONNECT accepted, in bytes.
	MaxConnectSize uint32
	// MaxConnectRate is connections accepted a second per listener, or zero
	// for no limit.
	MaxConnectRate int
	// SessionQueueBytes is the most one session holds of unacknowledged
	// deliveries, in bytes of memory rather than of payload.
	SessionQueueBytes int64
}

// ScalarText is a YAML scalar taken as its own text, whether YAML would read
// it as a number or a word, so a key that is either can be validated by name.
type ScalarText string

// UnmarshalYAML takes the scalar's text.
func (s *ScalarText) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("must be a single value")
	}
	*s = ScalarText(n.Value)
	return nil
}

// Loggable bounds a client-controlled string before it reaches a log line.
//
// Anything a client names is length-prefixed with two bytes on the wire, so
// the ceiling is 65,535 characters - and the places that log one are
// refusals, which is precisely where the string has *not* been through a
// bound: a publish topic is logged because it broke `max_topic_length`, a
// Will topic arrives in CONNECT and is never checked, a subscribe filter is
// not bounded at all, and a bridge's topic belongs to a broker saguin does
// not administer. At QoS 0 a client needs no acknowledgement, so it can
// produce those as fast as the socket takes them, and every one is copied
// into the operator's log. Five of them measured 300kB before this existed.
//
// It bounds topics, filters and payloads alike, which is why it is not
// named for any of them. `max_topic_length` is the number because it is the
// only bound in the file about a length rather than a byte count, and a
// kilobyte is as much of any of those as a log line can use.
//
// It lives here because both the broker and the bridge hold a Resolved and
// neither should carry its own copy of the rule - the same argument that
// keeps a bridge asking the broker for its bounds rather than holding them
// (invariant 10).
//
// What an operator needs is that it was oversized and roughly what it
// started with, so the front of it and the real length are kept.
func (r Resolved) Loggable(s string) string {
	if len(s) <= r.MaxTopicLength {
		return s
	}
	return s[:r.MaxTopicLength] +
		fmt.Sprintf("…(truncated from %d bytes)", len(s))
}

// The defaults are the values RFC 0002's example configuration shows,
// which is the closest thing to a considered recommendation saguin has.
const (
	defaultMaxMessageSize = "1MiB"
	defaultMaxTopicLength = 1024
	// A client id is not only logged: a durable consumer's stored position
	// is keyed by it, so it is written into the store and survives restarts.
	// 256 is comfortably above the device identifiers deployments use - AWS
	// IoT and Azure IoT both settle at 128 - and far below the 65,535 the
	// wire allows.
	defaultMaxClientIDLength = 256
	defaultMaxHeaderCount    = 32
	defaultMaxHeaderBytes    = "8KiB"
	defaultMaxConnections    = 10000
	// How many topic filters one client may hold. Each is memory in three
	// indexes - measured at about 2.3KB a filter - and without a count one
	// connection took 100,000 filters and 201MiB.
	// 10,000 because Home Assistant subscribes per entity, so thousands on one
	// client is legitimate; an edge box whose devices each hold a few can set
	// 100.
	defaultMaxSubscriptions = 10000
	// How many levels a topic name or filter may have. Every level of a
	// filter is a node in the topic index, measured at about 600 bytes, so
	// one SUBSCRIBE of a 60KB filter of 30,000 levels took 17MiB and two
	// seconds of CPU, and max_subscriptions of them a client could hold. 200
	// is one level stricter than mosquitto, whose check (TOPIC_HIERARCHY_LIMIT,
	// lib/util_topic.c) refuses more than 200 `/` and so accepts 201 levels;
	// either is far past any topic tree a deployment writes.
	defaultMaxTopicLevels = 200
	// How long a session, and so a durable consumer's stored position, may
	// outlive its connection. A client asking for longer is given this and
	// told so in the CONNACK, which is the mechanism MQTT provides.
	//
	// It is a bound rather than a policy: the substrate's own default is
	// 0xFFFFFFFF, about 136 years, so a fleet taking a fresh client id every
	// boot leaves one immortal row per boot per channel and nothing an
	// operator sets bounds it (invariant 13). That is the same shape saguin
	// already refused for Topic Alias and the client id, and this is the
	// only one of the three whose state is durable.
	//
	// Thirty days rather than something shorter, because the deployments
	// saguin is for go offline for weeks - a vessel on a voyage, a solar
	// sensor in winter, a vehicle parked for a month - and the README leads
	// with a device offline for a week resuming exactly where it stopped. A
	// stored position is one small row, so a shorter cap saves almost
	// nothing; what matters is that the table is bounded, and a month bounds
	// it as completely as a week. A consumer whose channel retention has
	// passed its position is told at CONNECT either way (invariant 1), so
	// keeping one longer costs a row and never correctness.
	defaultMaxSessionExpiry = "30d"

	// defaultWriteTimeout is how long a delivery may wait on a consumer
	// before that consumer is disconnected. A write only waits at all once
	// the kernel's own send buffer is full, which means the far end has
	// stopped reading rather than merely being slow - so this is generous
	// for any link that is working, and the key exists because "working"
	// on a metered satellite link is not the same number as on a LAN.
	defaultWriteTimeout = "5s"

	// defaultConnectTimeout is how long a socket may stay open without
	// sending CONNECT. A client sends it as its first bytes, so this is a
	// TLS handshake and a round trip on a slow link with room to spare.
	defaultConnectTimeout = "10s"

	// defaultMaxConnectSize is mosquitto's own CONNECT limit (100,000 bytes),
	// rounded to a size an operator writes. It admits a Will payload at MQTT's
	// 65,535-byte maximum with a long token as the password. Measured with
	// max_connections at its default of 10,000: handshakes each holding a
	// CONNECT at this size held 1.4 GiB, against 10.6 GiB at 1 MiB; 64 KiB
	// held 1.0 GiB and 16 KiB 0.7 GiB, because about 55 KiB of each handshake
	// is fixed cost - so a small box is better served by fewer connections
	// than by a smaller CONNECT.
	defaultMaxConnectSize = 100 << 10

	// defaultMaxConnectRate is HiveMQ's connect-rate default. Measured on a
	// Ryzen 7 260 with a password file, the broker authenticates about 2,270
	// connections a second on one core and 7,700 on four, so this is under a
	// quarter of one core; a fleet of 10,000 reconnecting at once takes 20s
	// rather than 1.3s. Not measured on edge hardware.
	defaultMaxConnectRate = "500"

	// defaultSessionQueueBytes is how much one session may hold of deliveries
	// it has not acknowledged, where the operator writes no figure.
	//
	// **The bound it replaces was a count and nothing else**: 8,192 packets a
	// session, however large. Each in-flight entry was measured at about 960
	// bytes of heap beside its payload, so at the default max_message_size the
	// worst case was 8,192 x (960 B + 1 MiB), about 8 GiB, for one session a
	// device had simply stopped reading.
	//
	// **A megabyte is about a thousand small messages**, which is what
	// mosquitto (max_queued_messages), EMQX (mqueue max_len) and HiveMQ each
	// queue for an offline client by default - a count there, a size here,
	// because a count is what let one session reach eight gigabytes.
	defaultSessionQueueBytes = 1 << 20
	// defaultSessionQueueBytesText is defaultSessionQueueBytes as a
	// configuration file spells it.
	defaultSessionQueueBytesText = "1MiB"
	// defaultLogLevel is what a broker that does not say logs at.
	defaultLogLevel = "info"
	// defaultMinScrapeIntervalText is defaultMinScrapeInterval as a
	// configuration file spells it. A Duration's own String gives "1m0s",
	// which this file's own parser refuses - so the written form is written
	// down rather than derived.
	defaultMinScrapeIntervalText = "60s"
	// A queue's attempts before a record is dead-lettered. RFC 0002 shows it
	// in the example configuration, and one is the least it may be: a
	// delivery that is never attempted is not a queue.
	defaultMaxAttempts = 5
)

// withDefaults fills in what was not configured. It runs in Resolve as
// well as in Load, because a zero Limits resolves to a broker that refuses
// every publish - max_topic_length 0 admits no topic - and that is too
// sharp an edge to leave on a struct anyone can construct.
func (l Limits) withDefaults() Limits {
	if l.MaxMessageSize == "" {
		l.MaxMessageSize = defaultMaxMessageSize
	}
	if l.MaxHeaderBytes == "" {
		l.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	if l.MaxTopicLength == nil {
		l.MaxTopicLength = intPtr(defaultMaxTopicLength)
	}
	if l.MaxClientIDLength == nil {
		l.MaxClientIDLength = intPtr(defaultMaxClientIDLength)
	}
	if l.MaxHeaderCount == nil {
		l.MaxHeaderCount = intPtr(defaultMaxHeaderCount)
	}
	if l.MaxConnections == nil {
		n := int64(defaultMaxConnections)
		l.MaxConnections = &n
	}
	if l.MaxSubscriptions == nil {
		l.MaxSubscriptions = intPtr(defaultMaxSubscriptions)
	}
	if l.MaxTopicLevels == nil {
		l.MaxTopicLevels = intPtr(defaultMaxTopicLevels)
	}
	if l.MaxSessionExpiry == "" {
		l.MaxSessionExpiry = defaultMaxSessionExpiry
	}
	if l.MaxKeepalive == "" {
		l.MaxKeepalive = "none"
	}
	if l.WriteTimeout == "" {
		l.WriteTimeout = defaultWriteTimeout
	}
	if l.ConnectTimeout == "" {
		l.ConnectTimeout = defaultConnectTimeout
	}
	if l.MaxConnectRate == "" {
		l.MaxConnectRate = defaultMaxConnectRate
	}
	// **The session queue's two, written like the rest.** They were resolved
	// into Resolved and never back into the document, so
	// `/v1/operations/config` and `--check-config --output` answered without
	// them while answering with `write_timeout` beside them - and RFC 0005
	// sells that route as "the resolved values, not the written ones, which
	// is the whole reason to ask a running broker". A reader could not tell
	// an absent key from one the broker had defaulted.
	//
	// The byte figure depends on max_message_size, which is defaulted at the
	// top of this function, so it is computable here: the default is 1MiB,
	// or the message size where that is larger, which is what Resolve does.
	if l.SessionQueueBytes == "" {
		// In the file's own units rather than a byte count, so the resolved
		// document reads like the document an operator writes - and this is
		// RFC 0002's sentence exactly: 1MiB, or max_message_size where that
		// is larger.
		l.SessionQueueBytes = defaultSessionQueueBytesText
		if msg, err := parseBytes(l.MaxMessageSize); err == nil && msg > defaultSessionQueueBytes {
			l.SessionQueueBytes = l.MaxMessageSize
		}
	}
	// MaxConnectSize is left unwritten here: its default depends on
	// max_message_size, and Resolve takes the smaller.
	return l
}

func intPtr(n int) *int { return &n }

// topicLevels is limits.max_topic_levels as written, or its default: what the
// filters in this file are held to before the limits have been resolved.
func (l Limits) topicLevels() int {
	if l.MaxTopicLevels != nil {
		return *l.MaxTopicLevels
	}
	return defaultMaxTopicLevels
}

// validate holds the three counted limits above zero. Each is refused with
// what zero would have meant, because zero is a number somebody writes
// meaning "no limit" and it means the opposite at all three.
func (l Limits) validate() []string {
	var findings []string
	if l.MaxTopicLength != nil && *l.MaxTopicLength < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_topic_length %d: it is the longest topic a client may publish to, "+
				"so at zero no publish is possible at all; omit the key for the default of %d",
			*l.MaxTopicLength, defaultMaxTopicLength))
	}
	// 23 rather than 1: MQTT-3.1.3-5 requires a server to allow every client
	// id between 1 and 23 bytes, so anything lower is a broker that refuses
	// connections the specification says it must take.
	if l.MaxClientIDLength != nil && *l.MaxClientIDLength < 23 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_client_id_length %d: MQTT requires a server to accept any client "+
				"id of 1 to 23 bytes, so below 23 saguin refuses connections it must allow; "+
				"omit the key for the default of %d",
			*l.MaxClientIDLength, defaultMaxClientIDLength))
	}
	if l.MaxHeaderCount != nil && *l.MaxHeaderCount < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_header_count %d: it is how many User Properties a publish may carry, "+
				"and saguin's own headers are User Properties; omit the key for the default of %d",
			*l.MaxHeaderCount, defaultMaxHeaderCount))
	}
	if l.MaxSubscriptions != nil && *l.MaxSubscriptions < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_subscriptions %d: it is how many topic filters one client may hold, so "+
				"at zero every SUBSCRIBE is refused; omit the key for the default of %d",
			*l.MaxSubscriptions, defaultMaxSubscriptions))
	}
	if l.MaxTopicLevels != nil && *l.MaxTopicLevels < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_topic_levels %d: it is how many levels a topic or filter may have, so "+
				"at zero no publish and no SUBSCRIBE is possible; omit the key for the default of %d",
			*l.MaxTopicLevels, defaultMaxTopicLevels))
	}
	if l.MaxConnections != nil && *l.MaxConnections < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.max_connections %d: it is how many clients may be connected, so at zero "+
				"every CONNECT is refused with \"server busy\"; omit the key for the default of %d",
			*l.MaxConnections, defaultMaxConnections))
	}
	// **A written zero is refused rather than read as "unset".** The key is
	// a pointer so the two can be told apart, and they mean opposite
	// things: absent is no bound at all, and zero would be a broker that
	// refuses every publish from every client. An operator who wants that
	// writes an acl_file, and one who wrote zero meaning "no limit" is told
	// rather than given the reverse of what they asked for.
	if l.PublishRate != nil && *l.PublishRate < 1 {
		findings = append(findings, fmt.Sprintf(
			"limits.publish_rate %d: it is how many messages a second one client may "+
				"publish, so at or below zero every publish is refused with \"quota "+
				"exceeded\"; omit the key for no bound, which is the default",
			*l.PublishRate))
	}
	if l.PublishBytes != nil {
		n, err := parseBytes(*l.PublishBytes)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"limits.publish_bytes %q: %v; write a size such as 128KiB, or omit the "+
					"key for no bound", *l.PublishBytes, err))
		case n < 1:
			findings = append(findings, fmt.Sprintf(
				"limits.publish_bytes %q: at or below zero every publish is refused; "+
					"omit the key for no bound, which is the default", *l.PublishBytes))
		}
	}
	// Whole seconds, because MQTT carries a Session Expiry Interval as a
	// number of them. `none` is deliberately not accepted, unlike the
	// retention keys: it would mean a session that never expires, which is
	// the unbounded table this key exists to close.
	if l.MaxSessionExpiry != "" {
		d, err := parseWholeSeconds(l.MaxSessionExpiry)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"limits.max_session_expiry %q: %v; omit the key for the default of %s",
				l.MaxSessionExpiry, err, defaultMaxSessionExpiry))
		case d/time.Second > math.MaxUint32:
			// The wire cannot carry it, and a value that has to be truncated
			// to be sent is a configuration saying one thing and a broker
			// doing another.
			findings = append(findings, fmt.Sprintf(
				"limits.max_session_expiry %q is longer than MQTT can express, which is "+
					"0xFFFFFFFF seconds or about 136 years; omit the key for the default of %s",
				l.MaxSessionExpiry, defaultMaxSessionExpiry))
		}
	}
	// `none` is accepted here and not on max_session_expiry, and the
	// difference is what zero means at each. A session that never expires
	// is an unbounded table on this broker; a client that never has to
	// speak is the behaviour saguin already had, and turning it off is
	// choosing not to impose a ceiling rather than removing a bound.
	if k := strings.TrimSpace(l.MaxKeepalive); k != "" && !strings.EqualFold(k, "none") {
		d, err := parseWholeSeconds(k)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"limits.max_keepalive %q: %v; write `none` for no ceiling", k, err))
		case d <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.max_keepalive %q: zero would be a client that never has to speak, "+
					"which is the opposite of a ceiling; write `none` for no ceiling", k))
		case d/time.Second > math.MaxUint16:
			// MQTT carries a keepalive as two bytes of seconds, so a longer
			// ceiling could not be sent - and a value truncated on the way
			// out is a configuration saying one thing and a broker doing
			// another.
			findings = append(findings, fmt.Sprintf(
				"limits.max_keepalive %q is longer than MQTT can express, which is 65535 "+
					"seconds or about 18 hours; write `none` for no ceiling", k))
		}
	}

	// `none` is accepted for the same reason it is above: it is the
	// behaviour saguin had before the key existed, and an operator who
	// wants it back should have to write it down rather than reach it by
	// leaving a key out.
	// **saguin's own parser, not time.ParseDuration.** Every other duration
	// in this file goes through parseDuration, and this key did not - so it
	// accepted forms RFC 0002's Rules refuse and refused one they allow.
	// `1.5s` and `1h30m` loaded, `500us` and `100ns` loaded and were
	// honoured to the nanosecond, and `1d` was rejected with Go's own
	// `unknown unit "d"`. A 100ns deadline disconnects every consumer at
	// once and times out the CONNACK, which is the outcome the `d <= 0`
	// check below exists to prevent, reached through a value the check
	// accepted.
	if w := strings.TrimSpace(l.WriteTimeout); w != "" && !strings.EqualFold(w, "none") {
		d, err := parseDuration(w)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"limits.write_timeout %q: %v; write `none` for no ceiling", w, err))
		case d <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.write_timeout %q: at or below zero every delivery times out "+
					"before it is written and every consumer is disconnected at once; "+
					"write `none` for no ceiling", w))
		}
	}

	// `none` is refused here, unlike write_timeout, for the reason
	// max_session_expiry refuses it: it is not the behaviour saguin had but
	// the defect this key was added to close.
	if c := strings.TrimSpace(l.ConnectTimeout); c != "" {
		d, err := parseWholeSeconds(c)
		switch {
		case strings.EqualFold(c, "none"):
			findings = append(findings, fmt.Sprintf(
				"limits.connect_timeout %q: a socket that never sends CONNECT would be held "+
					"for ever, outside max_connections, and a stopping broker waits for it; "+
					"write a duration of 1s or longer", c))
		case err != nil:
			findings = append(findings, fmt.Sprintf("limits.connect_timeout %q: %v", c, err))
		case d <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.connect_timeout %q: every connection would be closed before its "+
					"CONNECT arrived; write 1s or longer", c))
		}
	}

	if v := strings.TrimSpace(string(l.MaxConnectSize)); v != "" {
		n, err := parseBytes(v)
		// **Against the default where max_message_size is unwritten**, as
		// session_queue_bytes is below: validation runs before the defaults
		// are filled, and comparing with an unparseable "" skipped the
		// refusal, so 2MiB passed beside a 1MiB default and 4GiB passed and
		// wrapped to 0 in Resolve's uint32.
		// max_message_size is itself refused past 32 bits, so a size held
		// to it always fits.
		msgText := l.MaxMessageSize
		if msgText == "" {
			msgText = defaultMaxMessageSize
		}
		msg, msgErr := parseBytes(msgText)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf("limits.max_connect_size %q: %v", v, err))
		case n <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.max_connect_size %q: every CONNECT would be refused; write a size such as 100KiB", v))
		case msgErr == nil && n > msg:
			findings = append(findings, fmt.Sprintf(
				"limits.max_connect_size %q is larger than limits.max_message_size %q, which every "+
					"packet is already held to; write it no larger", v, msgText))
		}
	}

	// `none` is refused, for the reason max_session_expiry refuses it: an
	// unbounded session queue is the eight gigabytes this key exists to close.
	// Below max_message_size the largest message a client may publish could
	// never be queued for a session at all.
	if v := strings.TrimSpace(l.SessionQueueBytes); v != "" {
		n, err := parseBytes(v)
		msgText := l.MaxMessageSize
		if msgText == "" {
			msgText = defaultMaxMessageSize
		}
		msg, msgErr := parseBytes(msgText)
		switch {
		case strings.EqualFold(v, "none"):
			findings = append(findings, fmt.Sprintf(
				"limits.session_queue_bytes %q: a session that stops reading would hold "+
					"every delivery it is sent; write a size such as 1MiB", v))
		case err != nil:
			findings = append(findings, fmt.Sprintf("limits.session_queue_bytes %q: %v", v, err))
		case n <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.session_queue_bytes %q: no delivery could ever wait for a session; "+
					"write a size such as 1MiB", v))
		case msgErr == nil && n < msg:
			findings = append(findings, fmt.Sprintf(
				"limits.session_queue_bytes %q is smaller than limits.max_message_size %q, so "+
					"the largest message a client may publish could never wait for a session; "+
					"write it no smaller", v, msgText))
		}
	}
	// **limits.session_queue_full is refused by name, in every value** (RFC
	// 0002 "Validation"). A session's broadcast is a cursor into one log, so
	// giving up its oldest costs nothing, which was drop_newest's argument,
	// and a full session always gives up its oldest. Told "unknown key", an
	// operator goes looking for a spelling that no longer exists.
	if strings.TrimSpace(l.SessionQueueFull) != "" {
		findings = append(findings,
			"limits.session_queue_full is gone, in every value. A full session gives up the "+
				"oldest it is owed that is not on the wire, and the publisher is never refused "+
				"(RFC 0002 \"How much a session may hold\"). Delete the key")
	}

	if v := strings.TrimSpace(string(l.MaxConnectRate)); v != "" && !strings.EqualFold(v, "none") {
		n, err := strconv.Atoi(v)
		switch {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"limits.max_connect_rate %q: write a whole number of connections a second, or `none`", v))
		case n <= 0:
			findings = append(findings, fmt.Sprintf(
				"limits.max_connect_rate %q: no connection would ever be accepted; write 1 or more, "+
					"or `none` for no limit", v))
		}
	}
	return findings
}

// StorageReserve is how much of a provider's max_bytes is held back for the
// operations that free it: a bound never refuses the operation that would
// relieve it (RFC 0003), and on a full provider the dead-letter move, the
// attempt count and a stored position are all writes like any other.
//
// It is derived rather than configured, because it is one largest possible
// relief write - the biggest record a client may publish plus the headers it
// may carry - and every one of those numbers is already set here. Exposing
// it would be a knob whose only settings are the default and wrong: nobody
// can reason about "how many bytes of dead-letter headroom", and getting it
// low means a queue that strands work at a full provider.
//
// It is one record's worth rather than several because the room is spent
// once and then reused: a relief write grows the file, and the pages the
// removal frees are what the next one writes into.
func (l Limits) StorageReserve() int64 { return l.Resolve().StorageReserve() }

// StorageReserve is Limits.StorageReserve, for limits already resolved.
func (r Resolved) StorageReserve() int64 {
	return int64(r.MaxMessageSize) + int64(r.MaxHeaderBytes)
}

// Resolve returns the parsed limits. Load has already validated them.
func (l Limits) Resolve() Resolved {
	l = l.withDefaults()
	msg, _ := parseBytes(l.MaxMessageSize)
	hdr, _ := parseBytes(l.MaxHeaderBytes)
	expiry, _ := parseWholeSeconds(l.MaxSessionExpiry)
	connect, _ := parseWholeSeconds(strings.TrimSpace(l.ConnectTimeout))
	connectSize := int64(defaultMaxConnectSize)
	if msg > 0 && msg < connectSize {
		connectSize = msg
	}
	if v := strings.TrimSpace(string(l.MaxConnectSize)); v != "" {
		if n, err := parseBytes(v); err == nil && n > 0 {
			connectSize = n
		}
	}
	connectRate := 0
	if v := strings.TrimSpace(string(l.MaxConnectRate)); !strings.EqualFold(v, "none") {
		connectRate, _ = strconv.Atoi(v)
	}
	sessionQueue := int64(defaultSessionQueueBytes)
	if msg > sessionQueue {
		sessionQueue = msg
	}
	if v := strings.TrimSpace(l.SessionQueueBytes); v != "" {
		if n, err := parseBytes(v); err == nil && n > 0 {
			sessionQueue = n
		}
	}
	return Resolved{
		MaxMessageSize:    uint32(msg),
		MaxTopicLength:    *l.MaxTopicLength,
		MaxClientIDLength: *l.MaxClientIDLength,
		MaxHeaderCount:    *l.MaxHeaderCount,
		MaxHeaderBytes:    int(hdr),
		MaxConnections:    *l.MaxConnections,
		MaxSubscriptions:  *l.MaxSubscriptions,
		MaxTopicLevels:    *l.MaxTopicLevels,
		PublishRate:       publishRate(l.PublishRate),
		PublishBytes:      publishBytes(l.PublishBytes),
		MaxSessionExpiry:  uint32(expiry / time.Second),
		MaxKeepalive:      keepaliveSeconds(l.MaxKeepalive),
		WriteTimeout:      writeTimeout(l.WriteTimeout),
		ConnectTimeout:    connect,
		MaxConnectSize:    uint32(connectSize),
		MaxConnectRate:    connectRate,
		SessionQueueBytes: sessionQueue,
	}
}

// publishRate is the per-client publish ceiling, or zero for none.
func publishRate(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

// publishBytes is the byte ceiling, or zero for none.
func publishBytes(v *string) int64 {
	if v == nil {
		return 0
	}
	n, err := parseBytes(*v)
	if err != nil {
		return 0
	}
	return n
}

// writeTimeout is the deadline as a duration, or zero for `none`.
// Validation has already refused anything unparseable.
func writeTimeout(v string) time.Duration {
	if strings.TrimSpace(v) == "none" {
		return 0
	}
	// The same parser the validation above uses. Two parsers for one key is
	// how a value passes --check-config and resolves to something else.
	d, err := parseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0
	}
	return d
}

// keepaliveSeconds is the ceiling in seconds, or zero for none. Validation
// has already refused anything that does not fit.
func keepaliveSeconds(s string) uint16 {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "none") {
		return 0
	}
	d, err := parseWholeSeconds(s)
	if err != nil || d <= 0 || d/time.Second > math.MaxUint16 {
		return 0
	}
	return uint16(d / time.Second)
}

// ParseBytes is exported because an operator writes the same `64KiB` in
// more than one file - limits here, and a client's publish_bytes in the
// acl_file - and two parsers for one syntax is two answers waiting to
// disagree about an edge nobody tested.
func ParseBytes(s string) (int64, error) { return parseBytes(s) }

// parseBytes reads a byte count, with or without a binary suffix. Decimal
// suffixes are deliberately absent: KB meaning 1000 and KiB meaning 1024
// side by side is a source of bugs, and an operator who wants 1000 bytes
// can write 1000.
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	mult := int64(1)
	for suffix, m := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(s, suffix) {
			mult = m
			s = strings.TrimSpace(strings.TrimSuffix(s, suffix))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a byte count such as 512, 8KiB or 1MiB")
	}
	if n <= 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	// **Bounded before it is multiplied**, as parseDuration is. Unbounded,
	// 17179869185GiB wrapped to exactly 1GiB and was accepted: a retention
	// bound an operator wrote as "keep everything" trimmed at a gigabyte.
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("larger than saguin can hold, which is 8EiB")
	}
	return n * mult, nil
}

// validate checks the storage block on its own, before any channel names
// a provider in it.
func (s Storage) validate(reserve, maxConnections int64) []string {
	var findings []string

	// Mandatory. A broker with no provider used to keep channels in memory
	// it had not been told to use and warn at startup, and every store that
	// followed - retained values, unfinished exactly-once publishes - had
	// to be refused on such a broker one block at a time. One rule instead:
	// the operator says where everything is kept, before anything is kept.
	if len(s.Providers) == 0 {
		return append(findings, "broker.storage.providers: define at least one provider, and name it "+
			"in broker.storage.default - a memory provider with snapshot_dir: none keeps nothing "+
			"across a restart, if that is what is wanted")
	}

	names := make([]string, 0, len(s.Providers))
	for name := range s.Providers {
		names = append(names, name)
	}
	sort.Strings(names) // findings must not depend on map iteration order

	for _, name := range names {
		p := s.Providers[name]
		at := "broker.storage.providers." + name

		switch p.Type {
		case "memory":
			switch p.SnapshotDir {
			case "":
				findings = append(findings, fmt.Sprintf(
					"%s: a memory provider states either a snapshot_dir or snapshot_dir: %s. "+
						"There is no default, because a default path invents a filename in your filesystem "+
						"and defaulting to %s makes a durable channel quietly discard everything at shutdown",
					at, SnapshotNone, SnapshotNone))
			case SnapshotNone:
			default:
				if !filepath.IsAbs(p.SnapshotDir) {
					findings = append(findings, fmt.Sprintf(
						"%s.snapshot_dir %q is relative: a snapshot must not depend on where the broker was started from",
						at, p.SnapshotDir))
				}
			}
		case "sqlite":
			switch {
			case p.FilePath == "":
				findings = append(findings, fmt.Sprintf(
					"%s: a sqlite provider states the file_path it keeps its database in. "+
						"There is no default, because inventing a filename in your filesystem is not saguin's to do",
					at))
			case !filepath.IsAbs(p.FilePath):
				findings = append(findings, fmt.Sprintf(
					"%s.file_path %q is relative: a database must not depend on where the broker was started from",
					at, p.FilePath))
			}
			if p.SnapshotDir != "" {
				findings = append(findings, fmt.Sprintf(
					"%s.snapshot_dir belongs to a memory provider. A sqlite provider writes every record as it "+
						"arrives, so there is no snapshot and nothing a directory would hold", at))
			}
			findings = append(findings, p.commitFindings(at, maxConnections)...)
			findings = append(findings, p.flushFindings(at)...)
			if n := p.ReadConnections; n != nil && (*n < 0 || *n > store.SQLiteMaxReadConnections) {
				findings = append(findings, fmt.Sprintf(
					"%s.read_connections is %d: it is 0 for every read on the write connection, or up to %d "+
						"read connections beside it. Past %d a reader waits for a CPU rather than for a "+
						"connection, and each one is a page cache of memory",
					at, *n, store.SQLiteMaxReadConnections, store.SQLiteMaxReadConnections))
			}
		case "":
			findings = append(findings, fmt.Sprintf("%s: no type", at))
		default:
			findings = append(findings, fmt.Sprintf("%s: unknown type %q", at, p.Type))
		}
		if p.MaxBytes != "" {
			switch n, err := parseBytes(p.MaxBytes); {
			case err != nil:
				findings = append(findings, fmt.Sprintf("%s.max_bytes %q: %v", at, p.MaxBytes, err))
			case p.Type == "sqlite" && n < store.SQLiteEmptyBytes+2*reserve:
				// A sqlite file's tables take pages before any record does,
				// and a ceiling at or below them opens a provider that can
				// never hold one: SQLite refuses to set a ceiling below the
				// pages already in use, so it comes up over its own bound.
				findings = append(findings, fmt.Sprintf(
					"%s.max_bytes %q is %d bytes, and a sqlite provider's empty database takes %d before "+
						"any record, beside the %d held back for the operations that free it. Give it at "+
						"least %d, or lower broker.limits.max_message_size",
					at, p.MaxBytes, n, store.SQLiteEmptyBytes, reserve, store.SQLiteEmptyBytes+2*reserve))
			case n < 2*reserve:
				// A provider holds back one largest-possible record so that
				// the operations which free it are never refused. One that
				// cannot also hold a record beyond what it holds back is not
				// a provider, and the number that produced it is almost
				// always a slip.
				findings = append(findings, fmt.Sprintf(
					"%s.max_bytes %q is %d bytes, and a provider holds back %d for the operations that "+
						"free it - one largest message plus its headers. Give it at least %d, or lower "+
						"broker.limits.max_message_size", at, p.MaxBytes, n, reserve, 2*reserve))
			}
		}
		for _, k := range []struct {
			old, now string
			node     yaml.Node
		}{
			{"commit_interval", "publish_commit_interval", p.RetiredCommitInterval},
			{"commit_max_records", "publish_commit_max_records", p.RetiredCommitMaxRecords},
		} {
			if !k.node.IsZero() {
				findings = append(findings, fmt.Sprintf(
					"%s.%s is now %s: it batches this provider's publishes, and "+
						"broker.session.ack_commit_interval is the wait for acknowledgements. Rename the key",
					at, k.old, k.now))
			}
		}
		if p.Type == "memory" && p.PublishCommitInterval != "" {
			findings = append(findings, fmt.Sprintf(
				"%s.publish_commit_interval belongs to a sqlite provider. A memory provider has no transaction to "+
					"collect publishes into, and nothing it holds survives a crash whenever it was written", at))
		}
		if p.Type == "memory" && p.PublishCommitMaxRecords != 0 {
			findings = append(findings, fmt.Sprintf(
				"%s.publish_commit_max_records belongs to a sqlite provider, beside publish_commit_interval", at))
		}
		if p.Type == "memory" && p.FlushInterval != "" {
			findings = append(findings, fmt.Sprintf(
				"%s.flush_interval belongs to a sqlite provider. A memory provider has no write-ahead log "+
					"to force to disk", at))
		}
		if p.Type == "memory" && p.ReadConnections != nil {
			findings = append(findings, fmt.Sprintf(
				"%s.read_connections belongs to a sqlite provider. A memory provider's reads take no "+
					"connection", at))
		}
		if p.Type == "memory" && p.FilePath != "" {
			findings = append(findings, fmt.Sprintf(
				"%s.file_path belongs to a sqlite provider. A memory provider writes a directory of "+
					"snapshot files, one per channel, which is what snapshot_dir names", at))
		}
	}

	findings = append(findings, s.clashes(names)...)

	switch {
	case s.Default == "":
		findings = append(findings, "broker.storage.default: name the provider a channel gets when it does not name one")
	default:
		if _, ok := s.Providers[s.Default]; !ok {
			findings = append(findings, fmt.Sprintf(
				"broker.storage.default names %q, which is not a defined provider", s.Default))
		}
	}

	// Required, exactly as default is, and `none` is a value rather than an omission: a channel that keeps
	// everything says so. Silence here would have to mean one of the two
	// built-in figures RFC 0002 refuses, so silence is the error.
	if s.DefaultRetentionPeriod == "" {
		findings = append(findings, "broker.storage.default_retention_period: state how long a channel "+
			"keeps records when it does not say, or `none` to keep everything")
	} else if _, err := parseRetentionPeriod(s.DefaultRetentionPeriod); err != nil {
		findings = append(findings, fmt.Sprintf("broker.storage.default_retention_period %q: %v",
			s.DefaultRetentionPeriod, err))
	}
	if s.DefaultRetentionBytes == "" {
		findings = append(findings, "broker.storage.default_retention_bytes: state how much a channel "+
			"keeps when it does not say, or `none` to keep everything")
	} else if _, err := parseRetentionBytes(s.DefaultRetentionBytes); err != nil {
		findings = append(findings, fmt.Sprintf("broker.storage.default_retention_bytes %q: %v",
			s.DefaultRetentionBytes, err))
	}

	return findings
}

// retentionDefaults returns the broker-wide fallbacks. Validation has
// already refused a file that does not state both.
func (s Storage) retentionDefaults() (period, bytes int64) {
	period, _ = parseRetentionPeriod(s.DefaultRetentionPeriod)
	bytes, _ = parseRetentionBytes(s.DefaultRetentionBytes)
	return period, bytes
}

// retention reads the four retention keys onto a channel, applying the
// broker-wide defaults where the channel does not say, and reports every
// key that does not belong on this kind of channel.
//
// The three refusals are RFC 0002's, and each is a key that would configure
// something that cannot happen:
//
//   - retention_bytes on a latest channel. It holds one value per topic
//     rather than a history, so what grows there is the number of topics
//     and a byte cap answers a question the channel does not pose.
//   - either key on a queue. Removing unacknowledged work by age or size is
//     eviction of unresolved work, which is never permitted (invariant 2).
//     job_expires_after expires work, max_bytes bounds how much waits, and the
//     two dlq keys govern what it becomes.
//   - either dlq key on anything but a queue. Only a queue derives a
//     dead-letter channel, so elsewhere they name something that does not
//     exist.
func retention(c *channel.Channel, cc ChannelConfig, s Storage) []string {
	var findings []string
	at := fmt.Sprintf("channel %q", c.Name)
	defaultPeriod, defaultBytes := s.retentionDefaults()

	if c.Type == channel.Queue {
		if cc.RetentionPeriod != "" || cc.RetentionBytes != "" {
			findings = append(findings, at+": retention_period and retention_bytes do not apply to a "+
				"queue, which removes records by resolution - deleting unacknowledged work by age or "+
				"size is eviction of unresolved work. Use job_expires_after to expire work, max_bytes to "+
				"bound how much of it waits, and dlq_retention_period and dlq_retention_bytes for "+
				"what it becomes")
		}
		if cc.DeletionRetentionPeriod != "" {
			findings = append(findings, at+": deletion_retention_period applies only to a latest "+
				"channel, which is the only kind that stores a deletion")
		}
	} else if cc.DLQRetentionPeriod != "" || cc.DLQRetentionBytes != "" {
		findings = append(findings, at+": dlq_retention_period and dlq_retention_bytes "+
			"apply only to a queue, which is the only kind of channel that "+
			"derives a dead-letter channel")
	}

	// The channel's own pair. A queue keeps neither, and is not given the
	// default either: it has no age or size rule at all.
	if c.Type != channel.Queue {
		c.RetentionPeriod = defaultPeriod
		if cc.RetentionPeriod != "" {
			n, err := parseRetentionPeriod(cc.RetentionPeriod)
			if err != nil {
				findings = append(findings, fmt.Sprintf("%s: retention_period %q: %v", at, cc.RetentionPeriod, err))
			}
			c.RetentionPeriod = n
		}

		// A deletion's own clock, on the one channel type that stores one.
		if c.Type == channel.Latest {
			c.DeletionRetentionPeriod = DefaultDeletionRetentionPeriod
			if cc.DeletionRetentionPeriod != "" {
				n, err := parseRetentionPeriod(cc.DeletionRetentionPeriod)
				if err != nil {
					findings = append(findings, fmt.Sprintf("%s: deletion_retention_period %q: %v",
						at, cc.DeletionRetentionPeriod, err))
				}
				c.DeletionRetentionPeriod = n
			}
		} else if cc.DeletionRetentionPeriod != "" {
			findings = append(findings, at+": deletion_retention_period applies only to a latest "+
				"channel, which is the only kind that stores a deletion")
		}

		switch {
		case c.Type == channel.Latest && cc.RetentionBytes != "":
			findings = append(findings, at+": retention_bytes does not apply to a latest channel, which "+
				"holds one value per topic rather than a history; retention_period is what removes a "+
				"topic nothing publishes to")
		case c.Type == channel.Latest:
			// Deliberately not given the byte default either: there is no
			// history here for it to trim.
		default:
			c.RetentionBytes = defaultBytes
			if cc.RetentionBytes != "" {
				n, err := parseRetentionBytes(cc.RetentionBytes)
				if err != nil {
					findings = append(findings, fmt.Sprintf("%s: retention_bytes %q: %v", at, cc.RetentionBytes, err))
				}
				c.RetentionBytes = n
			}
		}
	}

	// The derived dead-letter channel's pair, which is an ordinary append
	// channel's and falls back to the same defaults.
	if c.Type == channel.Queue {
		c.DLQRetentionPeriod, c.DLQRetentionBytes = defaultPeriod, defaultBytes
		if cc.DLQRetentionPeriod != "" {
			n, err := parseRetentionPeriod(cc.DLQRetentionPeriod)
			if err != nil {
				findings = append(findings, fmt.Sprintf("%s: dlq_retention_period %q: %v", at, cc.DLQRetentionPeriod, err))
			}
			c.DLQRetentionPeriod = n
		}
		if cc.DLQRetentionBytes != "" {
			n, err := parseRetentionBytes(cc.DLQRetentionBytes)
			if err != nil {
				findings = append(findings, fmt.Sprintf("%s: dlq_retention_bytes %q: %v", at, cc.DLQRetentionBytes, err))
			}
			c.DLQRetentionBytes = n
		}
	}

	return findings
}

// assign records which provider holds a channel, and refuses a channel
// whose records could not be written where that provider keeps them.
func (s Storage) assign(c *channel.Channel, named string) string {
	// No providers at all is Storage.validate's finding; one line per
	// channel repeating it would bury it.
	if len(s.Providers) == 0 {
		return ""
	}

	c.Storage = named
	if c.Storage == "" {
		c.Storage = s.Default
	}
	p, ok := s.Providers[c.Storage]
	if !ok {
		return fmt.Sprintf("channel %q: storage names %q, which is not a defined provider", c.Name, c.Storage)
	}

	if p.SnapshotDir == "" || p.SnapshotDir == SnapshotNone {
		return ""
	}
	// A channel name is not a file name - a dot is legal in one and is
	// encoded in the other, which trebles it. An operator learns that here,
	// where it costs a restart, rather than at the shutdown that would
	// discover it by losing the channel.
	if _, err := store.SnapshotFileName(c.Name); err != nil {
		return fmt.Sprintf("channel %q: %v", c.Name, err)
	}
	return ""
}

// parseDuration reads a duration in milliseconds, seconds, minutes, hours
// or days.
//
// Not time.ParseDuration, because it has no day unit - RFC 0002's
// `retention_period: 7d` would be a startup error and an operator would
// have to write a retention policy in hours.
//
// One number and one unit: `90m`, not `1h30m`. Everything the long form
// can say the short form can say, and one shape is one thing to explain.
//
// Almost nothing may use `ms`. Nearly every duration in a configuration is
// held in whole seconds, and those keys call parseWholeSeconds instead -
// which is where the refusal lives, so that the message can say what is
// wrong rather than that `500ms` is not a duration at all.
func parseDuration(s string) (time.Duration, error) {
	bad := fmt.Errorf("not a duration such as 30s, 5m, 6h or 7d")

	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}

	// `ms` before the single-character units, because it ends in one of
	// them: read right to left, `5ms` is `5m` followed by a stray `s`.
	unit, digits := time.Duration(0), ""
	if rest, ok := strings.CutSuffix(s, "ms"); ok {
		unit, digits = time.Millisecond, rest
	} else {
		u, ok := map[byte]time.Duration{
			's': time.Second,
			'm': time.Minute,
			'h': time.Hour,
			'd': 24 * time.Hour,
		}[s[len(s)-1]]
		if !ok {
			return 0, bad
		}
		unit, digits = u, s[:len(s)-1]
	}

	n, err := strconv.ParseInt(strings.TrimSpace(digits), 10, 64)
	if err != nil {
		return 0, bad
	}
	if n <= 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	if n > int64(math.MaxInt64)/int64(unit) {
		return 0, fmt.Errorf("longer than saguin can hold, which is about 292 years")
	}
	return time.Duration(n) * unit, nil
}

// parseWholeSeconds reads a duration that must be a whole number of
// seconds, which is nearly every duration saguin accepts.
//
// The refusal belongs here rather than in parseDuration so that the message
// names what is actually wrong. A key that held `500ms` would report it as
// "not a duration such as 30s, 5m, 6h or 7d", which is true of the form and
// useless as a diagnosis: it sends an operator looking for a typo in
// something they spelled correctly.
//
// What it prevents is worse than the message. The value would otherwise be
// divided into whole seconds and come out as zero, and zero is not "a very
// short interval" at any of these call sites - it is a sentinel meaning
// something else entirely, and a different something at each one:
//
//   - A retention period of zero is what `none` parses to, and sweepByAge
//     skips a channel holding it. So `retention_period: 500ms` would mean
//     **keep everything, for ever** - the opposite of what was asked, and
//     invariant 13's failure rather than a short retention.
//   - A visibility timeout of zero leases a queue record for no time at all,
//     so it is immediately visible again: a redelivery loop rather than a
//     tight timeout.
//
// Neither is reachable, because parseDuration already refuses a written
// zero and this refuses everything that would round to one. Both are
// written down because "it comes out as zero" sounds harmless and is not.
//
// Four keys are fine-grained enough to want the other answer, and each
// calls parseDuration directly: a bridge's ack_interval, a sqlite
// provider's publish_commit_interval, broker.session.ack_commit_interval
// and limits.write_timeout (RFC 0002 "Validation").
func parseWholeSeconds(s string) (time.Duration, error) {
	d, err := parseDuration(s)
	if err != nil {
		return 0, err
	}
	if d%time.Second != 0 {
		return 0, fmt.Errorf(
			"is held in whole seconds, so %s cannot be honoured; write 1s or longer", s)
	}
	return d, nil
}

// ProtocolVersion is an MQTT version as an operator writes it.
//
// **A string that YAML may hand over as a number.** `3.1.1` is not a valid
// number so YAML reads it as text whatever you do, while `5` is an integer
// unless it is quoted - so a plain `string` field refuses half the file and
// an `int` refuses the other half. Taking the scalar's own text accepts
// both, which is what lets RFC 0002 write both quoted and a reader write
// either.
type ProtocolVersion string

// UnmarshalYAML takes the scalar as written.
func (p *ProtocolVersion) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("must be a version, written %q or %q", "3.1.1", "5")
	}
	*p = ProtocolVersion(n.Value)
	return nil
}

// minProtocolLevels maps what an operator writes to the protocol level the
// wire carries. **MQTT 3.1 is absent and that is the rule rather than an
// omission**: its SUBACK has no failure code and its CONNACK no Session
// Present flag, so a subscription saguin must refuse could only be granted
// and a consumer whose position was lost could not be told (RFC 0002).
var minProtocolLevels = map[ProtocolVersion]byte{"3.1.1": 4, "5": 5}

// MinProtocol is the protocol level below which a CONNECT is refused.
func (m MQTT) MinProtocol() byte {
	if m.MinProtocolVersion == nil {
		return 4
	}
	return minProtocolLevels[*m.MinProtocolVersion]
}

type MQTT struct {
	Listen Listen `yaml:"listen"`

	// PasswordFile holds the clients that may connect, in Mosquitto's
	// format, and it is **not** the file the operators are in
	// (broker.operations.password_file). An operator is not a device: one
	// file would mean every device that can publish could also read the
	// channel names, their volumes and their consumers' positions.
	//
	PasswordFile string `yaml:"password_file"`

	// AllowAnonymous says whether a client that offers no user name is
	// admitted, and it is a pointer so that a written `false` is not the
	// same as an absent key.
	//
	// **Absent takes its answer from the password file**: no file means
	// there is nothing to authenticate against and every client is
	// admitted; a file means the operator has said who may connect, and a
	// client offering nothing is not one of them. Written, it wins either
	// way - `true` beside a password file is Mosquitto's mixed mode, where
	// named clients authenticate and the rest are let in anyway.
	//
	// `false` with no password file is refused at startup: it is a broker
	// nothing can connect to, which is a configuration nobody means.
	AllowAnonymous *bool `yaml:"allow_anonymous"`

	// MinProtocolVersion is the oldest MQTT version this broker admits:
	// `3.1.1` - the default - or `5`. It is broker-wide rather than
	// per-listener because the version a device speaks is a property of the
	// device rather than of the door it arrives at (RFC 0002 "Which
	// versions may connect").
	//
	// **Admission is a decision an operator makes, not a side effect of
	// upgrading.** Without this key, the release that lowers the gate would
	// start admitting devices the release before it turned away, with
	// nothing in the configuration recording that anybody chose it. The
	// default is open - a differentiator behind a setting is not a
	// differentiator - and the key is here so the earlier promise is one
	// line away rather than a version pin.
	MinProtocolVersion *ProtocolVersion `yaml:"min_protocol_version"`

	// ACLFile names what each client may do, once it has been let in. It is
	// a different file from PasswordFile so that the password file stays
	// Mosquitto's format hash for hash, which is what makes an existing
	// fleet's credentials reusable.
	//
	// **Absent means every authenticated client may do anything**, which is
	// what saguin did before this key existed - presence is the switch, as
	// it is for the password file, TLS and the client authority.
	//
	// It requires something that authenticates a client - a password file,
	// or a listener's client_ca_file - and is refused at startup with
	// neither: authorization is a statement about an identity, and with
	// nothing to authenticate against there is no identity to make it about.
	ACLFile string `yaml:"acl_file"`
}

// ListenerAuth is the resolved authentication for each listener this
// configuration opens, keyed by the id the listener is created under -
// `tcp`, `ws` and `unix`. A listener that is not configured is absent.
//
// One function rather than the resolution written out at each listener,
// because the two callers that need it - the startup that loads the files
// and the check that opens them - must agree exactly. Two copies of this is
// how `--check-config` comes to pass a configuration the broker refuses.
func (m MQTT) ListenerAuth() map[string]Auth {
	out := map[string]Auth{}
	for _, d := range m.Listen.TCP {
		out[d.Name] = d.Auth
	}
	for _, d := range m.Listen.WS {
		out[d.Name] = d.Auth
	}
	for _, d := range m.Listen.Unix {
		out[d.Name] = d.Auth
	}
	return out
}

// listenPath is DoorPath generalised over which block's doors it names -
// broker.mqtt.listen or broker.operations.listen, the two lists this
// scheme applies to (RFC 0002 "Several listeners of a kind").
func listenPath(prefix, kind, name string) string {
	if name == kind {
		return prefix + "." + kind
	}
	return prefix + "." + kind + "[" + name + "]"
}

// DoorPath is how an MQTT door is named in a finding, a log line or a TLS
// error: `broker.mqtt.listen.tcp` for the door a single map names "tcp",
// and `broker.mqtt.listen.tcp[fleet]` once a name says which of several.
func DoorPath(kind, name string) string { return listenPath("broker.mqtt.listen", kind, name) }

// OpsDoorPath is DoorPath for an operations door.
func OpsDoorPath(kind, name string) string {
	return listenPath("broker.operations.listen", kind, name)
}

// listenAt names a door in a finding by its path once it has a name, or by
// its position - broker.mqtt.listen.tcp[2] - for the door doorClashes is in
// the middle of saying needs one, where a name is not yet there to use.
func listenAt(prefix, kind, name string, index int) string {
	if name != "" {
		return listenPath(prefix, kind, name)
	}
	return fmt.Sprintf("%s.%s[%d]", prefix, kind, index+1)
}

func doorAt(kind, name string, index int) string {
	return listenAt("broker.mqtt.listen", kind, name, index)
}

func opsDoorAt(kind, name string, index int) string {
	return listenAt("broker.operations.listen", kind, name, index)
}

// doorEntry is one door doorClashes compares, whichever block it came from.
type doorEntry struct{ kind, name, at, address, path string }

// namesClash refuses, within one closed set of doors, a second door of a
// kind with no name of its own and two doors sharing one name - whatever
// their kind. Shared by the MQTT doors and the operations doors, which are
// two separate sets: an MQTT door's name is what SetListenerCredentials,
// a certificate error and /v1/operations/users key on, and an operations
// door's name is what Operators keys on - two id spaces belonging to two
// unrelated listeners, each of which still has to make sense on its own.
// `what` names the set in the finding.
func namesClash(doors []doorEntry, what string) []string {
	var findings []string
	perKind := map[string]int{}
	for _, d := range doors {
		perKind[d.kind]++
	}
	for _, d := range doors {
		if d.name == "" && perKind[d.kind] > 1 {
			findings = append(findings, fmt.Sprintf(
				"%s: a second %s door, and this one names none - each door needs its own "+
					"name once there is more than one of a kind", d.at, d.kind))
		}
	}
	for i := 0; i < len(doors); i++ {
		for j := i + 1; j < len(doors); j++ {
			a, b := doors[i], doors[j]
			if a.name != "" && a.name == b.name {
				findings = append(findings, fmt.Sprintf(
					"%s and %s are both named %q: a door's name is what tells it apart in a "+
						"log line and a certificate error, so it must be unique among %s",
					a.at, b.at, a.name, what))
			}
		}
	}
	return findings
}

// doorClashes is --check-config's refusal by name (RFC 0002 "Several
// listeners of a kind"): a second door of a kind with no name of its own,
// two doors sharing one name, two doors that would try to bind the same
// port, and two Unix doors at the same path. Called from Load, so the same
// file that trips this at --check-config never gets far enough at startup
// to bind half its doors either - the two share one check rather than
// agreeing by construction.
func doorClashes(f *File) []string {
	var findings []string
	l := f.Broker.MQTT.Listen

	var mqttDoors []doorEntry
	for i, d := range l.TCP {
		mqttDoors = append(mqttDoors,
			doorEntry{"tcp", d.Name, doorAt("tcp", d.Name, i), d.Address.Address, ""})
	}
	for i, d := range l.WS {
		mqttDoors = append(mqttDoors,
			doorEntry{"ws", d.Name, doorAt("ws", d.Name, i), d.Address.Address, ""})
	}
	for i, d := range l.Unix {
		mqttDoors = append(mqttDoors,
			doorEntry{"unix", d.Name, doorAt("unix", d.Name, i), "", d.Path})
	}
	findings = append(findings, namesClash(mqttDoors, "every MQTT door")...)

	// **The operations doors are their own, separate id space**, checked
	// among themselves rather than against the MQTT ones: an operations
	// door names nothing an MQTT door does - not SetListenerCredentials,
	// not a route an MQTT client reaches - it only keys the operations
	// listener's own credential map (internal/broker.Operators) and its
	// own log lines and certificate errors, which an MQTT door never
	// touches. A tcp door named "fleet" beside an operations tcp door
	// also named "fleet" is two unrelated things sharing a word, not a
	// collision - so a deployment naming both after what they are for,
	// the same word on purpose, is not refused for it.
	var opsDoors []doorEntry
	if o := f.Broker.Operations; o != nil {
		for i, d := range o.Listen.TCP {
			opsDoors = append(opsDoors,
				doorEntry{"tcp", d.Name, opsDoorAt("tcp", d.Name, i), d.Address.Address, ""})
		}
		for i, d := range o.Listen.Unix {
			opsDoors = append(opsDoors,
				doorEntry{"unix", d.Name, opsDoorAt("unix", d.Name, i), "", d.Path})
		}
	}
	findings = append(findings, namesClash(opsDoors, "the operations doors")...)

	allDoors := make([]doorEntry, 0, len(mqttDoors)+len(opsDoors))
	allDoors = append(allDoors, mqttDoors...)
	allDoors = append(allDoors, opsDoors...)

	// **Every TCP-speaking door shares one address space - tcp, ws, and
	// the operations port - because the kernel's port table does, whatever
	// two configuration blocks think they are.** Two of them bound to the
	// same host and port is one `bind: address already in use`, discovered
	// on whichever starts second; a wildcard host binds every interface,
	// so it clashes with any other host on the same port too.
	type tcpish struct{ at, address string }
	var tcps []tcpish
	for _, d := range allDoors {
		if strings.TrimSpace(d.address) != "" {
			tcps = append(tcps, tcpish{d.at, d.address})
		}
	}
	for i := 0; i < len(tcps); i++ {
		for j := i + 1; j < len(tcps); j++ {
			if portClash(tcps[i].address, tcps[j].address) {
				findings = append(findings, fmt.Sprintf(
					"%s (%s) and %s (%s): the same port, on hosts that are equal or a "+
						"wildcard - only one of them can bind it",
					tcps[i].at, tcps[i].address, tcps[j].at, tcps[j].address))
			}
		}
	}

	// **Two Unix doors at one path is the same failure, the socket-file
	// kind**: whichever starts second finds the path already taken. The
	// MQTT doors and the operations socket share one filesystem.
	type unixish struct{ at, path string }
	var socks []unixish
	for _, d := range allDoors {
		if strings.TrimSpace(d.path) != "" {
			socks = append(socks, unixish{d.at, d.path})
		}
	}
	for i := 0; i < len(socks); i++ {
		for j := i + 1; j < len(socks); j++ {
			if samePath(socks[i].path, socks[j].path) {
				findings = append(findings, fmt.Sprintf(
					"%s and %s: the same path %q - only one of them can bind it",
					socks[i].at, socks[j].at, socks[i].path))
			}
		}
	}

	return findings
}

// samePath reports whether two socket paths name one file: compared as
// cleaned absolute paths, so X/r.sock, X/./r.sock and X/a/../r.sock clash.
// Symlinks are not resolved: that needs the filesystem, which
// --check-config does not touch.
func samePath(a, b string) bool {
	return cleanPath(a) == cleanPath(b)
}

func cleanPath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return filepath.Clean(p)
}

// portClash reports whether two addresses would fight over one port: the
// same port, and hosts that are equal or that either is a wildcard -
// 0.0.0.0, ::, or empty, which all mean every interface. An address that
// does not parse is not compared here: it is its own finding elsewhere,
// and guessing in the permissive direction would refuse a typo for the
// wrong reason.
func portClash(a, b string) bool {
	ha, sa, erra := net.SplitHostPort(a)
	hb, sb, errb := net.SplitHostPort(b)
	if erra != nil || errb != nil {
		return false
	}
	// Compared as numbers, the way the listener reads them: 032010 and
	// +32010 are port 32010. A port that does not resolve is its own
	// finding (checkListenAddress), not a clash.
	na, erra := net.LookupPort("tcp", sa)
	nb, errb := net.LookupPort("tcp", sb)
	if erra != nil || errb != nil || na != nb {
		return false
	}
	pa := strconv.Itoa(na)
	// Port 0 asks the kernel for whichever free port it has, a different
	// one for each listener that asks - the address every test in this
	// tree that wants no fixed port uses, and never a real clash.
	if pa == "0" {
		return false
	}
	return sameHost(ha, hb) || isWildcardHost(ha) || isWildcardHost(hb)
}

// checkListenAddress refuses a tcp or ws address net.Listen would refuse,
// judged the way it reads one: net.SplitHostPort, then net.LookupPort for
// the port - a number 0-65535 (leading zeros and a + are numbers), or a
// service name /etc/services knows, which is why that file is read here.
// An empty port is the kernel's pick, as at bind. A host name is not
// resolved (no DNS at check time), but one holding white space cannot
// resolve, so it is refused.
func checkListenAddress(address, where string) []string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return []string{fmt.Sprintf("%s: address %q: %v - write host:port, such as 0.0.0.0:1883, "+
			"with an IPv6 host in brackets", where, address, err)}
	}
	if _, err := net.LookupPort("tcp", port); err != nil {
		return []string{fmt.Sprintf("%s: address %q: port %q is not a number from 0 to 65535", where, address, port)}
	}
	if msg := checkListenHost(host); msg != "" {
		return []string{fmt.Sprintf("%s: address %q: host %q %s", where, address, host, msg)}
	}
	return nil
}

// checkListenHost is an allowlist of what a start can bind, "" if host is
// one: empty (every interface); an IP literal netip accepts, whose zone if
// written is not empty; or a host name in RFC 1123 syntax. A name is not
// resolved here (no DNS at check time), so a well-formed name that does not
// resolve is left to the bind. Two literals are refused because no bind of
// them ever succeeds, measured on the real listener: an IPv6 multicast
// address, and an IPv6 link-local unicast one with no zone.
func checkListenHost(host string) string {
	if host == "" {
		return ""
	}
	if strings.ContainsAny(host, ":%") {
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return "is not an IP literal: " + err.Error()
		}
		if ip.Is6() && !ip.Is4In6() {
			if ip.IsMulticast() {
				return "is an IPv6 multicast address, which cannot be bound"
			}
			if ip.IsLinkLocalUnicast() && ip.Zone() == "" {
				return "is an IPv6 link-local address with no zone: write it as fe80::1%eth0"
			}
		}
		return ""
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return ""
	}
	name := strings.TrimSuffix(host, ".")
	if name == "" || len(name) > 253 {
		return "is not a host name: 1 to 253 characters"
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "is not a host name: labels are 1 to 63 letters, digits and hyphens, none starting or ending in a hyphen"
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return "is not a host name or an IP literal: a name is letters, digits, hyphens and dots (RFC 1123)"
			}
		}
	}
	return ""
}

// hostKey is a host as the kernel tells it apart: an IP address, with the
// zone dropped - Linux ignores a zone on an address that is not link-local,
// so [::1%lo] is [::1] (measured: the second bind is "address already in
// use"). On a link-local or multicast address the zone picks the interface,
// so two zones are two sockets and it is kept.
func hostKey(h string) (netip.Addr, bool) {
	ip, err := netip.ParseAddr(h)
	if err != nil {
		return netip.Addr{}, false
	}
	ip = ip.Unmap()
	if !ip.IsLinkLocalUnicast() && !ip.IsMulticast() && !ip.IsInterfaceLocalMulticast() {
		ip = ip.WithZone("")
	}
	return ip, true
}

// sameHost reports whether two hosts are one. IP literals are compared as
// addresses, so 0:0:0:0:0:0:0:1 equals ::1 and ::ffff:127.0.0.1 equals
// 127.0.0.1. Names are compared as written: whether `localhost` is
// 127.0.0.1 is the resolver's and /etc/hosts' answer, which --check-config
// does not ask (no DNS at check time), so such a clash is left to the bind.
func sameHost(a, b string) bool {
	if a == b {
		return true
	}
	ia, oka := hostKey(a)
	ib, okb := hostKey(b)
	return oka && okb && ia == ib
}

// isWildcardHost reports whether host binds every interface rather than
// one.
func isWildcardHost(host string) bool {
	if host == "" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.WithZone("").Unmap().IsUnspecified()
}

// AnonymousBesideACL is every listener that, beside an acl_file, admits a
// client nothing identifies: its credentials resolve to anonymous - no
// password file, or allow_anonymous - and it does not require a client
// certificate. Such a client's identity is the empty name, which only a
// `*` pattern matches, so it is granted whatever `*` grants.
//
// **Said, not refused.** It is the failure the allow_anonymous refusal
// exists for, reached without anybody writing allow_anonymous: a
// pure-certificate estate's Unix socket, a plain ws door, or a tcp door
// with require_certificate: false and no password file. Whether to refuse
// it is the maintainer's; until then the start and --check-config name each
// such door. Sorted, for the reason every summary here is.
func (m MQTT) AnonymousBesideACL() []string {
	if strings.TrimSpace(m.ACLFile) == "" {
		return nil
	}
	certs := m.ListenerCertificates()
	var out []string
	for id, a := range m.ListenerAuth() {
		if _, anonymous := a.Resolve(m); anonymous && certs[id] != CertRequired {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// listenerAuthSite is one listener's Auth and the configuration path to name
// in a finding about it.
type listenerAuthSite struct {
	where string
	auth  Auth
}

// sortedListeners is every configured listener, in a fixed order so an
// operator fixing three findings is not handed them shuffled.
func sortedListeners(m MQTT) []listenerAuthSite {
	var out []listenerAuthSite
	for _, d := range m.Listen.TCP {
		out = append(out, listenerAuthSite{DoorPath("tcp", d.Name), d.Auth})
	}
	for _, d := range m.Listen.Unix {
		out = append(out, listenerAuthSite{DoorPath("unix", d.Name), d.Auth})
	}
	for _, d := range m.Listen.WS {
		out = append(out, listenerAuthSite{DoorPath("ws", d.Name), d.Auth})
	}
	return out
}

// Anonymous reports whether a client offering no user name is admitted.
func (m MQTT) Anonymous() bool {
	if m.AllowAnonymous != nil {
		return *m.AllowAnonymous
	}
	return strings.TrimSpace(m.PasswordFile) == ""
}

// Listen is where the broker accepts connections: any of the three kinds,
// or - when none is given - TCP on the default port.
//
// **Each kind is a list of doors rather than one block.** An operator with
// one door of a kind writes it exactly as before - a map, named after its
// kind (`tcp`, `ws`, `unix`) - and every existing file, log line and route
// answer is unchanged. An operator wanting a second door of a kind, plain
// beside TLS on `tcp` for instance, writes a list instead, and each entry
// carries its own `name`: the id logs, credentials, TLS errors and
// `/v1/operations/users` key on.
//
// Each kind is a block of its own rather than a bare address, so that a
// setting belonging to one cannot be written beside another. That is also
// why there is no rule here refusing a socket mode with no socket to apply
// it to: there is nowhere left to write one.
type Listen struct {
	TCP  []TCPDoor
	WS   []WSDoor
	Unix []UnixDoor
}

// TCPDoor is one `tcp` door: its address, TLS and auth, plus the name it
// is known by.
type TCPDoor struct {
	// Name is the door's id: what logs, credentials, TLS errors and
	// `/v1/operations/users` key it by. The single map form defaults it to
	// "tcp", so a door nobody named reads exactly as it always has; a list
	// entry with no name is refused once there is a second door to confuse
	// it with (--check-config, a later commit).
	Name    string `yaml:"name"`
	Address `yaml:",inline"`
}

// WSDoor is one `ws` door, named the way a TCPDoor is.
type WSDoor struct {
	Name       string `yaml:"name"`
	WSListener `yaml:",inline"`
}

// UnixDoor is one `unix` door, named the way a TCPDoor is.
type UnixDoor struct {
	Name       string `yaml:"name"`
	UnixSocket `yaml:",inline"`
}

// UnmarshalYAML reads `broker.mqtt.listen`: each of `tcp`, `ws` and `unix`
// as today's single mapping, or as a list of named doors. Unknown keys are
// still refused - a custom UnmarshalYAML on Listen turns off the KnownFields
// check for everything inside it, so each piece is strictly re-decoded, the
// same technique strictGroup and RetainedBlock use and for the same reason.
func (l *Listen) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("broker.mqtt.listen: write tcp, ws or unix under it")
	}
	var shadow struct {
		TCP  yaml.Node `yaml:"tcp"`
		WS   yaml.Node `yaml:"ws"`
		Unix yaml.Node `yaml:"unix"`
	}
	if err := decodeNode(n, &shadow); err != nil {
		return fmt.Errorf("broker.mqtt.listen: %w", err)
	}
	tcp, err := decodeTCPDoors(shadow.TCP, "broker.mqtt.listen.tcp")
	if err != nil {
		return err
	}
	ws, err := decodeWSDoors(shadow.WS)
	if err != nil {
		return err
	}
	unix, err := decodeUnixDoors(shadow.Unix, "broker.mqtt.listen.unix")
	if err != nil {
		return err
	}
	l.TCP, l.WS, l.Unix = tcp, ws, unix
	return nil
}

// MarshalYAML writes back the shape UnmarshalYAML read: a lone door named
// after its kind as the bare mapping, so a single-door configuration
// flattens to exactly what it was; two or more doors as a list.
//
// TestTheFlattenedConfigurationLoadsAgain is what holds this to account:
// the document this writes has to parse back into the same File.
func (l Listen) MarshalYAML() (any, error) {
	out := map[string]any{}
	if v := marshalTCPDoors(l.TCP); v != nil {
		out["tcp"] = v
	}
	if v := marshalWSDoors(l.WS); v != nil {
		out["ws"] = v
	}
	if v := marshalUnixDoors(l.Unix); v != nil {
		out["unix"] = v
	}
	return out, nil
}

// decodeNode strictly decodes a parsed node into v, refusing an unknown
// key. A yaml.Node decodes without KnownFields on its own, so this is the
// marshal-then-strict-decode round trip strictGroup and RetainedBlock use.
func decodeNode(n *yaml.Node, v any) error {
	b, err := yaml.Marshal(n)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// decodeTCPDoors reads a `tcp:` key, at either broker.mqtt.listen or
// broker.operations.listen - path names which, for its errors: absent
// (n.Kind == 0), written null - which means the same as absent, as it did
// before a door could be a list - the single map form (one door named
// "tcp"), or a list of named doors.
func decodeTCPDoors(n yaml.Node, path string) ([]TCPDoor, error) {
	bad := fmt.Errorf(
		"%s: write the keys of a tcp door under it, or a list of named doors sharing "+
			"this port kind", path)
	switch n.Kind {
	case 0:
		return nil, nil
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil, nil
		}
		return nil, bad
	case yaml.MappingNode:
		var a Address
		if err := decodeNode(&n, &a); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return []TCPDoor{{Name: "tcp", Address: a}}, nil
	case yaml.SequenceNode:
		var doors []TCPDoor
		if err := decodeNode(&n, &doors); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		// A list of one is a single door, named after its kind like the
		// mapping form: resolved here, once, so --check-config and startup
		// see the same name (RFC 0002 "Several listeners of a kind").
		if len(doors) == 1 && doors[0].Name == "" {
			doors[0].Name = "tcp"
		}
		return doors, nil
	default:
		return nil, bad
	}
}

// decodeWSDoors is decodeTCPDoors for broker.mqtt.listen.ws, which has no
// counterpart on the operations listener: WebSocket exists because a
// browser cannot open a raw MQTT socket, and nothing about /health and
// /metrics needs it.
func decodeWSDoors(n yaml.Node) ([]WSDoor, error) {
	const path = "broker.mqtt.listen.ws"
	bad := fmt.Errorf(
		"%s: write the keys of a ws door under it, or a list of named doors sharing "+
			"this port kind", path)
	switch n.Kind {
	case 0:
		return nil, nil
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil, nil
		}
		return nil, bad
	case yaml.MappingNode:
		var w WSListener
		if err := decodeNode(&n, &w); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return []WSDoor{{Name: "ws", WSListener: w}}, nil
	case yaml.SequenceNode:
		var doors []WSDoor
		if err := decodeNode(&n, &doors); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		// A list of one is a single door, named after its kind like the
		// mapping form: resolved here, once, so --check-config and startup
		// see the same name (RFC 0002 "Several listeners of a kind").
		if len(doors) == 1 && doors[0].Name == "" {
			doors[0].Name = "ws"
		}
		return doors, nil
	default:
		return nil, bad
	}
}

// decodeUnixDoors is decodeTCPDoors for `unix:`, at either listen block.
func decodeUnixDoors(n yaml.Node, path string) ([]UnixDoor, error) {
	bad := fmt.Errorf(
		"%s: write the keys of a unix door under it, or a list of named doors sharing "+
			"this port kind", path)
	switch n.Kind {
	case 0:
		return nil, nil
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil, nil
		}
		return nil, bad
	case yaml.MappingNode:
		var u UnixSocket
		if err := decodeNode(&n, &u); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return []UnixDoor{{Name: "unix", UnixSocket: u}}, nil
	case yaml.SequenceNode:
		var doors []UnixDoor
		if err := decodeNode(&n, &doors); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		// A list of one is a single door, named after its kind like the
		// mapping form: resolved here, once, so --check-config and startup
		// see the same name (RFC 0002 "Several listeners of a kind").
		if len(doors) == 1 && doors[0].Name == "" {
			doors[0].Name = "unix"
		}
		return doors, nil
	default:
		return nil, bad
	}
}

// marshalTCPDoors is the tcp half of Listen.MarshalYAML.
func marshalTCPDoors(doors []TCPDoor) any {
	if len(doors) == 0 {
		return nil
	}
	if len(doors) == 1 && doors[0].Name == "tcp" {
		return doors[0].Address
	}
	return doors
}

// marshalWSDoors is the ws half of Listen.MarshalYAML.
func marshalWSDoors(doors []WSDoor) any {
	if len(doors) == 0 {
		return nil
	}
	if len(doors) == 1 && doors[0].Name == "ws" {
		return doors[0].WSListener
	}
	return doors
}

// marshalUnixDoors is the unix half of Listen.MarshalYAML.
func marshalUnixDoors(doors []UnixDoor) any {
	if len(doors) == 0 {
		return nil
	}
	if len(doors) == 1 && doors[0].Name == "unix" {
		return doors[0].UnixSocket
	}
	return doors
}

// mutualTLSAnywhere reports whether any listener checks client
// certificates, and so whether a certificate's Common Name can be the
// identity an acl_file rule is about.
//
// Asked of the whole file rather than per listener because an acl_file is
// broker-wide: one mutually-authenticated door is enough for the rules to
// have somebody to govern, and a second door with no authentication is
// refused on its own account elsewhere.
func (f *File) mutualTLSAnywhere() bool {
	l := f.Broker.MQTT.Listen
	for _, d := range l.TCP {
		if d.TLS.MutualTLS() {
			return true
		}
	}
	for _, d := range l.WS {
		if d.TLS.MutualTLS() {
			return true
		}
	}
	return false
}

// Address is a listener that binds to a host and port, and optionally
// serves TLS on it.
type Address struct {
	Address string `yaml:"address"`
	TLS     *TLS   `yaml:"tls"`
	Auth    `yaml:",inline"`
}

// WSListener is the ws listener: an Address, and the web pages that may open
// it. A type of its own so that allowed_origins cannot be written on the tcp
// listener, where no browser can arrive.
type WSListener struct {
	Address `yaml:",inline"`
	// SameOrigin requires a page's scheme, host and port to be the request's
	// own, as NATS's websocket same_origin does. A pointer, because absent is
	// on - where NATS defaults it off - and only a written false turns it off.
	// Re-read on SIGUSR1.
	SameOrigin *bool `yaml:"same_origin"`
	// AllowedOrigins requires a page's origin to be one of these -
	// `https://dashboard.example.com` - as NATS's allowed_origins does. With
	// same_origin on as well, a page must pass both. Absent checks nothing.
	// Re-read on SIGUSR1.
	AllowedOrigins []string `yaml:"allowed_origins"`
}

// SameOriginOn is same_origin as the listener applies it: on unless the file
// writes false.
func (w *WSListener) SameOriginOn() bool {
	return w == nil || w.SameOrigin == nil || *w.SameOrigin
}

// Addr is the listener's Address, or nil for a ws listener that is not
// configured - so a caller walking every listener needs no nil check.
func (w *WSListener) Addr() *Address {
	if w == nil {
		return nil
	}
	return &w.Address
}

// Origins is allowed_origins in the form the listener compares them in.
// Validation has already refused anything that is not an origin.
func (w *WSListener) Origins() []string {
	if w == nil {
		return nil
	}
	var out []string
	for _, o := range w.AllowedOrigins {
		if n, err := listeners.NormalizeOrigin(o); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// Auth is who may connect on one listener, overriding what
// `broker.mqtt` says for the broker as a whole.
//
// **A default with an override, rather than keys that live only here.** The
// common deployment has one answer for the whole broker and writes it once;
// the deployment this exists for has two, and the one that differs is
// usually the Unix socket - a file whose permissions already decide who may
// reach it at all, serving a local tool that has no password to keep,
// beside a TCP port facing a fleet that must have one.
//
// Mosquitto reached the same place from the other direction and it is worth
// not repeating: its keys were broker-wide first, so per-listener values
// needed `per_listener_settings` to say which of the two a file meant. That
// flag exists because the shape changed after the fact. Here a listener's
// value simply wins where it is written, and there is nothing to switch.
type Auth struct {
	// PasswordFile is this listener's, or empty to use the broker's.
	PasswordFile string `yaml:"password_file"`

	// AllowAnonymous is this listener's answer, or absent to use the
	// broker's. A pointer for the reason the broker-wide one is: a written
	// `false` is not the same as an absent key, and defaulting one to the
	// other would silently open or close a listener.
	AllowAnonymous *bool `yaml:"allow_anonymous"`
}

// Resolve returns the password file and anonymous answer for a listener,
// given what the broker says. A listener that writes neither key gets the
// broker's pair unchanged.
//
// **`allow_anonymous` follows whichever password file is in force**, not
// the broker's. A listener naming its own file and saying nothing about
// anonymous connections means "these users, and nobody else" - taking the
// broker's `true` there would admit everyone to the listener that had just
// been given a credential list, which is the opposite of what writing one
// says.
func (a Auth) Resolve(broker MQTT) (passwordFile string, anonymous bool) {
	passwordFile = strings.TrimSpace(a.PasswordFile)
	own := passwordFile != ""
	if !own {
		passwordFile = strings.TrimSpace(broker.PasswordFile)
	}
	switch {
	case a.AllowAnonymous != nil:
		return passwordFile, *a.AllowAnonymous
	case own:
		// Its own users and no others, until it says otherwise.
		return passwordFile, false
	default:
		return passwordFile, broker.Anonymous()
	}
}

// TLS is a server certificate and its key, for one listener.
//
// **Per listener rather than per broker**, because the two listeners answer
// different people: the MQTT port faces a fleet whose certificate is
// whatever their estate issues, and the operations port faces a monitoring
// system that is often somewhere else entirely. One certificate for both
// would be one name for both, and a deployment that has both would have to
// pick which of them the name is wrong for.
//
// **There is no default and no self-signed fallback.** A broker that
// generated its own certificate would be offering encryption that no client
// can verify, and every client would be told to skip the check - which
// looks secured and is not, and is worse than plain text on a trusted link
// because it stops anybody asking.
type TLS struct {
	// CertFile holds the chain this listener presents, not only the leaf:
	// every certificate in it is sent. A leaf issued by an intermediate is
	// `cat leaf.pem intermediate.pem > cert.pem`, the same way nginx's
	// ssl_certificate works, and sending only the leaf fails on the
	// client's machine rather than this one.
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	// MinVersion is the lowest TLS version this listener accepts, "1.2" or
	// "1.3", and 1.2 when it is not written.
	//
	// **It is a floor rather than Mosquitto's `tls_version`, which pins one
	// version**, and the difference is which mistake each one makes easy.
	// A pin excludes clients that speak something newer, which is the
	// wrong way round for a fleet that upgrades over years. A floor only
	// ever excludes what is older than the operator asked for.
	//
	// There is no setting below 1.2. An estate where every client is modern
	// has a real reason to require 1.3 - that is what this key is for - and
	// nobody has a good reason to accept 1.0.
	MinVersion string `yaml:"min_version"`

	// ClientCAFile turns on mutual TLS: the authorities trusted when
	// checking a certificate a *client* presents, which is what Mosquitto's
	// `cafile` names. Absent, no client certificate is asked for or looked
	// at, and the listener is ordinary TLS.
	//
	// **The certificate's Common Name becomes the client's user name**,
	// which is Mosquitto's `use_identity_as_username` and is not optional
	// here. A certificate is a name; having it authenticate a connection
	// and then leave the name to a separate field would give one client two
	// identities and saguin two answers to "who published this".
	//
	// So a client presenting a certificate is authenticated by it, and the
	// password file is not consulted for that client at all: the CA has
	// already made the statement the password file exists to make.
	ClientCAFile string `yaml:"client_ca_file"`

	// RequireCertificate says whether every client must present one, and it
	// is a pointer so that a written `false` is not the same as an absent
	// key. Absent means required, which is the only reading of a configured
	// CA that is not a hole - Mosquitto's `require_certificate`, defaulted
	// the safe way round.
	//
	// `false` is the mixed mode a fleet migrating in batches needs: a client
	// with a certificate is authenticated by it, one without falls through
	// to the password file and `allow_anonymous`.
	RequireCertificate *bool `yaml:"require_certificate"`
}

// MutualTLS reports whether this listener checks client certificates.
func (t *TLS) MutualTLS() bool {
	return t != nil && strings.TrimSpace(t.ClientCAFile) != ""
}

// CertificateRequired reports whether every client must present one.
func (t *TLS) CertificateRequired() bool {
	if !t.MutualTLS() {
		return false
	}
	if t.RequireCertificate != nil {
		return *t.RequireCertificate
	}
	return true
}

// What a listener does about client certificates, as `/v1/operations/users`
// reports it (RFC 0005). Here rather than in the broker because it is a
// property of the configuration, and one definition is what stops the
// startup line and the route describing one door two ways.
const (
	CertRequired = "required" // nothing connects without one this broker verified
	CertAccepted = "accepted" // one is checked where offered; a password otherwise
	CertNone     = "none"     // no client certificate is examined on this door
)

// ListenerCertificates is what each configured listener does about client
// certificates, keyed as ListenerAuth is.
//
// **A Unix socket is always `none`**, and that is not an omission: it
// carries no TLS, and what decides who may reach it is the file's
// permissions, which the socket's own `mode` states.
func (m MQTT) ListenerCertificates() map[string]string {
	state := func(a *Address) string {
		switch t := a.TLSOf(); {
		case !t.MutualTLS():
			return CertNone
		case t.CertificateRequired():
			return CertRequired
		default:
			return CertAccepted
		}
	}
	out := map[string]string{}
	for _, d := range m.Listen.TCP {
		out[d.Name] = state(&d.Address)
	}
	for _, d := range m.Listen.WS {
		out[d.Name] = state(d.Addr())
	}
	for _, d := range m.Listen.Unix {
		out[d.Name] = CertNone
	}
	return out
}

// TLSOf is a listener's TLS block, or nil for a listener that is not
// configured at all - so that a caller walking every listener does not need
// a nil check of its own before asking about the certificates.
func (a *Address) TLSOf() *TLS {
	if a == nil {
		return nil
	}
	return a.TLS
}

// Version is the TLS floor this listener was given. Validation has already
// refused anything else.
func (t *TLS) Version() uint16 {
	if t != nil && t.MinVersion == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

// validate holds a TLS block to RFC 0002. `at` names it in a finding.
func (t *TLS) validate(at string) []string {
	if t == nil {
		return nil
	}
	var findings []string
	switch t.MinVersion {
	case "", "1.2", "1.3":
	default:
		findings = append(findings, fmt.Sprintf(
			"%s.tls.min_version %q: write 1.2 or 1.3. There is no setting below 1.2, and a "+
				"version above what your clients speak is a fleet that cannot connect",
			at, t.MinVersion))
	}
	for key, path := range map[string]string{"cert_file": t.CertFile, "key_file": t.KeyFile} {
		switch {
		case strings.TrimSpace(path) == "":
			findings = append(findings, fmt.Sprintf(
				"%s.tls.%s: no path. A tls block states both a certificate and its key",
				at, key))
		case !filepath.IsAbs(path):
			findings = append(findings, fmt.Sprintf(
				"%s.tls.%s %q is relative: which certificate a listener serves must not "+
					"depend on where the broker was started from", at, key, path))
		}
	}
	if ca := strings.TrimSpace(t.ClientCAFile); ca != "" && !filepath.IsAbs(ca) {
		findings = append(findings, fmt.Sprintf(
			"%s.tls.client_ca_file %q is relative: which authority a listener trusts must "+
				"not depend on where the broker was started from", at, ca))
	}
	// A rule with nothing to apply it to. Written beside no CA it reads as
	// though certificates were being demanded, and nothing would be.
	if t.RequireCertificate != nil && strings.TrimSpace(t.ClientCAFile) == "" {
		findings = append(findings, fmt.Sprintf(
			"%s.tls.require_certificate is set and %s.tls.client_ca_file names no authority, "+
				"so there is nothing to check a certificate against", at, at))
	}
	sort.Strings(findings) // a map was iterated to get here
	return findings
}

// Operations is the listener an operator reads: `/health` with no
// credential, and `/metrics` behind one (RFC 0005).
type Operations struct {
	// Listen is where it answers: a TCP address, a Unix socket, or both.
	//
	// The two entries are the ones broker.mqtt.listen already has, spelled
	// the same way, so an operator learns one schema rather than two. There
	// is deliberately no `ws`: WebSocket exists on the MQTT listener
	// because a browser cannot open a raw MQTT socket, and nothing about
	// /health and /metrics needs it - a scraper speaks HTTP already. A knob
	// whose only settings are the default and wrong is not a knob.
	//
	// The Unix socket is the other half of the loopback rule below. A
	// scraper on another box needs a tunnel while /metrics has no
	// authentication, but a local agent - a textfile collector, a sidecar,
	// a systemd unit - can read a socket with no port bound at all, and the
	// file's permissions are an authentication saguin already trusts.
	Listen OperationsListen `yaml:"listen"`

	// PasswordFile holds the operators who may read `/metrics`, in
	// Mosquitto's format, and it is **not** the file the MQTT clients are
	// in (RFC 0005 "Authentication and TLS").
	//
	// An operator is not a device. One file for both would mean every
	// device that can publish can also enumerate the channels, their
	// volumes and consumer positions - and a fleet's credentials are on the
	// fleet, readable by anybody holding one device. The two also rotate on
	// different schedules and are owned by different people, which is what
	// a shared file makes impossible rather than merely untidy.
	//
	// Absent means `/metrics` has no credential, which is why the listener
	// is then refused anywhere but loopback.
	PasswordFile string `yaml:"password_file"`

	// MinScrapeInterval is the shortest interval at which the metrics are
	// recomputed. A scrape arriving sooner is answered from the previous
	// one, so the cost of being observed is bounded by saguin rather than
	// by whoever configures the scraper - and by how many of them there
	// are, since five monitoring systems at one second each is five times a
	// bill this broker never agreed to.
	MinScrapeInterval string `yaml:"min_scrape_interval"`
}

// OperationsListen is where the operations listener answers. Absent
// entries open nothing, the same way a `listen` with no `unix` block opens
// no socket.
//
// **Both entries carry `password_file` and `allow_anonymous`**, inline with
// the types the MQTT listener shares. This listener resolves the password
// file per entry (OperatorFiles) and has no anonymous answer, so
// refuseOperationsAnonymous below refuses `allow_anonymous` at startup -
// where it also says why, and where an operator writing one will meet it.
type OperationsListen struct {
	TCP  []TCPDoor
	Unix []UnixDoor
}

// UnmarshalYAML reads `broker.operations.listen`: each of `tcp` and `unix`
// as today's single mapping, or as a list of named doors - the same shape
// broker.mqtt.listen's kinds take (RFC 0002 "Several listeners of a
// kind"), and read the same way: decodeTCPDoors and decodeUnixDoors are
// shared with Listen.UnmarshalYAML, which is what keeps the two schemas
// one schema rather than two that happen to agree today.
func (l *OperationsListen) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("broker.operations.listen: write tcp or unix under it")
	}
	var shadow struct {
		TCP  yaml.Node `yaml:"tcp"`
		Unix yaml.Node `yaml:"unix"`
	}
	if err := decodeNode(n, &shadow); err != nil {
		return fmt.Errorf("broker.operations.listen: %w", err)
	}
	tcp, err := decodeTCPDoors(shadow.TCP, "broker.operations.listen.tcp")
	if err != nil {
		return err
	}
	unix, err := decodeUnixDoors(shadow.Unix, "broker.operations.listen.unix")
	if err != nil {
		return err
	}
	l.TCP, l.Unix = tcp, unix
	return nil
}

// MarshalYAML is Listen.MarshalYAML's rule, minus ws.
func (l OperationsListen) MarshalYAML() (any, error) {
	out := map[string]any{}
	if v := marshalTCPDoors(l.TCP); v != nil {
		out["tcp"] = v
	}
	if v := marshalUnixDoors(l.Unix); v != nil {
		out["unix"] = v
	}
	return out, nil
}

// TCPNamed is the tcp door in this listen block named name, or nil.
func (l OperationsListen) TCPNamed(name string) *TCPDoor {
	for i := range l.TCP {
		if l.TCP[i].Name == name {
			return &l.TCP[i]
		}
	}
	return nil
}

// minScrapeIntervalFloor is the shortest this may be set to, and a shorter
// one is a startup error rather than a value quietly raised.
//
// **A minute is what a broker's metrics are worth**, and it is what the
// managed Kafka services expose: none of them offers broker metrics at
// fifteen-second granularity, because the numbers do not change usefully
// that fast and the reading costs the broker rather than the reader.
//
// The cost this bounds is real and measured. The worst lag on a channel
// means the lowest of one stored position per durable consumer, which at
// ten thousand consumers is 1.59ms on the sqlite store - per channel, per
// scrape (RFC 0005, "What a metric is allowed to cost"). A floor of a
// minute is that bill cut fourfold against a scraper set to Prometheus's
// own default of fifteen seconds.
//
// **Refused rather than raised.** An operator who writes 10s and is
// silently given 60 reads their graphs believing they have ten-second
// resolution, and every conclusion they draw about a spike is drawn at the
// wrong scale. That is the same defect as a knob whose only settings are
// the default and wrong, pointing the other way. Naming the floor costs one
// restart.
const minScrapeIntervalFloor = 60 * time.Second

// defaultMinScrapeInterval is what a block that does not say gets, and it
// is the floor: there is no interval between "the shortest allowed" and
// "what you get for saying nothing" that would mean anything.
const defaultMinScrapeInterval = minScrapeIntervalFloor

// ScrapeInterval is the validated minimum, or the default when the block
// does not say. Validation has already refused anything unparseable.
func (o *Operations) ScrapeInterval() time.Duration {
	if o == nil || o.MinScrapeInterval == "" {
		return defaultMinScrapeInterval
	}
	d, err := parseWholeSeconds(o.MinScrapeInterval)
	if err != nil || d < minScrapeIntervalFloor {
		// Validation refuses both of these, so reaching here means a caller
		// that did not validate. The floor is what it gets: the alternative
		// is an unvalidated configuration setting a cost the floor exists to
		// bound.
		return defaultMinScrapeInterval
	}
	return d
}

// OperatorFile is the password file one door of the operations listener
// authenticates against, and the configuration path that named it.
//
// The path is carried beside the file so that a finding, a --check-config
// line and a startup error all name the key the operator wrote rather than
// the door saguin resolved it to.
type OperatorFile struct {
	// Door is the door's name: "tcp" or "unix" for the single-mapping form,
	// or whatever a list entry was named (RFC 0002 "Several listeners of a
	// kind"). It is what ServeOperations tags a connection with, from the
	// listener it arrived on, never from anything the caller sent.
	Door string

	// Where is the configuration path that named Path - either the
	// listener's own key or broker.operations.password_file.
	Where string

	// Path is the file, or empty for a door with no credential at all.
	Path string
}

// OperatorFiles is the password file each configured door authenticates
// against, in a fixed order so that an operator fixing two findings is not
// handed them shuffled.
//
// **One function rather than the resolution written out at each call
// site**, for the reason MQTT.ListenerAuth is one: the startup that loads
// these files, the --check-config that opens them and the validation that
// decides whether a routable port is authenticated must agree exactly. Two
// copies of this is how --check-config comes to pass a configuration the
// broker refuses.
//
// **A listener's own file wins, and the block's file is what a door that
// names none takes.** That is the same shape as an MQTT listener, and it
// is here because the two doors reach two populations more often than the
// key table suggests: a monitoring system on the port whose credential
// rotates with the estate's secrets, and a local agent on the socket that
// belongs to this machine. One file for both makes those two rotate
// together, which is the thing an operator splitting them is trying to
// stop.
//
// **There is no allow_anonymous to resolve alongside it**, which is the one
// place this differs from an MQTT listener and is deliberate. On the port,
// `true` would re-open the hole gatedDoor documents - every process on the
// machine can reach a loopback port, so "reachable only locally" is not
// "the operator chose who knocks" - and on a routable address it would
// contradict the rule below that refuses an unauthenticated /metrics. On
// the socket it would only strip the name off a reader already getting in,
// since a caller there may name itself in a header the file permissions
// vouch for. A knob whose settings are the default and a hole is not a
// knob, so it is refused by refuseOperationsAnonymous below.
func (o *Operations) OperatorFiles() []OperatorFile {
	if o == nil {
		return nil
	}
	shared := strings.TrimSpace(o.PasswordFile)
	resolve := func(kind, name string, a Auth) OperatorFile {
		if own := strings.TrimSpace(a.PasswordFile); own != "" {
			return OperatorFile{name, OpsDoorPath(kind, name) + ".password_file", own}
		}
		return OperatorFile{name, "broker.operations.password_file", shared}
	}
	var out []OperatorFile
	for _, d := range o.Listen.TCP {
		out = append(out, resolve("tcp", d.Name, d.Auth))
	}
	for _, d := range o.Listen.Unix {
		out = append(out, resolve("unix", d.Name, d.Auth))
	}
	return out
}

// operatorFile is one door's resolved file, or the zero value when that
// door is not configured.
func (o *Operations) operatorFile(door string) OperatorFile {
	for _, f := range o.OperatorFiles() {
		if f.Door == door {
			return f
		}
	}
	return OperatorFile{}
}

// relativeOperatorFile refuses a password file whose path depends on where
// the broker was started from, wherever it is written.
//
// The same rule as a snapshot directory and a database, and it is a
// function rather than three copies because there are now three places to
// write one: the block's file and each listener's own.
func relativeOperatorFile(where, path string) []string {
	if p := strings.TrimSpace(path); p != "" && !filepath.IsAbs(p) {
		return []string{fmt.Sprintf(
			"%s %q is relative: which operators may read /metrics must not depend on "+
				"where the broker was started from", where, p)}
	}
	return nil
}

// refuseOperationsAnonymous refuses the one key an operations listener
// accepts from the type it shares with the MQTT listener and never reads.
//
// **Sharing a type is not sharing a schema.** `Address` and `UnixSocket`
// carry an `Auth` inline for the MQTT listener, which resolves both of its
// keys per listener. The operations listener resolves the password file the
// same way - OperatorFiles above - and has no anonymous answer to resolve,
// for the reasons written there.
//
// **This is a startup error rather than a line in a document because it
// reads as protection.** A key that decides who may read /metrics and is
// silently dropped is the shape of configuration error this file refuses
// everywhere else: the operator has written a decision and been answered
// `ok`.
//
// **Refused here rather than deleted from the type**, so the operator is
// told what does govern the door. Off the type it would be refused too, by
// a YAML parser naming a Go type nobody can find in the documentation,
// which sends somebody hunting a typo they did not make. That is the trade
// a `latest` channel already makes for retention_bytes and max_bytes.
func refuseOperationsAnonymous(where string, a Auth) []string {
	if a.AllowAnonymous == nil {
		return nil
	}
	return []string{fmt.Sprintf(
		"%s.allow_anonymous is not read, and the operations listener has no anonymous "+
			"answer to give: a door with a password file - its own, or "+
			"broker.operations.password_file - makes every reader prove who it is, and "+
			"a door with none has no credential at all, which is why a TCP listener "+
			"without one is refused anywhere but a loopback address", where)}
}

// validate holds the block to RFC 0005.
func (o *Operations) validate() []string {
	if o == nil {
		return nil
	}
	var findings []string
	for i := range o.Listen.TCP {
		d := &o.Listen.TCP[i]
		at := opsDoorAt("tcp", d.Name, i)
		if strings.TrimSpace(d.Address.Address) == "" {
			findings = append(findings, fmt.Sprintf("%s: no address, such as 127.0.0.1:9090", at))
		} else {
			findings = append(findings, checkListenAddress(d.Address.Address, at)...)
		}
		findings = append(findings, d.TLS.validate(at)...)
		findings = append(findings, refuseOperationsAnonymous(at, d.Auth)...)
		findings = append(findings, relativeOperatorFile(at+".password_file", d.PasswordFile)...)
	}
	for i := range o.Listen.Unix {
		u := &o.Listen.Unix[i]
		at := opsDoorAt("unix", u.Name, i)
		findings = append(findings, checkSocketPath(u.Path, at, "/run/saguin/operations.sock")...)
		// The same default and the same rule as the MQTT socket: whoever
		// can open the file can read the metrics.
		if u.Mode == "" {
			u.Mode = "0660"
		}
		if _, err := strconv.ParseUint(u.Mode, 8, 32); err != nil {
			findings = append(findings, fmt.Sprintf(
				"%s.mode %q is not an octal file mode such as 0660", at, u.Mode))
		}
		findings = append(findings, refuseOperationsAnonymous(at, u.Auth)...)
		findings = append(findings, relativeOperatorFile(at+".password_file", u.PasswordFile)...)
	}
	// A block that opens nothing is a block somebody meant to open
	// something with. Absent, the whole listener is off and that is a
	// decision; present and empty is a configuration that says /metrics is
	// wanted and then does not serve it anywhere.
	if len(o.Listen.TCP) == 0 && len(o.Listen.Unix) == 0 {
		findings = append(findings,
			"broker.operations.listen: no listener, such as tcp.address 127.0.0.1:9090 "+
				"or unix.path /run/saguin/operations.sock. Remove the operations block "+
				"to open nothing at all")
	}
	if o.MinScrapeInterval != "" {
		switch d, err := parseWholeSeconds(o.MinScrapeInterval); {
		case err != nil:
			findings = append(findings, fmt.Sprintf(
				"broker.operations.min_scrape_interval %q: %v", o.MinScrapeInterval, err))
		case d < minScrapeIntervalFloor:
			findings = append(findings, fmt.Sprintf(
				"broker.operations.min_scrape_interval %q is below the %s floor. The "+
					"catalogue is recomputed at most that often however fast anybody "+
					"asks, because reading it costs this broker rather than the scraper "+
					"- the worst lag on a channel is a pass over one stored position per "+
					"consumer. Raise it, or remove the key to take %s",
				o.MinScrapeInterval, minScrapeIntervalFloor, defaultMinScrapeInterval))
		}
	}

	// **The rule that stops "authentication later" being for ever.** The
	// port starts on localhost while authentication is unbuilt, which is
	// correct. Then a scraper arrives on another box, somebody changes the
	// address to 0.0.0.0 because that is what makes it reachable, and the
	// broker begins serving channel names, message volumes and consumer
	// counts to anyone who can route to it - with nothing anywhere saying
	// that a decision was made. A startup error is where that gets caught,
	// because it is the one moment the operator is looking.
	//
	//
	// It is asked of the TCP listener only. A Unix socket is not reachable
	// off the box at all, so it does not weaken the rule - it is the answer
	// for a local reader that would otherwise be the reason somebody
	// widened the port.
	//
	// **Authentication is what lifts it**, which is the whole point of the
	// rule: not "loopback for ever", but "say who may read this before you
	// let the network reach it". Two things say it, and the second was
	// refused for a while by a message insisting none was configured.
	//
	// A password file names the operators. Mutual TLS with a client
	// certificate *required* names them too - nothing without a
	// certificate this broker's authority signed completes the handshake,
	// so the port is not reachable by whoever can route to it. That is the
	// same argument the Unix socket already wins on: its file permissions
	// are accepted as the gate with no password file at all.
	//
	// **`require_certificate: false` is not enough**, and it is the only
	// spelling that is not. Absent means required, defaulted the safe way
	// round; written `false` is the mixed mode a fleet migrating in batches
	// needs, where a client without a certificate falls through to the
	// password file - and with no password file it falls through to
	// nothing.
	//
	// **It is asked of the door's own file, not the block's - and of each
	// TCP door in turn, now there can be more than one.** A listener
	// naming its own password_file has authenticated itself and lifts the
	// rule; a block-wide file lifts it for a listener that names none. The
	// two are one question - "is there a credential on this port" - and
	// asking it of the block alone was the defect that made a per-listener
	// file read as protection while governing nothing.
	for i := range o.Listen.TCP {
		d := &o.Listen.TCP[i]
		addr := strings.TrimSpace(d.Address.Address)
		if addr == "" || isLoopback(addr) {
			continue
		}
		if o.operatorFile(d.Name).Path != "" || d.TLS.CertificateRequired() {
			continue
		}
		findings = append(findings, fmt.Sprintf(
			"%s.address %q is not a loopback address and no authentication is configured "+
				"on it: /metrics would serve channel names, volumes and consumer positions "+
				"to anyone who can reach the port. Authenticate it either way - name the "+
				"operators in broker.operations.password_file or in this listener's own "+
				"password_file, or require a client certificate with tls.client_ca_file "+
				"(require_certificate is on unless you turn it off) - or bind it to "+
				"127.0.0.1, or use broker.operations.listen.unix for a reader on this "+
				"machine", opsDoorAt("tcp", d.Name, i), addr))
	}
	// The same rule as a snapshot directory and a database: a relative path
	// means the credentials depend on where the broker was started from, so
	// a systemd unit and a shell in a source tree read different files.
	findings = append(findings, relativeOperatorFile(
		"broker.operations.password_file", o.PasswordFile)...)
	return findings
}

// maxSocketPath is how long a Unix socket path may be.
//
// **A kernel limit, not a choice**: `sockaddr_un.sun_path` is 108 bytes
// including the terminator, so 107 characters bind and 108 do not. Measured
// rather than read off the header, because the off-by-one is the whole
// point of stating it.
const maxSocketPath = 107

// checkSocketPath says what is wrong with a Unix socket path before the
// broker tries to bind it.
//
// **The length is here because the kernel's refusal does not say it.** Over
// the limit, `bind` answers `invalid argument` - which reads as a
// malformed configuration, or a permissions problem, or a bug, and sends
// somebody checking everything except the length. It cost two restarts in
// one session of this feature's own testing, and it is the sort of path a
// scratch directory or a per-user runtime directory produces without
// anybody choosing it.
//
// One helper rather than a check at each socket, so that the next listener
// to take a socket path gets this without anybody remembering.
func checkSocketPath(path, where, example string) []string {
	raw := path
	path = strings.TrimSpace(path)
	if path == "" {
		return []string{where + ": no path, such as " + example}
	}
	// The kernel takes a path as a C string: a NUL ends it, so the lock
	// file beside it cannot even be opened ("invalid argument").
	if strings.ContainsRune(raw, 0) {
		return []string{where + ".path contains a NUL byte, which no file name can"}
	}
	// The last element must be a file name: a path ending in a slash, `.`
	// or `..` names a directory, never a socket file.
	if last := raw[strings.LastIndexByte(raw, '/')+1:]; last == "" || last == "." || last == ".." {
		return []string{fmt.Sprintf("%s.path: its last element is %q, which names a "+
			"directory, not a socket file", where, last)}
	}
	// The bind is given the path as written, so the length that counts is
	// the written one, not the trimmed one.
	if len(raw) > maxSocketPath {
		path = raw
	}
	if len(path) > maxSocketPath {
		return []string{fmt.Sprintf(
			"%s.path is %d characters and a Unix socket path may be %d: the kernel "+
				"refuses a longer one with \"invalid argument\", which says nothing "+
				"about the length. Put the socket somewhere shorter, such as %s",
			where, len(path), maxSocketPath, example)}
	}
	return nil
}

// isLoopback reports whether an address binds only to this machine.
//
// An unparseable address is not loopback: the bind will fail and say so,
// and guessing in the permissive direction here would open the port on the
// strength of a typo.
func isLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	// An empty host is every interface, which is the case this rule exists
	// for and the one an operator is most likely to write.
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	// A name rather than a literal. Only the one name that cannot mean
	// anything else is accepted - anything resolvable is resolvable to
	// somewhere else tomorrow, and a rule that depends on today's DNS is
	// not a rule.
	return host == "localhost"
}

// UnixSocket is a listener that binds to a file.
type UnixSocket struct {
	Path string `yaml:"path"`

	// Auth is this socket's own, and this is the listener it exists for:
	// the file's permissions already decide who may reach it, so a local
	// tool need not also carry a password the fleet on the TCP port must.
	Auth `yaml:",inline"`

	// ProxyProtocol says this socket is fed by a proxy that sends a PROXY
	// protocol v2 header, and that saguin should read it: the client's real
	// address, and the certificate Common Name where the proxy verified one.
	//
	// **Only here, and never on a TCP listener.** The header is the peer
	// asserting who its client is, so anything that can connect can claim
	// to be anybody - a socket's file permissions are what already decide
	// who may assert it, and a TCP listener would need a trusted-proxy
	// allowlist before this could be offered there at all.
	//
	// With it set, a connection arriving without a header is refused: this
	// socket exists because a proxy is in front of it, and serving one
	// anyway would mean every log line's address depended on whether the
	// proxy was working.
	ProxyProtocol bool `yaml:"proxy_protocol"`
	// Mode is the socket file's permissions, in octal, and it is the
	// access control for a Unix socket - whoever can open the file can
	// speak MQTT to the broker. Default 0660: owner and group, nobody else.
	Mode string `yaml:"mode"`
}

// FileMode returns the parsed socket permissions. Validation has already
// rejected anything unparseable, so this cannot fail at startup.
func (u *UnixSocket) FileMode() os.FileMode {
	n, err := strconv.ParseUint(u.Mode, 8, 32)
	if err != nil {
		return 0o660
	}
	return os.FileMode(n)
}

type ChannelConfig struct {
	Type string `yaml:"type"`

	// BridgeSource and DLQBridgeSource named the bridge a channel was a
	// copy from, and they are still fields for the reason Broker.Replica
	// is: a configuration written against the replica arrangement should be
	// told what became of its keys rather than shown the name of a Go type.
	BridgeSource    string `yaml:"bridge_source"`
	DLQBridgeSource string `yaml:"dlq_bridge_source"`

	// Filter is the MQTT topic filter this channel claims. Absent, the
	// channel claims `<name>/#` - what a channel name has always claimed -
	// so every configuration written before this key existed keeps its
	// meaning untouched (RFC 0002 "The topic space").
	Filter string `yaml:"filter"`

	Storage  string `yaml:"storage"`
	MaxBytes string `yaml:"max_bytes"`

	// Start is where a subscriber with no stored position begins reading an
	// `append` channel: `floor` - the default - for everything the channel
	// still holds, or `tail` for only what arrives next.
	//
	// **One rule for both protocols, decided by the operator rather than
	// inferred from the client.** saguin used to answer this from the
	// client's protocol version - the floor for MQTT 5, the tail for 3.1.1,
	// on the grounds that a client which cannot see an offset cannot tell a
	// replay from live traffic. That is a true observation and a bad rule:
	// it made the same application behave differently depending on which
	// library it linked, and it left a clean-session 3.1.1 device unable to
	// read history at all - it started at the tail and could not seek,
	// because seeking needs a durable session. Two defaults for one state
	// is a discontinuity nobody can hold in their head.
	//
	// So the question moves to where the answer actually lives. A channel
	// of commands a constrained fleet acts on wants `tail`, and it wants it
	// for every reader whatever protocol they speak; a channel of events
	// wants `floor`, likewise. The operator knows which it is and the
	// broker cannot.
	Start string `yaml:"start"`

	// Retention removes the oldest records to stay under; max_bytes refuses
	// the publish that would exceed. The difference is what happens to a
	// producer, and it is why they are two keys rather than two spellings of
	// one (RFC 0002 "Bounds on what a channel holds").
	RetentionPeriod string `yaml:"retention_period"`
	RetentionBytes  string `yaml:"retention_bytes"`

	VisibilityTimeout string `yaml:"visibility_timeout"`
	JobExpiresAfter   string `yaml:"job_expires_after"`
	Retry             Retry  `yaml:"retry"`

	// The same two keys for the dead-letter channel a queue derives, which
	// has no block of its own because its name is not the operator's to
	// write.
	DLQRetentionPeriod string `yaml:"dlq_retention_period"`
	DLQRetentionBytes  string `yaml:"dlq_retention_bytes"`

	// DeletionRetentionPeriod is how long a `latest` channel keeps a
	// deletion before removing it for good, or `none` to keep it as long as
	// the channel exists.
	//
	// A period of its own rather than retention_period's, because a value
	// and a deletion are not the same thing to keep: a value lives as long
	// as it is the truth, a deletion only until everything reading this
	// channel has seen it. What sets it is the longest a subscriber may be
	// away and still be told a topic is gone (RFC 0003).
	DeletionRetentionPeriod string `yaml:"deletion_retention_period"`
}

type Retry struct {
	// A pointer, so that a written `0` is not the same as an absent key.
	// Zero reads as "do not retry" and RFC 0002 requires at least 1; as a
	// plain integer it was indistinguishable from omission and silently
	// became 5, so an operator asking for no retries got five and was told
	// nothing.
	MaxAttempts *int `yaml:"max_attempts"`

	// Backoff is how long a returned job waits before it is offered again,
	// and BackoffBase is what that wait is built from. Absent means `none`,
	// which is what every queue did before these existed.
	Backoff     string `yaml:"backoff"`
	BackoffBase string `yaml:"backoff_base"`
}

// A configuration writes `channels:` either as one mapping, or as a list
// whose entries are each a group of channels: `!include <file>`, or
// channels written in place. One file stops being the right shape well
// below the ten thousand channels a fleet reaches, and the list is how a
// domain's channels move into a file its own owners review.
//
// The two forms are read by different paths because refusing an unknown
// key is a property of the decoder rather than of a parsed node. A typo is
// a defect, so every group is handed to a strict decoder of its own rather
// than pulled out of a tree that has already been read.
func decodeFile(f *File, b []byte, path string) error {
	if !hasChannelGroups(b) {
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true)
		if err := dec.Decode(f); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		// Every channel was written here. Left empty, a single-file
		// configuration's /v1/operations/config carried no configured_in,
		// against RFC 0005 and Report's own comment.
		f.channelsFrom = map[string]string{}
		for name := range f.Channels {
			f.channelsFrom[name] = path
		}
		return resolveProviders(f, path)
	}

	var g struct {
		Broker   Broker      `yaml:"broker"`
		Channels []yaml.Node `yaml:"channels"`
		// Every top-level key belongs here as well as on File. This path
		// decodes with KnownFields on, so a key that exists on File and not
		// here is not quietly dropped - it is a hard error naming a key the
		// operator was entitled to write, which sends them looking for a typo
		// they did not make.
		Bridges map[string]BridgeConfig `yaml:"bridges"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&g); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	f.Broker = g.Broker
	f.Bridges = g.Bridges
	f.Channels = map[string]ChannelConfig{}
	// Which file each name came from, because the whole point of a
	// duplicate being a hard error is being told where the other one is -
	// and because --output writes it beside each channel, for a reader
	// holding one document who needs to know which file to edit.
	from := map[string]string{}
	f.channelsFrom = from

	for i := range g.Channels {
		group, where, err := readGroup[ChannelConfig](&g.Channels[i], path, "channels")
		if err != nil {
			return err
		}
		for name, cc := range group {
			if prev, ok := from[name]; ok {
				return fmt.Errorf("channel %q is defined twice: in %s and in %s",
					name, prev, where)
			}
			from[name] = where
			f.Channels[name] = cc
		}
	}
	return resolveProviders(f, path)
}

// resolveProviders fills Storage.Providers from `providers:` as written,
// which is either one mapping or a list of groups, and records which file
// each name came from.
//
// It runs after the decode rather than during it because an `!include`
// names a path relative to the file holding it, and a decoder does not know
// which file that is.
func resolveProviders(f *File, path string) error {
	node := &f.Broker.Storage.ProvidersNode
	if node.Kind == 0 {
		return nil // no providers block, which is a real configuration
	}

	f.Broker.Storage.Providers = map[string]Provider{}
	// Which file each name came from, because the whole point of a
	// duplicate being a hard error is being told where the other one is.
	f.Broker.Storage.from = map[string]string{}

	groups := []*yaml.Node{node}
	if node.Kind == yaml.SequenceNode {
		groups = node.Content
	}
	for _, entry := range groups {
		group, where, err := readGroup[Provider](entry, path, "providers")
		if err != nil {
			return err
		}
		for name, p := range group {
			if prev, ok := f.Broker.Storage.from[name]; ok {
				return fmt.Errorf("storage provider %q is defined twice: in %s and in %s",
					name, prev, where)
			}
			f.Broker.Storage.from[name] = where
			f.Broker.Storage.Providers[name] = p
		}
	}
	return nil
}

// hasChannelGroups reports whether `channels:` is written as a list, which
// is the form that admits an include. A file that does not parse at all is
// left alone: the decoder reports that with the position it happened at,
// which is better than anything this could say.
func hasChannelGroups(b []byte) bool {
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return false
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return false
	}
	top := doc.Content[0].Content
	for i := 0; i+1 < len(top); i += 2 {
		if top[i].Value == "channels" {
			return top[i+1].Kind == yaml.SequenceNode
		}
	}
	return false
}

// readGroup is one entry under `channels:` or `storage.providers:` - the
// entries an include names, or the ones written in place - and the file
// they came from.
//
// It is generic because the rule is the same for both and a second copy of
// it would be a second place to fix: paths relative to the including file,
// no nesting, no wildcards, unknown keys refused. `what` is the word for
// the list in an error, because "an entry under channels" and "an entry
// under providers" are the only difference an operator should see.
func readGroup[T any](entry *yaml.Node, path, what string) (map[string]T, string, error) {
	if entry.Tag == "!include" {
		return readIncluded[T](entry, path)
	}
	if entry.Kind != yaml.MappingNode {
		return nil, "", fmt.Errorf("%s:%d: an entry under %s is either "+
			"`!include <file>` or %s written in place", path, entry.Line, what, what)
	}
	// A master file includes leaves and that is all. Refusing it here is the
	// half of that rule that catches an include buried inside a group;
	// readIncluded catches one written inside a file.
	if bad := findInclude(entry); bad != nil {
		return nil, "", fmt.Errorf("%s:%d: !include cannot sit inside a group of "+
			"%s; it is an entry of the list under %s", path, bad.Line, what, what)
	}
	group, err := strictGroup[T](entry, path)
	return group, path, err
}

// readIncluded reads the file an include names.
func readIncluded[T any](entry *yaml.Node, path string) (map[string]T, string, error) {
	name := strings.TrimSpace(entry.Value)
	if entry.Kind != yaml.ScalarNode || name == "" {
		return nil, "", fmt.Errorf("%s:%d: !include names one file", path, entry.Line)
	}
	// A directory listing is not configuration. With a wildcard, what the
	// broker serves depends on what happens to be in a directory rather than
	// on what somebody reviewed, and an editor's backup file becomes a
	// channel.
	if strings.ContainsAny(name, "*?[") {
		return nil, "", fmt.Errorf("%s:%d: !include %q: name each file, no wildcards",
			path, entry.Line, name)
	}
	// Against the including file rather than the process directory, so that
	// `make demo` and a systemd unit read the same configuration the same
	// way rather than disagreeing about what it means.
	leaf := name
	if !filepath.IsAbs(leaf) {
		leaf = filepath.Join(filepath.Dir(path), leaf)
	}

	b, err := os.ReadFile(leaf)
	if err != nil {
		return nil, "", fmt.Errorf("%s:%d: !include %q: %w", path, entry.Line, name, err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, "", fmt.Errorf("%s: %w", leaf, err)
	}
	if bad := findInclude(&doc); bad != nil {
		return nil, "", fmt.Errorf("%s:%d: an included file cannot include another; "+
			"the master file names every file", leaf, bad.Line)
	}

	group := map[string]T{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&group); err != nil && !errors.Is(err, io.EOF) {
		return nil, "", fmt.Errorf("%s: %w", leaf, err)
	}
	return group, leaf, nil
}

// strictGroup reads channels written in the master file itself. The node
// has been parsed already, but yaml.Node.Decode does not refuse an unknown
// key and a typo has to be refused, so the group is written back out and
// read by a strict decoder.
func strictGroup[T any](entry *yaml.Node, path string) (map[string]T, error) {
	b, err := yaml.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("%s:%d: %w", path, entry.Line, err)
	}
	group := map[string]T{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&group); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %w", path, shiftLines(err, entry.Line-1))
	}
	return group, nil
}

// findInclude is the first include anywhere in a tree, or nil.
func findInclude(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Tag == "!include" {
		return n
	}
	for _, c := range n.Content {
		if found := findInclude(c); found != nil {
			return found
		}
	}
	return nil
}

var yamlLine = regexp.MustCompile(`\bline (\d+):`)

// shiftLines moves a decoder's line numbers from the start of a group to
// the start of the file that group was written in.
//
// It is arithmetic rather than a position the parser reported, so a comment
// or a blank line inside the group moves the answer by that many lines. It
// is kept because the alternative is checking every key against the struct
// by hand, which is a second copy of what the decoder already does and one
// more thing to keep in step with the schema. The file is named exactly and
// the line lands inside the group being read, which is what somebody
// looking for a typo needs.
func shiftLines(err error, base int) error {
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return err
	}
	msgs := make([]string, len(te.Errors))
	for i, m := range te.Errors {
		msgs[i] = yamlLine.ReplaceAllStringFunc(m, func(s string) string {
			n, convErr := strconv.Atoi(yamlLine.FindStringSubmatch(s)[1])
			if convErr != nil {
				return s
			}
			return fmt.Sprintf("line %d:", n+base)
		})
	}
	return errors.New(strings.Join(msgs, "; "))
}

// Load reads, parses, and validates a configuration file, returning the
// channel registry the broker runs against.
func Load(path string) (*File, *channel.Registry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	var f File
	// An unknown key is a typo, and a typo is a defect.
	if err := decodeFile(&f, b, path); err != nil {
		return nil, nil, err
	}

	var findings []string
	var chans []*channel.Channel

	findings = append(findings, f.Broker.Storage.validate(f.Broker.Limits.StorageReserve(), *f.Broker.Limits.withDefaults().MaxConnections)...)
	if retained, err := f.Broker.RetainedBlock(); err != nil {
		findings = append(findings, err.Error())
	} else {
		findings = append(findings, retained.validate(f.Broker.Storage)...)
	}
	if qos2, err := f.Broker.QoS2Block(); err != nil {
		findings = append(findings, err.Error())
	} else {
		findings = append(findings, qos2.validate(f.Broker.Storage)...)
	}
	if session, err := f.Broker.SessionBlock(); err != nil {
		findings = append(findings, err.Error())
	} else {
		findings = append(findings, session.validate(f.Broker.Storage)...)
	}
	if share, err := f.Broker.ShareBlock(); err != nil {
		findings = append(findings, err.Error())
	} else {
		findings = append(findings, share.validate()...)
	}

	names := make([]string, 0, len(f.Channels))
	for name := range f.Channels {
		names = append(names, name)
	}
	sort.Strings(names) // findings must not depend on map iteration order

	for _, name := range names {
		cc := f.Channels[name]
		c := &channel.Channel{Name: name, Filter: cc.Filter,
			StartAtTail: strings.EqualFold(strings.TrimSpace(cc.Start), "tail")}

		switch channel.Type(cc.Type) {
		case channel.Append:
			c.Type = channel.Append
		case channel.Queue:
			c.Type = channel.Queue
		case channel.Latest:
			c.Type = channel.Latest
		case "":
			findings = append(findings, fmt.Sprintf("channel %q: no type", name))
			continue
		default:
			findings = append(findings, fmt.Sprintf("channel %q: unknown type %q", name, cc.Type))
			continue
		}

		if c.Type == channel.Queue {
			d := 30 * time.Second
			if cc.VisibilityTimeout != "" {
				parsed, err := parseWholeSeconds(cc.VisibilityTimeout)
				if err != nil {
					findings = append(findings, fmt.Sprintf("channel %q: visibility_timeout %q: %v", name, cc.VisibilityTimeout, err))
					continue
				}
				d = parsed
			}
			// No floor to one second here: the shortest duration the parser
			// admits is one second, so a value that would round to zero was
			// already refused by name.
			c.VisibilityTimeout = int64(d / time.Second)

			c.MaxAttempts = defaultMaxAttempts
			if cc.Retry.MaxAttempts != nil {
				c.MaxAttempts = *cc.Retry.MaxAttempts
			}
			if c.MaxAttempts < 1 {
				findings = append(findings, fmt.Sprintf("channel %q: retry.max_attempts must be at least 1", name))
				continue
			}

			// backoff and backoff_base are stated together or not at all.
			// Either alone is somebody who meant to have one and has not:
			// a shape with no base has no gap to build, and a base with no
			// shape is a duration nothing reads. Both are silent failures of
			// the kind that only show up as a retry storm against something
			// already down, which is the moment nobody is reading the
			// configuration file.
			switch cc.Retry.Backoff {
			case "", "none":
				if cc.Retry.BackoffBase != "" {
					findings = append(findings, fmt.Sprintf(
						"channel %q: retry.backoff_base is set with no retry.backoff to build a gap from; "+
							"write backoff: linear or backoff: exponential, or remove the base", name))
					continue
				}
			case "linear", "exponential":
				if cc.Retry.BackoffBase == "" {
					findings = append(findings, fmt.Sprintf(
						"channel %q: retry.backoff %s needs retry.backoff_base, such as 2s", name, cc.Retry.Backoff))
					continue
				}
				base, err := parseWholeSeconds(cc.Retry.BackoffBase)
				if err != nil {
					findings = append(findings, fmt.Sprintf(
						"channel %q: retry.backoff_base %q: %v", name, cc.Retry.BackoffBase, err))
					continue
				}
				c.BackoffBase = int64(base / time.Second)
				if cc.Retry.Backoff == "linear" {
					c.Backoff = store.BackoffLinear
				} else {
					c.Backoff = store.BackoffExponential
				}
			default:
				findings = append(findings, fmt.Sprintf(
					"channel %q: retry.backoff %q is not one of none, linear, exponential",
					name, cc.Retry.Backoff))
				continue
			}

			// job_expires_after expires work that nobody ever resolved. It is the
			// queue's substitute for retention, which is refused here because
			// deleting unacknowledged work by age is eviction of unresolved
			// work (RFC 0002 "Validation").
			if cc.JobExpiresAfter != "" {
				ttl, err := parseWholeSeconds(cc.JobExpiresAfter)
				if err != nil {
					findings = append(findings, fmt.Sprintf("channel %q: job_expires_after %q: %v", name, cc.JobExpiresAfter, err))
					continue
				}
				// A visibility timeout at or above the TTL means a record can
				// expire while a worker still holds it, so the worker's answer
				// arrives for a record that is already gone.
				if d >= ttl {
					findings = append(findings, fmt.Sprintf(
						"channel %q: visibility_timeout %s must be less than job_expires_after %s", name, d, ttl))
					continue
				}
				c.JobExpiresAfter = int64(ttl / time.Second)
			}
		} else if cc.VisibilityTimeout != "" || cc.JobExpiresAfter != "" || cc.Retry.MaxAttempts != nil ||
			cc.Retry.Backoff != "" || cc.Retry.BackoffBase != "" {
			findings = append(findings, fmt.Sprintf("channel %q: visibility_timeout, job_expires_after and retry apply only to a queue", name))
			continue
		}

		// max_bytes refuses the publish that would exceed it; it removes
		// nothing. On a queue that is backpressure - resolution frees the
		// space and the queue accepts work again - and on an append channel
		// it is the bound for records that must not be deleted to make room.
		//
		// A latest channel is refused it, per RFC 0002: it holds one value
		// per topic rather than a history, so what grows there is the number
		// of topics, and a byte cap answers a question it does not pose.
		if cc.MaxBytes != "" {
			if c.Type == channel.Latest {
				findings = append(findings, fmt.Sprintf(
					"channel %q: max_bytes does not apply to a latest channel, which holds one value "+
						"per topic rather than a history; use retention_period to remove a topic "+
						"nothing publishes to", name))
				continue
			}
			n, err := parseBytes(cc.MaxBytes)
			if err != nil {
				findings = append(findings, fmt.Sprintf("channel %q: max_bytes %q: %v", name, cc.MaxBytes, err))
				continue
			}
			c.MaxBytes = n
		}

		if bad := retention(c, cc, f.Broker.Storage); len(bad) > 0 {
			findings = append(findings, bad...)
			continue
		}

		if finding := f.Broker.Storage.assign(c, cc.Storage); finding != "" {
			findings = append(findings, finding)
			continue
		}

		chans = append(chans, c)
	}

	reg, err := channel.NewRegistry(chans)
	if err != nil {
		findings = append(findings, err.Error())
	}

	// **A filter must leave room for a topic to be published under it**, or
	// a channel is configured to claim topics no publish would be allowed to
	// carry. NewRegistry has filled in the default `<name>/#` by now, and
	// this is measured against the written filter rather than its `{a,b}`
	// expansions: the expansions are shorter or the same, and naming the
	// line the operator wrote is what makes the finding actionable.
	//
	// A `#` stands in for the rest, so it costs nothing and comes off; every
	// other level is a level a topic must carry.
	if reg != nil {
		for _, c := range chans {
			claim := strings.TrimSuffix(c.Filter, "/#")
			if claim == "#" {
				continue // claims everything and pins no length
			}
			max := defaultMaxTopicLength
			if f.Broker.Limits.MaxTopicLength != nil {
				max = *f.Broker.Limits.MaxTopicLength
			}
			if len(claim) >= max {
				findings = append(findings, fmt.Sprintf(
					"channel %q: filter %q is %d bytes and limits.max_topic_length is %d, so "+
						"no topic it claims could be published",
					c.Name, c.Filter, len(claim), max))
			}
		}
		// And its levels, for the same reason: a filter deeper than any
		// topic may be claims nothing a publish could carry.
		for _, c := range chans {
			if err := channel.TooDeep(c.Filter, f.Broker.Limits.topicLevels()); err != nil {
				findings = append(findings, fmt.Sprintf(
					"channel %q: filter %q %v, so no topic it claims could be published", c.Name, c.Filter, err))
			}
		}
	}

	// After the registry, because an inbound rule names a channel and the
	// check is whether that channel exists. reg is nil when the channels
	// themselves did not validate, and validateBridges skips the check rather
	// than reporting every rule as naming a channel that does not exist.
	bridges, bad := validateBridges(f.Bridges, reg, f.Broker.Limits.topicLevels())
	findings = append(findings, bad...)
	f.bridges = bridges

	// Before withDefaults, which is the whole point: it replaces a zero with
	// the default, so a check after it can only ever see the default and the
	// three below silently passed everything an operator wrote.
	findings = append(findings, f.Broker.Limits.validate()...)

	f.Broker.Limits = f.Broker.Limits.withDefaults()

	// **The two outside `limits`, for the same reason.** RFC 0005 promises
	// the resolved document has "every default filled in", and these were
	// the class that were not: a reader of `/v1/operations/config` or of
	// `--check-config --output` saw `write_timeout` defaulted and these
	// absent, so an absent key and a defaulted one looked alike. The CLI
	// viewer reads `min_scrape_interval` from exactly that route and had to
	// carry the RFC's floor as a fallback, which is a second place for the
	// number to be wrong.
	//
	// After validation, like the limits above: filling first would leave the
	// checks reading a default rather than what an operator wrote.
	if f.Broker.LogLevel == "" {
		f.Broker.LogLevel = defaultLogLevel
	}
	if f.Broker.Operations != nil && f.Broker.Operations.MinScrapeInterval == "" {
		f.Broker.Operations.MinScrapeInterval = defaultMinScrapeIntervalText
	}

	lim := &f.Broker.Limits

	if n, err := parseBytes(lim.MaxMessageSize); err != nil {
		findings = append(findings, fmt.Sprintf("broker.limits.max_message_size %q: %v", lim.MaxMessageSize, err))
	} else if n > math.MaxUint32 {
		// The limit rides in CONNACK's Maximum Packet Size, which is 32-bit.
		findings = append(findings, fmt.Sprintf("broker.limits.max_message_size %q: at most 4GiB", lim.MaxMessageSize))
	}
	if n, err := parseBytes(lim.MaxHeaderBytes); err != nil {
		findings = append(findings, fmt.Sprintf("broker.limits.max_header_bytes %q: %v", lim.MaxHeaderBytes, err))
	} else if n > math.MaxUint32 {
		// Headers ride inside a message, which is at most 4GiB; and the
		// two together are every provider's reserve, which a larger figure
		// would overflow.
		findings = append(findings, fmt.Sprintf("broker.limits.max_header_bytes %q: at most 4GiB", lim.MaxHeaderBytes))
	}

	l := &f.Broker.MQTT.Listen
	for _, d := range l.TCP {
		if strings.TrimSpace(d.Address.Address) == "" {
			findings = append(findings, fmt.Sprintf(
				"%s: no address, such as 0.0.0.0:1883", DoorPath("tcp", d.Name)))
		} else {
			findings = append(findings, checkListenAddress(d.Address.Address, DoorPath("tcp", d.Name))...)
		}
		findings = append(findings, d.TLS.validate(DoorPath("tcp", d.Name))...)
	}
	for _, d := range l.WS {
		if strings.TrimSpace(d.Address.Address) == "" {
			findings = append(findings, fmt.Sprintf(
				"%s: no address, such as 0.0.0.0:1883", DoorPath("ws", d.Name)))
		} else {
			findings = append(findings, checkListenAddress(d.Address.Address, DoorPath("ws", d.Name))...)
		}
		findings = append(findings, d.TLS.validate(DoorPath("ws", d.Name))...)
		// The same parser the listener compares with, so an entry that
		// loads is one that can match.
		for _, o := range d.AllowedOrigins {
			if _, err := listeners.NormalizeOrigin(o); err != nil {
				findings = append(findings, fmt.Sprintf(
					"%s.allowed_origins %q %v", DoorPath("ws", d.Name), o, err))
			}
		}
	}
	// A listener's own file is held to the same rule as the broker's. Its
	// absence meant a relative path here was reported by whatever
	// `os.Open` said from the directory the check ran in - so it passed from
	// a shell that happened to sit beside the file and failed from the unit,
	// which is the whole failure the absolute-path rule exists to prevent.
	for _, l := range sortedListeners(f.Broker.MQTT) {
		if pf := strings.TrimSpace(l.auth.PasswordFile); pf != "" && !filepath.IsAbs(pf) {
			findings = append(findings, fmt.Sprintf(
				"%s.password_file %q is relative: which clients may connect must not "+
					"depend on where the broker was started from", l.where, pf))
		}
	}
	if pf := strings.TrimSpace(f.Broker.MQTT.PasswordFile); pf != "" && !filepath.IsAbs(pf) {
		findings = append(findings, fmt.Sprintf(
			"broker.mqtt.password_file %q is relative: which clients may connect must not "+
				"depend on where the broker was started from", pf))
	}
	// A broker that admits nobody and has nobody to admit. Every client is
	// refused 0x86 and the log says so once per connection, which is a long
	// way round to learning that two keys disagree.
	// **Asked of every listener, through the resolution the broker uses.**
	// These two rules were written when the keys were only the broker's, and
	// read them directly; a listener writing its own pair got neither check,
	// so a door that admits nobody, or one that admits everybody beside an
	// ACL, passed as `ok`. Resolve is the one place that knows what a
	// listener ends up with, so it is what both rules now ask.
	for _, l := range sortedListeners(f.Broker.MQTT) {
		// **Only where the listener's own keys made the difference.** A
		// listener that writes neither takes the broker's pair, and the two
		// broker-level rules below already judge that pair - reporting it
		// again once per listener turns one typo into four findings, which
		// is the same trap the password-file check names.
		if l.auth.AllowAnonymous == nil && strings.TrimSpace(l.auth.PasswordFile) == "" {
			continue
		}
		file, anonymous := l.auth.Resolve(f.Broker.MQTT)
		if !anonymous && strings.TrimSpace(file) == "" {
			findings = append(findings, fmt.Sprintf(
				"%s: anonymous connections are not allowed and no password file is in "+
					"force, so nothing can connect there. Give it a password file, or "+
					"allow anonymous connections", l.where))
		}
		if anonymous && strings.TrimSpace(f.Broker.MQTT.ACLFile) != "" {
			findings = append(findings, fmt.Sprintf(
				"%s: anonymous connections are allowed beside broker.mqtt.acl_file, so a "+
					"client offering no user name is admitted, and the `*` pattern in "+
					"the acl_file matches its empty name - it is granted whatever that "+
					"pattern grants", l.where))
		}
	}
	starts := make([]string, 0, len(f.Channels))
	for name := range f.Channels {
		starts = append(starts, name)
	}
	sort.Strings(starts) // so two runs report the same findings in the same order
	// **The replica's per-channel keys, refused by name.** They said which
	// bridge a channel was a copy from; nothing names that now, because a
	// channel is not a copy of anything - see broker.replica above, which
	// this reports alongside rather than instead of, so an operator holding
	// a replica configuration is told about all of it in one pass.
	for _, name := range starts {
		cc := f.Channels[name]
		for _, k := range []struct{ key, val string }{
			{"bridge_source", cc.BridgeSource},
			{"dlq_bridge_source", cc.DLQBridgeSource},
		} {
			if strings.TrimSpace(k.val) == "" {
				continue
			}
			findings = append(findings, fmt.Sprintf(
				"channel %q: %s is gone. A channel is no longer a copy of anything: a "+
					"bridge rule lands records here by their topic like any publisher, "+
					"and which bridge brought one is a fact the broker keeps on the "+
					"record rather than a property of the channel. Delete the key",
				name, k.key))
		}
	}
	for _, name := range starts {
		cc := f.Channels[name]
		s := strings.ToLower(strings.TrimSpace(cc.Start))
		if s == "" {
			continue
		}
		if s != "floor" && s != "tail" {
			findings = append(findings, fmt.Sprintf(
				"channel %q: start %q is neither \"floor\" nor \"tail\"", name, cc.Start))
			continue
		}
		// **Refused where the question has no meaning**, rather than
		// accepted and ignored. A `latest` subscriber is always served
		// current state - that is what the type is - and a `queue` has no
		// per-consumer position at all, since work is claimed rather than
		// read from a place. A key that quietly does nothing on two of the
		// three types is a key somebody sets and then trusts.
		if t := strings.ToLower(strings.TrimSpace(cc.Type)); t != "" && t != "append" {
			findings = append(findings, fmt.Sprintf(
				"channel %q is a %s and writes start: %s. Only an append channel has a "+
					"place a reader starts from - a latest channel always serves current "+
					"state, and a queue's work is claimed rather than read from a position",
				name, t, s))
		}
	}
	if v := f.Broker.MQTT.MinProtocolVersion; v != nil {
		if _, ok := minProtocolLevels[*v]; !ok {
			// **Refused rather than defaulted**, because the two values this
			// key takes mean opposite things: a typo silently read as the
			// default would open a broker an operator had just written the
			// line to close.
			findings = append(findings, fmt.Sprintf(
				"broker.mqtt.min_protocol_version %q is not a version this broker admits: "+
					"write \"3.1.1\" for both protocols or \"5\" for MQTT 5 only. MQTT 3.1 "+
					"is admitted by neither - its SUBACK has no failure code and its CONNACK "+
					"no Session Present flag", string(*v)))
		}
	}
	if a := f.Broker.MQTT.AllowAnonymous; a != nil && !*a &&
		strings.TrimSpace(f.Broker.MQTT.PasswordFile) == "" {
		findings = append(findings,
			"broker.mqtt.allow_anonymous is false and broker.mqtt.password_file names no "+
				"file, so nothing can connect. Give it a password file, or remove the key")
	}
	if af := strings.TrimSpace(f.Broker.MQTT.ACLFile); af != "" {
		if !filepath.IsAbs(af) {
			findings = append(findings, fmt.Sprintf(
				"broker.mqtt.acl_file %q is relative: what a client may do must not depend "+
					"on where the broker was started from", af))
		}
		// Rules govern an identity. With nothing to authenticate against
		// there is no identity for them to be about, and a file full of
		// rules governing nobody reads as protection and is none.
		//
		// **A client authority is such a thing to authenticate against**,
		// and this used to ask only about the password file. A certificate
		// client is named by its Common Name and is deliberately *not*
		// looked up in the password file (RFC 0002 "Client certificates"),
		// so a pure-certificate estate had to write a password file that
		// authenticates nobody in order to have an `acl_file` at all - a
		// decoy, which is exactly the "reads as protection and is none"
		// shape this refusal exists to prevent, produced by the refusal
		// itself.
		if strings.TrimSpace(f.Broker.MQTT.PasswordFile) == "" && !f.mutualTLSAnywhere() {
			findings = append(findings,
				"broker.mqtt.acl_file names what each client may do, and nothing "+
					"authenticates a client: broker.mqtt.password_file names no file and no "+
					"listener names a client_ca_file. Give it a password file, or give a "+
					"listener a client authority so a certificate's Common Name is the "+
					"identity, or remove the acl_file")
		}
		// The same argument at the other end, and it is worse than "no rule
		// applies": an anonymous client's identity is the empty string, and
		// `*` matches it. So such a client is admitted **and granted
		// whatever the wildcard grants**, silently. The listener loop above
		// says the same thing for a listener that allows them; this covers
		// the pair written at broker level.
		if a := f.Broker.MQTT.AllowAnonymous; a != nil && *a {
			findings = append(findings,
				"broker.mqtt.allow_anonymous is true beside broker.mqtt.acl_file: a "+
					"client offering no user name is admitted, and the `*` pattern in "+
					"the acl_file matches its empty name - it is granted whatever that "+
					"pattern grants")
		}
	}
	findings = append(findings, f.Broker.Operations.validate()...)

	// Required, and there is nothing sensible to invent for it. It names
	// this broker in its own logs, which is unhelpful the first time two of
	// them appear in one log - and the moment a bridge exists, "which
	// broker said that" stops being a question anyone can answer by looking
	// at the machine.
	if strings.TrimSpace(f.Broker.ID) == "" {
		findings = append(findings, "broker.id: name this broker, such as edge-1")
	}
	// **The replica arrangement is gone, and its keys are refused by name.**
	// A copy was a broker that pulled another's records and refused local
	// writes; what replaced it is an ordinary bridge for the records and a
	// copy of the storage for everything else, which is RFC 0004's own
	// answer. Told "unknown key", an operator goes looking for the new
	// spelling of something that no longer has one.
	if f.Broker.Replica != nil {
		findings = append(findings,
			"broker.replica is gone, in either value. A copy is now an ordinary "+
				"bridge for the records - see bridges, direction: in - and a copy of "+
				"the storage for the positions and the queue state, which RFC 0004 "+
				"sets out under backing up. Delete the key")
	}
	// Named rather than numeric, and refused rather than defaulted: a
	// misspelt level that quietly became Info is a broker running at a
	// verbosity nobody chose, which is the thing this key exists to end.
	if lvl := strings.TrimSpace(f.Broker.LogLevel); lvl != "" {
		if _, ok := LogLevels[strings.ToLower(lvl)]; !ok {
			findings = append(findings, fmt.Sprintf(
				"broker.log_level %q: it is one of debug, info, warn or error; "+
					"omit the key for info", lvl))
		}
	}
	// Which file a process claims must not depend on the directory somebody
	// started it from: two copies started from two places would each write a
	// different file and each believe it held the only one.
	if pid := strings.TrimSpace(f.Broker.PIDFile); pid != "" && !filepath.IsAbs(pid) {
		findings = append(findings, fmt.Sprintf(
			"broker.pid_file %q is relative: which file a process claims must not "+
				"depend on where it was started from", pid))
	}
	for i := range l.Unix {
		u := &l.Unix[i]
		findings = append(findings, checkSocketPath(u.Path, DoorPath("unix", u.Name),
			"/run/saguin/saguin.sock")...)
		if u.Mode == "" {
			u.Mode = "0660"
		}
		if _, err := strconv.ParseUint(u.Mode, 8, 32); err != nil {
			findings = append(findings, fmt.Sprintf(
				"%s.mode %q is not an octal file mode such as 0660", DoorPath("unix", u.Name), u.Mode))
		}
	}
	findings = append(findings, doorClashes(&f)...)

	if len(findings) > 0 {
		return nil, nil, fmt.Errorf("%s:\n  - %s", path, strings.Join(findings, "\n  - "))
	}

	// The default applies only when nothing at all was asked for. A
	// configuration naming just a Unix socket must not silently also open a
	// TCP port, which is the listener its author was avoiding.
	if len(l.TCP) == 0 && len(l.WS) == 0 && len(l.Unix) == 0 {
		l.TCP = []TCPDoor{{Name: "tcp", Address: Address{Address: ":1883"}}}
	}

	return &f, reg, nil
}

// Flatten is the configuration as the broker resolved it: the master file
// and every `!include` in one document, every default filled in, and the
// file each channel was written in beside it as a comment.
//
// **It is a configuration, not a report.** What comes out loads again, and
// that is the property the whole thing rests on - RFC 0004's promotion
// runbook asks an operator to diff one deployment's configuration against
// another's, and a document that could not be started is not evidence about
// a broker. TestTheFlattenedConfigurationLoadsAgain is what holds it there.
//
// **The channels come from the registry rather than from the file**,
// because the registry is what the broker serves: a channel that named no
// storage has the default provider, one that named no retention has the
// broker-wide period, and the question an operator brings to this - which
// of my channels would survive this machine - is about the resolved values
// rather than the written ones.
//
// Derived dead-letter channels are left out. A queue's companion is not the
// operator's to name - the suffix is refused as a configured name - so
// writing one out would produce a document that does not load, which is the
// one thing this must not do.
//
// **The provenance is a comment because a reader wants it and a parser must
// not.** Nothing a round trip depends on is written in one: feed this back
// in and every comment can be stripped without changing what the broker
// serves.
func (f *File) Flatten(reg *channel.Registry) ([]byte, error) {
	out := *f
	out.Channels = map[string]ChannelConfig{}
	for name, c := range reg.All() {
		if strings.HasSuffix(name, channel.DLQSuffix) {
			continue
		}
		out.Channels[name] = channelConfigOf(c)
	}
	return render(out, f.channelsFrom, nil)
}

// render writes a configuration as one document: the resolved providers
// where the include was, every key nobody wrote pruned, the file each
// channel came from beside it, and whatever `edit` adds before it is
// marshalled. Flatten and Passive share it so that the two documents differ
// only where the transform says they do.
func render(out File, from map[string]string, edit func(*yaml.Node)) ([]byte, error) {
	var doc yaml.Node
	if err := doc.Encode(out); err != nil {
		return nil, err
	}
	// Encode writes the node `providers:` was read from, includes and all,
	// because the resolved map is not a yaml field. Both are answered by
	// putting the map where that node was: setProviders replaces the key
	// rather than adding to it, so the include cannot survive.
	if len(out.Broker.Storage.Providers) > 0 {
		if err := setProviders(&doc, out.Broker.Storage.Providers); err != nil {
			return nil, err
		}
	}
	prune(&doc)
	commentChannels(&doc, from)
	if edit != nil {
		edit(&doc)
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// channelConfigOf writes a resolved channel back in the words a
// configuration uses for it.
//
// Every duration is whole seconds and every size is a byte count, which
// both spellings the schema accepts read back as. `none` is written where a
// zero means "keep everything", because omission means the broker-wide
// default instead - the difference this document exists to make visible.
func channelConfigOf(c *channel.Channel) ChannelConfig {
	// **The filter is written out whether or not the operator wrote one**,
	// because `--check-config --output` is where somebody goes to find out
	// what the broker resolved - and the default `<name>/#` is exactly the
	// thing that is invisible in the file it came from. `{a,b}` levels are
	// shown as written; --route is what prints the expansions.
	cc := ChannelConfig{
		Type: string(c.Type), Filter: c.Filter,
		Storage: c.Storage,
	}
	if c.MaxBytes > 0 {
		cc.MaxBytes = strconv.FormatInt(c.MaxBytes, 10)
	}
	// **Written only when it is not the default**, as every other key here
	// is: `floor` is what an absent key means, and writing it back would
	// tell a reader the operator had chosen something they had not.
	if c.StartAtTail {
		cc.Start = "tail"
	}
	switch c.Type {
	case channel.Queue:
		// A queue has no retention of its own: removing unresolved work by
		// age or size is eviction (invariant 2). Its companion's two keys
		// are written here because that is where a configuration puts them.
		cc.VisibilityTimeout = seconds(c.VisibilityTimeout)
		if c.JobExpiresAfter > 0 {
			cc.JobExpiresAfter = seconds(c.JobExpiresAfter)
		}
		attempts := c.MaxAttempts
		cc.Retry = Retry{MaxAttempts: &attempts}
		if c.Backoff != store.BackoffNone {
			cc.Retry.Backoff = backoffName(c.Backoff)
			cc.Retry.BackoffBase = seconds(c.BackoffBase)
		}
		cc.DLQRetentionPeriod = periodOrNone(c.DLQRetentionPeriod)
		cc.DLQRetentionBytes = bytesOrNone(c.DLQRetentionBytes)
	case channel.Latest:
		// No size rule: a latest channel holds a value per topic rather than
		// a history, so what grows is the topic count.
		cc.RetentionPeriod = periodOrNone(c.RetentionPeriod)
		cc.DeletionRetentionPeriod = periodOrNone(c.DeletionRetentionPeriod)
	default:
		cc.RetentionPeriod = periodOrNone(c.RetentionPeriod)
		cc.RetentionBytes = bytesOrNone(c.RetentionBytes)
	}
	return cc
}

func seconds(n int64) string { return strconv.FormatInt(n, 10) + "s" }

// backoffName is the word a configuration writes for a backoff kind. The
// kind is a number, so converting it to a string gives the character with
// that code - which is what this exists to stop, having written one into a
// document that was supposed to load again.
func backoffName(k store.BackoffKind) string {
	switch k {
	case store.BackoffLinear:
		return "linear"
	case store.BackoffExponential:
		return "exponential"
	}
	return ""
}

// periodOrNone writes a period, or the word for keeping everything. Zero is
// `none` rather than an absent key: absent takes the broker-wide default,
// which is a different configuration.
func periodOrNone(n int64) string {
	if n == 0 {
		return RetentionNone
	}
	return seconds(n)
}

func bytesOrNone(n int64) string {
	if n == 0 {
		return RetentionNone
	}
	return strconv.FormatInt(n, 10)
}

// setProviders puts the resolved providers under broker.storage, where the
// struct's own yaml tag no longer carries them.
func setProviders(doc *yaml.Node, providers map[string]Provider) error {
	storage := find(find(doc, "broker"), "storage")
	if storage == nil {
		return nil // no broker block at all, which validation has refused
	}
	var node yaml.Node
	if err := node.Encode(providers); err != nil {
		return err
	}
	if existing := find(storage, "providers"); existing != nil {
		*existing = node
		return nil
	}
	storage.Content = append(storage.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: "providers"}, &node)
	return nil
}

// commentChannels writes the file each channel came from above its name.
//
// A configuration that came from one file gets no comments: the answer is
// the file the operator just named, and a line repeating it above every
// channel is noise in the document they have to read. Two files or more is
// when the question "which file do I edit" has an answer worth writing.
func commentChannels(doc *yaml.Node, from map[string]string) {
	files := map[string]bool{}
	for _, f := range from {
		files[f] = true
	}
	channels := find(doc, "channels")
	if channels == nil || len(files) < 2 {
		return
	}
	for i := 0; i+1 < len(channels.Content); i += 2 {
		if where, ok := from[channels.Content[i].Value]; ok {
			channels.Content[i].HeadComment = "from " + where
		}
	}
}

// prune drops every key whose value is absent - a null, an empty string, or
// a block left empty by its own absent keys.
//
// **This is readability rather than correctness**, which is worth saying
// plainly because the comment here first claimed the opposite: the unpruned
// document loads perfectly well. What it does is turn the example
// configuration from 100 lines into 59, and the missing 41 are keys nobody
// wrote - `ws: null`, `file_path: ""`, `retry: {}` on a channel that is not
// a queue. This document exists to be read in one pass and diffed against
// another, and in a diff those lines are noise that moves when a schema
// grows a field.
//
// The literal `none` is a value and is left exactly where it is: it means
// "keep nothing" where absence means "take the broker-wide default", and
// showing that difference is most of the point.
func prune(n *yaml.Node) {
	if n == nil {
		return
	}
	// Depth first, so that a block left empty by its own absent keys -
	// `retry: {}` on a channel that is not a queue - goes with them.
	for _, c := range n.Content {
		prune(c)
	}
	if n.Kind != yaml.MappingNode {
		return
	}
	var kept []*yaml.Node
	for i := 0; i+1 < len(n.Content); i += 2 {
		v := n.Content[i+1]
		switch {
		case v.Kind == yaml.ScalarNode && (v.Tag == "!!null" || v.Value == ""):
		case v.Kind == yaml.MappingNode && len(v.Content) == 0:
		default:
			kept = append(kept, n.Content[i], v)
		}
	}
	n.Content = kept
}

// find is the value of a key in a mapping node, or nil.
func find(n *yaml.Node, key string) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// Report is the resolved configuration as a generic document, for
// `/v1/operations/config` to answer with (RFC 0005).
//
// **It is a report, not a configuration**, which is the whole difference
// from Flatten above and is why the two are named as they are. Flatten
// produces something that loads again, and everything it does is in service
// of that: provenance lives in a comment because a parser must not depend
// on it, and derived channels are left out because a document naming one
// would not load. Nothing feeds this back - an operator reading it over
// HTTP is asking what the broker resolved, not building a file - so the
// provenance is an ordinary field here and is better for being one.
//
// **It is built from Flatten rather than beside it**, so there is one
// resolution and not two. A second walk over the same struct is a second
// answer to "what is this broker running", and the two would agree until
// somebody changed one - which is the failure this package has already had
// once, in the shape of a metric that disagreed with the thing it measured.
// What is lost in the round trip through YAML is exactly the comments, and
// the only comment Flatten writes is the provenance this puts back.
//
// The document is JSON-marshalable as it stands: yaml.v3 decodes nested
// mappings as map[string]any, and encoding/json orders a map's keys, so two
// calls on one configuration render byte for byte the same.
func (f *File) Report(reg *channel.Registry) (map[string]any, error) {
	doc, err := f.Flatten(reg)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := yaml.Unmarshal(doc, &m); err != nil {
		return nil, fmt.Errorf("the resolved configuration did not read back: %w", err)
	}

	// **Provenance goes on every channel that has one, whatever the file
	// count.** Flatten writes its comment only where two or more files are
	// involved, because with one file the comment repeats the name the
	// reader already typed. A caller here is a script rather than a reader:
	// a field that appears on some deployments and not others is one every
	// caller has to handle twice, and the redundancy costs a string.
	if channels, ok := m["channels"].(map[string]any); ok {
		for name, v := range channels {
			c, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if where := f.channelsFrom[name]; where != "" {
				c["configured_in"] = where
			}
		}
	}
	return m, nil
}
