package main

import (
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The broker's configuration, which the model reads (config.md,
// broker.limits). It is the same on every run, whatever -duration, so one
// seed gives one sequence.
const (
	// BadPackets' rows assume these two limits and maxTopicLevels
	// (harness, badpackets.go).
	maxPacketSize  = harness.BadMaxPacketSize
	maxConnectSize = harness.BadMaxConnectSize
	// The least config.md allows, so a CONNECT within max_connect_size can
	// hold a client id over it.
	maxClientIDLength = 23
	// Small enough for a Will topic over either to fit in a CONNECT, and
	// large enough for $saguin/sessions/disconnect (requests.go).
	maxTopicLength = 27
	maxTopicLevels = harness.BadMaxTopicLevels
	// qos2.max_inflight_per_client, the CONNACK's Receive Maximum.
	receiveMaximum = 20
	// design.md, CONNECT, step 8: "Topic Alias Maximum (16)".
	topicAliasMaximum = 16
	// limits.max_incoming_memory is the budget of the bodies being read
	// and stored, limits.max_outgoing_memory that of the deliveries queued
	// to writers (design.md, Message memory). config.md,
	// max_incoming_memory: "It must hold the largest body one reader can
	// ask for: max_packet_size, plus the largest S2 copy of it where
	// max_packet_size is over 4 KiB, plus max_topic_length where a bridge
	// has an in rule", 12,320 bytes for 6 KiB and a max_topic_length of
	// 27, as a body over 4 KiB "also asks for room for the largest copy S2
	// can make of it" (Message memory, Asking). Floods, their bodies of
	// half of max_packet_size up to its limit, each over 4 KiB asking about
	// twice its size, pass it in every run, so readers wait their turn in
	// the incoming budget (memoryFilled). The outgoing budget is above
	// every delivery's encoded size and the stored copy a compressed one
	// also reserves (a PUBLISH within max_packet_size, its Subscription
	// Identifiers, a Topic Alias and the Sagüin properties, then its
	// payload's S2 copy, under 13 KiB together), so none is discarded for
	// max_outgoing_memory, which the model does not predict.
	maxIncomingMemory = 16 << 10
	maxOutgoingMemory = 16 << 10
	maxSubscriptions  = 4
	maxInflight       = 4
)

// The run leaves out everything that waits on a clock (connect_timeout,
// keep-alive expiry, write_timeout): every deadline below
// is set past the longest run, so none comes due while it lasts.
const (
	// longestRun is the most -duration may be.
	longestRun = time.Hour
	// keepAliveFloor is the least non-zero Keep Alive a client asks, in
	// seconds: a client silent for 1.5 times it is closed (design.md,
	// DISCONNECT, MQTT-3.1.2-22), 90 minutes, past the longest run and its
	// end.
	keepAliveFloor = 3600
	// maxKeepalive caps what an MQTT 5 client asks, 0 included (design.md,
	// DISCONNECT: "a keep-alive of 0 is never idle-closed unless capped"):
	// twice the floor, so some asks are capped and the cap is out of reach.
	maxKeepalive = 2 * keepAliveFloor
	// expiryFloor is the least non-zero session expiry asked, in seconds:
	// a day, so no session expires within a run.
	expiryFloor = 24 * 3600
	// maxSessionExpiry caps it (config.md), and is MQTT 3.1.1 Clean
	// Session 0's: some asks are over it.
	maxSessionExpiry = 7 * expiryFloor
	// connectTimeout outlasts the longest run, so a connection that never
	// completes its CONNECT stays open until its client closes it.
	connectTimeout = 2 * longestRun
	// maxConnectRate is never reached: the driver opens one connection at
	// a time and waits for its answer, so no accept waits for the rate.
	maxConnectRate = 1_000_000
	// maxClients is the most -clients may be, and maxHeld the most
	// connections held without a CONNECT at once (silent, halfConnect).
	maxClients = 1000
	maxHeld    = 8
)

// maxConnections is limits.max_connections for a run of clients: the
// connections it opens at once, one per client, maxHeld held and one more:
// a takeover's second connection, a health check's or a malformed first
// packet's (badPacket), each in a step of its own. The run never opens
// more, so no CONNECT it sends is refused or held for want of a permit
// but briefly, while the permit of a connection it has just closed is on
// its way back (design.md, Flows, CONNECT, Connection permits: "While no
// permit is left, the listener holds that connection unserved"; "the core
// puts the permit back when the writer reports the close"), which only delays
// the answer.
func maxConnections(clients int) int {
	return clients + maxHeld + 1
}

// storageModes are broker.storage's modes (config.md, mode: "full, normal
// or memory"), one of which the broker a run starts takes: -mode's, or one
// drawn from the seed (storageMode).
var storageModes = []string{"full", "normal", "memory"}

// storageMode is the mode a run of seed takes where -mode names none: the
// seed alone decides it, so a replay of the seed takes it again, and the
// driver's own draws are left as they were.
func storageMode(seed uint64) string {
	return storageModes[seed%uint64(len(storageModes))]
}

// syncInterval is a normal-mode run's sync_interval, its least (config.md:
// "at least 100ms"), so the broker's syncs come among the run's steps as
// often as they can.
const syncInterval = 100 * time.Millisecond

// ramFolder is the tmpfs in which a memory-mode run makes the folder its
// memory_dir lies in: the folder of config.md's default memory_dir,
// /dev/shm/saguin.
const ramFolder = "/dev/shm"

// options is the broker's configuration for a run of clients, written in
// dir.
func options(dir string, clients int) *harness.Options {
	return &harness.Options{
		Dir:                  dir,
		MaxConnections:       maxConnections(clients),
		MaxPacketSize:        maxPacketSize,
		MaxConnectSize:       maxConnectSize,
		MaxClientIDLength:    maxClientIDLength,
		MaxTopicLength:       maxTopicLength,
		MaxTopicLevels:       maxTopicLevels,
		MaxInflightPerClient: receiveMaximum,
		MaxKeepalive:         maxKeepalive * time.Second,
		MaxSessionExpiry:     maxSessionExpiry * time.Second,
		ConnectTimeout:       connectTimeout,
		WriteTimeout:         stopWriteTimeout,
		MaxConnectRate:       maxConnectRate,
		MaxIncomingMemory:    maxIncomingMemory,
		MaxOutgoingMemory:    maxOutgoingMemory,
		MaxSubscriptions:     maxSubscriptions,
		MaxInflight:          maxInflight,
		Channels:             channelOptions(),
		Login:                harness.Login{PasswordFile: filepath.Join(dir, passwordFileName)},
		ACLFile:              filepath.Join(dir, aclFileName),
		// The run reads saguin_memory_full_seconds_total at its end
		// (memoryFilled), and calls each operations listener at its
		// checkpoints (operations.go): the plain one, one over TLS asking
		// for client certificates without requiring them, and a unix
		// socket with proxy_protocol, all with the run's operators' file.
		Operations:      true,
		OpsPasswordFile: filepath.Join(dir, operatorsFileName),
		OpsListeners: []harness.OpsListener{
			{TLS: &harness.TLS{
				CertFile:           filepath.Join(dir, certificateFileName),
				KeyFile:            filepath.Join(dir, keyFileName),
				ClientCAFile:       filepath.Join(dir, authorityFileName),
				RequireCertificate: "false",
			}},
			{Unix: &harness.Unix{Path: filepath.Join(dir, opsSocketName), ProxyProtocol: true}},
		},
		// tcp1 speaks plain MQTT, tcp2 TLS with the run's certificate
		// (writeCertificates), each client's connections going to either
		// (fuzzer.dial).
		TCPListeners: 2,
		ListenerTLS: []harness.TLS{{}, {
			CertFile: filepath.Join(dir, certificateFileName),
			KeyFile:  filepath.Join(dir, keyFileName),
		}},
		// A unix listener with proxy_protocol, its clients' connections
		// relayed behind a PROXY v2 header (fuzzer.dial): it carries no
		// SSL record, so its clients log in by their passwords as on TCP
		// (design.md, Flows, Transport setup).
		Unix: []harness.Unix{{Path: filepath.Join(dir, unixSocketName), ProxyProtocol: true}},
		// ws1 speaks MQTT over WebSocket, ws2 over WebSocket over TLS with
		// the run's certificate (fuzzer.dial).
		WS: []harness.WS{{}, {TLS: &harness.TLS{
			CertFile: filepath.Join(dir, certificateFileName),
			KeyFile:  filepath.Join(dir, keyFileName),
		}}},
	}
}

// unixSocketName is the unix listener's socket in the run's folder, whose
// path is at most 107 bytes (config.md, A listener, path) under make's
// TMPDIR.
const unixSocketName = "mqtt.sock"

// login is a user of the run's password file and the password its hash is
// of.
type login struct{ user, password string }

// logins are the users of the run's password file (config.md, broker.mqtt,
// password_file), which the broker the run starts reads, so with it
// allow_anonymous is false: every client logs in as one of them. A broker
// given with -addr is configured with the same users. Short, so a CONNECT
// carrying them keeps within max_connect_size about as often as one
// without; no password is another's or a prefix of another's, each two
// characters long, as the ones a reload gives them (reload.go).
var logins = []login{{"ann", "k3"}, {"bo", "xw"}, {"cy", "7q"}, {"di", "pz"}}

// loginIterations is the $7$ hashes' iteration count, as saguin --passwd
// writes them (design.md, Operations).
const loginIterations = 1000

// passwordFileName is the run's password file, in the broker's folder.
const passwordFileName = "passwords"

// writePasswords writes the run's password file in dir: one user:hash line
// per login, its $7$ hash made with a salt of 64 random bytes (config.md,
// password_file: "the hash is $7$<iterations>$<salt>$<hash>,
// PBKDF2-HMAC-SHA512 with a 64-byte output [...] salt and hash in standard
// base64").
func writePasswords(dir string) error {
	var lines strings.Builder
	for _, l := range logins {
		salt := make([]byte, 64)
		if _, err := rand.Read(salt); err != nil {
			return err
		}
		hash, err := harness.SevenHash(l.password, loginIterations, salt)
		if err != nil {
			return err
		}
		fmt.Fprintf(&lines, "%s:%s\n", l.user, hash)
	}
	return os.WriteFile(filepath.Join(dir, passwordFileName), []byte(lines.String()), 0o600)
}

// denials are the features the run's acl_file denies each user, none for
// a user it does not name (design.md, Access and reload: "A features rule
// takes deny: instead, naming any of persistent, will, share, retained and
// qos2"): cy and di have features withheld; ann and bo none.
var denials = map[string][]string{"cy": {"persistent", "will"}, "di": {"retained", "share", "qos2"}}

// denied reports whether the run's acl_file denies user feature.
func denied(user, feature string) bool {
	return slices.Contains(denials[user], feature)
}

// grant is one rule of the run's acl_file (design.md, Access and reload:
// "A rule names exactly one of channel:, topic: or broker: [...] A topic:
// names a $broadcast filter"; "A channel rule may add filter: to narrow its
// reach within the channel"): write and read on topic, a $broadcast
// filter, or on channel, narrowed to filter where set, and on a queue
// consume ("A queue rule may list write, read and seek for its topics, and
// consume for a worker").
type grant struct {
	topic, channel, filter string
	write, read, consume   bool
}

// restricted is the user whose rights are per topic (rights): its clients
// publish, subscribe and receive only what those allow.
const restricted = "bo"

// rights are restricted's grants: on $broadcast, read alone on topics of
// one level, write and read on two, write alone on three (which a reload
// takes away and gives back: reload.go), nothing on more;
// on the channels, read alone on ca, write and read on cab and on cz's
// cb/z/# (not its ca/z/#), write alone on small, nothing on cr, and write
// and read on kv's topics of two levels (kv/+), a deletion included
// (design.md, Access and reload: "A latest rule may list write and read,
// with write including an empty payload that deletes a value"), not on
// kv itself nor its deeper topics; write and read on the queue's topics but
// never consume, so it reads the queue and is no worker ("The two need
// separate permissions, consume and read", Channels, queue), and nothing
// on its dead-letter channel. Every other user may write and read
// everywhere, and consume the queue (grantsOf), but write no dead letter
// ("as an append channel allowing read and seek, never write", Channels,
// queue).
var rights = []grant{
	{topic: "+", read: true},
	{topic: "+/+", write: true, read: true},
	{topic: "+/+/+", write: true},
	{channel: "ca", read: true},
	{channel: "cab", write: true, read: true},
	{channel: "cz", filter: "cb/z/#", write: true, read: true},
	{channel: "small", write: true},
	{channel: "kv", filter: "kv/+", write: true, read: true},
	{channel: queueName, write: true, read: true},
}

// everything is the grants of every user but restricted: write and read on
// every $broadcast topic, "#", and on every configured channel, consume on
// the queue, and read on its dead-letter channel, so nothing they do is
// refused for want of a rule.
var everything = func() []grant {
	gs := make([]grant, 0, 1+len(channels))
	gs = append(gs, grant{topic: "#", write: true, read: true})
	for _, c := range channels {
		gs = append(gs, grant{channel: c.name, write: c.deadLetter == "", read: true, consume: c.queue != nil})
	}
	return gs
}()

// grantsOf is user's grants in the run's acl_file.
func grantsOf(user string) []grant {
	if user == restricted {
		return rights
	}
	return everything
}

// may reports whether the run's acl_file lets user write topic (write) or
// read it: a topic rule where it lands on $broadcast, its channel's rule
// otherwise (design.md, Words: Channel; Access and reload). No "+" or "#"
// matches a topic beginning with $ (MQTT-4.7.2-1).
func may(user, topic string, write bool) bool {
	ch := route(topic)
	for _, g := range grantsOf(user) {
		switch {
		case (write && !g.write) || (!write && !g.read):
		case g.channel == "" && ch == broadcast && matchesFilter(g.topic, topic):
			return true
		case g.channel != "" && g.channel == ch && (g.filter == "" || matchesFilter(g.filter, topic)):
			return true
		}
	}
	return false
}

// consumes reports whether the run's acl_file lets user consume the queue
// named (design.md, Access and reload: "A queue rule may list [...]
// consume for a worker").
func consumes(user, queue string) bool {
	return slices.ContainsFunc(grantsOf(user), func(g grant) bool { return g.channel == queue && g.consume })
}

// readsCache holds readsAny's answers, by user and filter.
var readsCache = map[[2]string]bool{}

// readsAny reports whether user may read some topic filter can match:
// design.md, Access and reload, "The broker refuses a filter with 0x87 only
// when the user may read nothing the filter can match". Every topic of up to one level
// more than max_topic_levels, a filter's longest, drawn from the levels
// the rules, the channels and filter write out and one other, stands for
// them all (topicsOf).
func readsAny(user, filter string) bool {
	key := [2]string{user, filter}
	if r, ok := readsCache[key]; ok {
		return r
	}
	found := false
	topicsOf(filter, maxTopicLevels+1, func(topic string) bool {
		found = may(user, topic, false)
		return !found
	})
	readsCache[key] = found
	return found
}

// aclFileName is the run's acl_file, in the broker's folder.
const aclFileName = "acl.yaml"

// writeACL writes the run's acl_file in dir (design.md, Access and reload):
// the role all, everything's grants with seek on each append channel and
// queue, a dead-letter channel included (a latest rule takes write and
// read alone: "a verb its subject does not take" refuses the file), and
// sessions: disconnect (requests.go), and the role part, rights; each user denied features has a role of its own
// denying them. Every user of the password file has an entry of its own
// name: restricted part, the others all, pacedUser's with its publish
// limits (pacing.go). A broker given with -addr reads the same file.
func writeACL(dir string) error {
	var acl strings.Builder
	acl.WriteString("roles:\n")
	for _, role := range []struct {
		name string
		gs   []grant
	}{{"all", everything}, {"part", rights}} {
		fmt.Fprintf(&acl, "  %s:\n", role.name)
		for _, g := range role.gs {
			var verbs []string
			if g.write {
				verbs = append(verbs, "write")
			}
			if g.read {
				verbs = append(verbs, "read")
			}
			if role.name == "all" && g.channel != "" && !isLatest(g.channel) {
				verbs = append(verbs, "seek")
			}
			if g.consume {
				verbs = append(verbs, "consume")
			}
			if len(verbs) == 0 {
				// A grant a reload has taken away (reload.go): "a rule
				// that allows or denies nothing" refuses the file.
				continue
			}
			switch {
			case g.channel == "":
				fmt.Fprintf(&acl, "    - topic: %q\n", g.topic)
			case g.filter == "":
				fmt.Fprintf(&acl, "    - channel: %s\n", g.channel)
			default:
				fmt.Fprintf(&acl, "    - channel: %s\n      filter: %q\n", g.channel, g.filter)
			}
			fmt.Fprintf(&acl, "      allow: [%s]\n", strings.Join(verbs, ", "))
		}
		if role.name == "all" {
			acl.WriteString("    - broker: sessions\n      allow: [disconnect]\n")
		}
	}
	for _, l := range logins {
		if d := denials[l.user]; len(d) > 0 {
			fmt.Fprintf(&acl, "  deny-%s:\n    - broker: features\n      deny: [%s]\n", l.user, strings.Join(d, ", "))
		}
	}
	acl.WriteString("users:\n")
	for _, l := range logins {
		roles := "all"
		if l.user == restricted {
			roles = "part"
		}
		if len(denials[l.user]) > 0 {
			roles += ", deny-" + l.user
		}
		if l.user == pacedUser {
			fmt.Fprintf(&acl, "  %s: {roles: [%s], limits: %s}\n", l.user, roles, pacedLimits)
			continue
		}
		fmt.Fprintf(&acl, "  %s: [%s]\n", l.user, roles)
	}
	return os.WriteFile(filepath.Join(dir, aclFileName), []byte(acl.String()), 0o600)
}

// certificateFileName and keyFileName are the TLS listeners' certificate
// and key, and authorityFileName the authority's certificate, in the
// broker's folder.
const (
	certificateFileName = "server.crt"
	keyFileName         = "server.key"
	authorityFileName   = "ca.pem"
)

// writeCertificates makes an authority and the certificate it issues to
// the broker for its loopback address, writes the certificate and its key
// in dir for the TLS listeners (config.md, A listener, tls: cert_file and
// key_file), and the authority's certificate for the operations listener
// asking for client certificates (client_ca_file); it returns the authority,
// which issues the operators' certificates (operations.go), and the TLS
// configuration of a client trusting it alone.
func writeCertificates(dir string) (*tls.Config, *testkit.Authority, error) {
	ca, err := testkit.NewAuthority("e2efuzz authority")
	if err != nil {
		return nil, nil, err
	}
	server, err := ca.Issue(&testkit.Leaf{CommonName: "saguin", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
	if err != nil {
		return nil, nil, err
	}
	for name, data := range map[string][]byte{certificateFileName: server.CertPEM, keyFileName: server.KeyPEM, authorityFileName: ca.PEM} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return nil, nil, err
		}
	}
	return &tls.Config{RootCAs: ca.Pool()}, ca, nil
}

// signIn gives p a user name and password, and returns it.
func signIn(p *harness.Connect, user, password string) *harness.Connect {
	p.HasUsername, p.Username, p.HasPassword, p.Password = true, user, true, []byte(password)
	return p
}

// knownLogin reports whether p carries the user name of a user of the
// password file and that user's password.
func knownLogin(p *harness.Connect) bool {
	return p.HasUsername && p.HasPassword && slices.Contains(logins, login{p.Username, string(p.Password)})
}

// The sizes of an accepting MQTT 5 CONNACK here. The smallest holds what
// CONNECT, step 8 puts in every one: a fixed header (2), the flags and the
// reason (2), the Property Length (1), Receive Maximum (3), Maximum Packet
// Size (5) and Topic Alias Maximum (3). The largest adds an Assigned Client
// Identifier (21), a capped Session Expiry Interval (5) and Server Keep
// Alive (3), and Retain Available, Shared Subscription Available and Maximum
// QoS (2 each), which MQTT 5 lets a server leave out. A client's Maximum Packet
// Size below the smallest gets no CONNACK, at or above the largest gets one
// (MQTT-3.1.2-24: "The Server MUST NOT send packets exceeding Maximum
// Packet Size to the Client"); between the two the CONNACK's optional
// properties decide, which the run never asks.
const (
	smallestConnack = 16
	largestConnack  = 16 + 21 + 5 + 3 + 2 + 2 + 2
)

// session is what the model knows of the session for one client id.
type session uint8

const (
	// noSession: none a CONNECT can keep, so one keeping its session gets
	// Session Present 0: none exists, or one ending with its connection is
	// connected, which a takeover ends (design.md, CONNECT: "A takeover of a
	// session whose expiry is 0 ends it and starts a new one").
	noSession session = iota
	// present: one exists, away, or connected with a session expiry above
	// 0; Session Present 1.
	present
	// unsettled: a session ending with its connection, whose connection
	// closed without a DISCONNECT (dropped, or closed by the broker), or
	// one an earlier run against -addr may have left. Its end reaches the
	// core only after the client sees the close (design.md, Flows, Closing a
	// connection, 1 and 2: "The writer writes the last packet, ends its sending
	// and stops the reader." "The reader stops reading and says so before it
	// reports its end to the core."), so a CONNECT for the id on another
	// connection races with it on the readers' channel. The model asks nothing
	// whose answer depends on that race: the id's next CONNECT starts clean,
	// Session Present 0 whichever arrives first.
	unsettled
)

// returnCode is the MQTT 3.1.1 return code of each MQTT 5 refusal the run
// provokes (design.md, CONNECT's refusal table).
var returnCode = map[byte]byte{0x85: 2, 0x86: 4, 0x87: 5, 0x90: 5, 0x9A: 5, 0x83: 3, 0x9B: 5}

// prediction is the answer the model predicts for one CONNECT.
type prediction struct {
	version byte // the form the answer comes in
	// tooBig: over max_connect_size, MQTT 5 is refused with 0x95 and MQTT
	// 3.1.1 just closed.
	tooBig bool
	// refusals are the reasons a refusing CONNACK may carry, in the
	// answer's form; none, the CONNECT is accepted.
	refusals []byte
	// unsendable: accepted, but the CONNACK is over the client's Maximum
	// Packet Size, so the broker sends nothing and closes.
	unsendable bool
	present    bool // Session Present
	anon       bool // an empty client id: the session is not the slot's id's
	durable    bool // design.md, Words: "Durable when its expiry is above zero"
	// MQTT 5's Session Expiry Interval, as asked and as granted.
	asked, expiry uint32
	keepAlive     uint16 // as asked
	// The CONNACK withholds retained messages, Retain Available 0, shared
	// subscriptions, Shared Subscription Available 0, and QoS 2, Maximum QoS
	// 1, the acl_file denying them the user (design.md, CONNECT, 8).
	noRetain, noShare, noQoS2 bool
}

// predict is the answer to p, size bytes long, from a broker configured by
// options, known being what the model knows of the session of p's client
// id. Each rule is the
// design's, cited where it is applied.
func predict(p *harness.Connect, size int, known session) *prediction {
	v5 := p.Version == harness.V5
	a := &prediction{version: harness.V311, keepAlive: p.KeepAlive, anon: p.ClientID == ""}
	if v5 && p.ProtocolName == "MQTT" {
		a.version = harness.V5
	}
	// design.md, Flows, Packet buffers: "A packet's size is checked before its
	// body is read." Thus, nothing else in the body is seen; CONNECT's table:
	// 0x95, "MQTT 3.1.1 return code: none: the connection closes".
	if size > maxConnectSize {
		a.tooBig = true
		return a
	}
	// CONNECT's table: "an unknown protocol level with the Protocol Name
	// MQTT, or MQTT 3.1 (MQIsdp, level 3)": return code 1, "sent in MQTT
	// 3.1.1 form". The rest of the CONNECT's layout depends on the level.
	if p.ProtocolName != "MQTT" || (p.Version != harness.V311 && !v5) {
		a.refusals = []byte{1}
		return a
	}
	// Step 2, "The broker checks password, access, version, client id and
	// limits": any refusal that applies is accepted, the order of the
	// admission reasons being TestConnectAdmissionOrder's and
	// TestACLWillAtConnect's (internal/harness/conformance).
	var refusals []byte
	// 0x85: "a client id over max_client_id_length, holding a control
	// character, or empty with Clean Start 0".
	if len(p.ClientID) > maxClientIDLength || strings.ContainsFunc(p.ClientID, unicode.IsControl) ||
		(a.anon && !p.CleanStart) {
		refusals = append(refusals, 0x85)
	}
	// 0x86: "a wrong or missing credential", the broker reading the run's
	// password file, so allow_anonymous false (config.md).
	loggedIn := knownLogin(p)
	if !loggedIn {
		refusals = append(refusals, 0x86)
	}
	// 0x87: "the Will: will denied, or its topic not allowed" (may).
	if p.Will != nil && loggedIn && (denied(p.Username, "will") || !may(p.Username, p.Will.Topic, true)) {
		refusals = append(refusals, 0x87)
	}
	// 0x90: "a Will topic that is not a valid topic name".
	if p.Will != nil && !willTopicValid(p.Will.Topic) {
		refusals = append(refusals, 0x90)
	}
	// 0x9A: "a retained Will, on any topic, with retained denied
	// (MQTT-3.2.2-13)".
	if w := p.Will; w != nil && loggedIn && w.Retain && denied(p.Username, "retained") &&
		willTopicValid(w.Topic) {
		refusals = append(refusals, 0x9A)
	}
	// 0x9B: "a QoS 2 Will with qos2 denied".
	if p.Will != nil && loggedIn && p.Will.QoS == 2 && denied(p.Username, "qos2") {
		refusals = append(refusals, 0x9B)
	}
	if v5 {
		// 0x83: "an unknown saguin- property"; no saguin- property is
		// known on a CONNECT.
		if slices.ContainsFunc(p.Props, func(q harness.Property) bool {
			return q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-")
		}) {
			refusals = append(refusals, 0x83)
		}
		// 0x8C: "an Authentication Method".
		if _, ok := p.Props.Get(harness.AuthenticationMethod); ok {
			refusals = append(refusals, 0x8C)
		}
	}
	if len(refusals) > 0 {
		if !v5 {
			for i, r := range refusals {
				refusals[i] = returnCode[r]
			}
		}
		a.refusals = refusals
		return a
	}
	// Accepted. "Session Present is 1 when the CONNECT keeps its session
	// [...] exists for the client id, either away or connected with a session
	// expiry above 0. Keeping the session means Clean Start 0, or MQTT 3.1.1
	// Clean Session 0. A takeover of a session whose expiry is 0 ends it and
	// starts a new one. Otherwise Session Present is 0".
	a.present = !p.CleanStart && !a.anon && known == present
	// design.md, CONNECT: "A client whose persistent is denied is accepted
	// with Session Expiry 0".
	persistent := !denied(p.Username, "persistent")
	a.noRetain, a.noShare, a.noQoS2 = denied(p.Username, "retained"), denied(p.Username, "share"), denied(p.Username, "qos2")
	if v5 {
		sei, _ := p.Props.Get(harness.SessionExpiryInterval)
		a.asked, a.expiry = sei.Int, min(sei.Int, maxSessionExpiry)
		if !persistent {
			a.expiry = 0
		}
		a.durable = a.expiry > 0
		mps, ok := p.Props.Get(harness.MaximumPacketSize)
		a.unsendable = ok && mps.Int < smallestConnack
	} else {
		// config.md: max_session_expiry "is MQTT 3.1.1 Clean Session 0's".
		a.durable = !p.CleanStart && persistent
	}
	return a
}

// willTopicValid reports whether t is a Will topic the broker takes:
// design.md, CONNECT, 0x90 for "a Will topic that is not a valid topic
// name: holding + or #, empty, over limits.max_topic_length or
// limits.max_topic_levels, or under $share/ or $saguin/".
func willTopicValid(t string) bool {
	return testkit.TopicNameOK(t) && len(t) <= maxTopicLength && strings.Count(t, "/") < maxTopicLevels &&
		!strings.HasPrefix(t, "$share/") && !strings.HasPrefix(t, "$saguin/")
}

// assignedForm is an Assigned Client Identifier's form (design.md,
// CONNECT, step 8): "the CONNECT's time in UTC as YYMMDDHHMMSS followed by
// six random letters or digits".
var assignedForm = regexp.MustCompile(`^\d{12}[0-9A-Za-z]{6}$`)

// checkConnack checks an accepting MQTT 5 CONNACK's properties (design.md,
// CONNECT, step 8: "the broker sends CONNACK with every value it capped or
// chose. These are Server Keep Alive, Session Expiry Interval, Receive
// Maximum [...] Assigned Client Identifier, Maximum Packet Size
// (limits.max_packet_size), Topic Alias Maximum (16), and the Maximum QoS,
// Retain Available and Shared Subscription Available the acl_file allows").
// What the acl_file allows MQTT 5 says by leaving Retain Available and
// Shared Subscription Available out or 1 (3.2.2.3.5, 3.2.2.3.13), and
// Maximum QoS out (3.2.2.3.4: "If the Maximum QoS is absent, the Client uses
// a Maximum QoS of 2"); what it denies by 0, and QoS 2 by Maximum QoS 1. A
// value kept as asked may be repeated (3.2.2.3.2,
// 3.2.2.3.14): a Session Expiry Interval of 0 granted to a user denied
// persistent, where it asked one above 0, is not.
func checkConnack(ack *harness.Connack, a *prediction) error {
	want := map[byte]uint32{
		harness.ReceiveMaximum:    receiveMaximum,
		harness.MaximumPacketSize: maxPacketSize,
		harness.TopicAliasMaximum: topicAliasMaximum,
	}
	if a.noQoS2 {
		want[harness.MaximumQoS] = 1
	}
	may := map[byte]uint32{}
	for _, f := range []struct {
		id     byte
		denied bool
	}{{harness.RetainAvailable, a.noRetain}, {harness.SharedSubscriptionAvailable, a.noShare}} {
		if f.denied {
			want[f.id] = 0
		} else {
			may[f.id] = 1
		}
	}
	if a.expiry != a.asked {
		want[harness.SessionExpiryInterval] = a.expiry
	} else {
		may[harness.SessionExpiryInterval] = a.asked
	}
	// design.md, DISCONNECT: "limits.max_keepalive caps what an MQTT 5
	// client may ask".
	if a.keepAlive == 0 || a.keepAlive > maxKeepalive {
		want[harness.ServerKeepAlive] = maxKeepalive
	} else {
		may[harness.ServerKeepAlive] = uint32(a.keepAlive)
	}
	assigned := false
	for _, p := range ack.Props {
		switch v, ok := want[p.ID]; {
		case ok && v == p.Int:
			delete(want, p.ID)
		case p.ID == harness.AssignedClientIdentifier && a.anon && !assigned:
			if !assignedForm.MatchString(p.Str) {
				return fmt.Errorf("CONNACK's Assigned Client Identifier %q is not YYMMDDHHMMSS and six letters or digits", p.Str)
			}
			assigned = true
		default:
			if v, ok := may[p.ID]; !ok || v != p.Int {
				return fmt.Errorf("CONNACK carries property 0x%02X = %d %q, which the broker neither capped nor chose", p.ID, p.Int, p.Str)
			}
			delete(may, p.ID)
		}
	}
	if len(want) > 0 {
		id := slices.Min(slices.Collect(maps.Keys(want)))
		return fmt.Errorf("CONNACK lacks property 0x%02X = %d", id, want[id])
	}
	if a.anon && !assigned {
		return errors.New("CONNACK to an empty client id carries no Assigned Client Identifier (MQTT-3.2.2-16)")
	}
	return nil
}

// String is the predicted answer, for the step log.
func (a *prediction) String() string {
	switch {
	case a.tooBig && a.version == harness.V5:
		return "CONNACK 0x95, then the close"
	case a.tooBig, a.unsendable:
		return "the close, no CONNACK"
	case len(a.refusals) > 0:
		return fmt.Sprintf("CONNACK refusing with one of % X, then the close", a.refusals)
	}
	return fmt.Sprintf("CONNACK 0x00, Session Present %v, then PINGRESP", a.present)
}

// describe is p, for the step log.
func describe(p *harness.Connect) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CONNECT %s level %d, id %q, clean %v, keep-alive %d", p.ProtocolName, p.Version, p.ClientID, p.CleanStart, p.KeepAlive)
	if p.HasUsername {
		fmt.Fprintf(&b, ", user %q", p.Username)
	}
	if p.HasPassword {
		fmt.Fprintf(&b, ", password %q", p.Password)
	}
	for _, q := range p.Props {
		b.WriteString(", " + propString(q))
	}
	if w := p.Will; w != nil {
		fmt.Fprintf(&b, ", Will %q QoS %d retain %v", w.Topic, w.QoS, w.Retain)
		for _, q := range w.Props {
			b.WriteString(", Will " + propString(q))
		}
	}
	return b.String()
}

// propString is one property, for the step log.
func propString(p harness.Property) string {
	switch {
	case p.ID == harness.UserProperty:
		return fmt.Sprintf("user %q=%q", p.Str, p.Value)
	case p.Str != "":
		return fmt.Sprintf("0x%02X %q", p.ID, p.Str)
	case p.Data != nil:
		return fmt.Sprintf("0x%02X %x", p.ID, p.Data)
	}
	return fmt.Sprintf("0x%02X %d", p.ID, p.Int)
}

// maxUint32 is the largest Four Byte Integer, a Session Expiry Interval
// that never expires (MQTT 5 3.1.2.11.2).
const maxUint32 = math.MaxUint32
