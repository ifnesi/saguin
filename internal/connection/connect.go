package connection

import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/topic"
)

// oversizeConnect reads only the Protocol Name and level, enough to choose a
// refusing CONNACK's wire version. The announced body is never allocated or
// read. Only the two recognized names can be answered (design.md, CONNECT).
func (r *reader) oversizeConnect(length int) (packet.Connect, error) {
	var prefix [9]byte
	if length < 2 {
		return packet.Connect{}, packet.ErrMalformed
	}
	if err := r.readFull(prefix[:2]); err != nil {
		return packet.Connect{}, err
	}
	n, _, _ := packet.DecodeUint16(prefix[:2])
	if (n != 4 && n != 6) || length < int(n)+3 {
		return packet.Connect{}, packet.ErrMalformed
	}
	if err := r.readFull(prefix[2 : int(n)+3]); err != nil {
		return packet.Connect{}, err
	}
	v, err := packet.DecodeConnectVersion(prefix[:int(n)+3])
	return packet.Connect{Version: v}, err
}

// connect checks broker restrictions and records only the connection-local
// scalars needed later. Neither reader nor writer retains the CONNECT buffer.
func (r *reader) connect(p *packet.Connect, reason byte) Event {
	if r.live != nil {
		r.accessCopy = r.live.Access()
		r.acl = r.accessCopy.ACL
		r.passwords = r.accessCopy.Passwords[r.passwordPath]
	}
	r.clientID = p.ClientID
	r.conn.connectUser = p.UserName
	r.conn.accessFile = r.acl
	r.version = p.Version
	if r.version != packet.V311 && r.version != packet.V5 {
		r.version = packet.V311 // unknown levels get the MQTT 3.1.1 refusal
	}
	peer := peerSettings{version: r.version}
	if reason == 0 {
		peer.maxSize, r.expiry = p.MaximumPacketSize, p.SessionExpiryInterval
		peer.aliases = p.TopicAliasMaximum
		r.problemInfo = p.RequestProblemInformation
		reason = r.admitConnect(p)
		r.keepAlive = time.Duration(KeepAlive(p.Version, p.KeepAlive, r.limits.MaxKeepalive)) * time.Second
		r.keepAlive += r.keepAlive / 2
	}
	r.access, r.connectAccess = r.conn.access, r.conn.access
	r.publishRate, r.publishBytes = r.limits.PublishRate, r.limits.PublishBytes
	r.connected = true
	if r.limits.PublishRate != 0 || r.limits.PublishBytes != 0 {
		r.rate = publishBuckets{at: r.conn.clock.Now(), messages: float64(r.limits.PublishRate), bytes: float64(r.limits.PublishBytes)}
	}
	r.conn.peer <- peer // happens before the core can send its CONNACK
	if reason == 0 && p.WillFlag {
		w := &p.Will
		if encoded := payload.Compress(w.Payload); encoded != nil {
			w.PayloadSize = len(w.Payload)
			prefix := len(w.StoredProperties) - len(w.Properties)
			w.StoredProperties = bytes.Clone(w.StoredProperties)
			w.Properties = packet.Properties(w.StoredProperties[prefix:])
			// Authentication and CONNECT property checks are complete. These
			// remaining views would otherwise keep the Will's original body.
			p.Properties, p.Password, w.Payload = nil, nil, nil
			w.StoredPayload = payload.Compact(encoded)
		}
	}
	return Event{Kind: Received, Conn: r.conn, Packet: p, Reason: reason}
}

// admitConnect finishes all admission checks on the reader, before the core
// can load, take over or save any session. Each early return fixes the first
// reason in CONNECT's specified policy order.
func (r *reader) admitConnect(p *packet.Connect) byte {
	if len(p.ClientID) > r.limits.MaxClientIDLength ||
		(p.ClientID == "" && !p.CleanStart) || strings.ContainsFunc(p.ClientID, unicode.IsControl) {
		return 0x85
	}
	if !r.authenticate(p) {
		return 0x86
	}
	if r.acl != nil {
		e := r.acl.Select(r.conn.user)
		if !e.AllowsClient(r.conn.user, p.ClientID) {
			return 0x86
		}
		r.conn.access = r.acl.Compile(e, r.conn.user, p.ClientID)
		if e != nil {
			if e.PublishRate != 0 {
				r.limits.PublishRate = e.PublishRate
			}
			if e.PublishBytes != 0 {
				r.limits.PublishBytes = config.Size(e.PublishBytes)
			}
		}
		if r.conn.access.Denied(acl.Persistent) {
			r.expiry = 0
		}
	}
	if p.WillFlag {
		if r.conn.access.Denied(acl.Will) || strings.HasPrefix(p.Will.Topic, "$SYS/") {
			return 0x87
		}
		channel, index := "", -1
		if r.routing != nil {
			index = r.routing.Resolve(p.Will.Topic)
			if index >= 0 {
				channel = r.routing.Channels[index].Name
			}
		}
		if strings.HasSuffix(channel, "__dlq") || !r.conn.access.Allows(acl.Write, channel, p.Will.Topic) {
			return 0x87
		}
		limits := topic.Limits{MaxLength: r.limits.MaxTopicLength, MaxLevels: r.limits.MaxTopicLevels}
		if topic.CheckName(p.Will.Topic, limits) != nil || topic.HasReservedPrefix(p.Will.Topic) {
			return 0x90
		}
		if p.Will.Retain && r.conn.access.Denied(acl.Retained) {
			return 0x9A
		}
		if p.Will.Retain && index < 0 && r.retainedMaximum != 0 && willBytes(&p.Will) > r.retainedMaximum {
			return 0x97
		}
		if p.Will.QoS == 2 && r.conn.access.Denied(acl.QoS2) {
			return 0x9B
		}
	}
	if p.HasSaguinProperty {
		return 0x83
	}
	if p.HasAuthenticationMethod {
		return 0x8C
	}
	return 0
}

// authenticate runs after packet validity and client-id admission, before any
// session lookup. Anonymous access permits an absent user name only; a name
// supplied beside a password file must always be proved by that password.
// A verified certificate is the complete proof and takes precedence over it.
func (r *reader) authenticate(p *packet.Connect) bool {
	name, source := r.commonName, "PROXY certificate"
	if cert := r.certificate; cert != nil {
		name, source = cert.Subject.CommonName, "TLS certificate"
		if name == "" && len(cert.DNSNames) != 0 {
			name = cert.DNSNames[0]
		}
	}
	if r.certificate != nil || r.proxyVerified {
		if !ValidIdentityName(name) {
			r.failure = fmt.Errorf("MQTT listener %s: client %q: verified %s has no usable user name", r.conn.listener, p.ClientID, source)
			return false
		}
		r.conn.user = name
		return true
	}
	if !p.UserNameFlag {
		return !r.requireUser
	}
	if r.passwords == nil {
		return true // no identity source: the supplied name is unproved
	}
	verified := r.passwords.Verify(p.UserName, string(p.Password))
	if !verified || !p.PasswordFlag {
		return false
	}
	r.conn.user = p.UserName
	return true
}

// ValidIdentityName applies the certificate-name rule to a verified TLS or
// PROXY identity. MQTT and operations use the same validity check.
func ValidIdentityName(name string) bool {
	return name != "" && utf8.ValidString(name) && !strings.ContainsFunc(name, unicode.IsControl)
}

// willBytes counts the stored topic, payload and property section, including
// its length prefix. A Will with no properties stores NULL.
func willBytes(w *packet.Will) int64 {
	n := len(w.Topic) + len(w.Payload)
	if len(w.Properties) != 0 {
		var length [4]byte
		prefix, _ := packet.AppendVarInt(length[:0], uint32(len(w.Properties)))
		n += len(prefix) + len(w.Properties)
	}
	return int64(n)
}

// KeepAlive returns the keep-alive in seconds selected for a CONNECT. Only
// MQTT 5 can advertise a cap, and a requested zero then also takes that cap.
// The reader's deadline and the core's CONNACK use this same choice.
// A resolved configuration supplies maximum in whole seconds, at most 65535.
func KeepAlive(version packet.Version, requested uint16, maximum time.Duration) uint16 {
	if version == packet.V5 && maximum != 0 &&
		(requested == 0 || time.Duration(requested)*time.Second > maximum) {
		return uint16(maximum / time.Second)
	}
	return requested
}
