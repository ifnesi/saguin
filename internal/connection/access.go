package connection

import (
	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
)

// negotiatedDenial checks the successful CONNACK, not the current ACL. MQTT
// 3.1.1 has no capability properties and keeps its initial protocol checks.
func (r *reader) negotiatedDenial(feature acl.Feature) bool {
	if r.version != packet.V5 {
		return r.connectAccess.Denied(feature)
	}
	return r.negotiatedDenied&feature != 0
}

// refreshAccess compiles once per changed generation. The unchanged busy path
// is one load and a pointer comparison, without a snapshot or allocation. Limits
// are selected here but applied only by the next PUBLISH's bucket accounting.
func (r *reader) refreshAccess() {
	if r.live == nil {
		return
	}
	snapshot := r.live.Access()
	if snapshot == r.accessCopy {
		return
	}
	r.accessCopy, r.acl, r.access = snapshot, snapshot.ACL, nil
	r.publishRate, r.publishBytes = r.baseRate, r.baseBytes
	if r.acl != nil {
		entry := r.acl.Select(r.conn.user)
		r.access = r.acl.Compile(entry, r.conn.user, r.clientID)
		if entry != nil {
			if entry.PublishRate != 0 {
				r.publishRate = entry.PublishRate
			}
			if entry.PublishBytes != 0 {
				r.publishBytes = config.Size(entry.PublishBytes)
			}
		}
	}
}
