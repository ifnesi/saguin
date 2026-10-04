package broker

import (
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// What a shared group is owed while its members are away is the broadcast
// log after the group's cursor (bgroup.go, RFC 0003 "Broadcast"). This is
// what the broker does with it outside the group's own drain: the bound
// broker.share.expires_after sets, at a start and while it runs.

// shareFiltersOf is the shared filters a session holds, and none for a
// client with no subscription table to read.
func shareFiltersOf(cl *mqtt.Client) []string {
	if cl.State.Subscriptions == nil {
		return nil
	}
	var groups []string
	// Walked in place: the callback is a prefix test and an append.
	cl.State.Subscriptions.Each(func(_ string, sub packets.Subscription) {
		if isShareFilter(sub.Filter) {
			groups = append(groups, sub.Filter)
		}
	})
	return groups
}

// isShareFilter reports whether a filter is a shared subscription's, which
// is the full `$share/<group>/<topic filter>` the index keys a group by.
func isShareFilter(filter string) bool {
	return strings.HasPrefix(filter, "$share/")
}

// SetShareExpiry is broker.share.expires_after: how long a group's delivery
// waits before it is dropped whatever its members' sessions say. Zero is no
// expiry of its own, and a backlog then lives exactly as long as a member
// session that could collect it. Called once at startup, before any
// listener opens.
func (b *Broker) SetShareExpiry(after time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shareExpiry = after
}

// ShareExpiry is what SetShareExpiry set, which the startup line reports.
func (b *Broker) ShareExpiry() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shareExpiry
}

// DropShareBacklogsLeftByAStop drops what the groups have held past
// broker.share.expires_after, and answers how many deliveries went.
//
// **Dropped by the start itself rather than by the first tick after it**,
// so an outage does not quietly extend what the operator bounded. Called
// after the broadcast log is started, which is where each group's list is
// rebuilt and a group the store ended at open is counted.
func (b *Broker) DropShareBacklogsLeftByAStop(now time.Time) int {
	if d := b.broadcastDrain(); d != nil {
		return d.expireGroups(now, b.shareExpiry)
	}
	return 0
}

// sweepShareBacklogs drops what has been held past `broker.share.expires_after`
// at a running broker, on the same tick the share cursors are swept on.
//
// **A bound that only applied at a restart would not be one.** The start
// enforces it too, because a broker that was down for a week must not serve
// week-old work on the way up; this is the same rule while it is running,
// and an operator who set an hour means an hour either way.
func (b *Broker) sweepShareBacklogs(now time.Time) {
	if d := b.broadcastDrain(); d != nil {
		d.expireGroups(now, b.shareExpiry)
	}
}
