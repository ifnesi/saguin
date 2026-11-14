package connection

import (
	"errors"
	"net"
	"strings"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/topic"
)

// Subscription carries checked filter syntax and options. OverLimit is a
// per-filter policy result, kept separate from packet validity: a malformed
// later filter must still close the connection before any filter is accepted.
type Subscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	OverLimit         bool
	ReadAccess        acl.Coverage
}

// Subscribe transfers checked subscriptions and their packet-wide properties.
// Its strings own only the fields the core needs, never the whole input body.
type Subscribe struct {
	AccessFile        *acl.File // generation of every filter's ReadAccess
	PacketID          uint16
	Identifier        uint32
	Unknown           bool
	Partitions        string
	Partition         topic.Partitions
	InvalidPartitions bool
	Filters           []Subscription
}

// Unsubscribe transfers checked filters and the diagnostic property section.
// Unread is already suitable for the UNSUBACK, respecting CONNECT's Request
// Problem Information setting.
type Unsubscribe struct {
	PacketID uint16
	Filters  []string
	Unread   packet.Properties
}

func (r *reader) subscribe(body []byte) (Event, error) {
	p, err := packet.DecodeSubscribe(body, r.version)
	if err != nil {
		return Event{}, err
	}
	if r.version == packet.V5 && p.HasShared && !r.connackWritten && !r.waitConnack() {
		return Event{}, net.ErrClosed
	}
	r.refreshAccess()
	s := &Subscribe{PacketID: p.PacketID, AccessFile: r.acl}
	var declarations []string
	violatesShare := false
	noLocalShare := false
	for prop := range p.Properties.Views() {
		if prop.ID == packet.SubscriptionIdentifier {
			s.Identifier = prop.Int
		} else if prop.ID == packet.UserProperty && packet.IsSaguinProperty(prop.Name) {
			if string(prop.Name) == "saguin-filter" {
				declaration := string(prop.Value)
				declarations = append(declarations, declaration)
				if !s.Partition.Add(declaration) {
					s.InvalidPartitions = true
				}
			} else {
				s.Unknown = true
			}
		}
	}
	s.Partitions = strings.Join(declarations, "\n")
	for sub := range p.Subscriptions.All() {
		filter := string(sub.Filter)
		over, err := r.checkFilter(filter)
		if err != nil {
			return Event{}, err
		}
		if r.version == packet.V5 && strings.HasPrefix(filter, "$share/") && r.negotiatedDenial(acl.Share) {
			// Complete decoding above validates the whole packet before this
			// CONNACK capability violation can end the connection.
			violatesShare = true
		}
		noLocalShare = noLocalShare || (sub.NoLocal && strings.HasPrefix(filter, "$share/"))
		coverage := SubscriptionCoverage(r.access, r.routing, filter)
		qos := sub.QoS
		_, service := r.routing.SeekChannel(filter, true)
		if !service && qos == 2 && r.access.Denied(acl.QoS2) {
			qos = 1
		}
		s.Filters = append(s.Filters, Subscription{Filter: filter, QoS: qos, ReadAccess: coverage,
			NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished,
			RetainHandling: sub.RetainHandling, OverLimit: over})
	}
	if violatesShare {
		return Event{Kind: Ended, Conn: r.conn, Reason: 0x9E}, nil
	}
	if noLocalShare {
		return Event{}, packet.ErrProtocol
	}
	return Event{Kind: Received, Conn: r.conn, Packet: s}, nil
}

// SubscriptionCoverage compiles the permission for both checked wire filters
// and restored subscriptions. Worker consume and ordinary read stay separate;
// malformed worker shapes proceed to their shape refusal in the core.
func SubscriptionCoverage(access *acl.Client, routing *config.Routing, filter string) acl.Coverage {
	if _, service := routing.SeekChannel(filter, true); service {
		return acl.Full
	}
	if index, worker := routing.WorkerChannel(filter); worker {
		if access.AllowsChannel(acl.Consume, routing.Channels[index].Name) {
			return acl.Full
		}
		return acl.None
	}
	switch {
	case filter == "$saguin/queue" || strings.HasPrefix(filter, "$saguin/queue/"):
		return acl.Full
	case strings.HasPrefix(filter, "$saguin/"):
		return acl.None
	case strings.HasPrefix(filter, "$share/"):
		if access.Denied(acl.Share) {
			return acl.None
		}
		_, filter, _ = topic.SplitShared(filter)
	}
	return access.ReadCoverage(filter)
}

func (r *reader) unsubscribe(body []byte) (Event, error) {
	p, err := packet.DecodeUnsubscribe(body, r.version)
	if err != nil {
		return Event{}, err
	}
	u := &Unsubscribe{PacketID: p.PacketID}
	for filter := range p.Filters.All() {
		f := string(filter)
		if _, err := r.checkFilter(f); err != nil {
			return Event{}, err
		}
		u.Filters = append(u.Filters, f)
	}
	var unread []packet.Property
	if r.problemInfo {
		for prop := range p.Properties.Views() {
			if prop.ID == packet.UserProperty && packet.IsSaguinProperty(prop.Name) {
				unread = append(unread, packet.Property{ID: packet.UserProperty,
					Pair: packet.StringPair{Name: "saguin-unread", Value: string(prop.Name)}})
			}
		}
	}
	u.Unread, err = packet.NewProperties(packet.UNSUBACK, unread)
	if err != nil {
		panic("connection: invalid unread properties: " + err.Error())
	}
	return Event{Kind: Received, Conn: r.conn, Packet: u}, nil
}

func (r *reader) checkFilter(filter string) (bool, error) {
	_, part, err := topic.SplitShared(filter)
	if err != nil {
		return false, packet.ErrMalformed
	}
	limits := topic.Limits{
		MaxLength: r.limits.MaxTopicLength, MaxLevels: r.limits.MaxTopicLevels,
	}
	err = topic.CheckFilter(part, limits)
	if errors.Is(err, topic.ErrMalformed) {
		return false, packet.ErrMalformed
	}
	return errors.Is(err, topic.ErrOverLimit) || len(filter) > limits.MaxLength || strings.Count(filter, "/") >= limits.MaxLevels, nil
}
