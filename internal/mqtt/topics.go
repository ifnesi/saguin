// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

var (
	SharePrefix = "$SHARE" // the prefix indicating a share topic
	SysPrefix   = "$SYS"   // the prefix indicating a system info topic
)

// TopicAliases contains inbound and outbound topic alias registrations.
type TopicAliases struct {
	Inbound  *InboundTopicAliases
	Outbound *OutboundTopicAliases
}

// NewTopicAliases returns an instance of TopicAliases.
func NewTopicAliases(topicAliasMaximum uint16) TopicAliases {
	return TopicAliases{
		Inbound:  NewInboundTopicAliases(topicAliasMaximum),
		Outbound: NewOutboundTopicAliases(topicAliasMaximum),
	}
}

// NewInboundTopicAliases returns a pointer to InboundTopicAliases.
func NewInboundTopicAliases(topicAliasMaximum uint16) *InboundTopicAliases {
	return &InboundTopicAliases{maximum: topicAliasMaximum} // its map is made by the first alias (Set)
}

// InboundTopicAliases contains a map of topic aliases received from the client.
type InboundTopicAliases struct {
	internal map[uint16]string
	sync.RWMutex
	maximum uint16
}

// Set sets a new alias for a specific topic.
//
// A zero length topic is not a registration, it is a request for the topic
// this alias already stands for. That mapping exists only if it was made on
// this same network connection, because [MQTT-3.3.2-7] forbids carrying one
// across connections, so a client that reconnects and goes on using its
// aliases arrives here with nothing to resolve. Set returns the empty string
// to say so, and never stores it: an alias bound to no topic is what section
// 3.3.2.3.4 case 3a calls a Protocol Error, and the caller is what raises it.
func (a *InboundTopicAliases) Set(id uint16, topic string) string {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return topic // ?
	}

	if topic == "" {
		return a.internal[id] // empty when this connection never registered one
	}

	if a.internal == nil {
		a.internal = map[uint16]string{}
	}
	a.internal[id] = topic
	return topic
}

// OutboundTopicAliases contains a map of topic aliases sent from the broker to the client.
type OutboundTopicAliases struct {
	internal map[string]uint16
	sync.RWMutex
	cursor  uint32
	maximum uint16
}

// NewOutboundTopicAliases returns a pointer to OutboundTopicAliases.
func NewOutboundTopicAliases(topicAliasMaximum uint16) *OutboundTopicAliases {
	return &OutboundTopicAliases{maximum: topicAliasMaximum} // its map is made by the first alias (Set)
}

// Set sets a new topic alias for a topic and returns the alias value, and a boolean
// indicating if the alias already existed.
func (a *OutboundTopicAliases) Set(topic string) (uint16, bool) {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return 0, false
	}

	if i, ok := a.internal[topic]; ok {
		return i, true
	}

	i := atomic.LoadUint32(&a.cursor)
	if i+1 > uint32(a.maximum) {
		// if i+1 > math.MaxUint16 {
		return 0, false
	}

	if a.internal == nil {
		a.internal = map[string]uint16{}
	}
	a.internal[topic] = uint16(i) + 1
	atomic.StoreUint32(&a.cursor, i+1)
	return uint16(i) + 1, false
}

// SharedSubscriptions contains a map of subscriptions to a shared filter,
// keyed on share group then client id.
type SharedSubscriptions struct {
	internal map[string]map[string]packets.Subscription
	sync.RWMutex
}

// NewSharedSubscriptions returns a new instance of Subscriptions.
//
// Its map is made by the first Add (saguin): a node of the topic index has
// one, and nearly none is ever given a shared subscription.
func NewSharedSubscriptions() *SharedSubscriptions {
	return &SharedSubscriptions{}
}

// Add creates a new shared subscription for a group and client id pair.
func (s *SharedSubscriptions) Add(group, id string, val packets.Subscription) {
	s.Lock()
	defer s.Unlock()
	if s.internal == nil {
		s.internal = map[string]map[string]packets.Subscription{}
	}
	if _, ok := s.internal[group]; !ok {
		s.internal[group] = map[string]packets.Subscription{}
	}
	s.internal[group][id] = val
}

// Delete deletes a client id from a shared subscription group, and reports
// whether it was in it.
func (s *SharedSubscriptions) Delete(group, id string) bool {
	s.Lock()
	defer s.Unlock()
	_, existed := s.internal[group][id]
	delete(s.internal[group], id)
	if len(s.internal[group]) == 0 {
		delete(s.internal, group)
	}
	return existed
}

// Get returns the subscription properties for a client id in a share group, if one exists.
func (s *SharedSubscriptions) Get(group, id string) (val packets.Subscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.internal[group]; !ok {
		return val, ok
	}

	val, ok = s.internal[group][id]
	return val, ok
}

// GroupLen returns the number of groups subscribed to the filter.
func (s *SharedSubscriptions) GroupLen() int {
	s.RLock()
	defer s.RUnlock()
	val := len(s.internal)
	return val
}

// Len returns the total number of shared subscriptions to a filter across all groups.
func (s *SharedSubscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	n := 0
	for _, group := range s.internal {
		n += len(group)
	}
	return n
}

// any reports whether fn answers true for the filter of a subscription in any
// group, stopping at the first that does, without copying the maps. It is
// asked each member's filter, as gatherSharedSubscriptions keys them: two
// members of one group may have spelt the share prefix differently.
func (s *SharedSubscriptions) any(fn func(filter string) bool) bool {
	s.RLock()
	defer s.RUnlock()
	for _, group := range s.internal {
		for _, sub := range group {
			if fn(sub.Filter) {
				return true
			}
		}
	}
	return false
}

// GetAll returns all shared subscription groups and their subscriptions.
func (s *SharedSubscriptions) GetAll() map[string]map[string]packets.Subscription {
	s.RLock()
	defer s.RUnlock()
	m := map[string]map[string]packets.Subscription{}
	for group, subs := range s.internal {
		if _, ok := m[group]; !ok {
			m[group] = map[string]packets.Subscription{}
		}

		for id, sub := range subs {
			m[group][id] = sub
		}
	}
	return m
}

// InlineSubFn is the signature for a callback function which will be called
// when an inline client receives a message on a topic it is subscribed to.
// The sub argument contains information about the subscription that was matched for any filters.
type InlineSubFn func(cl *Client, sub packets.Subscription, pk packets.Packet)

// InlineSubscriptions represents a map of internal subscriptions keyed on client.
type InlineSubscriptions struct {
	internal map[int]InlineSubscription
	sync.RWMutex
}

// NewInlineSubscriptions returns a new instance of InlineSubscriptions.
//
// Its map is made by the first Add (saguin), as SharedSubscriptions' is.
func NewInlineSubscriptions() *InlineSubscriptions {
	return &InlineSubscriptions{}
}

// Add adds a new internal subscription for a client id.
func (s *InlineSubscriptions) Add(val InlineSubscription) {
	s.Lock()
	defer s.Unlock()
	if s.internal == nil {
		s.internal = map[int]InlineSubscription{}
	}
	s.internal[val.Identifier] = val
}

// GetAll returns all internal subscriptions.
func (s *InlineSubscriptions) GetAll() map[int]InlineSubscription {
	s.RLock()
	defer s.RUnlock()
	m := map[int]InlineSubscription{}
	for k, v := range s.internal {
		m[k] = v
	}
	return m
}

// Get returns an internal subscription for a client id.
func (s *InlineSubscriptions) Get(id int) (val InlineSubscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok = s.internal[id]
	return val, ok
}

// Len returns the number of internal subscriptions.
func (s *InlineSubscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	val := len(s.internal)
	return val
}

// Delete removes an internal subscription by the client id.
func (s *InlineSubscriptions) Delete(id int) {
	s.Lock()
	defer s.Unlock()
	delete(s.internal, id)
}

// Subscriptions is a map of subscriptions keyed on client.
//
// **One subscription is held inline, and a map is made only for a second**
// (saguin). A filter a session holds is kept here twice - in the client's
// own, keyed on filter, and in the topic index's node for the filter, keyed
// on client - and the commonest session holds one filter that no other
// session holds: a device subscribing to its own command topic. A map of one
// such entry is some 750 bytes, so each of those sessions carried two of
// them for what fits in the struct. A second entry moves both into a map,
// and a map left holding one moves it back.
type Subscriptions struct {
	internal map[string]packets.Subscription // every subscription where there are two or more; nil otherwise
	oneID    string                          // the one subscription's id, where there is one and no map
	one      packets.Subscription
	hasOne   bool
	sync.RWMutex
}

// NewSubscriptions returns a new instance of Subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{}
}

// Add adds a new subscription for a client. ID can be a filter in the
// case this map is client state, or a client id if particle state.
func (s *Subscriptions) Add(id string, val packets.Subscription) {
	s.Lock()
	defer s.Unlock()
	switch {
	case s.internal != nil:
		s.internal[id] = val
	case !s.hasOne || s.oneID == id:
		s.oneID, s.one, s.hasOne = id, val, true
	default:
		s.internal = map[string]packets.Subscription{s.oneID: s.one, id: val}
		s.oneID, s.one, s.hasOne = "", packets.Subscription{}, false
	}
}

// GetAll returns all subscriptions.
func (s *Subscriptions) GetAll() map[string]packets.Subscription {
	m := map[string]packets.Subscription{}
	s.any(func(k string, v packets.Subscription) bool {
		m[k] = v
		return false
	})
	return m
}

// Each calls fn for every subscription, without copying the map. GetAll
// copies it, which on a filter thousands of clients hold is the copy plus
// its garbage, per topic looked up.
func (s *Subscriptions) Each(fn func(client string, sub packets.Subscription)) {
	s.any(func(client string, sub packets.Subscription) bool {
		fn(client, sub)
		return false
	})
}

// any reports whether fn answers true for a subscription, stopping at the
// first that does, without copying the map.
func (s *Subscriptions) any(fn func(client string, sub packets.Subscription) bool) bool {
	s.RLock()
	defer s.RUnlock()
	if s.hasOne {
		return fn(s.oneID, s.one)
	}
	for client, sub := range s.internal {
		if fn(client, sub) {
			return true
		}
	}
	return false
}

// Get returns a subscriptions for a specific client or filter id.
func (s *Subscriptions) Get(id string) (val packets.Subscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	if s.hasOne {
		if s.oneID == id {
			return s.one, true
		}
		return val, false
	}
	val, ok = s.internal[id]
	return val, ok
}

// Len returns the number of subscriptions.
func (s *Subscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	if s.hasOne {
		return 1
	}
	return len(s.internal)
}

// Delete removes a subscription by client or filter id, and reports whether
// there was one to remove.
func (s *Subscriptions) Delete(id string) bool {
	s.Lock()
	defer s.Unlock()
	if s.hasOne {
		if s.oneID != id {
			return false
		}
		s.oneID, s.one, s.hasOne = "", packets.Subscription{}, false
		return true
	}
	_, existed := s.internal[id]
	delete(s.internal, id)
	if len(s.internal) == 1 {
		for k, v := range s.internal {
			s.oneID, s.one, s.hasOne = k, v, true
		}
		s.internal = nil
	}
	return existed
}

// ClientSubscriptions is a map of aggregated subscriptions for a client.
type ClientSubscriptions map[string]packets.Subscription

type InlineSubscription struct {
	packets.Subscription
	Handler InlineSubFn
}

// Subscribers contains the shared and non-shared subscribers matching a topic.
type Subscribers struct {
	Shared              map[string]map[string]packets.Subscription
	SharedSelected      map[string]packets.Subscription
	Subscriptions       map[string]packets.Subscription
	InlineSubscriptions map[int]InlineSubscription
}

// SelectShared returns one subscriber for each shared subscription group.
func (s *Subscribers) SelectShared() {
	s.SharedSelected = map[string]packets.Subscription{}
	for _, subs := range s.Shared {
		for client, sub := range subs {
			cls, ok := s.SharedSelected[client]
			if !ok {
				cls = sub
			}

			s.SharedSelected[client] = cls.Merge(sub)
			break
		}
	}
}

// MergeSharedSelected merges the selected subscribers for a shared subscription group
// and the non-shared subscribers, to ensure that no subscriber gets multiple messages
// due to have both types of subscription matching the same filter.
func (s *Subscribers) MergeSharedSelected() {
	for client, sub := range s.SharedSelected {
		cls, ok := s.Subscriptions[client]
		if !ok {
			cls = sub
		}

		s.Subscriptions[client] = cls.Merge(sub)
	}
}

// TopicsIndex is a prefix/trie tree containing topic subscribers and retained messages.
type TopicsIndex struct {
	Retained *packets.Packets
	root     *particle // a leaf containing a message and more leaves.
}

// NewTopicsIndex returns a pointer to a new instance of Index.
func NewTopicsIndex() *TopicsIndex {
	return &TopicsIndex{
		Retained: packets.NewPackets(),
		root: &particle{
			particles:     newParticles(),
			subscriptions: NewSubscriptions(),
		},
	}
}

// InlineSubscribe adds a new internal subscription for a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) InlineSubscribe(subscription InlineSubscription) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var existed bool
	n := x.set(subscription.Filter, 0)
	_, existed = n.inlineSubscriptions.Get(subscription.Identifier)
	n.inlineSubscriptions.Add(subscription)

	return !existed
}

// InlineUnsubscribe removes an internal subscription for a topic filter associated with a specific client,
// returning true if the subscription existed.
func (x *TopicsIndex) InlineUnsubscribe(id int, filter string) bool {
	x.root.Lock()
	defer x.root.Unlock()

	particle := x.seek(filter, 0)
	if particle == nil {
		return false
	}

	particle.inlineSubscriptions.Delete(id)

	if particle.inlineSubscriptions.Len() == 0 {
		x.trim(particle)
	}
	return true
}

// Subscribe adds a new subscription for a client to a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) Subscribe(client string, subscription packets.Subscription) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var existed bool
	prefix, _ := isolateParticle(subscription.Filter, 0)
	if strings.EqualFold(prefix, SharePrefix) {
		group, _ := isolateParticle(subscription.Filter, 1)
		n := x.set(subscription.Filter, 2)
		_, existed = n.shared.Get(group, client)
		n.shared.Add(group, client, subscription)
	} else {
		n := x.set(subscription.Filter, 0)
		_, existed = n.subscriptions.Get(client)
		n.subscriptions.Add(client, subscription)
	}

	return !existed
}

// Unsubscribe removes a subscription filter for a client, returning true if the
// subscription existed.
func (x *TopicsIndex) Unsubscribe(filter, client string) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var d int
	prefix, _ := isolateParticle(filter, 0)
	shareSub := strings.EqualFold(prefix, SharePrefix)
	if shareSub {
		d = 2
	}

	particle := x.seek(filter, d)
	if particle == nil {
		return false
	}

	var removed bool
	if shareSub {
		group, _ := isolateParticle(filter, 1)
		removed = particle.shared.Delete(group, client)
	} else {
		removed = particle.subscriptions.Delete(client)
	}

	x.trim(particle)
	return removed
}

// RetainMessage saves a message payload to the end of a topic address. Returns
// 1 if a retained message was added, and -1 if the retained message was removed.
// 0 is returned if sequential empty payloads are received.
func (x *TopicsIndex) RetainMessage(pk packets.Packet) int64 {
	x.root.Lock()
	defer x.root.Unlock()

	n := x.set(pk.TopicName, 0)
	n.Lock()
	defer n.Unlock()
	if len(pk.Payload) > 0 {
		n.retainPath = pk.TopicName
		x.Retained.Add(pk.TopicName, pk)
		return 1
	}

	var out int64
	if pke, ok := x.Retained.Get(pk.TopicName); ok && len(pke.Payload) > 0 && pke.FixedHeader.Retain {
		out = -1 // if a retained packet existed, return -1
	}

	n.retainPath = ""
	x.Retained.Delete(pk.TopicName) // [MQTT-3.3.1-6] [MQTT-3.3.1-7]
	x.trim(n)

	return out
}

// ExpireRetained removes the retained message on a topic if expired says the
// value stored right now is expired, and reports whether it did. Judging and
// removing happen under the locks RetainMessage takes, so a value published
// after the caller last looked is judged on its own merits rather than
// deleted with the one it replaced; the node's retainPath is cleared and the
// node trimmed with it.
func (x *TopicsIndex) ExpireRetained(topic string, expired func(packets.Packet) bool) bool {
	x.root.Lock()
	defer x.root.Unlock()

	pk, ok := x.Retained.Get(topic)
	if !ok || !expired(pk) {
		return false
	}

	n := x.seek(topic, 0)
	if n != nil {
		n.Lock()
		defer n.Unlock()
		n.retainPath = ""
	}
	x.Retained.Delete(topic)
	if n != nil {
		x.trim(n)
	}
	return true
}

// set creates a topic address in the index and returns the final particle.
func (x *TopicsIndex) set(topic string, d int) *particle {
	var key string
	var hasNext = true
	n := x.root
	for hasNext {
		key, hasNext = isolateParticle(topic, d)
		d++

		p := n.particles.get(key)
		if p == nil {
			p = newParticle(key, n)
			n.particles.add(p)
		}
		n = p
	}

	return n
}

// seek finds the particle at a specific index in a topic filter.
func (x *TopicsIndex) seek(filter string, d int) *particle {
	var key string
	var hasNext = true
	n := x.root
	for hasNext {
		key, hasNext = isolateParticle(filter, d)
		n = n.particles.get(key)
		d++
		if n == nil {
			return nil
		}
	}

	return n
}

// trim removes empty filter particles from the index.
func (x *TopicsIndex) trim(n *particle) {
	for n.parent != nil && n.retainPath == "" && n.particles.len()+n.subscriptions.Len()+n.shared.Len()+n.inlineSubscriptions.Len() == 0 {
		key := n.key
		n = n.parent
		n.particles.delete(key)
	}
}

// Messages returns a slice of any retained messages which match a filter.
func (x *TopicsIndex) Messages(filter string) []packets.Packet {
	return x.scanMessages(filter, 0, nil, []packets.Packet{})
}

// scanMessages returns all retained messages on topics matching a given filter.
func (x *TopicsIndex) scanMessages(filter string, d int, n *particle, pks []packets.Packet) []packets.Packet {
	if n == nil {
		n = x.root
	}

	if len(filter) == 0 || x.Retained.Len() == 0 {
		return pks
	}

	if !strings.ContainsRune(filter, '#') && !strings.ContainsRune(filter, '+') {
		if pk, ok := x.Retained.Get(filter); ok {
			pks = append(pks, pk)
		}
		return pks
	}

	key, hasNext := isolateParticle(filter, d)
	if key == "+" || key == "#" || d == -1 {
		for _, adjacent := range n.particles.getAll() {
			if d == 0 && adjacent.key == SysPrefix {
				continue
			}

			if !hasNext {
				// retainPath is written by RetainMessage under the particle
				// lock; read it under the same lock (#200).
				adjacent.Lock()
				retainPath := adjacent.retainPath
				adjacent.Unlock()
				if retainPath != "" {
					if pk, ok := x.Retained.Get(retainPath); ok {
						pks = append(pks, pk)
					}
				}
			}

			if hasNext || (d >= 0 && key == "#") {
				pks = x.scanMessages(filter, d+1, adjacent, pks)
			}
		}
		return pks
	}

	if particle := n.particles.get(key); particle != nil {
		if hasNext {
			return x.scanMessages(filter, d+1, particle, pks)
		}

		particle.Lock()
		retainPath := particle.retainPath
		particle.Unlock()
		if pk, ok := x.Retained.Get(retainPath); ok {
			pks = append(pks, pk)
		}
	}

	return pks
}

// Subscribers returns a map of clients who are subscribed to matching filters,
// their subscription ids and highest qos.
func (x *TopicsIndex) Subscribers(topic string) *Subscribers {
	return x.scanSubscribers(topic, 0, nil, &Subscribers{
		Shared:              map[string]map[string]packets.Subscription{},
		SharedSelected:      map[string]packets.Subscription{},
		Subscriptions:       map[string]packets.Subscription{},
		InlineSubscriptions: map[int]InlineSubscription{},
	})
}

// EachSubscriber calls fn with the client of every ordinary subscription
// matching a topic, shared and inline subscriptions aside, and may call it
// more than once for a client holding several matching filters.
//
// **It is Subscribers without the copying.** Subscribers builds four maps
// and merges a packets.Subscription per matching client, which is most of
// what looking a topic up costs where a channel has thousands of
// subscribers - and a caller that only wants to know who matched pays all
// of it. saguin's delivery path takes identity from here and everything
// else from its own records.
func (x *TopicsIndex) EachSubscriber(topic string, fn func(client string)) {
	x.scanEach(topic, 0, nil, fn)
}

func (x *TopicsIndex) scanEach(topic string, d int, n *particle, fn func(client string)) {
	if n == nil {
		n = x.root
	}
	if len(topic) == 0 {
		return
	}

	gather := func(p *particle) {
		p.subscriptions.Each(func(client string, sub packets.Subscription) {
			// Don't match $ topics with top level wildcards
			// [MQTT-4.7.2-1], as gatherSubscriptions does.
			if len(sub.Filter) > 0 && topic[0] == '$' && (sub.Filter[0] == '+' || sub.Filter[0] == '#') {
				return
			}
			fn(client)
		})
	}

	key, hasNext := isolateParticle(topic, d)
	for _, partKey := range []string{key, "+"} {
		if particle := n.particles.get(partKey); particle != nil { // [MQTT-3.3.2-3]
			if hasNext {
				x.scanEach(topic, d+1, particle, fn)
			} else {
				gather(particle)
				if wild := particle.particles.get("#"); wild != nil && partKey != "+" {
					gather(wild) // filter/# matches filter, as per 4.7.1.2
				}
			}
		}
	}

	if particle := n.particles.get("#"); particle != nil {
		gather(particle)
	}
}

// AnySubscriber reports whether ordinary answers true for a client's
// ordinary subscription matching topic, or shared for the filter of a
// shared subscription matching it, and stops at the first that does.
// Inline subscriptions are not asked about.
//
// **It asks the question Subscribers answers, without building the
// answer.** The broadcast log asks, of every publish at QoS 1 or 2, whether
// any session it holds or any shared group with a cursor is owed it, and
// asking through Subscribers built four maps and merged a Subscription per
// matching client for a yes or no - the second of two walks every such
// publish took. The walk and the [MQTT-4.7.2-1] rule are scanSubscribers';
// only the gathering differs: nothing is merged, since a client's merged
// QoS is the highest of its matching subscriptions and asking each of them
// finds the same one. The functions run under the particle's read lock, so
// they must not take a lock a subscriber change can hold.
func (x *TopicsIndex) AnySubscriber(topic string, ordinary func(client string, sub packets.Subscription) bool, shared func(filter string) bool) bool {
	return x.scanAny(topic, 0, x.root, ordinary, shared)
}

func (x *TopicsIndex) scanAny(topic string, d int, n *particle, ordinary func(string, packets.Subscription) bool, shared func(string) bool) bool {
	if len(topic) == 0 {
		return false
	}

	gather := func(p *particle) bool {
		return p.subscriptions.any(func(client string, sub packets.Subscription) bool {
			// Don't match $ topics with top level wildcards
			// [MQTT-4.7.2-1], as gatherSubscriptions does.
			if len(sub.Filter) > 0 && topic[0] == '$' && (sub.Filter[0] == '+' || sub.Filter[0] == '#') {
				return false
			}
			return ordinary(client, sub)
		}) || p.shared.any(shared)
	}

	key, hasNext := isolateParticle(topic, d)
	for _, partKey := range []string{key, "+"} {
		if particle := n.particles.get(partKey); particle != nil { // [MQTT-3.3.2-3]
			if hasNext {
				if x.scanAny(topic, d+1, particle, ordinary, shared) {
					return true
				}
				continue
			}
			if gather(particle) {
				return true
			}
			if wild := particle.particles.get("#"); wild != nil && partKey != "+" && gather(wild) {
				return true // filter/# matches filter, as per 4.7.1.2
			}
		}
	}

	if particle := n.particles.get("#"); particle != nil {
		return gather(particle)
	}
	return false
}

// scanSubscribers returns a list of client subscriptions matching an indexed topic address.
func (x *TopicsIndex) scanSubscribers(topic string, d int, n *particle, subs *Subscribers) *Subscribers {
	if n == nil {
		n = x.root
	}

	if len(topic) == 0 {
		return subs
	}

	key, hasNext := isolateParticle(topic, d)
	for _, partKey := range []string{key, "+"} {
		if particle := n.particles.get(partKey); particle != nil { // [MQTT-3.3.2-3]
			if hasNext {
				x.scanSubscribers(topic, d+1, particle, subs)
			} else {
				x.gatherSubscriptions(topic, particle, subs)
				x.gatherSharedSubscriptions(particle, subs)
				x.gatherInlineSubscriptions(particle, subs)

				if wild := particle.particles.get("#"); wild != nil && partKey != "+" {
					x.gatherSubscriptions(topic, wild, subs) // also match any subs where filter/# is filter as per 4.7.1.2
					x.gatherSharedSubscriptions(wild, subs)
					x.gatherInlineSubscriptions(particle, subs)
				}
			}
		}
	}

	if particle := n.particles.get("#"); particle != nil {
		x.gatherSubscriptions(topic, particle, subs)
		x.gatherSharedSubscriptions(particle, subs)
		x.gatherInlineSubscriptions(particle, subs)
	}

	return subs
}

// gatherSubscriptions collects any matching subscriptions, and gathers any identifiers or highest qos values.
func (x *TopicsIndex) gatherSubscriptions(topic string, particle *particle, subs *Subscribers) {
	if subs.Subscriptions == nil {
		subs.Subscriptions = map[string]packets.Subscription{}
	}

	for client, sub := range particle.subscriptions.GetAll() {
		if len(sub.Filter) > 0 && topic[0] == '$' && (sub.Filter[0] == '+' || sub.Filter[0] == '#') { // don't match $ topics with top level wildcards [MQTT-4.7.2-1]
			continue
		}

		cls, ok := subs.Subscriptions[client]
		if !ok {
			cls = sub
		}

		subs.Subscriptions[client] = cls.Merge(sub)
	}
}

// gatherSharedSubscriptions gathers all shared subscriptions for a particle.
func (x *TopicsIndex) gatherSharedSubscriptions(particle *particle, subs *Subscribers) {
	if subs.Shared == nil {
		subs.Shared = map[string]map[string]packets.Subscription{}
	}

	for _, shares := range particle.shared.GetAll() {
		for client, sub := range shares {
			if _, ok := subs.Shared[sub.Filter]; !ok {
				subs.Shared[sub.Filter] = map[string]packets.Subscription{}
			}

			subs.Shared[sub.Filter][client] = sub
		}
	}
}

// gatherInlineSubscriptions gathers all inline subscriptions for a particle.
func (x *TopicsIndex) gatherInlineSubscriptions(particle *particle, subs *Subscribers) {
	if subs.InlineSubscriptions == nil {
		subs.InlineSubscriptions = map[int]InlineSubscription{}
	}

	for id, inline := range particle.inlineSubscriptions.GetAll() {
		subs.InlineSubscriptions[id] = inline
	}
}

// isolateParticle extracts a particle between d / and d+1 / without allocations.
func isolateParticle(filter string, d int) (particle string, hasNext bool) {
	var next, end int
	for i := 0; end > -1 && i <= d; i++ {
		end = strings.IndexRune(filter, '/')

		switch {
		case d > -1 && i == d && end > -1:
			hasNext = true
			particle = filter[next:end]
		case end > -1:
			hasNext = false
			filter = filter[end+1:]
		default:
			hasNext = false
			particle = filter[next:]
		}
	}

	return
}

// IsSharedFilter returns true if the filter uses the share prefix.
func IsSharedFilter(filter string) bool {
	prefix, _ := isolateParticle(filter, 0)
	return strings.EqualFold(prefix, SharePrefix)
}

// IsValidFilter returns true if the filter is valid.
func IsValidFilter(filter string, forPublish bool) bool {
	if !forPublish && len(filter) == 0 { // publishing can accept zero-length topic filter if topic alias exists, so we don't enforce for publish.
		return false // [MQTT-4.7.3-1]
	}

	if forPublish {
		if len(filter) >= len(SysPrefix) && strings.EqualFold(filter[0:len(SysPrefix)], SysPrefix) {
			// 4.7.2 Non-normative - The Server SHOULD prevent Clients from using such Topic Names [$SYS] to exchange messages with other Clients.
			return false
		}

		if strings.ContainsRune(filter, '+') || strings.ContainsRune(filter, '#') {
			return false //[MQTT-3.3.2-2]
		}
	}

	wildhash := strings.IndexRune(filter, '#')
	if wildhash >= 0 && wildhash != len(filter)-1 { // [MQTT-4.7.1-1]
		return false
	}

	prefix, hasNext := isolateParticle(filter, 0)
	if !hasNext && strings.EqualFold(prefix, SharePrefix) {
		return false // [MQTT-4.8.2-1]
	}

	if hasNext && strings.EqualFold(prefix, SharePrefix) {
		group, hasNext := isolateParticle(filter, 1)
		if !hasNext {
			return false // [MQTT-4.8.2-1]
		}

		if strings.ContainsRune(group, '+') || strings.ContainsRune(group, '#') {
			return false // [MQTT-4.8.2-2]
		}
	}

	return true
}

// particle is a child node on the tree.
type particle struct {
	key                 string               // the key of the particle
	parent              *particle            // a pointer to the parent of the particle
	particles           particles            // a map of child particles
	subscriptions       *Subscriptions       // a map of subscriptions made by clients to this ending address
	shared              *SharedSubscriptions // a map of shared subscriptions keyed on group name
	inlineSubscriptions *InlineSubscriptions // a map of inline subscriptions for this particle
	retainPath          string               // path of a retained message
	sync.Mutex                               // mutex for when making changes to the particle
}

// newParticle returns a pointer to a new instance of particle.
func newParticle(key string, parent *particle) *particle {
	return &particle{
		key:                 key,
		parent:              parent,
		particles:           newParticles(),
		subscriptions:       NewSubscriptions(),
		shared:              NewSharedSubscriptions(),
		inlineSubscriptions: NewInlineSubscriptions(),
	}
}

// particles is a concurrency safe map of particles.
type particles struct {
	internal map[string]*particle
	sync.RWMutex
}

// newParticles returns a map of particles, made by the first add (saguin):
// the node at the end of every filter is a leaf, with no particles at all.
func newParticles() particles {
	return particles{}
}

// add adds a new particle.
func (p *particles) add(val *particle) {
	p.Lock()
	if p.internal == nil {
		p.internal = map[string]*particle{}
	}
	p.internal[val.key] = val
	p.Unlock()
}

// getAll returns all particles.
func (p *particles) getAll() map[string]*particle {
	p.RLock()
	defer p.RUnlock()
	m := map[string]*particle{}
	for k, v := range p.internal {
		m[k] = v
	}
	return m
}

// get returns a particle by id (key).
func (p *particles) get(id string) *particle {
	p.RLock()
	defer p.RUnlock()
	return p.internal[id]
}

// len returns the number of particles.
func (p *particles) len() int {
	p.RLock()
	defer p.RUnlock()
	val := len(p.internal)
	return val
}

// delete removes a particle.
func (p *particles) delete(id string) {
	p.Lock()
	defer p.Unlock()
	delete(p.internal, id)
}
