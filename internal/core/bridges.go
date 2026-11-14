package core

import (
	"cmp"
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// bridgeState is owned only by the core. Closing links remain in connections
// until their writer reports Closed; only then can their next attempt start.
type bridgeState struct {
	since, lastSuback                       time.Time
	unanswered                              uint64
	registered                              bool
	replayOwed                              bool                   // survives links; cleared only by the replay completion commit
	replayHeld                              []store.HeldIdentifier // remaining inbound moves, in arrival order
	replayMark                              *list.Element          // bridge-owned pending bookkeeping, independent of its link
	replayMarkSaving                        bool                   // its mark write has not yet been answered
	id                                      int64
	sent                                    *metrics.Counter
	unsent                                  map[string]*metrics.Counter
	retainWarned                            bool
	lane                                    sessionLane
	received                                *metrics.Counter
	unmatched                               bool // first unmatched record on this link was logged
	releasingAcks                           bool // this batch reached its fixed acknowledgement deadline
	ackDeadline                             coreDeadline
	options                                 connection.DialOptions
	link                                    *connectionState
	cancel                                  context.CancelFunc
	started, up, everUp, attemptDone        bool
	failures, refusals                      uint // saturated counters, never exponential shifts of an unbounded value
	lastFailure                             string
	closedForStop                           bool // the core chose the stop as this live link's close cause
	downAt, nextDial                        time.Time
	peer                                    connection.PeerConnack
	connected                               *metrics.Gauge
	connectedDials, failedDials, reconnects *metrics.Counter
	rules                                   []bridgeRuleState
	retentionLoss                           positionLoss // scratch while applying one deletion's answer
	filters                                 []bridgeFilter
	subscriptions                           []packet.Subscription // core-owned construction scratch; only checked payloads transfer
	granted                                 int
	todo, retry                             []int
	pending                                 *packet.Subscribe // identity only after handover; its bytes belong to the writer
	pendingID                               uint16
	asked                                   []int
	subscriptionDeadline, retryDeadline     coreDeadline
	refusalActive                           bool
}

type bridgeRuleState struct {
	removed        bool // recovered MQTT obligations without a cursor or new selections
	row            *store.BridgeRule
	captures       topic.Captures
	config         config.BridgeRule
	in, out        *metrics.Gauge
	granted, total int
}

type bridgeFilter struct {
	filter  []byte
	rule    int
	granted bool
	refused bool
}

// ConfigureBridges prepares immutable dial input and registers all bridge
// series before any operations listener can scrape. Main calls it before Run.
func (c *Core) ConfigureBridges(cfg *config.Config, m *connection.Metrics, reg *metrics.Registry) {
	c.bridgesByName = make(map[string]*bridgeState, len(cfg.Bridges))
	c.retentionBridges = make([]*bridgeState, 0, len(cfg.Bridges))
	// Main has completed the checks that can refuse startup. Keep cleanup
	// and its notices together in the core's ordinary startup write path.
	if len(c.bridgeRows) != 0 {
		c.enqueue(&storeRequest{kind: registerChannels, op: &store.ReconcileBridges{
			Configured: cfg.Bridges, Bridges: c.bridgeRows, Notices: make([]string, 0, logger.Capacity)}})
	}
	stored := make(map[string]store.Bridge, len(c.bridgeRows))
	var highest int64
	for _, row := range c.bridgeRows {
		stored[row.Name] = row
		highest = max(highest, row.ID)
	}
	c.bridgeRows = nil
	add := &store.AddBridges{}
	for i := range cfg.Bridges {
		configured := &cfg.Bridges[i]
		b := &bridgeState{options: connection.DialOptions{Bridge: *configured, Limits: cfg.Broker.Limits,
			Routing: c.routing, Metrics: m, Dialers: reg.Goroutine("dialer")},
			rules: make([]bridgeRuleState, len(configured.Topics))}
		name := configured.Name
		b.since = time.UnixMilli(c.started)
		b.id, b.replayOwed = stored[name].ID, stored[name].ReplayOwed
		if b.id == 0 {
			highest++
			b.id = highest
			add.Bridges = append(add.Bridges, store.Bridge{ID: b.id, Name: name})
		}
		b.lane.bridge, b.ackDeadline.bridge = b, b
		reg.Info("saguin_bridge_info", "bridge", name, "peer", configured.Peer)
		b.connected = reg.Gauge("saguin_bridge_connected", "bridge", name)
		b.connectedDials = reg.Counter("saguin_bridge_connection_attempts_total", "bridge", name, "result", "connected")
		b.failedDials = reg.Counter("saguin_bridge_connection_attempts_total", "bridge", name, "result", "failed")
		b.reconnects = reg.Counter("saguin_bridge_reconnects_total", "bridge", name)
		b.received = reg.Counter("saguin_bridge_received_total", "bridge", name)
		b.sent = reg.Counter("saguin_bridge_sent_total", "bridge", name)
		b.unsent = make(map[string]*metrics.Counter, 4)
		for _, cause := range []string{"refused", "expired", "topic_invalid", "too_large"} {
			b.unsent[cause] = reg.Counter("saguin_bridge_unsent_total", "bridge", name, "cause", cause)
		}
		for i, rule := range configured.Topics {
			r := &b.rules[i]
			r.config = rule
			if rule.Direction != config.Out {
				r.in = reg.Gauge("saguin_bridge_rule_up", "bridge", name, "direction", "in", "filter", rule.Filter, "topic", rule.Topic)
				r.total = len(rule.Filters)
				for _, filter := range rule.Filters {
					b.filters = append(b.filters, bridgeFilter{filter: []byte(filter), rule: i})
				}
			}
			if rule.Direction != config.In {
				r.out = reg.Gauge("saguin_bridge_rule_up", "bridge", name, "direction", "out", "filter", rule.Filter, "topic", rule.Topic)
				r.row = &store.BridgeRule{Bridge: b.id, Filter: rule.Filter, Topic: rule.Topic}
				r.captures.Plus = make([]topic.Capture, rule.CaptureCount())
				reached := make(map[int64]struct{})
				for _, filter := range rule.Filters {
					for _, index := range c.routing.Reaches(filter) {
						ch := &c.broadcast
						if index >= 0 {
							ch = c.routed[index]
						}
						if _, exists := reached[ch.row.ID]; exists {
							continue
						}
						reached[ch.row.ID] = struct{}{}
						position := ch.row.Floor
						if ch.row.ID == 1 || ch.startTail {
							position = ch.row.Next
						}
						r.row.Cursors = append(r.row.Cursors, store.ChannelPosition{Channel: ch.row.ID, Offset: position})
					}
				}
				slices.SortFunc(r.row.Cursors, func(a, b store.ChannelPosition) int { return cmp.Compare(a.Channel, b.Channel) })
				add.Rules = append(add.Rules, r.row)
				c.bridgeConsumerLimit += len(r.row.Cursors)
			}
		}
		b.subscriptionDeadline.bridge, b.retryDeadline.bridge = b, b
		b.subscriptions = make([]packet.Subscription, 0, len(b.filters))
		c.bridges = append(c.bridges, b)
		c.bridgesByName[name] = b
	}
	slices.SortFunc(c.bridges, func(a, b *bridgeState) int { return strings.Compare(a.options.Bridge.Name, b.options.Bridge.Name) })
	c.bridgeTimeouts.reserve(len(c.bridges))
	c.bridgeRetries.reserve(len(c.bridges))
	c.bridgeAcks.reserve(len(c.bridges))
	if len(add.Bridges) != 0 || len(add.Rules) != 0 {
		c.enqueue(&storeRequest{kind: registerChannels, op: add})
	}
}

func (c *Core) startBridges(acts []Action) []Action {
	if c.phase != running {
		return acts
	}
	for _, b := range c.bridges {
		acts = c.dialBridge(acts, b)
	}
	return acts
}

func (c *Core) dialBridge(acts []Action, b *bridgeState) []Action {
	c.bridgeRetries.set(&b.retryDeadline, time.Time{})
	b.nextDial, b.attemptDone = time.Time{}, false
	c.dialing++
	return append(acts, Action{Reply: b})
}

// startDialer is the core side's only goroutine start for bridges (R1).
// Dial owns all establishment I/O and reports on the shared inbound channel.
func (c *Core) startDialer(b *bridgeState) {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	c.bridgeWorkers.Add(1)
	options := b.options
	options.CleanStart = b.replayOwed
	if c.reloadCopies != nil {
		options.TLS = c.reloadCopies.BridgeTLS(options.Bridge.Name)
	}
	go connection.Dial(ctx, &options, c.bridgeInbound, c.bridgeClock, &c.bridgeWorkers)
}

// WaitBridges joins the dialers and their common connection workers. Main
// calls it after Drained; the core never waits on the group (R2).
func (c *Core) WaitBridges() { c.bridgeWorkers.Wait() }

func (c *Core) stopBridges() {
	for _, b := range c.bridges {
		if b.cancel != nil {
			b.cancel()
		}
		c.clearBridgeTimers(b)
		b.releasingAcks = true
		if b.link != nil {
			c.queueReplies(b.link)
		}
	}
	c.saveBridgeReplayMarks()
}

func (c *Core) clearBridgeTimers(b *bridgeState) {
	b.releasingAcks = false
	c.bridgeAcks.set(&b.ackDeadline, time.Time{})
	c.bridgeTimeouts.set(&b.subscriptionDeadline, time.Time{})
	c.bridgeRetries.set(&b.retryDeadline, time.Time{})
}

// bridgeWait is the design's named random draw: uniform whole milliseconds,
// inclusive at both ends. Saturation precedes the shift and the increment.
func bridgeWait(n uint) time.Duration {
	maximum := min(int64(60000), int64(5000)<<min(n-1, 4))
	return time.Duration(1000+rand.Int64N(maximum-1000+1)) * time.Millisecond
}

func (c *Core) bridgeLog(acts []Action, b *bridgeState, level logger.Level, message string, now time.Time) []Action {
	if !c.logCredits.Admit(level) {
		return acts
	}
	return append(acts, Action{Line: logger.Line{At: now, Level: level,
		Msg: "bridge " + b.options.Bridge.Name + ": " + message}})
}

// A socket's ephemeral local address changes on every attempt. Keep the
// operation and underlying reason so the same failure remains a repeat.
func bridgeError(err error) string {
	if operation, ok := errors.AsType[*net.OpError](err); ok {
		return operation.Op + ": " + operation.Err.Error()
	}
	return err.Error()
}

func (c *Core) bridgeFailed(acts []Action, b *bridgeState, reason string, now time.Time) []Action {
	if b.attemptDone {
		return acts
	}
	b.attemptDone = true
	b.failedDials.Inc()
	level := logger.Warn
	if reason == b.lastFailure {
		level = logger.Debug
	}
	b.lastFailure = reason
	acts = c.bridgeLog(acts, b, level, "dial failed: "+reason, now)
	wait := time.Duration(0)
	if b.failures != 0 {
		wait = bridgeWait(b.failures)
	}
	b.failures = min(b.failures+1, 5)
	b.nextDial = now.Add(wait)
	return acts
}

func (c *Core) bridgeDown(acts []Action, b *bridgeState, reason string, now time.Time) []Action {
	c.clearBridgeTimers(b)
	if b.up {
		b.since = now
		b.up, b.downAt, b.nextDial, b.failures = false, now, now, 1
		b.granted = 0
		b.connected.Set(0)
		for i := range b.rules {
			r := &b.rules[i]
			r.granted = 0
			if r.in != nil {
				r.in.Set(0)
			}
			if r.out != nil {
				r.out.Set(0)
			}
		}
		if b.closedForStop {
			acts = c.bridgeLog(acts, b, logger.Info, "link closed: broker stopping", now)
		} else {
			acts = c.bridgeLog(acts, b, logger.Warn, "link down: "+reason, now)
		}
	} else if c.phase < quiescing {
		acts = c.bridgeFailed(acts, b, reason, now)
	}
	return acts
}

// bridgeEvent handles bridge control exchanges and outcomes, leaving ordinary
// publication, quota and delivery events on their shared core paths.
func (c *Core) bridgeEvent(acts []Action, ev connection.Event, now time.Time) (bool, []Action) {
	switch ev.Kind {
	case connection.DialStarted:
		b := c.bridgesByName[ev.Packet.(string)]
		if !b.started {
			b.started = true
			c.bridgeStarts++
		}
		return true, acts
	case connection.Dialed:
		result := ev.Packet.(connection.DialResult)
		b := c.bridgesByName[result.Name]
		c.dialing--
		b.cancel()
		b.cancel = nil
		if result.Err != nil {
			if c.phase < quiescing {
				acts = c.bridgeFailed(acts, b, bridgeError(result.Err), now)
				c.bridgeRetries.set(&b.retryDeadline, b.nextDial)
			}
			return true, acts
		}
		c.accepted(ev.Conn, connection.Accept{Bridge: result.Name, Remote: ev.Conn.RemoteAddr()})
		s := c.connections[ev.Conn]
		s.bridge, s.version, s.clientID, s.freeIdentifier = b, packet.V5, b.options.Bridge.ClientID, 1
		b.unmatched = false
		b.link = s
		if c.phase >= quiescing {
			acts = c.finish(c.cut(acts, s, 0, now), s, 0, now)
		}
		return true, acts
	}
	s := c.connections[ev.Conn]
	if s == nil || s.bridge == nil {
		return false, acts
	}
	b := s.bridge
	switch ev.Kind {
	case connection.Refused:
		if p := ev.Packet.(connection.PublishRefusal); p.UnmatchedTopic != "" && s.incoming[p.PacketID] == nil && s.phase == connected && !s.end.seen {
			level := logger.Warn
			if b.unmatched {
				level = logger.Debug
			}
			b.unmatched = true
			acts = c.bridgeLog(acts, b, level, "no inbound rule matches "+p.UnmatchedTopic, now)
		}
	case connection.Received:
		if c.phase >= quiescing || s.phase == closing {
			return false, acts
		}
		switch p := ev.Packet.(type) {
		case *connection.PeerConnack:
			if p.ReasonCode != 0 {
				acts = c.bridgeFailed(acts, b, packet.ReasonName(packet.CONNACK, p.ReasonCode), now)
				return true, c.finish(c.cut(acts, s, 0, now), s, 0, now)
			}
			return true, c.bridgeUp(acts, b, p, now)
		case *packet.Ack:
			acts = c.bridgeOutcome(acts, s, packet.PUBACK, p, now)
		case *connection.Ack:
			acts = c.bridgeOutcome(acts, s, p.Type, &p.Ack, now)
		case *packet.Suback:
			return true, c.bridgeSuback(acts, b, p, now)
		}
	case connection.ControlWritten:
		report := ev.Packet.(connection.ControlWrite)
		if b.pending == report.Packet && s.phase == connected && c.phase < quiescing {
			c.bridgeTimeouts.set(&b.subscriptionDeadline, report.At.Add(30*time.Second))
		}
		return true, acts
	case connection.Ended:
		wasClosing := s.phase == closing
		established := b.up
		readFailed := false
		reason := "connection closed"
		switch p := ev.Packet.(type) {
		case connection.ReadFailure:
			readFailed = true
			reason = bridgeError(p.Err)
			ev.Packet = nil
		case *packet.Disconnect:
			reason = packet.ReasonName(packet.DISCONNECT, p.ReasonCode)
		}
		acts = c.bridgeDown(acts, b, reason, now)
		// An establishment timeout or a lost socket cannot consume another
		// close wait before redial. The writer still owns orderly completion.
		if !wasClosing && (!established || (readFailed && ev.Reason == 0)) {
			acts = c.cut(acts, s, 0, now)
		}
		return true, c.ended(acts, ev, now)
	case connection.Closed:
		if failure, ok := ev.Packet.(connection.CannotBuild); ok {
			acts = c.bridgeLog(acts, b, logger.Error, failure.Err.Error(), now)
		}
		b.link, b.pending, b.asked, b.todo, b.retry = nil, nil, nil, nil, nil
		b.replayHeld = nil
		b.closedForStop = false
		c.closed(ev.Conn)
		if c.phase < quiescing {
			c.bridgeRetries.set(&b.retryDeadline, b.nextDial)
		}
		return true, acts
	}
	return false, acts
}

func (c *Core) bridgeUp(acts []Action, b *bridgeState, peer *connection.PeerConnack, now time.Time) []Action {
	b.since = now
	b.peer, b.up, b.attemptDone, b.failures, b.lastFailure = *peer, true, true, 0, ""
	b.link.phase, b.link.maximumPacket = connected, peer.MaximumPacketSize
	b.retainWarned = false
	b.link.stored = true
	b.link.topicAliases = peer.TopicAliasMaximum
	b.link.session.ReceiveMaximum = peer.ReceiveMaximum
	if !peer.SessionPresent || b.replayOwed {
		b.replayOwed = true
		c.queueBridgeReplayMark(b)
		c.saveBridgeReplayMarks()
		c.enqueue(&storeRequest{kind: loadBridgeHeld, conn: b.link, lane: &b.lane, op: &store.LoadBridgeHeld{Bridge: b.id}})
	} else {
		c.loadBridge(b.link)
	}
	b.connected.Set(1)
	b.connectedDials.Inc()
	message := "link up"
	if b.everUp {
		b.reconnects.Inc()
		message += " again after " + now.Sub(b.downAt).Round(time.Second).String()
		if !peer.SessionPresent {
			acts = c.bridgeLog(acts, b, logger.Warn, "peer no longer holds the session", now)
		}
	}
	b.everUp = true
	acts = c.bridgeLog(acts, b, logger.Info, message, now)
	for i := range b.filters {
		f := &b.filters[i]
		f.granted = false
		if !peer.WildcardAvailable && strings.ContainsAny(string(f.filter), "+#") {
			acts = c.bridgeLog(acts, b, logger.Error, fmt.Sprintf("subscription %s: peer does not allow wildcard subscriptions", f.filter), now)
			continue
		}
		size, err := packet.SubscribeSize(packet.V5, nil, 3+len(f.filter))
		if err != nil || (peer.MaximumPacketSize != 0 && uint32(size) > peer.MaximumPacketSize) {
			acts = c.bridgeLog(acts, b, logger.Error, fmt.Sprintf("subscription %s exceeds the peer's Maximum Packet Size", f.filter), now)
		} else {
			b.todo = append(b.todo, i)
		}
	}
	return acts // subscription identifiers are chosen after outbound recovery is loaded
}
