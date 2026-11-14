// Package core owns broker state and decides the actions each event needs.
// Handle, Answer and Fire make decisions; Run carries them out (R1, R2).
package core

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// Core holds state owned solely by the core goroutine. Connections remain
// until Closed; away sessions live only in the store, except while a request
// concerning them is unfinished. No decision reads a metric or the clock.
type Core struct {
	life                lifeLog
	pendingLogs         []Action // admitted state-change lines, bounded by log credit
	diskFull            bool
	bridges             []*bridgeState // complete name order
	bridgesByName       map[string]*bridgeState
	bridgeRows          []store.Bridge // startup identities consumed by ConfigureBridges
	bridgeConsumerLimit int            // configured rule/channel owners
	bridgeStarts        int
	dialing             int
	bridgeWorkers       sync.WaitGroup // main joins only after Drained
	bridgeInbound       chan<- connection.Event
	bridgeClock         connection.Clock
	bridgeTimeouts      deadlineQueue
	bridgeRetries       deadlineQueue
	bridgeAcks          deadlineQueue
	bridgeReplayMarks   list.List       // refused replay marks waiting for an ordinary commit
	reloadCopies        *config.Reloads // publication only; decisions use the event-owned copies
	reloads             chan Reload
	reloaded            chan struct{}
	reloadCounts        map[string]map[string]*metrics.Counter
	retentionReady      bool
	nextRequest         uint64
	publishBarrier      uint64 // pending batches at or before this request cannot grow
	takeoverCuts        deadlineQueue
	deliveryRetries     list.List
	access              *acl.File
	accessChannels      []config.Channel // original configuration, for access-file compilation
	incomingHeld        int64
	incomingGauge       *metrics.Gauge
	abandoned           *metrics.Counter
	held                int64
	heldGauge           *metrics.Gauge
	willsNotAuthorized  *metrics.Counter
	eventAt             time.Time // current event observation, never a clock read
	admission           admissionState
	listening           chan struct{}
	health              chan *views.Request
	operationRequests   list.List // store-reading view credits cover pending and dispatched reads together
	refusals            history[string, refusal]
	positionLosses      history[positionLossKey, positionLoss]
	retentionBridges    []*bridgeState // reusable scratch for one deletion's bridge observations
	listenersStarted    bool
	wasReady            bool
	readyGauge          *metrics.Gauge
	goroutine           *metrics.Gauge
	noCreditAt          time.Time
	noCredit            *metrics.DurationCounter
	readyReplies        []*connectionState // owners with completions to order at this event's end
	readActions         []Action           // reusable event output; replies precede newly dispatched reads

	limits                config.Limits
	receiveMaximum        uint32
	connections           map[*connection.Conn]*connectionState
	firstConn             *connectionState // listener name, then that listener's accept order
	lastConn              *connectionState
	nextConnection        uint64
	nextConsumer          uint64
	sessions              map[string]*connectionState // connected clients only
	sessionsByID          map[int64]*connectionState  // same owners; orphaned backing rows carry only this id
	nextSession           int64
	shares                map[string]*sharedGroup // only groups with a connected or pending member
	sharesByID            map[int64]*sharedGroup
	nextShare             int64
	sharesDropped         map[int64]*metrics.Counter
	protocols             [packet.V5 + 1]protocolCount
	endedCounts           [endCauses]*metrics.Counter
	capped                *metrics.Counter
	writes                chan<- store.Write
	reads                 chan<- store.Read
	lines                 chan<- logger.Line
	logCredits            *logger.Credits
	readPermit            *storeRequest         // oldest eligible read owns the next returned slot
	requests              map[any]*storeRequest // dispatched reads; writes use outstanding
	unconfirmed           int                   // decision reads whose views still hold later writes
	lanes                 map[string]*sessionLane
	parked                map[*storeRequest]struct{} // cleanup index, never a dispatch scan
	creditWait            list.List
	metadataWait          list.List // retention pages waiting for the shared metadata budget
	batchWait             list.List
	writeWait             list.List
	orderWait             list.List
	orderedRead           *storeRequest
	memoryReady           bool
	ready                 []*storeRequest
	readyAt               int // consumed prefix; retain queue storage across events
	expiry                sessionExpiry
	willExpiry            sessionExpiry
	started               int64 // recovery boundary in Unix milliseconds
	willCount             int64 // stored rows; no per-away-session state
	retained              retainedState
	retryAt               int64         // full-store retry, in Unix milliseconds
	shutdown              chan struct{} // main closes after every listener has ended
	initialized           chan struct{} // core closes after the startup pass
	drained               chan struct{} // core closes after every connection and request
	failures              chan error    // core sends its first fatal store error
	phase                 corePhase
	failure               error
	stopAt                time.Time         // during stopping, zero means the cutoff has fired
	memoryPeer            int64             // outgoing reservations held for peer input or writer completion
	memoryUsed            [2]int64          // incoming and outgoing reservations, independently bounded (R6)
	memoryHead            [3]*memoryRequest // incoming readers, outgoing deliveries, incoming Wills
	memoryTail            [3]*memoryRequest // incoming lists merge by ticket when reader grants are eligible
	memoryTicket          uint64            // original order for parked incoming requests and prerequisite Wills
	memoryFullAt          [2]time.Time
	memoryEpisodes        [2]memoryEpisode
	memoryGauge           [2]*metrics.Gauge
	memoryFull            [2]*metrics.DurationCounter
	broadcast             channelState
	channels              []*channelState // stored id order, including $broadcast
	routing               *config.Routing
	routed                []*channelState         // routing's indexes mapped to stable stored identities
	batchLevels           [channelBatches + 1]int // number of channels with each unresolved batch count
	channelFull           *metrics.Counter
	storeFull             *metrics.Counter
	commitEvery           time.Duration
	commitMaximum         int
	batchCount            int // accepted publication batches, through their final result
	bodyCount             int // potential batches reserved by granted or CONNECT-held bodies
	pendingBatches        int // publication lists awaiting common batch admission
	snapshot              snapshotState
	syncEvery             time.Duration
	syncAt                time.Time
	syncDirty             bool
	syncPending           bool // the one log sync, pending or dispatched
	commitAt              time.Time
	collecting            *writeGroup
	sealed                []*writeGroup
	outstanding           *writeGroup // the sole dispatched write transaction
	sessionWrites         *writeGroup // latest accepted group changing session-owned state
	commits               [commitCauses]*metrics.Counter
	committed             *metrics.Counter
	subscriptionRefused   [256]*metrics.Counter
	metricAt, metricEvery int64
	metricsReading        bool
	startupMetrics        bool // final startup observation requested; readiness awaits its answer
	metricGauges          store.MetricGauges
	metricMeasure         store.Read
	metricChannels        []store.RetentionPolicy
	metadataLimit         int
	metadataReads         int
	wakeTicket            uint64        // all consumers woken by one event share its ticket
	deliveryReady         consumerQueue // free windows needing metadata
	deliveryDeferred      consumerQueue // ready consumers waiting for metadata-page credit or coverage
	deliveryReadyChanged  bool          // reconsider page sharing only when ready membership changes
	deliveryLoads         consumerQueue // reserved prefixes ready for a payload read
	deliveryChanges       consumerQueue // durable exchanges and dirty progress
	queueHolds            deadlineQueue // active application holds
	queueRetries          deadlineQueue // one next retry per queue
	inflight              int64         // handed-off QoS 1/2 exchanges awaiting acknowledgement
	inflightGauge         *metrics.Gauge
	deliveries            [3]*metrics.Counter
	deliveriesExpired     *metrics.Counter
	requestsTotal         [serviceKinds][requestResults]*metrics.Counter
	noResponseTopic       *metrics.Counter
	willsPublished        [willCauses]*metrics.Counter
	willsCancelled        *metrics.Counter
	willsRetainedFull     *metrics.Counter
	willsChannelFull      *metrics.Counter
}

type endCause uint8

const (
	cleanStart endCause = iota
	expired
	connectionEnd
	endCauses
)

// protocolCount keeps the core's count beside the gauge it sets.
type protocolCount struct {
	connected int64
	gauge     *metrics.Gauge
}

// CheckChannels validates stored identities against the startup configuration
// before the broker starts its core and listeners. The store itself loads rows;
// configuration policy belongs to the broker.
func CheckChannels(cfg *config.Config, state *store.State) error {
	configured := make(map[string]store.ChannelType)
	for i := range cfg.Channels {
		ch := &cfg.Channels[i]
		configured[ch.Name] = store.ChannelType(ch.Type) + 1
		if ch.Type == config.Queue {
			configured[ch.Name+"__dlq"] = store.Append
		}
	}
	for i := range state.Channels {
		ch := &state.Channels[i]
		if ch.ID == 1 {
			continue
		}
		kind, exists := configured[ch.Name]
		if !exists {
			return fmt.Errorf("stored channel %s is no longer configured", ch.Name)
		}
		if kind != ch.Type {
			return fmt.Errorf("stored channel %s is configured with another type", ch.Name)
		}
	}
	return nil
}

// New returns a core for resolved cfg and the store's startup state. It
// registers every series the core owns, at zero, before the first scrape.
func New(cfg *config.Config, reg *metrics.Registry, state *store.State,
	writes chan<- store.Write, reads chan<- store.Read, lines chan<- logger.Line, credits *logger.Credits) *Core {
	c := &Core{access: cfg.ACL, accessChannels: cfg.Channels, reloadCopies: cfg.Live,
		life:    lifeLog{restored: state.SessionsRestored},
		reloads: make(chan Reload, 1), reloaded: make(chan struct{}, 1), reloadCounts: make(map[string]map[string]*metrics.Counter),
		listening: make(chan struct{}), health: make(chan *views.Request, InboundCapacity),
		readyGauge: reg.Gauge("saguin_ready"), goroutine: reg.Goroutine("core"),
		limits: cfg.Broker.Limits, receiveMaximum: uint32(cfg.Broker.QoS2.MaxInflightPerClient),
		connections: make(map[*connection.Conn]*connectionState), sessions: make(map[string]*connectionState),
		sessionsByID: make(map[int64]*connectionState),
		shares:       make(map[string]*sharedGroup), sharesByID: make(map[int64]*sharedGroup), nextShare: state.HighestShare, sharesDropped: make(map[int64]*metrics.Counter),
		nextSession: state.HighestSession, writes: writes, reads: reads, bridgeRows: state.Bridges,
		lines: lines, logCredits: credits,
		requests: make(map[any]*storeRequest), lanes: make(map[string]*sessionLane),
		shutdown:    make(chan struct{}),
		initialized: make(chan struct{}), drained: make(chan struct{}), failures: make(chan error, 1),
		capped: reg.Counter("saguin_session_expiry_capped_total"),
	}
	for _, part := range []string{"log_and_origins", "access", "tls"} {
		c.reloadCounts[part] = make(map[string]*metrics.Counter)
		for _, result := range []string{"applied", "refused"} {
			c.reloadCounts[part][result] = reg.Counter("saguin_reloads_total", "part", part, "result", result)
		}
	}
	for i, request := range serviceNames {
		for j, result := range requestResultNames {
			c.requestsTotal[i][j] = reg.Counter("saguin_requests_total", "request", request, "result", result)
		}
	}
	c.noResponseTopic = reg.Counter("saguin_publish_refused_total", "cause", "no_response_topic")
	c.admission.delay, c.admission.peerTimeout = cfg.Broker.DeliveryAdmissionDelay, cfg.Broker.DeliveryPeerTimeout
	c.admission.gauge = reg.Gauge("saguin_delivery_admission_gated")
	c.admission.seconds = reg.DurationCounter("saguin_delivery_admission_gated_seconds_total")
	c.started, c.willExpiry.next, c.willCount = state.Started, state.NextWill, state.Wills
	c.protocols[packet.V311].gauge = reg.Gauge("saguin_connections", "protocol", "3.1.1")
	c.protocols[packet.V5].gauge = reg.Gauge("saguin_connections", "protocol", "5")
	for i, cause := range [...]string{"clean_start", "expired", "connection_end"} {
		c.endedCounts[i] = reg.Counter("saguin_sessions_ended_total", "cause", cause)
	}
	if cfg.Broker.Storage.Mode == config.Normal {
		c.syncEvery = cfg.Broker.Storage.SyncInterval
		c.syncDirty = true // startup recovery also commits through the writer
	}
	if cfg.Broker.Storage.Mode == config.Memory {
		c.snapshot = snapshotState{
			every:  cfg.Broker.Storage.SnapshotInterval,
			dirty:  state.SnapshotDirty,
			source: cfg.MemoryPath(), destination: cfg.Broker.Storage.FilePath,
			ok:          reg.Counter("saguin_snapshots_total", "result", "ok"),
			failed:      reg.Counter("saguin_snapshots_total", "result", "failed"),
			lastSuccess: reg.SecondsGauge("saguin_snapshot_last_success_timestamp_seconds"),
			duration:    reg.SecondsGauge("saguin_snapshot_last_duration_seconds"),
		}
	}
	c.commitEvery = cfg.Broker.Storage.PublishCommitInterval
	c.commitMaximum = cfg.Broker.Storage.PublishCommitMaxRecords
	for _, cause := range [...]struct {
		value commitCause
		name  string
	}{
		{commitEvent, "event"}, {commitInterval, "interval"}, {commitMaxRecords, "max_records"},
		{commitBarrier, "barrier"}, {commitStop, "stop"},
	} {
		c.commits[cause.value] = reg.Counter("saguin_store_commits_total", "closed_by", cause.name)
	}
	c.committed = reg.Counter("saguin_store_committed_records_total")
	c.noCredit = reg.DurationCounter("saguin_store_no_credit_seconds_total")
	for class, direction := range [...]string{"incoming", "outgoing"} {
		c.memoryGauge[class] = reg.Gauge("saguin_memory_in_transit_bytes", "direction", direction)
		reg.Gauge("saguin_memory_max_bytes", "direction", direction).Set(c.memoryLimit(class))
		c.memoryFull[class] = reg.DurationCounter("saguin_memory_full_seconds_total", "direction", direction)
	}
	c.metadataLimit = cfg.Broker.Storage.ReadConnections
	for i, qos := range [...]string{"0", "1", "2"} {
		c.deliveries[i] = reg.Counter("saguin_deliveries_total", "qos", qos)
	}
	c.deliveriesExpired = reg.Counter("saguin_deliveries_expired_total")
	c.inflightGauge = reg.Gauge("saguin_inflight")
	c.routing = config.NewRouting(cfg.Channels)
	c.routed = make([]*channelState, len(c.routing.Channels))
	configured := make(map[string]int, len(c.routed))
	for i := range c.routing.Channels {
		ch := &c.routing.Channels[i]
		configured[ch.Name] = i
	}
	var highest int64
	for i := range state.Channels {
		ch := &state.Channels[i]
		highest = max(highest, ch.ID)
		owner := &c.broadcast
		if ch.ID != 1 {
			owner = new(channelState)
			c.routed[configured[ch.Name]] = owner
		}
		owner.row, owner.committedTail, owner.resolved = *ch, ch.Next, ch.Next
		owner.stored = reg.Counter("saguin_records_stored_total", "channel", ch.Name)
		c.channels = append(c.channels, owner)
	}
	add := &store.AddChannels{}
	for i := range c.routing.Channels {
		ch := &c.routing.Channels[i]
		owner := c.routed[i]
		if owner == nil {
			highest++
			row := store.Channel{ID: highest, Name: ch.Name, Type: store.ChannelType(ch.Type) + 1, Next: 1, Floor: 1}
			owner = &channelState{row: row, committedTail: 1, resolved: 1,
				stored: reg.Counter("saguin_records_stored_total", "channel", ch.Name)}
			c.routed[i] = owner
			c.channels = append(c.channels, owner)
			add.Channels = append(add.Channels, row)
		}
		owner.maximum = int64(ch.MaxBytes)
		owner.period, owner.retentionBytes = ch.RetentionPeriod.Milliseconds(), int64(ch.RetentionBytes)
		owner.startTail = ch.Start == config.Tail
		owner.tombstonePeriod = ch.TombstoneRetentionPeriod.Milliseconds()
	}
	for i := range c.routing.Channels {
		ch := &c.routing.Channels[i]
		if ch.Type == config.Queue {
			owner := c.routed[i]
			owner.deadLetter = c.routed[configured[ch.Name+"__dlq"]]
			for j, reason := range [...]string{"expired", "evicted", "attempts_exhausted"} {
				owner.deadLettered[j] = reg.Counter("saguin_queue_dead_lettered_total", "channel", ch.Name, "reason", reason)
			}
		}
	}
	for _, owner := range c.channels {
		if owner.row.Type == store.Latest && owner.row.Records != 0 {
			cfgChannel := &c.routing.Channels[configured[owner.row.Name]]
			c.enqueue(&storeRequest{kind: countUnmatchedValues, channel: owner,
				op: &store.CountUnmatchedValues{Channel: owner.row.ID, Before: owner.row.Next, Filters: cfgChannel.Filters}})
		}
		filter := "#"
		if owner != &c.broadcast {
			filter = c.routing.Channels[configured[owner.row.Name]].Filter
		}
		kind := [...]string{"", "append", "latest", "queue"}[owner.row.Type]
		reg.Info("saguin_channel_info", "channel", owner.row.Name, "type", kind, "filter", filter)
		owner.records = reg.Gauge("saguin_channel_records", "channel", owner.row.Name)
		owner.records.Set(owner.row.Records)
		owner.tail = reg.Gauge("saguin_channel_next_offset", "channel", owner.row.Name)
		owner.tail.Set(owner.row.Next)
		owner.filters = make(map[string]map[string]map[*consumerState]struct{})
		if owner.row.Type == store.Queue {
			c.initQueue(owner, &c.routing.Channels[configured[owner.row.Name]], reg)
		}
		owner.missed = reg.Counter("saguin_missed_total", "channel", owner.row.Name, "cause", "too_large")
		owner.retentionMissed = reg.Counter("saguin_missed_total", "channel", owner.row.Name, "cause", "retention")
		c.sharesDropped[owner.row.ID] = reg.Counter("saguin_shares_dropped_total", "channel", owner.row.Name)
		c.sharesDropped[owner.row.ID].Add(state.SharedDropped[owner.row.ID])
		owner.removed = reg.Counter("saguin_channel_retention_removed_total", "channel", owner.row.Name)
		if owner.row.Type != store.Latest {
			owner.floor = reg.Gauge("saguin_channel_floor_offset", "channel", owner.row.Name)
			owner.floor.Set(owner.row.Floor)
		}
		owner.bytes = reg.Gauge("saguin_channel_bytes", "channel", owner.row.Name)
		owner.bytes.Set(owner.row.Bytes)
		consumers := reg.Gauge("saguin_channel_consumers", "channel", owner.row.Name)
		consumers.Set(owner.row.Consumers)
		lowest := reg.OptionalGauge("saguin_channel_position_min", "channel", owner.row.Name)
		if owner.row.PositionMin.Valid {
			value := owner.row.PositionMin.Int64
			lowest.Set(&value)
		}
		c.metricChannels = append(c.metricChannels, store.RetentionPolicy{
			Channel: owner.row.ID, Bytes: owner.bytes, Consumers: consumers, PositionMin: lowest})
	}
	if len(add.Channels) != 0 {
		c.enqueue(&storeRequest{kind: registerChannels, op: add})
	}
	c.batchLevels[0] = len(c.channels)
	c.channelFull = reg.Counter("saguin_publish_refused_total", "cause", "channel_full")
	c.storeFull = reg.Counter("saguin_publish_refused_total", "cause", "store_full")
	for i, cause := range [...]string{"lost", "disconnect", "takeover", "session_end", "restart"} {
		c.willsPublished[i] = reg.Counter("saguin_wills_published_total", "cause", cause)
	}
	c.incomingGauge = reg.Gauge("saguin_qos2_held")
	c.abandoned = reg.Counter("saguin_qos2_abandoned_total")
	c.abandoned.Add(state.Abandoned)
	c.retryAt = state.RetryAt
	c.heldGauge = reg.Gauge("saguin_subscriptions_held")
	c.willsNotAuthorized = reg.Counter("saguin_wills_dropped_total", "cause", "not_authorized")
	c.willsCancelled = reg.Counter("saguin_wills_cancelled_total")
	c.willsRetainedFull = reg.Counter("saguin_wills_dropped_total", "cause", "retained_full")
	c.willsChannelFull = reg.Counter("saguin_wills_dropped_total", "cause", "channel_full")
	c.retained = retainedState{maximum: int64(cfg.Broker.Retained.MaxBytes),
		period:  cfg.Broker.Retained.RetentionPeriod.Milliseconds(),
		charged: max(state.RetainedCharge, state.RetainedBytes),
		bytes:   state.RetainedBytes, values: state.RetainedValues,
		full:             reg.Counter("saguin_publish_refused_total", "cause", "retained_full"),
		reservedProperty: reg.Counter("saguin_publish_refused_total", "cause", "reserved_property")}
	c.retained.next = state.RetainedExpiry.Int64
	if c.retained.period != 0 && state.RetainedTime.Valid {
		c.retained.next = earlier(c.retained.next, timestamp.AfterMilli(state.RetainedTime.Int64, c.retained.period))
	}
	c.broadcast.retentionAt = c.retained.next
	c.broadcast.period = cfg.Broker.Broadcast.RetentionPeriod.Milliseconds()
	c.broadcast.retentionBytes = int64(cfg.Broker.Broadcast.RetentionBytes)
	for _, ch := range c.channels {
		if ch.row.Next > 1 {
			// The shared walk handles overdue age and size from the floor
			// and establishes each deadline before startup is ready.
			c.scheduleRetention(ch)
		}
	}
	c.metricGauges = store.MetricGauges{
		RetainedValues: reg.Gauge("saguin_retained_values"), RetainedBytes: reg.Gauge("saguin_retained_bytes"),
		Away: reg.Gauge("saguin_sessions_away"), Subscriptions: reg.Gauge("saguin_subscriptions"),
		Wills: reg.Gauge("saguin_wills_waiting"),
	}
	c.metricGauges.Set(state.MetricCounts, 0)
	c.metricGauges.RetainedValues.Set(state.RetainedValues)
	c.metricGauges.RetainedBytes.Set(state.RetainedBytes)
	c.metricMeasure = state.Measure
	c.metricEvery = time.Minute.Milliseconds()
	if cfg.Broker.Operations != nil {
		c.metricEvery = cfg.Broker.Operations.MinScrapeInterval.Milliseconds()
	}
	if state.Started != 0 {
		c.metricAt = state.Started + c.metricEvery
	}
	// 0x80 is the MQTT 3.1.1-only refusal series named by metrics.md.
	for _, code := range [...]byte{0x87, 0x8F, 0x97, 0x83, 0x80} {
		c.subscriptionRefused[code] = reg.Counter("saguin_subscriptions_refused_total", "reason", metrics.ReasonLabel(packet.SUBACK, code))
	}
	return c
}

// Action asks the loop to send a reply or delivery, store request or admitted log line,
// grant message bytes, cut a raw socket or start a bridge dialer:
// exactly one kind per action. A read carries its originating connection,
// nil for a sweep. A write is a group whose members retain their owners.
type Action struct {
	Conn  *connection.Conn
	Reply connection.Reply // *connection.Delivery selects the second queue; *bridgeState starts its dialer
	Write store.Write
	Read  store.Read
	Line  logger.Line
	Cut   bool
	Cause connection.Closure // selected before a raw-socket cut
	Bytes int64              // grant to the reader's one outstanding body request
}

// Handle appends ev's actions in execution order, using Conn by identity
// only. The event's time supplies every decision and persisted timestamp.
func (c *Core) Handle(acts []Action, ev connection.Event, now time.Time) []Action {
	c.wakeTicket++
	if c.admission.delay != 0 {
		c.eventAt = now
	}
	if ev.Kind == connection.DialStarted || ev.Kind == connection.Dialed || (ev.Conn != nil && ev.Conn.ClientRole()) {
		handled, next := c.bridgeEvent(acts, ev, now)
		acts = next
		if handled {
			return c.dispatch(acts, now)
		}
	}
	switch ev.Kind {
	case connection.Accepted:
		c.accepted(ev.Conn, ev.Packet.(connection.Accept))
		if c.phase >= stopping {
			s := c.connections[ev.Conn]
			if c.stopCutDue(now) {
				acts = c.cut(acts, s, 0x8B, now)
			}
			acts = c.finish(acts, s, 0x8B, now)
		}
	case connection.Received:
		if c.phase >= quiescing {
			switch ev.Packet.(type) {
			case *packet.Publish, *connection.RoutedPublish, *connection.RequestPublish:
				c.returnBytes(c.takeBodyBytes(ev, now), now)
			}
			break
		}
		switch p := ev.Packet.(type) {
		case *connection.RoutedPublish:
			acts = c.publish(acts, ev, &p.Publish, now)
		case *packet.Publish:
			acts = c.publish(acts, ev, p, now)
		case *connection.RequestPublish:
			acts = c.publish(acts, ev, &p.Publish, now)
		case *packet.Ack:
			c.puback(ev.Conn, p, now)
		case *connection.Ack:
			switch p.Type {
			case packet.PUBREC:
				acts = c.pubrec(acts, ev.Conn, &p.Ack, now)
			case packet.PUBREL:
				acts = c.pubrel(acts, ev.Conn, p.PacketID)
			case packet.PUBCOMP:
				c.pubcomp(ev.Conn, p.PacketID, now)
			}
		case *packet.Connect:
			acts = c.connect(acts, ev.Conn, p, ev.Reason, now)
		case *connection.Subscribe, *connection.Unsubscribe:
			c.subscriptions(ev.Conn, p)
		case connection.Pingreq:
			acts = c.ping(acts, ev.Conn)
		}
	case connection.Ended:
		c.recordClosure(ev)
		acts = c.logReadEnd(acts, ev, now)
		// Operations reports carry no MQTT connection. They share log
		// admission with reader refusals, without ending or counting one.
		if ev.Conn != nil {
			acts = c.ended(acts, ev, now)
		}
	case connection.Closed:
		c.recordClosure(ev)
		if report, ok := ev.Packet.(connection.CannotBuild); ok {
			acts = c.logFailure(acts, logger.Error, report.Err, now)
		}
		c.closed(ev.Conn)
	case connection.BytesRequested:
		c.requestBytes(ev, now)
	case connection.Delivered:
		if p, ok := ev.Packet.(*connection.Release); ok {
			acts = c.released(acts, ev.Conn, p, now)
		} else {
			s := c.connections[ev.Conn]
			for p := ev.Packet.(*connection.Delivery); p != nil; {
				// Settlement resets the returned descriptor, including Next.
				next := p.Next
				acts = c.delivered(acts, s, p, now)
				// Preserve each packet's actions before settling its successor.
				acts = c.dispatch(acts, now)
				p = next
			}
			return acts // the last packet has already dispatched its work
		}
	case connection.Refused:
		acts = c.refusedPublish(acts, ev, now)
	}
	return c.dispatch(acts, now)
}

// Logged processes a line's completion result, returning only its credit.
func (c *Core) Logged(r logger.Result) {
	c.logCredits.Return(r)
}

// logFailure admits peer refusals and broker failures at their stated level.
func (c *Core) logFailure(acts []Action, level logger.Level, err error, now time.Time) []Action {
	if !c.logCredits.Admit(level) {
		return acts
	}
	return append(acts, Action{Line: logger.Line{At: now, Level: level, Msg: err.Error()}})
}

// useACL supplies access-file bytes through the same parser used at startup.
// It is called before CONNECTs; runtime replacement belongs to access reload.
func (c *Core) useACL(data []byte) error {
	cfg := config.Config{Channels: c.accessChannels}
	cfg.Broker.Limits = c.limits
	file, err := cfg.ParseACL(data, "acl_file")
	if err != nil {
		return err
	}
	c.access = file
	return nil
}
