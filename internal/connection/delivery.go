package connection

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/packet"
)

// Delivery transfers one stored message to the writer and back on inbound.
// A gathered write returns its packets as one chain through Next, in write
// order. Each packet keeps its own result; the sender relinquishes the whole
// chain when it reports the first packet.
// Properties has its stored Property Length; Payload is handed straight to
// the socket. The core sizes and reserves it before loading its payload;
// alias choice belongs only to the writer when it sends the packet.
type Delivery struct {
	Next                       *Delivery // next packet in this write report; nil when handed to the writer
	Bridge                     bool      // all channel types expire before a bridge first sends them
	Worker                     bool      // broker offer: replace request properties and omit Message Expiry
	Channel                    int64     // originating channel; 1 is $broadcast
	Slot                       uint32    // opaque core-owned window slot, returned unchanged
	Order                      uint64    // connection's PUBLISH hand-off order, for recovered PUBREL barriers
	Topic, Properties, Payload []byte
	Additions                  []byte // saved delivery properties, including original subscription identifiers
	QoS                        byte
	Retain                     bool
	Dup                        bool
	PacketID                   uint16
	SubscriptionIDs            []uint32
	Expires                    int64 // Unix milliseconds; zero means no expiry
	Result                     DeliveryResult
	Err                        error // damaged stored properties, fatal at the core
	properties                 packet.Properties
	additions                  packet.Properties
	expiryOffset               int
	expiryInterval             uint32
	ownID                      bool
}

// DeliveryResult distinguishes successful writes from terminal discards.
type DeliveryResult uint8

// Terminal outcomes returned on Delivered.
const (
	DeliveryWritten DeliveryResult = iota
	DeliveryExpired
	DeliveryTooLarge
	DeliveryDiscarded // connection ended; does not settle the consumer's position
)

// entries gives waiting replies priority even when a blocking select woke
// for a delivery at the same time. The held entry is part of the same window,
// not a third queue. Final entries end all sending and return queued ownership.
func (w *writer) entries() {
	var held *Delivery
	defer func() {
		if w.resetTimer != nil {
			w.resetTimer(time.Time{})
		}
	}()
	for {
		w.updatePeer()
		if w.conn.client && !w.closed {
			w.readPongs()
			w.checkPong()
		}
		if w.controlReport.Kind != 0 {
			w.waitEntry()
			continue
		}
		if reply := w.nextReply(); reply != nil {
			_, disconnect := reply.(*packet.Disconnect)
			_, closeOnly := reply.(Close)
			if disconnect || closeOnly {
				w.discardDeliveries(held)
				held = nil
			}
			if w.reply(reply) {
				w.discardDeliveries(held)
				return
			}
			continue
		}
		if held != nil {
			w.deliver(held)
			held = nil
			if w.final {
				w.discardDeliveries(nil)
				return
			}
			continue
		}
		if w.heldReply != nil {
			// The core handed off these preceding deliveries before the
			// recovered PUBREL. Keep the reply head, not a third queue.
			held = <-w.conn.deliveries
			continue
		}
		held = w.waitEntry()
	}
}

// waitEntry shares the writer's one timer between idle queue waits and a
// pending completion report, including during delivery collection. Until the
// report is sent, no later entry can overwrite it or report ahead of it.
// Connection-owned keep-alive work continues independently of core intake.
func (w *writer) waitEntry() *Delivery {
	var ticks <-chan time.Time
	if w.conn.client && !w.closed {
		w.keepAliveDue()
		next := w.clientDeadline()
		if !next.Equal(w.timerAt) {
			w.timerAt = next
			if w.resetTimer != nil {
				w.resetTimer(next)
			} else if !next.IsZero() {
				w.timer, w.resetTimer = w.conn.clock.Timer(next)
			}
		}
		if !next.IsZero() {
			ticks = w.timer
		}
	}
	replies, deliveries := w.conn.replies, w.conn.deliveries
	var report chan<- Event
	if w.controlReport.Kind != 0 {
		report = w.inbound
		replies, deliveries = nil, nil
	}
	select {
	case report <- w.controlReport:
		w.controlReport = Event{}
	case reply := <-replies:
		w.heldReply = reply
	case delivery := <-deliveries:
		return delivery
	case peer := <-w.conn.peer:
		w.setPeer(peer)
	case pong := <-w.conn.pongs:
		w.pong(pong)
	case <-ticks:
	}
	return nil
}

// nextReply keeps ordinary reply priority. A recovered PUBREL alone waits for
// the PUBLISHes handed off before it; both entries remain in the two existing
// queues, with at most their heads held by the writer.
func (w *writer) nextReply() Reply {
	if w.heldReply == nil {
		select {
		case w.heldReply = <-w.conn.replies:
		default:
			return nil
		}
	}
	if p, ok := w.heldReply.(*Release); ok && !w.closed && p.After > w.deliveryOrder {
		return nil
	}
	reply := w.heldReply
	w.heldReply = nil
	return reply
}

func (w *writer) discardDeliveries(held *Delivery) {
	for {
		if held != nil {
			held.Result = DeliveryDiscarded
			w.inbound <- Event{Kind: Delivered, Conn: w.conn, Packet: held}
		}
		select {
		case held = <-w.conn.deliveries:
		default:
			return
		}
	}
}

// Publish supplies the checked packet fields for both core sizing and writer
// encoding. The first caller structurally decodes the stored properties;
// subsequent ownership transfers preserve that check. No publish policy or
// topic limit is checked again, and property bytes are never copied here.
// Sizing keeps the full topic and counts a nonzero alias where allowed;
// only the writer chooses the alias and whether to send an empty topic.
func (d *Delivery) Publish(topic []byte, alias uint16) (packet.Publish, error) {
	if err := d.readProperties(); err != nil {
		return packet.Publish{}, err
	}
	expiryOffset := d.expiryOffset
	if d.Expires == 0 {
		expiryOffset = 0
	}
	return packet.Publish{Topic: topic, QoS: d.QoS, Retain: d.Retain, Dup: d.Dup, PacketID: d.PacketID,
		Properties: d.properties, TopicAlias: alias, SubscriptionIDs: d.SubscriptionIDs,
		DeliveryProperties: d.additions,
		ExpiryOffset:       expiryOffset, ExpiryInterval: d.expiryInterval}, nil
}

// SetChannelProperties uses the record's checked properties and adds only
// trusted broker values, including an optional worker offer's request fields.
// Stored and wire additions share the same bytes.
func (d *Delivery) SetChannelProperties(id string, offset, timestamp int64, channel string, worker *packet.WorkerProperties) error {
	if err := d.readProperties(); err != nil {
		return err
	}
	if d.ownID {
		id = ""
	}
	d.Additions, d.additions = packet.ChannelProperties(d.SubscriptionIDs, id, offset, timestamp, d.Expires, channel, worker)
	d.SubscriptionIDs = nil
	if worker != nil {
		d.Expires = 0 // only saguin-expires describes the job's lifetime
	}
	return nil
}

func (d *Delivery) readProperties() error {
	if d.properties == nil {
		var err error
		d.properties, err = packet.ReadStoredProperties(d.Properties, func(p packet.PropertyView) error {
			if p.ID == packet.UserProperty && string(p.Name) == "saguin-id" {
				d.ownID = true
			}
			if p.ID == packet.MessageExpiryInterval {
				d.expiryOffset, d.expiryInterval = p.Offset+2, p.Int
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("store: damaged delivery properties: %w", err)
		}
		if d.Worker {
			d.properties = d.properties.Without(packet.MessageExpiryInterval, packet.ResponseTopic, packet.CorrelationData)
		}
		if d.properties == nil {
			d.properties = packet.Properties{} // checked empty section
		}
		if d.additions == nil {
			d.additions, err = packet.ReadStoredProperties(d.Additions, nil)
			if err != nil {
				return fmt.Errorf("store: damaged delivery additions: %w", err)
			}
		}
	}
	return nil
}

// deliveryHead encodes only the header. Alias assignments include earlier
// headers in this vector, in stream order. A failed write ends the connection,
// so an assignment whose header did not finish can never be used afterwards.
func (w *writer) deliveryHead(d *Delivery) []byte {
	topic := d.Topic
	var alias uint16
	if w.peer.version == packet.V5 && w.peer.aliases != 0 {
		alias = w.aliases[string(topic)]
		if alias != 0 {
			topic = nil
		} else if len(w.aliases) < int(w.peer.aliases) {
			alias = uint16(len(w.aliases) + 1)
		}
	}
	p, err := d.Publish(topic, alias)
	if err != nil {
		d.Err = err
		return nil
	}
	if d.Expires != 0 {
		remaining := d.Expires - w.conn.clock.Now().UnixMilli()
		if remaining <= 0 && !d.Dup && (d.Channel <= 1 || d.Bridge) {
			d.Result = DeliveryExpired
			return nil
		}
		p.ExpiryInterval = uint32(min(int64(p.ExpiryInterval), max(0, (remaining+999)/1000)))
	}
	layout, err := packet.LayoutPublish(w.peer.version, &p, len(d.Payload))
	if err != nil {
		d.Result = DeliveryTooLarge
		return nil
	}
	if w.peer.maxSize != 0 && int64(layout.Size()) > int64(w.peer.maxSize) {
		d.Result = DeliveryTooLarge
		return nil
	}
	head := layout.AppendHead(nil, w.peer.version, &p)
	if alias != 0 && len(topic) != 0 {
		if w.aliases == nil {
			w.aliases = make(map[string]uint16)
		}
		w.aliases[string(d.Topic)] = alias
	}
	return head
}

// SetBridgeProperties keeps ordinary MQTT properties and supplies exactly
// the record id. No local channel metadata crosses a bridge.
func (d *Delivery) SetBridgeProperties(id string) error {
	var err error
	d.ownID = false
	d.properties, err = packet.FilterStoredProperties(d.Properties, func(p packet.PropertyView) bool {
		if p.ID == packet.UserProperty && packet.IsSaguinProperty(p.Name) {
			if string(p.Name) != "saguin-id" || string(p.Value) != id || d.ownID {
				return true
			}
			d.ownID = true
		}
		if p.ID == packet.MessageExpiryInterval {
			d.expiryOffset, d.expiryInterval = p.Offset+2, p.Int
		}
		return false
	})
	if err != nil {
		return fmt.Errorf("store: damaged delivery properties: %w", err)
	}
	if d.properties == nil {
		d.properties = packet.Properties{}
	}
	d.additions = nil
	if !d.ownID {
		d.additions, err = packet.NewProperties(packet.PUBLISH, []packet.Property{{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-id", Value: id}}})
	}
	return err
}
