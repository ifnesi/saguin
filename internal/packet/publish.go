package packet

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
)

// ErrTopicAlias is the PUBLISH protocol error with its own DISCONNECT code
// (MQTT 5, 3.3.4). It remains a protocol error for codec callers.
var ErrTopicAlias = fmt.Errorf("%w: topic alias invalid", ErrProtocol)

// PUBLISH flags from MQTT 5 Figure 3-8 and MQTT 3.1.1 Figure 3.10.
const (
	publishDup      = 0x08
	publishQoS      = 0x06
	publishQoSShift = 1
	publishRetain   = 0x01
	publishFlags    = publishDup | publishQoS | publishRetain
)

// publishTopicLengthSize is the Topic Name's Two Byte Integer length prefix.
const publishTopicLengthSize = 2

// Publish is a PUBLISH's flags, variable header and payload. PacketID is
// present only at QoS 1 and 2; only MQTT 5 carries Properties. Topic,
// Properties and Payload share the source buffer after decoding.
type Publish struct {
	Dup        bool
	QoS        byte
	Retain     bool
	Topic      []byte
	PacketID   uint16
	Properties Properties
	Payload    []byte
	// StoredProperties is the reader's property section in stored form:
	// Property Length first, Topic Alias removed, nil for none. Compressed
	// handoffs detach it from the read body. Decoders leave it nil;
	// encoders use Properties alone.
	StoredProperties []byte
	StoredPayload    []byte // reader-prepared S2 copy; nil when stored as received
	PayloadSize      int    // original length when the reader replaces Payload with StoredPayload
	// Delivery additions are broker-made, checked values. ExpiryOffset is
	// one plus the value's byte offset in Properties, or zero when absent.
	// The encoder writes these straight into the head without another section.
	TopicAlias         uint16
	SubscriptionIDs    []uint32
	DeliveryProperties Properties // saved broker additions, structurally checked by the delivery reader
	ExpiryOffset       int
	ExpiryInterval     uint32
}

// DecodePublish decodes a complete PUBLISH body with its fixed header's
// four flag bits. Topic, Properties and Payload share src. Topic validity,
// alias mappings and rules needing connection state or sender role are the
// caller's checks. Payload bytes are never checked against their format
// indicator, as design.md's Choices MQTT leaves open requires.
func DecodePublish(src []byte, flags byte, version Version) (Publish, error) {
	return DecodePublishProperties(src, flags, version, nil)
}

// DecodePublishProperties calls check while decoding each property, after
// checking its wire representation and numeric rules. The view borrows src;
// check must not mutate it. This lets a connection check the sender's rules
// without another traversal of the properties. A caller owning src may
// subsequently remove a connection-only property at its recorded Offset.
func DecodePublishProperties(src []byte, flags byte, version Version, check func(PropertyView) error) (Publish, error) {
	if (version != V311 && version != V5) || len(src) > maxVarInt {
		return Publish{}, ErrMalformed
	}
	qos, err := PublishQoS(flags)
	if err != nil {
		return Publish{}, err
	}
	topic, rest, err := decodeStringBytes(src)
	if err != nil {
		return Publish{}, err
	}
	p := Publish{Dup: flags&publishDup != 0, QoS: qos, Retain: flags&publishRetain != 0, Topic: topic}
	if qos != 0 {
		p.PacketID, rest, err = DecodeUint16(rest)
		if err != nil {
			return Publish{}, err
		}
		if p.PacketID == 0 {
			return Publish{}, ErrProtocol
		}
	}
	if version == V5 {
		values := publishPropertyValues{check: check}
		p.Properties, rest, err = decodeProperties(rest, atPUBLISH, values.valid)
		if err != nil {
			return Publish{}, err
		}
		if len(topic) == 0 && !values.alias {
			return Publish{}, ErrProtocol
		}
	}
	p.Payload = rest
	return p, nil
}

// PublishQoS checks the live fixed-header flags and returns their QoS.
// A reader can reject these errors before reserving or reading a body;
// DecodePublish shares the same check for callers with complete packets.
func PublishQoS(flags byte) (byte, error) {
	qos := (flags & publishQoS) >> publishQoSShift
	if flags&^publishFlags != 0 || qos > maxQoS {
		return 0, ErrMalformed
	}
	if flags&publishDup != 0 && qos == 0 {
		return 0, ErrProtocol
	}
	return qos, nil
}

// publishPropertyValues records Topic Alias presence in the same pass that
// checks the property values, for the decoder's empty Topic Name rule.
type publishPropertyValues struct {
	alias bool
	check func(PropertyView) error
}

func (v *publishPropertyValues) valid(p PropertyView) error {
	if p.ID == TopicAlias {
		v.alias = true
		if p.Int == 0 {
			return ErrTopicAlias
		}
	}
	if !validPublishProperty(p.ID, p.Int) {
		return ErrProtocol
	}
	if v.check != nil {
		return v.check(p)
	}
	return nil
}

// validPublishProperty applies the numeric value rules in MQTT 5 section
// 3.3.2.3, shared by the packet decoder and NewProperties.
func validPublishProperty(id byte, value uint32) bool {
	switch id {
	case PayloadFormatIndicator:
		return value <= 1
	case TopicAlias, SubscriptionIdentifier:
		return value != 0
	default:
		return true
	}
}

// AppendPublish appends a complete PUBLISH packet, including its fixed
// header. The caller supplies a supported version and checked Topic and
// Properties; an empty Topic Name with an outgoing alias is its choice.
// ErrCannotEncode leaves dst unchanged for invalid flags, a Packet Identifier
// at the wrong QoS, or lengths MQTT cannot carry.
// Rules needing connection state or sender role are the caller's checks;
// payload bytes are written unaltered.
func AppendPublish(dst []byte, version Version, p *Publish) ([]byte, error) {
	if p == nil {
		return dst, ErrCannotEncode
	}
	layout, err := LayoutPublish(version, p, len(p.Payload))
	if err != nil {
		return dst, err
	}
	dst = slices.Grow(dst, layout.Size())
	dst = appendPublishHead(dst, version, p, layout.body, layout.properties)
	return append(dst, p.Payload...), nil
}

// PublishSize returns the encoded size without examining checked topic or
// property bytes, or requiring a payload to have been loaded.
func PublishSize(version Version, p *Publish, payloadSize int) (int, error) {
	layout, err := LayoutPublish(version, p, payloadSize)
	if err != nil {
		return 0, err
	}
	return layout.Size(), nil
}

// PublishLayout retains checked lengths so a writer can check the peer's
// packet-size limit and then encode without sizing the same packet again.
type PublishLayout struct {
	body, properties, size, head int
}

// LayoutPublish sizes checked topic and properties without requiring a loaded
// payload. The returned layout belongs to these packet fields and version;
// changing lengths requires a new layout.
func LayoutPublish(version Version, p *Publish, payloadSize int) (PublishLayout, error) {
	body, properties, err := publishBodySize(version, p, payloadSize)
	if err != nil {
		return PublishLayout{}, err
	}
	size := 1 + varIntSize(uint32(body)) + body
	return PublishLayout{body: body, properties: properties, size: size, head: size - payloadSize}, nil
}

// Size is the complete encoded packet size, including the payload.
func (l PublishLayout) Size() int { return l.size }

// AppendHead encodes the sized packet's fixed and variable headers alone.
// The caller keeps its version and packet lengths unchanged since sizing.
func (l PublishLayout) AppendHead(dst []byte, version Version, p *Publish) []byte {
	dst = slices.Grow(dst, l.head)
	return appendPublishHead(dst, version, p, l.body, l.properties)
}

func publishBodySize(version Version, p *Publish, payloadSize int) (bodySize, propertySize int, err error) {
	if p == nil || p.QoS > maxQoS || (p.Dup && p.QoS == 0) ||
		(p.QoS == 0) != (p.PacketID == 0) || len(p.Topic) > math.MaxUint16 ||
		(version != V311 && version != V5) {
		return 0, 0, ErrCannotEncode
	}
	bodySize = publishTopicLengthSize + len(p.Topic)
	if p.QoS != 0 {
		bodySize += packetIDSize
	}
	if version == V5 {
		propertySize = publishPropertySize(p)
		bodySize += varIntSize(uint32(propertySize)) + propertySize
	}
	// Bound the payload before adding its length, so sizing cannot overflow.
	if payloadSize < 0 || payloadSize > maxVarInt-bodySize {
		return 0, 0, ErrCannotEncode
	}
	return bodySize + payloadSize, propertySize, nil
}

func publishPropertySize(p *Publish) int {
	n := len(p.Properties) + len(p.DeliveryProperties)
	if p.TopicAlias != 0 {
		n += 3
	}
	return n + subscriptionPropertySize(p.SubscriptionIDs)
}

// SubscriptionProperties stores broker-made delivery additions in the same
// wire form as record properties, with a Property Length. No section is kept
// when there are no additions. Identifiers were checked at SUBSCRIBE.
func SubscriptionProperties(ids []uint32) []byte {
	n := subscriptionPropertySize(ids)
	if n == 0 {
		return nil
	}
	dst := make([]byte, 0, varIntSize(uint32(n))+n)
	dst = binaryAppendVarInt(dst, uint32(n))
	return appendSubscriptionIDs(dst, ids)
}

// WorkerProperties supplies the broker-made request fields of a queue offer.
type WorkerProperties struct {
	Attempt, ResponseTopic string
	Number                 int64
}

// ChannelProperties writes broker-made delivery values directly. The result
// and its section share one allocation; neither needs property validation.
// An empty id means the publisher's property already supplies it.
func ChannelProperties(ids []uint32, id string, offset, timestamp, expires int64, channel string, worker *WorkerProperties) ([]byte, Properties) {
	size := 4 + subscriptionPropertySize(ids) + len(id) + len(channel) + 256
	if worker != nil {
		// Two length-prefixed request fields and a decimal int64 user value.
		size += 3 + len(worker.ResponseTopic) + 3 + len(worker.Attempt) + 5 + len("saguin-attempt") + 20
	}
	dst := make([]byte, 4, size)
	dst = appendSubscriptionIDs(dst, ids)
	if id != "" {
		dst = appendDeliveryString(dst, "saguin-id", id)
	}
	dst = appendDeliveryNumber(dst, "saguin-offset", offset)
	dst = appendDeliveryNumber(dst, "saguin-timestamp", timestamp)
	if expires != 0 {
		dst = appendDeliveryNumber(dst, "saguin-expires", expires)
	}
	if channel != "" {
		dst = appendDeliveryString(dst, "saguin-channel", channel)
	}
	if worker != nil {
		dst = appendProperty(dst, &Property{ID: ResponseTopic, String: worker.ResponseTopic}, propertyString)
		dst = appendProperty(dst, &Property{ID: CorrelationData, Binary: []byte(worker.Attempt)}, propertyBinary)
		dst = appendDeliveryNumber(dst, "saguin-attempt", worker.Number)
	}
	section := Properties(dst[4:])
	start := 4 - varIntSize(uint32(len(section)))
	binaryAppendVarInt(dst[start:start], uint32(len(section)))
	return dst[start:], section
}

func appendDeliveryString(dst []byte, name, value string) []byte {
	dst = append(dst, UserProperty)
	dst = AppendUint16(dst, uint16(len(name)))
	dst = append(dst, name...)
	dst = AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

func appendDeliveryNumber(dst []byte, name string, value int64) []byte {
	dst = append(dst, UserProperty)
	dst = AppendUint16(dst, uint16(len(name)))
	dst = append(dst, name...)
	at := len(dst)
	dst = append(dst, 0, 0)
	dst = strconv.AppendInt(dst, value, 10)
	dst[at], dst[at+1] = byte((len(dst)-at-2)>>8), byte(len(dst)-at-2)
	return dst
}

func subscriptionPropertySize(ids []uint32) int {
	n := 0
	for _, id := range ids {
		n += 1 + varIntSize(id)
	}
	return n
}

func appendSubscriptionIDs(dst []byte, ids []uint32) []byte {
	for _, id := range ids {
		dst = append(dst, SubscriptionIdentifier)
		dst = binaryAppendVarInt(dst, id)
	}
	return dst
}

func appendPublishHead(dst []byte, version Version, p *Publish, bodySize, propertySize int) []byte {
	flags := p.QoS << publishQoSShift
	if p.Dup {
		flags |= publishDup
	}
	if p.Retain {
		flags |= publishRetain
	}
	dst = append(dst, byte(PUBLISH)<<4|flags)
	dst = binaryAppendVarInt(dst, uint32(bodySize))
	dst = AppendUint16(dst, uint16(len(p.Topic)))
	dst = append(dst, p.Topic...)
	if p.QoS != 0 {
		dst = AppendUint16(dst, p.PacketID)
	}
	if version == V5 {
		dst = binaryAppendVarInt(dst, uint32(propertySize))
		start := len(dst)
		dst = append(dst, p.Properties...)
		if p.ExpiryOffset != 0 {
			binary.BigEndian.PutUint32(dst[start+p.ExpiryOffset-1:], p.ExpiryInterval)
		}
		if p.TopicAlias != 0 {
			dst = append(dst, TopicAlias)
			dst = AppendUint16(dst, p.TopicAlias)
		}
		dst = appendSubscriptionIDs(dst, p.SubscriptionIDs)
		dst = append(dst, p.DeliveryProperties...)
	}
	return dst
}
