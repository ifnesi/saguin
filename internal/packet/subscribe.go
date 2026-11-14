package packet

import (
	"bytes"
	"iter"
)

// Subscription Options from MQTT 5 Figure 3-20. MQTT 3.1.1 uses only the
// Maximum QoS bits; its other six bits are reserved.
const (
	subscriptionQoS         = 0x03
	subscriptionNoLocal     = 0x04
	subscriptionRetainAsPub = 0x08
	subscriptionRetain      = 0x30
	subscriptionRetainShift = 4
	subscriptionReserved    = 0xc0
)

// Subscription is a SUBSCRIBE payload's Topic Filter and Subscription
// Options. Filter shares the payload's bytes when read through All.
type Subscription struct {
	Filter            []byte
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// Subscriptions is a checked SUBSCRIBE payload in wire order. DecodeSubscribe
// checks it in place; NewSubscriptions checks and copies a supplied list.
// Callers must not construct unchecked payloads or change their bytes, or
// change the source buffer while a decoded payload is in use. Filter grammar
// and rules depending on connection state are the caller's checks.
type Subscriptions []byte

// All reads each subscription in wire order without copying or checking it
// again. Each Filter shares the payload's bytes.
func (s Subscriptions) All() iter.Seq[Subscription] {
	return func(yield func(Subscription) bool) {
		for body := []byte(s); len(body) != 0; {
			filter, rest := propertyField(body)
			options := rest[0]
			body = rest[1:]
			subscription := Subscription{
				Filter:            filter,
				QoS:               options & subscriptionQoS,
				NoLocal:           options&subscriptionNoLocal != 0,
				RetainAsPublished: options&subscriptionRetainAsPub != 0,
				RetainHandling:    (options & subscriptionRetain) >> subscriptionRetainShift,
			}
			if !yield(subscription) {
				return
			}
		}
	}
}

// NewSubscriptions makes a SUBSCRIBE payload for version, copying each
// filter and preserving list order. It checks UTF-8 strings and options,
// leaving filter grammar to the caller. An empty list, invalid field or
// unsupported version returns ErrCannotEncode and no payload.
func NewSubscriptions(version Version, list []Subscription) (Subscriptions, error) {
	if (version != V311 && version != V5) || len(list) == 0 {
		return nil, ErrCannotEncode
	}
	bodySize := 0
	for i := range list {
		s := &list[i]
		if !validStringBytes(s.Filter) || s.QoS > maxQoS || s.RetainHandling > 2 ||
			(version == V311 && (s.NoLocal || s.RetainAsPublished || s.RetainHandling != 0)) ||
			len(s.Filter) > maxVarInt-bodySize-3 {
			return nil, ErrCannotEncode
		}
		bodySize += 2 + len(s.Filter) + 1
	}
	body := make(Subscriptions, 0, bodySize)
	for i := range list {
		s := &list[i]
		body = AppendUint16(body, uint16(len(s.Filter)))
		body = append(body, s.Filter...)
		options := s.QoS | s.RetainHandling<<subscriptionRetainShift
		if s.NoLocal {
			options |= subscriptionNoLocal
		}
		if s.RetainAsPublished {
			options |= subscriptionRetainAsPub
		}
		body = append(body, options)
	}
	return body, nil
}

// Subscribe is a SUBSCRIBE's variable header and payload. Only MQTT 5
// carries Properties. Properties and Subscriptions share src after decoding.
type Subscribe struct {
	PacketID      uint16
	Properties    Properties
	Subscriptions Subscriptions
	HasShared     bool // decoded payload contains a $share/ filter
}

// DecodeSubscribe decodes a complete SUBSCRIBE body without its fixed
// header, checking strings, options and property values in place. ReadHeader
// checks the reserved flags. Filter grammar, including shared subscriptions
// with No Local, and rules needing connection state are the caller's checks.
func DecodeSubscribe(src []byte, version Version) (Subscribe, error) {
	id, properties, payload, err := decodeSubscriptionHeader(src, SUBSCRIBE, version)
	if err != nil {
		return Subscribe{}, err
	}
	if len(payload) == 0 {
		return Subscribe{}, ErrProtocol
	}
	shared := false
	for body := payload; len(body) != 0; {
		filter, rest, err := decodeStringBytes(body)
		if err != nil || len(rest) == 0 {
			return Subscribe{}, ErrMalformed
		}
		options := rest[0]
		if (version == V311 && options > maxQoS) || options&subscriptionReserved != 0 {
			return Subscribe{}, ErrMalformed
		}
		if options&subscriptionQoS > maxQoS || options&subscriptionRetain == subscriptionRetain {
			return Subscribe{}, ErrProtocol
		}
		shared = shared || bytes.HasPrefix(filter, []byte("$share/"))
		body = rest[1:]
	}
	return Subscribe{PacketID: id, Properties: properties, Subscriptions: payload, HasShared: shared}, nil
}

// validSubscribeProperty applies MQTT 5 section 3.8.2.1.2 in the property
// decoder's existing pass and when NewProperties builds a section.
func validSubscribeProperty(id byte, value uint32) bool {
	return id != SubscriptionIdentifier || value != 0
}

// AppendSubscribe appends a complete SUBSCRIBE packet, including its fixed
// header. Properties and Subscriptions are written as given, checked when
// made for version. A refusal returns ErrCannotEncode without changing dst.
func AppendSubscribe(dst []byte, version Version, s *Subscribe) ([]byte, error) {
	if s == nil || s.PacketID == 0 || len(s.Subscriptions) == 0 {
		return dst, ErrCannotEncode
	}
	bodySize, err := subscriptionPacketSize(version, s.Properties, len(s.Subscriptions))
	if err != nil {
		return dst, err
	}
	dst = appendSubscriptionHeader(dst, SUBSCRIBE, version, s.PacketID, s.Properties, bodySize)
	return append(dst, s.Subscriptions...), nil
}

// SubscribeSize gives the complete encoded size before a payload is built,
// so a bridge can partition filters without encoding trial packets. It uses
// the writer's body sizing and Remaining Length representation.
func SubscribeSize(version Version, properties Properties, payloadSize int) (int, error) {
	if (version != V311 && version != V5) || payloadSize < 0 || payloadSize > maxVarInt {
		return 0, ErrCannotEncode
	}
	bodySize, err := subscriptionPacketSize(version, properties, payloadSize)
	if err != nil || bodySize > maxVarInt {
		return 0, ErrCannotEncode
	}
	return 1 + varIntSize(uint32(bodySize)) + bodySize, nil
}
