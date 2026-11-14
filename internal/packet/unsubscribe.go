package packet

import "iter"

// TopicFilters is a checked UNSUBSCRIBE payload in wire order.
// DecodeUnsubscribe checks it in place. Callers must not construct unchecked
// payloads or change their bytes, or change the source buffer while a decoded
// payload is in use. Filter grammar is the caller's check.
type TopicFilters []byte

// All reads each filter in wire order without copying or checking it again.
// The filters share the payload's bytes.
func (f TopicFilters) All() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for body := []byte(f); len(body) != 0; {
			filter, rest := propertyField(body)
			body = rest
			if !yield(filter) {
				return
			}
		}
	}
}

// Unsubscribe is an UNSUBSCRIBE's variable header and payload. Only MQTT 5
// carries Properties. Properties and Filters share src after decoding.
type Unsubscribe struct {
	PacketID   uint16
	Properties Properties
	Filters    TopicFilters
}

// DecodeUnsubscribe decodes a complete UNSUBSCRIBE body without its fixed
// header, checking properties and filter strings in place. ReadHeader checks
// the reserved flags. Filter grammar and connection state are the caller's.
func DecodeUnsubscribe(src []byte, version Version) (Unsubscribe, error) {
	id, properties, payload, err := decodeSubscriptionHeader(src, UNSUBSCRIBE, version)
	if err != nil {
		return Unsubscribe{}, err
	}
	if len(payload) == 0 {
		return Unsubscribe{}, ErrProtocol
	}
	for body := payload; len(body) != 0; {
		_, rest, err := decodeStringBytes(body)
		if err != nil {
			return Unsubscribe{}, err
		}
		body = rest
	}
	return Unsubscribe{PacketID: id, Properties: properties, Filters: payload}, nil
}
