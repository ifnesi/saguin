package packet

// Suback is a SUBACK's variable header and payload. ReasonCodes holds
// MQTT 5 reason codes in both versions; decoding MQTT 3.1.1 keeps its four
// wire return codes, whose values have the same meanings in MQTT 5. Only
// MQTT 5 carries Properties. Properties and ReasonCodes share src after
// decoding and must not be changed while the packet is in use.
type Suback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []byte
}

// DecodeSuback decodes a complete SUBACK body without its fixed header.
// Properties and ReasonCodes are checked in place. The caller checks the
// identifier, code count and granted QoS against its outstanding SUBSCRIBE;
// ReadHeader checks the reserved flags.
func DecodeSuback(src []byte, version Version) (Suback, error) {
	id, properties, codes, err := decodeSubscriptionHeader(src, SUBACK, version)
	if err != nil {
		return Suback{}, err
	}
	if len(codes) == 0 {
		return Suback{}, ErrProtocol
	}
	for _, code := range codes {
		if ReasonName(SUBACK, code) == "" || (version == V311 && code > reasonUnspecifiedError) {
			return Suback{}, ErrProtocol
		}
	}
	return Suback{PacketID: id, Properties: properties, ReasonCodes: codes}, nil
}

// AppendSuback appends a complete SUBACK packet, including its fixed header.
// It checks each MQTT 5 reason code through ReasonName; MQTT 3.1.1 writes
// every failure as 0x80. Properties are written as given, checked when made.
// A refusal returns ErrCannotEncode without changing dst.
func AppendSuback(dst []byte, version Version, s *Suback) ([]byte, error) {
	if s == nil || s.PacketID == 0 || len(s.ReasonCodes) == 0 {
		return dst, ErrCannotEncode
	}
	bodySize, err := subscriptionPacketSize(version, s.Properties, len(s.ReasonCodes))
	if err != nil {
		return dst, err
	}
	// Keep the original slice for refusal; writes start after its bytes.
	out := appendSubscriptionHeader(dst, SUBACK, version, s.PacketID, s.Properties, bodySize)
	for _, code := range s.ReasonCodes {
		if ReasonName(SUBACK, code) == "" {
			return dst, ErrCannotEncode
		}
		if version == V311 {
			code = min(code, reasonUnspecifiedError)
		}
		out = append(out, code)
	}
	return out, nil
}

// Unsuback is an UNSUBACK's variable header and payload. ReasonCodes holds
// MQTT 5 reason codes; MQTT 3.1.1 carries no codes or properties.
type Unsuback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []byte
}

// AppendUnsuback appends a complete UNSUBACK packet, including its fixed
// header. MQTT 3.1.1 accepts no codes or only successful MQTT 5 codes, writes
// no payload, and refuses failures it cannot express. Properties are written
// as given, checked when made. A refusal returns ErrCannotEncode without
// changing dst.
func AppendUnsuback(dst []byte, version Version, u *Unsuback) ([]byte, error) {
	if u == nil || u.PacketID == 0 || (version == V5 && len(u.ReasonCodes) == 0) {
		return dst, ErrCannotEncode
	}
	payloadSize := len(u.ReasonCodes)
	if version == V311 {
		payloadSize = 0
	}
	bodySize, err := subscriptionPacketSize(version, u.Properties, payloadSize)
	if err != nil {
		return dst, err
	}
	// Check and write each code together, retaining dst for refusal.
	out := appendSubscriptionHeader(dst, UNSUBACK, version, u.PacketID, u.Properties, bodySize)
	for _, code := range u.ReasonCodes {
		if ReasonName(UNSUBACK, code) == "" || (version == V311 && code >= reasonUnspecifiedError) {
			return dst, ErrCannotEncode
		}
		if version == V5 {
			out = append(out, code)
		}
	}
	return out, nil
}
