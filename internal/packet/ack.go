package packet

// ackReasonSize is the one-byte MQTT 5 Reason Code after the Packet Identifier.
const ackReasonSize = 1

// Ack is the variable header shared by PUBACK, PUBREC, PUBREL and PUBCOMP.
// Only MQTT 5 carries ReasonCode and Properties; MQTT 3.1.1 reads as Success
// with no properties.
type Ack struct {
	PacketID   uint16
	ReasonCode byte
	Properties Properties
}

// DecodeAck decodes a complete PUBACK, PUBREC, PUBREL or PUBCOMP body,
// without its fixed header. Properties share src. MQTT 5's omitted reason
// and Property Length default to Success and zero. The caller checks the
// acknowledgement against the exchange's state and the sender's role;
// ReadHeader checks the fixed-header flags, including PUBREL's reserved bits.
func DecodeAck(src []byte, t Type, version Version) (Ack, error) {
	if t < PUBACK || t > PUBCOMP || (version != V311 && version != V5) ||
		(version == V311 && len(src) != packetIDSize) {
		return Ack{}, ErrMalformed
	}
	id, rest, err := DecodeUint16(src)
	if err != nil {
		return Ack{}, err
	}
	if id == 0 {
		return Ack{}, ErrProtocol
	}
	a := Ack{PacketID: id}
	if len(rest) != 0 {
		a.ReasonCode = rest[0]
		if ReasonName(t, a.ReasonCode) == "" {
			return Ack{}, ErrProtocol
		}
		if len(rest) > ackReasonSize {
			a.Properties, rest, err = decodeProperties(rest[ackReasonSize:], locations(1)<<t, nil)
			if err != nil {
				return Ack{}, err
			}
			if len(rest) != 0 {
				return Ack{}, ErrMalformed
			}
		}
	}
	return a, nil
}

// AppendAck appends a complete PUBACK, PUBREC, PUBREL or PUBCOMP packet,
// including its fixed header, in the shortest form allowed by version.
// It returns ErrCannotEncode without changing dst if a cannot be carried
// by that type or version. Properties are written as given, checked when
// made. The caller supplies a supported version and checks rules needing
// exchange state or sender role.
func AppendAck(dst []byte, t Type, version Version, a *Ack) ([]byte, error) {
	if a == nil || t < PUBACK || t > PUBCOMP || a.PacketID == 0 {
		return dst, ErrCannotEncode
	}
	bodySize := packetIDSize
	switch version {
	case V311:
		if a.ReasonCode != reasonSuccess || len(a.Properties) != 0 {
			return dst, ErrCannotEncode
		}
	case V5:
		if ReasonName(t, a.ReasonCode) == "" {
			return dst, ErrCannotEncode
		}
		if len(a.Properties) != 0 {
			bodySize = propertySectionSize(bodySize+ackReasonSize, a.Properties)
		} else if a.ReasonCode != reasonSuccess {
			bodySize += ackReasonSize
		}
	}
	// All checks precede the first write. Reuse the shared reserved-flags
	// table so PUBREL carries 0010 and the other acknowledgements carry 0000.
	dst = appendFixedHeader(dst, byte(t)<<4|fixedFlags[t], bodySize)
	dst = AppendUint16(dst, a.PacketID)
	if bodySize > packetIDSize {
		dst = append(dst, a.ReasonCode)
		if len(a.Properties) != 0 {
			dst = appendCheckedProperties(dst, a.Properties)
		}
	}
	return dst, nil
}
