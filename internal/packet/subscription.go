package packet

// decodeSubscriptionHeader reads the variable header common to SUBSCRIBE,
// UNSUBSCRIBE, SUBACK and UNSUBACK. It returns the remaining payload without
// copying. Only SUBSCRIBE has a property value rule beyond wire types.
func decodeSubscriptionHeader(src []byte, typ Type, version Version) (id uint16, properties Properties, rest []byte, err error) {
	if (version != V311 && version != V5) || len(src) > maxVarInt {
		return 0, nil, nil, ErrMalformed
	}
	id, rest, err = DecodeUint16(src)
	if err != nil {
		return 0, nil, nil, err
	}
	if id == 0 {
		return 0, nil, nil, ErrProtocol
	}
	if version == V5 {
		var check func(PropertyView) error
		if typ == SUBSCRIBE {
			check = propertyValueRule(validSubscribeProperty)
		}
		properties, rest, err = decodeProperties(rest, locations(1)<<typ, check)
		if err != nil {
			return 0, nil, nil, err
		}
	}
	return id, properties, rest, nil
}

// subscriptionPacketSize sizes a checked body without traversing its bytes.
// The four subscribing packets share a Packet Identifier and, in MQTT 5,
// a mandatory property section. MQTT 3.1.1 carries no properties.
func subscriptionPacketSize(version Version, properties Properties, payloadSize int) (int, error) {
	bodySize := packetIDSize
	switch version {
	case V311:
		if len(properties) != 0 {
			return 0, ErrCannotEncode
		}
	case V5:
		bodySize = propertySectionSize(bodySize, properties)
	}
	return bodySize + payloadSize, nil
}

// appendSubscriptionHeader grows dst once for a checked packet and writes
// its fixed and variable headers. Its caller appends the payload into the
// reserved space, without further allocation.
func appendSubscriptionHeader(dst []byte, typ Type, version Version, id uint16, properties Properties, bodySize int) []byte {
	dst = appendFixedHeader(dst, byte(typ)<<4|fixedFlags[typ], bodySize)
	dst = AppendUint16(dst, id)
	if version == V5 {
		dst = appendCheckedProperties(dst, properties)
	}
	return dst
}
