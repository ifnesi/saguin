package packet

// Connack is a CONNACK's variable header. ReasonCode is an MQTT 5 Connect
// Reason Code in both versions; the MQTT 3.1.1 wire return code is mapped
// by AppendConnack and DecodeConnack. Only MQTT 5 carries Properties.
type Connack struct {
	SessionPresent bool
	ReasonCode     byte
	ReturnCode311  byte // nonzero when CONNECT's cause selects a different 3.1.1 mapping
	Properties     Properties
}

// connackReasons311 maps wire return codes to the MQTT 5 reason code of the
// same name. The reverse mapping is broader because Sagüin's CONNECT refusal
// table assigns several distinct reasons to one MQTT 3.1.1 return code.
var connackReasons311 = [...]byte{
	reasonSuccess, reasonUnsupportedProtocolVersion, reasonClientIdentifierNotValid,
	reasonServerUnavailable, reasonBadUserNameOrPassword, reasonNotAuthorized,
}

// DecodeConnack decodes a complete CONNACK body, without its fixed header.
// Properties share src. Connection state rules, such as Session Present with
// Clean Start, are checked by the caller.
func DecodeConnack(src []byte, version Version) (Connack, error) {
	if (version != V311 && version != V5) || len(src) < 2 || (version == V311 && len(src) != 2) {
		return Connack{}, ErrMalformed
	}
	if src[0]&0xfe != 0 {
		return Connack{}, ErrMalformed
	}
	c := Connack{SessionPresent: src[0]&1 != 0, ReasonCode: src[1]}
	if version == V311 {
		if int(c.ReasonCode) >= len(connackReasons311) {
			return Connack{}, ErrProtocol
		}
		c.ReasonCode = connackReasons311[c.ReasonCode]
	} else if ReasonName(CONNACK, c.ReasonCode) == "" {
		return Connack{}, ErrProtocol
	}
	if c.SessionPresent && c.ReasonCode != reasonSuccess {
		return Connack{}, ErrProtocol
	}
	if version == V5 {
		properties, rest, err := decodeProperties(src[2:], atCONNACK, propertyValueRule(validConnackProperty))
		if err != nil {
			return Connack{}, err
		}
		if len(rest) != 0 {
			return Connack{}, ErrMalformed
		}
		c.Properties = properties
	}
	return c, nil
}

// AppendConnack appends a complete CONNACK packet, including its fixed
// header. It returns ErrCannotEncode without changing dst if c cannot be
// carried by version, including a reason with no MQTT 3.1.1 return code.
// The caller supplies a supported version.
func AppendConnack(dst []byte, version Version, c *Connack) ([]byte, error) {
	if c == nil || (c.SessionPresent && c.ReasonCode != reasonSuccess) {
		return dst, ErrCannotEncode
	}
	reason := c.ReasonCode
	bodySize := 2
	switch version {
	case V311:
		var mapped bool
		reason, mapped = connackReturnCode(reason)
		if c.ReturnCode311 != 0 {
			reason = c.ReturnCode311
			mapped = reason < byte(len(connackReasons311))
		}
		if !mapped || len(c.Properties) != 0 {
			return dst, ErrCannotEncode
		}
	case V5:
		if ReasonName(CONNACK, reason) == "" {
			return dst, ErrCannotEncode
		}
		err := checkProperties(c.Properties, atCONNACK, propertyValueRule(validConnackProperty))
		if err != nil {
			return dst, ErrCannotEncode
		}
		bodySize = propertySectionSize(bodySize, c.Properties)
	}
	// Every field has been checked; write directly into the caller's buffer.
	dst = appendFixedHeader(dst, byte(CONNACK)<<4, bodySize)
	var flags byte
	if c.SessionPresent {
		flags = 1
	}
	dst = append(dst, flags, reason)
	if version == V5 {
		dst = appendCheckedProperties(dst, c.Properties)
	}
	return dst, nil
}

// connackReturnCode follows the design's Flows, CONNECT refusal table, plus
// Success and Server unavailable paired by name in the two MQTT standards.
func connackReturnCode(reason byte) (byte, bool) {
	switch reason {
	case reasonSuccess:
		return 0, true
	case reasonUnsupportedProtocolVersion:
		return 1, true
	case reasonClientIdentifierNotValid:
		return 2, true
	case reasonServerUnavailable, reasonImplementationSpecificError, reasonQuotaExceeded:
		return 3, true
	case reasonBadUserNameOrPassword:
		return 4, true
	case reasonNotAuthorized, reasonTopicNameInvalid, reasonRetainNotSupported, reasonQoSNotSupported:
		return 5, true
	default:
		return 0, false
	}
}

// validConnackProperty applies the value ranges in MQTT 5 section 3.2.2.3.
func validConnackProperty(id byte, value uint32) bool {
	switch id {
	case ReceiveMaximum, MaximumPacketSize:
		return value != 0
	case MaximumQoS, RetainAvailable, WildcardSubscriptionAvailable,
		SubscriptionIdentifierAvailable, SharedSubscriptionAvailable:
		return value <= 1
	default:
		return true
	}
}
