package packet

// Disconnect is a DISCONNECT's variable header. Only MQTT 5 carries a
// ReasonCode and Properties; MQTT 3.1.1 reads as Normal disconnection with
// no properties.
type Disconnect struct {
	ReasonCode byte
	Properties Properties
}

// DecodeDisconnect decodes a complete DISCONNECT body, without its fixed
// header. Properties share src. MQTT 5's omitted reason and Property Length
// default to Normal disconnection and zero. Rules needing connection state
// or the sender's role, including changes to session expiry, are the caller's.
func DecodeDisconnect(src []byte, version Version) (Disconnect, error) {
	if (version != V311 && version != V5) || (version == V311 && len(src) != 0) {
		return Disconnect{}, ErrMalformed
	}
	if len(src) == 0 {
		return Disconnect{}, nil
	}
	d := Disconnect{ReasonCode: src[0]}
	if ReasonName(DISCONNECT, d.ReasonCode) == "" {
		return Disconnect{}, ErrProtocol
	}
	if len(src) > 1 {
		properties, rest, err := decodeProperties(src[1:], atDISCONNECT, nil)
		if err != nil {
			return Disconnect{}, err
		}
		if len(rest) != 0 {
			return Disconnect{}, ErrMalformed
		}
		d.Properties = properties
	}
	return d, nil
}

// AppendDisconnect appends a complete DISCONNECT packet, including its
// fixed header, in the shortest form allowed by version. It returns
// ErrCannotEncode without changing dst for an invalid reason code. Properties
// are written as given, checked when made. The caller supplies a supported
// version and checks rules that depend on connection state or sender role.
func AppendDisconnect(dst []byte, version Version, d *Disconnect) ([]byte, error) {
	bodySize := 0
	if version == V5 {
		if ReasonName(DISCONNECT, d.ReasonCode) == "" {
			return dst, ErrCannotEncode
		}
		if len(d.Properties) != 0 {
			bodySize = propertySectionSize(1, d.Properties)
		} else if d.ReasonCode != reasonNormalDisconnection {
			bodySize = 1
		}
	}
	// Validation and sizing are complete; grow once and write directly into
	// the caller's buffer without a scratch packet or repeated validation.
	dst = appendFixedHeader(dst, byte(DISCONNECT)<<4, bodySize)
	if bodySize != 0 {
		dst = append(dst, d.ReasonCode)
		if len(d.Properties) != 0 {
			dst = appendCheckedProperties(dst, d.Properties)
		}
	}
	return dst, nil
}
