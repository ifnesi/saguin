package packet

// Both ping packets consist only of their fixed header, with reserved flags
// and Remaining Length set to zero (MQTT 3.1.1 and 5, Figures 3.33 and 3.34).
const (
	pingreqHeader       byte = byte(PINGREQ) << 4
	pingrespHeader      byte = byte(PINGRESP) << 4
	pingRemainingLength byte = 0
)

// AppendPingreq appends a complete PINGREQ packet, including its fixed
// header, identical in MQTT 3.1.1 and 5.
func AppendPingreq(dst []byte, _ Version) ([]byte, error) {
	return append(dst, pingreqHeader, pingRemainingLength), nil
}

// DecodePingresp decodes a complete PINGRESP body, without its fixed header,
// for MQTT 3.1.1 or 5. Any body byte or unsupported version is ErrMalformed.
func DecodePingresp(src []byte, version Version) error {
	if (version != V311 && version != V5) || len(src) != 0 {
		return ErrMalformed
	}
	return nil
}

// AppendPingresp appends a complete PINGRESP packet, including its fixed
// header, identical in MQTT 3.1.1 and 5.
func AppendPingresp(dst []byte, _ Version) ([]byte, error) {
	return append(dst, pingrespHeader, pingRemainingLength), nil
}
