package packet

import (
	"math"

	"github.com/ifnesi/saguin/internal/topic"
)

// CONNECT flags from MQTT 5 Figure 3-4 and MQTT 3.1.1 Figure 3.4. Will QoS
// is the two-bit value in bits 4 and 3.
const (
	connectReserved     = 0x01
	connectClean        = 0x02
	connectWill         = 0x04
	connectWillQoS      = 0x18
	connectWillQoSShift = 3
	connectRetain       = 0x20
	connectPassword     = 0x40
	connectUserName     = 0x80
)

// connectHeaderSize is the CONNECT variable header before its properties:
// the Protocol Name "MQTT" (two length bytes and four characters), the
// Protocol Level, the Connect Flags and the two-byte Keep Alive.
const connectHeaderSize = 6 + 1 + 1 + 2

// Connect is a CONNECT's variable header and payload. CleanStart carries
// MQTT 3.1.1's Clean Session bit as well as MQTT 5's Clean Start bit. Flags
// distinguish absent fields from fields present with an empty value.
type Connect struct {
	Version      Version
	CleanStart   bool
	KeepAlive    uint16
	Properties   Properties
	ClientID     string
	WillFlag     bool
	Will         Will
	UserNameFlag bool
	UserName     string
	PasswordFlag bool
	Password     []byte

	// Decoding records these property values, including MQTT defaults for
	// absent properties. Encoding uses Properties alone.
	SessionExpiryInterval      uint32
	ReceiveMaximum             uint16
	MaximumPacketSize          uint32
	TopicAliasMaximum          uint16
	RequestProblemInformation  bool
	RequestResponseInformation bool
	HasAuthenticationMethod    bool
	HasSaguinProperty          bool
}

// Will is the message carried by a CONNECT with its Will Flag set.
type Will struct {
	QoS              byte
	Retain           bool
	Properties       Properties
	Topic            string
	Payload          []byte
	StoredProperties []byte // decoded property section, including its length; nil for none
	StoredPayload    []byte // reader-prepared S2 copy; nil when stored as received
	PayloadSize      int    // original length when the reader replaces Payload with StoredPayload
}

// DecodeConnectVersion checks only the Protocol Name and Level at the start
// of a CONNECT body. The broker can apply its minimum protocol version before
// complete packet validity. An unknown MQTT level, or MQTT 3.1 (MQIsdp at
// level 3), returns ErrUnsupportedVersion without reading later fields.
func DecodeConnectVersion(src []byte) (Version, error) {
	// Both supported versions encode the Protocol Name as these fixed bytes.
	// Comparing them directly avoids allocating a temporary string.
	if len(src) < 7 || string(src[:6]) != "\x00\x04MQTT" {
		// The design gives MQTT 3.1 the unsupported-version answer too.
		// Its name and level suffice; its remaining fields are not parsed.
		if len(src) >= 9 && string(src[:8]) == "\x00\x06MQIsdp" && src[8] == 3 {
			return 0, ErrUnsupportedVersion
		}
		return 0, ErrMalformed
	}
	version := Version(src[6])
	if version != V311 && version != V5 {
		return 0, ErrUnsupportedVersion
	}
	return version, nil
}

// DecodeConnect decodes a complete CONNECT body, without its fixed header.
// Strings outside properties are copied; Properties and Binary Data share src.
// Client id restrictions and Will topic validity are checked by the broker
// later, so it can refuse them with a CONNACK.
func DecodeConnect(src []byte) (Connect, error) {
	version, err := DecodeConnectVersion(src)
	if err != nil {
		return Connect{}, err
	}
	return DecodeConnectBody(src[7:], version)
}

// DecodeConnectBody decodes after the Protocol Name and Level, starting at
// the Connect Flags. version must be the result of DecodeConnectVersion on
// the complete body. A reader can check its minimum version before calling
// this decoder, without reading the prefix twice.
func DecodeConnectBody(src []byte, version Version) (Connect, error) {
	if len(src) < 3 {
		return Connect{}, ErrMalformed
	}
	flags := src[0]
	qos := (flags & connectWillQoS) >> connectWillQoSShift
	if flags&connectReserved != 0 || qos == 3 {
		return Connect{}, ErrMalformed
	}
	if (flags&connectWill == 0 && flags&(connectRetain|connectWillQoS) != 0) ||
		(version == V311 && flags&connectPassword != 0 && flags&connectUserName == 0) {
		return Connect{}, ErrProtocol
	}
	keepAlive, rest, err := DecodeUint16(src[1:])
	if err != nil {
		return Connect{}, err
	}
	c := Connect{
		Version:                   version,
		CleanStart:                flags&connectClean != 0,
		KeepAlive:                 keepAlive,
		WillFlag:                  flags&connectWill != 0,
		UserNameFlag:              flags&connectUserName != 0,
		PasswordFlag:              flags&connectPassword != 0,
		ReceiveMaximum:            65535,
		RequestProblemInformation: true,
	}
	if version == V5 {
		values := connectPropertyValues{connect: &c}
		c.Properties, rest, err = decodeProperties(rest, atCONNECT, values.valid)
		if err != nil {
			return Connect{}, err
		}
		if values.data && !c.HasAuthenticationMethod {
			return Connect{}, ErrProtocol
		}
	}
	c.ClientID, rest, err = DecodeString(rest)
	if err != nil {
		return Connect{}, err
	}
	if c.WillFlag {
		c.Will.QoS, c.Will.Retain = qos, flags&connectRetain != 0
		if version == V5 {
			section := rest
			c.Will.Properties, rest, err = decodeProperties(rest, atWill, validWillProperty)
			if err != nil {
				return Connect{}, err
			}
			if len(c.Will.Properties) != 0 {
				c.Will.StoredProperties = section[:len(section)-len(rest)]
			}
		}
		c.Will.Topic, rest, err = DecodeString(rest)
		if err != nil {
			return Connect{}, err
		}
		c.Will.Payload, rest, err = DecodeBinary(rest)
		if err != nil {
			return Connect{}, err
		}
	}
	if c.UserNameFlag {
		c.UserName, rest, err = DecodeString(rest)
		if err != nil {
			return Connect{}, err
		}
	}
	if c.PasswordFlag {
		c.Password, rest, err = DecodeBinary(rest)
		if err != nil {
			return Connect{}, err
		}
	}
	if len(rest) != 0 {
		return Connect{}, ErrMalformed
	}
	return c, nil
}

// connectPropertyValues records the connection's scalars while applying MQTT
// 5 section 3.1.2.11. Authentication Method may follow Authentication Data,
// so their dependency is checked once the set ends. Presence, rather than a
// non-empty value, satisfies the rule.
type connectPropertyValues struct {
	connect *Connect
	data    bool
}

func (v *connectPropertyValues) valid(p PropertyView) error {
	c := v.connect
	switch p.ID {
	case SessionExpiryInterval:
		c.SessionExpiryInterval = p.Int
	case ReceiveMaximum:
		if p.Int == 0 {
			return ErrProtocol
		}
		c.ReceiveMaximum = uint16(p.Int)
	case MaximumPacketSize:
		if p.Int == 0 {
			return ErrProtocol
		}
		c.MaximumPacketSize = p.Int
	case TopicAliasMaximum:
		c.TopicAliasMaximum = uint16(p.Int)
	case RequestProblemInformation, RequestResponseInformation:
		if p.Int > 1 {
			return ErrProtocol
		}
		if p.ID == RequestProblemInformation {
			c.RequestProblemInformation = p.Int != 0
		} else {
			c.RequestResponseInformation = p.Int != 0
		}
	case AuthenticationMethod:
		c.HasAuthenticationMethod = true
	case AuthenticationData:
		v.data = true
	case UserProperty:
		if IsSaguinProperty(p.Name) {
			c.HasSaguinProperty = true
		}
	}
	return nil
}

// validWillProperty checks the payload format and Response Topic while
// decoding. Broker topic limits do not apply to the Response Topic.
// Will Payload format validation is optional and is left to the broker.
func validWillProperty(p PropertyView) error {
	switch p.ID {
	case PayloadFormatIndicator:
		if p.Int > 1 {
			return ErrProtocol
		}
	case ResponseTopic:
		limits := topic.Limits{MaxLength: math.MaxUint16, MaxLevels: math.MaxUint16 + 1}
		if topic.CheckName(string(p.Value), limits) != nil {
			return ErrMalformed
		}
	}
	return nil
}

// AppendConnect appends a complete CONNECT packet, including its fixed
// header. The caller supplies checked fields and property sections.
func AppendConnect(dst []byte, c *Connect) ([]byte, error) {
	dst = appendFixedHeader(dst, byte(CONNECT)<<4, connectSize(c))
	dst = append(dst, '\x00', '\x04', 'M', 'Q', 'T', 'T', byte(c.Version), connectFlags(c))
	dst = AppendUint16(dst, c.KeepAlive)
	if c.Version == V5 {
		dst = appendCheckedProperties(dst, c.Properties)
	}
	dst = appendString(dst, c.ClientID)
	if c.WillFlag {
		if c.Version == V5 {
			dst = appendCheckedProperties(dst, c.Will.Properties)
		}
		dst = appendString(dst, c.Will.Topic)
		dst = AppendUint16(dst, uint16(len(c.Will.Payload)))
		dst = append(dst, c.Will.Payload...)
	}
	if c.UserNameFlag {
		dst = appendString(dst, c.UserName)
	}
	if c.PasswordFlag {
		dst = AppendUint16(dst, uint16(len(c.Password)))
		dst = append(dst, c.Password...)
	}
	return dst, nil
}

// connectSize computes the body size without rechecking fields or properties.
func connectSize(c *Connect) int {
	bodySize := connectHeaderSize + 2 + len(c.ClientID)
	if c.WillFlag {
		bodySize += 2 + len(c.Will.Topic) + 2 + len(c.Will.Payload)
	}
	if c.UserNameFlag {
		bodySize += 2 + len(c.UserName)
	}
	if c.PasswordFlag {
		bodySize += 2 + len(c.Password)
	}
	if c.Version == V5 {
		bodySize = propertySectionSize(bodySize, c.Properties)
		if c.WillFlag {
			bodySize = propertySectionSize(bodySize, c.Will.Properties)
		}
	}
	return bodySize
}

func connectFlags(c *Connect) byte {
	var flags byte
	if c.CleanStart {
		flags |= connectClean
	}
	if c.WillFlag {
		flags |= connectWill | c.Will.QoS<<connectWillQoSShift
		if c.Will.Retain {
			flags |= connectRetain
		}
	}
	if c.UserNameFlag {
		flags |= connectUserName
	}
	if c.PasswordFlag {
		flags |= connectPassword
	}
	return flags
}
