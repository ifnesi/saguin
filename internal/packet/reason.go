package packet

// MQTT 5 reason codes from section 2.4 and the packet-specific tables.
// Code zero has three names, according to the packet carrying it.
const (
	reasonSuccess                             byte = 0x00
	reasonNormalDisconnection                      = reasonSuccess
	reasonGrantedQoS1                         byte = 0x01
	reasonGrantedQoS2                         byte = 0x02
	reasonDisconnectWithWill                  byte = 0x04
	reasonNoMatchingSubscribers               byte = 0x10
	reasonNoSubscriptionExisted               byte = 0x11
	reasonContinueAuthentication              byte = 0x18
	reasonReAuthenticate                      byte = 0x19
	reasonUnspecifiedError                    byte = 0x80
	reasonMalformedPacket                     byte = 0x81
	reasonProtocolError                       byte = 0x82
	reasonImplementationSpecificError         byte = 0x83
	reasonUnsupportedProtocolVersion          byte = 0x84
	reasonClientIdentifierNotValid            byte = 0x85
	reasonBadUserNameOrPassword               byte = 0x86
	reasonNotAuthorized                       byte = 0x87
	reasonServerUnavailable                   byte = 0x88
	reasonServerBusy                          byte = 0x89
	reasonBanned                              byte = 0x8a
	reasonServerShuttingDown                  byte = 0x8b
	reasonBadAuthenticationMethod             byte = 0x8c
	reasonKeepAliveTimeout                    byte = 0x8d
	reasonSessionTakenOver                    byte = 0x8e
	reasonTopicFilterInvalid                  byte = 0x8f
	reasonTopicNameInvalid                    byte = 0x90
	reasonPacketIdentifierInUse               byte = 0x91
	reasonPacketIdentifierNotFound            byte = 0x92
	reasonReceiveMaximumExceeded              byte = 0x93
	reasonTopicAliasInvalid                   byte = 0x94
	reasonPacketTooLarge                      byte = 0x95
	reasonMessageRateTooHigh                  byte = 0x96
	reasonQuotaExceeded                       byte = 0x97
	reasonAdministrativeAction                byte = 0x98
	reasonPayloadFormatInvalid                byte = 0x99
	reasonRetainNotSupported                  byte = 0x9a
	reasonQoSNotSupported                     byte = 0x9b
	reasonUseAnotherServer                    byte = 0x9c
	reasonServerMoved                         byte = 0x9d
	reasonSharedSubscriptionsNotSupported     byte = 0x9e
	reasonConnectionRateExceeded              byte = 0x9f
	reasonMaximumConnectTime                  byte = 0xa0
	reasonSubscriptionIdentifiersNotSupported byte = 0xa1
	reasonWildcardSubscriptionsNotSupported   byte = 0xa2
)

type reasonDefinition struct {
	name    string
	allowed locations
}

// reasonDefinitions holds each MQTT 5 name and its allowed packets, indexed
// by code. Only zero needs several entries. Placement follows each packet's
// own table (3-1 and 3-4 through 3-11): DISCONNECT cannot carry 0x8C, despite
// Table 2-6, as design.md's Choices MQTT leaves open requires. Names follow
// Table 2-6, correcting Table 3-4's lower-case "identifier" at 0x91 and
// Table 3-5's trailing full stop at 0x10.
var reasonDefinitions = [...][]reasonDefinition{
	reasonSuccess: {
		{"Success", atCONNACK | atPUBACK | atPUBREC | atPUBREL | atPUBCOMP | atUNSUBACK | atAUTH},
		{"Normal disconnection", atDISCONNECT},
		{"Granted QoS 0", atSUBACK},
	},
	reasonGrantedQoS1:                         {{"Granted QoS 1", atSUBACK}},
	reasonGrantedQoS2:                         {{"Granted QoS 2", atSUBACK}},
	reasonDisconnectWithWill:                  {{"Disconnect with Will Message", atDISCONNECT}},
	reasonNoMatchingSubscribers:               {{"No matching subscribers", atPUBACK | atPUBREC}},
	reasonNoSubscriptionExisted:               {{"No subscription existed", atUNSUBACK}},
	reasonContinueAuthentication:              {{"Continue authentication", atAUTH}},
	reasonReAuthenticate:                      {{"Re-authenticate", atAUTH}},
	reasonUnspecifiedError:                    {{"Unspecified error", atCONNACK | atPUBACK | atPUBREC | atSUBACK | atUNSUBACK | atDISCONNECT}},
	reasonMalformedPacket:                     {{"Malformed Packet", atCONNACK | atDISCONNECT}},
	reasonProtocolError:                       {{"Protocol Error", atCONNACK | atDISCONNECT}},
	reasonImplementationSpecificError:         {{"Implementation specific error", atCONNACK | atPUBACK | atPUBREC | atSUBACK | atUNSUBACK | atDISCONNECT}},
	reasonUnsupportedProtocolVersion:          {{"Unsupported Protocol Version", atCONNACK}},
	reasonClientIdentifierNotValid:            {{"Client Identifier not valid", atCONNACK}},
	reasonBadUserNameOrPassword:               {{"Bad User Name or Password", atCONNACK}},
	reasonNotAuthorized:                       {{"Not authorized", atCONNACK | atPUBACK | atPUBREC | atSUBACK | atUNSUBACK | atDISCONNECT}},
	reasonServerUnavailable:                   {{"Server unavailable", atCONNACK}},
	reasonServerBusy:                          {{"Server busy", atCONNACK | atDISCONNECT}},
	reasonBanned:                              {{"Banned", atCONNACK}},
	reasonServerShuttingDown:                  {{"Server shutting down", atDISCONNECT}},
	reasonBadAuthenticationMethod:             {{"Bad authentication method", atCONNACK}},
	reasonKeepAliveTimeout:                    {{"Keep Alive timeout", atDISCONNECT}},
	reasonSessionTakenOver:                    {{"Session taken over", atDISCONNECT}},
	reasonTopicFilterInvalid:                  {{"Topic Filter invalid", atSUBACK | atUNSUBACK | atDISCONNECT}},
	reasonTopicNameInvalid:                    {{"Topic Name invalid", atCONNACK | atPUBACK | atPUBREC | atDISCONNECT}},
	reasonPacketIdentifierInUse:               {{"Packet Identifier in use", atPUBACK | atPUBREC | atSUBACK | atUNSUBACK}},
	reasonPacketIdentifierNotFound:            {{"Packet Identifier not found", atPUBREL | atPUBCOMP}},
	reasonReceiveMaximumExceeded:              {{"Receive Maximum exceeded", atDISCONNECT}},
	reasonTopicAliasInvalid:                   {{"Topic Alias invalid", atDISCONNECT}},
	reasonPacketTooLarge:                      {{"Packet too large", atCONNACK | atDISCONNECT}},
	reasonMessageRateTooHigh:                  {{"Message rate too high", atDISCONNECT}},
	reasonQuotaExceeded:                       {{"Quota exceeded", atCONNACK | atPUBACK | atPUBREC | atSUBACK | atDISCONNECT}},
	reasonAdministrativeAction:                {{"Administrative action", atDISCONNECT}},
	reasonPayloadFormatInvalid:                {{"Payload format invalid", atCONNACK | atPUBACK | atPUBREC | atDISCONNECT}},
	reasonRetainNotSupported:                  {{"Retain not supported", atCONNACK | atDISCONNECT}},
	reasonQoSNotSupported:                     {{"QoS not supported", atCONNACK | atDISCONNECT}},
	reasonUseAnotherServer:                    {{"Use another server", atCONNACK | atDISCONNECT}},
	reasonServerMoved:                         {{"Server moved", atCONNACK | atDISCONNECT}},
	reasonSharedSubscriptionsNotSupported:     {{"Shared Subscriptions not supported", atSUBACK | atDISCONNECT}},
	reasonConnectionRateExceeded:              {{"Connection rate exceeded", atCONNACK | atDISCONNECT}},
	reasonMaximumConnectTime:                  {{"Maximum connect time", atDISCONNECT}},
	reasonSubscriptionIdentifiersNotSupported: {{"Subscription Identifiers not supported", atSUBACK | atDISCONNECT}},
	reasonWildcardSubscriptionsNotSupported:   {{"Wildcard Subscriptions not supported", atSUBACK | atDISCONNECT}},
}

// ReasonName returns code's MQTT 5 name on a packet of type t, or an empty
// string when that packet cannot carry it. The codecs use the same lookup
// for validation. The lookup allocates nothing.
func ReasonName(t Type, code byte) string {
	if t > AUTH || int(code) >= len(reasonDefinitions) {
		return ""
	}
	place := locations(1) << t
	for _, definition := range reasonDefinitions[code] {
		if definition.allowed&place != 0 {
			return definition.name
		}
	}
	return ""
}
