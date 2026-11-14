package config

import "time"

// bridgeSchema exposes TLS only on peers that use it. Input and resolved
// output share the table's key order and the same value readers.
func bridgeSchema(b *Bridge) []field {
	fields := []field{
		{name: "peer", value: &b.Peer, required: true},
		{name: "client_id", value: &b.ClientID, required: true},
		{name: "username", value: &b.Username, none: true},
		{name: "password_file", value: &b.PasswordFile, none: true},
		duration("session_expiry", &b.SessionExpiry, time.Second, maxMQTTExpiry, false, false),
		count("receive_maximum", &b.ReceiveMaximum, 1, maxMQTTTwoByteInteger, false),
		duration("ack_interval", &b.AckInterval, time.Millisecond, time.Second, false, true),
	}
	if b.UsesTLS() {
		fields = append(fields, field{name: "tls", value: &b.TLS})
	}
	return append(fields, field{name: "topics", value: &b.Topics, required: true})
}

func bridgeTLSSchema(t *BridgeTLS) []field {
	return append([]field{{name: "ca_file", value: &t.CAFile, omit: t.CAFile == ""}},
		tlsSchema(&t.CertFile, &t.KeyFile, &t.MinVersion)...)
}

func bridgeRuleSchema(r *BridgeRule) []field {
	return []field{
		{name: "filter", value: &r.Filter, required: true},
		{name: "topic", value: &r.Topic, required: r.Direction != Both, omit: r.Direction == Both},
		{name: "direction", value: &r.Direction, required: true},
		{name: "qos", value: &r.QoS, minimum: 0, maximum: 2},
	}
}
