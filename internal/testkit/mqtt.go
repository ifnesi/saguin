package testkit

import (
	"strings"
	"unicode/utf8"
)

// The MQTT string oracles, stated from the standards alone, without the
// code they judge: test oracles come from the design and the MQTT
// standards, never from the implementation.

// UTF8StringOK reports whether s may be carried as a UTF-8 Encoded String:
// at most 65,535 bytes (MQTT 5 1.5.4; MQTT 3.1.1 1.5.3), well-formed UTF-8
// with no surrogate [MQTT-1.5.4-1], and no U+0000 [MQTT-1.5.4-2]. Go's
// UTF-8 check refuses encoded surrogates.
func UTF8StringOK(s string) bool {
	return len(s) <= 65_535 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// TopicNameOK reports whether s is a well-formed Topic Name: "at least one
// character long" [MQTT-4.7.3-1], and without the wildcard characters,
// which "MUST NOT be used within a Topic Name" (MQTT 5 [MQTT-4.7.0-1]; MQTT
// 3.1.1 [MQTT-4.7.1-1]).
func TopicNameOK(s string) bool {
	return s != "" && !strings.ContainsAny(s, "+#")
}

// TopicFilterOK reports whether s is a well-formed Topic Filter: at least
// one character [MQTT-4.7.3-1]; `#` "MUST be specified either on its own or
// following a topic level separator. In either case it MUST be the last
// character specified in the Topic Filter" (MQTT 5 [MQTT-4.7.1-1]; MQTT
// 3.1.1 [MQTT-4.7.1-2]); `+` "MUST occupy an entire level of the filter"
// (MQTT 5 [MQTT-4.7.1-2]; MQTT 3.1.1 [MQTT-4.7.1-3]).
func TopicFilterOK(s string) bool {
	if s == "" {
		return false
	}
	levels := strings.Split(s, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	return true
}
