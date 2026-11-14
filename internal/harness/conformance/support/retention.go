package support

import (
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Sized is a QoS 1 publication on topic whose record is n bytes: a payload
// of dots fills what the topic leaves.
func Sized(topic string, n int) *harness.Publish {
	return &harness.Publish{QoS: 1, Topic: topic, Payload: testkit.Filling(topic, n)}
}
