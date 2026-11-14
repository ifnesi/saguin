package conformance

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The test of the schema convention (item 0021a), end to end from the wire:
// design.md, Schemas. Requests are made as requests_test.go makes them
// (kvGot), and the acl_file is read as acl_test.go writes it (aclOptions).

// schemaACL is the Schemas section's acl_file, its roles producer and
// reader as the design writes them, with station, writing weather/%u/#,
// and display, reading weather, for the records that name a schema:
// acme and globex are producers, app a consumer.
const schemaACL = `roles:
  producer:
    - channel: schemas
      filter: schemas/%u/#
      allow: [write, read]
  reader:
    - channel: schemas
      allow: [read]
  station:
    - channel: weather
      filter: weather/%u/#
      allow: [write]
  display:
    - channel: weather
      allow: [read]
users:
  acme: [producer, station]
  globex: [producer, station]
  app: [reader, display]
`

// design.md, Schemas: the registry "is a latest channel and a convention";
// its channel, "type: latest, filter: schemas/#, retention_period: none";
// its acl_file, "producer [...] filter: schemas/%u/# [...] reader [...]
// allow: [read]". Each step cites its sentence:
//
//   - Register, "a producer publishes a schema's text to its own topic in
//     the registry. Each version has its own topic (schemas/acme/weather/v2)":
//     acme publishes the schema's text to schemas/acme/weather/v2, PUBACK
//     0x00.
//   - Produce, "a record carries Content Type, naming its serialization
//     format, and a User Property named schema holding the whole topic of
//     its schema"; "A publish carrying a property named saguin-schema is
//     refused with 0x83": acme publishes on weather/acme/lisbon a record
//     carrying saguin-schema, PUBACK 0x83, then one carrying Content Type
//     and schema, PUBACK 0x00.
//   - Consume, "a consumer reads the schema property, reads that topic with
//     $saguin/kv/get"; "The schema property is an ordinary User Property it
//     never reads"; Sagüin properties, "The publisher's own User Properties
//     go unaltered (MQTT-3.3.2-17)"; MQTT 5 3.3.2.3.9, "A Server MUST send
//     the Content Type unaltered to all subscribers" [MQTT-3.3.2-20]: app,
//     subscribed to weather/#, receives the record, and no other first,
//     with its payload, Content Type and one schema property as sent; its
//     kv/get of that property's value answers the schema's text.
//   - "The acl_file confines each producer to its own prefix, so two
//     producers can never write the same schema topic"; PUBLISH, "0x87 |
//     the acl_file refuses the topic": globex's publish to
//     schemas/acme/weather/v2 gets PUBACK 0x87, its own
//     schemas/globex/weather/v1 0x00.
//   - Retire, "A version is retired only once no stored record still names
//     it": app waits until retention, weather's retention_period 1s, has
//     deleted the record (metrics.md: saguin_channel_records 0,
//     saguin_channel_retention_removed_total 1); "Nothing retires a schema
//     by itself: the registry's retention_period: none keeps every version
//     until it is deleted": app's kv/get then still answers acme's text,
//     globex's refused write having changed nothing. "a producer [...]
//     publishes a zero-length payload to the version's topic, which deletes
//     it"; $saguin topics, kv/get, "An empty answer means no value": acme's
//     empty publish gets PUBACK 0x00, and app's kv/get answers empty.
func TestSchemaRegisterProduceReadRetire(t *testing.T) {
	const (
		version     = "schemas/acme/weather/v2"
		format      = "application/avro"
		recordTopic = "weather/acme/lisbon"
	)
	text := []byte(`{"type":"record","name":"Reading","fields":[{"name":"celsius","type":"float"}]}`)
	payload := []byte{0x02, 0x00, 0x00, 0xA0, 0x41} // opaque bytes to the broker

	o := aclOptions(t, schemaACL, "acme", "globex", "app")
	o.Operations = true
	o.Channels = []harness.Channel{
		{Name: "schemas", Type: "latest", Filter: "schemas/#", NoRetentionPeriod: true},
		{Name: "weather", Type: "append", Filter: "weather/#", RetentionPeriod: time.Second},
	}
	b := support.Launch(t, o)
	acme := loggedIn(t, b, "acme")
	app := support.Subscribed(t, b.Addr, harness.V5, aclLogin("app", "app"), support.At(1, "weather/#"))

	// Register.
	support.PublishAll(t, acme, []*harness.Publish{{QoS: 1, Topic: version, Payload: text}})

	// Produce: a saguin-schema property is refused, then the record goes in.
	sent := harness.Properties{{ID: harness.ContentType, Str: format}, harness.User("schema", version)}
	refused := &harness.Publish{QoS: 1, Topic: recordTopic, Payload: payload,
		Props: harness.Properties{{ID: harness.ContentType, Str: format}, harness.User("saguin-schema", version)}}
	record := &harness.Publish{QoS: 1, Topic: recordTopic, Payload: payload, Props: slices.Clone(sent)}
	if got := pubackReasons(t, acme, []*harness.Publish{refused, record}); !bytes.Equal(got, []byte{0x83, 0x00}) {
		t.Fatalf("acme's records carrying saguin-schema, then Content Type and schema: PUBACKs % X; want 83 00", got)
	}

	// Consume: the record arrives as sent, and its schema property is read.
	got := support.DeliveredAt(t, app, 1, recordTopic, payload)
	if err := app.Acknowledge(got); err != nil {
		t.Fatal(err)
	}
	ct, _ := got.Props.Get(harness.ContentType)
	schema := got.Props.User("schema")
	if ct.Str != format || len(schema) != 1 || schema[0] != version {
		t.Fatalf("the record arrived with Content Type %q and schema %q; want %q and [%q], as sent", ct.Str, schema, format, version)
	}
	if answer := kvGot(t, app, schema[0]); !bytes.Equal(answer, text) {
		t.Fatalf("app's kv/get of %s answered %q; want the registered text %q", schema[0], answer, text)
	}

	// A second producer is refused on the first's prefix, not on its own.
	globex := loggedIn(t, b, "globex")
	ps := []*harness.Publish{{QoS: 1, Topic: version, Payload: []byte("globex's text")},
		{QoS: 1, Topic: "schemas/globex/weather/v1", Payload: []byte("globex's text")}}
	if got := pubackReasons(t, globex, ps); !bytes.Equal(got, []byte{0x87, 0x00}) {
		t.Fatalf("globex's publishes to %s and to schemas/globex/weather/v1: PUBACKs % X; want 87 00", version, got)
	}

	// Retire, once no stored record names the version.
	support.AwaitSampled(t, b, "retention deleting weather's one record", map[string]float64{
		recordsSeries("weather"): 0,
		support.ChannelSeries("saguin_channel_retention_removed_total", "weather"): 1,
	})
	if answer := kvGot(t, app, version); !bytes.Equal(answer, text) {
		t.Fatalf("app's kv/get of %s, its records gone and globex refused, answered %q; want acme's text %q", version, answer, text)
	}
	support.PublishAll(t, acme, []*harness.Publish{value(version, "")})
	if answer := kvGot(t, app, version); len(answer) != 0 {
		t.Fatalf("app's kv/get of %s after acme retired it answered %q; want empty, no value", version, answer)
	}
}
