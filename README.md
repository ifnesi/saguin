# Sagüin

<img src="docs/img/saguin_256.png" align="left" alt="Sagüin" width="174" style="padding:16px;">

Sagüin (sah-GWEEN) is a lightweight MQTT broker for edge and small-scale
IoT/IIoT deployments. It adds three durable messaging primitives to
ordinary MQTT topics:

* replayable event stream
* latest-value state store
* work queue

It is designed for places where the network is unreliable, operational
simplicity matters, and running a broker plus a database, streaming
platform, cache, and queue is too much infrastructure. It is also for
teams that want more control over their broker.

Sagüin speaks standard MQTT. Existing devices and MQTT clients continue to
publish and subscribe normally; channels are configured at the broker and
change what happens after a message arrives.

One rule shapes every detail below: **Sagüin never acknowledges a promise
it cannot keep.** Where it cannot honour a request, it refuses with a
reason code rather than accepting and quietly doing less.

<br clear="left"/>

## Why Sagüin exists

A truck parks in a basement for nine days. A substation's cellular modem
sulks through a storm. A vessel crosses a stretch of ocean where the
only thing the antenna can see is more ocean.

All three come back. On a standard MQTT broker they find a session queue
that overflowed on day one and a broker under no obligation to tell
anybody. The readings are gone, no metric moved, and the first person to
notice is the customer looking at a gap in a chart three weeks later.

That is not a bug in any broker. MQTT is a transport, and it does the
job it says on the tin: it moves messages between things connected at
the same time. The trouble is that edge data stopped being telemetry and
became business data, and "the link dropped, so the readings are gone"
is not a sentence you can put in a billing dispute.

The usual fix stacks a streaming platform for history, a cache for
state, and a job queue for work beside the broker: four systems, four
client libraries, four failure modes, and four sets of credentials.
Sagüin provides the same semantics in a single Go binary of under 25 MB,
with memory or SQLite storage, keeping MQTT as the interface devices and
applications already use.

## Core concepts

A channel is a named, configured region of the MQTT topic space. Its MQTT
topic filter decides which messages it holds, and its type decides how
those messages behave.

| Channel type | Purpose | Behaviour |
|---|---|---|
| `append` | Events, telemetry, audit history | Durable append-only stream with replay and an independent position per consumer |
| `latest` | Device state, configuration, caches | One current value per topic, delivered on subscribe and available through point reads |
| `queue` | Commands and background work | One worker lease at a time, with acknowledgement, redelivery, attempt limits, and dead-lettering |

A topic that no channel claims remains ordinary MQTT broadcast: it is
delivered to connected subscribers and is not replayed. Retained broadcast
messages are always supported, kept in a store bounded by its storage
provider, and can be denied per user or role in the ACL file.

## Who it is for

Sagüin is aimed at:

* Gateways, vehicles, vessels, remote sites, substations, home labs, and
  other single-node edge deployments
* Links that are denied, degraded, intermittent, or bandwidth-limited
* Existing MQTT fleets that should not need new firmware or a proprietary
  client protocol
* Teams that value a small operational footprint and inspectable storage
  over clustering and very high scale
* Applications that need several messaging semantics but cannot justify
  several infrastructure components

Sagüin is built for 32-bit and 64-bit systems, ARM or x86, with nothing but
`GOARCH` set; its test suite runs on 64-bit x86. The 32-bit limit is how
much a process can address, which bounds a memory provider rather than
anything a channel promises; storage is pure Go, so there is no
cross-compiler to find and a SQLite database written on one width reads on
the other.

Sagüin is especially useful when the hard problem is surviving outages and
retaining useful local history, not processing millions of messages per
second.

## What makes it different

### A cursor, not a copy

MQTT already has durable-sounding words, and they are not the same
thing:

| | What it is | Survives the device being away? |
|---|---|---|
| **Retained message** | the one latest value on a topic | Yes: the latest value, never the history |
| **Persistent session** (Clean Start 0) | a per-client *copy* of what it missed, in a bounded queue | Until that queue overflows |
| **Sagüin `append` channel** | one shared log and a *cursor* per consumer | Yes, in full: bounded by retention, not by the client |

Every broker gives an offline session its own bounded copy of what it
missed. On an `append` channel Sagüin stores a cursor instead - the same
idea as a Kafka consumer group's committed offset, without the cluster -
and that one change buys three things at once:

1. A week offline costs the same as a minute: one integer either way.
2. It cannot overflow, because there is nothing to fill.
3. It survives a restart, because the position is stored where the data
   is.

Retention still removes old records - and when it catches up with a
consumer, Sagüin refuses the read rather than silently serving the
oldest survivor.

### An alert no standard broker can raise

```
saguin_channel_floor_offset > saguin_channel_consumer_position_min
```

When that expression fires, retention has passed a consumer's stored
position: that consumer's data is already gone, and the broker says so
before the customer does - it can, because it keeps both numbers anyway.

### Three messaging semantics behind MQTT

Publishers continue to send ordinary MQTT messages. Configuration decides
whether a topic is broadcast, appended to a log, stored as current state,
or treated as work.

### A real work queue

MQTT shared subscriptions provide load balancing, but in MQTT `PUBACK`
means "I received the bytes", not "I did the job", and most client
libraries send it before your code runs. A Sagüin queue adds an
application acknowledgement, visibility timeout, retry policy, attempt
count, and a derived dead-letter channel.

Queue processing is at-least-once. A worker may execute the same job more
than once, so workers must be idempotent.

### State with age and position

A `latest` delivery includes a message ID, channel offset, and broker
receipt timestamp. Consumers can determine which value is newer and how
old it is. A point read retrieves one value without creating a
subscription.

### Bounded, inspectable storage

Channels use named memory or SQLite providers. SQLite data can be
inspected with ordinary tools, and retention and capacity are explicit
configuration rather than hidden broker defaults.

### Low client lock-in

Devices speak MQTT 3.1.1 or MQTT 5. Sagüin-specific operations use
standard MQTT packets, properties, response topics, and shared
subscriptions. Moving to another MQTT broker loses the channel semantics,
not basic fleet connectivity.

## Deliberate constraints

Sagüin is intentionally narrow:

* One node and one process; no clustering, consensus, or automatic
  failover
* Deliberate recovery and promotion rather than automatic leader election
* At-least-once delivery by default; no exactly-once *processing* and no
  deduplication by message ID
* QoS 2 is always offered, with half-finished publishes held on a storage
  provider; it can be denied per user or role in the ACL file
* Thousands of connections rather than millions
* No built-in rule engine or multi-tenancy layer, and no UI inside the
  broker: [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) is a
  separate web and terminal client you run beside it
* Memory storage survives a graceful shutdown snapshot, not a process
  crash or power loss
* SQLite provides stronger durability but still reflects SQLite and
  filesystem durability trade-offs
* MQTT 3.1.1 devices can publish into channels and read `append` and
  `latest`, but queue workers require MQTT 5
* Queue work is not replicated through a bridge; recovery requires a
  database copy
* Early days: run it where you can watch it, and tell us what breaks

If plain publish/subscribe and retained messages are all you need, any
mature MQTT broker will serve you well. Sagüin earns its place when you
also need the replayable log, the state store or the work queue.

## How it compares

Columns describe typical, default configurations. Every one of these is
good software; the table is about shape, not quality.

| | Standard MQTT 5 clients | Replayable log | Durable state store | Work queue: exclusive + retry + DLQ | Backlog survives a long outage | Small single binary | Storage you can open |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **NanoMQ** | ✓ | - | retained | - | - | ✓ | - |
| **EMQX** | ✓ | partial | retained | - | partial | - | - |
| **RabbitMQ** + MQTT plugin | ✓ | ✓ | - | ✓ | ✓ | - | - |
| **NATS JetStream** | 3.1.1 only | ✓ | ✓ | ✓ | ✓ | ✓ | - |
| **Sagüin** *(early)* | **✓** | **✓** | **✓** | **✓** | **✓** | **✓** | **SQLite** |

If you outgrow Sagüin, NATS JetStream is the natural next step, and that
is a good outcome rather than a defeat.

Sagüin is the wrong choice when the deployment requires multiple active
nodes, automatic failover, very high throughput, sophisticated stream
processing, or a mature clustered ecosystem.

## Install

You need:

* **Go 1.25 or later** (the version `go.mod` names) and **Make**, to build
  from source.
* **Python 3 with the `venv` module**, for the examples and the guided tour.
* **Docker**, only for the two stack demos (`examples/home-automation` and
  `examples/bento-connectors`) and the Q&A agent. Nothing else here needs it.

Build the broker from a clone of this repository:

```sh
git clone https://github.com/ifnesi/saguin.git
cd saguin
go build -o bin/saguin ./cmd/saguin    # or: make build
bin/saguin --version
```

Prebuilt binaries for Linux (amd64, arm64, armv7) and macOS (amd64, arm64)
are on the [Releases page](https://github.com/ifnesi/saguin/releases). There
is no Windows build: on Windows, run the container image below, or the Linux
binary under WSL2.

### Run in Docker

A multi-arch image (amd64 and arm64) is published at
`ghcr.io/ifnesi/saguin`. It runs as a non-root user (uid 10001), reads its
configuration from `/etc/saguin/saguin.yaml`, and keeps data under
`/var/lib/saguin`. In your configuration, point a SQLite provider's
`file_path` (and a memory provider's `snapshot_dir`) there, and let the MQTT
door listen on `0.0.0.0:1883`:

```sh
docker run -d --name saguin -p 1883:1883 \
  -v "$PWD/saguin.yaml:/etc/saguin/saguin.yaml:ro" \
  -v saguin-data:/var/lib/saguin \
  ghcr.io/ifnesi/saguin:0.1.0-rc.1
```

The named volume `saguin-data` is writable by the image's user. A host
directory mounted for the data must be writable by uid 10001.

## Quick start

Everything below runs from the repository root, with `bin/saguin` built as
above.

**1. Start a plaintext broker.**
Channels are configuration, not client-side objects: save this one as
`quickstart.yaml`. It declares one channel of each type and keeps its
database under `/tmp`, which many systems clear at reboot; for a broker
that must keep what it stores, point `file_path` at a durable directory:

```yaml
broker:
  id: edge-1
  storage:
    default: local
    default_retention_period: 3d
    default_retention_bytes: none
    providers:
      local:
        type: sqlite
        file_path: /tmp/saguin-quickstart.db
  operations:
    listen:
      tcp:
        address: 127.0.0.1:9090

channels:
  events:
    type: append
    filter: iot/+/devices/telemetry/#
    storage: local
    retention_period: 7d

  state:
    type: latest
    filter: iot/+/devices/state
    storage: local
    retention_period: 30d

  jobs:
    type: queue
    storage: local
    filter: iot/+/work/+
    visibility_timeout: 30s
    retry:
      max_attempts: 5
    dlq_retention_period: 30d
```

Then start the broker and leave it running in its own terminal:

```sh
bin/saguin --check-config quickstart.yaml   # validate without starting
bin/saguin --config quickstart.yaml
```

It listens on plaintext TCP port 1883 and admits every client, which is
for trying it on your own machine and nothing else. The operations
listener on `127.0.0.1:9090` is reachable from this machine only; it is
what [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) reads. Stop
it with Ctrl-C.

**2. Install the Python examples**, once, in a second terminal at the
repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r examples/requirements.txt
```

The examples use ordinary MQTT 5 clients; these use Eclipse Paho for
Python 3 (paho-mqtt v2).

**3. Run the guided tour.** It starts a broker of its own, on other ports,
so it can run beside the one above:

```sh
make demo
```

Use `make demo-full` for every deep dive, or `make demo-server` to leave the
example broker running for manual experimentation.

**4. Try the snippets against your broker.** Publishing does not change
according to channel type; the topic determines where the message lands.
Under `quickstart.yaml`, these land as an appended event, current state,
queued work, and ordinary broadcast respectively. Save each snippet to a
file and run it with `python` in the activated environment, from the second
terminal. They connect to `localhost:1883` with no credentials because they
show the shape; [the guided tour](examples/README.md) is the path against a
broker with TLS, WebSocket and Unix-socket doors open. The two readers run
until you stop them with Ctrl-C.

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="producer", protocol=mqtt.MQTTv5)
c.connect("localhost", 1883)
c.loop_start()
for topic, payload in [
    ("iot/line1/devices/telemetry/reading", '{"total":42}'),  # append
    ("iot/line1/devices/state", "online"),                    # latest
    ("iot/line1/work/resize", '{"id":"abc"}'),                # queue
    ("sensors/hallway", "21.5"),                              # broadcast
]:
    c.publish(topic, payload, qos=1).wait_for_publish()
    print("published", topic)
c.loop_stop()
c.disconnect()
```

Read an event stream with a durable consumer (a session expiry keeps its
position across a disconnect):

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 300  # a durable session: the position survives a disconnect

c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="my-consumer", protocol=mqtt.MQTTv5)
c.on_connect = lambda c, u, f, rc, p: c.subscribe("iot/+/devices/telemetry/#", qos=1)
c.on_message = lambda c, u, m: print(m.topic, m.payload.decode())
c.connect("localhost", 1883, clean_start=False, properties=props)
c.loop_forever()
```

Replay by the clock: a consumer moves its own position by publishing a
duration or an RFC 3339 time, from the connection that will do the
reading, which is also where the replayed records arrive:

```python
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 300

c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="my-consumer", protocol=mqtt.MQTTv5)
c.on_publish = lambda c, u, mid, rc, p: print("seek answered:", rc)
c.on_message = lambda c, u, m: print(m.topic, m.payload.decode())  # the replay arrives here
c.connect("localhost", 1883, clean_start=False, properties=props)
c.loop_start()
c.publish("$saguin/consumer/events/seek", "-12h", qos=1).wait_for_publish()
time.sleep(3)
c.loop_stop()
c.disconnect()
```

If retention has already removed part of what was asked for, the reply
says so rather than quietly serving the oldest surviving record.
[RFC 0003](docs/rfcs/0003-delivery-semantics.md) has the replies and the
rules.

Read current state:

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="state-reader", protocol=mqtt.MQTTv5)
c.on_connect = lambda c, u, f, rc, p: c.subscribe("iot/+/devices/state", qos=1)
c.on_message = lambda c, u, m: print(m.topic, m.payload.decode())
c.connect("localhost", 1883)
c.loop_forever()
```

A queue worker subscribes to the queue channel's name in Sagüin's reserved
topic form and acknowledges work from the same MQTT session that received
it:

```text
$saguin/queue/<channel>
```

See [the examples](examples/README.md) for a guided tour and runnable
deployments.

## Configuration at a glance

The [Quick start](#quick-start) shows a channel of each type. A real
deployment points `file_path` at a durable directory such as `/var/lib/saguin`.

This is only an introduction. The accepted keys, defaults, routing rules,
security settings, bridges, limits, and validation behaviour are defined
in [RFC 0002](docs/rfcs/0002-channels-and-configuration.md).

Validate a configuration without starting the broker, here the fully
annotated one in this repository:

```sh
bin/saguin --check-config examples/saguin.yaml
```

## Documentation

The RFCs are the authoritative documentation. If this README implies
stronger behaviour than an RFC, the RFC wins.

| Document | Covers |
|---|---|
| [Invariants](docs/invariants.md) | The failures Sagüin is designed never to report as success |
| [RFC 0001: Overview and scope](docs/rfcs/0001-overview-and-scope.md) | Product scope, vocabulary, MQTT mapping, supported protocol versions, and non-goals |
| [RFC 0002: Channels and configuration](docs/rfcs/0002-channels-and-configuration.md) | Topic routing, channel configuration, authentication, authorization, limits, listeners, bridges, and refusal codes |
| [RFC 0003: Delivery semantics](docs/rfcs/0003-delivery-semantics.md) | Delivery guarantees, replay, durable positions, latest state, queues, acknowledgements, retries, dead letters, ordering, retained messages, and Wills |
| [RFC 0004: Storage](docs/rfcs/0004-storage.md) | Memory and SQLite providers, durability, retention, snapshots, migration, backup, restore, and recovery |
| [RFC 0005: Operations](docs/rfcs/0005-operations.md) | Health, metrics, the read-only operations API, observability, and operational security |

If you would rather ask than read,
[docs/saguin-agent](docs/saguin-agent/README.md) is a self-hosted Q&A
agent that answers from these documents and nothing else, ending every
answer with the file and section it came from. It needs Docker and
[Ollama](https://ollama.com) installed and running, and the default model
pulled once with `ollama pull qwen2.5:3b` (about 2 GB); the first
`docker compose up --build` also downloads a small embedding model and
indexes the documents, which takes a few minutes. With this checkout and
Claude Code, `/saguin-ask` is the same job without the container; with
Codex, use `$saguin-ask`.

Additional resources:

* [Examples](examples/README.md) describes the demonstrations and
  supporting files.
* [`examples/demo.py`](examples/demo.py) is the guided end-to-end tour of
  the channel types and their security model.
* The [home-automation](examples/home-automation/README.md) demo shows
  Sagüin in a practical home and IoT setting.
* [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) is two viewers in
  a repository of their own: a web UI for inspecting and driving a live
  broker - every topic as a tree, a live feed of everything arriving,
  publish and seek forms, dead letters with one-click requeue, and
  configurable dashboards to cover the metric catalogue - and a terminal one
  for a headless box with no browser and no Prometheus. Both are clients
  beside the broker rather than a UI inside it, which is why they are not in
  this tree.
* [Bento connectors example](examples/bento-connectors/README.md)
  demonstrates MQTT ingestion, channels, monitoring, and Kafka
  integration.
* [Sagüin Python](https://github.com/ifnesi/saguin-python) and
  [Sagüin JS](https://github.com/ifnesi/saguin-js) are early optional
  convenience layers, over Eclipse Paho and over MQTT.js. They carry the
  same verbs under the same names. No SDK is required.

## Recovery model

Sagüin does not provide clustering or automatic failover. For `append`
and `latest`, a second Sagüin can hold a passive copy through an inbound
bridge, promotion staying a deliberate operator action; a queue cannot be
copied that way - reading queue work takes a lease - so queue recovery is
SQLite backup and restore. The requirements and runbooks are in
[RFC 0004](docs/rfcs/0004-storage.md).

## Security and operations

Sagüin supports password authentication, mutual TLS, ACLs, per-client
publishing limits, TCP and Unix listeners, and deployment behind a trusted
reverse proxy. The operations listener exposes health, Prometheus metrics,
and read-only diagnostic routes.

The password files use the `$7$` PBKDF2-HMAC-SHA512 format, hash for hash,
and read `$6$` (SHA512) ones too, so a deployment that already has such a
file keeps the credentials it has; the `saguin --passwd` subcommands manage
them without a second tool.

Security defaults and proxy details are intentionally not reproduced here.
Before exposing a broker or operations listener outside a trusted
development environment, read:

* [RFC 0002](docs/rfcs/0002-channels-and-configuration.md) for MQTT
  listeners, credentials, TLS, ACLs, and proxies
* [RFC 0005](docs/rfcs/0005-operations.md) for health, metrics, operations
  routes, and operator access

## Project status

Sagüin is early. The three channel types, retention, durable positions,
QoS 2 publishes, SQLite and memory storage, inbound bridges, retained
broadcast messages, Wills, metrics, and the operations API are implemented
and exercised by end-to-end tests.

Known gaps and constraints include:

* No Go SDK
* No automatic failover or clustering
* No certificate revocation inside Sagüin
* Most configuration changes require a restart; only the documented
  `SIGUSR1` paths - credentials, TLS certificates, WebSocket origins and
  the log level - are reloadable

Treat the RFCs and tests as the source of truth for current behaviour.

## Measured at scale

One Sagüin process, on an ordinary x86 laptop-class processor, ran two
thirty-minute scale tests, each across three machines. **Neither lost an
acknowledged record, and no client lost its connection.** Every subscriber
received every record it asked for, in order - a small share more than
once, as QoS 1 allows - and on every append and broadcast topic the last
record delivered was the last one its publisher had acknowledged. Both
ran with TLS and ACLs: every client connected over TLS, and every client's
topics were under the `acl_file`.

**Fleet: many thin clients.** 30,007 concurrent MQTT sessions - 20,000
durable subscribers and 10,000 publishers, each publishing a small message
every five seconds (2,000/s) - across append, latest and queue channels on
ten storage providers, five memory and five SQLite, plus broadcast topics.
Sagüin delivered 7.26 million records in 30 minutes 40 seconds, about
3,950 a second, with no dropped deliveries, refusals or storage errors,
and a replay read back all 1.28 million stored records with no gaps.
1,023 publishes (0.028%) timed out on the radio before their
acknowledgement arrived, and every one was stored. It cost **0.91 CPU
cores on average**, measured on a second run, which delivered about 3,590
records a second, **and 2.41 GB of memory on average and 2.79 GB at peak -
about 93 KB for each of the 30,007 connections**, with the memory
channels' records held in RAM. `/health` answered in under 0.4 ms
throughout, and a shutdown that wrote every snapshot took 1.3 s.

**Gateway: a plant.** 100 publishers sending 16 KB records - 500 a
second, 8 MB/s - into SQLite, 15 consumers each taking the whole stream,
and 4 over the air taking one stream each, with retention trimming the log
at 512 MiB from the second minute on. The 15 wide consumers received
**115.8 MB/s of payload** between them. A publisher on the broker's machine
had its publish acknowledged in **466 µs at the median and 6.8 ms at
p99**; over WiFi it was 21 ms and 123 ms, most of that the radio. It cost
**1.6 CPU cores and 65 MB of memory at peak**: the records live in SQLite,
not in memory.

**What this tells an operator:** at these loads the broker was not the
constraint. Memory follows connections and what the memory channels hold,
SQLite-backed channels keep it flat, and retention never overtook a
consumer that kept up. **What it does not establish is a capacity.** Both
runs were held to what one shared WiFi access point carries, with the
broker on WiFi too: they show behaviour under load, not a ceiling.

These are not benchmark-suite numbers: they come from the scale harness
(`make scale ROLE=…`, in `internal/scaletest`), which anyone can run
against their own hardware - one role per machine, coordinated through the
broker itself. [RFC 0004](docs/rfcs/0004-storage.md) carries the per-path
benchmark figures and how they were taken.

## On a Raspberry Pi

**On a Raspberry Pi 4, Sagüin held 10,000 connected sessions in under
400 MB, sustained over 62,000 QoS 0 and 26,000 QoS 1 messages a second,
and delivered every message at every fixed rate in every run.**

**The machine.** A Raspberry Pi 4 Model B: 4 cores, 4 GB, 64-bit Linux,
CPU governor `performance`, ext4 on a USB SSD. The clients ran on a
separate machine over wired Ethernet, so their CPU is not the Pi's.
Sagüin was built from the development tree that became the first public
commit.

**The method.** Five rounds; the table gives the median of the five, with
the range where the runs differed widely. The traffic was 100 publishers,
each paired with one subscriber, sending 128-byte payloads, with 16
publishes in flight each and Receive Maximum 20. Latency is end to end,
from when a message was due to be sent until it arrived, with both ends
on one clock, read to within 25%. The highest rate is the fastest offered
rate at which at least 95% of messages arrived and the p99 stayed under
one second, found to within 5%.

| measure | memory provider | sqlite provider | memory provider, with TLS, passwords and an ACL |
|---|---|---|---|
| memory, idle | 14.2 MB | 16.2 MB | 42.1 MB (38.7–51.9) |
| memory, 1,000 sessions connected | 47 MB | 61 MB | 77 MB |
| memory, 10,000 sessions connected | 240 MB | 375 MB | 375 MB |
| memory under traffic, 10,000 msg/s | 32 MB | 34 MB | 60 MB |
| time to connect and subscribe 10,000 clients | 2.6 s | 7.7 s | 35.7 s |
| CPU for each 1,000 msg/s, QoS 0 | 20% of a core | 20% | 24–25% |
| CPU for each 1,000 msg/s, QoS 1 | 29–38% of a core | 29–38% | 32–45% |
| QoS 0 at 10,000/s, p50 / p99 | 0.47 / 5.4 ms | 0.47 / 5.4 ms | 0.58 / 8.5 ms |
| QoS 1 at 5,000/s, p50 / p99 | 0.47 / 2.8 ms | 0.47 / 3.5 ms | 0.58 / 5.4 ms |
| QoS 1 at 10,000/s, p50 / p99 | 0.47 / 10.6 ms | 0.47 / 10.6 ms | 0.73 / 20.7 ms |
| highest rate, QoS 0 | 62,400 msg/s (59,500–65,200) | 62,400 msg/s | 42,500 msg/s (40,500–42,500) |
| highest rate, QoS 1 | 27,900 msg/s | 26,900 msg/s (26,900–27,900) | 19,900 msg/s |
| broker CPU at the QoS 0 highest rate | 3.5 cores | 3.5 cores | 3.6 cores |
| offline backlog of 1,000 sessions × 100 QoS 1 messages: store / deliver | 3.5 s / 2.0 s | 16.4 s / 31.3 s | 3.9 s / 5.9 s |

**The third column is how a deployment usually runs:** every client
connected over TLS with an RSA-2048 server certificate, authenticated
against a password file of 21,210 users (PBKDF2-SHA512, 1,000
iterations) and held to its own topics by an `acl_file`, on the memory
provider, from the same tree, five rounds. Most of its idle memory is
that password file; most of its connect time is the TLS handshake and
the password check, spread over the Pi's four cores. A certificate with
an ECDSA P-256 key costs far less to handshake than an RSA one, as
[RFC 0002](docs/rfcs/0002-channels-and-configuration.md) measures.

**The two providers keep different things.** The memory provider writes a
snapshot at a clean stop, and a crash loses what memory held. The sqlite
provider (WAL, `synchronous=NORMAL`) keeps every commit through a crash; a
power cut can lose what was committed in about the last flush interval
plus one fsync. That durability is what the sqlite column's connect and
backlog times pay for.

Anyone can re-run these measurements on their own hardware with the
`compare` role of the scale harness (`internal/scaletest`).

## Built on

Sagüin's MQTT engine handles packets, sessions, subscriptions, and flow
control. Sagüin implements the channel, storage, retention, queue, and
operations semantics around it.

The engine began as [mochi-mqtt/server](https://github.com/mochi-mqtt/server)
and now lives in `internal/mqtt`, stripped to what Sagüin uses: no
persistence backends, no `$SYS` publishing, no HTTP listeners, and none of
mochi's own configuration. It is MIT licensed and its notice ships in the
binary, which `saguin --licenses` prints.

## Development approach

**Sagüin was written mostly by Claude.** The code, the tests and these
documents were written by Claude (Anthropic's AI), working to my
direction, and reviewed by separate Claude sessions whose job was to find
defects rather than confirm previous work. I know IoT and MQTT well, and
how a broker must and should behave, so I decided the design, what the
broker promises, and what ships. I spent the past four years immersed in
Apache Kafka, data streaming platforms, and the difference between
messaging and streaming, and that is where Sagüin comes from: messaging,
streaming, a key-value store and a work queue, all over standard MQTT,
packed into a single binary.

The project deliberately prefers adding information to the existing RFC
set over creating an expanding collection of loosely authoritative
documents.

## Developing

Run these from the repository root with Go and Make installed. Timings were
measured on an 8-core desktop; fewer cores take longer.

```sh
make quick        # gofmt, vet, build, and every package but the two end-to-end ones
make docs         # the tests that read the README or an RFC; seconds
make test         # the whole suite; about 11 minutes
make ci           # what GitHub runs on the Go side: gofmt, vet, build, the suite; about 11 minutes
make check        # ci plus race, timing, bench, fuzz; run by maintainers before merging; about 35 minutes
make conformance  # Eclipse Paho MQTT 5 and 3.1.1 interoperability suites; about 6 minutes
make stress       # the suite six ways: race and not, GOMAXPROCS 1, 2, 4, shuffled; overnight, before a release
```

`make conformance` needs `python3`, `git` and the network, and ports 11883
and 11884 free. It runs the Eclipse Paho MQTT 5 and MQTT 3.1.1
interoperability suites against the broker, because a broker's own test
suite only drives what its author thought of. Every test Sagüin does not
pass is named individually in the Makefile with the reason.

GitHub runs `make ci` and `make conformance` on every pull request and
push to `main`. Maintainers run `make check`, `make mqtt5test` and
`make mqttconf` locally before merging.

`make mqtt5test` and `make mqttconf` run two more that build their own
MQTT packets, sending what no client library will construct - the half a
suite built on a client cannot reach. They need `cargo` and the network,
and ports 11884 to 11887 free, and take about 2 minutes each. Each is
pinned to a revision with its expected results named in the Makefile, and
a difference in either direction fails. `make help` lists every target.

The `mqtt5test` suite is ifnesi/mqtt_test, a fork of sammiq/mqtt_test
(GPLv3), whose original repository is no longer available.

## Contributing

Contributions are welcome. The most valuable contribution is often finding
where an RFC is wrong, ambiguous, incomplete, or self-contradictory before
that defect reaches the implementation.

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions use a Developer
Certificate of Origin sign-off; there is no separate CLA.

## Licence

[Apache-2.0](LICENSE), except the MQTT engine in `internal/mqtt`, which
began as [mochi-mqtt/server](https://github.com/mochi-mqtt/server) and stays
under [MIT](internal/mqtt/LICENSE.md). That notice travels inside the
binary: `saguin --licenses` prints it, alongside every dependency's.

Copyright 2026 Italo F L Nesi.
