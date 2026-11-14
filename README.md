# Sagüin

<img src="docs/img/saguin_256.png" align="left" alt="Sagüin" width="174" style="padding:16px;">

Sagüin (sah-GWEEN) is a single-node MQTT 3.1.1 and 5 broker for edge and
small-scale IoT/IIoT deployments. It adds three durable messaging
primitives to ordinary MQTT topics:

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

One rule shapes every detail below: **stored before acknowledged.** A
reply confirming a change waits for its commit. What survives a failure
depends on the storage mode: `full` syncs every commit before replying;
`normal` syncs periodically; `memory` recovers from its last disk
snapshot. Several publications may share one commit. Where Sagüin cannot
honour a request, it refuses rather than accepting and quietly doing
less, with a reason code wherever MQTT allows one
([PUBLISH](docs/design.md#publish)).

## Why Sagüin exists

A truck parks in a basement for nine days. A substation's cellular modem
sulks through a storm. A vessel crosses a stretch of ocean where the
only thing the antenna can see is more ocean.

All three come back. If the broker kept only a bounded queue of what each
device missed, and that queue overflowed on day one, the readings are
gone, and the first person to notice is the customer looking at a gap in
a chart three weeks later.

That is not a bug in MQTT. MQTT defines how clients and brokers exchange
messages, including through persistent sessions. The trouble is that edge
data stopped being telemetry and became business data, and "the link
dropped, so the readings are gone" is not a sentence you can put in a
billing dispute.

The usual fix stacks a streaming platform for history, a cache for
state, and a job queue for work beside the broker: four systems, four
client libraries, four failure modes, and four sets of credentials.
Sagüin brings replayable history, current state and acknowledged work
into one binary and one SQLite store, using MQTT as the interface.

## Core concepts

A channel is a named, typed log holding the topics its MQTT topic filter
matches. Its type decides how those messages behave.

| Channel type | Purpose | Behaviour |
|---|---|---|
| `append` | Events, telemetry, audit history | A Kafka-like topic with delete retention: replay, and a stored position per consumer |
| `latest` | Device state, configuration, caches | A Kafka-like compacted topic: one current value per topic, sent on subscribe and available through point reads |
| `queue` | Commands and background work | Kafka-like share groups: one worker at a time per job, with acknowledgement, retries, attempt limits, and dead letters |

A topic that no channel claims goes to `$broadcast`, a reserved `append`
channel that follows MQTT. Every message on it is stored, whatever its QoS,
and each session keeps one position on it, stored if the session is
durable. Retained messages are a flag on those records, with their own
size limit (`retained.max_bytes`) and lifetime
(`retained.retention_period`), and can be denied per user or role in the
`acl_file`.

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

Sagüin is especially useful when the hard problem is surviving outages and
retaining useful local history, not processing millions of messages per
second.

## What makes it different

### A position, not a copy

MQTT already has durable-sounding words, and they are not the same
thing:

| | What it is | Survives the device being away? |
|---|---|---|
| **Retained message** | the one latest value on a topic | Yes: the latest value, never the history |
| **Persistent session** (Session Expiry above zero) | in MQTT, the messages a client missed, kept for it | As far as the broker keeps them |
| **Sagüin channel** | one shared log and a *position* per consumer | Yes: bounded by the channel's retention, not by the client |

Sagüin stores every message once, in its channel's log, and keeps a
position per consumer, stored for durable sessions: the same idea as a
Kafka consumer group's committed offset, without the cluster. That buys
three things at once:

1. A week offline costs the same as a minute: one number either way.
2. Nothing fills per client, because there is no per-client copy.
3. A restart restores durable positions from the state the storage mode
   kept ([Store](docs/design.md#store)).

The position stays small as a consumer falls behind. The retained history
still takes storage, and in `memory` mode that storage occupies RAM.

Retention still removes old records, as the operator sets it for the
consumers the channel must serve. When retention passes records a
consumer was owed, the consumer continues from the oldest surviving
record and the loss is counted: in `saguin_missed_total`, and per reader
in the operations route `position-lost`. A seek to a time retention has
already passed answers `below-floor` rather than quietly serving the
oldest survivor.

### An alert when retention passes a consumer

```
saguin_channel_floor_offset > saguin_channel_position_min
```

When that expression fires, retention has passed a consumer's stored
position: that consumer's data is already gone, and the broker says so
before the customer does. The offset gap is not a count of lost records;
the broker counts the records each consumer actually missed in
[`saguin_missed_total`](docs/metrics.md#delivery).

### Three messaging semantics behind MQTT

Publishers continue to send ordinary MQTT messages. Configuration decides
whether a topic is broadcast, appended to a log, stored as current state,
or treated as work.

### A real work queue

MQTT shared subscriptions provide load balancing, but in MQTT `PUBACK`
means "I received the bytes", not "I did the job". A Sagüin queue adds
an application acknowledgement: a worker answers `ack` or `return` for
each job. It also adds a visibility timeout, a retry policy (`fixed`,
`linear` or `exponential`), an attempt count, and a derived dead-letter
channel, `<channel>__dlq`, whose topics carry the prefix `__dlq/`.

A job whose attempt fails or times out follows the retry and dead-letter
rules in [Delivery, Queue jobs](docs/design.md#delivery). A worker may see
the same job more than once, so workers must be idempotent.

### State with age and position

A delivery from a configured channel to an MQTT 5 client carries
`saguin-id`, `saguin-offset` and `saguin-timestamp`. Consumers can tell
which value is newer and how old it is. A point read
(`$saguin/kv/get`) retrieves one current value without creating a
subscription.

### Bounded, inspectable storage

All stored state lives in one SQLite database, in one of three modes:
`full` (every commit synced), `normal` (periodic syncs) or
`memory` (run from a RAM copy, snapshotted to disk). Every table, with
its columns, keys and indexes, is documented in
[docs/storage.md](docs/storage.md#sagüin-storage). Retention and limits are
explicit configuration. Every resource the broker holds has a limit, set
by configuration or by the disk.

Use `full` unless the disk is slow, such as an SD card. There, `normal` or
`memory` write to it far less often, if losing recent data is acceptable.
With `normal`, a power cut or an operating-system crash loses the commits
since the last completed sync; the broker syncs every `sync_interval`
(1 second by default). With `memory`, a crash of the broker or a power cut returns to
the last snapshot, taken every `snapshot_interval` (30 minutes by default)
and at a graceful stop.

### Overload slows admission

When Sagüin falls behind on delivery work for consumers that have caught
up, it pauses new publication bodies while that work catches up. Waiting
for a client's acknowledgement or socket does not count as broker delay
([Delivery](docs/design.md#delivery)). Separate budgets bound message bytes
in transit ([Message memory](docs/design.md#message-memory)).

In practice, a stream shared by many subscribers stays deliverable. With
ten publishers sending as fast as they could to ten QoS 1 subscribers on
one stream, Sagüin delivered about 178,000 messages a second on a 16-core
AMD Ryzen 7, and every message it accepted reached all ten subscribers,
the broker at 60 MiB.

This does not guarantee end-to-end latency or prevent retention from
passing a consumer. Slow, disconnected and replaying consumers remain
subject to retention.

### Access control shaped by the data

One `acl_file` grants each role what it may do on each channel: write,
read and seek on a stream, consume on a queue, write and read on a latest
value. A rule can follow the client (`events/%u/#`), so a thousand sensors
share one role and each writes only its own topics. A role can also deny
whole features: persistent sessions, Wills, shared subscriptions,
retained messages, QoS 2. Users are matched by name pattern, each with its
roles, the client ids it may use, and publish limits per connection, in
messages and bytes per second. A mistake in the file refuses it and names
the entry, `saguin --acl` explains any client's rights, and SIGUSR1
applies a change without disconnecting anyone.

### Low client lock-in

Devices speak MQTT 3.1.1 or MQTT 5. Sagüin-specific operations use
standard MQTT packets, properties, response topics and ordinary
subscriptions. Moving to another MQTT broker loses the channel semantics,
not basic fleet connectivity.

## Deliberate constraints

Sagüin is intentionally narrow:

* One node and one process; no clustering
* At-least-once delivery at QoS 1 and 2; no exactly-once *processing*,
  and no deduplication by message id: a publish repeating a stored
  `saguin-id` makes a second record
* QoS 2 is offered, with half-finished incoming publishes stored; it can
  be denied per user or role in the `acl_file`
* Thousands of connections rather than millions
  (`limits.max_connections` defaults to 10,000)
* No UI inside the broker:
  [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) is a separate
  client you run beside it
* `full` mode keeps everything acknowledged through a crash or a power
  cut. `normal` mode keeps it through a broker crash; a power cut or an
  operating-system crash loses at most the commits since the last sync.
  `memory` mode restarts from its last snapshot, needs a tmpfs, and is
  refused on macOS
* Retention may pass a record a session is still owed, even one in
  flight: the session continues and the loss is counted
* MQTT 3.1.1 devices can publish into any channel and read `append` and
  `latest`, but receive no `saguin-` properties, and queue workers
  require MQTT 5
* 0.2.0 is an early release: run it where you can watch it, and tell us
  what breaks

Sagüin earns its place when a deployment needs replayable history,
current state or acknowledged work alongside MQTT. Storing and tracking
that work costs CPU and storage.

## When to choose something else

Sagüin is at its best when devices disappear for days and must catch up,
when consumers need to replay what they missed, when acknowledged data
must survive a power cut, and when one small machine has to do the work of
a broker, a log, a cache and a queue. That is the job it was built for.

It is not built for raw throughput. Throughput varies substantially with
the workload and machine.
Independent-topic traffic and `full` mode on slower storage need
particular care. CPU use can be substantial on small machines. Measure
the intended workload before sizing a deployment.

Sagüin does not provide clustering or automatic failover. Deployments
requiring those features, very high throughput or sophisticated stream
processing need a different architecture. If you outgrow it, a clustered
streaming platform such as Kafka is the natural next step, and that is a
good outcome rather than a defeat.

## Install

You need:

* **Go 1.27.2 or later** (the version `go.mod` names), **Make**, and
  **gcc** with a static glibc, to build from source: SQLite's C code is
  linked into one static Linux binary.
* **Python 3 with the `venv` module**, for the examples and the guided tour.
* **A container runtime with Compose**, only for the two stack demos
  (`examples/home-automation` and `examples/bento-connectors`).

Release binaries are built for Linux on 64-bit x86 (amd64) and 64-bit ARM
(arm64), with a container image for both at `ghcr.io/ifnesi/saguin`.
**Sagüin is 64-bit:** it has not been tested on 32-bit systems, as there
has been no demand for them so far. A Raspberry Pi therefore needs a 64-bit
operating system (a Pi Zero 2 W, a Pi 3 or later). On macOS or Windows,
run the container image (Docker Desktop, for example), or on Windows the
Linux binary under WSL2.

Download a release from
[GitHub's releases page](https://github.com/ifnesi/saguin/releases) and
check it against its checksums (use `arm64` in place of `amd64` on a 64-bit
ARM machine):

```sh
curl -LO https://github.com/ifnesi/saguin/releases/download/v0.2.0/saguin-0.2.0-linux-amd64.tar.gz
curl -LO https://github.com/ifnesi/saguin/releases/download/v0.2.0/SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
tar -xzf saguin-0.2.0-linux-amd64.tar.gz
saguin-0.2.0-linux-amd64/saguin --version
```

Or run the container image. It reads its configuration from
`/etc/saguin/saguin.yaml` and keeps its data in the volume
`/var/lib/saguin`, so point `file_path` there; its MQTT listener must
listen on `:1883`, not `127.0.0.1`, to be reachable from outside the
container:

```sh
docker run -d --name saguin -p 1883:1883 \
  -v "$PWD/saguin.yaml:/etc/saguin/saguin.yaml:ro" \
  -v saguin-data:/var/lib/saguin \
  ghcr.io/ifnesi/saguin:0.2.0
```

In a container, `memory` mode's RAM copy lives in `memory_dir`
(`/dev/shm/saguin` by default), and Docker gives `/dev/shm` 64 MB unless
`--shm-size` raises it.

Or build the broker from a clone of this repository:

```sh
git clone https://github.com/ifnesi/saguin.git
cd saguin
make build
bin/saguin --version
```

## Quick start

Everything below runs from the repository root, with `bin/saguin` built as
above.

**1. Start a plaintext broker.**
Channels are configuration, not client-side objects: save this one as
`quickstart.yaml`. It declares one channel of each type and keeps its
database under `/var/tmp`; for a broker that must keep what it stores,
point `file_path` at a durable directory such as `/var/lib/saguin`:

```yaml
broker:
  id: edge-1
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:1883
  storage:
    file_path: /var/tmp/saguin-quickstart.db
    default_retention_period: 3d
    default_retention_bytes: none
  operations:
    listen:
      tcp:
        address: 127.0.0.1:9090

channels:
  events:
    type: append
    filter: iot/+/devices/telemetry/#
    retention_period: 7d

  state:
    type: latest
    filter: iot/+/devices/state
    retention_period: 30d

  jobs:
    type: queue
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

It listens on plaintext TCP port 1883 on this machine only and admits
every client, which is for trying it and nothing else. The store runs in
`full` mode, the default. The operations listener on `127.0.0.1:9090` is
what [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) reads. Stop
the broker with Ctrl-C.

**2. Install the Python examples**, once, in a second terminal at the
repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r examples/requirements.txt
```

The examples use an ordinary MQTT 5 client library.

**3. Run the guided tour.** It starts a broker of its own, with the
shipped configuration:

```sh
make demo
```

Use `make demo-full` for every deep dive, or `make demo-server` to leave the
example broker running for manual experimentation.

**4. Try the snippets against your broker.** Publishing does not change
according to channel type; the topic determines where the message lands.
Under `quickstart.yaml`, these land as an appended event, current state,
queued work, and ordinary broadcast respectively. Save each snippet to a
file and run it with `python` in the activated environment. They connect
to `localhost:1883` with no credentials because they show the shape;
[the guided tour](examples/README.md) is the path against a broker with
TLS, WebSocket and Unix-socket listeners open. The readers run until you
stop them with Ctrl-C.

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
    ("sensors/hallway", "21.5"),                              # $broadcast
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

Replay by the clock: a durable session moves its own position by
publishing an offset, a duration ago or an RFC 3339 time to
`$saguin/consumer/<channel>/seek`. Reading then resumes from the new
position, on the session's subscription:

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
c.publish("$saguin/consumer/events/seek", "12h", qos=1).wait_for_publish()
time.sleep(3)
c.loop_stop()
c.disconnect()
```

If retention has already removed part of what was asked for, the seek is
refused (`below-floor`) rather than quietly serving the oldest surviving
record. [`$saguin` topics](docs/design.md#saguin-topics) has the replies
and the rules.

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

A queue worker subscribes at QoS 1 to the queue channel's name in
Sagüin's reserved topic form, and answers each job with `ack` or `return`
on its response topic, carrying the job's Correlation Data:

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

def on_message(c, u, m):
    print("job", m.topic, m.payload.decode())
    props = Properties(PacketTypes.PUBLISH)
    props.CorrelationData = m.properties.CorrelationData
    c.publish("$saguin/queue/jobs/response", "ack", qos=1, properties=props)

c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="worker-1", protocol=mqtt.MQTTv5)
c.on_connect = lambda c, u, f, rc, p: c.subscribe("$saguin/queue/jobs", qos=1)
c.on_message = on_message
c.connect("localhost", 1883)
c.loop_forever()
```

See [the examples](examples/README.md) for the guided tour and runnable
deployments.

## Configuration at a glance

The configuration is one YAML file, read strictly: an unknown or repeated
key stops startup. `channels:` is a map keyed by channel name, and may
pull groups of channels from other files with `!include`.

This is only an introduction. Every key, its default and its meaning are
in [docs/config.md](docs/config.md#sagüin-configuration). A key the broker
does not take stops startup; for the keys in its
[Dropped](docs/config.md#dropped) table, the refusal also names a replacement
where one exists.

Validate a configuration without starting the broker, here the fully
annotated one in this repository; `--output` prints it resolved, with
every default filled in:

```sh
bin/saguin --check-config examples/saguin.yaml
```

Three commented starting points sit in
[examples/starting-points](examples/starting-points): a fleet broker, a
home hub and a plant gateway.

## Documentation

The design is the authoritative documentation. If this README implies
stronger behaviour than the design, the design wins.

| Document | Covers |
|---|---|
| [Design](docs/design.md#sagüin-design) | The rules, channels, the store, `$saguin` topics, bridges, access and reload, operations, and every MQTT flow, with Sagüin's choices where MQTT leaves them open |
| [Configuration](docs/config.md#sagüin-configuration) | Every key, with its default and meaning, and the keys it refuses, with their replacements |
| [Metrics](docs/metrics.md#sagüin-metrics) | Every series on `/metrics`, with its labels and meaning |
| [Storage](docs/storage.md#sagüin-storage) | Every SQLite table, the schema version, and tuning: machines, drives, settings and capacity |
| [MQTT standards](docs/mqtt) | MQTT 5.0 and MQTT 3.1.1 Plus Errata 01, the texts Sagüin is tested against |

Additional resources:

* [Examples](examples/README.md) describes the guided tour and its
  supporting files.
* [`examples/demo.py`](examples/demo.py) is the guided end-to-end tour of
  the channel types and their security model.
* The [home-automation](examples/home-automation/README.md) demo shows
  Sagüin in a practical home and IoT setting, with emulated radio
  hardware.
* The [connectors example](examples/bento-connectors/README.md) moves
  readings from simulated devices through Sagüin into Kafka.
* [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) is a client in
  a repository of its own for inspecting a live broker, which is why it is
  not in this tree.
* [Sagüin Python](https://github.com/ifnesi/saguin-python) and
  [Sagüin JS](https://github.com/ifnesi/saguin-js) are optional
  convenience layers over ordinary MQTT clients. No SDK is required.

### Asking an AI agent

This repository carries two skills for AI coding agents. Each comes in two
copies: `.claude/skills/` for Claude Code, and `.agents/skills/` for
agents that read that folder. Open a session in a checkout and either
skill is offered when a request matches it, or you can name it.

| Skill | Use it when | What it does |
|---|---|---|
| `saguin-ask` | You want to know how Sagüin behaves, what a setting does, whether it can do something, or why the broker did what it did | Answers from this repository's documents, with a reference under every claim, and says "the documentation does not say" where there is none. It changes nothing |
| `saguin-config` | You are setting Sagüin up for a use case, or reviewing a configuration you already have | Runs a discovery session: it asks about your machine, clients, data flows, access control and security, one topic at a time, then writes the configuration to a folder you name, checks it with `--check-config` and explains each choice with a reference |

Without a coding agent, the [Q&A agent](docs/saguin-agent/README.md) in
`docs/saguin-agent/` does `saguin-ask`'s job as a small web app. It uses
ollama by default, or another model set in its `.env`. Run
`docker compose up --build` in that folder and open
<http://localhost:5005>. It answers from the same documents, lists what it
read under every answer, and states the commit those documents came from.

All three read the documents rather than remembering them, and the skills
only ever name keys from
[docs/config.md](docs/config.md#sagüin-configuration). Check their answers
as you would a colleague's: the design is still the authority.

## Recovery model

Sagüin does not provide clustering or automatic failover. A restart
recovers what its storage mode kept: durable sessions with their
subscriptions, positions, in-flight messages, held QoS 2 exchanges and
Wills, retained flags, queue attempt counts and retry times, and each
channel's offset counter. Queue holds are not restored: a job a worker
held is offered again. A damaged database stops the broker rather than
serving from it.

Bridges connect Sagüin to a peer broker, as an ordinary MQTT 5 client of
the peer, moving topics `in`, `out` or `both`. The receiving broker's own
channels decide where a record lands. [Bridges](docs/design.md#bridges)
has the rules.

## Security and operations

Sagüin supports password authentication, mutual TLS, an `acl_file`,
per-connection publish limits, TCP, WebSocket and Unix-socket listeners,
and the PROXY protocol for a trusted proxy in front. TLS listeners and
bridges accept TLS 1.2 and 1.3, with `min_version` `1.2` by default. Set
`min_version: 1.3` wherever every client supports it: TLS 1.3 offers only
modern cipher suites and always encrypts the handshake's certificates. The operations
listener exposes `/health`, Prometheus-format `/metrics`, and read-only
JSON routes under `/v1/operations/`: ACL, configuration, bridges,
consumers, sessions, users, queues, refused connections and lost
positions. A non-loopback operations listener needs a password file or a
required client certificate.

The password files use the `$7$` PBKDF2-HMAC-SHA512 format and read
`$6$` (SHA-512) ones too, so a deployment that already has such a file
keeps the credentials it has. The command line also does the operator's
work:

* `saguin --passwd` manages a password file
* `saguin --acl <config> <user>` explains what a client may do and why
* `saguin --route <config> <topic>` says which channel a topic lands in
* `saguin --drop-channel <config> <channel>` removes a channel the
  configuration no longer has, with the broker stopped

SIGUSR1 reloads the log level, WebSocket origins, password files, the
`acl_file` and every TLS certificate, without disconnecting anyone.
Nothing else reloads.

Sagüin writes its log to standard error only, never to a file, so keeping,
rotating and cleaning it up belongs to whatever runs it: systemd's journal
(`journalctl -u saguin`) or Docker's log driver options. Besides warnings
and errors, it logs its start and stop, each reload, refused credentials,
an operator's hang-up, a full disk, overload and certificates near their
expiry; per-message and per-connection events are counted in the metrics
instead ([Logging](docs/design.md#logging)).

Before exposing a broker or operations listener outside a trusted
development environment, read
[Access and reload](docs/design.md#access-and-reload),
[Operations](docs/design.md#operations) and the listener keys in
[docs/config.md](docs/config.md#a-listener).

## Project status

Sagüin 0.2.0 is an early release. Its database schema is version 21: a
file holding a schema at any other version is refused at startup, naming
both versions. A missing file, or one with no schema objects at version 0,
is created as a new database ([Storage](docs/storage.md#sagüin-storage)). The
three channel types, `$broadcast`
with retained values, retention, durable positions, seeks, partitioned
reading, QoS 2, Wills, shared subscriptions, the three storage modes,
bridges, metrics and the operations routes are in the design and tested
from outside, as clients and operators see the broker.

Treat the [design](docs/design.md#sagüin-design) as the source of truth for
current behaviour. Tests take their expectations from it.

## Measured

**The load.** 100 publishers and 100 durable subscribers at QoS 1, each
record going to 10 subscribers, 256-byte payloads, half the publishers at
QoS 0 and half at QoS 1, over loopback with the load tool on the same
machine. The store in `full` mode, every commit synced before it is
acknowledged. The checks reported no lost, duplicated or reordered
messages in those runs. They do not establish behavior under every
failure.

| Machine | Drive | As fast as the clients can, deliveries/s | Ten minutes at a fixed rate, publications/s kept up | Broker memory at peak |
|---|---|---|---|---|
| AMD Ryzen 7 260, 16 logical cores, 56 GB | consumer NVMe, ext4 | 165,628 | 12,400, every second | 40-60 MiB |
| AMD A10, 4 cores, 13 GB | SATA SSD, Btrfs, copy-on-write off | 31,794 | 2,000, every second | 47-91 MiB |
| Raspberry Pi 4 Model B, 4 ARM cores, 4 GB | SSD over USB, ext4 | 14,975 | 1,100, every second | 47-54 MiB |

On the two four-core machines the broker shared its cores with the load
tool, and one store writer's CPU was the limit. On the Ryzen, a backlog of
1,000,000 QoS 1 publications for away subscribers drained at 83,424 to
86,898 a second when they returned.

**External load generator.** On a 16-core Ryzen, fan-out to ten
subscribers reached 426,000 deliveries a second at QoS 0 and 178,000 at
QoS 1 in `memory` mode, 449,000 and 131,000 in `full` mode. emqtt-bench
exercised clean MQTT 5 subscribers with 256-byte payloads for 60 seconds.
Each result is the median of per-second rates from one run. Every topic
used `$broadcast`.

In QoS 1 fan-out to ten subscribers, observed deliveries/s in `memory` /
`full` were 178,489 / 131,197 on Ryzen, 27,790 / 4,584 on A10, and
14,870 / 9,270 on Pi. With 100 publisher/subscriber pairs, they were
37,484 / 33,185, 7,468 / 3,430, and 5,092 / 4,137 respectively.

Ryzen and A10 shared their CPUs with the generator; Pi used a remote
generator and capped publishing rates. These results do not establish
sustained capacity, durable-subscriber performance or a latency
guarantee.

**What it does not establish is a capacity.** These are single runs of
one recipe, on these machines. The measurements, the limits of each
workload and how to choose a machine and drive are in
[storage.md, Tuning](docs/storage.md#tuning). `make load` runs the same
recipe on your own hardware.

## Built on

Sagüin's code is its own, written from scratch. Its store is SQLite, linked
statically into the binary. `saguin --licenses` prints the licences of
every piece of code inside.

## Development approach

**Sagüin is written with Claude and GPT.** Claude (Anthropic) writes
every test, from the design and the MQTT standards, never from the
code; GPT (OpenAI) writes the code; a different model reviews and
audits, so writer and reviewer are never the same. I know IoT and MQTT
well, and how a broker must and should behave, so I decide the design,
what the broker promises, and what ships. I spent the past four years
immersed in Apache Kafka, data streaming platforms, and the difference
between messaging and streaming, and that is where Sagüin comes from:
messaging, streaming, a key-value store and a work queue, all over
standard MQTT, packed into a single binary.

## Developing

Run these from the repository root with Go, gcc and Make installed:

```sh
make build        # the static binary, bin/saguin
make check        # gofmt, module checks, vet, the linter, the race detector
make conformance  # the conformance packages, driving the broker from outside
make fuzz         # a short fuzz of every input from outside the broker
make soak         # a short soak with several clients
make ci           # check, conformance, fuzz, soak, end-to-end fuzz; one budget
make interop      # five outside MQTT 5 and 3.1.1 suites, pinned
make load         # the throughput and latency recipe; about half an hour
```

GitHub runs `make check` on every push and pull request; run `make ci`
locally before sending a change. `make interop` fetches its suites over the network unless
given local copies; each is pinned to a revision with its expected
results kept in the repository. `make load` wants the machine to itself.

## Contributing

Contributions are welcome. The most valuable contribution is often finding
where the design is wrong, ambiguous, incomplete, or self-contradictory
before that defect reaches the implementation.

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions use a Developer
Certificate of Origin sign-off; there is no separate CLA.

## Licence

[Apache-2.0](LICENSE). `saguin --licenses` prints every dependency's
licence, which travels inside the binary.

Copyright 2026 Italo F L Nesi.
