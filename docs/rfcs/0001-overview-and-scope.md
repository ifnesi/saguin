# RFC 0001 - Overview and scope

**Status:** Draft
**Authoritative on:** what Sagüin is, the vocabulary the other RFCs use,
and the MQTT 5 mechanism behind each Sagüin concept.

## What Sagüin is

Sagüin is a single-node MQTT 5 broker that adds three durable messaging
primitives to ordinary publish/subscribe:

| Channel type | What it is |
|---|---|
| `append` | A durable, replayable, append-only event stream |
| `latest` | A durable latest-value-per-topic state store |
| `queue` | Durable work distribution with acknowledgement, redelivery, and dead-lettering |

Anything published to a topic that no channel claims is ordinary MQTT
broadcast: delivered to whoever is connected, stored nowhere - the one
exception being a publish that asked to be retained, which the retained
store every broker has keeps as one current value per topic, bounded by
its provider (RFC 0002). Broadcast is not a channel type and is not
configured. It is what Sagüin does when you have not asked for
anything more.

A channel is configuration. It carries an ordinary MQTT topic filter and
gives every topic that filter matches a semantic. Where two channels'
filters both match a topic, the one that spells it out most exactly holds
it, so an operator dividing `iot/<site>/<device>/…` between a log, some
current state and a work queue writes three filters and nothing else.

**Nothing about any of that is visible on the wire.** A client publishes to
`iot/hq/dev-1/events` whether or not a channel claims it, and the
difference is entirely what the broker does afterwards. The one exception
is a queue, whose workers name the channel to form a consumer group, and
RFC 0002 says why a queue admits that one form and no other.

**The property that matters most at the edge falls out of that design
rather than being bolted onto it**, and it is the difference a reader
should take from this section. In an ordinary broker, an offline
persistent session accumulates a per-session copy of every message it
missed, bounded by a broker-side limit; a link down for six hours
overflows that buffer and the messages are gone. In Sagüin, a durable
consumer's position is a cursor into the channel's own log. The backlog
costs one integer per consumer, not one copy per consumer, so:

> **A persistent session cannot overflow, and a reconnecting consumer
> resumes from a durable offset rather than from whatever survived in a
> buffer.**

Standard clients get this without knowing it exists. It is the reason to
choose Sagüin over a broker that is otherwise more mature.

**What it does not remove is retention.** There is no per-session backlog
to overflow, and the channel's own records are still deleted by age and by
size on the schedule the operator wrote. A consumer offline longer than
its channel's retention period comes back to find its position below the
floor - which is reported rather than silently skipped, and is
[invariants.md](../invariants.md) 1 rather than this property failing.

## The protocol is MQTT 5, unmodified

Sagüin defines no packet types, no properties, no flags, and no wire
extensions. A stock `mosquitto_pub`, Paho, or MQTT.js client is a
first-class client, and the broker enforces every rule in this
specification against whatever such a client actually sends. The broker is
the only enforcement point; a rule that only an SDK upholds is not a rule.

Where Sagüin needs an operation MQTT does not provide - acknowledging
application work, replaying from a position, administering the broker -
it uses an ordinary `PUBLISH` to a documented topic under the reserved
`$saguin/` prefix. That is an application convention layered on standard
MQTT, in the same sense that a REST API is a convention layered on HTTP.
It is not a protocol extension, and a generic client can perform it with
no special support.

Every mechanism, on one page:

| Sagüin concept | MQTT 5 mechanism |
|---|---|
| Publish to a channel | `PUBLISH` to any topic the channel's filter matches |
| Durable consumer position | `client_id` + Clean Start = 0 + Session Expiry Interval, capped by `limits.max_session_expiry` |
| Queue consumption | `SUBSCRIBE` to `$saguin/queue/` + the queue's name, at QoS 1 |
| Consumer prefetch | Receive Maximum. A queue worker holds one job from each queue at a time, whatever its Receive Maximum |
| Application acknowledge / return | `PUBLISH` to the delivery's Response Topic, echoing its Correlation Data |
| Delivery ID | Correlation Data on the delivered message |
| Message ID | A User Property, `saguin-id` |
| A record's position on delivery | A User Property, `saguin-offset` |
| When the broker received a record | A User Property, `saguin-timestamp`, on every channel delivery |
| Whether to be sent current state on subscribe | Retain Handling, on the subscription |
| A device announcing its own death | Last Will, routed through the ordinary publish path |
| Waiting before announcing it | Will Delay Interval, held by the broker |
| Per-message expiry | Message Expiry Interval, delivered less the time the record waited |
| Which subscription matched a delivery | Subscription Identifier, on every delivery |
| Publish and subscribe refusals | `PUBACK` and `SUBACK` reason codes |
| Latest state on subscribe | Retained-message delivery, from a `latest` channel |

The table is the argument. Nothing in the left column required inventing
anything in the right.

**A channel delivers its own records** rather than letting the server fan
them out - which is what lets a subscriber be served from its position
rather than from whatever happened to arrive while it was connected, and
what keeps a wildcard away from a queue (invariant 11). So a delivery from
an `append` or `latest` channel is a packet Sagüin built rather than one
the server matched and forwarded, and it carries the Subscription
Identifier of every matching subscription, as MQTT 5 section 3.3.4
requires - on an `append` channel's records, a `latest` channel's values
and the retained store's alike - so one client gets one behaviour whether
or not an operator has placed a channel over the topic, which is a thing
that client is deliberately not told (RFC 0002 *What a filter reaches*).

**Where a record matches several of one client's subscriptions it carries
every one of their identifiers.** Sagüin sends a single copy, and 3.3.4
requires that copy to name them all.

**A publisher's expiry never removes a record.** A Message Expiry Interval
is stored with the record and delivered decremented by the time the record
has waited, as MQTT requires, and is not sent once it has run out - but the
record is still there for the next reader. **A channel's expiry is its
retention policy**, which is operator configuration rather than a
per-message property, and the two are not substitutes: retention is
declared once for every record in the channel (RFC 0003).

The broker adds neither of them on its own. A delivery carries a Message
Expiry Interval only when the publisher sent one, and a Subscription
Identifier only when the subscriber asked for one.

**The retain flag asks for a store, and Sagüin answers with whichever one
holds that topic.** A `latest` channel's subscribe pass carries the flag -
MQTT's own "this is current state" - and every change after it arrives
without it, unless the subscriber asked for Retain As Published. A
publisher's flag is honoured by an `append` or `latest` channel, or by
the retained store every broker keeps for broadcast topics, and dropped
on a queue (RFC 0003 "Retained messages").

**Topic Alias is offered, capped at sixteen per connection**, and the
`CONNACK` says so, so a client never has to guess. A device publishing to a
900-byte topic sends it once and two bytes afterwards, which is the MQTT 5
feature that pays for itself on exactly the link Sagüin is for.

The cap is the design rather than a limitation of it: left at the
substrate's default of 65,535 nothing bounds the table, where sixteen
makes the worst case sixteen times `max_topic_length` per connection - a
bound an operator can work out from two numbers they already set
(invariant 13). An alias above it is refused with `0x94`. What it gives
up is a client with more than sixteen long topics, which is not the case
the feature exists for.

**The alias is the publisher's, and a subscriber always gets the topic
name.** Sagüin sends no Topic Alias to a client, whatever Topic Alias
Maximum that client allows. An alias belongs to one connection, and a
message kept for a client outlives it: re-sent to a resumed session, or
held for want of window while a later message goes ahead, it would carry an
alias the client had never been told. mosquitto sends none either.

## Both protocols, and what each one gets

Sagüin admits MQTT 5 and MQTT 3.1.1, on the same listener, under one
configuration, one set of credentials and one retained store. They are not
kept apart by a bridge or by a second process: **a 3.1.1 device publishes
into the very channels an MQTT 5 client reads.** A Tasmota sensor feeds an
`append` channel or a queue, and the MQTT 5 worker on the other side gets
the whole of it - offsets, ids, acknowledgement, redelivery,
dead-lettering.

That is the shape of an edge fleet. The constrained tier - Tasmota,
Arduino `PubSubClient`, and similar - speaks 3.1.1 and cannot be upgraded;
the gateway, the worker, and the application that acknowledges work are
things you control and can build against MQTT 5. Splitting the two across
two brokers is one more process and one more configuration file on a box
whose whole reason for running Sagüin is that it is one.

**What a 3.1.1 client gets.** It publishes to any topic, including one a
channel claims, and what lands is an ordinary record: the id, the offset
and the timestamp are assigned by the broker on the way in, so nothing a
channel needs is something a 3.1.1 publisher would have had to send. It
subscribes to broadcast topics, to a `latest` channel, and to an `append`
channel - and a persistent session holds a durable position in an `append`
channel exactly as an MQTT 5 session does, resuming after a link outage
from the offset it last acknowledged rather than from whatever survived
in a buffer.

**Where that position starts is the channel's answer and not the
protocol's** - the floor, or the tail where the channel says so - the
same for both protocols, with history a seek away on either (RFC 0003
"Where a subscription starts").

**What it does not get is what 3.1.1 has no words for**, and the list is
short because Sagüin's dependence on MQTT 5 is almost entirely on the
delivery side:

- **Queues.** A delivery carries its Response Topic and Correlation Data
  as MQTT 5 properties, so a 3.1.1 worker would take work it could never
  resolve; the subscription is refused with the failure code 3.1.1 does
  have (RFC 0002).
- **Application acknowledgement and prefetch** - Response Topic,
  Correlation Data and Receive Maximum, none of which exist there.
- **The labels on a delivery.** `saguin-offset`, `saguin-id` and
  `saguin-timestamp` travel as User Properties, and the 3.1.1 PUBLISH
  frame has nowhere to put them. The records are unchanged; what is
  missing is what a delivery says about itself.

**And one thing it gets differently, which is the sharp edge of the
whole feature**: a refusal 3.1.1 cannot be told about closes the
connection rather than acknowledging a record that was thrown away, and
a CONNECT refusal none of its five return codes fits is answered with
the nearest one (RFC 0002 "Publishing").

**Admission is something an operator decides rather than something an
upgrade does.** `broker.mqtt.min_protocol_version` (RFC 0002) sets the
oldest version admitted. It defaults to 3.1.1 - a differentiator behind a
setting is not a differentiator - and an operator who wants the earlier
promise back, where only MQTT 5 could get in the door, writes `5` and has
it. Authentication and the ACL gate a 3.1.1 client exactly as they gate
any other, so this is an admission policy rather than a hole; it is stated
here because a deployment upgrading into this version starts admitting
devices that the version before it turned away.

**One paragraph for a reader who knows 3.1.1 well.** A persistent session
subscribed to a *broadcast* topic gets the ordinary MQTT per-session
queue, on 3.1.1 exactly as on MQTT 5 - what every broker gives. The
channel is what makes this one worth running, and both protocols get
both things.

## What Sagüin is for

Single-node deployments at the edge, on links that are denied, degraded,
intermittent, or bandwidth-limited. One binary, one configuration file,
one node, ARM or x86, no coordinator and no cluster.

**32-bit and 64-bit both, and both build.** A gateway with a 32-bit ARM or
x86 userland is a deployment Sagüin is for rather than one it tolerates, so
nothing in it takes a bound from the platform's integer: a channel offset is
64 bits wide on every target, and a threshold a client can ask about is a
number every build agrees on rather than whatever `int` is here (RFC 0003,
*What a subscriber may declare*). The storage provider is pure Go, so a
build needs no cross-compiler, only `GOARCH`, and a database written on one
width reads on the other. The test suite runs on 64-bit x86.

What 32-bit does bound is **address space, not correctness**. A memory
provider is held in the process, so what it can hold is what the process
can address - which on a 32-bit target is a few gigabytes less whatever the
rest of the broker is using, where a 64-bit one is bounded by the machine.
A sqlite provider is bounded by the disk on either.

## Vocabulary

These terms are used precisely throughout the RFCs. Where a word here
also has an MQTT meaning, the MQTT meaning is the one that governs on the
wire.

**Channel** - a configured region of the MQTT topic space, marked out by a
topic filter, with a type (`append`, `latest`, `queue`) and a policy.
Channels are created and destroyed by configuration, never by clients. A
channel's *name* is identity - one flat topic level, used for its storage,
its seek topic and its metric labels - and is not where its records live.

**Topic** - the complete MQTT topic name, exactly as the client published
it. The topic is the message's routing identity: it is what decides which
channel holds the record, and it is never rewritten. There is no separate
key field. The one topic Sagüin writes rather than accepts is a dead
letter's, which gains a `__dlq` level so that it can be read at all.

**Broadcast** - delivery to currently connected subscribers with no
storage, no position, and no replay. The behaviour of every topic no
channel claims. One thing is kept: a publish asking for a retained
message, which the retained store every broker has holds as a current
value per topic and nothing more (RFC 0002, RFC 0003). What that store
has no room for is refused with `0x97` rather than accepted and dropped.

**Record** - one durable entry in a channel: Message ID, timestamp,
topic, headers, payload, and offset.

**Offset** - a channel-local, monotonically increasing position assigned
by the broker, never reused. An ordering and replay position, not a byte
address.

**Message ID** - a UUIDv7 identifying the application message. Stable
across redelivery, dead-lettering, and replay. Distinct from the MQTT
packet identifier, which is per-hop and 16 bits.

**Retention floor** - the oldest offset a channel can still serve. Stored,
monotonically advancing, and the reason a position that retention has
deleted is reported rather than silently skipped.

**Delivery** - one attempt to hand a queue record to one consumer.
Carries a **Delivery ID** and an attempt number.

**Delivery ID** - an opaque identifier for a single delivery attempt,
distinct from the Message ID and unique to the attempt. It is what makes a
late acknowledgement harmless: one carrying a Delivery ID that is no
longer current changes nothing.

**Visibility timeout** - the interval during which a delivery holds its
record exclusively. Measured from the transport acknowledgement, not from
the send.

**Resolution** - the end of a delivery: acknowledged, returned, expired,
or dead-lettered.

**Dead-letter channel** - the `append` channel `<queue>__dlq`, reserved
and derived automatically for every queue. Never configured directly.

**Consumer** - an MQTT session. A durable consumer is one whose position
outlives its connection, identified by its `client_id`. A bridge is one
too, seen from the broker it reads: it connects, subscribes, and resumes
from a stored position like any other client.

**Bridge** - Sagüin as a client of another broker, in either direction:
an `in` rule subscribes at the far end and brings what arrives into
channels here through the ordinary publish path, and an `out` rule reads
this broker's channels and publishes what it finds at the peer, which
needs no bridge configuration and may be any broker. What crosses is
MQTT: a record's identity travels (`saguin-id`), its offset does not,
and disaster recovery is a copy of the storage (RFC 0004) rather than
anything a bridge does. A bridge carries no shared state, elects
nothing, and handles a broken link by not being connected; it is a
client, not a cluster member. RFC 0002 is authoritative.

## Scope of v0.1

In:

- An MQTT 5 server supporting QoS 0, 1 and 2, wildcard subscriptions,
  shared subscriptions, and persistent sessions. A half-finished QoS 2
  publish is held on a storage provider (RFC 0002 `broker.qos2`), and the
  `CONNACK` omits Maximum QoS, which MQTT reads as 2. A client asking for
  QoS 2 on a subscription is granted 2 and delivered at 2 - except a
  queue's form, which is granted at 1, because a queue's offers are sent
  at QoS 1 (RFC 0002, RFC 0003).

  **Exactly-once is always offered, and the store is what makes the offer
  honest**: MQTT hands the server ownership at the `PUBREC`, so a broker
  offering QoS 2 holds messages nobody has finished sending - kept on the
  default provider unless `broker.qos2` names another (RFC 0002).
- MQTT 3.1.1 clients on the same listener, publishing into any channel and
  reading `append` and `latest` ones with a durable position. A queue is
  the exception, and the only one: competing consumption is shared
  subscriptions and 3.1.1 has none, so it is refused rather than granted
  and left silent. `broker.mqtt.min_protocol_version` is where an operator
  turns them away instead.
- `append`, `latest`, and `queue` channels - with acknowledgement,
  return, visibility timeout, redelivery, attempt counting, and
  dead-lettering on the queue.
- Memory storage, with a versioned binary snapshot across graceful
  shutdown, and SQLite storage for every channel type, which survives a
  process crash whole and a power cut less the last commits - see
  [invariants.md](../invariants.md) 14, which says what each kind of
  storage promises and what it does not.
- Retention by age and by size, with the stored floor advancing over what
  it removed.
- An operations listener: the health endpoint, the metric catalogue in
  Prometheus text format, and the read-only routes behind a credential
  that answer what the broker is running, who may connect, what one user
  is allowed, and what is sitting in a channel (RFC 0005).
- TLS on any listener, mutual TLS taking a client's certificate as its
  name, and password authentication in Mosquitto's own format - one file
  for clients and another for operators (RFC 0002, RFC 0005).
- Grant-only authorization: an `acl_file` names roles whose rules only
  ever allow, clients are given roles by the **user name** they
  authenticated under, matched by pattern - never by client id, which is a
  string the client chose and nothing proves - and without the file every
  authenticated client may do anything (RFC 0002 "What a client may do").

Deferred: SDKs. This list is the record of what v0.1 is rather than of what
happens to be finished, so it moves when the scope moves.

**The acceptance criterion for v0.1 is one sentence:** any stock MQTT 5
client, with no Sagüin-specific software of any kind, can drive a full
queue cycle - publish, consume, acknowledge, time out, redeliver, exhaust
attempts, and read the result out of the dead-letter channel - and can
replay an `append` channel from the beginning after a restart. Until that
demonstration exists, no other work matters.

`mosquitto_pub` and `mosquitto_sub` do everything here except acknowledge:
an acknowledgement is accepted only from the session that received the
job (RFC 0003), and `mosquitto_sub` cannot publish from its session - a
second process is a second session, whose answer is acknowledged and
ignored. That is a limitation of one tool, not of the design.

## Non-goals

Durable exclusions, so that nobody has to guess. Postponed work is not a
non-goal: an entry here is a boundary, not a queue.

**Running on more than one machine.** Sagüin is a single process. No
clustering, no replication, no consensus, no leader election, no
rebalancing, no horizontal scaling. Two Sagüin processes never coordinate.

This is the decision the rest of the design rests on. It is what lets a
queue state transition be one transaction instead of a distributed
protocol, and it is why the specification is small enough to verify. A
deployment that outgrows one machine has outgrown Sagüin.

*Bridging is not clustering.* An MQTT bridge is a client: Sagüin connects
to another broker and subscribes there, publishes there, or both. It
carries no shared state, elects nothing, and handles a broken link by
simply not being connected.

That still holds when the peer is another Sagüin. Neither coordinates,
neither knows whether the other is healthy, and nothing on the wire is
Sagüin's: what crosses a bridge is MQTT, which has no offsets and no
positions.

**There is no replication protocol** - recovery is an operator's own copy
of the storage (RFC 0004), current to the moment it was made, and
**nothing crosses a bridge but records**: no position, no offset, no
queue state. A bridge carries in either direction, asks nothing of the
peer, and is not a replication channel; RFC 0002 has what it does not
carry, and the operator's obligation to keep the graph of them a tree.

**Extending MQTT.** No custom packet types, properties, reserved flags, or
negotiated capabilities that change how packets are parsed - as stated
above, with the sharp edge that every rule is enforced by the broker and
never by an SDK.

**Requiring a Sagüin client.** SDKs make channels, acknowledgement, and
replay pleasant - Python's and JavaScript's exist, over Eclipse Paho and
MQTT.js, and others will follow. They are thin layers over the
established MQTT client for each language - Paho and its equivalents -
not a client stack of Sagüin's own. Building one would mean maintaining
an MQTT implementation per language in order to add sugar to it.
Anything an SDK can do, a generic client can do by sending the same
packets; an SDK that could do more would be a protocol extension.

**A consumer-group protocol.** Competing consumption is MQTT shared
subscriptions and nothing else. No consumer groups, group coordinators,
rebalancing, generation IDs, or membership. A queue has exactly one
canonical subscription form; an application needing two independent work
streams configures two queues.

**Hyperscale.** The target is a single edge node: thousands of
connections, not millions; tens of thousands of messages per second on
memory storage, not millions. Where simplicity and throughput conflict,
simplicity wins - the SQLite write path is a single connection, and framing
is not zero-copy. Each is a deliberate omission that keeps the
implementation readable by one person.

Group commit is on without being asked for, because as Sagüin does it
nothing waits for it. A `sqlite` provider stores the publishes that arrive
while a transaction is committing together in the next one, and holds no
publish back for company (RFC 0002). What an operator may choose with
`publish_commit_interval` is the real trade - a transaction that waits for
more publishes, throughput against how long a publisher waits for its
acknowledgement - and the answer belongs to the deployment rather than to
Sagüin. `none` gives every publish a transaction of its own.

**Processing the data.** No stream processing, transformation, or routing
DSL, and no opinion about payload format. A payload is opaque bytes. A
schema registry is a `latest` channel and a naming convention (RFC 0003
"Saying which schema deserializes a payload"): Sagüin stores a schema as it
stores any value, and never parses, validates or enforces one.

**Delivery features that are somebody else's problem.** Priority queues.
Scheduled or delayed delivery. Distributed exactly-once processing -
Sagüin is at-least-once and says so wherever a user will read it. Ordered
processing across concurrent queue consumers: records are distributed in
order, but completion order is not a guarantee anyone can make once there
is more than one worker and a redelivery path.

**A user interface, or any second way to change something.** No console,
no dashboard, no web interface, and no route that writes: nothing an
operator can reach over HTTP alters this broker. The records themselves
come out the way every other client reads them - through MQTT, under the
same permissions as everybody else - and there is no second door to them.

The operations listener does answer more than "how much", behind a
credential, and RFC 0005 holds the rule that bounds it: nothing that
comes out is a credential, and nothing that goes in changes anything. A
dashboard is still whatever the operator already runs against it.

**A `$SYS` tree.** Sagüin publishes nothing under `$SYS`, and there is no
key to turn one on: it cannot be put behind a credential, and everything
Sagüin tells an operator sits on the operations listener instead, where
one can be - RFC 0005 "There is no `$SYS` tree" has the whole argument.

**Complex authorization.** Topic-filter allow-lists per principal, and
nothing more elaborate. No role hierarchies, delegation, attribute-based
policy, or external policy engine.
