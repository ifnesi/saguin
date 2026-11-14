# Sagüin storage

This document defines every SQLite table Sagüin creates or reads, with its
columns, keys and indexes, and the schema version the database records
([design.md, Store](design.md#store)). Storage limits and setup are described
in [Tuning](#tuning). Sections named here belong to
[design.md](design.md#sagüin-design). Its invariants are
[R1 to R9](design.md#rules).

**The schema version** is SQLite's own `user_version`: 21.
A missing file, or one holding no schema object at version 0, is new and
is created with the schema below ([Logs](#logs)).
A file holding any other version stops startup, naming both versions.
The broker never upgrades it.

A file is accepted when its catalogue matches these tables.
The comparison uses each object's name and statement, with whitespace folded.
Case, quoting and punctuation are significant.
A missing or unexpected object refuses startup.
The drop-channel command changes rows only
([`--drop-channel`](design.md#operations)).
The version stays 21 until the first release that holds it.
After a release, any change to a table's definition is a new schema version.
Neither the broker nor its build reads Markdown files. No test builds
the schema from this document. One test reads
[metrics.md](metrics.md#sagüin-metrics).

When the file is created, the tables, the `$broadcast` row in
`channels` and the version are written in one transaction
([`channels`](#channels)). A file without that row is damaged
([Store](design.md#store)).

**Every table is `STRICT`**, and every column is `NOT NULL` unless its
row says nullable. Times are Unix milliseconds, UTC. An offset is at least 1.
A row is never written by `INSERT OR REPLACE`.
A reused key is a failed write and a store error ([R8](design.md#rules)).
A value that changes is an `UPDATE`. The `CHECK`s repeat rules the
core keeps.

**Ids.** A channel, a session, a bridge, a bridge rule and a shared group
each have an integer id. The other tables use that id in place of the name.
The core assigns ids, as it assigns offsets, so it never waits for the store
to learn one. For each kind, ids start from 1 and assignment resumes at the
highest of that kind loaded at start plus one.
An id is therefore never reused while anything names it,
except after a memory-mode crash ([Store](design.md#store)).
Every session has an id, durable or not.
Only a durable session has a row in [`sessions`](#sessions-1).
The `$broadcast` channel is channel 1.
A column naming an id is at least 1, or 0 for none where its row allows it.

**A message's properties** are a `BLOB` holding its MQTT 5 properties in
their wire form. This is a Property Length followed by the properties,
as `internal/packet` writes them (MQTT 5 2.2.2).
The column is nullable and uses `NULL` for none.
Sagüin's own `saguin-` properties are User Properties among them.

**A message's bytes** are its topic's, payload's and properties' bytes,
the payload counted at its size as received.
Retention and the size limits count those bytes.

**A stored payload** is the payload as received or its S2 block encoding
(`github.com/klauspost/compress/s2`). The choice follows
[Store, Compression](design.md#compression). Every table holding
a payload has `payload_size`, an `INTEGER`: the payload's size as received.
A stored payload shorter than its `payload_size` is compressed; one of
equal length is not. Each such table enforces
`CHECK (length(payload) <= payload_size)`.

**What is stored** is what a restart restores, and no more
([Restart](design.md#restart)). Queue holds, non-durable sessions and
everything in flight to them stay in memory, with the following exceptions:

- An incoming QoS 2 channel publication from any session is held in the store
  before PUBREC ([PUBLISH](design.md#publish)).
- A Will is also stored, including a non-durable session's Will.
  Its lifetime follows [DISCONNECT](design.md#disconnect) and
  [Restart](design.md#restart).
- When an existing session continues with Session Expiry 0, its inherited
  outgoing exchange rows may remain as backing for bounded recovery and scan
  protection on that connection, settled markers included. They confer no
  durability and are never restored after a restart. Progress and departure
  reclaim them. Startup and the full-disk retry reclaim abandoned ones.
  New outgoing exchanges stay in memory.
- A shared group's bookkeeping of its members' assignments is stored,
  including a volatile member's assignments
  ([`share_returned`](#share_returned)).

**In-flight windows** give no protection from retention
([design.md, Words: Retention](design.md#words)).
Connected windows are held by the core.
Stored windows are represented by [`session_inflight`](#session_inflight)
and [`bridge_inflight`](#bridge_inflight).

## Logs

### `channels`

This table holds one row per stored log: configured channels, dead-letter
channels and `$broadcast` alike.

| Column | Type | Meaning |
|---|---|---|
| `id` | `INTEGER` | primary key |
| `name` | `TEXT` | unique: the channel's name, `<channel>__dlq` or `$broadcast` |
| `type` | `INTEGER` | 1 `append`, 2 `latest`, 3 `queue`; a dead-letter channel and `$broadcast` are 1 |
| `next` | `INTEGER` | the offset the next publication entering the channel takes at startup; raised, never lowered, with every saved record and live-start boundary ([design.md, Words: Offset](design.md#words)) |
| `floor` | `INTEGER` | the oldest offset retention has not passed |
| `bytes` | `INTEGER` | the bytes of its records, excluding on `$broadcast` those flagged retained ([Channels](design.md#channels)). It includes a queue's held jobs: the core subtracts them in memory while held, never here |

The table enforces
`CHECK (id >= 1 AND type IN (1, 2, 3) AND next >= 1 AND floor >= 1 AND floor
<= next AND bytes >= 0 AND (id <> 1 OR (name = '$broadcast' AND type = 1)))`.
At start, the store sums the charges of held QoS 2 messages and the bytes
held by `$broadcast`'s retained values from
[`held_publishes`](#held_publishes) and [`records`](#records).

### `records`

This table holds every stored message, of every channel.

| Column | Type | Meaning |
|---|---|---|
| `channel` | `INTEGER` | the channel's id |
| `offset` | `INTEGER` | its offset in the channel |
| `id` | `TEXT` | its `saguin-id` |
| `time` | `INTEGER` | its timestamp, the core's intake time ([PUBLISH](design.md#publish)); for a dead letter, the time it moved ([Channels](design.md#channels)) |
| `topic` | `TEXT` | its topic |
| `qos` | `INTEGER` | the QoS it was published at |
| `publisher` | `TEXT` | the publisher's client id, for No Local; `''` for one that arrived over a bridge; never sent |
| `bridge` | `INTEGER` | the id of the bridge it arrived over, 0 for none: such a record never leaves over a bridge |
| `expires` | `INTEGER` | nullable: when its Message Expiry runs out, counted from its receipt ([PUBLISH](design.md#publish)) |
| `properties` | `BLOB` | nullable: its properties |
| `published_retained` | `INTEGER` | MQTT's RETAIN bit as published, never changed: `$broadcast` only, 0 elsewhere |
| `retained` | `INTEGER` | 1 while it is its topic's current value: on `$broadcast`, its current retained value; on a `latest` channel, every record; 0 elsewhere |
| `payload` | `BLOB` | its stored payload; empty for a `latest` tombstone |
| `payload_size` | `INTEGER` | its payload's size as received |

The primary key is `(channel, offset)`.
The table enforces
`CHECK (channel >= 1 AND offset >= 1 AND qos
IN (0, 1, 2) AND bridge >= 0 AND published_retained IN (0, 1) AND retained IN
(0, 1) AND (channel = 1 OR published_retained = 0) AND
(retained = 0 OR channel <> 1 OR (published_retained = 1 AND length(payload)
> 0)))`.
The unique index `records_retained` is on `(channel, topic) WHERE retained = 1`.
It allows at most one current value per topic, and finds it.

A `latest` channel holds one record per topic. A new value updates the
topic's record in place: its offset, its `saguin-id`, its time, QoS,
publisher, bridge, expiry, properties, payload and payload size. The store
runs the update first and inserts a record only when the update matched
none, when the topic has no current record. Channel, topic and flag stay
as they are ([Channels](design.md#channels)).

### `queue_jobs`

This table holds a queue's unfinished jobs. The record stays in
[`records`](#records). A finished job's row is deleted. When retention ends
a job, one transaction moves its record to the dead-letter channel and
deletes its row.

| Column | Type | Meaning |
|---|---|---|
| `channel`, `offset` | `INTEGER` | the job's record |
| `spent` | `INTEGER` | attempts spent ([Delivery, Queue jobs](design.md#delivery)); [`retry.max_attempts`](config.md#channelsname) counts these |
| `failed` | `INTEGER` | attempts failed ([Delivery, Queue jobs](design.md#delivery)), the n of [`retry.mode`](config.md#channelsname) |
| `first_attempt` | `INTEGER` | nullable: when its first spent attempt began |
| `last_attempt` | `INTEGER` | nullable: when its last spent attempt began |
| `next_try` | `INTEGER` | nullable: when it may be offered again; `NULL` for at once |

The primary key is `(channel, offset)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (channel >= 1 AND
offset >= 1 AND spent >= 0 AND failed >= 0)`.
The index `queue_jobs_retry` is on `(channel, next_try, offset) WHERE
next_try IS NOT NULL`. It finds a queue's jobs whose retry is due, and the
next retry time, without reading the jobs not waiting.

## Sessions

### `sessions`

This table holds one row per durable session ([Words: Session](design.md#words)).
A session is away while its client is gone.
A session left connected by a crash is away from the start that follows,
except in the full-disk case ([Store](design.md#store)).

| Column | Type | Meaning |
|---|---|---|
| `id` | `INTEGER` | primary key |
| `client` | `TEXT` | unique: the client id |
| `user` | `TEXT` | the proved user name, `''` for none |
| `protocol` | `INTEGER` | 4 for MQTT 3.1.1, 5 for MQTT 5 |
| `expiry` | `INTEGER` | the session expiry granted, in seconds |
| `left` | `INTEGER` | when its connection ended; 0 while connected |
| `receive_maximum` | `INTEGER` | the client's Receive Maximum, which its in-flight rows were written under |
| `listener` | `TEXT` | the listener of its last connection ([Operations, `sessions`](design.md#http)) |
| `remote` | `TEXT` | its last connection's remote address |
| `keepalive` | `INTEGER` | its last keep-alive, in seconds |
| `clean_start` | `INTEGER` | its last CONNECT's Clean Start (MQTT 3.1.1: Clean Session), 0 or 1 |

The table enforces
`CHECK (id >= 1 AND protocol IN (4, 5) AND expiry >= 1 AND left >= 0 AND
receive_maximum BETWEEN 1 AND 65535 AND keepalive BETWEEN 0 AND 65535 AND
clean_start IN (0, 1))`.
The index `sessions_expiry` is on `(left + expiry * 1000)
WHERE left > 0 AND expiry <> 4294967295`.
It orders away sessions by the instant their expiry runs out.
An expiry of 4,294,967,295 never runs out, so it is excluded from the index
and from every due-expiry query ([DISCONNECT](design.md#disconnect)).

### `subscriptions`

This table holds a durable session's subscriptions. It has a row per filter,
so a SUBSCRIBE or an UNSUBSCRIBE writes only its own rows.

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | the session's id |
| `filter` | `TEXT` | the topic filter as subscribed, `$share/<group>/` included |
| `qos` | `INTEGER` | the QoS granted |
| `no_local`, `retain_as_published` | `INTEGER` | the options, 0 or 1 |
| `retain_handling` | `INTEGER` | 0 to 2 |
| `identifier` | `INTEGER` | the Subscription Identifier; 0 for none |
| `partitions` | `TEXT` | its `saguin-filter` partitioning ([SUBSCRIBE](design.md#subscribe)), as declared: each declaration as the client sent it, in its order, one per line; `''` for none |

The primary key is `(session, filter)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (session >= 1 AND qos
IN (0, 1, 2) AND no_local IN (0, 1) AND retain_as_published IN (0, 1) AND
retain_handling IN (0, 1, 2) AND identifier BETWEEN 0 AND 268435455)`.

### `subscription_starts`

This table holds a durable subscription's live-start boundary on each
`$broadcast` or `latest` channel it reaches
([SUBSCRIBE: "The start point"](design.md#subscribe)).
It has one row per subscription and channel.
The row is written with its subscription at SUBSCRIBE.
It is deleted with the subscription at UNSUBSCRIBE or when the session ends.
An `append` channel has no such boundary.
Every filter on that channel reads from its consumer's position.

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | the session's id |
| `filter` | `TEXT` | the subscription's filter, as in [`subscriptions`](#subscriptions) |
| `channel` | `INTEGER` | the channel's id |
| `start` | `INTEGER` | the live-start boundary: the channel's next offset when the core took the SUBSCRIBE ([SUBSCRIBE: "The start point"](design.md#subscribe)) |

The primary key is `(session, filter, channel)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (session >=
1 AND channel >= 1 AND start >= 1)`.

### `wills`

This table holds a session's Will, whether the session is durable or not.
The Will is written at its CONNECT ([R4](design.md#rules)).
It is read when it goes.

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | primary key: the session's id |
| `client` | `TEXT` | the publisher's client id, captured when the Will was accepted |
| `user` | `TEXT` | the proved user name captured when the Will was accepted, `''` for none: the `acl_file` is checked against it when the Will fires |
| `topic` | `TEXT` | the Will Topic |
| `qos` | `INTEGER` | its Will QoS |
| `retain` | `INTEGER` | its Will Retain, 0 or 1 |
| `delay` | `INTEGER` | its Will Delay Interval, in seconds; 0 for none |
| `due` | `INTEGER` | nullable: when it is published, once its client is gone; `NULL` while connected |
| `payload` | `BLOB` | the stored Will Payload |
| `payload_size` | `INTEGER` | the Will Payload's size as received |
| `properties` | `BLOB` | nullable: its Will Properties |

The table enforces
`CHECK (session >= 1 AND qos IN (0, 1, 2) AND retain IN (0, 1) AND delay >=
0)`.
The index `wills_due` is on `(due) WHERE due IS NOT NULL`.
It orders the Wills of gone clients by the instant they are due, so the
Will pass reads only those ([DISCONNECT](design.md#disconnect)).
The Wills of connected clients are left out of it. Only the full-store
retry reads them, to find a Will whose publication a full disk refused
([A full disk](design.md#a-full-disk)).

### `positions`

This table holds a durable session's place in each channel it consumes
([Words: Consumer](design.md#words)). A place on an `append` channel or a
queue stays until the session ends, also while no subscription reaches the
channel.

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | the session's id |
| `channel` | `INTEGER` | the channel's id |
| `offset` | `INTEGER` | the next offset for new delivery; unfinished exchanges may precede it |
| `last_seen` | `INTEGER` | nullable: when it last moved; `NULL` for never |

The primary key is `(session, channel)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (session >= 1 AND
channel >= 1 AND offset >= 1)`.

### `session_inflight`

This table holds messages in flight or prepared for a durable session,
by packet identifier ([Delivery](design.md#delivery)). It also holds the
inherited rows used for recovery by a session continuing with Session
Expiry 0 ([What is stored](#sagüin-storage)).
An acknowledgement names that identifier.
Messages are resent in `seq` order ([CONNECT, Resume](design.md#connect)).

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | the session's id |
| `packet_id` | `INTEGER` | the broker's packet identifier |
| `seq` | `INTEGER` | its place in the session's sending order, increasing |
| `channel`, `offset` | `INTEGER` | the record |
| `qos` | `INTEGER` | the QoS it went out at |
| `phase` | `INTEGER` | 1 PUBLISH sent or prepared ([Delivery, Preparation](design.md#delivery)), 2 PUBREC received and PUBREL owed, 3 a settled inherited exchange of a session continuing with Session Expiry 0, kept only so its scan does not deliver the record again ([Delivery, Positions](design.md#delivery)) |
| `moves` | `INTEGER` | 1 while the row protects or may move the current position; 0 after a seek, saved progress or the removal of the last subscription reaching the channel supersedes it. A later subscription does not set it back to 1 ([Delivery](design.md#delivery)) |
| `share` | `INTEGER` | the id of the shared group that handed it over; 0 for none |
| `attempt` | `TEXT` | nullable: a queue job's attempt id, its Correlation Data ([Words: Attempt](design.md#words)), so a resend is the same message |
| `attempt_number` | `INTEGER` | nullable: the job's `saguin-attempt` as sent |
| `attempt_began` | `INTEGER` | nullable: when the attempt was offered, its beginning once spent |
| `out_retain` | `INTEGER` | the first delivery's RETAIN value, 0 or 1 |
| `out_initial` | `INTEGER` | 1 when the first delivery belonged to a subscription's initial pass ([SUBSCRIBE, The start point](design.md#subscribe)): a value sent as a result of a new subscription; else 0 |
| `out_properties` | `BLOB` | nullable: the first delivery's delivery properties, in the [properties format above](#sagüin-storage), excluding Topic Alias and Message Expiry |

The `out_` columns are saved with the exchange before its first transmission
([Delivery](design.md#delivery)).
The primary key is `(session, packet_id)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (session >= 1 AND packet_id BETWEEN 1 AND 65535 AND seq >= 1
AND channel >= 1 AND offset >= 1 AND qos IN (1, 2) AND phase IN (1, 2, 3)
AND (phase <> 2 OR qos = 2) AND moves IN (0, 1) AND share >= 0 AND (attempt IS
NULL) = (attempt_number IS NULL) AND (attempt IS NULL) = (attempt_began IS
NULL) AND (attempt_number IS NULL OR attempt_number >= 1) AND out_retain IN
(0, 1) AND out_initial IN (0, 1))`.
The index `session_inflight_shared` is on `(share, channel, offset) WHERE
share > 0`. It finds a shared group's assignments of a record without
reading every exchange. A query uses it only when it repeats `share > 0`.

### `held_publishes`

This table holds incoming QoS 2 channel publications.
They are stored and charged to their channel before PUBREC, while waiting
for PUBREL ([PUBLISH](design.md#publish)).
They may come from any session or from a bridge's peer ([R4](design.md#rules)).
On PUBREL, the record takes its offset and retained flag, and the held row
is deleted in the same transaction.
A non-durable session's rows are deleted when it ends and at start.

| Column | Type | Meaning |
|---|---|---|
| `session` | `INTEGER` | the publishing session's id, durable or not; 0 for a bridge |
| `bridge` | `INTEGER` | the bridge's id; 0 for a session |
| `packet_id` | `INTEGER` | the sender's packet identifier |
| `channel` | `INTEGER` | the channel it will join |
| `id`, `time`, `topic`, `publisher`, `expires`, `payload`, `payload_size`, `properties` | | as in [`records`](#records) |
| `retain` | `INTEGER` | its RETAIN bit, 0 or 1 |

The primary key is `(session, bridge, packet_id)`.
The table enforces
`CHECK ((session = 0) <> (bridge =
0) AND session >= 0 AND bridge >= 0 AND packet_id BETWEEN 1 AND 65535 AND
channel >= 1 AND retain IN (0, 1))`.
The index `held_publishes_retained` is on `(topic) WHERE channel = 1 AND
retain = 1`. It finds a topic's held retained replacements, which count
toward its retained charge, without reading every held row
([Channels](design.md#channels)).

## Shared groups

### `share_groups`

This table holds each `$share` group for its defined lifetime, and while
an in-flight delivery still names it
([SUBSCRIBE, A shared group](design.md#subscribe)).

| Column | Type | Meaning |
|---|---|---|
| `id` | `INTEGER` | primary key |
| `share` | `TEXT` | unique: the group's `$share/<group>/<filter>` |
| `last_selected` | `TEXT` | nullable: the client id of the member it last selected ([SUBSCRIBE](design.md#subscribe)); `NULL` for no selection yet |

The table enforces
`CHECK (id >= 1 AND substr(share, 1, 7) = '$share/')`.

### `share_cursors`

This table holds a shared group's place in each channel it reaches.
That place is stored while any member is durable.

| Column | Type | Meaning |
|---|---|---|
| `share` | `INTEGER` | the group's id |
| `channel` | `INTEGER` | the channel's id |
| `offset` | `INTEGER` | the next offset the group is owed |

The primary key is `(share, channel)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (share >= 1 AND
channel >= 1 AND offset >= 1)`.

### `share_returned`

This table holds a shared group's bookkeeping for returned messages,
assignments and terminal evidence ([Delivery, Positions](design.md#delivery)).
QoS 1 messages handed back when a member's session ends are owed to the
group again. Assignments to volatile members have their exchanges in
memory. A durable member's assignment stays in
[`session_inflight`](#session_inflight).

Terminal evidence records messages settled, refused or dropped ahead of
the cursor. Terminal rows survive their member, carry no packet ownership,
and are reclaimed as the cursor advances or with the group. Retention and
loss counting apply as to any record.

| Column | Type | Meaning |
|---|---|---|
| `share`, `channel`, `offset` | `INTEGER` | the group's id and the record |
| `state` | `INTEGER` | 1 returned, 2 assigned to a volatile member, 3 terminal |
| `session` | `INTEGER` | nullable: the assigned member's session id; `NULL` unless assigned |
| `qos` | `INTEGER` | nullable: the assigned delivery's QoS; `NULL` unless assigned |

The primary key is `(share, channel, offset)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (share >= 1 AND channel >= 1 AND offset >= 1 AND state IN (1, 2, 3)
AND (state = 2) = (session IS NOT NULL) AND (state = 2) = (qos IS NOT
NULL) AND (qos IS NULL OR qos IN (1, 2)))`.

## Bridges

### `bridges`

This table holds each configured bridge and each removed bridge still named
by a record, rule or held publish.
A removed bridge's exchanges, held publishes and cursors are deleted at
start, in one transaction.
Its row is deleted only when nothing names it any more.

| Column | Type | Meaning |
|---|---|---|
| `id` | `INTEGER` | primary key |
| `name` | `TEXT` | unique: the bridge's name |
| `replay_owed` | `INTEGER` | 1 from a CONNACK with Session Present 0 until its replay commits ([Bridges](design.md#bridges)), else 0 |

The table enforces
`CHECK (id >= 1 AND replay_owed IN (0, 1))`.

### `bridge_rules`

This table holds each rule that sends to the peer, either an `out` rule
or the outbound half of a `both` rule ([Bridges](design.md#bridges)).
For a `both` rule, the `topic` is `''` and the topic is unchanged.
A removed rule's cursors are deleted at start ([Restart](design.md#restart)).
Its row stays while an in-flight row names it.

| Column | Type | Meaning |
|---|---|---|
| `id` | `INTEGER` | primary key |
| `bridge` | `INTEGER` | the bridge's id |
| `filter`, `topic` | `TEXT` | the rule; `topic` is its template, `''` for a `both` rule |

The table enforces
`CHECK (id >= 1 AND bridge >= 1)`.
The tuple `(bridge, filter, topic)` is unique.

### `bridge_cursors`

This table holds an `out` rule's place in each channel its filter reaches.
The rule is a durable consumer.

| Column | Type | Meaning |
|---|---|---|
| `rule` | `INTEGER` | the rule's id |
| `channel` | `INTEGER` | the channel's id |
| `offset` | `INTEGER` | the next offset the rule is owed |
| `last_seen` | `INTEGER` | nullable: when it last moved; `NULL` for never |

The primary key is `(rule, channel)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (rule >= 1 AND channel
>= 1 AND offset >= 1)`.

### `bridge_inflight`

This table holds records in flight to a bridge's peer, by packet identifier.
They are resent in `seq` order.

| Column | Type | Meaning |
|---|---|---|
| `bridge` | `INTEGER` | the bridge's id |
| `packet_id` | `INTEGER` | Sagüin's packet identifier at the peer |
| `seq`, `channel`, `offset`, `qos` | `INTEGER` | as in [`session_inflight`](#session_inflight) |
| `phase` | `INTEGER` | 1 PUBLISH sent or prepared, 2 PUBREC received and PUBREL owed |
| `rule` | `INTEGER` | the id of the rule whose cursor its acknowledgement moves |

The primary key is `(bridge, packet_id)`.
The table is `WITHOUT ROWID`.
The table enforces
`CHECK (bridge >= 1 AND
packet_id BETWEEN 1 AND 65535 AND seq >= 1 AND rule >= 1 AND channel >= 1 AND
offset >= 1 AND qos IN (1, 2) AND phase IN (1, 2) AND (phase = 1 OR qos =
2))`.

## Tuning

### What limits Sagüin

One store writer does every commit's work, so most limits are its own.
The measurements below were made on an AMD Ryzen 7 260 machine (x86-64,
NVMe, ext4) and a Raspberry Pi 4 Model B with 4 GB (aarch64, USB SSD).

| Workload | What limits it | Measured |
|---|---|---|
| Small payloads (to about 1 KiB), many publishers | nothing reached: kept up | Ryzen, 256 B and 1 KiB at 12,250-12,400 publications/s to 100 subscribers: kept up, delivery p99 under 6 ms |
| Few publishers at QoS 1 | each publisher's QoS 1 window: with Receive Maximum publications awaiting PUBACK it stops until one arrives, so its rate is at most Receive Maximum / PUBACK time | Ryzen, 256 B, 10 publishers at 13,000/s: 99.4% sent with PUBACK p99 2.4 ms; 92.6% when PUBACK p99 was 22.5 ms |
| Large payloads (10 KiB and up) | the store writer's CPU: each payload byte is written to the log and again to the database, about three times the bytes accepted | Ryzen, 10 KiB at 8,900/s to 100 subscribers (89,000 deliveries/s): kept up while the drive is in its fast phase; 32,000-64,000 deliveries/s in its slow phase |
| Payloads of 64 KiB to 1 MiB | the drive's write bandwidth, for about three times the payload bytes | Ryzen, mixed payloads at 6,400/s: 66% sent, about 180 MB/s of payload to store |
| A Raspberry Pi 4 Model B | the store writer's CPU on a slow core (87% busy), not the drive | 10 KiB at 1,000/s kept up, 1,500/s did not; the load tool shared the same four cores (24% idle) |

### Choosing a machine

One store writer does every commit's work ([What limits Sagüin](#what-limits-sagüin)),
so the speed of a single core matters most: a core of a recent
generation, with a high boost clock and a large cache. Clock speed alone
is a poor guide across generations. On the machines measured, a recent
laptop core boosting to about 5 GHz delivered about six times as much as
a ten-year-older desktop core at about 4 GHz, and about ten times as much
as a low-power board's core at under 2 GHz. Four to eight cores are
enough: the broker used about two and a half of four cores at full load,
and further cores add little. Prefer fewer fast cores to many slow ones.
Server-class parts with ECC memory suit a broker that keeps acknowledged
data.

The broker's own memory stayed between 40 and 230 MB in every run
measured. Most of the rest is the operating system's cache of the
database file, so 4 to 8 GB suit `full` and `normal` mode. `memory` mode
holds the whole database in `memory_dir`, and needs room for it there and
on disk for a second copy while a snapshot is written
([design.md, Store](design.md#store)).

In `full` mode, the drive comes before the CPU
([Drive and filesystem](#drive-and-filesystem)).

### Drive and filesystem

- **Choosing a drive:** In `full` mode, Sagüin makes small synced writes,
  one `fdatasync` per commit, all day. That sync's latency is a QoS 1
  publisher's acknowledgement time. In `normal` mode, the core's timer
  requests the sync. In `memory` mode, each commit syncs on the RAM
  filesystem. The sync and snapshot rules are defined in
  [Store](design.md#store). What matters for the drive, in order:
  1. **power-loss protection** (a capacitor-backed write cache, found on
     datacenter drives), which lets a sync return safely once data is in
     the drive's cache;
  2. **steady latency after bursts:** a drive with its own DRAM cache, not
     a DRAM-less or QLC drive, whose sync times swing after heavy writing,
     as the Ryzen's did;
  3. **endurance:** retention deletes as much as it writes, so endurance
     matters less than for most databases.

  Peak throughput figures matter least.
- **Synced-write latency** in `full` mode sets how soon a QoS 1 publication
  is acknowledged, and therefore a QoS 1 publisher's rate
  ([What limits Sagüin](#what-limits-sagüin)).
  A 64 KiB write followed by `fdatasync` took 0.16 ms on the Ryzen's NVMe
  and 3.7-4.0 ms on a SATA SSD.
  This measures the disk sync used for each commit in `full` mode.
  Other modes follow [Store](design.md#store).
- **A drive's slow phase:** the Ryzen's NVMe alternates between a fast and a
  slow phase. Its own write latency rises from about 0.45 ms to 4.3 ms per
  write.
  Sagüin's rates follow it ([What limits Sagüin](#what-limits-sagüin)).
- **Btrfs:** copy-on-write roughly halves synced writes.
  On a SATA SSD rewriting a 16 MiB region, rates were 9-15 MB/s with it off
  and 5-7 MB/s with it on. On Btrfs, the database's directory has
  copy-on-write off (`chattr +C`), set before the database is created.
- **A growing file** is slower on some drives.
  While growing a new file, the Ryzen's NVMe wrote 130 MB/s for 10 s, then
  9-50 MB/s. It wrote 350 MB/s into a file written beforehand.
  Pre-growing the database does not help. SQLite reuses the pages it frees,
  and a pre-grown database made commits slower at p90.

### Settings

- **`publish_commit_max_records`:** keep the default
  ([config.md, `broker.storage`](config.md#brokerstorage)).
  On the Ryzen, 1,024 nearly doubled the throughput of a few publishers, from
  45,000 to 83,000 deliveries/s. With many publishers, it cost 11%, from
  164,000 to 146,000 deliveries/s. It nearly tripled QoS 1 p99, from
  24 to 65-73 ms. A site with few, steady QoS 1 publishers and room in its
  latency budget may raise it.
- **`qos2.max_inflight_per_client`** also bounds a QoS 1 publisher
  ([config.md](config.md#brokerbroadcast-brokerretained-brokerqos2)).
  The window's effect on the publisher's rate is described in
  [What limits Sagüin](#what-limits-sagüin).
  A gateway publishing at QoS 1 needs Receive Maximum to be
  at least its publication rate x PUBACK p99.
  *Example: 1,300/s with PUBACK p99 at 22.5 ms needs 30, above the
  configured default ([config.md](config.md#brokerbroadcast-brokerretained-brokerqos2)).*

### Capacity

The store writer spends its time on two budgets.
What one load takes, the other loses:

- **Stored payload:** every accepted byte is written about three times.
- **Delivery bookkeeping:** each QoS 1 delivery to a durable session causes
  two to three row changes. Its exchange is inserted and later deleted,
  and it accounts for a share of position and supersede updates.
  A QoS 0 delivery adds no row of its own.

**Disk space:** space that retention or any other deletion frees stays
inside the database file, and SQLite reuses it for later writes. The file
grows to its busiest size and stays there; it never shrinks, not even
after a one-off backlog is cleared. Sagüin never compacts it. Size the disk
for the peak. In memory mode the same holds for the RAM copy in
`memory_dir`, and each snapshot writes a copy of it at its full size.

The following measurements are single runs on the Ryzen and the Raspberry Pi.
They used 10 KiB payloads, with half the publishers at QoS 1 and every
subscriber durable at QoS 1.

| Machine, fan-out, rate | Payload stored | Deliveries/s | Kept up |
|---|---|---|---|
| Ryzen, 10, 8,900/s | 91 MB/s | 89,000 | yes, in the drive's fast phase; 32,000-64,000 deliveries/s in its slow phase |
| Ryzen, 20, 4,686/s | 43 MB/s (89% sent) | 74,583 | no |
| Ryzen, 3, 21,553/s | 137 MB/s (62% sent) | 7,801 | no |
| Ryzen, 1, 37,824/s | 147 MB/s (38% sent) | 2,838 | no |
| Raspberry Pi, 10, 1,000/s | 10 MB/s | 10,000 | yes; 1,500/s did not; the load tool shared its four cores |

At fan-out 10 on the Ryzen, bookkeeping took about 32% of the writer's time
before its statements were grouped and about 28% after.
In these runs, about 140-150 MB/s of stored payload filled the writer.
That is a lower bound because the drive was partly in its slow phase.
