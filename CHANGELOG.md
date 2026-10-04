# Sagüin - Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com), and
this project adheres to [Semantic Versioning](https://semver.org):
MAJOR.MINOR.PATCH.

- **MAJOR** - incompatible or breaking changes
- **MINOR** - new, backward-compatible functionality
- **PATCH** - backward-compatible bug fixes

The newest release is listed first. Dates are ISO 8601 (YYYY-MM-DD).
Categories: Added, Changed, Deprecated, Removed, Fixed, Security.

**A section here means a tag on GitHub.** Nothing is recorded before it is
released, so what is in flight lives in `git log` until it ships under a
version.

## 0.1.0-rc.1 - 2026-10-04

The first release. Sagüin is a single-node MQTT broker for edge and
small-scale IoT and IIoT deployments. It keeps standard MQTT as the client
protocol and adds three durable channel types behind configured topic
filters.

### Added

**Channels and delivery**

- Three channel types, set in configuration. An `append` channel keeps a
  replayable log and a durable position per consumer. A `latest` channel
  keeps one current value per topic. A `queue` channel offers each record to
  one worker at a time, with application acknowledgement, return, visibility
  timeout, retry limits, backoff and dead-lettering. A topic no channel
  claims stays ordinary MQTT broadcast, with retained messages.
- Delivery is at-least-once. Queue work can run more than once, so workers
  must be idempotent. QoS 2 stops a retransmitted publish from creating a
  second record; it does not make processing exactly-once, and the broker
  does not deduplicate separate publishes that carry the same message ID.
- `append` records reach a consumer in offset order. When retention removes
  history a consumer has not read, the broker says so rather than serving the
  oldest surviving record as if nothing were missing.
- Queue attempt counts and stored records survive a restart. No delivery is
  restored as in flight: leases and deadlines start again from the available
  state.
- MQTT 5 and MQTT 3.1.1 clients share the same listeners, credentials,
  retained store and channels. The lowest admitted version is configurable
  and defaults to 3.1.1. Both can publish to any channel and read broadcast,
  `append` and `latest` topics. Queue workers need MQTT 5, because queue
  delivery and acknowledgement use properties and shared subscriptions that
  3.1.1 does not have. A queue is offered to its workers at QoS 1.

**Storage**

- Named memory and SQLite providers, shared by several channels, with
  explicit retention and capacity. A memory provider writes a snapshot at a
  graceful shutdown. A SQLite provider uses WAL mode with
  `synchronous=NORMAL`, survives a process crash, an OOM kill or `kill -9`,
  and keeps its data in a file ordinary SQLite tools can open.
- `flush_interval` sets how often a SQLite provider forces its write-ahead
  log to disk: 150 ms by default, from 10 ms to 1 s, and there is no way to
  turn it off. A power cut can lose the acknowledged publishes committed in
  about the last interval plus one fsync, never a gap below a record that
  survives. Measured on a Raspberry Pi 4 with a USB SSD, two power cuts at
  the default lost the last 52 ms and 138 ms of acknowledged records, with no
  gaps and no duplicates. A failed flush is counted in
  `saguin_storage_errors_total` and logged, never silent.
- Group commit on SQLite providers. Publishes that arrive while a
  transaction commits are stored together in the next one, and a client's
  session writes (connect, subscribe, acknowledgements, positions) are
  grouped the same way, with a waiting publish going first. Every
  transaction on a provider's write connection begins with `BEGIN IMMEDIATE`.
- Memory-backed records survive only a successful graceful-shutdown
  snapshot, not a crash or a power cut.

**What a client is told**

- An acknowledgement, a CONNACK's Session Present and the close that answers
  a DISCONNECT are sent only after the state they describe is stored. A
  SUBACK and an UNSUBACK say only what the session store holds. A refused
  SUBSCRIBE or UNSUBSCRIBE is refused once, so the answer and the store
  agree. What the store refused after a client was told it was stored is
  written again until it lands.
- One stated exception: a connected session's acknowledgements of a
  consumer position are written within `broker.session.ack_commit_interval`
  (200 ms by default), so an unclean stop can send again what was
  acknowledged in that window. That is a replay, never a skip.

**Connections and packets**

- `limits.max_connections` counts a connection from the moment its socket
  arrives, before CONNECT. Beyond the limit there is one small overflow
  budget (the smaller of `max_connections` and 32): a socket waits up to
  50 ms for a slot, the longest-waiting first, and one that gets none is
  answered with "Server busy" if it sent a CONNECT, or closed. Every socket
  not admitted is closed within 100 ms of arriving. A connection gives its
  slot back when the broker decides it ends, so a client that was turned away
  or disconnected can reconnect at once.
- A packet that MQTT 5 or MQTT 3.1.1 calls malformed is refused where it is
  read. The broker checks every property against the MQTT 5 property table
  (which packets may carry it, whether it may repeat, which values it may
  take), and refuses a packet that breaks the specifications' format rules,
  among them a Will QoS with no Will, a variable byte integer longer than four
  bytes, trailing bytes after a fixed-length packet, and an AUTH packet on
  3.1.1.
- Empty string and binary properties (Content Type, Response Topic,
  Correlation Data and the like) reach subscribers as empty properties and
  survive storage. A short-form MQTT 5 DISCONNECT carries its reason code, so
  a client disconnecting with 0x04 still has its Will published.
- A small heap has an 8 MB floor, so the garbage collector does not run
  dozens of times a second with a few hundred connections. On Linux the
  floor's pages are never resident, including where the kernel backs the
  surrounding memory with huge pages.

**Security and doors**

- Password authentication (`$7$` PBKDF2-HMAC-SHA512, and `$6$` files read as
  they are), mutual TLS, an ACL file with roles, and per-client publish
  limits. Credential files, TLS certificates, WebSocket origins and the log
  level are re-read on `SIGUSR1`; other configuration changes need a restart.
- MQTT listens on TCP, WebSocket and Unix-socket doors, several of each if
  named, with door-specific credentials and TLS. The operations listener
  takes TCP and Unix-socket doors. A trusted proxy can pass a client's
  identity (PROXY protocol v2 on an MQTT Unix socket; a header set by the
  proxy on the operations Unix socket).
- `saguin --check-config` and a plain start agree on door names, socket paths
  and addresses, and refuse a clash by name.

**Bridges**

- Inbound, outbound and bidirectional MQTT bridges carry selected records to
  or from another broker and resume across link outages. A bridge carries
  records, not channel offsets, consumer positions or queue state.

**Operations**

- An HTTP operations listener with `/health`, Prometheus metrics, and
  authenticated read-only routes for the resolved configuration, one user's
  permissions, consumers, queues, lost positions, refused clients, sessions
  and users. The metric `saguin_channel_floor_offset` against
  `saguin_channel_consumer_position_min` shows when retention has passed a
  consumer.
- `saguin --version`, `--licenses` (the licences of everything inside the
  binary), `--check-config`, and the `--passwd` subcommands.

**Distribution**

- Binaries for Linux (amd64, arm64, armv7) and macOS (amd64, arm64), with
  checksums, and a multi-arch container image (amd64, arm64) at
  `ghcr.io/ifnesi/saguin`. There is no Windows build: on Windows, run the
  container image, or the Linux binary under WSL2.

### Known limits

- One process on one node: no clustering, replication, consensus or
  automatic failover. A bridge is transport, not replication, and queue
  state cannot be copied by one; recovery uses a copy of the SQLite
  database.
- Sized for thousands of connections, not millions. A SQLite provider has one
  write connection.
- No exactly-once processing, priority or scheduled delivery, schema
  enforcement, multi-tenancy layer, or web interface inside the broker.
- A broadcast publish does not prove that any subscriber received it. A
  session's queue is bounded and may discard its oldest owed deliveries when
  full, which the metrics count.
- Certificate revocation is not implemented in Sagüin.
- The project is early. The RFCs in `docs/rfcs` are the source of truth for
  detailed behaviour and refusal codes.

### Known issues

A code review is under way before 0.1.0. These findings are known and will be
fixed or documented before then.

- A channel message delivered after a reconnect, behind messages being
  re-sent, can carry a Message Expiry Interval longer than the time actually
  left (MQTT 5 section 3.3.2-6). No data is lost.
- A connection that arrives while the broker is shutting down can have its
  socket closed twice. This is harmless and is listed for completeness.
- A rare connection refusal just after startup was seen once in about 800
  test runs, after the health endpoint had answered. It is not yet diagnosed.
  Clients that retry are unaffected.
- RFC 0003 does not yet say explicitly that broadcast order is not promised
  across different publishers or QoS levels. MQTT gives no such promise
  either.
- On macOS, `make check` has three test failures that are limits of the
  tests, not broker defects: one admission test times a refused connection
  from the client's dial rather than the broker's accept, and the
  broker-comparison harness reads Linux's `/proc`. The broker itself behaves
  correctly on macOS.
