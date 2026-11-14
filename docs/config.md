# Sagüin configuration

This document defines every key of Sagüin's configuration file, with its
default and meaning. Sections named here belong to
[design.md](design.md#sagüin-design). Its invariants are
[R1 to R9](design.md#rules).

The configuration is one YAML file holding one document. The loader reads it
strictly. An unknown or repeated key stops startup. Every key is a plain
string. The loader refuses YAML merge keys (`<<`). Anchors and aliases are
allowed, but the loader refuses a circular one.

The `channels:` section is one mapping of channels, or a list whose entries
are each a group of channels. A group is written in place or as
`!include <file>`. An included file holds one group and no `!include`.
Its path names one regular file, never a device, pipe or socket.
The path contains no `*`, `?` or `[`.
Neither `channels:`, a group nor an included file is empty.
With no channels, `channels:` is left out.

**Values.** A duration is a whole number above 0 followed by one unit,
`s`, `m`, `h` or `d` (24 hours).
Only `write_timeout`, `publish_commit_interval`,
`delivery_admission_delay`, `delivery_peer_timeout`, `sync_interval`, a
queue's `retry.delay` and a bridge's `ack_interval` also take `ms`.
Every other duration is at least 1s. A size is a whole number of bytes
above 0, optionally followed by `KiB`, `MiB` or `GiB`.
The value `none` is accepted where a row says so or where it is the default.
The loader refuses a key written empty or `~`.
Leave it out for its default. A boolean is `true` or `false`.
No number exceeds 9,223,372,036,854,775,807 bytes, items or nanoseconds
(about 106,751 days). A count is at least 1 unless its row says otherwise.

Every file path is absolute, except a Unix socket's.
A path Sagüin writes must differ from every other path in the configuration.
These paths are compared as files at start. They include `pid_file`,
`file_path` with its `-wal`, `-shm` and `-journal` files and its
lock file, `memory_dir`, memory mode's RAM copy with its `-wal`, `-shm`,
`-journal` and lock files, its `file_path.new` snapshot file, and a Unix
socket and its lock file.

## `broker`

| Key | Default | Meaning |
|---|---|---|
| `id` | required | the broker's name in logs and metrics: 1 to 64 of `A`–`Z`, `a`–`z`, `0`–`9`, `-` and `_` |
| `log_level` | `info` | `debug`, `info`, `warn` or `error`; output follows [Logging](design.md#logging) |
| `pid_file` | none | the file the process writes its id to |
| `delivery_admission_delay` | `100ms` | the delay threshold for the [delivery admission gate](design.md#capacities); `none` disables the pause |
| `delivery_peer_timeout` | `1s` | the timeout for [Delivery, Peer obligations](design.md#delivery), which determines whether a consumer's deliveries contribute to the admission delay |

## `broker.mqtt`

| Key | Default | Meaning |
|---|---|---|
| `listen` | one `tcp` listener on `:1883` | the MQTT listeners, by kind (`tcp`, `ws`, `unix`), each kind one listener or a list of at least one; writing any replaces the default |
| `password_file` | none | one `user:hash` line per user: a line empty, of spaces alone, or starting with `#` after spaces is ignored; otherwise the user name is everything before the first colon and the hash follows it, a second colon starting a third field; a user name is 1 to 65,535 bytes of valid UTF-8 with no control character, holds no `:`, does not start with `#` and has no leading or trailing space; the hash is `$7$<iterations>$<salt>$<hash>`, PBKDF2-HMAC-SHA512 with a 64-byte output, or `$6$<salt>$<hash>`, SHA-512 of the password then the salt; salt and hash in standard base64, their lengths unchecked; a `$7$` iteration count is a positive decimal; any other scheme, a line breaking this form (a carriage return or a user name outside those rules included), or a user named on two lines, is refused, naming the file and the lines; an unknown user name costs the same check as a `$7$` entry of 1,000 iterations, so keep one scheme and iteration count in a file: mixing them can let login timing reveal whether a user name exists |
| `allow_anonymous` | true without `password_file`, false with one | admits a client with no user name; true, on the broker or any listener, is refused beside an `acl_file` |
| `min_protocol_version` | `3.1.1` | `3.1.1` or `5`: the oldest MQTT version admitted |
| `acl_file` | none: everything allowed | [Access and reload](design.md#access-and-reload); needs a listener with an identity source |

## A listener

An *identity source* is a password file, a client CA or the PROXY protocol.

| Key | Default | Meaning |
|---|---|---|
| `name` | its kind | 1 to 64 of `A`–`Z`, `a`–`z`, `0`–`9`, `-` and `_`; required once a kind has two listeners; unique among MQTT listeners |
| `address` (`tcp`, `ws`) | required | `host:port`, the port in decimal, IPv6 in brackets; the host a name as in [`allowed_origins`](#a-listener), an IPv4 or IPv6 address, never a multicast one, with a zone (`%eth0`) only on a link-local IPv6 address and never empty, or empty for every address; no two TCP listeners, operations included, on one port with equal or wildcard hosts, port 0 aside |
| `path` (`unix`) | required | the socket file, at most 107 bytes, unique, held by a `<path>.lock` file |
| `mode` (`unix`) | `"0660"` | the socket's permissions, in octal: at most `0777`, the owner's read and write bits set |
| `proxy_protocol` (`unix`) | false | reads a PROXY protocol v2 header ([Flows, Transport setup](design.md#transport-setup)) |
| `password_file` | [the broker's](#brokermqtt) | the listener's own file |
| `allow_anonymous` | [the broker's](#brokermqtt); false with the listener's own `password_file` | false is refused on a listener with no identity source |
| `tls` (`tcp`, `ws`) | none | `cert_file` and `key_file`, written together; `min_version`, `1.2` (default) or `1.3`; `client_ca_file`, which asks for client certificates; `require_certificate`, true by default with a CA and refused without one. When `require_certificate` is false, a client may present no certificate, but a certificate it does present must verify, or the handshake fails. A certificate and key that do not load as a pair, or a client CA file that holds no certificate, stop the start, which names the listener, the file and the error |
| `same_origin` (`ws`) | true | a page's origin must equal the request's scheme, host and port |
| `allowed_origins` (`ws`) | none: no additional allowed origins | the origins allowed, each `http://` or `https://` with a host (a name of dot-separated labels of 1 to 63 letters, digits, `-` and `_`, none starting or ending with `-`, the last not all digits; an IPv4 address or an IPv6 address in brackets) and optional port (1 to 65,535, without a leading zero) |

WebSocket and PROXY protocol handling follow [design.md, Flows, Transport
setup](design.md#transport-setup).

## `broker.operations`

If this section is absent, Sagüin starts no operations listener.

| Key | Default | Meaning |
|---|---|---|
| `listen` | required | `tcp` and `unix` listeners with `name`, `address` or `path`, `mode`, `proxy_protocol`, `tls` as [above](#a-listener) and `password_file` (default: the [operators' file below](#brokeroperations)), never `allow_anonymous`; their names are their own; a non-loopback address (a host name included, `localhost` too) needs a password file or a required client certificate |
| `password_file` | none | the operators, in [the clients' format](#brokermqtt); a different file from the clients' |
| `min_scrape_interval` | `60s` | how often the store-held metric values are re-read ([metrics.md](metrics.md#rules)); at least 60s |

## `broker.storage`

`sync_interval` is for `normal` mode only. `snapshot_interval` and
`memory_dir` are for `memory` mode only. When one is written in another
mode, startup and [`--check-config`](design.md#operations) refuse it,
naming the key and the mode. When left out, each keeps its default.

| Key | Default | Meaning |
|---|---|---|
| `file_path` | required | the database on disk ([Store](design.md#store)); on Btrfs, see [storage.md, Drive and filesystem](storage.md#drive-and-filesystem) |
| `mode` | `full` | `full`, `normal` or `memory` ([Store](design.md#store)) |
| `default_retention_period`, `default_retention_bytes` | required | the retention of every log that omits its own, due as [Words: Retention](design.md#words) says; `none` allowed |
| `publish_commit_interval` | none | the core's collection period, at most `1s`; [Store, Writes](design.md#writes) defines grouping and deadlines |
| `publish_commit_max_records` | the smaller of 256 and [`limits.max_connections`](#brokerlimits) | the record limit per publication batch, 1 to [`limits.max_connections`](#brokerlimits), with or without an interval; [Store, Writes](design.md#writes) defines grouping; tuning: [storage.md, Settings](storage.md#settings) |
| `read_connections` | 2 | read-only connections beside the writer, 2 to 8; request handling follows [Store](design.md#store) |
| `sync_interval` | `1s` | `normal` mode: how often committed changes are synced to the disk, at least 100ms ([Store](design.md#store)) |
| `memory_dir` | `/dev/shm/saguin` | memory mode's RAM directory ([Store](design.md#store)) |
| `snapshot_interval` | `30m` | memory mode's snapshot period, at least 1m |

## `broker.broadcast`, `broker.retained`, `broker.qos2`

| Key | Default | Meaning |
|---|---|---|
| `broadcast.retention_period`, `broadcast.retention_bytes` | [the storage defaults](#brokerstorage) | `$broadcast`'s retention, due as [Words: Retention](design.md#words) says; `none` allowed |
| `retained.retention_period` | none | a retained value ends this long after its publish, due as [Words: Retention](design.md#words) says |
| `retained.max_bytes` | none | the retained values' own limit ([Channels](design.md#channels)) |
| `qos2.max_inflight_per_client` | 20 | the Receive Maximum sent in CONNACK; 1 to 65,535; tuning: [storage.md, Settings](storage.md#settings) |

## `broker.limits`

| Key | Default | Meaning |
|---|---|---|
| `max_packet_size` | `1MiB` | the largest packet, sent as Maximum Packet Size; at least 20 bytes, at most 268,435,460, the largest MQTT packet |
| `max_topic_length` | 1024 | the longest topic or filter, in bytes; 1 to 65,535 |
| `max_client_id_length` | 256 | the longest client id, in bytes; 23 to 65,535 |
| `max_header_count` | 32 | User Properties per publish |
| `max_connections` | 10000 | connections served at once; permit handling follows [CONNECT, Connection permits](design.md#connect) |
| `max_subscriptions` | 10000 | This bounds the filters per session. Refusals follow [SUBSCRIBE](design.md#subscribe). |
| `max_topic_levels` | 200 | levels in any topic or filter |
| `max_session_expiry` | `30d` | caps session expiry, and is MQTT 3.1.1 Clean Session 0's; at most 4,294,967,295 seconds |
| `max_keepalive` | none | caps an MQTT 5 client's keep-alive, at most 65,535 seconds |
| `publish_rate`, `publish_bytes` | none | messages and bytes per second per connection; throttling, per-user overrides and accounting follow [PUBLISH, Rate limits](design.md#publish) |
| `write_timeout` | `5s` | a write's deadline and a close's ([Closing a connection](design.md#closing-a-connection)); the silence that ends a PUBLISH body ([Message memory](design.md#message-memory)); the graceful stop's cutoff and the logger's last wait ([Restart](design.md#restart)); never `none` |
| `connect_timeout` | `10s` | how long a new connection has to send CONNECT |
| `max_connect_size` | the smaller of `100KiB` and `max_packet_size` | the largest CONNECT, read before authentication; at most `max_packet_size` |
| `max_connect_rate` | 500 | connections each MQTT listener accepts per second, in bursts of up to twice that; one over the rate waits in the accept queue; `none` allowed |
| `max_inflight` | 20 | [Delivery](design.md#delivery) |
| `max_incoming_memory` | `32MiB` | the incoming budget. It must hold the largest body one reader can ask for: `max_packet_size`, plus the largest S2 copy of it where `max_packet_size` is over 4 KiB, plus `max_topic_length` where a bridge has an `in` rule. A smaller value is refused, and the refusal names the value to set. Compression follows [design.md, Compression](design.md#compression). Admission, accounting and sizing are defined in [design.md, Message memory](design.md#message-memory) |
| `max_outgoing_memory` | `224MiB` | the outgoing budget. A delivery larger than it is discarded for that subscriber and counted as missed. Admission, accounting and sizing are defined in [design.md, Message memory](design.md#message-memory) |

## `channels.<name>`

A channel name contains 1 to 128 of `A`–`Z`, `a`–`z`, `0`–`9`,
`.`, `_` and `-`. It is not `.` or `..` and does not end in
`__dlq`. The name is unique. A key on a type it does not belong to is
refused.

| Key | Default | Meaning |
|---|---|---|
| `type` | required | `append`, `latest` or `queue` |
| `filter` | `<name>/#` | the topics it holds, never `none`: the first level written out (alone or as `{a,b}`), not empty and not starting with `$`, no `__dlq` level; a level holding `{` or `}` is exactly one `{a,b}` (not on a queue) offering alternatives: at least two, each a non-empty written level with no `{`, `}` or `,`, none repeated; `{a,b}` expanded, each filter within [`limits.max_topic_length`](#brokerlimits) and [`limits.max_topic_levels`](#brokerlimits), and no two channels with one filter |
| `start` (`append`, `queue`) | `floor` | `floor` or `tail`: where a reader with no position begins |
| `retention_period` | [the storage default](#brokerstorage) | `append`: removes old records; `latest`: retention of nonempty current values ([Channels](design.md#channels)); `queue`: dead-letters unfinished jobs as `expired`; due as [Words: Retention](design.md#words) says; `none` allowed |
| `retention_bytes` (`append`, `queue`) | [the storage default](#brokerstorage) | `append`: removes the oldest beyond it; `queue`: dead-letters as `evicted`; `none` allowed |
| `max_bytes` (`append`) | none | This is the channel's hard byte limit. Accounting follows [Channels](design.md#channels). Refusals follow [PUBLISH](design.md#publish). |
| `tombstone_retention_period` (`latest`) | `1d` | how long a tombstone stays, due as [Words: Retention](design.md#words) says |
| `visibility_timeout` (`queue`) | `30s` | a worker's hold; shorter than `retention_period` where one is set |
| `retry.max_attempts` (`queue`) | 5 | attempts before `attempts_exhausted` |
| `retry.mode` (`queue`) | `fixed` | `fixed`, `linear` or `exponential`: how the wait before a job is offered again grows ([Delivery](design.md#delivery)) |
| `retry.delay` (`queue`) | `5s` | the wait `retry.mode` starts from |
| `dlq_retention_period`, `dlq_retention_bytes` (`queue`) | [the storage defaults](#brokerstorage) | the dead-letter channel's retention, due as [Words: Retention](design.md#words) says; `none` allowed |

In `fixed` mode, the retry wait is `retry.delay`.
In `linear` mode, it is `retry.delay` x n.
In `exponential` mode, it is `retry.delay` x 2^(n-1).
Here n is the number of failed attempts so far.
Attempt failures and the wait's start are defined in
[Delivery, Queue jobs](design.md#delivery).

## `bridges.<name>`

A bridge name is 1 to 128 bytes with no whitespace.
A key on a peer it does not belong to is refused.
Neither `bridges:` nor a `tls` block is empty.
The `topics` value is a list.

| Key | Default | Meaning |
|---|---|---|
| `peer` | required | the peer's URL: `tcp://`, `tls://`, `ws://` or `wss://`, with a host as in [a listener's `address`](#a-listener), never empty, and a port from 1 to 65,535; `ws://` and `wss://` may add a path (default `/`); never a user, query or fragment |
| `client_id` | required | Sagüin's client id at the peer; unique per peer: the same host, compared without case, and port |
| `username` | none | Sagüin's user name at the peer |
| `password_file` | none | The file holds the password alone. One trailing line ending (`\n` or `\r\n`) is not counted. This key requires `username`. |
| `session_expiry` | `1d` | the session the peer keeps; never `none`; at most 4,294,967,295 seconds |
| `receive_maximum` | 20 | the Receive Maximum sent to the peer, 1 to 65,535 |
| `ack_interval` | `5ms` | the longest intentional acknowledgement batching delay, at most 1s; release and writing follow [Bridges](design.md#bridges) |
| `tls` (`tls`, `wss`) | none | `ca_file`, the authority for the peer's certificate (default: the system's roots); `cert_file` and `key_file`, Sagüin's certificate for the peer, written together; `min_version`, `1.2` (default) or `1.3` |
| `topics` | required | at least one rule: `filter`, `topic`, `direction` (`in`, `out` or `both`), `qos` (0, 1 or 2, default 1); topic substitution follows [Bridges](design.md#bridges) |

A `topic` template is required for `in` and `out`, and refused for `both`.
The template contains no `+` or `#`, or leading or trailing `/`.
It is never under `$share/` or `$saguin/`.
On an `in` rule it is also never under `$SYS/`.

A filter is never `none`, `$share` or under `$saguin/`.
On an `out` rule it is also never under `$SYS/`.
Braces such as `{a,b}` follow a channel filter's rules
([Channels](#channelsname)).
The `both` direction is refused on a `$` filter.

A bridge's inbound filters may not overlap.
A bridge's outbound filters, those of its `out` rules and of its `both`
rules, may not overlap, with `{a,b}` expanded.
A configuration where they do is refused at load, naming both rules.
Their topics may not overlap either. For this check each `topic` template
is read as a filter: a level holding any `$1`, `$2` and so on is read as
`+`, `$#` as `#`, and a `both` rule's topic is its filter. A topic starting
with `$` is compared like any other. Such a configuration is refused at
load, naming both rules.
*Example: `out` rules `s/a` to `one` and `s/b` to `one` are refused;
`s/+` to `x/$1` beside `t/+` to `y/$1` are accepted.*

## Dropped

A file using any of these keys is refused at startup. The refusal names
a replacement when one exists.

| Key | Instead |
|---|---|
| `storage.default`, `storage.providers`, `storage.type`, `storage.snapshot_dir` | [`broker.storage`](#brokerstorage): one SQLite store, `mode` |
| `storage` under a channel or under `broker.retained`, `broker.session`, `broker.share` or `broker.qos2` | [the one store](design.md#store) |
| `flush_interval` | Syncing follows the storage mode ([Store](design.md#store)). |
| `storage.max_bytes` | none: each channel's own limits, and the disk ([Store](design.md#store)) |
| `limits.max_message_size` | [`limits.max_packet_size`](#brokerlimits) |
| a queue's `retry.backoff`, `retry.backoff_base` | [`retry.mode`](#channelsname), [`retry.delay`](#channelsname) |
| `deletion_retention_period` | [`tombstone_retention_period`](#channelsname) |
| `limits.max_header_bytes` | none: [`limits.max_packet_size`](#brokerlimits) bounds a publish's User Properties |
| the `delete` verb in an `acl_file` `latest` rule | [`write`](design.md#access-and-reload), which covers an empty payload |
| `limits.session_queue_bytes`, `limits.session_queue_full` | [`broker.broadcast`](#brokerbroadcast-brokerretained-brokerqos2) retention |
| a queue's `max_bytes`, `job_expires_after` | its [`retention_bytes`](#channelsname), [`retention_period`](#channelsname) |
| `qos2.expires_after` | a held QoS 2 message stays until PUBREL or its session ends |
| `share.expires_after` | [the channel's retention](design.md#words) |
| `session.ack_commit_interval` | the next commit ([Delivery](design.md#delivery)) |
| `replica`, `bridge_source`, `dlq_bridge_source`, a bridge's `upstream` and `inbound`, a rule's `channel`, `commit_interval`, `commit_max_records`, `retained.retention_bytes`, `session.max_bytes`, `qos2.max_bytes`, an operations listener's `allow_anonymous` | none: refused, with no replacement |
