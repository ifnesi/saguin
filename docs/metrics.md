# Sagüin metrics

This document lists every metric Sagüin exposes on the operations
listener's `/metrics`, in the Prometheus text format. The route and the
`$SYS` policy are defined in [Operations, HTTP](design.md#http).
Each series carries its Meaning below as its HELP line, as plain text.

## Rules

- Every name starts with `saguin_`.
  A counter ends in `_total`, a duration in `_seconds`, and a size in
  `_bytes`.
- Every label's values are listed here or come from the configuration
  ([config.md](config.md#sagüin-configuration)).
  A `channel` is a configured channel, its `<channel>__dlq` or
  `$broadcast`. A `bridge` is a configured bridge.
  A bridge rule's `filter` and `topic` are as configured, as are the
  labels of the `_info` series.
  No label carries a client id, a user name or a topic a client chose.
  The `qos` is `0`, `1` or `2`.
  The `protocol` is `3.1.1` or `5`.
- Every series exists from start, for every value of its labels that applies.
  A floor exists only where a channel has one, and queue series exist only
  for queues. Counters start at 0.
  The `_info` series start at 1.
  Other gauges start at their current value.
- A counter is an atomic changed by the goroutine that sees its event.
  The same applies to a gauge moved where its event happens, such as open
  sockets or bytes in transit. A gauge only the core knows is an atomic
  the core sets ([R3](design.md#rules)).
  A value only the store knows, such as a size or count, is re-read every
  `min_scrape_interval` ([config.md](config.md#brokeroperations)).
  The core has a store reader read it.
  The value is kept as an atomic until the next read, and a scrape returns
  the values held.

  These values are [`saguin_channel_bytes`](#channels-and-retained-values),
  [`saguin_channel_consumers`](#channels-and-retained-values),
  [`saguin_channel_position_min`](#channels-and-retained-values),
  [`saguin_retained_values`](#channels-and-retained-values),
  [`saguin_retained_bytes`](#channels-and-retained-values),
  [`saguin_store_bytes`](#store-and-memory),
  [`saguin_store_wal_bytes`](#store-and-memory),
  [`saguin_sessions_away`](#connections-and-sessions),
  [`saguin_subscriptions`](#connections-and-sessions) and
  [`saguin_wills_waiting`](#connections-and-sessions).
  Each value reports the event or observation boundary defined in its row.
  Store-derived gauges share the Store-read cut used for that refresh
  ([design.md, Store reads](design.md#store-reads)).
  Queue retention bytes subtract the held jobs at that cut.
  A scrape returns published values without waiting. In particular, it
  never waits on the core or the store.
- For an MQTT reason code, the *reason* label is its MQTT 5 reason name
  in lower case, with each space or hyphen replaced by an underscore.
  Examples are `quota_exceeded` and `re_authenticate`.
  The reason is counted before an MQTT 3.1.1 client's code is mapped.
  An MQTT 3.1.1 connection closed where MQTT 5 would get a code is counted
  under that code's name. A reason that is not an MQTT code, such as
  `connect_timeout`, `client`, `writer_full` or a dead-letter reason,
  is named in its series' row.
- Per-consumer detail, such as who missed what, is never in a label.
  It is available on [`/v1/operations/consumers`](design.md#http) and
  [`/v1/operations/position-lost`](design.md#http).

## Process

| Series | Type | Meaning |
|---|---|---|
| `saguin_build_info{version,broker_id}` | gauge | always 1 |
| `saguin_uptime_seconds` | gauge | since start |
| `saguin_ready` | gauge | 1 while listeners and bridges have started, the last snapshot, if any yet, succeeded and the broker is not stopping: [`/health`](design.md#http)'s conditions but the core's answering, kept as flags the core sets from the state it answers `/health` from |
| `saguin_goroutines{role}` | gauge | live goroutines per [R1](design.md#the-goroutines-r1) row: `main`, `core`, `store_writer`, `store_reader`, `logger`, `listener`, `reader`, `writer`, `dialer`, `http_request` |
| `saguin_log_lines_dropped_total{cause}` | counter | lines lost: `cause` is `full` (the core held no log credit, [design.md, Logging](design.md#logging)) or `refused` (the log output refused it) |
| `saguin_reloads_total{part,result}` | counter | SIGUSR1 steps: `part` is `log_and_origins`, `access` (password files and `acl_file`) or `tls`; `result` is `applied` or `refused` |
| `saguin_operations_requests_total{route,code}` | counter | HTTP requests: `route` is `/health`, `/metrics`, `acl`, `config`, `bridges`, `consumers`, `sessions`, `users`, `queues`, `refused`, `position-lost` or `other`; `code` is `200`, `400`, `401`, `403`, `404`, `405` or `503` |

## Connections and sessions

| Series | Type | Meaning |
|---|---|---|
| `saguin_connections{protocol}` | gauge | connected clients, `3.1.1` or `5`: the sessions the core holds connected |
| `saguin_connections_open` | gauge | sockets served, from their permit to their close: what [`limits.max_connections`](config.md#brokerlimits) bounds |
| `saguin_connections_accepted_total` | counter | sockets accepted, refused ones included |
| `saguin_connections_refused_total{reason}` | counter | the CONNACK's reason ([design.md, CONNECT](design.md#connect)): `unsupported_protocol_version` (an unknown level included), `client_identifier_not_valid`, `bad_user_name_or_password`, `not_authorized`, `topic_name_invalid`, `retain_not_supported`, `qos_not_supported`, `packet_too_large`, `implementation_specific_error`, `bad_authentication_method`, `quota_exceeded`; `malformed_packet` (another Protocol Name included) and `protocol_error` (a first packet other than CONNECT included), for a connection closed before its CONNACK; `connect_timeout` (no CONNECT in time); `tls_handshake` (a failed TLS handshake, [design.md, Transport setup](design.md#transport-setup)); or `proxy_header` (a refused PROXY header, [design.md, Transport setup](design.md#transport-setup)). A refusing CONNACK that cannot fit the client's Maximum Packet Size counts once as [`saguin_disconnects_total{reason="unspecified_error"}`](#connections-and-sessions), instead of as a connection refusal. The refused history records that same closure event. No second event is counted for the unsent CONNACK |
| `saguin_disconnects_total{reason}` | counter | the reason Sagüin selected for a closure, including a closure without a sendable DISCONNECT ([design.md, DISCONNECT](design.md#disconnect)): `unspecified_error`, `malformed_packet`, `protocol_error`, `server_shutting_down`, `keep_alive_timeout`, `session_taken_over`, `receive_maximum_exceeded`, `topic_alias_invalid`, `packet_too_large`, `administrative_action`, `retain_not_supported`, `qos_not_supported`, `shared_subscriptions_not_supported`; for an MQTT 3.1.1 publish refusal, `not_authorized`, `topic_name_invalid`, `quota_exceeded` or `implementation_specific_error`; or `client` (the client's DISCONNECT), `connection_lost`, `writer_full`, `publish_stalled` (no body byte for [`limits.write_timeout`](config.md#brokerlimits) after the first body-read attempt or the latest body byte, [Message memory, A stalled body](design.md#message-memory)) |
| `saguin_sessions_away` | gauge | away sessions |
| `saguin_sessions_restored_total` | counter | durable sessions kept across the start, after those expired ended |
| `saguin_sessions_ended_total{cause}` | counter | `clean_start`, `expired`, `connection_end` (expiry 0) |
| `saguin_session_expiry_capped_total` | counter | CONNECTs given [`max_session_expiry`](config.md#brokerlimits) instead of what they asked |
| `saguin_subscriptions` | gauge | subscriptions held, every session: durable subscriptions from the store and non-durable subscriptions held by the core at [the same Store-read cut](design.md#store-reads). Neither source counts the other's subscriptions. Changes after that cut appear in a later refresh |
| `saguin_subscriptions_refused_total{reason}` | counter | refused filters, one per filter: `not_authorized`, `topic_filter_invalid`, `quota_exceeded`, `implementation_specific_error`, or `unspecified_error` for what only MQTT 3.1.1 is refused (a worker, `$share`) |
| `saguin_subscriptions_held` | gauge | subscriptions whose deliveries are held because the `acl_file` no longer allows them |
| `saguin_wills_waiting` | gauge | Wills waiting out their Will Delay |
| `saguin_wills_published_total{cause}` | counter | `disconnect` (0x04), `lost` (after its Will Delay, if any; a DISCONNECT with another reason code but 0x00 included), `takeover`, `session_end`, `restart`: one cause per Will, `restart` for one published after a restart, `lost` for one the full-store retry publishes (its cause is not stored), otherwise the event that made it due |
| `saguin_wills_cancelled_total` | counter | This counts Wills that a reconnect cancelled in time. |
| `saguin_wills_dropped_total{cause}` | counter | `channel_full`, `retained_full` (the retained values' limit), `not_authorized` |

## Publishing

| Series | Type | Meaning |
|---|---|---|
| `saguin_publishes_received_total{qos}` | counter | completely decoded PUBLISH packets from clients and inbound bridges, policy refusals included; packets rejected before complete decoding are excluded |
| `saguin_publish_refused_total{cause}` | counter | refused publishes, QoS 0 drops included ([design.md, PUBLISH](design.md#publish)): `not_authorized`, `topic_invalid`, `store_full`, `channel_full`, `retained_full`, `headers`, `reserved_property`, `no_response_topic`; a `$saguin` request refused for its payload counts only in [`saguin_requests_total`](#publishing) |
| `saguin_publish_throttled_seconds_total` | counter | time readers spent waiting for their connection's publish allowance to refill ([design.md, PUBLISH, Rate limits](design.md#publish)); added as each wait ends |
| `saguin_records_stored_total{channel}` | counter | records that entered a channel, dead-letter moves excluded |
| `saguin_delivery_admission_gated` | gauge | 1 while the [delivery admission gate](design.md#capacities) is closed, otherwise 0; independent of whether a publisher is waiting |
| `saguin_delivery_admission_gated_seconds_total` | counter | time the [delivery admission gate](design.md#capacities) was closed, by the core's event times. Includes completed intervals only; an ongoing interval is added when it ends |
| `saguin_qos2_held` | gauge | incoming QoS 2 messages of connected sessions waiting for their PUBREL: the identifiers the core holds against [`qos2.max_inflight_per_client`](config.md#brokerbroadcast-brokerretained-brokerqos2) |
| `saguin_qos2_abandoned_total` | counter | held QoS 2 messages discarded because their session ended |
| `saguin_bytes_received_total`, `saguin_bytes_sent_total` | counter | bytes on MQTT connections, bridges included |
| `saguin_requests_total{request,result}` | counter | `$saguin` requests: `request` is `seek`, `kv_get`, `catalogue` or `sessions_disconnect`; `result` is `refused`: a request refused on its PUBACK or PUBREC, including a refusal carried by its direct answer; `dropped`: a discarded QoS 0 request; `answer_too_large`: an otherwise completed request whose answer exceeds Maximum Packet Size; otherwise `answered`. Each request contributes one terminal result |

## Delivery

| Series | Type | Meaning |
|---|---|---|
| `saguin_deliveries_total{qos}` | counter | PUBLISH packets sent to clients and outbound bridges, resends included |
| `saguin_inflight` | gauge | QoS 1 and 2 deliveries to connected clients and bridges waiting for their acknowledgement: the windows the core holds |
| `saguin_deliveries_expired_total` | counter | `$broadcast` deliveries to clients skipped because their Message Expiry ran out (configured channels still deliver them); a bridge's are under [`saguin_bridge_unsent_total`](#bridges) |
| `saguin_missed_total{channel,cause}` | counter | records consumers were owed and never received, exact and in memory from each start: `retention` (the channel's total from each deletion's answer, away sessions included) or `too_large` ([design.md, Words: Missed](design.md#words)). Counts consumer-record losses, so one record owed to two consumers contributes two; a job discarded as too large to dead-letter contributes one |
| `saguin_shares_dropped_total{channel}` | counter | unfinished shared QoS 2 messages dropped when their member's session ended |

## Channels and retained values

| Series | Type | Meaning |
|---|---|---|
| `saguin_channel_info{channel,type,filter}` | gauge | always 1; `$broadcast` and a dead-letter channel are `append`, with filter `#` and the derived filter |
| `saguin_channel_records{channel}` | gauge | records held: counted from the store at start, then kept by the core from each committed change that adds or removes records |
| `saguin_channel_bytes{channel}` | gauge | bytes held, as retention counts them |
| `saguin_channel_next_offset{channel}` | gauge | the tail |
| `saguin_channel_floor_offset{channel}` | gauge | the floor; `append`, `queue` and `$broadcast` only |
| `saguin_channel_retention_removed_total{channel}` | counter | records retention deleted |
| `saguin_channel_consumers{channel}` | gauge | stored positions |
| `saguin_channel_position_min{channel}` | gauge | the lowest stored position; absent while the channel has none |
| `saguin_retained_values` | gauge | current retained values on `$broadcast` |
| `saguin_retained_bytes` | gauge | the size of the retained values on `$broadcast`, against [`retained.max_bytes`](config.md#brokerbroadcast-brokerretained-brokerqos2) |

## Queues

| Series | Type | Meaning |
|---|---|---|
| `saguin_queue_jobs{channel,state}` | gauge | unfinished jobs: `ready` (can be offered), `backoff` (waiting out its [`retry.mode`](config.md#channelsname) wait), `offered` (sent, its PUBACK not yet in) or `held` (with a worker) |
| `saguin_queue_workers{channel}` | gauge | connected workers |
| `saguin_queue_delivered_total{channel}` | counter | attempts handed to workers |
| `saguin_queue_acknowledged_total{channel}` | counter | `ack` responses |
| `saguin_queue_attempts_failed_total{channel,cause}` | counter | failed attempts: `return` (a refusing PUBACK included), `timeout` (a hold past [`visibility_timeout`](config.md#channelsname)) or `worker_gone` |
| `saguin_queue_stale_responses_total{channel}` | counter | responses that changed nothing: naming an attempt not current or not the session's, or with another payload |
| `saguin_queue_dead_lettered_total{channel,reason}` | counter | jobs moved to `<channel>__dlq`: `expired`, `evicted` or `attempts_exhausted` |

## Store and memory

| Series | Type | Meaning |
|---|---|---|
| `saguin_store_info{mode}` | gauge | always 1; `full`, `normal` or `memory` |
| `saguin_store_bytes` | gauge | the database's size |
| `saguin_store_wal_bytes` | gauge | the write-ahead log's size |
| `saguin_store_full` | gauge | 1 while the disk is full: from a write SQLite could not save for room until the next saved one |
| `saguin_store_no_credit_seconds_total` | counter | time the core held no store credit and took no packets ([R2](design.md#rules)). Includes completed intervals only; an ongoing interval is added when it ends |
| `saguin_store_commits_total{closed_by}` | counter | successfully committed transactions, by their fixed commit cause: `event`, `interval`, `max_records`, `barrier` or `stop`, as defined in [design.md, Store, Writes](design.md#writes). A batch sealed by its 4 MiB byte limit counts as `max_records` |
| `saguin_store_committed_records_total` | counter | records carried by successfully committed transactions |
| `saguin_snapshots_total{result}` | counter | In memory mode, counts snapshots with result `ok` or `failed`. |
| `saguin_snapshot_last_success_timestamp_seconds` | gauge | In memory mode, records when the last good snapshot finished. |
| `saguin_snapshot_last_duration_seconds` | gauge | In memory mode, records how long the last snapshot took, failed or not. |
| `saguin_memory_in_transit_bytes{direction}` | gauge | outstanding reservations of each budget, `incoming` or `outgoing` ([design.md, Message memory](design.md#message-memory)) |
| `saguin_memory_max_bytes{direction}` | gauge | each budget's size, `incoming` [`limits.max_incoming_memory`](config.md#brokerlimits) or `outgoing` [`limits.max_outgoing_memory`](config.md#brokerlimits) |
| `saguin_memory_full_seconds_total{direction}` | counter | each budget's full time, `incoming` or `outgoing` ([design.md, Message memory](design.md#message-memory)), by the events' times. Includes completed intervals only; an ongoing interval is added when it ends |

## Bridges

| Series | Type | Meaning |
|---|---|---|
| `saguin_bridge_info{bridge,peer}` | gauge | always 1 |
| `saguin_bridge_connected{bridge}` | gauge | 1 while the link is up |
| `saguin_bridge_rule_up{bridge,direction,filter,topic}` | gauge | `in`: 1 while the peer grants the subscription; `out`: 1 while sending; a `both` rule has one series each way, its `topic` empty |
| `saguin_bridge_connection_attempts_total{bridge,result}` | counter | dials: `connected` or `failed` |
| `saguin_bridge_reconnects_total{bridge}` | counter | links re-established |
| `saguin_bridge_received_total{bridge}` | counter | records from the peer that were stored |
| `saguin_bridge_sent_total{bridge}` | counter | records the peer acknowledged, or sent at QoS 0 |
| `saguin_bridge_unsent_total{bridge,cause}` | counter | `refused` (the peer refused it), `expired`, `topic_invalid` (its topic, after substitution, past a topic limit) or `too_large` (over the peer's Maximum Packet Size) ([design.md, Bridges](design.md#bridges)) |

## Go runtime

Each scrape reads these values from `runtime/metrics`.

| Series | Type | Meaning |
|---|---|---|
| `saguin_go_heap_live_bytes` | gauge | heap the last collection found reachable |
| `saguin_go_heap_goal_bytes` | gauge | heap size by which the collector aims to finish its current or next cycle |
| `saguin_go_gc_cycles_total` | counter | collections completed |
| `saguin_go_gc_cpu_seconds_total` | counter | CPU spent collecting |
| `saguin_go_gc_assist_cpu_seconds_total` | counter | the part of the CPU spent collecting that Sagüin's goroutines spent helping |
| `saguin_go_stack_bytes` | gauge | memory held for goroutine stacks |
| `saguin_go_allocated_bytes_total`, `saguin_go_allocated_objects_total` | counter | heap allocations |
| `saguin_go_gogc_percent` | gauge | the effective `GOGC`: `/gc/gogc:percent` read as a signed 32-bit value, so -1 when off |
| `saguin_go_memory_limit_bytes` | gauge | the effective `GOMEMLIMIT` |
