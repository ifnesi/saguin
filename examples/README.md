# The examples

`examples/saguin.yaml` is a working configuration naming every accepted key,
with defaults and comments explaining the settings. `examples/demo.py` is a
Python guided tour using Eclipse Paho's MQTT 5 client. Run commands from the
repository root. Building the broker needs Go and the C toolchain used by
`make build`.

## Running the tour

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r examples/requirements.txt
python examples/demo.py
```

The concise tour follows broadcast, append, latest, queue success and storage.
It pauses for ENTER on a terminal. `--full` adds the security, partitioning,
refusal, retry and dead-letter deep dives. `--section queue` selects a section;
repeat `--section` to select several. `--list` prints the section names, and
`--no-pause` runs without interaction. The script exits at its first failed
assertion and stops the broker. The tour uses MQTT acknowledgements, delivered
messages and receive counts as evidence; the log is written for manual inspection.

The script builds the broker through `make build`, starts it with the shipped
configuration, drives it, and stops it. `--skip-build` uses `bin/saguin` as it
stands. `make demo` runs the concise tour, `make demo-full` selects the full
tour, and `make demo-server` stages credentials and leaves the broker running
for experimentation. These targets use the repository's `.venv` for the tour.

| Section | What it exercises |
|---|---|
| Security | Password refusal and acceptance, server verification, a device certificate, ACL grants, WebSocket and Unix connections |
| Broadcast | Live delivery on topics no configured channel claims, and a new subscriber arriving after publication |
| Append | Independent consumers, replay, durable positions and seeking by offset |
| Partitioning | Three readers splitting topics by a declared hash partition |
| Latest | Replacing current values, subscribing for state, live updates, deletion and point reads |
| Queue | Exclusive worker offers, application acknowledgement, return, visibility timeout, retries and dead-letter observation |
| Storage | Publishing markers, stopping and restarting the broker, and counting delivered markers |

The broker opens TLS TCP on `localhost:8883`, plain TCP on `127.0.0.1:1884`,
WebSocket over TLS on `localhost:8083`, and `/var/tmp/saguin-demo/saguin.sock`.
Every listener authenticates. A verified device certificate supplies its
identity; a client with no certificate uses the password file. The ACL grants
permissions to that proved identity.

## The files

| File | Purpose |
|---|---|
| `demo.py` | The guided MQTT tour |
| `saguin.yaml` | The tour's annotated configuration and optional service examples |
| `acl.yaml` | Roles and grants for the tour and devices |
| `clients.passwd` | The demonstration user's password hash |
| `requirements.txt` | The Python dependency, pinned to paho-mqtt 2.1.0 |
| `starting-points/fleet-broker.yaml` | TLS and per-device grants, events, current status and work; uses `support/fleet-acl.yaml` |
| `starting-points/home-hub.yaml` | A small password-protected hub with a week of readings, current state and normal-mode storage |
| `starting-points/plant-gateway.yaml` | TLS devices, a week of local telemetry, setpoints, and an outbound bridge to a head office |
| `support/certs/` | The public demonstration authority, broker and device certificates and private keys |
| `support/channels/presence.yaml` | A separately owned channel group included by the tour configuration |
| `support/storage/durable.yaml` | A standalone normal-mode broker showing commit collection, reader connections and syncing |

## Paths and configuration checks

Sagüin requires absolute file paths, including channel includes. All example
runtime files use `/var/tmp/saguin-demo/`, the same disk-backed location on
every machine. The tour stages certificates, the client password file, both
ACL files, and `channels/presence.yaml` there before starting the broker.
Each example's database and Unix socket also live there; the tour's log is
`/var/tmp/saguin-demo/saguin-demo.log`.

Staging gives the shipped configurations absolute paths independent of the
checkout location. The script runs those configurations directly. The tour
starts with an empty state directory on each run.

Stage the credentials before checking or starting any configuration:

```sh
make build
python3 examples/demo.py --credentials
bin/saguin --check-config examples/saguin.yaml
bin/saguin --check-config examples/starting-points/fleet-broker.yaml
bin/saguin --check-config examples/starting-points/home-hub.yaml
bin/saguin --check-config examples/starting-points/plant-gateway.yaml
bin/saguin --check-config examples/support/storage/durable.yaml
```

To run a starting point, use `bin/saguin --config <file>`. Adapt its listeners,
paths, credentials and retention to your deployment. The plant gateway's
bridge needs a reachable peer and the appropriate trust and credentials.

The main check also reads its ACL, password file, certificates, keys and
included channel group. The fleet check reads `support/fleet-acl.yaml`.
ACLs and channel groups are fragments checked through their broker
configuration.

The annotated configuration names every key in [config.md](../docs/config.md).
Compatible settings are active; optional operations and bridge examples and
mode-specific alternatives are commented. A storage mode accepts only its own
keys. Use `--check-config <file> --output` to inspect the resolved settings.

## Channels and persistence

The four configured filters divide the same `iot/` hierarchy: events are an
append log, state and presence hold the latest value per topic, and work is
a queue. Filters match exactly four levels. A topic such as
`iot/depot/health/device-1` belongs to `$broadcast`.

All channels share one SQLite store. In this configuration `mode: full`
syncs every commit, preserving acknowledged writes across crashes and power
loss. The home hub and standalone storage example use `mode: normal`, which
preserves acknowledged writes across a process crash; a power cut may lose
commits since the last completed sync. Memory mode runs on a tmpfs copy and
restores the last disk snapshot on restart.

New broadcast subscriptions receive retained values and then live messages.
Durable sessions resume their saved positions. Append readers replay from
the floor unless configured to start at the tail. Latest subscribers receive
current values and surviving empty-payload tombstones, then changes. Channel
deliveries carry RETAIN 0.

Workers subscribe to `$saguin/queue/jobs` at QoS 1 over MQTT 5. A job response
publishes `ack` or `return` to its Response Topic with its Correlation Data.
The visibility hold starts at PUBACK; retry delay starts when an attempt ends.
Queue readers need `read`, separately from worker `consume`. Dead letters go
to the append channel `jobs__dlq`, with topics prefixed `__dlq/`.

Retention can pass a consumer's position. Logs have age and size retention,
append channels can have hard size caps, and retained values have their own
size cap. Size the disk for peak use: SQLite reuses freed pages and retains
the file's peak size. The [design](../docs/design.md) defines the delivery and
persistence rules in full.

## The certificates

**These are throwaway demonstration keys and the private keys are in this
repository on purpose.** They protect nothing, they are readable by anyone
who can read this file, and they must never be used for anything real. They
are checked in so that the tour runs with nothing to generate first.

`ca.pem` signs both sides: it is the listener's `client_ca_file` and it is
what every client verifies the broker against.

| | |
|---|---|
| `ca.pem`, `ca-key.pem` | The demonstration authority |
| `broker.pem`, `broker-key.pem` | What the listener serves. `CN=localhost`, with `subjectAltName` for `localhost` and `127.0.0.1` |
| `device-7.pem`, `device-7-key.pem` | A device. **`CN=device-7` is the whole point**: Sagüin makes a verified certificate's Common Name the client's user name, so this device authenticates with no password and its `acl_file` rule on `%u` matches it |
| `unrelated-ca.pem` | An authority with no relationship to any of the above, and no key beside it. The tour hands it to a client so the handshake fails - proving the broker is verified rather than describing it |

The shipped certificates expire in 2046. Long-lived deliberately: a
demonstration whose certificates lapse stops working years later with a TLS
error nobody connects to the cause. Real certificates should not be issued
this way; use shorter lifetimes and your own authority as below.

**Generating demonstration certificates** with OpenSSL 3.
`-copy_extensions` carries the `subjectAltName` into the signed certificate:

```sh
cd examples/support/certs

# One authority. It signs both the broker and the device.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -days 7300 -nodes -subj "/CN=saguin-demo-ca" \
  -keyout ca-key.pem -out ca.pem

# The broker's certificate. The subjectAltName must say what clients
# dial - a Common Name alone fails modern verification, and that is
# where a first attempt at TLS usually stops.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout broker-key.pem -out broker.csr
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 7300 -copy_extensions copy -out broker.pem

# A device. The Common Name is the client's name: it becomes the user
# name, it matches acl_file patterns, and the password file is not
# consulted for a client that presents one.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=device-7" -keyout device-7-key.pem -out device-7.csr
openssl x509 -req -in device-7.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 7300 -out device-7.pem

# An authority signing nothing, so a client can be given the wrong one.
# Its key is discarded: only the certificate is ever used, as something
# for a client to trust and fail with.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -days 7300 -subj "/CN=someone-elses-ca" \
  -keyout /dev/null -out unrelated-ca.pem

rm -f broker.csr device-7.csr ca.srl
```

To make your own certificates with OpenSSL 3, use your broker's real DNS
name in the subjectAltName and a shorter lifetime:

```sh
# One authority. It signs both the broker and the device.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -days 825 -nodes -subj "/CN=my-ca" \
  -keyout ca-key.pem -out ca.pem

# The broker's certificate. The subjectAltName must say what clients dial.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=broker.example.com" \
  -addext "subjectAltName=DNS:broker.example.com" \
  -keyout broker-key.pem -out broker.csr
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -copy_extensions copy -out broker.pem

# A device. The Common Name becomes the client's user name.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=device-7" -keyout device-7-key.pem -out device-7.csr
openssl x509 -req -in device-7.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -out device-7.pem

rm -f broker.csr device-7.csr ca.srl
```

Keep new `*-key.pem` files out of version control and readable only by the
account running the broker (`chmod 600`). These commands create your own
files; they do not replace the shipped demonstration credentials automatically.

## The password file

`clients.passwd` holds one user, `demo`, whose password is `hunter2`.
Each line is `user:hash`; this file uses PBKDF2-HMAC-SHA512 (`$7$`).
To set the demonstration password:

```sh
saguin --passwd add examples/clients.passwd demo hunter2
```

These are demo credentials, public; change them before exposing a broker.

`device-7` is deliberately **not** in it. It authenticates by certificate,
which is the half of `require_certificate: false` worth seeing: with a
certificate you are named by it, without one you fall through to the
password file, and both work on the same listener.

## Reading what a client may do

```sh
saguin --acl examples/saguin.yaml device-7
```

The exact `device-7` entry wins over `device-*` and supplies the whole
entry: its role, client-id constraint and limits of 200 messages/s and
64KiB/s. The effective broadcast grant is `iot/+/health/device-7` for write
and read, with `%u` substituted from the verified certificate identity.
Use `--json` for machine-readable output.
