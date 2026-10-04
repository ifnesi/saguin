# The examples

Everything here runs. `examples/saguin.yaml` is a working configuration
that names every key Sagüin accepts, and `examples/demo.py` is a guided
tour that starts a broker against it and walks every channel type in
order. The tour runs in Docker, or directly with Go and Python.

`bento-connectors/` and `home-automation/` are stacks rather than scripts:
one `docker compose up` each, and a README of their own.

## The guided tour

With nothing installed but Docker, from the repository root:

```sh
cd examples
docker compose run --rm --build demo           # the concise tour
docker compose run --rm --build demo --full    # every section
docker compose down                            # stops the viewer afterwards
```

Watch it in [Sagüin viewer](https://github.com/ifnesi/saguin-viewer) at
<http://localhost:8080> while it runs: the topics, a live feed and replay.
The tour runs against Sagüin's published image.

It stops at every step and waits for ENTER, so it can be read at the speed
of whoever is reading it, or presented to a room at the speed of whoever is
talking. With no arguments it gives the concise core tour. `--full` adds the
security, partitioning, refusal, retry and dead-letter deep dives;
`--section queue`, for example, runs one section independently. `--list`
prints every section and `--no-pause` makes any form non-interactive.

**Without Docker** it needs Go, to build the broker, and the shared examples
Python environment, from the repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r examples/requirements.txt
python examples/demo.py
```

It builds the broker, starts it, drives it, and stops it. `make demo` runs the
same concise path, `make demo-full` runs all of it, and `make demo-server`
leaves the configured broker running for experimentation.

The concise tour follows broadcast, append, latest, queue success and storage.
**The full tour walks these in order:**

| | |
|---|---|
| Security | A wrong password refused `0x86` and the right one accepted; TLS, with a client that verifies and one that cannot; mutual TLS, where a device authenticates with a certificate and no password at all; the `acl_file` refusing a topic with `0x87`; the WebSocket and Unix listeners |
| Broadcast | An ordinary MQTT topic no channel claims - and what a subscriber that arrived late does not get |
| `append` | A durable, replayable event stream: two consumers at their own positions, a replay from the start, and a seek by offset |
| `latest` | Current value per topic, as a key/value store: set, multi-get on subscribe, a point read, and a delete |
| `queue` | One worker at a time, with acknowledgement, return, the visibility timeout, retries and dead-lettering - with the seconds printed, because the gaps are the point |
| Storage | What each kind of storage still holds after a graceful stop and after a kill |

**It is a Paho client and nothing else**, which is the point: every step is
an ordinary publish or a subscription option, and there is no Sagüin SDK.
Two steps are why it is a library rather than a shell script - the SUBACK
reason code behind a refused queue subscription, which the command-line
tools print away, and a queue worker acknowledging, which the broker takes
only from the session that received the job.

**The broker it runs is a secured one.** All three listeners a broker can
have are open at once - TLS on TCP, wss on WebSocket, and a Unix socket -
behind a password file and an `acl_file`. There is no anonymous half:
authorization is a statement about an identity, and Sagüin refuses an
`acl_file` beside a listener that admits clients having none.

`make demo-server` starts the same broker against the same configuration and
leaves it running, for poking at by hand.

## The files

**What you run comes first, and `support/` is what those things are built
from.** Nothing under `support/` is a demo, which is the whole reason it has
a name: opening this directory should say which is which without reading a
line of it.

| | |
|---|---|
| `demo.py` | The guided tour |
| `docker-compose.yml`, `demo.Dockerfile`, `saguin-viewer.yaml` | The guided tour in a container, with nothing installed but Docker, and the viewer watching it |
| `docker/` | Sagüin and its viewer as published images, for your own devices rather than a demo. Its configuration mounts beside it |
| `saguin.yaml` | Every key Sagüin accepts, with its default and the reasoning beside it. The tour runs against this file unchanged |
| `acl.yaml` | What each client may do: roles carrying rules, clients given roles by pattern |
| `clients.passwd` | Who may connect. Mosquitto's format, hash for hash |
| `home-automation/` | A stack. Two real gateways, zigbee2mqtt and rtl_433, both stock images unmodified, publishing into Sagüin with no radio hardware at all: an emulated coordinator and an emulated dongle answer where the radios would. Its own README |
| `bento-connectors/` | A stack. Two Bento pipelines feeding Kafka from MQTT through three Sagüin channels, and Prometheus and Grafana watching the broker carry them - a dashboard covering every metric in the catalogue, drawn against a load nobody shaped to look good on it. It builds Bento from a branch, because the MQTT 5 client it needs is not released yet; its own README says which |
| `starting-points/` | Three first configurations to adapt - a home hub, an edge gateway bridged to a head office, a fleet broker with TLS and ACLs - each loaded by a test so none goes stale |
| `support/certs/` | The demo's certificate authority, the broker's certificate, and one device's |
| `support/channels/presence.yaml` | One channel in a file of its own, because a fleet's channels belong with the team that owns them |
| `support/storage/durable.yaml` | The same, for a storage provider |
| `support/worker/` | A queue worker in Go, for reading rather than running: the shape RFC 0003 recommends |

## Why the demo copies these to /tmp

**Sagüin refuses a relative path** for `cert_file`, `key_file`,
`client_ca_file`, `password_file` and `acl_file`, on purpose:

```
...tls.cert_file "examples/support/certs/broker.pem" is relative: which
certificate a listener serves must not depend on where the broker was
started from
```

A configuration that ships in a repository therefore cannot name these
files where they sit, because no absolute path into a checkout is true on
two machines. So `saguin.yaml` names `/tmp/saguin-demo/...`, and the demo
copies the files there before it starts. The demo's database and snapshots
live there for the same reason.

A real deployment never meets this: the files are at `/etc/saguin`, an
operator put them there, and the path is written once.

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

They expire in 2046. Long-lived deliberately - a demonstration whose
certificates lapse stops working years later with a TLS error nobody
connects to the cause. Real certificates should not be issued this way; 825
days is the usual ceiling, and RFC 0002, under "Client certificates", has
that recipe.

**How they were made** - openssl 3, and `-copy_extensions` is what carries
the `subjectAltName` into the signed certificate:

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

## The password file

`clients.passwd` holds one user, `demo`, whose password is `hunter2`. It is
Mosquitto's format, hash for hash, so a fleet moving from Mosquitto keeps
the credentials it has. Written with:

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

```
user "device-7", against /tmp/saguin-demo/acl.yaml

patterns matched:
  device-7                 -> device               applies
  device-*                 -> device               shadowed by "device-7"

Only "device-7" is in force: its roles and its limits are the whole of what this
client gets, and a shadowed entry adds nothing to them.

publish limits, what this client is held to:
  messages a second        200        from the acl_file entry "device-7"
  bytes a second           65536      from the acl_file entry "device-7"
  note: "device-*" carries limits and matches this client, but "device-7" applies.

effective grants:
  topic iot/+/health/device-7  write, read  (role device)
```

The rule is printed **resolved**: `%u` is gone and the device's own name is
in its place. A `%u` left in that output would be a grant the broker will
never match.
