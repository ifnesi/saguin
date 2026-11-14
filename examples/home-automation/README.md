# Home automation without radio hardware

Two stock gateways use Sagüin: zigbee2mqtt decodes Zigbee frames from an
emulated coordinator, and rtl_433 demodulates 433 MHz samples from an
emulated rtl_tcp transmitter. The simulators speak radio protocols and use
only the Go standard library. Only the gateways publish device readings to
MQTT. Zigbee commands travel back through zigbee2mqtt to the emulated plug;
the three 433 MHz sensors are one-way transmitters.

Run from this directory with Docker and Docker Compose:

```sh
docker compose up -d --build
```

To start from scratch, run `docker compose down -v` first.

Sagüin and Sagüin Viewer are pulled as their published images,
`ghcr.io/ifnesi/saguin:0.2.0` and `ghcr.io/ifnesi/saguin-viewer:0.2.0`; only
the two simulators are built, from this directory's independent Go module,
which the broker's package listing, vet, lint and rule checks exclude. The
broker image has no shell, so `saguin-health` asks its `/health` over the
compose network and the gateways and the viewer wait for it. Zigbee
readings come about every ten seconds; the three RF sensors take about 30
seconds for a round. Allow time for the gateways to start.

`seed/devices.jsonl` tells zigbee2mqtt it already knows three devices, so it
does not interview them at startup. This represents an already-paired
installation; pairing is outside this demo. The devices still announce
themselves, report readings and act on commands through frames from the
coordinator emulator. zigbee2mqtt publishes their decoded state to Sagüin;
the emulator never writes device state directly to MQTT.

On first start, the seed is copied to `/app/data/database.db` in the
zigbee2mqtt container. Its format is JSON Lines; the checked-in file uses
`.jsonl` because the repository ignores `.db` files as SQLite runtime data.
Its addresses and model names must match `coordinator/devices.go`. These
models have real zigbee2mqtt converters and use standard ZCL clusters, with
no manufacturer-specific attributes for the emulator to invent:

| Device | Model | Vendor |
|---|---|---|
| living-room-sensor, bedroom-sensor | `CK-TLSR8656-SS5-02(7014)` | eWeLink |
| kitchen-plug | `LDSENK01F` | ADEO |

Run only one example stack at a time: both publish MQTT port 1883. This
stack exposes operations at port 9091, and Sagüin Viewer at
<http://localhost:4001>: the house as a topic tree, a live feed, a publish
form for device commands, replay, the sessions with each user's access, and
the broker's metrics. It logs in as `viewer-hub`, whose `home-viewer` role is
in `acl.yaml`. The `mqtt-client` tools profile uses the Mosquitto image, so
no MQTT client installation is needed:

```sh
docker compose run --rm -T mqtt-client mosquitto_sub \
  -h saguin -p 1883 -V 5 -i reader-hub \
  -u reader-hub -P reader-password-change-me \
  -t 'zigbee2mqtt/#' -t 'rtl433/#' -v
```

A fresh subscription receives current bridge and gateway state, stored
bridge events and logs, then live device readings. Device channels start
at the tail for new readers; durable sessions resume their saved position.
All six channels share `/var/lib/saguin/home.db`, on a named volume in
`full` mode. Readings, events and logs have seven days of retention and
individual size bounds. State channels hold the latest value for each topic.
Retained broadcast messages use the same SQLite store. There is no whole
store byte cap; size the disk for the data you keep. Channel deliveries have
RETAIN 0, including initial current values.

Each gateway has its own password and ACL role and cannot write the other's
topics. Readers can read both and publish Zigbee commands, but cannot invent
device readings. The ACL binds each reader and zigbee2mqtt to a client id
equal to its username; rtl_433 generates an id matching `rtl_433-*`.
Use a different reader identity for concurrent clients to avoid takeover.

Stop the subscriber before sending a command with its identity:

```sh
docker compose run --rm -T mqtt-client mosquitto_pub \
  -h saguin -p 1883 -V 5 -q 1 -i reader-hub \
  -u reader-hub -P reader-password-change-me \
  -t zigbee2mqtt/kitchen-plug/set -m '{"state":"TOGGLE"}'
```

Subscribe again to see the state the plug reports back. Commands use
broadcast topics and are not replayed to a fresh subscriber.

```sh
curl http://localhost:9091/health
curl -u operator:operator-password-change-me http://localhost:9091/metrics
curl -u operator:operator-password-change-me \
  http://localhost:9091/v1/operations/sessions
docker compose exec saguin saguin --check-config /etc/saguin/saguin.yaml
./smoke.sh
```

The smoke script needs Bash, curl, timeout, Docker Compose and host
`mosquitto_pub`/`mosquitto_sub` tools. Start the stack first. Its 31 checks
cover authentication, identity binding, separate gateway permissions, live
readings, replay, durable sessions, broker restart, plug commands and the RF
Will. It restarts Sagüin and kills and starts rtl_433, leaving them running.
It takes several minutes because it waits for real gateway publications.
The broker stores broadcast records too; a fresh subscriber still receives
only retained values and live broadcast traffic, so checks described as
"not stored" test that client-visible behavior rather than inspecting storage.

```sh
docker compose logs -f zigbee2mqtt
docker compose logs -f rtl433
docker compose down       # preserve the broker and zigbee2mqtt volumes
docker compose down -v    # reset both volumes
```

The radio addresses are swappable: change `serial.port` in
`zigbee2mqtt.yaml` to a real coordinator over TCP, or rtl_433's `-d` address
to a real rtl_tcp server, with matching network settings and devices.
The coordinator's PAN id, extended PAN id, network key and channel must
match zigbee2mqtt. The seeded device addresses match the emulator.

When `ZBEMU_READY_FILE` is set, the coordinator writes `ready` followed by a
newline to that path after opening its listener. A failed write stops startup
and reports the error. The Compose stack and smoke script do not use this
optional readiness file.

All passwords here are public demonstration credentials. The interactive
reader uses `reader-password-change-me`; the gateways use
`hub-password-change-me` and `rf-password-change-me`; operations uses
`operator-password-change-me`. Smoke readers have separate credentials in
`clients.passwd`. Change them before exposing the stack. Connections are
plaintext; both broker and Zigbee configurations include commented TLS
settings for certificates issued by your own authority. Password files must
be readable by container uid 10001. Recreate the container after replacing a
bind-mounted file so it sees the new inode. Mounts carry `z` for SELinux.
