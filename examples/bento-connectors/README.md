# Bento connectors

Two Bento pipelines move readings from 200 simulated devices through Sagüin
into Kafka. `IoTDataGen.yaml` publishes protobuf measurements, 100 stationary
water-station locations and three schema definitions. `MQTT2Kafka.yaml`
decodes measurements to JSON, keys Kafka records by their MQTT topic and
forwards MQTT properties as headers. Both pipelines use MQTT 5.

Run from this directory with Docker and Docker Compose:

```sh
docker compose up -d --build
```

To start from scratch, run `docker compose down -v` first.

Sagüin and Sagüin Viewer are pulled as their published images,
`ghcr.io/ifnesi/saguin:0.2.0` and `ghcr.io/ifnesi/saguin-viewer:0.2.0`. The
Dockerfile builds Bento at the pinned `BENTO_REF` and the Python traffic
image. The first build downloads source, dependencies and images and can take
several minutes. The broker image has no shell, so `saguin-health` asks its
`/health` over the compose network and the pipelines, the traffic generator
and the viewer wait for it. Run only one of the two example stacks at a time:
both publish MQTT port 1883.

| Address | What to see |
|---|---|
| <http://localhost:4000> | Sagüin Viewer: topic tree, live feed decoded through the `schemas` channel, publish, replay, sessions and access, and the broker's metrics |
| <http://localhost:3000> | Grafana, anonymous viewing; the pipelines row shows records into Sagüin, out of it and into Kafka |
| <http://localhost:9021> | Kafka Control Center: Topics, select a topic, then Messages for decoded readings and headers |
| <http://localhost:9094> | Prometheus, scraping Sagüin and both pipelines every 60 seconds |
| `localhost:1883` | Sagüin MQTT, anonymous, plaintext |
| <http://localhost:9090/health> | Broker readiness, without credentials |
| <http://localhost:9090/metrics> | Broker metrics, `prometheus:prometheus` or `operator:operator` |
| `localhost:1884` | Mosquitto, the outside broker the inbound bridge reads |

Control Center also uses its own Prometheus on port 9099, Alertmanager on
9093 and Schema Registry on 8081. That registry stays empty: the pipelines
use a checked-in protobuf schema and publish plain JSON to Kafka. The MQTT
schema definitions live in Sagüin's `schemas` channel.

Read the measurements without installing a client:

```sh
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic weather-measurement \
  --from-beginning --max-messages 5 \
  --formatter-property print.key=true --formatter-property print.headers=true
```

The measurement channels are `append` logs with one hour of retention.
`water-location` and `schemas` are `latest` channels holding current values.
Kafka's corresponding topics use compaction. Stop and start `mqtt2kafka`
while the generator runs to observe its durable MQTT session catching up;
at-least-once delivery can produce duplicates around a restart.

`traffic.py` supplies jobs and a worker, another durable measurement reader,
traffic to Mosquitto, and deliberate mistakes. The `head-office` bridge
subscribes to `fleet/+/telemetry/+` at Mosquitto and maps it to
`iot/fleet/$2/$1` locally. The `fleet` channel keeps 30 seconds of these
readings. The queue retries three times, with a five-second visibility hold
and a linear one-second retry delay; unfinished jobs expire after five
minutes and dead letters have 15 minutes of retention.

All broker state shares `/var/lib/saguin/saguin.db`, on a named volume in
`full` mode. The database has no whole-store byte cap. The queue's 8 MiB retention
moves unfinished jobs to dead letters instead of refusing new jobs. Retained
broadcast values and fleet readings survive a restart within their retention.
Channel deliveries, including current values, have RETAIN 0.

Dead letters carry the `__dlq/` prefix: a failed job on
`iot/tasks/settle/1234` becomes `__dlq/iot/tasks/settle/1234`, in the derived
append channel `jobs__dlq`. Bento subscribes to `__dlq/iot/tasks/+/+` and
forwards those JSON payloads to Kafka's `jobs-dlq` with their
`saguin-dlq-*` provenance headers. The generator's deliberate publish into
that prefix is refused; only the queue can write dead letters.

Grafana groups the broker's documented metrics by area, with the documented
labels in each query. It shows connections, sessions, channel positions and
retention, queue state and attempts, publication and delivery, bridges, the
shared store, operations, process and Go runtime. The store panels show its
mode, database and WAL sizes, transactions and memory reservations. Snapshot
panels apply only in memory mode and have no series in this full-mode demo.
Counters start at zero, so an inactive path can draw zero without being broken.
The pipelines row uses Bento's own metrics.

Run the smoke script from this directory:

```sh
./smoke.sh
```

It needs Bash, curl, Python 3 and Docker Compose, brings the stack up, checks
operator HTTP responses and Kafka records and leaves it running. It may wait
several minutes for queue activity. It checks deliveries and visibility
timeouts advancing, an exhausted job reaching Kafka with its provenance,
and the single SQLite store holding data.

```sh
curl -u operator:operator http://localhost:9090/v1/operations/bridges
curl -u operator:operator http://localhost:9090/v1/operations/queues/jobs
docker compose exec saguin saguin --check-config /etc/saguin/saguin.yaml
docker compose down       # keep the broker volume
docker compose down -v    # erase the broker volume
```

The credentials are public demonstration credentials; change them before
exposing the stack. MQTT and Kafka have no authentication or TLS. Grafana's
admin account is `admin:admin`. The operator reaches `/metrics` and
`/v1/operations`; the scraper reaches only `/metrics`. Bind-mounted password
files must be readable by container uid 10001. Recreate the container after
replacing a mounted file so it sees the new inode. Mounts carry `z` for SELinux.
