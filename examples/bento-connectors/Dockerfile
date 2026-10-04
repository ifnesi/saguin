# Bento with an `mqtt_v5` input and output, built from source.
#
# **This exists because no published Bento image has those components yet.**
# They are warpstreamlabs/bento#991, merged on 2026-09-13 but in no release,
# so the only way to run this demo is to build Bento's `main` from source.
# The day a release carries them, this stage goes and docker-compose.yml
# goes back to `image: ghcr.io/warpstreamlabs/bento:<version>` for both
# pipelines.
#
# One more image comes out of this file - the traffic generator - and its
# stage is at the bottom. saguin is not built here: docker-compose.yml runs
# its published image, ghcr.io/ifnesi/saguin, so the broker, the pipelines
# and a load share one network and the stack is `docker compose up` and
# nothing else.
#
# The alternative for saguin was leaving it on the host and reaching it
# through host.docker.internal. That was tried and abandoned, and it is worth
# recording why: it works on macOS and Windows and fails on Linux the moment
# a firewall is enabled, where container-to-host packets are dropped rather
# than refused - a scrape that times out with nothing in any log to say a
# firewall did it. Fixing that means telling the reader to open a port to a
# subnet Docker assigned, which is a different instruction on ufw, firewalld
# and nftables and no instruction at all on the other two operating systems.
# On the compose network there is no host, no gateway and no firewall.
#
# **Every stage builds from the repository root**, so docker-compose.yml
# names one context for both. The Bento stage needs nothing from it and
# fetches its own source; the traffic generator is copied out of the checkout.

# The toolchain Bento's own go.mod asks for. A newer one would work; naming
# the exact version means the build does not start depending on whatever
# `1.26` resolves to this month.
FROM docker.io/library/golang:1.27.1-alpine AS bento-build

RUN apk add --no-cache git

# **Pinned to a commit, not to the branch.** `--depth 1 origin main`
# would build whatever the branch tip is on the day somebody runs this, and
# a demo that quietly changed what it was demonstrating is the failure worth
# spending one extra line to avoid. github.com serves a fetch by object name,
# so the pin costs nothing in download size.
#
# Override both to try another revision:
#   docker compose build --build-arg BENTO_REF=<sha>
# The pin is #991's merge commit.
ARG BENTO_REPO=https://github.com/warpstreamlabs/bento.git
ARG BENTO_REF=7fae30cae18ef2af1db6cd3ba90876bc5c5b9ce2

WORKDIR /src
RUN git init -q . \
 && git remote add origin "$BENTO_REPO" \
 && git fetch -q --depth 1 origin "$BENTO_REF" \
 && git checkout -q FETCH_HEAD

RUN go mod download

# CGO off for a static binary on a base image with no libc of its own, and
# `timetzdata` because that base has no zoneinfo either - without the tag a
# pipeline using a named timezone fails at run time rather than at build
# time. Both are what Bento's own resources/docker/Dockerfile does.
#
# **What the binary reports as its version.** Bento takes it from an ldflag
# its Makefile sets from `git describe --tags`, and a one-commit fetch has
# no tags - so left alone this binary answers `bento --version` with a blank
# line, which is the worst of the three possible answers. It is 1.21.2 with
# the merged `mqtt_v5` on top, and it says so.
# The date is the pinned commit's, not the moment of the build, so two
# people building this get the same answer. Bento's own Makefile stamps
# `date` here; that would make the binary differ every time for no reason a
# reader of this demo cares about.
ARG BENTO_VERSION=1.21.2
RUN CGO_ENABLED=0 go build -trimpath -tags timetzdata \
      -ldflags "-w -s \
        -X main.Version=${BENTO_VERSION}+mqtt-v5.$(git rev-parse --short HEAD) \
        -X main.DateBuilt=$(git show -s --format=%cI HEAD)" \
      -o /out/bento ./cmd/bento


FROM docker.io/library/alpine:3.22 AS bento

# Bento reaches Kafka and MQTT over plaintext in this stack and needs no
# certificates for it, but the moment somebody uncomments the Confluent
# Cloud block at the bottom of MQTT2Kafka.yaml it does.
RUN apk add --no-cache ca-certificates \
 && adduser -D -H -u 10001 bento

COPY --from=bento-build /out/bento /usr/local/bin/bento

USER bento

# Bento's own default: /ready and /metrics, and what docker-compose.yml's
# health check asks.
EXPOSE 4195

ENTRYPOINT ["/usr/local/bin/bento"]


# --------------------------------------------------------------------------
# The traffic generator.
#
# It is a separate image rather than a script the broker runs, because they
# are separate things: one is saguin and the other is a client that happens
# to be driving it. Running a load generator inside the broker's own
# container would put them in one failure and one log.
FROM docker.io/library/python:3.13-alpine AS traffic

# The same constraint examples/requirements.txt uses, for the same reason:
# paho 2.0 changed the callback signatures, and a demonstration that breaks
# on a library upgrade teaches somebody the broker is broken.
RUN pip install --no-cache-dir --root-user-action=ignore "paho-mqtt==2.1.0"

COPY examples/bento-connectors/traffic.py /traffic.py

USER nobody
ENTRYPOINT ["python3", "-u", "/traffic.py"]


# --------------------------------------------------------------------------
# **The viewer is not built here, and no longer in this repository either.** It
# is `ifnesi/saguin-viewer`, and docker-compose.yml runs its published image,
# ghcr.io/ifnesi/saguin-viewer.
#
# It exists because MQTT Explorer is no window onto this stack: that image
# builds its connection with MQTT.js and never sets protocolVersion, so it
# asks for MQTT 3.1.1. saguin refused that outright when this stack was
# built and admits it today, which changes nothing here - a 3.1.1 client is
# handed no user properties, so the offsets, the broker timestamps and the
# `schema` pointer are all invisible to it. Control Center is a step
# downstream - it shows what reached Kafka, not what the devices sent.
#
# What that costs here, said rather than discovered: **this stack's payloads
# are protobuf, and the viewer shows them as hex rather than as decoded
# readings.** Not because it knows no schemas - it reads them out of saguin's
# registry, which is what makes it a viewer for any deployment rather than
# for this one - but because the protobuf toolchain has no musl wheels and
# would add ten minutes to a build on Alpine, so its image leaves it out.
# Topics, message properties, offsets and the channel each record landed in
# are all there, and Control Center shows the decoded form on the Kafka side.
