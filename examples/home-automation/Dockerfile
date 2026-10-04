# Three images for this demo: the broker, and the two emulated radios that
# stand where hardware would.
#
# The viewer is NOT built here, and is not in this repository. It is
# ifnesi/saguin-viewer, which has a Dockerfile of its own, and
# docker-compose.yml builds from a checkout of it - beside saguin by default, or
# wherever SAGUIN_VIEWER_DIR points.

# --------------------------------------------------------------------------
FROM docker.io/library/golang:1.25-alpine AS build

# git, so the build can stamp what it built. Go reads the revision out of the
# checkout only when git is on PATH; without it saguin reports its version as
# "dev", and a demo whose version panel says "dev" answers the wrong question.
RUN apk add --no-cache git

WORKDIR /src

# The module graph first, so editing saguin does not re-download everything.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off, which costs nothing: saguin's SQLite provider is modernc.org/sqlite,
# Go rather than a C binding. What it buys is a static binary on a base image
# with no libc of its own.
RUN CGO_ENABLED=0 go build -trimpath -o /out/saguin ./cmd/saguin \
 && CGO_ENABLED=0 go build -trimpath -o /out/coordinator ./examples/home-automation/coordinator \
 && CGO_ENABLED=0 go build -trimpath -o /out/transmitter ./examples/home-automation/transmitter


# --------------------------------------------------------------------------
FROM docker.io/library/alpine:3.22 AS broker

# A shell on purpose: this is a thing to poke at. `docker compose exec saguin
# sh` and read the database, or run `saguin --passwd` against the credential
# file. Distroless would be right for a release and wrong here.
RUN adduser -D -H -u 10001 saguin \
 && mkdir -p /var/lib/saguin \
 && chown saguin:saguin /var/lib/saguin

COPY --from=build /out/saguin /usr/local/bin/saguin

USER saguin
WORKDIR /var/lib/saguin

EXPOSE 1883 9090

ENTRYPOINT ["/usr/local/bin/saguin"]
CMD ["--config", "/etc/saguin/saguin.yaml"]


# --------------------------------------------------------------------------
# The emulated radio. Answers where a coordinator would, so this demo needs
# no Zigbee hardware - and a real network-attached coordinator answers the
# same way, which is why zigbee2mqtt's configuration names an address here
# rather than a device file.
FROM docker.io/library/alpine:3.22 AS coordinator

COPY --from=build /out/coordinator /usr/local/bin/coordinator

USER nobody
EXPOSE 8888

ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["-listen", "0.0.0.0:8888"]



# --------------------------------------------------------------------------
# The other emulated radio, for the other gateway. rtl_433 takes its samples
# from a USB dongle or from an rtl_tcp server on the network, so this answers
# where the dongle would - the same swap the coordinator above makes for
# zigbee2mqtt, and for the same reason.
#
# What crosses this socket is radio: the sensors are modulated into samples
# here and demodulated by rtl_433, which has no idea anything is emulated.
FROM docker.io/library/alpine:3.22 AS transmitter

COPY --from=build /out/transmitter /usr/local/bin/transmitter

USER nobody
EXPOSE 1234

ENTRYPOINT ["/usr/local/bin/transmitter"]
CMD ["-listen", "0.0.0.0:1234"]
