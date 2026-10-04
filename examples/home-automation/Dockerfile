# The two emulated radios that stand where hardware would.
#
# The broker and the viewer are NOT built here: docker-compose.yml runs their
# published images, ghcr.io/ifnesi/saguin and ghcr.io/ifnesi/saguin-viewer.

# --------------------------------------------------------------------------
FROM docker.io/library/golang:1.25-alpine AS build

# git, so the build can stamp what it built. Go reads the revision out of the
# checkout only when git is on PATH.
RUN apk add --no-cache git

WORKDIR /src

# The module graph first, so editing the radios does not re-download everything.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off: a static binary on a base image with no libc of its own.
RUN CGO_ENABLED=0 go build -trimpath -o /out/coordinator ./examples/home-automation/coordinator \
 && CGO_ENABLED=0 go build -trimpath -o /out/transmitter ./examples/home-automation/transmitter


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
