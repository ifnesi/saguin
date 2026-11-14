# The two emulated radios, built from this directory. The broker and the
# viewer are pulled as their published images; docker-compose.yml names them.

# Standard-library-only simulators, isolated from the broker's module.
FROM docker.io/library/golang:1.25-alpine AS radios-build
WORKDIR /src
COPY go.mod ./
COPY coordinator ./coordinator
COPY transmitter ./transmitter
RUN CGO_ENABLED=0 go build -trimpath -o /out/coordinator ./coordinator \
 && CGO_ENABLED=0 go build -trimpath -o /out/transmitter ./transmitter


# --------------------------------------------------------------------------
# The emulated radio. Answers where a coordinator would, so this demo needs
# no Zigbee hardware - and a real network-attached coordinator answers the
# same way, which is why zigbee2mqtt's configuration names an address here
# rather than a device file.
FROM docker.io/library/alpine:3.22 AS coordinator

COPY --from=radios-build /out/coordinator /usr/local/bin/coordinator

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

COPY --from=radios-build /out/transmitter /usr/local/bin/transmitter

USER nobody
EXPOSE 1234

ENTRYPOINT ["/usr/local/bin/transmitter"]
CMD ["-listen", "0.0.0.0:1234"]
