# The image goreleaser builds: GoReleaser puts each target's binary in the
# build context under its platform (linux/amd64/saguin, linux/arm64/saguin).
#
# Nothing runs on the target architecture. The first stage runs on the
# build host and only supplies the public CA bundle (so TLS to a bridge peer
# or a database can verify certificates) and two directories owned by the
# unprivileged user, which the final image, with no shell, cannot make.
FROM --platform=$BUILDPLATFORM alpine:3.22 AS rootfs

RUN apk add --no-cache ca-certificates \
 && install -d -o 10001 -g 10001 /etc/saguin /var/lib/saguin

FROM scratch

ARG TARGETPLATFORM

COPY --from=rootfs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=rootfs --chown=10001:10001 /etc/saguin/ /etc/saguin/
COPY --from=rootfs --chown=10001:10001 /var/lib/saguin/ /var/lib/saguin/
COPY ${TARGETPLATFORM}/saguin /usr/local/bin/saguin

USER 10001:10001
WORKDIR /var/lib/saguin

# Where a configuration's SQLite file_path and snapshot_dir should point.
VOLUME ["/var/lib/saguin"]

# 1883 is the MQTT port Sagüin opens when a configuration names no door.
# 9090 is where the documentation puts the operations listener, which is
# off until a configuration enables it.
EXPOSE 1883 9090

ENTRYPOINT ["/usr/local/bin/saguin"]
CMD ["--config", "/etc/saguin/saguin.yaml"]
