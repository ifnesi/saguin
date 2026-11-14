# The release image, ghcr.io/ifnesi/saguin, for linux/amd64 and linux/arm64.
#
# Nothing is compiled here. The release workflow (.github/workflows/release.yml)
# builds each architecture's binary natively with `make build`, checks it, and
# lays the build context out by platform:
#
#   linux/amd64/saguin
#   linux/arm64/saguin
#
# This file copies the one for the platform being built, so the image holds
# the same bytes as the release archive.
#
# The first stage runs on the build host, whatever the target, and only
# supplies what the final image cannot make itself, having no shell: the
# public CA bundle, so TLS to a bridge peer can verify its certificate, and
# the two directories the broker uses, owned by its unprivileged user.
FROM --platform=$BUILDPLATFORM docker.io/library/alpine:3.24 AS rootfs

RUN apk add --no-cache ca-certificates \
 && install -d -m 0755 -o 10001 -g 10001 /etc/saguin /var/lib/saguin

# The binary is static, so the image needs no C library or other files.
FROM scratch

# Set by the builder to the platform being built, for example linux/arm64.
ARG TARGETPLATFORM

# Go reads the CA bundle from this path on Linux.
COPY --from=rootfs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=rootfs --chown=10001:10001 /etc/saguin/ /etc/saguin/
COPY --from=rootfs --chown=10001:10001 /var/lib/saguin/ /var/lib/saguin/
COPY ${TARGETPLATFORM}/saguin /usr/local/bin/saguin

# The broker runs as an unprivileged user with no entry in /etc/passwd: the
# binary looks up no user by name.
USER 10001:10001

# SQLite writes temporary files to the working directory when the image has
# no /tmp, so it is the data directory, which the broker's user can write.
WORKDIR /var/lib/saguin

# Where a configuration's `file_path` should point, so the database outlives
# the container.
VOLUME ["/var/lib/saguin"]

# 1883 is the MQTT listener Sagüin opens when a configuration names none.
# 9090 is where the documentation puts the operations listener, which is off
# until a configuration enables it.
EXPOSE 1883 9090

# The configuration is mounted at /etc/saguin/saguin.yaml; a different
# command line (`--version`, `--check-config <file>`) replaces CMD.
ENTRYPOINT ["/usr/local/bin/saguin"]
CMD ["--config", "/etc/saguin/saguin.yaml"]
