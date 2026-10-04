# The guided tour, examples/demo.py, in a container: the published saguin
# binary beside this checkout's examples/, which is the layout demo.py
# expects (bin/saguin and examples/ under one root). It starts, stops and
# restarts that broker itself, so the broker runs inside this container
# rather than as a service of its own. docker-compose.yml beside this builds it.
FROM ghcr.io/ifnesi/saguin:0.1.0-rc.1 AS saguin

FROM docker.io/library/python:3.13-slim

WORKDIR /saguin

COPY examples/requirements.txt examples/requirements.txt
RUN pip install --no-cache-dir --root-user-action=ignore -r examples/requirements.txt

COPY examples/ examples/
COPY --from=saguin /usr/local/bin/saguin bin/saguin

# --skip-build: the binary is the published one above, not a local build.
ENTRYPOINT ["python3", "examples/demo.py", "--skip-build"]
