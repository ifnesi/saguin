# Pinned, and not the host's Python: this image is built on whatever laptop
# happens to run it, and an image that tracks the host is an image that
# works on one machine.
FROM docker.io/library/python:3.12-slim

WORKDIR /app

# Dependencies before the application, so that editing a Python file does
# not re-resolve the wheels.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# **The embedding model is downloaded here, at build time, and lives in this
# layer.** Two reasons, and the second is the one that matters: an image
# that fetches its model on first run does not work where saguin usually
# lives, which is a box with no route to the internet; and a first run that
# silently downloads 130MB looks exactly like a hang behind a progress bar
# that has not moved.
#
# It is never committed - the repository ignores the cache directory, and
# this layer is the only copy.
ENV FASTEMBED_CACHE_PATH=/models
RUN python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

# The interface is React from a file, not from a CDN and not from npm.
# Vendored at build time for the reason the model is: saguin lives on boxes
# with no route to the internet, and a page that silently fails to load its
# framework is a blank rectangle with no error in it.
ADD https://unpkg.com/react@18.3.1/umd/react.production.min.js /app/static/vendor/react.js
ADD https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js /app/static/vendor/react-dom.js
ADD https://unpkg.com/htm@3.1.1/dist/htm.umd.js /app/static/vendor/htm.js
ADD https://unpkg.com/marked@14.1.3/marked.min.js /app/static/vendor/marked.js
RUN chmod -R a+r /app/static/vendor

COPY app/ app/
COPY static/ static/
COPY questions.yaml .

ENV PYTHONUNBUFFERED=1
EXPOSE 5005
CMD ["python", "-m", "app.main"]
