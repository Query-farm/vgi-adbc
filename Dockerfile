# Copyright 2026 Query Farm LLC - https://query.farm
#
# Single image serving BOTH transports of the vgi-adbc worker:
#   docker run ... IMG            -> HTTP server on $PORT (default 8000; /health, VGI RPC)
#   docker run -i ... IMG stdio   -> stdio worker DuckDB spawns on-host
# See docker-entrypoint.sh.
#
# The image bakes in: the worker + its embedded haybarn DuckDB, the adbc_scanner
# community extension (pre-installed at build so first ATTACH needs no network), and the
# PostgreSQL + SQLite + MySQL ADBC drivers (installed via `dbc` into the standard manifest
# path). `dbc` is kept in the image, so more drivers (mssql, trino, flightsql, …) can be
# added with `dbc install <name>` in a derived image (or at runtime for a live container).
# syntax=docker/dockerfile:1
FROM python:3.13-slim

ARG VERSION=0.0.0
ARG GIT_COMMIT=unknown
ARG SOURCE_URL=https://github.com/Query-farm/vgi-adbc

LABEL org.opencontainers.image.title="vgi-adbc" \
      org.opencontainers.image.description="Query PostgreSQL/SQLite/any ADBC database from DuckDB over VGI (stdio + HTTP)" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.licenses="MIT" \
      farm.query.vgi.transports='["http","stdio"]'

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8000

WORKDIR /app

# curl backs the HEALTHCHECK, the CI /health smoke, and the dbc installer.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install the worker + HTTP-serving extra from the LOCKFILE so the image ships the exact,
# reproducible dependency set (uv.lock) rather than whatever floats past the `>=` floors at
# build time. `uv export` pins every transitive dep (with hashes); the project itself is then
# installed --no-deps on top.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY vgi_adbc ./vgi_adbc
RUN uv export --frozen --no-dev --no-emit-project --extra serve -o /tmp/requirements.txt \
    && pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt \
    && pip install --no-cache-dir --no-deps . \
    && rm /tmp/requirements.txt

# Provision ADBC drivers into the standard manifest search path (~/.config/adbc/drivers,
# where adbc_scanner looks) and pre-install the adbc_scanner community extension so the
# first ATTACH does not need network access.
RUN curl -LsSf https://dbc.columnar.tech/install.sh | sh \
    && /root/.local/bin/dbc install postgresql \
    && /root/.local/bin/dbc install sqlite \
    && /root/.local/bin/dbc install mysql \
    && python -c "import haybarn; c=haybarn.connect(config={'allow_unsigned_extensions':'true'}); c.execute('INSTALL adbc_scanner FROM community'); c.execute('LOAD adbc_scanner'); print('adbc_scanner ready')"

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=8s \
    CMD curl -fsS "http://localhost:${PORT}/health" || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
