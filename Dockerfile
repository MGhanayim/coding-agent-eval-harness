FROM ubuntu:24.04

# Only the docker CLI is needed (mini-swe-agent shells out to `docker`;
# swebench talks to the mounted socket via docker-py) — the static client
# from the official image is ~50 MB vs several hundred for docker.io's
# full engine (dockerd/containerd/runc that would never run in here).
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mlops-assignment

COPY pyproject.toml .
COPY uv.lock .
COPY .python-version .

# --no-dev keeps pytest/ruff out of the runtime image; the cache mount keeps
# uv's wheel/http cache out of the layer (and speeds up rebuilds).
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev

ENV PATH="/mlops-assignment/.venv/bin:$PATH"

COPY pipeline pipeline/
