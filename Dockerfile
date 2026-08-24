FROM python:3.12.13-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLBACKEND=Agg \
    FCP_TAILSCALE_DISCOVERY_FILE=/app/data/tailscale_discovery.json

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY constraints-release.txt /app/constraints-release.txt
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.1 \
    && python -m pip install --no-cache-dir -r /app/requirements.txt -c /app/constraints-release.txt

COPY . /app

# The build commit is declared last on purpose. Everything above it is byte
# identical between builds of different commits, so it stays cached. Declaring
# the ARG above the dependency install -- where it used to be -- invalidated
# that layer and every layer after it on every changed commit, so each update
# re-ran the whole install and wrote roughly a gigabyte of fresh BuildKit cache
# per image. On the host that filled its drive, that cache reached 21 GB.
#
# The commit still reaches the image environment and label, so runtime
# verification against the exact target is unchanged.
ARG FCP_BUILD_COMMIT=unknown
ENV FCP_BUILD_COMMIT=${FCP_BUILD_COMMIT}
LABEL no.fcp.build_commit=${FCP_BUILD_COMMIT}

EXPOSE 5000

ENTRYPOINT ["python", "-m", "catalog.flask_app.app"]
