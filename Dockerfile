FROM python:3.12.13-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ARG FCP_BUILD_COMMIT=unknown
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLBACKEND=Agg \
    FCP_TAILSCALE_DISCOVERY_FILE=/app/data/tailscale_discovery.json \
    FCP_BUILD_COMMIT=${FCP_BUILD_COMMIT}
LABEL no.fcp.build_commit=${FCP_BUILD_COMMIT}

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY constraints-release.txt /app/constraints-release.txt
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.1 \
    && python -m pip install --no-cache-dir -r /app/requirements.txt -c /app/constraints-release.txt

COPY . /app

EXPOSE 5000

ENTRYPOINT ["python", "-m", "catalog.flask_app.app"]
