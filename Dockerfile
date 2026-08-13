FROM python:3.12-slim

ARG FCP_BUILD_COMMIT=unknown
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLBACKEND=Agg \
    FCP_TAILSCALE_DISCOVERY_FILE=/app/data/tailscale_discovery.json \
    FCP_BUILD_COMMIT=${FCP_BUILD_COMMIT}
LABEL no.fcp.build_commit=${FCP_BUILD_COMMIT}

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 5000

ENTRYPOINT ["python", "-m", "catalog.flask_app.app"]
