FROM python:3.12.13-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 MPLBACKEND=Agg
WORKDIR /opt/fcp-reviewer
COPY requirements.txt constraints-phase2.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.1 \
    && python -m pip install --no-cache-dir -r requirements.txt -c constraints-phase2.txt
COPY demo/icse/reviewer/prebuilt.py ./prebuilt.py
ARG FCP_BUILD_COMMIT=unknown
ENV FCP_BUILD_COMMIT=$FCP_BUILD_COMMIT
LABEL no.fcp.build_commit=$FCP_BUILD_COMMIT
ENTRYPOINT ["python", "-B", "/opt/fcp-reviewer/prebuilt.py"]
