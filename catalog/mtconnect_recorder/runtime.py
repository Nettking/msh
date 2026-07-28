"""HTTP client and runtime loop for the sequence-based MTConnect recorder."""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests

from .model import (
    MtconnectProtocolError,
    ProbeModel,
    SourceCheckpoint,
    StoredBatch,
    _bool_from_env,
    _float_from_env,
    _int_from_env,
    _local_name,
    _parse_sources_text,
    _read_json,
    _sources_from_environment,
    _utc_now,
    _write_json_atomic,
    normalize_agent_base_url,
)
from .parsing import (
    parse_probe,
    parse_stream_header,
    parse_streams,
    plan_sequence,
    validate_batch_continuity,
)
from .storage import DurableRecorderStore


class MtconnectClient:
    def __init__(self, base_url: str, *, timeout: float) -> None:
        self.base_url = normalize_agent_base_url(base_url)
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, endpoint: str, *, params: Mapping[str, Any] | None = None) -> str:
        response = self.session.get(
            f"{self.base_url}/{endpoint}",
            params=dict(params or {}),
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.text.strip()
        if not body:
            raise MtconnectProtocolError(f"MTConnect /{endpoint} returned an empty body.")
        if _local_name(ET.fromstring(body).tag) == "MTConnectError":
            parse_stream_header(body)
        return body

    def fetch_current(self) -> str:
        return self._get("current")

    def fetch_probe(self) -> str:
        return self._get("probe")

    def fetch_sample(self, *, from_sequence: int, count: int) -> str:
        return self._get(
            "sample",
            params={"from": int(from_sequence), "count": int(count)},
        )


DATA_DIR = Path(os.getenv("MSH_RECORDER_DATA_DIR", "data"))
STATE_FILE = Path(
    os.getenv(
        "MSH_RECORDER_STATE_FILE",
        "data/source_state/mtconnect_recorder_state.json",
    )
)
MANAGED_MODE = _bool_from_env("MSH_RECORDER_MANAGED", False)
CONTROL_FILE = Path(
    os.getenv(
        "MSH_RECORDER_CONTROL_FILE",
        "data/source_state/mtconnect_recorder_control.json",
    )
)
SETTINGS_FILE = Path(
    os.getenv(
        "MSH_RECORDER_SETTINGS_FILE",
        "data/server_setup/server_settings.json",
    )
)
STATUS_FILE = Path(
    os.getenv(
        "MSH_RECORDER_STATUS_FILE",
        "data/source_state/mtconnect_recorder_status.json",
    )
)
LOG_FILE = Path(
    os.getenv(
        "MSH_RECORDER_LOG_FILE",
        "data/source_state/mtconnect_recorder.log",
    )
)
REQUEST_TIMEOUT = _float_from_env("MSH_RECORDER_REQUEST_TIMEOUT", 10.0)
BATCH_SIZE = max(1, _int_from_env("MSH_RECORDER_BATCH_SIZE", 1000))
MAX_BATCHES_PER_CYCLE = max(1, _int_from_env("MSH_RECORDER_MAX_BATCHES_PER_CYCLE", 20))
CONFIG_REFRESH_INTERVAL = _float_from_env("MSH_RECORDER_CONFIG_REFRESH_INTERVAL", 1.0)
STATUS_INTERVAL = _float_from_env("MSH_RECORDER_STATUS_INTERVAL", 1.0)
BACKOFF_INITIAL = _float_from_env("MSH_RECORDER_BACKOFF_INITIAL", 0.5)
BACKOFF_MAX = _float_from_env("MSH_RECORDER_BACKOFF_MAX", 8.0)
RUN_ONCE = _bool_from_env("MSH_RECORDER_ONCE", False)

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("recorder")
log.setLevel(logging.INFO)
log.handlers.clear()
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=2_000_000,
    backupCount=3,
    encoding="utf-8",
)
file_handler.setFormatter(formatter)
log.addHandler(console_handler)
log.addHandler(file_handler)


class RecorderRuntime:
    """Durable MTConnect worker controlled by environment or desired-state files."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.sources: dict[str, str] = {}
        self.checkpoints: dict[str, SourceCheckpoint] = {}
        self.probes: dict[str, ProbeModel] = {}
        self.source_status: dict[str, dict[str, Any]] = {}
        self.next_attempt_at: dict[str, float] = {}
        self.backoff: dict[str, float] = {}
        self.enabled = not MANAGED_MODE
        self.configuration_ready = not MANAGED_MODE
        self.poll_interval = _float_from_env("MSH_RECORDER_POLL_INTERVAL", 0.2)
        self.observations_written = 0
        self.raw_batches_written = 0
        self.gaps_detected = 0
        self.recording_started_at: str | None = None
        self.last_commit_at: str | None = None
        self.last_error = ""
        self.message = "Recorder service is starting."
        self.state = "starting"
        self.last_config_refresh = 0.0
        self.last_status_write = 0.0
        self.store = DurableRecorderStore(DATA_DIR)
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="mtconnect")

    def load_state(self) -> None:
        payload = _read_json(STATE_FILE)
        if payload.get("schema") != "msh.mtconnect_recorder.checkpoints.v3":
            if payload:
                log.warning(
                    "Ignoring legacy recorder state; the loss-aware recorder will start from "
                    "the earliest sequence still retained by each Agent."
                )
            return
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            return
        for name, item in sources.items():
            if not isinstance(item, dict):
                continue
            try:
                self.checkpoints[str(name)] = SourceCheckpoint.from_dict(str(name), item)
            except (KeyError, TypeError, ValueError):
                log.warning("Ignoring invalid checkpoint for source %s", name)
        log.info("Restored durable checkpoints for %s source(s)", len(self.checkpoints))

    def save_state(self) -> None:
        with self.lock:
            payload = {
                "schema": "msh.mtconnect_recorder.checkpoints.v3",
                "updated_at": _utc_now(),
                "sources": {
                    name: checkpoint.to_dict()
                    for name, checkpoint in sorted(self.checkpoints.items())
                },
            }
            _write_json_atomic(STATE_FILE, payload)

    def reconcile_checkpoint_aliases(self, sources: Mapping[str, str]) -> bool:
        """Keep sequence continuity when a source adopts its MTConnect UUID.

        Older manual setup commonly used a generic alias such as ``Mazak``.
        Network discovery replaces that alias with a stable identity from
        ``/probe``. A checkpoint is safe to move when exactly one inactive
        checkpoint has the same normalized Agent URL.
        """

        changed = False
        active_names = set(sources)
        with self.lock:
            for source_name, base_url in sources.items():
                if source_name in self.checkpoints:
                    continue
                candidates: list[tuple[str, SourceCheckpoint]] = []
                for old_name, checkpoint in self.checkpoints.items():
                    if old_name in active_names:
                        continue
                    try:
                        checkpoint_url = normalize_agent_base_url(checkpoint.base_url)
                    except ValueError:
                        checkpoint_url = checkpoint.base_url.strip().rstrip("/")
                    if checkpoint_url == normalize_agent_base_url(base_url):
                        candidates.append((old_name, checkpoint))
                if len(candidates) != 1:
                    continue
                old_name, checkpoint = candidates[0]
                storage_aliases = list(
                    dict.fromkeys([*checkpoint.storage_aliases, old_name])
                )
                self.checkpoints[source_name] = replace(
                    checkpoint,
                    source_name=source_name,
                    base_url=normalize_agent_base_url(base_url),
                    storage_aliases=storage_aliases,
                )
                del self.checkpoints[old_name]
                changed = True
                log.info(
                    "Moved recorder checkpoint alias %s -> %s for %s",
                    old_name,
                    source_name,
                    base_url,
                )
        return changed

    def refresh_configuration(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_config_refresh < CONFIG_REFRESH_INTERVAL:
            return
        self.last_config_refresh = now

        try:
            if MANAGED_MODE:
                control = _read_json(CONTROL_FILE)
                settings = _read_json(SETTINGS_FILE)
                enabled = bool(control.get("enabled", False))
                role = str(settings.get("deployment_mode") or "")
                sources_text = str(settings.get("recorder_sources") or "").strip()
                role_ready = role in {"full-server", "recorder-only"}
                sources = _parse_sources_text(sources_text) if sources_text else {}
                configuration_ready = bool(role_ready and sources)
                poll_interval = float(settings.get("recorder_poll_interval") or 0.2)
            else:
                enabled = True
                sources = _sources_from_environment()
                configuration_ready = bool(sources)
                poll_interval = _float_from_env("MSH_RECORDER_POLL_INTERVAL", 0.2)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            with self.lock:
                self.configuration_ready = False
                self.last_error = f"Invalid recorder configuration: {exc}"
                self.state = "error"
                self.message = self.last_error
            log.error(self.last_error)
            return

        poll_interval = max(0.05, min(float(poll_interval), 60.0))
        checkpoint_aliases_changed = self.reconcile_checkpoint_aliases(sources)
        with self.lock:
            previous_enabled = self.enabled
            previous_sources = dict(self.sources)
            self.enabled = enabled
            self.configuration_ready = configuration_ready
            self.poll_interval = poll_interval
            self.sources = sources

            for name, base_url in sources.items():
                self.backoff.setdefault(name, BACKOFF_INITIAL)
                self.next_attempt_at.setdefault(name, 0.0)
                source = self.source_status.setdefault(name, {})
                source.setdefault("last_success_at", None)
                source.setdefault("last_error", "")
                source.update({"base_url": base_url})

            for removed in set(self.source_status) - set(sources):
                self.source_status.pop(removed, None)
                self.backoff.pop(removed, None)
                self.next_attempt_at.pop(removed, None)
                self.probes.pop(removed, None)

            active = self.enabled and self.configuration_ready
            if active and not previous_enabled:
                self.recording_started_at = _utc_now()
                self.last_error = ""
                log.info("Recording enabled from web control")
            elif not active and previous_enabled:
                log.info("Recording disabled; recorder remains on standby")

            if previous_sources != sources:
                log.info("Recorder sources updated: %s", ", ".join(sorted(sources)) or "none")

            if not self.enabled:
                self.state = "stopped"
                self.message = "Recorder service is healthy and waiting. Recording is off."
            elif not self.configuration_ready:
                self.state = "error"
                self.message = "Recording is enabled, but recorder role or sources are not configured."
                self.last_error = self.message
            else:
                self.state = "recording"
                self.message = f"Recording {len(self.sources)} MTConnect source(s) by sequence."
        if checkpoint_aliases_changed:
            self.save_state()

    def _load_probe(
        self,
        *,
        source_name: str,
        client: MtconnectClient,
        instance_id: int,
        force: bool = False,
    ) -> ProbeModel:
        existing = self.probes.get(source_name)
        checkpoint = self.checkpoints.get(source_name)
        if existing is not None and not force:
            return existing
        xml_text = client.fetch_probe()
        probe = parse_probe(xml_text)
        self.store.store_probe(
            source_name=source_name,
            instance_id=instance_id,
            xml_text=xml_text,
            probe=probe,
        )
        self.probes[source_name] = probe
        if checkpoint and checkpoint.probe_sha256 and checkpoint.probe_sha256 != probe.sha256:
            self.store.record_event(
                source_name=source_name,
                event_type="probe_changed",
                payload={
                    "agent_instance_id": instance_id,
                    "previous_probe_sha256": checkpoint.probe_sha256,
                    "probe_sha256": probe.sha256,
                },
            )
        return probe

    def _commit_checkpoint(
        self,
        *,
        source_name: str,
        base_url: str,
        machine_id: str,
        instance_id: int,
        next_sequence: int,
        probe_sha256: str,
        stored: StoredBatch,
        count_raw_batch: bool = True,
    ) -> None:
        with self.lock:
            previous_checkpoint = self.checkpoints.get(source_name)
            checkpoint = SourceCheckpoint(
                source_name=source_name,
                base_url=base_url,
                machine_id=machine_id,
                agent_instance_id=instance_id,
                next_sequence=next_sequence,
                probe_sha256=probe_sha256,
                storage_aliases=(
                    list(previous_checkpoint.storage_aliases)
                    if previous_checkpoint is not None
                    else []
                ),
                last_raw_file=str(stored.raw_path),
                last_observation_file=str(stored.observation_path),
                last_normalized_file=str(stored.normalized_path),
                latest_values=stored.latest_values,
                updated_at=_utc_now(),
            )
            self.checkpoints[source_name] = checkpoint
            self.save_state()
            self.observations_written += stored.observation_count
            if count_raw_batch:
                self.raw_batches_written += 1
            self.last_commit_at = checkpoint.updated_at

    def _recover_archived_batches(
        self,
        *,
        source_name: str,
        base_url: str,
        instance_id: int,
        expected: int,
        probe: ProbeModel,
        archive_source_names: tuple[str, ...] | None = None,
    ) -> int:
        """Finish raw batches that were archived before a crash.

        Raw manifests are the recovery journal. A batch is only selected when
        its first sequence equals the current durable checkpoint, which makes
        recovery deterministic and prevents skipping an unresolved range.
        """

        lookup_names = archive_source_names or (source_name,)
        refs = []
        seen_manifests: set[Path] = set()
        for archive_source_name in lookup_names:
            for ref in self.store.iter_raw_batches(
                source_name=archive_source_name,
                instance_id=instance_id,
            ):
                if ref.manifest_path in seen_manifests:
                    continue
                seen_manifests.add(ref.manifest_path)
                refs.append(ref)
        while True:
            candidates = [ref for ref in refs if ref.first_sequence == expected]
            if not candidates:
                return expected
            ref = max(candidates, key=lambda item: (item.last_sequence, item.raw_sha256))
            xml_text = self.store.read_raw_batch(ref)
            batch = parse_streams(
                xml_text,
                source_name=source_name,
                probe=probe,
            )
            if batch.header.instance_id != instance_id:
                raise MtconnectProtocolError(
                    f"Archived batch instance mismatch in {ref.raw_path}."
                )
            validate_batch_continuity(batch, expected)
            active_checkpoint = self.checkpoints.get(source_name)
            initial_values = (
                active_checkpoint.latest_values
                if active_checkpoint
                and active_checkpoint.agent_instance_id == instance_id
                and active_checkpoint.next_sequence == expected
                else {}
            )
            observation_path = self.store.store_observation_batch(
                source_name=source_name,
                batch=batch,
            )
            normalized_path, latest_values = self.store.store_normalized_batch(
                source_name=source_name,
                batch=batch,
                initial_values=initial_values,
            )
            machine_id = str(batch.observations[0].get("machine_id") or source_name)
            stored = StoredBatch(
                raw_path=ref.raw_path,
                observation_path=observation_path,
                normalized_path=normalized_path,
                raw_sha256=ref.raw_sha256,
                observation_count=ref.observation_count,
                latest_values=latest_values,
            )
            self._commit_checkpoint(
                source_name=source_name,
                base_url=base_url,
                machine_id=machine_id,
                instance_id=instance_id,
                next_sequence=batch.header.next_sequence,
                probe_sha256=probe.sha256,
                stored=stored,
                count_raw_batch=False,
            )
            expected = batch.header.next_sequence
            log.warning(
                "[%s] recovered archived sequences %s-%s after an incomplete commit",
                source_name,
                batch.first_observation_sequence,
                batch.last_observation_sequence,
            )

    def capture_source(self, source_name: str, base_url: str) -> tuple[str, bool, str]:
        try:
            client = MtconnectClient(base_url, timeout=REQUEST_TIMEOUT)
            current_xml = client.fetch_current()
            current_header = parse_stream_header(current_xml)
            checkpoint = self.checkpoints.get(source_name)

            # Recover any raw batch that reached disk before a previous crash.
            if checkpoint is not None:
                archive_source_names = tuple(
                    dict.fromkeys([source_name, *checkpoint.storage_aliases])
                )
                archived_probe = None
                for archive_source_name in archive_source_names:
                    archived_probe = self.store.load_archived_probe(
                        source_name=archive_source_name,
                        instance_id=checkpoint.agent_instance_id,
                        probe_sha256=checkpoint.probe_sha256 or None,
                    )
                    if archived_probe is not None:
                        break
                if archived_probe is not None:
                    self._recover_archived_batches(
                        source_name=source_name,
                        base_url=base_url,
                        instance_id=checkpoint.agent_instance_id,
                        expected=checkpoint.next_sequence,
                        probe=archived_probe,
                        archive_source_names=archive_source_names,
                    )
                    checkpoint = self.checkpoints.get(source_name)
                    if checkpoint and checkpoint.agent_instance_id == current_header.instance_id:
                        self.probes[source_name] = archived_probe

            # Fetch and archive the current model. This is also required when a
            # state file was lost but raw batches survived.
            current_probe = self._load_probe(
                source_name=source_name,
                client=client,
                instance_id=current_header.instance_id,
                force=(
                    checkpoint is None
                    or checkpoint.agent_instance_id != current_header.instance_id
                ),
            )

            if checkpoint is None:
                archived = self.store.iter_raw_batches(
                    source_name=source_name,
                    instance_id=current_header.instance_id,
                )
                if archived:
                    earliest = min(ref.first_sequence for ref in archived)
                    self._recover_archived_batches(
                        source_name=source_name,
                        base_url=base_url,
                        instance_id=current_header.instance_id,
                        expected=earliest,
                        probe=current_probe,
                    )
                    checkpoint = self.checkpoints.get(source_name)

            plan = plan_sequence(checkpoint, current_header)

            if plan.reason in {"agent_instance_changed", "sequence_regression"}:
                self.store.record_event(
                    source_name=source_name,
                    event_type=plan.reason,
                    payload={
                        "previous_instance_id": (
                            checkpoint.agent_instance_id if checkpoint else None
                        ),
                        "previous_next_sequence": (
                            checkpoint.next_sequence if checkpoint else None
                        ),
                        "agent_instance_id": current_header.instance_id,
                        "agent_first_sequence": current_header.first_sequence,
                        "agent_last_sequence": current_header.last_sequence,
                    },
                )
                checkpoint = None
                current_probe = self._load_probe(
                    source_name=source_name,
                    client=client,
                    instance_id=current_header.instance_id,
                    force=True,
                )

            if plan.gap_from is not None and plan.gap_to is not None:
                self.store.record_gap(
                    source_name=source_name,
                    instance_id=current_header.instance_id,
                    missing_from=plan.gap_from,
                    missing_to=plan.gap_to,
                    reason=plan.reason or "unknown",
                    agent_first_sequence=current_header.first_sequence,
                    agent_last_sequence=current_header.last_sequence,
                )
                with self.lock:
                    self.gaps_detected += 1
                log.error(
                    "[%s] unavoidable MTConnect gap: %s-%s (%s)",
                    source_name,
                    plan.gap_from,
                    plan.gap_to,
                    plan.reason,
                )

            expected = plan.requested_from
            batches = 0
            last_machine_id = source_name
            compatibility_values = (
                checkpoint.latest_values
                if checkpoint is not None
                and checkpoint.agent_instance_id == current_header.instance_id
                and plan.gap_from is None
                else {}
            )

            while batches < MAX_BATCHES_PER_CYCLE and not self.stop_event.is_set():
                if expected > current_header.last_sequence:
                    break

                sample_xml = client.fetch_sample(
                    from_sequence=expected,
                    count=BATCH_SIZE,
                )
                batch = parse_streams(
                    sample_xml,
                    source_name=source_name,
                    probe=current_probe,
                )

                if batch.header.instance_id != current_header.instance_id:
                    self.store.record_event(
                        source_name=source_name,
                        event_type="agent_instance_changed_during_fetch",
                        payload={
                            "previous_instance_id": current_header.instance_id,
                            "agent_instance_id": batch.header.instance_id,
                        },
                    )
                    raise MtconnectProtocolError(
                        "MTConnect Agent instance changed while a batch was being fetched."
                    )

                if expected < batch.header.first_sequence:
                    self.store.record_gap(
                        source_name=source_name,
                        instance_id=batch.header.instance_id,
                        missing_from=expected,
                        missing_to=batch.header.first_sequence - 1,
                        reason="agent_buffer_overflow",
                        agent_first_sequence=batch.header.first_sequence,
                        agent_last_sequence=batch.header.last_sequence,
                    )
                    with self.lock:
                        self.gaps_detected += 1
                    expected = batch.header.first_sequence
                    compatibility_values = {}
                    continue

                if not batch.observations:
                    break

                validate_batch_continuity(batch, expected)
                stored = self.store.store_batch(
                    source_name=source_name,
                    requested_from=expected,
                    xml_text=sample_xml,
                    batch=batch,
                    initial_values=compatibility_values,
                )
                first_record = batch.observations[0]
                last_machine_id = str(first_record.get("machine_id") or source_name)
                self._commit_checkpoint(
                    source_name=source_name,
                    base_url=base_url,
                    machine_id=last_machine_id,
                    instance_id=batch.header.instance_id,
                    next_sequence=batch.header.next_sequence,
                    probe_sha256=current_probe.sha256,
                    stored=stored,
                )
                expected = batch.header.next_sequence
                compatibility_values = stored.latest_values
                batches += 1
                log.info(
                    "[%s] committed sequences %s-%s (%s observations)",
                    source_name,
                    batch.first_observation_sequence,
                    batch.last_observation_sequence,
                    stored.observation_count,
                )

                if expected > batch.header.last_sequence:
                    break

            timestamp = _utc_now()
            with self.lock:
                source = self.source_status.setdefault(source_name, {})
                active_checkpoint = self.checkpoints.get(source_name)
                source.update(
                    {
                        "base_url": base_url,
                        "last_success_at": timestamp,
                        "last_error": "",
                        "agent_instance_id": current_header.instance_id,
                        "agent_first_sequence": current_header.first_sequence,
                        "agent_last_sequence": current_header.last_sequence,
                        "next_sequence": (
                            active_checkpoint.next_sequence if active_checkpoint else expected
                        ),
                        "machine_id": (
                            active_checkpoint.machine_id
                            if active_checkpoint
                            else last_machine_id
                        ),
                        "probe_sha256": current_probe.sha256,
                        "buffer_size": current_header.buffer_size,
                        "caught_up": expected > current_header.last_sequence,
                    }
                )
                self.backoff[source_name] = BACKOFF_INITIAL
                self.next_attempt_at[source_name] = 0.0
            return source_name, True, ""
        except Exception as exc:  # noqa: BLE001 - recorder must continue other sources
            error = f"{type(exc).__name__}: {exc}"
            with self.lock:
                delay = min(self.backoff.get(source_name, BACKOFF_INITIAL) * 2, BACKOFF_MAX)
                self.backoff[source_name] = delay
                self.next_attempt_at[source_name] = time.monotonic() + delay
                source = self.source_status.setdefault(source_name, {"base_url": base_url})
                source.update(
                    {
                        "base_url": base_url,
                        "last_error": error,
                        "next_retry_seconds": delay,
                    }
                )
            log.warning("[%s] recorder error: %s; retrying in %.1fs", source_name, error, delay)
            return source_name, False, error

    def run_fetch_cycle(self) -> None:
        with self.lock:
            if not (self.enabled and self.configuration_ready and self.sources):
                return
            now = time.monotonic()
            due_sources = {
                name: url
                for name, url in self.sources.items()
                if now >= self.next_attempt_at.get(name, 0.0)
            }

        if not due_sources:
            return

        futures = {
            self.executor.submit(self.capture_source, name, url): name
            for name, url in due_sources.items()
        }
        successful = 0
        errors: list[str] = []
        for future in as_completed(futures):
            _, ok, error = future.result()
            if ok:
                successful += 1
            elif error:
                errors.append(error)

        with self.lock:
            if successful:
                self.state = "recording"
                self.message = f"Recording {len(self.sources)} MTConnect source(s) by sequence."
                self.last_error = ""
            elif errors and len(due_sources) == len(self.sources):
                self.state = "error"
                self.last_error = "No configured MTConnect source responded successfully."
                self.message = self.last_error

    def publish_status(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_status_write < STATUS_INTERVAL:
            return
        self.last_status_write = now
        with self.lock:
            payload = {
                "schema": "msh.mtconnect_recorder.status.v2",
                "heartbeat_at": _utc_now(),
                "managed": MANAGED_MODE,
                "enabled": self.enabled,
                "configuration_ready": self.configuration_ready,
                "state": self.state,
                "message": self.message,
                "recording_started_at": self.recording_started_at,
                "sources": sorted(self.sources),
                "source_status": dict(self.source_status),
                "records_buffered": 0,
                "records_written": self.observations_written,
                "observations_written": self.observations_written,
                "raw_batches_written": self.raw_batches_written,
                "gaps_detected": self.gaps_detected,
                "last_flush_at": self.last_commit_at,
                "last_commit_at": self.last_commit_at,
                "last_error": self.last_error,
                "poll_interval_seconds": self.poll_interval,
                "request_timeout_seconds": REQUEST_TIMEOUT,
                "batch_size": BATCH_SIZE,
                "max_batches_per_cycle": MAX_BATCHES_PER_CYCLE,
                "conditions_included": True,
                "raw_archive_enabled": True,
                "detailed_observation_archive_enabled": True,
                "wide_compatibility_jsonl_enabled": True,
            }
        _write_json_atomic(STATUS_FILE, payload)

    def run(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.load_state()
        self.refresh_configuration(force=True)
        self.publish_status(force=True)

        log.info("Loss-aware MTConnect recorder started; managed=%s", MANAGED_MODE)
        log.info("Data directory: %s", DATA_DIR)
        log.info("Batch size: %s", BATCH_SIZE)
        if MANAGED_MODE:
            log.info("Control file: %s", CONTROL_FILE)
            log.info("Setup file: %s", SETTINGS_FILE)

        try:
            while not self.stop_event.is_set():
                cycle_started = time.monotonic()
                self.refresh_configuration()
                self.run_fetch_cycle()
                self.publish_status()

                if RUN_ONCE:
                    break

                with self.lock:
                    active = self.enabled and self.configuration_ready
                    interval = self.poll_interval if active else 0.25
                elapsed = time.monotonic() - cycle_started
                self.stop_event.wait(max(0.01, interval - elapsed))
        finally:
            with self.lock:
                self.state = "stopped"
                self.message = "Recorder service is shutting down."
            self.publish_status(force=True)
            self.executor.shutdown(wait=True, cancel_futures=True)
            log.info("Loss-aware MTConnect recorder stopped")

    def request_stop(self, signum: int | None = None, frame: Any = None) -> None:
        del frame
        if not self.stop_event.is_set():
            log.info("Stopping recorder service (signal=%s)", signum)
            self.stop_event.set()


def run() -> None:
    runtime = RecorderRuntime()
    signal.signal(signal.SIGINT, runtime.request_stop)
    signal.signal(signal.SIGTERM, runtime.request_stop)
    runtime.run()


if __name__ == "__main__":
    run()
