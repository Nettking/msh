"""Durable MTConnect recorder with optional browser-managed on/off control.

The recorder remains an independent service. In managed mode it watches files in
``data/source_state`` written by the Flask web UI. This allows recording to be
turned on and off without creating child processes inside Flask and without
stopping the Docker container.

Unmanaged/legacy mode remains available through the existing environment
variables.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional
from xml.etree import ElementTree as ET
import json
import logging
import os
import signal
import threading
import time

import requests


DEFAULT_SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000/current",
    "IG500": "http://192.168.200.251:5000/current",
    "VTC": "http://192.168.200.252:5000/current",
}


def _bool_from_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float_from_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_from_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _parse_sources_text(value: str) -> dict[str, str]:
    sources: dict[str, str] = {}
    for item in value.split(";"):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                "Recorder source entries must use MachineName=http://host:port/current format."
            )
        name, url = item.split("=", 1)
        name = name.strip()
        url = url.strip()
        if name and url:
            sources[name] = url
    return sources


def _sources_from_environment() -> dict[str, str]:
    json_text = os.getenv("MSH_RECORDER_SOURCES_JSON", "").strip()
    if json_text:
        payload = json.loads(json_text)
        if not isinstance(payload, dict):
            raise ValueError("MSH_RECORDER_SOURCES_JSON must be an object.")
        sources = {
            str(name).strip(): str(url).strip()
            for name, url in payload.items()
            if str(name).strip() and str(url).strip()
        }
        if not sources:
            raise ValueError("MSH_RECORDER_SOURCES_JSON contains no valid sources.")
        return sources

    list_text = os.getenv("MSH_RECORDER_SOURCES", "").strip()
    if list_text:
        sources = _parse_sources_text(list_text)
        if not sources:
            raise ValueError("MSH_RECORDER_SOURCES contains no valid sources.")
        return sources

    return dict(DEFAULT_SOURCES)


def try_number(val: Optional[str]) -> Any:
    if val is None:
        return None
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def extract_mtconnect_values(
    xml_text: str,
    include_condition: bool = False,
) -> dict[str, Any]:
    """Flatten one MTConnect response while retaining its lastSequence value."""

    out: dict[str, Any] = {}
    root = ET.fromstring(xml_text)

    if root.tag.startswith("{"):
        namespace_uri = root.tag.split("}", 1)[0].strip("{")
        namespace = {"m": namespace_uri}

        def query(tag: str) -> str:
            return f".//m:{tag}"

    else:
        namespace = {}

        def query(tag: str) -> str:
            return f".//{tag}"

    header = root.find(query("Header"), namespace)
    if header is not None and header.attrib.get("lastSequence") is not None:
        out["sequence"] = int(header.attrib["lastSequence"])

    for section in ("Samples", "Events"):
        for element in root.findall(query(section) + "/*", namespace):
            key = (
                element.attrib.get("name")
                or element.attrib.get("dataItemId")
                or element.tag.split("}")[-1]
            )
            text = element.text.strip() if element.text else None
            out[key] = try_number(text)

    if include_condition:
        for element in root.findall(query("Condition") + "/*", namespace):
            key = (
                element.attrib.get("name")
                or element.attrib.get("dataItemId")
                or element.tag.split("}")[-1]
            )
            out[key] = element.tag.split("}")[-1]

    return out


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
FLUSH_INTERVAL = _float_from_env("MSH_RECORDER_FLUSH_INTERVAL", 1.0)
REQUEST_TIMEOUT = _float_from_env("MSH_RECORDER_REQUEST_TIMEOUT", 1.0)
MAX_BUFFER_SIZE = _int_from_env("MSH_RECORDER_MAX_BUFFER_SIZE", 50_000)
CONFIG_REFRESH_INTERVAL = _float_from_env(
    "MSH_RECORDER_CONFIG_REFRESH_INTERVAL",
    1.0,
)
STATUS_INTERVAL = _float_from_env("MSH_RECORDER_STATUS_INTERVAL", 1.0)
BACKOFF_INITIAL = _float_from_env("MSH_RECORDER_BACKOFF_INITIAL", 0.5)
BACKOFF_MAX = _float_from_env("MSH_RECORDER_BACKOFF_MAX", 8.0)

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
    """One durable recorder worker controlled by desired-state files."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.buffer: list[dict[str, Any]] = []
        self.last_sequence: dict[str, int] = {}
        self.sources: dict[str, str] = {}
        self.source_status: dict[str, dict[str, Any]] = {}
        self.next_attempt_at: dict[str, float] = {}
        self.backoff: dict[str, float] = {}
        self.enabled = not MANAGED_MODE
        self.configuration_ready = not MANAGED_MODE
        self.poll_interval = _float_from_env("MSH_RECORDER_POLL_INTERVAL", 0.2)
        self.include_condition = _bool_from_env(
            "MSH_RECORDER_INCLUDE_CONDITION",
            False,
        )
        self.records_written = 0
        self.recording_started_at: str | None = None
        self.last_flush_at: str | None = None
        self.last_error = ""
        self.message = "Recorder service is starting."
        self.state = "starting"
        self.last_config_refresh = 0.0
        self.last_status_write = 0.0
        self.last_flush_monotonic = time.monotonic()
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="mtconnect")

    def load_state(self) -> None:
        payload = _read_json(STATE_FILE)
        sequences = payload.get("last_sequence") if isinstance(payload.get("last_sequence"), dict) else payload
        if isinstance(sequences, dict):
            for key, value in sequences.items():
                try:
                    self.last_sequence[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
        log.info("Restored sequence state for %s sources", len(self.last_sequence))

    def save_state(self) -> None:
        with self.lock:
            payload = {
                "schema": "msh.mtconnect_recorder.sequence_state.v2",
                "updated_at": _utc_now(),
                "last_sequence": dict(self.last_sequence),
            }
        _write_json_atomic(STATE_FILE, payload)

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
                include_condition = bool(settings.get("recorder_include_condition", False))
            else:
                enabled = True
                sources = _sources_from_environment()
                configuration_ready = bool(sources)
                poll_interval = _float_from_env("MSH_RECORDER_POLL_INTERVAL", 0.2)
                include_condition = _bool_from_env(
                    "MSH_RECORDER_INCLUDE_CONDITION",
                    False,
                )
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            with self.lock:
                self.configuration_ready = False
                self.last_error = f"Invalid recorder configuration: {exc}"
                self.state = "error"
                self.message = self.last_error
            log.error(self.last_error)
            return

        poll_interval = max(0.05, min(float(poll_interval), 60.0))
        with self.lock:
            previous_enabled = self.enabled
            previous_sources = dict(self.sources)
            self.enabled = enabled
            self.configuration_ready = configuration_ready
            self.poll_interval = poll_interval
            self.include_condition = include_condition
            self.sources = sources

            for name in sources:
                self.backoff.setdefault(name, BACKOFF_INITIAL)
                self.next_attempt_at.setdefault(name, 0.0)
                self.source_status.setdefault(
                    name,
                    {
                        "url": sources[name],
                        "last_success_at": None,
                        "last_error": "",
                        "last_sequence": self.last_sequence.get(name),
                    },
                )
                self.source_status[name]["url"] = sources[name]

            for removed in set(self.source_status) - set(sources):
                self.source_status.pop(removed, None)
                self.backoff.pop(removed, None)
                self.next_attempt_at.pop(removed, None)

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
                self.message = f"Polling {len(self.sources)} configured MTConnect source(s)."

    def fetch_source(self, name: str, url: str) -> tuple[str, bool, str]:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            body = response.text.strip()
            if not body:
                raise ValueError("empty response body")
            parsed = extract_mtconnect_values(
                body,
                include_condition=self.include_condition,
            )
            sequence = parsed.get("sequence")
            if sequence is None:
                raise ValueError("MTConnect Header did not contain lastSequence")
            sequence = int(sequence)
            timestamp = _utc_now()

            with self.lock:
                previous = self.last_sequence.get(name)
                if previous != sequence:
                    parsed.update(
                        {
                            "timestamp": timestamp,
                            "machine": name,
                            "machine_id": name,
                            "source": "mtconnect_recorder",
                        }
                    )
                    if len(self.buffer) >= MAX_BUFFER_SIZE:
                        drop_count = max(1, len(self.buffer) // 10)
                        del self.buffer[:drop_count]
                        log.warning(
                            "[%s] buffer full; dropped %s oldest rows",
                            name,
                            drop_count,
                        )
                    self.buffer.append(parsed)
                    self.last_sequence[name] = sequence

                source = self.source_status.setdefault(name, {})
                source.update(
                    {
                        "url": url,
                        "last_success_at": timestamp,
                        "last_error": "",
                        "last_sequence": sequence,
                    }
                )
                self.backoff[name] = BACKOFF_INITIAL
                self.next_attempt_at[name] = 0.0
            return name, True, ""
        except Exception as exc:  # noqa: BLE001 - recorder must continue other sources
            error = f"{type(exc).__name__}: {exc}"
            with self.lock:
                delay = min(self.backoff.get(name, BACKOFF_INITIAL) * 2, BACKOFF_MAX)
                self.backoff[name] = delay
                self.next_attempt_at[name] = time.monotonic() + delay
                source = self.source_status.setdefault(name, {"url": url})
                source.update(
                    {
                        "url": url,
                        "last_error": error,
                        "next_retry_seconds": delay,
                    }
                )
            log.warning("[%s] fetch error: %s; retrying in %.1fs", name, error, delay)
            return name, False, error

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
            self.executor.submit(self.fetch_source, name, url): name
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
                self.message = f"Polling {len(self.sources)} configured MTConnect source(s)."
                self.last_error = ""
            elif errors and len(due_sources) == len(self.sources):
                self.state = "error"
                self.last_error = "No configured MTConnect source responded successfully."
                self.message = self.last_error

    def flush_buffer(self) -> int:
        with self.lock:
            if not self.buffer:
                return 0
            rows = list(self.buffer)
            self.buffer.clear()

        written = 0
        failed_rows: list[dict[str, Any]] = []
        for entry in rows:
            machine = str(entry.get("machine") or "UNKNOWN")
            timestamp = str(entry.get("timestamp") or _utc_now())
            day = timestamp[:10]
            output_dir = (
                DATA_DIR
                / "sources"
                / "mtconnect_recorder"
                / "jsonl"
                / machine
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{day}.jsonl"
            try:
                with output_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
                written += 1
            except OSError as exc:
                failed_rows.append(entry)
                log.error("Failed to write %s: %s", output_path, exc)

        with self.lock:
            if failed_rows:
                self.buffer[:0] = failed_rows
                self.last_error = f"Failed to write {len(failed_rows)} telemetry row(s)."
            if written:
                self.records_written += written
                self.last_flush_at = _utc_now()

        if written:
            self.save_state()
            log.info("Flushed %s telemetry row(s)", written)
        return written

    def publish_status(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_status_write < STATUS_INTERVAL:
            return
        self.last_status_write = now
        with self.lock:
            payload = {
                "schema": "msh.mtconnect_recorder.status.v1",
                "heartbeat_at": _utc_now(),
                "managed": MANAGED_MODE,
                "enabled": self.enabled,
                "configuration_ready": self.configuration_ready,
                "state": self.state,
                "message": self.message,
                "recording_started_at": self.recording_started_at,
                "sources": sorted(self.sources),
                "source_status": dict(self.source_status),
                "records_buffered": len(self.buffer),
                "records_written": self.records_written,
                "last_flush_at": self.last_flush_at,
                "last_error": self.last_error,
                "poll_interval_seconds": self.poll_interval,
                "request_timeout_seconds": REQUEST_TIMEOUT,
            }
        _write_json_atomic(STATUS_FILE, payload)

    def run(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.load_state()
        self.refresh_configuration(force=True)
        self.publish_status(force=True)

        log.info("Recorder service started; managed=%s", MANAGED_MODE)
        log.info("Data directory: %s", DATA_DIR)
        if MANAGED_MODE:
            log.info("Control file: %s", CONTROL_FILE)
            log.info("Setup file: %s", SETTINGS_FILE)

        try:
            while not self.stop_event.is_set():
                cycle_started = time.monotonic()
                self.refresh_configuration()
                self.run_fetch_cycle()

                now = time.monotonic()
                if now - self.last_flush_monotonic >= FLUSH_INTERVAL:
                    self.flush_buffer()
                    self.last_flush_monotonic = now
                self.publish_status()

                with self.lock:
                    active = self.enabled and self.configuration_ready
                    interval = self.poll_interval if active else 0.25
                elapsed = time.monotonic() - cycle_started
                self.stop_event.wait(max(0.01, interval - elapsed))
        finally:
            self.flush_buffer()
            with self.lock:
                self.state = "stopped"
                self.message = "Recorder service is shutting down."
            self.publish_status(force=True)
            self.executor.shutdown(wait=True, cancel_futures=True)
            log.info("Recorder service stopped")

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
