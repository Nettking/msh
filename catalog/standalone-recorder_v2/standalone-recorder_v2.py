"""Poll MTConnect endpoints and persist new telemetry snapshots to JSONL.

The recorder can run alone as a server-side data-capture component, or together
with the Flask workbench in a full MSH server deployment. Runtime behavior is
configured through environment variables so the same repository can be deployed
as recorder-only, web-only, or full server.
"""

from __future__ import annotations

from datetime import datetime, timezone
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


def _sources_from_env() -> dict[str, str]:
    """Load MTConnect source map from JSON or a simple semicolon list.

    Supported forms:

    - ``MSH_RECORDER_SOURCES_JSON='{"IG500":"http://.../current"}'``
    - ``MSH_RECORDER_SOURCES='IG500=http://.../current;VTC=http://.../current'``

    If neither is set, the historical built-in lab defaults are used.
    """

    json_text = os.getenv("MSH_RECORDER_SOURCES_JSON", "").strip()
    if json_text:
        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid MSH_RECORDER_SOURCES_JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise SystemExit("MSH_RECORDER_SOURCES_JSON must be a JSON object of machine name to URL.")
        sources = {str(name).strip(): str(url).strip() for name, url in payload.items() if str(name).strip() and str(url).strip()}
        if not sources:
            raise SystemExit("MSH_RECORDER_SOURCES_JSON did not contain any valid machine URL entries.")
        return sources

    list_text = os.getenv("MSH_RECORDER_SOURCES", "").strip()
    if list_text:
        sources: dict[str, str] = {}
        for item in list_text.split(";"):
            if not item.strip():
                continue
            if "=" not in item:
                raise SystemExit("MSH_RECORDER_SOURCES entries must use MachineName=http://host:port/current format.")
            name, url = item.split("=", 1)
            name = name.strip()
            url = url.strip()
            if name and url:
                sources[name] = url
        if not sources:
            raise SystemExit("MSH_RECORDER_SOURCES did not contain any valid machine URL entries.")
        return sources

    return dict(DEFAULT_SOURCES)


SOURCES = _sources_from_env()
DATA_DIR = os.getenv("MSH_RECORDER_DATA_DIR", "data")
POLL_INTERVAL = _float_from_env("MSH_RECORDER_POLL_INTERVAL", 0.2)
FLUSH_INTERVAL = _float_from_env("MSH_RECORDER_FLUSH_INTERVAL", 1.0)
REQUEST_TIMEOUT = _float_from_env("MSH_RECORDER_REQUEST_TIMEOUT", 1.0)
MAX_BUFFER_SIZE = _int_from_env("MSH_RECORDER_MAX_BUFFER_SIZE", 50_000)
INCLUDE_CONDITION = _bool_from_env("MSH_RECORDER_INCLUDE_CONDITION", False)
STATE_FILE = os.getenv("MSH_RECORDER_STATE_FILE", "data/source_state/mtconnect_recorder_state.json")
BACKOFF_INITIAL = _float_from_env("MSH_RECORDER_BACKOFF_INITIAL", 0.5)
BACKOFF_MAX = _float_from_env("MSH_RECORDER_BACKOFF_MAX", 8.0)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("recorder")

stop_event = threading.Event()
buffer_lock = threading.Lock()
buffer: list[dict[str, Any]] = []
last_sequence: dict[str, int] = {}
per_source_backoff: dict[str, float] = {name: BACKOFF_INITIAL for name in SOURCES.keys()}
session = requests.Session()


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def state_load() -> None:
    global last_sequence
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            last_sequence.update({str(key): int(value) for key, value in data.items()})
            log.info("Restored last_sequence state for %s sources from %s", len(last_sequence), STATE_FILE)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to load state file %s: %s", STATE_FILE, exc)


def state_save() -> None:
    try:
        ensure_dir(os.path.dirname(STATE_FILE))
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(last_sequence, handle, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to persist state file %s: %s", STATE_FILE, exc)


def extract_mtconnect_values(xml_text: str, include_condition: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        root = ET.fromstring(xml_text)
        if root.tag.startswith("{"):
            ns_uri = root.tag.split("}")[0].strip("{")
            ns = {"m": ns_uri}

            def q(tag: str) -> str:
                return f".//m:{tag}"
        else:
            ns = {}

            def q(tag: str) -> str:
                return f".//{tag}"

        header = root.find(q("Header"), ns)
        if header is not None:
            seq = header.attrib.get("lastSequence")
            if seq is not None:
                out["sequence"] = int(seq)

        for section in ("Samples", "Events"):
            for element in root.findall(q(section) + "/*", ns):
                key = element.attrib.get("name") or element.attrib.get("dataItemId") or element.tag.split("}")[-1]
                text = element.text.strip() if element.text else None
                out[key] = try_number(text)

        if include_condition:
            for element in root.findall(q("Condition") + "/*", ns):
                key = element.attrib.get("name") or element.attrib.get("dataItemId") or element.tag.split("}")[-1]
                out[key] = element.tag.split("}")[-1]
    except Exception as exc:  # noqa: BLE001
        log.debug("[Extractor] parse error: %s", exc)
    return out


def fetch_loop() -> None:
    log.info("Fetch loop started for %s MTConnect sources", len(SOURCES))
    next_tick = time.monotonic()

    while not stop_event.is_set():
        ts = now_iso_utc()
        for name, url in SOURCES.items():
            if stop_event.is_set():
                break
            try:
                response = session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                body = response.text.strip()
                if not body:
                    continue

                parsed = extract_mtconnect_values(body, include_condition=INCLUDE_CONDITION)
                seq = parsed.get("sequence")
                if seq is None:
                    parsed["sequence"] = last_sequence.get(name, -1)

                if seq is None or seq != last_sequence.get(name):
                    parsed["timestamp"] = ts
                    parsed["machine"] = name
                    parsed["machine_id"] = name
                    parsed["source"] = "mtconnect_recorder"

                    with buffer_lock:
                        if len(buffer) >= MAX_BUFFER_SIZE:
                            drop = len(buffer) // 10 or 1
                            del buffer[:drop]
                            log.warning("[%s] buffer full, dropped %s oldest entries", name, drop)
                        buffer.append(parsed)

                    last_sequence[name] = int(parsed["sequence"])
                    per_source_backoff[name] = BACKOFF_INITIAL
            except Exception as exc:  # noqa: BLE001
                per_source_backoff[name] = min(per_source_backoff[name] * 2, BACKOFF_MAX)
                log.warning("[%s] fetch error: %s (backing off %.1fs)", name, exc, per_source_backoff[name])
                time.sleep(per_source_backoff[name])

        next_tick += POLL_INTERVAL
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.monotonic()


def flush_buffer_to_disk() -> None:
    with buffer_lock:
        if not buffer:
            return
        to_write, buffer[:] = list(buffer), []

    written = 0
    for entry in to_write:
        machine = str(entry.get("machine") or "UNKNOWN")
        ts_str = str(entry.get("timestamp") or now_iso_utc())
        day = ts_str[:10] if len(ts_str) >= 10 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dir_path = os.path.join(DATA_DIR, "sources", "mtconnect_recorder", "jsonl", machine)
        ensure_dir(dir_path)
        file_path = os.path.join(dir_path, f"{day}.jsonl")
        try:
            with open(file_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
            written += 1
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to write entry to %s: %s", file_path, exc)

    if written:
        log.info("Flushed %s entries", written)
        state_save()


def flush_loop() -> None:
    log.info("Flush loop started")
    while not stop_event.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_buffer_to_disk()


def request_stop(signum=None, frame=None) -> None:
    if not stop_event.is_set():
        log.info("Stopping recorder... (signal %s)", signum)
        stop_event.set()


def run() -> None:
    ensure_dir(DATA_DIR)
    ensure_dir(os.path.dirname(STATE_FILE))
    log.info("Recorder data dir: %s", DATA_DIR)
    log.info("Recorder state file: %s", STATE_FILE)
    for name, url in SOURCES.items():
        log.info("Recorder source: %s -> %s", name, url)
    state_load()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    t_fetch = threading.Thread(target=fetch_loop, name="fetch", daemon=True)
    t_flush = threading.Thread(target=flush_loop, name="flush", daemon=True)
    t_fetch.start()
    t_flush.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.2)
    finally:
        flush_buffer_to_disk()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    run()
