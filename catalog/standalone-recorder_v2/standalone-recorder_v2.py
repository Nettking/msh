"""
Poll MTConnect endpoints and persist new telemetry snapshots to machine/day JSONL files.

This recorder fetches MTConnect ``/current`` responses from configured machine
sources, extracts selected values into flat JSON records, suppresses duplicates
using MTConnect sequence numbers, buffers new records in memory, and flushes
them to disk periodically.

Pipeline
--------
1. Poll each configured MTConnect source at a fixed interval
2. Parse XML into a flat dictionary of telemetry values
3. Read ``Header.lastSequence`` when available
4. Record a new snapshot only when sequence has advanced
5. Buffer new records in memory
6. Flush buffered records to per-machine, per-day JSONL files
7. Persist last-seen sequence numbers to disk

Outputs
-------
- ``data/<machine>/<YYYY-MM-DD>.jsonl``:
    line-delimited JSON snapshots grouped by machine and local recording day
- ``recorder_state.json``:
    persisted last-seen sequence numbers used to reduce duplication across restarts

Important
---------
This is a practical recorder, not a fully fault-tolerant ingestion service.

In particular:
- records are buffered in memory until flushed
- local recorder time is stored as ``timestamp``
- duplicate suppression is based primarily on MTConnect sequence numbers
- state persistence reduces duplication on restart but does not guarantee exactly-once capture
- transient fetch/parse/write failures are handled pragmatically

Notes
-----
Configured source URLs are environment-specific and may require local editing.
"""

# File: standalone_recorder.py
import os
import json
import time
import signal
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from xml.etree import ElementTree as ET

import requests

# ===================== Configuration =====================

# MTConnect sources to poll.
# Keys are machine identifiers stored in output rows.
SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000/current",
    "IG500":     "http://192.168.200.249:5000/current",
    "VTC":       "http://192.168.200.252:5000/current",
}

# Root output directory for recorded JSONL files.
DATA_DIR = "data"

# Polling interval in seconds. 0.2 s is approximately 5 Hz.
POLL_INTERVAL = 0.2

# Flush buffered records to disk every 1 second.
FLUSH_INTERVAL = 1.0

# Per-request timeout when polling an MTConnect source.
REQUEST_TIMEOUT = 1.0

# Safety cap on in-memory buffered rows.
MAX_BUFFER_SIZE = 50_000

# Whether to also capture MTConnect Condition status values.
INCLUDE_CONDITION = False

# File used to persist last-seen sequence numbers across restarts.
STATE_FILE = "recorder_state.json"

# Per-source backoff when repeated fetches fail.
BACKOFF_INITIAL = 0.5
BACKOFF_MAX = 8.0

# ===================== Logging =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("recorder")

# ===================== Globals =====================

# Global shutdown signal shared by fetch/flush threads.
stop_event = threading.Event()

# Protects concurrent access to the shared in-memory buffer.
buffer_lock = threading.Lock()

# In-memory buffer of snapshots awaiting flush to disk.
buffer: list[dict] = []

# Last observed MTConnect sequence number per source.
last_sequence: Dict[str, int] = {}

# Per-source backoff duration used after fetch failures.
per_source_backoff: Dict[str, float] = {name: BACKOFF_INITIAL for name in SOURCES.keys()}

# Shared HTTP session for repeated polling requests.
session = requests.Session()


# ===================== Utilities =====================

def now_iso_utc() -> str:
    """
    Return the current UTC timestamp in ISO 8601 format.

    Returns
    -------
    str
        Current UTC timestamp as an ISO-formatted string.
    """
    return datetime.now(timezone.utc).isoformat()


def try_number(val: Optional[str]) -> Any:
    """
    Convert a string value to int or float when possible.

    Parameters
    ----------
    val : str | None
        Raw string value extracted from MTConnect XML.

    Returns
    -------
    Any
        Integer, float, original string, or None.

    Notes
    -----
    This is a permissive conversion helper intended for lightweight recording,
    not strict schema-aware typing.
    """
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
    """
    Create a directory if it does not already exist.

    Parameters
    ----------
    path : str
        Directory path to ensure exists.
    """
    os.makedirs(path, exist_ok=True)


def state_load() -> None:
    """
    Load persisted sequence state from disk if available.

    Behavior
    --------
    - Reads ``recorder_state.json`` when present
    - Restores per-source last-seen sequence numbers
    - Logs a warning if the file cannot be loaded

    Notes
    -----
    This reduces duplication after restart, but does not guarantee exact
    continuity across crashes or partial flushes.
    """
    global last_sequence
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                last_sequence.update({k: int(v) for k, v in data.items()})
                log.info(f"Restored last_sequence state for {len(last_sequence)} sources")
    except Exception as e:
        log.warning(f"Failed to load state file: {e}")


def state_save() -> None:
    """
    Persist current sequence state to disk atomically when possible.

    Notes
    -----
    The state is written through a temporary file and then replaced. Failures
    are logged but do not stop the recorder.
    """
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(last_sequence, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.warning(f"Failed to persist state: {e}")


# ===================== MTConnect parsing =====================

def extract_mtconnect_values(xml_text: str, include_condition: bool = False) -> dict:
    """
    Extract selected values from an MTConnect XML document.

    Parameters
    ----------
    xml_text : str
        Raw MTConnect XML response body.
    include_condition : bool, default=False
        If True, also extract Condition status elements.

    Returns
    -------
    dict
        Flat dictionary of extracted values.

    Behavior
    --------
    - Captures ``Header.lastSequence`` as ``sequence`` when available
    - Flattens ``Samples`` and ``Events`` into key/value pairs
    - Uses, in order of preference:
      - ``name``
      - ``dataItemId``
      - local XML tag name
      as the output key
    - Optionally records Condition status tags

    Notes
    -----
    Parsing errors return an empty dictionary and are logged at debug level.
    This function intentionally favors continued recording over strict failure.
    """
    out: Dict[str, Any] = {}
    try:
        root = ET.fromstring(xml_text)

        # Determine namespace if present, else use empty
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
            for el in root.findall(q(section) + "/*", ns):
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag.split("}")[-1]
                text = el.text.strip() if el.text else None
                out[key] = try_number(text)

        if include_condition:
            for el in root.findall(q("Condition") + "/*", ns):
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag.split("}")[-1]
                status = el.tag.split("}")[-1]
                out[key] = status

    except Exception as e:
        # Parsing errors can happen on malformed or partial responses.
        log.debug(f"[Extractor] parse error: {e}")
    return out


# ===================== Fetch / Flush loops =====================

def fetch_loop() -> None:
    """
    Poll all configured MTConnect sources and append new snapshots to the buffer.

    Behavior
    --------
    - Polls sources at approximately ``POLL_INTERVAL``
    - Uses per-source backoff after failures
    - Appends a record only when the MTConnect sequence advances
    - Adds recorder-side UTC timestamp and machine name before buffering

    Notes
    -----
    The stored ``timestamp`` is the local recorder observation time, not a
    source-provided event timestamp.
    """
    log.info("Fetch loop started")
    next_tick = time.monotonic()

    while not stop_event.is_set():
        ts = now_iso_utc()

        for name, url in SOURCES.items():
            if stop_event.is_set():
                break

            try:
                r = session.get(url, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                body = r.text.strip()
                if not body:
                    continue

                parsed = extract_mtconnect_values(body, include_condition=INCLUDE_CONDITION)
                seq = parsed.get("sequence")

                if seq is None:
                    # No sequence available: still emit a minimal record using the
                    # last known sequence as context.
                    parsed["sequence"] = last_sequence.get(name, -1)

                # Only record when sequence advances, which serves as duplicate suppression.
                if seq is None or seq != last_sequence.get(name):
                    parsed["timestamp"] = ts
                    parsed["machine"] = name

                    with buffer_lock:
                        if len(buffer) >= MAX_BUFFER_SIZE:
                            # Drop the oldest 10% of buffered records to keep recording.
                            drop = len(buffer) // 10 or 1
                            del buffer[:drop]
                            log.warning(f"[{name}] buffer full, dropped {drop} oldest entries")
                        buffer.append(parsed)

                    last_sequence[name] = parsed["sequence"]
                    per_source_backoff[name] = BACKOFF_INITIAL

            except Exception as e:
                # Exponential backoff avoids hammering failing endpoints.
                per_source_backoff[name] = min(per_source_backoff[name] * 2, BACKOFF_MAX)
                log.warning(f"[{name}] fetch error: {e} (backing off {per_source_backoff[name]:.1f}s)")
                time.sleep(per_source_backoff[name])

        # Maintain a steady polling cadence relative to monotonic time.
        next_tick += POLL_INTERVAL
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # If the loop is behind schedule, reset the target to avoid accumulating drift.
            next_tick = time.monotonic()


def flush_buffer_to_disk() -> None:
    """
    Flush buffered records to machine/day JSONL files.

    Behavior
    --------
    - Swaps the shared buffer into a local list under lock
    - Writes records under ``data/<machine>/<YYYY-MM-DD>.jsonl``
    - Saves recorder state after successful flush activity

    Notes
    -----
    The day bucket is derived from the recorded timestamp string by taking the
    first 10 characters (``YYYY-MM-DD``).
    """
    to_write: list[dict]
    with buffer_lock:
        if not buffer:
            return
        to_write, buffer[:] = buffer, []

    written = 0
    for entry in to_write:
        machine = entry.get("machine", "UNKNOWN")
        ts_str = entry.get("timestamp", now_iso_utc())
        try:
            day = ts_str[:10]
        except Exception:
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        dir_path = os.path.join(DATA_DIR, machine)
        ensure_dir(dir_path)
        file_path = os.path.join(dir_path, f"{day}.jsonl")

        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            written += 1
        except Exception as e:
            log.error(f"Failed to write entry to {file_path}: {e}")

    if written:
        log.info(f"Flushed {written} entries")
        state_save()


def flush_loop() -> None:
    """
    Periodically flush buffered records to disk until shutdown is requested.
    """
    log.info("Flush loop started")
    while not stop_event.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_buffer_to_disk()


# ===================== Signal handling =====================

def request_stop(signum=None, frame=None):
    """
    Request graceful recorder shutdown.

    Parameters
    ----------
    signum : int | None, optional
        Signal number if called from a signal handler.
    frame : Any, optional
        Current stack frame, unused.
    """
    if not stop_event.is_set():
        log.info(f"Stopping... (signal {signum})")
        stop_event.set()


# ===================== Main =====================

def run() -> None:
    """
    Start the recorder, worker threads, and graceful shutdown handling.

    Behavior
    --------
    - ensures output directory exists
    - restores persisted sequence state
    - installs SIGINT/SIGTERM handlers
    - starts fetch and flush worker threads
    - blocks until shutdown is requested
    - performs one final flush before exit
    """
    ensure_dir(DATA_DIR)
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