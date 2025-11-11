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
SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000/current",
    "IG500":     "http://192.168.200.249:5000/current",
    "VTC":       "http://192.168.200.252:5000/current",
}

DATA_DIR = "data"                 # root output directory
POLL_INTERVAL = 0.2               # seconds (≈5 Hz)
FLUSH_INTERVAL = 1.0              # seconds
REQUEST_TIMEOUT = 1.0             # seconds per request
MAX_BUFFER_SIZE = 50_000          # safety valve
INCLUDE_CONDITION = False         # also capture Condition status
STATE_FILE = "recorder_state.json"  # persists last_sequence

# Backoff (per request) when a source is failing
BACKOFF_INITIAL = 0.5             # seconds
BACKOFF_MAX = 8.0                 # seconds

# ===================== Logging =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("recorder")

# ===================== Globals =====================
stop_event = threading.Event()
buffer_lock = threading.Lock()
buffer: list[dict] = []
last_sequence: Dict[str, int] = {}
per_source_backoff: Dict[str, float] = {name: BACKOFF_INITIAL for name in SOURCES.keys()}
session = requests.Session()

# ===================== Utilities =====================
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
    os.makedirs(path, exist_ok=True)

def state_load() -> None:
    """Load persisted last_sequence if available."""
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
    Extract key values from MTConnect XML.
    - Captures Header.lastSequence as 'sequence'
    - Flattens Samples and Events by element name or dataItemId
    - Optionally captures Condition status by name/dataItemId
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
                # prefer 'name', then 'dataItemId', else tag localname
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag.split("}")[-1]
                text = el.text.strip() if el.text else None
                out[key] = try_number(text)

        if include_condition:
            for el in root.findall(q("Condition") + "/*", ns):
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag.split("}")[-1]
                status = el.tag.split("}")[-1]  # e.g., Normal, Warning, Fault, Unavailable
                out[key] = status

    except Exception as e:
        # Parsing errors are common on partial responses; return empty dict
        log.debug(f"[Extractor] parse error: {e}")
    return out

# ===================== Fetch / Flush loops =====================
def fetch_loop() -> None:
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
                    # No sequence -> still record a heartbeat sample with minimal info
                    parsed["sequence"] = last_sequence.get(name, -1)
                # Only append if sequence advanced (prevents duplicates)
                if seq is None or seq != last_sequence.get(name):
                    parsed["timestamp"] = ts
                    parsed["machine"] = name
                    with buffer_lock:
                        if len(buffer) >= MAX_BUFFER_SIZE:
                            # Drop oldest chunk to keep moving
                            drop = len(buffer) // 10 or 1
                            del buffer[:drop]
                            log.warning(f"[{name}] buffer full, dropped {drop} oldest entries")
                        buffer.append(parsed)
                    last_sequence[name] = parsed["sequence"]
                    per_source_backoff[name] = BACKOFF_INITIAL  # reset backoff on success
            except Exception as e:
                # Backoff per source to avoid hammering a failing endpoint
                per_source_backoff[name] = min(per_source_backoff[name] * 2, BACKOFF_MAX)
                log.warning(f"[{name}] fetch error: {e} (backing off {per_source_backoff[name]:.1f}s)")
                time.sleep(per_source_backoff[name])

        # Maintain steady polling rate
        next_tick += POLL_INTERVAL
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # We are behind; reset next_tick to now to avoid drift
            next_tick = time.monotonic()

def flush_buffer_to_disk() -> None:
    to_write: list[dict]
    with buffer_lock:
        if not buffer:
            return
        to_write, buffer[:] = buffer, []

    # Group by machine and day and append to files
    written = 0
    for entry in to_write:
        machine = entry.get("machine", "UNKNOWN")
        ts_str = entry.get("timestamp", now_iso_utc())
        try:
            day = ts_str[:10]  # YYYY-MM-DD
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
        # Persist state after successful flush to minimize duplication on restart
        state_save()

def flush_loop() -> None:
    log.info("Flush loop started")
    while not stop_event.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_buffer_to_disk()

# ===================== Signal handling =====================
def request_stop(signum=None, frame=None):
    if not stop_event.is_set():
        log.info(f"Stopping... (signal {signum})")
        stop_event.set()

# ===================== Main =====================
def run() -> None:
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
        # Ensure final flush before exit
        flush_buffer_to_disk()
        log.info("Shutdown complete.")

if __name__ == "__main__":
    run()
