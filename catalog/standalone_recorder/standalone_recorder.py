"""
Poll MTConnect endpoints and record new telemetry snapshots to daily JSONL files.

This script periodically fetches MTConnect XML from configured machine sources,
extracts selected values, suppresses repeated snapshots using MTConnect sequence
numbers, buffers new records in memory, and flushes them to disk at regular
intervals.

Pipeline
--------
1. Poll each configured MTConnect ``/current`` endpoint at a fixed interval
2. Parse XML into a flat dictionary of values
3. Keep only snapshots whose MTConnect sequence number has changed
4. Add local timestamp and machine name
5. Buffer new records in memory
6. Flush buffered records to a daily JSONL file at a separate interval

Outputs
-------
- ``data/YYYY-MM-DD.jsonl``:
    line-delimited JSON snapshots recorded on that day

Important
---------
This is a lightweight recorder intended for practical data capture, not a
fault-tolerant ingestion service. In particular:

- buffering is in-memory only until flushed
- no locking is used around the shared buffer
- duplicate suppression is based on MTConnect ``lastSequence`` only
- local wall-clock time is recorded, not source-provided event time

Notes
-----
The configured MTConnect endpoints are environment-specific and may require
local editing.
"""

import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from threading import Event, Thread

import requests

# MTConnect sources to poll.
# Keys are machine names recorded in the output JSONL rows.
SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000/current",
    "IG500": "http://192.168.200.251:5000/current",
    "VTC": "http://192.168.200.252:5000/current",
}

# Directory where daily JSONL files are written.
DATA_DIR = "data"

# Polling interval in seconds.
# 0.2 seconds corresponds to 5 Hz polling.
POLL_INTERVAL = 0.2

# Flush buffered rows to disk every 1 second.
FLUSH_INTERVAL = 1.0

# Shared in-memory buffer of newly observed records awaiting disk flush.
buffer = []

# Tracks the last seen MTConnect sequence number per machine so repeated
# snapshots can be skipped.
last_sequence = {}

# Global shutdown signal for both worker threads.
stop_event = Event()


def try_number(val):
    """
    Convert a string value to int or float when possible.

    Parameters
    ----------
    val : Any
        Raw XML text value.

    Returns
    -------
    int | float | Any
        Numeric conversion when possible, otherwise the original value.

    Notes
    -----
    This is a permissive convenience conversion used during extraction. It does
    not attempt strict schema-aware typing.
    """
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def extract_mtconnect_values(xml_text: str, include_condition: bool = False) -> dict:
    """
    Extract a flat dictionary of values from MTConnect XML.

    Parameters
    ----------
    xml_text : str
        Raw MTConnect XML response body.
    include_condition : bool, default=False
        If True, also extract Condition elements and record their status tags.

    Returns
    -------
    dict
        Extracted telemetry values, including ``sequence`` when available.

    Behavior
    --------
    - Reads ``lastSequence`` from the MTConnect Header when present.
    - Extracts all child elements under ``Samples`` and ``Events``.
    - Uses, in order of preference:
      - ``name``
      - ``dataItemId``
      - raw XML tag
      as the output key.
    - Converts values to int/float when possible using ``try_number``.

    Notes
    -----
    This function flattens XML into a simple dictionary for recording. It does
    not preserve full MTConnect structure or namespace information.
    """
    out = {}
    try:
        root = ET.fromstring(xml_text)
        ns = {"m": root.tag.split("}")[0].strip("{")}

        # MTConnect Header contains the global sequence marker used here for
        # duplicate suppression.
        header = root.find(".//m:Header", ns)
        if header is not None and "lastSequence" in header.attrib:
            out["sequence"] = int(header.attrib["lastSequence"])

        # Extract values from Samples and Events.
        for section in ["Samples", "Events"]:
            for el in root.findall(f".//m:{section}/*", ns):
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag
                text = el.text
                out[key] = try_number(text) if text is not None else None

        # Optionally include Condition elements using their status tag names
        # (e.g. Normal, Unavailable, Fault) as the recorded value.
        if include_condition:
            for el in root.findall(".//m:Condition/*", ns):
                key = el.attrib.get("name") or el.attrib.get("dataItemId") or el.tag
                status = el.tag.split("}")[-1]
                out[key] = status

    except Exception as e:
        print(f"[Extractor] parse error: {e}")

    return out


def fetch_loop():
    """
    Poll all configured MTConnect sources and append new snapshots to the buffer.

    Behavior
    --------
    - Polls each source every ``POLL_INTERVAL`` seconds
    - Parses XML responses into flat dictionaries
    - Appends a record only when the source sequence number has changed
    - Adds local timestamp and machine name before buffering

    Notes
    -----
    This loop records the local polling time, not a source-provided event time.
    Duplicate suppression is based only on the most recent ``sequence`` value
    seen per machine.
    """
    global buffer

    print("Fetching at 5Hz...")
    while not stop_event.is_set():
        timestamp = datetime.now().isoformat()

        for name, url in SOURCES.items():
            try:
                r = requests.get(url, timeout=1)
                r.raise_for_status()
                body = r.text.strip()
                if not body:
                    continue

                parsed = extract_mtconnect_values(body, include_condition=False)
                seq = parsed.get("sequence")

                if seq is not None and seq != last_sequence.get(name):
                    parsed["timestamp"] = timestamp
                    parsed["machine"] = name
                    buffer.append(parsed)
                    last_sequence[name] = seq
                    print(f"[{timestamp}] {name}: new seq {seq}")

            except Exception as e:
                print(f"[{timestamp}] {name} fetch error: {e}")

        time.sleep(POLL_INTERVAL)


def flush_buffer_to_disk():
    """
    Flush the current in-memory buffer to the daily JSONL output file.

    Behavior
    --------
    - Does nothing if the buffer is empty
    - Writes all buffered rows to ``data/YYYY-MM-DD.jsonl``
    - Clears the buffer after writing

    Notes
    -----
    The output file is chosen using the local current date at flush time.
    """
    global buffer

    if not buffer:
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.join(DATA_DIR, f"{datetime.now():%Y-%m-%d}.jsonl")

    with open(filename, "a") as f:
        for entry in buffer:
            f.write(json.dumps(entry) + "\n")

    print(f"[{datetime.now()}] Flushed {len(buffer)} entries.")
    buffer = []


def flush_loop():
    """
    Periodically flush buffered records to disk until shutdown is requested.
    """
    while not stop_event.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_buffer_to_disk()


def run():
    """
    Start the recorder and keep it running until interrupted.

    This starts:
    - one polling thread
    - one flushing thread

    On KeyboardInterrupt:
    - signals both threads to stop
    - waits for them to finish
    - performs one final flush to disk
    """
    fetch_thread = Thread(target=fetch_loop)
    flush_thread = Thread(target=flush_loop)

    fetch_thread.start()
    flush_thread.start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Stopping...")
        stop_event.set()
        fetch_thread.join()
        flush_thread.join()
        flush_buffer_to_disk()


if __name__ == "__main__":
    run()