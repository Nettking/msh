# File: standalone_recorder.py
import requests
import json
import os
import time
from datetime import datetime
from threading import Thread, Event
import xml.etree.ElementTree as ET

# Configuration
SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000/current",
    "IG500": "http://192.168.200.249:5000/current",
    "VTC": "http://192.168.200.252:5000/current"
}

DATA_DIR = "data"
POLL_INTERVAL = 0.2       # 5 Hz polling
FLUSH_INTERVAL = 1.0      # Write to disk every 1s

buffer = []
last_sequence = {}
stop_event = Event()

def try_number(val):
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val

def extract_mtconnect_values(xml_text: str, include_condition=False) -> dict:
    """Extract key values from MTConnect XML, including 'UNAVAILABLE' entries."""
    out = {}
    try:
        root = ET.fromstring(xml_text)
        ns = {'m': root.tag.split('}')[0].strip('{')}

        # Header sequence
        header = root.find('.//m:Header', ns)
        if header is not None and 'lastSequence' in header.attrib:
            out['sequence'] = int(header.attrib['lastSequence'])

        # Extract from Samples and Events
        for section in ['Samples', 'Events']:
            for el in root.findall(f".//m:{section}/*", ns):
                key = el.attrib.get('name') or el.attrib.get('dataItemId') or el.tag
                text = el.text
                out[key] = try_number(text) if text is not None else None

        # Optional: extract Condition elements
        if include_condition:
            for el in root.findall(".//m:Condition/*", ns):
                key = el.attrib.get('name') or el.attrib.get('dataItemId') or el.tag
                status = el.tag.split('}')[-1]  # e.g., Normal, Unavailable, Fault
                out[key] = status

    except Exception as e:
        print(f"[Extractor] parse error: {e}")
    return out

def fetch_loop():
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
                seq = parsed.get('sequence')
                if seq is not None and seq != last_sequence.get(name):
                    parsed['timestamp'] = timestamp
                    parsed['machine'] = name
                    buffer.append(parsed)
                    last_sequence[name] = seq
                    print(f"[{timestamp}] {name}: new seq {seq}")
            except Exception as e:
                print(f"[{timestamp}] {name} fetch error: {e}")
        time.sleep(POLL_INTERVAL)

def flush_buffer_to_disk():
    global buffer
    if not buffer:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.join(DATA_DIR, f"{datetime.now():%Y-%m-%d}.jsonl")
    with open(filename, 'a') as f:
        for entry in buffer:
            f.write(json.dumps(entry) + "\n")
    print(f"[{datetime.now()}] Flushed {len(buffer)} entries.")
    buffer = []

def flush_loop():
    while not stop_event.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_buffer_to_disk()

def run():
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

if __name__ == '__main__':
    run()
