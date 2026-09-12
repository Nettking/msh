"""Bounded public-API diagnostic; only an owned temporary artifact root is used."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

SOURCE = '2a9c9b8eb53edff74c2de23570ec56e054d29b22'
REPO = Path('C:/wsl/fcp-v1-2a9c9b8e-merged-main-20260912')
OUTPUT = Path(__file__).resolve().parent
AUDIT = Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip() == SOURCE
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO, text=True).strip()
sys.path.insert(0, str(REPO))
from catalog.capabilities.analysis.content_store import LocalArtifactContentStore

owned = Path(tempfile.mkdtemp(prefix='d08-resolve-', dir=AUDIT))
root = Path('\\\\?\\' + str(owned / 'artifacts'))
store = LocalArtifactContentStore(root, chunk_size=4)
key = 'analysis/session/work/plan.json'
payload = b'unchanged payload' * 100
identity = store.write_bytes(key, payload)
stop = threading.Event()
failures = []
exceptions = []
counts = {'writes': 0, 'resolves': 0}
deadline = time.monotonic() + 25

def trace(frame, event, arg):
    if frame.f_code.co_name != 'resolve' or frame.f_code.co_filename != str(REPO / 'catalog/capabilities/analysis/content_store.py'):
        return None
    if event == 'exception':
        exception = arg[1]
        if getattr(exception, 'code', None) == 'artifact-object-key-escape':
            failures.append(dict(root=str(frame.f_locals['self'].root),
                key=frame.f_locals.get('key'), candidate=str(frame.f_locals.get('candidate')),
                error=str(exception)))
            stop.set()
    return trace

def writer():
    try:
        for _ in range(500):
            if stop.is_set() or time.monotonic() >= deadline:
                break
            store.write_bytes(key, payload)
            counts['writes'] += 1
    except Exception as error:
        exceptions.append(dict(thread='writer', type=type(error).__name__, error=str(error)))
        stop.set()
    finally:
        stop.set()

def reader():
    try:
        while not stop.is_set() and time.monotonic() < deadline:
            store.resolve(key)
            counts['resolves'] += 1
    except Exception as error:
        exceptions.append(dict(thread='reader', type=type(error).__name__, error=str(error)))
        stop.set()

threading.settrace(trace)
try:
    with closing(store.stream(key, **vars(identity))) as held_reader:
        next(held_reader)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(reader) for _ in range(3)]
            futures.append(pool.submit(writer))
            for future in futures:
                future.result(timeout=40)
finally:
    threading.settrace(None)
    report = dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source_commit=SOURCE, diagnostic_only=True, source_modified=False,
        command='audit Python -B handoff/diagnostics/reproduce_d08_resolve.py',
        owned_root=str(owned), counts=counts, failures=failures, exceptions=exceptions,
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        protected_recorder_data='UNTOUCHED', physical_runtime_changed=False)
    (OUTPUT / 'D08-focused-resolve.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))
