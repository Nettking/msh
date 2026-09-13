"""One bounded await/thread-frame capture; no locals, payloads or product edits."""
import asyncio
import datetime
import json
import pathlib
import sys
import threading
import time

h = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
c = h / '.acceptance/native-faults'
r = pathlib.Path('C:/wsl/fcp-v1-1aac6148-native-faults-20260913')
record = c / 'P06-publication-stacks.json'
assert not record.exists(), 'Inspect the existing bounded capture instead of repeating it'
assert json.loads((c / 'P06-supervision-status.json').read_text())['status'] == 'STOPPED'
sys.path.insert(0, str(r))
from scripts.start_tailscale_recorder import _install_remote_session_creator_fallback
from catalog.mtconnect_recorder.federation_node import RecorderFederationNode

def frame_row(frame):
    path = pathlib.Path(frame.f_code.co_filename)
    try:
        name = path.relative_to(r).as_posix()
    except ValueError:
        name = path.name
    row = {'file': name, 'function': frame.f_code.co_name, 'line': frame.f_lineno}
    if frame.f_code.co_name == '_publication_loop':
        # Fixed product stage enum only; never retain arbitrary frame locals.
        stage = frame.f_locals.get('retry_stage')
        if stage in {'connect', 'client-close', 'announce', 'status', 'selection', 'route-build', 'delivery', 'jsonl', 'pending-read'}:
            row['stage'] = stage
    return row

def task_rows():
    rows = []
    for task in asyncio.all_tasks():
        current = task.get_coro()
        chain = []
        for _ in range(32):
            frame = getattr(current, 'cr_frame', None) or getattr(current, 'gi_frame', None)
            if frame is not None:
                chain.append(frame_row(frame))
            current = getattr(current, 'cr_await', None) or getattr(current, 'gi_yieldfrom', None)
            if current is None:
                break
        rows.append({'done': task.done(), 'await_chain': chain})
    return rows

restore = _install_remote_session_creator_fallback()
node = RecorderFederationNode(data_directory=c / 'data', display_name='Federation v1 native fault acceptance', source_names=tuple(f's{i:02}' for i in range(1, 9)))
out = {'candidate': '1aac6148759d7b2fd488ec26b97e1a786bdafa80', 'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'samples': [], 'new_grant_requested': False, 'product_changed': False, 'physical_pass': False, 'protected_data_accessed': False}
try:
    node.bootstrap(None)
    start = time.monotonic()
    loop = node.runtime._start_loop()
    for offset in (0, 5, 20, 40):
        while time.monotonic() - start < offset:
            time.sleep(min(0.2, offset - (time.monotonic() - start)))
        snapshot = node.snapshot()
        sample = {'elapsed_seconds': round(time.monotonic() - start, 3), 'snapshot': {key: getattr(snapshot, key) for key in ('status', 'storage_state', 'jsonl_state', 'pending_batches', 'last_committed_count', 'last_error_code')}, 'threads': []}
        for frame in sys._current_frames().values():
            chain = []
            while frame is not None and len(chain) < 32:
                chain.append(frame_row(frame))
                frame = frame.f_back
            sample['threads'].append(chain)
        event = threading.Event()
        def capture(sample=sample, event=event):
            sample['tasks'] = task_rows()
            event.set()
        loop.call_soon_threadsafe(capture)
        sample['loop_callback_observed'] = event.wait(2)
        out['samples'].append(sample)
        print(json.dumps({'elapsed_seconds': sample['elapsed_seconds'], 'snapshot': sample['snapshot'], 'publication': [row for task in sample.get('tasks', []) for row in task['await_chain'] if row['function'] == '_publication_loop']}), flush=True)
    out['status'] = 'CAPTURED'
except Exception as exc:
    out.update(status='CAPTURE_REFUSED', error_type=type(exc).__name__, error_code=str(getattr(exc, 'code', '')))
finally:
    node.stop()
    restore()
    out['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    record.write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps({'status': out['status'], 'samples': len(out['samples']), 'physical_pass': False}), flush=True)
