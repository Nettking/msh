"""Read one original ICSE worker log; export only allowlisted frame metadata."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess

TARGET = Path('/mnt/c/actions-runner/_work/_temp/fcp-icse-network-35732223068-1-Windows/private-state/voter-c/worker.stderr.log')
LIMIT = 1024 * 1024
BASE = '5d5d7e8f215717cf9b7012d3b357eaf03a1400c9'
result = {
    'observed_at': datetime.now(timezone.utc).isoformat(),
    'original_run_id': 35732223068, 'original_attempt': 1,
    'original_job_id': 106760391543, 'original_runner': 'Beast',
    'product_head': BASE,
    'original_tested_merge': 'e9a23147795b73eea7bb73f3f0b4e416e3e3b6fd',
    'target': str(TARGET), 'read_only': True, 'test_execution': False,
    'raw_log_exported': False, 'observation': None,
}

def identity(value):
    return {key: getattr(value, key) for key in ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')}

def observe():
    assert os.environ['RUNNER_NAME'] == 'Beast-Linux-WSL'
    assert os.environ['RUNNER_OS'] == 'Linux'
    assert os.environ['GITHUB_EVENT_NAME'] == 'workflow_dispatch'
    assert os.environ['GITHUB_ACTOR'] == 'Nettking'
    assert os.environ['GITHUB_REPOSITORY'] == 'Nettking/msh'
    assert os.environ['GITHUB_RUN_ATTEMPT'] == '1'
    result['reader_run_id'] = os.environ['GITHUB_RUN_ID']
    result['reader_attempt'] = os.environ['GITHUB_RUN_ATTEMPT']
    result['reader_source'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    assert result['reader_source'] == os.environ['GITHUB_SHA']
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD^'], text=True).strip() == BASE
    allowed_changes = {'.github/workflows/nitro-artifact-smoke.yml', 'scripts/read_original_icse_stderr.py'}
    changes = set(subprocess.check_output(['git', 'diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD'], text=True).splitlines())
    assert changes == allowed_changes
    # Keep every ancestor open and refuse symlinks, including intermediate ones.
    descriptors = []
    try:
        directory = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        descriptors.append(directory)
        for component in TARGET.parts[1:-1]:
            directory = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            descriptors.append(directory)
        before = os.stat(TARGET.name, dir_fd=directory, follow_symlinks=False)
        assert stat.S_ISREG(before.st_mode)
        result['file_before'] = identity(before)
        interval_start = datetime(2026, 9, 22, 13, 29, 40, tzinfo=timezone.utc).timestamp()
        interval_end = datetime(2026, 9, 22, 13, 31, 16, tzinfo=timezone.utc).timestamp()
        if not interval_start <= before.st_mtime <= interval_end:
            result['observation'] = 'mtime_outside_original_job_no_read'
            return
        if before.st_size > LIMIT:
            result['observation'] = 'size_limit_no_read'
            return
        fd = os.open(TARGET.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            assert identity(before) == identity(opened)
            raw = stream.read(LIMIT + 1)
            after = os.fstat(stream.fileno())
        named_after = os.stat(TARGET.name, dir_fd=directory, follow_symlinks=False)
        result['file_after'] = identity(after)
        if identity(before) != identity(after) or identity(before) != identity(named_after) or len(raw) != before.st_size:
            result['observation'] = 'changed_during_read_no_trace_export'
            return
        result['sha256'] = hashlib.sha256(raw).hexdigest()
        tracked = subprocess.check_output(['git', 'ls-files', '*.py'], text=True).splitlines()
        frames = []
        exceptions = []
        ordered = []
        for line in raw.decode('utf-8', errors='replace').splitlines():
            if line == 'Traceback (most recent call last):':
                ordered.append({'kind': 'traceback_start'})
            elif line == 'During handling of the above exception, another exception occurred:':
                ordered.append({'kind': 'exception_context'})
            elif line == 'The above exception was the direct cause of the following exception:':
                ordered.append({'kind': 'direct_cause'})
            match = re.fullmatch(r'\s*File "([^"]+)", line ([0-9]+), in ([A-Za-z_<>][A-Za-z0-9_.<>]*)', line)
            if match:
                path = match[1].replace('\\', '/')
                name = next((p for p in tracked if path.endswith('/' + p)), None)
                if name is None:
                    name = next((f'asyncio/{p}' for p in ('tasks.py','timeouts.py','futures.py','base_events.py','runners.py','events.py') if path.endswith('/asyncio/' + p)), 'unlisted-module')
                frames.append({'file': name, 'line': int(match[2]), 'function': match[3] if name != 'unlisted-module' else 'withheld'})
                ordered.append({'kind': 'frame', **frames[-1]})
            match = re.match(r'^(?:[A-Za-z_][A-Za-z0-9_]*\.)*(TimeoutError|CancelledError|RuntimeError|ConnectionError|ConnectionClosedError|ConnectionClosedOK|OSError|ValueError|AssertionError|WorkerCommandFailure)(?::|$)', line)
            if match:
                exceptions.append(match[1])
                ordered.append({'kind': 'exception_class', 'class': match[1]})
        result.update(observation='stable_original_read', frames=frames, exception_classes=exceptions, ordered_trace=ordered)
    except FileNotFoundError:
        result['observation'] = 'original_file_absent'
    except PermissionError:
        result['observation'] = 'original_file_unreadable'
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)

try:
    observe()
except Exception as error:
    # No raw error text, traceback, request values or original lines may escape.
    result.update(observation='read_refused_or_unavailable', reader_error_type=type(error).__name__)
out = Path(os.environ['RUNNER_TEMP']) / ('icse-original-stderr-106760391543-' + os.environ['GITHUB_RUN_ID'])
out.mkdir(mode=0o700, exist_ok=False)
with (out / 'observation.json').open('x', encoding='utf-8') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
print(json.dumps(result, sort_keys=True))
if result['observation'] not in ('stable_original_read', 'original_file_absent'):
    raise SystemExit(1)
