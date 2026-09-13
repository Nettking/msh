"""Exercise the real Windows process-identity guard against an owned unrelated PID."""
import datetime
import json
import os
import pathlib
import subprocess
import sys
import time

h = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
c = h / '.acceptance/onboarding-test'
work = h / '.acceptance/stale-responder-identity'
sha = '1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not work.exists(), 'Inspect prior fault evidence instead of repeating it'
work.mkdir()
sys.path.insert(0, str(h))
from catalog.federation.tailnet_join_responder import PROCESS_RECORD_SCHEMA, process_start_token, stop_previous_instance

env = {key: value for key, value in os.environ.items() if not key.startswith(('FCP_', 'COMPOSE_', 'OLLAMA_'))}
env.update(json.loads((c / 'environment.private.json').read_text()))
py = 'C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
runner = [py, '-m', 'scripts.acceptance.v1_physical_runner', '--checkout', str(h), '--evidence-root', str(h / 'evidence/v1-physical'), '--runtime-binding', str(c / 'runtime-binding.json')]
args = ['--commit', sha, '--host', 'nettking', '--scenario', 'P05', '--assertion', 'stale-responder-pid']
status = {'candidate': sha, 'status': 'RUNNING', 'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'product_changed': False, 'protected_data_accessed': False, 'physical_pass': False}
record = work / 'result.json'
def save():
    record.write_text(json.dumps(status, indent=2) + '\n')
def call(command, label):
    result = subprocess.run([*runner, *command], cwd=h, env=env, capture_output=True, text=True, timeout=120)
    (work / (label + '.json')).write_text(result.stdout)
    if result.stderr:
        (work / (label + '.private.stderr')).write_text(result.stderr)
    assert result.returncode == 0, label + ' refused; preserve original evidence'
    return json.loads(result.stdout)
startup = subprocess.STARTUPINFO()
startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
startup.wShowWindow = 0
def child():
    return subprocess.Popen(['C:/Python314/python.exe', '-u', '-c', 'import os,sys; print(os.getpid(),flush=True); sys.stdin.readline()'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, startupinfo=startup, creationflags=subprocess.CREATE_NO_WINDOW)
old = None
unrelated = None
save()
try:
    prepared = call(['prepare', *args], 'prepare')
    status['prepare_id'] = prepared['prepare_id']
    save()
    old = child()
    old_pid = int(old.stdout.readline())
    assert old_pid == old.pid
    old_token = process_start_token(old_pid)
    assert old_token
    old.communicate('\n', timeout=10)
    assert old.returncode == 0
    unrelated = child()
    pid = int(unrelated.stdout.readline())
    assert pid == unrelated.pid
    token = process_start_token(pid)
    assert token and token != old_token
    path = work / 'stale-responder.pid'
    # Deliberately assemble the stale-record fault state using a real exited
    # creation identity and a currently occupied unrelated PID. No kernel PID
    # reuse or artificial exhaustion is claimed or needed to test the guard.
    path.write_text(json.dumps({'schema': PROCESS_RECORD_SCHEMA, 'pid': pid, 'start_token': old_token}) + '\n')
    began = time.monotonic()
    result = stop_previous_instance(path)
    assert result is None and unrelated.poll() is None
    assert process_start_token(pid) == token
    time.sleep(1)
    assert unrelated.poll() is None
    status.update(guard_returned_none=True, unrelated_process_survived=True, current_creation_identity_unchanged=True, prior_creation_identity_from_real_exited_process=True, stale_record_assembled=True, actual_kernel_pid_reuse_claimed=False, elapsed_seconds=time.monotonic()-began)
    full = [*args, '--prepare-id', prepared['prepare_id']]
    call(['action', *full, '--note', 'Assembled a stale responder record with a real exited process creation token and the PID of a real owned unrelated Windows process. Called the unchanged product stop_previous_instance path. Its actual OS-handle identity check returned no replacement; the unrelated process stayed alive with the same creation identity. No kernel PID reuse is claimed. No process/token reader or termination function was mocked.'], 'action')
    verified = call(['verify', *full], 'verify')
    assert verified['verdict'] == 'pass'
    status.update(status='COMPLETED', verdict='pass', finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
except BaseException as exc:
    status.update(status='STOPPED', error_type=type(exc).__name__)
    raise
finally:
    for process in (old, unrelated):
        if process is not None and process.poll() is None:
            process.communicate('\n', timeout=10)
    status['owned_fixture_processes_exited'] = all(p is None or p.poll() is not None for p in (old, unrelated))
    save()
    print(json.dumps(status), flush=True)
