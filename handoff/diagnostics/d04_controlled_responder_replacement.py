"""Controlled host maintenance only: replace proved stale Nitro responder with clean N.

No source advance, Docker action, enrollment request, or Recorder-host access.
Run once via ssh_campaign_script.py; inspect durable host receipt before retry.
"""
import datetime, hashlib, json, os, pathlib, platform, signal, subprocess, sys, time, urllib.request

N = '0536f03d67eb277e11573c2188d8e820399627e3'
M = '9b286f931497bf6291e215f6340443c5162826b0'
BASE = pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910')
ROOT = BASE / 'source'
DATA = pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data')
OUT = BASE / 'inputs/d04-controlled-replacement-20260911.json'
LOG = BASE / 'inputs/d04-controlled-responder-20260911.log'
OLD = 1422341

def run(argv):
    p = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=20)
    assert p.returncode == 0, str(argv[:3]) + ' failed: ' + p.stderr[-250:]
    return p.stdout.strip()

def identity(pid):
    p = pathlib.Path('/proc') / str(pid)
    s = (p / 'stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': pid, 'ppid': int(s[1]), 'start_ticks': s[19], 'uid': p.stat().st_uid,
            'cwd': os.readlink(p / 'cwd'), 'exe': os.readlink(p / 'exe'),
            'argv_sha256': hashlib.sha256((p / 'cmdline').read_bytes()).hexdigest(),
            'cgroup': (p / 'cgroup').read_text().strip()}

def sockets5151():
    result = []
    for table in ('tcp', 'tcp6'):
        for line in pathlib.Path('/proc/net/' + table).read_text().splitlines()[1:]:
            v = line.split()
            if v[3] == '0A' and int(v[1].split(':')[1], 16) == 5151:
                result.append(v[9])
    return result

def owns(pid, inode):
    return any(os.readlink(fd) == 'socket:[' + inode + ']' for fd in (pathlib.Path('/proc') / str(pid) / 'fd').iterdir())

def containers():
    ids = run(['docker', 'ps', '-q', '--no-trunc', '--filter', 'label=com.docker.compose.project=fcp-v1-fba508-nitro']).splitlines()
    return [json.loads(run(['docker', 'inspect', cid]))[0] for cid in ids]

assert platform.node().casefold() == 'nitro' and os.getuid() == 1000
assert not OUT.exists() and not LOG.exists(), 'Inspect existing D04 operation before retry'
assert run(['git', 'rev-parse', 'HEAD']) == N
assert not run(['git', 'status', '--porcelain', '--untracked-files=all'])
before = containers()
cores = {x['Config']['Labels'].get('com.docker.compose.service'): x for x in before}
for name in ('flask', 'relay', 'recorder'):
    env = dict(v.split('=', 1) for v in cores[name]['Config']['Env'] if '=' in v)
    assert env['FCP_BUILD_COMMIT'] == N and cores[name]['State']['Running']
flask = cores['flask']
assert any(m['Source'] == str(DATA) and m['Destination'] == '/app/data' for m in flask['Mounts'])
ip = run(['tailscale', 'ip', '-4']).splitlines()[0]
ports = flask['NetworkSettings']['Ports']['5000/tcp']
assert len(ports) == 1 and ports[0]['HostIp'] == ip
app_url = 'http://' + ip + ':' + ports[0]['HostPort']
sys.path.insert(0, str(ROOT / 'scripts'))
from federation_host_runner import load_host_module
bridge = load_host_module('tailnet_join_bridge')
secret = DATA / bridge.SECRET_RELATIVE
pid_file = DATA / bridge.PID_RELATIVE
app_env = dict(v.split('=', 1) for v in flask['Config']['Env'] if '=' in v)
assert app_env.get('FCP_AUTO_JOIN_SECRET_FILE') == '/app/data/' + bridge.SECRET_RELATIVE
for directory in (DATA, DATA / 'federation', secret.parent):
    assert directory.is_dir() and not directory.is_symlink() and directory.stat().st_uid == os.getuid()
    assert directory.resolve() == directory
# Fresh metadata proves both paths absent. Normal checked-in helper creation is
# now an explicit Nitro-only configuration action; never copy legacy identity.
assert not os.path.lexists(secret) and not os.path.lexists(secret.with_name(secret.name + '.tmp'))
assert not os.path.lexists(pid_file), 'Reinspect any newly existing campaign responder state'
expected = {'pid': OLD, 'ppid': 1, 'start_ticks': '120268097', 'uid': 1000,
            'cwd': '/home/martin/fcp', 'exe': '/usr/bin/python3.14 (deleted)',
            'argv_sha256': 'c0b224917a90d9b662818ce970624ffc461f8b84d3ab4f144bb84bec8046bfb5',
            'cgroup': '0::/user.slice/user-1000.slice/session-601.scope'}
assert identity(OLD) == expected
assert sockets5151() == ['31225091'] and owns(OLD, '31225091')
fd = os.pidfd_open(OLD)
assert identity(OLD) == expected, 'Refuse PID reuse'
receipt = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'status': 'PRECONDITIONS_VERIFIED', 'host': 'nitro', 'qualified_main': M,
    'runtime_sha': N, 'command': 'SIGTERM exact stale instance; checked-in N federation_host_runner.py tailnet_join_responder on tailnet5151',
    'previous': expected, 'state_changed': False, 'physical_acceptance': False,
    'planned_state_writes': ['new Nitro-only shared host secret through checked-in ensure_secret', 'new Nitro-only canonical responder PID record', 'maintenance log/receipt'],
    'protected_recorder_data_untouched': True, 'enrollment_request_sent': False}

def save():
    OUT.write_text(json.dumps(receipt, indent=2) + '\n')
    OUT.chmod(0o600)

save()
try:
    signal.pidfd_send_signal(fd, signal.SIGTERM)
    receipt.update(status='STALE_SIGTERM_SENT', state_changed=True)
    save()
    deadline = time.monotonic() + 10
    while sockets5151() and time.monotonic() < deadline: time.sleep(0.25)
    assert not sockets5151(), 'Stale listener did not exit; no escalation authorized by this procedure'
    env = {k: v for k, v in os.environ.items() if not k.startswith(('FCP_', 'COMPOSE_', 'PYTHON'))}
    env.update(FCP_BUILD_COMMIT=N, FCP_DATA_DIR=str(DATA), FCP_AUTO_JOIN_PORT='5151',
               PYTHONDONTWRITEBYTECODE='1', FCP_AUTO_JOIN_APP_URL=app_url)
    argv = [str(BASE / 'host-venv/bin/python3'), '-B', str(ROOT / 'scripts/federation_host_runner.py'),
            'tailnet_join_responder', '--bind', ip, '--port', '5151', '--app-url', app_url]
    with LOG.open('xb') as log:
        p = subprocess.Popen(argv, cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    receipt.update(status='CURRENT_N_RESPONDER_STARTED', pid=p.pid)
    save()
    deadline = time.monotonic() + 15
    while not sockets5151() and p.poll() is None and time.monotonic() < deadline: time.sleep(0.25)
    assert p.poll() is None, 'Checked-in responder exited; inspect private host log'
    listeners = sockets5151()
    assert len(listeners) == 1 and owns(p.pid, listeners[0])
    new = identity(p.pid)
    assert new['cwd'] == str(ROOT)
    t = time.monotonic()
    with urllib.request.urlopen('http://' + ip + ':5151/fcp/federation/tailnet-join/health', timeout=10) as response:
        body = json.load(response)
        assert response.status == 200 and body.get('responder') == 'ready' and body.get('tailscale') is True
    health_elapsed = time.monotonic() - t
    time.sleep(10)
    assert identity(p.pid) == new and sockets5151() == listeners and owns(p.pid, listeners[0])
    after = containers()
    fields = lambda x: (x['Id'], x['Image'], x['State']['StartedAt'], x['RestartCount'])
    assert sorted(map(fields, before)) == sorted(map(fields, after))
    assert secret.is_file() and not secret.is_symlink() and bridge.read_secret(secret)
    assert secret.stat().st_uid == os.getuid() and secret.stat().st_mode & 0o777 == 0o600
    assert run(['git', 'rev-parse', 'HEAD']) == N and not run(['git', 'status', '--porcelain', '--untracked-files=all'])
    receipt.update(status='D04_CURRENT_RUNTIME_PORT_OWNERSHIP_VERIFIED', current=new,
        listener_inode=listeners[0], health={'status': 200, 'responder': body['responder'],
        'tailscale': body['tailscale'], 'elapsed_seconds': health_elapsed},
        no_stale_respawn_observed_seconds=10, core_container_identity_and_start_times_unchanged=True,
        new_nitro_shared_secret_created_by_checked_in_helper=True,
        existing_secret_overwritten=False, legacy_identity_copied=False, source_clean=True,
        next_action='Verify real Nettking peer health; freeze qualified M, clean revalidation, then align owned responder and all campaign runtime with M before physical evidence')
except Exception as exc:
    receipt.update(status='D04_REMEDIATION_INCOMPLETE_INSPECT_BEFORE_RETRY', error=str(exc))
finally:
    os.close(fd)
    receipt['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
print(json.dumps(receipt, indent=2))
