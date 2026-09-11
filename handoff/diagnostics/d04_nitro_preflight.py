"""Read-only D04 admission preflight; stream through existing authenticated SSH."""
import datetime, hashlib, json, os, pathlib, platform, subprocess

QUALIFIED = '9b286f931497bf6291e215f6340443c5162826b0'
RUNTIME = '0536f03d67eb277e11573c2188d8e820399627e3'
ROOT = pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
assert platform.node().casefold() == 'nitro'

def run(argv, cwd=None):
    p = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=15)
    assert p.returncode == 0, str(argv[:3]) + ': ' + p.stderr[-300:]
    return p.stdout.strip()

def process(pid):
    p = pathlib.Path('/proc') / str(pid)
    stat = (p / 'stat').read_text().rsplit(')', 1)[1].split()
    argv = (p / 'cmdline').read_bytes()
    return {'pid': pid, 'ppid': int(stat[1]), 'start_ticks': stat[19],
            'uid': p.stat().st_uid, 'cwd': os.readlink(p / 'cwd'),
            'executable': os.readlink(p / 'exe'),
            'argv_sha256': hashlib.sha256(argv).hexdigest(),
            'is_responder': b'tailnet_join_responder' in argv,
            'cgroup': (p / 'cgroup').read_text().splitlines()}

listeners = []
for table in ('tcp', 'tcp6'):
    for line in pathlib.Path('/proc/net/' + table).read_text().splitlines()[1:]:
        v = line.split()
        if v[3] == '0A' and int(v[1].split(':')[1], 16) == 5151:
            listeners.append({'table': table, 'port': 5151, 'socket_inode': v[9], 'owners': []})
inodes = {x['socket_inode']: x for x in listeners}
responders = []
for p in pathlib.Path('/proc').iterdir():
    if not p.name.isdigit(): continue
    try:
        if b'tailnet_join_responder' in (p / 'cmdline').read_bytes():
            responders.append(process(int(p.name)))
        for fd in (p / 'fd').iterdir():
            link = os.readlink(fd)
            if link.startswith('socket:[') and link[8:-1] in inodes:
                owner = process(int(p.name))
                if owner not in inodes[link[8:-1]]['owners']:
                    inodes[link[8:-1]]['owners'].append(owner)
    except (OSError, ProcessLookupError, PermissionError): pass
parents = []
for p in responders:
    try: parents.append(process(p['ppid']))
    except OSError: pass
containers = []
for cid in run(['docker', 'ps', '-q', '--no-trunc', '--filter', 'label=com.docker.compose.project=fcp-v1-fba508-nitro']).splitlines():
    d = json.loads(run(['docker', 'inspect', cid]))[0]
    env = dict(x.split('=', 1) for x in d['Config']['Env'] if '=' in x)
    containers.append({'id': cid, 'service': d['Config']['Labels'].get('com.docker.compose.service'),
                       'image_id': d['Image'], 'runtime_sha': env.get('FCP_BUILD_COMMIT'),
                       'started_at': d['State']['StartedAt'], 'running': d['State']['Running'],
                       'restarts': d['RestartCount'], 'oom_killed': d['State']['OOMKilled']})
print(json.dumps({'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'mode': 'HOST REMEDIATION PREFLIGHT - NOT PHYSICAL ACCEPTANCE EVIDENCE',
    'qualified_main': QUALIFIED, 'expected_current_runtime_sha': RUNTIME,
    'host': 'nitro', 'fingerprint': hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
    'source_sha': run(['git', 'rev-parse', 'HEAD'], ROOT),
    'source_clean': not run(['git', 'status', '--porcelain', '--untracked-files=all'], ROOT),
    'listeners': listeners, 'responders': responders, 'parents': parents, 'containers': containers,
    'state_changed': False, 'protected_recorder_data_untouched': True}, indent=2))
