"""Read owned runtime metadata only; never deploy product code or inspect records.

Run on Nettking directly or stream stdin to Nitro's existing Python. The only
local output is stdout; the caller retains it outside the campaign evidence.
"""
import datetime
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess

N = '0536f03d67eb277e11573c2188d8e820399627e3'
host = platform.node().lower()
assert host in {'nettking', 'nitro'}
project = 'fcp-v1-73c779-nettking' if host == 'nettking' else 'fcp-v1-fba508-nitro'
source = pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if host == 'nettking'
                      else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
commands = []


def run(args, timeout=15):
    commands.append(args)
    completed = subprocess.run(args, cwd=source, capture_output=True, text=True,
                               encoding='utf-8', errors='replace', timeout=timeout)
    if completed.returncode:
        raise RuntimeError(f'{args[:2]} exit {completed.returncode}')
    return completed.stdout.strip()


report = dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              mode='DIAGNOSTIC ONLY — NOT PHYSICAL ACCEPTANCE EVIDENCE',
              candidate_sha=N, host=host, project=project,
              source_sha=run(['git', 'rev-parse', 'HEAD']),
              source_clean=not run(['git', 'status', '--porcelain']),
              fingerprint=hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
              disk_free_bytes=shutil.disk_usage(source).free,
              services=[], state_changed=False, protected_recorder_data_untouched=True,
              physical_acceptance=False)
for service in ['flask', 'relay', 'recorder', 'ollama']:
    ids = run(['docker', 'ps', '-q', '--filter', 'label=com.docker.compose.project=' + project,
               '--filter', 'label=com.docker.compose.service=' + service,
               '--filter', 'label=com.docker.compose.oneoff=False']).splitlines()
    assert len(ids) == 1, (service, len(ids))
    value = json.loads(run(['docker', 'inspect', ids[0]]))[0]
    image = json.loads(run(['docker', 'image', 'inspect', value['Image']]))[0]
    env = dict(x.split('=', 1) for x in value['Config']['Env'] if '=' in x)
    state = value['State']
    item = dict(service=service, container_id=value['Id'], image_id=value['Image'],
                image_commit=image['Config'].get('Labels', {}).get('no.fcp.build_commit'),
                environment_commit=env.get('FCP_BUILD_COMMIT'),
                running=state['Running'], started_at=state['StartedAt'],
                restart_count=value['RestartCount'], oom_killed=state['OOMKilled'],
                health=state.get('Health', {}).get('Status'),
                memory_limit_bytes=value['HostConfig']['Memory'],
                device_requests=value['HostConfig'].get('DeviceRequests'),
                mounts=[{k:m[k] for k in ['Type','Source','Destination','RW']} for m in value['Mounts']])
    if service != 'ollama' and state['Running']:
        code = "import hashlib,pathlib,os,json;print(json.dumps(dict(build_commit=os.getenv('FCP_BUILD_COMMIT'),host_build_sha256=hashlib.sha256(pathlib.Path('/app/catalog/federation/host_build.py').read_bytes()).hexdigest())))"
        item['runtime_source'] = json.loads(run(['docker','exec',ids[0],'python','-B','-c',code]))
    if service == 'relay' and host == 'nitro':
        status = json.loads(run(['docker','exec',ids[0],'cat','/var/lib/fcp-relay/control-plane-status.json']))
        item['control'] = {k:status.get(k) for k in ['schema','role','ready','consensus_term','commit_index','last_applied']}
        item['control_identity_sha256'] = hashlib.sha256(json.dumps({k:status.get(k) for k in ['cluster_id','federation_id']},sort_keys=True).encode()).hexdigest()
    if service == 'ollama' and state['Running']:
        for label, command in [('version',['--version']),('models',['list']),('loaded_models',['ps'])]:
            item[label] = run(['docker','exec',ids[0],'ollama',*command])
    report['services'].append(item)
if host == 'nitro':
    report['memory'] = {line.split(':')[0]:line.split(':')[1].strip()
                        for line in pathlib.Path('/proc/meminfo').read_text().splitlines()
                        if line.startswith(('MemAvailable:','MemTotal:','SwapFree:'))}
    report['clock'] = run(['chronyc','tracking'])
else:
    report['gpu'] = run(['nvidia-smi','--query-gpu=name,memory.total,memory.used,utilization.gpu','--format=csv,noheader,nounits'])
    report['clock_status'] = run(['w32tm','/query','/status','/verbose'])
report['commands'] = commands
print(json.dumps(report, ensure_ascii=False))
