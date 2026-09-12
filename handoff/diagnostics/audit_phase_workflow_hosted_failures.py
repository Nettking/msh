"""Read-only infrastructure evidence for the eight proposed legacy CI retirements."""
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
from pathlib import Path
import sys
import urllib.error

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client, BASE

HEAD = '84c66f8185c1411d9dc8c5c33244a2f564845ce7'
CURRENT = '17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05'
NAMES = [
    'phase-f71-job-contracts.yml', 'phase-f72-provider-selection.yml',
    'phase-f73-durable-job-ownership.yml', 'phase-f74-worker-dispatch.yml',
    'phase-f75-retry-cancellation.yml', 'phase-f76-artifact-authorization.yml',
    'phase-f77-ai-runtime-integration.yml', 'phase-f84-compute-worker-activation.yml',
]
api = client()
runs = api('/actions/runs?head_sha=' + HEAD + '&per_page=100')['workflow_runs']
selected = [r for r in runs if r['path'].split('/')[-1] in NAMES]
assert {r['path'].split('/')[-1] for r in selected} == set(NAMES)

def inspect(run):
    jobs = api('/actions/runs/' + str(run['id']) + '/jobs?per_page=100')['jobs']
    records = []
    for job in jobs:
        url = job['check_run_url']
        assert url.startswith(BASE + '/check-runs/')
        annotations = api(url.removeprefix(BASE) + '/annotations?per_page=100')
        records.append({**{k:job.get(k) for k in ['id','name','status','conclusion',
            'runner_id','runner_name','labels','started_at','completed_at','steps','check_run_url']},
            'annotations': annotations})
    return dict(workflow=run['path'],run_id=run['id'],head_sha=run['head_sha'],
        event=run['event'],status=run['status'],conclusion=run['conclusion'],jobs=records)

with ThreadPoolExecutor(max_workers=4) as pool:
    records = list(pool.map(inspect, selected))

policy = {}
for path in ['/branches/main', '/branches/main/protection', '/rulesets?includes_parents=true']:
    try:
        result = api(path)
        if path == '/branches/main':
            policy[path] = {k:result.get(k) for k in ['name','protected','protection']}
        else:
            policy[path] = result
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            message = json.loads(raw).get('message')
        except (ValueError, AttributeError):
            message = 'Non-JSON API error; no credentials recorded'
        policy[path] = dict(http_status=error.code,message=message)

report = dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    current_main= CURRENT, pr=467, pr_head=HEAD, records=records, branch_policy=policy,
    state_changed=False, physical_runtime_changed=False, protected_recorder_data_untouched=True,
    scope='Historical hosted-job infrastructure inspection only; preserve all qualification evidence')
(ROOT/'phase-workflow-hosted-failures.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(dict(workflows=[dict(workflow=r['workflow'],run_id=r['run_id'],
    jobs=len(r['jobs']),steps=sum(len(j.get('steps') or []) for j in r['jobs']),
    assigned_runner_ids=sorted({j.get('runner_id') or 0 for j in r['jobs']}),
    conclusions=sorted({j['conclusion'] for j in r['jobs']}),
    messages=sorted({a['message'] for j in r['jobs'] for a in j['annotations']})) for r in records],
    branch_policy=policy)))
