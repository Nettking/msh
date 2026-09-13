"""Cancel only an offline runner's unfinished work, then recover its job/dependents."""
import datetime
import json
import pathlib
import sys
import time

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

api = client()
run_id, job_id = 34777368620, 103777830545
receipt = pathlib.Path(__file__).parent / 'storage-repair-offline-host-recovery.json'
assert not receipt.exists(), 'Inspect retained one-time recovery; do not duplicate'
assert api('/pulls/487')['head']['sha'] == '76ad339f1631e136bba7a8a85973bddb1650d570'
run = api(f'/actions/runs/{run_id}')
assert run['run_attempt'] == 1 and run['status'] == 'in_progress'
jobs = api(f'/actions/runs/{run_id}/jobs?filter=latest&per_page=100')['jobs']
active = [j for j in jobs if j['status'] != 'completed']
assert len(active) == 1 and active[0]['id'] == job_id
assert active[0]['runner_name'] == 'AQG7NCC-Linux'
runners = api('/actions/runners?per_page=100')['runners']
assert next(r for r in runners if r['name'] == 'AQG7NCC-Linux')['status'] == 'offline'
assert all(j['conclusion'] == 'success' for j in jobs if j['status'] == 'completed')
out = {
    'source': run['head_sha'], 'native_source': 'ec0bbd1d5ce45a97ddc10058bad38eea90f044b6',
    'run_id': run_id, 'interrupted_job_id': job_id,
    'requested_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'authorization': 'User reports AQG shutdown, instructs continued progress without waiting for it.',
    'classification': 'confirmed host offline interruption',
    'completed_green_job_ids_retained': [j['id'] for j in jobs if j['status'] == 'completed'],
    'scope': 'Only active job is on offline AQG. Cancel pending execution, rerun that job and dependent verdicts using unchanged workflow/source/seed/runner pool. No successful test rerun.',
    'status': 'cancel_requesting',
}

def save():
    receipt.write_text(json.dumps(out, indent=2) + '\n')

save()
out['cancel_response'] = api(f'/actions/runs/{run_id}/cancel', {})
out['status'] = 'cancel_accepted'
save()
for _ in range(12):
    live = api(f'/actions/runs/{run_id}')
    if live['status'] == 'completed':
        break
    time.sleep(2)
else:
    print(json.dumps({'status': out['status'], 'next': 'Inspect terminal state before one targeted recovery'}))
    raise SystemExit(0)
out['cancelled_conclusion'] = live['conclusion']
out['status'] = 'job_recovery_requesting'
save()
out['job_recovery_response'] = api(f'/actions/jobs/{job_id}/rerun', {})
out['status'] = 'job_recovery_accepted'
save()
print(json.dumps({'status': out['status'], 'retained_green_jobs': len(out['completed_green_job_ids_retained'])}))
