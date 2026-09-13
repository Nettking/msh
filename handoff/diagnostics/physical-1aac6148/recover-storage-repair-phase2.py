"""One failed Linux job recovery after two same-tree passes of the timed-out test."""
import datetime
import hashlib
import json
import pathlib
import sys

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

api = client()
root = pathlib.Path(__file__).parent
receipt = root / 'storage-repair-phase2-recovery.json'
assert not receipt.exists(), 'Inspect previous receipt; no repeated recovery'
sha = '76ad339f1631e136bba7a8a85973bddb1650d570'
run_id = 34777608170
assert api('/pulls/487')['head']['sha'] == sha
run = api(f'/actions/runs/{run_id}')
assert run['status'] == 'completed' and run['conclusion'] == 'failure'
assert run['run_attempt'] == 1 and run['head_sha'] == sha
jobs = api(f'/actions/runs/{run_id}/jobs?filter=latest&per_page=100')['jobs']
assert len(jobs) == 2
assert {j['id'] for j in jobs if j['conclusion'] == 'failure'} == {103778479160}
assert all(j['conclusion'] in {'success', 'failure'} for j in jobs)
log = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/pr487-qualification/phase2-linux-first-103778479160.private.log')
digest = hashlib.sha256(log.read_bytes()).hexdigest()
assert digest == '73b3fff98482725fa77a3c46305b92d97843f2280b604e8bc22bb043c75a031b'
out = {
    'source': sha, 'run_id': run_id, 'original_attempt': 1,
    'requested_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'classification': 'infrastructure/host/transient timing issue; no demonstrated candidate defect',
    'observation': 'Initial recorder enrollment/auth receive exceeds unchanged 3-second test deadline before scan publication. Same test passed in two independent equal-tree release executions in 0.090 and 0.344 seconds.',
    'first_failure_log_sha256': digest,
    'cross_proof': 'storage-reply-repair-pr487-audit.json',
    'scope': 'Failed Linux job only; retain successful Windows, no source/deadline/runner change.',
    'status': 'requesting',
}
receipt.write_text(json.dumps(out, indent=2) + '\n')
out['response'] = api(f'/actions/runs/{run_id}/rerun-failed-jobs', {})
out['status'] = 'accepted'
receipt.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({'run_id': run_id, 'status': out['status']}))
