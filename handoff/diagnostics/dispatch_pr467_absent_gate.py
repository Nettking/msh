"""Dispatch one reviewed absent exact-head gate; checkpoint after each invocation."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
REPO = Path('C:/wsl/fcp-analysis-content-resolve-race-20260912')
SHA = '84c66f8185c1411d9dc8c5c33244a2f564845ce7'
REF = 'codex/analysis-content-resolve-race'
sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import REQUIRED, client

assert len(sys.argv) == 2
name = sys.argv[1]
assert name in set(REQUIRED) | {'cfi2-onboarding-composition.yml', 'release-image-metadata.yml'}
review = json.loads((ROOT / 'pr467-prequalification-review.json').read_text())
assert review['source_commit'] == SHA and not review['findings']
assert review['required_qualification']['jobs'] == 37
state = json.loads((ROOT / 'pr467-qualification-state.json').read_text())['prs']['467']
assert state['head'] == SHA and name in state['missing_workflows']
path = ROOT / 'pr467-gap-dispatch-ledger.json'
ledger = json.loads(path.read_text()) if path.exists() else dict(dispatches=[])
assert not any(r['workflow'] == name and r['sha'] == SHA for r in ledger['dispatches'])
api = client()
pr = api('/pulls/467')
assert pr['head']['sha'] == SHA and not pr['draft'] and not pr['merged']
assert api('/git/ref/heads/' + REF)['object']['sha'] == SHA
runs = api('/actions/runs?head_sha=' + SHA + '&per_page=100')['workflow_runs']
assert not any(r['path'].split('/')[-1] == name for r in runs)
definition = subprocess.check_output(['git', 'show', SHA + ':.github/workflows/' + name], cwd=REPO)
assert b'workflow_dispatch' in definition
row = dict(pr=467, sha=SHA, ref=REF, workflow=name,
    requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    reason='Required reviewed exact-head gate has no existing run',
    workflow_sha256=hashlib.sha256(definition).hexdigest(), status='REQUESTING')
ledger['dispatches'].append(row)
path.write_text(json.dumps(ledger, indent=2) + '\n')
row['response'] = api('/actions/workflows/' + name + '/dispatches', {'ref': REF})
row['status'] = 'DISPATCHED'
path.write_text(json.dumps(ledger, indent=2) + '\n')
print(json.dumps(row))
