"""Fill only absent final-main workflows; never duplicate push qualification."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
target=json.loads((ROOT/'merged-main-target.json').read_text())
sha=target['source_commit']; api=client()
path=ROOT/'merged-main-gap-dispatches.json'
ledger=json.loads(path.read_text()) if path.exists() else dict(source_commit=sha,dispatches=[])
assert ledger['source_commit']==sha
for name in ['cf7-acceptance-harness.yml','cf7c-physical-test-readiness.yml','ci-test-sharding.yml']:
    assert api('/git/ref/heads/main')['object']['sha']==sha
    runs=api('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs']
    if any(r['path'].split('/')[-1]==name for r in runs):continue
    if any(r['workflow']==name for r in ledger['dispatches']):continue
    definition=subprocess.check_output(['git','show',sha+':.github/workflows/'+name],cwd=target['worktree'])
    assert b'workflow_dispatch' in definition
    row=dict(workflow=name,source_commit=sha,ref='main',requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),workflow_sha256=hashlib.sha256(definition).hexdigest(),status='REQUESTING')
    ledger['dispatches'].append(row)
    path.write_text(json.dumps(ledger,indent=2)+'\n')
    row['response']=api('/actions/workflows/'+name+'/dispatches',{'ref':'main'})
    row['status']='DISPATCHED'
    path.write_text(json.dumps(ledger,indent=2)+'\n')
    print(json.dumps(row),flush=True)
