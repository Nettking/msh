"""Dispatch only completed, source-mismatched required PR461 workflows."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

root=pathlib.Path(__file__).resolve().parent
audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sys.path.insert(0,str(audit))
from github_qualification import client

number=461;sha='5b826c6806ab1bdb960412ba20ca78192971fb1d'
ref='codex/tailnet-responder-port-propagation'
repo=pathlib.Path('C:/wsl/fcp-tailnet-responder-port-20260911')
api=client();pr=api('/pulls/'+str(number))
assert pr['head']['sha']==sha and not pr['merged']
assert api('/git/ref/heads/'+ref)['object']['sha']==sha
native=json.loads((root/'qualification-native-provenance.json').read_text())['prs'][str(number)]
assert native['source_commit']==sha
path=root/'completed-head-gap-dispatches.json'
ledger=json.loads(path.read_text()) if path.exists() else dict(dispatches=[])
runs=api('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs']
for workflow in ['product-branding.yml','release-image-metadata.yml']:
    matching=[r for r in runs if r['path'].split('/')[-1]==workflow]
    if any(r['event']=='workflow_dispatch' for r in matching):continue
    if any(r['workflow']==workflow for r in ledger['dispatches']):continue
    if not matching or any(r['status']!='completed' for r in matching):continue
    proof=[]
    for run in matching:
        jobs=api('/actions/runs/'+str(run['id'])+'/jobs?per_page=100')['jobs']
        assert jobs and all(j['status']=='completed' for j in jobs)
        for job in jobs:
            record=next(r for r in native['records'] if r['job_id']==job['id'])
            assert record.get('checkout_matches') is False
            proof.append({k:record[k] for k in ['job_id','run_id','checkout_commits','sha256']})
    definition=subprocess.check_output(['git','show',sha+':.github/workflows/'+workflow],cwd=repo)
    assert b'workflow_dispatch' in definition
    assert api('/git/ref/heads/'+ref)['object']['sha']==sha
    row=dict(pr=number,sha=sha,ref=ref,workflow=workflow,native_proof=proof,
             workflow_sha256=hashlib.sha256(definition).hexdigest(),
             requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),status='REQUESTING')
    ledger['dispatches'].append(row)
    path.write_text(json.dumps(ledger,indent=2)+'\n',encoding='utf-8')
    row['response']=api('/actions/workflows/'+workflow+'/dispatches',{'ref':ref})
    row['status']='DISPATCHED'
    path.write_text(json.dumps(ledger,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(workflow=workflow,status=row['status'],source_sha=sha)),flush=True)
