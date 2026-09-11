"""Fill enumerated exact-head gaps after sweep completion; never rerun valid jobs."""
import datetime
import json
import pathlib
import sys

audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sys.path.insert(0,str(audit))
from github_qualification import client

root=pathlib.Path(__file__).resolve().parent
assert 'Sweep complete:' in (root.parent/'DIAGNOSTIC_SWEEP_REPORT.md').read_text(encoding='utf-8')
out=root/'pr465-gap-dispatch-ledger.json'
ledger=json.loads(out.read_text()) if out.exists() else dict(dispatches=[])
api=client()
targets=[(465,'4749ab6689315a7ad953c11ce4b2ec942433d71a','codex/analysis-content-reader-sharing', ['cf7-acceptance-harness.yml','cf7c-physical-test-readiness.yml','cf8-role-retirement.yml','phase-f85-operator-federation-surface.yml','cfi2-onboarding-composition.yml','release-image-metadata.yml'],False)]
for number,sha,ref,workflows,require_merge_proof in targets:
    assert api('/pulls/'+str(number))['head']['sha']==sha
    assert api('/git/ref/heads/'+ref)['object']['sha']==sha
    runs=api('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs']
    for workflow in workflows:
        matching=[r for r in runs if r['path'].split('/')[-1]==workflow]
        if any(r['event']=='workflow_dispatch' for r in matching): continue
        if any(r.get('workflow')==workflow and r.get('sha')==sha for r in ledger['dispatches']): continue
        proof=[]
        if require_merge_proof:
            native=json.loads((audit/f'pr{number}-head-{sha[:8]}-native-retention.json').read_text())
            assert native['source_commit']==sha
            if not matching or any(r['status']!='completed' for r in matching): continue
            for run in matching:
                jobs=api('/actions/runs/'+str(run['id'])+'/jobs?per_page=100')['jobs']
                for job in jobs:
                    record=next((r for r in native['records'] if r.get('job_id')==job['id']),None)
                    assert record and record.get('checkout_matches') is False, 'Preserve valid or unverified work'
                    proof.append({k:record[k] for k in ['job_id','run_id','checkout_commits','sha256']})
        elif matching:
            continue
        assert api('/git/ref/heads/'+ref)['object']['sha']==sha
        row=dict(pr=number,sha=sha,ref=ref,workflow=workflow,
                 requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                 reason='No run exists' if not matching else 'Completed PR runs used another checkout SHA; native proof retained',
                 native_proof=proof,status='REQUESTING')
        ledger['dispatches'].append(row)
        out.write_text(json.dumps(ledger,indent=2)+'\n',encoding='utf-8')
        row['response']=api('/actions/workflows/'+workflow+'/dispatches',{'ref':ref})
        row['status']='DISPATCHED'
        out.write_text(json.dumps(ledger,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(row),flush=True)

