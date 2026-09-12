"""Compact state-change poll for D08 PR467 only; never dispatch or merge."""
import concurrent.futures
import datetime
import json
import pathlib
import sys

audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sys.path.insert(0,str(audit))
from github_qualification import REQUIRED, client

root=pathlib.Path(__file__).resolve().parent
path=root/'pr467-qualification-state.json'
previous=json.loads(path.read_text()) if path.exists() else {}
api=client()
targets=[(467,'84c66f8185c1411d9dc8c5c33244a2f564845ce7')]
wanted=set(REQUIRED)|{'cfi2-onboarding-composition.yml','release-image-metadata.yml'}
def poll(target):
    number,intended=target
    pr=api('/pulls/'+str(number))
    old=previous.get('prs',{}).get(str(number))
    if old is None:
        baseline=audit/f'pr{number}-head-{intended[:8]}-qualification-latest.json'
        old=json.loads(baseline.read_text()) if baseline.exists() else {}
    head=pr['head']['sha']
    result=dict(number=number,head=head,intended_head=intended,head_matches=head==intended,
                state=pr['state'],draft=pr['draft'],merged=pr['merged'],merge_commit=pr['merge_commit_sha'],
                review_comment_count=pr.get('review_comments',0),
                mergeable_state=pr.get('mergeable_state'),workflows=[])
    changes=[]
    for key in ['head','state','draft','merged','merge_commit','review_comment_count']:
        if key in old and old[key]!=result[key]:changes.append(dict(field=key,before=old[key],after=result[key]))
    runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
    selected={}
    for run in sorted(runs,key=lambda r:r['id']):
        name=run['path'].split('/')[-1]
        if name not in wanted:continue
        prior=selected.get(name)
        if prior and prior['event']=='workflow_dispatch' and run['event']!='workflow_dispatch':continue
        selected[name]=run
    old_runs={w['run_id']:w for w in old.get('workflows',[])}
    for name,run in selected.items():
        old_run=old_runs.get(run['id'],{})
        signature={k:run[k] for k in ['status','conclusion','run_attempt','updated_at']}
        # Non-terminal jobs can progress while the parent remains queued.
        # Reuse only terminal runs; never rely on parent timestamps as job events.
        unchanged=run['status']=='completed' and old_run.get('signature')==signature
        if unchanged:
            jobs=old_run['jobs']
        else:
            jobs=api('/actions/runs/'+str(run['id'])+'/jobs?per_page=100')['jobs']
            jobs=[{k:j.get(k) for k in ['id','name','status','conclusion','runner_name','started_at','completed_at']} for j in jobs]
        old_jobs={j['id']:j for j in old_run.get('jobs',[])}
        transitions=[]
        for job in jobs:
            before=old_jobs.get(job['id'])
            if before is None or (before['status'],before['conclusion'])!=(job['status'],job['conclusion']):
                transitions.append(dict(id=job['id'],name=job['name'],status=job['status'],conclusion=job['conclusion']))
        if transitions or not old_run or old_run.get('conclusion')!=run['conclusion']:
            changes.append(dict(workflow=name,run_id=run['id'],event=run['event'],status=run['status'],conclusion=run['conclusion'],jobs=transitions))
        result['workflows'].append(dict(workflow=name,run_id=run['id'],event=run['event'],
            api_head_sha=run['head_sha'],status=run['status'],conclusion=run['conclusion'],attempt=run['run_attempt'],
            required_count=REQUIRED.get(name),signature=signature,jobs=jobs))
    result['missing_workflows']=sorted(wanted-set(selected))
    return str(number),result,changes

report=dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),prs={})
changes={}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for number,result,delta in pool.map(poll,targets):
        report['prs'][number]=result
        if delta:changes[number]=delta
report['material_change']=bool(changes)
if changes or not path.exists():
    path.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
if changes:
    (root/'pr467-qualification-last-transition.json').write_text(json.dumps(dict(observed_at=report['observed_at'],changes=changes),indent=2)+'\n',encoding='utf-8')
print(json.dumps({'material_change':bool(changes),'head':report['prs']['467']['head'],'missing':report['prs']['467']['missing_workflows'],'runs':[{'workflow':w['workflow'],'id':w['run_id'],'status':w['status'],'conclusion':w['conclusion'],'passed':sum(j['conclusion']=='success' for j in w['jobs']),'failed':[j['name'] for j in w['jobs'] if j['conclusion'] in ('failure','timed_out','cancelled')]} for w in report['prs']['467']['workflows']]}))

