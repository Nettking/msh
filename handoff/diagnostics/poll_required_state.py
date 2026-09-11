"""Compact state-change poll for the three required PRs; never dispatch or merge."""
import concurrent.futures
import datetime
import json
import pathlib
import sys

audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sys.path.insert(0,str(audit))
from github_qualification import REQUIRED, client

root=pathlib.Path(__file__).resolve().parent
path=root/'qualification-state.json'
previous=json.loads(path.read_text()) if path.exists() else {}
api=client()
targets=[(456,'1a0c634f47f8a247b6d1d2d1a219f5c12590587d'),
         (457,'143fe7a9082193114af3d34dc437b85f845849a2'),
         (461,'5b826c6806ab1bdb960412ba20ca78192971fb1d')]
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
                mergeable_state=pr.get('mergeable_state'),workflows=[])
    changes=[]
    for key in ['head','state','draft','merged','merge_commit']:
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
        unchanged=old_run.get('signature')==signature
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
    result['missing_workflows']=sorted(set(REQUIRED)-set(selected))
    return str(number),result,changes

report=dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),prs={})
changes={}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for number,result,delta in pool.map(poll,targets):
        report['prs'][number]=result
        if delta:changes[number]=delta
report['material_change']=bool(changes)
path.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
if changes:
    (root/'qualification-last-transition.json').write_text(json.dumps(dict(observed_at=report['observed_at'],changes=changes),indent=2)+'\n',encoding='utf-8')
print(json.dumps(dict(material_change=bool(changes),changes=changes)))
