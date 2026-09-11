"""Read only the selected final main and required workflow deltas; never dispatch."""
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client, REQUIRED
target=json.loads((ROOT/'merged-main-target.json').read_text())
sha=target['source_commit'];api=client()
path=ROOT/'merged-main-qualification-state.json'
old=json.loads(path.read_text()) if path.exists() else {}
assert old.get('source_commit',sha)==sha
main=api('/git/ref/heads/main?audit='+str(int(time.time())))['object']['sha']
wanted=set(REQUIRED)|{'cfi2-onboarding-composition.yml','release-image-metadata.yml'}
runs=api('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs']
selected={}
for run in sorted(runs,key=lambda r:r['id']):
    name=run['path'].split('/')[-1]
    if name not in wanted:continue
    # Preserve the first actual final-head run; rerun attempts remain visible.
    if name not in selected:selected[name]=run
previous={r['run_id']:r for r in old.get('workflows',[])}
def poll(item):
    name,run=item;prior=previous.get(run['id'],{})
    signature={k:run[k] for k in ['status','conclusion','run_attempt','updated_at']}
    if run['status']=='completed' and prior.get('signature')==signature:
        jobs=prior['jobs']
    else:
        jobs=api('/actions/runs/'+str(run['id'])+'/jobs?per_page=100')['jobs']
        jobs=[{k:j.get(k) for k in ['id','name','status','conclusion','runner_name','started_at','completed_at']} for j in jobs]
    before={j['id']:j for j in prior.get('jobs',[])}
    delta=[{k:j[k] for k in ['id','name','status','conclusion']} for j in jobs
           if j['id'] not in before or (j['status'],j['conclusion'])!=(before[j['id']]['status'],before[j['id']]['conclusion'])]
    changed=bool(delta or not prior or prior.get('conclusion')!=run['conclusion'])
    row=dict(workflow=name,run_id=run['id'],event=run['event'],api_head_sha=run['head_sha'],
             status=run['status'],conclusion=run['conclusion'],attempt=run['run_attempt'],
             required_count=REQUIRED.get(name),signature=signature,jobs=jobs)
    return row,(dict(workflow=name,run_id=run['id'],status=run['status'],conclusion=run['conclusion'],jobs=delta) if changed else None)
rows=[];changes=[]
with ThreadPoolExecutor(max_workers=4) as pool:
    for row,delta in pool.map(poll,selected.items()):
        rows.append(row)
        if delta:changes.append(delta)
if old.get('actual_main',sha)!=main:
    changes.append(dict(field='actual_main',before=old.get('actual_main',sha),after=main))
report=dict(source_commit=sha,actual_main=main,main_matches=main==sha,
            recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            workflows=rows,missing_workflows=sorted(wanted-set(selected)))
if changes:
    path.write_text(json.dumps(report,indent=2)+'\n')
    (ROOT/'merged-main-last-transition.json').write_text(json.dumps(dict(recorded_at=report['recorded_at'],source_commit=sha,changes=changes),indent=2)+'\n')
print(json.dumps(dict(material_change=bool(changes),main_matches=main==sha,changes=changes)))
