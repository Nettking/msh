import sys, json, datetime, pathlib
sys.path.insert(0, r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client()
p=api('/pulls/468')
s=p['head']['sha']
runs=api('/actions/runs?head_sha='+s+'&per_page=100')['workflow_runs']
rows=[]
for r in runs:
    row={k:r.get(k) for k in ['id','name','path','status','conclusion','head_sha','event','run_attempt','created_at','updated_at','html_url']}
    if r['path'].endswith('phase-f7-closeout.yml'):
        row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
    rows.append(row)
o={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':468,'url':p['html_url'],'head':s,'base':p['base']['sha'],'merge_commit_sha':p['merge_commit_sha'],'draft':p['draft'],'state':p['state'],'runs':rows}
path=pathlib.Path('handoff/diagnostics/pr468-initial-ci-snapshot.json');path.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
for r in rows:
    print(json.dumps({k:v for k,v in r.items() if k!='jobs'}))
    for j in r.get('jobs',[]):print(json.dumps({k:j.get(k) for k in ['id','name','status','conclusion','runner_name','runner_id','started_at','steps']}))
print(json.dumps({k:v for k,v in o.items() if k!='runs'}))
