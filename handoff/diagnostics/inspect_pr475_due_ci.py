"""Timestamped state-driven PR475 check; completed unchanged runs are reused."""
import datetime,json,pathlib,sys
dest=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
previous_paths=sorted(dest.glob('pr475-ci-*.json'))
previous=json.loads((previous_paths[-1] if previous_paths else dest/'pr475-initial-ci.json').read_text())
old_runs={r['id']:r for r in previous['runs']}
old_jobs={j['id']:j for r in previous['runs'] for j in r['jobs']}
api=client();head='5e6f184311019b9982e8544a18f3dc02c1b16e98'
pr=api('/pulls/475');assert pr['head']['sha']==head
runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
now=datetime.datetime.now(datetime.timezone.utc)
out={'recorded_at':now.isoformat(),'pr':475,'head_sha':head,'merge_checkout':pr['merge_commit_sha'],'base_sha':pr['base']['sha'],'draft':pr['draft'],'pr_state':pr['state'],'runs':[],'new_completed_jobs':[]}
for r in runs:
    before=old_runs.get(r['id'])
    if before and before['status']=='completed' and before['updated_at']==r['updated_at']:
        out['runs'].append(before);continue
    item={k:r.get(k) for k in ['id','name','path','event','head_sha','status','conclusion','run_attempt','created_at','updated_at']}
    item['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?filter=latest&per_page=100')['jobs']
    out['runs'].append(item)
    for j in item['jobs']:
        old=old_jobs.get(j['id'])
        if j['status']=='completed' and (not old or old['status']!='completed'):
            out['new_completed_jobs'].append({'run_id':r['id'],'workflow':r['path'],'job':j})
name='pr475-ci-'+now.strftime('%Y%m%dT%H%M%S')+'.json';(dest/name).write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'snapshot':name,'pr_head':head,'draft':out['draft'],'runs':[{'id':r['id'],'name':r['name'],'status':r['status'],'conclusion':r['conclusion']} for r in out['runs']],'new_completed':[{'run':i['run_id'],'id':i['job']['id'],'name':i['job']['name'],'runner':i['job']['runner_name'],'conclusion':i['job']['conclusion'],'failed_steps':[s['name'] for s in i['job']['steps'] if s['conclusion']=='failure']} for i in out['new_completed_jobs']],'active_jobs':[{'id':j['id'],'runner':j['runner_name'],'step':[s['name'] for s in j['steps'] if s['status']=='in_progress']} for r in out['runs'] for j in r['jobs'] if j['status']=='in_progress']}))
