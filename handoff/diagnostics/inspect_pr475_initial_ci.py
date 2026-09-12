"""Single startup snapshot for the newly published draft; no dispatch/poll loop."""
import datetime,json,pathlib,sys
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();head='5e6f184311019b9982e8544a18f3dc02c1b16e98'
pr=api('/pulls/475');assert pr['head']['sha']==head and pr['draft'] and pr['state']=='open'
runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':475,'head_sha':head,'merge_checkout':pr['merge_commit_sha'],'base_sha':pr['base']['sha'],'draft':True,'runs':[],'manual_dispatches':0,'next_routine_check_no_earlier_than':'2026-09-12T17:55:00Z'}
for r in runs:
    item={k:r.get(k) for k in ['id','name','path','event','head_sha','status','conclusion','run_attempt','created_at','updated_at']}
    item['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?filter=latest&per_page=100')['jobs']
    out['runs'].append(item)
path=pathlib.Path(__file__).resolve().parent/'pr475-initial-ci.json';path.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'head_sha':head,'merge_checkout':out['merge_checkout'],'runs':[{'id':r['id'],'name':r['name'],'status':r['status'],'conclusion':r['conclusion'],'jobs':[{'id':j['id'],'name':j['name'],'runner':j['runner_name'],'status':j['status'],'conclusion':j['conclusion'],'active_steps':[s['name'] for s in j['steps'] if s['status']=='in_progress']} for j in r['jobs']]} for r in out['runs']]}))
