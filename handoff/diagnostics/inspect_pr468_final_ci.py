"""Inspect only automatic checks for the integrated PR468 head; never rerun CI."""
import datetime,json,pathlib,sys
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();head='ba44100ec1e4cde19daba0d3723b991c11742316';p=api('/pulls/468');assert p['head']['sha']==head
rows=[]
for r in api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']:
 row={k:r.get(k) for k in ['id','path','head_sha','status','conclusion','event','run_attempt','created_at','updated_at','html_url']}
 row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs'];rows.append(row)
 print(json.dumps({'run':r['id'],'workflow':r['path'].split('/')[-1],'status':r['status'],'conclusion':r['conclusion'],'passed':sum(j['conclusion']=='success' for j in row['jobs']),'jobs':len(row['jobs']),'active':[{k:j.get(k) for k in ['id','name','runner_name','started_at']} for j in row['jobs'] if j['status']=='in_progress'],'failures':[{k:j.get(k) for k in ['id','name','runner_name']} for j in row['jobs'] if j['conclusion'] in ['failure','timed_out','cancelled']]}))
now=datetime.datetime.now(datetime.timezone.utc);o={'recorded_at':now.isoformat(),'pr':468,'head_sha':head,'draft':p['draft'],'state':p['state'],'merge_commit_sha':p['merge_commit_sha'],'runs':rows,'purpose':'AUTOMATIC_FINAL_HEAD_CI; existing 2/2 native equivalence proof retained, no manual native rerun'}
path=pathlib.Path('handoff/diagnostics')/('pr468-ba44100e-auto-'+now.strftime('%Y%m%dT%H%M%S')+'.json');path.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8');print(json.dumps({'snapshot':str(path)}))
