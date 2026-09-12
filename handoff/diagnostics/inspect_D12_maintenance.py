"""Observe only the short, dedicated CI host-maintenance run."""
import datetime,json,pathlib,subprocess,sys
dest=pathlib.Path('handoff/diagnostics');private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
sys.path.insert(0,str(private))
from github_qualification import client
api=client();control=subprocess.check_output(['git','rev-parse','22abd041'],text=True).strip()
runs=api('/actions/runs?head_sha='+control+'&per_page=100')['workflow_runs'];rows=[]
for r in runs:
    assert r['path']=='.github/workflows/ci-beast-git-context-maintenance.yml','Unexpected maintenance-triggered workflow: '+r['path']
    row={k:r.get(k) for k in ['id','path','head_sha','status','conclusion','event','run_attempt','created_at','updated_at','html_url']};row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs'];rows.append(row)
now=datetime.datetime.now(datetime.timezone.utc)
record={'recorded_at':now.isoformat(),'control_workflow_sha':control,'purpose':'Dedicated CI host maintenance without checkout/product deployment','runs':rows}
name='D12-maintenance-'+now.strftime('%Y%m%dT%H%M%S')+'.json';(dest/name).write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
if len(rows)==1:
    (private/'D12-maintenance-qualification-latest.json').write_text(json.dumps({'source_commit':control,'workflows':[{'workflow':'ci-beast-git-context-maintenance.yml','run_id':rows[0]['id'],'jobs':rows[0]['jobs']}]},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'snapshot':name,'runs':[{'id':r['id'],'status':r['status'],'conclusion':r['conclusion'],'jobs':[{'id':j['id'],'runner':j['runner_name'],'status':j['status'],'conclusion':j['conclusion'],'failed_steps':[s['name'] for s in j['steps'] if s['conclusion']=='failure']} for j in r['jobs']]} for r in rows]}))
