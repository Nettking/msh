import sys,json,pathlib,datetime
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();runs=api('/actions/workflows/ci-test-sharding.yml/runs?per_page=20')['workflow_runs'];rows=[]
for r in runs:
 if r['created_at']<'2026-09-12T10:01:00Z':continue
 row={k:r.get(k) for k in ['id','head_sha','head_branch','event','status','conclusion','created_at','updated_at','html_url']};row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs'];rows.append(row)
 print(json.dumps({**{k:v for k,v in row.items() if k!='jobs'},'jobs':[{k:j.get(k) for k in ['id','name','runner_id','runner_name','conclusion','started_at','completed_at']} for j in row['jobs']]}))
o={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Recent CI test sharding runs since AQG registration was observed offline; bounded admission-evidence lookup, not completed main qualification polling','runner_id_of_interest':31,'runs':rows,'qualification_conclusion':'PENDING_NATIVE_REVIEW; no release-pool admission inferred from online state'}
pathlib.Path('handoff/diagnostics/aqg31-admission-evidence-lookup.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
