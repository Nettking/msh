"""Hourly state snapshot of automatic migration PR workflows; read-only."""
import datetime,json,pathlib,sys
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();rows=[]
for pr,head in [(468,'f3abe5452db2f21593a688bc62bc5f4b22d5c40e'),(469,'660bf23269893305c7e8ffcc4c910a7efaed567d')]:
    p=api('/pulls/'+str(pr));assert p['head']['sha']==head
    runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
    for r in runs:
        row={k:r.get(k) for k in ['id','path','head_sha','status','conclusion','event','run_attempt','created_at','updated_at','html_url']};row['pr']=pr
        row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
        rows.append(row)
        print(json.dumps({'pr':pr,'id':r['id'],'workflow':r['path'].split('/')[-1],'status':r['status'],'conclusion':r['conclusion'],'passed':sum(j['conclusion']=='success' for j in row['jobs']),'jobs':len(row['jobs']),'active':[{'id':j['id'],'name':j['name'],'runner':j['runner_name'],'started_at':j['started_at']} for j in row['jobs'] if j['status']=='in_progress'],'failed':[{'id':j['id'],'name':j['name'],'runner':j['runner_name'],'steps':len(j['steps']),'failing_steps':[s['name'] for s in j['steps'] if s['conclusion']=='failure']} for j in row['jobs'] if j['conclusion'] in ['failure','cancelled','timed_out']]}))
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'purpose':'AUTOMATIC_CI_MIGRATION_RUNS_NOT_PHYSICAL_ACCEPTANCE','runs':rows}
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S');p=pathlib.Path('handoff/diagnostics')/('ci-migration-auto-runs-'+stamp+'.json');p.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
