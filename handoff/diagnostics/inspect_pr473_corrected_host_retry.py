"""Read only the single affected-check retry, retaining immutable attempt metadata."""
import datetime,json,pathlib,sys
dest=pathlib.Path('handoff/diagnostics');sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();run=api('/actions/runs/34701429430');assert run['head_sha']=='440123f6bc6dc358eef3d233236bc14f91af60e0'
jobs=api('/actions/runs/34701429430/jobs?filter=latest&per_page=100')['jobs']
actual=[j for j in jobs if j['started_at'] and j['started_at']>='2026-09-12T16:20:45Z']
now=datetime.datetime.now(datetime.timezone.utc);out={'recorded_at':now.isoformat(),'run':{k:run.get(k) for k in ['id','head_sha','run_attempt','status','conclusion','updated_at']},'actual_new_executions':actual,'all_latest_job_metadata':jobs,'note':'GitHub copies prior successful jobs with new IDs and earlier timestamps; these are not reexecutions.','original_dispatch':'pr473-D12-corrected-host-retry-dispatch.json'}
name='pr473-D12-retry-'+now.strftime('%Y%m%dT%H%M%S')+'.json';(dest/name).write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'snapshot':name,'run':out['run'],'new_executions':[{'id':j['id'],'name':j['name'],'runner':j['runner_name'],'status':j['status'],'conclusion':j['conclusion'],'steps':[{'name':s['name'],'status':s['status'],'conclusion':s['conclusion']} for s in j['steps'] if s['status']!='pending']} for j in actual]}))
