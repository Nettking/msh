"""Check one actionable retry precondition after verified original-host repair."""
import datetime,json,pathlib,sys
dest=pathlib.Path('handoff/diagnostics');sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();head='440123f6bc6dc358eef3d233236bc14f91af60e0'
assert json.loads((dest/'D12-host-repair-reviewed.json').read_text())['status']=='HOST_CONFIGURATION_REPAIRED_AND_VERIFIED_AFFECTED_CI_CHECK_PENDING'
assert api('/pulls/473')['head']['sha']==head
run=api('/actions/runs/34701429430');assert run['head_sha']==head
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_sha':head,'reason':'Verified original-host correction creates a new actionable retry gate; this single metadata check is not a routine progress poll.','run':{k:run[k] for k in ['id','head_sha','status','conclusion','run_attempt']},'failed_job_to_retry':103573774515,'host_repair_evidence':'D12-host-repair-reviewed.json','status':'READY_FOR_ONE_AFFECTED_FAILED_JOB_RETRY' if run['status']=='completed' else 'DEFER_UNTIL_CURRENT_RUN_COMPLETES_NO_INTERRUPTION','passing_jobs_to_rerun':[],'source_changes':[],'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
if run['status']=='completed':out['completed_jobs']=api('/actions/runs/34701429430/jobs?per_page=100')['jobs']
(dest/'pr473-D12-revalidation-precondition.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8')+'\n## '+out['recorded_at']+' — D12 corrected-host retry precondition\n\nOne run-metadata check following verified host correction returned '+run['status']+' for PR473 release34701429430. Decision: '+out['status']+'. Receipt diagnostics/pr473-D12-revalidation-precondition.json. No active jobs interrupted or rerun; no additional routine poll until due. Exact PR head440123f6 unchanged.\n';p.write_text(s,encoding='utf-8');print(json.dumps({k:out[k] for k in ['status','run','failed_job_to_retry']}))
