import json,pathlib,sys
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control';sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import redact_text
p=c/'candidate-admission.json';status=json.loads(p.read_text()) if p.exists() else {'status':'NOT_STARTED'}
out={k:status.get(k) for k in ['candidate','host','status','started_at','finished_at','launcher_exit_code','error_type','configured_http_status','core_images','physical_pass']}
log=c/'candidate-admission.private.log'
if log.exists():
 with log.open('rb') as stream:stream.seek(max(0,log.stat().st_size-2600));tail=stream.read().decode(errors='replace')
 out['log_bytes']=log.stat().st_size;out['current_phase_tail']=redact_text(tail,cwd=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source'))
print(json.dumps(out))
