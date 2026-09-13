"""Read the existing Nitro P01 status without launching or changing the campaign."""
import json,pathlib,sys
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import redact_text
p=c/'P01-qualified-status.json';status=json.loads(p.read_text()) if p.exists() else {'status':'NOT_STARTED'}
out={k:status.get(k) for k in ['candidate','harness_sha','host','status','started_at','finished_at','error_type','physical_campaign_pass']}
out['completed_activations']=len(status.get('completed_activations',[]))
for name in ['three_activations','growth']:
 out[name]=status.get(name,{}).get('verdict')
if status.get('error'):out['error']=redact_text(status['error'],cwd=h)
receipt=c/'P01-qualified-dispatch.json'
if receipt.exists():out['dispatch']=json.loads(receipt.read_text())
print(json.dumps(out))
