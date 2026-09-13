import json,pathlib,sys
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control';sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import redact_text
p=c/'P01-build-failure-status.json'
status=json.loads(p.read_text()) if p.exists() else {'status':'NOT_STARTED'}
if status.get('error'):status['error']=redact_text(status['error'],cwd=h)
print(json.dumps(status))
