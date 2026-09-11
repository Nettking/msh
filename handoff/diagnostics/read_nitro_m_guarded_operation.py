"""Read only retained operation state; omit private control identifiers from output."""
import json,pathlib
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/inputs')
p=B/'supported-start-9b286f93-guarded.json'
if p.exists():
 d=json.loads(p.read_text());d.pop('control_before',None)
 print(json.dumps(d,indent=2))
else:
 print(json.dumps({'status':'NO_OPERATION_RECEIPT','controller_log_tail':(B/'m-activation-guarded-controller.log').read_text(errors='replace')[-2000:]}))
