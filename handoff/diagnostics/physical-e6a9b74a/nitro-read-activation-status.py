import json,pathlib
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
control=root/'.acceptance/runtime-control'
status=control/'activation-status.json'
r={'activation':json.loads(status.read_text()) if status.exists() else 'NOT_STARTED'}
for name in ['activation-wrapper.private.log','supported-start.private.log']:
 p=control/name
 if p.exists():r[name]=p.read_text(errors='replace')[-2200:]
print(json.dumps(r))
