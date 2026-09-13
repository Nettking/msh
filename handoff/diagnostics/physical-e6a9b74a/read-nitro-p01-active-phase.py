import json,pathlib,sys
h=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913');sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import redact_text
c=h/'.acceptance/runtime-control';s=json.loads((c/'P01-qualified-status.json').read_text())
out={'status':s['status'],'completed_activations':len(s['completed_activations'])}
for p in sorted(c.glob('qualified-activation-*.private.log')):
 raw=p.read_text(errors='replace');out[p.name]={'bytes':p.stat().st_size,'tail':redact_text(raw[-2500:],cwd=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source'))}
print(json.dumps(out))
