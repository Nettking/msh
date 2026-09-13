"""Prepare explicit local controls for the new harness; no probe or activation."""
import json,os,pathlib,subprocess,sys
windows=os.name=='nt'
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913' if windows else '/home/martin/fcp-v1-501b528e-harness-20260913')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
host='nettking' if windows else 'nitro'
candidate='e6a9b74a1d555609eed6bf40c800e1258f1c9077';tooling='501b528e9476878e6a6fe5cde8240b2d54b1d263'
for root,sha in [(r,candidate),(h,tooling)]:
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==sha
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()
assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
before=old/'.acceptance/runtime-control-clean';after=h/'.acceptance/runtime-control'
assert not after.exists(),'Inspect the existing controls, never restage blindly'
after.mkdir(parents=True)
b=json.loads((before/'runtime-binding.json').read_text())
assert b['target_candidate_sha']==candidate and b['host_id']==host
b['acceptance_harness_sha']=tooling;b['harness_checkout']=str(h)
(after/'runtime-binding.json').write_text(json.dumps(b,indent=2)+'\n')
(after/'environment.private.json').write_bytes((before/'environment.private.json').read_bytes())
sys.path.insert(0,str(h))
from scripts.acceptance import v1_physical_runtime_binding
bound=v1_physical_runtime_binding.load(after/'runtime-binding.json',host_id=host,target_candidate_sha=candidate)
assert bound.acceptance_harness_sha==tooling
out={'host':host,'product_candidate_sha':candidate,'acceptance_harness_sha':tooling,'explicit_binding_valid':True,'source_clean':True,'runtime_build_context_clean':True,'runtime_changed':False,'physical_campaign_started':False,'physical_pass':False}
(after/'controls-staged.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
