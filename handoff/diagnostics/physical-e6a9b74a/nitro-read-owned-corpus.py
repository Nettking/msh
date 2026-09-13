import json,pathlib,sys,dataclasses,subprocess
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
sys.path.insert(0,str(root))
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runtime_binding as binding
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
b=binding.load(root/'.acceptance/runtime-control/runtime-binding.json',host_id='nitro',target_candidate_sha=sha)
rows=[]
for subject in ['recorder','history']:
 c=probes.ProbeContext(checkout=root,evidence_root=root/'evidence/v1-physical',commit=sha,host_id='nitro',os_category='posix',profile='school-control',scenario='P07' if subject=='recorder' else 'P12',assertion='aged-corpus' if subject=='recorder' else 'aged-history',options={'subject':subject},runtime_binding=b,harness_sha=sha)
 rows.append(dataclasses.asdict(probes.execute(probes.probe_spec('corpus-size'),c)))
r={'candidate':sha,'host':'nitro','purpose':'Read-only owned-corpus admission inventory; no timed acceptance claim','probes':rows,'data_top_level_names':[p.name for p in b.data_root.iterdir()],'P07':'NOT_STARTED','P12':'NOT_STARTED','protected_recorder_data_accessed':False}
print(json.dumps(r))
