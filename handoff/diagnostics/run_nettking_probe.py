"""Execute one unchanged N probe; save diagnostic output, never campaign assertions."""
import dataclasses
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

N = '0536f03d67eb277e11573c2188d8e820399627e3'
H = pathlib.Path('C:/wsl/fcp-v1-0536f03d-nettking-20260911')
A = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
name = sys.argv[1]
allowed = {'checkout-identity','running-commit-identity','service-health',
           'host-resource-baseline','update-status','model-pull-floor','recorder-limits'}
assert name in allowed
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=H,text=True).strip() == N
assert not subprocess.check_output(['git','status','--porcelain'],cwd=H,text=True).strip()
out = pathlib.Path(__file__).parent / ('nettking-probe-' + name + '.json')
assert not out.exists(), 'Do not repeat a diagnostic without a new hypothesis.'
sys.path.insert(0, str(H))
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runtime_binding as bindings

binding = bindings.load(A/'nettking-73c779-runtime-control/runtime-binding-0536f03d.json',
                        host_id='nettking', target_candidate_sha=N)
context = probes.ProbeContext(checkout=H, evidence_root=A/'diagnostic-sweep-only',
                               commit=N, host_id='nettking', os_category='windows',
                               profile='local-ai', scenario='DIAGNOSTIC', assertion=name,
                               runtime_binding=binding, harness_sha=N)
record = dict(candidate_sha=N,host='nettking',stage='independent unchanged probe: '+name,
              mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
              command='run_nettking_probe.py '+name,
              checked_in_entrypoint='scripts.acceptance.v1_physical_probes.execute',
              probe_source_sha256=hashlib.sha256(pathlib.Path(probes.__file__).read_bytes()).hexdigest(),
              observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              state_changed=False,protected_recorder_data_untouched=True,physical_acceptance=False)
try:
    result = probes.execute(probes.probe_spec(name), context)
    record['diagnostic_probe'] = dataclasses.asdict(result)
except Exception as exc:
    record['error'] = dict(type=type(exc).__name__,message=str(exc))
finally:
    out.write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps(record))
