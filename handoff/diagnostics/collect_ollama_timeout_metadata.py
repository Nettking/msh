"""Read-only follow-up to D03; no inference or provider mutation."""
import datetime
import json
import pathlib
import subprocess

root = pathlib.Path(__file__).resolve().parent
cid = 'b18a34a67cc1faa14ae0850a7f0683e1e0e84d3aecd4140f4ec92c7ee6332a87'
def run(args):
    p = subprocess.run(args, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=20)
    return {'command': args, 'exit_code': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}
logs = run(['docker','logs','--since','2026-09-11T12:20:21Z','--until','2026-09-11T12:22:00Z',cid])
for key in ('stdout','stderr'):
    logs[key] = '\n'.join(line for line in logs[key].splitlines() if 'HEAD ' not in line and 'GET      "/api/tags"' not in line)
inspect = json.loads(run(['docker','inspect',cid])['stdout'])[0]
report = dict(candidate_sha='0536f03d67eb277e11573c2188d8e820399627e3',host='nettking',
    mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
    observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),logs=logs,
    state={k:inspect['State'].get(k) for k in ['Running','OOMKilled','StartedAt']},
    limits={k:inspect['HostConfig'].get(k) for k in ['Memory','MemorySwap','NanoCpus']},
    models=run(['docker','exec',cid,'ollama','ps']),
    stats=run(['docker','stats','--no-stream','--format','{{json .}}',cid]),
    correction='60 s was the audit-request bound, not the product default. N OllamaProbeTarget and adapter maximum are 120 s. No product deadline failure established by the first request alone.',
    mechanism='Server still loading/warming the model when client closed; canceled load and HTTP 499 after 1m0s. No OOM or restart. 1.5 GiB cgroup memory led Ollama to disable mmap. Causal impact of cap remains unproven.',
    state_changed=False,protected_recorder_data_untouched=True,physical_acceptance=False)
(root/'D03-provider-followup.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:v for k,v in report.items() if k!='logs'}))
