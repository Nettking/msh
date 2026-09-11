"""Invoke unchanged N adapter with its default bound; persist no product evidence."""
import datetime
import hashlib
import json
import pathlib
import subprocess

root = pathlib.Path(__file__).resolve().parent
out = root/'nettking-actual-ollama-adapter.json'
assert not out.exists(), 'Do not repeat a completed diagnostic blindly'
n = '0536f03d67eb277e11573c2188d8e820399627e3'
flask = '7468794b6dfc0e24aff14af91950bbc37a9ce5ae6ba912d1eec4632080a0e5a6'
ollama = 'b18a34a67cc1faa14ae0850a7f0683e1e0e84d3aecd4140f4ec92c7ee6332a87'
manifest = pathlib.Path('C:/Users/Martin/.ollama/models/manifests/registry.ollama.ai/library/llama3.2/3b')
expected = 'a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72'
def run(args, timeout=15):
    return subprocess.run(args,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
inspect = json.loads(run(['docker','inspect',flask,ollama]).stdout)
assert all(c['State']['Running'] for c in inspect)
assert 'FCP_BUILD_COMMIT='+n in inspect[0]['Config']['Env']
assert hashlib.sha256(manifest.read_bytes()).hexdigest() == expected
assert any(m['Destination']=='/root/.ollama/models' and not m['RW'] for m in inspect[1]['Mounts'])
prior = run(['docker','exec',ollama,'ollama','ps'])
assert prior.returncode == 0 and len(prior.stdout.strip().splitlines()) == 1, 'Prior load not idle'
code = '''import dataclasses,hashlib,json,pathlib,time
from catalog.capabilities.benchmarking.adapters.ollama import OllamaProbeTarget,OllamaBenchmarkAdapter
from catalog.capabilities.benchmarking.runner import BenchmarkExecutionContext
target=OllamaProbeTarget(service_id='diagnostic-existing-ollama',display_label='Existing local Ollama',base_url='http://ollama:11434',model='llama3.2:3b')
adapter=OllamaBenchmarkAdapter((target,),trusted_host_predicate=lambda host: host=='ollama')
start=time.monotonic()
ctx=BenchmarkExecutionContext(device_id='nettking-diagnostic-only',target_service_id=target.service_id,dependency_inputs=adapter.dependency_inputs(target.service_id),_deadline=start+adapter.definition.max_duration_seconds,_monotonic=time.monotonic,_cancelled=lambda:False)
observation=adapter.benchmark(ctx)
print(json.dumps({'elapsed_seconds':time.monotonic()-start,'target_timeout_seconds':target.request_timeout_seconds,'definition_max_seconds':adapter.definition.max_duration_seconds,'source_sha256':hashlib.sha256(pathlib.Path('/app/catalog/capabilities/benchmarking/adapters/ollama.py').read_bytes()).hexdigest(),'observation':dataclasses.asdict(observation)}))
'''
report = dict(candidate_sha=n, host='nettking',mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
    stage='N Ollama adapter isolated provider execution; no P10/B/CF7 or enrollment',
    observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    procedure=code,containers={c['Name']:dict(id=c['Id'],image=c['Image']) for c in inspect},
    prior_loaded_models=prior.stdout,model_manifest=expected,
    state_changed='Ephemeral model/GPU memory and owned logs only; no benchmark store, publication, authority, deployment or configuration change',
    protected_recorder_data_untouched=True,physical_acceptance=False)
def save():
    out.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
save()
try:
    result = run(['docker','exec',flask,'python','-B','-c',code],timeout=135)
    report['exit_code']=result.returncode
    report['stderr']=result.stderr
    report['result']=json.loads(result.stdout) if result.returncode==0 else result.stdout
except Exception as exc:
    report['error']=dict(type=type(exc).__name__,message=str(exc))
finally:
    report['manifest_unchanged']=hashlib.sha256(manifest.read_bytes()).hexdigest()==expected
    report['loaded_models_after']=run(['docker','exec',ollama,'ollama','ps']).stdout
    report['gpu_after']=run(['nvidia-smi','--query-gpu=memory.used,utilization.gpu','--format=csv,noheader,nounits']).stdout
    report['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
print(json.dumps(report))
