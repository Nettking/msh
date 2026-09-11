"""One bounded N HTTP-client inference on existing owned Ollama; no deployment."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT/'nettking-existing-ollama-inference.json'
assert not OUT.exists()
N = '0536f03d67eb277e11573c2188d8e820399627e3'
PROJECT = 'fcp-v1-73c779-nettking'
MANIFEST = pathlib.Path('C:/Users/Martin/.ollama/models/manifests/registry.ollama.ai/library/llama3.2/3b')
EXPECTED = 'a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72'


def command(args, timeout=15):
    result = subprocess.run(args,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{args[:3]} exit {result.returncode}: {result.stderr[-500:]}')
    return result.stdout.strip()


containers={}
for service in ['flask','ollama']:
    cid=command(['docker','ps','-q','--filter','label=com.docker.compose.project='+PROJECT,
                 '--filter','label=com.docker.compose.service='+service])
    assert cid and '\n' not in cid
    containers[service]=json.loads(command(['docker','inspect',cid]))[0]
flask=containers['flask']; ollama=containers['ollama']
assert dict(x.split('=',1) for x in flask['Config']['Env'] if '=' in x)['FCP_BUILD_COMMIT']==N
assert hashlib.sha256(MANIFEST.read_bytes()).hexdigest()==EXPECTED
assert any(m['Destination']=='/root/.ollama/models' and not m['RW'] for m in ollama['Mounts'])
report=dict(candidate_sha=N,host='nettking',mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
            stage='local AI provider, independent of discovery and enrollment; no formal P10/B/CF7',
            observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            containers={k:dict(id=v['Id'],image=v['Image']) for k,v in containers.items()},
            model='llama3.2:3b',manifest_sha256=EXPECTED,model_downloaded=False,
            state_changed='One bounded inference may load model/GPU memory and append owned service logs; no persistent configuration or membership mutation.',
            protected_recorder_data_untouched=True,physical_acceptance=False,gpu_samples=[])
code="""import json,time
from catalog.capabilities.benchmarking.adapters.ollama import _bounded_json_request
payload={'model':'llama3.2:3b','stream':False,'messages':[{'role':'system','content':'Reply with exactly OK.'},{'role':'user','content':'OK'}],'options':{'num_predict':8,'temperature':0}}
begin=time.monotonic()
value=_bounded_json_request('POST','http://ollama:11434/api/chat',payload,60.0,32768)
print(json.dumps({'elapsed_seconds':time.monotonic()-begin,'response':{k:value.get(k) for k in ['model','done','done_reason','total_duration','load_duration','prompt_eval_count','eval_count','eval_duration','message']}}))
"""
report['procedure']=dict(entrypoint='N catalog.capabilities.benchmarking.adapters.ollama._bounded_json_request inside existing exact-N Flask',python_stdin_code=code,http_timeout_seconds=60,response_limit_bytes=32768)
stop=threading.Event()


def save():
    OUT.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')


def sample():
    while not stop.is_set():
        try:
            value=command(['nvidia-smi','--query-gpu=timestamp,memory.used,utilization.gpu','--format=csv,noheader,nounits'],timeout=5)
            report['gpu_samples'].append(value)
        except Exception as exc:
            report['gpu_samples'].append(type(exc).__name__)
        stop.wait(0.5)


save()
worker=threading.Thread(target=sample,daemon=True);worker.start()
try:
    report['inference']=json.loads(command(['docker','exec',flask['Id'],'python','-B','-c',code],timeout=70))
    report['loaded_models']=command(['docker','exec',ollama['Id'],'ollama','ps'])
    report['stats']=json.loads(command(['docker','stats','--no-stream','--format','{{json .}}',ollama['Id']]))
except Exception as exc:
    report['error']=dict(type=type(exc).__name__,message=str(exc))
finally:
    stop.set();worker.join(timeout=6)
    report['manifest_unchanged']=hashlib.sha256(MANIFEST.read_bytes()).hexdigest()==EXPECTED
    report['final_running']={k:json.loads(command(['docker','inspect',v['Id']]))[0]['State']['Running'] for k,v in containers.items()}
    report['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
print(json.dumps(report))
