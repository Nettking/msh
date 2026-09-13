"""Expose only the named owned Ollama loopback socket and run native preflight."""
import datetime,hashlib,json,os,pathlib,subprocess
ROOT=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913')
CONTROL=ROOT/'.acceptance/runtime-control'
OUT=pathlib.Path(__file__).parent/'physical-e6a9b74a'
SHA='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
def run(args,env=None,timeout=50):
 p=subprocess.run(args,cwd=ROOT,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
 if p.returncode:raise RuntimeError('named model readiness action failed: '+args[0]+' exit '+str(p.returncode))
 return p.stdout.strip()
environment=json.loads((CONTROL/'environment.private.json').read_text())
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(environment)
assert run(['git','rev-parse','HEAD'])==SHA and not run(['git','status','--porcelain'])
assert json.loads((ROOT/'evidence/local-ai/gate-summary.json').read_text())['passed']
before=json.loads(run(['docker','inspect','a108c4ed7ef453353191ed9dda1c41b0fd1a281f5a86c960e01a1e6284d65f0d']))[0]
assert before['Config']['Labels']['com.docker.compose.project']=='fcp-v1-73c779-nettking'
assert before['Config']['Labels']['com.docker.compose.service']=='ollama' and before['State']['Running']
log=run(['docker','compose','up','-d','--no-deps','ollama'],env=env,timeout=90)
(CONTROL/'model-preflight-activation.private.log').write_text(log+'\n')
cid=run(['docker','compose','ps','-q','ollama'],env=env)
after=json.loads(run(['docker','inspect',cid]))[0]
assert after['Image']==before['Image'] and after['State']['Running']
assert sorted((m['Type'],m['Source'],m['Destination'],m['RW']) for m in before['Mounts'])==sorted((m['Type'],m['Source'],m['Destination'],m['RW']) for m in after['Mounts'])
bindings=after['HostConfig']['PortBindings']
assert bindings=={'11434/tcp':[{'HostIp':'127.0.0.1','HostPort':'11434'}]}
output=run([str(ROOT/'.venv/Scripts/python.exe'),'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(ROOT),'--evidence-root','evidence','preflight','--machine','local-ai','--commit',SHA],env=env)
summary=json.loads(output)
record={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':SHA,'host':'nettking','only_runtime_action':'Recreate owned Ollama with loopback port; identical pinned image and mounts','previous_container':before['Id'],'current_container':after['Id'],'readiness_preflight':summary,'product_core_source_still_previous':True,'protected_recorder_data_accessed':False,'physical_pass':False}
(OUT/'nettking-model-preflight.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record,indent=2))
