import json,os,pathlib,subprocess
windows=os.name=='nt'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
c=h/'.acceptance/runtime-control';host,lane=('nettking','windows') if windows else ('nitro','posix')
assert json.loads((c/'P01-qualified-status.json').read_text())['status']=='COMPLETED'
assert not (c/'P01-readonly-status.json').exists(),'Inspect retained proof'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()))
python=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'))
runner=[python,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
results=[]
for assertion in [lane+'-resource-baseline',lane+'-runtime-state']:
 p=subprocess.run([*runner,'probe','--commit','1aac6148759d7b2fd488ec26b97e1a786bdafa80','--host',host,'--scenario','P01','--assertion',assertion],cwd=h,env=env,capture_output=True,text=True,timeout=90)
 (c/(assertion+'.json')).write_text(p.stdout)
 if p.stderr:(c/(assertion+'.stderr')).write_text(p.stderr)
 result=json.loads(p.stdout);results.append({'assertion':assertion,'exit_code':p.returncode,'verdict':result.get('verdict')})
out={'host':host,'candidate':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','harness_sha':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','results':results,'physical_pass':False}
(c/'P01-readonly-status.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
