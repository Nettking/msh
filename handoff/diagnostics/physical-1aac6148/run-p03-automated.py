"""Run only the fresh P03 automated assertions assigned to this native host."""
import datetime,json,os,pathlib,subprocess
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source');py=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'))
record=c/'P03-automated-status.json';assert not record.exists(),'Inspect existing P03 evidence; do not repeat'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
host='nettking' if windows else 'nitro';assertions=['update-cmd-disposition','windows-concurrent-launchers','launcher-vs-update'] if windows else ['posix-concurrent-launchers']
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
status={'candidate':sha,'harness_sha':sha,'host':host,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':[],'physical_pass':False,'runtime_activation_requested':False,'protected_data_accessed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
save()
for assertion in assertions:
 p=subprocess.run([*runner,'probe','--commit',sha,'--host',host,'--scenario','P03','--assertion',assertion],cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/('P03-'+assertion+'.json')).write_text(p.stdout)
 if p.stderr:(c/('P03-'+assertion+'.private.stderr')).write_text(p.stderr)
 result=json.loads(p.stdout);status['results'].append({'assertion':assertion,'exit_code':p.returncode,'verdict':result.get('verdict'),'evidence':result.get('evidence')});save()
status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
raise SystemExit(0 if all(x['verdict']=='pass' for x in status['results']) else 1)
