"""Run the outstanding non-destructive P05 path-containment assertion once."""
import json,os,pathlib,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control';record=c/'P05-path-containment-status.json';assert not record.exists(),'Preserve the existing proof'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env['FCP_BUILD_COMMIT']='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
command=['C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe','-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json'),'probe','--commit',env['FCP_BUILD_COMMIT'],'--host','nettking','--scenario','P05','--assertion','malformed-timestamp-path']
p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
(c/'P05-path-containment.json').write_text(p.stdout)
if p.stderr:(c/'P05-path-containment.private.stderr').write_text(p.stderr)
out={'candidate':env['FCP_BUILD_COMMIT'],'host':'nettking','exit_code':p.returncode,'result':json.loads(p.stdout),'physical_pass':False,'protected_data_accessed':False}
record.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out));raise SystemExit(p.returncode)
