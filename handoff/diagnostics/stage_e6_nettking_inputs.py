"""Stage reviewed candidate inputs for the existing owned acceptance project."""
import hashlib,json,os,pathlib,subprocess
SHA='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
ROOT=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913')
OLD=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/nettking-73c779-runtime-control')
CONTROL=ROOT/'.acceptance/runtime-control'
assert not CONTROL.exists()
CONTROL.mkdir()
environment=json.loads((OLD/'environment.private.json').read_text())
overlay=json.loads((OLD/'compose.host.json').read_text())
# A native Windows readiness probe must reach the actual managed model server.
# Publish only the loopback socket; retain the existing read-only model mount.
assert not overlay['services']['ollama'].get('ports')
overlay['services']['ollama']['ports']=['127.0.0.1:11434:11434']
override=CONTROL/'compose.host.json'
override.write_text(json.dumps(overlay,indent=2)+'\n')
environment['COMPOSE_FILE']=str(ROOT/'docker-compose.yml')+';'+str(override)
environment['FCP_BUILD_COMMIT']=SHA
environment['FCP_CF7_OLLAMA_URL']='http://127.0.0.1:11434'
environment['FCP_CF7_OLLAMA_MODEL']=environment['FCP_AI_MODEL']
(CONTROL/'environment.private.json').write_text(json.dumps(environment,indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(environment)
p=subprocess.run(['docker','compose','config','--format','json'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Compose validation failed'
c=json.loads(p.stdout)
assert c['name']=='fcp-v1-73c779-nettking'
for name in ['flask','recorder','relay']:
 assert pathlib.Path(c['services'][name]['build']['context']).resolve()==ROOT.resolve()
 assert c['services'][name]['build']['args']['FCP_BUILD_COMMIT']==SHA
for name in ['flask','recorder']:
 for mount in c['services'][name].get('volumes',[]):
  if mount['target']=='/app/data':assert pathlib.Path(mount['source']).resolve()==(OLD/'data').resolve()
  if mount['target']=='/app/results':assert pathlib.Path(mount['source']).resolve()==(OLD/'results').resolve()
ollama=c['services']['ollama']
port=ollama['ports'][0]
assert port['host_ip']=='127.0.0.1' and str(port['published'])=='11434' and port['target']==11434
assert any(m.get('read_only') and m['target']=='/root/.ollama/models' for m in ollama['volumes'])
binding=json.loads((OLD/'runtime-binding-9b286f93.json').read_text())
binding.update(target_candidate_sha=SHA,acceptance_harness_sha=SHA,harness_checkout=str(ROOT))
binding['runtime']['working_directory']=str(ROOT)
binding['runtime']['config_files']=[str(ROOT/'docker-compose.yml'),str(override)]
(CONTROL/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
(CONTROL/'compose.resolved.private.json').write_text(p.stdout)
r={'candidate':SHA,'host':'nettking','source_clean':not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip(),'configuration_validated':True,'existing_owned_acceptance_project':True,'owned_data_results_mounts_preserved':True,'persistent_volume_names_preserved':True,'model_mount_readonly':True,'only_additional_exposure':'Ollama loopback socket for native readiness','protected_recorder_referenced':False,'runtime_changed':False,'physical_pass':False,'configuration_sha256':hashlib.sha256(p.stdout.encode()).hexdigest()}
(pathlib.Path(__file__).parent/'physical-e6a9b74a/nettking-input-review.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r,indent=2))
