"""Prepare a separate empty owned workbench using exact candidate source and cached models."""
import hashlib,json,os,pathlib,secrets,socket,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');old=h/'.acceptance/runtime-control';c=h/'.acceptance/onboarding-test'
r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';project='fcp-v1-1aac6148-onboarding-nettking'
assert not c.exists() and not r.exists(),'Inspect existing onboarding preparation; never reset it'
assert r.resolve().parent==pathlib.Path('C:/wsl').resolve()
tail=subprocess.check_output(['tailscale','ip','-4'],text=True,timeout=20).strip();assert '\n' not in tail
for address,port in [(tail,55040),(tail,58796),(tail,55041),('127.0.0.1',11436)]:
 with socket.socket() as s:s.bind((address,port))
oldenv=os.environ.copy();oldenv.update(json.loads((old/'environment.private.json').read_text()))
oldr=pathlib.Path(json.loads((old/'runtime-binding.json').read_text())['runtime']['working_directory'])
oldconfig=json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=oldr,env=oldenv,text=True,timeout=30))
subprocess.run(['git','worktree','add','--detach',str(r),sha],cwd=h,check=True,capture_output=True,text=True,timeout=90)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha and not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
c.mkdir();(c/'data').mkdir();(c/'results').mkdir()
settings={'COMPOSE_PROJECT_NAME':project,'FCP_BUILD_COMMIT':sha,'FCP_WEB_BIND':tail,'FCP_WEB_PORT':'55040','FCP_RELAY_BIND':tail,'FCP_AUTO_JOIN_PORT':'55041','FCP_DATA_DIR':str(c/'data'),'FCP_RESULTS_DIR':str(c/'results'),'FCP_SKIP_ORCHESTRATION':'1','FCP_FLASK_SECRET':secrets.token_urlsafe(48),'FCP_PASSWORD_SALT':secrets.token_urlsafe(32),'FCP_AI_MODEL':oldenv.get('FCP_AI_MODEL','llama3.2:3b'),'FCP_HUMAN_AUTH_BASE_URL':'http://'+tail+':55040','FCP_PAIRING_RELAY_URL':'ws://relay:8765','FCP_DEVICE_NAME':'Federation v1 isolated acceptance'}
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(settings)
config=json.loads(subprocess.check_output(['docker','compose','--file',str(r/'docker-compose.yml'),'config','--format','json'],cwd=r,env=env,text=True,timeout=30))
for service in ['flask','relay','recorder']:
 config['services'][service]['image']='fcp-v1-onboarding-'+service+':'+sha
 assert pathlib.Path(config['services'][service]['build']['context']).resolve()==r.resolve() and config['services'][service]['build']['args']['FCP_BUILD_COMMIT']==sha
config['services']['flask']['ports']=[{'target':5000,'published':'55040','host_ip':tail,'protocol':'tcp','mode':'ingress'}]
config['services']['relay']['ports']=[{'target':8765,'published':'58796','host_ip':tail,'protocol':'tcp','mode':'ingress'}]
config['services']['ollama']['ports']=[{'target':11434,'published':'11436','host_ip':'127.0.0.1','protocol':'tcp','mode':'ingress'}]
model_mounts=[]
for index,mount in enumerate(oldconfig['services']['ollama']['volumes']):
 item=dict(mount);item['read_only']=True
 if item['type']=='volume':
  original=oldconfig['volumes'][item['source']]['name'];key='read_only_acceptance_model_'+str(index)
  config.setdefault('volumes',{})[key]={'name':original,'external':True};item['source']=key
 model_mounts.append(item)
config['services']['ollama']['volumes']=model_mounts
for name,volume in config.get('volumes',{}).items():
 if not volume.get('external'):volume['name']=project+'_'+name
settings['FCP_RELAY_VOLUME_NAME']=config['volumes']['relay_state']['name']
settings['FCP_OLLAMA_VOLUME_NAME']=next(config['volumes'][m['source']]['name'] for m in model_mounts if m['type']=='volume' and m['target']=='/root/.ollama')
settings['FCP_MODEL_PROVIDER_VOLUME_NAME']=config['volumes'].get('model_provider_models',{'name':project+'_model_provider_models'})['name']
env.update(settings)
compose=c/'compose.private.json';compose.write_text(json.dumps(config,indent=2)+'\n');settings['COMPOSE_FILE']=str(compose);env.update(settings)
resolved=json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=r,env=env,text=True,timeout=30))
assert resolved['name']==project and all(m.get('read_only') for m in resolved['services']['ollama']['volumes'])
for service in ['flask','relay','recorder']:
 for mount in resolved['services'][service].get('volumes',[]):
  if mount['type']=='bind':assert pathlib.Path(mount['source']).resolve().is_relative_to(c.resolve()),'New workbench must bind only its new empty owned data/results'
credentials={'email':'federation-v1-acceptance@example.com','password':secrets.token_urlsafe(40),'purpose':'Local isolated acceptance owner only; no email or other external message is sent.'}
(c/'owner.private.json').write_text(json.dumps(credentials,indent=2)+'\n')
(c/'environment.private.json').write_text(json.dumps(settings,indent=2)+'\n')
binding={'schema':'fcp.v1.physical-runtime-binding.v1','host_id':'nettking','target_candidate_sha':sha,'acceptance_harness_sha':sha,'harness_checkout':str(h),'runtime_kind':'compose','runtime':{'project':project,'working_directory':str(r),'config_files':[str(compose)],'data_root':str(c/'data'),'results_root':str(c/'results')}}
(c/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
out={'candidate':sha,'harness_sha':sha,'status':'PREPARED','runtime_checkout':str(r),'new_empty_data_and_results':True,'separate_compose_project':project,'existing_models_read_only':True,'existing_installations_changed':False,'existing_authority_or_data_copied':False,'reset_requested':False,'protected_data_accessed':False,'AQG_requested':False,'web_port':55040,'relay_port':58796,'join_port':55041,'runtime_activated':False,'physical_pass':False}
(c/'preparation.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
