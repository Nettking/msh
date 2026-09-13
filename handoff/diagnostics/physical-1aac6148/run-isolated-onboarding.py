"""Initialize only the new isolated workbench, then exercise the supported tailnet launcher."""
import argparse,datetime,hashlib,json,os,pathlib,subprocess,urllib.request
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913')
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe';record=c/'bootstrap-status.json'
parser=argparse.ArgumentParser();parser.add_argument('--resume-reviewed-volume-selection',action='store_true');options=parser.parse_args()
if options.resume_reviewed_volume_selection:
 original=json.loads(record.read_text());assert original['status']=='STOPPED' and original['phase']=='BASELINE_START' and original['baseline-start']['exit_code']==1
 assert (c/'explicit-volume-review.json').exists() and not (c/'baseline-start-recovery.private.log').exists()
 assert 'Multiple Federation coordinator volumes exist' in (c/'baseline-start.private.log').read_text()
 assert not (c/'bootstrap-original-volume-refusal.json').exists()
 (c/'bootstrap-original-volume-refusal.json').write_bytes(record.read_bytes())
else:assert not record.exists(),'Inspect existing onboarding; never restart or reset it'
prep=json.loads((c/'preparation.json').read_text());assert prep['candidate']==sha and prep['new_empty_data_and_results'] and not prep['existing_authority_or_data_copied']
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
start_env=env.copy();start_env.pop('FCP_BUILD_COMMIT',None)
status={'candidate':sha,'harness_sha':sha,'host':'nettking','status':'RUNNING','phase':'BASELINE_START','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'physical_pass':False,'reset_requested':False,'existing_installations_changed':False,'protected_data_accessed':False,'AQG_requested':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def git(*args):return subprocess.check_output(['git',*args],cwd=r,text=True).strip()
def launch(script,label):
 assert git('rev-parse','HEAD')==sha and not git('status','--porcelain')
 assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
 log=c/(label+'.private.log')
 with log.open('wb') as output:
  p=subprocess.run(['cmd.exe','/d','/c',str(r/script)],cwd=r,env=start_env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,timeout=1500,creationflags=subprocess.CREATE_NO_WINDOW)
 status[label]={'exit_code':p.returncode,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()};save()
 assert p.returncode==0,label+' refused; inspect one retained attempt'
def product(command,label,input_text=None):
 p=subprocess.run(['docker','compose','exec','-T','flask','python','-m','catalog.flask_app.services.zero_touch_federation_cli','--json',command],cwd=r,env=env,input=input_text,capture_output=True,text=True,timeout=120)
 (c/(label+'.private.stdout')).write_text(p.stdout);(c/(label+'.private.stderr')).write_text(p.stderr)
 status[label+'_exit_code']=p.returncode;save();assert p.returncode==0,label+' failed; do not repeat account initialization'
 return json.loads(p.stdout)
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host','nettking','--scenario','P03','--assertion','start-tailscale-cmd']
def call(command,label):
 p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; retain original evidence'
 return json.loads(p.stdout)
save()
try:
 launch('start.cmd','baseline-start-recovery' if options.resume_reviewed_volume_selection else 'baseline-start');status['phase']='LOCAL_OWNER_INITIALIZATION';save()
 current=product('status','before-initialize');assert current.get('state')=='identity-missing','Only the fresh empty installation may be initialized'
 owner=json.loads((c/'owner.private.json').read_text())
 try:product('initialize','initialize-owner',owner['email']+'\n'+owner['password']+'\n')
 finally:owner.clear()
 current=product('status','after-initialize');status['product_federation_state']=current.get('state');save();assert current.get('state')=='connected'
 status['phase']='TAILNET_LAUNCHER';save();prepared=call([*runner,'prepare',*target],'tailnet-prepare');status['prepare_id']=prepared['prepare_id'];save()
 launch('start-tailscale.cmd','tailnet-start')
 status['phase']='VERIFY';save();images=[]
 for service in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',service],cwd=r,env=env,text=True,timeout=30).strip();assert cid and '\n' not in cid
  item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=30))[0]
  image=json.loads(subprocess.check_output(['docker','image','inspect',item['Image']],text=True,timeout=30))[0]
  assert item['State']['Running'] and image['Config']['Labels']['no.fcp.build_commit']==sha
  images.append({'service':service,'container':cid,'image':item['Image']})
 url='http://'+env['FCP_WEB_BIND']+':'+env['FCP_WEB_PORT']+'/onboarding'
 with urllib.request.urlopen(url,timeout=10) as response:assert response.status==200
 remote=c/'cross-host-http.private.py';remote.write_text('import json,urllib.request\nwith urllib.request.urlopen('+repr(url)+',timeout=10) as r: print(json.dumps({"http_status":r.status}))\n')
 p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(remote),'--timeout','30'],capture_output=True,text=True,timeout=40)
 assert p.returncode==0 and json.loads(p.stdout)['http_status']==200,'Cross-host tailnet HTTP is not proven'
 args=[*target,'--prepare-id',prepared['prepare_id']]
 call([*runner,'action',*args,'--note','Ran the unmodified supported start-tailscale.cmd once on a separate owned workbench initialized through the product first-administrator/Federation workflow. No existing data/identity was copied or reset. Launcher exit0, exact candidate images, and HTTP200 over the actual tailnet address from both Windows and native Nitro were verified. Cached model storage is read-only; AQG was not requested.'],'tailnet-action')
 proof=call([*runner,'verify',*args],'tailnet-verify')
 status.update(status='COMPLETED',phase='COMPLETE',core_images=images,tailnet_http_status=200,cross_host_http_status=200,verdict=proof.get('verdict'),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
