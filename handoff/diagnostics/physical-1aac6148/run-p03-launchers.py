"""Exercise each outstanding native P03 launcher once through prepare/action/verify."""
import datetime,hashlib,json,os,pathlib,subprocess,urllib.request
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source');py=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'))
record=c/'P03-launchers-status.json';assert not record.exists(),'Inspect existing launcher proof; never repeat'
auto=json.loads((c/'P03-automated-status.json').read_text());assert auto['status']=='COMPLETED' and all(x['verdict']=='pass' for x in auto['results'])
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
start_env=env.copy();start_env.pop('FCP_BUILD_COMMIT',None)
host='nettking' if windows else 'nitro';assertions=['start-cmd','start-tailscale-cmd'] if windows else ['start-sh']
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
status={'candidate':sha,'harness_sha':sha,'host':host,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':[],'physical_pass':False,'protected_data_accessed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def git(root,*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
def call(args,label):
 p=subprocess.run(args,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; inspect retained evidence'
 return json.loads(p.stdout)
def core_images():
 rows=[]
 for service in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',service],cwd=r,env=env,text=True,timeout=30).strip();assert cid and '\n' not in cid
  item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=30))[0]
  image=json.loads(subprocess.check_output(['docker','image','inspect',item['Image']],text=True,timeout=30))[0]
  assert item['State']['Running'] and image['Config']['Labels']['no.fcp.build_commit']==sha
  rows.append({'service':service,'container':cid,'image':item['Image'],'restart_count':item['RestartCount']})
 return rows
save()
try:
 for assertion in assertions:
  for root in [h,r]:assert git(root,'rev-parse','HEAD')==sha and not git(root,'status','--porcelain')
  assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
  bind=env.get('FCP_WEB_BIND','127.0.0.1');port=env.get('FCP_WEB_PORT','5000')
  if assertion=='start-tailscale-cmd':
   tailnet=subprocess.check_output(['tailscale','ip','-4'],text=True,timeout=20).strip().splitlines();assert len(tailnet)==1
   assert bind in [tailnet[0],'0.0.0.0','::'],'Owned web binding does not expose the actual tailnet path'
   address=tailnet[0]
  else:address='127.0.0.1' if bind in ['0.0.0.0','::'] else bind
  target=['--commit',sha,'--host',host,'--scenario','P03','--assertion',assertion]
  prep=call([*runner,'prepare',*target],'P03-'+assertion+'-prepare');status.update(active_assertion=assertion,prepare_id=prep['prepare_id']);save()
  log=c/('P03-'+assertion+'.private.log')
  script={'start-cmd':'start.cmd','start-tailscale-cmd':'start-tailscale.cmd','start-sh':'start.sh'}[assertion]
  command=['cmd.exe','/d','/c',str(r/script)] if windows else ['bash',script]
  with log.open('wb') as output:
   p=subprocess.run(command,cwd=r,env=start_env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,timeout=1500,**({'creationflags':subprocess.CREATE_NO_WINDOW} if windows else {}))
  status.update(last_launcher_exit_code=p.returncode,last_log_sha256=hashlib.sha256(log.read_bytes()).hexdigest());save()
  assert p.returncode==0,'Supported launcher refused; inspect one retained attempt'
  images=core_images()
  with urllib.request.urlopen('http://'+address+':'+str(port)+'/onboarding',timeout=10) as response:assert response.status==200
  args=[*target,'--prepare-id',prep['prepare_id']]
  note='Executed the unmodified supported '+script+' once on the owned native installation. Exit0, configured HTTP200 and all three frozen candidate image labels verified. Existing state preserved; no fresh/reset option used.'
  if assertion=='start-tailscale-cmd':note+=' HTTP was verified over the actual logged-in tailnet address.'
  call([*runner,'action',*args,'--note',note],'P03-'+assertion+'-action')
  proof=call([*runner,'verify',*args],'P03-'+assertion+'-verify')
  status['results'].append({'assertion':assertion,'verdict':proof.get('verdict'),'prepare_id':prep['prepare_id'],'launcher_exit_code':0,'configured_http_status':200,'core_images':images,'log_sha256':status['last_log_sha256']});save()
 status.pop('active_assertion',None);status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc));save();raise
