"""One fresh P01 activation/growth proof with the qualified independent harness."""
import datetime,hashlib,json,os,pathlib,subprocess
windows=os.name=='nt'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
c=h/'.acceptance/runtime-control';root=h/'evidence/v1-physical'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';tooling=sha
host,profile,lane=('nettking','local-ai','windows') if windows else ('nitro','school-control','posix')
python=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'))
status_file=c/'P01-qualified-status.json'
assert not status_file.exists(),'Inspect the existing bounded proof; never repeat blindly'
assert not root.exists(),'Do not reuse or rewrite old observations'
assert json.loads((c/'candidate-admission.json').read_text())['status']=='COMPLETED', 'Candidate admission must finish first'
def git(root,*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
assert git(h,'rev-parse','HEAD')==tooling and not git(h,'status','--porcelain')
assert git(r,'rev-parse','HEAD')==sha and not git(r,'status','--porcelain')
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))}
env.update(json.loads((c/'environment.private.json').read_text()))
start_env=env.copy();start_env.pop('FCP_BUILD_COMMIT',None);start_env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
env['FCP_BUILD_COMMIT']=sha
runner=[python,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(root),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host',host,'--scenario','P01']
status={'candidate':sha,'harness_sha':tooling,'host':host,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'completed_activations':[],'physical_campaign_pass':False,'ceilings_changed':False}
def save():status_file.write_text(json.dumps(status,indent=2)+'\n')
def call(args,label,allow_failure=False):
 p=subprocess.run(args,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.stderr')).write_text(p.stderr)
 if not allow_failure:assert p.returncode==0,label+' failed; inspect retained evidence'
 return json.loads(p.stdout)
save()
try:
 campaign=[python,'-m','scripts.acceptance.v1_physical_campaign','--checkout',str(r),'--evidence-root',str(root)]
 call([*campaign,'init','--commit',sha,'--operator','Martin','--harness-sha',tooling],'qualified-init')
 call([*campaign,'host','--commit',sha,'--host',host,'--role',profile,'--profile',profile],'qualified-host')
 call([*runner,'sample',*target,'--label','qualified-harness-baseline'],'qualified-baseline')
 prep=call([*runner,'prepare',*target,'--assertion',lane+'-three-activations','--option','activations=3'],'qualified-prepare')
 status['prepare_id']=prep['prepare_id'];save()
 for i in range(1,4):
  assert git(r,'rev-parse','HEAD')==sha and not git(r,'status','--porcelain')
  assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
  log=c/f'qualified-activation-{i}.private.log'
  command=['cmd.exe','/d','/c',str(r/'start.cmd')] if windows else ['bash','start.sh']
  with log.open('wb') as output:
   p=subprocess.run(command,cwd=r,env=start_env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,timeout=1200,**({'creationflags':subprocess.CREATE_NO_WINDOW} if windows else {}))
  assert p.returncode==0,'Supported activation failed; do not repeat'
  images=[]
  for svc in ['flask','relay','recorder']:
   cid=subprocess.check_output(['docker','compose','ps','-q',svc],cwd=r,env=env,text=True).strip();assert cid and '\n' not in cid
   info=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
   image=json.loads(subprocess.check_output(['docker','image','inspect',info['Image']],text=True))[0]
   assert info['State']['Running'] and image['Config']['Labels']['no.fcp.build_commit']==sha
   subprocess.run(['docker','exec',cid,'python','-c',"import pathlib; assert not any(pathlib.Path(p).exists() for p in ['/app/.acceptance','/app/evidence','/app/.env'])"],check=True,capture_output=True,timeout=20)
   images.append({'service':svc,'image':info['Image']})
  sample=call([*runner,'sample',*target,'--label',f'qualified-supported-activation-{i}'],f'qualified-sample-{i}')
  status['completed_activations'].append({'index':i,'exit_code':0,'images':images,'sample':sample,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()});save()
  print(json.dumps({'host':host,'activation':i,'candidate':sha,'sample_recorded':True}),flush=True)
 args=[*target,'--assertion',lane+'-three-activations','--prepare-id',status['prepare_id']]
 call([*runner,'action',*args,'--note','Performed three normal supported activations after preparation, verified exact frozen candidate images and clean build context, and recorded a bound resource sample after each. Harness independently pinned; limits unchanged.'],'qualified-action')
 status['three_activations']=call([*runner,'verify',*args,'--option','activations=3'],'qualified-three-activations',True);save()
 status['growth']=call([*runner,'probe',*target,'--assertion',lane+'-growth-bounded'],'qualified-growth',True)
 status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 print(json.dumps({'host':host,'status':'COMPLETED','three_activations':status['three_activations'].get('verdict'),'growth':status['growth'].get('verdict'),'physical_campaign_pass':False}),flush=True)
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc));save();raise
