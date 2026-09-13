"""One real missing-Dockerfile failure through the frozen host build controller."""
import datetime,hashlib,json,os,pathlib,re,subprocess,sys
windows=os.name=='nt';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910' if windows else '/home/martin/fcp-v1-73c779-nitro-20260910/source')
old=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913' if windows else '/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
py=str(old/('.venv/Scripts/python.exe' if windows else '.venv/bin/python'))
c=h/'.acceptance/runtime-control';record=c/'P01-build-failure-status.json'
assert not record.exists(),'Inspect existing fault; never repeat automatically'
assert json.loads((c/'P01-qualified-status.json').read_text())['status']=='COMPLETED'
assert all(x['verdict']=='pass' for x in json.loads((c/'P01-readonly-status.json').read_text())['results'])
if windows:assert json.loads((c/'P01-hour-status.json').read_text())['status']=='COMPLETED','Keep the declared passive hour undisturbed'
def git(root,*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
for p in [h,r]:assert git(p,'rev-parse','HEAD')==sha and not git(p,'status','--porcelain')
assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
host,lane=('nettking','windows') if windows else ('nitro','posix')
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host',host,'--scenario','P01','--assertion',lane+'-failed-build-cleanup']
status={'candidate':sha,'harness_sha':sha,'host':host,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'physical_pass':False,'runtime_activation_requested':False,'protected_data_accessed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def call(command,label):
 p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; inspect retained packet'
 return json.loads(p.stdout)
def cores():
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
 before=cores();status['core_before']=before;save();prep=call([*runner,'prepare',*target],'P01-build-failure-prepare');status['prepare_id']=prep['prepare_id'];save()
 missing='.__fcp_p01_intentionally_missing_Dockerfile__';assert not (r/missing).exists()
 override=c/'P01-missing-dockerfile.compose.json';assert not override.exists()
 override.write_text(json.dumps({'services':{svc:{'build':{'dockerfile':missing}} for svc in ['flask','relay','recorder']}},indent=2)+'\n')
 fault_env=env.copy();fault_env['COMPOSE_FILE']=env['COMPOSE_FILE']+env.get('COMPOSE_PATH_SEPARATOR',os.pathsep)+str(override)
 config=json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=r,env=fault_env,text=True,timeout=30))
 assert all(config['services'][svc]['build']['dockerfile']==missing and pathlib.Path(config['services'][svc]['build']['context']).resolve()==r.resolve() for svc in ['flask','relay','recorder'])
 marker=c/'P01-failed-build-result.txt';assert not marker.exists()
 if windows:
  command=['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(r/'scripts/windows/fcp_host_build.ps1'),'-RepoRoot',str(r),'-OutputFile',str(marker),'-ExpectedCommit',sha]
  builder='fcp-build-'+hashlib.sha256(str(r.resolve()).lower().encode()).hexdigest()[:24].upper()
 else:
  command=[py,'-m','catalog.federation.host_build','--repo-root',str(r),'--expected-commit',sha]
  sys.path.insert(0,str(r));from catalog.federation.host_build import builder_name
  builder=builder_name(r)
 log=c/'P01-build-failure.private.log';stdout=c/'P01-build-failure.private.stdout'
 with log.open('wb') as error,stdout.open('wb') as output:
  p=subprocess.run(command,cwd=r,env=fault_env,stdin=subprocess.DEVNULL,stdout=output,stderr=error,timeout=1100,**({'creationflags':subprocess.CREATE_NO_WINDOW} if windows else {}))
 text=log.read_text(errors='replace')+stdout.read_text(errors='replace')
 status.update(controller_exit_code=p.returncode,missing_dockerfile_error_observed=missing in text and 'no such file' in text.lower(),controller_refused_build=bool(re.search(r'FCP\s+host\s+build\s+refused:\s*core_image_\s*build_failed(?::1)?',text)),success_marker_written=marker.exists(),owned_builder=builder);save()
 assert p.returncode!=0 and status['missing_dockerfile_error_observed'] and status['controller_refused_build'] and not marker.exists(),'Unexpected fault result; do not record PASS'
 if not windows:assert not stdout.read_text().strip(),'Failed POSIX build published a success commit'
 assert cores()==before,'A core changed during the build-only fault'
 ids=subprocess.check_output(['docker','ps','-aq','--filter','name=buildx_buildkit_'+builder],text=True,timeout=30).split()
 builders=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=30)) if ids else []
 assert all(not b['State']['Running'] for b in builders),'Owned writer remains active'
 for root in [h,r]:assert git(root,'rev-parse','HEAD')==sha and not git(root,'status','--porcelain')
 status.update(core_containers_unchanged=True,owned_writer_stopped=True,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),stdout_sha256=hashlib.sha256(stdout.read_bytes()).hexdigest());save()
 args=[*target,'--prepare-id',prep['prepare_id']]
 call([*runner,'action',*args,'--note','Failed exactly one real build through the frozen native host build controller using a missing-Dockerfile Compose override outside the clean build context. The controller refused the failure, completed its own bounded cleanup, and stopped its writer. No success marker or commit was published. Running core container IDs, image IDs and restart counts remained unchanged.'],'P01-build-failure-action')
 proof=call([*runner,'verify',*args],'P01-build-failure-verify')
 status.update(status='COMPLETED',verdict=proof.get('verdict'),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc));save();raise
