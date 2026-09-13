"""Fail one owned build through its real controller; never activate containers."""
import datetime,hashlib,json,os,pathlib,re,subprocess,time
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910')
c=h/'.acceptance/runtime-control';sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
record=c/'P01-build-interruption-status.json'
assert not record.exists(),'Inspect the existing bounded fault action'
assert json.loads((c/'P01-qualified-status.json').read_text())['status']=='COMPLETED'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host','nettking','--scenario','P01','--assertion','windows-failed-build-cleanup']
def probe(args,label):
 p=subprocess.run(args,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; inspect evidence'
 return json.loads(p.stdout)
def cores():
 out=[]
 for svc in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',svc],cwd=r,env=env,text=True).strip();assert cid and '\n' not in cid
  item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
  image=json.loads(subprocess.check_output(['docker','image','inspect',item['Image']],text=True))[0]
  assert item['State']['Running'] and image['Config']['Labels']['no.fcp.build_commit']==sha
  out.append({'service':svc,'container_id':cid,'image_id':item['Image'],'restart_count':item['RestartCount']})
 return out
def children(pid):
 code=f"Get-CimInstance Win32_Process -Filter \"ParentProcessId={pid} AND Name='docker.exe'\" | Select-Object ProcessId,ParentProcessId,CommandLine,CreationDate | ConvertTo-Json -Compress"
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',code],capture_output=True,text=True,timeout=12,creationflags=subprocess.CREATE_NO_WINDOW)
 assert p.returncode==0,'Owned child enumeration failed'
 if not p.stdout.strip():return []
 data=json.loads(p.stdout);return data if isinstance(data,list) else [data]
status={'candidate':sha,'harness_sha':'501b528e9476878e6a6fe5cde8240b2d54b1d263','status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'physical_pass':False,'runtime_activation_requested':False,'protected_data_accessed':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
save()
try:
 before=cores();prep=probe([*runner,'prepare',*target],'build-interruption-prepare');status['prepare_id']=prep['prepare_id'];save()
 log=c/'build-interruption.private.log'
 with log.open('wb') as output:
  worker=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(r/'scripts/windows/fcp_host_build.ps1'),'-RepoRoot',str(r),'-OutputFile',str(c/'interrupted-build-result.txt'),'-ExpectedCommit',sha],cwd=r,env=env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
  status['controller_pid']=worker.pid;save();deadline=time.monotonic()+90;victim=None;builder=None
  while worker.poll() is None and time.monotonic()<deadline:
   for child in children(worker.pid):
    command=child.get('CommandLine') or ''
    found=re.search(r'compose\s+build\s+--builder\s+(fcp-build-[A-Fa-f0-9]+)\s+relay\s+flask\s+recorder',command)
    if found:
     current=next((p for p in children(worker.pid) if p['ProcessId']==child['ProcessId']),None)
     if current!=child:continue
     killed=subprocess.run(['taskkill.exe','/PID',str(child['ProcessId']),'/T','/F'],capture_output=True,text=True,timeout=15,creationflags=subprocess.CREATE_NO_WINDOW)
     assert killed.returncode==0,'Build client exited before interruption; no fault PASS'
     victim=child['ProcessId'];builder=found.group(1);break
   if victim:break
   time.sleep(0.25)
  status.update(build_client_pid=victim,owned_builder=builder,fault_injected=victim is not None);save()
  code=worker.wait(timeout=1100)
 assert victim is not None,'No live owned build client was interrupted'
 assert code!=0,'The interrupted controller did not refuse the build'
 text=log.read_text(errors='replace');assert 'core_image_build_failed' in text
 assert not (c/'interrupted-build-result.txt').exists(),'A failed build published a success result'
 after=cores();assert after==before,'A running core changed during the build-only fault'
 ids=subprocess.check_output(['docker','ps','-aq','--filter','name=buildx_buildkit_'+builder],text=True).split()
 builders=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 assert all(not b['State']['Running'] for b in builders),'Owned writer did not settle'
 status.update(controller_exit_code=code,owned_writer_stopped=True,core_containers_unchanged=True,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest());save()
 args=[*target,'--prepare-id',prep['prepare_id']]
 probe([*runner,'action',*args,'--note','Interrupted only the verified docker compose build client tree owned by the checked-in Windows build controller. The controller refused the build, ran its own bounded cache cleanup and stopped its writer. No success result was published; all running core container IDs, image IDs and restart counts stayed unchanged.'],'build-interruption-action')
 proof=probe([*runner,'verify',*args],'build-interruption-verify')
 status.update(status='COMPLETED',verdict=proof.get('verdict'),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc));save();raise
