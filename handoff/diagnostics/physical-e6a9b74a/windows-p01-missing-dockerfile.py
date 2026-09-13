"""One deterministic build-input fault, with source and running services unchanged."""
import datetime,hashlib,json,os,pathlib,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913');r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910');c=h/'.acceptance/runtime-control'
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077';record=c/'P01-missing-dockerfile-status.json';result=c/'missing-dockerfile-result.txt';log=c/'missing-dockerfile.private.log'
assert not record.exists() and not result.exists(),'Inspect the single existing deterministic fault'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
old_pid=json.loads((c/'P01-build-interruption-status.json').read_text())['controller_pid']
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',f"if(Get-Process -Id {old_pid} -ErrorAction SilentlyContinue){{exit 3}}"],capture_output=True)
assert p.returncode==0,'The previous controller is still active'
base={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};base.update(json.loads((c/'environment.private.json').read_text()));base.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
def cores():
 out=[]
 for svc in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',svc],cwd=r,env=base,text=True).strip();assert cid and '\n' not in cid
  item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
  assert item['State']['Running']
  out.append({'service':svc,'container':cid,'image':item['Image'],'restarts':item['RestartCount']})
 return out
before=cores();missing='.__fcp_p01_intentionally_missing_Dockerfile__';assert not (r/missing).exists()
overlay=c/'missing-dockerfile.compose.json';overlay.write_text(json.dumps({'services':{'flask':{'build':{'dockerfile':missing}}}},indent=2)+'\n')
fault=base.copy();fault['COMPOSE_FILE']=base['COMPOSE_FILE']+';'+str(overlay)
config=json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=r,env=fault,text=True))
assert config['services']['flask']['build']['dockerfile']==missing
for svc in ['flask','relay','recorder']:assert pathlib.Path(config['services'][svc]['build']['context']).resolve()==r.resolve()
status={'candidate':sha,'harness_sha':'501b528e9476878e6a6fe5cde8240b2d54b1d263','status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'fault':'Missing Dockerfile in temporary build-only Compose overlay','source_unchanged':True,'runtime_config_files_unchanged':True,'runtime_activation_requested':False,'physical_pass':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
save()
with log.open('wb') as output:
 p=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(r/'scripts/windows/fcp_host_build.ps1'),'-RepoRoot',str(r),'-OutputFile',str(result),'-ExpectedCommit',sha],cwd=r,env=fault,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
 status['controller_pid']=p.pid;save();exit_code=p.wait(timeout=1100)
raw=log.read_bytes();text=raw.decode(errors='replace')
status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),controller_exit_code=exit_code,missing_dockerfile_error_observed=missing in text and ('no such file' in text.lower() or 'not found' in text.lower()),success_marker_written=result.exists(),success_marker_matches_candidate=result.exists() and result.read_text().strip()==sha,core_containers_unchanged=cores()==before,log_sha256=hashlib.sha256(raw).hexdigest())
save();print(json.dumps(status))
