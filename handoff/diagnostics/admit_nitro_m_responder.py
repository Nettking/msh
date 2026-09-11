"""Run the unmodified M responder replacement path against its proved N instance."""
import datetime,hashlib,json,os,pathlib,platform,subprocess,sys,time,urllib.request
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source';I=B/'inputs'
M='9b286f931497bf6291e215f6340443c5162826b0';N='0536f03d67eb277e11573c2188d8e820399627e3'
OUT=I/'responder-9b286f93-admission.json';LOG=I/'responder-9b286f93-native.log'
assert platform.node().casefold()=='nitro' and not OUT.exists() and not LOG.exists()
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_','PYTHON'))}
env.update(json.loads((I/'c03-activation/supported-start-9b286f93.environment.private.json').read_text()));env['PYTHONDONTWRITEBYTECODE']='1'
assert env['FCP_BUILD_COMMIT']==M and env['FCP_AUTO_JOIN_PORT']=='5151'
def run(args):return subprocess.check_output(args,cwd=R,env=env,text=True,timeout=25).strip()
assert run(['git','rev-parse','HEAD'])==M and not run(['git','status','--porcelain','--untracked-files=all'])
data=pathlib.Path(env['FCP_DATA_DIR']);assert data==pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data')
assert not (data/'federation/update-agent/request.json').exists()
sys.path.insert(0,str(R));from catalog.federation.host_mutation import host_mutation_lock
sys.path.insert(0,str(R/'scripts'));from federation_host_runner import load_host_module
bridge=load_host_module('tailnet_join_bridge');responder=load_host_module('tailnet_join_responder')
pid_file=bridge.pid_path(env);secret=bridge.secret_path(env)
record=json.loads(pid_file.read_text());old=record['pid'];assert old==1174977
assert record['start_token']==responder.process_start_token(old) and record['start_token'].endswith(':185384331')
assert os.readlink(pathlib.Path('/proc')/str(old)/'cwd')==str(R)
oldenv=dict(x.split(b'=',1) for x in (pathlib.Path('/proc')/str(old)/'environ').read_bytes().split(b'\0') if b'=' in x)
assert oldenv.get(b'FCP_BUILD_COMMIT')==N.encode()
assert bridge.read_secret(secret);secret_stat=secret.stat()
flask=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=flask'])
f=json.loads(run(['docker','inspect',flask]))[0];fe=dict(x.split('=',1) for x in f['Config']['Env'] if '=' in x)
assert fe['FCP_BUILD_COMMIT']==M and fe['FCP_AUTO_JOIN_PORT']=='5151'
ip=run(['tailscale','ip','-4']).splitlines()[0];ports=f['NetworkSettings']['Ports']['5000/tcp'];assert len(ports)==1 and ports[0]['HostIp']==ip
app='http://'+ip+':'+ports[0]['HostPort']
argv=[str(B/'host-venv/bin/python3'),'-B',str(R/'scripts/federation_host_runner.py'),'tailnet_join_responder','--bind',ip,'--port','5151','--app-url',app]
check=subprocess.run([*argv,'--check'],cwd=R,env=env,capture_output=True,timeout=15);assert check.returncode==0
def owned_listener(pid):
 inodes=[]
 for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
  x=line.split()
  if x[3]=='0A' and int(x[1].split(':')[1],16)==5151:inodes.append(x[9])
 links={os.readlink(p) for p in (pathlib.Path('/proc')/str(pid)/'fd').iterdir()}
 assert len(inodes)==1 and 'socket:['+inodes[0]+']' in links
 return inodes[0]
old_inode=owned_listener(old)
receipt={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':M,'host':'nitro','stage':'Owned M responder activation',
 'status':'PRECONDITIONS_VERIFIED','previous_pid':old,'previous_runtime':N,'previous_listener_inode':old_inode,
 'procedure':'Unmodified checked-in M federation_host_runner.py tailnet_join_responder; main performs exact-instance replacement',
 'state_changed':False,'physical_acceptance':False,'protected_recorder_data_untouched':True,'grant_or_enrollment_requested':False}
def save():OUT.write_text(json.dumps(receipt,indent=2)+'\n');OUT.chmod(0o600)
save()
try:
 with host_mutation_lock(R,timeout_seconds=30):
  assert run(['git','rev-parse','HEAD'])==M and not run(['git','status','--porcelain','--untracked-files=all'])
  assert responder.process_start_token(old)==record['start_token'] and owned_listener(old)==old_inode
  with LOG.open('xb') as log:
   p=subprocess.Popen(argv,cwd=R,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  receipt.update(status='REPLACEMENT_STARTED',pid=p.pid,state_changed=True);save()
  deadline=time.monotonic()+15
  while time.monotonic()<deadline:
   if p.poll() is not None:raise RuntimeError('Checked-in responder exited '+str(p.returncode))
   current=json.loads(pid_file.read_text())
   if current.get('pid')==p.pid:break
   time.sleep(.2)
  assert current.get('pid')==p.pid and current['start_token']==responder.process_start_token(p.pid)
  inode=owned_listener(p.pid)
  t=time.monotonic()
  with urllib.request.urlopen('http://'+ip+':5151/fcp/federation/tailnet-join/health',timeout=10) as response:
   body=json.load(response);assert response.status==200 and body.get('responder')=='ready' and body.get('tailscale') is True
  assert secret.stat().st_ino==secret_stat.st_ino and secret.stat().st_mtime_ns==secret_stat.st_mtime_ns
  actual_env=dict(x.split(b'=',1) for x in (pathlib.Path('/proc')/str(p.pid)/'environ').read_bytes().split(b'\0') if b'=' in x)
  assert actual_env[b'FCP_BUILD_COMMIT']==M.encode()
  receipt.update(status='EXACT_M_RESPONDER_LOCAL_VERIFIED',listener_inode=inode,
   native_python_executable=os.readlink(pathlib.Path('/proc')/str(p.pid)/'exe'),
   process_start_token=current['start_token'],source_clean=True,actual_environment_commit=M,
   source_hashes={name:hashlib.sha256((R/name).read_bytes()).hexdigest() for name in ('scripts/federation_host_runner.py','catalog/federation/tailnet_join_responder.py')},
   health={'status':200,'body':body,'elapsed_seconds':time.monotonic()-t},existing_secret_unchanged=True)
except Exception as exc:
 receipt.update(status='RESPONDER_ADMISSION_FAILED_INSPECT_BEFORE_RETRY',error=str(exc))
 receipt['native_log']=LOG.read_text(errors='replace')[-2500:] if LOG.exists() else ''
finally:
 receipt['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print(json.dumps(receipt,indent=2))
