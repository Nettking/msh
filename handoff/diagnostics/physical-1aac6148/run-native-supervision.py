"""Real Windows supervisor restart/continuity/stop with unchanged production policy."""
import ctypes,datetime,json,os,pathlib,subprocess,time
from controlled_mtconnect_agent import Agent
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/native-faults';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-native-faults-20260913');data=c/'data';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
record=c/'P06-supervision-status.json';assert not record.exists(),'Inspect existing supervision before any continuation'
enrollment=json.loads((c/'P06-enrollment-status.json').read_text());assert enrollment['membership_saved'] and enrollment['native_federation_status']=='connected'
assert (data/'federation/onboarding/remote_pairing.json').exists()
for checkout in [h,r]:
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==sha
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout,text=True).strip()
agent=Agent();agent.nodes=json.loads((c/'agent-maximum-continuation-observations.json').read_text())['nodes'];agent.change('normal')
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))}
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
status={'candidate':sha,'harness_sha':sha,'status':'RUNNING','phase':'SUPPORTED_SUPERVISOR_START','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':[],'supervisor_defaults':{'started_runtime_seconds':10,'healthy_runtime_seconds':120,'restart_backoff_seconds':[5,15,45,120],'max_rapid_restarts':5},'policy_overrides':False,'new_grant_requested':False,'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False}
proc=None;actual_pid=None
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def read(path):
 try:return json.loads(path.read_text())
 except (OSError,json.JSONDecodeError):return {}
def state():return read(data/'source_state/mtconnect_recorder_status.json')
def checkpoints():return {k:int(v['next_sequence']) for k,v in read(data/'source_state/mtconnect_recorder_state.json').get('sources',{}).items()}
def wait_for(test,seconds,label):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  if proc is not None and proc.poll() is not None:raise RuntimeError('Supervisor exited before '+label)
  value=test()
  if value:return value
  time.sleep(0.25)
 raise RuntimeError('Bounded observation unavailable: '+label)
def call(args,label):
 p=subprocess.run([*runner,*args],cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; retain evidence'
 return json.loads(p.stdout)
def prepare(scenario,assertion):
 args=['--commit',sha,'--host','nettking','--scenario',scenario,'--assertion',assertion]
 prepared=call(['prepare',*args],scenario+'-'+assertion+'-prepare');return args,prepared['prepare_id']
def finish(args,pid,note,detail):
 full=[*args,'--prepare-id',pid];label=args[-1]
 call(['action',*full,'--note',note],label+'-action');result=call(['verify',*full],label+'-verify');assert result['verdict']=='pass'
 status['results'].append({'scenario':args[-3],'assertion':label,'prepare_id':pid,'verdict':'pass','detail':detail});save()
def process_info(pid):
 code=f'Get-CimInstance Win32_Process -Filter "ProcessId={int(pid)}" | Select-Object ProcessId,ParentProcessId,CreationDate,CommandLine | ConvertTo-Json -Compress'
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',code],capture_output=True,text=True,timeout=15)
 return json.loads(p.stdout) if p.returncode==0 and p.stdout.strip() else None
def verify_owned_child(runtime):
 pid=int(runtime['pid']);item=process_info(pid);assert item and 'scripts.start_tailscale_recorder' in item['CommandLine']
 ancestors=[];parent=int(item['ParentProcessId'])
 for _ in range(4):
  ancestors.append(parent)
  if parent==proc.pid:break
  info=process_info(parent);assert info;parent=int(info['ParentProcessId'])
 assert proc.pid in ancestors and runtime['build_commit']==sha and runtime['supervisor_session'] and runtime['process_nonce']
 return pid
def stop_supervisor():
 if proc is None or proc.poll() is not None:return
 kernel=ctypes.WinDLL('kernel32',use_last_error=True);kernel.FreeConsole();assert kernel.AttachConsole(proc.pid)
 try:
  assert kernel.SetConsoleCtrlHandler(None,True);assert kernel.GenerateConsoleCtrlEvent(0,0)
  proc.wait(timeout=40)
 finally:kernel.FreeConsole();kernel.SetConsoleCtrlHandler(None,False)
 status['operator_stop_exit_code']=proc.returncode;save()
 assert proc.returncode in [0,-1073741510], 'Unexpected operator-stop exit'
 if actual_pid is not None:assert process_info(actual_pid) is None,'Recorder child survived operator stop'
save()
try:
 wrapper=c/'start-production-supervisor.ps1'
 wrapper.write_text("$ErrorActionPreference = 'Stop'\n$recorderArguments = @('--data-dir', '"+str(data)+"', '--no-auto-scan')\n& '"+str(r/'scripts/windows/fcp_recorder_supervisor.ps1')+"' -RepoRoot '"+str(r)+"' -PythonExecutable '"+py+"' -RecorderArguments $recorderArguments\nexit $LASTEXITCODE\n")
 agent.start();startup=subprocess.STARTUPINFO();startup.dwFlags|=subprocess.STARTF_USESHOWWINDOW;startup.wShowWindow=0
 with (c/'P06-supervisor.private.log').open('wb') as log:
  proc=subprocess.Popen(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(wrapper)],cwd=r,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,startupinfo=startup,creationflags=subprocess.CREATE_NEW_CONSOLE)
 status['supervisor_pid']=proc.pid;save()
 def ready():
  value=state();runtime=value.get('native_runtime',{})
  return value if value.get('state')=='recording' and runtime.get('supervisor_session') and runtime.get('build_commit')==sha else None
 value=wait_for(ready,100,'native supported startup');runtime=value['native_runtime'];actual_pid=verify_owned_child(runtime)
 status.update(phase='PREPARE_ONE_CHILD_CRASH',initial_native_runtime=runtime,native_federation=value.get('federation',{}));save();print(json.dumps({'phase':status['phase']}),flush=True)
 # Let the first child pass the production started-runtime boundary, unchanged.
 initial_time=time.monotonic()
 while time.monotonic()-initial_time<11:
  assert proc.poll() is None;time.sleep(0.5)
 prepared=[prepare(s,a) for s,a in [('P06','unexpected-child-restart'),('P06','checkpoint-continuity'),('P05','native-recorder-crash')]]
 before=checkpoints();assert len(before)==8;actual_pid=verify_owned_child(runtime)
 status.update(phase='ONE_CHILD_CRASH',crashed_native_pid=actual_pid);save();started=time.monotonic()
 kernel=ctypes.WinDLL('kernel32',use_last_error=True);kernel.OpenProcess.restype=ctypes.c_void_p;kernel.OpenProcess.argtypes=[ctypes.c_uint,ctypes.c_int,ctypes.c_uint];kernel.TerminateProcess.argtypes=[ctypes.c_void_p,ctypes.c_uint];kernel.CloseHandle.argtypes=[ctypes.c_void_p]
 handle=kernel.OpenProcess(1,False,actual_pid);assert handle
 try:assert kernel.TerminateProcess(handle,42)
 finally:kernel.CloseHandle(handle)
 def replacement():
  value=ready()
  if not value:return None
  now=value['native_runtime']
  return value if now['process_nonce']!=runtime['process_nonce'] and now['supervisor_session']==runtime['supervisor_session'] else None
 new=wait_for(replacement,100,'bounded supervised replacement');actual_pid=verify_owned_child(new['native_runtime'])
 wait_for(lambda:len(checkpoints())==8 and all(checkpoints().get(k,0)>v for k,v in before.items()),30,'post-crash checkpoint advancement')
 detail={'before_native_runtime':runtime,'after_native_runtime':new['native_runtime'],'before_checkpoints':before,'after_checkpoints':checkpoints(),'observed_restart_seconds':time.monotonic()-started,'default_policy_preserved':True}
 for args,pid in prepared:finish(args,pid,'Terminated exactly the verified owned native recorder child with exit42 after it crossed the unchanged started-runtime boundary. The actual checked-in supervisor restarted it with a fresh process nonce and the same supervisor session/candidate. All eight durable checkpoints advanced without reset; Federation sharing was established by the supported launcher.',detail)
 args,pid=prepare('P06','operator-stop');status['phase']='OPERATOR_STOP';save();stop_supervisor();time.sleep(6)
 assert proc.poll() is not None and process_info(actual_pid) is None
 finish(args,pid,'Sent real Windows Ctrl+C to the isolated supervisor console. The supervisor and its native recorder child exited and neither restarted during the following observation. No policy parameters were overridden.',{'supervisor_exit_code':proc.returncode,'child_absent':True,'observation_seconds':6})
 status.update(status='COMPLETED',phase='COMPLETE',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 try:stop_supervisor()
 except BaseException as cleanup:status['cleanup_error_type']=type(cleanup).__name__;save()
 raise
finally:
 (c/'P06-agent-observations.json').write_text(json.dumps(agent.snapshot(),indent=2)+'\n');agent.close();save()
