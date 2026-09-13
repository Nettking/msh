"""One bounded native recorder campaign with real loopback HTTP faults and defaults."""
import argparse,ctypes,datetime,hashlib,json,os,pathlib,subprocess,sys,time
from controlled_mtconnect_agent import Agent

sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/native-faults';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-native-faults-20260913')
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
parser=argparse.ArgumentParser();parser.add_argument('--resume-after-observer-correction',action='store_true');parser.add_argument('--resume-maximum-start-boundary',action='store_true');options=parser.parse_args()
assert not (options.resume_after_observer_correction and options.resume_maximum_start_boundary)
resuming=options.resume_after_observer_correction or options.resume_maximum_start_boundary
suffix='-maximum-continuation' if options.resume_maximum_start_boundary else ('-recovery' if resuming else '')
record=c/('ingress'+suffix+'-status.json')
if options.resume_maximum_start_boundary:
 original=json.loads((c/'ingress-recovery-status.json').read_text())
 assert original['status']=='STOPPED' and original['phase']=='P04/maximum-ingress' and original['operator_stop_exit_code']==0
 assert [x['assertion'] for x in original['results']]==['concurrent-sources'] and original['results'][0]['verdict']=='pass'
 assert not record.exists(),'Inspect existing maximum-input continuation; do not repeat it'
elif options.resume_after_observer_correction:
 original=json.loads((c/'ingress-status.json').read_text())
 assert original['status']=='STOPPED' and original['phase']=='STARTUP' and original['error_type']=='PermissionError' and original['operator_stop_exit_code']==0 and not original['results']
 assert not record.exists(),'Inspect existing recovery; do not repeat it'
else:assert not c.exists() and not r.exists(),'Inspect existing native fault campaign before any continuation'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=h,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=h,text=True).strip()
if not resuming:
 subprocess.run(['git','worktree','add','--detach',str(r),sha],cwd=h,check=True,capture_output=True,text=True,timeout=60)
 c.mkdir();(c/'data').mkdir();(c/'results').mkdir()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
data=c/'data'
binding={'schema':'fcp.v1.physical-runtime-binding.v1','host_id':'nettking','target_candidate_sha':sha,'acceptance_harness_sha':sha,'harness_checkout':str(h),'runtime_kind':'native-recorder','runtime':{'data_root':str(data),'results_root':str(c/'results'),'recorder_status_file':str(data/'source_state/mtconnect_recorder_status.json')}}
if resuming:assert json.loads((c/'runtime-binding.json').read_text())==binding
else:(c/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env['FCP_RECORDER_BUILD_COMMIT']=sha
agent=Agent();proc=None;status={'candidate':sha,'harness_sha':sha,'status':'RUNNING','phase':'STARTUP','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'results':[],'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False,'request_deadline_seconds':10,'recorder_limit_overrides':False,'P07_started':False,'P12_started':False}
if resuming:
 agent.nodes=json.loads((c/('agent-recovery-observations.json' if options.resume_maximum_start_boundary else 'agent-observations.json')).read_text())['nodes']
 status['observer_correction']='Treat Windows transient sharing refusal as unavailable within the original observation bound; verify native child ancestry through the OS rather than equating the venv launcher PID with its Python child. Preserve prior corpus/checkpoints/receipts.'
if options.resume_maximum_start_boundary:
 status['results']=original['results']
 status['maximum_fixture_correction']='The previous live mode switch reached an already-in-flight sample before the maximum current/probe responses were served. Arm the full maximum-input sequence before native startup, reuse the prior preparation and preserve the incomplete input trace. No concurrent-sources rerun.'
 agent.change('maximum');maximum_target=agent.nodes['s08']['last']+1
def save():record.write_text(json.dumps(status,indent=2)+'\n')
def phase(value):status['phase']=value;save();print(json.dumps({'phase':value}),flush=True)
def read(path):
 try:return json.loads(path.read_text())
 except (OSError,json.JSONDecodeError):return {}
def state():return read(data/'source_state/mtconnect_recorder_status.json')
def checkpoints():return {k:int(v['next_sequence']) for k,v in read(data/'source_state/mtconnect_recorder_state.json').get('sources',{}).items()}
def wait_for(test,seconds,label):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  if proc is not None and proc.poll() is not None:raise RuntimeError('Native recorder exited before '+label)
  value=test()
  if value:return value
  time.sleep(0.2)
 raise RuntimeError('Bounded observation unavailable: '+label)
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
def call(args,label):
 p=subprocess.run([*runner,*args],cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; retain original result'
 return json.loads(p.stdout)
def prepare(scenario,assertion):
 phase(scenario+'/'+assertion);args=['--commit',sha,'--host','nettking','--scenario',scenario,'--assertion',assertion]
 p=call(['prepare',*args],scenario+'-'+assertion+'-prepare');status['active_prepare_id']=p['prepare_id'];save();return args,p['prepare_id']
def finish(args,identifier,note,detail):
 label=args[-1];full=[*args,'--prepare-id',identifier]
 call(['action',*full,'--note',note],label+'-action')
 proof=call(['verify',*full],label+'-verify');assert proof['verdict']=='pass'
 status['results'].append({'assertion':label,'verdict':'pass','prepare_id':identifier,'detail':detail});status.pop('active_prepare_id',None);save()
def source_error(name,phrase):return phrase.lower() in str(state().get('source_status',{}).get(name,{}).get('last_error','')).lower()
def normal_recovery(source):
 prior=wait_for(lambda:checkpoints() or None,5,'checkpoint before source recovery');before=prior[source];agent.change('normal')
 wait_for(lambda:checkpoints().get(source,0)>before and not state().get('source_status',{}).get(source,{}).get('last_error'),30,'source recovery '+source)
def stop_native():
 if proc is None or proc.poll() is not None:return
 kernel=ctypes.WinDLL('kernel32',use_last_error=True)
 kernel.FreeConsole();assert kernel.AttachConsole(proc.pid),'Cannot attach to the isolated recorder console'
 try:
  assert kernel.SetConsoleCtrlHandler(None,True)
  assert kernel.GenerateConsoleCtrlEvent(0,0),'Cannot send Ctrl+C to isolated recorder console'
  proc.wait(timeout=35)
 finally:kernel.FreeConsole();kernel.SetConsoleCtrlHandler(None,False)
 status['operator_stop_exit_code']=proc.returncode;save();assert proc.returncode==0,'Native recorder did not stop normally'
save()
try:
 agent.start();startup=subprocess.STARTUPINFO();startup.dwFlags|=subprocess.STARTF_USESHOWWINDOW;startup.wShowWindow=0
 command=[py,str(r/'start_recorder.py'),'--data-dir',str(data),'--no-auto-scan',*[f'{name}=http://127.0.0.1:{agent.server.server_port}/{name}' for name in agent.nodes]]
 (c/('launch-review'+suffix+'.json')).write_text(json.dumps({'candidate':sha,'source_checkout':str(r),'data_directory':str(data),'sources':8,'local_only':True,'default_poll_seconds':0.2,'default_timeout_seconds':10,'default_batch_size':1000,'no_auto_scan':True,'fresh_or_reset':False},indent=2)+'\n')
 with (c/('recorder'+suffix+'.private.log')).open('wb') as log:
  proc=subprocess.Popen(command,cwd=r,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,startupinfo=startup,creationflags=subprocess.CREATE_NEW_CONSOLE)
 status['recorder_pid']=proc.pid;save()
 wait_for(lambda:len(checkpoints())==8 and state().get('state')=='recording',60,'eight-source durable baseline')
 actual_pid=int(state()['native_runtime']['pid'])
 query=f'Get-CimInstance Win32_Process -Filter "ProcessId={actual_pid}" | Select-Object ProcessId,ParentProcessId,ExecutablePath | ConvertTo-Json -Compress'
 process=json.loads(subprocess.check_output(['powershell.exe','-NoProfile','-NonInteractive','-Command',query],text=True,timeout=20))
 assert actual_pid==proc.pid or int(process['ParentProcessId'])==proc.pid,'Published recorder must be the real launched process or its verified venv child'
 status['native_child_pid']=actual_pid;status['native_process_parent_pid']=process['ParentProcessId'];save()
 assert state()['native_runtime']['build_commit']==sha and state()['request_timeout_seconds']==10
 if not options.resume_maximum_start_boundary:
  args,pid=prepare('P04','concurrent-sources');before=checkpoints();agent.change('concurrent')
  wait_for(lambda:agent.peak>=8 and all(checkpoints().get(k,0)>v for k,v in before.items()),30,'eight simultaneous sources')
  finish(args,pid,'Eight actual source workers contacted the controlled loopback agent concurrently and all eight durable checkpoints advanced. Default recorder limits and deadlines retained.',{'peak_active_requests':agent.peak,'before':before,'after':checkpoints()});agent.change('normal')
  args,pid=prepare('P04','maximum-ingress');agent.change('maximum');target=agent.nodes['s08']['last']+1
 else:
  phase('P04/maximum-ingress');args=['--commit',sha,'--host','nettking','--scenario','P04','--assertion','maximum-ingress'];pid=original['active_prepare_id'];target=maximum_target
 wait_for(lambda:checkpoints().get('s08',0)>=target,60,'maximum accepted batch commitment')
 events=[x for x in agent.events if x['case']=='maximum' and x['source']=='s08']
 assert any(x['endpoint']=='current' and x['sent_bytes']==8*1024*1024 for x in events)
 assert any(x['endpoint']=='probe' and x['sent_bytes']==16*1024*1024 for x in events)
 assert any(x['endpoint']=='sample' and x['sent_bytes']==16*1024*1024 and x['observations']==10000 for x in events)
 finish(args,pid,'The native recorder accepted actual 8MiB current, 16MiB probe and 16MiB sample responses. The sample contained10000 contiguous observations spanning the unchanged10000-sequence limit and advanced its durable checkpoint.',{'maximum_response_bytes':{'current':8*1024*1024,'probe':16*1024*1024,'sample':16*1024*1024},'observations':10000,'checkpoint_after':checkpoints()['s08']})
 args,pid=prepare('P05','slow-trickle-response');before=checkpoints()['s01'];started=time.monotonic();agent.change('slow')
 wait_for(lambda:source_error('s08','deadline'),25,'default finite request deadline')
 elapsed=time.monotonic()-started;assert checkpoints()['s01']>before
 detail={'observed_deadline_error':True,'observation_elapsed_seconds':elapsed,'unchanged_deadline_seconds':10,'other_source_advanced':True}
 normal_recovery('s08');finish(args,pid,'A controlled source continuously trickled bytes without going idle; the real native recorder reported its finite request deadline, other sources continued, and the same source resumed after normal input was restored. Default10s deadline retained.',detail)
 args,pid=prepare('P05','oversized-ingress');proofs=[]
 for mode,phrase in [('oversized-declared','maximum'),('oversized-stream','maximum'),('oversized-observations','observations')]:
  agent.change(mode);wait_for(lambda:source_error('s08',phrase),25,mode+' rejection')
  proofs.append({'input':mode,'refusal_observed':True});normal_recovery('s08')
 finish(args,pid,'The real native recorder refused an over-limit declared response, an over-limit streaming response, and10001 observations. Each source recovered on valid input; finite byte/count/span/deadline bounds were unchanged.',{'cases':proofs})
 args,pid=prepare('P05','huge-sequence-gap');before=checkpoints()['s06'];started=time.monotonic();agent.change('gap','s06')
 wait_for(lambda:checkpoints().get('s06',0)>10**12,30,'huge gap handled')
 detail={'before':before,'after':checkpoints()['s06'],'gap_order':10**12,'elapsed_seconds':time.monotonic()-started};agent.change('normal')
 finish(args,pid,'The controlled agent advanced its first available sequence to one trillion. The native recorder recorded the discontinuity and committed subsequent observations without creating a proportional range; durable state remained readable.',detail)
 args,pid=prepare('P05','event-storm');agent.change('event-storm','s07')
 def event_proof():
  items=[read(p) for p in (data/'sources/mtconnect_recorder/events').rglob('*.json')]
  return next((x for x in items if x.get('event_type')=='agent_instance_changed_during_fetch' and int(x.get('occurrence_count',0))>=6),None)
 event=wait_for(event_proof,90,'six repeated discontinuities coalesced')
 normal_recovery('s07');finish(args,pid,'Repeated actual agent-instance discontinuities were driven through the native recorder with its default retry ladder. Six or more occurrences coalesced into the bounded event summary, then valid recording resumed.',{'occurrence_count':event['occurrence_count'],'coalesced_occurrence_count':event['coalesced_occurrence_count']})
 phase('STOP');stop_native();status.update(status='COMPLETED',phase='COMPLETE',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error=str(exc),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 try:stop_native()
 except BaseException as cleanup:status['cleanup_error_type']=type(cleanup).__name__;save()
 raise
finally:
 (c/('agent'+suffix+'-observations.json')).write_text(json.dumps(agent.snapshot(),indent=2)+'\n');agent.close();save()
 print(json.dumps(status),flush=True)
