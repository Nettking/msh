"""One predeclared hour-window follow-up; retain all samples and original failure."""
import datetime,json,os,pathlib,subprocess,time
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';status_file=c/'P01-hour-status.json'
assert not status_file.exists(),'Inspect the existing one-hour follow-up; never restart it blindly'
intent=json.loads((c/'P01-hour-intent.json').read_text());assert intent['candidate']==sha and intent['ceiling_bytes_per_hour']==1073741824 and intent['follow_up_samples']==1
due=datetime.datetime.fromisoformat(intent['sample_not_before'].replace('Z','+00:00'))
status={'candidate':sha,'status':'WAITING','sample_not_before':intent['sample_not_before'],'initial_failure_retained':True,'all_existing_samples_retained':True,'physical_pass':False,'runtime_activation_requested':False}
def save():status_file.write_text(json.dumps(status,indent=2)+'\n')
save()
try:
 while True:
  remaining=(due-datetime.datetime.now(datetime.timezone.utc)).total_seconds()
  if remaining<=0:break
  time.sleep(min(30,remaining))
 for root in [h,r]:
  assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==sha
  assert not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()
 env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env['FCP_BUILD_COMMIT']=sha
 base=['C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe','-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
 target=['--commit',sha,'--host','nettking','--scenario','P01']
 def call(command,label):
  result=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120,creationflags=subprocess.CREATE_NO_WINDOW)
  (c/(label+'.json')).write_text(result.stdout)
  if result.stderr:(c/(label+'.private.stderr')).write_text(result.stderr)
  assert result.stdout.strip(),label+' returned no evidence'
  return json.loads(result.stdout)
 status['status']='SAMPLING';save()
 status['sample']=call([*base,'sample',*target,'--label','declared-one-hour-follow-up'],'P01-hour-sample');save()
 packet=json.loads((h/status['sample']['evidence']).read_text())
 assert datetime.datetime.fromisoformat(packet['recorded_at'].replace('Z','+00:00'))>=due
 status['growth']=call([*base,'probe',*target,'--assertion','windows-growth-bounded'],'P01-hour-growth')
 status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 print(json.dumps(status))
except BaseException as error:
 status.update(status='STOPPED',error_type=type(error).__name__,error=str(error));save();raise
