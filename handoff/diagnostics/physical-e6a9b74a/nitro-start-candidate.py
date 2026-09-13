"""Start the reviewed owned Nitro candidate through its supported normal launcher."""
import json,pathlib,subprocess,os
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
control=root/'.acceptance/runtime-control'
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
assert not (control/'activation-status.json').exists(),'Inspect active or finished activation first'
assert json.loads((root/'evidence/school-control/gate-summary.json').read_text())['passed']
assert json.loads((root/'evidence/school-control/preflight.json').read_text())['checkout']['commit_sha']==sha
assert json.loads((control/'input-review.json').read_text())['candidate']==sha
subprocess.run(['git','-C',str(root),'remote','set-url','origin','https://github.com/Nettking/msh.git'],check=True)
code='''import datetime,json,pathlib,subprocess,os,signal,time
root=pathlib.Path(ROOT);control=root/'.acceptance/runtime-control'
sha=SHA
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()
values=json.loads((control/'environment.private.json').read_text())
data=pathlib.Path(values['FCP_DATA_DIR'])
assert str(data)=='/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data'
assert not (data/'federation/update-agent/request.json').exists(),'Pending update request requires review'
old_script='/home/martin/fcp-v1-73c779-nitro-20260910/source/scripts/posix/fcp_update_agent.py'
agents=[]
for folder in pathlib.Path('/proc').iterdir():
 if not folder.name.isdigit():continue
 try:argv=(folder/'cmdline').read_bytes().split(b'\\x00')
 except (OSError,PermissionError):continue
 if old_script.encode() in argv:agents.append(int(folder.name))
assert len(agents)<=1,'Multiple old owned updater processes require review'
for pid in agents:
 if old_script.encode() in pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\\x00'):os.kill(pid,signal.SIGTERM)
deadline=time.monotonic()+5
while agents and time.monotonic()<deadline:
 agents=[pid for pid in agents if pathlib.Path(f'/proc/{pid}/cmdline').exists() and old_script.encode() in pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\\x00')]
 if agents:time.sleep(.1)
assert not agents,'Old updater still running'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(values)
env.pop('FCP_BUILD_COMMIT',None);env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
result={'candidate':sha,'host':'nitro','status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'physical_pass':False,'protected_recorder_data_accessed':False}
(control/'activation-status.json').write_text(json.dumps(result,indent=2)+'\\n')
with (control/'supported-start.private.log').open('wb') as log:
 p=subprocess.run(['bash','start.sh'],cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
result.update(status='COMPLETED',exit_code=p.returncode,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
(control/'activation-status.json').write_text(json.dumps(result,indent=2)+'\\n')
'''
script=control/'activate-nitro.py';script.write_text('ROOT='+repr(str(root))+'\nSHA='+repr(sha)+'\n'+code)
log=(control/'activation-wrapper.private.log').open('wb')
process=subprocess.Popen([str(root/'.venv/bin/python'),str(script)],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({'candidate':sha,'activation_wrapper_pid':process.pid,'supported_command':'bash start.sh (normal mode)','runtime_scope':'Existing owned Nitro acceptance Compose project','physical_pass':False}))
