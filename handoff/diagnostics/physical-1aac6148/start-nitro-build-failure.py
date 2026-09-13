"""Dispatch one native P01 build-only failure after completed activation evidence."""
import json,pathlib,subprocess
root=pathlib.Path(__file__).parent
assert (root/'nitro-P01-initial-packets.zip').exists(),'Retain completed qualification first'
assert not (root/'nitro-build-failure-dispatch.json').exists(),'Inspect existing fault before any further action'
content=(root/'run-p01-build-failure.py').read_text()
code='''import datetime,json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
assert json.loads((c/'P01-qualified-status.json').read_text())['status']=='COMPLETED'
assert not (c/'P01-build-failure-status.json').exists(),'Never repeat an existing fault'
assert not (c/'P01-build-failure-dispatch.json').exists(),'Never dispatch twice'
script=h/'.acceptance/run-p01-build-failure.py';script.write_text(CONTENT)
log=(c/'P01-build-failure-wrapper.private.log').open('wb')
p=subprocess.Popen(['/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python',str(script)],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'candidate':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','host':'nitro','pid':p.pid,'dispatched_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'DISPATCHED_ONCE','physical_pass':False,'runtime_activation_requested':False,'protected_data_accessed':False}
(c/'P01-build-failure-dispatch.json').write_text(json.dumps(out,indent=2)+'\\n');print(json.dumps(out))
'''
script=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/dispatch-nitro-build-failure.py');script.write_text('CONTENT='+repr(content)+'\n'+code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Inspect the remote status and receipt before any mutation retry'
out=json.loads(p.stdout);(root/'nitro-build-failure-dispatch.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
