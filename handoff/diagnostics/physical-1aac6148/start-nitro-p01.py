"""Dispatch the prepared fresh Nitro P01 proof exactly once after verified admission."""
import json,pathlib,subprocess
root=pathlib.Path(__file__).parent
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert json.loads((c/'P01-qualified-status.json').read_text())['status']=='COMPLETED'
assert json.loads((c/'P01-hour-status.json').read_text())['status']=='WAITING'
assert not (root/'nitro-P01-dispatch.json').exists(),'Inspect existing remote P01; never redispatch'
content=(root/'run-qualified-p01.py').read_text()
code='''import datetime,json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
admission=json.loads((c/'candidate-admission.json').read_text())
assert admission['candidate']==sha and admission['status']=='COMPLETED' and admission['launcher_exit_code']==0 and admission['configured_http_status']==200
assert len(admission['core_images'])==3 and all(i['candidate']==sha for i in admission['core_images'])
assert not (c/'P01-qualified-status.json').exists(),'Inspect existing P01 execution'
assert not (c/'P01-qualified-dispatch.json').exists(),'Never dispatch twice'
assert not (h/'evidence/v1-physical').exists(),'Never overwrite earlier observations'
script=h/'.acceptance/run-qualified-p01.py';script.write_text(CONTENT)
log=(c/'P01-qualified-wrapper.private.log').open('wb')
p=subprocess.Popen(['/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python',str(script)],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'candidate':sha,'host':'nitro','pid':p.pid,'status':'DISPATCHED_ONCE','dispatched_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'windows_activation_work_terminal_before_dispatch':True,'physical_pass':False,'protected_data_accessed':False}
(c/'P01-qualified-dispatch.json').write_text(json.dumps(out,indent=2)+'\\n')
print(json.dumps({'dispatch':out,'admission':admission}))
'''
script=h/'.acceptance/dispatch-nitro-p01.py';script.write_text('CONTENT='+repr(content)+'\n'+code)
result=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert result.returncode==0,'Inspect existing remote status and receipt before any mutation retry'
out=json.loads(result.stdout)
assert out['dispatch']['candidate']==sha
(root/'nitro-P01-dispatch.json').write_text(json.dumps(out['dispatch'],indent=2)+'\n')
(root/'nitro-candidate-admission.json').write_text(json.dumps(out['admission'],indent=2)+'\n')
print(json.dumps(out['dispatch']))
