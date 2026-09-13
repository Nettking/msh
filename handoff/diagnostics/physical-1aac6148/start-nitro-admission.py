"""Start Nitro only after Windows activation work is terminal."""
import json,pathlib,subprocess
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');control=h/'.acceptance/runtime-control'
admission=json.loads((control/'candidate-admission.json').read_text());assert admission['status']=='COMPLETED'
p01=json.loads((control/'P01-qualified-status.json').read_text());assert p01['status'] in ['COMPLETED','STOPPED']
assert not (root/'nitro-admission-dispatch.json').exists(),'Inspect existing Nitro activation, do not redispatch'
content=(root/'admit-candidate.py').read_text()
code='''import json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
assert not (c/'candidate-admission.json').exists(),'Inspect the existing bounded activation'
assert not (c/'candidate-admission-dispatch.json').exists(),'Do not dispatch twice'
script=h/'.acceptance/admit-candidate.py';script.write_text(CONTENT)
log=(c/'candidate-admission-wrapper.private.log').open('wb')
p=subprocess.Popen(['/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python',str(script)],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'candidate':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','host':'nitro','pid':p.pid,'status':'DISPATCHED_ONCE','windows_activation_work_terminal_before_dispatch':True,'physical_pass':False,'protected_data_accessed':False}
(c/'candidate-admission-dispatch.json').write_text(json.dumps(out,indent=2)+'\\n');print(json.dumps(out))
'''
script=h/'.acceptance/dispatch-nitro-admission.py';script.write_text('CONTENT='+repr(content)+'\n'+code)
result=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert result.returncode==0,'Inspect remote activation receipt before retrying any mutation'
out=json.loads(result.stdout);(root/'nitro-admission-dispatch.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
