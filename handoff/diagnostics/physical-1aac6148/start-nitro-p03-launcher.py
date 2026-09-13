"""Start the outstanding POSIX launcher proof after Windows activation is terminal."""
import json,pathlib,subprocess
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
assert json.loads((h/'.acceptance/runtime-control/P03-launchers-status.json').read_text())['status'] in ['COMPLETED','STOPPED']
assert not (root/'nitro-P03-launcher-dispatch.json').exists(),'Inspect existing dispatch'
content=(root/'run-p03-launchers.py').read_text()
code='''import datetime,json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
assert not (c/'P03-launchers-status.json').exists(),'Inspect existing launcher'
assert not (c/'P03-launcher-dispatch.json').exists(),'Never dispatch twice'
script=h/'.acceptance/run-p03-launchers.py';script.write_text(CONTENT)
log=(c/'P03-launcher-wrapper.private.log').open('wb')
p=subprocess.Popen(['/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python',str(script)],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'candidate':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','host':'nitro','pid':p.pid,'dispatched_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'DISPATCHED_ONCE','physical_pass':False,'protected_data_accessed':False}
(c/'P03-launcher-dispatch.json').write_text(json.dumps(out,indent=2)+'\\n');print(json.dumps(out))
'''
script=h/'.acceptance/dispatch-nitro-p03.py';script.write_text('CONTENT='+repr(content)+'\n'+code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Inspect remote status and receipt before any mutation retry'
out=json.loads(p.stdout);(root/'nitro-P03-launcher-dispatch.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
