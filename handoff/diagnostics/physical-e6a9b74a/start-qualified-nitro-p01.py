import json,pathlib,subprocess
here=pathlib.Path(__file__).parent
win=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/runtime-control/P01-qualified-status.json')
w=json.loads(win.read_text());assert w['status']=='COMPLETED' and w['three_activations']['verdict']=='pass'
content=(here/'run-qualified-p01.py').read_text()
remote='''import json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913')
c=h/'.acceptance/runtime-control'
assert not (c/'P01-qualified-status.json').exists(),'Inspect existing bounded proof'
assert not (h/'.acceptance/run-qualified-p01.py').exists(),'Do not dispatch twice'
script=h/'.acceptance/run-qualified-p01.py';script.write_text(CONTENT)
log=(c/'qualified-wrapper.private.log').open('wb')
p=subprocess.Popen(['/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python',str(script)],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'host':'nitro','pid':p.pid,'candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','harness_sha':'501b528e9476878e6a6fe5cde8240b2d54b1d263','status':'DISPATCHED_ONCE','windows_activations_completed_before_start':True,'physical_pass':False}
(c/'qualified-dispatch.json').write_text(json.dumps(out,indent=2)+'\\n');print(json.dumps(out))
'''
temp=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/dispatch-nitro-p01.py')
temp.write_text('CONTENT='+repr(content)+'\n'+remote)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(temp),'--timeout','55'],capture_output=True,text=True,timeout=60)
assert p.returncode==0,p.stderr
out=json.loads(p.stdout);(here/'nitro-qualified-p01-dispatched.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
