"""Retain existing redacted native readiness evidence; do not rerun probes."""
import json,pathlib,subprocess
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
remote='''import json,pathlib
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source')
out={name:(h/path).read_text() for name,path in {'status.json':'.acceptance/native-readiness/status.json','preflight.json':'evidence/school-control/preflight.json','gate-summary.json':'evidence/school-control/gate-summary.json','commands.txt':'evidence/linux/commands.txt','input-review.json':'.acceptance/runtime-control/input-review.json'}.items()}
print(json.dumps(out))
'''
script=h/'.acceptance/export-nitro-readiness.py';script.write_text(remote)
result=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert result.returncode==0,'Existing Nitro evidence export unavailable; do not rerun preparation'
sources={'nitro':json.loads(result.stdout),'nettking':{name:(h/path).read_text() for name,path in {'status.json':'.acceptance/native-readiness/status.json','preflight.json':'evidence/local-ai/preflight.json','gate-summary.json':'evidence/local-ai/gate-summary.json','commands.txt':'evidence/windows/commands.txt','input-review.json':'.acceptance/runtime-control/input-review.json'}.items()}}
reports=[]
for host,files in sources.items():
 status=json.loads(files['status.json']);gate=json.loads(files['gate-summary.json']);preflight=json.loads(files['preflight.json']);inputs=json.loads(files['input-review.json'])
 assert status['status']=='COMPLETED' and status['candidate']==gate['commit_sha']==preflight['checkout']['commit_sha']==inputs['candidate']==sha
 assert gate['passed'] and len(gate['checks'])==4 and all(c['passed'] and c['returncode']==0 for c in gate['checks'])
 dest=root/(host+'-readiness');dest.mkdir(exist_ok=True)
 for name,value in files.items():(dest/name).write_text(value)
 reports.append({'host':host,'candidate':sha,'native_python':status['python_version'],'preflight':'PASS','local_gates':'4/4 PASS','inputs_preserve_live_configuration':True,'runtime_activated':False,'physical_pass':False})
(root/'native-readiness.json').write_text(json.dumps(reports,indent=2)+'\n');print(json.dumps(reports))
