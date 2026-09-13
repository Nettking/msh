"""Retain one completed fresh fault proof and all host P01 packets without edits."""
import argparse,base64,hashlib,json,pathlib,subprocess
parser=argparse.ArgumentParser();parser.add_argument('host',choices=['nitro','windows']);args=parser.parse_args()
root=pathlib.Path(__file__).parent;host=args.host
archive=root/(host+'-P01-after-build-failure.zip');assert not archive.exists(),'Never overwrite prior evidence'
code='''import base64,hashlib,io,json,os,pathlib,sys,zipfile
windows=os.name=='nt';h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source')
c=h/'.acceptance/runtime-control';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import scenario_status
status=json.loads((c/'P01-build-failure-status.json').read_text());assert status['status']=='COMPLETED' and status['verdict']=='pass' and status['candidate']==status['harness_sha']==sha
packets=[(p,json.loads(p.read_text())) for p in sorted((h/'evidence/v1-physical/observations/P01').glob('*.json'))]
assert all(d['candidate_sha']==d['harness_sha']==sha for _,d in packets)
lane='windows' if windows else 'posix';latest={}
for _,d in packets:
 if d.get('assertion','').startswith(lane+'-') and (d['assertion'] not in latest or d['recorded_at']>latest[d['assertion']]['recorded_at']):latest[d['assertion']]=d
assert len(latest)==5 and all(d['status']=='pass' for d in latest.values()),'Fresh P01 host assertions remain incomplete'
progress=scenario_status(h/'evidence/v1-physical','P01',expected_commit=sha)
assert not progress['failing_assertions']
buf=io.BytesIO()
with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
 for p,_ in packets:z.writestr(p.name,p.read_bytes())
raw=buf.getvalue()
print(json.dumps({'archive':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'status':status,'native_assertions':{a:d['status'] for a,d in latest.items()},'scenario_progress':progress}))
'''
script=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/export-'+host+'-p01-build-failure.py');script.write_text(code)
if host=='nitro':command=['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--nitro-runtime-python','--timeout','30']
else:command=['C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe',str(script)]
p=subprocess.run(command,capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Export incomplete; inspect original proof before retrying'
out=json.loads(p.stdout);raw=base64.b64decode(out.pop('archive'));assert hashlib.sha256(raw).hexdigest()==out['sha256']
archive.write_bytes(raw)
(root/(host+'-P01-build-failure-status.json')).write_text(json.dumps(out['status'],indent=2)+'\n')
report={'candidate':out['status']['candidate'],'host':host,'native_P01_assertions':'5/5 PASS','assertions':out['native_assertions'],'archive_sha256':out['sha256'],'original_failures_retained':True,'scenario_progress':out['scenario_progress'],'physical_pass':False,'protected_data_accessed':False}
(root/(host+'-P01-host-result.json')).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
