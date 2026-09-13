"""Retain portable P03/P05 progress and merge completed native evidence unchanged."""
import base64,hashlib,json,pathlib,subprocess,sys,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');e=h/'evidence/v1-physical';c=h/'.acceptance/runtime-control';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (root/'P03-progress-result.json').exists(),'Inspect retained progress before exporting another snapshot'
code='''import base64,hashlib,io,json,pathlib,zipfile
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control';status=json.loads((c/'P03-launchers-status.json').read_text());assert status['status']=='COMPLETED'
buf=io.BytesIO()
with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
 for p in sorted((h/'evidence/v1-physical/observations/P03').glob('*.json')):z.writestr(p.name,p.read_bytes())
raw=buf.getvalue();print(json.dumps({'archive':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'automated':json.loads((c/'P03-automated-status.json').read_text()),'launchers':status}))
'''
script=h/'.acceptance/export-nitro-p03.py';script.write_text(code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Native export unavailable; leave existing evidence intact'
out=json.loads(p.stdout);raw=base64.b64decode(out.pop('archive'));assert hashlib.sha256(raw).hexdigest()==out['sha256']
(root/'nitro-P03-packets.zip').write_bytes(raw)
with zipfile.ZipFile(root/'nitro-P03-packets.zip') as z:
 for name in z.namelist():
  assert pathlib.PurePosixPath(name).name==name
  raw=z.read(name);packet=json.loads(raw);assert packet['candidate_sha']==packet['harness_sha']==sha and packet['host_id']=='nitro' and packet['scenario']=='P03'
  path=e/'observations/P03'/name
  if path.exists():assert path.read_bytes()==raw
  else:
   with path.open('xb') as stream:stream.write(raw)
(root/'nitro-P03-status.json').write_text(json.dumps(out,indent=2)+'\n')
for name in ['P03-automated-status.json','P03-launchers-status.json','P03-model-isolation-status.json']:(root/('windows-'+name)).write_bytes((c/name).read_bytes())
(root/'windows-P03-tailnet-input-review.json').write_bytes((h/'.acceptance/p03-tailnet/input-review.json').read_bytes())
sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import scenario_status
progress={scenario:scenario_status(e,scenario,expected_commit=sha) for scenario in ['P03','P05']}
assert progress['P03']['missing_assertions']==['start-tailscale-cmd'] and not progress['P03']['failing_assertions']
archive=root/'P03-P05-progress-evidence.zip'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for p in [e/'campaign.json',*sorted((e/'hosts').glob('*.json')),*sorted((e/'observations/P03').glob('*.json')),*sorted((e/'observations/P05').glob('*.json'))]:z.writestr(p.relative_to(e).as_posix(),p.read_bytes())
report={'candidate':sha,'P03_assertions':'7/8 PASS','P03_remaining':'start-tailscale-cmd','P05_ollama_absence':'PASS','model_restored':True,'progress':progress,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'tailnet_binding_staged_not_activated':True,'tailnet_product_identity':'identity-missing in the owned workbench fixture','P02':'awaiting isolated mounted test storage or administrator-enabled test host','physical_pass':False,'protected_data_accessed':False}
(root/'P03-progress-result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
