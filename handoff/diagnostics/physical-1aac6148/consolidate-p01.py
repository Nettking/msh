"""Merge portable native packets unchanged and apply the checked-in P01 contract."""
import base64,hashlib,json,pathlib,subprocess,sys,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');e=h/'evidence/v1-physical';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (root/'P01-combined-result.json').exists(),'Inspect existing combined validation; never repeat green qualification'
for host in ['windows','nitro']:
 result=json.loads((root/(host+'-P01-host-result.json')).read_text());assert result['candidate']==sha and result['native_P01_assertions']=='5/5 PASS'
 archive=root/(host+'-P01-after-build-failure.zip');assert hashlib.sha256(archive.read_bytes()).hexdigest()==result['archive_sha256']
code="import base64,hashlib,json,pathlib\np=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source/evidence/v1-physical/hosts/nitro.json');raw=p.read_bytes();print(json.dumps({'bytes':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest()}))\n"
script=h/'.acceptance/export-nitro-host.py';script.write_text(code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Native host proof unavailable; leave packets untouched'
out=json.loads(p.stdout);raw=base64.b64decode(out['bytes']);assert hashlib.sha256(raw).hexdigest()==out['sha256']
native=json.loads(raw);assert native['candidate_sha']==sha and native['host_id']=='nitro' and native['os_category']=='posix'
def preserve(path,raw):
 if path.exists():assert path.read_bytes()==raw,'Portable evidence collision; never overwrite'
 else:
  with path.open('xb') as stream:stream.write(raw)
preserve(e/'hosts/nitro.json',raw)
imported=[]
with zipfile.ZipFile(root/'nitro-P01-after-build-failure.zip') as z:
 for name in z.namelist():
  assert pathlib.PurePosixPath(name).name==name and name.endswith('.json')
  raw=z.read(name);packet=json.loads(raw);assert packet['candidate_sha']==packet['harness_sha']==sha and packet['host_id']=='nitro'
  assert packet['host_fingerprint']==native['host_fingerprint'] and packet['os_category']=='posix' and packet['scenario']=='P01'
  preserve(e/'observations/P01'/name,raw);imported.append({'name':name,'sha256':hashlib.sha256(raw).hexdigest()})
sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import load_campaign,read_packets,scenario_status
campaign=load_campaign(h,e,sha);assert campaign['harness_sha']==sha and not campaign['accepted']
packets=read_packets(e,'P01',expected_commit=sha);assert all(x['harness_sha']==sha for x in packets)
status=scenario_status(e,'P01',expected_commit=sha);assert status['passed'] and not status['missing_assertions'] and not status['failing_assertions']
assert any(x.get('assertion')=='windows-growth-bounded' and x.get('status')=='fail' for x in packets),'Original failure must remain present'
archive=root/'P01-combined-evidence.zip';assert not archive.exists()
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for path in [e/'campaign.json',*sorted((e/'hosts').glob('*.json')),*sorted((e/'observations/P01').glob('*.json'))]:z.writestr(path.relative_to(e).as_posix(),path.read_bytes())
report={'candidate':sha,'harness_sha':sha,'P01':'PASS','native_assertions_passed':10,'native_hosts':['nettking','nitro'],'contract_result':status,'imported_native_packets':imported,'packet_bytes_preserved':True,'original_windows_failure_retained':True,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'physical_campaign_pass':False,'P07':'NOT_STARTED','P12':'NOT_STARTED','protected_data_accessed':False}
(root/'P01-combined-result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='imported_native_packets'}))
