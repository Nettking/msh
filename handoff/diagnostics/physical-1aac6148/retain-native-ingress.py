"""Retain the bounded real native input run and its corrected fixture observations."""
import datetime,gzip,hashlib,json,pathlib,sys,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/native-faults';data=c/'data';e=h/'evidence/v1-physical';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (root/'native-ingress-result.json').exists(),'Preserve prior snapshot'
status=json.loads((c/'ingress-maximum-continuation-status.json').read_text());assert status['status']=='COMPLETED' and status['operator_stop_exit_code']==0 and len(status['results'])==6
for name in ['ingress-status.json','ingress-recovery-status.json','ingress-maximum-continuation-status.json','launch-review.json','launch-review-recovery.json','launch-review-maximum-continuation.json']:
 (root/('native-'+name)).write_bytes((c/name).read_bytes())
maximum=[]
for kind in ['raw','probe']:
 for path in (data/'sources/mtconnect_recorder'/kind/'s08').rglob('*.gz'):
  with gzip.open(path,'rb') as stream:raw=stream.read(16*1024*1024+1)
  if len(raw)==16*1024*1024:maximum.append({'kind':kind,'archive_bytes':path.stat().st_size,'uncompressed_bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
assert {x['kind'] for x in maximum}=={'raw','probe'}
events=[]
for path in (data/'sources/mtconnect_recorder/events').rglob('*.json'):
 value=json.loads(path.read_text())
 if value.get('event_type')=='agent_instance_changed_during_fetch':events.append({'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'event':value})
assert any(x['event']['occurrence_count']>=6 and x['event']['coalesced_occurrence_count']>=5 for x in events)
logs=[{'name':p.name,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(c.glob('recorder*.private.log'))]
refusals=[]
for line in (c/'recorder-maximum-continuation.private.log').read_text(errors='replace').splitlines():
 if 'recorder error:' in line:
  message=line.split('recorder error:',1)[1].strip()
  if message not in refusals:refusals.append(message)
sys.path.insert(0,str(h));from scripts.acceptance.v1_physical_campaign import scenario_status
progress={s:scenario_status(e,s,expected_commit=sha) for s in ['P04','P05']}
archive=root/'P04-P05-native-ingress-evidence.zip'
with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
 for path in [e/'campaign.json',*sorted((e/'hosts').glob('*.json')),*sorted((e/'observations/P04').glob('*.json')),*sorted((e/'observations/P05').glob('*.json'))]:z.writestr(path.relative_to(e).as_posix(),path.read_bytes())
report={'candidate':sha,'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'P04_assertions':'2/6 PASS','P05_assertions':'6/16 PASS','new_assertions':[x['assertion'] for x in status['results']],'progress':progress,'maximum_archives':maximum,'discontinuity_events':events,'actual_recorder_refusals':refusals,'private_log_digests':logs,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'setup_disposition':'Native PID observer corrected for Windows venv child; transient checkpoint sharing reads remain unavailable within the same observation bound. Maximum-input fixture armed before startup after a live switch missed the current/probe boundaries. Original attempts retained; no repeated green concurrency test.','product_changed':False,'default_deadlines_and_limits_preserved':True,'native_recorder_stopped_cleanly':True,'loopback_agent_stopped':True,'active_executor':None,'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False,'P06':'NOT STARTED; real supervised native enrollment prerequisite remains','P07':'NOT STARTED','P12':'NOT STARTED'}
(root/'native-ingress-result.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['P04_assertions','P05_assertions','new_assertions','archive_sha256','native_recorder_stopped_cleanly','physical_pass']}))
