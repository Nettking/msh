"""Retain the single declared one-hour follow-up, including every earlier packet."""
import datetime,hashlib,json,pathlib,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';status=json.loads((c/'P01-hour-status.json').read_text());intent=json.loads((c/'P01-hour-intent.json').read_text())
assert status['candidate']==sha and status['status']=='COMPLETED' and status['growth']['verdict']=='pass'
packets=[(p,json.loads(p.read_text())) for p in sorted((h/'evidence/v1-physical/observations/P01').glob('*.json'))]
assert all(d['candidate_sha']==d['harness_sha']==sha for _,d in packets)
samples=[d for _,d in packets if d['kind']=='sample'];assert len(samples)==5
assert datetime.datetime.fromisoformat(samples[-1]['recorded_at'].replace('Z','+00:00'))>=datetime.datetime.fromisoformat(intent['sample_not_before'].replace('Z','+00:00'))
with zipfile.ZipFile(root/'windows-P01-initial-packets.zip') as original:
 for name in original.namelist():assert original.read(name)==(h/'evidence/v1-physical/observations/P01'/name).read_bytes(),'Original packet changed'
growth=[d for _,d in packets if d.get('assertion')=='windows-growth-bounded'];assert len(growth)==2 and [d['status'] for d in growth]==['fail','pass']
detail=growth[-1]['detail']['probes'][0]['detail'];assert detail['sample_count']==5 and detail['max_growth_bytes_per_hour']==1073741824
archive=root/'windows-P01-one-hour-packets.zip';assert not archive.exists(),'Never overwrite the bounded follow-up'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for p,_ in packets:z.writestr(p.name,p.read_bytes())
(root/'windows-P01-hour-status.json').write_bytes((c/'P01-hour-status.json').read_bytes())
report={'candidate':sha,'hourly_growth':'PASS','growth_detail':detail,'original_packet_bytes_unchanged':True,'initial_short_window_failure_retained':True,'follow_up_samples_added':1,'ceiling_changed':False,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'classification':'unresolved but non-demonstrated candidate defect; declared sustained-growth check passed','decision':'The single real-hour follow-up passes with every earlier sample retained and unchanged ceiling. No sustained candidate growth defect is demonstrated; close this bounded observation and continue physical acceptance without another diagnostic run.','physical_pass':False,'protected_data_accessed':False}
(root/'windows-P01-hour-disposition.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
