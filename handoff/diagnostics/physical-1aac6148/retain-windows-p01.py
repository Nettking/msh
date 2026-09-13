"""Retain the initial P01 result and valid assertions without changing evidence."""
import hashlib,json,pathlib,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
state=json.loads((c/'P01-qualified-status.json').read_text());assert state['status']=='COMPLETED' and state['candidate']==sha
readonly=json.loads((c/'P01-readonly-status.json').read_text());assert all(r['verdict']=='pass' for r in readonly['results'])
packets=[(p,json.loads(p.read_text())) for p in sorted((h/'evidence/v1-physical/observations/P01').glob('*.json'))]
assert all(d['candidate_sha']==d['harness_sha']==sha for _,d in packets)
samples=[d for _,d in packets if d['kind']=='sample'];assert len(samples)==4
growth=next(d for _,d in packets if d.get('assertion')=='windows-growth-bounded');detail=growth['detail']['probes'][0]['detail']
assert growth['status']=='fail' and detail['max_growth_bytes_per_hour']==1073741824
images=[a['images'] for a in state['completed_activations']]
archive=root/'windows-P01-initial-packets.zip';assert not archive.exists(),'Do not overwrite the initial observation archive'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for p,_ in packets:z.writestr(p.name,p.read_bytes())
for name in ['candidate-admission.json','P01-qualified-status.json','P01-readonly-status.json','input-review.json']:(root/('windows-'+name)).write_bytes((c/name).read_bytes())
report={'candidate':sha,'three_activations':'PASS','resource_baseline':'PASS','runtime_state':'PASS','hourly_growth':'FAIL','classification':'unresolved but non-demonstrated candidate defect','used_bytes_delta':detail['used_bytes_delta'],'bytes_per_activation':detail['used_bytes_delta']//3,'projected_growth_bytes_per_hour':detail['growth_bytes_per_hour'],'unchanged_hourly_ceiling':detail['max_growth_bytes_per_hour'],'sample_start':samples[0]['recorded_at'],'sample_end':samples[-1]['recorded_at'],'images_stable':all(i==images[0] for i in images),'docker_totals_stable':len({s['docker']['summary'] for s in samples})==1,'owned_recorder_raw_files':[s['extras']['recorder']['raw_files'] for s in samples],'owned_history_bytes':[sum(x['bytes'] for x in s['extras']['history']['databases'].values()) for s in samples],'restart_amplification':detail['restart_amplification'],'current_candidate_ci_finished_before_measurement':True,'host_allocation_attribution_complete':False,'decision':'Retain the failed short interval. Exactly one declared follow-up after the original window reaches one real hour will retain every sample and the 1 GiB/hour ceiling. Do not repeat the three activations or Windows fault work during the window. No product repair is justified by current attribution.','archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'physical_pass':False,'protected_data_accessed':False}
(root/'windows-P01-disposition.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
