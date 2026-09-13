import hashlib,json,pathlib
here=pathlib.Path(__file__).parent
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
c=h/'.acceptance/runtime-control';root=h/'evidence/v1-physical/observations/P01'
status=json.loads((c/'P01-qualified-status.json').read_text());assert status['status']=='COMPLETED'
packets=[(p,json.loads(p.read_text())) for p in sorted(root.glob('*.json'))]
samples=[p for _,p in packets if p['kind']=='sample']
assert all(p['harness_sha']==status['harness_sha'] and p['candidate_sha']==status['candidate'] for _,p in packets)
growth=next(p for _,p in packets if p.get('assertion')=='windows-growth-bounded')
detail=growth['detail']['probes'][0]['detail']
overlap=json.loads((here/'P01-qualified-ci-overlap.json').read_text())
for p in pathlib.Path('C:/actions-runner/_work/_temp').iterdir():
 alias=hashlib.sha256(p.name.encode()).hexdigest()[:16]
 if alias in overlap['groups']:
  overlap['groups'][alias]['ci_python_environment']=p.name.startswith('fcp-python-')
(here/'P01-qualified-ci-overlap.json').write_text(json.dumps(overlap,indent=2)+'\n')
images=[a['images'] for a in status['completed_activations']]
out={'candidate':status['candidate'],'harness_sha':status['harness_sha'],'three_activations':'PASS','hourly_growth':'FAIL','classification':'infrastructure/host issue: shared-volume measurement confounded by concurrent CI writes','candidate_defect_demonstrated':False,'used_bytes_delta':detail['used_bytes_delta'],'growth_bytes_per_hour':detail['growth_bytes_per_hour'],'unchanged_ceiling_bytes_per_hour':detail['max_growth_bytes_per_hour'],'images_stable':all(i==images[0] for i in images),'docker_totals_stable':len({s['docker']['summary'] for s in samples})==1,'restart_amplification':detail['restart_amplification'],'surviving_ci_files_created_during_window_bytes':overlap['created_logical_bytes_still_present'],'allocation_attribution_complete':False,'release_decision':'Preserve the failed assertion. No new product or harness repair is justified. Finish automatically scheduled CI, then collect one quiescent growth window at the unchanged ceiling; do not repeat the three activations.','physical_pass':False,'packets':[{'name':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'kind':d['kind'],'recorded_at':d['recorded_at']} for p,d in packets]}
(here/'P01-qualified-windows-disposition.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='packets'}))
