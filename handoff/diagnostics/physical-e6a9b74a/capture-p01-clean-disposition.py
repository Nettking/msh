"""Retain the bounded P01 finding without changing any observation packet."""
import datetime,hashlib,json,pathlib,sys
sys.path.insert(0,'C:/wsl/fcp-v1-e6a9b74a-main-20260913')
from catalog.federation.host_resources import measure_filesystem
h=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913')
c=h/'.acceptance/runtime-control-clean'
v=json.loads((c/'environment.private.json').read_text())
same=measure_filesystem(v['FCP_DATA_DIR']).resource_id==measure_filesystem(v['FCP_RESULTS_DIR']).resource_id
assert same
paths=sorted((h/'evidence/v1-physical-clean-runtime/observations/P01').glob('*-sample-*.json'))
packets=[json.loads(p.read_text()) for p in paths]
assert len(packets)==4
assert all(p['resources']['data']==p['resources']['results'] for p in packets)
delta=packets[-1]['resources']['data']['used_bytes']-packets[0]['resources']['data']['used_bytes']
elapsed=(datetime.datetime.fromisoformat(packets[-1]['recorded_at'].replace('Z','+00:00'))-datetime.datetime.fromisoformat(packets[0]['recorded_at'].replace('Z','+00:00'))).total_seconds()
status=json.loads((c/'P01-recovery-status.json').read_text())
assert status['status']=='COMPLETED'
assert all(a['images']==status['completed_activations'][0]['images'] for a in status['completed_activations'])
out={'candidate':status['candidate'],'classification':'deterministic harness defect','recovery_completed':True,'completed_activations':3,'three_activations_verdict':status['three_activations']['verdict'],'growth_verdict':status['growth']['verdict'],'same_native_backing_resource_verified':same,'actual_unique_volume_delta_bytes':delta,'incorrect_summed_delta_bytes':2*delta,'elapsed_seconds':elapsed,'unique_volume_growth_bytes_per_hour':int(delta*3600/elapsed),'unchanged_ceiling_bytes_per_hour':1073741824,'images_stable_across_all_activations':True,'docker_totals_stable':len({p['docker']['summary'] for p in packets})==1,'physical_pass':False,'old_packets_rewritten':False,'disposition':'Repair identity-based sampling and growth aggregation in independently pinned acceptance harness; preserve product freeze and old failures. Require fresh measurements, not a retroactive PASS.','packets':[{'name':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'recorded_at':d['recorded_at'],'resources':d['resources']} for p,d in zip(paths,packets)]}
path=pathlib.Path(__file__).parent/'P01-clean-recovery-disposition.json'
path.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='packets'}))
