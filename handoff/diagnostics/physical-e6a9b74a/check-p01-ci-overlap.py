"""Bounded metadata-only inventory of the known CI temporary root."""
import datetime,hashlib,json,os,pathlib,time
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
samples=[json.loads(p.read_text()) for p in sorted((h/'evidence/v1-physical/observations/P01').glob('*-sample-*.json'))]
start=datetime.datetime.fromisoformat(samples[0]['recorded_at'].replace('Z','+00:00')).timestamp()
end=datetime.datetime.fromisoformat(samples[-1]['recorded_at'].replace('Z','+00:00')).timestamp()
root=pathlib.Path('C:/actions-runner/_work/_temp');assert root.is_dir()
deadline=time.monotonic()+20;count=0;groups={};limited=False
for directory,dirs,files in os.walk(root,followlinks=False):
 dirs[:]=[d for d in dirs if not pathlib.Path(directory,d).is_symlink() and not pathlib.Path(directory,d).is_junction()]
 for name in files:
  count+=1
  if count>200000 or time.monotonic()>deadline:limited=True;break
  p=pathlib.Path(directory,name)
  try:
   if p.is_symlink():continue
   st=p.stat();born=st.st_birthtime
  except (OSError,AttributeError):continue
  if start<=born<=end:
   group=p.relative_to(root).parts[0]
   alias=hashlib.sha256(group.encode()).hexdigest()[:16]
   g=groups.setdefault(alias,{'files_created':0,'logical_bytes':0,'earliest':born,'latest':born})
   g['files_created']+=1;g['logical_bytes']+=st.st_size;g['earliest']=min(g['earliest'],born);g['latest']=max(g['latest'],born)
 if limited:break
out={'candidate':samples[0]['candidate_sha'],'harness_sha':samples[0]['harness_sha'],'sample_start':samples[0]['recorded_at'],'sample_end':samples[-1]['recorded_at'],'ci_temp_root_on_sampled_device':root.stat().st_dev==pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/nettking-73c779-runtime-control/data').stat().st_dev,'inventory_metadata_only':True,'files_inspected':count,'bounded_inventory_truncated':limited,'created_files_still_present':sum(g['files_created'] for g in groups.values()),'created_logical_bytes_still_present':sum(g['logical_bytes'] for g in groups.values()),'groups':groups,'attribution_limit':'Creation times and surviving logical bytes demonstrate concurrent non-product writes; they do not reconstruct exact allocated-byte deltas or deleted files.','protected_data_accessed':False,'physical_pass':False}
outpath=pathlib.Path(__file__).parent/'P01-qualified-ci-overlap.json';outpath.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
