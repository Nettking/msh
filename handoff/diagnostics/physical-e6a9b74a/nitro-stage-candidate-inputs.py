"""Resolve new source against LIVE named runtime settings without activation."""
import json,pathlib,os,subprocess,hashlib
old=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910')
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
control=root/'.acceptance/runtime-control';assert not control.exists() or not list(control.iterdir());control.mkdir(parents=True,exist_ok=True)
def run(args,env=None):return subprocess.check_output(args,cwd=root,env=env,text=True,timeout=40).strip()
assert run(['git','rev-parse','HEAD'])==sha and not run(['git','status','--porcelain'])
cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=flask']);assert cid and '\n' not in cid
live_flask=json.loads(run(['docker','inspect',cid]))[0]
files=live_flask['Config']['Labels']['com.docker.compose.project.config_files'].split(',')
assert files[0]==str(old/'source/docker-compose.yml')
assert pathlib.Path(files[-1]).name=='c03.supported-start-0536f03d.override.json'
receipt=json.loads((old/'inputs/c03-activation/supported-start-staging-receipt.json').read_text())
values=json.loads(pathlib.Path(receipt['environment_file']).read_text())
override=json.loads(pathlib.Path(files[-1]).read_text())
live={}
for svc in ['flask','relay','recorder']:
 ids=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.oneoff=False','--filter','label=com.docker.compose.service='+svc])
 if ids:
  assert '\n' not in ids
  live[svc]=json.loads(run(['docker','inspect',ids]))[0]
 override['services'][svc]['build']['context']=str(root)
 # Preserve actual product configuration where old staging values have drifted.
 if svc in live:
  runtime_env=dict(v.split('=',1) for v in live[svc]['Config']['Env'] if '=' in v)
  override['services'][svc].setdefault('environment',{}).update({k:v for k,v in runtime_env.items() if k.startswith('FCP_') and k!='FCP_BUILD_COMMIT'})
new_override=control/'compose.live-candidate.json';new_override.write_text(json.dumps(override,indent=2)+'\n')
values['COMPOSE_FILE']=':'.join([str(root/'docker-compose.yml'),*files[1:-1],str(new_override)])
values['FCP_BUILD_COMMIT']=sha
values['PATH']=str(root/'.venv/bin')+':'+os.environ['PATH']
values['VIRTUAL_ENV']=str(root/'.venv')
for key in ['TMPDIR','TEMP','TMP']:values[key]=str(root/'.acceptance/native-test-tmp')
env=os.environ.copy();env.update(values)
c=json.loads(run(['docker','compose','config','--format','json'],env))
assert c['name']=='fcp-v1-fba508-nitro'
for svc in ['flask','relay','recorder']:
 s=c['services'][svc]
 assert s['build']['context']==str(root) and s['build']['args']['FCP_BUILD_COMMIT']==sha
 assert s['image'].endswith(':'+sha)
 if svc in live:
  previous=dict(v.split('=',1) for v in live[svc]['Config']['Env'] if '=' in v)
  assert all(s['environment'].get(k)==v for k,v in previous.items() if k.startswith('FCP_') and k!='FCP_BUILD_COMMIT')
  actual={(m['Destination'],m['Type'],m.get('Name',m['Source'])) for m in live[svc]['Mounts']}
  proposed={(m['target'],m['type'],c['volumes'][m['source']]['name'] if m['type']=='volume' else m['source']) for m in s.get('volumes',[])}
  assert proposed==actual,'Live mount set differs from candidate inputs'
  assert s.get('user','')==live[svc]['Config']['User']
assert values['FCP_DATA_DIR']=='/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data'
(control/'environment.private.json').write_text(json.dumps(values,indent=2)+'\n')
(control/'compose.resolved.private.json').write_text(json.dumps(c,indent=2)+'\n')
binding={'schema':'fcp.v1.physical-runtime-binding.v1','host_id':'nitro','target_candidate_sha':sha,'acceptance_harness_sha':sha,'harness_checkout':str(root),'runtime_kind':'compose','runtime':{'project':c['name'],'working_directory':str(root),'config_files':values['COMPOSE_FILE'].split(':'),'data_root':values['FCP_DATA_DIR'],'results_root':values['FCP_RESULTS_DIR']}}
(control/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
result={'candidate':sha,'host':'nitro','live_config_labels_used':True,'existing_product_environment_preserved':True,'live_mount_sets_preserved':True,'source_clean':not run(['git','status','--porcelain']),'configuration_sha256':hashlib.sha256(json.dumps(c,sort_keys=True).encode()).hexdigest(),'runtime_changed':False,'physical_pass':False}
(control/'input-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
