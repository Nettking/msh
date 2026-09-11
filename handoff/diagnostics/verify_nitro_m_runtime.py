"""After successful guarded startup only: prove actual M core source and recovery."""
import datetime,hashlib,json,pathlib,platform,subprocess
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source'
M='9b286f931497bf6291e215f6340443c5162826b0'
start=json.loads((B/'inputs/supported-start-9b286f93-guarded.json').read_text())
assert start['status']=='LAUNCHER_SUCCEEDED_REVIEW_REQUIRED' and start['exit_code']==0, 'Wait for startup completion; inspect any failure'
def run(args):return subprocess.check_output(args,cwd=R,text=True,timeout=35).strip()
assert run(['git','rev-parse','HEAD'])==M and not run(['git','status','--porcelain','--untracked-files=all'])
paths=['catalog/federation/tailscale_host_discovery.py','catalog/flask_app/federation_pairing_routes.py','catalog/flask_app/services/capability_onboarding_service.py','catalog/flask_app/services/federation_pairing_service.py']
expected={p:hashlib.sha256((R/p).read_bytes()).hexdigest() for p in paths};services=[]
for old in start['before']:
 name=old['service'];cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service='+name]);assert cid and '\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0];im=json.loads(run(['docker','image','inspect',c['Image']]))[0]
 e=dict(v.split('=',1) for v in c['Config']['Env'] if '=' in v)
 assert c['State']['Running'] and not c['State']['OOMKilled']
 shape=lambda items:sorted((m['Type'],m['Source'],m['Destination'],m['RW']) for m in items)
 assert shape(c['Mounts'])==shape(old['mounts'])
 entry={'service':name,'container_id':cid,'image_id':c['Image'],'image_commit':im['Config'].get('Labels',{}).get('no.fcp.build_commit'),'environment_commit':e.get('FCP_BUILD_COMMIT'),'started_at':c['State']['StartedAt'],'running':True,'oom_killed':False,'restarts':c['RestartCount'],'mounts_preserved':True}
 if name!='ollama':
  assert entry['image_commit']==entry['environment_commit']==M
  code="import hashlib,json;from pathlib import Path;print(json.dumps({p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest() for p in "+repr(paths)+"}))"
  actual=json.loads(run(['docker','exec',cid,'python','-B','-c',code]));assert actual==expected
  entry['repaired_source_hashes']=actual
  if name=='flask':assert e['FCP_AUTO_JOIN_PORT']=='5151';entry['responder_port']=5151
 else:
  assert c['Image']=='sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42'
  entry['version']=run(['docker','exec',cid,'ollama','--version'])
  entry['existing_model']=[x for x in run(['docker','exec',cid,'ollama','list']).splitlines() if x.startswith('llama3.2:3b')];assert entry['existing_model']
  entry['manifest_sha256']=run(['docker','exec',cid,'sha256sum','/root/.ollama/models/manifests/registry.ollama.ai/library/llama3.2/3b']).split()[0]
  assert entry['manifest_sha256']=='a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72'
 services.append(entry)
relay=next(x['container_id'] for x in services if x['service']=='relay')
control=json.loads(run(['docker','exec',relay,'cat','/var/lib/fcp-relay/control-plane-status.json']))
old=start['control_before']
for key in ('cluster_id','federation_id','voter_id'):assert control[key]==old[key]
assert control['ready'] and control['commit_index']>=old['commit_index'] and control['last_applied']==control['commit_index']
assert control['consensus_term']>=old['consensus_term']
receipt={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':M,'host':'nitro','fingerprint':hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
 'source_clean':True,'services':services,'control_identity_preserved':True,'control':{k:control[k] for k in ('role','ready','consensus_term','commit_index','last_applied')},
 'startup_log_sha256':hashlib.sha256((B/'inputs/supported-start-9b286f93-guarded-native.log').read_bytes()).hexdigest(),
 'physical_acceptance':False,'protected_recorder_data_untouched':True,
 'next_action':'Replace still-imported N host responder using checked-in exact process identity guard, then verify M responder and real peer health; Recorder M admission/membership remain pending'}
print(json.dumps(receipt,indent=2))
