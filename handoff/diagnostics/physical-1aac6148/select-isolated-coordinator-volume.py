"""Resolve a refused new-installation fixture to explicit new owned volume names."""
import json,pathlib,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';record=json.loads((c/'bootstrap-status.json').read_text())
assert record['status']=='STOPPED' and record['phase']=='BASELINE_START' and record['baseline-start']['exit_code']==1
assert 'Multiple Federation coordinator volumes exist' in (c/'baseline-start.private.log').read_text()
assert not (c/'explicit-volume-review.json').exists(),'Inspect the existing selection review'
env=json.loads((c/'environment.private.json').read_text());config=json.loads((c/'compose.private.json').read_text());project=env['COMPOSE_PROJECT_NAME']
assert not subprocess.check_output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project],text=True).strip(),'No new installation container may exist before resolving this pre-start refusal'
(c/'compose-before-explicit-volume.private.json').write_bytes((c/'compose.private.json').read_bytes())
(c/'environment-before-explicit-volume.private.json').write_bytes((c/'environment.private.json').read_bytes())
for name,volume in config.get('volumes',{}).items():
 if not volume.get('external'):volume['name']=project+'_'+name
relay=config['volumes']['relay_state']['name'];assert relay.startswith(project+'_')
assert subprocess.run(['docker','volume','inspect',relay],capture_output=True).returncode!=0,'The intended new coordinator must not already contain state'
env['FCP_RELAY_VOLUME_NAME']=relay
env['FCP_OLLAMA_VOLUME_NAME']=next(config['volumes'][m['source']]['name'] for m in config['services']['ollama']['volumes'] if m['type']=='volume' and m['target']=='/root/.ollama')
env['FCP_MODEL_PROVIDER_VOLUME_NAME']=config['volumes'].get('model_provider_models',{'name':project+'_model_provider_models'})['name']
(c/'compose.private.json').write_text(json.dumps(config,indent=2)+'\n');(c/'environment.private.json').write_text(json.dumps(env,indent=2)+'\n')
out={'candidate':record['candidate'],'classification':'deterministic fixture setup omission; product resolver safely refused ambiguity','new_coordinator_volume':relay,'all_new_nonmodel_volumes_project_scoped':True,'cached_model_mounts_read_only':all(m.get('read_only') for m in config['services']['ollama']['volumes']),'prior_refusal_retained':True,'existing_authority_or_data_copied':False,'runtime_activated':False,'protected_data_accessed':False}
(c/'explicit-volume-review.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
