"""Fill only the six absent exact-source qualification gates for PR486."""
import datetime,json,pathlib,sys
from urllib.error import HTTPError
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
here=pathlib.Path(__file__).parent
head='fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe';source='94567b0d9ac916eec9e4f094d2f6754562b966aa';main='1492d925d791b9a0ebc7bcee39ce9b3b7477254b';ref='codex/federation-v1-qualify-94567b0d'
gates=['cf7-acceptance-harness.yml','cf7c-physical-test-readiness.yml','cf8-role-retirement.yml','ci-test-sharding.yml','cfi2-onboarding-composition.yml','release-image-metadata.yml']
receipt=here/'windows-repair-gap-dispatch.json';assert not receipt.exists(),'Inspect the durable dispatch receipt and live runs'
a=client();p=a('/pulls/486');assert p['head']['sha']==head and p['base']['sha']==main and p['merge_commit_sha']==source and p['state']=='open'
assert a('/git/ref/heads/main')['object']['sha']==main
state={'source':source,'review_head':head,'ref':ref,'pr':486,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actions':[],'no_existing_job_rerun':True}
def save():receipt.write_text(json.dumps(state,indent=2)+'\n')
save()
try:
 assert a('/git/ref/heads/'+ref)['object']['sha']==source
except HTTPError as error:
 if error.code!=404:raise
 state['branch_action']='creation pending';save();a('/git/refs',{'ref':'refs/heads/'+ref,'sha':source})
state['branch_action']='exact source pinned';save()
for gate in gates:
 existing=[]
 for sha in [source,head]:
  existing.extend(a('/actions/workflows/'+gate+'/runs?head_sha='+sha+'&per_page=100')['workflow_runs'])
 action={'workflow':gate,'existing_runs':[r['id'] for r in existing]};state['actions'].append(action);save()
 if existing:action['status']='reuse existing run; no dispatch';save();continue
 assert a('/git/ref/heads/'+ref)['object']['sha']==source
 action['status']='submission pending; inspect live state on uncertainty';save()
 action['response']=a('/actions/workflows/'+gate+'/dispatches',{'ref':ref});action['status']='accepted';save();print(json.dumps(action),flush=True)
print(json.dumps({'source':source,'review_head':head,'ref':ref,'receipt':receipt.name}),flush=True)
