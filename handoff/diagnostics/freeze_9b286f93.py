"""Bind the single qualified final-main candidate; no host runtime changes."""
import datetime,hashlib,json,pathlib,subprocess
here=pathlib.Path(__file__).resolve().parent
root=pathlib.Path('C:/wsl/fcp-v1-9b286f93-merged-main-20260911')
sha='9b286f931497bf6291e215f6340443c5162826b0'
def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
qualification=json.loads((here/'merged-main-final-qualification.json').read_text())
assert qualification['source_commit']==sha and qualification['required_jobs']==37
assert all(x['result']=='PASS' for x in qualification['workflows'])
assert qualification['all_required_qualified_heads_are_ancestors']
assert git('rev-parse','HEAD')==sha and not git('status','--porcelain','--untracked-files=all')
assert git('ls-remote','origin','refs/heads/main').split()[0]==sha
host=json.loads((here/'d04-controlled-replacement.json').read_text())
peer=json.loads((here/'d04-real-peer-health.json').read_text())
assert host['status']=='D04_CURRENT_RUNTIME_PORT_OWNERSHIP_VERIFIED'
assert peer['status']=='D04_REAL_PEER_HEALTH_VERIFIED'
out=here/'authoritative-candidate-9b286f93.json'
assert not out.exists(), 'Do not recreate an existing candidate freeze'
receipt={'frozen_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'AUTHORITATIVE_SHA':sha,'status':'FROZEN_QUALIFIED_CANDIDATE_PENDING_CLEAN_REVALIDATION_AND_RUNTIME_ADMISSION',
 'clean_checkout':str(root),'qualification_receipt':'merged-main-final-qualification.json',
 'qualification_receipt_sha256':hashlib.sha256((here/'merged-main-final-qualification.json').read_bytes()).hexdigest(),
 'd04':'Current-N host conflict resolved; issue459 closed; reverify M owner after admission',
 'previous_campaign_runtime_sha':'0536f03d67eb277e11573c2188d8e820399627e3',
 'runtime_deployed_to_new_candidate':False,'physical_acceptance':False,
 'carry_forward_observations':[],'P07':'NOT_STARTED','P12':'NOT_STARTED',
 'protected_recorder_data_untouched':True,
 'next_action':'Run checked-in physical_revalidation N-to-M from clean M; retain fail-closed result and require all fresh observations; review side effects then stage exact-M owned runtime admission'}
out.write_text(json.dumps(receipt,indent=2)+'\n')
target=json.loads((here/'merged-main-target.json').read_text())
target.update(candidate_frozen=True,candidate_receipt=out.name,next_action=receipt['next_action'])
(here/'merged-main-target.json').write_text(json.dumps(target,indent=2)+'\n')
print(json.dumps(receipt))
