"""Freeze only the fully qualified actual main, then run checked-in revalidation."""
import datetime,hashlib,json,pathlib,subprocess,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
root=pathlib.Path(__file__).parent;repo=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';baseline='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
qfile=root.parent/'main-1aac6148-qualification/qualification.json';q=json.loads(qfile.read_text())
assert q['source']==q['live_main']==sha and q['status']=='ACTUAL_MAIN_AUTOMATED_QUALIFIED'
assert q['required_jobs_passed']==37 and q['companion_jobs_passed']==3 and q['native_checkout_proofs']==38
a=client();assert a('/git/ref/heads/main')['object']['sha']==sha
state=json.loads((qfile.parent/'qualification-current.json').read_text());assert state['all_expected_green']
for w in state['workflows']:assert a(f"/actions/runs/{w['run_id']}")['conclusion']=='success'
def git(*args):return subprocess.check_output(['git',*args],cwd=repo,text=True).strip()
assert git('rev-parse','HEAD')==sha and not git('status','--porcelain') and git('rev-parse','HEAD^{tree}')==q['tree']
receipt=root.parent/'AUTHORITATIVE_CANDIDATE-1aac6148.json'
assert not receipt.exists(),'Inspect existing freeze instead of rewriting it'
frozen={'frozen_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':sha,'tree':q['tree'],'state':'AUTHORITATIVE_FROZEN_CANDIDATE','actual_main_verified':True,'required_jobs_passed':37,'companion_jobs_passed':3,'native_checkout_proofs':38,'qualification_sha256':hashlib.sha256(qfile.read_bytes()).hexdigest(),'qualification':'main-1aac6148-qualification/qualification.json','supersedes_blocked_candidate':baseline,'reason':'Normal merge of qualified shared-volume tooling and Windows build exit-status repair; actual resulting main qualified once.','physical_acceptance':'NOT_PASSED','P07':'NOT_STARTED','P12':'NOT_STARTED','protected_recorder_data':'UNTOUCHED','tag_or_publication_created':False}
receipt.write_text(json.dumps(frozen,indent=2)+'\n');(repo/'.acceptance/authoritative-freeze.json').write_text(json.dumps(frozen,indent=2)+'\n')
python='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
result=subprocess.run([python,'-m','catalog.federation.tests.cf7_acceptance.physical_revalidation','--baseline',baseline,'--candidate',sha,'--repo-root',str(repo)],cwd=repo,capture_output=True,text=True,timeout=45)
assert result.returncode==0,'Inspect checked-in revalidation refusal'
plan=json.loads(result.stdout);assert plan['safe'] and not plan['unknown_paths'] and not plan['carry_forward_scenarios'] and len(plan['impacted_scenarios'])==12
(root/'revalidation-from-e6a9b74a.json').write_text(json.dumps(plan,indent=2)+'\n')
print(json.dumps({'candidate':sha,'frozen':True,'revalidation':'PASS','fresh_scenarios':12,'carry_forward':0,'physical_pass':False}))
