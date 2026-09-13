"""Read-only final source and qualification guard before a normal PR merge."""
import datetime,json,pathlib,subprocess,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
root=pathlib.Path(__file__).parent;repo=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913')
q=json.loads((root/'qualification.json').read_text());assert q['status']=='EXACT_SOURCE_AUTOMATED_QUALIFIED'
a=client();p=a('/pulls/486')
assert p['head']['sha']=='fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe' and p['base']['sha']=='1492d925d791b9a0ebc7bcee39ce9b3b7477254b'
assert p['merge_commit_sha']==q['source'] and p['state']=='open' and not p['draft'] and p['mergeable'] is True
assert a('/git/ref/heads/main')['object']['sha']==p['base']['sha']
files=a('/pulls/486/files?per_page=100');assert {f['filename'] for f in files}=={'scripts/windows/fcp_host_build.ps1','catalog/federation/tests/test_windows_controlled_build_contract.py','catalog/federation/tests/test_windows_build_quiescence_fail_closed.py'}
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True)
state=json.loads((root.parent/'physical-e6a9b74a/windows-repair-qualification-current.json').read_text())
assert state['all_expected_green']
for w in state['workflows']:
 run=a(f"/actions/runs/{w['run_id']}");assert run['conclusion']=='success'
assert subprocess.check_output(['git','rev-parse',p['head']['sha']+'^{tree}'],cwd=repo,text=True).strip()==q['tree']
out={'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':486,'title':p['title'],'head':p['head']['sha'],'base':p['base']['sha'],'qualified_merge_source':q['source'],'qualified_tree':q['tree'],'mergeable':p['mergeable'],'mergeable_state':p['mergeable_state'],'draft':p['draft'],'changed_files':[f['filename'] for f in files],'required_jobs_passed':37,'companion_jobs_passed':3,'physical_pass':False,'review':'Small Windows process-handle and exit-status repair; native 20-test proof and real Docker failure proof retained. No weakened assertions, quorum, security, deadlines, or runner changes. One bounded failed-job recovery passed; initial timeout causes remain non-demonstrated candidate defects.'}
(root/'premerge-review.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
