"""Bind already-reviewed artifact audits to actual17ab main; does not run tests."""
from pathlib import Path

root=Path(__file__).resolve().parent
sha='17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05'
head='84c66f8185c1411d9dc8c5c33244a2f564845ce7'

def common(text):
    return text.replace(head,sha).replace('pr467-head-84c66f81','main-17ab3a05').replace(
        'C:/wsl/fcp-analysis-content-resolve-race-20260912','C:/wsl/fcp-v1-17ab3a05-merged-main-20260912').replace(
        'C:\\wsl\\fcp-analysis-content-resolve-race-20260912','C:\\wsl\\fcp-v1-17ab3a05-merged-main-20260912').replace(
        'pr467-', 'merged-main-').replace('merged-main-final-release-artifact-review.json','merged-main-release-artifact-review.json')

icse=common((root/'review_pr467_current_icse.py').read_text())
icse=icse.replace('10295001491','10295597069').replace('34681414877','34684218746').replace(
    'refs/heads/codex/analysis-content-resolve-race','refs/heads/main')
(root/'review_main_17ab_icse.py').write_text(icse)

skip=common((root/'review_pr467_current_skip_coverage.py').read_text()).replace("['prs']['467']",'')
skip=skip.replace("path = audit/'main-17ab3a05-delta-native-logs'/(str(row['job_id'])+'.log')",
    "paths = list(audit.glob('main-17ab3a05-delta-*-native-logs/'+str(row['job_id'])+'.log'))\n    assert len(paths)==1\n    path=paths[0]")
(root/'review_main_17ab_skip_coverage.py').write_text(skip)

final=common((root/'finalize_pr467_current_qualification.py').read_text()).replace("['prs']['467']",'')
final=final.replace("assert state['head'] == native['source_commit'] == SHA == git('rev-parse','HEAD')",
    "assert state['source_commit'] == native['source_commit'] == SHA == git('rev-parse','HEAD')\nassert state['main_matches']")
final=final.replace("assert w['event']=='workflow_dispatch' and w['api_head_sha']==SHA",
    "assert w['event'] in ['push','workflow_dispatch'] and w['api_head_sha']==SHA")
final=final.replace("def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()",
    "def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()\n\ndef native_path(job):\n    paths=list(A.glob('main-17ab3a05-delta-*-native-logs/'+str(job)+'.log'))\n    assert len(paths)==1\n    return paths[0]")
for variable in ["j['id']", "jobs[name]['id']", "f85_windows['id']"]:
    final=final.replace("A/'main-17ab3a05-delta-native-logs'/(str("+variable+")+'.log')",'native_path('+variable+')')
final=final.replace("read('merged-main-prequalification-review.json')", "read('pr467-prequalification-review.json')")
final=final.replace("assert prequalification['source_commit']==SHA and prequalification['base']==BASE",
    "assert prequalification['source_commit']=='"+head+"' and prequalification['base']==BASE\nassert git('rev-parse',SHA+'^{tree}')==git('rev-parse',prequalification['source_commit']+'^{tree}')")
start=final.index('api=client()\npr=api(')
end=final.index('assert len(d08_rows)==3',start)
final=final[:start]+"api=client()\nassert api('/git/ref/heads/main')['object']['sha']==SHA\n"+final[end:]
final=final.replace("status='QUALIFIED_REQUIRED_PR_HEAD_SCOPE'", "status='QUALIFIED_ACTUAL_MERGED_MAIN'")
final=final.replace("merge_boundary='Recheck ready-state review/head and merge requirements immediately before normal merge.'",
    "next_gate='Persist migration plan and equivalence matrix before CI source changes; no physical restart while candidate content is undecided.'")
(root/'finalize_main_17ab_qualification.py').write_text(final)
print('Prepared source-bound actual17ab reviewers; no tests executed.')
