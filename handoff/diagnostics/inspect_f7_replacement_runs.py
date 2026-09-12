"""Bounded snapshot of the two migration proofs; never dispatches or reruns CI."""
import datetime, json, pathlib, subprocess, sys
sys.path.insert(0, r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
ROOT=pathlib.Path(__file__).resolve().parent
PRIVATE=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
WORKTREE=r'C:\wsl\fcp-ci-f7-consolidation-20260912'
EXPECTED={468:(34689990990,'f3abe5452db2f21593a688bc62bc5f4b22d5c40e','b0fbb8a1a4e216b8696b8015a35594c1007319de'),469:(34690234286,'660bf23269893305c7e8ffcc4c910a7efaed567d','aa7b41d328d8d6cab756951ec8739a370099756b')}
def git(*args):return subprocess.check_output(['git','-C',WORKTREE,*args],text=True).strip()
def main():
    api=client();rows=[]
    cache_path=ROOT/'ci-f7-replacement-runs-latest.json'
    cached=json.loads(cache_path.read_text())['runs'] if cache_path.exists() else []
    for pr,(rid,head,merge) in EXPECTED.items():
        proof_path=ROOT/('ci-f7-pr'+str(pr)+'-native-proof.json')
        if proof_path.exists():
            proof=json.loads(proof_path.read_text());assert proof['api_head_sha']==head and proof['run_id']==rid
            previous=next(r for r in cached if r['pr']==pr);assert previous['conclusion']=='success'
            rows.append(previous);print(json.dumps({'pr':pr,'run_id':rid,'status':'REUSED_EXISTING_REVIEWED_PROOF_NO_POLL'}));continue
        r=api('/actions/runs/'+str(rid));assert r['head_sha']==head and r['event']=='pull_request'
        jobs=api('/actions/runs/'+str(rid)+'/jobs?per_page=100')['jobs']
        assert len(jobs)==2
        assert git('rev-parse',head+'^{tree}')==git('rev-parse',merge+'^{tree}')
        wf=git('rev-parse',head+':.github/workflows/phase-f7-closeout.yml')
        assert wf=='143db0b43c870bc5791c3629e34fd61a72293c47'
        row={'pr':pr,'run_id':rid,'api_head_sha':head,'actual_checkout_expected':merge,'checkout_tree_identical_to_head':True,'workflow_blob':wf,'event':r['event'],'status':r['status'],'conclusion':r['conclusion'],'attempt':r['run_attempt'],'created_at':r['created_at'],'updated_at':r['updated_at'],'jobs':jobs}
        rows.append(row)
        if r['status']=='completed':
            label='ci-f7-pr'+str(pr)+'-run'+str(rid)
            source={'source_commit':merge,'review_head':head,'workflow_blob':wf,'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'workflows':[{'workflow':'phase-f7-closeout.yml','run_id':rid,'jobs':jobs}]}
            (PRIVATE/(label+'-qualification-latest.json')).write_text(json.dumps(source,indent=2)+'\n',encoding='utf-8')
            row['retention_label']=label
        print(json.dumps({k:v for k,v in row.items() if k!='jobs'}))
        print(json.dumps({'pr':pr,'jobs':[{k:j.get(k) for k in ['id','status','conclusion','runner_name','runner_id','started_at','completed_at']} for j in jobs]}))
    out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'purpose':'CI_REPLACEMENT_PROOF_NOT_PHYSICAL_ACCEPTANCE','runs':rows,'green_native_review_complete':False}
    (ROOT/'ci-f7-replacement-runs-latest.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
if __name__=='__main__':main()
