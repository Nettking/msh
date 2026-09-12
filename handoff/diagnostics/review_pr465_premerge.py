"""Fresh live merge inputs; read-only, never bypass GitHub branch policy."""
import datetime
import json
from pathlib import Path
import sys

root=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api=client()
sha='4749ab6689315a7ad953c11ce4b2ec942433d71a'
pr=api('/pulls/465')
assert pr['head']['sha']==sha and not pr['draft'] and not pr['merged']
reviews=api('/pulls/465/reviews?per_page=100')
comments=api('/pulls/465/comments?per_page=100')
checks=api('/commits/'+sha+'/check-runs?per_page=100')['check_runs']
statuses=api('/commits/'+sha+'/status')
result=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    head=sha,base=pr['base']['sha'],mergeable=pr['mergeable'],mergeable_state=pr['mergeable_state'],
    draft=pr['draft'],requested_reviewers=[u['login'] for u in pr['requested_reviewers']],
    reviews=[{k:r.get(k) for k in ['id','state','body','commit_id','submitted_at']} for r in reviews],
    comments=[{k:c.get(k) for k in ['id','body','path','line','commit_id']} for c in comments],
    checks=[{k:c.get(k) for k in ['id','name','status','conclusion']} for c in checks],
    status= {k:statuses.get(k) for k in ['state','total_count','statuses']})
(root/'pr465-premerge-review-input.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(dict(head=sha,mergeable=result['mergeable'],mergeable_state=result['mergeable_state'],
    reviews=result['reviews'],comments=result['comments'],requested_reviewers=result['requested_reviewers'],
    nonpassing_checks=[r for r in result['checks'] if r['conclusion'] not in ['success','skipped','neutral']],
    status_state=result['status']['state'],status_count=result['status']['total_count'])))
