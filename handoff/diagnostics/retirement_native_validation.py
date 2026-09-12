"""Plan/dispatch one native final-head check; inspect only current new runs."""
import datetime
import json
import pathlib
import sys

dest=pathlib.Path('handoff/diagnostics');root=dest.parent
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();mode=sys.argv[1]
head='440123f6bc6dc358eef3d233236bc14f91af60e0';base='b7194820d8f1940ae60b8c9639e09b7f61e65c55';branch='codex/ci-phase-workflow-retirement-20260912'
pr=api('/pulls/473');assert pr['head']['sha']==head and pr['base']['sha']==base
now=datetime.datetime.now(datetime.timezone.utc).isoformat()
planpath=dest/'pr473-native-validation-plan.json'
if mode=='prepare':
    assert not planpath.exists()
    existing=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
    f7=[r for r in existing if r['path']=='.github/workflows/phase-f7-closeout.yml']
    assert not f7,'Use existing F7 execution instead of dispatching duplicate'
    plan={'recorded_at':now,'pr':473,'head_sha':head,'base_sha':base,'branch':branch,'draft':pr['draft'],'status':'PLANNED_NOT_DISPATCHED','workflow':'phase-f7-closeout.yml','reason':'Exact retirement source validation required by manifest/accepted plan; deleted sibling paths do not trigger retained F7 automatically. Existing2/2 replacement proofs and real JS-only event remain preserved. One new scoped native matrix, no repeated two-proof campaign.','source_changes':'Eight deletions/four docs only; all1406 remaining entries identical','existing_runs':[{'id':r['id'],'path':r['path'],'status':r['status']} for r in existing],'manual_dispatches_planned':1,'full37_dispatches_planned':0,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
    planpath.write_text(json.dumps(plan,indent=2)+'\n',encoding='utf-8')
    p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');a=s.index('**Current actionable checkpoint');b=s.index('Current user direction, September 11 2026:')
    s=s[:a]+'''**Current actionable checkpoint (2026-09-12T15:11Z):** draft PR473 contains
the separate proven retirement at440123f6bc6dc358eef3d233236bc14f91af60e0,
based on merged PR468 b7194820d8f1940ae60b8c9639e09b7f61e65c55.
It removes exactly eight hosted workflows and edits four documentation files;
all1406 other tracked entries, including all retained workflows/tests/product/
runner configuration, remain identical. Branding,29F7 contracts and diff hygiene
PASS on the clean exact head. No legacy filename consumers remain.

Prior F7 two-green proof and real JS-only event remain durable. F8.4 two-green
proof was reviewed from existing F8 runs plus each source's native release lint
checks, without reruns. Pre-delete inventory/proof: diagnostics/phase-workflow-
retirement-preflight.json. Final source: diagnostics/phase-retirement-final-head-
validation.json. PR473 is draft; deletion is not merged yet.

Next run one scoped native F7 matrix on the exact retirement head using existing
workflow_dispatch, since its automatic filters do not select deleted sibling
files. Plan: diagnostics/pr473-native-validation-plan.json. Preserve automatic
PR473 and post-merge b719 CI; no intermediate full37 campaign. Keep head fixed
while evidence runs. Next routine review15:55Z near completion; do not short-poll.

PR468 final-head release16/16/Phase2 2/2 passes are retained. Issues470/471/472
stay open for original-host causes; a pass on Nettking did not diagnose AQG/Beast.
Last fully qualified candidate remains17ab3a05 (37+3). Physical runtime9b286f93
unchanged; no frozen new candidate, physical PASS, P07/P12, runner pool/account
changes or protected Recorder-data access. Full eventual candidate qualification
must use actual final merged source, preserving all earlier source-specific proof.

'''+s[b:];s+='\n## '+now+' — PR473 opened; single native validation planned\n\nDraft PR473 has exact head440123f6. Existing automatic runs are listed in diagnostics/pr473-native-validation-plan.json; no F7 run exists yet. One checked-in F7 workflow dispatch is planned solely for required exact deletion-source validation. No full campaign or original two-proof repetitions.\n';p.write_text(s,encoding='utf-8')
    print(json.dumps(plan))
elif mode=='dispatch':
    plan=json.loads(planpath.read_text());assert plan['status']=='PLANNED_NOT_DISPATCHED'
    ledger=dest/'pr473-native-validation-dispatch.json';assert not ledger.exists()
    runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs']
    assert not any(r['path']=='.github/workflows/phase-f7-closeout.yml' for r in runs)
    record={'recorded_at':now,'head_sha':head,'workflow':'phase-f7-closeout.yml','ref':branch,'status':'REQUESTING_ONCE_DO_NOT_BLINDLY_RETRY'}
    ledger.write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
    response=api('/actions/workflows/phase-f7-closeout.yml/dispatches',{'ref':branch})
    record.update({'response':response,'status':'DISPATCH_ACCEPTED' if response.get('http_status')==204 else 'RESPONSE_REQUIRES_REVIEW'})
    ledger.write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
    p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8')+'\n## '+now+' — PR473 native F7 dispatch accepted\n\nOne exact-head checked-in F7 dispatch returned204; receipt diagnostics/pr473-native-validation-dispatch.json. No duplicate dispatch, source change or full37 campaign. Confirm startup once, then defer progress review until15:55Z.\n';p.write_text(s,encoding='utf-8');print(json.dumps(record))
elif mode=='snapshot':
    rows=[]
    for source in [head,base]:
        for r in api('/actions/runs?head_sha='+source+'&per_page=100')['workflow_runs']:
            row={k:r.get(k) for k in ['id','path','head_sha','status','conclusion','event','run_attempt','created_at','updated_at','html_url']}
            row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs'];rows.append(row)
    snap={'recorded_at':now,'pr':473,'head_sha':head,'merged_base':base,'pr_merge_checkout':pr['merge_commit_sha'],'purpose':'Initial current-source progress; preserve postmerge/PR automatic jobs, no repeats','runs':rows}
    (dest/'pr473-initial-native-progress.json').write_text(json.dumps(snap,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'snapshot':'pr473-initial-native-progress.json','runs':[{'id':r['id'],'source':r['head_sha'][:8],'path':r['path'],'status':r['status'],'conclusion':r['conclusion'],'jobs':[{'id':j['id'],'name':j['name'],'status':j['status'],'conclusion':j['conclusion'],'runner':j['runner_name']} for j in r['jobs']]} for r in rows]}))
else:raise ValueError(mode)
