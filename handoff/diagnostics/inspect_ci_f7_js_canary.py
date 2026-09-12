import datetime,json,pathlib,subprocess,sys
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();p=api('/pulls/469');files=api('/pulls/469/files?per_page=100');base=p['base']['sha'];head=p['head']['sha']
assert base=='f3abe5452db2f21593a688bc62bc5f4b22d5c40e'
assert head=='660bf23269893305c7e8ffcc4c910a7efaed567d'
assert len(files)==1 and files[0]['filename']=='catalog/flask_app/static/js/ai-explainer.js'
assert files[0]['additions']==1 and files[0]['deletions']==0
runs=api('/actions/runs?head_sha='+head+'&per_page=100')['workflow_runs'];rows=[]
for r in runs:
    row={k:r.get(k) for k in ['id','name','path','head_sha','event','status','conclusion','created_at','updated_at','run_attempt','html_url','pull_requests']}
    if r['path'].endswith('phase-f7-closeout.yml'):
        row['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
    rows.append(row)
def git(*args):
    return subprocess.check_output(['git','-C',r'C:\wsl\fcp-ci-f7-js-trigger-canary-20260912',*args],text=True).strip()
wf='.github/workflows/phase-f7-closeout.yml';bb=git('rev-parse',base+':'+wf);hb=git('rev-parse',head+':'+wf);assert bb==hb
changed=git('diff','--name-only',base,head).splitlines();assert changed==[files[0]['filename']]
f7=[r for r in rows if r['path'].endswith('phase-f7-closeout.yml')]
assert len(f7)<=1
if f7:assert f7[0]['event']=='pull_request' and f7[0]['head_sha']==head
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pr':469,'url':p['html_url'],'purpose':'DISPOSABLE_JS_ONLY_TRIGGER_PROOF_DO_NOT_MERGE','base_branch':p['base']['ref'],'base_sha':base,'head_sha':head,'merge_commit_sha':p['merge_commit_sha'],'files':files,'workflow_blob_base':bb,'workflow_blob_head':hb,'all_other_source_unchanged':True,'js_only_pull_request_event_observed':bool(f7),'native_green_review_complete':False,'runs':rows,'protected_recorder_data':'UNTOUCHED','physical_acceptance':False}
pathlib.Path('handoff/diagnostics/ci-f7-js-only-trigger-proof.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:v for k,v in out.items() if k not in ['files','runs']}))
for r in rows:
    print(json.dumps({k:v for k,v in r.items() if k not in ['jobs','pull_requests']}))
    for j in r.get('jobs',[]):print(json.dumps({k:j.get(k) for k in ['id','name','status','conclusion','runner_name','runner_id']}))
