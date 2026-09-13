import datetime,json,pathlib,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
a=client();p=a('/pulls/485');sha='501b528e9476878e6a6fe5cde8240b2d54b1d263'
assert p['head']['sha']==sha
runs=[x for x in a('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs'] if x['head_branch']=='codex/p01-filesystem-growth-20260913']
out={'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'main':a('/git/ref/heads/main')['object']['sha'],'pr':485,'head':sha,'merge_ref':p['merge_commit_sha'],'draft':p['draft'],'mergeable_state':p['mergeable_state'],'runs':[]}
for x in runs:
 jobs=a('/actions/runs/'+str(x['id'])+'/jobs?per_page=100')['jobs']
 out['runs'].append({'id':x['id'],'workflow':x['path'],'status':x['status'],'conclusion':x['conclusion'],'jobs':[{k:j.get(k) for k in ['id','name','status','conclusion','started_at','completed_at','runner_name','check_run_url']} for j in jobs]})
for r in out['runs']:
 if r['id']==34756997305:
  for j in r['jobs']:
   j['annotations']=a('/check-runs/'+j['check_run_url'].rsplit('/',1)[1]+'/annotations')
  r['disposition']='Infrastructure: hosted jobs never started because account billing/spending limits refuse admission. No candidate execution or candidate defect demonstrated.'
path=pathlib.Path(__file__).parent/'growth-repair-ci-current.json';path.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'reviewed_at':out['reviewed_at'],'main':out['main'],'runs':[{'id':r['id'],'workflow':r['workflow'],'status':r['status'],'conclusion':r['conclusion'],'jobs':[{'name':j['name'],'status':j['status'],'conclusion':j['conclusion']} for j in r['jobs']]} for r in out['runs']]}))
