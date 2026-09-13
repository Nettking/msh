"""Snapshot one guarded actual-main qualification, optionally fill missing gates once."""
import argparse,datetime,json,pathlib,re,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client,REQUIRED
p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--dispatch-missing',action='store_true');args=p.parse_args()
assert re.fullmatch('[0-9a-f]{40}',args.source)
root=pathlib.Path(__file__).parent/f'main-{args.source[:8]}-qualification';root.mkdir(exist_ok=True)
expected=dict(REQUIRED,**{'cfi2-onboarding-composition.yml':2,'release-image-metadata.yml':1})
a=client()
def guard():assert a('/git/ref/heads/main')['object']['sha']==args.source,'Main changed; do not dispatch or freeze'
guard()
def select():
 selected={}
 for r in sorted(a('/actions/runs?head_sha='+args.source+'&per_page=100')['workflow_runs'],key=lambda r:r['id']):
  name=r['path'].split('/')[-1]
  if name in expected:selected[name]=r
 return selected
runs=select()
if args.dispatch_missing:
 receipt=root/'gap-dispatch.json'
 if receipt.exists():raise SystemExit('Receipt exists; inspect live state, do not redispatch')
 actions={'source':args.source,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'existing_runs':{n:r['id'] for n,r in runs.items()},'actions':[]}
 def save():receipt.write_text(json.dumps(actions,indent=2)+'\n')
 save()
 for name in expected:
  if name in runs:continue
  guard();live=select()
  if name in live:continue
  action={'workflow':name,'status':'dispatching'};actions['actions'].append(action);save()
  action.update(status='accepted',response=a('/actions/workflows/'+name+'/dispatches',{'ref':'main'}));save()
 runs=select()
out={'source':args.source,'main':args.source,'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'missing_workflows':sorted(set(expected)-set(runs)),'workflows':[]}
for name,r in runs.items():
 jobs=a(f"/actions/runs/{r['id']}/jobs?per_page=100")['jobs']
 out['workflows'].append({'workflow':name,'run_id':r['id'],'api_head_sha':r['head_sha'],'event':r['event'],'attempt':r['run_attempt'],'status':r['status'],'conclusion':r['conclusion'],'required_jobs':expected[name],'jobs':[{k:j.get(k) for k in ['id','name','status','conclusion','started_at','completed_at','runner_name']} for j in jobs]})
out['all_expected_green']=not out['missing_workflows'] and all(w['conclusion']=='success' and len(w['jobs'])==w['required_jobs'] and all(j['conclusion']=='success' for j in w['jobs']) for w in out['workflows'])
(root/'qualification-current.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='workflows'}))
print(json.dumps([{'workflow':w['workflow'],'run_id':w['run_id'],'status':w['status'],'conclusion':w['conclusion'],'passed':sum(j['conclusion']=='success' for j in w['jobs']),'expected':w['required_jobs']} for w in out['workflows']]))
