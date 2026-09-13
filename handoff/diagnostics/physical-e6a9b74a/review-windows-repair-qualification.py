import datetime,json,pathlib,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client,REQUIRED
source='94567b0d9ac916eec9e4f094d2f6754562b966aa';head='fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe'
a=client();p=a('/pulls/486');assert p['head']['sha']==head
expected=dict(REQUIRED,**{'cfi2-onboarding-composition.yml':2,'release-image-metadata.yml':1})
runs={}
for sha in [source,head]:
 for r in a('/actions/runs?head_sha='+sha+'&per_page=100')['workflow_runs']:runs[r['id']]=r
selected={}
for r in sorted(runs.values(),key=lambda x:x['id']):
 workflow=r['path'].split('/')[-1]
 if workflow in expected:selected[workflow]=r
out={'source':source,'review_head':head,'pr':486,'main':a('/git/ref/heads/main')['object']['sha'],'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'missing_workflows':sorted(set(expected)-set(selected)),'workflows':[]}
for name,r in selected.items():
 jobs=a('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
 out['workflows'].append({'workflow':name,'run_id':r['id'],'api_head_sha':r['head_sha'],'event':r['event'],'status':r['status'],'conclusion':r['conclusion'],'required_jobs':expected[name],'jobs':[{k:j.get(k) for k in ['id','name','status','conclusion','started_at','completed_at','runner_name']} for j in jobs]})
out['all_expected_green']=not out['missing_workflows'] and all(w['conclusion']=='success' and len(w['jobs'])==w['required_jobs'] and all(j['conclusion']=='success' for j in w['jobs']) for w in out['workflows'])
(pathlib.Path(__file__).parent/'windows-repair-qualification-current.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='workflows'}));print(json.dumps([{'workflow':w['workflow'],'run_id':w['run_id'],'status':w['status'],'conclusion':w['conclusion'],'passed':sum(j['conclusion']=='success' for j in w['jobs']),'expected':w['required_jobs']} for w in out['workflows']]))
