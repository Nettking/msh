"""Persist one confirmed CI blocker or link its published issue, then push externally."""
import datetime,json,pathlib,sys
dest=pathlib.Path(__file__).resolve().parent;root=dest.parent
identifier=sys.argv[1];path=dest/(identifier+'-evidence.json');e=json.loads(path.read_text(encoding='utf-8'))
if len(sys.argv)>2:e['github_artifact']=sys.argv[2]
e['recorded_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
path.write_text(json.dumps(e,indent=2)+'\n',encoding='utf-8')
fields={'Finding':identifier,'Status':e['status'],'Candidate SHA':e['candidate_sha'],'Actual checkout':e['actual_checkout'],'Host(s)':e['hosts'],'Physical stage':e['stage'],'Observed':e['observed'],'Expected':e['expected'],'Classification':e['classification'],'Acceptance impact':e['acceptance_impact'],'Safe continuation':e['safe_continuation'],'Evidence':'['+identifier+'-evidence.json]('+identifier+'-evidence.json), [original native logs](pr475-completed-failures-logs.zip)','GitHub artifact':e['github_artifact'],'Repair':e['repair'],'Next diagnostic action':e['next_diagnostic_action'],'State changed':e['state_changed'],'Protected Recorder data':e['protected_recorder_data']}
(dest/(identifier+'.md')).write_text('# '+identifier+': '+e['title']+'\n\n'+'\n\n'.join('**'+k+':** '+str(v) for k,v in fields.items())+'\n',encoding='utf-8')
table=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';lines=table.read_text(encoding='utf-8').splitlines();row='|'+identifier+'|'+e['stage']+'|'+e['observed']+'|'+e['classification']+'|Native CI; focused diagnosis pending|PR475 release gate|'+e['safe_continuation']+'|'+e['github_artifact']+'; '+e['repair']+'|'
indices=[i for i,s in enumerate(lines) if s.startswith('|'+identifier+'|')]
if indices:lines[indices[0]]=row
else:
    last=max(i for i,s in enumerate(lines) if s.startswith('|D') and s[2:3].isdigit());lines.insert(last+1,row)
table.write_text('\n'.join(lines)+'\n',encoding='utf-8')
coord=root/'QUALIFICATION_COORDINATION.md';s=coord.read_text(encoding='utf-8')
marker='Current user direction, September 11 2026:'
current='\n**'+e['recorded_at']+' — '+identifier+' confirmed:** '+e['observed']+' Classification: '+e['classification']+'. Candidate5e6f1843 / actuala5e743fe, run34707260030/job'+str(e['job_id'])+'. '+e['github_artifact']+'. Receipt diagnostics/'+identifier+'.md. No retry or source change. NEXT: '+e['next_diagnostic_action']+'\n\n'
s=s.replace(marker,current+marker,1);coord.write_text(s,encoding='utf-8')
print(json.dumps({'finding':identifier,'artifact':e['github_artifact']}))
