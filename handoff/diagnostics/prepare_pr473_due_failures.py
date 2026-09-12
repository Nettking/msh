"""Retain new completed failures separately from successful release evidence."""
import datetime
import json
import pathlib

root=pathlib.Path('handoff');dest=root/'diagnostics';private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
snapshot=json.loads((dest/'pr473-current-20260912T155828.json').read_text())
out=[]
for head,label,checkout in [(snapshot['head_sha'],'pr473-new-failures',snapshot['pr_merge_checkout']),(snapshot['merged_base'],'main-b719-new-failures',snapshot['merged_base'])]:
    workflows=[]
    for r in snapshot['runs']:
        if r['head_sha']!=head or r['path'].endswith('/docs-portal.yml'):continue
        jobs=[j for j in r['jobs'] if j['conclusion']=='failure' and j['id']!=103572678982]
        if not jobs:continue
        workflows.append({'workflow':r['path'].split('/')[-1],'run_id':r['id'],'jobs':jobs})
        out.extend([{'source':head,'checkout':checkout,'run':r['id'],'job':j['id'],'runner':j['runner_name'],'failed_steps':[s for s in j['steps'] if s['conclusion']=='failure']} for j in jobs])
    (private/(label+'-qualification-latest.json')).write_text(json.dumps({'source_commit':checkout,'review_head':head,'workflows':workflows},indent=2)+'\n',encoding='utf-8')
release=next(r for r in snapshot['runs'] if r['id']==34701018870)
assert release['conclusion']=='success' and len(release['jobs'])==16
(private/'main-b719-release-pass-qualification-latest.json').write_text(json.dumps({'source_commit':snapshot['merged_base'],'workflows':[{'workflow':'federation-v1-release.yml','run_id':release['id'],'jobs':release['jobs']}]},indent=2)+'\n',encoding='utf-8')
receipt={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'snapshot':'pr473-current-20260912T155828.json','new_failed_jobs_pending_native_classification':out,'known_D12_job_not_reread':103572678982,'main_b719_release':'16/16 PASS native retention next; not full candidate qualification','pr473_release':'One failed check, shard2 still executing at15:58Z; no repoll before16:43Z','no_jobs_rerun':True,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
(dest/'pr473-due-failure-triage-plan.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');s+='\n## '+receipt['recorded_at']+' — due automatic checks expose additional failures\n\nMain b719 release34701018870 completed16/16green; preserve exact native/artifact evidence. PR473 release34701429430 has a failed Windows release check and shard2 still running. Post-merge F7/F8/update Windows on Beast and Phase2 Windows on AQG also failed. Native classification is pending; do not infer they share D12. Snapshot and selected failing steps are durable in diagnostics/pr473-current-20260912T155828.json and diagnostics/pr473-due-failure-triage-plan.json. Next retain/read only newly failing logs, classify each mechanism and publish independent findings immediately. No retries/source changes. PR473 stays draft.\n';p.write_text(s,encoding='utf-8')
print(json.dumps(out))
