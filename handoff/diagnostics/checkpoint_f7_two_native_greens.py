import datetime,json,pathlib
root=pathlib.Path('handoff');d=root/'diagnostics';a=json.loads((d/'ci-f7-pr468-native-proof.json').read_text());b=json.loads((d/'ci-f7-pr469-native-proof.json').read_text());trigger=json.loads((d/'ci-f7-js-only-trigger-proof.json').read_text())
assert a['workflow_blob']==b['workflow_blob']==trigger['workflow_blob_head'];assert trigger['js_only_pull_request_event_observed']
o={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'TWO_COMPLETE_GREEN_NATIVE_REPLACEMENT_EXECUTIONS_REVIEWED','reviewed_runs':[{'pr':p['pr'],'run_id':p['run_id'],'head_sha':p['api_head_sha'],'checkout_sha':p['actual_checkout'],'receipt':'ci-f7-pr'+str(p['pr'])+'-native-proof.json'} for p in [a,b]],'native_executions':4,'workflow_blob':a['workflow_blob'],'test_identities_per_native_matrix':722,'all_unique_windows_ai_modules_passed':12,'all_transfer_modules_passed':3,'real_js_only_pull_request_event_verified':True,'trigger_receipt':'ci-f7-js-only-trigger-proof.json','canary_inert_comment_not_part_of_candidate':True,'required_next_gate':'Integrate D09 after this receipt is pushed; validate actual new head, unchanged executable source and branding/reference/status contracts. Diagnose separate release checkout failure before merge.','retirement_or_merge_approved':False,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
(d/'ci-f7-two-native-greens.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
for name in ['ci-migration-equivalence-matrix.json','ci-f7-extension-checkpoint.json']:
 p=d/name;r=json.loads(p.read_text());target=r.get('implementation',r)
 if 'implementation' in r:r['status']='TWO_NATIVE_GREENS_REVIEWED_FINAL_HEAD_VALIDATION_PENDING';target['native_runs_verified']=2
 else:target['native_replacement_executions_verified']=2
 target['two_native_greens_receipt']='ci-f7-two-native-greens.json';p.write_text(json.dumps(r,indent=2)+'\n',encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');i=s.index('Next highest-value action:');j=s.index('Pre-integration comparison:',i)
s=s[:i]+'''Next highest-value action: integrate the separate D09 docs-only repair into PR468
through clean history, then validate the actual head and source equivalence.
The existing canary Windows job PASSED at12:08Z. Both native replacement runs
are fully reviewed (2/2), with exact source/command/runtime/JUnit evidence:
diagnostics/ci-f7-two-native-greens.json. The actual JS-only event is verified.
Canary660bf232 must never be merged; PR468 remains f3abe545 until D09 integration.
The13:00-bound snapshot found all PR468/469 automatic jobs completed; preserve
all successes. Separate release failures occurred at checkout on Beast in three
Windows regression jobs; their aggregates are consequences. Inspect the exact
checkout error and persist its classification before any merge or rerun.
After integrating ba44100e, rerun branding and required lightweight final-head/
reference/status checks, preserve unchanged native coverage with original provenance,
and require all replacement proof before merge or legacy retirement.
'''+s[j:]
s+='\n## '+o['recorded_at']+' — 2/2 native replacement proof retained\n\n'+'''Canary Windows103544075521 and Linux103544075635 both passed, using exact
checkout aa7b41d328d8d6cab756951ec8739a370099756b with the660bf232 tree.
Both native pairs have722 identities passing across platforms; all12 mapped AI
modules, three transfer modules, strict lint/runtime/Compose/hygiene guards pass.
Original digest-verified JUnit ZIPs and native receipts are retained. This meets
the two-run replacement proof requirement, not final-head validation or acceptance.

All PR468/469 automatic runs completed. New release failures are localized to
Beast checkout (three Windows shard jobs), with downstream red aggregates; exact
logs/classification are next. D09 branding and hosted billing reds remain separate.
No source head moved, CI restarted, physical runtime or Recorder data changed.
''';p.write_text(s,encoding='utf-8')
private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
latest=sorted(d.glob('ci-migration-auto-runs-*.json'))[-1];snap=json.loads(latest.read_text())
for pr,sha in [(468,'b0fbb8a1a4e216b8696b8015a35594c1007319de'),(469,'aa7b41d328d8d6cab756951ec8739a370099756b')]:
 row=next(r for r in snap['runs'] if r['pr']==pr and r['path'].endswith('federation-v1-release.yml'))
 jobs=[j for j in row['jobs'] if any(s['name']=='Run actions/checkout@v4' and s['conclusion']=='failure' for s in j['steps'])]
 (private/('ci-migration-pr'+str(pr)+'-checkout-qualification-latest.json')).write_text(json.dumps({'source_commit':sha,'review_head':row['head_sha'],'workflows':[{'workflow':'federation-v1-release.yml','run_id':row['id'],'jobs':jobs}]},indent=2)+'\n')
