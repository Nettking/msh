import datetime,hashlib,json,pathlib,sys
ROOT=pathlib.Path('handoff');D=ROOT/'diagnostics';P=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');sys.path.insert(0,str(P))
from github_qualification import client
api=client();head='ba44100ec1e4cde19daba0d3723b991c11742316';assert api('/pulls/468')['head']['sha']==head
ret=json.loads((P/'D12-existing-go-build-pass-native-retention.json').read_text());assert ret['records'][0]['checkout_matches'];path=P/'D12-existing-go-build-pass-native-logs'/(str(ret['records'][0]['job_id'])+'.log');raw=path.read_bytes();assert hashlib.sha256(raw).hexdigest()==ret['records'][0]['sha256'];log=raw.decode();assert 'go build -trimpath .' in log
compare={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_head':head,'actual_checkout':ret['source_commit'],'existing_native_go_build_pass':ret,'command_excerpts':[l for l in log.splitlines() if 'go build -trimpath' in l or 'go test ./...' in l or 'go version go1.25.7' in l],'interpretation':'Same-source Go tests and default-VCS-stamping build passed F6 on AQG Windows; Phase2 Go tests passed Beast, then VCS introspection failed. Source inspection found no Git/env writes in sidecar Go tests. Host/toolchain context remains the leading unresolved mechanism; no product repair or provenance bypass is justified.','next':'One unchanged-source targeted Phase2 Windows retry for reproducibility; keep VCS stamping, source/workflow, accounts and deadlines unchanged.'}
(D/'D12-existing-go-build-comparison.json').write_text(json.dumps(compare,indent=2)+'\n',encoding='utf-8')
plan={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_sha':head,'attempt_limit':1,'status':'PLANNED_NOT_DISPATCHED','jobs':[{'finding':'D11','run_id':34695331201,'job_id':103557723475,'reason':'One contextual timeout; three same-source native passes, prior AQG admission verified; original failed ZIP retained. Retry only failed shard and required dependent aggregates.'},{'finding':'D12','run_id':34695331247,'job_id':103557723740,'reason':'VCS-status introspection failure after passing Go tests; same-source F6 Go build passed another native Windows host. Retry only failed Windows job, without altering stamping or source.'}],'passing_jobs_to_rerun':[],'complete_native_replacement_runs_to_rerun':[],'source_or_workflow_changes':[],'merge_or_retirement':False,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
for row in plan['jobs']:
 run=api('/actions/runs/'+str(row['run_id']));assert run['head_sha']==head and run['status']=='completed' and run['run_attempt']==1;row['verified_run_attempt']=run['run_attempt']
(D/'D11-D12-targeted-retry-plan.json').write_text(json.dumps(plan,indent=2)+'\n',encoding='utf-8')
p=ROOT/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');s+='\n## '+plan['recorded_at']+' — one targeted retry per failed job planned\n\n'+'''Original D11/D12 failures, issues and source-specific logs are durable. D11 original
ZIP is protected from rerun overwrite, and all successful shard/full-order evidence
is retained. AQG admission is verified from already-valid evidence. D12 same-source
F6 Go build passed AQG native Windows with VCS stamping unchanged. Host/timing/
toolchain mechanisms remain unresolved; there is no basis for product modification.

Plan: diagnostics/D11-D12-targeted-retry-plan.json. Both runs are completed attempt1
and current PR head is unchanged ba44100e. Retry only D11 failed shard2 and D12
failed Windows job once; GitHub may rerun required dependent aggregates. Do not
repeat successful jobs or the two complete replacement executions. Persist new
attempt/runner provenance; do not treat a retry pass as erasure of the failure.
If either recurs, preserve it and obtain focused host/stage diagnostics instead
of entering a blind retry loop. No deadline, authority or VCS checks are weakened.
''';p.write_text(s,encoding='utf-8')
