import datetime,json,pathlib
root=pathlib.Path('handoff');d=root/'diagnostics';private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');records=[]
for pr,head in [(468,'f3abe5452db2f21593a688bc62bc5f4b22d5c40e'),(469,'660bf23269893305c7e8ffcc4c910a7efaed567d')]:
 label='ci-migration-pr'+str(pr)+'-checkout';r=json.loads((private/(label+'-native-retention.json')).read_text())
 for rec in r['records']:
  log=(private/(label+'-native-logs')/(str(rec['job_id'])+'.log')).read_text(encoding='utf-8')
  assert rec['checkout_commits']==[] and 'EPERM: operation not permitted' in log and '.pytest_cache' in log
  records.append({'pr':pr,'intended_head_sha':head,'expected_pr_merge_sha':r['source_commit'],'actual_candidate_checkout_completed':False,'retained_log':rec,'error_excerpts':[line for line in log.splitlines() if any(x in line for x in ['Permission denied','failed to remove','Unable to clean','##[error]'])]})
o={'finding':'D10','status':'CONFIRMED','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':'f3abe5452db2f21593a688bc62bc5f4b22d5c40e (intended PR source; checkout failed)','hosts':['Beast self-hosted Windows release runner'],'physical_stage':'NONE; CI actions/checkout before Windows release shards','procedure':'actions/checkout@v4 clean/reset; fallback recreation of the runner-owned work directory','observed':r"EPERM removing C:\actions-runner\_work\msh\msh\.pytest_cache; no successful candidate checkout, Python setup or test execution",'expected':'Runner-owned checkout is writable/cleanable and selected source is checked out before tests.','classification':'host/environment; immediate filesystem access failure. Underlying ACL/owner/open-handle cause remains unverified.','acceptance_impact':'Three automatic release shard jobs blocked; missing-artifact and three aggregate errors per run are downstream, not separate product regressions. Existing main17ab and two F7 native proofs unaffected.','safe_continuation':'YES: D09 docs integration/final-head review and independent healthy CI. Do not claim failed shards passed or rerun complete suites to mask the host issue.','state_changed':'Checkout action attempted clean/reset/recreate of its own CI workdir; investigation only read retained logs. No manual host mutation.','protected_recorder_data':'UNTOUCHED','physical_runtime_sha':'9b286f931497bf6291e215f6340443c5162826b0','records':records,'github_artifact':'PENDING_ISSUE_PUBLICATION','repair':'NONE; no product or runner-account/label change','next_action':'Publish host-environment issue with exact run/log references. Before retrying Beast jobs, inspect only CI-owned cache owner/ACL/reparse/open-handle state and current workload through an authorized read-only channel; preserve concurrent work. Continue separate D09 integration after 2/2 proof.'}
(d/'D10-evidence.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
(d/'D10.md').write_text('''# D10: Beast cannot clean its CI cache before checkout

**Finding:** D10

**Status:** CONFIRMED

**Candidate SHA:** `f3abe5452db2f21593a688bc62bc5f4b22d5c40e` intended by PR468; canary469 intends `660bf23269893305c7e8ffcc4c910a7efaed567d`. Neither failed job completed candidate checkout.

**Host(s):** Beast, self-hosted Windows release runner.

**Physical stage:** None; `actions/checkout@v4` before Windows release regression shards.

**Observed:** Permission denied / EPERM removing the CI-owned `.pytest_cache`; checkout fallback recreation also fails. Test and Python setup steps do not execute. Artifact upload then fails because no JUnit exists.

**Expected:** Cleanable runner-owned checkout and verified candidate checkout before tests.

**Classification:** Host/environment. Filesystem access failure is confirmed; ACL/owner/open-handle root cause is not yet established. No product failure was observed.

**Acceptance impact:** Two shards in PR468 release34689991000 and one in canary release34690234261 are blocked. Red release aggregates are consequences. Prior main17ab and 2/2 F7 replacement proofs remain valid for their sources.

**Safe continuation:** YES: preserve evidence, integrate D09 docs correction and continue independently healthy CI. Do not claim blocked shard coverage or change safety/runner policy.

**Evidence:** [source-specific native error excerpts and log hashes](D10-evidence.json).

**GitHub artifact:** Issue publication is the immediate next action.

**Repair:** NONE; environment investigation only. No runner service-account, label/pool or product change.

**Next diagnostic action:** Publish the environment issue, then inspect only CI-owned cache ownership/ACL/reparse/open-handle state and active workload through an authorized read-only host channel before any targeted recovery/retry.

The checkout action changed only its own CI workspace while attempting cleanup.
This investigation read retained logs. Physical runtime and protected Recorder
data were untouched; no cache/volume deletion, pruning or account change occurred.
''',encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8');pos=s.index('\n',s.index('| D09 |'));s=s[:pos]+'''\n| D10 | Post-sweep CI release checkout; no physical stage | Beast cannot remove runner-owned .pytest_cache (EPERM); three shards never reach candidate/test execution | Host/environment; underlying ACL/handle cause unresolved | Three jobs across468/469 | Those shards and dependent release aggregates; prior main/F7 evidence unaffected | YES: independent CI and D09 docs integration; preserve state | [Evidence](diagnostics/D10.md); issue pending; no product repair |'''+s[pos:];p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');s+='\n## '+o['recorded_at']+' — D10 checkout blocker classified before integration\n\n'+'''Three Beast release shard jobs failed to remove runner-owned .pytest_cache with
EPERM before candidate checkout/test execution. The host/environment blocker and
exact logs are persisted in D10; underlying ACL/open-handle cause remains unknown.
Missing JUnit and release aggregate reds are consequences. Next publish its durable
issue, then continue D09 integration; it is independent of the verified 2/2 native
replacement proof. No failing test is being relabeled or suppressed.
''';p.write_text(s,encoding='utf-8')
