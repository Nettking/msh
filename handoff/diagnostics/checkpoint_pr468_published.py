from pathlib import Path
import json
root=Path('handoff');p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');a=s.index('Next highest-value action:');b=s.index('Clean qualified checkout:',a)
s=s[:a]+'''Next highest-value action: review native F7 replacement evidence for draft
[PR468](https://github.com/Nettking/msh/pull/468), exact head
f3abe5452db2f21593a688bc62bc5f4b22d5c40e. Stage1 extension is pushed;
29 focused coverage-contract checks and lint/format/diff checks passed.
Initial automatic F7 run34689990990 started correctly on Nettking native Windows
and Nettking-Linux; Linux passed, Windows tests in progress at11:05Z. Preserve
all automatic PR468 runs. Next independent action: create the planned disposable
JS-only canary PR against this migration branch, retain its actual event/run/source
proof, then review two complete green native F7 executions before any retirement.
No duplicate37-job dispatch is needed for replacement proof. F7 jobs have30min
limits; check near expected completion, long release jobs no sooner than45-60min.
Plan: CI_COVERAGE_MIGRATION_PLAN.md; mapping: diagnostics/ci-migration-equivalence-matrix.json.
All eight legacy workflows remain. Release/F6/F8 and runner accounts/pools unchanged.
AQG Windows has fcp-windows but was offline at10:01Z, separate from AQG Linux;
Nettking supplies both current native jobs. No runner admission changes.
''' + s[b:]
s=s.replace('**Current actionable checkpoint (2026-09-12T10:00Z):**','**Current actionable checkpoint (2026-09-12T11:06Z):**')
s+='''\n## 2026-09-12T11:06Z — PR468 published and replacement started\n\nDraft PR468 binds f3abe5452db2f21593a688bc62bc5f4b22d5c40e; initial CI/source\nreceipt: diagnostics/pr468-initial-ci-snapshot.json. F7 automatic pull_request\nrun34689990990 uses the retained native matrix. Linux completed all test/lint/\nCompose/hygiene/evidence steps; Windows passed setup/dependencies/compile and\nwas testing. Complete native evidence review remains pending (zero verified runs).\nThe separate docs-portal run34689991015 failed admission on two hosted jobs: zero\nsteps, runner_id0, explicit payment/spending-limit annotations. This is the same\nknown CI infrastructure condition, not product failure; do not restart passing\njobs or broaden this eight-workflow migration into an unreviewed docs rewrite.\nNext: JS-only canary, then source/log/JUnit reconciliation and second green proof.\nNo physical state changed; protected Recorder data remained untouched.\n''';p.write_text(s,encoding='utf-8')
p=root/'CI_COVERAGE_MIGRATION_PLAN.md';s=p.read_text(encoding='utf-8').replace('Status: **PROPOSED; plan and matrix persisted before workflow changes.**','Status: **STAGE1 IMPLEMENTED in draft [PR468](https://github.com/Nettking/msh/pull/468); native equivalence proof pending.**\nPlan/matrix were pushed at41d89c63 before source edits. Exact source: f3abe5452db2f21593a688bc62bc5f4b22d5c40e.')
s=s.replace('implement/review stage1 in an isolated CI-only branch, validate its coverage mapping,\nthen obtain source-bound replacement evidence.','review PR468 native run34689990990 and create the planned JS-only canary;\nthen obtain two complete source-bound green replacement executions. Focused29\ncontract checks passed; see diagnostics/ci-f7-extension-checkpoint.json.')
p.write_text(s,encoding='utf-8')
p=root/'diagnostics/ci-migration-equivalence-matrix.json';o=json.loads(p.read_text());o['status']='STAGE1_IMPLEMENTED_NATIVE_PROOF_PENDING';o['implementation']={'pr':468,'head':'f3abe5452db2f21593a688bc62bc5f4b22d5c40e','receipt':'ci-f7-extension-checkpoint.json','initial_run':34689990990,'focused_contract_tests':29,'native_runs_verified':0,'js_only_event_verified':False,'legacy_retired':[]};p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
p=root/'diagnostics/ci-f7-extension-checkpoint.json';o=json.loads(p.read_text(encoding='utf-8-sig'));o['pr']=468;o['pr_url']='https://github.com/Nettking/msh/pull/468';p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
