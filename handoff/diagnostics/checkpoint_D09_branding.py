import datetime,json,pathlib,subprocess
root=pathlib.Path('handoff');private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');records=[]
for pr,rid,jid,head,checkout in [(468,34689990970,103543444121,'f3abe5452db2f21593a688bc62bc5f4b22d5c40e','b0fbb8a1a4e216b8696b8015a35594c1007319de'),(469,34690234307,103544075485,'660bf23269893305c7e8ffcc4c910a7efaed567d','aa7b41d328d8d6cab756951ec8739a370099756b')]:
 label='ci-migration-pr'+str(pr)+'-branding';r=json.loads((private/(label+'-native-retention.json')).read_text())['records'][0]
 log=(private/(label+'-native-logs')/(str(jid)+'.log')).read_text(encoding='utf-8');assert r['checkout_matches'] and 'retired product spelling in file: docs/implementation/f7_ci_consolidation.md' in log
 records.append({'pr':pr,'run_id':rid,'job_id':jid,'head_sha':head,'checkout_sha':checkout,'log_receipt':r,'error_excerpts':[line for line in log.splitlines() if 'check_product_branding.py' in line or 'retired product spelling in file:' in line or 'Process completed with exit code 1' in line]})
o={'finding':'D09','status':'CONFIRMED','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':'f3abe5452db2f21593a688bc62bc5f4b22d5c40e','hosts':['Nettking-Linux (self-hosted CI, runner27)'],'physical_stage':'NONE; CI migration product-branding check','command':'python scripts/check_product_branding.py','observed':'exit1: retired product spelling in file: docs/implementation/f7_ci_consolidation.md','expected':'exit0 without retired product spellings in new documentation','classification':'CI migration documentation regression; branding harness is correct, not a runtime or hosted-billing failure','mechanism':'Two repository-URL links in the new document contain the prohibited repository slug; the file has no scoped exception. Do not weaken the checker or add an exception.','acceptance_impact':'PR468 cannot merge as-is; completed main17ab qualification and first F7 proof remain valid for their exact sources; no physical acceptance','safe_continuation':'YES: preserve running immutable CI and develop a documentation-only correction on a separate branch; do not update PR468/469 source while doing so would cancel their valid active jobs','records':records,'state_changed':'CI-owned checkout/venv only; investigation read-only','protected_recorder_data':'UNTOUCHED','physical_runtime_sha':'9b286f931497bf6291e215f6340443c5162826b0','github_artifact':'https://github.com/Nettking/msh/pull/468','repair':'planned documentation-only branch; not yet developed','next_action':'Replace the two raw repository URLs with explicit coordination branch/path references; run unchanged branding checker and diff hygiene; push isolated repair branch and link from draft PR468. Promote to PR468 only after existing runs finish.'}
(root/'diagnostics/D09-evidence.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
(root/'diagnostics/D09.md').write_text('''# D09: migration documentation violates the existing branding contract

**Finding:** D09  
**Status:** CONFIRMED  
**Candidate SHA:** `f3abe5452db2f21593a688bc62bc5f4b22d5c40e`  
**Host(s):** Nettking-Linux, native self-hosted CI runner27  
**Physical stage:** None; CI migration product-branding gate  
**Observed:** `python scripts/check_product_branding.py` exits1: `retired product spelling in file: docs/implementation/f7_ci_consolidation.md`.  
**Expected:** New documentation passes the unchanged branding contract.  
**Classification:** CI migration documentation regression. The branding guard is correct; this is neither a runtime defect nor a hosted-billing failure.  
**Acceptance impact:** PR468 cannot merge as-is. Preserve completed17ab qualification and first F7 proof under their own exact sources. No physical acceptance or timing changes.  
**Safe continuation:** YES: independent immutable CI can finish, and a documentation-only correction can be prepared/pushed separately. Do not advance the active PR heads while that would cancel valid running jobs.  
**Evidence:** [exact native logs, SHA and command receipts](D09-evidence.json); [run34689990970](https://github.com/Nettking/msh/actions/runs/34689990970) and [canary run34690234307](https://github.com/Nettking/msh/actions/runs/34690234307). Both actual synthetic checkouts verified.  
**GitHub artifact:** [existing draft PR468](https://github.com/Nettking/msh/pull/468)  
**Repair:** Documentation-only correction to be pushed on a separate branch; no checker exception or product edit.  
**Next diagnostic action:** Replace the two prohibited raw repository URLs with explicit coordination branch/path references, run the unchanged checker/diff hygiene, push repair branch and reference it from draft468. Promote only when existing runs have finished.

Both URLs are in the newly added migration document. The checker intentionally
rejects the old spelling globally outside scoped compatibility exceptions. Keep
that policy unchanged. CI changed only its own checkout/venv; investigation was
read-only. Physical runtime9b286f93 and protected Recorder data were untouched.
''',encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8');pos=s.index('\n',s.index('| D08 |'));s=s[:pos]+'''\n| D09 | Post-sweep CI migration branding gate; no physical stage | New migration document contains two prohibited repository-URL spellings; checker exits1 | CI documentation regression; checker correct, no runtime defect | Same exact native error on PR468 and canary469 | Merge of current PR468 head; prior main/F7 evidence remains source-valid | YES: immutable CI and separate docs-only correction; do not cancel active runs | [Evidence](diagnostics/D09.md); existing [draft468](https://github.com/Nettking/msh/pull/468); correction pending |'''+s[pos:];p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');a=s.index('Next highest-value action:');b=s.index('No duplicate37-job',a);s=s[:a]+'''Next highest-value action: resolve the CI-only documentation blocker D09 in draft
PR468 (current head f3abe5452db2f21593a688bc62bc5f4b22d5c40e). The unchanged
branding check rejects two repository URLs in the new migration document.
Evidence: diagnostics/D09.md. Prepare/push a docs-only repair on a separate branch;
do not update current PR468/469 heads while their automatic runs are active.
Keep the checker and all workflow/product semantics unchanged.
First F7 native proof remains reviewed green. JS-only canary PR469/run34690234286
has Linux green and Windows queued at11:57Z; no second complete proof yet.
PR468 F6/F8 and Phase2 pairs are green; both PRs have active/queued release work
with no release failure at the snapshot. Preserve all running/completed evidence.
Hourly snapshot: diagnostics/ci-migration-auto-runs-20260912T1157.json.
'''+s[b:];s+='\n## '+o['recorded_at']+' — D09 confirmed before repair\n\n'+'''New independent CI documentation blocker recorded in the accumulating table and
D09 evidence. Native branding executed and failed on both source-bound PRs; do
not group it with zero-step hosted billing reds. The two new document URLs are
the immediate cause. Next: isolated docs-only correction and unchanged focused
branding check; keep active PR heads stable until automatic CI finishes.
Canary Linux F7 has completed green; Windows remains queued. No physical changes.
''';p.write_text(s,encoding='utf-8')
