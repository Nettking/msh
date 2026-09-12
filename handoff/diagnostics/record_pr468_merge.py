"""Persist the exact ordinary merge before beginning the separate retirement."""
import datetime
import json
import pathlib
import subprocess
import sys

root = pathlib.Path('handoff'); dest = root / 'diagnostics'
sys.path.insert(0, r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api = client(); pr = api('/pulls/468')
head = 'ba44100ec1e4cde19daba0d3723b991c11742316'
merged = 'b7194820d8f1940ae60b8c9639e09b7f61e65c55'
assert pr['merged'] and pr['head']['sha'] == head and pr['merge_commit_sha'] == merged
receipt = {'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'pr':468, 'head_sha':head, 'merge_sha':merged, 'merged_at':pr['merged_at'], 'merge_method':'ordinary merge with expected-head guard; no bypass', 'proof_checkpoint':'75b1191d', 'current_main':api('/git/ref/heads/main')['object']['sha'], 'next_action':'Prepare a separate coverage-proven workflow retirement change, verify existing F8 evidence before including F8.4, update documentation/manifest references, and validate exact retirement head. Preserve automatic main CI; do not dispatch an intermediate37-job campaign.', 'original_host_findings_open':[470,471,472], 'physical_runtime':'9b286f931497bf6291e215f6340443c5162826b0', 'physical_acceptance':False, 'protected_recorder_data':'UNTOUCHED'}
(dest / 'pr468-merge.json').write_text(json.dumps(receipt, indent=2)+'\n', encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');a=s.index('**Current actionable checkpoint');b=s.index('Current user direction, September 11 2026:')
s=s[:a]+'''**Current actionable checkpoint (2026-09-12T15:02Z):** PR468 is MERGED as
b7194820d8f1940ae60b8c9639e09b7f61e65c55 from reviewed ba44100e. Normal merge
used an expected-head guard; no status bypass. Receipt: diagnostics/pr468-merge.json.

The original2/2 native F7 replacement executions, real JS-only automatic event,
D09 documentation-only source comparison, branding and29focused contracts are
durable. Final PR release16/16 and Phase2 2/2 are green after one bounded retry
of each failed job; exact native outcomes reviewed. Issues470/471/472 remain
open for original-host root causes, without source repair or physical evidence.
AQG31 admission is verified from already-valid08:57Z evidence. No extra admission.

All eight legacy workflows remain. Next prepare a separate retirement change
using the explicit equivalence matrix, final references and manifest rules.
Review existing two-green F8 native evidence before including F8.4; otherwise
retire the proven F7 group independently. Update two OSL references and cleanup
manifest. Validate the exact retirement head and preserve automatic post-merge
CI. Do not manually launch an intermediate37-job campaign; final merged-main
qualification belongs to the final coherent cleanup state.

Actual main17ab3a05 remains the last fully qualified candidate (37+3); preserve
its evidence. b7194820 is a merged CI extension, not a newly frozen physical
candidate. Physical runtime9b286f93 unchanged; no physical PASS, P07/P12,
deployment, runner-account/pool changes or protected Recorder-data access.

'''+s[b:]
s=s.replace('## 2026-09-12T15:07Z — PR468 replacement gate complete, ready for merge','## 2026-09-12T15:02Z — PR468 replacement gate complete, ready for merge')
s+='\n## '+receipt['recorded_at']+' — PR468 merged\n\nOrdinary expected-head merge returned b7194820d8f1940ae60b8c9639e09b7f61e65c55. API confirms merged state and unchanged intended head. All eight legacy files remain; no physical/runtime or protected-data change. Next separate retirement preparation under the existing conditional user authorization.\n';p.write_text(s,encoding='utf-8')
p=root/'CI_COVERAGE_MIGRATION_PLAN.md';s=p.read_text(encoding='utf-8');s=s.replace('STAGE1 IMPLEMENTED in draft [PR468]','STAGE1 MERGED as b7194820 in [PR468]');a=s.index('## Current action');s=s[:a]+'''## Current action

PR468 is merged as b7194820d8f1940ae60b8c9639e09b7f61e65c55. Two native
replacement proofs, real JS-only event, final-source comparison, branding,
focused29 contracts and required final PR checks pass. See diagnostics/pr468-merge.json
and diagnostics/D11-D12-reviewed-retry-pass.json. Original host findings remain
open and are not physical evidence. All eight legacy files remain.

Next create the separate retirement branch from actual merged b7194820. Reuse
already-valid F7 equivalence evidence, review existing F8 proof before including
F8.4, update known manifest/OSL consumers and validate the exact deletion source.
No intermediate full37 campaign; preserve any automatic post-merge jobs.
''';p.write_text(s,encoding='utf-8')
print(json.dumps(receipt))
