"""Persist the two accepted single-job reruns and one initial progress snapshot."""
import datetime
import json
import pathlib
import sys

sys.path.insert(0, r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client

api = client()
root = pathlib.Path('handoff')
dest = root / 'diagnostics'
plan = json.loads((dest / 'D11-D12-targeted-retry-plan.json').read_text())
head = plan['head_sha']
assert api('/pulls/468')['head']['sha'] == head
receipt = {
    'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'head_sha': head,
    'dispatch_time_utc': '2026-09-12T14:18Z',
    'status': 'TWO_SINGLE_JOB_RETRIES_ACCEPTED',
    'tool': 'github_rerun_workflow_job',
    'tool_responses': {'D11': {'success': True}, 'D12': {'success': True}},
    'jobs': [],
    'passing_jobs_manually_rerun': [],
    'source_or_workflow_changes': [],
    'protected_recorder_data': 'UNTOUCHED',
    'physical_acceptance': False,
    'next_action': 'Inspect these retries near completion, no later than the 14:55Z heartbeat. Preserve actual attempt/runner/source logs and original failures. No further automatic retry if failure recurs.'
}
for planned in plan['jobs']:
    run_id = planned['run_id']
    run = api(f'/actions/runs/{run_id}')
    assert run['head_sha'] == head
    jobs = api(f'/actions/runs/{run_id}/jobs?filter=latest&per_page=100')['jobs']
    receipt['jobs'].append({
        'finding': planned['finding'],
        'original_job_id': planned['job_id'],
        'run': {k: run.get(k) for k in ['id', 'run_attempt', 'status', 'conclusion', 'head_sha', 'updated_at']},
        'latest_jobs': jobs
    })
(dest / 'D11-D12-targeted-retry-dispatch.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
p = root / 'QUALIFICATION_COORDINATION.md'
s = p.read_text(encoding='utf-8')
start = s.index('**Current actionable checkpoint')
end = s.index('Current user direction, September 11 2026:')
s = s[:start] + '''**Current actionable checkpoint (2026-09-12T14:18Z):** PR468 stays draft,
unmerged at ba44100ec1e4cde19daba0d3723b991c11742316. D09 documentation repair
is integrated and branding/final-source checks pass. The original two native
replacement executions and real AI-JavaScript-only automatic event are retained;
1417 non-document tracked entries are byte-identical to the proven source.

Actual merged main17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05 remains fully qualified
(37 required +3 companions). Do not poll or rerun that completed qualification.
Receipt: diagnostics/merged-main-final-qualification.json.

Final-head F6/F7/F8 and other independent checks passed. Two failures remain:
D11/#471 AQG Linux shard2 live-reinstatement retryable TimeoutError, and D12/#472
Beast Phase2 Windows Go VCS-status exit128 after Go tests passed. Root causes
remain unresolved. Three existing same-source native executions passed D11's
test; existing F6 on AQG Windows built the same Go source with VCS stamping.
AQG31 admission is VERIFIED from already-valid08:57Z evidence; no new admission.

Exactly one retry per failed job was accepted at14:18Z: D11 job103557723475 in
run34695331201; D12 job103557723740 in run34695331247. No successful job or full
native replacement matrix was manually repeated. GitHub may rerun dependent
release aggregates. Original failed D11 ZIP/logs and both issues were durably
retained first. Receipt: diagnostics/D11-D12-targeted-retry-dispatch.json.
Inspect retries near completion, no later than14:55Z; retain new job/attempt/
runner/source evidence. If either recurs, diagnose the actual stage/host instead
of another blind retry. A retry pass does not erase the original failure.

All eight legacy workflows remain. No merge or retirement until replacement-proof
and required final-source/status gates are satisfied. No product/workflow change,
runner account/pool change, physical deployment or protected Recorder access.
Physical runtime remains9b286f931497bf6291e215f6340443c5162826b0. No physical PASS;
P07/P12 have not started. Plans: CI_COVERAGE_MIGRATION_PLAN.md and diagnostics/
ci-migration-equivalence-matrix.json. Historical deltas below retain original dates.

''' + s[end:]
s += '\n## ' + receipt['recorded_at'] + ' — targeted retries dispatched\n\nBoth single-job API requests returned success. See diagnostics/D11-D12-targeted-retry-dispatch.json for the initial attempt/runner/progress snapshot. No source, workflow, runner, physical runtime or protected Recorder state was changed.\n'
p.write_text(s, encoding='utf-8')
for finding in ['D11', 'D12']:
    p = dest / (finding + '.md')
    s = p.read_text(encoding='utf-8')
    a = s.index('**Next diagnostic action:**')
    b = s.index('\n\n', a)
    s = s[:a] + '**Next diagnostic action:** One unchanged-source failed-job retry is now dispatched under [the persisted bounded plan](D11-D12-targeted-retry-plan.json). Inspect [dispatch/progress receipt](D11-D12-targeted-retry-dispatch.json), retain the exact attempt/runner/source outcome, and obtain focused stage/host diagnostics if it recurs. No additional blind retry, source repair or guard bypass is authorized by a retry pass.' + s[b:]
    s += '\n## 2026-09-12T14:18Z — single-job retry accepted\n\nThe original failure remains CONFIRMED, mechanism unresolved. Existing same-source passing evidence justified one controlled recurrence check; no product repair was inferred. Both issue and original evidence were pushed before dispatch. See [dispatch receipt](D11-D12-targeted-retry-dispatch.json).\n'
    p.write_text(s, encoding='utf-8')
print(json.dumps({'receipt': str(dest / 'D11-D12-targeted-retry-dispatch.json'), 'jobs': [{'finding': r['finding'], 'run': r['run'], 'new_jobs': [{k:j.get(k) for k in ['id','name','run_attempt','status','runner_name','started_at']} for j in r['latest_jobs'] if j.get('run_attempt',1)>1]} for r in receipt['jobs']]}))
