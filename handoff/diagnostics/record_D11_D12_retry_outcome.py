"""Record completed bounded retries without polling or starting additional jobs."""
import datetime
import json
import pathlib

root = pathlib.Path('handoff')
dest = root / 'diagnostics'
private = pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
snapshot = json.loads((dest / 'pr468-ba44100e-auto-20260912T145714.json').read_text())
selected = []
for run in snapshot['runs']:
    if run['id'] not in [34695331201, 34695331247]:
        continue
    assert run['status'] == 'completed' and run['conclusion'] == 'success' and run['run_attempt'] == 2
    # GitHub copies successful jobs with new IDs and original timestamps.
    executed = [j for j in run['jobs'] if j['started_at'] >= '2026-09-12T14:17:53Z']
    selected.append({'workflow': run['path'].split('/')[-1], 'run_id': run['id'], 'jobs': executed})
receipt = {
    'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'head_sha': snapshot['head_sha'],
    'actual_checkout_expected': 'f18e91adae2cb198fb5cebad61ee202a38d2bd8e',
    'status': 'BOTH_TARGETED_RETRIES_GREEN_NATIVE_REVIEW_PENDING',
    'snapshot': 'pr468-ba44100e-auto-20260912T145714.json',
    'new_executions': selected,
    'original_failures': {'D11': 'https://github.com/Nettking/msh/issues/471', 'D12': 'https://github.com/Nettking/msh/issues/472'},
    'interpretation': 'API success on unchanged head. Original AQG/Beast root causes remain unresolved; pass on Nettking does not establish those causes. No repeated successful suites, repairs or physical evidence.',
    'next_action': 'Retain/review exact native checkout, test/build commands, results and new shard artifacts, then assess remaining final-source/reference/retirement gates. No further retries.',
    'state_changed': 'Only two CI-owned job executions and GitHub evidence/check states; source/workflow/runner configuration unchanged.',
    'protected_recorder_data': 'UNTOUCHED',
    'physical_acceptance': False,
}
(dest / 'D11-D12-targeted-retry-outcome.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
source = {'source_commit': receipt['actual_checkout_expected'], 'review_head': snapshot['head_sha'], 'workflows': selected}
(private / 'D11-D12-retry-pass-qualification-latest.json').write_text(json.dumps(source, indent=2) + '\n', encoding='utf-8')
p = root / 'QUALIFICATION_COORDINATION.md'
s = p.read_text(encoding='utf-8')
s = s.replace('**Current actionable checkpoint (2026-09-12T14:18Z):**', '**Current actionable checkpoint (2026-09-12T14:57Z):**')
start = s.index('Exactly one retry per failed job')
end = s.index('\nAll eight legacy workflows remain.', start)
s = s[:start] + '''Both bounded retries have now completed SUCCESS on the unchanged PR head:
release34695331201 attempt2 is16/16 green; Phase2 run34695331247 attempt2 is2/2
green. New executions ran on Nettking Linux/Windows. Original AQG/Beast failures
remain durable and their root causes unresolved; no retry pass erases them.
Receipt: diagnostics/D11-D12-targeted-retry-outcome.json. Next retain/review actual
native checkout/test/build evidence and new shard artifacts, then assess remaining
final-source/reference/retirement gates. No further retry or full qualification.
''' + s[end:]
s += '\n## ' + receipt['recorded_at'] + ' — both bounded retries completed green\n\nAPI snapshot and selected actual reexecutions are persisted in diagnostics/D11-D12-targeted-retry-outcome.json. Successful-job metadata copies have original earlier timestamps and are not claimed as additional executions. Native outcome review is next; original issues stay open.\n'
p.write_text(s, encoding='utf-8')
print(json.dumps({'new_executions': [{ 'run':w['run_id'], 'jobs':[{k:j.get(k) for k in ['id','name','runner_name','started_at','completed_at']} for j in w['jobs']]} for w in selected]}))
