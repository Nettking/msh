"""Persist a new independent CI failure before further diagnosis."""
import datetime
import json
import pathlib

root = pathlib.Path('handoff')
dest = root / 'diagnostics'
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
private = pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
log = (private / 'pr473-corrected-host-retry-failed-native-logs/103583206021.log').read_text(encoding='utf-8', errors='replace')
excerpt = log[log.index('2026-09-12T16:24:11.5129233Z'):log.index('2026-09-12T16:24:11.9510287Z')]
(dest / 'D13-native-excerpt.log').write_text(excerpt, encoding='utf-8')
evidence = {
    'finding': 'D13', 'status': 'CONFIRMED', 'recorded_at': now,
    'candidate_sha': '440123f6bc6dc358eef3d233236bc14f91af60e0',
    'actual_checkout': '0355023f27c4c6886ab6beb48bb9a998b3ab2904',
    'identical_source_tree': 'ad163ea2ac60b4bf26177f84fd47660453e5c041',
    'host': 'NETTKING native Windows', 'run_id': 34701429430, 'attempt': 2, 'job_id': 103583206021,
    'stage': 'Release checks (Windows) / Discovery authority and bounded startup regressions; CI only',
    'test': 'catalog/federation/tests/test_tailnet_join_responder.py::test_unknown_paths_are_refused',
    'procedure': 'Checked-in release discovery pytest selection; POST {} to a test-owned 127.0.0.1 ephemeral responder at /anything-else with existing 5 second client timeout',
    'observed': 'ConnectionAbortedError WinError 10053 while reading HTTP status; server logged 404. Selection: 1 failed,149 passed,7 skipped in34.83s.',
    'expected': 'Client receives HTTP404; no pairing-authority invocation.',
    'classification': 'UNRESOLVED: product HTTP lifecycle, test lifecycle, or host/transient transport cause requires diagnosis.',
    'acceptance_impact': 'PR473 Windows release check and its three dependency aggregates remain red; no physical acceptance claim.',
    'safe_continuation': 'YES: source inspection and bounded isolated CI-owned loopback reproduction; no blanket retry or merge.',
    'state_changed': 'Only CI checkout, installed temporary dependencies and ephemeral test fixture resources; no new host repair during this retry.',
    'protected_recorder_data': 'UNTOUCHED', 'physical_runtime': 'UNCHANGED;9b286f931497bf6291e215f6340443c5162826b0',
    'repair': 'NONE', 'github_artifact': 'PENDING immediate issue publication',
    'hypotheses': ['Unknown-path handler returns before reading POST body; unread bytes and close behavior may abort the Windows socket. Not yet established as root cause.'],
    'next_diagnostic_action': 'Inspect unchanged responder/test socket lifecycle, then run only the failing test in a bounded isolated NETTKING loopback reproduction with existing deadlines and no production services.',
    'evidence': ['pr473-corrected-host-retry-failed-native-retention.json', 'pr473-corrected-host-retry-failed-logs.zip', 'D13-native-excerpt.log', 'pr473-D12-retry-20260912T163928.json'],
    'D12_note': 'Beast trust repair remains verified on Beast. Three prior launcher failures did not recur in this Nettking execution; different-host pass does not establish their original-host CI resolution.',
}
(dest / 'D13-evidence.json').write_text(json.dumps(evidence, indent=2) + '\n', encoding='utf-8')
md = '\n\n'.join('**' + k + ':** ' + str(v) for k,v in {
    'Finding': 'D13', 'Status': 'CONFIRMED', 'Candidate SHA': evidence['candidate_sha'],
    'Actual checkout': evidence['actual_checkout'] + ' (identical tree)', 'Host(s)': evidence['host'],
    'Physical stage': evidence['stage'], 'Observed': evidence['observed'], 'Expected': evidence['expected'],
    'Classification': evidence['classification'], 'Acceptance impact': evidence['acceptance_impact'],
    'Safe continuation': evidence['safe_continuation'], 'Evidence': '[Exact receipt](D13-evidence.json), [native excerpt](D13-native-excerpt.log), [complete native logs](pr473-corrected-host-retry-failed-logs.zip)',
    'GitHub artifact': evidence['github_artifact'], 'Repair': 'NONE', 'Next diagnostic action': evidence['next_diagnostic_action'],
    'State changed': evidence['state_changed'], 'Protected Recorder data': 'UNTOUCHED',
}.items())
(dest / 'D13.md').write_text('# D13: Windows unknown-path refusal aborts before client receives HTTP404\n\n' + md + '\n', encoding='utf-8')
table = root / 'FEDERATION_V1_DIAGNOSTIC_SWEEP.md'
s = table.read_text(encoding='utf-8')
lines = s.splitlines()
last = max(i for i,line in enumerate(lines) if line.startswith('|D12|') or line.startswith('| D12 '))
lines.insert(last+1, '|D13|CI Windows discovery/refusal|HTTP404 logged but client receives WinError10053|Unresolved transport/lifecycle|One native failure; focused reproduction pending|PR473 release gate|Yes, isolated loopback diagnosis|NONE; issue publication next|')
table.write_text('\n'.join(lines) + '\n', encoding='utf-8')
coord = root / 'QUALIFICATION_COORDINATION.md'
s = coord.read_text(encoding='utf-8')
start = s.index('**Current actionable checkpoint')
end = s.index('Current user direction, September 11 2026:')
new = f'''**Current actionable checkpoint ({now}):** PR473 stays fixed at
440123f6bc6dc358eef3d233236bc14f91af60e0. Its native F7 final-source proof,
original2/2 replacement proof and real JS-only event remain valid and retained.

The single affected-check retry34701429430/attempt2/job103583206021 completed
on Nettking with a DIFFERENT failure: D13 unknown-path refusal logged404 but
the client received WinError10053.149passed/7skipped/1failed; three red aggregates
are dependent consequences. Exact checkout0355023f/tree==440 verified in native
logs. See diagnostics/D13.md and full logs/receipt linked there. No further retry.
Next publish D13 issue immediately, then inspect unchanged socket lifecycle and
perform only a bounded isolated loopback reproduction with current deadlines.

D12 Beast scoped Git trust repair remains verified on the actual original runner;
the three former launcher failures did not recur on Nettking, but this does not
prove their original-host CI resolution. D11 remains unresolved on AQG; neither
finding is reclassified by D13. Main b719 release16/16 evidence is archived, its
companion failures preserved. Last fully qualified candidate17ab3a05(37+3) remains.
No merge/new candidate or blanket rerun. No physical PASS/P07/P12, deployment,
protected Recorder-data access, Docker reset/prune or new runner/account changes.
Physical runtime9b286f93 remains untouched. The coordination maintenance workflow
must never enter a candidate. No live jobs remain in the targeted release run.

'''
coord.write_text(s[:start] + new + s[end:], encoding='utf-8')
print(json.dumps({'finding':'D13','next':'commit and push, then publish GitHub issue before further diagnosis'}))
