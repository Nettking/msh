"""Retain the focused demonstrated defect and completed pre-pause assertions."""
import datetime
import hashlib
import json
import pathlib
import zipfile

root = pathlib.Path(__file__).parent
h = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
assert not (root / 'P06-demonstrated-composition-defect.json').exists()
repro = h / '.acceptance/p06-reproducer'
result = json.loads((repro / 'run-1/result.json').read_text())
assert result['red']['exception'] == 'TimeoutError'
assert result['red']['provider_content_present_and_equal']
assert result['red']['authenticated_storage_reply_diverted_to_lifecycle_other']['frame_ok']
assert result['green_control']['coordinator_committed']
assert result['red']['timeout_seconds'] == result['green_control']['timeout_seconds'] == 15.0
stale = h / '.acceptance/stale-responder-identity/result.json'
short = h / '.acceptance/onboarding-test/P10-P11-refusal-readonly-status.json'
assert json.loads(stale.read_text())['verdict'] == 'pass'
assert all(row['verdict'] == 'pass' for row in json.loads(short.read_text())['results'])
(root / 'P05-stale-responder-identity-result.json').write_bytes(stale.read_bytes())
(root / 'P10-P11-refusal-readonly-status.json').write_bytes(short.read_bytes())
archive = root / 'P06-composition-reproducer-evidence.zip'
with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as z:
    for path in (repro / 'reproduce_shared_reader.py', repro / 'run-1/result.json'):
        z.writestr(path.relative_to(h).as_posix(), path.read_bytes())
checks = root / 'pre-repair-additional-physical-evidence.zip'
e = h / 'evidence/v1-physical'
with zipfile.ZipFile(checks, 'x', zipfile.ZIP_DEFLATED) as z:
    for path in (e / 'campaign.json', *sorted((e / 'hosts').glob('*.json'))):
        z.writestr(path.relative_to(e).as_posix(), path.read_bytes())
    for scenario in ('P05', 'P10', 'P11'):
        for path in sorted((e / 'observations' / scenario).glob('*.json')):
            z.writestr(path.relative_to(e).as_posix(), path.read_bytes())
report = {
    'candidate': result['candidate'],
    'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'classification': 'product defect',
    'supersedes_disposition': 'P06-startup-disposition.json classified the initial observation as unresolved. The focused exact-source reproduction now demonstrates a materially credible applicable code defect.',
    'mechanism': 'Analysis can retain the original AI relay endpoint while creator storage is subsequently composed behind that same endpoint. Competing consumers can divert an authenticated successful local-provider reply into the analysis lifecycle unrelated-message queue, causing the authority request timeout.',
    'red': result['red'],
    'ordered_control': result['green_control'],
    'evidence_limit': 'This isolated exact-source reproducer uses the checked-in loopback transport fixture and actual provider/control/acknowledgement stores. It is regression evidence, not a physical acceptance PASS or proof of which live consumer took each captured frame.',
    'contract': 'docs/implementation/v1_robustness_reconciliation.md P05/P06/P07 require functioning supervised publication, durable progress and no silently dead required worker. Normal creator storage must complete authenticated logical ingest.',
    'release_effect': 'Physical campaign paused; C cannot be released with this demonstrated defect. Produce smallest repaired candidate, exact-source qualification, normal merge, actual-main qualification, new freeze and checked-in revalidation before fresh physical assertions.',
    'repair_worktree': 'C:/wsl/fcp-v1-storage-reply-repair-20260913',
    'repair_branch': 'codex/federation-v1-storage-reply-repair',
    'additional_pre_pause_checks': ['P05/stale-responder-pid', 'P10/emergency-floor', 'P11/helper-prestaged'],
    'C_progress': {'P01': '10/10 PASS', 'P03': '8/8 PASS', 'P04': '2/6 PASS', 'P05': '9/16 PASS', 'P10': '1/6 PASS', 'P11': '1/12 PASS'},
    'timed_prerequisite_correction': 'P07/P12 checked-in contracts permit meaningful owned product-generated history and do not require two real CNC endpoints. Separate CF7/CNC readiness requirements still apply. No duration, history threshold or assertion is weakened.',
    'reproducer_archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
    'additional_checks_archive_sha256': hashlib.sha256(checks.read_bytes()).hexdigest(),
    'P07': 'NOT STARTED',
    'P12': 'NOT STARTED',
    'physical_pass': False,
    'protected_data_accessed': False,
    'AQG_requested': False,
}
(root / 'P06-demonstrated-composition-defect.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'classification': report['classification'], 'C_progress': report['C_progress'], 'physical_campaign': 'PAUSED_FOR_REPAIR'}))
