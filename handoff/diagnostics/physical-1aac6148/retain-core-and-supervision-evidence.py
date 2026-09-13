"""Preserve completed core crash proof and the bounded native startup disposition."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys
import zipfile

root = pathlib.Path(__file__).parent
h = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
c = h / '.acceptance/onboarding-test'
n = h / '.acceptance/native-faults'
e = h / 'evidence/v1-physical'
sha = '1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (root / 'P06-startup-disposition.json').exists(), 'Preserve the original disposition'
sys.path.insert(0, str(h))
from scripts.acceptance.v1_physical_campaign import redact_text, scenario_status

core = json.loads((c / 'P05-core-crashes-status.json').read_text())
supervision = json.loads((n / 'P06-supervision-status.json').read_text())
stacks = json.loads((n / 'P06-publication-stacks.json').read_text())
assert core['status'] == 'COMPLETED' and len(core['results']) == 2
assert all(row['verdict'] == 'pass' and row['other_cores_unchanged'] and row['after_restarts'] == row['before_restarts'] + 1 for row in core['results'])
assert supervision['status'] == 'STOPPED' and supervision['operator_stop_exit_code'] == 0 and not supervision['results']
assert stacks['status'] == 'CAPTURED' and len(stacks['samples']) == 4
assert all(row['loop_callback_observed'] for row in stacks['samples'])
command = "@(Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -ne $PID -and $_.Name -match '^(python|pythonw|powershell|pwsh).*' -and ($_.CommandLine -like '*scripts.start_tailscale_recorder*' -or $_.CommandLine -like '*start-production-supervisor.ps1*') }).Count"
count = int(subprocess.check_output(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', command], text=True, timeout=20).strip())
assert count == 0, 'An owned supervisor/recorder process still needs review'
for name in ('P06-enrollment-status.json', 'P06-supervision-status.json', 'P06-sharing-trace.json', 'P06-publication-stacks.json', 'P06-storage-receipts.json'):
    (root / name).write_bytes((n / name).read_bytes())
(root / 'P05-core-crashes-status.json').write_bytes((c / 'P05-core-crashes-status.json').read_bytes())

logs = []
for name in ('P06-enrollment.private.error', 'P06-supervisor.private.log', 'P06-authority-reply-path.private.log'):
    path = n / name
    raw = path.read_bytes()
    logs.append({'name': name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    if name == 'P06-authority-reply-path.private.log':
        (root / 'P06-authority-reply-path.redacted.log').write_text(redact_text(raw.decode(errors='replace'), cwd=h) + '\n')

archive = root / 'P05-core-crash-evidence.zip'
with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as z:
    for path in (e / 'campaign.json', *sorted((e / 'hosts').glob('*.json')), *sorted((e / 'observations/P05').glob('*.json'))):
        z.writestr(path.relative_to(e).as_posix(), path.read_bytes())
progress = {scenario: scenario_status(e, scenario, expected_commit=sha) for scenario in ('P01', 'P03', 'P04', 'P05', 'P06')}
report = {
    'candidate': sha,
    'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'classification': 'unresolved but non-demonstrated candidate defect',
    'observed_failure': 'The supported Windows native supervisor could not establish required sharing readiness before the unchanged 45-second child startup deadline. No planned child-crash assertion began.',
    'demonstrated_path': [
        'Signed enrollment succeeded and saved membership was reused without another grant.',
        'Publication coroutine ran on a responsive relay loop and advanced through announcement and authority selection.',
        'At 5, 20 and 40 seconds it awaited RecorderFederationDeliveryWorker.run_cycle -> DurableRecorderDeliveryQueue.run_once -> RelayRecorderStorageClient.ingest_batch -> RelayNodeClient.request_message_response.',
        'The creator log identifies a nested TimeoutError from PhaseDLogicalStorageClient.ingest -> RelayStorageEndpoint.request while awaiting the assigned local provider response.',
        'Read-only receipts show acknowledgement policy primary, no assigned replicas, four prepared intents, zero committed items and 928 pending native outbox rows. No replica quorum was required or weakened.',
    ],
    'limits': 'The captures establish the current failure path, not the cause of the missing provider response. They do not demonstrate a deterministic product defect or justify a product patch, deadline change, data reset or another generic startup retry.',
    'release_effect': 'P06 remains incomplete; this disposition does not waive physical acceptance. Existing exact-source qualification and completed physical assertions remain evidence for C.',
    'next_action': 'Inspect the exact-source shared local-provider request/reply composition or obtain one focused reproducer tied to these stack frames before any further startup attempt. Resolve isolated storage and real-source/aged-corpus prerequisites independently.',
    'core_crash_archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
    'progress': progress,
    'private_log_digests': logs,
    'P04_assertions': '2/6 PASS',
    'P05_assertions': '8/16 PASS',
    'P06_pass_assertions_added': 0,
    'active_executor': None,
    'native_supervisor_and_child_absent': True,
    'product_changed': False,
    'policy_overrides': False,
    'protected_data_accessed': False,
    'AQG_requested': False,
    'P07': 'NOT STARTED',
    'P12': 'NOT STARTED',
    'physical_pass': False,
}
(root / 'P06-startup-disposition.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({key: report[key] for key in ('classification', 'P04_assertions', 'P05_assertions', 'P06_pass_assertions_added', 'active_executor', 'core_crash_archive_sha256')}))
