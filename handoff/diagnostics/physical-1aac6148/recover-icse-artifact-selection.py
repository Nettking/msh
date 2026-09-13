"""Archive-retained superseded CI copy removal, then publication-only recovery."""
import datetime
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import urllib.request

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

repo = pathlib.Path('C:/wsl/fcp-v1-diagnostic-sweep-20260911')
root = pathlib.Path(__file__).parent
receipt = root / 'storage-repair-icse-selection-recovery.json'
assert not receipt.exists(), 'Inspect original operation receipt; do not repeat'
api = client()
archive_commit = subprocess.check_output(['git', 'rev-parse', '89911820^{commit}'], cwd=repo, text=True).strip()
assert api('/git/ref/heads/codex/federation-v1-diagnostic-sweep-20260911')['object']['sha'] == archive_commit
archive_rel = 'handoff/diagnostics/physical-1aac6148/icse-artifact-10324271911-original-failure.zip'
archived = subprocess.check_output(['git', 'show', archive_commit + ':' + archive_rel], cwd=repo)
expected = 'de8a06ddce3ffc49fe6f8a1fc75d0fbc6d2d107c8b883d4659c194ac03cb4d62'
assert hashlib.sha256(archived).hexdigest() == expected
assert api('/pulls/487')['head']['sha'] == '76ad339f1631e136bba7a8a85973bddb1650d570'
run = api('/actions/runs/34777368640')
assert run['run_attempt'] == 2 and run['status'] == 'completed' and run['conclusion'] == 'failure'
jobs = api('/actions/runs/34777368640/jobs?filter=latest&per_page=100')['jobs']
assert {j['id'] for j in jobs if j['conclusion'] != 'success'} == {103780468967}
old = api('/actions/artifacts/10324271911')
new = api('/actions/artifacts/10323944077')
assert old['name'] == new['name'] == 'icse-network-summary-Linux'
assert old['digest'] == 'sha256:' + expected
assert old['created_at'] < new['created_at'] and old['id'] > new['id']
assert new['digest'] == 'sha256:d2851454a5efab2347f497775ca853e03424ebdc5d9546ac9d4426794af60de0'
proof = json.loads((root / 'storage-repair-icse-publication-disposition.json').read_text())
assert proof['unmodified_bundler_exit'] == 0 and proof['all_source_files_equal_exact_git_archive']
out = {
    'requested_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'source': run['head_sha'], 'native_source': proof['source'], 'run_id': run['id'],
    'original_artifact_id': old['id'], 'original_artifact_sha256': expected,
    'immutable_archive_commit': archive_commit,
    'immutable_archive': f'https://github.com/Nettking/msh/blob/{archive_commit}/{archive_rel}',
    'passing_artifact_id': new['id'],
    'scope': 'Remove only superseded CI artifact copy after exact bytes are committed and pushed. Original failed logs/verdict and immutable archived artifact retained. Rerun publication only; no demonstrations or source changes.',
    'status': 'remove_superseded_copy_requesting',
}

def save():
    receipt.write_text(json.dumps(out, indent=2) + '\n')

save()
credential = subprocess.run(['git', 'credential', 'fill'], input='protocol=https\nhost=github.com\n\n',
    capture_output=True, text=True, timeout=15,
    env=dict(os.environ, GIT_TERMINAL_PROMPT='0', GCM_INTERACTIVE='never'))
token = dict(line.split('=', 1) for line in credential.stdout.splitlines() if '=' in line)['password']
request = urllib.request.Request('https://api.github.com/repos/Nettking/msh/actions/artifacts/10324271911',
    method='DELETE', headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json'})
with urllib.request.urlopen(request, timeout=25) as response:
    out['delete_http_status'] = response.status
assert out['delete_http_status'] == 204
out['status'] = 'superseded_copy_removed'
save()
remaining = api('/actions/runs/34777368640/artifacts?per_page=100')['artifacts']
assert not any(a['id'] == old['id'] for a in remaining)
linux = [a for a in remaining if a['name'] == 'icse-network-summary-Linux']
assert len(linux) == 1 and linux[0]['id'] == new['id']
out['status'] = 'publication_recovery_requesting'
save()
out['recovery_response'] = api('/actions/jobs/103780468967/rerun', {})
out['status'] = 'publication_recovery_accepted'
save()
print(json.dumps({'status': out['status'], 'exact_failed_evidence_preserved_at': archive_commit,
                  'deleted_superseded_ci_copy': old['id'], 'tests_rerun': 0}))
