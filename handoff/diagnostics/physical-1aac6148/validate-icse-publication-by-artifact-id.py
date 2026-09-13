"""Validate current-source packaging using immutable, audited passing artifacts.

This is a local qualification proof, never a canonical tag/release archive.
It does not rewrite artifacts, rerun demonstrations, or change a CI verdict.
"""
import datetime
import hashlib
import json
import pathlib
import subprocess
import zipfile

source = 'ec0bbd1d5ce45a97ddc10058bad38eea90f044b6'
repo = pathlib.Path('C:/wsl/fcp-v1-pr487-merge-proof-20260913')
private = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/pr487-qualification')
root = pathlib.Path(__file__).parent
audit = json.loads((root / 'storage-reply-repair-pr487-audit.json').read_text())
stage = private / 'publication-by-explicit-id'
assert not stage.exists(), 'Inspect retained bounded packaging proof'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo, text=True).strip()
selected = {
    'icse-summary-Linux': 10323708214,
    'icse-summary-Windows': 10323249153,
    'icse-summary-compose': 10324203251,
    'icse-network-summary-Linux': 10323944077,
    'icse-network-summary-Windows': 10324068207,
}
stage.mkdir()
proofs = []
for name, artifact_id in selected.items():
    proof = next(p for p in audit['artifacts'] if p['id'] == artifact_id)
    archive = private / (name + '.zip')
    assert proof['name'] == name and proof['digest_verified'] is True
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == proof['sha256']
    network = name.startswith('icse-network-')
    destination = stage / ('network' if network else 'components') / name
    destination.mkdir(parents=True)
    with zipfile.ZipFile(archive) as z:
        expected = {'summary.json', 'events.jsonl', 'operator-report.html'} if network else {'icse-summary.json'}
        assert set(z.namelist()) == expected
        for member in expected:
            (destination / member).write_bytes(z.read(member))
    proofs.append({'id': artifact_id, 'name': name, 'sha256': proof['sha256'], 'created_at': proof['created_at']})
python = 'C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
command = [python, '-m', 'demo.icse.bundle', '--source-revision', source,
           '--source-ref', 'refs/pull/487/merge', '--workflow-run', '34777368640',
           '--version', 'candidate', '--evidence-root', str(stage / 'components'),
           '--network-evidence-root', str(stage / 'network'), '--output', str(stage / 'bundle')]
run = subprocess.run(command, cwd=repo, capture_output=True, text=True, timeout=120)
(stage / 'build.stdout').write_text(run.stdout)
(stage / 'build.stderr').write_text(run.stderr)
assert run.returncode == 0, 'Retain the original bundler refusal'
prefix = 'fcp-icse-tool-demo-candidate/source/'
expected_source = stage / 'expected-source.zip'
subprocess.run(['git', 'archive', '--format=zip', '--prefix=' + prefix, '-o', str(expected_source), source], cwd=repo, check=True)
bundle = stage / 'bundle/fcp-icse-tool-demo-candidate.zip'
with zipfile.ZipFile(expected_source) as expected, zipfile.ZipFile(bundle) as actual:
    names = {n for n in expected.namelist() if not n.endswith('/')}
    assert names == {n for n in actual.namelist() if n.startswith(prefix) and not n.endswith('/')}
    for name in names:
        assert hashlib.sha256(expected.read(name)).digest() == hashlib.sha256(actual.read(name)).digest()
manifest = json.loads((stage / 'bundle/artifact-manifest.json').read_text())
assert manifest['source_revision'] == source
assert {r['execution'] for r in manifest['evidence']} == {'Linux', 'Windows', 'compose'}
assert {r['execution'] for r in manifest['network_evidence']} == {'Linux', 'Windows'}
out = {
    'validated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'source': source, 'branch': audit['branch_sha'], 'tree': audit['equal_tree'],
    'classification': 'deterministic CI artifact-selection defect, not a product defect',
    'cause': 'Pinned download-artifact d3f86a106a0bac45b974a628896c90dbdf5c8093 filterLatest sorts descending numeric artifact ID. Earlier failed Linux artifact10324271911 has a larger ID than later passing10323944077, so the publication job selects the failed first attempt.',
    'first_failed_artifact': {'id': 10324271911, 'created_at': '2026-09-13T19:21:54Z', 'sha256': 'de8a06ddce3ffc49fe6f8a1fc75d0fbc6d2d107c8b883d4659c194ac03cb4d62'},
    'pinned_action_dist_sha256': '02c2cf24c5b60ae4b3f8d71d3cc85e2e6433dbdf106a38fee7ec3f1c96e2d18e',
    'failed_publication_job': 103780468967,
    'failed_publication_log_sha256': 'f632595807a91bd93b29bf149549818a4a5b7647e06b93a4c25885181ba86dc9',
    'artifact_inputs': proofs, 'unmodified_bundler_exit': run.returncode,
    'all_source_files_equal_exact_git_archive': True, 'source_file_count': len(names),
    'local_bundle_sha256': hashlib.sha256(bundle.read_bytes()).hexdigest(),
    'local_bundle_bytes': bundle.stat().st_size,
    'ci_publication_verdict': 'FAIL_RETAINED', 'physical_acceptance': 'NOT_EVALUATED',
    'canonical_publication_archive': False, 'remote_artifacts_deleted_or_changed': False,
    'scope': 'Exact-source local packaging proof only. Tag-triggered canonical paper artifact still requires its own successful CI publication per demo/icse/RELEASE.md. No new demonstration or CI cleanup source change.',
}
(root / 'storage-repair-icse-publication-disposition.json').write_text(json.dumps(out, indent=2) + '\n')
(root / 'storage-repair-icse-local-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'unmodified_bundler': 'PASS', 'source_files_verified': len(names), 'canonical_archive': False, 'ci_failure_retained': True}))
