"""Preserve reviewed dependency provenance of the completed synthetic release run."""
import datetime
import hashlib
import json
import pathlib
import subprocess

root = pathlib.Path(__file__).resolve().parent
repo = pathlib.Path('C:/wsl/fcp-responder-exit-wait-20260911')
audit = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
head = '2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee'
synthetic = '2cc004b3529ab3b0d99e10e1cdffbe1391f27231'
workflow = '.github/workflows/federation-v1-release.yml'
def blob(sha):
    return subprocess.check_output(['git', 'show', sha + ':' + workflow], cwd=repo)
raw = blob(head)
assert raw == blob(synthetic), 'Review actual synthetic workflow separately'
text = raw.decode()
order = text.split('  suite-order-independence:\n')[1].split('  release-verdict:\n')[0]
verdict = text.split('  release-verdict:\n')[1]
assert 'needs: [suite-order-runs]' in order
assert 'needs: [release-matrix, postgres-storage, suite-order-independence]' in verdict
assert 'actions/checkout' not in order + verdict
native = json.loads((root / 'pr463-native-provenance.json').read_text())
records = [r for r in native['records'] if r.get('run_id') == 34664467237]
assert len(records) == 16 and all(r['conclusion'] == 'success' for r in records)
byname = {r['name']: r for r in records}
definitions = {
    'Clean-checkout suite order independence': ['Full-suite order (fixed)', 'Full-suite order (rotating)'],
    'Federation v1 automated release verdict': ['Release matrix (Linux)', 'Release matrix (Windows)',
        'PostgreSQL storage release check', 'Clean-checkout suite order independence'],
}
review = []
for name, deps in definitions.items():
    record = byname[name]
    assert record['checkout_matches'] is None and record['checkout_commits'] == []
    log = (audit / 'pr463-head-2c1a8d93-delta-native-logs' / (str(record['job_id']) + '.log')).read_bytes()
    assert hashlib.sha256(log).hexdigest() == record['sha256']
    if name.startswith('Clean-checkout'):
        assert b'RESULT: success' in log
    else:
        assert log.count(b'test "success" = "success"') == 4  # group title plus three actual checks
    review.append(dict(job_id=record['job_id'], name=name, sha256=record['sha256'],
                       dependency_job_ids=[byname[d]['job_id'] for d in deps],
                       classification='dependency aggregate; no checkout by contract',
                       source_disposition='Dependencies ultimately used synthetic SHA; not exact-head qualification'))
sources = [r for r in records if r['name'] not in definitions]
assert len(sources) == 14
assert all(r['checkout_commits'] == [synthetic] and r['checkout_matches'] is False for r in sources)
result = dict(reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    source_commit=head, run_id=34664467237, synthetic_commit=synthetic,
    workflow_path=workflow, workflow_sha256=hashlib.sha256(raw).hexdigest(),
    workflow_identical_on_head_and_synthetic=True, source_jobs=14, aggregates=review,
    decision='Exact-head release dispatch justified; retain all existing native evidence')
(root / 'pr463-release-aggregate-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))



