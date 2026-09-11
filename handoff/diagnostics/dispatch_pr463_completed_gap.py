"""Dispatch one proven completed synthetic-source gap; no valid job is rerun."""
import datetime
import json
import pathlib
import sys

audit = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sys.path.insert(0, str(audit))
from github_qualification import client

root = pathlib.Path(__file__).resolve().parent
sha = '83955f65b7e6bb36de8e90f34608b97070fed33b'
ref = 'codex/responder-replacement-exit-wait'
allowed = {'phase2-federation.yml', 'federation-v1-release.yml', 'ci-test-sharding.yml',
           'icse-tool-demo.yml', 'product-branding.yml', 'cf7b-product-physical-acceptance.yml',
           'federation-software-update.yml'}
workflow = sys.argv[1]
assert workflow in allowed
path = root / 'pr463-gap-dispatch-ledger.json'
ledger = json.loads(path.read_text())
native = json.loads((root / 'pr463-native-provenance.json').read_text())
assert native['source_commit'] == sha
api = client()
pr = api('/pulls/463')
assert pr['head']['sha'] == sha and pr['state'] == 'open' and not pr['merged']
assert api('/git/ref/heads/' + ref)['object']['sha'] == sha
runs = api('/actions/runs?head_sha=' + sha + '&per_page=100')['workflow_runs']
matching = [r for r in runs if r['path'].split('/')[-1] == workflow]
if any(r['event'] == 'workflow_dispatch' for r in matching) or any(
        r['workflow'] == workflow and r['sha'] == sha for r in ledger['dispatches']):
    raise SystemExit('Existing dispatch or ledger entry: no duplicate')
assert matching and all(r['status'] == 'completed' and r['conclusion'] == 'success'
                        for r in matching), 'Wait for completion or classify failure first'
proof = []
aggregate_review = None
if workflow == 'federation-v1-release.yml':
    aggregate_review = json.loads((root / 'pr463-release-aggregate-review.json').read_text())
    assert aggregate_review['source_commit'] == sha
    assert aggregate_review['workflow_identical_on_head_and_synthetic'] is True
    assert aggregate_review['source_jobs'] == 14
for run in matching:
    jobs = api('/actions/runs/' + str(run['id']) + '/jobs?per_page=100')['jobs']
    assert jobs
    for job in jobs:
        record = next((r for r in native['records'] if r['job_id'] == job['id']), None)
        assert record, 'Preserve unverified work'
        if record.get('checkout_matches') is None:
            assert aggregate_review and aggregate_review['run_id'] == run['id']
            reviewed = next((r for r in aggregate_review['aggregates'] if r['job_id'] == job['id']), None)
            assert reviewed and reviewed['name'] == record['name'] and reviewed['sha256'] == record['sha256']
            assert record['checkout_commits'] == []
            proof.append(dict(job_id=job['id'], run_id=run['id'], sha256=record['sha256'],
                              aggregate_review='pr463-release-aggregate-review.json',
                              dependency_job_ids=reviewed['dependency_job_ids']))
            continue
        assert record.get('checkout_matches') is False, 'Preserve valid work'
        assert record['conclusion'] == 'success'
        proof.append({k: record[k] for k in ['job_id', 'run_id', 'checkout_commits', 'sha256']})
assert api('/git/ref/heads/' + ref)['object']['sha'] == sha
row = dict(pr=463, sha=sha, ref=ref, workflow=workflow,
           requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
           reason='Completed successful PR jobs checked out synthetic SHA, not intended head',
           native_proof=proof, status='REQUESTING')
ledger['dispatches'].append(row)
path.write_text(json.dumps(ledger, indent=2) + '\n', encoding='utf-8')
row['response'] = api('/actions/workflows/' + workflow + '/dispatches', {'ref': ref})
row['status'] = 'DISPATCHED'
path.write_text(json.dumps(ledger, indent=2) + '\n', encoding='utf-8')
print(json.dumps(row))
