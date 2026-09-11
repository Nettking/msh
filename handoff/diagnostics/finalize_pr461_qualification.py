"""Reconcile retained exact-head qualification; no tests, dispatches or host actions."""
from collections import Counter
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parent
A = Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
REPO = Path('C:/wsl/fcp-tailnet-responder-port-20260911')
SHA = '5b826c6806ab1bdb960412ba20ca78192971fb1d'
sys.path.insert(0, str(A))
from github_qualification import REQUIRED

def read(name):
    return json.loads((ROOT / name).read_text())

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

state = read('qualification-state.json')['prs']['461']
native = read('qualification-native-provenance.json')['prs']['461']
assert state['head'] == native['source_commit'] == SHA
assert not state['missing_workflows']
proofs = {r['job_id']: r for r in native['records']}
workflows = {w['workflow']: w for w in state['workflows']}
aggregates = {'Clean-checkout suite order independence',
              'Federation v1 automated release verdict'}
required = dict(REQUIRED, **{'cfi2-onboarding-composition.yml': 2,
                          'release-image-metadata.yml': 1})
rows = []
for name, count in required.items():
    w = workflows[name]
    assert w['event'] == 'workflow_dispatch'
    assert w['api_head_sha'] == SHA and w['conclusion'] == 'success'
    assert len(w['jobs']) == count
    for j in w['jobs']:
        r = proofs[j['id']]
        assert j['status'] == 'completed' and j['conclusion'] == r['conclusion'] == 'success'
        assert r['run_id'] == w['run_id'] and r['name'] == j['name']
        if j['name'] in aggregates:
            assert name == 'federation-v1-release.yml' and r['checkout_commits'] == []
        else:
            assert r['checkout_matches'] is True and r['checkout_commits'] == [SHA]
    rows.append(dict(workflow=name, run_id=w['run_id'], jobs=count, result='PASS'))
assert sum(REQUIRED.values()) == 37

release = workflows['federation-v1-release.yml']
jobs = {j['name']: j for j in release['jobs']}
def log(name):
    jid = jobs[name]['id']
    paths = list(A.glob(f'pr461-head-5b826c68*-native-logs/{jid}.log'))
    assert paths, (name, jid)
    path = next(p for p in paths if digest(p) == proofs[jid]['sha256'])
    return re.sub(r'\x1b\[[0-9;]*m', '', path.read_text(encoding='utf-8'))

workflow = subprocess.check_output(
    ['git', 'show', SHA + ':.github/workflows/federation-v1-release.yml'],
    cwd=REPO, text=True)
def block(name):
    found = re.findall(r'^  ' + re.escape(name) + r':\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)',
                       workflow, re.M | re.S)
    assert len(found) == 1
    return found[0]

assert 'needs: [suite-order-runs]' in block('suite-order-independence')
assert 'run: test "$RESULT" = success' in block('suite-order-independence')
assert 'RESULT: success' in log('Clean-checkout suite order independence')
assert 'needs: [release-checks, linux-regressions, windows-regressions]' in block('release-matrix')
for name in ['Release matrix (Linux)', 'Release matrix (Windows)']:
    output = log(name)
    for marker in ['CHECKS: success', 'LINUX: success', 'WINDOWS: success',
                   'Verified 4368 tests across 4 disjoint successful shards']:
        assert marker in output, (name, marker)
assert 'needs: [release-matrix, postgres-storage, suite-order-independence]' in block('release-verdict')
for dependency in ['release-matrix', 'postgres-storage', 'suite-order-independence']:
    assert 'test "${{ needs.' + dependency + '.result }}" = "success"' in block('release-verdict')
assert sum(line.partition(' ')[2] == 'test "success" = "success"'
           for line in log('Federation v1 automated release verdict').splitlines()) == 3

artifacts = read('pr461-final-release-artifact-review.json')
assert artifacts['source_commit'] == SHA and artifacts['run_id'] == release['run_id']
assert artifacts['complete_artifact_set'] and not artifacts['missing_artifacts']
assert artifacts['test_nodeids_covered'] == 4368 and len(artifacts['records']) == 9
manifest = json.loads((A / 'pr461-head-5b826c68-release-final-release-artifact-review/shards/shard-0.json').read_text())
def identity(nodeid):
    filename, sep, qualified = nodeid.partition('::')
    assert sep and filename.endswith('.py')
    base, paramsep, params = qualified.partition('[')
    names = base.split('::')
    return '.'.join([filename[:-3].replace('/', '.'), *names[:-1]]), names[-1] + (paramsep + params if paramsep else '')
expected = Counter(identity(n) for n in manifest['collected'])
assert manifest['source_sha'] == SHA and sum(expected.values()) == 4368
passing = set()
skipped = {}
for row in artifacts['records']:
    assert row['source_checkout_verified'] and row['failures'] == row['errors'] == 0
    path = A / f"pr461-head-5b826c68-release-final-raw-artifacts/{row['artifact_id']}.zip"
    with zipfile.ZipFile(path) as archive:
        xmls = [n for n in archive.namelist() if n.endswith('.xml')]
        assert len(xmls) == 1
        cases = list(ET.fromstring(archive.read(xmls[0])).iter('testcase'))
    if row['name'].startswith('full-suite-order-'):
        compose_cases=[c for c in cases if c.get('classname')=='catalog.flask_app.tests.test_tailnet_join_compose']
        assert len(compose_cases)==5 and all(c.find('skipped') is None and c.find('failure') is None and c.find('error') is None for c in compose_cases)
        assert Counter((c.get('classname'), c.get('name')) for c in cases) == expected
    for c in cases:
        key = (c.get('classname'), c.get('name'))
        skip = c.find('skipped')
        if skip is None:
            passing.add(key)
        else:
            skipped[key] = skip.get('message')

uncovered = [dict(test='.'.join(k), reason=v) for k, v in skipped.items() if k not in passing]
native_skip_map = read('pr461-skip-native-map.json')
native_covered_modules = {
    m for r in native_skip_map['matched']
    if r['summaries'] and all('skipped' not in s and 'deselected' not in s for s in r['summaries'])
    for m in r['modules']
}
platform_exclusions = []
for item in uncovered:
    module = item['test'].rsplit('.test_', 1)[0].replace('.', '/') + '.py'
    if module in native_covered_modules:
        continue
    # These pre-existing cases explicitly exclude POSIX; the required gate
    # contracts do not promise every collected test runs on every platform.
    source = subprocess.check_output(['git', 'show', SHA + ':' + module], cwd=REPO, text=True)
    baseline = subprocess.check_output(
        ['git', 'show', '0536f03d67eb277e11573c2188d8e820399627e3:' + module],
        cwd=REPO, text=True)
    assert source == baseline and 'skipif' in source
    assert item['reason'] in source
    assert 'os.name != "nt"' in source or 'sys.platform != "win32"' in source
    platform_exclusions.append(item)
assert len(platform_exclusions) == 14
icse = read('pr461-icse-artifact-review.json')
assert icse['source_sha'] == SHA and icse['source_export_exact']
assert len(icse['component']) == 3 and all(x['passed'] == 4 for x in icse['component'])
assert all(x['required_checks_passed'] == 10 and x['teardown'] for x in icse['network'])
review = read('pr461-premerge-review-input.json')
assert review['head'] == SHA and not review['comments'] and not review['reviews']
assert review['issue_comments'] == []
changed=subprocess.check_output(['git','diff','--name-only','0536f03d67eb277e11573c2188d8e820399627e3',SHA],cwd=REPO,text=True).splitlines()
assert changed==['catalog/flask_app/tests/test_tailnet_join_compose.py','docker-compose.yml']
compose_delta=subprocess.check_output(['git','diff','0536f03d67eb277e11573c2188d8e820399627e3',SHA,'--','docker-compose.yml'],cwd=REPO,text=True)
assert [line for line in compose_delta.splitlines() if line.startswith('+') and not line.startswith('+++')]==['+      - FCP_AUTO_JOIN_PORT=${FCP_AUTO_JOIN_PORT:-5151}']
assert not [line for line in compose_delta.splitlines() if line.startswith('-') and not line.startswith('---')]
report = dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              source_commit=SHA, required_jobs=37, additional_cfi2_registry_jobs=3,
              workflows=rows, release_native_checkouts=14, source_bound_aggregates=2,
              collected_test_identities=4368, full_orders_exact_test_identities=True,
              skips_with_pass_elsewhere=len(skipped)-len(uncovered),
              skips_covered_by_native_nonartifact_log=len(uncovered)-len(platform_exclusions),
              native_skip_map_sha256=digest(ROOT/'pr461-skip-native-map.json'),
              unchanged_intentional_posix_exclusions=platform_exclusions,
              release_artifact_sha256=digest(ROOT/'pr461-final-release-artifact-review.json'),
              icse_review_sha256=digest(ROOT/'pr461-icse-artifact-review.json'),
              no_reported_correctness_findings=True,
              manual_reviewed_files=changed, compose_regression_cases_per_full_order=5,
              status='QUALIFIED_REQUIRED_PR_HEAD_SCOPE',
              merge_boundary='Hold for complete required fix-set qualification; recheck live head, branch gates and correctness findings immediately before the separate merge.',
              physical_acceptance=False)
(ROOT/'pr461-final-qualification.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report))
