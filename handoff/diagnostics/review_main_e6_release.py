"""Retain the completed actual-main release evidence; never dispatch tests."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'main-e6a9b74a-qualification'
RELEASE = OUT / 'release'
CHECKOUT = Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913')
SHA = 'e6a9b74a1d555609eed6bf40c800e1258f1c9077'
TREE = 'cc9b29515d47d754f24199bf403213e7ff111315'
sys.path.insert(0, str(CHECKOUT))
from scripts.ci_pytest_shards import verify

snapshot = json.loads((ROOT / 'main-e6a9b74a-qualification-latest.json').read_text())
runs = snapshot['runs']
assert len(runs) == 13
assert all(r['head_sha'] == SHA and r['conclusion'] == 'success' for r in runs)
assert all(len(r['jobs']) == r['expected_jobs'] and all(j['conclusion'] == 'success' for j in r['jobs']) for r in runs)
assert sum(len(r['jobs']) for r in runs) == 40
run = next(r for r in runs if r['id'] == 34751832493)
native = json.loads((RELEASE / 'native-review.json').read_text())
assert {r['id'] for r in native} == {j['id'] for j in run['jobs']}
assert sum(bool(r['source_log']) for r in native) == 14
assert all(r['source'] == SHA for r in native)
archive = RELEASE / 'native-job-logs.zip'
logs = list(RELEASE.glob('native-job-*.log'))
if logs:
    assert len(logs) == 16
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for p in logs:
            z.write(p, p.name)
    with zipfile.ZipFile(archive) as z:
        assert all(z.read(p.name) == p.read_bytes() for p in logs)
with zipfile.ZipFile(archive) as z:
    assert len(z.namelist()) == 16
report = []
with tempfile.TemporaryDirectory(prefix='fcp-e6-reviewed-shards-') as tmp:
    shard_dir = Path(tmp)
    for a in run['artifacts']:
        p = RELEASE / (str(a['id']) + '.zip')
        assert p.stat().st_size == a['size_in_bytes']
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        assert 'sha256:' + digest == a['digest']
        assert a['workflow_run']['head_sha'] == SHA
        row = {'id': a['id'], 'name': a['name'], 'sha256': digest, 'entries': []}
        with zipfile.ZipFile(p) as z:
            infos = z.infolist()
            assert len(infos) < 10000 and sum(i.file_size for i in infos) < 200_000_000
            assert len({i.filename.casefold() for i in infos}) == len(infos)
            for i in infos:
                n = i.filename
                assert not PurePosixPath(n).is_absolute() and '..' not in PurePosixPath(n).parts
                assert '\\' not in n and ':' not in n and not stat.S_ISLNK(i.external_attr >> 16)
                data = z.read(i)
                entry = {'name': n, 'sha256': hashlib.sha256(data).hexdigest()}
                if n.endswith('.xml'):
                    root = ET.fromstring(data)
                    suites = list(root.iter('testsuite'))
                    assert suites and all(int(s.get('failures', '0')) == int(s.get('errors', '0')) == 0 for s in suites)
                    assert not list(root.iter('failure')) and not list(root.iter('error'))
                    entry['tests'] = sum(int(s.get('tests', '0')) for s in suites)
                    entry['skipped'] = sum(int(s.get('skipped', '0')) for s in suites)
                    assert len(list(root.iter('testcase'))) == entry['tests']
                elif n.endswith('.json'):
                    manifest = json.loads(data)
                    assert manifest['source_sha'] == SHA
                    assert manifest['source_identity_before'] == manifest['source_identity_after'] == {'sha': SHA, 'tree': TREE}
                    assert PurePosixPath(n).name == n
                    (shard_dir / n).write_bytes(data)
                else:
                    raise AssertionError('unexpected artifact entry')
                row['entries'].append(entry)
        report.append(row)
    covered = verify(shard_dir, 4, SHA)
    assert covered == 4503
review = {'source': SHA, 'run_id': run['id'], 'status': 'PASS', 'artifact_count': len(report), 'disjoint_complete_linux_test_coverage': covered, 'native_checkout_proofs': 14, 'aggregate_jobs_without_checkout': 2, 'artifacts': report}
(RELEASE / 'artifact-review.json').write_text(json.dumps(review, indent=2) + '\n')
def git(*args):
    return subprocess.check_output(['git', *args], cwd=CHECKOUT, text=True).strip()
assert git('rev-parse', 'HEAD') == SHA and git('rev-parse', 'HEAD^{tree}') == TREE
assert not git('status', '--porcelain')
sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api = client()
live_main = api('/git/ref/heads/main')['object']['sha']
assert live_main == SHA
qualification = {'at': datetime.now(timezone.utc).isoformat(), 'source': SHA, 'tree': TREE, 'live_main': live_main, 'status': 'ACTUAL_MAIN_AUTOMATED_QUALIFIED', 'required_jobs_passed': 37, 'companion_jobs_passed': 3, 'native_checkout_proofs': 38, 'aggregate_jobs_without_checkout': 2, 'source_checkout_clean': True, 'workflows': [{'id': r['id'], 'path': r['path'], 'jobs_passed': len(r['jobs']), 'url': 'https://github.com/Nettking/msh/actions/runs/' + str(r['id'])} for r in runs], 'evidence': ['short-qualification.json', 'native-review.json', 'native-job-logs.zip', 'artifact-review.json', 'release/native-review.json', 'release/native-job-logs.zip', 'release/artifact-review.json'], 'physical_acceptance': 'NOT_PASSED', 'P07': 'NOT_STARTED', 'P12': 'NOT_STARTED', 'protected_recorder_data': 'UNTOUCHED'}
(OUT / 'qualification.json').write_text(json.dumps(qualification, indent=2) + '\n')
print(json.dumps({k:v for k,v in qualification.items() if k not in ['workflows','evidence']}, indent=2))
print('Verified nine artifact digests, zero JUnit failures/errors, 4503 disjoint Linux tests; retained sixteen native logs.')
