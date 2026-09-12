"""Review retained source-bound retry evidence, keeping original failures open."""
import datetime
import hashlib
import json
import pathlib
import shutil
import sys
import xml.etree.ElementTree as ET
import zipfile

root = pathlib.Path('handoff'); dest = root / 'diagnostics'
private = pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
sys.path.insert(0, str(private))
from github_qualification import client
api = client()
native = json.loads((private / 'D11-D12-retry-pass-native-retention.json').read_text())
artifacts = json.loads((private / 'D11-D12-retry-pass-raw-artifact-retention.json').read_text())
head = 'ba44100ec1e4cde19daba0d3723b991c11742316'
checkout = 'f18e91adae2cb198fb5cebad61ee202a38d2bd8e'
logs = {}
for r in native['records']:
    raw = (private / 'D11-D12-retry-pass-native-logs' / (str(r['job_id']) + '.log')).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == r['sha256']
    assert r['conclusion'] == 'success'
    assert r['checkout_matches'] or r['job_id'] == 103567449153
    logs[r['job_id']] = raw.decode('utf-8')
assert '1102 passed, 19 skipped' in logs[103566982282]
assert 'go build -trimpath -o ../../build/fcp-peer-sidecar.exe .' in logs[103566982639]
assert '381 passed, 1 skipped' in logs[103566982639]
assert 'error obtaining VCS status' not in logs[103566982639]
artifact = next(a for a in artifacts['records'] if a['name'] == 'linux-regression-shard-2')
zpath = private / 'D11-D12-retry-pass-raw-artifacts' / (str(artifact['id']) + '.zip')
assert 'sha256:' + hashlib.sha256(zpath.read_bytes()).hexdigest() == artifact['digest']
with zipfile.ZipFile(zpath) as z:
    manifest = json.loads(z.read('shard-2.json'))
    assert manifest['source_sha'] == checkout and manifest['exit_code'] == 0 and not manifest['source_error']
    cases = list(ET.fromstring(z.read('junit-2.xml')).iter('testcase'))
    target = next(c for c in cases if c.get('name') == 'test_live_reinstatement_restores_replica_and_acknowledgement_policy')
    assert target.find('failure') is None and target.find('error') is None and target.find('skipped') is None
shutil.copyfile(zpath, dest / 'D11-successful-retry-shard-2.zip')
excerpt = '\n'.join(line for job, log in logs.items() for line in log.splitlines() if any(t in line for t in ['go test ./...', 'go build -trimpath', 'go version go1.25.7', '381 passed', '1102 passed', 'test_live_reinstatement_restores_replica_and_acknowledgement_policy', 'verified', 'Verified', 'passed coverage']))
(dest / 'D11-D12-successful-retry-excerpt.log').write_text(excerpt + '\n', encoding='utf-8')
pr = api('/pulls/468'); assert pr['head']['sha'] == head
files = api('/pulls/468/files?per_page=100')
assert {f['filename'] for f in files} == {'.github/workflows/phase-f7-closeout.yml', 'catalog/common/tests/test_f7_ci_coverage.py', 'docs/implementation/f7_ci_consolidation.md', 'docs/implementation/federation_v1_cleanup_manifest.md'}
receipt = {
    'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'status': 'TARGETED_RETRY_NATIVE_REVIEW_PASS_ORIGINAL_CAUSES_OPEN',
    'head_sha': head, 'actual_checkout': checkout,
    'native_retention': native, 'artifacts': artifacts,
    'D11': {'runner': 'Nettking-Linux', 'job': 103566982282, 'target_seconds': float(target.get('time')), 'test_result': 'PASS', 'shard_result': '1102 passed,19 skipped,3365 deselected', 'manifest': {k:manifest[k] for k in ['source_sha','source_identity_before','source_identity_after','source_error','exit_code','collection_complete']}, 'durable_zip': 'D11-successful-retry-shard-2.zip', 'original_failure': 'D11-original-shard-2.zip', 'classification': 'UNRESOLVED intermittent timing/host/test/runtime cause; no repair established'},
    'D12': {'runner': 'Nettking', 'job': 103566982639, 'go_build': 'PASS with unchanged default VCS stamping', 'python_result': '381 passed,1 skipped', 'classification': 'Git/VCS introspection execution failure on original Beast host; underlying host/toolchain cause unresolved, no product test failure observed'},
    'pr_metadata': {k:pr.get(k) for k in ['number','state','draft','mergeable','mergeable_state','merge_commit_sha','updated_at']},
    'pr_head': pr['head']['sha'], 'pr_base': pr['base']['sha'],
    'current_main': api('/git/ref/heads/main')['object']['sha'],
    'changed_files': [{k:f.get(k) for k in ['filename','status','additions','deletions']} for f in files],
    'reviews': api('/pulls/468/reviews?per_page=100'),
    'disposition': 'Required final-head checks now pass under the unchanged checked-in retry contract. Original failure issues stay open: these successes do not establish host root causes or repair product behavior. CI coverage-only extension can be assessed independently; no new physical candidate or physical PASS is selected from this evidence.',
    'protected_recorder_data': 'UNTOUCHED', 'physical_runtime_changed': False,
}
(dest / 'D11-D12-reviewed-retry-pass.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
for finding in ['D11','D12']:
    p = dest / (finding + '.md'); s = p.read_text(encoding='utf-8')
    a = s.index('**Next diagnostic action:**'); b = s.index('\n\n', a)
    s = s[:a] + '**Next diagnostic action:** The single retry passed on Nettking; see [reviewed evidence](D11-D12-reviewed-retry-pass.json). Keep this issue open for original-host root-cause investigation using read-only timing/Git evidence when that host is safely accessible. Do not rerun passing jobs or infer a product repair from the retry.' + s[b:]
    s += '\n## 2026-09-12T15:02Z — reviewed retry passed, original cause remains open\n\n[Native review](D11-D12-reviewed-retry-pass.json) binds the passing execution to unchanged ba44100e/f18e91ad source. The job ran on Nettking rather than the original failing host. No deadlines, checks, VCS stamping, source or runner configuration changed. Original failures remain separate durable evidence.\n'
    p.write_text(s, encoding='utf-8')
p = root / 'FEDERATION_V1_DIAGNOSTIC_SWEEP.md'; s = p.read_text(encoding='utf-8')
s = s.replace('One exact native failure; single unchanged-source retry running on Nettking, original host cause unresolved', 'One exact native failure; single unchanged-source retry PASS on Nettking, original host cause unresolved')
p.write_text(s, encoding='utf-8')
p = root / 'QUALIFICATION_COORDINATION.md'; s = p.read_text(encoding='utf-8')
s += '\n## ' + receipt['recorded_at'] + ' — retry native evidence reviewed\n\nD11 test PASS2.20s, shard1102PASS/19skips; clean manifest has unchanged f18e91ad source before/after. D12 default-stamped Go build PASS then381PythonPASS/1skip. Actual checkout verified for both and both release matrix verifiers; verdict is dependency-only. Original issues471/472 remain open, not repaired. Required final-head checks are green under the existing failed-job retry contract. Next finish the independent CI-extension merge/reference review; keep host causes distinct from migration and physical evidence. Receipt: diagnostics/D11-D12-reviewed-retry-pass.json.\n'
p.write_text(s, encoding='utf-8')
print(json.dumps({k:receipt[k] for k in ['status','pr_metadata','pr_head','pr_base','current_main']}))
