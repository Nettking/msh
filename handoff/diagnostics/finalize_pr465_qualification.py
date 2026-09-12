"""Reconcile exact-head native qualification and artifacts without rerunning tests."""
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
REPO = Path('C:/wsl/fcp-analysis-reader-sharing-20260912')
SHA = '4749ab6689315a7ad953c11ce4b2ec942433d71a'
BASE = '9b286f931497bf6291e215f6340443c5162826b0'
sys.path.insert(0, str(A))
from github_qualification import REQUIRED, client

def read(name): return json.loads((ROOT/name).read_text())
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def git(*args): return subprocess.check_output(['git',*args],cwd=REPO,text=True).strip()
state = read('pr465-qualification-state.json')['prs']['465']
native = read('pr465-native-provenance.json')
assert state['head'] == native['source_commit'] == SHA == git('rev-parse','HEAD')
assert not git('status','--porcelain') and not state['missing_workflows']
proofs = {r['job_id']:r for r in native['records']}
workflows = {w['workflow']:w for w in state['workflows']}
aggregate_names = {'Clean-checkout suite order independence','Federation v1 automated release verdict'}
required = dict(REQUIRED,**{'cfi2-onboarding-composition.yml':2,'release-image-metadata.yml':1})
rows = []
for name,count in required.items():
    w = workflows[name]
    assert w['event']=='workflow_dispatch' and w['api_head_sha']==SHA
    assert w['conclusion']=='success' and len(w['jobs'])==count
    for j in w['jobs']:
        r=proofs[j['id']]
        assert j['status']=='completed' and j['conclusion']==r['conclusion']=='success'
        assert r['run_id']==w['run_id'] and r['name']==j['name']
        path=A/'pr465-head-4749ab66-delta-native-logs'/(str(j['id'])+'.log')
        assert digest(path)==r['sha256']
        if j['name'] in aggregate_names:
            assert name=='federation-v1-release.yml' and r['checkout_commits']==[]
        else:
            assert r['checkout_matches'] is True and r['checkout_commits']==[SHA]
    rows.append(dict(workflow=name,run_id=w['run_id'],jobs=count,result='PASS'))
assert sum(REQUIRED.values())==37
release=workflows['federation-v1-release.yml']
jobs={j['name']:j for j in release['jobs']}
def log(name):
    p=A/'pr465-head-4749ab66-delta-native-logs'/(str(jobs[name]['id'])+'.log')
    return re.sub(r'\x1b\[[0-9;]*m','',p.read_text(encoding='utf-8'))
workflow=git('show',SHA+':.github/workflows/federation-v1-release.yml')
def block(name):
    found=re.findall(r'^  '+re.escape(name)+r':\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)',workflow,re.M|re.S)
    assert len(found)==1
    return found[0]
assert 'needs: [suite-order-runs]' in block('suite-order-independence')
assert 'run: test "$RESULT" = success' in block('suite-order-independence')
assert 'RESULT: success' in log('Clean-checkout suite order independence')
assert 'needs: [release-checks, linux-regressions, windows-regressions]' in block('release-matrix')
for name in ['Release matrix (Linux)','Release matrix (Windows)']:
    for marker in ['CHECKS: success','LINUX: success','WINDOWS: success',
                   'Verified 4426 tests across 4 disjoint successful shards']:
        assert marker in log(name),(name,marker)
assert 'needs: [release-matrix, postgres-storage, suite-order-independence]' in block('release-verdict')
for dep in ['release-matrix','postgres-storage','suite-order-independence']:
    assert 'test "${{ needs.'+dep+'.result }}" = "success"' in block('release-verdict')
assert sum(line.partition(' ')[2]=='test "success" = "success"'
           for line in log('Federation v1 automated release verdict').splitlines())==3
artifacts=read('pr465-final-release-artifact-review.json')
assert artifacts['source_commit']==SHA and artifacts['run_id']==release['run_id']
assert artifacts['complete_artifact_set'] and not artifacts['missing_artifacts']
assert artifacts['test_nodeids_covered']==4426 and len(artifacts['records'])==9
skips=read('pr465-skip-native-map.json')
assert skips['source_sha']==SHA and skips['collected']==4426
native_modules={m for r in skips['matched'] if r['summaries']
                and all('skipped' not in s and 'deselected' not in s for s in r['summaries'])
                for m in r['modules']}
exclusions=[]
for item in skips['uncovered']:
    module=item['test'].rsplit('.test_',1)[0].replace('.','/')+'.py'
    if module in native_modules: continue
    source=git('show',SHA+':'+module)
    assert source==git('show',BASE+':'+module)
    assert 'skipif' in source and item['reason'] in source
    assert 'os.name != "nt"' in source or 'sys.platform != "win32"' in source
    exclusions.append(item)
previous=read('merged-main-final-qualification.json')['unchanged_intentional_posix_exclusions']
assert sorted(exclusions,key=lambda x:x['test'])==sorted(previous,key=lambda x:x['test'])
assert len(exclusions)==11 and len(skips['uncovered'])-len(exclusions)==10
icse=read('pr465-icse-artifact-review.json')
assert icse['source_sha']==SHA and icse['icse_run']==workflows['icse-tool-demo.yml']['run_id']
assert icse['source_export_exact'] and len(icse['component'])==3
assert all(x['passed']==4 for x in icse['component'])
assert len(icse['network'])==2 and all(x['required_checks_passed']==10 and x['teardown'] for x in icse['network'])
changed=git('diff','--name-only',BASE,SHA).splitlines()
assert changed==['catalog/capabilities/analysis/content_store.py',
                 'catalog/capabilities/tests/test_analysis_content_store.py',
                 'catalog/federation/stable_filesystem.py']
regression_rows=[]
for row in artifacts['records']:
    with zipfile.ZipFile(A/'pr465-head-4749ab66-release-final-raw-artifacts'/(str(row['artifact_id'])+'.zip')) as archive:
        xml=next(n for n in archive.namelist() if n.endswith('.xml'))
        cases=list(ET.fromstring(archive.read(xml)).iter('testcase'))
    cases=[c for c in cases if c.get('classname')=='catalog.capabilities.tests.test_analysis_content_store']
    if row['name']=='windows-regression-capability-product':
        assert len(cases)==6 and all(c.find('skipped') is None for c in cases)
    if row['name'].startswith('full-suite-order-'):
        assert len(cases)==6 and sum(c.find('skipped') is not None for c in cases)==1
    if cases: regression_rows.append(dict(artifact_id=row['artifact_id'],tests=len(cases),
                                         passed=sum(c.find('skipped') is None for c in cases)))
api=client()
pr=api('/pulls/465')
assert pr['head']['sha']==SHA and pr['state']=='open' and not pr['merged']
comments=api('/pulls/465/comments?per_page=100')
reviews=api('/pulls/465/reviews?per_page=100')
assert not comments and not reviews,'Inspect current correctness findings before finalizing'
report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_commit=SHA,
    required_jobs=37,additional_cfi2_registry_jobs=3,workflows=rows,
    release_native_checkouts=14,source_bound_aggregates=2,collected_test_identities=4426,
    full_orders_exact_test_identities=True,skips_with_pass_elsewhere=skips['skips_with_pass_elsewhere'],
    skips_covered_by_native_nonartifact_log=10,unchanged_intentional_posix_exclusions=exclusions,
    native_skip_map_sha256=digest(ROOT/'pr465-skip-native-map.json'),
    release_artifact_sha256=digest(ROOT/'pr465-final-release-artifact-review.json'),
    icse_review_sha256=digest(ROOT/'pr465-icse-artifact-review.json'),
    manual_reviewed_files=changed,review_findings=[],reviewed_boundaries=[
      'reader snapshot and same-handle size validation',
      'pinned parents and same-resource native publication',
      'old replacement API default preserved',
      'unsupported publication preserves destination; no retry/unlink fallback'],
    regression_artifacts=regression_rows,status='QUALIFIED_REQUIRED_PR_HEAD_SCOPE',
    merge_boundary='Recheck ready-state review/head and merge requirements immediately before normal merge.',
    physical_acceptance=False,candidate_frozen=False)
(ROOT/'pr465-final-qualification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['source_commit','required_jobs','additional_cfi2_registry_jobs','status']}))
