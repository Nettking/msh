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
REPO = Path('C:/wsl/fcp-analysis-content-resolve-race-20260912')
SHA = '84c66f8185c1411d9dc8c5c33244a2f564845ce7'
BASE = '2a9c9b8eb53edff74c2de23570ec56e054d29b22'
sys.path.insert(0, str(A))
from github_qualification import REQUIRED, client

def read(name): return json.loads((ROOT/name).read_text(encoding='utf-8'))
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def git(*args): return subprocess.check_output(['git',*args],cwd=REPO,text=True).strip()
state = read('pr467-qualification-state.json')['prs']['467']
native = read('pr467-native-provenance.json')
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
        path=A/'pr467-head-84c66f81-delta-native-logs'/(str(j['id'])+'.log')
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
    p=A/'pr467-head-84c66f81-delta-native-logs'/(str(jobs[name]['id'])+'.log')
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
                   'Verified 4457 tests across 4 disjoint successful shards']:
        assert marker in log(name),(name,marker)
assert 'needs: [release-matrix, postgres-storage, suite-order-independence]' in block('release-verdict')
for dep in ['release-matrix','postgres-storage','suite-order-independence']:
    assert 'test "${{ needs.'+dep+'.result }}" = "success"' in block('release-verdict')
assert sum(line.partition(' ')[2]=='test "success" = "success"'
           for line in log('Federation v1 automated release verdict').splitlines())==3
artifacts=read('pr467-final-release-artifact-review.json')
assert artifacts['source_commit']==SHA and artifacts['run_id']==release['run_id']
assert artifacts['complete_artifact_set'] and not artifacts['missing_artifacts']
assert artifacts['test_nodeids_covered']==4457 and len(artifacts['records'])==9
skips=read('pr467-skip-native-map.json')
assert skips['source_sha']==SHA and skips['collected']==4457
native_modules={m for r in skips['matched'] if r['summaries']
                and all('skipped' not in s and 'deselected' not in s for s in r['summaries'])
                for m in r['modules']}
# The new focused exit module intentionally skips seven Linux-only cases on
# Windows. Account for those exact cases, not a blanket zero-skips assumption.
import ast
exit_source=git('show',SHA+':catalog/federation/tests/test_tailnet_join_responder_exit.py')
exit_tree=ast.parse(exit_source)
linux_cases={
    'test_main_waits_for_previous_process_to_release_its_listener':2,
    'test_linux_wait_requires_exit_readiness_within_the_bound':5,
}
expected_condition=ast.dump(ast.parse('not sys.platform.startswith("linux")',mode='eval').body)
for function,count in linux_cases.items():
    node=next(n for n in exit_tree.body if isinstance(n,ast.FunctionDef) and n.name==function)
    marks={d.func.attr:d for d in node.decorator_list if isinstance(d,ast.Call) and isinstance(d.func,ast.Attribute)}
    assert ast.dump(marks['skipif'].args[0])==expected_condition
    assert len(ast.literal_eval(marks['parametrize'].args[1]))==count
windows_log=log('Release checks (Windows)')
assert '150 passed, 7 skipped' in windows_log and 'deselected' not in windows_log
command=next(line for line in windows_log.splitlines() if 'python -m pytest -o addopts= -q' in line and 'test_tailnet_join_responder_exit.py' in line)
selected_modules=re.findall(r'(catalog/[a-zA-Z0-9_/]+\.py)',command)
assert len(selected_modules)==13
collection=json.loads((A/'pr467-head-84c66f81-release-final-release-artifact-review/shards/shard-0.json').read_text())['collected']
assert sum(n.split('::')[0] in selected_modules for n in collection)==157
assert sum(n.startswith('catalog/federation/tests/test_tailnet_join_responder_exit.py::'+f+'[')
           for n in collection for f in linux_cases)==7
recorder_module='catalog/mtconnect_recorder/tests/test_tailscale_recorder_launcher.py'
assert recorder_module in selected_modules
recorder_tree=ast.parse(git('show',SHA+':'+recorder_module))
windows_condition=ast.dump(ast.parse('os.name != "nt"',mode='eval').body)
recorder_skips=[]
for node in recorder_tree.body:
    if isinstance(node,ast.FunctionDef):
        for d in node.decorator_list:
            if isinstance(d,ast.Call) and isinstance(d.func,ast.Attribute) and d.func.attr=='skipif':
                assert ast.dump(d.args[0])==windows_condition
                recorder_skips.append(node.name)
assert len(recorder_skips)==3
native_modules.add(recorder_module)
exclusions=[]
for item in skips['uncovered']:
    module=item['test'].rsplit('.test_',1)[0].replace('.','/')+'.py'
    if module in native_modules: continue
    source=git('show',SHA+':'+module)
    assert source==git('show',BASE+':'+module)
    assert 'skipif' in source and item['reason'] in source
    assert 'os.name != "nt"' in source or 'sys.platform != "win32"' in source
    exclusions.append(item)
previous=read('archive-main-9b286f93/merged-main-final-qualification.json')['unchanged_intentional_posix_exclusions']
assert sorted(exclusions,key=lambda x:x['test'])==sorted(previous,key=lambda x:x['test'])
assert len(exclusions)==11 and len(skips['uncovered'])-len(exclusions)==10
icse=read('pr467-icse-artifact-review.json')
assert icse['source_sha']==SHA and icse['icse_run']==workflows['icse-tool-demo.yml']['run_id']
assert icse['source_export_exact'] and len(icse['component'])==3
assert all(x['passed']==4 for x in icse['component'])
assert len(icse['network'])==2 and all(x['required_checks_passed']==10 and x['teardown'] for x in icse['network'])
changed=git('diff','--name-only',BASE,SHA).splitlines()
assert changed==['catalog/capabilities/analysis/content_store.py',
    'catalog/capabilities/tests/test_analysis_content_path_resolution.py']
prequalification=read('pr467-prequalification-review.json')
assert prequalification['source_commit']==SHA and prequalification['base']==BASE
assert prequalification['findings']==[] and prequalification['changed_files']==changed
for boundary in ['catalog/federation/stable_filesystem.py',
                 'catalog/capabilities/artifact_contracts.py',
                 'catalog/capabilities/analysis/scheduler.py']:
    assert git('show',SHA+':'+boundary)==git('show',BASE+':'+boundary)
assert git('show',SHA+':catalog/federation/backup_recovery.py') == git('show',BASE+':catalog/federation/backup_recovery.py')
assert 'catalog/federation/tests/test_tailnet_join_responder_exit.py' in workflow
for platform,summary in [('Linux','154 passed, 3 skipped'),('Windows','150 passed, 7 skipped')]:
    output=log('Release checks ('+platform+')')
    assert 'python -m pytest -o addopts= -q' in output
    assert 'catalog/federation/tests/test_tailnet_join_responder_exit.py' in output
    assert summary in output

regression_rows=[]
d08_rows=[]
for row in artifacts['records']:
    with zipfile.ZipFile(A/'pr467-head-84c66f81-release-final-raw-artifacts'/(str(row['artifact_id'])+'.zip')) as archive:
        xml=next(n for n in archive.namelist() if n.endswith('.xml'))
        cases=list(ET.fromstring(archive.read(xml)).iter('testcase'))
    if row['name'].startswith('full-suite-order-'):
        for module,count in [('test_tailnet_join_responder_exit',11),('test_tailnet_join_responder',32),('test_backup_recovery',17)]:
            selected=[c for c in cases if c.get('classname')=='catalog.federation.tests.'+module]
            assert len(selected)==count and all(c.find('skipped') is None for c in selected)
            if module=='test_backup_recovery':
                assert {c.get('name') for c in selected if c.get('name').startswith('test_backup_retains')} == {
                    'test_backup_retains_its_stop_deadline_and_controlled_error[7.0]',
                    'test_backup_retains_its_stop_deadline_and_controlled_error[None]'}
            regression_rows.append(dict(artifact_id=row['artifact_id'],module=module,tests=count,passed=count))
    original=[c for c in cases if c.get('classname')=='catalog.capabilities.tests.test_analysis_scheduling'
        and c.get('name')=='test_concurrent_submission_beside_the_running_driver_strands_nothing']
    if row['name']=='windows-regression-capability-product' or row['name'].startswith('full-suite-order-'):
        assert len(original)==1 and original[0].find('skipped') is None
    d08=[c for c in cases if c.get('classname')=='catalog.capabilities.tests.test_analysis_content_path_resolution']
    if row['name']=='windows-regression-capability-product' or row['name'].startswith('full-suite-order-'):
        assert len(d08)==17
        skipped={c.get('name'):c.find('skipped').get('message') for c in d08 if c.find('skipped') is not None}
        if row['name']=='windows-regression-capability-product':
            expected_skips={c.get('name'):'Windows symlink privilege unavailable' for c in d08
                if c.get('name').startswith('test_resolution_preserves_real_link_containment[')}
            assert len(expected_skips)==4 and skipped==expected_skips
        else:
            expected_skips={c.get('name'):('Windows deleted-file final name'
                if c.get('name').startswith('test_resolution_survives_native_leaf_replacement[')
                else 'Windows junction containment') for c in d08
                if c.get('name').startswith(('test_resolution_survives_native_leaf_replacement[',
                    'test_resolution_preserves_real_junction_containment['))}
            assert len(expected_skips)==8 and skipped==expected_skips
        d08_rows.append(dict(artifact_id=row['artifact_id'],artifact_name=row['name'],tests=17,
            passed=17-len(skipped),skipped=skipped,original_failing_scheduler_test='PASS',
            cases=[dict(name=c.get('name'),result='SKIP' if c.find('skipped') is not None else 'PASS') for c in d08]))
    cases=[c for c in cases if c.get('classname')=='catalog.capabilities.tests.test_analysis_content_store']
    if row['name']=='windows-regression-capability-product':
        assert len(cases)==6 and all(c.find('skipped') is None for c in cases)
    if row['name'].startswith('full-suite-order-'):
        assert len(cases)==6 and sum(c.find('skipped') is not None for c in cases)==1
    if cases: regression_rows.append(dict(artifact_id=row['artifact_id'],tests=len(cases),
                                         passed=sum(c.find('skipped') is None for c in cases)))
api=client()
pr=api('/pulls/467')
assert pr['head']['sha']==SHA and pr['state']=='open' and not pr['merged']
comments=api('/pulls/467/comments?per_page=100')
reviews=api('/pulls/467/reviews?per_page=100')
assert len(comments)==len(reviews)==0, 'Review new findings before merge'
threads=read('pr467-current-review-threads.json')['review_threads']
assert threads==[]
assert not pr['draft'] and pr['base']['sha']==BASE
assert len(d08_rows)==3
all_d08_names={c['name'] for r in d08_rows for c in r['cases']}
passed_d08_names={c['name'] for r in d08_rows for c in r['cases'] if c['result']=='PASS'}
assert len(all_d08_names)==17 and all_d08_names==passed_d08_names
f85=workflows['phase-f85-operator-federation-surface.yml']
f85_windows=next(j for j in f85['jobs'] if 'Windows' in j['name'])
f85_path=A/'pr467-head-84c66f81-delta-native-logs'/(str(f85_windows['id'])+'.log')
assert digest(f85_path)==proofs[f85_windows['id']]['sha256']
assert '742 passed, 5 skipped' in f85_path.read_text(encoding='utf-8')

report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_commit=SHA,
    required_jobs=37,additional_cfi2_registry_jobs=3,workflows=rows,
    release_native_checkouts=14,source_bound_aggregates=2,collected_test_identities=4457,
    full_orders_exact_test_identities=True,skips_with_pass_elsewhere=skips['skips_with_pass_elsewhere'],
    skips_covered_by_native_nonartifact_log=10,unchanged_intentional_posix_exclusions=exclusions,
    native_skip_map_sha256=digest(ROOT/'pr467-skip-native-map.json'),
    windows_focused_skip_accounting=dict(total=157,passed=150,linux_only_skipped=7,
        linux_only_cases=linux_cases,recorder_windows_cases_passed=recorder_skips),
    release_artifact_sha256=digest(ROOT/'pr467-final-release-artifact-review.json'),
    icse_review_sha256=digest(ROOT/'pr467-icse-artifact-review.json'),
    manual_reviewed_files=changed,review_findings=[],resolved_review_threads=[],reviewed_boundaries=prequalification['reviewed_cases'],
    regression_artifacts=regression_rows,d08_regression_artifacts=d08_rows,
    d08_all_17_cases_have_native_pass=True,
    f85_windows_repaired_path=dict(job_id=f85_windows['id'],sha256=digest(f85_path),summary='742 passed, 5 skipped'),
    status='QUALIFIED_REQUIRED_PR_HEAD_SCOPE',
    merge_boundary='Recheck ready-state review/head and merge requirements immediately before normal merge.',
    physical_acceptance=False,candidate_frozen=False)
(ROOT/'pr467-final-qualification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['source_commit','required_jobs','additional_cfi2_registry_jobs','status']}))
