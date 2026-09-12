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
REPO = Path('C:/wsl/fcp-v1-2a9c9b8e-merged-main-20260912')
SHA = '2a9c9b8eb53edff74c2de23570ec56e054d29b22'
BASE = 'b6a96b218a513fe241ef4d6f051cf166444643ac'
sys.path.insert(0, str(A))
from github_qualification import REQUIRED, client

def read(name): return json.loads((ROOT/name).read_text(encoding='utf-8'))
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def native_path(job_id):
    paths=list(A.glob('main-2a9c9b8e-delta-*-native-logs/'+str(job_id)+'.log'))
    assert len(paths)==1
    return paths[0]
def git(*args): return subprocess.check_output(['git',*args],cwd=REPO,text=True).strip()
state = read('merged-main-qualification-state.json')
native = read('merged-main-native-provenance.json')
assert state['source_commit'] == native['source_commit'] == SHA == git('rev-parse','HEAD')
assert not git('status','--porcelain') and not state['missing_workflows']
proofs = {r['job_id']:r for r in native['records']}
workflows = {w['workflow']:w for w in state['workflows']}
aggregate_names = {'Clean-checkout suite order independence','Federation v1 automated release verdict'}
required = dict(REQUIRED,**{'cfi2-onboarding-composition.yml':2,'release-image-metadata.yml':1})
rows = []
for name,count in required.items():
    w = workflows[name]
    assert w['event'] in {'push','workflow_dispatch'} and w['api_head_sha']==SHA
    assert w['conclusion']=='success' and len(w['jobs'])==count
    for j in w['jobs']:
        r=proofs[j['id']]
        assert j['status']=='completed' and j['conclusion']==r['conclusion']=='success'
        assert r['run_id']==w['run_id'] and r['name']==j['name']
        path=native_path(j['id'])
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
    p=native_path(jobs[name]['id'])
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
                   'Verified 4440 tests across 4 disjoint successful shards']:
        assert marker in log(name),(name,marker)
assert 'needs: [release-matrix, postgres-storage, suite-order-independence]' in block('release-verdict')
for dep in ['release-matrix','postgres-storage','suite-order-independence']:
    assert 'test "${{ needs.'+dep+'.result }}" = "success"' in block('release-verdict')
assert sum(line.partition(' ')[2]=='test "success" = "success"'
           for line in log('Federation v1 automated release verdict').splitlines())==3
artifacts=read('merged-main-release-artifact-review.json')
assert artifacts['source_commit']==SHA and artifacts['run_id']==release['run_id']
assert artifacts['complete_artifact_set'] and not artifacts['missing_artifacts']
assert artifacts['test_nodeids_covered']==4440 and len(artifacts['records'])==9
skips=read('merged-main-skip-native-map.json')
assert skips['source_sha']==SHA and skips['collected']==4440
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
collection=json.loads((A/'main-2a9c9b8e-release-final-release-artifact-review/shards/shard-0.json').read_text())['collected']
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
icse=read('merged-main-icse-artifact-review.json')
assert icse['source_sha']==SHA and icse['icse_run']==workflows['icse-tool-demo.yml']['run_id']
assert icse['source_export_exact'] and len(icse['component'])==3
assert all(x['passed']==4 for x in icse['component'])
assert len(icse['network'])==2 and all(x['required_checks_passed']==10 and x['teardown'] for x in icse['network'])
changed=git('diff','--name-only',BASE,SHA).splitlines()
assert changed==['.github/workflows/federation-v1-release.yml',
    'catalog/federation/tailnet_join_responder.py',
    'catalog/federation/tests/test_backup_recovery.py',
    'catalog/federation/tests/test_tailnet_join_responder.py',
    'catalog/federation/tests/test_tailnet_join_responder_exit.py']
assert git('show',SHA+':catalog/federation/backup_recovery.py') == git('show',BASE+':catalog/federation/backup_recovery.py')
assert 'catalog/federation/tests/test_tailnet_join_responder_exit.py' in workflow
for platform,summary in [('Linux','154 passed, 3 skipped'),('Windows','150 passed, 7 skipped')]:
    output=log('Release checks ('+platform+')')
    assert 'python -m pytest -o addopts= -q' in output
    assert 'catalog/federation/tests/test_tailnet_join_responder_exit.py' in output
    assert summary in output

regression_rows=[]
for row in artifacts['records']:
    with zipfile.ZipFile(A/'main-2a9c9b8e-release-final-raw-artifacts'/(str(row['artifact_id'])+'.zip')) as archive:
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
    cases=[c for c in cases if c.get('classname')=='catalog.capabilities.tests.test_analysis_content_store']
    if row['name']=='windows-regression-capability-product':
        assert len(cases)==6 and all(c.find('skipped') is None for c in cases)
    if row['name'].startswith('full-suite-order-'):
        assert len(cases)==6 and sum(c.find('skipped') is not None for c in cases)==1
    if cases: regression_rows.append(dict(artifact_id=row['artifact_id'],tests=len(cases),
                                         passed=sum(c.find('skipped') is None for c in cases)))
api=client()
assert api('/git/ref/heads/main')['object']['sha']==SHA
assert state['main_matches'] and state['actual_main']==SHA
assert git('rev-parse','HEAD^1')==BASE
qualified_pr463='2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee'
assert git('rev-parse','HEAD^2')==qualified_pr463
assert not git('diff',qualified_pr463,SHA,'--')
prior_pr=read('pr463-final-qualification.json')
assert prior_pr['source_commit']==qualified_pr463 and prior_pr['status']=='QUALIFIED_REQUIRED_PR_HEAD_SCOPE'
assert prior_pr['manual_reviewed_files']==changed and not prior_pr['review_findings']
# PR review supplies source-review provenance only; all CI/artifact proof above
# belongs independently to the actual resulting main SHA.
report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_commit=SHA,
    required_jobs=37,additional_cfi2_registry_jobs=3,workflows=rows,
    release_native_checkouts=14,source_bound_aggregates=2,collected_test_identities=4440,
    full_orders_exact_test_identities=True,skips_with_pass_elsewhere=skips['skips_with_pass_elsewhere'],
    skips_covered_by_native_nonartifact_log=10,unchanged_intentional_posix_exclusions=exclusions,
    native_skip_map_sha256=digest(ROOT/'merged-main-skip-native-map.json'),
    windows_focused_skip_accounting=dict(total=157,passed=150,linux_only_skipped=7,
        linux_only_cases=linux_cases,recorder_windows_cases_passed=recorder_skips),
    release_artifact_sha256=digest(ROOT/'merged-main-release-artifact-review.json'),
    icse_review_sha256=digest(ROOT/'merged-main-icse-artifact-review.json'),
    manual_reviewed_files=changed,review_findings=[],resolved_review_threads=['PRRT_kwDOPZM3cc6hmxQd'],reviewed_boundaries=[
      'same pinned pidfd or Windows handle from identity verification through bounded exit wait',
      'timeout and wait error fail closed before bind or process-record overwrite',
      'default signal-only boolean API preserved for backup; backup source and10s deadline unchanged',
      'real Linux delayed-listener release, native Windows owned-child exit and Windows wait status coverage',
      'D07 six-case regression coverage preserved in current-head artifacts'],
    regression_artifacts=regression_rows,status='QUALIFIED_ACTUAL_MERGED_MAIN',
    merge_boundary='Actual main and merge parents verified; freeze exact candidate then clean checked-in revalidation before fresh physical acceptance.',
    physical_acceptance=False,candidate_frozen=False)
(ROOT/'merged-main-final-qualification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['source_commit','required_jobs','additional_cfi2_registry_jobs','status']}))
