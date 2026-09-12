"""Read-only command/path inventory for the current eight-workflow cleanup review.

Uses indentation extraction of these checked-in, simple job/run/path blocks, not a
general YAML interpreter. Raw blocks, line numbers and hashes remain in the report.
"""
import datetime
import fnmatch
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
REPO=Path('C:/wsl/fcp-v1-17ab3a05-merged-main-20260912')
SHA='17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05'
CANDIDATES=['phase-f71-job-contracts.yml','phase-f72-provider-selection.yml',
 'phase-f73-durable-job-ownership.yml','phase-f74-worker-dispatch.yml',
 'phase-f75-retry-cancellation.yml','phase-f76-artifact-authorization.yml',
 'phase-f77-ai-runtime-integration.yml','phase-f84-compute-worker-activation.yml']
RETAINED=['federation-v1-release.yml','phase-f6-closeout.yml','phase-f7-closeout.yml','phase-f8-closeout.yml']
ADDITIONAL=['capability-config.yml','cf4-contribution-service.yml','cfi5-contribution-composition.yml',
 'cfi4-benchmark-composition.yml','cfi3-device-inspection-composition.yml','cfi2-onboarding-composition.yml',
 'command-bootstrap.yml','phase-f83-remote-ai-binding.yml','phase-f86-reconnect-reconciliation.yml',
 'phase-f85-operator-federation-surface.yml','phase2-federation.yml']

def git(*args): return subprocess.check_output(['git',*args],cwd=REPO,text=True).strip()
assert git('rev-parse','HEAD')==SHA and not git('status','--porcelain')
tracked=git('ls-files').splitlines()
python_files={p for p in tracked if p.endswith('.py')}
manifest=Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/pr467-head-84c66f81-release-final-release-artifact-review/shards/shard-0.json')
collection=json.loads(manifest.read_text())
assert collection['source_sha']=='84c66f8185c1411d9dc8c5c33244a2f564845ce7'
assert git('rev-parse',SHA+'^{tree}')==git('rev-parse',collection['source_sha']+'^{tree}')
test_files={n.split('::')[0] for n in collection['collected']}

def scopes(command):
    return sorted(set(re.findall(r'\b(?:catalog|scripts)(?:/[\w./-]+)?',command)))

def expand(selectors, universe):
    return sorted(p for p in universe if any(p==s or p.startswith(s.rstrip('/')+'/') for s in selectors))

def inventory(name):
    path='.github/workflows/'+name
    raw=git('show',SHA+':'+path)
    on=re.search(r'^on:\n(.*?)(?=^[^\s#])',raw,re.M|re.S).group(1)
    events={}
    for event in ['pull_request','push']:
        part=re.search(r'^  '+event+r':\n(.*?)(?=^  \S|\Z)',on,re.M|re.S)
        events[event]=dict(paths=re.findall(r'^      - ["\x27](.*?)["\x27]$',part.group(1),re.M),
                          raw=part.group(1)) if part else None
    jobs_raw=raw.split('\njobs:\n',1)[1]
    jobs=[]
    for m in re.finditer(r'^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)',jobs_raw,re.M|re.S):
        job,block=m.group(1),m.group(2)
        commands=[]
        for r in re.finditer(r'^        run: ([^\n]*)(?:\n((?:          [^\n]*\n|\n)*))?',block,re.M):
            scalar=r.group(1)
            text=(r.group(2) or '') if scalar in ['>-','>','|','|-'] else scalar
            command=' '.join(line.strip() for line in text.splitlines()).strip()
            command_type=next((kind for marker,kind in [('python -m pytest ','pytest'),
                ('python -m ruff check','ruff'),('python -m compileall','compile'),
                ('docker compose config','compose'),('git diff --check','diff')] if marker in command),'other')
            commands.append(dict(type=command_type,command=command,scopes=scopes(command),
                line=raw[:raw.index('jobs:\n')+len('jobs:\n')+m.start(2)+r.start()].count('\n')+1))
        jobs.append(dict(id=job,runs_on=re.findall(r'^    runs-on: (.+)$',block,re.M),
            matrix_os=re.findall(r'^        os: (.+)$',block,re.M),
            commands=commands,raw=block))
    assert jobs and any(j['commands'] for j in jobs)
    return dict(path=path,blob=git('rev-parse',SHA+':'+path),sha256=hashlib.sha256(raw.encode()).hexdigest(),
        events=events,jobs=jobs,workflow_call=('workflow_call:' in on),workflow_dispatch=('workflow_dispatch:' in on))

workflows={name:inventory(name) for name in CANDIDATES+RETAINED+ADDITIONAL}
def selected(name, kind):
    return set(expand([s for j in workflows[name]['jobs'] for c in j['commands'] if c['type']==kind
                     for s in c['scopes']],test_files if kind=='pytest' else python_files))
retained_tests=set.union(*(selected(n,'pytest') for n in RETAINED))
retained_lint=set.union(*(selected(n,'ruff') for n in RETAINED))
retained_compile=set.union(*(selected(n,'compile') for n in RETAINED))
rows=[]
for name in CANDIDATES:
    w=workflows[name]
    assert all(any('ubuntu-latest' in r or 'matrix.os' in r for r in j['runs_on']) for j in w['jobs'])
    assert any('ubuntu-latest' in str(j['matrix_os']) and 'windows-latest' in str(j['matrix_os']) for j in w['jobs'])
    rows.append(dict(workflow=name,hosted_linux_windows=True,
        explicit_test_files=len(selected(name,'pytest')),tests_not_in_retained_explicit_union=sorted(selected(name,'pytest')-retained_tests),
        lint_not_in_retained_union=sorted(selected(name,'ruff')-retained_lint),
        compile_not_in_retained_union=sorted(selected(name,'compile')-retained_compile),
        trigger_paths=w['events']['pull_request']['paths']))

# A concrete changed-path probe matters: static union coverage does not imply
# equivalent Windows coverage on every path that used to trigger a phase gate.
probe='catalog/flask_app/static/js/ai-explainer.js'
assert probe in tracked
triggered={n:any(fnmatch.fnmatchcase(probe,p) for p in workflows[n]['events']['pull_request']['paths'])
    for n in CANDIDATES+RETAINED+ADDITIONAL}
release_windows={p for j in workflows['federation-v1-release.yml']['jobs']
    if j['id'] in ['windows-regressions','release-checks'] for c in j['commands'] if c['type']=='pytest'
    for p in expand(c['scopes'],test_files)}
legacy_windows=selected('phase-f77-ai-runtime-integration.yml','pytest')
missing_windows=sorted(legacy_windows-release_windows)
additional_providers={n:dict(triggered=triggered[n],self_hosted=any('self-hosted' in str(j['runs_on']) for j in workflows[n]['jobs']),
    explicit_test_files=sorted(selected(n,'pytest'))) for n in ADDITIONAL}
conservative_other_coverage=set.union(*(set(v['explicit_test_files']) for v in additional_providers.values()
    if v['triggered'] and v['self_hosted']))
still_missing=sorted(set(missing_windows)-conservative_other_coverage)

strict_ignore='I001,RUF022,B008,C408,PLC0206'
lenient_ignore=strict_ignore+',UP035'
settings={}
for label,ignored in [('legacy',strict_ignore),('retained',lenient_ignore)]:
    command=[sys.executable,'-B','-m','ruff','check','--no-cache','--show-settings',
        'catalog/capabilities/analysis/content_store.py','--ignore',ignored]
    output=subprocess.check_output(command,cwd=REPO,text=True)
    section=re.search(r'linter.rules.enabled = \[(.*?)\n\]',output,re.S).group(1)
    settings[label]=dict(command=command,sha256=hashlib.sha256(output.encode()).hexdigest(),
        enabled=sorted(set(re.findall(r'\(([A-Z]+\d+)\)',section))))
lint_delta=sorted(set(settings['legacy']['enabled'])-set(settings['retained']['enabled']))
reference_pattern='|'.join(re.escape(n) for n in CANDIDATES)
ref=subprocess.run(['git','grep','-n','-E',reference_pattern],cwd=REPO,text=True,capture_output=True)
assert ref.returncode in [0,1]
report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_commit=SHA,
    candidate_workflows=rows,retained_workflows=RETAINED,inventory=workflows,
    reference_matches=ref.stdout.splitlines(),
    raw_full_suite_reference=dict(source_sha=collection['source_sha'],identical_source_tree=True,collected=len(collection['collected']),
        meaning='Existing PR artifact used for static module mapping only; no merged-main evidence claimed'),
    changed_path_probe=dict(path=probe,triggered=triggered,legacy_f77_test_files_missing_from_release_windows=missing_windows),
    additional_possible_providers=additional_providers,
    missing_even_if_all_triggered_additional_self_hosted_tests_count_as_windows=still_missing,
    effective_ruff_settings=settings,legacy_enabled_rules_missing_from_retained=lint_delta,
    state_changed=False,physical_acceptance=False,cleanup_change_prepared=False)
(ROOT/'phase-workflow-coverage-audit.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(dict(candidates=rows,changed_path_probe=report['changed_path_probe'],
    missing_even_with_conservative_other_self_hosted_coverage=still_missing,legacy_lint_rule_delta=lint_delta,
    additional_providers=[dict(workflow=n,triggered=v['triggered'],self_hosted=v['self_hosted']) for n,v in additional_providers.items()])))
