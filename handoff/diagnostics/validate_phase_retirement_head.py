"""Validate only the clean retirement commit and its preserved source contract."""
import datetime
import hashlib
import json
import pathlib
import subprocess

dest=pathlib.Path('handoff/diagnostics');repo=pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912')
private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');python=private.parent/'.venv/Scripts/python.exe'
pre=json.loads((dest/'phase-workflow-retirement-preflight.json').read_text());base=pre['base_sha']
def run(args):
    p=subprocess.run(args,cwd=repo,capture_output=True,text=True,timeout=120)
    return {'command':[str(a) for a in args],'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
def git(*args):
    r=run(['git',*args]);assert r['exit_code']==0,r;return r['stdout'].strip()
head=git('rev-parse','HEAD');assert not git('status','--porcelain')
changed=git('diff','--name-status',base,head).splitlines()
deleted=pre['tracked_deletion_inventory'];docs=['docs/implementation/f7_ci_consolidation.md','docs/implementation/federation_v1_cleanup_manifest.md','docs/implementation/osl_integration/02_current_fcp_architecture.md','docs/implementation/osl_integration/08_validation_testing_and_ci.md']
assert set(changed)=={'D\t'+p for p in deleted}|{'M\t'+p for p in docs}
def tree(sha):
    return {line.split('\t',1)[1]:line.split('\t',1)[0] for line in git('ls-tree','-r',sha).splitlines()}
before=tree(base);after=tree(head);unchanged={p:v for p,v in before.items() if p not in deleted+docs}
assert all(after[p]==v for p,v in unchanged.items())
legacy=[pathlib.PurePosixPath(p).name for p in deleted]
matches=[]
for p in after:
    try:text=(repo/p).read_text(encoding='utf-8')
    except (OSError,UnicodeError):continue
    for n,line in enumerate(text.splitlines(),1):
        if any(name in line for name in legacy):matches.append({'path':p,'line':n,'text':line})
assert not matches,matches
checks=[run([str(python),'-B','scripts/check_product_branding.py']),run([str(python),'-B','-m','pytest','-o','addopts=','-p','no:cacheprovider','-q','catalog/common/tests/test_f7_ci_coverage.py','--junitxml='+str(private/'phase-retirement-final-lightweight.xml')]),run(['git','diff','--check',base,head])]
assert all(c['exit_code']==0 for c in checks),checks
assert not git('status','--porcelain')
(dest/'phase-retirement-final-lightweight.xml').write_bytes((private/'phase-retirement-final-lightweight.xml').read_bytes())
receipt={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'CLEAN_RETIREMENT_HEAD_LIGHTWEIGHT_AND_SOURCE_CONTRACT_PASS','head_sha':head,'base_sha':base,'clean':True,'changed_paths':changed,'unchanged_tracked_entries':len(unchanged),'unchanged_entry_digest':hashlib.sha256(json.dumps(unchanged,sort_keys=True).encode()).hexdigest(),'all_remaining_workflows_tests_runtime_commands_dependencies_runner_configuration_identical':True,'remaining_literal_legacy_file_references':matches,'checks':checks,'previous_native_evidence_preserved':['ci-f7-two-native-greens.json','ci-f7-js-only-trigger-proof.json','phase-workflow-retirement-preflight.json'],'next_gate':'Open separate draft retirement PR, obtain scoped native F7 execution on this exact head via existing workflow_dispatch plus applicable automatic release evidence. Do not repeat the two original native proof matrices or actual JS-only event; their source remains unchanged. Do not merge before exact final-source native validation.','physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
(dest/'phase-retirement-final-head-validation.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8');s+='\n## '+receipt['recorded_at']+' — separate retirement source pushed and clean checks pass\n\nBranch codex/ci-phase-workflow-retirement-20260912 at '+head+' removes exactly eight proven hosted workflow files and edits four documentation files. All'+str(len(unchanged))+' remaining entries are byte-identical to merged b7194820; zero remaining literal legacy filename references. Branding,29F7 contracts and diff hygiene PASS on the clean final head. Receipt: diagnostics/phase-retirement-final-head-validation.json. Next open a draft retirement PR, preserve automatic CI and run one exact-head F7 native validation through the existing dispatch path. No physical/runtime/protected-data change.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({k:receipt[k] for k in ['status','head_sha','unchanged_tracked_entries','remaining_literal_legacy_file_references']}))
