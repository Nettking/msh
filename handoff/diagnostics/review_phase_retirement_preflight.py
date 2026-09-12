"""Bound retirement to mapped source and already-completed native evidence."""
import datetime
import fnmatch
import hashlib
import json
import pathlib
import re
import subprocess
import zipfile

dest=pathlib.Path('handoff/diagnostics')
repo=pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912')
private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
base='b7194820d8f1940ae60b8c9639e09b7f61e65c55'
def git(*args):
    return subprocess.check_output(['git',*args],cwd=repo,text=True).strip()
assert git('rev-parse','HEAD')==base and not git('status','--porcelain')
legacy=['phase-f71-job-contracts.yml','phase-f72-provider-selection.yml','phase-f73-durable-job-ownership.yml','phase-f74-worker-dispatch.yml','phase-f75-retry-cancellation.yml','phase-f76-artifact-authorization.yml','phase-f77-ai-runtime-integration.yml','phase-f84-compute-worker-activation.yml']
paths=['.github/workflows/'+f for f in legacy]
inventory=git('ls-files','--',*paths).splitlines();assert sorted(inventory)==sorted(paths)
retained=['.github/workflows/'+f for f in ['phase-f7-closeout.yml','phase-f8-closeout.yml','phase-f6-closeout.yml','federation-v1-release.yml']]
sources=['f3abe5452db2f21593a688bc62bc5f4b22d5c40e','ba44100ec1e4cde19daba0d3723b991c11742316',base]
blobs={p:{s:git('rev-parse',s+':'+p) for s in sources} for p in retained}
assert all(len(set(v.values()))==1 for v in blobs.values())
target='catalog/relay/tests/test_phase_f84_compute_worker_activation.py'
f8=(repo/retained[1]).read_text(encoding='utf-8')
old=(repo/paths[-1]).read_text(encoding='utf-8')
release=(repo/retained[-1]).read_text(encoding='utf-8')
def paths_for(text,event):
    block=re.search(r'^  '+event+r':\n(.*?)(?=^  \S|^permissions:)',text,re.M|re.S).group(1)
    return re.findall(r'^      - "([^"\n]+)"$',block,re.M)
trigger_checks=[]
for event in ['pull_request','push']:
    for path in paths_for(old,event):
        if path==paths[-1]:continue
        assert path in paths_for(f8,event)
        representative=path.replace('/**','/example.py')
        assert any(fnmatch.fnmatchcase(representative,p) for p in paths_for(release,event))
        trigger_checks.append({'event':event,'old_path':path,'f8_exact_path':True,'release_path':True})
retentions=[]; outcomes=[]
with zipfile.ZipFile(dest/'f84-existing-native-proof-logs.zip','w',compression=zipfile.ZIP_DEFLATED) as z:
    for label in ['f84-existing-old','f84-existing-final']:
        native=json.loads((private/(label+'-native-retention.json')).read_text())
        assert len(native['records'])==4
        retentions.append(native)
        for r in native['records']:
            raw=(private/(label+'-native-logs')/(str(r['job_id'])+'.log')).read_bytes()
            assert hashlib.sha256(raw).hexdigest()==r['sha256'] and r['checkout_matches'] and r['conclusion']=='success'
            z.writestr(str(r['job_id'])+'.log',raw)
            log=raw.decode('utf-8')
            if r['name'].startswith('trusted-provider-closeout'):
                assert target in log and 'python -m compileall' in log and 'python -m pytest' in log
                assert 'docker compose config --quiet' in log and 'git diff --check' in log
                summary=re.findall(r'\b(\d+ passed, \d+ skipped[^\r\n]*)',log)
                assert len(summary)==1
                outcomes.append({'job_id':r['job_id'],'runner':r['runner_name'],'summary':summary[0]})
            else:
                commands=[line for line in log.splitlines() if 'python -m ruff check' in line]
                assert any(target in c and 'catalog/capabilities' in c and '--ignore I001,RUF022,B008,C408,PLC0206,UP035' in c for c in commands)
names=[(repo/p).read_text(encoding='utf-8').splitlines()[0].removeprefix('name: ') for p in paths]
matches=[]
for path in git('ls-files').splitlines():
    try:text=(repo/path).read_text(encoding='utf-8')
    except (UnicodeError,OSError):continue
    for n,line in enumerate(text.splitlines(),1):
        if any(value in line for value in legacy+names):matches.append({'path':path,'line':n,'text':line})
outside=[m for m in matches if m['path'] not in paths]
assert {m['path'] for m in outside} <= {'docs/implementation/federation_v1_cleanup_manifest.md','docs/implementation/osl_integration/02_current_fcp_architecture.md','docs/implementation/osl_integration/08_validation_testing_and_ci.md'}
receipt={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'base_sha':base,'status':'PRE_DELETE_ALL_EIGHT_EQUIVALENCE_AND_REFERENCE_REVIEW_PASS','tracked_deletion_inventory':inventory,'legacy_blobs':{p:git('rev-parse',base+':'+p) for p in paths},'retained_workflow_blobs':blobs,'F7_proof':'ci-f7-two-native-greens.json','F7_real_js_only_event':'ci-f7-js-only-trigger-proof.json','F8_proof':{'native_runs':[34689990982,34695331207],'supplemental_lint_runs':[34689991000,34695331201],'native_retention':retentions,'summaries':outcomes,'trigger_checks':trigger_checks,'archive':'f84-existing-native-proof-logs.zip','interpretation':'Two complete native F8 matrices green; same-source Linux/Windows release check steps supply the exact F8.4 relay lint scope missing from F8 broad lint. Aggregate failures in unrelated old release shards are not claimed green. All retained F8/release command bytes are identical across proof sources and current main. Existing platform skips remain unchanged; F8 target has no added exclusion.'},'references':matches,'external_executable_consumers':[],'status_contract':'None of these eight is in the current37 required set; main metadata unprotected/empty contexts, detailed policy API403 visibility limitation preserved. Ordinary PR gates remain enforced, no bypass.','index_cache_consequences':'repo_index.should_index excludes .github paths. Edited docs are indexed; load_cached_chunks compares full fingerprint including SHA256 and invalidates stale cache automatically. No runtime cache or protected data is touched. Exact export inventory will change and must be revalidated on the eventual new candidate.','authorized_scope':'User explicitly permits retirement of proven gates/coherent groups after replacement and references proven. This is the separate change required by cleanup manifestG, after PR468 extension merged.','planned_changes':'Delete these eight tracked YAML files; update cleanup manifest, F7 consolidation status and two OSL references. Retained workflows, tests, product, commands, dependencies, runner accounts/config and trigger bytes stay unchanged.','validation_plan':'Clean final commit: branding,29F7 CI contracts, diff hygiene, exact deletion inventory/reference/blob checks. Native F7 exact-head dispatch (existing path filters do not trigger on deleted sibling files), applicable automatic release checks; retain existing2/2 and actual JS-only event instead of repeating both. No full37 intermediate qualification.','physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
(dest/'phase-workflow-retirement-preflight.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8');s+='\n## '+receipt['recorded_at']+' — all eight retirement preconditions verified\n\nReceipt: diagnostics/phase-workflow-retirement-preflight.json. F7 two-green/JS-event proof remains valid. Existing F8 native pairs34689990982 and34695331207 plus each source release lint checks supply two-green F8.4 equivalence. Eight native logs are archived in diagnostics/f84-existing-native-proof-logs.zip; no jobs rerun. All four retained workflow blobs match original/final/merged sources. Exact deletion inventory and all literal consumers are recorded; only manifest/two OSL docs require edits. AI index excludes .github; documentation fingerprints invalidate automatically. Next delete only the eight proven files on the isolated retirement branch, update the four documentation files and validate the final source. No physical or runner state change.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({'status':receipt['status'],'inventory':len(inventory),'F8_outcomes':outcomes,'references':len(matches),'external_executable_consumers':0}))
