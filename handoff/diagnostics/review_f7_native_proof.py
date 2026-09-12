"""Review immutable native F7 proof artifacts; no CI dispatch or runtime changes."""
import argparse, datetime, hashlib, importlib.util, json, pathlib, re, shutil, subprocess, xml.etree.ElementTree as ET, zipfile
ROOT=pathlib.Path(__file__).resolve().parent
PRIVATE=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
SOURCE=pathlib.Path(r'C:\wsl\fcp-ci-f7-consolidation-20260912')
def digest(raw):return hashlib.sha256(raw).hexdigest()
def main():
    p=argparse.ArgumentParser();p.add_argument('pr',type=int,choices=[468,469]);args=p.parse_args()
    snapshot=json.loads((ROOT/'ci-f7-replacement-runs-latest.json').read_text())
    row=next(r for r in snapshot['runs'] if r['pr']==args.pr)
    assert row['status']=='completed' and row['conclusion']=='success'
    label=row['retention_label'];ret=json.loads((PRIVATE/(label+'-native-retention.json')).read_text())
    artifacts=json.loads((PRIVATE/(label+'-raw-artifact-retention.json')).read_text())['records']
    assert len(ret['records'])==len(artifacts)==len(row['jobs'])==2
    assert ret['source_commit']==row['actual_checkout_expected']
    spec=importlib.util.spec_from_file_location('ci_contract',SOURCE/'catalog/common/tests/test_f7_ci_coverage.py');contract=importlib.util.module_from_spec(spec);spec.loader.exec_module(contract)
    workflow=subprocess.check_output(['git','-C',str(SOURCE),'show',row['api_head_sha']+':.github/workflows/phase-f7-closeout.yml'],text=True)
    commands=[' '.join(c[1]) for m in ['compileall','pytest -o','ruff check','compose config','diff --check'] for c in contract._commands(workflow,m)]
    assert len(commands)==7
    evidence=ROOT/'ci-f7-native-evidence';evidence.mkdir(exist_ok=True)
    native=[];outcomes={}
    for job in row['jobs']:
        osname='Windows' if '(Windows,' in job['name'] else 'Linux'
        assert job['conclusion']=='success' and job['runner_id']
        assert ('fcp-windows' if osname=='Windows' else 'fcp-linux-fast') in job['labels']
        steps={s['name']:s for s in job['steps']}
        for name in ['Run actions/checkout@v4','Run ./.github/actions/self-hosted-python','Install dependencies','Compile complete F7 boundary','Complete F7 and Phase 6 acceptance matrix','Ruff complete F7 boundary','Preserve capability UP035 coverage','Preserve relay lifecycle lint coverage','Compose validation','Diff hygiene','Preserve native F7 test evidence']:
            assert steps[name]['conclusion']=='success',name
        if osname=='Linux':assert steps['Ubuntu storage precondition']['conclusion']=='success'
        rec=next(r for r in ret['records'] if r['job_id']==job['id']);assert rec['checkout_matches'] is True
        raw=(PRIVATE/(label+'-native-logs')/(str(job['id'])+'.log')).read_bytes();assert digest(raw)==rec['sha256']
        log=raw.decode('utf-8');actual=re.findall(r'##\[group\]Run ([^\r\n]+)',log)
        def normalize(c):return re.sub(r'--junitxml="[^"]+"','--junitxml=JUNIT',c)
        for c in commands:
            c=c.replace("${{ github.base_ref || 'main' }}",'main' if args.pr==468 else 'codex/ci-f7-coverage-consolidation')
            assert normalize(c) in [normalize(a) for a in actual],c
        version='3.12.10' if osname=='Windows' else '3.12.13'
        assert re.search(r'Z '+re.escape(version)+r'\r?\n',log)
        shell='WindowsPowerShell' if osname=='Windows' else '/usr/bin/bash'
        assert shell in log
        if osname=='Windows':assert r'TEMP: \\?\C:\fcp-qtmp' in log
        else:assert 'Runner meets the product host-resource precondition.' in log
        a=next(a for a in artifacts if a['name']=='f7-closeout-'+osname)
        assert a['api_workflow_head']==row['api_head_sha'] and a['run_id']==row['run_id']
        zpath=PRIVATE/(label+'-raw-artifacts')/(str(a['id'])+'.zip');zraw=zpath.read_bytes();assert 'sha256:'+digest(zraw)==a['digest']
        with zipfile.ZipFile(zpath) as z:
            assert z.namelist()==['f7-closeout-'+osname+'.xml'];xml=z.read(z.namelist()[0])
        root=ET.fromstring(xml);cases=list(root.iter('testcase'));assert len(cases)==722
        assert all(int(s.attrib[k])==0 for s in root.iter('testsuite') for k in ['errors','failures'])
        state={(c.get('classname'),c.get('name')):'skip' if c.find('skipped') is not None else 'pass' for c in cases};assert len(state)==722
        outcomes[osname]=state
        selected={}
        for path in [*contract.WINDOWS_AI_TESTS,*contract.TRANSFER_TESTS,'catalog/common/tests/test_f7_ci_coverage.py']:
            module=path[:-3].replace('/','.');matched=[value for (cls,_),value in state.items() if cls==module]
            assert matched and set(matched)=={'pass'},module
            selected[path]=len(matched)
        retained=evidence/(str(a['id'])+'.zip');shutil.copyfile(zpath,retained)
        excerpts=[line for line in log.splitlines() if ('##[group]Run ' in line and any(m in line for m in ['compileall','pytest -o','ruff check','compose config','diff --check'])) or 'Runner name:' in line or row['actual_checkout_expected'] in line or re.search(r'Z '+re.escape(version)+r'$',line) or 'Runner meets the product host-resource precondition.' in line]
        native.append({'os':osname,'job_id':job['id'],'runner_name':job['runner_name'],'runner_id':job['runner_id'],'labels':job['labels'],'checkout_commit':rec['checkout_commits'][0],'log_sha256':rec['sha256'],'log_bytes':rec['bytes'],'log_excerpts':excerpts,'python':version,'shell':shell,'all_required_steps_success':True,'tests':len(cases),'passed':sum(v=='pass' for v in state.values()),'skipped':sum(v=='skip' for v in state.values()),'failures':0,'errors':0,'all_unique_mapped_modules_passed':selected,'skip_details':[{'class':c.get('classname'),'name':c.get('name'),'reason':c.find('skipped').get('message')} for c in cases if c.find('skipped') is not None],'artifact':a,'retained_zip':retained.relative_to(ROOT).as_posix(),'junit_sha256':digest(xml)})
    assert outcomes['Windows'].keys()==outcomes['Linux'].keys()
    assert all(outcomes['Windows'][k]=='pass' or outcomes['Linux'][k]=='pass' for k in outcomes['Linux'])
    out={'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'ONE_COMPLETE_GREEN_NATIVE_REPLACEMENT_EXECUTION_REVIEWED','pr':args.pr,'run_id':row['run_id'],'api_head_sha':row['api_head_sha'],'actual_checkout':row['actual_checkout_expected'],'checkout_tree_identical_to_head':True,'workflow_blob':row['workflow_blob'],'event':row['event'],'native_jobs':native,'union_passed_test_identities':722,'every_skip_passes_on_other_native_os':True,'required_green_runs':2,'retirement_authorized_by_this_single_receipt':False,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
    (ROOT/('ci-f7-pr'+str(args.pr)+'-native-proof.json')).write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'pr':args.pr,'status':out['status'],'jobs':[{'os':r['os'],'passed':r['passed'],'skipped':r['skipped'],'all_unique_modules_passed':True} for r in native],'union':722}))
if __name__=='__main__':main()
