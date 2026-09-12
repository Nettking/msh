"""Review retained native test identities/outcomes and archive raw evidence."""
import datetime,hashlib,json,pathlib,xml.etree.ElementTree as ET,zipfile
dest=pathlib.Path(__file__).resolve().parent;private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
receipt=json.loads((dest/'pr475-completed-raw-artifact-retention.json').read_text())
targets={'D11':'test_live_reinstatement_restores_replica_and_acknowledgement_policy','D14':'test_skip_serializes_with_concurrent_benchmark_completion','D15':'test_three_device_federation_keeps_ai_compute_and_storage_authority_separate','D16':'test_reachable_isolated_leader_cannot_report_current_session_authority'}
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':'5e6f184311019b9982e8544a18f3dc02c1b16e98','native_checkout':'a5e743fee24e27bfd8d6d4f57c8efd589c6c3a42','artifacts':[],'target_observations':[],'physical_acceptance':False}
with zipfile.ZipFile(dest/'pr475-complete-native-artifacts.zip','w',zipfile.ZIP_STORED) as bundle:
    for rec in receipt['records']:
        path=private/'pr475-completed-raw-artifacts'/(str(rec['id'])+'.zip')
        assert 'sha256:'+hashlib.sha256(path.read_bytes()).hexdigest()==rec['digest']
        bundle.write(path,path.name)
        item=dict(rec);item['junit']=[]
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                if not name.endswith('.xml'):continue
                root=ET.fromstring(archive.read(name));cases=root.findall('.//testcase')
                item['junit'].append({'file':name,'tests':len(cases),'failed':len(root.findall('.//failure')),'errors':len(root.findall('.//error')),'skipped':len(root.findall('.//skipped'))})
                for case in cases:
                    for finding,test in targets.items():
                        if case.get('name')==test:
                            failures=case.findall('failure')+case.findall('error')
                            out['target_observations'].append({'finding':finding,'artifact_id':rec['id'],'artifact_name':rec['name'],'test':test,'class':case.get('classname'),'seconds':case.get('time'),'outcome':'failure' if failures else 'skipped' if case.find('skipped') is not None else 'pass','failure_text':[f.text for f in failures]})
        out['artifacts'].append(item)
(dest/'pr475-artifacts-reviewed.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'artifacts':len(out['artifacts']),'target_observations':[{k:v for k,v in x.items() if k!='failure_text'} for x in out['target_observations']]}))
