"""Preserve successful automatic release evidence; never claim full qualification."""
import datetime,hashlib,json,pathlib,subprocess,xml.etree.ElementTree as ET,zipfile
dest=pathlib.Path('handoff/diagnostics');private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');repo=pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912')
head='b7194820d8f1940ae60b8c9639e09b7f61e65c55';label='main-b719-release-pass'
native=json.loads((private/(label+'-native-retention.json')).read_text());art=json.loads((private/(label+'-raw-artifact-retention.json')).read_text());assert len(native['records'])==16 and len(art['records'])==9
verifydir=private/'main-b719-reviewed-shards';verifydir.mkdir(exist_ok=True);outcomes=[];manifests=[];summaries=[]
target='test_live_reinstatement_restores_replica_and_acknowledgement_policy'
with zipfile.ZipFile(dest/'main-b719-release-native-and-artifact-evidence.zip','w',compression=zipfile.ZIP_DEFLATED) as bundle:
    for r in native['records']:
        raw=(private/(label+'-native-logs')/(str(r['job_id'])+'.log')).read_bytes();assert hashlib.sha256(raw).hexdigest()==r['sha256'] and r['conclusion']=='success'
        assert r['checkout_matches'] or r['name'] in ['Clean-checkout suite order independence','Federation v1 automated release verdict']
        bundle.writestr('native/'+str(r['job_id'])+'.log',raw)
    for a in art['records']:
        raw=(private/(label+'-raw-artifacts')/(str(a['id'])+'.zip')).read_bytes();assert 'sha256:'+hashlib.sha256(raw).hexdigest()==a['digest'] and a['api_workflow_head']==head
        bundle.writestr('artifacts/'+str(a['id'])+'.zip',raw)
        with zipfile.ZipFile(private/(label+'-raw-artifacts')/(str(a['id'])+'.zip')) as z:
            for name in z.namelist():
                if name.startswith('shard-') and name.endswith('.json'):
                    m=json.loads(z.read(name));assert m['source_sha']==head and m['exit_code']==0 and not m['source_error']
                    assert pathlib.PurePosixPath(name).name==name
                    (verifydir/name).write_bytes(z.read(name));manifests.append(name)
                elif name.startswith('junit-') and name.endswith('.xml'):
                    assert pathlib.PurePosixPath(name).name==name
                    (verifydir/name).write_bytes(z.read(name))
                if name.endswith('.xml'):
                    root=ET.fromstring(z.read(name));cases=list(root.iter('testcase'))
                    assert all(c.find('failure') is None and c.find('error') is None for c in cases)
                    summaries.append({'artifact':a['name'],'member':name,'testcases':len(cases),'skips':sum(c.find('skipped') is not None for c in cases)})
                    for c in cases:
                        if c.get('name')==target:
                            assert c.find('skipped') is None
                            outcomes.append({'artifact':a['name'],'member':name,'seconds':float(c.get('time')),'status':'PASS'})
assert len(manifests)==4 and len(outcomes)==4
command=[str(private.parent/'.venv/Scripts/python.exe'),'-B','-m','scripts.ci_pytest_shards','verify','--directory',str(verifydir),'--count','4','--source-sha',head]
check=subprocess.run(command,cwd=repo,text=True,capture_output=True,timeout=40);assert check.returncode==0,check.stdout+check.stderr
record={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_sha':head,'run_id':34701018870,'status':'AUTOMATIC_RELEASE_16_OF_16_NATIVE_AND_ARTIFACTS_REVIEWED_NOT_FULL_CANDIDATE_QUALIFICATION','native':native,'artifacts':art,'test_artifact_summaries':summaries,'shard_verification':{'command':command,'exit_code':check.returncode,'stdout':check.stdout,'stderr':check.stderr,'verifier_source_note':'Checked-in verifier byte-identical between b719 and retirement440; data/source-sha remain explicitly b719.'},'D11_existing_same_source_passes':outcomes,'durable_bundle':'main-b719-release-native-and-artifact-evidence.zip','remaining_limits':'D11/D12 companion failures remain open; no physical candidate freeze, no full37 qualification claim, no reruns.','protected_recorder_data':'UNTOUCHED','physical_acceptance':False}
(dest/'main-b719-release-reviewed.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8')+'\n## '+record['recorded_at']+' — successful b719 release evidence retained\n\nAll16 native release logs and9digest-verified artifacts are durably bundled; all four source-bound clean shard manifests pass the checked-in verifier. Every retained JUnit has zero errors/failures. Four existing b719 executions of D11 test pass (shard2, fixed, rotating, Windows transport), retained distinctly from the failed AQG Phase2 stage. Receipt: diagnostics/main-b719-release-reviewed.json. This is the automatic release run only, not a complete candidate qualification or physical acceptance. No job rerun.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({'run':34701018870,'native':16,'artifacts':9,'D11_passes':outcomes,'shard_verification':check.stdout.strip()}))
