import datetime,hashlib,json,pathlib,shutil,sys,zipfile,xml.etree.ElementTree as ET
ROOT=pathlib.Path('handoff');D=ROOT/'diagnostics';P=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');sys.path.insert(0,str(P))
from github_qualification import client
api=client()
# One immutable job identity read binds already-retained admission evidence to runner31;
# it does not poll or rerun completed main qualification.
job=api('/actions/jobs/103528608892');assert job['runner_id']==31 and job['conclusion']=='success'
old=json.loads((P/'main-17ab3a05-delta-694d0f37e839-native-retention.json').read_text());native=next(r for r in old['records'] if r['job_id']==job['id']);assert native['checkout_matches']
logpath=P/'main-17ab3a05-delta-694d0f37e839-native-logs/103528608892.log';raw=logpath.read_bytes();assert hashlib.sha256(raw).hexdigest()==native['sha256'];log=raw.decode('utf-8')
for text in ['10 passed','3.12.13','go1.25.7 linux/amd64','Runner meets the product host-resource precondition.','docker buildx version','Remove this job']:
 assert text in log,text
admission={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'EXISTING_ADMISSION_EVIDENCE_VERIFIED_NO_RERUN','runner_id':31,'runner_name':'AQG7NCC-Linux','source_sha':old['source_commit'],'run_id':native['run_id'],'job_id':job['id'],'started_at':job['started_at'],'completed_at':job['completed_at'],'native_receipt':native,'steps':job['steps'],'excerpt':[line for line in log.splitlines() if any(x in line for x in ['Runner name:','10 passed','3.12.13','go1.25.7 linux/amd64','Runner meets the product host-resource precondition.','docker buildx version','Start isolated PostgreSQL','Remove this job'])],'interpretation':'The checked-in CI test sharding workflow passed on this exact runner at08:57Z with Python/shell/Go/storage/Buildx/isolated PostgreSQL prerequisites. Earlier lookup after10:01Z excluded it. Existing main qualification was not repeated. No new runner admission or label mutation occurred.'}
(D/'aqg31-existing-admission-proof.json').write_text(json.dumps(admission,indent=2)+'\n',encoding='utf-8')
art=json.loads((P/'pr468-ba44100e-release-complete-raw-artifact-retention.json').read_text());(D/'pr468-ba44100e-release-artifact-retention.json').write_text(json.dumps(art,indent=2)+'\n',encoding='utf-8')
target='test_live_reinstatement_restores_replica_and_acknowledgement_policy';outcomes=[]
for a in art['records']:
 zpath=P/'pr468-ba44100e-release-complete-raw-artifacts'/(str(a['id'])+'.zip');assert 'sha256:'+hashlib.sha256(zpath.read_bytes()).hexdigest()==a['digest']
 with zipfile.ZipFile(zpath) as z:
  for name in z.namelist():
   if name.endswith('.xml'):
    for c in ET.fromstring(z.read(name)).iter('testcase'):
     if c.get('name')==target:outcomes.append({'artifact':a,'member':name,'classname':c.get('classname'),'test':target,'seconds':float(c.get('time')),'failure':c.find('failure') is not None,'skipped':c.find('skipped') is not None})
  if a['name']=='linux-regression-shard-2':
   m=json.loads(z.read('shard-2.json'));assert m['exit_code']==1 and m['source_sha']=='f18e91adae2cb198fb5cebad61ee202a38d2bd8e' and not m['source_error'];shutil.copyfile(zpath,D/'D11-original-shard-2.zip');failed_manifest={k:m[k] for k in ['source_sha','source_identity_before','source_identity_after','source_error','exit_code','collection_complete']}
comparison=json.loads((P/'D11-comparison-passes-native-retention.json').read_text());assert all(r['checkout_matches'] for r in comparison['records'])
assert len(outcomes)==4 and sum(o['failure'] for o in outcomes)==1
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':'ba44100ec1e4cde19daba0d3723b991c11742316','actual_checkout':'f18e91adae2cb198fb5cebad61ee202a38d2bd8e','outcomes':outcomes,'passing_native_provenance':comparison,'failed_manifest':failed_manifest,'durable_original_failure_zip':'D11-original-shard-2.zip','conclusion':'Existing same-source fixed/rotating full suites and Windows transport run passed this test; AQG shard2 failed with TimeoutError. Failure is not shown to be deterministic or caused by the CI/docs change. Host/timing/order cause remains unresolved.','next_action':'One targeted unchanged-source retry of failed shard2 is justified for reproducibility after retaining original artifacts and verifying AQG admission; preserve all successful shards/full suites. No deadline/assertion/authority changes.'}
(D/'D11-existing-source-comparison.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
p=D/'D11.md';s=p.read_text(encoding='utf-8')+'\n## Existing same-source comparison\n\n[Retained comparison](D11-existing-source-comparison.json): the identical test\npassed fixed/rotating full suites on Nettking-Linux (3.945s/2.539s) and native\nWindows transport (6.707s); AQG shard2 failed at49.573s. Actual checkout/provenance\nare verified. [Original failed shard ZIP](D11-original-shard-2.zip) is durable\nbefore any rerun can overwrite its artifact name. AQG31 admission is verified\nfrom existing08:57Z CI test sharding/native prerequisites; no new admission.\n\nOne targeted unchanged-source failed-shard retry is justified to assess whether\nthe timeout recurs after initial concurrent work ended. It is not a repair or\nproof that the original failure disappeared. Keep original failure and any\nnew runner/attempt provenance separate. Do not change deadlines or guards.\n';p.write_text(s,encoding='utf-8')
p=ROOT/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');a=s.index('Next highest-value action:');b=s.index('Pre-integration comparison:',a);s=s[:a]+'''Next highest-value action: diagnose/retry only the two failed final-head checks
on unchanged ba44100ec1e4cde19daba0d3723b991c11742316, after original evidence
retention. D11/#471 is AQG Linux shard2 live-reinstatement TimeoutError; D12/#472
is Beast Phase2 Go build VCS-status exit128 after Go tests passed. Root causes
remain unresolved; no product repair is justified yet. Red release aggregates
are consequences. F6/F7/F8 and other final-head checks passed.
D11 comparison proves same-source fixed/rotating full suites and Windows transport
passed the identical test; original failed shard ZIP is durably retained. One
targeted failed-shard retry can test recurrence without rerunning successes.
AQG31 admission is now VERIFIED from existing08:57Z run34684352823/job103528608892
and retained native Python/Go/storage/Buildx/PostgreSQL evidence. The earlier
post10:01Z lookup missed it. No new admission or main qualification was performed.
D09 is resolved at final head, 2/2 native replacement and real JS-only event are
retained, but PR468 remains unmerged and all eight legacy files remain pending
resolution/classification of the two required failures. No physical/runtime change.
'''+s[b:];s+='\n## '+out['recorded_at']+' — prior admission found; D11 comparison retained\n\n'+admission['interpretation']+'\n\nExisting identical-source results constrain D11 to a non-deterministic/contextual\nfailure so far. The original failed ZIP is pushed before any targeted retry.\nNo complete native proof or passing job will be repeated. Next assess D12\nwith unchanged-source Go build context and select at most one targeted retry\nper failed job. Keep PR468 source fixed and preserve each attempt separately.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({'aqg_admission_run':native['run_id'],'aqg_admission_runner':job['runner_id'],'D11_outcomes':[{'artifact':o['artifact']['name'],'seconds':o['seconds'],'failed':o['failure']} for o in outcomes]}))
