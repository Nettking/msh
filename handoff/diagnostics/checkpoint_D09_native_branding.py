import datetime,json,pathlib,subprocess
root=pathlib.Path('handoff');d=root/'diagnostics';private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance');ret=json.loads((private/'pr468-ba44100e-branding-native-retention.json').read_text());assert ret['records'][0]['checkout_matches']
head='ba44100ec1e4cde19daba0d3723b991c11742316';checkout=ret['source_commit'];repo=r'C:\wsl\fcp-ci-f7-consolidation-20260912'
def git(*args):return subprocess.check_output(['git','-C',repo,*args],text=True).strip()
assert git('rev-parse',head+'^{tree}')==git('rev-parse',checkout+'^{tree}')
snap=json.loads(sorted(d.glob('pr468-ba44100e-auto-*.json'))[-1].read_text());row=next(r for r in snap['runs'] if r['path'].endswith('product-branding.yml'));assert row['conclusion']=='success'
o={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_sha':head,'actual_checkout':checkout,'checkout_tree_identical_to_head':True,'status':'NATIVE_FINAL_HEAD_BRANDING_PASS','run_id':row['id'],'native_receipt':ret,'job':row['jobs'][0],'original_two_native_proofs':'ci-f7-two-native-greens.json','not_physical_acceptance':True}
(d/'D09-final-native-branding.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
p=d/'D09-evidence.json';r=json.loads(p.read_text());r['status']='RESOLVED-IN-PR';r['repair']['integrated_pr_head']=head;r['repair']['not_promoted_to_pr468']=False;r['final_validation']='pr468-ba44100e-final-validation.json';r['native_branding']='D09-final-native-branding.json';p.write_text(json.dumps(r,indent=2)+'\n',encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8').replace('docs-only ba44100e pushed, focused branding PASS; promotion deferred','docs-only ba44100e integrated after2/2 native proof; final29 CI contracts and native branding PASS');p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8');s+='\n## '+o['recorded_at']+' — native final-head branding green; new capacity observed\n\n'+'''Run34695331243/job103557723619 passed branding on Beast-Linux-WSL29.
Actual checkout f18e91adae2cb198fb5cebad61ee202a38d2bd8e is tree-identical
to ba44100e. Native proof: diagnostics/D09-final-native-branding.json. This
resolves D09 on the integrated PR head; no checker exception was introduced.

Initial final-head automatic CI snapshot at13:10Z: release34695331201 has active
shards/full-order work, no failure then; F6/F8/update/Phase2/F7 still progressing.
No manual native/full qualification dispatch occurred. Docs-portal34695331180
again failed hosted billing admission before any step; exact annotations retained.
Next long-job observation13:55Z or later, preferably near expected completion.

AQG Windows30 and Linux31 are now online, distinct registrations. Windows F6
passed existing Python/Go/dependency setup; Linux release shard passed Python/Go/
storage prerequisites. This is observed execution, not an assumption of admission.
Before counting release evidence from AQG31, verify the required prior CI test
sharding/prerequisite evidence under docs/ci_parallel_testing.md. No labels,
accounts or admission decisions were changed by this task.

Beast28's new capability shard successfully checked out and entered tests; the
same runner had D10 earlier. Preserve D10 as an intermittent environment failure
with unknown underlying cause; no manual host cleanup was performed. Receipt:
diagnostics/D10-later-checkout-observation.json. Avoid speculative cleanup during
active jobs. All eight legacy workflows and physical/Recorder state are unchanged.
''';p.write_text(s,encoding='utf-8')
