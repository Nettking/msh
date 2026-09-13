"""Validate retained exact-source native artifacts after qualification is green."""
import argparse,datetime,hashlib,io,json,pathlib,re,stat,subprocess,sys,tarfile,tempfile,xml.etree.ElementTree as ET,zipfile
root=pathlib.Path(__file__).parent
repo=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913')
source='94567b0d9ac916eec9e4f094d2f6754562b966aa'
p=argparse.ArgumentParser();p.add_argument('--main-source');args=p.parse_args()
state_path=root.parent/'physical-e6a9b74a/windows-repair-qualification-current.json'
if args.main_source:
 assert re.fullmatch('[0-9a-f]{40}',args.main_source)
 source=args.main_source;root=root.parent/f'main-{source[:8]}-qualification'
 repo=pathlib.Path(f'C:/wsl/fcp-v1-{source[:8]}-main-20260913');state_path=root/'qualification-current.json'
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==source
 sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
 from github_qualification import client
 assert client()('/git/ref/heads/main')['object']['sha']==source
state=json.loads(state_path.read_text())
assert state['source']==source and state['all_expected_green']
assert len(state['workflows'])==13 and sum(len(w['jobs']) for w in state['workflows'])==40
native=json.loads((root/'native-review.json').read_text())
assert {j['job_id'] for j in native['jobs']}=={j['id'] for w in state['workflows'] for j in w['jobs']}
assert native['source_verified_jobs']==38 and all(j['conclusion']=='success' for j in native['jobs'])
assert native['archive_sha256']==hashlib.sha256((root/'native-job-logs.zip').read_bytes()).hexdigest()
tree=subprocess.check_output(['git','rev-parse',source+'^{tree}'],cwd=repo,text=True).strip()
assert tree==subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=repo,text=True).strip()
meta=json.loads((root/'artifact-metadata.json').read_text())['artifacts']
def sha(data):return hashlib.sha256(data).hexdigest()
def files(raw):
 with zipfile.ZipFile(io.BytesIO(raw)) as z:
  items=z.infolist();assert len(items)<10000 and sum(i.file_size for i in items)<200_000_000
  names=[i.filename for i in items];assert len(names)==len(set(names))==len({n.casefold() for n in names})
  for i in items:
   p=pathlib.PurePosixPath(i.filename)
   assert not p.is_absolute() and '..' not in p.parts and '\\' not in i.filename and ':' not in i.filename and not stat.S_ISLNK(i.external_attr>>16)
  return {i.filename:z.read(i) for i in items if not i.is_dir()}
def artifact(row):
 raw=(root/'artifacts'/f"{row['id']}.zip").read_bytes()
 assert len(raw)==row['size_in_bytes'] and 'sha256:'+sha(raw)==row['digest']
 return files(raw)
def junit(data):
 xml=ET.fromstring(data);suites=list(xml.iter('testsuite'));assert suites
 assert all(int(s.get('errors','0'))==int(s.get('failures','0'))==0 for s in suites)
 assert not list(xml.iter('failure')) and not list(xml.iter('error'))
 tests=sum(int(s.get('tests','0')) for s in suites);assert len(list(xml.iter('testcase')))==tests
 return {'tests':tests,'skipped':sum(int(s.get('skipped','0')) for s in suites)}
sys.path.insert(0,str(repo))
from scripts.ci_pytest_shards import verify
release=[]
with tempfile.TemporaryDirectory(prefix='reviewed-shards-',dir=repo/'.acceptance') as tmp:
 directory=pathlib.Path(tmp)
 rows=[a for a in meta if a['qualification_workflow']=='federation-v1-release.yml'];assert len(rows)==9
 for row in rows:
  entry={'id':row['id'],'name':row['name'],'digest':row['digest'],'files':[]}
  for name,data in artifact(row).items():
   e={'name':name,'sha256':sha(data)}
   if name.endswith('.xml'):e.update(junit(data))
   elif name.endswith('.json'):
    m=json.loads(data);assert m['source_sha']==source
    assert m['source_identity_before']==m['source_identity_after']=={'sha':source,'tree':tree}
    assert pathlib.PurePosixPath(name).name==name
    (directory/name).write_bytes(data)
   else:raise AssertionError('Unexpected release artifact entry')
   entry['files'].append(e)
  release.append(entry)
 coverage=verify(directory,4,source)
cf8=[]
for row in [a for a in meta if a['qualification_workflow']=='cf8-role-retirement.yml']:
 content=artifact(row);assert set(content)=={'cf8-acceptance.xml'}
 result=junit(content['cf8-acceptance.xml']);assert result['tests']==12
 cf8.append({'artifact_id':row['id'],**result})
assert len(cf8)==2
bundles=[a for a in meta if a['name']==f'fcp-icse-tool-demo-bundle-{source}'];assert len(bundles)==1
bundle=artifact(bundles[0]);manifest=json.loads(bundle['artifact-manifest.json'])
icse_run=next(w['run_id'] for w in state['workflows'] if w['workflow']=='icse-tool-demo.yml')
assert manifest['source_revision']==source and manifest['source_ref']==('refs/heads/main' if args.main_source else 'refs/pull/486/merge') and manifest['workflow_run']==str(icse_run)
for line in bundle['SHA256SUMS'].decode().splitlines():
 digest,name=line.split('  ',1);assert re.fullmatch('[0-9a-f]{64}',digest) and sha(bundle[name])==digest
publication=manifest['publication_archive']['file']
digest,name=bundle['ZENODO_SHA256'].decode().strip().split('  ',1);assert name==publication and sha(bundle[publication])==digest
nested=files(bundle[publication]);sp=manifest['publication_archive']['source_prefix'];ap=manifest['publication_archive']['artifact_prefix']
assert all(n.startswith(sp) or n.startswith(ap) for n in nested)
public_source={n.removeprefix(sp):b for n,b in nested.items() if n.startswith(sp)}
public_metadata={n.removeprefix(ap):b for n,b in nested.items() if n.startswith(ap)}
assert public_metadata=={n:b for n,b in bundle.items() if n not in (publication,'ZENODO_SHA256')}
reference=subprocess.check_output(['git','-c','core.autocrlf=false','-c','core.eol=lf','archive','--format=tar',source],cwd=repo)
with tarfile.open(fileobj=io.BytesIO(reference),mode='r:') as z:expected={i.name:z.extractfile(i).read() for i in z.getmembers() if i.isfile()}
assert public_source==expected and not any('.git' in pathlib.PurePosixPath(n).parts or 'private-state' in pathlib.PurePosixPath(n).parts for n in nested)
assert {e['execution'] for e in manifest['evidence']}=={'Linux','Windows','compose'}
for e in manifest['evidence']:
 data=bundle[e['file']];result=json.loads(data)
 assert sha(data)==e['sha256'] and result['implementation_commit']==source and result['passed']==result['total']==4
 assert all(s['result']=='pass' for s in result['scenarios'])
assert {e['execution'] for e in manifest['network_evidence']}=={'Linux','Windows'}
for e in manifest['network_evidence']:
 prefix='network-evidence/'+e['execution']+'/'
 assert {f['file'] for f in e['files']}=={prefix+n for n in ('summary.json','events.jsonl','operator-report.html')}
 for f in e['files']:
  data=bundle[f['file']];assert sha(data)==f['sha256']
  assert not any(t in data for t in (b'-----BEGIN ',b'ghp_',b'github_pat_'))
  assert not re.search(rb'"(?:transport_secret|enrollment_token|invitation_token|private_key)"\s*:',data)
 result=json.loads(bundle[prefix+'summary.json'])
 assert result['source_sha']==source and result['result']=='PASS' and len(result['checks'])==10 and set(result['checks'].values())=={'PASS'}
 assert result['all_owned_processes_stopped'] and not result.get('failure') and not result.get('shutdown_errors')
 assert [json.loads(l) for l in bundle[prefix+'events.jsonl'].decode().splitlines() if l.strip()]==result['events']
 assert bundle[prefix+'operator-report.html'].strip()
report={'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':source,'tree':tree,'status':'EXACT_SOURCE_AUTOMATED_QUALIFIED','required_jobs_passed':37,'companion_jobs_passed':3,'native_checkout_proofs':38,'aggregate_jobs_without_checkout':2,'disjoint_complete_linux_test_coverage':coverage,'release_artifacts':release,'cf8_junit':cf8,'publication_artifact':bundles[0]['id'],'publication_digest':bundles[0]['digest'],'publication_source_matches_git_archive':True,'network_checks':{'Linux':'10/10 PASS','Windows':'10/10 PASS'},'component_checks':'4/4 PASS Linux, Windows, Compose','physical_acceptance':'NOT_PASSED','protected_recorder_data':'UNTOUCHED'}
if args.main_source:report.update(status='ACTUAL_MAIN_AUTOMATED_QUALIFIED',live_main=source)
(root/'qualification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='release_artifacts'}))
