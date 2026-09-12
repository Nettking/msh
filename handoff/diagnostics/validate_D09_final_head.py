"""Lightweight validation of integrated D09 at the actual PR head."""
import datetime,hashlib,json,pathlib,subprocess,sys,urllib.error,xml.etree.ElementTree as ET
ROOT=pathlib.Path(__file__).resolve().parent;REPO=pathlib.Path(r'C:\wsl\fcp-ci-f7-consolidation-20260912');PRIVATE=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
sys.path.insert(0,str(PRIVATE))
from github_qualification import client
BASE='f3abe5452db2f21593a688bc62bc5f4b22d5c40e';HEAD='ba44100ec1e4cde19daba0d3723b991c11742316';DOC='docs/implementation/f7_ci_consolidation.md'
def git(*args):return subprocess.check_output(['git','-C',str(REPO),*args])
def tree(ref):
 out={}
 for e in git('ls-tree','-r','-z',ref).split(b'\0'):
  if e:m,p=e.split(b'\t',1);out[p.decode()]=m.decode()
 return out
api=client();pr=api('/pulls/468');assert pr['head']['sha']==HEAD and pr['state']=='open' and pr['draft']
assert git('rev-parse','HEAD').decode().strip()==HEAD and not git('status','--porcelain')
b,h=tree(BASE),tree(HEAD);assert b.keys()==h.keys();delta=[p for p in b if b[p]!=h[p]];assert delta==[DOC]
canonical='\n'.join(p+'\t'+h[p] for p in sorted(h) if p!=DOC)
checks=[]
def run(name,args):
 r=subprocess.run(args,cwd=REPO,capture_output=True,text=True);checks.append({'name':name,'command':args,'exit_code':r.returncode,'stdout':r.stdout,'stderr':r.stderr});assert r.returncode==0,name;return r
run('branding',[sys.executable,'-B','scripts/check_product_branding.py'])
junit=PRIVATE/'pr468-ba44100e-final-lightweight.xml'
run('29 focused CI contracts',[sys.executable,'-B','-m','pytest','-o','addopts=','-p','no:cacheprovider','-q','catalog/common/tests/test_f7_ci_coverage.py','--junitxml='+str(junit)])
x=ET.fromstring(junit.read_bytes());assert len(list(x.iter('testcase')))==29 and not list(x.iter('failure')) and not list(x.iter('error')) and not list(x.iter('skipped'))
(ROOT/'pr468-ba44100e-final-lightweight.xml').write_bytes(junit.read_bytes())
run('diff hygiene',['git','diff','--check',BASE,HEAD])
legacy=['phase-f71-job-contracts.yml','phase-f72-provider-selection.yml','phase-f73-durable-job-ownership.yml','phase-f74-worker-dispatch.yml','phase-f75-retry-cancellation.yml','phase-f76-artifact-authorization.yml','phase-f77-ai-runtime-integration.yml','phase-f84-compute-worker-activation.yml']
patterns=[];own={'.github/workflows/'+n for n in legacy}
for n in legacy:
 p=REPO/'.github/workflows'/n;assert p.is_file();patterns.extend([n,p.read_text(encoding='utf-8').splitlines()[0].removeprefix('name: ').strip('"\'')])
pat=PRIVATE/'ci-retirement-reference-patterns.txt';pat.write_text('\n'.join(patterns)+'\n',encoding='utf-8')
r=subprocess.run(['rg','--hidden','-n','-F','-f',str(pat),'--glob','!.git','--glob','!.git/**','.'],cwd=REPO,capture_output=True,text=True);assert r.returncode in [0,1]
matches=r.stdout.replace('\\','/').splitlines();external=[]
for line in matches:
 path=line.split(':',1)[0].removeprefix('./')
 if path not in own and not path.startswith('docs/'):
  external.append(line)
assert not external,external
policy={}
for endpoint in ['/branches/main','/branches/main/protection','/branches/main/protection/required_status_checks','/rulesets']:
 try:policy[endpoint]={'http_status':200,'data':api(endpoint)}
 except urllib.error.HTTPError as e:policy[endpoint]={'http_status':e.code,'message':json.loads(e.read().decode()).get('message')}
assert api('/pulls/468')['head']['sha']==HEAD
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'FINAL_HEAD_LIGHTWEIGHT_SOURCE_VALIDATION_PASS_WITH_STATUS_API_LIMITATION','pr':468,'actual_head_sha':HEAD,'base_sha':pr['base']['sha'],'clean':True,'compared_native_proven_source':BASE,'changed_paths':delta,'all_other_tracked_entries_identical':len(h)-1,'unchanged_source_entries_sha256':hashlib.sha256(canonical.encode()).hexdigest(),'executable_workflow_source_byte_identical':True,'two_native_greens_receipt':'ci-f7-two-native-greens.json','provenance_rule':'Retain original run/head/checkout attribution; reuse unchanged executable/command/OS coverage, do not relabel earlier runs as executions of final commit.','real_js_only_event_receipt':'ci-f7-js-only-trigger-proof.json','canary_comment_excluded':True,'checks':checks,'focused_tests':29,'legacy_workflows_still_present':legacy,'reference_matches':matches,'external_executable_workflow_name_or_file_consumers':external,'reference_disposition':'Current references intact. Retirement still requires updating the manifest and two OSL F7.7 documentation references; no deletion performed.','status_contract_api':policy,'status_contract_limitation':'Main branch metadata and accessible contexts are recorded; detailed protection/ruleset endpoints may be unavailable. Do not infer inaccessible policy contents.','no_contract_found_requiring_two_native_reruns_for_docs_only':'Reviewed cleanup manifest and F7 contracts in D09-preintegration-source-comparison.json. Final-source checks are distinct from the retained prior-source native evidence.','merge_or_retirement_approved':False,'physical_acceptance':False,'protected_recorder_data':'UNTOUCHED'}
(ROOT/'pr468-ba44100e-final-validation.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'head':HEAD,'changed_paths':delta,'unchanged_entries':len(h)-1,'branding':'PASS','focused_tests':29,'diff_hygiene':'PASS','external_executable_references':external,'status_api':{p:r['http_status'] for p,r in policy.items()},'reference_match_count':len(matches)}))
