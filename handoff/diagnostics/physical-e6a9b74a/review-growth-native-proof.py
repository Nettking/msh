import hashlib,json,pathlib,re,subprocess,zipfile
here=pathlib.Path(__file__).parent
h=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
sha='501b528e9476878e6a6fe5cde8240b2d54b1d263';tested='990cb8f277cd25bed5c04d2265e1e015dd890533'
def git(ref):return subprocess.check_output(['git','rev-parse',ref],cwd=h,text=True).strip()
assert git(sha+'^{tree}')==git(tested+'^{tree}')
state=json.loads((here/'growth-repair-ci-current.json').read_text())
jobs=[j for r in state['runs'] if r['id'] in [34756997294,34756997291] for j in r['jobs']]
assert len(jobs)==4 and all(j['conclusion']=='success' for j in jobs)
reports=[];archive=here/'growth-native-job-logs.zip'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for job in jobs:
  log=json.loads((h/'.acceptance/qualification'/f"{job['id']}.json").read_text())['content']
  lines=log.splitlines()
  checkout=[lines[i+1] for i,l in enumerate(lines[:-1]) if 'git' in l and 'log -1 --format=%H' in l]
  assert checkout and all(l.split()[-1]==tested for l in checkout)
  assert 'All checks passed!' in log
  expected='93 passed' if 'Campaign tooling' in job['name'] else '224 passed'
  assert expected in log
  raw=log.encode();z.writestr(f"{job['id']}.log",raw)
  reports.append({'job_id':job['id'],'name':job['name'],'checkout_sha':tested,'log_sha256':hashlib.sha256(raw).hexdigest(),'contract_tests':expected,'ruff':'PASS'})
out={'product_candidate_sha':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','acceptance_harness_sha':sha,'tested_merge_sha':tested,'tested_tree':git(tested+'^{tree}'),'harness_tree_matches':True,'jobs':reports,'log_archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'native_nitro_focused_tests':23,'physical_pass':False}
(here/'growth-native-proof.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
