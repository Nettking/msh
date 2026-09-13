"""Retain completed qualification logs and prove checkouts; never run tests."""
import argparse,concurrent.futures,datetime,hashlib,json,os,pathlib,re,subprocess,sys,urllib.error,urllib.request,zipfile
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from retain_qualification_logs import NoRedirect

root=pathlib.Path(__file__).parent
cache=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913/.acceptance/qualification')
p=argparse.ArgumentParser();p.add_argument('--main-source');args=p.parse_args()
state_path=root.parent/'physical-e6a9b74a/windows-repair-qualification-current.json'
if args.main_source:
 assert re.fullmatch('[0-9a-f]{40}',args.main_source)
 root=root.parent/f'main-{args.main_source[:8]}-qualification'
 state_path=root/'qualification-current.json'
 cache=pathlib.Path(f'C:/wsl/fcp-v1-{args.main_source[:8]}-main-20260913/.acceptance/qualification')
cache.mkdir(parents=True,exist_ok=True)
state=json.loads(state_path.read_text())
source=state['source'];assert source==(args.main_source or '94567b0d9ac916eec9e4f094d2f6754562b966aa')
credential=subprocess.run(['git','credential','fill'],input='protocol=https\nhost=github.com\n\n',capture_output=True,text=True,timeout=15,env=dict(os.environ,GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='never'))
token=dict(x.split('=',1) for x in credential.stdout.splitlines() if '=' in x)['password']
jobs=[(w,j) for w in state['workflows'] for j in w['jobs'] if j['status']=='completed' and j['conclusion']!='skipped']
def retain(pair):
 w,j=pair;path=cache/f"{j['id']}.log"
 if not path.exists():
  decoded=cache/f"{j['id']}.json"
  if decoded.exists():raw=json.loads(decoded.read_text())['content'].encode('utf-8')
  else:
   request=urllib.request.Request(f"https://api.github.com/repos/Nettking/msh/actions/jobs/{j['id']}/logs",headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json'})
   try:
    with urllib.request.build_opener(NoRedirect).open(request,timeout=25) as response:raw=response.read()
   except urllib.error.HTTPError as e:
    if e.code not in [301,302,303,307,308]:raise
    with urllib.request.urlopen(e.headers['Location'],timeout=30) as response:raw=response.read()
  path.write_bytes(raw)
 raw=path.read_bytes();lines=raw.decode('utf-8',errors='replace').splitlines()
 checkouts=[lines[i+1].split()[-1] for i,line in enumerate(lines[:-1]) if 'git' in line and 'log -1 --format=%H' in line]
 assert all(c==source for c in checkouts),(j['id'],checkouts)
 no_checkout={'Clean-checkout suite order independence','Federation v1 automated release verdict'}
 if j['conclusion']=='success':assert checkouts or j['name'] in no_checkout,(j['id'],j['name'])
 return {'job_id':j['id'],'run_id':w['run_id'],'name':j['name'],'conclusion':j['conclusion'],'runner':j['runner_name'],'checkout_sha':source if checkouts else None,'log_sha256':hashlib.sha256(raw).hexdigest(),'pytest_summaries':[x for x in lines if re.search(r'\b\d+ passed\b',x) and not x.lstrip().startswith('>')],'network_results':[x for x in lines if 'NETWORK_DEMO=' in x],'registry_results':[x for x in lines if 'REGISTRY_' in x and ('PASS' in x or 'FAIL' in x)]}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:reports=list(pool.map(retain,jobs))
archive=root/'native-job-logs.zip'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for p in sorted(cache.glob('*.log')):z.writestr(p.name,p.read_bytes())
report={'source':source,'reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'current_completed_jobs':len(reports),'source_verified_jobs':sum(bool(r['checkout_sha']) for r in reports),'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'jobs':reports}
(root/'native-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='jobs'}))
