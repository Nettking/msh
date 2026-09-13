"""Read immutable artifacts from existing qualification runs; no dispatch."""
import argparse,concurrent.futures,datetime,hashlib,json,os,pathlib,re,subprocess,sys,urllib.error,urllib.request
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
from retain_qualification_logs import NoRedirect
root=pathlib.Path(__file__).parent
p=argparse.ArgumentParser();p.add_argument('--main-source');args=p.parse_args()
state_path=root.parent/'physical-e6a9b74a/windows-repair-qualification-current.json'
if args.main_source:
 assert re.fullmatch('[0-9a-f]{40}',args.main_source)
 root=root.parent/f'main-{args.main_source[:8]}-qualification';state_path=root/'qualification-current.json'
dest=root/'artifacts';dest.mkdir(exist_ok=True)
a=client();state=json.loads(state_path.read_text())
assert state['source']==(args.main_source or '94567b0d9ac916eec9e4f094d2f6754562b966aa')
selected=[w for w in state['workflows'] if w['workflow'] in ['federation-v1-release.yml','icse-tool-demo.yml','cf8-role-retirement.yml']]
metadata=[]
for w in selected:
 rows=a(f"/actions/runs/{w['run_id']}/artifacts?per_page=100")['artifacts']
 for row in rows:row['qualification_workflow']=w['workflow']
 metadata.extend(rows)
credential=subprocess.run(['git','credential','fill'],input='protocol=https\nhost=github.com\n\n',capture_output=True,text=True,timeout=15,env=dict(os.environ,GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='never'))
token=dict(x.split('=',1) for x in credential.stdout.splitlines() if '=' in x)['password']
def fetch(row):
 p=dest/(str(row['id'])+'.zip');assert not row['expired']
 assert row['workflow_run']['head_sha'] in [state['source'],state.get('review_head',state['source'])]
 if not p.exists():
  request=urllib.request.Request(f"https://api.github.com/repos/Nettking/msh/actions/artifacts/{row['id']}/zip",headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json'})
  try:
   with urllib.request.build_opener(NoRedirect).open(request,timeout=25) as response:data=response.read()
  except urllib.error.HTTPError as e:
   if e.code not in [301,302,303,307,308]:raise
   with urllib.request.urlopen(e.headers['Location'],timeout=30) as response:data=response.read()
  assert len(data)==row['size_in_bytes'] and 'sha256:'+hashlib.sha256(data).hexdigest()==row['digest']
  p.write_bytes(data)
 assert 'sha256:'+hashlib.sha256(p.read_bytes()).hexdigest()==row['digest']
 return {'id':row['id'],'name':row['name'],'bytes':row['size_in_bytes'],'sha256':row['digest']}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(fetch,metadata))
(root/'artifact-metadata.json').write_text(json.dumps({'source':state['source'],'read_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'artifacts':metadata},indent=2)+'\n')
print(json.dumps(results))
