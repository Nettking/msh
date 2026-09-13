"""Read and retain the two current-source failed-job artifacts; never dispatch."""
import hashlib,json,os,pathlib,subprocess,sys,urllib.error,urllib.request,zipfile
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from retain_qualification_logs import NoRedirect

root=pathlib.Path(__file__).parent
creds=subprocess.run(['git','credential','fill'],input='protocol=https\nhost=github.com\n\n',capture_output=True,text=True,timeout=15,env=dict(os.environ,GIT_TERMINAL_PROMPT='0',GCM_INTERACTIVE='never'))
token=dict(x.split('=',1) for x in creds.stdout.splitlines() if '=' in x)['password']
opener=urllib.request.build_opener(NoRedirect)
for aid,name,digest in [(10318857798,'icse-windows-initial.zip','dda282c5a7206d18d1f747be2f4351bd7cab43443fb9e07ad8c6f9a6d9120944'),(10319505728,'windows-transport-initial.zip','958324560eb5ea1a35354011a5090e56359017859e481498eb75ab468df82f19')]:
 path=root/name
 if not path.exists():
  request=urllib.request.Request(f'https://api.github.com/repos/Nettking/msh/actions/artifacts/{aid}/zip',headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json'})
  try:
   with opener.open(request,timeout=25) as response:data=response.read()
  except urllib.error.HTTPError as e:
   if e.code not in [301,302,303,307,308]:raise
   with urllib.request.urlopen(e.headers['Location'],timeout=25) as response:data=response.read()
  assert hashlib.sha256(data).hexdigest()==digest
  path.write_bytes(data)
 assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
 with zipfile.ZipFile(path) as z:
  print(json.dumps({'artifact_id':aid,'name':name,'sha256':digest,'files':z.namelist()}))
  if name.startswith('icse'):
   summary=json.loads(z.read('summary.json'));assert summary['source_sha']=='94567b0d9ac916eec9e4f094d2f6754562b966aa'
   print(json.dumps({k:summary.get(k) for k in ['source_sha','result','checks','failure','all_owned_processes_stopped']}))
