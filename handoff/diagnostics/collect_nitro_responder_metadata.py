"""Streamed read-only process/listener and health metadata, no secret reads/grants."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import time
import urllib.request

source=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
n='0536f03d67eb277e11573c2188d8e820399627e3'
sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()
assert sha==n
report=dict(candidate_sha=n,host='nitro',mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
 observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_clean=not subprocess.check_output(['git','status','--porcelain'],cwd=source,text=True).strip(),
 processes=[],state_changed=False,protected_recorder_data_untouched=True,physical_acceptance=False)
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit(): continue
 try:
  argv=proc.joinpath('cmdline').read_bytes().decode().split('\0')
  if 'catalog.federation.tailnet_join_responder' not in argv: continue
  env=dict(x.split('=',1) for x in proc.joinpath('environ').read_bytes().decode().split('\0') if '=' in x)
  cwd=proc.joinpath('cwd').resolve()
  if cwd!=source: continue
  elapsed=subprocess.check_output(['ps','-o','etimes=','-p',proc.name],text=True).strip()
  data=pathlib.Path(env.get('FCP_DATA_DIR',str(source/'data')))
  secret=pathlib.Path(env.get('FCP_AUTO_JOIN_SECRET_FILE',str(data/'federation/onboarding/auto_join_secret')))
  item=dict(pid=int(proc.name),cwd=str(cwd),executable=str(proc.joinpath('exe').resolve()),
    elapsed_seconds=int(elapsed),source_sha=sha,source_hash=hashlib.sha256((source/'catalog/federation/tailnet_join_responder.py').read_bytes()).hexdigest(),
    build_commit_environment=env.get('FCP_BUILD_COMMIT'),port=int(env.get('FCP_AUTO_JOIN_PORT','5151')),
    secret_file_exists=secret.is_file(),secret_content_read=False,
    source_identity_caveat='Current source hash does not alone prove already imported process bytes; reconcile elapsed start time with N activation.')
  begin=time.monotonic()
  try:
   with urllib.request.urlopen('http://127.0.0.1:'+str(item['port'])+'/fcp/federation/tailnet-join/health',timeout=5) as response:
    item['health']=dict(status=response.status,body=json.loads(response.read(4096)),elapsed_seconds=time.monotonic()-begin)
  except Exception as exc:item['health']=dict(error=type(exc).__name__,elapsed_seconds=time.monotonic()-begin)
  report['processes'].append(item)
 except (OSError,ValueError,UnicodeError): continue
print(json.dumps(report))
