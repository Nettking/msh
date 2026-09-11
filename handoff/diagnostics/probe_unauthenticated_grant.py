"""One unchanged app negative-authority request on the host's owned N Flask."""
import datetime
import json
import platform
import subprocess

n='0536f03d67eb277e11573c2188d8e820399627e3'
host=platform.node().casefold()
assert host in ('nettking','nitro')
project='fcp-v1-73c779-nettking' if host=='nettking' else 'fcp-v1-fba508-nitro'
cid=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project='+project,'--filter','label=com.docker.compose.service=flask'],text=True).strip()
assert cid and '\n' not in cid
container=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
assert 'FCP_BUILD_COMMIT='+n in container['Config']['Env']
code='''import json,time,urllib.request,urllib.error
start=time.monotonic()
request=urllib.request.Request('http://127.0.0.1:5000/internal/federation/tailnet-join-grant',data=b'{}',method='POST',headers={'Content-Type':'application/json'})
try:
 response=urllib.request.urlopen(request,timeout=10)
except urllib.error.HTTPError as error:
 response=error
with response:
 body=json.loads(response.read(32768))
 print(json.dumps({'status':response.status,'accepted':body.get('accepted'),'error':body.get('error'),'grant_present':bool(body.get('pairing_code')),'elapsed_seconds':time.monotonic()-start}))
'''
p=subprocess.run(['docker','exec',cid,'python','-B','-c',code],capture_output=True,text=True,timeout=15)
report=dict(candidate_sha=n,host=host,stage='independent app grant fail-closed boundary; no actual peer grant requested',
 mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
 container=cid,image=container['Image'],procedure=code,exit_code=p.returncode,
 result=json.loads(p.stdout) if p.returncode==0 else None,error=p.stderr[-2000:],
 expected=dict(status=403,accepted=False,error='unauthenticated',grant_present=False),
 state_changed='Owned HTTP logs only; no supplied secret, no membership request or deployment',
 protected_recorder_data_untouched=True,physical_acceptance=False)
print(json.dumps(report))
