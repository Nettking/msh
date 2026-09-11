"""Observe direct POST response with no redirects, no secret and no grant redemption."""
import datetime
import json
import platform
import subprocess

host=platform.node().casefold();assert host in ('nettking','nitro')
n='0536f03d67eb277e11573c2188d8e820399627e3'
project='fcp-v1-73c779-nettking' if host=='nettking' else 'fcp-v1-fba508-nitro'
cid=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project='+project,'--filter','label=com.docker.compose.service=flask'],text=True).strip()
assert cid and '\n' not in cid
info=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
assert 'FCP_BUILD_COMMIT='+n in info['Config']['Env']
code='''import hashlib,http.client,json,time,urllib.parse
start=time.monotonic();conn=http.client.HTTPConnection('127.0.0.1',5000,timeout=10)
conn.request('POST','/internal/federation/tailnet-join-grant',body=b'{}',headers={'Content-Type':'application/json'})
response=conn.getresponse();raw=response.read(32768)
try:body=json.loads(raw)
except ValueError:body={}
print(json.dumps({'status':response.status,'content_type':response.getheader('Content-Type'),'location_path':urllib.parse.urlsplit(response.getheader('Location','')).path,'body_length':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'accepted':body.get('accepted'),'error':body.get('error'),'grant_present':bool(body.get('pairing_code')),'elapsed_seconds':time.monotonic()-start}))
conn.close()
'''
p=subprocess.run(['docker','exec',cid,'python','-B','-c',code],capture_output=True,text=True,timeout=15)
print(json.dumps(dict(candidate_sha=n,host=host,mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
 observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),container=cid,image=info['Image'],procedure=code,
 exit_code=p.returncode,result=json.loads(p.stdout) if p.returncode==0 else None,error=p.stderr[-2000:],
 expected='403 unauthenticated if route is reached; otherwise inspect direct admission gate',
 state_changed='Owned request logs; no followed redirects or supplied secret; no intentional persistent mutation',
 protected_recorder_data_untouched=True,physical_acceptance=False)))
