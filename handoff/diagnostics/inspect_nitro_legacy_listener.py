"""Read-only D04 socket ownership; never requests a grant or reads secrets."""
import datetime
import hashlib
import ipaddress
import json
import pathlib
import socket
import subprocess
import time
import urllib.request

pid=1422341
proc=pathlib.Path('/proc')/str(pid)
assert proc.joinpath('cwd').resolve()==pathlib.Path('/home/martin/fcp')
argv=proc.joinpath('cmdline').read_bytes().decode().split('\0')
assert any('tailnet_join_responder' in x for x in argv)
inodes=set()
for fd in proc.joinpath('fd').iterdir():
 try:
  target=str(fd.readlink())
  if target.startswith('socket:['):inodes.add(target[8:-1])
 except OSError:pass
listeners=[]
for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
 fields=line.split()
 if fields[3]!='0A' or fields[9] not in inodes:continue
 address,port=fields[1].split(':')
 host=socket.inet_ntoa(bytes.fromhex(address)[::-1]);port=int(port,16)
 item=dict(port=port,bind_class='tailnet' if ipaddress.ip_address(host) in ipaddress.ip_network('100.64.0.0/10') else 'other',socket_inode=fields[9])
 start=time.monotonic()
 if item['bind_class']=='tailnet':
  try:
   with urllib.request.urlopen(f'http://{host}:{port}/fcp/federation/tailnet-join/health',timeout=5) as r:
    item['health']=dict(status=r.status,body=json.loads(r.read(4096)),elapsed_seconds=time.monotonic()-start)
  except Exception as exc:item['health']=dict(error=type(exc).__name__,elapsed_seconds=time.monotonic()-start)
 listeners.append(item)
def git(*args):
 p=subprocess.run(['git',*args],cwd=proc.joinpath('cwd').resolve(),capture_output=True,text=True,timeout=10)
 return p.stdout.strip() if p.returncode==0 else 'unavailable'
report=dict(candidate_sha='0536f03d67eb277e11573c2188d8e820399627e3',host='nitro',pid=pid,
 mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
 legacy_checkout_sha=git('rev-parse','HEAD'),legacy_checkout_clean=not git('status','--porcelain'),
 legacy_source_caveat='Current disk HEAD does not prove old imported bytes; this process was not re-admitted on N.',
 listeners=listeners,candidate_default_port=5151,state_changed=False,protected_recorder_data_untouched=True,physical_acceptance=False)
print(json.dumps(report))
