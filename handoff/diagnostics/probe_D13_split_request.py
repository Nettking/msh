"""Controlled valid HTTP framing against unchanged source; no product patch."""
import contextlib
import datetime
import http.client
import io
import json
import pathlib
import platform
import socket
import subprocess
import sys
import time

source=pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912');dest=pathlib.Path(__file__).resolve().parent
def git(*args):
    return subprocess.check_output(['git','-C',str(source),*args],text=True).strip()
assert platform.node().upper()=='NETTKING'
assert git('rev-parse','HEAD')=='440123f6bc6dc358eef3d233236bc14f91af60e0'
assert not git('status','--porcelain')
sys.dont_write_bytecode=True;sys.path.insert(0,str(source))
from catalog.federation.tests import test_tailnet_join_responder as test
result={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_sha':git('rev-parse','HEAD'),'host':platform.node(),'python':platform.python_version(),
        'user_context':subprocess.check_output(['whoami'],text=True).strip(),'deadline_seconds':5,
        'procedure':'Valid identical HTTP/1.1 POST with Content-Length2 to unknown path, coalesced or separate header/body sends;10ms controlled client inter-send delay in third case;5 attempts per case.',
        'qualification':False,'product_source_modified':False,'protected_recorder_data':'UNTOUCHED','physical_runtime':'UNCHANGED','cases':[]}
def grant(**kwargs):
    raise AssertionError('pairing authority must never be invoked')
for case in ['coalesced','split_no_delay','split_10ms']:
    attempts=[]
    for i in range(5):
        stderr=io.StringIO()
        with contextlib.redirect_stderr(stderr):
            server,port=test._serve(lambda _address:test.PeerVerification(test.PEER,'verified'),grant)
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=5) as sock:
                    headers=(f'POST /anything-else HTTP/1.1\r\nHost:127.0.0.1:{port}\r\nContent-Length:2\r\nConnection:close\r\n\r\n').encode('ascii')
                    if case=='coalesced':
                        sock.sendall(headers+b'{}')
                    else:
                        sock.sendall(headers)
                        if case=='split_10ms':time.sleep(0.010)
                        sock.sendall(b'{}')
                    response=http.client.HTTPResponse(sock);response.begin()
                    payload=json.loads(response.read())
                    assert response.status==404 and payload=={'accepted':False,'error':'unknown-path'}
                    item={'outcome':'pass','status':response.status,'payload':payload}
            except Exception as error:
                item={'outcome':'failure','exception':type(error).__name__,'message':str(error)}
            finally:
                server.shutdown();server.server_close()
        item.update(iteration=i+1,server_stderr=stderr.getvalue());attempts.append(item)
    result['cases'].append({'case':case,'passed':sum(i['outcome']=='pass' for i in attempts),'failed':sum(i['outcome']=='failure' for i in attempts),'attempts':attempts})
assert not git('status','--porcelain');result['source_clean_after']=True
(dest/'D13-controlled-split-request.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'cases':[{k:v for k,v in c.items() if k!='attempts'} for c in result['cases']]}))
