"""Bounded diagnostic clients; unchanged server source and five-second deadline."""
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
import urllib.error
import urllib.request

source=pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912')
dest=pathlib.Path(__file__).resolve().parent
sys.dont_write_bytecode=True
def git(*args):
    return subprocess.check_output(['git','-C',str(source),*args],text=True).strip()
assert git('rev-parse','HEAD')=='440123f6bc6dc358eef3d233236bc14f91af60e0'
assert not git('status','--porcelain')
assert platform.node().upper()=='NETTKING'
sys.path.insert(0,str(source))
from catalog.federation.tests import test_tailnet_join_responder as test

result={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'candidate_sha':git('rev-parse','HEAD'),'host':platform.node(),
        'user_context':subprocess.check_output(['whoami'],text=True).strip(),
        'python':platform.python_version(),'deadline_seconds':5,
        'procedure':'20 attempts per framing case, on fresh ephemeral loopback unchanged product responder; server shutdown plus explicit fixture close after each attempt.',
        'state_changed':'Diagnostic-owned ephemeral sockets only; source and production services unchanged.',
        'protected_recorder_data':'UNTOUCHED','qualification':False,'cases':[]}
def grant(**kwargs):
    raise AssertionError('pairing authority must not be invoked')
started=time.monotonic()
for case in ['urllib_empty_body','urllib_two_byte_body','single_send_two_byte_body']:
    outcomes=[]
    for iteration in range(20):
        assert time.monotonic()-started<55
        stderr=io.StringIO()
        with contextlib.redirect_stderr(stderr):
            server,port=test._serve(lambda _address: test.PeerVerification(test.PEER,'verified'),grant)
            try:
                if case=='single_send_two_byte_body':
                    with socket.create_connection(('127.0.0.1',port),timeout=5) as sock:
                        sock.sendall(('POST /anything-else HTTP/1.1\r\nHost:127.0.0.1:'+str(port)+'\r\nContent-Length:2\r\nConnection:close\r\n\r\n{}').encode('ascii'))
                        response=http.client.HTTPResponse(sock)
                        response.begin();status=response.status;payload=json.loads(response.read())
                else:
                    request=urllib.request.Request(f'http://127.0.0.1:{port}/anything-else',data=b'' if case=='urllib_empty_body' else b'{}',method='POST')
                    try:
                        response=urllib.request.urlopen(request,timeout=5)
                    except urllib.error.HTTPError as error:
                        response=error
                    with response:
                        status=response.code;payload=json.loads(response.read())
                assert status==404 and payload=={'accepted':False,'error':'unknown-path'}, (status,payload)
                item={'iteration':iteration+1,'outcome':'pass'}
            except Exception as error:
                item={'iteration':iteration+1,'outcome':'failure','exception':type(error).__name__,'message':str(error)}
            finally:
                server.shutdown();server.server_close()
        item['server_stderr']=stderr.getvalue();outcomes.append(item)
    result['cases'].append({'case':case,'passed':sum(i['outcome']=='pass' for i in outcomes),'failed':sum(i['outcome']=='failure' for i in outcomes),'attempts':outcomes})
result['elapsed_seconds']=round(time.monotonic()-started,3)
result['source_clean_after']=not git('status','--porcelain');assert result['source_clean_after']
(dest/'D13-request-framing-contrast.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'cases':[{k:v for k,v in c.items() if k!='attempts'} for c in result['cases']],'elapsed_seconds':result['elapsed_seconds']}))
