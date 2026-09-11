"""Launch the reviewed audit controller detached; product launcher/source unmodified."""
import base64,hashlib,json,pathlib,subprocess,sys
A=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance');D=pathlib.Path(__file__).resolve().parent
source=(D/'activate_nitro_m.py').read_bytes();digest=hashlib.sha256(source).hexdigest()
remote="""
import base64,datetime,hashlib,json,pathlib,platform,subprocess
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');I=B/'inputs'
assert platform.node().casefold()=='nitro'
script=I/'activate_nitro_m.py';receipt=I/'m-activation-controller.json';log=I/'m-activation-controller.log'
assert not script.exists() and not receipt.exists() and not log.exists() and not (I/'supported-start-9b286f93.json').exists(), 'Inspect existing operation before retry'
raw=base64.b64decode(PAYLOAD);assert hashlib.sha256(raw).hexdigest()==DIGEST
with script.open('xb') as f:f.write(raw)
script.chmod(0o600)
with log.open('xb') as f:
 p=subprocess.Popen([str(B/'host-venv/bin/python3'),'-B',str(script)],cwd=B/'source',stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
result={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'CONTROLLER_DISPATCHED_INSPECT_OPERATION_RECEIPT','host':'nitro','candidate':'9b286f931497bf6291e215f6340443c5162826b0','controller_pid':p.pid,'controller_sha256':DIGEST,'controller_script':str(script),'operation_receipt':str(I/'supported-start-9b286f93.json'),'controller_log':str(log),'product_log':str(I/'supported-start-9b286f93-native.log'),'protected_recorder_operation':False,'physical_pass':False}
receipt.write_text(json.dumps(result,indent=2)+'\\n');receipt.chmod(0o600)
print(json.dumps(result))
"""
payload=A/'dispatch-nitro-m.private.py';payload.write_text('PAYLOAD='+repr(base64.b64encode(source).decode())+'\nDIGEST='+repr(digest)+'\n'+remote)
p=subprocess.run([sys.executable,'-B',str(A/'ssh_campaign_script.py'),'nitro',str(payload),'--nitro-runtime-python','--timeout','40'],capture_output=True,timeout=45)
assert p.returncode==0,p.stderr.decode(errors='replace')[-1000:]
r=json.loads(p.stdout);(D/'nitro-m-controller-dispatch.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
