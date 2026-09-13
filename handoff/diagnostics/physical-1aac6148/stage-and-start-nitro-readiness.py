"""Stage the frozen source on Nitro and dispatch its one native preparation."""
import base64,hashlib,json,pathlib,subprocess
root=pathlib.Path(__file__).parent;repo=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';baseline='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
freeze=json.loads((repo/'.acceptance/authoritative-freeze.json').read_text());assert freeze['candidate_sha']==sha and freeze['state']=='AUTHORITATIVE_FROZEN_CANDIDATE'
assert not (root/'nitro-native-dispatch.json').exists(),'Inspect the already dispatched native preparation'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==sha
bundle=repo/'.acceptance/native-source.bundle'
if not bundle.exists():subprocess.run(['git','bundle','create',str(bundle),'HEAD','^'+baseline],cwd=repo,check=True,capture_output=True)
raw=bundle.read_bytes();digest=hashlib.sha256(raw).hexdigest();content=(root/'prepare-native-readiness.py').read_text()
code='''import base64,hashlib,json,pathlib,subprocess
old=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
new=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source')
def run(args):return subprocess.check_output(args,text=True,timeout=40).strip()
if new.exists():
 assert run(['git','-C',str(new),'rev-parse','HEAD'])==SHA and not run(['git','-C',str(new),'status','--porcelain'])
else:
 assert run(['git','-C',str(old),'rev-parse','HEAD'])==BASELINE
 raw=base64.b64decode(PAYLOAD);assert hashlib.sha256(raw).hexdigest()==DIGEST
 bundle=old/'.acceptance/1aac6148-native-source.bundle';bundle.write_bytes(raw)
 run(['git','-C',str(old),'bundle','verify',str(bundle)])
 run(['git','-C',str(old),'fetch',str(bundle),'HEAD'])
 new.parent.mkdir(exist_ok=True)
 run(['git','-C',str(old),'worktree','add','--detach',str(new),SHA])
control=new/'.acceptance';control.mkdir(exist_ok=True)
receipt=control/'native-dispatch.json'
if receipt.exists():
 print(receipt.read_text());raise SystemExit(0)
assert not (control/'native-readiness/status.json').exists(),'Inspect existing native preparation'
assert not (new/'evidence').exists(),'Do not reuse or rewrite physical evidence'
(control/'authoritative-freeze.json').write_text(json.dumps(FREEZE,indent=2)+'\\n')
script=control/'prepare-native-readiness.py';script.write_text(CONTENT)
log=(control/'native-readiness-wrapper.private.log').open('wb')
p=subprocess.Popen([str(old/'.venv/bin/python'),str(script)],cwd=new,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
out={'candidate':SHA,'host':'nitro','native_preparation_pid':p.pid,'source_clean':True,'bundle_sha256':DIGEST,'status':'DISPATCHED_ONCE','runtime_activated':False,'physical_pass':False,'protected_data_accessed':False}
receipt.write_text(json.dumps(out,indent=2)+'\\n');print(json.dumps(out))
'''
script=repo/'.acceptance/stage-nitro-native.py'
prefix='SHA='+repr(sha)+'\nBASELINE='+repr(baseline)+'\nDIGEST='+repr(digest)+'\nPAYLOAD='+repr(base64.b64encode(raw).decode())+'\nFREEZE='+repr(freeze)+'\nCONTENT='+repr(content)+'\n'
script.write_text(prefix+code)
result=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','55'],capture_output=True,text=True,timeout=60)
assert result.returncode==0,'Inspect remote receipt before any retry; staging/preparation status may exist'
out=json.loads(result.stdout);(root/'nitro-native-dispatch.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
