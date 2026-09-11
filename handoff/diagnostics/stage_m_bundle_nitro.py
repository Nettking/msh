"""Stage exact qualified Git bundle on Nitro, without changing its source/runtime."""
import base64,hashlib,json,pathlib,subprocess,sys
A=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
D=pathlib.Path(__file__).resolve().parent
bundle=A/'source-9b286f93-from-0536.bundle';digest=hashlib.sha256(bundle.read_bytes()).hexdigest()
code="""
import base64,datetime,hashlib,json,pathlib,platform,subprocess
R=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
assert platform.node().casefold()=='nitro'
def git(*args):return subprocess.check_output(['git',*args],cwd=R,text=True,stderr=subprocess.PIPE,timeout=20).strip()
assert git('rev-parse','HEAD')=='0536f03d67eb277e11573c2188d8e820399627e3' and not git('status','--porcelain','--untracked-files=all')
path=R.parent/'inputs/source-9b286f93-from-0536.bundle'
raw=base64.b64decode(PAYLOAD);assert hashlib.sha256(raw).hexdigest()==DIGEST
if path.exists():assert hashlib.sha256(path.read_bytes()).hexdigest()==DIGEST
else:
 with path.open('xb') as f:f.write(raw)
 path.chmod(0o600)
git('bundle','verify',str(path))
assert git('bundle','list-heads',str(path))=='9b286f931497bf6291e215f6340443c5162826b0 HEAD'
print(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'EXACT_M_BUNDLE_STAGED_VERIFIED_NOT_DEPLOYED','host':'nitro','candidate':'9b286f931497bf6291e215f6340443c5162826b0','bundle_sha256':DIGEST,'bundle_bytes':len(raw),'remote_path':str(path),'current_source':'0536f03d67eb277e11573c2188d8e820399627e3','source_clean':True,'runtime_changed':False,'protected_recorder_data_untouched':True,'physical_acceptance':False}))
"""
payload=A/'stage-m-nitro-payload.private.py'
payload.write_text('PAYLOAD='+repr(base64.b64encode(bundle.read_bytes()).decode())+'\nDIGEST='+repr(digest)+'\n'+code)
result=subprocess.run([sys.executable,'-B',str(A/'ssh_campaign_script.py'),'nitro',str(payload),'--nitro-runtime-python','--timeout','50'],capture_output=True,timeout=55)
assert result.returncode==0,result.stderr.decode(errors='replace')[-1500:]
receipt=json.loads(result.stdout);(D/'m-nitro-bundle-staged.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
