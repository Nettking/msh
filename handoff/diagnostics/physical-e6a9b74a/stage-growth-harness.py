"""Stage the pinned acceptance-only repair without changing either runtime."""
import base64,hashlib,json,pathlib,subprocess
here=pathlib.Path(__file__).parent
bundle=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/growth-harness.bundle')
sha='501b528e9476878e6a6fe5cde8240b2d54b1d263'
payload=base64.b64encode(bundle.read_bytes()).decode()
digest=hashlib.sha256(bundle.read_bytes()).hexdigest()
code='''import base64,hashlib,json,pathlib,subprocess
old=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
new=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913')
assert not new.exists(), 'Inspect the already staged harness'
bundle=old/'.acceptance/growth-harness.bundle'
raw=base64.b64decode(PAYLOAD);assert hashlib.sha256(raw).hexdigest()==DIGEST
bundle.write_bytes(raw)
def run(args):return subprocess.check_output(args,text=True,timeout=40).strip()
assert run(['git','-C',str(old),'rev-parse','HEAD'])=='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
run(['git','-C',str(old),'fetch',str(bundle),'HEAD'])
run(['git','-C',str(old),'worktree','add','--detach',str(new),SHA])
assert run(['git','-C',str(new),'rev-parse','HEAD'])==SHA
assert not run(['git','-C',str(new),'status','--porcelain'])
print(json.dumps({'acceptance_harness_sha':SHA,'bundle_sha256':DIGEST,'source_clean':True,'product_candidate_unchanged':True,'runtime_activated':False,'physical_pass':False}))
'''
script=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/stage-nitro.py')
script.write_text('SHA='+repr(sha)+'\nDIGEST='+repr(digest)+'\nPAYLOAD='+repr(payload)+'\n'+code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','55'],capture_output=True,text=True,timeout=60)
assert p.returncode==0,p.stderr
out=json.loads(p.stdout)
(here/'nitro-growth-harness-staged.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
