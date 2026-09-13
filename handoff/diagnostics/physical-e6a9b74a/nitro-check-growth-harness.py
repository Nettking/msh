import hashlib,json,os,pathlib,subprocess
r=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913')
h=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()=='501b528e9476878e6a6fe5cde8240b2d54b1d263'
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
c=r/'.acceptance';c.mkdir(exist_ok=True)
assert not (c/'focused-linux.json').exists(),'Inspect existing focused proof'
env=os.environ.copy()
for k in ['TMPDIR','TEMP','TMP']:env[k]=str(h/'.acceptance/native-test-tmp')
cmd=[str(h/'.venv/bin/python'),'-m','pytest','-o','addopts=','-q','catalog/federation/tests/cf7_acceptance/test_v1_physical_growth.py','--junitxml='+str(c/'focused-linux.xml')]
p=subprocess.run(cmd,cwd=r,env=env,capture_output=True,text=True,timeout=50)
log=p.stdout+p.stderr;(c/'focused-linux.log').write_text(log)
out={'acceptance_harness_sha':'501b528e9476878e6a6fe5cde8240b2d54b1d263','native_python':subprocess.check_output([str(h/'.venv/bin/python'),'--version'],text=True).strip(),'exit_code':p.returncode,'log_sha256':hashlib.sha256(log.encode()).hexdigest(),'summary':p.stdout.strip().splitlines()[-1],'physical_pass':False}
(c/'focused-linux.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
