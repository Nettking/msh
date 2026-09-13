import json,pathlib,subprocess,os,shutil,hashlib
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
inputs=root.parent/'inputs'
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
assert subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip()==sha
assert not subprocess.check_output(['git','-C',str(root),'status','--porcelain'],text=True).strip()
assert not (inputs/'readiness-recovery-started.json').exists(),'Inspect the existing recovery; do not repeat it'
for source,name in [(root/'evidence/school-control/gate-summary.json','readiness-initial-failed-summary.json'),(root/'evidence/linux/commands.txt','readiness-initial-failed-commands.txt')]:
 shutil.copyfile(source,inputs/name)
tmp=root/'.acceptance/native-test-tmp';tmp.mkdir(parents=True,exist_ok=True)
env=os.environ.copy();env.update(TMPDIR=str(tmp),TMP=str(tmp),TEMP=str(tmp))
python=str(root/'.venv/bin/python')
p=subprocess.run([python,'scripts/ci_release_disk_preflight.py'],cwd=root,env=env,capture_output=True,text=True,timeout=30)
(inputs/'native-storage-precondition.log').write_text(p.stdout+'\n'+p.stderr)
assert p.returncode==0,'Unchanged product floor still refuses the selected filesystem'
log=(inputs/'readiness-recovery.log').open('wb')
process=subprocess.Popen([python,'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(root),'--evidence-root','evidence','gate','--machine','school-control','--commit',sha],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
r={'candidate':sha,'status':'RUNNING','process_id':process.pid,'recovery':'Set only test temporary-directory environment to owned candidate filesystem with adequate capacity','product_thresholds_unchanged':True,'original_temp_files_untouched':True,'initial_failure_classification':'Deterministic native test environment precondition failure; product resource refusal is correct','full_ci_rerun':False,'physical_pass':False}
(inputs/'readiness-recovery-started.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r))
