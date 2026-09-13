import json,pathlib,os,shutil,tempfile,subprocess
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
rows=[]
for alias,path in [('default-temporary',pathlib.Path(tempfile.gettempdir())),('candidate-checkout',root)]:
 usage=shutil.disk_usage(path);st=os.statvfs(path)
 rows.append({'alias':alias,'total_bytes':usage.total,'free_bytes':usage.free,'free_inodes':st.f_favail,'device_id':os.stat(path).st_dev})
print(json.dumps({'filesystems':rows,'tempdir_is_tmp':tempfile.gettempdir()=='/tmp'}))
p=subprocess.run([str(root/'.venv/bin/python'),'-m','pytest','-o','addopts=','-q','--tb=short','--maxfail=1','catalog/flask_app/tests/test_capability_inspection_route.py::test_app_factory_registers_cfi3_before_the_legacy_gate'],cwd=root,capture_output=True,text=True,timeout=70)
print('FOCUSED_DEFAULT_TMP_EXIT',p.returncode)
print(p.stdout[-13000:])
