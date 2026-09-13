import pathlib,json,subprocess,datetime
root=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source');new=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
r={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'old_runtime_sha':subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip(),'old_runtime_clean':not subprocess.check_output(['git','-C',str(root),'status','--porcelain'],text=True).strip(),'untracked_build_input_presence':{n:(root/n).exists() for n in ['.acceptance','evidence','.env','.venv']},'activation':json.loads((new/'.acceptance/runtime-control/activation-status.json').read_text())}
print(json.dumps(r))
