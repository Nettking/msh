import json,pathlib,subprocess,os
old=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910')
stage=old/'inputs/c03-activation'
receipt=json.loads((stage/'supported-start-staging-receipt.json').read_text())
values=json.loads(pathlib.Path(receipt['environment_file']).read_text())
env=os.environ.copy();env.update(values)
resolved=json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=old/'source',env=env,text=True,timeout=30))
out={'project':resolved['name'],'services':{},'named_volumes':resolved.get('volumes',{}),'image_setting':values.get('FCP_C03_IMAGE')}
for name in ['flask','relay','recorder','ollama']:
 s=resolved['services'][name]
 out['services'][name]={k:s.get(k) for k in ['build','image','entrypoint','command','user','volumes','ports']}
print(json.dumps(out))
