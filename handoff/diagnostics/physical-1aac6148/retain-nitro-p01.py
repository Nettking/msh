"""Retain the completed native P01 activation proof before the separate build fault."""
import base64,hashlib,json,pathlib,subprocess
root=pathlib.Path(__file__).parent
assert not (root/'nitro-P01-initial-packets.zip').exists(),'Never overwrite initial proof'
code='''import base64,hashlib,io,json,pathlib,zipfile
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
state=json.loads((c/'P01-qualified-status.json').read_text());readonly=json.loads((c/'P01-readonly-status.json').read_text())
assert state['status']=='COMPLETED' and state['candidate']==state['harness_sha']==sha
assert state['three_activations']['verdict']==state['growth']['verdict']=='pass'
assert all(x['verdict']=='pass' for x in readonly['results'])
packets=[(p,json.loads(p.read_text())) for p in sorted((h/'evidence/v1-physical/observations/P01').glob('*.json'))]
assert all(d['candidate_sha']==d['harness_sha']==sha for _,d in packets)
assert len([d for _,d in packets if d['kind']=='sample'])==4
buf=io.BytesIO()
with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
 for p,_ in packets:z.writestr(p.name,p.read_bytes())
raw=buf.getvalue();growth=next(d for _,d in packets if d.get('assertion')=='posix-growth-bounded')
print(json.dumps({'archive':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'state':state,'readonly':readonly,'growth_detail':growth['detail']['probes'][0]['detail']}))
'''
script=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/export-nitro-p01.py');script.write_text(code)
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(script),'--timeout','30'],capture_output=True,text=True,timeout=40)
assert p.returncode==0,'Native export unavailable; leave original proof in place'
out=json.loads(p.stdout);raw=base64.b64decode(out['archive']);assert hashlib.sha256(raw).hexdigest()==out['sha256']
(root/'nitro-P01-initial-packets.zip').write_bytes(raw)
for name,key in [('nitro-P01-qualified-status.json','state'),('nitro-P01-readonly-status.json','readonly')]:
 (root/name).write_text(json.dumps(out[key],indent=2)+'\n')
report={'candidate':out['state']['candidate'],'three_activations':'PASS','hourly_growth':'PASS','resource_baseline':'PASS','runtime_state':'PASS','growth_detail':out['growth_detail'],'archive_sha256':out['sha256'],'physical_pass':False,'protected_data_accessed':False}
(root/'nitro-P01-initial-result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
