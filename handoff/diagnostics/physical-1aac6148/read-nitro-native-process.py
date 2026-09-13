import json,pathlib,subprocess
root=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');control=root/'.acceptance'
dispatch=json.loads((control/'native-dispatch.json').read_text());parent=dispatch['native_preparation_pid']
lines=subprocess.check_output(['ps','-eo','pid=,ppid=,etimes=,stat=,comm='],text=True).splitlines()
rows=[]
for line in lines:
 values=line.split(None,4)
 if len(values)==5:rows.append({'pid':int(values[0]),'ppid':int(values[1]),'elapsed_seconds':int(values[2]),'state':values[3],'command':values[4]})
owned={parent}
for _ in range(8):owned.update(r['pid'] for r in rows if r['ppid'] in owned)
status=json.loads((control/'native-readiness/status.json').read_text())
gate=root/'evidence/school-control/gate-summary.json'
out={'candidate':status['candidate'],'preparation_status':status['status'],'processes':[r for r in rows if r['pid'] in owned],'gate_summary_exists':gate.exists(),'physical_pass':False}
if gate.exists():out['gate_summary']=json.loads(gate.read_text())
if status['status']=='STOPPED':out['error_type']=status.get('error_type')
(control/'native-readiness/process-snapshot.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
