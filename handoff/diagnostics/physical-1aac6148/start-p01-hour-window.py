import datetime,json,pathlib,subprocess
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/runtime-control';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (c/'P01-hour-intent.json').exists(),'One follow-up is already declared; inspect its existing status'
state=json.loads((c/'P01-qualified-status.json').read_text());assert state['status']=='COMPLETED' and state['three_activations']['verdict']=='pass' and state['growth']['verdict']=='fail'
first=json.loads((h/json.loads((c/'qualified-baseline.json').read_text())['evidence']).read_text())
last=json.loads((h/json.loads((c/'qualified-sample-3.json').read_text())['evidence']).read_text())
failure=json.loads((h/state['growth']['evidence']).read_text());detail=failure['detail']['probes'][0]['detail']
assert first['candidate_sha']==last['candidate_sha']==sha and detail['max_growth_bytes_per_hour']==1073741824
deadline=datetime.datetime.fromisoformat(first['recorded_at'].replace('Z','+00:00'))+datetime.timedelta(hours=1)
intent={'candidate':sha,'declared_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'original_baseline':first['recorded_at'],'sample_not_before':deadline.isoformat(),'follow_up_samples':1,'ceiling_bytes_per_hour':1073741824,'initial_result':'FAIL','initial_used_bytes_delta':detail['used_bytes_delta'],'initial_projected_bytes_per_hour':detail['growth_bytes_per_hour'],'initial_sample_count':detail['sample_count'],'three_activations_retained':'PASS','classification':'unresolved but non-demonstrated candidate defect','decision':'Preserve the failed short interval. One predeclared follow-up will measure the same candidate and backing filesystem across a real hour, retaining all original samples and the unchanged ceiling. No more Windows activation/fault work during this observation window.','physical_pass':False,'P07_started':False,'P12_started':False,'runtime_activation_requested':False}
(c/'P01-hour-intent.json').write_text(json.dumps(intent,indent=2)+'\n');(root/'windows-P01-hour-intent.json').write_text(json.dumps(intent,indent=2)+'\n')
log=(c/'P01-hour-wrapper.private.log').open('wb')
p=subprocess.Popen(['C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe',str(root/'observe-p01-hour.py')],cwd=h,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
dispatch={'candidate':sha,'pid':p.pid,'sample_not_before':intent['sample_not_before'],'status':'DISPATCHED_ONCE','physical_pass':False}
(c/'P01-hour-dispatch.json').write_text(json.dumps(dispatch,indent=2)+'\n');(root/'windows-P01-hour-dispatch.json').write_text(json.dumps(dispatch,indent=2)+'\n');print(json.dumps(dispatch))
