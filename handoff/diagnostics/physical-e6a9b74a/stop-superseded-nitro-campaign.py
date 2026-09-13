"""Stop only the acceptance orchestrator; its current product operation settles normally."""
import datetime,json,os,pathlib,signal
h=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913');c=h/'.acceptance/runtime-control'
assert not (c/'candidate-defect-stop.json').exists(),'Inspect the existing stop receipt'
status=json.loads((c/'P01-qualified-status.json').read_text());dispatch=json.loads((c/'qualified-dispatch.json').read_text());pid=dispatch['pid']
receipt={'candidate':dispatch['candidate'],'harness_sha':dispatch['harness_sha'],'reason':'Demonstrated Windows build-exit defect blocks this candidate; avoid further superseded physical activations.','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'product_operation_killed':False,'physical_pass':False}
proc=pathlib.Path('/proc')/str(pid)
if status['status']=='RUNNING' and proc.exists():
 command=(proc/'cmdline').read_bytes().split(b'\0');assert str(h/'.acceptance/run-qualified-p01.py').encode() in command
 children=[]
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   fields=(p/'stat').read_text().rsplit(')',1)[1].split()
   if int(fields[1])==pid:children.append({'pid':int(p.name),'start_ticks':fields[19]})
  except (OSError,ValueError,IndexError):continue
 os.kill(pid,signal.SIGTERM)
 receipt.update(orchestrator_stopped=True,orchestrator_pid=pid,current_children_allowed_to_settle=children,completed_activations=len(status['completed_activations']))
 status.update(status='STOPPED_FOR_CANDIDATE_DEFECT',stop_receipt='candidate-defect-stop.json',current_product_operation_may_still_be_settling=True)
 (c/'P01-qualified-status.json').write_text(json.dumps(status,indent=2)+'\n')
else:receipt.update(orchestrator_stopped=False,already_terminal_status=status['status'])
(c/'candidate-defect-stop.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
