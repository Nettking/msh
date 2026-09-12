"""Preserve failing development regression before changing product source."""
import datetime,json,pathlib,subprocess
dest=pathlib.Path('handoff/diagnostics');source=pathlib.Path(r'C:\wsl\fcp-fix-d13-windows-refusal-response-20260912')
def git(*args):return subprocess.check_output(['git','-C',str(source),*args],text=True).strip()
product='catalog/federation/tailnet_join_responder.py'
assert not git('diff','--',product)
xml=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance\D13-baseline-regression.xml')
(dest/xml.name).write_bytes(xml.read_bytes())
(dest/'D13-baseline-regression.patch').write_text(git('diff','--','catalog/federation/tests/test_tailnet_join_responder.py')+'\n',encoding='utf-8')
e={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'base_sha':git('rev-parse','HEAD'),'product_blob':git('rev-parse','HEAD:'+product),'product_source_unchanged':True,'development_test_only_delta':'D13-baseline-regression.patch','host':'NETTKING/Martin native Windows Python3.12.10','command':'python -B -m pytest -o addopts= -p no:cacheprovider -q catalog/federation/tests/test_tailnet_join_responder.py::test_unknown_path_response_precedes_segmented_body_and_graceful_close --junitxml=<private>/D13-baseline-regression.xml','observed':'1failed: closed before the declared body arrived; HTTP404 already received. Deterministic server-close event, no timing-only error assertion.','expected':'Write404 immediately, retain connection until bounded valid body consumed or deadline/EOF.','qualification':False,'protected_recorder_data':'UNTOUCHED','state_changed':'Isolated development test file only, ephemeral loopback sockets and evidence outputs.'}
(dest/'D13-baseline-regression.json').write_text(json.dumps(e,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md')
with p.open('a',encoding='utf-8') as f:f.write('\n## '+e['recorded_at']+' — D13 isolated regression proves baseline failure\n\nRepair worktree is C:/wsl/fcp-fix-d13-windows-refusal-response-20260912 on codex/fix-d13-windows-refusal-response, based on unchanged actual main b7194820. New development test receives404 before sending body and observes the premature server close:1expectedFAIL on unchanged product. Receipt diagnostics/D13-baseline-regression.json and XML/patch retained. Next implement the persisted bounded refusal-body cleanup plan, with no PR473 or physical changes.\n')
