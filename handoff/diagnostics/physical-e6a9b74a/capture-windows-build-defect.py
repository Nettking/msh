import hashlib,json,pathlib,subprocess,sys
here=pathlib.Path(__file__).parent
fix=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913')
old=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/runtime-control')
green=fix/'.acceptance/physical-build-proof-handle'
sys.path.insert(0,'C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
from scripts.acceptance.v1_physical_campaign import redact_text
red=json.loads((old/'P01-missing-dockerfile-status.json').read_text())
proof=json.loads((green/'proof.json').read_text())
sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=fix,text=True).strip()
assert subprocess.check_output(['git','rev-parse',sha+':scripts/windows/fcp_host_build.ps1'],cwd=fix,text=True).strip()==proof['controller_git_blob']
assert red['controller_exit_code']==0 and red['missing_dockerfile_error_observed'] and red['success_marker_written']
assert proof['controller_exit_code']==1 and proof['controller_refused_build'] and not proof['success_marker_written']
for name,source in [('windows-build-exit-red.log',old/'missing-dockerfile.private.log'),('windows-build-exit-green.log',green/'missing-dockerfile.private.log'),('windows-build-exit-tests.log',fix/'.acceptance/windows-build-tests.log')]:
 (here/name).write_text(redact_text(source.read_text(errors='replace'),cwd=fix)+'\n')
out={'classification':'demonstrated current-candidate product defect','affected_candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','also_present_on_main':'1492d925d791b9a0ebc7bcee39ce9b3b7477254b','checked_in_contract':'scripts/windows/fcp_host_build.ps1 Invoke-ControlledCoreBuild must refuse nonzero native build exit through its bounded failure cleanup; a failed build must not publish its success marker.','repair_head':sha,'red':red,'green':proof,'native_regression_before':'child exit7 falsely returns success; exit0 succeeds','native_regression_after':'exit0 succeeds; exit7 refuses with exact code; stdout/stderr preserved; existing cleanup observed','focused_tests_passed':20,'limits_or_authority_changed':False,'runtime_activated':False,'protected_data_accessed':False,'physical_campaign_pass':False,'release_decision':'C is blocked. Qualify repaired source, merge normally, qualify actual main, freeze replacement and revalidate before new physical acceptance.'}
(here/'windows-build-exit-defect.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'classification':out['classification'],'repair_head':sha,'red_exit':red['controller_exit_code'],'red_success_marker':red['success_marker_written'],'green_exit':proof['controller_exit_code'],'green_success_marker':proof['success_marker_written'],'tests_passed':20,'physical_pass':False}))
