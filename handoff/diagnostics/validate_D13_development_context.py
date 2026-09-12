"""Single failed-test recheck with CI-equivalent child PATH, no global change."""
import json,os,pathlib,subprocess,sys
source=pathlib.Path(r'C:\wsl\fcp-fix-d13-windows-refusal-response-20260912')
private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
environment=dict(os.environ,PATH=str(pathlib.Path(sys.executable).parent)+os.pathsep+os.environ['PATH'],PYTHONDONTWRITEBYTECODE='1')
args=[sys.executable,'-B','-m','pytest','-o','addopts=','-p','no:cacheprovider','-q','catalog/mtconnect_recorder/tests/test_tailscale_recorder_launcher.py::test_windows_probe_rejects_runtime_that_cannot_import_exact_entrypoint','--basetemp=C:\\fcp-qtmp\\d13-fallback-correct-context','--junitxml='+str(private/'D13-fallback-correct-context.xml')]
completed=subprocess.run(args,cwd=source,env=environment,capture_output=True,text=True,timeout=60)
result={'command':args,'returncode':completed.returncode,'output':completed.stdout+completed.stderr,'environment_delta':'Only child PATH prepended testing venv Scripts and PYTHONDONTWRITEBYTECODE=1; original source unchanged.','protected_recorder_data':'UNTOUCHED'}
path=pathlib.Path(__file__).resolve().parent/'D13-fallback-correct-context.json';path.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result));sys.exit(completed.returncode)
