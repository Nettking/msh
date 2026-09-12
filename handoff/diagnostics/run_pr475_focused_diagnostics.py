"""Bounded isolated D14/D15 reproduction; no workflow or runtime deployment."""
import os,pathlib,subprocess,sys
source=pathlib.Path('/mnt/c/wsl/fcp-fix-d13-windows-refusal-response-20260912')
# This is a Windows-owned worktree: its .git pointer uses a Windows absolute
# path. Verify using its native Git, while the test process remains Linux.
git=['/mnt/c/Program Files/Git/cmd/git.exe','-C',r'C:\wsl\fcp-fix-d13-windows-refusal-response-20260912']
assert subprocess.check_output([*git,'rev-parse','HEAD'],text=True).strip()=='5e6f184311019b9982e8544a18f3dc02c1b16e98'
assert not subprocess.check_output([*git,'status','--porcelain'],text=True).strip()
environment=dict(os.environ,PYTHONPATH=str(pathlib.Path(__file__).resolve().parent),PYTHONDONTWRITEBYTECODE='1')
args=[sys.executable,'-B','-m','pytest','-o','addopts=','-p','no:cacheprovider','-p','pr475_failure_observer','-q','--keep-duplicates','--basetemp=/tmp/fcp-d14-d15-focused-20260912','--junitxml=/mnt/c/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/D14-D15-focused.xml','catalog/flask_app/tests/test_capability_benchmark_route.py::test_skip_serializes_with_concurrent_benchmark_completion']
args+=['catalog/federation/tests/cf7_acceptance/test_product_acceptance.py::test_three_device_federation_keeps_ai_compute_and_storage_authority_separate']*5
completed=subprocess.run(args,cwd=source,env=environment,timeout=90)
sys.exit(completed.returncode)
