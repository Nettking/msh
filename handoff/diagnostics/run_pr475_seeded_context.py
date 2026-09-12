"""One specified context per invocation; stop for evidence review on failure."""
import datetime
import importlib.metadata
import json
import os
import pathlib
import platform
import subprocess
import sys
import tempfile
import time

group = sys.argv[1]
assert group in {"D14", "D15"}
source = pathlib.Path('/mnt/c/wsl/fcp-fix-d13-windows-refusal-response-20260912')
diagnostics = pathlib.Path(__file__).resolve().parent
output = pathlib.Path('/mnt/c/wsl/fcp-v1-fba508-nettking-20260910/.acceptance') / (group + '-seeded-context')
output.mkdir(exist_ok=False)
git = ['/mnt/c/Program Files/Git/cmd/git.exe', '-C', r'C:\wsl\fcp-fix-d13-windows-refusal-response-20260912']
def read_git(*args):
    return subprocess.check_output([*git, *args], text=True, timeout=30).strip()
sha = read_git('rev-parse', 'HEAD')
assert sha == '5e6f184311019b9982e8544a18f3dc02c1b16e98'
assert not read_git('status', '--porcelain')
assert platform.python_version() == '3.12.13'
assert importlib.metadata.version('pytest-randomly') == '4.1.0'
contexts = json.loads((diagnostics/'D14-D15-native-order-context.json').read_text(encoding='utf-8'))['contexts'][group]
nodes = [item['class'].replace('.', '/') + '.py::' + item['test'] for item in contexts['neighbors']
         if item['index'] <= contexts['index'] and (group == 'D14' or item['index'] >= contexts['index'] - 1)]
assert len(nodes) == (9 if group == 'D14' else 2) and len(nodes) == len(set(nodes))
owned = pathlib.Path(tempfile.mkdtemp(prefix='fcp-' + group.lower() + '-seeded-'))
args = [sys.executable, '-B', '-m', 'pytest', '-o', 'addopts=', '-p', 'no:cacheprovider',
        '-p', 'pr475_context_observer', '-p', 'randomly', '--randomly-seed=1702',
        '--randomly-dont-reorganize', '-q', '-x', '--durations=10',
        '--basetemp=' + str(owned/'pytest'), '--junitxml=' + str(output/'result.xml'), *nodes]
env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
           PYTHONPATH=os.pathsep.join([str(diagnostics), str(source)]),
           PR475_CONTEXT_OBSERVER=str(output/'observer.json'))
receipt = {'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'group': group,
           'candidate_sha': sha, 'host': platform.node(), 'python': sys.version,
           'executable': sys.executable, 'command': args, 'cwd': str(source),
           'qualification': False, 'context_limitation': 'NETTKING WSL development; different from original AQG runner; bounded predecessor selection only',
           'protected_recorder_data': 'UNTOUCHED', 'timeout_seconds': 180}
def save():
    (output/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n', encoding='utf-8')
save()
started = time.monotonic()
try:
    with (output/'pytest.log').open('w', encoding='utf-8') as log:
        result = subprocess.run(args, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT,
                                timeout=180, text=True)
    receipt['exit_code'] = result.returncode
except subprocess.TimeoutExpired:
    receipt['timeout'] = True
    receipt['exit_code'] = 124
finally:
    receipt['elapsed_seconds'] = time.monotonic() - started
    receipt['final_sha'] = read_git('rev-parse', 'HEAD')
    receipt['final_tracked_status'] = read_git('status', '--porcelain', '--untracked-files=no')
    save()
print(json.dumps(receipt))
assert receipt['final_sha'] == sha and not receipt['final_tracked_status']
sys.exit(receipt['exit_code'])
