"""Bounded unchanged-test diagnostic; no service/secret/data/runner mutation."""
import contextlib
import datetime
import hashlib
import importlib
import io
import json
import os
import pathlib
import platform
import subprocess
import sys
import time
import traceback

source = pathlib.Path(r'C:\wsl\fcp-ci-phase-workflow-retirement-20260912')
dest = pathlib.Path(__file__).resolve().parent
expected = '440123f6bc6dc358eef3d233236bc14f91af60e0'
assert platform.node().upper() == 'NETTKING'
def git(*args):
    return subprocess.check_output(['git', '-C', str(source), *args], text=True).strip()
assert git('rev-parse', 'HEAD') == expected
assert not git('status', '--porcelain')
sys.dont_write_bytecode = True
sys.path.insert(0, str(source))
module = importlib.import_module('catalog.federation.tests.test_tailnet_join_responder')
result = {'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'candidate_sha': expected, 'host': platform.node(),
          'user_context': subprocess.check_output(['whoami'], text=True).strip(),
          'python': platform.python_version(), 'platform': platform.platform(),
          'procedure': 'Call the unchanged test_unknown_paths_are_refused up to30 times; stop on first failure; original5s client timeout and server lifecycle.',
          'qualification': False, 'context_limitation': 'Interactive Martin context, not the NETWORK SERVICE Actions context.',
          'protected_recorder_data': 'UNTOUCHED', 'physical_runtime': 'UNCHANGED',
          'state_changed': 'Only ephemeral loopback test responders and diagnostic output; no production service or source changes.',
          'source_blobs': {p:git('rev-parse', 'HEAD:'+p) for p in ['catalog/federation/tests/test_tailnet_join_responder.py','catalog/federation/tailnet_join_responder.py']},
          'attempts': []}
started = time.monotonic()
for i in range(30):
    assert time.monotonic() - started < 55
    step = time.monotonic()
    stderr = io.StringIO()
    item = {'iteration':i+1}
    try:
        with contextlib.redirect_stderr(stderr):
            module.test_unknown_paths_are_refused()
        item['outcome'] = 'pass'
    except Exception as error:
        item.update(outcome='failure', exception=type(error).__name__, message=str(error), traceback=traceback.format_exc())
    item.update(seconds=round(time.monotonic()-step,4),stderr=stderr.getvalue())
    result['attempts'].append(item)
    if item['outcome']=='failure':
        break
result['elapsed_seconds']=round(time.monotonic()-started,4)
result['source_clean_after']=not git('status','--porcelain')
assert result['source_clean_after']
path=dest/'D13-unchanged-local-reproduction.json'
path.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'receipt':str(path),'attempts':len(result['attempts']),'last':result['attempts'][-1],'context':result['user_context']}))
