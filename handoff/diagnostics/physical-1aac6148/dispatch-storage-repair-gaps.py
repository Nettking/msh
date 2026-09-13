"""Dispatch only missing qualification workflows once for the reviewed repair head."""
import datetime
import json
import pathlib
import sys

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api = client()
sha = '76ad339f1631e136bba7a8a85973bddb1650d570'
branch = 'codex/federation-v1-storage-reply-repair'
path = pathlib.Path(__file__).parent / 'storage-reply-repair-gap-dispatch.json'
assert not path.exists(), 'Inspect the existing one-time dispatch receipt'
workflows = ('cf7-acceptance-harness.yml', 'cf7c-physical-test-readiness.yml', 'phase2-federation.yml', 'ci-test-sharding.yml', 'release-image-metadata.yml')
out = {'source': sha, 'ref': branch, 'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'actions': []}
def save():
    path.write_text(json.dumps(out, indent=2) + '\n')
save()
for name in workflows:
    assert api('/pulls/487')['head']['sha'] == sha
    assert api('/git/ref/heads/' + branch)['object']['sha'] == sha
    runs = api('/actions/runs?head_sha=' + sha + '&per_page=100')['workflow_runs']
    existing = [row['id'] for row in runs if row['path'].split('/')[-1] == name]
    row = {'workflow': name, 'existing': existing}
    out['actions'].append(row)
    save()
    if existing:
        row['status'] = 'already-present'
    else:
        row['status'] = 'dispatching'
        save()
        row['response'] = api('/actions/workflows/' + name + '/dispatches', {'ref': branch})
        row['status'] = 'accepted'
    save()
print(json.dumps(out))
